//go:build integration

// authz_revoke_race_integration_test.go owns spec task T044 for
// 008-nonce-manager (US5; FR-17/FR-23; observation.md §2.1/§3.4; V11; SC-05):
// the revoke-vs-admission (and supply-vs-admission) race over a real
// PostgreSQL.
//
// What it proves:
//   - RevokeGrant committing before the admission observes the share lock:
//     the admission's `SELECT … FOR SHARE` re-evaluates the row predicate under
//     the lock (READ COMMITTED) and refuses authorization_invalid with zero
//     binding and zero new 008 rows.
//   - SupplyGrant (re-supply) committing before the share lock is observed: the
//     admission binds against the freshly supplied row (version digest of the
//     new amount), so a committed upstream write is never missed.
//   - An admission that takes the share lock first commits, and the 007 writer
//     waits until it does: the writer's `SELECT … FOR UPDATE` blocks behind the
//     008 share lock and only proceeds after the admission commits. The fixed
//     order scope row → 006 gate reads → 007 authorization row → own rows
//     neither deadlocks nor lets 008 write a 007 row.
//
// The 007 writer is simulated with raw SQL in its own transaction (RevokeGrant
// / SupplyGrant lock order: SELECT … FOR UPDATE on the authorization row, then
// its UPDATE, then its audit append). The withdrawal package is never imported.
// The only 008 surface touched is the exported nonce.NewAllocator/Allocate API.
//
// Determinism: the admission is real, and the tests pin the interleaving with
// lock observation rather than sleeps alone. An ACCESS EXCLUSIVE lock on an
// 008 own-row table (nonce_observations) parks the admission *after* it has
// taken the 007 share lock, so the 007 writer's block on that share lock can be
// observed and asserted. Every raw transaction carries a lock_timeout so a
// lock cycle surfaces as an error, never a hang.
//
// The PostgreSQL container pattern is reused from migration_integration_test.go;
// every helper this file owns is `rr`-prefixed so it cannot collide with a
// sibling integration test.
package nonce_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/nonce"
)

const (
	rrChain  = int64(80092)
	rrCaller = int64(9)

	rrAuthRevoke      = "rr-race-revoke"
	rrAuthSupply      = "rr-race-supply"
	rrAuthShareRevoke = "rr-race-share-revoke"
	rrAuthShareSupply = "rr-race-share-supply"
	rrAuthPure        = "rr-race-pure"
	rrSupplyAmount    = "5"
	rrSeededAmount    = "1"
	rrHoldWindow      = 200 * time.Millisecond
	rrDeadline        = 10 * time.Second
	rrWaitDeadline    = 5 * time.Second
	rrRawLockTimeout  = "8s"
)

var (
	rrSenderRevoke      = nonceAddr("51")
	rrSenderSupply      = nonceAddr("52")
	rrSenderShareRevoke = nonceAddr("53")
	rrSenderShareSupply = nonceAddr("54")
	rrSenderPure        = nonceAddr("55")
	rrAsset             = nonceAddr("a2")
	rrRecipient         = nonceAddr("b3")
	rrHeadHash          = "0x" + strings.Repeat("ab", 32)
)

// rrUpstreamTables are every 006/007 domain table 008 reads but must never
// write. indexer_lease is deliberately excluded: it is the shared coordination
// row 008 serializes on, not 006/007-owned domain data.
var rrUpstreamTables = []string{
	"reorg_recovery", "reorg_recovery_events", "reorg_policy_history",
	"indexer_pause", "log_pause", "deposit_pause", "deposit_pause_audit",
	"chain_blocks", "indexer_checkpoint", "log_checkpoint",
	"erc20_transfer_logs", "deposit_observations", "deposit_checkpoint",
	"deposit_config_history", "confirmation_policy_history",
	"deposit_observation_transitions",
	"caller", "api_key", "withdrawal_requests", "withdrawal_authorizations",
	"withdrawal_request_audit", "withdrawal_grant_audit",
}

// rrRPC is the scripted JSON-RPC double: a self-consistent healthy chain view
// (latest == pending == 0, head 0) so each admission allocates the next nonce.
type rrRPC struct{}

func (rrRPC) CallContext(_ context.Context, result any, method string, args ...any) error {
	switch method {
	case "eth_getTransactionCount":
		return json.Unmarshal([]byte(`"0x0"`), result)
	case "eth_getBlockByNumber":
		payload := fmt.Sprintf(`{"number":"0x0","hash":%q}`, rrHeadHash)
		return json.Unmarshal([]byte(payload), result)
	default:
		return fmt.Errorf("rrRPC: unexpected method %q", method)
	}
}

func rrAllocator(pool *pgxpool.Pool) *nonce.Allocator {
	observer := nonce.NewObserver(rrRPC{}, nonce.ObserverConfig{
		RPCTimeout:   2 * time.Second,
		RetryInitial: time.Millisecond,
		RetryMax:     5 * time.Millisecond,
	})
	admitGate := nonce.NewRebuildGate()
	admitGate.Open()
	return nonce.NewAllocator(pool, observer, admitGate)
}

func rrOpenPool(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := db.OpenPool(ctx, dsn, 5*time.Second)
	if err != nil {
		t.Fatalf("open pgx pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// rrSeed inserts the shared 007 caller and one active registry row per sender.
func rrSeed(t *testing.T, sqlDB *sql.DB, senders ...string) {
	t.Helper()
	nonceMustExec(t, sqlDB, `INSERT INTO caller (caller_id, label) VALUES ($1, 'rr-it')`, rrCaller)
	for _, sender := range senders {
		nonceMustExec(t, sqlDB, `INSERT INTO nonce_wallet_registry
			(chain_id, sender, state, registry_seq) VALUES ($1, $2, 'active', 1)`, rrChain, sender)
	}
}

// rrSeedAuth inserts one 007 authorization row (amount 1, no expiry) via raw SQL.
func rrSeedAuth(t *testing.T, sqlDB *sql.DB, id, state string) {
	t.Helper()
	nonceMustExec(t, sqlDB, `INSERT INTO withdrawal_authorizations
		(authorization_id, caller_id, chain_id, asset, recipient, amount, state, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6::numeric, $7, NULL)`,
		id, rrCaller, rrChain, rrAsset, rrRecipient, rrSeededAmount, state)
}

// rrSnapshot renders each 006/007 table's full row set as one canonical string;
// equality of two maps is the byte-identity assertion.
func rrSnapshot(t *testing.T, sqlDB *sql.DB) map[string]string {
	t.Helper()
	out := make(map[string]string, len(rrUpstreamTables))
	for _, table := range rrUpstreamTables {
		out[table] = rrTableBlob(t, sqlDB, table)
	}
	return out
}

func rrTableBlob(t *testing.T, sqlDB *sql.DB, table string) string {
	t.Helper()
	var blob string
	query := fmt.Sprintf(
		`SELECT COALESCE(string_agg(t::text, E'\n' ORDER BY t::text), '<empty>') FROM %s t`, table)
	if err := sqlDB.QueryRow(query).Scan(&blob); err != nil {
		t.Fatalf("snapshot %s: %v", table, err)
	}
	return blob
}

// rrWantTablesUnchanged asserts every 006/007 table blob is byte-identical
// except the named tables the simulated 007 writer legitimately mutates.
func rrWantTablesUnchanged(t *testing.T, before, after map[string]string, allow ...string) {
	t.Helper()
	skip := make(map[string]bool, len(allow))
	for _, a := range allow {
		skip[a] = true
	}
	for _, table := range rrUpstreamTables {
		if skip[table] {
			continue
		}
		if before[table] != after[table] {
			t.Fatalf("008 wrote a 006/007 row in %s:\nbefore=%q\nafter =%q",
				table, before[table], after[table])
		}
	}
}

// rrAuthExceptBlob digests every authorization row except the raced one, so a
// change isolated to the raced row cannot mask a 008 write to a sibling row.
func rrAuthExceptBlob(t *testing.T, sqlDB *sql.DB, excludeID string) string {
	t.Helper()
	var blob string
	if err := sqlDB.QueryRow(`SELECT COALESCE(string_agg(t::text, E'\n' ORDER BY t::text), '<empty>')
		FROM withdrawal_authorizations t WHERE authorization_id <> $1`, excludeID).Scan(&blob); err != nil {
		t.Fatalf("snapshot authorizations except %s: %v", excludeID, err)
	}
	return blob
}

// rrAuthState reads the raced authorization row's state and amount.
func rrAuthState(t *testing.T, sqlDB *sql.DB, id string) (string, string) {
	t.Helper()
	var state, amount string
	if err := sqlDB.QueryRow(`SELECT state, amount::text FROM withdrawal_authorizations
		WHERE authorization_id = $1`, id).Scan(&state, &amount); err != nil {
		t.Fatalf("read authorization %s: %v", id, err)
	}
	return state, amount
}

func rrCount(t *testing.T, sqlDB *sql.DB, table string) int {
	t.Helper()
	var n int
	if err := sqlDB.QueryRow(fmt.Sprintf(`SELECT count(*) FROM %s`, table)).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

// rrCensus is the comparable whole-008 row census (zero value = empty).
type rrCensus struct {
	bindings     int
	events       int
	observations int
	holds        int
	scopes       int
}

func rrCensusOf(t *testing.T, sqlDB *sql.DB) rrCensus {
	t.Helper()
	var c rrCensus
	if err := sqlDB.QueryRow(`SELECT
		(SELECT count(*) FROM nonce_bindings),
		(SELECT count(*) FROM nonce_binding_events),
		(SELECT count(*) FROM nonce_observations),
		(SELECT count(*) FROM nonce_scope_holds),
		(SELECT count(*) FROM nonce_scope_state)`).
		Scan(&c.bindings, &c.events, &c.observations, &c.holds, &c.scopes); err != nil {
		t.Fatalf("count 008 rows: %v", err)
	}
	return c
}

// rrDigest independently re-derives the R10 authorization version digest
// (domain-tagged v1, newline-separated SHA-256 lowercase hex).
func rrDigest(authorizationID string, chainID int64, asset, recipient, amount, state, expires string) string {
	parts := []string{
		"txharbor:nonce:authorization:v1",
		authorizationID,
		strconv.FormatInt(chainID, 10),
		asset,
		recipient,
		amount,
		state,
		expires,
	}
	h := sha256.New()
	for _, part := range parts {
		_, _ = h.Write([]byte(part))
		_, _ = h.Write([]byte{'\n'})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// rrAllocResult carries one admission outcome out of its goroutine.
type rrAllocResult struct {
	binding *nonce.Binding
	outcome nonce.Outcome
	err     error
}

// rrBeginRaw opens one raw SQL transaction with a bounded lock_timeout (never
// statement_timeout: the writer must be able to block, not fail on the pause).
func rrBeginRaw(t *testing.T, sqlDB *sql.DB) *sql.Tx {
	t.Helper()
	ctx := context.Background()
	tx, err := sqlDB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin raw tx: %v", err)
	}
	t.Cleanup(func() { _ = tx.Rollback() }) // no-op after an explicit Commit
	if _, err := tx.ExecContext(ctx, `SET LOCAL lock_timeout = '`+rrRawLockTimeout+`'`); err != nil {
		t.Fatalf("set raw lock_timeout: %v", err)
	}
	return tx
}

func rrCommitRaw(t *testing.T, tx *sql.Tx) {
	t.Helper()
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit raw tx: %v", err)
	}
}

// rrLockTableRaw takes ACCESS EXCLUSIVE on an 008 own-row table to park a live
// admission *after* it has taken the 007 share lock (the blocked INSERT is the
// evidence the share lock precedes 008's own-row writes).
func rrLockTableRaw(t *testing.T, sqlDB *sql.DB, table string) *sql.Tx {
	t.Helper()
	tx := rrBeginRaw(t, sqlDB)
	if _, err := tx.ExecContext(context.Background(),
		fmt.Sprintf(`LOCK TABLE %s IN ACCESS EXCLUSIVE MODE`, table)); err != nil {
		t.Fatalf("lock table %s: %v", table, err)
	}
	return tx
}

// rrRawRevoke is the simulated RevokeGrant row work: lock order SELECT … FOR
// UPDATE → UPDATE state='revoked' → audit append (grantRevokeSQL + Table 6).
func rrRawRevoke(ctx context.Context, tx *sql.Tx, authID, opID string) error {
	var found string
	if err := tx.QueryRowContext(ctx, `SELECT authorization_id FROM withdrawal_authorizations
		WHERE authorization_id = $1 FOR UPDATE`, authID).Scan(&found); err != nil {
		return fmt.Errorf("raw revoke lock: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE withdrawal_authorizations
		SET state = 'revoked' WHERE authorization_id = $1`, authID); err != nil {
		return fmt.Errorf("raw revoke update: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO withdrawal_grant_audit
		(operation_id, authorization_id, caller_id, action, operator, reason, detail)
		VALUES ($1, $2, $3, 'revoked', 'rr', 'race', '')`, opID, authID, rrCaller); err != nil {
		return fmt.Errorf("raw revoke audit: %w", err)
	}
	return nil
}

// rrRawSupply is the simulated SupplyGrant re-supply: lock order SELECT … FOR
// UPDATE → parameter UPDATE (grantUnchangedSQL shape) → audit 'resupplied'.
func rrRawSupply(ctx context.Context, tx *sql.Tx, authID, opID, amount string) error {
	var found string
	if err := tx.QueryRowContext(ctx, `SELECT authorization_id FROM withdrawal_authorizations
		WHERE authorization_id = $1 FOR UPDATE`, authID).Scan(&found); err != nil {
		return fmt.Errorf("raw supply lock: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE withdrawal_authorizations
		SET amount = $2::numeric WHERE authorization_id = $1`, authID, amount); err != nil {
		return fmt.Errorf("raw supply update: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO withdrawal_grant_audit
		(operation_id, authorization_id, caller_id, action, operator, reason, detail)
		VALUES ($1, $2, $3, 'resupplied', 'rr', 'race', '')`, opID, authID, rrCaller); err != nil {
		return fmt.Errorf("raw supply audit: %w", err)
	}
	return nil
}

// rrWaitForWaiter blocks until some other session is waiting on a lock whose
// current statement matches needle (e.g. the 008 share SELECT, the 007 lock).
func rrWaitForWaiter(t *testing.T, sqlDB *sql.DB, needle string) {
	t.Helper()
	deadline := time.Now().Add(rrWaitDeadline)
	for {
		var n int
		if err := sqlDB.QueryRowContext(context.Background(), `SELECT count(*) FROM pg_stat_activity
			WHERE pid <> pg_backend_pid() AND wait_event_type = 'Lock'
			  AND position($1 in query) > 0`, needle).Scan(&n); err != nil {
			t.Fatalf("poll pg_stat_activity for %q: %v", needle, err)
		}
		if n > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("no session waited on a lock matching %q within %s", needle, rrWaitDeadline)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// rrWaitForPendingWrite blocks until a session is waiting to write an 008
// own-row table, proving a live admission already holds the 007 share lock.
func rrWaitForPendingWrite(t *testing.T, sqlDB *sql.DB) {
	t.Helper()
	deadline := time.Now().Add(rrWaitDeadline)
	for {
		var n int
		if err := sqlDB.QueryRowContext(context.Background(), `SELECT count(*) FROM pg_locks l
			JOIN pg_class c ON c.oid = l.relation
			WHERE c.relname = 'nonce_observations' AND NOT l.granted`).Scan(&n); err != nil {
			t.Fatalf("poll pending observation write: %v", err)
		}
		if n > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("admission never reached an own-row write within %s", rrWaitDeadline)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// rrWantHeldAlloc asserts an admission has not returned while it should block.
func rrWantHeldAlloc(t *testing.T, done <-chan rrAllocResult) {
	t.Helper()
	select {
	case res := <-done:
		t.Fatalf("admission returned while it should have blocked: outcome=%q err=%v", res.outcome, res.err)
	case <-time.After(rrHoldWindow):
	}
}

// rrWantHeldRaw asserts the simulated 007 writer has not committed while the
// admission holds the share lock.
func rrWantHeldRaw(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		t.Fatalf("007 writer committed while the admission held the share lock: %v", err)
	case <-time.After(rrHoldWindow):
	}
}

func rrWaitAlloc(t *testing.T, done <-chan rrAllocResult) rrAllocResult {
	t.Helper()
	select {
	case res := <-done:
		return res
	case <-time.After(rrDeadline):
		t.Fatal("admission did not complete within the deadline")
		return rrAllocResult{}
	}
}

func rrWaitRaw(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("007 writer failed: %v", err)
		}
	case <-time.After(rrDeadline):
		t.Fatal("007 writer did not complete within the deadline")
	}
}

func rrAllocateAsync(ctx context.Context, alloc *nonce.Allocator, req nonce.AllocationRequest) <-chan rrAllocResult {
	done := make(chan rrAllocResult, 1)
	go func() {
		binding, outcome, err := alloc.Allocate(ctx, req)
		done <- rrAllocResult{binding: binding, outcome: outcome, err: err}
	}()
	return done
}

// TestNonceAuthzRevokeRaceIntegration is the T044 V11 acceptance over one
// migrated scratch database. Real PostgreSQL; no Anvil (the chain view is
// scripted).
func TestNonceAuthzRevokeRaceIntegration(t *testing.T) {
	dsn := nonceStartPostgres(t)
	ctx := context.Background()

	var out bytes.Buffer
	if err := db.MigrateUp(ctx, nonceMigrateOptions(dsn), &out); err != nil {
		t.Fatalf("MigrateUp() error = %v (output %q)", err, out.String())
	}
	sqlDB := nonceOpenSQL(t, dsn)
	rrSeed(t, sqlDB, rrSenderRevoke, rrSenderSupply, rrSenderShareRevoke, rrSenderShareSupply, rrSenderPure)
	rrSeedAuth(t, sqlDB, rrAuthRevoke, "active")
	rrSeedAuth(t, sqlDB, rrAuthSupply, "active")
	rrSeedAuth(t, sqlDB, rrAuthShareRevoke, "active")
	rrSeedAuth(t, sqlDB, rrAuthShareSupply, "active")
	rrSeedAuth(t, sqlDB, rrAuthPure, "active")

	pool := rrOpenPool(t, dsn)
	alloc := rrAllocator(pool)

	// --- baseline: a plain 008 admission writes zero 006/007 rows ----------
	t.Run("pure_admission_leaves_007_byte_identical", func(t *testing.T) {
		before := rrSnapshot(t, sqlDB)
		binding, outcome, err := alloc.Allocate(ctx, nonce.AllocationRequest{
			IntentID: "rr-intent-pure", ChainID: rrChain, Sender: rrSenderPure, AuthorizationID: rrAuthPure,
		})
		if err != nil || outcome != nonce.OutcomeAllocated || binding == nil {
			t.Fatalf("pure allocate = (%+v, %q, %v), want allocated", binding, outcome, err)
		}
		rrWantTablesUnchanged(t, before, rrSnapshot(t, sqlDB))
	})

	// --- revoke commits before the admission observes the share lock -------
	t.Run("revoke_commits_first_refuses", func(t *testing.T) {
		before := rrSnapshot(t, sqlDB)
		authBefore := rrAuthExceptBlob(t, sqlDB, rrAuthRevoke)
		auditBefore := rrCount(t, sqlDB, "withdrawal_grant_audit")
		censusBefore := rrCensusOf(t, sqlDB)

		raw := rrBeginRaw(t, sqlDB)
		if err := rrRawRevoke(ctx, raw, rrAuthRevoke, "rr-op-revoke-first"); err != nil {
			t.Fatalf("raw revoke: %v", err)
		}
		done := rrAllocateAsync(ctx, alloc, nonce.AllocationRequest{
			IntentID: "rr-intent-revoke", ChainID: rrChain, Sender: rrSenderRevoke, AuthorizationID: rrAuthRevoke,
		})
		rrWaitForWaiter(t, sqlDB, "FOR SHARE") // the admission is blocked on the revoked row
		rrWantHeldAlloc(t, done)
		rrCommitRaw(t, raw) // the revoke lands while the admission waits for the share lock

		res := rrWaitAlloc(t, done)
		if res.binding != nil || res.outcome != nonce.OutcomeAuthorizationInvalid ||
			!nonce.IsOutcome(res.err, nonce.OutcomeAuthorizationInvalid) {
			t.Fatalf("revoke-first admission = (%+v, %q, %v), want authorization_invalid with no binding",
				res.binding, res.outcome, res.err)
		}
		if got := rrCensusOf(t, sqlDB); got != censusBefore {
			t.Fatalf("refused admission wrote 008 rows: %+v, want %+v", got, censusBefore)
		}
		state, amount := rrAuthState(t, sqlDB, rrAuthRevoke)
		if state != "revoked" || amount != rrSeededAmount {
			t.Fatalf("raced authorization = state %q amount %q, want revoked/%s", state, amount, rrSeededAmount)
		}
		if got := rrAuthExceptBlob(t, sqlDB, rrAuthRevoke); got != authBefore {
			t.Fatal("admission or revoke mutated a non-raced authorization row")
		}
		if got := rrCount(t, sqlDB, "withdrawal_grant_audit"); got != auditBefore+1 {
			t.Fatalf("grant audit rows = %d, want %d (only the simulated 007 writer appends)", got, auditBefore+1)
		}
		rrWantTablesUnchanged(t, before, rrSnapshot(t, sqlDB), "withdrawal_authorizations", "withdrawal_grant_audit")
	})

	// --- supply (re-supply) commits before the share lock: fresh version ----
	t.Run("supply_commits_first_binds_fresh_version", func(t *testing.T) {
		before := rrSnapshot(t, sqlDB)
		authBefore := rrAuthExceptBlob(t, sqlDB, rrAuthSupply)
		auditBefore := rrCount(t, sqlDB, "withdrawal_grant_audit")

		raw := rrBeginRaw(t, sqlDB)
		if err := rrRawSupply(ctx, raw, rrAuthSupply, "rr-op-supply-first", rrSupplyAmount); err != nil {
			t.Fatalf("raw supply: %v", err)
		}
		done := rrAllocateAsync(ctx, alloc, nonce.AllocationRequest{
			IntentID: "rr-intent-supply", ChainID: rrChain, Sender: rrSenderSupply, AuthorizationID: rrAuthSupply,
		})
		rrWaitForWaiter(t, sqlDB, "FOR SHARE")
		rrWantHeldAlloc(t, done)
		rrCommitRaw(t, raw)

		res := rrWaitAlloc(t, done)
		if res.err != nil || res.outcome != nonce.OutcomeAllocated || res.binding == nil {
			t.Fatalf("supply-first admission = (%+v, %q, %v), want allocated", res.binding, res.outcome, res.err)
		}
		wantVersion := rrDigest(rrAuthSupply, rrChain, rrAsset, rrRecipient, rrSupplyAmount, "active", "")
		if res.binding.AuthorizationVersion != wantVersion {
			t.Fatalf("binding version = %q, want the freshly supplied %q",
				res.binding.AuthorizationVersion, wantVersion)
		}
		state, amount := rrAuthState(t, sqlDB, rrAuthSupply)
		if state != "active" || amount != rrSupplyAmount {
			t.Fatalf("raced authorization = state %q amount %q, want active/%s", state, amount, rrSupplyAmount)
		}
		if got := rrAuthExceptBlob(t, sqlDB, rrAuthSupply); got != authBefore {
			t.Fatal("admission or supply mutated a non-raced authorization row")
		}
		if got := rrCount(t, sqlDB, "withdrawal_grant_audit"); got != auditBefore+1 {
			t.Fatalf("grant audit rows = %d, want %d", got, auditBefore+1)
		}
		rrWantTablesUnchanged(t, before, rrSnapshot(t, sqlDB), "withdrawal_authorizations", "withdrawal_grant_audit")
	})

	// --- admission holds the share lock first; a revoke waits for it -------
	t.Run("admission_share_lock_first_revoke_waits", func(t *testing.T) {
		before := rrSnapshot(t, sqlDB)
		authBefore := rrAuthExceptBlob(t, sqlDB, rrAuthShareRevoke)
		auditBefore := rrCount(t, sqlDB, "withdrawal_grant_audit")

		park := rrLockTableRaw(t, sqlDB, "nonce_observations")
		done := rrAllocateAsync(ctx, alloc, nonce.AllocationRequest{
			IntentID: "rr-intent-share-revoke", ChainID: rrChain, Sender: rrSenderShareRevoke, AuthorizationID: rrAuthShareRevoke,
		})
		rrWaitForPendingWrite(t, sqlDB) // the admission holds the 007 share lock

		rawDone := make(chan error, 1)
		go func() {
			tx, err := sqlDB.BeginTx(ctx, nil)
			if err != nil {
				rawDone <- err
				return
			}
			defer func() { _ = tx.Rollback() }()
			if _, err := tx.ExecContext(ctx, `SET LOCAL lock_timeout = '`+rrRawLockTimeout+`'`); err != nil {
				rawDone <- err
				return
			}
			if err := rrRawRevoke(ctx, tx, rrAuthShareRevoke, "rr-op-share-revoke"); err != nil {
				rawDone <- err
				return
			}
			rawDone <- tx.Commit()
		}()
		rrWaitForWaiter(t, sqlDB, "FOR UPDATE") // the revoke is blocked behind the share lock
		rrWantHeldRaw(t, rawDone)

		rrCommitRaw(t, park) // release the admission: it commits first
		res := rrWaitAlloc(t, done)
		if res.err != nil || res.outcome != nonce.OutcomeAllocated || res.binding == nil {
			t.Fatalf("share-first admission = (%+v, %q, %v), want allocated", res.binding, res.outcome, res.err)
		}
		wantVersion := rrDigest(rrAuthShareRevoke, rrChain, rrAsset, rrRecipient, rrSeededAmount, "active", "")
		if res.binding.AuthorizationVersion != wantVersion {
			t.Fatalf("binding version = %q, want the pre-revoke active %q",
				res.binding.AuthorizationVersion, wantVersion)
		}
		rrWaitRaw(t, rawDone) // the revoke proceeds only after the admission committed

		if state, amount := rrAuthState(t, sqlDB, rrAuthShareRevoke); state != "revoked" || amount != rrSeededAmount {
			t.Fatalf("post-race authorization = state %q amount %q, want revoked/%s", state, amount, rrSeededAmount)
		}
		if got := rrAuthExceptBlob(t, sqlDB, rrAuthShareRevoke); got != authBefore {
			t.Fatal("admission or revoke mutated a non-raced authorization row")
		}
		if got := rrCount(t, sqlDB, "withdrawal_grant_audit"); got != auditBefore+1 {
			t.Fatalf("grant audit rows = %d, want %d", got, auditBefore+1)
		}
		rrWantTablesUnchanged(t, before, rrSnapshot(t, sqlDB), "withdrawal_authorizations", "withdrawal_grant_audit")
	})

	// --- admission holds the share lock first; a supply waits for it -------
	t.Run("admission_share_lock_first_supply_waits", func(t *testing.T) {
		before := rrSnapshot(t, sqlDB)
		authBefore := rrAuthExceptBlob(t, sqlDB, rrAuthShareSupply)
		auditBefore := rrCount(t, sqlDB, "withdrawal_grant_audit")

		park := rrLockTableRaw(t, sqlDB, "nonce_observations")
		done := rrAllocateAsync(ctx, alloc, nonce.AllocationRequest{
			IntentID: "rr-intent-share-supply", ChainID: rrChain, Sender: rrSenderShareSupply, AuthorizationID: rrAuthShareSupply,
		})
		rrWaitForPendingWrite(t, sqlDB)

		rawDone := make(chan error, 1)
		go func() {
			tx, err := sqlDB.BeginTx(ctx, nil)
			if err != nil {
				rawDone <- err
				return
			}
			defer func() { _ = tx.Rollback() }()
			if _, err := tx.ExecContext(ctx, `SET LOCAL lock_timeout = '`+rrRawLockTimeout+`'`); err != nil {
				rawDone <- err
				return
			}
			if err := rrRawSupply(ctx, tx, rrAuthShareSupply, "rr-op-share-supply", rrSupplyAmount); err != nil {
				rawDone <- err
				return
			}
			rawDone <- tx.Commit()
		}()
		rrWaitForWaiter(t, sqlDB, "FOR UPDATE")
		rrWantHeldRaw(t, rawDone)

		rrCommitRaw(t, park)
		res := rrWaitAlloc(t, done)
		if res.err != nil || res.outcome != nonce.OutcomeAllocated || res.binding == nil {
			t.Fatalf("share-first admission = (%+v, %q, %v), want allocated", res.binding, res.outcome, res.err)
		}
		wantVersion := rrDigest(rrAuthShareSupply, rrChain, rrAsset, rrRecipient, rrSeededAmount, "active", "")
		if res.binding.AuthorizationVersion != wantVersion {
			t.Fatalf("binding version = %q, want the pre-supply active %q",
				res.binding.AuthorizationVersion, wantVersion)
		}
		rrWaitRaw(t, rawDone)

		if state, amount := rrAuthState(t, sqlDB, rrAuthShareSupply); state != "active" || amount != rrSupplyAmount {
			t.Fatalf("post-race authorization = state %q amount %q, want active/%s", state, amount, rrSupplyAmount)
		}
		if got := rrAuthExceptBlob(t, sqlDB, rrAuthShareSupply); got != authBefore {
			t.Fatal("admission or supply mutated a non-raced authorization row")
		}
		if got := rrCount(t, sqlDB, "withdrawal_grant_audit"); got != auditBefore+1 {
			t.Fatalf("grant audit rows = %d, want %d", got, auditBefore+1)
		}
		rrWantTablesUnchanged(t, before, rrSnapshot(t, sqlDB), "withdrawal_authorizations", "withdrawal_grant_audit")
	})
}
