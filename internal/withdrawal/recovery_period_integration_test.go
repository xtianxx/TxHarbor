//go:build integration

// recovery_period_integration_test.go owns spec task T027 (US6, FR-16/FR-17,
// SC-06/SC-07, V8): receive-during-recovery on a real PostgreSQL 18 container
// (testcontainers). T028 (recovery-snapshot query tests) APPENDS its cases to
// THIS file after T027 lands, reusing the recoveryPeriod* helpers below — T027
// owns the file, T028 must not redefine them.
//
// Anvil decision (documented, not silently skipped): 007 makes ZERO RPC calls.
// A 007 create persists a request row (007 DDL) plus a recovery signal read
// from 006 rows/keyed events — all PostgreSQL. There is no chain read, no
// nonce, no signature, no broadcast, and no height-indexed validity call
// (contracts/api.md §3/§4). V8's evidence is therefore a PostgreSQL fact plus
// an ABSENCE: no rows outside 007 scope change, no execution-artefact table
// exists or is created, and the withdrawal package's production files carry no
// RPC send path. None of that is observable on a chain, so a Foundry/Anvil
// harness — which the 006 indexer wave legitimately uses via
// logscanStartAnvilNode / rrecSetup in internal/indexer — would add container
// cost without adding evidence. PG-only is the honest carrier here.
//
// Receiving during an active 006 recovery is allowed and receive-only (FR-16):
// a compliant POST persists 201, and a later owner GET serves the request facts
// with recovery.state derived from the active row and execution constant
// not_started. 007 never writes 006 governance (pauses, de-authorization,
// isolation): the zero-side-effect proof compares 002–006 table snapshots
// before/after the create.
//
// It reuses the T006 container/migration helper (grantSetup), the T008 grant
// fixtures (intakeSupplyGrant et al. built on grantTestOp), and the T010 intake
// fixtures (intakeKey, intakeReq, intakeRequestCount, intakeAuditActions), and
// adds recoveryPeriod-prefixed helpers so no name collides with a sibling test
// file. Production code is untouched.
package withdrawal

import (
	"context"
	"io"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/db"
)

// recoveryPeriodScope006Tables are the 002–006-owned tables 007 must never
// write (the V8 absence surface). 007 reads reorg_recovery /
// reorg_policy_history / reorg_recovery_events through the exported 006 readers
// (read-only reuse in query.go) and writes none of them.
var recoveryPeriodScope006Tables = []string{
	"chain_blocks",
	"indexer_checkpoint",
	"indexer_lease",
	"indexer_pause",
	"erc20_transfer_logs",
	"log_checkpoint",
	"log_pause",
	"deposit_config_history",
	"deposit_observations",
	"deposit_checkpoint",
	"deposit_pause",
	"deposit_pause_audit",
	"confirmation_policy_history",
	"reorg_policy_history",
	"reorg_recovery",
	"reorg_recovery_events",
	"deposit_observation_transitions",
}

// recoveryPeriodScope007Tables are the only tables 007 may write: credentials
// (caller/api_key on issue), the grant supply read (withdrawal_authorizations)
// and the receipt/audit/grant-audit log.
var recoveryPeriodScope007Tables = []string{
	"caller",
	"api_key",
	"withdrawal_requests",
	"withdrawal_authorizations",
	"withdrawal_request_audit",
	"withdrawal_grant_audit",
}

// recoveryPeriodCounts returns the row count of each named table. Names are
// internal constants (never user input), so the interpolation is safe.
func recoveryPeriodCounts(t *testing.T, ctx context.Context, pool *pgxpool.Pool, tables []string) map[string]int {
	t.Helper()
	counts := make(map[string]int, len(tables))
	for _, table := range tables {
		var n int
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&n); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		counts[table] = n
	}
	return counts
}

// recoveryPeriodAssertCountsEqual proves no named table's row count moved.
func recoveryPeriodAssertCountsEqual(t *testing.T, before, after map[string]int, tables []string) {
	t.Helper()
	for _, table := range tables {
		if before[table] != after[table] {
			t.Fatalf("%s rows changed: %d -> %d (007 wrote outside its scope)", table, before[table], after[table])
		}
	}
}

// recoveryPeriodAssertDelta proves a named table's count moved by exactly want.
func recoveryPeriodAssertDelta(t *testing.T, before, after map[string]int, table string, want int) {
	t.Helper()
	if got := after[table] - before[table]; got != want {
		t.Fatalf("%s delta = %d, want %d", table, got, want)
	}
}

// recoveryPeriodSeedPolicy inserts one reorg_policy_history version (the FK
// target a recovery row must reference). Generic for T028 reuse.
func recoveryPeriodSeedPolicy(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID, policySeq, maxDepth int64) {
	t.Helper()
	if _, err := pool.Exec(ctx, `INSERT INTO reorg_policy_history
		(chain_id, policy_seq, max_depth, operator) VALUES ($1, $2, $3, 'bootstrap')`,
		chainID, policySeq, maxDepth); err != nil {
		t.Fatalf("seed reorg policy (chain %d seq %d): %v", chainID, policySeq, err)
	}
}

// recoveryPeriodSeedActiveRecovery seeds one ACTIVE 006 reorg_recovery row, with
// the policy version its FK needs, for chainID — the "recovery active"
// precondition every T027/T028 case shares. It deliberately mirrors 006's
// detection shape (policy_seq 1 / max_depth 64 / bound height 100) rather than
// inventing a height.
func recoveryPeriodSeedActiveRecovery(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, recoveryID, phase string) {
	t.Helper()
	recoveryPeriodSeedPolicy(t, ctx, pool, chainID, 1, 64)
	if _, err := pool.Exec(ctx, `INSERT INTO reorg_recovery
		(chain_id, recovery_id, phase, policy_seq, max_depth, bound_old_number, bound_old_hash, recovery_seq)
		VALUES ($1, $2, $3, 1, 64, 100, $4, 1)`,
		chainID, recoveryID, phase, withdrawalHash("aa")); err != nil {
		t.Fatalf("seed active recovery (chain %d phase %s): %v", chainID, phase, err)
	}
}

// recoveryPeriodRecoveryRow reads back the active recovery row's
// (phase, recovery_seq) so a case can prove 007 left it untouched.
func recoveryPeriodRecoveryRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64) (string, int64) {
	t.Helper()
	var phase string
	var seq int64
	if err := pool.QueryRow(ctx,
		`SELECT phase, recovery_seq FROM reorg_recovery WHERE chain_id = $1`, chainID).Scan(&phase, &seq); err != nil {
		t.Fatalf("read active recovery row for chain %d: %v", chainID, err)
	}
	return phase, seq
}

// recoveryPeriodEventCount returns the 006 recovery-event log size, so a case
// can prove 007 appended no terminal/replay event.
func recoveryPeriodEventCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM reorg_recovery_events`).Scan(&n); err != nil {
		t.Fatalf("count reorg_recovery_events: %v", err)
	}
	return n
}

// recoveryPeriodArtifactTableShape matches a table name that would betray a 007
// execution artefact (nonce / signature / broadcast / execution / payout /
// settlement). No current table matches.
var recoveryPeriodArtifactTableShape = regexp.MustCompile(`(?i)nonce|broadcast|sign|execution|payout|settlement`)

// recoveryPeriodAssertNoExecutionArtifactTables proves the schema carries no
// table that could hold a nonce/signature/broadcast — so none can be created by
// a 007 path.
func recoveryPeriodAssertNoExecutionArtifactTables(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	rows, err := pool.Query(ctx, `SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'`)
	if err != nil {
		t.Fatalf("list public tables: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan table name: %v", err)
		}
		if recoveryPeriodArtifactTableShape.MatchString(name) {
			t.Fatalf("execution-artefact table %q exists; 007 has no nonce/signer/broadcast surface", name)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate public tables: %v", err)
	}
}

// recoveryPeriodForbiddenImports are the RPC/signing packages no 007 production
// file may import: importing one is the minimum structural precondition for a
// broadcast. Read-only 006 reuse goes through internal/indexer's exported
// readers, which must not leak these into the withdrawal package's own files.
var recoveryPeriodForbiddenImports = []string{
	`"github.com/ethereum/go-ethereum/ethclient"`,
	`"github.com/ethereum/go-ethereum/rpc"`,
	`"github.com/xtianxx/txharbor/internal/eth"`,
}

// recoveryPeriodForbiddenSendCalls are send/sign identifiers that must not
// appear in a 007 production file. Case-sensitive on purpose: prose that says a
// path "never allocates nonces, signs, or broadcasts" (auth.go's header) is not
// a call.
var recoveryPeriodForbiddenSendCalls = []string{
	"SendTransaction",
	"SendRawTransaction",
	"eth_sendRawTransaction",
	"SignTx",
}

// recoveryPeriodAssertNoRPCSendPath scans this package's non-test source files
// (Go runs the test with the package directory as cwd) for an RPC client import
// or a send/sign call. It fails loudly if zero files were scanned, so the
// absence assertion can never pass vacuously.
func recoveryPeriodAssertNoRPCSendPath(t *testing.T) {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}
	scanned := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		data, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		src := string(data)
		for _, imp := range recoveryPeriodForbiddenImports {
			if strings.Contains(src, imp) {
				t.Fatalf("%s imports %s; 007 must not carry an RPC client", name, imp)
			}
		}
		for _, call := range recoveryPeriodForbiddenSendCalls {
			if strings.Contains(src, call) {
				t.Fatalf("%s references %s; 007 must have no send/broadcast path", name, call)
			}
		}
		scanned++
	}
	if scanned == 0 {
		t.Fatal("scanned zero production files; the RPC-absence assertion would be vacuous")
	}
}

// TestWithdrawalRecoveryPeriodIntakePersistsAndServes covers FR-16's
// receive-during-recovery core (V8): with an ACTIVE 006 recovery row on the
// request's chain, a compliant create still persists 201 with no pre-tx audit
// intent, and a later owner GET serves the request facts with
// recovery.state=recovering and execution=not_started — a same-key retry stays
// 200 with the original request_id and still writes nothing to 006.
func TestWithdrawalRecoveryPeriodIntakePersistsAndServes(t *testing.T) {
	// Given a migrated database, a caller with an active grant, and an active
	// 006 recovery instance on the request's chain (phase detected).
	ctx, pool := grantSetup(t)
	const (
		callerID   = int64(7401)
		authID     = "auth-recovery-period-intake"
		recoveryID = "rec-recovery-period-intake"
	)
	key := intakeKey(t, ctx, pool, callerID)
	intakeSupplyGrant(t, ctx, pool, callerID, authID, intakeAmount)
	recoveryPeriodSeedActiveRecovery(t, ctx, pool, intakeChainID, recoveryID, "detected")

	// When the compliant create lands during the active recovery.
	first, err := SubmitWithdrawal(ctx, pool, intakeReq(key, "idem-recovery-period", authID))
	if err != nil {
		t.Fatalf("submit during active recovery: %v", err)
	}

	// Then it is accepted and persisted with no pre-tx audit intent.
	if first.Status != 201 {
		t.Fatalf("status = %d (%+v), want 201", first.Status, first)
	}
	intakeWantNoAudit(t, first)
	if n := intakeRequestCount(t, ctx, pool); n != 1 {
		t.Fatalf("request rows = %d, want 1", n)
	}
	intakeWantActions(t, intakeAuditActions(t, ctx, pool), []string{auditActionCreated})

	// And the owner's later GET serves the request with the active-recovery
	// signal: recovering, never a claimed execution outcome.
	view, err := GetWithdrawal(ctx, pool, callerID, first.RequestID)
	if err != nil {
		t.Fatalf("GetWithdrawal during recovery: %v", err)
	}
	if view.Recovery.State != "recovering" || view.Recovery.Execution != "not_started" {
		t.Fatalf("recovery = %+v, want {recovering not_started}", view.Recovery)
	}
	if view.Status != "accepted" || view.Amount != intakeAmount {
		t.Fatalf("view = %+v, want accepted/%s", view, intakeAmount)
	}

	// And the seeded recovery row is untouched: same phase/seq, no event rows.
	if phase, seq := recoveryPeriodRecoveryRow(t, ctx, pool, intakeChainID); phase != "detected" || seq != 1 {
		t.Fatalf("recovery row after intake = (%s, %d), want (detected, 1)", phase, seq)
	}
	if n := recoveryPeriodEventCount(t, ctx, pool); n != 0 {
		t.Fatalf("recovery event rows = %d, want 0 (007 wrote no 006 event)", n)
	}

	// When the same key + params replays (a lost 201 response retried), the
	// recovery-period replay path must stay a 200 with the original id.
	replay, err := SubmitWithdrawal(ctx, pool, intakeReq(key, "idem-recovery-period", authID))
	if err != nil {
		t.Fatalf("replay during recovery: %v", err)
	}
	if replay.Status != 200 || replay.RequestID != first.RequestID {
		t.Fatalf("replay = %+v, want 200 with request_id %q", replay, first.RequestID)
	}
	intakeWantIntent(t, replay, auditActionReplayed, callerID, first.RequestID)
	if n := intakeRequestCount(t, ctx, pool); n != 1 {
		t.Fatalf("request rows after replay = %d, want 1", n)
	}
	intakeWantActions(t, intakeAuditActions(t, ctx, pool), []string{auditActionCreated})
}

// TestWithdrawalRecoveryPeriodPausedReconcileServed covers the other active
// phase mapping (V8): a reconcile_required recovery still accepts a compliant
// create and serves recovery.state=paused_reconcile — the paused signal is
// surfaced, never silently dropped, and still claims no execution.
func TestWithdrawalRecoveryPeriodPausedReconcileServed(t *testing.T) {
	// Given an active recovery in the reconcile_required (paused) phase.
	ctx, pool := grantSetup(t)
	const (
		callerID = int64(7402)
		authID   = "auth-recovery-period-paused"
	)
	key := intakeKey(t, ctx, pool, callerID)
	intakeSupplyGrant(t, ctx, pool, callerID, authID, intakeAmount)
	recoveryPeriodSeedActiveRecovery(t, ctx, pool, intakeChainID, "rec-recovery-period-paused", "reconcile_required")

	// When the create lands while the chain is paused for reconciliation.
	res, err := SubmitWithdrawal(ctx, pool, intakeReq(key, "idem-recovery-paused", authID))
	if err != nil || res.Status != 201 {
		t.Fatalf("submit during paused_reconcile = (%+v, %v), want 201", res, err)
	}

	// Then the request is servable and the snapshot state is paused_reconcile.
	view, err := GetWithdrawal(ctx, pool, callerID, res.RequestID)
	if err != nil {
		t.Fatalf("GetWithdrawal during paused_reconcile: %v", err)
	}
	if view.Recovery.State != "paused_reconcile" || view.Recovery.Execution != "not_started" {
		t.Fatalf("recovery = %+v, want {paused_reconcile not_started}", view.Recovery)
	}
}

// TestWithdrawalRecoveryPeriodZeroSideEffectsOutsideScope is the structural
// half of V8: a create during active recovery changes ONLY the two 007 rows it
// owns. Every 002–006 table (including the seeded recovery row, its policy, and
// every pause table) has the same row count before and after.
func TestWithdrawalRecoveryPeriodZeroSideEffectsOutsideScope(t *testing.T) {
	// Given a seeded active recovery and a snapshot of every table count taken
	// AFTER the fixtures are in place, so the create is the only variable.
	ctx, pool := grantSetup(t)
	const (
		callerID = int64(7403)
		authID   = "auth-recovery-period-scope"
	)
	key := intakeKey(t, ctx, pool, callerID)
	intakeSupplyGrant(t, ctx, pool, callerID, authID, intakeAmount)
	recoveryPeriodSeedActiveRecovery(t, ctx, pool, intakeChainID, "rec-recovery-period-scope", "detected")
	before006 := recoveryPeriodCounts(t, ctx, pool, recoveryPeriodScope006Tables)
	before007 := recoveryPeriodCounts(t, ctx, pool, recoveryPeriodScope007Tables)

	// When a compliant create lands during the active recovery.
	res, err := SubmitWithdrawal(ctx, pool, intakeReq(key, "idem-recovery-scope", authID))
	if err != nil || res.Status != 201 {
		t.Fatalf("submit during recovery = (%+v, %v), want 201", res, err)
	}

	// Then no 002–006 row count moved: no pause, no de-authorization, no
	// isolation, no recovery-event, no chain write of any kind.
	after006 := recoveryPeriodCounts(t, ctx, pool, recoveryPeriodScope006Tables)
	recoveryPeriodAssertCountsEqual(t, before006, after006, recoveryPeriodScope006Tables)

	// And exactly the two 007 rows the path owns changed: the request and its
	// in-tx `created` audit row. Grant/credential fixtures were pre-seeded, so
	// they too are unchanged.
	after007 := recoveryPeriodCounts(t, ctx, pool, recoveryPeriodScope007Tables)
	recoveryPeriodAssertDelta(t, before007, after007, "withdrawal_requests", 1)
	recoveryPeriodAssertDelta(t, before007, after007, "withdrawal_request_audit", 1)
	for _, table := range []string{"caller", "api_key", "withdrawal_authorizations", "withdrawal_grant_audit"} {
		recoveryPeriodAssertDelta(t, before007, after007, table, 0)
	}

	// And the active recovery row itself is the one 006 row we seeded — still
	// present, same phase, with no terminal event.
	if phase, seq := recoveryPeriodRecoveryRow(t, ctx, pool, intakeChainID); phase != "detected" || seq != 1 {
		t.Fatalf("recovery row = (%s, %d), want (detected, 1)", phase, seq)
	}
	if n := recoveryPeriodEventCount(t, ctx, pool); n != 0 {
		t.Fatalf("recovery event rows = %d, want 0", n)
	}
}

// recoveryPeriodPublicBaseTables lists public BASE TABLE names, the set a
// schema-shape assertion compares.
func recoveryPeriodPublicBaseTables(t *testing.T, ctx context.Context, pool *pgxpool.Pool) map[string]struct{} {
	t.Helper()
	rows, err := pool.Query(ctx, `SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE'`)
	if err != nil {
		t.Fatalf("list public base tables: %v", err)
	}
	defer rows.Close()
	out := map[string]struct{}{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan table name: %v", err)
		}
		out[name] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate public tables: %v", err)
	}
	return out
}

// recoveryPeriodTableCounts snapshots row counts for tables.
func recoveryPeriodTableCounts(t *testing.T, ctx context.Context, pool *pgxpool.Pool, tables []string) map[string]int64 {
	t.Helper()
	out := make(map[string]int64, len(tables))
	for _, tbl := range tables {
		var n int64
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM "`+tbl+`"`).Scan(&n); err != nil {
			t.Fatalf("count %s: %v", tbl, err)
		}
		out[tbl] = n
	}
	return out
}

// TestWithdrawalRecoveryPeriodNoExecutionArtefacts is the behavioural + code
// half of V8's "no RPC broadcast": 007 has no execution surface at all. Phase
// A keeps the original 007-range guarantee bit-for-bit — a database migrated
// with only 000001–000007 carries no nonce/signature/broadcast table, so a
// create creates none. Phase B extends it to the full embedded chain: every
// table introduced above 007 must hold exactly the same row count before and
// after a recovery-period create, i.e. the recovery path writes no execution
// record or state anywhere outside 007 scope. At the code layer no production
// file of this package imports an RPC client or references a send/sign call.
// 007's only recovery interaction is the read-only 006 reader reuse in
// query.go.
//
// 013 adjustment (B3/T029, documented, not a weakened guarantee):
// outbox_events is the one above-007 table allowed to change, because the
// approved 013 contract commits the receive fact
// (withdrawal.request.received) in the SAME receipt transaction (FR-07;
// contracts/events.md §3). The delta is pinned to exactly that one fact for
// the created request; every 008–011 execution-state table and every other
// 013 table (consumer_*/event_ops_audit/event_system_state) must stay
// bit-identical, which is the execution-artefact guarantee this test owns.
func TestWithdrawalRecoveryPeriodNoExecutionArtefacts(t *testing.T) {
	// Phase A: 007 historical range — the original absence proof, unchanged.
	dsnA := withdrawalStartPostgres(t)
	subset := withdrawalSubsetFS(t, 7)
	subsetOpts := withdrawalMigrateOptions(dsnA)
	subsetOpts.FS = subset
	if err := db.MigrateUp(context.Background(), subsetOpts, io.Discard); err != nil {
		t.Fatalf("migrate subset through 7: %v", err)
	}
	poolA, err := db.OpenPool(context.Background(), dsnA, 5*time.Second)
	if err != nil {
		t.Fatalf("open subset pool: %v", err)
	}
	t.Cleanup(poolA.Close)
	recoveryPeriodAssertNoExecutionArtifactTables(t, context.Background(), poolA)
	range007 := recoveryPeriodPublicBaseTables(t, context.Background(), poolA)

	// Phase B: full embedded chain — a compliant create during an active
	// recovery leaves every above-007 table exactly as it was, and creates
	// no new table. The lane schema is legitimately deployed (so an
	// absence assertion on the full chain would be false); the guarantee
	// under test is no execution side effect during recovery. Scoped to
	// THIS path only, not a universal zero-side-effect proof (see
	// TestWithdrawalRecoveryPeriodZeroSideEffectsOutsideScope for the
	// 002–006 snapshot + recovery phase/seq + event count).
	ctx, pool := grantSetup(t)
	const (
		callerID = int64(7404)
		authID   = "auth-recovery-period-artefacts"
	)
	key := intakeKey(t, ctx, pool, callerID)
	intakeSupplyGrant(t, ctx, pool, callerID, authID, intakeAmount)
	recoveryPeriodSeedActiveRecovery(t, ctx, pool, intakeChainID, "rec-recovery-period-artefacts", "detected")
	beforeTables := recoveryPeriodPublicBaseTables(t, ctx, pool)
	var above007 []string
	for tbl := range beforeTables {
		if _, ok := range007[tbl]; !ok {
			above007 = append(above007, tbl)
		}
	}
	// Every above-007 table except outbox_events must stay bit-identical; the
	// 013 outbox may gain exactly the receive fact below (see the doc note).
	var executionStateTables []string
	for _, tbl := range above007 {
		if tbl != "outbox_events" {
			executionStateTables = append(executionStateTables, tbl)
		}
	}
	var outboxBefore int64
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events`).Scan(&outboxBefore); err != nil {
		t.Fatalf("count outbox before: %v", err)
	}
	before := recoveryPeriodTableCounts(t, ctx, pool, executionStateTables)
	res, err := SubmitWithdrawal(ctx, pool, intakeReq(key, "idem-recovery-artefacts", authID))
	if err != nil || res.Status != 201 {
		t.Fatalf("submit = (%+v, %v), want 201", res, err)
	}
	after := recoveryPeriodTableCounts(t, ctx, pool, executionStateTables)
	for _, tbl := range executionStateTables {
		if after[tbl] != before[tbl] {
			t.Fatalf("above-007 table %s rows changed %d -> %d during recovery create; recovery path must write no execution state", tbl, before[tbl], after[tbl])
		}
	}
	// The 013 delta is exactly the receive fact of the created request.
	var outboxAfter int64
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events`).Scan(&outboxAfter); err != nil {
		t.Fatalf("count outbox after: %v", err)
	}
	if outboxAfter != outboxBefore+1 {
		t.Fatalf("outbox rows changed %d -> %d during recovery create, want exactly +1 (the receive fact)", outboxBefore, outboxAfter)
	}
	var receiveFacts int64
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events
		WHERE event_type = 'withdrawal.request.received' AND aggregate_id = $1`, res.RequestID).Scan(&receiveFacts); err != nil {
		t.Fatalf("count receive facts: %v", err)
	}
	if receiveFacts != 1 {
		t.Fatalf("receive facts for %s = %d, want exactly 1", res.RequestID, receiveFacts)
	}
	afterTables := recoveryPeriodPublicBaseTables(t, ctx, pool)
	for tbl := range afterTables {
		if _, ok := beforeTables[tbl]; !ok {
			t.Fatalf("recovery create created table %q; recovery path must create no execution artefact", tbl)
		}
	}
	for tbl := range beforeTables {
		if _, ok := afterTables[tbl]; !ok {
			t.Fatalf("recovery create dropped table %q; recovery path must not alter schema", tbl)
		}
	}

	// Then this package's own production files carry no RPC send path.
	recoveryPeriodAssertNoRPCSendPath(t)
}

// --- T028: recovery-snapshot query tests (US6; specs/007-withdrawal-creation/
// tasks.md T028; contracts/api.md §3 precedence + reader protocol) ---
//
// These cases APPEND to T027's file (T027 owns it). They read the SAME recovery
// fixtures T027 owns — reorg_policy_history / reorg_recovery /
// reorg_recovery_events rows through the recoveryPeriod* helpers above — and
// assert the §3 precedence through GetWithdrawal on real PostgreSQL. No
// production code, no new table, no new helper redefines a T027 helper.
//
// Precedence at the one REPEATABLE READ read-only snapshot (§3 reader protocol):
// active row → its phase-mapped state (reconcile_required → paused_reconcile,
// any other persisted phase → recovering); no row + terminal event → released;
// no row + no event → none. Every served view carries the standing
// execution=not_started fact, never a claimed outcome.
//
// The EITHER-read-failure → unknown branch has both layers: the pure-function
// precedence is covered exhaustively by T011's TestQueryCombineRecoveryState
// (query_test.go), and the LIVE query-path proof appended at the end of this
// file blocks each 006 reader's relation in the scratch database in turn and
// drives the failure through GetWithdrawal.
//
// 007 rows carry NO chain height: no height-indexed validity call
// (AnnotateRecoveryHeight or otherwise) appears anywhere below (§3).

// recoveryPeriodSeedTerminalEvent appends one 006 terminal recovery event for
// chainID (auto_completed or released) without an active row — the "row gone but
// history distinguishes released from never-recovered" signal RecoveryReleased
// reads. Events carry no FK, so they outlive the release DELETE.
func recoveryPeriodSeedTerminalEvent(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, recoveryID, event string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `INSERT INTO reorg_recovery_events
		(chain_id, recovery_id, recovery_seq, event_seq, event)
		VALUES ($1, $2, 1, 1, $3)`, chainID, recoveryID, event); err != nil {
		t.Fatalf("seed terminal recovery event %s (chain %d): %v", event, chainID, err)
	}
}

// recoveryPeriodSeedRecoveryRow inserts the active recovery ROW alone; the FK
// policy version (chain_id, policy_seq 1) must already exist. A
// release-then-re-establish case re-seeds a fresh active row on the same chain
// this way, without a duplicate reorg_policy_history insert.
func recoveryPeriodSeedRecoveryRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, recoveryID, phase string, recoverySeq int64) {
	t.Helper()
	if _, err := pool.Exec(ctx, `INSERT INTO reorg_recovery
		(chain_id, recovery_id, phase, policy_seq, max_depth, bound_old_number, bound_old_hash, recovery_seq)
		VALUES ($1, $2, $3, 1, 64, 100, $4, $5)`,
		chainID, recoveryID, phase, withdrawalHash("aa"), recoverySeq); err != nil {
		t.Fatalf("seed recovery row (chain %d phase %s seq %d): %v", chainID, phase, recoverySeq, err)
	}
}

// recoveryPeriodReleaseRecovery performs 006's terminal release: append the
// terminal event, then DELETE the active row (release = terminal event + NO
// active row). The events table has no FK, so the release survives the delete.
func recoveryPeriodReleaseRecovery(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, recoveryID string) {
	t.Helper()
	recoveryPeriodSeedTerminalEvent(t, ctx, pool, chainID, recoveryID, "auto_completed")
	if _, err := pool.Exec(ctx, `DELETE FROM reorg_recovery WHERE chain_id = $1`, chainID); err != nil {
		t.Fatalf("release recovery row (chain %d): %v", chainID, err)
	}
}

// recoveryPeriodPersistedRequest issues one credential + active grant and
// persists one compliant withdrawal, returning its request_id — the GET target
// every T028 snapshot case needs. It reuses the T010 intake fixtures.
func recoveryPeriodPersistedRequest(t *testing.T, ctx context.Context, pool *pgxpool.Pool, callerID int64, authID, idemKey string) string {
	t.Helper()
	key := intakeKey(t, ctx, pool, callerID)
	intakeSupplyGrant(t, ctx, pool, callerID, authID, intakeAmount)
	res, err := SubmitWithdrawal(ctx, pool, intakeReq(key, idemKey, authID))
	if err != nil || res.Status != 201 {
		t.Fatalf("persist request (%s) = (%+v, %v), want 201", authID, res, err)
	}
	return res.RequestID
}

// recoveryPeriodAssertSnapshot GETs requestID as callerID and asserts the served
// view is exactly wantState with the standing not_started execution and intact
// request facts.
func recoveryPeriodAssertSnapshot(t *testing.T, ctx context.Context, pool *pgxpool.Pool, callerID int64, requestID, wantState string) {
	t.Helper()
	view, err := GetWithdrawal(ctx, pool, callerID, requestID)
	if err != nil {
		t.Fatalf("GetWithdrawal(%s): %v", requestID, err)
	}
	if view.Recovery.State != wantState {
		t.Fatalf("recovery.state = %q, want %q", view.Recovery.State, wantState)
	}
	if view.Recovery.Execution != queryExecutionNotStarted {
		t.Fatalf("recovery.execution = %q, want %q", view.Recovery.Execution, queryExecutionNotStarted)
	}
	if view.RequestID != requestID || view.Status != "accepted" {
		t.Fatalf("view facts = %+v, want request_id %q status accepted", view, requestID)
	}
}

// TestWithdrawalRecoveryPeriodQuerySnapshotRecovering is precedence row 1: an
// active recovery row in a non-reconcile phase maps to recovering, and the
// served execution is the constant not_started.
func TestWithdrawalRecoveryPeriodQuerySnapshotRecovering(t *testing.T) {
	// Given an active recovery row (phase detected) on the request's chain.
	ctx, pool := grantSetup(t)
	const callerID = int64(7501)
	recoveryPeriodSeedActiveRecovery(t, ctx, pool, intakeChainID, "rec-snap-recovering", "detected")
	requestID := recoveryPeriodPersistedRequest(t, ctx, pool, callerID, "auth-recovery-snap-recovering", "idem-recovery-snap-recovering")

	// When the owner reads the request through the query path.
	// Then it serves recovering / not_started — never a claimed outcome.
	recoveryPeriodAssertSnapshot(t, ctx, pool, callerID, requestID, queryStateRecovering)
}

// TestWithdrawalRecoveryPeriodQuerySnapshotPausedReconcile is precedence row 2:
// an active row in the reconcile_required phase maps to paused_reconcile, with
// the same constant not_started execution.
func TestWithdrawalRecoveryPeriodQuerySnapshotPausedReconcile(t *testing.T) {
	// Given an active recovery row in the reconcile_required (paused) phase.
	ctx, pool := grantSetup(t)
	const callerID = int64(7502)
	recoveryPeriodSeedActiveRecovery(t, ctx, pool, intakeChainID, "rec-snap-paused", queryPhaseReconcileRequired)
	requestID := recoveryPeriodPersistedRequest(t, ctx, pool, callerID, "auth-recovery-snap-paused", "idem-recovery-snap-paused")

	// When the owner reads the request.
	// Then it serves paused_reconcile / not_started.
	recoveryPeriodAssertSnapshot(t, ctx, pool, callerID, requestID, queryStatePausedReconcile)
}

// TestWithdrawalRecoveryPeriodQuerySnapshotReleased is precedence row 3: no
// active row but a terminal release event on record serves released — the state
// 006 deliberately distinguishes from never-recovered.
func TestWithdrawalRecoveryPeriodQuerySnapshotReleased(t *testing.T) {
	// Given a persisted request and a terminal 006 release event with NO active
	// recovery row (release = event + row gone).
	ctx, pool := grantSetup(t)
	const callerID = int64(7503)
	requestID := recoveryPeriodPersistedRequest(t, ctx, pool, callerID, "auth-recovery-snap-released", "idem-recovery-snap-released")
	recoveryPeriodReleaseRecovery(t, ctx, pool, intakeChainID, "rec-snap-released-1")
	if n := recoveryPeriodEventCount(t, ctx, pool); n != 1 {
		t.Fatalf("terminal event rows = %d, want 1", n)
	}

	// When the owner reads the request.
	// Then it serves released / not_started.
	recoveryPeriodAssertSnapshot(t, ctx, pool, callerID, requestID, queryStateReleased)
}

// TestWithdrawalRecoveryPeriodQuerySnapshotNone is precedence row 4: no active
// row and no terminal event serves none — the ordinary pre-recovery state.
func TestWithdrawalRecoveryPeriodQuerySnapshotNone(t *testing.T) {
	// Given a persisted request and no 006 recovery row or event at all.
	ctx, pool := grantSetup(t)
	const callerID = int64(7504)
	requestID := recoveryPeriodPersistedRequest(t, ctx, pool, callerID, "auth-recovery-snap-none", "idem-recovery-snap-none")
	if n := recoveryPeriodEventCount(t, ctx, pool); n != 0 {
		t.Fatalf("recovery event rows = %d, want 0", n)
	}

	// When the owner reads the request.
	// Then it serves none / not_started.
	recoveryPeriodAssertSnapshot(t, ctx, pool, callerID, requestID, queryStateNone)
}

// TestWithdrawalRecoveryPeriodSnapshotReleaseReEstablishConcurrent drives the
// §3 single-snapshot reader protocol across a release-then-re-establish
// transition: N=4 concurrent GetWithdrawals race the re-establish write, and
// every response must be a VALID recovery state (none / recovering / released /
// unknown) — never an empty, mixed, or out-of-vocabulary state — with the
// constant not_started execution. The barrier makes the concurrency real; the
// assertions are validity-set membership (deterministic) rather than a timing
// guess at which state a reader happens to catch. Steady state afterwards is the
// NEW active row's state (recovering).
func TestWithdrawalRecoveryPeriodSnapshotReleaseReEstablishConcurrent(t *testing.T) {
	// Given a persisted request and a chain whose recovery was released (event +
	// row gone), about to be re-established as a fresh active row.
	ctx, pool := grantSetup(t)
	const callerID = int64(7505)
	requestID := recoveryPeriodPersistedRequest(t, ctx, pool, callerID, "auth-recovery-snap-cycle", "idem-recovery-snap-cycle")
	recoveryPeriodSeedActiveRecovery(t, ctx, pool, intakeChainID, "rec-snap-cycle-1", "detected")
	recoveryPeriodReleaseRecovery(t, ctx, pool, intakeChainID, "rec-snap-cycle-1")

	// validStates is the closed §3 vocabulary at one snapshot; a reader may land
	// on either side of the re-establish write but never outside this set.
	validStates := map[string]bool{
		queryStateNone:       true,
		queryStateRecovering: true,
		queryStateReleased:   true,
		queryStateUnknown:    true,
	}

	// When four readers are released at a barrier as the new active row lands.
	const readers = 4
	start := make(chan struct{})
	done := make(chan error, readers)
	var wg sync.WaitGroup
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			view, err := GetWithdrawal(ctx, pool, callerID, requestID)
			if err != nil {
				done <- err
				return
			}
			if !validStates[view.Recovery.State] {
				done <- &recoveryPeriodInvalidState{state: view.Recovery.State}
				return
			}
			if view.Recovery.Execution != queryExecutionNotStarted {
				done <- &recoveryPeriodInvalidState{state: "execution=" + view.Recovery.Execution}
				return
			}
			done <- nil
		}()
	}
	recoveryPeriodSeedRecoveryRow(t, ctx, pool, intakeChainID, "rec-snap-cycle-2", "detected", 2)
	close(start)
	wg.Wait()
	close(done)

	// Then no reader saw an error, an empty state, or a state outside the set.
	for err := range done {
		if err != nil {
			t.Fatalf("concurrent GetWithdrawal: %v (valid states: none/recovering/released/unknown)", err)
		}
	}

	// And steady-state reads show the NEW active row: recovering / not_started.
	recoveryPeriodAssertSnapshot(t, ctx, pool, callerID, requestID, queryStateRecovering)
}

// recoveryPeriodInvalidState reports a concurrent reader that returned a state
// outside the §3 vocabulary (or a non-constant execution) without calling Fatal
// from a non-test goroutine.
type recoveryPeriodInvalidState struct{ state string }

func (e *recoveryPeriodInvalidState) Error() string {
	return "invalid recovery snapshot state: " + e.state
}

// --- Review gap #5: LIVE either-read-failure → unknown proof (FR-16/FR-17, V8;
// contracts/api.md §3 "EITHER statement errors → ROLLBACK + state: unknown",
// "A failed read MUST NOT map to none-or-released"). T011-unit covers the
// combiner; the two cases below drive the failure through the REAL query path
// (GetWithdrawal) on real PostgreSQL. The faults are test-local: a relation
// rename in the scratch database plus a read-only pgx query tracer, never a
// production fault flag. No production file is touched.
//
// A GET is (request read) → one REPEATABLE READ read-only tx running (a)
// indexer.LoadRecoveryState then, only if (a) succeeded, (b)
// indexer.RecoveryReleased (query.go queryReadRecoverySignal). The first case
// blocks (a)'s relation; the second blocks (b)'s, so (a) demonstrably succeeds
// first (observed through the staged pre-failure snapshot, a phase probe, and
// the tracer's ordered statement sequence) before (b) fails.

// recoveryPeriodRecoveryFaultName / recoveryPeriodEventsFaultName are the
// scratch-database names the first/second reader relations are renamed to for
// the fault. They live only in the per-test container and are never restored:
// the container is torn down with the test.
const (
	recoveryPeriodRecoveryFaultName = "reorg_recovery_period_first_fault"
	recoveryPeriodEventsFaultName   = "reorg_recovery_events_period_second_fault"
)

// recoveryPeriodRenameRelation renames a relation in the scratch database — the
// test-local blocked-relation fault. Names are internal constants, so the
// interpolation is safe.
func recoveryPeriodRenameRelation(t *testing.T, ctx context.Context, pool *pgxpool.Pool, from, to string) {
	t.Helper()
	if _, err := pool.Exec(ctx, "ALTER TABLE "+from+" RENAME TO "+to); err != nil {
		t.Fatalf("rename %s -> %s (fault staging): %v", from, to, err)
	}
}

// recoveryPeriodTraceEntry is one traced statement: its SQL and the error the
// driver/server returned for it.
type recoveryPeriodTraceEntry struct {
	sql string
	err error
}

// recoveryPeriodTracer is a read-only pgx QueryTracer. It records the statement
// sequence a pooled connection actually executed, in order, so a test can show
// that the first recovery read succeeded before the second failed.
type recoveryPeriodTracer struct {
	mu      sync.Mutex
	entries []recoveryPeriodTraceEntry
}

// recoveryPeriodTraceSQLKey carries the SQL from TraceQueryStart to
// TraceQueryEnd (TraceQueryEndData has no SQL field).
type recoveryPeriodTraceSQLKey struct{}

func (tr *recoveryPeriodTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, recoveryPeriodTraceSQLKey{}, data.SQL)
}

func (tr *recoveryPeriodTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	sql, _ := ctx.Value(recoveryPeriodTraceSQLKey{}).(string)
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.entries = append(tr.entries, recoveryPeriodTraceEntry{sql: sql, err: data.Err})
}

// recoveryReads returns the traced entries that touched a 006 recovery
// relation, in execution order.
func (tr *recoveryPeriodTracer) recoveryReads() []recoveryPeriodTraceEntry {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	var out []recoveryPeriodTraceEntry
	for _, e := range tr.entries {
		if strings.Contains(e.sql, "reorg_recovery") {
			out = append(out, e)
		}
	}
	return out
}

// recoveryPeriodIsFirstRead / recoveryPeriodIsSecondRead classify one traced
// statement by the relation it reads: the active-row reader vs the terminal-
// event reader (readRecoveryRowSQL / readRecoveryReleasedSQL).
func recoveryPeriodIsFirstRead(sql string) bool {
	return strings.Contains(sql, "reorg_recovery") && !strings.Contains(sql, "reorg_recovery_events")
}

func recoveryPeriodIsSecondRead(sql string) bool {
	return strings.Contains(sql, "reorg_recovery_events")
}

// recoveryPeriodTracedPool opens a pgx pool over dsn whose every statement is
// recorded by a fresh tracer — the statement-level probe the second-read case
// uses to prove ordering. It never alters execution; it only observes.
func recoveryPeriodTracedPool(t *testing.T, dsn string) (*pgxpool.Pool, *recoveryPeriodTracer) {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse traced dsn: %v", err)
	}
	tracer := &recoveryPeriodTracer{}
	cfg.ConnConfig.Tracer = tracer
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("open traced pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool, tracer
}

// recoveryPeriodAssertUnknownFacts asserts a served view is exactly the
// either-read-failure contract: state unknown (never none/released), execution
// the standing not_started, and every request fact intact.
func recoveryPeriodAssertUnknownFacts(t *testing.T, view *WithdrawalView, requestID string, callerID int64) {
	t.Helper()
	if view.Recovery.State != queryStateUnknown {
		t.Fatalf("recovery.state = %q, want %q (a failed read is never none/released)", view.Recovery.State, queryStateUnknown)
	}
	if view.Recovery.Execution != queryExecutionNotStarted {
		t.Fatalf("recovery.execution = %q, want %q", view.Recovery.Execution, queryExecutionNotStarted)
	}
	if view.RequestID != requestID || view.CallerID != callerID ||
		view.ChainID != intakeChainID || view.Asset != intakeAsset ||
		view.Recipient != intakeRecipient || view.Amount != intakeAmount || view.Status != "accepted" {
		t.Fatalf("request facts = %+v, want request_id %q caller %d chain %d asset %s recipient %s amount %s status accepted",
			view, requestID, callerID, intakeChainID, intakeAsset, intakeRecipient, intakeAmount)
	}
}

// TestWithdrawalRecoveryPeriodQueryFirstReadFailsUnknown is review gap #5(a):
// when the FIRST recovery read (reorg_recovery) fails inside the REPEATABLE
// READ tx, a live GET yields state unknown with every request fact intact, and
// the reader short-circuits — the second read is never attempted. An active row
// AND a terminal release event are seeded up front so a failure wrongly folded
// into the no-row path would surface as releasing/recovering, not unknown.
func TestWithdrawalRecoveryPeriodQueryFirstReadFailsUnknown(t *testing.T) {
	// Given an active recovery row plus a terminal release event on the
	// request's chain (the two states a bad fallthrough could invent).
	ctx, pool, dsn := failureSetup(t)
	const (
		callerID = int64(7601)
		authID   = "auth-recovery-first-read-fault"
		idemKey  = "idem-recovery-first-read-fault"
	)
	recoveryPeriodSeedActiveRecovery(t, ctx, pool, intakeChainID, "rec-first-read-fault", "detected")
	recoveryPeriodSeedTerminalEvent(t, ctx, pool, intakeChainID, "rec-first-read-fault-rel", "auto_completed")
	requestID := recoveryPeriodPersistedRequest(t, ctx, pool, callerID, authID, idemKey)

	// When the first reader's relation is unavailable (test-local rename).
	recoveryPeriodRenameRelation(t, ctx, pool, "reorg_recovery", recoveryPeriodRecoveryFaultName)

	traced, tracer := recoveryPeriodTracedPool(t, dsn)
	view, err := GetWithdrawal(ctx, traced, callerID, requestID)
	if err != nil {
		t.Fatalf("GetWithdrawal with a failing first recovery read: %v", err)
	}

	// Then the live query still serves the request facts with state unknown.
	recoveryPeriodAssertUnknownFacts(t, view, requestID, callerID)

	// And the traced sequence shows exactly the first read, and it failed: the
	// second read was never attempted (the reader short-circuits).
	reads := tracer.recoveryReads()
	if len(reads) != 1 || !recoveryPeriodIsFirstRead(reads[0].sql) || reads[0].err == nil {
		t.Fatalf("recovery reads = %+v, want exactly one FAILING read of reorg_recovery and no second read", reads)
	}
}

// TestWithdrawalRecoveryPeriodQuerySecondReadFailsUnknown is review gap #5(b):
// when the SECOND recovery read fails, the first read demonstrably succeeded
// first and the live query still yields unknown with facts intact. The
// pre-failure value is observed three ways — a staged snapshot through the real
// path before the fault, a phase probe of the still-intact first relation while
// the second is blocked, and the tracer's ordered in-call sequence (first read
// nil error, then second read error).
func TestWithdrawalRecoveryPeriodQuerySecondReadFailsUnknown(t *testing.T) {
	// Given an active recovery row (phase detected) on the request's chain.
	ctx, pool, dsn := failureSetup(t)
	const (
		callerID = int64(7602)
		authID   = "auth-recovery-second-read-fault"
		idemKey  = "idem-recovery-second-read-fault"
	)
	recoveryPeriodSeedActiveRecovery(t, ctx, pool, intakeChainID, "rec-second-read-fault", "detected")
	requestID := recoveryPeriodPersistedRequest(t, ctx, pool, callerID, authID, idemKey)

	// Pre-failure value observed through the real query path: with both reads
	// intact, the first read's detected row maps to recovering. That state is
	// only reachable because the first read succeeded.
	recoveryPeriodAssertSnapshot(t, ctx, pool, callerID, requestID, queryStateRecovering)

	// When ONLY the second reader's relation is unavailable.
	recoveryPeriodRenameRelation(t, ctx, pool, "reorg_recovery_events", recoveryPeriodEventsFaultName)

	// The first read's source is still intact and carries the known pre-failure
	// phase while the second is blocked.
	if phase, _ := recoveryPeriodRecoveryRow(t, ctx, pool, intakeChainID); phase != "detected" {
		t.Fatalf("first-read phase after staging the second-read fault = %q, want detected", phase)
	}

	traced, tracer := recoveryPeriodTracedPool(t, dsn)
	view, err := GetWithdrawal(ctx, traced, callerID, requestID)
	if err != nil {
		t.Fatalf("GetWithdrawal with a failing second recovery read: %v", err)
	}

	// Then the live query still serves the request facts with state unknown.
	recoveryPeriodAssertUnknownFacts(t, view, requestID, callerID)

	// And the ordered in-call trace demonstrates the first read SUCCEEDED
	// before the second failed: a reorg_recovery read with nil error precedes
	// the failing reorg_recovery_events read.
	reads := tracer.recoveryReads()
	first, second := -1, -1
	for i, e := range reads {
		switch {
		case first < 0 && recoveryPeriodIsFirstRead(e.sql):
			first = i
			if e.err != nil {
				t.Fatalf("first recovery read failed (%v); the staged fault must hit only the second read", e.err)
			}
		case first >= 0 && second < 0 && recoveryPeriodIsSecondRead(e.sql):
			second = i
			if e.err == nil {
				t.Fatalf("second recovery read succeeded; the staged fault did not land")
			}
		}
	}
	if first < 0 {
		t.Fatalf("first recovery read (reorg_recovery) never executed: %+v", reads)
	}
	if second < 0 {
		t.Fatalf("second recovery read (reorg_recovery_events) never executed after the first: %+v", reads)
	}
}
