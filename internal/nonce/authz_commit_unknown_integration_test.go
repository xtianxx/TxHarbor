//go:build integration

// authz_commit_unknown_integration_test.go owns spec task T045 for
// 008-nonce-manager (V11/V12, FR-15/FR-17, observation.md §3.4/§4, SC-02/05):
// commit-unknown convergence across the 007 boundary over a real PostgreSQL.
//
// Two independent uncertain COMMITs are staged, each by wrapping the pooled
// connection so the COMMIT reaches the server (and is durable) while the caller
// observes a storage error:
//
//   - 008 admission: the admission commits its binding, the caller sees an
//     error, and a retry with the SAME intent_id converges on the original
//     binding (replay) — never a second binding;
//   - 007 grant: RevokeGrant/SupplyGrant commit their audit attempt, the caller
//     hits the uncertain-commit branch, and a retry with the SAME operation_id
//     converges through the recorded attempt (the original recorded action,
//     never re-interpreted or upgraded).
//
// Neither side infers the other's outcome: 008 writes zero 007 rows and reads
// no 007 attempt, 007 writes zero 008 rows and reads no 008 binding.
//
// The uncertainty is injected at the wire, never in production code: a DialFunc
// wraps every pgx connection. pgx always uses the simple protocol for the
// argument-less commit (pgx/conn.go: "Always use simple protocol when there are
// no arguments"), so when the client emits its COMMIT the wrapper forwards it
// (the server durably commits) and only then fails the caller's read of the
// post-commit response. No production fault-injection flag, endpoint or handler
// exists.
//
// The file is package nonce_test (the public nonce.NewAllocator/Allocate and
// withdrawal.SupplyGrant/RevokeGrant surfaces) and reuses the
// migration_integration_test.go container/seed helpers; its own helpers are
// cu-prefixed.
package nonce_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/nonce"
	"github.com/xtianxx/txharbor/internal/withdrawal"
)

const (
	cuChain  = int64(80045)
	cuCaller = int64(45)
)

var (
	cuSender     = nonceAddr("c1")
	cuAsset      = nonceAddr("c2")
	cuRecipient  = nonceAddr("c3")
	cuAuth008    = "cu-008-auth"
	cuAuth007    = "cu-007-revoke-target"
	cuAuthSupply = "cu-007-supply-target"
	cuOpRevoke   = "cu-007-revoke-op"
	cuOpSupply   = "cu-007-supply-op"
)

// cuInjector is the armed/disarmed switch shared by every wrapped connection.
// While armed, the next COMMIT a wrapped connection emits is turned into a
// commit-unknown; the switch then disarms so the recovery/retry reads run
// normally.
type cuInjector struct{ armed atomic.Bool }

func (i *cuInjector) arm()  { i.armed.Store(true) }
func (i *cuInjector) stop() { i.armed.Store(false) }

var errCUInjectedCommitUnknown = errors.New("cu: injected commit-unknown after a durable commit")

// cuCommitUnknownConn wraps one real PostgreSQL connection. In Write it tracks
// the client's outbound bytes (rolling a short window so a split flush still
// matches) and, when it sees the argument-less COMMIT while armed, records that
// the next read must be failed. In Read it waits for the server's post-commit
// response — CommandComplete for COMMIT is only emitted once the commit record
// is durable — and then fails the caller, leaving the transaction committed.
type cuCommitUnknownConn struct {
	net.Conn
	inj       *cuInjector
	probe     []byte
	sawCommit atomic.Bool
}

func (c *cuCommitUnknownConn) Write(b []byte) (int, error) {
	n, err := c.Conn.Write(b)
	if err == nil && c.inj.armed.Load() {
		c.probe = append(c.probe, b...)
		switch {
		case bytes.Contains(c.probe, []byte("commit")):
			c.sawCommit.Store(true)
			c.inj.stop()
			c.probe = c.probe[:0]
		case len(c.probe) > 16:
			c.probe = append(c.probe[:0], c.probe[len(c.probe)-16:]...)
		}
	}
	return n, err
}

func (c *cuCommitUnknownConn) Read(b []byte) (int, error) {
	if c.sawCommit.CompareAndSwap(true, false) {
		// The server's response to COMMIT is emitted only after the commit is
		// durable; one successful read proves the commit landed. Fail the
		// caller, then close so the pool discards the desynced connection.
		_ = c.Conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, _ = c.Conn.Read(make([]byte, 4096))
		_ = c.Conn.Close()
		return 0, errCUInjectedCommitUnknown
	}
	return c.Conn.Read(b)
}

// cuOpenWrappedPool opens a pgx pool whose every connection can be turned into
// a commit-unknown through inj.
func cuOpenWrappedPool(t *testing.T, dsn string, inj *cuInjector) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse wrapped pool config: %v", err)
	}
	cfg.ConnConfig.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, derr := (&net.Dialer{}).DialContext(ctx, network, addr)
		if derr != nil {
			return nil, derr
		}
		return &cuCommitUnknownConn{Conn: conn, inj: inj}, nil
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("open wrapped pgx pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// cuRPC is the scripted JSON-RPC double: a frozen healthy view (latest =
// pending = 0, head count 0) so a valid admission classifies consistent at
// nonce 0 without an Anvil container.
type cuRPC struct{ calls int64 }

func (r *cuRPC) CallContext(_ context.Context, result any, method string, _ ...any) error {
	atomic.AddInt64(&r.calls, 1)
	switch method {
	case "eth_getTransactionCount":
		return json.Unmarshal([]byte(`"0x0"`), result)
	case "eth_getBlockByNumber":
		payload := fmt.Sprintf(`{"number":"0x0","hash":%q}`, "0x"+strings.Repeat("ab", 32))
		return json.Unmarshal([]byte(payload), result)
	default:
		return fmt.Errorf("cuRPC: unexpected method %q", method)
	}
}

func cuAllocator(pool *pgxpool.Pool) *nonce.Allocator {
	observer := nonce.NewObserver(&cuRPC{}, nonce.ObserverConfig{
		RPCTimeout:   2 * time.Second,
		RetryInitial: time.Millisecond,
		RetryMax:     5 * time.Millisecond,
	})
	admitGate := nonce.NewRebuildGate()
	admitGate.Open()
	return nonce.NewAllocator(pool, observer, admitGate)
}

// cuSeed writes the FK prerequisites: one caller, one registered sender, and
// the two active 007 carrier rows the 008 admission validates.
func cuSeed(t *testing.T, sqlDB *sql.DB) {
	t.Helper()
	nonceMustExec(t, sqlDB, `INSERT INTO caller (caller_id, label) VALUES ($1, 'cu-it')`, cuCaller)
	nonceMustExec(t, sqlDB, `INSERT INTO nonce_wallet_registry
		(chain_id, sender, state, registry_seq) VALUES ($1, $2, 'active', 1)`, cuChain, cuSender)
	for _, id := range []string{cuAuth008, cuAuth007} {
		nonceMustExec(t, sqlDB, `INSERT INTO withdrawal_authorizations
			(authorization_id, caller_id, chain_id, asset, recipient, amount, state)
			VALUES ($1, $2, $3, $4, $5, 1, 'active')`, id, cuCaller, cuChain, cuAsset, cuRecipient)
	}
}

func cuCount(t *testing.T, sqlDB *sql.DB, query string, args ...any) int {
	t.Helper()
	var n int
	if err := sqlDB.QueryRow(query, args...).Scan(&n); err != nil {
		t.Fatalf("cu count %q: %v", query, err)
	}
	return n
}

// TestNonceAuthzCommitUnknownConvergenceIntegration is the T045 V11/V12
// acceptance.
func TestNonceAuthzCommitUnknownConvergenceIntegration(t *testing.T) {
	ctx := context.Background()
	dsn := nonceStartPostgres(t)

	var out bytes.Buffer
	if err := db.MigrateUp(ctx, nonceMigrateOptions(dsn), &out); err != nil {
		t.Fatalf("MigrateUp() error = %v (output %q)", err, out.String())
	}
	sqlDB := nonceOpenSQL(t, dsn)
	cuSeed(t, sqlDB)

	inj := &cuInjector{}
	pool := cuOpenWrappedPool(t, dsn, inj)
	alloc := cuAllocator(pool)

	// ---- 008 admission: uncertain COMMIT retried with the SAME intent_id ----
	req := nonce.AllocationRequest{
		IntentID:        "cu-008-intent",
		ChainID:         cuChain,
		Sender:          cuSender,
		AuthorizationID: cuAuth008,
	}
	inj.arm()
	binding, outcome, err := alloc.Allocate(ctx, req)
	inj.stop()
	if !nonce.IsOutcome(err, nonce.OutcomeTemporarilyUnavailable) || outcome != nonce.OutcomeTemporarilyUnavailable {
		t.Fatalf("008 uncertain commit = (%+v, %q, %v), want temporarily_unavailable with no binding", binding, outcome, err)
	}
	if binding != nil {
		t.Fatalf("008 uncertain commit handed back binding %+v, want none", binding)
	}
	// The COMMIT was durable even though the caller saw an error.
	if got := cuCount(t, sqlDB, `SELECT count(*) FROM nonce_bindings WHERE intent_id = $1`, req.IntentID); got != 1 {
		t.Fatalf("bindings after the uncertain commit = %d, want the 1 durable row", got)
	}
	var winnerID, winnerNonce string
	if err := sqlDB.QueryRow(
		`SELECT binding_id, nonce::text FROM nonce_bindings WHERE intent_id = $1`, req.IntentID).
		Scan(&winnerID, &winnerNonce); err != nil {
		t.Fatalf("read the committed winner: %v", err)
	}

	replayed, routcome, rerr := alloc.Allocate(ctx, req)
	if rerr != nil || routcome != nonce.OutcomeReplayed {
		t.Fatalf("008 retry = (%+v, %q, %v), want replayed", replayed, routcome, rerr)
	}
	if replayed == nil || replayed.BindingID != winnerID || nonce.FormatDecimal(replayed.Nonce) != winnerNonce {
		t.Fatalf("008 retry binding = %+v, want the original %s/%s", replayed, winnerID, winnerNonce)
	}
	if got := cuCount(t, sqlDB, `SELECT count(*) FROM nonce_bindings WHERE intent_id = $1`, req.IntentID); got != 1 {
		t.Fatalf("bindings after the 008 retry = %d, want still 1 (never a second binding)", got)
	}
	// 008 inferred nothing from, and wrote nothing to, 007.
	if got := cuCount(t, sqlDB, `SELECT count(*) FROM withdrawal_grant_audit WHERE authorization_id = $1`, cuAuth008); got != 0 {
		t.Fatalf("008 wrote %d 007 grant-audit rows, want 0", got)
	}
	var authState string
	if err := sqlDB.QueryRow(
		`SELECT state FROM withdrawal_authorizations WHERE authorization_id = $1`, cuAuth008).Scan(&authState); err != nil {
		t.Fatalf("read the 008 authorization: %v", err)
	}
	if authState != "active" {
		t.Fatalf("008 authorization state = %q, want active (008 never mutates 007)", authState)
	}

	// ---- 007 RevokeGrant: uncertain COMMIT retried with the SAME operation_id ----
	inj.arm()
	revoked, verr := withdrawal.RevokeGrant(ctx, pool, cuOpRevoke, cuAuth007, "cu-operator", "cu-reason")
	inj.stop()
	if verr != nil || revoked == nil || revoked.Action != "revoked" {
		t.Fatalf("007 revoke uncertain commit = (%+v, %v), want the recorded revoked attempt", revoked, verr)
	}
	// The retry re-runs the corpus, hits withdrawal_grant_audit_operation_id_uniq
	// and must report the ORIGINAL recorded action: the grant is already
	// revoked, so a naive re-run would say revoke_nop — convergence says
	// 'revoked', never re-interpreted and never upgraded.
	revokedRetry, verr := withdrawal.RevokeGrant(ctx, pool, cuOpRevoke, cuAuth007, "cu-operator", "cu-reason")
	if verr != nil || revokedRetry == nil || revokedRetry.Action != "revoked" {
		t.Fatalf("007 revoke retry = (%+v, %v), want the recorded revoked attempt", revokedRetry, verr)
	}
	if got := cuCount(t, sqlDB, `SELECT count(*) FROM withdrawal_grant_audit WHERE operation_id = $1`, cuOpRevoke); got != 1 {
		t.Fatalf("audit rows for the revoke operation = %d, want exactly 1", got)
	}
	var revokeState string
	if err := sqlDB.QueryRow(
		`SELECT state FROM withdrawal_authorizations WHERE authorization_id = $1`, cuAuth007).Scan(&revokeState); err != nil {
		t.Fatalf("read the revoked grant: %v", err)
	}
	if revokeState != "revoked" {
		t.Fatalf("revoked grant state = %q, want revoked", revokeState)
	}

	// ---- 007 SupplyGrant: uncertain COMMIT retried with the SAME operation_id ----
	op := withdrawal.OpInput{
		OperationID:     cuOpSupply,
		Action:          "supply",
		AuthorizationID: cuAuthSupply,
		CallerID:        cuCaller,
		ChainID:         cuChain,
		Asset:           cuAsset,
		Recipient:       cuRecipient,
		Amount:          "1",
	}
	inj.arm()
	supplied, serr := withdrawal.SupplyGrant(ctx, pool, op, "cu-operator", "cu-reason")
	inj.stop()
	if serr != nil || supplied == nil || supplied.Action != "supplied" {
		t.Fatalf("007 supply uncertain commit = (%+v, %v), want the recorded supplied attempt", supplied, serr)
	}
	// The retry is an equal re-supply on the now-present grant row; the SAME
	// operation id must still report the recorded 'supplied', never a fresh
	// 'resupplied' attempt.
	suppliedRetry, serr := withdrawal.SupplyGrant(ctx, pool, op, "cu-operator", "cu-reason")
	if serr != nil || suppliedRetry == nil || suppliedRetry.Action != "supplied" {
		t.Fatalf("007 supply retry = (%+v, %v), want the recorded supplied attempt", suppliedRetry, serr)
	}
	if got := cuCount(t, sqlDB, `SELECT count(*) FROM withdrawal_grant_audit WHERE operation_id = $1`, cuOpSupply); got != 1 {
		t.Fatalf("audit rows for the supply operation = %d, want exactly 1", got)
	}

	// 007 inferred nothing from, and wrote nothing to, 008.
	if got := cuCount(t, sqlDB,
		`SELECT count(*) FROM nonce_bindings WHERE authorization_id IN ($1, $2)`, cuAuth007, cuAuthSupply); got != 0 {
		t.Fatalf("007 grant operations produced %d 008 bindings, want 0", got)
	}
	if got := cuCount(t, sqlDB, `SELECT count(*) FROM nonce_observations`); got != 1 {
		t.Fatalf("008 observations = %d, want exactly the 1 admission observation (007 inferred nothing)", got)
	}
	if got := cuCount(t, sqlDB, `SELECT count(*) FROM nonce_bindings`); got != 1 {
		t.Fatalf("008 bindings = %d, want exactly the 1 admission binding", got)
	}
}
