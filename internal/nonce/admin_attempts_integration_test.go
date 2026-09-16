//go:build integration

// admin_attempts_integration_test.go owns spec task T038 for 008-nonce-manager
// (US5; contracts/observation.md §3.4; V12; FR-08; R7): operator attempt
// semantics over a real PostgreSQL.
//
// What it proves, per the T038 acceptance text:
//   - the same operation id + same op-input yields exactly one audit row and
//     the recorded `applied`/`refused`/`nop` outcome unchanged (never upgraded);
//   - a differing op-input for a recorded operation id yields
//     `operation_conflict` with zero writes;
//   - refusals are committed `refused` outcomes (not a transient failure);
//   - an uncertain COMMIT retries with the SAME operation id and converges
//     through the recorded attempt (one audit row, no double write).
//
// The chain view is a minimal scripted JSON-RPC double only for the
// hold-release refusal leg (the transaction library takes no RPC for the
// registry legs). A dedicated pooled connection injects the uncertain COMMIT:
// the COMMIT reaches the server so the durable commit lands, then the server's
// response is hidden from pgx, turning that one commit into a commit-unknown.
// Every helper this file owns is `oa`-prefixed so it cannot collide with a
// sibling integration test.
package nonce_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/nonce"
)

const (
	// oaChain is the test's deployment chain id.
	oaChain = int64(80041)
	// oaHeadHash is the head identity the scripted chain view reports.
	oaHeadHash = "0x" + "abababababababababababababababababababababababababababababababab"
	// oaObsRefused is the evidence observation reference the hold leg seeds.
	oaObsRefused = "no-oa-refused"
	// oaHoldRefused is the active hold the refusal leg releases.
	oaHoldRefused = "oa-hold-refused"
)

// oaCaller is the raw JSON-RPC surface the Observer needs; the exported
// signature matches the unexported rpcCaller interface NewObserver accepts.
type oaCaller interface {
	CallContext(ctx context.Context, result any, method string, args ...any) error
}

// oaRPC is a scripted chain view (latest == pending == count, head 0). The
// values are JSON literals because the observer's block-result type is
// package-private.
type oaRPC struct{ count string }

func oaHealthyRPC() oaRPC { return oaRPC{count: `"0x0"`} }

func (r oaRPC) CallContext(_ context.Context, result any, method string, _ ...any) error {
	switch method {
	case "eth_getTransactionCount":
		return json.Unmarshal([]byte(r.count), result)
	case "eth_getBlockByNumber":
		payload := fmt.Sprintf(`{"number":"0x0","hash":%q}`, oaHeadHash)
		return json.Unmarshal([]byte(payload), result)
	default:
		return fmt.Errorf("oaRPC: unexpected method %q", method)
	}
}

// oaRunner wires one AdminRunner over the pool with the scripted caller.
func oaRunner(pool *pgxpool.Pool, caller oaCaller) *nonce.AdminRunner {
	return nonce.NewAdminRunner(pool, nonce.NewObserver(caller, nonce.ObserverConfig{
		RPCTimeout:   2 * time.Second,
		RetryInitial: time.Millisecond,
		RetryMax:     5 * time.Millisecond,
	}))
}

// oaCommitUnknownConn is a net.Conn that lets the first COMMIT reach the server
// (so the durable commit lands) and then hides the server's response from pgx,
// turning that one commit into an uncertain COMMIT. Every later commit on the
// pool passes through untouched, so a same-operation-id retry can commit
// normally.
type oaCommitUnknownConn struct {
	net.Conn
	drop  *atomic.Bool
	armed bool
}

func (c *oaCommitUnknownConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	if err == nil && !c.armed &&
		bytes.Contains(bytes.ToLower(p), []byte("commit")) &&
		c.drop.CompareAndSwap(true, false) {
		c.armed = true
	}
	return n, err
}

func (c *oaCommitUnknownConn) Read(p []byte) (int, error) {
	if c.armed {
		c.armed = false
		// Receiving the server's post-commit response proves the durable
		// commit landed; the injected error below hides it from pgx.
		_ = c.Conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, _ = c.Conn.Read(make([]byte, 4096))
		return 0, errors.New("oa: injected commit-unknown after a durable commit")
	}
	return c.Conn.Read(p)
}

// oaUnknownCommitPool opens a pool whose first COMMIT is uncertain (the durable
// commit lands but the caller sees an error).
func oaUnknownCommitPool(ctx context.Context, t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse unknown-commit pool config: %v", err)
	}
	drop := new(atomic.Bool)
	drop.Store(true)
	dialer := &net.Dialer{}
	cfg.ConnConfig.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := dialer.DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		return &oaCommitUnknownConn{Conn: conn, drop: drop}, nil
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("open unknown-commit pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// oaRequest builds one registry action attempt.
func oaRequest(action, opID, sender, reason string) nonce.AdminRequest {
	return nonce.AdminRequest{
		Action:      action,
		OperationID: opID,
		ChainID:     oaChain,
		Sender:      sender,
		Operator:    "oa-operator",
		Reason:      reason,
	}
}

// oaHoldReleaseRequest builds one hold-release attempt against the seeded hold.
func oaHoldReleaseRequest(opID, sender, evidence string) nonce.AdminRequest {
	return nonce.AdminRequest{
		Action:        nonce.AdminActionHoldRelease,
		OperationID:   opID,
		ChainID:       oaChain,
		Sender:        sender,
		HoldID:        oaHoldRefused,
		ObservationID: oaObsRefused,
		Evidence:      evidence,
		Operator:      "oa-operator",
		Reason:        "release",
	}
}

// oaRun executes one attempt expecting no error.
func oaRun(t *testing.T, runner *nonce.AdminRunner, req nonce.AdminRequest) nonce.AdminResult {
	t.Helper()
	res, err := runner.Run(context.Background(), req)
	if err != nil {
		t.Fatalf("run %s: unexpected error %v", req.OperationID, err)
	}
	return res
}

// oaAuditRow reads the recorded attempt for its outcome assertions.
func oaAuditRow(t *testing.T, sqlDB *sql.DB, opID string) (outcome, subject string, count int) {
	t.Helper()
	if err := sqlDB.QueryRow(`SELECT count(*) FROM nonce_ops_audit WHERE operation_id = $1`, opID).
		Scan(&count); err != nil {
		t.Fatalf("count audit %q: %v", opID, err)
	}
	if count != 1 {
		return "", "", count
	}
	if err := sqlDB.QueryRow(`SELECT outcome, subject_id FROM nonce_ops_audit WHERE operation_id = $1`, opID).
		Scan(&outcome, &subject); err != nil {
		t.Fatalf("read audit %q: %v", opID, err)
	}
	return outcome, subject, count
}

// oaRegistry reads one wallet registry row; a missing row reports found=false.
func oaRegistry(t *testing.T, sqlDB *sql.DB, sender string) (state string, seq int64, found bool) {
	t.Helper()
	err := sqlDB.QueryRow(`SELECT state, registry_seq FROM nonce_wallet_registry
		WHERE chain_id = $1 AND sender = $2`, oaChain, sender).Scan(&state, &seq)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return "", 0, false
	case err != nil:
		t.Fatalf("read registry %s: %v", sender, err)
	}
	return state, seq, true
}

// oaSeedRegistryActive inserts one active registry row directly.
func oaSeedRegistryActive(t *testing.T, sqlDB *sql.DB, sender string) {
	t.Helper()
	nonceMustExec(t, sqlDB, `INSERT INTO nonce_wallet_registry
		(chain_id, sender, state, registry_seq) VALUES ($1, $2, 'active', 1)`, oaChain, sender)
}

// oaHoldStillActive reports whether the seeded hold is still the untouched
// active hold (status active, no release recorded).
func oaHoldStillActive(t *testing.T, sqlDB *sql.DB) bool {
	t.Helper()
	var status string
	var releaseOp sql.NullString
	if err := sqlDB.QueryRow(`SELECT status, release_operation_id FROM nonce_scope_holds
		WHERE hold_id = $1`, oaHoldRefused).Scan(&status, &releaseOp); err != nil {
		t.Fatalf("read hold %s: %v", oaHoldRefused, err)
	}
	return status == nonce.HoldStatusActive && !releaseOp.Valid
}

// TestNonceAdminAttemptsIntegration is the T038 V12 acceptance over one migrated
// scratch database, one dedicated sender scope per leg.
func TestNonceAdminAttemptsIntegration(t *testing.T) {
	ctx := context.Background()
	dsn := nonceStartPostgres(t)

	var out bytes.Buffer
	if err := db.MigrateUp(ctx, nonceMigrateOptions(dsn), &out); err != nil {
		t.Fatalf("MigrateUp() error = %v (output %q)", err, out.String())
	}
	sqlDB := nonceOpenSQL(t, dsn)
	pool := replayOpenPool(t, dsn)
	registry := nonce.NewAdminRunner(pool, nil) // registry actions take no RPC

	// --- same operation id + same op-input: one audit row, unchanged --------
	senderSame := nonceAddr("a1")
	reqSame := oaRequest(nonce.AdminActionRegistryRegister, "oa-op-same-1", senderSame, "register")
	if res := oaRun(t, registry, reqSame); res.Outcome != nonce.AdminApplied {
		t.Fatalf("first attempt outcome = %q, want applied", res.Outcome)
	}
	if outcome, subject, count := oaAuditRow(t, sqlDB, reqSame.OperationID); count != 1 ||
		outcome != string(nonce.AdminApplied) || subject != senderSame {
		t.Fatalf("audit after first attempt = (%s, %s, count %d), want (applied, %s, 1)",
			outcome, subject, count, senderSame)
	}
	if _, seq, found := oaRegistry(t, sqlDB, senderSame); !found || seq != 1 {
		t.Fatalf("registry after first attempt = (found %v, seq %d), want (true, 1)", found, seq)
	}
	// The replay recomputes `nop` (the sender is already active) but MUST report
	// the recorded `applied` and write nothing new.
	if res := oaRun(t, registry, reqSame); res.Outcome != nonce.AdminApplied {
		t.Fatalf("replayed attempt outcome = %q, want the recorded applied", res.Outcome)
	}
	if _, _, count := oaAuditRow(t, sqlDB, reqSame.OperationID); count != 1 {
		t.Fatalf("audit rows after replay = %d, want exactly 1", count)
	}
	if _, seq, _ := oaRegistry(t, sqlDB, senderSame); seq != 1 {
		t.Fatalf("registry_seq after replay = %d, want 1 (no double bump)", seq)
	}

	// --- recorded `nop` is never upgraded on replay -------------------------
	senderNop := nonceAddr("a2")
	reqNop := oaRequest(nonce.AdminActionRegistryDisable, "oa-op-nop-1", senderNop, "disable")
	if res := oaRun(t, registry, reqNop); res.Outcome != nonce.AdminNop {
		t.Fatalf("disable unregistered sender outcome = %q, want nop", res.Outcome)
	}
	if _, _, found := oaRegistry(t, sqlDB, senderNop); found {
		t.Fatalf("nop attempt created a registry row for %s", senderNop)
	}
	// Now the sender becomes active, so a recompute would flip it to `applied`;
	// the replay must still report the recorded `nop` and leave it untouched.
	oaSeedRegistryActive(t, sqlDB, senderNop)
	if res := oaRun(t, registry, reqNop); res.Outcome != nonce.AdminNop {
		t.Fatalf("replayed nop outcome = %q, want the recorded nop (never upgraded)", res.Outcome)
	}
	if state, seq, _ := oaRegistry(t, sqlDB, senderNop); state != "active" || seq != 1 {
		t.Fatalf("sender after replayed nop = (%s, seq %d), want (active, 1) untouched", state, seq)
	}
	if _, _, count := oaAuditRow(t, sqlDB, reqNop.OperationID); count != 1 {
		t.Fatalf("audit rows after replayed nop = %d, want exactly 1", count)
	}

	// --- differing op-input: operation_conflict with zero writes ------------
	senderConflict := nonceAddr("a3")
	reqConflict := oaRequest(nonce.AdminActionRegistryRegister, "oa-op-conflict-1", senderConflict, "register")
	if res := oaRun(t, registry, reqConflict); res.Outcome != nonce.AdminApplied {
		t.Fatalf("conflict-leg first attempt outcome = %q, want applied", res.Outcome)
	}
	conflict := oaRequest(nonce.AdminActionRegistryDisable, reqConflict.OperationID, senderConflict, "disable")
	if _, err := registry.Run(ctx, conflict); !nonce.IsOutcome(err, nonce.OutcomeOperationConflict) {
		t.Fatalf("differing op-input error = %v, want operation_conflict", err)
	}
	if state, seq, _ := oaRegistry(t, sqlDB, senderConflict); state != "active" || seq != 1 {
		t.Fatalf("sender after conflict = (%s, seq %d), want (active, 1): the conflict wrote", state, seq)
	}
	if _, _, count := oaAuditRow(t, sqlDB, reqConflict.OperationID); count != 1 {
		t.Fatalf("audit rows after conflict = %d, want exactly 1", count)
	}

	// --- refusals are committed `refused` outcomes --------------------------
	senderRefused := nonceAddr("a4")
	oaSeedRegistryActive(t, sqlDB, senderRefused)
	nonceMustExec(t, sqlDB, `INSERT INTO nonce_scope_state
		(chain_id, sender, reconciled_floor, last_pending) VALUES ($1, $2, NULL, NULL)`,
		oaChain, senderRefused)
	nonceMustExec(t, sqlDB, `INSERT INTO nonce_observations
		(observation_id, chain_id, sender, kind, classification,
		 latest_count, pending_count, head_number, head_hash, error_class, rpc_ref)
		VALUES ($1, $2, $3, 'reconcile', 'consistent', 0, 0, 0, $4, '', 'oa:seed')`,
		oaObsRefused, oaChain, senderRefused, oaHeadHash)
	nonceMustExec(t, sqlDB, `INSERT INTO nonce_scope_holds
		(hold_id, chain_id, sender, cause, status, evidence_observation_id, evidence_detail)
		VALUES ($1, $2, $3, $4, 'active', $5, $4)`,
		oaHoldRefused, oaChain, senderRefused, nonce.CauseUnexplainedGap, oaObsRefused)

	holdRunner := oaRunner(pool, oaHealthyRPC())
	reqRefused := oaHoldReleaseRequest("oa-op-refused-1", senderRefused, "")
	if res := oaRun(t, holdRunner, reqRefused); res.Outcome != nonce.AdminRefused {
		t.Fatalf("hold-release without evidence outcome = %q, want refused", res.Outcome)
	}
	if outcome, subject, count := oaAuditRow(t, sqlDB, reqRefused.OperationID); count != 1 ||
		outcome != string(nonce.AdminRefused) || subject != oaHoldRefused {
		t.Fatalf("refusal audit = (%s, %s, count %d), want (refused, %s, 1)",
			outcome, subject, count, oaHoldRefused)
	}
	if !oaHoldStillActive(t, sqlDB) {
		t.Fatalf("refused release changed the hold")
	}
	// A committed `refused` is never upgraded on replay.
	if res := oaRun(t, holdRunner, reqRefused); res.Outcome != nonce.AdminRefused {
		t.Fatalf("replayed refusal outcome = %q, want the recorded refused", res.Outcome)
	}
	if _, _, count := oaAuditRow(t, sqlDB, reqRefused.OperationID); count != 1 {
		t.Fatalf("audit rows after replayed refusal = %d, want exactly 1", count)
	}
	if !oaHoldStillActive(t, sqlDB) {
		t.Fatalf("replayed refusal changed the hold")
	}

	// --- uncertain COMMIT: same-id retry converges on the recorded attempt --
	senderUnknown := nonceAddr("a5")
	unknownPool := oaUnknownCommitPool(ctx, t, dsn)
	unknownRegistry := nonce.NewAdminRunner(unknownPool, nil)
	reqUnknown := oaRequest(nonce.AdminActionRegistryRegister, "oa-op-unknown-1", senderUnknown, "register")

	if _, err := unknownRegistry.Run(ctx, reqUnknown); !nonce.IsOutcome(err, nonce.OutcomeTemporarilyUnavailable) {
		t.Fatalf("uncertain-COMMIT error = %v, want temporarily_unavailable", err)
	}
	// The durable commit landed despite the client-visible error.
	if outcome, subject, count := oaAuditRow(t, sqlDB, reqUnknown.OperationID); count != 1 ||
		outcome != string(nonce.AdminApplied) || subject != senderUnknown {
		t.Fatalf("durable audit after uncertain COMMIT = (%s, %s, count %d), want (applied, %s, 1)",
			outcome, subject, count, senderUnknown)
	}
	if _, seq, found := oaRegistry(t, sqlDB, senderUnknown); !found || seq != 1 {
		t.Fatalf("durable registry after uncertain COMMIT = (found %v, seq %d), want (true, 1)", found, seq)
	}

	// The same-id retry recomputes `nop` but must converge through the
	// recorded attempt: one audit row, no double write.
	if res := oaRun(t, unknownRegistry, reqUnknown); res.Outcome != nonce.AdminApplied {
		t.Fatalf("same-id retry outcome = %q, want the recorded applied", res.Outcome)
	}
	if _, _, count := oaAuditRow(t, sqlDB, reqUnknown.OperationID); count != 1 {
		t.Fatalf("audit rows after uncertain-COMMIT retry = %d, want exactly 1", count)
	}
	if state, seq, _ := oaRegistry(t, sqlDB, senderUnknown); state != "active" || seq != 1 {
		t.Fatalf("registry after uncertain-COMMIT retry = (%s, seq %d), want (active, 1)", state, seq)
	}
}
