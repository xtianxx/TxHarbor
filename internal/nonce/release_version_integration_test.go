//go:build integration

// release_version_integration_test.go owns spec task T032 for 008-nonce-manager
// (US5; contracts/observation.md §2.1/§3.1; V7): the evidence-version drift
// refusal over a real PostgreSQL. No Anvil is needed — the pre-tx chain view is
// a scripted JSON-RPC double (the T025 binding_release_integration_test.go
// technique), and the real `nonce_ops_audit` / hold / floor rows are the
// evidence.
//
// What it proves against the T-hold-release re-verify (data-model
// §Transaction catalog, "Any verification failure or version drift versus the
// operator's evidence → audit row (refused) + zero hold/floor change"):
//   - a foreign or missing `--observation-id` is `refused` with zero hold/floor
//     change: the operator's reference does not belong to the scope;
//   - a durable scope version that drifted between the operator's evidence and
//     the in-tx re-verify (a `last_pending` regression ⇒ the fresh view
//     classifies `divergence`) is `refused`, naming the fresh classification;
//   - a hold already released is a committed `nop` with zero change — never a
//     second release and never a rewrite of the terminal row (data-model
//     T-hold-release: "missing/already released → nop");
//   - the applied release records its evidence version (`registry_seq`,
//     `registry_state`, the active-hold count, the 006 state, and the fresh
//     `release_observation_id`) immutably: a later registry seq/state change is
//     never reinterpreted against it, while a NEW release re-reads the current
//     registry version under the lock;
//   - a re-detected cause creates a NEW hold instance (a fresh hold_id), never
//     a silent re-open of the released row.
//
// Every helper this file owns is `vd`-prefixed so it cannot collide with a
// sibling integration test; the container/migration helpers and
// `replayOpenPool` / `nonceAddr` / `nonceMustExec` are reused from
// migration_integration_test.go and allocate_replay_integration_test.go.
package nonce_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/nonce"
)

const (
	// vdChain is the dedicated deployment chain for this test.
	vdChain = int64(80032)
	// vdHeadHash is the 0x + 64 hex head identity every scripted view returns.
	vdHeadHash = "0x" + "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd"
	// vdAuth is the active 007 authorization the re-detected-cause Allocate
	// scenario binds.
	vdAuth = "wa-vd-1"
	// vdEvidence is the operator finding every attempt supplies.
	vdEvidence = "vd: operator finding; the consumption below the frontier was reconciled out of band"
)

// One dedicated sender scope per drift scenario keeps each case's holds, floor
// and observations independent.
var (
	vdSenderForeign  = nonceAddr("f1")
	vdSenderStale    = nonceAddr("f2")
	vdSenderReleased = nonceAddr("f3")
	vdSenderReg      = nonceAddr("f4")
	vdSenderNew      = nonceAddr("f5")
	vdSenderOther    = nonceAddr("f6")

	vdSenders = []string{
		vdSenderForeign, vdSenderStale, vdSenderReleased,
		vdSenderReg, vdSenderNew, vdSenderOther,
	}
)

// Seeded operator evidence references and hold ids.
const (
	vdObsForeignEvidence = "no-vd-foreign-evidence"
	vdObsOther           = "no-vd-other"
	vdObsStale           = "no-vd-stale"
	vdObsReleased        = "no-vd-released"
	vdObsReg             = "no-vd-reg"
	vdObsNewRef          = "no-vd-new-ref"

	vdHoldForeign  = "nh-vd-foreign"
	vdHoldStale    = "nh-vd-stale"
	vdHoldReleased = "nh-vd-released"
	vdHoldReg      = "nh-vd-reg"
	vdHoldReg2     = "nh-vd-reg-2"
)

// vdCaller is the raw JSON-RPC surface the Observer needs; *gethrpc.Client and
// the scripted vdRPC below satisfy the unexported rpcCaller NewObserver takes.
type vdCaller interface {
	CallContext(ctx context.Context, result any, method string, args ...any) error
}

// vdRPC is a scripted chain view. latest/pending are JSON string literals
// because the observer unmarshals the raw quantity.
type vdRPC struct {
	latest  string
	pending string
}

// vdHealthy is the consistent view (latest == pending == 0).
func vdHealthy() vdRPC { return vdRPC{latest: `"0x0"`, pending: `"0x0"`} }

// vdHeld is the pending-only-above-frontier view (P=2, L=0): with a durable
// floor of 1 it classifies `unexplained_gap` and establishes a hold.
func vdHeld() vdRPC { return vdRPC{latest: `"0x0"`, pending: `"0x2"`} }

func (r vdRPC) CallContext(_ context.Context, result any, method string, args ...any) error {
	switch method {
	case "eth_getTransactionCount":
		if len(args) != 2 {
			return fmt.Errorf("vdRPC: eth_getTransactionCount got %d args, want 2", len(args))
		}
		tag, _ := args[1].(string)
		switch tag {
		case "latest":
			return json.Unmarshal([]byte(r.latest), result)
		case "pending":
			return json.Unmarshal([]byte(r.pending), result)
		default:
			return fmt.Errorf("vdRPC: unexpected block tag %q", tag)
		}
	case "eth_getBlockByNumber":
		payload := fmt.Sprintf(`{"number":"0x0","hash":%q}`, vdHeadHash)
		return json.Unmarshal([]byte(payload), result)
	default:
		return fmt.Errorf("vdRPC: unexpected method %q", method)
	}
}

// vdObserver wires one scripted caller with the sibling integration tests'
// timing knobs.
func vdObserver(caller vdCaller) *nonce.Observer {
	return nonce.NewObserver(caller, nonce.ObserverConfig{
		RPCTimeout:   2 * time.Second,
		RetryInitial: time.Millisecond,
		RetryMax:     5 * time.Millisecond,
	})
}

func vdRunner(pool *pgxpool.Pool, caller vdCaller) *nonce.AdminRunner {
	return nonce.NewAdminRunner(pool, vdObserver(caller))
}

func vdAllocator(pool *pgxpool.Pool, caller vdCaller) *nonce.Allocator {
	admitGate := nonce.NewRebuildGate()
	admitGate.Open()
	return nonce.NewAllocator(pool, vdObserver(caller), admitGate)
}

// vdRequest is one hold-release attempt.
func vdRequest(opID, sender, holdID, obsID, evidence string) nonce.AdminRequest {
	return nonce.AdminRequest{
		Action:        nonce.AdminActionHoldRelease,
		OperationID:   opID,
		ChainID:       vdChain,
		Sender:        sender,
		HoldID:        holdID,
		ObservationID: obsID,
		Evidence:      evidence,
		Operator:      "vd-operator",
		Reason:        "vd evidence-version drift",
	}
}

// vdAllocRequest is one admission attempt for the re-detected-cause scenario.
func vdAllocRequest(intentID, sender string) nonce.AllocationRequest {
	return nonce.AllocationRequest{
		IntentID:        intentID,
		ChainID:         vdChain,
		Sender:          sender,
		AuthorizationID: vdAuth,
	}
}

// vdSeed creates the 007 caller/authorization rows, one registry + scope row
// per scenario sender, the durable version drift inputs, and the active holds
// and operator evidence references the refusal scenarios reference.
func vdSeed(t *testing.T, sqlDB *sql.DB) {
	t.Helper()
	nonceMustExec(t, sqlDB, `INSERT INTO caller (caller_id, label) VALUES (1, 'vd-it')`)
	nonceMustExec(t, sqlDB, `INSERT INTO withdrawal_authorizations
		(authorization_id, caller_id, chain_id, asset, recipient, amount, state)
		VALUES ($1, 1, $2, $3, $4, 1, 'active')`, vdAuth, vdChain, nonceAddr("cc"), nonceAddr("dd"))

	for _, sender := range vdSenders {
		nonceMustExec(t, sqlDB, `INSERT INTO nonce_wallet_registry
			(chain_id, sender, state, registry_seq) VALUES ($1, $2, 'active', 1)`, vdChain, sender)
		nonceMustExec(t, sqlDB, `INSERT INTO nonce_scope_state (chain_id, sender) VALUES ($1, $2)`, vdChain, sender)
	}

	// Stale scope: the durable last_pending (5) sits above the fresh view
	// (P=0), so the in-tx re-verify classifies a regression (divergence).
	nonceMustExec(t, sqlDB, `UPDATE nonce_scope_state SET last_pending = 5
		WHERE chain_id = $1 AND sender = $2`, vdChain, vdSenderStale)
	// Re-detected cause: a durable floor makes a pending above it an
	// unexplained_gap rather than a healthy bootstrap.
	nonceMustExec(t, sqlDB, `UPDATE nonce_scope_state SET reconciled_floor = 1
		WHERE chain_id = $1 AND sender = $2`, vdChain, vdSenderNew)

	// Operator-attested in-scope evidence references plus one foreign one.
	vdInsertObservation(t, sqlDB, vdObsForeignEvidence, vdSenderForeign)
	vdInsertObservation(t, sqlDB, vdObsOther, vdSenderOther)
	vdInsertObservation(t, sqlDB, vdObsStale, vdSenderStale)
	vdInsertObservation(t, sqlDB, vdObsReleased, vdSenderReleased)
	vdInsertObservation(t, sqlDB, vdObsReg, vdSenderReg)
	vdInsertObservation(t, sqlDB, vdObsNewRef, vdSenderNew)

	// The active holds the re-verify reads.
	vdInsertHold(t, sqlDB, vdHoldForeign, vdSenderForeign, nonce.CauseUnexplainedGap, vdObsForeignEvidence)
	vdInsertHold(t, sqlDB, vdHoldStale, vdSenderStale, nonce.CauseUnexplainedGap, vdObsStale)
	vdInsertHold(t, sqlDB, vdHoldReleased, vdSenderReleased, nonce.CauseUnattributedConsumption, vdObsReleased)
	vdInsertHold(t, sqlDB, vdHoldReg, vdSenderReg, nonce.CauseChainViewDivergence, vdObsReg)
}

// vdInsertObservation persists one operator-attested evidence row.
func vdInsertObservation(t *testing.T, sqlDB *sql.DB, obsID, sender string) {
	t.Helper()
	nonceMustExec(t, sqlDB, `INSERT INTO nonce_observations
		(observation_id, chain_id, sender, kind, classification,
		 latest_count, pending_count, head_number, head_hash, error_class, rpc_ref)
		VALUES ($1, $2, $3, 'reconcile', 'consistent', 0, 0, 0, $4, '',
		        'vd:operator-reference')`,
		obsID, vdChain, sender, vdHeadHash)
}

// vdInsertHold persists one active hold instance with a chosen identity and
// evidence observation.
func vdInsertHold(t *testing.T, sqlDB *sql.DB, holdID, sender, cause, evidenceObs string) {
	t.Helper()
	nonceMustExec(t, sqlDB, `INSERT INTO nonce_scope_holds
		(hold_id, chain_id, sender, cause, status, evidence_observation_id, evidence_detail)
		VALUES ($1, $2, $3, $4, 'active', $5, 'vd drift fixture')`,
		holdID, vdChain, sender, cause, evidenceObs)
}

// vdHoldRow is the comparable hold fact set (the release columns included, so
// a pure status write is observable).
type vdHoldRow struct {
	holdID          string
	status          string
	cause           string
	evidenceObs     string
	releasedAt      sql.NullTime
	releasedBy      sql.NullString
	releaseOp       sql.NullString
	releaseEvidence sql.NullString
	releaseObs      sql.NullString
}

const vdHoldColumns = `hold_id, status, cause, evidence_observation_id,
	released_at, released_by, release_operation_id, release_evidence, release_observation_id`

func vdScanHold(scan func(...any) error) (vdHoldRow, error) {
	var h vdHoldRow
	err := scan(&h.holdID, &h.status, &h.cause, &h.evidenceObs,
		&h.releasedAt, &h.releasedBy, &h.releaseOp, &h.releaseEvidence, &h.releaseObs)
	return h, err
}

func vdReadHold(t *testing.T, sqlDB *sql.DB, holdID string) vdHoldRow {
	t.Helper()
	h, err := vdScanHold(sqlDB.QueryRow(
		`SELECT `+vdHoldColumns+` FROM nonce_scope_holds WHERE hold_id = $1`, holdID).Scan)
	if err != nil {
		t.Fatalf("read hold %q: %v", holdID, err)
	}
	return h
}

// vdSingleActiveHold returns the scope's one active hold (the re-detected-cause
// scenario asserts the instance identity).
func vdSingleActiveHold(t *testing.T, sqlDB *sql.DB, sender string) vdHoldRow {
	t.Helper()
	rows, err := sqlDB.Query(`SELECT `+vdHoldColumns+` FROM nonce_scope_holds
		WHERE chain_id = $1 AND sender = $2 AND status = 'active'`, vdChain, sender)
	if err != nil {
		t.Fatalf("query active holds: %v", err)
	}
	defer rows.Close()
	var (
		found vdHoldRow
		n     int
	)
	for rows.Next() {
		h, err := vdScanHold(rows.Scan)
		if err != nil {
			t.Fatalf("scan active hold: %v", err)
		}
		found = h
		n++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate active holds: %v", err)
	}
	if n != 1 {
		t.Fatalf("active holds for %s = %d, want exactly 1", sender, n)
	}
	return found
}

// vdFloor reads the scope's durable reconciled floor (NULL when unset).
func vdFloor(t *testing.T, sqlDB *sql.DB, sender string) sql.NullString {
	t.Helper()
	var f sql.NullString
	if err := sqlDB.QueryRow(`SELECT reconciled_floor::text FROM nonce_scope_state
		WHERE chain_id = $1 AND sender = $2`, vdChain, sender).Scan(&f); err != nil {
		t.Fatalf("read floor for %s: %v", sender, err)
	}
	return f
}

// vdReadAudit reads one recorded attempt.
func vdReadAudit(t *testing.T, sqlDB *sql.DB, opID string) (action, outcome, subject, detail string) {
	t.Helper()
	if err := sqlDB.QueryRow(`SELECT action, outcome, subject_id, detail
		FROM nonce_ops_audit WHERE operation_id = $1`, opID).
		Scan(&action, &outcome, &subject, &detail); err != nil {
		t.Fatalf("read audit %q: %v", opID, err)
	}
	return action, outcome, subject, detail
}

// vdCensus is the whole-DB row census (the container is dedicated to this
// test): a refusal/nop adds exactly one audit row and nothing else.
type vdCensus struct {
	holds, observations, audits, bindings, scopes int
}

func vdCount(t *testing.T, sqlDB *sql.DB) vdCensus {
	t.Helper()
	var c vdCensus
	if err := sqlDB.QueryRow(`SELECT
		(SELECT count(*) FROM nonce_scope_holds),
		(SELECT count(*) FROM nonce_observations),
		(SELECT count(*) FROM nonce_ops_audit),
		(SELECT count(*) FROM nonce_bindings),
		(SELECT count(*) FROM nonce_scope_state)`).
		Scan(&c.holds, &c.observations, &c.audits, &c.bindings, &c.scopes); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	return c
}

// vdWantCommitted asserts a committed non-applied attempt (refused or nop): the
// audit names the hold, the hold and floor are untouched, and the only new row
// is the audit itself.
func vdWantCommitted(t *testing.T, sqlDB *sql.DB, runner *nonce.AdminRunner, req nonce.AdminRequest,
	want nonce.Outcome, before vdCensus, holdBefore vdHoldRow, floorBefore sql.NullString) string {
	t.Helper()
	res, err := runner.Run(context.Background(), req)
	if err != nil {
		t.Fatalf("run %s: unexpected error %v", req.OperationID, err)
	}
	if res.Outcome != want {
		t.Fatalf("run %s outcome = %q, want %q (detail %q)", req.OperationID, res.Outcome, want, res.Detail)
	}
	action, outcome, subject, detail := vdReadAudit(t, sqlDB, req.OperationID)
	if action != nonce.AdminActionHoldRelease || outcome != string(want) || subject != req.HoldID {
		t.Fatalf("audit %s = (%s, %s, %s), want (hold_release, %s, %s)",
			req.OperationID, action, outcome, subject, want, req.HoldID)
	}
	if after := vdReadHold(t, sqlDB, req.HoldID); after != holdBefore {
		t.Fatalf("%s (%s) changed the hold: %+v -> %+v", req.OperationID, want, holdBefore, after)
	}
	if after := vdFloor(t, sqlDB, req.Sender); after != floorBefore {
		t.Fatalf("%s (%s) changed the floor: %v -> %v", req.OperationID, want, floorBefore, after)
	}
	after := vdCount(t, sqlDB)
	if after.audits != before.audits+1 ||
		after.holds != before.holds || after.observations != before.observations ||
		after.bindings != before.bindings || after.scopes != before.scopes {
		t.Fatalf("%s (%s) census = %+v, want %+v plus exactly one audit", req.OperationID, want, after, before)
	}
	return detail
}

// vdWantApplied asserts one applied release and returns the terminal row.
func vdWantApplied(t *testing.T, sqlDB *sql.DB, runner *nonce.AdminRunner, req nonce.AdminRequest) vdHoldRow {
	t.Helper()
	res, err := runner.Run(context.Background(), req)
	if err != nil {
		t.Fatalf("run %s: %v", req.OperationID, err)
	}
	if res.Outcome != nonce.AdminApplied {
		t.Fatalf("run %s outcome = %q, want applied (detail %q)", req.OperationID, res.Outcome, res.Detail)
	}
	action, outcome, subject, _ := vdReadAudit(t, sqlDB, req.OperationID)
	if action != nonce.AdminActionHoldRelease || outcome != string(nonce.AdminApplied) || subject != req.HoldID {
		t.Fatalf("audit %s = (%s, %s, %s), want (hold_release, applied, %s)",
			req.OperationID, action, outcome, subject, req.HoldID)
	}
	released := vdReadHold(t, sqlDB, req.HoldID)
	if released.status != nonce.HoldStatusReleased || !released.releasedAt.Valid ||
		!released.releaseOp.Valid || released.releaseOp.String != req.OperationID {
		t.Fatalf("hold %s after release = %+v, want terminal released with release_operation_id %s",
			req.HoldID, released, req.OperationID)
	}
	if released.holdID != req.HoldID || released.evidenceObs == "" {
		t.Fatalf("release rewrote immutable hold identity: %+v", released)
	}
	return released
}

// TestNonceReleaseVersionDriftIntegration is the T032 acceptance over one
// migrated scratch database.
func TestNonceReleaseVersionDriftIntegration(t *testing.T) {
	dsn := nonceStartPostgres(t)
	ctx := context.Background()

	var out bytes.Buffer
	if err := db.MigrateUp(ctx, nonceMigrateOptions(dsn), &out); err != nil {
		t.Fatalf("MigrateUp() error = %v (output %q)", err, out.String())
	}
	sqlDB := nonceOpenSQL(t, dsn)
	vdSeed(t, sqlDB)

	pool := replayOpenPool(t, dsn)
	healthy := vdRunner(pool, vdHealthy())

	// --- A. foreign / missing --observation-id → refused, zero change ------
	holdForeign := vdReadHold(t, sqlDB, vdHoldForeign)
	floorForeign := vdFloor(t, sqlDB, vdSenderForeign)
	for i, tc := range []struct {
		name  string
		obsID string
	}{
		{"foreign scope", vdObsOther},
		{"missing reference", "no-vd-does-not-exist"},
	} {
		opID := fmt.Sprintf("op-vd-foreign-%d", i+1)
		before := vdCount(t, sqlDB)
		detail := vdWantCommitted(t, sqlDB, healthy,
			vdRequest(opID, vdSenderForeign, vdHoldForeign, tc.obsID, vdEvidence),
			nonce.AdminRefused, before, holdForeign, floorForeign)
		if !strings.Contains(detail, "outside the scope") {
			t.Fatalf("%s refusal detail = %q, want the observation-scope cause", tc.name, detail)
		}
	}

	// --- B. durable scope version drift → refused, zero change -------------
	// The operator's evidence asserts a consistent scope, but the durable
	// last_pending (5) now exceeds the fresh pending (0): the in-tx re-verify
	// classifies a regression and refuses.
	holdStale := vdReadHold(t, sqlDB, vdHoldStale)
	floorStale := vdFloor(t, sqlDB, vdSenderStale)
	beforeStale := vdCount(t, sqlDB)
	staleDetail := vdWantCommitted(t, sqlDB, healthy,
		vdRequest("op-vd-stale-1", vdSenderStale, vdHoldStale, vdObsStale, vdEvidence),
		nonce.AdminRefused, beforeStale, holdStale, floorStale)
	if !strings.Contains(staleDetail, nonce.ClassificationDivergence) {
		t.Fatalf("stale-version refusal detail = %q, want the fresh %s classification",
			staleDetail, nonce.ClassificationDivergence)
	}

	// --- C. an already released hold → committed nop, zero change ----------
	firstRelease := vdWantApplied(t, sqlDB, healthy,
		vdRequest("op-vd-released-1", vdSenderReleased, vdHoldReleased, vdObsReleased, vdEvidence))
	if !strings.Contains(firstRelease.releaseEvidence.String, "registry_seq=1") ||
		!strings.Contains(firstRelease.releaseEvidence.String, "registry_state=active") {
		t.Fatalf("applied release evidence = %q, want the recorded evidence version", firstRelease.releaseEvidence.String)
	}
	if !firstRelease.releaseObs.Valid || !strings.HasPrefix(firstRelease.releaseObs.String, "no-") {
		t.Fatalf("release_observation_id = %v, want the fresh persisted observation", firstRelease.releaseObs)
	}
	floorReleased := vdFloor(t, sqlDB, vdSenderReleased)
	beforeNop := vdCount(t, sqlDB)
	nopDetail := vdWantCommitted(t, sqlDB, healthy,
		vdRequest("op-vd-released-2", vdSenderReleased, vdHoldReleased, vdObsReleased, vdEvidence),
		nonce.AdminNop, beforeNop, firstRelease, floorReleased)
	if !strings.Contains(nopDetail, "already released") {
		t.Fatalf("already-released detail = %q, want the nop cause", nopDetail)
	}

	// --- D. registry seq/state change is recorded, not reinterpreted -------
	firstReg := vdWantApplied(t, sqlDB, healthy,
		vdRequest("op-vd-reg-1", vdSenderReg, vdHoldReg, vdObsReg, vdEvidence))
	if !strings.Contains(firstReg.releaseEvidence.String, "registry_seq=1") ||
		!strings.Contains(firstReg.releaseEvidence.String, "registry_state=active") ||
		!strings.Contains(firstReg.releaseEvidence.String, "active_holds=1") {
		t.Fatalf("registry release evidence = %q, want registry + active-hold version", firstReg.releaseEvidence.String)
	}

	// The registry version changes after the applied release.
	nonceMustExec(t, sqlDB, `UPDATE nonce_wallet_registry
		SET state = 'disabled', registry_seq = 7
		WHERE chain_id = $1 AND sender = $2`, vdChain, vdSenderReg)
	if after := vdReadHold(t, sqlDB, vdHoldReg); after != firstReg {
		t.Fatalf("a later registry change reinterpreted the applied release: %+v -> %+v", firstReg, after)
	}

	// A NEW release re-reads the current registry version under the lock.
	vdInsertHold(t, sqlDB, vdHoldReg2, vdSenderReg, nonce.CauseUnexplainedGap, vdObsReg)
	secondReg := vdWantApplied(t, sqlDB, healthy,
		vdRequest("op-vd-reg-2", vdSenderReg, vdHoldReg2, vdObsReg, vdEvidence))
	if !strings.Contains(secondReg.releaseEvidence.String, "registry_seq=7") ||
		!strings.Contains(secondReg.releaseEvidence.String, "registry_state=disabled") {
		t.Fatalf("second registry release evidence = %q, want the freshly re-read registry version",
			secondReg.releaseEvidence.String)
	}
	if secondReg.releaseObs.String == firstReg.releaseObs.String {
		t.Fatalf("second release reused observation %q, want its own fresh observation", secondReg.releaseObs.String)
	}

	// --- E. a re-detected cause creates a NEW hold instance ----------------
	alloc := vdAllocator(pool, vdHeld())
	if held, outcome, err := alloc.Allocate(ctx, vdAllocRequest("vd-intent-1", vdSenderNew)); held != nil || outcome != nonce.OutcomeScopeHeld || !nonce.IsOutcome(err, nonce.OutcomeScopeHeld) {
		t.Fatalf("held-scope allocate = (%+v, %q, %v), want scope_held with no binding", held, outcome, err)
	}
	h1 := vdSingleActiveHold(t, sqlDB, vdSenderNew)
	if h1.cause != nonce.CauseUnexplainedGap {
		t.Fatalf("first hold cause = %q, want %q", h1.cause, nonce.CauseUnexplainedGap)
	}

	// The held admission above persisted its P=2 sample into the waterline
	// (admissions record last-observed facts like ticks do). No reconcile
	// loop runs in this test, so simulate the stabilizing tick production
	// would run before the release: without it no fresh view could ever
	// re-verify consistent against the spiked waterline.
	nonceMustExec(t, sqlDB, `UPDATE nonce_scope_state
		SET last_latest = 0, last_pending = 0 WHERE chain_id = $1 AND sender = $2`,
		vdChain, vdSenderNew)

	h1Released := vdWantApplied(t, sqlDB, healthy,
		vdRequest("op-vd-new-1", vdSenderNew, h1.holdID, vdObsNewRef, vdEvidence))

	if held, outcome, err := alloc.Allocate(ctx, vdAllocRequest("vd-intent-2", vdSenderNew)); held != nil || outcome != nonce.OutcomeScopeHeld || !nonce.IsOutcome(err, nonce.OutcomeScopeHeld) {
		t.Fatalf("re-detected allocate = (%+v, %q, %v), want scope_held with no binding", held, outcome, err)
	}
	h2 := vdSingleActiveHold(t, sqlDB, vdSenderNew)
	if h2.holdID == h1.holdID {
		t.Fatalf("re-detected cause reused the released hold instance %s", h1.holdID)
	}
	if h1Again := vdReadHold(t, sqlDB, h1.holdID); h1Again != h1Released {
		t.Fatalf("re-detection rewrote the released hold: %+v -> %+v", h1Released, h1Again)
	}
}
