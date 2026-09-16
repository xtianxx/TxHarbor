//go:build integration

// binding_release_integration_test.go owns spec task T025 for 008-nonce-manager
// (US3; contracts/observation.md §3.2; V4; SC-05): the operator
// `nonce-admin binding-release` disposition over a real PostgreSQL, with no
// Anvil needed (the chain view is a scripted JSON-RPC double).
//
// What it proves:
//   - only a non-terminal binding is disposable: a consumed binding is refused
//     and a terminal (already released) binding converges on `nop`;
//   - explicit no-side-effect evidence is REQUIRED: a bare timeout/connection
//     error alone is refused (US3-3) with zero hold/floor/binding change, and
//     so is an empty operator finding;
//   - a mined-beyond fresh view (`nonce < L`) is NOT releasable — that path
//     consumes, it never disposes;
//   - success is a terminal `released` + transition event + `applied` audit;
//     the released row is immutable (a second attempt never rewrites it) and
//     the terminal nonce is never reused: a later admission in the same scope
//     takes a different nonce, while a same-intent replacement stays on the
//     original binding (never reassigned);
//   - an in-flight item MAY remain in-flight: release with evidence resolves
//     it, and a refused release leaves it untouched;
//   - one assertion drives the real `internal/app` `binding-release` CLI
//     dispatch (in-process Deps) against the container, proving the wiring.
//
// The container/migration pattern is reused verbatim from
// migration_integration_test.go (nonceStartPostgres / nonceMigrateOptions /
// nonceOpenSQL / nonceMustExec / nonceAddr). Every helper this file owns is
// `rel`-prefixed so it cannot collide with a sibling integration test.
package nonce_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/app"
	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/nonce"
)

const (
	relChain    = int64(80011)
	relAuth     = "wa-rel-1"
	relAllocObs = "no-rel-alloc"
	relHeadHash = "0x" + "abababababababababababababababababababababababababababababababab"
)

// relAuthVersion is the 64-hex authorization-version digest the raw seed uses
// (the migration's nonce_bindings_authorization_version_check domain).
var relAuthVersion = strings.Repeat("a", 64)

// One dedicated sender scope per scenario keeps each case's bindings, scope
// row, holds and floor independent of the others.
var (
	relSenderRelease    = nonceAddr("a1")
	relSenderInFlight   = nonceAddr("a2")
	relSenderRefuse     = nonceAddr("a3")
	relSenderMined      = nonceAddr("a4")
	relSenderNoEvidence = nonceAddr("a5")
	relSenderConsumed   = nonceAddr("a6")
	relSenderCLI        = nonceAddr("a7")

	relSenders = []string{
		relSenderRelease, relSenderInFlight, relSenderRefuse, relSenderMined,
		relSenderNoEvidence, relSenderConsumed, relSenderCLI,
	}
)

// relCaller is the raw JSON-RPC surface the Observer needs; the exported
// signature matches the unexported rpcCaller interface NewObserver accepts.
type relCaller interface {
	CallContext(ctx context.Context, result any, method string, args ...any) error
}

// relRPC is the scripted healthy chain view (latest == pending == 0, head 0):
// the bound nonce was never mined and nothing is pending. The values are JSON
// literals because the observer's block-result type is package-private.
type relRPC struct {
	count    string // JSON string literal, e.g. `"0x0"`
	head     string
	headHash string
}

func relHealthyRPC() relRPC {
	return relRPC{count: `"0x0"`, head: "0x0", headHash: relHeadHash}
}

// relMinedRPC is the mined-beyond view for a nonce-0 binding: latest == pending
// == 1 means nonce 0 was consumed on chain (`nonce < L`), so release must be
// refused — that path belongs to reconcile, not disposition.
func relMinedRPC() relRPC {
	return relRPC{count: `"0x1"`, head: "0x1", headHash: relHeadHash}
}

func (r relRPC) CallContext(_ context.Context, result any, method string, args ...any) error {
	switch method {
	case "eth_getTransactionCount":
		return json.Unmarshal([]byte(r.count), result)
	case "eth_getBlockByNumber":
		payload := fmt.Sprintf(`{"number":%q,"hash":%q}`, r.head, r.headHash)
		return json.Unmarshal([]byte(payload), result)
	default:
		return fmt.Errorf("relRPC: unexpected method %q", method)
	}
}

// relFailRPC always fails: a timeout (context.DeadlineExceeded) or a
// transport/connection error. US3-3: neither is ever no-side-effect evidence.
type relFailRPC struct{ err error }

func (r relFailRPC) CallContext(context.Context, any, string, ...any) error { return r.err }

// relRunner wires one AdminRunner over the container pool with the scripted
// caller; the observer's timing knobs mirror the other integration tests.
func relRunner(pool *pgxpool.Pool, caller relCaller) *nonce.AdminRunner {
	return nonce.NewAdminRunner(pool, nonce.NewObserver(caller, nonce.ObserverConfig{
		RPCTimeout:   2 * time.Second,
		RetryInitial: time.Millisecond,
		RetryMax:     5 * time.Millisecond,
	}))
}

// relAllocator wires the Allocator with an explicitly opened gate over the
// same healthy view, used for the never-reused/replacement assertions.
func relAllocator(pool *pgxpool.Pool) *nonce.Allocator {
	admitGate := nonce.NewRebuildGate()
	admitGate.Open()
	return nonce.NewAllocator(pool, nonce.NewObserver(relHealthyRPC(), nonce.ObserverConfig{
		RPCTimeout:   2 * time.Second,
		RetryInitial: time.Millisecond,
		RetryMax:     5 * time.Millisecond,
	}), admitGate)
}

// relJSONRPCServer is a real HTTP JSON-RPC endpoint for the CLI dispatch test:
// the carrier dials cfg.RPCURL with the geth client, so the response must be a
// spec-shaped JSON-RPC envelope. count is the eth_getTransactionCount result.
func relJSONRPCServer(t *testing.T, count string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		var result any
		switch req.Method {
		case "eth_getTransactionCount":
			result = count
		case "eth_getBlockByNumber":
			result = map[string]string{"number": "0x0", "hash": relHeadHash}
		default:
			http.Error(w, "unknown method", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": result})
	}))
	t.Cleanup(srv.Close)
	return srv
}

// relSeed creates the 007 caller/authorization rows the re-admission phase
// needs plus one registry + scope row per scenario sender (lockScopeRowTx
// requires the scope row to exist).
func relSeed(t *testing.T, sqlDB *sql.DB) {
	t.Helper()
	nonceMustExec(t, sqlDB, `INSERT INTO caller (caller_id, label) VALUES (1, 'rel-it')`)
	nonceMustExec(t, sqlDB, `INSERT INTO withdrawal_authorizations
		(authorization_id, caller_id, chain_id, asset, recipient, amount, state)
		VALUES ($1, 1, $2, $3, $4, 1, 'active')`, relAuth, relChain, nonceAddr("cc"), nonceAddr("dd"))

	for _, sender := range relSenders {
		nonceMustExec(t, sqlDB, `INSERT INTO nonce_wallet_registry
			(chain_id, sender, state, registry_seq) VALUES ($1, $2, 'active', 1)`, relChain, sender)
		nonceMustExec(t, sqlDB, `INSERT INTO nonce_scope_state (chain_id, sender) VALUES ($1, $2)`, relChain, sender)
	}
}

// relInsertBinding inserts one durable binding directly; consumed_at is set
// only for a consumed row so the terminal-consistency CHECK holds.
func relInsertBinding(t *testing.T, sqlDB *sql.DB, sender, bindingID, intentID, state, nonceText string) {
	t.Helper()
	consumedAt := "NULL"
	if state == nonce.StateConsumed {
		consumedAt = "now()"
	}
	query := fmt.Sprintf(`INSERT INTO nonce_bindings
		(binding_id, intent_id, chain_id, sender, nonce, state, authorization_id,
		 authorization_version, registry_seq, allocation_observation_id, consumed_at)
		VALUES ($1, $2, $3, $4, $5::numeric, $6, $7, $8, 1, $9, %s)`, consumedAt)
	nonceMustExec(t, sqlDB, query,
		bindingID, intentID, relChain, sender, nonceText, state, relAuth, relAuthVersion, relAllocObs)
}

// relAttestObservation persists the durable operator-attested fact the release
// evidence references (observation.md §3.2: the operator supplies a finding
// plus an in-scope observation id). Here the fact is a downstream
// confirmed-abandon for a nonce the chain never saw: consistent, latest ==
// pending == 0.
func relAttestObservation(t *testing.T, sqlDB *sql.DB, obsID, sender string) {
	t.Helper()
	nonceMustExec(t, sqlDB, `INSERT INTO nonce_observations
		(observation_id, chain_id, sender, kind, classification,
		 latest_count, pending_count, head_number, head_hash, error_class, rpc_ref)
		VALUES ($1, $2, $3, 'reconcile', 'consistent', 0, 0, 0, $4, '',
		        'rel:operator-attested:downstream-confirmed-abandon')`,
		obsID, relChain, sender, relHeadHash)
}

// relBindingRow is the comparable binding fact set (terminal columns included,
// created/updated timestamps excluded so a pure state write is still equal).
type relBindingRow struct {
	bindingID  string
	intentID   string
	state      string
	nonce      string
	releasedAt sql.NullTime
	releaseOp  sql.NullString
	consumedAt sql.NullTime
}

func relReadBinding(t *testing.T, sqlDB *sql.DB, bindingID string) relBindingRow {
	t.Helper()
	var b relBindingRow
	if err := sqlDB.QueryRow(`SELECT binding_id, intent_id, state, nonce::text,
		released_at, release_operation_id, consumed_at
		FROM nonce_bindings WHERE binding_id = $1`, bindingID).
		Scan(&b.bindingID, &b.intentID, &b.state, &b.nonce, &b.releasedAt, &b.releaseOp, &b.consumedAt); err != nil {
		t.Fatalf("read binding %q: %v", bindingID, err)
	}
	return b
}

// relFloor reads the scope's durable reconciled floor (NULL when unset); the
// refusal proof asserts it is untouched.
func relFloor(t *testing.T, sqlDB *sql.DB, sender string) sql.NullString {
	t.Helper()
	var f sql.NullString
	if err := sqlDB.QueryRow(`SELECT reconciled_floor::text FROM nonce_scope_state
		WHERE chain_id = $1 AND sender = $2`, relChain, sender).Scan(&f); err != nil {
		t.Fatalf("read reconciled floor: %v", err)
	}
	return f
}

// relRequest is one binding-release attempt with a caller-supplied subject and
// observation reference.
func relRequest(opID, sender, bindingID, obsID, evidence string) nonce.AdminRequest {
	return nonce.AdminRequest{
		Action:        nonce.AdminActionBindingRelease,
		OperationID:   opID,
		ChainID:       relChain,
		Sender:        sender,
		BindingID:     bindingID,
		ObservationID: obsID,
		Evidence:      evidence,
		Operator:      "rel-operator",
		Reason:        "binding disposition",
	}
}

// relReadAudit reads one recorded attempt for its outcome assertions.
func relReadAudit(t *testing.T, sqlDB *sql.DB, opID string) (action, outcome, subject, detail string) {
	t.Helper()
	if err := sqlDB.QueryRow(`SELECT action, outcome, subject_id, detail
		FROM nonce_ops_audit WHERE operation_id = $1`, opID).
		Scan(&action, &outcome, &subject, &detail); err != nil {
		t.Fatalf("read audit %q: %v", opID, err)
	}
	return action, outcome, subject, detail
}

// relCensus is the whole-DB row census used to prove zero domain side effect:
// a refusal adds exactly one audit row and nothing else.
type relCensus struct {
	bindings, events, observations, holds, audits int
}

func relCount(t *testing.T, sqlDB *sql.DB) relCensus {
	t.Helper()
	var c relCensus
	if err := sqlDB.QueryRow(`SELECT
		(SELECT count(*) FROM nonce_bindings),
		(SELECT count(*) FROM nonce_binding_events),
		(SELECT count(*) FROM nonce_observations),
		(SELECT count(*) FROM nonce_scope_holds),
		(SELECT count(*) FROM nonce_ops_audit)`).
		Scan(&c.bindings, &c.events, &c.observations, &c.holds, &c.audits); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	return c
}

// relWantRefused asserts a committed `refused` attempt: the audit names the
// binding, the op-input's binding is untouched, holds/floor are unchanged, and
// the only new row is the audit itself (observation.md §3.2 evidence standard).
func relWantRefused(t *testing.T, sqlDB *sql.DB, runner *nonce.AdminRunner, req nonce.AdminRequest, before relCensus, bindingBefore relBindingRow, floorBefore sql.NullString) string {
	t.Helper()
	res, err := runner.Run(context.Background(), req)
	if err != nil {
		t.Fatalf("run %s: unexpected error %v", req.OperationID, err)
	}
	if res.Outcome != nonce.AdminRefused {
		t.Fatalf("run %s outcome = %q, want refused (detail %q)", req.OperationID, res.Outcome, res.Detail)
	}
	action, outcome, subject, detail := relReadAudit(t, sqlDB, req.OperationID)
	if action != nonce.AdminActionBindingRelease || outcome != string(nonce.AdminRefused) || subject != req.BindingID {
		t.Fatalf("audit %s = (%s, %s, %s), want (binding_release, refused, %s)",
			req.OperationID, action, outcome, subject, req.BindingID)
	}
	if after := relReadBinding(t, sqlDB, req.BindingID); after != bindingBefore {
		t.Fatalf("refused release %s changed the binding: %+v -> %+v", req.OperationID, bindingBefore, after)
	}
	var holds int
	if err := sqlDB.QueryRow(`SELECT count(*) FROM nonce_scope_holds WHERE chain_id = $1 AND sender = $2`,
		relChain, req.Sender).Scan(&holds); err != nil {
		t.Fatalf("count holds: %v", err)
	}
	if holds != 0 {
		t.Fatalf("refused release %s established %d hold(s), want 0", req.OperationID, holds)
	}
	var floorAfter sql.NullString
	if err := sqlDB.QueryRow(`SELECT reconciled_floor::text FROM nonce_scope_state
		WHERE chain_id = $1 AND sender = $2`, relChain, req.Sender).Scan(&floorAfter); err != nil {
		t.Fatalf("read floor after refusal: %v", err)
	}
	if floorAfter != floorBefore {
		t.Fatalf("refused release %s changed the floor: %v -> %v", req.OperationID, floorBefore, floorAfter)
	}
	after := relCount(t, sqlDB)
	if after.audits != before.audits+1 ||
		after.bindings != before.bindings || after.events != before.events ||
		after.observations != before.observations || after.holds != before.holds {
		t.Fatalf("refused release %s census = %+v, want %+v plus exactly one audit", req.OperationID, after, before)
	}
	return detail
}

// TestNonceBindingReleaseIntegration is the T025 acceptance over one migrated
// scratch database with one scope per scenario.
func TestNonceBindingReleaseIntegration(t *testing.T) {
	dsn := nonceStartPostgres(t)
	ctx := context.Background()

	var out bytes.Buffer
	if err := db.MigrateUp(ctx, nonceMigrateOptions(dsn), &out); err != nil {
		t.Fatalf("MigrateUp() error = %v (output %q)", err, out.String())
	}
	sqlDB := nonceOpenSQL(t, dsn)
	relSeed(t, sqlDB)

	pool := replayOpenPool(t, dsn)
	healthy := relRunner(pool, relHealthyRPC())
	alloc := relAllocator(pool)

	// --- success: allocated -> released, event + applied audit -------------
	const (
		relReleaseID     = "rel-b-release"
		relReleaseOp     = "op-rel-release-1"
		relReleaseIntent = "rel-intent-release"
		relAttestRelease = "no-rel-attest-release"
		relEvidence      = "rel: explicit no-side-effect evidence - chain re-observation shows the nonce was never mined and no transaction is pending; downstream confirmed-abandon"
	)
	relInsertBinding(t, sqlDB, relSenderRelease, relReleaseID, relReleaseIntent, nonce.StateAllocated, "0")
	relAttestObservation(t, sqlDB, relAttestRelease, relSenderRelease)

	before := relReadBinding(t, sqlDB, relReleaseID)
	res, err := healthy.Run(ctx, relRequest(relReleaseOp, relSenderRelease, relReleaseID, relAttestRelease, relEvidence))
	if err != nil {
		t.Fatalf("release %s: %v", relReleaseOp, err)
	}
	if res.Outcome != nonce.AdminApplied {
		t.Fatalf("release %s outcome = %q, want applied (detail %q)", relReleaseOp, res.Outcome, res.Detail)
	}
	released := relReadBinding(t, sqlDB, relReleaseID)
	if released.state != nonce.StateReleased || !released.releasedAt.Valid ||
		!released.releaseOp.Valid || released.releaseOp.String != relReleaseOp {
		t.Fatalf("binding after release = %+v, want terminal released with release_operation_id %s", released, relReleaseOp)
	}
	if released.bindingID != before.bindingID || released.intentID != before.intentID || released.nonce != before.nonce {
		t.Fatalf("release rewrote immutable binding identity: %+v -> %+v", before, released)
	}
	action, outcome, subject, _ := relReadAudit(t, sqlDB, relReleaseOp)
	if action != "binding_release" || outcome != "applied" || subject != relReleaseID {
		t.Fatalf("applied audit = (%s, %s, %s), want (binding_release, applied, %s)", action, outcome, subject, relReleaseID)
	}
	var eventFrom sql.NullString
	var eventTo, eventOp string
	if err := sqlDB.QueryRow(`SELECT from_state, to_state, operation_id
		FROM nonce_binding_events WHERE binding_id = $1 AND to_state = 'released'`, relReleaseID).
		Scan(&eventFrom, &eventTo, &eventOp); err != nil {
		t.Fatalf("read release event: %v", err)
	}
	if !eventFrom.Valid || eventFrom.String != nonce.StateAllocated || eventTo != nonce.StateReleased || eventOp != relReleaseOp {
		t.Fatalf("release event = (%v, %s, %s), want (allocated, released, %s)", eventFrom, eventTo, eventOp, relReleaseOp)
	}

	// The released row is immutable: a second attempt is `nop` and never
	// rewrites release_operation_id (terminal is terminal).
	const relReleaseOp2 = "op-rel-release-2"
	res2, err := healthy.Run(ctx, relRequest(relReleaseOp2, relSenderRelease, relReleaseID, relAttestRelease, relEvidence))
	if err != nil {
		t.Fatalf("second release %s: %v", relReleaseOp2, err)
	}
	if res2.Outcome != nonce.AdminNop {
		t.Fatalf("second release outcome = %q, want nop", res2.Outcome)
	}
	if _, outcome, _, _ := relReadAudit(t, sqlDB, relReleaseOp2); outcome != string(nonce.AdminNop) {
		t.Fatalf("second release audit outcome = %q, want nop", outcome)
	}
	if again := relReadBinding(t, sqlDB, relReleaseID); again != released {
		t.Fatalf("second release rewrote the terminal row: %+v -> %+v", released, again)
	}

	// Same intent: the replacement stays on the original binding (never
	// reassigned), even though that binding is terminal.
	replacement, replacementOutcome, err := alloc.Allocate(ctx, nonce.AllocationRequest{
		IntentID: relReleaseIntent, ChainID: relChain, Sender: relSenderRelease, AuthorizationID: relAuth,
	})
	if err != nil || replacementOutcome != nonce.OutcomeReplayed {
		t.Fatalf("same-intent replacement = (%+v, %q, %v), want replay of the original binding", replacement, replacementOutcome, err)
	}
	if replacement == nil || replacement.BindingID != relReleaseID || replacement.Nonce.String() != "0" {
		t.Fatalf("replacement = %+v, want the original %s/nonce 0 (never reassigned)", replacement, relReleaseID)
	}

	// A fresh admission in the same scope gets a DIFFERENT nonce: the released
	// nonce 0 is still occupied by the durable binding row and is never reused.
	fresh, freshOutcome, err := alloc.Allocate(ctx, nonce.AllocationRequest{
		IntentID: "rel-intent-release-next", ChainID: relChain, Sender: relSenderRelease, AuthorizationID: relAuth,
	})
	if err != nil || freshOutcome != nonce.OutcomeAllocated {
		t.Fatalf("fresh admission = (%+v, %q, %v), want a new binding", fresh, freshOutcome, err)
	}
	if fresh == nil || fresh.Nonce.String() != "1" {
		t.Fatalf("fresh admission nonce = %v, want 1 (the released nonce 0 is never reused)", fresh)
	}
	if fresh.BindingID == relReleaseID {
		t.Fatalf("fresh admission reused the released binding id %s", relReleaseID)
	}
	if kept := relReadBinding(t, sqlDB, relReleaseID); kept != released {
		t.Fatalf("fresh admission rewrote the released row: %+v -> %+v", released, kept)
	}

	// --- in-flight: releasable with explicit evidence ----------------------
	const (
		relInflightID     = "rel-b-inflight"
		relInflightOp     = "op-rel-inflight-1"
		relAttestInflight = "no-rel-attest-inflight"
	)
	relInsertBinding(t, sqlDB, relSenderInFlight, relInflightID, "rel-intent-inflight", nonce.StateInFlight, "0")
	relAttestObservation(t, sqlDB, relAttestInflight, relSenderInFlight)
	resIn, err := healthy.Run(ctx, relRequest(relInflightOp, relSenderInFlight, relInflightID, relAttestInflight, relEvidence))
	if err != nil {
		t.Fatalf("in-flight release: %v", err)
	}
	if resIn.Outcome != nonce.AdminApplied {
		t.Fatalf("in-flight release outcome = %q, want applied", resIn.Outcome)
	}
	if got := relReadBinding(t, sqlDB, relInflightID); got.state != nonce.StateReleased {
		t.Fatalf("in-flight binding state = %q, want released", got.state)
	}
	if err := sqlDB.QueryRow(`SELECT from_state FROM nonce_binding_events
		WHERE binding_id = $1 AND to_state = 'released'`, relInflightID).Scan(&eventFrom); err != nil {
		t.Fatalf("read in-flight release event: %v", err)
	}
	if !eventFrom.Valid || eventFrom.String != nonce.StateInFlight {
		t.Fatalf("in-flight release event from_state = %v, want in_flight", eventFrom)
	}

	// --- US3-3: a timeout/connection error alone is never evidence ---------
	const (
		relRefuseID     = "rel-b-refuse"
		relAttestRefuse = "no-rel-attest-refuse"
	)
	relInsertBinding(t, sqlDB, relSenderRefuse, relRefuseID, "rel-intent-refuse", nonce.StateInFlight, "0")
	relAttestObservation(t, sqlDB, relAttestRefuse, relSenderRefuse)
	refuseBefore := relReadBinding(t, sqlDB, relRefuseID)
	refuseFloor := relFloor(t, sqlDB, relSenderRefuse)
	bareErrors := []struct {
		name   string
		caller relCaller
	}{
		{"timeout", relFailRPC{context.DeadlineExceeded}},
		{"connection error", relFailRPC{errors.New("dial tcp: connect: connection refused")}},
	}
	for i, tc := range bareErrors {
		opID := fmt.Sprintf("op-rel-refuse-%d", i+1)
		beforeRefuse := relCount(t, sqlDB)
		detail := relWantRefused(t, sqlDB, relRunner(pool, tc.caller),
			relRequest(opID, relSenderRefuse, relRefuseID, relAttestRefuse, relEvidence),
			beforeRefuse, refuseBefore, refuseFloor)
		if !strings.Contains(detail, "no-side-effect") {
			t.Fatalf("%s refusal detail = %q, want the no-side-effect evidence standard", tc.name, detail)
		}
	}
	// The in-flight item MAY simply remain in-flight: it is still non-terminal.
	if kept := relReadBinding(t, sqlDB, relRefuseID); kept.state != nonce.StateInFlight {
		t.Fatalf("refused in-flight binding state = %q, want in_flight (release is not forced)", kept.state)
	}

	// --- a mined-beyond view is NOT releasable (that path consumes) --------
	const (
		relMinedID     = "rel-b-mined"
		relAttestMined = "no-rel-attest-mined"
	)
	relInsertBinding(t, sqlDB, relSenderMined, relMinedID, "rel-intent-mined", nonce.StateAllocated, "0")
	relAttestObservation(t, sqlDB, relAttestMined, relSenderMined)
	minedBefore := relReadBinding(t, sqlDB, relMinedID)
	minedFloor := relFloor(t, sqlDB, relSenderMined)
	beforeMinedCensus := relCount(t, sqlDB)
	minedDetail := relWantRefused(t, sqlDB, relRunner(pool, relMinedRPC()),
		relRequest("op-rel-mined-1", relSenderMined, relMinedID, relAttestMined, relEvidence),
		beforeMinedCensus, minedBefore, minedFloor)
	if !strings.Contains(minedDetail, "external side effect") {
		t.Fatalf("mined-beyond refusal detail = %q, want the external-side-effect cause", minedDetail)
	}

	// --- an empty operator finding is refused ------------------------------
	const (
		relNoEvidenceID     = "rel-b-noevidence"
		relAttestNoEvidence = "no-rel-attest-noevidence"
	)
	relInsertBinding(t, sqlDB, relSenderNoEvidence, relNoEvidenceID, "rel-intent-noevidence", nonce.StateAllocated, "0")
	relAttestObservation(t, sqlDB, relAttestNoEvidence, relSenderNoEvidence)
	noEvidenceBefore := relReadBinding(t, sqlDB, relNoEvidenceID)
	noEvidenceFloor := relFloor(t, sqlDB, relSenderNoEvidence)
	beforeNoEvidence := relCount(t, sqlDB)
	noEvidenceDetail := relWantRefused(t, sqlDB, healthy,
		relRequest("op-rel-noevidence-1", relSenderNoEvidence, relNoEvidenceID, relAttestNoEvidence, ""),
		beforeNoEvidence, noEvidenceBefore, noEvidenceFloor)
	if !strings.Contains(noEvidenceDetail, "explicit no-side-effect evidence") {
		t.Fatalf("empty-evidence refusal detail = %q, want the explicit-evidence requirement", noEvidenceDetail)
	}

	// --- a consumed binding is terminal and never disposed -----------------
	const (
		relConsumedID     = "rel-b-consumed"
		relAttestConsumed = "no-rel-attest-consumed"
	)
	relInsertBinding(t, sqlDB, relSenderConsumed, relConsumedID, "rel-intent-consumed", nonce.StateConsumed, "0")
	relAttestObservation(t, sqlDB, relAttestConsumed, relSenderConsumed)
	consumedBefore := relReadBinding(t, sqlDB, relConsumedID)
	consumedFloor := relFloor(t, sqlDB, relSenderConsumed)
	beforeConsumed := relCount(t, sqlDB)
	consumedDetail := relWantRefused(t, sqlDB, healthy,
		relRequest("op-rel-consumed-1", relSenderConsumed, relConsumedID, relAttestConsumed, relEvidence),
		beforeConsumed, consumedBefore, consumedFloor)
	if !strings.Contains(consumedDetail, "consumed") {
		t.Fatalf("consumed refusal detail = %q, want the consumed-never-released cause", consumedDetail)
	}

	// --- real CLI dispatch: `nonce-admin binding-release` ------------------
	const (
		relCLIOp      = "op-rel-cli-1"
		relAttestCLI  = "no-rel-attest-cli"
		relCLIBinding = "rel-b-cli"
	)
	relInsertBinding(t, sqlDB, relSenderCLI, relCLIBinding, "rel-intent-cli", nonce.StateAllocated, "0")
	relAttestObservation(t, sqlDB, relAttestCLI, relSenderCLI)

	rpcSrv := relJSONRPCServer(t, "0x0")
	env := map[string]string{
		config.EnvPGDSN:                 dsn,
		config.EnvRPCURL:                rpcSrv.URL,
		config.EnvChainID:               strconv.FormatInt(relChain, 10),
		config.EnvStartHeight:           "0",
		config.EnvLogStartHeight:        "0",
		config.EnvLogContracts:          nonceAddr("bb"),
		config.EnvDepositStartHeight:    "0",
		config.EnvDepositContracts:      nonceAddr("bb"),
		config.EnvDepositWatchAddresses: nonceAddr("bb"),
		config.EnvConfirmationDepth:     "12",
	}
	var cliOut, cliErr bytes.Buffer
	deps := app.Deps{
		Getenv: func(k string) (string, bool) { v, ok := env[k]; return v, ok },
		Stdout: &cliOut,
		Stderr: &cliErr,
	}
	cliArgs := []string{
		"binding-release",
		"--operation-id", relCLIOp,
		"--binding-id", relCLIBinding,
		"--chain-id", strconv.FormatInt(relChain, 10),
		"--sender", relSenderCLI,
		"--observation-id", relAttestCLI,
		"--evidence", relEvidence,
		"--operator", "rel-operator",
		"--reason", "cli disposition",
	}
	if code := app.NonceAdmin(ctx, cliArgs, deps); code != 0 {
		t.Fatalf("nonce-admin binding-release exit = %d, want 0 (stderr %q)", code, cliErr.String())
	}
	if !strings.Contains(cliOut.String(), "outcome=applied") || !strings.Contains(cliOut.String(), "action=binding_release") {
		t.Fatalf("nonce-admin stdout = %q, want an applied binding_release report", cliOut.String())
	}
	cliAfter := relReadBinding(t, sqlDB, relCLIBinding)
	if cliAfter.state != nonce.StateReleased || !cliAfter.releaseOp.Valid || cliAfter.releaseOp.String != relCLIOp {
		t.Fatalf("CLI release binding = %+v, want terminal released with release_operation_id %s", cliAfter, relCLIOp)
	}
}
