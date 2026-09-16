//go:build integration

// hold_release_integration_test.go owns spec task T031 for 008-nonce-manager
// (US5; contracts/observation.md §3.1/§4; V7; FR-08; SC-07): the operator
// `nonce-admin hold-release` path over a real PostgreSQL.
//
// What it proves, per the T031 acceptance text:
//   - insufficient evidence (a bare operator finding is missing, or the
//     observation reference is absent / outside the scope) is a committed
//     `refused` audit with zero hold/floor change (observation.md §3.1);
//   - a cause still present at the in-tx re-verify point (the fresh
//     observation still classifies divergence) is likewise `refused` with zero
//     hold/floor change — release is never automatic;
//   - a valid release clears ONLY the named hold and advances the durable
//     `reconciled_floor` to the observed pending count; a coexisting active
//     hold survives untouched (§4 multi-cause coexistence);
//   - `nonce-admin status` lists the active holds with their causes, evidence
//     and the scope floor (§5), and stops listing the released hold.
//
// The chain view is a scripted JSON-RPC double for the transaction-library
// legs and a real HTTP JSON-RPC endpoint for the CLI leg (the
// binding_release_integration_test.go T025 precedent), so the test needs only
// a real PostgreSQL container. Every helper this file owns is `hr`-prefixed
// so it cannot collide with a sibling integration test.
package nonce_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
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
	// hrChain is the test's deployment chain id; hrAuth is the seeded 007
	// authorization the binding row references.
	hrChain = int64(80031)
	hrAuth  = "wa-hr-1"
	// hrHeadHash is the head identity the scripted/HTTP chain views report.
	hrHeadHash = "0x" + "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd"
)

// hrAuthVersion is the 64-hex authorization-version digest the raw binding
// seed uses (the migration's nonce_bindings_authorization_version_check).
var hrAuthVersion = strings.Repeat("b", 64)

// One dedicated sender scope per scenario keeps each case's holds, floor and
// bindings independent.
var (
	hrSenderNoEvidence = nonceAddr("b1")
	hrSenderCause      = nonceAddr("b2")
	hrSenderValid      = nonceAddr("b3")

	hrSenders = []string{hrSenderNoEvidence, hrSenderCause, hrSenderValid}
)

// hrCaller is the raw JSON-RPC surface the Observer needs; the exported
// signature matches the unexported rpcCaller interface NewObserver accepts.
type hrCaller interface {
	CallContext(ctx context.Context, result any, method string, args ...any) error
}

// hrRPC is a scripted chain view (latest == pending == count, head 0). The
// values are JSON literals because the observer's block-result type is
// package-private.
type hrRPC struct {
	count    string // JSON string literal, e.g. `"0x0"`
	head     string
	headHash string
}

func hrHealthyRPC() hrRPC {
	return hrRPC{count: `"0x0"`, head: "0x0", headHash: hrHeadHash}
}

func (r hrRPC) CallContext(_ context.Context, result any, method string, args ...any) error {
	switch method {
	case "eth_getTransactionCount":
		return json.Unmarshal([]byte(r.count), result)
	case "eth_getBlockByNumber":
		payload := fmt.Sprintf(`{"number":%q,"hash":%q}`, r.head, r.headHash)
		return json.Unmarshal([]byte(payload), result)
	default:
		return fmt.Errorf("hrRPC: unexpected method %q", method)
	}
}

// hrRunner wires one AdminRunner over the container pool with the scripted
// caller; the observer's timing knobs mirror the sibling integration tests.
func hrRunner(pool *pgxpool.Pool, caller hrCaller) *nonce.AdminRunner {
	return nonce.NewAdminRunner(pool, nonce.NewObserver(caller, nonce.ObserverConfig{
		RPCTimeout:   2 * time.Second,
		RetryInitial: time.Millisecond,
		RetryMax:     5 * time.Millisecond,
	}))
}

// hrJSONRPCServer is a real HTTP JSON-RPC endpoint for the CLI leg: the
// carrier dials cfg.RPCURL with the geth client, so the response must be a
// spec-shaped JSON-RPC envelope and must distinguish the latest/pending
// eth_getTransactionCount blocks (the re-verify classification depends on it).
func hrJSONRPCServer(t *testing.T, latest, pending string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage   `json:"id"`
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		var result any
		switch req.Method {
		case "eth_getTransactionCount":
			block := "latest"
			if len(req.Params) > 1 {
				_ = json.Unmarshal(req.Params[1], &block)
			}
			if block == "pending" {
				result = pending
			} else {
				result = latest
			}
		case "eth_getBlockByNumber":
			result = map[string]string{"number": "0x0", "hash": hrHeadHash}
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

// hrSeed creates the shared 007 caller/authorization plus one active registry
// row per scenario sender (lockScopeRowTx requires the scope row to exist, and
// the holds/bindings carry the registry FK).
func hrSeed(t *testing.T, sqlDB *sql.DB) {
	t.Helper()
	nonceMustExec(t, sqlDB, `INSERT INTO caller (caller_id, label) VALUES (1, 'hr-it')`)
	nonceMustExec(t, sqlDB, `INSERT INTO withdrawal_authorizations
		(authorization_id, caller_id, chain_id, asset, recipient, amount, state)
		VALUES ($1, 1, $2, $3, $4, 1, 'active')`, hrAuth, hrChain, nonceAddr("cc"), nonceAddr("dd"))
	for _, sender := range hrSenders {
		nonceMustExec(t, sqlDB, `INSERT INTO nonce_wallet_registry
			(chain_id, sender, state, registry_seq) VALUES ($1, $2, 'active', 1)`, hrChain, sender)
	}
}

// hrInsertScope inserts the durable scope row; an empty floor/lastPending is a
// real SQL NULL.
func hrInsertScope(t *testing.T, sqlDB *sql.DB, sender, floor, lastPending string) {
	t.Helper()
	nonceMustExec(t, sqlDB, `INSERT INTO nonce_scope_state
		(chain_id, sender, reconciled_floor, last_pending)
		VALUES ($1, $2, NULLIF($3, '')::numeric, NULLIF($4, '')::numeric)`,
		hrChain, sender, floor, lastPending)
}

// hrInsertObservation persists one evidence row the holds/release reference.
func hrInsertObservation(t *testing.T, sqlDB *sql.DB, obsID, sender, classification, latest, pending string) {
	t.Helper()
	nonceMustExec(t, sqlDB, `INSERT INTO nonce_observations
		(observation_id, chain_id, sender, kind, classification,
		 latest_count, pending_count, head_number, head_hash, error_class, rpc_ref)
		VALUES ($1, $2, $3, 'reconcile', $4, $5::numeric, $6::numeric, 0, $7, '',
		        'hr:operator-attested')`,
		obsID, hrChain, sender, classification, latest, pending, hrHeadHash)
}

// hrInsertHold establishes one active hold row with its evidence observation.
func hrInsertHold(t *testing.T, sqlDB *sql.DB, holdID, sender, cause, obsID string) {
	t.Helper()
	nonceMustExec(t, sqlDB, `INSERT INTO nonce_scope_holds
		(hold_id, chain_id, sender, cause, status, evidence_observation_id, evidence_detail)
		VALUES ($1, $2, $3, $4, 'active', $5, $4)`, holdID, hrChain, sender, cause, obsID)
}

// hrInsertBinding inserts one durable allocated binding (the scope frontier M).
func hrInsertBinding(t *testing.T, sqlDB *sql.DB, sender, bindingID, intentID, nonceText string) {
	t.Helper()
	nonceMustExec(t, sqlDB, `INSERT INTO nonce_bindings
		(binding_id, intent_id, chain_id, sender, nonce, state, authorization_id,
		 authorization_version, registry_seq, allocation_observation_id)
		VALUES ($1, $2, $3, $4, $5::numeric, 'allocated', $6, $7, 1, $8)`,
		bindingID, intentID, hrChain, sender, nonceText, hrAuth, hrAuthVersion, "no-hr-alloc")
}

// hrHold is the comparable hold fact set (timestamps excluded so a pure
// refusal leaves it byte-identical).
type hrHold struct {
	holdID      string
	cause       string
	status      string
	evidenceObs string
	releasedAt  sql.NullTime
	releaseOp   sql.NullString
	releaseObs  sql.NullString
}

func hrReadHold(t *testing.T, sqlDB *sql.DB, holdID string) hrHold {
	t.Helper()
	var h hrHold
	if err := sqlDB.QueryRow(`SELECT hold_id, cause, status, evidence_observation_id,
		released_at, release_operation_id, release_observation_id
		FROM nonce_scope_holds WHERE hold_id = $1`, holdID).
		Scan(&h.holdID, &h.cause, &h.status, &h.evidenceObs,
			&h.releasedAt, &h.releaseOp, &h.releaseObs); err != nil {
		t.Fatalf("read hold %q: %v", holdID, err)
	}
	return h
}

// hrFloor reads the scope's durable reconciled floor (NULL when unset).
func hrFloor(t *testing.T, sqlDB *sql.DB, sender string) sql.NullString {
	t.Helper()
	var f sql.NullString
	if err := sqlDB.QueryRow(`SELECT reconciled_floor::text FROM nonce_scope_state
		WHERE chain_id = $1 AND sender = $2`, hrChain, sender).Scan(&f); err != nil {
		t.Fatalf("read reconciled floor: %v", err)
	}
	return f
}

// hrCensus is the whole-DB row census used to prove a refusal adds exactly one
// audit row and nothing else.
type hrCensus struct {
	bindings, events, observations, holds, audits int
}

func hrCount(t *testing.T, sqlDB *sql.DB) hrCensus {
	t.Helper()
	var c hrCensus
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

// hrRequest is one hold-release attempt.
func hrRequest(opID, sender, holdID, obsID, evidence string) nonce.AdminRequest {
	return nonce.AdminRequest{
		Action:        nonce.AdminActionHoldRelease,
		OperationID:   opID,
		ChainID:       hrChain,
		Sender:        sender,
		HoldID:        holdID,
		ObservationID: obsID,
		Evidence:      evidence,
		Operator:      "hr-operator",
		Reason:        "hold release",
	}
}

// hrReadAudit reads one recorded attempt for its outcome assertions.
func hrReadAudit(t *testing.T, sqlDB *sql.DB, opID string) (action, outcome, subject, detail string) {
	t.Helper()
	if err := sqlDB.QueryRow(`SELECT action, outcome, subject_id, detail
		FROM nonce_ops_audit WHERE operation_id = $1`, opID).
		Scan(&action, &outcome, &subject, &detail); err != nil {
		t.Fatalf("read audit %q: %v", opID, err)
	}
	return action, outcome, subject, detail
}

// hrWantRefused asserts a committed `refused` attempt: the audit names the
// hold, the named hold and the floor are untouched, and the only new row is
// the audit itself (observation.md §3.1).
func hrWantRefused(t *testing.T, sqlDB *sql.DB, runner *nonce.AdminRunner, req nonce.AdminRequest,
	holdBefore hrHold, floorBefore sql.NullString, censusBefore hrCensus) string {
	t.Helper()
	res, err := runner.Run(context.Background(), req)
	if err != nil {
		t.Fatalf("run %s: unexpected error %v", req.OperationID, err)
	}
	if res.Outcome != nonce.AdminRefused {
		t.Fatalf("run %s outcome = %q, want refused (detail %q)", req.OperationID, res.Outcome, res.Detail)
	}
	action, outcome, subject, detail := hrReadAudit(t, sqlDB, req.OperationID)
	if action != nonce.AdminActionHoldRelease || outcome != string(nonce.AdminRefused) || subject != req.HoldID {
		t.Fatalf("audit %s = (%s, %s, %s), want (hold_release, refused, %s)",
			req.OperationID, action, outcome, subject, req.HoldID)
	}
	if after := hrReadHold(t, sqlDB, req.HoldID); after != holdBefore {
		t.Fatalf("refused release %s changed the named hold: %+v -> %+v", req.OperationID, holdBefore, after)
	}
	if after := hrFloor(t, sqlDB, req.Sender); after != floorBefore {
		t.Fatalf("refused release %s changed the floor: %v -> %v", req.OperationID, floorBefore, after)
	}
	after := hrCount(t, sqlDB)
	if after.audits != censusBefore.audits+1 ||
		after.bindings != censusBefore.bindings || after.events != censusBefore.events ||
		after.observations != censusBefore.observations || after.holds != censusBefore.holds {
		t.Fatalf("refused release %s census = %+v, want %+v plus exactly one audit", req.OperationID, after, censusBefore)
	}
	return detail
}

// TestNonceHoldReleaseIntegration is the T031 V7 acceptance over one migrated
// scratch database with one scope per scenario.
func TestNonceHoldReleaseIntegration(t *testing.T) {
	ctx := context.Background()
	dsn := nonceStartPostgres(t)

	var out bytes.Buffer
	if err := db.MigrateUp(ctx, nonceMigrateOptions(dsn), &out); err != nil {
		t.Fatalf("MigrateUp() error = %v (output %q)", err, out.String())
	}
	sqlDB := nonceOpenSQL(t, dsn)
	hrSeed(t, sqlDB)

	pool := replayOpenPool(t, dsn)
	healthy := hrRunner(pool, hrHealthyRPC())

	const (
		hrHoldNoEvidence = "hr-hold-noevidence"
		hrObsNoEvidence  = "no-hr-noevidence"
		hrHoldCause      = "hr-hold-cause"
		hrObsCause       = "no-hr-cause"
		hrHoldGap        = "hr-hold-gap"
		hrObsGap         = "no-hr-gap"
		hrHoldSurvivor   = "hr-hold-survivor"
		hrObsDiv         = "no-hr-div"
		hrBindingValid   = "hr-binding-valid"
	)

	// Scenario scopes. hrSenderCause stores last_pending above the live view,
	// so the fresh observation still classifies divergence at re-verify time.
	hrInsertScope(t, sqlDB, hrSenderNoEvidence, "", "")
	hrInsertScope(t, sqlDB, hrSenderCause, "", "5")
	hrInsertScope(t, sqlDB, hrSenderValid, "0", "")

	hrInsertObservation(t, sqlDB, hrObsNoEvidence, hrSenderNoEvidence, "consistent", "0", "0")
	hrInsertObservation(t, sqlDB, hrObsCause, hrSenderCause, "divergence", "0", "5")
	hrInsertObservation(t, sqlDB, hrObsGap, hrSenderValid, "consistent", "0", "1")
	hrInsertObservation(t, sqlDB, hrObsDiv, hrSenderValid, "divergence", "0", "0")

	hrInsertHold(t, sqlDB, hrHoldNoEvidence, hrSenderNoEvidence, nonce.CauseUnexplainedGap, hrObsNoEvidence)
	hrInsertHold(t, sqlDB, hrHoldCause, hrSenderCause, nonce.CauseChainViewDivergence, hrObsCause)
	// hrSenderValid carries TWO coexisting active holds plus one allocated
	// binding at nonce 0 (base M+1 = 1, observed pending 1 => consistent).
	hrInsertHold(t, sqlDB, hrHoldGap, hrSenderValid, nonce.CauseUnexplainedGap, hrObsGap)
	hrInsertHold(t, sqlDB, hrHoldSurvivor, hrSenderValid, nonce.CauseChainViewDivergence, hrObsDiv)
	hrInsertBinding(t, sqlDB, hrSenderValid, hrBindingValid, "hr-intent-valid", "0")

	// --- insufficient evidence: a missing operator finding -----------------
	holdBefore := hrReadHold(t, sqlDB, hrHoldNoEvidence)
	floorBefore := hrFloor(t, sqlDB, hrSenderNoEvidence)
	censusBefore := hrCount(t, sqlDB)
	detail := hrWantRefused(t, sqlDB, healthy,
		hrRequest("op-hr-noevidence-1", hrSenderNoEvidence, hrHoldNoEvidence, hrObsNoEvidence, ""),
		holdBefore, floorBefore, censusBefore)
	if !strings.Contains(detail, "operator finding") {
		t.Fatalf("empty-evidence refusal detail = %q, want the operator-finding requirement", detail)
	}

	// --- insufficient evidence: an observation outside the scope -----------
	holdBefore = hrReadHold(t, sqlDB, hrHoldNoEvidence)
	censusBefore = hrCount(t, sqlDB)
	detail = hrWantRefused(t, sqlDB, healthy,
		hrRequest("op-hr-foreignobs-1", hrSenderNoEvidence, hrHoldNoEvidence, hrObsGap, "hr: finding text"),
		holdBefore, hrFloor(t, sqlDB, hrSenderNoEvidence), censusBefore)
	if !strings.Contains(detail, "outside the scope") {
		t.Fatalf("foreign-observation refusal detail = %q, want the in-scope requirement", detail)
	}

	// --- cause still present: the fresh view still diverges ----------------
	holdBefore = hrReadHold(t, sqlDB, hrHoldCause)
	floorBefore = hrFloor(t, sqlDB, hrSenderCause)
	censusBefore = hrCount(t, sqlDB)
	detail = hrWantRefused(t, sqlDB, healthy,
		hrRequest("op-hr-cause-1", hrSenderCause, hrHoldCause, hrObsCause, "hr: finding text"),
		holdBefore, floorBefore, censusBefore)
	if !strings.Contains(detail, nonce.ClassificationDivergence) {
		t.Fatalf("still-present-cause refusal detail = %q, want the divergence classification", detail)
	}
	if floorBefore.Valid {
		t.Fatalf("scenario seed left hrSenderCause with a floor %q, want NULL (untouched)", floorBefore.String)
	}

	// --- valid release through the REAL nonce-admin subcommand -------------
	rpcSrv := hrJSONRPCServer(t, "0x0", "0x1")
	env := map[string]string{
		config.EnvPGDSN:                 dsn,
		config.EnvRPCURL:                rpcSrv.URL,
		config.EnvChainID:               strconv.FormatInt(hrChain, 10),
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

	// status BEFORE the release: both holds listed with causes + evidence and
	// the scope floor.
	if code := app.NonceAdmin(ctx, []string{
		"status", "--chain-id", strconv.FormatInt(hrChain, 10), "--sender", hrSenderValid,
	}, deps); code != 0 {
		t.Fatalf("nonce-admin status (before) exit = %d, want 0 (stderr %q)", code, cliErr.String())
	}
	beforeStatus := cliOut.String()
	for _, want := range []string{hrHoldGap, hrHoldSurvivor, "cause=unexplained_gap",
		"cause=chain_view_divergence", hrObsGap, hrObsDiv, "reconciled_floor=0", "active_holds=2"} {
		if !strings.Contains(beforeStatus, want) {
			t.Fatalf("status (before) = %q, want it to list %q", beforeStatus, want)
		}
	}
	cliOut.Reset()
	cliErr.Reset()

	const (
		hrCLIOp       = "op-hr-cli-1"
		hrCLIEvidence = "hr: fresh chain re-observation is consistent; the gap is reconciled"
	)
	if code := app.NonceAdmin(ctx, []string{
		"hold-release",
		"--operation-id", hrCLIOp,
		"--hold-id", hrHoldGap,
		"--chain-id", strconv.FormatInt(hrChain, 10),
		"--sender", hrSenderValid,
		"--observation-id", hrObsGap,
		"--evidence", hrCLIEvidence,
		"--operator", "hr-operator",
		"--reason", "reconciled gap",
	}, deps); code != 0 {
		t.Fatalf("nonce-admin hold-release exit = %d, want 0 (stderr %q)", code, cliErr.String())
	}
	if !strings.Contains(cliOut.String(), "outcome=applied") ||
		!strings.Contains(cliOut.String(), "action=hold_release") ||
		!strings.Contains(cliOut.String(), "subject_id="+hrHoldGap) {
		t.Fatalf("nonce-admin stdout = %q, want an applied hold_release report", cliOut.String())
	}

	// ONLY the named hold cleared; the coexisting active hold survives.
	released := hrReadHold(t, sqlDB, hrHoldGap)
	if released.status != nonce.HoldStatusReleased || !released.releaseOp.Valid ||
		released.releaseOp.String != hrCLIOp || !released.releaseObs.Valid {
		t.Fatalf("released hold = %+v, want terminal released with release_operation_id %s", released, hrCLIOp)
	}
	survivor := hrReadHold(t, sqlDB, hrHoldSurvivor)
	if survivor.status != nonce.HoldStatusActive || survivor.releaseOp.Valid {
		t.Fatalf("coexisting hold = %+v, want it still active and untouched", survivor)
	}

	// The floor advanced to the observed pending count (0 -> 1).
	if floor := hrFloor(t, sqlDB, hrSenderValid); !floor.Valid || floor.String != "1" {
		t.Fatalf("reconciled floor after release = %v, want 1 (the observed pending)", floor)
	}
	if action, outcome, subject, _ := hrReadAudit(t, sqlDB, hrCLIOp); action != "hold_release" ||
		outcome != "applied" || subject != hrHoldGap {
		t.Fatalf("applied audit = (%s, %s, %s), want (hold_release, applied, %s)", action, outcome, subject, hrHoldGap)
	}

	// status AFTER the release: the surviving hold is listed, the released one
	// is not, and the floor is the advanced value.
	cliOut.Reset()
	cliErr.Reset()
	if code := app.NonceAdmin(ctx, []string{
		"status", "--chain-id", strconv.FormatInt(hrChain, 10), "--sender", hrSenderValid,
	}, deps); code != 0 {
		t.Fatalf("nonce-admin status (after) exit = %d, want 0 (stderr %q)", code, cliErr.String())
	}
	afterStatus := cliOut.String()
	if !strings.Contains(afterStatus, hrHoldSurvivor) ||
		!strings.Contains(afterStatus, "cause=chain_view_divergence") ||
		!strings.Contains(afterStatus, hrObsDiv) ||
		!strings.Contains(afterStatus, "reconciled_floor=1") ||
		!strings.Contains(afterStatus, "active_holds=1") {
		t.Fatalf("status (after) = %q, want the survivor, its cause/evidence and floor 1", afterStatus)
	}
	if strings.Contains(afterStatus, hrHoldGap) {
		t.Fatalf("status (after) still lists the released hold: %q", afterStatus)
	}
}

// TestNonceHoldReleaseUnderActive006Recovery pins the V7 coexistence semantic:
// releasing one named 008 hold while a 006 recovery is active clears ONLY that
// hold — the 006 recovery row, every other active hold, and the 006 pause
// itself are untouched. Clearing an 008 hold is never a 006 release.
func TestNonceHoldReleaseUnderActive006Recovery(t *testing.T) {
	ctx := context.Background()
	dsn := nonceStartPostgres(t)

	var out bytes.Buffer
	if err := db.MigrateUp(ctx, nonceMigrateOptions(dsn), &out); err != nil {
		t.Fatalf("MigrateUp() error = %v (output %q)", err, out.String())
	}
	sqlDB := nonceOpenSQL(t, dsn)
	hrSeed(t, sqlDB)

	sender := nonceAddr("b4")
	const (
		holdGap  = "hr-006-hold-gap"
		holdKeep = "hr-006-hold-keep"
		obsGap   = "no-hr-006-gap"
		obsKeep  = "no-hr-006-keep"
		opID     = "op-hr-006-1"
		recID    = "rec-hr-006-1"
	)
	nonceMustExec(t, sqlDB, `INSERT INTO nonce_wallet_registry
		(chain_id, sender, state, registry_seq) VALUES ($1, $2, 'active', 1)`, hrChain, sender)
	hrInsertScope(t, sqlDB, sender, "0", "")
	hrInsertObservation(t, sqlDB, obsGap, sender, "consistent", "0", "1")
	hrInsertObservation(t, sqlDB, obsKeep, sender, "divergence", "0", "0")
	hrInsertHold(t, sqlDB, holdGap, sender, nonce.CauseUnexplainedGap, obsGap)
	hrInsertHold(t, sqlDB, holdKeep, sender, nonce.CauseChainViewDivergence, obsKeep)
	hrInsertBinding(t, sqlDB, sender, "hr-006-binding", "hr-006-intent", "0")

	nonceMustExec(t, sqlDB, `INSERT INTO reorg_policy_history
		(chain_id, policy_seq, max_depth, operator) VALUES ($1, 1, 64, 'hr-006')`, hrChain)
	nonceMustExec(t, sqlDB, `INSERT INTO reorg_recovery
		(chain_id, recovery_id, phase, policy_seq, max_depth, bound_old_number, bound_old_hash, recovery_seq)
		VALUES ($1, $2, 'detected', 1, 64, 10, $3, 1)`, hrChain, recID, hrHeadHash)

	pool := replayOpenPool(t, dsn)
	runner := hrRunner(pool, hrRPC{count: `"0x1"`, head: "0x0", headHash: hrHeadHash})
	res, err := runner.Run(ctx, hrRequest(opID, sender, holdGap, obsGap,
		"hr: gap reconciled under active 006; the pauses are independent"))
	if err != nil {
		t.Fatalf("run %s: unexpected error %v", opID, err)
	}
	if res.Outcome != nonce.AdminApplied {
		t.Fatalf("run %s outcome = %q, want applied (detail %q)", opID, res.Outcome, res.Detail)
	}

	if got := hrReadHold(t, sqlDB, holdGap); got.status != nonce.HoldStatusReleased {
		t.Fatalf("released hold = %+v, want terminal released", got)
	}
	if got := hrReadHold(t, sqlDB, holdKeep); got.status != nonce.HoldStatusActive || got.releaseOp.Valid {
		t.Fatalf("coexisting hold = %+v, want it still active and untouched", got)
	}
	var phase string
	var seq int64
	if err := sqlDB.QueryRow(`SELECT phase, recovery_seq FROM reorg_recovery
		WHERE chain_id = $1 AND recovery_id = $2`, hrChain, recID).Scan(&phase, &seq); err != nil {
		t.Fatalf("read 006 recovery row: %v", err)
	}
	if phase != "detected" || seq != 1 {
		t.Fatalf("006 recovery = (%s, %d), want it untouched at (detected, 1)", phase, seq)
	}
	if floor := hrFloor(t, sqlDB, sender); !floor.Valid || floor.String != "1" {
		t.Fatalf("reconciled floor after release = %v, want 1", floor)
	}
}
