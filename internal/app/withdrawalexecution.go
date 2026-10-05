// withdrawalexecution.go owns the 011 execution HTTP surface on the existing
// serve listener (contracts/api.md §1-§2): POST admission and (T035) GET view
// under /withdrawals/{request_id}/execution. The handler is the transport
// boundary; the domain decisions live in internal/execution.
//
// T032 adds the 015 existing_withdrawal_recovery admission of the write path
// (servePOST): it is this HTTP funnel's own checkpoint — independent of the
// CLI `withdrawal-exec` execOperatorOp funnel — installed before
// execution.Admit, and it never replaces the 011 can_execute/execution gates
// (two-phase authority, F20/FR-025). The GET view stays on serve's query gate
// (T030); this file only gates the write path.
package app

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/execution"
	"github.com/xtianxx/txharbor/internal/logx"
	"github.com/xtianxx/txharbor/internal/metrics"
	"github.com/xtianxx/txharbor/internal/recovery"
	"github.com/xtianxx/txharbor/internal/withdrawal"
)

// codeForbidden is the fixed interface-permission refusal (contracts/api.md §1:
// absent/FALSE can_execute ⇒ 403 forbidden). It is deliberately distinct from
// 007's CodeUnauthorized (caller-level can_create).
const codeForbidden = "forbidden"

// WithdrawalExecutionHandler serves the 011 execution routes on the shared
// probe listener. Pool is the shared pool; ChainID is unused by the admission
// route but kept for symmetry with the view route (it scopes the 015 admission
// of the write path).
//
// The write path carries its own 015 existing_withdrawal_recovery admission
// (T032): the CLI `withdrawal-exec` funnel (execOperatorOp) is not this one,
// so the admission is installed here, before execution.Admit. It is assembled
// lazily from the process environment on first use — the handler's
// construction site is shared with the serve process, which does not pass its
// wiring — via withdrawalExecutionRecoveryLoad; tests may replace that seam.
type WithdrawalExecutionHandler struct {
	Pool    *pgxpool.Pool
	ChainID int64
	Metrics *metrics.Metrics

	recoveryOnce   sync.Once
	recoveryWiring *existingWithdrawalRecoveryWiring
	recoveryErr    error
}

// withdrawalExecutionRecoveryLoad assembles the 015 admission of the execution
// write path from the process environment. It is a seam for tests; production
// uses the process environment (the same environment the serve process
// started with).
var withdrawalExecutionRecoveryLoad = func(ctx context.Context, chainID uint64, getenv func(string) (string, bool)) (*existingWithdrawalRecoveryWiring, error) {
	cfg, err := existingWithdrawalRecoveryConfigFromEnv(getenv, chainID)
	if err != nil {
		return nil, err
	}
	return assembleExistingWithdrawalRecovery(ctx, cfg, getenv)
}

// recoveryAdmission returns the lazily assembled admission. A nil wiring with
// a nil error is normal mode (no control store configured, FR-023); a non-nil
// error is a fail-closed assembly refusal the caller must surface.
func (h *WithdrawalExecutionHandler) recoveryAdmission(ctx context.Context) (*existingWithdrawalRecoveryWiring, error) {
	h.recoveryOnce.Do(func() {
		h.recoveryWiring, h.recoveryErr = withdrawalExecutionRecoveryLoad(ctx, uint64(h.ChainID), os.LookupEnv)
	})
	return h.recoveryWiring, h.recoveryErr
}

// ServeHTTP dispatches by method; the mux registers the method-qualified
// patterns, so a wrong method is normally a mux-level 405 and this switch is a
// defensive fallback.
func (h *WithdrawalExecutionHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		h.servePOST(w, r)
	case http.MethodGet:
		h.serveGET(w, r)
	default:
		w.Header().Set("Allow", http.MethodGet+", "+http.MethodPost)
		withdrawalWriteError(w, http.StatusMethodNotAllowed, codeMethodNotAllowed, "method not allowed", "", "", newWithdrawalTraceID())
	}
}

// executionAdmissionResponse is the 201/200 body (contracts/api.md §1): the
// stable intent identity plus the echoed business facts. No credentials, keys,
// or raw bytes.
type executionAdmissionResponse struct {
	RequestID            string `json:"request_id"`
	IntentID             string `json:"intent_id"`
	State                string `json:"state"`
	Sender               string `json:"sender"`
	ChainID              int64  `json:"chain_id"`
	AuthorizationID      string `json:"authorization_id"`
	AuthorizationVersion int64  `json:"authorization_version"`
}

// servePOST runs the §1 order: 015 recovery admission (T032, fail-closed,
// before any body/auth/domain work), body shape (400, no DB), Bearer
// authentication (401), fixed can_execute permission (403, fail-closed, before
// any domain read), then the admission core. A refused admission is 422 with
// the recorded class; a storage failure is 503 and is never rendered as a
// false refusal.
//
// The 015 admission is a distinct funnel from the CLI `withdrawal-exec`
// (execOperatorOp): a refusal here produces no claim, no intent and no
// progress, and the allowed case still runs every 011 can_execute/execution
// gate unchanged (two-phase authority, FR-025).
func (h *WithdrawalExecutionHandler) servePOST(w http.ResponseWriter, r *http.Request) {
	if code := h.admitExecutionRecovery(w, r); code != 0 {
		return
	}
	if !emptyOrJSONBody(r.Body) {
		withdrawalWriteError(w, http.StatusBadRequest, string(withdrawal.CodeMalformedRequest), "request body is not valid JSON", "", "", newWithdrawalTraceID())
		return
	}

	token, ok := withdrawalBearerToken(r)
	if !ok {
		withdrawalWriteError(w, http.StatusUnauthorized, string(withdrawal.CodeUnauthenticated), "missing or invalid API key", "", "", newWithdrawalTraceID())
		return
	}

	auth, err := withdrawal.Authenticate(r.Context(), h.Pool, token)
	if err != nil {
		writeExecutionAuthError(w, err)
		return
	}

	allowed, err := execution.CanExecute(r.Context(), h.Pool, auth.Caller.ID)
	if err != nil {
		withdrawalWriteError(w, http.StatusServiceUnavailable, string(withdrawal.CodeTemporarilyUnavailable), withdrawalUnavailableMessage, "", "", newWithdrawalTraceID())
		return
	}
	if !allowed {
		withdrawalWriteError(w, http.StatusForbidden, codeForbidden, "execution permission is not granted", "", "", newWithdrawalTraceID())
		return
	}

	requestID := executionRequestID(r)
	if requestID == "" {
		withdrawalWriteError(w, http.StatusBadRequest, string(withdrawal.CodeValidationFailed), "request_id is required", "", "", newWithdrawalTraceID())
		return
	}

	outcome, err := execution.Admit(r.Context(), h.Pool, requestID, auth.Caller.ID)
	if err != nil {
		withdrawalWriteError(w, http.StatusServiceUnavailable, string(withdrawal.CodeTemporarilyUnavailable), withdrawalUnavailableMessage, requestID, "", newWithdrawalTraceID())
		return
	}

	switch outcome.Status {
	case execution.AdmissionCreated, execution.AdmissionRecorded:
		withdrawalWriteJSON(w, outcome.Status, newWithdrawalTraceID(), executionAdmissionResponse{
			RequestID:            outcome.Intent.RequestID,
			IntentID:             outcome.Intent.IntentID,
			State:                outcome.Intent.State,
			Sender:               outcome.Intent.Sender,
			ChainID:              outcome.Intent.ChainID,
			AuthorizationID:      outcome.Intent.AuthorizationID,
			AuthorizationVersion: outcome.Intent.AuthorizationVersion,
		})
	case execution.AdmissionNotFound:
		withdrawalWriteError(w, http.StatusNotFound, string(withdrawal.CodeNotFound), "withdrawal not found", "", "", newWithdrawalTraceID())
	case execution.AdmissionConflict:
		withdrawalWriteError(w, http.StatusConflict, string(withdrawal.CodeOperationConflict), "request is already bound to a different execution identity", requestID, "", newWithdrawalTraceID())
	default:
		code := string(outcome.Refusal)
		if code == "" {
			code = string(withdrawal.CodeValidationFailed)
		}
		withdrawalWriteError(w, http.StatusUnprocessableEntity, code, "execution admission refused", requestID, "", newWithdrawalTraceID())
	}
}

// executionRecoveryRefusalBody is the 015 refusal shape of the execution write
// path: the closed refusal_class is always exposed and the gate audits the
// refusal itself. It never carries credentials or signature bytes.
type executionRecoveryRefusalBody struct {
	Error        string `json:"error"`
	Capability   string `json:"capability"`
	RefusalClass string `json:"refusal_class"`
	Reason       string `json:"reason,omitempty"`
	InstanceID   string `json:"instance_id,omitempty"`
	TraceID      string `json:"trace_id"`
}

// admitExecutionRecovery evaluates the 015 existing_withdrawal_recovery
// admission before the write path acts (T032). It returns 0 when the action
// may proceed (normal mode included) and non-zero after writing the refusal
// (503, closed refusal_class) when the gate denies or the admission cannot be
// evaluated — there is no degraded pass-through. A refusal is a no-op for the
// 011 state: no claim, no intent admission, no progress, and the retry stays
// available for when the capability is released.
func (h *WithdrawalExecutionHandler) admitExecutionRecovery(w http.ResponseWriter, r *http.Request) int {
	wiring, err := h.recoveryAdmission(r.Context())
	if err != nil {
		writeExecutionRecoveryRefusal(w, recovery.CapabilityExistingWithdrawalRecovery,
			recovery.GateDecision{RefusalClass: recovery.RefusalControlStoreUnavailable, Reason: logx.Redact(err.Error())})
		return 1
	}
	if wiring == nil {
		return 0
	}
	dec, aerr := wiring.admit(r.Context(), recovery.CapabilityExistingWithdrawalRecovery,
		"execution_http_admit", executionRequestID(r))
	if aerr == nil && dec.Allowed {
		return 0
	}
	if aerr != nil && dec.Reason == "" {
		dec.Reason = logx.Redact(aerr.Error())
	}
	writeExecutionRecoveryRefusal(w, recovery.CapabilityExistingWithdrawalRecovery, dec)
	return 1
}

// writeExecutionRecoveryRefusal renders one 015 gate refusal as a bounded JSON
// 503. A decision without a known closed-set class is surfaced as
// control_store_unavailable: the response never invents a class and never a
// 2xx for a refused admission.
func writeExecutionRecoveryRefusal(w http.ResponseWriter, capability recovery.Capability, dec recovery.GateDecision) {
	class := existingWithdrawalRecoveryRefusalClass(dec)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusServiceUnavailable)
	_ = json.NewEncoder(w).Encode(executionRecoveryRefusalBody{
		Error:        "recovery_gate_refused",
		Capability:   string(capability),
		RefusalClass: string(class),
		Reason:       dec.Reason,
		InstanceID:   dec.InstanceID,
		TraceID:      newWithdrawalTraceID(),
	})
}

// executionRequestID extracts {request_id} from the path, tolerating both a
// Go 1.22 ServeMux path value and a raw trim.
func executionRequestID(r *http.Request) string {
	if id := r.PathValue("request_id"); id != "" {
		return id
	}
	id := strings.TrimPrefix(r.URL.Path, "/withdrawals/")
	id = strings.TrimSuffix(id, "/execution")
	return strings.TrimSuffix(id, "/")
}

// emptyOrJSONBody reports whether the body is empty or a valid JSON document.
// The admission route takes an empty body; anything else is a 400 without a DB
// read.
func emptyOrJSONBody(body io.Reader) bool {
	raw, err := io.ReadAll(io.LimitReader(body, 4096))
	if err != nil {
		return false
	}
	if len(strings.TrimSpace(string(raw))) == 0 {
		return true
	}
	return json.Valid(raw)
}

// executionViewResponse is the GET /withdrawals/{request_id}/execution body
// (contracts/api.md §2). The execution block is 011's authoritative rows; the
// lifecycle block is the display projection reference. No credentials, keys,
// raw bytes, or amounts.
type executionViewResponse struct {
	RequestID string              `json:"request_id"`
	IntentID  string              `json:"intent_id,omitempty"`
	Execution *executionViewBlock `json:"execution,omitempty"`
	Lifecycle *lifecycleViewBlock `json:"lifecycle,omitempty"`
	Note      string              `json:"note,omitempty"`
}

type executionViewBlock struct {
	State        string              `json:"state"`
	StateVersion int64               `json:"state_version"`
	Owner        string              `json:"owner,omitempty"`
	LeaseVersion int64               `json:"lease_version,omitempty"`
	ExpiresAt    string              `json:"expires_at,omitempty"`
	Steps        []executionStepView `json:"steps"`
}

type executionStepView struct {
	StepID    string `json:"step_id"`
	Action    string `json:"action"`
	State     string `json:"state"`
	AttemptID string `json:"attempt_id,omitempty"`
}

// lifecycleViewBlock always carries the four required references; a possibly
// stale reference is explicitly labelled and never presented as a verified
// current result.
type lifecycleViewBlock struct {
	AttemptID        string `json:"attempt_id"`
	LifecycleVersion int64  `json:"lifecycle_version"`
	ObservedAt       string `json:"observed_at"`
	Freshness        string `json:"freshness"`
}

const staleReferenceNote = "the lifecycle reference may be out of date; it is not a verified current result"

// serveGET renders the ownership-enforced execution view. It authenticates the
// Bearer credential (401), resolves the caller from the key row, and renders
// the same 404 for a missing and a foreign request. The endpoint is display
// only: its output is never an admission/qualification/send/reconcile permit.
func (h *WithdrawalExecutionHandler) serveGET(w http.ResponseWriter, r *http.Request) {
	token, ok := withdrawalBearerToken(r)
	if !ok {
		withdrawalWriteError(w, http.StatusUnauthorized, string(withdrawal.CodeUnauthenticated), "missing or invalid API key", "", "", newWithdrawalTraceID())
		return
	}
	auth, err := withdrawal.Authenticate(r.Context(), h.Pool, token)
	if err != nil {
		writeExecutionAuthError(w, err)
		return
	}
	requestID := executionRequestID(r)
	if requestID == "" {
		withdrawalWriteError(w, http.StatusBadRequest, string(withdrawal.CodeValidationFailed), "request_id is required", "", "", newWithdrawalTraceID())
		return
	}
	view, found, err := readExecutionView(r.Context(), h.Pool, auth.Caller.ID, requestID)
	if err != nil {
		withdrawalWriteError(w, http.StatusServiceUnavailable, string(withdrawal.CodeTemporarilyUnavailable), withdrawalUnavailableMessage, "", "", newWithdrawalTraceID())
		return
	}
	if !found {
		withdrawalWriteError(w, http.StatusNotFound, string(withdrawal.CodeNotFound), "withdrawal not found", "", "", newWithdrawalTraceID())
		return
	}
	withdrawalWriteJSON(w, http.StatusOK, newWithdrawalTraceID(), view)
}

// readExecutionView joins the 007 request row (ownership) to 011's
// authoritative execution rows and the display projection. found=false covers
// both a missing and a foreign request so the caller renders an identical 404.
func readExecutionView(ctx context.Context, pool *pgxpool.Pool, callerID int64, requestID string) (executionViewResponse, bool, error) {
	var (
		gotRequest string
		intentID   *string
		state      *string
		stateVers  *int64
	)
	err := pool.QueryRow(ctx, `SELECT r.request_id, i.intent_id, i.state, i.state_version
		FROM withdrawal_requests r
		LEFT JOIN payment_intents i ON i.request_id = r.request_id
		WHERE r.request_id = $1 AND r.caller_id = $2`, requestID, callerID).
		Scan(&gotRequest, &intentID, &state, &stateVers)
	if errors.Is(err, pgx.ErrNoRows) {
		return executionViewResponse{}, false, nil
	}
	if err != nil {
		return executionViewResponse{}, false, err
	}
	view := executionViewResponse{RequestID: gotRequest}
	if intentID == nil {
		return view, true, nil
	}
	view.IntentID = *intentID

	block := &executionViewBlock{State: *state, StateVersion: *stateVers}
	var (
		owner     string
		leaseVers int64
		expiresAt time.Time
	)
	err = pool.QueryRow(ctx, `SELECT owner_id, lease_version, expires_at FROM execution_claims WHERE intent_id = $1`, *intentID).
		Scan(&owner, &leaseVers, &expiresAt)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
	case err != nil:
		return executionViewResponse{}, false, err
	default:
		block.Owner = owner
		block.LeaseVersion = leaseVers
		block.ExpiresAt = expiresAt.UTC().Format(time.RFC3339)
	}

	rows, err := pool.Query(ctx, `SELECT step_id, action, state, COALESCE(attempt_id, '')
		FROM execution_steps WHERE intent_id = $1 ORDER BY issued_at, step_id`, *intentID)
	if err != nil {
		return executionViewResponse{}, false, err
	}
	defer rows.Close()
	for rows.Next() {
		var s executionStepView
		if err := rows.Scan(&s.StepID, &s.Action, &s.State, &s.AttemptID); err != nil {
			return executionViewResponse{}, false, err
		}
		block.Steps = append(block.Steps, s)
	}
	if err := rows.Err(); err != nil {
		return executionViewResponse{}, false, err
	}
	view.Execution = block

	projection, found, err := execution.ReadProjection(ctx, pool, gotRequest)
	if err != nil {
		return executionViewResponse{}, false, err
	}
	if found {
		life := &lifecycleViewBlock{LifecycleVersion: projection.LifecycleVersion, Freshness: projection.Freshness}
		if projection.LifecycleAttemptID != nil {
			life.AttemptID = *projection.LifecycleAttemptID
		}
		if projection.LifecycleObservedAt != nil {
			life.ObservedAt = projection.LifecycleObservedAt.UTC().Format(time.RFC3339)
		}
		view.Lifecycle = life
		if projection.Freshness == execution.FreshnessPossiblyStale {
			view.Note = staleReferenceNote
		}
	}
	return view, true, nil
}

// writeExecutionAuthError maps a credential failure: unauthenticated is 401,
// anything else (storage) is 503. The bearer token is never echoed.
func writeExecutionAuthError(w http.ResponseWriter, err error) {
	var e *withdrawal.Error
	if errors.As(err, &e) && e.Code == withdrawal.CodeUnauthenticated {
		withdrawalWriteError(w, http.StatusUnauthorized, string(withdrawal.CodeUnauthenticated), "missing or invalid API key", "", "", newWithdrawalTraceID())
		return
	}
	withdrawalWriteError(w, http.StatusServiceUnavailable, string(withdrawal.CodeTemporarilyUnavailable), withdrawalUnavailableMessage, "", "", newWithdrawalTraceID())
}
