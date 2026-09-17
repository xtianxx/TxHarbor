// withdrawalexecution.go owns the 011 execution HTTP surface on the existing
// serve listener (contracts/api.md §1-§2): POST admission and (T035) GET view
// under /withdrawals/{request_id}/execution. The handler is the transport
// boundary; the domain decisions live in internal/execution.
package app

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/execution"
	"github.com/xtianxx/txharbor/internal/metrics"
	"github.com/xtianxx/txharbor/internal/withdrawal"
)

// codeForbidden is the fixed interface-permission refusal (contracts/api.md §1:
// absent/FALSE can_execute ⇒ 403 forbidden). It is deliberately distinct from
// 007's CodeUnauthorized (caller-level can_create).
const codeForbidden = "forbidden"

// WithdrawalExecutionHandler serves the 011 execution routes on the shared
// probe listener. Pool is the shared pool; ChainID is unused by the admission
// route but kept for symmetry with the view route.
type WithdrawalExecutionHandler struct {
	Pool    *pgxpool.Pool
	ChainID int64
	Metrics *metrics.Metrics
}

// ServeHTTP dispatches by method; the mux registers the method-qualified
// patterns, so a wrong method is normally a mux-level 405 and this switch is a
// defensive fallback.
func (h *WithdrawalExecutionHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		h.servePOST(w, r)
	default:
		w.Header().Set("Allow", http.MethodPost)
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

// servePOST runs the §1 order: body shape (400, no DB), Bearer authentication
// (401), fixed can_execute permission (403, fail-closed, before any domain
// read), then the admission core. A refused admission is 422 with the recorded
// class; a storage failure is 503 and is never rendered as a false refusal.
func (h *WithdrawalExecutionHandler) servePOST(w http.ResponseWriter, r *http.Request) {
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
