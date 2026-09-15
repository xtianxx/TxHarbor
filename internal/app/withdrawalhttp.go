// withdrawalhttp.go owns the 007 HTTP wiring (T014): POST /withdrawals and
// GET /withdrawals/{id} mounted on the existing probe http.Server
// (contracts/api.md §1-§3). The handler is the transport boundary: it decodes
// the JSON body (400 malformed_request is handler-only), reads the Bearer
// credential, calls the frozen withdrawal core, and renders the wire body.
// caller_id is NEVER taken from the request body — GET resolves it via
// withdrawal.Authenticate and POST relies on the core's own authentication.
//
// Response-first rule: every pre-tx reject audit is returned as an AuditIntent
// on SubmitResult and is persisted by this handler AFTER the response is
// flushed, so a slow audit write (up to the 2s detached bound in
// withdrawal.WriteRejectAudit) can never delay or alter the response.
//
// Allowlist source (T020): every POST resolves the live FR-05 whitelist from
// the newest 003/004 deposit_config_history row via
// withdrawal.ResolveAssetAllowlist, AFTER the Bearer credential is checked and
// BEFORE the frozen core runs. An absent, blank, or unreadable policy row is a
// retryable 503 with the core's retry text plus one best-effort `unavailable`
// audit row — never an empty/full-chain fallback. T015 wires the outcome
// counters and builds no new logging plumbing: the handler emits no per-request
// logger of its own and rides the process logger the serve lifecycle already
// runs, with every value funnelled through logx.Redact (FR-20/FR-21, zero
// secrets).
package app

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/metrics"
	"github.com/xtianxx/txharbor/internal/withdrawal"
)

// codeMethodNotAllowed is the transport-only code for a method mismatch. It is
// not part of the contracts §1 response table: a wrong method never reaches the
// core, so it carries no withdrawal machine code.
const codeMethodNotAllowed = "method_not_allowed"

// withdrawalUnavailableMessage mirrors the core's 503 retry instruction for
// internal-defect outcomes (nil pool, dead context, CSPRNG failure) that reach
// the handler as a non-nil error rather than a SubmitResult. Kept in sync with
// intake.go's unexported constant of the same text.
const withdrawalUnavailableMessage = "withdrawal storage unavailable or the submission outcome is unknown;" +
	" retry with the same idempotency key and the same parameters (never rotate the key)"

// WithdrawalHandler serves the two 007 endpoints on the shared probe listener.
// Pool is the read/write connection pool; ChainID is the deployment chain bind
// (FR-04) and selects the live FR-05 policy row resolved per create attempt.
type WithdrawalHandler struct {
	Pool    *pgxpool.Pool
	ChainID int64
	// Metrics, when non-nil, receives one outcome bump per create attempt.
	// Wire-shape unit tests construct the handler without a registry, so every
	// increment is guarded.
	Metrics *metrics.Metrics
}

// observeWithdrawal bumps the 007 outcome counter for status when a registry
// is wired.
func (h *WithdrawalHandler) observeWithdrawal(status int) {
	if h.Metrics != nil {
		h.Metrics.ObserveWithdrawalStatus(status)
	}
}

// withdrawalRequestBody is one POST /withdrawals JSON body. It deliberately has
// no caller field: an unknown "caller_id" member is ignored by encoding/json,
// and identity always comes from the Bearer credential.
type withdrawalRequestBody struct {
	IdempotencyKey  string `json:"idempotency_key"`
	ChainID         int64  `json:"chain_id"`
	Asset           string `json:"asset"`
	Recipient       string `json:"recipient"`
	Amount          string `json:"amount"`
	AuthorizationID string `json:"authorization_id"`
}

// withdrawalErrorResponse is the error-body shape. RequestID/TraceID are
// omitted when empty; GET bodies omit the trace id so the FR-15 404
// normalization stays byte-equal (its trace id rides the response header).
type withdrawalErrorResponse struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"request_id,omitempty"`
	TraceID   string `json:"trace_id,omitempty"`
}

// withdrawalPostResponse is the POST success body: the persisted request
// identity plus the echoed business facts. It carries no recovery signal — the
// POST path never reads 006 recovery state (contracts §3).
type withdrawalPostResponse struct {
	RequestID string `json:"request_id"`
	ChainID   int64  `json:"chain_id"`
	Asset     string `json:"asset"`
	Recipient string `json:"recipient"`
	Amount    string `json:"amount"`
	Status    string `json:"status"`
	TraceID   string `json:"trace_id"`
}

// withdrawalRecoveryBody is the contracts §3 recovery signal.
type withdrawalRecoveryBody struct {
	State     string `json:"state"`
	Execution string `json:"execution"`
}

// withdrawalGetResponse is the contracts §3 query body (snake_case).
type withdrawalGetResponse struct {
	RequestID string                 `json:"request_id"`
	CallerID  int64                  `json:"caller_id"`
	ChainID   int64                  `json:"chain_id"`
	Asset     string                 `json:"asset"`
	Recipient string                 `json:"recipient"`
	Amount    string                 `json:"amount"`
	Status    string                 `json:"status"`
	CreatedAt string                 `json:"created_at"`
	Recovery  withdrawalRecoveryBody `json:"recovery"`
}

// ServeHTTP dispatches the two mounted routes; the collection path is POST,
// every "/withdrawals/{id}" path is GET.
func (h *WithdrawalHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/withdrawals" || r.URL.Path == "/withdrawals/" {
		h.ServePOST(w, r)
		return
	}
	h.ServeGET(w, r)
}

// ServePOST handles POST /withdrawals in the contracts §2 order: decode the
// body (400, no DB), require the Bearer credential (401, no DB, no audit), then
// hand the typed request to the core. The response is written and flushed
// BEFORE any pre-tx audit intent is persisted.
func (h *WithdrawalHandler) ServePOST(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		trace := newWithdrawalTraceID()
		withdrawalWriteError(w, http.StatusMethodNotAllowed, codeMethodNotAllowed, "method not allowed", "", trace, trace)
		return
	}

	var body withdrawalRequestBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		trace := newWithdrawalTraceID()
		withdrawalWriteError(w, http.StatusBadRequest, string(withdrawal.CodeMalformedRequest), "request body is not valid JSON", "", trace, trace)
		h.observeWithdrawal(http.StatusBadRequest)
		return
	}

	token, ok := withdrawalBearerToken(r)
	if !ok {
		trace := newWithdrawalTraceID()
		withdrawalWriteError(w, http.StatusUnauthorized, string(withdrawal.CodeUnauthenticated), "missing or invalid API key", "", trace, trace)
		h.observeWithdrawal(http.StatusUnauthorized)
		return
	}

	// (1) Authenticate before the policy read: a missing/revoked key stays a
	// 401, and a storage failure is the core's 503 shape with no audit row
	// (there is no verified caller to attribute it to).
	auth, err := withdrawal.Authenticate(r.Context(), h.Pool, token)
	if err != nil {
		trace := newWithdrawalTraceID()
		var e *withdrawal.Error
		if errors.As(err, &e) && e.Code == withdrawal.CodeUnauthenticated {
			withdrawalWriteError(w, http.StatusUnauthorized, string(withdrawal.CodeUnauthenticated), "missing or invalid API key", "", trace, trace)
			h.observeWithdrawal(http.StatusUnauthorized)
			return
		}
		withdrawalWriteError(w, http.StatusServiceUnavailable, string(withdrawal.CodeTemporarilyUnavailable), withdrawalUnavailableMessage, "", trace, trace)
		h.observeWithdrawal(http.StatusServiceUnavailable)
		return
	}

	// (3) FR-05: resolve the live whitelist from the newest 003/004 policy row
	// on every attempt. A missing, blank, or unreadable source is retryable —
	// never an empty/full-chain fallback that could allow an asset. The 503 is
	// flushed first, then exactly one best-effort `unavailable` audit row is
	// written for the authenticated caller (mirrors the core's 503 rule).
	allowlist, err := withdrawal.ResolveAssetAllowlist(r.Context(), h.Pool, h.ChainID)
	if err != nil {
		trace := newWithdrawalTraceID()
		withdrawalWriteError(w, http.StatusServiceUnavailable, string(withdrawal.CodeTemporarilyUnavailable), withdrawalUnavailableMessage, "", trace, trace)
		h.observeWithdrawal(http.StatusServiceUnavailable)
		_ = withdrawal.WriteRejectAudit(r.Context(), h.Pool, auth.Caller.ID, "",
			"unavailable", "storage failure while resolving withdrawal allowlist")
		return
	}

	res, err := withdrawal.SubmitWithdrawal(r.Context(), h.Pool, withdrawal.SubmitRequest{
		PresentedKey:    token,
		IdempotencyKey:  body.IdempotencyKey,
		ChainID:         body.ChainID,
		ExpectedChainID: h.ChainID,
		Asset:           body.Asset,
		Recipient:       body.Recipient,
		Amount:          body.Amount,
		AuthorizationID: body.AuthorizationID,
		Allowlist:       allowlist,
	})
	if err != nil {
		trace := newWithdrawalTraceID()
		withdrawalWriteError(w, http.StatusServiceUnavailable, string(withdrawal.CodeTemporarilyUnavailable), withdrawalUnavailableMessage, "", trace, trace)
		h.observeWithdrawal(http.StatusServiceUnavailable)
		return
	}

	trace := res.RequestID
	if trace == "" {
		trace = newWithdrawalTraceID()
	}
	if res.Status == http.StatusCreated || res.Status == http.StatusOK {
		withdrawalWriteJSON(w, res.Status, trace, withdrawalPostResponse{
			RequestID: res.RequestID,
			ChainID:   body.ChainID,
			Asset:     body.Asset,
			Recipient: body.Recipient,
			Amount:    body.Amount,
			Status:    "accepted",
			TraceID:   trace,
		})
	} else {
		withdrawalWriteError(w, res.Status, string(res.Code), res.Message, res.RequestID, trace, trace)
	}
	h.observeWithdrawal(res.Status)

	if res.Audit != nil {
		_ = withdrawal.WriteRejectAudit(r.Context(), h.Pool, res.Audit.CallerID, res.Audit.RequestID, res.Audit.Action, res.Audit.Detail)
	}
}

// ServeGET handles GET /withdrawals/{id}. It authenticates the Bearer
// credential (401), resolves the caller from the key row (never the body), and
// renders the ownership-enforced §3 view. A miss and a foreign id render the
// same 404 body. No audit is written on this read path.
func (h *WithdrawalHandler) ServeGET(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		withdrawalWriteError(w, http.StatusMethodNotAllowed, codeMethodNotAllowed, "method not allowed", "", "", newWithdrawalTraceID())
		return
	}

	token, ok := withdrawalBearerToken(r)
	if !ok {
		withdrawalWriteError(w, http.StatusUnauthorized, string(withdrawal.CodeUnauthenticated), "missing or invalid API key", "", "", newWithdrawalTraceID())
		return
	}

	auth, err := withdrawal.Authenticate(r.Context(), h.Pool, token)
	if err != nil {
		var e *withdrawal.Error
		if errors.As(err, &e) && e.Code == withdrawal.CodeUnauthenticated {
			withdrawalWriteError(w, http.StatusUnauthorized, string(withdrawal.CodeUnauthenticated), "missing or invalid API key", "", "", newWithdrawalTraceID())
			return
		}
		withdrawalWriteError(w, http.StatusServiceUnavailable, string(withdrawal.CodeTemporarilyUnavailable), withdrawalUnavailableMessage, "", "", newWithdrawalTraceID())
		return
	}

	id := r.PathValue("id")
	if id == "" {
		id = strings.TrimPrefix(r.URL.Path, "/withdrawals/")
	}

	view, err := withdrawal.GetWithdrawal(r.Context(), h.Pool, auth.Caller.ID, id)
	if err != nil {
		var e *withdrawal.Error
		if errors.As(err, &e) && e.Code == withdrawal.CodeNotFound {
			withdrawalWriteError(w, http.StatusNotFound, string(withdrawal.CodeNotFound), "withdrawal not found", "", "", newWithdrawalTraceID())
			return
		}
		withdrawalWriteError(w, http.StatusServiceUnavailable, string(withdrawal.CodeTemporarilyUnavailable), withdrawalUnavailableMessage, "", "", newWithdrawalTraceID())
		return
	}

	withdrawalWriteJSON(w, http.StatusOK, newWithdrawalTraceID(), withdrawalGetResponse{
		RequestID: view.RequestID,
		CallerID:  view.CallerID,
		ChainID:   view.ChainID,
		Asset:     view.Asset,
		Recipient: view.Recipient,
		Amount:    view.Amount,
		Status:    view.Status,
		CreatedAt: view.CreatedAt.UTC().Format("2006-01-02T15:04:05Z07:00"),
		Recovery: withdrawalRecoveryBody{
			State:     view.Recovery.State,
			Execution: view.Recovery.Execution,
		},
	})
}

// withdrawalBearerToken extracts the "Bearer <txh_…>" credential. A missing,
// empty, or non-Bearer header returns false; the token is never logged.
func withdrawalBearerToken(r *http.Request) (string, bool) {
	const prefix = "Bearer "
	header := r.Header.Get("Authorization")
	if !strings.HasPrefix(header, prefix) {
		return "", false
	}
	token := strings.TrimSpace(strings.TrimPrefix(header, prefix))
	return token, token != ""
}

// withdrawalWriteJSON writes one JSON response, stamps the trace header, and
// flushes. Content-Length is set explicitly so the client can read the complete
// body without waiting for the handler (and its post-response audit) to return.
func withdrawalWriteJSON(w http.ResponseWriter, status int, traceID string, body any) {
	raw, err := json.Marshal(body)
	if err != nil {
		http.Error(w, `{"code":"internal_error","message":"response encoding failed"}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(raw)))
	w.Header().Set("X-Request-Trace-Id", traceID)
	w.WriteHeader(status)
	_, _ = w.Write(raw)
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
}

// withdrawalWriteError writes the error-body shape. bodyTraceID "" omits the
// body trace member (GET, for FR-15 byte-equality); the header always carries
// the trace id.
func withdrawalWriteError(w http.ResponseWriter, status int, code, message, requestID, bodyTraceID, headerTraceID string) {
	withdrawalWriteJSON(w, status, headerTraceID, withdrawalErrorResponse{
		Code:      code,
		Message:   message,
		RequestID: requestID,
		TraceID:   bodyTraceID,
	})
}

// newWithdrawalTraceID mints the opaque response trace id: "tr-" + 8 lowercase
// hex characters. A CSPRNG failure yields a stable non-secret fallback.
func newWithdrawalTraceID() string {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "tr-unknown"
	}
	return "tr-" + hex.EncodeToString(b[:])
}
