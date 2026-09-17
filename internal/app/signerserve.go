// signerserve.go owns `txharbor signer-serve` (T013 wiring, completed to the
// real service by T043): the standalone signer process — the only binary path
// that constructs a KeyProvider — and its transport, contracts/api.md §§1–4.
// Exactly two authenticated operations exist: POST
// /signer/v1/signing-requests (submit) and GET
// /signer/v1/signing-requests/{id} (status), plus the shared metrics
// endpoint. Delivery is not an endpoint (api.md §4): signature bytes leave
// only through the submit response's T-deliver protected region
// (internal/signer/delivery.go), so the transport hands the region the HTTP
// response writer as its DeliverySink and encodes nothing of its own on
// success — it never re-signs, re-gates, or bypasses admission.
//
// Startup order: config gate (full Load validation + signer required-ness) →
// policy → pool → metrics → key provider → live 008 binding wiring →
// transport → listener → serve until ctx ends. Exit 0 on clean shutdown after
// SIGINT/SIGTERM, 1 on startup failure (redacted reason).
package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/logx"
	"github.com/xtianxx/txharbor/internal/metrics"
	"github.com/xtianxx/txharbor/internal/signer"
)

// maxSignerBodyBytes bounds one submit body read. api.md §2 bodies are small
// bounded JSON; a longer body is a malformed request, never an unbounded read.
const maxSignerBodyBytes = 64 * 1024

// signerLiveBinding is the live 008 half of the signer deps: the binding fact
// read (BindingReader) plus the T-deliver scope-row lock (ScopeLocker). Its
// production constructor is newSignerLiveBinding (signerlive.go), an untagged
// app-local adapter over *nonce.ReadProvider that is functionally identical to
// internal/signer's integration-tagged LiveBindingReader (whose internal/nonce
// import T023 keeps out of the default signer graph; the live parity test pins
// the two mapping copies together). signerBindingWiring is the injection seam:
// it defaults to the production constructor and a test may replace it.
type signerLiveBinding struct {
	Binding signer.BindingReader
	Scope   signer.ScopeLocker
}

// signerBindingWiring builds the live 008 binding reader for one process pool;
// the default is the production constructor, so every build — tagged or not —
// carries live 008 wiring.
var signerBindingWiring = func(pool *pgxpool.Pool, cfg *config.Config) (signerLiveBinding, error) {
	return newSignerLiveBinding(pool, cfg.NonceReadToken)
}

// signerTransport is the 009 HTTP transport (api.md §§1–4). It owns no
// decision: authentication/permission and the submit transaction are the
// library's, the delivery region is delivery.go's, and the wire shapes are
// api.md's.
type signerTransport struct {
	pool    *pgxpool.Pool
	submit  signer.SubmitDeps
	deliver signer.DeliveryDeps
}

// signerErrorBody is the api.md error shape: stable machine code + human
// message + trace id, with the offending field when the refusal names one and
// the desensitized §3 status view when a delivery/gate conflict recorded an
// observed basis. It never carries credentials or signature bytes.
type signerErrorBody struct {
	Code    string            `json:"code"`
	Message string            `json:"message"`
	Field   string            `json:"field,omitempty"`
	TraceID string            `json:"trace_id"`
	Status  *signerStatusBody `json:"status,omitempty"`
}

// signerStatusBody is the desensitized api.md §3 status view: recorded facts
// only. It has no signature field by construction; TxHash is non-empty only
// when the latest delivery admission was already cleared for delivery
// (admitted/delivered) — after revocation/expiry/pause the view is
// status-only (OC-6).
type signerStatusBody struct {
	SigningRequestID string                `json:"signing_request_id"`
	State            string                `json:"state"`
	ContentHash      string                `json:"content_hash"`
	AttemptID        string                `json:"attempt_id"`
	IntentID         string                `json:"intent_id"`
	PolicyVersion    string                `json:"policy_version"`
	RefusalClass     string                `json:"refusal_class,omitempty"`
	CreatedAt        string                `json:"created_at"`
	UpdatedAt        string                `json:"updated_at"`
	Delivery         *signerStatusDelivery `json:"delivery,omitempty"`
	TxHash           string                `json:"tx_hash,omitempty"`
}

// signerStatusDelivery is the latest recorded delivery admission (api.md §3):
// the last verdict + timestamp plus the observed blocking basis.
type signerStatusDelivery struct {
	Verdict            string `json:"verdict"`
	AttemptSeq         int    `json:"attempt_seq"`
	Reason             string `json:"reason,omitempty"`
	PauseBasis         string `json:"pause_basis,omitempty"`
	RecoveryBasis      string `json:"recovery_basis,omitempty"`
	AuthorizationState string `json:"authorization_state,omitempty"`
	BindingClass       string `json:"binding_class,omitempty"`
	CanSign            bool   `json:"can_sign"`
	DecidedAt          string `json:"decided_at"`
	DeliveredAt        string `json:"delivered_at,omitempty"`
}

// signerHTTPDeliverySink is the DeliverySink over the HTTP response writer:
// the region's bytes go straight to the wire, so a transport failure is
// observed by the region as the unknown outcome and nothing is buffered or
// re-rendered (delivery.go's fault-guarantee scope). started records that the
// response head is out; the handler then never writes an error body over it.
type signerHTTPDeliverySink struct {
	w       http.ResponseWriter
	trace   string
	started bool
}

// WriteDelivery writes the region-produced payload as the 200 response. It is
// called only from inside the T-deliver protected region (admission INSERT +
// bytes write + delivered marker + COMMIT happen together). Every transport
// failure it can observe — a write error, a short write, a flush error, or the
// server's WriteTimeout expiry surfaced at flush — is returned so the region
// resolves the current attempt as `unknown` (bytes MAY be out); it never claims
// success, and a marker already recorded by an earlier attempt is never used to
// turn a failed attempt into a success.
func (s *signerHTTPDeliverySink) WriteDelivery(_ context.Context, payload []byte) error {
	s.w.Header().Set("Content-Type", "application/json")
	s.w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
	s.w.Header().Set("X-Request-Trace-Id", s.trace)
	s.started = true
	s.w.WriteHeader(http.StatusOK)
	n, err := s.w.Write(payload)
	if err == nil && n != len(payload) {
		err = io.ErrShortWrite
	}
	// A bare http.Flusher.Flush() discards the transport error. net/http
	// buffers the small payload, so the flush is the first real socket write:
	// a peer reset or a WriteTimeout expiry is only observable here.
	// ResponseController.Flush returns it (Go 1.20+; net/http's response
	// implements FlushError). A writer that cannot flush cannot be verified, so
	// it is left on the conservative unknown path too.
	if err == nil {
		if ferr := http.NewResponseController(s.w).Flush(); ferr != nil {
			err = ferr
		}
	}
	if err != nil {
		// Secrets-free diagnostic: trace id + the redacted transport error.
		// The payload (signature/tx hash) never reaches a log.
		slog.Default().Warn("signer delivery write failed",
			"trace_id", s.trace,
			"outcome", "unknown",
			"bytes_may_be_out", true,
			"error", logx.Redact(err.Error()))
	}
	return err
}

// SignerServe runs `txharbor signer-serve`: the standalone signer process.
func SignerServe(ctx context.Context, args []string, d Deps) int {
	stderr := d.stderr()
	if len(args) != 0 {
		fmt.Fprintf(stderr, "txharbor signer-serve: takes no arguments\n")
		return 2
	}
	cfg, err := config.Load(d.getenv())
	if err != nil {
		fmt.Fprintf(stderr, "txharbor signer-serve: configuration error: %s\n", logx.Redact(err.Error()))
		return 1
	}
	policyCfg, err := cfg.SignerPolicyConfig()
	if err != nil {
		fmt.Fprintf(stderr, "txharbor signer-serve: configuration error: %s\n", logx.Redact(err.Error()))
		return 1
	}
	policy, err := signer.NewPolicy(policyCfg)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor signer-serve: configuration error: %s\n", logx.Redact(err.Error()))
		return 1
	}
	pool, err := db.OpenPool(ctx, cfg.PGDSN, cfg.ProbeTimeout)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor signer-serve: %s\n", logx.Redact(err.Error()))
		return 1
	}
	defer pool.Close()
	m := metrics.New(func() bool { return true })
	provider, err := signer.NewDevKeyProvider(signer.Mode(cfg.SignerMode), cfg.SignerKeyFile, cfg.SignerKeyTimeout)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor signer-serve: configuration error: %s\n", logx.Redact(err.Error()))
		return 1
	}
	live, err := signerBindingWiring(pool, cfg)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor signer-serve: startup failed (008 binding): %s\n", logx.Redact(err.Error()))
		return 1
	}
	if live.Binding == nil || live.Scope == nil {
		fmt.Fprintf(stderr, "txharbor signer-serve: startup failed (008 binding): incomplete live wiring\n")
		return 1
	}
	transport := &signerTransport{
		pool:    pool,
		submit:  signer.SubmitDeps{DB: pool, Policy: policy, Provider: provider, Binding: live.Binding, ScopeLock: live.Scope},
		deliver: signer.DeliveryDeps{DB: pool, Binding: live.Binding, ScopeLock: live.Scope},
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", m.Handler())
	mux.Handle("POST /signer/v1/signing-requests", http.HandlerFunc(transport.submitHTTP))
	mux.Handle("GET /signer/v1/signing-requests/{id}", http.HandlerFunc(transport.statusHTTP))
	srv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: cfg.ProbeTimeout,
		// WriteTimeout is the delivery send deadline (T043: existing knob, no
		// new timing surface): it bounds the region's outbound bytes write on
		// this listener the same way it already bounds the probe routes.
		WriteTimeout: cfg.ProbeTimeout,
	}
	listener, err := net.Listen("tcp", cfg.SignerHTTPAddr)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor signer-serve: startup failed (http listen): %s\n", logx.Redact(err.Error()))
		return 1
	}
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}()
	if err := srv.Serve(listener); err != nil && err != http.ErrServerClosed {
		fmt.Fprintf(stderr, "txharbor signer-serve: %s\n", logx.Redact(err.Error()))
		return 1
	}
	return 0
}

// submitHTTP handles POST /signer/v1/signing-requests in the fixed api.md §4
// order: authentication (401) → permission (403) → body shape/refusals
// (400/422) → submit transaction → delivery assessment. The success body is
// exactly the bytes the T-deliver protected region rendered from the
// persisted result; the handler never encodes, re-signs, or re-gates.
func (h *signerTransport) submitHTTP(w http.ResponseWriter, r *http.Request) {
	trace := newWithdrawalTraceID()
	token, ok := withdrawalBearerToken(r)
	if !ok {
		writeSignerError(w, trace, http.StatusUnauthorized, string(signer.ClassUnauthenticated), "", "credential rejected")
		return
	}
	// The caller is resolved once here so delivery can be scoped to it; Submit
	// authenticates again inside its own library step (api.md §4 step 1) and a
	// revocation racing either read fails closed (401 before signing,
	// signature_withheld before bytes).
	auth, err := signer.Authenticate(r.Context(), h.pool, token)
	if err != nil {
		h.writeRefusal(r.Context(), w, trace, 0, "", err)
		return
	}
	if err := signer.PermitSigning(auth.Caller); err != nil {
		h.writeRefusal(r.Context(), w, trace, auth.Caller.ID, "", err)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxSignerBodyBytes+1))
	if err != nil || len(body) > maxSignerBodyBytes {
		writeSignerError(w, trace, http.StatusBadRequest, string(signer.ClassMalformedRequest), "", "request body is not valid JSON")
		return
	}
	// Submit owns the first-receipt transaction and the replay path. Its
	// response is deliberately not the wire body: only the delivery region
	// renders bytes (persisted facts, byte-identical across re-deliveries).
	if _, err := signer.Submit(r.Context(), h.submit, token, body); err != nil {
		h.writeRefusal(r.Context(), w, trace, auth.Caller.ID, "", err)
		return
	}
	req, err := signer.DecodeRequest(body)
	if err != nil {
		// Submit accepted this body; a decode failure here is an internal
		// inconsistency, never a client error.
		writeSignerError(w, trace, http.StatusServiceUnavailable, string(signer.ClassStorageUnavailable), "", "storage unavailable")
		return
	}
	sink := &signerHTTPDeliverySink{w: w, trace: trace}
	if _, err := signer.Deliver(r.Context(), h.deliver, auth.Caller, req.SigningRequestID, sink); err != nil {
		if sink.started {
			// Indeterminate after response bytes were already going out: never
			// render over them (delivery.go: bytes MAY be out → unknown).
			slog.Default().Warn("signer delivery outcome unknown after response bytes",
				"signing_request_id", req.SigningRequestID, "class", logx.Redact(err.Error()))
			return
		}
		h.writeRefusal(r.Context(), w, trace, auth.Caller.ID, req.SigningRequestID, err)
	}
}

// statusHTTP handles GET /signer/v1/signing-requests/{id}: authentication
// (401), then the ownership-scoped desensitized view (api.md §3). Another
// caller's id and a nonexistent id are the identical 404; the view never
// carries signature bytes. Status never signs, gate-bypasses, or delivers.
func (h *signerTransport) statusHTTP(w http.ResponseWriter, r *http.Request) {
	trace := newWithdrawalTraceID()
	token, ok := withdrawalBearerToken(r)
	if !ok {
		writeSignerError(w, trace, http.StatusUnauthorized, string(signer.ClassUnauthenticated), "", "credential rejected")
		return
	}
	auth, err := signer.Authenticate(r.Context(), h.pool, token)
	if err != nil {
		h.writeRefusal(r.Context(), w, trace, 0, "", err)
		return
	}
	st, err := signer.LookupStatus(r.Context(), h.pool, auth.Caller.ID, r.PathValue("id"))
	switch {
	case err == nil:
		withdrawalWriteJSON(w, http.StatusOK, trace, signerStatusBodyOf(st))
	case errors.Is(err, signer.ErrStatusNotFound):
		writeSignerError(w, trace, http.StatusNotFound, "not_found", "", "signing request not found")
	default:
		h.writeRefusal(r.Context(), w, trace, auth.Caller.ID, "", err)
	}
}

// writeRefusal maps one library refusal to the api.md §2 wire: status by
// class, machine code, human message, the named field, and — on a status
// conflict — the desensitized §3 status view carrying the observed basis
// (status-only; no signature bytes, no credentials).
func (h *signerTransport) writeRefusal(ctx context.Context, w http.ResponseWriter, trace string, callerID int64, requestID string, err error) {
	class := signer.ClassStorageUnavailable
	field, message := "", "storage unavailable"
	var re *signer.RefusalError
	if errors.As(err, &re) {
		class = re.Class
		field = re.Field
		if re.Msg != "" {
			message = re.Msg
		} else {
			message = string(re.Class)
		}
	}
	status := signerHTTPStatus(class)
	body := signerErrorBody{Code: string(class), Message: message, Field: field, TraceID: trace}
	if status == http.StatusConflict && requestID != "" && callerID != 0 {
		if st, serr := signer.LookupStatus(ctx, h.pool, callerID, requestID); serr == nil {
			view := signerStatusBodyOf(st)
			body.Status = &view
		}
	}
	withdrawalWriteJSON(w, status, trace, body)
}

// signerHTTPStatus maps a refusal class to its api.md §2 status code. The
// taxonomy is closed (errors.go); an unrecognized class fails closed to the
// retryable 503 bucket.
func signerHTTPStatus(class signer.RefusalClass) int {
	switch class {
	case signer.ClassUnauthenticated:
		return http.StatusUnauthorized
	case signer.ClassSigningNotPermitted,
		signer.ClassAuthorizationInvalid, signer.ClassAuthorizationExpired,
		signer.ClassAuthorizationRevoked, signer.ClassAuthorizationUnverifiable:
		return http.StatusForbidden
	case signer.ClassMalformedRequest:
		return http.StatusBadRequest
	case signer.ClassArbitraryDigestRejected, signer.ClassValidationFailed, signer.ClassPolicyRefused:
		return http.StatusUnprocessableEntity
	case signer.ClassRequestConflict, signer.ClassBindingAbsent, signer.ClassBindingConflict,
		signer.ClassBindingPaused, signer.ClassBindingTerminal, signer.ClassRecoveryPaused,
		signer.ClassRecoveryActive, signer.ClassRecoveryVersionChanged, signer.ClassSignatureWithheld:
		return http.StatusConflict
	default:
		return http.StatusServiceUnavailable
	}
}

// writeSignerError renders one api.md error body (no secrets).
func writeSignerError(w http.ResponseWriter, trace string, status int, code, field, message string) {
	withdrawalWriteJSON(w, status, trace, signerErrorBody{
		Code:    code,
		Message: message,
		Field:   field,
		TraceID: trace,
	})
}

// signerStatusBodyOf renders the library status view as the api.md §3 wire
// body. Signature bytes cannot appear (the view has none); tx_hash passes
// through only when the latest admission cleared it for delivery.
func signerStatusBodyOf(st *signer.Status) signerStatusBody {
	body := signerStatusBody{
		SigningRequestID: st.SigningRequestID,
		State:            string(st.State),
		ContentHash:      st.ContentHash,
		AttemptID:        st.AttemptID,
		IntentID:         st.IntentID,
		PolicyVersion:    st.PolicyVersion,
		RefusalClass:     string(st.RefusalClass),
		CreatedAt:        signerRFC3339(st.CreatedAt),
		UpdatedAt:        signerRFC3339(st.UpdatedAt),
		TxHash:           st.TxHash,
	}
	if d := st.Delivery; d != nil {
		view := &signerStatusDelivery{
			Verdict:            string(d.Verdict),
			AttemptSeq:         d.AttemptSeq,
			Reason:             d.Reason,
			PauseBasis:         d.PauseBasis,
			RecoveryBasis:      d.RecoveryBasis,
			AuthorizationState: d.AuthorizationState,
			BindingClass:       d.BindingClass,
			CanSign:            d.CanSign,
			DecidedAt:          signerRFC3339(d.DecidedAt),
		}
		if d.DeliveredAt != nil {
			view.DeliveredAt = signerRFC3339(*d.DeliveredAt)
		}
		body.Delivery = view
	}
	return body
}

// signerRFC3339 renders one recorded timestamp for the status wire.
func signerRFC3339(t time.Time) string { return t.UTC().Format(time.RFC3339) }
