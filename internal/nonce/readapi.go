// readapi.go owns the read-only provider behind the authenticated
// GET /nonce/bindings endpoints (contracts/read-api.md §1–§4): identity lookup
// by binding_id or by intent_id with optional expected chain_id/sender
// cross-checks, exactly one of the five outcomes
// (bound / terminal / not_bound / mismatch / unavailable), the snapshot
// annotations (every active hold, the read-only 006 recovery state, the
// registry state), and the constant-time bearer credential check.
//
// Consistency (§4): every lookup opens exactly one REPEATABLE READ transaction
// in read-write mode (NOT READ ONLY: the scope-row FOR SHARE below is a lock
// statement, which READ ONLY rejects), takes SELECT … FOR SHARE on the scope's
// nonce_scope_state row (when the scope row exists) before the annotation
// reads, and commits without a single data modification. An 008-owned read
// failure collapses the whole answer to `unavailable` (all-or-nothing, never a
// partial fact set); a 006 read failure only degrades
// annotations.recovery.state to `unknown` — never to none/released. While the
// startup rebuild gate is not open the whole read path answers `unavailable`
// before opening a transaction (read-api.md §2: "rebuild gate not open"), and
// the read transaction is bounded by the same 5s guard every 008 write uses
// (coord.go localWriteGuard) so a blocked scope-row share never pins a
// connection.
//
// This file is transport-free: internal/app/serve.go (T017) owns the HTTP
// wiring and maps ReadResponse.HTTPStatus / UnauthenticatedResponse onto its
// mux. The read path never creates, changes, releases, or reassigns a binding,
// hold, floor, observation, or upstream (006/007) row (OC-6).
package nonce

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Fixed notices (contracts/read-api.md §3). The facts notice is the contract's
// literal; the others state the fail-closed obligation of each outcome.
const (
	ReadNoticeFacts           = "binding facts only; this response is not a signing or broadcast authorization"
	ReadNoticeNotBound        = "no binding exists for this identity; fail closed and never invent an identity"
	ReadNoticeMismatch        = "the supplied expected scope contradicts the durable binding; the durable binding is authoritative"
	ReadNoticeUnavailable     = "the read service cannot produce a trustworthy answer; retry later with the same identity"
	ReadNoticeUnauthenticated = "missing or invalid bearer credential"
)

// Annotation vocabulary (contracts/read-api.md §3.1).
const (
	ReadGateOpen = "open"
	ReadGateHeld = "held"

	RecoveryNone            = "none"
	RecoveryRecovering      = "recovering"
	RecoveryPausedReconcile = "paused_reconcile"
	RecoveryReleased        = "released"
	RecoveryUnknown         = "unknown"
)

// recoveryPhaseReconcileRequired is the 006 phase that maps to
// paused_reconcile in the 007/008 annotation subset; every other active row is
// `recovering` and an absent row reads the terminal release marker
// (internal/indexer/reorgquery.go AnnotateRecoveryHeight).
const recoveryPhaseReconcileRequired = "reconcile_required"

// readRecoveryPhaseSQL and readRecoveryReleasedSQL are read-only 006 probes;
// 008 never writes these tables.
const (
	readRecoveryPhaseSQL    = `SELECT phase FROM reorg_recovery WHERE chain_id = $1`
	readRecoveryReleasedSQL = `
SELECT 1 FROM reorg_recovery_events
WHERE chain_id = $1 AND event IN ('auto_completed', 'released') LIMIT 1`
)

// readLockGuard bounds a read's scope-row share wait. The statement guard is
// the repo's shared write bound (coord.go localWriteGuard, pinned to
// internal/indexer's writeGuard): reads take no lock beyond one snapshot, so
// no config knob or contract statement gives them their own magnitude. Both
// guards are the first statements of the read transaction, so a scope-row FOR
// SHARE blocked by a writer's FOR UPDATE fails as the retryable `unavailable`
// instead of pinning a pooled connection.
const readLockGuard = "SET LOCAL lock_timeout = '5s'"

// ReadRequest is one read lookup. Exactly one of BindingID / IntentID is set;
// ExpectedChainID/ExpectedSender are the optional by-intent expected-scope
// cross-checks. Each supplied field is checked only against itself: an absent
// field is no cross-check at all (never a default, never inferred from the
// other field), so a request that supplies only one axis narrows the check to
// that axis.
type ReadRequest struct {
	BindingID       string
	IntentID        string
	ExpectedChainID *int64
	ExpectedSender  string
}

// Validate refuses a request that names no identity or both identities. Shape
// validation is deliberately absent: an unknown key is `not_bound`, and a
// malformed expected scope can never equal a durable fact, so it degrades to
// `mismatch` without inventing a status.
func (r ReadRequest) Validate() error {
	if (r.BindingID == "") == (r.IntentID == "") {
		return errors.New("read request needs exactly one of binding_id or intent_id")
	}
	return nil
}

// ReadAuthorization is the OC-5 authorization identity + version digest
// recorded at admission; the read provider returns it as a fact and never
// re-validates it.
type ReadAuthorization struct {
	ID      string `json:"id"`
	Version string `json:"version"`
}

// ReadBinding is the binding fact body (contracts/read-api.md §3.1/§3.2).
// Nonce is a decimal string; terminal outcomes add terminal_at and, for a
// released binding, release_operation_id.
type ReadBinding struct {
	BindingID          string            `json:"binding_id"`
	IntentID           string            `json:"intent_id"`
	ChainID            int64             `json:"chain_id"`
	Sender             string            `json:"sender"`
	Nonce              string            `json:"nonce"`
	State              string            `json:"state"`
	CreatedAt          time.Time         `json:"created_at"`
	Authorization      ReadAuthorization `json:"authorization"`
	RegistrySeq        int64             `json:"registry_seq"`
	TerminalAt         *time.Time        `json:"terminal_at,omitempty"`
	ReleaseOperationID string            `json:"release_operation_id,omitempty"`
}

// ReadGateCause is one active hold at the snapshot (OC-6 multi-cause
// visibility).
type ReadGateCause struct {
	HoldID        string    `json:"hold_id"`
	Cause         string    `json:"cause"`
	EstablishedAt time.Time `json:"established_at"`
}

// ReadGate is the 008-owned admission gate annotation. `open` means only "no
// 008-owned hold at this snapshot" — it is never an execution authorization.
type ReadGate struct {
	State  string          `json:"state"`
	Causes []ReadGateCause `json:"causes"`
}

// ReadRecovery is the read-only 006 state at the same snapshot; `unknown` is
// emitted when the 006 read fails and MUST be treated as not-clear.
type ReadRecovery struct {
	State string `json:"state"`
}

// ReadAnnotations carries the gate/recovery/registry facts of one snapshot.
type ReadAnnotations struct {
	Gate          ReadGate     `json:"gate"`
	Recovery      ReadRecovery `json:"recovery"`
	RegistryState string       `json:"registry_state"`
}

// ReadErrorBody is the machine-only error body shared by not_bound / mismatch
// / unavailable (no internal detail).
type ReadErrorBody struct {
	Code           string `json:"code"`
	RequestTraceID string `json:"request_trace_id"`
}

// ReadResponse is exactly one of the five outcomes plus the 401
// authentication answer. Only the fields the outcome defines are emitted:
// bound/terminal carry binding + annotations + the fixed notice; error
// outcomes carry error + notice; mismatch additionally echoes the durable
// scope (DurableBindingID/DurableChainID/DurableSender).
type ReadResponse struct {
	Outcome     Outcome          `json:"outcome"`
	Binding     *ReadBinding     `json:"binding,omitempty"`
	Annotations *ReadAnnotations `json:"annotations,omitempty"`
	Notice      string           `json:"notice"`
	Error       *ReadErrorBody   `json:"error,omitempty"`

	DurableBindingID string `json:"binding_id,omitempty"`
	DurableChainID   *int64 `json:"chain_id,omitempty"`
	DurableSender    string `json:"sender,omitempty"`
}

// HTTPStatus maps the response outcome to the contract's status code.
func (r ReadResponse) HTTPStatus() int {
	switch r.Outcome {
	case ReadBound, ReadTerminal:
		return http.StatusOK
	case ReadNotBound:
		return http.StatusNotFound
	case ReadMismatch:
		return http.StatusConflict
	case ReadUnavailable:
		return http.StatusServiceUnavailable
	case ReadUnauthenticated:
		return http.StatusUnauthorized
	default:
		return http.StatusServiceUnavailable
	}
}

// readTx is the transaction surface the read provider drives; pgx.Tx
// satisfies it.
type readTx interface {
	txQuerier
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// ReadProvider is the read-only entry point T017 mounts. token is the
// deployment's TXHARBOR_NONCE_READ_TOKEN (T016 wires it); the provider never
// logs or echoes it. gate, when supplied, is the startup rebuild gate: while
// it is not open the provider answers `unavailable` without touching the
// database (read-api.md §2).
type ReadProvider struct {
	token string
	gate  *RebuildGate
	begin func(ctx context.Context) (readTx, error)
}

// NewReadProvider wires the pool with the deployment read token. The
// transaction is REPEATABLE READ + read-write, per contracts/read-api.md §4.
// An optional startup rebuild gate (R5/FR-13) may be supplied; while it is
// closed every read answers the retryable `unavailable` before opening a
// transaction. No gate leaves the provider ungated (the unit-test seam).
func NewReadProvider(pool *pgxpool.Pool, token string, gate ...*RebuildGate) *ReadProvider {
	p := &ReadProvider{
		token: token,
		begin: func(ctx context.Context) (readTx, error) {
			return pool.BeginTx(ctx, pgx.TxOptions{
				IsoLevel:   pgx.RepeatableRead,
				AccessMode: pgx.ReadWrite,
			})
		},
	}
	if len(gate) > 0 {
		p.gate = gate[0]
	}
	return p
}

// Authenticate compares the presented bearer credential against the
// configured token in constant time. Both sides are hashed first so the
// comparison does not leak the token length; an unconfigured token admits
// nothing (fail closed).
func (p *ReadProvider) Authenticate(presented string) bool {
	if p.token == "" {
		return false
	}
	expected := sha256.Sum256([]byte(p.token))
	got := sha256.Sum256([]byte(presented))
	return subtle.ConstantTimeCompare(expected[:], got[:]) == 1
}

// ReadByBindingID answers GET /nonce/bindings/{binding_id}.
func (p *ReadProvider) ReadByBindingID(ctx context.Context, bindingID string) (ReadResponse, error) {
	return p.Read(ctx, ReadRequest{BindingID: bindingID})
}

// ReadByIntent answers GET /nonce/bindings/by-intent/{intent_id} with the
// optional expected-scope cross-checks.
func (p *ReadProvider) ReadByIntent(ctx context.Context, intentID string, chainID *int64, sender string) (ReadResponse, error) {
	return p.Read(ctx, ReadRequest{IntentID: intentID, ExpectedChainID: chainID, ExpectedSender: sender})
}

// Read runs one lookup in one REPEATABLE READ transaction. A closed rebuild
// gate, any 008 read failure, or a failed commit of the read-only transaction
// is all-or-nothing `unavailable`; the transaction performs lock acquisition +
// SELECT only, carries the shared 5s statement/lock guards, and commits without
// writing.
func (p *ReadProvider) Read(ctx context.Context, req ReadRequest) (ReadResponse, error) {
	if err := req.Validate(); err != nil {
		return ReadResponse{}, err
	}
	// R5/FR-13: a gate that is not open cannot produce a trustworthy answer, so
	// the whole read path fails closed as retryable `unavailable` before any DB
	// access (read-api.md §2 "rebuild gate not open"; §4 008-owned failure).
	if p.gate != nil && !p.gate.IsOpen() {
		_, reason := p.gate.State()
		if reason == "" {
			reason = "startup rebuild verification has not completed"
		}
		return unavailableResponse(), Refuse(ReadUnavailable, "rebuild gate not open: "+reason)
	}
	tx, err := p.begin(ctx)
	if err != nil {
		return unavailableResponse(), readUnavailable("open read transaction", err)
	}
	if err := applyReadGuards(ctx, tx); err != nil {
		_ = tx.Rollback(ctx)
		return unavailableResponse(), readUnavailable("set read transaction guards", err)
	}
	resp, err := readBindingOutcomeTx(ctx, tx, req)
	if err != nil {
		_ = tx.Rollback(ctx)
		return unavailableResponse(), readUnavailable("read failed", err)
	}
	if cerr := tx.Commit(ctx); cerr != nil {
		_ = tx.Rollback(ctx)
		// The one in-tx statement whose failure is deliberately swallowed is
		// the best-effort 006 probe (readRecoveryStateTx). On a real backend
		// that error also aborts the whole transaction, so COMMIT comes back
		// as a rollback. Every fact was already read before the probe under
		// the same snapshot, and the annotation is already degraded to
		// `unknown`, so serve the contract response rather than collapsing an
		// otherwise successful read to `unavailable` (read-api.md §4).
		if degradedBy006Failure(resp) {
			return resp, nil
		}
		return unavailableResponse(), readUnavailable("commit read transaction", cerr)
	}
	return resp, nil
}

// applyReadGuards bounds one read transaction with the shared statement and
// lock guards as its first statements. A failure here (including a lock or
// statement timeout) aborts the read transaction, which Read maps to
// `unavailable`.
func applyReadGuards(ctx context.Context, q txQuerier) error {
	for _, guard := range []string{localWriteGuard, readLockGuard} {
		if _, err := q.Exec(ctx, guard); err != nil {
			return fmt.Errorf("read transaction guard: %w", err)
		}
	}
	return nil
}

// degradedBy006Failure reports whether resp is a fully assembled fact response
// whose only in-tx problem was the best-effort 006 probe: a bound/terminal body
// carrying the `unknown` recovery degradation. That is exactly the case where
// an aborted commit must still yield the contract response, never a 503.
func degradedBy006Failure(resp ReadResponse) bool {
	return (resp.Outcome == ReadBound || resp.Outcome == ReadTerminal) &&
		resp.Annotations != nil && resp.Annotations.Recovery.State == RecoveryUnknown
}

// readBindingOutcomeTx is the in-tx read: identity lookup, expected-scope
// cross-check, then — for a hit — the scope-row FOR SHARE followed by the
// annotation reads of one snapshot. not_bound (no binding) and mismatch (the
// durable binding's scope contradicts the supplied expected scope) return
// before the durable-scope share: there is no durable scope to lock on the
// first, and the second echoes the immutable binding row read under this same
// snapshot with no annotation reads, so neither can straddle versions
// (read-api.md §4).
func readBindingOutcomeTx(ctx context.Context, q txQuerier, req ReadRequest) (ReadResponse, error) {
	// When the expected scope is complete, the scope-row lock is the first
	// statement (contract order); otherwise the identity lookup must run first
	// to learn the scope, and the lock follows before the other reads.
	locked := false
	if req.IntentID != "" && req.ExpectedChainID != nil && req.ExpectedSender != "" {
		exists, err := shareScopeRowTx(ctx, q, *req.ExpectedChainID, req.ExpectedSender)
		if err != nil {
			return ReadResponse{}, err
		}
		locked = exists
	}

	var (
		b   *Binding
		err error
	)
	switch {
	case req.BindingID != "":
		b, err = readBindingByIDTx(ctx, q, req.BindingID)
	default:
		b, err = readBindingByIntentTx(ctx, q, req.IntentID)
	}
	if err != nil {
		return ReadResponse{}, err
	}
	if b == nil {
		return notBoundResponse(), nil
	}

	if req.IntentID != "" {
		if (req.ExpectedChainID != nil && *req.ExpectedChainID != b.ChainID) ||
			(req.ExpectedSender != "" && req.ExpectedSender != b.Sender) {
			return mismatchResponse(b), nil
		}
	}

	if !locked {
		exists, err := shareScopeRowTx(ctx, q, b.ChainID, b.Sender)
		if err != nil {
			return ReadResponse{}, err
		}
		if !exists {
			return ReadResponse{}, fmt.Errorf(
				"binding %s exists without its scope row (inconsistent snapshot)", b.BindingID)
		}
	}

	holds, err := readActiveHoldsTx(ctx, q, b.ChainID, b.Sender)
	if err != nil {
		return ReadResponse{}, err
	}
	registry, err := readRegistryBySenderTx(ctx, q, b.ChainID, b.Sender)
	if err != nil {
		return ReadResponse{}, err
	}
	if registry == nil {
		return ReadResponse{}, fmt.Errorf(
			"binding %s exists without its registry row (inconsistent snapshot)", b.BindingID)
	}
	recovery := readRecoveryStateTx(ctx, q, b.ChainID)
	return readBindingResponse(b, holds, registry, recovery), nil
}

// readRecoveryStateTx reads the 006 recovery annotation. A read failure yields
// `unknown` — never none/released — on an otherwise successful response
// (contracts/read-api.md §4).
func readRecoveryStateTx(ctx context.Context, q txQuerier, chainID int64) string {
	var phase string
	switch err := q.QueryRow(ctx, readRecoveryPhaseSQL, chainID).Scan(&phase); {
	case err == nil:
		if phase == recoveryPhaseReconcileRequired {
			return RecoveryPausedReconcile
		}
		return RecoveryRecovering
	case !errors.Is(err, pgx.ErrNoRows):
		return RecoveryUnknown
	}
	var one int
	switch err := q.QueryRow(ctx, readRecoveryReleasedSQL, chainID).Scan(&one); {
	case err == nil:
		return RecoveryReleased
	case errors.Is(err, pgx.ErrNoRows):
		return RecoveryNone
	default:
		return RecoveryUnknown
	}
}

// readBindingResponse assembles the bound/terminal body plus the snapshot
// annotations and the fixed notice.
func readBindingResponse(b *Binding, holds []ScopeHold, reg *WalletRegistry, recovery string) ReadResponse {
	causes := make([]ReadGateCause, 0, len(holds))
	for i := range holds {
		causes = append(causes, ReadGateCause{
			HoldID:        holds[i].HoldID,
			Cause:         holds[i].Cause,
			EstablishedAt: holds[i].EstablishedAt.UTC(),
		})
	}
	gate := ReadGateOpen
	if len(causes) > 0 {
		gate = ReadGateHeld
	}

	rb := &ReadBinding{
		BindingID:     b.BindingID,
		IntentID:      b.IntentID,
		ChainID:       b.ChainID,
		Sender:        b.Sender,
		Nonce:         FormatDecimal(b.Nonce),
		State:         b.State,
		CreatedAt:     b.CreatedAt.UTC(),
		Authorization: ReadAuthorization{ID: b.AuthorizationID, Version: b.AuthorizationVersion},
		RegistrySeq:   b.RegistrySeq,
	}
	outcome := ReadBound
	switch b.State {
	case StateConsumed:
		outcome = ReadTerminal
		if b.ConsumedAt != nil {
			t := b.ConsumedAt.UTC()
			rb.TerminalAt = &t
		}
	case StateReleased:
		outcome = ReadTerminal
		if b.ReleasedAt != nil {
			t := b.ReleasedAt.UTC()
			rb.TerminalAt = &t
		}
		if b.ReleaseOperationID != nil {
			rb.ReleaseOperationID = *b.ReleaseOperationID
		}
	}

	return ReadResponse{
		Outcome: outcome,
		Binding: rb,
		Annotations: &ReadAnnotations{
			Gate:          ReadGate{State: gate, Causes: causes},
			Recovery:      ReadRecovery{State: recovery},
			RegistryState: reg.State,
		},
		Notice: ReadNoticeFacts,
	}
}

// notBoundResponse is the fail-closed 404 body.
func notBoundResponse() ReadResponse {
	return ReadResponse{
		Outcome: ReadNotBound,
		Error:   &ReadErrorBody{Code: string(ReadNotBound)},
		Notice:  ReadNoticeNotBound,
	}
}

// mismatchResponse is the 409 body: the durable binding is authoritative and
// its scope is echoed.
func mismatchResponse(b *Binding) ReadResponse {
	chainID := b.ChainID
	return ReadResponse{
		Outcome:          ReadMismatch,
		Error:            &ReadErrorBody{Code: string(ReadMismatch)},
		Notice:           ReadNoticeMismatch,
		DurableBindingID: b.BindingID,
		DurableChainID:   &chainID,
		DurableSender:    b.Sender,
	}
}

// unavailableResponse is the retryable 503 body; it never claims absence.
func unavailableResponse() ReadResponse {
	return ReadResponse{
		Outcome: ReadUnavailable,
		Error:   &ReadErrorBody{Code: string(ReadUnavailable)},
		Notice:  ReadNoticeUnavailable,
	}
}

// UnauthenticatedResponse is the 401 body for missing/invalid credentials
// (contracts/read-api.md §1); T017 emits it after Authenticate returns false.
func UnauthenticatedResponse() ReadResponse {
	return ReadResponse{
		Outcome: ReadUnauthenticated,
		Error:   &ReadErrorBody{Code: string(ReadUnauthenticated)},
		Notice:  ReadNoticeUnauthenticated,
	}
}

// readUnavailable maps an 008 read-path failure to the retryable read
// outcome, carrying the cause for the caller's logs (never secrets).
func readUnavailable(reason string, cause error) *Error {
	return Refuse(ReadUnavailable, reason).Wrap(cause)
}
