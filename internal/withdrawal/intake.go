// intake.go owns the receipt transaction core (T010, research R6-R8): the
// frozen SubmitWithdrawal surface the HTTP wiring (T014) consumes, the
// fixed-order 23505 classify, the FOR SHARE grant-lock receipt transaction, and
// the response-first pre-tx reject-audit intents (data-model Table 5 C3/N1)
// the transport persists AFTER the response is written. The in-tx `created`
// audit row stays inside the receipt transaction (atomic persistence, FR-11).
//
// Contract boundary (contracts/api.md §1-2): every defined outcome
// (201/200/409/422/401/403/503) is returned as (*SubmitResult, nil) carrying
// Status + Code + Message, plus RequestID on 201, 200 and 409-of-existing. A
// non-nil error is reserved for internal defects (nil pool, an already-dead
// context, a CSPRNG failure) and is always *Error TemporarilyUnavailable. 400
// (malformed_request) is never emitted here: no JSON body crosses this
// boundary; the HTTP handler owns body decoding (T014).
//
// Exactly one lock object exists on this path (R7 FINAL): SELECT ... FOR SHARE
// on the single grant row. No advisory lock, no coordinator lock, no
// ON CONFLICT, and never FOR UPDATE (grant.go's readGrant is the supply path's
// FOR UPDATE read and MUST NOT be reused here).
package withdrawal

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/events"
)

// Audit action vocabulary of withdrawal_request_audit (data-model Table 5).
const (
	auditActionCreated     = "created"
	auditActionReplayed    = "replayed"
	auditActionConflict    = "conflict"
	auditActionRejected    = "rejected"
	auditActionAuthFailed  = "auth_failed"
	auditActionUnavailable = "unavailable"
)

// Receipt-path constants.
const (
	intakeRequestIDPrefix          = "wr-"
	intakeRejectMarkerPrefix       = "rej-"
	intakeRequestIDEntropyBytes    = 16
	intakeRejectMarkerEntropyBytes = 8
	// intakeRejectAuditTimeout is the detached write bound (data-model Table 5
	// C3/N1): the audit write is independent of the request context and is
	// attempted exactly once, never retried.
	intakeRejectAuditTimeout = 2 * time.Second
	// intakeUnavailableMessage is the only message the 503 paths return. It
	// never claims "definitely not created" and never says "Accepted"; it
	// tells the caller to retry with the SAME key and parameters.
	intakeUnavailableMessage = "withdrawal storage unavailable or the submission outcome is unknown;" +
		" retry with the same idempotency key and the same parameters (never rotate the key)"
	// intakeCapacityRefusedMessage is the PD-2 refusal (T070): the same
	// retryable 503 channel as the T062 limiter-unavailable path, with the
	// capacity-boundary cause. It never claims the request was created and
	// never suggests rotating the idempotency key.
	intakeCapacityRefusedMessage = "withdrawal intake is temporarily paused while the event backlog is at the capacity boundary;" +
		" retry with the same idempotency key and the same parameters (never rotate the key)"
	// intakeCapacityUnavailableMessage is the fail-closed refusal when the
	// capacity state itself cannot be observed: an unreadable state never
	// admits a new controllable write.
	intakeCapacityUnavailableMessage = "withdrawal intake is temporarily unavailable while the event backlog state is unknown;" +
		" retry with the same idempotency key and the same parameters (never rotate the key)"
)

// SQL statement scripts. intakeSelectGrantForShareSQL is the receipt path's
// sole lock (R7 FINAL); it reads the same shape as grant.go's grantRow but with
// FOR SHARE, never grant.go's FOR UPDATE supply read.
const (
	intakeWriteGuard = "SET LOCAL statement_timeout = '5s'"

	intakeSelectGrantForShareSQL = `
SELECT caller_id, chain_id, asset, recipient, amount::text, state, expires_at
FROM withdrawal_authorizations
WHERE authorization_id = $1
FOR SHARE`

	intakeSelectClockSQL = `SELECT clock_timestamp()`

	intakeSelectByKeySQL = `
SELECT request_id, authorization_id, chain_id, asset, recipient, amount::text
FROM withdrawal_requests
WHERE caller_id = $1 AND idempotency_key = $2`

	intakeSelectByAuthSQL = `
SELECT request_id FROM withdrawal_requests WHERE authorization_id = $1`

	intakeInsertRequestSQL = `
INSERT INTO withdrawal_requests
    (request_id, caller_id, idempotency_key, authorization_id, chain_id, asset, recipient, amount)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8::numeric)`

	intakeInsertAuditSQL = `
INSERT INTO withdrawal_request_audit (request_id, caller_id, action, detail)
VALUES ($1, $2, $3, $4)`
)

// SubmitRequest is one create-withdrawal attempt as the transport hands it to
// the core. caller_id is never carried: it comes from Authenticate. The
// addresses may be any FR-07 legal case form; they are canonicalized before
// compare and persist. Allowlist is a pre-resolved asset whitelist for tests
// and embedded callers; ResolveAllowlist, when non-nil, is the live FR-05
// source (e.g. ResolveAssetAllowlist over deposit_config_history) and takes
// precedence. Whichever supplies the list, membership is enforced ONLY on the
// first-create path, after the step-4 replay fast path (contracts §2
// permanent-replay invariant: only steps (1)–(2) are re-evaluated on replay).
// The core never inherits a list silently and never falls back to an
// empty/full-chain default.
//
// CapacityGate is the optional 013 PD-2 capacity admission surface (T070).
// When non-nil it is consulted exactly once per FIRST create, after the
// replay fast path and the allowlist gate, immediately before the receipt
// transaction: a soft/hard backlog refuses the new controllable funding write
// with the retryable 503 channel and capacity_refusals_total{op_class}. It
// never runs for replays or existing requests, never touches Redis, and never
// replaces, loosens or tightens any existing gate (all of them still run
// below for an admitted request). Nil keeps the PG-only baseline unchanged.
type SubmitRequest struct {
	PresentedKey     string
	IdempotencyKey   string
	ChainID          int64
	ExpectedChainID  int64
	Asset            string
	Recipient        string
	Amount           string
	AuthorizationID  string
	Allowlist        []string
	ResolveAllowlist func(context.Context) ([]string, error)
	CapacityGate     CapacityAdmitter
}

// CapacityAdmitter is the narrow PD-2 admission surface the intake consumes
// (T070); the concrete implementation is events.CapacityGuard. It evaluates
// the backlog before a new controllable work item is admitted and returns
// events.CapacityAdmission{Refused,...}. A non-nil error means the capacity
// state was unreadable: the intake fails closed for the NEW write (retryable
// 503) and must never treat an unreadable state as capacity available.
type CapacityAdmitter interface {
	AdmitNewControllable(ctx context.Context, opClass string) (events.CapacityAdmission, error)
}

// SubmitResult is the classified outcome of one attempt. Status is the HTTP
// status of the contract table; Code/Message describe the failure and are
// empty/"accepted" language on success. RequestID is set on 201, on a 200
// replay, and on a 409 conflict against an existing row (never on 401/403/422/
// 503).
//
// Audit is the pre-tx reject audit the transport MUST persist AFTER the
// response is written (response-first rule). It is nil on a 201 (its in-tx
// `created` row is written atomically inside the receipt transaction) and on
// the 401/503 auth-failure paths (no verifiable caller identity).
//
// Asset/Recipient/Amount are the canonical persisted facts of a successful
// create: the lowercase 0x addresses and the exact FR-06 amount string. They are
// set from the canonicalized request on a 201 and from the STORED row on a 200
// replay, so the transport echoes exactly what a later GET returns — never the
// caller's original case form. They are empty on every error outcome (the 409
// error shape carries no asset echo).
type SubmitResult struct {
	Status    int
	Code      Code
	Message   string
	RequestID string
	Asset     string
	Recipient string
	Amount    string
	Audit     *AuditIntent
}

// AuditIntent is one pre-tx reject audit the transport persists after the
// response. CallerID is the verified caller; RequestID is the existing "wr-…"
// id or "" for a never-created rejection (the writer mints a "rej-…" marker);
// Action is a Table 5 vocabulary member; Detail is the human explanation.
type AuditIntent struct {
	CallerID  int64
	RequestID string
	Action    string
	Detail    string
}

// 013 event emission vocabulary (T029; contracts/events.md §3). Aggregate
// type/state mirror the 007 receive-only facts; they never reinterpret them.
const (
	withdrawalRequestAggregateType = "withdrawal_request"
	withdrawalRequestStateAccepted = "accepted"
	withdrawalRequestSourceKind    = "withdrawal_intake"
)

// appendWithdrawalRequestReceivedEvent emits withdrawal.request.received for a
// first receipt (T029) inside the receipt transaction (T1), so the request row
// and its event commit or roll back together. Accepted means "received", not
// an execution authorization; the event is a fact notification and is never
// read as a send permission (FR-05).
func appendWithdrawalRequestReceivedEvent(ctx context.Context, tx pgx.Tx, requestID string, callerID, chainID int64) error {
	ev := events.Event{
		EventType:     events.EventTypeWithdrawalRequestReceived,
		SchemaVersion: events.SchemaVersionV1,
		IdentityKind:  events.IdentityKindBusinessObject,
		AggregateType: withdrawalRequestAggregateType,
		AggregateID:   requestID,
		Payload: map[string]any{
			"request_id": requestID,
			"caller":     callerID,
			"state":      withdrawalRequestStateAccepted,
			"chain_id":   chainID,
		},
		OccurredAt: time.Now().UTC(),
		SourceKind: withdrawalRequestSourceKind,
		SourceID:   requestID,
	}
	if _, err := events.Append(ctx, tx, ev); err != nil {
		return fmt.Errorf("append withdrawal request received event %s: %w", requestID, err)
	}
	return nil
}

// submitParams is the canonicalized FR-10 comparison set for one attempt
// (caller_id is the lookup key, so it is not repeated here).
type submitParams struct {
	chainID         int64
	asset           string
	recipient       string
	amount          string
	authorizationID string
}

// requestRow is one stored withdrawal_requests row read by the classify path.
type requestRow struct {
	requestID       string
	authorizationID string
	chainID         int64
	asset           string
	recipient       string
	amount          string
}

// matches reports FR-10 equality: chain_id, asset, recipient, amount and
// authorization_id must all be equal. Addresses are already canonical lowercase
// on both sides, so equality is exact; amount compares as the exact decimal
// string (FR-06 forbids any non-canonical transport form).
func (r requestRow) matches(p submitParams) bool {
	return r.authorizationID == p.authorizationID && r.chainID == p.chainID &&
		r.asset == p.asset && r.recipient == p.recipient && r.amount == p.amount
}

// SubmitWithdrawal runs one withdrawal create attempt in the contracts §2
// order: (1) auth, (2) interface permission, (3) input shape/semantics, (4)
// pre-tx existing-key fast path, (4b) first-create-only live allowlist gate,
// (5) the receipt transaction. Step (1) is the single authentication origin:
// the transport passes only the presented key, never a pre-authenticated
// identity. Every defined outcome returns (*SubmitResult, nil); a non-nil
// error means an internal defect (nil pool, already-dead context) before any
// work started.
func SubmitWithdrawal(ctx context.Context, pool *pgxpool.Pool, req SubmitRequest) (*SubmitResult, error) {
	if pool == nil {
		return nil, storageUnavailable("submit withdrawal", errors.New("nil pool"))
	}
	if err := ctx.Err(); err != nil {
		return nil, storageUnavailable("submit withdrawal", err)
	}

	// (1) Authenticate. A 401 or a storage 503 is a contract outcome, not a
	// defect. No audit row is written on this path (no verifiable identity).
	auth, err := Authenticate(ctx, pool, req.PresentedKey)
	if err != nil {
		return intakeResultFromError(err), nil
	}
	callerID := auth.Caller.ID

	// (2) Interface permission. Permission-denied uses the `rejected` action
	// (data-model Table 5: auth_failed is reserved for the post-commit
	// grant-bound path).
	if !auth.Caller.CanCreate {
		return &SubmitResult{
			Status:  403,
			Code:    CodeUnauthorized,
			Message: "caller lacks interface permission to create withdrawals",
			Audit:   auditIntent(callerID, "", auditActionRejected, "caller lacks interface permission"),
		}, nil
	}

	// (3) Input shape/semantics (validation order, contracts §2 step 3).
	norm, verr := normalizeSubmit(req)
	if verr != nil {
		return &SubmitResult{
			Status:  422,
			Code:    CodeValidationFailed,
			Message: intakeErrorMessage(verr),
			Audit:   auditIntent(callerID, "", auditActionRejected, verr.Error()),
		}, nil
	}

	// (4) Pre-tx fast path: read-only, optimization only. The UNIQUE index in
	// the tx below stays authoritative. The replay path re-checks only current
	// key-auth + interface permission (already done); it MUST NOT re-validate
	// the original grant row (Q5 replay immunity).
	row, found, cerr := readRequestByKey(ctx, pool, callerID, req.IdempotencyKey)
	if cerr != nil {
		return intakeResultFromError(cerr), nil
	}
	if found {
		if row.matches(norm) {
			return &SubmitResult{
				Status:    200,
				Message:   "withdrawal request already accepted",
				RequestID: row.requestID,
				Asset:     row.asset,
				Recipient: row.recipient,
				Amount:    row.amount,
				Audit:     auditIntent(callerID, row.requestID, auditActionReplayed, "same key and parameters"),
			}, nil
		}
		return &SubmitResult{
			Status:    409,
			Code:      CodeIdempotencyConflict,
			Message:   "idempotency key already used with different parameters",
			RequestID: row.requestID,
			Audit:     auditIntent(callerID, row.requestID, auditActionConflict, "same idempotency key with different parameters"),
		}, nil
	}

	// (4b) First-create-only FR-05 gate, reached solely on a fast-path miss:
	// resolve the live list when the transport supplied a resolver, then
	// enforce membership. A replay never reaches this point, so a later
	// policy removal cannot turn an accepted request's replay into a 422
	// (contracts §2). An unreadable source is retryable, never a fallback.
	allowlist := req.Allowlist
	if req.ResolveAllowlist != nil {
		resolved, rerr := req.ResolveAllowlist(ctx)
		if rerr != nil {
			return &SubmitResult{
				Status:  503,
				Code:    CodeTemporarilyUnavailable,
				Message: intakeUnavailableMessage,
				Audit:   auditIntent(callerID, "", auditActionUnavailable, "storage failure while resolving withdrawal allowlist"),
			}, nil
		}
		allowlist = resolved
	}
	if werr := ValidateAssetWhitelisted(norm.asset, allowlist); werr != nil {
		verr := intakeAsError(werr)
		return &SubmitResult{
			Status:  422,
			Code:    CodeValidationFailed,
			Message: intakeErrorMessage(verr),
			Audit:   auditIntent(callerID, "", auditActionRejected, verr.Error()),
		}, nil
	}

	// (4c) 013 capacity gate (T070; PD-2; contracts/capacity.md §3). It runs
	// ONLY for a first create (the step-4 replay fast path already returned
	// 200/409 above, so an accepted request and every replay of it are never
	// refused here), and BEFORE the receipt transaction, so nothing is
	// admitted and no existing gate below is skipped or weakened. The
	// decision reads PostgreSQL only (Redis never participates); a soft or
	// hard backlog refuses with the same retryable 503 channel as T062, an
	// unreadable state refuses too (fail closed) and never admits.
	if refusal := capacityAdmissionRefusal(ctx, callerID, req.CapacityGate); refusal != nil {
		return refusal, nil
	}

	// (5) Receipt transaction.
	return submitInTx(ctx, pool, callerID, req, norm)
}

// capacityAdmissionRefusal evaluates the optional PD-2 gate for one first
// create. It returns nil when the request may proceed (no gate configured, or
// the backlog is below the soft boundary) and the retryable 503 outcome when
// the new controllable write must be refused. It is pure apart from the
// injected gate call, so the refusal mapping is unit-testable without a
// database.
func capacityAdmissionRefusal(ctx context.Context, callerID int64, gate CapacityAdmitter) *SubmitResult {
	if gate == nil {
		return nil
	}
	admission, err := gate.AdmitNewControllable(ctx, events.CapacityOpWithdrawalCreate)
	if err != nil {
		return &SubmitResult{
			Status:  503,
			Code:    CodeTemporarilyUnavailable,
			Message: intakeCapacityUnavailableMessage,
			Audit:   auditIntent(callerID, "", auditActionUnavailable, "capacity state unreadable while admitting a new withdrawal"),
		}
	}
	if !admission.Refused {
		return nil
	}
	return &SubmitResult{
		Status:  503,
		Code:    CodeTemporarilyUnavailable,
		Message: intakeCapacityRefusedMessage,
		Audit: auditIntent(callerID, "", auditActionUnavailable,
			fmt.Sprintf("capacity boundary reached (level=%s pending=%d); new controllable write refused (PD-2)",
				admission.Level, admission.PendingTotal)),
	}
}

// submitInTx owns BEGIN..COMMIT for one receipt (T-accept): writeGuard →
// FOR SHARE grant row → clock_timestamp() → validity + FR-10 equality → plain
// INSERT request → audit → COMMIT. The request_id is minted before BEGIN: a
// unique collision (astronomical) surfaces as 23505 and falls through the
// fixed-order classify to a retryable 503, never to a replay.
func submitInTx(ctx context.Context, pool *pgxpool.Pool, callerID int64, req SubmitRequest, p submitParams) (*SubmitResult, error) {
	requestID, err := mintRequestID()
	if err != nil {
		return nil, storageUnavailable("mint withdrawal request id", err)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return intakeUnavailableResult(tx, pool, callerID, ctx), nil
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op after COMMIT

	if _, err := tx.Exec(ctx, intakeWriteGuard); err != nil {
		return intakeUnavailableResult(tx, pool, callerID, ctx), nil
	}

	// Sole lock on this path: FOR SHARE on the one grant row. A miss locks
	// nothing, so it rolls straight back to 403 rather than treating a missing
	// row as held.
	g, found, err := intakeReadGrantForShare(ctx, tx, p.authorizationID)
	if err != nil {
		return intakeUnavailableResult(tx, pool, callerID, ctx), nil
	}
	if !found {
		return intakeRejectResult(tx, pool, callerID, ctx, "authorization is missing"), nil
	}

	// A separate statement, evaluated AFTER the lock wait: now()/
	// transaction_timestamp() is fixed at tx start and MUST NOT back this
	// check (R8 clock protocol).
	var tCheck time.Time
	if err := tx.QueryRow(ctx, intakeSelectClockSQL).Scan(&tCheck); err != nil {
		return intakeUnavailableResult(tx, pool, callerID, ctx), nil
	}
	if !intakeGrantValid(*g, callerID, p, tCheck) {
		return intakeRejectResult(tx, pool, callerID, ctx,
			"authorization is not active or does not match the request"), nil
	}

	tag, err := tx.Exec(ctx, intakeInsertRequestSQL,
		requestID, callerID, req.IdempotencyKey, p.authorizationID, p.chainID, p.asset, p.recipient, p.amount)
	if err != nil {
		if pgErr := grantPgError(err); pgErr != nil {
			switch pgErr.Code {
			case "23505":
				// Aborted tx: ROLLBACK, then classify on the pool in fixed
				// order (R8/Table 6 T-dual-race). The reported ConstraintName
				// is never a semantic signal.
				_ = tx.Rollback(ctx)
				return classifyFixedOrder(ctx, pool, callerID, req.IdempotencyKey, p), nil
			case "23514":
				// CHECK violations are pre-covered by validate.go; map to 422
				// and never into the 23505 classify path.
				_ = tx.Rollback(ctx)
				return &SubmitResult{
					Status:  422,
					Code:    CodeValidationFailed,
					Message: "withdrawal request failed storage validation",
					Audit:   auditIntent(callerID, "", auditActionRejected, "request failed a storage constraint"),
				}, nil
			}
		}
		// Everything else (40P01 deadlock, 57014 statement timeout, connection
		// loss, ...) is retryable with the same key.
		return intakeUnavailableResult(tx, pool, callerID, ctx), nil
	}
	if tag.RowsAffected() != 1 {
		return intakeUnavailableResult(tx, pool, callerID, ctx), nil
	}

	if err := insertReceiptAudit(ctx, tx, requestID, callerID, auditActionCreated, "first receipt"); err != nil {
		return intakeUnavailableResult(tx, pool, callerID, ctx), nil
	}

	// 013 T029: the receipt and its withdrawal.request.received event commit
	// in this same transaction (T1). The event carries Accepted semantics
	// only: it is not an execution authorization and delivery is not part of
	// the receive contract (FR-05; contracts/events.md §3). An append failure
	// rolls the receipt back (retryable 503 with the SAME key, never a
	// half-committed receipt).
	if err := appendWithdrawalRequestReceivedEvent(ctx, tx, requestID, callerID, p.chainID); err != nil {
		return intakeUnavailableResult(tx, pool, callerID, ctx), nil
	}

	if err := tx.Commit(ctx); err != nil {
		// Uncertain commit: dual re-read in the same fixed order. A hit with
		// equality is our own committed row (200); otherwise the peer's
		// 200/409, or a retryable 503. Never a new key, never an "Accepted"
		// claim on an unconfirmed commit.
		return classifyFixedOrder(ctx, pool, callerID, req.IdempotencyKey, p), nil
	}
	return &SubmitResult{
		Status:    201,
		Message:   "withdrawal request accepted",
		RequestID: requestID,
		Asset:     p.asset,
		Recipient: p.recipient,
		Amount:    p.amount,
	}, nil
}

// classifyFixedOrder is the only allowed post-23505 diagnostic (R8): read by
// (caller_id, idempotency_key) first, then by authorization_id on a key miss,
// then retryable. A key 409 is never downgraded to 403 by a co-fired auth
// violation, and a same-key replay is never upgraded. It is reused for the
// commit-unknown re-read.
func classifyFixedOrder(ctx context.Context, pool *pgxpool.Pool, callerID int64, key string, p submitParams) *SubmitResult {
	row, found, err := readRequestByKey(ctx, pool, callerID, key)
	if err != nil {
		return intakeUnavailableResult(nil, pool, callerID, ctx)
	}
	if found {
		if row.matches(p) {
			return &SubmitResult{
				Status:    200,
				Message:   "withdrawal request already accepted",
				RequestID: row.requestID,
				Asset:     row.asset,
				Recipient: row.recipient,
				Amount:    row.amount,
				Audit:     auditIntent(callerID, row.requestID, auditActionReplayed, "same key and parameters"),
			}
		}
		return &SubmitResult{
			Status:    409,
			Code:      CodeIdempotencyConflict,
			Message:   "idempotency key already used with different parameters",
			RequestID: row.requestID,
			Audit:     auditIntent(callerID, row.requestID, auditActionConflict, "same idempotency key with different parameters"),
		}
	}
	bound, err := requestBoundToAuthorization(ctx, pool, p.authorizationID)
	if err != nil {
		return intakeUnavailableResult(nil, pool, callerID, ctx)
	}
	if bound {
		// T-auth-bound: the grant is already consumed by another request. Never
		// attributing the audit to that other request's identity (rej- marker).
		return &SubmitResult{
			Status:  403,
			Code:    CodeAuthorizationInvalid,
			Message: "authorization is already bound to another withdrawal request",
			Audit:   auditIntent(callerID, "", auditActionAuthFailed, "authorization already bound to another request"),
		}
	}
	// Dual miss: a request_id collision on a fresh key, a winner not yet
	// visible, or a genuine storage failure. Retryable with the SAME key.
	return intakeUnavailableResult(nil, pool, callerID, ctx)
}

// readRequestByKey reads one request row for the (caller, key) lookup scope.
func readRequestByKey(ctx context.Context, pool *pgxpool.Pool, callerID int64, key string) (requestRow, bool, error) {
	var r requestRow
	err := pool.QueryRow(ctx, intakeSelectByKeySQL, callerID, key).
		Scan(&r.requestID, &r.authorizationID, &r.chainID, &r.asset, &r.recipient, &r.amount)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return requestRow{}, false, nil
	case err != nil:
		return requestRow{}, false, storageUnavailable("classify withdrawal request by key", err)
	}
	return r, true, nil
}

// requestBoundToAuthorization reports whether any request already binds the
// grant (the authorization_id UNIQUE carrier).
func requestBoundToAuthorization(ctx context.Context, pool *pgxpool.Pool, authorizationID string) (bool, error) {
	var requestID string
	err := pool.QueryRow(ctx, intakeSelectByAuthSQL, authorizationID).Scan(&requestID)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return false, nil
	case err != nil:
		return false, storageUnavailable("classify withdrawal request by authorization", err)
	}
	return true, nil
}

// normalizeSubmit validates and canonicalizes one attempt before any pool use:
// key shape (FR-09), chain bind (FR-04), authorization id presence, asset shape
// (FR-07), recipient shape (FR-07), amount shape + range (FR-06). Addresses are
// lowercased here, so stored rows compare by equality. FR-05 whitelist
// membership is deliberately NOT checked here: it is a mutable policy read and
// belongs to the first-create path after the step-4 replay fast path
// (contracts §2 permanent-replay invariant).
func normalizeSubmit(req SubmitRequest) (submitParams, *Error) {
	if err := ValidateIdempotencyKey(req.IdempotencyKey); err != nil {
		return submitParams{}, intakeAsError(err)
	}
	if err := ValidateChainID(req.ChainID, req.ExpectedChainID); err != nil {
		return submitParams{}, intakeAsError(err)
	}
	if req.AuthorizationID == "" {
		return submitParams{}, New(CodeValidationFailed, "authorization_id is required").WithField("authorization_id")
	}
	asset, err := canonicalAddressField(req.Asset, "asset")
	if err != nil {
		return submitParams{}, intakeAsError(err)
	}
	recipient, err := canonicalAddressField(req.Recipient, "recipient")
	if err != nil {
		return submitParams{}, intakeAsError(err)
	}
	if _, err := ValidateAmount(req.Amount); err != nil {
		return submitParams{}, intakeAsError(err)
	}
	return submitParams{
		chainID:         req.ChainID,
		asset:           asset,
		recipient:       recipient,
		amount:          req.Amount,
		authorizationID: req.AuthorizationID,
	}, nil
}

// intakeReadGrantForShare reads the one grant row with FOR SHARE (the sole lock
// on this path). A missing row locks nothing.
func intakeReadGrantForShare(ctx context.Context, tx pgx.Tx, authorizationID string) (*grantRow, bool, error) {
	var g grantRow
	err := tx.QueryRow(ctx, intakeSelectGrantForShareSQL, authorizationID).
		Scan(&g.callerID, &g.chainID, &g.asset, &g.recipient, &g.amount, &g.state, &g.expiresAt)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return nil, false, nil
	case err != nil:
		return nil, false, fmt.Errorf("read authorization for share: %w", err)
	}
	return &g, true, nil
}

// intakeGrantValid decides validity with the post-lock clock: state must be
// active, expires_at NULL or strictly greater than tCheck (`=` is expired), and
// the grant must bind this caller and the full FR-10 parameter set.
func intakeGrantValid(g grantRow, callerID int64, p submitParams, tCheck time.Time) bool {
	if g.state != "active" {
		return false
	}
	if g.expiresAt != nil && !g.expiresAt.After(tCheck) {
		return false
	}
	return g.callerID == callerID && g.chainID == p.chainID &&
		g.asset == p.asset && g.recipient == p.recipient && g.amount == p.amount
}

// mintRequestID returns the opaque public id: "wr-" + 32 lowercase hex
// characters (16 CSPRNG bytes). The DB UNIQUE is the authority against
// collisions.
func mintRequestID() (string, error) {
	var b [intakeRequestIDEntropyBytes]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("mint withdrawal request id: %w", err)
	}
	return intakeRequestIDPrefix + hex.EncodeToString(b[:]), nil
}

// mintRejectMarker returns the opaque never-created audit marker: "rej-" + 16
// lowercase hex characters (8 CSPRNG bytes). It never fabricates a
// withdrawal_requests identity; a CSPRNG failure still yields a non-attributable
// marker.
func mintRejectMarker() string {
	var b [intakeRejectMarkerEntropyBytes]byte
	if _, err := rand.Read(b[:]); err != nil {
		return intakeRejectMarkerPrefix + "unknown"
	}
	return intakeRejectMarkerPrefix + hex.EncodeToString(b[:])
}

// statusForCode maps every machine code to its contracts §1 HTTP status.
func statusForCode(code Code) int {
	switch code {
	case CodeMalformedRequest:
		return 400
	case CodeValidationFailed:
		return 422
	case CodeUnauthenticated:
		return 401
	case CodeUnauthorized, CodeAuthorizationInvalid:
		return 403
	case CodeIdempotencyConflict, CodeOperationConflict:
		return 409
	case CodeNotFound:
		return 404
	case CodeTemporarilyUnavailable:
		return 503
	default:
		return 500
	}
}

// intakeErrorMessage renders a classified error for the wire: the offending
// field is appended when known, and every 503 gets the single retry message.
func intakeErrorMessage(e *Error) string {
	if statusForCode(e.Code) == 503 {
		return intakeUnavailableMessage
	}
	if e.Field != "" {
		return e.msg + " (field: " + e.Field + ")"
	}
	return e.msg
}

// intakeResultFromError turns a classified library error into a contract
// outcome. An unclassified error is treated as storage-unavailable.
func intakeResultFromError(err error) *SubmitResult {
	var e *Error
	if !errors.As(err, &e) {
		return &SubmitResult{Status: 503, Code: CodeTemporarilyUnavailable, Message: intakeUnavailableMessage}
	}
	return &SubmitResult{Status: statusForCode(e.Code), Code: e.Code, Message: intakeErrorMessage(e)}
}

// intakeUnavailableResult rolls back the receipt tx (when one is open) and
// returns the retryable 503 outcome carrying the `unavailable` audit intent
// the transport persists after the response.
func intakeUnavailableResult(tx pgx.Tx, _ *pgxpool.Pool, callerID int64, ctx context.Context) *SubmitResult {
	if tx != nil {
		_ = tx.Rollback(ctx)
	}
	return &SubmitResult{
		Status:  503,
		Code:    CodeTemporarilyUnavailable,
		Message: intakeUnavailableMessage,
		Audit:   auditIntent(callerID, "", auditActionUnavailable, "storage failure while submitting withdrawal"),
	}
}

// intakeRejectResult rolls back the receipt tx (discarding any in-tx audit) and
// returns the 403 outcome carrying exactly ONE `rejected` audit intent.
func intakeRejectResult(tx pgx.Tx, _ *pgxpool.Pool, callerID int64, ctx context.Context, detail string) *SubmitResult {
	_ = tx.Rollback(ctx)
	return &SubmitResult{
		Status:  403,
		Code:    CodeAuthorizationInvalid,
		Message: "authorization is missing, inactive, or does not match the request",
		Audit:   auditIntent(callerID, "", auditActionRejected, detail),
	}
}

// auditIntent builds a pre-tx reject audit intent, or nil when there is no
// verifiable caller identity (the writer's own no-op guard).
func auditIntent(callerID int64, requestID, action, detail string) *AuditIntent {
	if callerID <= 0 {
		return nil
	}
	return &AuditIntent{CallerID: callerID, RequestID: requestID, Action: action, Detail: detail}
}

// WriteRejectAudit appends exactly one best-effort row to
// withdrawal_request_audit in its own statement, on a DETACHED 2s context:
// the caller's context is deliberately unused so a cancelled request still gets
// one attempt, and the call never blocks or alters the already-decided
// response. There is no retry, no queue, and no background补记; the returned
// error is for observability and the caller normally ignores it.
//
// requestID carries the returned-or-would-be id: the existing "wr-…" for
// replays/conflicts, or an empty string to mint a "rej-…" opaque marker for a
// never-created rejection. It never fabricates a withdrawal_requests identity.
// A nil pool returns an error, so a caller can detect the defect instead of
// panicking.
func WriteRejectAudit(ctx context.Context, pool *pgxpool.Pool, callerID int64, requestID, action, detail string) error {
	_ = ctx // intentionally unused: the write is detached from the request
	if pool == nil {
		return storageUnavailable("write withdrawal reject audit", errors.New("nil pool"))
	}
	if callerID <= 0 {
		return New(CodeValidationFailed, "caller_id must be positive").WithField("caller_id")
	}
	if !validAuditAction(action) {
		return New(CodeValidationFailed, "unknown withdrawal audit action").WithField("action")
	}
	if requestID == "" {
		requestID = mintRejectMarker()
	}
	writeCtx, cancel := context.WithTimeout(context.Background(), intakeRejectAuditTimeout)
	defer cancel()
	if _, err := pool.Exec(writeCtx, intakeInsertAuditSQL, requestID, callerID, action, detail); err != nil {
		return storageUnavailable("write withdrawal reject audit", err)
	}
	return nil
}

// validAuditAction reports membership in the Table 5 action CHECK vocabulary.
func validAuditAction(action string) bool {
	switch action {
	case auditActionCreated, auditActionReplayed, auditActionConflict,
		auditActionRejected, auditActionAuthFailed, auditActionUnavailable:
		return true
	default:
		return false
	}
}

// insertReceiptAudit appends the in-tx receipt audit row and asserts exactly one
// row (it is discarded with the tx on rollback).
func insertReceiptAudit(ctx context.Context, tx pgx.Tx, requestID string, callerID int64, action, detail string) error {
	tag, err := tx.Exec(ctx, intakeInsertAuditSQL, requestID, callerID, action, detail)
	if err != nil {
		return fmt.Errorf("insert withdrawal audit: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("insert withdrawal audit affected %d rows, want 1", tag.RowsAffected())
	}
	return nil
}

// intakeAsError narrows a validator error to *Error, keeping an unexpected
// error class as a validation failure rather than dropping it.
func intakeAsError(err error) *Error {
	var e *Error
	if errors.As(err, &e) {
		return e
	}
	return New(CodeValidationFailed, err.Error())
}
