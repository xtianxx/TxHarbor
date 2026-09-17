// delivery.go owns the T-deliver implementation (T034; FR-14/FR-17/FR-23,
// contracts/persistence.md §§2/5/6, contracts/gates.md §3, research R6): the
// final delivery protocol — fixed lock order, in-lock gate re-read, one
// protected region (admission INSERT + bytes write + delivered marker UPDATE +
// COMMIT), the persisted delivery-unknown basis, and same-identity re-gate
// recovery by re-reading the durable result.
//
// 009 never broadcasts: the response bytes leave only through the caller's
// DeliverySink, which is the local transport (HTTP response writer). This file
// imports no RPC/dial package, and delivery never re-signs — the persisted
// `signature_results` row is the only signing material and is re-delivered
// byte-identically. There is no TTL/`valid_until` permission window: every
// attempt re-passes the current gates, so revocation/expiry/pause block even a
// byte-identical redelivery, and fault-guarantee scope is honest (a failed
// write may already have delivered bytes → `unknown`, never "nothing
// delivered").
package signer

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// deliverySendGuard bounds every statement of the delivery transaction, the
// LOCK TABLE wait included (the repo's existing 5s guard literal). The send
// region's network bound is the transport WriteTimeout (serve path), not this
// statement bound.
const deliverySendGuard = "SET LOCAL statement_timeout = '5s'"

// deliveryRowSQL reads the durable request identity and the persisted result
// (JOIN: a request without a result has nothing to deliver). A committed
// `delivered` marker is deliberately NOT read here: it records that a delivery
// did happen (never rewritten or retracted), but it is not a retransmit permit
// — every attempt re-passes the current gates, so replay re-gates from scratch
// (gates.md §3).
const deliveryRowSQL = `SELECT
  r.id, r.caller_id, r.signing_request_id, r.attempt_id, r.intent_id, r.binding_ref,
  r.recovery_version, r.chain_id, r.sender, r.authorization_id,
  r.authorization_fingerprint, r.authorization_state, r.authorization_version,
  r.asset, r.recipient, r.amount::text,
  r.state, s.signature, s.tx_hash
FROM signing_requests r
JOIN signature_results s ON s.signing_request_row = r.id
WHERE r.caller_id = $1 AND r.signing_request_id = $2`

// deliveryLockOwnSQL takes the own request row FOR UPDATE: the same-identity
// send-region serialization at the tail of the fixed lock order (persistence.md
// §2). The unique (request, attempt_seq) index backstops it.
const deliveryLockOwnSQL = `SELECT id FROM signing_requests WHERE id = $1 FOR UPDATE`

// deliveryNextSeqSQL is attempt_seq (1 = first response, >1 = retries).
const deliveryNextSeqSQL = `SELECT COALESCE(MAX(attempt_seq), 0) + 1
  FROM delivery_admissions WHERE signing_request_row = $1`

// deliveryCanSignSQL re-reads the caller permission FOR SHARE inside the
// delivery window (a can_sign off that raced the sign path is observed here).
const deliveryCanSignSQL = `SELECT can_sign FROM signer_caller WHERE caller_id = $1 FOR SHARE`

// deliveryInsertAdmissionSQL records the per-attempt gate snapshot. The row
// starts `admitted` and is updated to `delivered` in the same transaction as
// the bytes write, so a failed write leaves no admitted row behind.
const deliveryInsertAdmissionSQL = `INSERT INTO delivery_admissions
  (signing_request_row, attempt_seq, verdict, authorization_id, authorization_fingerprint,
   authorization_state, binding_class, can_sign, recovery_version, pause_basis, recovery_basis, reason)
  VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
  RETURNING admission_id`

// deliveryMarkSQL is the delivered marker: the one allowed post-insert update
// (data-model.md retention note).
const deliveryMarkSQL = `UPDATE delivery_admissions
  SET verdict = 'delivered', delivered_at = clock_timestamp()
  WHERE admission_id = $1`

// deliveryOverlapGrantSQL is the lockless post-COMMIT grant re-read used for
// overlap accounting only.
const deliveryOverlapGrantSQL = `SELECT state FROM withdrawal_authorizations WHERE authorization_id = $1`

// DeliverySink accepts the response bytes of exactly one delivery region. It
// is the only path by which bytes leave the service (009 never broadcasts);
// the caller wires the local transport (HTTP response writer) to it.
type DeliverySink interface {
	WriteDelivery(ctx context.Context, payload []byte) error
}

// ScopeLocker takes the 008 scope-row `FOR SHARE` joint coordination lock at
// the head of the delivery lock order (gates.md §3). It is optional until the
// live 008 adapter merges (T028); when nil the scope lock is skipped exactly as
// submitFirst skips it, and the remaining fixed order still holds.
type ScopeLocker interface {
	LockScope(ctx context.Context, tx pgx.Tx, chainID int64, sender string) error
}

// DeliveryDeps are the delivery transaction's collaborators. There is no
// KeyProvider: delivery never signs, only re-delivers the persisted result.
type DeliveryDeps struct {
	DB        DB
	Binding   BindingReader
	ScopeLock ScopeLocker
}

// DeliveryResult is the outcome of one delivery attempt: the recorded verdict,
// the attempt sequence, the refusal class when withheld/unknown, and the exact
// bytes handed to the sink on (re-)delivery.
type DeliveryResult struct {
	Verdict    Verdict
	AttemptSeq int
	Class      RefusalClass
	Bytes      []byte
}

// deliveryRow is the durable snapshot a delivery attempt is decided from.
type deliveryRow struct {
	rowID            int64
	callerID         int64
	signingRequestID string
	attemptID        string
	intentID         string
	bindingRef       string
	recoveryVersion  int64
	chainID          int64
	sender           string
	authorizationID  string
	fingerprint      string
	authState        string
	authVersion      *int64
	asset            string
	recipient        string
	amount           string
	state            string
	signature        string
	txHash           string
}

// deliveryPayload is the only success body that carries signing material
// (api.md §2): signature + tx_hash facts, never raw signed-transaction bytes.
// It is built from persisted columns only, so re-delivery is byte-identical.
type deliveryPayload struct {
	SigningRequestID string `json:"signing_request_id"`
	State            string `json:"state"`
	Signature        string `json:"signature"`
	TxHash           string `json:"tx_hash"`
	Delivery         string `json:"delivery"`
}

// deliverySnapshot is the recorded admission basis for one attempt.
type deliverySnapshot struct {
	authorizationID string
	fingerprint     string
	authState       string
	bindingClass    string
	canSign         bool
	recoveryVersion int64
	pauseBasis      string
	recoveryBasis   string
	reason          string
}

// Deliver runs one T-deliver attempt for a signed request identity through the
// current gates and hands the (byte-identical) persisted bytes to sink on a
// pass. callerID scopes ownership; the transport has already authenticated.
// A nil error means the bytes were written and the `delivered` marker
// committed. A *RefusalError carries `signature_withheld` (blocked), a
// retryable gate class, or `outcome_unknown` (indeterminate).
func Deliver(ctx context.Context, deps DeliveryDeps, caller Caller, signingRequestID string, sink DeliverySink) (*DeliveryResult, error) {
	if deps.DB == nil || deps.Binding == nil || sink == nil {
		return nil, refuse(ClassStorageUnavailable, "", "delivery dependencies incomplete")
	}
	row, err := readDeliveryRow(ctx, deps.DB, caller.ID, signingRequestID)
	if err != nil {
		return nil, refuse(ClassStorageUnavailable, "", "storage unavailable")
	}
	if row == nil {
		return nil, refuse(ClassOutcomeNotYetVisible, "", "no durable result for this identity; retry the same identity")
	}

	// Every delivery — first response, same-identity retransmit, or a replay of
	// an already-`delivered` request — runs the full T-deliver gate sequence. A
	// historic `delivered` marker is NOT a retransmit permit (gates.md §3): a
	// blocked replay writes zero signature bytes, an unblocked replay hands out
	// the persisted bytes byte-identically and never re-signs, and a replay
	// transport failure maps to `unknown` (never success-from-history).
	return deliverGated(ctx, deps, caller, row, sink)
}

// deliverGated is the T-deliver transaction: BEGIN → statement guard → fixed
// lock order → fresh gate re-reads → own-row FOR UPDATE → protected region
// (admission INSERT + bytes write + delivered marker UPDATE + COMMIT).
func deliverGated(ctx context.Context, deps DeliveryDeps, caller Caller, row *deliveryRow, sink DeliverySink) (*DeliveryResult, error) {
	tx, err := deps.DB.Begin(ctx)
	if err != nil {
		return nil, refuse(ClassStorageUnavailable, "", "storage unavailable")
	}
	// Rollback with a live context: the caller's ctx may already be canceled by
	// the time the region fails (session loss).
	defer func() { _ = tx.Rollback(context.Background()) }()

	if _, err := tx.Exec(ctx, deliverySendGuard); err != nil {
		return nil, refuse(ClassStorageUnavailable, "", "storage unavailable")
	}

	// Fixed lock order (persistence.md §2, R6): 008 scope row FOR SHARE → 006
	// gate tables LOCK IN SHARE MODE → 007 grant FOR SHARE → own rows FOR UPDATE.
	if deps.ScopeLock != nil {
		if err := deps.ScopeLock.LockScope(ctx, tx, row.chainID, row.sender); err != nil {
			return nil, refuse(ClassGateReadFailed, "", "scope lock failed")
		}
	}
	if _, err := tx.Exec(ctx, GateLockSQL); err != nil {
		return nil, refuse(ClassGateReadFailed, "", "gate lock failed")
	}

	gate, err := readRecoveryGate(ctx, tx, row.chainID)
	if err != nil {
		return nil, refuse(ClassGateReadFailed, "", "006 gate read failed")
	}
	gateClass := gate.Evaluate(row.recoveryVersion)

	binding, berr := deps.Binding.ReadBinding(ctx, row.intentID, row.attemptID)
	bindingClass := BindingRefusal(binding, berr)

	// The PB carrier row is loaded in the same sequence (H1/T036); the
	// persisted-version equality re-check is applied against it (H2/T037).
	grant, scope, found, err := readGrantForShare(ctx, tx, row.authorizationID)
	if err != nil {
		return nil, refuse(ClassGateReadFailed, "", "007 grant read failed")
	}
	var now time.Time
	if err := tx.QueryRow(ctx, submitClockSQL).Scan(&now); err != nil {
		return nil, refuse(ClassGateReadFailed, "", "database clock read failed")
	}
	authzClass := deliveryGrantClass(found, grant, scope, row, caller.ID, now)

	var canSign bool
	if err := tx.QueryRow(ctx, deliveryCanSignSQL, caller.ID).Scan(&canSign); err != nil {
		return nil, refuse(ClassStorageUnavailable, "", "storage unavailable")
	}

	var lockedID int64
	if err := tx.QueryRow(ctx, deliveryLockOwnSQL, row.rowID).Scan(&lockedID); err != nil {
		return nil, refuse(ClassStorageUnavailable, "", "storage unavailable")
	}

	snap := deliverySnapshot{
		authorizationID: row.authorizationID,
		fingerprint:     row.fingerprint,
		authState:       row.authState,
		bindingClass:    bindingClassName(binding),
		canSign:         canSign,
		recoveryVersion: row.recoveryVersion,
		pauseBasis:      pauseBasisOf(gate),
		recoveryBasis:   recoveryBasisOf(gate, row.recoveryVersion),
	}
	if found && grant != nil {
		snap.authState = grant.State
		snap.fingerprint = strings.TrimPrefix(grant.Fingerprint(), AuthzFingerprintDomain+":")
	}

	if class := deliveryBlockClass(gateClass, bindingClass, authzClass, canSign); class != "" {
		return recordBlocked(ctx, tx, deps, row, snap, class)
	}

	// One protected region (persistence.md §2): the admitted INSERT, the bytes
	// write, and the delivered marker commit together. There is no
	// admit-now-write-later split.
	attemptSeq, err := nextDeliverySeq(ctx, tx, row.rowID)
	if err != nil {
		return nil, refuse(ClassStorageUnavailable, "", "storage unavailable")
	}
	snap.reason = ""
	admissionID, err := insertAdmission(ctx, tx, row.rowID, attemptSeq, VerdictAdmitted, snap)
	if err != nil {
		return nil, refuse(ClassStorageUnavailable, "", "storage unavailable")
	}
	if _, err := tx.Exec(ctx, submitAuditSQL, row.signingRequestID, row.callerID, "delivery_admitted", "",
		"attempt_seq="+strconv.Itoa(attemptSeq)); err != nil {
		return nil, refuse(ClassStorageUnavailable, "", "storage unavailable")
	}

	// Best-effort pre-write liveness check (TOCTOU-limited; it narrows, never
	// closes, the unaware window): a detected dead session rolls back with zero
	// bytes, reported unknown.
	if conn := tx.Conn(); conn != nil {
		if err := conn.Ping(ctx); err != nil {
			return unknownDelivery(deps, row, attemptSeq)
		}
	}

	payload, err := renderDeliveryPayload(row)
	if err != nil {
		return nil, refuse(ClassStorageUnavailable, "", "delivery render failed")
	}
	if err := sink.WriteDelivery(ctx, payload); err != nil {
		return unknownDelivery(deps, row, attemptSeq)
	}
	if _, err := tx.Exec(ctx, deliveryMarkSQL, admissionID); err != nil {
		return unknownDelivery(deps, row, attemptSeq)
	}
	if err := tx.Commit(ctx); err != nil {
		return unknownDelivery(deps, row, attemptSeq)
	}

	// Committed handoff: a best-effort fresh, lockless gate re-read classifies
	// the delivery as clean or overlapped — accounting only, never recall/un-
	// delivery (persistence.md §2).
	recordOverlap(ctx, deps, row)
	return &DeliveryResult{Verdict: VerdictDelivered, AttemptSeq: attemptSeq, Bytes: payload}, nil
}

// recordBlocked persists the blocked admission (with the observed basis) and
// audit, commits it, and returns a status-only response: zero bytes.
func recordBlocked(ctx context.Context, tx pgx.Tx, deps DeliveryDeps, row *deliveryRow, snap deliverySnapshot, class RefusalClass) (*DeliveryResult, error) {
	respClass := deliveryResponseClass(class)
	attemptSeq, err := nextDeliverySeq(ctx, tx, row.rowID)
	if err != nil {
		return nil, refuse(ClassStorageUnavailable, "", "storage unavailable")
	}
	snap.reason = string(respClass)
	if _, err := insertAdmission(ctx, tx, row.rowID, attemptSeq, VerdictBlocked, snap); err != nil {
		return nil, refuse(ClassStorageUnavailable, "", "storage unavailable")
	}
	if _, err := tx.Exec(ctx, submitAuditSQL, row.signingRequestID, row.callerID, "delivery_blocked",
		string(respClass), deliveryBlockDetail(class, snap)); err != nil {
		return nil, refuse(ClassStorageUnavailable, "", "storage unavailable")
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, refuse(ClassStorageUnavailable, "", "storage unavailable")
	}
	return &DeliveryResult{Verdict: VerdictBlocked, AttemptSeq: attemptSeq, Class: respClass},
		refuse(respClass, "", "delivery blocked: "+string(class))
}

// unknownDelivery reports an indeterminate outcome: the region rolled back (no
// admitted row, no marker) but the bytes MAY already be out, so it is never
// "nothing delivered". The unknown basis is recorded best-effort outside the
// transaction; the durable evidence is the result row with no delivered marker
// (persistence.md §6).
func unknownDelivery(deps DeliveryDeps, row *deliveryRow, attemptSeq int) (*DeliveryResult, error) {
	bestEffortAudit(context.Background(), deps.DB, row.signingRequestID, row.callerID,
		"delivery_unknown", ClassOutcomeUnknown,
		"attempt_seq="+strconv.Itoa(attemptSeq)+" bytes_may_be_out=true")
	return &DeliveryResult{Verdict: VerdictUnknownReconcile, AttemptSeq: attemptSeq, Class: ClassOutcomeUnknown},
		refuse(ClassOutcomeUnknown, "", "delivery outcome unknown; reconcile and retry the same identity")
}

// renderDeliveryPayload builds the deterministic success body from persisted
// columns (byte-identical across re-deliveries).
func renderDeliveryPayload(row *deliveryRow) ([]byte, error) {
	return json.Marshal(deliveryPayload{
		SigningRequestID: row.signingRequestID,
		State:            row.state,
		Signature:        row.signature,
		TxHash:           row.txHash,
		Delivery:         string(VerdictDelivered),
	})
}

// readDeliveryRow loads the own request + persisted result snapshot.
func readDeliveryRow(ctx context.Context, db DB, callerID int64, signingRequestID string) (*deliveryRow, error) {
	var row deliveryRow
	var recoveryVersion, chainID int64
	err := db.QueryRow(ctx, deliveryRowSQL, callerID, signingRequestID).Scan(
		&row.rowID, &row.callerID, &row.signingRequestID, &row.attemptID, &row.intentID, &row.bindingRef,
		&recoveryVersion, &chainID, &row.sender, &row.authorizationID,
		&row.fingerprint, &row.authState, &row.authVersion, &row.asset, &row.recipient, &row.amount,
		&row.state, &row.signature, &row.txHash)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	row.recoveryVersion = recoveryVersion
	row.chainID = chainID
	return &row, nil
}

// deliveryGrantClass applies the 007 re-read rules against the row's bound
// fields: state/expiry/field equality first, then the persisted `authz:v1`
// fingerprint equality (a changed grant is authorization_invalid), then the H2
// persisted scope-version equality. The version check is additive: grant
// current state/validity is always re-checked, never version-equality alone.
func deliveryGrantClass(found bool, grant *AuthzGrant, scope GrantScope, row *deliveryRow, callerID int64, now time.Time) RefusalClass {
	req := &Request{ChainID: uint64(row.chainID), Asset: row.asset, Recipient: row.recipient, Amount: row.amount}
	if class := EvaluateGrant(found, grant, callerID, req, now); class != "" {
		return class
	}
	if found && grant != nil {
		if fp := strings.TrimPrefix(grant.Fingerprint(), AuthzFingerprintDomain+":"); fp != row.fingerprint {
			return ClassAuthorizationInvalid
		}
	}
	return scopeVersionClass(row.authVersion, scope)
}

// scopeVersionClass re-checks the submit-time scope version against the value
// observed now (H2). Only a snapshot and a live scope that agree pass; a
// missing side matches a missing side (the pre-extension stock stays on the
// fingerprint-only path), while a scope that appeared or vanished against the
// snapshot blocks instead of being silently adopted.
func scopeVersionClass(persisted *int64, scope GrantScope) RefusalClass {
	switch {
	case persisted == nil && !scope.Present:
		return ""
	case persisted == nil || !scope.Present:
		return ClassAuthorizationInvalid
	case *persisted != scope.AuthorizationVersion:
		return ClassAuthorizationInvalid
	}
	return ""
}

// deliveryBlockClass is the first observed blocking class ("" means pass).
func deliveryBlockClass(gateClass, bindingClass, authzClass RefusalClass, canSign bool) RefusalClass {
	switch {
	case gateClass != "":
		return gateClass
	case bindingClass != "":
		return bindingClass
	case authzClass != "":
		return authzClass
	case !canSign:
		return ClassSigningNotPermitted
	}
	return ""
}

// deliveryResponseClass reports a non-retryable gate failure as
// signature_withheld (409, status-only); a retryable read failure keeps its own
// class (fail-closed, retry same identity).
func deliveryResponseClass(class RefusalClass) RefusalClass {
	if RetryabilityOf(class) == RetryNow {
		return class
	}
	return ClassSignatureWithheld
}

// deliveryBlockDetail records the observed basis without secrets.
func deliveryBlockDetail(class RefusalClass, snap deliverySnapshot) string {
	parts := []string{
		"observed=" + string(class),
		"pause_basis=" + snap.pauseBasis,
		"recovery_basis=" + snap.recoveryBasis,
		"binding_class=" + snap.bindingClass,
	}
	if !snap.canSign {
		parts = append(parts, "can_sign=false")
	}
	if snap.authState != "" {
		parts = append(parts, "authorization_state="+snap.authState)
	}
	return strings.Join(parts, " ")
}

// pauseBasisOf names the observed 006 pause table(s), or "none".
func pauseBasisOf(g RecoveryGate) string {
	if bases := g.PauseBases(); len(bases) > 0 {
		return strings.Join(bases, ",")
	}
	return "none"
}

// recoveryBasisOf describes the observed recovery/version basis.
func recoveryBasisOf(g RecoveryGate, requestVersion int64) string {
	if g.HasRecovery {
		return "active"
	}
	if g.CurrentVersion() != requestVersion {
		return "version_changed"
	}
	if g.HasEventsMax {
		return "events_max=" + strconv.FormatInt(g.EventsMax, 10)
	}
	return "none"
}

// nextDeliverySeq is the next attempt sequence for the request.
func nextDeliverySeq(ctx context.Context, tx pgx.Tx, rowID int64) (int, error) {
	var seq int
	err := tx.QueryRow(ctx, deliveryNextSeqSQL, rowID).Scan(&seq)
	return seq, err
}

// insertAdmission writes one delivery_admissions row and returns its id.
func insertAdmission(ctx context.Context, tx pgx.Tx, rowID int64, attemptSeq int, verdict Verdict, snap deliverySnapshot) (int64, error) {
	var admissionID int64
	err := tx.QueryRow(ctx, deliveryInsertAdmissionSQL,
		rowID, attemptSeq, string(verdict), snap.authorizationID, snap.fingerprint,
		snap.authState, snap.bindingClass, snap.canSign, snap.recoveryVersion,
		snap.pauseBasis, snap.recoveryBasis, snap.reason).Scan(&admissionID)
	return admissionID, err
}

// recordOverlap is the best-effort post-COMMIT classification (persistence.md
// §2): a lockless re-read of the revocable gates; a change committed after the
// region is recorded in audit as overlap accounting. It never un-delivers.
func recordOverlap(ctx context.Context, deps DeliveryDeps, row *deliveryRow) {
	if class := overlapClass(ctx, deps, row); class != "" {
		bestEffortAudit(context.Background(), deps.DB, row.signingRequestID, row.callerID,
			"delivery_admitted", class, "overlap="+string(class))
	}
}

// overlapClass classifies the post-COMMIT gate state without any lock.
func overlapClass(ctx context.Context, deps DeliveryDeps, row *deliveryRow) RefusalClass {
	var (
		indexerPaused bool
		logPaused     bool
		depositPaused bool
		recoveryID    *string
		phase         *string
		seq           *int64
		eventsMax     *int64
	)
	if err := deps.DB.QueryRow(ctx, GateReadSQL, row.chainID).Scan(
		&indexerPaused, &logPaused, &depositPaused, &recoveryID, &phase, &seq, &eventsMax); err == nil {
		g := RecoveryGate{IndexerPaused: indexerPaused, LogPaused: logPaused, DepositPaused: depositPaused}
		if recoveryID != nil {
			g.HasRecovery = true
		}
		if phase != nil {
			g.RecoveryPhase = *phase
		}
		if seq != nil {
			g.RecoverySeq = *seq
		}
		if eventsMax != nil {
			g.HasEventsMax = true
			g.EventsMax = *eventsMax
		}
		if class := g.Evaluate(row.recoveryVersion); class != "" {
			return class
		}
	}
	if binding, err := deps.Binding.ReadBinding(ctx, row.intentID, row.attemptID); err == nil {
		if class := BindingRefusal(binding, nil); class != "" {
			return class
		}
	}
	var state string
	if err := deps.DB.QueryRow(ctx, deliveryOverlapGrantSQL, row.authorizationID).Scan(&state); err == nil {
		if state == "revoked" {
			return ClassAuthorizationRevoked
		}
		if state != "active" {
			return ClassAuthorizationInvalid
		}
	}
	return ""
}
