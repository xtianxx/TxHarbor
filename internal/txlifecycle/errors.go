// Package txlifecycle owns 010's transaction lifecycle storage and the
// 011-facing in-process surface: attempt identity/content (T1), signed bytes
// (T2), the gated send region (T3), reconcile/receipt/revision (T4), and the
// read-only Status projection. It never creates an intent, never allocates a
// nonce, never releases a binding, never clears a pause/recovery and holds no
// key material (FR-10/FR-13; send-api.md §1).
package txlifecycle

import "fmt"

// RefusalClass is the fail-closed send refusal taxonomy, verbatim from
// specs/010-transaction-lifecycle/contracts/send-api.md §3. Every class is
// recorded with its observed basis and produces zero dispatch; retryability is
// declared by Class 2/3 below.
type RefusalClass string

const (
	// Execution qualification (FR-14; natural expiry and explicit revocation
	// produce the same refusal, distinguished only in the recorded basis).
	ClassClaimAbsent          RefusalClass = "claim_absent"
	ClassClaimVersionMismatch RefusalClass = "claim_version_mismatch"
	ClassClaimExpired         RefusalClass = "claim_expired"
	ClassClaimRevoked         RefusalClass = "claim_revoked"

	// 006 gate (FR-07).
	ClassPausePresent           RefusalClass = "pause_present"
	ClassRecoveryActive         RefusalClass = "recovery_active"
	ClassRecoveryVersionChanged RefusalClass = "recovery_version_changed"
	ClassGateReadFailed         RefusalClass = "gate_read_failed"

	// 008 binding consumption.
	ClassBindingAbsent     RefusalClass = "binding_absent"
	ClassBindingConflict   RefusalClass = "binding_conflict"
	ClassBindingPaused     RefusalClass = "binding_paused"
	ClassBindingTerminal   RefusalClass = "binding_terminal"
	ClassBindingReadFailed RefusalClass = "binding_read_failed"

	// 007/PB grant facts.
	ClassAuthorizationMissing      RefusalClass = "authorization_missing"
	ClassAuthorizationInactive     RefusalClass = "authorization_inactive"
	ClassAuthorizationRevoked      RefusalClass = "authorization_revoked"
	ClassAuthorizationExpired      RefusalClass = "authorization_expired"
	ClassAuthorizationMismatch     RefusalClass = "authorization_mismatch"
	ClassAuthorizationUnverifiable RefusalClass = "authorization_unverifiable"

	// PB conditional reuse / fee scope (FR-06).
	ClassScopeReuseForbidden RefusalClass = "scope_reuse_forbidden"
	ClassFeeScopeExceeded    RefusalClass = "fee_scope_exceeded"

	// Identity/content invariant violations (FR-05/FR-12).
	ClassAttemptConflict        RefusalClass = "attempt_conflict"
	ClassHashConflict           RefusalClass = "hash_conflict"
	ClassReplacementMismatch    RefusalClass = "replacement_mismatch"
	ClassReplacementNoFeeChange RefusalClass = "replacement_no_fee_change"

	// Declared kind vs durable state (FR-04).
	ClassAlreadyAccepted  RefusalClass = "already_accepted"
	ClassSendModeMismatch RefusalClass = "send_mode_mismatch"

	// State/revision contradiction.
	ClassAttemptNotSendable RefusalClass = "attempt_not_sendable"
	ClassAttemptNotFound    RefusalClass = "attempt_not_found"
	ClassSendStale          RefusalClass = "send_stale"

	// 009 signature boundary (FR-02/FR-10).
	ClassSignatureRefused  RefusalClass = "signature_refused"
	ClassSignatureMismatch RefusalClass = "signature_mismatch"

	// Storage/coordination failure before dispatch.
	ClassCoordinationUnavailable RefusalClass = "coordination_unavailable"

	// G-010-2 class (c): an intent whose protection loss is confirmed (or an
	// unexcludable post-invalidation send) is frozen pending manual review.
	// This is an independent cause: releasing it never lifts revoke/expiry/
	// pause/eligibility gates (T039/T040).
	ClassIntentFrozen RefusalClass = "intent_frozen"
	// A controlled manual release presented without the configured permission
	// is refused fail-closed (T040).
	ClassReleaseNotPermitted RefusalClass = "release_not_permitted"
)

// Retryability is the send-api.md §3 retry column as a machine value.
type Retryability string

const (
	// RetryAfterStateChange: safe to retry the same identity once the state
	// that caused the refusal has changed (takeover/release/…).
	RetryAfterStateChange Retryability = "after_state_change"
	// RetryNewIdentity: the content or grant is stale/forbidden; a fresh
	// identity (and possibly a fresh authorization) is required.
	RetryNewIdentity Retryability = "new_identity_required"
	// RetryNever: the caller must correct its input.
	RetryNever Retryability = "never"
	// RetryInspect: read Status and decide before retrying.
	RetryInspect Retryability = "inspect_state"
	// RetrySignerDocumented: delivery retry semantics follow 009's contract.
	RetrySignerDocumented Retryability = "as_signer_documents"
)

// RefusalError is a zero-dispatch refusal: the class, the offending field when
// one exists, the observed basis evidence, and the retryability.
type RefusalError struct {
	Class RefusalClass
	Field string
	Basis string
	Msg   string
}

func (e *RefusalError) Error() string {
	if e.Field != "" {
		return fmt.Sprintf("%s: %s: %s", e.Class, e.Field, e.Msg)
	}
	return fmt.Sprintf("%s: %s", e.Class, e.Msg)
}

// Refuse builds a RefusalError.
func Refuse(class RefusalClass, field, msg string) *RefusalError {
	return &RefusalError{Class: class, Field: field, Msg: msg}
}

// RetryabilityOf returns the §3 retryability of a class. Unknown classes fail
// closed to RetryInspect (never silently retryable).
func RetryabilityOf(class RefusalClass) Retryability {
	switch class {
	case ClassClaimAbsent, ClassClaimVersionMismatch, ClassClaimExpired, ClassClaimRevoked,
		ClassPausePresent, ClassRecoveryActive, ClassGateReadFailed,
		ClassBindingAbsent, ClassBindingConflict, ClassBindingPaused, ClassBindingReadFailed,
		ClassAlreadyAccepted, ClassSendModeMismatch,
		ClassCoordinationUnavailable:
		return RetryAfterStateChange
	case ClassRecoveryVersionChanged,
		ClassAuthorizationMissing, ClassAuthorizationInactive, ClassAuthorizationRevoked,
		ClassAuthorizationExpired, ClassAuthorizationMismatch, ClassAuthorizationUnverifiable,
		ClassScopeReuseForbidden, ClassFeeScopeExceeded,
		ClassAttemptConflict, ClassHashConflict, ClassReplacementMismatch, ClassReplacementNoFeeChange,
		ClassBindingTerminal, ClassSignatureMismatch:
		return RetryNever
	case ClassIntentFrozen:
		// Only a controlled manual release lifts this independent cause; then
		// the caller retries and every gate is re-evaluated (T039/T040).
		return RetryAfterStateChange
	case ClassReleaseNotPermitted:
		return RetryNever
	case ClassSignatureRefused:
		return RetrySignerDocumented
	default:
		return RetryInspect
	}
}

// Attempt event vocabulary mirroring the data-model Table 6 CHECK list. These
// are the only values tx_attempt_events.event may hold.
const (
	EventCreated            = "created"
	EventReplayed           = "replayed"
	EventAttemptConflict    = "attempt_conflict"
	EventSignaturePersisted = "signature_persisted"
	EventSignatureRefused   = "signature_refused"
	EventSignatureMismatch  = "signature_mismatch"
	EventGateRefused        = "gate_refused"
	EventSendRejected       = "send_rejected"
	EventSendUnknown        = "send_unknown"
	EventReconcileObserved  = "reconcile_observed"
	EventReceiptVerified    = "receipt_verified"
	EventReceiptIneffective = "receipt_ineffective"
	EventConfirmed          = "confirmed"
	EventOrphaned           = "orphaned"
	EventReconfirmed        = "reconfirmed"
	EventReplaced           = "replaced"
	EventUnknownCleared     = "unknown_cleared"
	// G-010-1: a natural expiry landing between the last evaluation and
	// dispatch entry. Recorded as a residual, never described as legally
	// in-flight (send-gate.md §4(c)).
	EventPostFinalCheckExpiry = "post_final_check_expiry"
	// G-010-2(b): a reliably-preventable pre-dispatch abort. Not a send
	// result; the business effect stays unknown pending reconcile.
	EventRegionAbortedNoDispatch = "region_aborted_no_dispatch"
	// G-010-2(c)/T039: the intent's further sends are frozen pending review.
	EventFrozen = "frozen"
	// T040: a controlled manual release lifted the freeze cause.
	EventReleased = "released"
)

// validEvents mirrors Table 6's CHECK so append paths can reject a typo before
// it reaches storage (storage enforces the same list).
var validEvents = map[string]bool{
	EventCreated: true, EventReplayed: true, EventAttemptConflict: true,
	EventSignaturePersisted: true, EventSignatureRefused: true, EventSignatureMismatch: true,
	EventGateRefused: true, EventSendRejected: true, EventSendUnknown: true,
	EventReconcileObserved: true, EventReceiptVerified: true, EventReceiptIneffective: true,
	EventConfirmed: true, EventOrphaned: true, EventReconfirmed: true,
	EventReplaced: true, EventUnknownCleared: true,
	EventPostFinalCheckExpiry: true, EventRegionAbortedNoDispatch: true,
	EventFrozen: true, EventReleased: true,
}

// ValidEvent reports whether event is one of the Table 6 vocabulary values.
func ValidEvent(event string) bool { return validEvents[event] }
