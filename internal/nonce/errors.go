// errors.go owns the stable machine vocabulary of the 008 nonce manager:
// admission outcomes (contracts/downstream.md §1), operator attempt outcomes
// (contracts/observation.md §3.4/§4) and read outcomes (contracts/read-api.md
// §1–§3). The string values are wire vocabulary and MUST NOT be reworded once
// published (FR-14/FR-17).
package nonce

import "errors"

// Outcome is a stable machine-readable result string. Its value is the wire
// string.
type Outcome string

// Admission outcomes (contracts/downstream.md §1). Every non-allocated
// outcome is fail-closed and records its cause; none creates or mutates a
// binding.
const (
	// OutcomeAllocated means a new binding was committed for the intent.
	OutcomeAllocated Outcome = "allocated"
	// OutcomeReplayed means the original binding was returned on full
	// allocation-input equality; no row was touched.
	OutcomeReplayed Outcome = "replayed"
	// OutcomeAllocationConflict means the intent already has a binding and
	// any input field differs: the original stays untouched and no second
	// binding is ever created.
	OutcomeAllocationConflict Outcome = "allocation_conflict"
	// OutcomeChainViewUnavailable means the chain view could not be verified
	// (any RPC failure class): the observation is persisted, no domain state
	// changes.
	OutcomeChainViewUnavailable Outcome = "chain_view_unavailable"
	// OutcomeScopeHeld means the scope has at least one active 008 hold
	// (unattributed consumption / unexplained gap / divergence): fail closed
	// with the causes recorded.
	OutcomeScopeHeld Outcome = "scope_held"
	// OutcomeSenderNotRegistered means the sender is not in the controlled
	// wallet registry (authoritative, OC-2): fail closed.
	OutcomeSenderNotRegistered Outcome = "sender_not_registered"
	// OutcomeSenderDisabled means the registry marks the sender disabled:
	// new admissions refuse, existing binding facts stay valid.
	OutcomeSenderDisabled Outcome = "sender_disabled"
	// OutcomeAuthorizationInvalid means the 007 authorization named by the
	// request is missing/inactive/expired/mismatched/unreadable: zero binding.
	OutcomeAuthorizationInvalid Outcome = "authorization_invalid"
	// OutcomeRebuildIncomplete means the startup rebuild verification has not
	// completed (or failed): allocation refuses fail-closed (FR-13).
	OutcomeRebuildIncomplete Outcome = "rebuild_incomplete"
	// OutcomeRecoveryActive means a 006 recovery gate is active (pause rows or
	// an active reorg_recovery row): 006 precedence, refuse with the cause.
	OutcomeRecoveryActive Outcome = "recovery_active"
	// OutcomeTemporarilyUnavailable is the retryable outcome for an internal
	// serialization loss, an uncertain commit, or unclassified storage
	// failure: retry with the same input set.
	OutcomeTemporarilyUnavailable Outcome = "temporarily_unavailable"
	// OutcomeOperationConflict means one operation_id was presented with a
	// differing op-input: zero writes, the recorded attempt is authoritative.
	OutcomeOperationConflict Outcome = "operation_conflict"
)

// Hold causes (contracts/observation.md §2). A held scope records the cause
// on its nonce_scope_holds row; these are the same strings listed with a
// scope_held refusal. Untyped string constants so they compose with
// Decision.HoldCause without an Outcome conversion.
const (
	// CauseUnattributedConsumption means the chain consumed above the local
	// frontier with mined evidence (P > M+1, L > M+1).
	CauseUnattributedConsumption = "unattributed_consumption"
	// CauseUnexplainedGap means the chain consumed above the local frontier
	// with pending-only evidence (P > M+1, L <= M+1).
	CauseUnexplainedGap = "unexplained_gap"
	// CauseChainViewDivergence means a contradictory or regressing view
	// (L > P, or pending below the previous observation).
	CauseChainViewDivergence = "chain_view_divergence"
)

// Read outcomes (contracts/read-api.md §1–§3). Every response is exactly one
// of the five, plus the 401 authentication code.
const (
	// ReadBound is a non-terminal binding returned as facts (200).
	ReadBound Outcome = "bound"
	// ReadTerminal is a consumed/released binding returned for audit (200).
	ReadTerminal Outcome = "terminal"
	// ReadNotBound means no binding exists for the identity (404): fail
	// closed, never invent an identity.
	ReadNotBound Outcome = "not_bound"
	// ReadMismatch means the supplied expected scope contradicts the durable
	// binding (409): the durable binding is authoritative.
	ReadMismatch Outcome = "mismatch"
	// ReadUnavailable means 008 cannot produce a trustworthy answer (DB read
	// failure, rebuild gate not open) (503, retryable, never "absent").
	ReadUnavailable Outcome = "unavailable"
	// ReadUnauthenticated is the missing/invalid bearer credential code
	// (401, contracts/read-api.md §1).
	ReadUnauthenticated Outcome = "unauthenticated"
)

// Operator attempt outcomes (contracts/observation.md §3.4). Refusals and
// nops are committed outcomes and are never upgraded on replay.
const (
	// AdminApplied means the operator action changed the named subject.
	AdminApplied Outcome = "applied"
	// AdminNop means the named subject was missing or already disposed: the
	// audit row is recorded, zero state changes.
	AdminNop Outcome = "nop"
	// AdminRefused means the evidence standard or re-verification failed:
	// audit row recorded, zero subject state changes.
	AdminRefused Outcome = "refused"
)

// Error is the classified failure type carried across the nonce package: a
// stable Outcome plus a human-readable reason. It never carries key material
// or secrets.
type Error struct {
	Outcome Outcome
	Reason  string
	cause   error
}

// Refuse builds a classified refusal error.
func Refuse(outcome Outcome, reason string) *Error {
	return &Error{Outcome: outcome, Reason: reason}
}

// Wrap attaches a cause to a classified refusal.
func (e *Error) Wrap(cause error) *Error {
	clone := *e
	clone.cause = cause
	return &clone
}

// Error renders "outcome: reason".
func (e *Error) Error() string {
	if e.Reason == "" {
		return string(e.Outcome)
	}
	return string(e.Outcome) + ": " + e.Reason
}

// Unwrap returns the wrapped internal cause, letting errors.Is/errors.As
// traverse to it.
func (e *Error) Unwrap() error { return e.cause }

// IsOutcome reports whether err carries the given machine outcome.
func IsOutcome(err error, outcome Outcome) bool {
	var e *Error
	return errors.As(err, &e) && e.Outcome == outcome
}

// OutcomeOf returns the machine outcome of err, or "" when unclassified.
func OutcomeOf(err error) Outcome {
	var e *Error
	if errors.As(err, &e) {
		return e.Outcome
	}
	return ""
}
