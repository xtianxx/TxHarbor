package execution

import "context"

// AdvanceAction is one of the three send-class actions 011 may request of 010.
type AdvanceAction string

const (
	ActionFirstBroadcast AdvanceAction = "first_broadcast"
	ActionReplay         AdvanceAction = "replay"
	ActionReplace        AdvanceAction = "replace"
)

// AdvanceRequest is the identity/fencing/action payload 011 passes to 010. It
// carries no nonce, calldata, signatures or raw bytes: construction belongs to
// 010 and signing to 009 (contracts/lifecycle.md §2). Fee replacement is the
// one exception the round-2 contract adds: 010 has no fee oracle, so the
// caller supplies the candidate fee dimensions (exact decimal strings) and the
// replacement's fresh-grant/signing identity, and 010 validates all of them
// against the PB scope caps and the anchor before any write. The fields are
// ignored for first_broadcast/replay.
type AdvanceRequest struct {
	IntentID        string
	RequestID       string
	CallerID        int64
	OwnerID         string
	LeaseVersion    int64
	RecoveryVersion int64
	StepID          string
	Action          AdvanceAction
	AnchorAttemptID string
	ExpectedTxHash  string

	// ReplacementFeeMaxPerGas / ReplacementFeeMaxPriorityFeePerGas are the
	// caller-supplied candidate fee dimensions for ActionReplace (decimal
	// strings, native-coin smallest units, exact integers). 010 never derives
	// or invents a fee rate; both must be set or the replace is refused.
	ReplacementFeeMaxPerGas            string
	ReplacementFeeMaxPriorityFeePerGas string
	// ReplacementAuthorizationID names the PB grant the replacement is
	// submitted under. Empty = the anchor's own grant (conditional reuse,
	// gated on allows_fee_replacement + fee caps); a fresh grant passes the
	// full grant/scope gate instead.
	ReplacementAuthorizationID string
	// ReplacementSigningRequestID is the caller-preallocated signing identity
	// of the replacement (the 009 scope carrier fixes request_id ==
	// signing_request_id; 010 persists it 1:1 with the new attempt). Empty
	// falls back to a deterministic identity derived from StepID.
	ReplacementSigningRequestID string
}

// AdvanceOutcome is 010's classified answer: outcome class plus identity
// references only, never a copy of 010's content facts.
type AdvanceOutcome struct {
	Class           string
	AttemptID       string
	TxHash          string
	RevisionVersion int64
	Basis           string
}

// AttemptRef references one 010 attempt by identity and state only.
type AttemptRef struct {
	AttemptID string
	State     string
}

// UnknownRef is 010's unknown-recovery reference: the recovery condition plus
// the persisted facts, never an inference.
type UnknownRef struct {
	AttemptID         string
	TxHash            string
	RecoveryCondition string
}

// LifecycleFacts is the read side of the boundary.
type LifecycleFacts struct {
	Attempts         []AttemptRef
	CurrentAttemptID string
	RevisionVersion  int64
	Unknown          *UnknownRef
	Basis            string
}

// LifecycleAdvancer drives 010; 011 never constructs, signs or broadcasts.
type LifecycleAdvancer interface {
	Advance(ctx context.Context, req AdvanceRequest) (AdvanceOutcome, error)
}

// LifecycleReader reads 010's authority facts.
type LifecycleReader interface {
	Read(ctx context.Context, intentID string) (LifecycleFacts, error)
}

// 010 outcome class closed set (contracts/lifecycle.md §2/§7).
const (
	OutcomeSent              = "sent"
	OutcomeRefusedGate       = "refused_gate"
	OutcomeRefusedBasis      = "refused_basis"
	OutcomePendingUnknown    = "pending_unknown"
	OutcomeReconcileRequired = "reconcile_required"
	OutcomeUnavailable       = "unavailable"
)

// OutcomeClasses is the closed class list.
var OutcomeClasses = []string{
	OutcomeSent, OutcomeRefusedGate, OutcomeRefusedBasis,
	OutcomePendingUnknown, OutcomeReconcileRequired, OutcomeUnavailable,
}

// Step state constants (data-model Table 3 / state machine C).
const (
	StepIssued    = "issued"
	StepConverged = "converged"
	StepRefused   = "refused"
	StepUnknown   = "unknown"
)

// StepStateForOutcome maps a 010 outcome class to the 011 step terminal state
// per contracts/lifecycle.md §7: sent/refused_gate/refused_basis converge;
// pending_unknown and reconcile_required become unknown and reconcile first
// (never failure/not-paid); unavailable stays issued for a bounded retry.
// ok=false means the class is not in the closed set.
func StepStateForOutcome(class string) (state string, ok bool) {
	switch class {
	case OutcomeSent, OutcomeRefusedGate, OutcomeRefusedBasis:
		return StepConverged, true
	case OutcomePendingUnknown, OutcomeReconcileRequired:
		return StepUnknown, true
	case OutcomeUnavailable:
		return StepIssued, true
	default:
		return "", false
	}
}

// OutcomeRequiresReconcile reports whether the class maps to step unknown and
// intent reconciling (§7): pending_unknown / reconcile_required only.
func OutcomeRequiresReconcile(class string) bool {
	state, ok := StepStateForOutcome(class)
	return ok && state == StepUnknown
}
