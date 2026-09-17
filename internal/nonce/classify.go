// classify.go owns the observation classification matrix (data-model.md
// §Observation classification matrix; R2/R4) as a pure function of the
// durable frontier and the fresh chain view. It never adopts the chain view
// directly: ambiguous states are held and refused, and the forbidden
// max(M+1, P) shortcut is structurally absent.
package nonce

import "math/big"

// Observation classifications (the nonce_observations.classification CHECK
// domain).
const (
	ClassificationConsistent              = "consistent"
	ClassificationBootstrapExternal       = "bootstrap_external_consumed"
	ClassificationUnattributedConsumption = "unattributed_consumption"
	ClassificationUnexplainedGap          = "unexplained_gap"
	ClassificationDivergence              = "divergence"
	ClassificationUnavailable             = "unavailable"
)

// ClassificationInput is the matrix input: the fresh observation (Latest,
// Pending, or ReadFailed), the previous pending count (divergence signal),
// and the durable frontier (MaxBindingNonce = M with HasBindings, and/or
// ReconciledFloor = F).
type ClassificationInput struct {
	ReadFailed bool
	Latest     *big.Int
	Pending    *big.Int
	// PendingPrev is the scope's last observed pending count (nil when the
	// scope has no previous observation): a regression is divergence.
	PendingPrev *big.Int
	// MaxBindingNonce is the max bound nonce for the scope (nil when none);
	// HasBindings distinguishes "no bindings" from a zero-valued M.
	MaxBindingNonce *big.Int
	HasBindings     bool
	// ReconciledFloor is the durable reconciled floor F (nil when unset).
	ReconciledFloor *big.Int
}

// Decision is the classified outcome: the persisted classification, the
// candidate nonce when allocation may proceed, the hold cause to establish
// for a held scope, and the fail-closed admission refusal.
type Decision struct {
	Classification string
	Candidate      *big.Int
	HoldCause      string
	Refusal        Outcome
}

// classifyFrontierBase computes the durable "next nonce" base: M+1 when
// bindings exist, F when only a reconciled floor exists (a floor without
// bindings is the same durable-frontier condition — the consumed range below
// F is already reconciled), else 0.
func classifyFrontierBase(in ClassificationInput) (*big.Int, error) {
	switch {
	case in.HasBindings && in.MaxBindingNonce != nil:
		return NextNonce(in.MaxBindingNonce)
	case !in.HasBindings && in.ReconciledFloor != nil:
		if err := ValidateNonceRange(in.ReconciledFloor); err != nil {
			return nil, err
		}
		return new(big.Int).Set(in.ReconciledFloor), nil
	default:
		return big.NewInt(0), nil
	}
}

// Classify applies the matrix exactly. Order matters: unavailable (read
// failure) and divergence are checked before every candidate branch, and the
// `P > M+1` branches hold + refuse — they NEVER derive max(M+1, P).
func Classify(in ClassificationInput) Decision {
	if in.ReadFailed || in.Latest == nil || in.Pending == nil {
		return Decision{Classification: ClassificationUnavailable, Refusal: OutcomeChainViewUnavailable}
	}
	L, P := in.Latest, in.Pending

	// Divergence: contradictory (L > P) or regressing (P < P_prev) view.
	if L.Cmp(P) > 0 || (in.PendingPrev != nil && P.Cmp(in.PendingPrev) < 0) {
		return Decision{
			Classification: ClassificationDivergence,
			HoldCause:      CauseChainViewDivergence,
			Refusal:        OutcomeScopeHeld,
		}
	}

	frontierExists := in.HasBindings || in.ReconciledFloor != nil
	base, err := classifyFrontierBase(in)
	if err != nil {
		// Nonce space exhausted (M = 2⁶⁴−1): fail closed, never wrap.
		return Decision{Classification: ClassificationUnavailable, Refusal: OutcomeTemporarilyUnavailable}
	}

	switch {
	case !frontierExists && P.Sign() == 0:
		return Decision{Classification: ClassificationConsistent, Candidate: big.NewInt(0)}
	case !frontierExists:
		// 0 < P and L <= P (divergence already ruled out): healthy bootstrap
		// with pre-existing chain history — record [0, P) as externally
		// consumed evidence and admit at P. No hold.
		return Decision{Classification: ClassificationBootstrapExternal, Candidate: new(big.Int).Set(P)}
	case P.Cmp(base) <= 0:
		// Consistent: candidate comes from durable state, never from chain.
		return Decision{Classification: ClassificationConsistent, Candidate: MaxBig(base, in.ReconciledFloor)}
	default:
		// P > M+1 (the detectable unattributable consumption): hold + refuse.
		if L.Cmp(base) > 0 {
			return Decision{
				Classification: ClassificationUnattributedConsumption,
				HoldCause:      CauseUnattributedConsumption,
				Refusal:        OutcomeScopeHeld,
			}
		}
		return Decision{
			Classification: ClassificationUnexplainedGap,
			HoldCause:      CauseUnexplainedGap,
			Refusal:        OutcomeScopeHeld,
		}
	}
}

// MaxBoundNonce returns the maximum nonce across bindings (nil when empty).
// It is the M of the matrix, recomputed from nonce_bindings under the lock.
func MaxBoundNonce(bindings []Binding) *big.Int {
	var max *big.Int
	for i := range bindings {
		if bindings[i].Nonce == nil {
			continue
		}
		if max == nil || bindings[i].Nonce.Cmp(max) > 0 {
			max = new(big.Int).Set(bindings[i].Nonce)
		}
	}
	return max
}
