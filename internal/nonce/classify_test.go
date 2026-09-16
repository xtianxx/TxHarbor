package nonce

import (
	"math/big"
	"testing"
)

// classifyTestBig is the compact literal helper for the matrix table.
func classifyTestBig(n int64) *big.Int { return big.NewInt(n) }

// TestClassifyMatrix pins the FULL boundary matrix (data-model §Observation
// classification matrix; R2/R4): every unavailable/divergence/consistent/
// bootstrap/hold branch, the exact level boundaries (P = M+1, L = M+1, P = F),
// and the structural prohibition of the max(M+1, P) shortcut on the hold
// branches.
func TestClassifyMatrix(t *testing.T) {
	b := classifyTestBig
	none := ClassificationInput{Latest: b(0), Pending: b(0)} // no bindings, no floor, idle chain

	cases := []struct {
		name          string
		in            ClassificationInput
		wantClass     string
		wantCandidate string // "" means the candidate must be nil
		wantCause     string
		wantRefusal   Outcome
	}{
		// --- unavailable: any read failure refuses, no candidate ---
		{
			name:        "read failure",
			in:          ClassificationInput{ReadFailed: true, Latest: b(1), Pending: b(1)},
			wantClass:   ClassificationUnavailable,
			wantRefusal: OutcomeChainViewUnavailable,
		},
		{
			name:        "missing latest",
			in:          ClassificationInput{Pending: b(1)},
			wantClass:   ClassificationUnavailable,
			wantRefusal: OutcomeChainViewUnavailable,
		},
		{
			name:        "missing pending",
			in:          ClassificationInput{Latest: b(1)},
			wantClass:   ClassificationUnavailable,
			wantRefusal: OutcomeChainViewUnavailable,
		},

		// --- divergence: L > P or pending regression, both refuse + hold ---
		{
			name:        "divergence latest above pending",
			in:          ClassificationInput{Latest: b(6), Pending: b(5)},
			wantClass:   ClassificationDivergence,
			wantCause:   CauseChainViewDivergence,
			wantRefusal: OutcomeScopeHeld,
		},
		{
			name:        "divergence latest above pending with bindings",
			in:          ClassificationInput{Latest: b(6), Pending: b(5), HasBindings: true, MaxBindingNonce: b(4)},
			wantClass:   ClassificationDivergence,
			wantCause:   CauseChainViewDivergence,
			wantRefusal: OutcomeScopeHeld,
		},
		{
			name:        "divergence pending regression on first scope",
			in:          ClassificationInput{Latest: b(5), Pending: b(5), PendingPrev: b(6)},
			wantClass:   ClassificationDivergence,
			wantCause:   CauseChainViewDivergence,
			wantRefusal: OutcomeScopeHeld,
		},
		{
			name:        "divergence pending regression with bindings",
			in:          ClassificationInput{Latest: b(5), Pending: b(5), PendingPrev: b(6), HasBindings: true, MaxBindingNonce: b(4)},
			wantClass:   ClassificationDivergence,
			wantCause:   CauseChainViewDivergence,
			wantRefusal: OutcomeScopeHeld,
		},
		{
			name:          "pending at previous count is not a regression",
			in:            ClassificationInput{Latest: b(5), Pending: b(5), PendingPrev: b(5), HasBindings: true, MaxBindingNonce: b(4)},
			wantClass:     ClassificationConsistent,
			wantCandidate: "5",
		},

		// --- first scope (no bindings, no floor): bootstrap, never a hold ---
		{
			name:          "first scope idle chain admits zero",
			in:            none,
			wantClass:     ClassificationConsistent,
			wantCandidate: "0",
		},
		{
			name:          "first scope zero pending with previous zero observation",
			in:            ClassificationInput{Latest: b(0), Pending: b(0), PendingPrev: b(0)},
			wantClass:     ClassificationConsistent,
			wantCandidate: "0",
		},
		{
			name:          "first scope external history admits at P",
			in:            ClassificationInput{Latest: b(0), Pending: b(1)},
			wantClass:     ClassificationBootstrapExternal,
			wantCandidate: "1",
		},
		{
			name:          "first scope boundary latest equals pending",
			in:            ClassificationInput{Latest: b(3), Pending: b(3)},
			wantClass:     ClassificationBootstrapExternal,
			wantCandidate: "3",
		},
		{
			name:          "first scope latest below pending",
			in:            ClassificationInput{Latest: b(2), Pending: b(3)},
			wantClass:     ClassificationBootstrapExternal,
			wantCandidate: "3",
		},

		// --- bindings exist: consistent at/below M+1, candidate from durable state ---
		{
			name:          "bindings pending below M+1",
			in:            ClassificationInput{Latest: b(4), Pending: b(4), HasBindings: true, MaxBindingNonce: b(4)},
			wantClass:     ClassificationConsistent,
			wantCandidate: "5",
		},
		{
			name:          "bindings pending at M+1 boundary",
			in:            ClassificationInput{Latest: b(5), Pending: b(5), HasBindings: true, MaxBindingNonce: b(4)},
			wantClass:     ClassificationConsistent,
			wantCandidate: "5",
		},
		{
			name:          "bindings candidate takes the higher durable floor",
			in:            ClassificationInput{Latest: b(5), Pending: b(5), HasBindings: true, MaxBindingNonce: b(4), ReconciledFloor: b(7)},
			wantClass:     ClassificationConsistent,
			wantCandidate: "7",
		},
		{
			name:          "single binding at nonce zero",
			in:            ClassificationInput{Latest: b(1), Pending: b(1), HasBindings: true, MaxBindingNonce: b(0)},
			wantClass:     ClassificationConsistent,
			wantCandidate: "1",
		},
		{
			name:          "bindings chain view behind local state stays consistent",
			in:            ClassificationInput{Latest: b(0), Pending: b(0), HasBindings: true, MaxBindingNonce: b(4)},
			wantClass:     ClassificationConsistent,
			wantCandidate: "5",
		},

		// --- bindings exist, P > M+1: hold + refuse, never max(M+1, P) ---
		{
			name:        "unattributed consumption with mined evidence",
			in:          ClassificationInput{Latest: b(6), Pending: b(6), HasBindings: true, MaxBindingNonce: b(4)},
			wantClass:   ClassificationUnattributedConsumption,
			wantCause:   CauseUnattributedConsumption,
			wantRefusal: OutcomeScopeHeld,
		},
		{
			name:        "unattributed consumption boundary above M+1",
			in:          ClassificationInput{Latest: b(7), Pending: b(7), HasBindings: true, MaxBindingNonce: b(4)},
			wantClass:   ClassificationUnattributedConsumption,
			wantCause:   CauseUnattributedConsumption,
			wantRefusal: OutcomeScopeHeld,
		},
		{
			name:        "unexplained gap boundary latest at M+1",
			in:          ClassificationInput{Latest: b(5), Pending: b(6), HasBindings: true, MaxBindingNonce: b(4)},
			wantClass:   ClassificationUnexplainedGap,
			wantCause:   CauseUnexplainedGap,
			wantRefusal: OutcomeScopeHeld,
		},
		{
			name:        "unexplained gap pending only far above frontier",
			in:          ClassificationInput{Latest: b(0), Pending: b(9), HasBindings: true, MaxBindingNonce: b(4)},
			wantClass:   ClassificationUnexplainedGap,
			wantCause:   CauseUnexplainedGap,
			wantRefusal: OutcomeScopeHeld,
		},
		{
			name:        "hold branches compare against M+1 exactly",
			in:          ClassificationInput{Latest: b(6), Pending: b(6), HasBindings: true, MaxBindingNonce: b(4), ReconciledFloor: b(7)},
			wantClass:   ClassificationUnattributedConsumption,
			wantCause:   CauseUnattributedConsumption,
			wantRefusal: OutcomeScopeHeld,
		},

		// --- floor-only scope (no bindings, F set): F is the durable frontier ---
		{
			name:          "floor only pending at floor",
			in:            ClassificationInput{Latest: b(8), Pending: b(8), ReconciledFloor: b(8)},
			wantClass:     ClassificationConsistent,
			wantCandidate: "8",
		},
		{
			name:          "floor only pending below floor",
			in:            ClassificationInput{Latest: b(5), Pending: b(5), ReconciledFloor: b(8)},
			wantClass:     ClassificationConsistent,
			wantCandidate: "8",
		},
		{
			name:        "floor only unattributed above floor",
			in:          ClassificationInput{Latest: b(9), Pending: b(9), ReconciledFloor: b(8)},
			wantClass:   ClassificationUnattributedConsumption,
			wantCause:   CauseUnattributedConsumption,
			wantRefusal: OutcomeScopeHeld,
		},
		{
			name:        "floor only unexplained above floor",
			in:          ClassificationInput{Latest: b(8), Pending: b(9), ReconciledFloor: b(8)},
			wantClass:   ClassificationUnexplainedGap,
			wantCause:   CauseUnexplainedGap,
			wantRefusal: OutcomeScopeHeld,
		},
		{
			name:          "floor only at zero",
			in:            ClassificationInput{Latest: b(0), Pending: b(0), ReconciledFloor: b(0)},
			wantClass:     ClassificationConsistent,
			wantCandidate: "0",
		},

		// --- numeric boundaries ---
		{
			name:          "floor at the EVM maximum",
			in:            ClassificationInput{Latest: MaxNonceBig(), Pending: MaxNonceBig(), ReconciledFloor: MaxNonceBig()},
			wantClass:     ClassificationConsistent,
			wantCandidate: MaxNonceDecimal,
		},
		{
			name:        "binding frontier exhausted refuses instead of wrapping",
			in:          ClassificationInput{Latest: MaxNonceBig(), Pending: MaxNonceBig(), HasBindings: true, MaxBindingNonce: MaxNonceBig()},
			wantClass:   ClassificationUnavailable,
			wantRefusal: OutcomeTemporarilyUnavailable,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			latestBefore, pendingBefore := bigText(tc.in.Latest), bigText(tc.in.Pending)
			prevBefore, mBefore := bigText(tc.in.PendingPrev), bigText(tc.in.MaxBindingNonce)
			floorBefore := bigText(tc.in.ReconciledFloor)

			got := Classify(tc.in)

			if got.Classification != tc.wantClass {
				t.Fatalf("classification = %q, want %q", got.Classification, tc.wantClass)
			}
			if cand := bigText(got.Candidate); cand != tc.wantCandidate {
				t.Fatalf("candidate = %q, want %q (candidate must never be max(M+1, P))", cand, tc.wantCandidate)
			}
			if got.HoldCause != tc.wantCause {
				t.Fatalf("hold cause = %q, want %q", got.HoldCause, tc.wantCause)
			}
			if got.Refusal != tc.wantRefusal {
				t.Fatalf("refusal = %q, want %q", got.Refusal, tc.wantRefusal)
			}

			// Pure function: the input is never mutated.
			if bigText(tc.in.Latest) != latestBefore || bigText(tc.in.Pending) != pendingBefore ||
				bigText(tc.in.PendingPrev) != prevBefore || bigText(tc.in.MaxBindingNonce) != mBefore ||
				bigText(tc.in.ReconciledFloor) != floorBefore {
				t.Fatalf("Classify mutated its input")
			}
			// A candidate is a fresh value, never an alias of an input.
			if got.Candidate != nil && (got.Candidate == tc.in.Latest || got.Candidate == tc.in.Pending ||
				got.Candidate == tc.in.MaxBindingNonce || got.Candidate == tc.in.ReconciledFloor) {
				t.Fatalf("candidate aliases an input big.Int")
			}
		})
	}
}

// TestClassifyNeverAdoptsChainOnHold pins the shortcut prohibition directly:
// on every hold branch the decision carries no candidate at all.
func TestClassifyNeverAdoptsChainOnHold(t *testing.T) {
	for _, in := range []ClassificationInput{
		{Latest: classifyTestBig(10), Pending: classifyTestBig(10), HasBindings: true, MaxBindingNonce: classifyTestBig(4)},
		{Latest: classifyTestBig(5), Pending: classifyTestBig(10), HasBindings: true, MaxBindingNonce: classifyTestBig(4)},
		{Latest: classifyTestBig(10), Pending: classifyTestBig(10), ReconciledFloor: classifyTestBig(8)},
		{Latest: classifyTestBig(6), Pending: classifyTestBig(5)},
	} {
		got := Classify(in)
		if got.Candidate != nil {
			t.Fatalf("%q branch produced candidate %s; hold branches must refuse without adopting chain state",
				got.Classification, bigText(got.Candidate))
		}
		if got.Refusal != OutcomeScopeHeld {
			t.Fatalf("%q branch refusal = %q, want scope_held", got.Classification, got.Refusal)
		}
	}
}

// TestMaxBoundNonce pins M computation: nil-safe, skips empty nonces, returns
// a fresh maximum.
func TestMaxBoundNonce(t *testing.T) {
	if got := MaxBoundNonce(nil); got != nil {
		t.Fatalf("MaxBoundNonce(nil) = %s, want nil", got)
	}
	bindings := []Binding{
		{Nonce: nil},
		{Nonce: classifyTestBig(1)},
		{Nonce: classifyTestBig(5)},
		{Nonce: classifyTestBig(3)},
	}
	got := MaxBoundNonce(bindings)
	if bigText(got) != "5" {
		t.Fatalf("MaxBoundNonce = %s, want 5", bigText(got))
	}
	got.SetInt64(99)
	if bigText(bindings[2].Nonce) != "5" {
		t.Fatalf("MaxBoundNonce aliases the binding: binding changed to %s", bigText(bindings[2].Nonce))
	}
	if bigText(MaxBoundNonce([]Binding{{Nonce: classifyTestBig(7)}})) != "7" {
		t.Fatalf("single binding maximum not returned")
	}
}

// bigText renders a big.Int for comparison; nil renders as the empty string.
func bigText(n *big.Int) string {
	if n == nil {
		return ""
	}
	return n.String()
}
