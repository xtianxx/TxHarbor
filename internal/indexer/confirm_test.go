package indexer

import (
	"errors"
	"fmt"
	"testing"
)

// TestConfirmationReachedMatchesSpecFormula checks the gate against the spec
// formula confirmations=max(0,tip-h+1) >= N value by value, including N=1,
// N=MaxInt64 and tip<h (both sides false).
func TestConfirmationReachedMatchesSpecFormula(t *testing.T) {
	const maxN = uint64(1<<63 - 1)
	maxU := ^uint64(0)
	cases := []struct {
		name      string
		tip, h, n uint64
		wantGate  bool
		wantExact uint64
	}{
		{"N=1 tip==h", 7, 7, 1, true, 1},
		{"N=1 tip>h", 8, 7, 1, true, 2},
		{"N=1 tip<h both-false", 6, 7, 1, false, 0},
		{"N=10 conf=9 below", 108, 100, 10, false, 9},
		{"N=10 conf=10 at", 109, 100, 10, true, 10},
		{"N=10 conf=11 above", 110, 100, 10, true, 11},
		{"tip<h both-false", 50, 100, 10, false, 0},
		{"tip<h N=MaxInt64 both-false", 0, 1, maxN, false, 0},
		{"N=MaxInt64 just-below", maxN - 2, 0, maxN, false, maxN - 1},
		{"N=MaxInt64 at", maxN - 1, 0, maxN, true, maxN},
		{"N=MaxInt64 above", maxN, 0, maxN, true, maxN + 1},
		{"zero tip zero h N=1", 0, 0, 1, true, 1},
		{"zero tip N=2 empty", 0, 0, 2, false, 1},
		// Hypothetical MaxUint64 inputs: the gate stays overflow-free.
		{"maxUint64 tip==h N=1", maxU, maxU, 1, true, 1},
		{"maxUint64 tip==h N=MaxInt64", maxU, maxU, maxN, false, 1},
		{"maxUint64 h=maxUint64-1 N=2", maxU, maxU - 1, 2, true, 2},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := ConfirmationReached(c.tip, c.h, c.n); got != c.wantGate {
				t.Fatalf("ConfirmationReached(%d,%d,%d) = %v, want %v",
					c.tip, c.h, c.n, got, c.wantGate)
			}
			exact, err := ExactConfirmations(c.tip, c.h)
			if err != nil {
				t.Fatalf("ExactConfirmations(%d,%d) error = %v", c.tip, c.h, err)
			}
			if exact != c.wantExact {
				t.Fatalf("ExactConfirmations(%d,%d) = %d, want %d",
					c.tip, c.h, exact, c.wantExact)
			}
			// 逐值对照: gate must equal (exact >= N) on every row.
			if want := exact >= c.n; want != c.wantGate {
				t.Fatalf("gate/spec mismatch at tip=%d h=%d N=%d: gate=%v exact=%d",
					c.tip, c.h, c.n, c.wantGate, exact)
			}
		})
	}
}

// TestExactConfirmationsSaturationGuard is the unreachable-defense existence
// proof: tip==MaxUint64 && h==0 saturates, and the trigger refuses the commit
// instead of yielding a storable value.
func TestExactConfirmationsSaturationGuard(t *testing.T) {
	maxU := ^uint64(0)
	if _, err := ExactConfirmations(maxU, 0); !errors.Is(err, ErrConfirmationsSaturated) {
		t.Fatalf("ExactConfirmations(MaxUint64,0) error = %v, want ErrConfirmationsSaturated", err)
	}
	// Division of labor: the gate still decides without overflow on the same
	// input (true here since tip-h dwarfs any in-range N), while the exact
	// value refuses.
	if !ConfirmationReached(maxU, 0, uint64(1<<63-1)) {
		t.Fatal("ConfirmationReached(MaxUint64,0,MaxInt64) = false, want true (overflow-free gate)")
	}
}

func TestMaxEligibleHeight(t *testing.T) {
	maxU := ^uint64(0)
	const maxN = uint64(1<<63 - 1)
	cases := []struct {
		name   string
		tip, n uint64
		want   uint64
		wantOK bool
	}{
		{"basic", 100, 10, 91, true},
		{"N=1 identity", 100, 1, 100, true},
		{"zero tip N=1", 0, 1, 0, true},
		{"empty tip+1<N", 5, 10, 0, false},
		{"boundary empty", 8, 10, 0, false},
		{"boundary first-ok", 9, 10, 0, true},
		{"N=0 empty", 100, 0, 0, false},
		{"maxUint64 N=1 no-overflow", maxU, 1, maxU, true},
		{"maxUint64 N=MaxInt64", maxU, maxN, (uint64(1) << 63) + 1, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := MaxEligibleHeight(c.tip, c.n)
			if ok != c.wantOK || (ok && got != c.want) {
				t.Fatalf("MaxEligibleHeight(%d,%d) = (%d,%v), want (%d,%v)",
					c.tip, c.n, got, ok, c.want, c.wantOK)
			}
		})
	}
}

func TestNewConfirmationConfigValidation(t *testing.T) {
	const maxN = uint64(1<<63 - 1)
	if _, err := NewConfirmationConfig(ConfirmationConfig{ChainID: 1, ThresholdN: 1}); err != nil {
		t.Fatalf("N=1 valid, got %v", err)
	}
	if _, err := NewConfirmationConfig(ConfirmationConfig{ChainID: 1, ThresholdN: maxN}); err != nil {
		t.Fatalf("N=MaxInt64 valid, got %v", err)
	}
	for _, tc := range []struct {
		name string
		cfg  ConfirmationConfig
	}{
		{"N=0 rejected", ConfirmationConfig{ChainID: 1, ThresholdN: 0}},
		{"N=MaxInt64+1 rejected", ConfirmationConfig{ChainID: 1, ThresholdN: maxN + 1}},
		{"N=MaxUint64 rejected", ConfirmationConfig{ChainID: 1, ThresholdN: ^uint64(0)}},
		{"chain 0 rejected", ConfirmationConfig{ChainID: 0, ThresholdN: 10}},
		{"chain negative rejected", ConfirmationConfig{ChainID: -1, ThresholdN: 10}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewConfirmationConfig(tc.cfg); err == nil {
				t.Fatalf("NewConfirmationConfig(%+v) = nil error, want rejection", tc.cfg)
			}
		})
	}
}

// TestConfirmationReachedBoundaryMatrix is the T014 US2 decision matrix
// (FR-01, research R1复核后; US2-1/2/3, SC-01/02): N-1 stays, N/N+1 convert;
// N=1 with tip==h confirms with exactly 1; N=MaxInt64 is eligible only at the
// unreachable endpoint and never达标 at practical heights; tip<h is both-false.
// Every row is driven through the real gate predicate ConfirmationReached and
// cross-checked against the spec formula via ExactConfirmations. Saturation
// guard existence stays owned by T003 (TestExactConfirmationsSaturationGuard)
// and is only referenced, not duplicated, here.
func TestConfirmationReachedBoundaryMatrix(t *testing.T) {
	const maxN = uint64(1<<63 - 1)
	cases := []struct {
		name      string
		tip, h, n uint64
		wantGate  bool
		wantExact uint64
	}{
		{"N=10 conf=9 stays", 108, 100, 10, false, 9},
		{"N=10 conf=10 converts", 109, 100, 10, true, 10},
		{"N=10 conf=11 converts", 110, 100, 10, true, 11},
		{"N=1 tip==h conf=1 converts", 7, 7, 1, true, 1},
		{"N=MaxInt64 practical never达标", 1000000, 0, maxN, false, 1000001},
		{"N=MaxInt64 conf=10 practical never达标", 1000000, 999991, maxN, false, 10},
		{"N=MaxInt64 endpoint eligible", maxN - 1, 0, maxN, true, maxN},
		{"tip<h both-false", 50, 100, 10, false, 0},
		{"tip<h N=1 both-false", 6, 7, 1, false, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := ConfirmationReached(c.tip, c.h, c.n); got != c.wantGate {
				t.Fatalf("ConfirmationReached(%d,%d,%d) = %v, want %v",
					c.tip, c.h, c.n, got, c.wantGate)
			}
			exact, err := ExactConfirmations(c.tip, c.h)
			if err != nil {
				t.Fatalf("ExactConfirmations(%d,%d) error = %v", c.tip, c.h, err)
			}
			if exact != c.wantExact {
				t.Fatalf("ExactConfirmations(%d,%d) = %d, want %d",
					c.tip, c.h, exact, c.wantExact)
			}
			// 逐值对照: gate must equal (exact >= N) on every row.
			if want := exact >= c.n; want != c.wantGate {
				t.Fatalf("gate/spec mismatch at tip=%d h=%d N=%d: gate=%v exact=%d",
					c.tip, c.h, c.n, c.wantGate, exact)
			}
		})
	}
}

// TestConfirmationWaitStopPredicateMatrix is T020 (FR-06; US3-1/3):
// the pure-logic predicate matrix for data-model.md §候选分类, driven through
// the real classifyConfirmationOutcome (confirmscan.go, read-only here) and
// the real ConfirmationReached gate. below_depth is the only row-level wait
// (row stays Pending, batch continues); every other anomaly halts with the
// exact contracts/observability.md reason string.
//
// Loop-level distinction (not duplicated here; T018 owns the ServeLoop
// tests): a missing/untrusted tip on the loop read path waits for a trusted
// tip (state=1, zero commits), while the classifier maps the commit-time
// chain-view tip details below to halt verdicts (tip_missing/tip_untrusted)
// as specified — the wait-vs-halt split is read-path vs commit-path, and this
// test pins only the classifier side.
func TestConfirmationWaitStopPredicateMatrix(t *testing.T) {
	// below_depth arises two ways; both are row-wait, batch continues.
	t.Run("below_depth/pre-check gate false stays Pending", func(t *testing.T) {
		// tip=108 h=100 N=10: conf=9 < 10, gate false (same row as T014).
		if ConfirmationReached(108, 100, 10) {
			t.Fatal("ConfirmationReached(108,100,10) = true, want false (below_depth pre-check wait)")
		}
	})
	t.Run("below_depth/commit-time re-computed gate fails waits row", func(t *testing.T) {
		// Detail built exactly as confirmcommit.go builds it (Sprintf with
		// tip/h/N verbs); wrapped once to prove the As path survives
		// production-style %w layering.
		err := fmt.Errorf("confirm deposit observation: %w",
			&ConfirmationChainViewError{detail: fmt.Sprintf(
				"re-computed gate fails: tip=%d h=%d N=%d", 10, 5, 10)})
		got, reason := classifyConfirmationOutcome(err)
		if got != confirmWaitRow || reason != "below_depth" {
			t.Fatalf("classify(gate-fail) = (%d, %q), want (%d, %q)",
				got, reason, confirmWaitRow, "below_depth")
		}
	})

	// Halt matrix: each error is constructed exactly as production produces
	// it (confirmcommit.go Sprintf verbs, depositscanner.go streamPauseError
	// fields, lease.go ErrLeaseLost wrapping).
	halts := []struct {
		name       string
		err        error
		wantReason string
	}{
		{"reference missing halts",
			&ConfirmationChainViewError{detail: fmt.Sprintf(
				"reference block %d is missing or non-canonical under the lock", 5)},
			"reference_unverifiable"},
		{"hash mismatch halts",
			&ConfirmationChainViewError{detail: fmt.Sprintf(
				"reference block %d hash %s diverges from candidate bh %s", 5, "0xref", "0xbh")},
			"reference_unverifiable"},
		{"tip missing halts",
			&ConfirmationChainViewError{detail: "canonical tip is missing under the lock"},
			"tip_missing"},
		{"untrusted tip halts",
			&ConfirmationChainViewError{detail: fmt.Sprintf(
				"captured tip (%d %s) differs from canonical tip (%d %s)", 10, "0xa", 11, "0xb")},
			"tip_untrusted"},
		{"pause halts",
			&streamPauseError{stream: "log_pause", chainID: 7},
			"pause_present"},
		{"policy drift halts",
			&ConfirmationDriftError{detail: fmt.Sprintf(
				"captured policy (seq=%d N=%d) differs from effective (seq=%d N=%d)", 1, 10, 2, 10)},
			"policy_drift"},
		{"startup config mismatch halts as drift",
			&confirmationConfigMismatchError{detail: "effective policy threshold=7 differs from configured N=10"},
			"policy_drift"},
		{"lease lost halts",
			fmt.Errorf("%w: owner/fencing/expiry verdict failed", ErrLeaseLost),
			"lease_lost"},
	}
	for _, tc := range halts {
		t.Run(tc.name, func(t *testing.T) {
			got, reason := classifyConfirmationOutcome(tc.err)
			if got != confirmHalt || reason != tc.wantReason {
				t.Fatalf("classify(%v) = (%d, %q), want (%d, %q)",
					tc.err, got, reason, confirmHalt, tc.wantReason)
			}
		})
	}

	// Boundary rows (neither wait-row nor halt): stale basis re-ticks the
	// outer loop, an unknown transient backs off. Included so the matrix
	// shows below_depth is the ONLY wait branch.
	t.Run("stale basis reticks (not wait, not halt)", func(t *testing.T) {
		got, reason := classifyConfirmationOutcome(errStaleState)
		if got != confirmRetryTick || reason != "" {
			t.Fatalf("classify(errStaleState) = (%d, %q), want (%d, %q)",
				got, reason, confirmRetryTick, "")
		}
	})
	t.Run("transient backs off (not wait, not halt)", func(t *testing.T) {
		got, reason := classifyConfirmationOutcome(errors.New("connection refused"))
		if got != confirmBackoff || reason != "" {
			t.Fatalf("classify(transient) = (%d, %q), want (%d, %q)",
				got, reason, confirmBackoff, "")
		}
	})
}

// TestConfirmationHaltTransitionBuckets is T020 (FR-06; US3-1/3): the
// stale-vs-rejected counter mapping for contracts/observability.md
// transition_total (ok|stale|rejected). The bucket rule mirrors the ServeLoop
// halt branch (confirmscan.go: drift/pause reason → "rejected", every other
// commit-time halt reason → "stale"; lease_lost returns early with no
// transition_total sample). Reasons come from the real classifier; only the
// bucket rule is quoted here because it has no standalone helper (production
// owns it inline, read-only for this test).
func TestConfirmationHaltTransitionBuckets(t *testing.T) {
	// transitionBucket quotes the ServeLoop halt-branch counter rule.
	transitionBucket := func(reason string) string {
		if reason == "policy_drift" || reason == "pause_present" {
			return "rejected"
		}
		return "stale"
	}
	cases := []struct {
		name           string
		err            error
		wantReason     string
		wantTransition string // literal per ServeLoop + observability contract
	}{
		{"drift rejected",
			&ConfirmationDriftError{detail: "captured N=10 differs from configured N=7"},
			"policy_drift", "rejected"},
		{"startup mismatch rejected",
			&confirmationConfigMismatchError{detail: "x"},
			"policy_drift", "rejected"},
		{"pause rejected",
			&streamPauseError{stream: "deposit_pause", chainID: 7},
			"pause_present", "rejected"},
		{"reference missing stale",
			&ConfirmationChainViewError{detail: fmt.Sprintf(
				"reference block %d is missing or non-canonical under the lock", 91)},
			"reference_unverifiable", "stale"},
		{"hash mismatch stale",
			&ConfirmationChainViewError{detail: fmt.Sprintf(
				"reference block %d hash %s diverges from candidate bh %s", 91, "0xref", "0xbh")},
			"reference_unverifiable", "stale"},
		{"tip missing stale",
			&ConfirmationChainViewError{detail: "canonical tip is missing under the lock"},
			"tip_missing", "stale"},
		{"untrusted tip stale",
			&ConfirmationChainViewError{detail: fmt.Sprintf(
				"captured tip (%d %s) differs from canonical tip (%d %s)", 100, "0xa", 101, "0xb")},
			"tip_untrusted", "stale"},
		{"commit-time below_depth stale",
			&ConfirmationChainViewError{detail: fmt.Sprintf(
				"re-computed gate fails: tip=%d h=%d N=%d", 100, 95, 10)},
			"below_depth", "stale"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, reason := classifyConfirmationOutcome(c.err)
			if reason != c.wantReason {
				t.Fatalf("classify(%v) reason = %q, want %q", c.err, reason, c.wantReason)
			}
			if got := transitionBucket(reason); got != c.wantTransition {
				t.Fatalf("transition bucket for reason %q = %q, want %q", reason, got, c.wantTransition)
			}
		})
	}
	// lease_lost halts but emits no transition_total sample (ServeLoop takes
	// the early warn-and-return path before ObserveConfirmationTransition).
	t.Run("lease lost halts with no transition sample", func(t *testing.T) {
		got, reason := classifyConfirmationOutcome(
			fmt.Errorf("%w: owner/fencing/expiry verdict failed", ErrLeaseLost))
		if got != confirmHalt || reason != "lease_lost" {
			t.Fatalf("classify(lease-lost) = (%d, %q), want (%d, %q)",
				got, reason, confirmHalt, "lease_lost")
		}
	})
}
