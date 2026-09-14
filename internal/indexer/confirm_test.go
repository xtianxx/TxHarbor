package indexer

import (
	"errors"
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
