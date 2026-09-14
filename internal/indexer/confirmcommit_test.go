package indexer

import (
	"context"
	"errors"
	"testing"
)

// TestNewConfirmationCommitterValidation pins the constructor guards: nil
// pool and out-of-range thresholds never produce a committer.
func TestNewConfirmationCommitterValidation(t *testing.T) {
	if _, err := NewConfirmationCommitter(nil, ConfirmationConfig{ChainID: 1, ThresholdN: 10}); err == nil {
		t.Fatal("NewConfirmationCommitter(nil pool) = nil error, want rejection")
	}
	for _, tc := range []struct {
		name string
		cfg  ConfirmationConfig
	}{
		{"N=0", ConfirmationConfig{ChainID: 1, ThresholdN: 0}},
		{"N=MaxInt64+1", ConfirmationConfig{ChainID: 1, ThresholdN: uint64(1<<63-1) + 1}},
		{"chain 0", ConfirmationConfig{ChainID: 0, ThresholdN: 10}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewConfirmationCommitter(nil, tc.cfg); err == nil {
				t.Fatalf("NewConfirmationCommitter(%+v) = nil error, want rejection", tc.cfg)
			}
		})
	}
}

// TestConfirmNumericConfirmationsExact pins the NUMERIC binding path
// (uint64 -> base-10, no int64/float64 transit), including the 2^63 OI-1
// audit value.
func TestConfirmNumericConfirmationsExact(t *testing.T) {
	cases := []struct {
		in   uint64
		want string
	}{
		{0, "0"},
		{1, "1"},
		{10, "10"},
		{uint64(1<<63 - 1), "9223372036854775807"},
		{uint64(1) << 63, "9223372036854775808"},
		{^uint64(0), "18446744073709551615"},
	}
	for _, c := range cases {
		got := confirmNumericConfirmations(c.in)
		if !got.Valid || got.Exp != 0 {
			t.Fatalf("confirmNumericConfirmations(%d) = valid=%v exp=%d, want exact integer",
				c.in, got.Valid, got.Exp)
		}
		var dec string
		if got.Int == nil {
			t.Fatalf("confirmNumericConfirmations(%d) has nil coefficient", c.in)
		} else {
			dec = got.Int.String()
		}
		if dec != c.want {
			t.Fatalf("confirmNumericConfirmations(%d) = %q, want %q", c.in, dec, c.want)
		}
	}
}

// TestConfirmDepositUnitPreTransactionGuards pins the refusals issued before
// any statement runs (nil lease, foreign captured N): no pool is touched, so
// a pool-less committer suffices.
func TestConfirmDepositUnitPreTransactionGuards(t *testing.T) {
	ctx := context.Background()
	c := &ConfirmationCommitter{cfg: ConfirmationConfig{ChainID: 7, ThresholdN: 10}}

	if err := c.ConfirmDepositUnit(ctx, nil, ConfirmBasis{ThresholdN: 10, PolicySeq: 1}); err == nil {
		t.Fatal("ConfirmDepositUnit(nil lease) = nil error, want rejection")
	}
	if err := c.ConfirmDepositUnit(ctx, &Lease{}, ConfirmBasis{ThresholdN: 9, PolicySeq: 1}); err == nil {
		t.Fatal("ConfirmDepositUnit(N=9 vs cfg 10) = nil error, want rejection")
	} else {
		var drift *ConfirmationDriftError
		if !errors.As(err, &drift) {
			t.Fatalf("ConfirmDepositUnit(N=9 vs cfg 10) = %v (%T), want *ConfirmationDriftError", err, err)
		}
	}
	if err := c.ConfirmDepositUnit(ctx, &Lease{}, ConfirmBasis{ThresholdN: 10, PolicySeq: 0}); err == nil {
		t.Fatal("ConfirmDepositUnit(S=0) = nil error, want rejection")
	} else {
		var drift *ConfirmationDriftError
		if !errors.As(err, &drift) {
			t.Fatalf("ConfirmDepositUnit(S=0) = %v (%T), want *ConfirmationDriftError", err, err)
		}
	}
}
