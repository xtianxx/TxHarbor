package indexer

import (
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

func TestJitterWithinQuarter(t *testing.T) {
	for _, base := range []time.Duration{time.Millisecond, 200 * time.Millisecond, time.Second, 30 * time.Second} {
		lo, hi := base-base/4, base+base/4
		for i := 0; i < 1000; i++ {
			if got := jitter(base); got < lo || got > hi {
				t.Fatalf("jitter(%s) = %s, want within [%s, %s]", base, got, lo, hi)
			}
		}
	}
	// Durations below the jitter quantum are returned unchanged.
	if got := jitter(2 * time.Nanosecond); got != 2*time.Nanosecond {
		t.Fatalf("jitter(2ns) = %s, want 2ns", got)
	}
}

func TestBackoffGrowthCapAndReset(t *testing.T) {
	b := newBackoff(200*time.Millisecond, time.Second)
	want := []time.Duration{
		200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond,
		time.Second, time.Second, time.Second,
	}
	for i, base := range want {
		got := b.next()
		if !withinQuarter(got, base) {
			t.Fatalf("next() #%d = %s, want within +-25%% of %s", i+1, got, base)
		}
	}
	if b.attempt != len(want) {
		t.Fatalf("attempt = %d, want %d", b.attempt, len(want))
	}
	b.reset()
	if b.attempt != 0 || b.current != 0 {
		t.Fatalf("after reset attempt=%d current=%s, want 0/0", b.attempt, b.current)
	}
	if got := b.next(); !withinQuarter(got, 200*time.Millisecond) {
		t.Fatalf("after reset next() = %s, want ~200ms", got)
	}
}

func withinQuarter(d, base time.Duration) bool {
	return d >= base-base/4 && d <= base+base/4
}

func TestValidHash(t *testing.T) {
	valid := "0x" + strings.Repeat("ab", 32)
	if !validHash(valid) {
		t.Fatalf("validHash(%q) = false, want true", valid)
	}
	if !validHash("0x" + strings.Repeat("00", 32)) {
		t.Fatal("all-zero genesis parent hash must be valid")
	}
	for _, bad := range []string{
		"",
		"0x",
		"0x1234",
		strings.ToUpper(valid),
		"0X" + strings.Repeat("ab", 32),
		valid + "0",
		"0x" + strings.Repeat("zz", 32),
	} {
		if validHash(bad) {
			t.Fatalf("validHash(%q) = true, want false", bad)
		}
	}
	if got := hashHex(common.HexToHash(strings.ToUpper(valid))); got != valid {
		t.Fatalf("hashHex() = %q, want %q", got, valid)
	}
}

func TestCheckHeader(t *testing.T) {
	h := &types.Header{Number: big.NewInt(7)}
	if err := checkHeader(h, 7); err != nil {
		t.Fatalf("checkHeader() error = %v, want nil", err)
	}
	if err := checkHeader(h, 8); err == nil {
		t.Fatal("checkHeader() accepted a height mismatch")
	}
	if err := checkHeader(nil, 7); err == nil {
		t.Fatal("checkHeader(nil) = nil error, want error")
	}
	if err := checkHeader(&types.Header{}, 0); err == nil {
		t.Fatal("checkHeader() accepted a header without a number")
	}
}
