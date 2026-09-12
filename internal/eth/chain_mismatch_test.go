package eth

import (
	"context"
	"math/big"
	"strings"
	"testing"
	"time"
)

// TestChainIDMismatchReportsBothValues covers FR-010 / SC-004: the error must
// state the expected and the actual chain id so the operator can diagnose it.
func TestChainIDMismatchReportsBothValues(t *testing.T) {
	c := newTestClient(t, chainIDResult("0x1")) // endpoint serves chain 1
	err := c.CheckChainID(context.Background(), big.NewInt(31337), time.Second)
	if err == nil {
		t.Fatal("CheckChainID() expected mismatch error")
	}
	if got := KindOf(err); got != KindChainMismatch {
		t.Fatalf("KindOf(%v) = %q, want %q", err, got, KindChainMismatch)
	}
	msg := err.Error()
	if !strings.Contains(msg, "expected 31337") || !strings.Contains(msg, "actual 1") {
		t.Fatalf("mismatch error lacks expected/actual values: %q", msg)
	}
}
