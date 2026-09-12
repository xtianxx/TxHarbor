package eth

import (
	"context"
	"math/big"
	"net/http"
	"testing"
	"time"
)

// TestCheckChainIDSlowRPCDoesNotHang covers FR-013: a slow endpoint must
// surface a timeout failure instead of blocking the caller forever.
func TestCheckChainIDSlowRPCDoesNotHang(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(500 * time.Millisecond)
	})
	start := time.Now()
	err := c.CheckChainID(context.Background(), big.NewInt(1), 50*time.Millisecond)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("CheckChainID() expected timeout error")
	}
	if got := KindOf(err); got != KindTimeout {
		t.Fatalf("KindOf(%v) = %q, want %q", err, got, KindTimeout)
	}
	if elapsed > time.Second {
		t.Fatalf("CheckChainID() took %s, want failure within the timeout budget", elapsed)
	}
}
