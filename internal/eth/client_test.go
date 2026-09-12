package eth

import (
	"context"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// newTestClient starts a fake JSON-RPC endpoint and returns a client for it.
func newTestClient(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	c, err := Dial(context.Background(), srv.URL, time.Second)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	t.Cleanup(c.Close)
	return c
}

func chainIDResult(id string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0", "id": 1, "result": id,
		})
	}
}

func TestCheckChainIDSuccess(t *testing.T) {
	c := newTestClient(t, chainIDResult("0x1"))
	if err := c.CheckChainID(context.Background(), big.NewInt(1), time.Second); err != nil {
		t.Fatalf("CheckChainID() error = %v", err)
	}
}

func TestChainIDReturnsValue(t *testing.T) {
	c := newTestClient(t, chainIDResult("0x7a69")) // 31337
	id, err := c.ChainID(context.Background())
	if err != nil {
		t.Fatalf("ChainID() error = %v", err)
	}
	if id.Cmp(big.NewInt(31337)) != 0 {
		t.Fatalf("ChainID() = %s, want 31337", id)
	}
}

func TestChainIDTimeoutClassified(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(200 * time.Millisecond)
	})
	err := c.CheckChainID(context.Background(), big.NewInt(1), 30*time.Millisecond)
	if err == nil {
		t.Fatal("CheckChainID() expected timeout error")
	}
	if KindOf(err) != KindTimeout {
		t.Fatalf("KindOf(%v) = %q, want %q", err, KindOf(err), KindTimeout)
	}
}

func TestChainIDTransportErrorClassified(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	})
	err := c.CheckChainID(context.Background(), big.NewInt(1), time.Second)
	if err == nil {
		t.Fatal("CheckChainID() expected error")
	}
	if KindOf(err) != KindTransport {
		t.Fatalf("KindOf(%v) = %q, want %q", err, KindOf(err), KindTransport)
	}
}

func TestChainIDRPCErrorIsInvalidResponse(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"boom"}}`))
	})
	err := c.CheckChainID(context.Background(), big.NewInt(1), time.Second)
	if err == nil {
		t.Fatal("CheckChainID() expected error")
	}
	if KindOf(err) != KindInvalidResponse {
		t.Fatalf("KindOf(%v) = %q, want %q", err, KindOf(err), KindInvalidResponse)
	}
}

func TestChainIDMalformedJSONFails(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("not json"))
	})
	if err := c.CheckChainID(context.Background(), big.NewInt(1), time.Second); err == nil {
		t.Fatal("CheckChainID() expected error for malformed JSON")
	}
}

func TestDialUnreachableFailsWithinTimeout(t *testing.T) {
	// Reserved TEST-NET-1 address; the handshake must not hang.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := Dial(ctx, "http://192.0.2.1:8545", 100*time.Millisecond)
		done <- err
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Dial() hung on unreachable endpoint")
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	c := newTestClient(t, chainIDResult("0x1"))
	c.Close()
	c.Close() // must not panic
}
