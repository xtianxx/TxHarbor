package eth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
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

const zeroHash = "0x0000000000000000000000000000000000000000000000000000000000000000"

// headerResultJSON is a minimal valid eth_getBlockByNumber result.
func headerResultJSON(number uint64) string {
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"result":{`+
		`"parentHash":%q,"sha3Uncles":%q,"miner":"0x0000000000000000000000000000000000000000",`+
		`"stateRoot":%q,"transactionsRoot":%q,"receiptsRoot":%q,"logsBloom":"0x%s",`+
		`"difficulty":"0x0","number":"0x%x","gasLimit":"0x1c9c380","gasUsed":"0x0",`+
		`"timestamp":"0x64","extraData":"0x","mixHash":%q,"nonce":"0x0000000000000000"}}`,
		zeroHash, zeroHash, zeroHash, zeroHash, zeroHash,
		strings.Repeat("00", 256), number, zeroHash)
}

func TestHeaderByNumberSuccessUsesFullTxFalse(t *testing.T) {
	type reqInfo struct {
		method string
		params []any
	}
	reqs := make(chan reqInfo, 1)
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Method string `json:"method"`
			Params []any  `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		reqs <- reqInfo{method: req.Method, params: req.Params}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(headerResultJSON(42)))
	})

	h, err := c.HeaderByNumber(context.Background(), big.NewInt(42))
	if err != nil {
		t.Fatalf("HeaderByNumber() error = %v", err)
	}
	if h.Number.Uint64() != 42 {
		t.Fatalf("HeaderByNumber() number = %v, want 42", h.Number)
	}
	info := <-reqs
	if info.method != "eth_getBlockByNumber" {
		t.Fatalf("method = %q, want eth_getBlockByNumber", info.method)
	}
	if len(info.params) != 2 || info.params[1] != false {
		t.Fatalf("params = %v, want [number, false]", info.params)
	}
}

func TestHeaderByNumberNullResultIsNotFound(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":null}`))
	})
	h, err := c.HeaderByNumber(context.Background(), big.NewInt(1_000_000))
	if h != nil {
		t.Fatalf("HeaderByNumber() header = %v, want nil", h)
	}
	if KindOf(err) != KindNotFound {
		t.Fatalf("KindOf(%v) = %q, want %q", err, KindOf(err), KindNotFound)
	}
	if !errors.Is(err, ethereum.NotFound) {
		t.Fatalf("errors.Is(%v, ethereum.NotFound) = false, want true", err)
	}
}

func TestHeaderByNumberRateLimitedClassified(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte("rate limit exceeded"))
	})
	_, err := c.HeaderByNumber(context.Background(), big.NewInt(1))
	if err == nil {
		t.Fatal("HeaderByNumber() expected error for HTTP 429")
	}
	if KindOf(err) != KindRateLimited {
		t.Fatalf("KindOf(%v) = %q, want %q", err, KindOf(err), KindRateLimited)
	}
}

func TestHeaderByNumberTimeoutClassified(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(200 * time.Millisecond)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err := c.HeaderByNumber(ctx, big.NewInt(1))
	if err == nil {
		t.Fatal("HeaderByNumber() expected timeout error")
	}
	if KindOf(err) != KindTimeout {
		t.Fatalf("KindOf(%v) = %q, want %q", err, KindOf(err), KindTimeout)
	}
}

func TestHeaderByNumberMalformedJSONIsInvalidResponse(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("not json"))
	})
	_, err := c.HeaderByNumber(context.Background(), big.NewInt(1))
	if err == nil {
		t.Fatal("HeaderByNumber() expected error for malformed JSON")
	}
	if KindOf(err) != KindInvalidResponse {
		t.Fatalf("KindOf(%v) = %q, want %q", err, KindOf(err), KindInvalidResponse)
	}
}
