// budget_test.go is the T064 chain-client half of the evidence: the optional
// budget overlay refuses before dispatch with a classified retryable error
// (never a fabricated result), and a nil overlay leaves classification
// untouched. The RPC endpoint is unreachable on purpose: a refused call must
// never reach the network.
package eth

import (
	"context"
	"errors"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// fakeGate is a scripted BudgetGate.
type fakeGate struct {
	mu      sync.Mutex
	err     error
	classes []string
}

func (g *fakeGate) Admit(_ context.Context, class string) (func(), error) {
	g.mu.Lock()
	g.classes = append(g.classes, class)
	g.mu.Unlock()
	if g.err != nil {
		return nil, g.err
	}
	return func() {}, nil
}

func dialUnreachable(t *testing.T) *Client {
	t.Helper()
	client, err := Dial(context.Background(), "http://127.0.0.1:1", time.Second)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(client.Close)
	return client
}

func TestClientBudgetPauseRefusesBeforeDispatch(t *testing.T) {
	client := dialUnreachable(t)
	gate := &fakeGate{err: ErrBudgetPaused}
	client.SetBudget(gate)

	if _, err := client.ChainID(context.Background()); KindOf(err) != KindBudgetPaused {
		t.Fatalf("ChainID = %v (kind %q), want budget-paused", err, KindOf(err))
	}
	if _, err := client.HeaderByNumber(context.Background(), big.NewInt(1)); KindOf(err) != KindBudgetPaused {
		t.Fatalf("HeaderByNumber = %v (kind %q), want budget-paused", err, KindOf(err))
	}
	if _, err := client.SendSignedTransaction(context.Background(), []byte{1}, common.Hash{}); KindOf(err) != KindBudgetPaused {
		t.Fatalf("SendSignedTransaction = %v (kind %q), want budget-paused", err, KindOf(err))
	}
	gate.mu.Lock()
	classes := append([]string(nil), gate.classes...)
	gate.mu.Unlock()
	want := []string{"read", "read", "send"}
	if len(classes) != len(want) {
		t.Fatalf("gate classes = %v, want %v", classes, want)
	}
	for i := range want {
		if classes[i] != want[i] {
			t.Fatalf("gate classes = %v, want %v", classes, want)
		}
	}
}

func TestClientBudgetLimitIsClassifiedRetryable(t *testing.T) {
	client := dialUnreachable(t)
	client.SetBudget(&fakeGate{err: ErrBudgetLimited})
	if _, err := client.BlockNumber(context.Background()); KindOf(err) != KindRateLimited {
		t.Fatalf("BlockNumber = %v (kind %q), want rate-limited", err, KindOf(err))
	}
}

func TestClientNilBudgetLeavesClassificationUntouched(t *testing.T) {
	client := dialUnreachable(t)
	_, err := client.BlockNumber(context.Background())
	if err == nil {
		t.Fatal("BlockNumber against an unreachable endpoint succeeded, want a classified transport error")
	}
	if KindOf(err) == KindBudgetPaused {
		t.Fatalf("classification = %q without a budget overlay, want the transport class", KindOf(err))
	}
	var ethErr *Error
	if !errors.As(err, &ethErr) {
		t.Fatalf("error %v is not classified (*eth.Error)", err)
	}
}
