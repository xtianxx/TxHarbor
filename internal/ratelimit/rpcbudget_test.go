// rpcbudget_test.go is the T064 unit evidence: baseline-bounded classes
// continue at a bounded degraded concurrency when the distributed budget is
// unavailable, distributed-only classes pause safely (observable transition,
// resumed on recovery), and a healthy budget is a plain retryable overlay —
// never a check-skipping shortcut.
package ratelimit

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeBudgetObserver struct {
	mu     sync.Mutex
	paused []string
}

func (o *fakeBudgetObserver) ObserveRPCBudgetPaused(class string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.paused = append(o.paused, class)
}

func testBudget(t *testing.T, store ScriptStore, obs RPCBudgetObserver) *RPCBudget {
	t.Helper()
	limiter, err := NewLimiter(store, testConfig(), nil)
	if err != nil {
		t.Fatalf("NewLimiter: %v", err)
	}
	budget, err := NewRPCBudget(limiter, obs, map[RPCClass]RPCClassPolicy{
		RPCClassRead: {BaselineBounded: true, DegradedConcurrency: 2},
		RPCClassSend: {BaselineBounded: false},
	})
	if err != nil {
		t.Fatalf("NewRPCBudget: %v", err)
	}
	return budget
}

func TestRPCBudgetHealthyPathUsesDistributedBudget(t *testing.T) {
	store := &fakeScriptStore{result: []any{int64(1), int64(1), int64(0)}}
	budget := testBudget(t, store, nil)
	release, err := budget.Admit(context.Background(), RPCClassSend)
	if err != nil {
		t.Fatalf("Admit(send) with a healthy budget: %v", err)
	}
	release()
	if call := store.lastCall(t); call.keys[0] != "txharbor:rl:rpc" {
		t.Fatalf("distributed key = %q, want txharbor:rl:rpc", call.keys[0])
	}

	store.result = []any{int64(0), int64(0), int64(100)}
	if _, err := budget.Admit(context.Background(), RPCClassSend); !errors.Is(err, ErrLimited) {
		t.Fatalf("Admit(denied) = %v, want ErrLimited", err)
	}
}

func TestRPCBudgetPausesDistributedOnlyClassAndResumes(t *testing.T) {
	store := &fakeScriptStore{err: errors.New("connection refused")}
	obs := &fakeBudgetObserver{}
	budget := testBudget(t, store, obs)
	ctx := context.Background()

	if _, err := budget.Admit(ctx, RPCClassSend); !errors.Is(err, ErrRPCPaused) {
		t.Fatalf("Admit(send) during the outage = %v, want ErrRPCPaused", err)
	}
	if _, err := budget.Admit(ctx, RPCClassSend); !errors.Is(err, ErrRPCPaused) {
		t.Fatalf("second Admit(send) = %v, want ErrRPCPaused", err)
	}
	// The pause transition is counted exactly once.
	if len(obs.paused) != 1 || obs.paused[0] != "send" {
		t.Fatalf("paused observations = %v, want [send]", obs.paused)
	}
	if got := budget.Status()["send"]; got != "paused" {
		t.Fatalf("status[send] = %q, want paused", got)
	}

	// Recovery resumes the class.
	store.err = nil
	store.result = []any{int64(1), int64(1), int64(0)}
	release, err := budget.Admit(ctx, RPCClassSend)
	if err != nil {
		t.Fatalf("Admit(send) after recovery: %v", err)
	}
	release()
	if got := budget.Status()["send"]; got != "ok" {
		t.Fatalf("status[send] after recovery = %q, want ok", got)
	}
}

// TestRPCBudgetCallerCancelKeepsPosture: caller cancellation is not a
// dependency signal — it never pauses a class and never clears an existing
// pause, while a real transport fault during the same outage still pauses.
func TestRPCBudgetCallerCancelKeepsPosture(t *testing.T) {
	store := &fakeScriptStore{err: context.Canceled}
	obs := &fakeBudgetObserver{}
	budget := testBudget(t, store, obs)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := budget.Admit(ctx, RPCClassSend); !errors.Is(err, context.Canceled) || errors.Is(err, ErrRPCPaused) {
		t.Fatalf("Admit(send) on caller cancel = %v, want context.Canceled without ErrRPCPaused", err)
	}
	if got := budget.Status()["send"]; got != "ok" {
		t.Fatalf("status[send] after caller cancel = %q, want ok", got)
	}
	if len(obs.paused) != 0 {
		t.Fatalf("pause observations after caller cancel = %v, want none", obs.paused)
	}

	// A genuine transport fault still pauses, even with the caller canceled.
	store.err = errors.New("connection refused")
	if _, err := budget.Admit(ctx, RPCClassSend); !errors.Is(err, ErrRPCPaused) {
		t.Fatalf("Admit(send) on a transport fault = %v, want ErrRPCPaused", err)
	}
	if got := budget.Status()["send"]; got != "paused" {
		t.Fatalf("status[send] after the transport fault = %q, want paused", got)
	}
	if len(obs.paused) != 1 || obs.paused[0] != "send" {
		t.Fatalf("pause observations after the transport fault = %v, want [send]", obs.paused)
	}

	// A later caller cancellation must not clear the existing pause.
	store.err = context.Canceled
	if _, err := budget.Admit(ctx, RPCClassSend); !errors.Is(err, context.Canceled) {
		t.Fatalf("Admit(send) on a later caller cancel = %v, want context.Canceled", err)
	}
	if got := budget.Status()["send"]; got != "paused" {
		t.Fatalf("status[send] after a cancel during the outage = %q, want still paused", got)
	}
	if len(obs.paused) != 1 {
		t.Fatalf("pause observations after the cancel = %v, want still one", obs.paused)
	}
}

func TestRPCBudgetBaselineClassContinuesBoundedWhileDegraded(t *testing.T) {
	store := &fakeScriptStore{err: errors.New("timeout")}
	budget := testBudget(t, store, nil)
	ctx := context.Background()

	// The class continues: a nil error means the caller performs the RPC.
	var inFlight, maxInFlight atomic.Int64
	releaseCh := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			release, err := budget.Admit(ctx, RPCClassRead)
			if err != nil {
				t.Errorf("Admit(read) while degraded = %v, want continue", err)
				return
			}
			current := inFlight.Add(1)
			for {
				max := maxInFlight.Load()
				if current <= max || maxInFlight.CompareAndSwap(max, current) {
					break
				}
			}
			<-releaseCh
			inFlight.Add(-1)
			release()
		}()
	}
	deadline := time.Now().Add(2 * time.Second)
	for inFlight.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(30 * time.Millisecond)
	close(releaseCh)
	wg.Wait()
	if got := maxInFlight.Load(); got > 2 {
		t.Fatalf("max degraded concurrency = %d, want <= 2", got)
	}
	if got := budget.Status()["read"]; got != "degraded" {
		t.Fatalf("status[read] = %q, want degraded", got)
	}
}

func TestRPCBudgetUnknownClassFailsClosed(t *testing.T) {
	store := &fakeScriptStore{result: []any{int64(1), int64(1), int64(0)}}
	budget := testBudget(t, store, nil)
	if _, err := budget.Admit(context.Background(), RPCClass("bogus")); err == nil {
		t.Fatal("Admit(unknown class) succeeded, want fail-closed error")
	}
}

func TestNewRPCBudgetRejectsIncompletePolicy(t *testing.T) {
	limiter, err := NewLimiter(&fakeScriptStore{}, testConfig(), nil)
	if err != nil {
		t.Fatalf("NewLimiter: %v", err)
	}
	cases := []struct {
		name     string
		policies map[RPCClass]RPCClassPolicy
	}{
		{"no classes", nil},
		{"unknown class", map[RPCClass]RPCClassPolicy{"bogus": {BaselineBounded: false}}},
		{"baseline without concurrency", map[RPCClass]RPCClassPolicy{RPCClassRead: {BaselineBounded: true}}},
	}
	for _, tc := range cases {
		if _, err := NewRPCBudget(limiter, nil, tc.policies); err == nil {
			t.Errorf("%s: NewRPCBudget succeeded, want fail-closed error", tc.name)
		}
	}
	if _, err := NewRPCBudget(nil, nil, map[RPCClass]RPCClassPolicy{RPCClassSend: {}}); err == nil {
		t.Error("NewRPCBudget(nil limiter) succeeded, want fail-closed error")
	}
}
