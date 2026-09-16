// reconcile_test.go owns spec task T029's unit surface: the reconcile observer
// loop's scheduling and stop. No PostgreSQL/Anvil is involved — the loop's
// scope-list and per-scope work are injected fakes (the EARLY-VALIDATION
// pattern the other unit tests use) — so this pins exactly "starts, ticks on
// the configured cadence, stops cleanly when cancelled" and that a failing
// list/pass never stops the loop or becomes fatal.
package nonce

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	reconcileTestSenderA = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	reconcileTestSenderB = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

// TestReconcileLoopSchedulesAndStops runs Run under the Serve-like lifecycle
// (goroutine + cancel, as serve.go drives it with runCtx): the immediate pass
// reconciles every known scope, further passes follow the configured cadence,
// and cancellation joins the goroutine with no further ticks.
func TestReconcileLoopSchedulesAndStops(t *testing.T) {
	scopes := []string{reconcileTestSenderA, reconcileTestSenderB}
	var calls atomic.Int64
	l := &ReconcileLoop{
		interval:   5 * time.Millisecond,
		logger:     discardLog(),
		listScopes: func(context.Context) ([]string, error) { return scopes, nil },
		reconcile: func(context.Context, string) error {
			calls.Add(1)
			return nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); l.Run(ctx) }()

	reconcileWaitFor(t, 2*time.Second, func() bool { return calls.Load() >= int64(len(scopes)) })
	reconcileWaitFor(t, 2*time.Second, func() bool { return calls.Load() >= int64(2*len(scopes)) })

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancellation (goroutine leak)")
	}

	stopped := calls.Load()
	time.Sleep(4 * l.interval)
	if got := calls.Load(); got != stopped {
		t.Fatalf("loop ticked after stop: %d -> %d", stopped, got)
	}
}

// TestReconcileLoopSurvivesFailures proves neither a scope-list failure nor a
// per-scope failure is fatal: a failed list skips one pass, a failing scope
// skips only that scope, and the next pass still reconciles every scope.
func TestReconcileLoopSurvivesFailures(t *testing.T) {
	var listCalls atomic.Int64
	var mu sync.Mutex
	var attempted []string
	l := &ReconcileLoop{
		interval: 5 * time.Millisecond,
		logger:   discardLog(),
		listScopes: func(context.Context) ([]string, error) {
			if listCalls.Add(1) == 1 {
				return nil, errors.New("scope list unavailable")
			}
			return []string{reconcileTestSenderA, reconcileTestSenderB}, nil
		},
		reconcile: func(_ context.Context, sender string) error {
			mu.Lock()
			attempted = append(attempted, sender)
			mu.Unlock()
			if sender == reconcileTestSenderA {
				return errors.New("rpc unavailable")
			}
			return nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); l.Run(ctx) }()
	defer func() {
		cancel()
		<-done
	}()

	reconcileWaitFor(t, 2*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return reconcileContains(attempted, reconcileTestSenderA) &&
			reconcileContains(attempted, reconcileTestSenderB)
	})
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
	if listCalls.Load() < 2 {
		t.Fatalf("scope list calls = %d, want >= 2 (the failed list must not stop the loop)", listCalls.Load())
	}
}

// TestReconcileLoopNonPositiveIntervalStops proves a non-positive interval
// disables the loop instead of panicking in time.NewTicker (config refuses the
// value, so this is a defensive guard only).
func TestReconcileLoopNonPositiveIntervalStops(t *testing.T) {
	var listed, reconciled atomic.Int64
	l := &ReconcileLoop{
		interval:   0,
		logger:     discardLog(),
		listScopes: func(context.Context) ([]string, error) { listed.Add(1); return nil, nil },
		reconcile:  func(context.Context, string) error { reconciled.Add(1); return nil },
	}

	done := make(chan struct{})
	go func() { defer close(done); l.Run(context.Background()) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return for a non-positive interval")
	}
	if listed.Load() != 0 || reconciled.Load() != 0 {
		t.Fatalf("disabled loop did work: list=%d reconcile=%d", listed.Load(), reconciled.Load())
	}
}

func discardLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func reconcileWaitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}

func reconcileContains(values []string, want string) bool {
	for _, v := range values {
		if v == want {
			return true
		}
	}
	return false
}
