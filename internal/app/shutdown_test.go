package app

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestRunShutdownOrderedSteps covers FR-014: stop accepting work first, then
// release resources, all inside the shared budget.
func TestRunShutdownOrderedSteps(t *testing.T) {
	var mu sync.Mutex
	var order []string
	record := func(name string) func(context.Context) error {
		return func(context.Context) error {
			mu.Lock()
			defer mu.Unlock()
			order = append(order, name)
			return nil
		}
	}

	err := runShutdown(context.Background(), time.Second,
		record("stop-accepting"),
		record("close-rpc"),
		record("close-pool"),
	)
	if err != nil {
		t.Fatalf("runShutdown() error = %v", err)
	}
	want := []string{"stop-accepting", "close-rpc", "close-pool"}
	if len(order) != len(want) || order[0] != want[0] || order[1] != want[1] || order[2] != want[2] {
		t.Fatalf("step order = %v, want %v", order, want)
	}
}

// TestRunShutdownBudgetExceededStillReleases covers the timeout branch: the
// hanging step is cut off, later release steps still run, and the error is
// recorded for a non-zero exit.
func TestRunShutdownBudgetExceededStillReleases(t *testing.T) {
	var released bool
	start := time.Now()
	err := runShutdown(context.Background(), 30*time.Millisecond,
		func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		},
		func(context.Context) error {
			released = true
			return nil
		},
	)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("runShutdown() expected budget error")
	}
	if !strings.Contains(err.Error(), "shutdown budget") {
		t.Fatalf("error %q does not record the budget overrun", err)
	}
	if !released {
		t.Fatal("release step did not run after budget expiry")
	}
	if elapsed < 30*time.Millisecond || elapsed > time.Second {
		t.Fatalf("shutdown took %s, want ~30ms", elapsed)
	}
}

func TestRunShutdownStepFailurePropagates(t *testing.T) {
	sentinel := errors.New("close failed")
	err := runShutdown(context.Background(), time.Second,
		func(context.Context) error { return nil },
		func(context.Context) error { return sentinel },
	)
	if err == nil || !errors.Is(err, sentinel) {
		t.Fatalf("runShutdown() error = %v, want wrapped %v", err, sentinel)
	}
}
