// coordinator.go implements the 003 event-indexing coordinator (research R1):
// one lease acquisition loop and one heartbeat drive both the header scanner
// and the log scanner concurrently. The two ServeLoop bodies perform no
// acquisition and no renewal; they only consult the coordinator's checkLost
// before any write, so a process never competes with itself for the shared
// indexer_lease row.
package indexer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/xtianxx/txharbor/internal/logx"
)

// ServeFunc is one scanner loop as driven by RunPair. It returns nil on
// context cancellation, ErrLeaseLost when the lease is gone, and a stop error
// (durable pause, configuration refusal, non-retryable failure) otherwise.
type ServeFunc func(ctx context.Context, checkLost func() error) error

// pairPollInterval is how long a bystander waits before re-checking the lease
// (busy row or failed acquisition). It matches 002's default poll cadence; the
// coordinator intentionally takes no scanner configuration (research R1).
const pairPollInterval = time.Second

// leaseSession is the coordination surface RunPair drives; *Lease implements
// it. Tests substitute a scripted fake.
type leaseSession interface {
	Acquire(ctx context.Context) (bool, int64, error)
	Heartbeat(ctx context.Context) error
}

// RunPair is the only lease acquisition loop in the two-stream service. On a
// win it starts the single heartbeat and runs both serve loops concurrently
// under it. ErrLeaseLost and heartbeat loss cancel both loops and return to
// acquisition; any other error stops both loops and is returned, so the caller
// exits non-zero exactly like a 002 scanner failure. A cancelled ctx exits
// cleanly with nil.
func RunPair(ctx context.Context, lease leaseSession, headerServe, logServe ServeFunc) error {
	if lease == nil {
		return errors.New("indexer coordinator: nil lease")
	}
	if headerServe == nil || logServe == nil {
		return errors.New("indexer coordinator: both serve functions are required")
	}
	for {
		if ctx.Err() != nil {
			return nil
		}
		won, token, err := lease.Acquire(ctx)
		switch {
		case err != nil:
			if ctx.Err() != nil {
				return nil
			}
			slog.Warn("lease acquire failed; retrying",
				"error", logx.Redact(err.Error()))
			if !waitCtx(ctx, pairPollInterval) {
				return nil
			}
			continue
		case !won:
			slog.Debug("lease held by another instance; standing by")
			if !waitCtx(ctx, pairPollInterval) {
				return nil
			}
			continue
		}

		slog.Info("lease acquired", "fencing_token", token)
		err = servePair(ctx, lease, headerServe, logServe)
		if ctx.Err() != nil {
			return nil
		}
		if err == nil {
			return nil
		}
		if errors.Is(err, ErrLeaseLost) {
			slog.Warn("lease lost; re-acquiring", "error", logx.Redact(err.Error()))
			continue
		}
		return err
	}
}

// servePair runs both loops under one heartbeat. It returns nil when the loops
// stopped because ctx was cancelled, ErrLeaseLost when the heartbeat or a loop
// reported the lease gone, and the first stop error otherwise.
func servePair(ctx context.Context, lease leaseSession, headerServe, logServe ServeFunc) error {
	pairCtx, cancelPair := context.WithCancel(ctx)
	defer cancelPair()
	hbCtx, cancelHB := context.WithCancel(pairCtx)
	defer cancelHB()

	var (
		mu    sync.Mutex
		hbErr error
	)
	go func() {
		err := lease.Heartbeat(hbCtx)
		if err == nil {
			return // stopped because the pair or the caller is done
		}
		mu.Lock()
		hbErr = err
		mu.Unlock()
		cancelPair() // unconfirmed lease: stop both loops before any write
	}()

	checkLost := func() error {
		mu.Lock()
		defer mu.Unlock()
		if hbErr == nil {
			return nil
		}
		return fmt.Errorf("%w: %v", ErrLeaseLost, hbErr)
	}

	errs := make(chan error, 2)
	go func() { errs <- headerServe(pairCtx, checkLost) }()
	go func() { errs <- logServe(pairCtx, checkLost) }()

	var (
		firstErr error
		lost     bool
	)
	for i := 0; i < 2; i++ {
		err := <-errs
		switch {
		case err == nil:
		case errors.Is(err, ErrLeaseLost):
			lost = true
			cancelPair()
		default:
			if firstErr == nil {
				firstErr = err
			}
			cancelPair()
		}
	}

	mu.Lock()
	heartbeatErr := hbErr
	mu.Unlock()
	switch {
	case heartbeatErr != nil:
		return fmt.Errorf("%w: %v", ErrLeaseLost, heartbeatErr)
	case lost:
		return ErrLeaseLost
	default:
		return firstErr
	}
}

// waitCtx sleeps for d and reports false when ctx is done, so shutdown aborts
// the standby wait immediately.
func waitCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
