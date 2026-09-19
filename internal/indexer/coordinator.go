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
	return runStreams(ctx, lease, []ServeFunc{headerServe, logServe})
}

// RunTrio is the three-stream service (004 deposit detection): one lease
// acquisition loop and one heartbeat drive the header, log and deposit serve
// loops concurrently. Loss and terminal polarities match RunPair; the deposit
// loop joins the same cancellation fan-out, so any loop's stop error or lease
// loss stops all three before any further write.
func RunTrio(ctx context.Context, lease leaseSession, headerServe, logServe, depositServe ServeFunc) error {
	return runStreams(ctx, lease, []ServeFunc{headerServe, logServe, depositServe})
}

// RunQuatro is the four-stream service (005 confirmation tracking): one
// lease acquisition loop and one heartbeat drive the header, log, deposit
// and confirmation serve loops concurrently. Loss and terminal polarities
// match RunPair; the confirmation loop joins the same cancellation fan-out,
// so any loop's stop error or lease loss stops all four before any further
// write.
func RunQuatro(ctx context.Context, lease leaseSession, headerServe, logServe, depositServe, confirmServe ServeFunc) error {
	return runStreams(ctx, lease, []ServeFunc{headerServe, logServe, depositServe, confirmServe})
}

// RunQuatroPlusRecovery is the five-stream service (006 reorg recovery):
// the four ordinary loops plus the recovery executor under the same single
// lease acquisition loop and heartbeat (T017). It rides runStreams
// unchanged — no new lock order, no second coordination primitive; lease.go
// needs zero changes. Startup/loop/health readers observe the recovery row
// via LoadRecoveryState the same way they read pause rows today.
func RunQuatroPlusRecovery(ctx context.Context, lease leaseSession, headerServe, logServe, depositServe, confirmServe, recoveryServe ServeFunc) error {
	return runStreams(ctx, lease, []ServeFunc{headerServe, logServe, depositServe, confirmServe, recoveryServe})
}

// runStreams is the shared acquisition loop behind RunPair/RunTrio/RunQuatro.
func runStreams(ctx context.Context, lease leaseSession, serves []ServeFunc) error {
	if lease == nil {
		return errors.New("indexer coordinator: nil lease")
	}
	for _, serve := range serves {
		if serve == nil {
			return errors.New("indexer coordinator: all serve functions are required")
		}
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
		err = serveStreams(ctx, lease, serves)
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

// serveStreams runs every loop under one heartbeat. It returns nil when the loops
// stopped because ctx was cancelled, ErrLeaseLost when the heartbeat or a loop
// reported the lease gone, and the first fatal stop error otherwise.
//
// A durable business pause (a persisted pause row that halted a loop) is NOT a
// terminal failure: the paused loop stays stopped (no retry, no automatic
// unpause) while the remaining loops keep running. If every loop has stopped
// and only pauses account for it, the coordinator stays resident with the
// heartbeat until ctx ends or the lease is lost, so the HTTP/health/observation
// surface and the 006 recovery participant remain reachable (Lane-F5).
func serveStreams(ctx context.Context, lease leaseSession, serves []ServeFunc) error {
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
		cancelPair() // unconfirmed lease: stop all loops before any write
	}()

	checkLost := func() error {
		mu.Lock()
		defer mu.Unlock()
		if hbErr == nil {
			return nil
		}
		return fmt.Errorf("%w: %v", ErrLeaseLost, hbErr)
	}

	errs := make(chan error, len(serves))
	for _, serve := range serves {
		go func() { errs <- serve(pairCtx, checkLost) }()
	}

	var (
		firstErr error
		lost     bool
		paused   int
	)
	for range serves {
		err := <-errs
		switch {
		case err == nil:
		case errors.Is(err, ErrLeaseLost):
			lost = true
			cancelPair()
		case isPauseStop(err):
			// Durable business pause: halt this loop without cancelling its
			// siblings and without treating the stop as terminal. The pause
			// row, the scanner state and the audit/metric surface are already
			// recorded by the loop that stopped.
			paused++
			slog.Warn("indexer stream halted on a durable pause; service stays up",
				"error", logx.Redact(err.Error()))
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
	case firstErr != nil:
		return firstErr
	case paused > 0:
		// Every loop has stopped, and at least one stopped on a durable
		// business pause with no fatal error: stay resident (heartbeat keeps
		// the lease, zero writes, zero retries) until shutdown or lease loss.
		<-pairCtx.Done()
		mu.Lock()
		heartbeatErr = hbErr
		mu.Unlock()
		if heartbeatErr != nil {
			return fmt.Errorf("%w: %v", ErrLeaseLost, heartbeatErr)
		}
		return nil
	default:
		return nil
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
