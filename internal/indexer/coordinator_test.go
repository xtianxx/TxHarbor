package indexer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeLease is a scripted leaseSession: every Acquire wins and the heartbeat
// failure is one-shot so later rounds stay healthy until cancelled.
type fakeLease struct {
	mu       sync.Mutex
	acquires int
	hbErr    error
}

func (f *fakeLease) Acquire(context.Context) (bool, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acquires++
	return true, int64(f.acquires), nil
}

func (f *fakeLease) Heartbeat(ctx context.Context) error {
	f.mu.Lock()
	err := f.hbErr
	f.hbErr = nil
	f.mu.Unlock()
	if err != nil {
		return err
	}
	<-ctx.Done()
	return nil
}

func (f *fakeLease) failHeartbeat(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hbErr = err
}

func (f *fakeLease) acquisitions() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.acquires
}

func waitFor(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// TestRunPairLeaseLostStopsPeerAndReacquires covers the loss polarity: one loop
// returning ErrLeaseLost cancels the peer before the coordinator re-acquires,
// and the next round runs normally until shutdown.
func TestRunPairLeaseLostStopsPeerAndReacquires(t *testing.T) {
	lease := &fakeLease{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		headerCalls atomic.Int32
		logCalls    atomic.Int32
		peerStopped atomic.Bool
	)
	logStarted := make(chan struct{})
	reacquired := make(chan struct{})
	var reacquireOnce sync.Once

	headerServe := func(c context.Context, checkLost func() error) error {
		if headerCalls.Add(1) == 1 {
			<-logStarted // both loops are live before the loss
			return fmt.Errorf("%w: write verdict", ErrLeaseLost)
		}
		reacquireOnce.Do(func() { close(reacquired) })
		<-c.Done()
		return nil
	}
	logServe := func(c context.Context, checkLost func() error) error {
		if logCalls.Add(1) == 1 {
			close(logStarted)
			<-c.Done()
			peerStopped.Store(true)
			return nil
		}
		<-c.Done()
		return nil
	}

	done := make(chan error, 1)
	go func() { done <- RunPair(ctx, lease, headerServe, logServe) }()

	waitFor(t, reacquired, "re-acquisition after ErrLeaseLost")
	if !peerStopped.Load() {
		t.Fatal("peer loop was not cancelled before re-acquisition")
	}
	if got := lease.acquisitions(); got != 2 {
		t.Fatalf("acquisitions = %d, want 2", got)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunPair() = %v, want nil on shutdown", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RunPair did not stop on context cancellation")
	}
}

// TestRunPairHeartbeatLossStopsBothAndReacquires covers the heartbeat polarity:
// a failed renewal is reported by checkLost to both loops, both stop, and the
// coordinator re-acquires for a fresh round.
func TestRunPairHeartbeatLossStopsBothAndReacquires(t *testing.T) {
	lease := &fakeLease{}
	lease.failHeartbeat(errors.New("renew failed"))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		headerCalls atomic.Int32
		lossSeen    atomic.Int32
	)
	reacquired := make(chan struct{})
	var reacquireOnce sync.Once

	run := func(c context.Context, checkLost func() error) error {
		for {
			if err := checkLost(); err != nil {
				if !errors.Is(err, ErrLeaseLost) {
					return err
				}
				lossSeen.Add(1)
				return err
			}
			select {
			case <-c.Done():
				// The heartbeat error is stored before the pair is
				// cancelled, so the re-check observes it deterministically.
				if err := checkLost(); err != nil {
					if !errors.Is(err, ErrLeaseLost) {
						return err
					}
					lossSeen.Add(1)
				}
				return nil
			case <-time.After(time.Millisecond):
			}
		}
	}
	headerServe := func(c context.Context, checkLost func() error) error {
		if headerCalls.Add(1) >= 2 {
			reacquireOnce.Do(func() { close(reacquired) })
		}
		return run(c, checkLost)
	}
	logServe := func(c context.Context, checkLost func() error) error {
		return run(c, checkLost)
	}

	done := make(chan error, 1)
	go func() { done <- RunPair(ctx, lease, headerServe, logServe) }()

	waitFor(t, reacquired, "re-acquisition after heartbeat loss")
	if got := lossSeen.Load(); got != 2 {
		t.Fatalf("loops reporting lease loss = %d, want 2 (both stopped)", got)
	}
	if got := lease.acquisitions(); got != 2 {
		t.Fatalf("acquisitions = %d, want 2", got)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunPair() = %v, want nil on shutdown", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RunPair did not stop on context cancellation")
	}
}

// TestRunTrioRunsAllThreeLoops covers the 004 wiring: the deposit loop joins
// the same acquisition round and heartbeat as header/log, and a clean
// shutdown joins all three with nil.
func TestRunTrioRunsAllThreeLoops(t *testing.T) {
	lease := &fakeLease{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var calls [3]atomic.Int32
	mkServe := func(i int) ServeFunc {
		return func(c context.Context, checkLost func() error) error {
			calls[i].Add(1)
			<-c.Done()
			return nil
		}
	}

	done := make(chan error, 1)
	go func() { done <- RunTrio(ctx, lease, mkServe(0), mkServe(1), mkServe(2)) }()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if calls[0].Load() == 1 && calls[1].Load() == 1 && calls[2].Load() == 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	for i := range calls {
		if calls[i].Load() != 1 {
			t.Fatalf("loop %d calls = %d, want exactly 1 in the shared round", i, calls[i].Load())
		}
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunTrio() = %v, want nil on shutdown", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RunTrio did not stop on context cancellation")
	}
	if got := lease.acquisitions(); got != 1 {
		t.Fatalf("acquisitions = %d, want 1", got)
	}
}

// TestRunTrioDepositStopErrorStopsPeers covers the terminal polarity with
// three loops: the deposit loop's stop error cancels header/log and is
// returned as-is without re-acquisition.
func TestRunTrioDepositStopErrorStopsPeers(t *testing.T) {
	lease := &fakeLease{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var peersStopped [2]atomic.Bool
	started := make(chan struct{})
	var startOnce sync.Once
	headerServe := func(c context.Context, checkLost func() error) error {
		startOnce.Do(func() { close(started) })
		<-c.Done()
		peersStopped[0].Store(true)
		return nil
	}
	logServe := func(c context.Context, checkLost func() error) error {
		<-c.Done()
		peersStopped[1].Store(true)
		return nil
	}
	depositServe := func(c context.Context, checkLost func() error) error {
		<-started
		return fmt.Errorf("deposit config changed: %w", errStaleState)
	}

	done := make(chan error, 1)
	go func() { done <- RunTrio(ctx, lease, headerServe, logServe, depositServe) }()

	select {
	case err := <-done:
		if !errors.Is(err, errStaleState) {
			t.Fatalf("RunTrio() = %v, want the deposit stop error", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RunTrio did not return the deposit stop error")
	}
	for i := range peersStopped {
		if !peersStopped[i].Load() {
			t.Fatalf("peer loop %d was not stopped by the deposit terminal error", i)
		}
	}
	if got := lease.acquisitions(); got != 1 {
		t.Fatalf("acquisitions = %d, want 1 (no retry after a stop error)", got)
	}
}

// TestRunTrioLeaseLossReacquiresThreeLoops covers the loss polarity with
// three loops: a peer's ErrLeaseLost cancels the round and the next round
// runs all three loops again.
func TestRunTrioLeaseLossReacquiresThreeLoops(t *testing.T) {
	lease := &fakeLease{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var calls [3]atomic.Int32
	reacquired := make(chan struct{})
	var reacquireOnce sync.Once
	mkServe := func(i int, lose bool) ServeFunc {
		return func(c context.Context, checkLost func() error) error {
			if calls[i].Add(1) == 1 {
				if lose {
					return fmt.Errorf("%w: write verdict", ErrLeaseLost)
				}
				<-c.Done()
				return nil
			}
			if calls[0].Load() >= 2 && calls[1].Load() >= 2 && calls[2].Load() >= 2 {
				reacquireOnce.Do(func() { close(reacquired) })
			}
			<-c.Done()
			return nil
		}
	}

	done := make(chan error, 1)
	go func() {
		done <- RunTrio(ctx, lease, mkServe(0, false), mkServe(1, true), mkServe(2, false))
	}()

	waitFor(t, reacquired, "re-acquisition of all three loops after ErrLeaseLost")
	if got := lease.acquisitions(); got != 2 {
		t.Fatalf("acquisitions = %d, want 2", got)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunTrio() = %v, want nil on shutdown", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RunTrio did not stop on context cancellation")
	}
}
func TestRunPairStopErrorStopsBothWithoutReacquire(t *testing.T) {
	lease := &fakeLease{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var peerStopped atomic.Bool
	logStarted := make(chan struct{})
	headerServe := func(c context.Context, checkLost func() error) error {
		<-logStarted
		return fmt.Errorf("chain-level pause: %w", errPaused)
	}
	logServe := func(c context.Context, checkLost func() error) error {
		close(logStarted)
		<-c.Done()
		peerStopped.Store(true)
		return nil
	}

	err := RunPair(ctx, lease, headerServe, logServe)
	if !errors.Is(err, errPaused) {
		t.Fatalf("RunPair() = %v, want errPaused", err)
	}
	if !peerStopped.Load() {
		t.Fatal("peer loop was not stopped by the terminal error")
	}
	if got := lease.acquisitions(); got != 1 {
		t.Fatalf("acquisitions = %d, want 1 (no retry after a stop error)", got)
	}
}
