//go:build linux && drill

// borrowed-replacement-autonomous-observation_linux_test.go is the bounded
// autonomous observation during the fenced read-only window lane. It COMPOSES
// the closed observation-window hooks (never duplicating their transaction or
// observation logic and adding no DDL authority): the genuine replacement-bound
// capture and P1 registration, the parked Use, the complete observer readiness,
// the owner-fenced SHARE window and the synchronized terminal decision all come
// from borrowedReplacementObservationWindowRun. This lane adds ONLY a
// new-file-local autonomous checkpoint pump: started from the Plan.preRelease
// hook after the Use parks and before the owner window, it ALONE schedules
// repeated real requestCheckpoint calls serialized on the independently owned
// observer connection; its FIRST successful ack is awaited before the owner
// window is entered, and at least TWO newly completed autonomous samples are
// awaited inside the SHARE-held Plan.postProbe park WITHOUT requesting
// checkpoints from that hook. The pump keeps running through the existing final
// inspection/ack and the synchronized terminal decision; any pre-seal pump
// failure or unexpected termination is classified through the shared
// pre-seal-loss latch and cancels the EXTERNAL lane context (not merely the
// Use). After terminalization the pump is canceled and bounded-joined, the
// observer/Use join discipline is retained, and the session is retired only
// after ALL completion proofs with the guard unresolved and acceptance zero.
// Every spawn has immediate cancel-and-bounded-join cleanup, join timeouts are
// explicit UNKNOWN results that block retirement/reuse, worker results are only
// read after proven completion, and the owner callback never calls
// prefix/anchor/Health and never uses the retained P1 connection. There is no
// continuous exclusion, no atomic acceptance, no guard admission, no restore,
// no probe beyond the session's single fixed SELECT 1, no receipt authority, no
// clean transition, no acceptance, no manifest, no downstream and no Gate1
// authority. Secrets, verifiers and DSNs are never logged.
package recovery_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// borrowedReplacementAutonomousPump is the ONE new-file-local autonomous
// checkpoint pump: it schedules repeated real checkpoints on the independently
// owned observer connection, records a monotonic completed-sample sequence and
// classifies every pre-seal failure through the shared loss latch while
// canceling the external lane context.
type borrowedReplacementAutonomousPump struct {
	observer   *borrowedReplacementObservationWindowObserver
	state      *borrowedReplacementObservationWindowState
	cancelLane func()

	mu        sync.Mutex
	completed int64
	lastErr   error

	// checkpointCompleted/checkpointResult preserve the RAW returned result of
	// the active checkpoint, INCLUDING nil; checkpointViolation keeps any
	// synthesized protocol-violation error SEPARATE from the raw ack.
	checkpointCompleted bool
	checkpointResult    error
	checkpointViolation error

	entered            chan struct{}
	firstAck           chan struct{}
	observationEntered chan struct{}
	lossAcked          chan struct{}
	done               chan struct{}
	cancel             context.CancelFunc
	closeOnce          sync.Once

	enteredOnce     sync.Once
	firstAckOnce    sync.Once
	observationOnce sync.Once
	lossAckedOnce   sync.Once
	stallFn         func(context.Context) error
}

func startBorrowedReplacementAutonomousPump(t *testing.T, laneCtx context.Context, observer *borrowedReplacementObservationWindowObserver, cancelLane func(), observeOverride func(context.Context, func(context.Context) error) error) *borrowedReplacementAutonomousPump {
	t.Helper()
	pumpCtx, cancel := context.WithCancel(laneCtx)
	pump := &borrowedReplacementAutonomousPump{
		observer: observer, state: observer.state, cancelLane: cancelLane,
		entered: make(chan struct{}), firstAck: make(chan struct{}),
		observationEntered: make(chan struct{}), lossAcked: make(chan struct{}),
		done: make(chan struct{}), cancel: cancel,
	}
	// The observation wrapper (and any control override) is installed BEFORE
	// the pump goroutine starts, so the observeFn seam is never written
	// concurrently with a reader.
	observer.observeFn = func(observeCtx context.Context) error {
		pump.observationOnce.Do(func() { close(pump.observationEntered) })
		real := func(realCtx context.Context) error {
			return borrowedReplacementObservationWindowObserve(realCtx, observer.fixture, observer.fresh, observer.reg, observer.conn)
		}
		if observeOverride != nil {
			return observeOverride(observeCtx, real)
		}
		return real(observeCtx)
	}
	go func() {
		defer close(pump.done)
		pump.enteredOnce.Do(func() { close(pump.entered) })
		for {
			if err := pumpCtx.Err(); err != nil {
				// Expected post-seal shutdown preserves the terminal result; an
				// unexpected pre-seal termination is classified and cancels the
				// external lane context.
				pump.fail("autonomous pump terminated before the interval seal")
				return
			}
			if stallFn := pump.stallFnNow(); stallFn != nil {
				if err := stallFn(pumpCtx); err != nil {
					pump.fail("autonomous pump stall hook refused: " + err.Error())
					return
				}
			}
			checkCtx, cancelCheck := context.WithTimeout(pumpCtx, 60*time.Second)
			err := pump.observer.requestCheckpoint(checkCtx)
			canceled := pumpCtx.Err() != nil || checkCtx.Err() != nil
			cancelCheck()
			// Preserve the RAW returned result INCLUDING nil with a completion
			// flag BEFORE any classification.
			pump.recordCheckpointResult(err)
			if err != nil {
				pump.fail("autonomous pump checkpoint refused: " + err.Error())
				return
			}
			if canceled {
				// A canceled checkpoint that returned nil success is a
				// PROTOCOL VIOLATION: it must not count and must fail closed,
				// and the synthesized error stays SEPARATE from the raw ack.
				violation := errors.New("autonomous pump checkpoint returned nil success after cancellation")
				pump.recordCheckpointProtocolViolation(violation)
				pump.fail(violation.Error())
				return
			}
			pump.mu.Lock()
			pump.completed++
			pump.mu.Unlock()
			pump.firstAckOnce.Do(func() { close(pump.firstAck) })
		}
	}()
	t.Cleanup(func() {
		if err := pump.stopBounded(30 * time.Second); err != nil {
			t.Errorf("%v", err)
		}
	})
	return pump
}

// setStallFn installs the test-only scheduler stall after the first ack (the
// pump loop reads it under the same mutex).
func (p *borrowedReplacementAutonomousPump) setStallFn(stallFn func(context.Context) error) {
	p.mu.Lock()
	p.stallFn = stallFn
	p.mu.Unlock()
}

func (p *borrowedReplacementAutonomousPump) stallFnNow() func(context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stallFn
}

func (p *borrowedReplacementAutonomousPump) completedNow() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.completed
}

func (p *borrowedReplacementAutonomousPump) lastErrNow() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lastErr
}

func (p *borrowedReplacementAutonomousPump) lossReasonNow() string {
	return p.state.lossReasonNow()
}

// fail records the pump failure and classifies it through the shared
// mutex-protected pre-seal latch: a pre-seal failure cancels the external lane
// context; a post-seal failure preserves the terminal result.
func (p *borrowedReplacementAutonomousPump) fail(reason string) {
	p.mu.Lock()
	if p.lastErr == nil {
		p.lastErr = errors.New(reason)
	}
	p.mu.Unlock()
	latched := p.state.latchPreSealLoss(reason)
	p.ackLoss()
	if latched && p.cancelLane != nil {
		p.cancelLane()
	}
}

// ackLoss closes the loss acknowledgement exactly once.
func (p *borrowedReplacementAutonomousPump) ackLoss() {
	p.lossAckedOnce.Do(func() { close(p.lossAcked) })
}

// recordCheckpointResult preserves the RAW returned checkpoint result of the
// ACTIVE (latest completed) request, INCLUDING nil, with a completion flag; a
// synthesized error is never substituted for the raw acknowledgement.
func (p *borrowedReplacementAutonomousPump) recordCheckpointResult(err error) {
	p.mu.Lock()
	p.checkpointCompleted = true
	p.checkpointResult = err
	p.mu.Unlock()
}

// recordCheckpointProtocolViolation keeps a synthesized protocol-violation
// error SEPARATE from the raw acknowledgement.
func (p *borrowedReplacementAutonomousPump) recordCheckpointProtocolViolation(err error) {
	p.mu.Lock()
	if p.checkpointViolation == nil {
		p.checkpointViolation = err
	}
	p.mu.Unlock()
}

// checkpointResultNow returns the RAW checkpoint result and its completion flag.
func (p *borrowedReplacementAutonomousPump) checkpointResultNow() (error, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.checkpointResult, p.checkpointCompleted
}

func (p *borrowedReplacementAutonomousPump) checkpointProtocolViolationNow() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.checkpointViolation
}

// terminateForControl cancels the pump's internal context only (an unexpected
// pre-seal termination simulation).
func (p *borrowedReplacementAutonomousPump) terminateForControl() {
	p.cancel()
}

// stopBounded cancels and bounded-joins the pump exactly once; a join timeout
// is an explicit UNKNOWN termination result.
func (p *borrowedReplacementAutonomousPump) stopBounded(bound time.Duration) error {
	p.closeOnce.Do(func() { p.cancel() })
	select {
	case <-p.done:
		return nil
	case <-time.After(bound):
		return errors.New("autonomous pump termination is UNKNOWN after the bounded join")
	}
}

// borrowedReplacementAutonomousOutcome preserves the flow outcome, the pump and
// the explicit pump join result.
type borrowedReplacementAutonomousOutcome struct {
	flow          *borrowedReplacementObservationWindowOutcome
	runErr        error
	pump          *borrowedReplacementAutonomousPump
	pumpJoinErr   error
	samplesAtPark int64
}

// borrowedReplacementAutonomousAwaitSamples waits (bounded) for count NEW
// completed autonomous samples. The REAL progress-timeout path routes through
// the shared pre-seal classification and the external lane cancellation, so a
// stalled scheduler leaves a permanent UNKNOWN loss; an already-observed loss
// or pump termination refuses first.
func borrowedReplacementAutonomousAwaitSamples(postProbeCtx context.Context, pump *borrowedReplacementAutonomousPump, cancelLane func(), count int64, progressBound time.Duration) error {
	baseline := pump.completedNow()
	waitCtx, cancelWait := context.WithTimeout(postProbeCtx, progressBound)
	defer cancelWait()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for pump.completedNow() < baseline+count {
		if reason := pump.lossReasonNow(); reason != "" {
			return errors.New("autonomous loss detected while awaiting samples: " + reason)
		}
		select {
		case <-ticker.C:
		case <-pump.done:
			return errors.New("autonomous pump terminated while awaiting samples")
		case <-waitCtx.Done():
			// REAL progress timeout: shared pre-seal classification + external
			// lane cancellation; the UNKNOWN is permanent.
			latched := pump.state.latchPreSealLoss("autonomous observation/scheduler progress stalled")
			pump.ackLoss()
			if latched && cancelLane != nil {
				cancelLane()
			}
			return errors.New("autonomous observation/scheduler progress stalled: bounded UNKNOWN")
		}
	}
	return nil
}

// borrowedReplacementAutonomousRun composes the closed observation-window flow
// with the autonomous pump: the preRelease hook starts the pump and awaits its
// first successful ack before the owner window, the postProbe hook delegates to
// the caller, and after the flow returns the pump is canceled and
// bounded-joined with an explicit UNKNOWN join result.
func borrowedReplacementAutonomousRun(ctx context.Context, t *testing.T, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding, postProbe func(context.Context, *borrowedReplacementAutonomousPump, func()) error, observeOverride func(context.Context, func(context.Context) error) error, pumpJoinBound time.Duration) *borrowedReplacementAutonomousOutcome {
	t.Helper()
	laneCtx, cancelLane := context.WithCancel(ctx)
	defer cancelLane()
	outcome := &borrowedReplacementAutonomousOutcome{}
	plan := &borrowedReplacementObservationWindowPlan{
		preRelease: func(preCtx context.Context, observer *borrowedReplacementObservationWindowObserver) error {
			outcome.pump = startBorrowedReplacementAutonomousPump(t, laneCtx, observer, cancelLane, observeOverride)
			select {
			case <-outcome.pump.firstAck:
			case <-outcome.pump.done:
				return errors.New("autonomous pump terminated before its first ack")
			case <-preCtx.Done():
				return errors.New("autonomous pump first ack did not arrive before the owner window")
			}
			return nil
		},
		postProbe: func(hookCtx context.Context) error {
			if outcome.pump == nil {
				return errors.New("autonomous pump was not started")
			}
			outcome.samplesAtPark = outcome.pump.completedNow()
			return postProbe(hookCtx, outcome.pump, cancelLane)
		},
	}
	outcome.flow, outcome.runErr = borrowedReplacementObservationWindowRun(laneCtx, t, b, fresh, plan)
	// The lane is canceled after the terminalized flow so the pump exits; a
	// post-seal pump failure preserves the terminal result.
	cancelLane()
	if outcome.pump != nil {
		outcome.pumpJoinErr = outcome.pump.stopBounded(pumpJoinBound)
	}
	return outcome
}

// borrowedReplacementAutonomousRetireGuarded retires the session only after ALL
// completion proofs; an UNKNOWN pump join blocks retirement/reuse.
func borrowedReplacementAutonomousRetireGuarded(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, outcome *borrowedReplacementAutonomousOutcome) error {
	t.Helper()
	if outcome == nil || outcome.flow == nil {
		return errors.New("autonomous retirement refused: no flow outcome")
	}
	if outcome.pumpJoinErr != nil {
		return errors.New("autonomous retirement refused: pump termination is UNKNOWN")
	}
	if outcome.flow.terminationUnknown {
		return errors.New("autonomous retirement refused: flow termination is UNKNOWN")
	}
	borrowedReplacementSessionRetire(t, ctx, b, outcome.flow.conn, outcome.flow.reg.state)
	return nil
}

// borrowedReplacementAutonomousPostProbeSamples is the positive post-probe
// hook: at least TWO newly completed autonomous samples inside the park,
// without requesting checkpoints from the hook.
func borrowedReplacementAutonomousPostProbeSamples(postProbeCtx context.Context, pump *borrowedReplacementAutonomousPump, cancelLane func()) error {
	if err := borrowedReplacementAutonomousAwaitSamples(postProbeCtx, pump, cancelLane, 2, 60*time.Second); err != nil {
		return err
	}
	if pump.completedNow() < 2 {
		return errors.New("autonomous pump completed fewer than two samples inside the park")
	}
	return nil
}

// borrowedReplacementAutonomousBaseline creates the genuine baseline and the
// replacement-bound capture for one self-contained negative.
func borrowedReplacementAutonomousBaseline(t *testing.T, ctx context.Context) (*borrowedSuccessorBaseline, *borrowedReplacementBinding) {
	t.Helper()
	b := newBorrowedSuccessorBaseline(t, ctx)
	var stages int32
	orchestrateCtx, cancelOrchestrate := borrowedReplacementOrchestrationContext(ctx)
	fresh, err := borrowedReplacementBindingOrchestrate(orchestrateCtx, t, b, &stages)
	cancelOrchestrate()
	if err != nil || fresh == nil {
		t.Fatalf("autonomous observation genuine capture refused: capture=%v err=%v", fresh, err)
	}
	if got := atomic.LoadInt32(&stages); got != borrowedReplacementStagePrefixCaptured {
		t.Fatalf("autonomous observation pipeline stages=%d, want %d", got, borrowedReplacementStagePrefixCaptured)
	}
	if fresh.binding.TargetDatabaseOID() != fresh.replacementOID || fresh.replacementOID == fresh.oldOID {
		t.Fatalf("autonomous observation capture is not replacement-bound: replacement=%d old=%d binding=%d", fresh.replacementOID, fresh.oldOID, fresh.binding.TargetDatabaseOID())
	}
	return b, fresh
}

// borrowedReplacementAutonomousRegisteredLossRefuse: terminate the registered
// P1 while the owner hook is parked -> autonomous detection, permanent loss,
// owner/Use refusal after executions=1, no composite.
func borrowedReplacementAutonomousRegisteredLossRefuse(t *testing.T, ctx context.Context) {
	t.Helper()
	b, fresh := borrowedReplacementAutonomousBaseline(t, ctx)
	outcome := borrowedReplacementAutonomousRun(ctx, t, b, fresh, func(hookCtx context.Context, pump *borrowedReplacementAutonomousPump, cancelLane func()) error {
		if err := borrowedReplacementAutonomousAwaitSamples(hookCtx, pump, cancelLane, 2, 60*time.Second); err != nil {
			return err
		}
		var terminated bool
		termCtx, cancelTerm := context.WithTimeout(hookCtx, borrowedOwnerRotationQueryBudget)
		termErr := b.fixture.controlPool.QueryRow(termCtx, `SELECT pg_terminate_backend($1)`, pump.observer.reg.state.backendPID).Scan(&terminated)
		cancelTerm()
		if termErr != nil || !terminated {
			t.Fatalf("autonomous registered-P1 termination refused: terminated=%t err=%v", terminated, termErr)
		}
		borrowedAuthStagingAwaitDisappearance(hookCtx, t, b.fixture.fx.containerID, pump.observer.reg.state.backendPID, pump.observer.reg.state.osStart, borrowedAuthStagingDisappearanceBudget, true)
		select {
		case <-pump.lossAcked:
		case <-hookCtx.Done():
		}
		if pump.lossReasonNow() == "" {
			t.Fatalf("autonomous registered-P1 loss was not acknowledged before the hook context ended")
		}
		return errors.New("autonomous detection of the registered P1 loss")
	}, nil, 30*time.Second)
	if outcome.pump == nil || outcome.pump.lossReasonNow() == "" {
		t.Fatal("autonomous registered-P1 termination did not permanently latch the loss")
	}
	if outcome.flow.ownerErr == nil || outcome.flow.useErr == nil {
		t.Fatalf("autonomous registered-P1 termination did not refuse owner/use: owner=%v use=%v", outcome.flow.ownerErr, outcome.flow.useErr)
	}
	if outcome.flow.probeExecutions != 1 {
		t.Fatalf("autonomous registered-P1 termination probe executions=%d, want exactly 1", outcome.flow.probeExecutions)
	}
	if outcome.flow.state.compositePublished() {
		t.Fatal("autonomous registered-P1 termination published the composite")
	}
	borrowedSuccessorCloseConn(t, outcome.flow.conn)
	t.Logf("registered-P1 termination inside the park: autonomous detection latched the loss, owner/use refused after exactly one probe execution and no composite was published")
}

// borrowedReplacementAutonomousObserverLossRefuse: terminate the observer
// backend with the same discipline (no concurrent Close/query on the observer
// connection).
func borrowedReplacementAutonomousObserverLossRefuse(t *testing.T, ctx context.Context) {
	t.Helper()
	b, fresh := borrowedReplacementAutonomousBaseline(t, ctx)
	outcome := borrowedReplacementAutonomousRun(ctx, t, b, fresh, func(hookCtx context.Context, pump *borrowedReplacementAutonomousPump, cancelLane func()) error {
		if err := borrowedReplacementAutonomousAwaitSamples(hookCtx, pump, cancelLane, 2, 60*time.Second); err != nil {
			return err
		}
		observerPID := int(pump.observer.conn.PgConn().PID())
		if observerPID <= 0 {
			t.Fatalf("autonomous observer backend PID is not positive: %d", observerPID)
		}
		var terminated bool
		termCtx, cancelTerm := context.WithTimeout(hookCtx, borrowedOwnerRotationQueryBudget)
		termErr := b.fixture.controlPool.QueryRow(termCtx, `SELECT pg_terminate_backend($1)`, observerPID).Scan(&terminated)
		cancelTerm()
		if termErr != nil || !terminated {
			t.Fatalf("autonomous observer-backend termination refused: terminated=%t err=%v", terminated, termErr)
		}
		select {
		case <-pump.lossAcked:
		case <-hookCtx.Done():
		}
		if pump.lossReasonNow() == "" {
			t.Fatalf("autonomous observer loss was not acknowledged before the hook context ended")
		}
		return errors.New("autonomous detection of the observer-backend loss")
	}, nil, 30*time.Second)
	if outcome.pump == nil || outcome.pump.lossReasonNow() == "" {
		t.Fatal("autonomous observer-backend termination did not permanently latch the loss")
	}
	if outcome.flow.ownerErr == nil || outcome.flow.useErr == nil {
		t.Fatalf("autonomous observer-backend termination did not refuse owner/use: owner=%v use=%v", outcome.flow.ownerErr, outcome.flow.useErr)
	}
	if outcome.flow.probeExecutions != 1 {
		t.Fatalf("autonomous observer-backend termination probe executions=%d, want exactly 1", outcome.flow.probeExecutions)
	}
	if outcome.flow.state.compositePublished() {
		t.Fatal("autonomous observer-backend termination published the composite")
	}
	borrowedSuccessorCloseConn(t, outcome.flow.conn)
	t.Logf("observer-backend termination inside the park: autonomous detection latched the loss without a concurrent observer Close/query, owner/use refused and no composite was published")
}

// borrowedReplacementAutonomousActiveCancelRefuse: cancel during an
// acknowledged ACTIVE observation (explicitly fail on a nil acknowledgement or
// a failed control setup).
func borrowedReplacementAutonomousActiveCancelRefuse(t *testing.T, ctx context.Context) {
	t.Helper()
	b, fresh := borrowedReplacementAutonomousBaseline(t, ctx)
	entered := make(chan struct{})
	var enteredOnce sync.Once
	var activateActive atomic.Bool
	observeOverride := func(observeCtx context.Context, real func(context.Context) error) error {
		if !activateActive.Load() {
			return real(observeCtx)
		}
		enteredOnce.Do(func() { close(entered) })
		select {
		case <-observeCtx.Done():
			return errors.New("injected active observation canceled")
		case <-time.After(30 * time.Second):
			return errors.New("injected active observation timeout")
		}
	}
	outcome := borrowedReplacementAutonomousRun(ctx, t, b, fresh, func(hookCtx context.Context, pump *borrowedReplacementAutonomousPump, cancelLane func()) error {
		activateActive.Store(true)
		select {
		case <-entered:
		case <-hookCtx.Done():
			t.Fatalf("autonomous active observation never acknowledged its entry (control failure)")
		}
		cancelLane()
		// The lane cancellation ends the hook context immediately; the
		// acknowledgement is awaited on an independent bounded context.
		waitCtx, cancelWait := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancelWait()
		select {
		case <-pump.lossAcked:
		case <-waitCtx.Done():
			t.Fatalf("autonomous active-observation cancellation was not acknowledged (control failure)")
		}
		rawAck, completed := pump.checkpointResultNow()
		if !completed {
			t.Fatalf("autonomous active-observation cancellation produced NO completed checkpoint acknowledgement (control failure)")
		}
		if rawAck == nil {
			t.Fatalf("autonomous active-observation cancellation produced a NIL raw checkpoint acknowledgement (control failure)")
		}
		if pump.lossReasonNow() == "" {
			t.Fatalf("autonomous active-observation cancellation was not acknowledged (control failure)")
		}
		return errors.New("autonomous cancellation during an acknowledged active observation")
	}, observeOverride, 30*time.Second)
	if outcome.pump == nil || outcome.pump.lossReasonNow() == "" {
		t.Fatal("autonomous active-observation cancellation did not permanently latch the loss")
	}
	rawAck, completed := outcome.pump.checkpointResultNow()
	if !completed || rawAck == nil {
		t.Fatalf("autonomous active-observation cancellation raw acknowledgement: completed=%t ack=%v", completed, rawAck)
	}
	if outcome.pump.checkpointProtocolViolationNow() != nil {
		t.Fatalf("autonomous active-observation cancellation synthesized a protocol violation instead of using the raw ack: %v", outcome.pump.checkpointProtocolViolationNow())
	}
	if outcome.flow.state.compositePublished() {
		t.Fatal("autonomous active-observation cancellation published the composite")
	}
	borrowedReplacementAutonomousReplayRefuse(t, ctx, b, fresh, outcome, "post-active-cancel-UNKNOWN")
	borrowedSuccessorCloseConn(t, outcome.flow.conn)
	t.Logf("cancellation during an acknowledged active observation: the captured checkpoint refusal is PROVEN non-nil, the loss is latched and no composite was published")
}

// borrowedReplacementAutonomousReplayRefuse proves the autonomous composition
// keeps the session one-use and any permanent UNKNOWN loss copy-shared:
// replay, copies and reconstruction refuse and cannot rehabilitate.
func borrowedReplacementAutonomousReplayRefuse(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding, outcome *borrowedReplacementAutonomousOutcome, label string) {
	t.Helper()
	if outcome == nil || outcome.flow == nil || outcome.flow.reg == nil {
		t.Fatalf("%s: autonomous replay control requires the concrete flow outcome", label)
	}
	replayCtx, cancelReplay := context.WithTimeout(ctx, 30*time.Second)
	replayErr := outcome.flow.reg.Use(replayCtx)
	copied := *outcome.flow.reg
	copyErr := copied.Use(replayCtx)
	cancelReplay()
	if replayErr == nil || copyErr == nil {
		t.Fatalf("%s: autonomous replay/copy use was accepted", label)
	}
	if _, reconErr := newBorrowedReplacementSessionRegistration(t, ctx, b, fresh, outcome.flow.conn); reconErr == nil {
		t.Fatalf("%s: autonomous reconstruction minted a fresh session registration", label)
	}
	if outcome.flow.state.lossReasonNow() != "" {
		stateCopy := outcome.flow.state
		if stateCopy.lossReasonNow() == "" {
			t.Fatalf("%s: autonomous UNKNOWN loss is not copy-shared", label)
		}
		if err := stateCopy.publishDecision(ctx, nil); err == nil {
			t.Fatalf("%s: autonomous loss copy published the composite", label)
		}
	}
	t.Logf("%s: replay/copy/reconstruction refused under autonomous composition; the copy-shared state preserves one-use and any permanent UNKNOWN loss", label)
}

// borrowedReplacementAutonomousStallProgressRefuse: stall
// observation/scheduler progress -> bounded UNKNOWN with no success, and the
// UNKNOWN pump join blocks retirement/reuse.
func borrowedReplacementAutonomousStallProgressRefuse(t *testing.T, ctx context.Context) {
	t.Helper()
	b, fresh := borrowedReplacementAutonomousBaseline(t, ctx)
	stallRelease := make(chan struct{})
	var stallReleaseOnce sync.Once
	releaseStall := func() { stallReleaseOnce.Do(func() { close(stallRelease) }) }
	// The intentional stall is FAILURE-SAFE: an unconditional release is
	// registered immediately, including Fatal/Goexit paths.
	t.Cleanup(releaseStall)
	stallEntered := make(chan struct{})
	var stallEnteredOnce sync.Once
	acknowledgedStall := func(stallCtx context.Context) error {
		stallEnteredOnce.Do(func() { close(stallEntered) })
		select {
		case <-stallRelease:
			return errors.New("injected stalled scheduler released")
		case <-stallCtx.Done():
			// The stall deliberately ignores the pump context until released.
			<-stallRelease
			return errors.New("injected stalled scheduler released")
		}
	}
	var helperCanceledLane atomic.Bool
	outcome := borrowedReplacementAutonomousRun(ctx, t, b, fresh, func(hookCtx context.Context, pump *borrowedReplacementAutonomousPump, cancelLane func()) error {
		// Unwinding guarantee: an IMMEDIATELY installed deferred
		// release-and-bounded-join using the captured pump, so a Fatal/Goexit
		// after the stall entry but before the normal release can never leave
		// the pump blocked in its own bounded cleanup while the release is
		// still pending. t.Cleanup runs LIFO, so this runs BEFORE the pump's
		// own cleanup registered at start.
		t.Cleanup(func() {
			releaseStall()
			if err := pump.stopBounded(30 * time.Second); err != nil {
				t.Errorf("autonomous pump completion is unknown after the deferred stall release: %v", err)
			}
		})
		// Install the scheduler stall only after the first ack so the preRelease
		// first-ack wait cannot hang, and await its entry so no in-flight
		// sample can satisfy the progress wait.
		pump.setStallFn(acknowledgedStall)
		select {
		case <-stallEntered:
		case <-hookCtx.Done():
			t.Fatalf("autonomous scheduler stall never acknowledged its entry (control failure)")
		}
		// Exercise the ACTUAL progress-timeout helper (no separate manual
		// loop): it classifies the UNKNOWN through the shared pre-seal latch
		// and cancels the external lane context.
		proxyCancel := func() {
			helperCanceledLane.Store(true)
			cancelLane()
		}
		return borrowedReplacementAutonomousAwaitSamples(hookCtx, pump, proxyCancel, 1, 3*time.Second)
	}, nil, 3*time.Second)
	if outcome.pumpJoinErr == nil {
		t.Fatal("autonomous stalled pump join was expected to be UNKNOWN")
	}
	if !helperCanceledLane.Load() {
		t.Fatal("autonomous stalled progress did not cancel the external lane context through the real helper")
	}
	if outcome.flow.state.lossReasonNow() == "" {
		t.Fatal("autonomous stalled progress did not permanently latch the UNKNOWN loss")
	}
	if outcome.flow.state.compositePublished() {
		t.Fatal("autonomous stalled progress published the composite")
	}
	if err := borrowedReplacementAutonomousRetireGuarded(t, ctx, b, outcome); err == nil {
		t.Fatal("autonomous retirement was accepted while the pump join was UNKNOWN")
	}
	// Normal explicit release-and-join path (unchanged): release the
	// intentional stall and PROVE the pump completion before continuing.
	releaseStall()
	if err := outcome.pump.stopBounded(30 * time.Second); err != nil {
		t.Fatalf("autonomous pump completion is unknown after the stall release: %v", err)
	}
	borrowedReplacementAutonomousReplayRefuse(t, ctx, b, fresh, outcome, "post-stall-UNKNOWN")
	borrowedSuccessorCloseConn(t, outcome.flow.conn)
	t.Logf("stalled observation/scheduler progress: the real progress-timeout helper latched the permanent UNKNOWN and canceled the external lane context with no composite success; the UNKNOWN pump join blocked retirement/reuse and the pump completion was proven after the release")
}

// borrowedReplacementAutonomousUnexpectedTerminationRefuse: unexpected pre-seal
// pump termination with loss/publication ordering.
func borrowedReplacementAutonomousUnexpectedTerminationRefuse(t *testing.T, ctx context.Context) {
	t.Helper()
	b, fresh := borrowedReplacementAutonomousBaseline(t, ctx)
	outcome := borrowedReplacementAutonomousRun(ctx, t, b, fresh, func(hookCtx context.Context, pump *borrowedReplacementAutonomousPump, _ func()) error {
		pump.terminateForControl()
		// The pump termination cancels the external lane context, which ends
		// the hook context; completion is awaited on an independent bounded
		// context.
		waitCtx, cancelWait := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancelWait()
		select {
		case <-pump.done:
		case <-waitCtx.Done():
			t.Fatalf("autonomous pre-seal pump termination did not complete (control failure)")
		}
		if pump.lossReasonNow() == "" {
			t.Fatalf("autonomous pre-seal pump termination did not latch the loss (control failure)")
		}
		return errors.New("autonomous pump terminated unexpectedly before the seal")
	}, nil, 30*time.Second)
	if !outcome.flow.state.sealedNow() {
		t.Fatal("autonomous pre-seal pump termination did not terminalize the interval")
	}
	if outcome.flow.state.compositePublished() || !outcome.flow.state.lostNow() {
		t.Fatalf("autonomous pre-seal pump termination terminal state: composite=%t lost=%t", outcome.flow.state.compositePublished(), outcome.flow.state.lostNow())
	}
	if outcome.flow.probeExecutions != 1 {
		t.Fatalf("autonomous pre-seal pump termination probe executions=%d, want exactly 1", outcome.flow.probeExecutions)
	}
	borrowedSuccessorCloseConn(t, outcome.flow.conn)
	t.Logf("unexpected pre-seal pump termination: the loss was classified pre-seal, the interval terminalized with lost=true and composite=false (exactly one terminal outcome), and no composite was published")
}

// borrowedReplacementAutonomousPostSealRefuse: post-seal pump refusal preserves
// the terminal state.
func borrowedReplacementAutonomousPostSealRefuse(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding) {
	t.Helper()
	outcome := borrowedReplacementAutonomousRun(ctx, t, b, fresh, borrowedReplacementAutonomousPostProbeSamples, nil, 30*time.Second)
	if outcome.pumpJoinErr != nil {
		t.Fatalf("autonomous post-seal pump join refused: %v", outcome.pumpJoinErr)
	}
	if outcome.pump.lastErrNow() == nil {
		t.Fatal("autonomous post-seal pump did not record a refusal")
	}
	if !outcome.flow.state.compositePublished() || !outcome.flow.state.sealedNow() || outcome.flow.state.lostNow() {
		t.Fatalf("autonomous post-seal pump refusal changed the terminal state: composite=%t sealed=%t lost=%t",
			outcome.flow.state.compositePublished(), outcome.flow.state.sealedNow(), outcome.flow.state.lostNow())
	}
	borrowedSuccessorCloseConn(t, outcome.flow.conn)
	t.Logf("post-seal pump refusal: the terminal state was preserved (composite stays true, lost stays false) and the pump join completed")
}

// TestBorrowedReplacementAutonomousObservation is the bounded autonomous
// observation during the fenced read-only window lane described in the file
// header.
func TestBorrowedReplacementAutonomousObservation(t *testing.T) {
	ctx := t.Context()

	// Genuine baseline + replacement-bound capture on the shared fixture.
	b := newBorrowedSuccessorBaseline(t, ctx)
	originalTargetDSN := b.fixture.writerTargetDSN
	originalObserverDSN := b.fixture.observerTargetDSN
	var stages int32
	orchestrateCtx, cancelOrchestrate := borrowedReplacementOrchestrationContext(ctx)
	fresh, err := borrowedReplacementBindingOrchestrate(orchestrateCtx, t, b, &stages)
	cancelOrchestrate()
	if err != nil || fresh == nil {
		t.Fatalf("autonomous observation genuine capture refused: capture=%v err=%v", fresh, err)
	}
	if b.fixture.writerTargetDSN != originalTargetDSN || b.fixture.observerTargetDSN != originalObserverDSN {
		t.Fatal("autonomous observation lane rewrote an original fixture DSN")
	}
	if got := atomic.LoadInt32(&stages); got != borrowedReplacementStagePrefixCaptured {
		t.Fatalf("autonomous observation pipeline stages=%d, want %d", got, borrowedReplacementStagePrefixCaptured)
	}
	if fresh.binding.TargetDatabaseOID() != fresh.replacementOID || fresh.replacementOID == fresh.oldOID {
		t.Fatalf("autonomous observation capture is not replacement-bound: replacement=%d old=%d binding=%d", fresh.replacementOID, fresh.oldOID, fresh.binding.TargetDatabaseOID())
	}
	t.Logf("autonomous observation genuine capture: old OID %d -> replacement OID %d", fresh.oldOID, fresh.replacementOID)

	// Positive: autonomous pump first ack before the owner window, at least two
	// post-park samples, composite published, terminal sealed, pump joined.
	positive := borrowedReplacementAutonomousRun(ctx, t, b, fresh, borrowedReplacementAutonomousPostProbeSamples, nil, 30*time.Second)
	if positive.runErr != nil {
		t.Fatalf("autonomous observation positive flow refused: %v", positive.runErr)
	}
	if positive.flow.ownerErr != nil || positive.flow.useErr != nil || positive.flow.prefixErr != nil || positive.flow.anchorErr != nil ||
		positive.flow.healthErr != nil || positive.flow.inspectionErr != nil || positive.flow.ackErr != nil || positive.flow.decisionErr != nil {
		t.Fatalf("autonomous observation positive refused: owner=%v use=%v prefix=%v anchor=%v health=%v inspection=%v ack=%v decision=%v",
			positive.flow.ownerErr, positive.flow.useErr, positive.flow.prefixErr, positive.flow.anchorErr, positive.flow.healthErr, positive.flow.inspectionErr, positive.flow.ackErr, positive.flow.decisionErr)
	}
	if positive.pump == nil || positive.pumpJoinErr != nil {
		t.Fatalf("autonomous observation positive pump state: pump=%v joinErr=%v", positive.pump, positive.pumpJoinErr)
	}
	if positive.flow.probeExecutions != 1 {
		t.Fatalf("autonomous observation positive probe executions=%d, want exactly 1", positive.flow.probeExecutions)
	}
	if positive.samplesAtPark < 1 {
		t.Fatalf("autonomous observation positive had no pre-park samples: samplesAtPark=%d", positive.samplesAtPark)
	}
	if got := positive.pump.completedNow(); got < positive.samplesAtPark+2 {
		t.Fatalf("autonomous observation positive completed samples=%d, want at least two after the park (at park=%d)", got, positive.samplesAtPark)
	}
	if !positive.flow.state.compositePublished() || !positive.flow.state.sealedNow() || positive.flow.state.lostNow() {
		t.Fatalf("autonomous observation positive terminal state: composite=%t sealed=%t lost=%t",
			positive.flow.state.compositePublished(), positive.flow.state.sealedNow(), positive.flow.state.lostNow())
	}
	t.Logf("autonomous observation positive: first ack before the owner window, %d samples with at least two inside the park, composite published, interval sealed, pump joined, probe executions=%d", positive.pump.completedNow(), positive.flow.probeExecutions)
	borrowedReplacementAutonomousReplayRefuse(t, ctx, b, fresh, positive, "post-composite")
	if err := borrowedReplacementAutonomousRetireGuarded(t, ctx, b, positive); err != nil {
		t.Fatalf("autonomous observation positive retirement refused: %v", err)
	}

	// Non-destructive post-seal negative on the same fixture.
	borrowedReplacementAutonomousPostSealRefuse(t, ctx, b, fresh)

	// Destructive negatives, each on its own fresh fixture (a lane cancellation
	// permanently invalidates the shared replacement prefix).
	borrowedReplacementAutonomousRegisteredLossRefuse(t, ctx)
	borrowedReplacementAutonomousObserverLossRefuse(t, ctx)
	borrowedReplacementAutonomousActiveCancelRefuse(t, ctx)
	borrowedReplacementAutonomousStallProgressRefuse(t, ctx)
	borrowedReplacementAutonomousUnexpectedTerminationRefuse(t, ctx)

	// Guard unresolved and acceptance unchanged across the shared fixture.
	borrowedOwnerDDLGuard(t, ctx, b)
}
