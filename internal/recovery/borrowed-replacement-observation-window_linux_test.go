//go:build linux && drill

// borrowed-replacement-observation-window_linux_test.go is the bounded
// independent session observation across the owner window lane. It starts from
// a GENUINE replacement-bound capture (the actual bounded binding orchestration
// with stage counters) and ONE registered replacement session; the actual Use
// is parked after its prerequisite checks at the pre-probe barrier (reusing the
// closed window's two-stage barrier). ONE independently owned observer runs on
// a SEPARATE connection using the fixture's protected observer identity: the
// first COMPLETE successful observation (registered backend PID/start, role/DB
// OIDs, tuple/socket token and postmaster incarnation via fresh SQL and strict
// OS/census) is the readiness signal, and later checkpoints are acknowledged
// fresh observations. The existing owner window then runs (original-owner
// identity/advisory, SHARE, complete P1 facts before the release, the two 55P03
// mutation refusals, the in-window join and the supplied-tx recheck), with the
// window's post-probe hook used for an acknowledged fresh checkpoint. The final
// composite decision requires the owner transaction, the Use, the after-return
// prefix/anchor/health rechecks AND the fresh acknowledged observer check, and
// is synchronized with the copy-shared permanent loss latch and the caller
// context under one mutex with no I/O under it; the interval is sealed at that
// decision, the observer is canceled and joined within bounds, and the session
// is retired with the guard unresolved and acceptance zero. An observed loss
// permanently latches and cancels the pending operation; this proves observed
// loss cannot be forgotten or race into composite publication, NOT continuous
// exclusion. The full session revalidation is never called concurrently (the
// independent observer connection is used instead), and no prefix/anchor/Health
// call ever runs inside the owner callback or the in-window watcher iteration.
// There is no atomic acceptance, no guard admission, no restore, no probe
// beyond the session's single fixed SELECT 1, no receipt authority, no clean
// transition, no acceptance, no manifest, no downstream and no Gate1
// authority. Secrets, verifiers and DSNs are never logged.
package recovery_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// borrowedReplacementObservationWindowState is the copy-shared permanent loss
// latch, seal and composite publication state. Copies share this pointer, so an
// observed loss can never be forgotten by any copy.
type borrowedReplacementObservationWindowState struct {
	mu           sync.Mutex
	lost         bool
	lossReason   string
	sealed       bool
	composite    bool
	compositeErr string
}

// latchPreSealLoss classifies and records a pre-seal loss UNDER THE SAME mutex
// as the decision-plus-seal terminal transition: a loss event observed after
// the interval is sealed preserves the terminal result (no latch), while a
// pre-seal loss latches and therefore can never coexist with a composite
// success. It returns whether the loss was latched.
func (s *borrowedReplacementObservationWindowState) latchPreSealLoss(reason string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sealed {
		return false
	}
	if !s.lost {
		s.lost = true
		s.lossReason = reason
	}
	return true
}

func (s *borrowedReplacementObservationWindowState) lossReasonNow() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lossReason
}

func (s *borrowedReplacementObservationWindowState) lostNow() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lost
}

func (s *borrowedReplacementObservationWindowState) sealedNow() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sealed
}

func (s *borrowedReplacementObservationWindowState) compositePublished() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.composite
}

// publishDecision is the ONE terminal transition: under the shared mutex (no
// I/O) it evaluates every prerequisite, latches or refuses as needed AND seals
// the interval atomically, including every refusal and early exit. A second
// decision after the terminal transition is rejected without changing state.
func (s *borrowedReplacementObservationWindowState) publishDecision(ctx context.Context, prereq error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sealed {
		return errors.New("observation window composite decision was already terminal (sealed)")
	}
	// Decision-plus-seal is ONE terminal transition: seal first so no early
	// exit can leave the interval open and no second decision can be accepted.
	s.sealed = true
	if prereq != nil {
		s.compositeErr = prereq.Error()
		return errors.New("observation window composite prerequisites were not satisfied")
	}
	if s.lost {
		s.compositeErr = "observed loss latched"
		return errors.New("observation window composite refused: an observed loss is permanently latched")
	}
	if ctx == nil || ctx.Err() != nil {
		if !s.lost {
			s.lost = true
			s.lossReason = "observation window caller context ended before the composite decision"
		}
		s.compositeErr = "caller context ended before the composite decision"
		return errors.New("observation window composite refused: caller context ended before the decision")
	}
	s.composite = true
	return nil
}

// borrowedReplacementObservationWindowObserve performs ONE complete independent
// observation through the separate observer connection and the strict OS/census
// evidence: the registered backend PID/start, role/DB OIDs and names, the
// registered tuple/socket token and the postmaster incarnation. It never uses
// the retained P1 connection and never calls the full session revalidation.
func borrowedReplacementObservationWindowObserve(ctx context.Context, f *borrowedAuthHandoffFixture, fresh *borrowedReplacementBinding, reg *borrowedReplacementSessionRegistration, conn *pgx.Conn) error {
	if f == nil || fresh == nil || reg == nil || reg.state == nil || conn == nil {
		return errors.New("observation window requires the concrete fixture, capture, registration and observer connection")
	}
	state := reg.state
	var (
		backendStart time.Time
		roleName     string
		databaseName string
		roleOID      uint32
		databaseOID  uint32
	)
	readCtx, cancelRead := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	readErr := conn.QueryRow(readCtx, `
SELECT a.backend_start, a.usename::text, a.datname::text, r.oid::oid, d.oid::oid
FROM pg_stat_activity a
JOIN pg_roles r ON r.rolname = a.usename
JOIN pg_database d ON d.datname = a.datname
WHERE a.pid = $1`, state.backendPID).
		Scan(&backendStart, &roleName, &databaseName, &roleOID, &databaseOID)
	cancelRead()
	if readErr != nil {
		return errors.New("observation window registered backend read refused")
	}
	if !backendStart.Equal(state.backendStart) || roleName != f.writerRole || databaseName != f.targetDB ||
		roleOID != state.roleOID || databaseOID != state.targetDBOID {
		return errors.New("observation window registered backend identity changed")
	}
	var (
		postmasterStart time.Time
		systemID        string
	)
	incarnationCtx, cancelIncarnation := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	incarnationErr := conn.QueryRow(incarnationCtx, `SELECT pg_postmaster_start_time(), (SELECT system_identifier::text FROM pg_control_system())`).
		Scan(&postmasterStart, &systemID)
	cancelIncarnation()
	if incarnationErr != nil {
		return errors.New("observation window postmaster incarnation read refused")
	}
	if !postmasterStart.Equal(fresh.binding.PostmasterStartTime()) || systemID != fresh.binding.ClusterSystemIdentifier() {
		return errors.New("observation window postmaster incarnation changed")
	}
	token, err := borrowedSuccessorAssociateToken(ctx, f, state.serverLocal, state.clientLocal)
	if err != nil {
		return errors.New("observation window strict census association refused")
	}
	if token.ChildPID != state.backendPID || token.ChildStart != state.osStart || token.Inode != state.socketInode ||
		token.Local != state.serverLocal || token.Remote != state.clientLocal {
		return errors.New("observation window strict census token changed")
	}
	inspectCtx, cancelInspect := context.WithTimeout(ctx, borrowedAuthStagingAssociationBudget)
	_, inspectErr := inspectBorrowedIdentityStrict(inspectCtx, f.fx.containerID, f.prefix.strictHelperPath,
		f.fx.postmasterPID, f.fx.postmasterStr, state.backendPID, state.socketInode, borrowedIdentityStrictPhaseInitial)
	cancelInspect()
	if inspectErr != nil {
		return errors.New("observation window strict backend inspection refused")
	}
	osStatCtx, cancelOsStat := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	osState, osStart, osErr := borrowedAuthStagingContainerStat(osStatCtx, f.fx.containerID, state.backendPID)
	cancelOsStat()
	if osErr != nil || osStart != state.osStart || !borrowedAuthStagingLiveState(osState) {
		return errors.New("observation window strict OS identity refused")
	}
	return nil
}

// borrowedReplacementObservationWindowRequest is one checkpoint request: it
// carries the caller-derived phase context, so the WHOLE observation is bounded
// by that phase (not only the send/wait).
type borrowedReplacementObservationWindowRequest struct {
	ctx  context.Context
	resp chan error
}

// borrowedReplacementObservationWindowObserver is ONE independently owned
// observer: a separate observer-identity connection, an initial complete
// observation as the readiness signal, and acknowledged checkpoints afterwards.
// Any observed failure, phase-context end or unexpected pre-seal termination
// permanently latches the shared loss and cancels the pending operation.
type borrowedReplacementObservationWindowObserver struct {
	state         *borrowedReplacementObservationWindowState
	fixture       *borrowedAuthHandoffFixture
	fresh         *borrowedReplacementBinding
	reg           *borrowedReplacementSessionRegistration
	conn          *pgx.Conn
	ready         chan struct{}
	done          chan struct{}
	requests      chan *borrowedReplacementObservationWindowRequest
	loopCtx       context.Context
	cancel        context.CancelFunc
	cancelPending func()
	closeOnce     sync.Once

	// observeFn is the test-only injection seam for incomplete/timeout
	// observations; it is set before a checkpoint request and nil in real runs.
	observeFn func(context.Context) error
}

// observationContext bounds ONE observation by BOTH the caller-derived phase
// context AND the observer lifetime, so a checkpoint can never outlive the
// observer.
func (o *borrowedReplacementObservationWindowObserver) observationContext(phase context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(phase)
	stop := context.AfterFunc(o.loopCtx, cancel)
	return ctx, func() {
		stop()
		cancel()
	}
}

func startBorrowedReplacementObservationWindowObserver(t *testing.T, ctx context.Context, f *borrowedAuthHandoffFixture, fresh *borrowedReplacementBinding, reg *borrowedReplacementSessionRegistration, state *borrowedReplacementObservationWindowState, cancelPending func()) *borrowedReplacementObservationWindowObserver {
	t.Helper()
	conn := borrowedOwnerDDLConnect(t, ctx, f.observerTargetDSN)
	loopCtx, cancel := context.WithCancel(ctx)
	observer := &borrowedReplacementObservationWindowObserver{
		state: state, fixture: f, fresh: fresh, reg: reg, conn: conn,
		ready: make(chan struct{}), done: make(chan struct{}),
		requests: make(chan *borrowedReplacementObservationWindowRequest),
		loopCtx:  loopCtx, cancel: cancel, cancelPending: cancelPending,
	}
	latchAndCancel := func(reason string) {
		observer.latchPreSeal(reason)
	}
	go func() {
		defer close(observer.done)
		// Initial readiness has its OWN whole-observation phase budget, bounded
		// additionally by the observer lifetime.
		readinessPhase, cancelReadiness := context.WithTimeout(loopCtx, 120*time.Second)
		readinessCtx, cancelObservation := observer.observationContext(readinessPhase)
		readinessErr := observer.observe(readinessCtx)
		cancelObservation()
		if readinessErr != nil {
			cancelReadiness()
			latchAndCancel("initial independent observation refused: " + readinessErr.Error())
			return
		}
		// The readiness acknowledgement is only published while BOTH the
		// readiness phase and the observer lifetime are live.
		if err := loopCtx.Err(); err != nil {
			cancelReadiness()
			latchAndCancel("observer readiness phase context ended before the acknowledgement")
			return
		}
		if err := readinessPhase.Err(); err != nil {
			cancelReadiness()
			latchAndCancel("observer readiness phase budget ended before the acknowledgement")
			return
		}
		cancelReadiness()
		close(observer.ready)
		for {
			select {
			case req := <-observer.requests:
				// The WHOLE observation is bounded by BOTH the caller-derived
				// phase context carried with the request AND the observer
				// lifetime.
				observationCtx, cancelObservation := observer.observationContext(req.ctx)
				observationErr := observer.observe(observationCtx)
				cancelObservation()
				if observationErr != nil {
					latchAndCancel("independent observation refused: " + observationErr.Error())
					req.resp <- observationErr
					return
				}
				if err := loopCtx.Err(); err != nil {
					latchAndCancel("observer lifetime ended before the success acknowledgement")
					req.resp <- errors.New("observation window observer lifetime ended before the acknowledgement")
					return
				}
				if err := req.ctx.Err(); err != nil {
					latchAndCancel("observation phase context ended before the success acknowledgement")
					req.resp <- errors.New("observation window checkpoint phase context ended before the acknowledgement")
					return
				}
				req.resp <- nil
			case <-loopCtx.Done():
				// Expected post-seal shutdown preserves the terminal result; an
				// unexpected pre-seal termination latches and cancels UNDER THE
				// SAME MUTEX as the decision-plus-seal transition.
				if observer.state.latchPreSealLoss("observer terminated before the interval seal") {
					if observer.cancelPending != nil {
						observer.cancelPending()
					}
				}
				return
			}
		}
	}()
	return observer
}

// latchPreSeal routes EVERY loss producer through the same mutex-protected
// pre-seal classification: a post-seal refusal preserves the terminal result
// and never sets lost=true over a composite success.
func (o *borrowedReplacementObservationWindowObserver) latchPreSeal(reason string) {
	if o.state.latchPreSealLoss(reason) && o.cancelPending != nil {
		o.cancelPending()
	}
}

func (o *borrowedReplacementObservationWindowObserver) observe(ctx context.Context) error {
	if o.observeFn != nil {
		return o.observeFn(ctx)
	}
	return borrowedReplacementObservationWindowObserve(ctx, o.fixture, o.fresh, o.reg, o.conn)
}

// requestCheckpoint requests ONE acknowledged fresh observation carrying the
// caller-derived phase context. A stopped observer, an ended phase context or a
// failed observation permanently latches the shared loss and returns an error;
// a request timeout while the observation is still pending is UNKNOWN and
// latches the loss instead of returning usable state.
func (o *borrowedReplacementObservationWindowObserver) requestCheckpoint(ctx context.Context) error {
	if ctx == nil {
		o.latchPreSeal("checkpoint request phase context is missing")
		return errors.New("observation window checkpoint requires a caller-derived phase context")
	}
	req := &borrowedReplacementObservationWindowRequest{ctx: ctx, resp: make(chan error, 1)}
	select {
	case o.requests <- req:
	case <-o.done:
		o.latchPreSeal("observer stopped before the checkpoint request")
		return errors.New("observation window observer is not running")
	case <-ctx.Done():
		o.latchPreSeal("checkpoint request phase context ended before delivery")
		return errors.New("observation window checkpoint request phase context ended")
	}
	select {
	case err := <-req.resp:
		return err
	case <-o.done:
		o.latchPreSeal("observer stopped during the checkpoint")
		return errors.New("observation window observer stopped during the checkpoint")
	case <-ctx.Done():
		o.latchPreSeal("checkpoint phase context ended while the observation was pending")
		return errors.New("observation window checkpoint phase context ended while the observation was pending")
	}
}

// stopBounded cancels and bounded-joins the observer exactly once. A join
// timeout returns an explicit UNKNOWN termination error to the orchestration
// instead of silently proceeding.
func (o *borrowedReplacementObservationWindowObserver) stopBounded() error {
	o.closeOnce.Do(func() { o.cancel() })
	select {
	case <-o.done:
		return nil
	case <-time.After(30 * time.Second):
		return errors.New("observation window observer termination is UNKNOWN after the bounded join")
	}
}

// borrowedReplacementObservationWindowCloseObserverConn closes the observer
// connection with an independent bounded cleanup and preserves every error.
func borrowedReplacementObservationWindowCloseObserverConn(t *testing.T, conn *pgx.Conn) {
	t.Helper()
	closeCtx, cancelClose := context.WithTimeout(context.Background(), borrowedOwnerRotationCleanupBudget)
	closeErr := conn.Close(closeCtx)
	cancelClose()
	if closeErr != nil {
		t.Fatalf("observation window observer connection close refused: %v", closeErr)
	}
}

// borrowedReplacementObservationWindowPlan is the negative-injection plan of
// one observation-window flow.
type borrowedReplacementObservationWindowPlan struct {
	// preRelease runs after the Use parks at the pre-probe barrier and before
	// the owner window is entered.
	preRelease func(ctx context.Context, observer *borrowedReplacementObservationWindowObserver) error
	// checkpointAtPostProbe requests an acknowledged fresh checkpoint through
	// the window's post-probe hook (ignored when postProbe is set).
	checkpointAtPostProbe bool
	// postProbe overrides the window's post-probe hook (actual loss injection).
	postProbe func(context.Context) error
	// afterInspection runs after the final inspection checkpoint and before the
	// acknowledgement checkpoint (actual loss after the final inspection).
	afterInspection func(observer *borrowedReplacementObservationWindowObserver)
	// beforeDecision runs immediately before the terminal decision and may
	// cancel the decision phase context (actual caller-context cancellation at
	// the final publication).
	beforeDecision func(cancelDecision context.CancelFunc)
	// useResultAdapter is an optional test-only hook invoked only AFTER the Use
	// completion is channel-confirmed; it receives the ACTUAL useErr (read only
	// after proven completion) and may refuse the flow with a non-nil error.
	useResultAdapter func(useErr error) error
	// useResultRecorder is an optional test-only hook invoked with the ACTUAL
	// Use result BEFORE the completion channel is closed, so an in-window owner
	// tail can inspect the channel-confirmed result inside the transaction.
	useResultRecorder func(useErr error)
	// useWorkerStall is an optional test-only hook invoked after the Use worker
	// returns and BEFORE the result is recorded and the completion channel is
	// closed: it stalls the worker so the REAL bounded join can fail.
	useWorkerStall func(ctx context.Context)
	// useJoinBound overrides the Use join bound (default 30s) for the test-only
	// bounded worker-stall control.
	useJoinBound time.Duration
}

// borrowedReplacementObservationWindowOutcome preserves every error of one
// flow for the lane assertions. terminationUnknown marks an unproven
// cancellation/termination (no retirement or reuse is allowed then).
type borrowedReplacementObservationWindowOutcome struct {
	conn     *pgx.Conn
	reg      *borrowedReplacementSessionRegistration
	observer *borrowedReplacementObservationWindowObserver
	state    *borrowedReplacementObservationWindowState
	// useCompletion exposes the ACTUAL Use worker completion channel (closed by
	// the worker as its LAST act) so an owner tail can prove completion instead
	// of inferring it from the pre-completion recorder flag.
	useCompletion      <-chan struct{}
	preErr             error
	ownerErr           error
	useErr             error
	prefixErr          error
	anchorErr          error
	healthErr          error
	inspectionErr      error
	ackErr             error
	decisionErr        error
	terminationUnknown bool
	probeExecutions    int32
}

// borrowedReplacementObservationWindowRun is the common error-returning
// observation-window flow: readiness, the parked Use, the optional pre-release
// step, the existing owner window with the post-probe checkpoint/loss, the
// bounded joins, the after-return rechecks, the final inspection and
// acknowledgement checkpoints, the ONE synchronized composite decision and the
// interval seal. The observer is always canceled and bounded-joined.
func borrowedReplacementObservationWindowRun(ctx context.Context, t *testing.T, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding, plan *borrowedReplacementObservationWindowPlan) (*borrowedReplacementObservationWindowOutcome, error) {
	t.Helper()
	if plan == nil {
		plan = &borrowedReplacementObservationWindowPlan{}
	}
	// runCtx is the externally cancelable Run context: the whole run (observer,
	// parked Use, owner window, rechecks, checkpoints and the final decision)
	// derives from it, so a caller cancel at the final publication cancels the
	// RUN context itself, not only an internal decision child.
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	conn, reg := borrowedReplacementSessionRegisterP1(t, runCtx, b, fresh)
	barrier := installBorrowedReplacementCatalogWindowBarrier(t, reg)
	state := &borrowedReplacementObservationWindowState{}
	useCtx, cancelUse := context.WithCancel(runCtx)
	cancelPending := func() {
		cancelUse()
		barrier.cancel()
	}
	observer := startBorrowedReplacementObservationWindowObserver(t, runCtx, b.fixture, fresh, reg, state, cancelPending)
	observerStopped := false
	t.Cleanup(func() {
		if !observerStopped {
			if err := observer.stopBounded(); err != nil {
				t.Errorf("%v", err)
			}
		}
	})
	var useErr error
	useClosed := make(chan struct{})
	go func() {
		useErr = reg.Use(useCtx)
		if plan.useWorkerStall != nil {
			plan.useWorkerStall(useCtx)
		}
		if plan.useResultRecorder != nil {
			plan.useResultRecorder(useErr)
		}
		close(useClosed)
	}()
	joined := false
	t.Cleanup(func() {
		cancelPending()
		barrier.releaseAll()
		if joined {
			return
		}
		select {
		case <-useClosed:
		case <-time.After(30 * time.Second):
			t.Errorf("observation window use join is unknown after the bounded cleanup join")
		}
	})
	outcome := &borrowedReplacementObservationWindowOutcome{conn: conn, reg: reg, observer: observer, state: state, useCompletion: useClosed}
	// joinUse only reads the worker-owned useErr after PROVEN completion; a
	// join timeout is an explicit UNKNOWN termination error.
	useJoinBound := plan.useJoinBound
	if useJoinBound <= 0 {
		useJoinBound = 30 * time.Second
	}
	joinUse := func() error {
		select {
		case <-useClosed:
			joined = true
			outcome.useErr = useErr
			return nil
		case <-time.After(useJoinBound):
			outcome.terminationUnknown = true
			return errors.New("observation window use termination is UNKNOWN after the bounded join")
		}
	}
	// terminalize seals the interval on EVERY early exit through the ONE
	// terminal decision transition.
	terminalize := func(prereq error) {
		outcome.decisionErr = state.publishDecision(runCtx, errors.Join(prereq, errors.New("observation window flow ended before the final decision")))
	}
	// Readiness: the FIRST complete successful observation, not the goroutine
	// start.
	readyCtx, cancelReady := context.WithTimeout(runCtx, 120*time.Second)
	select {
	case <-observer.ready:
	case <-observer.done:
		outcome.preErr = errors.New("observation window observer stopped before readiness")
	case <-readyCtx.Done():
		outcome.preErr = errors.New("observation window observer readiness timed out")
	}
	cancelReady()
	if outcome.preErr != nil {
		cancelPending()
		barrier.releaseAll()
		runErr := joinUse()
		stopErr := observer.stopBounded()
		observerStopped = true
		if stopErr != nil {
			outcome.terminationUnknown = true
		}
		terminalize(outcome.preErr)
		outcome.probeExecutions = atomic.LoadInt32(&reg.state.probeExecutions)
		return outcome, errors.Join(runErr, stopErr)
	}
	// Park at the pre-probe barrier.
	parkCtx, cancelPark := context.WithTimeout(runCtx, 120*time.Second)
	select {
	case <-barrier.preEntered:
	case <-parkCtx.Done():
		outcome.preErr = errors.New("observation window use never reached the pre-probe barrier")
	}
	cancelPark()
	if outcome.preErr == nil && plan.preRelease != nil {
		outcome.preErr = plan.preRelease(runCtx, observer)
	}
	if outcome.preErr != nil {
		cancelPending()
		barrier.releaseAll()
		runErr := joinUse()
		stopErr := observer.stopBounded()
		observerStopped = true
		if stopErr != nil {
			outcome.terminationUnknown = true
		}
		terminalize(outcome.preErr)
		outcome.probeExecutions = atomic.LoadInt32(&reg.state.probeExecutions)
		return outcome, errors.Join(runErr, stopErr)
	}
	// The existing owner window (closed window file): the post-probe hook is
	// the acknowledged fresh checkpoint or the injected actual loss.
	postProbe := plan.postProbe
	if postProbe == nil && plan.checkpointAtPostProbe {
		postProbe = func(hookCtx context.Context) error {
			return observer.requestCheckpoint(hookCtx)
		}
	}
	ownerCtx, cancelOwner := context.WithTimeout(runCtx, 180*time.Second)
	outcome.ownerErr = borrowedReplacementCatalogWindowOwnerTx(ownerCtx, t, b, barrier, useClosed, b.verifierP1, b.preState, postProbe)
	cancelOwner()
	cancelPending()
	barrier.releaseAll()
	runErr := joinUse()
	// Test-only nil-default adapter: invoked ONLY after the Use completion is
	// proven. On an UNKNOWN join the nil-default useErr is NEVER passed and an
	// explicit refusal is propagated into the decision prerequisites.
	var useAdapterErr error
	if outcome.terminationUnknown {
		useAdapterErr = errors.New("observation window use completion is UNKNOWN; the use result is not proven and the adapter is skipped")
		runErr = errors.Join(runErr, useAdapterErr)
	} else if plan.useResultAdapter != nil {
		useAdapterErr = plan.useResultAdapter(outcome.useErr)
		runErr = errors.Join(runErr, useAdapterErr)
	}
	// After-return rechecks (fresh prefix, original anchor, owner health).
	prefixCtx, cancelPrefix := context.WithTimeout(runCtx, borrowedOwnerRotationQueryBudget)
	outcome.prefixErr = fresh.prefix.Recheck(t, prefixCtx)
	cancelPrefix()
	anchorCtx, cancelAnchor := context.WithTimeout(runCtx, borrowedOwnerRotationQueryBudget)
	outcome.anchorErr = fresh.anchor.Recheck(anchorCtx)
	cancelAnchor()
	healthCtx, cancelHealth := context.WithTimeout(runCtx, borrowedOwnerRotationHealthBudget)
	outcome.healthErr = b.fixture.lock.Health(healthCtx)
	cancelHealth()
	// Final inspection checkpoint and the acknowledgement checkpoint (an actual
	// loss after the inspection is awaited through the ack latch).
	inspectionCtx, cancelInspection := context.WithTimeout(runCtx, 60*time.Second)
	outcome.inspectionErr = observer.requestCheckpoint(inspectionCtx)
	cancelInspection()
	if plan.afterInspection != nil {
		plan.afterInspection(observer)
	}
	ackCtx, cancelAck := context.WithTimeout(runCtx, 60*time.Second)
	outcome.ackErr = observer.requestCheckpoint(ackCtx)
	cancelAck()
	// The ONE terminal decision-plus-seal transition. The optional hook cancels
	// the RUN's caller context itself (not an internal decision child), proving
	// an actual caller cancel at the final publication.
	if plan.beforeDecision != nil {
		plan.beforeDecision(cancelRun)
	}
	prereq := errors.Join(outcome.ownerErr, outcome.useErr, outcome.prefixErr, outcome.anchorErr, outcome.healthErr, outcome.inspectionErr, outcome.ackErr, useAdapterErr)
	outcome.decisionErr = state.publishDecision(runCtx, prereq)
	stopErr := observer.stopBounded()
	observerStopped = true
	if stopErr != nil {
		outcome.terminationUnknown = true
		runErr = errors.Join(runErr, stopErr)
	}
	outcome.probeExecutions = atomic.LoadInt32(&reg.state.probeExecutions)
	return outcome, runErr
}

// borrowedReplacementObservationWindowPositive is the positive flow: readiness,
// the parked Use, the owner window with the acknowledged post-probe checkpoint,
// both checkpoints, the composite publication, the interval seal and the
// bounded observer join, followed by the replay/copy/reconstruction refusal.
func borrowedReplacementObservationWindowPositive(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding) {
	t.Helper()
	outcome, runErr := borrowedReplacementObservationWindowRun(ctx, t, b, fresh, &borrowedReplacementObservationWindowPlan{checkpointAtPostProbe: true})
	if runErr != nil {
		t.Fatalf("observation window positive orchestration refused: %v", runErr)
	}
	if outcome.preErr != nil || outcome.ownerErr != nil || outcome.useErr != nil || outcome.prefixErr != nil || outcome.anchorErr != nil || outcome.healthErr != nil ||
		outcome.inspectionErr != nil || outcome.ackErr != nil || outcome.decisionErr != nil {
		t.Fatalf("observation window positive refused: pre=%v latch=%q owner=%v use=%v prefix=%v anchor=%v health=%v inspection=%v ack=%v decision=%v",
			outcome.preErr, outcome.state.lossReasonNow(), outcome.ownerErr, outcome.useErr, outcome.prefixErr, outcome.anchorErr, outcome.healthErr, outcome.inspectionErr, outcome.ackErr, outcome.decisionErr)
	}
	if outcome.probeExecutions != 1 {
		t.Fatalf("observation window positive probe executions=%d, want exactly 1", outcome.probeExecutions)
	}
	if outcome.terminationUnknown {
		t.Fatalf("observation window positive termination is UNKNOWN: %v", runErr)
	}
	if !outcome.state.compositePublished() {
		t.Fatal("observation window positive did not publish the composite")
	}
	if outcome.state.lossReasonNow() != "" {
		t.Fatalf("observation window positive latched a loss: %s", outcome.state.lossReasonNow())
	}
	if !outcome.state.sealedNow() {
		t.Fatal("observation window positive did not seal the interval")
	}
	// Replay/copy/reconstruction cannot rehabilitate.
	replayCtx, cancelReplay := context.WithTimeout(ctx, 30*time.Second)
	replayErr := outcome.reg.Use(replayCtx)
	copied := *outcome.reg
	copyErr := copied.Use(replayCtx)
	cancelReplay()
	if replayErr == nil || copyErr == nil {
		t.Fatal("observation window positive replay/copy use was accepted")
	}
	if got := atomic.LoadInt32(&outcome.reg.state.probeExecutions); got != 1 {
		t.Fatalf("observation window replay executed the probe: executions=%d", got)
	}
	if _, reconErr := newBorrowedReplacementSessionRegistration(t, ctx, b, fresh, outcome.conn); reconErr == nil {
		t.Fatal("observation window positive reconstruction minted a fresh session registration")
	}
	t.Logf("observation window positive: readiness, acknowledged post-probe checkpoint, composite published, interval sealed, observer joined, replay/copy/reconstruction refused (probe executions=%d)", outcome.probeExecutions)
	borrowedReplacementSessionRetire(t, ctx, b, outcome.conn, outcome.reg.state)
}

// borrowedReplacementObservationWindowBackendLossRefuse proves an actual
// registered-backend termination before the release refuses with zero probe
// executions and no composite publication (the observer detects the loss,
// latches it and cancels the pending operation).
func borrowedReplacementObservationWindowBackendLossRefuse(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding) {
	t.Helper()
	var captured *borrowedReplacementObservationWindowObserver
	outcome, runErr := borrowedReplacementObservationWindowRun(ctx, t, b, fresh, &borrowedReplacementObservationWindowPlan{
		preRelease: func(preCtx context.Context, observer *borrowedReplacementObservationWindowObserver) error {
			captured = observer
			var terminated bool
			termCtx, cancelTerm := context.WithTimeout(preCtx, borrowedOwnerRotationQueryBudget)
			termErr := b.fixture.controlPool.QueryRow(termCtx, `SELECT pg_terminate_backend($1)`, observer.reg.state.backendPID).Scan(&terminated)
			cancelTerm()
			if termErr != nil || !terminated {
				return fmt.Errorf("observation window backend-loss terminate refused: terminated=%t err=%v", terminated, termErr)
			}
			borrowedAuthStagingAwaitDisappearance(preCtx, t, b.fixture.fx.containerID, observer.reg.state.backendPID, observer.reg.state.osStart, borrowedAuthStagingDisappearanceBudget, true)
			return observer.requestCheckpoint(preCtx)
		},
	})
	if runErr != nil {
		t.Fatalf("observation window backend-loss orchestration refused: %v", runErr)
	}
	if captured == nil {
		t.Fatal("observation window backend-loss observer was not captured")
	}
	if outcome.preErr == nil {
		t.Fatal("registered-backend termination before the release was not detected by the observer checkpoint")
	}
	if outcome.probeExecutions != 0 {
		t.Fatalf("registered-backend termination executed the probe %d times, want 0", outcome.probeExecutions)
	}
	if outcome.state.lossReasonNow() == "" {
		t.Fatal("registered-backend termination did not permanently latch the observed loss")
	}
	if outcome.state.compositePublished() {
		t.Fatal("registered-backend termination published the composite")
	}
	borrowedSuccessorCloseConn(t, outcome.conn)
	t.Logf("registered-backend termination before the release: the observer detected the loss, latched it and canceled the pending operation with zero probe executions and no composite publication")
}

// borrowedReplacementObservationWindowObserverConnLossRefuse proves an actual
// observer-connection loss at the post-probe barrier (after exactly one probe
// execution) fails the owner window and refuses the composite.
func borrowedReplacementObservationWindowObserverConnLossRefuse(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding) {
	t.Helper()
	var captured *borrowedReplacementObservationWindowObserver
	outcome, runErr := borrowedReplacementObservationWindowRun(ctx, t, b, fresh, &borrowedReplacementObservationWindowPlan{
		preRelease: func(_ context.Context, observer *borrowedReplacementObservationWindowObserver) error {
			captured = observer
			return nil
		},
		postProbe: func(hookCtx context.Context) error {
			if captured == nil {
				return errors.New("observation window observer was not captured")
			}
			borrowedReplacementObservationWindowCloseObserverConn(t, captured.conn)
			return captured.requestCheckpoint(hookCtx)
		},
	})
	if runErr != nil {
		t.Fatalf("observation window observer-conn-loss orchestration refused: %v", runErr)
	}
	if outcome.ownerErr == nil {
		t.Fatal("observer-connection loss at the post-probe barrier did not fail the owner window")
	}
	if outcome.probeExecutions != 1 {
		t.Fatalf("observer-connection loss probe executions=%d, want exactly 1", outcome.probeExecutions)
	}
	if outcome.state.lossReasonNow() == "" {
		t.Fatal("observer-connection loss did not permanently latch the observed loss")
	}
	if outcome.state.compositePublished() {
		t.Fatal("observer-connection loss published the composite")
	}
	if outcome.useErr == nil {
		t.Fatal("observer-connection loss did not refuse the parked use")
	}
	borrowedSuccessorCloseConn(t, outcome.conn)
	t.Logf("observer-connection loss at the post-probe barrier: the checkpoint failed, the owner window refused after exactly one probe execution, the loss latched and no composite was published")
}

// borrowedReplacementObservationWindowOwnerLossRefuse proves an actual owner
// loss injected at the post-probe barrier fails the owner window and refuses
// the composite with no successful use publication.
func borrowedReplacementObservationWindowOwnerLossRefuse(t *testing.T, ctx context.Context) {
	t.Helper()
	b := newBorrowedSuccessorBaseline(t, ctx)
	orchestrateCtx, cancelOrchestrate := borrowedReplacementOrchestrationContext(ctx)
	var stages int32
	fresh, err := borrowedReplacementBindingOrchestrate(orchestrateCtx, t, b, &stages)
	cancelOrchestrate()
	if err != nil || fresh == nil {
		t.Fatalf("observation window owner-loss pipeline refused: capture=%v err=%v", fresh, err)
	}
	controlPID := fresh.binding.ControlBackendPID()
	if controlPID <= 0 || controlPID != b.owner.BackendPID {
		t.Fatalf("observation window owner-loss control PID %d is missing or not the anchored owner PID %d", controlPID, b.owner.BackendPID)
	}
	outcome, runErr := borrowedReplacementObservationWindowRun(ctx, t, b, fresh, &borrowedReplacementObservationWindowPlan{
		postProbe: func(hookCtx context.Context) error {
			var terminated bool
			termCtx, cancelTerm := context.WithTimeout(hookCtx, borrowedOwnerRotationQueryBudget)
			termErr := b.fixture.controlPool.QueryRow(termCtx, `SELECT pg_terminate_backend($1)`, controlPID).Scan(&terminated)
			cancelTerm()
			if termErr != nil || !terminated {
				return fmt.Errorf("observation window owner-loss terminate refused: terminated=%t err=%v", terminated, termErr)
			}
			return nil
		},
	})
	if runErr != nil {
		t.Fatalf("observation window owner-loss orchestration refused: %v", runErr)
	}
	if outcome.ownerErr == nil {
		t.Fatal("owner loss at the post-probe barrier did not fail the owner window")
	}
	if outcome.useErr == nil {
		t.Fatal("owner loss did not refuse the parked use")
	}
	if outcome.state.compositePublished() {
		t.Fatal("owner loss published the composite")
	}
	healthCtx, cancelHealth := context.WithTimeout(ctx, borrowedOwnerRotationHealthBudget)
	healthErr := b.fixture.lock.Health(healthCtx)
	cancelHealth()
	if healthErr == nil {
		t.Fatal("owner health remained successful after real owner loss")
	}
	borrowedSuccessorCloseConn(t, outcome.conn)
	t.Logf("owner loss at the post-probe barrier: the owner window and the parked use both refused and no composite was published")
}

// borrowedReplacementObservationWindowPrefixLossRefuse proves an actual
// copied-prefix invalidation refuses the use pre-execution and the composite
// (LAST on the fixture: it permanently invalidates the shared replacement
// prefix).
func borrowedReplacementObservationWindowPrefixLossRefuse(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding) {
	t.Helper()
	outcome, runErr := borrowedReplacementObservationWindowRun(ctx, t, b, fresh, &borrowedReplacementObservationWindowPlan{
		preRelease: func(_ context.Context, _ *borrowedReplacementObservationWindowObserver) error {
			borrowedReplacementSessionCopiedPrefixLoss(t, fresh)
			return nil
		},
		checkpointAtPostProbe: true,
	})
	if runErr != nil {
		t.Fatalf("observation window prefix-loss orchestration refused: %v", runErr)
	}
	if outcome.useErr == nil {
		t.Fatal("copied-prefix invalidation did not refuse the use")
	}
	if outcome.probeExecutions != 0 {
		t.Fatalf("copied-prefix invalidation executed the probe %d times, want 0", outcome.probeExecutions)
	}
	if outcome.state.compositePublished() {
		t.Fatal("copied-prefix invalidation published the composite")
	}
	if _, reconErr := newBorrowedReplacementSessionRegistration(t, ctx, b, fresh, outcome.conn); reconErr == nil {
		t.Fatal("copied-prefix invalidation reconstruction minted a fresh session registration")
	}
	borrowedSuccessorCloseConn(t, outcome.conn)
	t.Logf("copied-prefix invalidation: the use refused pre-execution and no composite was published; reconstruction refused")
}

// borrowedReplacementObservationWindowCancelRefuse proves a caller cancel at
// the pre-probe park refuses with zero probe executions and no composite
// publication (cancel-before-release bounded join).
func borrowedReplacementObservationWindowCancelRefuse(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding) {
	t.Helper()
	outcome, runErr := borrowedReplacementObservationWindowRun(ctx, t, b, fresh, &borrowedReplacementObservationWindowPlan{
		preRelease: func(_ context.Context, observer *borrowedReplacementObservationWindowObserver) error {
			observer.cancelPending()
			return nil
		},
	})
	if runErr != nil {
		t.Fatalf("observation window cancel orchestration refused: %v", runErr)
	}
	if outcome.useErr == nil {
		t.Fatal("caller cancel published a successful use")
	}
	if outcome.probeExecutions != 0 {
		t.Fatalf("caller cancel executed the probe %d times, want 0", outcome.probeExecutions)
	}
	if outcome.state.compositePublished() {
		t.Fatal("caller cancel published the composite")
	}
	borrowedSuccessorCloseConn(t, outcome.conn)
	t.Logf("caller cancel at the pre-probe park: the use refused with zero probe executions and no composite publication")
}

// borrowedReplacementObservationWindowUnknownRefuse proves incomplete and
// timed-out observations permanently latch the loss and never yield composite
// success (UNKNOWN is never success).
func borrowedReplacementObservationWindowUnknownRefuse(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding) {
	t.Helper()
	for _, kind := range []string{"incomplete", "timeout"} {
		kind := kind
		outcome, runErr := borrowedReplacementObservationWindowRun(ctx, t, b, fresh, &borrowedReplacementObservationWindowPlan{
			preRelease: func(preCtx context.Context, observer *borrowedReplacementObservationWindowObserver) error {
				if kind == "incomplete" {
					observer.observeFn = func(context.Context) error {
						return errors.New("injected incomplete observation")
					}
				} else {
					observer.observeFn = func(observeCtx context.Context) error {
						<-observeCtx.Done()
						return errors.New("injected observation timeout")
					}
				}
				checkCtx, cancelCheck := context.WithTimeout(preCtx, 5*time.Second)
				defer cancelCheck()
				return observer.requestCheckpoint(checkCtx)
			},
		})
		if runErr != nil {
			t.Fatalf("observation window %s orchestration refused: %v", kind, runErr)
		}
		if outcome.preErr == nil {
			t.Fatalf("injected %s observation was accepted", kind)
		}
		if outcome.probeExecutions != 0 {
			t.Fatalf("injected %s observation executed the probe %d times, want 0", kind, outcome.probeExecutions)
		}
		if outcome.state.lossReasonNow() == "" {
			t.Fatalf("injected %s observation did not permanently latch the loss", kind)
		}
		if outcome.state.compositePublished() {
			t.Fatalf("injected %s observation published the composite", kind)
		}
		borrowedSuccessorCloseConn(t, outcome.conn)
	}
	t.Logf("incomplete/timeout observation negative: both permanently latched the loss and never yielded composite success")
}

// borrowedReplacementObservationWindowLossAfterInspectionRefuse proves an
// actual observer loss after the final inspection and before publication is
// awaited through the acknowledgement latch and refuses the composite even
// though the owner transaction, the use and the after-return rechecks all
// succeeded.
func borrowedReplacementObservationWindowLossAfterInspectionRefuse(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding) {
	t.Helper()
	outcome, runErr := borrowedReplacementObservationWindowRun(ctx, t, b, fresh, &borrowedReplacementObservationWindowPlan{
		checkpointAtPostProbe: true,
		afterInspection: func(observer *borrowedReplacementObservationWindowObserver) {
			borrowedReplacementObservationWindowCloseObserverConn(t, observer.conn)
		},
	})
	if runErr != nil {
		t.Fatalf("observation window loss-after-inspection orchestration refused: %v", runErr)
	}
	if outcome.ownerErr != nil || outcome.useErr != nil {
		t.Fatalf("loss-after-inspection prerequisites were expected to succeed: owner=%v use=%v", outcome.ownerErr, outcome.useErr)
	}
	if outcome.inspectionErr != nil {
		t.Fatalf("loss-after-inspection final inspection was expected to succeed: %v", outcome.inspectionErr)
	}
	if outcome.ackErr == nil {
		t.Fatal("loss after the final inspection was not detected by the acknowledgement checkpoint")
	}
	if outcome.state.lossReasonNow() == "" {
		t.Fatal("loss after the final inspection did not permanently latch the observed loss")
	}
	if outcome.decisionErr == nil {
		t.Fatal("loss after the final inspection still published the composite decision")
	}
	if outcome.state.compositePublished() {
		t.Fatal("loss after the final inspection published the composite")
	}
	if outcome.probeExecutions != 1 {
		t.Fatalf("loss after the final inspection probe executions=%d, want exactly 1", outcome.probeExecutions)
	}
	borrowedSuccessorCloseConn(t, outcome.conn)
	t.Logf("loss after the final inspection before publication: the acknowledgement latch refused the composite despite the owner transaction, the use and the rechecks succeeding (probe executions=%d)", outcome.probeExecutions)
}

// borrowedReplacementObservationWindowLatchedCopyRefuse proves a latched loss
// is copy-shared and can never be rehabilitated by copies or reconstructions,
// and that a latched state can never publish the composite.
func borrowedReplacementObservationWindowLatchedCopyRefuse(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding) {
	t.Helper()
	outcome, runErr := borrowedReplacementObservationWindowRun(ctx, t, b, fresh, &borrowedReplacementObservationWindowPlan{
		preRelease: func(preCtx context.Context, observer *borrowedReplacementObservationWindowObserver) error {
			observer.observeFn = func(context.Context) error {
				return errors.New("injected copy-shared loss observation")
			}
			checkCtx, cancelCheck := context.WithTimeout(preCtx, 5*time.Second)
			defer cancelCheck()
			return observer.requestCheckpoint(checkCtx)
		},
	})
	if runErr != nil {
		t.Fatalf("observation window latched-copy orchestration refused: %v", runErr)
	}
	if outcome.state.lossReasonNow() == "" {
		t.Fatal("latched-copy negative did not latch the loss")
	}
	stateCopy := outcome.state
	if stateCopy.lossReasonNow() == "" {
		t.Fatal("latched loss is not copy-shared")
	}
	if err := stateCopy.publishDecision(ctx, nil); err == nil {
		t.Fatal("latched state copy published the composite")
	}
	copied := *outcome.reg
	if copied.Use(ctx) == nil {
		t.Fatal("latched-copy registration copy rehabilitated the session")
	}
	if _, reconErr := newBorrowedReplacementSessionRegistration(t, ctx, b, fresh, outcome.conn); reconErr == nil {
		t.Fatal("latched-copy reconstruction minted a fresh session registration")
	}
	borrowedSuccessorCloseConn(t, outcome.conn)
	t.Logf("latched-copy negative: the loss latch is copy-shared, a latched state can never publish the composite and registration copies/reconstructions refuse")
}

// borrowedReplacementObservationWindowPhaseTimeoutRefuse proves a checkpoint
// phase-context timeout while the observation is still pending is UNKNOWN: the
// loss is latched, the pending work is canceled and no later success is usable.
func borrowedReplacementObservationWindowPhaseTimeoutRefuse(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding) {
	t.Helper()
	outcome, runErr := borrowedReplacementObservationWindowRun(ctx, t, b, fresh, &borrowedReplacementObservationWindowPlan{
		preRelease: func(preCtx context.Context, observer *borrowedReplacementObservationWindowObserver) error {
			observer.observeFn = func(context.Context) error {
				// The injected observation deliberately ignores its phase
				// context and would "succeed" later.
				time.Sleep(1500 * time.Millisecond)
				return nil
			}
			checkCtx, cancelCheck := context.WithTimeout(preCtx, 200*time.Millisecond)
			defer cancelCheck()
			return observer.requestCheckpoint(checkCtx)
		},
	})
	if runErr != nil {
		t.Fatalf("observation window phase-timeout orchestration refused: %v", runErr)
	}
	if outcome.preErr == nil {
		t.Fatal("checkpoint phase-context timeout was not refused")
	}
	if outcome.probeExecutions != 0 {
		t.Fatalf("checkpoint phase-context timeout executed the probe %d times, want 0", outcome.probeExecutions)
	}
	if outcome.state.lossReasonNow() == "" {
		t.Fatal("checkpoint phase-context timeout did not permanently latch the loss")
	}
	if outcome.state.compositePublished() {
		t.Fatal("checkpoint phase-context timeout published the composite")
	}
	if outcome.terminationUnknown {
		t.Fatalf("phase-timeout observer termination was expected to be proven: %v", runErr)
	}
	if err := outcome.state.publishDecision(ctx, nil); err == nil {
		t.Fatal("a later decision succeeded after the phase-timeout latch")
	}
	borrowedSuccessorCloseConn(t, outcome.conn)
	t.Logf("checkpoint phase-context timeout: UNKNOWN latched and pending work canceled; no later success and no composite publication")
}

// borrowedReplacementObservationWindowDecisionCancelRefuse proves an ACTUAL
// caller-context cancellation of the RUN context at the final publication
// latches the loss, seals the interval and refuses the composite even though
// the owner transaction, the use and every checkpoint succeeded.
func borrowedReplacementObservationWindowDecisionCancelRefuse(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding) {
	t.Helper()
	outcome, runErr := borrowedReplacementObservationWindowRun(ctx, t, b, fresh, &borrowedReplacementObservationWindowPlan{
		checkpointAtPostProbe: true,
		beforeDecision: func(cancelRun context.CancelFunc) {
			cancelRun()
		},
	})
	if runErr != nil {
		t.Fatalf("observation window decision-cancel orchestration refused: %v", runErr)
	}
	if outcome.ownerErr != nil || outcome.useErr != nil || outcome.inspectionErr != nil || outcome.ackErr != nil {
		t.Fatalf("decision-cancel prerequisites were expected to succeed: owner=%v use=%v inspection=%v ack=%v",
			outcome.ownerErr, outcome.useErr, outcome.inspectionErr, outcome.ackErr)
	}
	if outcome.decisionErr == nil {
		t.Fatal("caller-context cancellation at the final publication still published the composite")
	}
	if outcome.state.compositePublished() {
		t.Fatal("caller-context cancellation at the final publication published the composite")
	}
	if outcome.state.lossReasonNow() == "" {
		t.Fatal("caller-context cancellation at the final publication did not latch the loss")
	}
	if !outcome.state.sealedNow() {
		t.Fatal("caller-context cancellation at the final publication did not seal the interval")
	}
	if outcome.probeExecutions != 1 {
		t.Fatalf("decision-cancel probe executions=%d, want exactly 1", outcome.probeExecutions)
	}
	borrowedSuccessorCloseConn(t, outcome.conn)
	t.Logf("caller-context cancellation at the final publication: the loss latched, the interval sealed and the composite was refused despite owner/use/checkpoints succeeding (probe executions=%d)", outcome.probeExecutions)
}

// borrowedReplacementObservationWindowConcurrentDecisionRefuse proves the
// terminal decision-plus-seal is atomic: racing callers resolve to exactly one
// publication and every later decision is rejected; a loss raced against the
// decision never yields a composite success.
func borrowedReplacementObservationWindowConcurrentDecisionRefuse(t *testing.T, ctx context.Context) {
	t.Helper()
	state := &borrowedReplacementObservationWindowState{}
	results := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	for i := 0; i < 2; i++ {
		go func() {
			defer wg.Done()
			results <- state.publishDecision(ctx, nil)
		}()
	}
	joined := make(chan struct{})
	go func() { wg.Wait(); close(joined) }()
	joinedNow := false
	t.Cleanup(func() {
		if joinedNow {
			return
		}
		select {
		case <-joined:
		case <-time.After(30 * time.Second):
			t.Errorf("concurrent decision join is unknown after the bounded join")
		}
	})
	select {
	case <-joined:
		joinedNow = true
	case <-time.After(30 * time.Second):
		t.Fatalf("concurrent decision did not complete within the bounded join")
	}
	published, refused := 0, 0
	for i := 0; i < 2; i++ {
		if err := <-results; err == nil {
			published++
		} else {
			refused++
		}
	}
	if published != 1 || refused != 1 {
		t.Fatalf("concurrent decision outcomes: published=%d refused=%d, want exactly one each", published, refused)
	}
	if !state.compositePublished() || !state.sealedNow() {
		t.Fatal("concurrent decision did not publish+seal exactly once")
	}
	// A loss raced against a fresh decision: the loss is classified under the
	// SAME mutex as the decision-plus-seal, so exactly ONE terminal outcome
	// holds (composite XOR lost), never both and never neither.
	state2 := &borrowedReplacementObservationWindowState{}
	lossDone := make(chan struct{})
	go func() {
		defer close(lossDone)
		state2.latchPreSealLoss("raced loss")
	}()
	lossJoined := false
	t.Cleanup(func() {
		if lossJoined {
			return
		}
		select {
		case <-lossDone:
		case <-time.After(30 * time.Second):
			t.Errorf("raced loss goroutine join is unknown after the bounded cleanup join")
		}
	})
	decisionErr := state2.publishDecision(ctx, nil)
	select {
	case <-lossDone:
		lossJoined = true
	case <-time.After(30 * time.Second):
		t.Errorf("raced loss goroutine did not complete within the bounded join; outcome unknown")
		return
	}
	if !state2.sealedNow() {
		t.Fatal("raced loss/decision did not seal the terminal transition")
	}
	composite := state2.compositePublished()
	lost := state2.lostNow()
	if composite == lost {
		t.Fatalf("raced loss/decision terminal state is contradictory or empty: composite=%t lost=%t", composite, lost)
	}
	if decisionErr == nil && !composite {
		t.Fatal("raced decision reported success without the composite")
	}
	if decisionErr != nil && composite {
		t.Fatal("raced decision failure still produced a composite success")
	}
	if err := state2.publishDecision(ctx, nil); err == nil {
		t.Fatal("a subsequent decision after the terminal transition was accepted")
	}
	// Deterministic ordering 1: loss THEN publish -> no composite success.
	lossFirst := &borrowedReplacementObservationWindowState{}
	if !lossFirst.latchPreSealLoss("pre-seal loss") {
		t.Fatal("pre-seal loss was not latched before any decision")
	}
	if err := lossFirst.publishDecision(ctx, nil); err == nil {
		t.Fatal("loss-then-publish ordering still published the composite")
	}
	if lossFirst.compositePublished() || !lossFirst.lostNow() || !lossFirst.sealedNow() {
		t.Fatal("loss-then-publish ordering terminal state is inconsistent")
	}
	// Deterministic ordering 2: publish THEN loss -> the terminal result is
	// preserved (post-interval events never contradict it).
	publishFirst := &borrowedReplacementObservationWindowState{}
	if err := publishFirst.publishDecision(ctx, nil); err != nil {
		t.Fatalf("publish-then-loss ordering decision refused: %v", err)
	}
	if publishFirst.latchPreSealLoss("post-interval loss") {
		t.Fatal("post-interval loss was latched over the preserved terminal result")
	}
	if !publishFirst.compositePublished() || publishFirst.lostNow() {
		t.Fatal("publish-then-loss ordering did not preserve the composite terminal result")
	}
	t.Logf("concurrent decision controls: racing callers resolved to exactly one publication; the raced loss is classified under the same mutex with exactly one terminal outcome; loss-then-publish refuses and publish-then-loss preserves the terminal result; subsequent decisions are rejected")
}

// borrowedReplacementObservationWindowCancellationDuringObservationRefuse
// proves a cancellation during a LIVE checkpoint observation is acknowledged:
// the observation does not return nil success, the request phase outcome is
// UNKNOWN and latched, and no later success is usable.
func borrowedReplacementObservationWindowCancellationDuringObservationRefuse(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding) {
	t.Helper()
	var captured *borrowedReplacementObservationWindowObserver
	var checkpointRefusal error
	outcome, runErr := borrowedReplacementObservationWindowRun(ctx, t, b, fresh, &borrowedReplacementObservationWindowPlan{
		preRelease: func(preCtx context.Context, observer *borrowedReplacementObservationWindowObserver) error {
			captured = observer
			entered := make(chan struct{})
			var enteredOnce sync.Once
			observer.observeFn = func(observeCtx context.Context) error {
				enteredOnce.Do(func() { close(entered) })
				// The live observation only ends through the combined phase +
				// observer-lifetime context.
				select {
				case <-observeCtx.Done():
					return errors.New("injected live observation canceled")
				case <-time.After(30 * time.Second):
					return nil
				}
			}
			done := make(chan error, 1)
			checkCtx, cancelCheck := context.WithTimeout(preCtx, 60*time.Second)
			go func() {
				done <- observer.requestCheckpoint(checkCtx)
			}()
			joined := false
			t.Cleanup(func() {
				cancelCheck()
				if joined {
					return
				}
				select {
				case <-done:
				case <-time.After(30 * time.Second):
					t.Errorf("cancellation-during-observation checkpoint join is unknown after the bounded cleanup join")
				}
			})
			select {
			case <-entered:
			case <-time.After(60 * time.Second):
				cancelCheck()
				t.Fatalf("cancellation-during-observation checkpoint never entered the live observation (control failure)")
			}
			// Cancel the observer while the observation is live: the combined
			// context ends and the observation must NOT return nil success. An
			// unexpected stop failure is a CONTROL failure, never the expected
			// observation refusal.
			if stopErr := observer.stopBounded(); stopErr != nil {
				cancelCheck()
				t.Fatalf("cancellation-during-observation observer stop unexpectedly failed: %v", stopErr)
			}
			select {
			case checkpointRefusal = <-done:
				joined = true
			case <-time.After(30 * time.Second):
				cancelCheck()
				t.Fatalf("cancellation-during-observation checkpoint did not complete within the bounded join (control failure)")
			}
			cancelCheck()
			if checkpointRefusal == nil {
				t.Fatalf("cancellation-during-observation checkpoint acknowledgement was nil (control failure, not the expected observation refusal)")
			}
			return checkpointRefusal
		},
	})
	if runErr != nil {
		t.Fatalf("cancellation-during-observation orchestration refused: %v", runErr)
	}
	if captured == nil {
		t.Fatal("cancellation-during-observation observer was not captured")
	}
	// The captured checkpoint result itself is the proven non-nil refusal.
	if checkpointRefusal == nil {
		t.Fatal("cancellation-during-observation captured checkpoint result was nil")
	}
	if outcome.preErr == nil {
		t.Fatal("cancellation during a live observation was not refused")
	}
	if outcome.probeExecutions != 0 {
		t.Fatalf("cancellation during a live observation executed the probe %d times, want 0", outcome.probeExecutions)
	}
	if outcome.state.lossReasonNow() == "" {
		t.Fatal("cancellation during a live observation did not permanently latch the loss")
	}
	if outcome.state.compositePublished() {
		t.Fatal("cancellation during a live observation published the composite")
	}
	if err := outcome.state.publishDecision(ctx, nil); err == nil {
		t.Fatal("a later decision succeeded after the cancellation-during-observation latch")
	}
	borrowedSuccessorCloseConn(t, outcome.conn)
	t.Logf("cancellation during a live observation: the captured checkpoint result is a PROVEN non-nil refusal (%v), the request phase outcome is UNKNOWN and latched, and no later success is usable", checkpointRefusal)
}

// borrowedReplacementObservationWindowPostSealCheckpointRefuse proves an ACTUAL
// checkpoint refusal after successful publication and observer shutdown
// preserves the terminal result: the real requestCheckpoint path must NOT set
// lost=true and the composite stays published.
func borrowedReplacementObservationWindowPostSealCheckpointRefuse(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding) {
	t.Helper()
	outcome, runErr := borrowedReplacementObservationWindowRun(ctx, t, b, fresh, &borrowedReplacementObservationWindowPlan{checkpointAtPostProbe: true})
	if runErr != nil {
		t.Fatalf("observation window post-seal orchestration refused: %v", runErr)
	}
	if !outcome.state.compositePublished() || !outcome.state.sealedNow() || outcome.state.lostNow() {
		t.Fatalf("post-seal control requires the published terminal state: composite=%t sealed=%t lost=%t",
			outcome.state.compositePublished(), outcome.state.sealedNow(), outcome.state.lostNow())
	}
	// ACTUAL post-seal checkpoint refusal on the real path: the observer is
	// already stopped, so requestCheckpoint refuses through the stopped path.
	checkCtx, cancelCheck := context.WithTimeout(ctx, 5*time.Second)
	checkErr := outcome.observer.requestCheckpoint(checkCtx)
	cancelCheck()
	if checkErr == nil {
		t.Fatal("post-seal checkpoint unexpectedly succeeded")
	}
	if outcome.state.lostNow() {
		t.Fatal("post-seal checkpoint refusal set lost=true over the composite terminal result")
	}
	if !outcome.state.compositePublished() || !outcome.state.sealedNow() {
		t.Fatal("post-seal checkpoint refusal changed the terminal state")
	}
	borrowedSuccessorCloseConn(t, outcome.conn)
	t.Logf("post-seal checkpoint refusal: the real requestCheckpoint path preserved the terminal result (composite stays true, lost stays false)")
}

// TestBorrowedReplacementObservationWindow is the bounded independent session
// observation across the owner window lane described in the file header.
func TestBorrowedReplacementObservationWindow(t *testing.T) {
	ctx := t.Context()

	// Genuine replacement capture: the actual bounded shared orchestration with
	// stage counters; never a projection.
	b := newBorrowedSuccessorBaseline(t, ctx)
	originalTargetDSN := b.fixture.writerTargetDSN
	originalObserverDSN := b.fixture.observerTargetDSN
	var stages int32
	orchestrateCtx, cancelOrchestrate := borrowedReplacementOrchestrationContext(ctx)
	fresh, err := borrowedReplacementBindingOrchestrate(orchestrateCtx, t, b, &stages)
	cancelOrchestrate()
	if err != nil {
		t.Fatalf("observation window genuine capture refused: %v", err)
	}
	if fresh == nil {
		t.Fatal("observation window genuine capture produced no capture")
	}
	if b.fixture.writerTargetDSN != originalTargetDSN || b.fixture.observerTargetDSN != originalObserverDSN {
		t.Fatal("observation window lane rewrote an original fixture DSN")
	}
	if got := atomic.LoadInt32(&stages); got != borrowedReplacementStagePrefixCaptured {
		t.Fatalf("observation window pipeline stages=%d, want %d", got, borrowedReplacementStagePrefixCaptured)
	}
	if fresh.binding.TargetDatabaseOID() != fresh.replacementOID || fresh.replacementOID == fresh.oldOID {
		t.Fatalf("observation window capture is not replacement-bound: replacement=%d old=%d binding=%d", fresh.replacementOID, fresh.oldOID, fresh.binding.TargetDatabaseOID())
	}
	t.Logf("observation window genuine capture: old OID %d -> replacement OID %d", fresh.oldOID, fresh.replacementOID)

	// Positive: readiness, acknowledged post-probe checkpoint, composite,
	// interval seal, observer join and retirement.
	borrowedReplacementObservationWindowPositive(t, ctx, b, fresh)

	// Non-destructive negatives on the same fixture.
	borrowedReplacementObservationWindowBackendLossRefuse(t, ctx, b, fresh)
	borrowedReplacementObservationWindowObserverConnLossRefuse(t, ctx, b, fresh)
	borrowedReplacementObservationWindowCancelRefuse(t, ctx, b, fresh)
	borrowedReplacementObservationWindowUnknownRefuse(t, ctx, b, fresh)
	borrowedReplacementObservationWindowLossAfterInspectionRefuse(t, ctx, b, fresh)
	borrowedReplacementObservationWindowLatchedCopyRefuse(t, ctx, b, fresh)
	borrowedReplacementObservationWindowPhaseTimeoutRefuse(t, ctx, b, fresh)
	borrowedReplacementObservationWindowCancellationDuringObservationRefuse(t, ctx, b, fresh)
	borrowedReplacementObservationWindowPostSealCheckpointRefuse(t, ctx, b, fresh)
	borrowedReplacementObservationWindowDecisionCancelRefuse(t, ctx, b, fresh)
	borrowedReplacementObservationWindowConcurrentDecisionRefuse(t, ctx)

	// Copied-prefix invalidation LAST on this fixture.
	borrowedReplacementObservationWindowPrefixLossRefuse(t, ctx, b, fresh)

	// Destructive negative on a fresh fixture.
	borrowedReplacementObservationWindowOwnerLossRefuse(t, ctx)

	// Guard unresolved and acceptance unchanged across the whole lane.
	borrowedOwnerDDLGuard(t, ctx, b)
}
