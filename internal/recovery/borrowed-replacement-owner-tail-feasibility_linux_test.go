//go:build linux && drill

// borrowed-replacement-owner-tail-feasibility_linux_test.go is the bounded
// owner-held final eligibility checkpoint lane. It composes the existing
// machinery without duplicating any owner transaction: the genuine autonomous
// baseline/capture, the observation flow (P1 registration, parked Use, complete
// observer readiness, owner-fenced SHARE window, synchronized terminal
// decision), the autonomous pump and the real in-window sample wait. This lane
// adds ONE new-file-local owner-tail step invoked through the two narrow
// nil-default test-only glue hooks (the catalog-window tail hook, invoked AFTER
// the proven Use completion and the successful supplied-tx facts recheck but
// BEFORE the owner callback returns, and the observation use-result adapter,
// which reads the ACTUAL useErr only after the channel-confirmed completion).
// The tail performs a fresh acknowledged observation, verifies the actual
// supplied-tx owner identity and the held catalog fence, and arbitrates the
// caller context and the shared observed loss under one mutex with NO I/O under
// it, returning PROVISIONAL eligibility only: no capability, no receipt, no
// guard update and no terminal seal. The unchanged flow then commits, rechecks,
// decides and joins; retirement happens only after all completion proofs with
// the guard unresolved and acceptance zero. There is no continuous exclusion,
// no controlled admission, no restore, no probe beyond the session's single
// fixed SELECT 1, no receipt authority, no clean transition, no atomic
// acceptance, no manifest, no downstream and no Gate1 authority. Secrets,
// verifiers and DSNs are never logged.
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

// Owner-tail refusal stage sentinels: controls capture and assert the ACTUAL
// refusal stage/result instead of any non-nil error.
var (
	errOwnerTailUseUnproven = errors.New("owner-tail use result is not proven")
	errOwnerTailUseFailed   = errors.New("owner-tail channel-confirmed use failed")
	errOwnerTailFacts       = errors.New("owner-tail final supplied-tx facts verification refused")
	errOwnerTailArbitration = errors.New("owner-tail arbitration refused")
)

// borrowedReplacementOwnerTailState is the one-run provisional eligibility
// state: eligibility is granted at most once, consumed at most once, and copies
// share the same state pointer (never reusable authority).
type borrowedReplacementOwnerTailState struct {
	mu                  sync.Mutex
	tailInvocations     int64
	adapterInvocations  int64
	eligibility         bool
	eligibilityNonce    int64
	eligibilityConsumed bool
	recorded            bool
	useErr              error
}

func (s *borrowedReplacementOwnerTailState) tailInvocationsNow() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tailInvocations
}

func (s *borrowedReplacementOwnerTailState) eligibleNow() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.eligibility
}

// consumeEligibility is the one-shot consumption of the provisional
// eligibility: the first consume succeeds, every later consume (including from
// a copy sharing this state) refuses.
func (s *borrowedReplacementOwnerTailState) consumeEligibility() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.eligibility || s.eligibilityConsumed {
		return false
	}
	s.eligibilityConsumed = true
	return true
}

func (s *borrowedReplacementOwnerTailState) eligibilityNonceNow() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.eligibilityNonce
}

func (s *borrowedReplacementOwnerTailState) useErrNow() (error, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.useErr, s.recorded
}

func (s *borrowedReplacementOwnerTailState) adapterInvocationsNow() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.adapterInvocations
}

// borrowedReplacementOwnerTailHooks is the test-only injection plan of one
// owner-tail run.
type borrowedReplacementOwnerTailHooks struct {
	ownerPID         int
	ownerStart       time.Time
	expectedVerifier string
	expectedState    borrowedOwnerRotationRoleState
	// useWorkerStall is the test-only bounded worker stall (invoked after the
	// Use worker returns and before the completion channel closes).
	useWorkerStall func(ctx context.Context)
	// useJoinBound overrides the Use join bound for the stall control.
	useJoinBound time.Duration
	preRelease   func(ctx context.Context, pump *borrowedReplacementAutonomousPump, cancelLane func()) error
	postProbe    func(ctx context.Context, pump *borrowedReplacementAutonomousPump, cancelLane func()) error
	// atTail runs after the identity/fence checks and BEFORE the final
	// supplied-tx facts verification (late-facts injection).
	atTail func(ctx context.Context, pump *borrowedReplacementAutonomousPump, cancelLane func(), tx pgx.Tx) error
	// atArbitration runs after the successful final facts verification and
	// IMMEDIATELY before the shared-mutex arbitration (loss/cancel injection).
	atArbitration func(ctx context.Context, pump *borrowedReplacementAutonomousPump, cancelLane func(), tx pgx.Tx) error
	adapter       func(useErr error, pump *borrowedReplacementAutonomousPump, cancelLane func()) error
}

// borrowedReplacementOwnerTailOutcome preserves the flow outcome, the pump and
// the explicit join results.
type borrowedReplacementOwnerTailOutcome struct {
	flow        *borrowedReplacementObservationWindowOutcome
	runErr      error
	pump        *borrowedReplacementAutonomousPump
	pumpJoinErr error
	state       *borrowedReplacementOwnerTailState
}

// tail is the owner-tail step: it increments the invocation counter, performs a
// fresh acknowledged observation, verifies the actual supplied-tx owner
// identity and held catalog fence, runs the optional injection, and arbitrates
// the caller context and the observed loss under the shared mutex with NO I/O
// under it, returning PROVISIONAL eligibility only.
func borrowedReplacementOwnerTailStep(tailCtx context.Context, tx pgx.Tx, pump *borrowedReplacementAutonomousPump, lane *borrowedReplacementOwnerTailState, hooks *borrowedReplacementOwnerTailHooks, cancelLane func()) error {
	if pump == nil {
		return errors.New("owner-tail requires the running pump")
	}
	lane.mu.Lock()
	lane.tailInvocations++
	lane.mu.Unlock()
	// P2-B1: channel-confirm and inspect the ACTUAL Use result INSIDE the owner
	// window BEFORE any eligibility payload; a non-nil result refuses.
	lane.mu.Lock()
	recorded := lane.recorded
	useErr := lane.useErr
	lane.mu.Unlock()
	if !recorded {
		return fmt.Errorf("%w: the completion channel was not confirmed", errOwnerTailUseUnproven)
	}
	if useErr != nil {
		return fmt.Errorf("%w: the channel-confirmed use failed", errOwnerTailUseFailed)
	}
	// Fresh acknowledged observation through the independently owned observer.
	obsCtx, cancelObs := context.WithTimeout(tailCtx, 60*time.Second)
	obsErr := pump.observer.requestCheckpoint(obsCtx)
	cancelObs()
	if obsErr != nil {
		return errors.New("owner-tail fresh acknowledged observation refused")
	}
	// Actual supplied-tx owner identity + held catalog fence.
	var (
		pid   int
		start time.Time
	)
	identityCtx, cancelIdentity := context.WithTimeout(tailCtx, borrowedOwnerRotationQueryBudget)
	identityErr := tx.QueryRow(identityCtx, `SELECT pid::int, backend_start FROM pg_stat_activity WHERE pid = pg_backend_pid()`).Scan(&pid, &start)
	cancelIdentity()
	if identityErr != nil || pid != hooks.ownerPID || !start.Equal(hooks.ownerStart) {
		return errors.New("owner-tail supplied-tx owner identity changed")
	}
	var shareHeld bool
	fenceCtx, cancelFence := context.WithTimeout(tailCtx, borrowedOwnerRotationQueryBudget)
	fenceErr := tx.QueryRow(fenceCtx, `
SELECT EXISTS (
  SELECT 1 FROM pg_locks l
  JOIN pg_class c ON c.oid = l.relation
  WHERE l.locktype = 'relation' AND l.mode = 'ShareLock' AND l.granted AND l.pid = pg_backend_pid()
    AND c.relname = 'pg_authid' AND c.relnamespace = 'pg_catalog'::regnamespace
)`).Scan(&shareHeld)
	cancelFence()
	if fenceErr != nil || !shareHeld {
		return errors.New("owner-tail catalog fence is not held through the supplied tx")
	}
	if hooks.atTail != nil {
		if err := hooks.atTail(tailCtx, pump, cancelLane, tx); err != nil {
			return err
		}
	}
	// P2-B3.1: lane-side FINAL supplied-tx facts verification AFTER the initial
	// window check and the optional injection; this is the late boundary.
	if err := borrowedReplacementCatalogWindowValidateFacts(tailCtx, tx, pump.observer.fixture, hooks.expectedVerifier, hooks.expectedState); err != nil {
		return fmt.Errorf("%w: the final supplied-tx facts verification refused", errOwnerTailFacts)
	}
	// P2-3: loss/cancel injections run AFTER the successful final facts
	// verification and IMMEDIATELY before the arbitration, so the arbitration
	// itself must execute and decide.
	if hooks.atArbitration != nil {
		if err := hooks.atArbitration(tailCtx, pump, cancelLane, tx); err != nil {
			return err
		}
	}
	// P2-B2: hold the SHARED observation-loss mutex across the loss/context
	// checks AND the provisional eligibility write (consistent lock order
	// loss.mu -> lane.mu, NO I/O under it, and the locking lossReasonNow getter
	// is never called while holding it).
	lossMu := &pump.state.mu
	lossMu.Lock()
	if err := tailCtx.Err(); err != nil {
		lossMu.Unlock()
		return fmt.Errorf("%w: the caller context ended before the eligibility decision", errOwnerTailArbitration)
	}
	if pump.state.lost {
		lossMu.Unlock()
		return fmt.Errorf("%w: the observed loss is permanently latched", errOwnerTailArbitration)
	}
	lane.mu.Lock()
	if lane.eligibility {
		lane.mu.Unlock()
		lossMu.Unlock()
		return fmt.Errorf("%w: eligibility was already granted for this run", errOwnerTailArbitration)
	}
	lane.eligibility = true
	lane.eligibilityNonce++
	lane.mu.Unlock()
	lossMu.Unlock()
	return nil
}

// borrowedReplacementOwnerTailRun composes the observation flow with the
// autonomous pump and the owner-tail glue seam: the pump is started in
// preRelease with its first ack awaited, the real in-window sample wait runs in
// postProbe, the observation use-result adapter consumes the channel-confirmed
// useErr, and the window tail hook is installed for the run and cleared after.
func borrowedReplacementOwnerTailRun(ctx context.Context, t *testing.T, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding, hooks *borrowedReplacementOwnerTailHooks) *borrowedReplacementOwnerTailOutcome {
	t.Helper()
	if hooks == nil {
		hooks = &borrowedReplacementOwnerTailHooks{}
	}
	laneCtx, cancelLane := context.WithCancel(ctx)
	defer cancelLane()
	lane := &borrowedReplacementOwnerTailState{}
	outcome := &borrowedReplacementOwnerTailOutcome{state: lane}
	var pump *borrowedReplacementAutonomousPump
	plan := &borrowedReplacementObservationWindowPlan{
		useResultRecorder: func(useErr error) {
			lane.mu.Lock()
			lane.recorded = true
			lane.useErr = useErr
			lane.mu.Unlock()
		},
		useWorkerStall: hooks.useWorkerStall,
		useJoinBound:   hooks.useJoinBound,
		preRelease: func(preCtx context.Context, observer *borrowedReplacementObservationWindowObserver) error {
			pump = startBorrowedReplacementAutonomousPump(t, laneCtx, observer, cancelLane, nil)
			outcome.pump = pump
			select {
			case <-pump.firstAck:
			case <-pump.done:
				return errors.New("owner-tail pump terminated before its first ack")
			case <-preCtx.Done():
				return errors.New("owner-tail pump first ack did not arrive before the owner window")
			}
			if hooks.preRelease != nil {
				return hooks.preRelease(preCtx, pump, cancelLane)
			}
			return nil
		},
		postProbe: func(postProbeCtx context.Context) error {
			if pump == nil {
				return errors.New("owner-tail pump was not started")
			}
			// Retain the real in-window autonomous sample wait.
			if err := borrowedReplacementAutonomousAwaitSamples(postProbeCtx, pump, cancelLane, 2, 60*time.Second); err != nil {
				return err
			}
			if hooks.postProbe != nil {
				return hooks.postProbe(postProbeCtx, pump, cancelLane)
			}
			return nil
		},
		useResultAdapter: func(useErr error) error {
			lane.mu.Lock()
			lane.adapterInvocations++
			eligible := lane.eligibility
			lane.mu.Unlock()
			if useErr != nil {
				lane.mu.Lock()
				lane.eligibility = false
				lane.mu.Unlock()
				return errors.New("owner-tail adapter refused: the channel-confirmed use failed")
			}
			if !eligible {
				return errors.New("owner-tail adapter refused: no provisional tail eligibility")
			}
			if hooks.adapter != nil {
				return hooks.adapter(useErr, pump, cancelLane)
			}
			return nil
		},
	}
	tailHook := func(tailCtx context.Context, tx pgx.Tx) error {
		return borrowedReplacementOwnerTailStep(tailCtx, tx, pump, lane, hooks, cancelLane)
	}
	borrowedReplacementCatalogWindowTailHook.Store(&tailHook)
	defer borrowedReplacementCatalogWindowTailHook.Store(nil)
	outcome.flow, outcome.runErr = borrowedReplacementObservationWindowRun(laneCtx, t, b, fresh, plan)
	cancelLane()
	if pump != nil {
		outcome.pumpJoinErr = pump.stopBounded(30 * time.Second)
	}
	return outcome
}

// borrowedReplacementOwnerTailRetireGuarded retires only after all completion
// proofs; an UNKNOWN pump join or flow termination blocks retirement/reuse.
func borrowedReplacementOwnerTailRetireGuarded(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, outcome *borrowedReplacementOwnerTailOutcome) error {
	t.Helper()
	if outcome == nil || outcome.flow == nil || outcome.flow.reg == nil {
		return errors.New("owner-tail retirement refused: no flow outcome")
	}
	if outcome.pumpJoinErr != nil {
		return errors.New("owner-tail retirement refused: pump termination is UNKNOWN")
	}
	if outcome.flow.terminationUnknown {
		return errors.New("owner-tail retirement refused: flow termination is UNKNOWN")
	}
	borrowedReplacementSessionRetire(t, ctx, b, outcome.flow.conn, outcome.flow.reg.state)
	return nil
}

// borrowedReplacementOwnerTailReplayRefuse proves the composed session keeps
// one-use and any permanent UNKNOWN loss under the owner-tail composition.
func borrowedReplacementOwnerTailReplayRefuse(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding, outcome *borrowedReplacementOwnerTailOutcome, label string) {
	t.Helper()
	if outcome == nil || outcome.flow == nil || outcome.flow.reg == nil {
		t.Fatalf("%s: owner-tail replay control requires the concrete flow outcome", label)
	}
	replayCtx, cancelReplay := context.WithTimeout(ctx, 30*time.Second)
	replayErr := outcome.flow.reg.Use(replayCtx)
	copied := *outcome.flow.reg
	copyErr := copied.Use(replayCtx)
	cancelReplay()
	if replayErr == nil || copyErr == nil {
		t.Fatalf("%s: owner-tail replay/copy use was accepted", label)
	}
	if _, reconErr := newBorrowedReplacementSessionRegistration(t, ctx, b, fresh, outcome.flow.conn); reconErr == nil {
		t.Fatalf("%s: owner-tail reconstruction minted a fresh session registration", label)
	}
	if outcome.flow.state.lossReasonNow() != "" {
		stateCopy := outcome.flow.state
		if stateCopy.lossReasonNow() == "" {
			t.Fatalf("%s: owner-tail UNKNOWN loss is not copy-shared", label)
		}
		if err := stateCopy.publishDecision(ctx, nil); err == nil {
			t.Fatalf("%s: owner-tail loss copy published the composite", label)
		}
	}
	// The provisional eligibility is never reusable authority: any further
	// consume (including through the same shared state) refuses.
	if outcome.state.consumeEligibility() {
		t.Fatalf("%s: owner-tail eligibility was consumable again", label)
	}
	t.Logf("%s: replay/copy/reconstruction refused and the provisional eligibility is one-shot, never reusable authority", label)
}

// TestBorrowedReplacementOwnerTailFeasibility is the bounded owner-held final
// eligibility checkpoint lane described in the file header.
func TestBorrowedReplacementOwnerTailFeasibility(t *testing.T) {
	ctx := t.Context()

	// Positive: autonomous monitoring with the owner-tail provisional
	// eligibility, composite publication and guarded retirement.
	b, fresh := borrowedReplacementAutonomousBaseline(t, ctx)
	positive := borrowedReplacementOwnerTailRun(ctx, t, b, fresh, &borrowedReplacementOwnerTailHooks{
		ownerPID: b.owner.BackendPID, ownerStart: b.owner.BackendStart,
		expectedVerifier: b.verifierP1, expectedState: b.preState,
	})
	if positive.runErr != nil {
		t.Fatalf("owner-tail positive flow refused: %v", positive.runErr)
	}
	if positive.flow.ownerErr != nil || positive.flow.useErr != nil || positive.flow.prefixErr != nil || positive.flow.anchorErr != nil ||
		positive.flow.healthErr != nil || positive.flow.inspectionErr != nil || positive.flow.ackErr != nil || positive.flow.decisionErr != nil {
		t.Fatalf("owner-tail positive refused: owner=%v use=%v prefix=%v anchor=%v health=%v inspection=%v ack=%v decision=%v",
			positive.flow.ownerErr, positive.flow.useErr, positive.flow.prefixErr, positive.flow.anchorErr, positive.flow.healthErr, positive.flow.inspectionErr, positive.flow.ackErr, positive.flow.decisionErr)
	}
	if positive.pump == nil || positive.pumpJoinErr != nil {
		t.Fatalf("owner-tail positive pump state: pump=%v joinErr=%v", positive.pump, positive.pumpJoinErr)
	}
	if got := positive.state.tailInvocationsNow(); got != 1 {
		t.Fatalf("owner-tail positive invocations=%d, want exactly 1", got)
	}
	if got := positive.state.adapterInvocationsNow(); got != 1 {
		t.Fatalf("owner-tail positive adapter invocations=%d, want exactly 1", got)
	}
	if !positive.state.eligibleNow() {
		t.Fatal("owner-tail positive did not grant the provisional eligibility")
	}
	useErr, recorded := positive.state.useErrNow()
	if !recorded || useErr != nil {
		t.Fatalf("owner-tail positive channel-confirmed use result: recorded=%t err=%v", recorded, useErr)
	}
	if positive.flow.probeExecutions != 1 {
		t.Fatalf("owner-tail positive probe executions=%d, want exactly 1", positive.flow.probeExecutions)
	}
	if !positive.flow.state.compositePublished() || !positive.flow.state.sealedNow() || positive.flow.state.lostNow() {
		t.Fatalf("owner-tail positive terminal state: composite=%t sealed=%t lost=%t",
			positive.flow.state.compositePublished(), positive.flow.state.sealedNow(), positive.flow.state.lostNow())
	}
	if !positive.state.consumeEligibility() {
		t.Fatal("owner-tail positive provisional eligibility was not consumable once")
	}
	if positive.state.consumeEligibility() {
		t.Fatal("owner-tail positive provisional eligibility was consumable twice")
	}
	t.Logf("owner-tail positive: provisional eligibility granted once (nonce=%d) with the channel-confirmed use result, composite published, interval sealed, pump joined, probe executions=%d", positive.state.eligibilityNonceNow(), positive.flow.probeExecutions)
	borrowedReplacementOwnerTailReplayRefuse(t, ctx, b, fresh, positive, "post-composite")
	if err := borrowedReplacementOwnerTailRetireGuarded(t, ctx, b, positive); err != nil {
		t.Fatalf("owner-tail positive retirement refused: %v", err)
	}

	// N1 (P2-B1): REAL copied-prefix loss at the post-probe park (nil injection
	// return): the Use refuses publication, the tail inspects the
	// channel-confirmed failure and refuses BEFORE the eligibility payload,
	// while the identity/catalog stay intact.
	func() {
		b1, fresh1 := borrowedReplacementAutonomousBaseline(t, ctx)
		outcome := borrowedReplacementOwnerTailRun(ctx, t, b1, fresh1, &borrowedReplacementOwnerTailHooks{
			ownerPID: b1.owner.BackendPID, ownerStart: b1.owner.BackendStart,
			expectedVerifier: b1.verifierP1, expectedState: b1.preState,
			postProbe: func(_ context.Context, _ *borrowedReplacementAutonomousPump, _ func()) error {
				borrowedReplacementSessionCopiedPrefixLoss(t, fresh1)
				return nil
			},
		})
		if got := outcome.state.tailInvocationsNow(); got != 1 {
			t.Fatalf("owner-tail copied-prefix-loss invocations=%d, want exactly 1", got)
		}
		if outcome.state.eligibleNow() {
			t.Fatal("owner-tail copied-prefix-loss still granted eligibility")
		}
		useErr, recorded := outcome.state.useErrNow()
		if !recorded || useErr == nil {
			t.Fatalf("owner-tail copied-prefix-loss use result: recorded=%t err=%v, want a channel-confirmed failure", recorded, useErr)
		}
		if outcome.flow.state.compositePublished() {
			t.Fatal("owner-tail copied-prefix-loss published the composite")
		}
		// Identity/catalog intact: the registered backend is still present and
		// the committed P1 verifier/role facts are unchanged, proven through
		// the fixture administrator (the observer is already stopped).
		var present int
		rowCtx, cancelRow := context.WithTimeout(ctx, 5*time.Second)
		rowErr := b1.fixture.controlPool.QueryRow(rowCtx, `SELECT count(*) FROM pg_stat_activity WHERE pid=$1`, outcome.flow.reg.state.backendPID).Scan(&present)
		cancelRow()
		if rowErr != nil || present != 1 {
			t.Fatalf("owner-tail copied-prefix-loss damaged the registered identity: present=%d err=%v", present, rowErr)
		}
		catalog, catalogErr := borrowedSuccessorReadCatalogFacts(ctx, b1.fixture)
		if catalogErr != nil {
			t.Fatalf("owner-tail copied-prefix-loss catalog read refused: %v", catalogErr)
		}
		if catalog.verifier != b1.verifierP1 || !borrowedFenceGapRoleStateEqual(catalog, b1.preState) {
			t.Fatal("owner-tail copied-prefix-loss changed the committed catalog facts")
		}
		borrowedSuccessorCloseConn(t, outcome.flow.conn)
		t.Logf("copied-prefix-loss negative: the loss at the post-probe park made the Use refuse publication, the tail inspected the channel-confirmed failure and refused before any eligibility payload, and the identity/catalog stayed intact")
	}()

	// N2 (P2-B3.1): LATE supplied-tx facts refusal AFTER the initial check
	// succeeded (proves the final recheck, not an early refusal).
	func() {
		b2, fresh2 := borrowedReplacementAutonomousBaseline(t, ctx)
		initialCheckPassed := false
		outcome := borrowedReplacementOwnerTailRun(ctx, t, b2, fresh2, &borrowedReplacementOwnerTailHooks{
			ownerPID: b2.owner.BackendPID, ownerStart: b2.owner.BackendStart,
			expectedVerifier: b2.verifierP1, expectedState: b2.preState,
			postProbe: func(_ context.Context, _ *borrowedReplacementAutonomousPump, _ func()) error {
				initialCheckPassed = true
				return nil
			},
			atTail: func(tailCtx context.Context, _ *borrowedReplacementAutonomousPump, _ func(), tx pgx.Tx) error {
				mutateCtx, cancelMutate := context.WithTimeout(tailCtx, borrowedOwnerRotationQueryBudget)
				_, mutateErr := tx.Exec(mutateCtx, `ALTER ROLE `+pgx.Identifier{b2.fixture.writerRole}.Sanitize()+` PASSWORD `+sqlLiteral(b2.preState.verifier))
				cancelMutate()
				if mutateErr != nil {
					t.Fatalf("owner-tail late facts mutation refused: %v", mutateErr)
				}
				return nil
			},
		})
		if !initialCheckPassed {
			t.Fatal("owner-tail late-facts control never proved the initial facts check succeeded")
		}
		if outcome.flow.probeExecutions != 1 {
			t.Fatalf("owner-tail late-facts probe executions=%d, want exactly 1", outcome.flow.probeExecutions)
		}
		if got := outcome.state.tailInvocationsNow(); got != 1 {
			t.Fatalf("owner-tail late-facts invocations=%d, want exactly 1", got)
		}
		if outcome.state.eligibleNow() {
			t.Fatal("owner-tail late facts still granted eligibility")
		}
		if outcome.flow.ownerErr == nil {
			t.Fatal("owner-tail late supplied-tx facts change did not refuse the owner transaction")
		}
		if outcome.flow.state.compositePublished() {
			t.Fatal("owner-tail late supplied-tx facts refusal published the composite")
		}
		borrowedSuccessorCloseConn(t, outcome.flow.conn)
		t.Logf("late-facts negative: the initial facts check passed, the mutation landed at the tail, and the FINAL supplied-tx facts check refused with no eligibility and no composite")
	}()

	// N3 (P2-B3.2): at-tail registered-P1 loss FOLLOWED BY a nil injection
	// return: the REAL arbitration must refuse.
	func() {
		b3, fresh3 := borrowedReplacementAutonomousBaseline(t, ctx)
		outcome := borrowedReplacementOwnerTailRun(ctx, t, b3, fresh3, &borrowedReplacementOwnerTailHooks{
			ownerPID: b3.owner.BackendPID, ownerStart: b3.owner.BackendStart,
			expectedVerifier: b3.verifierP1, expectedState: b3.preState,
			atArbitration: func(tailCtx context.Context, pump *borrowedReplacementAutonomousPump, _ func(), _ pgx.Tx) error {
				var terminated bool
				termCtx, cancelTerm := context.WithTimeout(tailCtx, borrowedOwnerRotationQueryBudget)
				termErr := b3.fixture.controlPool.QueryRow(termCtx, `SELECT pg_terminate_backend($1)`, pump.observer.reg.state.backendPID).Scan(&terminated)
				cancelTerm()
				if termErr != nil || !terminated {
					t.Fatalf("owner-tail at-arbitration termination refused: terminated=%t err=%v", terminated, termErr)
				}
				select {
				case <-pump.lossAcked:
				case <-tailCtx.Done():
					t.Fatalf("owner-tail at-arbitration loss was not acknowledged before the hook context ended")
				}
				return nil
			},
		})
		if got := outcome.state.tailInvocationsNow(); got != 1 {
			t.Fatalf("owner-tail at-arbitration loss invocations=%d, want exactly 1", got)
		}
		if outcome.state.eligibleNow() {
			t.Fatal("owner-tail at-arbitration loss still granted eligibility through the real arbitration")
		}
		if !errors.Is(outcome.flow.ownerErr, errOwnerTailArbitration) {
			t.Fatalf("owner-tail at-arbitration loss refusal was not the ACTUAL arbitration stage: %v", outcome.flow.ownerErr)
		}
		if outcome.flow.state.lossReasonNow() == "" {
			t.Fatal("owner-tail at-tail loss was not permanently latched")
		}
		if outcome.flow.state.compositePublished() {
			t.Fatal("owner-tail at-tail loss published the composite")
		}
		borrowedSuccessorCloseConn(t, outcome.flow.conn)
		t.Logf("at-arbitration loss negative: the loss injection ran immediately before the arbitration, which executed and refused (captured as the ACTUAL arbitration stage) with no eligibility and no composite")
	}()

	// N4 (P2-B2): CHANNEL-COORDINATED competing-loss control: the initial loss
	// state is ASSERTED empty, a bounded competing goroutine attempts the real
	// shared-mutex latch while the tail's arbitration runs, and ONLY the
	// ordering actually observed is reported.
	func() {
		b4, fresh4 := borrowedReplacementAutonomousBaseline(t, ctx)
		competeDone := make(chan struct{})
		outcome := borrowedReplacementOwnerTailRun(ctx, t, b4, fresh4, &borrowedReplacementOwnerTailHooks{
			ownerPID: b4.owner.BackendPID, ownerStart: b4.owner.BackendStart,
			expectedVerifier: b4.verifierP1, expectedState: b4.preState,
			atArbitration: func(_ context.Context, pump *borrowedReplacementAutonomousPump, _ func(), _ pgx.Tx) error {
				// ASSERT the initial empty shared loss state.
				pump.state.mu.Lock()
				initialLost := pump.state.lost
				pump.state.mu.Unlock()
				if initialLost {
					t.Fatal("owner-tail competing control requires an initially empty shared loss")
				}
				// Coordinate the competing execution: the goroutine attempts the
				// real shared-mutex latch while the arbitration is about to run.
				competeReady := make(chan struct{})
				go func() {
					defer close(competeDone)
					close(competeReady)
					pump.state.latchPreSealLoss("competing owner-tail loss")
				}()
				select {
				case <-competeReady:
				case <-time.After(30 * time.Second):
					t.Fatalf("owner-tail competing latch never started")
				}
				return nil
			},
		})
		// Bounded completion of the competing goroutine.
		joined := false
		t.Cleanup(func() {
			if joined {
				return
			}
			select {
			case <-competeDone:
			case <-time.After(30 * time.Second):
				t.Errorf("owner-tail competing latch completion is unknown after the bounded join")
			}
		})
		select {
		case <-competeDone:
			joined = true
		case <-time.After(30 * time.Second):
			t.Fatalf("owner-tail competing latch did not complete within the bounded join")
		}
		eligible := outcome.state.eligibleNow()
		lost := outcome.flow.state.lostNow()
		if !lost {
			t.Fatal("owner-tail competing latch did not land")
		}
		if outcome.flow.state.compositePublished() {
			t.Fatal("owner-tail competing loss published the composite")
		}
		if err := outcome.flow.state.publishDecision(ctx, nil); err == nil {
			t.Fatal("a subsequent decision after the competing ordering was accepted")
		}
		if eligible {
			t.Logf("competing-loss control (observed ordering publish-then-loss): the arbitration published the provisional eligibility first, the competing latch landed afterward, the composite was still refused and subsequent decisions were rejected")
		} else {
			t.Logf("competing-loss control (observed ordering loss-then-arbitration): the competing latch landed before the shared-mutex arbitration, which refused and never set eligibility; subsequent decisions were rejected")
		}
		borrowedSuccessorCloseConn(t, outcome.flow.conn)
	}()

	// N5: caller cancel at the tail -> refusal without eligibility.
	func() {
		b5, fresh5 := borrowedReplacementAutonomousBaseline(t, ctx)
		var helperCanceled atomic.Bool
		outcome := borrowedReplacementOwnerTailRun(ctx, t, b5, fresh5, &borrowedReplacementOwnerTailHooks{
			ownerPID: b5.owner.BackendPID, ownerStart: b5.owner.BackendStart,
			expectedVerifier: b5.verifierP1, expectedState: b5.preState,
			atArbitration: func(_ context.Context, _ *borrowedReplacementAutonomousPump, cancelLane func(), _ pgx.Tx) error {
				if cancelLane == nil {
					t.Fatal("owner-tail at-arbitration cancel hook has no lane cancel")
				}
				helperCanceled.Store(true)
				cancelLane()
				return nil
			},
		})
		if !helperCanceled.Load() {
			t.Fatal("owner-tail at-arbitration cancel was not exercised")
		}
		if outcome.state.eligibleNow() {
			t.Fatal("owner-tail granted eligibility despite the at-arbitration caller cancel")
		}
		if !errors.Is(outcome.flow.ownerErr, errOwnerTailArbitration) {
			t.Fatalf("owner-tail caller-cancel refusal was not the ACTUAL arbitration stage: %v", outcome.flow.ownerErr)
		}
		if outcome.flow.state.compositePublished() {
			t.Fatal("owner-tail at-tail caller cancel published the composite")
		}
		borrowedSuccessorCloseConn(t, outcome.flow.conn)
		t.Logf("at-tail caller-cancel negative: the eligibility arbitration refused after the caller context ended")
	}()

	// N6: observation/progress timeout at the tail through the REAL helper.
	func() {
		b6, fresh6 := borrowedReplacementAutonomousBaseline(t, ctx)
		stallRelease := make(chan struct{})
		var stallReleaseOnce sync.Once
		releaseStall := func() { stallReleaseOnce.Do(func() { close(stallRelease) }) }
		t.Cleanup(releaseStall)
		stallEntered := make(chan struct{})
		var stallEnteredOnce sync.Once
		var helperCanceled atomic.Bool
		outcome := borrowedReplacementOwnerTailRun(ctx, t, b6, fresh6, &borrowedReplacementOwnerTailHooks{
			ownerPID: b6.owner.BackendPID, ownerStart: b6.owner.BackendStart,
			expectedVerifier: b6.verifierP1, expectedState: b6.preState,
			atArbitration: func(tailCtx context.Context, pump *borrowedReplacementAutonomousPump, cancelLane func(), _ pgx.Tx) error {
				t.Cleanup(func() {
					releaseStall()
					if err := pump.stopBounded(30 * time.Second); err != nil {
						t.Errorf("owner-tail stalled pump completion is unknown after the deferred release: %v", err)
					}
				})
				pump.setStallFn(func(stallCtx context.Context) error {
					stallEnteredOnce.Do(func() { close(stallEntered) })
					select {
					case <-stallRelease:
						return errors.New("owner-tail stalled scheduler released")
					case <-stallCtx.Done():
						<-stallRelease
						return errors.New("owner-tail stalled scheduler released")
					}
				})
				select {
				case <-stallEntered:
				case <-tailCtx.Done():
					t.Fatalf("owner-tail scheduler stall never acknowledged its entry")
				}
				proxyCancel := func() {
					helperCanceled.Store(true)
					cancelLane()
				}
				return borrowedReplacementAutonomousAwaitSamples(tailCtx, pump, proxyCancel, 1, 3*time.Second)
			},
		})
		if !helperCanceled.Load() {
			t.Fatal("owner-tail at-tail progress timeout did not cancel the external lane context through the real helper")
		}
		if outcome.state.eligibleNow() || outcome.flow.state.compositePublished() {
			t.Fatal("owner-tail at-tail progress timeout still produced eligibility/composite")
		}
		if outcome.flow.state.lossReasonNow() == "" {
			t.Fatal("owner-tail at-tail progress timeout did not permanently latch the UNKNOWN loss")
		}
		releaseStall()
		if err := outcome.pump.stopBounded(30 * time.Second); err != nil {
			t.Fatalf("owner-tail at-tail progress timeout pump completion is unknown after the release: %v", err)
		}
		borrowedSuccessorCloseConn(t, outcome.flow.conn)
		t.Logf("at-tail progress-timeout negative: the real sample-wait helper latched the permanent UNKNOWN and canceled the external lane context with no eligibility and no composite")
	}()

	// N7: loss AFTER the provisional eligibility -> no final composite success.
	func() {
		b7, fresh7 := borrowedReplacementAutonomousBaseline(t, ctx)
		outcome := borrowedReplacementOwnerTailRun(ctx, t, b7, fresh7, &borrowedReplacementOwnerTailHooks{
			ownerPID: b7.owner.BackendPID, ownerStart: b7.owner.BackendStart,
			expectedVerifier: b7.verifierP1, expectedState: b7.preState,
			adapter: func(_ error, pump *borrowedReplacementAutonomousPump, _ func()) error {
				var terminated bool
				termCtx, cancelTerm := context.WithTimeout(context.Background(), borrowedOwnerRotationQueryBudget)
				termErr := b7.fixture.controlPool.QueryRow(termCtx, `SELECT pg_terminate_backend($1)`, pump.observer.reg.state.backendPID).Scan(&terminated)
				cancelTerm()
				if termErr != nil || !terminated {
					t.Fatalf("owner-tail post-eligibility termination refused: terminated=%t err=%v", terminated, termErr)
				}
				select {
				case <-pump.lossAcked:
				case <-time.After(30 * time.Second):
					t.Fatalf("owner-tail post-eligibility loss was not acknowledged")
				}
				return errors.New("owner-tail loss injected after the provisional eligibility")
			},
		})
		if outcome.state.tailInvocationsNow() != 1 || !outcome.state.eligibleNow() {
			t.Fatalf("owner-tail post-eligibility loss state: invocations=%d eligible=%t", outcome.state.tailInvocationsNow(), outcome.state.eligibleNow())
		}
		if !outcome.flow.state.lostNow() {
			t.Fatal("owner-tail post-eligibility loss was not latched")
		}
		if outcome.flow.state.compositePublished() {
			t.Fatal("owner-tail loss after the provisional eligibility still published the composite")
		}
		borrowedSuccessorCloseConn(t, outcome.flow.conn)
		t.Logf("post-eligibility loss negative: the actual loss after the provisional eligibility was latched and the final composite was never published (COMMIT acknowledgement loss stays UNKNOWN and is never asserted as a rollback)")
	}()

	// N8 (P2-1 residual): ACTUAL bounded worker stall BEFORE completion closes:
	// the REAL join-failure path with an unwind-safe cancel -> release ->
	// bounded completion-CHANNEL proof ORDERED BEFORE the flow cleanup join,
	// and the original UNKNOWN + retirement refusal asserted AFTER proven
	// completion.
	func() {
		b8, fresh8 := borrowedReplacementAutonomousBaseline(t, ctx)
		workerRelease := make(chan struct{})
		var workerReleaseOnce sync.Once
		releaseWorker := func() { workerReleaseOnce.Do(func() { close(workerRelease) }) }
		// Early idempotent strand-safe fallback for an in-run Fatal/Goexit.
		t.Cleanup(releaseWorker)
		workerEntered := make(chan struct{})
		var workerEnteredOnce sync.Once
		laneCtx, cancelLane := context.WithCancel(ctx)
		defer cancelLane()
		outcome := borrowedReplacementOwnerTailRun(laneCtx, t, b8, fresh8, &borrowedReplacementOwnerTailHooks{
			ownerPID: b8.owner.BackendPID, ownerStart: b8.owner.BackendStart,
			expectedVerifier: b8.verifierP1, expectedState: b8.preState,
			useJoinBound: 2 * time.Second,
			useWorkerStall: func(_ context.Context) {
				// The worker stall deliberately ignores the worker context until
				// released, so the REAL bounded join must fail.
				workerEnteredOnce.Do(func() { close(workerEntered) })
				<-workerRelease
			},
			postProbe: func(_ context.Context, _ *borrowedReplacementAutonomousPump, _ func()) error {
				// Stop the window before its own join so the FLOW's real join
				// failure path (and the adapter skip) is exercised.
				return errors.New("owner-tail worker-stall control: window stopped before its join")
			},
		})
		// Unwind-safe: registered AFTER the flow (LIFO -> runs BEFORE the flow
		// cleanup join): cancel -> release -> bounded wait on the ACTUAL worker
		// completion channel, so a Fatal/Goexit can never strand the worker and
		// the flow cleanup join always observes the ACTUAL completion.
		completionProven := false
		t.Cleanup(func() {
			cancelLane()
			releaseWorker()
			if completionProven {
				return
			}
			select {
			case <-outcome.flow.useCompletion:
				completionProven = true
			case <-time.After(30 * time.Second):
				t.Errorf("owner-tail worker completion is unknown after the unwind release")
			}
		})
		select {
		case <-workerEntered:
		case <-time.After(60 * time.Second):
			t.Fatalf("owner-tail worker stall never acknowledged its entry")
		}
		if got := outcome.state.tailInvocationsNow(); got != 0 {
			t.Fatalf("owner-tail worker-stall tail invocations=%d, want ZERO while completion is unproven", got)
		}
		if got := outcome.state.adapterInvocationsNow(); got != 0 {
			t.Fatalf("owner-tail worker-stall adapter invocations=%d, want ZERO", got)
		}
		if _, recorded := outcome.state.useErrNow(); recorded {
			t.Fatal("owner-tail worker-stall recorded a result while completion was unproven")
		}
		// Cancel-before-release, then prove the worker completion through the
		// ACTUAL completion channel (NOT the pre-completion recorded flag).
		cancelLane()
		releaseWorker()
		select {
		case <-outcome.flow.useCompletion:
			completionProven = true
		case <-time.After(30 * time.Second):
			t.Fatal("owner-tail worker completion was not proven after the release")
		}
		if !outcome.flow.terminationUnknown {
			t.Fatal("owner-tail worker-stall did not exercise the REAL join-failure path")
		}
		if outcome.flow.state.compositePublished() {
			t.Fatal("owner-tail worker-stall published the composite")
		}
		if err := borrowedReplacementOwnerTailRetireGuarded(t, ctx, b8, outcome); err == nil {
			t.Fatal("owner-tail retirement was accepted while the Use termination was UNKNOWN")
		}
		borrowedSuccessorCloseConn(t, outcome.flow.conn)
		t.Logf("worker-stall negative: the REAL bounded worker stall made the Use join fail (zero tail/adapter invocations and no recording while stalled); AFTER proven completion through the ACTUAL completion channel (unwind-safe cancel-before-release ordered before the flow cleanup join) the original UNKNOWN was preserved and retirement/reuse was blocked")
	}()

	// N9 (P2-B3.3): UNKNOWN pump join (stalled scheduler) blocking retirement
	// with the unconditional release installed immediately and post-release
	// completion proven.
	func() {
		b9, fresh9 := borrowedReplacementAutonomousBaseline(t, ctx)
		stallRelease := make(chan struct{})
		var stallReleaseOnce sync.Once
		releaseStall := func() { stallReleaseOnce.Do(func() { close(stallRelease) }) }
		t.Cleanup(releaseStall)
		stallEntered := make(chan struct{})
		var stallEnteredOnce sync.Once
		outcome := borrowedReplacementOwnerTailRun(ctx, t, b9, fresh9, &borrowedReplacementOwnerTailHooks{
			ownerPID: b9.owner.BackendPID, ownerStart: b9.owner.BackendStart,
			expectedVerifier: b9.verifierP1, expectedState: b9.preState,
			postProbe: func(postProbeCtx context.Context, pump *borrowedReplacementAutonomousPump, cancelLane func()) error {
				t.Cleanup(func() {
					releaseStall()
					if err := pump.stopBounded(30 * time.Second); err != nil {
						t.Errorf("owner-tail stalled pump completion is unknown after the deferred release: %v", err)
					}
				})
				pump.setStallFn(func(stallCtx context.Context) error {
					stallEnteredOnce.Do(func() { close(stallEntered) })
					select {
					case <-stallRelease:
						return errors.New("owner-tail stalled scheduler released")
					case <-stallCtx.Done():
						<-stallRelease
						return errors.New("owner-tail stalled scheduler released")
					}
				})
				select {
				case <-stallEntered:
				case <-postProbeCtx.Done():
					t.Fatalf("owner-tail scheduler stall never acknowledged its entry")
				}
				return borrowedReplacementAutonomousAwaitSamples(postProbeCtx, pump, cancelLane, 1, 3*time.Second)
			},
		})
		if outcome.pumpJoinErr == nil {
			t.Fatal("owner-tail stalled pump join was expected to be UNKNOWN")
		}
		if err := borrowedReplacementOwnerTailRetireGuarded(t, ctx, b9, outcome); err == nil {
			t.Fatal("owner-tail retirement was accepted while the pump join was UNKNOWN")
		}
		releaseStall()
		if err := outcome.pump.stopBounded(30 * time.Second); err != nil {
			t.Fatalf("owner-tail stalled pump completion is unknown after the release: %v", err)
		}
		borrowedSuccessorCloseConn(t, outcome.flow.conn)
		t.Logf("UNKNOWN-join negative: the UNKNOWN pump join blocked retirement/reuse and the pump completion was proven after the release")
	}()

	// Guard unresolved and acceptance unchanged across the positive fixture.
	borrowedOwnerDDLGuard(t, ctx, b)
}
