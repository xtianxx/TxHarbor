//go:build linux && drill

// borrowed-replacement-owner-rollback-unwind_linux_test.go is the bounded
// owner rollback-response-loss unwind lane. It composes the existing machinery
// without duplicating any transaction or observer path: the genuine autonomous
// baseline/capture and guard-row preamble created through the UNARMED fixed
// fault, the recycled actual-refusal helper, and the existing observed journal
// transaction with exact guard-row locking, same-transaction audit insertion,
// supplied-tx readback, pending invisibility and the structured contender
// 55P03. The lane drives the drill-only fixed rollback controls: the owner-tail
// atArbitration hook (after the insertion and the successful final facts
// verification) publishes no I/O under a mutex and arms the ROLLBACK-only gate
// before the actual cancellation; the ACTUAL rollback request is recognized,
// forwarded (the server really rolls back) and its strictly validated
// PostgreSQL ROLLBACK completion withheld BEFORE pgx receives it. The
// production repair then unwinds through the independent bounded rollback
// context: the deadline-driven return happens WITHOUT any watchdog rescue, and
// an uncertain rollback permanently retires the owner handle and boundedly
// disposes its connection (never the Release SQL path and never an unbounded
// close). The refusal is asserted only AFTER the actual completion: no
// composite, no eligibility/replay authority, unresolved unchanged guard,
// acceptance zero, independently checked journal visibility and released row
// fencing, and worker/pump/Use completion before dependency cleanup. A
// watchdog break is cleanup-only and turns the case NOTESTABLISHED, never a
// pass. There is no manifest, restore, admission, instance binding, guard
// resolution, continuous exclusion, arbitrary network-failure boundedness or
// Gate1 authority; there is no arbitrary SQL, connector or socket callback.
// Secrets, verifiers and DSNs are never logged.
package recovery_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xtianxx/txharbor/internal/recovery"
)

// borrowedReplacementOwnerRollbackWatchdogBound is the explicit case bound: the
// production rollback/disposal budget is 10s, so a boundedly unwinding case
// finishes well inside it; a watchdog that fires turns the case NOTESTABLISHED
// and can never make it pass.
const borrowedReplacementOwnerRollbackWatchdogBound = 30 * time.Second

// TestBorrowedReplacementOwnerRollbackUnwind is the bounded owner
// rollback-response-loss unwind lane described in the file header.
func TestBorrowedReplacementOwnerRollbackUnwind(t *testing.T) {
	ctx := t.Context()
	var probeCalls, acceptanceCalls int32

	// P: the ACTUAL rollback is dispatched, its validated completion is
	// withheld, the deadline-driven unwind completes WITHOUT watchdog rescue,
	// the owner handle is permanently retired with a bounded disposal, and the
	// refusal is asserted only after the actual completion.
	func() {
		fault, reset, err := recovery.ArmTargetLockCommitAmbiguityFault()
		if err != nil {
			t.Fatalf("rollback-unwind fault reservation refused: %v", err)
		}
		defer reset()
		b, fresh := borrowedReplacementGuardRowFixtures(t, ctx)
		if got := fault.WrappedCount(); got != 1 {
			t.Fatalf("rollback-unwind reserved fault wrapped %d owner sockets, want exactly 1", got)
		}
		beforeDisposition, beforeOperation := borrowedReplacementGuardRowRead(t, ctx, b.fixture.controlPool, b.fixture.guardKey)
		attemptID := borrowedReplacementOwnerJournalActualRefusal(t, ctx, b, fresh, "rollback-unwind", &probeCalls, &acceptanceCalls)
		facts := &borrowedReplacementOwnerJournalFacts{}
		var canceled atomic.Bool
		var decisiveAt time.Time
		watchdogFired := atomic.Bool{}
		watchdog := time.AfterFunc(borrowedReplacementOwnerRollbackWatchdogBound, func() {
			watchdogFired.Store(true)
			fault.Break()
		})
		defer watchdog.Stop()
		outcome := borrowedReplacementOwnerJournalRun(ctx, t, b, fresh, &borrowedReplacementOwnerJournalStage{
			key: b.fixture.guardKey, expectedOperation: b.fixture.operation,
			insert: true, attemptID: attemptID, contenderProof: true,
		}, facts, &borrowedReplacementOwnerTailHooks{
			atArbitration: func(_ context.Context, _ *borrowedReplacementAutonomousPump, cancelLane func(), _ pgx.Tx) error {
				if !facts.inserted || facts.auditID <= 0 {
					return errors.New("rollback-unwind arm refused: the insertion was not confirmed")
				}
				if armErr := fault.ArmRollbackGate(); armErr != nil {
					return fmt.Errorf("rollback-unwind arm refused: %w", armErr)
				}
				decisiveAt = time.Now()
				canceled.Store(true)
				cancelLane()
				return nil
			},
		})
		if watchdogFired.Load() {
			t.Fatal("NOTESTABLISHED: the rollback unwind required the watchdog break instead of the bounded rollback deadline")
		}
		if !canceled.Load() || !facts.inserted || facts.auditID <= 0 {
			t.Fatalf("rollback-unwind control: canceled=%t inserted=%t auditID=%d", canceled.Load(), facts.inserted, facts.auditID)
		}
		if !fault.RollbackMode() || !fault.RollbackDispatched() || !fault.RollbackObserved() {
			t.Fatal("rollback-unwind did not dispatch the ACTUAL rollback request")
		}
		if !fault.RollbackWithheld() {
			t.Fatal("rollback-unwind did not withhold the validated ROLLBACK completion")
		}
		if fault.CommitDispatched() || fault.CommitObserved() {
			t.Fatal("rollback-unwind dispatched or observed a COMMIT")
		}
		if outcome.flow.ownerErr == nil {
			t.Fatal("rollback-unwind uncertain rollback was reported as success")
		}
		if !errors.Is(outcome.flow.ownerErr, errOwnerTailArbitration) {
			t.Fatalf("rollback-unwind refusal was not the ACTUAL arbitration sentinel: %v", outcome.flow.ownerErr)
		}
		if !strings.Contains(outcome.flow.ownerErr.Error(), "target-lock acceptance callback") {
			t.Fatalf("rollback-unwind owner error was not the callback refusal path: %v", outcome.flow.ownerErr)
		}
		if !strings.Contains(outcome.flow.ownerErr.Error(), "rollback target-lock acceptance transaction") {
			t.Fatalf("rollback-unwind owner error was not the uncertain rollback path: %v", outcome.flow.ownerErr)
		}
		if !errors.Is(outcome.flow.ownerErr, context.DeadlineExceeded) && !errors.Is(outcome.flow.ownerErr, os.ErrDeadlineExceeded) {
			t.Fatalf("rollback-unwind did not preserve the independent-cleanup deadline cause: %v", outcome.flow.ownerErr)
		}
		if strings.Contains(outcome.flow.ownerErr.Error(), "commit target-lock acceptance transaction") {
			t.Fatalf("rollback-unwind owner error must never be an ambiguous COMMIT: %v", outcome.flow.ownerErr)
		}
		if decisiveAt.IsZero() {
			t.Fatal("rollback-unwind decisive boundary was never recorded")
		}
		if elapsed := time.Since(decisiveAt); elapsed > 25*time.Second {
			t.Fatalf("rollback-unwind unwind from the decisive boundary exceeded the explicit budget bound: %v", elapsed)
		}
		// Uncertain rollback: the owner handle is permanently retired and its
		// connection was disposed boundedly.
		healthCtx, cancelHealth := context.WithTimeout(ctx, borrowedOwnerRotationHealthBudget)
		healthErr := b.fixture.lock.Health(healthCtx)
		cancelHealth()
		if healthErr == nil || !strings.Contains(healthErr.Error(), "closed") {
			t.Fatalf("rollback-unwind uncertain rollback did not permanently retire the owner handle: %v", healthErr)
		}
		closedCtx, cancelClosed := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
		closedErr := b.fixture.lock.WithTransaction(closedCtx, func(context.Context, pgx.Tx) error { return nil })
		cancelClosed()
		if closedErr == nil || !strings.Contains(closedErr.Error(), "closed") {
			t.Fatalf("rollback-unwind retired owner handle still accepted a transaction: %v", closedErr)
		}
		if outcome.flow.state.compositePublished() || outcome.state.eligibleNow() {
			t.Fatal("rollback-unwind produced a composite or eligibility")
		}
		reuseCtx, cancelReuse := context.WithTimeout(ctx, 30*time.Second)
		reuseErr := outcome.flow.reg.Use(reuseCtx)
		copied := *outcome.flow.reg
		copyErr := copied.Use(reuseCtx)
		cancelReuse()
		if reuseErr == nil || copyErr == nil {
			t.Fatal("rollback-unwind replay/copy use was accepted after the uncertain rollback")
		}
		// Independent journal visibility + released row fencing; zero rows alone
		// is never taken as an acknowledged rollback.
		if count := borrowedReplacementOwnerJournalCount(t, ctx, b.fixture.controlPool, attemptID); count != 0 {
			t.Fatalf("rollback-unwind durable journal rows=%d, want zero after the actual rollback", count)
		}
		postDisposition, postOperation := borrowedReplacementGuardRowRead(t, ctx, b.fixture.controlPool, b.fixture.guardKey)
		if postDisposition != beforeDisposition || postOperation != beforeOperation {
			t.Fatal("rollback-unwind changed the guard contents")
		}
		contenderDisposition, contenderOperation, contenderErr := borrowedReplacementGuardRowContenderLock(ctx, b.fixture.controlPool, b.fixture.guardKey)
		if contenderErr != nil {
			t.Fatalf("rollback-unwind released guard row was not lockable: %v", contenderErr)
		}
		if contenderDisposition != beforeDisposition || contenderOperation != beforeOperation {
			t.Fatal("rollback-unwind contender observed changed guard contents")
		}
		borrowedOwnerDDLGuard(t, ctx, b)
		// Worker/pump/Use completion before dependency cleanup.
		if outcome.pumpJoinErr != nil {
			t.Fatalf("rollback-unwind pump join was UNKNOWN: %v", outcome.pumpJoinErr)
		}
		if _, recorded := outcome.state.useErrNow(); !recorded {
			t.Fatal("rollback-unwind use completion was not channel-confirmed")
		}
		borrowedSuccessorCloseConn(t, outcome.flow.conn)
		t.Logf("rollback-unwind positive: the ACTUAL rollback was dispatched after the caller cancellation, its validated ROLLBACK completion was withheld, the independent bounded rollback budget drove the return WITHOUT watchdog rescue (watchdog never fired), the uncertain rollback permanently retired the owner handle with a bounded disposal, no COMMIT was dispatched, the composite/eligibility were refused, the journal showed zero rows with the released guard row independently lockable and unchanged, and worker/pump/Use completion was proven before cleanup")
	}()

	// N1: unarmed cancellation: the rollback is acknowledged normally, zero
	// matching rows remain and the original owner stays healthy.
	func() {
		fault, reset, err := recovery.ArmTargetLockCommitAmbiguityFault()
		if err != nil {
			t.Fatalf("rollback-unwind unarmed fault reservation refused: %v", err)
		}
		defer reset()
		b, fresh := borrowedReplacementGuardRowFixtures(t, ctx)
		if got := fault.WrappedCount(); got != 1 {
			t.Fatalf("rollback-unwind unarmed fault wrapped %d owner sockets, want exactly 1", got)
		}
		beforeDisposition, beforeOperation := borrowedReplacementGuardRowRead(t, ctx, b.fixture.controlPool, b.fixture.guardKey)
		attemptID := borrowedReplacementOwnerJournalActualRefusal(t, ctx, b, fresh, "rollback-unarmed", &probeCalls, &acceptanceCalls)
		facts := &borrowedReplacementOwnerJournalFacts{}
		var canceled atomic.Bool
		outcome := borrowedReplacementOwnerJournalRun(ctx, t, b, fresh, &borrowedReplacementOwnerJournalStage{
			key: b.fixture.guardKey, expectedOperation: b.fixture.operation,
			insert: true, attemptID: attemptID, contenderProof: true,
		}, facts, &borrowedReplacementOwnerTailHooks{
			atArbitration: func(_ context.Context, _ *borrowedReplacementAutonomousPump, cancelLane func(), _ pgx.Tx) error {
				if cancelLane == nil {
					t.Fatal("rollback-unwind unarmed control has no lane cancel")
				}
				canceled.Store(true)
				cancelLane()
				return nil
			},
		})
		if !canceled.Load() || !facts.inserted {
			t.Fatalf("rollback-unwind unarmed control canceled=%t inserted=%t", canceled.Load(), facts.inserted)
		}
		if outcome.flow.ownerErr == nil || !strings.Contains(outcome.flow.ownerErr.Error(), "target-lock acceptance callback") {
			t.Fatalf("rollback-unwind unarmed control was not the callback refusal path: %v", outcome.flow.ownerErr)
		}
		if !errors.Is(outcome.flow.ownerErr, errOwnerTailArbitration) {
			t.Fatalf("rollback-unwind unarmed refusal was not the ACTUAL arbitration sentinel: %v", outcome.flow.ownerErr)
		}
		if strings.Contains(outcome.flow.ownerErr.Error(), "rollback target-lock acceptance transaction") {
			t.Fatalf("rollback-unwind unarmed rollback was not acknowledged: %v", outcome.flow.ownerErr)
		}
		if fault.RollbackDispatched() || fault.RollbackWithheld() || fault.RollbackObserved() || fault.CommitObserved() {
			t.Fatal("rollback-unwind UNARMED gate claimed a rollback or commit witness")
		}
		healthCtx, cancelHealth := context.WithTimeout(ctx, borrowedOwnerRotationHealthBudget)
		healthErr := b.fixture.lock.Health(healthCtx)
		cancelHealth()
		if healthErr != nil {
			t.Fatalf("rollback-unwind unarmed rollback did not keep the original owner healthy: %v", healthErr)
		}
		if count := borrowedReplacementOwnerJournalCount(t, ctx, b.fixture.controlPool, attemptID); count != 0 {
			t.Fatalf("rollback-unwind unarmed rollback left %d durable journal rows", count)
		}
		postDisposition, postOperation := borrowedReplacementGuardRowRead(t, ctx, b.fixture.controlPool, b.fixture.guardKey)
		if postDisposition != beforeDisposition || postOperation != beforeOperation {
			t.Fatal("rollback-unwind unarmed control changed the guard contents")
		}
		if _, _, contenderErr := borrowedReplacementGuardRowContenderLock(ctx, b.fixture.controlPool, b.fixture.guardKey); contenderErr != nil {
			t.Fatalf("rollback-unwind unarmed control released guard row was not lockable: %v", contenderErr)
		}
		if outcome.flow.state.compositePublished() || outcome.state.eligibleNow() {
			t.Fatal("rollback-unwind unarmed control produced a composite or eligibility")
		}
		reuseCtx, cancelReuse := context.WithTimeout(ctx, 30*time.Second)
		reuseErr := outcome.flow.reg.Use(reuseCtx)
		copied := *outcome.flow.reg
		copyErr := copied.Use(reuseCtx)
		cancelReuse()
		if reuseErr == nil || copyErr == nil {
			t.Fatal("rollback-unwind unarmed replay/copy use was accepted")
		}
		borrowedOwnerDDLGuard(t, ctx, b)
		if outcome.pumpJoinErr != nil {
			t.Fatalf("rollback-unwind unarmed pump join was UNKNOWN: %v", outcome.pumpJoinErr)
		}
		borrowedSuccessorCloseConn(t, outcome.flow.conn)
		t.Logf("rollback-unwind unarmed control: the cancellation produced an acknowledged rollback with zero matching rows, the original owner session stayed healthy, the guard stayed unchanged and unresolved and the composite/eligibility were refused")
	}()

	// N2: deterministic rollback-completion negatives: missing, partial and
	// malformed completions never publish a completion witness and stay UNKNOWN.
	func() {
		request := recovery.TargetLockRollbackRequestFrame()
		completion := recovery.TargetLockRollbackCompletionFrames()
		// Boundaries derive from the ENCODED lengths: the ROLLBACK
		// CommandComplete body is "ROLLBACK\0" (14 bytes including the header),
		// not 12.
		ccFrames := completion[:1+4+len("ROLLBACK\x00")]
		zFrames := completion[1+4+len("ROLLBACK\x00"):]
		// Missing completion.
		missing := recovery.NewTargetLockCommitAmbiguityControl()
		missing.ArmRollbackGate()
		if !missing.RollbackMode() {
			t.Fatal("rollback-unwind deterministic control did not enter rollback mode")
		}
		if err := missing.ControlWriteClientBytes(request); err != nil {
			t.Fatalf("rollback-unwind missing-completion request refused: %v", err)
		}
		if _, err := missing.ControlRead(make([]byte, 64)); err == nil {
			t.Fatal("rollback-unwind missing rollback completion was accepted")
		}
		if missing.RollbackWithheld() || missing.CommitDispatched() || !missing.ReadFailed() {
			t.Fatal("rollback-unwind missing rollback completion published a witness")
		}
		missing.Break()
		if !missing.CleanupUncertain() {
			t.Fatal("rollback-unwind missing rollback completion did not remain UNKNOWN")
		}
		// Partial completion then an explicit failure.
		partial := recovery.NewTargetLockCommitAmbiguityControl()
		partial.ArmRollbackGate()
		if err := partial.ControlWriteClientBytes(request); err != nil {
			t.Fatalf("rollback-unwind partial request refused: %v", err)
		}
		if err := partial.ControlFeedServerBytes(completion[:6]); err != nil {
			t.Fatalf("rollback-unwind partial feed refused: %v", err)
		}
		if err := partial.ControlFeedServerError(os.ErrDeadlineExceeded); err != nil {
			t.Fatalf("rollback-unwind partial error feed refused: %v", err)
		}
		if _, err := partial.ControlRead(make([]byte, 64)); err == nil {
			t.Fatal("rollback-unwind partial rollback completion was accepted")
		}
		if partial.RollbackWithheld() || !partial.ReadFailed() {
			t.Fatal("rollback-unwind partial rollback completion published a witness")
		}
		if got := partial.AssembledBytes(); got != 6 {
			t.Fatalf("rollback-unwind partial completion consumed %d bytes, want the 6 partial bytes", got)
		}
		partial.Break()
		if !partial.CleanupUncertain() {
			t.Fatal("rollback-unwind partial rollback completion did not remain UNKNOWN")
		}
		// Malformed completions: wrong tag, missing NUL and a short ReadyForQuery.
		malformedCases := []struct {
			label  string
			frames []byte
		}{
			{"commit-tag", recovery.TargetLockCommitCompletionFrames()},
			{"missing-NUL", append(append([]byte{}, borrowedReplacementCommitAmbiguityFrame(nil, 'C', []byte("ROLLBACK"))...), zFrames...)},
			{"short-ready", append(append([]byte{}, ccFrames...), borrowedReplacementCommitAmbiguityFrame(nil, 'Z', nil)...)},
			{"wrong-status", append(append([]byte{}, ccFrames...), borrowedReplacementCommitAmbiguityFrame(nil, 'Z', []byte{'T'})...)},
		}
		for _, malformedCase := range malformedCases {
			malformed := recovery.NewTargetLockCommitAmbiguityControl()
			malformed.ArmRollbackGate()
			if err := malformed.ControlWriteClientBytes(request); err != nil {
				t.Fatalf("rollback-unwind malformed (%s) request refused: %v", malformedCase.label, err)
			}
			if err := malformed.ControlFeedServerBytes(malformedCase.frames); err != nil {
				t.Fatalf("rollback-unwind malformed (%s) feed refused: %v", malformedCase.label, err)
			}
			if _, err := malformed.ControlRead(make([]byte, 64)); err == nil {
				t.Fatalf("rollback-unwind malformed (%s) rollback completion was accepted", malformedCase.label)
			}
			if malformed.RollbackWithheld() || !malformed.ReadFailed() {
				t.Fatalf("rollback-unwind malformed (%s) published a completion witness", malformedCase.label)
			}
			malformed.Break()
			if !malformed.CleanupUncertain() {
				t.Fatalf("rollback-unwind malformed (%s) did not remain UNKNOWN", malformedCase.label)
			}
		}
		// A queued error wakes a stalled reader promptly and fails closed.
		stalled := recovery.NewTargetLockCommitAmbiguityControl()
		stalled.ArmRollbackGate()
		if err := stalled.ControlWriteClientBytes(request); err != nil {
			t.Fatalf("rollback-unwind stalled request refused: %v", err)
		}
		if err := stalled.ControlFeedServerBytes(completion[:6]); err != nil {
			t.Fatalf("rollback-unwind stalled partial feed refused: %v", err)
		}
		if err := stalled.ControlStallAfterBytes(6); err != nil {
			t.Fatalf("rollback-unwind stalled arm refused: %v", err)
		}
		stalledDone := make(chan struct{})
		var stalledErr error
		t.Cleanup(func() {
			stalled.Break()
			select {
			case <-stalledDone:
			case <-time.After(10 * time.Second):
				t.Errorf("rollback-unwind stalled reader did not finish within the bounded unwind join")
			}
		})
		go func() {
			defer close(stalledDone)
			_, stalledErr = stalled.ControlRead(make([]byte, 64))
		}()
		stallCtx, cancelStall := context.WithTimeout(ctx, 5*time.Second)
		stallReached := stalled.ControlStallReached(stallCtx)
		cancelStall()
		if !stallReached {
			t.Fatal("rollback-unwind stalled reader was never reached")
		}
		if err := stalled.ControlFeedServerError(io.EOF); err != nil {
			t.Fatalf("rollback-unwind stalled error feed refused: %v", err)
		}
		select {
		case <-stalledDone:
		case <-time.After(5 * time.Second):
			t.Fatal("rollback-unwind queued error did not wake the stalled reader")
		}
		if stalledErr == nil || stalled.RollbackWithheld() {
			t.Fatal("rollback-unwind queued error published a completion witness")
		}
		stalled.Break()
		t.Logf("rollback-unwind completion negatives: missing, partial and malformed ROLLBACK completions plus a queued read error never publish a completion witness and remain UNKNOWN, with the partial bytes genuinely consumed")
	}()

	// N4 (P2-B1/B2): sealed command-kind controls: conflicting re-arms are
	// rejected (the kind is sealed at the first arm), a wrong COMMIT frame while
	// expecting ROLLBACK is observed independently but never dispatched as the
	// selected kind, and concurrent arms seal exactly one kind.
	func() {
		commitFirst := recovery.NewTargetLockCommitAmbiguityControl()
		if err := commitFirst.ArmGate(); err != nil {
			t.Fatalf("rollback-unwind commit-first arm refused: %v", err)
		}
		if commitFirst.CommandKind() != "commit" {
			t.Fatalf("rollback-unwind commit-first kind=%s", commitFirst.CommandKind())
		}
		if err := commitFirst.ArmRollbackGate(); err == nil {
			t.Fatal("rollback-unwind conflicting rollback re-arm was accepted after a commit arm")
		}
		if err := commitFirst.ArmGate(); err != nil {
			t.Fatalf("rollback-unwind same-kind re-arm was not idempotent: %v", err)
		}
		if commitFirst.CommandKind() != "commit" {
			t.Fatal("rollback-unwind command kind was relabeled by a re-arm")
		}
		commitFirst.Break()
		rollbackFirst := recovery.NewTargetLockCommitAmbiguityControl()
		if err := rollbackFirst.ArmRollbackGate(); err != nil {
			t.Fatalf("rollback-unwind rollback-first arm refused: %v", err)
		}
		if err := rollbackFirst.ArmGate(); err == nil {
			t.Fatal("rollback-unwind conflicting commit re-arm was accepted after a rollback arm")
		}
		if rollbackFirst.CommandKind() != "rollback" {
			t.Fatalf("rollback-unwind rollback-first kind=%s", rollbackFirst.CommandKind())
		}
		// Wrong command: a COMMIT frame while expecting ROLLBACK.
		if err := rollbackFirst.ControlWriteClientBytes(recovery.TargetLockCommitRequestFrame()); err != nil {
			t.Fatalf("rollback-unwind wrong-command write refused: %v", err)
		}
		if rollbackFirst.RollbackDispatched() || rollbackFirst.CommitDispatched() {
			t.Fatal("rollback-unwind wrong command was dispatched as the sealed kind")
		}
		if !rollbackFirst.CommitObserved() || rollbackFirst.RollbackObserved() {
			t.Fatal("rollback-unwind wrong-command observation was not independently recorded")
		}
		if rollbackFirst.Witness() != recovery.TargetLockCommitWitnessNotEstablished {
			t.Fatal("rollback-unwind wrong command claimed a dispatch witness")
		}
		if err := rollbackFirst.ControlWriteClientBytes(recovery.TargetLockRollbackRequestFrame()); err != nil {
			t.Fatalf("rollback-unwind sealed rollback write refused: %v", err)
		}
		if !rollbackFirst.RollbackDispatched() || !rollbackFirst.RollbackObserved() || !rollbackFirst.CommitObserved() {
			t.Fatal("rollback-unwind sealed rollback kind was not dispatched after the wrong command")
		}
		rollbackFirst.Break()
		// Concurrent arms seal exactly one kind.
		concurrent := recovery.NewTargetLockCommitAmbiguityControl()
		start := make(chan struct{})
		var wait sync.WaitGroup
		wait.Add(2)
		var commitErr, rollbackErr error
		go func() {
			defer wait.Done()
			<-start
			commitErr = concurrent.ArmGate()
		}()
		go func() {
			defer wait.Done()
			<-start
			rollbackErr = concurrent.ArmRollbackGate()
		}()
		close(start)
		wait.Wait()
		kind := concurrent.CommandKind()
		if (commitErr == nil) == (rollbackErr == nil) {
			t.Fatalf("rollback-unwind concurrent arms did not seal exactly one kind: commitErr=%v rollbackErr=%v kind=%s", commitErr, rollbackErr, kind)
		}
		if kind != "commit" && kind != "rollback" {
			t.Fatalf("rollback-unwind concurrent arms sealed kind=%s", kind)
		}
		if commitErr == nil && kind != "commit" {
			t.Fatal("rollback-unwind commit arm succeeded but the sealed kind is not commit")
		}
		if rollbackErr == nil && kind != "rollback" {
			t.Fatal("rollback-unwind rollback arm succeeded but the sealed kind is not rollback")
		}
		concurrent.Break()
		t.Logf("rollback-unwind command-kind controls: conflicting re-arms were rejected with the kind sealed at the first arm, a wrong COMMIT frame while expecting ROLLBACK was observed independently but never dispatched, and concurrent arms sealed exactly one kind (%s)", kind)
	}()

	// N3: early finalization control: a callback that COMMITS the supplied
	// transaction and still returns an error must be conservatively treated as
	// rollback uncertainty (ErrTxClosed is NOT an acknowledged rollback of that
	// transaction), retiring the owner WITHOUT any deadline/budget credit.
	func() {
		b := newBorrowedSuccessorBaseline(t, ctx)
		beforeDisposition, beforeOperation := borrowedReplacementGuardRowRead(t, ctx, b.fixture.controlPool, b.fixture.guardKey)
		started := time.Now()
		txCtx, cancelTx := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
		txErr := b.fixture.lock.WithTransaction(txCtx, func(callbackCtx context.Context, tx pgx.Tx) error {
			if err := tx.Commit(callbackCtx); err != nil {
				return fmt.Errorf("rollback-unwind early finalization commit refused: %w", err)
			}
			return errors.New("rollback-unwind early finalization refusal after the supplied transaction was committed")
		})
		cancelTx()
		elapsed := time.Since(started)
		if txErr == nil {
			t.Fatal("rollback-unwind early finalization was reported as success")
		}
		if !strings.Contains(txErr.Error(), "target-lock acceptance callback") {
			t.Fatalf("rollback-unwind early finalization was not the callback path: %v", txErr)
		}
		if !strings.Contains(txErr.Error(), "rollback target-lock acceptance transaction") {
			t.Fatalf("rollback-unwind early finalization did not report rollback uncertainty: %v", txErr)
		}
		if !errors.Is(txErr, pgx.ErrTxClosed) {
			t.Fatalf("rollback-unwind early finalization was not the ErrTxClosed uncertainty path: %v", txErr)
		}
		if errors.Is(txErr, context.DeadlineExceeded) || errors.Is(txErr, os.ErrDeadlineExceeded) {
			t.Fatal("rollback-unwind early finalization must not get budget credit (deadline cause)")
		}
		if elapsed > 5*time.Second {
			t.Fatalf("rollback-unwind early finalization waited on the rollback budget: %v", elapsed)
		}
		healthCtx, cancelHealth := context.WithTimeout(ctx, borrowedOwnerRotationHealthBudget)
		healthErr := b.fixture.lock.Health(healthCtx)
		cancelHealth()
		if healthErr == nil || !strings.Contains(healthErr.Error(), "closed") {
			t.Fatalf("rollback-unwind early finalization did not retire the owner handle: %v", healthErr)
		}
		postDisposition, postOperation := borrowedReplacementGuardRowRead(t, ctx, b.fixture.controlPool, b.fixture.guardKey)
		if postDisposition != beforeDisposition || postOperation != beforeOperation {
			t.Fatal("rollback-unwind early finalization changed the guard contents")
		}
		borrowedOwnerDDLGuard(t, ctx, b)
		t.Logf("rollback-unwind early-finalization control: a callback that committed the supplied transaction and still returned an error was conservatively treated as rollback uncertainty (ErrTxClosed, retirement without deadline/budget credit), the owner handle was permanently retired inside %v and the guard stayed unchanged and unresolved", elapsed.Round(time.Millisecond))
	}()

	if got := atomic.LoadInt32(&probeCalls); got != 0 {
		t.Fatalf("rollback-unwind lane ran the ordinary probe %d times, want 0", got)
	}
	if got := atomic.LoadInt32(&acceptanceCalls); got != 0 {
		t.Fatalf("rollback-unwind lane ran ordinary acceptance %d times, want 0", got)
	}
}
