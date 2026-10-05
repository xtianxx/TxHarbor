//go:build linux && drill

// borrowed-replacement-owner-commit-ambiguity_linux_test.go is the bounded real
// owner COMMIT acknowledgement-loss lane. It composes the existing machinery
// without duplicating any transaction or observer path: the genuine autonomous
// baseline/capture and guard-row preamble (created through the UNARMED fixed
// fault gate so ordinary traffic is unchanged), the recycled actual-refusal
// helper (real unique refused attempt identity, no-Start/no-callback fact
// only), and the existing observed journal transaction with exact guard-row
// locking, same-transaction journal insertion and pending invisibility. The
// lane drives the drill-only fixed fault companion
// (targetlock_commitambiguity_hook_linux_test.go): the production socket-wrap
// seam is reserved ONCE, consumed by exactly one real acquisition, and its wire
// gate - armed ONLY in the owner-tail atArbitration hook after the insertion
// and the successful final facts verification - establishes that the ACTUAL
// COMMIT was sent, bounded-assembles and validates the REAL PostgreSQL COMMIT
// completion, and withholds it BEFORE pgx receives it. While withheld, an
// independent bounded control-store read must observe the exact committed
// refused journal row (and the released guard row); then the owner transport is
// broken, causing the ACTUAL Commit call to return an error - never an injected
// error after a received acknowledgement. The UNKNOWN COMMIT acknowledgement
// loss is never reported as success or rollback: the composite is refused, the
// durable refused non-instance-bound journal row remains, the provisional
// eligibility never authorizes retry/acceptance/recovery, the guard stays
// unresolved, and the fault worker/pump/Use are finished with cancellation
// aware waits, immediate unwind-safe break/join cleanups and bounded joins
// without reacquiring or reviving the owner. There is no manifest, restore,
// admission, atomic acceptance, downstream or Gate1 authority; no replacement
// owner, DSN/key rebinding, copied acquisition loop, raw connection or
// arbitrary connector/SQL authority; authentication payloads, secrets,
// verifiers and DSNs are never logged.
package recovery_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xtianxx/txharbor/internal/recovery"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

// borrowedReplacementCommitAmbiguityTuple is the immutable audit-ID/attempt
// tuple published to the fault worker at the arming boundary.
type borrowedReplacementCommitAmbiguityTuple struct {
	auditID   int64
	attemptID string
}

// borrowedReplacementCommitAmbiguityResult preserves the actually witnessed
// fault-worker facts.
type borrowedReplacementCommitAmbiguityResult struct {
	tuple            borrowedReplacementCommitAmbiguityTuple
	dispatch         bool
	withheld         bool
	broken           bool
	durableRows      int
	durableRow       borrowedReplacementOwnerJournalRow
	guardLocked      bool
	guardDisposition string
	guardOperation   string
	err              error
}

// borrowedReplacementCommitAmbiguityFaultWorker waits for the published tuple,
// witnesses the dispatch and the validated withholding, independently reads the
// exact committed refused row while the completion is withheld, and breaks the
// owner transport so the ACTUAL Commit call returns an error. Every wait is
// cancellation-aware and the constructor installs the unwind-safe
// cancel -> break -> bounded done-channel join IMMEDIATELY.
type borrowedReplacementCommitAmbiguityFaultWorker struct {
	ctx      context.Context
	cancel   context.CancelFunc
	b        *borrowedSuccessorBaseline
	fault    *recovery.TargetLockCommitAmbiguityFault
	tupleCh  chan borrowedReplacementCommitAmbiguityTuple
	resultCh chan *borrowedReplacementCommitAmbiguityResult
	doneCh   chan struct{}
}

func newBorrowedReplacementCommitAmbiguityFaultWorker(ctx context.Context, t *testing.T, b *borrowedSuccessorBaseline, fault *recovery.TargetLockCommitAmbiguityFault) *borrowedReplacementCommitAmbiguityFaultWorker {
	t.Helper()
	workerCtx, cancel := context.WithCancel(ctx)
	worker := &borrowedReplacementCommitAmbiguityFaultWorker{
		ctx: workerCtx, cancel: cancel, b: b, fault: fault,
		tupleCh:  make(chan borrowedReplacementCommitAmbiguityTuple, 1),
		resultCh: make(chan *borrowedReplacementCommitAmbiguityResult, 1),
		doneCh:   make(chan struct{}),
	}
	// P2-B3: the unwind-safe cleanup is installed IMMEDIATELY at construction:
	// cancel -> break/release -> bounded wait on the ACTUAL goroutine-done
	// channel, so a fatal can never leave the worker up to 90s unproven.
	t.Cleanup(func() {
		cancel()
		fault.Break()
		select {
		case <-worker.doneCh:
		case <-time.After(10 * time.Second):
			t.Errorf("commit-ambiguity fault worker did not exit within the bounded unwind join")
		}
	})
	go worker.run()
	return worker
}

// publish hands the immutable tuple to the worker through a synchronized
// buffered channel; no shared publication mutex is held and no I/O is
// performed while arming.
func (w *borrowedReplacementCommitAmbiguityFaultWorker) publish(tuple borrowedReplacementCommitAmbiguityTuple) {
	w.tupleCh <- tuple
}

func (w *borrowedReplacementCommitAmbiguityFaultWorker) join(t *testing.T) *borrowedReplacementCommitAmbiguityResult {
	t.Helper()
	select {
	case res := <-w.resultCh:
		return res
	case <-time.After(120 * time.Second):
		t.Fatal("commit-ambiguity fault worker did not finish within the bounded join")
		return nil
	}
}

func (w *borrowedReplacementCommitAmbiguityFaultWorker) run() {
	defer close(w.doneCh)
	res := &borrowedReplacementCommitAmbiguityResult{}
	// ALWAYS break the owner transport: a fault-path cancellation can never
	// leave the owner COMMIT withheld forever.
	defer func() {
		w.fault.Break()
		res.broken = true
		w.resultCh <- res
	}()
	select {
	case tuple := <-w.tupleCh:
		res.tuple = tuple
	case <-w.ctx.Done():
		res.err = errors.New("cancelled before the armed gate tuple was published")
		return
	case <-time.After(90 * time.Second):
		res.err = errors.New("NOTESTABLISHED: the armed gate tuple was never published")
		return
	}
	dispatchCtx, cancelDispatch := context.WithTimeout(w.ctx, 90*time.Second)
	defer cancelDispatch()
	if !w.fault.AwaitDispatched(dispatchCtx) {
		res.err = errors.New("NOTESTABLISHED: the real COMMIT was never observed on the wire")
		return
	}
	res.dispatch = true
	withheldCtx, cancelWithheld := context.WithTimeout(w.ctx, 90*time.Second)
	defer cancelWithheld()
	if !w.fault.AwaitWithheld(withheldCtx) {
		res.err = errors.New("NOTESTABLISHED: the validated COMMIT completion was never withheld")
		return
	}
	res.withheld = true
	// While the validated completion is withheld (the server has committed),
	// the exact refused journal row MUST be independently visible.
	countCtx, cancelCount := context.WithTimeout(w.ctx, borrowedOwnerRotationQueryBudget)
	countErr := w.b.fixture.controlPool.QueryRow(countCtx,
		`SELECT count(*)::int FROM recovery_audit WHERE action=$1 AND operation_id=$2 AND result='refused'`,
		borrowedReplacementOwnerJournalAction, res.tuple.attemptID).Scan(&res.durableRows)
	cancelCount()
	if countErr != nil {
		res.err = fmt.Errorf("independent committed-journal read refused: %w", countErr)
		return
	}
	rowCtx, cancelRow := context.WithTimeout(w.ctx, borrowedOwnerRotationQueryBudget)
	row, rowErr := borrowedReplacementOwnerJournalRead(rowCtx, w.b.fixture.controlPool, res.tuple.auditID)
	cancelRow()
	if rowErr != nil {
		res.err = fmt.Errorf("independent committed-row read refused: %w", rowErr)
		return
	}
	res.durableRow = row
	// The server-side commit also released the owner guard row: a contender can
	// lock it while pgx still has not seen the completion.
	guardCtx, cancelGuard := context.WithTimeout(w.ctx, borrowedOwnerRotationQueryBudget)
	_, _, contenderErr := borrowedReplacementGuardRowContenderLock(guardCtx, w.b.fixture.controlPool, w.b.fixture.guardKey)
	cancelGuard()
	res.guardLocked = contenderErr == nil
	guardReadCtx, cancelGuardRead := context.WithTimeout(w.ctx, borrowedOwnerRotationQueryBudget)
	guardReadErr := w.b.fixture.controlPool.QueryRow(guardReadCtx,
		`SELECT disposition, operation_id FROM recovery_target_guard WHERE target_guard_key=$1`,
		w.b.fixture.guardKey).Scan(&res.guardDisposition, &res.guardOperation)
	cancelGuardRead()
	if guardReadErr != nil {
		res.err = fmt.Errorf("independent guard read refused: %w", guardReadErr)
		return
	}
}

// borrowedReplacementCommitAmbiguityAcquire retains one real owner lock session
// on a distinct key of the fixture control store with an independent bounded
// cleanup.
func borrowedReplacementCommitAmbiguityAcquire(t *testing.T, ctx context.Context, dsn string, key recovery.TargetKey, label string) *recovery.TargetLock {
	t.Helper()
	acqCtx, cancelAcq := context.WithTimeout(ctx, borrowedOwnerRotationAuthBudget)
	lock, err := recovery.AcquireTargetLock(acqCtx, dsn, key, 3*time.Second, 10*time.Millisecond)
	cancelAcq()
	if err != nil {
		t.Fatalf("commit-ambiguity %s acquisition refused: %v", label, err)
	}
	t.Cleanup(func() {
		releaseCtx, cancelRelease := context.WithTimeout(context.Background(), borrowedOwnerRotationCleanupBudget)
		_ = lock.Release(releaseCtx)
		cancelRelease()
	})
	return lock
}

// borrowedReplacementCommitAmbiguityOrdinaryCommit runs one ordinary owner
// transaction on the session and requires an acknowledged commit.
func borrowedReplacementCommitAmbiguityOrdinaryCommit(t *testing.T, ctx context.Context, lock *recovery.TargetLock, label string) {
	t.Helper()
	txCtx, cancelTx := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	commitErr := lock.WithTransaction(txCtx, func(context.Context, pgx.Tx) error { return nil })
	cancelTx()
	if commitErr != nil {
		t.Fatalf("commit-ambiguity %s ordinary commit refused: %v", label, commitErr)
	}
}

// TestBorrowedReplacementOwnerCommitAmbiguity is the bounded real owner COMMIT
// acknowledgement-loss lane described in the file header.
func TestBorrowedReplacementOwnerCommitAmbiguity(t *testing.T) {
	ctx := t.Context()
	var probeCalls, acceptanceCalls int32

	// P: the ACTUAL COMMIT is sent, its REAL completion is validated and
	// withheld before pgx sees it, the committed refused journal row is
	// independently observed, the transport is then broken so the ACTUAL Commit
	// call returns an error, and the UNKNOWN loss is never a success or a
	// rollback.
	func() {
		fault, reset, err := recovery.ArmTargetLockCommitAmbiguityFault()
		if err != nil {
			t.Fatalf("commit-ambiguity fault reservation refused: %v", err)
		}
		defer reset()
		b, fresh := borrowedReplacementGuardRowFixtures(t, ctx)
		if got := fault.WrappedCount(); got != 1 {
			t.Fatalf("commit-ambiguity reserved fault wrapped %d owner sockets, want exactly 1", got)
		}
		beforeDisposition, beforeOperation := borrowedReplacementGuardRowRead(t, ctx, b.fixture.controlPool, b.fixture.guardKey)
		attemptID := borrowedReplacementOwnerJournalActualRefusal(t, ctx, b, fresh, "commit-ambiguity", &probeCalls, &acceptanceCalls)
		facts := &borrowedReplacementOwnerJournalFacts{}
		worker := newBorrowedReplacementCommitAmbiguityFaultWorker(ctx, t, b, fault)
		outcome := borrowedReplacementOwnerJournalRun(ctx, t, b, fresh, &borrowedReplacementOwnerJournalStage{
			key: b.fixture.guardKey, expectedOperation: b.fixture.operation,
			insert: true, attemptID: attemptID, contenderProof: true,
		}, facts, &borrowedReplacementOwnerTailHooks{
			atArbitration: func(_ context.Context, _ *borrowedReplacementAutonomousPump, _ func(), _ pgx.Tx) error {
				if !facts.inserted || facts.auditID <= 0 {
					return errors.New("commit-ambiguity arm refused: the insertion was not confirmed")
				}
				fault.ArmGate()
				worker.publish(borrowedReplacementCommitAmbiguityTuple{auditID: facts.auditID, attemptID: attemptID})
				return nil
			},
		})
		res := worker.join(t)
		if res.err != nil {
			t.Fatalf("commit-ambiguity fault worker refused: %v", res.err)
		}
		if !res.dispatch || !res.withheld || !res.broken {
			t.Fatalf("commit-ambiguity witnesses: dispatch=%t withheld=%t broken=%t", res.dispatch, res.withheld, res.broken)
		}
		if res.durableRows != 1 {
			t.Fatalf("commit-ambiguity committed journal rows while withheld=%d, want exactly 1", res.durableRows)
		}
		if res.durableRow.auditID != facts.auditID || res.durableRow.result != controlstore.AuditRefused || res.durableRow.instanceID != "" || res.durableRow.operationID != attemptID {
			t.Fatalf("commit-ambiguity committed row mismatch: id=%d result=%q instance=%q operation=%q", res.durableRow.auditID, res.durableRow.result, res.durableRow.instanceID, res.durableRow.operationID)
		}
		if !res.guardLocked {
			t.Fatal("commit-ambiguity committed transaction did not release the owner guard row")
		}
		if res.guardDisposition != beforeDisposition || res.guardOperation != beforeOperation {
			t.Fatal("commit-ambiguity committed transaction changed the guard contents")
		}
		if !facts.fenced || !facts.inserted || facts.auditID <= 0 || !facts.pendingViaTx || facts.pendingViaPool != 0 || facts.contenderCode != "55P03" {
			t.Fatalf("commit-ambiguity stage: fenced=%t inserted=%t auditID=%d pendingTx=%t pendingPool=%d contender=%q",
				facts.fenced, facts.inserted, facts.auditID, facts.pendingViaTx, facts.pendingViaPool, facts.contenderCode)
		}
		if got := outcome.state.tailInvocationsNow(); got != 1 {
			t.Fatalf("commit-ambiguity tail invocations=%d, want exactly 1", got)
		}
		if outcome.runErr != nil {
			t.Fatalf("commit-ambiguity flow returned an unexpected join error: %v", outcome.runErr)
		}
		if outcome.flow.ownerErr == nil {
			t.Fatal("commit-ambiguity broken owner transport was accepted as a success")
		}
		if !strings.Contains(outcome.flow.ownerErr.Error(), "commit target-lock acceptance transaction") {
			t.Fatalf("commit-ambiguity owner error was not the ambiguous COMMIT path: %v", outcome.flow.ownerErr)
		}
		if strings.Contains(outcome.flow.ownerErr.Error(), "rollback target-lock acceptance transaction") {
			t.Fatalf("commit-ambiguity owner error must never be reported as a rollback: %v", outcome.flow.ownerErr)
		}
		if outcome.flow.state.compositePublished() || outcome.flow.decisionErr == nil {
			t.Fatal("commit-ambiguity UNKNOWN COMMIT still produced a terminal verdict")
		}
		if count := borrowedReplacementOwnerJournalCount(t, ctx, b.fixture.controlPool, attemptID); count != 1 {
			t.Fatalf("commit-ambiguity durable journal rows=%d, want exactly 1", count)
		}
		// The provisional eligibility never authorizes retry, acceptance or
		// recovery after the UNKNOWN COMMIT.
		if !outcome.state.consumeEligibility() {
			t.Fatal("commit-ambiguity provisional eligibility was not consumable once")
		}
		if outcome.state.consumeEligibility() {
			t.Fatal("commit-ambiguity provisional eligibility was reusable")
		}
		reuseCtx, cancelReuse := context.WithTimeout(ctx, 30*time.Second)
		reuseErr := outcome.flow.reg.Use(reuseCtx)
		copied := *outcome.flow.reg
		copyErr := copied.Use(reuseCtx)
		cancelReuse()
		if reuseErr == nil || copyErr == nil {
			t.Fatal("commit-ambiguity retry/reuse was accepted after the UNKNOWN COMMIT")
		}
		borrowedOwnerDDLGuard(t, ctx, b)
		borrowedSuccessorCloseConn(t, outcome.flow.conn)
		t.Logf("commit-ambiguity positive: the fixed drill fault established that the ACTUAL COMMIT was sent and withheld its VALIDATED PostgreSQL completion before pgx saw it (audit_id=%d), the independent bounded read observed exactly ONE committed refused non-instance-bound row and the released guard row while withheld, the transport break made the ACTUAL Commit call return an error (never an injected acknowledgement error), the composite refused, the durable row remained, the provisional eligibility was one-shot and non-reusable, and the guard stayed unresolved with acceptance zero", facts.auditID)
	}()

	// N1: UNARMED control: ordinary traffic is unchanged, the COMMIT is
	// acknowledged, exactly one genuine refused row is durable, the guard is
	// unchanged, guarded retirement runs, replay/copy plus the ordinary
	// dirty-guard refusal are preserved, and the reservation/consume controls
	// exercise real PG gate behavior (repeated acquisitions, overlapping arms,
	// stale reset, and a gated commit on the newer installation).
	func() {
		fault, reset, err := recovery.ArmTargetLockCommitAmbiguityFault()
		if err != nil {
			t.Fatalf("commit-ambiguity unarmed fault reservation refused: %v", err)
		}
		defer reset()
		b, fresh := borrowedReplacementGuardRowFixtures(t, ctx)
		if got := fault.WrappedCount(); got != 1 {
			t.Fatalf("commit-ambiguity unarmed fault wrapped %d owner sockets, want exactly 1", got)
		}
		beforeDisposition, beforeOperation := borrowedReplacementGuardRowRead(t, ctx, b.fixture.controlPool, b.fixture.guardKey)
		attemptID := borrowedReplacementOwnerJournalActualRefusal(t, ctx, b, fresh, "unarmed", &probeCalls, &acceptanceCalls)
		facts := &borrowedReplacementOwnerJournalFacts{}
		outcome := borrowedReplacementOwnerJournalRun(ctx, t, b, fresh, &borrowedReplacementOwnerJournalStage{
			key: b.fixture.guardKey, expectedOperation: b.fixture.operation,
			insert: true, attemptID: attemptID, contenderProof: true,
		}, facts, &borrowedReplacementOwnerTailHooks{})
		if outcome.runErr != nil || outcome.flow.ownerErr != nil {
			t.Fatalf("commit-ambiguity unarmed control refused: runErr=%v ownerErr=%v", outcome.runErr, outcome.flow.ownerErr)
		}
		if !outcome.flow.state.compositePublished() || outcome.flow.state.lostNow() {
			t.Fatalf("commit-ambiguity unarmed terminal state: composite=%t lost=%t", outcome.flow.state.compositePublished(), outcome.flow.state.lostNow())
		}
		if !facts.inserted || facts.auditID <= 0 {
			t.Fatal("commit-ambiguity unarmed control did not insert the refused row")
		}
		if count := borrowedReplacementOwnerJournalCount(t, ctx, b.fixture.controlPool, attemptID); count != 1 {
			t.Fatalf("commit-ambiguity unarmed durable rows=%d, want exactly 1", count)
		}
		if fault.Witness() != recovery.TargetLockCommitWitnessNotEstablished {
			t.Fatalf("commit-ambiguity UNARMED fault claimed witness %q", fault.Witness())
		}
		witnessCtx, cancelWitness := context.WithTimeout(ctx, 2*time.Second)
		unarmedDispatch := fault.AwaitDispatched(witnessCtx)
		cancelWitness()
		if unarmedDispatch {
			t.Fatal("commit-ambiguity UNARMED fault claimed a COMMIT dispatch")
		}
		postDisposition, postOperation := borrowedReplacementGuardRowRead(t, ctx, b.fixture.controlPool, b.fixture.guardKey)
		if postDisposition != beforeDisposition || postOperation != beforeOperation {
			t.Fatal("commit-ambiguity unarmed control changed the guard contents")
		}
		if err := borrowedReplacementOwnerTailRetireGuarded(t, ctx, b, outcome); err != nil {
			t.Fatalf("commit-ambiguity unarmed guarded retirement refused: %v", err)
		}
		borrowedOwnerDDLGuard(t, ctx, b)
		reuseCtx, cancelReuse := context.WithTimeout(ctx, 30*time.Second)
		reuseErr := outcome.flow.reg.Use(reuseCtx)
		copied := *outcome.flow.reg
		copyErr := copied.Use(reuseCtx)
		cancelReuse()
		if reuseErr == nil || copyErr == nil {
			t.Fatal("commit-ambiguity unarmed replay/copy use was accepted")
		}
		ordinaryID := borrowedReplacementOwnerJournalActualRefusal(t, ctx, b, fresh, "ordinary", &probeCalls, &acceptanceCalls)
		t.Logf("commit-ambiguity unarmed control: the UNARMED fixed fault passed all ordinary traffic (no witness claimed), the COMMIT was acknowledged with exactly ONE genuine refused row (audit_id=%d, attempt %s), the guard stayed unchanged, guarded retirement ran, replay/copy refused and the ordinary native-ready attempt was still refused by the dirty guard", facts.auditID, ordinaryID)

		// Reservation/consume controls with ACTUAL PG behavior.
		keyA := recovery.TargetKey{}
		keyA[0] = 0xA1
		keyB := recovery.TargetKey{}
		keyB[0] = 0xA2
		keyC := recovery.TargetKey{}
		keyC[0] = 0xA3
		repeatFault, repeatReset, repeatErr := recovery.ArmTargetLockCommitAmbiguityFault()
		if repeatErr != nil {
			t.Fatalf("commit-ambiguity repeat reservation refused: %v", repeatErr)
		}
		lockA := borrowedReplacementCommitAmbiguityAcquire(t, ctx, b.fixture.controlDSN, keyA, "repeat-1")
		if got := repeatFault.WrappedCount(); got != 1 {
			t.Fatalf("commit-ambiguity repeat reservation wrapped %d sockets, want exactly 1", got)
		}
		borrowedReplacementCommitAmbiguityOrdinaryCommit(t, ctx, lockA, "repeat-1")
		lockB := borrowedReplacementCommitAmbiguityAcquire(t, ctx, b.fixture.controlDSN, keyB, "repeat-2")
		if got := repeatFault.WrappedCount(); got != 1 {
			t.Fatalf("commit-ambiguity a later acquisition was wrapped by the consumed installation: %d", got)
		}
		borrowedReplacementCommitAmbiguityOrdinaryCommit(t, ctx, lockB, "repeat-2")
		repeatReset()
		// Overlapping reservations are rejected.
		newerFault, newerReset, newerErr := recovery.ArmTargetLockCommitAmbiguityFault()
		if newerErr != nil {
			t.Fatalf("commit-ambiguity newer reservation refused: %v", newerErr)
		}
		if _, _, overlapErr := recovery.ArmTargetLockCommitAmbiguityFault(); overlapErr == nil {
			t.Fatal("commit-ambiguity overlapping reservation was accepted")
		}
		// Stale resets must never clear the newer reserved installation.
		repeatReset()
		lockC := borrowedReplacementCommitAmbiguityAcquire(t, ctx, b.fixture.controlDSN, keyC, "stale-reset")
		if got := newerFault.WrappedCount(); got != 1 {
			t.Fatalf("commit-ambiguity stale reset cleared the newer installation: wrapped=%d", got)
		}
		borrowedReplacementCommitAmbiguityOrdinaryCommit(t, ctx, lockC, "stale-reset")
		// Actual gate behavior on the newer installation: arm its gate, withhold
		// its validated completion, then break so the ACTUAL commit errors.
		newerFault.ArmGate()
		commitErrCh := make(chan error, 1)
		commitDone := make(chan struct{})
		t.Cleanup(func() {
			newerFault.Break()
			select {
			case <-commitDone:
			case <-time.After(10 * time.Second):
				t.Errorf("commit-ambiguity newer-installation reader did not finish within the bounded unwind join")
			}
		})
		go func() {
			defer close(commitDone)
			txCtx, cancelTx := context.WithTimeout(ctx, 60*time.Second)
			commitErrCh <- lockC.WithTransaction(txCtx, func(context.Context, pgx.Tx) error { return nil })
			cancelTx()
		}()
		newerWithheldCtx, cancelNewerWithheld := context.WithTimeout(ctx, 30*time.Second)
		newerWithheld := newerFault.AwaitWithheld(newerWithheldCtx)
		cancelNewerWithheld()
		if !newerWithheld {
			t.Fatal("commit-ambiguity newer installation did not withhold its VALIDATED COMMIT completion")
		}
		newerFault.Break()
		select {
		case <-commitDone:
		case <-time.After(30 * time.Second):
			t.Fatal("commit-ambiguity newer-installation commit did not finish after the break")
		}
		newerCommitErr := <-commitErrCh
		if newerCommitErr == nil || !strings.Contains(newerCommitErr.Error(), "commit target-lock acceptance transaction") {
			t.Fatalf("commit-ambiguity newer-installation commit was not the ambiguous COMMIT path: %v", newerCommitErr)
		}
		if strings.Contains(newerCommitErr.Error(), "rollback target-lock acceptance transaction") {
			t.Fatalf("commit-ambiguity newer-installation error must never be a rollback: %v", newerCommitErr)
		}
		newerReset()
		t.Logf("commit-ambiguity reservation controls: the reserved installation was consumed by exactly ONE real acquisition (repeated acquisitions unwrapped and committed ordinarily), overlapping reservations were rejected, stale resets never cleared the newer installation, and the newer installation's armed gate actually withheld its VALIDATED COMMIT completion until the transport break made the ACTUAL commit return an ambiguous-COMMIT error")
	}()

	// N2: pre-COMMIT cancellation with the gate ARMED: the REAL arbitration
	// refuses precisely, no COMMIT is dispatched, the rollback is confirmed
	// successful, zero journal rows are durable and the witness-timeout stays
	// NOTESTABLISHED (never inferred as a rollback).
	func() {
		fault, reset, err := recovery.ArmTargetLockCommitAmbiguityFault()
		if err != nil {
			t.Fatalf("commit-ambiguity pre-COMMIT reservation refused: %v", err)
		}
		defer reset()
		b, fresh := borrowedReplacementGuardRowFixtures(t, ctx)
		if got := fault.WrappedCount(); got != 1 {
			t.Fatalf("commit-ambiguity pre-COMMIT fault wrapped %d owner sockets, want exactly 1", got)
		}
		attemptID := borrowedReplacementOwnerJournalActualRefusal(t, ctx, b, fresh, "precommit-cancel", &probeCalls, &acceptanceCalls)
		facts := &borrowedReplacementOwnerJournalFacts{}
		var canceled atomic.Bool
		outcome := borrowedReplacementOwnerJournalRun(ctx, t, b, fresh, &borrowedReplacementOwnerJournalStage{
			key: b.fixture.guardKey, expectedOperation: b.fixture.operation,
			insert: true, attemptID: attemptID, contenderProof: true,
		}, facts, &borrowedReplacementOwnerTailHooks{
			atArbitration: func(_ context.Context, _ *borrowedReplacementAutonomousPump, cancelLane func(), _ pgx.Tx) error {
				if cancelLane == nil {
					t.Fatal("commit-ambiguity pre-COMMIT cancel control has no lane cancel")
				}
				fault.ArmGate()
				canceled.Store(true)
				cancelLane()
				return nil
			},
		})
		if !facts.inserted || !canceled.Load() {
			t.Fatalf("commit-ambiguity pre-COMMIT control inserted=%t canceled=%t", facts.inserted, canceled.Load())
		}
		if !errors.Is(outcome.flow.ownerErr, errOwnerTailArbitration) {
			t.Fatalf("commit-ambiguity pre-COMMIT refusal was not the ACTUAL arbitration stage: %v", outcome.flow.ownerErr)
		}
		if outcome.flow.ownerErr == nil || strings.Contains(outcome.flow.ownerErr.Error(), "commit target-lock acceptance transaction") {
			t.Fatalf("commit-ambiguity pre-COMMIT control attempted a COMMIT: %v", outcome.flow.ownerErr)
		}
		if strings.Contains(outcome.flow.ownerErr.Error(), "rollback target-lock acceptance transaction") {
			t.Fatalf("commit-ambiguity pre-COMMIT rollback was not confirmed clean: %v", outcome.flow.ownerErr)
		}
		if outcome.state.eligibleNow() || outcome.flow.state.compositePublished() {
			t.Fatal("commit-ambiguity pre-COMMIT control still produced eligibility/composite")
		}
		if count := borrowedReplacementOwnerJournalCount(t, ctx, b.fixture.controlPool, attemptID); count != 0 {
			t.Fatalf("commit-ambiguity pre-COMMIT refusal left %d durable journal rows", count)
		}
		witnessCtx, cancelWitness := context.WithTimeout(ctx, 2*time.Second)
		dispatched := fault.AwaitDispatched(witnessCtx)
		cancelWitness()
		if dispatched {
			t.Fatal("commit-ambiguity pre-COMMIT refusal still dispatched a COMMIT")
		}
		if fault.Witness() != recovery.TargetLockCommitWitnessNotEstablished {
			t.Fatalf("commit-ambiguity pre-COMMIT witness=%q, want not-established", fault.Witness())
		}
		if fault.CleanupUncertain() {
			t.Fatal("commit-ambiguity pre-COMMIT control left an uncertain cleanup")
		}
		healthCtx, cancelHealth := context.WithTimeout(ctx, borrowedOwnerRotationHealthBudget)
		healthErr := b.fixture.lock.Health(healthCtx)
		cancelHealth()
		if healthErr != nil {
			t.Fatalf("commit-ambiguity pre-COMMIT rollback did not preserve the owner session: %v", healthErr)
		}
		t.Logf("commit-ambiguity pre-COMMIT cancellation: the committed rollback was confirmed clean, NO COMMIT was dispatched (witness-timeout stayed not-established, never inferred as a rollback), zero journal rows were durable and the owner session survived")
	}()

	// N3: deterministic fixed-control negatives with the in-memory transport:
	// unrecognized framing, bare COMMIT text, failed reads, partial frames,
	// missing barriers and witness timeouts stay NOTESTABLISHED/UNCERTAIN; the
	// REAL completion frames are validated and withheld; a deadline change
	// interrupts the withheld wait WITHOUT upgrading the witness.
	func() {
		unrecognized := recovery.NewTargetLockCommitAmbiguityControl()
		unrecognized.ArmGate()
		if err := unrecognized.ControlWriteClientBytes([]byte("garbage-frame")); err != nil {
			t.Fatalf("commit-ambiguity unrecognized framing write refused: %v", err)
		}
		if unrecognized.Witness() != recovery.TargetLockCommitWitnessNotEstablished || unrecognized.UnrecognizedFramingCount() == 0 {
			t.Fatal("commit-ambiguity unrecognized framing claimed an establishment")
		}
		unrecognized.Break()
		if !unrecognized.CleanupUncertain() {
			t.Fatal("commit-ambiguity unrecognized framing cleanup was not uncertain")
		}
		// The bare "COMMIT\0" server text is NOT a validated completion frame.
		bare := recovery.NewTargetLockCommitAmbiguityControl()
		bare.ArmGate()
		if err := bare.ControlWriteClientBytes(recovery.TargetLockCommitRequestFrame()); err != nil {
			t.Fatalf("commit-ambiguity bare-completion request refused: %v", err)
		}
		bare.ControlFeedServerBytes([]byte("COMMIT\x00"))
		if _, err := bare.ControlRead(make([]byte, 64)); err == nil {
			t.Fatal("commit-ambiguity bare COMMIT text was accepted as a completion")
		}
		if bare.Witness() == recovery.TargetLockCommitWitnessWithheld || !bare.ReadFailed() {
			t.Fatal("commit-ambiguity bare COMMIT text published WITHHELD")
		}
		bare.Break()
		if !bare.CleanupUncertain() {
			t.Fatal("commit-ambiguity bare-COMMIT cleanup was not uncertain")
		}
		// Failed read after dispatch.
		failedRead := recovery.NewTargetLockCommitAmbiguityControl()
		failedRead.ArmGate()
		if err := failedRead.ControlWriteClientBytes(recovery.TargetLockCommitRequestFrame()); err != nil {
			t.Fatalf("commit-ambiguity failed-read request refused: %v", err)
		}
		failedRead.ControlFeedServerError(io.EOF)
		if _, err := failedRead.ControlRead(make([]byte, 64)); err == nil {
			t.Fatal("commit-ambiguity failed read was accepted as a completion")
		}
		if failedRead.Witness() != recovery.TargetLockCommitWitnessDispatched || !failedRead.ReadFailed() {
			t.Fatal("commit-ambiguity failed read published WITHHELD")
		}
		failedRead.Break()
		if !failedRead.CleanupUncertain() {
			t.Fatal("commit-ambiguity failed-read cleanup was not uncertain")
		}
		// Partial completion frame then a deadline read failure.
		partial := recovery.NewTargetLockCommitAmbiguityControl()
		partial.ArmGate()
		if err := partial.ControlWriteClientBytes(recovery.TargetLockCommitRequestFrame()); err != nil {
			t.Fatalf("commit-ambiguity partial-frame request refused: %v", err)
		}
		partial.ControlFeedServerBytes(recovery.TargetLockCommitCompletionFrames()[:6])
		partial.ControlFeedServerError(os.ErrDeadlineExceeded)
		if _, err := partial.ControlRead(make([]byte, 64)); err == nil {
			t.Fatal("commit-ambiguity partial frame was accepted as a completion")
		}
		if partial.Witness() == recovery.TargetLockCommitWitnessWithheld || !partial.ReadFailed() {
			t.Fatal("commit-ambiguity partial frame published WITHHELD")
		}
		partial.Break()
		if !partial.CleanupUncertain() {
			t.Fatal("commit-ambiguity partial-frame cleanup was not uncertain")
		}
		// Missing barriers: a break before any establishment.
		missingBarriers := recovery.NewTargetLockCommitAmbiguityControl()
		missingBarriers.ArmGate()
		missingBarriers.Break()
		if missingBarriers.Witness() != recovery.TargetLockCommitWitnessNotEstablished || !missingBarriers.CleanupUncertain() {
			t.Fatal("commit-ambiguity missing-barrier break claimed an establishment")
		}
		// Witness timeout: no COMMIT within the bound.
		timeoutGate := recovery.NewTargetLockCommitAmbiguityControl()
		timeoutGate.ArmGate()
		timeoutCtx, cancelTimeout := context.WithTimeout(ctx, 100*time.Millisecond)
		timedOut := timeoutGate.AwaitDispatched(timeoutCtx)
		cancelTimeout()
		if timedOut || timeoutGate.Witness() != recovery.TargetLockCommitWitnessNotEstablished {
			t.Fatal("commit-ambiguity witness timeout claimed an establishment")
		}
		timeoutGate.Break()
		// The REAL completion frames are validated and withheld; the withheld
		// wait honors a deadline change without upgrading the witness, and its
		// reader always has an immediate break/bounded-join cleanup.
		recognized := recovery.NewTargetLockCommitAmbiguityControl()
		recognized.ArmGate()
		if err := recognized.ControlWriteClientBytes(recovery.TargetLockCommitRequestFrame()); err != nil {
			t.Fatalf("commit-ambiguity recognized request refused: %v", err)
		}
		recognized.ControlFeedServerBytes(recovery.TargetLockCommitCompletionFrames())
		readDone := make(chan struct{})
		var readErr error
		t.Cleanup(func() {
			recognized.Break()
			select {
			case <-readDone:
			case <-time.After(10 * time.Second):
				t.Errorf("commit-ambiguity control reader did not finish within the bounded unwind join")
			}
		})
		go func() {
			defer close(readDone)
			_, readErr = recognized.ControlRead(make([]byte, 64))
		}()
		withheldCtx, cancelWithheld := context.WithTimeout(ctx, 5*time.Second)
		withheld := recognized.AwaitWithheld(withheldCtx)
		cancelWithheld()
		if !withheld || recognized.Witness() != recovery.TargetLockCommitWitnessWithheld {
			t.Fatal("commit-ambiguity REAL completion frames were not validated and withheld")
		}
		if recognized.CleanupUncertain() {
			t.Fatal("commit-ambiguity validated completion was reported uncertain")
		}
		recognized.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		select {
		case <-readDone:
		case <-time.After(5 * time.Second):
			t.Fatal("commit-ambiguity withheld wait ignored the deadline change")
		}
		if !errors.Is(readErr, os.ErrDeadlineExceeded) {
			t.Fatalf("commit-ambiguity withheld wait error=%v, want the bounded deadline", readErr)
		}
		if recognized.Witness() != recovery.TargetLockCommitWitnessWithheld {
			t.Fatal("commit-ambiguity deadline change upgraded or cleared the witness")
		}
		recognized.Break()
		t.Logf("commit-ambiguity gate negatives: unrecognized framing, bare COMMIT text, failed reads, partial frames, missing barriers and witness timeouts all stayed NOTESTABLISHED/UNCERTAIN and were never inferred as a rollback; the REAL PostgreSQL COMMIT completion frames were validated and withheld, and a deadline change ended the withheld wait boundedly without upgrading the witness")
	}()

	if got := atomic.LoadInt32(&probeCalls); got != 0 {
		t.Fatalf("commit-ambiguity lane ran the ordinary probe %d times, want 0", got)
	}
	if got := atomic.LoadInt32(&acceptanceCalls); got != 0 {
		t.Fatalf("commit-ambiguity lane ran ordinary acceptance %d times, want 0", got)
	}
}
