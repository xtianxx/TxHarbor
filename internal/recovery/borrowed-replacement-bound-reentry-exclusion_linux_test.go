//go:build linux && drill

// borrowed-replacement-bound-reentry-exclusion_linux_test.go is the bounded
// controlled re-entry/exclusion prerequisite lane. On ONE authentic bound
// fixture it proves: the approved bound baseline -> genuine single-consumed
// receipt/entry -> same-owner P1 -> the existing orchestration five stages
// complete -> the unchanged comparator accepts the source capture with the
// authentic InstanceID/owner/anchor retained -> the bound retained-session
// prerequisite (exact P1 registered, used once, strict retirement) -> a
// complete pre-attempt snapshot (guard row, instance identity/generation/hash,
// NULL inventory, evidence/audit rows, replacement catalog) -> a fresh bound
// re-entry attempt (same real InstanceID, retained lock/store, P1 credentials,
// observer, sealed tools, new operation ID, new gate, run-bound prefix,
// independently verified archive, rejecting callbacks) whose lane-local
// Prelaunch wrapper runs the AUTHENTIC transactional marker helper exactly once
// (retaining the actual token) and then, using ONLY the supplied coordinator
// preparation transaction, invokes the unchanged guard-row fence for the
// original canonical key and original guard-operation provenance, requires the
// real row unresolved and signals a barrier only after marker success and
// actual row-lock acquisition. While the barrier is demonstrably held: a
// separate control-store transaction locks the same row FOR UPDATE NOWAIT and
// receives empty fields with the structured 55P03 refusal; a distinct control
// session pg_try_advisory_lock on the original target key succeeds as a query
// and returns FALSE with the original owner independently confirmed holding it;
// and a bounded AcquireTargetLock on the same key returns a NIL lock at the
// bounded acquisition stage (never a connect/config failure). The barrier is
// released without granting any authority, the actual coordinator continues and
// refuses with the exact bound dirty-guard refusal; afterwards there is no
// child, command, receipt, probe or acceptance output, the marker callback ran
// exactly once with a genuine transaction-local token, the marker/guard
// transaction rolled back with every durable snapshot and row unchanged, a
// fresh contender can lock and read the unchanged row and roll back, the
// original advisory owner is held and healthy, the original anchor rechecks,
// the replacement catalog is unchanged, the guard stays unresolved and the
// inventory stays NULL. This proves sampled advisory exclusion + a held-row
// interval + the durable dirty-guard refusal ONLY: there is no continuous
// exclusion, no external isolation, no admissible re-entry, no rebuild
// readiness, no controlled recovery API, no clean transition, no atomic
// acceptance, no manifest, no downstream and no Gate1 authority. Secrets,
// verifiers, DSNs and archive content are never logged.
package recovery_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xtianxx/txharbor/internal/recovery"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

// borrowedBoundReentryOutcome is the complete captured coordinator outcome.
type borrowedBoundReentryOutcome struct {
	result  recovery.TargetWriterResult
	receipt recovery.DrillTargetProcessReceipt
	err     error
}

// borrowedBoundReentryBarrier is the lane-local Prelaunch wrapper: the
// authentic transactional marker helper runs exactly once and its actual token
// is retained; then the unchanged guard-row fence runs through ONLY the
// supplied coordinator preparation transaction for the original canonical key
// and original guard-operation provenance and must lock the real unresolved
// row; the barrier is signalled only after that success. While held the
// callback waits for release or the caller context, so a cancellation observed
// at the held barrier refuses without fabricating a marker.
type borrowedBoundReentryBarrier struct {
	marker            *borrowedBoundNativeReadyMarker
	key               string
	expectedOperation string
	ownerPID          int

	entered     chan struct{}
	release     chan struct{}
	enteredOnce sync.Once
	releaseOnce sync.Once

	mu          sync.Mutex
	txPID       int
	fenced      bool
	disposition string
	operation   string
	fenceErr    error
	callbackErr error
	cancelled   bool
}

func newBorrowedBoundReentryBarrier(t *testing.T, attemptID, actorRole, key, expectedOperation string, ownerPID int) *borrowedBoundReentryBarrier {
	t.Helper()
	if key == "" || expectedOperation == "" || ownerPID <= 0 {
		t.Fatalf("bound re-entry barrier requires the original key/operation and the owner identity: key=%q operation=%q ownerPID=%d", key, expectedOperation, ownerPID)
	}
	return &borrowedBoundReentryBarrier{
		marker:            newBorrowedBoundNativeReadyMarker(attemptID, actorRole),
		key:               key,
		expectedOperation: expectedOperation,
		ownerPID:          ownerPID,
		entered:           make(chan struct{}),
		release:           make(chan struct{}),
	}
}

// hook is the lane-local Prelaunch callback. It returns the authentic marker
// token only after the marker succeeded, the supplied preparation transaction
// was proven distinct from the retained advisory-owner session, the unchanged
// guard-row fence locked the real unresolved row for the original canonical key
// and operation provenance, and the held barrier was released by the lane.
func (pb *borrowedBoundReentryBarrier) hook(hookCtx context.Context, tx pgx.Tx, locked controlstore.InstanceToken) (recovery.EvidenceToken, error) {
	token, err := pb.marker.hook(hookCtx, tx, locked)
	if err != nil {
		pb.setCallbackErr(err)
		return recovery.EvidenceToken{}, err
	}
	var txPID int
	if identErr := tx.QueryRow(hookCtx, `SELECT pg_backend_pid()`).Scan(&txPID); identErr != nil {
		identRefusal := errors.New("re-entry callback transaction identity refused")
		pb.setCallbackErr(identRefusal)
		return recovery.EvidenceToken{}, identRefusal
	}
	pb.mu.Lock()
	pb.txPID = txPID
	pb.mu.Unlock()
	if txPID == pb.ownerPID {
		ownerRefusal := errors.New("re-entry Prelaunch transaction is the retained advisory-owner session")
		pb.setCallbackErr(ownerRefusal)
		return recovery.EvidenceToken{}, ownerRefusal
	}
	disposition, operation, fenceErr := borrowedReplacementGuardRowFence(hookCtx, tx, pb.key, pb.expectedOperation)
	pb.mu.Lock()
	pb.disposition = disposition
	pb.operation = operation
	pb.fenceErr = fenceErr
	pb.mu.Unlock()
	if fenceErr != nil {
		pb.setCallbackErr(fenceErr)
		return recovery.EvidenceToken{}, fenceErr
	}
	pb.mu.Lock()
	pb.fenced = true
	pb.mu.Unlock()
	pb.enteredOnce.Do(func() { close(pb.entered) })
	select {
	case <-pb.release:
	case <-hookCtx.Done():
		pb.mu.Lock()
		pb.cancelled = true
		pb.callbackErr = hookCtx.Err()
		pb.mu.Unlock()
		return recovery.EvidenceToken{}, fmt.Errorf("re-entry callback caller context ended while the barrier was held: %w", hookCtx.Err())
	}
	return token, nil
}

func (pb *borrowedBoundReentryBarrier) setCallbackErr(err error) {
	pb.mu.Lock()
	pb.callbackErr = err
	pb.mu.Unlock()
}

func (pb *borrowedBoundReentryBarrier) releaseBarrier() {
	pb.releaseOnce.Do(func() { close(pb.release) })
}

func (pb *borrowedBoundReentryBarrier) txPIDNow() int {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	return pb.txPID
}

func (pb *borrowedBoundReentryBarrier) fencedNow() bool {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	return pb.fenced
}

func (pb *borrowedBoundReentryBarrier) fenceResultNow() (string, string, error) {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	return pb.disposition, pb.operation, pb.fenceErr
}

func (pb *borrowedBoundReentryBarrier) callbackErrNow() error {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	return pb.callbackErr
}

func (pb *borrowedBoundReentryBarrier) cancelledNow() bool {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	return pb.cancelled
}

// prepareBorrowedBoundReentryAttempt prepares the fresh bound re-entry attempt
// through the EXISTING common attempt constructor with the real bound options
// and the lane-local callback, asserting the preserved lineage fields and the
// deliberately NEW operation identity.
func prepareBorrowedBoundReentryAttempt(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding, attemptID string, prelaunch func(context.Context, pgx.Tx, controlstore.InstanceToken) (recovery.EvidenceToken, error), probeCalls, acceptanceCalls *int32) *borrowedReplacementPrelaunchAttempt {
	t.Helper()
	f := b.fixture
	attempt, err := newBorrowedReplacementPrelaunchAttemptWith(t, ctx, b, fresh, borrowedReplacementPrelaunchArchive(t, f), attemptID,
		probeCalls, acceptanceCalls, &borrowedReplacementPrelaunchBoundOptions{InstanceID: f.instanceID, Prelaunch: prelaunch})
	if err != nil || attempt == nil {
		t.Fatalf("bound re-entry attempt preparation refused: attempt=%v err=%v", attempt, err)
	}
	if attempt.source != fresh || attempt.gate == nil || attempt.prefix == nil || attempt.prefix.run != attempt.run {
		t.Fatal("bound re-entry attempt did not retain the genuine source capture, gate and fresh prefix")
	}
	runBinding := attempt.run.Binding()
	if runBinding.OriginalInstanceID() != f.instanceID ||
		runBinding.OriginalTargetKey() != fresh.binding.OriginalTargetKey() ||
		runBinding.OriginalRoleFingerprint() != fresh.binding.OriginalRoleFingerprint() {
		t.Fatalf("bound re-entry attempt lost the preserved lineage: %+v", runBinding)
	}
	if runBinding.OriginalOperationID() != attemptID || runBinding.OriginalOperationID() == fresh.binding.OriginalOperationID() {
		t.Fatalf("bound re-entry attempt did not carry its NEW operation identity: attempt=%q source=%q", runBinding.OriginalOperationID(), fresh.binding.OriginalOperationID())
	}
	return attempt
}

// borrowedBoundReentryAssertOutcomeBoundaries enforces the complete output
// contract for EVERY coordinator outcome: empty probe output, the exact opaque
// zero receipt with the exact absence refusal, no child incarnation and zero
// replacement probe/acceptance callback counts.
func borrowedBoundReentryAssertOutcomeBoundaries(t *testing.T, label string, attempt *borrowedReplacementPrelaunchAttempt, result recovery.TargetWriterResult, receipt recovery.DrillTargetProcessReceipt, probeCalls, acceptanceCalls *int32) {
	t.Helper()
	borrowedBoundNativeReadyRequireNoProbeOutput(t, label, result)
	borrowedBoundNativeReadyRequireAbsentReceipt(t, label, receipt)
	identity := attempt.run.Observation().StartedIdentity()
	if identity.Started || identity.PID != 0 || identity.StartID != 0 {
		t.Fatalf("%s: coordinator outcome started a child: %+v", label, identity)
	}
	if got := atomic.LoadInt32(probeCalls); got != 0 {
		t.Fatalf("%s: coordinator outcome ran the replacement probe %d times, want 0", label, got)
	}
	if got := atomic.LoadInt32(acceptanceCalls); got != 0 {
		t.Fatalf("%s: coordinator outcome ran acceptance %d times, want 0", label, got)
	}
}

// borrowedBoundReentryAssertCallbackFailurePublishedNothing requires a Prelaunch
// callback failure outcome to publish no marker token, no command outcome and no
// probe output while STILL carrying the trusted canonical target key and the
// legitimately initialized production attempt application tag: both are
// established before the callback runs and must never be zero or wrong.
func borrowedBoundReentryAssertCallbackFailurePublishedNothing(t *testing.T, label, expectedKey string, result recovery.TargetWriterResult) {
	t.Helper()
	if result.TargetKey.String() != expectedKey {
		t.Fatalf("%s: callback failure published a zero/wrong target key: got=%q want=%q", label, result.TargetKey.String(), expectedKey)
	}
	applicationErr := recovery.ValidateAttemptApplicationName(result.Application)
	if applicationErr != nil || !strings.HasPrefix(result.Application, "txh015_") || len(result.Application) != len("txh015_")+32 {
		t.Fatalf("%s: callback failure did not publish the legitimately initialized production attempt application tag: application=%q err=%v", label, result.Application, applicationErr)
	}
	if result.MarkerToken != (recovery.EvidenceToken{}) {
		t.Fatalf("%s: callback failure published a marker token: %+v", label, result.MarkerToken)
	}
	if result.Command != (recovery.PGCommandResult{}) {
		t.Fatalf("%s: callback failure published a command outcome: %+v", label, result.Command)
	}
	borrowedBoundNativeReadyRequireNoProbeOutput(t, label, result)
}

// borrowedBoundReentryOriginalKey derives the original canonical TargetKey from
// the fixture target DSN and requires it to match the retained guard key.
func borrowedBoundReentryOriginalKey(t *testing.T, f *borrowedAuthHandoffFixture) recovery.TargetKey {
	t.Helper()
	target, err := controlstore.ParseDSNTarget(f.writerTargetDSN)
	if err != nil {
		t.Fatalf("bound re-entry original target identity refused: %v", err)
	}
	key, err := recovery.CanonicalTargetKey(target)
	if err != nil {
		t.Fatalf("bound re-entry original target key refused: %v", err)
	}
	if key.String() != f.guardKey {
		t.Fatalf("bound re-entry original canonical key %q does not match the retained guard key %q", key.String(), f.guardKey)
	}
	return key
}

// borrowedBoundReentryDifferentKey derives a genuinely different canonical key
// from a real existing different database of the same fixture.
func borrowedBoundReentryDifferentKey(t *testing.T, b *borrowedSuccessorBaseline) recovery.TargetKey {
	t.Helper()
	dsn := borrowedAuthRoleDSN(t, b.fixture.writerSourceDSN, b.fixture.writerRole, b.passwordP1)
	target, err := controlstore.ParseDSNTarget(dsn)
	if err != nil {
		t.Fatalf("bound re-entry different-database target identity refused: %v", err)
	}
	key, err := recovery.CanonicalTargetKey(target)
	if err != nil {
		t.Fatalf("bound re-entry different-database target key refused: %v", err)
	}
	if key.String() == b.fixture.guardKey {
		t.Fatal("bound re-entry different-database key equals the original canonical key")
	}
	return key
}

// borrowedBoundReentryAdvisoryProbe uses a DISTINCT usable control session to
// attempt the original advisory key: the query must succeed and return FALSE,
// and the original owner must be independently confirmed holding it in
// pg_locks.
func borrowedBoundReentryAdvisoryProbe(t *testing.T, ctx context.Context, f *borrowedAuthHandoffFixture, originalKey recovery.TargetKey, ownerPID int) {
	t.Helper()
	if ownerPID <= 0 {
		t.Fatalf("bound re-entry advisory probe requires the positive owner pid, got %d", ownerPID)
	}
	k1, k2 := originalKey.AdvisoryLockKey()
	acqCtx, cancelAcq := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	poolConn, err := f.controlPool.Acquire(acqCtx)
	cancelAcq()
	if err != nil {
		t.Fatalf("bound re-entry advisory probe connection refused: %v", err)
	}
	defer poolConn.Release()
	var pid int
	pidCtx, cancelPID := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	if err := poolConn.QueryRow(pidCtx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
		cancelPID()
		t.Fatalf("bound re-entry advisory probe session identity refused: %v", err)
	}
	cancelPID()
	if pid <= 0 || pid == ownerPID {
		t.Fatalf("bound re-entry advisory probe session is not distinct/usable: pid=%d owner=%d", pid, ownerPID)
	}
	var acquired bool
	lockCtx, cancelLock := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	lockErr := poolConn.QueryRow(lockCtx, `SELECT pg_try_advisory_lock($1, $2)`, k1, k2).Scan(&acquired)
	cancelLock()
	if lockErr != nil {
		t.Fatalf("bound re-entry advisory try-lock query refused: %v", lockErr)
	}
	if acquired {
		unlockCtx, cancelUnlock := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
		_, _ = poolConn.Exec(unlockCtx, `SELECT pg_advisory_unlock($1, $2)`, k1, k2)
		cancelUnlock()
		t.Fatal("distinct control session acquired the original advisory key while the owner holds it")
	}
	var held int
	heldCtx, cancelHeld := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	heldErr := poolConn.QueryRow(heldCtx, `
SELECT count(*)::int FROM pg_locks
WHERE locktype='advisory' AND pid=$1
  AND classid::bigint = ($2::bigint & 4294967295)
  AND objid::bigint = ($3::bigint & 4294967295)`, ownerPID, k1, k2).Scan(&held)
	cancelHeld()
	if heldErr != nil || held != 1 {
		t.Fatalf("original advisory owner does not independently hold the original key: held=%d err=%v", held, heldErr)
	}
}

// borrowedBoundReentryAbsentRowFence proves the unchanged fence refuses a
// genuinely different canonical key whose row absence is independently proven,
// with empty outputs and the no-row sentinel, leaving the row absent and the
// original inventory untouched (no fabricated hash, no inventory fallback).
func borrowedBoundReentryAbsentRowFence(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline) {
	t.Helper()
	f := b.fixture
	absentKey := borrowedBoundReentryDifferentKey(t, b)
	if count := borrowedReplacementGuardRowCount(t, ctx, f.controlPool, absentKey.String()); count != 0 {
		t.Fatalf("absent-row control key already has %d guard rows", count)
	}
	beforeDisposition, beforeOperation := borrowedReplacementGuardRowRead(t, ctx, f.controlPool, f.guardKey)
	tx, err := f.controlPool.Begin(ctx)
	if err != nil {
		t.Fatalf("absent-row control transaction refused: %v", err)
	}
	disposition, operation, fenceErr := borrowedReplacementGuardRowFence(ctx, tx, absentKey.String(), f.operation)
	rollbackCtx, cancelRollback := context.WithTimeout(context.Background(), borrowedOwnerRotationCleanupBudget)
	_ = tx.Rollback(rollbackCtx)
	cancelRollback()
	if disposition != "" || operation != "" {
		t.Fatalf("absent-row fence returned outputs: disposition=%q operation=%q", disposition, operation)
	}
	if fenceErr == nil || !errors.Is(fenceErr, errGuardRowWindowRowRefused) || !strings.Contains(fenceErr.Error(), "no guard row exists") {
		t.Fatalf("absent-row fence was not refused at the no-row stage: %v", fenceErr)
	}
	if count := borrowedReplacementGuardRowCount(t, ctx, f.controlPool, absentKey.String()); count != 0 {
		t.Fatalf("absent-row fence created a fallback inventory row: count=%d", count)
	}
	if disposition, operation := borrowedReplacementGuardRowRead(t, ctx, f.controlPool, f.guardKey); disposition != beforeDisposition || operation != beforeOperation {
		t.Fatalf("absent-row control changed the original guard row: %q/%q -> %q/%q", beforeDisposition, beforeOperation, disposition, operation)
	}
	t.Logf("bound re-entry absent-row control: the unchanged fence refused the genuinely absent different canonical key with empty outputs and the no-row sentinel, the row stayed absent and the original inventory row was unchanged")
}

// borrowedBoundReentryProvenanceMismatch proves the unchanged fence refuses the
// REAL row when the supplied operation provenance is deliberately mismatched,
// with empty outputs at the operation-provenance stage and the row unchanged;
// this is never relabelled as an InstanceID isolation.
func borrowedBoundReentryProvenanceMismatch(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, mismatchedOperation string) {
	t.Helper()
	f := b.fixture
	beforeDisposition, beforeOperation := borrowedReplacementGuardRowRead(t, ctx, f.controlPool, f.guardKey)
	tx, err := f.controlPool.Begin(ctx)
	if err != nil {
		t.Fatalf("provenance-mismatch control transaction refused: %v", err)
	}
	disposition, operation, fenceErr := borrowedReplacementGuardRowFence(ctx, tx, f.guardKey, mismatchedOperation)
	rollbackCtx, cancelRollback := context.WithTimeout(context.Background(), borrowedOwnerRotationCleanupBudget)
	_ = tx.Rollback(rollbackCtx)
	cancelRollback()
	if disposition != "" || operation != "" {
		t.Fatalf("provenance-mismatch fence returned outputs: disposition=%q operation=%q", disposition, operation)
	}
	if fenceErr == nil || !errors.Is(fenceErr, errGuardRowWindowRowRefused) || !strings.Contains(fenceErr.Error(), "operation provenance") {
		t.Fatalf("provenance-mismatch fence was not refused at the operation-provenance stage: %v", fenceErr)
	}
	if disposition, operation := borrowedReplacementGuardRowRead(t, ctx, f.controlPool, f.guardKey); disposition != beforeDisposition || operation != beforeOperation {
		t.Fatalf("provenance-mismatch control changed the real guard row: %q/%q -> %q/%q", beforeDisposition, beforeOperation, disposition, operation)
	}
	t.Logf("bound re-entry provenance-mismatch control: the unchanged fence refused the real row at the operation-provenance stage (not an InstanceID isolation) with empty outputs and the row unchanged")
}

// borrowedBoundReentryDifferentKeyControl proves a genuinely different existing
// database of the same fixture yields a different canonical key whose advisory
// lock is actually acquirable through the real bounded acquirer, that only the
// temporary lock is released, and that no second recovery instance exists and
// the original owner is neither released nor reacquired.
func borrowedBoundReentryDifferentKeyControl(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding) {
	t.Helper()
	f := b.fixture
	differentKey := borrowedBoundReentryDifferentKey(t, b)
	lock, err := recovery.AcquireTargetLock(ctx, f.controlDSN, differentKey, 3*time.Second, 10*time.Millisecond)
	if err != nil || lock == nil {
		t.Fatalf("different-database canonical key was not acquirable through the real bounded acquirer: lock=%v err=%v", lock, err)
	}
	releaseCtx, cancelRelease := context.WithTimeout(ctx, borrowedOwnerRotationCleanupBudget)
	releaseErr := lock.Release(releaseCtx)
	cancelRelease()
	if releaseErr != nil {
		t.Fatalf("different-database temporary lock release refused: %v", releaseErr)
	}
	var openInstances int
	countCtx, cancelCount := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	countErr := f.controlPool.QueryRow(countCtx, `SELECT count(*)::int FROM recovery_instance WHERE instance_id=$1 AND state='open'`, f.instanceID).Scan(&openInstances)
	cancelCount()
	if countErr != nil || openInstances != 1 {
		t.Fatalf("different-key control changed the single-open instance state: open=%d err=%v", openInstances, countErr)
	}
	if ownerPID := fresh.binding.ControlBackendPID(); ownerPID <= 0 {
		t.Fatalf("different-key control lost the original owner identity: %d", ownerPID)
	}
	t.Logf("bound re-entry different-key control: the real different-database canonical key was acquirable and only its temporary lock was released; the single authentic instance stayed open and the original owner was neither released nor reacquired")
}

// borrowedBoundReentryReplayRefuse proves the pre-dispatch retained attempt and
// run copies cannot register, dispatch or replay after the one actual dispatch:
// each later call refuses at its real stage and publishes nothing.
func borrowedBoundReentryReplayRefuse(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, attempt *borrowedReplacementPrelaunchAttempt, attemptCopy *borrowedReplacementPrelaunchAttempt, runCopy *recovery.DrillBorrowedWriterRun, marker *borrowedBoundNativeReadyMarker, probeCalls, acceptanceCalls *int32, snap borrowedBoundSessionSnapshot, rows borrowedBoundNativeReadyRows) {
	t.Helper()
	if err := attemptCopy.registerOrigin(ctx); err == nil || !strings.Contains(err.Error(), "already frozen") {
		t.Fatalf("later bound re-entry registration was not refused at the frozen gate stage: %v", err)
	}
	runCtx, cancelRun := context.WithTimeout(ctx, 60*time.Second)
	dispatchResult, dispatchReceipt, dispatchErr := attemptCopy.dispatchRun(runCtx)
	cancelRun()
	if dispatchErr == nil || !strings.Contains(dispatchErr.Error(), "already reserved") {
		t.Fatalf("later bound re-entry dispatch was not refused as already reserved: %v", dispatchErr)
	}
	borrowedBoundNativeReadyRequireNothingPublished(t, "later bound re-entry dispatch", dispatchResult, dispatchReceipt)
	runCopyCtx, cancelRunCopy := context.WithTimeout(ctx, 30*time.Second)
	copyResult, copyReceipt, copyErr := runCopy.Run(runCopyCtx)
	cancelRunCopy()
	if copyErr == nil || !strings.Contains(copyErr.Error(), "already reserved") {
		t.Fatalf("copied bound re-entry run replay was not refused as already reserved: %v", copyErr)
	}
	borrowedBoundNativeReadyRequireNothingPublished(t, "copied bound re-entry run replay", copyResult, copyReceipt)
	borrowedBoundReentryAssertOutcomeBoundaries(t, "bound re-entry replay", attempt, dispatchResult, dispatchReceipt, probeCalls, acceptanceCalls)
	if got := marker.callsNow(); got != 1 {
		t.Fatalf("bound re-entry replay marker calls=%d, want exactly 1", got)
	}
	borrowedBoundSessionAssertSnapshotEqual(t, "bound re-entry replay refusal", snap, borrowedBoundSessionSnapshotNow(t, ctx, b))
	if after := borrowedBoundNativeReadyRowsNow(t, ctx, b); after != rows {
		t.Fatalf("bound re-entry replay refusal left durable rows: before=%+v after=%+v", rows, after)
	}
	t.Logf("bound re-entry replay/copy control: the later registration was refused at the frozen gate, the later dispatch and copied run were refused as already reserved, and nothing was published")
}

// borrowedBoundReentryRowHolderFirstControl holds the REAL guard row through an
// independent transaction first: the callback's unchanged fence receives the
// structured 55P03 with empty outputs and the barrier never reports successful
// fencing; the production refusal is the sanitized marker-hook error, and the
// real stage error is recorded lane-locally.
func borrowedBoundReentryRowHolderFirstControl(t *testing.T, ctx context.Context) {
	t.Helper()
	b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
	f := b.fixture
	ownerPID := fresh.binding.ControlBackendPID()
	if ownerPID <= 0 {
		t.Fatalf("bound re-entry row-holder-first owner identity is missing: %d", ownerPID)
	}
	// Complete pre-attempt durable baseline captured BEFORE the hold and the
	// attempt: the full instance snapshot, the evidence/audit row digest, the
	// replacement catalog identity and the real guard row, so an independently
	// committed write during the refusal cannot escape the post comparison.
	snap0 := borrowedBoundSessionSnapshotNow(t, ctx, b)
	rows0 := borrowedBoundNativeReadyRowsNow(t, ctx, b)
	catalogOID, oidErr := borrowedOwnerDDLTargetOID(ctx, b)
	if oidErr != nil || catalogOID != fresh.replacementOID {
		t.Fatalf("bound re-entry row-holder-first replacement catalog identity before the attempt: oid=%d err=%v", catalogOID, oidErr)
	}
	beforeDisposition, beforeOperation := borrowedReplacementGuardRowRead(t, ctx, f.controlPool, f.guardKey)
	holder, err := borrowedReplacementGuardRowHoldRow(ctx, f.controlPool, f.guardKey)
	if err != nil {
		t.Fatalf("bound re-entry row-holder-first hold refused: %v", err)
	}
	defer holder.release()
	var probeCalls, acceptanceCalls int32
	attemptID := fmt.Sprintf("borrowed-replacement-bound-reentry-holder-%d", time.Now().UnixNano())
	barrier := newBorrowedBoundReentryBarrier(t, attemptID, f.adminRole, f.guardKey, f.operation, ownerPID)
	attempt := prepareBorrowedBoundReentryAttempt(t, ctx, b, fresh, attemptID, barrier.hook, &probeCalls, &acceptanceCalls)
	if err := attempt.registerOrigin(ctx); err != nil {
		t.Fatalf("bound re-entry row-holder-first origin registration refused: %v", err)
	}
	result, receipt, runErr := dispatchBorrowedBoundNativeReady(t, ctx, attempt)
	if runErr == nil {
		t.Fatal("bound re-entry row-holder-first attempt was accepted")
	}
	if runErr.Error() != "transactional restore-start marker hook failed" {
		t.Fatalf("row-holder-first refusal is not the production-sanitized marker-hook refusal: %v", runErr)
	}
	if strings.Contains(runErr.Error(), "55P03") || strings.Contains(runErr.Error(), "guard-row") {
		t.Fatalf("row-holder-first refusal leaked the raw fence stage through sanitization: %v", runErr)
	}
	if got := barrier.marker.callsNow(); got != 1 {
		t.Fatalf("row-holder-first marker calls=%d, want exactly 1", got)
	}
	if token := barrier.marker.token(); token.InstanceID != f.instanceID || token.State != "open" || token.Generation <= 0 || token.Hash == "" {
		t.Fatalf("row-holder-first marker did not return the genuine transaction-local token: %+v", token)
	}
	if barrier.fencedNow() {
		t.Fatal("row-holder-first barrier reported successful fencing while the row was held elsewhere")
	}
	select {
	case <-barrier.entered:
		t.Fatal("row-holder-first barrier signalled entry without a successful fence")
	default:
	}
	disposition, operation, fenceErr := barrier.fenceResultNow()
	if disposition != "" || operation != "" {
		t.Fatalf("row-holder-first fence returned outputs: disposition=%q operation=%q", disposition, operation)
	}
	if fenceErr == nil || !errors.Is(fenceErr, errGuardRowWindowRowRefused) || borrowedReplacementGuardRowCode(fenceErr) != "55P03" {
		t.Fatalf("row-holder-first fence was not refused with the structured NOWAIT stage: code=%s err=%v", borrowedReplacementGuardRowCode(fenceErr), fenceErr)
	}
	if callbackErr := barrier.callbackErrNow(); callbackErr == nil || !errors.Is(callbackErr, errGuardRowWindowRowRefused) {
		t.Fatalf("row-holder-first callback did not record the real row-fence stage: %v", callbackErr)
	}
	borrowedBoundReentryAssertCallbackFailurePublishedNothing(t, "row-holder-first", f.guardKey, result)
	borrowedBoundReentryAssertOutcomeBoundaries(t, "row-holder-first", attempt, result, receipt, &probeCalls, &acceptanceCalls)
	borrowedBoundSessionAssertSnapshotEqual(t, "bound re-entry row-holder-first", snap0, borrowedBoundSessionSnapshotNow(t, ctx, b))
	if after := borrowedBoundNativeReadyRowsNow(t, ctx, b); after != rows0 {
		t.Fatalf("row-holder-first refusal left durable evidence/audit rows: before=%+v after=%+v", rows0, after)
	}
	if afterOID, oidErr := borrowedOwnerDDLTargetOID(ctx, b); oidErr != nil || afterOID != catalogOID {
		t.Fatalf("row-holder-first refusal changed the replacement catalog: oid=%d err=%v", afterOID, oidErr)
	}
	holder.release()
	if disposition, operation := borrowedReplacementGuardRowRead(t, ctx, f.controlPool, f.guardKey); disposition != beforeDisposition || operation != beforeOperation {
		t.Fatalf("row-holder-first control changed the guard row: %q/%q -> %q/%q", beforeDisposition, beforeOperation, disposition, operation)
	}
	if released, releasedOperation, releasedErr := borrowedReplacementGuardRowContenderLock(ctx, f.controlPool, f.guardKey); releasedErr != nil || released != beforeDisposition || releasedOperation != beforeOperation {
		t.Fatalf("released row is not lockable/unchanged: %q/%q err=%v", released, releasedOperation, releasedErr)
	}
	borrowedOwnerDDLGuard(t, ctx, b)
	t.Logf("bound re-entry row-holder-first control: the callback fence received the structured 55P03 with empty outputs and no successful fencing, the production refusal was the sanitized marker-hook error, and the released row was lockable/unchanged with the complete pre-attempt snapshot/rows/catalog unchanged")
}

// borrowedBoundReentryCancellationControl proves a caller cancellation observed
// ONLY after the marker succeeded and the row was fenced refuses through the
// callback while the barrier is held: the callback records the cancellation,
// the production refusal is the sanitized marker-hook error, no durable marker
// or row mutation survives, and the join is bounded with an immediate
// cancel/release unwind.
func borrowedBoundReentryCancellationControl(t *testing.T, ctx context.Context) {
	t.Helper()
	b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
	f := b.fixture
	ownerPID := fresh.binding.ControlBackendPID()
	if ownerPID <= 0 {
		t.Fatalf("bound re-entry cancellation owner identity is missing: %d", ownerPID)
	}
	snap0 := borrowedBoundSessionSnapshotNow(t, ctx, b)
	rows0 := borrowedBoundNativeReadyRowsNow(t, ctx, b)
	var probeCalls, acceptanceCalls int32
	attemptID := fmt.Sprintf("borrowed-replacement-bound-reentry-cancel-%d", time.Now().UnixNano())
	barrier := newBorrowedBoundReentryBarrier(t, attemptID, f.adminRole, f.guardKey, f.operation, ownerPID)
	attempt := prepareBorrowedBoundReentryAttempt(t, ctx, b, fresh, attemptID, barrier.hook, &probeCalls, &acceptanceCalls)
	if err := attempt.registerOrigin(ctx); err != nil {
		t.Fatalf("bound re-entry cancellation origin registration refused: %v", err)
	}
	runCtx, cancelRun := context.WithCancel(ctx)
	done := make(chan borrowedBoundReentryOutcome, 1)
	go func() {
		result, receipt, err := attempt.dispatchRun(runCtx)
		done <- borrowedBoundReentryOutcome{result: result, receipt: receipt, err: err}
	}()
	joined := false
	unwind := func() {
		cancelRun()
		barrier.releaseBarrier()
		if joined {
			return
		}
		select {
		case <-done:
			joined = true
		case <-time.After(30 * time.Second):
			t.Errorf("canceled bound re-entry dispatch did not complete within the bounded unwind; outcome unknown")
		}
	}
	defer unwind()
	t.Cleanup(unwind)
	select {
	case <-barrier.entered:
	case <-time.After(60 * time.Second):
		t.Errorf("bound re-entry cancellation never reached the confirmed marker/fence barrier; completion unknown")
		return
	}
	cancelRun()
	select {
	case out := <-done:
		joined = true
		if out.err == nil || out.err.Error() != "transactional restore-start marker hook failed" {
			t.Fatalf("held-barrier cancellation refusal is not the sanitized marker-hook refusal: %v", out.err)
		}
		borrowedBoundReentryAssertCallbackFailurePublishedNothing(t, "held-barrier cancellation", f.guardKey, out.result)
		borrowedBoundReentryAssertOutcomeBoundaries(t, "held-barrier cancellation", attempt, out.result, out.receipt, &probeCalls, &acceptanceCalls)
	case <-time.After(30 * time.Second):
		t.Errorf("held-barrier cancellation did not complete within the bounded join; outcome unknown")
		return
	}
	if !barrier.cancelledNow() {
		t.Fatal("held-barrier cancellation was not observed by the callback")
	}
	if got := barrier.marker.callsNow(); got != 1 {
		t.Fatalf("held-barrier cancellation marker calls=%d, want exactly 1", got)
	}
	if !barrier.fencedNow() {
		t.Fatal("held-barrier cancellation did not fence the row before the cancel")
	}
	borrowedBoundSessionAssertSnapshotEqual(t, "held-barrier cancellation", snap0, borrowedBoundSessionSnapshotNow(t, ctx, b))
	if after := borrowedBoundNativeReadyRowsNow(t, ctx, b); after != rows0 {
		t.Fatalf("held-barrier cancellation left durable rows: before=%+v after=%+v", rows0, after)
	}
	disposition, operation := borrowedReplacementGuardRowRead(t, ctx, f.controlPool, f.guardKey)
	if disposition == "clean" {
		t.Fatal("held-barrier cancellation resolved the deliberately unresolved guard")
	}
	if released, releasedOperation, releasedErr := borrowedReplacementGuardRowContenderLock(ctx, f.controlPool, f.guardKey); releasedErr != nil || released != disposition || releasedOperation != operation {
		t.Fatalf("held-barrier cancellation left the row unlocked/unchanged: %q/%q err=%v", released, releasedOperation, releasedErr)
	}
	borrowedOwnerDDLGuard(t, ctx, b)
	t.Logf("bound re-entry held-barrier cancellation control: the callback observed the caller cancellation after marker+fence and refused; the production refusal was sanitized, no durable marker/row mutation survived and the released row was lockable/unchanged")
}

// borrowedBoundReentrySourceLossControl proves the real copied-prefix source
// loss before dispatch permanently invalidates the attempt: registration and
// dispatch refuse at the attempt-wrapper source stages with nothing published,
// reconstruction returns a nil attempt, and a new session registration returns
// a nil registration attributable to the actual invalid-prefix stage. Raw Run
// source-invalidation is NOT claimed: the check lives in the attempt wrapper.
func borrowedBoundReentrySourceLossControl(t *testing.T, ctx context.Context) {
	t.Helper()
	b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
	f := b.fixture
	ownerPID := fresh.binding.ControlBackendPID()
	if ownerPID <= 0 {
		t.Fatalf("bound re-entry source-loss owner identity is missing: %d", ownerPID)
	}
	// Complete pre-loss durable baseline captured before the attempt is prepared
	// and the copied-prefix loss is inflicted: the full instance snapshot, the
	// evidence/audit row digest, the replacement catalog identity and the real
	// guard row, so an independently committed write during either refusal
	// cannot escape the post comparison.
	snap0 := borrowedBoundSessionSnapshotNow(t, ctx, b)
	rows0 := borrowedBoundNativeReadyRowsNow(t, ctx, b)
	catalogOID, oidErr := borrowedOwnerDDLTargetOID(ctx, b)
	if oidErr != nil || catalogOID != fresh.replacementOID {
		t.Fatalf("bound re-entry source-loss replacement catalog identity before the loss: oid=%d err=%v", catalogOID, oidErr)
	}
	beforeDisposition, beforeOperation := borrowedReplacementGuardRowRead(t, ctx, f.controlPool, f.guardKey)
	var probeCalls, acceptanceCalls int32
	attemptID := fmt.Sprintf("borrowed-replacement-bound-reentry-loss-%d", time.Now().UnixNano())
	marker := newBorrowedBoundNativeReadyMarker(attemptID, f.adminRole)
	prepared := prepareBorrowedBoundReentryAttempt(t, ctx, b, fresh, attemptID, marker.hook, &probeCalls, &acceptanceCalls)
	attemptCopy := *prepared
	runCopy := *prepared.run
	sourcePrefixCopy := *fresh.prefix
	borrowedReplacementSessionCopiedPrefixLoss(t, fresh)
	if invalid, reason := fresh.prefix.Invalid(); !invalid || reason == "" {
		t.Fatal("bound re-entry source loss did not retain the shared prefix loss")
	}
	if invalid, _ := sourcePrefixCopy.Invalid(); !invalid {
		t.Fatal("bound re-entry source prefix copy did not share the permanent loss")
	}
	if attemptCopy.run != prepared.run {
		t.Fatal("bound re-entry source-loss attempt copy did not retain the authentic run handle")
	}
	if err := prepared.registerOrigin(ctx); err == nil || !strings.Contains(err.Error(), "permanently invalidated") {
		t.Fatalf("source-lost bound re-entry registration was not refused at the wrapper source stage: %v", err)
	}
	runCtx, cancelRun := context.WithTimeout(ctx, 30*time.Second)
	dispatchResult, dispatchReceipt, dispatchErr := prepared.dispatchRun(runCtx)
	cancelRun()
	if dispatchErr == nil || !strings.Contains(dispatchErr.Error(), "permanently invalidated") {
		t.Fatalf("source-lost bound re-entry dispatch was not refused at the wrapper source stage: %v", dispatchErr)
	}
	borrowedBoundNativeReadyRequireNothingPublished(t, "bound re-entry source-lost dispatch", dispatchResult, dispatchReceipt)
	borrowedBoundReentryAssertOutcomeBoundaries(t, "bound re-entry source loss", prepared, dispatchResult, dispatchReceipt, &probeCalls, &acceptanceCalls)
	if err := attemptCopy.registerOrigin(ctx); err == nil {
		t.Fatal("source-lost bound re-entry attempt copy rehabilitated the registration")
	}
	copyResult, copyReceipt, copyErr := attemptCopy.dispatchRun(ctx)
	if copyErr == nil {
		t.Fatal("source-lost bound re-entry attempt copy dispatched the coordinator")
	}
	borrowedBoundNativeReadyRequireNothingPublished(t, "bound re-entry source-lost attempt copy", copyResult, copyReceipt)
	if runCopy.Binding().OriginalInstanceID() != f.instanceID {
		t.Fatal("bound re-entry source-loss run copy lost the authentic instance identity")
	}
	if reconAttempt, reconErr := newBorrowedReplacementPrelaunchAttemptWith(t, ctx, b, fresh, nil,
		fmt.Sprintf("borrowed-replacement-bound-reentry-recon-%d", time.Now().UnixNano()), &probeCalls, &acceptanceCalls,
		&borrowedReplacementPrelaunchBoundOptions{InstanceID: f.instanceID, Prelaunch: marker.hook}); reconErr == nil || reconAttempt != nil {
		t.Fatalf("post-loss bound re-entry reconstruction minted an attempt: attempt=%v err=%v", reconAttempt, reconErr)
	} else if !strings.Contains(reconErr.Error(), "permanently invalidated") {
		t.Fatalf("post-loss bound re-entry reconstruction refusal is not the invalid-prefix stage: %v", reconErr)
	}
	conn := borrowedReplacementSessionConnectP1(t, ctx, b)
	reg, regErr := newBorrowedReplacementSessionRegistration(t, ctx, b, fresh, conn)
	if reg != nil || regErr == nil {
		t.Fatalf("shared-prefix loss minted a session registration (reg=%v err=%v)", reg, regErr)
	}
	if !strings.Contains(regErr.Error(), "permanently invalidated") {
		t.Fatalf("session registration refusal is not the actual invalid-prefix/revalidation stage: %v", regErr)
	}
	borrowedSuccessorCloseConn(t, conn)
	if got := marker.callsNow(); got != 0 {
		t.Fatalf("source-lost bound re-entry ran the marker %d times, want 0", got)
	}
	borrowedBoundSessionAssertSnapshotEqual(t, "bound re-entry source loss", snap0, borrowedBoundSessionSnapshotNow(t, ctx, b))
	if after := borrowedBoundNativeReadyRowsNow(t, ctx, b); after != rows0 {
		t.Fatalf("source-lost bound re-entry left durable evidence/audit rows: before=%+v after=%+v", rows0, after)
	}
	if afterOID, oidErr := borrowedOwnerDDLTargetOID(ctx, b); oidErr != nil || afterOID != catalogOID {
		t.Fatalf("source-lost bound re-entry changed the replacement catalog: oid=%d err=%v", afterOID, oidErr)
	}
	if disposition, operation := borrowedReplacementGuardRowRead(t, ctx, f.controlPool, f.guardKey); disposition != beforeDisposition || operation != beforeOperation {
		t.Fatalf("source-lost bound re-entry changed the guard row: %q/%q -> %q/%q", beforeDisposition, beforeOperation, disposition, operation)
	}
	borrowedOwnerDDLGuard(t, ctx, b)
	t.Logf("bound re-entry pre-dispatch source-loss control: the real copied-prefix loss refused wrapper registration/dispatch at the source stages with nothing published, reconstruction and a new session registration refused with nil values at the actual invalid-prefix stage, no raw-Run invalidation is claimed, and the complete pre-loss snapshot/rows/catalog/guard row were unchanged")
}

// TestBorrowedReplacementBoundReentryExclusion is the bounded controlled
// re-entry/exclusion prerequisite lane described in the file header.
func TestBorrowedReplacementBoundReentryExclusion(t *testing.T) {
	ctx := t.Context()

	// P: the single chain over ONE authentic bound fixture.
	func() {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		f := b.fixture
		ownerPID := fresh.binding.ControlBackendPID()
		if ownerPID <= 0 {
			t.Fatalf("bound re-entry original owner identity is missing: %d", ownerPID)
		}
		originalKey := borrowedBoundReentryOriginalKey(t, f)

		// Bound retained-session prerequisite: exact P1 registered, used once
		// (identity queries counted separately), strict retirement.
		conn, reg := borrowedReplacementSessionRegisterP1(t, ctx, b, fresh)
		borrowedBoundSessionAssertRegistrationRetained(t, ctx, b, fresh, conn, reg, "bound re-entry prerequisite")
		useCtx, cancelUse := context.WithTimeout(ctx, 60*time.Second)
		useErr := reg.Use(useCtx)
		cancelUse()
		if useErr != nil {
			t.Fatalf("bound re-entry prerequisite session use refused: %v", useErr)
		}
		if got := atomic.LoadInt32(&reg.state.probeExecutions); got != 1 {
			t.Fatalf("bound re-entry prerequisite session probe executions=%d, want exactly 1", got)
		}
		if got := atomic.LoadInt32(&reg.state.identityQueries); got == 0 {
			t.Fatal("bound re-entry prerequisite identity-discovery queries were not counted separately")
		}
		borrowedReplacementSessionRetire(t, ctx, b, conn, reg.state)
		if _, closedErr := borrowedSuccessorReadConnIdentity(ctx, conn); closedErr == nil {
			t.Fatal("retired bound re-entry prerequisite connection still reads an identity")
		}

		// Complete snapshot before the fresh attempt.
		snap0 := borrowedBoundSessionSnapshotNow(t, ctx, b)
		rows0 := borrowedBoundNativeReadyRowsNow(t, ctx, b)
		catalogOID, oidErr := borrowedOwnerDDLTargetOID(ctx, b)
		if oidErr != nil || catalogOID != fresh.replacementOID {
			t.Fatalf("bound re-entry replacement catalog identity before the attempt: oid=%d err=%v", catalogOID, oidErr)
		}
		beforeDisposition, beforeOperation := borrowedReplacementGuardRowRead(t, ctx, f.controlPool, f.guardKey)
		if beforeDisposition == "clean" || beforeOperation != f.operation {
			t.Fatalf("bound re-entry fixture guard row is not the deliberately unresolved original: disposition=%q operation=%q", beforeDisposition, beforeOperation)
		}

		// Genuine-row controls on the same fixture (non-destructive).
		borrowedBoundReentryDifferentKeyControl(t, ctx, b, fresh)
		borrowedBoundReentryAbsentRowFence(t, ctx, b)
		attemptID := fmt.Sprintf("borrowed-replacement-bound-reentry-%d", time.Now().UnixNano())
		borrowedBoundReentryProvenanceMismatch(t, ctx, b, attemptID)
		borrowedBoundSessionAssertSnapshotEqual(t, "bound re-entry pre-attempt controls", snap0, borrowedBoundSessionSnapshotNow(t, ctx, b))

		// Fresh bound re-entry attempt with the lane-local barrier callback.
		var probeCalls, acceptanceCalls int32
		barrier := newBorrowedBoundReentryBarrier(t, attemptID, f.adminRole, f.guardKey, f.operation, ownerPID)
		attempt := prepareBorrowedBoundReentryAttempt(t, ctx, b, fresh, attemptID, barrier.hook, &probeCalls, &acceptanceCalls)
		if err := attempt.registerOrigin(ctx); err != nil {
			t.Fatalf("bound re-entry origin registration refused: %v", err)
		}
		attemptCopy := *attempt
		runCopy := *attempt.run

		runCtx, cancelRun := context.WithTimeout(ctx, 120*time.Second)
		done := make(chan borrowedBoundReentryOutcome, 1)
		go func() {
			result, receipt, err := attempt.dispatchRun(runCtx)
			done <- borrowedBoundReentryOutcome{result: result, receipt: receipt, err: err}
		}()
		var out borrowedBoundReentryOutcome
		joined := false
		unwind := func() {
			cancelRun()
			barrier.releaseBarrier()
			if joined {
				return
			}
			select {
			case out = <-done:
				joined = true
			case <-time.After(30 * time.Second):
				t.Errorf("bound re-entry dispatch did not complete within the bounded unwind; outcome unknown")
			}
		}
		defer unwind()
		t.Cleanup(unwind)

		select {
		case <-barrier.entered:
		case <-time.After(60 * time.Second):
			t.Errorf("bound re-entry barrier never signalled marker+fence success; completion unknown")
			return
		}
		txPID := barrier.txPIDNow()
		if txPID <= 0 || txPID == ownerPID {
			t.Fatalf("Prelaunch transaction is not the coordinator preparation transaction: txPID=%d ownerPID=%d", txPID, ownerPID)
		}

		// Probe 1: same-row contender through a separate transaction gets empty
		// fields and the structured 55P03 NOWAIT refusal.
		contDisposition, contOperation, contErr := borrowedReplacementGuardRowContenderLock(ctx, f.controlPool, f.guardKey)
		if contErr == nil || contDisposition != "" || contOperation != "" || borrowedReplacementGuardRowCode(contErr) != "55P03" {
			t.Fatalf("held-row contender was not refused with empty fields and 55P03: fields=%q/%q code=%s err=%v", contDisposition, contOperation, borrowedReplacementGuardRowCode(contErr), contErr)
		}
		// Probe 2: distinct control session advisory try-lock on the original key
		// returns FALSE with the owner independently confirmed holding it.
		borrowedBoundReentryAdvisoryProbe(t, ctx, f, originalKey, ownerPID)
		// Probe 3: bounded AcquireTargetLock on the same key returns a NIL lock at
		// the bounded acquisition stage, never a connect/config failure.
		lock, acqErr := recovery.AcquireTargetLock(ctx, f.controlDSN, originalKey, 3*time.Second, 10*time.Millisecond)
		if acqErr == nil || lock != nil {
			if lock != nil {
				_ = lock.Release(ctx)
			}
			t.Fatalf("bounded acquisition of the held original key was not refused with a nil lock: lock=%v err=%v", lock, acqErr)
		}
		if !strings.Contains(acqErr.Error(), "bounded target advisory lock acquisition") {
			t.Fatalf("original-key acquisition refusal is not the bounded acquisition stage: %v", acqErr)
		}
		if strings.Contains(acqErr.Error(), "connect dedicated") || strings.Contains(acqErr.Error(), "identity") {
			t.Fatalf("original-key acquisition refusal is a connect/config failure: %v", acqErr)
		}

		// Release the barrier without granting authority and join.
		barrier.releaseBarrier()
		select {
		case out = <-done:
			joined = true
		case <-time.After(30 * time.Second):
			t.Errorf("bound re-entry dispatch did not complete within the bounded join; outcome unknown")
			return
		}
		if out.err == nil || out.err.Error() != "bound target guard is not clean" {
			t.Fatalf("bound re-entry refusal is not the exact production bound-guard refusal: %v", out.err)
		}
		if got := barrier.marker.callsNow(); got != 1 {
			t.Fatalf("bound re-entry marker calls=%d, want exactly 1", got)
		}
		token := barrier.marker.token()
		if token.InstanceID != f.instanceID || token.State != "open" || token.Generation <= 0 || token.Hash == "" {
			t.Fatalf("bound re-entry marker did not return the genuine transaction-local token: %+v", token)
		}
		if !barrier.fencedNow() {
			t.Fatal("bound re-entry barrier did not fence the real row")
		}
		fenceDisposition, fenceOperation, fenceErr := barrier.fenceResultNow()
		if fenceErr != nil || fenceDisposition != beforeDisposition || fenceOperation != beforeOperation {
			t.Fatalf("bound re-entry fence result is not the unchanged original row: %q/%q err=%v", fenceDisposition, fenceOperation, fenceErr)
		}
		if out.result.MarkerToken != token {
			t.Fatalf("bound re-entry production result did not carry the genuine transaction-local marker: result=%+v marker=%+v", out.result.MarkerToken, token)
		}
		if out.result.TargetKey.String() != f.guardKey || out.result.Application == "" {
			t.Fatalf("bound re-entry refusal lost the legitimate target/application metadata: key=%q application=%q", out.result.TargetKey.String(), out.result.Application)
		}
		if out.result.Command != (recovery.PGCommandResult{}) {
			t.Fatalf("bound re-entry refusal published a command outcome: %+v", out.result.Command)
		}
		borrowedBoundReentryAssertOutcomeBoundaries(t, "bound re-entry refusal", attempt, out.result, out.receipt, &probeCalls, &acceptanceCalls)

		// After: fresh contender locks/reads the unchanged row and rolls back.
		postDisposition, postOperation, postErr := borrowedReplacementGuardRowContenderLock(ctx, f.controlPool, f.guardKey)
		if postErr != nil || postDisposition != beforeDisposition || postOperation != beforeOperation {
			t.Fatalf("released row is not lockable/unchanged after the refusal: %q/%q err=%v", postDisposition, postOperation, postErr)
		}
		// Original advisory owner still held + healthy, anchor rechecks.
		borrowedBoundReentryAdvisoryProbe(t, ctx, f, originalKey, ownerPID)
		healthCtx, cancelHealth := context.WithTimeout(ctx, borrowedOwnerRotationHealthBudget)
		healthErr := f.lock.Health(healthCtx)
		cancelHealth()
		if healthErr != nil {
			t.Fatalf("bound re-entry refusal original control owner health: %v", healthErr)
		}
		anchorCtx, cancelAnchor := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
		anchorErr := b.anchor.Recheck(anchorCtx)
		cancelAnchor()
		if anchorErr != nil {
			t.Fatalf("bound re-entry refusal original anchor recheck: %v", anchorErr)
		}
		// Durable state unchanged; guard unresolved; inventory NULL.
		borrowedBoundSessionAssertSnapshotEqual(t, "bound re-entry refusal", snap0, borrowedBoundSessionSnapshotNow(t, ctx, b))
		if after := borrowedBoundNativeReadyRowsNow(t, ctx, b); after != rows0 {
			t.Fatalf("bound re-entry refusal left durable rows: before=%+v after=%+v", rows0, after)
		}
		if afterOID, oidErr := borrowedOwnerDDLTargetOID(ctx, b); oidErr != nil || afterOID != catalogOID {
			t.Fatalf("bound re-entry refusal changed the replacement catalog: oid=%d err=%v", afterOID, oidErr)
		}
		if disposition := borrowedReplacementPrelaunchGuardDisposition(t, ctx, f); disposition == "clean" {
			t.Fatal("bound re-entry refusal resolved the deliberately unresolved guard")
		}
		t.Logf("bound re-entry refusal: the held barrier proven by a 55P03 same-row contender, a FALSE advisory try-lock with the owner independently held, and a nil bounded acquisition; the actual coordinator then refused with %q; marker once with token %s generation %d, no child/command/receipt/probe/acceptance, durable snapshots/rows/catalog unchanged, owner/anchor healthy, guard unresolved, inventory NULL", out.err.Error(), token.InstanceID, token.Generation)

		// Replay/copy refusals.
		borrowedBoundReentryReplayRefuse(t, ctx, b, attempt, &attemptCopy, &runCopy, barrier.marker, &probeCalls, &acceptanceCalls, snap0, rows0)
		borrowedOwnerDDLGuard(t, ctx, b)
	}()

	// Destructive controls on independent authentic bound fixtures.
	borrowedBoundReentryRowHolderFirstControl(t, ctx)
	borrowedBoundReentryCancellationControl(t, ctx)
	borrowedBoundReentrySourceLossControl(t, ctx)
}
