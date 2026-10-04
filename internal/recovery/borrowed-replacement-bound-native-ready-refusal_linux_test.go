//go:build linux && drill

// borrowed-replacement-bound-native-ready-refusal_linux_test.go is the bounded
// instance-bound native-ready replacement refusal lane. It proves, over the
// approved bound fixture and the EXISTING bounded replacement orchestration
// (all five stages, genuine replacement OID, unchanged comparator accepting
// the source capture): one authentic bound retained-session prerequisite
// (register the exact genuine P1, use once, retire via the existing strict
// disappearance), a complete pre-preparation snapshot (guard-row hash,
// instance identity/generation/hash, NULL inventory, relevant evidence/audit
// row digest), a native-ready attempt prepared through the existing
// newBorrowedReplacementPrelaunchAttemptWith path with the SAME authentic
// InstanceID, same retained owner/control store, exact private P1 target DSN,
// genuine observer, new gate, fresh prefix captured against that run, an
// independently opened descriptor-verified archive at offset zero, a unique
// operation ID, a counted transactional restore_started marker wrapper and
// rejecting probe/acceptance callbacks, origin registration against that run,
// and EXACTLY ONE actual coordinator dispatch refused by the exact production
// refusal `bound target guard is not clean` after the genuine transaction-local
// marker token was produced and rolled back: marker callback exactly once
// returning a genuine token, no child, absent receipt, zero replacement
// probe/acceptance callbacks, unchanged replacement catalog, unchanged
// complete guard/instance/evidence/audit snapshot, no durable marker and the
// original owner/anchor still healthy with the guard unresolved and the
// inventory NULL. Negatives start through the bound constructor and the actual
// primitives: replay/copy, concurrent reservation, preparation cancellation,
// mid-preparation source loss, prepared-before-loss, wrong observer, canceled
// factory, unavailable archive, archive boundary, partial/UNKNOWN DDL and
// contaminated replacement. No second instance is opened per fixture, no
// CLOSED helper is modified, no rebuild witness is invoked, no fabricated
// token/evidence is used, and this lane grants no admission, guard-clean,
// successful restore-probe, atomic acceptance, manifest, downstream or Gate1
// authority. Secrets, verifiers, DSNs and archive content are never logged.
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
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

// newBorrowedBoundNativeReadyCapture creates ONE authentic bound baseline and
// its genuine replacement capture through the EXISTING bound-session capture
// helper, then asserts the unchanged comparator accepts the source capture and
// that the authentic InstanceID provenance is intact.
func newBorrowedBoundNativeReadyCapture(t *testing.T, ctx context.Context) (*borrowedSuccessorBaseline, *borrowedReplacementBinding) {
	t.Helper()
	b, fresh := newBorrowedBoundSessionCapture(t, ctx)
	f := b.fixture
	if err := borrowedReplacementCompareBindings(f.run.Binding(), fresh.binding, fresh.replacementOID); err != nil {
		t.Fatalf("bound native-ready source capture was not accepted by the unchanged comparator: %v", err)
	}
	borrowedBoundSessionAssertProvenance(t, ctx, b, fresh, "bound native-ready")
	return b, fresh
}

// borrowedBoundNativeReadyRows is the complete comparable digest of the
// instance's relevant evidence and audit rows (count + ordered row digest), so
// a rolled-back marker can never leave a durable row behind unnoticed.
type borrowedBoundNativeReadyRows struct {
	evidenceCount int64
	evidenceHash  string
	auditCount    int64
	auditHash     string
}

func borrowedBoundNativeReadyRowsNow(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline) borrowedBoundNativeReadyRows {
	t.Helper()
	var rows borrowedBoundNativeReadyRows
	readCtx, cancelRead := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	defer cancelRead()
	if err := b.fixture.controlPool.QueryRow(readCtx, `
SELECT count(*), COALESCE(md5(string_agg(row_to_json(e)::text, '|' ORDER BY e.evidence_id)), '')
FROM recovery_evidence e WHERE e.instance_id=$1`, b.fixture.instanceID).Scan(&rows.evidenceCount, &rows.evidenceHash); err != nil {
		t.Fatalf("bound native-ready evidence row digest refused: %v", err)
	}
	if err := b.fixture.controlPool.QueryRow(readCtx, `
SELECT count(*), COALESCE(md5(string_agg(row_to_json(a)::text, '|' ORDER BY a.audit_id)), '')
FROM recovery_audit a WHERE a.instance_id=$1`, b.fixture.instanceID).Scan(&rows.auditCount, &rows.auditHash); err != nil {
		t.Fatalf("bound native-ready audit row digest refused: %v", err)
	}
	return rows
}

// borrowedBoundNativeReadyAttemptProof projects THIS attempt's completed R5
// retained proof row (count + full-row digest). Scoped to the exact operation
// so a sibling attempt's proof can never satisfy this lane.
func borrowedBoundNativeReadyAttemptProof(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, operationID string) (int64, string) {
	t.Helper()
	readCtx, cancelRead := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	defer cancelRead()
	var count int64
	var hash string
	if err := b.fixture.controlPool.QueryRow(readCtx, `
SELECT count(*), COALESCE(md5(string_agg(row_to_json(a)::text, '|' ORDER BY a.audit_id)), '')
FROM recovery_audit a WHERE a.instance_id=$1 AND a.action='attempt_proof' AND a.result='ok'
  AND a.operation_id=$2`,
		b.fixture.instanceID, operationID).Scan(&count, &hash); err != nil {
		t.Fatalf("bound native-ready attempt_proof row digest refused: %v", err)
	}
	return count, hash
}

// borrowedBoundNativeReadyMarker is the counted wrapper around the genuine
// fixture transactional restore_started marker: it counts every callback and
// retains the actual protocol token returned by a successful callback.
type borrowedBoundNativeReadyMarker struct {
	calls     int32
	lastToken recovery.EvidenceToken
	inner     func(context.Context, pgx.Tx, controlstore.InstanceToken) (recovery.EvidenceToken, error)
}

func newBorrowedBoundNativeReadyMarker(operation, actorRole string) *borrowedBoundNativeReadyMarker {
	return &borrowedBoundNativeReadyMarker{inner: borrowedAuthFixtureRestoreStartedMarker(operation, actorRole)}
}

func (m *borrowedBoundNativeReadyMarker) hook(hookCtx context.Context, tx pgx.Tx, locked controlstore.InstanceToken) (recovery.EvidenceToken, error) {
	atomic.AddInt32(&m.calls, 1)
	token, err := m.inner(hookCtx, tx, locked)
	if err == nil {
		m.lastToken = token
	}
	return token, err
}

func (m *borrowedBoundNativeReadyMarker) callsNow() int32 { return atomic.LoadInt32(&m.calls) }

func (m *borrowedBoundNativeReadyMarker) token() recovery.EvidenceToken { return m.lastToken }

// prepareBorrowedBoundNativeReadyAttempt prepares the native-ready attempt
// through the EXISTING common attempt constructor with the REAL bound options
// and asserts the preserved lineage fields plus the deliberately NEW operation
// identity (the unchanged comparator is NOT asked to accept that different
// operation; only the source capture is compared above).
func prepareBorrowedBoundNativeReadyAttempt(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding, attemptID string, marker *borrowedBoundNativeReadyMarker, probeCalls, acceptanceCalls *int32) *borrowedReplacementPrelaunchAttempt {
	t.Helper()
	f := b.fixture
	attempt, err := newBorrowedReplacementPrelaunchAttemptWith(t, ctx, b, fresh, borrowedReplacementPrelaunchArchive(t, f), attemptID,
		probeCalls, acceptanceCalls, &borrowedReplacementPrelaunchBoundOptions{InstanceID: f.instanceID, Prelaunch: marker.hook})
	if err != nil || attempt == nil {
		t.Fatalf("bound native-ready attempt preparation refused: attempt=%v err=%v", attempt, err)
	}
	if attempt.source != fresh || attempt.gate == nil || attempt.prefix == nil || attempt.prefix.run != attempt.run {
		t.Fatal("bound native-ready attempt did not retain the genuine source capture, gate and fresh prefix")
	}
	runBinding := attempt.run.Binding()
	if runBinding.OriginalInstanceID() != f.instanceID ||
		runBinding.OriginalTargetKey() != fresh.binding.OriginalTargetKey() ||
		runBinding.OriginalRoleFingerprint() != fresh.binding.OriginalRoleFingerprint() {
		t.Fatalf("bound native-ready attempt lost the preserved lineage: %+v", runBinding)
	}
	if runBinding.OriginalOperationID() != attemptID || runBinding.OriginalOperationID() == fresh.binding.OriginalOperationID() {
		t.Fatalf("bound native-ready attempt did not carry its NEW operation identity: attempt=%q source=%q", runBinding.OriginalOperationID(), fresh.binding.OriginalOperationID())
	}
	return attempt
}

// dispatchBorrowedBoundNativeReady dispatches the one actual coordinator run on
// an explicit bounded caller child.
func dispatchBorrowedBoundNativeReady(t *testing.T, ctx context.Context, attempt *borrowedReplacementPrelaunchAttempt) (recovery.TargetWriterResult, recovery.DrillTargetProcessReceipt, error) {
	t.Helper()
	runCtx, cancelRun := context.WithTimeout(ctx, 60*time.Second)
	defer cancelRun()
	return attempt.dispatchRun(runCtx)
}

// borrowedBoundNativeReadyRequireNoProbeOutput requires a result to publish no
// probe output at all: an empty probe outcome, an empty probe application name
// and nil probe evidence, so a refusal carrying accepted probe evidence can
// never pass beside its expected error, receipt and callback counts.
func borrowedBoundNativeReadyRequireNoProbeOutput(t *testing.T, label string, result recovery.TargetWriterResult) {
	t.Helper()
	if result.Probe.Outcome != "" || result.Probe.ApplicationName != "" || len(result.Probe.Evidence) != 0 {
		t.Fatalf("%s: result published probe output: outcome=%q application=%q evidence=%s", label, result.Probe.Outcome, result.Probe.ApplicationName, result.Probe.Evidence)
	}
}

// borrowedBoundNativeReadyRequireNothingPublished requires a refusal outcome to
// publish no result metadata at all (no marker token, target key, application,
// command outcome or probe output) AND the exact absent-receipt refusal, so a
// reservation or wrapper refusal publishing outputs beside its expected error
// can never pass.
func borrowedBoundNativeReadyRequireNothingPublished(t *testing.T, label string, result recovery.TargetWriterResult, receipt recovery.DrillTargetProcessReceipt) {
	t.Helper()
	if result.MarkerToken != (recovery.EvidenceToken{}) ||
		result.TargetKey != (recovery.TargetKey{}) ||
		result.Application != "" ||
		result.Command != (recovery.PGCommandResult{}) {
		t.Fatalf("%s: refusal published result metadata: marker=%+v key=%+v application=%q command=%+v", label, result.MarkerToken, result.TargetKey, result.Application, result.Command)
	}
	borrowedBoundNativeReadyRequireNoProbeOutput(t, label, result)
	borrowedBoundNativeReadyRequireAbsentReceipt(t, label, receipt)
}

// borrowedBoundNativeReadyRequireAbsentReceipt requires a captured refusal
// receipt to be the exact opaque zero value whose ConsumeFacts refusal is the
// exact absence error: a consumable empty-executable receipt or any other
// ConsumeFacts outcome can never pass beside the expected refusal.
func borrowedBoundNativeReadyRequireAbsentReceipt(t *testing.T, label string, receipt recovery.DrillTargetProcessReceipt) {
	t.Helper()
	if receipt != (recovery.DrillTargetProcessReceipt{}) {
		t.Fatalf("%s: refusal published a non-zero process receipt: %+v", label, receipt)
	}
	facts, factsErr := receipt.ConsumeFacts()
	if factsErr == nil || factsErr.Error() != "process receipt is absent" {
		t.Fatalf("%s: refusal receipt ConsumeFacts is not the exact absence refusal: facts=%+v err=%v", label, facts, factsErr)
	}
	if facts.Started || facts.Cmd != nil || facts.Process != nil || facts.PID != 0 {
		t.Fatalf("%s: absence refusal exposed executable fields: %+v", label, facts)
	}
}

// borrowedBoundNativeReadyReplayRefuse proves the pre-dispatch retained attempt
// and run copies cannot register, dispatch or replay after the one actual
// dispatch: each later call refuses at its real stage, returns no usable
// result/receipt and adds no callback, child or durable mutation.
func borrowedBoundNativeReadyReplayRefuse(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, attempt *borrowedReplacementPrelaunchAttempt, attemptCopy *borrowedReplacementPrelaunchAttempt, runCopy *recovery.DrillBorrowedWriterRun, marker *borrowedBoundNativeReadyMarker, probeCalls, acceptanceCalls *int32, snap borrowedBoundSessionSnapshot, rows borrowedBoundNativeReadyRows) {
	t.Helper()
	if err := attemptCopy.registerOrigin(ctx); err == nil || !strings.Contains(err.Error(), "already frozen") {
		t.Fatalf("later bound native-ready registration was not refused at the real gate stage: %v", err)
	}
	dispatchResult, dispatchReceipt, dispatchErr := dispatchBorrowedBoundNativeReady(t, ctx, attemptCopy)
	if dispatchErr == nil || !strings.Contains(dispatchErr.Error(), "already reserved") {
		t.Fatalf("later bound native-ready dispatch was not refused as already reserved: %v", dispatchErr)
	}
	borrowedBoundNativeReadyRequireNothingPublished(t, "later bound native-ready dispatch", dispatchResult, dispatchReceipt)
	runCopyCtx, cancelRunCopy := context.WithTimeout(ctx, 30*time.Second)
	copyResult, copyReceipt, copyErr := runCopy.Run(runCopyCtx)
	cancelRunCopy()
	if copyErr == nil || !strings.Contains(copyErr.Error(), "already reserved") {
		t.Fatalf("copied bound native-ready run replay was not refused as already reserved: %v", copyErr)
	}
	borrowedBoundNativeReadyRequireNothingPublished(t, "copied bound native-ready run replay", copyResult, copyReceipt)
	if identity := attempt.run.Observation().StartedIdentity(); identity.Started {
		t.Fatalf("bound native-ready replay refused a child: %+v", identity)
	}
	if got := marker.callsNow(); got != 1 {
		t.Fatalf("bound native-ready replay marker calls=%d, want exactly 1", got)
	}
	if got := atomic.LoadInt32(probeCalls); got != 0 {
		t.Fatalf("bound native-ready replay ran the replacement probe %d times, want 0", got)
	}
	if got := atomic.LoadInt32(acceptanceCalls); got != 0 {
		t.Fatalf("bound native-ready replay ran acceptance %d times, want 0", got)
	}
	borrowedBoundSessionAssertSnapshotEqual(t, "bound native-ready replay refusal", snap, borrowedBoundSessionSnapshotNow(t, ctx, b))
	if after := borrowedBoundNativeReadyRowsNow(t, ctx, b); after != rows {
		t.Fatalf("bound native-ready replay refusal left durable rows: before=%+v after=%+v", rows, after)
	}
	t.Logf("bound native-ready replay/copy control: the later registration was refused at the frozen gate, the later dispatch and the copied run were refused as already reserved with no result/receipt, and no callback, child or durable mutation was added")
}

// borrowedBoundNativeReadyPreparationCancelRefuse proves the mid-preparation
// publication seam can cancel the common preparation: nothing is published and
// no marker callback runs, with an immediate call-domain unwind.
func borrowedBoundNativeReadyPreparationCancelRefuse(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding, probeCalls, acceptanceCalls *int32) {
	t.Helper()
	archive := borrowedReplacementPrelaunchArchive(t, b.fixture)
	seam := installBorrowedReplacementPrelaunchPreparationSeam(t)
	prepCtx, cancelPrep := context.WithCancel(ctx)
	type prepOutcome struct {
		attempt *borrowedReplacementPrelaunchAttempt
		err     error
	}
	done := make(chan prepOutcome, 1)
	attemptID := fmt.Sprintf("borrowed-replacement-bound-native-ready-cancel-%d", time.Now().UnixNano())
	marker := newBorrowedBoundNativeReadyMarker(attemptID, b.fixture.adminRole)
	go func() {
		attempt, err := newBorrowedReplacementPrelaunchAttemptWith(t, prepCtx, b, fresh, archive, attemptID, probeCalls, acceptanceCalls,
			&borrowedReplacementPrelaunchBoundOptions{InstanceID: b.fixture.instanceID, Prelaunch: marker.hook})
		done <- prepOutcome{attempt: attempt, err: err}
	}()
	joined := false
	unwind := func() {
		cancelPrep()
		seam.releaseSeam()
		if joined {
			return
		}
		select {
		case <-done:
			joined = true
		case <-time.After(30 * time.Second):
			t.Errorf("canceled bound native-ready preparation join is unknown after the bounded unwind")
		}
	}
	defer unwind()
	t.Cleanup(unwind)
	select {
	case <-seam.entered:
	case <-time.After(60 * time.Second):
		t.Errorf("bound native-ready preparation never reached the mid-preparation seam; completion unknown")
		return
	}
	cancelPrep()
	seam.releaseSeam()
	select {
	case got := <-done:
		joined = true
		if got.err == nil || got.attempt != nil {
			t.Fatalf("canceled bound native-ready preparation published an attempt: attempt=%v err=%v", got.attempt, got.err)
		}
		if !strings.Contains(got.err.Error(), "caller context ended before publication") {
			t.Fatalf("canceled bound native-ready preparation refusal is not the caller-context stage: %v", got.err)
		}
	case <-time.After(30 * time.Second):
		t.Errorf("canceled bound native-ready preparation did not complete within the bounded join; outcome unknown")
		return
	}
	if got := marker.callsNow(); got != 0 {
		t.Fatalf("canceled bound native-ready preparation ran the marker %d times, want 0", got)
	}
	if got := atomic.LoadInt32(probeCalls); got != 0 {
		t.Fatalf("canceled bound native-ready preparation ran the replacement probe %d times, want 0", got)
	}
	if got := atomic.LoadInt32(acceptanceCalls); got != 0 {
		t.Fatalf("canceled bound native-ready preparation ran acceptance %d times, want 0", got)
	}
	t.Logf("bound native-ready preparation-cancellation control: the caller context was canceled at the real mid-preparation seam, no attempt was published and no callback ran")
}

// borrowedBoundNativeReadyPreparationSourceLossRefuse proves the ACTUAL
// copied-prefix loss injected mid-preparation refuses publication with the
// genuine-source-loss stage on an independent bound fixture.
func borrowedBoundNativeReadyPreparationSourceLossRefuse(t *testing.T, ctx context.Context) {
	t.Helper()
	b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
	var probeCalls, acceptanceCalls int32
	attemptID := fmt.Sprintf("borrowed-replacement-bound-native-ready-midloss-%d", time.Now().UnixNano())
	marker := newBorrowedBoundNativeReadyMarker(attemptID, b.fixture.adminRole)
	lossHook := func() { borrowedReplacementSessionCopiedPrefixLoss(t, fresh) }
	borrowedReplacementPrelaunchPreparationHook.Store(&lossHook)
	t.Cleanup(func() { borrowedReplacementPrelaunchPreparationHook.Store(nil) })
	attempt, prepErr := newBorrowedReplacementPrelaunchAttemptWith(t, ctx, b, fresh, borrowedReplacementPrelaunchArchive(t, b.fixture), attemptID,
		&probeCalls, &acceptanceCalls, &borrowedReplacementPrelaunchBoundOptions{InstanceID: b.fixture.instanceID, Prelaunch: marker.hook})
	borrowedReplacementPrelaunchPreparationHook.Store(nil)
	if prepErr == nil || attempt != nil {
		t.Fatalf("mid-preparation bound source loss published an attempt: attempt=%v err=%v", attempt, prepErr)
	}
	if !strings.Contains(prepErr.Error(), "genuine source capture was permanently invalidated") {
		t.Fatalf("mid-preparation bound source loss was not refused at the genuine-source-loss stage: %v", prepErr)
	}
	if got := marker.callsNow(); got != 0 {
		t.Fatalf("mid-preparation bound source loss ran the marker %d times, want 0", got)
	}
	if got := atomic.LoadInt32(&probeCalls); got != 0 {
		t.Fatalf("mid-preparation bound source loss ran the replacement probe %d times, want 0", got)
	}
	if got := atomic.LoadInt32(&acceptanceCalls); got != 0 {
		t.Fatalf("mid-preparation bound source loss ran acceptance %d times, want 0", got)
	}
	t.Logf("bound native-ready preparation source-loss control: the actual copied-prefix loss at the existing mid-preparation seam published no attempt and was refused at the genuine-source-loss stage")
}

// borrowedBoundNativeReadyPreparedBeforeLossRefuse proves a valid
// prepared-before-loss attempt is permanently invalidated by the actual
// copied-prefix loss: the pre-fault attempt copy refuses registration and
// dispatch at the attempt-wrapper source stage with ZERO published outputs,
// the retained authentic run copy keeps its identity (raw Run is NOT claimed
// source-invalidated: source/context checking lives in the attempt wrapper),
// the source prefix copy shares the permanent loss, reconstruction returns a
// nil attempt with the invalid-prefix refusal, and a new session registration
// returns a nil registration with an error.
func borrowedBoundNativeReadyPreparedBeforeLossRefuse(t *testing.T, ctx context.Context) {
	t.Helper()
	b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
	var probeCalls, acceptanceCalls int32
	attemptID := fmt.Sprintf("borrowed-replacement-bound-native-ready-prepared-%d", time.Now().UnixNano())
	marker := newBorrowedBoundNativeReadyMarker(attemptID, b.fixture.adminRole)
	prepared := prepareBorrowedBoundNativeReadyAttempt(t, ctx, b, fresh, attemptID, marker, &probeCalls, &acceptanceCalls)
	attemptCopy := *prepared
	runCopy := *prepared.run
	sourcePrefixCopy := *fresh.prefix
	borrowedReplacementSessionCopiedPrefixLoss(t, fresh)
	if invalid, reason := fresh.prefix.Invalid(); !invalid || reason == "" {
		t.Fatal("bound prepared-before-loss shared prefix did not retain the actual loss")
	}
	if invalid, _ := sourcePrefixCopy.Invalid(); !invalid {
		t.Fatal("bound prepared-before-loss source prefix copy did not share the permanent loss")
	}
	if err := prepared.registerOrigin(ctx); err == nil || !strings.Contains(err.Error(), "permanently invalidated") {
		t.Fatalf("prepared-before-loss attempt registered its origin after the source loss: %v", err)
	}
	dispatchResult, dispatchReceipt, dispatchErr := dispatchBorrowedBoundNativeReady(t, ctx, prepared)
	if dispatchErr == nil || !strings.Contains(dispatchErr.Error(), "permanently invalidated") {
		t.Fatalf("prepared-before-loss attempt dispatched the coordinator after the source loss: %v", dispatchErr)
	}
	borrowedBoundNativeReadyRequireNothingPublished(t, "prepared-before-loss dispatch", dispatchResult, dispatchReceipt)
	if prepared.state.invalidReasonNow() == "" {
		t.Fatal("bound source loss did not permanently invalidate the prepared attempt")
	}
	if attemptCopy.run != prepared.run {
		t.Fatal("prepared-before-loss attempt copy did not retain the authentic run handle")
	}
	if err := attemptCopy.registerOrigin(ctx); err == nil || !strings.Contains(err.Error(), "permanently invalidated") {
		t.Fatalf("prepared-before-loss attempt copy registration was not refused at the source stage: %v", err)
	}
	copyResult, copyReceipt, copyErr := dispatchBorrowedBoundNativeReady(t, ctx, &attemptCopy)
	if copyErr == nil || !strings.Contains(copyErr.Error(), "permanently invalidated") {
		t.Fatalf("prepared-before-loss attempt copy dispatch was not refused at the source stage: %v", copyErr)
	}
	borrowedBoundNativeReadyRequireNothingPublished(t, "prepared-before-loss attempt copy", copyResult, copyReceipt)
	if runCopy.Binding().OriginalInstanceID() != b.fixture.instanceID {
		t.Fatal("prepared-before-loss run copy lost the authentic instance identity")
	}
	if identity := prepared.run.Observation().StartedIdentity(); identity.Started {
		t.Fatalf("prepared-before-loss attempt started a child: %+v", identity)
	}
	if reconAttempt, reconErr := newBorrowedReplacementPrelaunchAttemptWith(t, ctx, b, fresh, nil,
		fmt.Sprintf("borrowed-replacement-bound-native-ready-recon-%d", time.Now().UnixNano()), &probeCalls, &acceptanceCalls,
		&borrowedReplacementPrelaunchBoundOptions{InstanceID: b.fixture.instanceID, Prelaunch: marker.hook}); reconErr == nil || reconAttempt != nil {
		t.Fatalf("post-loss reconstruction minted a bound attempt: attempt=%v err=%v", reconAttempt, reconErr)
	} else if !strings.Contains(reconErr.Error(), "permanently invalidated") {
		t.Fatalf("post-loss reconstruction refusal is not the invalid-prefix stage: %v", reconErr)
	}
	conn := borrowedReplacementSessionConnectP1(t, ctx, b)
	reg, regErr := newBorrowedReplacementSessionRegistration(t, ctx, b, fresh, conn)
	if reg != nil || regErr == nil {
		t.Fatalf("shared-prefix loss minted a session registration (reg=%v err=%v)", reg, regErr)
	}
	borrowedSuccessorCloseConn(t, conn)
	if got := marker.callsNow(); got != 0 {
		t.Fatalf("prepared-before-loss control ran the marker %d times, want 0", got)
	}
	if got := atomic.LoadInt32(&probeCalls); got != 0 {
		t.Fatalf("prepared-before-loss control ran the replacement probe %d times, want 0", got)
	}
	if got := atomic.LoadInt32(&acceptanceCalls); got != 0 {
		t.Fatalf("prepared-before-loss control ran acceptance %d times, want 0", got)
	}
	borrowedOwnerDDLGuard(t, ctx, b)
	t.Logf("bound native-ready prepared-before-loss control: the actual copied-prefix loss permanently invalidated the attempt and its pre-fault attempt/source-prefix copies through the attempt wrapper (registration/dispatch refused with zero results and absent receipts), the retained authentic run copy kept its identity without any raw-Run invalidation claim, the reconstruction and a new session registration refused with nil values, and no callback or child ran")
}

// borrowedBoundNativeReadyWrongObserverRefuse proves the real bound factory
// refuses a real wrong-database observer before publishing any run.
func borrowedBoundNativeReadyWrongObserverRefuse(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, probeCalls, acceptanceCalls *int32) {
	t.Helper()
	f := b.fixture
	p1TargetDSN := borrowedReplacementP1TargetDSN(t, b)
	targetTarget, err := controlstore.ParseDSNTarget(p1TargetDSN)
	if err != nil {
		t.Fatalf("bound native-ready wrong-observer trusted target refused: %v", err)
	}
	gate := newOriginGate(t, ctx, f.fx)
	storeCtx, cancelStore := context.WithTimeout(ctx, borrowedReplacementPrelaunchStoreBudget)
	store, err := controlstore.NewStore(storeCtx, f.controlPool)
	cancelStore()
	if err != nil {
		t.Fatalf("bound native-ready wrong-observer control store refused: %v", err)
	}
	attemptID := fmt.Sprintf("borrowed-replacement-bound-native-ready-wrong-%d", time.Now().UnixNano())
	marker := newBorrowedBoundNativeReadyMarker(attemptID, f.adminRole)
	wrongObserver := borrowedAuthRoleDSN(t, f.writerSourceDSN, f.observerRole, f.observerPass)
	factoryCtx, cancelFactory := context.WithTimeout(ctx, borrowedReplacementPrelaunchFactoryBudget)
	run, prepErr := recovery.DrillNewBorrowedWriterRun(factoryCtx, recovery.TargetWriterOptions{
		OperationKind: recovery.TargetWriterOperationRestore,
		Store:         store,
		ControlDSN:    f.controlDSN,
		TargetDSN:     p1TargetDSN,
		ObserverDSN:   wrongObserver,
		TrustedTarget: targetTarget,
		OperationID:   attemptID,
		Archive:       borrowedReplacementPrelaunchArchive(t, f),
		InstanceID:    f.instanceID,
		Prelaunch:     marker.hook,
		Probe: func(context.Context, recovery.TargetWriterProof) (recovery.TargetWriterProbeResult, error) {
			atomic.AddInt32(probeCalls, 1)
			return recovery.TargetWriterProbeResult{}, errors.New("bound native-ready wrong-observer probe must never run")
		},
		Acceptance: func(context.Context, pgx.Tx, recovery.TargetWriterProof) (recovery.TargetWriterAcceptance, error) {
			atomic.AddInt32(acceptanceCalls, 1)
			return recovery.TargetWriterAcceptance{}, errors.New("bound native-ready wrong-observer acceptance must never run")
		},
	}, f.lock, gate.endpoint, f.tools)
	cancelFactory()
	if prepErr == nil || run != nil {
		t.Fatalf("bound native-ready wrong observer published a run: run=%v err=%v", run, prepErr)
	}
	if !strings.Contains(prepErr.Error(), "observer identity does not match the original trusted target") {
		t.Fatalf("bound native-ready wrong observer was not refused at the intended identity stage: %v", prepErr)
	}
	if got := marker.callsNow(); got != 0 {
		t.Fatalf("bound native-ready wrong observer ran the marker %d times, want 0", got)
	}
	t.Logf("bound native-ready wrong-observer control: a real wrong-database observer was refused pre-launch by the actual bound factory with the intended identity refusal")
}

// borrowedBoundNativeReadyCanceledFactoryRefuse proves an already-canceled
// caller refuses the real bound factory at the cancellation stage (never a
// malformed-option refusal) and publishes no run.
func borrowedBoundNativeReadyCanceledFactoryRefuse(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, probeCalls, acceptanceCalls *int32) {
	t.Helper()
	f := b.fixture
	p1TargetDSN := borrowedReplacementP1TargetDSN(t, b)
	targetTarget, err := controlstore.ParseDSNTarget(p1TargetDSN)
	if err != nil {
		t.Fatalf("bound native-ready canceled trusted target refused: %v", err)
	}
	gate := newOriginGate(t, ctx, f.fx)
	storeCtx, cancelStore := context.WithTimeout(ctx, borrowedReplacementPrelaunchStoreBudget)
	store, err := controlstore.NewStore(storeCtx, f.controlPool)
	cancelStore()
	if err != nil {
		t.Fatalf("bound native-ready canceled control store refused: %v", err)
	}
	attemptID := fmt.Sprintf("borrowed-replacement-bound-native-ready-canceled-%d", time.Now().UnixNano())
	marker := newBorrowedBoundNativeReadyMarker(attemptID, f.adminRole)
	cancelCtx, cancelPrep := context.WithCancel(ctx)
	cancelPrep()
	run, prepErr := recovery.DrillNewBorrowedWriterRun(cancelCtx, recovery.TargetWriterOptions{
		OperationKind: recovery.TargetWriterOperationRestore,
		Store:         store,
		ControlDSN:    f.controlDSN,
		TargetDSN:     p1TargetDSN,
		ObserverDSN:   f.observerTargetDSN,
		TrustedTarget: targetTarget,
		OperationID:   attemptID,
		Archive:       borrowedReplacementPrelaunchArchive(t, f),
		InstanceID:    f.instanceID,
		Prelaunch:     marker.hook,
		Probe: func(context.Context, recovery.TargetWriterProof) (recovery.TargetWriterProbeResult, error) {
			atomic.AddInt32(probeCalls, 1)
			return recovery.TargetWriterProbeResult{}, errors.New("bound native-ready canceled probe must never run")
		},
		Acceptance: func(context.Context, pgx.Tx, recovery.TargetWriterProof) (recovery.TargetWriterAcceptance, error) {
			atomic.AddInt32(acceptanceCalls, 1)
			return recovery.TargetWriterAcceptance{}, errors.New("bound native-ready canceled acceptance must never run")
		},
	}, f.lock, gate.endpoint, f.tools)
	if prepErr == nil || run != nil {
		t.Fatalf("canceled bound native-ready factory published a run: run=%v err=%v", run, prepErr)
	}
	if prepErr.Error() != "borrowed target lock health is uncertain" {
		t.Fatalf("canceled bound native-ready factory refusal is not the real lock-health-under-cancellation stage: %v", prepErr)
	}
	if strings.Contains(prepErr.Error(), "requires") || strings.Contains(prepErr.Error(), "identity") {
		t.Fatalf("canceled bound native-ready factory was refused as a malformed option: %v", prepErr)
	}
	// Causality evidence: the SAME retained lock is healthy under a live
	// context, so the refusal above is attributable to the canceled caller.
	healthCtx, cancelHealth := context.WithTimeout(ctx, borrowedOwnerRotationHealthBudget)
	healthErr := b.fixture.lock.Health(healthCtx)
	cancelHealth()
	if healthErr != nil {
		t.Fatalf("bound native-ready control lock is not healthy under a live context: %v", healthErr)
	}
	if got := marker.callsNow(); got != 0 {
		t.Fatalf("canceled bound native-ready factory ran the marker %d times, want 0", got)
	}
	t.Logf("bound native-ready canceled-factory control: the already-canceled caller refused the real bound factory at the cancellation stage and published no run")
}

// borrowedBoundNativeReadyUnavailableArchiveRefuse proves a nil archive is
// refused by the actual coordinator required-option check, never by the guard,
// with all outputs captured and no callback or child.
func borrowedBoundNativeReadyUnavailableArchiveRefuse(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding, probeCalls, acceptanceCalls *int32) {
	t.Helper()
	f := b.fixture
	attemptID := fmt.Sprintf("borrowed-replacement-bound-native-ready-unavailable-%d", time.Now().UnixNano())
	marker := newBorrowedBoundNativeReadyMarker(attemptID, f.adminRole)
	attempt, err := newBorrowedReplacementPrelaunchAttemptWith(t, ctx, b, fresh, nil, attemptID, probeCalls, acceptanceCalls,
		&borrowedReplacementPrelaunchBoundOptions{InstanceID: f.instanceID, Prelaunch: marker.hook})
	if err != nil || attempt == nil {
		t.Fatalf("bound native-ready unavailable-archive preparation refused: attempt=%v err=%v", attempt, err)
	}
	if err := attempt.registerOrigin(ctx); err != nil {
		t.Fatalf("bound native-ready unavailable-archive origin registration refused: %v", err)
	}
	result, receipt, runErr := dispatchBorrowedBoundNativeReady(t, ctx, attempt)
	if runErr == nil {
		t.Fatal("unavailable bound archive was accepted as a native-ready attempt")
	}
	if !strings.Contains(runErr.Error(), "archive") || strings.Contains(runErr.Error(), "guard") {
		t.Fatalf("unavailable bound archive refusal is not the required-option archive stage: %v", runErr)
	}
	borrowedBoundNativeReadyRequireNothingPublished(t, "unavailable bound archive attempt", result, receipt)
	if identity := attempt.run.Observation().StartedIdentity(); identity.Started {
		t.Fatalf("unavailable bound archive attempt started a child: %+v", identity)
	}
	if got := marker.callsNow(); got != 0 {
		t.Fatalf("unavailable bound archive attempt ran the marker %d times, want 0", got)
	}
	if got := atomic.LoadInt32(probeCalls); got != 0 {
		t.Fatalf("unavailable bound archive attempt ran the replacement probe %d times, want 0", got)
	}
	if got := atomic.LoadInt32(acceptanceCalls); got != 0 {
		t.Fatalf("unavailable bound archive attempt ran acceptance %d times, want 0", got)
	}
	t.Logf("bound native-ready unavailable-archive control: the nil archive was refused by the actual required-option check (never the guard) with no marker callback, absent receipt, no child and zero callbacks")
}

// borrowedBoundNativeReadyArchiveBoundary proves the descriptor-bound archive
// boundary: nil fixture/descriptor refusals, a non-empty genuine digest at
// offset zero, and the existing pathname-substitution/content-change/
// oversize/restore controls.
func borrowedBoundNativeReadyArchiveBoundary(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline) {
	t.Helper()
	if reader, err := borrowedReplacementPrelaunchArchiveReader(nil); err == nil || reader != nil {
		t.Fatal("nil fixture archive published a reader")
	}
	if digest, err := borrowedReplacementPrelaunchArchiveDigest(nil); err == nil || digest != "" || !strings.Contains(err.Error(), "requires the concrete descriptor") {
		t.Fatalf("nil descriptor digest did not refuse: digest=%q err=%v", digest, err)
	}
	genuine := borrowedReplacementPrelaunchArchive(t, b.fixture)
	digest, err := borrowedReplacementPrelaunchArchiveDigest(genuine)
	if err != nil || digest == "" {
		t.Fatalf("genuine bound archive digest is empty or refused: %v", err)
	}
	if offset, err := genuine.Seek(0, io.SeekCurrent); err != nil || offset != 0 {
		t.Fatalf("genuine bound archive reader did not start at offset zero: offset=%d err=%v", offset, err)
	}
	// Direct in-lane pathname-substitution and oversize assertions: the wrapper
	// reader must be NIL and the oversized digest EMPTY beside their expected
	// refusals (the CLOSED controls check only the error).
	path := b.fixture.archive.Name()
	backup := path + ".native-ready-backup"
	if err := os.Rename(path, backup); err != nil {
		t.Fatalf("bound archive substitution backup rename refused: %v", err)
	}
	restored := false
	t.Cleanup(func() {
		if restored {
			return
		}
		_ = os.Remove(path)
		_ = os.Rename(backup, path)
	})
	if err := os.WriteFile(path, []byte("bound-native-ready-substitute-content"), 0o600); err != nil {
		t.Fatalf("bound archive substitution write refused: %v", err)
	}
	substituted, subErr := borrowedReplacementPrelaunchArchiveReader(b.fixture)
	if subErr == nil || substituted != nil {
		if substituted != nil {
			_ = substituted.Close()
		}
		t.Fatalf("pathname-substituted bound archive published a reader (reader=%v err=%v)", substituted, subErr)
	}
	if err := os.Remove(path); err != nil {
		t.Fatalf("bound archive substitution cleanup refused: %v", err)
	}
	if err := os.Rename(backup, path); err != nil {
		t.Fatalf("bound archive substitution restore refused: %v", err)
	}
	restored = true
	oversized, err := os.CreateTemp(t.TempDir(), "bound-native-ready-oversize")
	if err != nil {
		t.Fatalf("bound oversize archive fixture refused: %v", err)
	}
	t.Cleanup(func() { _ = oversized.Close() })
	if err := oversized.Truncate(borrowedReplacementPrelaunchArchiveLimit + 1); err != nil {
		t.Fatalf("bound oversize archive truncate refused: %v", err)
	}
	overDigest, overErr := borrowedReplacementPrelaunchArchiveDigest(oversized)
	if overErr == nil || overDigest != "" || !strings.Contains(overErr.Error(), "exceeds the supported size limit") {
		t.Fatalf("oversized bound descriptor digest did not refuse empty: digest=%q err=%v", overDigest, overErr)
	}
	borrowedReplacementPrelaunchArchiveControls(t, b.fixture)
	t.Logf("bound native-ready archive boundary: the nil fixture/descriptor refused, the genuine descriptor digest was non-empty at offset zero, the direct in-lane pathname-substitution nil-reader and oversize empty-digest assertions passed, and the existing pathname-substitution/content-change/oversize/restore controls passed")
}

// borrowedBoundNativeReadyConcurrentRefuse proves two concurrent dispatches
// through ONE prepared bound attempt resolve to exactly one reservation
// refusal and one bound dirty-guard refusal, with the marker callback exactly
// once and no child.
func borrowedBoundNativeReadyConcurrentRefuse(t *testing.T, ctx context.Context) {
	t.Helper()
	b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
	var probeCalls, acceptanceCalls int32
	attemptID := fmt.Sprintf("borrowed-replacement-bound-native-ready-concurrent-%d", time.Now().UnixNano())
	marker := newBorrowedBoundNativeReadyMarker(attemptID, b.fixture.adminRole)
	attempt := prepareBorrowedBoundNativeReadyAttempt(t, ctx, b, fresh, attemptID, marker, &probeCalls, &acceptanceCalls)
	if err := attempt.registerOrigin(ctx); err != nil {
		t.Fatalf("bound native-ready concurrent origin registration refused: %v", err)
	}
	runCtx, cancelRun := context.WithTimeout(ctx, 60*time.Second)
	type concurrentOutcome struct {
		result  recovery.TargetWriterResult
		receipt recovery.DrillTargetProcessReceipt
		err     error
	}
	outcomes := make(chan concurrentOutcome, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	for i := 0; i < 2; i++ {
		go func() {
			defer wg.Done()
			result, receipt, err := attempt.dispatchRun(runCtx)
			outcomes <- concurrentOutcome{result: result, receipt: receipt, err: err}
		}()
	}
	joined := make(chan struct{})
	go func() { wg.Wait(); close(joined) }()
	joinedNow := false
	unwind := func() {
		cancelRun()
		if joinedNow {
			return
		}
		select {
		case <-joined:
			joinedNow = true
		case <-time.After(30 * time.Second):
			t.Errorf("concurrent bound native-ready dispatch join is unknown after the bounded unwind")
		}
	}
	defer unwind()
	t.Cleanup(unwind)
	select {
	case <-joined:
		joinedNow = true
	case <-time.After(60 * time.Second):
		t.Errorf("concurrent bound native-ready dispatch did not complete within the bounded join; outcome unknown")
		return
	}
	cancelRun()
	var reserved, guard int
	var reservedOutcome, guardOutcome concurrentOutcome
	for i := 0; i < 2; i++ {
		outcome := <-outcomes
		if outcome.err == nil {
			t.Fatal("concurrent bound native-ready dispatch was accepted")
		}
		switch {
		case strings.Contains(outcome.err.Error(), "already reserved"):
			reserved++
			reservedOutcome = outcome
		case outcome.err.Error() == "bound target guard is not clean":
			guard++
			guardOutcome = outcome
		default:
			t.Fatalf("concurrent bound native-ready dispatch refused with an unexpected error: %v", outcome.err)
		}
	}
	if reserved != 1 || guard != 1 {
		t.Fatalf("concurrent bound native-ready outcomes: reserved=%d guard=%d, want exactly one each", reserved, guard)
	}
	// The reservation outcome must publish NOTHING: a zero result (no marker
	// token) and the exact absent-receipt refusal.
	borrowedBoundNativeReadyRequireNothingPublished(t, "concurrent reservation", reservedOutcome.result, reservedOutcome.receipt)
	// The dirty-guard outcome may carry the legitimate transaction-local
	// marker token (produced and rolled back) but must publish no receipt.
	if guardOutcome.result.MarkerToken != marker.token() || guardOutcome.result.MarkerToken.InstanceID != b.fixture.instanceID {
		t.Fatalf("concurrent dirty-guard outcome did not carry the genuine transaction-local marker: outcome=%+v marker=%+v", guardOutcome.result.MarkerToken, marker.token())
	}
	borrowedBoundNativeReadyRequireNoProbeOutput(t, "concurrent dirty-guard", guardOutcome.result)
	borrowedBoundNativeReadyRequireAbsentReceipt(t, "concurrent dirty-guard", guardOutcome.receipt)
	if got := marker.callsNow(); got != 1 {
		t.Fatalf("concurrent bound native-ready marker calls=%d, want exactly 1", got)
	}
	if identity := attempt.run.Observation().StartedIdentity(); identity.Started {
		t.Fatalf("concurrent bound native-ready dispatch started a child: %+v", identity)
	}
	if got := atomic.LoadInt32(&probeCalls); got != 0 {
		t.Fatalf("concurrent bound native-ready dispatch ran the replacement probe %d times, want 0", got)
	}
	if got := atomic.LoadInt32(&acceptanceCalls); got != 0 {
		t.Fatalf("concurrent bound native-ready dispatch ran acceptance %d times, want 0", got)
	}
	borrowedOwnerDDLGuard(t, ctx, b)
	t.Logf("bound native-ready concurrent control: exactly one reservation refusal and one bound dirty-guard refusal resolved concurrently, with the marker callback exactly once and no child")
}

// borrowedBoundNativeReadyPartialDDLRefuse proves a partial/UNKNOWN DDL stops
// the bound orchestration before the replacement verification, with the stage
// counter exactly at the successor retirement.
func borrowedBoundNativeReadyPartialDDLRefuse(t *testing.T, ctx context.Context) {
	t.Helper()
	b := newBorrowedSuccessorBaselineBound(t, ctx)
	f := b.fixture
	templateCtx, cancelTemplate := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	templateBase := borrowedIdentityRoute(t, templateCtx, f.fx, "template1")
	cancelTemplate()
	templateConn := borrowedOwnerDDLConnect(t, ctx, borrowedAuthRoleDSN(t, templateBase, f.controlRole, f.controlPass))
	var stages int32
	orchestrateCtx, cancelOrchestrate := borrowedReplacementOrchestrationContext(ctx)
	captured, err := borrowedReplacementBindingOrchestrate(orchestrateCtx, t, b, &stages)
	cancelOrchestrate()
	if err == nil || captured != nil {
		t.Fatalf("bound partial/UNKNOWN DDL produced a capture: capture=%v err=%v", captured, err)
	}
	if got := atomic.LoadInt32(&stages); got != borrowedReplacementStageSuccessorRetired {
		t.Fatalf("bound partial/UNKNOWN DDL stages=%d, want stop at %d", got, borrowedReplacementStageSuccessorRetired)
	}
	borrowedSuccessorCloseConn(t, templateConn)
	borrowedOwnerDDLGuard(t, ctx, b)
	t.Logf("bound partial/UNKNOWN DDL control: the capture was nil and the orchestration stopped exactly at the successor retirement stage")
}

// borrowedBoundNativeReadyContaminatedRefuse proves a contaminated replacement
// stops the bound orchestration after the DDL but before any capture, with the
// stage counter exactly at the DDL commit.
func borrowedBoundNativeReadyContaminatedRefuse(t *testing.T, ctx context.Context) {
	t.Helper()
	b := newBorrowedSuccessorBaselineBound(t, ctx)
	borrowedOwnerDDLContaminateTemplate(t, ctx, b, []string{
		`CREATE VIEW public.owner_ddl_replacement_probe AS SELECT 1 AS one`,
	})
	var stages int32
	orchestrateCtx, cancelOrchestrate := borrowedReplacementOrchestrationContext(ctx)
	captured, err := borrowedReplacementBindingOrchestrate(orchestrateCtx, t, b, &stages)
	cancelOrchestrate()
	if err == nil || captured != nil {
		t.Fatalf("bound contaminated replacement produced a capture: capture=%v err=%v", captured, err)
	}
	if got := atomic.LoadInt32(&stages); got != borrowedReplacementStageDDLCommitted {
		t.Fatalf("bound contaminated replacement stages=%d, want stop at %d", got, borrowedReplacementStageDDLCommitted)
	}
	borrowedOwnerDDLGuard(t, ctx, b)
	t.Logf("bound contaminated-replacement control: the capture was nil and the orchestration stopped exactly at the DDL commit stage")
}

// TestBorrowedReplacementBoundNativeReadyRefusal is the bounded instance-bound
// native-ready replacement refusal lane described in the file header.
func TestBorrowedReplacementBoundNativeReadyRefusal(t *testing.T) {
	ctx := t.Context()

	// P: the single chain over ONE authentic bound fixture: prerequisite
	// session, snapshot, native-ready preparation, origin registration and
	// EXACTLY ONE actual coordinator dispatch refused by the exact production
	// bound dirty-guard refusal.
	func() {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		f := b.fixture

		// Descriptor-bound archive boundary before any preparation.
		borrowedBoundNativeReadyArchiveBoundary(t, ctx, b)

		// Bound retained-session prerequisite: register the exact genuine P1,
		// use once (identity queries counted separately), retire through the
		// existing strict disappearance.
		conn, reg := borrowedReplacementSessionRegisterP1(t, ctx, b, fresh)
		borrowedBoundSessionAssertRegistrationRetained(t, ctx, b, fresh, conn, reg, "bound native-ready prerequisite")
		useCtx, cancelUse := context.WithTimeout(ctx, 60*time.Second)
		useErr := reg.Use(useCtx)
		cancelUse()
		if useErr != nil {
			t.Fatalf("bound native-ready prerequisite session use refused: %v", useErr)
		}
		if got := atomic.LoadInt32(&reg.state.probeExecutions); got != 1 {
			t.Fatalf("bound native-ready prerequisite session probe executions=%d, want exactly 1", got)
		}
		if got := atomic.LoadInt32(&reg.state.identityQueries); got == 0 {
			t.Fatal("bound native-ready prerequisite identity-discovery queries were not counted separately")
		}
		borrowedReplacementSessionRetire(t, ctx, b, conn, reg.state)
		if _, closedErr := borrowedSuccessorReadConnIdentity(ctx, conn); closedErr == nil {
			t.Fatal("retired bound native-ready prerequisite connection still reads an identity")
		}

		// Counter distinctions: the baseline intentional probe refusal ran once,
		// acceptance never ran, and the replacement coordinator was never Run
		// before the single dispatch below.
		if got := atomic.LoadInt32(&f.probeCalls); got != 1 {
			t.Fatalf("bound native-ready baseline intentional probe refusals=%d, want 1", got)
		}
		if got := atomic.LoadInt32(&f.acceptanceCalls); got != 0 {
			t.Fatalf("bound native-ready baseline ran acceptance %d times", got)
		}

		// Snapshot before preparation.
		snap0 := borrowedBoundSessionSnapshotNow(t, ctx, b)
		rows0 := borrowedBoundNativeReadyRowsNow(t, ctx, b)

		// Native-ready preparation with the REAL bound options.
		var probeCalls, acceptanceCalls int32
		attemptID := fmt.Sprintf("borrowed-replacement-bound-native-ready-%d", time.Now().UnixNano())
		marker := newBorrowedBoundNativeReadyMarker(attemptID, f.adminRole)
		attempt := prepareBorrowedBoundNativeReadyAttempt(t, ctx, b, fresh, attemptID, marker, &probeCalls, &acceptanceCalls)
		if err := attempt.registerOrigin(ctx); err != nil {
			t.Fatalf("bound native-ready origin registration refused: %v", err)
		}

		// Pre-dispatch retained copies for the replay control.
		attemptCopy := *attempt
		runCopy := *attempt.run

		// EXACTLY ONE actual coordinator dispatch.
		beforeOID, oidErr := borrowedOwnerDDLTargetOID(ctx, b)
		if oidErr != nil || beforeOID != fresh.replacementOID {
			t.Fatalf("bound native-ready replacement catalog identity before the dispatch: oid=%d err=%v", beforeOID, oidErr)
		}
		result, receipt, runErr := dispatchBorrowedBoundNativeReady(t, ctx, attempt)
		if runErr == nil {
			t.Fatal("bound native-ready attempt was accepted with a dirty guard")
		}
		if runErr.Error() != "bound target guard is not clean" {
			t.Fatalf("bound native-ready refusal is not the exact production bound-guard refusal: %v", runErr)
		}
		if got := marker.callsNow(); got != 1 {
			t.Fatalf("bound native-ready marker callback calls=%d, want exactly 1", got)
		}
		markerToken := marker.token()
		if markerToken.InstanceID != f.instanceID || markerToken.State != "open" || markerToken.Generation <= 0 || markerToken.Hash == "" {
			t.Fatalf("bound native-ready marker did not return the genuine transaction-local token: %+v", markerToken)
		}
		if result.MarkerToken != markerToken {
			t.Fatalf("bound native-ready production result did not carry the genuine transaction-local marker token: result=%+v marker=%+v", result.MarkerToken, markerToken)
		}
		borrowedBoundNativeReadyRequireNoProbeOutput(t, "bound native-ready refusal", result)
		if identity := attempt.run.Observation().StartedIdentity(); identity.Started || identity.PID != 0 || identity.StartID != 0 {
			t.Fatalf("bound native-ready refusal started a child: %+v", identity)
		}
		borrowedBoundNativeReadyRequireAbsentReceipt(t, "bound native-ready refusal", receipt)
		if got := atomic.LoadInt32(&probeCalls); got != 0 {
			t.Fatalf("bound native-ready refusal ran the replacement probe %d times, want 0", got)
		}
		if got := atomic.LoadInt32(&acceptanceCalls); got != 0 {
			t.Fatalf("bound native-ready refusal ran acceptance %d times, want 0", got)
		}
		afterOID, oidErr := borrowedOwnerDDLTargetOID(ctx, b)
		if oidErr != nil || afterOID != beforeOID {
			t.Fatalf("bound native-ready refusal changed the replacement catalog: oid=%d err=%v", afterOID, oidErr)
		}
		borrowedBoundSessionAssertSnapshotEqual(t, "bound native-ready refusal", snap0, borrowedBoundSessionSnapshotNow(t, ctx, b))
		if rowsAfter := borrowedBoundNativeReadyRowsNow(t, ctx, b); rowsAfter != rows0 {
			t.Fatalf("bound native-ready refusal left durable marker/evidence rows: before=%+v after=%+v", rows0, rowsAfter)
		}
		healthCtx, cancelHealth := context.WithTimeout(ctx, borrowedOwnerRotationHealthBudget)
		healthErr := f.lock.Health(healthCtx)
		cancelHealth()
		if healthErr != nil {
			t.Fatalf("bound native-ready refusal original control owner health: %v", healthErr)
		}
		anchorCtx, cancelAnchor := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
		anchorErr := b.anchor.Recheck(anchorCtx)
		cancelAnchor()
		if anchorErr != nil {
			t.Fatalf("bound native-ready refusal original anchor recheck: %v", anchorErr)
		}
		if disposition := borrowedReplacementPrelaunchGuardDisposition(t, ctx, f); disposition == "clean" {
			t.Fatal("bound native-ready refusal resolved the deliberately dirty bound guard")
		}
		t.Logf("bound native-ready refusal: marker callback exactly once with genuine token %s generation %d, exact refusal %q, no child, absent receipt, zero replacement callbacks, catalog %d unchanged, snapshot/rows unchanged, owner/anchor healthy, guard unresolved, inventory NULL", markerToken.InstanceID, markerToken.Generation, runErr.Error(), afterOID)

		// Replay/copy negatives on the same genuine fixture.
		borrowedBoundNativeReadyReplayRefuse(t, ctx, b, attempt, &attemptCopy, &runCopy, marker, &probeCalls, &acceptanceCalls, snap0, rows0)

		// Non-destructive pre-launch negatives on the same genuine fixture.
		borrowedBoundNativeReadyPreparationCancelRefuse(t, ctx, b, fresh, &probeCalls, &acceptanceCalls)
		borrowedBoundNativeReadyWrongObserverRefuse(t, ctx, b, &probeCalls, &acceptanceCalls)
		borrowedBoundNativeReadyCanceledFactoryRefuse(t, ctx, b, &probeCalls, &acceptanceCalls)
		borrowedBoundNativeReadyUnavailableArchiveRefuse(t, ctx, b, fresh, &probeCalls, &acceptanceCalls)

		// Final snapshot equality after all non-destructive controls.
		borrowedBoundSessionAssertSnapshotEqual(t, "bound native-ready non-destructive controls", snap0, borrowedBoundSessionSnapshotNow(t, ctx, b))
		borrowedOwnerDDLGuard(t, ctx, b)
	}()

	// Destructive controls on independent bound fixtures.
	borrowedBoundNativeReadyConcurrentRefuse(t, ctx)
	borrowedBoundNativeReadyPreparationSourceLossRefuse(t, ctx)
	borrowedBoundNativeReadyPreparedBeforeLossRefuse(t, ctx)
	borrowedBoundNativeReadyPartialDDLRefuse(t, ctx)
	borrowedBoundNativeReadyContaminatedRefuse(t, ctx)
}
