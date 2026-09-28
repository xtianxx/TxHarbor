//go:build integration

// restore_invalidation_integration_test.go covers the duplicate-restore
// invalidation timing (B5-B6 follow-up, data-model §5 + INV-3, resumption-gate
// §4): what makes the old evidence/approvals unusable for admission when an
// instance with a valid release is restored again.
//
// Trace at HEAD 7e0a9cf (before the fix this file pins):
//
//   - ExecuteRestore (restore.go) validated its preconditions and ran
//     pg_restore straight away; the only generation-advancing write
//     (MutationRestoreProbeAccepted) happened after the probes passed.
//
//   - CommitEvidenceWrite (generation.go) invalidates every release/approval
//     bound to the previous generation/hash when it accepts a write.
//
//   - Gate.Admit (gate.go) re-derives validity inside the instance row lock on
//     every admission and refuses release_invalidated_generation /
//     approval_stale.
//
//     => nothing made the old evidence/approvals unusable before the first
//     target write, and an interrupted restore left the old authorization
//     basis valid. The fix commits a MutationRestoreStarted marker through
//     the same generation protocol before pg_restore, so the generation
//     advances before the first target write and stays advanced when the
//     restore fails or is interrupted (the failure never restores the old
//     permission).
//
// Fixture note: the approval/release/isolation write paths of T048-T051 are
// not delivered in this batch, so the authorization basis is seeded through
// the real controlstore.AppendApprovalDecision / AppendReleaseDecision paths
// and fixture SQL exactly as in gate_integration_test.go (base fixture:
// newBkpFixture + gateFixture seeds). The restore path under test is the real
// executor with the real container-backed PGCommand.
//
// In-flight boundary (scenario 4): a gate admission that linearized before
// the marker commit is an admitted, in-flight action. These tests assert only
// the existing semantics (the admission stands, the next admission refuses)
// and do not invent a runtime stop, rewind or cross-system atomicity.
package recovery

import (
	"context"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

const rsiScope = "chain=1;asset=usdc;kind=restore-invalidation"

// rsiGateFixture extends the recovery fixture with the approver identity and
// returns a gateFixture view over the same control store and instance.
func rsiGateFixture(t *testing.T, f *bkpFixture) *gateFixture {
	t.Helper()
	gateMapIdentity(t, f.ctx, f.store, "auth:approver", "person-approver")
	gateRegister(t, f.ctx, f.store, f.instanceID, "auth:approver", "approver")
	return &gateFixture{
		ctx: f.ctx, dsn: f.ctrlDSN, pool: f.ctrl, store: f.store,
		instanceID: f.instanceID, scope: rsiScope,
	}
}

// rsiVerifiedBackup runs one real backup and one real isolated verify-backup
// (the S1/S2 path), leaving a verified manifest plus the backup_manifest
// evidence row bound to the instance.
func rsiVerifiedBackup(t *testing.T, f *bkpFixture) BackupResult {
	t.Helper()
	backup := f.backup(t, nil)
	verifyTarget := f.createDatabase(t, "tgt_verify_rsi")
	if got := f.verifyBackup(t, backup.ManifestPath, verifyTarget); got.State != VerificationVerified {
		t.Fatalf("isolated verify state = %q, want verified (checks=%+v)", got.State, got.Checks)
	}
	return backup
}

// rsiMarkerAuditCount counts the operator-facing pre-write marker rows of one
// backup.
func rsiMarkerAuditCount(t *testing.T, f *bkpFixture, backupID string) int {
	t.Helper()
	var n int
	if err := f.ctrl.QueryRow(f.ctx, `
SELECT count(*) FROM recovery_audit
WHERE instance_id = $1 AND action = $2 AND result = 'ok'
  AND target->>'backup_id' = $3`,
		f.instanceID, ActionRestoreStarted, backupID).Scan(&n); err != nil {
		t.Fatalf("count restore_started audit rows: %v", err)
	}
	return n
}

// rsiBarrierPGCommand wraps the real container-backed PGCommand: the first
// tool call signals the test and waits before running the real tool. This is
// the controlled barrier that fixes the "before the first target write" point.
type rsiBarrierPGCommand struct {
	inner   PGCommand
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *rsiBarrierPGCommand) Run(ctx context.Context, name string, args []string,
	stdin io.Reader, stdout, stderr io.Writer) error {
	c.once.Do(func() {
		close(c.entered)
		<-c.release
	})
	return c.inner.Run(ctx, name, args, stdin, stdout, stderr)
}

// rsiCountingPGCommand wraps the real container-backed PGCommand and counts
// tool calls (target-touch evidence), never substituting for the real tool.
type rsiCountingPGCommand struct {
	inner PGCommand
	calls atomic.Int64
}

func (c *rsiCountingPGCommand) Run(ctx context.Context, name string, args []string,
	stdin io.Reader, stdout, stderr io.Writer) error {
	c.calls.Add(1)
	return c.inner.Run(ctx, name, args, stdin, stdout, stderr)
}

type rsiOutcome struct {
	result RestoreResult
	err    error
}

func rsiRestore(f *bkpFixture, backup BackupResult, target string, pg PGCommand) (RestoreResult, error) {
	return ExecuteRestore(f.ctx, RestoreOptions{
		ManifestPath:      backup.ManifestPath,
		InstanceID:        f.instanceID,
		ControlStore:      f.store,
		ControlDSN:        f.ctrlDSN,
		TargetDSN:         target,
		TargetDeclaration: TargetIsolated,
		Actor:             "deploy:executor",
		ProgramVersion:    bkpProgramVersion,
		PG:                pg,
	})
}

// TestRestorePreWriteMarkerInvalidatesBeforeFirstTargetWrite is scenario 2:
// before a new real restore starts, the invalidation is persisted (generation
// advance through the marker) so the old release/approval can no longer be
// used for admission before the first target write.
func TestRestorePreWriteMarkerInvalidatesBeforeFirstTargetWrite(t *testing.T) {
	f := newBkpFixture(t)
	f.seedProbeTable(t)
	backup := rsiVerifiedBackup(t, f)
	gf := rsiGateFixture(t, f)
	gf.seedIsolationSet(t, CapabilityQuery)
	approvalID := gf.approve(t, CapabilityQuery, "auth:approver", "person-approver", ApprovalClassSingleNonExecutor)
	gf.release(t, CapabilityQuery, []string{approvalID})
	gate := gateNewGate(t, f.store, GateOptions{})
	if d := gf.admit(t, gate, CapabilityQuery); !d.Allowed {
		t.Fatalf("fixture release must allow before the restore, got %+v", d)
	}
	generationBefore, hashBefore := gateInstanceToken(t, f.ctx, f.ctrl, f.instanceID)

	target := f.createDatabase(t, "tgt_rsi_marker")
	barrier := &rsiBarrierPGCommand{inner: f.pg, entered: make(chan struct{}), release: make(chan struct{})}
	done := make(chan rsiOutcome, 1)
	go func() {
		result, err := rsiRestore(f, backup, target, barrier)
		done <- rsiOutcome{result: result, err: err}
	}()
	select {
	case <-barrier.entered:
	case <-time.After(30 * time.Second):
		t.Fatal("the restore never reached its first tool call")
	}

	// The first target write has not happened yet.
	if got := f.userTableCount(t, target); got != 0 {
		t.Fatalf("the target was written before the marker barrier: %d user tables", got)
	}

	// The marker committed: exactly one generation advance, audited with the
	// marker action and the accepted generation.
	generationAtMarker, hashAtMarker := gateInstanceToken(t, f.ctx, f.ctrl, f.instanceID)
	if generationAtMarker != generationBefore+1 || hashAtMarker == hashBefore {
		t.Fatalf("pre-write marker must advance exactly one generation: %d -> %d (hash changed=%t)",
			generationBefore, generationAtMarker, hashAtMarker != hashBefore)
	}
	if n := rsiMarkerAuditCount(t, f, backup.Manifest.BackupID); n != 1 {
		t.Fatalf("restore_started marker audit rows = %d, want 1", n)
	}
	var markerGeneration int64
	if err := f.ctrl.QueryRow(f.ctx, `
SELECT evidence_generation FROM recovery_audit
WHERE instance_id = $1 AND action = $2 AND result = 'ok' AND target->>'backup_id' = $3`,
		f.instanceID, ActionRestoreStarted, backup.Manifest.BackupID).Scan(&markerGeneration); err != nil {
		t.Fatalf("read restore_started marker generation: %v", err)
	}
	if markerGeneration != generationAtMarker {
		t.Fatalf("restore_started marker generation = %d, want the accepted %d", markerGeneration, generationAtMarker)
	}
	// The protocol row records the captured generation and the accepted one in
	// its target (data-model §5 audit shape).
	var capturedGeneration int64
	var acceptedGeneration string
	var protocolKind string
	if err := f.ctrl.QueryRow(f.ctx, `
SELECT evidence_generation, target->>'kind', target->>'accepted_generation' FROM recovery_audit
WHERE instance_id = $1 AND action = 'evidence_write' AND result = 'ok'
  AND target->>'kind' = 'restore_started'`,
		f.instanceID).Scan(&capturedGeneration, &protocolKind, &acceptedGeneration); err != nil {
		t.Fatalf("read restore_started protocol audit: %v", err)
	}
	if protocolKind != string(MutationRestoreStarted) || capturedGeneration != generationBefore ||
		acceptedGeneration != strconv.FormatInt(generationAtMarker, 10) {
		t.Fatalf("marker protocol audit = kind %s captured %d accepted %s, want %s/%d/%d",
			protocolKind, capturedGeneration, acceptedGeneration, MutationRestoreStarted, generationBefore, generationAtMarker)
	}

	// Before the first target write, the old release is already unusable for
	// admission (the answer to the duplicate-restore timing question).
	if d := gf.admit(t, gate, CapabilityQuery); d.Allowed || d.RefusalClass != RefusalReleaseInvalidatedGeneration {
		t.Fatalf("old release must be stale before the first target write, got %+v", d)
	}

	close(barrier.release)
	var restored rsiOutcome
	select {
	case restored = <-done:
	case <-time.After(2 * time.Minute):
		t.Fatal("the restore did not finish")
	}
	if restored.err != nil || !restored.result.Restored {
		t.Fatalf("restore after the barrier = %+v / %v, want restored", restored.result, restored.err)
	}

	// The acceptance advanced a second generation and wrote the restored
	// evidence at it.
	if generationAfter, _ := gateInstanceToken(t, f.ctx, f.ctrl, f.instanceID); generationAfter != generationBefore+2 {
		t.Fatalf("generation after the successful restore = %d, want %d", generationAfter, generationBefore+2)
	}
	if n := f.controlEvidenceCount(t, "restore_probe", backup.Manifest.BackupID); n != 1 {
		t.Fatalf("restore_probe evidence rows = %d, want 1", n)
	}
	// The old release stays stale after the restore, and the old approval
	// cannot be re-bound by a release recorded at the new generation (INV-3).
	if d := gf.admit(t, gate, CapabilityQuery); d.Allowed || d.RefusalClass != RefusalReleaseInvalidatedGeneration {
		t.Fatalf("old release must stay stale after the restore, got %+v", d)
	}
	gf.release(t, CapabilityQuery, []string{approvalID})
	if d := gf.admit(t, gate, CapabilityQuery); d.Allowed || d.RefusalClass != RefusalApprovalStale {
		t.Fatalf("old approval must be stale for a release recorded at the new generation, got %+v", d)
	}
}

// TestRestoreInterruptionAfterFirstWriteKeepsOldPermissionStale is scenario 3:
// after the first write fails/is interrupted, the capability stays closed and
// the failure never brings the old permission back.
func TestRestoreInterruptionAfterFirstWriteKeepsOldPermissionStale(t *testing.T) {
	f := newBkpFixture(t)
	f.seedProbeTable(t)
	backup := rsiVerifiedBackup(t, f)
	gf := rsiGateFixture(t, f)
	gf.seedIsolationSet(t, CapabilityQuery)
	approvalID := gf.approve(t, CapabilityQuery, "auth:approver", "person-approver", ApprovalClassSingleNonExecutor)
	gf.release(t, CapabilityQuery, []string{approvalID})
	gate := gateNewGate(t, f.store, GateOptions{})
	if d := gf.admit(t, gate, CapabilityQuery); !d.Allowed {
		t.Fatalf("fixture release must allow before the restore, got %+v", d)
	}
	generationBefore, _ := gateInstanceToken(t, f.ctx, f.ctrl, f.instanceID)

	target := f.createDatabase(t, "tgt_rsi_interrupt")
	targetDB := bkpDBNameOf(t, f.adminDSN, target)
	// Park the real pg_restore on its first write: the target carries a
	// conflicting probe table whose ACCESS EXCLUSIVE lock the test holds.
	locker := bkpConnect(t, target)
	if _, err := locker.Exec(f.ctx, `CREATE TABLE `+bkpProbeTable+` (dummy text)`); err != nil {
		t.Fatalf("create conflicting table: %v", err)
	}
	if _, err := locker.Exec(f.ctx, `BEGIN`); err != nil {
		t.Fatalf("begin locker: %v", err)
	}
	if _, err := locker.Exec(f.ctx, `LOCK TABLE `+bkpProbeTable+` IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatalf("lock conflicting table: %v", err)
	}

	done := make(chan rsiOutcome, 1)
	go func() {
		result, err := rsiRestore(f, backup, target, f.pg)
		done <- rsiOutcome{result: result, err: err}
	}()
	pids := f.waitForLockWaiter(t, targetDB, 30*time.Second)

	// The marker is committed while the first write is parked: the capability
	// is closed before any target write can complete.
	generationParked, _ := gateInstanceToken(t, f.ctx, f.ctrl, f.instanceID)
	if generationParked != generationBefore+1 {
		t.Fatalf("generation while pg_restore is parked = %d, want the marker generation %d",
			generationParked, generationBefore+1)
	}
	if d := gf.admit(t, gate, CapabilityQuery); d.Allowed || d.RefusalClass != RefusalReleaseInvalidatedGeneration {
		t.Fatalf("capability must be closed during the restore, got %+v", d)
	}

	// Interrupt the real pg_restore mid-flight.
	f.terminateBackends(t, pids)
	if _, err := locker.Exec(f.ctx, `ROLLBACK`); err != nil {
		t.Fatalf("release blocker lock: %v", err)
	}
	if err := locker.Close(f.ctx); err != nil {
		t.Fatalf("close locker: %v", err)
	}
	var interrupted rsiOutcome
	select {
	case interrupted = <-done:
	case <-time.After(2 * time.Minute):
		t.Fatal("the interrupted restore did not return")
	}
	if interrupted.err == nil || interrupted.result.Restored {
		t.Fatalf("interrupted restore = %+v / %v, want refused", interrupted.result, interrupted.err)
	}

	// The failure neither wrote acceptance evidence nor rolled the generation
	// back: the marker stays.
	if generationAfter, _ := gateInstanceToken(t, f.ctx, f.ctrl, f.instanceID); generationAfter != generationParked {
		t.Fatalf("interrupted restore changed the generation: %d -> %d", generationParked, generationAfter)
	}
	if n := f.controlEvidenceCount(t, "restore_probe", backup.Manifest.BackupID); n != 0 {
		t.Fatalf("restore_probe evidence rows after interruption = %d, want 0", n)
	}
	// The old permission is not restored by the failure.
	if d := gf.admit(t, gate, CapabilityQuery); d.Allowed || d.RefusalClass != RefusalReleaseInvalidatedGeneration {
		t.Fatalf("old release must stay stale after the interruption, got %+v", d)
	}
	// The old approval died with the pre-restore generation: a release
	// recorded at the marker generation referencing it is stale.
	gf.release(t, CapabilityQuery, []string{approvalID})
	if d := gf.admit(t, gate, CapabilityQuery); d.Allowed || d.RefusalClass != RefusalApprovalStale {
		t.Fatalf("old approval must be stale after the interruption, got %+v", d)
	}

	// Retry = rebuild the target database, then rerun (idempotent): the rerun
	// starts a fresh marker and its own acceptance; exactly one restore_probe
	// row exists afterwards.
	rebuilt := f.rebuildTarget(t, bkpTarget{name: targetDB, dsn: target})
	rerun := f.restore(t, backup.ManifestPath, rebuilt, TargetIsolated, "")
	if !rerun.Restored {
		t.Fatalf("rerun after rebuild = %+v, want Restored=true", rerun)
	}
	if generationRerun, _ := gateInstanceToken(t, f.ctx, f.ctrl, f.instanceID); generationRerun != generationParked+2 {
		t.Fatalf("generation after the rerun = %d, want %d", generationRerun, generationParked+2)
	}
	if n := f.controlEvidenceCount(t, "restore_probe", backup.Manifest.BackupID); n != 1 {
		t.Fatalf("restore_probe evidence rows after the rerun = %d, want 1", n)
	}
	// The old basis is still invalid; only a fresh approval+release at the
	// current generation reopens the capability (no reuse of the old one).
	if d := gf.admit(t, gate, CapabilityQuery); d.Allowed || d.RefusalClass != RefusalReleaseInvalidatedGeneration {
		t.Fatalf("old release must stay stale after the rerun, got %+v", d)
	}
	freshApproval := gf.approve(t, CapabilityQuery, "auth:approver", "person-approver", ApprovalClassSingleNonExecutor)
	gf.release(t, CapabilityQuery, []string{freshApproval})
	if d := gf.admit(t, gate, CapabilityQuery); !d.Allowed {
		t.Fatalf("fresh approval+release at the current generation must allow, got %+v", d)
	}
}

// TestRestoreStartInterleavingDoesNotRewindAdmittedAction is scenario 4: an
// action admitted before the restore start is in-flight. The marker waits on
// the same instance row lock the admission holds; the admitted decision stands
// (the restore path has no stop/rewind), the target is not touched while the
// marker waits, and the next admission refuses.
func TestRestoreStartInterleavingDoesNotRewindAdmittedAction(t *testing.T) {
	f := newBkpFixture(t)
	f.seedProbeTable(t)
	backup := rsiVerifiedBackup(t, f)
	gf := rsiGateFixture(t, f)
	gf.seedIsolationSet(t, CapabilityQuery)
	approvalID := gf.approve(t, CapabilityQuery, "auth:approver", "person-approver", ApprovalClassSingleNonExecutor)
	gf.release(t, CapabilityQuery, []string{approvalID})
	generationBefore, _ := gateInstanceToken(t, f.ctx, f.ctrl, f.instanceID)

	entered := make(chan struct{})
	releaseAdmission := make(chan struct{})
	var enteredOnce sync.Once
	gate, err := NewGate(f.store, GateOptions{TTL: time.Minute, FundGates: func(context.Context, GateRequest) error {
		enteredOnce.Do(func() { close(entered) })
		<-releaseAdmission
		return nil
	}})
	if err != nil {
		t.Fatalf("NewGate: %v", err)
	}

	type admitOutcome struct {
		decision GateDecision
		err      error
	}
	admitted := make(chan admitOutcome, 1)
	go func() {
		decision, err := gate.Admit(f.ctx, GateRequest{
			InstanceID: f.instanceID, Capability: CapabilityQuery, ScopeHash: rsiScope,
			Actor: "deploy:executor", OperationID: gateOperation("rsi-admit"), Action: "test:restore-interleaving",
		})
		admitted <- admitOutcome{decision: decision, err: err}
	}()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the admission never reached the judgment point")
	}

	// The restore starts on a named control-store pool so its marker lock wait
	// is observable in pg_stat_activity (a controlled barrier, not a sleep).
	restorePool := gateNamedPool(t, f.ctrlDSN, "rsi-restore-marker")
	restoreStore, err := controlstore.NewStore(f.ctx, restorePool)
	if err != nil {
		t.Fatalf("NewStore for the restore: %v", err)
	}
	target := f.createDatabase(t, "tgt_rsi_interleave")
	pg := &rsiCountingPGCommand{inner: f.pg}
	done := make(chan rsiOutcome, 1)
	go func() {
		result, err := ExecuteRestore(f.ctx, RestoreOptions{
			ManifestPath:      backup.ManifestPath,
			InstanceID:        f.instanceID,
			ControlStore:      restoreStore,
			ControlDSN:        f.ctrlDSN,
			TargetDSN:         target,
			TargetDeclaration: TargetIsolated,
			Actor:             "deploy:executor",
			ProgramVersion:    bkpProgramVersion,
			PG:                pg,
		})
		done <- rsiOutcome{result: result, err: err}
	}()
	waitGateLockWait(t, f.ctx, f.ctrl, "rsi-restore-marker")
	if calls := pg.calls.Load(); calls != 0 {
		t.Fatalf("the target was touched (%d tool calls) while the marker was still waiting on the instance lock", calls)
	}

	// Let the admitted action finish: it linearized before the marker, so it
	// stands as an admitted (in-flight) action.
	close(releaseAdmission)
	var first admitOutcome
	select {
	case first = <-admitted:
	case <-time.After(30 * time.Second):
		t.Fatal("the admission did not finish")
	}
	if first.err != nil || !first.decision.Allowed {
		t.Fatalf("admission linearized before the marker must stand, got %+v / %v", first.decision, first.err)
	}
	if first.decision.EvidenceGeneration != generationBefore {
		t.Fatalf("admitted decision generation = %d, want the pre-marker %d",
			first.decision.EvidenceGeneration, generationBefore)
	}

	var restored rsiOutcome
	select {
	case restored = <-done:
	case <-time.After(2 * time.Minute):
		t.Fatal("the restore did not finish")
	}
	if restored.err != nil || !restored.result.Restored {
		t.Fatalf("restore after the admission = %+v / %v, want restored", restored.result, restored.err)
	}
	if calls := pg.calls.Load(); calls == 0 {
		t.Fatal("the restore reported success without calling the real tool")
	}
	// The next admission refuses: the marker invalidated the release.
	if d := gf.admit(t, gate, CapabilityQuery); d.Allowed || d.RefusalClass != RefusalReleaseInvalidatedGeneration {
		t.Fatalf("admission after the marker must refuse, got %+v", d)
	}
}

// TestRestoreRefusedBeforeStartWritesNoMarkerAndKeepsReleaseUsable pins the
// other side of the boundary: a refusal that never starts a restore (for
// example an unverified manifest) must not invalidate anything.
func TestRestoreRefusedBeforeStartWritesNoMarkerAndKeepsReleaseUsable(t *testing.T) {
	f := newBkpFixture(t)
	f.seedProbeTable(t)
	backup := f.backup(t, nil) // deliberately left unverified
	gf := rsiGateFixture(t, f)
	gf.seedIsolationSet(t, CapabilityQuery)
	approvalID := gf.approve(t, CapabilityQuery, "auth:approver", "person-approver", ApprovalClassSingleNonExecutor)
	gf.release(t, CapabilityQuery, []string{approvalID})
	gate := gateNewGate(t, f.store, GateOptions{})
	if d := gf.admit(t, gate, CapabilityQuery); !d.Allowed {
		t.Fatalf("fixture release must allow before the refused restore, got %+v", d)
	}

	target := f.createDatabase(t, "tgt_rsi_refused")
	refused := f.refusedRestore(t, backup.ManifestPath, target)
	if len(refused.Blocked) == 0 {
		t.Fatal("refusal did not report the missing preconditions")
	}
	if n := rsiMarkerAuditCount(t, f, backup.Manifest.BackupID); n != 0 {
		t.Fatalf("a refusal before the restore start wrote %d marker rows, want 0", n)
	}
	if got := f.userTableCount(t, target); got != 0 {
		t.Fatalf("refused restore created %d tables in the target", got)
	}
	if n := f.controlEvidenceCount(t, "restore_probe", backup.Manifest.BackupID); n != 0 {
		t.Fatalf("restore_probe evidence rows after a refusal = %d, want 0", n)
	}
	// A refusal that never touched the target must not invalidate the release.
	if d := gf.admit(t, gate, CapabilityQuery); !d.Allowed {
		t.Fatalf("a refused restore that never started must keep the release usable, got %+v", d)
	}
}
