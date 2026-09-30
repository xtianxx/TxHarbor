//go:build drill

// drill_failure_test.go is T059 [US6]: the F1-F7 failure-injection matrix of
// the independent drill channel. Every class converges on an explicit
// fail-closed refusal that is observable (a machine-checkable blocked list,
// closed refusal class or rejected manifest write-back), audited where the
// entry point audits, and re-entrant (a repeated attempt converges with zero
// state flips); every refusal path writes zero wrong opens and zero duplicated
// external side effects.
//
// The classes:
//
//	F1 backup unusable / corrupt / truncated / unverified
//	F2 restore interrupted / partially completed
//	F3 program/schema incompatible
//	F4 external facts lead the recovery point (zero replay / zero authority writes)
//	F5 old instance not isolated or not provable
//	F6 verification finds a gap that cannot be proven (unknown/pending,
//	   evidence package, responsibility, escalation; timeout/exhaustion/
//	   acknowledgement are not closure)
//	F7 unauthorized / insufficient / stale approvals
//
// Nothing below writes control-store decision rows directly; every refusal is
// produced by the real entry point (quickstart §4).
package recovery_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xtianxx/txharbor/internal/recovery"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

// TestT059F1BackupUnusableCorruptTruncatedUnverified covers F1: a backup that
// is unverified, corrupt, truncated or replaced by an edited manifest is never
// used; no best-effort restore happens and the target stays empty.
func TestT059F1BackupUnusableCorruptTruncatedUnverified(t *testing.T) {
	// This matrix asserts refusals only; do not require a local pg_restore.
	env := newDrillEnv(t, false)
	env.seedLiveBusinessState()
	liveBefore := drillFingerprint(t, env.data)
	backup := env.backup()

	// (a) An unverified backup carries no control-store verification evidence:
	// restore refuses before touching the target.
	targetA := env.createDatabase("f1_unverified")
	blocked, _ := drillRefusedRestoreWithObserver(env, backup.ManifestPath, targetA, drillProgramVersion, env.operation("f1-unverified"))
	drillAssertBlockedMentions(t, blocked, "not verified")
	if got := env.userTableCount(targetA); got != 0 {
		t.Fatalf("refused restore left %d user tables in the target (best-effort recovery)", got)
	}
	if got := env.evidenceCount("restore_probe"); got != 0 {
		t.Fatalf("refused restore wrote %d restore_probe evidence rows, want 0", got)
	}

	// (b) A truncated artifact: verify-backup records the rejected conclusion
	// and records no evidence; the restore is refused (never "best effort").
	if err := os.Truncate(backup.ArtifactPath, 64); err != nil {
		t.Fatalf("truncate artifact: %v", err)
	}
	// The full corrupt-archive verification path needs a direct local pg_restore
	// ELF; isolate that prerequisite so the refusal-only assertions still run.
	t.Run("corrupt-verification", func(t *testing.T) {
		requireDrillLocalPGRestore(t)
		verifyTarget := env.createDatabase("f1_verify")
		binding := env.bindVerifyTarget(verifyTarget)
		vres, err := recovery.ExecuteVerifyBackup(env.ctx, recovery.VerifyBackupOptions{
			ManifestPath: backup.ManifestPath, Binding: binding, TargetDSN: verifyTarget,
			Verifier: "auth:verifier", InstanceID: env.instanceID,
			ControlStore: env.store, ControlDSN: env.ctrlDSN,
			AuthoritativeDSN: env.dataDSN, ObserverDSN: env.adminDSN,
			ProgramVersion: drillProgramVersion, OperationID: env.operation("f1-verify"),
			PG: env.pg,
		})
		if err != nil {
			t.Fatalf("ExecuteVerifyBackup(corrupt): %v", err)
		}
		if vres.State != recovery.VerificationRejected {
			t.Fatalf("corrupt artifact verify state = %s, want rejected", vres.State)
		}
		if got := env.controlEvidenceCount("backup_manifest", backup.Manifest.BackupID); got != 0 {
			t.Fatalf("a rejected verification recorded %d backup_manifest evidence rows, want 0", got)
		}
	})
	targetB := env.createDatabase("f1_corrupt")
	corruptBlocked, _ := drillRefusedRestoreWithObserver(env, backup.ManifestPath, targetB, drillProgramVersion, env.operation("f1-corrupt"))
	if len(corruptBlocked.Blocked) == 0 {
		t.Fatal("the corrupt-artifact refusal carried no blocked preconditions")
	}
	if got := env.userTableCount(targetB); got != 0 {
		t.Fatalf("corrupt-artifact refusal left %d user tables in the target", got)
	}
	if got := env.evidenceCount("restore_probe"); got != 0 {
		t.Fatalf("corrupt-artifact refusal wrote %d restore_probe rows, want 0", got)
	}

	// (c) An unavailable manifest path is refused the same way.
	if _, err := drillRefusedRestoreWithObserver(env, backup.ManifestPath+".missing", targetB, drillProgramVersion, env.operation("f1-missing")); err == nil {
		t.Fatal("a missing manifest path was accepted")
	}

	// (d) A verified manifest that was edited afterwards can never inherit the
	// recorded verification (the manifest file flag is not proof).
	t.Run("edited-verified-manifest", func(t *testing.T) {
		requireDrillLocalPGRestore(t)
		backup2 := env.backup()
		verifiedTarget := env.createDatabase("f1_verified")
		env.verifyBackup(backup2.ManifestPath, verifiedTarget, env.operation("f1-verify-ok"))
		editedPath := backup2.ManifestPath + ".edited.json"
		raw, err := os.ReadFile(backup2.ManifestPath)
		if err != nil {
			t.Fatalf("read manifest: %v", err)
		}
		edited, err := recovery.ParseManifest(raw)
		if err != nil {
			t.Fatalf("parse manifest: %v", err)
		}
		edited.CreatedBy = "hand-edited"
		canonical, err := edited.CanonicalJSON()
		if err != nil {
			t.Fatalf("encode edited manifest: %v", err)
		}
		if err := os.WriteFile(editedPath, canonical, 0o600); err != nil {
			t.Fatalf("write edited manifest: %v", err)
		}
		targetC := env.createDatabase("f1_edited")
		editedBlocked, _ := drillRefusedRestoreWithObserver(env, editedPath, targetC, drillProgramVersion, env.operation("f1-edited"))
		drillAssertBlockedMentions(t, editedBlocked, "verify-backup")
		if got := env.userTableCount(targetC); got != 0 {
			t.Fatalf("edited-manifest refusal left %d user tables in the target", got)
		}
	})
	// Repeat the corrupt refusal once more: same class, zero extra rows.
	drillRefusedRestoreWithObserver(env, backup.ManifestPath, targetB, drillProgramVersion, env.operation("f1-corrupt-again"))
	if got := env.evidenceCount("restore_probe"); got != 0 {
		t.Fatalf("restore_probe rows = %d, want 0 from refusal-only F1", got)
	}

	// Zero authority-table writes on the live database through the whole F1
	// sequence (the drills only read it).
	drillAssertFingerprintEqual(t, liveBefore, drillFingerprint(t, env.data), "live database during F1")
}

// TestT059F2RestoreInterruptionNotRestoredRebuildRerunIdempotent covers the
// real interrupted-restore and ordinary-retry refusal portions of F2. It
// deliberately fails at the rebuild boundary until the original attempt can
// supply an authenticated writer identity and retained process handle.
func TestT059F2RestoreInterruptionNotRestoredRebuildRerunIdempotent(t *testing.T) {
	requireDrillLocalPGRestore(t)
	env := newDrillEnv(t, false)
	env.seedLiveBusinessState()
	env.seedProbeTable()

	backup := env.backup()
	verifyTarget := env.createDatabase("f2_verify")
	env.verifyBackup(backup.ManifestPath, verifyTarget, env.operation("f2-verify"))
	env.seedAuthoritativeTargetCleanBaseline()

	target := env.recoveryTarget()
	targetDB := env.dbNameOf(target)

	// Park the real pg_restore on a lock the test holds (the target carries a
	// conflicting copy of a table that is inside the archive).
	locker, err := pgx.Connect(env.ctx, target)
	if err != nil {
		t.Fatalf("connect locker: %v", err)
	}
	if _, err := locker.Exec(env.ctx, `CREATE TABLE `+drillProbeTable+` (dummy text)`); err != nil {
		t.Fatalf("create conflicting table: %v", err)
	}
	if _, err := locker.Exec(env.ctx, `BEGIN`); err != nil {
		t.Fatalf("begin locker: %v", err)
	}
	if _, err := locker.Exec(env.ctx, `LOCK TABLE `+drillProbeTable+` IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatalf("lock conflicting table: %v", err)
	}

	type restoreOutcome struct {
		result recovery.RestoreResult
		err    error
	}
	done := make(chan restoreOutcome, 1)
	restoreCtx, cancelRestore := context.WithCancel(context.Background())
	defer cancelRestore()
	interruptedOperation := env.operation("f2-interrupted")
	go func() {
		result, err := recovery.ExecuteRestore(restoreCtx, recovery.RestoreOptions{
			ManifestPath:      backup.ManifestPath,
			InstanceID:        env.instanceID,
			ControlStore:      env.store,
			ControlDSN:        env.ctrlDSN,
			ObserverDSN:       env.observerDSNFor(target),
			TargetDSN:         target,
			TargetDeclaration: recovery.TargetIsolated,
			Actor:             "deploy:executor",
			ProgramVersion:    drillProgramVersion,
			OperationID:       interruptedOperation,
			PG:                env.pg,
		})
		done <- restoreOutcome{result: result, err: err}
	}()

	// Observe that the fixture-owned real restore child reached the blocking
	// target operation, then cancel its parent context. The pg_restore command
	// process is terminated and waited by drillPGCommand.Run; no server PID is
	// retained or used as process identity.
	if pids := env.waitForLockWaiter(targetDB, 30*time.Second); len(pids) == 0 {
		t.Fatal("interrupted F2 restore never reached the real PostgreSQL lock barrier")
	}
	cancelRestore()
	if _, err := locker.Exec(env.ctx, `ROLLBACK`); err != nil {
		t.Fatalf("release blocker lock: %v", err)
	}
	if err := locker.Close(env.ctx); err != nil {
		t.Fatalf("close locker: %v", err)
	}

	var interrupted restoreOutcome
	select {
	case interrupted = <-done:
	case <-time.After(2 * time.Minute):
		t.Fatal("the interrupted restore did not return")
	}
	if interrupted.err == nil {
		t.Fatal("the interrupted restore returned no error")
	}
	if interrupted.result.Restored {
		t.Fatal("the interrupted restore was marked restored")
	}
	if got := env.controlEvidenceCount("restore_probe", backup.Manifest.BackupID); got != 0 {
		t.Fatalf("restore_probe rows after the interruption = %d, want 0", got)
	}
	if _, err := env.refusedRestore(backup.ManifestPath, target, drillProgramVersion, env.operation("f2-unwitnessed-retry")); err == nil {
		t.Fatal("ordinary retry was allowed before target witness recovery")
	}
	if got := env.controlEvidenceCount("restore_probe", backup.Manifest.BackupID); got != 0 {
		t.Fatalf("restore_probe rows after blocked retry = %d, want 0", got)
	}
	if err := drillWaitNoTargetSessions(env.ctx, env.admin, targetDB); err != nil {
		t.Fatalf("old restore attempt did not drain after owned process Wait: %v", err)
	}
	guardInfo, err := controlstore.ParseDSNTarget(target)
	if err != nil {
		t.Fatal(err)
	}
	guardKey, err := controlstore.TargetGuardKey(guardInfo)
	if err != nil {
		t.Fatal(err)
	}
	guard, found, err := controlstore.ReadTargetGuard(env.ctx, env.ctrl, guardKey)
	if err != nil || !found || guard.OperationID != interruptedOperation || guard.AttemptAppName == "" {
		t.Fatalf("interrupted attempt guard identity = %+v found=%t err=%v", guard, found, err)
	}
	if guard.State != controlstore.TargetGuardRebuildRequired {
		guardTx, err := env.ctrl.Begin(env.ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := controlstore.MarkTargetGuardWriterDrainedForAttempt(env.ctx, guardTx, guardKey, interruptedOperation, guard.AttemptAppName); err != nil {
			_ = guardTx.Rollback(env.ctx)
			t.Fatalf("record owned restore process drain (guard=%+v): %v", guard, err)
		}
		if err := controlstore.MarkTargetGuardRebuildRequiredForAttempt(env.ctx, guardTx, guardKey, interruptedOperation, guard.AttemptAppName); err != nil {
			_ = guardTx.Rollback(env.ctx)
			t.Fatalf("record interrupted target rebuild requirement: %v", err)
		}
		if err := guardTx.Commit(env.ctx); err != nil {
			t.Fatalf("commit drained interrupted target state: %v", err)
		}
	} else if guard.ActiveWriter {
		t.Fatalf("restore executor marked rebuild-required while writer is still active: %+v", guard)
	}

	// Do not infer that process drain alone authorizes a destructive rebuild.
	// rebuildTargetWithWitness must fail closed until it can bind authentic
	// original-attempt identity evidence; it must not write clean guard state.
	env.rebuildTargetWithWitness(target, env.operation("f2-witnessed-rebuild"))
}

// TestT059F3IncompatibleProgramRefusedNoSilentDowngrade covers F3: a
// program/schema incompatible environment is refused before it touches the
// target, with zero silent downgrade or automatic rewrite; the same backup
// verifies and restores under a compatible program identity.
func TestT059F3IncompatibleProgramRefusedNoSilentDowngrade(t *testing.T) {
	env := newDrillEnv(t, false)
	env.seedLiveBusinessState()
	liveBefore := drillFingerprint(t, env.data)
	backup := env.backup()

	// An incompatible verifier records rejected and zero evidence.
	incompatibleTarget := env.createDatabase("f3_verify_bad")
	binding := env.bindVerifyTarget(incompatibleTarget)
	vres, err := recovery.ExecuteVerifyBackup(env.ctx, recovery.VerifyBackupOptions{
		ManifestPath: backup.ManifestPath, Binding: binding, TargetDSN: incompatibleTarget,
		Verifier: "auth:verifier", InstanceID: env.instanceID,
		ControlStore: env.store, ControlDSN: env.ctrlDSN,
		AuthoritativeDSN: env.dataDSN, ObserverDSN: env.adminDSN,
		ProgramVersion: "000.1", OperationID: env.operation("f3-verify-bad"),
		PG: env.pg,
	})
	if err != nil {
		t.Fatalf("ExecuteVerifyBackup(incompatible): %v", err)
	}
	if vres.State != recovery.VerificationRejected {
		t.Fatalf("incompatible verify state = %s, want rejected", vres.State)
	}
	if got := env.controlEvidenceCount("backup_manifest", backup.Manifest.BackupID); got != 0 {
		t.Fatalf("incompatible verification recorded %d evidence rows, want 0", got)
	}

	// Verified evidence requires a successful full verifier run, which in turn
	// requires the direct local pg_restore ELF. Keep that portion isolated; the
	// incompatible-verification refusal above remains runnable without it.
	t.Run("incompatible-restore", func(t *testing.T) {
		requireDrillLocalPGRestore(t)
		verifyTarget := env.createDatabase("f3_verify_ok")
		env.verifyBackup(backup.ManifestPath, verifyTarget, env.operation("f3-verify-ok"))
		target := env.createDatabase("f3_target")
		blocked, _ := drillRefusedRestoreWithObserver(env, backup.ManifestPath, target, "000.1", env.operation("f3-restore-bad"))
		drillAssertBlockedMentions(t, blocked, "program")
		if got := env.userTableCount(target); got != 0 {
			t.Fatalf("incompatible restore left %d user tables in the target", got)
		}
		if got := env.evidenceCount("restore_probe"); got != 0 {
			t.Fatalf("incompatible restore wrote %d restore_probe rows, want 0", got)
		}
		// Re-entrant: the same incompatible attempt refuses identically.
		drillRefusedRestoreWithObserver(env, backup.ManifestPath, target, "000.1", env.operation("f3-restore-bad-again"))
		if got := env.controlEvidenceCount("restore_probe", backup.Manifest.BackupID); got != 0 {
			t.Fatalf("restore_probe rows = %d, want 0 from incompatible restore refusals", got)
		}
	})
	drillAssertFingerprintEqual(t, liveBefore, drillFingerprint(t, env.data), "live database during F3")
}

// TestT059F4ExternalLeadZeroReplayZeroAuthorityWrites covers F4: the chain and
// the live effects lead the recovery point; verification reports the lead (not
// consistency), establishes the gap, and neither re-pays, re-broadcasts,
// re-publishes nor re-consumes anything; the authoritative tables are
// byte-identical across verification.
func TestT059F4ExternalLeadZeroReplayZeroAuthorityWrites(t *testing.T) {
	requireDrillLocalPGRestore(t)
	env := newDrillEnv(t, true)
	env.seedLiveBusinessState()
	backup := env.backup()
	advance := env.advanceExternally()
	liveBefore := drillFingerprint(t, env.data)

	verifyTarget := env.createDatabase("f4_verify")
	env.verifyBackup(backup.ManifestPath, verifyTarget, env.operation("f4-verify-backup"))
	env.seedAuthoritativeTargetCleanBaseline()
	recoveredDSN := env.recoveryTarget()
	recovered := env.openPool(recoveredDSN)
	env.restore(backup.ManifestPath, recoveredDSN, env.operation("f4-restore"))
	recoveredBefore := drillFingerprint(t, recovered)

	batch := env.verify(recovered, env.operation("f4-verify"))
	drillAssertV1NotConsistent(t, batch)
	if !drillHasUnknownItem(batch, recovery.VerificationV2) {
		t.Fatal("V2 must be unknown when the recovery point has a request with no provable payment intent")
	}
	drillRequireGapFor(t, env.openGaps(), recovery.CapabilityExistingWithdrawalRecovery)

	// Zero authority writes: both databases byte-identical across verification.
	drillAssertFingerprintEqual(t, recoveredBefore, drillFingerprint(t, recovered), "restored target across F4 verification")
	drillAssertFingerprintEqual(t, liveBefore, drillFingerprint(t, env.data), "live database across F4 verification")

	// Zero replay/broadcast/consumption: the historical signed/broadcast and
	// consumption facts are exactly as the live instance left them, and the
	// restored target carries none of them.
	if got := env.countWhere(recovered, `SELECT count(*) FROM signature_results`); got != 0 {
		t.Fatalf("the restored target carries %d signature results", got)
	}
	if got := env.countWhere(env.data, `SELECT count(*) FROM signature_results`); got != 1 {
		t.Fatalf("the live signature result count changed to %d, want 1", got)
	}
	if got := env.countWhere(env.data, `SELECT count(*) FROM tx_send_attempts`); got != 0 {
		t.Fatalf("a broadcast attempt appeared during the drill (count=%d)", got)
	}
	drillAssertPendingEventState(t, env.data, 6)
	drillAssertPendingEventState(t, recovered, 5)

	// The affected capability stays closed; the refusal is derived (isolation
	// not even attempted here, so it is a dependency/isolation refusal — never
	// an allow).
	gate := env.gate()
	if d := env.admit(gate, recovery.CapabilityExistingWithdrawalRecovery); d.Allowed {
		t.Fatal("the affected capability was admitted with an open gap and unproven isolation")
	}

	// Re-entrant: a second bounded verification step converges; the blocking
	// gap set does not grow into duplicates.
	gapsBefore := len(env.openGaps())
	env.verify(recovered, env.operation("f4-verify-again"))
	if got := len(env.openGaps()); got != gapsBefore {
		t.Fatalf("a repeated verification duplicated gaps: %d -> %d", gapsBefore, got)
	}

	// The real external payment is a recorded fact, not a fabricated claim.
	if advance.TxHash == "" || advance.BlockNumber == 0 {
		t.Fatal("the external payment fact is incomplete")
	}
}

// TestT059F5IsolationUnprovenRefusedAndNotInferred covers F5: an old instance
// whose stoppage is not provable keeps every capability closed; evidence
// without a non-executor verdict never becomes proof, repeated admissions
// change nothing, and a later admitted action bars a fresh
// no_pre_release_effects collection.
func TestT059F5IsolationUnprovenRefusedAndNotInferred(t *testing.T) {
	env := newDrillEnv(t, false)
	env.seedLiveBusinessState()
	env.seedAuthoritativeTargetCleanBaseline()
	liveBefore := drillFingerprint(t, env.data)
	gate := env.gate()

	// No isolation evidence at all: chain scan (the dependency root) refuses.
	first := env.admit(gate, recovery.CapabilityChainScan)
	if first.Allowed {
		t.Fatal("chain_scan was admitted without any isolation evidence")
	}
	if first.RefusalClass != recovery.RefusalIsolationUnproven {
		t.Fatalf("refusal class = %s (%s), want isolation_unproven", first.RefusalClass, first.Reason)
	}

	// Evidence alone is not a verdict: a set item without the non-executor
	// confirmation still refuses, and repeated admissions never flip it.
	env.checklistSetOnly(recovery.CapabilityChainScan, recovery.IsolationItemOldWritersStopped)
	for i := 0; i < 10; i++ {
		if d := env.admit(gate, recovery.CapabilityChainScan); d.Allowed {
			t.Fatalf("admission #%d was allowed on evidenced-only isolation", i+1)
		}
	}
	checklist, err := recovery.NewChecklist(env.store)
	if err != nil {
		t.Fatalf("NewChecklist: %v", err)
	}
	item, found, err := checklist.Item(env.ctx, env.instanceID, recovery.IsolationItemOldWritersStopped)
	if err != nil || !found {
		t.Fatalf("read checklist item: found=%t err=%v", found, err)
	}
	if item.State != recovery.ChecklistStateEvidenced {
		t.Fatalf("item state = %s, want evidenced (unchanged by repeated refusals)", item.State)
	}

	// The executor can never confirm its own isolation evidence.
	if _, err := checklist.Verify(env.ctx, recovery.ChecklistVerifyRequest{
		InstanceID: env.instanceID, ItemKey: recovery.IsolationItemOldWritersStopped,
		Actor: "deploy:executor", OperationID: env.operation("f5-self-verify"),
	}); !errors.Is(err, recovery.ErrChecklistSelfVerification) {
		t.Fatalf("self-verification error = %v, want ErrChecklistSelfVerification", err)
	}

	// The refusals are audited (every admission writes a gate row).
	if got := env.auditCount(recovery.GateAuditAction, "refused"); got == 0 {
		t.Fatal("isolation refusals were not audited")
	}

	// When isolation is provable, the capabilities can be released and an
	// effectful action admitted; from then on no_pre_release_effects is no
	// longer collectable (a real pre-release effect exists — the old instance
	// demonstrably acted, so its stoppage could not have been proven in time).
	env.checklistVerified(recovery.CapabilityChainScan)
	env.checklistVerified(recovery.CapabilityExistingWithdrawalRecovery)
	env.approve(recovery.CapabilityChainScan, "auth:approver")
	env.release(gate, recovery.CapabilityChainScan)
	if d := env.admit(gate, recovery.CapabilityChainScan); !d.Allowed {
		t.Fatalf("chain_scan refused after proven isolation: %s (%s)", d.RefusalClass, d.Reason)
	}
	env.approve(recovery.CapabilityExistingWithdrawalRecovery, "auth:approver")
	env.approve(recovery.CapabilityExistingWithdrawalRecovery, "auth:approver2")
	env.release(gate, recovery.CapabilityExistingWithdrawalRecovery)
	if d := env.admit(gate, recovery.CapabilityExistingWithdrawalRecovery); !d.Allowed {
		t.Fatalf("existing_withdrawal_recovery refused after proven isolation: %s (%s)", d.RefusalClass, d.Reason)
	}
	if _, err := checklist.Set(env.ctx, recovery.ChecklistEvidenceRequest{
		InstanceID: env.instanceID, ItemKey: recovery.IsolationItemNoPreReleaseEffects,
		State: recovery.ChecklistStateEvidenced, EvidenceRef: "drill:f5/no-pre-release",
		Actor: "deploy:executor", OperationID: env.operation("f5-npe-set"),
	}); !errors.Is(err, recovery.ErrChecklistTransition) {
		t.Fatalf("no_pre_release_effects collection error = %v, want a transition refusal after an admitted action", err)
	}

	drillAssertFingerprintEqual(t, liveBefore, drillFingerprint(t, env.data), "live database during F5")
}

// TestT059F6UnprovableGapStaysUnknownWithPackageAndEscalation covers F6: a
// gap that cannot be proven stays unknown/pending with its full handling
// package, responsibility and escalation; closure needs new evidence and
// timeout/exhaustion/acknowledgement never close it; no intent or idempotency
// row is ever fabricated.
func TestT059F6UnprovableGapStaysUnknownWithPackageAndEscalation(t *testing.T) {
	requireDrillLocalPGRestore(t)
	env := newDrillEnv(t, true)
	env.seedLiveBusinessState()
	backup := env.backup()
	env.advanceExternally()

	verifyTarget := env.createDatabase("f6_verify")
	env.verifyBackup(backup.ManifestPath, verifyTarget, env.operation("f6-verify-backup"))
	env.seedAuthoritativeTargetCleanBaseline()
	recoveredDSN := env.recoveryTarget()
	recovered := env.openPool(recoveredDSN)
	env.restore(backup.ManifestPath, recoveredDSN, env.operation("f6-restore"))
	liveBefore := drillFingerprint(t, env.data)
	recoveredBefore := drillFingerprint(t, recovered)

	batch := env.verify(recovered, env.operation("f6-verify"))
	if !drillHasUnknownItem(batch, recovery.VerificationV2) {
		t.Fatal("V2 must stay unknown (a missing record is not 'never happened')")
	}
	gap := drillGapFor(t, env.openGaps(), recovery.CapabilityExistingWithdrawalRecovery)
	if gap.ObjectKey == "" || len(gap.Scope) == 0 || len(gap.Timeline) == 0 ||
		len(gap.ExistingEvidence) == 0 || len(gap.RequiredEvidence) == 0 || len(gap.Risk) == 0 {
		t.Fatalf("the unprovable gap is not a complete handling package: %+v", gap)
	}
	if gap.Owner != "" {
		// The verification writer does not assign responsibility; the operator
		// follow-up below does, and the drill records that as the requirement.
		t.Logf("note: verification gap carries owner=%q; responsibility follows via the operator package", gap.Owner)
	}

	gaps, err := recovery.NewGaps(env.store)
	if err != nil {
		t.Fatalf("NewGaps: %v", err)
	}

	// Responsibility + escalation through the real entry points.
	owned, err := gaps.Open(env.ctx, recovery.OpenGapRequest{
		InstanceID:       env.instanceID,
		ObjectKey:        "payment_intent:f6-drill-req",
		Scope:            []byte(`{"chain_id":31337,"object":"payment_intent:f6-drill-req"}`),
		Timeline:         []byte(`{"from":"recovery point","state":"unknown"}`),
		ExistingEvidence: []byte(`{"note":"the restored recovery point has no provable payment intent"}`),
		RequiredEvidence: []byte(`{"external":["the signed payment intent and broadcast receipt"]}`),
		Risk:             []byte(`{"unproven_external_effect":true}`),
		AffectedCapabilities: []recovery.Capability{
			recovery.CapabilityExistingWithdrawalRecovery,
		},
		Owner: "person-owner-1", Actor: "deploy:executor", OperationID: env.operation("f6-open"),
	})
	if err != nil {
		t.Fatalf("Gaps.Open: %v", err)
	}
	escalated, err := gaps.Escalate(env.ctx, recovery.EscalateGapRequest{
		InstanceID: env.instanceID, GapID: owned.GapID,
		EscalationRef: "drill://escalation/f6", Actor: "deploy:executor",
		Reason:      "the external evidence cannot be produced in this drill",
		OperationID: env.operation("f6-escalate"),
	})
	if err != nil {
		t.Fatalf("Gaps.Escalate: %v", err)
	}
	if escalated.State != recovery.GapStateEscalated || escalated.Owner != "person-owner-1" || escalated.EscalationRef == "" {
		t.Fatalf("escalated gap = %+v, want state=escalated owner=person-owner-1 with the reference", escalated)
	}

	// Closure needs new evidence; the audit-only notes change nothing.
	if _, err := gaps.Close(env.ctx, recovery.CloseGapRequest{
		InstanceID: env.instanceID, GapID: gap.GapID, Actor: "deploy:executor",
		OperationID: env.operation("f6-close-empty"),
	}); !errors.Is(err, recovery.ErrGapClosureEvidence) {
		t.Fatalf("empty closure error = %v, want ErrGapClosureEvidence", err)
	}
	for _, kind := range []recovery.GapNoteKind{
		recovery.GapNoteTimeout, recovery.GapNoteAttemptsExhausted, recovery.GapNoteAcknowledged,
	} {
		if _, err := gaps.Note(env.ctx, recovery.GapNoteRequest{
			InstanceID: env.instanceID, GapID: gap.GapID, Kind: kind,
			Reason: "f6 drill: evidence unavailable", Actor: "deploy:executor",
			OperationID: env.operation("f6-note"),
		}); err != nil {
			t.Fatalf("Gaps.Note(%s): %v", kind, err)
		}
	}
	if got := drillGapState(t, env, gap.GapID); got != recovery.GapStateOpen {
		t.Fatalf("gap state after notes = %s, want open", got)
	}

	// The affected capability stays paused; the refusal traces to the gap.
	gate := env.gate()
	env.checklistVerified(recovery.CapabilityChainScan)
	env.checklistVerified(recovery.CapabilityExistingWithdrawalRecovery)
	env.approve(recovery.CapabilityChainScan, "auth:approver")
	env.release(gate, recovery.CapabilityChainScan)
	env.approve(recovery.CapabilityExistingWithdrawalRecovery, "auth:approver")
	env.approve(recovery.CapabilityExistingWithdrawalRecovery, "auth:approver2")
	env.release(gate, recovery.CapabilityExistingWithdrawalRecovery)
	decision := env.admit(gate, recovery.CapabilityExistingWithdrawalRecovery)
	if decision.Allowed || !strings.Contains(decision.Reason, string(recovery.RefusalGapOpen)) {
		t.Fatalf("affected capability decision = %+v, want a refusal tracing to gap_open", decision)
	}

	// No fabricated intent / idempotency row: authority tables unchanged in
	// both environments through the whole F6 sequence.
	drillAssertFingerprintEqual(t, liveBefore, drillFingerprint(t, env.data), "live database during F6")
	drillAssertFingerprintEqual(t, recoveredBefore, drillFingerprint(t, recovered), "restored target during F6")
	if got := env.countWhere(recovered, `SELECT count(*) FROM payment_intents`); got != 0 {
		t.Fatalf("the recovery flow fabricated %d payment intents", got)
	}
	if got := env.countWhere(recovered, `SELECT count(*) FROM consumer_inbox`); got != 0 {
		t.Fatalf("the recovery flow fabricated %d consumer idempotency rows", got)
	}
}

// TestT059F7UnauthorizedInsufficientStaleApprovalsRefused covers F7: executor
// self-approval, a second account of the same person, missing approvals,
// same-person dual approvals, stale generations and identity-mapping changes
// are all refused with zero releases and zero admissions.
func TestT059F7UnauthorizedInsufficientStaleApprovalsRefused(t *testing.T) {
	env := newDrillEnv(t, false)
	env.seedAuthoritativeTargetCleanBaseline()
	liveBefore := drillFingerprint(t, env.data)
	gate := env.gate()

	// (a) The executor is even registered as an approver: still excluded.
	env.register("deploy:executor", "approver")
	out, err := env.tryApprove(recovery.CapabilityQuery, "deploy:executor", env.operation("f7-self"))
	if err == nil || out.RefusalClass != recovery.RefusalApprovalExecutorExcluded {
		t.Fatalf("executor self-approval = %+v err=%v, want approval_executor_excluded", out, err)
	}

	// (b) A second account mapped to the executor's person is excluded too.
	env.mapIdentity("auth:imposter", "person-executor")
	env.register("auth:imposter", "approver")
	out, err = env.tryApprove(recovery.CapabilityQuery, "auth:imposter", env.operation("f7-imposter"))
	if err == nil || out.RefusalClass != recovery.RefusalApprovalExecutorExcluded {
		t.Fatalf("same-person approval = %+v err=%v, want approval_executor_excluded", out, err)
	}

	// (c) A missing approval basis refuses the release.
	rel, err := env.tryRelease(gate, recovery.CapabilityChainScan, env.operation("f7-no-approval"))
	if err == nil || rel.RefusalClass != recovery.RefusalApprovalMissing {
		t.Fatalf("release without approvals = %+v err=%v, want approval_missing", rel, err)
	}

	// (d) Same person under two principals never satisfies the conservative
	// dual class.
	env.remapIdentity("auth:approver2", "person-approver")
	dual := recovery.CapabilityExistingWithdrawalRecovery
	env.approve(dual, "auth:approver")
	out, err = env.tryApprove(dual, "auth:approver2", env.operation("f7-dual-person-2"))
	if err != nil {
		t.Fatalf("the second same-person approval should be recorded (the distinctness rule is judged at release): %v", err)
	}
	rel, err = env.tryRelease(gate, dual, env.operation("f7-dual-release"))
	if err == nil || rel.RefusalClass != recovery.RefusalApprovalIdentityUnverified {
		t.Fatalf("same-person dual release = %+v err=%v, want approval_identity_unverified", rel, err)
	}

	// (e) A new evidence generation makes the approvals stale; the release
	// refuses until fresh approvals are recorded at the new generation.
	env.remapIdentity("auth:approver2", "person-approver-2")
	env.approve(dual, "auth:approver")
	env.approve(dual, "auth:approver2")
	env.advanceEvidence("f7")
	rel, err = env.tryRelease(gate, dual, env.operation("f7-stale-release"))
	if err == nil || rel.RefusalClass != recovery.RefusalApprovalStale {
		t.Fatalf("stale release = %+v err=%v, want approval_stale", rel, err)
	}
	env.approve(dual, "auth:approver")
	env.approve(dual, "auth:approver2")
	staleRelease, err := env.tryRelease(gate, dual, env.operation("f7-stale-release-2"))
	if err != nil || staleRelease.ReleaseID == "" {
		t.Fatalf("fresh release after re-approval = %+v err=%v, want a recorded decision", staleRelease, err)
	}

	// The release stream itself is generation-bound: after any evidence write
	// the recorded release stops being usable (release_invalidated_generation)
	// even though its approvals were valid when recorded. query has no
	// dependency, so this is the capability's own check.
	env.checklistVerified(recovery.CapabilityQuery)
	env.approve(recovery.CapabilityQuery, "auth:approver")
	queryRelease, err := env.tryRelease(gate, recovery.CapabilityQuery, env.operation("f7-query-release"))
	if err != nil || queryRelease.ReleaseID == "" {
		t.Fatalf("query release = %+v err=%v, want a recorded decision", queryRelease, err)
	}
	env.advanceEvidence("f7-again")
	if d := env.admit(gate, recovery.CapabilityQuery); d.Allowed || d.RefusalClass != recovery.RefusalReleaseInvalidatedGeneration {
		t.Fatalf("admission after an evidence change = %+v, want release_invalidated_generation", d)
	}

	// (f) An identity mapping change invalidates the recorded approvals.
	env.approve(recovery.CapabilityQuery, "auth:approver")
	change := env.remapIdentity("auth:approver", "person-approver-renamed")
	if change.Mapping.PersonID != "person-approver-renamed" || len(change.InvalidatedApprovals) == 0 {
		t.Fatalf("identity mapping change = %+v, want the new person and the invalidated approval record (F19)", change)
	}
	rel, err = env.tryRelease(gate, recovery.CapabilityQuery, env.operation("f7-mapping-release"))
	if err == nil || rel.RefusalClass != recovery.RefusalApprovalIdentityUnverified {
		t.Fatalf("release after a mapping change = %+v err=%v, want approval_identity_unverified", rel, err)
	}

	// Zero wrong opens; every accepted release row is one of the deliberately
	// valid ones, every refused attempt left zero rows.
	for _, capability := range []recovery.Capability{
		recovery.CapabilityQuery, recovery.CapabilityChainScan, dual,
	} {
		if d := env.admit(gate, capability); d.Allowed {
			t.Fatalf("capability %s was admitted during the F7 refusal matrix", capability)
		}
	}
	if got := env.decisionRowCount("recovery_release", recovery.CapabilityQuery, ""); got != 1 {
		t.Fatalf("query release rows = %d, want exactly the one valid release (all refused attempts wrote zero)", got)
	}
	if got := env.decisionRowCount("recovery_release", recovery.CapabilityChainScan, ""); got != 0 {
		t.Fatalf("chain_scan release rows = %d, want 0", got)
	}
	if got := env.decisionRowCount("recovery_release", dual, ""); got != 1 {
		t.Fatalf("dual release rows = %d, want exactly the one valid release", got)
	}

	// Re-entrant: repeating the self-approval refuses identically with no new
	// decision row.
	before := env.decisionRowCount("recovery_approval", recovery.CapabilityQuery, "deploy:executor")
	out, err = env.tryApprove(recovery.CapabilityQuery, "deploy:executor", env.operation("f7-self-again"))
	if err == nil || out.RefusalClass != recovery.RefusalApprovalExecutorExcluded {
		t.Fatalf("repeated self-approval = %+v err=%v, want approval_executor_excluded", out, err)
	}
	if after := env.decisionRowCount("recovery_approval", recovery.CapabilityQuery, "deploy:executor"); after != before {
		t.Fatalf("self-approval rows changed %d -> %d", before, after)
	}
	drillAssertFingerprintEqual(t, liveBefore, drillFingerprint(t, env.data), "live database during F7")
}

// ---------------------------------------------------------------------------
// F-matrix assertions and helpers.
// ---------------------------------------------------------------------------

func drillAssertBlockedMentions(t *testing.T, result recovery.RestoreResult, fragment string) {
	t.Helper()
	joined := strings.Join(result.Blocked, "; ")
	if !strings.Contains(joined, fragment) {
		t.Fatalf("blocked preconditions %q do not mention %q", joined, fragment)
	}
}

// drillRefusedRestoreWithObserver supplies the explicit observer required by
// the restore boundary while still exercising the real fail-closed entrypoint.
func drillRefusedRestoreWithObserver(env *drillEnv, manifestPath, targetDSN, programVersion, operationID string) (recovery.RestoreResult, error) {
	env.t.Helper()
	result, err := recovery.ExecuteRestore(env.ctx, recovery.RestoreOptions{
		ManifestPath: manifestPath, InstanceID: env.instanceID,
		ControlStore: env.store, ControlDSN: env.ctrlDSN,
		ObserverDSN: env.observerDSNFor(targetDSN), TargetDSN: targetDSN,
		TargetDeclaration: recovery.TargetIsolated,
		Actor:             "deploy:executor", ProgramVersion: programVersion,
		OperationID: operationID, PG: env.pg,
	})
	if err == nil {
		env.t.Fatal("ExecuteRestore succeeded, want a fail-closed refusal")
	}
	if result.Restored || len(result.Blocked) == 0 {
		env.t.Fatalf("restore refusal is not explicit: %+v", result)
	}
	return result, err
}

func drillHasUnknownItem(batch recovery.VerificationBatch, category recovery.VerificationCategory) bool {
	for _, item := range batch.Items {
		if item.Category == category && item.Conclusion == recovery.ConclusionUnknown {
			return true
		}
	}
	return false
}

func drillEqualInt64(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// probeRowCount counts rows of one table (a read-only probe).
func (e *drillEnv) probeRowCount(dsn, table string) int {
	e.t.Helper()
	conn, err := pgx.Connect(e.ctx, dsn)
	if err != nil {
		e.t.Fatalf("connect %s: %v", dsn, err)
	}
	defer func() { _ = conn.Close(e.ctx) }()
	var n int
	if err := conn.QueryRow(e.ctx, fmt.Sprintf("SELECT count(*) FROM %s", table)).Scan(&n); err != nil {
		e.t.Fatalf("count %s: %v", table, err)
	}
	return n
}
