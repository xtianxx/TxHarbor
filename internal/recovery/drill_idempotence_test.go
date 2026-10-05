//go:build drill

// drill_idempotence_test.go is T060 [US6]: the drill-layer idempotence and
// re-entrancy proof (FR-024/SC-007). verify/approve/release/close are repeated
// at least ten times through their real entry points with zero external side
// effects and zero state flips; revocation is explicit, authorized and
// audited; released and unreleased capabilities stay separately observable.
//
// Restore has one honest meaning here: a real restore of the same recovery
// point is a new authoritative act by design (it advances the evidence
// generation and invalidates earlier authorizations before its first target
// write). Its repetition is therefore exercised as re-entry after interruption
// — ten interrupted attempts never reach the restored state, advance exactly
// by their committed start markers, invalidate the earlier release from the
// first attempt on, and a final rebuild + rerun converges to exactly one
// restored dataset (one probe row, one goose set, no duplicates).
package recovery_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xtianxx/txharbor/internal/recovery"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

// TestT060DrillRepeatedVerifyApproveReleaseCloseNoFlips repeats every
// decision command ten times with the same operation id (plus a real
// mid-sequence revocation) and asserts one row, one verdict, zero flips.
func TestT060DrillRepeatedVerifyApproveReleaseCloseNoFlips(t *testing.T) {
	requireDrillLocalPGRestore(t)
	env := newDrillEnv(t, true)
	env.seedLiveBusinessState()
	backup := env.backup()
	env.advanceExternally()
	verifyTarget := env.createDatabase("idem_verify")
	env.verifyBackup(backup.ManifestPath, verifyTarget, env.operation("idem-verify-backup"))
	env.seedAuthoritativeTargetCleanBaseline()
	recoveredDSN := env.recoveryTarget()
	recovered := env.openPool(recoveredDSN)
	env.restore(backup.ManifestPath, recoveredDSN, env.operation("idem-restore"))

	verification, err := recovery.NewVerification(env.store, recovery.VerificationOptions{
		Tolerance: time.Hour, BatchLimit: 1000,
	})
	if err != nil {
		t.Fatalf("NewVerification: %v", err)
	}
	verifyRequest := recovery.VerificationRequest{
		InstanceID:  env.instanceID,
		Actor:       "deploy:executor",
		Scope:       []byte(`{"purpose":"idempotence drill (local test input only)"}`),
		OperationID: "drill-idem-verify",
		Sources:     env.sourcesFor(recovered),
	}
	first, err := verification.Verify(env.ctx, verifyRequest)
	if err != nil {
		t.Fatalf("first Verify: %v", err)
	}
	itemCount := env.countWhere(env.ctrl, `SELECT count(*) FROM recovery_verification_item WHERE instance_id = '`+env.instanceID+`'`)
	generation := env.evidenceGeneration()
	for i := 0; i < 10; i++ {
		replay, err := verification.Verify(env.ctx, verifyRequest)
		if err != nil {
			t.Fatalf("Verify replay #%d: %v", i+1, err)
		}
		if replay.BatchID != first.BatchID || replay.Discarded {
			t.Fatalf("Verify replay #%d = %+v, want the recorded batch %s", i+1, replay, first.BatchID)
		}
		if got := env.countWhere(env.ctrl, `SELECT count(*) FROM recovery_verification_item WHERE instance_id = '`+env.instanceID+`'`); got != itemCount {
			t.Fatalf("Verify replay #%d appended items: %d -> %d", i+1, itemCount, got)
		}
	}
	if got := env.evidenceGeneration(); got != generation {
		t.Fatalf("verification replays advanced the generation: %d -> %d", generation, got)
	}

	// Isolation is written once; approvals and releases bind the current
	// generation. query carries no dependency and is affected by no
	// verification gap, so its released/unreleased states are separately
	// observable across the decision sequence below.
	env.checklistVerified(recovery.CapabilityQuery)
	bindGeneration := env.evidenceGeneration()

	gate := env.gate()
	if d := env.admit(gate, recovery.CapabilityQuery); d.Allowed || d.RefusalClass != recovery.RefusalNoRelease {
		t.Fatalf("query before its release = %+v, want no_release", d)
	}

	approvalID := ""
	for i := 0; i < 11; i++ {
		outcome, err := env.tryApprove(recovery.CapabilityQuery, "auth:approver", "drill-idem-approve")
		if err != nil {
			t.Fatalf("Approve #%d: %v", i+1, err)
		}
		if i == 0 && outcome.Recorded {
			t.Fatal("the first approve must insert, not replay")
		}
		if i > 0 && (!outcome.Recorded || outcome.ApprovalID != approvalID) {
			t.Fatalf("Approve replay #%d = %+v, want the recorded %s with Recorded=true", i+1, outcome, approvalID)
		}
		approvalID = outcome.ApprovalID
	}
	if got := env.decisionRowCount("recovery_approval", recovery.CapabilityQuery, "auth:approver"); got != 1 {
		t.Fatalf("approval rows = %d, want 1", got)
	}

	releaseID := ""
	for i := 0; i < 11; i++ {
		outcome, err := env.tryRelease(gate, recovery.CapabilityQuery, "drill-idem-release")
		if err != nil {
			t.Fatalf("Release #%d: %v", i+1, err)
		}
		if i == 0 && outcome.Recorded {
			t.Fatal("the first release must insert, not replay")
		}
		if i > 0 && (!outcome.Recorded || outcome.ReleaseID != releaseID) {
			t.Fatalf("Release replay #%d = %+v, want the recorded %s", i+1, outcome, releaseID)
		}
		releaseID = outcome.ReleaseID
	}
	if got := env.decisionRowCount("recovery_release", recovery.CapabilityQuery, ""); got != 1 {
		t.Fatalf("release rows = %d, want 1", got)
	}
	if got := env.evidenceGeneration(); got != bindGeneration {
		t.Fatalf("decision replays advanced the generation: %d -> %d", bindGeneration, got)
	}

	if d := env.admit(gate, recovery.CapabilityQuery); !d.Allowed {
		t.Fatalf("query not admitted after the release: %s (%s)", d.RefusalClass, d.Reason)
	}

	// Explicit, authorized revocation: only the executor may revoke; the
	// revocation is audited and flips exactly the revoked stream.
	if _, err := env.tryRevokeRelease(gate, recovery.CapabilityQuery, "auth:approver", "drill-idem-revoke-unauthorized"); err == nil {
		t.Fatal("a non-executor release revocation was accepted")
	}
	if got := env.decisionRowCount("recovery_release", recovery.CapabilityQuery, ""); got != 1 {
		t.Fatalf("an unauthorized revocation appended a release row (count=%d)", got)
	}
	revoked, err := env.tryRevokeRelease(gate, recovery.CapabilityQuery, "deploy:executor", "drill-idem-revoke")
	if err != nil || revoked.ReleaseID == "" {
		t.Fatalf("executor revocation = %+v err=%v, want an appended revoke", revoked, err)
	}
	if got := env.auditCount(controlstore.ActionReleaseDecision, "ok"); got == 0 {
		t.Fatal("the revocation was not audited")
	}
	if d := env.admit(gate, recovery.CapabilityQuery); d.Allowed || d.RefusalClass != recovery.RefusalReleaseRevoked {
		t.Fatalf("query decision after revocation = %+v, want release_revoked", d)
	}
	// Replaying the revocation changes nothing.
	revokedID := revoked.ReleaseID
	for i := 0; i < 10; i++ {
		replay, err := env.tryRevokeRelease(gate, recovery.CapabilityQuery, "deploy:executor", "drill-idem-revoke")
		if err != nil || !replay.Recorded || replay.ReleaseID != revokedID {
			t.Fatalf("revocation replay #%d = %+v err=%v, want recorded %s", i+1, replay, err, revokedID)
		}
	}
	if got := env.decisionRowCount("recovery_release", recovery.CapabilityQuery, ""); got != 2 {
		t.Fatalf("release rows after revoke replays = %d, want 2 (release + revoke)", got)
	}

	// A re-release is explicit and requires a fresh decision (never an
	// automatic flip back).
	reReleased, err := env.tryRelease(gate, recovery.CapabilityQuery, "drill-idem-release-2")
	if err != nil || reReleased.ReleaseID == "" {
		t.Fatalf("explicit re-release = %+v err=%v", reReleased, err)
	}
	if d := env.admit(gate, recovery.CapabilityQuery); !d.Allowed {
		t.Fatalf("query not admitted after the explicit re-release: %s (%s)", d.RefusalClass, d.Reason)
	}

	// close: one closure under a stable operation id, then a repeated close
	// attempt (new operation id) refuses without reopening anything.
	gaps, err := recovery.NewGaps(env.store)
	if err != nil {
		t.Fatalf("NewGaps: %v", err)
	}
	gap := env.advanceEvidence("idem-close")
	closure := []byte(`{"external":"drill idempotence: closure evidence (local test input)"}`)
	var closed recovery.Gap
	for i := 0; i < 11; i++ {
		result, err := gaps.Close(env.ctx, recovery.CloseGapRequest{
			InstanceID: env.instanceID, GapID: gap.GapID,
			ClosureEvidence: closure, Actor: "auth:verifier",
			OperationID: "drill-idem-close",
		})
		if err != nil {
			t.Fatalf("Close #%d: %v", i+1, err)
		}
		if i == 0 {
			if result.State != recovery.GapStateClosed {
				t.Fatalf("first close state = %s, want closed", result.State)
			}
		} else if result.State != recovery.GapStateClosed || result.ClosedAt.IsZero() {
			t.Fatalf("Close replay #%d = %+v, want the recorded closed gap", i+1, result)
		}
		closed = result
	}
	if got := drillGapState(t, env, gap.GapID); got != recovery.GapStateClosed {
		t.Fatalf("gap state = %s, want closed", got)
	}
	if _, err := gaps.Close(env.ctx, recovery.CloseGapRequest{
		InstanceID: env.instanceID, GapID: gap.GapID,
		ClosureEvidence: closure, Actor: "auth:verifier",
		OperationID: "drill-idem-close-again",
	}); !errors.Is(err, recovery.ErrGapTransition) {
		t.Fatalf("closing a closed gap error = %v, want ErrGapTransition (no reopen)", err)
	}
	if got := drillGapState(t, env, gap.GapID); got != recovery.GapStateClosed {
		t.Fatalf("gap state after the repeated close = %s, want closed", got)
	}
	if closed.ClosedBy != "auth:verifier" {
		t.Fatalf("closure actor = %q, want the authenticated verifier", closed.ClosedBy)
	}
}

// TestT060DrillRestoreReentryConvergesWithSingleState supervises one real
// interrupted restore and proves ordinary retries remain blocked. It fails
// closed at the rebuild boundary because the original attempt's authenticated
// writer identity/process handle is not retained for local witness proof.
func TestT060DrillRestoreReentryConvergesWithSingleState(t *testing.T) {
	requireDrillLocalPGRestore(t)
	env := newDrillEnv(t, false)
	env.seedLiveBusinessState()
	env.seedProbeTable()

	backup := env.backup()
	verifyTarget := env.createDatabase("reentry_verify")
	env.verifyBackup(backup.ManifestPath, verifyTarget, env.operation("reentry-verify-backup"))
	env.seedAuthoritativeTargetCleanBaseline()
	target := env.recoveryTarget()
	targetDB := env.dbNameOf(target)

	// One valid release exists before the restore attempts; the first committed
	// restore start marker must invalidate it and no re-entry may revive it.
	env.checklistVerified(recovery.CapabilityChainScan)
	env.approve(recovery.CapabilityChainScan, "auth:approver")
	env.release(env.gate(), recovery.CapabilityChainScan)
	if d := env.admit(env.gate(), recovery.CapabilityChainScan); !d.Allowed {
		t.Fatalf("chain_scan not admitted before the restore attempts: %s (%s)", d.RefusalClass, d.Reason)
	}

	locker, err := pgx.Connect(env.ctx, target)
	if err != nil {
		t.Fatalf("connect locker: %v", err)
	}
	defer func() { _ = locker.Close(context.Background()) }()
	if _, err := locker.Exec(env.ctx, `CREATE TABLE `+drillProbeTable+` (dummy text)`); err != nil {
		t.Fatalf("create conflicting table: %v", err)
	}

	if _, err := locker.Exec(env.ctx, `BEGIN`); err != nil {
		t.Fatalf("begin blocker transaction: %v", err)
	}
	if _, err := locker.Exec(env.ctx, `LOCK TABLE `+drillProbeTable+` IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatalf("lock conflicting table: %v", err)
	}
	type restoreOutcome struct {
		result recovery.RestoreResult
		err    error
	}
	interruptedOperation := env.operation("reentry-interrupted")
	restoreCtx, cancelRestore := context.WithCancel(context.Background())
	done := make(chan restoreOutcome, 1)
	go func() {
		result, err := recovery.ExecuteRestore(restoreCtx, recovery.RestoreOptions{
			ManifestPath: backup.ManifestPath, InstanceID: env.instanceID,
			ControlStore: env.store, ControlDSN: env.ctrlDSN,
			ObserverDSN: env.observerDSNFor(target), TargetDSN: target,
			TargetDeclaration: recovery.TargetIsolated, TargetReason: "drill: isolated recovery environment", Actor: "deploy:executor",
			ProgramVersion: drillProgramVersion, OperationID: interruptedOperation, PG: env.pg,
		})
		done <- restoreOutcome{result: result, err: err}
	}()
	if pids := env.waitForLockWaiter(targetDB, 30*time.Second); len(pids) == 0 {
		t.Fatal("interrupted reentry restore never reached the real PostgreSQL lock barrier")
	}
	cancelRestore() // the owned pg_restore command is terminated and Waited by Run
	if _, err := locker.Exec(env.ctx, `ROLLBACK`); err != nil {
		t.Fatalf("release blocker lock: %v", err)
	}
	if err := locker.Close(env.ctx); err != nil {
		t.Fatalf("close lock owner: %v", err)
	}
	var interrupted restoreOutcome
	select {
	case interrupted = <-done:
	case <-time.After(2 * time.Minute):
		t.Fatal("the interrupted restore did not return")
	}
	if interrupted.err == nil || interrupted.result.Restored {
		t.Fatalf("interrupted restore = %+v err=%v, want a refusal", interrupted.result, interrupted.err)
	}
	if got := env.controlEvidenceCount("restore_probe", backup.Manifest.BackupID); got != 0 {
		t.Fatalf("restore_probe rows after interruption = %d, want 0", got)
	}
	if err := drillWaitNoTargetSessions(env.ctx, env.admin, targetDB); err != nil {
		t.Fatalf("old writer did not drain after Wait: %v", err)
	}
	info, err := controlstore.ParseDSNTarget(target)
	if err != nil {
		t.Fatal(err)
	}
	key, err := controlstore.TargetGuardKey(info)
	if err != nil {
		t.Fatal(err)
	}
	guard, found, err := controlstore.ReadTargetGuard(env.ctx, env.ctrl, key)
	if err != nil || !found || guard.OperationID != interruptedOperation || guard.AttemptAppName == "" {
		t.Fatalf("interrupted guard identity = %+v found=%t err=%v", guard, found, err)
	}
	if guard.State != controlstore.TargetGuardRebuildRequired {
		guardTx, err := env.ctrl.Begin(env.ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := controlstore.MarkTargetGuardWriterDrainedForAttempt(env.ctx, guardTx, key, interruptedOperation, guard.AttemptAppName); err != nil {
			_ = guardTx.Rollback(env.ctx)
			t.Fatalf("mark interrupted writer drained (guard=%+v): %v", guard, err)
		}
		if err := controlstore.MarkTargetGuardRebuildRequiredForAttempt(env.ctx, guardTx, key, interruptedOperation, guard.AttemptAppName); err != nil {
			_ = guardTx.Rollback(env.ctx)
			t.Fatal(err)
		}
		if err := guardTx.Commit(env.ctx); err != nil {
			t.Fatal(err)
		}
	} else if guard.ActiveWriter {
		t.Fatalf("restore executor marked rebuild-required while writer is still active: %+v", guard)
	}
	if _, err := env.refusedRestore(backup.ManifestPath, target, drillProgramVersion, env.operation("reentry-blocked-retry")); err == nil {
		t.Fatal("ordinary retry was allowed before target witness recovery")
	}
	if got := env.controlEvidenceCount("restore_probe", backup.Manifest.BackupID); got != 0 {
		t.Fatalf("restore_probe rows after blocked retry = %d, want 0", got)
	}
	// The first committed restore marker invalidates the pre-restore release;
	// no failed re-entry or witness transition revives it.
	if d := env.admit(env.gate(), recovery.CapabilityChainScan); d.Allowed {
		t.Fatal("chain_scan was admitted after interrupted restore")
	}

	// The re-entered target is still not a restored environment: every attempt
	// committed its start marker (the fail-closed invalidation), and none
	// produced restored evidence.
	var markers int
	if err := env.ctrl.QueryRow(env.ctx,
		`SELECT count(*) FROM recovery_audit WHERE instance_id = $1 AND action = $2 AND result = 'ok'`,
		env.instanceID, recovery.ActionRestoreStarted).Scan(&markers); err != nil {
		t.Fatalf("count restore start markers: %v", err)
	}
	if markers != 1 {
		t.Fatalf("restore start markers = %d, want exactly one supervised interrupted attempt", markers)
	}
	if got := env.controlEvidenceCount("restore_probe", backup.Manifest.BackupID); got != 0 {
		t.Fatalf("restore_probe rows after the re-entry loop = %d, want 0", got)
	}

	// This is intentionally a required failure, not a skip or successful
	// convergence claim, until authentic original-attempt proof is available.
	env.rebuildTargetWithWitness(target, env.operation("reentry-witnessed-rebuild"))
}

// evidenceGeneration reads the instance's current evidence generation.
func (e *drillEnv) evidenceGeneration() int64 {
	e.t.Helper()
	var generation int64
	if err := e.ctrl.QueryRow(e.ctx,
		`SELECT evidence_generation FROM recovery_instance WHERE instance_id = $1`,
		e.instanceID).Scan(&generation); err != nil {
		e.t.Fatalf("read evidence generation: %v", err)
	}
	return generation
}

// TestT060DrillInstanceCloseOnlyWhenAllReleasedAndIdempotent covers the
// close command of FR-024: every capability starts closed (S3), a partial
// release set refuses closure, the same close repeated after a successful
// closure never flips the instance back, and the accepted close is audited
// exactly once.
func TestT060DrillInstanceCloseOnlyWhenAllReleasedAndIdempotent(t *testing.T) {
	env := newDrillEnv(t, false)
	env.seedAuthoritativeTargetCleanBaseline()
	liveBefore := drillFingerprint(t, env.data)
	gate := env.gate()
	var targetGuardKey, targetRoleFingerprint string
	if err := env.ctrl.QueryRow(env.ctx,
		`SELECT target_guard_key, target_role_fingerprint FROM recovery_instance WHERE instance_id = $1`, env.instanceID,
	).Scan(&targetGuardKey, &targetRoleFingerprint); err != nil {
		t.Fatalf("read authoritative target binding: %v", err)
	}
	entryScope := func(capability recovery.Capability) string {
		scope, err := recovery.CapabilityScope(1, capability)
		if err != nil {
			t.Fatalf("capability scope %s: %v", capability, err)
		}
		return scope
	}
	approveAtEntry := func(capability recovery.Capability, principal string) {
		t.Helper()
		if _, err := recovery.Approve(env.ctx, env.store, recovery.ApprovalRequest{
			InstanceID: env.instanceID, Capability: capability, ScopeHash: entryScope(capability),
			Principal: principal, Reason: "drill approval (local test input only)", OperationID: env.operation("approve-entry"),
		}); err != nil {
			t.Fatalf("Approve(%s by %s): %v", capability, principal, err)
		}
	}
	releaseAtEntry := func(capability recovery.Capability) {
		t.Helper()
		if _, err := recovery.Release(env.ctx, env.store, gate, recovery.ReleaseRequest{
			InstanceID: env.instanceID, Capability: capability, ScopeHash: entryScope(capability),
			Principal: "deploy:executor", Reason: "drill release (local test input only)", OperationID: env.operation("release-entry"),
		}); err != nil {
			t.Fatalf("Release(%s): %v", capability, err)
		}
	}
	admitAtEntry := func(capability recovery.Capability) recovery.GateDecision {
		t.Helper()
		decision, err := gate.Admit(env.ctx, recovery.GateRequest{
			InstanceID: env.instanceID, Capability: capability, ScopeHash: entryScope(capability),
			Actor: "deploy:executor", OperationID: env.operation("admit-entry"), Action: "drill",
		})
		if err != nil {
			t.Fatalf("gate.Admit(%s): %v", capability, err)
		}
		return decision
	}

	// S3: a fresh recovery instance has every capability default-closed.
	for _, capability := range recovery.KnownCapabilities() {
		if d := env.admit(gate, capability); d.Allowed {
			t.Fatalf("capability %s was admitted on a fresh instance", capability)
		}
	}

	// Isolation for every capability (shared items collected once), then the
	// dependency-ordered releases: single approvals for query/chain_scan/
	// deposit_confirmation, dual approvals for the high-impact capabilities.
	// query is deliberately held back so the partial-set closure refusal (S11)
	// and the separate released/unreleased observability can be exercised.
	for _, capability := range recovery.KnownCapabilities() {
		env.checklistVerified(capability)
	}
	releaseOrder := []recovery.Capability{
		recovery.CapabilityChainScan,
		recovery.CapabilityDepositConfirmation,
		recovery.CapabilityExistingWithdrawalRecovery,
		recovery.CapabilityNewWithdrawalCreation,
		recovery.CapabilityEventPublishing,
		recovery.CapabilityEventConsuming,
		recovery.CapabilityQuery,
	}
	dual := map[recovery.Capability]bool{
		recovery.CapabilityExistingWithdrawalRecovery: true,
		recovery.CapabilityNewWithdrawalCreation:      true,
		recovery.CapabilityEventPublishing:            true,
		recovery.CapabilityEventConsuming:             true,
	}
	for _, capability := range releaseOrder {
		if capability == recovery.CapabilityQuery {
			continue
		}
		approveAtEntry(capability, "auth:approver")
		if dual[capability] {
			approveAtEntry(capability, "auth:approver2")
		}
		releaseAtEntry(capability)
	}

	// 已开放与未开放项可分别观察: the released dependency root is admitted
	// while the still-unreleased query stays refused.
	if d := admitAtEntry(recovery.CapabilityChainScan); !d.Allowed {
		t.Fatalf("a released capability was not admitted: %s (%s)", d.RefusalClass, d.Reason)
	}
	if d := admitAtEntry(recovery.CapabilityQuery); d.Allowed || d.RefusalClass != recovery.RefusalNoRelease {
		t.Fatalf("the unreleased capability = %+v, want no_release", d)
	}

	// A partial release set never closes (S11): the refusal is explicit and
	// lists the unreleased remainder, the instance stays open and the refusal
	// is audited — nothing reads as an accepted close.
	partial, err := recovery.CloseInstance(env.ctx, env.store, gate, recovery.CloseInstanceRequest{
		InstanceID: env.instanceID, Actor: "deploy:executor",
		Reason: "drill: close over the partial release set", OperationID: "drill-idem-close-partial",
		TrustedEntryChains: []uint64{1}, TargetGuardKey: targetGuardKey, TargetRoleFingerprint: targetRoleFingerprint,
	})
	if !errors.Is(err, recovery.ErrInstanceCloseBlocked) {
		t.Fatalf("close over the partial release set = (%+v, %v), want ErrInstanceCloseBlocked", partial, err)
	}
	if partial.Closed || len(partial.Blocked) == 0 {
		t.Fatalf("partial close result = %+v, want Closed=false with the unreleased remainder listed", partial)
	}
	var state string
	if err := env.ctrl.QueryRow(env.ctx,
		`SELECT state FROM recovery_instance WHERE instance_id = $1`, env.instanceID).Scan(&state); err != nil {
		t.Fatalf("read instance state: %v", err)
	}
	if state != "open" {
		t.Fatalf("instance state after the refused close = %s, want open", state)
	}
	if got := env.auditCount(recovery.ActionInstanceClose, "ok"); got != 0 {
		t.Fatalf("accepted close audit rows after the refused close = %d, want 0", got)
	}
	if got := env.auditCount(recovery.ActionInstanceClose, "refused"); got != 1 {
		t.Fatalf("refused close audit rows = %d, want 1", got)
	}

	// Release the remaining capability, then close with the complete set.
	approveAtEntry(recovery.CapabilityQuery, "auth:approver")
	releaseAtEntry(recovery.CapabilityQuery)
	if d := admitAtEntry(recovery.CapabilityQuery); !d.Allowed {
		t.Fatalf("query not admitted after its release: %s (%s)", d.RefusalClass, d.Reason)
	}

	// Close with the complete release set.
	first, err := recovery.CloseInstance(env.ctx, env.store, gate, recovery.CloseInstanceRequest{
		InstanceID: env.instanceID, Actor: "deploy:executor",
		Reason: "drill: all seven capabilities released", OperationID: "drill-idem-close-instance",
		TrustedEntryChains: []uint64{1}, TargetGuardKey: targetGuardKey, TargetRoleFingerprint: targetRoleFingerprint,
	})
	if err != nil || !first.Closed {
		t.Fatalf("close over the complete release set = (%+v, %v), want closed", first, err)
	}
	if err := env.ctrl.QueryRow(env.ctx,
		`SELECT state FROM recovery_instance WHERE instance_id = $1`, env.instanceID).Scan(&state); err != nil {
		t.Fatalf("read instance state: %v", err)
	}
	if state != "closed" {
		t.Fatalf("instance state = %s, want closed", state)
	}
	// Repeating the close (same and fresh operation ids) never re-closes or
	// flips the instance.
	for i := 0; i < 10; i++ {
		op := "drill-idem-close-instance"
		if i%2 == 1 {
			op = env.operation("close-instance-replay")
		}
		if _, err := recovery.CloseInstance(env.ctx, env.store, gate, recovery.CloseInstanceRequest{
			InstanceID: env.instanceID, Actor: "deploy:executor", Reason: "repeated close", OperationID: op,
			TrustedEntryChains: []uint64{1}, TargetGuardKey: targetGuardKey, TargetRoleFingerprint: targetRoleFingerprint,
		}); err == nil {
			t.Fatalf("repeated close #%d succeeded; a closed instance must never re-close", i+1)
		}
	}
	if err := env.ctrl.QueryRow(env.ctx,
		`SELECT state FROM recovery_instance WHERE instance_id = $1`, env.instanceID).Scan(&state); err != nil {
		t.Fatalf("read instance state: %v", err)
	}
	if state != "closed" {
		t.Fatalf("instance state flipped after repeated closes: %s", state)
	}
	if got := env.auditCount(recovery.ActionInstanceClose, "ok"); got != 1 {
		t.Fatalf("accepted close audit rows = %d, want exactly 1", got)
	}

	drillAssertFingerprintEqual(t, liveBefore, drillFingerprint(t, env.data), "live database during the close lifecycle")
}
