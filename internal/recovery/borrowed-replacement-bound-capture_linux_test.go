//go:build linux && drill

// borrowed-replacement-bound-capture_linux_test.go is the bounded single-chain
// instance-bound replacement capture lane. Over the APPROVED bound fixture it
// proves: the real first-open instance with its original factory binding -> the
// real receipt/entry single consumption -> the same-owner committed P1 -> the
// EXISTING DDL replacement orchestration with independent replacement
// validation -> a fresh transport binding and fresh OS prefix carrying the SAME
// retained owner, control database, target key, role fingerprint and authentic
// InstanceID -> the UNCHANGED borrowedReplacementCompareBindings accepts. The
// negatives exercise a real mismatched genuine-capture comparator refusal, a
// real cancellation at the publication pause and a real copied-prefix source
// loss with pre-fault retained copies permanently refused. Only real stages are
// asserted; there is no arbitrary caller-supplied instance id, no comparator
// change, no copied orchestration, no second-instance bypass, no replacement
// child and no admission waiter, and the instance inventory stays NULL with no
// EntryChainBound credit. This lane grants no restore, probe, acceptance,
// guard-clean, manifest, downstream or Gate1 authority; short caller-derived
// children, independent cleanup, cancel-and-bounded-join and unlogged secrets
// are preserved.
package recovery_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
)

// TestBorrowedReplacementBoundCapture is the bounded single-chain instance-bound
// replacement capture lane described in the file header.
func TestBorrowedReplacementBoundCapture(t *testing.T) {
	ctx := t.Context()
	var primary *borrowedSuccessorBaseline
	var primaryFresh *borrowedReplacementBinding
	var second *borrowedSuccessorBaseline

	// P: the single chain over the approved bound fixture, the EXISTING DDL
	// replacement orchestration and the UNCHANGED comparator.
	func() {
		primary = newBorrowedSuccessorBaselineBound(t, ctx)
		f := primary.fixture
		if !f.bound || f.instanceID == "" {
			t.Fatalf("bound capture baseline provenance: bound=%t instance=%q", f.bound, f.instanceID)
		}
		if entered, reason := primary.entry.stageNow(); !entered || reason != "" {
			t.Fatalf("bound capture baseline entry is not the single consumed entry: entered=%t reason=%q", entered, reason)
		}
		original := f.run.Binding()
		if original.OriginalInstanceID() != f.instanceID || original.OriginalInstanceID() == "" {
			t.Fatalf("original factory binding did not capture the authentic instance id: %q != %q", original.OriginalInstanceID(), f.instanceID)
		}
		// The instance inventory is NULL with zero EntryChainBound credit.
		var inventoryNull, versionNull bool
		invCtx, cancelInv := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
		invErr := f.controlPool.QueryRow(invCtx, `
SELECT entry_chain_inventory IS NULL, entry_chain_inventory_version IS NULL
FROM recovery_instance WHERE instance_id=$1`, f.instanceID).Scan(&inventoryNull, &versionNull)
		cancelInv()
		if invErr != nil || !inventoryNull || !versionNull {
			t.Fatalf("bound capture instance inventory is not NULL (inventory=%t version=%t err=%v)", inventoryNull, versionNull, invErr)
		}

		var stages int32
		orchestrateCtx, cancelOrchestrate := borrowedReplacementOrchestrationContext(ctx)
		fresh, err := borrowedReplacementBindingOrchestrate(orchestrateCtx, t, primary, &stages)
		cancelOrchestrate()
		if err != nil || fresh == nil {
			t.Fatalf("bound replacement capture refused: capture=%v err=%v", fresh, err)
		}
		if got := atomic.LoadInt32(&stages); got != borrowedReplacementStagePrefixCaptured {
			t.Fatalf("bound replacement capture stages=%d, want %d", got, borrowedReplacementStagePrefixCaptured)
		}
		primaryFresh = fresh
		freshBinding := fresh.binding
		// The persistent instance binding is consistent across the replacement.
		var persistedState, persistedKey, persistedFingerprint string
		persCtx, cancelPers := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
		persErr := f.controlPool.QueryRow(persCtx, `
SELECT state, target_guard_key, target_role_fingerprint FROM recovery_instance WHERE instance_id=$1`,
			f.instanceID).Scan(&persistedState, &persistedKey, &persistedFingerprint)
		cancelPers()
		if persErr != nil || persistedState != "open" || persistedKey != original.OriginalTargetKey().String() || persistedFingerprint != original.OriginalRoleFingerprint() {
			t.Fatalf("bound instance binding changed across the replacement: state=%q key=%q fingerprint=%q err=%v", persistedState, persistedKey, persistedFingerprint, persErr)
		}
		// Same retained owner / same control database / target key / role
		// fingerprint / InstanceID on the fresh transport.
		if freshBinding.OriginalInstanceID() != f.instanceID {
			t.Fatalf("fresh transport lost the authentic instance id: %q != %q", freshBinding.OriginalInstanceID(), f.instanceID)
		}
		if freshBinding.ControlBackendPID() != original.ControlBackendPID() || !freshBinding.ControlBackendStart().Equal(original.ControlBackendStart()) {
			t.Fatal("fresh transport changed the retained control owner identity")
		}
		if freshBinding.ControlDatabaseOID() != original.ControlDatabaseOID() ||
			freshBinding.ControlPostmasterStart() != original.ControlPostmasterStart() ||
			freshBinding.ControlTargetKey() != original.ControlTargetKey() {
			t.Fatal("fresh transport changed the control database identity")
		}
		if freshBinding.OriginalTargetKey() != original.OriginalTargetKey() ||
			freshBinding.OriginalRoleFingerprint() != original.OriginalRoleFingerprint() ||
			freshBinding.OriginalOperationID() != original.OriginalOperationID() {
			t.Fatal("fresh transport changed the logical target identity")
		}
		if freshBinding.ClusterSystemIdentifier() != original.ClusterSystemIdentifier() ||
			freshBinding.PostmasterStartTime() != original.PostmasterStartTime() ||
			freshBinding.WriterRoleOID() != original.WriterRoleOID() {
			t.Fatal("fresh transport changed the cluster/role identity")
		}
		if fresh.replacementOID == 0 || fresh.replacementOID == fresh.oldOID || freshBinding.TargetDatabaseOID() != fresh.replacementOID {
			t.Fatalf("fresh transport is not replacement-bound: replacement=%d old=%d binding=%d", fresh.replacementOID, fresh.oldOID, freshBinding.TargetDatabaseOID())
		}
		if err := borrowedReplacementCompareBindings(original, freshBinding, fresh.replacementOID); err != nil {
			t.Fatalf("UNCHANGED authentic comparator refused the genuine bound replacement capture: %v", err)
		}
		// Real same-capture mismatch: the retained old OID is not the verified
		// replacement.
		if oidErr := borrowedReplacementCompareBindings(original, freshBinding, fresh.oldOID); oidErr == nil ||
			oidErr.Error() != "replacement binding target database OID is not the verified replacement" {
			t.Fatalf("comparator did not refuse the retained old OID at its real OID stage: %v", oidErr)
		}
		prefixCtx, cancelPrefix := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
		prefixErr := fresh.prefix.Recheck(t, prefixCtx)
		cancelPrefix()
		if prefixErr != nil {
			t.Fatalf("fresh bound prefix recheck refused: %v", prefixErr)
		}
		borrowedOwnerDDLGuard(t, ctx, primary)
		t.Logf("bound capture positive: the real first-open instance %s (NULL inventory, no EntryChainBound credit) carried its original factory binding through the single receipt/entry consumption and same-owner P1 into the existing DDL replacement orchestration; the fresh transport kept the retained owner, control database, target key, role fingerprint and InstanceID and the UNCHANGED comparator accepted it against replacement OID %d", f.instanceID, fresh.replacementOID)
	}()

	// N1: a real cancellation at the publication pause on a second bound
	// baseline publishes no capture and permanently invalidates the fresh state.
	func() {
		second = newBorrowedSuccessorBaselineBound(t, ctx)
		entered := make(chan struct{})
		var enteredOnce sync.Once
		var observed *borrowedIdentityPrefix
		cancelCh := make(chan context.CancelFunc, 1)
		hook := func(hookCtx context.Context, prefix *borrowedIdentityPrefix) {
			observed = prefix
			enteredOnce.Do(func() { close(entered) })
			cancel := <-cancelCh
			cancel()
			<-hookCtx.Done()
		}
		borrowedReplacementPublishHook.Store(&hook)
		defer borrowedReplacementPublishHook.Store(nil)
		var stages int32
		useCtx, cancelUse := borrowedReplacementOrchestrationContext(ctx)
		cancelCh <- cancelUse
		captured, pipelineErr := borrowedReplacementBindingOrchestrate(useCtx, t, second, &stages)
		if pipelineErr == nil || captured != nil {
			t.Fatalf("cancelled bound publication produced a capture: capture=%v err=%v", captured, pipelineErr)
		}
		if useCtx.Err() == nil {
			t.Fatal("cancelled bound publication did not end its pipeline context")
		}
		select {
		case <-entered:
		default:
			t.Fatal("cancelled bound publication never reached the real publication pause")
		}
		if got := atomic.LoadInt32(&stages); got != borrowedReplacementStageTransportCaptured {
			t.Fatalf("cancelled bound publication stages=%d, want stop at %d", got, borrowedReplacementStageTransportCaptured)
		}
		if observed == nil {
			t.Fatal("cancelled bound publication did not observe the fresh prefix")
		}
		if invalid, reason := observed.Invalid(); !invalid || reason == "" {
			t.Fatal("cancelled bound publication did not permanently invalidate the fresh state")
		}
		copied := *observed
		if invalid, _ := copied.Invalid(); !invalid {
			t.Fatal("cancelled bound publication loss is not shared with the copy")
		}
		borrowedOwnerDDLGuard(t, ctx, second)
		t.Logf("bound capture cancelled publication: the caller context ended at the real publication pause, no capture was published, the pipeline stopped at %d and the fresh state was permanently invalidated with the loss shared", borrowedReplacementStageTransportCaptured)
	}()

	// N2: a real capture comparison with mismatched bound fields: two genuine
	// bound lineages with distinct real instance identities refuse at the
	// UNCHANGED comparator's real stage.
	func() {
		if primary == nil || second == nil {
			t.Fatal("bound capture mismatch control requires both genuine lineages")
		}
		firstBinding := primary.fixture.run.Binding()
		otherBinding := second.fixture.run.Binding()
		if firstBinding.OriginalInstanceID() == "" || otherBinding.OriginalInstanceID() == "" ||
			firstBinding.OriginalInstanceID() == otherBinding.OriginalInstanceID() {
			t.Fatalf("bound capture mismatch control requires two distinct real instance ids: %q / %q", firstBinding.OriginalInstanceID(), otherBinding.OriginalInstanceID())
		}
		compareErr := borrowedReplacementCompareBindings(firstBinding, otherBinding, otherBinding.TargetDatabaseOID())
		if compareErr == nil {
			t.Fatal("UNCHANGED comparator accepted two genuinely mismatched bound captures")
		}
		switch compareErr.Error() {
		case "replacement binding control owner identity changed",
			"replacement binding control session identity changed",
			"replacement binding logical target identity changed",
			"replacement binding cluster/role identity changed",
			"replacement binding target database OID is not the verified replacement":
		default:
			t.Fatalf("mismatched bound captures were not refused at a real comparator stage: %v", compareErr)
		}
		t.Logf("bound capture mismatch control: the UNCHANGED comparator refused two genuine bound lineages with distinct real instance ids at its real stage %q", compareErr)
	}()

	// N3: a real shared-prefix source loss permanently refuses the pre-fault
	// retained copies of the genuine bound capture.
	func() {
		fresh := primaryFresh
		if fresh == nil || fresh.prefix == nil {
			t.Fatal("bound capture prefix-loss control requires the positive capture")
		}
		retainedCapture := *fresh
		retainedPrefix := *fresh.prefix
		borrowedReplacementSessionCopiedPrefixLoss(t, fresh)
		if invalid, reason := retainedPrefix.Invalid(); !invalid || reason == "" {
			t.Fatal("pre-fault retained prefix copy did not observe the shared source loss")
		}
		if invalid, _ := retainedCapture.prefix.Invalid(); !invalid {
			t.Fatal("pre-fault retained capture copy did not observe the shared source loss")
		}
		for attempt := 1; attempt <= 2; attempt++ {
			recheckCtx, cancelRecheck := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
			recheckErr := retainedPrefix.Recheck(t, recheckCtx)
			cancelRecheck()
			if recheckErr == nil {
				t.Fatalf("pre-fault retained prefix copy recheck %d succeeded after the real shared-prefix loss", attempt)
			}
		}
		if invalid, _ := fresh.prefix.Invalid(); !invalid {
			t.Fatal("original bound capture prefix did not retain the shared source loss")
		}
		t.Logf("bound capture source loss: the genuine bound prefix was actually lost through the unchanged copied-prefix mechanism and both pre-fault retained copies remained permanently refused")
	}()
}
