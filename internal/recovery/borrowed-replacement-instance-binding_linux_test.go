//go:build linux && drill

// borrowed-replacement-instance-binding_linux_test.go is the bounded genuine
// immutable instance-bound owner lineage lane. It exercises the authorized
// bound fixture-construction variant: bounded bootstrap target lock first ->
// pristine proof -> insert-only fixture guard init -> exact clean/provenance
// validation -> authentic Store.OpenInstance -> CONFIRMED bounded bootstrap
// release -> retained owner acquisition -> the same factory call with the
// authentic instance id and the transactional restore_started marker. The
// genuine positive chain (prerequisite native attempt with the deliberate
// probe rejection and no acceptance) carries the identity through the actual
// receipt/entry and the anchor identifies the RETAINED owner. A matching
// persisted lineage still cannot admit the dirty guard: the ordinary refusal is
// exercised over the SAME retained fixture with no child, no probe, no
// acceptance and no new marker/generation. Empty/nonexistent/foreign/wrong
// target/wrong role bindings and absent/stale/wrong-operation markers refuse
// without usable publication, an options-id substitution cannot promote an old
// unbound receipt (and the unchanged authentic capture validator isolates a
// real-candidate instance-id substitution), a genuine bound source loss
// permanently refuses retained pre-fault copies and replay, and the unbound
// default lineage stays byte-behaviorally unchanged. The guard stays unresolved, no accepted restore evidence or
// reusable authority is produced, there is no manifest/admission/atomic
// acceptance/downstream/Gate1 authority, and secrets, verifiers and DSNs are
// never logged.
package recovery_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/xtianxx/txharbor/internal/recovery"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

// borrowedInstanceBindingDispatch is the exact observed outcome of one bound
// production run dispatch.
type borrowedInstanceBindingDispatch struct {
	runErr         error
	started        bool
	probeCalls     int32
	acceptCalls    int32
	prelaunchCalls int32
}

// dispatchBorrowedInstanceBindingRun dispatches ONE production restore run
// over the bound fixture with the supplied instance id and marker hook,
// asserting no child/probe/acceptance from the refusal path.
func dispatchBorrowedInstanceBindingRun(t *testing.T, ctx context.Context, f *borrowedAuthHandoffFixture, instanceID, attemptID string, hook func(context.Context, pgx.Tx, controlstore.InstanceToken) (recovery.EvidenceToken, error)) borrowedInstanceBindingDispatch {
	t.Helper()
	return dispatchBorrowedInstanceBindingRunOn(t, ctx, f, f, f.writerTargetDSN, instanceID, attemptID, hook)
}

// dispatchBorrowedInstanceBindingRunOn dispatches with an explicit target DSN
// and split holder/source fixtures: the control store and instance come from
// the holder, while the target/transport (gate, lock, observer, tools, archive)
// come from the source. It witnesses the Prelaunch callback invocation.
func dispatchBorrowedInstanceBindingRunOn(t *testing.T, ctx context.Context, holder, source *borrowedAuthHandoffFixture, targetDSN, instanceID, attemptID string, hook func(context.Context, pgx.Tx, controlstore.InstanceToken) (recovery.EvidenceToken, error)) borrowedInstanceBindingDispatch {
	t.Helper()
	targetTarget, err := controlstore.ParseDSNTarget(targetDSN)
	if err != nil {
		t.Fatalf("instance-binding target identity refused: %v", err)
	}
	gate := newOriginGate(t, ctx, source.fx)
	store, storeErr := controlstore.NewStore(ctx, holder.controlPool)
	if storeErr != nil {
		t.Fatalf("instance-binding control store refused: %v", storeErr)
	}
	var probeCalls, acceptCalls, prelaunchCalls int32
	run, err := recovery.DrillNewBorrowedWriterRun(ctx, recovery.TargetWriterOptions{
		OperationKind: recovery.TargetWriterOperationRestore,
		Store:         store,
		ControlDSN:    holder.controlDSN,
		TargetDSN:     targetDSN,
		ObserverDSN:   source.observerTargetDSN,
		TrustedTarget: targetTarget,
		OperationID:   attemptID,
		Archive:       borrowedReplacementPrelaunchArchive(t, source),
		InstanceID:    instanceID,
		Prelaunch: func(prelaunchCtx context.Context, tx pgx.Tx, locked controlstore.InstanceToken) (recovery.EvidenceToken, error) {
			atomic.AddInt32(&prelaunchCalls, 1)
			return hook(prelaunchCtx, tx, locked)
		},
		Probe: func(context.Context, recovery.TargetWriterProof) (recovery.TargetWriterProbeResult, error) {
			atomic.AddInt32(&probeCalls, 1)
			return recovery.TargetWriterProbeResult{}, errors.New("instance-binding lane probe intentionally rejects")
		},
		Acceptance: func(context.Context, pgx.Tx, recovery.TargetWriterProof) (recovery.TargetWriterAcceptance, error) {
			atomic.AddInt32(&acceptCalls, 1)
			return recovery.TargetWriterAcceptance{}, errors.New("instance-binding lane acceptance must never run")
		},
		QuiescenceTimeout: 5 * time.Second, QuiescenceInterval: 50 * time.Millisecond,
	}, holder.lock, gate.endpoint, source.tools)
	if err != nil {
		return borrowedInstanceBindingDispatch{runErr: err}
	}
	runCtx, cancelRun := context.WithTimeout(ctx, 60*time.Second)
	_, _, runErr := run.Run(runCtx)
	cancelRun()
	identity := run.Observation().StartedIdentity()
	return borrowedInstanceBindingDispatch{
		runErr: runErr, started: identity.Started || identity.PID != 0,
		probeCalls: atomic.LoadInt32(&probeCalls), acceptCalls: atomic.LoadInt32(&acceptCalls),
		prelaunchCalls: atomic.LoadInt32(&prelaunchCalls),
	}
}

// borrowedInstanceBindingGuardSnapshotHash returns a safe hash of the COMPLETE
// guard row (never dumping its contents).
func borrowedInstanceBindingGuardSnapshotHash(t *testing.T, ctx context.Context, pool interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, key string) string {
	t.Helper()
	readCtx, cancelRead := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	defer cancelRead()
	var snapshot string
	if err := pool.QueryRow(readCtx, `SELECT COALESCE(row_to_json(g)::text, '') FROM recovery_target_guard g WHERE target_guard_key=$1`, key).Scan(&snapshot); err != nil {
		t.Fatalf("instance-binding guard snapshot refused: %v", err)
	}
	sum := sha256.Sum256([]byte(snapshot))
	return hex.EncodeToString(sum[:])
}

// borrowedInstanceBindingMutateTargetDB derives a same-role different-database
// target DSN so a target-key mismatch is isolated from the role fingerprint.
func borrowedInstanceBindingMutateTargetDB(t *testing.T, dsn, suffix string) string {
	t.Helper()
	parsed, err := url.Parse(dsn)
	if err != nil || parsed.Scheme == "" {
		t.Fatalf("instance-binding target DSN is not url-parseable: %v", err)
	}
	parsed.Path = parsed.Path + suffix
	return parsed.String()
}

// borrowedInstanceBindingFreshCapture builds a genuine factory binding capture
// over the fixture's retained lock with an optional authentic instance id and
// never runs the factory child. The capture carries the real transport/control
// facts so the UNCHANGED authentic capture validator can compare it.
func borrowedInstanceBindingFreshCapture(t *testing.T, ctx context.Context, f *borrowedAuthHandoffFixture, instanceID string) (recovery.DrillBorrowedControlBinding, error) {
	t.Helper()
	targetTarget, err := controlstore.ParseDSNTarget(f.writerTargetDSN)
	if err != nil {
		return recovery.DrillBorrowedControlBinding{}, errors.New("instance-binding fresh capture target identity is invalid")
	}
	endpoint, err := recovery.DrillOpenOriginEndpoint()
	if err != nil {
		return recovery.DrillBorrowedControlBinding{}, errors.New("instance-binding fresh capture endpoint refused")
	}
	t.Cleanup(func() { _ = endpoint.Listener().Close() })
	opts := recovery.TargetWriterOptions{
		OperationKind: recovery.TargetWriterOperationRestore,
		ControlDSN:    f.controlDSN,
		TargetDSN:     f.writerTargetDSN,
		ObserverDSN:   f.observerTargetDSN,
		TrustedTarget: targetTarget,
		OperationID:   f.operation,
		InstanceID:    instanceID,
	}
	if instanceID != "" {
		opts.Prelaunch = func(context.Context, pgx.Tx, controlstore.InstanceToken) (recovery.EvidenceToken, error) {
			return recovery.EvidenceToken{}, errors.New("instance-binding fresh capture hook must never run")
		}
	}
	factoryCtx, cancelFactory := context.WithTimeout(ctx, borrowedReplacementFactoryBudget)
	defer cancelFactory()
	run, err := recovery.DrillNewBorrowedWriterRun(factoryCtx, opts, f.lock, endpoint, f.tools)
	if err != nil {
		return recovery.DrillBorrowedControlBinding{}, err
	}
	return run.Binding(), nil
}

// borrowedInstanceBindingGeneration reads the instance generation bounded.
func borrowedInstanceBindingGeneration(t *testing.T, ctx context.Context, pool interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, instanceID string) int64 {
	t.Helper()
	readCtx, cancelRead := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	defer cancelRead()
	var generation int64
	if err := pool.QueryRow(readCtx, `SELECT evidence_generation FROM recovery_instance WHERE instance_id=$1`, instanceID).Scan(&generation); err != nil {
		t.Fatalf("instance-binding generation read refused: %v", err)
	}
	return generation
}

// borrowedInstanceBindingGuardUnresolved asserts the deliberate non-clean
// guard and zero acceptance for one fixture.
func borrowedInstanceBindingGuardUnresolved(t *testing.T, ctx context.Context, f *borrowedAuthHandoffFixture) {
	t.Helper()
	if disposition := borrowedReplacementPrelaunchGuardDisposition(t, ctx, f); disposition == "clean" {
		t.Fatal("instance-binding lane resolved the deliberately non-clean guard")
	}
	if acceptance := atomic.LoadInt32(&f.acceptanceCalls); acceptance != 0 {
		t.Fatalf("instance-binding lane ran acceptance %d times", acceptance)
	}
}

// borrowedInstanceBindingNoChild asserts the refusal path stayed pre-launch.
func borrowedInstanceBindingNoChild(t *testing.T, dispatch borrowedInstanceBindingDispatch, label string) {
	t.Helper()
	if dispatch.runErr == nil {
		t.Fatalf("%s: was accepted", label)
	}
	if dispatch.started {
		t.Fatalf("%s: started a child", label)
	}
	if dispatch.probeCalls != 0 || dispatch.acceptCalls != 0 {
		t.Fatalf("%s: probe/acceptance ran (%d/%d)", label, dispatch.probeCalls, dispatch.acceptCalls)
	}
}

// TestBorrowedReplacementInstanceBinding is the bounded genuine immutable
// instance-bound owner lineage lane described in the file header.
func TestBorrowedReplacementInstanceBinding(t *testing.T) {
	ctx := t.Context()
	var probeCalls, acceptanceCalls int32

	// P: the matching persisted bound lineage reaches the guard-specific
	// ordinary refusal with no child/probe/acceptance and no new
	// marker/generation.
	func() {
		boundOpts := &borrowedAuthHandoffBoundOptions{
			Kind: "recovery", OpenedBy: "deploy:instance-binding", Reason: "bound lineage prerequisite",
		}
		f, session, outcome := runBorrowedReceiptAuthEntryPositiveChainWith(t, func(t *testing.T) *borrowedAuthHandoffFixture {
			return newBorrowedAuthHandoffFixtureBound(t, boundOpts)
		})
		if !f.bound || f.instanceID == "" {
			t.Fatalf("bound fixture provenance: bound=%t instance=%q", f.bound, f.instanceID)
		}
		// B4 narrowing: NULL inventory is explicitly asserted and NO
		// EntryChainBound credit is claimed by this lane.
		var inventoryNull, inventoryVersionNull bool
		invCtx, cancelInv := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
		invErr := f.controlPool.QueryRow(invCtx, `
SELECT entry_chain_inventory IS NULL, entry_chain_inventory_version IS NULL
FROM recovery_instance WHERE instance_id=$1`, f.instanceID).Scan(&inventoryNull, &inventoryVersionNull)
		cancelInv()
		if invErr != nil || !inventoryNull || !inventoryVersionNull {
			t.Fatalf("bound fixture inventory is not NULL (inventory=%t version=%t err=%v)", inventoryNull, inventoryVersionNull, invErr)
		}
		if got := f.run.Binding().OriginalInstanceID(); got != f.instanceID {
			t.Fatalf("factory did not capture the authentic instance id: %q != %q", got, f.instanceID)
		}
		if outcome.err == nil || !outcome.handoff.Diagnostics().Present {
			t.Fatalf("bound prerequisite chain produced no authentic token: err=%v", outcome.err)
		}
		// The actual receipt/entry identity carries the authentic instance id.
		entry, entryErr := newBorrowedReceiptAuthEntry(f, session, outcome.handoff)
		if entryErr != nil {
			t.Fatalf("bound receipt/entry identity refused the authentic lineage: %v", entryErr)
		}
		if entry == nil {
			t.Fatal("bound receipt/entry construction returned no entry")
		}
		// B1: retain the copy and the replay handle BEFORE the fault, then
		// exercise the ACTUAL live recheck/replay refusal under fresh contexts
		// on the BOUND lineage: the own-cancellation publication-seam loss is
		// shared with the pre-fault copy and with a replay constructed over the
		// same authentic run/session/handoff, and no live caller can
		// rehabilitate the permanently lost checkpoint.
		retainedBoundCopy := *entry
		boundReplay, boundReplayErr := newBorrowedReceiptAuthEntry(f, session, outcome.handoff)
		if boundReplayErr != nil || boundReplay == nil {
			t.Fatalf("bound replay construction over the same authentic run/session/handoff refused: entry=%v err=%v", boundReplay, boundReplayErr)
		}
		boundSeam := installBorrowedReceiptAuthEntrySeam(t, entry, borrowedReceiptAuthEntryStageRecheckPublish)
		boundSeamCtx, cancelBoundSeam := context.WithCancel(ctx)
		boundSeamCh := make(chan error, 1)
		go func() { boundSeamCh <- entry.Recheck(boundSeamCtx) }()
		select {
		case <-boundSeam.entered:
		case <-time.After(60 * time.Second):
			cancelBoundSeam()
			t.Fatal("bound recheck never reached the pre-publication seam")
		}
		cancelBoundSeam()
		boundSeam.releaseSeam()
		select {
		case boundSeamErr := <-boundSeamCh:
			if boundSeamErr == nil {
				t.Fatal("bound recheck published success after its own cancellation")
			}
		case <-time.After(30 * time.Second):
			t.Fatal("cancelled bound recheck did not return")
		}
		if boundSeamEntered, boundSeamReason := entry.stageNow(); boundSeamEntered || boundSeamReason == "" {
			t.Fatal("bound cancellation did not permanently invalidate the checkpoint")
		}
		if err := retainedBoundCopy.Recheck(ctx); err == nil {
			t.Fatal("retained pre-fault bound copy rechecked after the permanent cancellation loss")
		}
		if err := retainedBoundCopy.Enter(ctx); err == nil {
			t.Fatal("retained pre-fault bound copy entered the auth stage after the permanent cancellation loss")
		}
		if copiedEntered, copiedReason := retainedBoundCopy.stageNow(); copiedEntered || copiedReason == "" {
			t.Fatal("retained pre-fault bound copy did not retain the shared permanent cancellation loss")
		}
		if err := boundReplay.Recheck(ctx); err == nil {
			t.Fatal("bound replay rechecked after the permanent cancellation loss")
		}
		if err := boundReplay.Enter(ctx); err == nil {
			t.Fatal("bound replay entered the auth stage after the permanent cancellation loss")
		}
		if replayEntered, replayReason := boundReplay.stageNow(); replayEntered || replayReason == "" {
			t.Fatal("bound replay did not retain the shared permanent cancellation loss")
		}
		// The anchor identifies the RETAINED owner (the bootstrap lock was
		// released before acquisition) and retains the instance id.
		anchorCtx, cancelAnchor := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
		anchor, anchorErr := f.run.CaptureControlAnchor(anchorCtx)
		cancelAnchor()
		if anchorErr != nil {
			t.Fatalf("bound anchor capture refused: %v", anchorErr)
		}
		facts := anchor.Diagnostics()
		if !facts.Present || facts.Invalidated {
			t.Fatalf("bound anchor is absent or invalidated: %+v", facts)
		}
		if facts.OriginalInstanceID != f.instanceID {
			t.Fatalf("bound anchor lost the authentic instance id: %q != %q", facts.OriginalInstanceID, f.instanceID)
		}
		if facts.BackendPID <= 0 {
			t.Fatal("bound anchor did not identify the retained owner incarnation")
		}
		// The genuine prerequisite attempt prepared the guard; a matching
		// persisted bound lineage still refuses the ordinary dispatch.
		generationBefore := borrowedInstanceBindingGeneration(t, ctx, f.controlPool, f.instanceID)
		dispatch := dispatchBorrowedInstanceBindingRun(t, ctx, f, f.instanceID,
			"instance-binding-refusal-"+f.operation, borrowedAuthFixtureRestoreStartedMarker("instance-binding-refusal-"+f.operation, f.adminRole))
		borrowedInstanceBindingNoChild(t, dispatch, "instance-binding matching-lineage refusal")
		if !strings.Contains(dispatch.runErr.Error(), "guard") || !strings.Contains(dispatch.runErr.Error(), "not clean") {
			t.Fatalf("matching-lineage refusal was not the guard-specific ordinary refusal: %v", dispatch.runErr)
		}
		if after := borrowedInstanceBindingGeneration(t, ctx, f.controlPool, f.instanceID); after != generationBefore {
			t.Fatalf("bound dirty-guard refusal changed the instance generation: %d -> %d", generationBefore, after)
		}
		borrowedInstanceBindingGuardUnresolved(t, ctx, f)
		t.Logf("instance-binding positive: the authentic bound lineage (kind=%s, NULL inventory explicitly asserted, no EntryChainBound credit) carried the instance id through the factory binding, the actual receipt/entry and the retained-owner anchor; the matching persisted lineage still refused the ordinary dispatch with the guard-specific refusal, no child/probe/acceptance, an unchanged instance generation %d and the guard unresolved", boundOpts.Kind, generationBefore)
	}()

	// B1b: BOUND source-loss permanence on an INDEPENDENT genuine bound
	// positive chain: the fixture's genuine captured identity prefix is
	// actually lost through the unchanged copied-prefix mechanism, and the
	// retained pre-fault copy plus a replay constructed over the same
	// authentic run/session/handoff must refuse Recheck/Enter live under fresh
	// contexts with permanent shared invalidation, zero acceptance and no
	// guard/generation effect.
	func() {
		lossFixture, lossSession, lossOutcome := runBorrowedReceiptAuthEntryPositiveChainWith(t, func(t *testing.T) *borrowedAuthHandoffFixture {
			return newBorrowedAuthHandoffFixtureBound(t, &borrowedAuthHandoffBoundOptions{
				Kind: "recovery", OpenedBy: "deploy:instance-binding-source-loss", Reason: "bound source-loss fixture",
			})
		})
		if lossFixture.instanceID == "" || lossFixture.prefix == nil {
			t.Fatal("bound source-loss fixture has no authentic instance/prefix provenance")
		}
		lossGeneration := borrowedInstanceBindingGeneration(t, ctx, lossFixture.controlPool, lossFixture.instanceID)
		lossEntry, lossEntryErr := newBorrowedReceiptAuthEntry(lossFixture, lossSession, lossOutcome.handoff)
		if lossEntryErr != nil || lossEntry == nil {
			t.Fatalf("bound source-loss entry construction refused: entry=%v err=%v", lossEntry, lossEntryErr)
		}
		retainedLossCopy := *lossEntry
		lossReplay, lossReplayErr := newBorrowedReceiptAuthEntry(lossFixture, lossSession, lossOutcome.handoff)
		if lossReplayErr != nil || lossReplay == nil {
			t.Fatalf("bound source-loss replay construction refused: entry=%v err=%v", lossReplay, lossReplayErr)
		}
		// ACTUAL copied-prefix source loss on the bound fixture's genuine
		// captured prefix through the UNCHANGED genuine loss helper (the
		// binding literal is only a carrier for the real prefix).
		borrowedReplacementSessionCopiedPrefixLoss(t, &borrowedReplacementBinding{prefix: lossFixture.prefix})
		if invalid, reason := lossFixture.prefix.Invalid(); !invalid || reason == "" {
			t.Fatal("bound fixture genuine prefix did not retain the copied-prefix source loss")
		}
		if err := retainedLossCopy.Recheck(ctx); err == nil {
			t.Fatal("retained pre-fault bound copy rechecked after the genuine bound source loss")
		}
		if err := retainedLossCopy.Enter(ctx); err == nil {
			t.Fatal("retained pre-fault bound copy entered the auth stage after the genuine bound source loss")
		}
		if entered, reason := retainedLossCopy.stageNow(); entered || reason == "" {
			t.Fatal("bound source loss did not permanently invalidate the retained pre-fault copy")
		}
		if err := lossReplay.Recheck(ctx); err == nil {
			t.Fatal("bound source-loss replay rechecked after the genuine bound source loss")
		}
		if err := lossReplay.Enter(ctx); err == nil {
			t.Fatal("bound source-loss replay entered the auth stage after the genuine bound source loss")
		}
		if entered, reason := lossReplay.stageNow(); entered || reason == "" {
			t.Fatal("bound source-loss replay did not retain the shared permanent loss")
		}
		if acceptance := atomic.LoadInt32(&lossFixture.acceptanceCalls); acceptance != 0 {
			t.Fatalf("bound source-loss control ran acceptance %d times", acceptance)
		}
		if after := borrowedInstanceBindingGeneration(t, ctx, lossFixture.controlPool, lossFixture.instanceID); after != lossGeneration {
			t.Fatalf("bound source-loss control changed the instance generation: %d -> %d", lossGeneration, after)
		}
		if disposition := borrowedReplacementPrelaunchGuardDisposition(t, ctx, lossFixture); disposition == "clean" {
			t.Fatal("bound source-loss control resolved the deliberately non-clean guard")
		}
		t.Logf("instance-binding bound source-loss: the genuine bound fixture prefix was actually lost through the unchanged copied-prefix mechanism; the retained pre-fault copy and the replay refused Recheck/Enter live under fresh contexts with permanent shared invalidation, zero acceptance and no guard/generation effect")
	}()

	// N1: marker controls over a CLEAN bound fixture: absent, stale and
	// wrong-operation markers refuse pre-write with no child and no
	// generation change; the guard stays exactly clean.
	func() {
		f := newBorrowedAuthHandoffFixtureBound(t, &borrowedAuthHandoffBoundOptions{
			Kind: "baseline", OpenedBy: "deploy:instance-binding-marker", Reason: "marker control fixture",
		})
		if f.instanceID == "" {
			t.Fatal("marker control fixture has no instance id")
		}
		markerOperation := func(label string) string { return "instance-binding-marker-" + label + "-" + f.operation }
		cases := []struct {
			label string
			stage string
			hook  func(context.Context, pgx.Tx, controlstore.InstanceToken) (recovery.EvidenceToken, error)
		}{
			{"absent", "prelaunch hook did not return the committed marker evidence token", func(context.Context, pgx.Tx, controlstore.InstanceToken) (recovery.EvidenceToken, error) {
				return recovery.EvidenceToken{}, nil
			}},
			{"stale", "prelaunch hook did not return the committed marker evidence token", func(markerCtx context.Context, tx pgx.Tx, locked controlstore.InstanceToken) (recovery.EvidenceToken, error) {
				write, err := recovery.CommitEvidenceWriteTx(markerCtx, tx, recovery.EvidenceWriteRequest{
					InstanceID: locked.InstanceID, Token: recovery.LockedEvidenceToken(locked),
					Kind: recovery.MutationRestoreStarted, Actor: "fixture-admin:" + f.adminRole,
					OperationID: markerOperation("stale"),
					Apply:       func(context.Context, pgx.Tx, recovery.EvidenceToken) error { return nil },
				})
				if err != nil || write.Discarded {
					return recovery.EvidenceToken{}, errors.New("instance-binding stale marker write refused")
				}
				// Deliberately return the PRE-write token (stale generation).
				return recovery.LockedEvidenceToken(locked), nil
			}},
			{"wrong-operation", "prelaunch hook did not commit restore_started evidence for this operation", func(markerCtx context.Context, tx pgx.Tx, locked controlstore.InstanceToken) (recovery.EvidenceToken, error) {
				write, err := recovery.CommitEvidenceWriteTx(markerCtx, tx, recovery.EvidenceWriteRequest{
					InstanceID: locked.InstanceID, Token: recovery.LockedEvidenceToken(locked),
					Kind: recovery.MutationRestoreStarted, Actor: "fixture-admin:" + f.adminRole,
					OperationID: markerOperation("wrong-operation") + "-foreign",
					Apply:       func(context.Context, pgx.Tx, recovery.EvidenceToken) error { return nil },
				})
				if err != nil || write.Discarded {
					return recovery.EvidenceToken{}, errors.New("instance-binding wrong-operation marker write refused")
				}
				return write.Token, nil
			}},
		}
		for _, markerCase := range cases {
			generationBefore := borrowedInstanceBindingGeneration(t, ctx, f.controlPool, f.instanceID)
			snapshotBefore := borrowedInstanceBindingGuardSnapshotHash(t, ctx, f.controlPool, f.guardKey)
			attemptID := markerOperation(markerCase.label)
			dispatch := dispatchBorrowedInstanceBindingRun(t, ctx, f, f.instanceID, attemptID, markerCase.hook)
			borrowedInstanceBindingNoChild(t, dispatch, "instance-binding marker "+markerCase.label)
			if dispatch.prelaunchCalls != 1 {
				t.Fatalf("instance-binding marker %s did not witness exactly one Prelaunch callback: %d", markerCase.label, dispatch.prelaunchCalls)
			}
			if !strings.Contains(dispatch.runErr.Error(), markerCase.stage) {
				t.Fatalf("instance-binding marker %s was not the expected production rejection stage %q: %v", markerCase.label, markerCase.stage, dispatch.runErr)
			}
			if after := borrowedInstanceBindingGeneration(t, ctx, f.controlPool, f.instanceID); after != generationBefore {
				t.Fatalf("instance-binding marker %s changed the generation: %d -> %d", markerCase.label, generationBefore, after)
			}
			snapshotAfter := borrowedInstanceBindingGuardSnapshotHash(t, ctx, f.controlPool, f.guardKey)
			if snapshotAfter != snapshotBefore {
				t.Fatalf("instance-binding marker %s changed the complete guard row snapshot", markerCase.label)
			}
			if disposition := borrowedReplacementPrelaunchGuardDisposition(t, ctx, f); disposition != "clean" {
				t.Fatalf("instance-binding marker %s changed the clean guard: %q", markerCase.label, disposition)
			}
		}
		if acceptance := atomic.LoadInt32(&f.acceptanceCalls); acceptance != 0 {
			t.Fatalf("instance-binding marker controls ran acceptance %d times", acceptance)
		}
		t.Logf("instance-binding marker controls: Prelaunch was WITNESSED for each case and absent/stale/wrong-operation markers refused at their exact production stages (committed-marker-token vs restore_started evidence for this operation) with no child/probe/acceptance, no generation change and the COMPLETE guard row snapshot unchanged")
	}()

	// N2: instance-id negatives: nonexistent, wrong-role, foreign and
	// substitution controls; empty/unbound controls stay unbound.
	func() {
		f := newBorrowedAuthHandoffFixtureBound(t, &borrowedAuthHandoffBoundOptions{
			Kind: "recovery", OpenedBy: "deploy:instance-binding-negative", Reason: "id negative fixture",
		})
		generationBefore := borrowedInstanceBindingGeneration(t, ctx, f.controlPool, f.instanceID)
		// Nonexistent instance id refuses at the instance binding.
		missing := dispatchBorrowedInstanceBindingRun(t, ctx, f, uuid.NewString(),
			"instance-binding-missing-"+f.operation, borrowedAuthFixtureRestoreStartedMarker("instance-binding-missing-"+f.operation, f.adminRole))
		borrowedInstanceBindingNoChild(t, missing, "instance-binding nonexistent id")
		if !strings.Contains(missing.runErr.Error(), "instance target binding is not usable") {
			t.Fatalf("nonexistent id refusal was not the instance-binding stage: %v", missing.runErr)
		}
		// Wrong-role instance: the single-open invariant means the wrong-role
		// instance must live in its OWN fixture control store; that fixture's
		// clean guard lets the authentic open succeed, and its bound run
		// refuses the role-fingerprint mismatch.
		wrongRoleFixture := newBorrowedAuthHandoffFixture(t)
		wrongRoleFingerprint := "sha256:" + strings.Repeat("a", 64)
		openedWrongRole, wrongRoleErr := controlstore.NewStore(ctx, wrongRoleFixture.controlPool)
		if wrongRoleErr != nil {
			t.Fatalf("instance-binding wrong-role store refused: %v", wrongRoleErr)
		}
		targetTarget, targetErr := controlstore.ParseDSNTarget(wrongRoleFixture.writerTargetDSN)
		if targetErr != nil {
			t.Fatalf("instance-binding wrong-role target identity refused: %v", targetErr)
		}
		key, keyErr := recovery.CanonicalTargetKey(targetTarget)
		if keyErr != nil {
			t.Fatalf("instance-binding wrong-role target key refused: %v", keyErr)
		}
		wrongRoleOpen, wrongRoleOpenErr := openedWrongRole.OpenInstance(ctx, controlstore.OpenInstanceRequest{
			Kind: "baseline", OpenedBy: "deploy:instance-binding-wrong-role", Reason: "wrong role control",
			TargetGuardKey: key.String(), TargetRoleFingerprint: wrongRoleFingerprint,
		})
		if wrongRoleOpenErr != nil {
			t.Fatalf("instance-binding wrong-role open refused unexpectedly early: %v", wrongRoleOpenErr)
		}
		// Persisted authentic first-open state: open, matching target key with the
		// role fingerprint as the ISOLATED logical-identity mismatch.
		var persistedState, persistedKey, persistedFingerprint string
		persistCtx, cancelPersist := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
		persistErr := wrongRoleFixture.controlPool.QueryRow(persistCtx, `
SELECT state, target_guard_key, target_role_fingerprint FROM recovery_instance WHERE instance_id=$1`,
			wrongRoleOpen.InstanceID).Scan(&persistedState, &persistedKey, &persistedFingerprint)
		cancelPersist()
		if persistErr != nil || persistedState != "open" {
			t.Fatalf("wrong-role persisted open state refused: state=%q err=%v", persistedState, persistErr)
		}
		if persistedKey != key.String() {
			t.Fatalf("wrong-role instance target key is not the run key: %q != %q", persistedKey, key.String())
		}
		if persistedFingerprint != wrongRoleFingerprint || wrongRoleFingerprint == targetTarget.DataTargetFingerprint().RoleFingerprint {
			t.Fatalf("wrong-role fingerprint is not the isolated mismatch: persisted=%q", persistedFingerprint)
		}
		wrongRole := dispatchBorrowedInstanceBindingRun(t, ctx, wrongRoleFixture, wrongRoleOpen.InstanceID,
			"instance-binding-wrong-role-"+wrongRoleFixture.operation, borrowedAuthFixtureRestoreStartedMarker("instance-binding-wrong-role-"+wrongRoleFixture.operation, wrongRoleFixture.adminRole))
		borrowedInstanceBindingNoChild(t, wrongRole, "instance-binding wrong-role")
		if wrongRole.prelaunchCalls != 0 {
			t.Fatalf("wrong-role refusal reached Prelaunch %d times, want refusal before the marker stage", wrongRole.prelaunchCalls)
		}
		if !strings.Contains(wrongRole.runErr.Error(), "instance target binding is not usable") {
			t.Fatalf("wrong-role refusal was not the instance-binding stage: %v", wrongRole.runErr)
		}
		// Authentic first-open WRONG-TARGET instance in an initially empty
		// isolated store: the persisted target key is the GENUINE canonical key
		// of the primary fixture's real target (never a synthetic fingerprint),
		// opened under that key's own bounded BOOTSTRAP target serialization
		// lock with consistent guard prerequisites, while the persisted role
		// fingerprint is the isolated dispatch target's, so the target-key
		// mismatch is decisive. The dispatch uses the isolated control store
		// with the isolated retained target/transport.
		isolated := newBorrowedAuthHandoffFixture(t) // unbound, clean guard, empty instance slot
		isolatedTarget, isolatedTargetErr := controlstore.ParseDSNTarget(isolated.writerTargetDSN)
		if isolatedTargetErr != nil {
			t.Fatalf("instance-binding isolated target identity refused: %v", isolatedTargetErr)
		}
		isolatedFingerprint := isolatedTarget.DataTargetFingerprint().RoleFingerprint
		if isolatedFingerprint == "" {
			t.Fatal("instance-binding isolated target fingerprint is empty")
		}
		secondTarget, secondTargetErr := controlstore.ParseDSNTarget(f.writerTargetDSN)
		if secondTargetErr != nil {
			t.Fatalf("instance-binding second target identity refused: %v", secondTargetErr)
		}
		secondKey, secondKeyErr := recovery.CanonicalTargetKey(secondTarget)
		if secondKeyErr != nil {
			t.Fatalf("instance-binding second target key refused: %v", secondKeyErr)
		}
		if secondKey.String() == isolated.guardKey {
			t.Fatal("instance-binding second target key equals the isolated dispatch key")
		}
		secondBootstrap := acquireBorrowedAuthFixtureBootstrapLock(t, ctx, isolated.controlDSN, secondKey)
		isolatedStore, isolatedStoreErr := controlstore.NewStore(ctx, isolated.controlPool)
		if isolatedStoreErr != nil {
			t.Fatalf("instance-binding isolated store refused: %v", isolatedStoreErr)
		}
		wrongTargetOpen, wrongTargetOpenErr := isolatedStore.OpenInstance(ctx, controlstore.OpenInstanceRequest{
			Kind: "recovery", OpenedBy: "deploy:instance-binding-wrong-target", Reason: "wrong target control",
			TargetGuardKey: secondKey.String(), TargetRoleFingerprint: isolatedFingerprint,
		})
		if wrongTargetOpenErr != nil {
			t.Fatalf("instance-binding wrong-target open refused unexpectedly early: %v", wrongTargetOpenErr)
		}
		if releaseErr := releaseBorrowedAuthFixtureBootstrapLock(ctx, secondBootstrap); releaseErr != nil {
			t.Fatalf("instance-binding wrong-target bootstrap target lock release was not confirmed: %v", releaseErr)
		}
		var wrongTargetState, wrongTargetKey, wrongTargetFingerprint string
		wtCtx, cancelWt := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
		wtErr := isolated.controlPool.QueryRow(wtCtx, `
SELECT state, target_guard_key, target_role_fingerprint FROM recovery_instance WHERE instance_id=$1`,
			wrongTargetOpen.InstanceID).Scan(&wrongTargetState, &wrongTargetKey, &wrongTargetFingerprint)
		cancelWt()
		if wtErr != nil || wrongTargetState != "open" {
			t.Fatalf("instance-binding wrong-target persisted open state refused: state=%q err=%v", wrongTargetState, wtErr)
		}
		if wrongTargetKey != secondKey.String() || wrongTargetFingerprint != isolatedFingerprint {
			t.Fatalf("instance-binding wrong-target persisted identity mismatch: key=%q fingerprint=%q", wrongTargetKey, wrongTargetFingerprint)
		}
		if wrongTargetKey == isolated.guardKey {
			t.Fatal("instance-binding wrong-target instance key matches the dispatching fixture key")
		}
		var secondGuardDisposition string
		guardReadCtx, cancelGuardRead := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
		guardReadErr := isolated.controlPool.QueryRow(guardReadCtx, `SELECT disposition FROM recovery_target_guard WHERE target_guard_key=$1`, secondKey.String()).Scan(&secondGuardDisposition)
		cancelGuardRead()
		if guardReadErr != nil || secondGuardDisposition != "unknown" {
			t.Fatalf("instance-binding wrong-target guard prerequisite is not the consistent provisioned inventory: disposition=%q err=%v", secondGuardDisposition, guardReadErr)
		}
		wrongTargetDispatch := dispatchBorrowedInstanceBindingRun(t, ctx, isolated, wrongTargetOpen.InstanceID,
			"instance-binding-wrong-target-instance-"+isolated.operation, borrowedAuthFixtureRestoreStartedMarker("instance-binding-wrong-target-instance-"+isolated.operation, isolated.adminRole))
		borrowedInstanceBindingNoChild(t, wrongTargetDispatch, "instance-binding wrong-target instance")
		if wrongTargetDispatch.prelaunchCalls != 0 {
			t.Fatalf("wrong-target instance refusal reached Prelaunch %d times, want refusal before the marker stage", wrongTargetDispatch.prelaunchCalls)
		}
		if !strings.Contains(wrongTargetDispatch.runErr.Error(), "instance target binding is not usable") {
			t.Fatalf("wrong-target instance refusal was not the decisive instance-binding key mismatch: %v", wrongTargetDispatch.runErr)
		}
		// Target-mismatch control on the ONE authentic first-open instance of the
		// primary fixture: a same-role different-database target keeps the role
		// fingerprint but changes the canonical target key.
		primaryTarget, primaryTargetErr := controlstore.ParseDSNTarget(f.writerTargetDSN)
		if primaryTargetErr != nil {
			t.Fatalf("instance-binding primary target identity refused: %v", primaryTargetErr)
		}
		mutatedDSN := borrowedInstanceBindingMutateTargetDB(t, f.writerTargetDSN, "_mismatch")
		mutatedTarget, mutatedErr := controlstore.ParseDSNTarget(mutatedDSN)
		if mutatedErr != nil {
			t.Fatalf("instance-binding mutated target identity refused: %v", mutatedErr)
		}
		mutatedKey, mutatedKeyErr := recovery.CanonicalTargetKey(mutatedTarget)
		if mutatedKeyErr != nil {
			t.Fatalf("instance-binding mutated target key refused: %v", mutatedKeyErr)
		}
		if mutatedKey.String() == f.guardKey {
			t.Fatal("instance-binding mutated target did not change the canonical key")
		}
		if mutatedTarget.DataTargetFingerprint().RoleFingerprint != primaryTarget.DataTargetFingerprint().RoleFingerprint {
			t.Fatal("instance-binding mutated target did not preserve the role fingerprint")
		}
		targetMismatch := dispatchBorrowedInstanceBindingRunOn(t, ctx, f, f, mutatedDSN, f.instanceID,
			"instance-binding-target-mismatch-"+f.operation, borrowedAuthFixtureRestoreStartedMarker("instance-binding-target-mismatch-"+f.operation, f.adminRole))
		borrowedInstanceBindingNoChild(t, targetMismatch, "instance-binding target mismatch")
		if targetMismatch.prelaunchCalls != 0 {
			t.Fatalf("target-mismatch refusal reached Prelaunch %d times, want refusal before the marker stage", targetMismatch.prelaunchCalls)
		}
		// Separately labelled early-stage control: the mutated target identity is
		// refused by the trusted-target transport binding BEFORE the instance
		// lock (observed stage asserted, never forced or credited as the
		// instance-binding decisive mismatch).
		if !strings.Contains(targetMismatch.runErr.Error(), "does not match the original trusted target") {
			t.Fatalf("target-mismatch refusal was not the trusted-target binding stage: %v", targetMismatch.runErr)
		}
		// Foreign instance: an instance opened in ANOTHER fixture's control
		// store is unknown here and refuses.
		foreign := newBorrowedAuthHandoffFixtureBound(t, &borrowedAuthHandoffBoundOptions{
			Kind: "baseline", OpenedBy: "deploy:instance-binding-foreign", Reason: "foreign instance fixture",
		})
		foreignDispatch := dispatchBorrowedInstanceBindingRun(t, ctx, f, foreign.instanceID,
			"instance-binding-foreign-"+f.operation, borrowedAuthFixtureRestoreStartedMarker("instance-binding-foreign-"+f.operation, f.adminRole))
		borrowedInstanceBindingNoChild(t, foreignDispatch, "instance-binding foreign id")
		// Supplementary dispatch control (NOT credited as the provenance
		// comparison): a nonexistent random candidate id on a new attempt run
		// is refused at the instance-binding lookup and cannot promote an old
		// unbound receipt. The authentic-capture InstanceID isolation with a
		// genuinely usable candidate follows below.
		b := newBorrowedSuccessorBaseline(t, ctx)
		var stages int32
		orchestrateCtx, cancelOrchestrate := borrowedReplacementOrchestrationContext(ctx)
		fresh, orchestrateErr := borrowedReplacementBindingOrchestrate(orchestrateCtx, t, b, &stages)
		cancelOrchestrate()
		if orchestrateErr != nil {
			t.Fatalf("instance-binding unbound predecessor capture refused: %v", orchestrateErr)
		}
		if b.fixture.bound || b.fixture.instanceID != "" || b.fixture.run.Binding().OriginalInstanceID() != "" {
			t.Fatal("unbound predecessor fixture is not unbound")
		}
		substitutedID := uuid.NewString()
		attemptID := "instance-binding-substitution-" + b.fixture.operation
		attempt, prepErr := newBorrowedReplacementPrelaunchAttemptWith(t, ctx, b, fresh,
			borrowedReplacementPrelaunchArchive(t, b.fixture), attemptID, &probeCalls, &acceptanceCalls,
			&borrowedReplacementPrelaunchBoundOptions{InstanceID: substitutedID,
				Prelaunch: borrowedAuthFixtureRestoreStartedMarker(attemptID, b.fixture.adminRole)})
		if prepErr != nil {
			t.Fatalf("instance-binding substitution attempt preparation refused: %v", prepErr)
		}
		if got := attempt.run.Binding().OriginalInstanceID(); got != substitutedID {
			t.Fatalf("substituted attempt run did not capture its own options id: %q != %q", got, substitutedID)
		}
		if err := attempt.registerOrigin(ctx); err != nil {
			t.Fatalf("instance-binding substitution origin registration refused: %v", err)
		}
		dispatchCtx, cancelDispatch := context.WithTimeout(ctx, 60*time.Second)
		_, _, dispatchErr := attempt.dispatchRun(dispatchCtx)
		cancelDispatch()
		if dispatchErr == nil || !strings.Contains(dispatchErr.Error(), "instance target binding is not usable") {
			t.Fatalf("substituted unbound receipt was not refused at the instance binding: %v", dispatchErr)
		}
		if b.fixture.run.Binding().OriginalInstanceID() != "" {
			t.Fatal("options-id substitution promoted the old unbound receipt")
		}
		// B2a: the UNCHANGED authentic capture validator, exercised with
		// GENUINE captures whose earlier provenance fields match, isolates an
		// InstanceID substitution using a REAL usable candidate: the lane's
		// authentic open bound instance (persisted open state asserted here,
		// never a nonexistent random id).
		candidateID := f.instanceID
		var candidateState, candidatePersistedKey string
		candidateReadCtx, cancelCandidateRead := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
		candidateReadErr := f.controlPool.QueryRow(candidateReadCtx, `SELECT state, target_guard_key FROM recovery_instance WHERE instance_id=$1`, candidateID).Scan(&candidateState, &candidatePersistedKey)
		cancelCandidateRead()
		if candidateReadErr != nil || candidateState != "open" || candidatePersistedKey != f.guardKey {
			t.Fatalf("instance-binding substitution candidate is not a real open usable instance: state=%q key=%q err=%v", candidateState, candidatePersistedKey, candidateReadErr)
		}
		originalCapture := b.fixture.run.Binding()
		plainCapture, plainErr := borrowedInstanceBindingFreshCapture(t, ctx, b.fixture, "")
		if plainErr != nil {
			t.Fatalf("instance-binding identical-field fresh capture refused: %v", plainErr)
		}
		if compareErr := borrowedReplacementCompareBindings(originalCapture, plainCapture, plainCapture.TargetDatabaseOID()); compareErr != nil {
			t.Fatalf("instance-binding identical-field genuine captures did not match: %v", compareErr)
		}
		candidateCapture, candidateErr := borrowedInstanceBindingFreshCapture(t, ctx, b.fixture, candidateID)
		if candidateErr != nil {
			t.Fatalf("instance-binding candidate fresh capture refused: %v", candidateErr)
		}
		if compareErr := borrowedReplacementCompareBindings(originalCapture, candidateCapture, candidateCapture.TargetDatabaseOID()); compareErr == nil || !strings.Contains(compareErr.Error(), "logical target identity changed") {
			t.Fatalf("authentic capture validator did not isolate the real-candidate InstanceID substitution: %v", compareErr)
		}
		// Empty/unbound control remains unbound with no instance identity.
		unbound := newBorrowedAuthHandoffFixture(t)
		if unbound.bound || unbound.instanceID != "" || unbound.run.Binding().OriginalInstanceID() != "" {
			t.Fatal("default unbound fixture gained an instance identity")
		}
		if after := borrowedInstanceBindingGeneration(t, ctx, f.controlPool, f.instanceID); after != generationBefore {
			t.Fatalf("instance-binding id negatives changed the generation: %d -> %d", generationBefore, after)
		}
		if disposition := borrowedReplacementPrelaunchGuardDisposition(t, ctx, f); disposition != "clean" {
			t.Fatalf("instance-binding id negatives changed the clean guard: %q", disposition)
		}
		t.Logf("instance-binding id negatives: nonexistent, wrong-role and foreign instance ids refused at the instance binding with no child/probe/acceptance, the unknown-candidate dispatch substitution could not promote the unbound predecessor receipt, the UNCHANGED authentic capture validator isolated a real-candidate InstanceID substitution whose earlier provenance fields matched, and the empty/unbound control stayed unbound")
	}()

	// N3: invalid-inventory input refusals and the inconsistent-guard open
	// refusal (distinct from a target mismatch) through the authentic open path.
	func() {
		f := newBorrowedAuthHandoffFixtureBound(t, &borrowedAuthHandoffBoundOptions{
			Kind: "recovery", OpenedBy: "deploy:instance-binding-inventory", Reason: "inventory control fixture",
		})
		targetTarget, targetErr := controlstore.ParseDSNTarget(f.writerTargetDSN)
		if targetErr != nil {
			t.Fatalf("instance-binding inventory target identity refused: %v", targetErr)
		}
		key, keyErr := recovery.CanonicalTargetKey(targetTarget)
		if keyErr != nil {
			t.Fatalf("instance-binding inventory target key refused: %v", keyErr)
		}
		store, storeErr := controlstore.NewStore(ctx, f.controlPool)
		if storeErr != nil {
			t.Fatalf("instance-binding inventory store refused: %v", storeErr)
		}
		if _, inventoryErr := store.OpenInstance(ctx, controlstore.OpenInstanceRequest{
			Kind: "recovery", OpenedBy: "deploy:instance-binding-inventory", Reason: "invalid inventory control",
			EntryChainInventory: []uint64{0}, TargetGuardKey: key.String(),
			TargetRoleFingerprint: targetTarget.DataTargetFingerprint().RoleFingerprint,
		}); inventoryErr == nil {
			t.Fatal("instance-binding absent/zero inventory was accepted")
		}
		if _, descendingErr := store.OpenInstance(ctx, controlstore.OpenInstanceRequest{
			Kind: "recovery", OpenedBy: "deploy:instance-binding-inventory", Reason: "descending inventory control",
			EntryChainInventory: []uint64{2, 1}, TargetGuardKey: key.String(),
			TargetRoleFingerprint: targetTarget.DataTargetFingerprint().RoleFingerprint,
		}); descendingErr == nil {
			t.Fatal("instance-binding descending inventory was accepted")
		}
		// The INCONSISTENT-GUARD open must run where the single-open invariant
		// is still free: a fresh unbound fixture control store.
		inconsistentGuardFixture := newBorrowedAuthHandoffFixture(t)
		inconsistentGuardStore, inconsistentGuardStoreErr := controlstore.NewStore(ctx, inconsistentGuardFixture.controlPool)
		if inconsistentGuardStoreErr != nil {
			t.Fatalf("instance-binding inconsistent-guard store refused: %v", inconsistentGuardStoreErr)
		}
		// A deliberately INCONSISTENT guard for the foreign key (launch intent
		// without an attempt app name) must refuse the authentic open through
		// the guard consistency check, never normalize it.
		foreignKey := "sha256:" + strings.Repeat("b", 64)
		insertedInconsistent := false
		for _, column := range []string{"clean_at", "launched_at", "rebuild_required_at"} {
			insertCtx, cancelInsert := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
			_, insertErr := inconsistentGuardFixture.controlPool.Exec(insertCtx, `
INSERT INTO recovery_target_guard (target_guard_key, disposition, operation_id, `+column+`)
VALUES ($1, 'unknown', $2, now())`, foreignKey, "instance-binding-inconsistent-"+inconsistentGuardFixture.operation)
			cancelInsert()
			if insertErr == nil {
				insertedInconsistent = true
				break
			}
		}
		if !insertedInconsistent {
			t.Fatal("instance-binding inconsistent guard insert refused for every lifecycle column")
		}
		if _, inconsistentGuardErr := inconsistentGuardStore.OpenInstance(ctx, controlstore.OpenInstanceRequest{
			Kind: "recovery", OpenedBy: "deploy:instance-binding-inventory", Reason: "inconsistent guard control",
			EntryChainInventory: []uint64{1}, TargetGuardKey: foreignKey,
			TargetRoleFingerprint: targetTarget.DataTargetFingerprint().RoleFingerprint,
		}); inconsistentGuardErr == nil || !strings.Contains(inconsistentGuardErr.Error(), "dirty, active, or inconsistent") {
			t.Fatalf("instance-binding inconsistent-guard open was not refused by the guard consistency check: %v", inconsistentGuardErr)
		}
		if acceptance := atomic.LoadInt32(&f.acceptanceCalls); acceptance != 0 {
			t.Fatalf("instance-binding inventory controls ran acceptance %d times", acceptance)
		}
		t.Logf("instance-binding inventory controls: absent/zero and descending inventories refused as invalid input, and the INCONSISTENT-GUARD open (distinct from a target mismatch) was refused through the guard consistency check; no EntryChainBound credit is claimed by this lane")
	}()

	// N4 (B1): error-returning bound-construction controls: REAL absent and
	// unknown guard rows are refused by the REAL validator; a failed/uncertain
	// bootstrap release returns nil/error BEFORE any lineage publication. The
	// injected release uncertainty is labelled and never credited as an
	// observed PG failure.
	func() {
		if nilFixture, nilErr := newBorrowedAuthHandoffFixtureBoundResult(t, nil); nilErr == nil || nilFixture != nil {
			t.Fatalf("bound result constructor accepted nil options: fixture=%v err=%v", nilFixture, nilErr)
		}
		if absentFixture, absentErr := newBorrowedAuthHandoffFixtureBoundResult(t, &borrowedAuthHandoffBoundOptions{
			Kind: "recovery", OpenedBy: "deploy:instance-binding-absent", Reason: "absent guard control",
			GuardInitMode: "absent",
		}); absentErr == nil || absentFixture != nil || !strings.Contains(absentErr.Error(), "absent") {
			t.Fatalf("bound construction with an absent guard row was not refused with a nil fixture: fixture=%v err=%v", absentFixture, absentErr)
		}
		if unknownFixture, unknownErr := newBorrowedAuthHandoffFixtureBoundResult(t, &borrowedAuthHandoffBoundOptions{
			Kind: "recovery", OpenedBy: "deploy:instance-binding-unknown", Reason: "unknown guard control",
			GuardInitMode: "unknown",
		}); unknownErr == nil || unknownFixture != nil || !strings.Contains(unknownErr.Error(), "exact pre-existing fixture-initialized clean guard") {
			t.Fatalf("bound construction with a real unknown guard row was not refused with a nil fixture: fixture=%v err=%v", unknownFixture, unknownErr)
		}
		if releaseFixture, releaseErr := newBorrowedAuthHandoffFixtureBoundResult(t, &borrowedAuthHandoffBoundOptions{
			Kind: "recovery", OpenedBy: "deploy:instance-binding-release", Reason: "release uncertainty control",
			InjectBootstrapReleaseError: errors.New("drill-injected release uncertainty"),
		}); releaseErr == nil || releaseFixture != nil || !strings.Contains(releaseErr.Error(), "release was not confirmed") || !strings.Contains(releaseErr.Error(), "drill-injected") {
			t.Fatalf("bound construction with an uncertain bootstrap release did not refuse with a nil fixture: fixture=%v err=%v", releaseFixture, releaseErr)
		}
		t.Logf("instance-binding constructor controls: REAL absent and unknown guard rows were refused by the REAL exact validator and a labelled drill-injected bootstrap-release uncertainty returned nil fixture/error BEFORE the retained owner acquisition and factory publication (never credited as an observed PG failure)")
	}()

	if got := atomic.LoadInt32(&probeCalls); got != 0 {
		t.Fatalf("instance-binding lane ran the ordinary probe %d times, want 0", got)
	}
	if got := atomic.LoadInt32(&acceptanceCalls); got != 0 {
		t.Fatalf("instance-binding lane ran ordinary acceptance %d times, want 0", got)
	}
}
