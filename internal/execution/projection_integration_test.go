//go:build integration

// projection_integration_test.go executes quickstart V9 (T037): the display
// projection moves only forward, an older input affects zero rows, authority
// unavailability marks possibly_stale and a successful read clears it, the
// decision paths never read the projection (static scan + runtime), and a
// forced refresh is audited. No display SLA is asserted anywhere (C11 stays
// unspecified). The joint projection/revision propagation is 010:T049 and is
// NOT claimed here; the double used is test-only (C10).
package execution

import (
	"errors"
	"os"
	"strings"
	"testing"
)

func TestProjectionV9VersionMonotonic(t *testing.T) {
	ctx, pool := executionPool(t)
	seedFullAdmission(t, ctx, pool, "req-v9", "authz-v9", "intent-v9")
	if out, err := Admit(ctx, pool, "req-v9", 1); err != nil || out.Status != AdmissionCreated {
		t.Fatalf("Admit = %+v (err %v), want created", out, err)
	}

	moved, err := applyLifecycleProjection(ctx, pool, "req-v9", "attempt-new", 5)
	if err != nil || !moved {
		t.Fatalf("newer lifecycle apply moved=%v err=%v, want true", moved, err)
	}
	moved, err = applyLifecycleProjection(ctx, pool, "req-v9", "attempt-old", 3)
	if err != nil || moved {
		t.Fatalf("older lifecycle apply moved=%v err=%v, want zero rows", moved, err)
	}
	row, found, err := ReadProjection(ctx, pool, "req-v9")
	if err != nil || !found {
		t.Fatalf("ReadProjection found=%v err=%v", found, err)
	}
	if row.LifecycleVersion != 5 || row.LifecycleAttemptID == nil || *row.LifecycleAttemptID != "attempt-new" {
		t.Fatalf("payload attempt ref = %v version=%d, want attempt-new/5", row.LifecycleAttemptID, row.LifecycleVersion)
	}

	moved, err = applyStateProjection(ctx, pool, "req-v9", IntentExecuting, 4)
	if err != nil || !moved {
		t.Fatalf("newer state apply moved=%v err=%v, want true", moved, err)
	}
	moved, err = applyStateProjection(ctx, pool, "req-v9", IntentClaimed, 2)
	if err != nil || moved {
		t.Fatalf("older state apply moved=%v err=%v, want zero rows", moved, err)
	}
	row, _, _ = ReadProjection(ctx, pool, "req-v9")
	if row.ExecutionState != IntentExecuting || row.StateVersion != 4 {
		t.Fatalf("state stream = %s/%d, want executing/4", row.ExecutionState, row.StateVersion)
	}
}

func TestProjectionV9StaleClearedBySuccessfulRead(t *testing.T) {
	ctx, pool := executionPool(t)
	seedFullAdmission(t, ctx, pool, "req-v9-stale", "authz-v9-stale", "intent-v9-stale")
	if out, err := Admit(ctx, pool, "req-v9-stale", 1); err != nil || out.Status != AdmissionCreated {
		t.Fatalf("Admit = %+v (err %v), want created", out, err)
	}

	if err := markProjectionStale(ctx, pool, "req-v9-stale"); err != nil {
		t.Fatalf("markProjectionStale: %v", err)
	}
	row, _, _ := ReadProjection(ctx, pool, "req-v9-stale")
	if row.Freshness != FreshnessPossiblyStale || row.StaleSince == nil {
		t.Fatalf("stale marking = %s stale_since=%v, want possibly_stale", row.Freshness, row.StaleSince)
	}

	if _, err := applyLifecycleProjection(ctx, pool, "req-v9-stale", "attempt-1", 7); err != nil {
		t.Fatalf("recovery apply: %v", err)
	}
	row, _, _ = ReadProjection(ctx, pool, "req-v9-stale")
	if row.Freshness != FreshnessConfirmed || row.StaleSince != nil {
		t.Fatalf("recovery freshness = %s stale_since=%v, want confirmed", row.Freshness, row.StaleSince)
	}

	// An equal-version successful read at version >= stored also clears.
	if err := markProjectionStale(ctx, pool, "req-v9-stale"); err != nil {
		t.Fatalf("re-mark stale: %v", err)
	}
	if _, err := applyLifecycleProjection(ctx, pool, "req-v9-stale", "attempt-1", 7); err != nil {
		t.Fatalf("equal-version apply: %v", err)
	}
	row, _, _ = ReadProjection(ctx, pool, "req-v9-stale")
	if row.Freshness != FreshnessConfirmed || row.LifecycleVersion != 7 {
		t.Fatalf("equal-version read = %s/%d, want confirmed/7", row.Freshness, row.LifecycleVersion)
	}

	// An older read never clears staleness.
	if err := markProjectionStale(ctx, pool, "req-v9-stale"); err != nil {
		t.Fatalf("re-mark stale 2: %v", err)
	}
	if moved, err := applyLifecycleProjection(ctx, pool, "req-v9-stale", "attempt-old", 6); err != nil || moved {
		t.Fatalf("older read moved=%v err=%v, want zero rows (must not clear)", moved, err)
	}
	row, _, _ = ReadProjection(ctx, pool, "req-v9-stale")
	if row.Freshness != FreshnessPossiblyStale {
		t.Fatalf("older read freshness = %s, want possibly_stale preserved", row.Freshness)
	}
}

// TestProjectionV9DecisionPathsNeverRead statically asserts the decision paths
// carry no reference to the display table (T034/T037).
func TestProjectionV9DecisionPathsNeverRead(t *testing.T) {
	for _, name := range []string{"claim.go", "advance.go", "reconcile.go", "revision.go"} {
		raw, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if strings.Contains(string(raw), "request_status_projection") {
			t.Errorf("decision path %s references request_status_projection", name)
		}
	}
}

// TestProjectionV9RuntimeDecisionSeparation poisons the display row and proves
// the send and reconcile decision paths neither read nor obey it.
func TestProjectionV9RuntimeDecisionSeparation(t *testing.T) {
	ctx, pool := executionPool(t)
	store := execClaimStore(t, pool)
	double := newLifecycleDouble()
	driver := &StepDriver{Pool: pool, Claims: store, Binding: bindingDouble{result: BindingMatches}, Advancer: double}
	version := seedExecClaimedIntent(t, ctx, pool, "req-v9-rt", "authz-v9-rt", "intent-v9-rt", "owner-a", store)

	if _, err := pool.Exec(ctx, `UPDATE request_status_projection
		SET execution_state = 'failed', state_version = 999, lifecycle_version = 999,
		    freshness = 'possibly_stale', stale_since = now()
		WHERE request_id = 'req-v9-rt'`); err != nil {
		t.Fatalf("poison projection: %v", err)
	}

	out, err := driver.IssueAndAdvance(ctx, StepRequest{
		IntentID: "intent-v9-rt", RequestID: "req-v9-rt", CallerID: 1,
		OwnerID: "owner-a", LeaseVersion: version, Action: ActionFirstBroadcast,
	})
	if err != nil || out.FinalStepState != StepConverged {
		t.Fatalf("issue under poisoned projection = %+v (err %v), want converged", out, err)
	}
	if got := intentState(t, ctx, pool, "intent-v9-rt"); got != IntentExecuting {
		t.Fatalf("intent state = %s, want executing (projection 'failed' must not be decisive)", got)
	}
	var poisonedState string
	var poisonedVersion int64
	if err := pool.QueryRow(ctx, `SELECT execution_state, state_version FROM request_status_projection
		WHERE request_id = 'req-v9-rt'`).Scan(&poisonedState, &poisonedVersion); err != nil {
		t.Fatalf("read poisoned projection: %v", err)
	}
	if poisonedState != IntentFailed || poisonedVersion != 999 {
		t.Fatalf("projection changed by decision = %s/%d, want untouched failed/999", poisonedState, poisonedVersion)
	}

	double.readFacts["intent-v9-rt"] = LifecycleFacts{
		CurrentAttemptID: out.AttemptID,
		Attempts:         []AttemptRef{{AttemptID: out.AttemptID, State: "confirmed"}},
		RevisionVersion:  5,
	}
	rec := &Reconciler{Pool: pool, Reader: double}
	if _, err := rec.ReconcileIntent(ctx, "intent-v9-rt", "owner-a", version); err != nil {
		t.Fatalf("reconcile under poisoned projection: %v", err)
	}
	if got := intentState(t, ctx, pool, "intent-v9-rt"); got != IntentCompleted {
		t.Fatalf("intent state = %s, want completed (reconcile must not read the projection)", got)
	}
}

// TestProjectionV9ForcedRefresh covers the forced-refresh mechanism: an applied
// version-guarded update and the unavailable-authority refusal that marks the
// row possibly_stale with zero business-state writes. No timing assertion is
// made (no display SLA, C11).
func TestProjectionV9ForcedRefresh(t *testing.T) {
	ctx, pool := executionPool(t)
	seedFullAdmission(t, ctx, pool, "req-v9-ref", "authz-v9-ref", "intent-v9-ref")
	if out, err := Admit(ctx, pool, "req-v9-ref", 1); err != nil || out.Status != AdmissionCreated {
		t.Fatalf("Admit = %+v (err %v), want created", out, err)
	}
	double := newLifecycleDouble()
	double.readFacts["intent-v9-ref"] = LifecycleFacts{CurrentAttemptID: "attempt-ref", RevisionVersion: 9}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin applied refresh: %v", err)
	}
	res, err := RefreshProjection(ctx, tx, double, "req-v9-ref")
	if err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("RefreshProjection applied: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit applied refresh: %v", err)
	}
	if res.Outcome != ProjectionRefreshApplied || res.RevisionVersion != 9 {
		t.Fatalf("refresh = %+v, want applied/9", res)
	}
	row, _, _ := ReadProjection(ctx, pool, "req-v9-ref")
	if row.Freshness != FreshnessConfirmed || row.LifecycleVersion != 9 {
		t.Fatalf("projection after refresh = %s/%d, want confirmed/9", row.Freshness, row.LifecycleVersion)
	}
	var refreshed int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM execution_events
		WHERE intent_id = 'intent-v9-ref' AND kind = 'projection_refreshed'`).Scan(&refreshed); err != nil || refreshed != 1 {
		t.Fatalf("projection_refreshed events = %d (err %v), want 1", refreshed, err)
	}

	bad := newLifecycleDouble()
	bad.readErr = errors.New("authority unavailable")
	tx2, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin refused refresh: %v", err)
	}
	res2, err := RefreshProjection(ctx, tx2, bad, "req-v9-ref")
	if err != nil {
		_ = tx2.Rollback(ctx)
		t.Fatalf("RefreshProjection refused: %v", err)
	}
	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("commit refused refresh: %v", err)
	}
	if res2.Outcome != ProjectionRefreshRefused || res2.Basis != string(ClassLifecycleUnavailable) {
		t.Fatalf("refused refresh = %+v, want refused/lifecycle_unavailable", res2)
	}
	row, _, _ = ReadProjection(ctx, pool, "req-v9-ref")
	if row.Freshness != FreshnessPossiblyStale || row.StaleSince == nil {
		t.Fatalf("projection after refusal = %s stale_since=%v, want possibly_stale", row.Freshness, row.StaleSince)
	}
	if row.LifecycleVersion != 9 {
		t.Fatalf("refusal rewrote the stored version to %d, want 9 unchanged", row.LifecycleVersion)
	}
	if got := intentState(t, ctx, pool, "intent-v9-ref"); got != IntentAdmitted {
		t.Fatalf("business state changed to %s on refusal, want admitted (zero business writes)", got)
	}
}
