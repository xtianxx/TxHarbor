//go:build integration

// revision_integration_test.go executes quickstart V10 (T040) for US6:
// authority revision consumption (completed→revised) keeps the same intent and
// history with no compensation, needs no claim and consumes no authorization
// (M2), an older revision affects zero rows, a send after revision re-runs the
// full gate set, and a failed payment is never auto-repaid. It also carries the
// T039 freeze-consumer evidence: the 010 freeze class is recorded, sends are
// refused while frozen, and reconcile observes without resetting or converting.
// The joint reorg-revision run is 010:T049 and is NOT claimed here.
package execution

import (
	"context"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestRevisionV10CompletedToRevised(t *testing.T) {
	ctx, pool := executionPool(t)
	store := execClaimStore(t, pool)
	double := newLifecycleDouble()
	driver := &StepDriver{Pool: pool, Claims: store, Binding: bindingDouble{result: BindingMatches}, Advancer: double}
	version := seedExecClaimedIntent(t, ctx, pool, "req-v10", "authz-v10", "intent-v10", "owner-a", store)

	out, err := driver.IssueAndAdvance(ctx, StepRequest{
		IntentID: "intent-v10", RequestID: "req-v10", CallerID: 1,
		OwnerID: "owner-a", LeaseVersion: version, Action: ActionFirstBroadcast,
	})
	if err != nil || out.FinalStepState != StepConverged {
		t.Fatalf("first broadcast = %+v (err %v), want converged", out, err)
	}
	double.readFacts["intent-v10"] = LifecycleFacts{
		CurrentAttemptID: out.AttemptID,
		Attempts:         []AttemptRef{{AttemptID: out.AttemptID, State: "confirmed"}},
		RevisionVersion:  1,
	}
	rec := &Reconciler{Pool: pool, Reader: double}
	if _, err := rec.ReconcileIntent(ctx, "intent-v10", "owner-a", version); err != nil {
		t.Fatalf("reconcile to completed: %v", err)
	}
	if got := intentState(t, ctx, pool, "intent-v10"); got != IntentCompleted {
		t.Fatalf("intent state = %s, want completed before revision", got)
	}

	if err := store.Release(ctx, "intent-v10", "owner-a", version); err != nil {
		t.Fatalf("release claim: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE withdrawal_authorizations SET state = 'revoked' WHERE authorization_id = 'authz-v10'`); err != nil {
		t.Fatalf("revoke grant: %v", err)
	}

	consumer := &RevisionConsumer{Pool: pool}
	res, err := consumer.Apply(ctx, RevisionFact{
		IntentID: "intent-v10", RevisionVersion: 2, AttemptID: out.AttemptID,
		TargetState: IntentRevised, Basis: "receipt_invalidated",
	})
	if err != nil {
		t.Fatalf("revision Apply: %v", err)
	}
	if !res.Applied || res.FromState != IntentCompleted || res.ToState != IntentRevised {
		t.Fatalf("revision result = %+v, want completed->revised applied (no claim, no authorization)", res)
	}
	if got := countIntents(t, ctx, pool, "req-v10"); got != 1 {
		t.Fatalf("intent rows = %d, want 1 (no second intent, no compensation payment)", got)
	}
	if got := revisionAppliedCount(t, ctx, pool, "intent-v10"); got != 1 {
		t.Fatalf("revision_applied events = %d, want 1", got)
	}
	row, _, _ := ReadProjection(ctx, pool, "req-v10")
	if row.LifecycleVersion != 2 {
		t.Fatalf("projection lifecycle_version = %d, want 2", row.LifecycleVersion)
	}

	older, err := consumer.Apply(ctx, RevisionFact{
		IntentID: "intent-v10", RevisionVersion: 1, TargetState: IntentRevised, Basis: "stale",
	})
	if err != nil || older.Applied {
		t.Fatalf("older revision = %+v (err %v), want zero rows", older, err)
	}
	if got := intentState(t, ctx, pool, "intent-v10"); got != IntentRevised {
		t.Fatalf("intent state = %s, want revised unchanged", got)
	}
	if got := revisionAppliedCount(t, ctx, pool, "intent-v10"); got != 1 {
		t.Fatalf("revision_applied events = %d after older input, want 1", got)
	}

	if _, err := pool.Exec(ctx, `UPDATE withdrawal_authorizations SET state = 'active' WHERE authorization_id = 'authz-v10'`); err != nil {
		t.Fatalf("reactivate grant: %v", err)
	}

	before := double.totalCalls()
	refused, err := driver.IssueAndAdvance(ctx, StepRequest{
		IntentID: "intent-v10", RequestID: "req-v10", CallerID: 1,
		OwnerID: "owner-a", LeaseVersion: version, Action: ActionFirstBroadcast,
	})
	if err != nil {
		t.Fatalf("send without a claim: %v", err)
	}
	if refused.Refusal != ClassClaimNotCurrent {
		t.Fatalf("send without a claim = %+v, want claim_not_current", refused)
	}
	if double.totalCalls() != before {
		t.Fatal("send without a current claim reached the 010 boundary")
	}
	if got := intentState(t, ctx, pool, "intent-v10"); got != IntentRevised {
		t.Fatalf("intent state = %s after refused send, want revised", got)
	}

	reclaim, err := store.Claim(ctx, "intent-v10", "owner-b")
	if err != nil || !reclaim.Acquired {
		t.Fatalf("re-claim = %+v (err %v), want acquired", reclaim, err)
	}
	next, err := driver.IssueAndAdvance(ctx, StepRequest{
		IntentID: "intent-v10", RequestID: "req-v10", CallerID: 1,
		OwnerID: "owner-b", LeaseVersion: reclaim.Version, Action: ActionFirstBroadcast,
	})
	if err != nil || next.FinalStepState != StepConverged {
		t.Fatalf("send after revision = %+v (err %v), want converged under full gates + claim", next, err)
	}
	if got := intentState(t, ctx, pool, "intent-v10"); got != IntentExecuting {
		t.Fatalf("intent state = %s, want executing after the gated revised->executing edge", got)
	}
	if got := countIntents(t, ctx, pool, "req-v10"); got != 1 {
		t.Fatalf("intent rows = %d after resume, want 1", got)
	}
}

func TestRevisionV10FailedNeverAutoRepaid(t *testing.T) {
	ctx, pool := executionPool(t)
	store := execClaimStore(t, pool)
	double := newLifecycleDouble()
	driver := &StepDriver{Pool: pool, Claims: store, Binding: bindingDouble{result: BindingMatches}, Advancer: double}
	version := seedExecClaimedIntent(t, ctx, pool, "req-v10f", "authz-v10f", "intent-v10f", "owner-a", store)

	if out, err := driver.IssueAndAdvance(ctx, StepRequest{
		IntentID: "intent-v10f", RequestID: "req-v10f", CallerID: 1,
		OwnerID: "owner-a", LeaseVersion: version, Action: ActionFirstBroadcast,
	}); err != nil || out.FinalStepState != StepConverged {
		t.Fatalf("setup broadcast = %+v (err %v)", out, err)
	}
	intent, _, _ := ReadIntent(ctx, pool, "intent-v10f")
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin failed transition: %v", err)
	}
	if err := TransitionIntent(ctx, tx, "intent-v10f", intent.State, intent.StateVersion, IntentFailed, version); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("authority failed transition: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit failed transition: %v", err)
	}
	if ClassifyTransition(IntentFailed, IntentExecuting) != TransitionNone {
		t.Fatal("failed->executing must not be a legal edge (no auto-repay)")
	}

	stepsBefore := countIntentSteps(t, ctx, pool, "intent-v10f")
	if _, err := driver.IssueAndAdvance(ctx, StepRequest{
		IntentID: "intent-v10f", RequestID: "req-v10f", CallerID: 1,
		OwnerID: "owner-a", LeaseVersion: version, Action: ActionFirstBroadcast,
	}); err == nil {
		t.Fatal("a failed intent issued a send-class step; failed must never auto-repay")
	}
	if got := countIntentSteps(t, ctx, pool, "intent-v10f"); got != stepsBefore {
		t.Fatalf("steps = %d, want %d unchanged", got, stepsBefore)
	}

	double.readFacts["intent-v10f"] = LifecycleFacts{}
	rec := &Reconciler{Pool: pool, Reader: double}
	if _, err := rec.ReconcileIntent(ctx, "intent-v10f", "owner-a", version); err != nil {
		t.Fatalf("reconcile failed intent: %v", err)
	}
	if got := intentState(t, ctx, pool, "intent-v10f"); got != IntentFailed {
		t.Fatalf("intent state = %s, want failed (reconcile never repays)", got)
	}
}

func TestFreezeConsumerRefusesSendAndSurfacesEvidence(t *testing.T) {
	ctx, pool := executionPool(t)
	store := execClaimStore(t, pool)
	double := newLifecycleDouble()
	driver := &StepDriver{Pool: pool, Claims: store, Binding: bindingDouble{result: BindingMatches}, Advancer: double}
	version := seedExecClaimedIntent(t, ctx, pool, "req-freeze", "authz-freeze", "intent-freeze", "owner-a", store)

	double.readFacts["intent-freeze"] = LifecycleFacts{
		CurrentAttemptID: "attempt-x",
		Unknown:          &UnknownRef{AttemptID: "attempt-x", RecoveryCondition: "lock_loss"},
	}
	rec := &Reconciler{Pool: pool, Reader: double}
	res, err := rec.ReconcileIntent(ctx, "intent-freeze", "owner-a", version)
	if err != nil || !res.Reconciling {
		t.Fatalf("frozen reconcile = %+v (err %v), want reconciling", res, err)
	}
	frozen, class, err := IsFrozen(ctx, pool, "intent-freeze")
	if err != nil || !frozen || class != FreezeLockLoss {
		t.Fatalf("freeze marker = %v/%q (err %v), want true/%s", frozen, class, err, FreezeLockLoss)
	}

	before := double.totalCalls()
	out, err := driver.IssueAndAdvance(ctx, StepRequest{
		IntentID: "intent-freeze", RequestID: "req-freeze", CallerID: 1,
		OwnerID: "owner-a", LeaseVersion: version, Action: ActionFirstBroadcast,
	})
	if err != nil {
		t.Fatalf("frozen send: %v", err)
	}
	if out.Refusal != "" || out.Basis != "frozen:"+FreezeLockLoss {
		t.Fatalf("frozen send = %+v, want a frozen refusal with zero sends", out)
	}
	if double.totalCalls() != before {
		t.Fatal("a frozen intent reached the 010 boundary")
	}

	res2, err := rec.ReconcileIntent(ctx, "intent-freeze", "owner-a", version)
	if err != nil || !res2.Reconciling {
		t.Fatalf("second frozen reconcile = %+v (err %v), want reconciling", res2, err)
	}
	stillFrozen, _, _ := IsFrozen(ctx, pool, "intent-freeze")
	if !stillFrozen {
		t.Fatal("reconcile reset the freeze marker; only the 010-side release may clear it")
	}
	if got := intentState(t, ctx, pool, "intent-freeze"); got == IntentCompleted || got == IntentFailed {
		t.Fatalf("frozen reconcile converted the condition to %s", got)
	}
}

func revisionAppliedCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, intentID string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM execution_events
		WHERE intent_id = $1 AND kind = 'revision_applied'`, intentID).Scan(&n); err != nil {
		t.Fatalf("count revision_applied: %v", err)
	}
	return n
}

func projectionRefreshedCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, intentID string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM execution_events
		WHERE intent_id = $1 AND kind = 'projection_refreshed'`, intentID).Scan(&n); err != nil {
		t.Fatalf("count projection_refreshed: %v", err)
	}
	return n
}

// completedIntentV10 drives the V10 setup up to `completed` and returns the
// attempt identity plus the pool-backed consumer.
func completedIntentV10(t *testing.T, requestID, intentID string) (*pgxpool.Pool, *RevisionConsumer, string) {
	t.Helper()
	ctx, pool := executionPool(t)
	store := execClaimStore(t, pool)
	double := newLifecycleDouble()
	driver := &StepDriver{Pool: pool, Claims: store, Binding: bindingDouble{result: BindingMatches}, Advancer: double}
	version := seedExecClaimedIntent(t, ctx, pool, requestID, "authz-"+intentID, intentID, "owner-a", store)
	out, err := driver.IssueAndAdvance(ctx, StepRequest{
		IntentID: intentID, RequestID: requestID, CallerID: 1,
		OwnerID: "owner-a", LeaseVersion: version, Action: ActionFirstBroadcast,
	})
	if err != nil || out.FinalStepState != StepConverged {
		t.Fatalf("setup broadcast = %+v (err %v)", out, err)
	}
	double.readFacts[intentID] = LifecycleFacts{
		CurrentAttemptID: out.AttemptID,
		Attempts:         []AttemptRef{{AttemptID: out.AttemptID, State: "confirmed"}},
		RevisionVersion:  1,
	}
	rec := &Reconciler{Pool: pool, Reader: double}
	if _, err := rec.ReconcileIntent(ctx, intentID, "owner-a", version); err != nil {
		t.Fatalf("reconcile to completed: %v", err)
	}
	if got := intentState(t, ctx, pool, intentID); got != IntentCompleted {
		t.Fatalf("intent state = %s, want completed before revisions", got)
	}
	return pool, &RevisionConsumer{Pool: pool}, out.AttemptID
}

// TestRevisionReconfirmAndProjectionOnlyVersions pins the Lane-F3/B2
// projection-consumption rules: a newer revision whose state edge is already
// applied (a reconfirm after completed->revised) and a newer version with no
// revision edge at all both advance the projection in order, exactly once,
// while older/equal versions and repeat observations affect zero rows and
// append no evidence; no state is rewritten and no revision is fabricated.
func TestRevisionReconfirmAndProjectionOnlyVersions(t *testing.T) {
	ctx := context.Background()
	pool, consumer, attemptID := completedIntentV10(t, "req-v10r", "intent-v10r")

	// The invalidation revision: completed -> revised, projection to 2.
	res, err := consumer.Apply(ctx, RevisionFact{
		IntentID: "intent-v10r", RevisionVersion: 2, AttemptID: attemptID,
		TargetState: IntentRevised, Basis: "receipt_invalidated",
	})
	if err != nil || !res.Applied {
		t.Fatalf("invalidation revision = %+v (err %v), want applied", res, err)
	}

	// A reconfirm revision (same target already applied): the version is
	// consumed into the projection, the state stays revised.
	res, err = consumer.Apply(ctx, RevisionFact{
		IntentID: "intent-v10r", RevisionVersion: 3, AttemptID: attemptID,
		TargetState: IntentRevised, Basis: "authority_reconfirmed",
	})
	if err != nil {
		t.Fatalf("reconfirm revision: %v", err)
	}
	if res.Applied {
		t.Fatalf("reconfirm revision = %+v, want no second state edge", res)
	}
	row, found, err := ReadProjection(ctx, pool, "req-v10r")
	if err != nil || !found || row.LifecycleVersion != 3 {
		t.Fatalf("projection after reconfirm = %+v found=%v err=%v, want lifecycle_version 3", row, found, err)
	}
	if got := revisionAppliedCount(t, ctx, pool, "intent-v10r"); got != 1 {
		t.Fatalf("revision_applied events = %d, want 1 (the reconfirm consumes no state edge)", got)
	}
	if got := projectionRefreshedCount(t, ctx, pool, "intent-v10r"); got != 1 {
		t.Fatalf("projection_refreshed events after reconfirm = %d, want 1 (the version was consumed)", got)
	}
	if got := intentState(t, ctx, pool, "intent-v10r"); got != IntentRevised {
		t.Fatalf("intent state = %s, want revised", got)
	}

	// A version with no revision edge: projection-only advance, exactly one
	// event, no revision_applied.
	res, err = consumer.Observe(ctx, RevisionFact{
		IntentID: "intent-v10r", RevisionVersion: 4, AttemptID: attemptID, Basis: "authority revision observed",
	})
	if err != nil {
		t.Fatalf("observe revision: %v", err)
	}
	if res.Applied {
		t.Fatalf("observe = %+v, want projection-only", res)
	}
	if got := projectionRefreshedCount(t, ctx, pool, "intent-v10r"); got != 2 {
		t.Fatalf("projection_refreshed events = %d, want 2", got)
	}
	if got := revisionAppliedCount(t, ctx, pool, "intent-v10r"); got != 1 {
		t.Fatalf("revision_applied events after observe = %d, want 1", got)
	}

	// Repeat and older versions: zero rows, zero new evidence.
	if _, err := consumer.Observe(ctx, RevisionFact{IntentID: "intent-v10r", RevisionVersion: 4, AttemptID: attemptID}); err != nil {
		t.Fatalf("repeat observe: %v", err)
	}
	if _, err := consumer.Observe(ctx, RevisionFact{IntentID: "intent-v10r", RevisionVersion: 3, AttemptID: attemptID}); err != nil {
		t.Fatalf("older observe: %v", err)
	}
	if got := projectionRefreshedCount(t, ctx, pool, "intent-v10r"); got != 2 {
		t.Fatalf("projection_refreshed events after repeats = %d, want 2 (idempotent)", got)
	}
	row, found, err = ReadProjection(ctx, pool, "req-v10r")
	if err != nil || !found || row.LifecycleVersion != 4 {
		t.Fatalf("projection after repeats = %+v found=%v err=%v, want lifecycle_version 4 (never rewritten downward)", row, found, err)
	}
}

// TestRevisionConcurrentConsumption pins the concurrency guards: simultaneous
// consumers of one revision version produce exactly one state edge and one
// consumed version, and simultaneous projection observers append exactly one
// event for the advanced version.
func TestRevisionConcurrentConsumption(t *testing.T) {
	ctx := context.Background()
	pool, _, attemptID := completedIntentV10(t, "req-v10c", "intent-v10c")
	consumer := &RevisionConsumer{Pool: pool}

	var wg sync.WaitGroup
	applied := make(chan bool, 4)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := consumer.Apply(ctx, RevisionFact{
				IntentID: "intent-v10c", RevisionVersion: 2, AttemptID: attemptID,
				TargetState: IntentRevised, Basis: "concurrent invalidation",
			})
			if err != nil {
				t.Errorf("concurrent apply: %v", err)
				return
			}
			applied <- res.Applied
		}()
	}
	wg.Wait()
	close(applied)
	var wins int
	for ok := range applied {
		if ok {
			wins++
		}
	}
	if wins != 1 {
		t.Fatalf("applied revisions = %d, want exactly 1", wins)
	}
	if got := revisionAppliedCount(t, ctx, pool, "intent-v10c"); got != 1 {
		t.Fatalf("revision_applied events = %d, want 1", got)
	}
	if got := intentState(t, ctx, pool, "intent-v10c"); got != IntentRevised {
		t.Fatalf("intent state = %s, want revised", got)
	}
	row, _, _ := ReadProjection(ctx, pool, "req-v10c")
	if row.LifecycleVersion != 2 {
		t.Fatalf("projection lifecycle_version = %d, want 2", row.LifecycleVersion)
	}

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := consumer.Observe(ctx, RevisionFact{
				IntentID: "intent-v10c", RevisionVersion: 3, AttemptID: attemptID, Basis: "concurrent observe",
			}); err != nil {
				t.Errorf("concurrent observe: %v", err)
			}
		}()
	}
	wg.Wait()
	if got := projectionRefreshedCount(t, ctx, pool, "intent-v10c"); got != 1 {
		t.Fatalf("projection_refreshed events = %d, want exactly 1", got)
	}
	row, _, _ = ReadProjection(ctx, pool, "req-v10c")
	if row.LifecycleVersion != 3 {
		t.Fatalf("projection lifecycle_version = %d, want 3", row.LifecycleVersion)
	}
}
