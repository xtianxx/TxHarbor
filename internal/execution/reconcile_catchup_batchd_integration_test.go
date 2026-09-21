//go:build integration

// Batch D 011 reconcile catch-up coverage on the labeled test-only
// lifecycleDouble (T030; never joint evidence):
//
//   - D2a: an exhausted driver leaves an open `issued` step with no attempt
//     identity recorded anywhere in 011; once 010's authority facts report the
//     single sent attempt for that same (intent_id, step_id), the reconciler
//     converges the step and completes the intent, and repeating the reconcile
//     is idempotent.
//   - D2b: the startup catch-up (ReconcileAllOpenSteps) reconciles every intent
//     that has an open step and leaves intents without one untouched; after a
//     step converges, a new logical step needs a new step_id, and a concurrent
//     second issue while a step is open is refused step_open_unreconciled with
//     zero writes.
package execution

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// countIntentEvents counts one intent's append-only execution_events rows.
func countIntentEvents(t *testing.T, ctx context.Context, pool *pgxpool.Pool, intentID string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM execution_events WHERE intent_id = $1`, intentID).Scan(&n); err != nil {
		t.Fatalf("count events: %v", err)
	}
	return n
}

// TestExhaustThenReconcileConvergesConfirmedSent closes the recovery gap for an
// exhausted step: bounded retries leave the step `issued`, and the reconciler
// must converge it from 010's positive sent fact.
//
// Defect: none known — coverage gap.
// Invariant: lifecycle.md §4/§5 and persistence.md §3.2: after the 010 call and
// before T-step-converge the step stays open and is reconciled from 010's
// facts; a sent/confirmed fact converges the step to sent and completes the
// intent (fact transition), never a second attempt or dispatch, and a repeated
// reconcile is idempotent (no rewrite of the terminal step or of known
// results).
// Gap: TestAdvanceExhaustedRetriesLeaveStepIssued stops at the issued step;
// TestReconcileCrashPoints covers unknown/no-attempt recovery, not the
// confirmed-sent convergence of an exhausted step.
// Level: integration (real PostgreSQL; labeled test-only lifecycle double).
// Criteria: after unavailableTimes=99 exhausts the driver, the step is issued
// with no recorded attempt; pointing the double's readFacts at one positive
// sent attempt (the same step key) makes ReconcileIntent converge the step to
// converged/sent on that single attempt_id and complete the intent, with
// exactly one execution_steps row and exactly one recorded attempt; a second
// ReconcileIntent takes the already-terminal path with zero new events and no
// step rewrite.
func TestExhaustThenReconcileConvergesConfirmedSent(t *testing.T) {
	ctx, pool := executionPool(t)
	store := execClaimStore(t, pool)
	double := newLifecycleDouble()
	double.unavailableTimes = 99
	driver := &StepDriver{Pool: pool, Claims: store, Binding: bindingDouble{result: BindingMatches}, Advancer: double}
	const intentID = "intent-d2a"
	version := seedExecClaimedIntent(t, ctx, pool, "req-d2a", "authz-d2a", intentID, "owner-a", store)

	out, err := driver.IssueAndAdvance(ctx, StepRequest{
		IntentID: intentID, RequestID: "req-d2a", CallerID: 1,
		OwnerID: "owner-a", LeaseVersion: version, Action: ActionFirstBroadcast,
	})
	if err != nil || out.FinalStepState != StepIssued || !out.Retryable || out.StepID == "" {
		t.Fatalf("exhausted retries = %+v (err %v), want issued/retryable with a step id", out, err)
	}
	double.mu.Lock()
	recorded := len(double.attempts)
	double.mu.Unlock()
	if recorded != 0 {
		t.Fatalf("exhausted retries recorded %d attempts, want 0", recorded)
	}
	if got := countIntentSteps(t, ctx, pool, intentID); got != 1 {
		t.Fatalf("execution_steps rows = %d, want 1", got)
	}

	// Model the crash window: 010 converged the attempt for the same
	// (intent_id, step_id) while 011's converge was lost, so the durable
	// authority fact is exactly one sent attempt for this step.
	double.mu.Lock()
	double.unavailableTimes = 0
	double.mu.Unlock()
	fact, err := double.Advance(ctx, AdvanceRequest{IntentID: intentID, StepID: out.StepID, Action: ActionFirstBroadcast})
	if err != nil || fact.Class != OutcomeSent || fact.AttemptID == "" {
		t.Fatalf("recorded 010 fact = %+v (err %v), want sent with an attempt id", fact, err)
	}
	double.mu.Lock()
	double.readFacts[intentID] = LifecycleFacts{
		CurrentAttemptID: fact.AttemptID,
		Attempts:         []AttemptRef{{AttemptID: fact.AttemptID, State: "sent"}},
		RevisionVersion:  fact.RevisionVersion,
	}
	double.mu.Unlock()

	rec := &Reconciler{Pool: pool, Reader: double}
	res, err := rec.ReconcileIntent(ctx, intentID, "owner-a", version)
	if err != nil || !res.Converged || res.FinalStepState != StepConverged || res.OutcomeClass != OutcomeSent {
		t.Fatalf("reconcile = %+v (err %v), want converged/sent", res, err)
	}
	state, outcome := stepState(t, ctx, pool, out.StepID)
	if state != StepConverged || outcome != OutcomeSent {
		t.Fatalf("step = %s/%s, want converged/sent", state, outcome)
	}
	var stepAttempt string
	if err := pool.QueryRow(ctx,
		`SELECT COALESCE(attempt_id, '') FROM execution_steps WHERE step_id = $1`, out.StepID).Scan(&stepAttempt); err != nil {
		t.Fatal(err)
	}
	if stepAttempt != fact.AttemptID {
		t.Fatalf("step attempt = %s, want the single recorded attempt %s", stepAttempt, fact.AttemptID)
	}
	if got := intentState(t, ctx, pool, intentID); got != IntentCompleted {
		t.Fatalf("intent state = %s, want completed", got)
	}
	double.mu.Lock()
	recorded = len(double.attempts)
	double.mu.Unlock()
	if recorded != 1 {
		t.Fatalf("recorded attempts = %d, want exactly 1", recorded)
	}
	if got := countIntentSteps(t, ctx, pool, intentID); got != 1 {
		t.Fatalf("execution_steps rows = %d, want exactly 1", got)
	}

	// Repeat reconcile: the already-terminal path rewrites nothing.
	eventsBefore := countIntentEvents(t, ctx, pool, intentID)
	var updatedBefore time.Time
	if err := pool.QueryRow(ctx,
		`SELECT updated_at FROM execution_steps WHERE step_id = $1`, out.StepID).Scan(&updatedBefore); err != nil {
		t.Fatal(err)
	}
	again, err := rec.ReconcileIntent(ctx, intentID, "owner-a", version)
	if err != nil {
		t.Fatalf("repeat reconcile: %v", err)
	}
	if again.Converged || again.FinalStepState != "" {
		t.Fatalf("repeat reconcile = %+v, want the already-terminal idempotent path", again)
	}
	if got := countIntentEvents(t, ctx, pool, intentID); got != eventsBefore {
		t.Fatalf("repeat reconcile appended %d events", got-eventsBefore)
	}
	var updatedAfter time.Time
	if err := pool.QueryRow(ctx,
		`SELECT updated_at FROM execution_steps WHERE step_id = $1`, out.StepID).Scan(&updatedAfter); err != nil {
		t.Fatal(err)
	}
	if !updatedAfter.Equal(updatedBefore) {
		t.Fatalf("repeat reconcile rewrote the step row: %s -> %s", updatedBefore, updatedAfter)
	}
	if got := intentState(t, ctx, pool, intentID); got != IntentCompleted {
		t.Fatalf("intent state after repeat = %s, want completed", got)
	}

	// Pin the already-terminal converge guard itself (reconcileConvergeSQL's
	// exact-state WHERE): a converge attempt against the terminal step matches
	// zero rows and appends no event, so a repeated/late reconcile can never
	// duplicate the sent convergence.
	step, found, err := ReadStep(ctx, pool, out.StepID)
	if err != nil || !found {
		t.Fatalf("read terminal step = %+v found=%v (err %v)", step, found, err)
	}
	intent, found, err := ReadIntent(ctx, pool, intentID)
	if err != nil || !found {
		t.Fatalf("read intent = %+v found=%v (err %v)", intent, found, err)
	}
	eventsBefore = countIntentEvents(t, ctx, pool, intentID)
	if err := rec.convergeOpenStep(ctx, intent, step, StepConverged, OutcomeSent,
		fact.AttemptID, "", fact.RevisionVersion, "idempotent-probe"); err != nil {
		t.Fatalf("terminal converge probe: %v", err)
	}
	if got := countIntentEvents(t, ctx, pool, intentID); got != eventsBefore {
		t.Fatalf("terminal converge probe appended %d events (RowsAffected must be 0)", got-eventsBefore)
	}
}

// TestReconcileAllOpenStepsCatchUp covers the startup catch-up across several
// intents and the structural one-open-step rule.
//
// Defect: none known — coverage gap.
// Invariant: persistence.md §5 / migration 000012: one open (issued) step per
// intent is structural (execution_steps_open_uniq), a new step while one is
// open is refused step_open_unreconciled with zero writes, and the startup
// catch-up reconciles every intent that has an open step without touching
// intents that do not. A new logical step must use a new step_id
// (lifecycle.md §4).
// Gap: ReconcileAllOpenSteps had no multi-intent test and the concurrent
// second-issue refusal was never pinned at the driver boundary.
// Level: integration (real PostgreSQL; labeled test-only lifecycle double).
// Criteria: two intents with exhausted issued steps plus one claimed intent
// without a step; ReconcileAllOpenSteps converges the two exhausted steps to
// sent (and completes their intents) while the third intent stays claimed with
// zero steps; a post-converge IssueAndAdvance converges with a NEW step_id;
// a further open step plus two concurrent second issues both refuse
// step_open_unreconciled with no step id and zero new step/event rows.
func TestReconcileAllOpenStepsCatchUp(t *testing.T) {
	ctx, pool := executionPool(t)
	store := execClaimStore(t, pool)
	double := newLifecycleDouble()
	double.unavailableTimes = 99
	driver := &StepDriver{Pool: pool, Claims: store, Binding: bindingDouble{result: BindingMatches}, Advancer: double}

	type catchIntent struct {
		intentID string
		stepID   string
		version  int64
	}
	seedExhausted := func(intentID string) catchIntent {
		t.Helper()
		version := seedExecClaimedIntent(t, ctx, pool, "req-"+intentID, "authz-"+intentID, intentID, "owner-a", store)
		out, err := driver.IssueAndAdvance(ctx, StepRequest{
			IntentID: intentID, RequestID: "req-" + intentID, CallerID: 1,
			OwnerID: "owner-a", LeaseVersion: version, Action: ActionFirstBroadcast,
		})
		if err != nil || out.FinalStepState != StepIssued || out.StepID == "" {
			t.Fatalf("exhausted %s = %+v (err %v), want issued with a step id", intentID, out, err)
		}
		return catchIntent{intentID: intentID, stepID: out.StepID, version: version}
	}
	a := seedExhausted("intent-catch-a")
	b := seedExhausted("intent-catch-b")

	// One intent without an open step: claimed, no execution_steps row.
	const cID = "intent-catch-c"
	cVersion := seedExecClaimedIntent(t, ctx, pool, "req-"+cID, "authz-"+cID, cID, "owner-a", store)

	double.mu.Lock()
	double.readFacts[a.intentID] = LifecycleFacts{
		CurrentAttemptID: "attempt-catch-a",
		Attempts:         []AttemptRef{{AttemptID: "attempt-catch-a", State: "sent"}},
		RevisionVersion:  1,
	}
	double.readFacts[b.intentID] = LifecycleFacts{
		CurrentAttemptID: "attempt-catch-b",
		Attempts:         []AttemptRef{{AttemptID: "attempt-catch-b", State: "sent"}},
		RevisionVersion:  1,
	}
	double.mu.Unlock()

	rec := &Reconciler{Pool: pool, Reader: double}
	if err := rec.ReconcileAllOpenSteps(ctx); err != nil {
		t.Fatalf("ReconcileAllOpenSteps: %v", err)
	}
	for _, s := range []catchIntent{a, b} {
		state, outcome := stepState(t, ctx, pool, s.stepID)
		if state != StepConverged || outcome != OutcomeSent {
			t.Fatalf("%s step = %s/%s, want converged/sent", s.intentID, state, outcome)
		}
		if got := intentState(t, ctx, pool, s.intentID); got != IntentCompleted {
			t.Fatalf("%s intent state = %s, want completed", s.intentID, got)
		}
	}
	if got := intentState(t, ctx, pool, cID); got != IntentClaimed {
		t.Fatalf("%s intent state = %s, want claimed (untouched by catch-up)", cID, got)
	}
	if got := countIntentSteps(t, ctx, pool, cID); got != 0 {
		t.Fatalf("%s steps = %d, want 0", cID, got)
	}

	// Post-converge: a fresh step under the current claim converges and must
	// carry a NEW step_id (a step id is never reused for a new logical step).
	double.mu.Lock()
	double.unavailableTimes = 0
	double.mu.Unlock()
	fresh, err := driver.IssueAndAdvance(ctx, StepRequest{
		IntentID: cID, RequestID: "req-" + cID, CallerID: 1,
		OwnerID: "owner-a", LeaseVersion: cVersion, Action: ActionFirstBroadcast,
	})
	if err != nil || fresh.FinalStepState != StepConverged || fresh.OutcomeClass != OutcomeSent {
		t.Fatalf("post-converge issue = %+v (err %v), want converged/sent", fresh, err)
	}
	if fresh.StepID == "" || fresh.StepID == a.stepID || fresh.StepID == b.stepID {
		t.Fatalf("post-converge step id = %q, want a NEW step id", fresh.StepID)
	}

	// A new open step (the double exhausts), then concurrent second issues
	// while it is open: the structural one-open-step rule refuses with zero
	// writes.
	double.mu.Lock()
	double.unavailableTimes = 99
	double.mu.Unlock()
	open, err := driver.IssueAndAdvance(ctx, StepRequest{
		IntentID: cID, RequestID: "req-" + cID, CallerID: 1,
		OwnerID: "owner-a", LeaseVersion: cVersion, Action: ActionFirstBroadcast,
	})
	if err != nil || open.FinalStepState != StepIssued || open.StepID == "" {
		t.Fatalf("open-step setup = %+v (err %v), want issued with a step id", open, err)
	}
	if open.StepID == fresh.StepID {
		t.Fatal("a new logical step reused the previous step_id")
	}

	stepsBefore := countIntentSteps(t, ctx, pool, cID)
	eventsBefore := countIntentEvents(t, ctx, pool, cID)
	type issueOutcome struct {
		out StepOutcome
		err error
	}
	results := make(chan issueOutcome, 2)
	for i := 0; i < 2; i++ {
		go func() {
			o, err := driver.IssueAndAdvance(ctx, StepRequest{
				IntentID: cID, RequestID: "req-" + cID, CallerID: 1,
				OwnerID: "owner-a", LeaseVersion: cVersion, Action: ActionFirstBroadcast,
			})
			results <- issueOutcome{out: o, err: err}
		}()
	}
	for i := 0; i < 2; i++ {
		r := <-results
		if r.err != nil {
			t.Fatalf("concurrent second issue: %v", r.err)
		}
		if r.out.Refusal != ClassStepOpenUnreconciled || r.out.StepID != "" {
			t.Fatalf("concurrent second issue = %+v, want %s with no step id",
				r.out, ClassStepOpenUnreconciled)
		}
	}
	if got := countIntentSteps(t, ctx, pool, cID); got != stepsBefore {
		t.Fatalf("refused second issue wrote a step: %d -> %d", stepsBefore, got)
	}
	if got := countIntentEvents(t, ctx, pool, cID); got != eventsBefore {
		t.Fatalf("refused second issue wrote events: %d -> %d", eventsBefore, got)
	}
}
