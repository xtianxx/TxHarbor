//go:build integration

// advance_integration_test.go executes quickstart V7 (T031) against the labeled
// test-only double: refused classes converge with evidence, pending_unknown
// becomes unknown/reconciling with zero failure claims, a retry with the same
// step_id never creates a second attempt, crash points reconcile the open step,
// and exhausted bounded retries leave the step issued. The joint reconcile run
// is 010:T048 and is NOT claimed here.
package execution

import (
	"errors"
	"testing"
)

func TestAdvanceRefusedAndUnknownOutcomes(t *testing.T) {
	ctx, pool := executionPool(t)
	store := execClaimStore(t, pool)
	double := newLifecycleDouble()
	driver := &StepDriver{Pool: pool, Claims: store, Binding: bindingDouble{result: BindingMatches}, Advancer: double}
	version := seedExecClaimedIntent(t, ctx, pool, "req-v7", "authz-v7", "intent-v7", "owner-a", store)
	req := StepRequest{IntentID: "intent-v7", RequestID: "req-v7", CallerID: 1, OwnerID: "owner-a", LeaseVersion: version, Action: ActionFirstBroadcast}

	double.resultClass = OutcomeRefusedBasis
	refused, err := driver.IssueAndAdvance(ctx, req)
	if err != nil || refused.FinalStepState != StepConverged || refused.OutcomeClass != OutcomeRefusedBasis {
		t.Fatalf("refused_basis = %+v (err %v)", refused, err)
	}

	double.resultClass = OutcomePendingUnknown
	unknown, err := driver.IssueAndAdvance(ctx, req)
	if err != nil || unknown.FinalStepState != StepUnknown || !unknown.Reconciling {
		t.Fatalf("pending_unknown = %+v (err %v)", unknown, err)
	}
	if got := intentState(t, ctx, pool, "intent-v7"); got != IntentReconciling {
		t.Fatalf("intent state = %s, want reconciling", got)
	}
	var failed int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM payment_intents WHERE state = 'failed'`).Scan(&failed); err != nil || failed != 0 {
		t.Fatalf("failed intents = %d (err %v), want 0", failed, err)
	}
}

func TestAdvanceBoundedRetrySameStep(t *testing.T) {
	ctx, pool := executionPool(t)
	store := execClaimStore(t, pool)
	double := newLifecycleDouble()
	double.unavailableTimes = 2
	driver := &StepDriver{Pool: pool, Claims: store, Binding: bindingDouble{result: BindingMatches}, Advancer: double}
	version := seedExecClaimedIntent(t, ctx, pool, "req-retry", "authz-retry", "intent-retry", "owner-a", store)
	req := StepRequest{IntentID: "intent-retry", RequestID: "req-retry", CallerID: 1, OwnerID: "owner-a", LeaseVersion: version, Action: ActionFirstBroadcast}

	out, err := driver.IssueAndAdvance(ctx, req)
	if err != nil || out.FinalStepState != StepConverged {
		t.Fatalf("bounded retry = %+v (err %v), want converged after retries", out, err)
	}
	if got := double.callCount(out.StepID); got != 3 {
		t.Fatalf("advance calls for step = %d, want 3 (2 unavailable + 1 sent)", got)
	}
	double.mu.Lock()
	attempts := len(double.attempts)
	double.mu.Unlock()
	if attempts != 1 {
		t.Fatalf("attempts recorded = %d, want 1 (same step_id converges)", attempts)
	}
}

// TestAdvanceExhaustedRetriesLeaveStepIssued is quickstart V7's exhaustion arm:
// an Advancer that always reports transport/unavailable exhausts the bounded
// same-step retry budget and leaves the step issued for the next cycle.
//
// Defect: none known - coverage gap. The original test asserted only the final
// step state, so advance.go's retry budget `attempt < max` could relax to
// `<= max` (one extra external 010 call) without failing.
// Invariant: for transport/unavailable classes the advanceLoop calls the 010
// boundary EXACTLY maxRetries times (default 3, StepDriver.maxRetries) with the
// same step_id; exhaustion is issued/retryable with ClassLifecycleUnavailable
// and fabricates no attempt, outcome class, success, or failure (advance.go:239
// -268). The step row stays issued with attempt_id NULL for reconcile.
// Gap: no assertion counted the boundary calls or pinned the exhausted step's
// attempt/outcome columns.
// Level: integration (real PostgreSQL scratch DB + the labeled test-only
// lifecycleDouble; never joint evidence).
// Criteria: callCount(stepID) == 3 (exactly the budget, not 4); outcome
// issued/retryable/ClassLifecycleUnavailable; DB step state issued with no
// outcome_class and no attempt_id; zero attempts recorded by the double; and
// no step_converged/step_unknown evidence for the step.
func TestAdvanceExhaustedRetriesLeaveStepIssued(t *testing.T) {
	ctx, pool := executionPool(t)
	store := execClaimStore(t, pool)
	double := newLifecycleDouble()
	double.unavailableTimes = 99
	driver := &StepDriver{Pool: pool, Claims: store, Binding: bindingDouble{result: BindingMatches}, Advancer: double}
	version := seedExecClaimedIntent(t, ctx, pool, "req-exhaust", "authz-exhaust", "intent-exhaust", "owner-a", store)
	req := StepRequest{IntentID: "intent-exhaust", RequestID: "req-exhaust", CallerID: 1, OwnerID: "owner-a", LeaseVersion: version, Action: ActionFirstBroadcast}

	out, err := driver.IssueAndAdvance(ctx, req)
	if err != nil {
		t.Fatalf("IssueAndAdvance: %v", err)
	}
	if out.FinalStepState != StepIssued || !out.Retryable {
		t.Fatalf("exhausted retries = %+v, want issued/retryable (never failure)", out)
	}
	if out.Refusal != ClassLifecycleUnavailable {
		t.Fatalf("exhausted refusal = %s, want %s", out.Refusal, ClassLifecycleUnavailable)
	}
	// The retry budget is exactly maxRetries (3): an off-by-one calls the 010
	// boundary a fourth time.
	if got := double.callCount(out.StepID); got != 3 {
		t.Fatalf("advance calls for step = %d, want exactly 3 (the bounded budget; exhaustion must not call again)", got)
	}
	// Exhaustion writes no attempt and no outcome: the step stays durable
	// reconcile evidence with attempt_id NULL.
	if state, outcome := stepState(t, ctx, pool, out.StepID); state != StepIssued || outcome != "" {
		t.Fatalf("step = %s/%q, want issued with no outcome_class", state, outcome)
	}
	var attemptID *string
	if err := pool.QueryRow(ctx, `SELECT attempt_id FROM execution_steps WHERE step_id = $1`, out.StepID).Scan(&attemptID); err != nil {
		t.Fatalf("read step attempt_id: %v", err)
	}
	if attemptID != nil {
		t.Fatalf("step attempt_id = %q, want NULL (exhaustion must not fabricate an attempt)", *attemptID)
	}
	double.mu.Lock()
	attempts := len(double.attempts)
	double.mu.Unlock()
	if attempts != 0 {
		t.Fatalf("attempts recorded = %d, want 0", attempts)
	}
	var converged int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM execution_events
		WHERE step_id = $1 AND kind IN ('step_converged', 'step_unknown')`, out.StepID).Scan(&converged); err != nil {
		t.Fatalf("count converge evidence: %v", err)
	}
	if converged != 0 {
		t.Fatalf("converge/unknown events for the exhausted step = %d, want 0", converged)
	}
}

func TestReconcileCrashPoints(t *testing.T) {
	ctx, pool := executionPool(t)
	store := execClaimStore(t, pool)
	double := newLifecycleDouble()
	double.unavailableTimes = 99
	driver := &StepDriver{Pool: pool, Claims: store, Binding: bindingDouble{result: BindingMatches}, Advancer: double}
	version := seedExecClaimedIntent(t, ctx, pool, "req-crash", "authz-crash", "intent-crash", "owner-a", store)
	req := StepRequest{IntentID: "intent-crash", RequestID: "req-crash", CallerID: 1, OwnerID: "owner-a", LeaseVersion: version, Action: ActionFirstBroadcast}
	out, err := driver.IssueAndAdvance(ctx, req)
	if err != nil || out.FinalStepState != StepIssued {
		t.Fatalf("crash-after-issue setup = %+v (err %v)", out, err)
	}

	rec := &Reconciler{Pool: pool, Reader: double}
	double.mu.Lock()
	double.unavailableTimes = 0
	double.readFacts["intent-crash"] = LifecycleFacts{
		Unknown: &UnknownRef{AttemptID: "attempt-9", TxHash: "0x" + repeatHex(), RecoveryCondition: "rpc_timeout"},
	}
	double.mu.Unlock()

	res, err := rec.ReconcileIntent(ctx, "intent-crash", "owner-a", version)
	if err != nil || !res.Reconciling {
		t.Fatalf("reconcile unknown = %+v (err %v), want reconciling", res, err)
	}
	if state, _ := stepState(t, ctx, pool, out.StepID); state != StepUnknown {
		t.Fatalf("reconciled step = %s, want unknown", state)
	}

	// Positive "no attempt persisted" evidence lets reconcile move back to
	// executing and a fresh step be issued.
	double.mu.Lock()
	double.readFacts["intent-crash"] = LifecycleFacts{}
	double.resultClass = OutcomeSent
	double.mu.Unlock()
	if _, err := rec.ReconcileIntent(ctx, "intent-crash", "owner-a", version); err != nil {
		t.Fatalf("reconcile no-attempt: %v", err)
	}
	if got := intentState(t, ctx, pool, "intent-crash"); got != IntentExecuting {
		t.Fatalf("intent state = %s, want executing after positive no-attempt evidence", got)
	}
	next, err := driver.IssueAndAdvance(ctx, req)
	if err != nil || next.FinalStepState != StepConverged {
		t.Fatalf("re-issue = %+v (err %v), want converged", next, err)
	}
	if next.StepID == out.StepID {
		t.Fatal("re-issue reused the same step row; a new logical step needs a new step_id")
	}
}

func TestReconcileReaderFailureMarksStale(t *testing.T) {
	ctx, pool := executionPool(t)
	store := execClaimStore(t, pool)
	double := newLifecycleDouble()
	version := seedExecClaimedIntent(t, ctx, pool, "req-stale", "authz-stale", "intent-stale", "owner-a", store)
	double.readErr = errors.New("ledger unavailable")

	rec := &Reconciler{Pool: pool, Reader: double}
	res, err := rec.ReconcileIntent(ctx, "intent-stale", "owner-a", version)
	if err != nil || !res.Retryable {
		t.Fatalf("failed read reconcile = %+v (err %v), want retryable", res, err)
	}
	var freshness string
	var staleSince *string
	if err := pool.QueryRow(ctx, `SELECT freshness, stale_since::text FROM request_status_projection WHERE request_id = 'req-stale'`).Scan(&freshness, &staleSince); err != nil {
		t.Fatalf("read projection: %v", err)
	}
	if freshness != "possibly_stale" || staleSince == nil {
		t.Fatalf("projection freshness = %s stale_since=%v, want possibly_stale with a timestamp", freshness, staleSince)
	}
	if got := intentState(t, ctx, pool, "intent-stale"); got != IntentClaimed {
		t.Fatalf("intent state = %s, want claimed (no business rewrite on failed read)", got)
	}
}

func repeatHex() string {
	b := make([]byte, 64)
	for i := range b {
		b[i] = 'a'
	}
	return string(b)
}
