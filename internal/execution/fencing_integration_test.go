//go:build integration

// fencing_integration_test.go executes quickstart V3 (T024): after a
// disqualification (expiry or operator revocation) the old holder's writes and
// converges affect zero rows, and the three recorded 010 outcome facts stay
// distinct. The 010-side refusal of the three send kinds is the joint half
// (010:T047/J2) and is NOT claimed here; the independent run asserts 011's side
// with the labeled test-only double (T030).
package execution

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func seedExecClaimedIntent(t *testing.T, ctx context.Context, pool *pgxpool.Pool, requestID, authorizationID, intentID, owner string, store *ClaimStore) int64 {
	t.Helper()
	seedFullAdmission(t, ctx, pool, requestID, authorizationID, intentID)
	out, err := Admit(ctx, pool, requestID, 1)
	if err != nil || out.Status != AdmissionCreated {
		t.Fatalf("Admit = %d (err %v), want 201", out.Status, err)
	}
	res, err := store.Claim(ctx, intentID, owner)
	if err != nil || !res.Acquired {
		t.Fatalf("Claim = %+v (err %v), want acquired", res, err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin claim transition: %v", err)
	}
	if err := TransitionIntent(ctx, tx, intentID, IntentAdmitted, 1, IntentClaimed, res.Version); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("admitted->claimed: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit admitted->claimed: %v", err)
	}
	return res.Version
}

func execRevoke(t *testing.T, ctx context.Context, pool *pgxpool.Pool, intentID string, version int64) {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin revoke: %v", err)
	}
	ok, err := RevokeClaimTx(ctx, tx, intentID, version, "operator", "test evidence")
	if err != nil || !ok {
		_ = tx.Rollback(ctx)
		t.Fatalf("RevokeClaimTx = %v (err %v), want applied", ok, err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit revoke: %v", err)
	}
}

func countIntentSteps(t *testing.T, ctx context.Context, pool *pgxpool.Pool, intentID string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM execution_steps WHERE intent_id = $1`, intentID).Scan(&n); err != nil {
		t.Fatalf("count steps: %v", err)
	}
	return n
}

func stepState(t *testing.T, ctx context.Context, pool *pgxpool.Pool, stepID string) (string, string) {
	t.Helper()
	var state, outcome string
	if err := pool.QueryRow(ctx, `SELECT state, COALESCE(outcome_class, '') FROM execution_steps WHERE step_id = $1`, stepID).Scan(&state, &outcome); err != nil {
		t.Fatalf("read step %s: %v", stepID, err)
	}
	return state, outcome
}

func intentState(t *testing.T, ctx context.Context, pool *pgxpool.Pool, intentID string) string {
	t.Helper()
	var state string
	if err := pool.QueryRow(ctx, `SELECT state FROM payment_intents WHERE intent_id = $1`, intentID).Scan(&state); err != nil {
		t.Fatalf("read intent: %v", err)
	}
	return state
}

func TestDisqualificationFencesHolderWrites(t *testing.T) {
	ctx, pool := executionPool(t)
	store := execClaimStore(t, pool)
	double := newLifecycleDouble()
	driver := &StepDriver{Pool: pool, Claims: store, Binding: bindingDouble{result: BindingMatches}, Advancer: double}
	owner, version := "owner-a", int64(0)
	version = seedExecClaimedIntent(t, ctx, pool, "req-fence", "authz-fence", "intent-fence", owner, store)

	req := StepRequest{IntentID: "intent-fence", RequestID: "req-fence", CallerID: 1, OwnerID: owner, LeaseVersion: version, Action: ActionFirstBroadcast}
	out, err := driver.IssueAndAdvance(ctx, req)
	if err != nil {
		t.Fatalf("IssueAndAdvance: %v", err)
	}
	if out.FinalStepState != StepConverged || out.OutcomeClass != OutcomeSent {
		t.Fatalf("step outcome = %s/%s, want converged/sent", out.FinalStepState, out.OutcomeClass)
	}

	execRevoke(t, ctx, pool, "intent-fence", version)

	if err := store.AdvanceProgress(ctx, "intent-fence", owner, version); !errors.Is(err, ErrClaimLost) {
		t.Fatalf("disqualified progress write = %v, want ErrClaimLost", err)
	}
	if err := store.Release(ctx, "intent-fence", owner, version); !errors.Is(err, ErrClaimLost) {
		t.Fatalf("disqualified release = %v, want ErrClaimLost", err)
	}

	// The old holder cannot converge its step: the live claim check refuses it
	// and the already-converged row is never rewritten.
	conv, err := driver.converge(ctx, req, out.StepID, advanceResult{class: OutcomeRefusedGate})
	if err != nil {
		t.Fatalf("converge by disqualified holder: %v", err)
	}
	if conv.Refusal != ClassClaimNotCurrent {
		t.Fatalf("converge refusal = %q, want %s", conv.Refusal, ClassClaimNotCurrent)
	}
	if state, class := stepState(t, ctx, pool, out.StepID); state != StepConverged || class != OutcomeSent {
		t.Fatalf("step was rewritten to %s/%s, want converged/sent", state, class)
	}

	// A fresh step issue by the old qualification is refused with zero writes.
	again, err := driver.IssueAndAdvance(ctx, req)
	if err != nil {
		t.Fatalf("second IssueAndAdvance: %v", err)
	}
	if again.Refusal != ClassClaimNotCurrent || again.StepID != "" {
		t.Fatalf("old-holder issue = %+v, want claim_not_current with no step", again)
	}
	if got := countIntentSteps(t, ctx, pool, "intent-fence"); got != 1 {
		t.Fatalf("step rows = %d, want 1 (zero new steps)", got)
	}
}

func TestRecordedOutcomeThreeCasesStayDistinct(t *testing.T) {
	ctx, pool := executionPool(t)
	store := execClaimStore(t, pool)
	double := newLifecycleDouble()
	driver := &StepDriver{Pool: pool, Claims: store, Binding: bindingDouble{result: BindingMatches}, Advancer: double}
	owner := "owner-a"
	version := seedExecClaimedIntent(t, ctx, pool, "req-three", "authz-three", "intent-three", owner, store)
	req := StepRequest{IntentID: "intent-three", RequestID: "req-three", CallerID: 1, OwnerID: owner, LeaseVersion: version, Action: ActionFirstBroadcast}

	// (iii) known result preserved verbatim.
	double.resultClass = OutcomeRefusedGate
	known, err := driver.IssueAndAdvance(ctx, req)
	if err != nil || known.FinalStepState != StepConverged || known.OutcomeClass != OutcomeRefusedGate {
		t.Fatalf("known case = %+v (err %v)", known, err)
	}

	// (ii) a previously-unknown business effect stays persisted; never failure.
	double.resultClass = OutcomePendingUnknown
	unknown, err := driver.IssueAndAdvance(ctx, req)
	if err != nil || unknown.FinalStepState != StepUnknown || !unknown.Reconciling {
		t.Fatalf("unknown case = %+v (err %v)", unknown, err)
	}
	if got := intentState(t, ctx, pool, "intent-three"); got != IntentReconciling {
		t.Fatalf("intent state = %s, want reconciling (never failure)", got)
	}

	// (i) confirmed-no-send this attempt: distinct from unknown and known.
	double.resultClass = ""
	double.noSendResult = true
	noSend, err := driver.IssueAndAdvance(ctx, req)
	if err != nil || noSend.FinalStepState != StepRefused {
		t.Fatalf("no-send case = %+v (err %v), want refused", noSend, err)
	}
	state, class := stepState(t, ctx, pool, noSend.StepID)
	if state != StepRefused || class != "" {
		t.Fatalf("no-send step = %s/%q, want refused with no outcome class", state, class)
	}
	if got, _ := stepState(t, ctx, pool, unknown.StepID); got == state {
		t.Fatalf("no-send and unknown share step state %s; they must stay distinct", state)
	}

	var failed int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM payment_intents WHERE intent_id = 'intent-three' AND state = 'failed'`).Scan(&failed); err != nil || failed != 0 {
		t.Fatalf("failed intent rows = %d (err %v), want 0", failed, err)
	}
	var noSendEvents int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM execution_events
		WHERE intent_id = 'intent-three' AND kind = 'step_refused' AND detail LIKE 'no_send_result=%'`).Scan(&noSendEvents); err != nil || noSendEvents != 1 {
		t.Fatalf("no_send_result events = %d (err %v), want 1", noSendEvents, err)
	}
}
