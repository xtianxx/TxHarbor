//go:build integration

// takeover_integration_test.go executes quickstart V4 (T025, M1n): after
// disqualification the SAME worker may re-acquire and receives a new
// lease_version (no identity ban); the new holder continues the same intent and
// attempt history; a gate failure on re-acquisition refuses with zero sends;
// old-version steps are visible but cannot be converged by the old version.
package execution

import "testing"

func TestEqualRecompetitionSameWorker(t *testing.T) {
	ctx, pool := executionPool(t)
	store := execClaimStore(t, pool)
	owner := "owner-a"
	v1 := seedExecClaimedIntent(t, ctx, pool, "req-recomp", "authz-recomp", "intent-recomp", owner, store)

	execRevoke(t, ctx, pool, "intent-recomp", v1)

	res, err := store.Claim(ctx, "intent-recomp", owner)
	if err != nil {
		t.Fatalf("re-claim by same owner: %v", err)
	}
	if !res.Acquired || res.Version != v1+1 {
		t.Fatalf("re-claim = %+v, want acquired with version %d", res, v1+1)
	}
	var intents int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM payment_intents WHERE intent_id = 'intent-recomp'`).Scan(&intents); err != nil || intents != 1 {
		t.Fatalf("intent rows = %d (err %v), want 1", intents, err)
	}
}

func TestTakeoverContinuesIntentAndHistory(t *testing.T) {
	ctx, pool := executionPool(t)
	store := execClaimStore(t, pool)
	double := newLifecycleDouble()
	driver := &StepDriver{Pool: pool, Claims: store, Binding: bindingDouble{result: BindingMatches}, Advancer: double}
	ownerA := "owner-a"
	v1 := seedExecClaimedIntent(t, ctx, pool, "req-takeover", "authz-takeover", "intent-takeover", ownerA, store)

	reqA := StepRequest{IntentID: "intent-takeover", RequestID: "req-takeover", CallerID: 1, OwnerID: ownerA, LeaseVersion: v1, Action: ActionFirstBroadcast}
	first, err := driver.IssueAndAdvance(ctx, reqA)
	if err != nil || first.FinalStepState != StepConverged {
		t.Fatalf("first step = %+v (err %v)", first, err)
	}

	expireExecClaim(t, ctx, pool, "intent-takeover")
	ownerB := "owner-b"
	res, err := store.Claim(ctx, "intent-takeover", ownerB)
	if err != nil || !res.Acquired || res.Version != v1+1 {
		t.Fatalf("takeover = %+v (err %v), want acquired v%d", res, err, v1+1)
	}

	// The same intent continues; no second intent exists.
	var intents int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM payment_intents`).Scan(&intents); err != nil || intents != 1 {
		t.Fatalf("intent rows = %d (err %v), want 1", intents, err)
	}

	// The old step is visible; the old qualification cannot converge it.
	conv, err := driver.converge(ctx, reqA, first.StepID, advanceResult{class: OutcomeRefusedGate})
	if err != nil || conv.Refusal != ClassClaimNotCurrent {
		t.Fatalf("old-version converge = %+v (err %v), want claim_not_current", conv, err)
	}
	if state, class := stepState(t, ctx, pool, first.StepID); state != StepConverged || class != OutcomeSent {
		t.Fatalf("old step = %s/%s, want unchanged converged/sent", state, class)
	}

	// Re-acquisition re-verifies gates: a paused binding refuses with zero new
	// steps for the new holder.
	paused := &StepDriver{Pool: pool, Claims: store, Binding: bindingDouble{result: BindingPaused}, Advancer: double}
	reqB := StepRequest{IntentID: "intent-takeover", RequestID: "req-takeover", CallerID: 1, OwnerID: ownerB, LeaseVersion: res.Version, Action: ActionFirstBroadcast}
	if got, err := paused.IssueAndAdvance(ctx, reqB); err != nil || got.Refusal != ClassBindingPaused || got.StepID != "" {
		t.Fatalf("paused re-verification = %+v (err %v), want binding_paused", got, err)
	}

	// Under current gates the new holder continues the same intent.
	second, err := driver.IssueAndAdvance(ctx, reqB)
	if err != nil || second.FinalStepState != StepConverged {
		t.Fatalf("new-holder step = %+v (err %v)", second, err)
	}
	if got := countIntentSteps(t, ctx, pool, "intent-takeover"); got != 2 {
		t.Fatalf("step rows = %d, want 2 (one per generation)", got)
	}
}
