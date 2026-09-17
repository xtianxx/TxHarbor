//go:build integration

// state_machine_integration_test.go executes quickstart V11 (T047) against real
// PostgreSQL: illegal edges are refused with zero writes and never recorded as
// transitions, repeated refusals leave state unchanged and observable, every
// legal edge is (state, state_version) CAS-guarded, and concurrent actors
// converge to exactly one winner with no lost update. The version-guard is
// physical: TransitionIntent's UPDATE carries state + state_version, so a loser
// affects zero rows (ErrTransitionRefused). No 010 side is exercised here.
package execution

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func intentStateVersion(t *testing.T, ctx context.Context, pool *pgxpool.Pool, intentID string) (string, int64) {
	t.Helper()
	var state string
	var version int64
	if err := pool.QueryRow(ctx, `SELECT state, state_version FROM payment_intents WHERE intent_id = $1`, intentID).
		Scan(&state, &version); err != nil {
		t.Fatalf("read intent %s: %v", intentID, err)
	}
	return state, version
}

func stateChangedEvents(t *testing.T, ctx context.Context, pool *pgxpool.Pool, intentID string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM execution_events
		WHERE intent_id = $1 AND kind = 'state_changed'`, intentID).Scan(&n); err != nil {
		t.Fatalf("count state_changed events: %v", err)
	}
	return n
}

func admittedIntent(t *testing.T, ctx context.Context, pool *pgxpool.Pool, requestID, authorizationID, intentID string) {
	t.Helper()
	seedFullAdmission(t, ctx, pool, requestID, authorizationID, intentID)
	out, err := Admit(ctx, pool, requestID, 1)
	if err != nil || out.Status != AdmissionCreated {
		t.Fatalf("Admit = %d (err %v), want 201", out.Status, err)
	}
}

// TestV11IllegalEdgesRefusedZeroWrites pins that edges outside state machine A
// are refused before any write: admitted->completed/executing and
// failed->completed are not legal transitions (they need authority facts or a
// claim), and a revision with no source version is a no-op, not a transition.
func TestV11IllegalEdgesRefusedZeroWrites(t *testing.T) {
	ctx, pool := executionPool(t)

	admittedIntent(t, ctx, pool, "req-v11-illegal", "authz-v11-illegal", "intent-v11-illegal")
	state, version := intentStateVersion(t, ctx, pool, "intent-v11-illegal")
	beforeEvents := stateChangedEvents(t, ctx, pool, "intent-v11-illegal")

	for _, to := range []string{IntentCompleted, IntentExecuting} {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		err = TransitionIntent(ctx, tx, "intent-v11-illegal", IntentAdmitted, version, to, 0)
		_ = tx.Rollback(ctx)
		if !errors.Is(err, ErrIllegalTransition) {
			t.Fatalf("admitted->%s = %v, want ErrIllegalTransition", to, err)
		}
	}
	if got, gotVersion := intentStateVersion(t, ctx, pool, "intent-v11-illegal"); got != state || gotVersion != version {
		t.Fatalf("illegal edges changed state to %s v%d, want %s v%d", got, gotVersion, state, version)
	}
	if got := stateChangedEvents(t, ctx, pool, "intent-v11-illegal"); got != beforeEvents {
		t.Fatalf("illegal edges wrote %d state_changed events, want %d", got, beforeEvents)
	}

	// Reach failed through legal edges only, then assert failed->completed is
	// still refused (only an authority revision may leave failed).
	store := execClaimStore(t, pool)
	version = seedExecClaimedIntent(t, ctx, pool, "req-v11-failed", "authz-v11-failed", "intent-v11-failed", "owner-a", store)
	step := func(from string, fromVersion int64, to string) {
		t.Helper()
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin %s->%s: %v", from, to, err)
		}
		if err := TransitionIntent(ctx, tx, "intent-v11-failed", from, fromVersion, to, version); err != nil {
			_ = tx.Rollback(ctx)
			t.Fatalf("%s->%s: %v", from, to, err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("commit %s->%s: %v", from, to, err)
		}
	}
	_, v := intentStateVersion(t, ctx, pool, "intent-v11-failed")
	step(IntentClaimed, v, IntentExecuting)
	_, v = intentStateVersion(t, ctx, pool, "intent-v11-failed")
	step(IntentExecuting, v, IntentFailed)

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin failed->completed: %v", err)
	}
	_, failedVersion := intentStateVersion(t, ctx, pool, "intent-v11-failed")
	err = TransitionIntent(ctx, tx, "intent-v11-failed", IntentFailed, failedVersion, IntentCompleted, version)
	_ = tx.Rollback(ctx)
	if !errors.Is(err, ErrIllegalTransition) {
		t.Fatalf("failed->completed = %v, want ErrIllegalTransition", err)
	}

	// A revision incident carries no source version: it must be a recorded
	// no-op, never a transition.
	consumer := &RevisionConsumer{Pool: pool}
	res, err := consumer.Apply(ctx, RevisionFact{
		IntentID: "intent-v11-failed", RevisionVersion: 0, AttemptID: "attempt-x", Basis: "no version probe",
	})
	if err != nil || res.Applied || res.Basis != "no source version" {
		t.Fatalf("revision without source version = %+v (err %v), want applied=false basis=no source version", res, err)
	}
	if got, _ := intentStateVersion(t, ctx, pool, "intent-v11-failed"); got != IntentFailed {
		t.Fatalf("revision without source version changed state to %s, want failed", got)
	}
}

// TestV11UnclaimedSendEnablingRefused pins that claimed->executing is not
// reachable without a current claim: the send-class driver re-verifies the
// qualification in the issue transaction and refuses with zero step rows.
func TestV11UnclaimedSendEnablingRefused(t *testing.T) {
	ctx, pool := executionPool(t)
	store := execClaimStore(t, pool)
	version := seedExecClaimedIntent(t, ctx, pool, "req-v11-unclaimed", "authz-v11-unclaimed", "intent-v11-unclaimed", "owner-a", store)
	if _, err := pool.Exec(ctx, `DELETE FROM execution_claims WHERE intent_id = 'intent-v11-unclaimed'`); err != nil {
		t.Fatalf("drop claim row: %v", err)
	}
	driver := &StepDriver{Pool: pool, Claims: store, Binding: bindingDouble{result: BindingMatches}, Advancer: newLifecycleDouble()}
	out, err := driver.IssueAndAdvance(ctx, StepRequest{
		IntentID: "intent-v11-unclaimed", RequestID: "req-v11-unclaimed", CallerID: 1,
		OwnerID: "owner-a", LeaseVersion: version, Action: ActionFirstBroadcast,
	})
	if err != nil {
		t.Fatalf("IssueAndAdvance: %v", err)
	}
	if out.Refusal != ClassClaimNotCurrent || out.FinalStepState != "" {
		t.Fatalf("unclaimed send-enabling = %+v, want refusal claim_not_current with no step", out)
	}
	if got := countIntentSteps(t, ctx, pool, "intent-v11-unclaimed"); got != 0 {
		t.Fatalf("unclaimed issue wrote %d step rows, want 0", got)
	}
	if got := intentState(t, ctx, pool, "intent-v11-unclaimed"); got != IntentClaimed {
		t.Fatalf("intent state = %s, want claimed (refusal is not a transition)", got)
	}
}

// TestV11RefusalIsNotTransition pins that repeated gate refusals leave the
// intent state and version untouched and remain observable through the returned
// refusal class: a refusal is evidence, never an edge.
func TestV11RefusalIsNotTransition(t *testing.T) {
	ctx, pool := executionPool(t)
	store := execClaimStore(t, pool)
	version := seedExecClaimedIntent(t, ctx, pool, "req-v11-refuse", "authz-v11-refuse", "intent-v11-refuse", "owner-a", store)
	driver := &StepDriver{Pool: pool, Claims: store, Binding: bindingDouble{result: BindingPaused}, Advancer: newLifecycleDouble()}
	req := StepRequest{
		IntentID: "intent-v11-refuse", RequestID: "req-v11-refuse", CallerID: 1,
		OwnerID: "owner-a", LeaseVersion: version, Action: ActionFirstBroadcast,
	}
	state, stateVersion := intentStateVersion(t, ctx, pool, "intent-v11-refuse")
	events := stateChangedEvents(t, ctx, pool, "intent-v11-refuse")

	for i := 0; i < 3; i++ {
		out, err := driver.IssueAndAdvance(ctx, req)
		if err != nil {
			t.Fatalf("refusal %d: %v", i, err)
		}
		if out.Refusal != ClassBindingPaused {
			t.Fatalf("refusal %d class = %s, want binding_paused (observable)", i, out.Refusal)
		}
		if out.FinalStepState != "" {
			t.Fatalf("refusal %d wrote a step state %q, want none", i, out.FinalStepState)
		}
	}
	if got, gotVersion := intentStateVersion(t, ctx, pool, "intent-v11-refuse"); got != state || gotVersion != stateVersion {
		t.Fatalf("repeated refusals changed state to %s v%d, want %s v%d", got, gotVersion, state, stateVersion)
	}
	if got := stateChangedEvents(t, ctx, pool, "intent-v11-refuse"); got != events {
		t.Fatalf("repeated refusals wrote %d state_changed events, want %d", got, events)
	}
	if got := countIntentSteps(t, ctx, pool, "intent-v11-refuse"); got != 0 {
		t.Fatalf("repeated refusals wrote %d step rows, want 0", got)
	}
}

// TestV11VersionGuardSingleWinner converges concurrent actors to one winner: all
// N goroutines present the same (state, state_version) CAS; exactly one UPDATE
// matches, the rest affect zero rows and are refused, so no update is lost and
// the state_changed evidence is written exactly once.
func TestV11VersionGuardSingleWinner(t *testing.T) {
	ctx, pool := executionPool(t)
	seedExecIntentRow(t, ctx, pool, "intent-v11-race")
	state, version := intentStateVersion(t, ctx, pool, "intent-v11-race")

	const n = 6
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tx, err := pool.Begin(ctx)
			if err != nil {
				errs[i] = err
				return
			}
			err = TransitionIntent(ctx, tx, "intent-v11-race", IntentAdmitted, version, IntentClaimed, 0)
			if err != nil {
				_ = tx.Rollback(ctx)
				errs[i] = err
				return
			}
			errs[i] = tx.Commit(ctx)
		}(i)
	}
	wg.Wait()

	winners, refused := 0, 0
	for i, err := range errs {
		switch {
		case err == nil:
			winners++
		case errors.Is(err, ErrTransitionRefused):
			refused++
		default:
			t.Fatalf("goroutine %d: %v", i, err)
		}
	}
	if winners != 1 || refused != n-1 {
		t.Fatalf("winners=%d refused=%d, want 1/%d", winners, refused, n-1)
	}
	if got, gotVersion := intentStateVersion(t, ctx, pool, "intent-v11-race"); got != IntentClaimed || gotVersion != version+1 {
		t.Fatalf("final state = %s v%d, want claimed v%d", got, gotVersion, version+1)
	}
	if got := stateChangedEvents(t, ctx, pool, "intent-v11-race"); got != 1 {
		t.Fatalf("state_changed events = %d, want 1 (no lost update, no double transition)", got)
	}
	if state != IntentAdmitted {
		t.Fatalf("seed state = %s, want admitted", state)
	}
}
