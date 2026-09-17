//go:build integration

// recovery_gate_integration_test.go executes quickstart V8 (T033): a pause row
// or an active recovery instance refuses every send-enabling step with zero
// sends and zero "queued for later"; multiple independent pause rows are not
// bypassed by clearing one; a recovery-version move makes 011 record the new
// observed version and 010's refused_basis converge the step, then re-observe
// and retry under current gates with no grace period. 011 only reads 006 state.
package execution

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func insertIndexerPause(t *testing.T, ctx context.Context, q *pgxpool.Pool, chainID int64) {
	t.Helper()
	if _, err := q.Exec(ctx, `INSERT INTO indexer_pause (chain_id, height, expected_hash, actual_hash, kind)
		VALUES ($1, 1, '0x' || repeat('a',64), '0x' || repeat('b',64), 'hash_mismatch')`, chainID); err != nil {
		t.Fatalf("insert indexer_pause: %v", err)
	}
}

func insertLogPause(t *testing.T, ctx context.Context, q *pgxpool.Pool, chainID int64) {
	t.Helper()
	if _, err := q.Exec(ctx, `INSERT INTO log_pause (chain_id, height, kind)
		VALUES ($1, 1, 'chain_view_changed')`, chainID); err != nil {
		t.Fatalf("insert log_pause: %v", err)
	}
}

func TestPauseRefusesStepIssue(t *testing.T) {
	ctx, pool := executionPool(t)
	store := execClaimStore(t, pool)
	double := newLifecycleDouble()
	driver := &StepDriver{Pool: pool, Claims: store, Binding: bindingDouble{result: BindingMatches}, Advancer: double}
	version := seedExecClaimedIntent(t, ctx, pool, "req-v8a", "authz-v8a", "intent-v8a", "owner-a", store)
	req := StepRequest{IntentID: "intent-v8a", RequestID: "req-v8a", CallerID: 1, OwnerID: "owner-a", LeaseVersion: version, Action: ActionFirstBroadcast}

	insertIndexerPause(t, ctx, pool, execChainID)
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM indexer_pause WHERE chain_id = $1`, execChainID)
	})

	out, err := driver.IssueAndAdvance(ctx, req)
	if err != nil || out.Refusal != ClassRecoveryPaused || out.StepID != "" {
		t.Fatalf("paused issue = %+v (err %v), want recovery_paused with no step", out, err)
	}
	if got := countIntentSteps(t, ctx, pool, "intent-v8a"); got != 0 {
		t.Fatalf("steps = %d, want 0 (no queue-for-later)", got)
	}
	if double.totalCalls() != 0 {
		t.Fatalf("advance calls = %d, want 0 while paused", double.totalCalls())
	}
}

func TestMultiplePauseRowsNotBypassed(t *testing.T) {
	ctx, pool := executionPool(t)
	store := execClaimStore(t, pool)
	double := newLifecycleDouble()
	driver := &StepDriver{Pool: pool, Claims: store, Binding: bindingDouble{result: BindingMatches}, Advancer: double}
	version := seedExecClaimedIntent(t, ctx, pool, "req-v8b", "authz-v8b", "intent-v8b", "owner-a", store)
	req := StepRequest{IntentID: "intent-v8b", RequestID: "req-v8b", CallerID: 1, OwnerID: "owner-a", LeaseVersion: version, Action: ActionFirstBroadcast}

	insertIndexerPause(t, ctx, pool, execChainID)
	insertLogPause(t, ctx, pool, execChainID)
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM indexer_pause WHERE chain_id = $1`, execChainID)
		_, _ = pool.Exec(context.Background(), `DELETE FROM log_pause WHERE chain_id = $1`, execChainID)
	})

	if _, err := pool.Exec(ctx, `DELETE FROM indexer_pause WHERE chain_id = $1`, execChainID); err != nil {
		t.Fatalf("clear one pause: %v", err)
	}
	if out, err := driver.IssueAndAdvance(ctx, req); err != nil || out.Refusal != ClassRecoveryPaused {
		t.Fatalf("clearing one pause bypassed the other: %+v (err %v)", out, err)
	}
}

func TestActiveRecoveryRefusesButUnknownStays(t *testing.T) {
	ctx, pool := executionPool(t)
	store := execClaimStore(t, pool)
	double := newLifecycleDouble()
	double.unavailableTimes = 99
	driver := &StepDriver{Pool: pool, Claims: store, Binding: bindingDouble{result: BindingMatches}, Advancer: double}
	version := seedExecClaimedIntent(t, ctx, pool, "req-v8c", "authz-v8c", "intent-v8c", "owner-a", store)
	req := StepRequest{IntentID: "intent-v8c", RequestID: "req-v8c", CallerID: 1, OwnerID: "owner-a", LeaseVersion: version, Action: ActionFirstBroadcast}
	out, err := driver.IssueAndAdvance(ctx, req)
	if err != nil || out.FinalStepState != StepIssued {
		t.Fatalf("setup open step = %+v (err %v)", out, err)
	}

	if _, err := pool.Exec(ctx, `INSERT INTO reorg_policy_history (chain_id, policy_seq, max_depth) VALUES ($1, 1, 100)`, execChainID); err != nil {
		t.Fatalf("seed policy: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO reorg_recovery
		(chain_id, recovery_id, phase, policy_seq, max_depth, bound_old_number, bound_old_hash, recovery_seq)
		VALUES ($1, 'rec-v8', 'detected', 1, 100, 10, '0x' || repeat('c',64), 9)`, execChainID); err != nil {
		t.Fatalf("seed recovery: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM reorg_recovery WHERE chain_id = $1`, execChainID)
		_, _ = pool.Exec(context.Background(), `DELETE FROM reorg_policy_history WHERE chain_id = $1`, execChainID)
	})

	if got, err := driver.IssueAndAdvance(ctx, req); err != nil || got.Refusal != ClassRecoveryActive {
		t.Fatalf("active recovery issue = %+v (err %v), want recovery_active", got, err)
	}

	// Reconcile remains allowed while recovery is active; unknown stays unknown.
	double.mu.Lock()
	double.unavailableTimes = 0
	double.readFacts["intent-v8c"] = LifecycleFacts{Unknown: &UnknownRef{AttemptID: "attempt-v8", RecoveryCondition: "reorg"}}
	double.mu.Unlock()
	rec := &Reconciler{Pool: pool, Reader: double}
	if res, err := rec.ReconcileIntent(ctx, "intent-v8c", "owner-a", version); err != nil || !res.Reconciling {
		t.Fatalf("reconcile under recovery = %+v (err %v), want reconciling", res, err)
	}
	if state, _ := stepState(t, ctx, pool, out.StepID); state != StepUnknown {
		t.Fatalf("step state = %s, want unknown (never failure)", state)
	}
}

func TestRecoveryVersionMoveRefusedBasis(t *testing.T) {
	ctx, pool := executionPool(t)
	store := execClaimStore(t, pool)
	double := newLifecycleDouble()
	driver := &StepDriver{Pool: pool, Claims: store, Binding: bindingDouble{result: BindingMatches}, Advancer: double}
	version := seedExecClaimedIntent(t, ctx, pool, "req-v8d", "authz-v8d", "intent-v8d", "owner-a", store)
	req := StepRequest{IntentID: "intent-v8d", RequestID: "req-v8d", CallerID: 1, OwnerID: "owner-a", LeaseVersion: version, Action: ActionFirstBroadcast}

	if _, err := pool.Exec(ctx, `INSERT INTO reorg_recovery_events (chain_id, recovery_id, recovery_seq, event_seq, event)
		VALUES ($1, 'rec-v8d', 5, 1, 'established')`, execChainID); err != nil {
		t.Fatalf("seed recovery event: %v", err)
	}

	// 011 records the newly observed version and 010's refused_basis converges
	// the step; there is no grace period.
	double.resultClass = OutcomeRefusedBasis
	first, err := driver.IssueAndAdvance(ctx, req)
	if err != nil || first.FinalStepState != StepConverged || first.OutcomeClass != OutcomeRefusedBasis {
		t.Fatalf("version-move step = %+v (err %v), want converged/refused_basis", first, err)
	}
	var observed int64
	if err := pool.QueryRow(ctx, `SELECT recovery_version FROM execution_steps WHERE step_id = $1`, first.StepID).Scan(&observed); err != nil || observed != 5 {
		t.Fatalf("observed recovery_version = %d (err %v), want 5", observed, err)
	}

	double.resultClass = OutcomeSent
	if again, err := driver.IssueAndAdvance(ctx, req); err != nil || again.FinalStepState != StepConverged {
		t.Fatalf("retry under current gates = %+v (err %v), want converged", again, err)
	}
}
