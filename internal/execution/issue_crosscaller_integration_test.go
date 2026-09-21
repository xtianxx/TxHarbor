//go:build integration

// issue_crosscaller_integration_test.go closes the caller-identity gap at the
// T-step-issue gate (advance.go:153).
//
// Defect: none known - coverage gap. The cross-caller guard
// (`req.CallerID != 0 && request.CallerID != req.CallerID`) had zero coverage:
// every execution integration test issued with CallerID 1 (the request/grant
// owner), and the Batch C signer test pinned only the 009 layer without ever
// executing issue(). A mutant relaxing the guard to `req.CallerID == 0` is
// therefore invisible to the suite.
// Invariant: a send-class step may only be issued for the caller that owns the
// 007 request row (gates.md §2: the request carries the business fields the
// grant must equal). A different caller is refused ClassGateReadFailed with
// zero side effects - no execution_steps row, no execution_events, no intent
// transition, no claim move, and no 010 advance call (hence no signature, send,
// or attempt anywhere).
// Level: integration (real PostgreSQL scratch DB + the labeled test-only
// lifecycleDouble; never joint evidence).
// Criteria: caller 2 - seeded as a real caller WITH can_execute=true, so the
// refusal cannot be blamed on permission - issuing caller 1's request returns
// Refusal=ClassGateReadFailed with the 007 caller-mismatch basis, out.StepID
// empty, zero new execution_steps rows, zero advance calls/attempts, the intent
// still claimed, and every relevant table byte-identical (md5 row digests).
package execution

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// executionSnapshotTables are the tables a refused issue must leave untouched.
// The tx_attempt_* tables belong to the 010/009 lane, which this driver never
// reaches on a refusal: asserting them empty-stable pins "zero signatures,
// sends, and attempts" end to end.
var executionSnapshotTables = []string{
	"caller",
	"payment_intents",
	"execution_claims",
	"execution_steps",
	"execution_events",
	"execution_caller_permission",
	"execution_ops_audit",
	"request_status_projection",
	"withdrawal_requests",
	"withdrawal_authorizations",
	"withdrawal_authorization_scopes",
	"nonce_wallet_registry",
	"nonce_scope_state",
	"nonce_bindings",
	"tx_attempts",
	"tx_attempt_signings",
	"tx_send_attempts",
	"tx_attempt_events",
	"tx_intent_freezes",
}

// snapshotExecutionTables returns one order-independent md5 digest per table
// (all rows serialized via to_jsonb), so any new, removed, or changed row in a
// refused path is detectable without pinning column-by-column values.
func snapshotExecutionTables(t *testing.T, ctx context.Context, pool *pgxpool.Pool) map[string]string {
	t.Helper()
	snap := make(map[string]string, len(executionSnapshotTables))
	for _, table := range executionSnapshotTables {
		var digest string
		err := pool.QueryRow(ctx,
			`SELECT COALESCE(md5(string_agg(x, E'\n' ORDER BY x)), '') FROM (SELECT to_jsonb(t)::text AS x FROM `+table+` t) s`).
			Scan(&digest)
		if err != nil {
			t.Fatalf("snapshot table %s: %v", table, err)
		}
		snap[table] = digest
	}
	return snap
}

func assertExecutionTablesUnchanged(t *testing.T, before, after map[string]string) {
	t.Helper()
	for _, table := range executionSnapshotTables {
		if before[table] != after[table] {
			t.Fatalf("table %s changed across a refused issue: %s -> %s", table, before[table], after[table])
		}
	}
}

func TestIssueRefusesCrossCallerRequest(t *testing.T) {
	ctx, pool := executionPool(t)
	store := execClaimStore(t, pool)
	double := newLifecycleDouble()
	driver := &StepDriver{Pool: pool, Claims: store, Binding: bindingDouble{result: BindingMatches}, Advancer: double}
	version := seedExecClaimedIntent(t, ctx, pool, "req-xcaller", "authz-xcaller", "intent-xcaller", "owner-a", store)

	// Caller 2 is a fully permitted executor: the mismatch check at
	// advance.go:153 runs before the grant gate (line 156) and before any
	// permission read, so the refusal must come from request ownership alone.
	seedExecCaller(t, ctx, pool, 2)
	seedExecPermission(t, ctx, pool, 2, true)

	before := snapshotExecutionTables(t, ctx, pool)
	stepsBefore := countIntentSteps(t, ctx, pool, "intent-xcaller")

	req := StepRequest{IntentID: "intent-xcaller", RequestID: "req-xcaller", CallerID: 2, OwnerID: "owner-a", LeaseVersion: version, Action: ActionFirstBroadcast}
	out, err := driver.IssueAndAdvance(ctx, req)
	if err != nil {
		t.Fatalf("IssueAndAdvance: %v", err)
	}
	if out.Refusal != ClassGateReadFailed {
		t.Fatalf("cross-caller issue = %+v, want refusal %s (caller 2 must not issue caller 1's request)", out, ClassGateReadFailed)
	}
	if !strings.Contains(out.Basis, "caller") {
		t.Fatalf("cross-caller basis = %q, want the 007 caller-mismatch basis", out.Basis)
	}
	if out.StepID != "" {
		t.Fatalf("refused issue returned step id %q, want none", out.StepID)
	}

	// Zero durable execution position: no step, no boundary call, no attempt.
	if got := countIntentSteps(t, ctx, pool, "intent-xcaller"); got != stepsBefore {
		t.Fatalf("execution_steps rows = %d, want %d (no step may be issued)", got, stepsBefore)
	}
	if got := double.totalCalls(); got != 0 {
		t.Fatalf("010 advance calls = %d, want 0 (the refusal precedes any external call)", got)
	}
	double.mu.Lock()
	attempts := len(double.attempts)
	double.mu.Unlock()
	if attempts != 0 {
		t.Fatalf("attempts recorded = %d, want 0", attempts)
	}
	if got := intentState(t, ctx, pool, "intent-xcaller"); got != IntentClaimed {
		t.Fatalf("intent state = %s, want claimed (a refusal must not transition)", got)
	}

	// Every row byte-identical: covers zero signatures/sends/attempts (010's
	// tables) plus the 007/011/012 upstream tables.
	assertExecutionTablesUnchanged(t, before, snapshotExecutionTables(t, ctx, pool))
}
