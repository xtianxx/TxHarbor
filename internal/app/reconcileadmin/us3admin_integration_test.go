//go:build integration

// us3admin_integration_test.go is the T028 US3 acceptance layer for the
// `reconcile-admin reverify` / `close` commands over the real 000016 schema
// (quickstart §11 + §call-path; FR-005/010/017/018/019; Q1/Q5;
// contracts/discrepancy-lifecycle.md): repeated budgeted invocations converge
// and every closure is evidence-gated.
//
//   - dispose hands a ticket to pending_verify through the real admin paths,
//     and close refuses it without a fresh consistent reverify row (disposed ≠
//     reverified ≠ closed);
//   - the real CLI `reverify` drives one bounded slice with the production
//     EventStateReverifyEvaluator: missing slice bounds are refused by name,
//     the slice re-reads the closed item, and the output never claims
//     completeness (verified_complete=false while a gap is open; the advanced
//     traversal cursor is progress, not a completeness claim);
//   - the bounded Store.RunReverifySweep executor (the T027 production path;
//     its ReverifyEvaluator seam is the documented extension point for a
//     caller that owns the full T013–T015 compare path) writes a fresh
//     consistent reverify row; after a conclusion-affecting change moves the
//     ticket back to pending_verify, the real CLI close succeeds on that fresh
//     consistent row and records the close_basis snapshot;
//   - a later divergent revalidation supersedes that row and invalidates the
//     closed ticket back to pending_verify, and the old consistent result can
//     no longer close (the refusal is audited and nothing is written);
//   - the 007–012 funds tables never move.
//
// The one seeded precondition is a recorded closed conclusion (state closed +
// close_basis) because the sweep's contract (reverifyScopePredicate) walks
// closed items only: the consistent row it writes must land while the ticket
// is closed. Every state transition under proof then goes through Store/admin
// methods — no DB state is hand-modified to fake a closure. PostgreSQL comes
// from testcontainers; when no Docker provider is healthy the package reports
// NOT RUN (t.Skip), never a pass.
//
// The exact call path this harness drives through the real Run entry (the
// binary equivalents):
//
//	txharbor reconcile-admin start --chain-id CHAIN --scope-kind height --from 100 --to 200 \
//	    --business-types withdrawal --confirm-threshold-n 3 --upstream-receipts withdrawal=ledger:connected
//	txharbor reconcile-admin resume --task-id TASK
//	txharbor reconcile-admin scan --task-id TASK
//	txharbor reconcile-admin reverify --task-id TASK --max-items 4 --max-pg-requests 20 --max-item-attempts 3
//	txharbor reconcile-admin claim --discrepancy-id D --operator op --reason r --operation-id op1
//	txharbor reconcile-admin dispose --discrepancy-id D --kind ack_only --result done --operator op --reason r --operation-id op2
//	txharbor reconcile-admin close --discrepancy-id D --close-basis '{"range":"100..200","block":101,"version":"v1"}' --reason r
package reconcileadmin

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/reconciliation"
)

const (
	us3ITFrom = int64(100)
	us3ITTo   = int64(200)
)

// us3ITInt64 points at v for the *int64 AuthScope range API.
func us3ITInt64(v int64) *int64 { return &v }

// us3ITAuthScope is the ticket/task authorization scope of every US3 case.
func us3ITAuthScope(chainID string) reconciliation.AuthScope {
	return reconciliation.AuthScope{
		ChainID: chainID, Kind: reconciliation.ScopeHeight,
		BusinessTypes: []reconciliation.BusinessType{reconciliation.BusinessWithdrawal},
		RangeStart:    us3ITInt64(us3ITFrom), RangeEnd: us3ITInt64(us3ITTo),
	}
}

// us3ITEnv returns a valid command environment bound to one principal (the
// window-probe bound is configured because the reverify command needs it).
func us3ITEnv(dsn, principal string) map[string]string {
	env := recAdminEnv(dsn, nil, 8)
	env[config.EnvReconPrincipal] = principal
	return env
}

// us3ITSeedDiscrepancy inserts one ticket carrying the real scope marker the
// production authorization seam parses from evidence_version_domain. A closed
// seed requires the recorded close_basis snapshot (migration CHECK).
func us3ITSeedDiscrepancy(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID, id, businessKey string,
	state reconciliation.DiscrepancyState, closeBasis string) {
	t.Helper()
	if !state.Valid() {
		t.Fatalf("seed discrepancy with unknown state %q", state)
	}
	if state == reconciliation.DiscrepancyStateClosed && strings.TrimSpace(closeBasis) == "" {
		t.Fatalf("a closed seed requires a close_basis snapshot")
	}
	scope, err := reconciliation.HeightIdentityScope(chainID, us3ITFrom, us3ITTo, reconciliation.BusinessWithdrawal)
	if err != nil {
		t.Fatalf("HeightIdentityScope: %v", err)
	}
	domain, err := reconciliation.PersistedEvidenceDomainJSON(reconciliation.VersionDomain{
		BlockNumber: 101, BlockHash: "0xus3admin", EvidenceAt: time.Now().UTC().Add(-2 * time.Minute),
	}, &scope)
	if err != nil {
		t.Fatalf("persist evidence domain of %s: %v", id, err)
	}
	var basis any
	if strings.TrimSpace(closeBasis) != "" {
		basis = closeBasis
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO discrepancy (
		    discrepancy_id, category, business_key, content_hash, evidence_version_domain,
		    state, close_basis)
		VALUES ($1::uuid, 'missing', $2, $3, $4::jsonb, $5, $6::jsonb)`,
		id, businessKey, []byte{0x03, 0x04}, string(domain), string(state), basis); err != nil {
		t.Fatalf("seed discrepancy %s: %v", id, err)
	}
}

// us3ITCount runs one scalar count query.
func us3ITCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, query string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(ctx, query, args...).Scan(&n); err != nil {
		t.Fatalf("count query %q: %v", query, err)
	}
	return n
}

// us3ITState reads the observable ticket state.
func us3ITState(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id string) reconciliation.DiscrepancyState {
	t.Helper()
	var state string
	if err := pool.QueryRow(ctx, `SELECT state FROM discrepancy WHERE discrepancy_id = $1::uuid`, id).Scan(&state); err != nil {
		t.Fatalf("read state of %s: %v", id, err)
	}
	return reconciliation.DiscrepancyState(state)
}

// us3ITLatestVerdict reads the newest recorded reverify verdict ("" when the
// ticket has no reverify row).
func us3ITLatestVerdict(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id string) string {
	t.Helper()
	var verdict string
	err := pool.QueryRow(ctx, `
		SELECT verdict FROM reverify WHERE discrepancy_id = $1::uuid
		ORDER BY created_at DESC, reverify_id DESC LIMIT 1`, id).Scan(&verdict)
	if errors.Is(err, pgx.ErrNoRows) {
		return ""
	}
	if err != nil {
		t.Fatalf("read latest verdict of %s: %v", id, err)
	}
	return verdict
}

// us3ITHistoryCursorAdvanced reports whether a traversal cursor was persisted
// for the task.
func us3ITHistoryCursorAdvanced(t *testing.T, ctx context.Context, pool *pgxpool.Pool, taskID string) bool {
	t.Helper()
	return us3ITCount(t, ctx, pool,
		`SELECT count(*)::bigint FROM recon_task WHERE task_id = $1::uuid AND history_sweep_through IS NOT NULL`,
		taskID) == 1
}

// us3ITBudget builds the task-total budget of one sweep invocation.
func us3ITBudget(t *testing.T) *reconciliation.Budget {
	t.Helper()
	budget, err := reconciliation.NewBudget(reconciliation.BudgetLimits{
		MaxConcurrency: 1, MaxSpanPerClaim: 4, MaxDuration: time.Minute,
		MaxPGRequests: 100, MaxRPCRequests: 20,
	})
	if err != nil {
		t.Fatalf("NewBudget: %v", err)
	}
	return budget
}

// us3ITSweep runs one real bounded history-revalidation slice through the T027
// production executor with the caller-supplied evidence seam. crossCheck
// carries the scan-loop cross-check candidates (the production carrier of an
// old-range evidence change; finder 1 of the sweep).
func us3ITSweep(t *testing.T, ctx context.Context, store *reconciliation.Store, taskID string,
	evaluator reconciliation.ReverifyEvaluator, crossCheck ...reconciliation.ReverifyCandidate) reconciliation.ReverifySweepResult {
	t.Helper()
	result, err := store.RunReverifySweep(ctx, reconciliation.ReverifySweepRequest{
		TaskID:       taskID,
		Actor:        "deploy:it-sweep",
		Slice:        reconciliation.ReverifySlice{MaxItems: 4, MaxPGRequests: 20, MaxItemAttempts: 3},
		ParentBudget: us3ITBudget(t),
		Evaluator:    evaluator,
		CrossCheck:   crossCheck,
	})
	if err != nil {
		t.Fatalf("RunReverifySweep: %v", err)
	}
	return result
}

// us3ITConsistentEvaluator is the documented compare-path seam shape: a caller
// that owns the full T013–T015 re-read returns a fresh consistent finding with
// an evidence reference (a single-party adapter can never prove it; Q5-4).
func us3ITConsistentEvaluator() reconciliation.ReverifyEvaluator {
	return reconciliation.ReverifyEvaluatorFunc(func(ctx context.Context, item reconciliation.ClosedDiscrepancy,
		budget reconciliation.ScanQueryBudget) (reconciliation.ReverifyFinding, error) {
		if err := budget.ConsumePG(ctx, 1); err != nil {
			return reconciliation.ReverifyFinding{}, err
		}
		return reconciliation.ReverifyFinding{
			Verdict:     reconciliation.ReverifyConsistent,
			EvidenceRef: "us3it:compare-path:" + item.DiscrepancyID,
			FreshnessAt: time.Now().UTC(),
			Detail:      "test compare-path re-read: all parties re-observed consistent",
		}, nil
	})
}

// us3ITDivergentEvaluator reports the conclusion-affecting change the later
// re-read observed (new evidence on the recorded identity).
func us3ITDivergentEvaluator() reconciliation.ReverifyEvaluator {
	return reconciliation.ReverifyEvaluatorFunc(func(ctx context.Context, item reconciliation.ClosedDiscrepancy,
		budget reconciliation.ScanQueryBudget) (reconciliation.ReverifyFinding, error) {
		if err := budget.ConsumePG(ctx, 1); err != nil {
			return reconciliation.ReverifyFinding{}, err
		}
		return reconciliation.ReverifyFinding{
			Verdict:     reconciliation.ReverifyDivergent,
			EvidenceRef: "us3it:changed-evidence:" + item.DiscrepancyID,
			FreshnessAt: time.Now().UTC(),
			Trigger:     reconciliation.InvalidationNewEvidence,
			Detail:      "test compare-path re-read: the recorded conclusion changed",
		}, nil
	})
}

func TestIntegrationReconcileAdminUS3CallPathConvergence(t *testing.T) {
	ctx := context.Background()
	pool, dsn := recAdminPG(t)
	fundsBefore := recAdminFundsCounts(t, ctx, pool)
	store, err := reconciliation.NewStore(pool)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	env := us3ITEnv(dsn, recAdminPrincipal)

	t.Run("dispose_hands_over_to_pending_verify_and_close_needs_evidence", func(t *testing.T) {
		const chainID = "31361"
		recAdminGrant(t, ctx, pool, reconciliation.PermissionExceptionHandle, us3ITAuthScope(chainID))
		recAdminGrant(t, ctx, pool, reconciliation.PermissionDisposeAck, us3ITAuthScope(chainID))
		recAdminGrant(t, ctx, pool, reconciliation.PermissionClose, us3ITAuthScope(chainID))
		id := uuid.NewString()
		us3ITSeedDiscrepancy(t, ctx, pool, chainID, id, "tx_hash=0x"+strings.Repeat("a1", 32),
			reconciliation.DiscrepancyStateOpenClaimable, "")

		if code, _, stderr := recAdminRun(ctx, env, "claim", "--discrepancy-id", id,
			"--operator", "it-operator", "--reason", "it", "--operation-id", "us3-it-claim-1"); code != 0 {
			t.Fatalf("claim exit = %d, stderr=%q", code, stderr)
		}
		code, stdout, stderr := recAdminRun(ctx, env, "dispose", "--discrepancy-id", id,
			"--kind", "ack_only", "--result", "done", "--operator", "it-operator",
			"--reason", "it", "--operation-id", "us3-it-dispose-1")
		if code != 0 {
			t.Fatalf("dispose exit = %d, stderr=%q", code, stderr)
		}
		if got := recAdminField(t, stdout, "state_after"); got != "pending_verify" {
			t.Fatalf("dispose state_after = %q, want pending_verify", got)
		}

		// The disposed ticket carries no reverify evidence: close must refuse,
		// write no basis and stay pending_verify (disposed != reverified != closed).
		code, closeOut, closeErr := recAdminRun(ctx, env, "close", "--discrepancy-id", id,
			"--close-basis", `{"range":"100..200","block":200,"version":"v1"}`, "--reason", "it")
		if code == 0 {
			t.Fatalf("close without a reverify row exited 0: %s", closeOut)
		}
		if !strings.Contains(closeErr, "close refused") {
			t.Fatalf("close stderr = %q, want an explicit refusal", closeErr)
		}
		if state := us3ITState(t, ctx, pool, id); state != reconciliation.DiscrepancyStatePendingVerify {
			t.Fatalf("state after the refused close = %s, want pending_verify", state)
		}
		if n := us3ITCount(t, ctx, pool, `
			SELECT count(*)::bigint FROM discrepancy
			WHERE discrepancy_id = $1::uuid AND close_basis IS NULL`, id); n != 1 {
			t.Fatalf("the refused close wrote a close_basis")
		}
		if n := us3ITCount(t, ctx, pool, `
			SELECT count(*)::bigint FROM recon_audit
			WHERE action = 'refuse' AND result = 'refused' AND target->>'discrepancy_id' = $1`, id); n != 1 {
			t.Fatalf("refuse audit rows = %d, want 1 (the failed guard is audited)", n)
		}
	})

	t.Run("reverify_cli_requires_bounds_and_never_claims_completeness", func(t *testing.T) {
		const chainID = "31362"
		taskID := recAdminSeedHeightTask(t, ctx, pool, chainID, us3ITFrom, us3ITTo,
			[]string{"withdrawal"}, `{"withdrawal":{"source":"ledger","connected":true}}`)
		recAdminGrant(t, ctx, pool, reconciliation.PermissionScanManage, us3ITAuthScope(chainID))
		id := uuid.NewString()
		us3ITSeedDiscrepancy(t, ctx, pool, chainID, id, "tx_hash=0x"+strings.Repeat("b2", 32),
			reconciliation.DiscrepancyStateClosed,
			`{"range":"100..200","block":101,"version":"v1","observed_at":"2026-01-01T00:00:00Z"}`)

		// Missing slice bounds are refused by name before any work: no default
		// is invented, and nothing is written.
		code, _, stderr := recAdminRun(ctx, env, "reverify", "--task-id", taskID)
		if code == 0 {
			t.Fatalf("reverify without slice bounds exited 0")
		}
		if !strings.Contains(stderr, "--max-items") {
			t.Fatalf("stderr %q does not name the missing --max-items bound", stderr)
		}
		if n := us3ITCount(t, ctx, pool,
			`SELECT count(*)::bigint FROM reverify WHERE discrepancy_id = $1::uuid`, id); n != 0 {
			t.Fatalf("reverify rows after the refused invocation = %d, want 0", n)
		}

		// One bounded slice over the closed item. The production event-state
		// evaluator cannot prove three-party consistency (Q5-4), so the
		// verdict stays unknown, the gap stays visible and completeness is
		// never claimed even though the traversal cursor advanced.
		code, stdout, stderr := recAdminRun(ctx, env, "reverify", "--task-id", taskID,
			"--max-items", "4", "--max-pg-requests", "20", "--max-item-attempts", "3")
		if code != 0 {
			t.Fatalf("reverify exit = %d, stderr=%q", code, stderr)
		}
		for field, want := range map[string]string{
			"rechecked": "1", "consistent": "0", "divergent": "0", "pending": "1",
			// unknown is not consistent/divergent, so it is both pending and a
			// failed attempt (bounded retry keeps it visible).
			"failed": "1", "gaps": "1", "cursor_advanced": "true",
			"stop": "waterline_end", "verified_complete": "false",
		} {
			if got := recAdminField(t, stdout, field); got != want {
				t.Fatalf("reverify %s = %q, want %q (stdout %q)", field, got, want, stdout)
			}
		}
		if verdict := us3ITLatestVerdict(t, ctx, pool, id); verdict != "unknown" {
			t.Fatalf("latest verdict = %q, want unknown (never consistent off a single-party read)", verdict)
		}
		if state := us3ITState(t, ctx, pool, id); state != reconciliation.DiscrepancyStateClosed {
			t.Fatalf("state after an unknown revalidation = %s, want closed (unknown invalidates nothing)", state)
		}
		if !us3ITHistoryCursorAdvanced(t, ctx, pool, taskID) {
			t.Fatalf("history_sweep_through was not persisted after the slice")
		}
	})

	t.Run("consistent_sweep_row_then_evidence_gated_close_then_invalidation", func(t *testing.T) {
		const chainID = "31363"
		taskID := recAdminSeedHeightTask(t, ctx, pool, chainID, us3ITFrom, us3ITTo,
			[]string{"withdrawal"}, `{"withdrawal":{"source":"ledger","connected":true}}`)
		recAdminGrant(t, ctx, pool, reconciliation.PermissionScanManage, us3ITAuthScope(chainID))
		recAdminGrant(t, ctx, pool, reconciliation.PermissionClose, us3ITAuthScope(chainID))
		id := uuid.NewString()
		us3ITSeedDiscrepancy(t, ctx, pool, chainID, id, "tx_hash=0x"+strings.Repeat("c3", 32),
			reconciliation.DiscrepancyStateClosed,
			`{"range":"100..200","block":101,"version":"v1","observed_at":"2026-01-01T00:00:00Z"}`)
		closeBasis := `{"range":"100..200","block":101,"version":"v1","observed_at":"2026-01-02T00:00:00Z"}`

		// Default deny: a principal without the close grant cannot close (the
		// refusal is audited by the evaluator and nothing is written).
		unprivileged := us3ITEnv(dsn, "deploy:us3-unprivileged")
		code, _, stderr := recAdminRun(ctx, unprivileged, "close", "--discrepancy-id", id,
			"--close-basis", closeBasis, "--reason", "it")
		if code == 0 {
			t.Fatalf("close without the close grant exited 0")
		}
		if n := us3ITCount(t, ctx, pool, `
			SELECT count(*)::bigint FROM recon_audit
			WHERE action = 'refuse' AND actor = 'deploy:us3-unprivileged'`); n != 1 {
			t.Fatalf("authorization refusal audits = %d, want 1", n)
		}
		if state := us3ITState(t, ctx, pool, id); state != reconciliation.DiscrepancyStateClosed {
			t.Fatalf("state after the unauthorized close = %s, want closed", state)
		}

		// One real bounded sweep through the T027 executor writes the fresh
		// consistent row (the compare-path evaluator seam; not a DB edit).
		sweep := us3ITSweep(t, ctx, store, taskID, us3ITConsistentEvaluator())
		if sweep.Rechecked != 1 || sweep.Consistent != 1 || sweep.Divergent != 0 || sweep.Failed != 0 {
			t.Fatalf("sweep rechecked/consistent/divergent/failed = %d/%d/%d/%d, want 1/1/0/0",
				sweep.Rechecked, sweep.Consistent, sweep.Divergent, sweep.Failed)
		}
		if !sweep.VerifiedComplete() {
			t.Fatalf("sweep with no open gap and no failure did not reach its honest completeness claim")
		}
		if verdict := us3ITLatestVerdict(t, ctx, pool, id); verdict != "consistent" {
			t.Fatalf("latest verdict after the sweep = %q, want consistent", verdict)
		}

		// A conclusion-affecting change invalidates closed -> pending_verify
		// through the T026 store evaluator (reverify flow only, never a close).
		invalidation, err := store.EvaluateInvalidationForDiscrepancy(ctx, reconciliation.DiscrepancyInvalidationRequest{
			DiscrepancyID: id,
			Signal: &reconciliation.InvalidationSignal{
				Trigger:     reconciliation.InvalidationNewEvidence,
				EvidenceRef: "us3it:conclusion-affecting-change",
			},
			Actor:  "deploy:it-sweep",
			Reason: "us3 it: conclusion-affecting change",
		})
		if err != nil {
			t.Fatalf("EvaluateInvalidationForDiscrepancy: %v", err)
		}
		if !invalidation.Applied || invalidation.State != reconciliation.DiscrepancyStatePendingVerify {
			t.Fatalf("invalidation = applied %t state %s, want applied pending_verify",
				invalidation.Applied, invalidation.State)
		}

		// The real CLI close now succeeds on the fresh consistent row and
		// records the basis snapshot.
		code, stdout, stderr := recAdminRun(ctx, env, "close", "--discrepancy-id", id,
			"--close-basis", closeBasis, "--reason", "it verified consistent")
		if code != 0 {
			t.Fatalf("close exit = %d, stderr=%q", code, stderr)
		}
		if got := recAdminField(t, stdout, "from"); got != "pending_verify" {
			t.Fatalf("close from = %q, want pending_verify", got)
		}
		if got := recAdminField(t, stdout, "to"); got != "closed" {
			t.Fatalf("close to = %q, want closed", got)
		}
		if got := recAdminField(t, stdout, "reopen_count"); got != "0" {
			t.Fatalf("close reopen_count = %q, want 0", got)
		}
		if state := us3ITState(t, ctx, pool, id); state != reconciliation.DiscrepancyStateClosed {
			t.Fatalf("state after the verified close = %s, want closed", state)
		}
		var rangeText, blockText string
		if err := pool.QueryRow(ctx, `
			SELECT close_basis->>'range', close_basis->>'block'
			FROM discrepancy WHERE discrepancy_id = $1::uuid`, id).Scan(&rangeText, &blockText); err != nil {
			t.Fatalf("read the recorded close_basis: %v", err)
		}
		if rangeText != "100..200" || blockText != "101" {
			t.Fatalf("close_basis = range %q block %q, want the recorded snapshot", rangeText, blockText)
		}
		if n := us3ITCount(t, ctx, pool, `
			SELECT count(*)::bigint FROM recon_audit
			WHERE action = 'close' AND target->>'discrepancy_id' = $1`, id); n != 1 {
			t.Fatalf("close audit rows = %d, want 1", n)
		}

		// A later revalidation observes the change. The scan loop's cross-check
		// candidate (an old-range item whose recorded evidence changed) feeds
		// the slice; the divergent verdict supersedes the old consistent row
		// and invalidates closed -> pending_verify again (reverify flow only).
		divergent := us3ITSweep(t, ctx, store, taskID, us3ITDivergentEvaluator(),
			reconciliation.ReverifyCandidate{DiscrepancyID: id, EvidenceRef: "us3it:changed-evidence"})
		if divergent.Divergent != 1 || divergent.InvalidationErrors != 0 {
			t.Fatalf("divergent sweep divergent/invalidation_errors = %d/%d, want 1/0",
				divergent.Divergent, divergent.InvalidationErrors)
		}
		if state := us3ITState(t, ctx, pool, id); state != reconciliation.DiscrepancyStatePendingVerify {
			t.Fatalf("state after the divergent revalidation = %s, want pending_verify", state)
		}
		if verdict := us3ITLatestVerdict(t, ctx, pool, id); verdict != "divergent" {
			t.Fatalf("latest verdict after the divergent revalidation = %q, want divergent", verdict)
		}

		// The old consistent result can no longer close: the latest row is
		// divergent (stale evidence is structurally refused).
		code, _, stderr = recAdminRun(ctx, env, "close", "--discrepancy-id", id,
			"--close-basis", closeBasis, "--reason", "it retry on the old result")
		if code == 0 {
			t.Fatalf("close over the superseded consistent result exited 0")
		}
		if !strings.Contains(stderr, "close refused") {
			t.Fatalf("second close stderr = %q, want an explicit refusal", stderr)
		}
		if state := us3ITState(t, ctx, pool, id); state != reconciliation.DiscrepancyStatePendingVerify {
			t.Fatalf("state after the stale close = %s, want pending_verify", state)
		}
		if verdict := us3ITLatestVerdict(t, ctx, pool, id); verdict != "divergent" {
			t.Fatalf("latest verdict after the stale close = %q, want divergent", verdict)
		}
	})

	fundsAfter := recAdminFundsCounts(t, ctx, pool)
	for _, table := range recAdminFundsTables {
		if fundsAfter[table] != fundsBefore[table] {
			t.Fatalf("014 US3 paths wrote the funds table %s: rows %d -> %d; no payment/signature/broadcast side effect is permitted",
				table, fundsBefore[table], fundsAfter[table])
		}
	}
}
