//go:build integration

// ticketverify_integration_test.go is the real-entry acceptance layer for the
// production pending_verify re-verification (`reconcile-admin
// reverify-ticket`): a legitimate discrepancy is detected by the real scan
// path, disposed into pending_verify, the underlying fact converges, the real
// full-comparison entry re-verifies it as `consistent`, and an authorized
// close succeeds — all through the built `txharbor` binary, a real PostgreSQL
// and a real isolated Anvil chain. No consistent evaluator is injected, no
// reverify row is hand-inserted and no ticket state is hand-modified.
//
// Negative coverage in the same harness:
//
//   - an unrepaired missing ticket and a partially recorded tx aggregate stay
//     divergent and cannot close;
//   - an event-coverage gap stays unknown/pending with a visible gap and
//     cannot close;
//   - a ticket outside the task scope and a ticket that is not pending_verify
//     are refused and audited;
//   - a conclusion-affecting change after a consistent verdict, an expired
//     consistent row and a revoked close permission are each refused;
//   - concurrent closes on one consistent row: exactly one wins (row-lock
//     CAS); concurrent re-verifications append bounded rows without lifecycle
//     changes;
//   - repeated re-verification is idempotent (same verdict, append-only rows),
//     the 007-012 money-path tables do not move around verify/close, and the
//     closed-history sweep still runs unchanged (no regression).
//
// PostgreSQL and Anvil come from testcontainers. When no Docker provider is
// healthy the package reports NOT RUN (t.Skip), never a pass.
package reconcileadmin

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/events"
	"github.com/xtianxx/txharbor/internal/reconciliation"
	"github.com/xtianxx/txharbor/internal/withdrawal"
)

const recPtConfirmN = "3"

// recPtBuildBinary builds the ordinary repo binary (no special tags) and
// returns its path; every invocation below runs the built binary itself.
func recPtBuildBinary(t *testing.T) string {
	t.Helper()
	out := filepath.Join(t.TempDir(), "txharbor")
	build := exec.Command("go", "build", "-o", out, "./cmd/txharbor")
	build.Dir = "../../.." // repo root relative to internal/app/reconcileadmin
	var errBuf bytes.Buffer
	build.Stderr = &errBuf
	if err := build.Run(); err != nil {
		t.Fatalf("go build ./cmd/txharbor: %v\n%s", err, errBuf.String())
	}
	return out
}

// recPtEnv is the full command environment of every binary invocation.
func recPtEnv(dsn, rpcURL string) map[string]string {
	env := recAdminEnv(dsn, []string{recAdminSender}, 8)
	env[config.EnvRPCURL] = rpcURL
	return env
}

// recPtRun runs the built binary with the given environment and returns its
// exit code plus captured output.
func recPtRun(t *testing.T, ctx context.Context, bin string, env map[string]string, args ...string) (int, string, string) {
	t.Helper()
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + os.Getenv("HOME"),
	}
	for key, value := range env {
		cmd.Env = append(cmd.Env, key+"="+value)
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	if err == nil {
		return 0, stdout.String(), stderr.String()
	}
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		return exit.ExitCode(), stdout.String(), stderr.String()
	}
	t.Fatalf("run %s: %v", strings.Join(args, " "), err)
	return -1, "", ""
}

// recPtRunAsync is the concurrent variant: it returns a channel with the exit
// code (never fails the test from the goroutine).
func recPtRunAsync(bin string, env map[string]string, args ...string) <-chan int {
	done := make(chan int, 1)
	go func() {
		cmd := exec.Command(bin, args...)
		cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME")}
		for key, value := range env {
			cmd.Env = append(cmd.Env, key+"="+value)
		}
		var stdout, stderr bytes.Buffer
		cmd.Stdout, cmd.Stderr = &stdout, &stderr
		err := cmd.Run()
		if err == nil {
			done <- 0
			return
		}
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			done <- exit.ExitCode()
			return
		}
		done <- -1
	}()
	return done
}

// recPtStartResumeScan drives the real command entry end to end: start,
// resume, scan.
func recPtStartResumeScan(t *testing.T, ctx context.Context, bin string, env map[string]string,
	taskID, chainID string, from, to int64, businessTypes, upstream string) string {
	t.Helper()
	if code, _, stderr := recPtRun(t, ctx, bin, env, "reconcile-admin", "start",
		"--task-id", taskID, "--chain-id", chainID, "--scope-kind", "height",
		"--from", fmt.Sprint(from), "--to", fmt.Sprint(to),
		"--business-types", businessTypes, "--confirm-threshold-n", recPtConfirmN,
		"--upstream-receipts", upstream); code != 0 {
		t.Fatalf("start exit = %d, stderr=%q", code, stderr)
	}
	if code, _, stderr := recPtRun(t, ctx, bin, env, "reconcile-admin", "resume",
		"--task-id", taskID, "--reason", "it"); code != 0 {
		t.Fatalf("resume exit = %d, stderr=%q", code, stderr)
	}
	code, stdout, stderr := recPtRun(t, ctx, bin, env, "reconcile-admin", "scan", "--task-id", taskID)
	if code != 0 {
		t.Fatalf("scan exit = %d, stderr=%q stdout=%q", code, stderr, stdout)
	}
	return stdout
}

// recPtDispose hands one ticket to pending_verify through the real claim and
// dispose commands.
func recPtDispose(t *testing.T, ctx context.Context, bin string, env map[string]string,
	discrepancyID, suffix string) {
	t.Helper()
	if code, _, stderr := recPtRun(t, ctx, bin, env, "reconcile-admin", "claim",
		"--discrepancy-id", discrepancyID, "--operator", "it-op", "--reason", "it",
		"--operation-id", "op-claim-"+suffix); code != 0 {
		t.Fatalf("claim exit = %d, stderr=%q", code, stderr)
	}
	code, stdout, stderr := recPtRun(t, ctx, bin, env, "reconcile-admin", "dispose",
		"--discrepancy-id", discrepancyID, "--kind", "ack_only", "--result", "done",
		"--operator", "it-op", "--reason", "it", "--operation-id", "op-dispose-"+suffix)
	if code != 0 {
		t.Fatalf("dispose exit = %d, stderr=%q", code, stderr)
	}
	if got := recAdminField(t, stdout, "state_after"); got != "pending_verify" {
		t.Fatalf("dispose state_after = %q, want pending_verify", got)
	}
}

// recPtTicketID reads the id of one business-key ticket.
func recPtTicketID(t *testing.T, ctx context.Context, pool *pgxpool.Pool, businessKey string) string {
	t.Helper()
	var id string
	if err := pool.QueryRow(ctx,
		`SELECT discrepancy_id::text FROM discrepancy WHERE business_key = $1`, businessKey).Scan(&id); err != nil {
		t.Fatalf("read ticket %s: %v", businessKey, err)
	}
	return id
}

// recPtState reads the ticket state.
func recPtState(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id string) reconciliation.DiscrepancyState {
	t.Helper()
	var state string
	if err := pool.QueryRow(ctx,
		`SELECT state FROM discrepancy WHERE discrepancy_id = $1::uuid`, id).Scan(&state); err != nil {
		t.Fatalf("read state of %s: %v", id, err)
	}
	return reconciliation.DiscrepancyState(state)
}

// recPtLatestVerdict reads the newest reverify verdict ("" when none).
func recPtLatestVerdict(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id string) string {
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

// recPtCount runs one scalar count.
func recPtCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, query string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(ctx, query, args...).Scan(&n); err != nil {
		t.Fatalf("count query: %v", err)
	}
	return n
}

// recPtGrantScope grants one exact-scope permission of the test principal.
func recPtGrantScope(t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	permission reconciliation.Permission, chainID string,
	types []reconciliation.BusinessType, from, to int64) {
	t.Helper()
	recHdrGrant(t, ctx, pool, permission, reconciliation.AuthScope{
		ChainID: chainID, Kind: reconciliation.ScopeHeight,
		BusinessTypes: types, RangeStart: &from, RangeEnd: &to,
	})
}

// recPtSeedWithdrawalTicket seeds one withdrawal chain fact in the scope.
func recPtSeedWithdrawalTicket(t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	chainID int64, headers map[int64]recHdrHeader, block int64, txHash string) {
	t.Helper()
	recHdrInsertLog(t, ctx, pool, chainID, block, headers[block].Hash, txHash, 0,
		recAdminAsset, recAdminSender, recAdminOutsider)
}

func TestIntegrationReconcileAdminPendingVerifyAcceptance(t *testing.T) {
	ctx := context.Background()
	pool, dsn := recAdminPG(t)
	rpcURL, headers := recHdrAnvil(t)
	bin := recPtBuildBinary(t)
	env := recPtEnv(dsn, rpcURL)

	// ---------------------------------------------------------------------
	// 1. positive closed loop: real missing detection -> dispose ->
	//    fact convergence -> real consistent re-verification -> authorized
	//    close. Everything runs through the built binary.
	// ---------------------------------------------------------------------
	t.Run("positive_loop_missing_converges_then_verify_consistent_then_close", func(t *testing.T) {
		chainID := int64(33101)
		recHdrSeedChain(t, ctx, pool, chainID, headers, 2, 5)
		recAdminSeedDepositConfig(t, ctx, pool, chainID, 1, 0, recAdminAsset+":0", recAdminWatch+":0")
		txHash := "0x" + strings.Repeat("a1", 32)
		recPtSeedWithdrawalTicket(t, ctx, pool, chainID, headers, 4, txHash)
		recPtGrantScope(t, ctx, pool, reconciliation.PermissionScanManage, "33101",
			[]reconciliation.BusinessType{reconciliation.BusinessWithdrawal}, 2, 5)
		recPtGrantScope(t, ctx, pool, reconciliation.PermissionExceptionHandle, "33101",
			[]reconciliation.BusinessType{reconciliation.BusinessWithdrawal}, 2, 5)
		recPtGrantScope(t, ctx, pool, reconciliation.PermissionDisposeAck, "33101",
			[]reconciliation.BusinessType{reconciliation.BusinessWithdrawal}, 2, 5)
		recPtGrantScope(t, ctx, pool, reconciliation.PermissionClose, "33101",
			[]reconciliation.BusinessType{reconciliation.BusinessWithdrawal}, 2, 5)

		taskID := recHdrTaskAndGrant(t, ctx, pool, "33101", 2, 5, []string{"withdrawal"})
		out := recPtStartResumeScan(t, ctx, bin, env, taskID, "33101", 2, 5,
			"withdrawal", "withdrawal=ledger:connected")
		t.Logf("scan output: %s", out)
		recHdrAssertFields(t, out, map[string]string{
			"stop": "scope_exhausted", "tickets": "1", "pending": "0", "gaps": "0",
		})
		id := recPtTicketID(t, ctx, pool, "tx_hash="+txHash)
		if state := recPtState(t, ctx, pool, id); state != reconciliation.DiscrepancyStateOpenClaimable {
			t.Fatalf("state after scan = %s, want open_claimable", state)
		}
		recPtDispose(t, ctx, bin, env, id, "positive")
		if state := recPtState(t, ctx, pool, id); state != reconciliation.DiscrepancyStatePendingVerify {
			t.Fatalf("state after dispose = %s, want pending_verify", state)
		}

		// The fact converges through the real authoritative 011/012 execution
		// chain of the same transaction (the fixture boundary: the underlying
		// business fact, never a verification verdict).
		recHdrSeedWithdrawalChain(t, ctx, pool, chainID, txHash, 4, headers[4].Hash, true)

		// Snapshot the money-path tables after convergence; verify/close must
		// not move a single row of them.
		fundsBefore := recHdrFundsSnapshot(t, ctx, pool)

		// Before the re-verification: close must refuse (no reverify row).
		if code, _, stderr := recPtRun(t, ctx, bin, env, "reconcile-admin", "close",
			"--discrepancy-id", id, "--close-basis", `{"range":"2..5","block":4}`,
			"--reason", "it premature"); code == 0 {
			t.Fatalf("close before re-verification exited 0")
		} else if !strings.Contains(stderr, "close refused") {
			t.Fatalf("premature close stderr = %q, want an explicit refusal", stderr)
		}

		// The real re-verification: full three-party comparison through the
		// production adapters. The tx_hash aggregate carries no catalog event
		// aggregate, so the event dimension is N/A (R2) and chain+PG agreement
		// is conclusive.
		code, stdout, stderr := recPtRun(t, ctx, bin, env, "reconcile-admin", "reverify-ticket",
			"--task-id", taskID, "--discrepancy-id", id, "--max-pg-requests", "40", "--max-item-attempts", "3")
		if code != 0 {
			t.Fatalf("reverify-ticket exit = %d, stderr=%q stdout=%q", code, stderr, stdout)
		}
		t.Logf("reverify-ticket output: %s", stdout)
		recHdrAssertFields(t, stdout, map[string]string{
			"verdict": "consistent", "consistent": "true", "pending": "false",
			"failed": "false", "discarded": "false",
		})
		if verdict := recPtLatestVerdict(t, ctx, pool, id); verdict != "consistent" {
			t.Fatalf("latest verdict = %q, want consistent", verdict)
		}
		if state := recPtState(t, ctx, pool, id); state != reconciliation.DiscrepancyStatePendingVerify {
			t.Fatalf("state after re-verification = %s, want pending_verify (verify writes no lifecycle edge)", state)
		}
		var freshness *time.Time
		if err := pool.QueryRow(ctx, `
			SELECT freshness_at FROM reverify WHERE discrepancy_id = $1::uuid
			ORDER BY created_at DESC, reverify_id DESC LIMIT 1`, id).Scan(&freshness); err != nil {
			t.Fatalf("read reverify freshness: %v", err)
		}
		if freshness == nil {
			t.Fatalf("consistent reverify row has no freshness_at")
		}
		var evidenceRef string
		if err := pool.QueryRow(ctx, `
			SELECT evidence_ref FROM reverify WHERE discrepancy_id = $1::uuid
			ORDER BY created_at DESC, reverify_id DESC LIMIT 1`, id).Scan(&evidenceRef); err != nil {
			t.Fatalf("read reverify evidence_ref: %v", err)
		}
		if evidenceRef == "" || !strings.Contains(evidenceRef, "ticketverify/v1") {
			t.Fatalf("consistent evidence_ref = %q, want the bounded ticketverify ref", evidenceRef)
		}

		// The authorized close succeeds on the fresh row and records the
		// basis snapshot.
		code, stdout, stderr = recPtRun(t, ctx, bin, env, "reconcile-admin", "close",
			"--discrepancy-id", id, "--close-basis", `{"range":"2..5","block":4,"version":"v1"}`,
			"--reason", "it verified")
		if code != 0 {
			t.Fatalf("close exit = %d, stderr=%q stdout=%q", code, stderr, stdout)
		}
		recHdrAssertFields(t, stdout, map[string]string{"from": "pending_verify", "to": "closed"})
		if state := recPtState(t, ctx, pool, id); state != reconciliation.DiscrepancyStateClosed {
			t.Fatalf("state after close = %s, want closed", state)
		}

		fundsAfter := recHdrFundsSnapshot(t, ctx, pool)
		for table, snapshot := range fundsBefore {
			if fundsAfter[table] != snapshot {
				t.Fatalf("reverify-ticket/close wrote the money-path table %s: %s -> %s",
					table, snapshot, fundsAfter[table])
			}
		}

		// Idempotent repeat: re-running the closed-history sweep on the closed
		// item still works (production single-party evaluator -> unknown, no
		// lifecycle change), proving the sweep contract is unchanged.
		code, sweepOut, sweepErr := recPtRun(t, ctx, bin, env, "reconcile-admin", "reverify",
			"--task-id", taskID, "--max-items", "4", "--max-pg-requests", "40", "--max-item-attempts", "3")
		if code != 0 {
			t.Fatalf("closed sweep exit = %d, stderr=%q", code, sweepErr)
		}
		t.Logf("closed sweep output: %s", sweepOut)
		if verdict := recPtLatestVerdict(t, ctx, pool, id); verdict != "unknown" {
			t.Fatalf("latest verdict after the closed sweep = %q, want unknown (single-party re-read)", verdict)
		}
		if state := recPtState(t, ctx, pool, id); state != reconciliation.DiscrepancyStateClosed {
			t.Fatalf("closed sweep changed the state to %s, want closed (sweep never invalidates on unknown)", state)
		}
	})

	// ---------------------------------------------------------------------
	// 2. unrepaired missing and partially recorded aggregate cannot close.
	// ---------------------------------------------------------------------
	t.Run("unrepaired_missing_and_partial_aggregate_stay_divergent", func(t *testing.T) {
		// 2a. unrepaired missing withdrawal tx.
		chainID := int64(33102)
		recHdrSeedChain(t, ctx, pool, chainID, headers, 2, 5)
		recAdminSeedDepositConfig(t, ctx, pool, chainID, 1, 0, recAdminAsset+":0", recAdminWatch+":0")
		txHash := "0x" + strings.Repeat("b2", 32)
		recPtSeedWithdrawalTicket(t, ctx, pool, chainID, headers, 3, txHash)
		for _, permission := range []reconciliation.Permission{
			reconciliation.PermissionScanManage, reconciliation.PermissionExceptionHandle,
			reconciliation.PermissionDisposeAck, reconciliation.PermissionClose,
		} {
			recPtGrantScope(t, ctx, pool, permission, "33102",
				[]reconciliation.BusinessType{reconciliation.BusinessWithdrawal}, 2, 5)
		}
		taskID := recHdrTaskAndGrant(t, ctx, pool, "33102", 2, 5, []string{"withdrawal"})
		recPtStartResumeScan(t, ctx, bin, env, taskID, "33102", 2, 5, "withdrawal", "withdrawal=ledger:connected")
		id := recPtTicketID(t, ctx, pool, "tx_hash="+txHash)
		recPtDispose(t, ctx, bin, env, id, "unrepaired")

		code, stdout, stderr := recPtRun(t, ctx, bin, env, "reconcile-admin", "reverify-ticket",
			"--task-id", taskID, "--discrepancy-id", id, "--max-pg-requests", "40", "--max-item-attempts", "3")
		if code != 0 {
			t.Fatalf("unrepaired reverify-ticket exit = %d, stderr=%q", code, stderr)
		}
		recHdrAssertFields(t, stdout, map[string]string{"verdict": "divergent", "consistent": "false"})
		if verdict := recPtLatestVerdict(t, ctx, pool, id); verdict != "divergent" {
			t.Fatalf("unrepaired latest verdict = %q, want divergent", verdict)
		}
		if code, _, _ := recPtRun(t, ctx, bin, env, "reconcile-admin", "close",
			"--discrepancy-id", id, "--close-basis", `{"range":"2..5"}`, "--reason", "it"); code == 0 {
			t.Fatalf("close over an unrepaired missing ticket exited 0")
		}

		// 2b. partially recorded deposit aggregate: two project credit logs in
		// one transaction, only one stored credit row.
		partialChain := int64(33103)
		recHdrSeedChain(t, ctx, pool, partialChain, headers, 2, 5)
		recAdminSeedDepositConfig(t, ctx, pool, partialChain, 1, 0, recAdminAsset+":0", recAdminWatch+":0")
		partialTx := "0x" + strings.Repeat("c3", 32)
		for logIndex := int64(0); logIndex < 2; logIndex++ {
			recHdrInsertLog(t, ctx, pool, partialChain, 4, headers[4].Hash, partialTx, logIndex,
				recAdminAsset, recAdminOutsider, recAdminWatch)
		}
		for _, permission := range []reconciliation.Permission{
			reconciliation.PermissionScanManage, reconciliation.PermissionExceptionHandle,
			reconciliation.PermissionDisposeAck, reconciliation.PermissionClose,
		} {
			recPtGrantScope(t, ctx, pool, permission, "33103",
				[]reconciliation.BusinessType{reconciliation.BusinessDeposit}, 2, 5)
		}
		partialTask := recHdrTaskAndGrant(t, ctx, pool, "33103", 2, 5, []string{"deposit"})
		recPtStartResumeScan(t, ctx, bin, env, partialTask, "33103", 2, 5, "deposit", "deposit=ledger:connected")
		partialID := recPtTicketID(t, ctx, pool, "tx_hash="+partialTx)
		recPtDispose(t, ctx, bin, env, partialID, "partial")
		// Only log 0 converges.
		recHdrSeedDepositObservation(t, ctx, pool, partialChain, 1, 4, headers[4].Hash, partialTx, 0,
			recAdminAsset, recAdminOutsider, recAdminWatch)

		code, stdout, stderr = recPtRun(t, ctx, bin, env, "reconcile-admin", "reverify-ticket",
			"--task-id", partialTask, "--discrepancy-id", partialID, "--max-pg-requests", "40", "--max-item-attempts", "3")
		if code != 0 {
			t.Fatalf("partial aggregate reverify-ticket exit = %d, stderr=%q", code, stderr)
		}
		t.Logf("partial aggregate output: %s", stdout)
		recHdrAssertFields(t, stdout, map[string]string{"verdict": "divergent", "consistent": "false"})
		if !strings.Contains(stdout, "partially recorded") {
			t.Fatalf("partial aggregate output %q does not report the partial recording", stdout)
		}
		if code, _, _ := recPtRun(t, ctx, bin, env, "reconcile-admin", "close",
			"--discrepancy-id", partialID, "--close-basis", `{"range":"2..5"}`, "--reason", "it"); code == 0 {
			t.Fatalf("close over a partially recorded aggregate exited 0")
		}
	})

	// ---------------------------------------------------------------------
	// 3. unknown/gap coverage cannot close; wrong state and out-of-scope
	//    tickets are refused and audited.
	// ---------------------------------------------------------------------
	t.Run("unknown_gap_and_scope_refusals", func(t *testing.T) {
		chainID := int64(33104)
		recHdrSeedChain(t, ctx, pool, chainID, headers, 2, 5)
		recAdminSeedDepositConfig(t, ctx, pool, chainID, 1, 0, recAdminAsset+":0", recAdminWatch+":0")
		txHash := "0x" + strings.Repeat("d4", 32)
		recPtSeedWithdrawalTicket(t, ctx, pool, chainID, headers, 4, txHash)
		for _, permission := range []reconciliation.Permission{
			reconciliation.PermissionScanManage, reconciliation.PermissionExceptionHandle,
			reconciliation.PermissionDisposeAck, reconciliation.PermissionClose,
		} {
			recPtGrantScope(t, ctx, pool, permission, "33104",
				[]reconciliation.BusinessType{reconciliation.BusinessWithdrawal}, 2, 5)
		}
		taskID := recHdrTaskAndGrant(t, ctx, pool, "33104", 2, 5, []string{"withdrawal"})
		recPtStartResumeScan(t, ctx, bin, env, taskID, "33104", 2, 5, "withdrawal", "withdrawal=ledger:connected")
		id := recPtTicketID(t, ctx, pool, "tx_hash="+txHash)

		// Wrong state: an open_claimable ticket is refused and audited before
		// any evidence read.
		code, _, _ := recPtRun(t, ctx, bin, env, "reconcile-admin", "reverify-ticket",
			"--task-id", taskID, "--discrepancy-id", id, "--max-pg-requests", "40", "--max-item-attempts", "3")
		if code == 0 {
			t.Fatalf("reverify-ticket on an open_claimable ticket exited 0")
		}
		if n := recPtCount(t, ctx, pool, `
			SELECT count(*)::bigint FROM recon_audit
			WHERE action = 'refuse' AND result = 'refused' AND target->>'discrepancy_id' = $1`, id); n != 1 {
			t.Fatalf("wrong-state refusal audits = %d, want 1", n)
		}
		if n := recPtCount(t, ctx, pool,
			`SELECT count(*)::bigint FROM reverify WHERE discrepancy_id = $1::uuid`, id); n != 0 {
			t.Fatalf("wrong-state refusal wrote %d reverify rows, want 0", n)
		}

		// Unclosed event coverage: a published-but-unconsumed event in the
		// re-read window downgrades the event bundle, so the comparison cannot
		// conclude anything.
		recPtDispose(t, ctx, bin, env, id, "unknown")
		occurredAt := headers[4].Time
		recHdrSeedOutbox(t, ctx, pool, chainID, recHdrEventRow{
			IdentityKind: "business_object", EventType: events.EventTypeWithdrawalRequestReceived,
			AggregateType: withdrawal.RequestAggregateType, AggregateID: "pending-" + strings.Repeat("e", 8),
			AggregateVersion: 1, OccurredAt: occurredAt, PublishState: "pending",
		})
		code, stdout, stderr := recPtRun(t, ctx, bin, env, "reconcile-admin", "reverify-ticket",
			"--task-id", taskID, "--discrepancy-id", id, "--max-pg-requests", "40", "--max-item-attempts", "3")
		if code != 0 {
			t.Fatalf("unclosed-coverage reverify-ticket exit = %d, stderr=%q", code, stderr)
		}
		t.Logf("unclosed coverage output: %s", stdout)
		recHdrAssertFields(t, stdout, map[string]string{
			"verdict": "unknown", "consistent": "false", "pending": "true", "gap": "true",
		})
		if code, _, _ := recPtRun(t, ctx, bin, env, "reconcile-admin", "close",
			"--discrepancy-id", id, "--close-basis", `{"range":"2..5"}`, "--reason", "it"); code == 0 {
			t.Fatalf("close over unknown/gapped evidence exited 0")
		}

		// Out-of-scope: a second task whose scope does not contain the ticket.
		otherTask := recHdrTaskAndGrant(t, ctx, pool, "33104", 3, 4, []string{"withdrawal"})
		if code, _, stderr := recPtRun(t, ctx, bin, env, "reconcile-admin", "start",
			"--task-id", otherTask, "--chain-id", "33104", "--scope-kind", "height",
			"--from", "3", "--to", "4", "--business-types", "withdrawal",
			"--confirm-threshold-n", recPtConfirmN, "--upstream-receipts", "withdrawal=ledger:connected"); code != 0 {
			t.Fatalf("other task start exit = %d, stderr=%q", code, stderr)
		}
		if code, _, stderr := recPtRun(t, ctx, bin, env, "reconcile-admin", "resume",
			"--task-id", otherTask, "--reason", "it"); code != 0 {
			t.Fatalf("other task resume exit = %d, stderr=%q", code, stderr)
		}
		code, _, _ = recPtRun(t, ctx, bin, env, "reconcile-admin", "reverify-ticket",
			"--task-id", otherTask, "--discrepancy-id", id, "--max-pg-requests", "40", "--max-item-attempts", "3")
		if code == 0 {
			t.Fatalf("reverify-ticket under a non-containing task scope exited 0")
		}
		if n := recPtCount(t, ctx, pool, `
			SELECT count(*)::bigint FROM recon_audit
			WHERE action = 'refuse' AND target->>'task_id' = $1`, otherTask); n != 1 {
			t.Fatalf("out-of-scope refusal audits = %d, want 1", n)
		}
	})

	// ---------------------------------------------------------------------
	// 4. change / expiry / revocation / concurrency refusals and idempotent
	//    repeat verification.
	// ---------------------------------------------------------------------
	t.Run("change_expiry_revocation_concurrency", func(t *testing.T) {
		chainID := int64(33105)
		recHdrSeedChain(t, ctx, pool, chainID, headers, 2, 5)
		recAdminSeedDepositConfig(t, ctx, pool, chainID, 1, 0, recAdminAsset+":0", recAdminWatch+":0")
		txHash := "0x" + strings.Repeat("f5", 32)
		recPtSeedWithdrawalTicket(t, ctx, pool, chainID, headers, 4, txHash)
		for _, permission := range []reconciliation.Permission{
			reconciliation.PermissionScanManage, reconciliation.PermissionExceptionHandle,
			reconciliation.PermissionDisposeAck, reconciliation.PermissionClose,
		} {
			recPtGrantScope(t, ctx, pool, permission, "33105",
				[]reconciliation.BusinessType{reconciliation.BusinessWithdrawal}, 2, 5)
		}
		taskID := recHdrTaskAndGrant(t, ctx, pool, "33105", 2, 5, []string{"withdrawal"})
		recPtStartResumeScan(t, ctx, bin, env, taskID, "33105", 2, 5, "withdrawal", "withdrawal=ledger:connected")
		id := recPtTicketID(t, ctx, pool, "tx_hash="+txHash)
		recPtDispose(t, ctx, bin, env, id, "change")

		// Convergence, then repeated real re-verifications: both append
		// consistent rows, the state never moves, and the verdict is stable.
		recHdrSeedWithdrawalChain(t, ctx, pool, chainID, txHash, 4, headers[4].Hash, true)
		fundsBefore := recHdrFundsSnapshot(t, ctx, pool)
		for i := 0; i < 2; i++ {
			code, stdout, stderr := recPtRun(t, ctx, bin, env, "reconcile-admin", "reverify-ticket",
				"--task-id", taskID, "--discrepancy-id", id, "--max-pg-requests", "40", "--max-item-attempts", "3",
				"--reason", fmt.Sprintf("it repeat %d", i))
			if code != 0 {
				t.Fatalf("repeat reverify-ticket %d exit = %d, stderr=%q", i, code, stderr)
			}
			recHdrAssertFields(t, stdout, map[string]string{"verdict": "consistent", "consistent": "true"})
		}
		if n := recPtCount(t, ctx, pool,
			`SELECT count(*)::bigint FROM reverify WHERE discrepancy_id = $1::uuid AND verdict = 'consistent'`, id); n != 2 {
			t.Fatalf("consistent reverify rows after repeats = %d, want 2 (append-only)", n)
		}
		if state := recPtState(t, ctx, pool, id); state != reconciliation.DiscrepancyStatePendingVerify {
			t.Fatalf("state after repeats = %s, want pending_verify", state)
		}
		fundsAfter := recHdrFundsSnapshot(t, ctx, pool)
		for table, snapshot := range fundsBefore {
			if fundsAfter[table] != snapshot {
				t.Fatalf("reverify-ticket repeats wrote the money-path table %s: %s -> %s",
					table, snapshot, fundsAfter[table])
			}
		}

		// Concurrent verifications: all perform one bounded attempt and only
		// append verdict rows; no lifecycle change, no error.
		var wg sync.WaitGroup
		results := make([]int, 3)
		for i := range results {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				done := recPtRunAsync(bin, env, "reconcile-admin", "reverify-ticket",
					"--task-id", taskID, "--discrepancy-id", id, "--max-pg-requests", "40",
					"--max-item-attempts", "9", "--reason", "it concurrent")
				results[index] = <-done
			}(i)
		}
		wg.Wait()
		for index, code := range results {
			if code != 0 {
				t.Fatalf("concurrent reverify-ticket %d exit = %d, want 0", index, code)
			}
		}
		if state := recPtState(t, ctx, pool, id); state != reconciliation.DiscrepancyStatePendingVerify {
			t.Fatalf("state after concurrent verifications = %s, want pending_verify", state)
		}

		// Expiry: a positive but tighter close tolerance must refuse the old
		// (minute-old) evidence row.
		if code, _, stderr := recPtRun(t, ctx, bin, env, "reconcile-admin", "close",
			"--discrepancy-id", id, "--close-basis", `{"range":"2..5"}`,
			"--reason", "it expired", "--reverify-tolerance", "1ns"); code == 0 {
			t.Fatalf("close with an expired tolerance exited 0")
		} else if !strings.Contains(stderr, "close refused") {
			t.Fatalf("expired close stderr = %q, want an explicit refusal", stderr)
		}

		// Revocation: removing the close grant refuses (and audits) even with
		// a fresh consistent row.
		if _, err := pool.Exec(ctx, `
			DELETE FROM recon_permission WHERE principal = $1 AND action = $2`,
			recAdminPrincipal, string(reconciliation.PermissionClose)); err != nil {
			t.Fatalf("revoke close grant: %v", err)
		}
		if code, _, stderr := recPtRun(t, ctx, bin, env, "reconcile-admin", "close",
			"--discrepancy-id", id, "--close-basis", `{"range":"2..5"}`, "--reason", "it revoked"); code == 0 {
			t.Fatalf("close after revocation exited 0")
		} else if !strings.Contains(stderr, "refused") {
			t.Fatalf("revoked close stderr = %q, want a refusal", stderr)
		}
		recPtGrantScope(t, ctx, pool, reconciliation.PermissionClose, "33105",
			[]reconciliation.BusinessType{reconciliation.BusinessWithdrawal}, 2, 5)
		if state := recPtState(t, ctx, pool, id); state != reconciliation.DiscrepancyStatePendingVerify {
			t.Fatalf("state after refusals = %s, want pending_verify", state)
		}

		// Concurrent closes: exactly one wins the row-lock CAS.
		closeCodes := make([]int, 3)
		var closeWG sync.WaitGroup
		for i := range closeCodes {
			closeWG.Add(1)
			go func(index int) {
				defer closeWG.Done()
				done := recPtRunAsync(bin, env, "reconcile-admin", "close",
					"--discrepancy-id", id, "--close-basis", `{"range":"2..5"}`, "--reason", "it concurrent close")
				closeCodes[index] = <-done
			}(i)
		}
		closeWG.Wait()
		winners := 0
		for _, code := range closeCodes {
			if code == 0 {
				winners++
			}
		}
		if winners != 1 {
			t.Fatalf("concurrent close winners = %d, want exactly 1 (codes %v)", winners, closeCodes)
		}
		if state := recPtState(t, ctx, pool, id); state != reconciliation.DiscrepancyStateClosed {
			t.Fatalf("state after concurrent closes = %s, want closed", state)
		}

		// Change after a consistent verdict: remove the authoritative PG row
		// (the fact changes back) and re-verify; the new divergent verdict
		// supersedes the old consistent one and a later close must refuse.
		secondChain := int64(33106)
		recHdrSeedChain(t, ctx, pool, secondChain, headers, 2, 5)
		recAdminSeedDepositConfig(t, ctx, pool, secondChain, 1, 0, recAdminAsset+":0", recAdminWatch+":0")
		secondTx := "0x" + strings.Repeat("a6", 32)
		recPtSeedWithdrawalTicket(t, ctx, pool, secondChain, headers, 4, secondTx)
		for _, permission := range []reconciliation.Permission{
			reconciliation.PermissionScanManage, reconciliation.PermissionExceptionHandle,
			reconciliation.PermissionDisposeAck, reconciliation.PermissionClose,
		} {
			recPtGrantScope(t, ctx, pool, permission, "33106",
				[]reconciliation.BusinessType{reconciliation.BusinessWithdrawal}, 2, 5)
		}
		secondTask := recHdrTaskAndGrant(t, ctx, pool, "33106", 2, 5, []string{"withdrawal"})
		recPtStartResumeScan(t, ctx, bin, env, secondTask, "33106", 2, 5, "withdrawal", "withdrawal=ledger:connected")
		secondID := recPtTicketID(t, ctx, pool, "tx_hash="+secondTx)
		recPtDispose(t, ctx, bin, env, secondID, "change2")
		recHdrSeedWithdrawalChain(t, ctx, pool, secondChain, secondTx, 4, headers[4].Hash, true)
		if code, _, stderr := recPtRun(t, ctx, bin, env, "reconcile-admin", "reverify-ticket",
			"--task-id", secondTask, "--discrepancy-id", secondID,
			"--max-pg-requests", "40", "--max-item-attempts", "3"); code != 0 {
			t.Fatalf("change-case first reverify-ticket exit = %d, stderr=%q", code, stderr)
		}
		if verdict := recPtLatestVerdict(t, ctx, pool, secondID); verdict != "consistent" {
			t.Fatalf("change-case first verdict = %q, want consistent", verdict)
		}
		// The fact changes back (the authoritative execution row disappears).
		if _, err := pool.Exec(ctx, `DELETE FROM tx_receipts`); err != nil {
			t.Fatalf("remove receipt: %v", err)
		}
		if _, err := pool.Exec(ctx, `DELETE FROM tx_attempt_signings`); err != nil {
			t.Fatalf("remove signing: %v", err)
		}
		code, stdout, stderr := recPtRun(t, ctx, bin, env, "reconcile-admin", "reverify-ticket",
			"--task-id", secondTask, "--discrepancy-id", secondID,
			"--max-pg-requests", "40", "--max-item-attempts", "3", "--reason", "it changed")
		if code != 0 {
			t.Fatalf("change-case second reverify-ticket exit = %d, stderr=%q", code, stderr)
		}
		recHdrAssertFields(t, stdout, map[string]string{"verdict": "divergent", "consistent": "false"})
		if verdict := recPtLatestVerdict(t, ctx, pool, secondID); verdict != "divergent" {
			t.Fatalf("change-case latest verdict = %q, want divergent", verdict)
		}
		if code, _, _ := recPtRun(t, ctx, bin, env, "reconcile-admin", "close",
			"--discrepancy-id", secondID, "--close-basis", `{"range":"2..5"}`, "--reason", "it stale"); code == 0 {
			t.Fatalf("close over a superseded consistent row exited 0")
		}
	})
}
