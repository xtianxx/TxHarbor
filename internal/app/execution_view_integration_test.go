//go:build integration

// execution_view_integration_test.go executes the T035 execution-view and T036
// projection-refresh evidence against a real PostgreSQL and the real route
// registration: ownership-enforced identical 404, the four lifecycle
// references, the possibly-stale note that never presents a stale reference as
// verified, no credential leakage, and the operation_id-deduplicated audited
// forced refresh with zero business-state writes on the unavailable authority.
package app

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/execution"
	"github.com/xtianxx/txharbor/internal/withdrawal"
)

func executionViewMux(pool *pgxpool.Pool) http.Handler {
	mux := http.NewServeMux()
	h := &WithdrawalExecutionHandler{Pool: pool, ChainID: execHTTPChainID}
	mux.Handle("POST /withdrawals/{request_id}/execution", h)
	mux.Handle("GET /withdrawals/{request_id}/execution", h)
	return mux
}

func withdrawalExecRun(ctx context.Context, env map[string]string, args ...string) (int, string, string) {
	var stdout, stderr bytes.Buffer
	code := WithdrawalExec(ctx, args, Deps{Getenv: fakeEnv(env), Stdout: &stdout, Stderr: &stderr})
	return code, stdout.String(), stderr.String()
}

func TestExecutionViewIntegration(t *testing.T) {
	ctx, pool, _ := withdrawalHTTPSetup(t)
	executionHTTPSeed(t, ctx, pool, "req-view", "authz-view", "intent-view", 1)
	key, _, err := withdrawal.IssueKey(ctx, pool, 1, "view")
	if err != nil {
		t.Fatalf("IssueKey: %v", err)
	}
	srv := httptest.NewServer(executionViewMux(pool))
	defer srv.Close()

	if status, raw := withdrawalHTTPDo(t, http.MethodPost, srv.URL+"/withdrawals/req-view/execution", key, ""); status != http.StatusCreated {
		t.Fatalf("POST status = %d (%s), want 201", status, raw)
	}

	status, raw := withdrawalHTTPDo(t, http.MethodGet, srv.URL+"/withdrawals/req-view/execution", key, "")
	if status != http.StatusOK {
		t.Fatalf("GET status = %d (%s), want 200", status, raw)
	}
	var body executionViewResponse
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("decode view %s: %v", raw, err)
	}
	if body.RequestID != "req-view" || body.IntentID != "intent-view" {
		t.Fatalf("view identity = %+v, want req-view/intent-view", body)
	}
	if body.Execution == nil || body.Execution.State != execution.IntentAdmitted {
		t.Fatalf("execution block = %+v, want admitted", body.Execution)
	}
	if body.Lifecycle == nil || body.Lifecycle.Freshness != execution.FreshnessConfirmed {
		t.Fatalf("lifecycle block = %+v, want confirmed", body.Lifecycle)
	}
	if strings.Contains(string(raw), key) {
		t.Fatalf("view leaked the credential: %s", raw)
	}

	if _, err := pool.Exec(ctx, `UPDATE request_status_projection
		SET freshness = 'possibly_stale', stale_since = now() WHERE request_id = 'req-view'`); err != nil {
		t.Fatalf("mark stale: %v", err)
	}
	_, rawStale := withdrawalHTTPDo(t, http.MethodGet, srv.URL+"/withdrawals/req-view/execution", key, "")
	var staleBody executionViewResponse
	if err := json.Unmarshal(rawStale, &staleBody); err != nil {
		t.Fatalf("decode stale view %s: %v", rawStale, err)
	}
	if staleBody.Lifecycle == nil || staleBody.Lifecycle.Freshness != execution.FreshnessPossiblyStale {
		t.Fatalf("stale lifecycle = %+v, want possibly_stale", staleBody.Lifecycle)
	}
	if staleBody.Note == "" {
		t.Fatal("possibly-stale payload lacks the out-of-date reference note")
	}

	if status, _ := withdrawalHTTPDo(t, http.MethodGet, srv.URL+"/withdrawals/req-view/execution", "", ""); status != http.StatusUnauthorized {
		t.Fatalf("missing credential GET status = %d, want 401", status)
	}
	foreignKey, _, err := withdrawal.IssueKey(ctx, pool, 2, "view-foreign")
	if err != nil {
		t.Fatalf("IssueKey foreign: %v", err)
	}
	_, foreignBody := withdrawalHTTPDo(t, http.MethodGet, srv.URL+"/withdrawals/req-view/execution", foreignKey, "")
	_, missingBody := withdrawalHTTPDo(t, http.MethodGet, srv.URL+"/withdrawals/nope/execution", key, "")
	var fb, mb map[string]any
	_ = json.Unmarshal(foreignBody, &fb)
	_ = json.Unmarshal(missingBody, &mb)
	delete(fb, "trace_id")
	delete(mb, "trace_id")
	fNorm, _ := json.Marshal(fb)
	mNorm, _ := json.Marshal(mb)
	if string(fNorm) != string(mNorm) {
		t.Fatalf("foreign %s vs missing %s; 404 must render identically", foreignBody, missingBody)
	}
}

func TestProjectionRefreshCLIDedupIntegration(t *testing.T) {
	ctx, pool, dsn := withdrawalHTTPSetup(t)
	env := confirmAuthEnv(dsn)
	executionHTTPSeed(t, ctx, pool, "req-pr", "authz-pr", "intent-pr", 1)
	if out, err := execution.Admit(ctx, pool, "req-pr", 1); err != nil || out.Status != execution.AdmissionCreated {
		t.Fatalf("Admit = %+v (err %v), want created", out, err)
	}

	args := []string{"projection-refresh", "--request-id", "req-pr", "--operation-id", "op-pr-1", "--operator", "alice"}
	code, out, errOut := withdrawalExecRun(ctx, env, args...)
	if code != 0 || !strings.Contains(out, execution.ProjectionRefreshRefused) {
		t.Fatalf("projection-refresh exit=%d out=%q err=%q, want refused recorded", code, out, errOut)
	}
	var freshness string
	if err := pool.QueryRow(ctx, `SELECT freshness FROM request_status_projection WHERE request_id = 'req-pr'`).Scan(&freshness); err != nil {
		t.Fatalf("read projection: %v", err)
	}
	if freshness != execution.FreshnessPossiblyStale {
		t.Fatalf("projection freshness = %s, want possibly_stale after unavailable authority", freshness)
	}
	var action, outcome string
	if err := pool.QueryRow(ctx, `SELECT action, outcome FROM execution_ops_audit WHERE operation_id = 'op-pr-1'`).
		Scan(&action, &outcome); err != nil {
		t.Fatalf("read audit: %v", err)
	}
	if action != "projection_refresh" || outcome != execution.ProjectionRefreshRefused {
		t.Fatalf("audit = %s/%s, want projection_refresh/refused", action, outcome)
	}
	if got := intentStateForRequest(t, ctx, pool, "req-pr"); got != execution.IntentAdmitted {
		t.Fatalf("intent state = %s, want admitted (zero business-state writes)", got)
	}

	code, out2, errOut2 := withdrawalExecRun(ctx, env, args...)
	if code != 0 || !strings.Contains(out2, "recorded=true") {
		t.Fatalf("replay exit=%d out=%q err=%q, want the recorded outcome", code, out2, errOut2)
	}
	var auditRows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM execution_ops_audit WHERE operation_id = 'op-pr-1'`).Scan(&auditRows); err != nil || auditRows != 1 {
		t.Fatalf("audit rows = %d (err %v), want 1 after replay", auditRows, err)
	}

	executionHTTPSeed(t, ctx, pool, "req-pr2", "authz-pr2", "intent-pr2", 1)
	if out, err := execution.Admit(ctx, pool, "req-pr2", 1); err != nil || out.Status != execution.AdmissionCreated {
		t.Fatalf("Admit second = %+v (err %v), want created", out, err)
	}
	code, _, errOut3 := withdrawalExecRun(ctx, env,
		"projection-refresh", "--request-id", "req-pr2", "--operation-id", "op-pr-1", "--operator", "alice")
	if code != 1 || !strings.Contains(errOut3, "operation_conflict") {
		t.Fatalf("conflicting input exit=%d err=%q, want operation_conflict", code, errOut3)
	}
	if got := intentStateForRequest(t, ctx, pool, "req-pr2"); got != execution.IntentAdmitted {
		t.Fatalf("second intent state = %s, want admitted (zero writes on conflict)", got)
	}
}

func intentStateForRequest(t *testing.T, ctx context.Context, pool *pgxpool.Pool, requestID string) string {
	t.Helper()
	var state string
	if err := pool.QueryRow(ctx, `SELECT state FROM payment_intents WHERE request_id = $1`, requestID).Scan(&state); err != nil {
		t.Fatalf("read intent state for %s: %v", requestID, err)
	}
	return state
}
