//go:build integration

// execution_http_integration_test.go executes quickstart V1 on the HTTP side
// (T018) against the real `serve` route registration and a real PostgreSQL:
// 201/200/401/403/404/409/422/503 mapping, identical 404 rendering for
// missing/foreign requests, permission fail-closed with 401 before any DB
// read, zero intent rows on refusals, and body hygiene (no credentials).
package app

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/withdrawal"
)

const (
	execHTTPChainID   int64 = 31337
	execHTTPAsset           = "0x1111111111111111111111111111111111111111"
	execHTTPRecipient       = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	execHTTPSender          = "0xcccccccccccccccccccccccccccccccccccccccc"
)

// executionHTTPMux mirrors the serve.go registration for the 011 route: the
// method+pattern entry is more specific than the /withdrawals/ subtree.
func executionHTTPMux(pool *pgxpool.Pool) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("POST /withdrawals/{request_id}/execution",
		&WithdrawalExecutionHandler{Pool: pool, ChainID: execHTTPChainID})
	return mux
}

func executionHTTPSeed(t *testing.T, ctx context.Context, pool *pgxpool.Pool, requestID, authorizationID, intentID string, callerID int64) {
	t.Helper()
	if _, err := pool.Exec(ctx, `INSERT INTO caller (caller_id, label) VALUES ($1, 'ops')
		ON CONFLICT (caller_id) DO NOTHING`, callerID); err != nil {
		t.Fatalf("seed caller: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO withdrawal_authorizations
		(authorization_id, caller_id, chain_id, asset, recipient, amount, state)
		VALUES ($1, $2, $3, $4, $5, 100, 'active')`,
		authorizationID, callerID, execHTTPChainID, execHTTPAsset, execHTTPRecipient); err != nil {
		t.Fatalf("seed authorization: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO withdrawal_requests
		(request_id, caller_id, idempotency_key, authorization_id, chain_id, asset, recipient, amount)
		VALUES ($1, $2, $3, $4, $5, $6, $7, 100)`,
		requestID, callerID, "idem-"+requestID, authorizationID, execHTTPChainID, execHTTPAsset, execHTTPRecipient); err != nil {
		t.Fatalf("seed request: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO withdrawal_authorization_scopes
		(authorization_id, intent_id, request_id, sender, fee_max_total, fee_max_per_gas,
		 fee_max_priority, allows_fee_replacement, authorization_version, attested_by)
		VALUES ($1, $2, $3, $4, 0, 0, 0, FALSE, 1, 'test')`,
		authorizationID, intentID, requestID, execHTTPSender); err != nil {
		t.Fatalf("seed scope: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO nonce_wallet_registry (chain_id, sender, state, registry_seq)
		VALUES ($1, $2, 'active', 1) ON CONFLICT (chain_id, sender) DO UPDATE SET state = 'active'`,
		execHTTPChainID, execHTTPSender); err != nil {
		t.Fatalf("seed registry: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO execution_caller_permission (caller_id, can_execute, updated_by)
		VALUES ($1, TRUE, 'test') ON CONFLICT (caller_id) DO UPDATE SET can_execute = TRUE`, callerID); err != nil {
		t.Fatalf("seed permission: %v", err)
	}
}

func executionHTTPCountIntents(t *testing.T, ctx context.Context, pool *pgxpool.Pool, requestID string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM payment_intents WHERE request_id = $1`, requestID).Scan(&n); err != nil {
		t.Fatalf("count intents: %v", err)
	}
	return n
}

func TestExecutionHTTPAdmission201And200(t *testing.T) {
	ctx, pool, _ := withdrawalHTTPSetup(t)
	seedExecFull := func() string {
		executionHTTPSeed(t, ctx, pool, "req-http-1", "authz-http-1", "intent-http-1", 1)
		key, _, err := withdrawal.IssueKey(ctx, pool, 1, "http")
		if err != nil {
			t.Fatalf("IssueKey: %v", err)
		}
		return key
	}
	key := seedExecFull()
	srv := httptest.NewServer(executionHTTPMux(pool))
	defer srv.Close()

	status, raw := withdrawalHTTPDo(t, http.MethodPost, srv.URL+"/withdrawals/req-http-1/execution", key, "")
	if status != http.StatusCreated {
		t.Fatalf("first POST status = %d (%s), want 201", status, raw)
	}
	var body executionAdmissionResponse
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("decode body %s: %v", raw, err)
	}
	if body.IntentID != "intent-http-1" || body.State != "admitted" || body.RequestID != "req-http-1" {
		t.Fatalf("body = %+v, want the declared intent/admitted/request", body)
	}
	if strings.Contains(string(raw), key) {
		t.Fatalf("response body leaked the credential: %s", raw)
	}

	status, raw2 := withdrawalHTTPDo(t, http.MethodPost, srv.URL+"/withdrawals/req-http-1/execution", key, "")
	if status != http.StatusOK {
		t.Fatalf("replay POST status = %d (%s), want 200", status, raw2)
	}
	if got := executionHTTPCountIntents(t, ctx, pool, "req-http-1"); got != 1 {
		t.Fatalf("intent rows = %d, want 1", got)
	}
}

func TestExecutionHTTPAuthAndPermission(t *testing.T) {
	ctx, pool, _ := withdrawalHTTPSetup(t)
	executionHTTPSeed(t, ctx, pool, "req-http-auth", "authz-http-auth", "intent-http-auth", 1)
	key, _, err := withdrawal.IssueKey(ctx, pool, 1, "http")
	if err != nil {
		t.Fatalf("IssueKey: %v", err)
	}

	// Nil pool: a malformed credential is 401 with no DB access (no panic).
	nilSrv := httptest.NewServer(executionHTTPMux(nil))
	defer nilSrv.Close()
	if status, _ := withdrawalHTTPDo(t, http.MethodPost, nilSrv.URL+"/withdrawals/req-http-auth/execution", "", ""); status != http.StatusUnauthorized {
		t.Fatalf("missing credential status = %d, want 401", status)
	}
	if status, _ := withdrawalHTTPDo(t, http.MethodPost, nilSrv.URL+"/withdrawals/req-http-auth/execution", "not-a-txh-key", ""); status != http.StatusUnauthorized {
		t.Fatalf("malformed credential status = %d, want 401", status)
	}
	// A well-formed key with a nil pool is storage unavailable, never a refusal.
	wellFormed, _, _, err := withdrawal.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	if status, _ := withdrawalHTTPDo(t, http.MethodPost, nilSrv.URL+"/withdrawals/req-http-auth/execution", wellFormed, ""); status != http.StatusServiceUnavailable {
		t.Fatalf("nil-pool status = %d, want 503", status)
	}

	srv := httptest.NewServer(executionHTTPMux(pool))
	defer srv.Close()

	// Permission FALSE is 403 fail-closed with zero intent rows.
	if _, err := pool.Exec(ctx, `UPDATE execution_caller_permission SET can_execute = FALSE WHERE caller_id = 1`); err != nil {
		t.Fatalf("revoke permission: %v", err)
	}
	if status, raw := withdrawalHTTPDo(t, http.MethodPost, srv.URL+"/withdrawals/req-http-auth/execution", key, ""); status != http.StatusForbidden {
		t.Fatalf("permission-false status = %d (%s), want 403", status, raw)
	}
	if got := executionHTTPCountIntents(t, ctx, pool, "req-http-auth"); got != 0 {
		t.Fatalf("intent rows = %d, want 0 after 403", got)
	}

	// Absent permission row is also 403 (fail-closed).
	if _, err := pool.Exec(ctx, `DELETE FROM execution_caller_permission WHERE caller_id = 1`); err != nil {
		t.Fatalf("delete permission: %v", err)
	}
	if status, _ := withdrawalHTTPDo(t, http.MethodPost, srv.URL+"/withdrawals/req-http-auth/execution", key, ""); status != http.StatusForbidden {
		t.Fatalf("absent-permission status = %d, want 403", status)
	}
}

func TestExecutionHTTPStatusMapping(t *testing.T) {
	ctx, pool, _ := withdrawalHTTPSetup(t)
	executionHTTPSeed(t, ctx, pool, "req-http-map", "authz-http-map", "intent-http-map", 1)
	key1, _, err := withdrawal.IssueKey(ctx, pool, 1, "http")
	if err != nil {
		t.Fatalf("IssueKey 1: %v", err)
	}
	key2, _, err := withdrawal.IssueKey(ctx, pool, 2, "http")
	if err != nil {
		t.Fatalf("IssueKey 2: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO execution_caller_permission (caller_id, can_execute, updated_by)
		VALUES (2, TRUE, 'test') ON CONFLICT (caller_id) DO UPDATE SET can_execute = TRUE`); err != nil {
		t.Fatalf("seed permission 2: %v", err)
	}
	srv := httptest.NewServer(executionHTTPMux(pool))
	defer srv.Close()

	// 400 malformed body (no DB).
	if status, _ := withdrawalHTTPDo(t, http.MethodPost, srv.URL+"/withdrawals/req-http-map/execution", key1, "not-json"); status != http.StatusBadRequest {
		t.Fatalf("malformed body status = %d, want 400", status)
	}
	// 404 nonexistent request.
	if status, _ := withdrawalHTTPDo(t, http.MethodPost, srv.URL+"/withdrawals/nope/execution", key1, ""); status != http.StatusNotFound {
		t.Fatalf("missing request status = %d, want 404", status)
	}
	// 404 foreign request: identical body to the missing case.
	_, foreignBody := withdrawalHTTPDo(t, http.MethodPost, srv.URL+"/withdrawals/req-http-map/execution", key2, "")
	_, missingBody := withdrawalHTTPDo(t, http.MethodPost, srv.URL+"/withdrawals/nope/execution", key1, "")
	if normalizeTrace(string(foreignBody)) != normalizeTrace(string(missingBody)) {
		t.Fatalf("foreign body = %s, missing body = %s; must render identically", foreignBody, missingBody)
	}
	if got := executionHTTPCountIntents(t, ctx, pool, "req-http-map"); got != 0 {
		t.Fatalf("intent rows = %d, want 0 before creation", got)
	}

	// Remove the scope: admission refuses 422 with zero rows.
	if _, err := pool.Exec(ctx, `DELETE FROM withdrawal_authorization_scopes WHERE authorization_id = 'authz-http-map'`); err != nil {
		t.Fatalf("delete scope: %v", err)
	}
	status, raw := withdrawalHTTPDo(t, http.MethodPost, srv.URL+"/withdrawals/req-http-map/execution", key1, "")
	if status != http.StatusUnprocessableEntity {
		t.Fatalf("missing-scope status = %d (%s), want 422", status, raw)
	}
	if got := executionHTTPCountIntents(t, ctx, pool, "req-http-map"); got != 0 {
		t.Fatalf("intent rows = %d, want 0 after refusal", got)
	}

	// 409: a created intent whose scope is re-supplied with a different
	// identity basis conflicts with zero writes.
	executionHTTPSeed(t, ctx, pool, "req-http-409", "authz-http-409", "intent-http-409", 1)
	if status, raw := withdrawalHTTPDo(t, http.MethodPost, srv.URL+"/withdrawals/req-http-409/execution", key1, ""); status != http.StatusCreated {
		t.Fatalf("conflict seed status = %d (%s), want 201", status, raw)
	}
	if _, err := pool.Exec(ctx, `UPDATE withdrawal_authorization_scopes
		SET intent_id = 'intent-http-409b' WHERE authorization_id = 'authz-http-409'`); err != nil {
		t.Fatalf("resupply scope: %v", err)
	}
	if status, raw := withdrawalHTTPDo(t, http.MethodPost, srv.URL+"/withdrawals/req-http-409/execution", key1, ""); status != http.StatusConflict {
		t.Fatalf("different-basis status = %d (%s), want 409", status, raw)
	}
	if got := executionHTTPCountIntents(t, ctx, pool, "req-http-409"); got != 1 {
		t.Fatalf("intent rows = %d, want 1 (zero writes on conflict)", got)
	}
}

// normalizeTrace strips the per-response trace id so two error bodies can be
// compared for byte equality modulo the trace header.
func normalizeTrace(body string) string {
	var m map[string]any
	if err := json.Unmarshal([]byte(body), &m); err != nil {
		return body
	}
	delete(m, "trace_id")
	out, _ := json.Marshal(m)
	return string(out)
}
