//go:build integration

// withdrawalhttp_integration_test.go owns the T014 end-to-end HTTP tests against
// a real PostgreSQL 18 container (testcontainers) and a real httptest.Server:
// create + self-query, replay/conflict, bad params, the 401 zero-audit path,
// FR-15 404 byte-equality, recovery-state annotation, and the response-first
// proof that a pre-tx reject audit never delays the HTTP response.
package app

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"

	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/withdrawal"
)

const (
	withdrawalHTTPChainID    int64 = 31337
	withdrawalHTTPAsset            = "0x1111111111111111111111111111111111111111"
	withdrawalHTTPOtherAsset       = "0x2222222222222222222222222222222222222222"
	withdrawalHTTPWatch            = "0x3333333333333333333333333333333333333333"
	withdrawalHTTPRecipient        = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	withdrawalHTTPAmount           = "100"
)

// withdrawalHTTPSetup boots a migrated scratch PostgreSQL, opens a pool, and
// returns the DSN plus the context for direct seeding/polling.
func withdrawalHTTPSetup(t *testing.T) (context.Context, *pgxpool.Pool, string) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()
	ctr := startPostgresContainer(t)
	dsn := postgresDSN(t, ctr)
	if err := db.MigrateUp(ctx, db.MigrateOptions{DSN: dsn, LockTimeout: 5 * time.Second, ConnectTimeout: 5 * time.Second}, io.Discard); err != nil {
		t.Fatalf("migrate up: %v", err)
	}
	pool, err := db.OpenPool(ctx, dsn, 5*time.Second)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return ctx, pool, dsn
}

// withdrawalHTTPKey issues one credential for callerID (creating the caller).
func withdrawalHTTPKey(t *testing.T, ctx context.Context, pool *pgxpool.Pool, callerID int64) string {
	t.Helper()
	plaintext, _, err := withdrawal.IssueKey(ctx, pool, callerID, "http-test")
	if err != nil {
		t.Fatalf("IssueKey(caller %d): %v", callerID, err)
	}
	return plaintext
}

// withdrawalHTTPSupply seeds one active grant bound to callerID.
func withdrawalHTTPSupply(t *testing.T, ctx context.Context, pool *pgxpool.Pool, callerID int64, authorizationID string) {
	t.Helper()
	out, err := withdrawal.SupplyGrant(ctx, pool, withdrawal.OpInput{
		OperationID:     "seed-" + authorizationID,
		Action:          "supply",
		AuthorizationID: authorizationID,
		CallerID:        callerID,
		ChainID:         withdrawalHTTPChainID,
		Asset:           withdrawalHTTPAsset,
		Recipient:       withdrawalHTTPRecipient,
		Amount:          withdrawalHTTPAmount,
	}, "http-test", "seed")
	if err != nil {
		t.Fatalf("SupplyGrant(%s): %v", authorizationID, err)
	}
	if out.Action != "supplied" {
		t.Fatalf("SupplyGrant(%s) action = %s, want supplied", authorizationID, out.Action)
	}
}

// withdrawalHTTPHandler builds the handler under test for one pool. The FR-05
// allowlist is resolved per request from deposit_config_history, so no static
// allowlist is injected.
func withdrawalHTTPHandler(pool *pgxpool.Pool) *WithdrawalHandler {
	return &WithdrawalHandler{
		Pool:    pool,
		ChainID: withdrawalHTTPChainID,
	}
}

// withdrawalHTTPPolicy appends one 004 policy version whose asset set is the
// canonical `<address>:<effective>` snapshot lines. The FR-05 reader consumes
// only the newest version's assets.
func withdrawalHTTPPolicy(t *testing.T, ctx context.Context, pool *pgxpool.Pool, version int64, assets string) {
	t.Helper()
	_, err := pool.Exec(ctx, `
INSERT INTO deposit_config_history
    (chain_id, version_seq, config_hash, start_block, assets, watches, replay_from, request_id)
VALUES ($1, $2, $3, 0, $4, $5, 0, $6)`,
		withdrawalHTTPChainID, version, strings.Repeat("a", 64), assets,
		withdrawalHTTPWatch+":0", fmt.Sprintf("req-http-%d", version))
	if err != nil {
		t.Fatalf("insert deposit_config_history v%d: %v", version, err)
	}
}

// withdrawalHTTPSeedPolicy seeds the v1 policy carrying withdrawalHTTPAsset.
func withdrawalHTTPSeedPolicy(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	withdrawalHTTPPolicy(t, ctx, pool, 1, withdrawalHTTPAsset+":0")
}

// withdrawalHTTPAuditActionCount counts the Table 5 audit rows for one action.
func withdrawalHTTPAuditActionCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, action string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM withdrawal_request_audit WHERE action = $1`, action).Scan(&n); err != nil {
		t.Fatalf("count withdrawal_request_audit action %q: %v", action, err)
	}
	return n
}

// withdrawalHTTPGrantBindCount counts withdrawal_requests bound to one grant
// (the authorization_id UNIQUE carrier).
func withdrawalHTTPGrantBindCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, authorizationID string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM withdrawal_requests WHERE authorization_id = $1`, authorizationID).Scan(&n); err != nil {
		t.Fatalf("count grant binds for %q: %v", authorizationID, err)
	}
	return n
}

// withdrawalHTTPGrantAuditCount counts the operator supply/revoke audit rows
// (withdrawal_grant_audit); the create path MUST never touch this table.
func withdrawalHTTPGrantAuditCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM withdrawal_grant_audit`).Scan(&n); err != nil {
		t.Fatalf("count withdrawal_grant_audit: %v", err)
	}
	return n
}

// withdrawalHTTPDo fires one request and returns status + full body bytes.
func withdrawalHTTPDo(t *testing.T, method, url, token, body string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, raw
}

// withdrawalHTTPCreateBody renders the canonical create JSON with the given
// idempotency key, amount, and authorization id.
func withdrawalHTTPCreateBody(t *testing.T, key, amount, authorizationID string) string {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"idempotency_key":  key,
		"chain_id":         withdrawalHTTPChainID,
		"asset":            withdrawalHTTPAsset,
		"recipient":        withdrawalHTTPRecipient,
		"amount":           amount,
		"authorization_id": authorizationID,
	})
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	return string(raw)
}

func withdrawalHTTPRequestCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM withdrawal_requests`).Scan(&n); err != nil {
		t.Fatalf("count withdrawal_requests: %v", err)
	}
	return n
}

func withdrawalHTTPAuditCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM withdrawal_request_audit`).Scan(&n); err != nil {
		t.Fatalf("count withdrawal_request_audit: %v", err)
	}
	return n
}

// TestWithdrawalHTTPCreateThenSelfQuery covers SC-01/SC-08: a create returns
// 201 with its request id, and a self-query returns the same facts (amount
// string round-trip) with the standing not_started execution fact.
func TestWithdrawalHTTPCreateThenSelfQuery(t *testing.T) {
	ctx, pool, _ := withdrawalHTTPSetup(t)
	key := withdrawalHTTPKey(t, ctx, pool, 8101)
	withdrawalHTTPSupply(t, ctx, pool, 8101, "auth-http-1")
	withdrawalHTTPSeedPolicy(t, ctx, pool)

	srv := httptest.NewServer(withdrawalHTTPHandler(pool))
	defer srv.Close()

	status, raw := withdrawalHTTPDo(t, http.MethodPost, srv.URL+"/withdrawals", key,
		withdrawalHTTPCreateBody(t, "idem-http-1", withdrawalHTTPAmount, "auth-http-1"))
	if status != http.StatusCreated {
		t.Fatalf("create status = %d (%s), want 201", status, raw)
	}
	var created withdrawalPostResponse
	if err := json.Unmarshal(raw, &created); err != nil {
		t.Fatalf("decode create body %s: %v", raw, err)
	}
	if created.RequestID == "" || created.Amount != withdrawalHTTPAmount {
		t.Fatalf("create body = %+v, want a request_id and amount %s", created, withdrawalHTTPAmount)
	}

	status, raw = withdrawalHTTPDo(t, http.MethodGet, srv.URL+"/withdrawals/"+created.RequestID, key, "")
	if status != http.StatusOK {
		t.Fatalf("self-query status = %d (%s), want 200", status, raw)
	}
	var view withdrawalGetResponse
	if err := json.Unmarshal(raw, &view); err != nil {
		t.Fatalf("decode view body %s: %v", raw, err)
	}
	if view.RequestID != created.RequestID || view.Amount != created.Amount {
		t.Fatalf("view = %+v, want request_id %q amount %q", view, created.RequestID, created.Amount)
	}
	if view.CallerID != 8101 || view.Status != "accepted" {
		t.Fatalf("view caller/status = (%d, %q), want (8101, accepted)", view.CallerID, view.Status)
	}
	if view.Recovery.Execution != "not_started" || view.Recovery.State != "none" {
		t.Fatalf("recovery = %+v, want {none not_started}", view.Recovery)
	}
	if view.CreatedAt == "" {
		t.Fatal("view created_at is empty")
	}
}

// TestWithdrawalHTTPReplayAndConflict covers the same-key fast path: an equal
// replay returns 200 with the original id, and a differing amount returns 409,
// leaving exactly one request row.
func TestWithdrawalHTTPReplayAndConflict(t *testing.T) {
	ctx, pool, _ := withdrawalHTTPSetup(t)
	key := withdrawalHTTPKey(t, ctx, pool, 8102)
	withdrawalHTTPSupply(t, ctx, pool, 8102, "auth-http-2")
	withdrawalHTTPSeedPolicy(t, ctx, pool)

	srv := httptest.NewServer(withdrawalHTTPHandler(pool))
	defer srv.Close()

	createBody := withdrawalHTTPCreateBody(t, "idem-http-2", withdrawalHTTPAmount, "auth-http-2")
	status, raw := withdrawalHTTPDo(t, http.MethodPost, srv.URL+"/withdrawals", key, createBody)
	if status != http.StatusCreated {
		t.Fatalf("create status = %d (%s), want 201", status, raw)
	}
	var first withdrawalPostResponse
	if err := json.Unmarshal(raw, &first); err != nil {
		t.Fatalf("decode create body: %v", err)
	}

	status, raw = withdrawalHTTPDo(t, http.MethodPost, srv.URL+"/withdrawals", key, createBody)
	if status != http.StatusOK {
		t.Fatalf("replay status = %d (%s), want 200", status, raw)
	}
	var replay withdrawalPostResponse
	if err := json.Unmarshal(raw, &replay); err != nil {
		t.Fatalf("decode replay body: %v", err)
	}
	if replay.RequestID != first.RequestID {
		t.Fatalf("replay request_id = %q, want original %q", replay.RequestID, first.RequestID)
	}

	status, raw = withdrawalHTTPDo(t, http.MethodPost, srv.URL+"/withdrawals", key,
		withdrawalHTTPCreateBody(t, "idem-http-2", "101", "auth-http-2"))
	if status != http.StatusConflict {
		t.Fatalf("conflict status = %d (%s), want 409", status, raw)
	}
	var conflict withdrawalErrorResponse
	if err := json.Unmarshal(raw, &conflict); err != nil {
		t.Fatalf("decode conflict body: %v", err)
	}
	if conflict.Code != string(withdrawal.CodeIdempotencyConflict) || conflict.RequestID != first.RequestID {
		t.Fatalf("conflict = %+v, want idempotency_conflict with request_id %q", conflict, first.RequestID)
	}
	if n := withdrawalHTTPRequestCount(t, ctx, pool); n != 1 {
		t.Fatalf("request rows = %d, want 1 (original untouched)", n)
	}
}

// TestWithdrawalHTTPBadParams covers the 422 row over the real handler.
func TestWithdrawalHTTPBadParams(t *testing.T) {
	ctx, pool, _ := withdrawalHTTPSetup(t)
	key := withdrawalHTTPKey(t, ctx, pool, 8103)
	withdrawalHTTPSupply(t, ctx, pool, 8103, "auth-http-3")
	withdrawalHTTPSeedPolicy(t, ctx, pool)

	srv := httptest.NewServer(withdrawalHTTPHandler(pool))
	defer srv.Close()

	status, raw := withdrawalHTTPDo(t, http.MethodPost, srv.URL+"/withdrawals", key,
		withdrawalHTTPCreateBody(t, "idem-http-bad", "1.5", "auth-http-3"))
	if status != http.StatusUnprocessableEntity {
		t.Fatalf("bad-param status = %d (%s), want 422", status, raw)
	}
	var body withdrawalErrorResponse
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.Code != string(withdrawal.CodeValidationFailed) {
		t.Fatalf("code = %q, want %q", body.Code, withdrawal.CodeValidationFailed)
	}
	if n := withdrawalHTTPRequestCount(t, ctx, pool); n != 0 {
		t.Fatalf("request rows = %d, want 0", n)
	}
}

// TestWithdrawalHTTPNoKeyWritesZeroAuditRows covers the 401 path end to end:
// no credential writes no request and no Table 5 audit row (the core reports
// Audit=nil on the 401/503 auth-failure paths).
func TestWithdrawalHTTPNoKeyWritesZeroAuditRows(t *testing.T) {
	ctx, pool, _ := withdrawalHTTPSetup(t)

	srv := httptest.NewServer(withdrawalHTTPHandler(pool))
	defer srv.Close()

	status, raw := withdrawalHTTPDo(t, http.MethodPost, srv.URL+"/withdrawals", "",
		withdrawalHTTPCreateBody(t, "idem-http-401", withdrawalHTTPAmount, "auth-http-401"))
	if status != http.StatusUnauthorized {
		t.Fatalf("no-key status = %d (%s), want 401", status, raw)
	}
	if n := withdrawalHTTPRequestCount(t, ctx, pool); n != 0 {
		t.Fatalf("request rows after 401 = %d, want 0", n)
	}
	if n := withdrawalHTTPAuditCount(t, ctx, pool); n != 0 {
		t.Fatalf("audit rows after 401 = %d, want 0", n)
	}
}

// TestWithdrawalHTTPCrossCallerNotFoundByteEqual covers FR-15: another
// caller's id and a nonexistent id render the identical 404 body, so no
// existence signal leaks.
func TestWithdrawalHTTPCrossCallerNotFoundByteEqual(t *testing.T) {
	ctx, pool, _ := withdrawalHTTPSetup(t)
	keyA := withdrawalHTTPKey(t, ctx, pool, 8104)
	keyB := withdrawalHTTPKey(t, ctx, pool, 8105)
	withdrawalHTTPSupply(t, ctx, pool, 8104, "auth-http-4a")
	withdrawalHTTPSeedPolicy(t, ctx, pool)

	srv := httptest.NewServer(withdrawalHTTPHandler(pool))
	defer srv.Close()

	status, raw := withdrawalHTTPDo(t, http.MethodPost, srv.URL+"/withdrawals", keyA,
		withdrawalHTTPCreateBody(t, "idem-http-4a", withdrawalHTTPAmount, "auth-http-4a"))
	if status != http.StatusCreated {
		t.Fatalf("create status = %d (%s), want 201", status, raw)
	}
	var created withdrawalPostResponse
	if err := json.Unmarshal(raw, &created); err != nil {
		t.Fatalf("decode create body: %v", err)
	}

	statusForeign, foreign := withdrawalHTTPDo(t, http.MethodGet, srv.URL+"/withdrawals/"+created.RequestID, keyB, "")
	statusMissing, missing := withdrawalHTTPDo(t, http.MethodGet, srv.URL+"/withdrawals/wr-00000000000000000000000000000000", keyB, "")
	if statusForeign != http.StatusNotFound || statusMissing != http.StatusNotFound {
		t.Fatalf("404 statuses = (%d, %d), want (404, 404)", statusForeign, statusMissing)
	}
	if !bytes.Equal(foreign, missing) {
		t.Fatalf("404 bodies differ:\nforeign=%s\nmissing=%s", foreign, missing)
	}
	if !bytes.Contains(foreign, []byte(string(withdrawal.CodeNotFound))) {
		t.Fatalf("404 body %s does not carry %q", foreign, withdrawal.CodeNotFound)
	}
}

// TestWithdrawalHTTPRecoveryStateServed covers the contracts §3 recovery
// annotation: with an active 006 recovery row the request facts stay servable
// and the state is the phase-mapped recovering, execution still not_started.
func TestWithdrawalHTTPRecoveryStateServed(t *testing.T) {
	ctx, pool, _ := withdrawalHTTPSetup(t)
	key := withdrawalHTTPKey(t, ctx, pool, 8106)
	withdrawalHTTPSupply(t, ctx, pool, 8106, "auth-http-6")
	withdrawalHTTPSeedPolicy(t, ctx, pool)

	srv := httptest.NewServer(withdrawalHTTPHandler(pool))
	defer srv.Close()

	status, raw := withdrawalHTTPDo(t, http.MethodPost, srv.URL+"/withdrawals", key,
		withdrawalHTTPCreateBody(t, "idem-http-6", withdrawalHTTPAmount, "auth-http-6"))
	if status != http.StatusCreated {
		t.Fatalf("create status = %d (%s), want 201", status, raw)
	}
	var created withdrawalPostResponse
	if err := json.Unmarshal(raw, &created); err != nil {
		t.Fatalf("decode create body: %v", err)
	}

	if _, err := pool.Exec(ctx, `INSERT INTO reorg_policy_history
		(chain_id, policy_seq, max_depth, operator) VALUES ($1, 1, 64, 'bootstrap')`, withdrawalHTTPChainID); err != nil {
		t.Fatalf("seed reorg policy: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO reorg_recovery
		(chain_id, recovery_id, phase, policy_seq, max_depth, bound_old_number, bound_old_hash, recovery_seq)
		VALUES ($1, 'rec-http', 'detected', 1, 64, 100, $2, 1)`,
		withdrawalHTTPChainID, "0x"+strings.Repeat("aa", 32)); err != nil {
		t.Fatalf("seed active recovery: %v", err)
	}

	status, raw = withdrawalHTTPDo(t, http.MethodGet, srv.URL+"/withdrawals/"+created.RequestID, key, "")
	if status != http.StatusOK {
		t.Fatalf("query during recovery status = %d (%s), want 200", status, raw)
	}
	var view withdrawalGetResponse
	if err := json.Unmarshal(raw, &view); err != nil {
		t.Fatalf("decode view body: %v", err)
	}
	if view.Recovery.State != "recovering" || view.Recovery.Execution != "not_started" {
		t.Fatalf("recovery = %+v, want {recovering not_started}", view.Recovery)
	}
	if view.RequestID != created.RequestID {
		t.Fatalf("view request_id = %q, want %q", view.RequestID, created.RequestID)
	}
}

// TestWithdrawalHTTPResponseFirst is the critical proof of the response-first
// rule: a 422 POST is fired while withdrawal_request_audit is locked ACCESS
// EXCLUSIVE in another transaction, so the handler's post-response audit write
// blocks for the full 2s detached bound. The client MUST still read the
// complete, correct response well under that bound. The handler pool has
// MaxConns=1, so no spare connection can paper over the block.
func TestWithdrawalHTTPResponseFirst(t *testing.T) {
	ctx, pool, dsn := withdrawalHTTPSetup(t)
	key := withdrawalHTTPKey(t, ctx, pool, 8107)
	withdrawalHTTPSeedPolicy(t, ctx, pool)

	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse pool config: %v", err)
	}
	poolCfg.MaxConns = 1
	handlerPool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		t.Fatalf("open single-connection pool: %v", err)
	}
	defer handlerPool.Close()

	srv := httptest.NewServer(withdrawalHTTPHandler(handlerPool))
	defer srv.Close()

	lockConn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect lock holder: %v", err)
	}
	defer func() { _ = lockConn.Close(ctx) }()
	lockTx, err := lockConn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin lock tx: %v", err)
	}
	if _, err := lockTx.Exec(ctx, `LOCK TABLE withdrawal_request_audit IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatalf("lock audit table: %v", err)
	}

	start := time.Now()
	status, raw := withdrawalHTTPDo(t, http.MethodPost, srv.URL+"/withdrawals", key,
		withdrawalHTTPCreateBody(t, "idem-http-7", "1.5", "auth-http-7"))
	elapsed := time.Since(start)

	if status != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d (%s), want 422", status, raw)
	}
	var body withdrawalErrorResponse
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("decode body %s: %v", raw, err)
	}
	if body.Code != string(withdrawal.CodeValidationFailed) {
		t.Fatalf("code = %q, want %q", body.Code, withdrawal.CodeValidationFailed)
	}
	if elapsed >= 1500*time.Millisecond {
		t.Fatalf("response took %s; audit blocked the response (want well under the 2s audit bound)", elapsed)
	}

	// Release the lock: the pending audit write must now land, proving the
	// intent is persisted (never dropped) once the block clears.
	if err := lockTx.Rollback(ctx); err != nil {
		t.Fatalf("release lock: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		if n := withdrawalHTTPAuditCount(t, ctx, pool); n >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("reject audit row never appeared after the lock was released")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// withdrawalHTTPWaitAuditAction polls until action has at least want rows: the
// handler flushes the response before its detached audit write, so the row can
// land just after the client returns.
func withdrawalHTTPWaitAuditAction(t *testing.T, ctx context.Context, pool *pgxpool.Pool, action string, want int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if n := withdrawalHTTPAuditActionCount(t, ctx, pool, action); n >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("audit action %q never reached %d rows", action, want)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestWithdrawalHTTPColdStartNoPolicyIs503Then201 covers T020's live policy
// source at cold start: with no deposit_config_history row the create is a
// retryable 503 (never an empty/full-chain fallback), exactly one `unavailable`
// audit row is written for the authenticated caller, no request/grant row is
// bound, and the SAME idempotency key converges to 201 once the policy lands —
// no restart required.
func TestWithdrawalHTTPColdStartNoPolicyIs503Then201(t *testing.T) {
	ctx, pool, _ := withdrawalHTTPSetup(t)
	key := withdrawalHTTPKey(t, ctx, pool, 8301)
	withdrawalHTTPSupply(t, ctx, pool, 8301, "auth-cold")

	srv := httptest.NewServer(withdrawalHTTPHandler(pool))
	defer srv.Close()

	body := withdrawalHTTPCreateBody(t, "idem-cold-1", withdrawalHTTPAmount, "auth-cold")
	status, raw := withdrawalHTTPDo(t, http.MethodPost, srv.URL+"/withdrawals", key, body)
	if status != http.StatusServiceUnavailable {
		t.Fatalf("cold-start status = %d (%s), want 503", status, raw)
	}
	var errBody withdrawalErrorResponse
	if err := json.Unmarshal(raw, &errBody); err != nil {
		t.Fatalf("decode 503 body %s: %v", raw, err)
	}
	if errBody.Code != string(withdrawal.CodeTemporarilyUnavailable) ||
		!strings.Contains(errBody.Message, "retry with the same idempotency key") {
		t.Fatalf("503 body = %+v, want temporarily_unavailable with the same-key retry instruction", errBody)
	}
	if n := withdrawalHTTPRequestCount(t, ctx, pool); n != 0 {
		t.Fatalf("request rows at cold start = %d, want 0", n)
	}
	if n := withdrawalHTTPGrantBindCount(t, ctx, pool, "auth-cold"); n != 0 {
		t.Fatalf("grant binds at cold start = %d, want 0", n)
	}
	withdrawalHTTPWaitAuditAction(t, ctx, pool, "unavailable", 1)

	// Same key and parameters after the policy lands: no restart, 201.
	withdrawalHTTPSeedPolicy(t, ctx, pool)
	status, raw = withdrawalHTTPDo(t, http.MethodPost, srv.URL+"/withdrawals", key, body)
	if status != http.StatusCreated {
		t.Fatalf("retry-after-policy status = %d (%s), want 201", status, raw)
	}
	var created withdrawalPostResponse
	if err := json.Unmarshal(raw, &created); err != nil {
		t.Fatalf("decode 201 body %s: %v", raw, err)
	}
	if created.RequestID == "" {
		t.Fatalf("retry-after-policy body = %+v, want a request_id", created)
	}
	if n := withdrawalHTTPRequestCount(t, ctx, pool); n != 1 {
		t.Fatalf("request rows after policy = %d, want 1", n)
	}
}

// TestWithdrawalHTTPLivePolicyAllowVsDeny covers T020: with a policy row the
// allowed asset persists 201, while a shape-valid non-allowed asset is rejected
// per contract (422) with zero request/grant bind rows and no operator
// grant-audit row.
func TestWithdrawalHTTPLivePolicyAllowVsDeny(t *testing.T) {
	ctx, pool, _ := withdrawalHTTPSetup(t)
	key := withdrawalHTTPKey(t, ctx, pool, 8302)
	withdrawalHTTPSupply(t, ctx, pool, 8302, "auth-deny")
	withdrawalHTTPSeedPolicy(t, ctx, pool)
	grantAuditBefore := withdrawalHTTPGrantAuditCount(t, ctx, pool)

	srv := httptest.NewServer(withdrawalHTTPHandler(pool))
	defer srv.Close()

	status, raw := withdrawalHTTPDo(t, http.MethodPost, srv.URL+"/withdrawals", key,
		withdrawalHTTPCreateBody(t, "idem-allow", withdrawalHTTPAmount, "auth-deny"))
	if status != http.StatusCreated {
		t.Fatalf("allowed status = %d (%s), want 201", status, raw)
	}

	denyBody := strings.Replace(
		withdrawalHTTPCreateBody(t, "idem-deny", withdrawalHTTPAmount, "auth-deny"),
		withdrawalHTTPAsset, withdrawalHTTPOtherAsset, 1)
	status, raw = withdrawalHTTPDo(t, http.MethodPost, srv.URL+"/withdrawals", key, denyBody)
	if status != http.StatusUnprocessableEntity {
		t.Fatalf("non-allowed status = %d (%s), want 422", status, raw)
	}
	var errBody withdrawalErrorResponse
	if err := json.Unmarshal(raw, &errBody); err != nil {
		t.Fatalf("decode 422 body %s: %v", raw, err)
	}
	if errBody.Code != string(withdrawal.CodeValidationFailed) {
		t.Fatalf("non-allowed code = %q, want %q", errBody.Code, withdrawal.CodeValidationFailed)
	}
	if n := withdrawalHTTPRequestCount(t, ctx, pool); n != 1 {
		t.Fatalf("request rows = %d, want 1 (only the allowed create)", n)
	}
	if n := withdrawalHTTPGrantBindCount(t, ctx, pool, "auth-deny"); n != 1 {
		t.Fatalf("grant binds = %d, want 1", n)
	}
	if n := withdrawalHTTPGrantAuditCount(t, ctx, pool); n != grantAuditBefore {
		t.Fatalf("withdrawal_grant_audit rows = %d, want baseline %d (create path never touches it)", n, grantAuditBefore)
	}
}

// TestWithdrawalHTTPPolicyUpdateWithoutRestart covers T020: a new policy version
// is honored by the already-running server on the next request.
func TestWithdrawalHTTPPolicyUpdateWithoutRestart(t *testing.T) {
	ctx, pool, _ := withdrawalHTTPSetup(t)
	key := withdrawalHTTPKey(t, ctx, pool, 8303)
	withdrawalHTTPSupply(t, ctx, pool, 8303, "auth-update")
	withdrawalHTTPPolicy(t, ctx, pool, 1, withdrawalHTTPAsset+":0")

	srv := httptest.NewServer(withdrawalHTTPHandler(pool))
	defer srv.Close()

	status, raw := withdrawalHTTPDo(t, http.MethodPost, srv.URL+"/withdrawals", key,
		withdrawalHTTPCreateBody(t, "idem-update-1", withdrawalHTTPAmount, "auth-update"))
	if status != http.StatusCreated {
		t.Fatalf("pre-update status = %d (%s), want 201", status, raw)
	}

	// Advance the live policy to a version that drops the asset; no restart.
	withdrawalHTTPPolicy(t, ctx, pool, 2, withdrawalHTTPOtherAsset+":0")

	status, raw = withdrawalHTTPDo(t, http.MethodPost, srv.URL+"/withdrawals", key,
		withdrawalHTTPCreateBody(t, "idem-update-2", "101", "auth-update"))
	if status != http.StatusUnprocessableEntity {
		t.Fatalf("post-update status = %d (%s), want 422 from the new policy", status, raw)
	}
	if n := withdrawalHTTPRequestCount(t, ctx, pool); n != 1 {
		t.Fatalf("request rows = %d, want 1 (update rejected before persist)", n)
	}
}

// TestWithdrawalHTTPPolicySourceUnreadableIs503 covers T020's fail-closed read:
// when the policy source cannot be read the create is a retryable 503, never an
// allow, and no request row is written.
func TestWithdrawalHTTPPolicySourceUnreadableIs503(t *testing.T) {
	ctx, pool, _ := withdrawalHTTPSetup(t)
	key := withdrawalHTTPKey(t, ctx, pool, 8304)
	withdrawalHTTPSupply(t, ctx, pool, 8304, "auth-unreadable")
	withdrawalHTTPSeedPolicy(t, ctx, pool)

	// Make the live source unreadable for this throwaway database: the reader
	// query then fails rather than returning a policy.
	if _, err := pool.Exec(ctx, `ALTER TABLE deposit_config_history RENAME TO deposit_config_history_hidden`); err != nil {
		t.Fatalf("make policy source unreadable: %v", err)
	}

	srv := httptest.NewServer(withdrawalHTTPHandler(pool))
	defer srv.Close()

	status, raw := withdrawalHTTPDo(t, http.MethodPost, srv.URL+"/withdrawals", key,
		withdrawalHTTPCreateBody(t, "idem-unreadable", withdrawalHTTPAmount, "auth-unreadable"))
	if status != http.StatusServiceUnavailable {
		t.Fatalf("unreadable-source status = %d (%s), want 503 (never allow)", status, raw)
	}
	if n := withdrawalHTTPRequestCount(t, ctx, pool); n != 0 {
		t.Fatalf("request rows = %d, want 0", n)
	}
	withdrawalHTTPWaitAuditAction(t, ctx, pool, "unavailable", 1)
}

// TestWithdrawalHTTPReplayPreservedAcrossPolicyUpdate covers T020: a same-key
// replay is not re-evaluated against a policy change and still returns 200 with
// the original request_id, row count unchanged. The core's step order (whitelist
// validation at step 3 before the step-4 replay fast path) is untouched, so the
// test then pins the frozen-order boundary: a policy that drops the persisted
// request's own asset makes the next same-key POST a 422, not a replay.
func TestWithdrawalHTTPReplayPreservedAcrossPolicyUpdate(t *testing.T) {
	ctx, pool, _ := withdrawalHTTPSetup(t)
	key := withdrawalHTTPKey(t, ctx, pool, 8305)
	withdrawalHTTPSupply(t, ctx, pool, 8305, "auth-replay")
	// v1 carries both assets so a later version can drop one without touching
	// the persisted asset.
	withdrawalHTTPPolicy(t, ctx, pool, 1, withdrawalHTTPAsset+":0\n"+withdrawalHTTPOtherAsset+":0")

	srv := httptest.NewServer(withdrawalHTTPHandler(pool))
	defer srv.Close()

	body := withdrawalHTTPCreateBody(t, "idem-replay", withdrawalHTTPAmount, "auth-replay")
	status, raw := withdrawalHTTPDo(t, http.MethodPost, srv.URL+"/withdrawals", key, body)
	if status != http.StatusCreated {
		t.Fatalf("create status = %d (%s), want 201", status, raw)
	}
	var first withdrawalPostResponse
	if err := json.Unmarshal(raw, &first); err != nil {
		t.Fatalf("decode create body: %v", err)
	}

	// Policy advances and keeps withdrawalHTTPAsset: the replay is a 200 with
	// the original id, and no row changes.
	withdrawalHTTPPolicy(t, ctx, pool, 2, withdrawalHTTPAsset+":0")
	status, raw = withdrawalHTTPDo(t, http.MethodPost, srv.URL+"/withdrawals", key, body)
	if status != http.StatusOK {
		t.Fatalf("replay status = %d (%s), want 200", status, raw)
	}
	var replay withdrawalPostResponse
	if err := json.Unmarshal(raw, &replay); err != nil {
		t.Fatalf("decode replay body: %v", err)
	}
	if replay.RequestID != first.RequestID {
		t.Fatalf("replay request_id = %q, want original %q", replay.RequestID, first.RequestID)
	}
	if n := withdrawalHTTPRequestCount(t, ctx, pool); n != 1 {
		t.Fatalf("request rows = %d, want 1 (replay untouched)", n)
	}

	// Frozen order boundary: a policy dropping the persisted asset is checked at
	// step 3 before the step-4 replay fast path, so the same-key POST becomes a
	// 422 with the original untouched. A 200 here would require reordering the
	// core step 3/4, which T020 explicitly does not do.
	withdrawalHTTPPolicy(t, ctx, pool, 3, withdrawalHTTPOtherAsset+":0")
	status, raw = withdrawalHTTPDo(t, http.MethodPost, srv.URL+"/withdrawals", key, body)
	if status != http.StatusUnprocessableEntity {
		t.Fatalf("dropped-asset status = %d (%s), want 422 (frozen step order)", status, raw)
	}
	if n := withdrawalHTTPRequestCount(t, ctx, pool); n != 1 {
		t.Fatalf("request rows after dropped-asset = %d, want 1", n)
	}
}

// TestWithdrawalHTTPRejectedCreateWritesNoBindRows covers T020's zero-bind rule:
// a rejected create writes no withdrawal_requests row, binds no grant
// (withdrawal_requests.authorization_id), and writes no withdrawal_grant_audit
// row; only the response-first `rejected` audit intent lands.
func TestWithdrawalHTTPRejectedCreateWritesNoBindRows(t *testing.T) {
	ctx, pool, _ := withdrawalHTTPSetup(t)
	key := withdrawalHTTPKey(t, ctx, pool, 8306)
	withdrawalHTTPSupply(t, ctx, pool, 8306, "auth-reject-bind")
	withdrawalHTTPSeedPolicy(t, ctx, pool)
	grantAuditBefore := withdrawalHTTPGrantAuditCount(t, ctx, pool)

	srv := httptest.NewServer(withdrawalHTTPHandler(pool))
	defer srv.Close()

	denyBody := strings.Replace(
		withdrawalHTTPCreateBody(t, "idem-reject-bind", withdrawalHTTPAmount, "auth-reject-bind"),
		withdrawalHTTPAsset, withdrawalHTTPOtherAsset, 1)
	status, raw := withdrawalHTTPDo(t, http.MethodPost, srv.URL+"/withdrawals", key, denyBody)
	if status != http.StatusUnprocessableEntity {
		t.Fatalf("rejected status = %d (%s), want 422", status, raw)
	}
	if n := withdrawalHTTPRequestCount(t, ctx, pool); n != 0 {
		t.Fatalf("withdrawal_requests rows = %d, want 0", n)
	}
	if n := withdrawalHTTPGrantBindCount(t, ctx, pool, "auth-reject-bind"); n != 0 {
		t.Fatalf("grant binds = %d, want 0", n)
	}
	if n := withdrawalHTTPGrantAuditCount(t, ctx, pool); n != grantAuditBefore {
		t.Fatalf("withdrawal_grant_audit rows = %d, want baseline %d (rejected create adds none)", n, grantAuditBefore)
	}
	withdrawalHTTPWaitAuditAction(t, ctx, pool, "rejected", 1)
}
