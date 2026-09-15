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
	withdrawalHTTPChainID   int64 = 31337
	withdrawalHTTPAsset           = "0x1111111111111111111111111111111111111111"
	withdrawalHTTPRecipient       = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	withdrawalHTTPAmount          = "100"
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

// withdrawalHTTPHandler builds the handler under test for one pool.
func withdrawalHTTPHandler(pool *pgxpool.Pool) *WithdrawalHandler {
	return &WithdrawalHandler{
		Pool:      pool,
		ChainID:   withdrawalHTTPChainID,
		Allowlist: []string{withdrawalHTTPAsset},
	}
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
