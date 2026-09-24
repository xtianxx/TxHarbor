//go:build e2e

// ratelimit_failure_integration_test.go is the T067 acceptance (E2E): on the
// real serve routes and middleware wiring, a Redis outage makes limiting
// unavailable and the PD-1 policy refuses new withdrawal creation with a
// clear retryable error (Retry-After, 007 taxonomy code) while the accepted
// withdrawal, the query path and the 008 read path continue; recovery is
// graded, and every admitted request still passes the original gates
// (authentication, idempotency) with 0 bypass. The distributed RPC budget's
// paused class is observable.
//
// No Anvil/Kafka is needed: the test drives the serve middleware stack, the
// real 007 handler and a real PostgreSQL + Redis. The full-process drills
// belong to B9 (faultdrill).
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

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/health"
	"github.com/xtianxx/txharbor/internal/nonce"
	"github.com/xtianxx/txharbor/internal/ratelimit"
	"github.com/xtianxx/txharbor/internal/withdrawal"
)

// t067BudgetObserver records RPC budget pause transitions.
type t067BudgetObserver struct {
	paused chan string
}

func (o *t067BudgetObserver) ObserveRPCBudgetPaused(class string) {
	select {
	case o.paused <- class:
	default:
	}
}

func TestRatelimitFailureHTTPPolicy(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dsn := e2eStartPostgres(t)
	pool := e2eOpenPool(t, dsn)
	redisCtr := e2eStartRedis(t)

	env := e2eBaseEnv(dsn, "http://127.0.0.1:1", e2eFreeAddr(t), []string{"127.0.0.1:9092"})
	env["TXHARBOR_REDIS_ADDR"] = redisCtr.HostPort()
	env["TXHARBOR_REDIS_TIMEOUT"] = "1s"
	cfg, err := config.Load(e2eGetenv(env))
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}

	// The real 013 middleware stack over a real Redis.
	redisClient := redis.NewClient(&redis.Options{
		Addr:         cfg.Redis.Addr,
		DialTimeout:  cfg.Redis.Timeout,
		ReadTimeout:  cfg.Redis.Timeout,
		WriteTimeout: cfg.Redis.Timeout,
	})
	t.Cleanup(func() { _ = redisClient.Close() })
	scriptStore, err := ratelimit.NewRedisScriptStore(redisClient)
	if err != nil {
		t.Fatalf("NewRedisScriptStore: %v", err)
	}
	limiter, err := buildLimiter(scriptStore, cfg, nil)
	if err != nil {
		t.Fatalf("buildLimiter: %v", err)
	}
	policy, err := ratelimit.NewPolicy(limiter)
	if err != nil {
		t.Fatalf("NewPolicy: %v", err)
	}
	budgetObserver := &t067BudgetObserver{paused: make(chan string, 4)}
	budget, err := buildRPCBudget(limiter, budgetObserver)
	if err != nil {
		t.Fatalf("buildRPCBudget: %v", err)
	}

	// Real dependency probe: the Redis container stop/start drives the
	// non-critical signal and the degradation annotation.
	signals := health.NewDependencySignals()
	depRunner := &health.DependencyRunner{
		Interval: 200 * time.Millisecond,
		Timeout:  cfg.Redis.Timeout,
		Signals:  signals,
		Probes: []health.DependencyProbe{{
			Name:  "redis",
			Probe: func(ctx context.Context) error { return redisClient.Ping(ctx).Err() },
		}},
	}
	go depRunner.Run(ctx)
	degradation := newDegradationState(true, signals, "redis")

	const (
		callerID    = int64(6701)
		asset       = "0x1111111111111111111111111111111111111111"
		watch       = "0x2222222222222222222222222222222222222222"
		recipient   = "0x3333333333333333333333333333333333333333"
		authorizeID = "authz-t067"
		amount      = "1000"
	)
	seedT067(t, ctx, pool, callerID, asset, watch, authorizeID, recipient, amount)
	apiKey, _, err := withdrawal.IssueKey(ctx, pool, callerID, "t067")
	if err != nil {
		t.Fatalf("IssueKey: %v", err)
	}

	withdrawH := &WithdrawalHandler{Pool: pool, ChainID: 31337}
	nonceReadH := &nonceReadHandler{provider: nonce.NewReadProvider(pool, cfg.NonceReadToken)}
	mux := http.NewServeMux()
	mux.Handle("/withdrawals", guardRoute(policy, degradation, ratelimit.ClassNewWithdrawal, ratelimit.ClassQuery, withdrawH))
	mux.Handle("/withdrawals/", guardRoute(policy, degradation, ratelimit.ClassNewWithdrawal, ratelimit.ClassQuery, withdrawH))
	mux.Handle("/nonce/bindings/", guardRoute(policy, degradation, ratelimit.ClassQuery, ratelimit.ClassQuery, nonceReadH))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	// 1. Baseline (Redis up): a real create is admitted and passes the
	// handler's gates.
	body := t067CreateBody(t, "idem-t067-1", asset, recipient, amount, authorizeID)
	status, raw := e2eJSONDo(t, http.MethodPost, srv.URL+"/withdrawals", apiKey, body)
	if status != http.StatusCreated && status != http.StatusOK {
		t.Fatalf("baseline create status=%d body=%s", status, raw)
	}
	var created struct {
		RequestID string `json:"request_id"`
	}
	if err := json.Unmarshal(raw, &created); err != nil || created.RequestID == "" {
		t.Fatalf("baseline create body=%s err=%v", raw, err)
	}

	// 2. Redis outage: the limiter becomes unavailable and the probe signal
	// follows.
	if err := redisCtr.Stop(ctx); err != nil {
		t.Fatalf("stop redis: %v", err)
	}
	e2eWait(t, 15*time.Second, "limiter unavailable", func() bool {
		_, err := limiter.Allow(ctx, ratelimit.ClassQuery)
		return err != nil && policy.Degraded()
	})

	// 3. PD-1: a NEW create is refused with the explicit retryable shape and
	// no request row is created (0 unlimited pass).
	newBody := t067CreateBody(t, "idem-t067-2", asset, recipient, amount, authorizeID)
	status, raw = e2eJSONDo(t, http.MethodPost, srv.URL+"/withdrawals", apiKey, newBody)
	if status != http.StatusServiceUnavailable {
		t.Fatalf("create during outage status=%d body=%s, want 503", status, raw)
	}
	if !bytes.Contains(raw, []byte(`"code":"temporarily_unavailable"`)) {
		t.Fatalf("refusal body %s lacks the 007 taxonomy code", raw)
	}
	var refused struct {
		Message string `json:"message"`
	}
	_ = json.Unmarshal(raw, &refused)
	if !strings.Contains(refused.Message, "same idempotency key") {
		t.Fatalf("refusal message %q must instruct a same-key retry", refused.Message)
	}
	if got := t067Count(t, ctx, pool,
		`SELECT count(*) FROM withdrawal_requests WHERE caller_id = $1 AND idempotency_key = 'idem-t067-2'`, callerID); got != 0 {
		t.Fatalf("refused create produced %d request rows, want 0", got)
	}

	// 4. Existing flows continue: the accepted withdrawal is queryable, the
	// query path is annotated (cache bypass + degraded), and the 008 read
	// path still answers its own 401 (no gate bypass, no block).
	getStatus, getRaw, getHeaders := t067GET(t, srv.URL+"/withdrawals/"+created.RequestID, apiKey)
	if getStatus != http.StatusOK {
		t.Fatalf("GET accepted withdrawal during outage status=%d body=%s", getStatus, getRaw)
	}
	if !bytes.Contains(getRaw, []byte(created.RequestID)) {
		t.Fatalf("GET body %s does not carry the accepted withdrawal", getRaw)
	}
	if getHeaders.Get("X-TXHarbor-Cache") != "bypass" {
		t.Fatalf("X-TXHarbor-Cache = %q, want bypass", getHeaders.Get("X-TXHarbor-Cache"))
	}
	if getHeaders.Get("X-TXHarbor-Degraded") != "true" {
		t.Fatalf("X-TXHarbor-Degraded = %q, want true", getHeaders.Get("X-TXHarbor-Degraded"))
	}
	nonceStatus, _, _ := t067GET(t, srv.URL+"/nonce/bindings/unknown", "")
	if nonceStatus != http.StatusUnauthorized {
		t.Fatalf("008 read during outage = %d, want its own 401 (query continues, gate intact)", nonceStatus)
	}

	// 5. The distributed RPC budget's paused class is observable.
	budgetStatus := budget.Status()
	if budgetStatus["send"] != "paused" || budgetStatus["read"] != "degraded" {
		t.Fatalf("rpc budget status = %v, want send=paused read=degraded", budgetStatus)
	}
	select {
	case class := <-budgetObserver.paused:
		if class != "send" {
			t.Fatalf("paused class = %q, want send", class)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("rpc budget pause transition was never observed")
	}

	// 6. Recovery: Redis comes back; the first admitted request still passes
	// the original gates (missing bearer is the handler's 401), and the
	// replay of the accepted create is idempotent.
	if err := redisCtr.Start(ctx); err != nil {
		t.Fatalf("start redis: %v", err)
	}
	e2eWait(t, 30*time.Second, "limiter recovered", func() bool {
		_, err := limiter.Allow(ctx, ratelimit.ClassQuery)
		return err == nil
	})
	status, _ = e2eJSONDo(t, http.MethodPost, srv.URL+"/withdrawals", "", body)
	if status != http.StatusUnauthorized {
		t.Fatalf("create without bearer after recovery = %d, want the handler's 401", status)
	}
	status, raw = e2eJSONDo(t, http.MethodPost, srv.URL+"/withdrawals", apiKey, body)
	if status != http.StatusOK && status != http.StatusCreated {
		t.Fatalf("replay after recovery status=%d body=%s", status, raw)
	}
	var replayed struct {
		RequestID string `json:"request_id"`
	}
	if err := json.Unmarshal(raw, &replayed); err != nil || replayed.RequestID != created.RequestID {
		t.Fatalf("replay request_id = %q (err %v), want %s", replayed.RequestID, err, created.RequestID)
	}
	if got := t067Count(t, ctx, pool,
		`SELECT count(*) FROM withdrawal_requests WHERE caller_id = $1 AND idempotency_key = 'idem-t067-1'`, callerID); got != 1 {
		t.Fatalf("idempotent replay produced %d request rows, want 1", got)
	}
	if got := budget.Status()["send"]; got != "ok" {
		t.Fatalf("rpc budget send after recovery = %q, want ok", got)
	}
}

// t067CreateBody builds one POST /withdrawals body.
func t067CreateBody(t *testing.T, idempotencyKey, asset, recipient, amount, authorizeID string) string {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"idempotency_key":  idempotencyKey,
		"chain_id":         31337,
		"asset":            asset,
		"recipient":        recipient,
		"amount":           amount,
		"authorization_id": authorizeID,
	})
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	return string(raw)
}

// t067GET issues one GET and returns status, body and headers.
func t067GET(t *testing.T, url, token string) (int, []byte, http.Header) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	return resp.StatusCode, raw, resp.Header
}

// t067Count runs one count query.
func t067Count(t *testing.T, ctx context.Context, pool *pgxpool.Pool, query string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(ctx, query, args...).Scan(&n); err != nil {
		t.Fatalf("count query: %v", err)
	}
	return n
}

// seedT067 seeds the 007 prerequisites: caller, the allowlist bootstrap row
// (the deposit scanner would create the same row in a full serve process) and
// an active supply grant.
func seedT067(t *testing.T, ctx context.Context, pool *pgxpool.Pool, callerID int64, asset, watch, authorizeID, recipient, amount string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `INSERT INTO caller (caller_id, label, can_create) VALUES ($1, 't067', TRUE)
		ON CONFLICT (caller_id) DO UPDATE SET can_create = TRUE`, callerID); err != nil {
		t.Fatalf("seed caller: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO deposit_config_history
		(chain_id, version_seq, config_hash, prev_seq, start_block, assets, watches, replay_from, operator, reason)
		VALUES (31337, 1, repeat('a', 64), NULL, 0, $1, $2, 0, 'bootstrap', 'bootstrap')
		ON CONFLICT (chain_id, version_seq) DO NOTHING`, asset+":0", watch+":0"); err != nil {
		t.Fatalf("seed allowlist: %v", err)
	}
	if _, err := withdrawal.SupplyGrant(ctx, pool, withdrawal.OpInput{
		OperationID:     "op-t067-supply",
		Action:          "supply",
		AuthorizationID: authorizeID,
		CallerID:        callerID,
		ChainID:         31337,
		Asset:           asset,
		Recipient:       recipient,
		Amount:          amount,
	}, "t067", "controlled t067 supply"); err != nil {
		t.Fatalf("SupplyGrant: %v", err)
	}
}
