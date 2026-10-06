//go:build e2e

// ratelimit_budget_assembly_integration_test.go is the assembly-level
// acceptance of the 013 "rate-limit decision total wait budget" supplement
// (design docs/evidence/013/redis-latency-budget-design.md §5/§7):
//
//   - the production dedicated limiter client options (ContextTimeoutEnabled,
//     MaxRetries=-1, explicit small pool) built by newLimiterRedisClient;
//   - the token-bucket key TTL keeps the ORIGINAL algorithm (2 × the
//     effective Redis.Timeout) and never follows the tightened decision
//     budget;
//   - a response lost after a confirmed execution must not re-send the
//     non-idempotent bucket script (no double deduction) — the shared-style
//     client contrast preserves the re-send counterexample that motivated the
//     dedicated client;
//   - a held pool bounds every decision at the budget and the pool at
//     limiterPoolSize.
//
// Real Redis carrier and the testutil scripted gate; no serve process and no
// PostgreSQL are needed. Counters follow the three-way discipline (app calls /
// server-bound sends / confirmed executions): execution is confirmed via INFO
// commandstats and bucket state, never inferred from a missing reply.
package app

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/ratelimit"
	"github.com/xtianxx/txharbor/internal/testutil"
)

// rlBudgetBucketKey is the limiter's token-bucket key for ClassQuery
// (Allow builds "txharbor:rl:" + class).
const rlBudgetBucketKey = "txharbor:rl:query"

// rlBudgetDirectClient opens the observation client against the real Redis
// container, bypassing any gate: bucket state and INFO commandstats read from
// here are the ground truth for "the server executed".
func rlBudgetDirectClient(t *testing.T, addr string) *redis.Client {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: addr, ReadTimeout: 5 * time.Second})
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// rlBudgetEvalCalls reads cmdstat_eval:calls from INFO commandstats: the
// confirmed server-side EVAL execution count.
func rlBudgetEvalCalls(t *testing.T, direct *redis.Client) uint64 {
	t.Helper()
	text, err := direct.Do(context.Background(), "INFO", "commandstats").Text()
	if err != nil {
		t.Fatalf("INFO commandstats: %v", err)
	}
	for line := range strings.SplitSeq(text, "\n") {
		if after, ok := strings.CutPrefix(line, "cmdstat_eval:calls="); ok {
			n, convErr := strconv.ParseUint(strings.SplitN(after, ",", 2)[0], 10, 64)
			if convErr != nil {
				t.Fatalf("parse cmdstat_eval calls=%q: %v", after, convErr)
			}
			return n
		}
	}
	t.Fatalf("cmdstat_eval not found in INFO commandstats output")
	return 0
}

// rlBudgetTokens reads the bucket's `tokens` field ("" when the key expired).
func rlBudgetTokens(t *testing.T, direct *redis.Client) string {
	t.Helper()
	v, err := direct.HGet(context.Background(), rlBudgetBucketKey, "tokens").Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "" // key expired / not created yet: recorded, never inferred
		}
		t.Fatalf("HGET tokens: %v", err)
	}
	return v
}

// rlBudgetTokenValue parses one stored token value.
func rlBudgetTokenValue(t *testing.T, raw string) float64 {
	t.Helper()
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		t.Fatalf("parse bucket tokens %q: %v", raw, err)
	}
	return v
}

// TestRatelimitAssemblyDedicatedClientOptions pins the production dedicated
// limiter client's options. No container and no dial: Options() is static.
func TestRatelimitAssemblyDedicatedClientOptions(t *testing.T) {
	env := e2eBaseEnv(
		"postgres://u:p@127.0.0.1:5432/db?sslmode=disable",
		"http://127.0.0.1:1",
		e2eFreeAddr(t),
		[]string{"127.0.0.1:9092"},
	)
	env["TXHARBOR_REDIS_ADDR"] = "127.0.0.1:6399" // placeholder: Options() never dials
	cfg, err := config.Load(e2eGetenv(env))
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}

	client := newLimiterRedisClient(cfg)
	t.Cleanup(func() { _ = client.Close() })
	opts := client.Options()

	if opts.Addr != cfg.Redis.Addr {
		t.Errorf("Addr = %q, want %q", opts.Addr, cfg.Redis.Addr)
	}
	if !opts.ContextTimeoutEnabled {
		t.Errorf("ContextTimeoutEnabled = false, want true: the command context deadline must bound every wait (pool, dial, read)")
	}
	// go-redis v9 normalizes Options at construction (options.init): the
	// configured MaxRetries=-1 ("disable retries": a lost response must not
	// re-send the non-idempotent bucket script) reads back as the EFFECTIVE
	// 0 — exactly one attempt — while an unset 0 would read back as the
	// library default 3. Assert the effective value, not the raw input.
	if opts.MaxRetries != 0 {
		t.Errorf("MaxRetries = %d, want the effective 0 (constructed as -1: no re-send; the library default would read back as 3)", opts.MaxRetries)
	}
	if opts.PoolSize != limiterPoolSize {
		t.Errorf("PoolSize = %d, want %d (limiterPoolSize)", opts.PoolSize, limiterPoolSize)
	}
	if opts.DialTimeout != cfg.Redis.Timeout {
		t.Errorf("DialTimeout = %v, want cfg.Redis.Timeout = %v", opts.DialTimeout, cfg.Redis.Timeout)
	}
	if opts.ReadTimeout != cfg.Redis.Timeout {
		t.Errorf("ReadTimeout = %v, want cfg.Redis.Timeout = %v", opts.ReadTimeout, cfg.Redis.Timeout)
	}
	if opts.WriteTimeout != cfg.Redis.Timeout {
		t.Errorf("WriteTimeout = %v, want cfg.Redis.Timeout = %v", opts.WriteTimeout, cfg.Redis.Timeout)
	}
}

// ttlDecouplingRecord is one budget/TTL decoupling observation.
type ttlDecouplingRecord struct {
	Variant         string  `json:"variant"`
	BudgetInherited bool    `json:"budget_inherited"`
	RedisTimeoutMS  float64 `json:"redis_timeout_ms"`
	BudgetMS        float64 `json:"budget_ms"`
	ExpectedTTLMS   float64 `json:"expected_ttl_ms"` // 2 × Redis.Timeout, never 2 × budget
	PTTLMS          float64 `json:"pttl_ms"`
	BoundPass       bool    `json:"bound_pass"`
}

// TestRatelimitBudgetTTLDecoupling asserts the token-bucket key TTL is the
// original algorithm's 2 × effective Redis.Timeout and independent of the
// decision budget: tightening the budget must never shrink the bucket
// lifecycle (idle expiry would silently reset the bucket).
func TestRatelimitBudgetTTLDecoupling(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	redisCtr := e2eStartRedis(t)
	direct := rlBudgetDirectClient(t, redisCtr.HostPort())
	writer, err := testutil.NewEvidenceWriter()
	if err != nil {
		t.Fatalf("NewEvidenceWriter: %v", err)
	}
	t.Cleanup(func() { _ = writer.Close() })
	t.Logf("ttl decoupling evidence dir: %s", writer.Dir())

	variants := []struct {
		name    string
		timeout time.Duration
		budget  string // "" = unset: the budget inherits the effective Redis timeout
	}{
		{"timeout_1s_budget_unset", time.Second, ""},
		{"timeout_1s_budget_200ms", time.Second, "200ms"},
		{"timeout_3s_budget_unset", 3 * time.Second, ""},
		{"timeout_3s_budget_200ms", 3 * time.Second, "200ms"},
	}

	var records []ttlDecouplingRecord
	for _, v := range variants {
		t.Run(v.name, func(t *testing.T) {
			env := e2eBaseEnv(
				"postgres://u:p@127.0.0.1:5432/db?sslmode=disable",
				"http://127.0.0.1:1",
				e2eFreeAddr(t),
				[]string{"127.0.0.1:9092"},
			)
			env["TXHARBOR_REDIS_ADDR"] = redisCtr.HostPort()
			env["TXHARBOR_REDIS_TIMEOUT"] = v.timeout.String()
			if v.budget != "" {
				env[config.EnvRateLimitBudget] = v.budget
			}
			cfg, err := config.Load(e2eGetenv(env))
			if err != nil {
				t.Fatalf("config.Load: %v", err)
			}
			if cfg.Redis.Timeout != v.timeout {
				t.Fatalf("cfg.Redis.Timeout = %v, want %v", cfg.Redis.Timeout, v.timeout)
			}
			wantBudget := v.timeout // default: inherits the effective Redis timeout
			inherited := true
			if v.budget != "" {
				wantBudget = 200 * time.Millisecond
				inherited = false
			}
			if cfg.RateLimit.Budget != wantBudget {
				t.Fatalf("cfg.RateLimit.Budget = %v, want %v (inherited=%v)", cfg.RateLimit.Budget, wantBudget, inherited)
			}

			// The dedicated client must be built with the container address.
			cfg.Redis.Addr = redisCtr.HostPort()
			if err := direct.Del(ctx, rlBudgetBucketKey).Err(); err != nil {
				t.Fatalf("del bucket: %v", err)
			}
			client := newLimiterRedisClient(cfg)
			t.Cleanup(func() { _ = client.Close() })
			store, err := ratelimit.NewRedisScriptStore(client)
			if err != nil {
				t.Fatalf("NewRedisScriptStore: %v", err)
			}
			limiter, err := buildLimiter(store, cfg, nil)
			if err != nil {
				t.Fatalf("buildLimiter: %v", err)
			}
			decision, err := limiter.Allow(ctx, ratelimit.ClassQuery)
			if err != nil || !decision.Allowed {
				t.Fatalf("warm allow: allowed=%v err=%v", decision.Allowed, err)
			}

			pttl, err := direct.PTTL(ctx, rlBudgetBucketKey).Result()
			if err != nil {
				t.Fatalf("PTTL: %v", err)
			}
			expected := 2 * v.timeout
			rec := ttlDecouplingRecord{
				Variant:         v.name,
				BudgetInherited: inherited,
				RedisTimeoutMS:  float64(v.timeout) / float64(time.Millisecond),
				BudgetMS:        float64(wantBudget) / float64(time.Millisecond),
				ExpectedTTLMS:   float64(expected) / float64(time.Millisecond),
				PTTLMS:          float64(pttl) / float64(time.Millisecond),
			}
			rec.BoundPass = pttl > expected-300*time.Millisecond && pttl <= expected
			records = append(records, rec)
			if !rec.BoundPass {
				t.Errorf("PTTL = %v, want %v < PTTL <= %v (2 × Redis.Timeout = %v, never 2 × budget = %v)",
					pttl, expected-300*time.Millisecond, expected, expected, 2*wantBudget)
			}
			if !inherited && pttl <= 2*wantBudget {
				t.Errorf("PTTL = %v equals the 2 × budget formula %v: the TTL must follow 2 × Redis.Timeout, not the budget",
					pttl, 2*wantBudget)
			}
		})
	}
	if err := writer.WriteJSON("ttl_decoupling_records.json", records); err != nil {
		t.Fatalf("write ttl_decoupling_records: %v", err)
	}
}

// assemblyDropRecord is one drop counterexample / control observation row.
type assemblyDropRecord struct {
	Group        string  `json:"group"`
	CTE          bool    `json:"cte"`
	MaxRetries   int     `json:"max_retries"` // configured input: -1 = retries disabled; 0 = unset (library default 3)
	Sends        uint64  `json:"server_bound_sends"`
	Execs        uint64  `json:"confirmed_executions"`
	TokensBefore string  `json:"tokens_before"`
	TokensAfter  string  `json:"tokens_after"`
	DeductedEst  float64 `json:"tokens_deducted_est"`
	DurationMS   float64 `json:"duration_ms"`
	ErrorText    string  `json:"error_text,omitempty"`
}

// TestRatelimitAssemblyDropNoDoubleDeduct drives the response-loss
// counterexample through the real buildLimiter wiring. With the production
// dedicated client (MaxRetries=-1) a response lost after a confirmed
// execution fails single-shot: one server-bound send, one confirmed
// execution, one token deduction. The shared-style client contrast keeps the
// re-send/double-execution counterexample observable — do NOT relax it.
func TestRatelimitAssemblyDropNoDoubleDeduct(t *testing.T) {
	const (
		redisTimeout = time.Second
		budget       = 200 * time.Millisecond
		tolerance    = 150 * time.Millisecond
		dropClose    = 30 * time.Millisecond // close well BEFORE budget expiry
	)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	redisCtr := e2eStartRedis(t)
	gate, err := testutil.NewRedisGate(redisCtr.HostPort())
	if err != nil {
		t.Fatalf("NewRedisGate: %v", err)
	}
	t.Cleanup(func() { _ = gate.Close() })
	direct := rlBudgetDirectClient(t, redisCtr.HostPort())
	writer, err := testutil.NewEvidenceWriter()
	if err != nil {
		t.Fatalf("NewEvidenceWriter: %v", err)
	}
	t.Cleanup(func() { _ = writer.Close() })
	t.Logf("assembly drop evidence dir: %s", writer.Dir())

	env := e2eBaseEnv(
		"postgres://u:p@127.0.0.1:5432/db?sslmode=disable",
		"http://127.0.0.1:1",
		e2eFreeAddr(t),
		[]string{"127.0.0.1:9092"},
	)
	env["TXHARBOR_REDIS_ADDR"] = gate.HostPort()
	env["TXHARBOR_REDIS_TIMEOUT"] = redisTimeout.String()
	env[config.EnvRateLimitBudget] = budget.String()
	// Low-rate bucket (test input): one EVAL's deduction stays observable
	// across the drop window (refill ≤ 0.1 token), mirroring the
	// response-loss experiment fixture.
	env["TXHARBOR_RATELIMIT_QUERY"] = "1/10"
	cfg, err := config.Load(e2eGetenv(env))
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	if cfg.Redis.Timeout != redisTimeout || cfg.RateLimit.Budget != budget {
		t.Fatalf("cfg redis timeout = %v budget = %v, want %v / %v", cfg.Redis.Timeout, cfg.RateLimit.Budget, redisTimeout, budget)
	}
	cfg.Redis.Addr = gate.HostPort()

	var records []assemblyDropRecord

	// --- NEW assembly: the production dedicated client ---
	if err := gate.SetMode(testutil.GatePass); err != nil {
		t.Fatalf("gate pass: %v", err)
	}
	newClient := newLimiterRedisClient(cfg)
	t.Cleanup(func() { _ = newClient.Close() })
	newStore, err := ratelimit.NewRedisScriptStore(newClient)
	if err != nil {
		t.Fatalf("NewRedisScriptStore: %v", err)
	}
	newLimiter, err := buildLimiter(newStore, cfg, nil)
	if err != nil {
		t.Fatalf("buildLimiter: %v", err)
	}
	if err := direct.Del(ctx, rlBudgetBucketKey).Err(); err != nil {
		t.Fatalf("del bucket: %v", err)
	}
	if _, err := newLimiter.Allow(ctx, ratelimit.ClassQuery); err != nil {
		t.Fatalf("new assembly warm allow: %v", err)
	}
	// Flip to Drop AFTER the warm: the warm pair is torn down and the
	// measured Allow dials fresh into Drop mode (HELLO passes, the EVAL
	// reply is withheld and the client socket closes dropClose later).
	gate.SetDropClose(dropClose)
	if err := gate.SetMode(testutil.GateDrop); err != nil {
		t.Fatalf("gate drop: %v", err)
	}
	newBefore := gate.CounterSnapshot()
	newExecBefore := rlBudgetEvalCalls(t, direct)
	newTokensBefore := rlBudgetTokens(t, direct)

	start := time.Now()
	_, newErr := newLimiter.Allow(ctx, ratelimit.ClassQuery)
	newDur := time.Since(start)

	newAfter := gate.CounterSnapshot()
	newExecAfter := rlBudgetEvalCalls(t, direct)
	newTokensAfter := rlBudgetTokens(t, direct)
	newSends := newAfter.EvalFrames - newBefore.EvalFrames
	newExecs := newExecAfter - newExecBefore
	if newErr == nil {
		t.Errorf("new assembly: Allow returned nil error, want ErrUnavailable (response lost)")
	} else if !errors.Is(newErr, ratelimit.ErrUnavailable) {
		t.Errorf("new assembly: err = %v, want errors.Is(err, ratelimit.ErrUnavailable)", newErr)
	}
	if newSends != 1 {
		t.Errorf("new assembly: server-bound EVAL sends = %d, want exactly 1 (MaxRetries=-1 must prevent the re-send)", newSends)
	}
	if newExecs != 1 {
		t.Errorf("new assembly: confirmed executions = %d, want exactly 1 (no double execution)", newExecs)
	}
	if newTokensBefore == "" || newTokensAfter == "" {
		t.Fatalf("new assembly: bucket tokens missing (before=%q after=%q)", newTokensBefore, newTokensAfter)
	}
	newDeducted := rlBudgetTokenValue(t, newTokensBefore) - rlBudgetTokenValue(t, newTokensAfter)
	if newDeducted < 0.5 || newDeducted >= 1.5 {
		t.Errorf("new assembly: tokens deducted = %.3f, want in [0.5, 1.5) (exactly one deduction)", newDeducted)
	}
	if newDur > budget+tolerance {
		t.Errorf("new assembly: Allow took %v, want <= %v (budget %v + %v tolerance)", newDur, budget+tolerance, budget, tolerance)
	}
	records = append(records, assemblyDropRecord{
		Group: "new_assembly_dedicated_client", CTE: true, MaxRetries: -1,
		Sends: newSends, Execs: newExecs,
		TokensBefore: newTokensBefore, TokensAfter: newTokensAfter, DeductedEst: newDeducted,
		DurationMS: float64(newDur.Microseconds()) / 1000.0,
		ErrorText:  errText(newErr),
	})

	// --- OLD assembly contrast: shared-style client (no CTE, default retries) ---
	// The counterexample this supplement fixes must stay observable: a lost
	// response inside the budget is retried and re-executed here.
	if err := gate.SetMode(testutil.GatePass); err != nil {
		t.Fatalf("gate pass: %v", err)
	}
	oldClient := redis.NewClient(&redis.Options{
		Addr:         cfg.Redis.Addr,
		DialTimeout:  cfg.Redis.Timeout,
		ReadTimeout:  cfg.Redis.Timeout,
		WriteTimeout: cfg.Redis.Timeout,
	})
	t.Cleanup(func() { _ = oldClient.Close() })
	oldStore, err := ratelimit.NewRedisScriptStore(oldClient)
	if err != nil {
		t.Fatalf("NewRedisScriptStore: %v", err)
	}
	oldLimiter, err := buildLimiter(oldStore, cfg, nil)
	if err != nil {
		t.Fatalf("buildLimiter: %v", err)
	}
	if err := direct.Del(ctx, rlBudgetBucketKey).Err(); err != nil {
		t.Fatalf("del bucket: %v", err)
	}
	if _, err := oldLimiter.Allow(ctx, ratelimit.ClassQuery); err != nil {
		t.Fatalf("old assembly warm allow: %v", err)
	}
	gate.SetDropClose(dropClose)
	if err := gate.SetMode(testutil.GateDrop); err != nil {
		t.Fatalf("gate drop: %v", err)
	}
	oldBefore := gate.CounterSnapshot()
	oldExecBefore := rlBudgetEvalCalls(t, direct)
	oldTokensBefore := rlBudgetTokens(t, direct)

	oldStart := time.Now()
	_, oldErr := oldLimiter.Allow(ctx, ratelimit.ClassQuery)
	oldDur := time.Since(oldStart)

	oldAfter := gate.CounterSnapshot()
	oldExecAfter := rlBudgetEvalCalls(t, direct)
	oldTokensAfter := rlBudgetTokens(t, direct)
	oldSends := oldAfter.EvalFrames - oldBefore.EvalFrames
	oldExecs := oldExecAfter - oldExecBefore
	if oldSends < 2 {
		t.Errorf("old assembly contrast: server-bound EVAL sends = %d, want >= 2 (the shared client's default retry re-sends after a pre-expiry close)", oldSends)
	}
	if oldExecs < 2 {
		t.Errorf("old assembly contrast: confirmed executions = %d, want >= 2 (double execution counterexample; do NOT relax)", oldExecs)
	}
	records = append(records, assemblyDropRecord{
		Group: "old_assembly_shared_client", CTE: false, MaxRetries: 0, // 0 = library default (3 retries)
		Sends: oldSends, Execs: oldExecs,
		TokensBefore: oldTokensBefore, TokensAfter: oldTokensAfter,
		DeductedEst: rlBudgetTokenValue(t, oldTokensBefore) - rlBudgetTokenValue(t, oldTokensAfter),
		DurationMS:  float64(oldDur.Microseconds()) / 1000.0,
		ErrorText:   errText(oldErr),
	})

	if err := writer.WriteJSON("assembly_drop_records.json", records); err != nil {
		t.Fatalf("write assembly_drop_records: %v", err)
	}
}

// errText renders an error for an evidence record ("" when nil).
func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// poolCompetitionRecord is one pooled-competition observation.
type poolCompetitionRecord struct {
	BudgetMS         float64 `json:"budget_ms"`
	Goroutines       int     `json:"goroutines"`
	PoolSize         int     `json:"pool_size"`
	MinDurationMS    float64 `json:"min_duration_ms"`
	MedianDurationMS float64 `json:"median_duration_ms"`
	MaxDurationMS    float64 `json:"max_duration_ms"`
	FailedCalls      int     `json:"failed_calls"`
	TotalConns       uint32  `json:"total_conns"`
	TotalConnsBound  int     `json:"total_conns_bound"`
	Unavailable      bool    `json:"limiter_unavailable"`
}

// TestRatelimitAssemblyPoolCompetition holds the gate (accepted connections
// that never answer) and fires twice the pool size concurrently: every
// decision must return by the budget (pool wait, dial and read are all
// ctx-bounded under ContextTimeoutEnabled) and the dedicated pool must stay
// within limiterPoolSize.
func TestRatelimitAssemblyPoolCompetition(t *testing.T) {
	const (
		budget    = 200 * time.Millisecond
		tolerance = 150 * time.Millisecond
	)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	redisCtr := e2eStartRedis(t)
	gate, err := testutil.NewRedisGate(redisCtr.HostPort())
	if err != nil {
		t.Fatalf("NewRedisGate: %v", err)
	}
	t.Cleanup(func() { _ = gate.Close() })
	writer, err := testutil.NewEvidenceWriter()
	if err != nil {
		t.Fatalf("NewEvidenceWriter: %v", err)
	}
	t.Cleanup(func() { _ = writer.Close() })
	t.Logf("pool competition evidence dir: %s", writer.Dir())

	env := e2eBaseEnv(
		"postgres://u:p@127.0.0.1:5432/db?sslmode=disable",
		"http://127.0.0.1:1",
		e2eFreeAddr(t),
		[]string{"127.0.0.1:9092"},
	)
	env["TXHARBOR_REDIS_ADDR"] = gate.HostPort()
	env[config.EnvRateLimitBudget] = budget.String()
	cfg, err := config.Load(e2eGetenv(env))
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	if cfg.RateLimit.Budget != budget {
		t.Fatalf("cfg.RateLimit.Budget = %v, want %v", cfg.RateLimit.Budget, budget)
	}
	cfg.Redis.Addr = gate.HostPort()

	client := newLimiterRedisClient(cfg)
	t.Cleanup(func() { _ = client.Close() })
	store, err := ratelimit.NewRedisScriptStore(client)
	if err != nil {
		t.Fatalf("NewRedisScriptStore: %v", err)
	}
	limiter, err := buildLimiter(store, cfg, nil)
	if err != nil {
		t.Fatalf("buildLimiter: %v", err)
	}
	// Warm in Pass so the pool holds one established pair, then hold: the
	// listener and every pair swallow reads without replying.
	if err := gate.SetMode(testutil.GatePass); err != nil {
		t.Fatalf("gate pass: %v", err)
	}
	if _, err := limiter.Allow(ctx, ratelimit.ClassQuery); err != nil {
		t.Fatalf("warm allow: %v", err)
	}
	if err := gate.SetMode(testutil.GateHold); err != nil {
		t.Fatalf("gate hold: %v", err)
	}

	workers := 2 * limiterPoolSize
	durations := make([]time.Duration, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := range workers {
		go func() {
			defer wg.Done()
			start := time.Now()
			_, err := limiter.Allow(context.Background(), ratelimit.ClassQuery)
			durations[i] = time.Since(start)
			errs[i] = err
		}()
	}
	wg.Wait()

	failed := 0
	for i := range workers {
		if errs[i] == nil {
			t.Errorf("worker %d: Allow returned nil error under GateHold (no trusted reply can arrive)", i)
		} else {
			failed++
		}
		if durations[i] > budget+tolerance {
			t.Errorf("worker %d: Allow took %v, want <= %v (budget %v + %v tolerance)", i, durations[i], budget+tolerance, budget, tolerance)
		}
	}
	sorted := append([]time.Duration(nil), durations...)
	sort.Slice(sorted, func(a, b int) bool { return sorted[a] < sorted[b] })
	stats := client.PoolStats()
	if stats.TotalConns > limiterPoolSize {
		t.Errorf("PoolStats().TotalConns = %d, want <= %d (limiterPoolSize)", stats.TotalConns, limiterPoolSize)
	}
	if !limiter.Unavailable() {
		t.Errorf("limiter.Unavailable() = false after %d held/cut decisions, want true", workers)
	}
	record := poolCompetitionRecord{
		BudgetMS:         float64(budget) / float64(time.Millisecond),
		Goroutines:       workers,
		PoolSize:         limiterPoolSize,
		MinDurationMS:    float64(sorted[0].Microseconds()) / 1000.0,
		MedianDurationMS: float64(sorted[len(sorted)/2].Microseconds()) / 1000.0,
		MaxDurationMS:    float64(sorted[len(sorted)-1].Microseconds()) / 1000.0,
		FailedCalls:      failed,
		TotalConns:       stats.TotalConns,
		TotalConnsBound:  limiterPoolSize,
		Unavailable:      limiter.Unavailable(),
	}
	if err := writer.WriteJSON("pool_competition_records.json", record); err != nil {
		t.Fatalf("write pool_competition_records: %v", err)
	}
}

// cancelIdentityObserver records the limiter availability transitions and RPC
// pause transitions the assembly-level cancellation acceptance checks.
type cancelIdentityObserver struct {
	mu      sync.Mutex
	unavail []bool
	paused  []string
}

func (o *cancelIdentityObserver) ObserveRateLimitDenied(string) {}

func (o *cancelIdentityObserver) SetRateLimitUnavailable(unavailable bool) {
	o.mu.Lock()
	o.unavail = append(o.unavail, unavailable)
	o.mu.Unlock()
}

func (o *cancelIdentityObserver) ObserveRateLimitRecovery(string) {}

func (o *cancelIdentityObserver) ObserveRPCBudgetPaused(class string) {
	o.mu.Lock()
	o.paused = append(o.paused, class)
	o.mu.Unlock()
}

func (o *cancelIdentityObserver) snapshot() (unavail []bool, paused []string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]bool(nil), o.unavail...), append([]string(nil), o.paused...)
}

// cancelIdentityRecord is one row of the cancellation acceptance evidence.
type cancelIdentityRecord struct {
	Scenario      string  `json:"scenario"`
	CancelAtMS    float64 `json:"cancel_at_ms"`
	DurationMS    float64 `json:"duration_ms"`
	ErrorText     string  `json:"error_text"`
	IsCanceled    bool    `json:"errors_is_context_canceled"`
	IsDeadline    bool    `json:"errors_is_deadline_exceeded"`
	Marked        bool    `json:"limiter_marked_unavailable_after"`
	UnavailEvents int     `json:"unavailable_observer_events"`
	PausedEvents  int     `json:"rpc_pause_observer_events"`
	RPCStatusSend string  `json:"rpc_status_send"`
	Note          string  `json:"note"`
}

// TestRatelimitAssemblyCancelIdentity: with the production dedicated client
// (ContextTimeoutEnabled + MaxRetries=-1 — the in-flight terminal error is a
// socket timeout, NOT a context error) a caller cancellation must still be
// reported as context.Canceled (identity from the context join), must NOT mark
// the limiter unavailable and must NOT pause the RPC send class; the
// dependency-timeout control path keeps marking (only a clear caller cancel is
// exempt). This is the real-socket acceptance the design §7 requires.
func TestRatelimitAssemblyCancelIdentity(t *testing.T) {
	const (
		budget    = 200 * time.Millisecond
		tolerance = 150 * time.Millisecond
		cancelAt  = 60 * time.Millisecond
	)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	redisCtr := e2eStartRedis(t)
	gate, err := testutil.NewRedisGate(redisCtr.HostPort())
	if err != nil {
		t.Fatalf("NewRedisGate: %v", err)
	}
	t.Cleanup(func() { _ = gate.Close() })
	writer, err := testutil.NewEvidenceWriter()
	if err != nil {
		t.Fatalf("NewEvidenceWriter: %v", err)
	}
	t.Cleanup(func() { _ = writer.Close() })
	t.Logf("cancel identity evidence dir: %s", writer.Dir())

	env := e2eBaseEnv(
		"postgres://u:p@127.0.0.1:5432/db?sslmode=disable",
		"http://127.0.0.1:1",
		e2eFreeAddr(t),
		[]string{"127.0.0.1:9092"},
	)
	env["TXHARBOR_REDIS_ADDR"] = gate.HostPort()
	env[config.EnvRateLimitBudget] = budget.String()
	cfg, err := config.Load(e2eGetenv(env))
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	if cfg.RateLimit.Budget != budget {
		t.Fatalf("cfg.RateLimit.Budget = %v, want %v", cfg.RateLimit.Budget, budget)
	}

	obs := &cancelIdentityObserver{}
	client := newLimiterRedisClient(cfg)
	t.Cleanup(func() { _ = client.Close() })
	store, err := ratelimit.NewRedisScriptStore(client)
	if err != nil {
		t.Fatalf("NewRedisScriptStore: %v", err)
	}
	limiter, err := buildLimiter(store, cfg, obs)
	if err != nil {
		t.Fatalf("buildLimiter: %v", err)
	}
	rpcBudget, err := buildRPCBudget(limiter, obs)
	if err != nil {
		t.Fatalf("buildRPCBudget: %v", err)
	}

	// Warm in Pass so an established pair exists, then hold everything.
	if err := gate.SetMode(testutil.GatePass); err != nil {
		t.Fatalf("gate pass: %v", err)
	}
	if _, err := limiter.Allow(ctx, ratelimit.ClassQuery); err != nil {
		t.Fatalf("warm allow: %v", err)
	}
	if err := gate.SetMode(testutil.GateHold); err != nil {
		t.Fatalf("gate hold: %v", err)
	}

	var records []cancelIdentityRecord

	// (a) In-flight caller cancellation, no-retry client.
	activeCtx, activeCancel := context.WithCancel(ctx)
	time.AfterFunc(cancelAt, activeCancel)
	start := time.Now()
	_, cancelErr := limiter.Allow(activeCtx, ratelimit.ClassQuery)
	cancelDur := time.Since(start)
	unavailAfterCancel, pauseAfterCancel := obs.snapshot()
	cancelMarked := limiter.Unavailable()
	records = append(records, cancelIdentityRecord{
		Scenario: "cancel_inflight_dedicated", CancelAtMS: float64(cancelAt) / float64(time.Millisecond),
		DurationMS: float64(cancelDur.Microseconds()) / 1000.0, ErrorText: errText(cancelErr),
		IsCanceled:    errors.Is(cancelErr, context.Canceled),
		IsDeadline:    errors.Is(cancelErr, context.DeadlineExceeded),
		Marked:        cancelMarked,
		UnavailEvents: len(unavailAfterCancel), PausedEvents: len(pauseAfterCancel),
		RPCStatusSend: rpcBudget.Status()["send"],
		Note:          "non-immediate (read runs to the budget); identity must come from the context join",
	})
	if cancelErr == nil {
		t.Errorf("cancel_inflight: Allow returned nil error, want a cancel-identified failure")
	} else {
		if !errors.Is(cancelErr, ratelimit.ErrUnavailable) {
			t.Errorf("cancel_inflight: err = %v, want errors.Is(err, ErrUnavailable) (external shape unchanged)", cancelErr)
		}
		if !errors.Is(cancelErr, context.Canceled) {
			t.Errorf("cancel_inflight: err = %v, want errors.Is(err, context.Canceled) (no-retry terminal error is a socket timeout; the join must carry the cancel)", cancelErr)
		}
		if errors.Is(cancelErr, context.DeadlineExceeded) {
			t.Errorf("cancel_inflight: err = %v, want NO deadline identity (the wait ended by caller cancel)", cancelErr)
		}
	}
	if cancelDur < cancelAt+100*time.Millisecond || cancelDur > budget+tolerance {
		t.Errorf("cancel_inflight: %v, want in [cancel+100ms, L+tolerance] = [%v, %v] (non-immediate, budget-bounded)", cancelDur, cancelAt+100*time.Millisecond, budget+tolerance)
	}
	if cancelMarked {
		t.Errorf("cancel_inflight: limiter marked unavailable on a caller cancel, want untouched")
	}
	if len(unavailAfterCancel) != 0 || len(pauseAfterCancel) != 0 {
		t.Errorf("cancel_inflight: observer saw unavailable=%v paused=%v events, want none on a caller cancel", unavailAfterCancel, pauseAfterCancel)
	}

	// (b) RPC budget posture on a caller cancel (limiter still healthy).
	rpcCtx, rpcCancel := context.WithCancel(ctx)
	time.AfterFunc(cancelAt, rpcCancel)
	rpcStart := time.Now()
	_, rpcErr := rpcBudget.Admit(rpcCtx, ratelimit.RPCClassSend)
	rpcDur := time.Since(rpcStart)
	_, pauseAfterRPC := obs.snapshot()
	rpcStatus := rpcBudget.Status()["send"]
	records = append(records, cancelIdentityRecord{
		Scenario: "rpc_budget_cancel", CancelAtMS: float64(cancelAt) / float64(time.Millisecond),
		DurationMS: float64(rpcDur.Microseconds()) / 1000.0, ErrorText: errText(rpcErr),
		IsCanceled:    errors.Is(rpcErr, context.Canceled),
		IsDeadline:    errors.Is(rpcErr, context.DeadlineExceeded),
		Marked:        limiter.Unavailable(),
		UnavailEvents: len(unavailAfterCancel), PausedEvents: len(pauseAfterRPC),
		RPCStatusSend: rpcStatus,
		Note:          "the send class must not pause on a caller cancel",
	})
	if !errors.Is(rpcErr, context.Canceled) {
		t.Errorf("rpc cancel: err = %v, want errors.Is(err, context.Canceled)", rpcErr)
	}
	if errors.Is(rpcErr, ratelimit.ErrRPCPaused) {
		t.Errorf("rpc cancel: err = %v, want NOT ErrRPCPaused (a cancel is not a budget pause)", rpcErr)
	}
	if rpcStatus != "ok" {
		t.Errorf("rpc cancel: send status = %q, want ok (posture untouched by the cancel)", rpcStatus)
	}
	if len(pauseAfterRPC) != 0 {
		t.Errorf("rpc cancel: pause observer events = %v, want none", pauseAfterRPC)
	}

	// (c) Dependency-timeout control: no cancel, the hold must still mark.
	controlStart := time.Now()
	_, controlErr := limiter.Allow(ctx, ratelimit.ClassQuery)
	controlDur := time.Since(controlStart)
	unavailAfterControl, _ := obs.snapshot()
	controlMarked := limiter.Unavailable()
	records = append(records, cancelIdentityRecord{
		Scenario: "timeout_control", CancelAtMS: 0,
		DurationMS: float64(controlDur.Microseconds()) / 1000.0, ErrorText: errText(controlErr),
		IsCanceled:    errors.Is(controlErr, context.Canceled),
		IsDeadline:    errors.Is(controlErr, context.DeadlineExceeded),
		Marked:        controlMarked,
		UnavailEvents: len(unavailAfterControl), PausedEvents: len(pauseAfterRPC),
		RPCStatusSend: rpcBudget.Status()["send"],
		Note:          "the dependency-timeout path keeps marking (only a caller cancel is exempt)",
	})
	if controlErr == nil {
		t.Errorf("timeout control: Allow returned nil error, want a budget-timeout failure")
	} else {
		if !errors.Is(controlErr, context.DeadlineExceeded) {
			t.Errorf("timeout control: err = %v, want errors.Is(err, context.DeadlineExceeded)", controlErr)
		}
		if errors.Is(controlErr, context.Canceled) {
			t.Errorf("timeout control: err = %v, want NO cancel identity", controlErr)
		}
	}
	if controlDur > budget+tolerance {
		t.Errorf("timeout control: %v > %v (budget bound violated)", controlDur, budget+tolerance)
	}
	if !controlMarked {
		t.Errorf("timeout control: limiter not marked unavailable on the dependency-timeout path")
	}
	if len(unavailAfterControl) != 1 || !unavailAfterControl[0] {
		t.Errorf("timeout control: unavailable observer events = %v, want exactly one true (the control marked)", unavailAfterControl)
	}

	if err := writer.WriteJSON("cancel_identity_records.json", records); err != nil {
		t.Fatalf("write cancel_identity_records: %v", err)
	}
	for _, r := range records {
		t.Logf("%-26s dur=%7.1fms canceled=%-5v deadline=%-5v marked=%-5v unavailEvents=%d pausedEvents=%d rpc_send=%s %q",
			r.Scenario, r.DurationMS, r.IsCanceled, r.IsDeadline, r.Marked, r.UnavailEvents, r.PausedEvents, r.RPCStatusSend, r.ErrorText)
	}
}
