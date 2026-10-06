//go:build integration_redis

// redisbudget_cte_test.go: directional mechanism validation for the "rate-
// limit decision total wait budget" supplement design
// (docs/evidence/013/redis-latency-budget-design.md). Test-side only:
// ContextTimeoutEnabled and the candidate budget are set on TEST clients;
// no production code, config default, error mapping or CI is touched.
//
// What this file proves (and what it deliberately does NOT prove):
//   - CTE boundary: with ContextTimeoutEnabled=true, caller-returned time for
//     hot-read-block / cold-init-block / pool-wait / parent-deadline-early is
//     bounded by min(parent, L) + budgetVerifyEpsilon (scheduling jitter
//     ONLY — v9.22.0 computes the socket deadline from time.Now()
//     (conn.go:29-40), so the historical "≤50ms cached clock staleness"
//     referenced by older notes does not exist in this version; even under a
//     stale clock the direction would only be earlier deadlines).
//   - "socket adopts the context deadline" ≠ "active cancel instantly
//     interrupts all I/O": cancelling mid-flight read does NOT return at the
//     cancel instant; the read runs to the socket deadline (already computed
//     at read start) and only the FOLLOWING ctx check (retry Sleep) reports
//     the cancel. Cancelling during a pool wait DOES return promptly. Both
//     are measured, not asserted from prose.
//   - Caller-returned ≠ background goroutines exited: the only background
//     observable here is the gate accept counter over a fixed 2s window
//     AFTER both callers returned. Zero new accepts in that window is
//     recorded as an observation, never as proof of goroutine exit.
//   - Response loss: confirmed-server-execution is read from Redis
//     INFO commandstats (cmdstat_eval:calls) plus the bucket's `tokens`
//     field — NEVER inferred from a missing reply. Application calls,
//     server-bound sends (gate EVAL marker count) and confirmed executions
//     are recorded as three separate counters.
//
// Candidate values (budgetVerifyL / budgetVerifyEpsilon / closeAfter) are
// verification INPUTS for this experiment, not production defaults and not
// approved SLOs.
package ratelimit

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"

	"github.com/xtianxx/txharbor/internal/testutil"
)

// budgetVerifyL is the candidate verification budget for the decision path.
// Test input only — the production default stays TXHARBOR_REDIS_TIMEOUT=1s
// and no candidate here is an approved SLO (待测/待裁决).
const budgetVerifyL = 200 * time.Millisecond

// budgetVerifyEpsilon is the POSITIVE scheduling tolerance for caller-return
// assertions on this host: Go timer/scheduler jitter only. v9.22.0 computes
// socket deadlines from time.Now() (conn.go:29-40), so there is no cached
// clock staleness in this version; even under a historical stale clock the
// direction would only be earlier deadlines (never upper-side). Verification
// input.
const budgetVerifyEpsilon = 80 * time.Millisecond

// budgetVerifyCancelAt is the moment the cancel variants cancel mid-flight.
const budgetVerifyCancelAt = 60 * time.Millisecond

// budgetVerifyDropClose is how long after the first withheld EVAL reply the
// gate closes the client socket in the close-before-expiry group: far below
// budgetVerifyL by construction ("closed BEFORE budget expiry").
const budgetVerifyDropClose = 30 * time.Millisecond

// cteOpts configures a test limiter/client pair.
type cteOpts struct {
	cte        bool // ContextTimeoutEnabled
	maxRetries int  // go-redis Options.MaxRetries (0 = library default 3; -1 = disabled)
	poolSize   int  // 0 = library default
	classes    map[Class]ClassConfig
}

// defaultBudgetClasses keeps the bench-scale 500/100 (fault is attributable
// to the injection, not to a local ceiling).
func defaultBudgetClasses() map[Class]ClassConfig {
	c := ClassConfig{RatePerSecond: 500, Burst: 100}
	return map[Class]ClassConfig{
		ClassNewWithdrawal: c, ClassWrite: c, ClassQuery: c,
		ClassOperator: c, ClassRPC: c,
	}
}

// buildCTEBudgetLimiter builds the real 013 limiter over a TEST client with
// the experiment's options; returns the concrete *redis.Client so PoolStats
// can be observed.
func buildCTEBudgetLimiter(t *testing.T, addr string, o cteOpts) (*Limiter, *redis.Client) {
	t.Helper()
	ropts := &redis.Options{
		Addr:                  addr,
		DialTimeout:           1 * time.Second, // production knob stays 1s (test does NOT shorten it)
		ReadTimeout:           1 * time.Second,
		WriteTimeout:          1 * time.Second,
		ContextTimeoutEnabled: o.cte,
		MaxRetries:            o.maxRetries,
	}
	if o.poolSize > 0 {
		ropts.PoolSize = o.poolSize
	}
	client := redis.NewClient(ropts)
	t.Cleanup(func() { _ = client.Close() })
	store, err := NewRedisScriptStore(client)
	if err != nil {
		t.Fatalf("NewRedisScriptStore: %v", err)
	}
	classes := o.classes
	if classes == nil {
		classes = defaultBudgetClasses()
	}
	limiter, err := NewLimiter(store, Config{
		Classes:        classes,
		Timeout:        budgetVerifyL,
		RecoveryWindow: 10 * time.Second,
	}, &t066Observer{})
	if err != nil {
		t.Fatalf("NewLimiter: %v", err)
	}
	return limiter, client
}

// connState is one PoolStats observation (connection-cleanup evidence).
type connState struct {
	TotalConns uint32 `json:"total_conns"`
	IdleConns  uint32 `json:"idle_conns"`
	StaleConns uint32 `json:"stale_conns"`
	Hits       uint32 `json:"hits"`
	Misses     uint32 `json:"misses"`
	Timeouts   uint32 `json:"timeouts"`
}

func observeConnState(c *redis.Client) connState {
	ps := c.PoolStats()
	return connState{
		TotalConns: ps.TotalConns, IdleConns: ps.IdleConns, StaleConns: ps.StaleConns,
		Hits: ps.Hits, Misses: ps.Misses, Timeouts: ps.Timeouts,
	}
}

// cteScenarioRecord is one measured boundary scenario.
type cteScenarioRecord struct {
	Scenario      string    `json:"scenario"`
	Caller        string    `json:"caller"` // "primary" | "secondary"
	DurationMS    float64   `json:"duration_ms"`
	OK            bool      `json:"ok"`
	ErrorCode     string    `json:"error_class"`
	ErrorText     string    `json:"error_text,omitempty"`
	ConnBefore    connState `json:"conn_before"`
	ConnAfter     connState `json:"conn_after"`
	GateAtReturn  int       `json:"gate_accepts_at_return"`
	GateAfter2s   int       `json:"gate_accepts_after_2s"`
	BackgroundAdd int       `json:"gate_accepts_added_in_2s_after_return"`
	DurationBound string    `json:"duration_bound_checked"`
	BoundPass     bool      `json:"bound_pass"`
	Note          string    `json:"note,omitempty"`
}

// errorTextFor renders an error for the record; "" when nil.
func errorTextFor(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// observeBackgroundAccepts samples gate accepts, waits, samples again.
func observeBackgroundAccepts(gate *testutil.RedisGate) (at, after2s, added int) {
	at64 := gate.CounterSnapshot().Accepts
	time.Sleep(2 * time.Second)
	after64 := gate.CounterSnapshot().Accepts
	return int(at64), int(after64), int(after64 - at64)
}

// TestRedisBudgetCTEBoundary measures caller-returned time / error / conn
// cleanup / background-accept window for six scenarios under candidate
// budget L=200ms. Assertions are the stated bounds; NOTHING is relaxed to
// pass — a failure here is a mechanism finding, not a flake to paper over.
func TestRedisBudgetCTEBoundary(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	redisCtr, err := testutil.StartRedis(ctx)
	if err != nil {
		t.Fatalf("StartRedis: %v", err)
	}
	t.Cleanup(func() { _ = redisCtr.Close(context.Background()) })
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
	t.Logf("cte evidence dir: %s", writer.Dir())

	var records []cteScenarioRecord
	allowOnce := func(l *Limiter, cctx context.Context) (time.Duration, error) {
		start := time.Now()
		_, err := l.Allow(cctx, ClassQuery)
		return time.Since(start), err
	}

	// --- 1a. hot read blocked + concurrent pool-wait (CTE on, PoolSize=1) ---
	t.Run("hot_poolwait", func(t *testing.T) {
		if err := gate.SetMode(testutil.GatePass); err != nil {
			t.Fatalf("pass: %v", err)
		}
		limiter, client := buildCTEBudgetLimiter(t, gate.HostPort(), cteOpts{cte: true, poolSize: 1})
		warmCtx, warmCancel := context.WithTimeout(ctx, 5*time.Second)
		if err := client.Ping(warmCtx).Err(); err != nil {
			warmCancel()
			t.Fatalf("warm ping: %v", err)
		}
		warmCancel()
		if err := gate.SetMode(testutil.GateHold); err != nil {
			t.Fatalf("hold: %v", err)
		}
		type res struct {
			name string
			dur  time.Duration
			err  error
		}
		var wg sync.WaitGroup
		results := make([]res, 2)
		wg.Add(1)
		go func() {
			defer wg.Done()
			d, err := allowOnce(limiter, ctx)
			results[0] = res{"primary", d, err}
		}()
		time.Sleep(30 * time.Millisecond) // primary wins the single pool slot
		wg.Add(1)
		go func() {
			defer wg.Done()
			d, err := allowOnce(limiter, ctx)
			results[1] = res{"secondary", d, err}
		}()
		wg.Wait()
		connAfter := observeConnState(client)
		at, after2s, added := observeBackgroundAccepts(gate)
		for _, r := range results {
			bound := "duration <= L + epsilon"
			pass := r.err != nil && r.dur <= budgetVerifyL+budgetVerifyEpsilon
			records = append(records, cteScenarioRecord{
				Scenario: "hot_poolwait", Caller: r.name,
				DurationMS: float64(r.dur.Microseconds()) / 1000.0, OK: r.err == nil,
				ErrorCode: classifyRedisError(r.err), ErrorText: errorTextFor(r.err),
				ConnAfter: connAfter, GateAtReturn: at, GateAfter2s: after2s,
				BackgroundAdd: added, DurationBound: bound, BoundPass: pass,
				Note: "primary: in-flight read blocked on hold; secondary: pool-wait (PoolSize=1) then hold",
			})
		}
		if results[0].err == nil || results[1].err == nil {
			t.Errorf("hot_poolwait: expected both callers to fail under hold, got %v / %v", results[0].err, results[1].err)
		}
		for _, r := range results {
			if r.dur > budgetVerifyL+budgetVerifyEpsilon {
				t.Errorf("hot_poolwait/%s: returned in %v > L+epsilon=%v (CTE bound violated)", r.name, r.dur, budgetVerifyL+budgetVerifyEpsilon)
			}
		}
		// Connection cleanup: both callers' conns must be removed (bad-conn
		// path) by the time Allow returned; release runs before return.
		if connAfter.TotalConns != 0 {
			t.Errorf("hot_poolwait: PoolStats.TotalConns=%d after return, want 0 (bad conns removed)", connAfter.TotalConns)
		}
		// Background boundary: no new conns observed for 2s after return.
		if added != 0 {
			t.Errorf("hot_poolwait: gate accepts +%d in 2s after caller return (background dial observed)", added)
		}
	})

	// --- 1b. cancel mid-flight read vs cancel during pool-wait ---
	t.Run("cancel_points", func(t *testing.T) {
		if err := gate.SetMode(testutil.GatePass); err != nil {
			t.Fatalf("pass: %v", err)
		}
		limiter, client := buildCTEBudgetLimiter(t, gate.HostPort(), cteOpts{cte: true, poolSize: 1})
		warmCtx, warmCancel := context.WithTimeout(ctx, 5*time.Second)
		if err := client.Ping(warmCtx).Err(); err != nil {
			warmCancel()
			t.Fatalf("warm ping: %v", err)
		}
		warmCancel()
		if err := gate.SetMode(testutil.GateHold); err != nil {
			t.Fatalf("hold: %v", err)
		}
		type res struct {
			name   string
			dur    time.Duration
			err    error
			cancel time.Duration
		}
		var wg sync.WaitGroup
		results := make([]res, 2)
		wg.Add(1)
		go func() { // cancel mid-in-flight read: expected NON-immediate
			defer wg.Done()
			cctx, ccancel := context.WithCancel(ctx)
			time.AfterFunc(budgetVerifyCancelAt, ccancel)
			start := time.Now()
			_, err := limiter.Allow(cctx, ClassQuery)
			results[0] = res{"cancel_inflight", time.Since(start), err, budgetVerifyCancelAt}
		}()
		time.Sleep(30 * time.Millisecond) // primary holds the single slot
		wg.Add(1)
		go func() { // cancel during pool-wait: expected PROMPT
			defer wg.Done()
			cctx, ccancel := context.WithCancel(ctx)
			time.AfterFunc(budgetVerifyCancelAt, ccancel)
			start := time.Now()
			_, err := limiter.Allow(cctx, ClassQuery)
			results[1] = res{"cancel_poolwait", time.Since(start), err, budgetVerifyCancelAt}
		}()
		wg.Wait()
		connAfter := observeConnState(client)
		at, after2s, added := observeBackgroundAccepts(gate)
		for _, r := range results {
			var pass bool
			var bound string
			switch r.name {
			case "cancel_inflight":
				// NOT immediate: read deadline was computed at read start;
				// caller returns near L, error reports the cancel only
				// after that read ends (retry Sleep ctx check).
				bound = "cancel_at + 100ms <= duration <= L + epsilon (NOT immediate)"
				pass = r.dur >= budgetVerifyCancelAt+100*time.Millisecond && r.dur <= budgetVerifyL+budgetVerifyEpsilon
			case "cancel_poolwait":
				// Prompt: pool-wait select on ctx.Done returns at cancel.
				bound = "duration <= cancel_at + epsilon"
				pass = r.dur <= budgetVerifyCancelAt+budgetVerifyEpsilon
			}
			records = append(records, cteScenarioRecord{
				Scenario: r.name, Caller: "solo",
				DurationMS: float64(r.dur.Microseconds()) / 1000.0, OK: r.err == nil,
				ErrorCode: classifyRedisError(r.err), ErrorText: errorTextFor(r.err),
				ConnAfter: connAfter, GateAtReturn: at, GateAfter2s: after2s,
				BackgroundAdd: added, DurationBound: bound, BoundPass: pass,
				Note: fmt.Sprintf("cancelled at %v", r.cancel),
			})
		}
		// Cancel mid-in-flight read is NOT instant — this is the recorded
		// mechanism fact the design doc must carry (absolute "cancel
		// interrupts immediately" bound is withdrawn).
		if results[0].dur < budgetVerifyCancelAt+100*time.Millisecond {
			t.Errorf("cancel_inflight: returned %v after cancel-at %v — expected NON-immediate (read runs to socket deadline)", results[0].dur, budgetVerifyCancelAt)
		}
		if results[0].dur > budgetVerifyL+budgetVerifyEpsilon {
			t.Errorf("cancel_inflight: %v > L+epsilon %v", results[0].dur, budgetVerifyL+budgetVerifyEpsilon)
		}
		if !strings.Contains(errorTextFor(results[0].err), "context canceled") {
			t.Errorf("cancel_inflight: error=%q, want context canceled", errorTextFor(results[0].err))
		}
		if results[1].dur > budgetVerifyCancelAt+budgetVerifyEpsilon {
			t.Errorf("cancel_poolwait: %v > cancel+epsilon %v (pool-wait must honor cancel promptly)", results[1].dur, budgetVerifyCancelAt+budgetVerifyEpsilon)
		}
		if !strings.Contains(errorTextFor(results[1].err), "context canceled") {
			t.Errorf("cancel_poolwait: error=%q, want context canceled", errorTextFor(results[1].err))
		}
	})

	// --- 2. cold connection init blocked (HELLO held; fresh client) ---
	t.Run("cold_init", func(t *testing.T) {
		if err := gate.SetMode(testutil.GatePass); err != nil {
			t.Fatalf("pass: %v", err)
		}
		if err := gate.SetMode(testutil.GateHold); err != nil {
			t.Fatalf("hold: %v", err)
		}
		limiter, client := buildCTEBudgetLimiter(t, gate.HostPort(), cteOpts{cte: true}) // fresh pool: no warm
		d, err := allowOnce(limiter, ctx)
		connAfter := observeConnState(client)
		at, after2s, added := observeBackgroundAccepts(gate)
		pass := err != nil && d <= budgetVerifyL+budgetVerifyEpsilon
		records = append(records, cteScenarioRecord{
			Scenario: "cold_init", Caller: "solo",
			DurationMS: float64(d.Microseconds()) / 1000.0, OK: err == nil,
			ErrorCode: classifyRedisError(err), ErrorText: errorTextFor(err),
			ConnAfter: connAfter, GateAtReturn: at, GateAfter2s: after2s,
			BackgroundAdd: added, DurationBound: "duration <= L + epsilon", BoundPass: pass,
			Note: "fresh client: dial OK (gate accepts), HELLO read held; init-failure conn must be Removed",
		})
		if err == nil {
			t.Errorf("cold_init: expected failure under hold, got nil")
		}
		if d > budgetVerifyL+budgetVerifyEpsilon {
			t.Errorf("cold_init: %v > L+epsilon %v", d, budgetVerifyL+budgetVerifyEpsilon)
		}
		if connAfter.TotalConns != 0 {
			t.Errorf("cold_init: TotalConns=%d want 0 (init-failure conn removed)", connAfter.TotalConns)
		}
		if added != 0 {
			t.Errorf("cold_init: gate accepts +%d in 2s after return (background dial observed)", added)
		}
	})

	// --- 3. parent deadline earlier than L ---
	t.Run("parent_deadline", func(t *testing.T) {
		if err := gate.SetMode(testutil.GatePass); err != nil {
			t.Fatalf("pass: %v", err)
		}
		if err := gate.SetMode(testutil.GateHold); err != nil {
			t.Fatalf("hold: %v", err)
		}
		limiter, _ := buildCTEBudgetLimiter(t, gate.HostPort(), cteOpts{cte: true})
		parentBudget := 80 * time.Millisecond
		pctx, pcancel := context.WithTimeout(ctx, parentBudget)
		defer pcancel()
		start := time.Now()
		_, err := limiter.Allow(pctx, ClassQuery)
		d := time.Since(start)
		at, after2s, added := observeBackgroundAccepts(gate)
		bound := "duration <= parent_budget + epsilon AND < L"
		pass := err != nil && d <= parentBudget+budgetVerifyEpsilon && d < budgetVerifyL
		records = append(records, cteScenarioRecord{
			Scenario: "parent_deadline", Caller: "solo",
			DurationMS: float64(d.Microseconds()) / 1000.0, OK: err == nil,
			ErrorCode: classifyRedisError(err), ErrorText: errorTextFor(err),
			GateAtReturn: at, GateAfter2s: after2s, BackgroundAdd: added,
			DurationBound: bound, BoundPass: pass,
			Note: fmt.Sprintf("parent budget %v < L %v: min(parent, L) must win", parentBudget, budgetVerifyL),
		})
		if err == nil {
			t.Errorf("parent_deadline: expected failure, got nil")
		}
		if d > parentBudget+budgetVerifyEpsilon {
			t.Errorf("parent_deadline: %v > parent+epsilon %v (min(parent,L) must win)", d, parentBudget+budgetVerifyEpsilon)
		}
		if d >= budgetVerifyL {
			t.Errorf("parent_deadline: %v >= L %v — parent deadline was ignored", d, budgetVerifyL)
		}
	})

	// --- 4. control: WITHOUT CTE the L bound does NOT hold (conditions) ---
	t.Run("cte_off_hot_control", func(t *testing.T) {
		if err := gate.SetMode(testutil.GatePass); err != nil {
			t.Fatalf("pass: %v", err)
		}
		limiter, client := buildCTEBudgetLimiter(t, gate.HostPort(), cteOpts{cte: false})
		warmCtx, warmCancel := context.WithTimeout(ctx, 5*time.Second)
		if err := client.Ping(warmCtx).Err(); err != nil {
			warmCancel()
			t.Fatalf("warm ping: %v", err)
		}
		warmCancel()
		if err := gate.SetMode(testutil.GateHold); err != nil {
			t.Fatalf("hold: %v", err)
		}
		d, err := allowOnce(limiter, ctx)
		at, after2s, added := observeBackgroundAccepts(gate)
		connAfter := observeConnState(client)
		records = append(records, cteScenarioRecord{
			Scenario: "cte_off_hot_control", Caller: "solo",
			DurationMS: float64(d.Microseconds()) / 1000.0, OK: err == nil,
			ErrorCode: classifyRedisError(err), ErrorText: errorTextFor(err),
			ConnAfter: connAfter, GateAtReturn: at, GateAfter2s: after2s,
			BackgroundAdd: added, DurationBound: "duration >= 0.8s (socket deadline = now+ReadTimeout, NOT L)",
			BoundPass: d >= 800*time.Millisecond,
			Note:      "CTE=false control: caller waits the socket ReadTimeout after the ctx deadline — documents WHY CTE is required for L+epsilon",
		})
		if err == nil {
			t.Errorf("cte_off_hot_control: expected failure, got nil")
		}
		if d < 800*time.Millisecond {
			t.Errorf("cte_off_hot_control: %v < 800ms — expected socket-deadline wait (~1s) without CTE; result differs from the mechanism prediction", d)
		}
		if added != 0 {
			t.Errorf("cte_off_hot_control: gate accepts +%d in 2s after return (background dial observed)", added)
		}
	})

	// --- 5. normal + recovery (contract unchanged under candidate budget) ---
	t.Run("normal_and_recovery", func(t *testing.T) {
		if err := gate.SetMode(testutil.GatePass); err != nil {
			t.Fatalf("pass: %v", err)
		}
		limiter, client := buildCTEBudgetLimiter(t, gate.HostPort(), cteOpts{cte: true})
		d, err := allowOnce(limiter, ctx)
		at, after2s, added := observeBackgroundAccepts(gate)
		records = append(records, cteScenarioRecord{
			Scenario: "normal", Caller: "solo",
			DurationMS: float64(d.Microseconds()) / 1000.0, OK: err == nil,
			ErrorCode:    classifyRedisError(err),
			GateAtReturn: at, GateAfter2s: after2s, BackgroundAdd: added,
			DurationBound: "duration < L AND background_add==0 (sampled)",
			BoundPass:     err == nil && d < budgetVerifyL && added == 0,
		})
		if err != nil {
			t.Errorf("normal: Allow failed: %v", err)
		}
		if d >= budgetVerifyL {
			t.Errorf("normal: %v >= L %v (healthy path must stay far below the budget)", d, budgetVerifyL)
		}
		if added != 0 {
			t.Errorf("normal: gate accepts +%d after success (unexpected background activity)", added)
		}
		// Fault, then recovery: graded reopening window must still open.
		if err := gate.SetMode(testutil.GateHold); err != nil {
			t.Fatalf("hold: %v", err)
		}
		if _, err := limiter.Allow(ctx, ClassQuery); err == nil {
			t.Fatalf("expected failure under hold")
		}
		if !limiter.Unavailable() {
			t.Errorf("limiter not marked unavailable under hold")
		}
		if err := gate.SetMode(testutil.GatePass); err != nil {
			t.Fatalf("pass: %v", err)
		}
		recoveryStart := time.Now()
		recoveryDeadline := time.Now().Add(30 * time.Second)
		var recoveryErr error
		for {
			_, recoveryErr = limiter.Allow(ctx, ClassQuery)
			if recoveryErr == nil {
				break
			}
			if time.Now().After(recoveryDeadline) {
				t.Fatalf("limiter never recovered within 30s: %v", recoveryErr)
			}
			time.Sleep(100 * time.Millisecond)
		}
		recoveryDur := time.Since(recoveryStart)
		if recoveryDur > 30*time.Second {
			t.Errorf("recovery: first trusted Allow took %v > 30s window", recoveryDur)
		}
		if !limiter.Recovering() {
			t.Errorf("limiter not in graded reopening window after recovery (contract)")
		}
		rAt, rAfter2s, rAdded := observeBackgroundAccepts(gate)
		records = append(records, cteScenarioRecord{
			Scenario: "recovery", Caller: "solo",
			DurationMS: float64(recoveryDur.Microseconds()) / 1000.0, OK: true,
			GateAtReturn: rAt, GateAfter2s: rAfter2s, BackgroundAdd: rAdded,
			DurationBound: "first trusted Allow <= 30s + Recovering()==true",
			BoundPass:     recoveryDur <= 30*time.Second,
			Note:          "graded reopening window still opens (10s, unchanged); background window recorded, not asserted (recovery re-dials legitimately inside the recovery loop)",
		})
		_ = client
	})

	if err := writer.WriteJSON("cte_boundary_records.json", records); err != nil {
		t.Fatalf("write cte_boundary_records: %v", err)
	}
	for _, r := range records {
		t.Logf("%-26s %-9s %8.1fms ok=%-5v bound=%-6v class=%s",
			r.Scenario, r.Caller, r.DurationMS, r.OK, r.BoundPass, r.ErrorCode)
	}
}

// budgetBucketKey is the limiter's token-bucket key for ClassQuery.
const budgetBucketKey = "txharbor:rl:query"

// responseLossRecord is one counterexample/ control-group observation.
type responseLossRecord struct {
	Group             string  `json:"group"` // close_before_expiry | hold_to_expiry | isolated_noretry
	CTE               bool    `json:"cte"`
	MaxRetries        int     `json:"max_retries"`
	DropCloseAfterMS  float64 `json:"drop_close_after_ms"` // <0 = hold to expiry
	AppCalls          int     `json:"app_calls"`
	Sends             uint64  `json:"server_bound_sends"`   // gate EVAL marker count
	ConfirmedExecs    uint64  `json:"confirmed_executions"` // Redis INFO cmdstat_eval delta
	DroppedReplies    uint64  `json:"responses_withheld"`   // gate observed & withheld
	TokensBefore      string  `json:"tokens_before"`
	TokensAfter       string  `json:"tokens_after"`
	TokensSettled     string  `json:"tokens_settled_after_sleep"` // "" = bucket expired (TTL=2*L)
	TokensDeductedEst float64 `json:"tokens_deducted_est"`        // before-after (refill only lowers it)
	DurationMS        float64 `json:"duration_ms"`
	ErrorCode         string  `json:"error_class"`
	ErrorText         string  `json:"error_text,omitempty"`
	ReSendObserved    bool    `json:"resend_observed"` // sends > appCalls
	DoubleDeducted    bool    `json:"double_deducted"` // confirmedExecs > 1
	VerdictNote       string  `json:"verdict_note"`
}

// evalConfirmedExecutions reads cmdstat_eval:calls from the DIRECT backend
// connection (bypassing the gate): the ground truth for "server executed".
func evalConfirmedExecutions(t *testing.T, direct *redis.Client) uint64 {
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

// bucketTokens reads the `tokens` field (floor as Redis stores it) or "".
func bucketTokens(t *testing.T, direct *redis.Client) string {
	t.Helper()
	v, err := direct.HGet(context.Background(), budgetBucketKey, "tokens").Result()
	if err != nil {
		if err == redis.Nil {
			return "" // key expired / not created yet: recorded, not inferred
		}
		t.Fatalf("HGET tokens: %v", err)
	}
	return v
}

// TestRedisBudgetResponseLossCounterexample: confirm the server EXECUTED the
// EVAL (INFO commandstats + bucket tokens), then withhold the response;
// group A closes the client socket 30ms later (BEFORE the 200ms budget
// expires) — testing whether default go-redis retries re-send and double-
// deduct tokens. Group B withholds until budget expiry (control). Group C is
// the candidate mitigation (isolated-style MaxRetries=-1 client) — proving
// the candidate strategy, NOT changing production behavior.
func TestRedisBudgetResponseLossCounterexample(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	redisCtr, err := testutil.StartRedis(ctx)
	if err != nil {
		t.Fatalf("StartRedis: %v", err)
	}
	t.Cleanup(func() { _ = redisCtr.Close(context.Background()) })
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
	t.Logf("response-loss evidence dir: %s", writer.Dir())

	// Direct observation client: bypasses the gate (execution ground truth).
	direct := redis.NewClient(&redis.Options{Addr: redisCtr.HostPort(), ReadTimeout: 5 * time.Second})
	t.Cleanup(func() { _ = direct.Close() })

	// Low-rate bucket so token deduction is observable: refill over each
	// group's <1s window ≤1 token with rate=1/s; burst covers max retries.
	lossClasses := func() map[Class]ClassConfig {
		c := ClassConfig{RatePerSecond: 1, Burst: 10}
		return map[Class]ClassConfig{
			ClassNewWithdrawal: c, ClassWrite: c, ClassQuery: c,
			ClassOperator: c, ClassRPC: c,
		}
	}

	runGroup := func(group string, dropClose time.Duration, maxRetries int) responseLossRecord {
		t.Helper()
		if err := gate.SetMode(testutil.GatePass); err != nil {
			t.Fatalf("pass: %v", err)
		}
		gate.SetDropClose(dropClose)
		limiter, _ := buildCTEBudgetLimiter(t, gate.HostPort(), cteOpts{
			cte: true, maxRetries: maxRetries, classes: lossClasses(),
		})
		// Fresh bucket per group: delete + one warm (Pass-mode) allow so
		// tokensBefore is deterministic and inside the 2*Timeout=400ms TTL.
		if err := direct.Del(context.Background(), budgetBucketKey).Err(); err != nil {
			t.Fatalf("del bucket: %v", err)
		}
		if _, err := limiter.Allow(ctx, ClassQuery); err != nil {
			t.Fatalf("warm allow: %v", err)
		}
		// Flip to Drop AFTER warm: existing pair torn down; measured Allow
		// dials fresh into Drop mode (HELLO passes, EVAL reply withheld).
		if err := gate.SetMode(testutil.GateDrop); err != nil {
			t.Fatalf("drop: %v", err)
		}
		before := gate.CounterSnapshot()
		execBefore := evalConfirmedExecutions(t, direct)
		tokensBefore := bucketTokens(t, direct)

		// Measured: exactly ONE application-layer call.
		start := time.Now()
		_, allowErr := limiter.Allow(ctx, ClassQuery)
		dur := time.Since(start)

		// Read the bucket IMMEDIATELY (inside the script TTL = 2*L = 400ms
		// with the candidate budget — the reading after the settle sleep
		// would miss the expired key; that expiry is recorded separately as
		// TTL-coupling evidence, see Note).
		execAfter := evalConfirmedExecutions(t, direct)
		tokensAfter := bucketTokens(t, direct)

		// Wait past dropClose (30ms) + budget (200ms) so the settle state
		// (retries done, socket closes done) is fully observed.
		time.Sleep(budgetVerifyL + 150*time.Millisecond)
		after := gate.CounterSnapshot()
		tokensSettled := bucketTokens(t, direct)

		deducted := 0.0
		if tokensBefore != "" && tokensAfter != "" {
			if b, perr := strconv.ParseFloat(tokensBefore, 64); perr == nil {
				if a, perr2 := strconv.ParseFloat(tokensAfter, 64); perr2 == nil {
					deducted = b - a // refill only LOWERS this estimate
				}
			}
		}
		sends := after.EvalFrames - before.EvalFrames
		execs := execAfter - execBefore
		rec := responseLossRecord{
			Group: group, CTE: true, MaxRetries: maxRetries,
			// Keep the sign: the hold-to-expiry control group passes -1ns;
			// Microseconds()/1000 would truncate it to 0 (review P2).
			DropCloseAfterMS: float64(dropClose) / float64(time.Millisecond),
			AppCalls:         1, Sends: sends, ConfirmedExecs: execs,
			DroppedReplies: after.DroppedReplies - before.DroppedReplies,
			TokensBefore:   tokensBefore, TokensAfter: tokensAfter,
			TokensSettled:     tokensSettled,
			TokensDeductedEst: deducted,
			DurationMS:        float64(dur.Microseconds()) / 1000.0,
			ErrorCode:         classifyRedisError(allowErr),
			ErrorText:         errorTextFor(allowErr),
			ReSendObserved:    sends > 1,
			DoubleDeducted:    execs > 1,
		}
		if rec.ReSendObserved && rec.DoubleDeducted {
			rec.VerdictNote = "re-send + double deduction: response lost after execution, default retry re-executed (counterexample proven)"
		} else if sends == 1 && execs == 1 {
			rec.VerdictNote = "single send, single confirmed execution (no re-send)"
		} else {
			rec.VerdictNote = fmt.Sprintf("sends=%d execs=%d recorded without verdict (see raw counts)", sends, execs)
		}
		return rec
	}

	var records []responseLossRecord

	// Group A: close BEFORE budget expiry — the counterexample.
	a := runGroup("close_before_expiry", budgetVerifyDropClose, 0) // 0 = library default MaxRetries(3)
	records = append(records, a)
	if a.ConfirmedExecs < 1 {
		t.Errorf("close_before_expiry: confirmedExecs=%d, want >=1 (server DID execute — execution must be confirmed, not inferred)", a.ConfirmedExecs)
	}
	if !a.ReSendObserved {
		t.Errorf("close_before_expiry: sends=%d want >1 (expected default retry to re-send after connection closed before budget expiry; if this fails the re-send assumption must be corrected)", a.Sends)
	}
	if !a.DoubleDeducted {
		t.Errorf("close_before_expiry: confirmedExecs=%d want >1 (default retry re-executed the script — double deduction counterexample not observed; do NOT relax this assertion, investigate)", a.ConfirmedExecs)
	}
	// Token-deduction iron proof: ≥2 deductions minus ≤~0.4 refill (rate=1/s
	// over <0.4s window) ⇒ deducted_est ≥1.5 proves DOUBLE deduction by the
	// server-side bucket (not inferred from the missing reply).
	if a.TokensDeductedEst < 1.5 {
		t.Errorf("close_before_expiry: tokens_deducted=%.2f, want >=1.5 (bucket proof of double deduction; before=%s after=%s)", a.TokensDeductedEst, a.TokensBefore, a.TokensAfter)
	}

	// Group B: hold response to budget expiry — control (no close, read
	// runs to deadline, retry blocked by expired ctx).
	b := runGroup("hold_to_expiry", -1, 0)
	records = append(records, b)
	if b.Sends != 1 || b.ConfirmedExecs != 1 {
		t.Errorf("hold_to_expiry: sends=%d execs=%d, want 1/1 (ctx expiry must block re-send)", b.Sends, b.ConfirmedExecs)
	}
	if b.DroppedReplies < 1 {
		t.Errorf("hold_to_expiry: responses_withheld=%d, want >=1 (the response WAS observed server-side)", b.DroppedReplies)
	}
	// Single deduction proof: 0.5 ≤ deducted < 1.5 (one execution, refill
	// cancels at most ~0.4).
	if b.TokensDeductedEst < 0.5 || b.TokensDeductedEst >= 1.5 {
		t.Errorf("hold_to_expiry: tokens_deducted=%.2f, want in [0.5, 1.5) (exactly one deduction)", b.TokensDeductedEst)
	}

	// Group C: candidate mitigation — MaxRetries disabled on an ISOLATED
	// client (test-side proof of the candidate strategy; production untouched).
	c := runGroup("isolated_noretry", budgetVerifyDropClose, -1)
	records = append(records, c)
	if c.Sends != 1 || c.ConfirmedExecs != 1 {
		t.Errorf("isolated_noretry: sends=%d execs=%d, want 1/1 (MaxRetries=-1 must prevent re-send)", c.Sends, c.ConfirmedExecs)
	}
	if c.TokensDeductedEst < 0.5 || c.TokensDeductedEst >= 1.5 {
		t.Errorf("isolated_noretry: tokens_deducted=%.2f, want in [0.5, 1.5) (exactly one deduction under the candidate strategy)", c.TokensDeductedEst)
	}

	if err := writer.WriteJSON("response_loss_records.json", records); err != nil {
		t.Fatalf("write response_loss_records: %v", err)
	}
	for _, r := range records {
		settledNote := "bucket_alive"
		if r.TokensSettled == "" {
			// TTL coupling observation: bucket TTL = 2*L = 400ms with the
			// candidate budget; idle past it expires the bucket (this is a
			// MEASUREMENT of the TTL coupling the design doc flags, not a
			// failure of this test).
			settledNote = "bucket_expired(TTL=2*L)"
		}
		t.Logf("%-20s sends=%d execs=%d withheld=%d tokens %s→%s settled=%q deduct≈%.1f dur=%.1fms resended=%v doubled=%v %s %s",
			r.Group, r.Sends, r.ConfirmedExecs, r.DroppedReplies,
			r.TokensBefore, r.TokensAfter, r.TokensSettled, r.TokensDeductedEst,
			r.DurationMS, r.ReSendObserved, r.DoubleDeducted, r.ErrorCode, settledNote)
	}
}
