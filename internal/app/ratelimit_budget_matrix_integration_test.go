//go:build e2e

// ratelimit_budget_matrix_integration_test.go is the 013 decision-budget
// measurement matrix (design §5/§7): caller-visible latency and outcome shape
// for the OLD limiter assembly versus the dedicated limiter client at the
// inherited 1s default and at the explicit 200ms/300ms candidates, each under
// a normal Redis and under a 400ms mid-congestion EVAL reply delay
// (testutil.RedisGate GateDelay).
//
// This is a MEASUREMENT, not a threshold approval: the latency and
// rejection-rate columns are recorded raw into matrix_records.json and are
// never asserted against a limit. Only the deterministic shape assertions
// below can fail:
//
//   - every admitted (2xx) key left exactly one withdrawal_requests row, and
//     every refused key left zero (no new payment intent);
//   - normal: every assembly admits every request;
//   - congestion at the 1s assemblies: every request is admitted and takes
//     >=350ms (the injected delay is observed, no budget clip);
//   - congestion at the 200/300ms candidates: every request is refused with
//     the PD-1 503 (the (L,1s) middle-band false rejection), each within
//     L+150ms, carrying the retryable taxonomy code.
//
// No Anvil/Kafka is needed: the real 007 handler runs over a real PostgreSQL
// and Redis behind the real guardRoute middleware stack; the gate fronts
// Redis for the limiter's Redis client only.
package app

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/health"
	"github.com/xtianxx/txharbor/internal/ratelimit"
	"github.com/xtianxx/txharbor/internal/testutil"
	"github.com/xtianxx/txharbor/internal/withdrawal"
)

const (
	// mtxCallerID is a fresh caller (6701 belongs to the T067 acceptance).
	mtxCallerID = int64(6801)
	mtxChainID  = int64(31337)
	// All-lowercase canonical addresses: the EIP-55 checksum test does not
	// apply, and stored rows compare by equality after canonicalization.
	mtxAsset     = "0x4444444444444444444444444444444444444444"
	mtxWatch     = "0x5555555555555555555555555555555555555555"
	mtxRecipient = "0x6666666666666666666666666666666666666666"
	mtxAmount    = "1"

	// mtxRequests is the per-(cell, scenario) sample count.
	mtxRequests = 32

	// mtxRateBurst is the bench-measured per-class rate/burst for all five
	// classes: 32 requests never approach the local ceiling.
	mtxRateBurst = "500/100"

	// mtxCongestionDelay is the injected backend->client reply delay: above
	// both candidate budgets (200/300ms) and inside the 1s socket read
	// timeout, so the candidates clip and the 1s assemblies do not.
	mtxCongestionDelay = 400 * time.Millisecond

	// mtxDelayObservedFloor is the congestion lower bound for the 1s
	// assemblies: the injected 400ms delay must be observable in the
	// caller-visible duration.
	mtxDelayObservedFloor = 350 * time.Millisecond

	// mtxBudgetSlack bounds the caller-visible overshoot around a clipped
	// budget (design §7 epsilon discipline: scheduling jitter only).
	mtxBudgetSlack = 150 * time.Millisecond

	// mtxGateSettle lets the FINs from a gate mode flip reach the pooled
	// client sockets before the measured window opens; the pool's own
	// connCheck would discard a dead idle connection anyway, this only keeps
	// the first sample off that race.
	mtxGateSettle = 50 * time.Millisecond
)

// mtxScenarios are the two gate postures every cell is measured under.
var mtxScenarios = []string{"normal", "congestion"}

// mtxCell is one assembly/budget candidate.
type mtxCell struct {
	name string
	// budget is the TXHARBOR_RATELIMIT_BUDGET value; "" = unset (inherits
	// the effective cfg.Redis.Timeout).
	budget string
	// oldAssembly selects the pre-013 client shape: no
	// ContextTimeoutEnabled, library-default retries.
	oldAssembly bool
}

// mtxRecord is one (cell, scenario) measurement record exported as evidence.
type mtxRecord struct {
	Cell           string         `json:"cell"`
	Budget         string         `json:"budget"`
	Scenario       string         `json:"scenario"`
	N              int            `json:"n"`
	Statuses       map[string]int `json:"statuses"`
	Admitted2xx    int            `json:"admitted_2xx"`
	Refused503     int            `json:"refused_503"`
	Other          int            `json:"other"`
	Unavailable    int            `json:"unavailable"` // 503s carrying the retryable taxonomy code
	P50MS          float64        `json:"p50_ms"`
	P95MS          float64        `json:"p95_ms"`
	P99MS          float64        `json:"p99_ms"`
	MaxMS          float64        `json:"max_ms"`
	MeanMS         float64        `json:"mean_ms"`
	AcceptedRows1  int            `json:"accepted_keys_with_one_row"`
	RefusedRows0   int            `json:"refused_keys_with_zero_rows"`
	GateDelayed    uint64         `json:"gate_delayed_replies"`
	GateEvalFrames uint64         `json:"gate_eval_frames"`
}

// TestRatelimitBudgetMeasurementMatrix measures the four assemblies: the old
// client shape at the inherited 1s budget, the dedicated limiter client at
// the inherited 1s default, and the dedicated client at the explicit
// 200ms/300ms candidates — each under a normal Redis and under a 400ms reply
// delay. Raw numbers go to matrix_records.json; only the shape assertions in
// mtxRunScenario can fail.
func TestRatelimitBudgetMeasurementMatrix(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dsn := e2eStartPostgres(t)
	pool := e2eOpenPool(t, dsn)
	redisCtr := e2eStartRedis(t)

	// The gate fronts the Redis container for the limiter client only; its
	// backend address is fixed for the gate's lifetime, so every cell keeps
	// one address across the mode flips.
	gate, err := testutil.NewRedisGate(redisCtr.HostPort())
	if err != nil {
		t.Fatalf("NewRedisGate: %v", err)
	}
	t.Cleanup(func() { _ = gate.Close() })

	// Base fixture (the seedT067 pattern at a fresh caller id) plus one
	// single-use grant per measured key. The receipt path binds a grant once
	// (authorization_id is UNIQUE) and checks the full FR-10 parameter set,
	// so a request that leaked past the middleware would land a real receipt
	// instead of a 403: the refusal-cell 0-row assertion stays meaningful.
	seedT067(t, ctx, pool, mtxCallerID, mtxAsset, mtxWatch, "authz-mtx-base", mtxRecipient, mtxAmount)
	apiKey, _, err := withdrawal.IssueKey(ctx, pool, mtxCallerID, "mtx")
	if err != nil {
		t.Fatalf("IssueKey: %v", err)
	}

	cells := []mtxCell{
		// A: the pre-013 assembly shape; budget unset (inherits 1s).
		{name: "old_assembly", oldAssembly: true},
		// B: the dedicated client; budget unset (inherited 1s default).
		{name: "dedicated_default_1s"},
		// C/D: the dedicated client with the explicit candidate budgets.
		{name: "dedicated_candidate_200ms", budget: "200ms"},
		{name: "dedicated_candidate_300ms", budget: "300ms"},
	}
	for _, cell := range cells {
		for _, scenario := range mtxScenarios {
			for i := range mtxRequests {
				mtxSeedGrant(t, ctx, pool, mtxAuthorizationID(cell.name, scenario, i))
			}
		}
	}

	handler := &WithdrawalHandler{Pool: pool, ChainID: mtxChainID}
	writer, err := testutil.NewEvidenceWriter()
	if err != nil {
		t.Fatalf("NewEvidenceWriter: %v", err)
	}
	defer func() { _ = writer.Close() }()

	t.Logf("matrix evidence dir: %s", writer.Dir())
	t.Logf("%-28s %-7s %-10s %4s %5s %5s %5s %8s %8s %8s %8s %8s",
		"cell", "budget", "scenario", "n", "2xx", "503", "other", "p50_ms", "p95_ms", "max_ms", "unavail", "delayed")

	records := make([]mtxRecord, 0, len(cells)*len(mtxScenarios))
	for _, cell := range cells {
		cfg := mtxLoadConfig(t, dsn, gate.HostPort(), cell.budget)
		srv, closeCell := mtxBuildCell(t, cfg, cell, handler)
		for _, scenario := range mtxScenarios {
			rec := mtxRunScenario(t, ctx, pool, srv, apiKey, cell, cfg, scenario, gate)
			records = append(records, rec)
			t.Logf("%-28s %-7s %-10s %4d %5d %5d %5d %8.1f %8.1f %8.1f %8d %8d",
				rec.Cell, rec.Budget, rec.Scenario, rec.N,
				rec.Admitted2xx, rec.Refused503, rec.Other,
				rec.P50MS, rec.P95MS, rec.MaxMS, rec.Unavailable, rec.GateDelayed)
		}
		closeCell()
	}

	if err := writer.WriteJSON("matrix_records.json", records); err != nil {
		t.Fatalf("write matrix_records.json: %v", err)
	}
	t.Logf("matrix_records.json: %d records written", len(records))
}

// mtxLoadConfig loads one cell's configuration: the fixture Redis timeout is
// pinned at 1s, every class gets the bench-measured 500/100 rate/burst, and
// the budget key is set only for the explicit candidates (A/B inherit the 1s
// effective Redis timeout).
func mtxLoadConfig(t *testing.T, dsn, redisAddr, budget string) *config.Config {
	t.Helper()
	env := e2eBaseEnv(dsn, "http://127.0.0.1:1", e2eFreeAddr(t), []string{"127.0.0.1:9092"})
	env[config.EnvRedisAddr] = redisAddr
	env[config.EnvRedisTimeout] = "1s"
	env[config.EnvRateLimitNewWithdrawal] = mtxRateBurst
	env[config.EnvRateLimitWrite] = mtxRateBurst
	env[config.EnvRateLimitQuery] = mtxRateBurst
	env[config.EnvRateLimitOperator] = mtxRateBurst
	env[config.EnvRateLimitRPC] = mtxRateBurst
	if budget != "" {
		env[config.EnvRateLimitBudget] = budget
	}
	cfg, err := config.Load(e2eGetenv(env))
	if err != nil {
		t.Fatalf("config.Load (budget %q): %v", budget, err)
	}
	return cfg
}

// mtxBuildCell builds one cell: a fresh Redis client (old shared-shaped
// assembly or the dedicated limiter client), a fresh limiter and policy over
// it, the real guardRoute middleware and an httptest server. The returned
// close function tears the cell down before the next gate flip.
func mtxBuildCell(t *testing.T, cfg *config.Config, cell mtxCell, handler http.Handler) (*httptest.Server, func()) {
	t.Helper()
	var client *redis.Client
	if cell.oldAssembly {
		client = redis.NewClient(&redis.Options{
			Addr:         cfg.Redis.Addr,
			DialTimeout:  cfg.Redis.Timeout,
			ReadTimeout:  cfg.Redis.Timeout,
			WriteTimeout: cfg.Redis.Timeout,
		})
	} else {
		client = newLimiterRedisClient(cfg)
	}
	store, err := ratelimit.NewRedisScriptStore(client)
	if err != nil {
		_ = client.Close()
		t.Fatalf("%s: NewRedisScriptStore: %v", cell.name, err)
	}
	limiter, err := buildLimiter(store, cfg, nil)
	if err != nil {
		_ = client.Close()
		t.Fatalf("%s: buildLimiter: %v", cell.name, err)
	}
	policy, err := ratelimit.NewPolicy(limiter)
	if err != nil {
		_ = client.Close()
		t.Fatalf("%s: NewPolicy: %v", cell.name, err)
	}
	degradation := newDegradationState(true, health.NewDependencySignals(), "redis")
	mux := http.NewServeMux()
	mux.Handle("/withdrawals", guardRoute(policy, degradation, ratelimit.ClassNewWithdrawal, ratelimit.ClassQuery, handler))
	srv := httptest.NewServer(mux)
	return srv, func() {
		srv.Close()
		_ = client.Close()
	}
}

// mtxRunScenario opens one measurement window: it flips the gate into the
// scenario posture, issues mtxRequests POST /withdrawals with unique
// idempotency keys, checks the deterministic shape and row invariants, and
// returns the raw record. Latency and rate columns are recorded, never
// thresholded.
func mtxRunScenario(t *testing.T, ctx context.Context, pool *pgxpool.Pool, srv *httptest.Server, apiKey string, cell mtxCell, cfg *config.Config, scenario string, gate *testutil.RedisGate) mtxRecord {
	t.Helper()

	// Posture: normal = healthy pass-through; congestion = every post-EVAL
	// backend reply chunk delayed 400ms (Redis alive but slow: the (L,1s)
	// middle band).
	if scenario == "congestion" {
		gate.SetDelay(mtxCongestionDelay)
		if err := gate.SetMode(testutil.GateDelay); err != nil {
			t.Fatalf("%s/%s: gate delay: %v", cell.name, scenario, err)
		}
	} else {
		gate.SetDelay(0)
		if err := gate.SetMode(testutil.GatePass); err != nil {
			t.Fatalf("%s/%s: gate pass: %v", cell.name, scenario, err)
		}
	}
	time.Sleep(mtxGateSettle)

	before := gate.CounterSnapshot()
	samples := make([]testutil.LatencySample, 0, mtxRequests)
	statuses := make(map[string]int)
	statusList := make([]int, 0, mtxRequests)
	keys := make([]string, 0, mtxRequests)
	unavailable := 0
	for i := range mtxRequests {
		key := mtxKey(cell.name, scenario, i)
		body := t067CreateBody(t, key, mtxAsset, mtxRecipient, mtxAmount, mtxAuthorizationID(cell.name, scenario, i))
		start := time.Now()
		status, raw := e2eJSONDo(t, http.MethodPost, srv.URL+"/withdrawals", apiKey, body)
		dur := time.Since(start)
		keys = append(keys, key)
		statusList = append(statusList, status)
		statuses[strconv.Itoa(status)]++
		sample := testutil.LatencySample{
			Variant:    cell.name,
			Shape:      scenario,
			Round:      1,
			Caller:     int(mtxCallerID),
			DurationMS: float64(dur) / float64(time.Millisecond),
			OK:         status == http.StatusCreated || status == http.StatusOK,
			At:         time.Now(),
		}
		switch status {
		case http.StatusCreated, http.StatusOK:
			// Admitted: the row invariant is checked below.
		case http.StatusServiceUnavailable:
			sample.ErrorClass = "http_503"
			// The 503 must be the PD-1 retryable shape, not a coincidental
			// availability failure from another gate.
			if !bytes.Contains(raw, []byte(`"code":"temporarily_unavailable"`)) {
				t.Errorf("%s/%s key %s: 503 without the retryable taxonomy code: %s", cell.name, scenario, key, raw)
			} else {
				unavailable++
			}
		default:
			sample.ErrorClass = "http_" + strconv.Itoa(status)
		}
		samples = append(samples, sample)
	}
	after := gate.CounterSnapshot()

	// Row invariants: an admitted key has exactly one receipt; a refused key
	// has none (no new payment intent).
	rows := mtxRowCounts(t, ctx, pool, keys)
	acceptedRows, refusedRows := 0, 0
	for i, key := range keys {
		count := rows[key]
		switch statusList[i] {
		case http.StatusCreated, http.StatusOK:
			if count != 1 {
				t.Errorf("%s/%s key %s: %d withdrawal_requests rows after an admitted request, want exactly 1", cell.name, scenario, key, count)
			} else {
				acceptedRows++
			}
		case http.StatusServiceUnavailable:
			if count != 0 {
				t.Errorf("%s/%s key %s: %d withdrawal_requests rows after a refusal, want 0", cell.name, scenario, key, count)
			} else {
				refusedRows++
			}
		}
	}

	// Shape assertions (deterministic; the measurement columns above are
	// never thresholded).
	switch scenario {
	case "normal":
		for i, status := range statusList {
			if status != http.StatusCreated && status != http.StatusOK {
				t.Errorf("%s/normal request %d: status %d, want 2xx", cell.name, i, status)
			}
		}
	case "congestion":
		if cell.budget == "" {
			// 1s assemblies (old and dedicated default): the injected delay
			// is observed, never clipped — every request is admitted.
			for i, status := range statusList {
				if status != http.StatusCreated && status != http.StatusOK {
					t.Errorf("%s/congestion request %d: status %d, want 2xx (no budget clip at 1s)", cell.name, i, status)
				}
			}
			floorMS := float64(mtxDelayObservedFloor) / float64(time.Millisecond)
			for i, s := range samples {
				if s.DurationMS < floorMS {
					t.Errorf("%s/congestion request %d: %.1fms < %v — the injected delay was not observed", cell.name, i, s.DurationMS, mtxDelayObservedFloor)
				}
			}
		} else {
			// Candidate budgets below the injected delay: the decision clips
			// at L and every request takes the PD-1 503 — the (L,1s)
			// mid-congestion false rejection this design measures.
			limit := cfg.RateLimit.Budget
			boundMS := float64(limit+mtxBudgetSlack) / float64(time.Millisecond)
			for i, status := range statusList {
				if status != http.StatusServiceUnavailable {
					t.Errorf("%s/congestion request %d: status %d, want 503 (budget clip)", cell.name, i, status)
				}
			}
			for i, s := range samples {
				if s.DurationMS > boundMS {
					t.Errorf("%s/congestion request %d: %.1fms > L+%v=%v — clip bound violated", cell.name, i, s.DurationMS, mtxBudgetSlack, limit+mtxBudgetSlack)
				}
			}
		}
	}

	stats := testutil.ComputeLatencyStats(samples)
	admitted := statuses[strconv.Itoa(http.StatusCreated)] + statuses[strconv.Itoa(http.StatusOK)]
	refused := statuses[strconv.Itoa(http.StatusServiceUnavailable)]
	return mtxRecord{
		Cell:           cell.name,
		Budget:         cfg.RateLimit.Budget.String(),
		Scenario:       scenario,
		N:              stats.Count,
		Statuses:       statuses,
		Admitted2xx:    admitted,
		Refused503:     refused,
		Other:          stats.Count - admitted - refused,
		Unavailable:    unavailable,
		P50MS:          stats.P50MS,
		P95MS:          stats.P95MS,
		P99MS:          stats.P99MS,
		MaxMS:          stats.MaxMS,
		MeanMS:         stats.MeanMS,
		AcceptedRows1:  acceptedRows,
		RefusedRows0:   refusedRows,
		GateDelayed:    after.DelayedReplies - before.DelayedReplies,
		GateEvalFrames: after.EvalFrames - before.EvalFrames,
	}
}

// mtxSeedGrant supplies one single-use authorization grant bound to the
// fixture's exact request parameters, so every measured key can be admitted
// once by the real receipt path.
func mtxSeedGrant(t *testing.T, ctx context.Context, pool *pgxpool.Pool, authorizationID string) {
	t.Helper()
	if _, err := withdrawal.SupplyGrant(ctx, pool, withdrawal.OpInput{
		OperationID:     "op-" + authorizationID,
		Action:          "supply",
		AuthorizationID: authorizationID,
		CallerID:        mtxCallerID,
		ChainID:         mtxChainID,
		Asset:           mtxAsset,
		Recipient:       mtxRecipient,
		Amount:          mtxAmount,
	}, "mtx", "measurement matrix fixture"); err != nil {
		t.Fatalf("SupplyGrant %s: %v", authorizationID, err)
	}
}

// mtxRowCounts reads one withdrawal_requests count per measured key for the
// fixture caller.
func mtxRowCounts(t *testing.T, ctx context.Context, pool *pgxpool.Pool, keys []string) map[string]int64 {
	t.Helper()
	rows, err := pool.Query(ctx,
		`SELECT idempotency_key, count(*) FROM withdrawal_requests
		 WHERE caller_id = $1 AND idempotency_key = ANY($2::text[])
		 GROUP BY idempotency_key`,
		mtxCallerID, keys)
	if err != nil {
		t.Fatalf("count withdrawal_requests: %v", err)
	}
	defer rows.Close()
	counts := make(map[string]int64, len(keys))
	for rows.Next() {
		var key string
		var n int64
		if err := rows.Scan(&key, &n); err != nil {
			t.Fatalf("scan withdrawal_requests count: %v", err)
		}
		counts[key] = n
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read withdrawal_requests counts: %v", err)
	}
	return counts
}

// mtxKey is one measured idempotency key: mtx-<cell>-<scenario>-<i>.
func mtxKey(cell, scenario string, i int) string {
	return fmt.Sprintf("mtx-%s-%s-%d", cell, scenario, i)
}

// mtxAuthorizationID binds the matching grant for one measured key.
func mtxAuthorizationID(cell, scenario string, i int) string {
	return fmt.Sprintf("authz-mtx-%s-%s-%d", cell, scenario, i)
}
