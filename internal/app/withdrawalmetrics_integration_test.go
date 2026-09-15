//go:build integration

// withdrawalmetrics_integration_test.go owns the T015 proof that real HTTP
// requests through the serve-mounted handler move the REAL scraped registry
// (not isolated counter objects): the serve-used *metrics.Metrics is injected
// into WithdrawalHandler, and the exposition text rendered by the very
// m.Handler() the serve mux mounts at /metrics shows the outcome counters
// (FR-20/FR-21).
package app

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/metrics"
)

// withdrawalMetricsHandler is the T014 wiring plus the serve-used registry
// pointer that serve.go passes into construction.
func withdrawalMetricsHandler(pool *pgxpool.Pool, m *metrics.Metrics) *WithdrawalHandler {
	return &WithdrawalHandler{
		Pool:    pool,
		ChainID: withdrawalHTTPChainID,
		Metrics: m,
	}
}

// withdrawalMetricsScrape renders the exposition text exactly as serve does:
// through m.Handler(), the handler health.NewServer mounts at /metrics.
func withdrawalMetricsScrape(t *testing.T, m *metrics.Metrics) string {
	t.Helper()
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("scrape status = %d, want 200", rec.Code)
	}
	return rec.Body.String()
}

// withdrawalMetricsOutcomeNames is every label-free 007 outcome series the
// production registry exposes. There are six series but eight HTTP outcomes:
// 400 malformed, 403 permission and 422 validation deliberately share the
// `rejected` series (metrics.go ObserveWithdrawalStatus), so this test watches
// all six names rather than inventing per-status counters.
var withdrawalMetricsOutcomeNames = []string{
	metrics.WithdrawalAcceptedMetricName,
	metrics.WithdrawalReplayedMetricName,
	metrics.WithdrawalConflictMetricName,
	metrics.WithdrawalRejectedMetricName,
	metrics.WithdrawalUnavailableMetricName,
	metrics.WithdrawalUnauthenticatedMetricName,
}

// withdrawalMetricsSeries parses one exposition scrape into a name->value map
// for the six 007 outcome counters. An absent series reads as 0: a label-free
// counter has no series until its first observation (metrics.go).
func withdrawalMetricsSeries(t *testing.T, m *metrics.Metrics) map[string]float64 {
	t.Helper()
	series := make(map[string]float64, len(withdrawalMetricsOutcomeNames))
	for _, name := range withdrawalMetricsOutcomeNames {
		series[name] = 0
	}
	for _, line := range strings.Split(withdrawalMetricsScrape(t, m), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		if _, ok := series[fields[0]]; !ok {
			continue
		}
		v, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			t.Fatalf("parse %q value %q: %v", fields[0], fields[1], err)
		}
		series[fields[0]] = v
	}
	return series
}

// withdrawalMetricsAssertExactlyOnce fires one real attempt and proves the
// serve-used registry moved ONLY target, by EXACTLY one. "Exactly once" here is
// per-attempt counter accounting: one request lands one bump on its outcome
// series, and the post-response audit write adds none. It is explicitly NOT a
// global exactly-once guarantee across crash or network retry — a client that
// retries after a lost response creates a second attempt and therefore a second
// bump, by design.
func withdrawalMetricsAssertExactlyOnce(t *testing.T, m *metrics.Metrics, target string, fire func()) {
	t.Helper()
	before := withdrawalMetricsSeries(t, m)
	fire()
	// The response is flushed before observeWithdrawal runs, so the client can
	// return while the bump is still a beat behind: poll until it lands.
	var after map[string]float64
	deadline := time.Now().Add(5 * time.Second)
	for {
		after = withdrawalMetricsSeries(t, m)
		if after[target] != before[target] {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("series %s never moved after its attempt", target)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if delta := after[target] - before[target]; delta != 1 {
		t.Errorf("series %s bumped by %v for one attempt, want exactly 1", target, delta)
	}
	for _, name := range withdrawalMetricsOutcomeNames {
		if name == target {
			continue
		}
		if after[name] != before[name] {
			t.Errorf("series %s moved %v -> %v during a %s attempt; want unchanged",
				name, before[name], after[name], target)
		}
	}
}

// TestWithdrawalMetricsOutcomesFromRealRequests drives ALL EIGHT create
// outcomes — 201 accepted, 200 replayed, 409 conflict, 422 rejected, 401
// unauthenticated, 403 permission-rejected, 503 storage/policy-unavailable and
// 400 malformed — through a real handler against real PostgreSQL, and asserts
// against the scrape rendered by the serve-used registry. Each attempt is
// checked as a delta: its own series increments by exactly one and every other
// series stays put, so a missing bump and a double bump (the post-response
// audit write must not add a second one) are both caught. The scope of
// "exactly once" is the per-attempt counter bump proven here, not global
// exactly-once under crash or network retry.
func TestWithdrawalMetricsOutcomesFromRealRequests(t *testing.T) {
	ctx, pool, _ := withdrawalHTTPSetup(t)
	m := metrics.New(func() bool { return true })

	key := withdrawalHTTPKey(t, ctx, pool, 8201)
	withdrawalHTTPSupply(t, ctx, pool, 8201, "auth-metrics-1")
	badKey := withdrawalHTTPKey(t, ctx, pool, 8202)
	withdrawalHTTPSupply(t, ctx, pool, 8202, "auth-metrics-2")
	denyKey := withdrawalHTTPKey(t, ctx, pool, 8203)
	withdrawalHTTPSupply(t, ctx, pool, 8203, "auth-metrics-3")
	coldKey := withdrawalHTTPKey(t, ctx, pool, 8204)
	withdrawalHTTPSupply(t, ctx, pool, 8204, "auth-metrics-4")

	srv := httptest.NewServer(withdrawalMetricsHandler(pool, m))
	defer srv.Close()

	// 503 unavailable: no policy row exists yet, so the authenticated first
	// create fails closed and retryably (T020). Fired before the policy lands.
	withdrawalMetricsAssertExactlyOnce(t, m, metrics.WithdrawalUnavailableMetricName, func() {
		if status, raw := withdrawalHTTPDo(t, http.MethodPost, srv.URL+"/withdrawals", coldKey,
			withdrawalHTTPCreateBody(t, "idem-metrics-cold", withdrawalHTTPAmount, "auth-metrics-4")); status != http.StatusServiceUnavailable {
			t.Fatalf("cold-start status = %d (%s), want 503", status, raw)
		}
	})

	withdrawalHTTPSeedPolicy(t, ctx, pool)

	// 400 malformed: handler-only decode rejection, no DB (contracts §1).
	withdrawalMetricsAssertExactlyOnce(t, m, metrics.WithdrawalRejectedMetricName, func() {
		if status, raw := withdrawalHTTPDo(t, http.MethodPost, srv.URL+"/withdrawals", key, "{"); status != http.StatusBadRequest {
			t.Fatalf("malformed status = %d (%s), want 400", status, raw)
		}
	})

	// 401 unauthenticated: no verifiable credential (contracts §2).
	withdrawalMetricsAssertExactlyOnce(t, m, metrics.WithdrawalUnauthenticatedMetricName, func() {
		if status, raw := withdrawalHTTPDo(t, http.MethodPost, srv.URL+"/withdrawals", "",
			withdrawalHTTPCreateBody(t, "idem-metrics-401", withdrawalHTTPAmount, "auth-metrics-1")); status != http.StatusUnauthorized {
			t.Fatalf("no-key status = %d (%s), want 401", status, raw)
		}
	})

	// 403 rejected: a valid key whose caller lost create permission.
	if _, err := pool.Exec(ctx, `UPDATE caller SET can_create = false WHERE caller_id = $1`, 8203); err != nil {
		t.Fatalf("drop create permission: %v", err)
	}
	withdrawalMetricsAssertExactlyOnce(t, m, metrics.WithdrawalRejectedMetricName, func() {
		if status, raw := withdrawalHTTPDo(t, http.MethodPost, srv.URL+"/withdrawals", denyKey,
			withdrawalHTTPCreateBody(t, "idem-metrics-403", withdrawalHTTPAmount, "auth-metrics-3")); status != http.StatusForbidden {
			t.Fatalf("no-permission status = %d (%s), want 403", status, raw)
		}
	})

	// 422 rejected: shape-valid body carrying an invalid amount.
	withdrawalMetricsAssertExactlyOnce(t, m, metrics.WithdrawalRejectedMetricName, func() {
		if status, raw := withdrawalHTTPDo(t, http.MethodPost, srv.URL+"/withdrawals", badKey,
			withdrawalHTTPCreateBody(t, "idem-metrics-bad", "1.5", "auth-metrics-2")); status != http.StatusUnprocessableEntity {
			t.Fatalf("bad-param status = %d (%s), want 422", status, raw)
		}
	})

	// 201 accepted: first create persists one request row.
	withdrawalMetricsAssertExactlyOnce(t, m, metrics.WithdrawalAcceptedMetricName, func() {
		if status, raw := withdrawalHTTPDo(t, http.MethodPost, srv.URL+"/withdrawals", key,
			withdrawalHTTPCreateBody(t, "idem-metrics-1", withdrawalHTTPAmount, "auth-metrics-1")); status != http.StatusCreated {
			t.Fatalf("create status = %d (%s), want 201", status, raw)
		}
	})

	// 200 replayed: same key and parameters return the stored row.
	withdrawalMetricsAssertExactlyOnce(t, m, metrics.WithdrawalReplayedMetricName, func() {
		if status, raw := withdrawalHTTPDo(t, http.MethodPost, srv.URL+"/withdrawals", key,
			withdrawalHTTPCreateBody(t, "idem-metrics-1", withdrawalHTTPAmount, "auth-metrics-1")); status != http.StatusOK {
			t.Fatalf("replay status = %d (%s), want 200", status, raw)
		}
	})

	// 409 conflict: same key, different parameters.
	withdrawalMetricsAssertExactlyOnce(t, m, metrics.WithdrawalConflictMetricName, func() {
		if status, raw := withdrawalHTTPDo(t, http.MethodPost, srv.URL+"/withdrawals", key,
			withdrawalHTTPCreateBody(t, "idem-metrics-1", "101", "auth-metrics-1")); status != http.StatusConflict {
			t.Fatalf("conflict status = %d (%s), want 409", status, raw)
		}
	})

	// Final settle: after every attempt has landed, the scraped totals must be
	// the per-outcome sum, so a delayed second bump cannot slip past a delta.
	// The audit writes are still allowed to be in flight; they add no counter.
	time.Sleep(200 * time.Millisecond)
	final := withdrawalMetricsSeries(t, m)
	want := map[string]float64{
		metrics.WithdrawalAcceptedMetricName:        1,
		metrics.WithdrawalReplayedMetricName:        1,
		metrics.WithdrawalConflictMetricName:        1,
		metrics.WithdrawalRejectedMetricName:        3, // 400 + 403 + 422 share this series
		metrics.WithdrawalUnavailableMetricName:     1,
		metrics.WithdrawalUnauthenticatedMetricName: 1,
	}
	for _, name := range withdrawalMetricsOutcomeNames {
		if final[name] != want[name] {
			t.Errorf("final series %s = %v, want %v", name, final[name], want[name])
		}
	}

	// Names must match the exported consts the production wiring uses.
	body := withdrawalMetricsScrape(t, m)
	for _, name := range withdrawalMetricsOutcomeNames {
		if !strings.Contains(body, name) {
			t.Errorf("scrape missing metric name %q", name)
		}
	}
}
