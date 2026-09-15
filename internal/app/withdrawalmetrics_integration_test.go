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
	"strings"
	"testing"

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

// TestWithdrawalMetricsOutcomesFromRealRequests drives every outcome through a
// real handler against real PostgreSQL and asserts the scrape from the
// serve-used registry, so the counters are proven to ride the request path.
func TestWithdrawalMetricsOutcomesFromRealRequests(t *testing.T) {
	ctx, pool, _ := withdrawalHTTPSetup(t)
	m := metrics.New(func() bool { return true })

	key := withdrawalHTTPKey(t, ctx, pool, 8201)
	withdrawalHTTPSupply(t, ctx, pool, 8201, "auth-metrics-1")
	badKey := withdrawalHTTPKey(t, ctx, pool, 8202)
	withdrawalHTTPSupply(t, ctx, pool, 8202, "auth-metrics-2")
	withdrawalHTTPSeedPolicy(t, ctx, pool)

	srv := httptest.NewServer(withdrawalMetricsHandler(pool, m))
	defer srv.Close()

	if status, raw := withdrawalHTTPDo(t, http.MethodPost, srv.URL+"/withdrawals", key,
		withdrawalHTTPCreateBody(t, "idem-metrics-1", withdrawalHTTPAmount, "auth-metrics-1")); status != http.StatusCreated {
		t.Fatalf("create status = %d (%s), want 201", status, raw)
	}
	if status, raw := withdrawalHTTPDo(t, http.MethodPost, srv.URL+"/withdrawals", key,
		withdrawalHTTPCreateBody(t, "idem-metrics-1", withdrawalHTTPAmount, "auth-metrics-1")); status != http.StatusOK {
		t.Fatalf("replay status = %d (%s), want 200", status, raw)
	}
	if status, raw := withdrawalHTTPDo(t, http.MethodPost, srv.URL+"/withdrawals", key,
		withdrawalHTTPCreateBody(t, "idem-metrics-1", "101", "auth-metrics-1")); status != http.StatusConflict {
		t.Fatalf("conflict status = %d (%s), want 409", status, raw)
	}
	if status, raw := withdrawalHTTPDo(t, http.MethodPost, srv.URL+"/withdrawals", badKey,
		withdrawalHTTPCreateBody(t, "idem-metrics-bad", "1.5", "auth-metrics-2")); status != http.StatusUnprocessableEntity {
		t.Fatalf("bad-param status = %d (%s), want 422", status, raw)
	}
	if status, raw := withdrawalHTTPDo(t, http.MethodPost, srv.URL+"/withdrawals", "",
		withdrawalHTTPCreateBody(t, "idem-metrics-401", withdrawalHTTPAmount, "auth-metrics-1")); status != http.StatusUnauthorized {
		t.Fatalf("no-key status = %d (%s), want 401", status, raw)
	}

	body := withdrawalMetricsScrape(t, m)
	want := []string{
		"txharbor_withdrawal_accepted_total 1",
		"txharbor_withdrawal_replayed_total 1",
		"txharbor_withdrawal_conflict_total 1",
		"txharbor_withdrawal_rejected_total 1",
		"txharbor_withdrawal_unauthenticated_total 1",
	}
	for _, line := range want {
		if !strings.Contains(body, line+"\n") {
			t.Errorf("scrape missing %q\n%s", line, body)
		}
	}
	// Names must match the exported consts the production wiring uses.
	for _, name := range []string{
		metrics.WithdrawalAcceptedMetricName,
		metrics.WithdrawalReplayedMetricName,
		metrics.WithdrawalConflictMetricName,
		metrics.WithdrawalRejectedMetricName,
		metrics.WithdrawalUnauthenticatedMetricName,
	} {
		if !strings.Contains(body, name) {
			t.Errorf("scrape missing metric name %q", name)
		}
	}
}
