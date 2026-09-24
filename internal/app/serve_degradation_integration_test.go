//go:build integration

// serve_degradation_integration_test.go is the T018 evidence (Integration-PG):
// with a dependency signal down, the non-critical degradation state is
// annotated on real serve routes and exposed on GET /status/degradation while
// the critical handlers still run unchanged (0 blocking, 0 gate reads).
// Redis/Kafka availability is injected through the signal set — the point is
// the wiring, not a container restart (that fault cycle belongs to the B9
// drills and to T065/T066 at the component layer).
package app

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"

	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/health"
	"github.com/xtianxx/txharbor/internal/ratelimit"
)

func TestServeDegradationAnnotationAndStatus(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()
	pgCtr := startPostgresContainer(t)
	dsn := postgresDSN(t, pgCtr)
	if err := db.MigrateUp(ctx, db.MigrateOptions{DSN: dsn, LockTimeout: 10 * time.Second, ConnectTimeout: 10 * time.Second}, io.Discard); err != nil {
		t.Fatalf("migrate up: %v", err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)

	// The dependency set is down (Redis unavailable): the degradation state
	// must report it without touching readiness or funding gates.
	signals := health.NewDependencySignals()
	signals.Set("redis", false)
	degradation := newDegradationState(true, signals, "redis")

	withdrawH := &WithdrawalHandler{Pool: pool, ChainID: 31337}
	mux := http.NewServeMux()
	mux.Handle("/withdrawals", guardRoute(degradation.policy, degradation, ratelimit.ClassNewWithdrawal, ratelimit.ClassQuery, withdrawH))
	mux.Handle("/withdrawals/", guardRoute(degradation.policy, degradation, ratelimit.ClassNewWithdrawal, ratelimit.ClassQuery, withdrawH))
	mux.Handle("GET /status/degradation", &degradationStatusHandler{state: degradation, pool: pool})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	// 1. The status surface reports the degraded non-critical state with the
	// annotation fields present.
	resp, err := http.Get(srv.URL + "/status/degradation")
	if err != nil {
		t.Fatalf("GET /status/degradation: %v", err)
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status endpoint = %d body=%s", resp.StatusCode, body)
	}
	var status struct {
		Status              string            `json:"status"`
		NonCriticalDegraded bool              `json:"non_critical_degraded"`
		Dependencies        map[string]string `json:"dependencies"`
		EventDelivery       string            `json:"event_delivery"`
		Note                string            `json:"note"`
	}
	if err := json.Unmarshal(body, &status); err != nil {
		t.Fatalf("decode status body: %v", err)
	}
	if !status.NonCriticalDegraded || status.Status != "degraded" {
		t.Fatalf("status = %+v, want degraded", status)
	}
	if status.Dependencies["redis"] != "down" {
		t.Fatalf("dependencies.redis = %q, want down", status.Dependencies["redis"])
	}
	if status.EventDelivery == "" || status.Note == "" {
		t.Fatalf("annotation fields missing: %+v", status)
	}

	// 2. A critical route still reaches its real handler: the missing bearer
	// is the handler's own 401, so degradation never blocked or bypassed the
	// 007 gate order.
	getResp, err := http.Get(srv.URL + "/withdrawals/req-1")
	if err != nil {
		t.Fatalf("GET /withdrawals/req-1: %v", err)
	}
	_, _ = io.Copy(io.Discard, getResp.Body)
	getResp.Body.Close()
	if getResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("GET /withdrawals/req-1 = %d, want the handler's 401 (gate ran unchanged)", getResp.StatusCode)
	}
	if got := getResp.Header.Get("X-TXHarbor-Degraded"); got != "true" {
		t.Fatalf("X-TXHarbor-Degraded = %q, want true", got)
	}
	if got := getResp.Header.Get("X-TXHarbor-Cache"); got != "bypass" {
		t.Fatalf("X-TXHarbor-Cache = %q, want bypass (authority reads never use the cache)", got)
	}

	// 3. The same critical route with the dependency back up carries no
	// degradation annotation (and still runs the handler).
	signals.Set("redis", true)
	getResp, err = http.Get(srv.URL + "/withdrawals/req-1")
	if err != nil {
		t.Fatalf("GET /withdrawals/req-1 (up): %v", err)
	}
	_, _ = io.Copy(io.Discard, getResp.Body)
	getResp.Body.Close()
	if getResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("GET (up) = %d, want 401", getResp.StatusCode)
	}
	if got := getResp.Header.Get("X-TXHarbor-Degraded"); got != "" {
		t.Fatalf("X-TXHarbor-Degraded = %q after recovery, want absent", got)
	}
}
