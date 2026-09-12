package health

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestAggregateFlipsWithFakeProbe covers FR-011/012: one failing dependency
// makes the aggregate not-ready, and its recovery makes it ready again with no
// restart. The probe is a fake so the flip logic is exercised in isolation.
func TestAggregateFlipsWithFakeProbe(t *testing.T) {
	var failing atomic.Bool
	failing.Store(true)

	agg := New("db", "rpc")
	probe := func(context.Context) []Result {
		if failing.Load() {
			return []Result{{Name: "db", Err: errors.New("connection refused")}, {Name: "rpc"}}
		}
		return []Result{{Name: "db"}, {Name: "rpc"}}
	}
	runner := &Runner{
		Interval: 10 * time.Millisecond,
		Timeout:  time.Second,
		Agg:      agg,
		Checks:   []Check{{Name: "db", Probe: probe}},
	}

	runner.ProbeOnce(context.Background())
	if agg.Ready() {
		t.Fatal("aggregate ready while db probe fails")
	}
	rep := agg.Report()
	if rep.Ready {
		t.Fatal("report ready while db probe fails")
	}
	if len(rep.Failed) != 1 || rep.Failed[0] != "db" {
		t.Fatalf("failed = %v, want [db]", rep.Failed)
	}
	if !strings.Contains(rep.Detail, "connection refused") {
		t.Fatalf("detail lost diagnosis: %q", rep.Detail)
	}

	failing.Store(false)
	runner.ProbeOnce(context.Background())
	if !agg.Ready() {
		t.Fatal("aggregate did not recover to ready after probe success")
	}
	if rep := agg.Report(); !rep.Ready || len(rep.Failed) != 0 {
		t.Fatalf("report after recovery = %+v", rep)
	}
}

func TestRunnerObserveCallbackCountsResults(t *testing.T) {
	agg := New("db")
	var success, failure int
	runner := &Runner{
		Interval: time.Second,
		Timeout:  time.Second,
		Agg:      agg,
		Observe: func(dep string, ok bool) {
			if dep != "db" {
				t.Errorf("observed dep = %q, want db", dep)
			}
			if ok {
				success++
			} else {
				failure++
			}
		},
		Checks: []Check{{Name: "db", Probe: func(context.Context) []Result {
			return []Result{{Name: "db", Err: errors.New("down")}}
		}}},
	}
	runner.ProbeOnce(context.Background())
	if success != 0 || failure != 1 {
		t.Fatalf("observe counts = success:%d failure:%d, want 0/1", success, failure)
	}
}

func TestLivezAlwaysAliveAndReadyzLifecycle(t *testing.T) {
	agg := New("db", "rpc", "version", "chain")
	srv := NewServer(agg, nil)
	handler := srv.Handler()

	get := func(path string) (*httptest.ResponseRecorder, map[string]any) {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		var body map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("GET %s: invalid JSON %q: %v", path, rec.Body.String(), err)
		}
		return rec, body
	}

	// Dependencies down: livez stays 200, readyz is 503 with failed list.
	rec, body := get("/livez")
	if rec.Code != http.StatusOK || body["status"] != "alive" {
		t.Fatalf("livez = %d %v", rec.Code, body)
	}
	rec, body = get("/readyz")
	if rec.Code != http.StatusServiceUnavailable || body["status"] != "not-ready" {
		t.Fatalf("readyz = %d %v", rec.Code, body)
	}
	if failed, ok := body["failed"].([]any); !ok || len(failed) != 4 {
		t.Fatalf("readyz failed = %v, want 4 entries", body["failed"])
	}

	// All checks pass: readyz 200 with structured checks.
	agg.Set("db", nil)
	agg.Set("rpc", nil)
	agg.Set("version", nil)
	agg.Set("chain", nil)
	rec, body = get("/readyz")
	if rec.Code != http.StatusOK || body["status"] != "ready" {
		t.Fatalf("readyz = %d %v", rec.Code, body)
	}
	checks, ok := body["checks"].(map[string]any)
	if !ok || checks["db"] != "ok" || checks["rpc"] != "ok" || checks["version"] != "ok" || checks["chain"] != "ok" {
		t.Fatalf("readyz checks = %v", body["checks"])
	}
}

func TestReadyzRedactsCredentialsInDetail(t *testing.T) {
	agg := New("db")
	agg.Set("db", errors.New("ping postgres://txharbor:hunter2@127.0.0.1:5432/txharbor failed"))
	srv := NewServer(agg, nil)

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz status = %d, want 503", rec.Code)
	}
	body := rec.Body.String()
	if strings.Contains(body, "hunter2") {
		t.Fatalf("readyz leaks credential: %s", body)
	}
	if !strings.Contains(body, "[REDACTED]") {
		t.Fatalf("readyz missing redaction marker: %s", body)
	}
}
