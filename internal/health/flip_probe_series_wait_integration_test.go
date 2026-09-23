//go:build integration

// flip_probe_series_wait_integration_test.go owns the bounded first-probe
// observation used by TestReadyzFlipsAndRecoversWithRealDependencies before it
// asserts the /metrics foundation body, plus the Docker-free controls that pin
// the wait condition itself.
//
// Root cause it addresses: /readyz 200 and txharbor_ready 1 do not prove the
// probe runner has run. serve pre-seeds the aggregate ready before the listener
// opens (internal/app/serve.go) and txharbor_ready is a GaugeFunc, while
// txharbor_probe_total is a CounterVec whose dep/result child exists only after
// the runner's first ObserveProbe (internal/metrics/metrics.go). The old
// assertion scraped /metrics once and raced that first cycle: a listener that
// wins the scheduler answers the scrape while the family is still empty.
//
// The wait is a pure test-side observation loop: no goroutine, no fixed sleep
// beyond the shared 100ms polling idiom, bounded deadline. It does not relax
// the assertion — it only waits until real probe evidence (a full
// name/label/value sample) is exposed, and then the original checks run against
// a fresh scrape.
package health_test

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/xtianxx/txharbor/internal/health"
	"github.com/xtianxx/txharbor/internal/metrics"
)

// flipProbeSeriesWaitTimeout bounds the wait for the runner's first probe
// cycle. The runner probes immediately on start and each check is bounded by
// its ProbeTimeout (5s default in this scene), so a healthy scene exposes its
// first series within milliseconds; 10s is scheduling headroom for a loaded CI
// box, not a sleep. A probe that never reports still fails bounded (see
// TestFlipProbeSeriesWaitControls).
const flipProbeSeriesWaitTimeout = 10 * time.Second

// pollFlipProbeSeries polls /metrics until one txharbor_probe_total sample
// (family name, both labels and a numeric value) is exposed, or until the
// deadline expires. It never fails the test: it returns the last observation
// and whether the series was seen, so the caller owns the failure message.
//
// Request failure and metric absence stay distinct: a transport-level failure
// is returned as reqErr (non-nil), while a successful scrape without the series
// is reqErr == nil with observed == false. body always carries the most recent
// successful scrape, so a timeout can print it as diagnostics either way.
func pollFlipProbeSeries(base string, timeout time.Duration) (body string, reqErr error, observed bool) {
	deadline := time.Now().Add(timeout)
	for {
		b, err := flipBarrierMetricsGet(base + "/metrics")
		if err != nil {
			reqErr = err
		} else {
			body, reqErr = b, nil
			if flipProbeSeries.MatchString(b) {
				return body, nil, true
			}
		}
		if !time.Now().Before(deadline) {
			return body, reqErr, false
		}
		time.Sleep(flipBarrierPollInterval)
	}
}

// waitFlipProbeSeriesObserved blocks until the runner's first probe cycle is
// observable in /metrics. Timeout fails the test with the last scrape body
// (metric absent) or the last request error (metrics endpoint failing) as
// diagnostics, keeping "request failed" and "metric absent" distinguishable.
func waitFlipProbeSeriesObserved(t *testing.T, base string, timeout time.Duration) {
	t.Helper()
	body, reqErr, observed := pollFlipProbeSeries(base, timeout)
	if observed {
		return
	}
	if reqErr != nil {
		if body == "" {
			body = "(no successful /metrics response)"
		}
		t.Fatalf("txharbor_probe_total not observed within %s: latest /metrics request failed: %v; last successful body:\n%s",
			timeout, reqErr, body)
	}
	t.Fatalf("txharbor_probe_total not observed within %s: latest /metrics body carries no probe series:\n%s",
		timeout, body)
}

// TestFlipProbeSeriesWaitControls is the Docker-free deterministic control for
// the wait condition, driven by the real production pieces (health.Runner,
// metrics.New, the exposition handler):
//
//   - normal path: after the runner's first ProbeOnce the series must be
//     observable, so the wait is guaranteed to succeed on a healthy scene;
//   - broken path: without probe evidence the wait must end observed=false at
//     its own deadline — it never passes vacuously and never blocks;
//   - transport path: a failing /metrics request stays a request error,
//     distinct from metric absence.
//
// The first subtest also reproduces the root-cause ordering: the ready gauge is
// exposed before the runner has run while the probe family is absent.
func TestFlipProbeSeriesWaitControls(t *testing.T) {
	t.Run("real first probe cycle is observed", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		m := metrics.New(func() bool { return true })
		srv := httptest.NewServer(m.Handler())
		defer srv.Close()

		pre, err := flipBarrierMetricsGet(srv.URL + "/metrics")
		if err != nil {
			t.Fatalf("pre-run /metrics: %v", err)
		}
		if !strings.Contains(pre, "txharbor_ready 1") {
			t.Fatalf("root-cause control: ready gauge absent before the runner ran:\n%s", pre)
		}
		if strings.Contains(pre, "txharbor_probe_total") {
			t.Fatalf("root-cause control: probe family already exposed before the runner ran:\n%s", pre)
		}

		agg := health.New("db")
		runner := &health.Runner{
			Interval: time.Hour, // the immediate first cycle is all this control needs
			Timeout:  time.Second,
			Agg:      agg,
			Checks: []health.Check{{Name: "db", Probe: func(context.Context) []health.Result {
				return []health.Result{{Name: "db"}}
			}}},
			Observe: m.ObserveProbe,
		}
		go runner.Run(ctx)

		start := time.Now()
		waitFlipProbeSeriesObserved(t, srv.URL, flipProbeSeriesWaitTimeout)
		t.Logf("first probe series observed %s after runner start", time.Since(start).Round(time.Millisecond))

		body, err := flipBarrierMetricsGet(srv.URL + "/metrics")
		if err != nil {
			t.Fatalf("post-run /metrics: %v", err)
		}
		if !strings.Contains(body, `txharbor_probe_total{dep="db",result="success"} 1`) {
			t.Fatalf("waited-up series lacks the expected name/labels/value:\n%s", body)
		}
	})

	t.Run("absent probe evidence times out bounded", func(t *testing.T) {
		srv := httptest.NewServer(metrics.New(func() bool { return true }).Handler())
		defer srv.Close()

		start := time.Now()
		body, err, observed := pollFlipProbeSeries(srv.URL, 300*time.Millisecond)
		elapsed := time.Since(start)
		if observed {
			t.Fatal("probe series observed without any probe cycle")
		}
		if err != nil {
			t.Fatalf("metric absence reported as a request failure: %v", err)
		}
		if elapsed < 250*time.Millisecond {
			t.Fatalf("bounded wait returned after %s, before its 300ms deadline", elapsed)
		}
		if !strings.Contains(body, "txharbor_ready 1") {
			t.Fatalf("absence diagnostics lost the last scrape body:\n%s", body)
		}
	})

	t.Run("request failure is distinct from absence", func(t *testing.T) {
		srv := httptest.NewServer(metrics.New(func() bool { return true }).Handler())
		url := srv.URL
		srv.Close() // every subsequent request fails at the transport

		body, err, observed := pollFlipProbeSeries(url, 300*time.Millisecond)
		if observed {
			t.Fatal("probe series observed from a closed metrics endpoint")
		}
		if err == nil {
			t.Fatal("closed metrics endpoint surfaced as metric absence, not a request failure")
		}
		if body != "" {
			t.Fatalf("request failure carried a scrape body: %q", body)
		}
	})
}
