package app

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/xtianxx/txharbor/internal/health"
	"github.com/xtianxx/txharbor/internal/indexer"
	"github.com/xtianxx/txharbor/internal/metrics"
)

// TestRecoveryObserverMetricsWiring drives the serve-side gauge mirror with
// scripted snapshots against the real registry (T032): establish, progress,
// reconcile hold, release, repeat ticks, read failures and restarts each
// land the contract gauges without touching counters, funds, permissions or
// readiness. The production load function is LoadRecoverySnapshot; the
// registry served here is the same one health.NewServer exposes.
func TestRecoveryObserverMetricsWiring(t *testing.T) {
	agg := health.New("db", "rpc", "version", "chain")
	agg.Set("db", nil)
	agg.Set("rpc", nil)
	agg.Set("version", nil)
	agg.Set("chain", nil)
	m := metrics.New(agg.Ready)
	const chain = int64(31337)

	o := &recoveryObserver{read: func(ctx context.Context) (indexer.RecoveryState, indexer.Validity, bool) {
		return indexer.RecoveryStateNone, indexer.ValidityUnaffected, true
	}}
	mirror := func(snap indexer.RecoverySnapshot, err error) {
		t.Helper()
		o.observeMetrics(context.Background(), m, chain,
			func(context.Context) (indexer.RecoverySnapshot, error) { return snap, err })
	}

	ancestor := int64(14)
	frontBlock, frontLog, sweep := int64(18), int64(20), int64(20)
	established := indexer.RecoverySnapshot{
		Row:   &indexer.RecoveryRow{RecoveryID: "r1", Phase: "detected", MaxDepth: 25, BoundOldNumber: 20, Seq: 1},
		Bound: 25,
	}
	mirror(established, nil)
	if got, ok := gaugeValue(t, m.Gatherer(), metrics.ReorgActiveMetricName, map[string]string{"chain": "31337"}); !ok || got != 1 {
		t.Fatalf("active after establish = (%v,%v), want (1,true)", got, ok)
	}
	if got, ok := gaugeValue(t, m.Gatherer(), metrics.ReorgBoundMetricName, map[string]string{"chain": "31337"}); !ok || got != 25 {
		t.Fatalf("bound after establish = (%v,%v), want (25,true)", got, ok)
	}
	if _, ok := gaugeValue(t, m.Gatherer(), metrics.ReorgDepthMetricName, map[string]string{"chain": "31337"}); ok {
		t.Fatal("depth exposed while ancestor unconfirmed, want absent (unknown, not zero)")
	}
	if got, ok := gaugeValue(t, m.Gatherer(), metrics.ReorgReconcileMetricName, map[string]string{"chain": "31337"}); !ok || got != 0 {
		t.Fatalf("reconcile after establish = (%v,%v), want (0,true)", got, ok)
	}
	for _, stream := range []string{"block", "log", "deposit"} {
		if _, ok := gaugeValue(t, m.Gatherer(), metrics.ReorgFrontierLagMetricName, map[string]string{"chain": "31337", "stream": stream}); ok {
			t.Fatalf("frontier lag %s exposed before any frontier, want absent", stream)
		}
	}

	progress := indexer.RecoverySnapshot{
		Row:      &indexer.RecoveryRow{RecoveryID: "r1", Phase: "replaying", MaxDepth: 25, BoundOldNumber: 20, AncestorNumber: &ancestor, BlockFrontier: &frontBlock, LogFrontier: &frontLog, Seq: 1},
		SweepEnd: &sweep,
		Depth:    ptrInt64(6),
		Bound:    25,
	}
	mirror(progress, nil)
	if got, _ := gaugeValue(t, m.Gatherer(), metrics.ReorgDepthMetricName, map[string]string{"chain": "31337"}); got != 6 {
		t.Fatalf("depth after ancestor confirm = %v, want 6", got)
	}
	if got, _ := gaugeValue(t, m.Gatherer(), metrics.ReorgFrontierLagMetricName, map[string]string{"chain": "31337", "stream": "block"}); got != 2 {
		t.Fatalf("block lag = %v, want 2 (swept 20 - frontier 18)", got)
	}
	if got, ok := gaugeValue(t, m.Gatherer(), metrics.ReorgFrontierLagMetricName, map[string]string{"chain": "31337", "stream": "log"}); !ok || got != 0 {
		t.Fatalf("log lag = (%v,%v), want (0,true)", got, ok)
	}
	if _, ok := gaugeValue(t, m.Gatherer(), metrics.ReorgFrontierLagMetricName, map[string]string{"chain": "31337", "stream": "deposit"}); ok {
		t.Fatal("deposit lag exposed with nil frontier, want absent")
	}

	// Repeat tick with identical state rewrites identical values and never
	// manufactures a counter series.
	mirror(progress, nil)
	if got, _ := gaugeValue(t, m.Gatherer(), metrics.ReorgDepthMetricName, map[string]string{"chain": "31337"}); got != 6 {
		t.Fatalf("depth after repeat tick = %v, want still 6", got)
	}
	for _, name := range []string{metrics.ReorgOrphanedMetricName, metrics.ReorgRevivedMetricName, metrics.ReorgEvidenceWaitMetricName} {
		if n := seriesCount(t, m.Gatherer(), name); n != 0 {
			t.Fatalf("observer created %d %s series, want 0 (counters are executor-owned)", n, name)
		}
	}

	held := progress
	held.Reconcile = true
	mirror(held, nil)
	if got, _ := gaugeValue(t, m.Gatherer(), metrics.ReorgReconcileMetricName, map[string]string{"chain": "31337"}); got != 1 {
		t.Fatalf("reconcile while held = %v, want 1", got)
	}

	// A failed read keeps every gauge at its last value — never zeroed into
	// fake idle, never distinguished from a held state by invention.
	before := snapshotGauges(t, m.Gatherer())
	mirror(indexer.RecoverySnapshot{}, errors.New("connection refused"))
	after := snapshotGauges(t, m.Gatherer())
	if before != after {
		t.Fatalf("gauges moved on read failure:\nbefore %s\nafter  %s", before, after)
	}

	// Release (clean terminal read, no row): active and reconcile clear,
	// frontier lags disappear, depth/bound keep last known values.
	mirror(indexer.RecoverySnapshot{}, nil)
	if got, _ := gaugeValue(t, m.Gatherer(), metrics.ReorgActiveMetricName, map[string]string{"chain": "31337"}); got != 0 {
		t.Fatalf("active after release = %v, want 0 (no active=1 residue)", got)
	}
	if got, _ := gaugeValue(t, m.Gatherer(), metrics.ReorgReconcileMetricName, map[string]string{"chain": "31337"}); got != 0 {
		t.Fatalf("reconcile after release = %v, want 0", got)
	}
	for _, stream := range []string{"block", "log", "deposit"} {
		if _, ok := gaugeValue(t, m.Gatherer(), metrics.ReorgFrontierLagMetricName, map[string]string{"chain": "31337", "stream": stream}); ok {
			t.Fatalf("frontier lag %s survives release, want absent", stream)
		}
	}

	// Restart (fresh registry, pre-existing row): the first tick loads the
	// durable state — counters restart at zero by Prometheus reset
	// semantics, gauges reflect the row.
	m2 := metrics.New(agg.Ready)
	o2 := &recoveryObserver{read: o.read}
	o2.observeMetrics(context.Background(), m2, chain,
		func(context.Context) (indexer.RecoverySnapshot, error) { return progress, nil })
	if got, ok := gaugeValue(t, m2.Gatherer(), metrics.ReorgActiveMetricName, map[string]string{"chain": "31337"}); !ok || got != 1 {
		t.Fatalf("restarted active = (%v,%v), want (1,true)", got, ok)
	}
	if got, _ := gaugeValue(t, m2.Gatherer(), metrics.ReorgDepthMetricName, map[string]string{"chain": "31337"}); got != 6 {
		t.Fatalf("restarted depth = %v, want 6", got)
	}

	if !agg.Ready() {
		t.Fatal("readiness flipped during metrics observation: recovery gauges must never drive readyz")
	}

	// The served exposition carries the wired registry: the endpoint the
	// operator scrapes is the registry the loop counts into.
	srv := httptest.NewServer(m.Handler())
	defer srv.Close()
	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !contains(string(body), metrics.ReorgActiveMetricName) {
		t.Fatalf("served exposition lacks %s", metrics.ReorgActiveMetricName)
	}
}

// TestRecoveryMetricsAdapterCounts pins the executor funnel: exact
// conversion totals land in the registry counters with chain-only labels
// (no heights, hashes, or evidence text — SC-12 redaction boundary).
func TestRecoveryMetricsAdapterCounts(t *testing.T) {
	agg := health.New("db")
	agg.Set("db", nil)
	m := metrics.New(agg.Ready)
	const chain = int64(31337)
	a := recoveryMetricsAdapter{m: m}

	a.AddReorgOrphaned(chain, 3)
	a.AddReorgRevived(chain, 2)
	a.AddReorgEvidenceWait(chain, "timeout")
	a.AddReorgEvidenceWait(chain, "timeout")
	a.AddReorgEvidenceWait(chain, "contradictory")

	if got := counterValue(t, m.Gatherer(), metrics.ReorgOrphanedMetricName, map[string]string{"chain": "31337"}); got != 3 {
		t.Fatalf("orphaned = %v, want 3", got)
	}
	if got := counterValue(t, m.Gatherer(), metrics.ReorgRevivedMetricName, map[string]string{"chain": "31337"}); got != 2 {
		t.Fatalf("revived = %v, want 2", got)
	}
	if got := counterValue(t, m.Gatherer(), metrics.ReorgEvidenceWaitMetricName, map[string]string{"chain": "31337", "class": "timeout"}); got != 2 {
		t.Fatalf("evidence_wait{timeout} = %v, want 2", got)
	}
	if got := counterValue(t, m.Gatherer(), metrics.ReorgEvidenceWaitMetricName, map[string]string{"chain": "31337", "class": "contradictory"}); got != 1 {
		t.Fatalf("evidence_wait{contradictory} = %v, want 1", got)
	}
	assertChainOnlyLabels(t, m.Gatherer(), metrics.ReorgOrphanedMetricName)
	assertChainOnlyLabels(t, m.Gatherer(), metrics.ReorgRevivedMetricName)
}

func ptrInt64(v int64) *int64 { return &v }

func gatherFamily(t *testing.T, g prometheus.Gatherer, name string) *dto.MetricFamily {
	t.Helper()
	families, err := g.Gather()
	if err != nil {
		t.Fatalf("Gather(): %v", err)
	}
	for _, f := range families {
		if f.GetName() == name {
			return f
		}
	}
	return nil
}

func matchLabels(m *dto.Metric, want map[string]string) bool {
	got := map[string]string{}
	for _, lp := range m.Label {
		got[lp.GetName()] = lp.GetValue()
	}
	if len(got) != len(want) {
		return false
	}
	for k, v := range want {
		if got[k] != v {
			return false
		}
	}
	return true
}

func gaugeValue(t *testing.T, g prometheus.Gatherer, name string, labels map[string]string) (float64, bool) {
	t.Helper()
	f := gatherFamily(t, g, name)
	if f == nil {
		return 0, false
	}
	for _, m := range f.Metric {
		if matchLabels(m, labels) && m.Gauge != nil {
			return m.Gauge.GetValue(), true
		}
	}
	return 0, false
}

func counterValue(t *testing.T, g prometheus.Gatherer, name string, labels map[string]string) float64 {
	t.Helper()
	f := gatherFamily(t, g, name)
	if f == nil {
		t.Fatalf("counter family %s missing", name)
	}
	for _, m := range f.Metric {
		if matchLabels(m, labels) && m.Counter != nil {
			return m.Counter.GetValue()
		}
	}
	t.Fatalf("counter %s{%v} missing", name, labels)
	return 0
}

func seriesCount(t *testing.T, g prometheus.Gatherer, name string) int {
	t.Helper()
	f := gatherFamily(t, g, name)
	if f == nil {
		return 0
	}
	return len(f.Metric)
}

func snapshotGauges(t *testing.T, g prometheus.Gatherer) string {
	t.Helper()
	families, err := g.Gather()
	if err != nil {
		t.Fatalf("Gather(): %v", err)
	}
	out := ""
	for _, f := range families {
		for _, m := range f.Metric {
			if m.Gauge == nil {
				continue
			}
			out += f.GetName() + labelsString(m) + "\n"
		}
	}
	return out
}

func labelsString(m *dto.Metric) string {
	s := ""
	for _, lp := range m.Label {
		s += lp.GetName() + "=" + lp.GetValue() + ","
	}
	return s
}

func assertChainOnlyLabels(t *testing.T, g prometheus.Gatherer, name string) {
	t.Helper()
	f := gatherFamily(t, g, name)
	if f == nil {
		t.Fatalf("family %s missing", name)
	}
	for _, m := range f.Metric {
		if len(m.Label) != 1 || m.Label[0].GetName() != "chain" {
			t.Fatalf("%s carries non-chain labels: %v", name, m.Label)
		}
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
