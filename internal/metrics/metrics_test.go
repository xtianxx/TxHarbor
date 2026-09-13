package metrics

import (
	"strings"
	"testing"
)

func TestRegistryExposesOnlyFoundationMetrics(t *testing.T) {
	ready := true
	m := New(func() bool { return ready })
	m.ObserveProbe("db", true)
	m.ObserveProbe("db", false)
	m.ObserveProbe("rpc", true)
	m.ObserveIndexerState(31337, 0)
	m.ObserveIndexerCheckpoint(31337, 1, true)
	m.ObserveIndexerRPC("timeout", false)
	m.ObserveIndexerPause(31337)
	m.ObserveLogState(31337, 0)
	m.ObserveLogCheckpointNext(31337, 1, true)
	m.ObserveLogLag(31337, 0, true)
	m.ObserveLogRPC("incomplete", false)
	m.ObserveLogPause(31337)

	families, err := m.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}

	allowed := func(name string) bool {
		switch name {
		case ReadyMetricName, ProbeMetricName,
			IndexerCheckpointMetricName, IndexerStateMetricName,
			IndexerRPCMetricName, IndexerPauseMetricName,
			LogCheckpointNextMetricName, LogLagMetricName,
			LogStateMetricName, LogRPCMetricName, LogPauseMetricName:
			return true
		}
		return strings.HasPrefix(name, "go_") || strings.HasPrefix(name, "process_")
	}

	var sawReady, sawProbe, sawProcess bool
	for _, f := range families {
		name := f.GetName()
		if !allowed(name) {
			t.Errorf("unexpected metric %q (business/backlog metrics are out of scope)", name)
		}
		if name == ReadyMetricName {
			sawReady = true
		}
		if name == ProbeMetricName {
			sawProbe = true
		}
		if strings.HasPrefix(name, "process_") {
			sawProcess = true
		}
		// The default Go collector ships go_gc_duration_seconds (a summary);
		// the 001 prohibition targets business latency histograms/summaries,
		// so only txharbor_* metrics are type-checked.
		if strings.HasPrefix(name, "txharbor_") {
			switch f.GetType().String() {
			case "GAUGE", "COUNTER":
			default:
				t.Errorf("metric %q has forbidden type %s (no histograms/summaries in 001)", name, f.GetType())
			}
		}
	}
	if !sawReady || !sawProbe {
		t.Fatalf("missing foundation metrics: ready=%t probe=%t", sawReady, sawProbe)
	}
	if !sawProcess {
		t.Fatal("missing default process collectors")
	}
}

func TestReadyGaugeFollowsReadiness(t *testing.T) {
	ready := false
	m := New(func() bool { return ready })

	if got := gatherGauge(t, m, ReadyMetricName); got != 0 {
		t.Fatalf("txharbor_ready = %v, want 0", got)
	}
	ready = true
	if got := gatherGauge(t, m, ReadyMetricName); got != 1 {
		t.Fatalf("txharbor_ready = %v, want 1", got)
	}
}

func TestProbeCounterLabels(t *testing.T) {
	m := New(func() bool { return true })
	m.ObserveProbe("db", true)
	m.ObserveProbe("db", false)
	m.ObserveProbe("rpc", true)

	counters := map[string]float64{}
	families, err := m.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	for _, f := range families {
		if f.GetName() != ProbeMetricName {
			continue
		}
		for _, metric := range f.GetMetric() {
			labels := map[string]string{}
			for _, lp := range metric.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			counters[labels["dep"]+"/"+labels["result"]] = metric.GetCounter().GetValue()
		}
	}
	want := map[string]float64{
		"db/success":  1,
		"db/failure":  1,
		"rpc/success": 1,
	}
	for key, val := range want {
		if counters[key] != val {
			t.Errorf("txharbor_probe_total{%s} = %v, want %v (all: %v)", key, counters[key], val, counters)
		}
	}
}

func gatherGauge(t *testing.T, m *Metrics, name string) float64 {
	t.Helper()
	families, err := m.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	for _, f := range families {
		if f.GetName() == name && len(f.GetMetric()) > 0 {
			return f.GetMetric()[0].GetGauge().GetValue()
		}
	}
	t.Fatalf("metric %q not found", name)
	return 0
}

// TestIndexerMetricsContract covers the 002 observability contract: four state
// values, checkpoint series absent while progress is empty, RPC outcome kinds,
// and a monotonic pause counter.
func TestIndexerMetricsContract(t *testing.T) {
	m := New(func() bool { return true })

	for _, state := range []int{0, 1, 2, 3} {
		m.ObserveIndexerState(31337, state)
		if got := gatherGauge(t, m, IndexerStateMetricName); got != float64(state) {
			t.Fatalf("%s = %v, want %d", IndexerStateMetricName, got, state)
		}
	}

	m.ObserveIndexerCheckpoint(31337, 42, true)
	if got := gatherGauge(t, m, IndexerCheckpointMetricName); got != 42 {
		t.Fatalf("%s = %v, want 42", IndexerCheckpointMetricName, got)
	}
	m.ObserveIndexerCheckpoint(31337, 42, false)
	if got := familyLen(t, m, IndexerCheckpointMetricName); got != 0 {
		t.Fatalf("empty progress exposed %d %s series, want 0", got, IndexerCheckpointMetricName)
	}

	m.ObserveIndexerRPC("not-found", true)
	m.ObserveIndexerRPC("timeout", false)
	m.ObserveIndexerRPC("timeout", false)
	rpc := gatherCounters(t, m, IndexerRPCMetricName)
	if rpc["kind=not-found,result=ok"] != 1 || rpc["kind=timeout,result=error"] != 2 {
		t.Fatalf("%s = %v", IndexerRPCMetricName, rpc)
	}

	m.ObserveIndexerPause(31337)
	m.ObserveIndexerPause(31337)
	if got := gatherCounters(t, m, IndexerPauseMetricName)["chain=31337"]; got != 2 {
		t.Fatalf("%s = %v, want 2", IndexerPauseMetricName, got)
	}
}

// TestLogMetricsContract covers the 003 observability contract: log state,
// next-block series absent while progress is empty, lag absent when either
// checkpoint is empty, RPC outcome kinds and a monotonic pause counter.
func TestLogMetricsContract(t *testing.T) {
	m := New(func() bool { return true })

	m.ObserveLogState(31337, 3)
	if got := gatherGauge(t, m, LogStateMetricName); got != 3 {
		t.Fatalf("%s = %v, want 3", LogStateMetricName, got)
	}

	m.ObserveLogCheckpointNext(31337, 100, true)
	if got := gatherGauge(t, m, LogCheckpointNextMetricName); got != 100 {
		t.Fatalf("%s = %v, want 100", LogCheckpointNextMetricName, got)
	}
	m.ObserveLogCheckpointNext(31337, 100, false)
	if got := familyLen(t, m, LogCheckpointNextMetricName); got != 0 {
		t.Fatalf("empty progress exposed %d %s series, want 0", got, LogCheckpointNextMetricName)
	}

	m.ObserveLogLag(31337, 7, true)
	if got := gatherGauge(t, m, LogLagMetricName); got != 7 {
		t.Fatalf("%s = %v, want 7", LogLagMetricName, got)
	}
	m.ObserveLogLag(31337, 7, false)
	if got := familyLen(t, m, LogLagMetricName); got != 0 {
		t.Fatalf("empty checkpoint exposed %d %s series, want 0", got, LogLagMetricName)
	}

	m.ObserveLogRPC("incomplete", false)
	m.ObserveLogRPC("incomplete", false)
	m.ObserveLogRPC("not-found", true)
	rpc := gatherCounters(t, m, LogRPCMetricName)
	if rpc["kind=incomplete,result=error"] != 2 || rpc["kind=not-found,result=ok"] != 1 {
		t.Fatalf("%s = %v", LogRPCMetricName, rpc)
	}

	m.ObserveLogPause(31337)
	m.ObserveLogPause(31337)
	if got := gatherCounters(t, m, LogPauseMetricName)["chain=31337"]; got != 2 {
		t.Fatalf("%s = %v, want 2", LogPauseMetricName, got)
	}
}

func familyLen(t *testing.T, m *Metrics, name string) int {
	t.Helper()
	families, err := m.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	total := 0
	for _, f := range families {
		if f.GetName() == name {
			total += len(f.GetMetric())
		}
	}
	return total
}

func gatherCounters(t *testing.T, m *Metrics, name string) map[string]float64 {
	t.Helper()
	families, err := m.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	out := map[string]float64{}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, metric := range f.GetMetric() {
			key := ""
			for _, lp := range metric.GetLabel() {
				if key != "" {
					key += ","
				}
				key += lp.GetName() + "=" + lp.GetValue()
			}
			out[key] = metric.GetCounter().GetValue()
		}
	}
	return out
}
