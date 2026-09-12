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

	families, err := m.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}

	allowed := func(name string) bool {
		return name == ReadyMetricName || name == ProbeMetricName ||
			strings.HasPrefix(name, "go_") || strings.HasPrefix(name, "process_")
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
