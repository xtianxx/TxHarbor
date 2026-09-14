package metrics

import (
	"reflect"
	"strings"
	"testing"

	"github.com/xtianxx/txharbor/internal/logx"
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
	m.ObserveDepositState(31337, 0)
	m.ObserveDepositNext(31337, 1, true)
	m.ObserveDepositLag(31337, 0, true)
	m.ObserveDepositObservation(31337, "nomatch")
	m.ObserveDepositPause(31337)
	m.ObserveDepositTransition(31337, "ok")

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
			LogStateMetricName, LogRPCMetricName, LogPauseMetricName,
			DepositNextMetricName, DepositLagMetricName, DepositStateMetricName,
			DepositObservationsMetricName, DepositPauseMetricName, DepositTransitionMetricName,
			ConfirmationPendingMetricName, ConfirmationLagMetricName,
			ConfirmationStateMetricName, ConfirmationPolicySeqMetricName,
			ConfirmationConfirmedMetricName, ConfirmationSkippedMetricName,
			ConfirmationTransitionMetricName, ConfirmationPolicyTransitionMetricName:
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

// TestDepositMetricsContract covers the 004 observability contract: five
// deposit states (4 = structural gap halt), next/lag series absent while
// progress is empty, the four observation result classes, the monotonic pause
// counter and the authorised-transition result counter.
func TestDepositMetricsContract(t *testing.T) {
	m := New(func() bool { return true })

	for _, state := range []int{0, 1, 2, 3, 4} {
		m.ObserveDepositState(31337, state)
		if got := gatherGauge(t, m, DepositStateMetricName); got != float64(state) {
			t.Fatalf("%s = %v, want %d", DepositStateMetricName, got, state)
		}
	}

	m.ObserveDepositNext(31337, 42, true)
	if got := gatherGauge(t, m, DepositNextMetricName); got != 42 {
		t.Fatalf("%s = %v, want 42", DepositNextMetricName, got)
	}
	m.ObserveDepositNext(31337, 42, false)
	if got := familyLen(t, m, DepositNextMetricName); got != 0 {
		t.Fatalf("empty progress exposed %d %s series, want 0", got, DepositNextMetricName)
	}

	m.ObserveDepositLag(31337, 7, true)
	if got := gatherGauge(t, m, DepositLagMetricName); got != 7 {
		t.Fatalf("%s = %v, want 7", DepositLagMetricName, got)
	}
	m.ObserveDepositLag(31337, 7, false)
	if got := familyLen(t, m, DepositLagMetricName); got != 0 {
		t.Fatalf("empty checkpoint exposed %d %s series, want 0", got, DepositLagMetricName)
	}

	observed := []string{"matched", "nomatch", "zero", "invalid"}
	for _, result := range observed {
		m.ObserveDepositObservation(31337, result)
	}
	results := gatherCounters(t, m, DepositObservationsMetricName)
	for _, result := range observed {
		if got := results["chain=31337,result="+result]; got != 1 {
			t.Fatalf("%s{result=%s} = %v, want 1 (all: %v)", DepositObservationsMetricName, result, got, results)
		}
	}

	m.ObserveDepositPause(31337)
	m.ObserveDepositPause(31337)
	if got := gatherCounters(t, m, DepositPauseMetricName)["chain=31337"]; got != 2 {
		t.Fatalf("%s = %v, want 2", DepositPauseMetricName, got)
	}

	for _, result := range []string{"ok", "error", "rejected"} {
		m.ObserveDepositTransition(31337, result)
	}
	transitions := gatherCounters(t, m, DepositTransitionMetricName)
	for _, result := range []string{"ok", "error", "rejected"} {
		if got := transitions["chain=31337,result="+result]; got != 1 {
			t.Fatalf("%s{result=%s} = %v, want 1 (all: %v)", DepositTransitionMetricName, result, got, transitions)
		}
	}
}

// TestDepositStateDoesNotFlipReady freezes readyz semantics: deposit pause
// (state=3) and structural halt (state=4) must not change readiness, which
// only reflects dependency health (contracts/observability.md).
func TestDepositStateDoesNotFlipReady(t *testing.T) {
	ready := true
	m := New(func() bool { return ready })

	for _, state := range []int{3, 4} {
		m.ObserveDepositState(31337, state)
		if got := gatherGauge(t, m, ReadyMetricName); got != 1 {
			t.Fatalf("txharbor_ready = %v with deposit_state=%d, want 1 (readyz frozen)", got, state)
		}
	}
	ready = false
	if got := gatherGauge(t, m, ReadyMetricName); got != 0 {
		t.Fatalf("txharbor_ready = %v after dependency loss, want 0", got)
	}
}

// TestDepositLogContract freezes the structured log manifest and the
// redaction hook (contracts/observability.md, SC-09): every event carries
// chain_id first, no field name is credential-bearing, and DepositLogRedact
// scrubs credentials before they reach slog.
func TestDepositLogContract(t *testing.T) {
	want := map[string][]string{
		DepositLogAdvance:      {"chain_id", "from_block", "to_block", "matched", "nomatch", "zero", "attempt"},
		DepositLogWait:         {"chain_id", "next_block", "reason"},
		DepositLogRetry:        {"chain_id", "from_block", "to_block", "kind", "attempt", "retry_in"},
		DepositLogGap:          {"chain_id", "gap_from", "gap_to", "class", "cause", "config"},
		DepositLogPause:        {"chain_id", "pause_id", "revision", "height", "kind", "detail"},
		DepositLogRelease:      {"chain_id", "pause_id", "revision", "operator", "reason", "result"},
		DepositLogConfigReject: {"chain_id", "reason", "detail"},
		DepositLogTransition:   {"chain_id", "request_id", "operator", "old_config", "new_config", "replay_from", "result", "reason"},
	}
	if !reflect.DeepEqual(DepositLogFields, want) {
		t.Fatalf("DepositLogFields drifted from contracts/observability.md:\ngot  %v\nwant %v", DepositLogFields, want)
	}

	for event, fields := range DepositLogFields {
		if len(fields) == 0 || fields[0] != "chain_id" {
			t.Errorf("event %q fields %v: chain_id must come first", event, fields)
		}
		seen := map[string]bool{}
		for _, field := range fields {
			if seen[field] {
				t.Errorf("event %q repeats field %q", event, field)
			}
			seen[field] = true
			for _, banned := range []string{"token", "secret", "password", "passwd", "pwd", "dsn", "url", "key", "authorization"} {
				if strings.Contains(strings.ToLower(field), banned) {
					t.Errorf("event %q field %q looks credential-bearing", event, field)
				}
			}
		}
	}

	line := "dsn=postgres://txharbor:hunter2@db:5432/txharbor Authorization: Bearer sk-live-123 authorization=Bearer sk-live-456 password=p@ss"
	got := DepositLogRedact(line)
	for _, secret := range []string{"hunter2", "sk-live-123", "sk-live-456", "p@ss"} {
		if strings.Contains(got, secret) {
			t.Fatalf("DepositLogRedact(%q) leaked %q: %q", line, secret, got)
		}
	}
	if !strings.Contains(got, logx.Redacted) {
		t.Fatalf("DepositLogRedact(%q) = %q, want %s", line, got, logx.Redacted)
	}
}

// TestConfirmationMetricsContract covers the 005 observability contract: four
// confirmation states, pending exposed as 0 while empty, lag/policy_seq
// series absent while unready, the confirmed counter, the below_depth skipped
// reason and the transition/policy-transition result classes.
func TestConfirmationMetricsContract(t *testing.T) {
	m := New(func() bool { return true })

	for _, state := range []int{0, 1, 2, 3} {
		m.ObserveConfirmationState(31337, state)
		if got := gatherGauge(t, m, ConfirmationStateMetricName); got != float64(state) {
			t.Fatalf("%s = %v, want %d", ConfirmationStateMetricName, got, state)
		}
	}

	m.ObserveConfirmationPending(31337, 0)
	if got := gatherGauge(t, m, ConfirmationPendingMetricName); got != 0 {
		t.Fatalf("%s = %v, want 0 (empty exposed as 0)", ConfirmationPendingMetricName, got)
	}
	m.ObserveConfirmationPending(31337, 5)
	if got := gatherGauge(t, m, ConfirmationPendingMetricName); got != 5 {
		t.Fatalf("%s = %v, want 5", ConfirmationPendingMetricName, got)
	}

	m.ObserveConfirmationLag(31337, 7, true)
	if got := gatherGauge(t, m, ConfirmationLagMetricName); got != 7 {
		t.Fatalf("%s = %v, want 7", ConfirmationLagMetricName, got)
	}
	m.ObserveConfirmationLag(31337, 7, false)
	if got := familyLen(t, m, ConfirmationLagMetricName); got != 0 {
		t.Fatalf("unready checkpoint exposed %d %s series, want 0", got, ConfirmationLagMetricName)
	}

	m.ObserveConfirmationPolicySeq(31337, 3, true)
	if got := gatherGauge(t, m, ConfirmationPolicySeqMetricName); got != 3 {
		t.Fatalf("%s = %v, want 3", ConfirmationPolicySeqMetricName, got)
	}
	m.ObserveConfirmationPolicySeq(31337, 3, false)
	if got := familyLen(t, m, ConfirmationPolicySeqMetricName); got != 0 {
		t.Fatalf("empty policy exposed %d %s series, want 0", got, ConfirmationPolicySeqMetricName)
	}

	m.ObserveConfirmationConfirmed(31337)
	m.ObserveConfirmationConfirmed(31337)
	if got := gatherCounters(t, m, ConfirmationConfirmedMetricName)["chain=31337"]; got != 2 {
		t.Fatalf("%s = %v, want 2", ConfirmationConfirmedMetricName, got)
	}

	m.ObserveConfirmationSkipped(31337, "below_depth")
	if got := gatherCounters(t, m, ConfirmationSkippedMetricName)["chain=31337,reason=below_depth"]; got != 1 {
		t.Fatalf("%s{below_depth} = %v, want 1", ConfirmationSkippedMetricName, got)
	}

	for _, result := range []string{"ok", "stale", "rejected"} {
		m.ObserveConfirmationTransition(31337, result)
	}
	transitions := gatherCounters(t, m, ConfirmationTransitionMetricName)
	for _, result := range []string{"ok", "stale", "rejected"} {
		if got := transitions["chain=31337,result="+result]; got != 1 {
			t.Fatalf("%s{result=%s} = %v, want 1 (all: %v)", ConfirmationTransitionMetricName, result, got, transitions)
		}
	}

	for _, result := range []string{"ok", "rejected"} {
		m.ObserveConfirmationPolicyTransition(31337, result)
	}
	policy := gatherCounters(t, m, ConfirmationPolicyTransitionMetricName)
	for _, result := range []string{"ok", "rejected"} {
		if got := policy["chain=31337,result="+result]; got != 1 {
			t.Fatalf("%s{result=%s} = %v, want 1 (all: %v)", ConfirmationPolicyTransitionMetricName, result, got, policy)
		}
	}
}

// TestConfirmationStateDoesNotFlipReady freezes readyz semantics: the
// confirmation stop state (state=3) must not change readiness, which only
// reflects dependency health (contracts/observability.md).
func TestConfirmationStateDoesNotFlipReady(t *testing.T) {
	ready := true
	m := New(func() bool { return ready })

	m.ObserveConfirmationState(31337, 3)
	if got := gatherGauge(t, m, ReadyMetricName); got != 1 {
		t.Fatalf("txharbor_ready = %v with confirmation_state=3, want 1 (readyz frozen)", got)
	}
	ready = false
	if got := gatherGauge(t, m, ReadyMetricName); got != 0 {
		t.Fatalf("txharbor_ready = %v after dependency loss, want 0", got)
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
