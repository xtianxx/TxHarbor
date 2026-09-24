package metrics

import (
	"reflect"
	"sort"
	"testing"

	dto "github.com/prometheus/client_model/go"
)

// eventsManifest is the frozen 013 registration table (T003): exposition
// name, Prometheus type and the exact label set. A change here is an
// observability contract change (verification.md §1).
var eventsManifest = []struct {
	name   string
	typ    dto.MetricType
	labels []string
}{
	{OutboxPendingMetricName, dto.MetricType_GAUGE, []string{"event_family"}},
	{OutboxPendingOldestAgeMetricName, dto.MetricType_GAUGE, []string{"event_family"}},
	{OutboxPublishFailuresMetricName, dto.MetricType_COUNTER, []string{"error_class"}},
	{OutboxPublishedMetricName, dto.MetricType_COUNTER, nil},
	{OutboxAttemptsMetricName, dto.MetricType_COUNTER, nil},
	{OutboxBlockedMetricName, dto.MetricType_GAUGE, nil},
	{ConsumerLagSecondsMetricName, dto.MetricType_GAUGE, []string{"consumer_name", "partition"}},
	{ConsumerLagMessagesMetricName, dto.MetricType_GAUGE, []string{"consumer_name", "partition"}},
	{ConsumerAppliedMetricName, dto.MetricType_COUNTER, nil},
	{ConsumerRetryMetricName, dto.MetricType_COUNTER, []string{"failure_class"}},
	{ConsumerQuarantineMetricName, dto.MetricType_COUNTER, []string{"failure_class"}},
	{ConsumerCatchupSecondsMetricName, dto.MetricType_GAUGE, []string{"consumer_name"}},
	{ConsumerReplayClockMetricName, dto.MetricType_GAUGE, []string{"consumer_name"}},
	{EventReplayOpsMetricName, dto.MetricType_COUNTER, []string{"consumer_name", "op_kind"}},
	{CacheHitsMetricName, dto.MetricType_COUNTER, []string{"family"}},
	{CacheMissesMetricName, dto.MetricType_COUNTER, []string{"family"}},
	{CacheFallbackMetricName, dto.MetricType_COUNTER, []string{"family"}},
	{CacheEpochRotationsMetricName, dto.MetricType_COUNTER, nil},
	{RateLimitDeniedMetricName, dto.MetricType_COUNTER, []string{"interface_class"}},
	{RateLimitUnavailableMetricName, dto.MetricType_GAUGE, nil},
	{RateLimitRecoveryMetricName, dto.MetricType_COUNTER, []string{"interface_class"}},
	{RPCBudgetPausedMetricName, dto.MetricType_COUNTER, []string{"class"}},
	{CapacitySoftBreachesMetricName, dto.MetricType_COUNTER, nil},
	{CapacityHardBreachesMetricName, dto.MetricType_COUNTER, nil},
	{CapacityRefusalsMetricName, dto.MetricType_COUNTER, []string{"op_class"}},
	{RedisAvailableMetricName, dto.MetricType_GAUGE, nil},
	{KafkaAvailableMetricName, dto.MetricType_GAUGE, nil},
	{EventsIdentityConflictMetricName, dto.MetricType_COUNTER, []string{"event_type"}},
}

// observeAll013 initializes every 013 series with one sample so the gathered
// registry exposes each family (Vec families are absent until observed).
func observeAll013(m *Metrics) {
	m.SetOutboxPending("deposit", 3)
	m.SetOutboxPendingOldestAge("deposit", 1.5)
	m.ObserveOutboxPublishFailure("transient")
	m.ObserveOutboxPublished(2)
	m.ObserveOutboxAttempts(4)
	m.SetOutboxBlocked(1)
	m.SetConsumerLag("ref", 0, 1.25, 9)
	m.ObserveConsumerApplied()
	m.ObserveConsumerRetry("retryable")
	m.ObserveConsumerQuarantine("version_gap")
	m.SetConsumerCatchupSeconds("ref", 12.5)
	m.SetConsumerReplayClock("ref", 1700000000)
	m.ObserveEventReplay("replay", "ref")
	m.ObserveCacheHit("deposit")
	m.ObserveCacheMiss("deposit")
	m.ObserveCacheFallback("deposit")
	m.ObserveCacheEpochRotation()
	m.ObserveRateLimitDenied("new_withdrawal")
	m.SetRateLimitUnavailable(true)
	m.ObserveRateLimitRecovery("new_withdrawal")
	m.ObserveRPCBudgetPaused("logs")
	m.ObserveCapacitySoftBreach()
	m.ObserveCapacityHardBreach()
	m.ObserveCapacityRefusal("withdrawal_create")
	m.SetRedisAvailable(false)
	m.SetKafkaAvailable(true)
	m.ObserveEventsIdentityConflict("deposit.observation.created")
}

// TestEventsRegistryManifest is the T003 registry-manifest test: every 013
// family is registered with the exact name, type and label set from
// verification.md §1.
func TestEventsRegistryManifest(t *testing.T) {
	m := New(func() bool { return true })
	observeAll013(m)

	families, err := m.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	byName := make(map[string]*dto.MetricFamily, len(families))
	for _, f := range families {
		byName[f.GetName()] = f
	}

	for _, want := range eventsManifest {
		f, ok := byName[want.name]
		if !ok {
			t.Errorf("missing 013 metric %q", want.name)
			continue
		}
		if f.GetType() != want.typ {
			t.Errorf("%s type = %s, want %s", want.name, f.GetType(), want.typ)
		}
		if len(f.GetMetric()) == 0 {
			t.Errorf("%s has no series after observation", want.name)
			continue
		}
		var got []string
		for _, lp := range f.GetMetric()[0].GetLabel() {
			got = append(got, lp.GetName())
		}
		sort.Strings(got)
		wantLabels := append([]string(nil), want.labels...)
		sort.Strings(wantLabels)
		if !reflect.DeepEqual(got, wantLabels) {
			t.Errorf("%s labels = %v, want %v", want.name, got, wantLabels)
		}
	}
}

// TestEventsLabelVocabulary checks that no 013 family grows a label outside
// the approved fixed vocabularies: event family/type, interface class,
// error/failure class, consumer name, partition, cache family and operation
// class. Credential/payload/identity content must never reach a label.
func TestEventsLabelVocabulary(t *testing.T) {
	allowed := map[string]bool{
		"event_family": true, "event_type": true, "interface_class": true,
		"error_class": true, "failure_class": true, "consumer_name": true,
		"partition": true, "family": true, "class": true, "op_class": true,
		"op_kind": true,
	}
	m := New(func() bool { return true })
	observeAll013(m)
	families, err := m.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	for _, f := range families {
		name := f.GetName()
		if !is013Metric(name) {
			continue
		}
		for _, metric := range f.GetMetric() {
			for _, lp := range metric.GetLabel() {
				if !allowed[lp.GetName()] {
					t.Errorf("%s carries unapproved label %q", name, lp.GetName())
				}
			}
		}
	}
}

func is013Metric(name string) bool {
	for _, entry := range eventsManifest {
		if entry.name == name {
			return true
		}
	}
	return false
}
