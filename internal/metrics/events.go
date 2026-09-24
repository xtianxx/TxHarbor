package metrics

import "github.com/prometheus/client_golang/prometheus"

// 013 reliable-event-infrastructure series (specs/013-reliable-event-infrastructure/
// verification.md §1, tasks.md T003). Contract names are unprefixed in the
// design documents; the exposition names below carry the txharbor_ prefix like
// every other custom series in this package. Mapping:
//
//	outbox_pending_count                -> txharbor_outbox_pending_count
//	outbox_pending_oldest_age_seconds   -> txharbor_outbox_pending_oldest_age_seconds
//	outbox_publish_failures_total       -> txharbor_outbox_publish_failures_total
//	outbox_published_total              -> txharbor_outbox_published_total
//	outbox_attempts_total               -> txharbor_outbox_attempts_total
//	outbox_blocked_count                -> txharbor_outbox_blocked_count
//	consumer_lag_seconds                -> txharbor_consumer_lag_seconds
//	consumer_lag_messages               -> txharbor_consumer_lag_messages
//	consumer_applied_total              -> txharbor_consumer_applied_total
//	consumer_retry_total                -> txharbor_consumer_retry_total
//	consumer_quarantine_total           -> txharbor_consumer_quarantine_total
//	consumer_catchup_seconds            -> txharbor_consumer_catchup_seconds
//	consumer_replay_clock               -> txharbor_consumer_replay_clock
//	event_replay_ops_total              -> txharbor_event_replay_ops_total
//	cache_hits_total                    -> txharbor_cache_hits_total
//	cache_misses_total                  -> txharbor_cache_misses_total
//	cache_fallback_total                -> txharbor_cache_fallback_total
//	cache_epoch_rotations_total         -> txharbor_cache_epoch_rotations_total
//	ratelimit_denied_total              -> txharbor_ratelimit_denied_total
//	ratelimit_unavailable               -> txharbor_ratelimit_unavailable
//	ratelimit_recovery_total            -> txharbor_ratelimit_recovery_total
//	rpc_budget_paused_total             -> txharbor_rpc_budget_paused_total
//	capacity_soft_breaches_total        -> txharbor_capacity_soft_breaches_total
//	capacity_hard_breaches_total        -> txharbor_capacity_hard_breaches_total
//	capacity_refusals_total             -> txharbor_capacity_refusals_total
//	redis_available                     -> txharbor_redis_available
//	kafka_available                     -> txharbor_kafka_available
//	events_identity_conflict_total      -> txharbor_events_identity_conflict_total
//
// Every label is a fixed low-cardinality vocabulary (event family/type,
// interface class, error/failure class, consumer name, partition) — no
// credential, payload byte, business identity value or caller content ever
// becomes a label (FR-24/FR-27; research R18).
const (
	OutboxPendingMetricName          = "txharbor_outbox_pending_count"
	OutboxPendingOldestAgeMetricName = "txharbor_outbox_pending_oldest_age_seconds"
	OutboxPublishFailuresMetricName  = "txharbor_outbox_publish_failures_total"
	OutboxPublishedMetricName        = "txharbor_outbox_published_total"
	OutboxAttemptsMetricName         = "txharbor_outbox_attempts_total"
	OutboxBlockedMetricName          = "txharbor_outbox_blocked_count"

	ConsumerLagSecondsMetricName     = "txharbor_consumer_lag_seconds"
	ConsumerLagMessagesMetricName    = "txharbor_consumer_lag_messages"
	ConsumerAppliedMetricName        = "txharbor_consumer_applied_total"
	ConsumerRetryMetricName          = "txharbor_consumer_retry_total"
	ConsumerQuarantineMetricName     = "txharbor_consumer_quarantine_total"
	ConsumerCatchupSecondsMetricName = "txharbor_consumer_catchup_seconds"
	ConsumerReplayClockMetricName    = "txharbor_consumer_replay_clock"
	EventReplayOpsMetricName         = "txharbor_event_replay_ops_total"

	CacheHitsMetricName           = "txharbor_cache_hits_total"
	CacheMissesMetricName         = "txharbor_cache_misses_total"
	CacheFallbackMetricName       = "txharbor_cache_fallback_total"
	CacheEpochRotationsMetricName = "txharbor_cache_epoch_rotations_total"

	RateLimitDeniedMetricName      = "txharbor_ratelimit_denied_total"
	RateLimitUnavailableMetricName = "txharbor_ratelimit_unavailable"
	RateLimitRecoveryMetricName    = "txharbor_ratelimit_recovery_total"
	RPCBudgetPausedMetricName      = "txharbor_rpc_budget_paused_total"

	CapacitySoftBreachesMetricName = "txharbor_capacity_soft_breaches_total"
	CapacityHardBreachesMetricName = "txharbor_capacity_hard_breaches_total"
	CapacityRefusalsMetricName     = "txharbor_capacity_refusals_total"

	RedisAvailableMetricName = "txharbor_redis_available"
	KafkaAvailableMetricName = "txharbor_kafka_available"

	EventsIdentityConflictMetricName = "txharbor_events_identity_conflict_total"
)

// eventsMetrics groups the 013 series. The series only appear once observed
// (Vec semantics), so a fresh registry carries no 013 noise.
type eventsMetrics struct {
	outboxPending          *prometheus.GaugeVec
	outboxPendingOldestAge *prometheus.GaugeVec
	outboxPublishFailures  *prometheus.CounterVec
	outboxPublished        *prometheus.CounterVec
	outboxAttempts         *prometheus.CounterVec
	outboxBlocked          *prometheus.GaugeVec

	consumerLagSeconds     *prometheus.GaugeVec
	consumerLagMessages    *prometheus.GaugeVec
	consumerApplied        *prometheus.CounterVec
	consumerRetry          *prometheus.CounterVec
	consumerQuarantine     *prometheus.CounterVec
	consumerCatchupSeconds *prometheus.GaugeVec
	consumerReplayClock    *prometheus.GaugeVec
	eventReplayOps         *prometheus.CounterVec

	cacheHits           *prometheus.CounterVec
	cacheMisses         *prometheus.CounterVec
	cacheFallback       *prometheus.CounterVec
	cacheEpochRotations *prometheus.CounterVec

	ratelimitDenied      *prometheus.CounterVec
	ratelimitUnavailable *prometheus.GaugeVec
	ratelimitRecovery    *prometheus.CounterVec
	rpcBudgetPaused      *prometheus.CounterVec

	capacitySoftBreaches *prometheus.CounterVec
	capacityHardBreaches *prometheus.CounterVec
	capacityRefusals     *prometheus.CounterVec

	redisAvailable *prometheus.GaugeVec
	kafkaAvailable *prometheus.GaugeVec

	eventsIdentityConflict *prometheus.CounterVec
}

// registerEvents builds the 013 series, registers them on the shared registry
// and returns the group. Called from New; kept here so metrics.go only gains
// one field and one call.
func registerEvents(registry *prometheus.Registry) eventsMetrics {
	e := eventsMetrics{
		outboxPending: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: OutboxPendingMetricName,
			Help: "Outbox events awaiting publication by event family (PG query; Redis never participates).",
		}, []string{"event_family"}),
		outboxPendingOldestAge: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: OutboxPendingOldestAgeMetricName,
			Help: "Age in seconds of the oldest pending outbox event by event family.",
		}, []string{"event_family"}),
		outboxPublishFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: OutboxPublishFailuresMetricName,
			Help: "Publisher failures by error class (transient/permanent/contract); permanent classes must alert.",
		}, []string{"error_class"}),
		outboxPublished: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: OutboxPublishedMetricName,
			Help: "Outbox events marked published after broker acknowledgement.",
		}, nil),
		outboxAttempts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: OutboxAttemptsMetricName,
			Help: "Outbox publish attempts; repeats are visible and absorbed by at-least-once delivery.",
		}, nil),
		outboxBlocked: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: OutboxBlockedMetricName,
			Help: "Outbox events permanently blocked and awaiting an audited unblock; never silently dropped.",
		}, nil),

		consumerLagSeconds: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: ConsumerLagSecondsMetricName,
			Help: "Consumer lag in seconds by consumer and partition (Kafka high-watermark minus position).",
		}, []string{"consumer_name", "partition"}),
		consumerLagMessages: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: ConsumerLagMessagesMetricName,
			Help: "Consumer lag in messages by consumer and partition.",
		}, []string{"consumer_name", "partition"}),
		consumerApplied: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: ConsumerAppliedMetricName,
			Help: "Events applied exactly once by the consumer (inbox-deduplicated effects).",
		}, nil),
		consumerRetry: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: ConsumerRetryMetricName,
			Help: "Bounded consumer retries by failure class.",
		}, []string{"failure_class"}),
		consumerQuarantine: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: ConsumerQuarantineMetricName,
			Help: "Events quarantined by failure class (retry_exhausted/non_retryable/version_gap/schema_unsupported/identity_mismatch).",
		}, []string{"failure_class"}),
		consumerCatchupSeconds: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: ConsumerCatchupSecondsMetricName,
			Help: "Catch-up time in seconds from recovery to lag below threshold; value is measured, never fabricated.",
		}, []string{"consumer_name"}),
		consumerReplayClock: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: ConsumerReplayClockMetricName,
			Help: "Unix timestamp of the last audited manual replay per consumer (automatic retries are excluded).",
		}, []string{"consumer_name"}),
		eventReplayOps: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: EventReplayOpsMetricName,
			Help: "Audited manual replay operations by op_kind and consumer; kept separate from automatic retries.",
		}, []string{"op_kind", "consumer_name"}),

		cacheHits: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: CacheHitsMetricName,
			Help: "Cache hits by family (non-authoritative read models only).",
		}, []string{"family"}),
		cacheMisses: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: CacheMissesMetricName,
			Help: "Cache misses by family; a miss reads through to PostgreSQL.",
		}, []string{"family"}),
		cacheFallback: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: CacheFallbackMetricName,
			Help: "Cache fallbacks by family: Redis unavailable/timeout/untrusted, direct PG read.",
		}, []string{"family"}),
		cacheEpochRotations: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: CacheEpochRotationsMetricName,
			Help: "Cache epoch rotations; a rotation makes pre-rotation keys unreachable (no stale values).",
		}, nil),

		ratelimitDenied: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: RateLimitDeniedMetricName,
			Help: "Rate-limit denials by interface class; never an authorization decision.",
		}, []string{"interface_class"}),
		ratelimitUnavailable: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: RateLimitUnavailableMetricName,
			Help: "Rate limiter unavailable: 1 while Redis failures make limiting impossible, else 0.",
		}, nil),
		ratelimitRecovery: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: RateLimitRecoveryMetricName,
			Help: "Smooth rate-limit recoveries by interface class (graded reopening, never instant).",
		}, []string{"interface_class"}),
		rpcBudgetPaused: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: RPCBudgetPausedMetricName,
			Help: "RPC call classes safely paused because only the distributed budget could keep them bounded.",
		}, []string{"class"}),

		capacitySoftBreaches: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: CapacitySoftBreachesMetricName,
			Help: "Outbox capacity soft-boundary breaches; new controllable writes are refused from here.",
		}, nil),
		capacityHardBreaches: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: CapacityHardBreachesMetricName,
			Help: "Outbox capacity hard-boundary arrivals (admission gate + pause trigger, not a physical limit).",
		}, nil),
		capacityRefusals: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: CapacityRefusalsMetricName,
			Help: "Capacity-guard refusals of new controllable work by operation class.",
		}, []string{"op_class"}),

		redisAvailable: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: RedisAvailableMetricName,
			Help: "Redis availability as a non-authoritative signal: 1 up, 0 down; never read by a funding gate.",
		}, nil),
		kafkaAvailable: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: KafkaAvailableMetricName,
			Help: "Kafka availability as a non-authoritative signal: 1 up, 0 down; never read by a funding gate.",
		}, nil),

		eventsIdentityConflict: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: EventsIdentityConflictMetricName,
			Help: "Same-identity different-content event conflicts by event type; must alert, never overwritten silently.",
		}, []string{"event_type"}),
	}
	registry.MustRegister(
		e.outboxPending, e.outboxPendingOldestAge, e.outboxPublishFailures,
		e.outboxPublished, e.outboxAttempts, e.outboxBlocked,
		e.consumerLagSeconds, e.consumerLagMessages, e.consumerApplied,
		e.consumerRetry, e.consumerQuarantine, e.consumerCatchupSeconds,
		e.consumerReplayClock, e.eventReplayOps,
		e.cacheHits, e.cacheMisses, e.cacheFallback, e.cacheEpochRotations,
		e.ratelimitDenied, e.ratelimitUnavailable, e.ratelimitRecovery,
		e.rpcBudgetPaused,
		e.capacitySoftBreaches, e.capacityHardBreaches, e.capacityRefusals,
		e.redisAvailable, e.kafkaAvailable, e.eventsIdentityConflict,
	)
	return e
}

// SetOutboxPending records the pending outbox count for one event family.
func (m *Metrics) SetOutboxPending(eventFamily string, count int) {
	m.events.outboxPending.WithLabelValues(eventFamily).Set(float64(count))
}

// SetOutboxPendingOldestAge records the oldest pending wait in seconds for
// one event family.
func (m *Metrics) SetOutboxPendingOldestAge(eventFamily string, seconds float64) {
	m.events.outboxPendingOldestAge.WithLabelValues(eventFamily).Set(seconds)
}

// ObserveOutboxPublishFailure counts one publish failure by error class
// (fixed taxonomy vocabulary; permanent classes alert).
func (m *Metrics) ObserveOutboxPublishFailure(errorClass string) {
	m.events.outboxPublishFailures.WithLabelValues(errorClass).Inc()
}

// ObserveOutboxPublished counts n events marked published after ack.
func (m *Metrics) ObserveOutboxPublished(n int) {
	if n <= 0 {
		return
	}
	m.events.outboxPublished.WithLabelValues().Add(float64(n))
}

// ObserveOutboxAttempts counts n publish attempts (repeats are expected).
func (m *Metrics) ObserveOutboxAttempts(n int) {
	if n <= 0 {
		return
	}
	m.events.outboxAttempts.WithLabelValues().Add(float64(n))
}

// SetOutboxBlocked records the count of permanently blocked events.
func (m *Metrics) SetOutboxBlocked(n int) {
	m.events.outboxBlocked.WithLabelValues().Set(float64(n))
}

// SetConsumerLag records both lag gauges for one consumer partition.
func (m *Metrics) SetConsumerLag(consumerName string, partition int, lagSeconds float64, lagMessages int64) {
	label := chainLabel(int64(partition))
	m.events.consumerLagSeconds.WithLabelValues(consumerName, label).Set(lagSeconds)
	m.events.consumerLagMessages.WithLabelValues(consumerName, label).Set(float64(lagMessages))
}

// ObserveConsumerApplied counts one exactly-once effect application.
func (m *Metrics) ObserveConsumerApplied() {
	m.events.consumerApplied.WithLabelValues().Inc()
}

// ObserveConsumerRetry counts one bounded retry by failure class.
func (m *Metrics) ObserveConsumerRetry(failureClass string) {
	m.events.consumerRetry.WithLabelValues(failureClass).Inc()
}

// ObserveConsumerQuarantine counts one quarantined event by failure class.
func (m *Metrics) ObserveConsumerQuarantine(failureClass string) {
	m.events.consumerQuarantine.WithLabelValues(failureClass).Inc()
}

// SetConsumerCatchupSeconds records the measured catch-up time for a consumer.
func (m *Metrics) SetConsumerCatchupSeconds(consumerName string, seconds float64) {
	m.events.consumerCatchupSeconds.WithLabelValues(consumerName).Set(seconds)
}

// SetConsumerReplayClock records the last audited manual replay timestamp
// (unix seconds) for a consumer.
func (m *Metrics) SetConsumerReplayClock(consumerName string, unixSeconds float64) {
	m.events.consumerReplayClock.WithLabelValues(consumerName).Set(unixSeconds)
}

// ObserveEventReplay counts one audited manual replay operation.
func (m *Metrics) ObserveEventReplay(opKind, consumerName string) {
	m.events.eventReplayOps.WithLabelValues(opKind, consumerName).Inc()
}

// ObserveCacheHit counts one cache hit by family.
func (m *Metrics) ObserveCacheHit(family string) {
	m.events.cacheHits.WithLabelValues(family).Inc()
}

// ObserveCacheMiss counts one cache miss by family.
func (m *Metrics) ObserveCacheMiss(family string) {
	m.events.cacheMisses.WithLabelValues(family).Inc()
}

// ObserveCacheFallback counts one fallback (direct PG read) by family.
func (m *Metrics) ObserveCacheFallback(family string) {
	m.events.cacheFallback.WithLabelValues(family).Inc()
}

// ObserveCacheEpochRotation counts one cache epoch rotation.
func (m *Metrics) ObserveCacheEpochRotation() {
	m.events.cacheEpochRotations.WithLabelValues().Inc()
}

// ObserveRateLimitDenied counts one limiter denial by interface class.
func (m *Metrics) ObserveRateLimitDenied(interfaceClass string) {
	m.events.ratelimitDenied.WithLabelValues(interfaceClass).Inc()
}

// SetRateLimitUnavailable records whether the limiter is currently unable to
// make decisions (PD-1: new withdrawal creation fails closed from here).
func (m *Metrics) SetRateLimitUnavailable(unavailable bool) {
	if unavailable {
		m.events.ratelimitUnavailable.WithLabelValues().Set(1)
		return
	}
	m.events.ratelimitUnavailable.WithLabelValues().Set(0)
}

// ObserveRateLimitRecovery counts one smooth limiter recovery per class.
func (m *Metrics) ObserveRateLimitRecovery(interfaceClass string) {
	m.events.ratelimitRecovery.WithLabelValues(interfaceClass).Inc()
}

// ObserveRPCBudgetPaused counts one safely paused RPC call class.
func (m *Metrics) ObserveRPCBudgetPaused(class string) {
	m.events.rpcBudgetPaused.WithLabelValues(class).Inc()
}

// ObserveCapacitySoftBreach counts one soft-boundary breach.
func (m *Metrics) ObserveCapacitySoftBreach() {
	m.events.capacitySoftBreaches.WithLabelValues().Inc()
}

// ObserveCapacityHardBreach counts one hard-boundary arrival.
func (m *Metrics) ObserveCapacityHardBreach() {
	m.events.capacityHardBreaches.WithLabelValues().Inc()
}

// ObserveCapacityRefusal counts one refusal of new controllable work by
// operation class.
func (m *Metrics) ObserveCapacityRefusal(opClass string) {
	m.events.capacityRefusals.WithLabelValues(opClass).Inc()
}

// SetRedisAvailable records the non-authoritative Redis health signal.
func (m *Metrics) SetRedisAvailable(available bool) {
	if available {
		m.events.redisAvailable.WithLabelValues().Set(1)
		return
	}
	m.events.redisAvailable.WithLabelValues().Set(0)
}

// SetKafkaAvailable records the non-authoritative Kafka health signal.
func (m *Metrics) SetKafkaAvailable(available bool) {
	if available {
		m.events.kafkaAvailable.WithLabelValues().Set(1)
		return
	}
	m.events.kafkaAvailable.WithLabelValues().Set(0)
}

// ObserveEventsIdentityConflict counts one same-identity different-content
// conflict by event type (must alert).
func (m *Metrics) ObserveEventsIdentityConflict(eventType string) {
	m.events.eventsIdentityConflict.WithLabelValues(eventType).Inc()
}
