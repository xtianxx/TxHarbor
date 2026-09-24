package config

import (
	"strings"
	"testing"
	"time"
)

// eventsEnv returns the minimal complete 013 runtime configuration: the
// base 001–011 environment plus every required 013 knob. Values are test
// inputs, not proposed production thresholds (spec forbids inventing those).
func eventsEnv() map[string]string {
	env := baseEnv()
	env[EnvEventsEnabled] = "true"
	env[EnvKafkaBrokers] = "127.0.0.1:9092,127.0.0.1:9093"
	env[EnvRedisAddr] = "127.0.0.1:6379"
	env[EnvRateLimitNewWithdrawal] = "10/20"
	env[EnvRateLimitWrite] = "50/100"
	env[EnvRateLimitQuery] = "100/200"
	env[EnvRateLimitOperator] = "5/10"
	env[EnvRateLimitRPC] = "20/40"
	env[EnvEventsCapacitySoftLimit] = "1000"
	env[EnvEventsCapacityHardLimit] = "2000"
	env[EnvEventsCapacityReserve] = "100"
	env[EnvEventsCapacityRetention] = "24h"
	env[EnvEventsCapacityMaxShutdown] = "1h"
	env[EnvEventsCapacityDrainTarget] = "30m"
	return env
}

// TestLoadEvents013DisabledKeepsExistingSemantics pins that the 013 addition
// is invisible while TXHARBOR_EVENTS_ENABLED is off: baseEnv loads unchanged,
// technical defaults are pre-populated, and thresholds stay zero (no invented
// defaults), and the startup summary line is not extended.
func TestLoadEvents013DisabledKeepsExistingSemantics(t *testing.T) {
	cfg, err := Load(fakeEnv(baseEnv()))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Events.Enabled {
		t.Fatal("Events.Enabled = true, want false by default")
	}
	if cfg.Events.Publisher.Batch != DefaultEventsPublisherBatch ||
		cfg.Events.Publisher.PollInterval != DefaultEventsPublisherPollInterval ||
		cfg.Events.Publisher.LeaseTTL != DefaultEventsPublisherLeaseTTL ||
		cfg.Events.Publisher.BackoffBase != DefaultEventsPublisherBackoffBase ||
		cfg.Events.Publisher.BackoffMax != DefaultEventsPublisherBackoffMax {
		t.Errorf("publisher defaults = %+v, want the annotated initial values", cfg.Events.Publisher)
	}
	if cfg.Events.Consumer.Batch != DefaultEventsConsumerBatch ||
		cfg.Events.Consumer.BackoffBase != DefaultEventsConsumerBackoffBase ||
		cfg.Events.Consumer.BackoffMax != DefaultEventsConsumerBackoffMax ||
		cfg.Events.Consumer.RetryLimit != DefaultEventsConsumerRetryLimit ||
		cfg.Events.Consumer.GapWait != DefaultEventsConsumerGapWait {
		t.Errorf("consumer defaults = %+v, want the annotated initial values", cfg.Events.Consumer)
	}
	if cfg.Redis.Timeout != DefaultRedisTimeout || cfg.Redis.CacheTTL != DefaultRedisCacheTTL ||
		cfg.Redis.CacheEpoch != DefaultRedisCacheEpoch {
		t.Errorf("redis defaults = %+v, want annotated initial values", cfg.Redis)
	}
	if cfg.Kafka.Topic != DefaultKafkaTopic || cfg.Kafka.ConsumerGroupPrefix != DefaultKafkaConsumerGroupPrefix {
		t.Errorf("kafka defaults = %+v, want documented defaults", cfg.Kafka)
	}
	if cfg.Redis.Addr != "" || len(cfg.Kafka.Brokers) != 0 {
		t.Errorf("redis addr / kafka brokers = %q/%v, want empty while disabled", cfg.Redis.Addr, cfg.Kafka.Brokers)
	}
	if (cfg.Capacity != CapacityConfig{}) {
		t.Errorf("Capacity = %+v, want zero while disabled (no invented thresholds)", cfg.Capacity)
	}
	if (cfg.RateLimit != RateLimitConfig{}) {
		t.Errorf("RateLimit = %+v, want zero while disabled", cfg.RateLimit)
	}
	if (cfg.Events.Alerts != AlertsConfig{}) {
		t.Errorf("Events.Alerts = %+v, want zero while disabled", cfg.Events.Alerts)
	}
	if strings.Contains(cfg.Summary(), "events_enabled") {
		t.Errorf("summary %q must not carry the 013 block while disabled", cfg.Summary())
	}
}

// TestLoadEvents013EnabledParsesFullSet proves the complete runtime set parses
// and reaches the typed config.
func TestLoadEvents013EnabledParsesFullSet(t *testing.T) {
	env := eventsEnv()
	env[EnvEventsPublisherBatch] = "7"
	env[EnvEventsPublisherPollInterval] = "2s"
	env[EnvEventsPublisherLeaseTTL] = "45s"
	env[EnvEventsPublisherBackoffBase] = "2s"
	env[EnvEventsPublisherBackoffMax] = "90s"
	env[EnvEventsConsumerBatch] = "9"
	env[EnvEventsConsumerRetryLimit] = "3"
	env[EnvEventsConsumerGapWait] = "5s"
	env[EnvRedisTimeout] = "250ms"
	env[EnvRedisCacheTTL] = "1m"
	env[EnvRedisCacheEpoch] = "epoch-a"
	env[EnvKafkaTopic] = "txharbor.events.test"
	env[EnvKafkaConsumerGroupPrefix] = "txharbor.test"

	cfg, err := Load(fakeEnv(env))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !cfg.Events.Enabled {
		t.Fatal("Events.Enabled = false, want true")
	}
	if cfg.Events.Publisher.Batch != 7 || cfg.Events.Publisher.PollInterval != 2*time.Second ||
		cfg.Events.Publisher.LeaseTTL != 45*time.Second || cfg.Events.Publisher.BackoffBase != 2*time.Second ||
		cfg.Events.Publisher.BackoffMax != 90*time.Second {
		t.Errorf("publisher = %+v", cfg.Events.Publisher)
	}
	if cfg.Events.Consumer.Batch != 9 || cfg.Events.Consumer.RetryLimit != 3 ||
		cfg.Events.Consumer.GapWait != 5*time.Second {
		t.Errorf("consumer = %+v", cfg.Events.Consumer)
	}
	if cfg.Redis.Addr != "127.0.0.1:6379" || cfg.Redis.Timeout != 250*time.Millisecond ||
		cfg.Redis.CacheTTL != time.Minute || cfg.Redis.CacheEpoch != "epoch-a" {
		t.Errorf("redis = %+v", cfg.Redis)
	}
	if len(cfg.Kafka.Brokers) != 2 || cfg.Kafka.Brokers[0] != "127.0.0.1:9092" ||
		cfg.Kafka.Topic != "txharbor.events.test" || cfg.Kafka.ConsumerGroupPrefix != "txharbor.test" {
		t.Errorf("kafka = %+v", cfg.Kafka)
	}
	if cfg.RateLimit.NewWithdrawal != (RateLimitClassConfig{RatePerSecond: 10, Burst: 20}) ||
		cfg.RateLimit.RPC != (RateLimitClassConfig{RatePerSecond: 20, Burst: 40}) {
		t.Errorf("rate limit = %+v", cfg.RateLimit)
	}
	if cfg.Capacity.SoftLimit != 1000 || cfg.Capacity.HardLimit != 2000 || cfg.Capacity.Reserve != 100 ||
		cfg.Capacity.Retention != 24*time.Hour || cfg.Capacity.MaxShutdownWindow != time.Hour ||
		cfg.Capacity.DrainTargetWindow != 30*time.Minute {
		t.Errorf("capacity = %+v", cfg.Capacity)
	}
	for _, want := range []string{"events_enabled=true", "kafka_topic=txharbor.events.test", "capacity_soft=1000",
		"alerts_enabled=true"} {
		if !strings.Contains(cfg.Summary(), want) {
			t.Errorf("summary %q lacks %q", cfg.Summary(), want)
		}
	}
}

// TestLoadEvents013AlertKeys pins the T075 alert wiring keys: the alert switch
// defaults on while the feature is enabled (observability only), an explicit
// false disables it, and the optional sustained-window override parses while
// its invalid forms are refused fail-closed.
func TestLoadEvents013AlertKeys(t *testing.T) {
	cfg, err := Load(fakeEnv(eventsEnv()))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !cfg.Events.Alerts.Enabled {
		t.Error("Alerts.Enabled = false, want the default on while events are enabled")
	}
	if cfg.Events.Alerts.SoftSustainedWindow != 0 {
		t.Errorf("SoftSustainedWindow = %v, want zero (derive from the capacity drain window)", cfg.Events.Alerts.SoftSustainedWindow)
	}

	env := eventsEnv()
	env[EnvEventsAlertEnabled] = "false"
	env[EnvEventsAlertSoftSustainedWindow] = "90s"
	cfg, err = Load(fakeEnv(env))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Events.Alerts.Enabled {
		t.Error("Alerts.Enabled = true, want the explicit false")
	}
	if cfg.Events.Alerts.SoftSustainedWindow != 90*time.Second {
		t.Errorf("SoftSustainedWindow = %v, want 90s", cfg.Events.Alerts.SoftSustainedWindow)
	}
}

// TestLoadEvents013FailClosed is the table-driven refusal matrix: every
// missing required knob, malformed value and illegal combination must refuse
// startup while the feature is enabled (T001 completion condition).
func TestLoadEvents013FailClosed(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(env map[string]string)
	}{
		{"non-boolean switch", func(env map[string]string) { env[EnvEventsEnabled] = "yes" }},
		{"missing kafka brokers", func(env map[string]string) { delete(env, EnvKafkaBrokers) }},
		{"broker without port", func(env map[string]string) { env[EnvKafkaBrokers] = "127.0.0.1" }},
		{"blank broker entry", func(env map[string]string) { env[EnvKafkaBrokers] = "127.0.0.1:9092,,127.0.0.1:9093" }},
		{"topic with whitespace", func(env map[string]string) { env[EnvKafkaTopic] = "txharbor events" }},
		{"missing redis addr", func(env map[string]string) { delete(env, EnvRedisAddr) }},
		{"redis addr without port", func(env map[string]string) { env[EnvRedisAddr] = "127.0.0.1" }},
		{"redis addr port zero", func(env map[string]string) { env[EnvRedisAddr] = "127.0.0.1:0" }},
		{"epoch with whitespace", func(env map[string]string) { env[EnvRedisCacheEpoch] = "a b" }},
		{"missing new-withdrawal rate", func(env map[string]string) { delete(env, EnvRateLimitNewWithdrawal) }},
		{"missing query rate", func(env map[string]string) { delete(env, EnvRateLimitQuery) }},
		{"missing rpc rate", func(env map[string]string) { delete(env, EnvRateLimitRPC) }},
		{"rate without burst", func(env map[string]string) { env[EnvRateLimitWrite] = "10" }},
		{"zero burst", func(env map[string]string) { env[EnvRateLimitWrite] = "10/0" }},
		{"non-numeric rate", func(env map[string]string) { env[EnvRateLimitOperator] = "fast/10" }},
		{"missing soft limit", func(env map[string]string) { delete(env, EnvEventsCapacitySoftLimit) }},
		{"missing hard limit", func(env map[string]string) { delete(env, EnvEventsCapacityHardLimit) }},
		{"missing reserve", func(env map[string]string) { delete(env, EnvEventsCapacityReserve) }},
		{"reserve not below soft", func(env map[string]string) { env[EnvEventsCapacityReserve] = "1000" }},
		{"soft not below hard", func(env map[string]string) { env[EnvEventsCapacityHardLimit] = "1000" }},
		{"missing retention", func(env map[string]string) { delete(env, EnvEventsCapacityRetention) }},
		{"missing max shutdown window", func(env map[string]string) { delete(env, EnvEventsCapacityMaxShutdown) }},
		{"missing drain target window", func(env map[string]string) { delete(env, EnvEventsCapacityDrainTarget) }},
		{"zero retention", func(env map[string]string) { env[EnvEventsCapacityRetention] = "0s" }},
		{"publisher backoff max below base", func(env map[string]string) {
			env[EnvEventsPublisherBackoffBase] = "5s"
			env[EnvEventsPublisherBackoffMax] = "1s"
		}},
		{"consumer retry limit zero", func(env map[string]string) { env[EnvEventsConsumerRetryLimit] = "0" }},
		{"publisher batch zero", func(env map[string]string) { env[EnvEventsPublisherBatch] = "0" }},
		{"alert switch not boolean", func(env map[string]string) { env[EnvEventsAlertEnabled] = "yes" }},
		{"alert sustained window zero", func(env map[string]string) { env[EnvEventsAlertSoftSustainedWindow] = "0s" }},
		{"alert sustained window malformed", func(env map[string]string) { env[EnvEventsAlertSoftSustainedWindow] = "soon" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := eventsEnv()
			tc.mutate(env)
			if _, err := Load(fakeEnv(env)); err == nil {
				t.Fatalf("Load() = nil, want refusal for %s", tc.name)
			}
		})
	}
}

// TestLoadEvents013PartialConfigRefusedWhileDisabled proves the fail-closed
// guard also rejects half-configured 013 blocks while the switch is off: a
// present-but-partial capacity set is a configuration error, never a silent
// no-op, and malformed present values are still refused.
func TestLoadEvents013PartialConfigRefusedWhileDisabled(t *testing.T) {
	env := baseEnv()
	env[EnvEventsCapacitySoftLimit] = "1000"
	if _, err := Load(fakeEnv(env)); err == nil {
		t.Fatal("Load() = nil, want refusal for a partial capacity set while disabled")
	}

	env = baseEnv()
	env[EnvKafkaBrokers] = "not-a-broker"
	if _, err := Load(fakeEnv(env)); err == nil {
		t.Fatal("Load() = nil, want refusal for a malformed broker while disabled")
	}
}
