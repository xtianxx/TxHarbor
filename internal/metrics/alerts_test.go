package metrics

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/xtianxx/txharbor/internal/logx"
)

// alertTestConfig is the fixed test configuration. The numbers are test
// inputs (soft < hard with a short window), never proposed production
// thresholds.
func alertTestConfig() AlertConfig {
	return AlertConfig{
		Enabled:           true,
		SoftLimit:         50,
		HardLimit:         100,
		DrainTargetWindow: 10 * time.Second,
	}
}

// newTestAlerter builds a Metrics registry, an Alerter over it and a fake
// clock the test drives.
func newTestAlerter(t *testing.T, cfg AlertConfig, logger *slog.Logger) (*Metrics, *Alerter, *time.Time) {
	t.Helper()
	m := New(func() bool { return true })
	a := NewAlerter(cfg, m.Gatherer(), logger)
	now := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	a.now = func() time.Time { return now }
	return m, a, &now
}

// TestAlertCatalogThresholdsAndSources is the T075 evidence check: every rule
// in the catalog carries a non-empty condition, threshold and threshold
// source, the numeric thresholds mirror the configuration, and only the hard
// boundary is P1 (contracts/capacity.md §5.6).
func TestAlertCatalogThresholdsAndSources(t *testing.T) {
	cfg := alertTestConfig()
	catalog := AlertCatalog(cfg)
	if len(catalog) != 5 {
		t.Fatalf("catalog rules = %d, want 5", len(catalog))
	}
	seen := map[AlertRuleID]bool{}
	p1 := 0
	for _, rule := range catalog {
		if rule.ID == "" || rule.Condition == "" || rule.Threshold == "" || rule.ThresholdSource == "" {
			t.Errorf("rule %+v has an empty field", rule)
		}
		if !strings.Contains(rule.ThresholdSource, "config:") && !strings.Contains(rule.ThresholdSource, "ruling:") {
			t.Errorf("rule %s source %q lacks a provenance marker", rule.ID, rule.ThresholdSource)
		}
		if seen[rule.ID] {
			t.Errorf("rule %s duplicated", rule.ID)
		}
		seen[rule.ID] = true
		if rule.Severity == AlertP1 {
			p1++
			if rule.ID != AlertRuleHardReached {
				t.Errorf("rule %s is P1; only the hard boundary is P1 by ruling", rule.ID)
			}
		}
	}
	if p1 != 1 {
		t.Errorf("P1 rules = %d, want exactly 1", p1)
	}
	// The numeric thresholds must mirror the configured values, never a
	// hardcoded number.
	byID := map[AlertRuleID]AlertRule{}
	for _, rule := range catalog {
		byID[rule.ID] = rule
	}
	if got := byID[AlertRuleSoftSustained].Threshold; !strings.Contains(got, "soft_limit=50") ||
		!strings.Contains(got, "sustained_window=10s") {
		t.Errorf("soft rule threshold = %q, want the configured soft limit and window", got)
	}
	if got := byID[AlertRuleHardReached].Threshold; !strings.Contains(got, "hard_limit=100") {
		t.Errorf("hard rule threshold = %q, want the configured hard limit", got)
	}

	// The optional override switches the window source annotation.
	override := cfg
	override.SoftSustainedWindow = 3 * time.Minute
	over := AlertCatalog(override)
	if got := over[0].ThresholdSource; !strings.Contains(got, "ALERT_SOFT_SUSTAINED_WINDOW") {
		t.Errorf("override source = %q, want the override key named", got)
	}
	if got := over[0].Threshold; !strings.Contains(got, "sustained_window=3m0s") {
		t.Errorf("override threshold = %q, want the override window", got)
	}
}

// TestAlertSoftSustainedWindow proves the sustained-soft condition: it fires
// only after the configured window has continuously held, fires once per
// episode and re-arms after recovery.
func TestAlertSoftSustainedWindow(t *testing.T) {
	m, a, now := newTestAlerter(t, alertTestConfig(), slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)))
	ctx := context.Background()

	m.SetOutboxPending("deposit_observation", 60)
	if alerts, err := a.Evaluate(ctx, AlertObservation{}); err != nil || len(alerts) != 0 {
		t.Fatalf("first evaluation = %v/%v, want none (window not elapsed)", alerts, err)
	}
	*now = now.Add(9 * time.Second)
	if alerts, _ := a.Evaluate(ctx, AlertObservation{}); len(alerts) != 0 {
		t.Fatalf("evaluation before the window fired %d alerts", len(alerts))
	}
	*now = now.Add(1 * time.Second)
	alerts, err := a.Evaluate(ctx, AlertObservation{})
	if err != nil || len(alerts) != 1 {
		t.Fatalf("evaluation at the window = %v/%v, want one alert", alerts, err)
	}
	if alerts[0].Rule != AlertRuleSoftSustained || alerts[0].Severity != AlertWarning {
		t.Errorf("alert = %+v, want the warning soft-sustained rule", alerts[0])
	}
	if !strings.Contains(alerts[0].Threshold, "soft_limit=50") {
		t.Errorf("alert threshold = %q, want the configured soft limit", alerts[0].Threshold)
	}
	if !strings.Contains(alerts[0].Observed, "pending=60") {
		t.Errorf("alert observed = %q, want the observed pending", alerts[0].Observed)
	}
	if alerts[0].Fields["event_families"] != "deposit_observation=60" {
		t.Errorf("alert families = %q, want the pending breakdown", alerts[0].Fields["event_families"])
	}
	*now = now.Add(30 * time.Second)
	if alerts, _ := a.Evaluate(ctx, AlertObservation{}); len(alerts) != 0 {
		t.Fatalf("episode fired %d duplicate alerts", len(alerts))
	}

	// Recovery resets the episode; a new sustained window alerts again.
	m.SetOutboxPending("deposit_observation", 10)
	if alerts, _ := a.Evaluate(ctx, AlertObservation{}); len(alerts) != 0 {
		t.Fatalf("recovery fired %d alerts", len(alerts))
	}
	m.SetOutboxPending("deposit_observation", 60)
	if alerts, _ := a.Evaluate(ctx, AlertObservation{}); len(alerts) != 0 {
		t.Fatalf("new episode fired %d alerts before the window", len(alerts))
	}
	*now = now.Add(10 * time.Second)
	if alerts, _ := a.Evaluate(ctx, AlertObservation{}); len(alerts) != 1 {
		t.Fatalf("new episode = %d alerts, want one", len(alerts))
	}
}

// TestAlertHardImmediate proves the hard boundary fires P1 immediately, once
// per episode.
func TestAlertHardImmediate(t *testing.T) {
	m, a, now := newTestAlerter(t, alertTestConfig(), slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)))
	ctx := context.Background()

	m.SetOutboxPending("deposit_observation", 100)
	alerts, err := a.Evaluate(ctx, AlertObservation{})
	if err != nil || len(alerts) != 1 {
		t.Fatalf("hard evaluation = %v/%v, want one immediate alert", alerts, err)
	}
	if alerts[0].Rule != AlertRuleHardReached || alerts[0].Severity != AlertP1 {
		t.Errorf("alert = %+v, want the P1 hard rule", alerts[0])
	}
	if !strings.Contains(alerts[0].Threshold, "hard_limit=100") {
		t.Errorf("threshold = %q, want the configured hard limit", alerts[0].Threshold)
	}
	if alerts, _ := a.Evaluate(ctx, AlertObservation{}); len(alerts) != 0 {
		t.Fatalf("hard episode fired %d duplicates", len(alerts))
	}
	m.SetOutboxPending("deposit_observation", 10)
	if alerts, _ := a.Evaluate(ctx, AlertObservation{}); len(alerts) != 0 {
		t.Fatalf("recovery fired %d alerts", len(alerts))
	}
	m.SetOutboxPending("deposit_observation", 150)
	if alerts, _ := a.Evaluate(ctx, AlertObservation{}); len(alerts) != 1 {
		t.Fatalf("new hard episode = %d alerts, want one", len(alerts))
	}
	_ = now
}

// TestAlertOutboxBlockedEpisode proves a permanently blocked event alerts
// immediately, once per episode.
func TestAlertOutboxBlockedEpisode(t *testing.T) {
	m, a, _ := newTestAlerter(t, alertTestConfig(), slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)))
	ctx := context.Background()

	m.SetOutboxBlocked(1)
	alerts, err := a.Evaluate(ctx, AlertObservation{})
	if err != nil || len(alerts) != 1 || alerts[0].Rule != AlertRuleOutboxBlocked {
		t.Fatalf("blocked evaluation = %v/%v, want one blocked alert", alerts, err)
	}
	if alerts, _ := a.Evaluate(ctx, AlertObservation{}); len(alerts) != 0 {
		t.Fatalf("blocked episode fired %d duplicates", len(alerts))
	}
	m.SetOutboxBlocked(0)
	if alerts, _ := a.Evaluate(ctx, AlertObservation{}); len(alerts) != 0 {
		t.Fatalf("cleared block fired %d alerts", len(alerts))
	}
	m.SetOutboxBlocked(2)
	if alerts, _ := a.Evaluate(ctx, AlertObservation{}); len(alerts) != 1 {
		t.Fatalf("new blocked episode = %d alerts, want one", len(alerts))
	}
}

// TestAlertIdentityConflictIncrease proves an identity conflict alerts on the
// increase (not on the baseline) and carries the event type.
func TestAlertIdentityConflictIncrease(t *testing.T) {
	m, a, _ := newTestAlerter(t, alertTestConfig(), slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)))
	ctx := context.Background()

	if alerts, _ := a.Evaluate(ctx, AlertObservation{}); len(alerts) != 0 {
		t.Fatalf("baseline fired %d alerts", len(alerts))
	}
	m.ObserveEventsIdentityConflict("deposit.observation.created")
	alerts, err := a.Evaluate(ctx, AlertObservation{})
	if err != nil || len(alerts) != 1 || alerts[0].Rule != AlertRuleIdentityConflict {
		t.Fatalf("conflict evaluation = %v/%v, want one conflict alert", alerts, err)
	}
	if alerts[0].Fields["event_type"] != "deposit.observation.created" {
		t.Errorf("alert fields = %v, want the event type", alerts[0].Fields)
	}
	if alerts, _ := a.Evaluate(ctx, AlertObservation{}); len(alerts) != 0 {
		t.Fatalf("unchanged counter fired %d alerts", len(alerts))
	}
	m.ObserveEventsIdentityConflict("deposit.observation.created")
	if alerts, _ := a.Evaluate(ctx, AlertObservation{}); len(alerts) != 1 {
		t.Fatalf("second conflict = %d alerts, want one", len(alerts))
	}
}

// TestAlertQuarantineIncrease proves a new quarantine entry alerts and carries
// the failure class.
func TestAlertQuarantineIncrease(t *testing.T) {
	m, a, _ := newTestAlerter(t, alertTestConfig(), slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)))
	ctx := context.Background()

	if alerts, _ := a.Evaluate(ctx, AlertObservation{}); len(alerts) != 0 {
		t.Fatalf("baseline fired %d alerts", len(alerts))
	}
	m.ObserveConsumerQuarantine("retry_exhausted")
	alerts, err := a.Evaluate(ctx, AlertObservation{})
	if err != nil || len(alerts) != 1 || alerts[0].Rule != AlertRuleQuarantineAdded {
		t.Fatalf("quarantine evaluation = %v/%v, want one quarantine alert", alerts, err)
	}
	if alerts[0].Fields["failure_class"] != "retry_exhausted" {
		t.Errorf("alert fields = %v, want the failure class", alerts[0].Fields)
	}
	if alerts, _ := a.Evaluate(ctx, AlertObservation{}); len(alerts) != 0 {
		t.Fatalf("unchanged quarantine counter fired %d alerts", len(alerts))
	}
}

// TestAlertDisabledEmitsNothing pins that the observability switch never
// fabricates an alert and never gates anything.
func TestAlertDisabledEmitsNothing(t *testing.T) {
	cfg := alertTestConfig()
	cfg.Enabled = false
	m, a, _ := newTestAlerter(t, cfg, slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)))
	ctx := context.Background()
	m.SetOutboxPending("deposit_observation", 150)
	m.SetOutboxBlocked(3)
	m.ObserveEventsIdentityConflict("deposit.observation.created")
	m.ObserveConsumerQuarantine("retry_exhausted")
	alerts, err := a.Evaluate(ctx, AlertObservation{})
	if err != nil {
		t.Fatalf("disabled evaluate error = %v", err)
	}
	if len(alerts) != 0 {
		t.Fatalf("disabled evaluate fired %d alerts", len(alerts))
	}
}

// TestAlertMissingSeriesIsNotFabricated proves an empty registry evaluates
// cleanly: an absent series is "not observed", never an invented zero.
func TestAlertMissingSeriesIsNotFabricated(t *testing.T) {
	a := NewAlerter(alertTestConfig(), prometheus.NewRegistry(), slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)))
	alerts, err := a.Evaluate(context.Background(), AlertObservation{})
	if err != nil {
		t.Fatalf("empty registry error = %v", err)
	}
	if len(alerts) != 0 {
		t.Fatalf("empty registry fired %d alerts", len(alerts))
	}
}

// TestAlertLogCarriesIdentityAndRedactsSecrets is the T075 redaction scan: an
// alert record carries the structured identity context (event/chain/business
// identity, attempts) and credential-shaped values are redacted by logx.
func TestAlertLogCarriesIdentityAndRedactsSecrets(t *testing.T) {
	var buf bytes.Buffer
	m, a, _ := newTestAlerter(t, alertTestConfig(), slog.New(slog.NewTextHandler(&buf, nil)))
	ctx := context.Background()

	m.SetOutboxBlocked(1)
	secrets := []string{"hunter2", "super-secret-token", "0xdeadbeefcafe"}
	alerts, err := a.Emit(ctx, AlertObservation{
		EventType:        "withdrawal.request.received",
		EventFamily:      "withdrawal",
		ChainID:          "31337",
		BusinessIdentity: "postgres://txharbor:hunter2@127.0.0.1:5432/txharbor",
		Attempts:         3,
	})
	if err != nil || len(alerts) != 1 {
		t.Fatalf("emit = %v/%v, want one alert", alerts, err)
	}
	out := buf.String()
	for _, field := range []string{"event_type=withdrawal.request.received", "chain_id=31337", "attempts=3",
		"alert_rule=outbox_blocked", "threshold_source="} {
		if !strings.Contains(out, field) {
			t.Errorf("alert record lacks %q: %s", field, out)
		}
	}
	if !strings.Contains(out, logx.Redacted) {
		t.Errorf("alert record lacks the redaction placeholder: %s", out)
	}
	for _, secret := range secrets {
		if strings.Contains(out, secret) {
			t.Errorf("alert record leaked %q: %s", secret, out)
		}
	}
	// The bearer form is redacted too.
	buf.Reset()
	_, _ = a.Emit(ctx, AlertObservation{BusinessIdentity: "Authorization: Bearer super-secret-token"})
	if strings.Contains(buf.String(), "super-secret-token") {
		t.Errorf("alert record leaked a bearer token: %s", buf.String())
	}
}

// TestAlertObservedValuesAreRedacted pins that a credential-shaped observed
// value cannot reach the record verbatim even when a caller builds it from
// data.
func TestAlertObservedValuesAreRedacted(t *testing.T) {
	var buf bytes.Buffer
	m, a, _ := newTestAlerter(t, alertTestConfig(), slog.New(slog.NewTextHandler(&buf, nil)))
	m.SetOutboxPending("deposit_observation", 100)
	alerts, err := a.Evaluate(context.Background(), AlertObservation{})
	if err != nil || len(alerts) != 1 {
		t.Fatalf("evaluate = %v/%v", alerts, err)
	}
	// Re-log a tampered observed value through the same funnel.
	alert := alerts[0]
	alert.Observed = "dsn=postgres://txharbor:hunter2@127.0.0.1:5432/txharbor"
	a.log(context.Background(), alert)
	if strings.Contains(buf.String(), "hunter2") {
		t.Fatalf("observed value leaked a credential: %s", buf.String())
	}
}
