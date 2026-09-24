// alerts.go owns the 013 alert wiring (T075; FR-24; verification.md §1;
// contracts/capacity.md §5.6). It defines the fixed alert catalog with each
// rule's threshold provenance and a stateful evaluator that turns the 013
// metric series into structured alert records.
//
// Discipline (MUST NOT be weakened):
//
//   - every numeric threshold comes from configuration (capacity soft/hard
//     limits and the drain target window; optionally the sustained-window
//     override). Nothing is hardcoded and no business number is invented:
//     "any increase" rules carry an explicit ruling source instead of a made-up
//     count;
//   - alert emission is observability only. Disabling it never changes a gate,
//     a refusal, a delivery decision or any financial state;
//   - alert log records carry the structured identity context (event type,
//     chain, business identity, attempts) and every string value passes
//     through logx.Redact: no key, credential, token or raw signature material
//     can reach a record (FR-24/FR-27);
//   - the evaluator never mutates the metrics registry and never writes to the
//     database; it only reads the exposition.
//
// Assembly seam (T075 file scope): NewAlerter takes any prometheus.Gatherer, so
// a process that owns a registry (today the serve process) can evaluate the
// catalog on its own cadence and log the records. Installing the identity
// conflict observer into the producer path and starting the periodic
// evaluation loop belong to the runtime wiring files (internal/app), which are
// owned by their own single-writer chains; this file deliberately changes
// neither.
package metrics

import (
	"context"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/xtianxx/txharbor/internal/logx"
)

// AlertSeverity is the closed severity vocabulary. Only the hard-boundary rule
// carries P1: contracts/capacity.md §5.6 rules "到达 hard 立即告警（P1）"
// explicitly; the remaining rules alert at warning severity and any escalation
// is an operational runbook decision, not a hardcoded one.
type AlertSeverity string

const (
	// AlertP1 is the immediate page-level severity (hard boundary only).
	AlertP1 AlertSeverity = "P1"
	// AlertWarning is the alert severity for the remaining rules.
	AlertWarning AlertSeverity = "warning"
)

// AlertRuleID is the closed rule vocabulary (also the log record's
// `alert_rule` value).
type AlertRuleID string

const (
	// AlertRuleSoftSustained: pending >= soft_limit held longer than the
	// configured sustained window.
	AlertRuleSoftSustained AlertRuleID = "outbox_soft_sustained"
	// AlertRuleHardReached: pending >= hard_limit (immediate P1).
	AlertRuleHardReached AlertRuleID = "outbox_hard_reached"
	// AlertRuleOutboxBlocked: permanently blocked outbox events exist.
	AlertRuleOutboxBlocked AlertRuleID = "outbox_blocked"
	// AlertRuleIdentityConflict: a same-identity different-content conflict
	// was observed.
	AlertRuleIdentityConflict AlertRuleID = "events_identity_conflict"
	// AlertRuleQuarantineAdded: new consumer quarantine entries appeared.
	AlertRuleQuarantineAdded AlertRuleID = "consumer_quarantine_added"
)

// AlertConfig is the resolved alert configuration. Numeric thresholds are
// configuration inputs (never constants in this file): SoftLimit/HardLimit and
// DrainTargetWindow mirror the capacity configuration; SoftSustainedWindow is
// the optional operator override for the sustained-soft window and falls back
// to DrainTargetWindow when zero.
type AlertConfig struct {
	Enabled             bool
	SoftLimit           int64
	HardLimit           int64
	DrainTargetWindow   time.Duration
	SoftSustainedWindow time.Duration
}

// sustainedWindow resolves the sustained-soft window: the explicit override
// when configured, otherwise the capacity drain target window. A zero value
// (feature not configured) resolves to zero and the rule is inert.
func (c AlertConfig) sustainedWindow() time.Duration {
	if c.SoftSustainedWindow > 0 {
		return c.SoftSustainedWindow
	}
	return c.DrainTargetWindow
}

// AlertRule is one catalog entry: the rule, its severity, the human-readable
// condition and the provenance of its threshold (the "来源标注" the task
// requires). ThresholdSource is never empty.
type AlertRule struct {
	ID              AlertRuleID
	Severity        AlertSeverity
	Condition       string
	Threshold       string
	ThresholdSource string
}

// Threshold source vocabulary: every rule names either the configuration key
// that carries the value or the ruling that fixes the condition.
const (
	sourceSoftLimit = "config:TXHARBOR_EVENTS_CAPACITY_SOFT_LIMIT " +
		"(contracts/capacity.md §5 measurement method; value from measurement/ruling)"
	sourceHardLimit = "config:TXHARBOR_EVENTS_CAPACITY_HARD_LIMIT " +
		"(contracts/capacity.md §5.6: hard arrival alerts immediately as P1)"
	sourceDrainWindow = "config:TXHARBOR_EVENTS_CAPACITY_DRAIN_TARGET_WINDOW " +
		"(verification.md §1 / contracts/capacity.md §5.6 configured window)"
	sourceSustainedOverride = "config:TXHARBOR_EVENTS_ALERT_SOFT_SUSTAINED_WINDOW " +
		"(operator override; contracts/capacity.md §5.6 configured window)"
	sourceBlocked = "ruling:FR-24/verification.md §1 — permanently blocked events alert " +
		"(contracts/outbox-publisher.md §5); no invented count"
	sourceIdentityConflict = "ruling:FR-24/verification.md §1 — identity conflicts alert " +
		"(contracts/events.md §2); no invented count"
	sourceQuarantine = "ruling:FR-24/verification.md §1 — new quarantine entries alert " +
		"(contracts/consumer.md §3–§4); no invented count"
)

// AlertCatalog renders the fixed rule list with the configured thresholds and
// their sources. It is the machine-checkable "alert list + provenance"
// evidence (T075).
func AlertCatalog(cfg AlertConfig) []AlertRule {
	window := cfg.sustainedWindow()
	windowSource := sourceDrainWindow
	if cfg.SoftSustainedWindow > 0 {
		windowSource = sourceSustainedOverride
	}
	return []AlertRule{
		{
			ID:              AlertRuleSoftSustained,
			Severity:        AlertWarning,
			Condition:       "pending >= soft_limit held continuously for more than the configured window",
			Threshold:       "soft_limit=" + itoa(cfg.SoftLimit) + ", sustained_window=" + window.String(),
			ThresholdSource: sourceSoftLimit + "; " + windowSource,
		},
		{
			ID:              AlertRuleHardReached,
			Severity:        AlertP1,
			Condition:       "pending >= hard_limit (admission gate + pause trigger; not a physical capacity limit)",
			Threshold:       "hard_limit=" + itoa(cfg.HardLimit),
			ThresholdSource: sourceHardLimit,
		},
		{
			ID:              AlertRuleOutboxBlocked,
			Severity:        AlertWarning,
			Condition:       "outbox_blocked_count > 0 (any permanently blocked event)",
			Threshold:       "> 0",
			ThresholdSource: sourceBlocked,
		},
		{
			ID:              AlertRuleIdentityConflict,
			Severity:        AlertWarning,
			Condition:       "events_identity_conflict_total increases (any same-identity different-content conflict)",
			Threshold:       "increase > 0",
			ThresholdSource: sourceIdentityConflict,
		},
		{
			ID:              AlertRuleQuarantineAdded,
			Severity:        AlertWarning,
			Condition:       "consumer_quarantine_total increases (any new quarantine entry)",
			Threshold:       "increase > 0",
			ThresholdSource: sourceQuarantine,
		},
	}
}

// Alert is one emitted alert record.
type Alert struct {
	Rule            AlertRuleID   `json:"rule"`
	Severity        AlertSeverity `json:"severity"`
	At              time.Time     `json:"at"`
	Observed        string        `json:"observed"`
	Threshold       string        `json:"threshold"`
	ThresholdSource string        `json:"threshold_source"`
	// Fields carries the structured identity context (event type, chain,
	// business identity, attempts, ...). Values are redacted before logging.
	Fields map[string]string `json:"fields,omitempty"`
}

// AlertObservation is the optional identity context an evaluator call may
// attach to the emitted records. The metric series themselves carry only
// low-cardinality labels; a runtime that knows the affected event/chain/
// business identity and attempt count passes them here so the alert record is
// diagnosable (FR-24). Every value is redacted before it reaches a log record.
type AlertObservation struct {
	EventType        string
	EventFamily      string
	ChainID          string
	BusinessIdentity string
	Attempts         int
}

// Alerter evaluates the 013 metric series against the alert catalog. It is
// safe for single-goroutine periodic use; the caller owns the cadence.
type Alerter struct {
	cfg      AlertConfig
	gatherer prometheus.Gatherer
	logger   *slog.Logger
	now      func() time.Time

	softSince  time.Time
	softFired  bool
	hardFired  bool
	blockedOn  bool
	identityAt map[string]float64 // event_type -> counter
	quarantine map[string]float64 // failure_class -> counter
	baselined  bool
}

// NewAlerter builds the evaluator over a gatherer (the shared metrics
// registry), the resolved configuration and a structured logger.
func NewAlerter(cfg AlertConfig, gatherer prometheus.Gatherer, logger *slog.Logger) *Alerter {
	if logger == nil {
		logger = slog.Default()
	}
	return &Alerter{
		cfg:        cfg,
		gatherer:   gatherer,
		logger:     logger,
		now:        time.Now,
		identityAt: map[string]float64{},
		quarantine: map[string]float64{},
	}
}

// Evaluate reads the current series, applies the alert conditions and returns
// the alerts to emit (empty when nothing fires or alerts are disabled). It
// never returns an error for missing series (an absent series is "not
// observed", never a fabricated zero); a gatherer failure is returned.
func (a *Alerter) Evaluate(ctx context.Context, obs AlertObservation) ([]Alert, error) {
	if a == nil || !a.cfg.Enabled {
		return nil, nil
	}
	families, err := a.gatherer.Gather()
	if err != nil {
		return nil, err
	}
	now := a.now()
	var alerts []Alert
	fields := a.contextFields(obs)

	pending, pendingByFamily := gaugeSum(families, OutboxPendingMetricName)
	blocked, _ := gaugeSum(families, OutboxBlockedMetricName)
	identities := counterByLabel(families, EventsIdentityConflictMetricName, "event_type")
	quarantines := counterByLabel(families, ConsumerQuarantineMetricName, "failure_class")

	// --- sustained soft / hard (capacity thresholds from configuration) ----
	level := classifyPending(pending, a.cfg.SoftLimit, a.cfg.HardLimit)
	switch level {
	case capacityHard:
		a.softSince, a.softFired = time.Time{}, false
		if !a.hardFired {
			a.hardFired = true
			alert := a.alert(AlertRuleHardReached, now, "pending="+itoa(int64(pending)), fields)
			alert.Fields["event_families"] = renderFamilies(pendingByFamily)
			alerts = append(alerts, alert)
		}
	case capacitySoft:
		a.hardFired = false
		if a.softSince.IsZero() {
			a.softSince = now
		}
		window := a.cfg.sustainedWindow()
		if window > 0 && !a.softFired && now.Sub(a.softSince) >= window {
			a.softFired = true
			alert := a.alert(AlertRuleSoftSustained, now,
				"pending="+itoa(int64(pending))+", sustained="+now.Sub(a.softSince).String(), fields)
			alert.Fields["event_families"] = renderFamilies(pendingByFamily)
			alerts = append(alerts, alert)
		}
	default:
		a.softSince, a.softFired, a.hardFired = time.Time{}, false, false
	}

	// --- permanently blocked events (immediate; state-based) ---------------
	if blocked > 0 {
		if !a.blockedOn {
			a.blockedOn = true
			alerts = append(alerts, a.alert(AlertRuleOutboxBlocked, now, "blocked="+itoa(int64(blocked)), fields))
		}
	} else {
		a.blockedOn = false
	}

	// --- identity conflicts / quarantine additions (immediate; increase) ---
	// The first observation establishes the baseline: pre-existing history
	// (e.g. rows committed before this process started) is not replayed as a
	// new alert. Every later increase fires.
	if !a.baselined {
		a.identityAt, a.quarantine = identities, quarantines
		a.baselined = true
	} else {
		for _, eventType := range increased(a.identityAt, identities) {
			alertFields := mergeFields(fields, map[string]string{"event_type": eventType})
			alerts = append(alerts, a.alert(AlertRuleIdentityConflict, now,
				"events_identity_conflict_total{"+eventType+"}="+itoa(int64(identities[eventType])), alertFields))
		}
		for _, failureClass := range increased(a.quarantine, quarantines) {
			alertFields := mergeFields(fields, map[string]string{"failure_class": failureClass})
			alerts = append(alerts, a.alert(AlertRuleQuarantineAdded, now,
				"consumer_quarantine_total{"+failureClass+"}="+itoa(int64(quarantines[failureClass])), alertFields))
		}
		a.identityAt, a.quarantine = identities, quarantines
	}
	return alerts, nil
}

// Emit evaluates and writes one structured log record per fired alert. It
// returns the alerts for tests/evidence and never panics on a nil logger.
func (a *Alerter) Emit(ctx context.Context, obs AlertObservation) ([]Alert, error) {
	alerts, err := a.Evaluate(ctx, obs)
	if err != nil {
		return nil, err
	}
	for _, alert := range alerts {
		a.log(ctx, alert)
	}
	return alerts, nil
}

// log writes one alert as a structured record. Every value passes through
// logx.Redact: a credential-shaped context value can never reach the record
// verbatim (T075 redaction scan).
func (a *Alerter) log(ctx context.Context, alert Alert) {
	level := slog.LevelWarn
	if alert.Severity == AlertP1 {
		level = slog.LevelError
	}
	attrs := []slog.Attr{
		slog.String("alert_rule", string(alert.Rule)),
		slog.String("severity", string(alert.Severity)),
		slog.String("observed", logx.Redact(alert.Observed)),
		slog.String("threshold", logx.Redact(alert.Threshold)),
		slog.String("threshold_source", logx.Redact(alert.ThresholdSource)),
	}
	keys := make([]string, 0, len(alert.Fields))
	for key := range alert.Fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		attrs = append(attrs, slog.String(key, logx.Redact(alert.Fields[key])))
	}
	a.logger.LogAttrs(ctx, level, "013 alert", attrs...)
}

// alert builds one Alert from the catalog with the configured threshold and
// source.
func (a *Alerter) alert(id AlertRuleID, at time.Time, observed string, fields map[string]string) Alert {
	rule := a.rule(id)
	return Alert{
		Rule:            id,
		Severity:        rule.Severity,
		At:              at.UTC(),
		Observed:        observed,
		Threshold:       rule.Threshold,
		ThresholdSource: rule.ThresholdSource,
		Fields:          fields,
	}
}

// rule returns the catalog entry for id (the catalog is fixed, so a missing
// id is a programming error; the zero rule keeps emission safe).
func (a *Alerter) rule(id AlertRuleID) AlertRule {
	for _, rule := range AlertCatalog(a.cfg) {
		if rule.ID == id {
			return rule
		}
	}
	return AlertRule{ID: id, Severity: AlertWarning, ThresholdSource: "unknown rule"}
}

// contextFields renders the optional observation context, dropping empty
// values (a record never carries an empty identity claim).
func (a *Alerter) contextFields(obs AlertObservation) map[string]string {
	fields := map[string]string{}
	add := func(key, value string) {
		if strings.TrimSpace(value) != "" {
			fields[key] = value
		}
	}
	add("event_type", obs.EventType)
	add("event_family", obs.EventFamily)
	add("chain_id", obs.ChainID)
	add("business_identity", obs.BusinessIdentity)
	if obs.Attempts > 0 {
		fields["attempts"] = itoa(int64(obs.Attempts))
	}
	return fields
}

// capacityLevel is the local closed classification (mirrors the capacity
// guard's vocabulary without importing the events package).
type capacityLevel int

const (
	capacityNormal capacityLevel = iota
	capacitySoft
	capacityHard
)

// classifyPending maps pending onto the closed level set using the configured
// thresholds. An unconfigured threshold set (zero) classifies as normal: the
// alert wiring is inert until the feature is configured (fail-closed gates
// live in the capacity guard, not here).
func classifyPending(pending float64, soft, hard int64) capacityLevel {
	switch {
	case hard > 0 && pending >= float64(hard):
		return capacityHard
	case soft > 0 && pending >= float64(soft):
		return capacitySoft
	default:
		return capacityNormal
	}
}

// gaugeSum sums one gauge family over all label sets and returns the per-label
// breakdown keyed by the family's first label (event_family).
func gaugeSum(families []*dto.MetricFamily, name string) (float64, map[string]float64) {
	byLabel := map[string]float64{}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			value := metric.GetGauge().GetValue()
			label := firstLabel(metric)
			byLabel[label] += value
		}
	}
	var total float64
	for _, value := range byLabel {
		total += value
	}
	return total, byLabel
}

// counterByLabel reads one counter family into a label-value map. An absent
// series stays absent (never a fabricated zero).
func counterByLabel(families []*dto.MetricFamily, name, label string) map[string]float64 {
	out := map[string]float64{}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			value := 0.0
			if metric.GetCounter() != nil {
				value = metric.GetCounter().GetValue()
			}
			key := labelValue(metric, label)
			out[key] = value
		}
	}
	return out
}

// increased reports the labels whose counter value grew relative to before,
// sorted for deterministic alert order.
func increased(before, after map[string]float64) []string {
	var grew []string
	for label, value := range after {
		if value > before[label] {
			grew = append(grew, label)
		}
	}
	sort.Strings(grew)
	return grew
}

// firstLabel returns the metric's first label value (or "").
func firstLabel(metric *dto.Metric) string {
	labels := metric.GetLabel()
	if len(labels) == 0 {
		return ""
	}
	return labels[0].GetValue()
}

// labelValue returns one named label value (or "").
func labelValue(metric *dto.Metric, name string) string {
	for _, label := range metric.GetLabel() {
		if label.GetName() == name {
			return label.GetValue()
		}
	}
	return ""
}

// renderFamilies renders the pending breakdown for a record field.
func renderFamilies(byFamily map[string]float64) string {
	if len(byFamily) == 0 {
		return ""
	}
	keys := make([]string, 0, len(byFamily))
	for key := range byFamily {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+itoa(int64(byFamily[key])))
	}
	return strings.Join(parts, ",")
}

// mergeFields copies base and adds extra (extra wins).
func mergeFields(base, extra map[string]string) map[string]string {
	out := make(map[string]string, len(base)+len(extra))
	for key, value := range base {
		out[key] = value
	}
	for key, value := range extra {
		out[key] = value
	}
	return out
}

// itoa renders a decimal integer for alert thresholds/observations (the
// package's chainLabel helper is reserved for chain labels).
func itoa(n int64) string { return strconv.FormatInt(n, 10) }
