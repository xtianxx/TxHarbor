package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// 015 backup/recovery/drill observability surface (specs/015-backup-recovery-
// safe-resumption, T055). Contract mapping (data-model.md §3.3/§4.2,
// quickstart §S12, FR-031/FR-036):
//
//	recovery release/refusal/gap/discard/retry counts:
//	  release_total{capability,result}        -> txharbor_recovery_release_total
//	  refusal_total{capability,refusal_class} -> txharbor_recovery_refusal_total
//	  gaps{state}                             -> txharbor_recovery_gaps
//	  gap_dispositions_total{disposition}     -> txharbor_recovery_gap_dispositions_total
//	  evidence_writes_total{result}           -> txharbor_recovery_evidence_writes_total
//
//	separate timing scopes (never collapsed, never summed):
//	  recovery_point_timestamp_seconds        -> txharbor_recovery_recovery_point_timestamp_seconds
//	  db_restore_seconds                      -> txharbor_recovery_db_restore_seconds
//	  verification_seconds                    -> txharbor_recovery_verification_seconds
//	  capability_release_seconds{capability}  -> txharbor_recovery_capability_release_seconds
//	  backup_lag_seconds                      -> txharbor_recovery_backup_lag_seconds
//	  uncovered_interval_seconds              -> txharbor_recovery_uncovered_interval_seconds
//
//	configured targets vs explicit "not configured" (C1/FR-036):
//	  constraint_configured{constraint}       -> txharbor_recovery_constraint_configured
//	  rto_target_seconds                      -> txharbor_recovery_rto_target_seconds
//	  rto_checks_total{result}                -> txharbor_recovery_rto_checks_total
//
// Discipline (MUST NOT be weakened):
//
//   - every label is a fixed low-cardinality vocabulary that mirrors a frozen
//     015 column/enum (capability, refusal class, outcome, gap state,
//     disposition, evidence-write result, constraint key). No instance id,
//     backup id, manifest digest, tx hash, principal, object key, log ref or
//     credential can ever become a label (T055; C1); an out-of-vocabulary
//     value collapses into the single bounded "other" bucket instead of
//     creating unbounded time series;
//   - an unobserved measure is absent (or explicit 0 for "not configured"),
//     never a fabricated zero: backup lag and uncovered interval have no
//     known value in many drills and deleting the series says "unknown",
//     while zero would read as "no lag";
//   - RTO is only ever judged end-to-end over the full safe-resumption scope
//     (recovery point -> last capability release). A database connection, a
//     completed restore or a single timing scope is never an RTO measurement
//     (EvaluateRTOTarget refuses such a scope; FR-031/FR-036, C1);
//   - a configured time target that is exceeded is recorded as not met, is
//     flagged for alert and escalation, and MUST NOT permanently block later
//     safe resumption: safety gates (evidence gaps, isolation, approvals)
//     stay the only blockers (C1). RTOEvaluation carries that boundary
//     explicitly (PermanentResumptionBlock is always false);
//   - an unconfigured required constraint is exposed as an explicit
//     `constraint_configured{constraint} 0` series, never silently absent:
//     "not configured" is a state, and no production recovery objective may
//     be claimed from it (FR-036).
const (
	// RecoveryReleaseMetricName counts release/refusal outcomes per
	// capability; the release decision remains derived (INV-2), this counter
	// only mirrors observations.
	RecoveryReleaseMetricName = "txharbor_recovery_release_total"
	// RecoveryRefusalMetricName counts refusals by the closed refusal class of
	// data-model §3.3.
	RecoveryRefusalMetricName = "txharbor_recovery_refusal_total"
	// RecoveryGapsMetricName is the point-in-time evidence-gap count by state
	// (open|closed|escalated). open and escalated both block their affected
	// capabilities; escalation is never closure.
	RecoveryGapsMetricName = "txharbor_recovery_gaps"
	// RecoveryGapDispositionsMetricName counts gap dispositions (establish,
	// close by new evidence, escalation and the audit-only notes). Timeout and
	// acknowledgement notes are observations, never closures.
	RecoveryGapDispositionsMetricName = "txharbor_recovery_gap_dispositions_total"
	// RecoveryEvidenceWritesMetricName counts evidence-generation writes by
	// result: accepted, discarded (stale token) or retried. A discarded write
	// changes nothing.
	RecoveryEvidenceWritesMetricName = "txharbor_recovery_evidence_writes_total"
	// RecoveryRecoveryPointMetricName is the recovery point wall-clock time
	// (unix seconds). Absent while the recovery point is unknown.
	RecoveryRecoveryPointMetricName = "txharbor_recovery_recovery_point_timestamp_seconds"
	// RecoveryDBRestoreSecondsMetricName is the database restore duration
	// alone. It is one separate scope and never an RTO measurement.
	RecoveryDBRestoreSecondsMetricName = "txharbor_recovery_db_restore_seconds"
	// RecoveryVerificationSecondsMetricName is the verification duration
	// alone; separate from the restore scope.
	RecoveryVerificationSecondsMetricName = "txharbor_recovery_verification_seconds"
	// RecoveryCapabilityReleaseSecondsMetricName is the per-capability safe
	// resumption duration from the common origin (recovery/restore start).
	RecoveryCapabilityReleaseSecondsMetricName = "txharbor_recovery_capability_release_seconds"
	// RecoveryBackupLagSecondsMetricName is the recovery-point-to-latest-
	// external-fact/local-write distance. Absent while unknown (never a
	// fabricated zero).
	RecoveryBackupLagSecondsMetricName = "txharbor_recovery_backup_lag_seconds"
	// RecoveryUncoveredIntervalSecondsMetricName is the uncovered interval
	// (data/time range the backup does not cover). Absent while unknown.
	RecoveryUncoveredIntervalSecondsMetricName = "txharbor_recovery_uncovered_interval_seconds"
	// RecoveryConstraintConfiguredMetricName is 1/0 per required recovery
	// constraint; 0 is the explicit "not configured" state.
	RecoveryConstraintConfiguredMetricName = "txharbor_recovery_constraint_configured"
	// RecoveryRTOTargetSecondsMetricName is the configured RTO target; the
	// series is absent while no target is configured.
	RecoveryRTOTargetSecondsMetricName = "txharbor_recovery_rto_target_seconds"
	// RecoveryRTOChecksMetricName counts RTO evaluations by result
	// (within_target|exceeded|not_configured|not_measured).
	RecoveryRTOChecksMetricName = "txharbor_recovery_rto_checks_total"
)

// Frozen 015 vocabularies. They mirror the control-store CHECK constraints of
// the 0001 control schema, the closed capability/refusal sets of
// data-model.md §3.3 and the V1-V9/FR-019 dispositions verbatim; a value
// outside them collapses to recoveryOtherLabel rather than being trusted.
var (
	recoveryCapabilities = []string{
		"query", "chain_scan", "deposit_confirmation", "existing_withdrawal_recovery",
		"new_withdrawal_creation", "event_publishing", "event_consuming",
	}
	recoveryReleaseResults = []string{"released", "refused"}
	recoveryRefusalClasses = []string{
		"no_instance", "instance_mismatch", "no_release", "release_invalidated_generation",
		"release_revoked", "capability_dependency_closed", "isolation_unproven", "gap_open",
		"approval_missing", "approval_identity_unverified", "approval_executor_excluded",
		"approval_stale", "hard_gate_active", "control_store_unavailable", "scope_mismatch",
	}
	recoveryGapStates = []string{"open", "closed", "escalated"}
	// recoveryGapDispositions mirrors gaps.go: establishment, closure by new
	// evidence, escalation and the three audit-only notes (timeout,
	// attempts_exhausted, acknowledged). The notes are never closures.
	recoveryGapDispositions = []string{
		"opened", "closed_by_evidence", "escalated",
		"note_timeout", "note_attempts_exhausted", "note_acknowledged",
	}
	recoveryEvidenceWriteResults = []string{"accepted", "discarded", "retried"}
	// recoveryConstraintKeys are the required recovery constraints of
	// FR-036/C1: the objectives, cadence and retention. Their absence is an
	// explicit unconfigured state.
	recoveryConstraintKeys = []string{"rpo_target", "rto_target", "backup_frequency", "retention"}
	recoveryRTOStatuses    = []string{"within_target", "exceeded", "not_configured", "not_measured"}
)

// recoveryOtherLabel is the bounded sink for out-of-vocabulary label values.
const recoveryOtherLabel = "other"

// recoveryEnum returns value when it is one of the allowed vocabulary values
// and recoveryOtherLabel otherwise. It is the redaction boundary: caller
// text (an instance id, backup id, digest, tx hash, principal or credential)
// can never reach a label.
func recoveryEnum(value string, allowed []string) string {
	for _, candidate := range allowed {
		if value == candidate {
			return value
		}
	}
	return recoveryOtherLabel
}

// recoveryCount renders a point-in-time count: a negative caller value is
// clamped to a known-empty 0 rather than exposed as a negative series.
func recoveryCount(count int) float64 {
	if count < 0 {
		return 0
	}
	return float64(count)
}

// recoveryMetrics groups the 015 series. The series only appear once observed
// (Vec semantics), so a fresh registry carries no 015 noise.
type recoveryMetrics struct {
	release               *prometheus.CounterVec
	refusals              *prometheus.CounterVec
	gaps                  *prometheus.GaugeVec
	gapDispositions       *prometheus.CounterVec
	evidenceWrites        *prometheus.CounterVec
	recoveryPoint         *prometheus.GaugeVec
	dbRestoreSeconds      *prometheus.GaugeVec
	verificationSeconds   *prometheus.GaugeVec
	capabilityReleaseSecs *prometheus.GaugeVec
	backupLagSeconds      *prometheus.GaugeVec
	uncoveredIntervalSecs *prometheus.GaugeVec
	constraintConfigured  *prometheus.GaugeVec
	rtoTargetSeconds      *prometheus.GaugeVec
	rtoChecks             *prometheus.CounterVec
}

// registerRecovery builds the 015 series, registers them on the shared
// registry and returns the group. Called from New; kept here so metrics.go
// only gains one field and one call.
func registerRecovery(registry *prometheus.Registry) recoveryMetrics {
	r := recoveryMetrics{
		release: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: RecoveryReleaseMetricName,
			Help: "015 release/refusal observations per capability (released|refused); the release decision stays derived and this counter grants nothing.",
		}, []string{"capability", "result"}),
		refusals: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: RecoveryRefusalMetricName,
			Help: "015 refusals by capability and the closed refusal class of data-model §3.3; every class is fail-closed and never a permission.",
		}, []string{"capability", "refusal_class"}),
		gaps: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: RecoveryGapsMetricName,
			Help: "015 evidence-gap count by state (open|closed|escalated); open and escalated both block their affected capabilities and escalation is never closure.",
		}, []string{"state"}),
		gapDispositions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: RecoveryGapDispositionsMetricName,
			Help: "015 gap dispositions (opened|closed_by_evidence|escalated|note_timeout|note_attempts_exhausted|note_acknowledged); the notes never close a gap or permit a release.",
		}, []string{"disposition"}),
		evidenceWrites: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: RecoveryEvidenceWritesMetricName,
			Help: "015 evidence-generation writes by result (accepted|discarded|retried); a discarded write changes nothing.",
		}, []string{"result"}),
		recoveryPoint: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: RecoveryRecoveryPointMetricName,
			Help: "015 recovery point wall-clock time in unix seconds; absent while the recovery point is unknown (never a fabricated zero).",
		}, nil),
		dbRestoreSeconds: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: RecoveryDBRestoreSecondsMetricName,
			Help: "015 database restore duration in seconds. One separate scope only: a completed restore or a reachable database is never an RTO measurement.",
		}, nil),
		verificationSeconds: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: RecoveryVerificationSecondsMetricName,
			Help: "015 verification duration in seconds; separate from the restore scope and never combined with it into an RTO claim.",
		}, nil),
		capabilityReleaseSecs: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: RecoveryCapabilityReleaseSecondsMetricName,
			Help: "015 per-capability safe resumption duration in seconds from the common origin (recovery/restore start); an unreleased capability has no series.",
		}, []string{"capability"}),
		backupLagSeconds: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: RecoveryBackupLagSecondsMetricName,
			Help: "015 backup lag in seconds (recovery point to latest external fact/local write); absent while unknown, never a fabricated zero.",
		}, nil),
		uncoveredIntervalSecs: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: RecoveryUncoveredIntervalSecondsMetricName,
			Help: "015 uncovered interval in seconds (range the backup does not cover); absent while unknown, never a fabricated zero.",
		}, nil),
		constraintConfigured: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: RecoveryConstraintConfiguredMetricName,
			Help: "015 required recovery constraint configuration: 1=configured, 0=explicitly not configured (rpo_target|rto_target|backup_frequency|retention). Not configured never supports a production recovery-objective claim.",
		}, []string{"constraint"}),
		rtoTargetSeconds: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: RecoveryRTOTargetSecondsMetricName,
			Help: "015 configured RTO target in seconds; absent while no target is configured (nothing is claimed).",
		}, nil),
		rtoChecks: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: RecoveryRTOChecksMetricName,
			Help: "015 RTO evaluations by result (within_target|exceeded|not_configured|not_measured); only the full safe-resumption scope can ever be within_target, and exceeded never permanently blocks later safe resumption.",
		}, []string{"result"}),
	}
	registry.MustRegister(
		r.release, r.refusals, r.gaps, r.gapDispositions, r.evidenceWrites,
		r.recoveryPoint, r.dbRestoreSeconds, r.verificationSeconds,
		r.capabilityReleaseSecs, r.backupLagSeconds, r.uncoveredIntervalSecs,
		r.constraintConfigured, r.rtoTargetSeconds, r.rtoChecks,
	)
	return r
}

// ObserveRecoveryRelease counts one release/refusal observation per
// capability. result is the frozen vocabulary released|refused; an
// out-of-vocabulary value lands in the bounded "other" bucket. The counter
// mirrors an observation only: it never grants a release.
func (m *Metrics) ObserveRecoveryRelease(capability, result string) {
	m.recovery.release.WithLabelValues(
		recoveryEnum(capability, recoveryCapabilities),
		recoveryEnum(result, recoveryReleaseResults),
	).Inc()
}

// ObserveRecoveryRefusal counts one refusal by capability and the closed
// refusal class. Unknown classes collapse into the bounded "other" bucket.
func (m *Metrics) ObserveRecoveryRefusal(capability, refusalClass string) {
	m.recovery.refusals.WithLabelValues(
		recoveryEnum(capability, recoveryCapabilities),
		recoveryEnum(refusalClass, recoveryRefusalClasses),
	).Inc()
}

// SetRecoveryGaps records the point-in-time evidence-gap count for one state.
// A negative count is clamped to 0. open/escalated are blocking states.
func (m *Metrics) SetRecoveryGaps(state string, count int) {
	m.recovery.gaps.WithLabelValues(recoveryEnum(state, recoveryGapStates)).Set(recoveryCount(count))
}

// ObserveRecoveryGapDisposition counts one gap disposition. The timeout and
// acknowledged notes are audit-only: observing them here never implies
// closure or a release permission.
func (m *Metrics) ObserveRecoveryGapDisposition(disposition string) {
	m.recovery.gapDispositions.WithLabelValues(
		recoveryEnum(disposition, recoveryGapDispositions),
	).Inc()
}

// ObserveRecoveryEvidenceWrite counts one evidence-generation write by result
// (accepted|discarded|retried). A discarded (stale token) write changed
// nothing and is counted as such, never as accepted.
func (m *Metrics) ObserveRecoveryEvidenceWrite(result string) {
	m.recovery.evidenceWrites.WithLabelValues(
		recoveryEnum(result, recoveryEvidenceWriteResults),
	).Inc()
}

// ObserveRecoveryPoint records the recovery point wall-clock time.
func (m *Metrics) ObserveRecoveryPoint(at time.Time) {
	m.recovery.recoveryPoint.WithLabelValues().Set(float64(at.Unix()))
}

// ObserveRecoveryPointUnknown removes the recovery-point series: unknown is
// not a zero timestamp.
func (m *Metrics) ObserveRecoveryPointUnknown() {
	m.recovery.recoveryPoint.DeleteLabelValues()
}

// ObserveDBRestoreSeconds records the database restore duration alone. The
// caller must not treat this value as an RTO measurement (see
// EvaluateRTOTarget): the separate verification and per-capability release
// scopes are recorded independently.
func (m *Metrics) ObserveDBRestoreSeconds(seconds float64) {
	m.recovery.dbRestoreSeconds.WithLabelValues().Set(seconds)
}

// ObserveVerificationSeconds records the verification duration alone.
func (m *Metrics) ObserveVerificationSeconds(seconds float64) {
	m.recovery.verificationSeconds.WithLabelValues().Set(seconds)
}

// ObserveCapabilityReleaseSeconds records one capability's safe resumption
// duration from the common origin. Only measured, already-observed releases
// are recorded; an unreleased capability has no series (never a zero).
func (m *Metrics) ObserveCapabilityReleaseSeconds(capability string, seconds float64) {
	m.recovery.capabilityReleaseSecs.WithLabelValues(
		recoveryEnum(capability, recoveryCapabilities),
	).Set(seconds)
}

// ObserveBackupLag records the backup lag. known=false removes the series:
// unknown lag is never a fabricated zero.
func (m *Metrics) ObserveBackupLag(seconds float64, known bool) {
	if !known {
		m.recovery.backupLagSeconds.DeleteLabelValues()
		return
	}
	m.recovery.backupLagSeconds.WithLabelValues().Set(seconds)
}

// ObserveUncoveredInterval records the uncovered interval. known=false
// removes the series (unknown, never zero).
func (m *Metrics) ObserveUncoveredInterval(seconds float64, known bool) {
	if !known {
		m.recovery.uncoveredIntervalSecs.DeleteLabelValues()
		return
	}
	m.recovery.uncoveredIntervalSecs.WithLabelValues().Set(seconds)
}

// SetRecoveryConstraintConfigured records the explicit configured /
// not-configured state of one required recovery constraint (FR-036/C1). An
// unconfigured constraint is exposed as 0, never hidden.
func (m *Metrics) SetRecoveryConstraintConfigured(constraint string, configured bool) {
	value := 0.0
	if configured {
		value = 1
	}
	m.recovery.constraintConfigured.WithLabelValues(
		recoveryEnum(constraint, recoveryConstraintKeys),
	).Set(value)
}

// ObserveRTOEvaluation records one RTO evaluation: the configured target (or
// its absence) and the result class. The evaluation itself carries the alert/
// escalation disposition; this method never changes a gate or blocks
// resumption.
func (m *Metrics) ObserveRTOEvaluation(eval RTOEvaluation) {
	if eval.Target > 0 {
		m.recovery.rtoTargetSeconds.WithLabelValues().Set(eval.Target.Seconds())
	} else {
		m.recovery.rtoTargetSeconds.DeleteLabelValues()
	}
	m.recovery.rtoChecks.WithLabelValues(
		recoveryEnum(string(eval.Status), recoveryRTOStatuses),
	).Inc()
}

// ---------------------------------------------------------------------------
// RTO target evaluation (FR-031/FR-036; C1)
// ---------------------------------------------------------------------------

// RTOScope is the measurement scope of one RTO evaluation. Only
// RTOScopeSafeResumption is a valid RTO scope: the timing scopes of FR-031
// must stay separate, and a single scope (a completed restore, a reachable
// database, a verification duration) is never an RTO measurement.
type RTOScope string

const (
	// RTOScopeDBRestoreOnly is the database-restore scope alone. It can never
	// produce within_target/exceeded.
	RTOScopeDBRestoreOnly RTOScope = "db_restore_only"
	// RTOScopeVerificationOnly is the verification scope alone. It can never
	// produce within_target/exceeded.
	RTOScopeVerificationOnly RTOScope = "verification_only"
	// RTOScopeSafeResumption is the end-to-end scope: recovery point to the
	// last capability's safe resumption, composed of the separate scopes.
	RTOScopeSafeResumption RTOScope = "full_safe_resumption"
)

// RTOStatus is the closed RTO evaluation vocabulary.
type RTOStatus string

const (
	// RTOStatusWithinTarget: a configured target was met end-to-end.
	RTOStatusWithinTarget RTOStatus = "within_target"
	// RTOStatusExceeded: a configured target was missed end-to-end; recorded
	// as not met, alerted and escalated.
	RTOStatusExceeded RTOStatus = "exceeded"
	// RTOStatusNotConfigured: no target is configured; nothing is claimed.
	RTOStatusNotConfigured RTOStatus = "not_configured"
	// RTOStatusNotMeasured: a target is configured but no end-to-end
	// safe-resumption measurement exists (or the supplied scope is not the
	// safe-resumption scope); nothing is claimed.
	RTOStatusNotMeasured RTOStatus = "not_measured"
)

// RTOEvaluation is one RTO judgment. It is the machine-readable form of the
// 2026-09-28 ruling: separate timing scopes, no claim from a reachable
// database, and a configured timeout records not-met + alert + escalation
// without ever becoming a permanent prohibition of later safe resumption.
type RTOEvaluation struct {
	Status RTOStatus `json:"status"`
	Scope  RTOScope  `json:"scope"`
	// Target is the configured RTO target; zero means not configured.
	Target time.Duration `json:"target,omitempty"`
	// Measured is the end-to-end safe-resumption duration; meaningful only
	// when MeasuredKnown and Scope=full_safe_resumption.
	Measured      time.Duration `json:"measured,omitempty"`
	MeasuredKnown bool          `json:"measured_known"`
	// NotMet reports a configured target that was missed.
	NotMet bool `json:"not_met"`
	// Alert and EscalationRequired report the mandated handling of a missed
	// target (record not-met, alert, escalate).
	Alert              bool `json:"alert"`
	EscalationRequired bool `json:"escalation_required"`
	// PermanentResumptionBlock is always false: a time target never becomes a
	// permanent prohibition of later safe resumption (C1). Evidence gaps,
	// isolation and approvals remain the only blockers.
	PermanentResumptionBlock bool   `json:"permanent_resumption_block"`
	Reason                   string `json:"reason"`
}

// EvaluateRTOTarget judges one RTO measurement against the configured target.
// It is pure and side-effect free; callers record the outcome with
// ObserveRTOEvaluation.
//
// Rules (deliberately fail-closed, FR-031/FR-036, C1):
//
//   - target <= 0 → not_configured: no target, no claim;
//   - scope other than full_safe_resumption → not_measured: a database
//     connection, a completed restore or any single timing scope is never an
//     RTO measurement;
//   - !measuredKnown → not_measured;
//   - measured > target → exceeded with NotMet/Alert/EscalationRequired set
//     and PermanentResumptionBlock always false;
//   - otherwise within_target.
func EvaluateRTOTarget(target, measured time.Duration, scope RTOScope, measuredKnown bool) RTOEvaluation {
	eval := RTOEvaluation{
		Scope:                    scope,
		Target:                   target,
		Measured:                 measured,
		MeasuredKnown:            measuredKnown,
		PermanentResumptionBlock: false,
	}
	if target <= 0 {
		eval.Status = RTOStatusNotConfigured
		eval.Reason = "no RTO target is configured; no recovery-objective claim is made (FR-036: not configured is a state)"
		return eval
	}
	if scope != RTOScopeSafeResumption {
		eval.Status = RTOStatusNotMeasured
		eval.Reason = "scope " + string(scope) + " is a single timing scope; RTO is only evaluated end-to-end over the full safe-resumption scope (recovery point to last capability release). A reachable database or a completed restore is never an RTO measurement"
		return eval
	}
	if !measuredKnown || measured < 0 {
		eval.Status = RTOStatusNotMeasured
		eval.Reason = "the end-to-end safe-resumption duration was not measured; nothing is claimed"
		return eval
	}
	if measured > target {
		eval.Status = RTOStatusExceeded
		eval.NotMet = true
		eval.Alert = true
		eval.EscalationRequired = true
		eval.Reason = "the configured RTO target was missed: recorded as not met, alerted and escalated; the timeout does not permanently block later safe resumption (evidence gaps, isolation and approvals remain the only blockers)"
		return eval
	}
	eval.Status = RTOStatusWithinTarget
	eval.Reason = "the configured RTO target was met end-to-end across the separate recovery, verification and capability-release scopes"
	return eval
}
