package metrics

import (
	"encoding/json"

	"github.com/prometheus/client_golang/prometheus"
)

// 014 reconciliation-exception-handling series (specs/014-reconciliation-
// exception-handling, T029). Contract names are prefixed like every other
// custom series in this package. Mapping:
//
//	recon_task_state              -> txharbor_recon_task_state
//	recon_scan_progress           -> txharbor_recon_scan_progress
//	recon_history_sweep_progress  -> txharbor_recon_history_sweep_progress
//	recon_verifications_total     -> txharbor_recon_verifications_total
//	recon_gaps_open               -> txharbor_recon_gaps_open
//	recon_discrepancies           -> txharbor_recon_discrepancies
//	recon_duplicates_total        -> txharbor_recon_duplicates_total
//	recon_dispositions_total      -> txharbor_recon_dispositions_total
//	recon_audit_actions_total     -> txharbor_recon_audit_actions_total
//
// Discipline (MUST NOT be weakened):
//
//   - every label is a fixed low-cardinality vocabulary that mirrors a frozen
//     014 column/enum (scope_kind, task/discrepancy state, gap reason,
//     category, reverify verdict, duplicate outcome, disposition kind/result,
//     audit action). No task id, business key, content hash, principal,
//     operator, request id, payload or credential can ever become a label
//     (FR-024/FR-027 redaction; constitution XII);
//   - every observe method funnels its label value through reconEnum, so a
//     caller bug cannot widen a series: an out-of-vocabulary value collapses
//     into the single bounded "other" bucket instead of creating unbounded
//     time series;
//   - scan progress, history-sweep progress, verification completeness and
//     open gaps are four distinct series: an advanced traversal cursor is
//     never a verified-completeness claim (data-model §6), and a gapped or
//     paused scope is visible as open gaps, never rendered as fully
//     consistent (Q3-5);
//   - absorbed duplicates are metrics-only and separate from discrepancy
//     (fund ticket) counts, so the duplicate rate can alert without implying
//     a fund anomaly (Q4/FR-009). Only the DB by-action aggregation helper
//     below may count recon_audit rows, and it MUST exclude management rows.
const (
	// ReconTaskStateMetricName counts current recon_task rows by scope kind
	// and lifecycle state.
	ReconTaskStateMetricName = "txharbor_recon_task_state"
	// ReconScanProgressMetricName is the persisted-through checkpoint
	// watermark of the observed task per scope kind.
	ReconScanProgressMetricName = "txharbor_recon_scan_progress"
	// ReconHistorySweepProgressMetricName is the history-sweep evidence-age
	// waterline per scope kind; it is never a verified-completeness claim.
	ReconHistorySweepProgressMetricName = "txharbor_recon_history_sweep_progress"
	// ReconVerificationsMetricName counts reverify verdicts; unknown/stale
	// are incomplete verifications, never consistent.
	ReconVerificationsMetricName = "txharbor_recon_verifications_total"
	// ReconGapsOpenMetricName counts unresolved recon_gap rows by reason.
	ReconGapsOpenMetricName = "txharbor_recon_gaps_open"
	// ReconDiscrepanciesMetricName counts current discrepancy (fund ticket)
	// rows by category and lifecycle state.
	ReconDiscrepanciesMetricName = "txharbor_recon_discrepancies"
	// ReconDuplicatesMetricName counts Q4 duplicate-delivery judgments;
	// absorbed is metrics/audit only and stays separate from fund tickets.
	ReconDuplicatesMetricName = "txharbor_recon_duplicates_total"
	// ReconDispositionsMetricName counts recorded dispositions by kind and
	// result (FR-013: the three disposition classes are countable).
	ReconDispositionsMetricName = "txharbor_recon_dispositions_total"
	// ReconAuditActionsMetricName counts recon_audit rows by action with
	// management rows excluded (see ReconAuditManagementFilter).
	ReconAuditActionsMetricName = "txharbor_recon_audit_actions_total"
)

// Frozen 014 vocabularies. They mirror the migration 000016 CHECK constraints
// and the classifier verdicts (internal/reconciliation/classify.go) verbatim;
// a value outside them is collapsed to reconOtherLabel rather than trusted.
var (
	reconScopeKinds  = []string{"height", "time"}
	reconTaskStates  = []string{"created", "running", "paused", "suspended_budget", "done", "cancelled"}
	reconGapReasons  = []string{"not_started", "interrupted", "budget_exhausted", "paused", "freshness_hold", "upstream_unconnected", "query_failed"}
	reconCategories  = []string{"missing", "duplicate_divergent", "state_mismatch", "unknown", "incomplete"}
	reconTicketState = []string{"open_claimable", "claimed", "disposing", "pending_verify", "closed", "reopened"}
	reconVerdicts    = []string{"consistent", "divergent", "unknown", "stale"}
	// reconDuplicateOutcomes deliberately omits the classifier's "none": no
	// duplicate is not a duplicate observation and is never counted.
	reconDuplicateOutcomes  = []string{"absorbed", "divergent", "unproven"}
	reconDispositionKinds   = []string{"ack_only", "reuse_recovery", "new_fix_rule"}
	reconDispositionResults = []string{"done", "refused", "failed", "dry_run"}
	reconAuditActions       = []string{"query", "start", "pause", "resume", "claim", "dispose", "reverify", "close", "reopen", "refuse"}
)

// reconOtherLabel is the bounded sink for out-of-vocabulary label values.
const reconOtherLabel = "other"

// reconEnum returns value when it is one of the frozen vocabulary values and
// reconOtherLabel otherwise. It is the redaction boundary: caller-supplied
// text (a principal, ticket id, request id, payload fragment or credential)
// can never reach a label; it lands in one bounded bucket (FR-024/FR-027).
func reconEnum(value string, allowed []string) string {
	for _, candidate := range allowed {
		if value == candidate {
			return value
		}
	}
	return reconOtherLabel
}

// reconCount renders an observation count: a negative caller value is clamped
// to a known-empty 0 rather than exposed as a negative series.
func reconCount(count int) float64 {
	if count < 0 {
		return 0
	}
	return float64(count)
}

// reconciliationMetrics groups the 014 series. The series only appear once
// observed (Vec semantics), so a fresh registry carries no 014 noise.
type reconciliationMetrics struct {
	taskState            *prometheus.GaugeVec
	scanProgress         *prometheus.GaugeVec
	historySweepProgress *prometheus.GaugeVec
	verifications        *prometheus.CounterVec
	gapsOpen             *prometheus.GaugeVec
	discrepancies        *prometheus.GaugeVec
	duplicates           *prometheus.CounterVec
	dispositions         *prometheus.CounterVec
	auditActions         *prometheus.CounterVec
}

// registerReconciliation builds the 014 series, registers them on the shared
// registry and returns the group. Called from New; kept here so metrics.go
// only gains one field and one call.
func registerReconciliation(registry *prometheus.Registry) reconciliationMetrics {
	r := reconciliationMetrics{
		taskState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: ReconTaskStateMetricName,
			Help: "014 reconciliation tasks by scope kind (height|time) and lifecycle state; count snapshot, never a per-task label.",
		}, []string{"scope_kind", "state"}),
		scanProgress: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: ReconScanProgressMetricName,
			Help: "Persisted-through scan checkpoint watermark of the observed 014 task per scope kind (height=block number, time=unix seconds); absent when no checkpoint is persisted (unknown, never a fabricated zero).",
		}, []string{"scope_kind"}),
		historySweepProgress: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: ReconHistorySweepProgressMetricName,
			Help: "History-sweep evidence-age waterline per scope kind. An advanced cursor never proves verified completeness: gapped items stay open gaps and are never counted as verified.",
		}, []string{"scope_kind"}),
		verifications: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: ReconVerificationsMetricName,
			Help: "014 reverify verdicts (consistent|divergent|unknown|stale); consistent only on complete fresh evidence, unknown/stale are incomplete and never rendered consistent.",
		}, []string{"verdict"}),
		gapsOpen: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: ReconGapsOpenMetricName,
			Help: "Unresolved recon_gap rows by reason (not_started|interrupted|budget_exhausted|paused|freshness_hold|upstream_unconnected|query_failed); a paused/gapped scope is never fully consistent.",
		}, []string{"reason"}),
		discrepancies: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: ReconDiscrepanciesMetricName,
			Help: "Current discrepancy (fund ticket) counts by category (missing|duplicate_divergent|state_mismatch|unknown|incomplete) and lifecycle state; kept separate from the absorbed-duplicate rate.",
		}, []string{"category", "state"}),
		duplicates: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: ReconDuplicatesMetricName,
			Help: "Q4 duplicate-delivery judgments by outcome (absorbed|divergent|unproven). Absorbed duplicates are metrics/audit only, never a ticket, so their rate can alert separately from fund tickets; unproven stays pending and claims nothing.",
		}, []string{"outcome"}),
		dispositions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: ReconDispositionsMetricName,
			Help: "Recorded 014 dispositions by kind (ack_only|reuse_recovery|new_fix_rule) and result (done|refused|failed|dry_run); details stay on the disposition rows and audit trail.",
		}, []string{"kind", "result"}),
		auditActions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: ReconAuditActionsMetricName,
			Help: "recon_audit rows by lifecycle action. Management permission rows are excluded (they borrow start/close; target.management_action is the discriminator), so grant/revoke never inflate those buckets.",
		}, []string{"action"}),
	}
	registry.MustRegister(
		r.taskState, r.scanProgress, r.historySweepProgress, r.verifications,
		r.gapsOpen, r.discrepancies, r.duplicates, r.dispositions,
		r.auditActions,
	)
	return r
}

// SetReconTaskState records the current recon_task row count for one
// (scope_kind, state) pair. count is a point-in-time snapshot from the task
// table; zero is exposed as a known-empty 0.
func (m *Metrics) SetReconTaskState(scopeKind, state string, count int) {
	m.recon.taskState.WithLabelValues(
		reconEnum(scopeKind, reconScopeKinds),
		reconEnum(state, reconTaskStates),
	).Set(reconCount(count))
}

// SetReconScanProgress records the persisted-through checkpoint watermark of
// the observed task for one scope kind (height: block number; time: unix
// seconds). ok=false means no checkpoint is persisted: the series is removed
// rather than exposed as a fabricated zero.
func (m *Metrics) SetReconScanProgress(scopeKind string, watermark uint64, ok bool) {
	label := reconEnum(scopeKind, reconScopeKinds)
	if !ok {
		m.recon.scanProgress.DeleteLabelValues(label)
		return
	}
	m.recon.scanProgress.WithLabelValues(label).Set(float64(watermark))
}

// SetReconHistorySweepProgress records the history-sweep evidence-age waterline
// (history_sweep_through) for one scope kind. ok=false means no sweep has run:
// the series is removed. Advancing this cursor is progress, not verification:
// items skipped as gaps remain open gaps until individually re-verified.
func (m *Metrics) SetReconHistorySweepProgress(scopeKind string, watermark uint64, ok bool) {
	label := reconEnum(scopeKind, reconScopeKinds)
	if !ok {
		m.recon.historySweepProgress.DeleteLabelValues(label)
		return
	}
	m.recon.historySweepProgress.WithLabelValues(label).Set(float64(watermark))
}

// ObserveReconVerification counts one reverify verdict: consistent is recorded
// only on complete, fresh evidence satisfying the consistency rule; divergent
// is an evidenced divergence; unknown and stale are incomplete verifications
// and are never counted (or rendered) as consistent (FR-010/Q5).
func (m *Metrics) ObserveReconVerification(verdict string) {
	m.recon.verifications.WithLabelValues(reconEnum(verdict, reconVerdicts)).Inc()
}

// SetReconOpenGaps records the current unresolved recon_gap row count by
// reason. Paused, budget-suspended, interrupted, freshness-held, unconnected
// and query-failed scopes stay visible here and must never render as fully
// consistent (Q3-5/FR-019).
func (m *Metrics) SetReconOpenGaps(reason string, count int) {
	m.recon.gapsOpen.WithLabelValues(reconEnum(reason, reconGapReasons)).Set(reconCount(count))
}

// SetReconDiscrepancies records the current discrepancy (fund ticket) count
// for one (category, state) pair: open tickets, work in claim/dispose, the
// pending-verify backlog and closed/reopened history are all distinguishable.
// Absorbed duplicates never appear here (they are metrics-only, Q4).
func (m *Metrics) SetReconDiscrepancies(category, state string, count int) {
	m.recon.discrepancies.WithLabelValues(
		reconEnum(category, reconCategories),
		reconEnum(state, reconTicketState),
	).Set(reconCount(count))
}

// ObserveReconDuplicate counts one Q4 duplicate-delivery judgment. absorbed is
// metrics/audit only (no ticket) and stays separate from the discrepancy
// (fund ticket) series, so the absorbed-duplicate rate can alert without
// implying a fund anomaly; divergent is already a ticket; unproven stays
// pending and never claims safe absorption. The no-duplicate verdict is not an
// observation and is not counted.
func (m *Metrics) ObserveReconDuplicate(outcome string) {
	m.recon.duplicates.WithLabelValues(reconEnum(outcome, reconDuplicateOutcomes)).Inc()
}

// ObserveReconDisposition counts one recorded disposition by kind
// (ack_only|reuse_recovery|new_fix_rule) and result
// (done|refused|failed|dry_run), making the three FR-013 classes countable.
// Operator/reason/evidence text stays on the disposition row and audit trail,
// never on a label.
func (m *Metrics) ObserveReconDisposition(kind, result string) {
	m.recon.dispositions.WithLabelValues(
		reconEnum(kind, reconDispositionKinds),
		reconEnum(result, reconDispositionResults),
	).Inc()
}

// ObserveReconAuditAction counts one recon_audit row by its frozen lifecycle
// action. Management permission rows MUST NOT be counted through this method:
// they borrow the start/close buckets and are excluded by
// ReconAuditManagementFilter / ReconAuditActionCounts, which every by-action
// aggregation must use.
func (m *Metrics) ObserveReconAuditAction(action string) {
	m.recon.auditActions.WithLabelValues(reconEnum(action, reconAuditActions)).Inc()
}

// ReconAuditManagementFilter is the mandatory predicate for every recon_audit
// aggregation by action. The frozen recon_audit.action vocabulary (migration
// 000016) has no management token: permission grant borrows the `start` bucket
// and revoke borrows `close`, while target.management_action carries the
// authoritative operation. Without this predicate the start/close buckets
// silently over-count permission management rows. No new audit token and no
// migration are introduced for this (T029).
const ReconAuditManagementFilter = `NOT (target ? 'management_action')`

// ReconAuditActionCountsQuery returns the bounded by-action aggregation behind
// txharbor_recon_audit_actions_total. It is the only approved way to aggregate
// recon_audit by action: management rows are excluded so grant/revoke never
// inflate start/close, and the frozen action vocabulary keeps the series
// bounded.
func ReconAuditActionCountsQuery() string {
	return "SELECT action::text, count(*)::bigint\n" +
		"FROM recon_audit\n" +
		"WHERE " + ReconAuditManagementFilter + "\n" +
		"GROUP BY action\n" +
		"ORDER BY action"
}

// ReconAuditRow is one recon_audit row projected for action counting: the
// frozen action token and the JSONB target (which carries management_action on
// permission management rows).
type ReconAuditRow struct {
	Action string
	Target []byte
}

// ReconAuditManagementRow reports whether target marks a management row, i.e.
// whether the top-level management_action key is present. That is exactly the
// `target ? 'management_action'` semantics (key presence, not value truth): a
// present-but-null key is still a management row, and an unparseable target is
// never silently excluded.
func ReconAuditManagementRow(target []byte) bool {
	if len(target) == 0 {
		return false
	}
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(target, &doc); err != nil {
		// Migration 000016 constrains target to a JSON object, so this only
		// fires on a non-DB caller bug; fail open (counted) rather than
		// silently dropping an audit row.
		return false
	}
	_, ok := doc["management_action"]
	return ok
}

// ReconAuditActionCounts aggregates audit rows by action with management rows
// excluded (the Go equivalent of ReconAuditActionCountsQuery for callers that
// already hold the rows). Actions outside the frozen vocabulary collapse into
// the bounded "other" bucket, so no caller-supplied text can widen the series.
func ReconAuditActionCounts(rows []ReconAuditRow) map[string]int64 {
	counts := make(map[string]int64, len(reconAuditActions))
	for _, row := range rows {
		if ReconAuditManagementRow(row.Target) {
			continue
		}
		counts[reconEnum(row.Action, reconAuditActions)]++
	}
	return counts
}
