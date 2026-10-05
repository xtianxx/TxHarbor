package metrics

import (
	"testing"
	"time"
)

// TestRecoveryMetricsContract covers the T055 surface: exact names/types, the
// bounded label vocabulary (out-of-vocabulary values collapse into "other"),
// separate timing scopes (unknown is absent, never a fabricated zero), the
// explicit constraint-configured 0 and the RTO result classes.
func TestRecoveryMetricsContract(t *testing.T) {
	m := New(func() bool { return true })

	m.ObserveRecoveryRelease("query", "released")
	m.ObserveRecoveryRelease("new_withdrawal_creation", "refused")
	m.ObserveRecoveryRelease("11111111-1111-4111-8111-111111111111", "released") // instance id is never a capability label
	m.ObserveRecoveryRefusal("query", "gap_open")
	m.ObserveRecoveryRefusal("query", "not-a-real-class")
	m.SetRecoveryGaps("open", 2)
	m.SetRecoveryGaps("escalated", 1)
	m.SetRecoveryGaps("closed", -3) // clamped to known-empty 0
	m.ObserveRecoveryGapDisposition("closed_by_evidence")
	m.ObserveRecoveryGapDisposition("note_timeout")
	m.ObserveRecoveryEvidenceWrite("accepted")
	m.ObserveRecoveryEvidenceWrite("discarded")
	m.ObserveRecoveryEvidenceWrite("retried")
	m.ObserveRecoveryPoint(time.Unix(1700000000, 0).UTC())
	m.ObserveDBRestoreSeconds(12.5)
	m.ObserveVerificationSeconds(3.25)
	m.ObserveCapabilityReleaseSeconds("chain_scan", 30)
	m.ObserveBackupLag(60, true)
	m.ObserveUncoveredInterval(90, true)
	m.SetRecoveryConstraintConfigured("rto_target", true)
	m.SetRecoveryConstraintConfigured("retention", false)

	families, err := m.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	byName := map[string]string{}
	for _, f := range families {
		byName[f.GetName()] = f.GetType().String()
	}
	wantTypes := map[string]string{
		RecoveryReleaseMetricName:                  "COUNTER",
		RecoveryRefusalMetricName:                  "COUNTER",
		RecoveryGapsMetricName:                     "GAUGE",
		RecoveryGapDispositionsMetricName:          "COUNTER",
		RecoveryEvidenceWritesMetricName:           "COUNTER",
		RecoveryRecoveryPointMetricName:            "GAUGE",
		RecoveryDBRestoreSecondsMetricName:         "GAUGE",
		RecoveryVerificationSecondsMetricName:      "GAUGE",
		RecoveryCapabilityReleaseSecondsMetricName: "GAUGE",
		RecoveryBackupLagSecondsMetricName:         "GAUGE",
		RecoveryUncoveredIntervalSecondsMetricName: "GAUGE",
		RecoveryConstraintConfiguredMetricName:     "GAUGE",
	}
	for name, typ := range wantTypes {
		if got, ok := byName[name]; !ok {
			t.Errorf("recovery series %q missing from Gatherer output", name)
		} else if got != typ {
			t.Errorf("recovery series %q has type %s, want %s", name, got, typ)
		}
	}

	releases := gatherCounters(t, m, RecoveryReleaseMetricName)
	if got := releases["capability=query,result=released"]; got != 1 {
		t.Errorf("%s{query,released} = %v, want 1", RecoveryReleaseMetricName, got)
	}
	if got := releases["capability=other,result=released"]; got != 1 {
		t.Errorf("an id-like capability must collapse into the bounded other bucket, got %v (all: %v)", got, releases)
	}
	refusals := gatherCounters(t, m, RecoveryRefusalMetricName)
	if got := refusals["capability=query,refusal_class=gap_open"]; got != 1 {
		t.Errorf("%s{query,gap_open} = %v, want 1", RecoveryRefusalMetricName, got)
	}
	if got := refusals["capability=query,refusal_class=other"]; got != 1 {
		t.Errorf("an out-of-vocabulary refusal class must collapse into other, got %v", got)
	}

	gaps := gatherGauges(t, m, RecoveryGapsMetricName)
	if gaps["state=open"] != 2 || gaps["state=escalated"] != 1 || gaps["state=closed"] != 0 {
		t.Errorf("%s = %v, want open=2 escalated=1 closed=0", RecoveryGapsMetricName, gaps)
	}
	if got := gatherCounters(t, m, RecoveryGapDispositionsMetricName)["disposition=note_timeout"]; got != 1 {
		t.Errorf("timeout note disposition = %v, want 1 (an observation, never a closure)", got)
	}
	if got := gatherCounters(t, m, RecoveryEvidenceWritesMetricName); got["result=discarded"] != 1 || got["result=retried"] != 1 {
		t.Errorf("%s = %v, want discarded=1 retried=1", RecoveryEvidenceWritesMetricName, got)
	}

	if got := gatherGauge(t, m, RecoveryRecoveryPointMetricName); got != 1700000000 {
		t.Errorf("%s = %v, want 1700000000", RecoveryRecoveryPointMetricName, got)
	}
	if got := gatherGauge(t, m, RecoveryDBRestoreSecondsMetricName); got != 12.5 {
		t.Errorf("%s = %v, want 12.5", RecoveryDBRestoreSecondsMetricName, got)
	}
	if got := gatherGauge(t, m, RecoveryVerificationSecondsMetricName); got != 3.25 {
		t.Errorf("%s = %v, want 3.25", RecoveryVerificationSecondsMetricName, got)
	}
	if got := gatherGauges(t, m, RecoveryCapabilityReleaseSecondsMetricName)["capability=chain_scan"]; got != 30 {
		t.Errorf("%s{chain_scan} = %v, want 30", RecoveryCapabilityReleaseSecondsMetricName, got)
	}
	if got := gatherGauge(t, m, RecoveryBackupLagSecondsMetricName); got != 60 {
		t.Errorf("%s = %v, want 60", RecoveryBackupLagSecondsMetricName, got)
	}
	if got := gatherGauge(t, m, RecoveryUncoveredIntervalSecondsMetricName); got != 90 {
		t.Errorf("%s = %v, want 90", RecoveryUncoveredIntervalSecondsMetricName, got)
	}
	constraints := gatherGauges(t, m, RecoveryConstraintConfiguredMetricName)
	if constraints["constraint=rto_target"] != 1 || constraints["constraint=retention"] != 0 {
		t.Errorf("%s = %v, want rto_target=1 retention=0 (explicit not-configured)", RecoveryConstraintConfiguredMetricName, constraints)
	}
}

// TestRecoveryMetricsUnknownMeasuresAreAbsent pins the "unknown is never a
// fabricated zero" rule for the lag/interval/recovery-point series.
func TestRecoveryMetricsUnknownMeasuresAreAbsent(t *testing.T) {
	m := New(func() bool { return true })
	m.ObserveBackupLag(5, true)
	m.ObserveUncoveredInterval(7, true)
	m.ObserveRecoveryPoint(time.Unix(1700000000, 0).UTC())
	m.ObserveBackupLag(0, false)
	m.ObserveUncoveredInterval(0, false)
	m.ObserveRecoveryPointUnknown()
	// Re-observe a second metric to keep the family non-empty where needed.
	m.ObserveDBRestoreSeconds(1)

	for _, name := range []string{
		RecoveryBackupLagSecondsMetricName,
		RecoveryUncoveredIntervalSecondsMetricName,
		RecoveryRecoveryPointMetricName,
	} {
		if got := familyLen(t, m, name); got != 0 {
			t.Errorf("%s exposed %d series after unknown/cleared, want 0 (absent, never zero)", name, got)
		}
	}
}

// TestEvaluateRTOTargetDiscipline is the C1 heart of T055: a configured
// target is only ever judged end-to-end, a single scope never produces a
// verdict, and an exceeded target is recorded not-met + alert + escalation
// without ever becoming a permanent resumption block.
func TestEvaluateRTOTargetDiscipline(t *testing.T) {
	cases := []struct {
		name          string
		target        time.Duration
		measured      time.Duration
		scope         RTOScope
		known         bool
		wantStatus    RTOStatus
		wantNotMet    bool
		wantAlert     bool
		wantEscalate  bool
		wantPermBlock bool
	}{
		{
			name: "unconfigured target claims nothing even with a measurement",
			// target 0 = not configured.
			measured: 10 * time.Second, scope: RTOScopeSafeResumption, known: true,
			wantStatus: RTOStatusNotConfigured,
		},
		{
			name:   "db restore scope alone never verdicts even under the target",
			target: time.Minute, measured: 10 * time.Second, scope: RTOScopeDBRestoreOnly, known: true,
			wantStatus: RTOStatusNotMeasured,
		},
		{
			name:   "verification scope alone never verdicts",
			target: time.Minute, measured: 10 * time.Second, scope: RTOScopeVerificationOnly, known: true,
			wantStatus: RTOStatusNotMeasured,
		},
		{
			name:   "unmeasured safe resumption claims nothing",
			target: time.Minute, scope: RTOScopeSafeResumption, known: false,
			wantStatus: RTOStatusNotMeasured,
		},
		{
			name:   "exceeded target is not-met + alert + escalation but never a permanent block",
			target: time.Minute, measured: 61 * time.Second, scope: RTOScopeSafeResumption, known: true,
			wantStatus: RTOStatusExceeded, wantNotMet: true, wantAlert: true, wantEscalate: true, wantPermBlock: false,
		},
		{
			name:   "met target within the safe-resumption scope",
			target: time.Minute, measured: 42 * time.Second, scope: RTOScopeSafeResumption, known: true,
			wantStatus: RTOStatusWithinTarget,
		},
		{
			name:   "exactly on the target is met (no invented margin)",
			target: time.Minute, measured: time.Minute, scope: RTOScopeSafeResumption, known: true,
			wantStatus: RTOStatusWithinTarget,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			eval := EvaluateRTOTarget(tc.target, tc.measured, tc.scope, tc.known)
			if eval.Status != tc.wantStatus {
				t.Fatalf("status = %q, want %q (reason=%q)", eval.Status, tc.wantStatus, eval.Reason)
			}
			if eval.NotMet != tc.wantNotMet || eval.Alert != tc.wantAlert || eval.EscalationRequired != tc.wantEscalate {
				t.Fatalf("disposition = not_met=%t alert=%t escalation=%t, want %t/%t/%t",
					eval.NotMet, eval.Alert, eval.EscalationRequired, tc.wantNotMet, tc.wantAlert, tc.wantEscalate)
			}
			if eval.PermanentResumptionBlock != tc.wantPermBlock {
				t.Fatalf("PermanentResumptionBlock = %t, want %t (a time target never permanently blocks safe resumption)",
					eval.PermanentResumptionBlock, tc.wantPermBlock)
			}
			if eval.Reason == "" {
				t.Fatalf("evaluation reason must never be empty")
			}
		})
	}
}

// TestObserveRTOEvaluationSeries pins the exposition side of the RTO
// evaluation: the result class counter and the target gauge (absent while
// unconfigured).
func TestObserveRTOEvaluationSeries(t *testing.T) {
	m := New(func() bool { return true })

	m.ObserveRTOEvaluation(EvaluateRTOTarget(0, 0, RTOScopeSafeResumption, false))
	if got := familyLen(t, m, RecoveryRTOTargetSecondsMetricName); got != 0 {
		t.Fatalf("unconfigured target exposed %d %s series, want 0", got, RecoveryRTOTargetSecondsMetricName)
	}
	m.ObserveRTOEvaluation(EvaluateRTOTarget(time.Minute, 61*time.Second, RTOScopeSafeResumption, true))
	m.ObserveRTOEvaluation(EvaluateRTOTarget(time.Minute, 30*time.Second, RTOScopeSafeResumption, true))
	m.ObserveRTOEvaluation(EvaluateRTOTarget(time.Minute, 5*time.Second, RTOScopeDBRestoreOnly, true))

	if got := gatherGauge(t, m, RecoveryRTOTargetSecondsMetricName); got != 60 {
		t.Fatalf("%s = %v, want 60", RecoveryRTOTargetSecondsMetricName, got)
	}
	checks := gatherCounters(t, m, RecoveryRTOChecksMetricName)
	if checks["result=exceeded"] != 1 || checks["result=within_target"] != 1 ||
		checks["result=not_configured"] != 1 || checks["result=not_measured"] != 1 {
		t.Fatalf("%s = %v, want one per class", RecoveryRTOChecksMetricName, checks)
	}
}
