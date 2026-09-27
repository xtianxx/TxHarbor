package metrics

import (
	"reflect"
	"sort"
	"strings"
	"testing"

	dto "github.com/prometheus/client_model/go"
)

// reconManifest is the frozen 014 registration table (T029): exposition name,
// Prometheus type and the exact label set. A change here is an observability
// contract change (FR-024/FR-027; constitution XII).
var reconManifest = []struct {
	name   string
	typ    dto.MetricType
	labels []string
}{
	{ReconTaskStateMetricName, dto.MetricType_GAUGE, []string{"scope_kind", "state"}},
	{ReconScanProgressMetricName, dto.MetricType_GAUGE, []string{"scope_kind"}},
	{ReconHistorySweepProgressMetricName, dto.MetricType_GAUGE, []string{"scope_kind"}},
	{ReconVerificationsMetricName, dto.MetricType_COUNTER, []string{"verdict"}},
	{ReconGapsOpenMetricName, dto.MetricType_GAUGE, []string{"reason"}},
	{ReconDiscrepanciesMetricName, dto.MetricType_GAUGE, []string{"category", "state"}},
	{ReconDuplicatesMetricName, dto.MetricType_COUNTER, []string{"outcome"}},
	{ReconDispositionsMetricName, dto.MetricType_COUNTER, []string{"kind", "result"}},
	{ReconAuditActionsMetricName, dto.MetricType_COUNTER, []string{"action"}},
}

// observeAllRecon initializes every 014 series with one sample so the gathered
// registry exposes each family (Vec families are absent until observed).
func observeAllRecon(m *Metrics) {
	for _, scope := range reconScopeKinds {
		for _, state := range reconTaskStates {
			m.SetReconTaskState(scope, state, 1)
		}
		m.SetReconScanProgress(scope, 100, true)
		m.SetReconHistorySweepProgress(scope, 50, true)
	}
	for _, reason := range reconGapReasons {
		m.SetReconOpenGaps(reason, 1)
	}
	for _, category := range reconCategories {
		for _, state := range reconTicketState {
			m.SetReconDiscrepancies(category, state, 1)
		}
	}
	for _, verdict := range reconVerdicts {
		m.ObserveReconVerification(verdict)
	}
	for _, outcome := range reconDuplicateOutcomes {
		m.ObserveReconDuplicate(outcome)
	}
	for _, kind := range reconDispositionKinds {
		for _, result := range reconDispositionResults {
			m.ObserveReconDisposition(kind, result)
		}
	}
	for _, action := range reconAuditActions {
		m.ObserveReconAuditAction(action)
	}
}

// TestReconMetricsRegistryManifest is the T029 registry-manifest test: every
// 014 family is registered with the exact name, type and label set, and the
// group adds exactly nine recon series to the shared registry.
func TestReconMetricsRegistryManifest(t *testing.T) {
	m := New(func() bool { return true })
	observeAllRecon(m)

	families, err := m.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	byName := make(map[string]*dto.MetricFamily, len(families))
	reconFamilies := 0
	for _, f := range families {
		byName[f.GetName()] = f
		if strings.HasPrefix(f.GetName(), "txharbor_recon_") {
			reconFamilies++
		}
	}
	if reconFamilies != len(reconManifest) {
		t.Errorf("gathered %d txharbor_recon_* families, want %d", reconFamilies, len(reconManifest))
	}

	for _, want := range reconManifest {
		f, ok := byName[want.name]
		if !ok {
			t.Errorf("missing 014 metric %q", want.name)
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

	// Observing the whole vocabulary yields exactly the bounded label-product
	// series count per family: no label value ever splits a series beyond the
	// frozen enum product.
	wantSeries := map[string]int{
		ReconTaskStateMetricName:            len(reconScopeKinds) * len(reconTaskStates),
		ReconScanProgressMetricName:         len(reconScopeKinds),
		ReconHistorySweepProgressMetricName: len(reconScopeKinds),
		ReconVerificationsMetricName:        len(reconVerdicts),
		ReconGapsOpenMetricName:             len(reconGapReasons),
		ReconDiscrepanciesMetricName:        len(reconCategories) * len(reconTicketState),
		ReconDuplicatesMetricName:           len(reconDuplicateOutcomes),
		ReconDispositionsMetricName:         len(reconDispositionKinds) * len(reconDispositionResults),
		ReconAuditActionsMetricName:         len(reconAuditActions),
	}
	for name, want := range wantSeries {
		if got := len(byName[name].GetMetric()); got != want {
			t.Errorf("%s exposed %d series, want %d (bounded enum product)", name, got, want)
		}
	}
}

// TestReconLabelValuesStayInVocabulary freezes the FR-024/FR-027 redaction
// boundary: label NAMES are a fixed allowlist and label VALUES come only from
// the frozen enums (or the single bounded "other" sink). Principal, task-id,
// request-id, payload and credential-shaped caller input can never become a
// label value.
func TestReconLabelValuesStayInVocabulary(t *testing.T) {
	m := New(func() bool { return true })

	hostile := []string{
		"principal:apikey:local:ops",
		"11111111-2222-3333-4444-555555555555",
		`{"request_id":"req-9","payload":"0xdeadbeef"}`,
		"Bearer sk-live-secret-token",
		"tx-hash-0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd",
	}
	for _, value := range hostile {
		m.SetReconTaskState(value, value, 1)
		m.SetReconScanProgress(value, 7, true)
		m.SetReconHistorySweepProgress(value, 7, true)
		m.ObserveReconVerification(value)
		m.SetReconOpenGaps(value, 1)
		m.SetReconDiscrepancies(value, value, 1)
		m.ObserveReconDuplicate(value)
		m.ObserveReconDisposition(value, value)
		m.ObserveReconAuditAction(value)
	}

	allowedLabelNames := map[string]bool{
		"scope_kind": true, "state": true, "verdict": true, "reason": true,
		"category": true, "outcome": true, "kind": true, "result": true,
		"action": true,
	}
	allowedValues := map[string]bool{reconOtherLabel: true}
	for _, values := range [][]string{
		reconScopeKinds, reconTaskStates, reconGapReasons, reconCategories,
		reconTicketState, reconVerdicts, reconDuplicateOutcomes,
		reconDispositionKinds, reconDispositionResults, reconAuditActions,
	} {
		for _, value := range values {
			allowedValues[value] = true
		}
	}

	families, err := m.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	sawOther := false
	for _, f := range families {
		if !strings.HasPrefix(f.GetName(), "txharbor_recon_") {
			continue
		}
		for _, metric := range f.GetMetric() {
			for _, lp := range metric.GetLabel() {
				if !allowedLabelNames[lp.GetName()] {
					t.Errorf("%s carries unapproved label %q", f.GetName(), lp.GetName())
				}
				if !allowedValues[lp.GetValue()] {
					t.Errorf("%s label %q leaked caller value %q (FR-024/FR-027)",
						f.GetName(), lp.GetName(), lp.GetValue())
				}
				if lp.GetValue() == reconOtherLabel {
					sawOther = true
				}
			}
		}
	}
	if !sawOther {
		t.Fatal("out-of-vocabulary input did not collapse into the bounded other bucket")
	}
}

// TestReconProgressSeriesAreDistinct freezes the honesty rule behind the four
// progress signals: scan progress, history-sweep progress, verification
// completeness and open gaps are separate series. An empty checkpoint deletes
// its series (unknown, never a fabricated zero) without touching the others,
// and stale/unknown verdicts are never counted as consistent.
func TestReconProgressSeriesAreDistinct(t *testing.T) {
	m := New(func() bool { return true })

	m.SetReconScanProgress("height", 42, true)
	m.SetReconHistorySweepProgress("height", 10, true)
	if got := gatherGauges(t, m, ReconScanProgressMetricName)["scope_kind=height"]; got != 42 {
		t.Fatalf("%s{height} = %v, want 42", ReconScanProgressMetricName, got)
	}
	if got := gatherGauges(t, m, ReconHistorySweepProgressMetricName)["scope_kind=height"]; got != 10 {
		t.Fatalf("%s{height} = %v, want 10", ReconHistorySweepProgressMetricName, got)
	}

	// Empty scan progress removes only the scan series: the sweep waterline
	// and the verification/gap series are untouched and still exposed.
	m.SetReconScanProgress("height", 42, false)
	if got := familyLen(t, m, ReconScanProgressMetricName); got != 0 {
		t.Fatalf("empty scan checkpoint exposed %d %s series, want 0", got, ReconScanProgressMetricName)
	}
	if got := gatherGauges(t, m, ReconHistorySweepProgressMetricName)["scope_kind=height"]; got != 10 {
		t.Fatalf("%s{height} = %v after scan delete, want 10", ReconHistorySweepProgressMetricName, got)
	}

	m.ObserveReconVerification("consistent")
	m.ObserveReconVerification("unknown")
	m.ObserveReconVerification("stale")
	verdicts := gatherCounters(t, m, ReconVerificationsMetricName)
	if verdicts["verdict=consistent"] != 1 || verdicts["verdict=unknown"] != 1 || verdicts["verdict=stale"] != 1 {
		t.Fatalf("%s = %v, want one per verdict", ReconVerificationsMetricName, verdicts)
	}
	if verdicts["verdict=divergent"] != 0 {
		t.Fatalf("incomplete verdicts leaked into a consistent/divergent bucket: %v", verdicts)
	}

	m.SetReconOpenGaps("paused", 3)
	m.SetReconOpenGaps("query_failed", 2)
	gaps := gatherGauges(t, m, ReconGapsOpenMetricName)
	if gaps["reason=paused"] != 3 || gaps["reason=query_failed"] != 2 {
		t.Fatalf("%s = %v, want paused=3 query_failed=2", ReconGapsOpenMetricName, gaps)
	}
}

// TestReconDuplicatesSeparatedFromFundTickets freezes Q4: absorbed duplicates
// are metrics-only and never create a fund-ticket series, while divergent
// duplicates are counted on the duplicate series only (the ticket itself stays
// a discrepancy row).
func TestReconDuplicatesSeparatedFromFundTickets(t *testing.T) {
	m := New(func() bool { return true })

	m.ObserveReconDuplicate("absorbed")
	m.ObserveReconDuplicate("absorbed")
	m.ObserveReconDuplicate("unproven")
	duplicates := gatherCounters(t, m, ReconDuplicatesMetricName)
	if duplicates["outcome=absorbed"] != 2 || duplicates["outcome=unproven"] != 1 {
		t.Fatalf("%s = %v, want absorbed=2 unproven=1", ReconDuplicatesMetricName, duplicates)
	}
	if got := familyLen(t, m, ReconDiscrepanciesMetricName); got != 0 {
		t.Fatalf("absorbed duplicate exposed %d %s series, want 0 (never a fund ticket)", got, ReconDiscrepanciesMetricName)
	}
	if got := familyLen(t, m, ReconGapsOpenMetricName); got != 0 {
		t.Fatalf("absorbed duplicate exposed %d %s series, want 0", got, ReconGapsOpenMetricName)
	}

	m.ObserveReconDuplicate("divergent")
	if got := gatherCounters(t, m, ReconDuplicatesMetricName)["outcome=divergent"]; got != 1 {
		t.Fatalf("%s{divergent} = %v, want 1", ReconDuplicatesMetricName, got)
	}

	// Fund tickets are counted on their own series by category and state.
	m.SetReconDiscrepancies("missing", "open_claimable", 1)
	m.SetReconDiscrepancies("duplicate_divergent", "open_claimable", 1)
	tickets := gatherGauges(t, m, ReconDiscrepanciesMetricName)
	if tickets["category=missing,state=open_claimable"] != 1 ||
		tickets["category=duplicate_divergent,state=open_claimable"] != 1 {
		t.Fatalf("%s = %v, want one ticket per category", ReconDiscrepanciesMetricName, tickets)
	}
	if got := gatherCounters(t, m, ReconDuplicatesMetricName)["outcome=absorbed"]; got != 2 {
		t.Fatalf("%s{absorbed} = %v after ticket snapshot, want 2 (separate series)", ReconDuplicatesMetricName, got)
	}
}

// TestReconAuditManagementFilterExcludesManagementRows is the T029 management
// discriminator test. The frozen recon_audit action vocabulary has no
// management token: grant borrows `start`, revoke borrows `close`, and
// target.management_action is the discriminator. Both the SQL aggregation and
// its Go equivalent must exclude those rows, so grant/revoke never inflate the
// start/close buckets (no new audit token, no migration).
func TestReconAuditManagementFilterExcludesManagementRows(t *testing.T) {
	if !strings.Contains(ReconAuditManagementFilter, `target ? 'management_action'`) {
		t.Fatalf("management filter %q does not use target ? 'management_action'", ReconAuditManagementFilter)
	}
	if !strings.HasPrefix(ReconAuditManagementFilter, "NOT (") {
		t.Fatalf("management filter %q must be an exclusion predicate", ReconAuditManagementFilter)
	}
	query := ReconAuditActionCountsQuery()
	if !strings.Contains(query, ReconAuditManagementFilter) {
		t.Fatalf("aggregation query %q omits the management filter", query)
	}
	if !strings.Contains(query, "FROM recon_audit") || !strings.Contains(query, "GROUP BY action") {
		t.Fatalf("aggregation query %q is not a recon_audit by-action aggregation", query)
	}

	grant := []byte(`{"management_action":"grant","actor_principal":"local:ops","target_principal":"svc:recon","permission":"scan_manage","operation_id":"op-1","state_before":{"present":false},"state_after":{"present":true}}`)
	revoke := []byte(`{"management_action":"revoke","actor_principal":"local:ops","target_principal":"svc:recon","permission":"close","operation_id":"op-2"}`)
	rows := []ReconAuditRow{
		{Action: "start", Target: grant},                                   // management grant in the start bucket
		{Action: "close", Target: revoke},                                  // management revoke in the close bucket
		{Action: "refuse", Target: []byte(`{"management_action":null}`)},   // present-null key is still management
		{Action: "start", Target: []byte(`{"task_id":"t-1"}`)},             // real lifecycle start
		{Action: "close", Target: []byte(`{}`)},                            // real lifecycle close
		{Action: "claim", Target: []byte(`{"task_id":"t-1","owner":"o"}`)}, // untouched action
		{Action: "resume", Target: []byte(`not-json`)},                     // unparseable: counted, not silently dropped
		{Action: "dispose", Target: nil},                                   // no target: counted
	}
	counts := ReconAuditActionCounts(rows)
	want := map[string]int64{"start": 1, "close": 1, "claim": 1, "resume": 1, "dispose": 1}
	if !reflect.DeepEqual(counts, want) {
		t.Fatalf("ReconAuditActionCounts = %v, want %v (grant/revoke must not inflate start/close)", counts, want)
	}

	if !ReconAuditManagementRow(grant) || !ReconAuditManagementRow(revoke) {
		t.Fatal("management rows are not recognized by key presence")
	}
	if ReconAuditManagementRow([]byte(`{"task_id":"t-1"}`)) || ReconAuditManagementRow(nil) ||
		ReconAuditManagementRow([]byte(`{}`)) {
		t.Fatal("lifecycle rows must not be classified as management rows")
	}

	// The counter itself only accepts the frozen action vocabulary: the
	// management token "grant" is not an action and collapses to the bounded
	// other bucket instead of creating a new series.
	m := New(func() bool { return true })
	m.ObserveReconAuditAction("grant")
	actions := gatherCounters(t, m, ReconAuditActionsMetricName)
	if actions["action=other"] != 1 {
		t.Fatalf("%s = %v, want the non-action token in the bounded other bucket", ReconAuditActionsMetricName, actions)
	}
}

// TestReconMetricsIndependentRegistries pins the private-registry pattern: a
// second New never panics on a duplicate registration and carries no stale 014
// series.
func TestReconMetricsIndependentRegistries(t *testing.T) {
	m1 := New(func() bool { return true })
	m1.ObserveReconDuplicate("absorbed")
	m2 := New(func() bool { return true })
	if got := familyLen(t, m2, ReconDuplicatesMetricName); got != 0 {
		t.Fatalf("fresh registry exposed %d %s series, want 0", got, ReconDuplicatesMetricName)
	}
	if got := gatherCounters(t, m1, ReconDuplicatesMetricName)["outcome=absorbed"]; got != 1 {
		t.Fatalf("%s{absorbed} = %v on the first registry, want 1", ReconDuplicatesMetricName, got)
	}
}

// gatherGauges reads one gauge family into a label-keyed map.
func gatherGauges(t *testing.T, m *Metrics, name string) map[string]float64 {
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
			out[key] = metric.GetGauge().GetValue()
		}
	}
	return out
}
