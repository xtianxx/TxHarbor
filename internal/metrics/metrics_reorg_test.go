package metrics

import (
	"regexp"
	"strings"
	"testing"

	"github.com/xtianxx/txharbor/internal/logx"
)

// TestReorgMetricsContract covers the 006 observability contract: the eight
// reorg series are registered with exact names and types, active/reconcile
// expose 1/0, depth/bound are paired gauges, frontier lag deletes per
// chain+stream while progress is empty, and the conversion/evidence counters
// are monotonic.
func TestReorgMetricsContract(t *testing.T) {
	m := New(func() bool { return true })

	m.ObserveReorgActive(31337, true)
	m.ObserveReorgDepthBound(31337, 5, 25)
	for _, stream := range []string{"block", "log", "deposit"} {
		m.ObserveReorgFrontierLag(31337, stream, 3, true)
	}
	m.ObserveReorgOrphaned(31337)
	m.ObserveReorgOrphaned(31337)
	m.ObserveReorgRevived(31337)
	m.ObserveReorgReconcile(31337, true)
	m.ObserveReorgEvidenceWait(31337, "timeout")
	m.ObserveReorgEvidenceWait(31337, "timeout")

	families, err := m.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	byName := map[string]string{}
	for _, f := range families {
		byName[f.GetName()] = f.GetType().String()
	}
	wantTypes := map[string]string{
		ReorgActiveMetricName:       "GAUGE",
		ReorgDepthMetricName:        "GAUGE",
		ReorgBoundMetricName:        "GAUGE",
		ReorgFrontierLagMetricName:  "GAUGE",
		ReorgOrphanedMetricName:     "COUNTER",
		ReorgRevivedMetricName:      "COUNTER",
		ReorgReconcileMetricName:    "GAUGE",
		ReorgEvidenceWaitMetricName: "COUNTER",
	}
	for name, typ := range wantTypes {
		if got, ok := byName[name]; !ok {
			t.Errorf("reorg series %q missing from Gatherer output", name)
		} else if got != typ {
			t.Errorf("reorg series %q has type %s, want %s", name, got, typ)
		}
	}

	if got := gatherGauge(t, m, ReorgActiveMetricName); got != 1 {
		t.Fatalf("%s = %v, want 1 while recovery row exists", ReorgActiveMetricName, got)
	}
	m.ObserveReorgActive(31337, false)
	if got := gatherGauge(t, m, ReorgActiveMetricName); got != 0 {
		t.Fatalf("%s = %v, want 0 after release", ReorgActiveMetricName, got)
	}

	if got := gatherGauge(t, m, ReorgDepthMetricName); got != 5 {
		t.Fatalf("%s = %v, want 5", ReorgDepthMetricName, got)
	}
	if got := gatherGauge(t, m, ReorgBoundMetricName); got != 25 {
		t.Fatalf("%s = %v, want 25", ReorgBoundMetricName, got)
	}

	if got := familyLen(t, m, ReorgFrontierLagMetricName); got != 3 {
		t.Fatalf("exposed %d %s series, want 3 (block|log|deposit)", got, ReorgFrontierLagMetricName)
	}
	m.ObserveReorgFrontierLag(31337, "log", 0, false)
	if got := familyLen(t, m, ReorgFrontierLagMetricName); got != 2 {
		t.Fatalf("empty log progress exposed %d %s series, want 2", got, ReorgFrontierLagMetricName)
	}

	if got := gatherCounters(t, m, ReorgOrphanedMetricName)["chain=31337"]; got != 2 {
		t.Fatalf("%s = %v, want 2 (monotonic)", ReorgOrphanedMetricName, got)
	}
	if got := gatherCounters(t, m, ReorgRevivedMetricName)["chain=31337"]; got != 1 {
		t.Fatalf("%s = %v, want 1 (monotonic)", ReorgRevivedMetricName, got)
	}

	if got := gatherGauge(t, m, ReorgReconcileMetricName); got != 1 {
		t.Fatalf("%s = %v, want 1 while reconcile_required", ReorgReconcileMetricName, got)
	}
	m.ObserveReorgReconcile(31337, false)
	if got := gatherGauge(t, m, ReorgReconcileMetricName); got != 0 {
		t.Fatalf("%s = %v, want 0 after release", ReorgReconcileMetricName, got)
	}

	if got := gatherCounters(t, m, ReorgEvidenceWaitMetricName)["chain=31337,class=timeout"]; got != 2 {
		t.Fatalf("%s{timeout} = %v, want 2", ReorgEvidenceWaitMetricName, got)
	}
}

// TestReorgMetricsRedaction enforces the SC-12 boundary on the new surface:
// metric names and Help strings plus a terminalDetail-shaped diagnostic carry
// heights/hashes/ranges/versions with zero secret/plaintext-credential
// occurrences. 0x + 64-hex is hash evidence (allowed and asserted present);
// credential-shaped material must be absent, and logx.Redact must leave the
// clean diagnostic unchanged while scrubbing a planted fake secret.
func TestReorgMetricsRedaction(t *testing.T) {
	m := New(func() bool { return true })
	m.ObserveReorgActive(31337, true)
	m.ObserveReorgDepthBound(31337, 5, 25)
	m.ObserveReorgFrontierLag(31337, "block", 3, true)
	m.ObserveReorgOrphaned(31337)
	m.ObserveReorgRevived(31337)
	m.ObserveReorgReconcile(31337, true)
	m.ObserveReorgEvidenceWait(31337, "timeout")

	families, err := m.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	reorgNames := map[string]bool{
		ReorgActiveMetricName: true, ReorgDepthMetricName: true,
		ReorgBoundMetricName: true, ReorgFrontierLagMetricName: true,
		ReorgOrphanedMetricName: true, ReorgRevivedMetricName: true,
		ReorgReconcileMetricName: true, ReorgEvidenceWaitMetricName: true,
	}
	secretRe := regexp.MustCompile(`(?i)private[_-]?key|BEGIN[^-]*PRIVATE|password|passwd|pwd|secret|api[_-]?key|access[_-]?key|authorization|bearer|dsn\s*=`)
	var namesAndHelp strings.Builder
	for _, f := range families {
		if !reorgNames[f.GetName()] {
			continue
		}
		namesAndHelp.WriteString(f.GetName())
		namesAndHelp.WriteString("\n")
		namesAndHelp.WriteString(f.GetHelp())
		namesAndHelp.WriteString("\n")
	}
	if got := namesAndHelp.String(); secretRe.MatchString(got) {
		t.Fatalf("reorg metric names/help carry credential-shaped text: %q", got)
	}

	// Sample shaped like the real terminalDetail render (reorgcommit.go):
	// bound tip, ancestor, swept range, disposition list, surviving pauses,
	// policy + fencing versions — heights, hashes, ranges, versions retained.
	oldHash := "0x" + strings.Repeat("ab", 32)
	ancestorHash := "0x" + strings.Repeat("cd", 32)
	detail := "recovery=0193b2f1-0001 event=auto_completed bound_old=120:" + oldHash +
		" policy=3 ancestor=115:" + ancestorHash +
		" swept=116-120 disposition=[orphaned=2 revived=1] surviving_pauses=[] version=2"

	hashRe := regexp.MustCompile(`0x[0-9a-fA-F]{64}`)
	if !hashRe.MatchString(detail) {
		t.Fatalf("diagnostic sample lost hash evidence: %q", detail)
	}
	for _, want := range []string{"120", "115", "116-120", "policy=3", "version=2"} {
		if !strings.Contains(detail, want) {
			t.Fatalf("diagnostic sample lost %q: %q", want, detail)
		}
	}
	if secretRe.MatchString(detail) {
		t.Fatalf("diagnostic sample carries credential-shaped text: %q", detail)
	}
	if got := logx.Redact(detail); got != detail {
		t.Fatalf("logx.Redact altered clean heights/hashes diagnostic:\n got %q\nwant %q", got, detail)
	}

	planted := detail + " password=hunter2"
	scrubbed := logx.Redact(planted)
	if strings.Contains(scrubbed, "hunter2") {
		t.Fatalf("logx.Redact(%q) leaked the planted secret: %q", planted, scrubbed)
	}
	if !strings.Contains(scrubbed, logx.Redacted) {
		t.Fatalf("logx.Redact(%q) = %q, want %s", planted, scrubbed, logx.Redacted)
	}
	if !strings.Contains(scrubbed, "116-120") || !hashRe.MatchString(scrubbed) {
		t.Fatalf("logx.Redact dropped heights/hashes while scrubbing: %q", scrubbed)
	}
}
