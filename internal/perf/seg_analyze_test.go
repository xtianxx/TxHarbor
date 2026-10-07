//go:build perf

// seg_analyze_test.go is the reader side of the segmented normal-query batch:
//
//   - TestSegAnalyze is env-gated (TXHARBOR_SEG_ANALYZE_DIR) and turns an
//     existing evidence tree into summary.json + summary.md;
//   - TestSegAnalyzeSynthetic builds a known small tree in t.TempDir() and
//     asserts the percentiles, pairing, invariants and factor deltas, so the
//     analyzer itself is verifiable offline (no Docker, no network, no env).
//
// The analyzer never starts a server, container or connection: it reads the
// frozen JSONL/meta shapes written by the batch runner.
package perf

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestSegAnalyze analyzes TXHARBOR_SEG_ANALYZE_DIR and writes the summaries.
func TestSegAnalyze(t *testing.T) {
	root := os.Getenv("TXHARBOR_SEG_ANALYZE_DIR")
	if root == "" {
		t.Skip("TXHARBOR_SEG_ANALYZE_DIR 未设置：离线统计程序按需运行")
	}
	report, err := segAnalyzeRoot(root)
	if err != nil {
		t.Fatalf("analyze %s: %v", root, err)
	}
	jsonPath, mdPath, err := segWriteSummary(root, report)
	if err != nil {
		t.Fatalf("write summary: %v", err)
	}
	violations := 0
	for _, check := range report.Invariants.Checks {
		violations += check.Violations
	}
	t.Logf("seg analyze: cells=%d arms=%d 不变量违规=%d（%v）", report.Completeness.Cells, len(report.Arms),
		violations, report.Invariants.ViolationCounts)
	if violations > 0 {
		t.Logf("seg analyze: 违规明细见 %s（前 %d 条）", jsonPath, len(report.Invariants.Violations))
	}
	for _, rule := range report.CandidateRules {
		t.Logf("seg analyze: 候选规则（非裁决）[%s] %s — %s", rule.Status, rule.Rule, rule.Observed)
	}
	t.Logf("seg analyze: summary.json=%s summary.md=%s", jsonPath, mdPath)
}

// ------------------------------------------------------------------ fixtures

const (
	segSynthRepeatWindow = 12 * time.Second
	segSynthPrefixA      = "SELECT * FROM withdrawal_views WHERE id = $1"
	segSynthPrefixB      = "SELECT count(*) FROM chain_events WHERE block > $1"
	segSynthPrefixAKey   = "SELECT * FROM withdrawal_views…"    // first four tokens + truncation mark
	segSynthPrefixBKey   = "SELECT count(*) FROM chain_events…" // first four tokens + truncation mark
	segSynthPrefixC      = "SELECT * FROM rate_limit_buckets WHERE key = $1"
	segSynthPrefixCKey   = "SELECT * FROM rate_limit_buckets…" // first four tokens + truncation mark
)

// segSynthArmSpec is one synthetic arm's exact latency shape.
type segSynthArmSpec struct {
	Arm            string
	LimiterOn      bool
	BackgroundOn   bool
	Collect        bool
	QueryMS        float64
	AdmitMS        float64 // 0 → the limiter-off arms record admit_skipped
	AdmitSkippedMS float64
	BelowAdmitMS   float64
	ServerTotalMS  float64
	PGQAms         float64
	PGQBms         float64
	ExtraQuery     bool // nocoll: one extra query sample outside the window

	// ClockStepFromID/ClockStepDeltaMS simulate a wall-clock step: every label
	// of a record whose id is >= ClockStepFromID shifts by the delta (the meta
	// window_end shifts with it, exactly like a polluted window_end).
	ClockStepFromID       uint64
	ClockStepDeltaMS      float64
	ClockStepBeforeWindow bool   // step 1 happened before the steady window: window_start shares its timebase
	ClockStep2FromID      uint64 // optional second (window-internal) step
	ClockStep2DeltaMS     float64
	PreWindowSegments     int // segment-only requests whose ids precede the steady ids

	// QuerySamples/QuerySpacingMS, when set, replace the sparse four-sample
	// query stream with a dense one (needed to exercise the clock-step
	// detection, whose thresholds assume request spacing well under 500ms).
	QuerySamples   int
	QuerySpacingMS float64
}

func segSynthMainSpecs() []segSynthArmSpec {
	return []segSynthArmSpec{
		{Arm: "R0G0", LimiterOn: false, BackgroundOn: false, Collect: true,
			QueryMS: 40, AdmitSkippedMS: 1.0, BelowAdmitMS: 38.5, ServerTotalMS: 40.5, PGQAms: 19, PGQBms: 19},
		{Arm: "R1G0", LimiterOn: true, BackgroundOn: false, Collect: true,
			QueryMS: 50, AdmitMS: 1.5, BelowAdmitMS: 48.5, ServerTotalMS: 50.5, PGQAms: 24, PGQBms: 24},
		{Arm: "R0G1", LimiterOn: false, BackgroundOn: true, Collect: true,
			QueryMS: 42, AdmitSkippedMS: 1.0, BelowAdmitMS: 40.5, ServerTotalMS: 42.5, PGQAms: 19.75, PGQBms: 19.75},
		{Arm: "R1G1", LimiterOn: true, BackgroundOn: true, Collect: true,
			QueryMS: 52, AdmitMS: 1.6, BelowAdmitMS: 50.5, ServerTotalMS: 52.5, PGQAms: 24.75, PGQBms: 24.75},
		{Arm: "R1G1:nocoll", LimiterOn: true, BackgroundOn: true, Collect: false,
			QueryMS: 51, AdmitMS: 1.0, BelowAdmitMS: 50.0, ServerTotalMS: 51.5, PGQAms: 24.75, PGQBms: 24.75, ExtraQuery: true},
	}
}

func segSynthRFC(t time.Time) string { return t.Format(time.RFC3339Nano) }

func segSynthSampleJSON(id uint64, class string, at time.Time, ms float64, status int, ok bool, errClass string) string {
	row := fmt.Sprintf(`{"at":%q,"state":"normal","class":%q,"duration_ms":%v,"status":%d,"ok":%t`,
		segSynthRFC(at), class, ms, status, ok)
	if errClass != "" {
		row += fmt.Sprintf(`,"err_class":%q`, errClass)
	}
	if id != 0 {
		row += fmt.Sprintf(`,"perf_id":%d`, id)
	}
	return row + "}"
}

func segSynthSegJSON(name, class string, id uint64, ns int64, at time.Time) string {
	return fmt.Sprintf(`{"kind":"seg","id":%d,"seg":%q,"class":%q,"dur_ns":%d,"at_unix_ns":%d,"method":"GET","path":"/withdrawals/x"}`,
		id, name, class, ns, at.UnixNano())
}

func segSynthPgqJSON(id uint64, sql string, ns int64, at time.Time, errFlag bool) string {
	return fmt.Sprintf(`{"kind":"pgq","id":%d,"sql":%q,"dur_ns":%d,"at_unix_ns":%d,"err":%t}`,
		id, sql, ns, at.UnixNano(), errFlag)
}

func segSynthMs(v float64) int64 { return int64(math.Round(v * 1e6)) }

// segSynthWriteArm writes one arm's fixture files; gz selects the compressed
// sibling names for every JSONL file.
func segSynthWriteArm(t *testing.T, dir string, spec segSynthArmSpec, windowStart time.Time, gz bool) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	stepShift := func(id uint64, at time.Time) time.Time {
		if spec.ClockStepFromID != 0 && id >= spec.ClockStepFromID {
			at = at.Add(time.Duration(spec.ClockStepDeltaMS * float64(time.Millisecond)))
		}
		if spec.ClockStep2FromID != 0 && id >= spec.ClockStep2FromID {
			at = at.Add(time.Duration(spec.ClockStep2DeltaMS * float64(time.Millisecond)))
		}
		return at
	}
	// A pre-window step shifts window_start too: the origin is recorded after
	// the clock already moved, so the steady window lives in the same timebase.
	origin := windowStart
	if spec.ClockStepFromID != 0 && spec.ClockStepBeforeWindow {
		origin = origin.Add(time.Duration(spec.ClockStepDeltaMS * float64(time.Millisecond)))
	}
	endShift := time.Duration(0)
	if spec.ClockStepFromID != 0 && !spec.ClockStepBeforeWindow {
		endShift += time.Duration(spec.ClockStepDeltaMS * float64(time.Millisecond))
	}
	if spec.ClockStep2FromID != 0 {
		endShift += time.Duration(spec.ClockStep2DeltaMS * float64(time.Millisecond))
	}

	var clients, warmups, segs []string

	// emitSegments writes the frozen segment shape of one request id.
	emitSegments := func(id uint64, class string, label time.Time) {
		segs = append(segs, segSynthSegJSON("server_total", class, id, segSynthMs(spec.ServerTotalMS), label))
		if spec.AdmitMS > 0 {
			segs = append(segs, segSynthSegJSON("admit", class, id, segSynthMs(spec.AdmitMS), label))
		} else {
			segs = append(segs, segSynthSegJSON("admit_skipped", class, id, segSynthMs(spec.AdmitSkippedMS), label))
		}
		segs = append(segs, segSynthSegJSON("below_admit", class, id, segSynthMs(spec.BelowAdmitMS), label))
		segs = append(segs, segSynthPgqJSON(id, segSynthPrefixA, segSynthMs(spec.PGQAms), label, false))
		segs = append(segs, segSynthPgqJSON(id, segSynthPrefixB, segSynthMs(spec.PGQBms), label, false))
	}

	preCount := uint64(spec.PreWindowSegments)
	// Pre-window requests: segments only (their client samples would live in
	// warmup_samples.jsonl), ids below the steady range. Labels are paired on
	// the same grid as the steady stream, so an injected step is observed with
	// its exact magnitude.
	for i := 1; i <= spec.PreWindowSegments; i++ {
		id := uint64(i)
		natural := windowStart.Add(-time.Duration((spec.PreWindowSegments+2-i)/2) * 500 * time.Millisecond)
		emitSegments(id, "query", stepShift(id, natural))
	}

	createID := uint64(5)
	if spec.QuerySamples > 0 {
		// Dense query stream: one request every QuerySpacingMS across the whole
		// window, so adjacent-label deltas stay far below the forward-jump
		// threshold and the injected step is the only discontinuity.
		for i := 1; i <= spec.QuerySamples; i++ {
			id := uint64(i) + preCount
			label := stepShift(id, windowStart.Add(time.Duration(float64((i+1)/2)*spec.QuerySpacingMS*float64(time.Millisecond))))
			clients = append(clients, segSynthSampleJSON(id, "query", label, spec.QueryMS, 200, true, ""))
			emitSegments(id, "query", label)
		}
		createID = preCount + uint64(spec.QuerySamples) + 1
		createOffset := time.Duration(float64((spec.QuerySamples+2)/2) * spec.QuerySpacingMS * float64(time.Millisecond))
		createAt := stepShift(createID, windowStart.Add(createOffset))
		clients = append(clients, segSynthSampleJSON(createID, "create", createAt, 60, 201, true, ""))
		emitSegments(createID, "new_withdrawal", createAt)
	} else {
		offsets := map[uint64]time.Duration{1: 500 * time.Millisecond, 2: time.Second, 3: 4500 * time.Millisecond, 4: 8500 * time.Millisecond}
		for i := uint64(1); i <= 4; i++ {
			id := i + preCount
			label := stepShift(id, windowStart.Add(offsets[i]))
			clients = append(clients, segSynthSampleJSON(id, "query", label, spec.QueryMS, 200, true, ""))
			emitSegments(id, "query", label)
		}
		// The create-class request carries the same segment shape in this fixture.
		createID = preCount + 5
		createAt := stepShift(createID, windowStart.Add(time.Second))
		clients = append(clients, segSynthSampleJSON(createID, "create", createAt, 60, 201, true, ""))
		emitSegments(createID, "new_withdrawal", createAt)
	}

	if spec.ExtraQuery {
		at := stepShift(6, windowStart.Add(13*time.Second))
		clients = append(clients, segSynthSampleJSON(6, "query", at, spec.QueryMS, 200, true, ""))
		segs = append(segs, segSynthSegJSON("server_total", "query", 6, segSynthMs(spec.ServerTotalMS), at))
		segs = append(segs, segSynthSegJSON("admit", "query", 6, segSynthMs(spec.AdmitMS), at))
		segs = append(segs, segSynthSegJSON("below_admit", "query", 6, segSynthMs(spec.BelowAdmitMS), at))
		segs = append(segs, segSynthPgqJSON(6, segSynthPrefixA, segSynthMs(10), at, false))
		segs = append(segs, segSynthPgqJSON(6, segSynthPrefixB, segSynthMs(10), at, false))
		segs = append(segs, segSynthPgqJSON(6, segSynthPrefixC, segSynthMs(10), at, false))
	}

	// Warm-up: deliberately far from every steady value, so a leak into the
	// steady aggregation is visible.
	for i := range 2 {
		warmups = append(warmups, segSynthSampleJSON(uint64(90+i), "query", windowStart.Add(-5*time.Second+time.Duration(i)*time.Second), 999, 200, true, ""))
	}

	meta := fmt.Sprintf(`{
  "repeat": %d,
  "order_index": 1,
  "arm": {"name": %q, "limiter_on": %t, "background_on": %t, "collect": %t},
  "commit": "synthetic",
  "go_version": "go1.26.5",
  "started_at": %q,
  "finished_at": %q,
  "setup_seconds": 1.5,
  "warmup_start": %q,
  "warmup_end": %q,
  "window_start": %q,
  "window_end": %q,
  "counts": {"client_samples": %d, "warmup_samples": %d, "seg_records": %d, "pg_records": 0, "query_samples": 0, "errors": 0, "status_counts": {}},
  "notes": []
}
`,
		0, spec.Arm, spec.LimiterOn, spec.BackgroundOn, spec.Collect,
		segSynthRFC(origin.Add(-6*time.Second)), segSynthRFC(origin.Add(segSynthRepeatWindow)),
		segSynthRFC(origin.Add(-6*time.Second)), segSynthRFC(origin.Add(-1*time.Second)),
		segSynthRFC(origin), segSynthRFC(origin.Add(segSynthRepeatWindow+endShift)),
		len(clients), len(warmups), len(segs))
	if err := os.WriteFile(filepath.Join(dir, "meta.json"), []byte(meta), 0o644); err != nil {
		t.Fatalf("write meta: %v", err)
	}
	segSynthWriteLines(t, filepath.Join(dir, "client_samples.jsonl"), clients, gz)
	segSynthWriteLines(t, filepath.Join(dir, "warmup_samples.jsonl"), warmups, gz)
	segSynthWriteLines(t, filepath.Join(dir, "server_segments.jsonl"), segs, gz)
}

func segSynthWriteLines(t *testing.T, path string, lines []string, gz bool) {
	t.Helper()
	payload := []byte(strings.Join(lines, "\n") + "\n")
	if !gz {
		if err := os.WriteFile(path, payload, 0o644); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
		return
	}
	file, err := os.Create(path + ".gz")
	if err != nil {
		t.Fatalf("create %s.gz: %v", path, err)
	}
	writer := gzip.NewWriter(file)
	if _, err := writer.Write(payload); err != nil {
		t.Fatalf("gzip write %s.gz: %v", path, err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("gzip close %s.gz: %v", path, err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close %s.gz: %v", path, err)
	}
}

func segSynthWriteTree(t *testing.T, root string, repeats []int) {
	t.Helper()
	specs := segSynthMainSpecs()
	for _, repeat := range repeats {
		windowStart := time.Date(2026, 10, 7, 10, repeat*10, 0, 0, time.UTC)
		for _, spec := range specs {
			dir := filepath.Join(root, fmt.Sprintf("repeat%d", repeat), spec.Arm)
			// The nocoll cells prove the .jsonl.gz tolerance.
			segSynthWriteArm(t, dir, spec, windowStart, strings.Contains(spec.Arm, "nocoll"))
		}
	}
}

// ------------------------------------------------------------ synthetic test

func segSynthApprox(t *testing.T, label string, got *float64, want float64) {
	t.Helper()
	if got == nil {
		t.Errorf("%s: got nil, want %.4f", label, want)
		return
	}
	if math.Abs(*got-want) > 1e-6 {
		t.Errorf("%s: got %.6f, want %.6f", label, *got, want)
	}
}

// segSynthAssertPhasesEqual compares two phase bucket lists (counts and p95).
func segSynthAssertPhasesEqual(t *testing.T, label string, got, want []segPhaseBucketReport) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: %d buckets, want %d", label, len(got), len(want))
	}
	for i := range got {
		if got[i].N != want[i].N {
			t.Errorf("%s: bucket %s n = %d, want %d", label, got[i].Label, got[i].N, want[i].N)
		}
		switch {
		case got[i].Latency.P95MS == nil && want[i].Latency.P95MS == nil:
		case got[i].Latency.P95MS == nil || want[i].Latency.P95MS == nil:
			t.Errorf("%s: bucket %s p95 = %v, want %v", label, got[i].Label, got[i].Latency.P95MS, want[i].Latency.P95MS)
		default:
			if math.Abs(*got[i].Latency.P95MS-*want[i].Latency.P95MS) > 1e-6 {
				t.Errorf("%s: bucket %s p95 = %.6f, want %.6f", label, got[i].Label, *got[i].Latency.P95MS, *want[i].Latency.P95MS)
			}
		}
	}
}

func segSynthArmOf(t *testing.T, report *segReport, repeat int, arm string) segArmReport {
	t.Helper()
	for _, candidate := range report.Arms {
		if candidate.Repeat == repeat && candidate.Arm == arm {
			return candidate
		}
	}
	t.Fatalf("arm repeat%d/%s not found", repeat, arm)
	return segArmReport{}
}

func TestSegAnalyzeSynthetic(t *testing.T) {
	root := t.TempDir()
	segSynthWriteTree(t, root, []int{1, 2})

	report, err := segAnalyzeRoot(root)
	if err != nil {
		t.Fatalf("segAnalyzeRoot: %v", err)
	}
	if len(report.Arms) != 10 {
		t.Fatalf("arms = %d, want 10 (2 repeats × 5 arms)", len(report.Arms))
	}

	// --- client query aggregates, per arm and repeat.
	expectedP95 := map[string]float64{
		"R0G0": 40, "R1G0": 50, "R0G1": 42, "R1G1": 52, "R1G1:nocoll": 51,
	}
	for _, repeat := range []int{1, 2} {
		for arm, want := range expectedP95 {
			cell := segSynthArmOf(t, report, repeat, arm)
			wantN := 4
			if arm == "R1G1:nocoll" {
				wantN = 5
			}
			if cell.ClientQuery.N != wantN {
				t.Errorf("repeat%d/%s client query n = %d, want %d", repeat, arm, cell.ClientQuery.N, wantN)
			}
			segSynthApprox(t, fmt.Sprintf("repeat%d/%s query p50", repeat, arm), cell.ClientQuery.Latency.P50MS, want)
			segSynthApprox(t, fmt.Sprintf("repeat%d/%s query p95", repeat, arm), cell.ClientQuery.Latency.P95MS, want)
			segSynthApprox(t, fmt.Sprintf("repeat%d/%s query p99", repeat, arm), cell.ClientQuery.Latency.P99MS, want)
			segSynthApprox(t, fmt.Sprintf("repeat%d/%s query max", repeat, arm), cell.ClientQuery.Latency.MaxMS, want)
			if got := cell.ClientQuery.Status.ByStatus["200"]; got != wantN {
				t.Errorf("repeat%d/%s status 200 = %d, want %d", repeat, arm, got, wantN)
			}
			if cell.ClientQuery.Status.ByStatus["201"] != 0 {
				t.Errorf("repeat%d/%s: create samples leaked into the query class", repeat, arm)
			}
			create, ok := cell.OtherClasses["create"]
			if !ok || create.N != 1 {
				t.Errorf("repeat%d/%s create class = %+v, want n=1", repeat, arm, create)
			}
			// Warm-up stays in its own section.
			if cell.Warmup.Samples != 2 || cell.Warmup.Query.N != 2 {
				t.Errorf("repeat%d/%s warmup = %+v, want 2 samples", repeat, arm, cell.Warmup)
			}
			segSynthApprox(t, fmt.Sprintf("repeat%d/%s warmup p95", repeat, arm), cell.Warmup.Query.Latency.P95MS, 999)
		}
	}

	// --- phase buckets on a clean arm and on the nocoll arm.
	clean := segSynthArmOf(t, report, 1, "R0G0")
	if len(clean.QueryPhases) != 3 {
		t.Fatalf("query phases = %d, want 3", len(clean.QueryPhases))
	}
	for i, want := range []int{2, 1, 1} {
		if clean.QueryPhases[i].N != want {
			t.Errorf("R0G0 phase %d n = %d, want %d", i, clean.QueryPhases[i].N, want)
		}
	}
	segSynthApprox(t, "R0G0 phase0 p95", clean.QueryPhases[0].Latency.P95MS, 40)
	if clean.QueryOutside != 0 {
		t.Errorf("R0G0 outside-window samples = %d, want 0", clean.QueryOutside)
	}
	nocoll := segSynthArmOf(t, report, 2, "R1G1:nocoll")
	if nocoll.QueryOutside != 1 {
		t.Errorf("nocoll outside-window samples = %d, want 1", nocoll.QueryOutside)
	}
	if len(nocoll.ClientQuery.Status.ByStatus) != 1 || nocoll.ClientQuery.Status.ByStatus["200"] != 5 {
		t.Errorf("nocoll status counts = %v, want only 200×5", nocoll.ClientQuery.Status.ByStatus)
	}

	// --- segments and pairing.
	expectedServerTotal := map[string]float64{"R0G0": 40.5, "R1G0": 50.5, "R0G1": 42.5, "R1G1": 52.5, "R1G1:nocoll": 51.5}
	expectedBelow := map[string]float64{"R0G0": 38.5, "R1G0": 48.5, "R0G1": 40.5, "R1G1": 50.5, "R1G1:nocoll": 50.0}
	for arm := range expectedP95 {
		cell := segSynthArmOf(t, report, 1, arm)
		segSynthApprox(t, arm+" server_total p95", cell.Segments["server_total"].P95MS, expectedServerTotal[arm])
		segSynthApprox(t, arm+" below_admit p95", cell.Segments["below_admit"].P95MS, expectedBelow[arm])
		wantSegs := 5
		if arm == "R1G1:nocoll" {
			wantSegs = 6 // the extra out-of-window request is still fully segmented
		}
		if cell.Segments["server_total"].Count != wantSegs {
			t.Errorf("%s server_total records = %d, want %d", arm, cell.Segments["server_total"].Count, wantSegs)
		}
		if cell.Paired.IDs != wantSegs {
			t.Errorf("%s paired ids = %d, want %d", arm, cell.Paired.IDs, wantSegs)
		}
		segSynthApprox(t, arm+" paired server_total p95", cell.Paired.ServerTotal.P95MS, expectedServerTotal[arm])
		segSynthApprox(t, arm+" paired below_admit p95", cell.Paired.BelowAdmit.P95MS, expectedBelow[arm])
		p := cell.Pairing
		wantClient := 5 // 4 query + 1 create
		if arm == "R1G1:nocoll" {
			wantClient = 6
		}
		if p.ClientSamples != wantClient || p.ClientIDs != wantClient || p.ClientWithoutPerfID != 0 {
			t.Errorf("%s pairing client side = %+v, want %d samples/%d ids/0 without perf_id", arm, p, wantClient, wantClient)
		}
		if p.ServerTotalIDs != wantSegs || p.ClientWithoutServerTotal != 0 || p.ServerTotalWithoutClient != 0 {
			t.Errorf("%s bilateral unmatched = %+v, want 0/0 over %d ids", arm, p, wantSegs)
		}
		for kind, count := range p.DuplicateIDs {
			if count != 0 {
				t.Errorf("%s duplicate %s ids = %d, want 0", arm, kind, count)
			}
		}
		if got := p.ExtraSegmentCounts["below_admit"]; got != 0 {
			t.Errorf("%s extra below_admit = %d, want 0", arm, got)
		}
		if got := p.MissingSegmentCounts["below_admit"]; got != 0 {
			t.Errorf("%s missing below_admit = %d, want 0", arm, got)
		}
	}
	if got := segSynthArmOf(t, report, 1, "R1G0").Segments["admit"].P95MS; got == nil || math.Abs(*got-1.5) > 1e-6 {
		t.Errorf("R1G0 admit p95 = %v, want 1.5", got)
	}
	if got := segSynthArmOf(t, report, 1, "R0G0").Segments["admit_skipped"].P95MS; got == nil || math.Abs(*got-1.0) > 1e-6 {
		t.Errorf("R0G0 admit_skipped p95 = %v, want 1.0", got)
	}

	// --- pgq aggregation and per-request distribution.
	expectedPGTotal := map[string]float64{"R0G0": 38, "R1G0": 48, "R0G1": 39.5, "R1G1": 49.5}
	for arm, want := range expectedPGTotal {
		cell := segSynthArmOf(t, report, 1, arm)
		segSynthApprox(t, arm+" pg total p95", cell.PgTotal.P95MS, want)
		if cell.Pgq.Total != 10 {
			t.Errorf("%s pgq records = %d, want 10", arm, cell.Pgq.Total)
		}
		if cell.Pgq.DistinctPrefixes != 2 || len(cell.Pgq.TopPrefixes) != 2 {
			t.Errorf("%s pgq prefixes = %d distinct / %d listed, want 2/2", arm, cell.Pgq.DistinctPrefixes, len(cell.Pgq.TopPrefixes))
		}
		if cell.Pgq.TopPrefixes[0].Prefix != segSynthPrefixAKey || cell.Pgq.TopPrefixes[0].Count != 5 {
			t.Errorf("%s pgq top prefix = %+v, want %q×5", arm, cell.Pgq.TopPrefixes[0], segSynthPrefixAKey)
		}
		if cell.Pgq.TopPrefixes[1].Prefix != segSynthPrefixBKey {
			t.Errorf("%s pgq second prefix = %q, want %q", arm, cell.Pgq.TopPrefixes[1].Prefix, segSynthPrefixBKey)
		}
		if cell.PgPerRequest.Requests != 5 || cell.PgPerRequest.RequestsWithoutPGQ != 0 {
			t.Errorf("%s per-request pg base = %+v, want 5/0", arm, cell.PgPerRequest)
		}
		if got := cell.PgPerRequest.ByStatements["2"]; got != 5 {
			t.Errorf("%s per-request pg distribution = %v, want 2 statements ×5", arm, cell.PgPerRequest.ByStatements)
		}
		segSynthApprox(t, arm+" pg statements mean", cell.PgPerRequest.Mean, 2)
		segSynthApprox(t, arm+" pg statements p95", cell.PgPerRequest.P95, 2)
	}
	nocollCell := segSynthArmOf(t, report, 1, "R1G1:nocoll")
	if nocollCell.Pgq.DistinctPrefixes != 3 || nocollCell.Pgq.TopPrefixes[2].Prefix != segSynthPrefixCKey {
		t.Errorf("nocoll pgq prefixes = %d distinct / %+v, want 3 (C last)", nocollCell.Pgq.DistinctPrefixes, nocollCell.Pgq.TopPrefixes)
	}
	nocollPer := nocollCell.PgPerRequest
	if nocollPer.ByStatements["2"] != 5 || nocollPer.ByStatements["3"] != 1 {
		t.Errorf("nocoll per-request pg distribution = %v, want {2:5, 3:1}", nocollPer.ByStatements)
	}
	segSynthApprox(t, "nocoll pg statements p95", nocollPer.P95, 2.75)

	// --- invariants: clean tree, so every check must be present with zero
	// violations (the check list is never hidden when it is empty).
	wantChecks := map[string]int{
		"server_total_ge_admit_plus_below_admit(±0.05ms)": 5,
		"sum(pgq)_le_below_admit_plus_1ms":                5,
		"admit_skipped_only_when_limiter_off":             0, // this arm records admit, not admit_skipped
	}
	cell := segSynthArmOf(t, report, 1, "R1G0")
	if len(cell.Invariants.Checks) != len(wantChecks) {
		t.Fatalf("invariant checks = %d, want %d", len(cell.Invariants.Checks), len(wantChecks))
	}
	for _, check := range cell.Invariants.Checks {
		want, ok := wantChecks[check.Name]
		if !ok {
			t.Errorf("unexpected invariant check %q", check.Name)
			continue
		}
		if check.Checked != want {
			t.Errorf("check %s checked = %d, want %d", check.Name, check.Checked, want)
		}
		if check.Violations != 0 {
			t.Errorf("check %s violations = %d, want 0", check.Name, check.Violations)
		}
	}
	if got := segSynthArmOf(t, report, 1, "R1G0").Paired.IDsWithAdmitAndBelowAdmit; got != 5 {
		t.Errorf("R1G0 admit+below_admit ids = %d, want 5 (disjoint slices, not a violation)", got)
	}
	if len(report.Invariants.Violations) != 0 {
		t.Errorf("global violations = %d, want 0", len(report.Invariants.Violations))
	}
	if report.Invariants.Truncated {
		t.Error("global invariant list marked truncated on a clean tree")
	}

	// --- factor effects (differences only).
	client := segFindMetric(report.FactorEffects, "client_query_p95_ms")
	if client == nil {
		t.Fatal("client_query_p95_ms effects missing")
	}
	for name, want := range map[string]float64{
		"R_at_G0": 10, "R_at_G1": 10, "G_at_R0": 2, "G_at_R1": 2,
		"interaction": 0, "total_diff_R0G0_to_R1G1": 12,
	} {
		series, ok := client.Effects[name]
		if !ok {
			t.Errorf("effect %s missing", name)
			continue
		}
		segSynthApprox(t, "effect "+name+" median", series.MedianMS, want)
		if len(series.PerRepeat) != 2 {
			t.Errorf("effect %s per-repeat entries = %d, want 2", name, len(series.PerRepeat))
			continue
		}
		for _, entry := range series.PerRepeat {
			if math.Abs(entry.Value-want) > 1e-6 {
				t.Errorf("effect %s repeat%d = %.6f, want %.6f", name, entry.Repeat, entry.Value, want)
			}
		}
		segSynthApprox(t, "effect "+name+" range", series.RangeMS, 0)
	}
	pgTotal := segFindMetric(report.FactorEffects, "pg_total_p95_ms")
	if pgTotal == nil {
		t.Fatal("pg_total_p95_ms effects missing")
	}
	segSynthApprox(t, "pg_total R_at_G0 median", pgTotal.Effects["R_at_G0"].MedianMS, 10)
	segSynthApprox(t, "pg_total G_at_R0 median", pgTotal.Effects["G_at_R0"].MedianMS, 1.5)
	admit := segFindMetric(report.FactorEffects, "admit_p95_ms")
	if admit == nil {
		t.Fatal("admit_p95_ms effects missing")
	}
	if series, ok := admit.Effects["R_at_G0"]; !ok || series.MedianMS != nil {
		t.Errorf("admit R_at_G0 = %+v, want no value (limiter-off arms have no admit)", series)
	}
	segSynthApprox(t, "admit G_at_R1 median", admit.Effects["G_at_R1"].MedianMS, 0.1)
	if series, ok := admit.Effects["R_at_G1"]; !ok || series.MedianMS != nil {
		t.Errorf("admit R_at_G1 = %+v, want no value (the limiter-off arm records admit_skipped)", series)
	}

	// --- cross-repeat spread.
	spread := report.RepeatSpread["R1G0"]["client_query_p95_ms"]
	if len(spread.PerRepeat) != 2 {
		t.Fatalf("R1G0 spread repeats = %d, want 2", len(spread.PerRepeat))
	}
	segSynthApprox(t, "R1G0 spread median", spread.MedianMS, 50)
	segSynthApprox(t, "R1G0 spread min", spread.MinMS, 50)
	segSynthApprox(t, "R1G0 spread max", spread.MaxMS, 50)
	segSynthApprox(t, "R1G0 spread range", spread.RangeMS, 0)

	// --- overhead comparison (nocoll vs steady R1G1, inside each repeat).
	overhead := report.OverheadCompare
	if !overhead.Present {
		t.Fatal("overhead comparison missing")
	}
	if len(overhead.PairedRepeats) != 2 {
		t.Errorf("overhead paired repeats = %v, want [1 2]", overhead.PairedRepeats)
	}
	if overhead.BaseSide.NTotal != 8 || overhead.VariantSide.NTotal != 10 {
		t.Errorf("overhead sample sizes = base %d / variant %d, want 8/10", overhead.BaseSide.NTotal, overhead.VariantSide.NTotal)
	}
	segSynthApprox(t, "overhead base p95", overhead.BaseSide.P95.MedianMS, 52)
	segSynthApprox(t, "overhead variant p95", overhead.VariantSide.P95.MedianMS, 51)
	segSynthApprox(t, "overhead delta p95", overhead.DeltaP95.MedianMS, -1)
	segSynthApprox(t, "overhead delta p50", overhead.DeltaP50.MedianMS, -1)

	// --- candidate rules (candidate rules, never a verdict).
	if len(report.CandidateRules) != 3 {
		t.Fatalf("candidate rules = %d, want 3", len(report.CandidateRules))
	}
	for _, rule := range report.CandidateRules {
		if rule.Status != segRuleSatisfied {
			t.Errorf("candidate rule %q status = %s, want %s (%s)", rule.Rule, rule.Status, segRuleSatisfied, rule.Observed)
		}
	}
	segSynthApprox(t, "rule admit median", report.CandidateRules[0].MedianMS, 1.5)
	segSynthApprox(t, "rule share median", report.CandidateRules[2].MedianMS, 10.0/12.0)

	// --- completeness.
	if len(report.Completeness.Repeats) != 2 || report.Completeness.Cells != 10 {
		t.Errorf("completeness = %+v, want 2 repeats / 10 cells", report.Completeness)
	}
	if len(report.Completeness.MissingSteadyArms) != 0 {
		t.Errorf("missing steady arms = %v, want none", report.Completeness.MissingSteadyArms)
	}

	// --- writer + determinism (the generated timestamp is the only
	// intentional nondeterminism).
	jsonPath, mdPath, err := segWriteSummary(root, report)
	if err != nil {
		t.Fatalf("write summary: %v", err)
	}
	raw, err := os.ReadFile(jsonPath)
	if err != nil {
		t.Fatalf("read summary.json: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("summary.json is not valid JSON: %v", err)
	}
	for _, key := range segTopLevelFields {
		if _, ok := decoded[key]; !ok {
			t.Errorf("summary.json lacks top-level key %q", key)
		}
	}
	md, err := os.ReadFile(mdPath)
	if err != nil {
		t.Fatalf("read summary.md: %v", err)
	}
	for _, want := range []string{
		"p95 不可相加", "差值对比（禁止相加/占比证明）", "89ef787", "与 burst 重叠",
		"候选规则，非裁决", "预热样本", "不变量",
	} {
		if !strings.Contains(string(md), want) {
			t.Errorf("summary.md lacks %q", want)
		}
	}

	second, err := segAnalyzeRoot(root)
	if err != nil {
		t.Fatalf("second analyze: %v", err)
	}
	report.GeneratedAt, second.GeneratedAt = "", ""
	firstJSON, err := json.Marshal(report)
	if err != nil {
		t.Fatalf("marshal first: %v", err)
	}
	secondJSON, err := json.Marshal(second)
	if err != nil {
		t.Fatalf("marshal second: %v", err)
	}
	if string(firstJSON) != string(secondJSON) {
		t.Error("two analyses of the same tree differ (analyzer must be deterministic)")
	}
}

// TestSegAnalyzeSyntheticFaults exercises the violating/unpaired paths with a
// second, deliberately broken tree.
func TestSegAnalyzeSyntheticFaults(t *testing.T) {
	root := t.TempDir()
	windowStart := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	dir := filepath.Join(root, "repeat1", "R1G0")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	var clients, segs []string
	clients = append(clients,
		segSynthSampleJSON(1, "query", windowStart.Add(time.Second), 50, 200, true, ""),
		segSynthSampleJSON(1, "query", windowStart.Add(time.Second), 50, 200, true, ""), // duplicate id
		segSynthSampleJSON(2, "query", windowStart.Add(5*time.Second), 60, 429, false, "429"),
		segSynthSampleJSON(3, "query", windowStart.Add(9*time.Second), 70, 0, false, "transport"),
		segSynthSampleJSON(0, "query", windowStart.Add(9500*time.Millisecond), 80, 200, true, ""), // no perf_id
		segSynthSampleJSON(99, "query", windowStart.Add(time.Second), 90, 200, true, ""),          // no server records
	)
	at1 := windowStart.Add(time.Second)
	segs = append(segs,
		// id 1: duplicate server_total + pgq sum above below_admit+1ms + a
		// pre-burst admit (feeds candidate rule 1).
		segSynthSegJSON("server_total", "query", 1, segSynthMs(50.5), at1),
		segSynthSegJSON("server_total", "query", 1, segSynthMs(50.5), at1),
		segSynthSegJSON("admit", "query", 1, segSynthMs(1), at1),
		segSynthSegJSON("below_admit", "query", 1, segSynthMs(5), at1),
		segSynthPgqJSON(1, "SELECT a FROM b", segSynthMs(5), at1, false),
		segSynthPgqJSON(1, "SELECT a FROM b", segSynthMs(5), at1, false),
		// id 2: server_total below admit+below_admit.
		segSynthSegJSON("server_total", "query", 2, segSynthMs(1), windowStart.Add(5*time.Second)),
		segSynthSegJSON("below_admit", "query", 2, segSynthMs(5), windowStart.Add(5*time.Second)),
		// id 3: admit_skipped while limiter_on=true.
		segSynthSegJSON("server_total", "query", 3, segSynthMs(2), windowStart.Add(9*time.Second)),
		segSynthSegJSON("admit_skipped", "query", 3, segSynthMs(1), windowStart.Add(9*time.Second)),
		// id 4: admit and below_admit for the same id (exclusive violation).
		segSynthSegJSON("server_total", "query", 4, segSynthMs(3), windowStart.Add(9500*time.Millisecond)),
		segSynthSegJSON("admit", "query", 4, segSynthMs(1), windowStart.Add(9500*time.Millisecond)),
		segSynthSegJSON("below_admit", "query", 4, segSynthMs(1), windowStart.Add(9500*time.Millisecond)),
		// id 5: server_total without below_admit or client (label monotone in
		// id order, so no clock step is invented by the fixture).
		segSynthSegJSON("server_total", "query", 5, segSynthMs(4), windowStart.Add(10*time.Second)),
		// id 6: below_admit without server_total.
		segSynthSegJSON("below_admit", "query", 6, segSynthMs(1), windowStart.Add(10200*time.Millisecond)),
		// id 7: admit without server_total (pre-burst, feeds candidate rule 1).
		segSynthSegJSON("admit", "query", 7, segSynthMs(1), at1),
	)
	meta := fmt.Sprintf(`{"repeat":1,"order_index":1,"arm":{"name":"R1G0","limiter_on":true,"background_on":false,"collect":true},
 "commit":"synthetic","window_start":%q,"window_end":%q}`, segSynthRFC(windowStart), segSynthRFC(windowStart.Add(12*time.Second)))
	if err := os.WriteFile(filepath.Join(dir, "meta.json"), []byte(meta), 0o644); err != nil {
		t.Fatalf("write meta: %v", err)
	}
	segSynthWriteLines(t, filepath.Join(dir, "client_samples.jsonl"), clients, false)
	segSynthWriteLines(t, filepath.Join(dir, "server_segments.jsonl"), segs, false)

	// A second arm without meta.json: discovery must fall back to the name and
	// report the missing window origin instead of dropping the cell.
	dir2 := filepath.Join(root, "repeat1", "R0G0")
	if err := os.MkdirAll(dir2, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	var clients2, segs2 []string
	clients2 = append(clients2, segSynthSampleJSON(20, "query", windowStart.Add(time.Second), 10, 200, true, ""))
	segs2 = append(segs2, segSynthSegJSON("below_admit", "query", 20, segSynthMs(100), windowStart.Add(time.Second)))
	pgqCounts := []int{3, 2, 2, 1, 1, 1, 1, 1, 1, 1}
	for i, count := range pgqCounts {
		prefix := fmt.Sprintf("SELECT %02d FROM t", i+1)
		for range count {
			segs2 = append(segs2, segSynthPgqJSON(20, prefix, segSynthMs(1), windowStart.Add(time.Second), false))
		}
	}
	segSynthWriteLines(t, filepath.Join(dir2, "client_samples.jsonl"), clients2, false)
	segSynthWriteLines(t, filepath.Join(dir2, "server_segments.jsonl"), segs2, false)

	report, err := segAnalyzeRoot(root)
	if err != nil {
		t.Fatalf("segAnalyzeRoot: %v", err)
	}
	broken := segSynthArmOf(t, report, 1, "R1G0")
	p := broken.Pairing
	if p.ClientSamples != 6 || p.ClientWithoutPerfID != 1 || p.ClientIDs != 4 {
		t.Errorf("broken client side = %+v, want 6 samples/1 without perf_id/4 ids", p)
	}
	if p.ClientWithoutServerTotal != 1 || p.ServerTotalWithoutClient != 2 {
		t.Errorf("broken bilateral unmatched = client↛server_total %d / server_total↛client %d, want 1/2 (id4 has no perf_id)",
			p.ClientWithoutServerTotal, p.ServerTotalWithoutClient)
	}
	if p.BelowAdmitWithoutServerTotal != 1 || p.AdmitWithoutServerTotal != 1 {
		t.Errorf("broken extra segments = below %d / admit %d, want 1/1", p.BelowAdmitWithoutServerTotal, p.AdmitWithoutServerTotal)
	}
	if p.DuplicateIDs["client"] != 1 || p.DuplicateIDs["server_total"] != 1 || p.DuplicateIDs["pgq"] != 1 {
		t.Errorf("broken duplicates = %v, want client/server_total/pgq = 1", p.DuplicateIDs)
	}
	if got := p.MissingSegmentCounts["admit"]; got != 3 {
		t.Errorf("broken missing admit = %d, want 3", got)
	}
	if got := p.ExtraSegmentCounts["below_admit"]; got != 1 {
		t.Errorf("broken extra below_admit = %d, want 1", got)
	}
	if broken.ClientQuery.Status.Non2xx != 1 || broken.ClientQuery.Status.Transport != 1 || broken.ClientQuery.Status.Errors != 2 {
		t.Errorf("broken status counts = %+v, want non2xx=1 transport=1 errors=2", broken.ClientQuery.Status)
	}
	if len(broken.QueryPhases) != 3 || broken.QueryPhases[0].N != 3 || broken.QueryPhases[1].N != 1 || broken.QueryPhases[2].N != 2 {
		t.Errorf("broken phases = %+v, want 3/1/2", broken.QueryPhases)
	}

	// --- invariants: exactly one violation per crafted kind, all listed.
	wantViolations := map[string]int{
		"server_total_ge_admit_plus_below_admit(±0.05ms)": 1,
		"sum(pgq)_le_below_admit_plus_1ms":                1,
		"admit_skipped_only_when_limiter_off":             1,
	}
	if len(broken.Invariants.Violations) != 3 {
		t.Errorf("broken violations listed = %d, want 3 (%+v)", len(broken.Invariants.Violations), broken.Invariants.ViolationCounts)
	}
	for _, check := range broken.Invariants.Checks {
		want, ok := wantViolations[check.Name]
		if !ok {
			t.Errorf("unexpected check %q", check.Name)
			continue
		}
		if check.Violations != want {
			t.Errorf("check %s violations = %d, want %d", check.Name, check.Violations, want)
		}
		if check.Skipped {
			t.Errorf("check %s skipped, want evaluated (limiter_on known)", check.Name)
		}
	}
	for kind, want := range wantViolations {
		if got := report.Invariants.ViolationCounts[kind]; got != want {
			t.Errorf("global violation count %s = %d, want %d", kind, got, want)
		}
	}
	if report.Invariants.Truncated {
		t.Error("global violation list marked truncated, want complete (3 records)")
	}
	if got := broken.Paired.IDsWithAdmitAndBelowAdmit; got != 2 {
		t.Errorf("broken admit+below_admit ids = %d, want 2 (informational, not a violation)", got)
	}
	// The hand-written fixture is sparse (one request every few seconds), so its
	// inter-request gaps exceed the forward-jump threshold: they must be
	// disclosed as ambiguous and never corrected.
	if broken.ClockStepsBackward != 0 {
		t.Errorf("broken arm backward steps = %d, want 0 (labels follow id order)", broken.ClockStepsBackward)
	}
	if broken.ClockStepsForwardSuspect == 0 {
		t.Error("broken arm forward jumps = 0, want the sparse-gap disclosures")
	}
	if broken.PhaseMethod != segPhaseMethodWall {
		t.Errorf("broken arm phase_method = %s, want %s (forward jumps are never corrected)", broken.PhaseMethod, segPhaseMethodWall)
	}
	if broken.WindowSecondsCorrected != nil || broken.WindowSecondsCorrectedNote == "" {
		t.Errorf("broken arm window_seconds_corrected = %v note=%q, want unknown + reason",
			broken.WindowSecondsCorrected, broken.WindowSecondsCorrectedNote)
	}

	// --- the meta-less arm: name fallback, approximate window origin, loud
	// warning (the cell is reported, never dropped).
	noMeta := segSynthArmOf(t, report, 1, "R0G0")
	if noMeta.MetaPresent {
		t.Error("meta-less arm reported meta_present=true")
	}
	if noMeta.LimiterOn == nil || *noMeta.LimiterOn {
		t.Errorf("meta-less arm limiter_on = %v, want false (from the R0G0 name)", noMeta.LimiterOn)
	}
	if noMeta.WindowOrigin != "first_client_sample_fallback" {
		t.Errorf("meta-less arm window origin = %q, want the flagged fallback", noMeta.WindowOrigin)
	}
	if len(noMeta.QueryPhases) != 3 || noMeta.QueryPhases[0].N != 1 {
		t.Errorf("meta-less arm phases = %+v, want one sample in [0s,4s)", noMeta.QueryPhases)
	}
	if noMeta.QueryOutside != 0 {
		t.Errorf("meta-less arm outside-window samples = %d, want 0 (fallback origin is the first sample)", noMeta.QueryOutside)
	}
	if noMeta.Pgq.DistinctPrefixes != 10 || len(noMeta.Pgq.TopPrefixes) != segPGQTopPrefixes {
		t.Errorf("meta-less pgq prefixes = %d distinct / %d listed, want 10/8", noMeta.Pgq.DistinctPrefixes, len(noMeta.Pgq.TopPrefixes))
	}
	if noMeta.Pgq.TopPrefixes[0].Prefix != "SELECT 01 FROM t" || noMeta.Pgq.TopPrefixes[0].Count != 3 {
		t.Errorf("pgq top prefix = %+v, want SELECT 01×3", noMeta.Pgq.TopPrefixes[0])
	}
	if noMeta.Pgq.TopPrefixes[7].Prefix != "SELECT 08 FROM t" {
		t.Errorf("pgq 8th prefix = %q, want SELECT 08 FROM t (count-desc, prefix tie-break)", noMeta.Pgq.TopPrefixes[7].Prefix)
	}
	if len(noMeta.MissingFiles) == 0 {
		t.Error("meta-less arm missing_files empty, want warmup_samples.jsonl listed")
	}
	missingMeta := strings.Join(report.Completeness.MetaMissing, ",")
	if !strings.Contains(missingMeta, "repeat1/R0G0") {
		t.Errorf("completeness meta_missing = %v, want repeat1/R0G0", report.Completeness.MetaMissing)
	}
	warned := strings.Join(noMeta.Warnings, "; ")
	if !strings.Contains(warned, "meta.json 缺失") {
		t.Errorf("meta-less arm warnings = %q, want the meta-missing warning", warned)
	}

	// --- candidate rules: rule 1 is evaluable, rule 3 is not.
	if got := report.CandidateRules[0].Status; got != segRuleSatisfied {
		t.Errorf("rule 1 status = %s, want %s (%s)", got, segRuleSatisfied, report.CandidateRules[0].Observed)
	}
	segSynthApprox(t, "rule 1 median (meta-less admit 1ms)", report.CandidateRules[0].MedianMS, 1)
	if got := report.CandidateRules[2].Status; got != segRuleInsufficient {
		t.Errorf("rule 3 status = %s, want %s", got, segRuleInsufficient)
	}

	if _, mdPath, err := segWriteSummary(root, report); err != nil {
		t.Fatalf("write summary: %v", err)
	} else if _, err := os.Stat(mdPath); err != nil {
		t.Fatalf("summary.md missing: %v", err)
	}
}

// TestSegAnalyzeSyntheticClock pins the wall-clock step detection and the
// step-corrected phase view against a step-free reference tree.
func TestSegAnalyzeSyntheticClock(t *testing.T) {
	specs := segSynthMainSpecs()
	pick := func(stepFromID uint64, deltaMS float64) segSynthArmSpec {
		for _, spec := range specs {
			if spec.Arm == "R1G0" {
				// Dense stream: two queries per 500ms grid slot (pairs share
				// a label). The steady inter-request delta stays well under
				// the forward-jump threshold, and the injected step is the
				// only discontinuity above the backward threshold.
				spec.QuerySamples = 40
				spec.QuerySpacingMS = 500
				spec.ClockStepFromID = stepFromID
				spec.ClockStepDeltaMS = deltaMS
				return spec
			}
		}
		t.Fatal("R1G0 spec missing")
		return segSynthArmSpec{}
	}
	windowStart := time.Date(2026, 10, 7, 10, 0, 0, 0, time.UTC)
	build := func(t *testing.T, spec segSynthArmSpec) *segReport {
		t.Helper()
		root := t.TempDir()
		segSynthWriteArm(t, filepath.Join(root, "repeat1", "R1G0"), spec, windowStart, false)
		report, err := segAnalyzeRoot(root)
		if err != nil {
			t.Fatalf("segAnalyzeRoot: %v", err)
		}
		return report
	}

	cleanReport := build(t, pick(0, 0))
	clean := segSynthArmOf(t, cleanReport, 1, "R1G0")
	backwardReport := build(t, pick(20, -1500))
	backward := segSynthArmOf(t, backwardReport, 1, "R1G0")

	// --- detection.
	if backward.ClockStepsBackward != 1 || backward.ClockStepsForwardSuspect != 0 {
		t.Fatalf("clock steps = %d backward / %d forward, want 1/0", backward.ClockStepsBackward, backward.ClockStepsForwardSuspect)
	}
	step := backward.ClockSteps[0]
	if step.BoundaryID != 19 || step.FirstAffectedID != 20 {
		t.Errorf("step boundary = id %d → id %d, want 19 → 20", step.BoundaryID, step.FirstAffectedID)
	}
	if math.Abs(step.DeltaMS+1500) > 1e-6 {
		t.Errorf("step delta = %.3fms, want -1500", step.DeltaMS)
	}
	if step.Kind != "backward" || step.Ambiguous {
		t.Errorf("step kind/ambiguous = %s/%t, want backward/false", step.Kind, step.Ambiguous)
	}
	if math.Abs(step.CorrectionMS-1500) > 1e-6 {
		t.Errorf("step correction = %.3fms, want 1500", step.CorrectionMS)
	}
	labelBefore, errBefore := time.Parse(time.RFC3339Nano, step.LabelBefore)
	labelAfter, errAfter := time.Parse(time.RFC3339Nano, step.LabelAfter)
	if errBefore != nil || errAfter != nil {
		t.Errorf("step labels unparseable: %q / %q (%v / %v)", step.LabelBefore, step.LabelAfter, errBefore, errAfter)
	} else if !labelAfter.Before(labelBefore) {
		t.Errorf("step labels = %s → %s, want a backward jump", step.LabelBefore, step.LabelAfter)
	}
	if backward.PhaseMethod != segPhaseMethodStepCorrected {
		t.Errorf("phase_method = %s, want %s", backward.PhaseMethod, segPhaseMethodStepCorrected)
	}
	if !step.AppliesToWindow || backward.ClockStepsApplied != 1 {
		t.Errorf("step applies_to_window/applied = %t/%d, want true/1", step.AppliesToWindow, backward.ClockStepsApplied)
	}

	// --- corrected phases equal the step-free reference.
	if len(backward.QueryPhases) != len(clean.QueryPhases) {
		t.Fatalf("corrected phases = %d buckets, want %d", len(backward.QueryPhases), len(clean.QueryPhases))
	}
	for i := range clean.QueryPhases {
		if backward.QueryPhases[i].N != clean.QueryPhases[i].N {
			t.Errorf("bucket %s n = %d, want %d (clean reference)", backward.QueryPhases[i].Label, backward.QueryPhases[i].N, clean.QueryPhases[i].N)
		}
		if clean.QueryPhases[i].Latency.P95MS == nil {
			t.Errorf("clean reference bucket %s has no p95", clean.QueryPhases[i].Label)
			continue
		}
		segSynthApprox(t, "bucket "+backward.QueryPhases[i].Label+" p95",
			backward.QueryPhases[i].Latency.P95MS, *clean.QueryPhases[i].Latency.P95MS)
	}
	// --- the raw wall view is kept for audit and differs.
	if len(backward.QueryPhasesWall) != 3 {
		t.Fatalf("raw wall phases = %d, want 3", len(backward.QueryPhasesWall))
	}
	wantWall := []int{15, 21, 4}
	for i, want := range wantWall {
		if backward.QueryPhasesWall[i].N != want {
			t.Errorf("raw wall bucket %s n = %d, want %d", backward.QueryPhasesWall[i].Label, backward.QueryPhasesWall[i].N, want)
		}
	}
	wantCorrected := []int{14, 16, 10}
	for i, want := range wantCorrected {
		if backward.QueryPhases[i].N != want {
			t.Errorf("corrected bucket %s n = %d, want %d", backward.QueryPhases[i].Label, backward.QueryPhases[i].N, want)
		}
	}
	// --- segment phases are corrected too (server_total is fully segmented).
	if clean.SegmentPhases["server_total"][2].Latency.Count != 11 {
		t.Fatalf("clean reference segment bucket 3 count = %d, want 11 (10 queries + the create request)",
			clean.SegmentPhases["server_total"][2].Latency.Count)
	}
	for i := range clean.SegmentPhases["server_total"] {
		if got, want := backward.SegmentPhases["server_total"][i].Latency.Count, clean.SegmentPhases["server_total"][i].Latency.Count; got != want {
			t.Errorf("corrected segment bucket %d count = %d, want %d (clean reference)", i, got, want)
		}
	}
	if got := backward.SegmentOutside["server_total"]; got != 0 {
		t.Errorf("corrected outside-window segments = %d, want 0", got)
	}
	// --- windows: raw is polluted, corrected is the real length.
	if backward.WindowSeconds == nil || math.Abs(*backward.WindowSeconds-10.5) > 1e-6 {
		t.Errorf("window_seconds = %v, want the polluted 10.500", backward.WindowSeconds)
	}
	if plain := backward.ClockStepsNote; !strings.Contains(plain, "回拨") {
		t.Errorf("clock_steps_note = %q, want the backward-step note", plain)
	}
	segSynthApprox(t, "window_seconds_corrected", backward.WindowSecondsCorrected, 12)
	if clean.WindowSecondsCorrected == nil || math.Abs(*clean.WindowSecondsCorrected-12) > 1e-6 {
		t.Errorf("clean window_seconds_corrected = %v, want 12", clean.WindowSecondsCorrected)
	}
	// --- top-level rollup.
	if len(backwardReport.ClockSteps) != 1 || len(backwardReport.ClockSteps[0].Steps) != 1 {
		t.Errorf("top-level clock_steps rollup = %+v, want one arm with one step", backwardReport.ClockSteps)
	}
	if len(cleanReport.ClockSteps) != 1 || len(cleanReport.ClockSteps[0].Steps) != 0 {
		t.Errorf("clean clock_steps rollup = %+v, want one arm with no steps", cleanReport.ClockSteps)
	}

	// --- (i) pre-window step: window_start shares its timebase, nothing is
	// corrected, and the phases must equal the step-free reference.
	preSpec := pick(0, 0)
	preSpec.PreWindowSegments = 12
	preSpec.ClockStepFromID = 6
	preSpec.ClockStepDeltaMS = -1500
	preSpec.ClockStepBeforeWindow = true
	preRef := preSpec
	preRef.ClockStepFromID = 0
	preRef.ClockStepDeltaMS = 0
	preRef.ClockStepBeforeWindow = false
	preRefArm := segSynthArmOf(t, build(t, preRef), 1, "R1G0")
	preReport := build(t, preSpec)
	pre := segSynthArmOf(t, preReport, 1, "R1G0")
	// The fixture block boundary adds a sparse-gap forward jump; both steps
	// precede the steady window and must stay uncorrected.
	if len(pre.ClockSteps) != 2 {
		t.Fatalf("pre-window clock steps = %d, want 2 (backward + boundary gap)", len(pre.ClockSteps))
	}
	preStep := pre.ClockSteps[0]
	if preStep.Kind != "backward" || preStep.FirstAffectedID != 6 || math.Abs(preStep.DeltaMS+1500) > 1e-6 {
		t.Errorf("pre-window step = %s id %d / %.3fms, want backward id 6 / -1500", preStep.Kind, preStep.FirstAffectedID, preStep.DeltaMS)
	}
	for _, candidate := range pre.ClockSteps {
		if candidate.AppliesToWindow || candidate.Reason != "pre_window" {
			t.Errorf("pre-window step %+v applies to the window, want false/pre_window", candidate)
		}
	}
	if pre.PhaseMethod != segPhaseMethodWall || pre.ClockStepsApplied != 0 {
		t.Errorf("pre-window phase_method/applied = %s/%d, want wall/0", pre.PhaseMethod, pre.ClockStepsApplied)
	}
	if len(pre.QueryPhasesWall) != 0 {
		t.Errorf("pre-window wall audit view = %d buckets, want none (nothing was corrected)", len(pre.QueryPhasesWall))
	}
	segSynthAssertPhasesEqual(t, "pre-window phases", pre.QueryPhases, preRefArm.QueryPhases)
	if pre.QueryOutside != preRefArm.QueryOutside {
		t.Errorf("pre-window outside = %d, want %d (reference)", pre.QueryOutside, preRefArm.QueryOutside)
	}
	if pre.WindowSeconds == nil || math.Abs(*pre.WindowSeconds-12) > 1e-6 {
		t.Errorf("pre-window window_seconds = %v, want 12.000 (origin shares the shifted timebase)", pre.WindowSeconds)
	}
	segSynthApprox(t, "pre-window window_seconds_corrected", pre.WindowSecondsCorrected, 12)
	if got := pre.SegmentOutside["server_total"]; got != 12 {
		t.Errorf("pre-window outside segments = %d, want 12 (the segment-only pre-window ids)", got)
	}
	if pre.ClockStepsBackward != 1 {
		t.Errorf("pre-window backward steps = %d, want 1 (detected, not applied)", pre.ClockStepsBackward)
	}

	// --- (iii) mixed: a pre-window step plus a window-internal step; only the
	// latter is applied.
	mixSpec := pick(0, 0)
	mixSpec.PreWindowSegments = 12
	mixSpec.ClockStepFromID = 6
	mixSpec.ClockStepDeltaMS = -1500
	mixSpec.ClockStepBeforeWindow = true
	mixSpec.ClockStep2FromID = 24
	mixSpec.ClockStep2DeltaMS = -1200
	mixRef := mixSpec
	mixRef.ClockStepFromID = 0
	mixRef.ClockStepDeltaMS = 0
	mixRef.ClockStepBeforeWindow = false
	mixRef.ClockStep2FromID = 0
	mixRef.ClockStep2DeltaMS = 0
	mixRefArm := segSynthArmOf(t, build(t, mixRef), 1, "R1G0")
	mixReport := build(t, mixSpec)
	mix := segSynthArmOf(t, mixReport, 1, "R1G0")
	if len(mix.ClockSteps) != 3 {
		t.Fatalf("mixed clock steps = %d, want 3 (pre-window backward + boundary gap + in-window backward)", len(mix.ClockSteps))
	}
	applied := 0
	preWindow := 0
	for _, candidate := range mix.ClockSteps {
		switch {
		case !candidate.AppliesToWindow:
			preWindow++
			if candidate.Reason != "pre_window" {
				t.Errorf("mixed non-applicable step reason = %q, want pre_window", candidate.Reason)
			}
		case candidate.Ambiguous:
			t.Error("mixed step marked applicable and ambiguous, want forward jumps never applicable")
		default:
			applied++
		}
	}
	if applied != 1 || preWindow != 2 {
		t.Errorf("mixed steps = %d applied / %d pre-window, want 1/2", applied, preWindow)
	}
	if mix.ClockStepsApplied != 1 || mix.PhaseMethod != segPhaseMethodStepCorrected {
		t.Errorf("mixed applied/phase_method = %d/%s, want 1/%s", mix.ClockStepsApplied, mix.PhaseMethod, segPhaseMethodStepCorrected)
	}
	segSynthAssertPhasesEqual(t, "mixed corrected phases", mix.QueryPhases, mixRefArm.QueryPhases)
	if mix.WindowSeconds == nil || math.Abs(*mix.WindowSeconds-10.8) > 1e-6 {
		t.Errorf("mixed window_seconds = %v, want 10.800 (raw wall, polluted by the in-window step)", mix.WindowSeconds)
	}
	segSynthApprox(t, "mixed window_seconds_corrected", mix.WindowSecondsCorrected, 12)
	if len(mix.QueryPhasesWall) != 3 {
		t.Errorf("mixed wall audit view = %d buckets, want 3", len(mix.QueryPhasesWall))
	}

	// --- forward jump: disclosed, never corrected.
	forwardReport := build(t, pick(20, 700))
	forward := segSynthArmOf(t, forwardReport, 1, "R1G0")
	if forward.ClockStepsForwardSuspect != 1 || forward.ClockStepsBackward != 0 {
		t.Fatalf("forward steps = %d forward / %d backward, want 1/0", forward.ClockStepsForwardSuspect, forward.ClockStepsBackward)
	}
	fstep := forward.ClockSteps[0]
	if fstep.Kind != "forward_suspect" || !fstep.Ambiguous || fstep.CorrectionMS != 0 {
		t.Errorf("forward step = %+v, want forward_suspect/ambiguous/no correction", fstep)
	}
	if math.Abs(fstep.DeltaMS-700) > 1e-6 {
		t.Errorf("forward delta = %.3fms, want +700 (fixture labels stay shifted: nothing is corrected)", fstep.DeltaMS)
	}
	// The forward labels stay inflated (no correction): the reported counts are
	// the shifted ones, not the step-free reference.
	wantForward := []int{14, 14, 12}
	for i, want := range wantForward {
		if forward.QueryPhases[i].N != want {
			t.Errorf("forward bucket %s n = %d, want %d (uncorrected shifted labels)",
				forward.QueryPhases[i].Label, forward.QueryPhases[i].N, want)
		}
	}
	if forward.QueryPhases[1].N == clean.QueryPhases[1].N {
		t.Error("forward counts match the step-free reference, want the shifted counts (nothing was corrected)")
	}
	if forward.PhaseMethod != segPhaseMethodWall {
		t.Errorf("forward phase_method = %s, want %s (no correction)", forward.PhaseMethod, segPhaseMethodWall)
	}
	if len(forward.QueryPhasesWall) != 0 {
		t.Errorf("forward raw wall view = %d buckets, want none (nothing was corrected)", len(forward.QueryPhasesWall))
	}
	if forward.WindowSecondsCorrected != nil || forward.WindowSecondsCorrectedNote == "" {
		t.Errorf("forward window_seconds_corrected = %v note=%q, want unknown + reason",
			forward.WindowSecondsCorrected, forward.WindowSecondsCorrectedNote)
	}
	warnedForward := strings.Join(forward.Warnings, "; ")
	if !strings.Contains(warnedForward, "可疑前跳") {
		t.Errorf("forward warnings = %q, want the ambiguous-jump warning", warnedForward)
	}
	if _, _, err := segWriteSummary(t.TempDir(), forwardReport); err != nil {
		t.Fatalf("write summary: %v", err)
	}
}
