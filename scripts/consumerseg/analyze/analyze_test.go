// analyze_test.go drives the analyzer over synthetic evidence trees built in
// temporary directories. The fixtures use both ".csv" and ".csv.gz" artifacts,
// contain straddling/outside spans, a nested-overlap violation, a counting
// mismatch, microsecond boundary cases and an empty poll, and they assert the
// clipping math, the residual, the distribution values (linear-interpolation
// percentiles on known small samples), the decile curve and the cross-run
// group delta.
package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ---------------------------------------------------------------- utilities

func writeFile(t *testing.T, root, rel, content string) {
	t.Helper()
	path := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", rel, err)
	}
}

func writeGzFile(t *testing.T, root, rel, content string) {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := io.WriteString(zw, content); err != nil {
		t.Fatalf("gzip write %s: %v", rel, err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("gzip close %s: %v", rel, err)
	}
	writeFile(t, root, rel, buf.String())
}

func writeJSON(t *testing.T, root, rel string, doc any) {
	t.Helper()
	body, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal %s: %v", rel, err)
	}
	writeFile(t, root, rel, string(body))
}

func analyze(t *testing.T, root string, extra ...string) *allSummary {
	t.Helper()
	return analyzeTo(t, root, filepath.Join(root, "analysis"), extra...)
}

func analyzeTo(t *testing.T, root, outDir string, extra ...string) *allSummary {
	t.Helper()
	var stdout, stderr bytes.Buffer
	args := append([]string{"-dir", root}, extra...)
	if code := run(args, &stdout, &stderr); code != 0 {
		t.Fatalf("run(%v) = %d, want 0; stderr=%s", args, code, stderr.String())
	}
	body, err := os.ReadFile(filepath.Join(outDir, "summary_all.json"))
	if err != nil {
		t.Fatalf("read summary_all.json: %v", err)
	}
	var all allSummary
	if err := json.Unmarshal(body, &all); err != nil {
		t.Fatalf("unmarshal summary_all.json: %v", err)
	}
	return &all
}

func runNamed(t *testing.T, all *allSummary, label string) *runSummary {
	t.Helper()
	for _, r := range all.Runs {
		if r.Label == label {
			return r
		}
	}
	t.Fatalf("run %s not found in %+v", label, all.Runs)
	return nil
}

func detailWith(r *runSummary, substr string) (string, bool) {
	for _, d := range r.Checks.Details {
		if strings.Contains(d, substr) {
			return d, true
		}
	}
	return "", false
}

func anomalyWith(all *allSummary, label, kind string) (anomaly, bool) {
	for _, a := range all.Anomalies {
		if a.Label == label && a.Kind == kind {
			return a, true
		}
	}
	return anomaly{}, false
}

func nearly(t *testing.T, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 1e-9 {
		t.Fatalf("got %v, want %v", got, want)
	}
}

// nearly6 compares against a value that passed through round6.
func nearly6(t *testing.T, got, want float64) {
	t.Helper()
	nearly(t, got, round6(want))
}

// ------------------------------------------------------------- fixtures

// countsAll is a fully consistent anchors.counts block for n events.
func countsAll(n int64) map[string]any {
	return map[string]any{
		"observed_process": n, "observed_applied": n, "observed_duplicate": 0,
		"observed_version_skip": 0, "observed_quarantined": 0, "observed_errors": 0,
		"sql_spans": 0, "sql_unattributed": 0, "applied_cb": 0,
	}
}

// anchorsFixture builds a complete anchors.json map. drainStartUS offsets
// every value (the producer's anchors are monotonic offsets from the epoch);
// last_cycle_end/pending_zero stay 40us/30us before publishDone, so the
// derived latencies are 10us and 40us.
func anchorsFixture(drainStartUS, publishDoneUS, pollConfirmUS int64, segEnabled bool, counts map[string]any) map[string]any {
	return map[string]any{
		"drain_start_us": drainStartUS,
		"publish": map[string]any{
			"first_cycle_start_us":     10,
			"last_cycle_end_us":        publishDoneUS - 40,
			"pending_zero_observed_us": publishDoneUS - 30,
			"publish_done_us":          publishDoneUS,
			"cycles":                   1,
			"claimed":                  50,
			"acked":                    50,
			"released":                 0,
			"blocked":                  0,
			"applied_at_publish_done":  42,
		},
		"consume": map[string]any{
			"poll_confirm_us":     pollConfirmUS,
			"consumer_stopped_us": pollConfirmUS + 5,
			"applied_at_confirm":  42,
			"last_commit_end_us":  240,
			"last_applied_cb_us":  250,
		},
		"counts":      counts,
		"lag_final":   map[string]int64{"0": 2, "1": 0},
		"seg_enabled": segEnabled,
	}
}

// onLoopCSV is the fully collected top-level loop evidence. Spans are strictly
// disjoint; the tail window of the ON fixture is [190,260):
//
//	poll [100,150) outside · process1 [150,183) outside · poll [185,205) partial
//	process2 [210,240) full · rebalance [250,280) partial · lag [300,320) outside
//	mark [320,325) outside
//
// The second poll is an empty poll (records=0).
const onLoopCSV = `kind,seq,t_us,dur_us,i1,i2,s1
poll,,100,50,3,,
process,1,150,33,0,4,INSERT 0 1
poll,,185,20,0,,
process,2,210,30,0,5,INSERT 0 1
rebalance,,250,30,,,
lag,,300,20,,,
mark,1,320,5,0,5,
applied_cb,1,179,0,,,
applied_cb,2,235,0,,,
`

// onSQLCSV nests both events under their process spans.
const onSQLCSV = `seq,t_us,dur_us,kind,sql,err
1,151,5,begin,BEGIN,0
1,160,10,inbox_insert,INSERT INTO inbox,0
1,178,4,commit,COMMIT,0
2,212,2,begin,BEGIN,0
2,220,8,effect_insert,INSERT INTO effects,0
2,230,3,commit,COMMIT,0
`

const onSamplesCSV = `t_us,pending,published,applied,ledger,progress,acquire_count,empty_acquire_count,acquire_duration_us,canceled_acquire_count,total_conns,idle_conns
0,50,0,0,50,0,10,1,1000,0,2,1
100000,40,1,0,50,1,20,2,20000,0,2,1
200000,30,2,1,50,2,30,2,50000,0,2,1
300000,20,2,1,50,2,40,3,90000,0,2,1
400000,10,2,2,50,2,45,3,120000,0,2,1
500000,0,2,2,50,2,50,3,123456,1,2,1
`

const onPublishCSV = `iter,t_count_us,count_dur_us,pending_seen,t_publish_us,publish_dur_us,claimed,acked,released,blocked
1,10,20,50,30,60,50,50,0,0
2,70,10,0,0,0,0,0,0,0
`

// writeOnRun writes the fully collected ON fixture (loop/sql as .csv.gz,
// samples/publish as plain .csv) into <root>/runs/<label>.
func writeOnRun(t *testing.T, root, label string) {
	t.Helper()
	writeJSON(t, root, "runs/"+label+"/meta.json", map[string]any{
		"harness": "013-supplement/consumer-seg/1", "mode": "on", "n": 2, "seg_enabled": true,
		"vcs_modified": "false", // the producer records the setting as a string
		"drops":        map[string]any{"loop_unpaired": 0, "samples_dropped": 0},
	})
	counts := countsAll(2)
	counts["sql_spans"] = 6
	counts["applied_cb"] = 2
	writeJSON(t, root, "runs/"+label+"/anchors.json", anchorsFixture(100, 190, 260, true, counts))
	writeJSON(t, root, "runs/"+label+"/report.json", map[string]any{
		"harness":                        "013-supplement/consumer-seg/1",
		"n":                              2,
		"publish_seconds":                float64(90) / 1e6,
		"consumer_catchup_tail_seconds":  float64(70) / 1e6,
		"total_drain_seconds":            float64(160) / 1e6,
		"end_to_end_events_per_second":   2.0 / (float64(160) / 1e6),
		"publish_events_per_second":      2.0 / (float64(90) / 1e6),
		"consumer_applied":               2,
		"publish_cycles":                 1,
		"publish_claimed":                50,
		"publish_acked":                  50,
		"publish_released":               0,
		"publish_blocked":                0,
		"seg_enabled":                    true,
		"applied_at_publish_done":        42,
		"publish_last_cycle_end_seconds": float64(50) / 1e6,
		"pending_zero_observed_seconds":  float64(60) / 1e6,
		"consume_confirm_seconds":        float64(160) / 1e6,
		"last_commit_end_seconds":        float64(140) / 1e6,
		"last_applied_cb_seconds":        float64(150) / 1e6,
		"process_observed":               2,
		"sql_span_count":                 6,
		"seg_drops":                      map[string]any{"loop_unpaired": 0, "samples_dropped": 0},
		"lag_final":                      map[string]int64{"0": 2, "1": 0},
		"pool_final": map[string]any{
			"acquire_count": 50, "empty_acquire_count": 3,
			"acquire_duration_seconds": 0.123456, "canceled_acquire_count": 1,
		},
	})
	writeFile(t, root, "runs/"+label+"/exit_code", "0\n")
	writeFile(t, root, "logs/"+label+".rc", "0\n")
	writeGzFile(t, root, "runs/"+label+"/loop.csv.gz", onLoopCSV)
	writeGzFile(t, root, "runs/"+label+"/sql.csv.gz", onSQLCSV)
	writeFile(t, root, "runs/"+label+"/samples.csv", onSamplesCSV)
	writeFile(t, root, "runs/"+label+"/publish.csv", onPublishCSV)
}

// writeTimingRun writes a collected run with a fully consistent anchors.json
// and the samples.csv monitor only (the producer's arm shape: loop/sql/publish
// are written for the ON arm, samples.csv for both arms).
func writeTimingRun(t *testing.T, root, label, mode string, n int64, publishS, tailS float64) {
	t.Helper()
	publishDoneUS := int64(publishS * 1e6)
	pollConfirmUS := publishDoneUS + int64(tailS*1e6)
	writeJSON(t, root, "runs/"+label+"/meta.json", map[string]any{
		"harness": "013-supplement/consumer-seg/1", "mode": mode, "n": n, "seg_enabled": mode == "on",
		"drops": map[string]any{"loop_unpaired": 0, "samples_dropped": 0},
	})
	writeJSON(t, root, "runs/"+label+"/anchors.json", anchorsFixture(0, publishDoneUS, pollConfirmUS, mode == "on", countsAll(n)))
	writeFile(t, root, "runs/"+label+"/exit_code", "0\n")
	writeFile(t, root, "logs/"+label+".rc", "0\n")
	writeFile(t, root, "runs/"+label+"/samples.csv", "t_us,pending,published,applied,ledger,progress,acquire_count,empty_acquire_count,acquire_duration_us,canceled_acquire_count,total_conns,idle_conns\n"+
		"0,0,0,0,0,0,5,0,100,0,2,1\n")
}

// --------------------------------------------------------------- tests

func TestTailClippingFullPartialOutsideAndResidual(t *testing.T) {
	root := t.TempDir()
	writeOnRun(t, root, "r01_on")
	all := analyze(t, root)
	r := runNamed(t, all, "r01_on")

	if !r.Tail.Collected {
		t.Fatalf("tail clipping not collected: %+v", r.Tail)
	}
	if got := *r.Tail.WindowStartUS; got != 190 {
		t.Fatalf("window start = %d, want 190", got)
	}
	if got := *r.Tail.WindowEndUS; got != 260 {
		t.Fatalf("window end = %d, want 260", got)
	}
	if got := *r.Tail.TailUS; got != 70 {
		t.Fatalf("tail = %d, want 70", got)
	}
	if r.Tail.TopSpans != 7 {
		t.Fatalf("top spans = %d, want 7", r.Tail.TopSpans)
	}
	if r.Tail.FullSpans != 1 || r.Tail.PartialSpans != 2 || r.Tail.OutsideSpans != 4 {
		t.Fatalf("full/partial/outside = %d/%d/%d, want 1/2/4",
			r.Tail.FullSpans, r.Tail.PartialSpans, r.Tail.OutsideSpans)
	}
	if r.Tail.CoverageUS != 55 {
		t.Fatalf("coverage = %d, want 55 (partial spans count only the in-window part)", r.Tail.CoverageUS)
	}
	if got := *r.Tail.ResidualUS; got != 15 {
		t.Fatalf("residual = %d, want 15", got)
	}
	nearly6(t, *r.Tail.ResidualPct, 1500.0/70.0)
	if r.Tail.RawFullSpansUS != 30 {
		t.Fatalf("raw full spans = %d, want 30 (audit only)", r.Tail.RawFullSpansUS)
	}
	if r.Tail.RawFullVsTail == "" {
		t.Fatal("raw_full_vs_tail warning missing")
	}
	poll := r.Tail.ByKind["poll"]
	if poll.Spans != 2 || poll.Partial != 1 || poll.Outside != 1 || poll.CoverageUS != 15 {
		t.Fatalf("poll clip = %+v, want 2 spans / 1 partial / 1 outside / 15us", poll)
	}
	rebalance := r.Tail.ByKind["rebalance"]
	if rebalance.Partial != 1 || rebalance.CoverageUS != 10 {
		t.Fatalf("rebalance clip = %+v, want 1 partial / 10us", rebalance)
	}
	if r.Tail.OverlapPairs != 0 || r.Tail.OverlapSpans != 0 {
		t.Fatalf("overlap pairs = %d/%d, want 0/0", r.Tail.OverlapPairs, r.Tail.OverlapSpans)
	}
	if r.Tail.NegativeDurations != 0 || r.Tail.PreEpochSpans != 0 {
		t.Fatalf("negative/pre-epoch = %d/%d, want 0/0", r.Tail.NegativeDurations, r.Tail.PreEpochSpans)
	}
	if !r.Checks.OverlapOK || !r.Checks.NestingOK || !r.Checks.CountsOK {
		t.Fatalf("checks = %+v, want overlap/nesting/counts OK", r.Checks)
	}
	// Empty poll and batch-size view.
	if p := r.Segments.Poll; p == nil || p.Count != 2 || p.Empty != 1 {
		t.Fatalf("poll = %+v, want count=2 empty=1", p)
	} else {
		nearly(t, *p.RecordsMean, 1.5)
		if *p.RecordsMin != 0 || *p.RecordsMax != 3 {
			t.Fatalf("records min/max = %d/%d, want 0/3", *p.RecordsMin, *p.RecordsMax)
		}
	}
	// Detector latencies.
	if got := *r.Latency.PublishObserveLatencyUS; got != 10 {
		t.Fatalf("publish_observe_latency = %d, want 10", got)
	}
	if got := *r.Latency.PublishDoneLatencyUS; got != 40 {
		t.Fatalf("publish_done_latency = %d, want 40", got)
	}
	if got := *r.Latency.ConsumeConfirmLatencyUS; got != 10 {
		t.Fatalf("consume_confirm_latency = %d, want 10", got)
	}
	if got := *r.Latency.AppliedCBLatencyUS; got != 10 {
		t.Fatalf("applied_cb_latency = %d, want 10", got)
	}
	// Top-level timing from the anchors (drain_start = 100us).
	nearly(t, *r.PublishS, 0.000090)
	nearly(t, *r.TailS, 0.000070)
	nearly(t, *r.TotalS, 0.000160)
	nearly6(t, *r.E2ERate, 2.0/0.000160)
	nearly6(t, *r.PublishRate, 2.0/0.000090)
	if r.TimingSource != "anchors" {
		t.Fatalf("timing source = %q, want anchors", r.TimingSource)
	}
	// Publish cycle rollup: one productive cycle plus the terminal boundary row.
	if !r.Publish.Collected || r.Publish.Rows != 2 || r.Publish.ProductiveCycles != 1 ||
		r.Publish.BoundaryRows != 1 || r.Publish.Claimed != 50 || !r.Publish.LastCycleTerminal {
		t.Fatalf("publish = %+v, want 2 rows / 1 productive cycle / 1 boundary row / terminal", r.Publish)
	}
	// Publish spans by partition.
	if len(r.Partitions.ByPartition) != 1 || r.Partitions.ByPartition[0].Partition != 0 ||
		r.Partitions.ByPartition[0].Count != 2 || r.Partitions.ByPartition[0].MaxOffset != 5 {
		t.Fatalf("partitions = %+v, want partition 0 count 2 max offset 5", r.Partitions.ByPartition)
	}
	if r.Partitions.LagFinal == nil || r.Partitions.LagFinal["0"] != 2 || r.Partitions.LagFinalSource != "anchors" {
		t.Fatalf("lag_final = %+v", r.Partitions)
	}
	if len(r.NotCollected) != 0 {
		t.Fatalf("not_collected = %v, want empty", r.NotCollected)
	}
	if r.Mode != "on" || r.NSource != "meta" || r.N == nil || *r.N != 2 {
		t.Fatalf("meta.json did not decode: mode=%q n=%v source=%q", r.Mode, r.N, r.NSource)
	}
	if got := *r.Pool; got.AcquireDurationSeconds != 0.123456 || got.EmptyAcquireCount != 3 {
		t.Fatalf("pool = %+v", got)
	}
}

func TestDecileCurveFromSamples(t *testing.T) {
	root := t.TempDir()
	writeOnRun(t, root, "r01_on")
	all := analyze(t, root)
	r := runNamed(t, all, "r01_on")
	if !r.Curve.Collected || r.Curve.Samples != 6 {
		t.Fatalf("curve = %+v, want 6 samples", r.Curve)
	}
	applied := r.Curve.Applied
	if len(applied) != 10 {
		t.Fatalf("applied deciles = %d, want 10", len(applied))
	}
	nearly(t, *applied[0].TMS, 199.9) // 10% -> applied>=1 at t=200000us, relative to drain_start=100us
	nearly(t, *applied[5].TMS, 399.9) // 60% -> applied>=2 at 400000us
	nearly(t, *applied[9].TMS, 399.9) // 100%
	nearly(t, *r.Curve.Published[0].TMS, 99.9)
	nearly(t, *r.Curve.Published[9].TMS, 199.9)
	nearly(t, *r.Curve.Progress[0].TMS, 99.9)
	nearly(t, *r.Curve.Progress[9].TMS, 199.9)
}

func TestDecileSeriesZeroFinalYieldsNilPoints(t *testing.T) {
	rows := []sampleRow{{TUS: 0}, {TUS: 1000}, {TUS: 2000}}
	points := decileSeries(rows, new(int64(0)), func(s sampleRow) int64 { return s.Applied })
	if len(points) != 10 {
		t.Fatalf("points = %d, want 10", len(points))
	}
	for _, p := range points {
		if p.TUS != nil || p.TMS != nil {
			t.Fatalf("decile %d = %+v, want nil (final value 0)", p.Pct, p)
		}
	}
}

func TestOverlapViolationDetected(t *testing.T) {
	root := t.TempDir()
	label := "r03_on"
	writeJSON(t, root, "runs/"+label+"/meta.json", map[string]any{
		"mode": "on", "n": 1, "seg_enabled": true,
		"drops": map[string]any{"loop_unpaired": 0, "samples_dropped": 0},
	})
	counts := countsAll(1)
	counts["sql_spans"] = 2
	writeJSON(t, root, "runs/"+label+"/anchors.json", anchorsFixture(0, 0, 1000, true, counts))
	writeFile(t, root, "runs/"+label+"/loop.csv", `kind,seq,t_us,dur_us,i1,i2,s1
poll,,0,100,1,,
process,1,50,100,0,1,INSERT 0 1
`)
	writeFile(t, root, "runs/"+label+"/sql.csv", `seq,t_us,dur_us,kind,sql,err
1,60,1,begin,BEGIN,0
1,140,1,commit,COMMIT,0
`)
	writeFile(t, root, "runs/"+label+"/exit_code", "0\n")

	all := analyze(t, root)
	r := runNamed(t, all, label)
	if r.Tail.OverlapPairs != 1 || r.Tail.OverlapSpans != 1 {
		t.Fatalf("overlap pairs/spans = %d/%d, want 1/1", r.Tail.OverlapPairs, r.Tail.OverlapSpans)
	}
	if r.Checks.OverlapOK {
		t.Fatal("overlap_ok = true, want false")
	}
	if _, ok := detailWith(r, "顶层 span 两两交叠 1 对"); !ok {
		t.Fatalf("overlap detail missing: %v", r.Checks.Details)
	}
	if _, ok := anomalyWith(all, label, "overlap_ok"); !ok {
		t.Fatalf("overlap anomaly missing: %+v", all.Anomalies)
	}
}

// TestPreEpochSpanFailsOverlapCheck pins the overlap_ok boolean to the
// pre-epoch branch: a top-level span with t_us < 0 must fail the check, not
// only add a detail line. The fixture stays otherwise clean — no pairwise
// overlaps, no negative durations, and one real zero-duration point event
// inside the window (a fact counted as ZeroDurations, never as a defect).
func TestPreEpochSpanFailsOverlapCheck(t *testing.T) {
	root := t.TempDir()
	label := "r11_on"
	writeJSON(t, root, "runs/"+label+"/meta.json", map[string]any{
		"mode": "on", "n": 1, "seg_enabled": true,
		"drops": map[string]any{"loop_unpaired": 0, "samples_dropped": 0},
	})
	counts := countsAll(1)
	counts["sql_spans"] = 2
	writeJSON(t, root, "runs/"+label+"/anchors.json", anchorsFixture(1000, 2000, 2400, true, counts))
	writeFile(t, root, "runs/"+label+"/loop.csv", `kind,seq,t_us,dur_us,i1,i2,s1
poll,,-20,10,1,,
process,1,2000,20,0,1,INSERT 0 1
rebalance,,2200,0,,,
lag,,2500,20,,,
`)
	writeFile(t, root, "runs/"+label+"/sql.csv", `seq,t_us,dur_us,kind,sql,err
1,2000,1,begin,BEGIN,0
1,2018,1,commit,COMMIT,0
`)
	writeFile(t, root, "runs/"+label+"/exit_code", "0\n")

	all := analyze(t, root)
	r := runNamed(t, all, label)
	// The pre-epoch span is the only anomalous span: the window [2000,2400)
	// sees no pairwise overlap, no negative duration, and the zero-duration
	// point event at 2200us clips as full.
	if r.Tail.PreEpochSpans != 1 || r.Tail.OverlapPairs != 0 || r.Tail.NegativeDurations != 0 {
		t.Fatalf("pre-epoch/overlap/negative = %d/%d/%d, want 1/0/0",
			r.Tail.PreEpochSpans, r.Tail.OverlapPairs, r.Tail.NegativeDurations)
	}
	if r.Tail.ZeroDurations != 1 || r.Tail.FullSpans != 2 || r.Tail.OutsideSpans != 2 {
		t.Fatalf("zero/full/outside = %d/%d/%d, want 1/2/2",
			r.Tail.ZeroDurations, r.Tail.FullSpans, r.Tail.OutsideSpans)
	}
	if r.Checks.OverlapOK {
		t.Fatal("overlap_ok = true, want false: a pre-epoch span must fail the check, not only add a detail")
	}
	if _, ok := detailWith(r, "epoch 之前的 t_us span=1"); !ok {
		t.Fatalf("pre-epoch detail missing: %v", r.Checks.Details)
	}
	if _, ok := anomalyWith(all, label, "overlap_ok"); !ok {
		t.Fatalf("overlap anomaly missing: %+v", all.Anomalies)
	}
}

func TestStatsKnownSmallSampleAndTxSkipped(t *testing.T) {
	root := t.TempDir()
	label := "r04_on"
	writeJSON(t, root, "runs/"+label+"/meta.json", map[string]any{
		"mode": "on", "n": 1, "seg_enabled": true,
		"drops": map[string]any{"loop_unpaired": 0, "samples_dropped": 0},
	})
	counts := countsAll(1)
	counts["sql_spans"] = 5
	writeJSON(t, root, "runs/"+label+"/anchors.json", anchorsFixture(0, 0, 200, true, counts))
	writeFile(t, root, "runs/"+label+"/loop.csv", `kind,seq,t_us,dur_us,i1,i2,s1
process,1,0,100,0,1,INSERT 0 1
`)
	writeFile(t, root, "runs/"+label+"/sql.csv", `seq,t_us,dur_us,kind,sql,err
1,0,1,inbox_insert,INSERT a,0
1,10,2,inbox_insert,INSERT b,0
1,20,3,inbox_insert,INSERT c,0
1,30,4,inbox_insert,INSERT d,0
1,40,5,inbox_insert,INSERT e,0
`)
	writeFile(t, root, "runs/"+label+"/exit_code", "0\n")

	all := analyze(t, root)
	r := runNamed(t, all, label)
	d := r.Segments.SQL["inbox_insert"]
	if d == nil || d.N != 5 {
		t.Fatalf("inbox_insert dist = %+v, want n=5", d)
	}
	nearly(t, d.Mean, 3)
	nearly(t, d.P50, 3)
	nearly(t, d.P95, 4.8)  // rank = 0.95*4 = 3.8 -> 4*(1-0.8)+5*0.8
	nearly(t, d.P99, 4.96) // rank = 3.96
	nearly(t, d.Max, 5)
	nearly(t, d.Min, 1)
	proc := r.Segments.Loop["process"]
	if proc == nil || proc.N != 1 {
		t.Fatalf("process dist = %+v, want n=1", proc)
	}
	nearly(t, proc.Mean, 100)
	if r.Segments.TxEvents != 0 || r.Segments.TxSkipped != 1 || r.Segments.TxTotal != nil {
		t.Fatalf("tx = events=%d skipped=%d total=%+v, want 0/1/nil (event without begin/commit)",
			r.Segments.TxEvents, r.Segments.TxSkipped, r.Segments.TxTotal)
	}
	if !r.Checks.NestingOK || !r.Checks.CountsOK {
		t.Fatalf("checks = %+v, want nesting/counts OK", r.Checks)
	}
	// A mark span was never emitted: the kind stays nil, never 0.
	if r.Segments.Loop["mark"] != nil {
		t.Fatalf("mark dist = %+v, want nil", r.Segments.Loop["mark"])
	}
}

func TestNestingViolationBoundaries(t *testing.T) {
	root := t.TempDir()
	label := "r05_on"
	writeJSON(t, root, "runs/"+label+"/meta.json", map[string]any{
		"mode": "on", "n": 4, "seg_enabled": true,
		"drops": map[string]any{"loop_unpaired": 0, "samples_dropped": 0},
	})
	counts := countsAll(4)
	counts["sql_spans"] = 11
	writeJSON(t, root, "runs/"+label+"/anchors.json", anchorsFixture(0, 0, 400, true, counts))
	writeFile(t, root, "runs/"+label+"/loop.csv", `kind,seq,t_us,dur_us,i1,i2,s1
process,1,0,100,0,1,INSERT 0 1
process,2,100,100,0,2,INSERT 0 1
process,3,200,100,0,3,INSERT 0 1
process,4,300,100,0,4,INSERT 0 1
`)
	// Event 1: Σsql = 103us = process(100) + rows(3) -> exactly at the eps border, OK.
	// Event 2: Σsql = 104us -> one us over the border, violation.
	// Event 3: tx_total = 103us = process(100) + 3us -> one us over the tx border, violation.
	// Event 4: tx_total = 101us = process(100) + 1us -> OK.
	writeFile(t, root, "runs/"+label+"/sql.csv", `seq,t_us,dur_us,kind,sql,err
1,0,1,begin,BEGIN,0
1,2,101,inbox_insert,INSERT,0
1,4,1,commit,COMMIT,0
2,100,1,begin,BEGIN,0
2,102,102,inbox_insert,INSERT,0
2,104,1,commit,COMMIT,0
3,200,1,begin,BEGIN,0
3,303,0,commit,COMMIT,0
4,300,1,begin,BEGIN,0
4,401,0,commit,COMMIT,0
5,999,1,inbox_insert,ORPHAN,0
`)
	writeFile(t, root, "runs/"+label+"/exit_code", "0\n")

	all := analyze(t, root)
	r := runNamed(t, all, label)
	n := r.Segments.Nesting
	if n.EventsChecked != 4 {
		t.Fatalf("events checked = %d, want 4 (the orphan row has no process span)", n.EventsChecked)
	}
	if n.SumViolations != 1 {
		t.Fatalf("sum violations = %d, want 1 (104 > 100+3)", n.SumViolations)
	}
	if n.TxViolations != 1 {
		t.Fatalf("tx violations = %d, want 1 (103 > 100+2)", n.TxViolations)
	}
	if len(n.Examples) == 0 {
		t.Fatal("nesting examples missing")
	}
	if r.Checks.NestingOK {
		t.Fatal("nesting_ok = true, want false")
	}
	if _, ok := anomalyWith(all, label, "nesting_ok"); !ok {
		t.Fatalf("nesting anomaly missing: %+v", all.Anomalies)
	}
	// The orphan row (seq=5, no process span) is counted, never silently used.
	if r.Segments.SQLRowsNoProcess != 1 {
		t.Fatalf("sql rows without process = %d, want 1", r.Segments.SQLRowsNoProcess)
	}
}

func TestCountsMismatchDetected(t *testing.T) {
	root := t.TempDir()
	writeOnRun(t, root, "r01_on")
	// Rewrite the anchors with a broken partition identity: 5 != 3+1.
	counts := countsAll(3)
	counts["observed_process"] = 5
	counts["observed_applied"] = 3
	counts["observed_duplicate"] = 1
	counts["sql_spans"] = 6
	counts["applied_cb"] = 2
	writeJSON(t, root, "runs/r01_on/anchors.json", anchorsFixture(100, 190, 260, true, counts))

	all := analyze(t, root)
	r := runNamed(t, all, "r01_on")
	if r.Checks.CountsOK {
		t.Fatal("counts_ok = true, want false")
	}
	if _, ok := detailWith(r, "identity: process=5 != applied(3)+duplicate(1)"); !ok {
		t.Fatalf("identity detail missing: %v", r.Checks.Details)
	}
	if _, ok := anomalyWith(all, "r01_on", "counts_ok"); !ok {
		t.Fatalf("counts anomaly missing: %+v", all.Anomalies)
	}
	if r.Counts.IdentityOK == nil || *r.Counts.IdentityOK {
		t.Fatalf("identity_ok = %v, want false", r.Counts.IdentityOK)
	}
}

func TestOffRunNotCollectedAndGroupDelta(t *testing.T) {
	root := t.TempDir()
	writeTimingRun(t, root, "r01_on", "on", 1000, 10, 10)
	writeTimingRun(t, root, "r02_off", "off", 1000, 8, 8)

	all := analyze(t, root)
	on := runNamed(t, all, "r01_on")
	off := runNamed(t, all, "r02_off")
	if off.Segments.Collected || off.Tail.Collected {
		t.Fatalf("off run must mark loop/sql as not collected: %+v %+v", off.Segments, off.Tail)
	}
	if !off.Curve.Collected {
		t.Fatalf("the producer writes samples.csv for the OFF arm too: %+v", off.Curve)
	}
	if !off.Checks.OverlapOK || !off.Checks.NestingOK {
		t.Fatalf("unevaluated checks must not fail: %+v", off.Checks)
	}
	if _, ok := detailWith(off, "未采集（未评估）"); !ok {
		t.Fatalf("off run must name the unevaluated checks: %v", off.Checks.Details)
	}
	if len(off.NotCollected) != 3 {
		t.Fatalf("not_collected = %v, want 3 entries (loop/sql/publish)", off.NotCollected)
	}
	nearly(t, *on.TotalS, 20)
	nearly(t, *off.TotalS, 16)

	// Cross-run comparison: publish_s on=10 vs off=8 -> +2 absolute, +25%.
	var publish *metricCompare
	for i := range all.Comparison.Metrics {
		if all.Comparison.Metrics[i].Key == "publish_s" {
			publish = &all.Comparison.Metrics[i]
		}
	}
	if publish == nil {
		t.Fatal("publish_s metric missing from the comparison")
	}
	if publish.Groups["on"].N != 1 || publish.Groups["off"].N != 1 {
		t.Fatalf("group sizes = %+v", publish.Groups)
	}
	nearly(t, *publish.Groups["on"].Median, 10)
	nearly(t, *publish.Groups["off"].Median, 8)
	if publish.DeltaOnMinusOff == nil {
		t.Fatal("delta on-off missing")
	}
	nearly(t, publish.DeltaOnMinusOff.Abs, 2)
	nearly(t, *publish.DeltaOnMinusOff.RelPct, 25)
}

func TestPristineRunUsesReportTiming(t *testing.T) {
	root := t.TempDir()
	label := "r00_pristine"
	writeJSON(t, root, "runs/"+label+"/report.json", map[string]any{
		"harness":                       "backlog-drain",
		"n":                             10000,
		"publish_seconds":               12.5,
		"consumer_catchup_tail_seconds": 7.5,
		"total_drain_seconds":           20.0,
		"end_to_end_events_per_second":  500.0,
		"publish_events_per_second":     800.0,
	})
	writeFile(t, root, "runs/"+label+"/exit_code", "0\n")

	all := analyze(t, root)
	r := runNamed(t, all, label)
	if r.Collection != "pristine" || r.Group != "pristine" {
		t.Fatalf("collection/group = %q/%q, want pristine/pristine", r.Collection, r.Group)
	}
	if r.TimingSource != "report" {
		t.Fatalf("timing source = %q, want report", r.TimingSource)
	}
	nearly(t, *r.PublishS, 12.5)
	nearly(t, *r.TailS, 7.5)
	nearly(t, *r.TotalS, 20.0)
	nearly(t, *r.E2ERate, 500)
	nearly(t, *r.PublishRate, 800)
	if r.Checks.AnchorsComplete {
		t.Fatal("anchors_complete = true for a pristine run, want false (factual)")
	}
	if len(r.Checks.FilesMissing) != 0 {
		t.Fatalf("files_missing = %v, want empty (report.json + exit_code are the pristine protocol)", r.Checks.FilesMissing)
	}
	if len(r.NotCollected) != 6 {
		t.Fatalf("not_collected = %v, want 6 entries", r.NotCollected)
	}
	if _, ok := detailWith(r, "pristine"); !ok {
		t.Fatalf("pristine note missing: %v", r.Checks.Details)
	}
	if _, ok := anomalyWith(all, label, "anchors_complete"); ok {
		t.Fatal("a pristine run must not raise an anchors_complete anomaly")
	}
}

func TestReportVsAnchorsDeviationDetected(t *testing.T) {
	root := t.TempDir()
	writeOnRun(t, root, "r01_on")
	// Bump publish_seconds by 1e-5 s (10x the 1e-6 tolerance).
	body, err := os.ReadFile(filepath.Join(root, "runs/r01_on/report.json"))
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := json.Unmarshal(body, &doc); err != nil {
		t.Fatal(err)
	}
	doc["publish_seconds"] = float64(190)/1e6 + 1e-5
	writeJSON(t, root, "runs/r01_on/report.json", doc)

	all := analyze(t, root)
	r := runNamed(t, all, "r01_on")
	if r.Checks.ReportVsAnchorsOK {
		t.Fatal("report_vs_anchors_ok = true, want false")
	}
	if _, ok := detailWith(r, "publish_seconds anchors=0.000090000"); !ok {
		t.Fatalf("deviation detail missing: %v", r.Checks.Details)
	}
	if _, ok := anomalyWith(all, "r01_on", "report_vs_anchors_ok"); !ok {
		t.Fatalf("report_vs_anchors anomaly missing: %+v", all.Anomalies)
	}
}

func TestReportExtensionSecondsAreSinceDrain(t *testing.T) {
	root := t.TempDir()
	writeOnRun(t, root, "r01_on")
	// Rewrite one extension field as an epoch-absolute value: the anchor's
	// last cycle end is 150us absolute, i.e. 50us since drain_start=100us.
	body, err := os.ReadFile(filepath.Join(root, "runs/r01_on/report.json"))
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := json.Unmarshal(body, &doc); err != nil {
		t.Fatal(err)
	}
	doc["publish_last_cycle_end_seconds"] = float64(150) / 1e6
	writeJSON(t, root, "runs/r01_on/report.json", doc)

	all := analyze(t, root)
	r := runNamed(t, all, "r01_on")
	if r.Checks.ReportVsAnchorsOK {
		t.Fatal("report_vs_anchors_ok = true, want false: the extension field is an offset since drain_start")
	}
	if _, ok := detailWith(r, "publish_last_cycle_end_seconds anchors=0.000050000"); !ok {
		t.Fatalf("since-drain detail missing: %v", r.Checks.Details)
	}
}

func TestPublishBoundaryRowIsNotACycle(t *testing.T) {
	root := t.TempDir()
	writeOnRun(t, root, "r01_on")
	// Drop the terminal boundary row: the file then holds one productive cycle
	// for anchors.publish.cycles=1, but the terminal form is gone.
	writeFile(t, root, "runs/r01_on/publish.csv", `iter,t_count_us,count_dur_us,pending_seen,t_publish_us,publish_dur_us,claimed,acked,released,blocked
1,10,20,50,30,60,50,50,0,0
`)
	all := analyze(t, root)
	r := runNamed(t, all, "r01_on")
	if r.Publish.Rows != 1 || r.Publish.ProductiveCycles != 1 || r.Publish.BoundaryRows != 0 {
		t.Fatalf("publish = %+v, want 1 row / 1 productive / 0 boundary", r.Publish)
	}
	if r.Publish.LastCycleTerminal {
		t.Fatal("last_cycle_terminal = true, want false without the boundary row")
	}
	if r.Checks.CountsOK {
		t.Fatal("counts_ok = true, want false: the terminal boundary row is missing")
	}
}

// writeOffSentinelRun writes the producer's OFF-arm shape where the two
// collection-dependent consume anchors carry the frozen 0 sentinel.
func writeOffSentinelRun(t *testing.T, root, label string) {
	t.Helper()
	anchors := anchorsFixture(1000, 5000, 9000, false, countsAll(50))
	consume := anchors["consume"].(map[string]any)
	consume["last_commit_end_us"] = 0
	consume["last_applied_cb_us"] = 0
	writeJSON(t, root, "runs/"+label+"/meta.json", map[string]any{
		"harness": "013-supplement/consumer-seg/1", "mode": "off", "n": 50, "seg_enabled": false,
		"drops": map[string]any{"loop_unpaired": 0, "samples_dropped": 0},
	})
	writeJSON(t, root, "runs/"+label+"/anchors.json", anchors)
	writeJSON(t, root, "runs/"+label+"/report.json", map[string]any{
		"n":                              50,
		"publish_seconds":                float64(4000) / 1e6,
		"consumer_catchup_tail_seconds":  float64(4000) / 1e6,
		"total_drain_seconds":            float64(8000) / 1e6,
		"end_to_end_events_per_second":   50.0 / (float64(8000) / 1e6),
		"publish_events_per_second":      50.0 / (float64(4000) / 1e6),
		"consumer_applied":               50,
		"publish_cycles":                 1,
		"publish_claimed":                50,
		"publish_acked":                  50,
		"publish_released":               0,
		"publish_blocked":                0,
		"seg_enabled":                    false,
		"applied_at_publish_done":        42,
		"publish_last_cycle_end_seconds": float64(3960) / 1e6,
		"pending_zero_observed_seconds":  float64(3970) / 1e6,
		"consume_confirm_seconds":        float64(8000) / 1e6,
		"last_commit_end_seconds":        0,
		"last_applied_cb_seconds":        0,
		"process_observed":               50,
		"sql_span_count":                 0,
		"seg_drops":                      map[string]any{"loop_unpaired": 0, "samples_dropped": 0},
		"lag_final":                      map[string]int64{"0": 2, "1": 0},
	})
	writeFile(t, root, "runs/"+label+"/exit_code", "0\n")
	writeFile(t, root, "logs/"+label+".rc", "0\n")
}

func TestOffArmZeroSentinelsAreNotCollected(t *testing.T) {
	root := t.TempDir()
	writeOffSentinelRun(t, root, "r02_off")
	all := analyze(t, root)
	r := runNamed(t, all, "r02_off")

	// All checks stay green: the sentinel is a consistent not-collected value,
	// not a mismatch.
	for name, ok := range map[string]bool{
		"anchors_complete":     r.Checks.AnchorsComplete,
		"counts_ok":            r.Checks.CountsOK,
		"drops_ok":             r.Checks.DropsOK,
		"overlap_ok":           r.Checks.OverlapOK,
		"nesting_ok":           r.Checks.NestingOK,
		"report_vs_anchors_ok": r.Checks.ReportVsAnchorsOK,
	} {
		if !ok {
			t.Fatalf("%s = false, want true; details=%v", name, r.Checks.Details)
		}
	}
	if _, ok := detailWith(r, "哨兵"); !ok {
		t.Fatalf("sentinel note missing: %v", r.Checks.Details)
	}

	// The two dependent latencies are not collected (no epoch-scale artifact);
	// the two publish-side latencies are unaffected.
	if r.Latency.ConsumeConfirmLatencyUS != nil {
		t.Fatalf("consume_confirm_latency = %d, want not collected", *r.Latency.ConsumeConfirmLatencyUS)
	}
	if r.Latency.AppliedCBLatencyUS != nil {
		t.Fatalf("applied_cb_latency = %d, want not collected", *r.Latency.AppliedCBLatencyUS)
	}
	if r.Latency.PublishObserveLatencyUS == nil || *r.Latency.PublishObserveLatencyUS != 10 {
		t.Fatalf("publish_observe_latency = %v, want 10", r.Latency.PublishObserveLatencyUS)
	}
	if r.Latency.PublishDoneLatencyUS == nil || *r.Latency.PublishDoneLatencyUS != 40 {
		t.Fatalf("publish_done_latency = %v, want 40", r.Latency.PublishDoneLatencyUS)
	}

	// The anchors view marks the sentinel fields as not collected instead of
	// echoing a "0 offset".
	if r.Anchors.Consume == nil {
		t.Fatal("anchors.consume missing")
	}
	if r.Anchors.Consume.LastCommitEndUS != nil || r.Anchors.Consume.LastAppliedCBUS != nil {
		t.Fatalf("anchors.consume = %+v, want the sentinel fields null", r.Anchors.Consume)
	}
	nc := strings.Join(r.Anchors.Consume.NotCollected, ",")
	if !strings.Contains(nc, "last_commit_end_us") || !strings.Contains(nc, "last_applied_cb_us") {
		t.Fatalf("anchors.consume.not_collected = %v, want both sentinel fields", r.Anchors.Consume.NotCollected)
	}

	// The markdown overview renders "-" for the two uncollected latencies and
	// never a huge number.
	md := renderMarkdown(all)
	var row string
	for _, line := range strings.Split(md, "\n") {
		if strings.HasPrefix(line, "| r02_off ") {
			row = line
			break
		}
	}
	if row == "" {
		t.Fatal("r02_off overview row not found in summary.md")
	}
	cells := strings.Split(row, "|")
	if len(cells) < 15 {
		t.Fatalf("overview row = %q", row)
	}
	// Column order: label, group, collection, n, rc, publish_s, tail_s,
	// total_s, e2e_rate, observe, done, confirm, cb, timing source.
	if strings.TrimSpace(cells[12]) != "-" || strings.TrimSpace(cells[13]) != "-" {
		t.Fatalf("确认延迟/cb延迟 cells = %q/%q, want \"-\"", cells[12], cells[13])
	}
	if strings.Contains(row, "10253570") || strings.Contains(row, "-9.5") {
		t.Fatalf("overview row leaks a sentinel-derived value: %q", row)
	}
}

func TestSentinelReportMismatchIsFlagged(t *testing.T) {
	root := t.TempDir()
	writeOffSentinelRun(t, root, "r02_off")
	// A report that carries an offset for a sentinel anchor is inconsistent:
	// the comparison must flag it instead of converting 0 into an offset.
	body, err := os.ReadFile(filepath.Join(root, "runs/r02_off/report.json"))
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := json.Unmarshal(body, &doc); err != nil {
		t.Fatal(err)
	}
	doc["last_commit_end_seconds"] = float64(7000) / 1e6
	writeJSON(t, root, "runs/r02_off/report.json", doc)

	all := analyze(t, root)
	r := runNamed(t, all, "r02_off")
	if r.Checks.ReportVsAnchorsOK {
		t.Fatal("report_vs_anchors_ok = true, want false for a sentinel/offset mismatch")
	}
	if _, ok := detailWith(r, "anchors=0（未采集哨兵）但 report="); !ok {
		t.Fatalf("sentinel mismatch detail missing: %v", r.Checks.Details)
	}
}

func TestWritesPerRunSummaryAndCustomOut(t *testing.T) {
	root := t.TempDir()
	writeOnRun(t, root, "r01_on")
	out := filepath.Join(root, "custom-out")
	all := analyzeTo(t, root, out, "-out", out)
	if len(all.Runs) != 1 {
		t.Fatalf("runs = %d, want 1", len(all.Runs))
	}
	if _, err := os.Stat(filepath.Join(out, "summary.md")); err != nil {
		t.Fatalf("summary.md in custom out: %v", err)
	}
	body, err := os.ReadFile(filepath.Join(root, "runs/r01_on/summary.json"))
	if err != nil {
		t.Fatalf("per-run summary.json: %v", err)
	}
	var rs runSummary
	if err := json.Unmarshal(body, &rs); err != nil {
		t.Fatalf("unmarshal per-run summary.json: %v", err)
	}
	if rs.Label != "r01_on" || rs.Checks.AnchorsComplete != true {
		t.Fatalf("per-run summary = %+v", rs)
	}
	md, err := os.ReadFile(filepath.Join(out, "summary.md"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"# 013-supplement 消费者追赶分段测量分析", "## 4. tail 裁剪", "raw full spans", "## 9. 跨轮对照"} {
		if !strings.Contains(string(md), want) {
			t.Fatalf("summary.md misses %q", want)
		}
	}
}

func TestNoParseableRunExitsOne(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "runs", "r99_on"), 0o755); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if code := run([]string{"-dir", root}, &stdout, &stderr); code != 1 {
		t.Fatalf("run = %d, want 1; stderr=%s", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "no parseable run") {
		t.Fatalf("stderr = %q, want the no-parseable-run message", stderr.String())
	}
}

func TestUsageErrorExitsTwo(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run(nil, &stdout, &stderr); code != 2 {
		t.Fatalf("run(nil) = %d, want 2", code)
	}
	if code := run([]string{"-dir", t.TempDir(), "-nope"}, &stdout, &stderr); code != 2 {
		t.Fatalf("run(bad flag) = %d, want 2", code)
	}
}
