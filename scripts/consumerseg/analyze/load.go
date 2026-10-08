// load.go reads the frozen v1 evidence layout of the 013-supplement
// consumer-catchup segmented batch:
//
//	<root>/runs/<label>/{meta.json,anchors.json,report.json,exit_code}
//	<root>/runs/<label>/{samples,loop,sql,publish}.csv[.gz]
//	<root>/logs/<label>.rc
//
// Every file is optional. A pristine legacy run carries only report.json and
// exit_code; an OFF control run carries no loop/sql collection. Missing files
// are reported as "not collected" by the caller and are never an error. Only
// row-level defects are counted as parse errors and skipped.
package main

import (
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// loopKinds is the frozen top-level loop-span vocabulary of the measurement
// seam (applied_cb is the nested observer callback and stays out of the
// top-level clipping).
var loopKinds = []string{"poll", "process", "mark", "rebalance", "lag"}

// loopKindAppliedCB is the nested observer callback kind (dur is always 0).
const loopKindAppliedCB = "applied_cb"

// loopKindProcess is the per-event processing span kind.
const loopKindProcess = "process"

// sqlKinds is the frozen SQL statement vocabulary; any other kind is folded
// into "other".
var sqlKinds = []string{
	"begin", "commit", "rollback", "inbox_insert", "version_read",
	"version_probe", "effect_insert", "version_upsert", "progress_update",
	"quarantine_insert", "other",
}

// csvBases are the four CSV evidence files, probed plain first, ".gz" second.
var csvBases = []string{"samples.csv", "loop.csv", "sql.csv", "publish.csv"}

var (
	samplesHeader = []string{
		"t_us", "pending", "published", "applied", "ledger", "progress",
		"acquire_count", "empty_acquire_count", "acquire_duration_us",
		"canceled_acquire_count", "total_conns", "idle_conns",
	}
	loopHeader    = []string{"kind", "seq", "t_us", "dur_us", "i1", "i2", "s1"}
	sqlHeader     = []string{"seq", "t_us", "dur_us", "kind", "sql", "err"}
	publishHeader = []string{
		"iter", "t_count_us", "count_dur_us", "pending_seen", "t_publish_us",
		"publish_dur_us", "claimed", "acked", "released", "blocked",
	}
)

// runInput is one discovered run directory.
type runInput struct {
	Label string
	Dir   string
}

// discoverRuns lists <root>/runs/<label> in a deterministic (label-sorted)
// order.
func discoverRuns(root string) ([]runInput, error) {
	base := filepath.Join(root, "runs")
	entries, err := os.ReadDir(base)
	if err != nil {
		return nil, fmt.Errorf("read runs directory: %w", err)
	}
	runs := make([]runInput, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		runs = append(runs, runInput{Label: e.Name(), Dir: filepath.Join(base, e.Name())})
	}
	sort.Slice(runs, func(i, j int) bool { return runs[i].Label < runs[j].Label })
	return runs, nil
}

// ------------------------------------------------------------------ json docs

// dropsDoc mirrors the meta/report drop counters (loop_unpaired must be 0;
// samples_dropped is recorded, not gated).
type dropsDoc struct {
	LoopUnpaired   *int64 `json:"loop_unpaired"`
	SamplesDropped *int64 `json:"samples_dropped"`
}

// metaDoc mirrors the frozen meta.json keys the analyzer consumes; unknown
// keys are ignored and every scalar is optional so absence stays visible.
// (vcs_modified is deliberately not typed: the producer records it as a
// string, and a mistyped field would fail the whole document.)
type metaDoc struct {
	Harness        string    `json:"harness"`
	Mode           string    `json:"mode"`
	StartedAtWall  string    `json:"started_at_wall"`
	FinishedAtWall string    `json:"finished_at_wall"`
	EpochWall      string    `json:"epoch_wall"`
	GoVersion      string    `json:"go_version"`
	GOMAXPROCS     *int      `json:"gomaxprocs"`
	HostCPUs       *int      `json:"host_cpus"`
	VCSRevision    string    `json:"vcs_revision"`
	N              *int64    `json:"n"`
	SegEnabled     *bool     `json:"seg_enabled"`
	Drops          *dropsDoc `json:"drops"`
}

// publishAnchorsDoc mirrors anchors.publish.
type publishAnchorsDoc struct {
	FirstCycleStartUS     *int64 `json:"first_cycle_start_us"`
	LastCycleEndUS        *int64 `json:"last_cycle_end_us"`
	PendingZeroObservedUS *int64 `json:"pending_zero_observed_us"`
	PublishDoneUS         *int64 `json:"publish_done_us"`
	Cycles                *int64 `json:"cycles"`
	Claimed               *int64 `json:"claimed"`
	Acked                 *int64 `json:"acked"`
	Released              *int64 `json:"released"`
	Blocked               *int64 `json:"blocked"`
	AppliedAtPublishDone  *int64 `json:"applied_at_publish_done"`
}

// consumeAnchorsDoc mirrors anchors.consume.
type consumeAnchorsDoc struct {
	PollConfirmUS     *int64 `json:"poll_confirm_us"`
	ConsumerStoppedUS *int64 `json:"consumer_stopped_us"`
	AppliedAtConfirm  *int64 `json:"applied_at_confirm"`
	LastCommitEndUS   *int64 `json:"last_commit_end_us"`
	LastAppliedCBUS   *int64 `json:"last_applied_cb_us"`
}

// countsAnchorsDoc mirrors anchors.counts.
type countsAnchorsDoc struct {
	ObservedProcess     *int64 `json:"observed_process"`
	ObservedApplied     *int64 `json:"observed_applied"`
	ObservedDuplicate   *int64 `json:"observed_duplicate"`
	ObservedVersionSkip *int64 `json:"observed_version_skip"`
	ObservedQuarantined *int64 `json:"observed_quarantined"`
	ObservedErrors      *int64 `json:"observed_errors"`
	SQLSpans            *int64 `json:"sql_spans"`
	SQLUnattributed     *int64 `json:"sql_unattributed"`
	AppliedCB           *int64 `json:"applied_cb"`
}

// anchorsDoc mirrors anchors.json. Every scalar is a pointer so a missing key
// is distinguishable from a legitimate zero.
type anchorsDoc struct {
	DrainStartUS *int64             `json:"drain_start_us"`
	Publish      *publishAnchorsDoc `json:"publish"`
	Consume      *consumeAnchorsDoc `json:"consume"`
	Counts       *countsAnchorsDoc  `json:"counts"`
	LagFinal     map[string]int64   `json:"lag_final"`
	SegEnabled   *bool              `json:"seg_enabled"`
}

// poolDoc mirrors the report's pool_final block.
type poolDoc struct {
	AcquireCount           *int64   `json:"acquire_count"`
	EmptyAcquireCount      *int64   `json:"empty_acquire_count"`
	AcquireDurationSeconds *float64 `json:"acquire_duration_seconds"`
	CanceledAcquireCount   *int64   `json:"canceled_acquire_count"`
}

// reportDoc mirrors the original backlogReport plus the v1 extension fields.
// The original witness keys keep their original names; the extension keys are
// the reconciliation targets of the analyzer.
type reportDoc struct {
	Harness string `json:"harness"`

	// original backlogReport witness fields.
	N                      *int64             `json:"n"`
	SeedSeconds            *float64           `json:"seed_seconds"`
	PublishSeconds         *float64           `json:"publish_seconds"`
	PublishEventsPerSec    *float64           `json:"publish_events_per_second"`
	TotalDrainSeconds      *float64           `json:"total_drain_seconds"`
	EndToEndEventsPerSec   *float64           `json:"end_to_end_events_per_second"`
	ConsumerTailSeconds    *float64           `json:"consumer_catchup_tail_seconds"`
	ConsumerApplied        *int64             `json:"consumer_applied"`
	PublishCycles          *int64             `json:"publish_cycles"`
	PublishClaimed         *int64             `json:"publish_claimed"`
	PublishAcked           *int64             `json:"publish_acked"`
	PublishReleased        *int64             `json:"publish_released"`
	PublishBlocked         *int64             `json:"publish_blocked"`
	PublishFailuresByClass map[string]float64 `json:"publish_failures_by_class"`

	// v1 extension fields (consumer-seg).
	SegEnabled                 *bool            `json:"seg_enabled"`
	AppliedAtPublishDone       *int64           `json:"applied_at_publish_done"`
	PublishLastCycleEndSeconds *float64         `json:"publish_last_cycle_end_seconds"`
	PendingZeroObservedSeconds *float64         `json:"pending_zero_observed_seconds"`
	ConsumeConfirmSeconds      *float64         `json:"consume_confirm_seconds"`
	LastCommitEndSeconds       *float64         `json:"last_commit_end_seconds"`
	LastAppliedCBSeconds       *float64         `json:"last_applied_cb_seconds"`
	ProcessObserved            *int64           `json:"process_observed"`
	SQLSpanCount               *int64           `json:"sql_span_count"`
	SegDrops                   *dropsDoc        `json:"seg_drops"`
	LagFinal                   map[string]int64 `json:"lag_final"`
	PoolFinal                  *poolDoc         `json:"pool_final"`
}

// usOffset reports whether p carries a real monotonic microsecond offset.
//
// Frozen v1 semantics: 0 is the "never observed / not collected" sentinel for
// the anchors whose values come from the segmented collection — the producer
// writes 0 for anchors.consume.last_commit_end_us and last_applied_cb_us when
// the arm has no statement tracer and no applied-callback loop rows (the OFF
// arm), and its own seconds conversion maps 0 to 0. A real offset is always
// >= drain_start (seconds into the run) and therefore never 0.
func usOffset(p *int64) bool { return p != nil && *p != 0 }

// missingKeys lists the anchors.json keys of the frozen v1 schema that are
// absent (so checks.anchors_complete can name exactly what is missing).
func (a *anchorsDoc) missingKeys() []string {
	if a == nil {
		return []string{"anchors.json"}
	}
	var missing []string
	add := func(cond bool, name string) {
		if cond {
			missing = append(missing, name)
		}
	}
	add(a.DrainStartUS == nil, "drain_start_us")
	if a.Publish == nil {
		missing = append(missing, "publish")
	} else {
		p := a.Publish
		add(p.FirstCycleStartUS == nil, "publish.first_cycle_start_us")
		add(p.LastCycleEndUS == nil, "publish.last_cycle_end_us")
		add(p.PendingZeroObservedUS == nil, "publish.pending_zero_observed_us")
		add(p.PublishDoneUS == nil, "publish.publish_done_us")
		add(p.Cycles == nil, "publish.cycles")
		add(p.Claimed == nil, "publish.claimed")
		add(p.Acked == nil, "publish.acked")
		add(p.Released == nil, "publish.released")
		add(p.Blocked == nil, "publish.blocked")
		add(p.AppliedAtPublishDone == nil, "publish.applied_at_publish_done")
	}
	if a.Consume == nil {
		missing = append(missing, "consume")
	} else {
		c := a.Consume
		add(c.PollConfirmUS == nil, "consume.poll_confirm_us")
		add(c.ConsumerStoppedUS == nil, "consume.consumer_stopped_us")
		add(c.AppliedAtConfirm == nil, "consume.applied_at_confirm")
		add(c.LastCommitEndUS == nil, "consume.last_commit_end_us")
		add(c.LastAppliedCBUS == nil, "consume.last_applied_cb_us")
	}
	if a.Counts == nil {
		missing = append(missing, "counts")
	} else {
		c := a.Counts
		add(c.ObservedProcess == nil, "counts.observed_process")
		add(c.ObservedApplied == nil, "counts.observed_applied")
		add(c.ObservedDuplicate == nil, "counts.observed_duplicate")
		add(c.ObservedVersionSkip == nil, "counts.observed_version_skip")
		add(c.ObservedQuarantined == nil, "counts.observed_quarantined")
		add(c.ObservedErrors == nil, "counts.observed_errors")
		add(c.SQLSpans == nil, "counts.sql_spans")
		add(c.SQLUnattributed == nil, "counts.sql_unattributed")
		add(c.AppliedCB == nil, "counts.applied_cb")
	}
	add(a.LagFinal == nil, "lag_final")
	add(a.SegEnabled == nil, "seg_enabled")
	return missing
}

// ----------------------------------------------------------------- row types

// loopRow is one loop.csv row. Values that do not apply to the row's kind stay
// absent (Has* false).
type loopRow struct {
	Kind   string
	Seq    int64
	HasSeq bool
	T      int64
	Dur    int64
	I1     int64
	HasI1  bool
	I2     int64
	HasI2  bool
	S1     string
}

// sqlRow is one sql.csv row (seq == -1 means "outside every event").
type sqlRow struct {
	Seq  int64
	T    int64
	Dur  int64
	Kind string
	SQL  string
	Err  int
}

// publishRow is one publish.csv row (one bounded publish cycle).
type publishRow struct {
	Iter         int64
	TCountUS     int64
	CountDurUS   int64
	PendingSeen  int64
	TPublishUS   int64
	PublishDurUS int64
	Claimed      int64
	Acked        int64
	Released     int64
	Blocked      int64
}

// sampleRow is one 200ms monitor sample.
type sampleRow struct {
	TUS                  int64
	Pending              int64
	Published            int64
	Applied              int64
	Ledger               int64
	Progress             int64
	AcquireCount         int64
	EmptyAcquireCount    int64
	AcquireDurationUS    int64
	CanceledAcquireCount int64
	TotalConns           int64
	IdleConns            int64
}

// loadedRun is one run directory after the read pass: every artifact is
// optional, every defect is a note, never a fatal error.
type loadedRun struct {
	label  string
	dir    string
	relDir string

	present map[string]string // logical file name -> concrete path
	notes   []string

	meta      *metaDoc
	anchors   *anchorsDoc
	report    *reportDoc
	reportSet bool

	exitCode       *int
	exitCodeSource string
	rcExit         *int
	rcSet          bool

	samples   []sampleRow
	loop      []loopRow
	sql       []sqlRow
	publish   []publishRow
	parseErrs map[string]int // file base name -> row parse errors
}

// probeFile resolves a logical run file: the plain name first, the ".gz"
// sibling second.
func probeFile(dir, name string) (string, bool) {
	plain := filepath.Join(dir, name)
	if st, err := os.Stat(plain); err == nil && !st.IsDir() {
		return plain, true
	}
	gz := plain + ".gz"
	if st, err := os.Stat(gz); err == nil && !st.IsDir() {
		return gz, true
	}
	return "", false
}

// gzReadCloser closes both the gzip reader and the underlying file.
type gzReadCloser struct {
	zr *gzip.Reader
	f  *os.File
}

func (g *gzReadCloser) Read(p []byte) (int, error) { return g.zr.Read(p) }

func (g *gzReadCloser) Close() error {
	err := g.zr.Close()
	if cerr := g.f.Close(); err == nil {
		err = cerr
	}
	return err
}

// openMaybeGzip opens a file and transparently decompresses a ".gz" suffix.
func openMaybeGzip(path string) (io.ReadCloser, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	if !strings.HasSuffix(path, ".gz") {
		return f, nil
	}
	zr, err := gzip.NewReader(f)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("gzip: %w", err)
	}
	return &gzReadCloser{zr: zr, f: f}, nil
}

// readCSV streams one CSV file after validating its frozen header. The header
// must match exactly; a mismatch aborts the file with an error (the caller
// records it as a note and treats the artifact as unusable).
func readCSV(path string, header []string, fn func(rec []string) error) error {
	rc, err := openMaybeGzip(path)
	if err != nil {
		return err
	}
	defer func() { _ = rc.Close() }()
	cr := csv.NewReader(rc)
	cr.FieldsPerRecord = -1
	first, err := cr.Read()
	if err != nil {
		return fmt.Errorf("read header: %w", err)
	}
	if len(first) != len(header) {
		return fmt.Errorf("header has %d columns, want %d", len(first), len(header))
	}
	for i, want := range header {
		if strings.TrimSpace(first[i]) != want {
			return fmt.Errorf("header column %d is %q, want %q", i+1, strings.TrimSpace(first[i]), want)
		}
	}
	for {
		rec, err := cr.Read()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read row: %w", err)
		}
		if err := fn(rec); err != nil {
			return err
		}
	}
}

func parseInt64(s string) (int64, error) {
	return strconv.ParseInt(strings.TrimSpace(s), 10, 64)
}

// optInt64 parses an optional integer cell ("" means absent).
func optInt64(s string) (v int64, ok bool, err error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, false, nil
	}
	v, err = parseInt64(s)
	if err != nil {
		return 0, false, err
	}
	return v, true, nil
}

// decodeJSONFile decodes one JSON document.
func decodeJSONFile(path string, dst any) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, dst)
}

// scanSamplesFile reads samples.csv[.gz].
func scanSamplesFile(path string) (rows []sampleRow, parseErrors int, err error) {
	err = readCSV(path, samplesHeader, func(rec []string) error {
		if len(rec) != len(samplesHeader) {
			parseErrors++
			return nil
		}
		values := make([]int64, len(rec))
		for i, cell := range rec {
			v, perr := parseInt64(cell)
			if perr != nil {
				parseErrors++
				return nil
			}
			values[i] = v
		}
		rows = append(rows, sampleRow{
			TUS: values[0], Pending: values[1], Published: values[2],
			Applied: values[3], Ledger: values[4], Progress: values[5],
			AcquireCount: values[6], EmptyAcquireCount: values[7],
			AcquireDurationUS: values[8], CanceledAcquireCount: values[9],
			TotalConns: values[10], IdleConns: values[11],
		})
		return nil
	})
	return rows, parseErrors, err
}

// scanLoopFile reads loop.csv[.gz].
func scanLoopFile(path string) (rows []loopRow, parseErrors int, err error) {
	err = readCSV(path, loopHeader, func(rec []string) error {
		if len(rec) != len(loopHeader) {
			parseErrors++
			return nil
		}
		row := loopRow{Kind: strings.TrimSpace(rec[0]), S1: strings.TrimSpace(rec[6])}
		if row.T, err = parseInt64(rec[2]); err != nil {
			parseErrors++
			return nil
		}
		if row.Dur, err = parseInt64(rec[3]); err != nil {
			parseErrors++
			return nil
		}
		if v, ok, perr := optInt64(rec[1]); perr != nil {
			parseErrors++
			return nil
		} else if ok {
			row.Seq, row.HasSeq = v, true
		}
		if v, ok, perr := optInt64(rec[4]); perr != nil {
			parseErrors++
			return nil
		} else if ok {
			row.I1, row.HasI1 = v, true
		}
		if v, ok, perr := optInt64(rec[5]); perr != nil {
			parseErrors++
			return nil
		} else if ok {
			row.I2, row.HasI2 = v, true
		}
		rows = append(rows, row)
		return nil
	})
	return rows, parseErrors, err
}

// scanSQLFile reads sql.csv[.gz].
func scanSQLFile(path string) (rows []sqlRow, parseErrors int, err error) {
	err = readCSV(path, sqlHeader, func(rec []string) error {
		if len(rec) != len(sqlHeader) {
			parseErrors++
			return nil
		}
		var row sqlRow
		var perr error
		if row.Seq, perr = parseInt64(rec[0]); perr != nil {
			parseErrors++
			return nil
		}
		if row.T, perr = parseInt64(rec[1]); perr != nil {
			parseErrors++
			return nil
		}
		if row.Dur, perr = parseInt64(rec[2]); perr != nil {
			parseErrors++
			return nil
		}
		row.Kind = strings.TrimSpace(rec[3])
		row.SQL = strings.TrimSpace(rec[4])
		if v, ok, perr := optInt64(rec[5]); perr != nil {
			parseErrors++
			return nil
		} else if ok {
			row.Err = int(v)
		}
		rows = append(rows, row)
		return nil
	})
	return rows, parseErrors, err
}

// scanPublishFile reads publish.csv[.gz].
func scanPublishFile(path string) (rows []publishRow, parseErrors int, err error) {
	err = readCSV(path, publishHeader, func(rec []string) error {
		if len(rec) != len(publishHeader) {
			parseErrors++
			return nil
		}
		values := make([]int64, len(rec))
		for i, cell := range rec {
			v, perr := parseInt64(cell)
			if perr != nil {
				parseErrors++
				return nil
			}
			values[i] = v
		}
		rows = append(rows, publishRow{
			Iter: values[0], TCountUS: values[1], CountDurUS: values[2],
			PendingSeen: values[3], TPublishUS: values[4], PublishDurUS: values[5],
			Claimed: values[6], Acked: values[7], Released: values[8], Blocked: values[9],
		})
		return nil
	})
	return rows, parseErrors, err
}

// loadRun reads every artifact of one run directory, tolerating absence.
func loadRun(root string, ri runInput) *loadedRun {
	lr := &loadedRun{
		label:     ri.Label,
		dir:       ri.Dir,
		present:   map[string]string{},
		parseErrs: map[string]int{},
	}
	lr.relDir = relPath(root, ri.Dir)

	if path, ok := probeFile(ri.Dir, "meta.json"); ok {
		lr.present["meta.json"] = path
		doc := &metaDoc{}
		if err := decodeJSONFile(path, doc); err != nil {
			lr.notes = append(lr.notes, "meta.json: "+err.Error())
		} else {
			lr.meta = doc
		}
	}
	if path, ok := probeFile(ri.Dir, "anchors.json"); ok {
		lr.present["anchors.json"] = path
		doc := &anchorsDoc{}
		if err := decodeJSONFile(path, doc); err != nil {
			lr.notes = append(lr.notes, "anchors.json: "+err.Error())
		} else {
			lr.anchors = doc
		}
	}
	if path, ok := probeFile(ri.Dir, "report.json"); ok {
		lr.present["report.json"] = path
		doc := &reportDoc{}
		if err := decodeJSONFile(path, doc); err != nil {
			lr.notes = append(lr.notes, "report.json: "+err.Error())
		} else {
			lr.report = doc
			lr.reportSet = true
		}
	}
	if path, ok := probeFile(ri.Dir, "exit_code"); ok {
		lr.present["exit_code"] = path
		raw, err := os.ReadFile(path)
		if err != nil {
			lr.notes = append(lr.notes, "exit_code: "+err.Error())
		} else if v, err := strconv.Atoi(strings.TrimSpace(string(raw))); err != nil {
			lr.notes = append(lr.notes, "exit_code: unparsable value "+strconv.Quote(strings.TrimSpace(string(raw))))
		} else {
			lr.exitCode = new(v)
			lr.exitCodeSource = "exit_code"
		}
	}
	rcPath := filepath.Join(root, "logs", ri.Label+".rc")
	if raw, err := os.ReadFile(rcPath); err == nil {
		if v, err := strconv.Atoi(strings.TrimSpace(string(raw))); err == nil {
			lr.rcExit = new(v)
			lr.rcSet = true
		} else {
			lr.notes = append(lr.notes, "logs/"+ri.Label+".rc: unparsable value")
		}
	}
	if lr.exitCode == nil && lr.rcSet {
		lr.exitCode = lr.rcExit
		lr.exitCodeSource = "logs/" + ri.Label + ".rc"
	}

	scanners := []struct {
		base string
		fn   func(path string) (int, error)
	}{
		{"samples.csv", func(path string) (int, error) {
			rows, n, err := scanSamplesFile(path)
			lr.samples = rows
			return n, err
		}},
		{"loop.csv", func(path string) (int, error) {
			rows, n, err := scanLoopFile(path)
			lr.loop = rows
			return n, err
		}},
		{"sql.csv", func(path string) (int, error) {
			rows, n, err := scanSQLFile(path)
			lr.sql = rows
			return n, err
		}},
		{"publish.csv", func(path string) (int, error) {
			rows, n, err := scanPublishFile(path)
			lr.publish = rows
			return n, err
		}},
	}
	for _, sc := range scanners {
		path, ok := probeFile(ri.Dir, sc.base)
		if !ok {
			continue
		}
		lr.present[sc.base] = path
		n, err := sc.fn(path)
		lr.parseErrs[sc.base] = n
		if err != nil {
			lr.notes = append(lr.notes, sc.base+": "+err.Error())
		}
	}
	return lr
}

// relPath returns dir relative to root when possible (keeps summaries
// portable); otherwise the original path.
func relPath(root, dir string) string {
	if rel, err := filepath.Rel(root, dir); err == nil {
		return rel
	}
	return dir
}
