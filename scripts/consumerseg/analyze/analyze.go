// analyze.go turns one run directory into the per-run summary (summary.json):
// top-level timing, detector latencies, segment statistics, the tail-window
// clipping, counting/loss reconciliation, the decile curves, partition/offset
// rollups, the publish-cycle rollup and the per-run checks. A defect never
// aborts the analyzer: it becomes a checks detail line ("<check>: ...") that
// the aggregate pass surfaces in the anomaly list.
package main

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

// ------------------------------------------------------------------- shapes

// runSummary is one run's summary.json (also embedded in summary_all.json).
type runSummary struct {
	Label          string `json:"label"`
	Group          string `json:"group"`
	Collection     string `json:"collection"`
	Dir            string `json:"dir"`
	Mode           string `json:"mode,omitempty"`
	N              *int64 `json:"n,omitempty"`
	NSource        string `json:"n_source,omitempty"`
	ExitCode       *int   `json:"exit_code,omitempty"`
	ExitCodeSource string `json:"exit_code_source,omitempty"`
	SegEnabled     *bool  `json:"seg_enabled,omitempty"`

	TimingSource         string   `json:"timing_source,omitempty"`
	PublishS             *float64 `json:"publish_s"`
	TailS                *float64 `json:"tail_s"`
	TotalS               *float64 `json:"total_s"`
	E2ERate              *float64 `json:"e2e_rate"`
	PublishRate          *float64 `json:"publish_rate"`
	AppliedAtPublishDone *int64   `json:"applied_at_publish_done"`

	Latency    latencyBlock    `json:"latency_us"`
	Anchors    anchorsView     `json:"anchors"`
	Segments   segmentsBlock   `json:"segments"`
	Tail       tailBlock       `json:"tail"`
	Counts     countsBlock     `json:"counts"`
	Curve      curveBlock      `json:"curve"`
	Partitions partitionsBlock `json:"partitions"`
	Publish    publishBlock    `json:"publish"`
	Pool       *poolBlock      `json:"pool_final_samples,omitempty"`

	Checks       checksBlock `json:"checks"`
	NotCollected []string    `json:"not_collected"`
}

// latencyBlock carries the four detector latencies in microseconds.
type latencyBlock struct {
	PublishObserveLatencyUS *int64 `json:"publish_observe_latency_us"`
	PublishDoneLatencyUS    *int64 `json:"publish_done_latency_us"`
	ConsumeConfirmLatencyUS *int64 `json:"consume_confirm_latency_us"`
	AppliedCBLatencyUS      *int64 `json:"applied_cb_latency_us"`
}

// anchorsView echoes the anchors.json values the summary is built from.
type anchorsView struct {
	DrainStartUS *int64              `json:"drain_start_us,omitempty"`
	Publish      *publishAnchorsView `json:"publish,omitempty"`
	Consume      *consumeAnchorsView `json:"consume,omitempty"`
	Counts       *countsAnchorsView  `json:"counts,omitempty"`
	LagFinal     map[string]int64    `json:"lag_final,omitempty"`
	SegEnabled   *bool               `json:"seg_enabled,omitempty"`
	Missing      []string            `json:"missing,omitempty"`
}

type publishAnchorsView struct {
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

type consumeAnchorsView struct {
	PollConfirmUS     *int64   `json:"poll_confirm_us"`
	ConsumerStoppedUS *int64   `json:"consumer_stopped_us"`
	AppliedAtConfirm  *int64   `json:"applied_at_confirm"`
	LastCommitEndUS   *int64   `json:"last_commit_end_us"`
	LastAppliedCBUS   *int64   `json:"last_applied_cb_us"`
	NotCollected      []string `json:"not_collected,omitempty"`
}

type countsAnchorsView struct {
	ObservedProcess     int64 `json:"observed_process"`
	ObservedApplied     int64 `json:"observed_applied"`
	ObservedDuplicate   int64 `json:"observed_duplicate"`
	ObservedVersionSkip int64 `json:"observed_version_skip"`
	ObservedQuarantined int64 `json:"observed_quarantined"`
	ObservedErrors      int64 `json:"observed_errors"`
	SQLSpans            int64 `json:"sql_spans"`
	SQLUnattributed     int64 `json:"sql_unattributed"`
	AppliedCB           int64 `json:"applied_cb"`
}

// segmentsBlock is the per-kind duration statistics plus the batch-size view.
type segmentsBlock struct {
	Collected        bool             `json:"collected"`
	Loop             map[string]*dist `json:"loop_kinds_us"`
	SQL              map[string]*dist `json:"sql_kinds_us"`
	TxTotal          *dist            `json:"tx_total_us"`
	TxEvents         int              `json:"tx_events"`
	TxSkipped        int              `json:"tx_skipped"`
	Poll             *pollBlock       `json:"poll,omitempty"`
	LoopRows         int              `json:"loop_rows"`
	SQLRows          int              `json:"sql_rows"`
	LoopParseErrors  int              `json:"loop_parse_errors"`
	SQLParseErrors   int              `json:"sql_parse_errors"`
	UnknownLoopRows  int              `json:"unknown_loop_rows"`
	SQLUnattributed  int              `json:"sql_unattributed"`
	SQLRowsNoProcess int              `json:"sql_rows_without_process"`
	Nesting          nestingBlock     `json:"nesting"`
}

// pollBlock describes the poll batch sizes.
type pollBlock struct {
	Count            int           `json:"count"`
	Empty            int           `json:"empty"`
	RecordsTotal     int64         `json:"records_total"`
	RecordsMin       *int64        `json:"records_min,omitempty"`
	RecordsMax       *int64        `json:"records_max,omitempty"`
	RecordsMean      *float64      `json:"records_mean,omitempty"`
	RecordsHistogram []histoBucket `json:"records_histogram"`
}

// histoBucket is one power-of-two records bucket.
type histoBucket struct {
	Range string `json:"range"`
	Count int    `json:"count"`
}

// nestingBlock reports the per-event nesting obligations (violations never
// fail the analyzer).
type nestingBlock struct {
	EventsChecked int      `json:"events_checked"`
	SumViolations int      `json:"sum_violations"`
	TxViolations  int      `json:"tx_violations"`
	Examples      []string `json:"examples,omitempty"`
}

// tailBlock is the tail-window clipping of the top-level loop spans.
type tailBlock struct {
	Collected         bool                 `json:"collected"`
	WindowStartUS     *int64               `json:"window_start_us,omitempty"`
	WindowEndUS       *int64               `json:"window_end_us,omitempty"`
	TailUS            *int64               `json:"tail_us,omitempty"`
	ByKind            map[string]*kindClip `json:"by_kind,omitempty"`
	TopSpans          int                  `json:"top_spans"`
	FullSpans         int                  `json:"full_spans"`
	PartialSpans      int                  `json:"partial_spans"`
	OutsideSpans      int                  `json:"outside_spans"`
	CoverageUS        int64                `json:"coverage_us"`
	ResidualUS        *int64               `json:"residual_us,omitempty"`
	ResidualPct       *float64             `json:"residual_pct,omitempty"`
	RawFullSpansUS    int64                `json:"raw_full_spans_us"`
	OverlapPairs      int                  `json:"overlap_pairs"`
	OverlapSpans      int                  `json:"overlap_spans"`
	NegativeDurations int                  `json:"negative_durations"`
	ZeroDurations     int                  `json:"zero_durations"`
	PreEpochSpans     int                  `json:"pre_epoch_spans"`
	RawFullVsTail     string               `json:"raw_full_vs_tail,omitempty"`
}

// kindClip is the per-kind clipping tally.
type kindClip struct {
	Spans      int   `json:"spans"`
	Full       int   `json:"full"`
	Partial    int   `json:"partial"`
	Outside    int   `json:"outside"`
	CoverageUS int64 `json:"coverage_us"`
	RawFullUS  int64 `json:"raw_full_us"`
}

// countsBlock reconciles the anchor counters, the loop-derived counters, the
// report counters and the drop counters.
type countsBlock struct {
	Anchors        *countsAnchorsView `json:"anchors,omitempty"`
	Loop           *loopCountsView    `json:"loop,omitempty"`
	Report         *reportCountsView  `json:"report,omitempty"`
	Drops          *dropsView         `json:"drops,omitempty"`
	IdentityOK     *bool              `json:"identity_ok,omitempty"`
	AppliedEqualsN *bool              `json:"applied_equals_n,omitempty"`
}

type loopCountsView struct {
	ProcessSpans          *int `json:"process_spans"`
	AppliedCBRows         *int `json:"applied_cb_rows"`
	MarkSpans             *int `json:"mark_spans"`
	PollSpans             *int `json:"poll_spans"`
	ErrorOutcomes         *int `json:"error_outcomes"`
	SQLRows               *int `json:"sql_rows"`
	SQLUnattributed       *int `json:"sql_unattributed"`
	SQLRowsWithoutProcess *int `json:"sql_rows_without_process"`
}

type reportCountsView struct {
	ConsumerApplied *int64 `json:"consumer_applied"`
	ProcessObserved *int64 `json:"process_observed"`
	SQLSpanCount    *int64 `json:"sql_span_count"`
}

type dropsView struct {
	LoopUnpaired   *int64 `json:"loop_unpaired,omitempty"`
	SamplesDropped *int64 `json:"samples_dropped,omitempty"`
	Source         string `json:"source,omitempty"`
}

// curveBlock holds the decile times of the three monotone counters.
type curveBlock struct {
	Collected bool          `json:"collected"`
	Samples   int           `json:"samples"`
	Applied   []decilePoint `json:"applied"`
	Published []decilePoint `json:"published"`
	Progress  []decilePoint `json:"progress"`
}

// decilePoint is the first sample time at which a counter reached a decile.
// TUS is absolute (epoch-relative) microseconds; TMS is relative to
// drain_start and is nil when anchors.json is not collected.
type decilePoint struct {
	Pct int      `json:"pct"`
	TUS *int64   `json:"t_us,omitempty"`
	TMS *float64 `json:"t_ms,omitempty"`
}

// partitionsBlock is the process-span partition rollup plus the final lag.
type partitionsBlock struct {
	Collected      bool             `json:"collected"`
	ByPartition    []partitionStat  `json:"by_partition"`
	ProcessSpans   int              `json:"process_spans"`
	ErrorOutcomes  int              `json:"error_outcomes"`
	Outcomes       []outcomeCount   `json:"process_outcomes,omitempty"`
	LagFinal       map[string]int64 `json:"lag_final,omitempty"`
	LagFinalSource string           `json:"lag_final_source,omitempty"`
}

type partitionStat struct {
	Partition int   `json:"partition"`
	Count     int   `json:"count"`
	MaxOffset int64 `json:"max_offset"`
	Errors    int   `json:"errors"`
}

type outcomeCount struct {
	Outcome string `json:"outcome"`
	Count   int    `json:"count"`
}

// publishBlock is the publish.csv rollup. The file carries one row per
// productive cycle plus exactly one terminal boundary row (pending_seen=0 and
// no publish outcome), which is why Rows = ProductiveCycles + BoundaryRows.
type publishBlock struct {
	Collected           bool   `json:"collected"`
	Rows                int    `json:"rows"`
	ProductiveCycles    int    `json:"productive_cycles"`
	BoundaryRows        int    `json:"boundary_rows"`
	Claimed             int64  `json:"claimed"`
	Acked               int64  `json:"acked"`
	Released            int64  `json:"released"`
	Blocked             int64  `json:"blocked"`
	LastPendingSeen     *int64 `json:"last_pending_seen,omitempty"`
	LastCycleTerminal   bool   `json:"last_cycle_terminal"`
	InvariantViolations int    `json:"invariant_violations"`
	UnclassifiedRows    int    `json:"unclassified_rows"`
	ParseErrors         int    `json:"parse_errors"`
}

// poolBlock is the final pool counters taken from the last monitor sample.
type poolBlock struct {
	Samples                int     `json:"samples"`
	AcquireCount           int64   `json:"acquire_count"`
	EmptyAcquireCount      int64   `json:"empty_acquire_count"`
	AcquireDurationSeconds float64 `json:"acquire_duration_seconds"`
	CanceledAcquireCount   int64   `json:"canceled_acquire_count"`
	TotalConns             int64   `json:"total_conns"`
	IdleConns              int64   `json:"idle_conns"`
}

// checksBlock is the frozen per-run check set.
type checksBlock struct {
	AnchorsComplete   bool     `json:"anchors_complete"`
	CountsOK          bool     `json:"counts_ok"`
	DropsOK           bool     `json:"drops_ok"`
	OverlapOK         bool     `json:"overlap_ok"`
	NestingOK         bool     `json:"nesting_ok"`
	ReportVsAnchorsOK bool     `json:"report_vs_anchors_ok"`
	FilesMissing      []string `json:"files_missing"`
	Details           []string `json:"details"`
}

// ------------------------------------------------------------ classificaton

// collectionKind describes how much of the v1 evidence protocol this run
// carries: "seg" (meta/anchors present), "pristine" (report + exit_code only)
// or "partial" (anything in between).
func (lr *loadedRun) collectionKind() string {
	if lr.pristine() {
		return "pristine"
	}
	if lr.meta != nil || lr.anchors != nil {
		return "seg"
	}
	return "partial"
}

// pristine recognizes the legacy run that predates the segmented collection:
// only report.json and exit_code are present.
func (lr *loadedRun) pristine() bool {
	if !lr.reportSet || lr.meta != nil || lr.anchors != nil {
		return false
	}
	for _, base := range csvBases {
		if _, ok := lr.present[base]; ok {
			return false
		}
	}
	return true
}

// group derives the comparison group: meta.mode first, the frozen label suffix
// ("_on"/"_off") second, the pristine shape third.
func (lr *loadedRun) group() string {
	if lr.meta != nil {
		switch lr.meta.Mode {
		case "on", "off":
			return lr.meta.Mode
		}
	}
	lower := strings.ToLower(lr.label)
	switch {
	case strings.HasSuffix(lower, "_on"):
		return "on"
	case strings.HasSuffix(lower, "_off"):
		return "off"
	case lr.pristine():
		return "pristine"
	}
	return "unknown"
}

// segEnabled resolves the collection switch: meta.json, then anchors.json,
// then report.json.
func (lr *loadedRun) segEnabled() *bool {
	if lr.meta != nil && lr.meta.SegEnabled != nil {
		return lr.meta.SegEnabled
	}
	if lr.anchors != nil && lr.anchors.SegEnabled != nil {
		return lr.anchors.SegEnabled
	}
	if lr.report != nil && lr.report.SegEnabled != nil {
		return lr.report.SegEnabled
	}
	return nil
}

// n resolves the seeded event count: meta.json, then report.json.
func (lr *loadedRun) n() (*int64, string) {
	if lr.meta != nil && lr.meta.N != nil {
		return lr.meta.N, "meta"
	}
	if lr.report != nil && lr.report.N != nil {
		return lr.report.N, "report"
	}
	return nil, ""
}

// drops resolves the drop counters: meta.json, then report.seg_drops.
func (lr *loadedRun) drops() (*dropsDoc, string) {
	if lr.meta != nil && lr.meta.Drops != nil {
		return lr.meta.Drops, "meta"
	}
	if lr.report != nil && lr.report.SegDrops != nil {
		return lr.report.SegDrops, "report"
	}
	return nil, ""
}

// ------------------------------------------------------------------ analysis

// analyzeRun builds the run summary; it returns nil only when the directory
// carries neither report.json nor anchors.json (nothing to analyze).
func analyzeRun(root string, ri runInput) *runSummary {
	lr := loadRun(root, ri)
	if lr.anchors == nil && !lr.reportSet && lr.meta == nil {
		return nil
	}
	rs := &runSummary{
		Label:      ri.Label,
		Dir:        lr.relDir,
		Collection: lr.collectionKind(),
		Group:      lr.group(),
	}
	if lr.meta != nil {
		rs.Mode = lr.meta.Mode
	}
	rs.ExitCode, rs.ExitCodeSource = lr.exitCode, lr.exitCodeSource
	rs.SegEnabled = lr.segEnabled()
	rs.N, rs.NSource = lr.n()

	publishS, tailS, totalS, timingSource := deriveTiming(lr)
	rs.PublishS, rs.TailS, rs.TotalS = publishS, tailS, totalS
	rs.TimingSource = timingSource
	if rs.N != nil && totalS != nil && *totalS > 0 {
		rs.E2ERate = new(round6(float64(*rs.N) / *totalS))
	}
	if rs.N != nil && publishS != nil && *publishS > 0 {
		rs.PublishRate = new(round6(float64(*rs.N) / *publishS))
	}

	rs.Anchors = anchorsViewOf(lr.anchors)
	rs.Latency = deriveLatency(lr.anchors)
	if lr.anchors != nil && lr.anchors.Publish != nil && lr.anchors.Publish.AppliedAtPublishDone != nil {
		rs.AppliedAtPublishDone = lr.anchors.Publish.AppliedAtPublishDone
	} else if lr.report != nil {
		rs.AppliedAtPublishDone = lr.report.AppliedAtPublishDone
	}

	rs.Segments = buildSegments(lr)
	rs.Tail = buildTail(lr)
	rs.Counts = buildCounts(lr, rs)
	rs.Curve = buildCurve(lr)
	rs.Partitions = buildPartitions(lr)
	rs.Publish = buildPublish(lr)
	rs.Pool = buildPool(lr)
	rs.Checks = buildChecks(lr, rs)
	rs.NotCollected = notCollected(lr)
	return rs
}

// usToSeconds converts microsecond epochs/deltas to (rounded) seconds.
func usToSeconds(us int64) float64 { return round6(float64(us) / 1e6) }

// deriveTiming computes publish_s / tail_s / total_s, preferring the anchors
// (monotonic) over the report (the pristine witness); the source records which
// side was used per field ("anchors", "report", "mixed").
func deriveTiming(lr *loadedRun) (publishS, tailS, totalS *float64, source string) {
	a, r := lr.anchors, lr.report
	var p *publishAnchorsDoc
	if a != nil {
		p = a.Publish
	}
	anchorsPublish := a != nil && p != nil && p.PublishDoneUS != nil && a.DrainStartUS != nil
	anchorsTail := p != nil && p.PublishDoneUS != nil && a != nil && a.Consume != nil && a.Consume.PollConfirmUS != nil
	anchorsTotal := a != nil && a.DrainStartUS != nil && a.Consume != nil && a.Consume.PollConfirmUS != nil
	if anchorsPublish {
		publishS = new(usToSeconds(*p.PublishDoneUS - *a.DrainStartUS))
	}
	if anchorsTail {
		tailS = new(usToSeconds(*a.Consume.PollConfirmUS - *p.PublishDoneUS))
	}
	if anchorsTotal {
		totalS = new(usToSeconds(*a.Consume.PollConfirmUS - *a.DrainStartUS))
	}
	fromReport := 0
	if r != nil {
		if publishS == nil && r.PublishSeconds != nil {
			publishS = new(round6(*r.PublishSeconds))
			fromReport++
		}
		if tailS == nil && r.ConsumerTailSeconds != nil {
			tailS = new(round6(*r.ConsumerTailSeconds))
			fromReport++
		}
		if totalS == nil && r.TotalDrainSeconds != nil {
			totalS = new(round6(*r.TotalDrainSeconds))
			fromReport++
		}
	}
	fromAnchors := 0
	if anchorsPublish {
		fromAnchors++
	}
	if anchorsTail {
		fromAnchors++
	}
	if anchorsTotal {
		fromAnchors++
	}
	switch {
	case publishS == nil && tailS == nil && totalS == nil:
		source = ""
	case fromAnchors == 3:
		source = "anchors"
	case fromReport == 3:
		source = "report"
	default:
		source = "mixed"
	}
	return publishS, tailS, totalS, source
}

// deriveLatency computes the four detector latencies in microseconds. Every
// component must carry a real monotonic offset (usOffset): a 0 sentinel means
// the observation was never collected (OFF arm), and a latency derived from it
// would be an epoch-scale artifact — such a latency stays nil and renders as
// "-"/not collected.
func deriveLatency(a *anchorsDoc) latencyBlock {
	var lb latencyBlock
	if a == nil || a.Publish == nil || a.Consume == nil {
		return lb
	}
	p, c := a.Publish, a.Consume
	if usOffset(p.LastCycleEndUS) && usOffset(p.PendingZeroObservedUS) {
		lb.PublishObserveLatencyUS = new(*p.PendingZeroObservedUS - *p.LastCycleEndUS)
	}
	if usOffset(p.LastCycleEndUS) && usOffset(p.PublishDoneUS) {
		lb.PublishDoneLatencyUS = new(*p.PublishDoneUS - *p.LastCycleEndUS)
	}
	if usOffset(c.LastAppliedCBUS) && usOffset(c.PollConfirmUS) {
		lb.ConsumeConfirmLatencyUS = new(*c.PollConfirmUS - *c.LastAppliedCBUS)
	}
	if usOffset(c.LastCommitEndUS) && usOffset(c.LastAppliedCBUS) {
		lb.AppliedCBLatencyUS = new(*c.LastAppliedCBUS - *c.LastCommitEndUS)
	}
	return lb
}

// anchorsViewOf echoes the anchors document (nil stays empty and the missing
// keys are listed by the checks).
func anchorsViewOf(a *anchorsDoc) anchorsView {
	if a == nil {
		return anchorsView{Missing: []string{"anchors.json"}}
	}
	v := anchorsView{
		DrainStartUS: a.DrainStartUS,
		LagFinal:     a.LagFinal,
		SegEnabled:   a.SegEnabled,
		Missing:      a.missingKeys(),
	}
	if a.Publish != nil {
		p := a.Publish
		v.Publish = &publishAnchorsView{
			FirstCycleStartUS:     p.FirstCycleStartUS,
			LastCycleEndUS:        p.LastCycleEndUS,
			PendingZeroObservedUS: p.PendingZeroObservedUS,
			PublishDoneUS:         p.PublishDoneUS,
			Cycles:                p.Cycles,
			Claimed:               p.Claimed,
			Acked:                 p.Acked,
			Released:              p.Released,
			Blocked:               p.Blocked,
			AppliedAtPublishDone:  p.AppliedAtPublishDone,
		}
	}
	if a.Consume != nil {
		c := a.Consume
		v.Consume = &consumeAnchorsView{
			PollConfirmUS:     c.PollConfirmUS,
			ConsumerStoppedUS: c.ConsumerStoppedUS,
			AppliedAtConfirm:  c.AppliedAtConfirm,
			LastCommitEndUS:   c.LastCommitEndUS,
			LastAppliedCBUS:   c.LastAppliedCBUS,
		}
		// 0 is the frozen "not collected" sentinel (OFF arm: no statement
		// tracer, no applied-callback rows): render it as not collected so no
		// consumer derives an epoch-scale delta from a "0 offset".
		if !usOffset(v.Consume.LastCommitEndUS) {
			if v.Consume.LastCommitEndUS != nil {
				v.Consume.NotCollected = append(v.Consume.NotCollected, "last_commit_end_us")
			}
			v.Consume.LastCommitEndUS = nil
		}
		if !usOffset(v.Consume.LastAppliedCBUS) {
			if v.Consume.LastAppliedCBUS != nil {
				v.Consume.NotCollected = append(v.Consume.NotCollected, "last_applied_cb_us")
			}
			v.Consume.LastAppliedCBUS = nil
		}
	}
	if a.Counts != nil {
		c := a.Counts
		v.Counts = &countsAnchorsView{
			ObservedProcess:     i64(c.ObservedProcess),
			ObservedApplied:     i64(c.ObservedApplied),
			ObservedDuplicate:   i64(c.ObservedDuplicate),
			ObservedVersionSkip: i64(c.ObservedVersionSkip),
			ObservedQuarantined: i64(c.ObservedQuarantined),
			ObservedErrors:      i64(c.ObservedErrors),
			SQLSpans:            i64(c.SQLSpans),
			SQLUnattributed:     i64(c.SQLUnattributed),
			AppliedCB:           i64(c.AppliedCB),
		}
	}
	return v
}

// buildSegments computes the per-kind statistics, the poll batch sizes and the
// tx/nesting obligations.
func buildSegments(lr *loadedRun) segmentsBlock {
	sb := segmentsBlock{
		Collected:       lr.present["loop.csv"] != "" || lr.present["sql.csv"] != "",
		Loop:            map[string]*dist{},
		SQL:             map[string]*dist{},
		LoopRows:        len(lr.loop),
		SQLRows:         len(lr.sql),
		LoopParseErrors: lr.parseErrs["loop.csv"],
		SQLParseErrors:  lr.parseErrs["sql.csv"],
	}
	loopDurs := map[string][]int64{}
	for _, r := range lr.loop {
		switch r.Kind {
		case "poll", "process", "mark", "rebalance", "lag":
			loopDurs[r.Kind] = append(loopDurs[r.Kind], r.Dur)
		case loopKindAppliedCB:
		default:
			sb.UnknownLoopRows++
		}
	}
	for _, kind := range loopKinds {
		sb.Loop[kind] = distFromInt64(loopDurs[kind])
	}

	sqlDurs := map[string][]int64{}
	for _, r := range lr.sql {
		kind := normalizeSQLKind(r.Kind)
		sqlDurs[kind] = append(sqlDurs[kind], r.Dur)
	}
	for _, kind := range sqlKinds {
		sb.SQL[kind] = distFromInt64(sqlDurs[kind])
	}

	if sb.Collected {
		sb.Poll = buildPoll(lr.loop)
	}
	buildTxAndNesting(lr, &sb)
	return sb
}

// normalizeSQLKind folds an unknown statement kind into "other".
func normalizeSQLKind(kind string) string {
	for _, known := range sqlKinds {
		if kind == known {
			return kind
		}
	}
	return "other"
}

// pollHistogramBuckets are the frozen power-of-two records buckets.
var pollHistogramBuckets = []struct {
	lo, hi int64
	label  string
}{
	{0, 0, "0"},
	{1, 1, "1"},
	{2, 3, "2-3"},
	{4, 7, "4-7"},
	{8, 15, "8-15"},
	{16, 31, "16-31"},
	{32, 63, "32-63"},
	{64, 127, "64-127"},
	{128, 255, "128-255"},
	{256, 511, "256-511"},
	{512, -1, ">=512"},
}

// buildPoll summarizes the poll spans (count, empty polls, records
// distribution and histogram).
func buildPoll(rows []loopRow) *pollBlock {
	pb := &pollBlock{RecordsHistogram: make([]histoBucket, len(pollHistogramBuckets))}
	for i, b := range pollHistogramBuckets {
		pb.RecordsHistogram[i].Range = b.label
	}
	var records []int64
	for _, r := range rows {
		if r.Kind != "poll" {
			continue
		}
		pb.Count++
		rec := int64(0)
		if r.HasI1 {
			rec = r.I1
		}
		records = append(records, rec)
		if rec == 0 {
			pb.Empty++
		}
	}
	if len(records) == 0 {
		return pb
	}
	minV, maxV := records[0], records[0]
	var sum int64
	for _, v := range records {
		if v < minV {
			minV = v
		}
		if v > maxV {
			maxV = v
		}
		sum += v
	}
	pb.RecordsTotal = sum
	pb.RecordsMin = new(minV)
	pb.RecordsMax = new(maxV)
	pb.RecordsMean = new(round6(float64(sum) / float64(len(records))))
	for _, v := range records {
		for i, b := range pollHistogramBuckets {
			if v >= b.lo && (b.hi < 0 || v <= b.hi) {
				pb.RecordsHistogram[i].Count++
				break
			}
		}
	}
	return pb
}

// sqlEventAgg accumulates the per-event SQL view used for tx_total and the
// nesting obligations.
type sqlEventAgg struct {
	rows       []sqlRow
	sum        int64
	processDur int64
	hasProcess bool
	tx         *int64
}

// buildTxAndNesting groups the SQL rows by event sequence, computes the
// tx_total spans (commit end − begin start) and evaluates the two nesting
// obligations with the documented microsecond quantization allowance:
//
//	Σ sql dur ≤ process dur + rows×1µs   (per-row truncation allowance)
//	tx_total  ≤ process dur + 2µs        (two boundary rows)
func buildTxAndNesting(lr *loadedRun, sb *segmentsBlock) {
	events := map[int64]*sqlEventAgg{}
	seqs := make([]int64, 0, len(lr.sql))
	for _, r := range lr.sql {
		if r.Seq < 0 {
			sb.SQLUnattributed++
			continue
		}
		ev := events[r.Seq]
		if ev == nil {
			ev = &sqlEventAgg{}
			events[r.Seq] = ev
			seqs = append(seqs, r.Seq)
		}
		ev.rows = append(ev.rows, r)
		ev.sum += r.Dur
	}
	loopPresent := lr.present["loop.csv"] != ""
	for _, r := range lr.loop {
		if r.Kind != loopKindProcess || !r.HasSeq {
			continue
		}
		if ev := events[r.Seq]; ev != nil && !ev.hasProcess {
			ev.processDur = r.Dur
			ev.hasProcess = true
		}
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })

	var txDurs []int64
	for _, seq := range seqs {
		ev := events[seq]
		sort.SliceStable(ev.rows, func(i, j int) bool { return ev.rows[i].T < ev.rows[j].T })
		begin, commit := -1, -1
		for i, r := range ev.rows {
			if r.Kind == "begin" && begin < 0 {
				begin = i
			}
			if r.Kind == "commit" {
				commit = i
			}
		}
		if begin >= 0 && commit >= 0 {
			tx := (ev.rows[commit].T + ev.rows[commit].Dur) - ev.rows[begin].T
			ev.tx = new(tx)
			txDurs = append(txDurs, tx)
		}
	}
	sb.TxEvents = len(txDurs)
	sb.TxSkipped = len(seqs) - len(txDurs)
	sb.TxTotal = distFromInt64(txDurs)

	if !loopPresent || len(lr.sql) == 0 {
		return
	}
	for _, seq := range seqs {
		ev := events[seq]
		if !ev.hasProcess {
			sb.SQLRowsNoProcess += len(ev.rows)
			continue
		}
		sb.Nesting.EventsChecked++
		eps := int64(len(ev.rows))
		if ev.sum > ev.processDur+eps {
			sb.Nesting.SumViolations++
			if len(sb.Nesting.Examples) < 5 {
				sb.Nesting.Examples = append(sb.Nesting.Examples, fmt.Sprintf(
					"seq=%d: Σsql=%dus > process=%dus (+%dus eps)", seq, ev.sum, ev.processDur, eps))
			}
		}
		if ev.tx != nil && *ev.tx > ev.processDur+2 {
			sb.Nesting.TxViolations++
			if len(sb.Nesting.Examples) < 5 {
				sb.Nesting.Examples = append(sb.Nesting.Examples, fmt.Sprintf(
					"seq=%d: tx_total=%dus > process=%dus (+2us eps)", seq, *ev.tx, ev.processDur))
			}
		}
	}
}

// rawFullVsTailWarning is attached to every clipping block: the raw full-span
// sum is an audit number, never a tail decomposition.
const rawFullVsTailWarning = "raw full spans (完整批次) 的总时长不得与 tail 直接比较：本表已按窗口 " +
	"[publish_done_us, poll_confirm_us] 裁剪 —— full 全额计入、partial 仅计窗内交叠、outside 不计入；" +
	"residual = tail − Σ窗内覆盖。raw_full_spans_us 仅供核对，不作结论。"

// buildTail clips the top-level loop spans against the tail window.
func buildTail(lr *loadedRun) tailBlock {
	a := lr.anchors
	if lr.present["loop.csv"] == "" || a == nil || a.Publish == nil || a.Publish.PublishDoneUS == nil ||
		a.Consume == nil || a.Consume.PollConfirmUS == nil {
		return tailBlock{}
	}
	spans := make([]span, 0, len(lr.loop))
	for _, r := range lr.loop {
		switch r.Kind {
		case "poll", "process", "mark", "rebalance", "lag":
			spans = append(spans, span{Kind: r.Kind, Start: r.T, Dur: r.Dur})
		}
	}
	tb := clipTail(spans, *a.Publish.PublishDoneUS, *a.Consume.PollConfirmUS)
	tb.TopSpans = len(spans)
	tb.OverlapPairs, tb.OverlapSpans = overlapPairs(spans)
	tb.RawFullVsTail = rawFullVsTailWarning
	return tb
}

// buildCounts reconciles the four counter sources.
func buildCounts(lr *loadedRun, rs *runSummary) countsBlock {
	cb := countsBlock{}
	if a := lr.anchors; a != nil && a.Counts != nil {
		c := a.Counts
		cb.Anchors = &countsAnchorsView{
			ObservedProcess:     i64(c.ObservedProcess),
			ObservedApplied:     i64(c.ObservedApplied),
			ObservedDuplicate:   i64(c.ObservedDuplicate),
			ObservedVersionSkip: i64(c.ObservedVersionSkip),
			ObservedQuarantined: i64(c.ObservedQuarantined),
			ObservedErrors:      i64(c.ObservedErrors),
			SQLSpans:            i64(c.SQLSpans),
			SQLUnattributed:     i64(c.SQLUnattributed),
			AppliedCB:           i64(c.AppliedCB),
		}
		if c.ObservedProcess != nil && c.ObservedApplied != nil && c.ObservedDuplicate != nil &&
			c.ObservedVersionSkip != nil && c.ObservedQuarantined != nil && c.ObservedErrors != nil {
			ok := *c.ObservedProcess == *c.ObservedApplied+*c.ObservedDuplicate+*c.ObservedVersionSkip+
				*c.ObservedQuarantined+*c.ObservedErrors
			cb.IdentityOK = new(ok)
		}
		if c.ObservedApplied != nil && rs.N != nil {
			ok := *c.ObservedApplied == *rs.N
			cb.AppliedEqualsN = new(ok)
		}
	}
	if lr.present["loop.csv"] != "" || lr.present["sql.csv"] != "" {
		lv := &loopCountsView{}
		var process, appliedCB, marks, polls, errors int
		for _, r := range lr.loop {
			switch r.Kind {
			case loopKindProcess:
				process++
				if r.S1 == "" {
					errors++
				}
			case loopKindAppliedCB:
				appliedCB++
			case "mark":
				marks++
			case "poll":
				polls++
			}
		}
		lv.ProcessSpans = new(process)
		lv.AppliedCBRows = new(appliedCB)
		lv.MarkSpans = new(marks)
		lv.PollSpans = new(polls)
		lv.ErrorOutcomes = new(errors)
		lv.SQLRows = new(len(lr.sql))
		lv.SQLUnattributed = new(rs.Segments.SQLUnattributed)
		lv.SQLRowsWithoutProcess = new(rs.Segments.SQLRowsNoProcess)
		cb.Loop = lv
	}
	if r := lr.report; r != nil {
		cb.Report = &reportCountsView{
			ConsumerApplied: r.ConsumerApplied,
			ProcessObserved: r.ProcessObserved,
			SQLSpanCount:    r.SQLSpanCount,
		}
	}
	if d, source := lr.drops(); d != nil {
		cb.Drops = &dropsView{LoopUnpaired: d.LoopUnpaired, SamplesDropped: d.SamplesDropped, Source: source}
	} else if lr.meta != nil || lr.reportSet {
		cb.Drops = &dropsView{Source: "absent"}
	}
	return cb
}

// buildCurve computes the applied/published/progress decile times.
func buildCurve(lr *loadedRun) curveBlock {
	cb := curveBlock{Collected: lr.present["samples.csv"] != "", Samples: len(lr.samples)}
	var base *int64
	if lr.anchors != nil {
		base = lr.anchors.DrainStartUS
	}
	cb.Applied = decileSeries(lr.samples, base, func(s sampleRow) int64 { return s.Applied })
	cb.Published = decileSeries(lr.samples, base, func(s sampleRow) int64 { return s.Published })
	cb.Progress = decileSeries(lr.samples, base, func(s sampleRow) int64 { return s.Progress })
	return cb
}

// decileSeries returns the first sample reaching each decile of the series
// maximum (10%..100%). A series whose maximum is 0 yields ten nil points.
func decileSeries(rows []sampleRow, base *int64, pick func(sampleRow) int64) []decilePoint {
	final := int64(0)
	for _, r := range rows {
		if v := pick(r); v > final {
			final = v
		}
	}
	points := make([]decilePoint, 0, 10)
	for pct := 10; pct <= 100; pct += 10 {
		p := decilePoint{Pct: pct}
		if final > 0 {
			for _, r := range rows {
				if pick(r)*100 >= final*int64(pct) {
					t := r.TUS
					p.TUS = new(t)
					if base != nil {
						p.TMS = new(round6(float64(t-*base) / 1000))
					}
					break
				}
			}
		}
		points = append(points, p)
	}
	return points
}

// buildPartitions rolls the process spans up by partition and echoes the final
// lag (anchors preferred, report fallback).
func buildPartitions(lr *loadedRun) partitionsBlock {
	pb := partitionsBlock{Collected: lr.present["loop.csv"] != ""}
	type agg struct {
		stat partitionStat
	}
	byPart := map[int64]*agg{}
	outcomes := map[string]int{}
	for _, r := range lr.loop {
		if r.Kind != loopKindProcess {
			continue
		}
		part := int64(-1)
		if r.HasI1 {
			part = r.I1
		}
		a := byPart[part]
		if a == nil {
			a = &agg{stat: partitionStat{Partition: int(part)}}
			byPart[part] = a
		}
		a.stat.Count++
		pb.ProcessSpans++
		if r.HasI2 && r.I2 > a.stat.MaxOffset {
			a.stat.MaxOffset = r.I2
		}
		if r.S1 == "" {
			a.stat.Errors++
			pb.ErrorOutcomes++
			continue
		}
		outcomes[r.S1]++
	}
	keys := make([]int64, 0, len(byPart))
	for k := range byPart {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	for _, k := range keys {
		pb.ByPartition = append(pb.ByPartition, byPart[k].stat)
	}
	names := make([]string, 0, len(outcomes))
	for name := range outcomes {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool {
		if outcomes[names[i]] != outcomes[names[j]] {
			return outcomes[names[i]] > outcomes[names[j]]
		}
		return names[i] < names[j]
	})
	for _, name := range names {
		pb.Outcomes = append(pb.Outcomes, outcomeCount{Outcome: name, Count: outcomes[name]})
	}
	if lr.anchors != nil && lr.anchors.LagFinal != nil {
		pb.LagFinal = lr.anchors.LagFinal
		pb.LagFinalSource = "anchors"
	} else if lr.report != nil && lr.report.LagFinal != nil {
		pb.LagFinal = lr.report.LagFinal
		pb.LagFinalSource = "report"
	}
	return pb
}

// buildPublish rolls up the publish cycles: one productive cycle per row plus
// the terminal boundary row (pending_seen=0, no publish outcome).
func buildPublish(lr *loadedRun) publishBlock {
	pb := publishBlock{
		Collected:   lr.present["publish.csv"] != "",
		Rows:        len(lr.publish),
		ParseErrors: lr.parseErrs["publish.csv"],
	}
	for _, r := range lr.publish {
		pb.Claimed += r.Claimed
		pb.Acked += r.Acked
		pb.Released += r.Released
		pb.Blocked += r.Blocked
		if r.Acked+r.Released+r.Blocked != r.Claimed {
			pb.InvariantViolations++
		}
		publishOutcome := r.TPublishUS != 0 || r.PublishDurUS != 0 ||
			r.Claimed+r.Acked+r.Released+r.Blocked != 0
		switch {
		case publishOutcome:
			pb.ProductiveCycles++
		case r.PendingSeen == 0:
			pb.BoundaryRows++
		default:
			pb.UnclassifiedRows++
		}
	}
	if n := len(lr.publish); n > 0 {
		last := lr.publish[n-1]
		pb.LastPendingSeen = new(last.PendingSeen)
		pb.LastCycleTerminal = last.PendingSeen == 0 && last.TPublishUS == 0 && last.PublishDurUS == 0 &&
			last.Claimed+last.Acked+last.Released+last.Blocked == 0
	}
	return pb
}

// buildPool takes the final pool counters from the last monitor sample.
func buildPool(lr *loadedRun) *poolBlock {
	if len(lr.samples) == 0 {
		return nil
	}
	last := lr.samples[len(lr.samples)-1]
	return &poolBlock{
		Samples:                len(lr.samples),
		AcquireCount:           last.AcquireCount,
		EmptyAcquireCount:      last.EmptyAcquireCount,
		AcquireDurationSeconds: round6(float64(last.AcquireDurationUS) / 1e6),
		CanceledAcquireCount:   last.CanceledAcquireCount,
		TotalConns:             last.TotalConns,
		IdleConns:              last.IdleConns,
	}
}

// notCollected lists the v1 artifacts this run does not carry.
func notCollected(lr *loadedRun) []string {
	missing := []string{}
	labels := []struct{ file, label string }{
		{"meta.json", "meta.json（运行元数据）"},
		{"anchors.json", "anchors.json（单调基准/检测锚点）"},
		{"samples.csv", "samples.csv（200ms 采样曲线/连接池）"},
		{"loop.csv", "loop.csv（loop 分段）"},
		{"sql.csv", "sql.csv（SQL 分段）"},
		{"publish.csv", "publish.csv（发布周期）"},
	}
	for _, l := range labels {
		if _, ok := lr.present[l.file]; !ok {
			missing = append(missing, "未采集: "+l.label)
		}
	}
	return missing
}

// missingExpectedFiles lists what the run's own protocol expects but does not
// have: report.json + exit_code always; meta/anchors for any collected run;
// the four CSV artifacts only for a seg-enabled run (an OFF control carries
// no collection by design and its absent CSVs are marked "not collected",
// never missing).
func missingExpectedFiles(lr *loadedRun, rs *runSummary) []string {
	expected := []string{"report.json", "exit_code"}
	if lr.meta != nil || lr.anchors != nil {
		expected = append(expected, "meta.json", "anchors.json")
	}
	if rs.SegEnabled != nil && *rs.SegEnabled {
		expected = append(expected, "samples.csv", "loop.csv", "sql.csv", "publish.csv")
	}
	missing := []string{}
	for _, name := range expected {
		if _, ok := lr.present[name]; !ok {
			missing = append(missing, name)
		}
	}
	if _, a := lr.present["exit_code"]; !a && !lr.rcSet {
		missing = append(missing, "logs/<label>.rc")
	}
	return missing
}

// buildChecks evaluates the frozen per-run check set. Findings are detail
// lines prefixed with the check (or finding) name; informational lines are
// prefixed "note: ".
func buildChecks(lr *loadedRun, rs *runSummary) checksBlock {
	ck := checksBlock{
		CountsOK:          true,
		DropsOK:           true,
		OverlapOK:         true,
		NestingOK:         true,
		ReportVsAnchorsOK: true,
		FilesMissing:      missingExpectedFiles(lr, rs),
	}
	var findings, notes []string
	fail := func(name, format string, args ...any) {
		findings = append(findings, name+": "+fmt.Sprintf(format, args...))
	}
	note := func(format string, args ...any) {
		notes = append(notes, "note: "+fmt.Sprintf(format, args...))
	}

	// anchors_complete
	if lr.anchors == nil {
		if lr.pristine() {
			note("anchors_complete: anchors.json 未采集（pristine 运行按协议仅含 report.json + exit_code）")
		} else {
			fail("anchors_complete", "anchors.json 未采集")
		}
	} else if missing := lr.anchors.missingKeys(); len(missing) > 0 {
		fail("anchors_complete", "anchors.json 缺少字段: %s", strings.Join(missing, ", "))
	} else {
		ck.AnchorsComplete = true
	}

	// counts_ok
	checkCounts(lr, rs, fail, note, &ck)

	// drops_ok
	drops, dropsSource := lr.drops()
	if drops == nil {
		note("drops_ok: drops 未采集（未评估）")
	} else {
		if drops.LoopUnpaired != nil && *drops.LoopUnpaired != 0 {
			ck.DropsOK = false
			fail("drops_ok", "loop_unpaired=%d != 0 (source=%s)", *drops.LoopUnpaired, dropsSource)
		}
		if drops.SamplesDropped != nil && *drops.SamplesDropped > 0 {
			fail("drops_ok", "samples_dropped=%d（记录项；不判失败，但列为异常）", *drops.SamplesDropped)
		}
		if drops.LoopUnpaired == nil && drops.SamplesDropped == nil {
			note("drops_ok: loops/samples drop 计数缺失（source=%s）", dropsSource)
		}
	}
	if m := lr.meta; m != nil && m.Drops != nil && lr.report != nil && lr.report.SegDrops != nil {
		if !sameDropPtr(m.Drops.LoopUnpaired, lr.report.SegDrops.LoopUnpaired) ||
			!sameDropPtr(m.Drops.SamplesDropped, lr.report.SegDrops.SamplesDropped) {
			ck.DropsOK = false
			fail("drops_ok", "meta.drops 与 report.seg_drops 不一致")
		}
	}

	// overlap_ok
	if rs.Tail.Collected {
		if rs.Tail.OverlapPairs != 0 {
			ck.OverlapOK = false
			fail("overlap_ok", "顶层 span 两两交叠 %d 对（%d 个 span 起始于其他 span 内部）",
				rs.Tail.OverlapPairs, rs.Tail.OverlapSpans)
		}
		if rs.Tail.NegativeDurations != 0 {
			ck.OverlapOK = false
			fail("overlap_ok", "负数时长 span=%d", rs.Tail.NegativeDurations)
		}
		if rs.Tail.PreEpochSpans != 0 {
			ck.OverlapOK = false
			fail("overlap_ok", "epoch 之前的 t_us span=%d", rs.Tail.PreEpochSpans)
		}
	} else {
		note("overlap_ok: loop.csv 未采集（未评估）")
	}

	// nesting_ok
	if lr.present["loop.csv"] != "" && lr.present["sql.csv"] != "" {
		violations := rs.Segments.Nesting.SumViolations + rs.Segments.Nesting.TxViolations
		if violations != 0 {
			ck.NestingOK = false
			fail("nesting_ok", "嵌套义务违例: Σsql>process %d 例; tx_total>process %d 例",
				rs.Segments.Nesting.SumViolations, rs.Segments.Nesting.TxViolations)
			for _, ex := range rs.Segments.Nesting.Examples {
				fail("nesting_ok", "%s", ex)
			}
		}
		if rs.Segments.SQLRowsNoProcess != 0 {
			note("nesting_ok: %d 行 sql 无对应 process span（事件外/缺 span）", rs.Segments.SQLRowsNoProcess)
		}
	} else {
		note("nesting_ok: loop.csv/sql.csv 未采集（未评估）")
	}

	// report_vs_anchors_ok
	repFailures, repNotes := compareReportAnchors(lr, rs)
	if len(repFailures) > 0 {
		ck.ReportVsAnchorsOK = false
		findings = append(findings, repFailures...)
	}
	notes = append(notes, repNotes...)

	// exit code witnesses
	if lr.exitCode != nil && lr.rcSet && *lr.rcExit != *lr.exitCode {
		fail("exit_code", "exit_code=%d 与 logs/%s.rc=%d 不一致", *lr.exitCode, lr.label, *lr.rcExit)
	}
	if lr.exitCode != nil && *lr.exitCode != 0 {
		fail("exit_code", "go test 退出码 %d != 0（运行未成功完成；日志见 logs/%s.log[.gz]）", *lr.exitCode, lr.label)
	}

	// parse defects
	for _, base := range csvBases {
		if n := lr.parseErrs[base]; n > 0 {
			fail("parse", "%s 行解析失败 %d 行（已跳过）", base, n)
		}
	}
	for _, noteText := range lr.notes {
		fail("parse", "%s", noteText)
	}

	// tail residual
	if rs.Tail.Collected && rs.Tail.ResidualUS != nil && *rs.Tail.ResidualUS < 0 {
		fail("tail", "residual=%dus < 0：窗内覆盖超过 tail（窗口或 span 口径可疑）", *rs.Tail.ResidualUS)
	}

	// latency sanity: the first three anchors are ordered by construction
	// (an observation/confirmation cannot precede the event it follows), while
	// last_applied_cb_us is a callback fired during the record's own
	// processing, i.e. before that record's COMMIT end — a negative
	// applied_cb_latency is an ordering fact, not a defect.
	for _, lat := range []struct {
		name  string
		v     *int64
		order bool
	}{
		{"publish_observe_latency_us", rs.Latency.PublishObserveLatencyUS, true},
		{"publish_done_latency_us", rs.Latency.PublishDoneLatencyUS, true},
		{"consume_confirm_latency_us", rs.Latency.ConsumeConfirmLatencyUS, true},
		{"applied_cb_latency_us", rs.Latency.AppliedCBLatencyUS, false},
	} {
		if lat.v == nil || *lat.v >= 0 {
			continue
		}
		if lat.order {
			fail("latency", "%s=%d < 0", lat.name, *lat.v)
			continue
		}
		note("latency: %s=%d < 0（observer 回调在事件处理期间触发，早于该事件提交结束；排序事实）", lat.name, *lat.v)
	}

	ck.Details = make([]string, 0, len(findings)+len(notes))
	ck.Details = append(ck.Details, findings...)
	ck.Details = append(ck.Details, notes...)
	return ck
}

// checkCounts reconciles anchors.counts, the loop/CSV-derived counters, the
// report counters and n.
func checkCounts(lr *loadedRun, rs *runSummary, fail func(string, string, ...any), note func(string, ...any), ck *checksBlock) {
	a := lr.anchors
	if a == nil || a.Counts == nil {
		note("counts_ok: anchors.counts 未采集（未评估）")
		return
	}
	c := a.Counts
	if allI64(c.ObservedProcess, c.ObservedApplied, c.ObservedDuplicate, c.ObservedVersionSkip, c.ObservedQuarantined, c.ObservedErrors) {
		sum := *c.ObservedApplied + *c.ObservedDuplicate + *c.ObservedVersionSkip + *c.ObservedQuarantined + *c.ObservedErrors
		if *c.ObservedProcess != sum {
			ck.CountsOK = false
			fail("counts_ok", "identity: process=%d != applied(%d)+duplicate(%d)+version_skip(%d)+quarantined(%d)+errors(%d)=%d",
				*c.ObservedProcess, *c.ObservedApplied, *c.ObservedDuplicate, *c.ObservedVersionSkip,
				*c.ObservedQuarantined, *c.ObservedErrors, sum)
		}
	} else {
		note("counts_ok: anchors.counts 计数字段不完整（identity 未评估）")
	}
	if c.ObservedApplied != nil && rs.N != nil && *c.ObservedApplied != *rs.N {
		ck.CountsOK = false
		fail("counts_ok", "observed_applied=%d != n=%d", *c.ObservedApplied, *rs.N)
	}
	if l := rs.Counts.Loop; l != nil {
		if l.ProcessSpans != nil && c.ObservedProcess != nil && int64(*l.ProcessSpans) != *c.ObservedProcess {
			ck.CountsOK = false
			fail("counts_ok", "loop process spans=%d != anchors.observed_process=%d", *l.ProcessSpans, *c.ObservedProcess)
		}
		if l.ErrorOutcomes != nil && c.ObservedErrors != nil && int64(*l.ErrorOutcomes) != *c.ObservedErrors {
			ck.CountsOK = false
			fail("counts_ok", "loop process error outcomes=%d != anchors.observed_errors=%d", *l.ErrorOutcomes, *c.ObservedErrors)
		}
		if l.AppliedCBRows != nil && c.AppliedCB != nil && int64(*l.AppliedCBRows) != *c.AppliedCB {
			ck.CountsOK = false
			fail("counts_ok", "loop applied_cb rows=%d != anchors.applied_cb=%d", *l.AppliedCBRows, *c.AppliedCB)
		}
		if l.SQLRows != nil && c.SQLSpans != nil && int64(*l.SQLRows) != *c.SQLSpans {
			ck.CountsOK = false
			fail("counts_ok", "sql.csv rows=%d != anchors.sql_spans=%d", *l.SQLRows, *c.SQLSpans)
		}
		if l.SQLUnattributed != nil && c.SQLUnattributed != nil && int64(*l.SQLUnattributed) != *c.SQLUnattributed {
			ck.CountsOK = false
			fail("counts_ok", "sql seq=-1 rows=%d != anchors.sql_unattributed=%d", *l.SQLUnattributed, *c.SQLUnattributed)
		}
	}
	if rp := rs.Counts.Report; rp != nil {
		if rp.ConsumerApplied != nil && c.ObservedApplied != nil && *rp.ConsumerApplied != *c.ObservedApplied {
			ck.CountsOK = false
			fail("counts_ok", "report.consumer_applied=%d != anchors.observed_applied=%d", *rp.ConsumerApplied, *c.ObservedApplied)
		}
		if rp.ProcessObserved != nil && c.ObservedProcess != nil && *rp.ProcessObserved != *c.ObservedProcess {
			ck.CountsOK = false
			fail("counts_ok", "report.process_observed=%d != anchors.observed_process=%d", *rp.ProcessObserved, *c.ObservedProcess)
		}
		if rp.SQLSpanCount != nil && rs.Counts.Loop != nil && rs.Counts.Loop.SQLRows != nil &&
			*rp.SQLSpanCount != int64(*rs.Counts.Loop.SQLRows) {
			ck.CountsOK = false
			fail("counts_ok", "report.sql_span_count=%d != sql.csv rows=%d", *rp.SQLSpanCount, *rs.Counts.Loop.SQLRows)
		}
	}
	// publish.csv vs anchors.publish
	if p := a.Publish; p != nil {
		if rs.Publish.Collected {
			if p.Cycles != nil && int64(rs.Publish.ProductiveCycles) != *p.Cycles {
				ck.CountsOK = false
				fail("counts_ok", "publish.csv 生产周期行=%d != anchors.publish.cycles=%d", rs.Publish.ProductiveCycles, *p.Cycles)
			}
			if rs.Publish.Rows != rs.Publish.ProductiveCycles+rs.Publish.BoundaryRows {
				ck.CountsOK = false
				fail("counts_ok", "publish.csv 行分类不完整: rows=%d productive=%d boundary=%d unclassified=%d",
					rs.Publish.Rows, rs.Publish.ProductiveCycles, rs.Publish.BoundaryRows, rs.Publish.UnclassifiedRows)
			}
			if !rs.Publish.LastCycleTerminal {
				ck.CountsOK = false
				fail("counts_ok", "publish.csv 末轮非终止形态（pending_seen=%s）", fint(rs.Publish.LastPendingSeen))
			}
			for _, pair := range []struct {
				name    string
				csv     int64
				anchors *int64
			}{
				{"claimed", rs.Publish.Claimed, p.Claimed},
				{"acked", rs.Publish.Acked, p.Acked},
				{"released", rs.Publish.Released, p.Released},
				{"blocked", rs.Publish.Blocked, p.Blocked},
			} {
				if pair.anchors != nil && pair.csv != *pair.anchors {
					ck.CountsOK = false
					fail("counts_ok", "publish.csv Σ%s=%d != anchors.publish.%s=%d", pair.name, pair.csv, pair.name, *pair.anchors)
				}
			}
			if rs.Publish.InvariantViolations != 0 {
				ck.CountsOK = false
				fail("counts_ok", "publish.csv 周期不变量违例 %d 行（acked+released+blocked != claimed）", rs.Publish.InvariantViolations)
			}
		}
	}
}

// compareReportAnchors reconciles report.json against anchors.json. The float
// tolerance is the frozen 1e-6; integer and map comparisons are exact. A key
// present on the anchors side but missing on the report side is a finding (the
// report is not the v1 extended report).
func compareReportAnchors(lr *loadedRun, rs *runSummary) (failures, notes []string) {
	a, r := lr.anchors, lr.report
	if r == nil {
		return nil, []string{"note: report_vs_anchors_ok: report.json 未采集（未评估）"}
	}
	if a == nil {
		return nil, []string{"note: report_vs_anchors_ok: anchors.json 未采集（未评估）"}
	}
	var p *publishAnchorsDoc
	if a.Publish != nil {
		p = a.Publish
	}
	var c *consumeAnchorsDoc
	if a.Consume != nil {
		c = a.Consume
	}
	sub := func(x, y *int64) (float64, bool) {
		if x == nil || y == nil {
			return 0, false
		}
		return float64(*x-*y) / 1e6, true
	}
	// rateTol widens the frozen 1e-6 absolute tolerance by the anchors'
	// quantization bound: the anchors carry whole microseconds, so a rate
	// derived from them deviates from the report's nanosecond-derived rate by
	// up to |rate| * 1us / duration.
	rateTol := func(rate, durationS float64) float64 {
		if durationS <= 0 {
			return 1e-6
		}
		return 1e-6 + math.Abs(rate)*1e-6/durationS
	}
	floatCheck := func(name string, av float64, avOK bool, rv *float64, tol float64) {
		if !avOK {
			return
		}
		if rv == nil {
			failures = append(failures, fmt.Sprintf("report_vs_anchors_ok: report.%s 缺失（anchors 侧存在）", name))
			return
		}
		if math.Abs(av-*rv) > tol {
			failures = append(failures, fmt.Sprintf("report_vs_anchors_ok: %s anchors=%.9f report=%.9f |Δ|=%.3g > tol=%.3g",
				name, av, *rv, math.Abs(av-*rv), tol))
		}
	}
	var publishDone, lastCycleEnd, pendingZero, pollConfirm, lastCommitEnd, lastAppliedCB *int64
	var drainStart *int64
	if p != nil {
		publishDone, lastCycleEnd, pendingZero = p.PublishDoneUS, p.LastCycleEndUS, p.PendingZeroObservedUS
	}
	if c != nil {
		pollConfirm, lastCommitEnd, lastAppliedCB = c.PollConfirmUS, c.LastCommitEndUS, c.LastAppliedCBUS
	}
	drainStart = a.DrainStartUS

	publishS, publishOK := sub(publishDone, drainStart)
	tailS, tailOK := sub(pollConfirm, publishDone)
	totalS, totalOK := sub(pollConfirm, drainStart)
	floatCheck("publish_seconds", publishS, publishOK, r.PublishSeconds, 1e-6)
	floatCheck("consumer_catchup_tail_seconds", tailS, tailOK, r.ConsumerTailSeconds, 1e-6)
	floatCheck("total_drain_seconds", totalS, totalOK, r.TotalDrainSeconds, 1e-6)
	e2e, e2eOK := 0.0, rs.N != nil && totalOK && totalS > 0
	if e2eOK {
		e2e = float64(*rs.N) / totalS
	}
	floatCheck("end_to_end_events_per_second", e2e, e2eOK, r.EndToEndEventsPerSec, rateTol(e2e, totalS))
	publishRate, publishRateOK := 0.0, rs.N != nil && publishOK && publishS > 0
	if publishRateOK {
		publishRate = float64(*rs.N) / publishS
	}
	floatCheck("publish_events_per_second", publishRate, publishRateOK, r.PublishEventsPerSec, rateTol(publishRate, publishS))
	// The report's *_seconds extension fields are offsets since drain_start,
	// not epoch-absolute seconds. A 0 anchor is the frozen "not collected"
	// sentinel (the producer's own conversion maps 0 to 0): the report must
	// carry 0 as well and no offset comparison is formed.
	var sentinels []string
	checkSeconds := func(name string, us *int64, rv *float64) {
		if us == nil {
			return
		}
		if *us == 0 {
			sentinels = append(sentinels, name)
			if rv != nil && *rv != 0 {
				failures = append(failures, fmt.Sprintf(
					"report_vs_anchors_ok: %s anchors=0（未采集哨兵）但 report=%.9f != 0", name, *rv))
			}
			return
		}
		if drainStart == nil {
			return
		}
		floatCheck(name, float64(*us-*drainStart)/1e6, true, rv, 1e-6)
	}
	checkSeconds("publish_last_cycle_end_seconds", lastCycleEnd, r.PublishLastCycleEndSeconds)
	checkSeconds("pending_zero_observed_seconds", pendingZero, r.PendingZeroObservedSeconds)
	checkSeconds("consume_confirm_seconds", pollConfirm, r.ConsumeConfirmSeconds)
	checkSeconds("last_commit_end_seconds", lastCommitEnd, r.LastCommitEndSeconds)
	checkSeconds("last_applied_cb_seconds", lastAppliedCB, r.LastAppliedCBSeconds)
	if len(sentinels) > 0 {
		notes = append(notes, fmt.Sprintf(
			"note: report_vs_anchors_ok: %s = 0 哨兵（该观测未采集，如 OFF 臂无 tracer/回调行）；report 对应值同为 0，按一致处理，不构成偏移比较",
			strings.Join(sentinels, ", ")))
	}

	intCheck := func(name string, av *int64, rv *int64) {
		if av == nil {
			return
		}
		if rv == nil {
			failures = append(failures, fmt.Sprintf("report_vs_anchors_ok: report.%s 缺失（anchors 侧存在）", name))
			return
		}
		if *av != *rv {
			failures = append(failures, fmt.Sprintf("report_vs_anchors_ok: %s anchors=%d report=%d", name, *av, *rv))
		}
	}
	if p != nil {
		intCheck("applied_at_publish_done", p.AppliedAtPublishDone, r.AppliedAtPublishDone)
		intCheck("publish_cycles", p.Cycles, r.PublishCycles)
		intCheck("publish_claimed", p.Claimed, r.PublishClaimed)
		intCheck("publish_acked", p.Acked, r.PublishAcked)
		intCheck("publish_released", p.Released, r.PublishReleased)
		intCheck("publish_blocked", p.Blocked, r.PublishBlocked)
	}
	if a.Counts != nil {
		intCheck("consumer_applied", a.Counts.ObservedApplied, r.ConsumerApplied)
		intCheck("process_observed", a.Counts.ObservedProcess, r.ProcessObserved)
		intCheck("sql_span_count", a.Counts.SQLSpans, r.SQLSpanCount)
	}
	if meta := lr.meta; meta != nil && meta.N != nil && r.N != nil && *meta.N != *r.N {
		failures = append(failures, fmt.Sprintf("report_vs_anchors_ok: n meta=%d report=%d", *meta.N, *r.N))
	}
	if a.SegEnabled != nil && r.SegEnabled != nil && *a.SegEnabled != *r.SegEnabled {
		failures = append(failures, fmt.Sprintf("report_vs_anchors_ok: seg_enabled anchors=%t report=%t", *a.SegEnabled, *r.SegEnabled))
	}
	if meta := lr.meta; meta != nil && meta.SegEnabled != nil && r.SegEnabled != nil && *meta.SegEnabled != *r.SegEnabled {
		failures = append(failures, fmt.Sprintf("report_vs_anchors_ok: seg_enabled meta=%t report=%t", *meta.SegEnabled, *r.SegEnabled))
	}
	if a.LagFinal != nil {
		if r.LagFinal == nil {
			failures = append(failures, "report_vs_anchors_ok: report.lag_final 缺失（anchors 侧存在）")
		} else if !sameLag(a.LagFinal, r.LagFinal) {
			failures = append(failures, fmt.Sprintf("report_vs_anchors_ok: lag_final anchors=%s report=%s", lagString(a.LagFinal), lagString(r.LagFinal)))
		}
	}
	return failures, notes
}

// sameLag compares two lag maps key by key.
func sameLag(x, y map[string]int64) bool {
	if len(x) != len(y) {
		return false
	}
	for k, v := range x {
		if yv, ok := y[k]; !ok || yv != v {
			return false
		}
	}
	return true
}

// lagString renders a lag map deterministically.
func lagString(m map[string]int64) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString("{")
	for i, k := range keys {
		if i > 0 {
			b.WriteString(" ")
		}
		b.WriteString(k)
		b.WriteString("=")
		b.WriteString(strconv.FormatInt(m[k], 10))
	}
	b.WriteString("}")
	return b.String()
}

// ------------------------------------------------------------------ helpers

// i64 dereferences an optional int64 (0 when absent).
func i64(p *int64) int64 {
	if p == nil {
		return 0
	}
	return *p
}

// allI64 reports whether every pointer is non-nil.
func allI64(ptrs ...*int64) bool {
	for _, p := range ptrs {
		if p == nil {
			return false
		}
	}
	return true
}

// sameDropPtr compares two optional counters (nil-safe).
func sameDropPtr(x, y *int64) bool {
	if x == nil || y == nil {
		return x == y
	}
	return *x == *y
}
