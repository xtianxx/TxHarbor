//go:build perf

// seg_stats.go is the deterministic, stdlib-only analyzer core for the
// segmented normal-query batch (perf layer only; offline: it reads evidence
// from disk and never starts a container, listener or connection).
//
// Input layout (frozen with the batch runner):
//
//	<root>/repeat<N>/<arm>/meta.json
//	<root>/repeat<N>/<arm>/client_samples.jsonl
//	<root>/repeat<N>/<arm>/warmup_samples.jsonl
//	<root>/repeat<N>/<arm>/server_segments.jsonl   (kind: "seg" | "pgq")
//
// <arm> is the raw arm token (R0G0, R1G0, R0G1, R1G1, R1G1:nocoll); every
// line is scanned line by line (a large file is never loaded whole) and
// ".jsonl.gz" siblings are accepted for tolerance. The analyzer is read-only;
// apart from the recorded generation timestamp it is deterministic and
// concurrency-free, so two runs over one tree produce identical summaries.
//
// Nested timing vocabulary (never added up as if disjoint):
//
//	server_total ⊇ {admit ⊕ below_admit ⊕ wrapper overhead}
//	below_admit ⊇ auth + query + ...
//	pgq statements ⊆ below_admit
//
// Discipline (also rendered into summary.md):
//
//   - every client sample, including non-2xx (429/503/500) and transport
//     errors, counts in the status counts and in the percentile denominator;
//     nothing is dropped and an error response is never treated as missing;
//   - p95 values are not additive: cross-segment and cross-metric statements
//     are differences on one ID set only;
//   - a large share of one segment is not proof that the segment is the
//     source of the difference;
//   - single-host synthetic load is not a production SLO;
//   - the 89ef787 numbers (47.5/72.7ms) stay historical measurements and are
//     never presented as a current-main measurement.
package perf

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	segSchemaVersion = 1

	segSummaryJSONName = "summary.json"
	segSummaryMDName   = "summary.md"

	segKindSeg = "seg"
	segKindPGQ = "pgq"

	segServerTotal  = "server_total"
	segAdmit        = "admit"
	segAdmitSkipped = "admit_skipped"
	segBelowAdmit   = "below_admit"

	segClassQuery = "query"

	// segShortfallToleranceNS is the numeric jitter allowance of the
	// server_total >= admit + below_admit invariant (0.05ms).
	segShortfallToleranceNS = 50_000
	// segPgqSlackNS is the allowance of the sum(pgq) <= below_admit + 1ms
	// invariant.
	segPgqSlackNS = 1_000_000

	segPhaseSeconds        = 4
	segPhaseCount          = 3
	segMaxListedViolations = 200
	segMaxArmViolations    = 50
	segPGQTopPrefixes      = 8
	segMaxWarnLines        = 3

	// Clock-step thresholds on the server-side wall labels, walked in id order:
	// a backward step means the wall clock was set back mid-run; a forward jump
	// cannot be told apart from a stall (GC, scheduling, event-loop block) and
	// is therefore disclosed as ambiguous and never corrected.
	segBackwardStepThresholdNS = -50 * int64(time.Millisecond)
	segForwardStepThresholdNS  = 500 * int64(time.Millisecond)

	segPhaseMethodWall          = "wall"
	segPhaseMethodStepCorrected = "step_corrected"
)

// segSegNames is the frozen segment vocabulary, in report order.
var segSegNames = []string{segServerTotal, segAdmit, segAdmitSkipped, segBelowAdmit}

var segPhaseLabels = []string{"[0s,4s)", "[4s,8s)", "[8s,12s)"}

// segFactorArmRe recognizes the frozen arm tokens R0G0/R1G0/R0G1/R1G1.
var segFactorArmRe = regexp.MustCompile(`^R([01])G([01])$`)

// segRepeatDirRe recognizes repeat directory names (repeat1, repeat-2, ...).
var segRepeatDirRe = regexp.MustCompile(`(?i)^repeat[-_ ]?0*([0-9]+)$`)

var segDiscipline = []string{
	"选样偏差控制：全部客户端样本（含 429/503/500 等非 2xx 与 transport 错误）均计入状态计数与分位数分母；不做选择性剔除，错误响应不计为缺失。",
	"p95 不可相加：分位数不可加；跨段/跨项比较只允许同一 ID 集合上的差值（差值对比（禁止相加/占比证明））。",
	"分段占比≠贡献证明：某段占总时长的比例大，不构成该段是收益来源的证明。",
	"单机合成负载非生产 SLO：单机、in-process 合成负载的绝对值不代表生产 SLO，仅用于同一环境内的受控对比。",
	"历史数值：89ef787 的 47.5/72.7ms 为历史测量，本轮不得标为当前 main 的实测数值。",
}

var segLimitations = []string{
	"相位分桶 [0s,4s)/[4s,8s)/[8s,12s) 相对 meta.window_start；固定 burst 在 8s 触发，第三档 [8s,12s) 与 burst 重叠，不可视为纯稳态。",
	"预热样本只做单独小节，不并入稳态统计。",
	"pgq 语句级记录仅在 TXHARBOR_PERF_SEG_TRACE=1 时存在；pgq 缺失不等于没有 PG 活动。",
	"perf_id 为服务端逐请求铸造；缺少 perf_id 的客户端样本无法配对，单独计数。",
	"计时嵌套：server_total ⊇ {admit ⊕ below_admit ⊕ 包装开销}，below_admit ⊇ 查询等，pgq 语句 ⊆ below_admit；禁止把各段相加当作总时长。",
	"因子效应为差值（R 效应 @G0/@G1、G 效应 @R0/@R1、交互），由臂间差值定义；不得对分位数做加和或占比证明。",
	"时钟步进：wall clock 回拨（按 server_total 的 id 序检测，阈值 <−50ms）按 |Δ| 对边界之后的标签做校正，并披露原始 wall 相位（client_query_phases_wall）；前跳（>+500ms）与停顿/GC 不可区分（ambiguous），只披露不校正；跨机绝对时间不可比；时长类指标（duration_ms / dur_ns）取自单调时钟，不受回拨影响。",
	"校正精度受相邻请求间隔限制：Σ|Δ| 含一次正常间隔，校正后标签可能仍偏早约一个间隔（≈0.1s）；相位归类可用，精确时刻不可复原。",
	"适用范围：只有 first_affected_id > first_steady_id（稳态 client perf_id 最小值）的步进才作用于窗口；预热期回拨不校正，window_start 永不校正。",
	"若批次把预热流量也写进 server_segments.jsonl，其段记录落在窗口之外并被相位分桶排除，逐段计数见 segment_outside_window。",
}

// segTopLevelFields documents the summary.json top-level keys (also rendered
// into summary.md).
var segTopLevelFields = []string{
	"schema_version", "generated_at", "root", "factor_definition", "factor_disclaimer",
	"arms", "repeat_p95_spread", "clock_steps", "factor_effects", "overhead_compare", "candidate_rules",
	"pairing_totals", "invariants", "completeness", "discipline", "limitations", "warnings",
}

// ---------------------------------------------------------------- latency math

// segLatency is the p50/p95/p99/min/max summary of one value set. Absent
// values (empty set) stay nil and render as "-" in markdown, never as 0.
type segLatency struct {
	Count int      `json:"count"`
	P50MS *float64 `json:"p50_ms,omitempty"`
	P95MS *float64 `json:"p95_ms,omitempty"`
	P99MS *float64 `json:"p99_ms,omitempty"`
	MinMS *float64 `json:"min_ms,omitempty"`
	MaxMS *float64 `json:"max_ms,omitempty"`
}

// segRound6 rounds milliseconds to 1e-6ms (nanosecond precision).
func segRound6(v float64) float64 { return math.Round(v*1e6) / 1e6 }

// segFinitePtr returns a rounded pointer, or nil for NaN/Inf so JSON never
// carries an invalid number.
func segFinitePtr(v float64) *float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return nil
	}
	r := segRound6(v)
	return &r
}

// segPercentile is the linear-interpolation percentile of an ascending slice.
// The rank is computed in float64 and clamped into [0, n-1], so it is safe
// for the very large line-scanned inputs (no integer overflow, no panic).
func segPercentile(sorted []float64, p float64) float64 {
	n := len(sorted)
	if n == 0 || math.IsNaN(p) {
		return math.NaN()
	}
	if n == 1 || p <= 0 {
		return sorted[0]
	}
	if p >= 100 {
		return sorted[n-1]
	}
	rank := p / 100 * float64(n-1)
	if rank <= 0 {
		return sorted[0]
	}
	if rank >= float64(n-1) {
		return sorted[n-1]
	}
	lo := int(math.Floor(rank))
	hi := int(math.Ceil(rank))
	if lo < 0 {
		lo = 0
	}
	if hi > n-1 {
		hi = n - 1
	}
	if lo == hi {
		return sorted[lo]
	}
	return sorted[lo] + (sorted[hi]-sorted[lo])*(rank-float64(lo))
}

// segLatencyFromMS summarizes raw millisecond values (the input is copied;
// the caller's slice is untouched).
func segLatencyFromMS(values []float64) segLatency {
	lat := segLatency{Count: len(values)}
	if len(values) == 0 {
		return lat
	}
	sorted := make([]float64, len(values))
	copy(sorted, values)
	sort.Float64s(sorted)
	lat.MinMS = segFinitePtr(sorted[0])
	lat.MaxMS = segFinitePtr(sorted[len(sorted)-1])
	lat.P50MS = segFinitePtr(segPercentile(sorted, 50))
	lat.P95MS = segFinitePtr(segPercentile(sorted, 95))
	lat.P99MS = segFinitePtr(segPercentile(sorted, 99))
	return lat
}

// segLatencyFromNS summarizes raw nanosecond durations.
func segLatencyFromNS(values []int64) segLatency {
	ms := make([]float64, 0, len(values))
	for _, ns := range values {
		ms = append(ms, float64(ns)/1e6)
	}
	return segLatencyFromMS(ms)
}

// segRepeatValue is one per-repeat observation.
type segRepeatValue struct {
	Repeat int     `json:"repeat"`
	Value  float64 `json:"value"`
}

// segSeries is the per-repeat value list plus the cross-repeat median and
// range (min-max) of one metric. MissingRepeats names the repeats where a
// level was absent (the effect could not be formed there).
type segSeries struct {
	PerRepeat      []segRepeatValue `json:"per_repeat,omitempty"`
	MedianMS       *float64         `json:"median_ms,omitempty"`
	MinMS          *float64         `json:"min_ms,omitempty"`
	MaxMS          *float64         `json:"max_ms,omitempty"`
	RangeMS        *float64         `json:"range_ms,omitempty"`
	MissingRepeats []int            `json:"missing_repeats,omitempty"`
}

// segSeriesFrom builds the series; pairs are sorted by repeat.
func segSeriesFrom(pairs []segRepeatValue, missing []int) segSeries {
	out := segSeries{MissingRepeats: missing}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].Repeat < pairs[j].Repeat })
	out.PerRepeat = pairs
	if len(pairs) == 0 {
		return out
	}
	vals := make([]float64, 0, len(pairs))
	for _, p := range pairs {
		vals = append(vals, p.Value)
	}
	sort.Float64s(vals)
	out.MedianMS = segFinitePtr(segPercentile(vals, 50))
	out.MinMS = segFinitePtr(vals[0])
	out.MaxMS = segFinitePtr(vals[len(vals)-1])
	out.RangeMS = segFinitePtr(vals[len(vals)-1] - vals[0])
	return out
}

// segClockStep is one detected discontinuity of the server-side wall labels.
// Labels of records with id > BoundaryID carry the shifted clock; Backward
// steps are corrected by |DeltaMS|, forward jumps are disclosed only.
type segClockStep struct {
	BoundaryID      uint64  `json:"boundary_id"`
	FirstAffectedID uint64  `json:"first_affected_id"`
	DeltaMS         float64 `json:"delta_ms"`
	Kind            string  `json:"kind"`
	Ambiguous       bool    `json:"ambiguous"`
	// AppliesToWindow is false when the step happened before the steady
	// window (first_affected_id <= first_steady_id): window_start was then
	// recorded in the same shifted timebase, so sample-vs-origin differences
	// are already correct and nothing may be corrected.
	AppliesToWindow bool    `json:"applies_to_window"`
	Reason          string  `json:"reason,omitempty"`
	LabelBefore     string  `json:"label_before,omitempty"`
	LabelAfter      string  `json:"label_after,omitempty"`
	CorrectionMS    float64 `json:"correction_ms"`
	Note            string  `json:"note,omitempty"`
}

// segClockStepRollup is the per-arm clock-step view of the summary.
type segClockStepRollup struct {
	Repeat              int            `json:"repeat"`
	Arm                 string         `json:"arm"`
	BackwardSteps       int            `json:"backward_steps"`
	AppliedSteps        int            `json:"applied_steps"`
	ForwardSuspectSteps int            `json:"forward_suspect_steps"`
	PhaseMethod         string         `json:"phase_method"`
	Steps               []segClockStep `json:"steps"`
	Note                string         `json:"note,omitempty"`
}

// segSegSample is one deferred segment record (phase classification needs the
// clock-step correction, which is known only after the whole file is read).
type segSegSample struct {
	Name  string
	ID    uint64
	AtNS  int64
	DurNS int64
}

// segQuerySample is one deferred client query sample.
type segQuerySample struct {
	AtNS     int64
	MS       float64
	Status   int
	OK       bool
	ErrClass string
	PerfID   uint64
}

// segClassBrief is one class of the client-side sample stream.
type segClassBrief struct {
	N       int              `json:"n"`
	Status  segOutcomeCounts `json:"status"`
	Latency segLatency       `json:"latency"`
}

// segOutcomeCounts partitions one sample set by outcome. The partition is
// complete: ok_2xx + non_2xx + transport_errors + no_status == total.
type segOutcomeCounts struct {
	Total      int            `json:"total"`
	OK2xx      int            `json:"ok_2xx"`
	Non2xx     int            `json:"non_2xx"`
	Transport  int            `json:"transport_errors"`
	NoStatus   int            `json:"no_status"`
	Errors     int            `json:"errors"`
	ByStatus   map[string]int `json:"by_status"`
	ByErrClass map[string]int `json:"by_err_class,omitempty"`
}

// segStatusKey names a sample outcome: the HTTP status, or the transport /
// no-status bucket when the status is 0.
func segStatusKey(status int, errClass string) string {
	if status > 0 {
		return strconv.Itoa(status)
	}
	if errClass != "" {
		return errClass
	}
	return "no_status"
}

func segAddStatus(dst *segOutcomeCounts, status int, ok bool, errClass string) {
	if dst.ByStatus == nil {
		dst.ByStatus = map[string]int{}
	}
	dst.Total++
	dst.ByStatus[segStatusKey(status, errClass)]++
	switch {
	case status == 0 && errClass == "transport":
		dst.Transport++
	case status == 0:
		dst.NoStatus++
	case status >= 200 && status < 300:
		dst.OK2xx++
	default:
		dst.Non2xx++
	}
	if !ok {
		dst.Errors++
	}
	if errClass != "" {
		if dst.ByErrClass == nil {
			dst.ByErrClass = map[string]int{}
		}
		dst.ByErrClass[errClass]++
	}
}

// ------------------------------------------------------------------- evidence

// segClientSample mirrors internal/perf.Sample plus the perf_id join key.
type segClientSample struct {
	At         time.Time `json:"at"`
	State      string    `json:"state"`
	Class      string    `json:"class"`
	DurationMS float64   `json:"duration_ms"`
	Status     int       `json:"status"`
	OK         bool      `json:"ok"`
	ErrClass   string    `json:"err_class"`
	ErrText    string    `json:"err_text"`
	PerfID     uint64    `json:"perf_id"`
}

// segRecord is one server segment or one PG statement record; the kind field
// discriminates the two shapes in server_segments.jsonl.
type segRecord struct {
	Kind     string `json:"kind"`
	ID       uint64 `json:"id"`
	Seg      string `json:"seg"`
	Class    string `json:"class"`
	DurNS    int64  `json:"dur_ns"`
	AtUnixNS int64  `json:"at_unix_ns"`
	Method   string `json:"method"`
	Path     string `json:"path"`
	SQL      string `json:"sql"`
	Err      bool   `json:"err"`
}

// segMetaArm mirrors meta.arm.
type segMetaArm struct {
	Name         string `json:"name"`
	LimiterOn    bool   `json:"limiter_on"`
	BackgroundOn bool   `json:"background_on"`
	Collect      bool   `json:"collect"`
}

// segMeta mirrors the frozen meta.json keys the analyzer consumes; unknown
// keys are ignored.
type segMeta struct {
	Repeat       int             `json:"repeat"`
	OrderIndex   int             `json:"order_index"`
	Arm          segMetaArm      `json:"arm"`
	Commit       string          `json:"commit"`
	GoVersion    string          `json:"go_version"`
	StartedAt    json.RawMessage `json:"started_at"`
	FinishedAt   json.RawMessage `json:"finished_at"`
	WarmupStart  json.RawMessage `json:"warmup_start"`
	WarmupEnd    json.RawMessage `json:"warmup_end"`
	WindowStart  json.RawMessage `json:"window_start"`
	WindowEnd    json.RawMessage `json:"window_end"`
	SetupSeconds float64         `json:"setup_seconds"`
	Notes        []string        `json:"notes"`
}

// ---------------------------------------------------------------- report shape

type segPhaseBucketReport struct {
	Label   string           `json:"label"`
	N       int              `json:"n"`
	Status  segOutcomeCounts `json:"status"`
	Latency segLatency       `json:"latency"`
}

type segPhaseLatency struct {
	Label   string     `json:"label"`
	Latency segLatency `json:"latency"`
}

type segPairedSegments struct {
	IDs                       int        `json:"ids"`
	IDsWithAdmit              int        `json:"ids_with_admit"`
	IDsWithAdmitSkipped       int        `json:"ids_with_admit_skipped"`
	IDsWithAdmitAndBelowAdmit int        `json:"ids_with_admit_and_below_admit"`
	ServerTotal               segLatency `json:"server_total"`
	Admit                     segLatency `json:"admit"`
	AdmitSkipped              segLatency `json:"admit_skipped"`
	BelowAdmit                segLatency `json:"below_admit"`
}

type segPGQPrefixStat struct {
	Prefix string   `json:"prefix"`
	Count  int      `json:"count"`
	P50MS  *float64 `json:"p50_ms,omitempty"`
	P95MS  *float64 `json:"p95_ms,omitempty"`
}

type segPGQReport struct {
	Total            int                `json:"total"`
	Errors           int                `json:"errors"`
	DistinctPrefixes int                `json:"distinct_prefixes"`
	Latency          segLatency         `json:"latency"`
	TopPrefixes      []segPGQPrefixStat `json:"top_prefixes"`
}

type segPgPerRequest struct {
	Requests           int            `json:"requests"`
	RequestsWithoutPGQ int            `json:"requests_without_pgq"`
	ByStatements       map[string]int `json:"by_statements"`
	Mean               *float64       `json:"mean,omitempty"`
	P50                *float64       `json:"p50,omitempty"`
	P95                *float64       `json:"p95,omitempty"`
	Max                int            `json:"max"`
}

type segWarmupReport struct {
	Samples int                      `json:"samples"`
	Note    string                   `json:"note"`
	Query   segClassBrief            `json:"query"`
	ByClass map[string]segClassBrief `json:"by_class,omitempty"`
}

type segPairing struct {
	ClientSamples                       int            `json:"client_samples"`
	ClientWithoutPerfID                 int            `json:"client_without_perf_id"`
	ClientIDs                           int            `json:"client_ids"`
	ServerTotalIDs                      int            `json:"server_total_ids"`
	ClientWithoutServerTotal            int            `json:"client_without_server_total"`
	ServerTotalWithoutClient            int            `json:"server_total_without_client"`
	ServerTotalWithoutBelowAdmit        int            `json:"server_total_without_below_admit"`
	BelowAdmitWithoutServerTotal        int            `json:"below_admit_without_server_total"`
	ServerTotalWithoutAdmitOrBelowAdmit int            `json:"server_total_without_admit_or_below_admit"`
	AdmitWithoutServerTotal             int            `json:"admit_without_server_total"`
	AdmitSkippedWithoutServerTotal      int            `json:"admit_skipped_without_server_total"`
	PgqWithoutClient                    int            `json:"pgq_without_client"`
	DuplicateIDs                        map[string]int `json:"duplicate_ids"`
	MissingSegmentCounts                map[string]int `json:"missing_segment_counts"`
	ExtraSegmentCounts                  map[string]int `json:"extra_segment_counts"`
}

type segInvariantCheck struct {
	Name       string `json:"name"`
	Checked    int    `json:"checked"`
	Violations int    `json:"violations"`
	Skipped    bool   `json:"skipped,omitempty"`
	SkipReason string `json:"skip_reason,omitempty"`
}

type segViolation struct {
	Repeat int    `json:"repeat"`
	Arm    string `json:"arm"`
	ID     uint64 `json:"id"`
	Kind   string `json:"kind"`
	Detail string `json:"detail"`
}

type segInvariants struct {
	Checks          []segInvariantCheck `json:"checks"`
	Violations      []segViolation      `json:"violations"`
	ViolationCounts map[string]int      `json:"violation_counts"`
	Truncated       bool                `json:"violations_truncated"`
}

type segArmReport struct {
	Repeat                     int                          `json:"repeat"`
	Arm                        string                       `json:"arm"`
	Dir                        string                       `json:"dir"`
	MetaPresent                bool                         `json:"meta_present"`
	Commit                     string                       `json:"commit,omitempty"`
	OrderIndex                 int                          `json:"order_index,omitempty"`
	LimiterOn                  *bool                        `json:"limiter_on,omitempty"`
	BackgroundOn               *bool                        `json:"background_on,omitempty"`
	Collect                    *bool                        `json:"collect,omitempty"`
	WindowStart                string                       `json:"window_start,omitempty"`
	WindowEnd                  string                       `json:"window_end,omitempty"`
	WindowOrigin               string                       `json:"window_origin,omitempty"`
	WindowSeconds              *float64                     `json:"window_seconds,omitempty"`
	Files                      []string                     `json:"files"`
	ClientQuery                segClassBrief                `json:"client_query"`
	OtherClasses               map[string]segClassBrief     `json:"other_classes,omitempty"`
	QueryPhases                []segPhaseBucketReport       `json:"client_query_phases,omitempty"`
	QueryPhasesWall            []segPhaseBucketReport       `json:"client_query_phases_wall,omitempty"`
	QueryOutside               int                          `json:"client_query_outside_window"`
	QueryOutsideWall           int                          `json:"client_query_outside_window_wall,omitempty"`
	PhaseNote                  string                       `json:"phase_note,omitempty"`
	PhaseMethod                string                       `json:"phase_method"`
	ClockSteps                 []segClockStep               `json:"clock_steps,omitempty"`
	ClockStepsBackward         int                          `json:"clock_steps_backward"`
	ClockStepsApplied          int                          `json:"clock_steps_applied"`
	ClockStepsForwardSuspect   int                          `json:"clock_steps_forward_suspect"`
	ClockStepsNote             string                       `json:"clock_steps_note,omitempty"`
	WindowSecondsCorrected     *float64                     `json:"window_seconds_corrected,omitempty"`
	WindowSecondsCorrectedNote string                       `json:"window_seconds_corrected_note,omitempty"`
	Segments                   map[string]segLatency        `json:"segments"`
	SegmentPhases              map[string][]segPhaseLatency `json:"segment_phases,omitempty"`
	SegmentPreBurst            map[string]segLatency        `json:"segment_pre_burst,omitempty"`
	SegmentOutside             map[string]int               `json:"segment_outside_window,omitempty"`
	Paired                     segPairedSegments            `json:"paired_segments"`
	PgTotal                    segLatency                   `json:"pg_total_per_request"`
	Pgq                        segPGQReport                 `json:"pgq"`
	PgPerRequest               segPgPerRequest              `json:"pg_statements_per_request"`
	Warmup                     segWarmupReport              `json:"warmup"`
	Pairing                    segPairing                   `json:"pairing"`
	Invariants                 segInvariants                `json:"invariants"`
	LineErrors                 int                          `json:"line_errors"`
	MissingFiles               []string                     `json:"missing_files,omitempty"`
	Warnings                   []string                     `json:"warnings,omitempty"`
}

type segMetricEffects struct {
	Metric     string               `json:"metric"`
	Definition string               `json:"definition"`
	Levels     map[string]segSeries `json:"levels"`
	Effects    map[string]segSeries `json:"effects"`
}

type segOverheadSide struct {
	NPerRepeat []segRepeatValue `json:"n_per_repeat,omitempty"`
	NTotal     int              `json:"n_total"`
	P50        segSeries        `json:"p50_ms"`
	P95        segSeries        `json:"p95_ms"`
	P99        segSeries        `json:"p99_ms"`
}

type segOverheadCompare struct {
	Base          string           `json:"base"`
	Variant       string           `json:"variant"`
	Present       bool             `json:"present"`
	PairedRepeats []int            `json:"paired_repeats,omitempty"`
	BaseSide      segOverheadSide  `json:"base_side"`
	VariantSide   segOverheadSide  `json:"variant_side"`
	DeltaP50      segSeries        `json:"delta_p50_ms"`
	DeltaP95      segSeries        `json:"delta_p95_ms"`
	DeltaP99      segSeries        `json:"delta_p99_ms"`
	Reference     *segOverheadSide `json:"reference_steady_r1g1,omitempty"`
	Note          string           `json:"note"`
}

type segRuleCheck struct {
	Rule      string           `json:"rule"`
	Status    string           `json:"status"`
	Observed  string           `json:"observed"`
	PerRepeat []segRepeatValue `json:"per_repeat,omitempty"`
	MedianMS  *float64         `json:"median_ms,omitempty"`
	Note      string           `json:"note,omitempty"`
}

type segCompleteness struct {
	Cells              int                 `json:"cells"`
	Repeats            []int               `json:"repeats"`
	ArmsByRepeat       map[string][]string `json:"arms_by_repeat"`
	ExpectedSteadyArms []string            `json:"expected_steady_arms"`
	MissingSteadyArms  map[string][]string `json:"missing_steady_arms_by_repeat,omitempty"`
	OverheadOnlyRepeat []int               `json:"overhead_only_repeats,omitempty"`
	NocollArms         []string            `json:"nocoll_arms,omitempty"`
	MetaMissing        []string            `json:"meta_missing,omitempty"`
	FilesMissing       map[string][]string `json:"files_missing,omitempty"`
	LineErrors         int                 `json:"line_errors"`
}

type segReport struct {
	SchemaVersion    int                             `json:"schema_version"`
	GeneratedAt      string                          `json:"generated_at"`
	Root             string                          `json:"root"`
	FactorDefinition string                          `json:"factor_definition"`
	FactorDisclaimer string                          `json:"factor_disclaimer"`
	Arms             []segArmReport                  `json:"arms"`
	RepeatSpread     map[string]map[string]segSeries `json:"repeat_p95_spread"`
	ClockSteps       []segClockStepRollup            `json:"clock_steps"`
	FactorEffects    []segMetricEffects              `json:"factor_effects"`
	OverheadCompare  segOverheadCompare              `json:"overhead_compare"`
	CandidateRules   []segRuleCheck                  `json:"candidate_rules"`
	PairingTotals    segPairing                      `json:"pairing_totals"`
	Invariants       segInvariants                   `json:"invariants"`
	Completeness     segCompleteness                 `json:"completeness"`
	Discipline       []string                        `json:"discipline"`
	Limitations      []string                        `json:"limitations"`
	// Warnings is always present (never omitted) so the summary key set is
	// stable for consumers.
	Warnings []string `json:"warnings"`
}

// ------------------------------------------------------------ file discovery

type segArmInput struct {
	Dir    string
	Repeat int
	Arm    string
}

// segDiscoverCells lists <root>/repeat<N>/<arm>/ cells in a deterministic
// order (repeat number, then arm name).
func segDiscoverCells(root string) ([]segArmInput, []string, error) {
	var warnings []string
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, nil, fmt.Errorf("read analyze root: %w", err)
	}
	type repeatDir struct {
		name   string
		number int
	}
	var repeats []repeatDir
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		m := segRepeatDirRe.FindStringSubmatch(entry.Name())
		if m == nil {
			warnings = append(warnings, fmt.Sprintf("目录 %s 不匹配 repeat<N>，已忽略", entry.Name()))
			continue
		}
		n, err := strconv.Atoi(m[1])
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("目录 %s 的重复号无法解析，已忽略", entry.Name()))
			continue
		}
		repeats = append(repeats, repeatDir{name: entry.Name(), number: n})
	}
	sort.Slice(repeats, func(i, j int) bool {
		if repeats[i].number != repeats[j].number {
			return repeats[i].number < repeats[j].number
		}
		return repeats[i].name < repeats[j].name
	})
	if len(repeats) == 0 {
		return nil, warnings, fmt.Errorf("no repeat<N> directories under %s", root)
	}
	var cells []segArmInput
	for _, rd := range repeats {
		dir := filepath.Join(root, rd.name)
		armEntries, err := os.ReadDir(dir)
		if err != nil {
			return nil, warnings, fmt.Errorf("read repeat dir %s: %w", rd.name, err)
		}
		var names []string
		for _, entry := range armEntries {
			if entry.IsDir() {
				names = append(names, entry.Name())
			}
		}
		sort.Strings(names)
		for _, name := range names {
			cells = append(cells, segArmInput{Dir: filepath.Join(dir, name), Repeat: rd.number, Arm: name})
		}
	}
	return cells, warnings, nil
}

// segFileCandidates maps a logical base name to accepted file names.
func segFileCandidates(base string) []string {
	return []string{base + ".jsonl", base + ".jsonl.gz", base + ".ndjson", base + ".ndjson.gz"}
}

// segFindFile locates one logical JSONL file inside dir.
func segFindFile(dir, base string) (string, bool) {
	for _, cand := range segFileCandidates(base) {
		path := filepath.Join(dir, cand)
		if st, err := os.Stat(path); err == nil && !st.IsDir() {
			return path, true
		}
	}
	return "", false
}

// segScanJSONL streams one JSONL file (gzip tolerated) line by line. Comment
// lines ("#", "//") and blank lines are skipped.
func segScanJSONL(path string, fn func(line []byte) error) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	var reader io.Reader = file
	if strings.HasSuffix(path, ".gz") {
		gz, err := gzip.NewReader(file)
		if err != nil {
			return fmt.Errorf("gzip %s: %w", path, err)
		}
		defer gz.Close()
		reader = gz
	}
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 0, 1<<20), 64<<20)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 || line[0] == '#' {
			continue
		}
		if len(line) > 1 && line[0] == '/' && line[1] == '/' {
			continue
		}
		if err := fn(line); err != nil {
			return err
		}
	}
	return scanner.Err()
}

// segParseTime accepts the Go time.Time JSON encoding (RFC3339Nano string)
// and, tolerantly, a numeric unix timestamp.
func segParseTime(raw json.RawMessage) (time.Time, bool) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || string(trimmed) == "null" {
		return time.Time{}, false
	}
	if trimmed[0] == '"' {
		var s string
		if err := json.Unmarshal(trimmed, &s); err != nil {
			return time.Time{}, false
		}
		for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05.999999999"} {
			if ts, err := time.Parse(layout, s); err == nil {
				return ts.UTC(), true
			}
		}
		return time.Time{}, false
	}
	var num float64
	if err := json.Unmarshal(trimmed, &num); err != nil {
		return time.Time{}, false
	}
	switch {
	case num >= 1e17:
		return time.Unix(0, int64(num)).UTC(), true
	case num >= 1e14:
		return time.Unix(0, int64(num)*1e3).UTC(), true
	case num >= 1e11:
		return time.Unix(0, int64(num)*1e6).UTC(), true
	default:
		sec := int64(num)
		return time.Unix(sec, int64((num-float64(sec))*1e9)).UTC(), true
	}
}

// segLoadMeta loads meta.json from the arm directory (tolerating a few
// filename variants); a missing meta is not a fatal error.
func segLoadMeta(dir string) (*segMeta, string, bool) {
	for _, name := range []string{"meta.json", "meta.json.gz", "meta.txt"} {
		path := filepath.Join(dir, name)
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		if strings.HasSuffix(name, ".gz") {
			zr, err := gzip.NewReader(bytes.NewReader(data))
			if err != nil {
				continue
			}
			data, err = io.ReadAll(zr)
			zr.Close()
			if err != nil {
				continue
			}
		}
		var meta segMeta
		if err := json.Unmarshal(data, &meta); err != nil {
			continue
		}
		return &meta, name, true
	}
	return nil, "", false
}

// -------------------------------------------------------------- arm scanning

type segPhaseAgg struct {
	Count  int
	DursMS []float64
	Status segOutcomeCounts
}

type segClassAgg struct {
	DursMS []float64
	Status segOutcomeCounts
}

type segIDAgg struct {
	ServerTotalNS  []int64
	AdmitNS        []int64
	AdmitSkippedNS []int64
	BelowAdmitNS   []int64
	PGQSumNS       int64
	PGQRecords     int
}

type segPrefixAgg struct {
	Count  int
	Errors int
	DursNS []int64
}

type segArmAcc struct {
	clientClasses map[string]*segClassAgg
	clientIDs     map[uint64]int
	clientNoID    int
	clientSamples int
	firstClientAt time.Time
	haveFirstAt   bool

	queryPhases      [segPhaseCount]segPhaseAgg
	queryPhasesWall  [segPhaseCount]segPhaseAgg
	queryOutside     int
	queryOutsideWall int
	querySamples     []segQuerySample

	warmupClasses map[string]*segClassAgg

	segDurs      map[string][]int64
	segSamples   []segSegSample
	segPhaseDurs map[string][segPhaseCount][]int64
	segPhaseOut  map[string]int

	byID map[uint64]*segIDAgg

	pgqPrefixes   map[string]*segPrefixAgg
	pgqTotal      int
	pgqErrors     int
	pgqDurs       []int64
	pgqCountID    map[uint64]int
	pgqPrefixByID map[uint64]map[string]int
	pgqPerPrefix  int

	unknownSegs  map[string]int
	unknownKinds map[string]int

	lineErrors int
	warnLines  []string
}

func segNewArmAcc() *segArmAcc {
	return &segArmAcc{
		clientClasses: map[string]*segClassAgg{},
		clientIDs:     map[uint64]int{},
		warmupClasses: map[string]*segClassAgg{},
		segDurs:       map[string][]int64{},
		segPhaseDurs:  map[string][segPhaseCount][]int64{},
		segPhaseOut:   map[string]int{},
		byID:          map[uint64]*segIDAgg{},
		pgqPrefixes:   map[string]*segPrefixAgg{},
		pgqCountID:    map[uint64]int{},
		pgqPrefixByID: map[uint64]map[string]int{},
		unknownSegs:   map[string]int{},
		unknownKinds:  map[string]int{},
	}
}

func (acc *segArmAcc) classAgg(class string) *segClassAgg {
	agg := acc.clientClasses[class]
	if agg == nil {
		agg = &segClassAgg{}
		acc.clientClasses[class] = agg
	}
	return agg
}

func (acc *segArmAcc) warmupAgg(class string) *segClassAgg {
	agg := acc.warmupClasses[class]
	if agg == nil {
		agg = &segClassAgg{}
		acc.warmupClasses[class] = agg
	}
	return agg
}

func (acc *segArmAcc) idAgg(id uint64) *segIDAgg {
	agg := acc.byID[id]
	if agg == nil {
		agg = &segIDAgg{}
		acc.byID[id] = agg
	}
	return agg
}

func (acc *segArmAcc) noteLineError(path string, err error) {
	acc.lineErrors++
	if len(acc.warnLines) < segMaxWarnLines {
		acc.warnLines = append(acc.warnLines, fmt.Sprintf("%s: %v", filepath.Base(path), err))
	}
}

// ------------------------------------------------------------------- scanning

// segPhaseIndex returns the phase bucket of atNS relative to the window
// origin: 0 → [0,4s), 1 → [4s,8s), 2 → [8s,12s); -1 → outside (or unknown).
func segPhaseIndex(atNS int64, windowStartNS int64, known bool) int {
	if !known {
		return -1
	}
	rel := atNS - windowStartNS
	if rel < 0 {
		return -1
	}
	idx := int(rel / int64(segPhaseSeconds*int64(time.Second)))
	if idx < 0 || idx >= segPhaseCount {
		return -1
	}
	return idx
}

// segPhaseReports renders the three phase buckets of an accumulator.
func segPhaseReports(phases [segPhaseCount]segPhaseAgg) []segPhaseBucketReport {
	out := make([]segPhaseBucketReport, 0, segPhaseCount)
	for i := range segPhaseCount {
		phase := phases[i]
		out = append(out, segPhaseBucketReport{
			Label:   segPhaseLabels[i],
			N:       phase.Count,
			Status:  phase.Status,
			Latency: segLatencyFromMS(phase.DursMS),
		})
	}
	return out
}

// firstSteadyID is the smallest perf_id of a steady client sample (query or
// create); it separates the pre-window ids from the window ids.
func (acc *segArmAcc) firstSteadyID() (uint64, bool) {
	var minID uint64
	have := false
	for _, id := range segSortedUint64Keys(acc.clientIDs) {
		if !have || id < minID {
			minID = id
			have = true
		}
	}
	return minID, have
}

// segCorrectedWindowSeconds returns the wall window length plus the backward
// step sum of the steps that apply to the window. Pre-window steps leave the
// window length untouched (window_start shares their shifted timebase), and an
// applicable ambiguous (forward) step makes the length indeterminable.
func segCorrectedWindowSeconds(rawSeconds float64, steps []segClockStep) (*float64, string) {
	var correctionMS float64
	ambiguous, applied := false, false
	for _, step := range steps {
		if !step.AppliesToWindow {
			continue
		}
		if step.Ambiguous {
			ambiguous = true
			continue
		}
		applied = true
		correctionMS += step.CorrectionMS
	}
	switch {
	case ambiguous:
		return nil, "作用于窗口的可疑前跳（ambiguous）：窗口长度不可判定"
	case !applied:
		v := segRound6(rawSeconds)
		return &v, ""
	default:
		v := segRound6(rawSeconds + correctionMS/1000)
		return &v, fmt.Sprintf("原始 wall 窗口 + 作用于窗口的回拨校正 %.3fms", correctionMS)
	}
}

func (acc *segArmAcc) scanClientLine(line []byte, _ int64, _ bool) error {
	var s segClientSample
	if err := json.Unmarshal(line, &s); err != nil {
		return err
	}
	acc.clientSamples++
	agg := acc.classAgg(s.Class)
	segAddStatus(&agg.Status, s.Status, s.OK, s.ErrClass)
	if !math.IsNaN(s.DurationMS) && !math.IsInf(s.DurationMS, 0) {
		agg.DursMS = append(agg.DursMS, s.DurationMS)
	}
	if s.PerfID != 0 {
		acc.clientIDs[s.PerfID]++
	} else {
		acc.clientNoID++
	}
	if !s.At.IsZero() && (!acc.haveFirstAt || s.At.Before(acc.firstClientAt)) {
		acc.firstClientAt = s.At
		acc.haveFirstAt = true
	}
	if s.Class == segClassQuery && !s.At.IsZero() {
		// Phase classification waits for the window origin (meta or first-sample
		// fallback) and for the clock-step correction, both known only after the
		// whole arm has been read.
		acc.querySamples = append(acc.querySamples, segQuerySample{
			AtNS: s.At.UnixNano(), MS: s.DurationMS,
			Status: s.Status, OK: s.OK, ErrClass: s.ErrClass, PerfID: s.PerfID,
		})
	}
	return nil
}

// segAddPhaseSample buckets one sample into a phase aggregate.
func segAddPhaseSample(phases *[segPhaseCount]segPhaseAgg, outside *int, atNS int64, ms float64, status int, ok bool, errClass string, windowStartNS int64, windowKnown bool) {
	idx := segPhaseIndex(atNS, windowStartNS, windowKnown)
	if idx < 0 {
		*outside++
		return
	}
	phase := &phases[idx]
	phase.Count++
	segAddStatus(&phase.Status, status, ok, errClass)
	if !math.IsNaN(ms) && !math.IsInf(ms, 0) {
		phase.DursMS = append(phase.DursMS, ms)
	}
}

// finalizePhases classifies the deferred query samples twice: with the
// clock-step-corrected labels (the reported view) and with the raw wall labels
// (the audit view).
func (acc *segArmAcc) finalizePhases(windowStartNS int64, windowKnown bool, corr func(uint64) int64) {
	for _, sample := range acc.querySamples {
		segAddPhaseSample(&acc.queryPhases, &acc.queryOutside,
			sample.AtNS+corr(sample.PerfID), sample.MS, sample.Status, sample.OK, sample.ErrClass, windowStartNS, windowKnown)
		segAddPhaseSample(&acc.queryPhasesWall, &acc.queryOutsideWall,
			sample.AtNS, sample.MS, sample.Status, sample.OK, sample.ErrClass, windowStartNS, windowKnown)
	}
	acc.querySamples = nil
}

// finalizeSegments buckets the deferred segment records with the corrected
// labels.
func (acc *segArmAcc) finalizeSegments(windowStartNS int64, windowKnown bool, corr func(uint64) int64) {
	for _, sample := range acc.segSamples {
		idx := segPhaseIndex(sample.AtNS+corr(sample.ID), windowStartNS, windowKnown)
		if idx < 0 {
			acc.segPhaseOut[sample.Name]++
			continue
		}
		buckets := acc.segPhaseDurs[sample.Name]
		buckets[idx] = append(buckets[idx], sample.DurNS)
		acc.segPhaseDurs[sample.Name] = buckets
	}
	acc.segSamples = nil
}

// segDetectClockSteps walks the server_total wall labels in id order (the
// request ids are monotonic, the wall clock is not) and reports every
// discontinuity beyond the thresholds.
func segDetectClockSteps(samples []segSegSample) []segClockStep {
	type label struct {
		id   uint64
		atNS int64
	}
	var labels []label
	seen := map[uint64]bool{}
	for _, sample := range samples {
		if sample.Name != segServerTotal || seen[sample.ID] {
			continue
		}
		seen[sample.ID] = true
		labels = append(labels, label{id: sample.ID, atNS: sample.AtNS})
	}
	sort.Slice(labels, func(i, j int) bool { return labels[i].id < labels[j].id })
	var steps []segClockStep
	for i := 1; i < len(labels); i++ {
		deltaNS := labels[i].atNS - labels[i-1].atNS
		step := segClockStep{
			BoundaryID:      labels[i-1].id,
			FirstAffectedID: labels[i].id,
			DeltaMS:         segRound6(float64(deltaNS) / 1e6),
			LabelBefore:     time.Unix(0, labels[i-1].atNS).UTC().Format(time.RFC3339Nano),
			LabelAfter:      time.Unix(0, labels[i].atNS).UTC().Format(time.RFC3339Nano),
		}
		switch {
		case deltaNS < segBackwardStepThresholdNS:
			step.Kind = "backward"
			step.CorrectionMS = segRound6(-float64(deltaNS) / 1e6)
			steps = append(steps, step)
		case deltaNS > segForwardStepThresholdNS:
			step.Kind = "forward_suspect"
			step.Ambiguous = true
			step.Note = "前跳无法与停顿/GC/事件循环阻塞区分，不做自动校正"
			steps = append(steps, step)
		}
	}
	return steps
}

// segClockCorrection builds the label correction: every record whose id is
// greater than a backward step's boundary is shifted forward by the cumulative
// |delta| of the backward steps before it. Records without an id are never
// corrected.
func segClockCorrection(steps []segClockStep) func(uint64) int64 {
	type point struct {
		boundary uint64
		cumNS    int64
	}
	var points []point
	var cumNS int64
	for _, step := range steps {
		if !step.AppliesToWindow || step.Ambiguous || step.CorrectionMS <= 0 {
			continue
		}
		cumNS += int64(math.Round(step.CorrectionMS * 1e6))
		points = append(points, point{boundary: step.BoundaryID, cumNS: cumNS})
	}
	if len(points) == 0 {
		return func(uint64) int64 { return 0 }
	}
	return func(id uint64) int64 {
		if id == 0 {
			return 0
		}
		var total int64
		for _, p := range points {
			if id > p.boundary {
				total = p.cumNS
			}
		}
		return total
	}
}

func (acc *segArmAcc) scanWarmupLine(line []byte) error {
	var s segClientSample
	if err := json.Unmarshal(line, &s); err != nil {
		return err
	}
	agg := acc.warmupAgg(s.Class)
	segAddStatus(&agg.Status, s.Status, s.OK, s.ErrClass)
	if !math.IsNaN(s.DurationMS) && !math.IsInf(s.DurationMS, 0) {
		agg.DursMS = append(agg.DursMS, s.DurationMS)
	}
	return nil
}

func (acc *segArmAcc) scanSegmentLine(line []byte, _ int64, _ bool) error {
	var r segRecord
	if err := json.Unmarshal(line, &r); err != nil {
		return err
	}
	kind := r.Kind
	if kind == "" {
		switch {
		case r.Seg != "":
			kind = segKindSeg
		case r.SQL != "":
			kind = segKindPGQ
		}
	}
	switch kind {
	case segKindPGQ:
		acc.pgqTotal++
		if r.Err {
			acc.pgqErrors++
		}
		acc.pgqDurs = append(acc.pgqDurs, r.DurNS)
		prefix := segSQLPrefix(r.SQL)
		if prefix == "" {
			prefix = "(empty)"
		}
		agg := acc.pgqPrefixes[prefix]
		if agg == nil {
			agg = &segPrefixAgg{}
			acc.pgqPrefixes[prefix] = agg
		}
		agg.Count++
		agg.DursNS = append(agg.DursNS, r.DurNS)
		if r.Err {
			agg.Errors++
		}
		acc.pgqCountID[r.ID]++
		prefixCounts := acc.pgqPrefixByID[r.ID]
		if prefixCounts == nil {
			prefixCounts = map[string]int{}
			acc.pgqPrefixByID[r.ID] = prefixCounts
		}
		prefixCounts[prefix]++
		id := acc.idAgg(r.ID)
		id.PGQSumNS += r.DurNS
		id.PGQRecords++
	case segKindSeg:
		if r.Seg != "" {
			acc.segDurs[r.Seg] = append(acc.segDurs[r.Seg], r.DurNS)
			acc.segSamples = append(acc.segSamples, segSegSample{Name: r.Seg, ID: r.ID, AtNS: r.AtUnixNS, DurNS: r.DurNS})
			switch r.Seg {
			case segServerTotal, segAdmit, segAdmitSkipped, segBelowAdmit:
			default:
				acc.unknownSegs[r.Seg]++
			}
			id := acc.idAgg(r.ID)
			switch r.Seg {
			case segServerTotal:
				id.ServerTotalNS = append(id.ServerTotalNS, r.DurNS)
			case segAdmit:
				id.AdmitNS = append(id.AdmitNS, r.DurNS)
			case segAdmitSkipped:
				id.AdmitSkippedNS = append(id.AdmitSkippedNS, r.DurNS)
			case segBelowAdmit:
				id.BelowAdmitNS = append(id.BelowAdmitNS, r.DurNS)
			}
		} else {
			acc.unknownKinds["seg(empty name)"]++
		}
	default:
		acc.unknownKinds[r.Kind]++
	}
	return nil
}

// ------------------------------------------------------------------ helpers

func segSumNS(values []int64) int64 {
	var sum int64
	for _, v := range values {
		sum += v
	}
	return sum
}

func segMaxNS(values []int64) int64 {
	max := int64(math.MinInt64)
	for _, v := range values {
		if v > max {
			max = v
		}
	}
	return max
}

// segSQLPrefix normalizes whitespace and returns a bounded leading prefix of
// the statement so statements differing only in arguments group together.
func segSQLPrefix(sql string) string {
	const maxTokens = 4
	const maxRunes = 64
	fields := strings.Fields(sql)
	if len(fields) == 0 {
		return ""
	}
	var b strings.Builder
	truncated := false
	for i, field := range fields {
		if i >= maxTokens {
			truncated = true
			break
		}
		if i > 0 {
			b.WriteByte(' ')
		}
		if b.Len()+len(field) > maxRunes {
			runes := []rune(field)
			room := maxRunes - b.Len()
			if room <= 0 {
				truncated = true
				break
			}
			if room < len(runes) {
				b.WriteString(string(runes[:room]))
				truncated = true
			} else {
				b.WriteString(field)
			}
			break
		}
		b.WriteString(field)
	}
	out := b.String()
	if truncated {
		out += "…"
	}
	return out
}

// segFactorKey derives the factorial cell key (R<limiter>G<background>) of an
// arm; the overhead variant (collect off / ":nocoll") is excluded.
func segFactorKey(arm segArmReport) (string, bool) {
	if strings.Contains(arm.Arm, "nocoll") {
		return "", false
	}
	if arm.Collect != nil && !*arm.Collect {
		return "", false
	}
	if arm.LimiterOn != nil && arm.BackgroundOn != nil {
		r, g := "0", "0"
		if *arm.LimiterOn {
			r = "1"
		}
		if *arm.BackgroundOn {
			g = "1"
		}
		return "R" + r + "G" + g, true
	}
	if m := segFactorArmRe.FindStringSubmatch(arm.Arm); m != nil {
		return "R" + m[1] + "G" + m[2], true
	}
	return "", false
}

func new(v bool) *bool { return &v }

// segSortedKeys returns the sorted keys of a string-keyed map.
func segSortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func segSortedUint64Keys[V any](m map[uint64]V) []uint64 {
	keys := make([]uint64, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

// -------------------------------------------------------------- arm analysis

// segAnalyzeCell reads one <repeat>/<arm> cell and builds its report.
func segAnalyzeCell(cell segArmInput) (segArmReport, error) {
	out := segArmReport{Repeat: cell.Repeat, Arm: cell.Arm, Dir: cell.Dir}
	acc := segNewArmAcc()

	meta, metaName, metaPresent := segLoadMeta(cell.Dir)
	out.MetaPresent = metaPresent
	if metaPresent {
		out.Commit = meta.Commit
		out.OrderIndex = meta.OrderIndex
		if meta.Arm.Name != "" {
			out.LimiterOn = new(meta.Arm.LimiterOn)
			out.BackgroundOn = new(meta.Arm.BackgroundOn)
			out.Collect = new(meta.Arm.Collect)
		}
		out.Files = append(out.Files, metaName)
		if meta.Arm.Name != "" && meta.Arm.Name != cell.Arm {
			out.Warnings = append(out.Warnings,
				fmt.Sprintf("meta.arm.name=%q 与目录名 %q 不一致（以 meta 为准）", meta.Arm.Name, cell.Arm))
		}
	} else {
		if m := segFactorArmRe.FindStringSubmatch(cell.Arm); m != nil {
			out.LimiterOn = new(m[1] == "1")
			out.BackgroundOn = new(m[2] == "1")
			out.Collect = new(!strings.Contains(cell.Arm, "nocoll"))
			out.Warnings = append(out.Warnings, "meta.json 缺失：因子水平回退为目录名解析")
		} else {
			out.Warnings = append(out.Warnings, "meta.json 缺失：limiter_on/background_on 未知")
		}
	}
	if out.LimiterOn == nil {
		// meta.json exists but carries no arm block: fall back to the arm token
		// instead of leaving the factors unknown.
		if m := segFactorArmRe.FindStringSubmatch(cell.Arm); m != nil {
			out.LimiterOn = new(m[1] == "1")
			out.BackgroundOn = new(m[2] == "1")
			out.Collect = new(!strings.Contains(cell.Arm, "nocoll"))
			out.Warnings = append(out.Warnings, "meta.arm 缺失：因子水平回退为目录名解析")
		}
	}

	var windowStart time.Time
	windowKnown := false
	if metaPresent {
		if ts, ok := segParseTime(meta.WindowStart); ok {
			windowStart = ts
			windowKnown = true
			out.WindowStart = ts.Format(time.RFC3339Nano)
			out.WindowOrigin = "meta"
		}
		if ts, ok := segParseTime(meta.WindowEnd); ok {
			out.WindowEnd = ts.Format(time.RFC3339Nano)
		}
		if windowKnown {
			if end, ok := segParseTime(meta.WindowEnd); ok && end.After(windowStart) {
				seconds := segRound6(end.Sub(windowStart).Seconds())
				out.WindowSeconds = &seconds
			}
		}
	}
	windowStartNS := int64(0)
	if windowKnown {
		windowStartNS = windowStart.UnixNano()
	}

	// Discovered files.
	clientPath, hasClient := segFindFile(cell.Dir, "client_samples")
	warmupPath, hasWarmup := segFindFile(cell.Dir, "warmup_samples")
	segmentsPath, hasSegments := segFindFile(cell.Dir, "server_segments")
	var missing []string
	if !hasClient {
		missing = append(missing, "client_samples.jsonl")
	}
	if !hasWarmup {
		missing = append(missing, "warmup_samples.jsonl")
	}
	if !hasSegments {
		missing = append(missing, "server_segments.jsonl")
	}

	if hasClient {
		out.Files = append(out.Files, filepath.Base(clientPath))
		err := segScanJSONL(clientPath, func(line []byte) error {
			if err := acc.scanClientLine(line, windowStartNS, windowKnown); err != nil {
				acc.noteLineError(clientPath, err)
			}
			return nil
		})
		if err != nil {
			return out, fmt.Errorf("scan %s: %w", clientPath, err)
		}
	}
	// Fallback window origin: the earliest client sample (flagged).
	if !windowKnown && acc.haveFirstAt {
		windowStart = acc.firstClientAt
		windowStartNS = windowStart.UnixNano()
		windowKnown = true
		out.WindowStart = windowStart.Format(time.RFC3339Nano)
		out.WindowOrigin = "first_client_sample_fallback"
		out.Warnings = append(out.Warnings,
			"meta.window_start 缺失：相位原点回退为首个客户端样本时间（相位分桶为近似）")
	}
	if hasWarmup {
		out.Files = append(out.Files, filepath.Base(warmupPath))
		err := segScanJSONL(warmupPath, func(line []byte) error {
			if err := acc.scanWarmupLine(line); err != nil {
				acc.noteLineError(warmupPath, err)
			}
			return nil
		})
		if err != nil {
			return out, fmt.Errorf("scan %s: %w", warmupPath, err)
		}
	}
	if hasSegments {
		out.Files = append(out.Files, filepath.Base(segmentsPath))
		err := segScanJSONL(segmentsPath, func(line []byte) error {
			if err := acc.scanSegmentLine(line, windowStartNS, windowKnown); err != nil {
				acc.noteLineError(segmentsPath, err)
			}
			return nil
		})
		if err != nil {
			return out, fmt.Errorf("scan %s: %w", segmentsPath, err)
		}
	}

	// Clock steps are derived from the server_total labels in id order; the
	// correction shifts the labels of every record carrying an id (segments and
	// client samples with a perf_id). Duration columns come from monotonic
	// clocks and are never corrected.
	steps := segDetectClockSteps(acc.segSamples)
	firstSteadyID, haveSteady := acc.firstSteadyID()
	for i := range steps {
		step := &steps[i]
		switch {
		case !haveSteady:
			step.AppliesToWindow = false
			step.Reason = "first_steady_id_unknown"
		case step.FirstAffectedID <= firstSteadyID:
			// The step predates the steady window: window_start was recorded in
			// the same shifted timebase, so nothing may be corrected.
			step.AppliesToWindow = false
			step.Reason = "pre_window"
		default:
			step.AppliesToWindow = true
		}
	}
	corr := segClockCorrection(steps)
	acc.finalizePhases(windowStartNS, windowKnown, corr)
	acc.finalizeSegments(windowStartNS, windowKnown, corr)
	out.ClockSteps = steps
	var backwardMS float64
	for _, step := range steps {
		if step.AppliesToWindow && step.Ambiguous {
			out.ClockStepsForwardSuspect++
			out.Warnings = append(out.Warnings,
				fmt.Sprintf("作用于窗口的可疑前跳 +%.3fms（id %d→%d）：与停顿/GC 不可区分，未做自动校正（ambiguous）",
					step.DeltaMS, step.BoundaryID, step.FirstAffectedID))
			continue
		}
		if step.AppliesToWindow && !step.Ambiguous {
			out.ClockStepsBackward++
			backwardMS += step.CorrectionMS
			out.ClockStepsApplied++
			continue
		}
		if step.Ambiguous {
			out.ClockStepsForwardSuspect++
			continue
		}
		out.ClockStepsBackward++
	}
	switch {
	case len(steps) == 0:
		out.PhaseMethod = segPhaseMethodWall
		out.ClockStepsNote = "未检测到时钟步进（阈值：回拨 <−50ms，前跳 >+500ms）"
	case out.ClockStepsApplied > 0:
		out.PhaseMethod = segPhaseMethodStepCorrected
		out.ClockStepsNote = fmt.Sprintf("检测到 %d 次回拨中 %d 次作用于窗口（合计 %.3fms）：边界之后的标签已校正，本次移原始 wall 相位保留在 client_query_phases_wall",
			out.ClockStepsBackward+out.ClockStepsApplied*0, out.ClockStepsApplied, backwardMS)
		out.Warnings = append(out.Warnings, fmt.Sprintf("wall clock 回拨 %d 次作用于窗口（合计 %.3fms）：已按 |Δ| 校正标签（phase_method=step_corrected）",
			out.ClockStepsApplied, backwardMS))
	case out.ClockStepsBackward > 0:
		out.PhaseMethod = segPhaseMethodWall
		out.ClockStepsNote = fmt.Sprintf("检测到 %d 次 wall clock 回拨，但均发生在稳态窗口之前（first_steady_id=%d）：window_start 与稳态标签同处位移后的时基，不校正",
			out.ClockStepsBackward, firstSteadyID)
	default:
		out.PhaseMethod = segPhaseMethodWall
		out.ClockStepsNote = "仅检测到可疑前跳：不做校正"
	}
	if out.WindowSeconds != nil {
		out.WindowSecondsCorrected, out.WindowSecondsCorrectedNote =
			segCorrectedWindowSeconds(*out.WindowSeconds, steps)
	}

	out.LineErrors = acc.lineErrors
	if len(acc.warnLines) > 0 {
		out.Warnings = append(out.Warnings, fmt.Sprintf("JSONL 行解析失败 %d 行（示例：%s）",
			acc.lineErrors, strings.Join(acc.warnLines, "; ")))
	}
	for _, name := range segSortedKeys(acc.unknownKinds) {
		out.Warnings = append(out.Warnings, fmt.Sprintf("未知记录 kind=%q ×%d 已忽略", name, acc.unknownKinds[name]))
	}
	for _, name := range segSortedKeys(acc.unknownSegs) {
		out.Warnings = append(out.Warnings, fmt.Sprintf("未知段名 %q ×%d 计数在 segments 之外", name, acc.unknownSegs[name]))
	}
	if len(missing) > 0 {
		out.MissingFiles = missing
		out.Warnings = append(out.Warnings, fmt.Sprintf("缺少文件：%s", strings.Join(missing, ", ")))
	}

	// Client-side classes.
	if agg := acc.clientClasses[segClassQuery]; agg != nil {
		out.ClientQuery = segClassBrief{N: agg.Status.Total, Status: agg.Status, Latency: segLatencyFromMS(agg.DursMS)}
	} else {
		out.ClientQuery.Status = segOutcomeCounts{ByStatus: map[string]int{}}
	}
	for _, class := range segSortedKeys(acc.clientClasses) {
		if class == segClassQuery {
			continue
		}
		agg := acc.clientClasses[class]
		if out.OtherClasses == nil {
			out.OtherClasses = map[string]segClassBrief{}
		}
		out.OtherClasses[class] = segClassBrief{N: agg.Status.Total, Status: agg.Status, Latency: segLatencyFromMS(agg.DursMS)}
	}

	// Phase buckets (client query class).
	if windowKnown {
		out.QueryPhases = segPhaseReports(acc.queryPhases)
		if out.PhaseMethod == segPhaseMethodStepCorrected {
			out.QueryPhasesWall = segPhaseReports(acc.queryPhasesWall)
			out.QueryOutsideWall = acc.queryOutsideWall
		}
	} else {
		out.PhaseNote = "window_start 未知：相位分桶未计算"
	}
	out.QueryOutside = acc.queryOutside

	// Raw segment latencies.
	out.Segments = map[string]segLatency{}
	for _, name := range segSegNames {
		out.Segments[name] = segLatencyFromNS(acc.segDurs[name])
	}
	if windowKnown {
		out.SegmentPhases = map[string][]segPhaseLatency{}
		out.SegmentPreBurst = map[string]segLatency{}
		for _, name := range segSegNames {
			buckets := acc.segPhaseDurs[name]
			list := make([]segPhaseLatency, 0, segPhaseCount)
			for i := range segPhaseCount {
				list = append(list, segPhaseLatency{Label: segPhaseLabels[i], Latency: segLatencyFromNS(buckets[i])})
			}
			out.SegmentPhases[name] = list
			pre := make([]int64, 0, len(buckets[0])+len(buckets[1]))
			pre = append(pre, buckets[0]...)
			pre = append(pre, buckets[1]...)
			out.SegmentPreBurst[name] = segLatencyFromNS(pre)
		}
		outside := 0
		for _, name := range segSegNames {
			if count := acc.segPhaseOut[name]; count > 0 {
				if out.SegmentOutside == nil {
					out.SegmentOutside = map[string]int{}
				}
				out.SegmentOutside[name] = count
				outside += count
			}
		}
		if outside > 0 {
			out.Warnings = append(out.Warnings, fmt.Sprintf(
				"%d 条段记录落在窗口 [window_start, window_start+12s) 之外，相位分桶已忽略（检查时钟偏差或窗口原点）", outside))
		}
	}

	// Paired segments: server_total ∩ below_admit on the same perf_id.
	paired := segPairedSegments{}
	var pgTotals []int64
	{
		var stNS, admitNS, skippedNS, belowNS []int64
		for _, id := range segSortedUint64Keys(acc.byID) {
			agg := acc.byID[id]
			if len(agg.ServerTotalNS) == 0 || len(agg.BelowAdmitNS) == 0 {
				continue
			}
			paired.IDs++
			stNS = append(stNS, agg.ServerTotalNS...)
			belowNS = append(belowNS, agg.BelowAdmitNS...)
			if len(agg.AdmitNS) > 0 {
				paired.IDsWithAdmit++
				admitNS = append(admitNS, agg.AdmitNS...)
			}
			if len(agg.AdmitSkippedNS) > 0 {
				paired.IDsWithAdmitSkipped++
				skippedNS = append(skippedNS, agg.AdmitSkippedNS...)
			}
			if len(agg.AdmitNS) > 0 && len(agg.BelowAdmitNS) > 0 {
				paired.IDsWithAdmitAndBelowAdmit++
			}
		}
		paired.ServerTotal = segLatencyFromNS(stNS)
		paired.Admit = segLatencyFromNS(admitNS)
		paired.AdmitSkipped = segLatencyFromNS(skippedNS)
		paired.BelowAdmit = segLatencyFromNS(belowNS)
	}
	out.Paired = paired

	// PG statement aggregation.
	for _, id := range segSortedUint64Keys(acc.clientIDs) {
		if agg := acc.byID[id]; agg != nil && agg.PGQRecords > 0 {
			pgTotals = append(pgTotals, agg.PGQSumNS)
		}
	}
	out.PgTotal = segLatencyFromNS(pgTotals)

	pgq := segPGQReport{
		Total:            acc.pgqTotal,
		Errors:           acc.pgqErrors,
		DistinctPrefixes: len(acc.pgqPrefixes),
		Latency:          segLatencyFromNS(acc.pgqDurs),
	}
	type prefixRow struct {
		prefix string
		agg    *segPrefixAgg
	}
	rows := make([]prefixRow, 0, len(acc.pgqPrefixes))
	for _, prefix := range segSortedKeys(acc.pgqPrefixes) {
		rows = append(rows, prefixRow{prefix: prefix, agg: acc.pgqPrefixes[prefix]})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].agg.Count != rows[j].agg.Count {
			return rows[i].agg.Count > rows[j].agg.Count
		}
		return rows[i].prefix < rows[j].prefix
	})
	for i, row := range rows {
		if i >= segPGQTopPrefixes {
			break
		}
		lat := segLatencyFromNS(row.agg.DursNS)
		pgq.TopPrefixes = append(pgq.TopPrefixes, segPGQPrefixStat{
			Prefix: row.prefix,
			Count:  row.agg.Count,
			P50MS:  lat.P50MS,
			P95MS:  lat.P95MS,
		})
	}
	out.Pgq = pgq

	// Per-request PG statement counts (base: distinct client perf_ids).
	perReq := segPgPerRequest{ByStatements: map[string]int{}}
	var counts []float64
	var totalStatements int
	for _, id := range segSortedUint64Keys(acc.clientIDs) {
		count := acc.pgqCountID[id]
		perReq.Requests++
		perReq.ByStatements[strconv.Itoa(count)]++
		counts = append(counts, float64(count))
		totalStatements += count
		if count == 0 {
			perReq.RequestsWithoutPGQ++
		}
		if count > perReq.Max {
			perReq.Max = count
		}
	}
	if perReq.Requests > 0 {
		mean := segRound6(float64(totalStatements) / float64(perReq.Requests))
		perReq.Mean = &mean
		sort.Float64s(counts)
		perReq.P50 = segFinitePtr(segPercentile(counts, 50))
		perReq.P95 = segFinitePtr(segPercentile(counts, 95))
	}
	out.PgPerRequest = perReq

	// Warmup: its own section, never merged into the steady state.
	warm := segWarmupReport{
		Note:    "预热样本单独小节，不并入稳态统计",
		ByClass: map[string]segClassBrief{},
	}
	for _, class := range segSortedKeys(acc.warmupClasses) {
		agg := acc.warmupClasses[class]
		brief := segClassBrief{N: agg.Status.Total, Status: agg.Status, Latency: segLatencyFromMS(agg.DursMS)}
		warm.Samples += brief.N
		if class == segClassQuery {
			warm.Query = brief
		}
		warm.ByClass[class] = brief
	}
	if len(warm.ByClass) == 0 {
		warm.ByClass = nil
	}
	out.Warmup = warm

	out.Pairing = segBuildPairing(acc)
	out.Invariants = segBuildInvariants(cell, acc, out.LimiterOn)
	return out, nil
}

// segBuildPairing computes the bilateral unmatched counts, duplicates and
// missing/extra segment counts of one cell.
func segBuildPairing(acc *segArmAcc) segPairing {
	p := segPairing{
		ClientSamples:        acc.clientSamples,
		ClientWithoutPerfID:  acc.clientNoID,
		ClientIDs:            len(acc.clientIDs),
		DuplicateIDs:         map[string]int{},
		MissingSegmentCounts: map[string]int{},
		ExtraSegmentCounts:   map[string]int{},
	}
	clientDup := 0
	for _, id := range segSortedUint64Keys(acc.clientIDs) {
		if acc.clientIDs[id] > 1 {
			clientDup++
		}
	}
	p.DuplicateIDs["client"] = clientDup
	for _, name := range segSegNames {
		switch name {
		case segServerTotal:
			p.DuplicateIDs[name] = 0
			for _, id := range segSortedUint64Keys(acc.byID) {
				if len(acc.byID[id].ServerTotalNS) > 1 {
					p.DuplicateIDs[name]++
				}
			}
		case segAdmit:
			p.DuplicateIDs[name] = 0
			for _, id := range segSortedUint64Keys(acc.byID) {
				if len(acc.byID[id].AdmitNS) > 1 {
					p.DuplicateIDs[name]++
				}
			}
		case segAdmitSkipped:
			p.DuplicateIDs[name] = 0
			for _, id := range segSortedUint64Keys(acc.byID) {
				if len(acc.byID[id].AdmitSkippedNS) > 1 {
					p.DuplicateIDs[name]++
				}
			}
		case segBelowAdmit:
			p.DuplicateIDs[name] = 0
			for _, id := range segSortedUint64Keys(acc.byID) {
				if len(acc.byID[id].BelowAdmitNS) > 1 {
					p.DuplicateIDs[name]++
				}
			}
		}
	}
	// pgq duplicate: the same request carrying the same statement prefix more
	// than once (multiple statements per request are normal and not a
	// duplicate id).
	pgqDup := 0
	for _, id := range segSortedUint64Keys(acc.pgqPrefixByID) {
		for _, count := range acc.pgqPrefixByID[id] {
			if count > 1 {
				pgqDup++
				break
			}
		}
	}
	p.DuplicateIDs[segKindPGQ] = pgqDup

	for _, id := range segSortedUint64Keys(acc.byID) {
		agg := acc.byID[id]
		hasServer := len(agg.ServerTotalNS) > 0
		_, hasClient := acc.clientIDs[id]
		if hasServer {
			p.ServerTotalIDs++
		}
		if hasClient && !hasServer {
			p.ClientWithoutServerTotal++
		}
		if hasServer && !hasClient {
			p.ServerTotalWithoutClient++
		}
		if hasServer && len(agg.BelowAdmitNS) == 0 {
			p.ServerTotalWithoutBelowAdmit++
		}
		if len(agg.BelowAdmitNS) > 0 && !hasServer {
			p.BelowAdmitWithoutServerTotal++
		}
		if hasServer && len(agg.AdmitNS) == 0 && len(agg.AdmitSkippedNS) == 0 && len(agg.BelowAdmitNS) == 0 {
			p.ServerTotalWithoutAdmitOrBelowAdmit++
		}
		if len(agg.AdmitNS) > 0 && !hasServer {
			p.AdmitWithoutServerTotal++
		}
		if len(agg.AdmitSkippedNS) > 0 && !hasServer {
			p.AdmitSkippedWithoutServerTotal++
		}
		if agg.PGQRecords > 0 && !hasClient {
			p.PgqWithoutClient++
		}
	}
	// Client ids with no segment record at all never enter the byID loop.
	for _, id := range segSortedUint64Keys(acc.clientIDs) {
		if _, known := acc.byID[id]; !known {
			p.ClientWithoutServerTotal++
		}
	}
	// Missing: for server_total the domain is the client ids; for the nested
	// segments the domain is the server_total ids.
	p.MissingSegmentCounts[segServerTotal] = p.ClientWithoutServerTotal
	for _, name := range []string{segAdmit, segAdmitSkipped, segBelowAdmit} {
		count := 0
		for _, id := range segSortedUint64Keys(acc.byID) {
			agg := acc.byID[id]
			if len(agg.ServerTotalNS) == 0 {
				continue
			}
			switch name {
			case segAdmit:
				if len(agg.AdmitNS) == 0 {
					count++
				}
			case segAdmitSkipped:
				if len(agg.AdmitSkippedNS) == 0 {
					count++
				}
			case segBelowAdmit:
				if len(agg.BelowAdmitNS) == 0 {
					count++
				}
			}
		}
		p.MissingSegmentCounts[name] = count
	}
	p.ExtraSegmentCounts[segServerTotal] = p.ServerTotalWithoutClient
	p.ExtraSegmentCounts[segAdmit] = p.AdmitWithoutServerTotal
	p.ExtraSegmentCounts[segAdmitSkipped] = p.AdmitSkippedWithoutServerTotal
	p.ExtraSegmentCounts[segBelowAdmit] = p.BelowAdmitWithoutServerTotal
	return p
}

// segBuildInvariants evaluates the frozen invariants for one cell. Violations
// are listed (never hidden); the per-cell list is capped but the exact counts
// are kept, and the truncation is flagged.
func segBuildInvariants(cell segArmInput, acc *segArmAcc, limiterOn *bool) segInvariants {
	const (
		checkShortfall = "server_total_ge_admit_plus_below_admit(±0.05ms)"
		checkPgq       = "sum(pgq)_le_below_admit_plus_1ms"
		checkSkipped   = "admit_skipped_only_when_limiter_off"
	)
	inv := segInvariants{ViolationCounts: map[string]int{}}
	checks := map[string]*segInvariantCheck{
		checkShortfall: {Name: checkShortfall},
		checkPgq:       {Name: checkPgq},
		checkSkipped:   {Name: checkSkipped},
	}
	add := func(kind string, id uint64, detail string) {
		checks[kind].Violations++
		inv.ViolationCounts[kind]++
		if len(inv.Violations) < segMaxArmViolations {
			inv.Violations = append(inv.Violations, segViolation{
				Repeat: cell.Repeat, Arm: cell.Arm, ID: id, Kind: kind, Detail: detail,
			})
		} else {
			inv.Truncated = true
		}
	}

	for _, id := range segSortedUint64Keys(acc.byID) {
		agg := acc.byID[id]
		if len(agg.ServerTotalNS) > 0 &&
			(len(agg.AdmitNS) > 0 || len(agg.AdmitSkippedNS) > 0 || len(agg.BelowAdmitNS) > 0) {
			checks[checkShortfall].Checked++
			required := segSumNS(agg.AdmitNS) + segSumNS(agg.AdmitSkippedNS) + segSumNS(agg.BelowAdmitNS)
			got := segMaxNS(agg.ServerTotalNS)
			if got+segShortfallToleranceNS < required {
				add(checkShortfall, id, fmt.Sprintf("server_total=%dns < admit+below=%dns（差 %dns）",
					got, required, required-got))
			}
		}
		if len(agg.BelowAdmitNS) > 0 {
			checks[checkPgq].Checked++
			limit := segMaxNS(agg.BelowAdmitNS) + segPgqSlackNS
			if agg.PGQSumNS > limit {
				add(checkPgq, id, fmt.Sprintf("sum(pgq)=%dns > below_admit+1ms=%dns", agg.PGQSumNS, limit))
			}
		}
		if len(agg.AdmitSkippedNS) > 0 {
			if limiterOn == nil {
				checks[checkSkipped].Skipped = true
				checks[checkSkipped].SkipReason = "limiter_on 未知（meta 缺失）"
			} else {
				checks[checkSkipped].Checked += len(agg.AdmitSkippedNS)
				if *limiterOn {
					add(checkSkipped, id, fmt.Sprintf("limiter_on=true 时出现 admit_skipped ×%d", len(agg.AdmitSkippedNS)))
				}
			}
		}
	}
	for _, name := range []string{checkShortfall, checkPgq, checkSkipped} {
		inv.Checks = append(inv.Checks, *checks[name])
	}
	return inv
}

// --------------------------------------------------------- cross-cell rollups

// segClockStepRollups collects the per-arm clock-step view of the summary.
func segClockStepRollups(arms []segArmReport) []segClockStepRollup {
	out := make([]segClockStepRollup, 0, len(arms))
	for _, arm := range arms {
		entry := segClockStepRollup{
			Repeat:              arm.Repeat,
			Arm:                 arm.Arm,
			BackwardSteps:       arm.ClockStepsBackward,
			AppliedSteps:        arm.ClockStepsApplied,
			ForwardSuspectSteps: arm.ClockStepsForwardSuspect,
			PhaseMethod:         arm.PhaseMethod,
			Steps:               arm.ClockSteps,
			Note:                arm.ClockStepsNote,
		}
		if entry.Steps == nil {
			entry.Steps = []segClockStep{}
		}
		out = append(out, entry)
	}
	return out
}

// segMetricDef describes one factorial metric.
type segMetricDef struct {
	Key        string
	Definition string
	Extract    func(segArmReport) *float64
}

var segMetricDefs = []segMetricDef{
	{"client_query_p95_ms", "稳态客户端 query 类 p95（含全部状态码与错误响应）",
		func(a segArmReport) *float64 { return a.ClientQuery.Latency.P95MS }},
	{"server_total_p95_ms", "server_total 段 p95（全部 ID）",
		func(a segArmReport) *float64 { return a.Segments[segServerTotal].P95MS }},
	{"admit_p95_ms", "admit 段 p95",
		func(a segArmReport) *float64 { return a.Segments[segAdmit].P95MS }},
	{"below_admit_p95_ms", "below_admit 段 p95",
		func(a segArmReport) *float64 { return a.Segments[segBelowAdmit].P95MS }},
	{"pg_total_p95_ms", "每请求 PG 语句耗时合计的 p95（仅含 ≥1 条 pgq 的请求）",
		func(a segArmReport) *float64 { return a.PgTotal.P95MS }},
}

// segRepeatSpread builds the per-arm cross-repeat p95 spread for every
// factorial metric.
func segRepeatSpread(arms []segArmReport) map[string]map[string]segSeries {
	byArm := map[string]map[string][]segRepeatValue{}
	seen := map[string]map[int]bool{}
	for _, arm := range arms {
		for _, def := range segMetricDefs {
			value := def.Extract(arm)
			if value == nil {
				continue
			}
			if byArm[arm.Arm] == nil {
				byArm[arm.Arm] = map[string][]segRepeatValue{}
			}
			if seen[arm.Arm] == nil {
				seen[arm.Arm] = map[int]bool{}
			}
			key := arm.Arm + "|" + def.Key
			if seen[key] == nil {
				seen[key] = map[int]bool{}
			}
			if seen[key][arm.Repeat] {
				continue
			}
			seen[key][arm.Repeat] = true
			byArm[arm.Arm][def.Key] = append(byArm[arm.Arm][def.Key], segRepeatValue{Repeat: arm.Repeat, Value: *value})
		}
	}
	out := map[string]map[string]segSeries{}
	for _, armName := range segSortedKeys(byArm) {
		out[armName] = map[string]segSeries{}
		for _, def := range segMetricDefs {
			pairs := byArm[armName][def.Key]
			if len(pairs) == 0 {
				continue
			}
			out[armName][def.Key] = segSeriesFrom(pairs, nil)
		}
	}
	return out
}

// segFactorEffects computes the factorial contrasts per metric from the
// per-repeat arm levels. Every entry is a difference on one ID set; nothing is
// added up across the nested timing vocabulary.
func segFactorEffects(arms []segArmReport) []segMetricEffects {
	type cellKey struct {
		repeat int
		key    string
	}
	levels := map[cellKey]segArmReport{}
	repeatsSet := map[int]bool{}
	for _, arm := range arms {
		key, ok := segFactorKey(arm)
		if !ok {
			continue
		}
		ck := cellKey{repeat: arm.Repeat, key: key}
		if _, exists := levels[ck]; !exists {
			levels[ck] = arm
			repeatsSet[arm.Repeat] = true
		}
	}
	repeats := make([]int, 0, len(repeatsSet))
	for r := range repeatsSet {
		repeats = append(repeats, r)
	}
	sort.Ints(repeats)

	var out []segMetricEffects
	for _, def := range segMetricDefs {
		metric := segMetricEffects{
			Metric:     def.Key,
			Definition: def.Definition,
			Levels:     map[string]segSeries{},
			Effects:    map[string]segSeries{},
		}
		for _, key := range []string{"R0G0", "R1G0", "R0G1", "R1G1"} {
			var pairs []segRepeatValue
			var missing []int
			for _, repeat := range repeats {
				arm, ok := levels[cellKey{repeat: repeat, key: key}]
				if !ok {
					missing = append(missing, repeat)
					continue
				}
				value := def.Extract(arm)
				if value == nil {
					missing = append(missing, repeat)
					continue
				}
				pairs = append(pairs, segRepeatValue{Repeat: repeat, Value: *value})
			}
			metric.Levels[key] = segSeriesFrom(pairs, missing)
		}
		effect := func(name string, required []string, f func(v map[string]float64) (float64, bool)) {
			var pairs []segRepeatValue
			var missing []int
			for _, repeat := range repeats {
				values := map[string]float64{}
				complete := true
				for _, key := range required {
					arm, ok := levels[cellKey{repeat: repeat, key: key}]
					if !ok {
						complete = false
						break
					}
					value := def.Extract(arm)
					if value == nil {
						complete = false
						break
					}
					values[key] = *value
				}
				if !complete {
					missing = append(missing, repeat)
					continue
				}
				v, ok := f(values)
				if !ok {
					missing = append(missing, repeat)
					continue
				}
				pairs = append(pairs, segRepeatValue{Repeat: repeat, Value: v})
			}
			metric.Effects[name] = segSeriesFrom(pairs, missing)
		}
		allFour := []string{"R0G0", "R1G0", "R0G1", "R1G1"}
		effect("R_at_G0", []string{"R0G0", "R1G0"}, func(v map[string]float64) (float64, bool) { return v["R1G0"] - v["R0G0"], true })
		effect("R_at_G1", []string{"R0G1", "R1G1"}, func(v map[string]float64) (float64, bool) { return v["R1G1"] - v["R0G1"], true })
		effect("G_at_R0", []string{"R0G0", "R0G1"}, func(v map[string]float64) (float64, bool) { return v["R0G1"] - v["R0G0"], true })
		effect("G_at_R1", []string{"R1G0", "R1G1"}, func(v map[string]float64) (float64, bool) { return v["R1G1"] - v["R1G0"], true })
		effect("interaction", allFour, func(v map[string]float64) (float64, bool) {
			return (v["R1G1"] - v["R0G1"]) - (v["R1G0"] - v["R0G0"]), true
		})
		effect("total_diff_R0G0_to_R1G1", []string{"R0G0", "R1G1"}, func(v map[string]float64) (float64, bool) { return v["R1G1"] - v["R0G0"], true })
		out = append(out, metric)
	}
	return out
}

// segOverheadSide aggregates one side of the overhead comparison.
func segOverheadSideFor(arms []segArmReport) segOverheadSide {
	side := segOverheadSide{}
	var p50s, p95s, p99s []segRepeatValue
	for _, arm := range arms {
		side.NPerRepeat = append(side.NPerRepeat, segRepeatValue{Repeat: arm.Repeat, Value: float64(arm.ClientQuery.N)})
		side.NTotal += arm.ClientQuery.N
		if arm.ClientQuery.Latency.P50MS != nil {
			p50s = append(p50s, segRepeatValue{Repeat: arm.Repeat, Value: *arm.ClientQuery.Latency.P50MS})
		}
		if arm.ClientQuery.Latency.P95MS != nil {
			p95s = append(p95s, segRepeatValue{Repeat: arm.Repeat, Value: *arm.ClientQuery.Latency.P95MS})
		}
		if arm.ClientQuery.Latency.P99MS != nil {
			p99s = append(p99s, segRepeatValue{Repeat: arm.Repeat, Value: *arm.ClientQuery.Latency.P99MS})
		}
	}
	side.P50 = segSeriesFrom(p50s, nil)
	side.P95 = segSeriesFrom(p95s, nil)
	side.P99 = segSeriesFrom(p99s, nil)
	return side
}

// segIsNocollArm recognizes the collection-off overhead variant either by the
// frozen ":nocoll" token or by meta.arm.collect == false.
func segIsNocollArm(arm segArmReport) bool {
	if strings.Contains(arm.Arm, "nocoll") {
		return true
	}
	return arm.Collect != nil && !*arm.Collect
}

// segOverheadCompareFor pairs R1G1 vs R1G1:nocoll (client query latency).
// Deltas are formed only inside a repeat that holds both cells; when nocoll
// lives in its own repeat (repeat0), the steady R1G1 cells of the other
// repeats are reported as a reference, never mixed into the delta.
func segOverheadCompareFor(arms []segArmReport) segOverheadCompare {
	out := segOverheadCompare{
		Base:    "R1G1",
		Variant: "R1G1:nocoll",
		Note:    "差值对比（禁止相加/占比证明）：同一重复内 nocoll 与 steady 的客户端 query 分位数差值；无配对重复则不产出差值。",
	}
	baseSteady := map[int]segArmReport{}
	var steadyArms []segArmReport
	var nocollArms []segArmReport
	for _, arm := range arms {
		if segIsNocollArm(arm) {
			nocollArms = append(nocollArms, arm)
			continue
		}
		steady := arm.Arm == "R1G1"
		if !steady && arm.LimiterOn != nil && arm.BackgroundOn != nil {
			steady = *arm.LimiterOn && *arm.BackgroundOn
		}
		if steady {
			if _, exists := baseSteady[arm.Repeat]; !exists {
				baseSteady[arm.Repeat] = arm
			}
			steadyArms = append(steadyArms, arm)
		}
	}
	if len(nocollArms) == 0 {
		return out
	}
	out.Present = true
	var pairedBase, pairedVariant []segArmReport
	type segOverheadDelta struct {
		repeat        int
		p50, p95, p99 *float64
	}
	var deltas []segOverheadDelta
	seen := map[int]bool{}
	for _, variant := range nocollArms {
		if seen[variant.Repeat] {
			continue
		}
		seen[variant.Repeat] = true
		base, ok := baseSteady[variant.Repeat]
		if !ok {
			continue
		}
		out.PairedRepeats = append(out.PairedRepeats, variant.Repeat)
		pairedBase = append(pairedBase, base)
		pairedVariant = append(pairedVariant, variant)
		delta := segOverheadDelta{repeat: variant.Repeat}
		if variant.ClientQuery.Latency.P50MS != nil && base.ClientQuery.Latency.P50MS != nil {
			delta.p50 = segFinitePtr(*variant.ClientQuery.Latency.P50MS - *base.ClientQuery.Latency.P50MS)
		}
		if variant.ClientQuery.Latency.P95MS != nil && base.ClientQuery.Latency.P95MS != nil {
			delta.p95 = segFinitePtr(*variant.ClientQuery.Latency.P95MS - *base.ClientQuery.Latency.P95MS)
		}
		if variant.ClientQuery.Latency.P99MS != nil && base.ClientQuery.Latency.P99MS != nil {
			delta.p99 = segFinitePtr(*variant.ClientQuery.Latency.P99MS - *base.ClientQuery.Latency.P99MS)
		}
		deltas = append(deltas, delta)
	}
	sort.Ints(out.PairedRepeats)
	out.VariantSide = segOverheadSideFor(pairedVariant)
	out.BaseSide = segOverheadSideFor(pairedBase)
	if len(pairedVariant) == 0 {
		out.VariantSide = segOverheadSideFor(nocollArms)
		out.BaseSide = segOverheadSideFor(nil)
	}
	var d50, d95, d99 []segRepeatValue
	for _, delta := range deltas {
		if delta.p50 != nil {
			d50 = append(d50, segRepeatValue{Repeat: delta.repeat, Value: *delta.p50})
		}
		if delta.p95 != nil {
			d95 = append(d95, segRepeatValue{Repeat: delta.repeat, Value: *delta.p95})
		}
		if delta.p99 != nil {
			d99 = append(d99, segRepeatValue{Repeat: delta.repeat, Value: *delta.p99})
		}
	}
	out.DeltaP50 = segSeriesFrom(d50, nil)
	out.DeltaP95 = segSeriesFrom(d95, nil)
	out.DeltaP99 = segSeriesFrom(d99, nil)
	if len(steadyArms) > 0 && len(out.PairedRepeats) == 0 {
		side := segOverheadSideFor(steadyArms)
		out.Reference = &side
	}
	return out
}

// ------------------------------------------------------------ candidate rules

const (
	segRuleSatisfied     = "满足"
	segRuleViolated      = "不满足"
	segRuleInsufficient  = "证据不足"
	segRuleNotApplicable = "不适用"

	segRuleShareFloorMS = 0.05
)

func segFindMetric(effects []segMetricEffects, key string) *segMetricEffects {
	for i := range effects {
		if effects[i].Metric == key {
			return &effects[i]
		}
	}
	return nil
}

// segRuleAdmitLatency checks "admit 正常态 p95 ≤2ms" (candidate rule, not a
// verdict) on the limiter-on / background-off cell, pre-burst phase only.
func segRuleAdmitLatency(arms []segArmReport) segRuleCheck {
	check := segRuleCheck{
		Rule: "候选规则，非裁决：正常态（窗口 [0s,8s)，排除 burst 档）admit p95 ≤ 2ms",
		Note: "正常态口径取相位分桶 [0s,4s)+[4s,8s)；[8s,12s) 与固定 burst 重叠，故排除。评估臂：limiter_on=true/background_on=false。",
	}
	var cells []segArmReport
	for _, arm := range arms {
		if strings.Contains(arm.Arm, "nocoll") {
			continue
		}
		if arm.LimiterOn != nil && arm.BackgroundOn != nil && *arm.LimiterOn && !*arm.BackgroundOn {
			cells = append(cells, arm)
		}
	}
	if len(cells) == 0 {
		check.Status = segRuleInsufficient
		check.Observed = "未找到 limiter_on=true/background_on=false 的臂"
		return check
	}
	var pairs []segRepeatValue
	for _, arm := range cells {
		lat, ok := arm.SegmentPreBurst[segAdmit]
		if !ok || lat.P95MS == nil {
			continue
		}
		pairs = append(pairs, segRepeatValue{Repeat: arm.Repeat, Value: *lat.P95MS})
	}
	check.PerRepeat = pairs
	if len(pairs) == 0 {
		check.Status = segRuleInsufficient
		check.Observed = "该臂无正常态 admit 样本（可能 limiter 关闭或未采集）"
		return check
	}
	check.MedianMS = segFinitePtr(segPercentile(segSortedValues(pairs), 50))
	if check.MedianMS == nil {
		check.Status = segRuleInsufficient
		return check
	}
	worst := 0.0
	worstRepeat := pairs[0].Repeat
	for _, p := range pairs {
		if p.Value > worst {
			worst = p.Value
			worstRepeat = p.Repeat
		}
	}
	if worst <= 2 {
		check.Status = segRuleSatisfied
	} else {
		check.Status = segRuleViolated
	}
	check.Observed = fmt.Sprintf("跨重复中位数 p95=%.3fms（逐重复最大值 %.3fms @repeat%d，n=%d 个重复）",
		*check.MedianMS, worst, worstRepeat, len(pairs))
	return check
}

// segRuleREffect checks "|R 效应| ≥10ms" (candidate rule, not a verdict) on
// the client query p95 metric.
func segRuleREffect(effects []segMetricEffects) segRuleCheck {
	check := segRuleCheck{
		Rule: "候选规则，非裁决：客户端 query p95 的 |R 效应| ≥ 10ms（R@G0 与 R@G1）",
		Note: "R 效应是同一指标、同一重复内的臂间差值（差值对比（禁止相加/占比证明））；两个水平都可用的重复才计入。",
	}
	metric := segFindMetric(effects, "client_query_p95_ms")
	if metric == nil {
		check.Status = segRuleInsufficient
		check.Observed = "缺少 client_query_p95_ms 因子效应"
		return check
	}
	var available []string
	for _, name := range []string{"R_at_G0", "R_at_G1"} {
		if series, ok := metric.Effects[name]; ok && series.MedianMS != nil {
			available = append(available, fmt.Sprintf("%s 中位数=%+.3fms", name, *series.MedianMS))
		}
	}
	if len(available) == 0 {
		check.Status = segRuleInsufficient
		check.Observed = "R@G0/R@G1 均无可用重复（臂或分位数缺失）"
		return check
	}
	okAll := true
	worst := math.Inf(1)
	for _, name := range []string{"R_at_G0", "R_at_G1"} {
		series, ok := metric.Effects[name]
		if !ok || series.MedianMS == nil {
			continue
		}
		if v := math.Abs(*series.MedianMS); v < 10 {
			okAll = false
			if v < worst {
				worst = v
			}
		}
	}
	if okAll {
		check.Status = segRuleSatisfied
	} else {
		check.Status = segRuleViolated
	}
	check.Observed = strings.Join(available, "；")
	return check
}

// segRuleREffectShare checks "R 效应对总差 ≥50%" (candidate rule, not a
// verdict) on the client query p95 metric.
func segRuleREffectShare(effects []segMetricEffects) segRuleCheck {
	check := segRuleCheck{
		Rule: "候选规则，非裁决：客户端 query p95 的 R 效应 @G1 占总差（R1G1−R0G0）≥50%",
		Note: "占比仅作候选规则标注，不构成因果或收益证明；总差 ≤0.05ms 的重复不计入。",
	}
	metric := segFindMetric(effects, "client_query_p95_ms")
	if metric == nil {
		check.Status = segRuleInsufficient
		check.Observed = "缺少 client_query_p95_ms 因子效应"
		return check
	}
	rEffect, okR := metric.Effects["R_at_G1"]
	total, okT := metric.Effects["total_diff_R0G0_to_R1G1"]
	if !okR || !okT || rEffect.MedianMS == nil || total.MedianMS == nil {
		check.Status = segRuleInsufficient
		check.Observed = "R@G1 或总差的重复级数值缺失"
		return check
	}
	byRepeat := map[int]float64{}
	for _, p := range rEffect.PerRepeat {
		byRepeat[p.Repeat] = p.Value
	}
	var pairs []segRepeatValue
	for _, p := range total.PerRepeat {
		if math.Abs(p.Value) < segRuleShareFloorMS {
			continue
		}
		effect, ok := byRepeat[p.Repeat]
		if !ok {
			continue
		}
		pairs = append(pairs, segRepeatValue{Repeat: p.Repeat, Value: effect / p.Value})
	}
	check.PerRepeat = pairs
	if len(pairs) == 0 {
		check.Status = segRuleInsufficient
		check.Observed = fmt.Sprintf("总差（R1G1−R0G0）在全部重复内都 < %.2fms", segRuleShareFloorMS)
		return check
	}
	vals := segSortedValues(pairs)
	check.MedianMS = segFinitePtr(segPercentile(vals, 50))
	if check.MedianMS == nil {
		check.Status = segRuleInsufficient
		return check
	}
	if *check.MedianMS >= 0.5 {
		check.Status = segRuleSatisfied
	} else {
		check.Status = segRuleViolated
	}
	check.Observed = fmt.Sprintf("跨重复中位数占比=%.1f%%（逐重复 n=%d）", *check.MedianMS*100, len(pairs))
	return check
}

func segSortedValues(pairs []segRepeatValue) []float64 {
	vals := make([]float64, 0, len(pairs))
	for _, p := range pairs {
		vals = append(vals, p.Value)
	}
	sort.Float64s(vals)
	return vals
}

// --------------------------------------------------------------- rollups

// segSumPairing adds the per-cell pairing figures into the report totals.
func segSumPairing(arms []segArmReport) segPairing {
	total := segPairing{
		DuplicateIDs:         map[string]int{},
		MissingSegmentCounts: map[string]int{},
		ExtraSegmentCounts:   map[string]int{},
	}
	for _, arm := range arms {
		p := arm.Pairing
		total.ClientSamples += p.ClientSamples
		total.ClientWithoutPerfID += p.ClientWithoutPerfID
		total.ClientIDs += p.ClientIDs
		total.ServerTotalIDs += p.ServerTotalIDs
		total.ClientWithoutServerTotal += p.ClientWithoutServerTotal
		total.ServerTotalWithoutClient += p.ServerTotalWithoutClient
		total.ServerTotalWithoutBelowAdmit += p.ServerTotalWithoutBelowAdmit
		total.BelowAdmitWithoutServerTotal += p.BelowAdmitWithoutServerTotal
		total.ServerTotalWithoutAdmitOrBelowAdmit += p.ServerTotalWithoutAdmitOrBelowAdmit
		total.AdmitWithoutServerTotal += p.AdmitWithoutServerTotal
		total.AdmitSkippedWithoutServerTotal += p.AdmitSkippedWithoutServerTotal
		total.PgqWithoutClient += p.PgqWithoutClient
		for key, value := range p.DuplicateIDs {
			total.DuplicateIDs[key] += value
		}
		for key, value := range p.MissingSegmentCounts {
			total.MissingSegmentCounts[key] += value
		}
		for key, value := range p.ExtraSegmentCounts {
			total.ExtraSegmentCounts[key] += value
		}
	}
	return total
}

// segMergeInvariants merges the per-cell invariant evaluations. Violations are
// listed, never hidden: the list is capped for report size and the exact
// per-kind counts stay intact, with the truncation flagged.
func segMergeInvariants(arms []segArmReport) segInvariants {
	merged := segInvariants{ViolationCounts: map[string]int{}}
	checks := map[string]*segInvariantCheck{}
	var order []string
	for _, arm := range arms {
		for _, check := range arm.Invariants.Checks {
			entry := checks[check.Name]
			if entry == nil {
				entry = &segInvariantCheck{Name: check.Name}
				checks[check.Name] = entry
				order = append(order, check.Name)
			}
			entry.Checked += check.Checked
			entry.Violations += check.Violations
			if check.Skipped {
				if entry.Checked == 0 {
					entry.Skipped = true
					entry.SkipReason = check.SkipReason
				} else if entry.SkipReason == "" {
					entry.SkipReason = fmt.Sprintf("%d 个单元跳过：%s", 1, check.SkipReason)
				}
			}
		}
		for _, violation := range arm.Invariants.Violations {
			if len(merged.Violations) < segMaxListedViolations {
				merged.Violations = append(merged.Violations, violation)
			} else {
				merged.Truncated = true
			}
		}
		if arm.Invariants.Truncated {
			merged.Truncated = true
		}
		for _, name := range segSortedKeys(arm.Invariants.ViolationCounts) {
			merged.ViolationCounts[name] += arm.Invariants.ViolationCounts[name]
		}
	}
	for _, name := range order {
		if merged.ViolationCounts[name] > len(merged.Violations) {
			merged.Truncated = true
		}
		if merged.ViolationCounts[name] == 0 && !checks[name].Skipped {
			// still listed: an evaluated-and-clean check stays visible.
		}
		merged.Checks = append(merged.Checks, *checks[name])
	}
	if len(merged.Checks) == 0 {
		merged.Checks = []segInvariantCheck{}
	}
	if len(merged.Violations) == 0 {
		merged.Violations = nil
	}
	return merged
}

// segCompletenessFor summarizes coverage: which repeats and arms were
// analyzed and what is missing.
func segCompletenessFor(arms []segArmReport) segCompleteness {
	out := segCompleteness{
		ExpectedSteadyArms: []string{"R0G0", "R1G0", "R0G1", "R1G1"},
		ArmsByRepeat:       map[string][]string{},
	}
	out.Cells = len(arms)
	byRepeat := map[int][]string{}
	for _, arm := range arms {
		key := strconv.Itoa(arm.Repeat)
		byRepeat[arm.Repeat] = append(byRepeat[arm.Repeat], arm.Arm)
		out.LineErrors += arm.LineErrors
		if !arm.MetaPresent {
			out.MetaMissing = append(out.MetaMissing, fmt.Sprintf("repeat%d/%s", arm.Repeat, arm.Arm))
		}
		if strings.Contains(arm.Arm, "nocoll") {
			out.NocollArms = append(out.NocollArms, fmt.Sprintf("repeat%d/%s", arm.Repeat, arm.Arm))
		}
		if len(arm.MissingFiles) > 0 {
			if out.FilesMissing == nil {
				out.FilesMissing = map[string][]string{}
			}
			out.FilesMissing["repeat"+key+"/"+arm.Arm] = arm.MissingFiles
		}
	}
	repeats := make([]int, 0, len(byRepeat))
	for repeat := range byRepeat {
		repeats = append(repeats, repeat)
	}
	sort.Ints(repeats)
	for _, repeat := range repeats {
		names := byRepeat[repeat]
		sort.Strings(names)
		out.Repeats = append(out.Repeats, repeat)
		out.ArmsByRepeat[strconv.Itoa(repeat)] = names
		hasNocoll := false
		overheadOnly := len(names) > 0
		for _, name := range names {
			if strings.Contains(name, "nocoll") {
				hasNocoll = true
				continue
			}
			if name != "R1G1" {
				overheadOnly = false
			}
		}
		overheadOnly = overheadOnly && hasNocoll
		if overheadOnly {
			out.OverheadOnlyRepeat = append(out.OverheadOnlyRepeat, repeat)
			continue
		}
		if len(names) == 0 {
			continue
		}
		var missing []string
		for _, expected := range out.ExpectedSteadyArms {
			found := false
			for _, name := range names {
				if name == expected {
					found = true
					break
				}
			}
			if !found {
				missing = append(missing, expected)
			}
		}
		if len(missing) > 0 {
			if out.MissingSteadyArms == nil {
				out.MissingSteadyArms = map[string][]string{}
			}
			out.MissingSteadyArms[strconv.Itoa(repeat)] = missing
		}
	}
	return out
}

// segAnalyzeRoot analyzes one evidence root and returns the stable summary
// model (summary.json is its JSON encoding).
func segAnalyzeRoot(root string) (*segReport, error) {
	cells, warnings, err := segDiscoverCells(root)
	if err != nil {
		return nil, err
	}
	if len(cells) == 0 {
		return nil, fmt.Errorf("no arm directories discovered under %s", root)
	}
	rep := &segReport{
		SchemaVersion: segSchemaVersion,
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339Nano),
		Root:          root,
		FactorDefinition: "R = limiter_on（限流开关）；G = background_on（后台负载开关）；" +
			"R1G1:nocoll = collect=false 的开销对照臂（不进因子对比）。",
		FactorDisclaimer: "差值对比（禁止相加/占比证明）：全部因子效应都是同一指标、同一 ID 集合上的臂间差值；分位数不可加，占比不构成因果或收益证明。",
		Discipline:       append([]string(nil), segDiscipline...),
		Limitations:      append([]string(nil), segLimitations...),
		Warnings:         warnings,
	}
	if rep.Warnings == nil {
		rep.Warnings = []string{}
	}
	for _, cell := range cells {
		arm, err := segAnalyzeCell(cell)
		if err != nil {
			return nil, err
		}
		rep.Arms = append(rep.Arms, arm)
	}
	rep.RepeatSpread = segRepeatSpread(rep.Arms)
	rep.ClockSteps = segClockStepRollups(rep.Arms)
	rep.FactorEffects = segFactorEffects(rep.Arms)
	rep.OverheadCompare = segOverheadCompareFor(rep.Arms)
	rep.CandidateRules = []segRuleCheck{
		segRuleAdmitLatency(rep.Arms),
		segRuleREffect(rep.FactorEffects),
		segRuleREffectShare(rep.FactorEffects),
	}
	rep.PairingTotals = segSumPairing(rep.Arms)
	rep.Invariants = segMergeInvariants(rep.Arms)
	rep.Completeness = segCompletenessFor(rep.Arms)
	return rep, nil
}

// ---------------------------------------------------------- markdown report

func segMDMS(p *float64) string {
	if p == nil {
		return "—"
	}
	return strconv.FormatFloat(*p, 'f', 3, 64)
}

func segMDTable(b *strings.Builder, headers []string, rows [][]string) {
	b.WriteString("| " + strings.Join(headers, " | ") + " |\n")
	sep := make([]string, len(headers))
	for i := range sep {
		sep[i] = "---"
	}
	b.WriteString("| " + strings.Join(sep, " | ") + " |\n")
	for _, row := range rows {
		b.WriteString("| " + strings.Join(row, " | ") + " |\n")
	}
	b.WriteString("\n")
}

// segMDMap renders a string-keyed count map deterministically.
func segMDMap(m map[string]int) string {
	keys := segSortedKeys(m)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", key, m[key]))
	}
	return strings.Join(parts, " ")
}

func segMDStatus(c segOutcomeCounts) string {
	parts := []string{fmt.Sprintf("total=%d", c.Total)}
	for _, key := range segSortedKeys(c.ByStatus) {
		parts = append(parts, fmt.Sprintf("%s×%d", key, c.ByStatus[key]))
	}
	parts = append(parts, fmt.Sprintf("2xx=%d/非2xx=%d/transport=%d/no_status=%d/err=%d",
		c.OK2xx, c.Non2xx, c.Transport, c.NoStatus, c.Errors))
	return strings.Join(parts, " ")
}

func segMDSeries(s segSeries) string {
	if len(s.PerRepeat) == 0 {
		if len(s.MissingRepeats) > 0 {
			return fmt.Sprintf("无可用重复（缺失 %v）", s.MissingRepeats)
		}
		return "—"
	}
	parts := make([]string, 0, len(s.PerRepeat))
	for _, p := range s.PerRepeat {
		parts = append(parts, fmt.Sprintf("r%d=%+.3f", p.Repeat, p.Value))
	}
	return fmt.Sprintf("%s；中位数=%s；极差=[%s, %s]",
		strings.Join(parts, " "), segMDMS(s.MedianMS), segMDMS(s.MinMS), segMDMS(s.MaxMS))
}

// segRenderMarkdown renders the human-readable summary.
func segRenderMarkdown(rep *segReport) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# 分段正常查询批次 离线统计（summary.md）\n\n")
	fmt.Fprintf(&b, "- 根目录：`%s`\n- 生成时间：%s\n- schema_version：%d\n- 单元数：%d\n\n",
		rep.Root, rep.GeneratedAt, rep.SchemaVersion, len(rep.Arms))

	b.WriteString("## 0. 口径与纪律（必读）\n\n")
	for _, line := range rep.Discipline {
		fmt.Fprintf(&b, "- %s\n", line)
	}
	b.WriteString("\n限制：\n")
	for _, line := range rep.Limitations {
		fmt.Fprintf(&b, "- %s\n", line)
	}
	b.WriteString("\n顶层字段（summary.json）：`")
	b.WriteString(strings.Join(segTopLevelFields, "`, `"))
	b.WriteString("`\n\n")
	fmt.Fprintf(&b, "因子定义：%s\n\n", rep.FactorDefinition)
	fmt.Fprintf(&b, "因子对比口径：%s\n\n", rep.FactorDisclaimer)

	b.WriteString("## 1. 覆盖率与完整性\n\n")
	comp := rep.Completeness
	fmt.Fprintf(&b, "- 重复：%v；单元：%d；行解析失败：%d\n", comp.Repeats, comp.Cells, comp.LineErrors)
	for _, repeat := range segSortedKeys(comp.ArmsByRepeat) {
		fmt.Fprintf(&b, "- repeat%s 臂：%s\n", repeat, strings.Join(comp.ArmsByRepeat[repeat], ", "))
	}
	if len(comp.OverheadOnlyRepeat) > 0 {
		repeats := make([]string, 0, len(comp.OverheadOnlyRepeat))
		for _, r := range comp.OverheadOnlyRepeat {
			repeats = append(repeats, strconv.Itoa(r))
		}
		fmt.Fprintf(&b, "- 开销对照重复（仅 R1G1/R1G1:nocoll，不计为稳态缺口）：%s\n", strings.Join(repeats, ", "))
	}
	for _, repeat := range segSortedKeys(comp.MissingSteadyArms) {
		fmt.Fprintf(&b, "- repeat%s 缺失稳态臂：%s\n", repeat, strings.Join(comp.MissingSteadyArms[repeat], ", "))
	}
	if len(comp.MetaMissing) > 0 {
		fmt.Fprintf(&b, "- meta.json 缺失：%s\n", strings.Join(comp.MetaMissing, ", "))
	}
	for _, key := range segSortedKeys(comp.FilesMissing) {
		fmt.Fprintf(&b, "- 缺文件 %s：%s\n", key, strings.Join(comp.FilesMissing[key], ", "))
	}
	if len(rep.Warnings) > 0 {
		b.WriteString("\n警告：\n")
		for _, warning := range rep.Warnings {
			fmt.Fprintf(&b, "- %s\n", warning)
		}
	}
	b.WriteString("\n")

	b.WriteString("相位口径：相位分桶基于**校正后标签**（详见 §11 时钟步进与校正）；phase_method=wall 表示未检测到回拨。\n\n")
	b.WriteString("## 2. 每臂×重复：客户端与相位\n\n")
	rows := [][]string{}
	for _, arm := range rep.Arms {
		rows = append(rows, []string{
			strconv.Itoa(arm.Repeat), "`" + arm.Arm + "`",
			strconv.Itoa(arm.ClientQuery.N),
			segMDMS(arm.ClientQuery.Latency.P50MS), segMDMS(arm.ClientQuery.Latency.P95MS),
			segMDMS(arm.ClientQuery.Latency.P99MS), segMDMS(arm.ClientQuery.Latency.MaxMS),
			segMDStatus(arm.ClientQuery.Status),
		})
	}
	segMDTable(&b, []string{"repeat", "arm", "query n", "p50ms", "p95ms", "p99ms", "maxms", "状态计数（全部样本，含非 2xx/transport）"}, rows)
	b.WriteString("相位分桶（相对 meta.window_start；[8s,12s) 与固定 burst 重叠，非纯稳态）：\n\n")
	rows = [][]string{}
	for _, arm := range rep.Arms {
		if len(arm.QueryPhases) == 0 {
			rows = append(rows, []string{strconv.Itoa(arm.Repeat), "`" + arm.Arm + "`", "—", "—", "—", "—", arm.PhaseNote})
			continue
		}
		for _, phase := range arm.QueryPhases {
			rows = append(rows, []string{
				strconv.Itoa(arm.Repeat), "`" + arm.Arm + "`", phase.Label,
				strconv.Itoa(phase.N), segMDMS(phase.Latency.P50MS), segMDMS(phase.Latency.P95MS),
				segMDStatus(phase.Status),
			})
		}
	}
	segMDTable(&b, []string{"repeat", "arm", "相位", "n", "p50ms", "p95ms", "状态计数"}, rows)

	b.WriteString("## 3. 配对段（同一 perf_id 的 server_total ∩ below_admit）\n\n")
	rows = [][]string{}
	for _, arm := range rep.Arms {
		paired := arm.Paired
		rows = append(rows, []string{
			strconv.Itoa(arm.Repeat), "`" + arm.Arm + "`", strconv.Itoa(paired.IDs),
			fmt.Sprintf("%d/%d", paired.ServerTotal.Count, paired.Admit.Count),
			segMDMS(paired.ServerTotal.P95MS), segMDMS(paired.Admit.P95MS),
			segMDMS(paired.BelowAdmit.P95MS),
		})
	}
	segMDTable(&b, []string{"repeat", "arm", "配对数", "server_total/admit n", "server_total p95", "admit p95", "below_admit p95"}, rows)
	b.WriteString("原始段分位数（未配对，全 ID）：\n\n")
	rows = [][]string{}
	for _, arm := range rep.Arms {
		rows = append(rows, []string{
			strconv.Itoa(arm.Repeat), "`" + arm.Arm + "`",
			fmt.Sprintf("%d/%s/%s", arm.Segments[segServerTotal].Count, segMDMS(arm.Segments[segServerTotal].P95MS), segMDMS(arm.Segments[segServerTotal].MaxMS)),
			fmt.Sprintf("%d/%s/%s", arm.Segments[segAdmit].Count, segMDMS(arm.Segments[segAdmit].P95MS), segMDMS(arm.Segments[segAdmit].MaxMS)),
			fmt.Sprintf("%d/%s/%s", arm.Segments[segAdmitSkipped].Count, segMDMS(arm.Segments[segAdmitSkipped].P95MS), segMDMS(arm.Segments[segAdmitSkipped].MaxMS)),
			fmt.Sprintf("%d/%s/%s", arm.Segments[segBelowAdmit].Count, segMDMS(arm.Segments[segBelowAdmit].P95MS), segMDMS(arm.Segments[segBelowAdmit].MaxMS)),
			segMDMS(arm.PgTotal.P95MS),
		})
	}
	segMDTable(&b, []string{"repeat", "arm", "server_total n/p95/max", "admit n/p95/max", "admit_skipped n/p95/max", "below_admit n/p95/max", "每请求 pg 合计 p95"}, rows)
	rows = [][]string{}
	for _, arm := range rep.Arms {
		if len(arm.SegmentOutside) == 0 {
			continue
		}
		rows = append(rows, []string{strconv.Itoa(arm.Repeat), "`" + arm.Arm + "`", fmt.Sprintf("%v", arm.SegmentOutside)})
	}
	if len(rows) > 0 {
		b.WriteString("窗口外段记录（未进相位分桶；检查时钟偏差或窗口原点）：\n\n")
		segMDTable(&b, []string{"repeat", "arm", "窗口外条数"}, rows)
	}

	b.WriteString("## 4. PG 语句（pgq）\n\n")
	rows = [][]string{}
	for _, arm := range rep.Arms {
		pgq := arm.Pgq
		prefixes := make([]string, 0, len(pgq.TopPrefixes))
		for _, stat := range pgq.TopPrefixes {
			prefixes = append(prefixes, fmt.Sprintf("`%s`×%d(p95=%s)", stat.Prefix, stat.Count, segMDMS(stat.P95MS)))
		}
		dist := make([]string, 0, len(arm.PgPerRequest.ByStatements))
		for _, key := range segSortedKeys(arm.PgPerRequest.ByStatements) {
			dist = append(dist, fmt.Sprintf("%s条×%d", key, arm.PgPerRequest.ByStatements[key]))
		}
		rows = append(rows, []string{
			strconv.Itoa(arm.Repeat), "`" + arm.Arm + "`",
			fmt.Sprintf("%d（err=%d）", pgq.Total, pgq.Errors),
			strconv.Itoa(pgq.DistinctPrefixes),
			strings.Join(prefixes, "<br>"),
			fmt.Sprintf("n=%d 无 pgq=%d mean=%s p50=%s p95=%s max=%d", arm.PgPerRequest.Requests, arm.PgPerRequest.RequestsWithoutPGQ,
				segMDMS(arm.PgPerRequest.Mean), segMDMS(arm.PgPerRequest.P50), segMDMS(arm.PgPerRequest.P95), arm.PgPerRequest.Max),
			strings.Join(dist, ", "),
		})
	}
	segMDTable(&b, []string{"repeat", "arm", "pgq 条数", "SQL 前缀数", "前≤8 前缀（条数/p95）", "每请求语句数（以客户端 perf_id 为基数）", "分布"}, rows)

	b.WriteString("## 5. 预热样本（单独小节，不并入稳态）\n\n")
	rows = [][]string{}
	for _, arm := range rep.Arms {
		warm := arm.Warmup
		rows = append(rows, []string{
			strconv.Itoa(arm.Repeat), "`" + arm.Arm + "`", strconv.Itoa(warm.Samples),
			strconv.Itoa(warm.Query.N), segMDMS(warm.Query.Latency.P95MS), segMDMS(warm.Query.Latency.MaxMS),
			segMDStatus(warm.Query.Status),
		})
	}
	segMDTable(&b, []string{"repeat", "arm", "预热样本", "query n", "query p95", "query max", "query 状态计数"}, rows)

	b.WriteString("## 6. 跨重复 p95 的逐重复值 / 中位数 / 极差\n\n")
	rows = [][]string{}
	for _, armName := range segSortedKeys(rep.RepeatSpread) {
		for _, def := range segMetricDefs {
			series, ok := rep.RepeatSpread[armName][def.Key]
			if !ok {
				continue
			}
			rows = append(rows, []string{"`" + armName + "`", def.Key, segMDSeries(series)})
		}
	}
	segMDTable(&b, []string{"arm", "指标", "逐重复值；中位数；极差(min–max)"}, rows)

	b.WriteString("## 7. 配对完整性与不变量（违规逐条可见）\n\n")
	rows = [][]string{}
	for _, arm := range rep.Arms {
		p := arm.Pairing
		rows = append(rows, []string{
			strconv.Itoa(arm.Repeat), "`" + arm.Arm + "`",
			strconv.Itoa(p.ClientSamples), strconv.Itoa(p.ClientWithoutPerfID), strconv.Itoa(p.ClientIDs), strconv.Itoa(p.ServerTotalIDs),
			fmt.Sprintf("client↛server_total=%d; server_total↛client=%d", p.ClientWithoutServerTotal, p.ServerTotalWithoutClient),
			fmt.Sprintf("server_total↛below=%d; below↛server_total=%d", p.ServerTotalWithoutBelowAdmit, p.BelowAdmitWithoutServerTotal),
			"dup: " + segMDMap(p.DuplicateIDs),
			"missing: " + segMDMap(p.MissingSegmentCounts) + " / extra: " + segMDMap(p.ExtraSegmentCounts),
		})
	}
	segMDTable(&b, []string{"repeat", "arm", "客户端样本", "无 perf_id", "client id", "server_total id", "双向未配对", "below_admit 未配对", "重复 id", "缺失/多出段计数"}, rows)

	b.WriteString("不变量检查（checked=受检 ID/记录数，violations=违规数）：\n\n")
	rows = [][]string{}
	for _, check := range rep.Invariants.Checks {
		status := "已评估"
		if check.Skipped {
			status = "跳过：" + check.SkipReason
		} else if check.SkipReason != "" {
			status = "部分跳过：" + check.SkipReason
		}
		rows = append(rows, []string{check.Name, strconv.Itoa(check.Checked), strconv.Itoa(check.Violations), status})
	}
	segMDTable(&b, []string{"不变量", "checked", "violations", "备注"}, rows)
	if len(rep.Invariants.Violations) > 0 {
		fmt.Fprintf(&b, "违规明细（列出 %d 条，按不变量计数：%s；截断=%v；完整计数见 summary.json）：\n\n",
			len(rep.Invariants.Violations), segMDMap(rep.Invariants.ViolationCounts), rep.Invariants.Truncated)
		rows = [][]string{}
		for _, violation := range rep.Invariants.Violations {
			rows = append(rows, []string{
				strconv.Itoa(violation.Repeat), "`" + violation.Arm + "`", strconv.FormatUint(violation.ID, 10),
				violation.Kind, violation.Detail,
			})
		}
		segMDTable(&b, []string{"repeat", "arm", "id", "不变量", "明细"}, rows)
	} else {
		b.WriteString("无违规记录。\n\n")
	}

	b.WriteString("## 8. 因子对比（差值对比（禁止相加/占比证明））\n\n")
	for _, metric := range rep.FactorEffects {
		fmt.Fprintf(&b, "### %s\n\n%s\n\n", metric.Metric, metric.Definition)
		rows = [][]string{}
		for _, key := range []string{"R0G0", "R1G0", "R0G1", "R1G1"} {
			series, ok := metric.Levels[key]
			if !ok {
				continue
			}
			rows = append(rows, []string{"水平 " + key, segMDSeries(series)})
		}
		for _, key := range []string{"R_at_G0", "R_at_G1", "G_at_R0", "G_at_R1", "interaction", "total_diff_R0G0_to_R1G1"} {
			series, ok := metric.Effects[key]
			if !ok {
				continue
			}
			rows = append(rows, []string{"效应 " + key, segMDSeries(series)})
		}
		segMDTable(&b, []string{"项", "逐重复值；中位数；极差(min–max)"}, rows)
	}

	b.WriteString("## 9. 开销对照：R1G1 vs R1G1:nocoll（客户端 query）\n\n")
	overhead := rep.OverheadCompare
	if !overhead.Present {
		b.WriteString("未发现 nocoll 臂，未产出对照。\n\n")
	} else {
		fmt.Fprintf(&b, "配对重复：%v\n\n", overhead.PairedRepeats)
		rows = [][]string{
			{"`" + overhead.Base + "`", strconv.Itoa(overhead.BaseSide.NTotal), segMDSeries(overhead.BaseSide.P50), segMDSeries(overhead.BaseSide.P95), segMDSeries(overhead.BaseSide.P99)},
			{"`" + overhead.Variant + "`", strconv.Itoa(overhead.VariantSide.NTotal), segMDSeries(overhead.VariantSide.P50), segMDSeries(overhead.VariantSide.P95), segMDSeries(overhead.VariantSide.P99)},
			{"差值（nocoll − steady）", "—", segMDSeries(overhead.DeltaP50), segMDSeries(overhead.DeltaP95), segMDSeries(overhead.DeltaP99)},
		}
		segMDTable(&b, []string{"侧", "样本量", "p50", "p95", "p99"}, rows)
		if overhead.Reference != nil {
			fmt.Fprintf(&b, "参考（未配对，仅背景信息）：稳态 `R1G1` 样本量=%d，p50=%s p95=%s p99=%s\n\n",
				overhead.Reference.NTotal, segMDSeries(overhead.Reference.P50), segMDSeries(overhead.Reference.P95), segMDSeries(overhead.Reference.P99))
		}
		fmt.Fprintf(&b, "%s\n\n", overhead.Note)
	}

	b.WriteString("## 10. 候选规则检查（候选规则，非裁决）\n\n")
	rows = [][]string{}
	for _, rule := range rep.CandidateRules {
		rows = append(rows, []string{rule.Rule, rule.Status, rule.Observed, rule.Note})
	}
	segMDTable(&b, []string{"候选规则（非裁决）", "状态", "观测", "口径注记"}, rows)

	b.WriteString("## 11. 时钟步进与校正（wall clock 回拨）\n\n")
	b.WriteString("- 检测口径：每臂 server_total 记录按 **id 升序**取相邻标签差 Δ；Δ<−50ms 记为回拨步进并校正；Δ>+500ms 记为「可疑前跳/停顿」（ambiguous=true），只披露不校正。\n")
	b.WriteString("- 适用范围：**仅当** first_affected_id > first_steady_id（稳态 client perf_id 最小值；预热样本不参与该判定）时，步进才视为作用于窗口；预热期回拨不校正（window_start 与稳态标签同处位移后的时基），window_start 永不校正。\n")
	b.WriteString("- 校正口径：以适用步进的边界 id 为序，对 id > boundary_id 的记录标签加 Σ|回拨|；segments 与带 perf_id 的客户端样本统一校正，无 id 的样本不参与相位。\n")
	b.WriteString("- 相位：相位分桶一律使用**校正后标签**与 window_start；原始 wall 相位计数保留为 `client_query_phases_wall` 供审计。\n")
	b.WriteString("- 窗口：window_seconds 为原始 wall 时长；window_seconds_corrected = 原始 + Σ(作用于窗口的回拨 |Δ|)（无适用步进时即为原始值）；作用于窗口的可疑前跳则标注不可判定原因。\n")
	b.WriteString("- 限制：前跳不可判（停顿/GC/事件循环阻塞与时钟前跳不可区分）；跨机绝对时间不可比；时长类指标（duration_ms / dur_ns）取自单调时钟，不受回拨影响；Σ|Δ| 含一次正常请求间隔，校正后标签可能仍偏早约一个间隔（≈0.1s），相位归类可用、精确时刻不可复原。\n\n")
	rows = [][]string{}
	for _, rollup := range rep.ClockSteps {
		for _, step := range rollup.Steps {
			rows = append(rows, []string{
				strconv.Itoa(rollup.Repeat), "`" + rollup.Arm + "`", step.Kind,
				strconv.FormatUint(step.BoundaryID, 10), strconv.FormatUint(step.FirstAffectedID, 10),
				strconv.FormatFloat(step.DeltaMS, 'f', 3, 64),
				strconv.FormatFloat(step.CorrectionMS, 'f', 3, 64),
				strconv.FormatBool(step.Ambiguous), strconv.FormatBool(step.AppliesToWindow), step.Reason, step.Note,
			})
		}
	}
	if len(rows) == 0 {
		b.WriteString("未检测到时钟步进（阈值：回拨 <−50ms，前跳 >+500ms）：无步进，无需校正。\n\n")
	} else {
		segMDTable(&b, []string{"repeat", "arm", "kind", "boundary_id", "first_affected_id", "delta_ms", "correction_ms", "ambiguous", "applies_to_window", "reason", "注记"}, rows)
	}
	rows = [][]string{}
	for _, arm := range rep.Arms {
		rawWindow, correctedWindow := "—", "—"
		if arm.WindowSeconds != nil {
			rawWindow = strconv.FormatFloat(*arm.WindowSeconds, 'f', 3, 64)
		}
		if arm.WindowSecondsCorrected != nil {
			correctedWindow = strconv.FormatFloat(*arm.WindowSecondsCorrected, 'f', 3, 64)
		} else if arm.WindowSecondsCorrectedNote != "" {
			correctedWindow = "不可判定（" + arm.WindowSecondsCorrectedNote + "）"
		}
		rows = append(rows, []string{
			strconv.Itoa(arm.Repeat), "`" + arm.Arm + "`", arm.PhaseMethod,
			strconv.Itoa(arm.ClockStepsBackward), strconv.Itoa(arm.ClockStepsApplied), strconv.Itoa(arm.ClockStepsForwardSuspect),
			rawWindow, correctedWindow, arm.ClockStepsNote,
		})
	}
	segMDTable(&b, []string{"repeat", "arm", "phase_method", "回拨步数", "作用于窗口的回拨", "可疑前跳", "window_seconds（raw）", "window_seconds_corrected", "注记"}, rows)

	return b.String()
}

// segWriteSummary writes summary.json and summary.md into root.
func segWriteSummary(root string, rep *segReport) (string, string, error) {
	jsonPath := filepath.Join(root, segSummaryJSONName)
	mdPath := filepath.Join(root, segSummaryMDName)
	data, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return "", "", fmt.Errorf("marshal summary: %w", err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(jsonPath, data, 0o644); err != nil {
		return "", "", fmt.Errorf("write %s: %w", jsonPath, err)
	}
	if err := os.WriteFile(mdPath, []byte(segRenderMarkdown(rep)), 0o644); err != nil {
		return "", "", fmt.Errorf("write %s: %w", mdPath, err)
	}
	return jsonPath, mdPath, nil
}
