// render.go builds the aggregate document (comparison across runs, anomaly
// list) and renders the Chinese markdown summary. Everything here is
// deterministic: maps are sorted before rendering and no map iteration order
// ever reaches the output.
package main

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

// allSummary is the aggregate document (summary_all.json).
type allSummary struct {
	Schema      string          `json:"schema"`
	EvidenceDir string          `json:"evidence_dir"`
	GeneratedAt string          `json:"generated_at"`
	Runs        []*runSummary   `json:"runs"`
	Skipped     []skippedRun    `json:"skipped,omitempty"`
	Comparison  comparisonBlock `json:"comparison"`
	Anomalies   []anomaly       `json:"anomalies"`
	Notes       []string        `json:"notes"`
}

type skippedRun struct {
	Label  string `json:"label"`
	Reason string `json:"reason"`
}

type comparisonBlock struct {
	Metrics []metricCompare `json:"metrics"`
	Note    string          `json:"note"`
}

type metricCompare struct {
	Key             string                `json:"key"`
	Unit            string                `json:"unit"`
	PerRun          []runValue            `json:"per_run"`
	Groups          map[string]*groupStat `json:"groups"`
	DeltaOnMinusOff *deltaStat            `json:"delta_on_minus_off"`
}

type runValue struct {
	Label string  `json:"label"`
	Group string  `json:"group"`
	Value float64 `json:"value"`
}

type groupStat struct {
	N      int      `json:"n"`
	Median *float64 `json:"median"`
	Min    *float64 `json:"min"`
	Max    *float64 `json:"max"`
}

type deltaStat struct {
	Abs    float64  `json:"abs"`
	RelPct *float64 `json:"rel_pct"`
}

type anomaly struct {
	Label    string `json:"label"`
	Kind     string `json:"kind"`
	Evidence string `json:"evidence"`
	Cause    string `json:"cause"`
}

// comparisonNote is the frozen discipline sentence of the cross-run block.
const comparisonNote = "描述性汇总：n 小、方差未测；组基准为组内中位数；组间差 = (on 中位数 − off 中位数)，相对% 以 off 中位数为分母。" +
	"不得因一次小差值声称『无扰动』或其他因果结论；所有百分比仅诊断参考，非 SLO/阈值。"

// comparisonMetrics are the cross-run metrics of the frozen spec. Detection
// latencies stay in microseconds; the rest carry their own unit.
var comparisonMetrics = []struct {
	key  string
	unit string
	pick func(*runSummary) *float64
}{
	{"publish_s", "s", func(r *runSummary) *float64 { return r.PublishS }},
	{"tail_s", "s", func(r *runSummary) *float64 { return r.TailS }},
	{"total_s", "s", func(r *runSummary) *float64 { return r.TotalS }},
	{"e2e_rate", "events/s", func(r *runSummary) *float64 { return r.E2ERate }},
	{"applied_at_publish_done", "events", func(r *runSummary) *float64 {
		if r.AppliedAtPublishDone == nil {
			return nil
		}
		return new(float64(*r.AppliedAtPublishDone))
	}},
	{"publish_observe_latency_us", "us", func(r *runSummary) *float64 { return int64PtrAsFloat(r.Latency.PublishObserveLatencyUS) }},
	{"publish_done_latency_us", "us", func(r *runSummary) *float64 { return int64PtrAsFloat(r.Latency.PublishDoneLatencyUS) }},
	{"consume_confirm_latency_us", "us", func(r *runSummary) *float64 { return int64PtrAsFloat(r.Latency.ConsumeConfirmLatencyUS) }},
	{"applied_cb_latency_us", "us", func(r *runSummary) *float64 { return int64PtrAsFloat(r.Latency.AppliedCBLatencyUS) }},
}

func int64PtrAsFloat(p *int64) *float64 {
	if p == nil {
		return nil
	}
	return new(float64(*p))
}

// compareRuns builds the descriptive cross-run comparison.
func compareRuns(runs []*runSummary) comparisonBlock {
	cb := comparisonBlock{Note: comparisonNote}
	for _, spec := range comparisonMetrics {
		mc := metricCompare{Key: spec.key, Unit: spec.unit, Groups: map[string]*groupStat{}}
		byGroup := map[string][]float64{}
		for _, r := range runs {
			v := spec.pick(r)
			if v == nil {
				continue
			}
			mc.PerRun = append(mc.PerRun, runValue{Label: r.Label, Group: r.Group, Value: round6(*v)})
			byGroup[r.Group] = append(byGroup[r.Group], *v)
		}
		for _, g := range []string{"on", "off", "pristine", "unknown"} {
			vals, ok := byGroup[g]
			if !ok {
				if g == "unknown" {
					continue
				}
				mc.Groups[g] = &groupStat{}
				continue
			}
			stat := &groupStat{N: len(vals)}
			if med, ok := median(vals); ok {
				stat.Median = new(round6(med))
				minV, maxV := vals[0], vals[0]
				for _, v := range vals {
					if v < minV {
						minV = v
					}
					if v > maxV {
						maxV = v
					}
				}
				stat.Min = new(round6(minV))
				stat.Max = new(round6(maxV))
			}
			mc.Groups[g] = stat
		}
		on, off := mc.Groups["on"], mc.Groups["off"]
		if on != nil && off != nil && on.Median != nil && off.Median != nil {
			d := deltaStat{Abs: round6(*on.Median - *off.Median)}
			if *off.Median != 0 {
				d.RelPct = new(round6((*on.Median - *off.Median) / math.Abs(*off.Median) * 100))
			}
			mc.DeltaOnMinusOff = &d
		}
		cb.Metrics = append(cb.Metrics, mc)
	}
	return cb
}

// collectAnomalies surfaces only verifiable evidence: every non-note check
// detail line becomes one anomaly, and the cross-run deviation screen adds
// runs whose total_s is beyond the documented diagnostic threshold. Causes
// that cannot be derived from the evidence are marked unknown.
func collectAnomalies(runs []*runSummary) []anomaly {
	var out []anomaly
	for _, r := range runs {
		seen := map[string]bool{}
		for _, d := range r.Checks.Details {
			if strings.HasPrefix(d, "note: ") {
				continue
			}
			kind := d
			if i := strings.Index(d, ": "); i > 0 {
				kind = d[:i]
			}
			seen[kind] = true
			out = append(out, anomaly{Label: r.Label, Kind: kind, Evidence: d, Cause: anomalyCause(kind)})
		}
		mentioned := func(name string) bool {
			for _, d := range r.Checks.Details {
				if strings.HasPrefix(strings.TrimPrefix(d, "note: "), name+": ") {
					return true
				}
			}
			return false
		}
		for _, chk := range []struct {
			name string
			ok   bool
		}{
			{"anchors_complete", r.Checks.AnchorsComplete},
			{"counts_ok", r.Checks.CountsOK},
			{"drops_ok", r.Checks.DropsOK},
			{"overlap_ok", r.Checks.OverlapOK},
			{"nesting_ok", r.Checks.NestingOK},
			{"report_vs_anchors_ok", r.Checks.ReportVsAnchorsOK},
		} {
			if !chk.ok && !seen[chk.name] && !mentioned(chk.name) {
				out = append(out, anomaly{Label: r.Label, Kind: chk.name,
					Evidence: chk.name + " = false（详见该轮 summary.json checks.details）", Cause: "unknown"})
			}
		}
	}
	out = append(out, deviationAnomalies(runs)...)
	return out
}

// anomalyCause maps the kind onto a short, evidence-grounded cause sentence.
func anomalyCause(kind string) string {
	switch kind {
	case "exit_code":
		return "运行未以成功退出（go test 返回非零）；具体原因见 logs/<label>.log，未知时不推断"
	case "parse":
		return "证据文件行级解析缺陷（未采集或损坏）；原因未知"
	case "anchors_complete":
		return "anchors.json 缺失或不完整；原因未知"
	case "drops_ok":
		return "采集侧丢弃计数；原因未知（见 evidence 数值）"
	case "overlap_ok":
		return "顶层 span 结构异常（交叠/负时长/epoch 前）；原因未知"
	case "nesting_ok":
		return "嵌套义务违例（Σsql 或 tx_total 超过 process span）；原因未知"
	case "counts_ok":
		return "计数对账不符；原因未知（见 evidence 数值）"
	case "report_vs_anchors_ok":
		return "report.json 与 anchors.json 数值不一致；原因未知"
	case "tail":
		return "tail 窗口裁剪出现负残差；窗口/span 口径可疑，原因未知"
	case "latency":
		return "检测延迟为负；时间锚点顺序可疑，原因未知"
	}
	return "unknown（原因未知）"
}

// deviationAnomalies screens total_s inside a group with >= 3 runs against the
// group median (> 25%, documented as diagnostic). The threshold is disclosed;
// a hit is a flag, never a conclusion.
func deviationAnomalies(runs []*runSummary) []anomaly {
	const threshold = 0.25
	var out []anomaly
	for _, group := range []string{"on", "off"} {
		var members []*runSummary
		var vals []float64
		for _, r := range runs {
			if r.Group != group || r.TotalS == nil {
				continue
			}
			members = append(members, r)
			vals = append(vals, *r.TotalS)
		}
		if len(members) < 3 {
			continue
		}
		med, ok := median(vals)
		if !ok || med <= 0 {
			continue
		}
		for _, r := range members {
			dev := math.Abs(*r.TotalS-med) / med
			if dev <= threshold {
				continue
			}
			out = append(out, anomaly{
				Label: r.Label,
				Kind:  "deviation_total_s",
				Evidence: fmt.Sprintf("total_s=%.6fs 对组 %s 中位数 %.6fs 相对偏差 %+.1f%%（筛查阈值 25%%，诊断性；n=%d，方差未测）; publish_s=%s tail_s=%s",
					*r.TotalS, group, med, (*r.TotalS-med)/med*100, len(members),
					fnum(r.PublishS, 6), fnum(r.TailS, 6)),
				Cause: "unknown（原因未知；数值由该轮 anchors/report 佐证，需人读 logs/<label>.log）",
			})
		}
	}
	return out
}

// --------------------------------------------------------------- markdown

// renderMarkdown renders the Chinese aggregate report.
func renderMarkdown(all *allSummary) string {
	var b strings.Builder
	groupCounts := map[string]int{}
	for _, r := range all.Runs {
		groupCounts[r.Group]++
	}
	fmt.Fprintf(&b, "# 013-supplement 消费者追赶分段测量分析\n\n")
	fmt.Fprintf(&b, "- 证据根：`%s`\n", all.EvidenceDir)
	fmt.Fprintf(&b, "- 运行：%d 个（on=%d, off=%d, pristine=%d, unknown=%d）\n",
		len(all.Runs), groupCounts["on"], groupCounts["off"], groupCounts["pristine"], groupCounts["unknown"])
	fmt.Fprintf(&b, "- 生成时间：%s（唯一非确定性字段；其余输出对同一证据树可复现）\n", all.GeneratedAt)
	fmt.Fprintf(&b, "- 输出：本文件与 `summary_all.json`；每轮 `runs/<label>/summary.json`\n\n")

	b.WriteString("## 0. 口径（冻结 v1）\n\n")
	for _, n := range all.Notes {
		fmt.Fprintf(&b, "- %s\n", n)
	}
	b.WriteString("\n")

	renderOverview(&b, all)
	renderChecks(&b, all)
	renderSegments(&b, all)
	renderTail(&b, all)
	renderCounts(&b, all)
	renderCurve(&b, all)
	renderPartitions(&b, all)
	renderPublish(&b, all)
	renderComparison(&b, all)
	renderAnomalies(&b, all)
	renderNotCollected(&b, all)
	return b.String()
}

func renderOverview(b *strings.Builder, all *allSummary) {
	b.WriteString("## 1. 逐轮概览\n\n")
	headers := []string{"label", "group", "collection", "n", "rc", "publish_s", "tail_s", "total_s", "e2e_rate",
		"观察延迟(µs)", "完成延迟(µs)", "确认延迟(µs)", "cb延迟(µs)", "timing源"}
	rows := make([][]string, 0, len(all.Runs))
	for _, r := range all.Runs {
		rc := "-"
		if r.ExitCode != nil {
			rc = strconv.Itoa(*r.ExitCode)
		}
		rows = append(rows, []string{
			r.Label, r.Group, r.Collection, fint(r.N), rc,
			fnum(r.PublishS, 6), fnum(r.TailS, 6), fnum(r.TotalS, 6), fnum(r.E2ERate, 6),
			fint(r.Latency.PublishObserveLatencyUS), fint(r.Latency.PublishDoneLatencyUS),
			fint(r.Latency.ConsumeConfirmLatencyUS), fint(r.Latency.AppliedCBLatencyUS),
			r.TimingSource,
		})
	}
	mdTable(b, headers, rows)
	b.WriteString("\n`-` 表示未采集/不可得；`e2e_rate` = n / total_s（events/s）。\n\n")
}

func renderChecks(b *strings.Builder, all *allSummary) {
	b.WriteString("## 2. 检查（checks）\n\n")
	headers := []string{"label", "anchors_complete", "counts_ok", "drops_ok", "overlap_ok", "nesting_ok", "report_vs_anchors_ok", "files_missing"}
	rows := make([][]string, 0, len(all.Runs))
	for _, r := range all.Runs {
		rows = append(rows, []string{
			r.Label,
			mark(r.Checks.AnchorsComplete), mark(r.Checks.CountsOK), mark(r.Checks.DropsOK),
			mark(r.Checks.OverlapOK), mark(r.Checks.NestingOK), mark(r.Checks.ReportVsAnchorsOK),
			strings.Join(r.Checks.FilesMissing, ", "),
		})
	}
	mdTable(b, headers, rows)
	for _, r := range all.Runs {
		if len(r.Checks.Details) == 0 {
			continue
		}
		fmt.Fprintf(b, "\n**%s** details：\n", r.Label)
		for _, d := range r.Checks.Details {
			fmt.Fprintf(b, "- %s\n", d)
		}
	}
	b.WriteString("\n")
}

func renderSegments(b *strings.Builder, all *allSummary) {
	b.WriteString("## 3. 分段统计（loop/sql 种类，µs）\n\n")
	any := false
	for _, r := range all.Runs {
		if !r.Segments.Collected {
			continue
		}
		any = true
		fmt.Fprintf(b, "### %s（loop 行=%d, sql 行=%d, 解析失败 loop=%d sql=%d）\n\n",
			r.Label, r.Segments.LoopRows, r.Segments.SQLRows, r.Segments.LoopParseErrors, r.Segments.SQLParseErrors)
		rows := [][]string{}
		for _, kind := range loopKinds {
			rows = append(rows, append([]string{"loop." + kind}, distCells(r.Segments.Loop[kind])...))
		}
		for _, kind := range sqlKinds {
			rows = append(rows, append([]string{"sql." + kind}, distCells(r.Segments.SQL[kind])...))
		}
		rows = append(rows, append([]string{"tx_total"}, distCells(r.Segments.TxTotal)...))
		mdTable(b, []string{"kind", "n", "mean", "p50", "p95", "p99", "max", "min"}, rows)
		fmt.Fprintf(b, "\ntx_events=%d, tx_skipped=%d（无 begin/commit）\n", r.Segments.TxEvents, r.Segments.TxSkipped)
		if p := r.Segments.Poll; p != nil {
			fmt.Fprintf(b, "poll: 次数=%d, 空 poll=%d（records=0）, records min/mean/max=%s/%s/%s, Σrecords=%d；直方图 %s\n",
				p.Count, p.Empty, fint(p.RecordsMin), fnum(p.RecordsMean, 3), fint(p.RecordsMax), p.RecordsTotal,
				histoString(p.RecordsHistogram))
		}
		fmt.Fprintf(b, "sql_unattributed（seq=-1 行）=%d；无 process span 的 sql 行=%d；未知 loop kind 行=%d\n\n",
			r.Segments.SQLUnattributed, r.Segments.SQLRowsNoProcess, r.Segments.UnknownLoopRows)
	}
	if !any {
		b.WriteString("（无 loop/sql 采集）\n\n")
	}
}

func renderTail(b *strings.Builder, all *allSummary) {
	b.WriteString("## 4. tail 裁剪（窗口 [publish_done_us, poll_confirm_us]）\n\n")
	any := false
	for _, r := range all.Runs {
		if !r.Tail.Collected {
			continue
		}
		any = true
		t := r.Tail
		fmt.Fprintf(b, "### %s\n\n", r.Label)
		rows := [][]string{}
		kinds := make([]string, 0, len(t.ByKind))
		for k := range t.ByKind {
			kinds = append(kinds, k)
		}
		sort.Strings(kinds)
		for _, k := range kinds {
			kc := t.ByKind[k]
			rows = append(rows, []string{k, strconv.Itoa(kc.Spans), strconv.Itoa(kc.Full), strconv.Itoa(kc.Partial),
				strconv.Itoa(kc.Outside), strconv.FormatInt(kc.CoverageUS, 10), strconv.FormatInt(kc.RawFullUS, 10)})
		}
		mdTable(b, []string{"kind", "spans", "full", "partial", "outside", "窗内覆盖(µs)", "raw full(µs)"}, rows)
		fmt.Fprintf(b, "\nwindow=[%s, %s]µs, tail=%sµs, 覆盖=%dµs, residual=%sµs（%s%%）, 交叠对=%d, 负时长=%d, 零时长=%d（含 rebalance 点事件等无时长 span）, epoch 前=%d\n\n",
			fint(t.WindowStartUS), fint(t.WindowEndUS), fint(t.TailUS), t.CoverageUS,
			fint(t.ResidualUS), fnum(t.ResidualPct, 3), t.OverlapPairs, t.NegativeDurations, t.ZeroDurations, t.PreEpochSpans)
		b.WriteString("> " + t.RawFullVsTail + "\n\n")
	}
	if !any {
		b.WriteString("（无 loop 采集或无窗口锚点）\n\n")
	}
}

func renderCounts(b *strings.Builder, all *allSummary) {
	b.WriteString("## 5. 计数与丢失\n\n")
	headers := []string{"label", "process", "applied", "duplicate", "version_skip", "quarantined", "errors",
		"sql_spans", "sql_unattributed", "applied_cb", "loop 计数", "drops", "identity", "applied==n"}
	rows := make([][]string, 0, len(all.Runs))
	for _, r := range all.Runs {
		c := r.Counts
		row := []string{r.Label, "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-"}
		if c.Anchors != nil {
			row[1] = strconv.FormatInt(c.Anchors.ObservedProcess, 10)
			row[2] = strconv.FormatInt(c.Anchors.ObservedApplied, 10)
			row[3] = strconv.FormatInt(c.Anchors.ObservedDuplicate, 10)
			row[4] = strconv.FormatInt(c.Anchors.ObservedVersionSkip, 10)
			row[5] = strconv.FormatInt(c.Anchors.ObservedQuarantined, 10)
			row[6] = strconv.FormatInt(c.Anchors.ObservedErrors, 10)
			row[7] = strconv.FormatInt(c.Anchors.SQLSpans, 10)
			row[8] = strconv.FormatInt(c.Anchors.SQLUnattributed, 10)
			row[9] = strconv.FormatInt(c.Anchors.AppliedCB, 10)
		}
		if c.Loop != nil {
			row[10] = fmt.Sprintf("process=%s cb=%s err=%s sql=%s seq=-1:%s",
				fintPtr(c.Loop.ProcessSpans), fintPtr(c.Loop.AppliedCBRows), fintPtr(c.Loop.ErrorOutcomes),
				fintPtr(c.Loop.SQLRows), fintPtr(c.Loop.SQLUnattributed))
		}
		if c.Drops != nil {
			row[11] = fmt.Sprintf("loop_unpaired=%s samples_dropped=%s (%s)",
				fint(c.Drops.LoopUnpaired), fint(c.Drops.SamplesDropped), c.Drops.Source)
		}
		row[12] = fboolPtr(c.IdentityOK)
		row[13] = fboolPtr(c.AppliedEqualsN)
		rows = append(rows, row)
	}
	mdTable(b, headers, rows)
	b.WriteString("\n")
}

func renderCurve(b *strings.Builder, all *allSummary) {
	b.WriteString("## 6. 曲线十分位（达到各比例的时间，相对 drain_start，ms）\n\n")
	headers := []string{"label/系列", "10%", "20%", "30%", "40%", "50%", "60%", "70%", "80%", "90%", "100%"}
	any := false
	for _, r := range all.Runs {
		if !r.Curve.Collected {
			continue
		}
		any = true
		var rows [][]string
		for _, series := range []struct {
			name   string
			points []decilePoint
		}{
			{"applied", r.Curve.Applied},
			{"published", r.Curve.Published},
			{"progress", r.Curve.Progress},
		} {
			row := []string{r.Label + "/" + series.name}
			for _, p := range series.points {
				row = append(row, fnum(p.TMS, 3))
			}
			rows = append(rows, row)
		}
		mdTable(b, headers, rows)
	}
	if !any {
		b.WriteString("（无 samples.csv 采集）\n\n")
		return
	}
	b.WriteString("\n`-` = 未达到（系列最大值为 0，或 anchors 缺失导致相对基准不可得）。\n\n")
}

func renderPartitions(b *strings.Builder, all *allSummary) {
	b.WriteString("## 7. 分区/offset 与 lag_final\n\n")
	for _, r := range all.Runs {
		if !r.Partitions.Collected && r.Partitions.LagFinal == nil {
			continue
		}
		fmt.Fprintf(b, "### %s\n\n", r.Label)
		if r.Partitions.Collected {
			rows := [][]string{}
			for _, p := range r.Partitions.ByPartition {
				rows = append(rows, []string{strconv.Itoa(p.Partition), strconv.Itoa(p.Count), strconv.FormatInt(p.MaxOffset, 10), strconv.Itoa(p.Errors)})
			}
			mdTable(b, []string{"partition", "process span 数", "最大 offset", "错误 outcome（s1 空）"}, rows)
			if len(r.Partitions.Outcomes) > 0 {
				parts := make([]string, 0, len(r.Partitions.Outcomes))
				for _, o := range r.Partitions.Outcomes {
					parts = append(parts, fmt.Sprintf("%s=%d", o.Outcome, o.Count))
				}
				fmt.Fprintf(b, "\nprocess outcome 分布：%s（含错误合计 %d）\n", strings.Join(parts, ", "), r.Partitions.ErrorOutcomes)
			}
		}
		if r.Partitions.LagFinal != nil {
			fmt.Fprintf(b, "\nlag_final（%s）：%s\n", r.Partitions.LagFinalSource, lagString(r.Partitions.LagFinal))
		}
		b.WriteString("\n")
	}
}

func renderPublish(b *strings.Builder, all *allSummary) {
	b.WriteString("## 8. 发布周期（publish.csv）\n\n")
	headers := []string{"label", "rows", "生产周期", "边界行", "Σclaimed", "Σacked", "Σreleased", "Σblocked", "末轮 pending_seen", "末轮终止形态", "周期不变量违例"}
	rows := [][]string{}
	for _, r := range all.Runs {
		if !r.Publish.Collected {
			continue
		}
		rows = append(rows, []string{
			r.Label, strconv.Itoa(r.Publish.Rows), strconv.Itoa(r.Publish.ProductiveCycles),
			strconv.Itoa(r.Publish.BoundaryRows),
			strconv.FormatInt(r.Publish.Claimed, 10), strconv.FormatInt(r.Publish.Acked, 10),
			strconv.FormatInt(r.Publish.Released, 10), strconv.FormatInt(r.Publish.Blocked, 10),
			fint(r.Publish.LastPendingSeen), mark(r.Publish.LastCycleTerminal),
			strconv.Itoa(r.Publish.InvariantViolations),
		})
	}
	if len(rows) == 0 {
		b.WriteString("（无 publish.csv 采集）\n\n")
		return
	}
	mdTable(b, headers, rows)
	b.WriteString("\n`边界行` = 冻结的末轮观测行（pending_seen=0 且无发布结果）；`末轮终止形态` = 该行形态成立。\n\n")
}

func renderComparison(b *strings.Builder, all *allSummary) {
	b.WriteString("## 9. 跨轮对照（描述性）\n\n")
	for _, m := range all.Comparison.Metrics {
		fmt.Fprintf(b, "### %s（单位 %s）\n\n", m.Key, m.Unit)
		rows := [][]string{}
		for _, g := range []string{"on", "off", "pristine", "unknown"} {
			st, ok := m.Groups[g]
			if !ok || st == nil {
				continue
			}
			rows = append(rows, []string{g, strconv.Itoa(st.N), fnum(st.Median, 6), fnum(st.Min, 6), fnum(st.Max, 6)})
		}
		mdTable(b, []string{"组", "n", "median", "min", "max"}, rows)
		if d := m.DeltaOnMinusOff; d != nil {
			fmt.Fprintf(b, "\n组间差 on−off：绝对 %.6f，相对 %s%%\n", d.Abs, fnum(d.RelPct, 3))
		} else {
			b.WriteString("\n组间差 on−off：不可得（缺 on 或 off 组值）\n")
		}
		if len(m.PerRun) > 0 {
			parts := make([]string, 0, len(m.PerRun))
			for _, v := range m.PerRun {
				parts = append(parts, fmt.Sprintf("%s(%s)=%.6f", v.Label, v.Group, v.Value))
			}
			fmt.Fprintf(b, "\n逐轮值：%s\n", strings.Join(parts, ", "))
		}
		b.WriteString("\n")
	}
	b.WriteString("> " + all.Comparison.Note + "\n\n")
}

func renderAnomalies(b *strings.Builder, all *allSummary) {
	b.WriteString("## 10. 异常与不可解释项\n\n")
	if len(all.Anomalies) == 0 {
		b.WriteString("未发现可验证异常（checks 全部通过且无解析/交叠/嵌套/计数问题）。\n\n")
		return
	}
	for _, a := range all.Anomalies {
		fmt.Fprintf(b, "- [%s] %s：%s —— 原因：%s\n", a.Kind, a.Label, mdEscape(a.Evidence), a.Cause)
	}
	b.WriteString("\n")
}

func renderNotCollected(b *strings.Builder, all *allSummary) {
	b.WriteString("## 11. 未采集清单\n\n")
	for _, r := range all.Runs {
		if len(r.NotCollected) == 0 {
			continue
		}
		fmt.Fprintf(b, "- %s（%s）：%s\n", r.Label, r.Collection, strings.Join(r.NotCollected, "；"))
	}
	for _, s := range all.Skipped {
		fmt.Fprintf(b, "- %s：跳过（%s）\n", s.Label, s.Reason)
	}
	b.WriteString("\n")
}

// ------------------------------------------------------------ md primitives

func mark(ok bool) string {
	if ok {
		return "OK"
	}
	return "FAIL"
}

func mdEscape(s string) string {
	s = strings.ReplaceAll(s, "\n", " ")
	return strings.ReplaceAll(s, "|", "\\|")
}

func mdTable(b *strings.Builder, headers []string, rows [][]string) {
	b.WriteString("| " + strings.Join(headers, " | ") + " |\n")
	sep := make([]string, len(headers))
	for i := range sep {
		sep[i] = "---"
	}
	b.WriteString("| " + strings.Join(sep, " | ") + " |\n")
	for _, row := range rows {
		cells := make([]string, len(headers))
		for i := range headers {
			if i < len(row) {
				cells[i] = mdEscape(row[i])
			} else {
				cells[i] = "-"
			}
		}
		b.WriteString("| " + strings.Join(cells, " | ") + " |\n")
	}
}

func fnum(v *float64, digits int) string {
	if v == nil {
		return "-"
	}
	return strconv.FormatFloat(*v, 'f', digits, 64)
}

func fint(v *int64) string {
	if v == nil {
		return "-"
	}
	return strconv.FormatInt(*v, 10)
}

func fintPtr(v *int) string {
	if v == nil {
		return "-"
	}
	return strconv.Itoa(*v)
}

func fboolPtr(v *bool) string {
	if v == nil {
		return "-"
	}
	return mark(*v)
}

// distCells renders the six-number distribution in table order
// (n, mean, p50, p95, p99, max, min).
func distCells(d *dist) []string {
	if d == nil {
		return []string{"-", "-", "-", "-", "-", "-", "-"}
	}
	return []string{
		strconv.Itoa(d.N),
		fnum(new(d.Mean), 3), fnum(new(d.P50), 3), fnum(new(d.P95), 3), fnum(new(d.P99), 3),
		fnum(new(d.Max), 3), fnum(new(d.Min), 3),
	}
}

// histoString renders the poll histogram as "bucket=count" pairs.
func histoString(buckets []histoBucket) string {
	parts := make([]string, 0, len(buckets))
	for _, b := range buckets {
		if b.Count == 0 {
			continue
		}
		parts = append(parts, b.Range+":"+strconv.Itoa(b.Count))
	}
	if len(parts) == 0 {
		return "-"
	}
	return strings.Join(parts, " ")
}
