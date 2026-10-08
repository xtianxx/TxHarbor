// Command analyze is the offline analyzer for the 013-supplement consumer
// catch-up segmented batch ("consumer-seg"). It reads an evidence root that
// carries one directory per run plus the runner logs:
//
//	<dir>/runs/<label>/{meta.json,anchors.json,report.json,exit_code}
//	<dir>/runs/<label>/{samples,loop,sql,publish}.csv[.gz]
//	<dir>/logs/<label>.rc
//
// and writes, per run, runs/<label>/summary.json, plus one aggregate
// <out>/summary_all.json and <out>/summary.md (default <out> = <dir>/analysis).
//
// The tool is stdlib-only, read-only with respect to every input file,
// concurrency-free and — apart from the recorded generation timestamp —
// deterministic. It never starts a container, broker, database or network
// connection. Missing artifacts are marked "not collected"; they never fail
// the analyzer. Only an evidence root without a single parseable run is an
// error.
//
// Usage:
//
//	go run ./scripts/consumerseg/analyze -dir <evidence-dir> [-out <dir>]
//
// Exit codes: 0 = at least one run analyzed (failing checks do not fail the
// analyzer), 1 = no parseable run or fatal I/O error, 2 = usage error.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// schemaTag identifies the aggregate document shape.
const schemaTag = "013-supplement/consumer-seg/analysis/1"

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run is the testable entry point: it never calls os.Exit and writes all human
// output to the provided writers.
func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("consumerseg-analyze", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dir := fs.String("dir", "", "evidence root (contains runs/<label>/ and logs/)")
	out := fs.String("out", "", "output directory for summary_all.json and summary.md (default <dir>/analysis)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 0 || strings.TrimSpace(*dir) == "" {
		fmt.Fprintln(stderr, "usage: analyze -dir <evidence-dir> [-out <dir>]")
		return 2
	}
	root := filepath.Clean(*dir)
	outDir := strings.TrimSpace(*out)
	if outDir == "" {
		outDir = filepath.Join(root, "analysis")
	} else {
		outDir = filepath.Clean(outDir)
	}
	all, err := analyzeRoot(root)
	if err != nil {
		fmt.Fprintf(stderr, "analyze: %v\n", err)
		return 1
	}
	if err := writeAggregates(outDir, all); err != nil {
		fmt.Fprintf(stderr, "analyze: %v\n", err)
		return 1
	}
	printReport(stdout, all, outDir)
	return 0
}

// analyzeRoot discovers the runs, analyzes each one, writes the per-run
// summaries and assembles the aggregate document.
func analyzeRoot(root string) (*allSummary, error) {
	runs, err := discoverRuns(root)
	if err != nil {
		return nil, err
	}
	all := &allSummary{
		Schema:      schemaTag,
		EvidenceDir: root,
		GeneratedAt: time.Now().Format(time.RFC3339),
		Runs:        []*runSummary{},
		Skipped:     []skippedRun{},
		Anomalies:   []anomaly{},
		Notes:       globalNotes(),
	}
	for _, ri := range runs {
		rs := analyzeRun(root, ri)
		if rs == nil {
			all.Skipped = append(all.Skipped, skippedRun{
				Label:  ri.Label,
				Reason: "无可解析的 report.json/anchors.json/meta.json",
			})
			continue
		}
		if err := writeJSONFile(filepath.Join(ri.Dir, "summary.json"), rs); err != nil {
			return nil, fmt.Errorf("write %s/summary.json: %w", ri.Label, err)
		}
		all.Runs = append(all.Runs, rs)
	}
	if len(all.Runs) == 0 {
		return nil, fmt.Errorf("no parseable run under %s", filepath.Join(root, "runs"))
	}
	all.Comparison = compareRuns(all.Runs)
	all.Anomalies = append(all.Anomalies, collectAnomalies(all.Runs)...)
	return all, nil
}

// globalNotes are the frozen statement-discipline notes carried into both
// output documents.
func globalNotes() []string {
	return []string{
		"所有百分比仅诊断参考，非 SLO/阈值。",
		"组内/组间统计为描述性汇总：n 小、方差未测；不得因一次小差值声称『无扰动』或其他因果结论。",
		"分位数：线性插值，rank = p·(n−1)。",
		"tx_total 口径：commit 行 (t_us+dur_us) − begin 行 t_us；无 begin/commit 的事件跳过并计数。",
		"嵌套容差：Σsql ≤ process_dur + 行数×1µs；tx_total ≤ process_dur + 2µs（µs 量化裕度）。",
		"tail 裁剪：窗口 [publish_done_us, poll_confirm_us]；partial 仅计窗内交叠；residual = tail − Σ窗内覆盖。",
		"report↔anchors 对账：秒值容差 1e-6（=1µs，锚点分辨率）；速率容差 = 1e-6 + |rate|·1µs/时长（µs 量化传播）；report 的 *_seconds 扩展字段是相对 drain_start 的偏移。",
		"检测延迟为锚点差值（µs）；applied_cb_latency 是回调与提交排序之差，可为负（回调在事件处理期间触发）。",
		"锚点时间字段的 0 是「未采集」哨兵（如 OFF 臂无 tracer/回调行）：对应延迟渲染为 -（not collected），report 对应字段须同为 0/缺失，绝不做 0 的偏移换算。",
		"顶层时长为 s，检测延迟为 µs，分段/采样时长为 µs（除标注外）。",
	}
}

// writeJSONFile writes one indented JSON document with a trailing newline.
func writeJSONFile(path string, doc any) error {
	body, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return err
	}
	body = append(body, '\n')
	return os.WriteFile(path, body, 0o644)
}

// writeAggregates writes summary_all.json and summary.md into outDir.
func writeAggregates(outDir string, all *allSummary) error {
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}
	if err := writeJSONFile(filepath.Join(outDir, "summary_all.json"), all); err != nil {
		return fmt.Errorf("write summary_all.json: %w", err)
	}
	md := renderMarkdown(all)
	if err := os.WriteFile(filepath.Join(outDir, "summary.md"), []byte(md), 0o644); err != nil {
		return fmt.Errorf("write summary.md: %w", err)
	}
	return nil
}

// printReport prints the short stdout digest of one analyzer invocation.
func printReport(w io.Writer, all *allSummary, outDir string) {
	groups := map[string]int{}
	for _, r := range all.Runs {
		groups[r.Group]++
	}
	names := make([]string, 0, len(groups))
	for g := range groups {
		names = append(names, g)
	}
	sort.Strings(names)
	parts := make([]string, 0, len(names))
	for _, g := range names {
		parts = append(parts, fmt.Sprintf("%s=%d", g, groups[g]))
	}
	fmt.Fprintf(w, "analyze: evidence=%s runs=%d (%s) anomalies=%d skipped=%d\n",
		all.EvidenceDir, len(all.Runs), strings.Join(parts, ", "), len(all.Anomalies), len(all.Skipped))
	for _, r := range all.Runs {
		fmt.Fprintf(w, "  %-14s group=%-8s collection=%-8s checks: anchors=%s counts=%s drops=%s overlap=%s nesting=%s report=%s\n",
			r.Label, r.Group, r.Collection,
			mark(r.Checks.AnchorsComplete), mark(r.Checks.CountsOK), mark(r.Checks.DropsOK),
			mark(r.Checks.OverlapOK), mark(r.Checks.NestingOK), mark(r.Checks.ReportVsAnchorsOK))
	}
	for _, s := range all.Skipped {
		fmt.Fprintf(w, "  %-14s skipped (%s)\n", s.Label, s.Reason)
	}
	fmt.Fprintf(w, "wrote %d per-run summary.json; %s and %s\n",
		len(all.Runs), filepath.Join(outDir, "summary_all.json"), filepath.Join(outDir, "summary.md"))
}
