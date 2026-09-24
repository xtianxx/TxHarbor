//go:build perf

// bench_test.go is the T077 A/B benchmark execution (V-BENCH; SC-11; FR-25;
// adr.md §3/§4; PD-3):
//
//  1. TestHarnessSkeleton is the T076 completion check: the harness boots the
//     PG-only path, runs one short fixed-shape load window and records the
//     environment spec — proving the harness runs a skeleton scenario;
//  2. TestBenchmarkAB runs the full fixed profile for path A (PG-only) and
//     path B (full stack), repeat after repeat, with the identical load
//     generator and fault timeline, then renders the V-BENCH report from the
//     measured data.
//
// Environment knobs (all recorded in the environment spec):
//
//	TXHARBOR_PERF_EVIDENCE_DIR   evidence directory (default: temp dir)
//	TXHARBOR_PERF_REPEATS        repeats per path (default 1)
//	TXHARBOR_PERF_STATE_WINDOW   debug override of the state window
//	TXHARBOR_PERF_WARMUP         debug override of the warm-up
//
// The committed report uses the fixed BenchProfile (no overrides).
package perf

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

// TestHarnessSkeleton is the T076 completion check: a runnable skeleton
// scenario on the PG-only path with the fixed generator shape, one short
// window and a recorded environment spec. It is intentionally cheap (no
// Redis/Kafka) so the harness itself can be validated before the full A/B
// run.
func TestHarnessSkeleton(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	evidence, err := NewEvidenceWriter()
	if err != nil {
		t.Fatalf("evidence writer: %v", err)
	}
	profile := BenchProfile
	profile.Warmup = time.Second
	profile.StateWindow = 3 * time.Second

	env := startEnv(t, ctx, PathPGOnly, 0, profile, evidence)
	defer env.stop()

	// One skeleton window: the fixed ladder shape at a reduced window; the
	// assertion is that the real listener answered the real load (the full
	// benchmark produces the measured comparison).
	samples := env.runLoadWindow(t, ctx, StateNormal, Profile{
		StateWindow: 3 * time.Second,
		QueryLadder: profile.QueryLadder,
		Burst:       BurstSpec{},
		CreateRPS:   1,
		DepositRPS:  1,
	}, true)
	queries := computeClassStats("query", samples, 3)
	creates := computeClassStats("create", samples, 3)
	if queries.Count == 0 {
		t.Fatalf("skeleton: no query samples%s", env.diagnostics())
	}
	if queries.Errors != 0 {
		t.Fatalf("skeleton: query errors = %d, want 0%s", queries.Errors, env.diagnostics())
	}
	if creates.Count == 0 || creates.Errors != 0 {
		t.Fatalf("skeleton: create samples=%d errors=%d, want >0/0%s", creates.Count, creates.Errors, env.diagnostics())
	}
	after, err := env.sampleState(ctx)
	if err != nil {
		t.Fatalf("skeleton state: %v", err)
	}
	if after.OutboxPending == 0 {
		t.Fatalf("skeleton: outbox pending = 0, want accumulated path-A rows (013 wiring off, Append unconditional)")
	}
	if err := evidence.WriteJSON("skeleton_result.json", map[string]any{
		"path": PathPGOnly, "query": queries, "create": creates, "state_after": after,
	}); err != nil {
		t.Fatalf("write skeleton evidence: %v", err)
	}
	t.Logf("perf skeleton: query n=%d p95=%.1fms create n=%d outbox_pending=%d (evidence: %s)",
		queries.Count, queries.P95MS, creates.Count, after.OutboxPending, evidence.Dir())
}

// TestReportRendererFields pins that the renderer produces every required
// V-BENCH section and never renders an unmeasured value as "已达标". It uses
// synthetic inputs and writes nothing: it is a renderer field test, not
// evidence.
func TestReportRendererFields(t *testing.T) {
	state := func(kind FaultState, p95 float64) StateResult {
		return StateResult{
			State: kind, Label: StateLabel[kind], WindowSeconds: 12,
			Query:     ClassStats{Class: "query", Count: 100, P50MS: p95 / 2, P95MS: p95, P99MS: p95 * 2, ErrorRate: 0.01},
			Create:    ClassStats{Class: "create", Count: 24, P95MS: 20, ErrorRate: 0.5},
			Deposit:   ClassStats{Class: "deposit", Count: 12},
			Before:    DurableState{},
			After:     DurableState{OutboxPending: 10, OutboxAttemptSum: 20, CatchupGap: 3},
			Resources: ResourceDelta{CPUSeconds: 1.5, RSSBytes: 100 << 20},
		}
	}
	build := func(path Path, scale float64) PathResult {
		result := PathResult{Path: path, Run: 1, Spec: EnvSpec{PGImage: "pg", AnvilImage: "anvil", KafkaImage: "kafka", RedisImage: "redis", KafkaTopic: "topic", DatasetScale: "synthetic"}}
		for _, kind := range StateSequence {
			s := state(kind, 10*scale)
			if path == PathPGOnly {
				s.After.CatchupGap = -1
				s.CatchupSeconds = -1
				s.DrainSeconds = -1
			} else {
				s.CatchupSeconds = 1.5
				s.DrainSeconds = 3.5
			}
			result.States = append(result.States, s)
		}
		return result
	}
	report := RenderReport(ReportInput{
		Commit: "synthetic", GoVersion: "go", Profile: BenchProfile, Host: HostSpec{CPUCores: 8},
		Runs:   []BenchRun{{Run: 1, PathA: build(PathPGOnly, 1), PathB: build(PathFullStack, 1.2)}},
		RawDir: "/tmp/synthetic", GeneratedAt: time.Now(),
	})
	for _, want := range []string{
		"# 013 对照基准报告（V-BENCH / SC-11）",
		"## 1. 方法", "## 2. 环境规格", "## 3. 路径 A", "## 4. 路径 B",
		"## 5. A/B 对照", "## 6. 资源占用", "## 7. 重复运行与测量误差",
		"## 8. 结论与置信限制", "## 9. 未测 / 待裁决",
		"commit: `synthetic`", "查询 p95", "追赶时间", "PD-3",
		// The §6/§8 RSS medians are MiB, never raw bytes: the synthetic
		// input is 100 MiB per path, so the rendered line must say 100.0.
		"RSS A=100.0 MiB，B=100.0 MiB",
	} {
		if !strings.Contains(report, want) {
			t.Errorf("report lacks %q", want)
		}
	}
	// "已达标" may appear only inside the discipline disclaimer
	// ("0 次表述为「已达标」"), never as a claim.
	if got, quoted := strings.Count(report, "已达标"), strings.Count(report, "0 次表述为「已达标」"); got != quoted {
		t.Errorf("report renders a 已达标 claim (%d occurrences, %d in the disclaimer); unmeasured values must stay 待测/待裁决", got, quoted)
	}
	// An unmeasured (NaN) value renders as 待测, never as a number.
	if got := num(math.NaN()); got != "待测" {
		t.Errorf("num(NaN) = %q, want 待测", got)
	}
}

// TestBenchmarkAB runs the full A/B benchmark and renders the V-BENCH report.
func TestBenchmarkAB(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx, cancel := context.WithTimeout(context.Background(), 55*time.Minute)
	defer cancel()

	profile := benchProfileFromEnv()
	repeats := 1
	if raw := os.Getenv("TXHARBOR_PERF_REPEATS"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			repeats = n
		}
	}
	evidence, err := NewEvidenceWriter()
	if err != nil {
		t.Fatalf("evidence writer: %v", err)
	}
	host := captureHostSpec()
	t.Logf("perf benchmark: repeats=%d window=%s warmup=%s evidence=%s",
		repeats, profile.StateWindow, profile.Warmup, evidence.Dir())

	var runs []BenchRun
	for run := 1; run <= repeats; run++ {
		pathA := runPath(t, ctx, PathPGOnly, run, profile, evidence)
		pathB := runPath(t, ctx, PathFullStack, run, profile, evidence)
		runs = append(runs, BenchRun{Run: run, PathA: pathA, PathB: pathB})
	}

	report := RenderReport(ReportInput{
		Commit:      detectCommit(),
		GoVersion:   runtime.Version(),
		Profile:     profile,
		Host:        host,
		Runs:        runs,
		RawDir:      evidence.Dir(),
		GeneratedAt: time.Now(),
	})
	if err := evidence.WriteJSON("host_spec.json", host); err != nil {
		t.Fatalf("write host spec: %v", err)
	}
	if err := evidence.WriteFile("benchmark_report.md", []byte(report)); err != nil {
		t.Fatalf("write report: %v", err)
	}
	t.Logf("V-BENCH report written: %s", filepath.Join(evidence.Dir(), "benchmark_report.md"))
}
