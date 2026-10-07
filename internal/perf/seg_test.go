//go:build perf

// seg_test.go is the controlled four-condition, single-state segment batch
// (normal query path only; no fault matrix): for every arm parsed from
// TXHARBOR_SEG_ARMS it boots one full-stack environment, optionally collects
// the server segment/statement timeline, runs the fixed warm-up and one normal
// steady-state window, and persists the frozen evidence layout:
//
//	<dir>/repeat<N>/<arm>/client_samples.jsonl    steady window samples (verbatim)
//	<dir>/repeat<N>/<arm>/warmup_samples.jsonl    warm-up samples (never merged)
//	<dir>/repeat<N>/<arm>/server_segments.jsonl   seg + pgq records
//	<dir>/repeat<N>/<arm>/serve_stderr.log        captured serve stderr
//	<dir>/repeat<N>/<arm>/meta.json               arm metadata (frozen keys)
//
// Environment knobs:
//
//	TXHARBOR_SEG_ARMS          arm list, e.g. "R0G0,R1G0,R0G1,R1G1" (unset = skip)
//	TXHARBOR_SEG_EVIDENCE_DIR  batch evidence root (unset = skip)
//	TXHARBOR_SEG_REPEAT        repeat index, default 1 (0 = overhead runs)
//	TXHARBOR_SEG_TIMEOUT_MIN   batch context timeout in minutes, default 30
//
// The arms run serially in the given array order; the first failure aborts the
// batch (diagnostics are written before failing).
package perf

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"

	"github.com/xtianxx/txharbor/internal/app"
	"github.com/xtianxx/txharbor/internal/db"
)

// perfSegTraceEnv gates the db query tracer; it must be decided before the
// pool is built (the tracer attaches at pool construction).
const perfSegTraceEnv = "TXHARBOR_PERF_SEG_TRACE"

// TestSegNormalBatch runs one controlled segment batch: every arm of
// TXHARBOR_SEG_ARMS, serially, on the normal query path only.
func TestSegNormalBatch(t *testing.T) {
	armsRaw := os.Getenv("TXHARBOR_SEG_ARMS")
	root := os.Getenv("TXHARBOR_SEG_EVIDENCE_DIR")
	if armsRaw == "" || root == "" {
		t.Skip("TXHARBOR_SEG_ARMS/TXHARBOR_SEG_EVIDENCE_DIR unset: controlled segment batch not requested")
	}
	arms, err := ParseSegArms(armsRaw)
	if err != nil {
		t.Fatalf("perf seg: %v", err)
	}
	repeat := 1
	if raw := os.Getenv("TXHARBOR_SEG_REPEAT"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 0 {
			t.Fatalf("perf seg: invalid TXHARBOR_SEG_REPEAT %q", raw)
		}
		repeat = n
	}
	timeoutMin := 30
	if raw := os.Getenv("TXHARBOR_SEG_TIMEOUT_MIN"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			t.Fatalf("perf seg: invalid TXHARBOR_SEG_TIMEOUT_MIN %q", raw)
		}
		timeoutMin = n
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatalf("perf seg: create evidence root: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutMin)*time.Minute)
	defer cancel()
	testcontainers.SkipIfProviderIsNotHealthy(t)
	t.Cleanup(func() { _ = os.Unsetenv(perfSegTraceEnv) })

	t.Logf("perf seg batch: arms=%v repeat=%d timeout=%dm evidence=%s", armsRaw, repeat, timeoutMin, root)
	for orderIndex, arm := range arms {
		t.Logf("perf seg: arm %s (#%d) limiter=%v background=%v collect=%v",
			arm.Name, orderIndex, arm.LimiterOn, arm.BackgroundOn, arm.Collect)
		segRunArm(t, ctx, root, repeat, orderIndex, arm)
	}
}

// segRunArm runs one arm end to end: tracer knob, environment, warm-up,
// steady window, evidence, stop.
func segRunArm(t *testing.T, ctx context.Context, root string, repeat, orderIndex int, arm Arm) {
	t.Helper()
	armDir := filepath.Join(root, fmt.Sprintf("repeat%d", repeat), arm.Name)
	if err := os.MkdirAll(armDir, 0o755); err != nil {
		t.Fatalf("perf seg: create arm dir: %v", err)
	}
	notes := []string{
		"order_index 为本调用内的臂序号（0 基）；跨调用的全局顺序由 run_batch.sh 的固定调用顺序决定",
		"warmup 样本只写入 warmup_samples.jsonl，绝不并入稳态统计",
		"counts.errors/status_counts 覆盖稳态窗口的全部类别样本；query_samples 只数 query 类",
		"server_segments.jsonl 同时含 seg/pgq 记录，覆盖预热+稳态窗口（按 meta 时间戳分桶；接缝只插桩 /withdrawals 路径）",
		"meta 额外键 log_counts 与 metrics_scrape{before,after} 为本批次补充证据",
	}

	// (a) The tracer knob must be decided before the pool is built.
	if arm.Collect {
		if err := os.Setenv(perfSegTraceEnv, "1"); err != nil {
			t.Fatalf("perf seg: set %s: %v", perfSegTraceEnv, err)
		}
	} else if err := os.Unsetenv(perfSegTraceEnv); err != nil {
		t.Fatalf("perf seg: unset %s: %v", perfSegTraceEnv, err)
	}

	// (b) One full-stack environment; only the background runtimes follow the
	// arm.
	startedAt := time.Now().UTC()
	setupStart := time.Now()
	env := startEnvWith(t, ctx, PathFullStack, repeat, BenchProfile,
		&EvidenceWriter{dir: armDir}, EnvOpts{Background: arm.BackgroundOn})
	setupSeconds := time.Since(setupStart).Seconds()
	defer env.stop() // idempotent safety net; stopped explicitly below

	// (c) Per-request seams. Both are process-global, so the closing defers
	// restore the defaults for the next arm.
	collector := newSegCollector()
	if arm.Collect {
		app.PerfEnableSeg(collector)
		db.PerfSetQueryRecorder(collector.RecordQuery)
		defer func() {
			db.PerfSetQueryRecorder(nil)
			app.PerfDisableSeg()
		}()
	} else {
		notes = append(notes, "本臂 collect=false：未采集分段/语句记录（seg_records=pg_records=0）")
	}
	app.PerfSetQueryLimiterEnabled(arm.LimiterOn)
	defer app.PerfSetQueryLimiterEnabled(true)
	if !arm.LimiterOn {
		notes = append(notes, "本臂 limiter_on=false：查询类准入不走限流器")
	}

	// (d) Warm-up: the committed generator shape, recorded but never merged
	// into the steady-state statistics.
	warmupProfile := segWarmupProfile(BenchProfile)
	warmupStart := time.Now().UTC()
	warmupSamples := env.runLoadWindow(t, ctx, StateNormal, warmupProfile, true)
	warmupEnd := time.Now().UTC()
	t.Logf("perf seg[%s]: warm-up %s -> %d samples", arm.Name, warmupProfile.StateWindow, len(warmupSamples))

	if alive, code := env.serveAlive(); !alive {
		env.writeDiagnostics(fmt.Sprintf("serve_%s_run%d", PathFullStack, repeat))
		t.Fatalf("perf seg[%s]: serve exited during warm-up (code %d); stderr:\n%s", arm.Name, code, env.serveErr.String())
	}
	if err := env.checkServe(); err != nil {
		env.writeDiagnostics(fmt.Sprintf("serve_%s_run%d", PathFullStack, repeat))
		t.Fatalf("perf seg[%s]: %v%s", arm.Name, err, env.diagnostics())
	}

	// (e) Before snapshot.
	before := segSnapshotArm(t, ctx, env, arm.Name+"/before")

	// (f) Steady-state window: StateNormal only, the fixed committed profile.
	windowStart := time.Now().UTC()
	steadySamples := env.runLoadWindow(t, ctx, StateNormal, BenchProfile, true)
	windowEnd := time.Now().UTC()

	// (i, part 1) A dead serve invalidates every HTTP measurement.
	if alive, code := env.serveAlive(); !alive {
		env.writeDiagnostics(fmt.Sprintf("serve_%s_run%d", PathFullStack, repeat))
		t.Fatalf("perf seg[%s]: serve exited during the steady window (code %d); stderr:\n%s", arm.Name, code, env.serveErr.String())
	}
	if err := env.checkServe(); err != nil {
		env.writeDiagnostics(fmt.Sprintf("serve_%s_run%d", PathFullStack, repeat))
		t.Fatalf("perf seg[%s]: %v%s", arm.Name, err, env.diagnostics())
	}

	// (g) After snapshot plus the runtime progress and log counters.
	after := segSnapshotArm(t, ctx, env, arm.Name+"/after")
	logs := env.logCounts()
	progress := segArmRuntimeProgress{KafkaEndOffsets: -1, KafkaCommitted: -1}
	if arm.BackgroundOn {
		probeCtx, probeCancel := context.WithTimeout(ctx, 3*time.Second)
		if ends, err := env.kafkaEndOffsets(probeCtx); err == nil {
			progress.KafkaEndOffsets = ends
		} else {
			notes = append(notes, fmt.Sprintf("runtime_progress: Kafka 高水位采样失败：%v", err))
		}
		if committed, err := env.kafkaCommitted(probeCtx); err == nil {
			progress.KafkaCommitted = committed
		} else {
			notes = append(notes, fmt.Sprintf("runtime_progress: 消费组位点采样失败：%v", err))
		}
		probeCancel()
	} else {
		notes = append(notes, "本臂 background_on=false：发布器/消费者未启动，runtime_progress 记 -1")
	}

	if len(steadySamples) == 0 {
		env.writeDiagnostics(fmt.Sprintf("serve_%s_run%d", PathFullStack, repeat))
		t.Fatalf("perf seg[%s]: steady window recorded no samples%s", arm.Name, env.diagnostics())
	}
	segs, stmts := collector.Counts()
	counts := segArmCounts{
		ClientSamples: len(steadySamples),
		WarmupSamples: len(warmupSamples),
		SegRecords:    segs,
		PgRecords:     stmts,
		QuerySamples:  segClassCount(steadySamples, "query"),
		Errors:        segErrorCount(steadySamples),
		StatusCounts:  segSampleStatusCounts(steadySamples),
	}
	if counts.QuerySamples == 0 {
		t.Fatalf("perf seg[%s]: steady window recorded no query samples", arm.Name)
	}

	meta := segArmMeta{
		Repeat:     repeat,
		OrderIndex: orderIndex,
		Arm: segArmMetaArm{
			Name:         arm.Name,
			LimiterOn:    arm.LimiterOn,
			BackgroundOn: arm.BackgroundOn,
			Collect:      arm.Collect,
		},
		Commit:            detectCommit(),
		GoVersion:         runtime.Version(),
		StartedAt:         startedAt,
		SetupSeconds:      setupSeconds,
		WarmupStart:       warmupStart,
		WarmupEnd:         warmupEnd,
		WindowStart:       windowStart,
		WindowEnd:         windowEnd,
		EnvSpec:           env.Spec,
		EnvMapRedacted:    segArmRedactedEnvMap(env.EnvMap),
		PoolStatBefore:    before.Pool,
		PoolStatAfter:     after.Pool,
		LimiterPoolBefore: before.Limiter,
		LimiterPoolAfter:  after.Limiter,
		DurableBefore:     before.Durable,
		DurableAfter:      after.Durable,
		MetricsScrape:     segArmScrape{Before: before.Scrape, After: after.Scrape},
		Resources:         resourceDelta(before.Resources, after.Resources),
		RuntimeProgress:   progress,
		LogCounts:         segArmLogCounts{PublishFailures: logs.publishFailures, Quarantine: logs.quarantine},
		Counts:            counts,
		Notes:             notes,
	}
	if before.Limiter == nil || after.Limiter == nil {
		notes = append(notes, "limiter_pool_* 为 null：本环境未提供限流器 Redis client")
		meta.Notes = notes
	}
	if before.Pool.Serve.TotalConns == 0 && before.Pool.Serve.MaxConns == 0 {
		notes = append(notes, "serve 池 stat 全零：未取到 perf seam 注册的 serve 池")
		meta.Notes = notes
	}

	// (h) Evidence: steady samples verbatim, warm-up samples separate, the
	// segment/statement records, then the captured serve stderr.
	if err := segWriteJSONL(filepath.Join(armDir, "client_samples.jsonl"), steadySamples); err != nil {
		t.Fatalf("perf seg[%s]: client samples: %v", arm.Name, err)
	}
	if err := segWriteJSONL(filepath.Join(armDir, "warmup_samples.jsonl"), warmupSamples); err != nil {
		t.Fatalf("perf seg[%s]: warmup samples: %v", arm.Name, err)
	}
	if err := collector.Flush(armDir); err != nil {
		t.Fatalf("perf seg[%s]: segment evidence: %v", arm.Name, err)
	}

	// (i, part 2) Stop, then persist the final stderr and the metadata.
	env.stop()
	if err := os.WriteFile(filepath.Join(armDir, "serve_stderr.log"), []byte(env.serveErr.String()), 0o644); err != nil {
		t.Fatalf("perf seg[%s]: serve stderr: %v", arm.Name, err)
	}
	meta.FinishedAt = time.Now().UTC()
	if err := segWriteJSON(filepath.Join(armDir, "meta.json"), meta); err != nil {
		t.Fatalf("perf seg[%s]: meta: %v", arm.Name, err)
	}
	t.Logf("perf seg[%s]: steady=%d (query=%d errors=%d) seg=%d pgq=%d -> %s",
		arm.Name, counts.ClientSamples, counts.QuerySamples, counts.Errors, counts.SegRecords, counts.PgRecords, armDir)
}

// segWarmupProfile is the committed warm-up shape: the first ladder step only,
// no burst, the same create/deposit rates.
func segWarmupProfile(profile Profile) Profile {
	ladder := []LadderStep{{Offset: 0, RPS: 10}}
	if len(profile.QueryLadder) > 0 {
		ladder[0].RPS = profile.QueryLadder[0].RPS
	}
	return Profile{
		StateWindow: profile.Warmup,
		QueryLadder: ladder,
		Burst:       BurstSpec{},
		CreateRPS:   profile.CreateRPS,
		DepositRPS:  profile.DepositRPS,
	}
}

// --- frozen meta shape -------------------------------------------------------

type segArmMetaArm struct {
	Name         string `json:"name"`
	LimiterOn    bool   `json:"limiter_on"`
	BackgroundOn bool   `json:"background_on"`
	Collect      bool   `json:"collect"`
}

type segArmPoolStat struct {
	TotalConns           int32 `json:"total_conns"`
	AcquiredConns        int32 `json:"acquired_conns"`
	IdleConns            int32 `json:"idle_conns"`
	MaxConns             int32 `json:"max_conns"`
	AcquireCount         int64 `json:"acquire_count"`
	AcquireDurationNS    int64 `json:"acquire_duration_ns"`
	EmptyAcquireCount    int64 `json:"empty_acquire_count"`
	CanceledAcquireCount int64 `json:"canceled_acquire_count"`
	NewConnsCount        int64 `json:"new_conns_count"`
}

type segArmPoolPair struct {
	Serve   segArmPoolStat `json:"serve"`
	Harness segArmPoolStat `json:"harness"`
}

type segArmLimiterPool struct {
	Hits       uint32 `json:"hits"`
	Misses     uint32 `json:"misses"`
	Timeouts   uint32 `json:"timeouts"`
	TotalConns uint32 `json:"total_conns"`
	IdleConns  uint32 `json:"idle_conns"`
	StaleConns uint32 `json:"stale_conns"`
}

type segArmScrape struct {
	Before ScrapedMetrics `json:"before"`
	After  ScrapedMetrics `json:"after"`
}

type segArmRuntimeProgress struct {
	KafkaEndOffsets int64 `json:"kafka_end_offsets"`
	KafkaCommitted  int64 `json:"kafka_committed"`
}

type segArmLogCounts struct {
	PublishFailures int `json:"publish_failures_logged"`
	Quarantine      int `json:"quarantine_logged"`
}

type segArmCounts struct {
	ClientSamples int            `json:"client_samples"`
	WarmupSamples int            `json:"warmup_samples"`
	SegRecords    int            `json:"seg_records"`
	PgRecords     int            `json:"pg_records"`
	QuerySamples  int            `json:"query_samples"`
	Errors        int            `json:"errors"`
	StatusCounts  map[string]int `json:"status_counts"`
}

type segArmMeta struct {
	Repeat            int                   `json:"repeat"`
	OrderIndex        int                   `json:"order_index"`
	Arm               segArmMetaArm         `json:"arm"`
	Commit            string                `json:"commit"`
	GoVersion         string                `json:"go_version"`
	StartedAt         time.Time             `json:"started_at"`
	FinishedAt        time.Time             `json:"finished_at"`
	SetupSeconds      float64               `json:"setup_seconds"`
	WarmupStart       time.Time             `json:"warmup_start"`
	WarmupEnd         time.Time             `json:"warmup_end"`
	WindowStart       time.Time             `json:"window_start"`
	WindowEnd         time.Time             `json:"window_end"`
	EnvSpec           EnvSpec               `json:"env_spec"`
	EnvMapRedacted    map[string]string     `json:"env_map_redacted"`
	PoolStatBefore    segArmPoolPair        `json:"pool_stat_before"`
	PoolStatAfter     segArmPoolPair        `json:"pool_stat_after"`
	LimiterPoolBefore *segArmLimiterPool    `json:"limiter_pool_before"`
	LimiterPoolAfter  *segArmLimiterPool    `json:"limiter_pool_after"`
	DurableBefore     DurableState          `json:"durable_before"`
	DurableAfter      DurableState          `json:"durable_after"`
	MetricsScrape     segArmScrape          `json:"metrics_scrape"`
	Resources         ResourceDelta         `json:"resources"`
	RuntimeProgress   segArmRuntimeProgress `json:"runtime_progress"`
	LogCounts         segArmLogCounts       `json:"log_counts"`
	Counts            segArmCounts          `json:"counts"`
	Notes             []string              `json:"notes"`
}

// segArmSnapshot is the before/after observation set.
type segArmSnapshot struct {
	Pool      segArmPoolPair
	Limiter   *segArmLimiterPool
	Durable   DurableState
	Resources ResourceSample
	Scrape    ScrapedMetrics
}

// segSnapshotArm takes one before/after snapshot: serve pool stat, limiter
// pool stats, harness pool stat, durable state, process resources, metrics.
func segSnapshotArm(t *testing.T, ctx context.Context, env *Env, phase string) segArmSnapshot {
	t.Helper()
	servePool := segArmPoolStatOf(app.PerfServePool())
	limiter := segArmLimiterPoolOf(app.PerfLimiterClient())
	harnessPool := segArmPoolStatOf(env.Pool)
	durable, err := env.sampleState(ctx)
	if err != nil {
		t.Fatalf("perf seg: durable snapshot (%s): %v%s", phase, err, env.diagnostics())
	}
	return segArmSnapshot{
		Pool:      segArmPoolPair{Serve: servePool, Harness: harnessPool},
		Limiter:   limiter,
		Durable:   durable,
		Resources: env.readResources(),
		Scrape:    env.scrapeServeMetrics(ctx),
	}
}

// segArmPoolStatOf renders one pgx pool stat (zero value for a nil pool).
func segArmPoolStatOf(pool *pgxpool.Pool) segArmPoolStat {
	if pool == nil {
		return segArmPoolStat{}
	}
	stat := pool.Stat()
	return segArmPoolStat{
		TotalConns:           stat.TotalConns(),
		AcquiredConns:        stat.AcquiredConns(),
		IdleConns:            stat.IdleConns(),
		MaxConns:             stat.MaxConns(),
		AcquireCount:         stat.AcquireCount(),
		AcquireDurationNS:    int64(stat.AcquireDuration()),
		EmptyAcquireCount:    stat.EmptyAcquireCount(),
		CanceledAcquireCount: stat.CanceledAcquireCount(),
		NewConnsCount:        stat.NewConnsCount(),
	}
}

// segArmLimiterPoolOf renders the limiter client pool stats (nil when the
// environment exposes no limiter client).
func segArmLimiterPoolOf(client *redis.Client) *segArmLimiterPool {
	if client == nil {
		return nil
	}
	stat := client.PoolStats()
	return &segArmLimiterPool{
		Hits:       stat.Hits,
		Misses:     stat.Misses,
		Timeouts:   stat.Timeouts,
		TotalConns: stat.TotalConns,
		IdleConns:  stat.IdleConns,
		StaleConns: stat.StaleConns,
	}
}

// segArmRedactedEnvMap copies the serve environment with credential-bearing
// values replaced.
func segArmRedactedEnvMap(env map[string]string) map[string]string {
	out := make(map[string]string, len(env))
	for key, value := range env {
		if segArmSensitiveKey(key) {
			out[key] = "<redacted>"
			continue
		}
		out[key] = value
	}
	return out
}

func segArmSensitiveKey(key string) bool {
	upper := strings.ToUpper(key)
	for _, needle := range []string{"DSN", "PASSWORD", "SECRET", "TOKEN", "CREDENTIAL"} {
		if strings.Contains(upper, needle) {
			return true
		}
	}
	return false
}

// --- small helpers -----------------------------------------------------------

func segClassCount(samples []Sample, class string) int {
	n := 0
	for _, sample := range samples {
		if sample.Class == class {
			n++
		}
	}
	return n
}

func segErrorCount(samples []Sample) int {
	n := 0
	for _, sample := range samples {
		if !sample.OK {
			n++
		}
	}
	return n
}

func segSampleStatusCounts(samples []Sample) map[string]int {
	counts := map[string]int{}
	for _, sample := range samples {
		counts[strconv.Itoa(sample.Status)]++
	}
	return counts
}

func segWriteJSONL(path string, rows []Sample) error {
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	for _, row := range rows {
		if err := encoder.Encode(row); err != nil {
			return err
		}
	}
	return os.WriteFile(path, buf.Bytes(), 0o644)
}

func segWriteJSON(path string, doc any) error {
	raw, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(raw, '\n'), 0o644)
}
