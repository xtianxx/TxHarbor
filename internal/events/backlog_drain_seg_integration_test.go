//go:build integration_backlog && perf

// backlog_drain_seg_integration_test.go is the instrumented (ON/OFF) twin of
// the 013 larger-backlog drain drill: it mirrors TestBacklogDrain's load,
// middleware, drain and assertion flow line by line, and adds the segmented
// measurement of the consumer catch-up phase (013 supplement, V-CATCHUP).
//
// What it adds on top of the pristine drill:
//
//   - the consumer-side seam of internal/events (perf build tag) is armed with
//     a recorder, so poll/process/mark/lag/rebalance spans and the applied
//     callback are timestamped on the consumer's own goroutine;
//   - the harness pool carries a pgx.QueryTracer that attributes every
//     statement the consumer issues to the event sequence it ran under, using
//     a private context mark on the consumer's run context (every other
//     caller keeps an unmarked context and is forwarded without allocation);
//   - the monitor tick additionally records the pool's cumulative counters, so
//     the ON and OFF arms have matched sampling;
//   - anchors.json pins the phase boundaries (publish drain start, last
//     publish cycle, pending=0, consumer confirm, consumer stop, last commit
//     and last applied callback) on the same monotonic base as the samples.
//
// Arms (TXHARBOR_BACKLOG_SEG):
//
//	on   the full segmented collection: span rows, SQL spans and CSV archives;
//	off  the identical instrumented run with collection off: the same seam
//	     calls, pairing checks and tallies, no span rows, no tracer and no
//	     loop/sql/publish CSV. meta/anchors/samples/report are written either
//	     way, so the two arms stay comparable.
//
// Environment knobs (in addition to the pristine harness's knobs):
//
//	TXHARBOR_BACKLOG_SEG      "" / "0" = off, "1" = on; any other value is
//	                          refused fail-closed.
//	TXHARBOR_BACKLOG_SEG_DIR  evidence directory of this run; required when
//	                          SEG=1, optional when off (no directory means no
//	                          archive). Every path this harness writes comes
//	                          from here or from TXHARBOR_BACKLOG_REPORT; no
//	                          DSN, credential or broker address is ever
//	                          written.
//
// Layer and CI discipline: the dedicated `integration_backlog` build tag still
// applies and `perf` is required on top of it. Run explicitly, for example:
//
//	go test -tags "integration_backlog perf" -count=1 -timeout 40m \
//	  -run '^TestBacklogDrainSeg$' -v ./internal/events/
//
// Statement discipline is unchanged: delivery is at-least-once and processing
// is idempotent; this harness never claims cross-system exactly-once.
package events_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"

	"github.com/xtianxx/txharbor/internal/events"
	"github.com/xtianxx/txharbor/internal/testutil"
)

// Segmented-measurement knobs and the frozen harness name of this layer.
const (
	backlogSegEnv     = "TXHARBOR_BACKLOG_SEG"
	backlogSegDirEnv  = "TXHARBOR_BACKLOG_SEG_DIR"
	backlogSegHarness = "013-supplement/consumer-seg/1"

	backlogSegModeOn  = "on"
	backlogSegModeOff = "off"
)

// backlogSegParams parses the SEG switch and the archive directory. An empty
// or "0" switch is off, "1" is on and anything else is refused fail-closed;
// an ON run without a directory could not archive its evidence and is
// refused too.
func backlogSegParams(segRaw, dirRaw string) (enabled bool, dir string, err error) {
	switch strings.TrimSpace(segRaw) {
	case "", "0":
		enabled = false
	case "1":
		enabled = true
	default:
		return false, "", fmt.Errorf("%s=%q must be empty, %q or %q", backlogSegEnv, segRaw, "0", "1")
	}
	dir = strings.TrimSpace(dirRaw)
	if enabled && dir == "" {
		return false, "", fmt.Errorf("%s=1 requires %s to name this run's evidence directory", backlogSegEnv, backlogSegDirEnv)
	}
	return enabled, dir, nil
}

// backlogSegMetaConfigSink is the sink section of meta.json's config.
type backlogSegMetaConfigSink struct {
	DeliveryTimeoutMs int `json:"delivery_timeout_ms"`
	RequestTimeoutMs  int `json:"request_timeout_ms"`
	MaxInflight       int `json:"max_inflight"`
	MaxBuffered       int `json:"max_buffered"`
}

// backlogSegMetaConfig pins every bounded parameter of the run (format v1).
// It carries no DSN, credential or host port.
type backlogSegMetaConfig struct {
	PublishBatch           int                      `json:"publish_batch"`
	PollBatch              int                      `json:"poll_batch"`
	SeedBatch              int                      `json:"seed_batch"`
	GapWaitMs              int                      `json:"gap_wait_ms"`
	ConsumerBackoffBaseMs  int                      `json:"consumer_backoff_base_ms"`
	ConsumerBackoffMaxMs   int                      `json:"consumer_backoff_max_ms"`
	RetryLimit             int                      `json:"retry_limit"`
	CommitIntervalMs       int                      `json:"commit_interval_ms"`
	PublisherLeaseTTLS     int                      `json:"publisher_lease_ttl_s"`
	PublisherBackoffBaseMs int                      `json:"publisher_backoff_base_ms"`
	PublisherBackoffMaxMs  int                      `json:"publisher_backoff_max_ms"`
	Sink                   backlogSegMetaConfigSink `json:"sink"`
	PoolMaxConns           int                      `json:"pool_max_conns"`
	PoolMinConns           int                      `json:"pool_min_conns"`
	Topic                  string                   `json:"topic"`
	TopicPartitions        int32                    `json:"topic_partitions"`
	KafkaImage             string                   `json:"kafka_image"`
	PGImage                string                   `json:"pg_image"`
}

// backlogSegMeta is meta.json (format v1).
type backlogSegMeta struct {
	Harness        string               `json:"harness"`
	Mode           string               `json:"mode"`
	StartedAtWall  string               `json:"started_at_wall"`
	FinishedAtWall string               `json:"finished_at_wall"`
	EpochWall      string               `json:"epoch_wall"`
	GoVersion      string               `json:"go_version"`
	GOMAXPROCS     int                  `json:"gomaxprocs"`
	HostCPUs       int                  `json:"host_cpus"`
	VCSRevision    string               `json:"vcs_revision"`
	VCSModified    string               `json:"vcs_modified"`
	N              int                  `json:"n"`
	Config         backlogSegMetaConfig `json:"config"`
	SegEnabled     bool                 `json:"seg_enabled"`
	Drops          backlogSegDrops      `json:"drops"`
}

// backlogSegPublishAnchors pins the publish phase on the monotonic base.
type backlogSegPublishAnchors struct {
	FirstCycleStartUs     int64 `json:"first_cycle_start_us"`
	LastCycleEndUs        int64 `json:"last_cycle_end_us"`
	PendingZeroObservedUs int64 `json:"pending_zero_observed_us"`
	PublishDoneUs         int64 `json:"publish_done_us"`
	Cycles                int   `json:"cycles"`
	Claimed               int   `json:"claimed"`
	Acked                 int   `json:"acked"`
	Released              int   `json:"released"`
	Blocked               int   `json:"blocked"`
	AppliedAtPublishDone  int   `json:"applied_at_publish_done"`
}

// backlogSegConsumeAnchors pins the consume phase on the monotonic base.
type backlogSegConsumeAnchors struct {
	PollConfirmUs     int64 `json:"poll_confirm_us"`
	ConsumerStoppedUs int64 `json:"consumer_stopped_us"`
	AppliedAtConfirm  int   `json:"applied_at_confirm"`
	LastCommitEndUs   int64 `json:"last_commit_end_us"`
	LastAppliedCBUs   int64 `json:"last_applied_cb_us"`
}

// backlogSegAnchors is anchors.json (format v1).
type backlogSegAnchors struct {
	DrainStartUs int64                    `json:"drain_start_us"`
	Publish      backlogSegPublishAnchors `json:"publish"`
	Consume      backlogSegConsumeAnchors `json:"consume"`
	Counts       backlogSegCounts         `json:"counts"`
	LagFinal     map[string]int           `json:"lag_final"`
	SegEnabled   bool                     `json:"seg_enabled"`
}

// backlogSegPoolFinal is the report's pool section after the drain.
type backlogSegPoolFinal struct {
	AcquireCount         int64   `json:"acquire_count"`
	EmptyAcquireCount    int64   `json:"empty_acquire_count"`
	AcquireDurationSecs  float64 `json:"acquire_duration_seconds"`
	CanceledAcquireCount int64   `json:"canceled_acquire_count"`
}

// backlogSegReport embeds the pristine measurement record (its field names
// stay unchanged) and appends the segmented evidence.
type backlogSegReport struct {
	backlogReport

	SegEnabled                 bool                `json:"seg_enabled"`
	AppliedAtPublishDone       int                 `json:"applied_at_publish_done"`
	PublishLastCycleEndSeconds float64             `json:"publish_last_cycle_end_seconds"`
	PendingZeroObservedSeconds float64             `json:"pending_zero_observed_seconds"`
	ConsumeConfirmSeconds      float64             `json:"consume_confirm_seconds"`
	LastCommitEndSeconds       float64             `json:"last_commit_end_seconds"`
	LastAppliedCBSeconds       float64             `json:"last_applied_cb_seconds"`
	ProcessObserved            int                 `json:"process_observed"`
	SQLSpanCount               int                 `json:"sql_span_count"`
	SegDrops                   backlogSegDrops     `json:"seg_drops"`
	LagFinal                   map[string]int      `json:"lag_final"`
	PoolFinal                  backlogSegPoolFinal `json:"pool_final"`
}

// backlogSegOpenPool opens the harness pool with the pristine connection
// bound (MaxConns=8, MinConns=1, MaxConnIdleTime=5m) and, only for the ON arm
// (rec != nil), attaches the statement tracer. An existing tracer is
// forwarded unchanged; with a nil recorder the pool is opened exactly like
// the pristine backlogOpenPool.
func backlogSegOpenPool(t *testing.T, dsn string, rec *backlogSegRecorder) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("pgxpool.ParseConfig: %v", err)
	}
	cfg.MaxConns = 8
	cfg.MinConns = 1
	cfg.MaxConnIdleTime = 5 * time.Minute
	if rec != nil {
		next, _ := cfg.ConnConfig.Tracer.(pgx.QueryTracer)
		cfg.ConnConfig.Tracer = &backlogSegTracer{rec: rec, next: next}
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("pgxpool.NewWithConfig: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// backlogSegMonitor samples the durable state during the drain on the
// pristine 200ms cadence and queries, plus the pool's cumulative counters on
// the same tick (both arms sample identically). It never gates the drain:
// sampling failures are recorded and asserted afterwards.
type backlogSegMonitor struct {
	rec     *backlogSegRecorder
	base    backlogSegBase
	started time.Time
	stop    chan struct{}
	done    chan struct{}
	once    sync.Once
}

func backlogSegStartMonitor(ctx context.Context, pool *pgxpool.Pool, pub *events.Publisher,
	observer *backlogConsumerObserver, started time.Time, rec *backlogSegRecorder) *backlogSegMonitor {
	m := &backlogSegMonitor{rec: rec, base: rec.base, started: started, stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(m.done)
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-m.stop:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				m.sample(ctx, pool, pub, observer)
			}
		}
	}()
	return m
}

func (m *backlogSegMonitor) sample(ctx context.Context, pool *pgxpool.Pool, pub *events.Publisher,
	observer *backlogConsumerObserver) {
	if ctx.Err() != nil {
		return
	}
	now := time.Now()
	var pending, published int64
	if err := pool.QueryRow(ctx, `
SELECT count(*) FILTER (WHERE publish_state = 'pending'),
       count(*) FILTER (WHERE publish_state = 'published')
FROM outbox_events`).Scan(&pending, &published); err != nil {
		m.rec.recordSampleErr("outbox sample: " + err.Error())
		return
	}
	var ledger int64
	if err := pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s`, backlogEffectTable)).Scan(&ledger); err != nil {
		m.rec.recordSampleErr("ledger sample: " + err.Error())
		return
	}
	var progress int64
	if err := pool.QueryRow(ctx,
		`SELECT coalesce(sum(next_offset), 0) FROM consumer_progress WHERE consumer_name = $1`,
		backlogConsumerName).Scan(&progress); err != nil {
		m.rec.recordSampleErr("progress sample: " + err.Error())
		return
	}
	if pub != nil && pending > 0 {
		// Observability during the drain: refresh the real outbox gauges.
		// A refresh failure is recorded, never treated as a drain gate.
		if err := pub.RefreshGauges(ctx); err != nil {
			m.rec.recordSampleErr("RefreshGauges: " + err.Error())
		}
	}
	stats := pool.Stat()
	m.rec.recordSample(backlogSegSampleRow{
		ElapsedSeconds:       now.Sub(m.started).Seconds(),
		TUs:                  m.base.us(now),
		Pending:              pending,
		Published:            published,
		Applied:              int64(observer.appliedCount()),
		Ledger:               ledger,
		Progress:             progress,
		AcquireCount:         stats.AcquireCount(),
		EmptyAcquireCount:    stats.EmptyAcquireCount(),
		AcquireDurationUs:    stats.AcquireDuration().Microseconds(),
		CanceledAcquireCount: stats.CanceledAcquireCount(),
		TotalConns:           int64(stats.TotalConns()),
		IdleConns:            int64(stats.IdleConns()),
	})
}

func (m *backlogSegMonitor) stopAndWait() {
	m.once.Do(func() { close(m.stop) })
	<-m.done
}

// backlogSegMonotonicInput projects the segmented samples onto the pristine
// monitor's sample shape so the pristine monotonicity assertion applies to
// this run too.
func backlogSegMonotonicInput(rows []backlogSegSampleRow) []backlogSample {
	out := make([]backlogSample, 0, len(rows))
	for _, row := range rows {
		out = append(out, backlogSample{
			ElapsedSeconds: row.ElapsedSeconds,
			Pending:        row.Pending,
			Published:      row.Published,
			Applied:        int(row.Applied),
			Ledger:         row.Ledger,
			Progress:       row.Progress,
		})
	}
	return out
}

// backlogSegSinceDrain converts one absolute epoch-relative offset to seconds
// from the drain start. An anchor that was never observed (0, the OFF arm's
// untraced commit/callback anchors) stays 0 instead of becoming a negative
// span before the drain.
func backlogSegSinceDrain(base backlogSegBase, drainStartUs, us int64) float64 {
	if us == 0 {
		return 0
	}
	return base.seconds(us) - base.seconds(drainStartUs)
}

// backlogSegVCS reads one vcs.* build setting ("revision", "modified").
func backlogSegVCS(key string) string {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			if setting.Key == "vcs."+key {
				return setting.Value
			}
		}
	}
	return ""
}

// backlogSegWriteJSON writes one evidence record as JSON with a trailing
// newline.
func backlogSegWriteJSON(t *testing.T, path string, value any) {
	t.Helper()
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal %s: %v", path, err)
	}
	if err := os.WriteFile(path, append(body, '\n'), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// backlogSegEmitReport prints the stable one-line JSON record (the pristine
// prefix, so existing consumers keep working) and, when
// TXHARBOR_BACKLOG_REPORT is set, writes it to that path.
func backlogSegEmitReport(t *testing.T, report backlogSegReport) {
	t.Helper()
	body, err := json.Marshal(report)
	if err != nil {
		t.Fatalf("marshal backlog report: %v", err)
	}
	t.Logf("TXHARBOR-BACKLOG-DRAIN %s", body)
	path := strings.TrimSpace(os.Getenv(backlogReportEnv))
	if path == "" {
		return
	}
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("create report directory %s: %v", dir, err)
		}
	}
	if err := os.WriteFile(path, append(body, '\n'), 0o644); err != nil {
		t.Fatalf("write backlog report %s: %v", path, err)
	}
	t.Logf("backlog report written to %s", path)
}

// backlogSegWriteArchive writes meta.json, anchors.json, samples.csv and — for
// the ON arm — the span archives loop.csv, sql.csv and publish.csv.
func backlogSegWriteArchive(t *testing.T, dir string, meta backlogSegMeta, anchors backlogSegAnchors,
	rec *backlogSegRecorder, collect bool) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("create segmented evidence directory %s: %v", dir, err)
	}
	backlogSegWriteJSON(t, filepath.Join(dir, "meta.json"), meta)
	backlogSegWriteJSON(t, filepath.Join(dir, "anchors.json"), anchors)
	if err := rec.writeSamplesCSV(filepath.Join(dir, "samples.csv")); err != nil {
		t.Fatalf("write samples.csv: %v", err)
	}
	if !collect {
		return
	}
	if err := rec.writeLoopCSV(filepath.Join(dir, "loop.csv")); err != nil {
		t.Fatalf("write loop.csv: %v", err)
	}
	if err := rec.writeSQLCSV(filepath.Join(dir, "sql.csv")); err != nil {
		t.Fatalf("write sql.csv: %v", err)
	}
	if err := rec.writePublishCSV(filepath.Join(dir, "publish.csv")); err != nil {
		t.Fatalf("write publish.csv: %v", err)
	}
}

// TestBacklogDrainSeg is the instrumented larger-backlog drain: the pristine
// drill's load, middleware, drain and terminal assertions, plus the consumer
// segmentation evidence of one ON or OFF arm.
func TestBacklogDrainSeg(t *testing.T) {
	segEnabled, segDir, err := backlogSegParams(os.Getenv(backlogSegEnv), os.Getenv(backlogSegDirEnv))
	if err != nil {
		t.Fatalf("invalid segmented-measurement parameter: %v", err)
	}
	load, inVerificationRange, err := backlogLoadParams(os.Getenv(backlogNEnv))
	if err != nil {
		t.Fatalf("invalid load parameter: %v", err)
	}
	testcontainers.SkipIfProviderIsNotHealthy(t)

	// The single monotonic timing base of this run: every t_us/dur_us in the
	// archives and anchors is a microsecond offset from it.
	epoch := time.Now()
	epochWall := epoch.UTC()
	startedAtWall := epochWall

	rec := newBacklogSegRecorder(epoch, segEnabled)

	ctx, cancel := context.WithTimeout(context.Background(), backlogTestTimeout)
	defer cancel()

	kafka, err := testutil.StartKafka(ctx)
	if err != nil {
		t.Fatalf("StartKafka: %v", err)
	}
	t.Cleanup(func() { _ = kafka.Close(context.Background()) })
	if err := kafka.EnsureTopic(ctx, testutil.KafkaTopic, testutil.KafkaPartitions); err != nil {
		t.Fatalf("EnsureTopic: %v", err)
	}
	pool := backlogSegOpenPool(t, backlogStartPostgres(t), rec.traceTarget())
	backlogCreateEffectTable(t, ctx, pool)

	// --- seed the backlog through the real Append path -----------------------
	memBeforeSeed := backlogMemSnapshot()
	dbSizeBefore, outboxSizeBefore := backlogPGSizes(t, ctx, pool)
	seedStart := time.Now()
	backlogSeed(t, ctx, pool, load)
	seedSeconds := time.Since(seedStart).Seconds()

	rows := backlogWantCount(t, ctx, pool, "seeded outbox rows", `SELECT count(*) FROM outbox_events`, int64(load))
	backlogWantCount(t, ctx, pool, "seeded pending rows",
		`SELECT count(*) FROM outbox_events WHERE publish_state = 'pending'`, int64(load))
	backlogWantCount(t, ctx, pool, "seeded published rows",
		`SELECT count(*) FROM outbox_events WHERE publish_state = 'published'`, 0)
	backlogWantCount(t, ctx, pool, "seeded blocked rows",
		`SELECT count(*) FROM outbox_events WHERE publish_state = 'blocked'`, 0)
	backlogWantCount(t, ctx, pool, "seeded distinct aggregates",
		`SELECT count(DISTINCT aggregate_id) FROM outbox_events`, int64(load))
	initialOldest := backlogScalarFloat(t, ctx, pool,
		`SELECT coalesce(extract(epoch FROM now() - min(created_at)), 0) FROM outbox_events WHERE publish_state = 'pending'`)
	memAfterSeed := backlogMemSnapshot()
	t.Logf("seeded n=%d rows=%d in %.2fs (%.0f events/s), initial oldest age %.2fs",
		load, rows, seedSeconds, float64(load)/seedSeconds, initialOldest)

	// --- real publisher + real consumer --------------------------------------
	sink, err := events.NewKafkaSink(events.KafkaSinkConfig{
		Brokers:         kafka.Brokers(),
		Topic:           testutil.KafkaTopic,
		DeliveryTimeout: 10 * time.Second,
		RequestTimeout:  3 * time.Second,
		MaxInflight:     5,
		MaxBuffered:     2048,
	})
	if err != nil {
		t.Fatalf("NewKafkaSink: %v", err)
	}
	t.Cleanup(sink.Close)

	pubObserver := newBacklogPublisherObserver()
	pub, err := events.NewPublisher(pool, sink, "owner-013-backlog-drain", events.PublisherOptions{
		Batch:       backlogPublishBatch,
		LeaseTTL:    60 * time.Second,
		BackoffBase: time.Second,
		BackoffMax:  30 * time.Second,
		Jitter:      func() float64 { return 0 },
		Observer:    pubObserver,
	})
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}

	effect := &backlogEffect{}
	consumerObserver := newBacklogConsumerObserver()
	consumer, err := events.NewConsumer(pool, backlogConsumerName, effect, events.ConsumerOptions{
		GapWait:     time.Second,
		BackoffBase: 50 * time.Millisecond,
		BackoffMax:  time.Second,
		RetryLimit:  5,
		ChainID:     31337,
		Jitter:      func() float64 { return 0 },
		Observer:    &backlogSegObserver{next: consumerObserver, rec: rec},
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	runtimeConsumer, err := events.NewKafkaConsumer(events.KafkaConsumerConfig{
		Brokers:        kafka.Brokers(),
		Topic:          testutil.KafkaTopic,
		GroupPrefix:    "txharbor",
		ConsumerName:   backlogConsumerName,
		PollBatch:      backlogPollBatch,
		CommitInterval: 200 * time.Millisecond,
	}, consumer)
	if err != nil {
		t.Fatalf("NewKafkaConsumer: %v", err)
	}
	// Both arms arm the recorder so the matched control carries the same seam
	// calls, pairing checks and tallies; the arm switch is the recorder's own
	// collect flag: ON stores the span/SQL rows and attaches the statement
	// tracer, OFF stores none of them (see newBacklogSegRecorder).
	events.PerfEnableConsumerSeg(rec)
	t.Cleanup(events.PerfDisableConsumerSeg)

	runCtx, stopConsumer := context.WithCancel(ctx)
	if segEnabled {
		// Only the consumer's own queries are attributed: the mark lives on
		// its run context, never on the harness's or the monitor's context.
		runCtx = backlogSegMarked(runCtx)
	}
	consumerDone := make(chan error, 1)
	go func() { consumerDone <- runtimeConsumer.Run(runCtx) }()

	// --- bounded publish drain, consumer catching up in parallel -------------
	drainStart := time.Now()
	monitor := backlogSegStartMonitor(ctx, pool, pub, consumerObserver, drainStart, rec)

	cycles := 0
	var claimed, acked, released, blocked int
	var firstCycleStartUs, lastCycleEndUs, pendingZeroObservedUs int64
	publishDeadline := drainStart.Add(backlogPublishWindow)
	for {
		countStart := time.Now()
		pending := backlogCount(t, ctx, pool,
			`SELECT count(*) FROM outbox_events WHERE publish_state = 'pending'`)
		countEnd := time.Now()
		if pending == 0 {
			// Boundary row: the pending count that ended the publisher loop,
			// with no publish outcome attached.
			rec.recordPublish(backlogSegPublishRow{
				Iter:       cycles + 1,
				TCountUs:   rec.base.us(countStart),
				CountDurUs: rec.base.dur(countEnd.Sub(countStart)),
			})
			pendingZeroObservedUs = rec.base.us(countEnd)
			break
		}
		if time.Now().After(publishDeadline) {
			t.Fatalf("publish drain did not reach 0 pending within %s (pending = %d)",
				backlogPublishWindow, pending)
		}
		cycleStart := time.Now()
		outcome, err := pub.PublishOnce(ctx)
		cycleEnd := time.Now()
		if err != nil {
			t.Fatalf("PublishOnce: %v", err)
		}
		if outcome.Claimed > backlogPublishBatch {
			t.Fatalf("cycle claimed %d records, want <= the bounded batch %d", outcome.Claimed, backlogPublishBatch)
		}
		if outcome.Acked+outcome.Released+outcome.Blocked != outcome.Claimed {
			t.Fatalf("cycle accounting %+v: acked+released+blocked != claimed", outcome)
		}
		cycles++
		claimed += outcome.Claimed
		acked += outcome.Acked
		released += outcome.Released
		blocked += outcome.Blocked
		if cycles == 1 {
			firstCycleStartUs = rec.base.us(cycleStart)
		}
		lastCycleEndUs = rec.base.us(cycleEnd)
		rec.recordPublish(backlogSegPublishRow{
			Iter:        cycles,
			TCountUs:    rec.base.us(countStart),
			CountDurUs:  rec.base.dur(countEnd.Sub(countStart)),
			PendingSeen: pending,
			TPublishUs:  rec.base.us(cycleStart),
			PublishDur:  rec.base.dur(cycleEnd.Sub(cycleStart)),
			Claimed:     outcome.Claimed,
			Acked:       outcome.Acked,
			Released:    outcome.Released,
			Blocked:     outcome.Blocked,
		})
		if outcome.Claimed == 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}
	publishDone := time.Now()
	appliedAtPublishDone := consumerObserver.appliedCount()

	consumeDeadline := publishDone.Add(backlogConsumeWindow)
	for consumerObserver.appliedCount() < load {
		if time.Now().After(consumeDeadline) {
			t.Fatalf("consumer applied %d/%d events within %s after the publish drain",
				consumerObserver.appliedCount(), load, backlogConsumeWindow)
		}
		time.Sleep(100 * time.Millisecond)
	}
	consumerDoneAt := time.Now()
	if consumerDoneAt.Before(publishDone) {
		t.Fatalf("consumer finished before the publisher (consumer=%s publish=%s)", consumerDoneAt, publishDone)
	}
	appliedAtConfirm := consumerObserver.appliedCount()

	// The final lag is read while the consumer client is still open (Run
	// closes it on return); the phase anchors are captured around the stop.
	lagFinal := map[string]int{}
	lags, err := runtimeConsumer.Lag(ctx)
	if err != nil {
		t.Fatalf("final lag observation: %v", err)
	}
	for partition, lag := range lags {
		lagFinal[strconv.Itoa(int(partition))] = int(lag)
	}

	monitor.stopAndWait()
	stopConsumer()
	select {
	case err := <-consumerDone:
		if err != nil {
			t.Fatalf("KafkaConsumer.Run: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("KafkaConsumer.Run did not stop within 30s")
	}
	consumerStoppedAt := time.Now()

	// --- final state: progress and effect, not just the pending count --------
	sampleRows := rec.sampleRows()
	backlogAssertMonotonic(t, backlogSegMonotonicInput(sampleRows), rec.sampleErrs())
	drops := rec.snapshotDrops()
	counts := rec.snapshotCounts()
	sqlEventSeqs := rec.sqlEventSeqs()

	finalRows := backlogWantCount(t, ctx, pool, "outbox rows after drain", `SELECT count(*) FROM outbox_events`, int64(load))
	backlogWantCount(t, ctx, pool, "pending after drain",
		`SELECT count(*) FROM outbox_events WHERE publish_state = 'pending'`, 0)
	backlogWantCount(t, ctx, pool, "published after drain",
		`SELECT count(*) FROM outbox_events WHERE publish_state = 'published'`, int64(load))
	backlogWantCount(t, ctx, pool, "blocked after drain",
		`SELECT count(*) FROM outbox_events WHERE publish_state = 'blocked'`, 0)
	backlogWantCount(t, ctx, pool, "unpublished rows after drain",
		`SELECT count(*) FROM outbox_events WHERE published_at IS NULL`, 0)
	attemptsSum := backlogCount(t, ctx, pool, `SELECT coalesce(sum(attempt_count), 0) FROM outbox_events`)
	oldestWait := backlogScalarFloat(t, ctx, pool,
		`SELECT coalesce(extract(epoch FROM max(published_at - created_at)), 0) FROM outbox_events WHERE publish_state = 'published'`)

	effectRows := backlogWantCount(t, ctx, pool, "effect rows", fmt.Sprintf(`SELECT count(*) FROM %s`, backlogEffectTable), int64(load))
	effectDistinct := backlogWantCount(t, ctx, pool, "distinct effect events",
		fmt.Sprintf(`SELECT count(DISTINCT event_id) FROM %s`, backlogEffectTable), int64(load))
	backlogWantCount(t, ctx, pool, "events with duplicate effects",
		fmt.Sprintf(`SELECT count(*) FROM (SELECT event_id FROM %s GROUP BY event_id HAVING count(*) > 1) dup`, backlogEffectTable), 0)
	var effectMax int64
	if err := pool.QueryRow(ctx, fmt.Sprintf(
		`SELECT coalesce(max(c), 0) FROM (SELECT count(*) AS c FROM %s GROUP BY event_id) s`, backlogEffectTable)).Scan(&effectMax); err != nil {
		t.Fatalf("max effect rows per event: %v", err)
	}
	backlogWantCount(t, ctx, pool, "inbox rows",
		`SELECT count(*) FROM consumer_inbox WHERE consumer_name = $1`, int64(load), backlogConsumerName)
	quarantineRows := backlogWantCount(t, ctx, pool, "quarantine rows",
		`SELECT count(*) FROM consumer_quarantine WHERE consumer_name = $1`, 0, backlogConsumerName)
	if got := consumerObserver.appliedCount(); got != load {
		t.Fatalf("consumer applied observations = %d, want %d", got, load)
	}
	if quarantines := consumerObserver.quarantineSnapshot(); len(quarantines) != 0 {
		t.Fatalf("consumer quarantine observations = %v, want none", quarantines)
	}
	if releaseCount := released; releaseCount > 0 {
		t.Logf("note: %d publish releases (transient retries) occurred during the drain", releaseCount)
	}
	// Transient failures are the designed retry path and are recorded; the
	// permanent/contract classes are fail-closed outcomes and must not occur.
	for class, n := range pubObserver.failures {
		if n > 0 && class != string(events.ClassTransient) {
			t.Fatalf("non-transient publish failure class %q x%d: %v", class, n, pubObserver.failures)
		}
	}
	if acked != load {
		t.Fatalf("publisher acked %d rows, want %d (0 loss)", acked, load)
	}
	if pubObserver.published != load {
		t.Fatalf("publisher observed %d published rows, want %d", pubObserver.published, load)
	}
	if pubObserver.attempts != claimed {
		t.Fatalf("publisher observed %d attempts, want the claimed total %d", pubObserver.attempts, claimed)
	}
	versionRows := backlogWantCount(t, ctx, pool, "version rows",
		`SELECT count(*) FROM consumer_versions WHERE consumer_name = $1`, int64(load), backlogConsumerName)
	var versionSum, versionMax int64
	if err := pool.QueryRow(ctx, `
SELECT coalesce(sum(max_version), 0), coalesce(max(max_version), 0)
FROM consumer_versions WHERE consumer_name = $1`, backlogConsumerName).Scan(&versionSum, &versionMax); err != nil {
		t.Fatalf("version aggregate: %v", err)
	}
	if versionSum != int64(load) || versionMax != 1 {
		t.Fatalf("version guard state = (sum %d, max %d), want (%d, 1)", versionSum, versionMax, load)
	}
	progressRows := backlogCount(t, ctx, pool,
		`SELECT count(*) FROM consumer_progress WHERE consumer_name = $1`, backlogConsumerName)
	if progressRows < 1 || progressRows > int64(testutil.KafkaPartitions) {
		t.Fatalf("progress partitions = %d, want within [1, %d]", progressRows, testutil.KafkaPartitions)
	}
	progressSum := backlogCount(t, ctx, pool,
		`SELECT coalesce(sum(next_offset), 0) FROM consumer_progress WHERE consumer_name = $1`, backlogConsumerName)
	brokerRecords := backlogBrokerRecords(t, ctx, kafka.Brokers(), testutil.KafkaTopic)
	if brokerRecords < int64(load) {
		t.Fatalf("broker records = %d, want >= %d (0 loss)", brokerRecords, load)
	}
	if progressSum < int64(load) || progressSum > brokerRecords {
		t.Fatalf("durable progress sum = %d, want within [%d, broker records %d]", progressSum, load, brokerRecords)
	}
	if len(sampleRows) > 0 {
		if last := sampleRows[len(sampleRows)-1]; last.Progress > progressSum {
			t.Fatalf("monitor saw progress %d above the final %d (progress must be monotonic)", last.Progress, progressSum)
		}
	}

	// --- recorder cross-checks ----------------------------------------------
	// The recorder observes the same consumer the durable assertions above
	// describe: one applied callback per applied event and one paired span per
	// step. A redelivered record produces a process span (and its own
	// statements) without an applied callback, so the three counts agree only
	// while the stream delivers each record once — a difference under
	// redelivery is recorded instead of asserted.
	if counts.ObservedApplied != load {
		t.Fatalf("recorder observed %d applied outcomes, want %d", counts.ObservedApplied, load)
	}
	if drops.LoopUnpaired != 0 {
		t.Fatalf("consumer loop spans did not pair: %d unpaired (a harness bug, never a record)", drops.LoopUnpaired)
	}
	if segEnabled {
		// Every processed record issues statements under its own sequence:
		// the SQL side and the span side must agree.
		if counts.ObservedProcess != sqlEventSeqs {
			t.Fatalf("process spans = %d, sql event sequences = %d, want equality", counts.ObservedProcess, sqlEventSeqs)
		}
		if counts.SQLSpans == 0 {
			t.Fatalf("no SQL span was traced although the ON arm attached the tracer")
		}
	}
	if counts.ObservedProcess != counts.AppliedCB || counts.ObservedProcess != load {
		if counts.ObservedDuplicate > 0 || counts.ObservedProcess > load {
			t.Logf("note: %d process spans and %d applied callbacks for %d events (%d duplicate outcomes) — the stream redelivered records",
				counts.ObservedProcess, counts.AppliedCB, load, counts.ObservedDuplicate)
		} else {
			t.Fatalf("process spans = %d, applied callbacks = %d, want %d each",
				counts.ObservedProcess, counts.AppliedCB, load)
		}
	}

	memAfterDrain := backlogMemSnapshot()
	dbSizeAfter, outboxSizeAfter := backlogPGSizes(t, ctx, pool)
	poolStats := pool.Stat()

	// --- measurement record --------------------------------------------------
	publishSeconds := publishDone.Sub(drainStart).Seconds()
	totalSeconds := consumerDoneAt.Sub(drainStart).Seconds()
	tailSeconds := consumerDoneAt.Sub(publishDone).Seconds()
	curve := make([]int64, 0, len(sampleRows))
	for _, s := range sampleRows {
		curve = append(curve, s.Pending)
	}
	drainStartUs := rec.base.us(drainStart)
	lastCommitEndUs := int64(0)
	lastAppliedCBUs := int64(0)
	for _, row := range rec.sqlRows() {
		if row.Kind == backlogSegSQLCommit && row.TUs+row.DurUs > lastCommitEndUs {
			lastCommitEndUs = row.TUs + row.DurUs
		}
	}
	for _, row := range rec.loopRows() {
		if row.Kind == backlogSegKindAppliedCB && row.TUs > lastAppliedCBUs {
			lastAppliedCBUs = row.TUs
		}
	}

	report := backlogSegReport{
		backlogReport: backlogReport{
			Harness:             "013-verify-supplement/backlog-drain",
			StartedAt:           drainStart.UTC().Format(time.RFC3339),
			VCSRevision:         backlogVCSRevision(),
			GoVersion:           runtime.Version(),
			GOOS:                runtime.GOOS,
			GOARCH:              runtime.GOARCH,
			GOMAXPROCS:          runtime.GOMAXPROCS(0),
			HostCPUs:            runtime.NumCPU(),
			N:                   load,
			InVerificationRange: inVerificationRange,
			SeedBatch:           backlogSeedBatch,
			PublishBatch:        backlogPublishBatch,
			PollBatch:           backlogPollBatch,
			TopicPartitions:     testutil.KafkaPartitions,
			KafkaImage:          testutil.KafkaImage,
			PostgresImage:       backlogPostgresImage,
			PublisherConfig: map[string]any{
				"batch": backlogPublishBatch, "lease_ttl_s": 60, "backoff_base_ms": 1000, "backoff_max_ms": 30000,
			},
			ConsumerConfig: map[string]any{
				"name": backlogConsumerName, "gap_wait_ms": 1000, "backoff_base_ms": 50, "backoff_max_ms": 1000,
				"retry_limit": 5, "poll_batch": backlogPollBatch, "commit_interval_ms": 200,
			},
			SinkConfig: map[string]any{
				"delivery_timeout_ms": 10000, "request_timeout_ms": 3000, "max_inflight": 5, "max_buffered": 2048,
			},
			SeedSeconds:                 seedSeconds,
			SeedEventsPerSec:            float64(load) / seedSeconds,
			PublishSeconds:              publishSeconds,
			PublishEventsPerSec:         float64(load) / publishSeconds,
			TotalDrainSeconds:           totalSeconds,
			EndToEndEventsPerSec:        float64(load) / totalSeconds,
			ConsumerTailSeconds:         tailSeconds,
			PublishCycles:               cycles,
			PublishClaimed:              claimed,
			PublishAcked:                acked,
			PublishReleased:             released,
			PublishBlocked:              blocked,
			PublishFailures:             pubObserver.failures,
			ConsumerApplied:             consumerObserver.appliedCount(),
			ConsumerRetries:             consumerObserver.retryCount(),
			ConsumerQuarantines:         consumerObserver.quarantineSnapshot(),
			EffectCalls:                 effect.calls.Load(),
			InitialOldestAgeSeconds:     initialOldest,
			OldestWaitSeconds:           oldestWait,
			MaxObservedOldestAgeSeconds: pubObserver.maxObservedOldest(),
			ObservabilityTicks:          pubObserver.ticks,
			OutboxRows:                  finalRows,
			OutboxPending:               0,
			OutboxPublished:             int64(load),
			OutboxBlocked:               int64(blocked),
			OutboxAttemptsSum:           attemptsSum,
			OutboxPublishedAtNull:       0,
			EffectRows:                  effectRows,
			EffectDistinctEvents:        effectDistinct,
			EffectMaxRowsPerEvent:       effectMax,
			InboxRows:                   int64(load),
			QuarantineRows:              quarantineRows,
			VersionRows:                 versionRows,
			VersionSum:                  versionSum,
			VersionMax:                  versionMax,
			ProgressRows:                progressRows,
			ProgressSumNextOffset:       progressSum,
			BrokerRecords:               brokerRecords,
			Memory: backlogMemBasis{
				BeforeSeed: memBeforeSeed,
				AfterSeed:  memAfterSeed,
				AfterDrain: memAfterDrain,
			},
			PGSizes: map[string]int64{
				"db_before_seed":     dbSizeBefore,
				"db_after_drain":     dbSizeAfter,
				"outbox_before_seed": outboxSizeBefore,
				"outbox_after_drain": outboxSizeAfter,
			},
			PoolStats: map[string]any{
				"max_conns":              poolStats.MaxConns(),
				"total_conns":            poolStats.TotalConns(),
				"idle_conns":             poolStats.IdleConns(),
				"acquire_count":          poolStats.AcquireCount(),
				"empty_acquire_count":    poolStats.EmptyAcquireCount(),
				"canceled_acquire_count": poolStats.CanceledAcquireCount(),
			},
			PendingCurve:   backlogDownsample(curve, 200),
			CurveDropped:   drops.SamplesDropped,
			MonitorSamples: len(sampleRows),
		},
		SegEnabled:                 segEnabled,
		AppliedAtPublishDone:       appliedAtPublishDone,
		PublishLastCycleEndSeconds: backlogSegSinceDrain(rec.base, drainStartUs, lastCycleEndUs),
		PendingZeroObservedSeconds: backlogSegSinceDrain(rec.base, drainStartUs, pendingZeroObservedUs),
		ConsumeConfirmSeconds:      backlogSegSinceDrain(rec.base, drainStartUs, rec.base.us(consumerDoneAt)),
		LastCommitEndSeconds:       backlogSegSinceDrain(rec.base, drainStartUs, lastCommitEndUs),
		LastAppliedCBSeconds:       backlogSegSinceDrain(rec.base, drainStartUs, lastAppliedCBUs),
		ProcessObserved:            counts.ObservedProcess,
		SQLSpanCount:               counts.SQLSpans,
		SegDrops:                   drops,
		LagFinal:                   lagFinal,
		PoolFinal: backlogSegPoolFinal{
			AcquireCount:         poolStats.AcquireCount(),
			EmptyAcquireCount:    poolStats.EmptyAcquireCount(),
			AcquireDurationSecs:  poolStats.AcquireDuration().Seconds(),
			CanceledAcquireCount: poolStats.CanceledAcquireCount(),
		},
	}

	anchors := backlogSegAnchors{
		DrainStartUs: drainStartUs,
		Publish: backlogSegPublishAnchors{
			FirstCycleStartUs:     firstCycleStartUs,
			LastCycleEndUs:        lastCycleEndUs,
			PendingZeroObservedUs: pendingZeroObservedUs,
			PublishDoneUs:         rec.base.us(publishDone),
			Cycles:                cycles,
			Claimed:               claimed,
			Acked:                 acked,
			Released:              released,
			Blocked:               blocked,
			AppliedAtPublishDone:  appliedAtPublishDone,
		},
		Consume: backlogSegConsumeAnchors{
			PollConfirmUs:     rec.base.us(consumerDoneAt),
			ConsumerStoppedUs: rec.base.us(consumerStoppedAt),
			AppliedAtConfirm:  appliedAtConfirm,
			LastCommitEndUs:   lastCommitEndUs,
			LastAppliedCBUs:   lastAppliedCBUs,
		},
		Counts:     counts,
		LagFinal:   lagFinal,
		SegEnabled: segEnabled,
	}

	mode := backlogSegModeOff
	if segEnabled {
		mode = backlogSegModeOn
	}
	meta := backlogSegMeta{
		Harness:        backlogSegHarness,
		Mode:           mode,
		StartedAtWall:  startedAtWall.Format(time.RFC3339Nano),
		FinishedAtWall: time.Now().UTC().Format(time.RFC3339Nano),
		EpochWall:      epochWall.Format(time.RFC3339Nano),
		GoVersion:      runtime.Version(),
		GOMAXPROCS:     runtime.GOMAXPROCS(0),
		HostCPUs:       runtime.NumCPU(),
		VCSRevision:    backlogSegVCS("revision"),
		VCSModified:    backlogSegVCS("modified"),
		N:              load,
		Config: backlogSegMetaConfig{
			PublishBatch:           backlogPublishBatch,
			PollBatch:              backlogPollBatch,
			SeedBatch:              backlogSeedBatch,
			GapWaitMs:              1000,
			ConsumerBackoffBaseMs:  50,
			ConsumerBackoffMaxMs:   1000,
			RetryLimit:             5,
			CommitIntervalMs:       200,
			PublisherLeaseTTLS:     60,
			PublisherBackoffBaseMs: 1000,
			PublisherBackoffMaxMs:  30000,
			Sink: backlogSegMetaConfigSink{
				DeliveryTimeoutMs: 10000,
				RequestTimeoutMs:  3000,
				MaxInflight:       5,
				MaxBuffered:       2048,
			},
			PoolMaxConns:    8,
			PoolMinConns:    1,
			Topic:           testutil.KafkaTopic,
			TopicPartitions: testutil.KafkaPartitions,
			KafkaImage:      testutil.KafkaImage,
			PGImage:         backlogPostgresImage,
		},
		SegEnabled: segEnabled,
		Drops:      drops,
	}

	if segDir != "" {
		backlogSegWriteArchive(t, segDir, meta, anchors, rec, segEnabled)
	}
	backlogSegEmitReport(t, report)

	t.Logf("seg arm: mode=%s enabled=%t drops=%+v counts=%+v sql_event_seqs=%d",
		mode, segEnabled, drops, counts, sqlEventSeqs)
	t.Logf("phase anchors (s from drain start): last publish cycle %.2f, pending=0 %.2f, consume confirm %.2f, last commit %.2f, last applied cb %.2f",
		report.PublishLastCycleEndSeconds, report.PendingZeroObservedSeconds, report.ConsumeConfirmSeconds,
		report.LastCommitEndSeconds, report.LastAppliedCBSeconds)
	t.Logf("publish drain: %.2fs (%.0f events/s), cycles=%d claimed=%d acked=%d released=%d blocked=%d",
		publishSeconds, report.PublishEventsPerSec, cycles, claimed, acked, released, blocked)
	t.Logf("end-to-end drain: %.2fs (%.0f events/s), consumer catch-up tail %.2fs",
		totalSeconds, report.EndToEndEventsPerSec, tailSeconds)
	t.Logf("oldest wait: initial %.2fs, published_at-created_at max %.2fs, max observed gauge %.2fs",
		initialOldest, oldestWait, report.MaxObservedOldestAgeSeconds)
	t.Logf("errors: publish failures=%v, releases=%d, consumer retries=%d, quarantines=%v",
		pubObserver.failures, released, consumerObserver.retryCount(), consumerObserver.quarantineSnapshot())
	t.Logf("memory (heap alloc before/after seed/after drain): %d -> %d -> %d bytes",
		memBeforeSeed.HeapAllocBytes, memAfterSeed.HeapAllocBytes, memAfterDrain.HeapAllocBytes)
	t.Logf("effect table: rows=%d distinct=%d max-per-event=%d; inbox=%d versions=%d progress=%d/%d",
		effectRows, effectDistinct, effectMax, int64(load), versionRows, progressSum, brokerRecords)
	t.Logf("pool: acquire=%d empty=%d canceled=%d acquire_seconds=%.3f total_conns=%d idle=%d",
		poolStats.AcquireCount(), poolStats.EmptyAcquireCount(), poolStats.CanceledAcquireCount(),
		poolStats.AcquireDuration().Seconds(), poolStats.TotalConns(), poolStats.IdleConns())
	t.Logf("final lag (messages per partition): %v", lagFinal)
}
