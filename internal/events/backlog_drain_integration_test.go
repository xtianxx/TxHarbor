//go:build integration_backlog

// backlog_drain_integration_test.go is the 013 verification supplement's
// larger-backlog drain drill (V-CATCHUP/SC-09 supplement; FR-13/FR-21).
//
// What it exercises with real middleware, not with hand-called state helpers:
//
//   - a fixed, parameterized backlog of 5000-10000 outbox events is seeded
//     through the real events.Append path in bounded batches (never one huge
//     transaction, never a direct INSERT that would bypass identity/version
//     derivation and payload canonicalization); batch commits and bounded
//     drain cycles keep the harness memory-bounded for the largest load;
//   - the backlog is drained through the real Publisher.PublishOnce cycle
//     (bounded claim batches, lease/attempt accounting, at-least-once
//     publication to a real broker with explicit topic creation);
//   - the delivered stream is consumed through the real T4 consumer runtime
//     (KafkaConsumer.Run over the persistent inbox dedup, version guard,
//     effect, quarantine and durable progress);
//   - the final state is verified as a whole: pending/published/blocked row
//     counts, effect rows per event (effective application = 1), inbox rows,
//     quarantine rows, version rows and monotonic progress samples;
//   - drain wall time, oldest wait, throughput, error counts and the
//     before/after memory + resource basis are measured and emitted as one
//     stable JSON line (`TXHARBOR-BACKLOG-DRAIN {...}`); setting
//     TXHARBOR_BACKLOG_REPORT=<path> also writes that JSON to <path>.
//
// Layer and CI discipline: this file carries the dedicated
// `integration_backlog` build tag. It is an opt-in measurement harness; it is
// NOT wired into any Makefile target and MUST NOT run in ordinary PR CI
// (a 5k-10k event drill is a verification-supplement run, not a PR gate).
// Run it explicitly, for example:
//
//	go test -tags integration_backlog -count=1 -timeout 40m \
//	  -run 'TestBacklogDrain' -v ./internal/events/
//
// Environment knobs (both optional):
//
//	TXHARBOR_BACKLOG_DRAIN_N   event count; default 5000 (the lower bound of
//	                           the 5k-10k verification range). Values below
//	                           5000 are smoke values: the run stays valid but
//	                           the report marks it out of the verification
//	                           range. The harness refuses > 50000.
//	TXHARBOR_BACKLOG_REPORT    path of the JSON report to write (raw evidence
//	                           belongs outside the repository, e.g. under
//	                           /tmp/opencode/013-supplement/).
//
// Statement discipline: delivery is at-least-once and processing is
// idempotent; PostgreSQL and Kafka share no transaction, so this layer never
// claims a cross-system exactly-once guarantee. "Effective application = 1"
// is measured inside the consumer's PostgreSQL effects (the persistent inbox
// absorbs every redelivery), which is exactly the boundary this project's
// evidence claims.
package events_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/events"
	"github.com/xtianxx/txharbor/internal/testutil"
)

// Harness constants. Every value is either a fixed load parameter or a
// test-sized bound; none of them is a production threshold.
const (
	backlogPostgresImage = "postgres:18.6-trixie"
	backlogEffectTable   = "t013_backlog_effects"
	backlogConsumerName  = "txharbor.backlog-drain.v1"

	backlogDefaultN         = 5000
	backlogVerificationMinN = 5000
	backlogVerificationMaxN = 10000
	backlogHarnessMaxN      = 50000

	backlogSeedBatch    = 250
	backlogPublishBatch = 250
	backlogPollBatch    = 500

	backlogMaxSamples = 4000
)

// Test-sized timing bounds: wall-clock limits that turn a stuck drain into a
// loud failure instead of an unbounded hang. They are not capacity or latency
// thresholds.
const (
	backlogTestTimeout   = 30 * time.Minute
	backlogPublishWindow = 12 * time.Minute
	backlogConsumeWindow = 15 * time.Minute
)

// Environment knobs.
const (
	backlogNEnv      = "TXHARBOR_BACKLOG_DRAIN_N"
	backlogReportEnv = "TXHARBOR_BACKLOG_REPORT"
)

// backlogLoadParams parses the load knob. An empty value selects the default
// (5000 = the lower bound of the verification range). Values outside the
// 5000-10000 range are accepted as smoke runs and reported as out of range;
// invalid values and values beyond the harness cap are refused fail-closed.
func backlogLoadParams(raw string) (load int, inVerificationRange bool, err error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return backlogDefaultN, true, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, false, fmt.Errorf("%s=%q is not an integer: %w", backlogNEnv, raw, err)
	}
	if n <= 0 {
		return 0, false, fmt.Errorf("%s=%d must be positive", backlogNEnv, n)
	}
	if n > backlogHarnessMaxN {
		return 0, false, fmt.Errorf("%s=%d exceeds the harness cap %d", backlogNEnv, n, backlogHarnessMaxN)
	}
	return n, n >= backlogVerificationMinN && n <= backlogVerificationMaxN, nil
}

// TestBacklogDrainLoadProfile pins the load-parameter boundaries without any
// Docker dependency: it is the seconds-long smoke of this harness.
func TestBacklogDrainLoadProfile(t *testing.T) {
	cases := []struct {
		raw       string
		want      int
		inRange   bool
		wantError bool
	}{
		{raw: "", want: 5000, inRange: true},
		{raw: "5000", want: 5000, inRange: true},
		{raw: "10000", want: 10000, inRange: true},
		{raw: " 7500 ", want: 7500, inRange: true},
		{raw: "50", want: 50, inRange: false},
		{raw: "0", wantError: true},
		{raw: "-5", wantError: true},
		{raw: "abc", wantError: true},
		{raw: "50001", wantError: true},
	}
	for _, tc := range cases {
		got, inRange, err := backlogLoadParams(tc.raw)
		if tc.wantError {
			if err == nil {
				t.Errorf("backlogLoadParams(%q) = %d, want an error", tc.raw, got)
			}
			continue
		}
		if err != nil {
			t.Fatalf("backlogLoadParams(%q): %v", tc.raw, err)
		}
		if got != tc.want || inRange != tc.inRange {
			t.Errorf("backlogLoadParams(%q) = (%d, %v), want (%d, %v)", tc.raw, got, inRange, tc.want, tc.inRange)
		}
	}
}

// backlogStartPostgres boots one real PostgreSQL container with the embedded
// migrations applied. Skips (never passes) when no Docker provider is
// available; the container is terminated by t.Cleanup even when the test
// fails.
func backlogStartPostgres(t *testing.T) string {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()
	ctr, err := postgres.Run(ctx, backlogPostgresImage,
		postgres.WithDatabase("txharbor"),
		postgres.WithUsername("txharbor"),
		postgres.WithPassword("txharbor"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2)),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })
	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres connection string: %v", err)
	}
	opts := db.MigrateOptions{DSN: dsn, LockTimeout: 10 * time.Second, ConnectTimeout: 10 * time.Second}
	if err := db.MigrateUp(ctx, opts, io.Discard); err != nil {
		t.Fatalf("MigrateUp() error = %v", err)
	}
	return dsn
}

// backlogOpenPool opens the harness pool with an explicit connection bound
// (part of the resource basis recorded in the report).
func backlogOpenPool(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("pgxpool.ParseConfig: %v", err)
	}
	cfg.MaxConns = 8
	cfg.MinConns = 1
	cfg.MaxConnIdleTime = 5 * time.Minute
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("pgxpool.NewWithConfig: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// backlogCreateEffectTable creates the scratch effect table. It deliberately
// has no unique constraint on event_id: a duplicate effect would show up as a
// second row and fail the per-event count assertion.
func backlogCreateEffectTable(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
	event_id          UUID        NOT NULL,
	event_type        TEXT        NOT NULL,
	aggregate_type    TEXT        NOT NULL,
	aggregate_id      TEXT        NOT NULL,
	aggregate_version BIGINT      NOT NULL,
	applied_at        TIMESTAMPTZ NOT NULL DEFAULT now()
)`, backlogEffectTable)); err != nil {
		t.Fatalf("create effect table: %v", err)
	}
}

// backlogEffect is the scratch consumer effect: one row per effectively
// applied event, committed inside the T4 transaction (never on redelivery).
type backlogEffect struct {
	calls atomic.Int64
}

// Apply implements events.Effect.
func (e *backlogEffect) Apply(ctx context.Context, tx pgx.Tx, env events.Envelope) error {
	e.calls.Add(1)
	_, err := tx.Exec(ctx, fmt.Sprintf(`
INSERT INTO %s (event_id, event_type, aggregate_type, aggregate_id, aggregate_version)
VALUES ($1, $2, $3, $4, $5)`, backlogEffectTable),
		env.EventID, env.EventType, env.AggregateType, env.AggregateID, env.AggregateVersion)
	return err
}

// backlogSeed writes n events through the real Append path in bounded
// transaction batches: identity derivation, payload canonicalization, version
// derivation and the outbox insert all run, while the commit size keeps the
// harness from holding one huge transaction for the largest load.
func backlogSeed(t *testing.T, ctx context.Context, pool *pgxpool.Pool, n int) {
	t.Helper()
	for start := 0; start < n; start += backlogSeedBatch {
		end := start + backlogSeedBatch
		if end > n {
			end = n
		}
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin seed tx [%d,%d): %v", start, end, err)
		}
		for i := start; i < end; i++ {
			ev, err := events.NewEvent(events.Event{
				EventType:     events.EventTypeDepositObservationStatusChanged,
				SchemaVersion: events.SchemaVersionV1,
				IdentityKind:  events.IdentityKindBusinessObject,
				AggregateType: "deposit_observation",
				AggregateID:   fmt.Sprintf("t013-backlog-obs-%08d", i),
				Payload: map[string]any{
					"from_state": "pending",
					"to_state":   "confirmed",
					"reason":     "013 larger-backlog drain load",
				},
				OccurredAt: time.Now().UTC(),
			})
			if err != nil {
				_ = tx.Rollback(ctx)
				t.Fatalf("build load event %d: %v", i, err)
			}
			res, err := events.Append(ctx, tx, ev)
			if err != nil {
				_ = tx.Rollback(ctx)
				t.Fatalf("Append load event %d: %v", i, err)
			}
			if res.Noop {
				_ = tx.Rollback(ctx)
				t.Fatalf("load event %d was an idempotent no-op; the load must be unique", i)
			}
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("commit seed tx [%d,%d): %v", start, end, err)
		}
	}
}

// backlogCount runs one scalar count query.
func backlogCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sql string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(ctx, sql, args...).Scan(&n); err != nil {
		t.Fatalf("count (%s): %v", sql, err)
	}
	return n
}

// backlogScalarFloat runs one scalar float query.
func backlogScalarFloat(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sql string, args ...any) float64 {
	t.Helper()
	var v float64
	if err := pool.QueryRow(ctx, sql, args...).Scan(&v); err != nil {
		t.Fatalf("scalar (%s): %v", sql, err)
	}
	return v
}

// backlogWantCount is the count assertion helper (loud, non-zero on failure).
func backlogWantCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, label, sql string, want int64, args ...any) int64 {
	t.Helper()
	got := backlogCount(t, ctx, pool, sql, args...)
	if got != want {
		t.Fatalf("%s = %d, want %d", label, got, want)
	}
	return got
}

// backlogMem is the process-memory basis of one snapshot (Go runtime, after a
// forced GC so heap values are comparable).
type backlogMem struct {
	HeapAllocBytes  uint64 `json:"heap_alloc_bytes"`
	HeapSysBytes    uint64 `json:"heap_sys_bytes"`
	SysBytes        uint64 `json:"sys_bytes"`
	TotalAllocBytes uint64 `json:"total_alloc_bytes"`
	NumGC           uint32 `json:"num_gc"`
}

func backlogMemSnapshot() backlogMem {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return backlogMem{
		HeapAllocBytes:  m.HeapAlloc,
		HeapSysBytes:    m.HeapSys,
		SysBytes:        m.Sys,
		TotalAllocBytes: m.TotalAlloc,
		NumGC:           m.NumGC,
	}
}

// backlogPGSizes records the server-side basis: database size and the outbox
// relation size (table + indexes).
func backlogPGSizes(t *testing.T, ctx context.Context, pool *pgxpool.Pool) (dbBytes, outboxBytes int64) {
	t.Helper()
	if err := pool.QueryRow(ctx, `SELECT pg_database_size(current_database())`).Scan(&dbBytes); err != nil {
		t.Fatalf("pg_database_size: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT pg_total_relation_size('outbox_events')`).Scan(&outboxBytes); err != nil {
		t.Fatalf("pg_total_relation_size(outbox_events): %v", err)
	}
	return dbBytes, outboxBytes
}

// backlogBrokerRecords sums the topic's end offsets: the broker-side count of
// records ever accepted (at-least-once, so duplicates are allowed and this is
// an upper bound on distinct events).
func backlogBrokerRecords(t *testing.T, ctx context.Context, brokers []string, topic string) int64 {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatalf("kafka admin client: %v", err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	offsets, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		t.Fatalf("ListEndOffsets(%s): %v", topic, err)
	}
	var total int64
	offsets.Each(func(o kadm.ListedOffset) {
		if o.Err != nil {
			t.Fatalf("end offset %s[%d]: %v", o.Topic, o.Partition, o.Err)
		}
		if o.Offset > 0 {
			total += o.Offset
		}
	})
	return total
}

// backlogPublisherObserver records the publisher's real outcome observations
// and the outbox gauges refreshed during the drain.
type backlogPublisherObserver struct {
	mu        sync.Mutex
	failures  map[string]int
	published int
	attempts  int
	blocked   int
	pending   map[string]int
	oldest    map[string]float64
	ticks     int
}

func newBacklogPublisherObserver() *backlogPublisherObserver {
	return &backlogPublisherObserver{
		failures: map[string]int{},
		pending:  map[string]int{},
		oldest:   map[string]float64{},
	}
}

func (o *backlogPublisherObserver) ObserveOutboxPublishFailure(errorClass string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.failures[errorClass]++
}

func (o *backlogPublisherObserver) ObserveOutboxPublished(n int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.published += n
}

func (o *backlogPublisherObserver) ObserveOutboxAttempts(n int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.attempts += n
}

func (o *backlogPublisherObserver) SetOutboxBlocked(n int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.blocked = n
}

func (o *backlogPublisherObserver) SetOutboxPending(family string, count int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.pending[family] = count
	if count > 0 {
		o.ticks++
	}
}

func (o *backlogPublisherObserver) SetOutboxPendingOldestAge(family string, seconds float64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.oldest[family] = seconds
}

func (o *backlogPublisherObserver) failureTotal() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	total := 0
	for _, n := range o.failures {
		total += n
	}
	return total
}

func (o *backlogPublisherObserver) maxObservedOldest() float64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	max := 0.0
	for _, age := range o.oldest {
		if age > max {
			max = age
		}
	}
	return max
}

// backlogConsumerObserver records the consumer runtime observations.
type backlogConsumerObserver struct {
	mu          sync.Mutex
	applied     int
	retries     int
	quarantines []string
	lagCalls    int
}

func newBacklogConsumerObserver() *backlogConsumerObserver {
	return &backlogConsumerObserver{}
}

func (o *backlogConsumerObserver) ObserveConsumerApplied() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.applied++
}

func (o *backlogConsumerObserver) ObserveConsumerRetry(string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.retries++
}

func (o *backlogConsumerObserver) ObserveConsumerQuarantine(failureClass string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.quarantines = append(o.quarantines, failureClass)
}

func (o *backlogConsumerObserver) SetConsumerLag(string, int, float64, int64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.lagCalls++
}

func (o *backlogConsumerObserver) ObserveEventReplay(string, string) {}

func (o *backlogConsumerObserver) SetConsumerReplayClock(string, float64) {}

func (o *backlogConsumerObserver) appliedCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.applied
}

func (o *backlogConsumerObserver) quarantineSnapshot() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string{}, o.quarantines...)
}

func (o *backlogConsumerObserver) retryCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.retries
}

// backlogSample is one monitor tick: the durable progress/effect state at a
// point in the drain.
type backlogSample struct {
	ElapsedSeconds float64 `json:"elapsed_s"`
	Pending        int64   `json:"pending"`
	Published      int64   `json:"published"`
	Applied        int     `json:"applied"`
	Ledger         int64   `json:"ledger"`
	Progress       int64   `json:"progress_sum_next_offset"`
}

// backlogMonitor samples the durable state during the drain. It never gates
// the drain: sampling failures are recorded and asserted afterwards.
type backlogMonitor struct {
	mu      sync.Mutex
	samples []backlogSample
	dropped int
	errs    []string
	stop    chan struct{}
	done    chan struct{}
	once    sync.Once
}

func backlogStartMonitor(ctx context.Context, pool *pgxpool.Pool, pub *events.Publisher,
	observer *backlogConsumerObserver, started time.Time) *backlogMonitor {
	m := &backlogMonitor{stop: make(chan struct{}), done: make(chan struct{})}
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
				m.sample(ctx, pool, pub, observer, started)
			}
		}
	}()
	return m
}

func (m *backlogMonitor) sample(ctx context.Context, pool *pgxpool.Pool, pub *events.Publisher,
	observer *backlogConsumerObserver, started time.Time) {
	if ctx.Err() != nil {
		return
	}
	var pending, published int64
	if err := pool.QueryRow(ctx, `
SELECT count(*) FILTER (WHERE publish_state = 'pending'),
       count(*) FILTER (WHERE publish_state = 'published')
FROM outbox_events`).Scan(&pending, &published); err != nil {
		m.recordErr("outbox sample: " + err.Error())
		return
	}
	var ledger int64
	if err := pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s`, backlogEffectTable)).Scan(&ledger); err != nil {
		m.recordErr("ledger sample: " + err.Error())
		return
	}
	var progress int64
	if err := pool.QueryRow(ctx,
		`SELECT coalesce(sum(next_offset), 0) FROM consumer_progress WHERE consumer_name = $1`,
		backlogConsumerName).Scan(&progress); err != nil {
		m.recordErr("progress sample: " + err.Error())
		return
	}
	if pub != nil && pending > 0 {
		// Observability during the drain: refresh the real outbox gauges.
		// A refresh failure is recorded, never treated as a drain gate.
		if err := pub.RefreshGauges(ctx); err != nil {
			m.recordErr("RefreshGauges: " + err.Error())
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.samples) >= backlogMaxSamples {
		m.dropped++
		return
	}
	m.samples = append(m.samples, backlogSample{
		ElapsedSeconds: time.Since(started).Seconds(),
		Pending:        pending,
		Published:      published,
		Applied:        observer.appliedCount(),
		Ledger:         ledger,
		Progress:       progress,
	})
}

func (m *backlogMonitor) recordErr(text string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.errs) < 20 {
		m.errs = append(m.errs, text)
	}
}

func (m *backlogMonitor) stopAndWait() {
	m.once.Do(func() { close(m.stop) })
	<-m.done
}

func (m *backlogMonitor) snapshot() (samples []backlogSample, errs []string, dropped int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]backlogSample(nil), m.samples...), append([]string(nil), m.errs...), m.dropped
}

// backlogAssertMonotonic pins the drain's progress monotonicity from the
// sampled durable state: pending never grows, published/applied/effect/progress
// never rewind.
func backlogAssertMonotonic(t *testing.T, samples []backlogSample, errs []string) {
	t.Helper()
	if len(errs) > 0 {
		t.Fatalf("monitor sampling errors: %v", errs)
	}
	for i := 1; i < len(samples); i++ {
		prev, cur := samples[i-1], samples[i]
		switch {
		case cur.Pending > prev.Pending:
			t.Fatalf("pending grew during the drain: %d -> %d (sample %d)", prev.Pending, cur.Pending, i)
		case cur.Published < prev.Published:
			t.Fatalf("published rewound during the drain: %d -> %d (sample %d)", prev.Published, cur.Published, i)
		case cur.Applied < prev.Applied:
			t.Fatalf("consumer applied rewound during the drain: %d -> %d (sample %d)", prev.Applied, cur.Applied, i)
		case cur.Ledger < prev.Ledger:
			t.Fatalf("effect rows rewound during the drain: %d -> %d (sample %d)", prev.Ledger, cur.Ledger, i)
		case cur.Progress < prev.Progress:
			t.Fatalf("consumer progress rewound during the drain: %d -> %d (sample %d)", prev.Progress, cur.Progress, i)
		}
	}
}

// backlogMemBasis is the report's process-memory section.
type backlogMemBasis struct {
	BeforeSeed backlogMem `json:"before_seed"`
	AfterSeed  backlogMem `json:"after_seed"`
	AfterDrain backlogMem `json:"after_drain"`
}

// backlogReport is the machine-readable measurement record emitted as one JSON
// line. Every value is an actual measurement of this run ("pending" fields are
// explicit: nothing here is a production threshold or a fabricated budget).
type backlogReport struct {
	Harness             string `json:"harness"`
	StartedAt           string `json:"started_at"`
	VCSRevision         string `json:"vcs_revision,omitempty"`
	GoVersion           string `json:"go_version"`
	GOOS                string `json:"goos"`
	GOARCH              string `json:"goarch"`
	GOMAXPROCS          int    `json:"gomaxprocs"`
	HostCPUs            int    `json:"host_cpus"`
	N                   int    `json:"n"`
	InVerificationRange bool   `json:"in_verification_range"`
	SeedBatch           int    `json:"seed_batch"`
	PublishBatch        int    `json:"publish_batch"`
	PollBatch           int    `json:"poll_batch"`
	TopicPartitions     int32  `json:"topic_partitions"`
	KafkaImage          string `json:"kafka_image"`
	PostgresImage       string `json:"postgres_image"`

	PublisherConfig map[string]any `json:"publisher_config"`
	ConsumerConfig  map[string]any `json:"consumer_config"`
	SinkConfig      map[string]any `json:"sink_config"`

	SeedSeconds      float64 `json:"seed_seconds"`
	SeedEventsPerSec float64 `json:"seed_events_per_second"`

	PublishSeconds       float64 `json:"publish_seconds"`
	PublishEventsPerSec  float64 `json:"publish_events_per_second"`
	TotalDrainSeconds    float64 `json:"total_drain_seconds"`
	EndToEndEventsPerSec float64 `json:"end_to_end_events_per_second"`
	ConsumerTailSeconds  float64 `json:"consumer_catchup_tail_seconds"`

	PublishCycles   int            `json:"publish_cycles"`
	PublishClaimed  int            `json:"publish_claimed"`
	PublishAcked    int            `json:"publish_acked"`
	PublishReleased int            `json:"publish_released"`
	PublishBlocked  int            `json:"publish_blocked"`
	PublishFailures map[string]int `json:"publish_failures_by_class"`

	ConsumerApplied     int      `json:"consumer_applied"`
	ConsumerRetries     int      `json:"consumer_retries"`
	ConsumerQuarantines []string `json:"consumer_quarantines"`
	EffectCalls         int64    `json:"consumer_effect_calls"`

	InitialOldestAgeSeconds     float64 `json:"initial_oldest_age_seconds"`
	OldestWaitSeconds           float64 `json:"oldest_wait_seconds"`
	MaxObservedOldestAgeSeconds float64 `json:"max_observed_oldest_age_seconds"`
	ObservabilityTicks          int     `json:"observability_ticks"`

	OutboxRows            int64 `json:"outbox_rows"`
	OutboxPending         int64 `json:"outbox_pending"`
	OutboxPublished       int64 `json:"outbox_published"`
	OutboxBlocked         int64 `json:"outbox_blocked"`
	OutboxAttemptsSum     int64 `json:"outbox_attempts_sum"`
	OutboxPublishedAtNull int64 `json:"outbox_published_at_null"`

	EffectRows            int64 `json:"effect_rows"`
	EffectDistinctEvents  int64 `json:"effect_distinct_events"`
	EffectMaxRowsPerEvent int64 `json:"effect_max_rows_per_event"`
	InboxRows             int64 `json:"inbox_rows"`
	QuarantineRows        int64 `json:"quarantine_rows"`
	VersionRows           int64 `json:"version_rows"`
	VersionSum            int64 `json:"version_sum_max_version"`
	VersionMax            int64 `json:"version_max_max_version"`
	ProgressRows          int64 `json:"progress_rows"`
	ProgressSumNextOffset int64 `json:"progress_sum_next_offset"`
	BrokerRecords         int64 `json:"broker_records"`

	Memory    backlogMemBasis  `json:"memory"`
	PGSizes   map[string]int64 `json:"pg_sizes_bytes"`
	PoolStats map[string]any   `json:"pool_stats_after"`

	PendingCurve   []int64 `json:"pending_curve"`
	CurveDropped   int     `json:"curve_samples_dropped"`
	MonitorSamples int     `json:"monitor_samples"`
}

// backlogDownsample keeps up to max evenly spaced values.
func backlogDownsample(values []int64, max int) []int64 {
	if len(values) <= max || max <= 0 {
		return values
	}
	out := make([]int64, 0, max)
	step := float64(len(values)-1) / float64(max-1)
	for i := 0; i < max; i++ {
		out = append(out, values[int(float64(i)*step+0.5)])
	}
	return out
}

func backlogVCSRevision() string {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" {
				return setting.Value
			}
		}
	}
	return ""
}

// backlogEmitReport prints the stable one-line JSON record and, when
// TXHARBOR_BACKLOG_REPORT is set, writes it to that path.
func backlogEmitReport(t *testing.T, report backlogReport) {
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

// TestBacklogDrain is the larger-backlog drain acceptance of the 013
// verification supplement: fixed seed -> bounded seed batches -> real
// PublishOnce drain to a real broker -> real idempotent consumer -> final
// state and measurement record.
func TestBacklogDrain(t *testing.T) {
	load, inVerificationRange, err := backlogLoadParams(os.Getenv(backlogNEnv))
	if err != nil {
		t.Fatalf("invalid load parameter: %v", err)
	}
	testcontainers.SkipIfProviderIsNotHealthy(t)

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
	pool := backlogOpenPool(t, backlogStartPostgres(t))
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
		Observer:    consumerObserver,
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

	runCtx, stopConsumer := context.WithCancel(ctx)
	consumerDone := make(chan error, 1)
	go func() { consumerDone <- runtimeConsumer.Run(runCtx) }()

	// --- bounded publish drain, consumer catching up in parallel -------------
	drainStart := time.Now()
	monitor := backlogStartMonitor(ctx, pool, pub, consumerObserver, drainStart)

	cycles := 0
	var claimed, acked, released, blocked int
	publishDeadline := drainStart.Add(backlogPublishWindow)
	for {
		pending := backlogCount(t, ctx, pool,
			`SELECT count(*) FROM outbox_events WHERE publish_state = 'pending'`)
		if pending == 0 {
			break
		}
		if time.Now().After(publishDeadline) {
			t.Fatalf("publish drain did not reach 0 pending within %s (pending = %d)",
				backlogPublishWindow, pending)
		}
		outcome, err := pub.PublishOnce(ctx)
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
		if outcome.Claimed == 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}
	publishDone := time.Now()

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

	// --- final state: progress and effect, not just the pending count --------
	samples, monitorErrs, dropped := monitor.snapshot()
	backlogAssertMonotonic(t, samples, monitorErrs)

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
	if len(samples) > 0 {
		if last := samples[len(samples)-1]; last.Progress > progressSum {
			t.Fatalf("monitor saw progress %d above the final %d (progress must be monotonic)", last.Progress, progressSum)
		}
	}

	memAfterDrain := backlogMemSnapshot()
	dbSizeAfter, outboxSizeAfter := backlogPGSizes(t, ctx, pool)
	poolStats := pool.Stat()

	// --- measurement record --------------------------------------------------
	publishSeconds := publishDone.Sub(drainStart).Seconds()
	totalSeconds := consumerDoneAt.Sub(drainStart).Seconds()
	tailSeconds := consumerDoneAt.Sub(publishDone).Seconds()
	curve := make([]int64, 0, len(samples))
	for _, s := range samples {
		curve = append(curve, s.Pending)
	}
	report := backlogReport{
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
		CurveDropped:   dropped,
		MonitorSamples: len(samples),
	}
	backlogEmitReport(t, report)

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
	t.Logf("pending curve (sampled, downsample<=200): %v", report.PendingCurve)
}
