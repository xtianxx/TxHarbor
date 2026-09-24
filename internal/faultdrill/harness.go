//go:build fault

// harness.go is the T073 fault-drill harness skeleton (test-support): real
// PostgreSQL/Redis/Kafka environments through internal/testutil, stop/start
// fault injection (Redis, Kafka, dual), scenario orchestration primitives,
// metric/state snapshot collection and evidence persistence (timeline JSONL,
// environment spec + commit, metric exposition).
//
// Discipline:
//
//   - a scenario is repeatable and fails loudly: every helper returns its
//     error and Run loops never swallow one;
//   - the drill uses the real dependencies (containers, real migrations, real
//     publisher/consumer runtimes), not a hand-called state function;
//   - evidence is written outside the repository by default
//     (TXHARBOR_FAULT_EVIDENCE_DIR overrides the temp directory), so a drill
//     run never pollutes the working tree;
//   - catch-up times and lag series are recorded as measured values only;
//     no threshold is invented here.
//
// T019 (B9) extends this harness; keep additions additive.
package faultdrill

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http/httptest"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	goredis "github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/events"
	"github.com/xtianxx/txharbor/internal/metrics"
	"github.com/xtianxx/txharbor/internal/testutil"
)

// Drill defaults. Timing values are drill-local bounds (a scenario must not
// hang forever); they are never presented as production thresholds.
const (
	DrillPGImage       = "postgres:18.6-trixie"
	DrillOwner         = "faultdrill"
	DrillPollInterval  = 50 * time.Millisecond
	DrillAppendTimeout = 30 * time.Second
)

// Env is one real-dependency drill environment: a migrated PostgreSQL
// container, a real Kafka broker with the canonical topic, a real Redis
// instance, the shared metrics registry and the evidence writer.
type Env struct {
	DSN      string
	Pool     *pgxpool.Pool
	Kafka    *testutil.Kafka
	Redis    *testutil.Redis
	Anvil    *Anvil
	Metrics  *metrics.Metrics
	Evidence *EvidenceWriter

	commit string

	mu      sync.Mutex
	closers []func()
}

// StartEnv boots the real dependencies, applies the embedded migrations and
// opens the pool. The caller owns Close.
func StartEnv(ctx context.Context) (*Env, error) {
	env := &Env{commit: detectCommit()}

	ctr, err := postgres.Run(ctx, DrillPGImage,
		postgres.WithDatabase("txharbor"),
		postgres.WithUsername("txharbor"),
		postgres.WithPassword("txharbor"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2)),
	)
	if err != nil {
		return nil, fmt.Errorf("start postgres container: %w", err)
	}
	env.addCloser("postgres", func() { _ = ctr.Terminate(context.Background()) })
	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		_ = env.Close(context.Background())
		return nil, fmt.Errorf("postgres connection string: %w", err)
	}
	env.DSN = dsn
	if err := db.MigrateUp(ctx, db.MigrateOptions{DSN: dsn, LockTimeout: 10 * time.Second, ConnectTimeout: 10 * time.Second}, io.Discard); err != nil {
		_ = env.Close(context.Background())
		return nil, fmt.Errorf("apply migrations: %w", err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		_ = env.Close(context.Background())
		return nil, fmt.Errorf("pgxpool.New: %w", err)
	}
	env.Pool = pool

	kafka, err := testutil.StartKafka(ctx)
	if err != nil {
		_ = env.Close(context.Background())
		return nil, fmt.Errorf("start kafka: %w", err)
	}
	env.Kafka = kafka
	if err := kafka.EnsureTopic(ctx, testutil.KafkaTopic, testutil.KafkaPartitions); err != nil {
		_ = env.Close(context.Background())
		return nil, fmt.Errorf("ensure topic: %w", err)
	}

	redis, err := testutil.StartRedis(ctx)
	if err != nil {
		_ = env.Close(context.Background())
		return nil, fmt.Errorf("start redis: %w", err)
	}
	env.Redis = redis

	env.Metrics = metrics.New(func() bool { return true })

	evidence, err := NewEvidenceWriter()
	if err != nil {
		_ = env.Close(context.Background())
		return nil, fmt.Errorf("evidence writer: %w", err)
	}
	env.Evidence = evidence
	if err := evidence.WriteJSON("env_spec.json", env.EnvSpec()); err != nil {
		_ = env.Close(context.Background())
		return nil, fmt.Errorf("write env spec: %w", err)
	}
	return env, nil
}

// Close releases every dependency and the evidence writer; it is safe to call
// after a partial start (each closer is nil-guarded).
func (e *Env) Close(ctx context.Context) error {
	if e == nil {
		return nil
	}
	if e.Pool != nil {
		e.Pool.Close()
		e.Pool = nil
	}
	if e.Anvil != nil {
		_ = e.Anvil.Terminate(ctx)
		e.Anvil = nil
	}
	if e.Kafka != nil {
		_ = e.Kafka.Close(ctx)
		e.Kafka = nil
	}
	if e.Redis != nil {
		_ = e.Redis.Close(ctx)
		e.Redis = nil
	}
	e.mu.Lock()
	closers := e.closers
	e.closers = nil
	e.mu.Unlock()
	for i := len(closers) - 1; i >= 0; i-- {
		closers[i]()
	}
	if e.Evidence != nil {
		return e.Evidence.Close()
	}
	return nil
}

// StartAnvil boots the pinned local chain once per environment (idempotent).
// The five-state drills need real on-chain deposits; the harness never
// fabricates an observation.
func (e *Env) StartAnvil(ctx context.Context) (*Anvil, error) {
	if e.Anvil != nil {
		return e.Anvil, nil
	}
	anvil, err := startAnvil(ctx)
	if err != nil {
		return nil, err
	}
	e.Anvil = anvil
	e.addCloser("anvil", func() { _ = anvil.Terminate(context.Background()) })
	return anvil, nil
}

func (e *Env) addCloser(_ string, fn func()) {
	e.mu.Lock()
	e.closers = append(e.closers, fn)
	e.mu.Unlock()
}

// EnvSpec is the environment record persisted as evidence (no credentials:
// the DSN is deliberately omitted).
type EnvSpec struct {
	Commit       string    `json:"commit"`
	GoVersion    string    `json:"go_version"`
	PGImage      string    `json:"pg_image"`
	KafkaImage   string    `json:"kafka_image"`
	RedisImage   string    `json:"redis_image"`
	KafkaTopic   string    `json:"kafka_topic"`
	KafkaBrokers []string  `json:"kafka_brokers"`
	RedisAddr    string    `json:"redis_addr"`
	StartedAt    time.Time `json:"started_at"`
	Layer        string    `json:"layer"`
}

// EnvSpec renders the current environment record.
func (e *Env) EnvSpec() EnvSpec {
	spec := EnvSpec{
		Commit:     e.commit,
		GoVersion:  runtime.Version(),
		PGImage:    DrillPGImage,
		KafkaImage: testutil.KafkaImage,
		RedisImage: testutil.RedisImage,
		KafkaTopic: testutil.KafkaTopic,
		StartedAt:  time.Now().UTC(),
		Layer:      "fault",
	}
	if e.Kafka != nil {
		spec.KafkaBrokers = e.Kafka.Brokers()
	}
	if e.Redis != nil {
		spec.RedisAddr = e.Redis.HostPort()
	}
	return spec
}

// --- fault injection ---------------------------------------------------------

// StopKafka removes the broker ("Kafka unavailable").
func (e *Env) StopKafka(ctx context.Context) error {
	if e.Kafka == nil {
		return errors.New("faultdrill: kafka is not running")
	}
	return e.Kafka.Stop(ctx)
}

// StartKafka boots a fresh broker on the same fixed address and re-creates the
// canonical topic explicitly (the testcontainers Kafka module cannot restart
// the same container; auto-create stays disabled).
func (e *Env) StartKafka(ctx context.Context) error {
	if e.Kafka == nil {
		return errors.New("faultdrill: kafka is not configured")
	}
	if err := e.Kafka.Start(ctx); err != nil {
		return err
	}
	return e.Kafka.EnsureTopic(ctx, testutil.KafkaTopic, testutil.KafkaPartitions)
}

// SuspendKafka freezes the broker's Java processes (SIGSTOP): the broker is
// unavailable to clients while its log segments and committed offsets are
// preserved. This is the outage primitive for consumer catch-up drills, where
// a fresh broker (Start) would invalidate the durable resume offset.
func (e *Env) SuspendKafka(ctx context.Context) error {
	if e.Kafka == nil {
		return errors.New("faultdrill: kafka is not running")
	}
	return e.Kafka.Suspend(ctx)
}

// ResumeKafka unfreezes a suspended broker (SIGCONT); it continues exactly
// where it stopped.
func (e *Env) ResumeKafka(ctx context.Context) error {
	if e.Kafka == nil {
		return errors.New("faultdrill: kafka is not configured")
	}
	return e.Kafka.Resume(ctx)
}

// SuspendDual suspends Kafka and stops Redis together ("dual fault", PG stays
// up).
func (e *Env) SuspendDual(ctx context.Context) error {
	if err := e.SuspendKafka(ctx); err != nil {
		return err
	}
	return e.StopRedis(ctx)
}

// ResumeAll resumes Kafka and starts Redis.
func (e *Env) ResumeAll(ctx context.Context) error {
	if err := e.ResumeKafka(ctx); err != nil {
		return err
	}
	return e.StartRedis(ctx)
}

// StopRedis stops the Redis container ("Redis unavailable").
func (e *Env) StopRedis(ctx context.Context) error {
	if e.Redis == nil {
		return errors.New("faultdrill: redis is not running")
	}
	return e.Redis.Stop(ctx)
}

// StartRedis restarts the Redis container on the same fixed address.
func (e *Env) StartRedis(ctx context.Context) error {
	if e.Redis == nil {
		return errors.New("faultdrill: redis is not configured")
	}
	return e.Redis.Start(ctx)
}

// StopDual stops Kafka and Redis together ("dual fault", PG stays up).
func (e *Env) StopDual(ctx context.Context) error {
	if err := e.StopKafka(ctx); err != nil {
		return err
	}
	return e.StopRedis(ctx)
}

// StartAll recovers both dependencies.
func (e *Env) StartAll(ctx context.Context) error {
	if err := e.StartKafka(ctx); err != nil {
		return err
	}
	return e.StartRedis(ctx)
}

// --- observation -------------------------------------------------------------

// Snapshot is one point-in-time observation of the drill state: the durable
// outbox counts, the oldest pending wait, the consumer progress rows, the
// reference-ledger effect rows and the broker availability/high-watermark.
type Snapshot struct {
	At                     time.Time        `json:"at"`
	OutboxPending          int64            `json:"outbox_pending"`
	OutboxPublished        int64            `json:"outbox_published"`
	OutboxBlocked          int64            `json:"outbox_blocked"`
	OutboxOldestAgeSeconds float64          `json:"outbox_oldest_age_seconds"`
	PendingFamilies        map[string]int64 `json:"pending_families"`
	ConsumerProgressRows   int64            `json:"consumer_progress_rows"`
	LedgerRows             int64            `json:"ledger_rows"`
	KafkaAvailable         bool             `json:"kafka_available"`
	KafkaHighWatermark     int64            `json:"kafka_high_watermark"`
}

// Snapshot reads the durable state. The PostgreSQL part is required (an error
// fails the scenario); the Kafka part is optional and reports availability.
func (e *Env) Snapshot(ctx context.Context) (Snapshot, error) {
	snap := Snapshot{At: time.Now().UTC(), PendingFamilies: map[string]int64{}}
	if err := e.Pool.QueryRow(ctx, `
SELECT count(*) FILTER (WHERE publish_state = 'pending'),
       count(*) FILTER (WHERE publish_state = 'published'),
       count(*) FILTER (WHERE publish_state = 'blocked'),
       coalesce(extract(epoch FROM now() - min(created_at) FILTER (WHERE publish_state = 'pending')), 0)
FROM outbox_events`).
		Scan(&snap.OutboxPending, &snap.OutboxPublished, &snap.OutboxBlocked, &snap.OutboxOldestAgeSeconds); err != nil {
		return Snapshot{}, fmt.Errorf("outbox snapshot: %w", err)
	}
	rows, err := e.Pool.Query(ctx, `
SELECT split_part(event_type, '.', 1), count(*)::bigint
FROM outbox_events WHERE publish_state = 'pending' GROUP BY 1`)
	if err != nil {
		return Snapshot{}, fmt.Errorf("pending families: %w", err)
	}
	for rows.Next() {
		var family string
		var count int64
		if err := rows.Scan(&family, &count); err != nil {
			rows.Close()
			return Snapshot{}, fmt.Errorf("scan pending family: %w", err)
		}
		snap.PendingFamilies[family] = count
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return Snapshot{}, fmt.Errorf("iterate pending families: %w", err)
	}
	if err := e.Pool.QueryRow(ctx, `SELECT count(*) FROM consumer_progress`).Scan(&snap.ConsumerProgressRows); err != nil {
		return Snapshot{}, fmt.Errorf("consumer progress snapshot: %w", err)
	}
	if err := e.Pool.QueryRow(ctx, `SELECT count(*) FROM `+events.RefLedgerTable).Scan(&snap.LedgerRows); err != nil {
		return Snapshot{}, fmt.Errorf("ledger snapshot: %w", err)
	}
	if highWater, err := e.kafkaHighWatermarkBounded(ctx); err == nil {
		snap.KafkaAvailable = true
		snap.KafkaHighWatermark = highWater
	}
	return snap, nil
}

// kafkaHighWatermarkBounded bounds the availability probe so a suspended or
// stopped broker cannot stall a snapshot.
func (e *Env) kafkaHighWatermarkBounded(ctx context.Context) (int64, error) {
	probeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return e.KafkaHighWatermark(probeCtx)
}

// KafkaHighWatermark sums the end offsets of the canonical topic's partitions.
func (e *Env) KafkaHighWatermark(ctx context.Context) (int64, error) {
	if e.Kafka == nil || len(e.Kafka.Brokers()) == 0 {
		return 0, errors.New("faultdrill: kafka is not running")
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(e.Kafka.Brokers()...))
	if err != nil {
		return 0, fmt.Errorf("kafka admin client: %w", err)
	}
	defer cl.Close()
	ends, err := kadm.NewClient(cl).ListEndOffsets(ctx, testutil.KafkaTopic)
	if err != nil {
		return 0, fmt.Errorf("list end offsets: %w", err)
	}
	var total int64
	for _, partitions := range ends {
		for _, listed := range partitions {
			if listed.Err != nil {
				return 0, listed.Err
			}
			if listed.Offset > 0 {
				total += listed.Offset
			}
		}
	}
	return total, nil
}

// RedisPing reports whether Redis accepts a command: a drill-only,
// non-authoritative availability probe used to verify the fault injection
// actually took effect. It never participates in any event or funding
// decision (the event pipeline must not depend on Redis at all).
func (e *Env) RedisPing(ctx context.Context) bool {
	if e.Redis == nil {
		return false
	}
	client := goredis.NewClient(&goredis.Options{
		Addr:         e.Redis.HostPort(),
		DialTimeout:  time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	})
	defer client.Close()
	pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return client.Ping(pingCtx).Err() == nil
}

// CatchupGap counts published events without a reference-ledger effect: the
// durable consumer catch-up gap (decreases to 0 when the consumer has caught
// up). It reads PostgreSQL only.
func (e *Env) CatchupGap(ctx context.Context) (int64, error) {
	var gap int64
	err := e.Pool.QueryRow(ctx, `
SELECT count(*) FROM outbox_events o
WHERE o.publish_state = 'published'
  AND NOT EXISTS (
    SELECT 1 FROM `+events.RefLedgerTable+` l
    WHERE l.consumer_name = $1 AND l.event_id = o.event_id)`, events.RefConsumerName).Scan(&gap)
	if err != nil {
		return 0, fmt.Errorf("catch-up gap: %w", err)
	}
	return gap, nil
}

// MetricsText renders the Prometheus exposition of the shared registry for the
// evidence package.
func (e *Env) MetricsText() string {
	if e.Metrics == nil {
		return ""
	}
	rec := httptest.NewRecorder()
	e.Metrics.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	return rec.Body.String()
}

// --- real runtimes -----------------------------------------------------------

// AppendDepositEvent commits one deposit.observation.created event through the
// real Append path in its own transaction and returns its deterministic event
// identity.
func (e *Env) AppendDepositEvent(ctx context.Context, n int) (string, error) {
	obsID := fmt.Sprintf("faultdrill-obs-%d-%d", time.Now().UnixNano(), n)
	ev, err := events.NewEvent(events.Event{
		EventType:     events.EventTypeDepositObservationCreated,
		SchemaVersion: events.SchemaVersionV1,
		IdentityKind:  events.IdentityKindEVMLog,
		AggregateType: "deposit_observation",
		AggregateID:   obsID,
		Payload:       map[string]any{"observation_id": obsID, "state": "pending"},
		OccurredAt:    time.Now().UTC(),
		ChainID:       31337,
		BlockNumber:   int64(5000 + n),
		BlockHash:     fmt.Sprintf("0x%064x", 0x3300+n),
		TxHash:        fmt.Sprintf("0x%064x", 0x4400+n),
		LogIndex:      n,
	})
	if err != nil {
		return "", fmt.Errorf("build event: %w", err)
	}
	tx, err := e.Pool.Begin(ctx)
	if err != nil {
		return "", fmt.Errorf("begin append: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	res, err := events.Append(ctx, tx, ev)
	if err != nil {
		return "", fmt.Errorf("append event: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return "", fmt.Errorf("commit append: %w", err)
	}
	return res.EventID.String(), nil
}

// NewPublisher builds the real publisher over a real Kafka sink and the shared
// metrics observer. The sink is closed with the environment.
func (e *Env) NewPublisher(owner string, batch int) (*events.Publisher, error) {
	sink, err := events.NewKafkaSink(events.KafkaSinkConfig{
		Brokers:         e.Kafka.Brokers(),
		Topic:           testutil.KafkaTopic,
		DeliveryTimeout: 3 * time.Second,
		RequestTimeout:  time.Second,
		MaxInflight:     5,
		MaxBuffered:     64,
	})
	if err != nil {
		return nil, fmt.Errorf("kafka sink: %w", err)
	}
	e.mu.Lock()
	e.closers = append(e.closers, sink.Close)
	e.mu.Unlock()
	pub, err := events.NewPublisher(e.Pool, sink, owner, events.PublisherOptions{
		Batch:       batch,
		LeaseTTL:    30 * time.Second,
		BackoffBase: time.Second,
		BackoffMax:  5 * time.Second,
		Jitter:      func() float64 { return 0 },
		Observer:    e.Metrics,
	})
	if err != nil {
		return nil, fmt.Errorf("publisher: %w", err)
	}
	return pub, nil
}

// EnsureReferenceLedger creates the reference consumer's simulated-ledger
// schema without building a Kafka runtime. Building a runtime client is a
// consumer-group join (franz-go's group manager starts at construction and the
// member holds partitions even before Run), so a schema-only step MUST NOT
// create one: a leaked client would own partitions it never fetches and stall
// every real consumer in the same group.
func (e *Env) EnsureReferenceLedger(ctx context.Context) error {
	reference, err := events.NewReferenceConsumer(e.Pool, e.referenceOptions())
	if err != nil {
		return fmt.Errorf("reference consumer: %w", err)
	}
	return reference.EnsureLedgerSchema(ctx)
}

// referenceOptions are the drill bounds of the reference consumer.
func (e *Env) referenceOptions() events.ConsumerOptions {
	return events.ConsumerOptions{
		GapWait:     500 * time.Millisecond,
		BackoffBase: 10 * time.Millisecond,
		BackoffMax:  100 * time.Millisecond,
		RetryLimit:  4,
		ChainID:     31337,
		Jitter:      func() float64 { return 0 },
		Observer:    e.Metrics,
	}
}

// NewConsumerRuntime builds the real reference consumer (+ simulated ledger)
// and the real Kafka group consumer over it. Chain identity is the drill
// chain.
//
// Group-membership pitfall (observed in T073): the returned runtime's client
// joins the consumer group at construction and holds its assigned partitions
// even if Run is never called. A runtime that is not going to be Run MUST be
// closed (or simply not created: use EnsureReferenceLedger for schema-only
// work), otherwise it becomes a phantom member that stalls the real consumer.
func (e *Env) NewConsumerRuntime(ctx context.Context) (*events.KafkaConsumer, *events.ReferenceConsumer, error) {
	reference, err := events.NewReferenceConsumer(e.Pool, e.referenceOptions())
	if err != nil {
		return nil, nil, fmt.Errorf("reference consumer: %w", err)
	}
	if err := reference.EnsureLedgerSchema(ctx); err != nil {
		return nil, nil, fmt.Errorf("ledger schema: %w", err)
	}
	runtime, err := events.NewKafkaConsumer(events.KafkaConsumerConfig{
		Brokers:        e.Kafka.Brokers(),
		Topic:          testutil.KafkaTopic,
		GroupPrefix:    "faultdrill",
		ConsumerName:   events.RefConsumerName,
		PollBatch:      10,
		CommitInterval: 200 * time.Millisecond,
	}, reference.Consumer)
	if err != nil {
		return nil, nil, fmt.Errorf("kafka consumer: %w", err)
	}
	return runtime, reference, nil
}

// ForceRetry clears the retry backoff of every pending row so a recovered
// drill does not wait out a backoff window (drill-local determinism).
func (e *Env) ForceRetry(ctx context.Context) error {
	if _, err := e.Pool.Exec(ctx, `UPDATE outbox_events SET next_attempt_at = now() WHERE publish_state = 'pending'`); err != nil {
		return fmt.Errorf("force retry: %w", err)
	}
	return nil
}

// RunPublisherLoop drives bounded publish cycles until ctx is done. observe,
// when non-nil, receives one outcome per cycle so a scenario can assert
// bounded claims and record the drain curve. Errors are returned, never
// swallowed (a drill fails loudly), and every cycle is wall-clock bounded so
// an unresponsive broker cannot hang the loop silently.
func RunPublisherLoop(ctx context.Context, pub *events.Publisher, interval time.Duration,
	observe func(events.PublishOutcome)) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		outcome, err := PublishOnceBounded(ctx, pub, 30*time.Second)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if observe != nil {
			observe(outcome)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(interval):
		}
	}
}

// PublishOnceBounded runs one publish cycle with a hard wall-clock bound: a
// cycle that never returns is a drill failure (a loud error, never a silent
// hang). The produce promise can outlive the sink's configured timeouts when
// a broker stops answering mid-request, so drills must bound the wait.
func PublishOnceBounded(ctx context.Context, pub *events.Publisher, limit time.Duration) (events.PublishOutcome, error) {
	type result struct {
		outcome events.PublishOutcome
		err     error
	}
	ch := make(chan result, 1)
	go func() {
		outcome, err := pub.PublishOnce(ctx)
		ch <- result{outcome, err}
	}()
	select {
	case r := <-ch:
		return r.outcome, r.err
	case <-time.After(limit):
		return events.PublishOutcome{}, fmt.Errorf("publish cycle did not return within %s", limit)
	}
}

// RunConsumerLoop runs the real group consumer; an error is returned, never
// swallowed.
func RunConsumerLoop(ctx context.Context, runtime *events.KafkaConsumer) error {
	return runtime.Run(ctx)
}

// WaitFor polls cond until it holds or the timeout elapses; the last error is
// reported.
func WaitFor(ctx context.Context, timeout time.Duration, cond func() (bool, error)) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		ok, err := cond()
		if err != nil {
			lastErr = err
		} else if ok {
			return nil
		}
		if time.Now().After(deadline) {
			if lastErr != nil {
				return fmt.Errorf("condition not reached within %s: %w", timeout, lastErr)
			}
			return fmt.Errorf("condition not reached within %s", timeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(DrillPollInterval):
		}
	}
}

// --- evidence ----------------------------------------------------------------

// evidenceDirEnv overrides the evidence directory. Evidence never defaults
// into the repository working tree.
const evidenceDirEnv = "TXHARBOR_FAULT_EVIDENCE_DIR"

// EvidenceWriter persists the drill evidence: timeline.jsonl (one JSON object
// per recorded event), named JSON exports and free-form files. It is safe for
// concurrent use.
type EvidenceWriter struct {
	dir  string
	mu   sync.Mutex
	file *os.File
}

// NewEvidenceWriter opens the evidence directory (the environment override or
// a fresh temp directory).
func NewEvidenceWriter() (*EvidenceWriter, error) {
	dir := strings.TrimSpace(os.Getenv(evidenceDirEnv))
	if dir == "" {
		var err error
		dir, err = os.MkdirTemp("", "txharbor-fault-evidence-*")
		if err != nil {
			return nil, fmt.Errorf("evidence temp dir: %w", err)
		}
	} else if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("evidence dir: %w", err)
	}
	file, err := os.OpenFile(dir+"/timeline.jsonl", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open timeline: %w", err)
	}
	return &EvidenceWriter{dir: dir, file: file}, nil
}

// Dir returns the evidence directory.
func (w *EvidenceWriter) Dir() string { return w.dir }

// Record appends one JSON line to the timeline.
func (w *EvidenceWriter) Record(kind string, data any) error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	entry := map[string]any{"at": time.Now().UTC(), "kind": kind, "data": data}
	line, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal evidence entry: %w", err)
	}
	if _, err := w.file.Write(append(line, '\n')); err != nil {
		return fmt.Errorf("write evidence entry: %w", err)
	}
	return nil
}

// WriteJSON writes one named JSON export into the evidence directory.
func (w *EvidenceWriter) WriteJSON(name string, data any) error {
	if w == nil {
		return nil
	}
	body, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal %s: %w", name, err)
	}
	return os.WriteFile(w.dir+"/"+name, append(body, '\n'), 0o644)
}

// WriteFile writes one named free-form file into the evidence directory.
func (w *EvidenceWriter) WriteFile(name string, body []byte) error {
	if w == nil {
		return nil
	}
	return os.WriteFile(w.dir+"/"+name, body, 0o644)
}

// Close flushes and closes the timeline.
func (w *EvidenceWriter) Close() error {
	if w == nil || w.file == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	err := w.file.Close()
	w.file = nil
	return err
}

// detectCommit resolves the evidence commit: TXHARBOR_COMMIT when set, else
// `git rev-parse HEAD` from the repository, else "unknown" (never invented).
func detectCommit() string {
	if commit := strings.TrimSpace(os.Getenv("TXHARBOR_COMMIT")); commit != "" {
		return commit
	}
	out, err := exec.Command("git", "rev-parse", "HEAD").Output()
	if err != nil {
		return "unknown"
	}
	commit := strings.TrimSpace(string(out))
	if commit == "" {
		return "unknown"
	}
	return commit
}

// --- T019: five-state orchestration ------------------------------------------

// StateKind names one of the five service states of the FR-04 matrix.
type StateKind string

const (
	// StateNormal: Redis and Kafka both reachable.
	StateNormal StateKind = "normal"
	// StateRedisDown: Redis unavailable, Kafka reachable.
	StateRedisDown StateKind = "redis_down"
	// StateKafkaDown: Kafka unavailable (broker processes suspended: its log
	// and committed offsets survive), Redis reachable.
	StateKafkaDown StateKind = "kafka_down"
	// StateDual: Redis unavailable and Kafka unavailable, PostgreSQL and the
	// local chain stay up (the only dual fault in scope).
	StateDual StateKind = "dual"
	// StateCatchup: both dependencies reachable after a fault; the publisher
	// drains and the consumer catches up from durable progress.
	StateCatchup StateKind = "catchup"
)

// StateSequence is the canonical drill order (normal → faults → recovery).
var StateSequence = []StateKind{StateNormal, StateRedisDown, StateKafkaDown, StateDual, StateCatchup}

// StateLabel maps the state to the approved spec matrix column.
var StateLabel = map[StateKind]string{
	StateNormal:    "正常",
	StateRedisDown: "仅 Redis 故障",
	StateKafkaDown: "仅 Kafka 故障",
	StateDual:      "双故障",
	StateCatchup:   "恢复追赶",
}

// KafkaReachable reports whether a bounded broker probe answers.
func (e *Env) KafkaReachable(ctx context.Context) bool {
	if e.Kafka == nil {
		return false
	}
	probeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_, err := e.KafkaHighWatermark(probeCtx)
	return err == nil
}

// EnsureKafkaUp restores a reachable broker: SIGCONT when the broker was
// suspended (log and offsets preserved), Start + explicit topic when it was
// stopped. It waits until the broker answers.
func (e *Env) EnsureKafkaUp(ctx context.Context) error {
	if e.KafkaReachable(ctx) {
		return nil
	}
	if err := e.ResumeKafka(ctx); err != nil {
		if startErr := e.StartKafka(ctx); startErr != nil {
			return fmt.Errorf("restore kafka: resume: %v; start: %w", err, startErr)
		}
	}
	return WaitFor(ctx, 90*time.Second, func() (bool, error) {
		return e.KafkaReachable(ctx), nil
	})
}

// EnsureRedisUp starts Redis when it does not answer and waits for readiness.
func (e *Env) EnsureRedisUp(ctx context.Context) error {
	if e.RedisPing(ctx) {
		return nil
	}
	if err := e.StartRedis(ctx); err != nil {
		return fmt.Errorf("restore redis: %w", err)
	}
	return WaitFor(ctx, 60*time.Second, func() (bool, error) {
		return e.RedisPing(ctx), nil
	})
}

// ApplyState moves the environment into one of the five states. Faults are
// injected through the real primitives: Redis container stop and Kafka broker
// SIGSTOP (the broker keeps its log and committed offsets, so a consumer's
// durable resume offset stays valid).
func (e *Env) ApplyState(ctx context.Context, state StateKind) error {
	switch state {
	case StateNormal, StateCatchup:
		if err := e.EnsureRedisUp(ctx); err != nil {
			return err
		}
		return e.EnsureKafkaUp(ctx)
	case StateRedisDown:
		if err := e.EnsureKafkaUp(ctx); err != nil {
			return err
		}
		return e.StopRedis(ctx)
	case StateKafkaDown:
		if err := e.EnsureRedisUp(ctx); err != nil {
			return err
		}
		return e.SuspendKafka(ctx)
	case StateDual:
		if err := e.SuspendKafka(ctx); err != nil {
			return err
		}
		return e.StopRedis(ctx)
	default:
		return fmt.Errorf("faultdrill: unknown state %q", state)
	}
}

// VerifyState waits until the observed dependency reachability matches the
// state and fails loudly on a timeout. A state that was not actually injected
// must never be recorded as evidence.
func (e *Env) VerifyState(ctx context.Context, state StateKind) error {
	wantRedis, wantKafka := false, false
	switch state {
	case StateNormal, StateCatchup:
		wantRedis, wantKafka = true, true
	case StateRedisDown:
		wantRedis, wantKafka = false, true
	case StateKafkaDown:
		wantRedis, wantKafka = true, false
	case StateDual:
		wantRedis, wantKafka = false, false
	default:
		return fmt.Errorf("faultdrill: unknown state %q", state)
	}
	return WaitFor(ctx, 90*time.Second, func() (bool, error) {
		return e.RedisPing(ctx) == wantRedis && e.KafkaReachable(ctx) == wantKafka, nil
	})
}

// --- T019: seven-class matrix vocabulary -------------------------------------

// The seven operation classes of the FR-04 matrix.
const (
	ClassDepositProcessing = "deposit_processing"
	ClassConfirmationReorg = "confirmation_reorg"
	ClassWithdrawalCreate  = "withdrawal_create"
	ClassExistingExecution = "existing_execution"
	ClassQuery             = "query"
	ClassEventSubscription = "event_subscription"
	ClassNonCritical       = "non_critical"
)

// MatrixClasses is the canonical column order.
var MatrixClasses = []string{
	ClassDepositProcessing, ClassConfirmationReorg, ClassWithdrawalCreate,
	ClassExistingExecution, ClassQuery, ClassEventSubscription, ClassNonCritical,
}

// Verdict vocabulary: the observed behavior reduced to the matrix cell.
const (
	// VerdictContinue: the operation proceeds under its existing gates.
	VerdictContinue = "continue"
	// VerdictDegraded: the operation proceeds with an explicit degradation
	// annotation (query/status surfaces).
	VerdictDegraded = "degraded"
	// VerdictRefused: the operation is refused with a clear retryable error.
	VerdictRefused = "refused"
	// VerdictBacklog: delivery stops while committed events stay in the
	// Outbox (observable backlog); nothing is lost.
	VerdictBacklog = "backlog"
	// VerdictCatchup: delivery resumes and the consumer catches up from
	// durable facts.
	VerdictCatchup = "catchup"
)

// MatrixExpected is the approved spec matrix (FR-04 table) reduced to machine
// verdicts. It is the comparison target of the drill assertions; changing it
// would be a business-semantics change and MUST NOT be done silently.
var MatrixExpected = map[StateKind]map[string]string{
	StateNormal: {
		ClassDepositProcessing: VerdictContinue,
		ClassConfirmationReorg: VerdictContinue,
		ClassWithdrawalCreate:  VerdictContinue,
		ClassExistingExecution: VerdictContinue,
		ClassQuery:             VerdictContinue,
		ClassEventSubscription: VerdictContinue,
		ClassNonCritical:       VerdictContinue,
	},
	StateRedisDown: {
		ClassDepositProcessing: VerdictContinue,
		ClassConfirmationReorg: VerdictContinue,
		ClassWithdrawalCreate:  VerdictRefused, // PD-1
		ClassExistingExecution: VerdictContinue,
		ClassQuery:             VerdictDegraded,
		ClassEventSubscription: VerdictContinue,
		ClassNonCritical:       VerdictDegraded,
	},
	StateKafkaDown: {
		ClassDepositProcessing: VerdictContinue,
		ClassConfirmationReorg: VerdictContinue,
		ClassWithdrawalCreate:  VerdictContinue, // refused only at the capacity boundary (PD-2)
		ClassExistingExecution: VerdictContinue,
		ClassQuery:             VerdictDegraded,
		ClassEventSubscription: VerdictBacklog,
		ClassNonCritical:       VerdictDegraded,
	},
	StateDual: {
		ClassDepositProcessing: VerdictContinue,
		ClassConfirmationReorg: VerdictContinue,
		ClassWithdrawalCreate:  VerdictRefused, // PD-1 + PD-2
		ClassExistingExecution: VerdictContinue,
		ClassQuery:             VerdictDegraded,
		ClassEventSubscription: VerdictBacklog,
		ClassNonCritical:       VerdictDegraded,
	},
	StateCatchup: {
		ClassDepositProcessing: VerdictContinue,
		ClassConfirmationReorg: VerdictContinue,
		ClassWithdrawalCreate:  VerdictContinue,
		ClassExistingExecution: VerdictContinue,
		ClassQuery:             VerdictContinue,
		ClassEventSubscription: VerdictCatchup,
		ClassNonCritical:       VerdictContinue,
	},
}

// AssertMatrixRow compares one observed state row with the approved matrix and
// fails loudly on any missing or mismatching cell (一致性率 must be 100%).
func AssertMatrixRow(t *testing.T, state StateKind, observed map[string]string) {
	t.Helper()
	expected := MatrixExpected[state]
	if expected == nil {
		t.Fatalf("no matrix expectation for state %q", state)
	}
	for _, class := range MatrixClasses {
		want, ok := expected[class]
		if !ok {
			t.Fatalf("matrix expectation for state %q is missing class %q", state, class)
		}
		got, ok := observed[class]
		if !ok {
			t.Fatalf("state %q: class %q was not observed (matrix cell missing)", state, class)
		}
		if got != want {
			t.Fatalf("state %q (%s) class %q: observed %q, matrix requires %q",
				state, StateLabel[state], class, got, want)
		}
	}
}

// MatrixConsistency counts matching cells across every observed state row.
func MatrixConsistency(observed map[StateKind]map[string]string) (consistent, total int) {
	for state, row := range observed {
		expected := MatrixExpected[state]
		for _, class := range MatrixClasses {
			total++
			if expected[class] == row[class] {
				consistent++
			}
		}
	}
	return consistent, total
}

// --- T019: FR-06 invariant collection ----------------------------------------

// OutboxTotals is the durable outbox state.
type OutboxTotals struct {
	Pending   int64 `json:"pending"`
	Published int64 `json:"published"`
	Blocked   int64 `json:"blocked"`
	Total     int64 `json:"total"`
}

// OutboxTotals reads the outbox counts (PostgreSQL only).
func (e *Env) OutboxTotals(ctx context.Context) (OutboxTotals, error) {
	var totals OutboxTotals
	err := e.Pool.QueryRow(ctx, `
SELECT count(*) FILTER (WHERE publish_state = 'pending'),
       count(*) FILTER (WHERE publish_state = 'published'),
       count(*) FILTER (WHERE publish_state = 'blocked'),
       count(*)
FROM outbox_events`).Scan(&totals.Pending, &totals.Published, &totals.Blocked, &totals.Total)
	if err != nil {
		return OutboxTotals{}, fmt.Errorf("outbox totals: %w", err)
	}
	return totals, nil
}

// AuthorityFingerprint counts every send-side authority table. Event
// delivery, consumption, replay and catch-up MUST leave every count
// unchanged: a processed event is never a send permission (FR-05). The
// receive-side withdrawal_requests table is deliberately not part of the
// fingerprint: it changes through the explicit create API, never through the
// event path.
func (e *Env) AuthorityFingerprint(ctx context.Context) (map[string]int64, error) {
	tables := []string{
		"payment_intents", "nonce_bindings",
		"tx_attempts", "tx_attempt_signings", "tx_send_attempts",
	}
	fingerprint := make(map[string]int64, len(tables))
	for _, table := range tables {
		var count int64
		if err := e.Pool.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&count); err != nil {
			return nil, fmt.Errorf("count %s: %w", table, err)
		}
		fingerprint[table] = count
	}
	return fingerprint, nil
}

// InvariantReport is the FR-06 five-zero evidence collectable from durable
// facts after the drill.
type InvariantReport struct {
	DuplicateIntents      int64 `json:"duplicate_intents"`
	DuplicateNonces       int64 `json:"duplicate_nonces"`
	DuplicateSendSlots    int64 `json:"duplicate_send_slots"`
	OrphanedLedgerCredits int64 `json:"orphaned_ledger_credits"`
	MissingEventsForUnits int64 `json:"missing_events_for_committed_units"`
	BlockedRows           int64 `json:"blocked_rows"`
	OpenQuarantine        int64 `json:"open_quarantine"`
	OutboxTotal           int64 `json:"outbox_total"`
	SubmittedRequests     int64 `json:"submitted_requests"`
	SubmittedObservations int64 `json:"submitted_observations"`
}

// AssertInvariants fails the drill when any FR-06 zero-invariant is violated.
func AssertInvariants(t *testing.T, report InvariantReport) {
	t.Helper()
	if report.DuplicateIntents != 0 {
		t.Fatalf("duplicate withdrawal intents = %d, want 0", report.DuplicateIntents)
	}
	if report.DuplicateNonces != 0 {
		t.Fatalf("duplicate nonce allocations = %d, want 0", report.DuplicateNonces)
	}
	if report.DuplicateSendSlots != 0 {
		t.Fatalf("duplicate broadcast/send slots = %d, want 0", report.DuplicateSendSlots)
	}
	if report.OrphanedLedgerCredits != 0 {
		t.Fatalf("orphaned deposits credited by the reference consumer = %d, want 0", report.OrphanedLedgerCredits)
	}
	if report.MissingEventsForUnits != 0 {
		t.Fatalf("committed units without their event = %d, want 0 (silent event loss)", report.MissingEventsForUnits)
	}
	if report.BlockedRows != 0 {
		t.Fatalf("blocked outbox rows = %d, want 0", report.BlockedRows)
	}
	if report.OpenQuarantine != 0 {
		t.Fatalf("open quarantine rows = %d, want 0", report.OpenQuarantine)
	}
}

// --- T019: persistence-failure primitive -------------------------------------

// OutboxWriteLock holds an ACCESS EXCLUSIVE lock on outbox_events: a real
// "persistence is not safe" condition for the T090 capacity pause drill. The
// 003/004 write transactions carry their own 5s statement timeout, so a write
// attempted while the lock is held fails as a real persistence error instead
// of waiting forever. The lock is released explicitly (or when the connection
// drops).
type OutboxWriteLock struct {
	conn      *pgx.Conn
	release   sync.Once
	releaseFn func()
}

// LockOutboxWrites opens a dedicated connection and holds an ACCESS EXCLUSIVE
// lock on outbox_events until Release.
func (e *Env) LockOutboxWrites(ctx context.Context) (*OutboxWriteLock, error) {
	conn, err := pgx.Connect(ctx, e.DSN)
	if err != nil {
		return nil, fmt.Errorf("lock outbox writes: connect: %w", err)
	}
	if _, err := conn.Exec(ctx, "BEGIN"); err != nil {
		_ = conn.Close(ctx)
		return nil, fmt.Errorf("lock outbox writes: begin: %w", err)
	}
	if _, err := conn.Exec(ctx, "LOCK TABLE outbox_events IN ACCESS EXCLUSIVE MODE"); err != nil {
		_ = conn.Close(ctx)
		return nil, fmt.Errorf("lock outbox writes: lock: %w", err)
	}
	lock := &OutboxWriteLock{conn: conn}
	lock.releaseFn = func() {
		bg := context.Background()
		_, _ = conn.Exec(bg, "ROLLBACK")
		_ = conn.Close(bg)
	}
	return lock, nil
}

// Release drops the lock (idempotent).
func (l *OutboxWriteLock) Release() {
	if l == nil {
		return
	}
	l.release.Do(l.releaseFn)
}

// TerminateBlockedOutboxWriter waits for the first backend blocked on a lock
// while writing outbox_events (the 003/004 persistence failure) and terminates
// it, so the in-flight commit fails immediately instead of waiting out its own
// statement timeout. It uses a dedicated connection (the shared pool's
// connections are themselves blocked on the held lock). Returns the terminated
// backend pid; an error when no writer appeared within the window.
func (e *Env) TerminateBlockedOutboxWriter(ctx context.Context, timeout time.Duration, debug func(string)) (int32, error) {
	watcher, err := pgx.Connect(ctx, e.DSN)
	if err != nil {
		return 0, fmt.Errorf("blocked-writer watcher connect: %w", err)
	}
	defer func() { _ = watcher.Close(context.Background()) }()

	deadline := time.Now().Add(timeout)
	lastDebug := time.Now()
	var pid int32
	for {
		err := watcher.QueryRow(ctx, `
SELECT pid FROM pg_stat_activity
WHERE datname = current_database()
  AND wait_event_type = 'Lock'
  AND (query ILIKE '%aggregate_version%' OR query ILIKE '%INSERT INTO outbox_events%')
ORDER BY query_start
LIMIT 1`).Scan(&pid)
		if err == nil && pid != 0 {
			break
		}
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return 0, fmt.Errorf("find blocked outbox writer: %w", err)
		}
		if debug != nil && time.Since(lastDebug) >= 3*time.Second {
			lastDebug = time.Now()
			debug(e.describeWaiters(ctx, watcher))
		}
		if time.Now().After(deadline) {
			return 0, fmt.Errorf("no outbox writer blocked on the lock within %s%s", timeout, e.describeWaiters(ctx, watcher))
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(25 * time.Millisecond):
		}
	}
	var terminated bool
	if err := watcher.QueryRow(ctx, `SELECT pg_terminate_backend($1)`, pid).Scan(&terminated); err != nil {
		return 0, fmt.Errorf("terminate backend %d: %w", pid, err)
	}
	if !terminated {
		return 0, fmt.Errorf("pg_terminate_backend(%d) returned false", pid)
	}
	return pid, nil
}

// describeWaiters renders the current lock waiters/writers for diagnostics.
func (e *Env) describeWaiters(ctx context.Context, watcher *pgx.Conn) string {
	if watcher == nil {
		return ""
	}
	rows, err := watcher.Query(ctx, `
SELECT pid, state, coalesce(wait_event_type, ''), coalesce(wait_event, ''), left(query, 90)
FROM pg_stat_activity WHERE datname = current_database() ORDER BY pid`)
	if err != nil {
		return " (waiter dump failed: " + err.Error() + ")"
	}
	defer rows.Close()
	var b strings.Builder
	for rows.Next() {
		var pid int32
		var state, waitType, waitEvent, query string
		if err := rows.Scan(&pid, &state, &waitType, &waitEvent, &query); err != nil {
			continue
		}
		if waitType != "" || strings.Contains(query, "outbox") || strings.Contains(query, "indexer_lease") {
			fmt.Fprintf(&b, "\n  pid=%d state=%s wait=%s/%s query=%q", pid, state, waitType, waitEvent, query)
		}
	}
	return b.String()
}
