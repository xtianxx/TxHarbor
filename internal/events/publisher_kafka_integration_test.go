//go:build integration_kafka

// publisher_kafka_integration_test.go is the T037 Integration-Kafka layer
// (V-PUBLISHER, Kafka half): delivery acknowledgement against a real broker,
// the explicit topic creation (auto-create disabled), duplicate publishes with
// identical bytes (absorbable by the consumer's persistent idempotency; the
// consumer itself lands in T041–T051), multi-instance claim/publish and the
// stop -> recover -> drain path with bounded batches. It runs through
// `make test-integration-kafka`.
//
// Statement discipline: this layer never claims a cross-system delivery
// guarantee; delivery is at-least-once and processing is idempotent.
package events

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/testutil"
)

// startKafkaPostgres boots a real PostgreSQL container with the embedded
// migrations applied (the integration-tagged helper is not compiled under this
// tag). Skips (never passes) when no Docker provider is available.
func startKafkaPostgres(t *testing.T) string {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()
	ctr, err := postgres.Run(ctx, "postgres:18.6-trixie",
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

func openKafkaPool(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// appendKafkaEvent emits one committed deposit observation event with a unique
// log identity.
func appendKafkaEvent(t *testing.T, pool *pgxpool.Pool, n int) (string, int64) {
	t.Helper()
	ctx := context.Background()
	obsID := fmt.Sprintf("t037-obs-%d", n)
	ev, err := NewEvent(Event{
		EventType:     EventTypeDepositObservationCreated,
		SchemaVersion: SchemaVersionV1,
		IdentityKind:  IdentityKindEVMLog,
		AggregateType: "deposit_observation",
		AggregateID:   obsID,
		Payload: map[string]any{
			"observation_id": obsID,
			"state":          "pending",
		},
		OccurredAt:  time.Now().UTC(),
		ChainID:     31337,
		BlockNumber: 100,
		BlockHash:   fmt.Sprintf("0x%064x", 0x1000+n),
		TxHash:      fmt.Sprintf("0x%064x", 0x2000+n),
		LogIndex:    n,
	})
	if err != nil {
		t.Fatalf("build event: %v", err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	res, err := Append(ctx, tx, ev)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	return obsID, res.OutboxID
}

// kafkaOutboxRow is the observable state of one outbox row under this tag
// (the integration-tagged helper is not compiled here).
type kafkaOutboxRow struct {
	publishState string
	attemptCount int
}

func readKafkaOutboxRow(t *testing.T, pool *pgxpool.Pool, id int64) kafkaOutboxRow {
	t.Helper()
	var row kafkaOutboxRow
	if err := pool.QueryRow(context.Background(),
		`SELECT publish_state, attempt_count FROM outbox_events WHERE id = $1`, id).
		Scan(&row.publishState, &row.attemptCount); err != nil {
		t.Fatalf("read outbox row %d: %v", id, err)
	}
	return row
}

// kafkaTestSink builds the real sink with test-sized bounded timeouts.
func kafkaTestSink(t *testing.T, brokers []string, topic string) *KafkaSink {
	t.Helper()
	sink, err := NewKafkaSink(KafkaSinkConfig{
		Brokers:         brokers,
		Topic:           topic,
		DeliveryTimeout: 3 * time.Second,
		RequestTimeout:  time.Second,
		MaxInflight:     5,
		MaxBuffered:     64,
	})
	if err != nil {
		t.Fatalf("NewKafkaSink: %v", err)
	}
	t.Cleanup(sink.Close)
	return sink
}

func kafkaTestPublisher(t *testing.T, pool *pgxpool.Pool, sink PublishSink, owner string, batch int, lease time.Duration) *Publisher {
	t.Helper()
	pub, err := NewPublisher(pool, sink, owner, PublisherOptions{
		Batch:       batch,
		LeaseTTL:    lease,
		BackoffBase: time.Second,
		BackoffMax:  time.Minute,
		Jitter:      func() float64 { return 0 },
	})
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	return pub
}

// consumeByEventID consumes from the start of the topic until every wanted
// event id was seen at least want times (bounded by a deadline) and returns
// the records per event id. Records of unrelated event ids are ignored.
func consumeByEventID(ctx context.Context, t *testing.T, brokers []string, topic string, want map[string]int) map[string][]*kgo.Record {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("kafka consumer: %v", err)
	}
	defer cl.Close()

	got := map[string][]*kgo.Record{}
	deadline := time.Now().Add(45 * time.Second)
	for {
		satisfied := true
		for id, n := range want {
			if len(got[id]) < n {
				satisfied = false
				break
			}
		}
		if satisfied {
			return got
		}
		if time.Now().After(deadline) {
			counts := map[string]int{}
			for id, recs := range got {
				counts[id] = len(recs)
			}
			t.Fatalf("timed out consuming: got %v, want %v", counts, want)
		}
		fetchCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		fetches := cl.PollFetches(fetchCtx)
		cancel()
		for _, fe := range fetches.Errors() {
			if fe.Err != context.DeadlineExceeded {
				t.Fatalf("PollFetches: %v", fe.Err)
			}
		}
		fetches.EachRecord(func(rec *kgo.Record) {
			for _, h := range rec.Headers {
				if h.Key == "event_id" {
					got[string(h.Value)] = append(got[string(h.Value)], rec)
				}
			}
		})
	}
}

// drainPublisher runs bounded publish cycles until the pending set is empty
// or the deadline passes: rows inside their retry backoff window are waited
// out, exactly like the long-running loop's poll cadence would.
func drainPublisher(ctx context.Context, t *testing.T, pub *Publisher, pool *pgxpool.Pool) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		var pending int64
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM outbox_events WHERE publish_state = 'pending'`).Scan(&pending); err != nil {
			t.Fatalf("count pending: %v", err)
		}
		if pending == 0 {
			return
		}
		outcome, err := pub.PublishOnce(ctx)
		if err != nil {
			t.Fatalf("PublishOnce: %v", err)
		}
		t.Logf("drain cycle: pending=%d claimed=%d acked=%d released=%d blocked=%d",
			pending, outcome.Claimed, outcome.Acked, outcome.Released, outcome.Blocked)
		if outcome.Claimed == 0 {
			time.Sleep(500 * time.Millisecond)
		}
	}
	t.Fatal("publisher did not drain within the deadline")
}

// TestPublisherKafkaRuntime covers the real-broker runtime: envelope delivery,
// duplicate publishes with identical bytes, multi-instance claim/publish and
// the explicit topic creation.
func TestPublisherKafkaRuntime(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	k, err := testutil.StartKafka(ctx)
	if err != nil {
		t.Fatalf("StartKafka: %v", err)
	}
	t.Cleanup(func() { _ = k.Close(context.Background()) })
	if err := k.EnsureTopic(ctx, testutil.KafkaTopic, testutil.KafkaPartitions); err != nil {
		t.Fatalf("EnsureTopic: %v", err)
	}
	pool := openKafkaPool(t, startKafkaPostgres(t))
	sink := kafkaTestSink(t, k.Brokers(), testutil.KafkaTopic)

	t.Run("topic_explicit_creation", func(t *testing.T) {
		// Idempotent explicit creation: no auto-create anywhere.
		if err := sink.EnsureTopic(ctx, testutil.KafkaPartitions); err != nil {
			t.Fatalf("EnsureTopic (second, idempotent): %v", err)
		}
		detail, err := k.TopicDetail(ctx, testutil.KafkaTopic)
		if err != nil || detail.Err != nil || len(detail.Partitions) != int(testutil.KafkaPartitions) {
			t.Fatalf("topic detail = %+v, err = %v", detail, err)
		}
		unknown := "txharbor.unknown." + fmt.Sprint(time.Now().UnixNano())
		missing, err := k.TopicDetail(ctx, unknown)
		if err != nil {
			t.Fatalf("TopicDetail(unknown): %v", err)
		}
		if missing.Err == nil {
			t.Fatalf("unknown topic %q reported healthy; auto-create must be disabled", unknown)
		}
	})

	t.Run("delivers_envelope", func(t *testing.T) {
		obsID, outboxID := appendKafkaEvent(t, pool, 1)
		eventID := readOutboxEventID(t, pool, outboxID)
		pub := kafkaTestPublisher(t, pool, sink, "owner-k1", 10, 30*time.Second)
		outcome, err := pub.PublishOnce(ctx)
		if err != nil || outcome.Acked != 1 {
			t.Fatalf("PublishOnce = %+v, err = %v", outcome, err)
		}
		if got := readKafkaOutboxRow(t, pool, outboxID); got.publishState != string(PublishStatePublished) {
			t.Fatalf("row state = %s, want published", got.publishState)
		}
		records := consumeByEventID(ctx, t, k.Brokers(), testutil.KafkaTopic, map[string]int{eventID: 1})
		rec := records[eventID][0]
		if string(rec.Key) != "deposit_observation:"+obsID {
			t.Fatalf("record key = %q, want the aggregate partition key", rec.Key)
		}
		headers := map[string]string{}
		for _, h := range rec.Headers {
			headers[h.Key] = string(h.Value)
		}
		if headers["event_id"] != eventID || headers["event_type"] != EventTypeDepositObservationCreated ||
			headers["schema_version"] != "1" {
			t.Fatalf("record headers = %v", headers)
		}
		var env map[string]any
		if err := json.Unmarshal(rec.Value, &env); err != nil {
			t.Fatalf("record value is not an envelope: %v", err)
		}
		if env["event_id"] != eventID || env["event_type"] != EventTypeDepositObservationCreated ||
			env["schema_version"] != float64(1) || env["identity_kind"] != string(IdentityKindEVMLog) ||
			env["aggregate_version"] != float64(1) {
			t.Fatalf("envelope = %v", env)
		}
		payload, ok := env["payload"].(map[string]any)
		if !ok || payload["observation_id"] != obsID {
			t.Fatalf("envelope payload = %v", env["payload"])
		}
	})

	t.Run("duplicate_publish_is_absorbable", func(t *testing.T) {
		_, outboxID := appendKafkaEvent(t, pool, 2)
		eventID := readOutboxEventID(t, pool, outboxID)
		// First publish, then a simulated crash before the ack mark: the lease
		// expires and a second instance publishes the same identity again.
		first := kafkaTestPublisher(t, pool, sink, "owner-dup-1", 10, 300*time.Millisecond)
		claimed, err := first.ClaimBatch(ctx)
		if err != nil || len(claimed) != 1 {
			t.Fatalf("first claim = %d rows, err = %v", len(claimed), err)
		}
		if res := first.Sink.Publish(ctx, claimed); len(res.Acked) != 1 {
			t.Fatalf("first publish acked = %d, want 1", len(res.Acked))
		}
		time.Sleep(400 * time.Millisecond)
		second := kafkaTestPublisher(t, pool, sink, "owner-dup-2", 10, 30*time.Second)
		if _, err := second.PublishOnce(ctx); err != nil {
			t.Fatalf("second PublishOnce: %v", err)
		}
		got := readKafkaOutboxRow(t, pool, outboxID)
		if got.publishState != string(PublishStatePublished) || got.attemptCount != 2 {
			t.Fatalf("row after duplicate recovery = %+v", got)
		}
		// The broker carries both publishes; they are byte-identical, so the
		// consumer's persistent idempotency absorbs the duplicate.
		records := consumeByEventID(ctx, t, k.Brokers(), testutil.KafkaTopic, map[string]int{eventID: 2})
		recs := records[eventID]
		if len(recs) < 2 {
			t.Fatalf("duplicate records = %d, want >= 2", len(recs))
		}
		if string(recs[0].Value) != string(recs[1].Value) {
			t.Fatal("duplicate publishes carry different bytes; they would not be absorbable")
		}
	})

	t.Run("multi_instance_drain", func(t *testing.T) {
		const events = 8
		want := map[string]int{}
		for i := 0; i < events; i++ {
			_, outboxID := appendKafkaEvent(t, pool, 100+i)
			want[readOutboxEventID(t, pool, outboxID)] = 1
		}
		pubA := kafkaTestPublisher(t, pool, sink, "owner-multi-a", 3, 30*time.Second)
		pubB := kafkaTestPublisher(t, pool, sink, "owner-multi-b", 3, 30*time.Second)
		for i := 0; i < 100; i++ {
			var wg sync.WaitGroup
			var claimed int32
			for _, pub := range []*Publisher{pubA, pubB} {
				wg.Add(1)
				go func(pub *Publisher) {
					defer wg.Done()
					outcome, err := pub.PublishOnce(ctx)
					if err != nil {
						t.Errorf("PublishOnce: %v", err)
						return
					}
					atomic.AddInt32(&claimed, int32(outcome.Claimed))
				}(pub)
			}
			wg.Wait()
			if atomic.LoadInt32(&claimed) == 0 {
				break
			}
		}
		// 0 loss: every event row is published.
		var published, pending int64
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FILTER (WHERE publish_state = 'published'),
			       count(*) FILTER (WHERE publish_state <> 'published')
			FROM outbox_events WHERE event_type = $1`, EventTypeDepositObservationCreated).
			Scan(&published, &pending); err != nil {
			t.Fatalf("count rows: %v", err)
		}
		if pending != 0 || published < events {
			t.Fatalf("published = %d, pending = %d, want >= %d and 0", published, pending, events)
		}
		// Every event id reached the broker at least once (duplicates allowed).
		consumeByEventID(ctx, t, k.Brokers(), testutil.KafkaTopic, want)
	})
}

// TestPublisherKafkaOutageRecoveryDrains covers the stop -> recover -> drain
// path: during the outage nothing is lost (rows stay pending with a bounded
// backoff) and after recovery the publisher drains from the persistent pending
// set with bounded batches.
func TestPublisherKafkaOutageRecoveryDrains(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	k, err := testutil.StartKafka(ctx)
	if err != nil {
		t.Fatalf("StartKafka: %v", err)
	}
	t.Cleanup(func() { _ = k.Close(context.Background()) })
	if err := k.EnsureTopic(ctx, testutil.KafkaTopic, testutil.KafkaPartitions); err != nil {
		t.Fatalf("EnsureTopic: %v", err)
	}
	pool := openKafkaPool(t, startKafkaPostgres(t))
	sink := kafkaTestSink(t, k.Brokers(), testutil.KafkaTopic)

	const events = 3
	ids := map[string]int{}
	for i := 0; i < events; i++ {
		_, outboxID := appendKafkaEvent(t, pool, 200+i)
		ids[readOutboxEventID(t, pool, outboxID)] = 1
	}
	pub := kafkaTestPublisher(t, pool, sink, "owner-outage", events, 30*time.Second)

	// Outage: every publish fails transiently; rows stay pending with a
	// bounded backoff and 0 are published.
	if err := k.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	outcome, err := pub.PublishOnce(ctx)
	if err != nil {
		t.Fatalf("PublishOnce during outage: %v", err)
	}
	if outcome.Claimed != events || outcome.Acked != 0 || outcome.Released != events {
		t.Fatalf("outage outcome = %+v, want %d claimed/0 acked/%d released", outcome, events, events)
	}
	var published int64
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM outbox_events WHERE event_type = $1 AND publish_state = 'published'`,
		EventTypeDepositObservationCreated).Scan(&published); err != nil {
		t.Fatalf("count published: %v", err)
	}
	if published != 0 {
		t.Fatalf("published during outage = %d, want 0", published)
	}

	// Recovery: a fresh broker on the same address; the topic is re-ensured
	// explicitly (auto-create stays disabled) and the pending rows drain.
	if err := k.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := k.EnsureTopic(ctx, testutil.KafkaTopic, testutil.KafkaPartitions); err != nil {
		t.Fatalf("EnsureTopic after recovery: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE outbox_events SET next_attempt_at = now() WHERE publish_state = 'pending'`); err != nil {
		t.Fatalf("force retry: %v", err)
	}
	drainPublisher(ctx, t, pub, pool)
	var pending int64
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM outbox_events WHERE event_type = $1 AND publish_state <> 'published'`,
		EventTypeDepositObservationCreated).Scan(&pending); err != nil {
		t.Fatalf("count pending: %v", err)
	}
	if pending != 0 {
		t.Fatalf("pending after recovery = %d, want 0 (0 loss)", pending)
	}
	consumeByEventID(ctx, t, k.Brokers(), testutil.KafkaTopic, ids)
}

// readOutboxEventID returns the derived event_id of one outbox row.
func readOutboxEventID(t *testing.T, pool *pgxpool.Pool, outboxID int64) string {
	t.Helper()
	var eventID string
	if err := pool.QueryRow(context.Background(),
		`SELECT event_id FROM outbox_events WHERE id = $1`, outboxID).Scan(&eventID); err != nil {
		t.Fatalf("read event id: %v", err)
	}
	return eventID
}

// forbiddenClaimFragments are the statement fragments this layer must never
// carry (assembled from pieces so the scanner itself stays clean): the design
// provides at-least-once delivery plus idempotent processing, nothing
// stronger.
var forbiddenClaimFragments = []string{
	"跨系统" + "恰好一次",
	"端到端" + "恰好一次",
	"cross-system " + "exactly once",
	"end-to-end " + "exactly once",
}

// TestPublisherKafkaStatementDiscipline scans this test file and the publisher
// runtime sources for a forbidden delivery claim.
func TestPublisherKafkaStatementDiscipline(t *testing.T) {
	for name, path := range map[string]string{
		"publisher_kafka_integration_test.go": "publisher_kafka_integration_test.go",
		"publisher.go":                        "publisher.go",
		"audit.go":                            "audit.go",
	} {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		for _, fragment := range forbiddenClaimFragments {
			if strings.Contains(string(body), fragment) {
				t.Errorf("%s carries a forbidden delivery claim (%q)", name, fragment)
			}
		}
	}
}
