//go:build integration_kafka

// consumer_kafka_integration_test.go is the T050 Integration-Kafka layer:
// duplicate and out-of-order delivery over a real broker, a real consumer
// group rebalance across two members, observable lag, and the rule that a
// processed event never grants a send permission (the upstream 007/008/009/
// 010/011 tables gain no row). The effective application count is measured on
// the consumer's PostgreSQL effect: exactly 1 per event (SC-04).
//
// Statement discipline: delivery is at-least-once and processing is
// idempotent. PostgreSQL and Kafka share no transaction; this layer never
// claims cross-system exactly-once.
package events

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/xtianxx/txharbor/internal/testutil"
)

// kafkaConsumerEffectRows counts the reference consumer's ledger rows for
// one event: exactly 1 is the "effective application = 1" evidence (SC-04).
func kafkaConsumerEffectRows(t *testing.T, pool *pgxpool.Pool, eventID uuid.UUID) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(context.Background(), `
SELECT count(*) FROM `+RefLedgerTable+` WHERE consumer_name = $1 AND event_id = $2`,
		RefConsumerName, eventID).Scan(&n); err != nil {
		t.Fatalf("count reference ledger rows: %v", err)
	}
	return n
}

// kafkaConsumerObserver counts the consumer observations of this layer.
type kafkaConsumerObserver struct {
	mu          sync.Mutex
	applied     int
	quarantines []string
	lagCalls    int
}

func newKafkaConsumerObserver() *kafkaConsumerObserver { return &kafkaConsumerObserver{} }

func (o *kafkaConsumerObserver) ObserveConsumerApplied() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.applied++
}

func (o *kafkaConsumerObserver) ObserveConsumerRetry(string) {}

func (o *kafkaConsumerObserver) ObserveConsumerQuarantine(failureClass string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.quarantines = append(o.quarantines, failureClass)
}

func (o *kafkaConsumerObserver) SetConsumerLag(string, int, float64, int64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.lagCalls++
}

func (o *kafkaConsumerObserver) ObserveEventReplay(string, string) {}

func (o *kafkaConsumerObserver) SetConsumerReplayClock(string, float64) {}

func (o *kafkaConsumerObserver) lagCallCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.lagCalls
}

func (o *kafkaConsumerObserver) appliedCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.applied
}

// kafkaConsumerStatusChangedEvent builds one business_object event for a
// deposit observation aggregate (the integration-tagged builders are not
// compiled under this tag).
func kafkaConsumerStatusChangedEvent(t *testing.T, observationID, toState string) Event {
	t.Helper()
	ev, err := NewEvent(Event{
		EventType:     EventTypeDepositObservationStatusChanged,
		SchemaVersion: SchemaVersionV1,
		IdentityKind:  IdentityKindBusinessObject,
		AggregateType: "deposit_observation",
		AggregateID:   observationID,
		Payload: map[string]any{
			"from_state": "pending",
			"to_state":   toState,
			"reason":     "t050",
		},
		OccurredAt: time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("build status_changed event: %v", err)
	}
	return ev
}

// kafkaConsumerWithdrawalReceivedEvent builds one withdrawal.request.received
// event.
func kafkaConsumerWithdrawalReceivedEvent(t *testing.T, requestID string) Event {
	t.Helper()
	ev, err := NewEvent(Event{
		EventType:     EventTypeWithdrawalRequestReceived,
		SchemaVersion: SchemaVersionV1,
		IdentityKind:  IdentityKindBusinessObject,
		AggregateType: "withdrawal_request",
		AggregateID:   requestID,
		Payload: map[string]any{
			"request_id": requestID,
			"caller":     1,
			"state":      "accepted",
		},
		OccurredAt: time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("build withdrawal.request.received: %v", err)
	}
	return ev
}

// kafkaConsumerExecutionEvent builds one
// withdrawal.execution.state_changed event.
func kafkaConsumerExecutionEvent(t *testing.T, intentID, from, to string) Event {
	t.Helper()
	ev, err := NewEvent(Event{
		EventType:     EventTypeWithdrawalExecutionStateChanged,
		SchemaVersion: SchemaVersionV1,
		IdentityKind:  IdentityKindBusinessObject,
		AggregateType: "withdrawal_intent",
		AggregateID:   intentID,
		Payload: map[string]any{
			"from_state": from,
			"to_state":   to,
			"intent_id":  intentID,
		},
		OccurredAt: time.Date(2026, 9, 24, 12, 0, 1, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("build withdrawal.execution.state_changed: %v", err)
	}
	return ev
}

// appendKafkaConsumerEvent appends one event through the real Append path.
func appendKafkaConsumerEvent(t *testing.T, pool *pgxpool.Pool, ev Event) uuid.UUID {
	t.Helper()
	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin append: %v", err)
	}
	result, err := Append(context.Background(), tx, ev)
	if err != nil {
		_ = tx.Rollback(context.Background())
		t.Fatalf("Append(%s): %v", ev.EventType, err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatalf("commit append: %v", err)
	}
	return result.EventID
}

// kafkaConsumerEnvelope renders the exact transport bytes of one outbox event.
func kafkaConsumerEnvelope(t *testing.T, pool *pgxpool.Pool, eventID uuid.UUID) []byte {
	t.Helper()
	msg, err := outboxReplayMessage(context.Background(), pool, eventID, testutil.KafkaTopic)
	if err != nil {
		t.Fatalf("render envelope: %v", err)
	}
	if msg == nil {
		t.Fatalf("outbox row for %s not found", eventID)
	}
	return msg.Value
}

// newKafkaConsumerRuntime builds the group runtime over the reference
// consumer (the acceptance-evidence consumer; contracts/consumer.md §8). Its
// simulated ledger is the PG effect the exactly-once assertion counts. The
// consumer name is ignored for the processor (the reference consumer carries
// its own independent name) and only documents the layer.
func newKafkaConsumerRuntime(t *testing.T, pool *pgxpool.Pool, brokers []string, consumerName string,
	observer ConsumerObserver, batch int) *KafkaConsumer {
	t.Helper()
	_ = consumerName
	reference, err := NewReferenceConsumer(pool, ConsumerOptions{
		GapWait:     500 * time.Millisecond,
		BackoffBase: 10 * time.Millisecond,
		BackoffMax:  100 * time.Millisecond,
		RetryLimit:  4,
		ChainID:     1,
		Jitter:      func() float64 { return 0 },
		Observer:    observer,
	})
	if err != nil {
		t.Fatalf("NewReferenceConsumer: %v", err)
	}
	if err := reference.EnsureLedgerSchema(context.Background()); err != nil {
		t.Fatalf("EnsureLedgerSchema: %v", err)
	}
	runtime, err := NewKafkaConsumer(KafkaConsumerConfig{
		Brokers:        brokers,
		Topic:          testutil.KafkaTopic,
		GroupPrefix:    "txharbor",
		ConsumerName:   RefConsumerName,
		PollBatch:      batch,
		CommitInterval: 200 * time.Millisecond,
	}, reference.Consumer)
	if err != nil {
		t.Fatalf("NewKafkaConsumer: %v", err)
	}
	return runtime
}

// waitForKafkaConsumerEffect waits until one event has exactly want effect
// rows.
func waitForKafkaConsumerEffect(t *testing.T, pool *pgxpool.Pool, eventID uuid.UUID, want int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last int64
	for time.Now().Before(deadline) {
		last = kafkaConsumerEffectRows(t, pool, eventID)
		if last == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("event %s effect rows = %d, want %d within %s", eventID, last, want, timeout)
}

// startKafkaConsumerBroker boots the shared Kafka helper for this layer.
func startKafkaConsumerBroker(t *testing.T) *testutil.Kafka {
	t.Helper()
	kafka, err := testutil.StartKafka(context.Background())
	if err != nil {
		t.Fatalf("start kafka: %v", err)
	}
	t.Cleanup(func() { _ = kafka.Close(context.Background()) })
	return kafka
}

// TestConsumerKafkaDuplicateOutOfOrderRebalanceAndLag is the T050 acceptance:
// a real broker with duplicate and out-of-order delivery, a real two-member
// consumer group rebalance, lag observation, and exactly-once PostgreSQL
// effects per event.
func TestConsumerKafkaDuplicateOutOfOrderRebalanceAndLag(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	kafka := startKafkaConsumerBroker(t)
	if err := kafka.EnsureTopic(ctx, testutil.KafkaTopic, testutil.KafkaPartitions); err != nil {
		t.Fatalf("ensure topic: %v", err)
	}
	pool := openKafkaPool(t, startKafkaPostgres(t))

	producer, err := kgo.NewClient(kgo.SeedBrokers(kafka.Brokers()...), kgo.DefaultProduceTopic(testutil.KafkaTopic))
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	defer producer.Close()
	produce := func(t *testing.T, key string, value []byte) {
		t.Helper()
		if err := producer.ProduceSync(ctx, &kgo.Record{Key: []byte(key), Value: value}).FirstErr(); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}

	// Duplicate delivery of one event and an out-of-order pair (v2 before v1)
	// for a second aggregate.
	dupID := appendKafkaConsumerEvent(t, pool, kafkaConsumerStatusChangedEvent(t, "obs-t050-dup", "confirmed"))
	oooV1 := appendKafkaConsumerEvent(t, pool, kafkaConsumerStatusChangedEvent(t, "obs-t050-ooo", "pending"))
	oooV2 := appendKafkaConsumerEvent(t, pool, kafkaConsumerStatusChangedEvent(t, "obs-t050-ooo", "confirmed"))
	key := "deposit_observation:obs-t050"
	produce(t, key, kafkaConsumerEnvelope(t, pool, dupID))
	produce(t, key, kafkaConsumerEnvelope(t, pool, dupID)) // duplicate delivery
	produce(t, key, kafkaConsumerEnvelope(t, pool, oooV2))
	produce(t, key, kafkaConsumerEnvelope(t, pool, oooV1)) // late old version

	observer := newKafkaConsumerObserver()
	first := newKafkaConsumerRuntime(t, pool, kafka.Brokers(), "t050-consumer", observer, 10)
	firstCtx, stopFirst := context.WithCancel(ctx)
	firstDone := make(chan error, 1)
	go func() { firstDone <- first.Run(firstCtx) }()

	waitForKafkaConsumerEffect(t, pool, dupID, 1, 60*time.Second)
	waitForKafkaConsumerEffect(t, pool, oooV2, 1, 60*time.Second)
	// Give the consumer a moment to also process the duplicate and the old
	// version (both must be no-ops).
	time.Sleep(2 * time.Second)
	if n := kafkaConsumerEffectRows(t, pool, dupID); n != 1 {
		t.Fatalf("duplicate delivery effect rows = %d, want 1", n)
	}
	if n := kafkaConsumerEffectRows(t, pool, oooV1); n != 0 {
		t.Fatalf("old-version effect rows = %d, want 0 (never overwrite the newer state)", n)
	}
	var inboxDup int64
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM consumer_inbox WHERE consumer_name = $1 AND event_id = $2`,
		RefConsumerName, dupID).Scan(&inboxDup); err != nil {
		t.Fatalf("count inbox: %v", err)
	}
	if inboxDup != 1 {
		t.Fatalf("duplicate inbox rows = %d, want 1", inboxDup)
	}

	// A second member joins the group: a real rebalance. New events must be
	// processed by whichever member owns the partition, still exactly once.
	second := newKafkaConsumerRuntime(t, pool, kafka.Brokers(), "t050-consumer", observer, 10)
	secondCtx, stopSecond := context.WithCancel(ctx)
	secondDone := make(chan error, 1)
	go func() { secondDone <- second.Run(secondCtx) }()
	time.Sleep(4 * time.Second) // let the rebalance settle

	rebalanced := appendKafkaConsumerEvent(t, pool, kafkaConsumerStatusChangedEvent(t, "obs-t050-rebalance", "confirmed"))
	produce(t, "deposit_observation:obs-t050-rebalance", kafkaConsumerEnvelope(t, pool, rebalanced))
	waitForKafkaConsumerEffect(t, pool, rebalanced, 1, 60*time.Second)

	// The first member leaves: another rebalance; the surviving member keeps
	// consuming from the durable PostgreSQL progress.
	stopFirst()
	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatalf("first consumer stopped with %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("first consumer did not stop")
	}
	afterLeave := appendKafkaConsumerEvent(t, pool, kafkaConsumerStatusChangedEvent(t, "obs-t050-leave", "confirmed"))
	produce(t, "deposit_observation:obs-t050-leave", kafkaConsumerEnvelope(t, pool, afterLeave))
	waitForKafkaConsumerEffect(t, pool, afterLeave, 1, 60*time.Second)

	// Lag is observable through the broker (the consumer group lag query) and
	// through the observer.
	time.Sleep(time.Second)
	lags, err := second.Lag(ctx)
	if err != nil {
		t.Fatalf("Lag: %v", err)
	}
	for partition, lag := range lags {
		if lag < 0 {
			t.Fatalf("partition %d lag = %d, want >= 0", partition, lag)
		}
	}
	if observer.lagCallCount() == 0 {
		t.Fatal("the observer never observed lag")
	}
	if observer.appliedCount() < 4 {
		t.Fatalf("applied observations = %d, want >= 4", observer.appliedCount())
	}
	if len(observer.quarantines) != 0 {
		t.Fatalf("quarantines = %v, want none", observer.quarantines)
	}
	stopSecond()
	select {
	case err := <-secondDone:
		if err != nil {
			t.Fatalf("second consumer stopped with %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("second consumer did not stop")
	}
}

// TestConsumerKafkaProcessedEventIsNotASendPermission covers the statement
// rule: consuming a withdrawal request / execution event never creates an
// intent, nonce, signature or broadcast. The upstream authority tables are
// counted before and after a real broker round trip.
func TestConsumerKafkaProcessedEventIsNotASendPermission(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	kafka := startKafkaConsumerBroker(t)
	if err := kafka.EnsureTopic(ctx, testutil.KafkaTopic, testutil.KafkaPartitions); err != nil {
		t.Fatalf("ensure topic: %v", err)
	}
	pool := openKafkaPool(t, startKafkaPostgres(t))

	producer, err := kgo.NewClient(kgo.SeedBrokers(kafka.Brokers()...), kgo.DefaultProduceTopic(testutil.KafkaTopic))
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	defer producer.Close()

	counts := func() map[string]int64 {
		t.Helper()
		out := map[string]int64{}
		for _, table := range []string{"withdrawal_requests", "payment_intents", "nonce_bindings", "tx_attempts", "tx_attempt_signings", "tx_send_attempts"} {
			var n int64
			if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&n); err != nil {
				t.Fatalf("count %s: %v", table, err)
			}
			out[table] = n
		}
		return out
	}
	before := counts()

	requestID := appendKafkaConsumerEvent(t, pool, kafkaConsumerWithdrawalReceivedEvent(t, "req-t050"))
	executionID := appendKafkaConsumerEvent(t, pool, kafkaConsumerExecutionEvent(t, "intent-t050", "executing", "completed"))
	produce := func(id uuid.UUID) {
		t.Helper()
		if err := producer.ProduceSync(ctx, &kgo.Record{Key: []byte("withdrawal_request:req-t050"), Value: kafkaConsumerEnvelope(t, pool, id)}).FirstErr(); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}
	produce(requestID)
	produce(executionID)

	observer := newKafkaConsumerObserver()
	runtime := newKafkaConsumerRuntime(t, pool, kafka.Brokers(), "t050-boundary", observer, 10)
	runtimeCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- runtime.Run(runtimeCtx) }()

	waitForKafkaConsumerEffect(t, pool, requestID, 1, 60*time.Second)
	waitForKafkaConsumerEffect(t, pool, executionID, 1, 60*time.Second)
	time.Sleep(time.Second)

	after := counts()
	for table, want := range before {
		if after[table] != want {
			t.Fatalf("%s rows changed after consuming the events: %d -> %d (a processed event is not a send permission)",
				table, want, after[table])
		}
	}
	stop()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("consumer stopped with %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("consumer did not stop")
	}
}

// TestKafkaConsumerRuntimeValidatesBounds pins the fail-closed runtime
// constructor.
func TestKafkaConsumerRuntimeValidatesBounds(t *testing.T) {
	processor := &Consumer{Name: "c"}
	cases := []KafkaConsumerConfig{
		{Brokers: nil, Topic: "t", ConsumerName: "c", PollBatch: 1, CommitInterval: time.Second},
		{Brokers: []string{"127.0.0.1:1"}, Topic: "", ConsumerName: "c", PollBatch: 1, CommitInterval: time.Second},
		{Brokers: []string{"127.0.0.1:1"}, Topic: "t", ConsumerName: "", PollBatch: 1, CommitInterval: time.Second},
		{Brokers: []string{"127.0.0.1:1"}, Topic: "t", ConsumerName: "c", PollBatch: 0, CommitInterval: time.Second},
		{Brokers: []string{"127.0.0.1:1"}, Topic: "t", ConsumerName: "c", PollBatch: 1, CommitInterval: 0},
	}
	for i, cfg := range cases {
		if _, err := NewKafkaConsumer(cfg, processor); err == nil {
			t.Fatalf("invalid kafka config case %d accepted", i)
		}
	}
	if _, err := NewKafkaConsumer(KafkaConsumerConfig{
		Brokers: []string{"127.0.0.1:1"}, Topic: "t", ConsumerName: "c", PollBatch: 1, CommitInterval: time.Second,
	}, nil); err == nil {
		t.Fatal("nil consumer processor accepted")
	}
}
