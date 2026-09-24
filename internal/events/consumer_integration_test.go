//go:build integration

// consumer_integration_test.go is the T046 Integration-PG layer
// (V-IDEMPOTENCY, PG half): duplicate delivery (including a restart with a
// fresh consumer instance and a simulated rebalance redelivery), out-of-order
// old-version arrival, the bounded version-gap wait with quarantine and
// partition continuation, and the rule that no branch is ever a silent skip.
// It runs against the real 000015 migration through `make test-integration`.
package events

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// consumerEffectTable is the scratch effect table the integration tests write
// as the consumer's business effect. It is deliberately not part of any
// migration: it only materializes "the effect ran once" for assertions.
const consumerEffectTable = "t046_consumer_effects"

func createConsumerEffectTable(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
	event_id          UUID   NOT NULL,
	aggregate_type    TEXT   NOT NULL,
	aggregate_id      TEXT   NOT NULL,
	aggregate_version BIGINT NOT NULL
)`, consumerEffectTable)); err != nil {
		t.Fatalf("create consumer effect table: %v", err)
	}
}

// consumerTestEffect is the scratch consumer effect: it records one row per
// applied event and can inject classified failures by attempt number.
type consumerTestEffect struct {
	mu       sync.Mutex
	attempts map[uuid.UUID]int
	fail     func(env Envelope, attempt int) error
}

func newConsumerTestEffect() *consumerTestEffect {
	return &consumerTestEffect{attempts: map[uuid.UUID]int{}}
}

func (e *consumerTestEffect) Apply(ctx context.Context, tx pgx.Tx, env Envelope) error {
	e.mu.Lock()
	e.attempts[env.EventID]++
	attempt := e.attempts[env.EventID]
	e.mu.Unlock()
	if e.fail != nil {
		if err := e.fail(env, attempt); err != nil {
			return err
		}
	}
	_, err := tx.Exec(ctx, fmt.Sprintf(`
INSERT INTO %s (event_id, aggregate_type, aggregate_id, aggregate_version)
VALUES ($1, $2, $3, $4)`, consumerEffectTable),
		env.EventID, env.AggregateType, env.AggregateID, env.AggregateVersion)
	return err
}

func (e *consumerTestEffect) attemptCount(eventID uuid.UUID) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.attempts[eventID]
}

// effectRows counts the scratch effect rows of one event: exactly 1 is the
// "effective application = 1" evidence (SC-04).
func effectRows(t *testing.T, pool *pgxpool.Pool, eventID uuid.UUID) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(context.Background(),
		fmt.Sprintf(`SELECT count(*) FROM %s WHERE event_id = $1`, consumerEffectTable), eventID).Scan(&n); err != nil {
		t.Fatalf("count effect rows for %s: %v", eventID, err)
	}
	return n
}

// consumerTestObserver records the consumer observations.
type consumerTestObserver struct {
	mu          sync.Mutex
	applied     int
	retries     []string
	quarantines []string
	replays     []string
	lagMessages map[int]int64
}

func newConsumerTestObserver() *consumerTestObserver {
	return &consumerTestObserver{lagMessages: map[int]int64{}}
}

func (o *consumerTestObserver) ObserveConsumerApplied() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.applied++
}

func (o *consumerTestObserver) ObserveConsumerRetry(failureClass string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.retries = append(o.retries, failureClass)
}

func (o *consumerTestObserver) ObserveConsumerQuarantine(failureClass string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.quarantines = append(o.quarantines, failureClass)
}

func (o *consumerTestObserver) SetConsumerLag(_ string, partition int, _ float64, lagMessages int64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.lagMessages[partition] = lagMessages
}

func (o *consumerTestObserver) ObserveEventReplay(opKind, _ string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.replays = append(o.replays, opKind)
}

func (o *consumerTestObserver) SetConsumerReplayClock(string, float64) {}

func (o *consumerTestObserver) snapshot() (int, int, int, int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.applied, len(o.retries), len(o.quarantines), len(o.replays)
}

// appendConsumerEvent appends one event through the real Append path and
// returns its derived event id and outbox id.
func appendConsumerEvent(t *testing.T, pool *pgxpool.Pool, ev Event) (uuid.UUID, int64) {
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
	return result.EventID, result.OutboxID
}

// wireEnvelope renders the exact transport bytes of one outbox event (the
// same materialization the publisher sends), so the consumer parses the real
// wire shape.
func wireEnvelope(t *testing.T, pool *pgxpool.Pool, eventID uuid.UUID) []byte {
	t.Helper()
	msg, err := outboxReplayMessage(context.Background(), pool, eventID, "txharbor.events.v1")
	if err != nil {
		t.Fatalf("render envelope for %s: %v", eventID, err)
	}
	if msg == nil {
		t.Fatalf("outbox row for %s not found", eventID)
	}
	return msg.Value
}

// newTestConsumer builds a consumer over the migrated pool with deterministic
// test bounds (short gap waits are calibrated values for the test, never
// business thresholds).
func newTestConsumer(t *testing.T, pool *pgxpool.Pool, name string, effect Effect,
	observer ConsumerObserver, gapWait time.Duration) *Consumer {
	t.Helper()
	consumer, err := NewConsumer(pool, name, effect, ConsumerOptions{
		GapWait:     gapWait,
		BackoffBase: 10 * time.Millisecond,
		BackoffMax:  100 * time.Millisecond,
		RetryLimit:  4,
		ChainID:     1,
		Jitter:      func() float64 { return 0 },
		Observer:    observer,
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	return consumer
}

// mustProcess runs one Process and fails the test on a transport error.
func mustProcess(t *testing.T, consumer *Consumer, msg Message) ProcessResult {
	t.Helper()
	result, err := consumer.Process(context.Background(), msg)
	if err != nil {
		t.Fatalf("Process(offset %d): %v", msg.Offset, err)
	}
	return result
}

// statusChangedConsumerEvent builds one business_object event for a deposit
// observation aggregate.
func statusChangedConsumerEvent(t *testing.T, observationID, toState string) Event {
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
			"reason":     "t046",
		},
		OccurredAt: time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("build status_changed event: %v", err)
	}
	return ev
}

// readVersions reads the consumer version high-water mark of one aggregate.
func readVersions(t *testing.T, pool *pgxpool.Pool, consumerName, aggregateType, aggregateID string) (int64, bool) {
	t.Helper()
	var maxVersion int64
	err := pool.QueryRow(context.Background(), `
SELECT max_version FROM consumer_versions
WHERE consumer_name = $1 AND aggregate_type = $2 AND aggregate_id = $3`,
		consumerName, aggregateType, aggregateID).Scan(&maxVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false
	}
	if err != nil {
		t.Fatalf("read consumer versions: %v", err)
	}
	return maxVersion, true
}

// readProgressRow reads one durable progress row.
func readProgressRow(t *testing.T, pool *pgxpool.Pool, consumerName, topic string, partition int) (int64, bool) {
	t.Helper()
	var next int64
	err := pool.QueryRow(context.Background(), `
SELECT next_offset FROM consumer_progress
WHERE consumer_name = $1 AND topic = $2 AND partition = $3`,
		consumerName, topic, partition).Scan(&next)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false
	}
	if err != nil {
		t.Fatalf("read consumer progress: %v", err)
	}
	return next, true
}

// readInboxCount counts the inbox rows of one event.
func readInboxCount(t *testing.T, pool *pgxpool.Pool, consumerName string, eventID uuid.UUID) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(context.Background(), `
SELECT count(*) FROM consumer_inbox WHERE consumer_name = $1 AND event_id = $2`,
		consumerName, eventID).Scan(&n); err != nil {
		t.Fatalf("count inbox rows: %v", err)
	}
	return n
}

// TestConsumerDuplicateDeliveryAppliesOnceAcrossRestart covers SC-04: a
// duplicate delivery, including a redelivery after a restart (a fresh
// consumer instance with the same name, as a rebalance or restart produces),
// never applies the effect twice and never creates a second inbox row.
func TestConsumerDuplicateDeliveryAppliesOnceAcrossRestart(t *testing.T) {
	dsn := startMigratedPostgres(t)
	pool := openEventsPool(t, dsn)
	createConsumerEffectTable(t, pool)

	const consumerName = "t046-dup"
	eventID, _ := appendConsumerEvent(t, pool, statusChangedConsumerEvent(t, "obs-t046-dup", "confirmed"))
	msg := Message{Topic: "txharbor.events.v1", Partition: 0, Offset: 5, Value: wireEnvelope(t, pool, eventID)}

	observer := newConsumerTestObserver()
	effect := newConsumerTestEffect()
	first := newTestConsumer(t, pool, consumerName, effect, observer, 200*time.Millisecond)
	if result := mustProcess(t, first, msg); result.Outcome != OutcomeApplied {
		t.Fatalf("first delivery outcome = %s, want applied", result.Outcome)
	}
	if result := mustProcess(t, first, msg); result.Outcome != OutcomeDuplicate {
		t.Fatalf("second delivery outcome = %s, want duplicate", result.Outcome)
	}

	// Restart / rebalance: a fresh instance with the same consumer name.
	restartedEffect := newConsumerTestEffect()
	restarted := newTestConsumer(t, pool, consumerName, restartedEffect, observer, 200*time.Millisecond)
	if result := mustProcess(t, restarted, msg); result.Outcome != OutcomeDuplicate {
		t.Fatalf("redelivery after restart outcome = %s, want duplicate", result.Outcome)
	}

	if n := readInboxCount(t, pool, consumerName, eventID); n != 1 {
		t.Fatalf("inbox rows = %d, want 1 (UNIQUE dedup)", n)
	}
	if n := effectRows(t, pool, eventID); n != 1 {
		t.Fatalf("effect rows = %d, want 1 (effective application exactly once)", n)
	}
	if n := restartedEffect.attemptCount(eventID); n != 0 {
		t.Fatalf("restarted effect ran %d times, want 0 (inbox dedup skipped the effect)", n)
	}
	if maxVersion, found := readVersions(t, pool, consumerName, "deposit_observation", "obs-t046-dup"); !found || maxVersion != 1 {
		t.Fatalf("consumer version = (%d, found=%v), want (1, true)", maxVersion, found)
	}
	if next, found := readProgressRow(t, pool, consumerName, "txharbor.events.v1", 0); !found || next != 6 {
		t.Fatalf("progress = (%d, found=%v), want (6, true)", next, found)
	}
	if applied, _, _, _ := observer.snapshot(); applied != 1 {
		t.Fatalf("applied observations = %d, want 1", applied)
	}
}

// TestConsumerOutOfOrderOldVersionNeverOverwrites covers FR-10/SC-04: a newer
// version applied first is never overwritten by a late old version, and the
// old version leaves no effect and no durable inbox marker.
func TestConsumerOutOfOrderOldVersionNeverOverwrites(t *testing.T) {
	dsn := startMigratedPostgres(t)
	pool := openEventsPool(t, dsn)
	createConsumerEffectTable(t, pool)

	const consumerName = "t046-ooo"
	v1, _ := appendConsumerEvent(t, pool, statusChangedConsumerEvent(t, "obs-t046-ooo", "pending"))
	v2, _ := appendConsumerEvent(t, pool, statusChangedConsumerEvent(t, "obs-t046-ooo", "confirmed"))
	msgV1 := Message{Topic: "txharbor.events.v1", Partition: 0, Offset: 0, Value: wireEnvelope(t, pool, v1)}
	msgV2 := Message{Topic: "txharbor.events.v1", Partition: 0, Offset: 1, Value: wireEnvelope(t, pool, v2)}

	consumer := newTestConsumer(t, pool, consumerName, newConsumerTestEffect(), nil, 200*time.Millisecond)
	if result := mustProcess(t, consumer, msgV2); result.Outcome != OutcomeApplied {
		t.Fatalf("newer version first outcome = %s, want applied", result.Outcome)
	}
	if result := mustProcess(t, consumer, msgV1); result.Outcome != OutcomeVersionSkip {
		t.Fatalf("late old version outcome = %s, want version_skip", result.Outcome)
	}
	if n := effectRows(t, pool, v1); n != 0 {
		t.Fatalf("old-version effect rows = %d, want 0 (never overwrite the newer state)", n)
	}
	if n := effectRows(t, pool, v2); n != 1 {
		t.Fatalf("newer-version effect rows = %d, want 1", n)
	}
	if maxVersion, found := readVersions(t, pool, consumerName, "deposit_observation", "obs-t046-ooo"); !found || maxVersion != 2 {
		t.Fatalf("consumer version = (%d, found=%v), want (2, true)", maxVersion, found)
	}
	// The old version carries no inbox marker: a redelivery stays a skip
	// instead of being swallowed as a duplicate.
	if n := readInboxCount(t, pool, consumerName, v1); n != 0 {
		t.Fatalf("old-version inbox rows = %d, want 0", n)
	}
	if result := mustProcess(t, consumer, msgV1); result.Outcome != OutcomeVersionSkip {
		t.Fatalf("redelivered old version outcome = %s, want version_skip", result.Outcome)
	}
}

// TestConsumerVersionGapBoundedWaitQuarantineAndPartitionContinues covers
// FR-10/FR-14/SC-05: a surviving gap is quarantined after the bounded wait
// (never silently skipped, never an infinite block), the partition keeps
// moving, and the later arrival of the missing version does not un-quarantine
// the event by itself.
func TestConsumerVersionGapBoundedWaitQuarantineAndPartitionContinues(t *testing.T) {
	dsn := startMigratedPostgres(t)
	pool := openEventsPool(t, dsn)
	createConsumerEffectTable(t, pool)

	const consumerName = "t046-gap"
	v1, _ := appendConsumerEvent(t, pool, statusChangedConsumerEvent(t, "obs-t046-gap", "pending"))
	v2, _ := appendConsumerEvent(t, pool, statusChangedConsumerEvent(t, "obs-t046-gap", "confirmed"))
	v3, _ := appendConsumerEvent(t, pool, statusChangedConsumerEvent(t, "obs-t046-gap", "orphaned"))

	observer := newConsumerTestObserver()
	consumer := newTestConsumer(t, pool, consumerName, newConsumerTestEffect(), observer, 300*time.Millisecond)

	if result := mustProcess(t, consumer, Message{Topic: "txharbor.events.v1", Partition: 0, Offset: 0, Value: wireEnvelope(t, pool, v1)}); result.Outcome != OutcomeApplied {
		t.Fatalf("v1 outcome = %s, want applied", result.Outcome)
	}
	gap := mustProcess(t, consumer, Message{Topic: "txharbor.events.v1", Partition: 0, Offset: 1, Value: wireEnvelope(t, pool, v3)})
	if gap.Outcome != OutcomeQuarantined || gap.Reason != QuarantineVersionGap {
		t.Fatalf("gap outcome = %+v, want quarantined/version_gap", gap)
	}
	if n := effectRows(t, pool, v3); n != 0 {
		t.Fatalf("gapped event effect rows = %d, want 0", n)
	}
	var quarantined int64
	if err := pool.QueryRow(context.Background(), `
SELECT count(*) FROM consumer_quarantine
WHERE consumer_name = $1 AND event_id = $2 AND status = 'open' AND failure_class = 'version_gap'`,
		consumerName, v3).Scan(&quarantined); err != nil {
		t.Fatalf("count quarantine rows: %v", err)
	}
	if quarantined != 1 {
		t.Fatalf("open version_gap quarantine rows = %d, want 1", quarantined)
	}
	if next, found := readProgressRow(t, pool, consumerName, "txharbor.events.v1", 0); !found || next != 2 {
		t.Fatalf("progress after quarantine = (%d, found=%v), want (2, true): the partition must continue", next, found)
	}
	// The partition continues: the missing version applies when it arrives.
	if result := mustProcess(t, consumer, Message{Topic: "txharbor.events.v1", Partition: 0, Offset: 2, Value: wireEnvelope(t, pool, v2)}); result.Outcome != OutcomeApplied {
		t.Fatalf("v2 after quarantine outcome = %s, want applied", result.Outcome)
	}
	if n := effectRows(t, pool, v2); n != 1 {
		t.Fatalf("v2 effect rows = %d, want 1", n)
	}
	if _, _, quarantines, _ := observer.snapshot(); quarantines != 1 {
		t.Fatalf("quarantine observations = %d, want 1", quarantines)
	}
	// The audited replay of the quarantined snapshot applies once the gap is
	// closed, and closes the quarantine entry.
	replay := mustReplay(t, pool, consumer, ReplayScope{Kind: ReplayScopeEventIDs, EventIDs: []uuid.UUID{v3}})
	if replay.Replayed != 1 {
		t.Fatalf("replay result = %+v, want one applied replay", replay)
	}
	if n := effectRows(t, pool, v3); n != 1 {
		t.Fatalf("replayed v3 effect rows = %d, want 1", n)
	}
	var status string
	if err := pool.QueryRow(context.Background(), `
SELECT status FROM consumer_quarantine WHERE consumer_name = $1 AND event_id = $2`,
		consumerName, v3).Scan(&status); err != nil {
		t.Fatalf("read quarantine status: %v", err)
	}
	if status != string(QuarantineStatusReplayed) {
		t.Fatalf("quarantine status = %s, want replayed", status)
	}
}

// TestConsumerVersionGapClosesDuringWait covers the other half of the gap
// policy: when the missing earlier version arrives inside the window, the
// waiting event applies and nothing is quarantined.
func TestConsumerVersionGapClosesDuringWait(t *testing.T) {
	dsn := startMigratedPostgres(t)
	pool := openEventsPool(t, dsn)
	createConsumerEffectTable(t, pool)

	const consumerName = "t046-gap-close"
	v1, _ := appendConsumerEvent(t, pool, statusChangedConsumerEvent(t, "obs-t046-close", "pending"))
	v2, _ := appendConsumerEvent(t, pool, statusChangedConsumerEvent(t, "obs-t046-close", "confirmed"))
	v3, _ := appendConsumerEvent(t, pool, statusChangedConsumerEvent(t, "obs-t046-close", "orphaned"))

	consumer := newTestConsumer(t, pool, consumerName, newConsumerTestEffect(), nil, 5*time.Second)
	if result := mustProcess(t, consumer, Message{Topic: "txharbor.events.v1", Partition: 0, Offset: 0, Value: wireEnvelope(t, pool, v1)}); result.Outcome != OutcomeApplied {
		t.Fatalf("v1 outcome = %s, want applied", result.Outcome)
	}
	// Another worker applies the missing v2 while the gap wait is running.
	go func() {
		time.Sleep(150 * time.Millisecond)
		other := newTestConsumer(t, pool, consumerName, newConsumerTestEffect(), nil, time.Second)
		_, _ = other.Process(context.Background(), Message{Topic: "txharbor.events.v1", Partition: 0, Offset: 1, Value: wireEnvelope(t, pool, v2)})
	}()
	result := mustProcess(t, consumer, Message{Topic: "txharbor.events.v1", Partition: 0, Offset: 2, Value: wireEnvelope(t, pool, v3)})
	if result.Outcome != OutcomeApplied {
		t.Fatalf("gap closed during the wait outcome = %s, want applied", result.Outcome)
	}
	var open int64
	if err := pool.QueryRow(context.Background(), `
SELECT count(*) FROM consumer_quarantine WHERE consumer_name = $1 AND event_id = $2`,
		consumerName, v3).Scan(&open); err != nil {
		t.Fatalf("count quarantine rows: %v", err)
	}
	if open != 0 {
		t.Fatalf("quarantine rows after a closed gap = %d, want 0", open)
	}
}

// mustReplay runs one audited replay and fails the test on error.
func mustReplay(t *testing.T, pool *pgxpool.Pool, consumer *Consumer, scope ReplayScope) ReplayResult {
	t.Helper()
	result, err := Replay(context.Background(), pool, consumer, ReplayOptions{
		ConsumerName: consumer.Name,
		Scope:        scope,
		Reason:       "t046 integration replay",
		Operator:     "t046-operator",
		Topic:        "txharbor.events.v1",
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	return result
}
