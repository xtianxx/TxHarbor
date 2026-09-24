//go:build integration

// quarantine_integration_test.go is the T048 Integration-PG layer
// (V-RETRY-QUARANTINE): retryable failures retry inside the bounded backoff,
// exhausted/permanent/unknown-version/identity-mismatch failures are
// persistently quarantined with an alert and never silently dropped, the
// partition keeps moving, and a replay after the fix is idempotent with zero
// unbounded retries.
package events

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// mutateEnvelope rewrites one JSON field of a transport envelope.
func mutateEnvelope(t *testing.T, raw []byte, mutate func(wire map[string]any)) []byte {
	t.Helper()
	var wire map[string]any
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	mutate(wire)
	body, err := json.Marshal(wire)
	if err != nil {
		t.Fatalf("encode envelope: %v", err)
	}
	return body
}

// readQuarantineRow reads the single quarantine row of one event.
func readQuarantineRow(t *testing.T, pool *pgxpool.Pool, consumerName string, eventID uuid.UUID) (failureClass, status string, attempts int) {
	t.Helper()
	err := pool.QueryRow(context.Background(), `
SELECT failure_class, status, attempt_count
FROM consumer_quarantine WHERE consumer_name = $1 AND event_id = $2`,
		consumerName, eventID).Scan(&failureClass, &status, &attempts)
	if err != nil {
		t.Fatalf("read quarantine row: %v", err)
	}
	return failureClass, status, attempts
}

// TestConsumerRetryableFailureRetriesWithinBoundThenApplies covers the
// retryable half: transient failures retry with the bounded backoff and the
// event still applies exactly once.
func TestConsumerRetryableFailureRetriesWithinBoundThenApplies(t *testing.T) {
	dsn := startMigratedPostgres(t)
	pool := openEventsPool(t, dsn)
	createConsumerEffectTable(t, pool)

	const consumerName = "t048-retry"
	eventID, _ := appendConsumerEvent(t, pool, statusChangedConsumerEvent(t, "obs-t048-retry", "confirmed"))
	observer := newConsumerTestObserver()
	effect := newConsumerTestEffect()
	effect.fail = func(env Envelope, attempt int) error {
		if attempt <= 2 {
			return Transient(errors.New("transient database failure"))
		}
		return nil
	}
	consumer := newTestConsumer(t, pool, consumerName, effect, observer, 200*time.Millisecond)
	result := mustProcess(t, consumer, Message{Topic: "txharbor.events.v1", Partition: 0, Offset: 0, Value: wireEnvelope(t, pool, eventID)})
	if result.Outcome != OutcomeApplied || result.Attempts != 3 {
		t.Fatalf("result = %+v, want applied after 3 attempts", result)
	}
	if n := effectRows(t, pool, eventID); n != 1 {
		t.Fatalf("effect rows = %d, want 1", n)
	}
	if applied, retries, _, _ := observer.snapshot(); applied != 1 || retries != 2 {
		t.Fatalf("observations = (applied=%d, retries=%d), want (1, 2)", applied, retries)
	}
}

// TestConsumerRetryExhaustionQuarantinesWithoutInfiniteRetry covers SC-05: a
// retryable failure that survives the bounded budget is quarantined
// retry_exhausted with the attempt count; the retry count is exactly the
// budget, never unbounded.
func TestConsumerRetryExhaustionQuarantinesWithoutInfiniteRetry(t *testing.T) {
	dsn := startMigratedPostgres(t)
	pool := openEventsPool(t, dsn)
	createConsumerEffectTable(t, pool)

	const consumerName = "t048-exhaust"
	eventID, _ := appendConsumerEvent(t, pool, statusChangedConsumerEvent(t, "obs-t048-exhaust", "confirmed"))
	observer := newConsumerTestObserver()
	effect := newConsumerTestEffect()
	effect.fail = func(Envelope, int) error { return Transient(errors.New("permanent outage")) }
	consumer, err := NewConsumer(pool, consumerName, effect, ConsumerOptions{
		GapWait:     200 * time.Millisecond,
		BackoffBase: 5 * time.Millisecond,
		BackoffMax:  20 * time.Millisecond,
		RetryLimit:  3,
		ChainID:     1,
		Jitter:      func() float64 { return 0 },
		Observer:    observer,
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	result := mustProcess(t, consumer, Message{Topic: "txharbor.events.v1", Partition: 0, Offset: 0, Value: wireEnvelope(t, pool, eventID)})
	if result.Outcome != OutcomeQuarantined || result.Reason != QuarantineRetryExhausted {
		t.Fatalf("result = %+v, want quarantined/retry_exhausted", result)
	}
	if result.Attempts != 3 {
		t.Fatalf("attempts = %d, want exactly the bounded budget 3", result.Attempts)
	}
	if n := effect.attemptCount(eventID); n != 3 {
		t.Fatalf("effect attempts = %d, want 3 (no unbounded retry)", n)
	}
	if n := effectRows(t, pool, eventID); n != 0 {
		t.Fatalf("effect rows = %d, want 0", n)
	}
	failureClass, status, attempts := readQuarantineRow(t, pool, consumerName, eventID)
	if failureClass != string(QuarantineRetryExhausted) || status != string(QuarantineStatusOpen) || attempts != 3 {
		t.Fatalf("quarantine row = (%s, %s, %d), want (retry_exhausted, open, 3)", failureClass, status, attempts)
	}
	if _, _, quarantines, _ := observer.snapshot(); quarantines != 1 {
		t.Fatalf("quarantine observations = %d, want 1 (alert)", quarantines)
	}
}

// TestConsumerNonRetryableQuarantinesImmediately covers the non-retryable
// half: a permanent failure is never retried and is persisted with its
// snapshot.
func TestConsumerNonRetryableQuarantinesImmediately(t *testing.T) {
	dsn := startMigratedPostgres(t)
	pool := openEventsPool(t, dsn)
	createConsumerEffectTable(t, pool)

	const consumerName = "t048-permanent"
	eventID, _ := appendConsumerEvent(t, pool, statusChangedConsumerEvent(t, "obs-t048-permanent", "confirmed"))
	effect := newConsumerTestEffect()
	effect.fail = func(Envelope, int) error { return Permanent(errors.New("contract refusal")) }
	consumer := newTestConsumer(t, pool, consumerName, effect, nil, 200*time.Millisecond)
	result := mustProcess(t, consumer, Message{Topic: "txharbor.events.v1", Partition: 0, Offset: 0, Value: wireEnvelope(t, pool, eventID)})
	if result.Outcome != OutcomeQuarantined || result.Reason != QuarantineNonRetryable || result.Attempts != 1 {
		t.Fatalf("result = %+v, want quarantined/non_retryable in one attempt", result)
	}
	failureClass, _, _ := readQuarantineRow(t, pool, consumerName, eventID)
	if failureClass != string(QuarantineNonRetryable) {
		t.Fatalf("failure class = %s, want non_retryable", failureClass)
	}
}

// TestConsumerUnknownSchemaAndIdentityMismatchQuarantine covers the
// fail-closed parse branches: an unsupported schema version and a chain
// mismatch are quarantined with their own classes, never guess-parsed or
// applied.
func TestConsumerUnknownSchemaAndIdentityMismatchQuarantine(t *testing.T) {
	dsn := startMigratedPostgres(t)
	pool := openEventsPool(t, dsn)
	createConsumerEffectTable(t, pool)

	const consumerName = "t048-parse"
	eventID, _ := appendConsumerEvent(t, pool, statusChangedConsumerEvent(t, "obs-t048-parse", "confirmed"))
	base := wireEnvelope(t, pool, eventID)
	consumer := newTestConsumer(t, pool, consumerName, newConsumerTestEffect(), nil, 200*time.Millisecond)

	unsupported := mutateEnvelope(t, base, func(wire map[string]any) { wire["schema_version"] = 2 })
	result := mustProcess(t, consumer, Message{Topic: "txharbor.events.v1", Partition: 0, Offset: 0, Value: unsupported})
	if result.Outcome != OutcomeQuarantined || result.Reason != QuarantineSchemaUnsupported {
		t.Fatalf("unsupported version result = %+v, want quarantined/schema_unsupported", result)
	}
	if n := effectRows(t, pool, eventID); n != 0 {
		t.Fatalf("unsupported-version effect rows = %d, want 0", n)
	}

	mismatch := mutateEnvelope(t, base, func(wire map[string]any) { wire["chain_id"] = 999 })
	result = mustProcess(t, consumer, Message{Topic: "txharbor.events.v1", Partition: 0, Offset: 1, Value: mismatch})
	if result.Outcome != OutcomeQuarantined || result.Reason != QuarantineIdentityMismatch {
		t.Fatalf("identity mismatch result = %+v, want quarantined/identity_mismatch", result)
	}
	var classes []string
	rows, err := pool.Query(context.Background(), `
SELECT failure_class FROM consumer_quarantine WHERE consumer_name = $1 ORDER BY id`, consumerName)
	if err != nil {
		t.Fatalf("read quarantine classes: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var class string
		if err := rows.Scan(&class); err != nil {
			t.Fatalf("scan quarantine class: %v", err)
		}
		classes = append(classes, class)
	}
	if len(classes) != 2 || classes[0] != string(QuarantineSchemaUnsupported) || classes[1] != string(QuarantineIdentityMismatch) {
		t.Fatalf("quarantine classes = %v, want [schema_unsupported identity_mismatch]", classes)
	}
}

// TestConsumerQuarantineDoesNotBlockPartitionAndReplayIsIdempotent covers the
// partition-continuation rule and the audited replay: after a poison event is
// quarantined the next event applies; after the fix a replay applies exactly
// once, marks the entry replayed, and an identical CLI retry converges on the
// stored audit result instead of replaying again.
func TestConsumerQuarantineDoesNotBlockPartitionAndReplayIsIdempotent(t *testing.T) {
	dsn := startMigratedPostgres(t)
	pool := openEventsPool(t, dsn)
	createConsumerEffectTable(t, pool)
	ctx := context.Background()

	const consumerName = "t048-replay"
	poisonID, _ := appendConsumerEvent(t, pool, statusChangedConsumerEvent(t, "obs-t048-poison", "confirmed"))
	followID, _ := appendConsumerEvent(t, pool, statusChangedConsumerEvent(t, "obs-t048-follow", "confirmed"))

	effect := newConsumerTestEffect()
	effect.fail = func(env Envelope, attempt int) error {
		if env.AggregateID == "obs-t048-poison" {
			return Permanent(errors.New("poison payload"))
		}
		return nil
	}
	consumer := newTestConsumer(t, pool, consumerName, effect, nil, 200*time.Millisecond)

	poison := mustProcess(t, consumer, Message{Topic: "txharbor.events.v1", Partition: 0, Offset: 0, Value: wireEnvelope(t, pool, poisonID)})
	if poison.Outcome != OutcomeQuarantined {
		t.Fatalf("poison outcome = %+v, want quarantined", poison)
	}
	follow := mustProcess(t, consumer, Message{Topic: "txharbor.events.v1", Partition: 0, Offset: 1, Value: wireEnvelope(t, pool, followID)})
	if follow.Outcome != OutcomeApplied {
		t.Fatalf("follow-up outcome = %+v, want applied (the partition must continue)", follow)
	}
	if next, found := readProgressRow(t, pool, consumerName, "txharbor.events.v1", 0); !found || next != 2 {
		t.Fatalf("progress = (%d, found=%v), want (2, true)", next, found)
	}

	// Fix the cause and replay the quarantined event: exactly one effect.
	effect.fail = nil
	first := mustReplay(t, pool, consumer, ReplayScope{Kind: ReplayScopeEventIDs, EventIDs: []uuid.UUID{poisonID}})
	if first.Replayed != 1 || first.Total != 1 {
		t.Fatalf("first replay = %+v, want one applied replay", first)
	}
	if n := effectRows(t, pool, poisonID); n != 1 {
		t.Fatalf("replayed effect rows = %d, want 1", n)
	}
	_, status, _ := readQuarantineRow(t, pool, consumerName, poisonID)
	if status != string(QuarantineStatusReplayed) {
		t.Fatalf("quarantine status = %s, want replayed", status)
	}

	// The identical CLI retry (same parameters -> same operation id) returns
	// the stored audit result and replays nothing.
	second := mustReplay(t, pool, consumer, ReplayScope{Kind: ReplayScopeEventIDs, EventIDs: []uuid.UUID{poisonID}})
	if !second.Deduplicated || second.StoredResult == "" {
		t.Fatalf("second replay = %+v, want a deduplicated stored result", second)
	}
	if n := effectRows(t, pool, poisonID); n != 1 {
		t.Fatalf("effect rows after the deduplicated retry = %d, want 1", n)
	}

	// A deliberately new operation (explicit id) re-processes the event: the
	// inbox dedup absorbs it, so the effect count stays 1.
	again, err := Replay(ctx, pool, consumer, ReplayOptions{
		ConsumerName: consumerName,
		Scope:        ReplayScope{Kind: ReplayScopeEventIDs, EventIDs: []uuid.UUID{poisonID}},
		Reason:       "second look",
		Operator:     "t048-operator",
		OperationID:  "t048-explicit-replay-2",
		Topic:        "txharbor.events.v1",
	})
	if err != nil {
		t.Fatalf("explicit second replay: %v", err)
	}
	if again.SkippedDuplicate != 1 || again.Replayed != 0 {
		t.Fatalf("explicit second replay = %+v, want one duplicate skip", again)
	}
	if n := effectRows(t, pool, poisonID); n != 1 {
		t.Fatalf("effect rows after the explicit second replay = %d, want 1", n)
	}

	// The audit rows are complete and carry the operator/scope/reason/result.
	var operator, reason, result string
	var scope []byte
	if err := pool.QueryRow(ctx, `
SELECT operator, reason, result, scope FROM event_ops_audit WHERE operation_id = $1`,
		first.OperationID).Scan(&operator, &reason, &result, &scope); err != nil {
		t.Fatalf("read audit row: %v", err)
	}
	if operator == "" || reason == "" || result == "" || len(scope) == 0 {
		t.Fatalf("audit row incomplete: operator=%q reason=%q result=%q scope=%s", operator, reason, result, scope)
	}
	if !strings.Contains(result, "replayed=1") {
		t.Fatalf("audit result %q does not record the replay count", result)
	}
}
