//go:build integration

// consumer_progress_integration_test.go is the T047 Integration-PG layer
// (V-PROGRESS): a lost or failed Kafka offset commit must never lose the
// durable PostgreSQL progress, a restart resumes 100% from consumer_progress,
// a redelivery produces zero duplicate financial effects, and progress/lag
// stay readable.
package events

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestConsumerProgressResumesFromPostgresAfterOffsetCommitLoss covers SC-06:
// the effect and its progress commit together; a Kafka offset commit loss
// only causes a redelivery the inbox absorbs, and the durable high-water mark
// never rewinds.
func TestConsumerProgressResumesFromPostgresAfterOffsetCommitLoss(t *testing.T) {
	dsn := startMigratedPostgres(t)
	pool := openEventsPool(t, dsn)
	createConsumerEffectTable(t, pool)
	ctx := context.Background()

	const consumerName = "t047-progress"
	const topic = "txharbor.events.v1"
	for i := 0; i < 3; i++ {
		ev := statusChangedConsumerEvent(t, fmt.Sprintf("obs-t047-%d", i), "confirmed")
		eventID, _ := appendConsumerEvent(t, pool, ev)
		msg := Message{Topic: topic, Partition: 0, Offset: int64(i), Value: wireEnvelope(t, pool, eventID)}
		consumer := newTestConsumer(t, pool, consumerName, newConsumerTestEffect(), nil, 200*time.Millisecond)
		if result := mustProcess(t, consumer, msg); result.Outcome != OutcomeApplied {
			t.Fatalf("event %d outcome = %s, want applied", i, result.Outcome)
		}
	}
	if next, found := readProgressRow(t, pool, consumerName, topic, 0); !found || next != 3 {
		t.Fatalf("progress = (%d, found=%v), want (3, true)", next, found)
	}

	// Kafka offset commit loss: a fresh instance (restart) is redelivered the
	// whole partition from offset 0. Every delivery is a duplicate; the
	// effects stay at one row each and the progress stays at 3.
	restarted := newTestConsumer(t, pool, consumerName, newConsumerTestEffect(), nil, 200*time.Millisecond)
	for i := 0; i < 3; i++ {
		var eventID uuid.UUID
		if err := pool.QueryRow(ctx, `
SELECT event_id FROM consumer_inbox WHERE consumer_name = $1 ORDER BY "offset" OFFSET $2 LIMIT 1`,
			consumerName, i).Scan(&eventID); err != nil {
			t.Fatalf("read inbox event %d: %v", i, err)
		}
		msg := Message{Topic: topic, Partition: 0, Offset: int64(i), Value: wireEnvelope(t, pool, eventID)}
		if result := mustProcess(t, restarted, msg); result.Outcome != OutcomeDuplicate {
			t.Fatalf("redelivered event %d outcome = %s, want duplicate", i, result.Outcome)
		}
	}
	var effects int64
	if err := pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT count(*) FROM %s`, consumerEffectTable)).Scan(&effects); err != nil {
		t.Fatalf("count effect rows: %v", err)
	}
	if effects != 3 {
		t.Fatalf("effect rows after redelivery = %d, want 3 (0 duplicate financial effects)", effects)
	}
	if next, found := readProgressRow(t, pool, consumerName, topic, 0); !found || next != 3 {
		t.Fatalf("progress after redelivery = (%d, found=%v), want (3, true)", next, found)
	}

	// A stale redelivery below the high-water mark never rewinds progress.
	stale := Message{Topic: topic, Partition: 0, Offset: 0, Value: wireEnvelope(t, pool, firstEventID(t, pool, consumerName))}
	if result := mustProcess(t, restarted, stale); result.Outcome != OutcomeDuplicate {
		t.Fatalf("stale redelivery outcome = %s, want duplicate", result.Outcome)
	}
	if next, _ := readProgressRow(t, pool, consumerName, topic, 0); next != 3 {
		t.Fatalf("progress after a stale redelivery = %d, want 3 (GREATEST keeps it monotonic)", next)
	}

	// The durable progress is readable through the consumer surface.
	progress, err := restarted.ReadProgress(ctx)
	if err != nil {
		t.Fatalf("ReadProgress: %v", err)
	}
	if len(progress) != 1 || progress[0].NextOffset != 3 || progress[0].UpdatedAt.IsZero() {
		t.Fatalf("ReadProgress = %+v, want one row at next_offset 3", progress)
	}
	next, found, err := restarted.ResumeOffset(ctx, topic, 0)
	if err != nil || !found || next != 3 {
		t.Fatalf("ResumeOffset = (%d, %v, %v), want (3, true, nil)", next, found, err)
	}
	if _, _, err := restarted.ResumeOffset(ctx, topic, 9); err != nil {
		t.Fatalf("ResumeOffset of an untouched partition errored: %v", err)
	}
	if _, found, _ := restarted.ResumeOffset(ctx, topic, 9); found {
		t.Fatal("ResumeOffset reported progress for an untouched partition")
	}
}

// firstEventID reads the first inbox event of one consumer.
func firstEventID(t *testing.T, pool *pgxpool.Pool, consumerName string) uuid.UUID {
	t.Helper()
	var eventID uuid.UUID
	if err := pool.QueryRow(context.Background(), `
SELECT event_id FROM consumer_inbox WHERE consumer_name = $1 ORDER BY "offset" LIMIT 1`,
		consumerName).Scan(&eventID); err != nil {
		t.Fatalf("read first inbox event: %v", err)
	}
	return eventID
}
