//go:build integration_kafka

// recovery_boundary_integration_test.go is the T053 [US5] Integration-Kafka
// layer over a real broker and a real migrated PostgreSQL:
//
//   - the PG consumer_progress / broker committed-offset relation is compared
//     through the real broker offset fetch (kadm): equality with a readable
//     idempotency history is the only consistent verdict, a broker position
//     ahead of the restored progress is a detected rollback (divergent), and a
//     positive restored progress without inbox/version history is unknown —
//     never silently backfilled as processed and never re-executable;
//   - duplicate delivery over the real broker is absorbed by the existing
//     consumer_inbox/consumer_versions idempotency with zero extra effects;
//   - a missing idempotency record triggers no side-effectful reprocessing by
//     the boundary report itself (which is read-only) and no automatic
//     redelivery of the already-committed range; re-processing exists only
//     through the existing authorized quarantine-replay path, which is audited
//     and converges on its operation id;
//   - the possible external duplicate effect of a missing history is reported
//     (never resolved) and no cross-system exactly-once is claimed.
//
// Docker discipline: without a Docker provider the test reports NOT RUN
// (testcontainers.SkipIfProviderIsNotHealthy), never a pass.
package events

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/xtianxx/txharbor/internal/recovery"
	"github.com/xtianxx/txharbor/internal/testutil"
)

// t053KafkaOffsets is the read-only broker surface of one consumer group: the
// committed offset (the next offset to consume) of one topic/partition, read
// through kadm. It implements recovery.EventBoundaryBrokerOffsets.
type t053KafkaOffsets struct {
	brokers []string
	groupID string
}

// CommittedOffset implements recovery.EventBoundaryBrokerOffsets.
func (r t053KafkaOffsets) CommittedOffset(ctx context.Context, topic string, partition int) (int64, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(r.brokers...))
	if err != nil {
		return 0, fmt.Errorf("kafka offsets client: %w", err)
	}
	defer cl.Close()
	responses, err := kadm.NewClient(cl).FetchOffsets(ctx, r.groupID)
	if err != nil {
		return 0, fmt.Errorf("fetch committed offsets of group %s: %w", r.groupID, err)
	}
	response, ok := responses.Lookup(topic, int32(partition))
	if !ok {
		return 0, fmt.Errorf("no committed-offset response for %s/%d", topic, partition)
	}
	if response.Err != nil {
		return 0, fmt.Errorf("committed offset %s/%d: %w", topic, partition, response.Err)
	}
	if response.At < 0 {
		return 0, fmt.Errorf("no committed offset exists for %s/%d", topic, partition)
	}
	return response.At, nil
}

// t053Count counts rows of one query (test-local; the integration-tagged
// helpers of other layers are not compiled under this tag).
func t053Count(t *testing.T, pool *pgxpool.Pool, query string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("count (%s): %v", query, err)
	}
	return n
}

// t053WaitProgress waits until consumerName carries exactly one progress row
// and returns its partition and next_offset.
func t053WaitProgress(t *testing.T, pool *pgxpool.Pool, consumerName string, timeout time.Duration) (int, int64) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var (
		lastPartition int
		lastNext      int64
	)
	for time.Now().Before(deadline) {
		rows, err := pool.Query(context.Background(), `
SELECT partition, next_offset FROM consumer_progress
WHERE consumer_name = $1 ORDER BY partition`, consumerName)
		if err != nil {
			t.Fatalf("read consumer progress: %v", err)
		}
		type row struct {
			partition int
			next      int64
		}
		var got []row
		for rows.Next() {
			var r row
			if err := rows.Scan(&r.partition, &r.next); err != nil {
				rows.Close()
				t.Fatalf("scan consumer progress: %v", err)
			}
			got = append(got, r)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			t.Fatalf("iterate consumer progress: %v", err)
		}
		if len(got) == 1 {
			return got[0].partition, got[0].next
		}
		if len(got) > 1 {
			t.Fatalf("consumer %s has %d progress rows; this layer expects one event/partition", consumerName, len(got))
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("consumer %s has no durable progress row within %s (partition=%d next=%d)",
		consumerName, timeout, lastPartition, lastNext)
	return 0, 0
}

// t053WaitBrokerOffset waits until the broker committed offset of the
// group/topic/partition reaches want.
func t053WaitBrokerOffset(t *testing.T, ctx context.Context, reader t053KafkaOffsets,
	topic string, partition int, want int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last error
	for time.Now().Before(deadline) {
		got, err := reader.CommittedOffset(ctx, topic, partition)
		if err == nil && got == want {
			return
		}
		last = err
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("broker committed offset %s/%d never reached %d (last error: %v)", topic, partition, want, last)
}

// t053BoundaryInspector builds the read-only boundary inspector over the pool
// and the real broker reader.
func t053BoundaryInspector(t *testing.T, pool *pgxpool.Pool, broker recovery.EventBoundaryBrokerOffsets) *recovery.EventBoundaryInspector {
	t.Helper()
	inspector, err := recovery.NewEventBoundaryInspector(recovery.EventBoundaryOptions{Data: pool, Broker: broker})
	if err != nil {
		t.Fatalf("NewEventBoundaryInspector: %v", err)
	}
	return inspector
}

// t053Produce publishes one envelope to partition 0 of the canonical topic.
// The producer is built with the manual partitioner so both deliveries of a
// scenario land on one partition deterministically.
func t053Produce(t *testing.T, ctx context.Context, producer *kgo.Client, key string, value []byte) {
	t.Helper()
	if err := producer.ProduceSync(ctx, &kgo.Record{Partition: 0, Key: []byte(key), Value: value}).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}
}

// testRecoveryBoundaryKafkaOffsetRollbackDetected is the rollback half of
// T053: equality with a readable history is consistent; a broker position
// ahead of the restored progress is a detected rollback; a positive restored
// progress without history is unknown and never re-executable. The detection
// itself writes nothing anywhere.
func TestRecoveryBoundaryKafkaOffsetRollbackDetected(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	kafka := startKafkaConsumerBroker(t)
	if err := kafka.EnsureTopic(ctx, testutil.KafkaTopic, testutil.KafkaPartitions); err != nil {
		t.Fatalf("ensure topic: %v", err)
	}
	pool := openKafkaPool(t, startKafkaPostgres(t))

	producer, err := kgo.NewClient(
		kgo.SeedBrokers(kafka.Brokers()...),
		kgo.DefaultProduceTopic(testutil.KafkaTopic),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	defer producer.Close()

	// Two events of one aggregate (same partition), consumed in order.
	firstID := appendKafkaConsumerEvent(t, pool, kafkaConsumerStatusChangedEvent(t, "obs-t053-rollback", "pending"))
	secondID := appendKafkaConsumerEvent(t, pool, kafkaConsumerStatusChangedEvent(t, "obs-t053-rollback", "confirmed"))
	const key = "deposit_observation:obs-t053-rollback"
	t053Produce(t, ctx, producer, key, kafkaConsumerEnvelope(t, pool, firstID))
	t053Produce(t, ctx, producer, key, kafkaConsumerEnvelope(t, pool, secondID))

	observer := newKafkaConsumerObserver()
	runtime := newKafkaConsumerRuntime(t, pool, kafka.Brokers(), "t053-rollback", observer, 10)
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- runtime.Run(runCtx) }()

	waitForKafkaConsumerEffect(t, pool, firstID, 1, 60*time.Second)
	waitForKafkaConsumerEffect(t, pool, secondID, 1, 60*time.Second)
	partition, next := t053WaitProgress(t, pool, RefConsumerName, 30*time.Second)
	if next != 2 {
		t.Fatalf("durable progress = %d, want 2 (both deliveries consumed)", next)
	}
	offsets := t053KafkaOffsets{brokers: kafka.Brokers(), groupID: runtime.GroupID}
	t053WaitBrokerOffset(t, ctx, offsets, testutil.KafkaTopic, partition, next, 30*time.Second)

	stop()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("consumer stopped with %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("consumer did not stop")
	}

	inspector := t053BoundaryInspector(t, pool, offsets)

	// Equality plus a readable idempotency history is the consistent case.
	report, err := inspector.Report(ctx)
	if err != nil {
		t.Fatalf("Report: %v", err)
	}
	if report.Conclusion != recovery.ConclusionConsistent || !report.Passes() {
		t.Fatalf("pre-rollback report = %+v, want consistent", report)
	}
	if report.RollbackDetected || len(report.MissingIdempotencyHistory) != 0 ||
		len(report.PossibleExternalDuplicateEffects) != 0 {
		t.Fatalf("pre-rollback report carries rollback/missing flags: %+v", report)
	}
	if report.CrossSystemExactlyOnce {
		t.Fatal("the report must not claim cross-system exactly-once")
	}

	ledgerRows := kafkaConsumerEffectRows(t, pool, firstID) + kafkaConsumerEffectRows(t, pool, secondID)
	if ledgerRows != 2 {
		t.Fatalf("reference ledger rows = %d, want 2", ledgerRows)
	}

	// Simulate the restore-point regression: the restore point predates the
	// second consumption (progress 2 -> 1, the second inbox row is gone) while
	// the broker position stays at 2.
	if _, err := pool.Exec(ctx, `DELETE FROM consumer_inbox
		WHERE consumer_name = $1 AND topic = $2 AND "offset" = 1`, RefConsumerName, testutil.KafkaTopic); err != nil {
		t.Fatalf("delete the restored-away inbox row: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE consumer_progress SET next_offset = 1
		WHERE consumer_name = $1 AND topic = $2 AND partition = $3`, RefConsumerName, testutil.KafkaTopic, partition); err != nil {
		t.Fatalf("regress the restored progress: %v", err)
	}
	auditBefore := t053Count(t, pool, `SELECT count(*) FROM event_ops_audit`)

	report, err = inspector.Report(ctx)
	if err != nil {
		t.Fatalf("Report after the rollback: %v", err)
	}
	if report.Conclusion != recovery.ConclusionDivergent || report.Passes() {
		t.Fatalf("rollback report = %+v, want divergent (never a pass)", report)
	}
	if !report.RollbackDetected {
		t.Fatalf("rollback report did not detect the broker position ahead of the restored progress: %+v", report)
	}
	if len(report.Checks) != 1 || report.Checks[0].OffsetRelation != recovery.EventOffsetBrokerAhead {
		t.Fatalf("rollback checks = %+v, want one broker_ahead relation", report.Checks)
	}
	if len(report.PossibleExternalDuplicateEffects) == 0 {
		t.Fatal("a rollback must report possible external duplicate effects")
	}
	// The rollback detection itself wrote nothing.
	if n := t053Count(t, pool, `SELECT count(*) FROM consumer_inbox WHERE consumer_name = $1`, RefConsumerName); n != 1 {
		t.Fatalf("consumer_inbox rows = %d after the rollback report, want 1 (unchanged)", n)
	}
	if n := t053Count(t, pool, `SELECT next_offset FROM consumer_progress
		WHERE consumer_name = $1 AND topic = $2 AND partition = $3`, RefConsumerName, testutil.KafkaTopic, partition); n != 1 {
		t.Fatalf("consumer_progress next_offset = %d after the rollback report, want 1 (unchanged)", n)
	}
	if n := kafkaConsumerEffectRows(t, pool, firstID) + kafkaConsumerEffectRows(t, pool, secondID); n != ledgerRows {
		t.Fatalf("reference ledger rows = %d after the rollback report, want %d (zero effects)", n, ledgerRows)
	}

	// A positive restored progress with no idempotency history: unknown,
	// missing history reported, never backfilled and never re-executable. The
	// restore point is set back to the consumed position so the offset
	// relation is equal and only the missing history is under test.
	if _, err := pool.Exec(ctx, `UPDATE consumer_progress SET next_offset = 2
		WHERE consumer_name = $1 AND topic = $2 AND partition = $3`,
		RefConsumerName, testutil.KafkaTopic, partition); err != nil {
		t.Fatalf("restore the consumed progress position: %v", err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM consumer_inbox WHERE consumer_name = $1`, RefConsumerName); err != nil {
		t.Fatalf("delete the restored inbox history: %v", err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM consumer_versions WHERE consumer_name = $1`, RefConsumerName); err != nil {
		t.Fatalf("delete the restored version history: %v", err)
	}
	report, err = inspector.Report(ctx)
	if err != nil {
		t.Fatalf("Report after the missing history: %v", err)
	}
	if report.Conclusion != recovery.ConclusionUnknown || report.Passes() {
		t.Fatalf("missing-history report = %+v, want unknown", report)
	}
	if len(report.Checks) != 1 || report.Checks[0].OffsetRelation != recovery.EventOffsetEqual {
		t.Fatalf("missing-history checks = %+v, want one equal relation", report.Checks)
	}
	if len(report.MissingIdempotencyHistory) != 1 || len(report.PossibleExternalDuplicateEffects) != 1 {
		t.Fatalf("missing-history report = %+v, want one missing-history and one possible-duplicate entry", report)
	}
	if !strings.Contains(report.Checks[0].Reason, "never") {
		t.Fatalf("missing-history reason %q does not state the record is never backfilled", report.Checks[0].Reason)
	}

	// The detection is read-only: no inbox row was backfilled, no progress
	// advanced, no effect applied, no quarantine entry and no audit row added.
	if n := t053Count(t, pool, `SELECT count(*) FROM consumer_inbox WHERE consumer_name = $1`, RefConsumerName); n != 0 {
		t.Fatalf("consumer_inbox rows = %d after the report, want 0 (never backfilled)", n)
	}
	if n := t053Count(t, pool, `SELECT count(*) FROM consumer_versions WHERE consumer_name = $1`, RefConsumerName); n != 0 {
		t.Fatalf("consumer_versions rows = %d after the report, want 0", n)
	}
	if n := t053Count(t, pool, `SELECT next_offset FROM consumer_progress
		WHERE consumer_name = $1 AND topic = $2 AND partition = $3`, RefConsumerName, testutil.KafkaTopic, partition); n != 2 {
		t.Fatalf("consumer_progress next_offset = %d after the report, want 2 (never advanced)", n)
	}
	if n := kafkaConsumerEffectRows(t, pool, firstID) + kafkaConsumerEffectRows(t, pool, secondID); n != ledgerRows {
		t.Fatalf("reference ledger rows = %d after the report, want %d (zero effects)", n, ledgerRows)
	}
	if n := t053Count(t, pool, `SELECT count(*) FROM consumer_quarantine`); n != 0 {
		t.Fatalf("consumer_quarantine rows = %d after the report, want 0 (nothing quarantined)", n)
	}
	if n := t053Count(t, pool, `SELECT count(*) FROM event_ops_audit`); n != auditBefore {
		t.Fatalf("event_ops_audit rows = %d after the report, want %d (the report writes no audit)", n, auditBefore)
	}

	// The broker still carries the committed position: the detection did not
	// rewind or commit anything.
	committed, err := offsets.CommittedOffset(ctx, testutil.KafkaTopic, partition)
	if err != nil {
		t.Fatalf("re-read broker committed offset: %v", err)
	}
	if committed != 2 {
		t.Fatalf("broker committed offset = %d after the report, want 2", committed)
	}
}

// TestRecoveryBoundaryKafkaDuplicateAndAuthorizedReplay is the duplicate half
// of T053: duplicate broker delivery is absorbed by the persistent idempotency
// with zero extra effects; a missing idempotency record triggers no automatic
// side-effectful reprocessing and no redelivery of the committed range; and
// the only reprocessing path is the audited quarantine replay, which applies
// the quarantined event exactly once and converges on its operation id.
func TestRecoveryBoundaryKafkaDuplicateAndAuthorizedReplay(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	kafka := startKafkaConsumerBroker(t)
	if err := kafka.EnsureTopic(ctx, testutil.KafkaTopic, testutil.KafkaPartitions); err != nil {
		t.Fatalf("ensure topic: %v", err)
	}
	pool := openKafkaPool(t, startKafkaPostgres(t))

	producer, err := kgo.NewClient(
		kgo.SeedBrokers(kafka.Brokers()...),
		kgo.DefaultProduceTopic(testutil.KafkaTopic),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	defer producer.Close()

	eventID := appendKafkaConsumerEvent(t, pool, kafkaConsumerStatusChangedEvent(t, "obs-t053-dup", "confirmed"))
	const key = "deposit_observation:obs-t053-dup"
	value := kafkaConsumerEnvelope(t, pool, eventID)
	t053Produce(t, ctx, producer, key, value)
	t053Produce(t, ctx, producer, key, value) // duplicate delivery

	observer := newKafkaConsumerObserver()
	runtime := newKafkaConsumerRuntime(t, pool, kafka.Brokers(), "t053-dup", observer, 10)
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- runtime.Run(runCtx) }()

	waitForKafkaConsumerEffect(t, pool, eventID, 1, 60*time.Second)
	time.Sleep(1500 * time.Millisecond) // let the duplicate be consumed as a no-op
	if n := kafkaConsumerEffectRows(t, pool, eventID); n != 1 {
		t.Fatalf("reference ledger rows after the duplicate = %d, want 1 (idempotent absorption)", n)
	}
	if n := t053Count(t, pool, `SELECT count(*) FROM consumer_inbox
		WHERE consumer_name = $1 AND event_id = $2`, RefConsumerName, eventID); n != 1 {
		t.Fatalf("consumer_inbox rows after the duplicate = %d, want 1", n)
	}
	partition, next := t053WaitProgress(t, pool, RefConsumerName, 30*time.Second)
	if next != 2 {
		t.Fatalf("durable progress = %d, want 2 (both deliveries absorbed)", next)
	}
	offsets := t053KafkaOffsets{brokers: kafka.Brokers(), groupID: runtime.GroupID}
	t053WaitBrokerOffset(t, ctx, offsets, testutil.KafkaTopic, partition, next, 30*time.Second)

	stop()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("consumer stopped with %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("consumer did not stop")
	}

	inspector := t053BoundaryInspector(t, pool, offsets)
	report, err := inspector.Report(ctx)
	if err != nil {
		t.Fatalf("Report: %v", err)
	}
	if report.Conclusion != recovery.ConclusionConsistent || !report.Passes() {
		t.Fatalf("post-duplicate report = %+v, want consistent (the duplicate was absorbed)", report)
	}

	// Simulate the loss of the historical idempotency records at the restore
	// point (the progress row keeps the positive offset).
	if _, err := pool.Exec(ctx, `DELETE FROM consumer_inbox WHERE consumer_name = $1`, RefConsumerName); err != nil {
		t.Fatalf("delete the inbox history: %v", err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM consumer_versions WHERE consumer_name = $1`, RefConsumerName); err != nil {
		t.Fatalf("delete the version history: %v", err)
	}
	ledgerBefore := kafkaConsumerEffectRows(t, pool, eventID)
	auditBefore := t053Count(t, pool, `SELECT count(*) FROM event_ops_audit`)
	for i := 0; i < 3; i++ {
		report, err = inspector.Report(ctx)
		if err != nil {
			t.Fatalf("Report %d after the missing history: %v", i, err)
		}
		if report.Passes() {
			t.Fatalf("missing-history report %d = %+v, want a non-passing verdict", i, report)
		}
		if len(report.MissingIdempotencyHistory) != 1 || len(report.PossibleExternalDuplicateEffects) != 1 {
			t.Fatalf("missing-history report %d = %+v, want the missing/duplicate flags", i, report)
		}
	}
	// The report itself never reprocesses: no effect, no backfill, no audit.
	if n := kafkaConsumerEffectRows(t, pool, eventID); n != ledgerBefore {
		t.Fatalf("reference ledger rows = %d after the reports, want %d (no side-effectful reprocessing)", n, ledgerBefore)
	}
	if n := t053Count(t, pool, `SELECT count(*) FROM consumer_inbox WHERE consumer_name = $1`, RefConsumerName); n != 0 {
		t.Fatalf("consumer_inbox rows = %d after the reports, want 0 (never backfilled as processed)", n)
	}
	if n := t053Count(t, pool, `SELECT count(*) FROM event_ops_audit`); n != auditBefore {
		t.Fatalf("event_ops_audit rows = %d after the reports, want %d", n, auditBefore)
	}

	// Restarting the group does not re-read the committed range: the resume
	// position is the broker committed offset, so the missing history triggers
	// no automatic redelivery and no duplicate effect.
	restart := newKafkaConsumerRuntime(t, pool, kafka.Brokers(), "t053-dup-restart", observer, 10)
	restartCtx, stopRestart := context.WithCancel(ctx)
	restartDone := make(chan error, 1)
	go func() { restartDone <- restart.Run(restartCtx) }()
	time.Sleep(2 * time.Second)
	stopRestart()
	select {
	case err := <-restartDone:
		if err != nil {
			t.Fatalf("restarted consumer stopped with %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("restarted consumer did not stop")
	}
	if n := kafkaConsumerEffectRows(t, pool, eventID); n != ledgerBefore {
		t.Fatalf("reference ledger rows = %d after the restart, want %d (no automatic reprocessing)", n, ledgerBefore)
	}

	// The only reprocessing path is the authorized quarantine replay: an
	// event that could not be applied (version gap) is replayed through
	// events.Replay, applied exactly once, audited, and a repeated invocation
	// converges on the stored operation id without a second effect.
	reference, err := NewReferenceConsumer(pool, ConsumerOptions{
		GapWait:     200 * time.Millisecond,
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
	if err := reference.EnsureLedgerSchema(ctx); err != nil {
		t.Fatalf("EnsureLedgerSchema: %v", err)
	}
	v1 := appendKafkaConsumerEvent(t, pool, kafkaConsumerStatusChangedEvent(t, "obs-t053-replay", "pending"))
	v2 := appendKafkaConsumerEvent(t, pool, kafkaConsumerStatusChangedEvent(t, "obs-t053-replay", "confirmed"))
	v3 := appendKafkaConsumerEvent(t, pool, kafkaConsumerStatusChangedEvent(t, "obs-t053-replay", "reinstated"))

	process := func(id uuid.UUID, offset int64) ProcessResult {
		t.Helper()
		result, err := reference.Process(ctx, Message{
			Topic: testutil.KafkaTopic, Partition: 0, Offset: offset, Value: kafkaConsumerEnvelope(t, pool, id),
		})
		if err != nil {
			t.Fatalf("Process(%s): %v", id, err)
		}
		return result
	}
	if result := process(v1, 0); result.Outcome != OutcomeApplied {
		t.Fatalf("v1 outcome = %+v, want applied", result)
	}
	if result := process(v3, 2); result.Outcome != OutcomeQuarantined || result.Reason != QuarantineVersionGap {
		t.Fatalf("v3 outcome = %+v, want a version_gap quarantine", result)
	}
	if result := process(v2, 1); result.Outcome != OutcomeApplied {
		t.Fatalf("v2 outcome = %+v, want applied (the gap is now closed)", result)
	}

	replay, err := Replay(ctx, pool, reference.Consumer, ReplayOptions{
		ConsumerName: RefConsumerName,
		Scope:        ReplayScope{Kind: ReplayScopeEventIDs, EventIDs: []uuid.UUID{v3}},
		Reason:       "t053 authorized quarantine replay",
		Operator:     "t053-operator",
		Topic:        testutil.KafkaTopic,
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if replay.Replayed != 1 || replay.Total != 1 {
		t.Fatalf("replay result = %+v, want one replayed event", replay)
	}
	if n := kafkaConsumerEffectRows(t, pool, v3); n != 1 {
		t.Fatalf("reference ledger rows for the replayed event = %d, want 1", n)
	}
	var quarantineStatus string
	if err := pool.QueryRow(ctx, `SELECT status FROM consumer_quarantine
		WHERE consumer_name = $1 AND event_id = $2`, RefConsumerName, v3).Scan(&quarantineStatus); err != nil {
		t.Fatalf("read the quarantine status: %v", err)
	}
	if quarantineStatus != "replayed" {
		t.Fatalf("quarantine status = %q, want replayed", quarantineStatus)
	}
	var auditKind, auditResult string
	if err := pool.QueryRow(ctx, `SELECT op_kind, result FROM event_ops_audit
		WHERE operation_id = $1`, replay.OperationID).Scan(&auditKind, &auditResult); err != nil {
		t.Fatalf("read the replay audit row: %v", err)
	}
	if auditKind != "replay" || !strings.Contains(auditResult, "replayed=1") {
		t.Fatalf("replay audit = (%q, %q), want a replay row with replayed=1", auditKind, auditResult)
	}
	repeat, err := Replay(ctx, pool, reference.Consumer, ReplayOptions{
		ConsumerName: RefConsumerName,
		Scope:        ReplayScope{Kind: ReplayScopeEventIDs, EventIDs: []uuid.UUID{v3}},
		Reason:       "t053 authorized quarantine replay",
		Operator:     "t053-operator",
		Topic:        testutil.KafkaTopic,
	})
	if err != nil {
		t.Fatalf("repeated Replay: %v", err)
	}
	if !repeat.Deduplicated || repeat.StoredResult != auditResult {
		t.Fatalf("repeated replay = %+v, want the stored result %q", repeat, auditResult)
	}
	if n := t053Count(t, pool, `SELECT count(*) FROM event_ops_audit WHERE op_kind = 'replay'`); n != 1 {
		t.Fatalf("replay audit rows = %d, want 1 (operation-id convergence)", n)
	}
	if n := kafkaConsumerEffectRows(t, pool, v3); n != 1 {
		t.Fatalf("reference ledger rows for the replayed event = %d after the repeated replay, want 1", n)
	}
	// The replayed event's effect never reaches an upstream authority: no
	// intent, nonce, signature or broadcast row appears.
	for _, table := range []string{"payment_intents", "nonce_bindings", "tx_attempts", "signing_requests", "tx_send_attempts"} {
		if n := t053Count(t, pool, `SELECT count(*) FROM `+table); n != 0 {
			t.Fatalf("%s rows = %d, want 0 (a replayed consumer event is never a send permission)", table, n)
		}
	}
}
