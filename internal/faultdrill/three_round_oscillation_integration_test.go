//go:build fault

// three_round_oscillation_integration_test.go is the 013-supplement task-2
// drill: three pre-fixed dependency-oscillation rounds against the real
// containers. It reuses the T073/T019 fault-drill harness primitives
// (StopKafka/StartKafka, SuspendKafka/ResumeKafka, SuspendDual/ResumeAll) and
// the real publisher/consumer runtimes; no state function replaces a service
// path.
//
// Fixed timeline (declared once in oscillationTimeline, never adapted at run
// time):
//
//	round | fault                                        | recovery
//	------+----------------------------------------------+----------------------------------
//	  1   | Kafka container Stop (real terminate; the    | Kafka container Start: a fresh
//	      | original topic log is replaced)              | broker on the same fixed address,
//	      |                                              | canonical topic re-created
//	  2   | Kafka broker SIGSTOP (log + offsets kept)    | SIGCONT (the same log resumes)
//	  3   | Kafka SIGSTOP + Redis container stop (dual)  | SIGCONT + Redis container start
//
// Ordering note (why the container replacement is round 1): the harness's
// Start boots a *fresh* broker whose log replaces the old one, so old Kafka
// offset coordinates (and the PostgreSQL offset mirror derived from them) are
// no longer meaningful — the harness documents that boundary itself. This
// drill therefore runs the container replacement before any durable consumer
// progress exists and claims no durable-offset resume across a broker
// replacement; rounds 2–3 verify durable-progress resume on a preserved log.
// A single-node container Stop/Start is a real container replacement, NOT a
// cluster election/failover verification.
//
// Every round asserts, on the real dependencies:
//
//   - business continues during the fault: events appended through the real
//     Append path stay pending (0 loss), nothing is published or applied;
//   - recovery drains the durable pending set with bounded claims, and the
//     reference consumer catches up from durable PostgreSQL progress until the
//     catch-up gap reaches 0 (recorded as a non-increasing series);
//   - the durable consumer progress never rewinds (sampled and compared);
//   - event identity: the same fact re-appended through the real Append path
//     is an idempotent no-op with the same event_id and payload hash; the
//     delivered envelope bytes equal the stored outbox row's deterministic
//     serialization; and a transport-level duplicate publication (the round's
//     rows reset to pending and republished through the real publisher) is
//     absorbed: group commits advance, the ledger keeps exactly one effect per
//     event;
//   - the reference consumer records exactly one simulated-ledger effect per
//     event, and a byte-identical redelivery returns the duplicate outcome.
//
// Final state: pending/blocked 0, every row published, catch-up gap 0, the
// FR-06 zero-invariants hold and the send-side authority fingerprint is
// unchanged (a delivered event is never a send permission).
//
// Evidence: env_spec.json, timeline.jsonl (oscillation_timeline /
// oscillation_round / oscillation_final) and metrics.txt in the evidence
// directory (a temp dir unless TXHARBOR_FAULT_EVIDENCE_DIR is set). Raw run
// logs stay outside the repository; the committed record is
// docs/evidence/013-supplement/oscillation.md.
package faultdrill

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/xtianxx/txharbor/internal/events"
	"github.com/xtianxx/txharbor/internal/testutil"
)

// oscillationOccurredAt is the frozen occurred_at of the deterministic
// identity fact: a fixed value keeps the exact bytes re-derivable.
var oscillationOccurredAt = time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)

// oscillationFact builds the deterministic evm_log fact of one round. The
// same step always produces the same identity material and the same canonical
// payload bytes, so re-appending it must be an idempotent no-op (FR-09).
func oscillationFact(step int) events.Event {
	obsID := fmt.Sprintf("oscillation-obs-%d", step)
	return events.Event{
		EventType:     events.EventTypeDepositObservationCreated,
		SchemaVersion: events.SchemaVersionV1,
		IdentityKind:  events.IdentityKindEVMLog,
		AggregateType: "deposit_observation",
		AggregateID:   obsID,
		Payload:       map[string]any{"observation_id": obsID, "state": "pending"},
		OccurredAt:    oscillationOccurredAt,
		ChainID:       31337,
		BlockNumber:   int64(9000 + step),
		BlockHash:     fmt.Sprintf("0x%064x", 0x9100+step),
		TxHash:        fmt.Sprintf("0x%064x", 0x9200+step),
		LogIndex:      step,
	}
}

// oscRoundSpec is one pre-fixed oscillation round: the fault/recovery
// primitive, the event volume and the identity step never change at run time.
type oscRoundSpec struct {
	round int
	// fault/recovery are the evidence labels of the injected primitive.
	fault    string
	recovery string
	// containerStopStart marks a round whose fault/recovery uses a real
	// container stop/start (not a process SIGSTOP/SIGCONT).
	containerStopStart bool
	// redisDown marks the dual round (Redis container stopped as well).
	redisDown bool
	// events is the number of harness deposit events appended while the
	// dependency is unavailable; the identity pair row is in addition.
	events int
	// identityStep identifies the deterministic same-identity pair.
	identityStep int
	inject       func(context.Context, *Env) error
	recover      func(context.Context, *Env) error
}

// oscillationTimeline is the fixed three-round timeline of this drill.
var oscillationTimeline = []oscRoundSpec{
	{
		round:              1,
		fault:              "kafka_container_stop",
		recovery:           "kafka_container_start_fresh_broker",
		containerStopStart: true,
		events:             3,
		identityStep:       1,
		inject:             func(ctx context.Context, env *Env) error { return env.StopKafka(ctx) },
		recover:            func(ctx context.Context, env *Env) error { return env.StartKafka(ctx) },
	},
	{
		round:        2,
		fault:        "kafka_sigstop",
		recovery:     "kafka_sigcont",
		events:       4,
		identityStep: 2,
		inject:       func(ctx context.Context, env *Env) error { return env.SuspendKafka(ctx) },
		recover:      func(ctx context.Context, env *Env) error { return env.ResumeKafka(ctx) },
	},
	{
		round:              3,
		fault:              "kafka_sigstop+redis_container_stop",
		recovery:           "kafka_sigcont+redis_container_start",
		containerStopStart: true,
		redisDown:          true,
		events:             4,
		identityStep:       3,
		inject:             func(ctx context.Context, env *Env) error { return env.SuspendDual(ctx) },
		recover:            func(ctx context.Context, env *Env) error { return env.ResumeAll(ctx) },
	},
}

// appendOscillationEvent commits one prepared event through the real Append
// path in its own transaction.
func appendOscillationEvent(t *testing.T, ctx context.Context, env *Env, ev events.Event) events.AppendResult {
	t.Helper()
	tx, err := env.Pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin append: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	res, err := events.Append(ctx, tx, ev)
	if err != nil {
		t.Fatalf("append oscillation event %s/%s: %v", ev.AggregateType, ev.AggregateID, err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit append: %v", err)
	}
	return res
}

// appendIdentityPair appends the deterministic fact of one round twice: the
// second Append must be an idempotent no-op with the same identity and payload
// hash (same identity → same bytes, nothing written, no conflict).
func appendIdentityPair(t *testing.T, ctx context.Context, env *Env, step int) events.AppendResult {
	t.Helper()
	first, err := events.NewEvent(oscillationFact(step))
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}
	second, err := events.NewEvent(oscillationFact(step))
	if err != nil {
		t.Fatalf("NewEvent (same fact): %v", err)
	}
	if !bytes.Equal(first.PayloadBytes(), second.PayloadBytes()) || first.PayloadHash() != second.PayloadHash() {
		t.Fatalf("the same fact produced different canonical payload bytes or hashes")
	}
	firstRes := appendOscillationEvent(t, ctx, env, first)
	if firstRes.Noop {
		t.Fatalf("first append of identity step %d reported a no-op", step)
	}
	secondRes := appendOscillationEvent(t, ctx, env, second)
	if !secondRes.Noop || secondRes.EventID != firstRes.EventID ||
		secondRes.OutboxID != firstRes.OutboxID || secondRes.AggregateVersion != firstRes.AggregateVersion {
		t.Fatalf("same-identity re-append was not absorbed: first=%+v second=%+v", firstRes, secondRes)
	}
	var rows int64
	if err := env.Pool.QueryRow(ctx,
		`SELECT count(*) FROM outbox_events WHERE event_id = $1`, firstRes.EventID).Scan(&rows); err != nil {
		t.Fatalf("same-identity row count: %v", err)
	}
	if rows != 1 {
		t.Fatalf("same-identity rows = %d, want 1 (the duplicate append must not write a row)", rows)
	}
	var storedHash string
	if err := env.Pool.QueryRow(ctx,
		`SELECT payload_hash FROM outbox_events WHERE event_id = $1`, firstRes.EventID).Scan(&storedHash); err != nil {
		t.Fatalf("stored payload hash: %v", err)
	}
	if storedHash != first.PayloadHash() {
		t.Fatalf("stored payload_hash = %s, want the canonical hash %s", storedHash, first.PayloadHash())
	}
	return firstRes
}

// readConsumerProgress reads the durable consumer progress rows of the
// reference consumer for the canonical topic.
func readConsumerProgress(t *testing.T, ctx context.Context, env *Env) map[int]int64 {
	t.Helper()
	rows, err := env.Pool.Query(ctx, `
SELECT partition, next_offset FROM consumer_progress
WHERE consumer_name = $1 AND topic = $2 ORDER BY partition`,
		events.RefConsumerName, testutil.KafkaTopic)
	if err != nil {
		t.Fatalf("consumer progress query: %v", err)
	}
	defer rows.Close()
	out := map[int]int64{}
	for rows.Next() {
		var partition int
		var next int64
		if err := rows.Scan(&partition, &next); err != nil {
			t.Fatalf("scan consumer progress: %v", err)
		}
		out[partition] = next
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate consumer progress: %v", err)
	}
	return out
}

// progressMapsEqual reports whether two progress snapshots carry the same
// partitions and offsets.
func progressMapsEqual(a, b map[int]int64) bool {
	if len(a) != len(b) {
		return false
	}
	for partition, offset := range a {
		if b[partition] != offset {
			return false
		}
	}
	return true
}

// assertProgressNotRegressed fails when any partition's durable next_offset
// moved backwards (durable progress is monotonic; FR-15).
func assertProgressNotRegressed(t *testing.T, before, after map[int]int64) {
	t.Helper()
	for partition, prev := range before {
		next, ok := after[partition]
		if !ok {
			t.Fatalf("consumer progress partition %d disappeared (progress must stay durable)", partition)
		}
		if next < prev {
			t.Fatalf("consumer progress partition %d rewound: %d -> %d (progress must be monotonic)", partition, prev, next)
		}
	}
}

// readTopicRecords polls the canonical topic directly (no consumer group, so
// the real group is never disturbed) until it has collected want copies of the
// event's published envelope or the window elapses.
func readTopicRecords(ctx context.Context, env *Env, eventID string, want int) ([]events.Message, bool) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(env.Kafka.Brokers()...),
		kgo.ConsumeTopics(testutil.KafkaTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return nil, false
	}
	defer cl.Close()
	var found []events.Message
	deadline := time.Now().Add(30 * time.Second)
	for len(found) < want && time.Now().Before(deadline) && ctx.Err() == nil {
		fetchCtx, cancel := context.WithTimeout(ctx, time.Second)
		fetches := cl.PollRecords(fetchCtx, 500)
		cancel()
		fetches.EachRecord(func(rec *kgo.Record) {
			if len(found) >= want {
				return
			}
			for _, header := range rec.Headers {
				if header.Key == "event_id" && string(header.Value) == eventID {
					found = append(found, events.Message{
						Topic:     rec.Topic,
						Partition: int(rec.Partition),
						Offset:    rec.Offset,
						Value:     append([]byte(nil), rec.Value...),
					})
					return
				}
			}
		})
	}
	return found, len(found) >= want
}

// assertIdentityBytesAndRedelivery checks the round's identity event end to
// end: every published copy must be byte-identical to the stored outbox row's
// deterministic serialization, and a redelivery of the exact delivered bytes
// must be absorbed as a duplicate without touching the ledger.
func assertIdentityBytesAndRedelivery(t *testing.T, ctx context.Context, env *Env,
	reference *events.ReferenceConsumer, eventID uuid.UUID, round int) {
	t.Helper()
	rec, err := loadOutboxRecord(ctx, env, eventID)
	if err != nil {
		t.Fatalf("round %d load outbox row %s: %v", round, eventID, err)
	}
	want, err := rec.EnvelopeJSON()
	if err != nil {
		t.Fatalf("round %d render stored outbox row %s: %v", round, eventID, err)
	}
	copies, ok := readTopicRecords(ctx, env, eventID.String(), 2)
	if !ok {
		t.Fatalf("round %d: fewer than 2 published copies of %s on the topic (original + duplicate publication)", round, eventID)
	}
	for i, copy := range copies {
		if !bytes.Equal(want, copy.Value) {
			t.Fatalf("round %d: published copy %d of %s is not byte-identical to the stored outbox row serialization", round, i, eventID)
		}
	}
	before, err := reference.LedgerApplications(ctx, eventID)
	if err != nil {
		t.Fatalf("round %d ledger applications of %s: %v", round, eventID, err)
	}
	if before != 1 {
		t.Fatalf("round %d ledger applications of %s before redelivery = %d, want 1", round, eventID, before)
	}
	result, err := reference.Process(ctx, copies[0])
	if err != nil {
		t.Fatalf("round %d redeliver %s: %v", round, eventID, err)
	}
	if result.Outcome != events.OutcomeDuplicate {
		t.Fatalf("round %d redelivered identical publication of %s = %q, want %q",
			round, eventID, result.Outcome, events.OutcomeDuplicate)
	}
	after, err := reference.LedgerApplications(ctx, eventID)
	if err != nil {
		t.Fatalf("round %d ledger applications of %s after redelivery: %v", round, eventID, err)
	}
	if after != 1 {
		t.Fatalf("round %d ledger applications of %s after redelivery = %d, want 1", round, eventID, after)
	}
}

// runOscillationRound drives one pre-fixed round and returns the cumulative
// published row count. Every failure is fatal: a drill that cannot prove its
// assertion never reports a pass.
func runOscillationRound(t *testing.T, ctx context.Context, env *Env, spec oscRoundSpec, publishedBefore int64) int64 {
	t.Helper()
	record := func(kind string, data any) {
		t.Helper()
		if err := env.Evidence.Record(kind, data); err != nil {
			t.Fatalf("round %d: record evidence %s: %v", spec.round, kind, err)
		}
	}
	roundRows := int64(spec.events + 1)
	publishedAfter := publishedBefore + roundRows

	// 0. Pre-round precondition: the previous round is fully caught up.
	preTotals, err := env.OutboxTotals(ctx)
	if err != nil {
		t.Fatalf("round %d pre totals: %v", spec.round, err)
	}
	preGap, err := env.CatchupGap(ctx)
	if err != nil {
		t.Fatalf("round %d pre catch-up gap: %v", spec.round, err)
	}
	preLedger := drillLedgerRows(t, ctx, env)
	if preTotals.Pending != 0 || preTotals.Blocked != 0 || preTotals.Published != publishedBefore ||
		preTotals.Total != publishedBefore || preGap != 0 || preLedger != publishedBefore {
		t.Fatalf("round %d pre-state = pending=%d published=%d total=%d blocked=%d gap=%d ledger=%d, want a caught-up state at %d",
			spec.round, preTotals.Pending, preTotals.Published, preTotals.Total, preTotals.Blocked,
			preGap, preLedger, publishedBefore)
	}
	preProgress := readConsumerProgress(t, ctx, env)

	// 1. Fault injection, then verify the dependency actually went away.
	if err := spec.inject(ctx, env); err != nil {
		t.Fatalf("round %d inject %s: %v", spec.round, spec.fault, err)
	}
	if err := WaitFor(ctx, 90*time.Second, func() (bool, error) {
		return !env.KafkaReachable(ctx), nil
	}); err != nil {
		t.Fatalf("round %d: kafka still reachable after %s: %v", spec.round, spec.fault, err)
	}
	if spec.redisDown {
		if err := WaitFor(ctx, 60*time.Second, func() (bool, error) {
			return !env.RedisPing(ctx), nil
		}); err != nil {
			t.Fatalf("round %d: redis still reachable during the dual fault: %v", spec.round, err)
		}
	}

	// 2. Business continues: events commit through the real Append path while
	// the dependency is unavailable (0 loss; rows stay pending).
	roundEventIDs := make([]uuid.UUID, 0, roundRows)
	for i := 0; i < spec.events; i++ {
		id, err := env.AppendDepositEvent(ctx, 1000*spec.round+i)
		if err != nil {
			t.Fatalf("round %d append event %d during the fault: %v", spec.round, i, err)
		}
		parsed, err := uuid.Parse(id)
		if err != nil {
			t.Fatalf("round %d parse appended event id %q: %v", spec.round, id, err)
		}
		roundEventIDs = append(roundEventIDs, parsed)
	}
	identityAppend := appendIdentityPair(t, ctx, env, spec.identityStep)
	roundEventIDs = append(roundEventIDs, identityAppend.EventID)

	// 3. Durable outage evidence: backlog grows, nothing published/applied,
	// nothing blocked, the oldest wait is a measured positive age and the
	// durable progress does not move backwards.
	outage, err := env.Snapshot(ctx)
	if err != nil {
		t.Fatalf("round %d outage snapshot: %v", spec.round, err)
	}
	if outage.KafkaAvailable {
		t.Fatalf("round %d: %s did not take the broker away (snapshot still reported it available)", spec.round, spec.fault)
	}
	if outage.OutboxPending != roundRows || outage.OutboxPublished != publishedBefore || outage.OutboxBlocked != 0 {
		t.Fatalf("round %d outage snapshot = pending=%d published=%d blocked=%d, want %d/%d/0",
			spec.round, outage.OutboxPending, outage.OutboxPublished, outage.OutboxBlocked, roundRows, publishedBefore)
	}
	if outage.OutboxOldestAgeSeconds <= 0 {
		t.Fatalf("round %d: oldest pending wait = %v, want a measured positive age", spec.round, outage.OutboxOldestAgeSeconds)
	}
	if outage.LedgerRows != publishedBefore {
		t.Fatalf("round %d ledger rows during the outage = %d, want %d (no effect without consumption)",
			spec.round, outage.LedgerRows, publishedBefore)
	}
	if spec.redisDown && env.RedisPing(ctx) {
		t.Fatalf("round %d: redis reported available during the dual fault", spec.round)
	}
	assertProgressNotRegressed(t, preProgress, readConsumerProgress(t, ctx, env))

	// 4. Recovery, then verify both dependencies answer again.
	if err := spec.recover(ctx, env); err != nil {
		t.Fatalf("round %d recover %s: %v", spec.round, spec.recovery, err)
	}
	if err := WaitFor(ctx, 120*time.Second, func() (bool, error) {
		return env.KafkaReachable(ctx), nil
	}); err != nil {
		t.Fatalf("round %d: kafka unreachable after %s: %v", spec.round, spec.recovery, err)
	}
	if err := WaitFor(ctx, 60*time.Second, func() (bool, error) {
		return env.RedisPing(ctx), nil
	}); err != nil {
		t.Fatalf("round %d: redis unreachable after %s: %v", spec.round, spec.recovery, err)
	}
	if err := env.ForceRetry(ctx); err != nil {
		t.Fatalf("round %d force retry: %v", spec.round, err)
	}

	// 5. Bounded publisher drain. The consumer is not running yet, so the
	// catch-up gap right after the drain is exactly this round's backlog: a
	// deterministic decreasing series when the consumer starts.
	const batch = 4
	publishCycles := 0
	overClaim := false
	pubLoop := startPublisherLoop(t, ctx, env, fmt.Sprintf("oscillation-r%d", spec.round), batch, 100*time.Millisecond,
		func(outcome events.PublishOutcome) {
			publishCycles++
			if outcome.Claimed > batch {
				overClaim = true
			}
		})
	drainStart := time.Now()
	if err := WaitFor(ctx, 3*time.Minute, func() (bool, error) {
		pubLoop.check(t)
		snap, err := env.Snapshot(ctx)
		if err != nil {
			return false, err
		}
		return snap.OutboxPending == 0 && snap.OutboxPublished == publishedAfter, nil
	}); err != nil {
		t.Fatalf("round %d publisher recovery drain: %v", spec.round, err)
	}
	drainSeconds := time.Since(drainStart).Seconds()
	pubLoop.stop(t)
	if overClaim {
		t.Fatalf("round %d: publisher claimed more than its bounded batch %d", spec.round, batch)
	}
	if publishCycles == 0 {
		t.Fatalf("round %d: publisher drain recorded no cycle", spec.round)
	}
	gapBefore, err := env.CatchupGap(ctx)
	if err != nil {
		t.Fatalf("round %d catch-up gap before consumer start: %v", spec.round, err)
	}
	if gapBefore != roundRows {
		t.Fatalf("round %d catch-up gap before consumer start = %d, want %d", spec.round, gapBefore, roundRows)
	}
	if ledger := drillLedgerRows(t, ctx, env); ledger != publishedBefore {
		t.Fatalf("round %d ledger rows after the drain = %d, want %d (the consumer has not started yet)",
			spec.round, ledger, publishedBefore)
	}

	// 6. Consumer catch-up from durable PostgreSQL progress: the gap must
	// decrease monotonically to 0 and every event must get exactly one effect.
	consLoop, reference := startConsumerLoop(t, ctx, env)
	gapSeries := []int64{gapBefore}
	progressSamples := []map[int]int64{preProgress}
	catchupStart := time.Now()
	lastCatchupLog := time.Now()
	if err := WaitFor(ctx, 4*time.Minute, func() (bool, error) {
		consLoop.check(t)
		gap, err := env.CatchupGap(ctx)
		if err != nil {
			return false, err
		}
		if gap != gapSeries[len(gapSeries)-1] {
			gapSeries = append(gapSeries, gap)
		}
		progress := readConsumerProgress(t, ctx, env)
		if !progressMapsEqual(progressSamples[len(progressSamples)-1], progress) {
			progressSamples = append(progressSamples, progress)
		}
		if time.Since(lastCatchupLog) > 10*time.Second {
			lastCatchupLog = time.Now()
			t.Logf("round %d catch-up: elapsed=%s ledger=%d gap=%d progress_rows=%d",
				spec.round, time.Since(catchupStart).Round(time.Second), drillLedgerRows(t, ctx, env), gap, countProgressRows(t, ctx, env))
		}
		return drillLedgerRows(t, ctx, env) == publishedAfter && gap == 0, nil
	}); err != nil {
		dumpCatchupDiagnostics(t, ctx, env)
		t.Fatalf("round %d consumer catch-up: %v (ledger=%d gap=%s progress_rows=%d)",
			spec.round, err, drillLedgerRows(t, ctx, env), catchupGapOrError(ctx, env), countProgressRows(t, ctx, env))
	}
	catchupSeconds := time.Since(catchupStart).Seconds()

	// 7. Transport-level duplicate publication must be absorbed. The consumer
	// keeps running: the round's rows are reset to pending (drill-local fault
	// injection) and republished through the real publisher; the group commits
	// must advance past the duplicate records while the ledger stays at one
	// effect per event.
	committedBefore, err := groupCommittedSum(ctx, env, "faultdrill."+events.RefConsumerName)
	if err != nil {
		t.Fatalf("round %d committed offsets before duplicate publication: %v", spec.round, err)
	}
	if _, err := env.Pool.Exec(ctx, `
UPDATE outbox_events
SET publish_state = 'pending', published_at = NULL, claim_owner = NULL,
    claim_expires_at = NULL, next_attempt_at = now()
WHERE event_id = ANY($1::uuid[])`, roundEventIDs); err != nil {
		t.Fatalf("round %d reset rows for duplicate publication: %v", spec.round, err)
	}
	dupPub := startPublisherLoop(t, ctx, env, fmt.Sprintf("oscillation-r%d-duplicate", spec.round), batch, 100*time.Millisecond, nil)
	if err := WaitFor(ctx, 3*time.Minute, func() (bool, error) {
		dupPub.check(t)
		consLoop.check(t)
		var republished int64
		if err := env.Pool.QueryRow(ctx, `
SELECT count(*) FROM outbox_events
WHERE event_id = ANY($1::uuid[]) AND publish_state = 'published' AND attempt_count >= 2`,
			roundEventIDs).Scan(&republished); err != nil {
			return false, err
		}
		return republished == roundRows, nil
	}); err != nil {
		t.Fatalf("round %d duplicate publication did not complete: %v", spec.round, err)
	}
	if err := WaitFor(ctx, 3*time.Minute, func() (bool, error) {
		dupPub.check(t)
		consLoop.check(t)
		sum, err := groupCommittedSum(ctx, env, "faultdrill."+events.RefConsumerName)
		if err != nil {
			return false, err
		}
		totals, err := env.OutboxTotals(ctx)
		if err != nil {
			return false, err
		}
		gap, err := env.CatchupGap(ctx)
		if err != nil {
			return false, err
		}
		return sum >= committedBefore+roundRows && totals.Pending == 0 && gap == 0 &&
			drillLedgerRows(t, ctx, env) == publishedAfter, nil
	}); err != nil {
		t.Fatalf("round %d duplicate publication was not absorbed (committed_before=%d): %v", spec.round, committedBefore, err)
	}
	dupPub.stop(t)
	consLoop.stop(t)

	// 8. Per-round assertions: gap series, monotonic durable progress, exactly
	// one effect per event, identity bytes and redelivery absorption.
	for i := 1; i < len(gapSeries); i++ {
		if gapSeries[i] > gapSeries[i-1] {
			t.Fatalf("round %d catch-up gap increased: %v (the lag must decrease)", spec.round, gapSeries)
		}
	}
	if gapSeries[0] != roundRows || gapSeries[len(gapSeries)-1] != 0 {
		t.Fatalf("round %d catch-up gap series %v must start at %d and end at 0", spec.round, gapSeries, roundRows)
	}
	for i := 1; i < len(progressSamples); i++ {
		assertProgressNotRegressed(t, progressSamples[i-1], progressSamples[i])
	}
	postProgress := progressSamples[len(progressSamples)-1]
	assertProgressNotRegressed(t, preProgress, postProgress)
	if len(postProgress) == 0 {
		t.Fatalf("round %d: no durable consumer progress after catch-up", spec.round)
	}
	assertNoBlocked(t, ctx, env)
	if gap, err := env.CatchupGap(ctx); err != nil || gap != 0 {
		t.Fatalf("round %d terminal catch-up gap = %d (err %v), want 0", spec.round, gap, err)
	}
	assertLedgerExactlyOnce(t, ctx, env, int(publishedAfter))
	for _, id := range roundEventIDs {
		applications, err := reference.LedgerApplications(ctx, id)
		if err != nil {
			t.Fatalf("round %d ledger applications of %s: %v", spec.round, id, err)
		}
		if applications != 1 {
			t.Fatalf("round %d event %s applied %d times, want exactly 1 (no duplicate payment effect)", spec.round, id, applications)
		}
	}
	assertIdentityBytesAndRedelivery(t, ctx, env, reference, identityAppend.EventID, spec.round)

	record("oscillation_round", map[string]any{
		"round":                        spec.round,
		"fault":                        spec.fault,
		"recovery":                     spec.recovery,
		"container_stop_start":         spec.containerStopStart,
		"events":                       roundRows,
		"identity_step":                spec.identityStep,
		"noop_reappend":                1,
		"duplicate_publication":        roundRows,
		"pending_during_fault":         outage.OutboxPending,
		"published_before":             publishedBefore,
		"ledger_before":                preLedger,
		"oldest_pending_age_seconds":   outage.OutboxOldestAgeSeconds,
		"gap_series":                   gapSeries,
		"drain_seconds":                drainSeconds,
		"catchup_seconds":              catchupSeconds,
		"publish_cycles":               publishCycles,
		"committed_offsets_before_dup": committedBefore,
		"progress_before":              preProgress,
		"progress_after":               postProgress,
		"event_ids":                    eventIDStrings(roundEventIDs),
		"identity_event_id":            identityAppend.EventID.String(),
		"envelope_bytes_identical":     true,
		"duplicate_absorbed":           true,
		"effects_per_event":            1,
		"loss":                         0,
		"duplicate_effects":            0,
		"note":                         "drain/catch-up times are measured; no threshold is asserted (数值待测)",
	})
	return publishedAfter
}

// eventIDStrings renders event ids for the evidence record.
func eventIDStrings(ids []uuid.UUID) []string {
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		out = append(out, id.String())
	}
	return out
}

// TestThreeRoundDependencyOscillation is the 013-supplement task-2 acceptance:
// three fixed oscillation rounds (container replacement, SIGSTOP, dual) with
// durable progress, catch-up, identity and exactly-once-effect assertions.
func TestThreeRoundDependencyOscillation(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	env, err := StartEnv(ctx)
	if err != nil {
		t.Fatalf("StartEnv: %v", err)
	}
	t.Cleanup(func() { _ = env.Close(context.Background()) })
	if err := env.EnsureReferenceLedger(ctx); err != nil {
		t.Fatalf("reference ledger schema: %v", err)
	}
	record := func(kind string, data any) {
		t.Helper()
		if err := env.Evidence.Record(kind, data); err != nil {
			t.Fatalf("record evidence %s: %v", kind, err)
		}
	}

	timeline := make([]map[string]any, 0, len(oscillationTimeline))
	for _, spec := range oscillationTimeline {
		timeline = append(timeline, map[string]any{
			"round": spec.round, "fault": spec.fault, "recovery": spec.recovery,
			"container_stop_start": spec.containerStopStart, "redis_down": spec.redisDown,
			"events": spec.events + 1, "identity_step": spec.identityStep,
		})
	}
	record("oscillation_timeline", map[string]any{
		"rounds": timeline,
		"note": "single-node containers: a Stop/Start is a real container replacement, not a cluster election verification; " +
			"the container replacement runs before any durable consumer progress exists (a fresh broker invalidates old Kafka offset coordinates)",
	})

	before, err := env.AuthorityFingerprint(ctx)
	if err != nil {
		t.Fatalf("authority fingerprint before: %v", err)
	}

	// Three fixed rounds; round 1 carries the real container replacement.
	var published int64
	for _, spec := range oscillationTimeline {
		published = runOscillationRound(t, ctx, env, spec, published)
	}

	// Final state: nothing pending or blocked, every row published, the
	// catch-up gap is 0, the zero-invariants hold and no send-side authority
	// changed through delivery.
	final, err := env.OutboxTotals(ctx)
	if err != nil {
		t.Fatalf("final outbox totals: %v", err)
	}
	if final.Pending != 0 || final.Blocked != 0 || final.Published != published || final.Total != published {
		t.Fatalf("final outbox totals = %+v, want all %d published with none pending/blocked", final, published)
	}
	if gap, err := env.CatchupGap(ctx); err != nil || gap != 0 {
		t.Fatalf("final catch-up gap = %d (err %v), want 0", gap, err)
	}
	assertLedgerExactlyOnce(t, ctx, env, int(published))
	assertNoBlocked(t, ctx, env)
	invariants, err := env.CollectInvariants(ctx)
	if err != nil {
		t.Fatalf("CollectInvariants: %v", err)
	}
	AssertInvariants(t, invariants)
	after, err := env.AuthorityFingerprint(ctx)
	if err != nil {
		t.Fatalf("authority fingerprint after: %v", err)
	}
	for table, count := range before {
		if after[table] != count {
			t.Fatalf("%s changed through the oscillation drill: %d -> %d (a delivered event is never a send permission)",
				table, count, after[table])
		}
	}
	record("oscillation_final", map[string]any{
		"total_events":                published,
		"published":                   final.Published,
		"pending":                     final.Pending,
		"blocked":                     final.Blocked,
		"gap":                         0,
		"loss":                        0,
		"duplicate_effects":           0,
		"progress_rows":               countProgressRows(t, ctx, env),
		"invariants":                  invariants,
		"authority_before":            before,
		"authority_after":             after,
		"container_stop_start_rounds": []int{1, 3},
		"note":                        "0 silent loss / 0 duplicate effects; every round reached gap 0; R1 Kafka container Stop/Start and R3 Redis container Stop/Start are single-node, not cluster election verification",
	})
	if err := env.Evidence.WriteFile("metrics.txt", []byte(env.MetricsText())); err != nil {
		t.Fatalf("write metrics evidence: %v", err)
	}
	entries, err := os.ReadDir(env.Evidence.Dir())
	if err != nil || len(entries) == 0 {
		t.Fatalf("evidence directory %q is empty (err=%v)", env.Evidence.Dir(), err)
	}
	t.Logf("evidence written to %s", env.Evidence.Dir())
}
