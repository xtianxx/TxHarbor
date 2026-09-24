//go:build fault

// catchup_test.go is the T073 fault-layer scenario (V-CATCHUP, quickstart Q9):
// recovery drain and consumer catch-up against the real dependencies, with a
// re-failure during catch-up.
//
// Scenario:
//
//  1. normal: real publisher + real group consumer deliver a first event set;
//     every event reaches the simulated ledger exactly once;
//  2. Kafka outage (broker processes suspended: the broker is unavailable
//     while its log and offsets are preserved — the drill's outage primitive;
//     a fresh broker would invalidate the durable resume offset), business
//     continues appending events through the real Append path: they stay
//     pending, nothing is lost and nothing is published;
//  3. recovery: the publisher drains the durable pending set with bounded
//     batches, the consumer catches up from its durable PostgreSQL progress;
//     the catch-up gap (published-not-applied) decreases to 0, the catch-up
//     time is measured and recorded (never fabricated);
//  4. re-failure during catch-up (Kafka suspended + Redis stopped mid-drain):
//     the drill re-pauses safely — 0 loss, 0 duplicate effects, progress
//     resumable, no blocked rows — then recovers a final time and applies
//     every event exactly once.
//
// Evidence: env_spec.json, timeline.jsonl (phases, snapshots, gap series,
// catch-up seconds) and a Prometheus exposition snapshot are written to the
// evidence directory (temp dir unless TXHARBOR_FAULT_EVIDENCE_DIR is set).
// Unmeasured scenario values are reported as "待测" in the summary record, not
// invented.
package faultdrill

import (
	"context"
	"fmt"
	"os"
	"runtime/pprof"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/xtianxx/txharbor/internal/events"
	"github.com/xtianxx/txharbor/internal/testutil"
)

// drillLoop is one running real runtime (publisher or consumer) that the
// scenario can stop deterministically.
type drillLoop struct {
	name   string
	cancel context.CancelFunc
	done   chan error
}

func startPublisherLoop(t *testing.T, ctx context.Context, env *Env, owner string, batch int,
	interval time.Duration, observe func(events.PublishOutcome)) *drillLoop {
	t.Helper()
	pub, err := env.NewPublisher(owner, batch)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	loopCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- RunPublisherLoop(loopCtx, pub, interval, func(outcome events.PublishOutcome) {
			if observe != nil {
				observe(outcome)
			}
		})
	}()
	return &drillLoop{name: owner, cancel: cancel, done: done}
}

func startConsumerLoop(t *testing.T, ctx context.Context, env *Env) (*drillLoop, *events.ReferenceConsumer) {
	t.Helper()
	runtime, reference, err := env.NewConsumerRuntime(ctx)
	if err != nil {
		t.Fatalf("consumer runtime: %v", err)
	}
	loopCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- RunConsumerLoop(loopCtx, runtime) }()
	return &drillLoop{name: "consumer", cancel: cancel, done: done}, reference
}

func (l *drillLoop) stop(t *testing.T) {
	t.Helper()
	l.cancel()
	select {
	case err := <-l.done:
		if err != nil {
			t.Fatalf("%s loop failed: %v", l.name, err)
		}
	case <-time.After(30 * time.Second):
		t.Logf("%s loop did not stop; dumping goroutines to localize the stall", l.name)
		_ = pprof.Lookup("goroutine").WriteTo(os.Stderr, 1)
		t.Fatalf("%s loop did not stop", l.name)
	}
}

// check fails the test as soon as a loop died early: a scenario must never
// wait out its full timeout on a loop that already returned.
func (l *drillLoop) check(t *testing.T) {
	t.Helper()
	select {
	case err := <-l.done:
		l.done <- err
		if err != nil {
			t.Fatalf("%s loop failed: %v", l.name, err)
		}
		t.Fatalf("%s loop exited early", l.name)
	default:
	}
}

// drillLedgerRows counts the simulated-ledger effect rows (the "0 duplicate
// financial effect" evidence).
func drillLedgerRows(t *testing.T, ctx context.Context, env *Env) int64 {
	t.Helper()
	var rows int64
	if err := env.Pool.QueryRow(ctx,
		`SELECT count(*) FROM `+events.RefLedgerTable).Scan(&rows); err != nil {
		t.Fatalf("ledger rows: %v", err)
	}
	return rows
}

// countProgressRows counts the durable consumer progress rows.
func countProgressRows(t *testing.T, ctx context.Context, env *Env) int64 {
	t.Helper()
	var rows int64
	if err := env.Pool.QueryRow(ctx, `SELECT count(*) FROM consumer_progress`).Scan(&rows); err != nil {
		t.Fatalf("progress rows: %v", err)
	}
	return rows
}

// catchupGapOrError renders the catch-up gap for a failure message.
func catchupGapOrError(ctx context.Context, env *Env) string {
	gap, err := env.CatchupGap(ctx)
	if err != nil {
		return "err:" + err.Error()
	}
	return fmt.Sprintf("%d", gap)
}

// dumpCatchupDiagnostics prints the durable progress, the group committed
// offsets, the topic end offsets and the raw topic records on a catch-up
// failure, so a stuck event can be located without another drill cycle.
func dumpCatchupDiagnostics(t *testing.T, ctx context.Context, env *Env) {
	t.Helper()
	rows, err := env.Pool.Query(ctx, `
SELECT o.event_id::text, o.aggregate_id, o.attempt_count
FROM outbox_events o
WHERE o.publish_state = 'published'
  AND NOT EXISTS (SELECT 1 FROM `+events.RefLedgerTable+` l
                  WHERE l.consumer_name = $1 AND l.event_id = o.event_id)`, events.RefConsumerName)
	if err != nil {
		t.Logf("diagnostics: missing events query: %v", err)
	} else {
		for rows.Next() {
			var eventID, aggregateID string
			var attempts int
			if err := rows.Scan(&eventID, &aggregateID, &attempts); err == nil {
				t.Logf("diagnostics: unapplied event=%s aggregate=%s attempts=%d", eventID, aggregateID, attempts)
			}
		}
		rows.Close()
	}
	progress, err := env.Pool.Query(ctx,
		`SELECT partition, next_offset FROM consumer_progress WHERE consumer_name = $1 AND topic = $2 ORDER BY partition`,
		events.RefConsumerName, testutil.KafkaTopic)
	if err != nil {
		t.Logf("diagnostics: progress query: %v", err)
	} else {
		for progress.Next() {
			var partition int
			var next int64
			if err := progress.Scan(&partition, &next); err == nil {
				t.Logf("diagnostics: pg progress partition=%d next=%d", partition, next)
			}
		}
		progress.Close()
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(env.Kafka.Brokers()...))
	if err != nil {
		t.Logf("diagnostics: kafka client: %v", err)
		return
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	if committed, err := adm.FetchOffsets(ctx, "faultdrill."+events.RefConsumerName); err != nil {
		t.Logf("diagnostics: committed offsets: %v", err)
	} else {
		for _, partitions := range committed {
			for partition, offset := range partitions {
				t.Logf("diagnostics: group committed partition=%d at=%d", partition, offset.At)
			}
		}
	}
	if ends, err := adm.ListEndOffsets(ctx, testutil.KafkaTopic); err != nil {
		t.Logf("diagnostics: end offsets: %v", err)
	} else {
		for _, partitions := range ends {
			for partition, listed := range partitions {
				t.Logf("diagnostics: log end partition=%d offset=%d err=%v", partition, listed.Offset, listed.Err)
			}
		}
	}
	t.Logf("diagnostics: metrics begin\n%s\ndiagnostics: metrics end", env.MetricsText())
	raw, err := kgo.NewClient(
		kgo.SeedBrokers(env.Kafka.Brokers()...),
		kgo.ConsumeTopics(testutil.KafkaTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Logf("diagnostics: raw consumer: %v", err)
		return
	}
	defer raw.Close()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		fetchCtx, cancel := context.WithTimeout(ctx, time.Second)
		fetches := raw.PollRecords(fetchCtx, 200)
		cancel()
		fetches.EachRecord(func(rec *kgo.Record) {
			eventID := ""
			for _, h := range rec.Headers {
				if h.Key == "event_id" {
					eventID = string(h.Value)
				}
			}
			t.Logf("diagnostics: topic record partition=%d offset=%d event=%s", rec.Partition, rec.Offset, eventID)
		})
	}
}

// assertLedgerExactlyOnce asserts every appended event has exactly one ledger
// effect row and no event has more than one.
func assertLedgerExactlyOnce(t *testing.T, ctx context.Context, env *Env, total int) {
	t.Helper()
	rows := drillLedgerRows(t, ctx, env)
	if rows != int64(total) {
		t.Fatalf("ledger rows = %d, want %d (0 loss, 0 duplicate effects)", rows, total)
	}
	var duplicated int64
	if err := env.Pool.QueryRow(ctx, `
SELECT count(*) FROM (
  SELECT event_id FROM `+events.RefLedgerTable+` GROUP BY event_id HAVING count(*) > 1
) d`).Scan(&duplicated); err != nil {
		t.Fatalf("duplicate ledger rows: %v", err)
	}
	if duplicated != 0 {
		t.Fatalf("events with duplicate ledger effects = %d, want 0", duplicated)
	}
	var quarantined int64
	if err := env.Pool.QueryRow(ctx, `SELECT count(*) FROM consumer_quarantine WHERE status = 'open'`).Scan(&quarantined); err != nil {
		t.Fatalf("quarantine count: %v", err)
	}
	if quarantined != 0 {
		t.Fatalf("open quarantine rows = %d, want 0", quarantined)
	}
}

func assertNoBlocked(t *testing.T, ctx context.Context, env *Env) {
	t.Helper()
	var blocked int64
	if err := env.Pool.QueryRow(ctx,
		`SELECT count(*) FROM outbox_events WHERE publish_state = 'blocked'`).Scan(&blocked); err != nil {
		t.Fatalf("blocked count: %v", err)
	}
	if blocked != 0 {
		t.Fatalf("blocked outbox rows = %d, want 0", blocked)
	}
}

// stallLogOnce guards the one-shot goroutine dump that localizes a stalled
// poll loop.
var stallLogOnce sync.Once

// TestCatchupRecoveryAndRefailure is the T073 acceptance scenario.
func TestCatchupRecoveryAndRefailure(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	env, err := StartEnv(ctx)
	if err != nil {
		t.Fatalf("StartEnv: %v", err)
	}
	t.Cleanup(func() { _ = env.Close(context.Background()) })

	// Create the reference ledger schema before any snapshot queries it. This
	// is schema-only: it must not build a Kafka runtime (an unclosed runtime
	// client would join the group and stall the real consumer).
	if err := env.EnsureReferenceLedger(ctx); err != nil {
		t.Fatalf("reference ledger: %v", err)
	}

	record := func(kind string, data any) {
		t.Helper()
		if err := env.Evidence.Record(kind, data); err != nil {
			t.Fatalf("record evidence %s: %v", kind, err)
		}
	}
	snapshot := func(label string) Snapshot {
		t.Helper()
		snap, err := env.Snapshot(ctx)
		if err != nil {
			t.Fatalf("snapshot %s: %v", label, err)
		}
		record("snapshot:"+label, snap)
		return snap
	}

	// --- phase 1: normal delivery -------------------------------------------
	const initial = 6
	for i := 0; i < initial; i++ {
		if _, err := env.AppendDepositEvent(ctx, i); err != nil {
			t.Fatalf("append initial event %d: %v", i, err)
		}
	}
	pubLoop := startPublisherLoop(t, ctx, env, "catchup-normal", 10, 100*time.Millisecond, nil)
	consLoop, _ := startConsumerLoop(t, ctx, env)
	started := time.Now()
	if err := WaitFor(ctx, 4*time.Minute, func() (bool, error) {
		pubLoop.check(t)
		consLoop.check(t)
		pending, err := env.Snapshot(ctx)
		if err != nil {
			return false, err
		}
		rows := drillLedgerRows(t, ctx, env)
		if time.Since(started) > 20*time.Second {
			stallLogOnce.Do(func() {
				t.Logf("phase 1 stalled: pending=%d published=%d blocked=%d ledger=%d",
					pending.OutboxPending, pending.OutboxPublished, pending.OutboxBlocked, rows)
				_ = pprof.Lookup("goroutine").WriteTo(os.Stderr, 1)
			})
		}
		return pending.OutboxPending == 0 && rows == initial, nil
	}); err != nil {
		t.Fatalf("phase 1 delivery: %v", err)
	}
	assertLedgerExactlyOnce(t, ctx, env, initial)
	assertNoBlocked(t, ctx, env)
	pubLoop.stop(t)
	consLoop.stop(t)
	normal := snapshot("phase1_normal")
	if !normal.KafkaAvailable {
		t.Fatal("phase 1: broker reported unavailable")
	}
	record("phase1_assessment", map[string]any{
		"delivered": initial, "loss": 0, "duplicate_effects": 0,
	})

	// --- phase 2: Kafka outage, business continues ---------------------------
	if err := env.SuspendKafka(ctx); err != nil {
		t.Fatalf("suspend kafka: %v", err)
	}
	const outageEvents = 4
	for i := 0; i < outageEvents; i++ {
		if _, err := env.AppendDepositEvent(ctx, 100+i); err != nil {
			t.Fatalf("append outage event %d: %v", i, err)
		}
	}
	outage := snapshot("phase2_kafka_outage")
	if outage.KafkaAvailable {
		t.Fatal("phase 2: suspended broker still reported available")
	}
	if outage.OutboxPending != outageEvents {
		t.Fatalf("pending during outage = %d, want %d", outage.OutboxPending, outageEvents)
	}
	if outage.OutboxPublished != initial || outage.OutboxBlocked != 0 {
		t.Fatalf("outage snapshot = %+v, want published=%d blocked=0", outage, initial)
	}
	if rows := drillLedgerRows(t, ctx, env); rows != initial {
		t.Fatalf("ledger rows during outage = %d, want %d (business did not depend on Kafka)", rows, initial)
	}
	// Outbox observability: the oldest pending wait is a real measured age,
	// and the Kafka-only outage leaves Redis untouched (the event pipeline
	// must not depend on it).
	if outage.OutboxOldestAgeSeconds <= 0 {
		t.Fatalf("oldest pending wait = %v, want a measured positive age", outage.OutboxOldestAgeSeconds)
	}
	if !env.RedisPing(ctx) {
		t.Fatal("redis reported unavailable during the Kafka-only outage")
	}
	// A suspended broker never drains: the publisher releases transient
	// failures, rows stay pending (0 loss). The cycle is wall-clock bounded:
	// a publish that never returns is a drill failure, not a silent hang.
	pubOnly, err := env.NewPublisher("catchup-outage-probe", 5)
	if err != nil {
		t.Fatalf("outage publisher: %v", err)
	}
	if _, err := PublishOnceBounded(ctx, pubOnly, 30*time.Second); err != nil {
		t.Fatalf("bounded PublishOnce during outage: %v", err)
	}
	if snap, err := env.Snapshot(ctx); err != nil || snap.OutboxPending != outageEvents || snap.OutboxPublished != initial {
		t.Fatalf("after failed publish: pending=%+v err=%v, want %d pending/%d published", snap, err, outageEvents, initial)
	}
	record("phase2_assessment", map[string]any{
		"pending": outageEvents, "published": initial, "loss": 0,
		"oldest_pending_age_seconds": outage.OutboxOldestAgeSeconds,
		"kafka_available":            false, "redis_available": true,
	})

	// --- phase 3: recovery and catch-up --------------------------------------
	recoveryStart := time.Now().UTC()
	if err := env.ResumeKafka(ctx); err != nil {
		t.Fatalf("resume kafka: %v", err)
	}
	if err := env.ForceRetry(ctx); err != nil {
		t.Fatalf("force retry: %v", err)
	}
	publishCycles := 0
	unbounded := false
	pubLoop = startPublisherLoop(t, ctx, env, "catchup-recovery", 10, 100*time.Millisecond, func(outcome events.PublishOutcome) {
		publishCycles++
		if outcome.Claimed > 10 {
			unbounded = true
		}
	})
	if err := WaitFor(ctx, 3*time.Minute, func() (bool, error) {
		pubLoop.check(t)
		snap, err := env.Snapshot(ctx)
		if err != nil {
			return false, err
		}
		return snap.OutboxPublished == initial+outageEvents, nil
	}); err != nil {
		t.Fatalf("publisher recovery drain: %v", err)
	}
	if unbounded {
		t.Fatal("publisher claimed more than its configured batch during recovery")
	}
	pubLoop.stop(t)
	if publishCycles == 0 {
		t.Fatal("publisher drain recorded no cycle")
	}
	gap, err := env.CatchupGap(ctx)
	if err != nil {
		t.Fatalf("catch-up gap: %v", err)
	}
	if gap != outageEvents {
		t.Fatalf("catch-up gap before consumer start = %d, want %d", gap, outageEvents)
	}
	gapSamples := []int64{gap}
	record("phase3_gap_sample", map[string]any{"gap": gap, "stage": "published_not_applied"})
	consLoop, _ = startConsumerLoop(t, ctx, env)
	catchupStart := time.Now()
	lastCatchupLog := time.Now()
	if err := WaitFor(ctx, 4*time.Minute, func() (bool, error) {
		consLoop.check(t)
		rows := drillLedgerRows(t, ctx, env)
		remaining, err := env.CatchupGap(ctx)
		if err != nil {
			return false, err
		}
		if gapSamples[len(gapSamples)-1] != remaining {
			gapSamples = append(gapSamples, remaining)
		}
		if time.Since(lastCatchupLog) > 10*time.Second {
			lastCatchupLog = time.Now()
			t.Logf("catch-up progress: elapsed=%s ledger=%d gap=%d progress_rows=%d",
				time.Since(catchupStart).Round(time.Second), rows, remaining, countProgressRows(t, ctx, env))
		}
		return rows == initial+outageEvents && remaining == 0, nil
	}); err != nil {
		dumpCatchupDiagnostics(t, ctx, env)
		t.Fatalf("consumer catch-up: %v (ledger=%d gap=%v progress_rows=%d)",
			err, drillLedgerRows(t, ctx, env), catchupGapOrError(ctx, env), countProgressRows(t, ctx, env))
	}
	consLoop.stop(t)
	catchupSeconds := time.Since(recoveryStart).Seconds()
	for i := 1; i < len(gapSamples); i++ {
		if gapSamples[i] > gapSamples[i-1] {
			t.Fatalf("catch-up gap increased: %v (lag must decrease)", gapSamples)
		}
	}
	if gapSamples[0] == 0 || gapSamples[len(gapSamples)-1] != 0 {
		t.Fatalf("catch-up gap series %v must start > 0 and end at 0", gapSamples)
	}
	assertLedgerExactlyOnce(t, ctx, env, initial+outageEvents)
	assertNoBlocked(t, ctx, env)
	catchup := snapshot("phase3_catchup")
	record("phase3_assessment", map[string]any{
		"catchup_seconds": catchupSeconds,
		"gap_series":      gapSamples,
		"publish_cycles":  publishCycles,
		"pending":         catchup.OutboxPending,
		"note":            "catch-up time is a measured value; no target threshold is asserted (数值待测)",
	})
	if catchup.OutboxPending != 0 || catchup.OutboxPublished != initial+outageEvents {
		t.Fatalf("catch-up snapshot = %+v, want drained", catchup)
	}

	// --- phase 4: re-failure during catch-up ---------------------------------
	// The catch-up is interrupted mid-flight: the backlog is accrued, the
	// catch-up starts for real, and then the dependency fails before the
	// catch-up can finish. The durable state must show a safe re-pause (work
	// stays pending/published, nothing blocked, progress resumable) and the
	// final recovery must apply every event exactly once.
	const refailureEvents = 20
	if err := env.SuspendDual(ctx); err != nil {
		t.Fatalf("suspend dual: %v", err)
	}
	for i := 0; i < refailureEvents; i++ {
		if _, err := env.AppendDepositEvent(ctx, 200+i); err != nil {
			t.Fatalf("append re-failure event %d: %v", i, err)
		}
	}
	preFailure := snapshot("phase4_backlog")
	if preFailure.OutboxPending != refailureEvents || preFailure.KafkaAvailable {
		t.Fatalf("re-failure backlog snapshot = %+v, want %d pending and no broker", preFailure, refailureEvents)
	}
	if preFailure.OutboxOldestAgeSeconds <= 0 {
		t.Fatalf("oldest pending wait at the second fault = %v, want a measured positive age", preFailure.OutboxOldestAgeSeconds)
	}
	if env.RedisPing(ctx) {
		t.Fatal("redis reported available during the dual fault")
	}
	// Catch-up starts (bounded publisher batches, consumer kept running).
	if err := env.ResumeAll(ctx); err != nil {
		t.Fatalf("resume dual: %v", err)
	}
	if err := env.ForceRetry(ctx); err != nil {
		t.Fatalf("force retry: %v", err)
	}
	pubLoop = startPublisherLoop(t, ctx, env, "catchup-refailure", 1, 300*time.Millisecond, nil)
	consLoop, _ = startConsumerLoop(t, ctx, env)
	if err := WaitFor(ctx, 2*time.Minute, func() (bool, error) {
		pubLoop.check(t)
		consLoop.check(t)
		snap, err := env.Snapshot(ctx)
		if err != nil {
			return false, err
		}
		return snap.OutboxPublished > initial+outageEvents, nil
	}); err != nil {
		t.Fatalf("re-failure drain did not start: %v", err)
	}
	// Interrupt the catch-up: stop the publisher cleanly while the broker is
	// still up, then fail the dependency with work still outstanding. The
	// consumer stays running through the fault and must recover with it.
	pubLoop.stop(t)
	if err := env.SuspendDual(ctx); err != nil {
		t.Fatalf("second suspend: %v", err)
	}
	reFailed := snapshot("phase4_refailure")
	if reFailed.OutboxBlocked != 0 {
		t.Fatalf("blocked after re-failure = %d, want 0", reFailed.OutboxBlocked)
	}
	if reFailed.OutboxPending == 0 {
		t.Fatalf("re-failure snapshot = %+v, want remaining pending work (safe re-pause)", reFailed)
	}
	totalRows := int64(initial + outageEvents + refailureEvents)
	if reFailed.OutboxPending+reFailed.OutboxPublished != totalRows {
		t.Fatalf("rows after re-failure = pending %d + published %d, want %d (0 loss)",
			reFailed.OutboxPending, reFailed.OutboxPublished, totalRows)
	}
	rowsAfterFailure := drillLedgerRows(t, ctx, env)
	if rowsAfterFailure > totalRows {
		t.Fatalf("ledger rows after re-failure = %d > %d (duplicate effects)", rowsAfterFailure, totalRows)
	}
	var progressRows int64
	if err := env.Pool.QueryRow(ctx, `SELECT count(*) FROM consumer_progress`).Scan(&progressRows); err != nil {
		t.Fatalf("progress rows after re-failure: %v", err)
	}
	if progressRows == 0 {
		t.Fatal("consumer progress rows disappeared after the re-failure (progress must stay resumable)")
	}
	record("phase4_assessment", map[string]any{
		"pending": reFailed.OutboxPending, "published": reFailed.OutboxPublished,
		"ledger_rows": rowsAfterFailure, "blocked": reFailed.OutboxBlocked,
		"progress_rows": progressRows, "loss": 0, "duplicate_effects": 0,
		"kafka_available": false, "redis_available": false,
	})

	// Final recovery: catch up completely. The consumer loop kept running
	// through the fault and must recover with the broker (no restart needed);
	// the publisher is restarted over the recovered dependency.
	if err := env.ResumeAll(ctx); err != nil {
		t.Fatalf("final resume: %v", err)
	}
	if !env.RedisPing(ctx) {
		t.Fatal("redis did not recover after ResumeAll")
	}
	if err := env.ForceRetry(ctx); err != nil {
		t.Fatalf("final force retry: %v", err)
	}
	pubLoop = startPublisherLoop(t, ctx, env, "catchup-final", 4, 100*time.Millisecond, nil)
	if err := WaitFor(ctx, 4*time.Minute, func() (bool, error) {
		pubLoop.check(t)
		consLoop.check(t)
		snap, err := env.Snapshot(ctx)
		if err != nil {
			return false, err
		}
		return snap.OutboxPending == 0 && drillLedgerRows(t, ctx, env) == totalRows, nil
	}); err != nil {
		t.Fatalf("final recovery: %v", err)
	}
	pubLoop.stop(t)
	consLoop.stop(t)
	assertLedgerExactlyOnce(t, ctx, env, int(totalRows))
	assertNoBlocked(t, ctx, env)
	final := snapshot("final")
	if final.OutboxPending != 0 || final.OutboxPublished != totalRows {
		t.Fatalf("final snapshot = %+v, want all %d published", final, totalRows)
	}
	record("final_assessment", map[string]any{
		"total_events": totalRows, "loss": 0, "duplicate_effects": 0,
		"blocked": 0, "catchup_seconds": catchupSeconds,
		"note": "0 loss / 0 duplicate financial effects across recovery and re-failure; catch-up time measured, threshold 待测",
	})
	if err := env.Evidence.WriteFile("metrics.txt", []byte(env.MetricsText())); err != nil {
		t.Fatalf("write metrics evidence: %v", err)
	}
	entries, err := os.ReadDir(env.Evidence.Dir())
	if err != nil || len(entries) == 0 {
		t.Fatalf("evidence directory %q is empty (err=%v)", env.Evidence.Dir(), err)
	}
	t.Logf("evidence written to %s", env.Evidence.Dir())

	// The drill must leave a resumable, non-blocked state.
	if gap, err := env.CatchupGap(ctx); err != nil || gap != 0 {
		t.Fatalf("terminal catch-up gap = %d (err=%v), want 0", gap, err)
	}
}
