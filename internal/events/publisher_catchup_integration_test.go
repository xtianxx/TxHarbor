//go:build integration_kafka

// publisher_catchup_integration_test.go is the T074 Integration-Kafka layer
// (V-CATCHUP, quickstart Q2/Q9): the publisher's recovery drain from the
// persistent pending set against a real broker, with bounded batches, ack
// stability, transient backoff for unacknowledged rows, a second outage with
// 0 loss and outbox observability during the whole drain. It runs through
// `make test-integration-kafka`.
//
// Statement discipline: delivery is at-least-once and processing is
// idempotent; this layer never claims a cross-system exactly-once guarantee.
package events

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"

	"github.com/xtianxx/txharbor/internal/testutil"
)

// catchupCount runs one count query.
func catchupCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sql string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(ctx, sql, args...).Scan(&n); err != nil {
		t.Fatalf("count (%s): %v", sql, err)
	}
	return n
}

// publishBounded runs one publish cycle with a hard wall-clock bound: a
// produce against an unreachable broker must fail (or succeed) within the
// sink's configured timeouts; a cycle that never returns is a drill failure,
// reported loudly instead of hanging the run.
func publishBounded(t *testing.T, ctx context.Context, pub *Publisher, limit time.Duration) PublishOutcome {
	t.Helper()
	type result struct {
		outcome PublishOutcome
		err     error
	}
	ch := make(chan result, 1)
	go func() {
		outcome, err := pub.PublishOnce(ctx)
		ch <- result{outcome, err}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("PublishOnce: %v", r.err)
		}
		return r.outcome
	case <-time.After(limit):
		t.Fatalf("PublishOnce did not return within %s (publish must stay bounded)", limit)
		return PublishOutcome{}
	}
}

// t074Publisher builds the publisher with a flat, drill-sized retry backoff.
// The outage phase uses a long backoff so released rows cannot be re-claimed
// before every fresh row had its first claim; the drain phase uses a short one
// so a transient post-recovery failure cannot stall the drain.
func t074Publisher(t *testing.T, pool *pgxpool.Pool, sink PublishSink, owner string, batch int, backoff time.Duration) *Publisher {
	t.Helper()
	pub, err := NewPublisher(pool, sink, owner, PublisherOptions{
		Batch:       batch,
		LeaseTTL:    30 * time.Second,
		BackoffBase: backoff,
		BackoffMax:  backoff,
		Jitter:      func() float64 { return 0 },
	})
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	return pub
}

// catchupObserver records the publisher's outbox observations per cycle.
type catchupObserver struct {
	mu      sync.Mutex
	pending map[string]int
	oldest  map[string]float64
	cycles  int
}

func newCatchupObserver() *catchupObserver {
	return &catchupObserver{pending: map[string]int{}, oldest: map[string]float64{}}
}

func (o *catchupObserver) ObserveOutboxPublishFailure(string) {}
func (o *catchupObserver) ObserveOutboxPublished(int)         {}
func (o *catchupObserver) ObserveOutboxAttempts(int)          {}
func (o *catchupObserver) SetOutboxBlocked(int)               {}

func (o *catchupObserver) SetOutboxPending(family string, count int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.pending[family] = count
	o.cycles++
}

func (o *catchupObserver) SetOutboxPendingOldestAge(family string, seconds float64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.oldest[family] = seconds
}

func (o *catchupObserver) pendingTotal() map[string]int {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make(map[string]int, len(o.pending))
	for k, v := range o.pending {
		out[k] = v
	}
	return out
}

// TestPublisherCatchupDrainBoundedAndRefailure is the T074 acceptance.
func TestPublisherCatchupDrainBoundedAndRefailure(t *testing.T) {
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

	// --- backlog while the broker is down -----------------------------------
	const initial = 7
	const batch = 2
	want := map[string]int{}
	for i := 0; i < initial; i++ {
		_, outboxID := appendKafkaEvent(t, pool, 300+i)
		want[readOutboxEventID(t, pool, outboxID)] = 1
	}
	if err := k.Stop(ctx); err != nil {
		t.Fatalf("Stop kafka: %v", err)
	}
	observer := newCatchupObserver()
	// Outage phase: a long flat backoff keeps one failing cycle from
	// re-claiming a row before every fresh row had its first claim.
	pubOutage := t074Publisher(t, pool, sink, "owner-t074-outage", batch, time.Minute)
	// Drain phase: a short bounded backoff.
	pub := t074Publisher(t, pool, sink, "owner-t074", batch, 3*time.Second)
	pub.Observer = observer

	// First outage cycle: the claimed rows are unacknowledged, so they return
	// to the retry queue with a future backoff (never dropped, never blocked).
	first := publishBounded(t, ctx, pubOutage, 30*time.Second)
	if first.Claimed != batch || first.Acked != 0 || first.Released != batch || first.Blocked != 0 {
		t.Fatalf("first outage cycle = %+v, want %d claimed/0 acked/%d released/0 blocked",
			first, batch, batch)
	}
	if future := catchupCount(t, ctx, pool,
		`SELECT count(*) FROM outbox_events WHERE publish_state = 'pending' AND next_attempt_at > now()`); future != batch {
		t.Fatalf("unacked rows scheduled with a future backoff = %d, want %d", future, batch)
	}
	// Drain the remaining backlog through further failing cycles (bounded).
	for cycle := 0; cycle < 10; cycle++ {
		if unclaimed := catchupCount(t, ctx, pool,
			`SELECT count(*) FROM outbox_events WHERE publish_state = 'pending' AND attempt_count = 0`); unclaimed == 0 {
			break
		}
		outcome := publishBounded(t, ctx, pubOutage, 30*time.Second)
		if outcome.Claimed > batch || outcome.Acked != 0 || outcome.Released != outcome.Claimed || outcome.Blocked != 0 {
			t.Fatalf("outage cycle %d = %+v, want <= %d claimed, 0 acked, all claimed released, 0 blocked",
				cycle, outcome, batch)
		}
	}
	var published, pending, claimedAtLeastOnce, scheduled int64
	if err := pool.QueryRow(ctx, `
SELECT count(*) FILTER (WHERE publish_state = 'published'),
       count(*) FILTER (WHERE publish_state = 'pending'),
       count(*) FILTER (WHERE publish_state = 'pending' AND attempt_count >= 1),
       count(*) FILTER (WHERE publish_state = 'pending' AND next_attempt_at >= created_at)
FROM outbox_events`).Scan(&published, &pending, &claimedAtLeastOnce, &scheduled); err != nil {
		t.Fatalf("outage counts: %v", err)
	}
	if published != 0 || pending != initial || claimedAtLeastOnce != initial || scheduled != initial {
		t.Fatalf("after outage: published=%d pending=%d claimed=%d scheduled=%d, want 0/%d/%d/%d",
			published, pending, claimedAtLeastOnce, scheduled, initial, initial, initial)
	}
	if blocked := catchupCount(t, ctx, pool, `SELECT count(*) FROM outbox_events WHERE publish_state = 'blocked'`); blocked != 0 {
		t.Fatalf("blocked during outage = %d, want 0", blocked)
	}

	// --- recovery: bounded drain from the persistent pending set ------------
	if err := k.Start(ctx); err != nil {
		t.Fatalf("Start kafka: %v", err)
	}
	if err := k.EnsureTopic(ctx, testutil.KafkaTopic, testutil.KafkaPartitions); err != nil {
		t.Fatalf("EnsureTopic after recovery: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE outbox_events SET next_attempt_at = now() WHERE publish_state = 'pending'`); err != nil {
		t.Fatalf("force retry: %v", err)
	}
	curve := []int64{pending}
	drainObservations := 0
	for cycle := 0; cycle < 100; cycle++ {
		currentPending := catchupCount(t, ctx, pool, `SELECT count(*) FROM outbox_events WHERE publish_state = 'pending'`)
		if currentPending == 0 {
			break
		}
		before := catchupCount(t, ctx, pool, `SELECT count(*) FROM outbox_events WHERE publish_state = 'published'`)
		outcome := publishBounded(t, ctx, pub, 30*time.Second)
		if outcome.Claimed > batch {
			t.Fatalf("cycle claimed %d rows, want <= batch %d (bounded drain)", outcome.Claimed, batch)
		}
		after := catchupCount(t, ctx, pool, `SELECT count(*) FROM outbox_events WHERE publish_state = 'published'`)
		if after < before || after > int64(initial) {
			t.Fatalf("published moved %d -> %d, want monotonic and <= %d", before, after, initial)
		}
		newPending := currentPending - (after - before)
		if newPending > currentPending {
			t.Fatalf("pending moved %d -> %d (must never grow during the drain)", currentPending, newPending)
		}
		curve = append(curve, newPending)
		if newPending > 0 {
			// Observability during the drain: every remaining pending row is
			// reported by family (outbox_pending_count rate 100%).
			if err := pub.RefreshGauges(ctx); err != nil {
				t.Fatalf("RefreshGauges: %v", err)
			}
			observed := observer.pendingTotal()
			var observedTotal int
			for _, count := range observed {
				observedTotal += count
			}
			if observedTotal == 0 && len(observed) == 0 {
				t.Fatalf("cycle %d: no pending family observed while pending=%d", cycle, newPending)
			}
			if int64(observedTotal) != newPending {
				t.Fatalf("cycle %d: observed pending=%d, want %d", cycle, observedTotal, newPending)
			}
			if len(observed) == 0 {
				t.Fatalf("cycle %d: no family observed while pending=%d", cycle, newPending)
			}
			drainObservations++
		}
		if outcome.Claimed == 0 {
			time.Sleep(200 * time.Millisecond)
		}
	}
	if remaining := catchupCount(t, ctx, pool, `SELECT count(*) FROM outbox_events WHERE publish_state = 'pending'`); remaining != 0 {
		t.Fatalf("pending after drain = %d, want 0", remaining)
	}
	if drainObservations == 0 {
		t.Fatal("the drain recorded no outbox observation while work was pending")
	}

	// Ack stability: acked rows stay published, keep attempt_count 1 and are
	// never claimed again.
	rows, err := pool.Query(ctx, `SELECT id FROM outbox_events WHERE publish_state = 'published' ORDER BY id`)
	if err != nil {
		t.Fatalf("read published ids: %v", err)
	}
	var ackedIDs []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			t.Fatalf("scan published id: %v", err)
		}
		ackedIDs = append(ackedIDs, id)
	}
	rows.Close()
	if len(ackedIDs) != initial {
		t.Fatalf("published rows = %d, want %d", len(ackedIDs), initial)
	}
	beforeAttempts := make(map[int64]int, len(ackedIDs))
	for _, id := range ackedIDs {
		row := readKafkaOutboxRow(t, pool, id)
		if row.publishState != string(PublishStatePublished) || row.attemptCount < 1 {
			t.Fatalf("acked row %d = %+v, want published with attempt_count >= 1", id, row)
		}
		beforeAttempts[id] = row.attemptCount
	}
	idleOutcome := publishBounded(t, ctx, pub, 30*time.Second)
	if idleOutcome.Claimed != 0 {
		t.Fatalf("idle cycle claimed %d published rows, want 0 (ack stability)", idleOutcome.Claimed)
	}
	for _, id := range ackedIDs {
		row := readKafkaOutboxRow(t, pool, id)
		if row.publishState != string(PublishStatePublished) || row.attemptCount != beforeAttempts[id] {
			t.Fatalf("acked row %d = %+v, want published and unchanged (attempts before=%d)",
				id, row, beforeAttempts[id])
		}
	}
	t.Logf("drain curve (pending per cycle, batch %d): %v", batch, curve)

	// --- second outage: 0 loss, then a final drain ---------------------------
	const second = 4
	for i := 0; i < second; i++ {
		_, outboxID := appendKafkaEvent(t, pool, 400+i)
		want[readOutboxEventID(t, pool, outboxID)] = 1
	}
	// The second outage suspends the broker process (log and topic state
	// preserved). The publisher must be able to make progress again after
	// recovery; during the outage this layer asserts the durable state only
	// (the first outage already proved the bounded release of a failing
	// publish cycle).
	if err := k.Suspend(ctx); err != nil {
		t.Fatalf("second Suspend kafka: %v", err)
	}
	if secondPending := catchupCount(t, ctx, pool, `SELECT count(*) FROM outbox_events WHERE publish_state = 'pending'`); secondPending != second {
		t.Fatalf("pending after second outage = %d, want %d", secondPending, second)
	}
	if blockedNow := catchupCount(t, ctx, pool, `SELECT count(*) FROM outbox_events WHERE publish_state = 'blocked'`); blockedNow != 0 {
		t.Fatalf("blocked after second outage = %d, want 0", blockedNow)
	}
	if publishedBefore := catchupCount(t, ctx, pool, `SELECT count(*) FROM outbox_events WHERE publish_state = 'published'`); publishedBefore != initial {
		t.Fatalf("published after second outage = %d, want %d (0 loss of acked rows)", publishedBefore, initial)
	}
	totalRows := int64(initial + second)
	if all := catchupCount(t, ctx, pool, `SELECT count(*) FROM outbox_events`); all != totalRows {
		t.Fatalf("outbox rows = %d, want %d (rows are never deleted)", all, totalRows)
	}
	if err := k.Resume(ctx); err != nil {
		t.Fatalf("final Resume kafka: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE outbox_events SET next_attempt_at = now() WHERE publish_state = 'pending'`); err != nil {
		t.Fatalf("final force retry: %v", err)
	}
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		if catchupCount(t, ctx, pool, `SELECT count(*) FROM outbox_events WHERE publish_state = 'pending'`) == 0 {
			break
		}
		publishBounded(t, ctx, pub, 30*time.Second)
		time.Sleep(500 * time.Millisecond)
	}
	if remaining := catchupCount(t, ctx, pool, `SELECT count(*) FROM outbox_events WHERE publish_state <> 'published'`); remaining != 0 {
		t.Fatalf("pending after final drain = %d, want 0 (0 loss)", remaining)
	}
	if blocked := catchupCount(t, ctx, pool, `SELECT count(*) FROM outbox_events WHERE publish_state = 'blocked'`); blocked != 0 {
		t.Fatalf("blocked after final drain = %d, want 0", blocked)
	}
	// Every event reached the real broker at least once (duplicates allowed).
	records := consumeByEventID(ctx, t, k.Brokers(), testutil.KafkaTopic, want)
	if len(records) != len(want) {
		t.Fatalf("broker records for %d events, want %d", len(records), len(want))
	}
}
