//go:build integration

// publisher_integration_test.go is the T036 Integration-PG layer (V-PUBLISHER,
// PG half): claim mutual exclusion, lease-expiry takeover, the owner-guarded
// ack, the crash-point matrix (before/after claim, before/after ack), the
// bounded transient backoff, blocked visibility/auditability and the rule that
// uncommitted rows are never published and no row lock is held during a
// network publish. It runs against the real 000015 migration through
// `make test-integration`.
package events

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// recordingSink records one delivery per outbox id, can inject per-record
// failures and can run an assertion hook inside Publish.
type recordingSink struct {
	mu        sync.Mutex
	delivered map[int64]int
	fail      func(rec OutboxRecord) error
	hook      func(ctx context.Context, records []OutboxRecord)
}

func newRecordingSink() *recordingSink {
	return &recordingSink{delivered: map[int64]int{}}
}

func (s *recordingSink) Publish(ctx context.Context, records []OutboxRecord) PublishResult {
	result := PublishResult{Failures: map[int64]error{}}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.hook != nil {
		s.hook(ctx, records)
	}
	for _, rec := range records {
		if s.fail != nil {
			if err := s.fail(rec); err != nil {
				result.Failures[rec.OutboxID] = err
				continue
			}
		}
		s.delivered[rec.OutboxID]++
		result.Acked = append(result.Acked, rec.OutboxID)
	}
	return result
}

func (s *recordingSink) deliveryCount(id int64) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.delivered[id]
}

// countingPublishObserver records the publisher observations.
type countingPublishObserver struct {
	mu        sync.Mutex
	failures  []string
	published int
	attempts  int
	blocked   int
	pending   map[string]int
	oldest    map[string]float64
}

func newCountingPublishObserver() *countingPublishObserver {
	return &countingPublishObserver{pending: map[string]int{}, oldest: map[string]float64{}}
}

func (o *countingPublishObserver) ObserveOutboxPublishFailure(class string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.failures = append(o.failures, class)
}

func (o *countingPublishObserver) ObserveOutboxPublished(n int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.published += n
}

func (o *countingPublishObserver) ObserveOutboxAttempts(n int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.attempts += n
}

func (o *countingPublishObserver) SetOutboxBlocked(n int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.blocked = n
}

func (o *countingPublishObserver) SetOutboxPending(family string, count int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.pending[family] = count
}

func (o *countingPublishObserver) SetOutboxPendingOldestAge(family string, seconds float64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.oldest[family] = seconds
}

// appendPublisherEvent inserts one committed deposit observation event through
// the real Append path and returns its observation id and outbox id.
func appendPublisherEvent(t *testing.T, pool *pgxpool.Pool, n int) (string, int64) {
	t.Helper()
	ctx := context.Background()
	obsID := fmt.Sprintf("t036-obs-%d", n)
	ev := depositCreatedEvent(t, obsID, fmt.Sprintf("0x%064x", n), fmt.Sprintf("0x%064x", n+1), n, "pending")
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

// outboxRow is the observable state of one outbox row.
type outboxRow struct {
	publishState string
	claimOwner   *string
	attemptCount int
	lastError    *string
	nextAttempt  time.Time
	publishedAt  *time.Time
}

func readOutboxRow(t *testing.T, pool *pgxpool.Pool, id int64) outboxRow {
	t.Helper()
	var row outboxRow
	if err := pool.QueryRow(context.Background(), `
		SELECT publish_state, claim_owner, attempt_count, last_error_class, next_attempt_at, published_at
		FROM outbox_events WHERE id = $1`, id).
		Scan(&row.publishState, &row.claimOwner, &row.attemptCount, &row.lastError, &row.nextAttempt, &row.publishedAt); err != nil {
		t.Fatalf("read outbox row %d: %v", id, err)
	}
	return row
}

// newTestPublisher builds a publisher over the migrated pool.
func newTestPublisher(t *testing.T, pool *pgxpool.Pool, sink PublishSink, owner string, batch int, lease time.Duration) *Publisher {
	t.Helper()
	pub, err := NewPublisher(pool, sink, owner, PublisherOptions{
		Batch:       batch,
		LeaseTTL:    lease,
		BackoffBase: time.Second,
		BackoffMax:  60 * time.Second,
		Jitter:      func() float64 { return 0 },
	})
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	return pub
}

// TestPublisherClaimMutualExclusion covers the SKIP LOCKED + lease claim: two
// instances never hold the same row, the claimed sets partition the pending
// rows, and live leases are not stealable.
func TestPublisherClaimMutualExclusion(t *testing.T) {
	dsn := startMigratedPostgres(t)
	pool := openEventsPool(t, dsn)
	ctx := context.Background()

	const rows = 6
	want := map[int64]bool{}
	for i := 0; i < rows; i++ {
		_, id := appendPublisherEvent(t, pool, i)
		want[id] = true
	}

	pubA := newTestPublisher(t, pool, newRecordingSink(), "owner-a", rows, 30*time.Second)
	pubB := newTestPublisher(t, pool, newRecordingSink(), "owner-b", rows, 30*time.Second)

	type claimResult struct {
		owner string
		ids   []int64
		err   error
	}
	results := make(chan claimResult, 2)
	for _, pair := range []struct {
		pub   *Publisher
		owner string
	}{{pubA, "owner-a"}, {pubB, "owner-b"}} {
		go func(pub *Publisher, owner string) {
			records, err := pub.ClaimBatch(ctx)
			ids := make([]int64, 0, len(records))
			for _, rec := range records {
				ids = append(ids, rec.OutboxID)
			}
			results <- claimResult{owner: owner, ids: ids, err: err}
		}(pair.pub, pair.owner)
	}
	claimed := map[int64]string{}
	for i := 0; i < 2; i++ {
		res := <-results
		if res.err != nil {
			t.Fatalf("concurrent ClaimBatch: %v", res.err)
		}
		for _, id := range res.ids {
			if prev, dup := claimed[id]; dup {
				t.Fatalf("row %d claimed by both %s and %s", id, prev, res.owner)
			}
			claimed[id] = res.owner
		}
	}
	if len(claimed) != rows {
		t.Fatalf("claimed rows = %d, want %d (no loss)", len(claimed), rows)
	}
	for id := range want {
		if _, ok := claimed[id]; !ok {
			t.Fatalf("row %d was never claimed", id)
		}
	}
	// Live leases are not stealable: a third claim finds nothing.
	third, err := pubA.ClaimBatch(ctx)
	if err != nil {
		t.Fatalf("third ClaimBatch: %v", err)
	}
	if len(third) != 0 {
		t.Fatalf("third claim took %d rows under live leases, want 0", len(third))
	}
	// Each row carries exactly one owner.
	for id := range want {
		row := readOutboxRow(t, pool, id)
		if row.claimOwner == nil || *row.claimOwner != claimed[id] {
			t.Fatalf("row %d owner = %v, want %s", id, row.claimOwner, claimed[id])
		}
	}
}

// TestPublisherLeaseExpiryReclaim covers the lease takeover rule: an expired
// claim is reclaimable and the attempt counter advances.
func TestPublisherLeaseExpiryReclaim(t *testing.T) {
	dsn := startMigratedPostgres(t)
	pool := openEventsPool(t, dsn)
	ctx := context.Background()

	_, id := appendPublisherEvent(t, pool, 0)
	pubA := newTestPublisher(t, pool, newRecordingSink(), "owner-a", 10, 200*time.Millisecond)
	pubB := newTestPublisher(t, pool, newRecordingSink(), "owner-b", 10, 30*time.Second)

	claimed, err := pubA.ClaimBatch(ctx)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("owner-a claim = %d rows, err = %v", len(claimed), err)
	}
	if got := readOutboxRow(t, pool, id); got.attemptCount != 1 || got.claimOwner == nil || *got.claimOwner != "owner-a" {
		t.Fatalf("after owner-a claim: %+v", got)
	}
	// A live lease is not stealable.
	if rows, err := pubB.ClaimBatch(ctx); err != nil || len(rows) != 0 {
		t.Fatalf("owner-b claim under live lease = %d rows, err = %v", len(rows), err)
	}
	// After expiry the same row is reclaimable by another instance.
	time.Sleep(300 * time.Millisecond)
	reclaimed, err := pubB.ClaimBatch(ctx)
	if err != nil || len(reclaimed) != 1 || reclaimed[0].OutboxID != id {
		t.Fatalf("owner-b reclaim = %+v, err = %v", reclaimed, err)
	}
	if reclaimed[0].AttemptCount != 2 {
		t.Fatalf("reclaim attempt = %d, want 2", reclaimed[0].AttemptCount)
	}
	if got := readOutboxRow(t, pool, id); got.claimOwner == nil || *got.claimOwner != "owner-b" {
		t.Fatalf("after reclaim: owner = %v", got.claimOwner)
	}
}

// TestPublisherAckOwnerGuard covers the owner-guarded ack: a wrong owner marks
// zero rows, the right owner marks exactly its claimed row, and published
// rows can never be re-acked or reopened.
func TestPublisherAckOwnerGuard(t *testing.T) {
	dsn := startMigratedPostgres(t)
	pool := openEventsPool(t, dsn)
	ctx := context.Background()

	_, id := appendPublisherEvent(t, pool, 0)
	pubA := newTestPublisher(t, pool, newRecordingSink(), "owner-a", 10, 30*time.Second)
	pubB := newTestPublisher(t, pool, newRecordingSink(), "owner-b", 10, 30*time.Second)

	claimed, err := pubA.ClaimBatch(ctx)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim = %d rows, err = %v", len(claimed), err)
	}
	if n, err := pubB.markPublished(ctx, []int64{id}); err != nil || n != 0 {
		t.Fatalf("wrong-owner ack marked %d rows, err = %v (want 0)", n, err)
	}
	if got := readOutboxRow(t, pool, id); got.publishState != string(PublishStatePending) {
		t.Fatalf("row state after wrong-owner ack = %s", got.publishState)
	}
	if n, err := pubA.markPublished(ctx, []int64{id}); err != nil || n != 1 {
		t.Fatalf("owner ack marked %d rows, err = %v (want 1)", n, err)
	}
	got := readOutboxRow(t, pool, id)
	if got.publishState != string(PublishStatePublished) || got.publishedAt == nil || got.claimOwner != nil {
		t.Fatalf("row after ack = %+v", got)
	}
	// published -> published is refused.
	if n, err := pubA.markPublished(ctx, []int64{id}); err != nil || n != 0 {
		t.Fatalf("re-ack marked %d rows, err = %v (want 0)", n, err)
	}
}

// TestPublisherCrashPointMatrix walks the documented crash points (data-model
// §5 T2/T3): claim rolled back, claim committed but never published, published
// but never acked, and acked. Every path converges with 0 loss; duplicates
// are possible and expected.
func TestPublisherCrashPointMatrix(t *testing.T) {
	dsn := startMigratedPostgres(t)
	pool := openEventsPool(t, dsn)
	ctx := context.Background()

	t.Run("claim_transaction_rolled_back", func(t *testing.T) {
		_, id := appendPublisherEvent(t, pool, 100)
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}
		rows, err := tx.Query(ctx, ClaimPendingSQL, 10)
		if err != nil {
			t.Fatalf("claim select: %v", err)
		}
		var ids []int64
		for rows.Next() {
			rec, err := scanOutboxRecord(rows)
			if err != nil {
				t.Fatalf("scan: %v", err)
			}
			ids = append(ids, rec.OutboxID)
		}
		rows.Close()
		if len(ids) != 1 {
			t.Fatalf("locked claim rows = %d, want 1", len(ids))
		}
		marked, err := tx.Query(ctx, ClaimMarkSQL, ids, "crashed-owner", 30.0)
		if err != nil {
			t.Fatalf("claim mark: %v", err)
		}
		for marked.Next() {
		}
		marked.Close()
		if err := marked.Err(); err != nil {
			t.Fatalf("claim mark rows: %v", err)
		}
		// Crash before COMMIT: no claim survives.
		if err := tx.Rollback(ctx); err != nil {
			t.Fatalf("Rollback: %v", err)
		}
		got := readOutboxRow(t, pool, id)
		if got.claimOwner != nil || got.publishState != string(PublishStatePending) {
			t.Fatalf("after rollback: %+v", got)
		}
		sink := newRecordingSink()
		pub := newTestPublisher(t, pool, sink, "owner-recover", 10, 30*time.Second)
		if _, err := pub.PublishOnce(ctx); err != nil {
			t.Fatalf("PublishOnce: %v", err)
		}
		if sink.deliveryCount(id) != 1 || readOutboxRow(t, pool, id).publishState != string(PublishStatePublished) {
			t.Fatalf("recovery did not publish exactly once")
		}
	})

	t.Run("claim_committed_before_publish", func(t *testing.T) {
		_, id := appendPublisherEvent(t, pool, 101)
		crashed := newTestPublisher(t, pool, newRecordingSink(), "owner-crashed", 10, 200*time.Millisecond)
		claimed, err := crashed.ClaimBatch(ctx)
		if err != nil || len(claimed) != 1 {
			t.Fatalf("claim = %d rows, err = %v", len(claimed), err)
		}
		// Crash after COMMIT, before publish: the lease expires and the row is
		// re-claimed and published. 0 loss, no duplicate yet.
		time.Sleep(300 * time.Millisecond)
		sink := newRecordingSink()
		pub := newTestPublisher(t, pool, sink, "owner-recover", 10, 30*time.Second)
		if _, err := pub.PublishOnce(ctx); err != nil {
			t.Fatalf("PublishOnce: %v", err)
		}
		if sink.deliveryCount(id) != 1 || readOutboxRow(t, pool, id).publishState != string(PublishStatePublished) {
			t.Fatalf("recovery delivery = %d, want 1 published", sink.deliveryCount(id))
		}
	})

	t.Run("published_before_ack", func(t *testing.T) {
		_, id := appendPublisherEvent(t, pool, 102)
		crashed := newTestPublisher(t, pool, newRecordingSink(), "owner-crashed", 10, 200*time.Millisecond)
		claimed, err := crashed.ClaimBatch(ctx)
		if err != nil || len(claimed) != 1 {
			t.Fatalf("claim = %d rows, err = %v", len(claimed), err)
		}
		// The broker accepted the record, then the process died before the
		// ack mark: the lease expires and the row is published again. The
		// duplicate is the documented at-least-once behavior; the consumer's
		// persistent idempotency absorbs it (T046/T050).
		result := crashed.Sink.Publish(ctx, claimed)
		if len(result.Acked) != 1 {
			t.Fatalf("first publish acked = %d, want 1", len(result.Acked))
		}
		time.Sleep(300 * time.Millisecond)
		sink := newRecordingSink()
		pub := newTestPublisher(t, pool, sink, "owner-recover", 10, 30*time.Second)
		if _, err := pub.PublishOnce(ctx); err != nil {
			t.Fatalf("PublishOnce: %v", err)
		}
		if sink.deliveryCount(id) != 1 {
			t.Fatalf("recovery delivery = %d, want the duplicate publish", sink.deliveryCount(id))
		}
		got := readOutboxRow(t, pool, id)
		if got.publishState != string(PublishStatePublished) || got.attemptCount != 2 {
			t.Fatalf("after duplicate recovery: %+v", got)
		}
	})

	t.Run("acked_then_crash", func(t *testing.T) {
		_, id := appendPublisherEvent(t, pool, 103)
		sink := newRecordingSink()
		pub := newTestPublisher(t, pool, sink, "owner-a", 10, 30*time.Second)
		if _, err := pub.PublishOnce(ctx); err != nil {
			t.Fatalf("PublishOnce: %v", err)
		}
		// Crash after the ack mark: no re-claim, no extra delivery.
		if rows, err := pub.ClaimBatch(ctx); err != nil || len(rows) != 0 {
			t.Fatalf("post-ack claim = %d rows, err = %v", len(rows), err)
		}
		if sink.deliveryCount(id) != 1 {
			t.Fatalf("post-ack delivery = %d, want 1", sink.deliveryCount(id))
		}
		if got := readOutboxRow(t, pool, id); got.publishState != string(PublishStatePublished) {
			t.Fatalf("post-ack state = %s", got.publishState)
		}
	})
}

// TestPublisherShutdownReleasesUnconfirmed covers the graceful-stop path: a
// claim taken but not published when the context is cancelled is released
// immediately (no backoff) so a restart can re-publish without waiting out the
// lease; nothing is delivered and nothing is lost.
func TestPublisherShutdownReleasesUnconfirmed(t *testing.T) {
	dsn := startMigratedPostgres(t)
	pool := openEventsPool(t, dsn)
	ctx := context.Background()

	_, id := appendPublisherEvent(t, pool, 0)
	sink := newRecordingSink()
	pub := newTestPublisher(t, pool, sink, "owner-a", 10, 30*time.Second)
	claimed, err := pub.ClaimBatch(ctx)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim = %d rows, err = %v", len(claimed), err)
	}

	stopped, cancel := context.WithCancel(ctx)
	cancel()
	outcome, err := pub.PublishClaimed(stopped, claimed)
	if err != nil {
		t.Fatalf("PublishClaimed on stop: %v", err)
	}
	if outcome.Released != 1 || outcome.Acked != 0 {
		t.Fatalf("outcome on stop = %+v, want 1 released / 0 acked", outcome)
	}
	if sink.deliveryCount(id) != 0 {
		t.Fatal("a record reached the broker after the stop was observed before publish")
	}
	got := readOutboxRow(t, pool, id)
	if got.publishState != string(PublishStatePending) || got.claimOwner != nil {
		t.Fatalf("row after shutdown release = %+v", got)
	}
	if delay := time.Until(got.nextAttempt); delay > time.Second {
		t.Fatalf("shutdown release scheduled a backoff (%s); a restart must be able to re-publish immediately", delay)
	}
	// A restart claims the row immediately.
	next, err := pub.ClaimBatch(ctx)
	if err != nil || len(next) != 1 || next[0].OutboxID != id {
		t.Fatalf("restart claim = %+v, err = %v", next, err)
	}
}

// TestPublisherTransientFailureBackoff covers the bounded retry: a transient
// failure releases the claim with the exponential backoff of the attempt that
// failed (deterministic jitter), the row stays pending, and the delay grows
// with the attempt count while staying capped.
func TestPublisherTransientFailureBackoff(t *testing.T) {
	dsn := startMigratedPostgres(t)
	pool := openEventsPool(t, dsn)
	ctx := context.Background()

	_, id := appendPublisherEvent(t, pool, 0)
	sink := newRecordingSink()
	sink.fail = func(OutboxRecord) error { return Transient(errors.New("broker unreachable")) }
	pub := newTestPublisher(t, pool, sink, "owner-a", 10, 30*time.Second)

	outcome, err := pub.PublishOnce(ctx)
	if err != nil || outcome.Released != 1 {
		t.Fatalf("outcome = %+v, err = %v", outcome, err)
	}
	got := readOutboxRow(t, pool, id)
	if got.publishState != string(PublishStatePending) || got.claimOwner != nil ||
		got.lastError == nil || *got.lastError != string(ClassTransient) {
		t.Fatalf("after transient failure: %+v", got)
	}
	firstDelay := time.Until(got.nextAttempt)
	if firstDelay < 500*time.Millisecond || firstDelay > 2*time.Second {
		t.Fatalf("first backoff = %s, want ~1s (base, deterministic jitter)", firstDelay)
	}
	// The backoff is honored: no immediate re-claim.
	if rows, err := pub.ClaimBatch(ctx); err != nil || len(rows) != 0 {
		t.Fatalf("claim during backoff = %d rows, err = %v", len(rows), err)
	}
	// Force the retry due and observe the doubled backoff on attempt 2.
	if _, err := pool.Exec(ctx, `UPDATE outbox_events SET next_attempt_at = now() WHERE id = $1`, id); err != nil {
		t.Fatalf("force retry: %v", err)
	}
	if _, err := pub.PublishOnce(ctx); err != nil {
		t.Fatalf("second PublishOnce: %v", err)
	}
	got = readOutboxRow(t, pool, id)
	if got.attemptCount != 2 {
		t.Fatalf("attempt_count = %d, want 2", got.attemptCount)
	}
	secondDelay := time.Until(got.nextAttempt)
	if secondDelay < time.Second || secondDelay > 3*time.Second {
		t.Fatalf("second backoff = %s, want ~2s (base*2)", secondDelay)
	}
}

// TestPublisherPermanentFailureBlocksVisibleAuditable covers the permanent and
// contract classes: the row becomes blocked with the class recorded, stays
// visible, is never claimable again and is never dropped.
func TestPublisherPermanentFailureBlocksVisibleAuditable(t *testing.T) {
	dsn := startMigratedPostgres(t)
	pool := openEventsPool(t, dsn)
	ctx := context.Background()

	_, permanentID := appendPublisherEvent(t, pool, 200)
	_, contractID := appendPublisherEvent(t, pool, 201)

	sink := newRecordingSink()
	sink.fail = func(rec OutboxRecord) error {
		if rec.OutboxID == permanentID {
			return Permanent(errors.New("invalid topic shape"))
		}
		return fmt.Errorf("%w: payload contract violation", ErrContract)
	}
	observer := newCountingPublishObserver()
	pub, err := NewPublisher(pool, sink, "owner-a", PublisherOptions{
		Batch: 10, LeaseTTL: 30 * time.Second, BackoffBase: time.Second, BackoffMax: time.Minute,
		Jitter: func() float64 { return 0 }, Observer: observer,
	})
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}

	outcome, err := pub.PublishOnce(ctx)
	if err != nil || outcome.Blocked != 2 {
		t.Fatalf("outcome = %+v, err = %v", outcome, err)
	}
	for id, wantClass := range map[int64]string{permanentID: string(ClassPermanent), contractID: string(ClassContract)} {
		got := readOutboxRow(t, pool, id)
		if got.publishState != string(PublishStateBlocked) || got.publishedAt != nil ||
			got.claimOwner != nil || got.lastError == nil || *got.lastError != wantClass {
			t.Fatalf("blocked row %d = %+v, want class %s", id, got, wantClass)
		}
	}
	// Blocked rows are not claimable: only an audited unblock returns them.
	if rows, err := pub.ClaimBatch(ctx); err != nil || len(rows) != 0 {
		t.Fatalf("claim of blocked rows = %d rows, err = %v", len(rows), err)
	}
	// Visible/auditable: the rows still exist and the blocked gauge observed.
	var blocked int64
	if err := pool.QueryRow(ctx, BlockedCountSQL).Scan(&blocked); err != nil || blocked != 2 {
		t.Fatalf("blocked count = %d, err = %v (want 2: never dropped)", blocked, err)
	}
	observer.mu.Lock()
	blockedGauge := observer.blocked
	failures := append([]string(nil), observer.failures...)
	observer.mu.Unlock()
	if blockedGauge != 2 {
		t.Fatalf("observer blocked gauge = %d, want 2", blockedGauge)
	}
	if len(failures) != 2 {
		t.Fatalf("observer failures = %v, want 2", failures)
	}
}

// TestPublisherUncommittedRowsNeverPublishedAndNoLockDuringPublish covers the
// two structural rules: a row written in an open transaction is invisible to
// the claim, and the network publish runs after the claim COMMIT with no row
// lock held.
func TestPublisherUncommittedRowsNeverPublishedAndNoLockDuringPublish(t *testing.T) {
	dsn := startMigratedPostgres(t)
	pool := openEventsPool(t, dsn)
	ctx := context.Background()

	// Uncommitted row: the claim cannot see it.
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	ev := depositCreatedEvent(t, "t036-uncommitted", fmt.Sprintf("0x%064x", 300), fmt.Sprintf("0x%064x", 301), 1, "pending")
	res, err := Append(ctx, tx, ev)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	sink := newRecordingSink()
	pub := newTestPublisher(t, pool, sink, "owner-a", 10, 30*time.Second)
	if outcome, err := pub.PublishOnce(ctx); err != nil || outcome.Claimed != 0 {
		t.Fatalf("claim with an uncommitted row = %+v, err = %v (want 0 claimed)", outcome, err)
	}
	if sink.deliveryCount(res.OutboxID) != 0 {
		t.Fatal("an uncommitted row was published")
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// The claim is committed before the publish and no row lock is held while
	// the sink runs: a separate connection can lock the row immediately.
	sink.hook = func(hookCtx context.Context, records []OutboxRecord) {
		for _, rec := range records {
			var owner *string
			if err := pool.QueryRow(hookCtx,
				`SELECT claim_owner FROM outbox_events WHERE id = $1 FOR UPDATE NOWAIT`, rec.OutboxID).
				Scan(&owner); err != nil {
				t.Errorf("row %d is locked during publish: %v", rec.OutboxID, err)
				continue
			}
			if owner == nil || *owner != "owner-a" {
				t.Errorf("row %d claim_owner = %v during publish, want owner-a (claim committed first)", rec.OutboxID, owner)
			}
		}
	}
	if outcome, err := pub.PublishOnce(ctx); err != nil || outcome.Acked != 1 {
		t.Fatalf("publish after commit = %+v, err = %v", outcome, err)
	}
}

// TestPublisherRefreshGauges covers the PG-only backlog observation: pending
// counts/oldest age per family and the blocked gauge.
func TestPublisherRefreshGauges(t *testing.T) {
	dsn := startMigratedPostgres(t)
	pool := openEventsPool(t, dsn)
	ctx := context.Background()

	appendPublisherEvent(t, pool, 400)
	appendPublisherEvent(t, pool, 401)
	_, blockedID := appendPublisherEvent(t, pool, 402)
	if _, err := pool.Exec(ctx, `
		UPDATE outbox_events SET publish_state = 'blocked', last_error_class = 'permanent'
		WHERE id = $1`, blockedID); err != nil {
		t.Fatalf("seed blocked row: %v", err)
	}

	observer := newCountingPublishObserver()
	pub, err := NewPublisher(pool, newRecordingSink(), "owner-a", PublisherOptions{
		Batch: 10, LeaseTTL: 30 * time.Second, BackoffBase: time.Second, BackoffMax: time.Minute, Observer: observer,
	})
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	if err := pub.RefreshGauges(ctx); err != nil {
		t.Fatalf("RefreshGauges: %v", err)
	}
	observer.mu.Lock()
	pending := observer.pending["deposit"]
	oldest := observer.oldest["deposit"]
	blocked := observer.blocked
	observer.mu.Unlock()
	if pending != 2 {
		t.Fatalf("pending gauge = %d, want 2", pending)
	}
	if oldest < 0 {
		t.Fatalf("oldest age = %f, want >= 0", oldest)
	}
	if blocked != 1 {
		t.Fatalf("blocked gauge = %d, want 1", blocked)
	}
}
