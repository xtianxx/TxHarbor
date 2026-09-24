//go:build integration

// capacity_integration_test.go is the T072 Integration-PG layer (V-CAPACITY,
// quickstart Q7): the capacity guard against the real PostgreSQL migration and
// the real first-create path, not against hand-called state helpers.
//
// What it exercises:
//
//   - growing the pending backlog to the soft boundary and observing that the
//     real intake (`withdrawal.SubmitWithdrawal` with the real
//     events.CapacityGuard) refuses new controllable funding writes with the
//     retryable 503 channel while an already-accepted request and its replay
//     keep being served (PD-2: stop new, keep in-flight);
//   - chain facts (observation created + confirmation confirmed, committed
//     with their business rows in one transaction) continuing above the hard
//     boundary — pending may exceed hard_limit and no fact is ever rejected;
//   - an accepted in-flight withdrawal transition completing above the hard
//     boundary with its `withdrawal.execution.state_changed` event present
//     (real 011 TransitionIntent; no new payment intent is created by the
//     capacity path);
//   - recovery drain through the real publisher PG half (a test sink; the
//     real-broker half is T074), with 0 loss, 0 blocked, no row removed and
//     pending/oldest-age observation at 100% of the families that have pending
//     rows;
//   - the T071 hard-boundary pause decision read from a real 004
//     deposit_checkpoint row, with the rescan window asserted for 0 missing /
//     0 duplicate heights and the no-new-intent invariant checked by row
//     counts.
//
// The intake and the chain facts run against the actual dependencies (real
// migration, real pool, real guard queries); the publisher sink is the
// documented PG-half test double of the T036 layer, never Kafka evidence.
package events_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/events"
	"github.com/xtianxx/txharbor/internal/execution"
	"github.com/xtianxx/txharbor/internal/indexer"
	"github.com/xtianxx/txharbor/internal/withdrawal"
)

const (
	capacityChainID      = int64(31337)
	capacityAsset        = "0x1111111111111111111111111111111111111111"
	capacityRecipient    = "0x2222222222222222222222222222222222222222"
	capacityAmount       = "250"
	capacityProbeTable   = "t072_business_probe"
	capacityCheckpointID = int64(990001)
)

// startCapacityPostgres boots a real PostgreSQL container with the embedded
// migrations applied. Skips (never passes) when no Docker provider is
// available.
func startCapacityPostgres(t *testing.T) string {
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

func openCapacityPool(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// capacityTestObserver records the guard observations for the metric-rate and
// refusal assertions (the production observer is *metrics.Metrics).
type capacityTestObserver struct {
	pending    map[string]int
	oldest     map[string]float64
	softBreach int
	hardBreach int
	refusals   []string
}

func newCapacityTestObserver() *capacityTestObserver {
	return &capacityTestObserver{pending: map[string]int{}, oldest: map[string]float64{}}
}

func (o *capacityTestObserver) SetOutboxPending(family string, count int) { o.pending[family] = count }
func (o *capacityTestObserver) SetOutboxPendingOldestAge(family string, seconds float64) {
	o.oldest[family] = seconds
}
func (o *capacityTestObserver) ObserveCapacitySoftBreach() { o.softBreach++ }
func (o *capacityTestObserver) ObserveCapacityHardBreach() { o.hardBreach++ }
func (o *capacityTestObserver) ObserveCapacityRefusal(opClass string) {
	o.refusals = append(o.refusals, opClass)
}

// ackAllSink is the T036-style PG-half publish double: it acknowledges every
// claimed record and counts deliveries (the real broker half is T074).
type ackAllSink struct{ delivered map[int64]int }

func newAckAllSink() *ackAllSink { return &ackAllSink{delivered: map[int64]int{}} }

func (s *ackAllSink) Publish(_ context.Context, records []events.OutboxRecord) events.PublishResult {
	result := events.PublishResult{Failures: map[int64]error{}}
	for _, rec := range records {
		s.delivered[rec.OutboxID]++
		result.Acked = append(result.Acked, rec.OutboxID)
	}
	return result
}

func capacityCount(t *testing.T, pool *pgxpool.Pool, sql string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatalf("count (%s): %v", sql, err)
	}
	return n
}

func capacityOutboxCount(t *testing.T, pool *pgxpool.Pool) int64 {
	t.Helper()
	return capacityCount(t, pool, `SELECT count(*) FROM outbox_events`)
}

func capacityPendingTotal(t *testing.T, pool *pgxpool.Pool) int64 {
	t.Helper()
	return capacityCount(t, pool, `SELECT count(*) FROM outbox_events WHERE publish_state = 'pending'`)
}

func createCapacityProbeTable(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s (id TEXT PRIMARY KEY, kind TEXT NOT NULL)`, capacityProbeTable)); err != nil {
		t.Fatalf("create probe table: %v", err)
	}
}

// appendCapacityFact commits one business probe row and its
// deposit.observation.created event in the SAME transaction — the real I-CAP
// shape: an accepted unit always carries its event.
func appendCapacityFact(t *testing.T, pool *pgxpool.Pool, n int) int64 {
	t.Helper()
	ctx := context.Background()
	probeID := fmt.Sprintf("t072-probe-%d", n)
	obsID := fmt.Sprintf("t072-obs-%d", n)
	ev, err := events.NewEvent(events.Event{
		EventType:     events.EventTypeDepositObservationCreated,
		SchemaVersion: events.SchemaVersionV1,
		IdentityKind:  events.IdentityKindEVMLog,
		AggregateType: "deposit_observation",
		AggregateID:   obsID,
		Payload:       map[string]any{"observation_id": obsID, "state": "pending"},
		OccurredAt:    time.Now().UTC(),
		ChainID:       capacityChainID,
		BlockNumber:   int64(1000 + n),
		BlockHash:     fmt.Sprintf("0x%064x", 0x7200+n),
		TxHash:        fmt.Sprintf("0x%064x", 0x7300+n),
		LogIndex:      n,
	})
	if err != nil {
		t.Fatalf("build fact event: %v", err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin fact tx: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (id, kind) VALUES ($1, 'deposit_observation')`, capacityProbeTable), probeID); err != nil {
		t.Fatalf("insert probe row: %v", err)
	}
	res, err := events.Append(ctx, tx, ev)
	if err != nil {
		t.Fatalf("Append fact: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit fact: %v", err)
	}
	return res.OutboxID
}

// appendCapacityHardObservation commits one probe row and one
// deposit.observation.created event (evm_log identity, unique log triple) for a
// hard-boundary fact; appendCapacityChainFact then adds the confirmation as its
// business-object successor.
func appendCapacityHardObservation(t *testing.T, pool *pgxpool.Pool, obsID string) int64 {
	t.Helper()
	ctx := context.Background()
	ev, err := events.NewEvent(events.Event{
		EventType:     events.EventTypeDepositObservationCreated,
		SchemaVersion: events.SchemaVersionV1,
		IdentityKind:  events.IdentityKindEVMLog,
		AggregateType: "deposit_observation",
		AggregateID:   obsID,
		Payload:       map[string]any{"observation_id": obsID, "state": "pending"},
		OccurredAt:    time.Now().UTC(),
		ChainID:       capacityChainID,
		BlockNumber:   9001,
		BlockHash:     fmt.Sprintf("0x%064x", 0x8100),
		TxHash:        fmt.Sprintf("0x%064x", 0x8200),
		LogIndex:      1,
	})
	if err != nil {
		t.Fatalf("build hard observation event: %v", err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin hard observation tx: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (id, kind) VALUES ($1, 'hard_observation')`, capacityProbeTable), obsID); err != nil {
		t.Fatalf("insert hard observation probe row: %v", err)
	}
	res, err := events.Append(ctx, tx, ev)
	if err != nil {
		t.Fatalf("Append hard observation: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit hard observation: %v", err)
	}
	return res.OutboxID
}

// appendCapacityChainFact commits a business_object deposit fact (confirmation)
// for the same observation aggregate: aggregate_version continues from the
// created event, and the business row + event stay atomic.
func appendCapacityChainFact(t *testing.T, pool *pgxpool.Pool, obsID string) int64 {
	t.Helper()
	ctx := context.Background()
	ev, err := events.NewEvent(events.Event{
		EventType:     events.EventTypeDepositConfirmationConfirmed,
		SchemaVersion: events.SchemaVersionV1,
		IdentityKind:  events.IdentityKindBusinessObject,
		AggregateType: "deposit_observation",
		AggregateID:   obsID,
		Payload: map[string]any{
			"policy_version":         1,
			"confirmed_block_number": 1001,
			"confirmed_block_hash":   fmt.Sprintf("0x%064x", 0x9999),
		},
		OccurredAt: time.Now().UTC(),
		ChainID:    capacityChainID,
	})
	if err != nil {
		t.Fatalf("build confirmation event: %v", err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin confirmation tx: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (id, kind) VALUES ($1, 'confirmation') ON CONFLICT (id) DO NOTHING`, capacityProbeTable), obsID+"-confirm"); err != nil {
		t.Fatalf("insert confirmation probe row: %v", err)
	}
	res, err := events.Append(ctx, tx, ev)
	if err != nil {
		t.Fatalf("Append confirmation: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit confirmation: %v", err)
	}
	return res.OutboxID
}

// seedCapacityGrant creates one active grant row matching the canonical
// request parameters.
func seedCapacityGrant(t *testing.T, pool *pgxpool.Pool, callerID int64, authorizationID string) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `
INSERT INTO withdrawal_authorizations
    (authorization_id, caller_id, chain_id, asset, recipient, amount, state)
VALUES ($1, $2, $3, $4, $5, $6, 'active')`,
		authorizationID, callerID, capacityChainID, capacityAsset, capacityRecipient, capacityAmount); err != nil {
		t.Fatalf("seed grant %s: %v", authorizationID, err)
	}
}

// submitCapacityCreate drives the real first-create path.
func submitCapacityCreate(t *testing.T, pool *pgxpool.Pool, presentedKey, idemKey, authorizationID string, gate withdrawal.CapacityAdmitter) *withdrawal.SubmitResult {
	t.Helper()
	res, err := withdrawal.SubmitWithdrawal(context.Background(), pool, withdrawal.SubmitRequest{
		PresentedKey:    presentedKey,
		IdempotencyKey:  idemKey,
		ChainID:         capacityChainID,
		ExpectedChainID: capacityChainID,
		Asset:           capacityAsset,
		Recipient:       capacityRecipient,
		Amount:          capacityAmount,
		AuthorizationID: authorizationID,
		Allowlist:       []string{capacityAsset},
		CapacityGate:    gate,
	})
	if err != nil {
		t.Fatalf("SubmitWithdrawal internal error: %v", err)
	}
	return res
}

// TestCapacityGuardAgainstRealIntakeAndStreams is the T072 acceptance.
func TestCapacityGuardAgainstRealIntakeAndStreams(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	pool := openCapacityPool(t, startCapacityPostgres(t))
	createCapacityProbeTable(t, pool)

	observer := newCapacityTestObserver()
	limits := events.CapacityLimits{Reserve: 1, SoftLimit: 3, HardLimit: 6}
	guard, err := events.NewCapacityGuard(pool, limits, observer)
	if err != nil {
		t.Fatalf("NewCapacityGuard: %v", err)
	}

	callerID := int64(901)
	presentedKey, _, err := withdrawal.IssueKey(ctx, pool, callerID, "t072-capacity")
	if err != nil {
		t.Fatalf("IssueKey: %v", err)
	}

	// --- 1. below the soft boundary the first create is admitted -------------
	seedCapacityGrant(t, pool, callerID, "authz-cap-1")
	first := submitCapacityCreate(t, pool, presentedKey, "t072-key-1", "authz-cap-1", guard)
	if first.Status != 201 || first.RequestID == "" {
		t.Fatalf("first create = %d/%s (%s), want 201 with a request id", first.Status, first.Code, first.Message)
	}
	requestID := first.RequestID
	if got := capacityCount(t, pool, `SELECT count(*) FROM withdrawal_requests WHERE caller_id = $1`, callerID); got != 1 {
		t.Fatalf("withdrawal_requests = %d, want 1", got)
	}
	if got := capacityCount(t, pool,
		`SELECT count(*) FROM outbox_events WHERE aggregate_id = $1 AND event_type = $2`,
		requestID, events.EventTypeWithdrawalRequestReceived); got != 1 {
		t.Fatalf("request event rows = %d, want 1 (atomic with the receipt)", got)
	}

	// Grow the pending backlog to the soft boundary: the create event plus two
	// committed facts (facts are never rejected).
	appendCapacityFact(t, pool, 1)
	appendCapacityFact(t, pool, 2)
	if pending := capacityPendingTotal(t, pool); pending < limits.SoftLimit {
		t.Fatalf("pending = %d, want >= soft_limit %d", pending, limits.SoftLimit)
	}
	snapshot, err := guard.Observe(ctx)
	if err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if snapshot.Level != events.CapacitySoft {
		t.Fatalf("level = %s, want soft (pending=%d limits=%+v)", snapshot.Level, snapshot.PendingTotal, limits)
	}
	if snapshot.OldestAgeSeconds < 0 {
		t.Fatalf("oldest age = %v, want a real measured value", snapshot.OldestAgeSeconds)
	}
	// Observation covers every pending family (100% observability).
	pendingFamilies := capacityCount(t, pool, `SELECT count(DISTINCT split_part(event_type, '.', 1)) FROM outbox_events WHERE publish_state = 'pending'`)
	observedFamilies := int64(len(observer.pending))
	if observedFamilies < pendingFamilies {
		t.Fatalf("observed families = %d, pending families = %d (observability must be 100%%)", observedFamilies, pendingFamilies)
	}

	// A new controllable write is refused with the retryable channel.
	outboxBefore := capacityOutboxCount(t, pool)
	seedCapacityGrant(t, pool, callerID, "authz-cap-2")
	refused := submitCapacityCreate(t, pool, presentedKey, "t072-key-2", "authz-cap-2", guard)
	if refused.Status != 503 || refused.Code != withdrawal.CodeTemporarilyUnavailable {
		t.Fatalf("soft-boundary create = %d/%s, want 503/temporarily_unavailable", refused.Status, refused.Code)
	}
	if !strings.Contains(refused.Message, "capacity boundary") ||
		!strings.Contains(refused.Message, "retry with the same idempotency key") {
		t.Fatalf("refusal message %q is not an explicit same-key capacity retry", refused.Message)
	}
	if len(observer.refusals) != 1 || observer.refusals[0] != events.CapacityOpWithdrawalCreate {
		t.Fatalf("capacity refusals = %v, want one withdrawal_create refusal", observer.refusals)
	}
	if got := capacityCount(t, pool, `SELECT count(*) FROM withdrawal_requests WHERE caller_id = $1`, callerID); got != 1 {
		t.Fatalf("withdrawal_requests after refusal = %d, want 1 (no new request)", got)
	}
	if got := capacityOutboxCount(t, pool); got != outboxBefore {
		t.Fatalf("outbox rows after refusal = %d, want %d (no event for a refused create)", got, outboxBefore)
	}

	// An already-accepted request and its replay are never refused.
	replay := submitCapacityCreate(t, pool, presentedKey, "t072-key-1", "authz-cap-1", guard)
	if replay.Status != 200 || replay.RequestID != requestID {
		t.Fatalf("replay = %d/%s request=%s, want 200 replay of %s", replay.Status, replay.Code, replay.RequestID, requestID)
	}
	if got := capacityCount(t, pool, `SELECT count(*) FROM withdrawal_requests WHERE caller_id = $1`, callerID); got != 1 {
		t.Fatalf("withdrawal_requests after replay = %d, want 1", got)
	}

	// --- 2. the hard boundary: facts and in-flight work continue -------------
	appendCapacityFact(t, pool, 3)
	appendCapacityFact(t, pool, 4)
	appendCapacityFact(t, pool, 5)
	snapshot, err = guard.Observe(ctx)
	if err != nil {
		t.Fatalf("Observe at hard: %v", err)
	}
	if snapshot.Level != events.CapacityHard || snapshot.PendingTotal < limits.HardLimit {
		t.Fatalf("level = %s pending = %d, want hard with pending >= %d", snapshot.Level, snapshot.PendingTotal, limits.HardLimit)
	}

	// New controllable work is still refused at the hard boundary.
	seedCapacityGrant(t, pool, callerID, "authz-cap-3")
	refusedHard := submitCapacityCreate(t, pool, presentedKey, "t072-key-3", "authz-cap-3", guard)
	if refusedHard.Status != 503 {
		t.Fatalf("hard-boundary create = %d, want 503", refusedHard.Status)
	}
	if len(observer.refusals) != 2 {
		t.Fatalf("refusals = %v, want 2", observer.refusals)
	}

	// On-chain facts keep entering the Outbox with their business rows; pending
	// may exceed hard_limit (no fact is rejected).
	pendingBeforeFacts := capacityPendingTotal(t, pool)
	obsID := "t072-obs-hard-1"
	appendCapacityHardObservation(t, pool, obsID)
	appendCapacityChainFact(t, pool, obsID)
	if pending := capacityPendingTotal(t, pool); pending <= snapshot.PendingTotal {
		t.Fatalf("pending after hard-boundary facts = %d, want > %d (facts are never rejected)", pending, snapshot.PendingTotal)
	}
	if pending := capacityPendingTotal(t, pool); pending <= limits.HardLimit {
		t.Fatalf("pending = %d, want pending > hard_limit %d at this point", pending, limits.HardLimit)
	}
	if got := capacityPendingTotal(t, pool) - pendingBeforeFacts; got != 2 {
		t.Fatalf("facts added %d pending rows, want 2", got)
	}

	// An accepted in-flight withdrawal completes above the hard boundary with
	// its execution event present (real 011 transition; no new intent).
	intentsBefore := capacityCount(t, pool, `SELECT count(*) FROM payment_intents`)
	if _, err := pool.Exec(ctx, `
INSERT INTO payment_intents
    (intent_id, request_id, chain_id, sender, authorization_id, authorization_version, state, admitted_recovery_version)
VALUES ('t072-intent-1', $1, $2, $3, 'authz-cap-1', 1, 'executing', 0)`,
		requestID, capacityChainID, capacityAsset); err != nil {
		t.Fatalf("seed intent: %v", err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin transition tx: %v", err)
	}
	if err := execution.TransitionIntent(ctx, tx, "t072-intent-1", execution.IntentExecuting, 1, execution.IntentCompleted, 0); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("TransitionIntent executing->completed: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit transition: %v", err)
	}
	var intentState string
	if err := pool.QueryRow(ctx, `SELECT state FROM payment_intents WHERE intent_id = 't072-intent-1'`).Scan(&intentState); err != nil {
		t.Fatalf("read intent state: %v", err)
	}
	if intentState != execution.IntentCompleted {
		t.Fatalf("intent state = %s, want completed", intentState)
	}
	if got := capacityCount(t, pool,
		`SELECT count(*) FROM outbox_events WHERE aggregate_id = 't072-intent-1' AND event_type = $1`,
		events.EventTypeWithdrawalExecutionStateChanged); got != 1 {
		t.Fatalf("execution state_changed rows = %d, want 1 (in-flight completion carries its event)", got)
	}
	if got := capacityCount(t, pool, `SELECT count(*) FROM payment_intents`); got != intentsBefore+1 {
		t.Fatalf("payment_intents = %d, want %d (the capacity path creates no intent)", got, intentsBefore+1)
	}

	// No "business committed without event" gap: every probe row has its outbox
	// counterpart and every accepted request has its received event.
	probeRows := capacityCount(t, pool, fmt.Sprintf(`SELECT count(*) FROM %s`, capacityProbeTable))
	factRows := capacityCount(t, pool,
		`SELECT count(*) FROM outbox_events WHERE aggregate_id LIKE 't072-obs-%' AND event_type IN ($1, $2)`,
		events.EventTypeDepositObservationCreated, events.EventTypeDepositConfirmationConfirmed)
	if factRows < probeRows {
		t.Fatalf("outbox fact rows = %d < probe rows = %d: a committed business row has no event", factRows, probeRows)
	}
	requestRows := capacityCount(t, pool, `SELECT count(*) FROM withdrawal_requests WHERE caller_id = $1`, callerID)
	receivedRows := capacityCount(t, pool,
		`SELECT count(*) FROM outbox_events WHERE event_type = $1`, events.EventTypeWithdrawalRequestReceived)
	if requestRows != receivedRows {
		t.Fatalf("requests = %d, received events = %d: the receipt/event atomicity gap", requestRows, receivedRows)
	}

	// --- 3. recovery drain (PG half) with 0 loss and full observability ------
	rowsBefore := capacityOutboxCount(t, pool)
	pendingBeforeDrain := capacityPendingTotal(t, pool)
	if rowsBefore != pendingBeforeDrain {
		t.Fatalf("rows = %d, pending = %d; nothing was published yet", rowsBefore, pendingBeforeDrain)
	}
	// A final observation must cover every pending family before the drain
	// (pending_count/oldest_age observability rate 100%).
	snapshot, err = guard.Observe(ctx)
	if err != nil {
		t.Fatalf("Observe before drain: %v", err)
	}
	if snapshot.PendingTotal != pendingBeforeDrain {
		t.Fatalf("observed pending = %d, want %d", snapshot.PendingTotal, pendingBeforeDrain)
	}
	families := capacityCount(t, pool, `SELECT count(DISTINCT split_part(event_type, '.', 1)) FROM outbox_events WHERE publish_state = 'pending'`)
	if int64(len(observer.pending)) < families {
		t.Fatalf("observer saw %d families, %d have pending rows", len(observer.pending), families)
	}
	rows, err := pool.Query(ctx, `
SELECT split_part(event_type, '.', 1) AS family, count(*)::bigint
FROM outbox_events WHERE publish_state = 'pending'
GROUP BY 1`)
	if err != nil {
		t.Fatalf("query pending families: %v", err)
	}
	var familyRows int64
	for rows.Next() {
		var family string
		var count int64
		if err := rows.Scan(&family, &count); err != nil {
			t.Fatalf("scan pending family: %v", err)
		}
		familyRows++
		if observer.pending[family] != int(count) {
			t.Fatalf("family %s observed %d, want %d", family, observer.pending[family], count)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate pending families: %v", err)
	}
	if familyRows != families {
		t.Fatalf("family rows = %d, want %d", familyRows, families)
	}

	sink := newAckAllSink()
	publisher, err := events.NewPublisher(pool, sink, "t072-pg-half", events.PublisherOptions{
		Batch:       10,
		LeaseTTL:    30 * time.Second,
		BackoffBase: time.Millisecond,
		BackoffMax:  time.Second,
		Jitter:      func() float64 { return 0 },
	})
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	for cycle := 0; cycle < 100; cycle++ {
		if capacityPendingTotal(t, pool) == 0 {
			break
		}
		if _, err := publisher.PublishOnce(ctx); err != nil {
			t.Fatalf("PublishOnce: %v", err)
		}
	}
	if pending := capacityPendingTotal(t, pool); pending != 0 {
		t.Fatalf("pending after drain = %d, want 0 (recovery drain)", pending)
	}
	published := capacityCount(t, pool, `SELECT count(*) FROM outbox_events WHERE publish_state = 'published'`)
	if published != rowsBefore {
		t.Fatalf("published = %d, want %d (0 loss, no row removed)", published, rowsBefore)
	}
	if got := capacityCount(t, pool, `SELECT count(*) FROM outbox_events WHERE publish_state = 'blocked'`); got != 0 {
		t.Fatalf("blocked = %d, want 0", got)
	}
	if got := capacityCount(t, pool, `SELECT count(*) FROM outbox_events`); got != rowsBefore {
		t.Fatalf("outbox rows = %d, want %d (rows are never deleted by the drain)", got, rowsBefore)
	}
	if len(sink.delivered) != int(rowsBefore) {
		t.Fatalf("delivered distinct rows = %d, want %d", len(sink.delivered), rowsBefore)
	}
	drained, err := guard.Observe(ctx)
	if err != nil {
		t.Fatalf("Observe after drain: %v", err)
	}
	if drained.PendingTotal != 0 || drained.Level != events.CapacityNormal {
		t.Fatalf("post-drain observation = %+v, want normal/0", drained)
	}

	// --- 4. hard-boundary pause + rescan continuity (T071 scenario) ----------
	configHash := strings.Repeat("a", 64)
	if _, err := pool.Exec(ctx, `
INSERT INTO deposit_checkpoint (chain_id, start_block, config_hash, next_block)
VALUES ($1, 10, $2, 100)`, capacityCheckpointID, configHash); err != nil {
		t.Fatalf("seed deposit checkpoint: %v", err)
	}
	progress, err := indexer.ReadReliableProgress(ctx, pool, capacityCheckpointID, indexer.ReliableProgressDepositSource)
	if err != nil {
		t.Fatalf("ReadReliableProgress: %v", err)
	}
	if !progress.HasProgress || progress.ResumeHeight != 100 || progress.LastHeight != 99 || progress.Hash != configHash {
		t.Fatalf("progress = %+v, want last=99 resume=100 bound to the config hash", progress)
	}

	intentsBeforePause := capacityCount(t, pool, `SELECT count(*) FROM payment_intents`)
	outboxBeforePause := capacityOutboxCount(t, pool)
	decision := indexer.EvaluateCapacityPause(events.CapacityHard, errors.New("append: persistence not safe"), progress)
	if !decision.Pause || decision.Reason != indexer.CapacityPauseReasonNotPersistable {
		t.Fatalf("pause decision = %+v, want a not-persistable pause", decision)
	}
	plan := indexer.RescanFrom(decision)
	if !plan.HasProgress || plan.ResumeFrom != 100 || plan.ReprocessAll {
		t.Fatalf("rescan plan = %+v, want resume from 100", plan)
	}
	if intents := capacityCount(t, pool, `SELECT count(*) FROM payment_intents`); intents != intentsBeforePause {
		t.Fatalf("payment_intents changed by the pause decision (%d -> %d)", intentsBeforePause, intents)
	}
	if rows := capacityOutboxCount(t, pool); rows != outboxBeforePause {
		t.Fatalf("outbox rows changed by the pause decision (%d -> %d)", outboxBeforePause, rows)
	}
	// Recovery rescan: every height from the resume point is re-observed
	// exactly once; a skipped height is reported as missing.
	missing, duplicates := indexer.RescanContinuity(plan, []uint64{100, 101, 102, 103})
	if len(missing) != 0 || len(duplicates) != 0 {
		t.Fatalf("clean rescan = missing %v duplicates %v, want 0/0", missing, duplicates)
	}
	missing, _ = indexer.RescanContinuity(plan, []uint64{100, 102, 103})
	if len(missing) != 1 || missing[0] != 101 {
		t.Fatalf("skipped rescan = missing %v, want [101]", missing)
	}
}
