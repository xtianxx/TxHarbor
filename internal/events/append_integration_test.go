//go:build integration

// append_integration_test.go is the T015 integration layer: Append atomicity
// (business row + outbox row commit/roll back together), identity and version
// continuity, the idempotent no-op, same-identity/different-content refusal
// and the concurrent same-version race against the real 000015 migration.
// It is one member of the T009 x T014 x T015 x T016 merge gate.
//
// The scratch probe table stands in for an upstream business table: B2 must
// not wire producers (that is B3), but the atomicity contract is the same
// one-transaction shape every producer will use.
package events

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/xtianxx/txharbor/internal/db"
)

const probeTable = "t015_business_probe"

// startMigratedPostgres boots a real PostgreSQL container and applies the
// embedded migrations, including 000015. Skips (never passes) when no Docker
// provider is available.
func startMigratedPostgres(t *testing.T) string {
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

func openEventsPool(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func createProbeTable(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(context.Background(), fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s (id TEXT PRIMARY KEY, version BIGINT NOT NULL)`, probeTable))
	if err != nil {
		t.Fatalf("create probe table: %v", err)
	}
}

func depositCreatedEvent(t *testing.T, observationID, blockHash, txHash string, logIndex int, state string) Event {
	t.Helper()
	ev, err := NewEvent(Event{
		EventType:     EventTypeDepositObservationCreated,
		SchemaVersion: SchemaVersionV1,
		IdentityKind:  IdentityKindEVMLog,
		AggregateType: "deposit_observation",
		AggregateID:   observationID,
		Payload: map[string]any{
			"observation_id": observationID,
			"state":          state,
		},
		OccurredAt:  time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC),
		ChainID:     31337,
		BlockNumber: 100,
		BlockHash:   blockHash,
		TxHash:      txHash,
		LogIndex:    logIndex,
	})
	if err != nil {
		t.Fatalf("build created event: %v", err)
	}
	return ev
}

func statusChangedEvent(t *testing.T, observationID, from, to, reason string) Event {
	t.Helper()
	ev, err := NewEvent(Event{
		EventType:     EventTypeDepositObservationStatusChanged,
		SchemaVersion: SchemaVersionV1,
		IdentityKind:  IdentityKindBusinessObject,
		AggregateType: "deposit_observation",
		AggregateID:   observationID,
		Payload: map[string]any{
			"from_state": from,
			"to_state":   to,
			"reason":     reason,
		},
		OccurredAt: time.Date(2026, 9, 24, 12, 0, 1, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("build status_changed event: %v", err)
	}
	return ev
}

func revisionEvent(t *testing.T, observationID string, revises uuid.UUID) Event {
	t.Helper()
	ev, err := NewEvent(Event{
		EventType:     EventTypeDepositRevisionApplied,
		SchemaVersion: SchemaVersionV1,
		IdentityKind:  IdentityKindBusinessObject,
		AggregateType: "deposit_observation",
		AggregateID:   observationID,
		Payload: map[string]any{
			"superseded_identity": map[string]any{
				"event_id":   revises.String(),
				"block_hash": "0xblock-a",
			},
			"to_state": "orphaned",
			"reason":   "reorg",
		},
		OccurredAt:      time.Date(2026, 9, 24, 12, 0, 2, 0, time.UTC),
		ChainID:         31337,
		BlockNumber:     99,
		BlockHash:       "0xblock-a",
		RecoveryVersion: 1,
		RevisesEventID:  revises,
	})
	if err != nil {
		t.Fatalf("build revision event: %v", err)
	}
	return ev
}

func countOutboxRows(t *testing.T, pool *pgxpool.Pool, aggregateID string) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM outbox_events WHERE aggregate_type = 'deposit_observation' AND aggregate_id = $1`,
		aggregateID).Scan(&count); err != nil {
		t.Fatalf("count outbox rows: %v", err)
	}
	return count
}

func countProbeRows(t *testing.T, pool *pgxpool.Pool, id string) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(context.Background(),
		fmt.Sprintf(`SELECT count(*) FROM %s WHERE id = $1`, probeTable), id).Scan(&count); err != nil {
		t.Fatalf("count probe rows: %v", err)
	}
	return count
}

// TestAppendAtomicCommitAndRollback covers the commit/rollback paths for the
// evm_log, business_object and revision integration-point shapes: commit means
// the business row and the outbox row appear together with continuous object
// versions; rollback means neither exists and nothing is left behind.
func TestAppendAtomicCommitAndRollback(t *testing.T) {
	dsn := startMigratedPostgres(t)
	pool := openEventsPool(t, dsn)
	createProbeTable(t, pool)
	ctx := context.Background()

	// Commit path 1: evm_log created event, object version 1.
	created := depositCreatedEvent(t, "obs-1", "0xblock-a", "0xtx-a", 0, "pending")
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (id, version) VALUES ($1, $2)`, probeTable), "obs-1", 1); err != nil {
		t.Fatalf("insert probe row: %v", err)
	}
	res1, err := Append(ctx, tx, created)
	if err != nil {
		t.Fatalf("Append(created): %v", err)
	}
	if res1.AggregateVersion != 1 || res1.Noop {
		t.Fatalf("Append(created) = %#v, want version 1 insert", res1)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if got := countProbeRows(t, pool, "obs-1"); got != 1 {
		t.Fatalf("probe rows after commit = %d, want 1", got)
	}
	if got := countOutboxRows(t, pool, "obs-1"); got != 1 {
		t.Fatalf("outbox rows after commit = %d, want 1", got)
	}
	var identityKind, publishState, blockHash, storedHash, payloadState string
	var storedVersion int64
	if err := pool.QueryRow(ctx, `
		SELECT identity_kind, publish_state, aggregate_version, block_hash, payload_hash, payload->>'state'
		FROM outbox_events WHERE aggregate_id = 'obs-1'`).
		Scan(&identityKind, &publishState, &storedVersion, &blockHash, &storedHash, &payloadState); err != nil {
		t.Fatalf("read created row: %v", err)
	}
	if identityKind != string(IdentityKindEVMLog) || publishState != string(PublishStatePending) {
		t.Fatalf("created row = (%s, %s), want (evm_log, pending)", identityKind, publishState)
	}
	if storedVersion != 1 || blockHash != "0xblock-a" || payloadState != "pending" {
		t.Fatalf("created row = (v%d, %s, state=%s)", storedVersion, blockHash, payloadState)
	}
	if storedHash != created.PayloadHash() {
		t.Fatalf("stored payload_hash = %s, want %s", storedHash, created.PayloadHash())
	}

	// Commit path 2: business_object status_changed, same object, version 2.
	changed := statusChangedEvent(t, "obs-1", "pending", "confirmed", "policy_v1")
	tx2, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin 2: %v", err)
	}
	if _, err := tx2.Exec(ctx, fmt.Sprintf(`UPDATE %s SET version = 2 WHERE id = $1`, probeTable), "obs-1"); err != nil {
		t.Fatalf("update probe row: %v", err)
	}
	res2, err := Append(ctx, tx2, changed)
	if err != nil {
		t.Fatalf("Append(status_changed): %v", err)
	}
	if res2.AggregateVersion != 2 {
		t.Fatalf("status_changed version = %d, want 2", res2.AggregateVersion)
	}
	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("Commit 2: %v", err)
	}

	// Commit path 3: revision event with revises_event_id + recovery_version,
	// version 3.
	revision := revisionEvent(t, "obs-1", res1.EventID)
	tx3, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin 3: %v", err)
	}
	res3, err := Append(ctx, tx3, revision)
	if err != nil {
		t.Fatalf("Append(revision): %v", err)
	}
	if res3.AggregateVersion != 3 {
		t.Fatalf("revision version = %d, want 3", res3.AggregateVersion)
	}
	if err := tx3.Commit(ctx); err != nil {
		t.Fatalf("Commit 3: %v", err)
	}
	var revises uuid.UUID
	var recoveryVersion int64
	if err := pool.QueryRow(ctx,
		`SELECT revises_event_id, recovery_version FROM outbox_events WHERE aggregate_id = 'obs-1' AND aggregate_version = 3`).
		Scan(&revises, &recoveryVersion); err != nil {
		t.Fatalf("read revision row: %v", err)
	}
	if revises != res1.EventID || recoveryVersion != 1 {
		t.Fatalf("revision row = (revises=%s, recovery=%d), want (%s, 1)", revises, recoveryVersion, res1.EventID)
	}

	// Versions are continuous 1,2,3 for the object.
	var versions []int64
	rows, err := pool.Query(ctx,
		`SELECT aggregate_version FROM outbox_events WHERE aggregate_id = 'obs-1' ORDER BY aggregate_version`)
	if err != nil {
		t.Fatalf("read versions: %v", err)
	}
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("scan version: %v", err)
		}
		versions = append(versions, v)
	}
	rows.Close()
	if len(versions) != 3 || versions[0] != 1 || versions[1] != 2 || versions[2] != 3 {
		t.Fatalf("object versions = %v, want [1 2 3]", versions)
	}

	// Rollback path: business row + event roll back together, 0 residue.
	before := countOutboxRows(t, pool, "obs-2")
	tx4, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin 4: %v", err)
	}
	if _, err := tx4.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (id, version) VALUES ($1, $2)`, probeTable), "obs-2", 1); err != nil {
		t.Fatalf("insert probe row 2: %v", err)
	}
	rolledBack := depositCreatedEvent(t, "obs-2", "0xblock-b", "0xtx-b", 1, "pending")
	if _, err := Append(ctx, tx4, rolledBack); err != nil {
		t.Fatalf("Append(rolled back): %v", err)
	}
	if err := tx4.Rollback(ctx); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if got := countProbeRows(t, pool, "obs-2"); got != 0 {
		t.Fatalf("probe rows after rollback = %d, want 0", got)
	}
	if got := countOutboxRows(t, pool, "obs-2"); got != before {
		t.Fatalf("outbox rows after rollback = %d, want %d (no residue)", got, before)
	}
}

// TestAppendIntegrationSameIdentitySameContentIsNoop covers the idempotent
// repeat: the same evm_log identity with the same payload returns the existing
// row and leaves the table untouched.
func TestAppendIntegrationSameIdentitySameContentIsNoop(t *testing.T) {
	dsn := startMigratedPostgres(t)
	pool := openEventsPool(t, dsn)
	ctx := context.Background()

	ev := depositCreatedEvent(t, "obs-noop", "0xblock-c", "0xtx-c", 2, "pending")
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	first, err := Append(ctx, tx, ev)
	if err != nil {
		t.Fatalf("first Append: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	tx2, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin 2: %v", err)
	}
	second, err := Append(ctx, tx2, ev)
	if err != nil {
		t.Fatalf("repeat Append: %v", err)
	}
	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("Commit 2: %v", err)
	}
	if !second.Noop {
		t.Fatal("repeat Append was not an idempotent no-op")
	}
	if second.EventID != first.EventID || second.OutboxID != first.OutboxID {
		t.Fatalf("repeat identity = (%s, %d), want (%s, %d)", second.EventID, second.OutboxID, first.EventID, first.OutboxID)
	}
	if second.AggregateVersion != 1 {
		t.Fatalf("no-op AggregateVersion = %d, want the existing row version 1", second.AggregateVersion)
	}
	if got := countOutboxRows(t, pool, "obs-noop"); got != 1 {
		t.Fatalf("outbox rows after repeat = %d, want 1", got)
	}
}

// TestAppendSameIdentityDifferentContentIsRejected covers the conflict rule:
// the same identity with a different payload refuses the transaction, alerts,
// and never overwrites the stored row.
func TestAppendSameIdentityDifferentContentIsRejected(t *testing.T) {
	dsn := startMigratedPostgres(t)
	pool := openEventsPool(t, dsn)
	ctx := context.Background()

	observer := &countingConflictObserver{}
	SetIdentityConflictObserver(observer)
	t.Cleanup(func() { SetIdentityConflictObserver(nil) })

	first := depositCreatedEvent(t, "obs-conflict", "0xblock-d", "0xtx-d", 3, "pending")
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if _, err := Append(ctx, tx, first); err != nil {
		t.Fatalf("first Append: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	conflicting := depositCreatedEvent(t, "obs-conflict", "0xblock-d", "0xtx-d", 3, "confirmed")
	tx2, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin 2: %v", err)
	}
	if _, err := Append(ctx, tx2, conflicting); !errors.Is(err, ErrIdentityConflict) {
		t.Fatalf("conflicting Append error = %v, want ErrIdentityConflict", err)
	}
	if err := tx2.Rollback(ctx); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if observer.count() != 1 || observer.last() != EventTypeDepositObservationCreated {
		t.Fatalf("conflict alerts = %v, want one for %s", observer.types, EventTypeDepositObservationCreated)
	}
	if got := countOutboxRows(t, pool, "obs-conflict"); got != 1 {
		t.Fatalf("outbox rows after conflict = %d, want 1", got)
	}
	var payloadState string
	if err := pool.QueryRow(ctx,
		`SELECT payload->>'state' FROM outbox_events WHERE aggregate_id = 'obs-conflict'`).Scan(&payloadState); err != nil {
		t.Fatalf("read stored payload: %v", err)
	}
	if payloadState != "pending" {
		t.Fatalf("stored payload state = %q, want the first writer's %q (no silent overwrite)", payloadState, "pending")
	}
}

// connectNamed opens a dedicated connection with a recognizable
// application_name so the race test can observe the blocked insert.
func connectNamed(t *testing.T, ctx context.Context, dsn, name string) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("pgx.Connect(%s): %v", name, err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	if _, err := conn.Exec(ctx, "SET application_name = '"+name+"'"); err != nil {
		t.Fatalf("SET application_name: %v", err)
	}
	return conn
}

// waitForBlockedStatement polls pg_stat_activity until the named connection is
// waiting on a lock: the deterministic signal that its version query already
// ran against the pre-commit snapshot and the insert is now blocked on the
// identity index.
func waitForBlockedStatement(t *testing.T, ctx context.Context, watcher *pgx.Conn, applicationName string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		var waiting int
		if err := watcher.QueryRow(ctx,
			`SELECT count(*) FROM pg_stat_activity WHERE application_name = $1 AND wait_event IS NOT NULL`,
			applicationName).Scan(&waiting); err != nil {
			t.Fatalf("poll pg_stat_activity: %v", err)
		}
		if waiting > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s never blocked on the outbox identity index", applicationName)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

type appendOutcome struct {
	res AppendResult
	err error
}

// TestAppendConcurrentVersionRace covers the UNIQUE backstop: two producers
// that both observed the same high-water mark derive the same
// aggregate_version. Equal payloads converge to one row (no-op); different
// payloads refuse the loser and never overwrite the winner.
func TestAppendConcurrentVersionRace(t *testing.T) {
	dsn := startMigratedPostgres(t)
	pool := openEventsPool(t, dsn)
	ctx := context.Background()

	observer := &countingConflictObserver{}
	SetIdentityConflictObserver(observer)
	t.Cleanup(func() { SetIdentityConflictObserver(nil) })

	conn1 := connectNamed(t, ctx, dsn, "t015race1")
	conn2 := connectNamed(t, ctx, dsn, "t015race2")
	watcher := connectNamed(t, ctx, dsn, "t015watch")

	// Part A: both transactions derive version 1 with the same content.
	same := statusChangedEvent(t, "obs-race", "pending", "confirmed", "policy_v1")
	tx1, err := conn1.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin race tx1: %v", err)
	}
	res1, err := Append(ctx, tx1, same)
	if err != nil {
		t.Fatalf("race tx1 Append: %v", err)
	}
	if res1.AggregateVersion != 1 {
		t.Fatalf("race tx1 version = %d, want 1", res1.AggregateVersion)
	}

	tx2, err := conn2.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin race tx2: %v", err)
	}
	outcomes := make(chan appendOutcome, 1)
	go func() {
		res, err := Append(ctx, tx2, same)
		outcomes <- appendOutcome{res: res, err: err}
	}()
	waitForBlockedStatement(t, ctx, watcher, "t015race2")
	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("race tx1 Commit: %v", err)
	}
	got := <-outcomes
	if got.err != nil {
		t.Fatalf("race tx2 Append error = %v, want the idempotent no-op", got.err)
	}
	if !got.res.Noop || got.res.AggregateVersion != 1 {
		t.Fatalf("race tx2 = %#v, want noop at version 1", got.res)
	}
	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("race tx2 Commit: %v", err)
	}
	if got := countOutboxRows(t, pool, "obs-race"); got != 1 {
		t.Fatalf("outbox rows after equal-content race = %d, want 1", got)
	}
	if observer.count() != 0 {
		t.Fatalf("equal-content race raised %d conflict alerts, want 0", observer.count())
	}

	// Part B: both transactions derive version 2 with different content.
	winner := statusChangedEvent(t, "obs-race", "confirmed", "orphaned", "reorg")
	loser := statusChangedEvent(t, "obs-race", "confirmed", "reinstated", "other")
	tx3, err := conn1.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin race tx3: %v", err)
	}
	res3, err := Append(ctx, tx3, winner)
	if err != nil {
		t.Fatalf("race tx3 Append: %v", err)
	}
	if res3.AggregateVersion != 2 {
		t.Fatalf("race tx3 version = %d, want 2", res3.AggregateVersion)
	}

	tx4, err := conn2.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin race tx4: %v", err)
	}
	outcomes = make(chan appendOutcome, 1)
	go func() {
		res, err := Append(ctx, tx4, loser)
		outcomes <- appendOutcome{res: res, err: err}
	}()
	waitForBlockedStatement(t, ctx, watcher, "t015race2")
	if err := tx3.Commit(ctx); err != nil {
		t.Fatalf("race tx3 Commit: %v", err)
	}
	got = <-outcomes
	if !errors.Is(got.err, ErrIdentityConflict) {
		t.Fatalf("race tx4 error = %v, want ErrIdentityConflict", got.err)
	}
	if err := tx4.Rollback(ctx); err != nil {
		t.Fatalf("race tx4 Rollback: %v", err)
	}
	if observer.count() != 1 {
		t.Fatalf("conflict alerts after different-content race = %d, want 1", observer.count())
	}
	if got := countOutboxRows(t, pool, "obs-race"); got != 2 {
		t.Fatalf("outbox rows after conflicting race = %d, want 2", got)
	}
	var storedTo string
	if err := pool.QueryRow(ctx,
		`SELECT payload->>'to_state' FROM outbox_events WHERE aggregate_id = 'obs-race' AND aggregate_version = 2`).
		Scan(&storedTo); err != nil {
		t.Fatalf("read raced payload: %v", err)
	}
	if storedTo != "orphaned" {
		t.Fatalf("raced payload to_state = %q, want the winner's %q (0 silent overwrite)", storedTo, "orphaned")
	}
	if winner.PayloadHash() == loser.PayloadHash() {
		t.Fatal("test setup: winner and loser payloads must differ")
	}
}
