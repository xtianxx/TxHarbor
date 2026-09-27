//go:build integration

// obligation_integration_test.go is the T040 producer-side integration layer
// for the durable expectation carrier (event_obligation, migration 000017)
// over the real migrated schema:
//
//   - every catalog v1 event type writes its marker in the same transaction
//     as the outbox row (commit means both, rollback means neither);
//   - an idempotent no-op replay writes no second marker and never backfills
//     history (zero appends on replay);
//   - a marker failure fails Append so the caller's transaction rolls back the
//     business row, the event row and the marker together (no half-committed
//     emission);
//   - the carrier survives a legal events-admin retention prune while the
//     published outbox row is deleted: this is the evidence that lets the
//     014 read path separate "possibly legally trimmed" from a real loss.
//
// CAPABILITY BOUNDARY: the marker is written because Append ran; a producer
// code path that never calls Append produces neither event nor marker and is
// beyond this carrier's detection range (see the append.go comment and
// docs/evidence/014/t040_discriminator_evidence.md §2). These tests prove the
// same-transaction atomicity of the carrier, not the completeness of all
// producer paths.
//
// PostgreSQL comes from testcontainers; without a Docker provider the package
// reports NOT RUN (t.Skip), never a pass.
package events

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// countObligationRows counts the marker rows of one aggregate.
func countObligationRows(t *testing.T, pool *pgxpool.Pool, aggregateType, aggregateID string) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM event_obligation WHERE aggregate_type = $1 AND aggregate_id = $2`,
		aggregateType, aggregateID).Scan(&count); err != nil {
		t.Fatalf("count obligation rows: %v", err)
	}
	return count
}

// appendInOwnTx commits one event through the real Append path.
func appendInOwnTx(t *testing.T, pool *pgxpool.Pool, ev Event) AppendResult {
	t.Helper()
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	res, err := Append(ctx, tx, ev)
	if err != nil {
		t.Fatalf("Append(%s): %v", ev.EventType, err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit(%s): %v", ev.EventType, err)
	}
	return res
}

// TestIntegrationAppendWritesObligationForEveryCatalogType pins the producer
// wiring against the frozen 8-type catalog: every type emits its marker with
// the event's aggregate triple and the transition instant.
func TestIntegrationAppendWritesObligationForEveryCatalogType(t *testing.T) {
	dsn := startMigratedPostgres(t)
	pool := openEventsPool(t, dsn)
	ctx := context.Background()

	revisionRef := uuid.New()
	cases := []struct {
		eventType     string
		aggregateType string
		aggregateID   string
		event         func() Event
	}{
		{
			EventTypeDepositObservationCreated, "deposit_observation", "obs-created",
			func() Event { return depositCreatedEvent(t, "obs-created", "0xblock-c", "0xtx-c", 0, "pending") },
		},
		{
			EventTypeDepositObservationStatusChanged, "deposit_observation", "obs-status",
			func() Event { return statusChangedEvent(t, "obs-status", "pending", "confirmed", "policy_v1") },
		},
		{
			EventTypeDepositObservationReinstated, "deposit_observation", "obs-reinstated",
			func() Event {
				ev, err := NewEvent(Event{
					EventType: EventTypeDepositObservationReinstated, SchemaVersion: SchemaVersionV1,
					IdentityKind:  IdentityKindBusinessObject,
					AggregateType: "deposit_observation", AggregateID: "obs-reinstated",
					Payload:    map[string]any{"observation_id": "obs-reinstated", "reason": "reorg_revived"},
					OccurredAt: time.Date(2026, 9, 24, 12, 1, 0, 0, time.UTC),
				})
				if err != nil {
					t.Fatalf("build reinstated event: %v", err)
				}
				return ev
			},
		},
		{
			EventTypeDepositConfirmationConfirmed, "deposit_observation", "obs-confirmed",
			func() Event {
				ev, err := NewEvent(Event{
					EventType: EventTypeDepositConfirmationConfirmed, SchemaVersion: SchemaVersionV1,
					IdentityKind:  IdentityKindBusinessObject,
					AggregateType: "deposit_observation", AggregateID: "obs-confirmed",
					Payload: map[string]any{
						"policy_version": 1, "confirmed_block_number": 100, "confirmed_block_hash": "0xblock-c",
					},
					OccurredAt: time.Date(2026, 9, 24, 12, 2, 0, 0, time.UTC),
				})
				if err != nil {
					t.Fatalf("build confirmed event: %v", err)
				}
				return ev
			},
		},
		{
			EventTypeDepositRevisionApplied, "deposit_observation", "obs-revision",
			func() Event { return revisionEvent(t, "obs-revision", revisionRef) },
		},
		{
			EventTypeWithdrawalRequestReceived, "withdrawal_request", "req-1",
			func() Event {
				ev, err := NewEvent(Event{
					EventType: EventTypeWithdrawalRequestReceived, SchemaVersion: SchemaVersionV1,
					IdentityKind:  IdentityKindBusinessObject,
					AggregateType: "withdrawal_request", AggregateID: "req-1",
					Payload:    map[string]any{"request_id": "req-1", "caller": "it-caller", "state": "accepted"},
					OccurredAt: time.Date(2026, 9, 24, 12, 3, 0, 0, time.UTC),
					SourceKind: "withdrawal_intake", SourceID: "req-1",
				})
				if err != nil {
					t.Fatalf("build request event: %v", err)
				}
				return ev
			},
		},
		{
			EventTypeWithdrawalExecutionStateChanged, "withdrawal_intent", "int-1",
			func() Event {
				ev, err := NewEvent(Event{
					EventType: EventTypeWithdrawalExecutionStateChanged, SchemaVersion: SchemaVersionV1,
					IdentityKind:  IdentityKindBusinessObject,
					AggregateType: "withdrawal_intent", AggregateID: "int-1",
					Payload: map[string]any{
						"from_state": "created", "to_state": "signing", "intent_id": "int-1",
					},
					OccurredAt: time.Date(2026, 9, 24, 12, 4, 0, 0, time.UTC),
					SourceKind: "withdrawal_execution", SourceID: "int-1",
				})
				if err != nil {
					t.Fatalf("build state_changed event: %v", err)
				}
				return ev
			},
		},
		{
			EventTypeWithdrawalExecutionRevised, "withdrawal_intent", "int-revised",
			func() Event {
				ev, err := NewEvent(Event{
					EventType: EventTypeWithdrawalExecutionRevised, SchemaVersion: SchemaVersionV1,
					IdentityKind:  IdentityKindBusinessObject,
					AggregateType: "withdrawal_intent", AggregateID: "int-revised",
					Payload: map[string]any{
						"superseded_identity": map[string]any{"event_id": revisionRef.String()},
						"to_state":            "revised",
						"reason":              "it",
					},
					OccurredAt:      time.Date(2026, 9, 24, 12, 5, 0, 0, time.UTC),
					ChainID:         31337,
					BlockNumber:     101,
					BlockHash:       "0xblock-r",
					RecoveryVersion: 1,
					RevisesEventID:  revisionRef,
					SourceKind:      "withdrawal_execution",
					SourceID:        "int-revised",
				})
				if err != nil {
					t.Fatalf("build revised event: %v", err)
				}
				return ev
			},
		},
	}

	seen := map[string]bool{}
	for _, tc := range cases {
		if seen[tc.eventType] {
			t.Fatalf("duplicate catalog case %s", tc.eventType)
		}
		seen[tc.eventType] = true
		t.Run(tc.eventType, func(t *testing.T) {
			ev := tc.event()
			res := appendInOwnTx(t, pool, ev)
			if res.Noop {
				t.Fatalf("first Append was a no-op")
			}
			var (
				storedType string
				storedAt   time.Time
				sourceKind string
				storedVer  int64
			)
			if err := pool.QueryRow(ctx, `
				SELECT expected_event_type, obligated_at, source_kind, aggregate_version
				FROM event_obligation WHERE aggregate_type = $1 AND aggregate_id = $2`,
				tc.aggregateType, tc.aggregateID).Scan(&storedType, &storedAt, &sourceKind, &storedVer); err != nil {
				t.Fatalf("read obligation marker: %v", err)
			}
			if storedType != tc.eventType {
				t.Fatalf("expected_event_type = %q, want %q", storedType, tc.eventType)
			}
			if !storedAt.Equal(ev.OccurredAt) {
				t.Fatalf("obligated_at = %v, want the transition instant %v", storedAt, ev.OccurredAt)
			}
			if storedVer != res.AggregateVersion {
				t.Fatalf("marker aggregate_version = %d, want the emitted version %d", storedVer, res.AggregateVersion)
			}
			if ev.SourceKind != "" && sourceKind != ev.SourceKind {
				t.Fatalf("marker source_kind = %q, want %q", sourceKind, ev.SourceKind)
			}
		})
	}
	if len(seen) != 8 {
		t.Fatalf("catalog coverage = %d types, want the frozen 8", len(seen))
	}
}

// TestIntegrationAppendObligationAtomicRollback pins the T040 atomicity
// contract: commit means business row + event row + marker together; rollback
// means none of them exists.
func TestIntegrationAppendObligationAtomicRollback(t *testing.T) {
	dsn := startMigratedPostgres(t)
	pool := openEventsPool(t, dsn)
	createProbeTable(t, pool)
	ctx := context.Background()

	t.Run("commit writes all three", func(t *testing.T) {
		ev := depositCreatedEvent(t, "obs-atomic", "0xblock-a", "0xtx-a", 0, "pending")
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO t015_business_probe (id, version) VALUES ($1, $2)`, "obs-atomic", 1); err != nil {
			t.Fatalf("insert probe row: %v", err)
		}
		if _, err := Append(ctx, tx, ev); err != nil {
			t.Fatalf("Append: %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("Commit: %v", err)
		}
		if got := countProbeRows(t, pool, "obs-atomic"); got != 1 {
			t.Fatalf("probe rows = %d, want 1", got)
		}
		if got := countOutboxRows(t, pool, "obs-atomic"); got != 1 {
			t.Fatalf("outbox rows = %d, want 1", got)
		}
		if got := countObligationRows(t, pool, "deposit_observation", "obs-atomic"); got != 1 {
			t.Fatalf("obligation rows = %d, want 1", got)
		}
	})

	t.Run("rollback leaves none", func(t *testing.T) {
		ev := depositCreatedEvent(t, "obs-rolled-back", "0xblock-b", "0xtx-b", 0, "pending")
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO t015_business_probe (id, version) VALUES ($1, $2)`, "obs-rolled-back", 1); err != nil {
			t.Fatalf("insert probe row: %v", err)
		}
		if _, err := Append(ctx, tx, ev); err != nil {
			t.Fatalf("Append: %v", err)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatalf("Rollback: %v", err)
		}
		if got := countProbeRows(t, pool, "obs-rolled-back"); got != 0 {
			t.Fatalf("probe rows after rollback = %d, want 0", got)
		}
		if got := countOutboxRows(t, pool, "obs-rolled-back"); got != 0 {
			t.Fatalf("outbox rows after rollback = %d, want 0", got)
		}
		if got := countObligationRows(t, pool, "deposit_observation", "obs-rolled-back"); got != 0 {
			t.Fatalf("obligation rows after rollback = %d, want 0", got)
		}
	})
}

// TestIntegrationAppendReplayWritesNoSecondObligation pins the idempotent
// replay rule: the no-op path appends zero rows and never backfills a marker.
func TestIntegrationAppendReplayWritesNoSecondObligation(t *testing.T) {
	dsn := startMigratedPostgres(t)
	pool := openEventsPool(t, dsn)
	ctx := context.Background()

	ev := depositCreatedEvent(t, "obs-replay", "0xblock-a", "0xtx-a", 0, "pending")
	first := appendInOwnTx(t, pool, ev)
	if first.Noop {
		t.Fatal("first Append was a no-op")
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	second, err := Append(ctx, tx, ev)
	if err != nil {
		t.Fatalf("replay Append: %v", err)
	}
	if !second.Noop {
		t.Fatal("replay Append did not report the idempotent no-op")
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit replay: %v", err)
	}
	if got := countObligationRows(t, pool, "deposit_observation", "obs-replay"); got != 1 {
		t.Fatalf("obligation rows after replay = %d, want 1 (zero appends on replay)", got)
	}
}

// TestIntegrationAppendFailsClosedWhenMarkerCannotBeWritten pins the failure
// direction: a marker write that violates the closed aggregate/event mapping
// fails Append, so the caller rolls back the business row and the event row
// together with the marker (no half-committed emission).
func TestIntegrationAppendFailsClosedWhenMarkerCannotBeWritten(t *testing.T) {
	dsn := startMigratedPostgres(t)
	pool := openEventsPool(t, dsn)
	createProbeTable(t, pool)
	ctx := context.Background()

	// withdrawal.request.received on the deposit_observation aggregate is not
	// in the closed 000017 mapping; the event itself passes 013 validation,
	// so the failure is owned by the carrier constraint.
	ev, err := NewEvent(Event{
		EventType: EventTypeWithdrawalRequestReceived, SchemaVersion: SchemaVersionV1,
		IdentityKind:  IdentityKindBusinessObject,
		AggregateType: "deposit_observation", AggregateID: "obs-unmapped",
		Payload:    map[string]any{"request_id": "req-x", "caller": "it", "state": "accepted"},
		OccurredAt: time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("build event: %v", err)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO t015_business_probe (id, version) VALUES ($1, $2)`, "obs-unmapped", 1); err != nil {
		t.Fatalf("insert probe row: %v", err)
	}
	if _, err := Append(ctx, tx, ev); err == nil {
		t.Fatal("Append accepted an unmapped aggregate pair, want a constraint failure")
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if got := countProbeRows(t, pool, "obs-unmapped"); got != 0 {
		t.Fatalf("probe rows = %d, want 0 (rolled back with the marker)", got)
	}
	if got := countOutboxRows(t, pool, "obs-unmapped"); got != 0 {
		t.Fatalf("outbox rows = %d, want 0 (rolled back with the marker)", got)
	}
	if got := countObligationRows(t, pool, "deposit_observation", "obs-unmapped"); got != 0 {
		t.Fatalf("obligation rows = %d, want 0", got)
	}
}

// TestIntegrationObligationSurvivesRetentionPrune pins the carrier's reason to
// exist: a legal events-admin retention prune deletes the published outbox row
// but never the expectation marker, so the 014 read path can separate
// "possibly legally trimmed" from a real loss.
func TestIntegrationObligationSurvivesRetentionPrune(t *testing.T) {
	dsn := startMigratedPostgres(t)
	pool := openEventsPool(t, dsn)
	ctx := context.Background()

	ev := depositCreatedEvent(t, "obs-pruned", "0xblock-a", "0xtx-a", 0, "pending")
	appendInOwnTx(t, pool, ev)
	if _, err := pool.Exec(ctx, `
		UPDATE outbox_events
		SET publish_state = 'published', published_at = now() - interval '2 hours'
		WHERE aggregate_id = 'obs-pruned'`); err != nil {
		t.Fatalf("mark event published: %v", err)
	}

	result, err := RetentionPrune(ctx, pool, PruneOptions{
		Retention: time.Hour, Reason: "integration retention", Operator: "it-operator",
	})
	if err != nil {
		t.Fatalf("RetentionPrune: %v", err)
	}
	if result.Deleted != 1 {
		t.Fatalf("retention deleted %d rows, want the 1 published event", result.Deleted)
	}
	if got := countOutboxRows(t, pool, "obs-pruned"); got != 0 {
		t.Fatalf("outbox rows after prune = %d, want 0 (legally trimmed)", got)
	}
	if got := countObligationRows(t, pool, "deposit_observation", "obs-pruned"); got != 1 {
		t.Fatalf("obligation rows after prune = %d, want 1 (the marker survives the trim)", got)
	}
}
