//go:build integration

// revision_consumer_integration_test.go is the B6/T057 V-REVISION consumer
// layer on real PostgreSQL: the T4 transaction applies revision facts
// idempotently (duplicates, out-of-order older versions and a revision that
// arrives before its original all converge with zero overwrite), a
// revision-aware effect tracks the effective state so Orphaned data is never
// treated as a valid canonical fact, the FR-08 reinstated fact is applied as a
// revival of the SAME object (never a second observation), malformed revision
// envelopes are quarantined (missing revises_event_id / recovery_version ->
// non_retryable; chain mismatch -> identity_mismatch) with the partition
// continuing, and no revision delivery creates a withdrawal intent, nonce,
// signature or broadcast (007/008/009/010/011 row counts unchanged).
//
// The effective-state table is test scaffolding only; the production consumer
// keeps its persistent inbox/version/quarantine state in PostgreSQL. The
// reference-consumer FR-16 boundary is unchanged: this evidence covers this
// project's event identity/version/delivery semantics plus the consumer
// idempotency contract, never an external real ledger.
package events

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// revisionStateTable is the revision-aware consumer effect's scratch table.
const revisionStateTable = "t057_revision_state"

func createRevisionStateTable(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
	consumer_name   TEXT    NOT NULL,
	aggregate_id    TEXT    NOT NULL,
	effective_state TEXT    NOT NULL,
	canonical       BOOLEAN NOT NULL,
	last_version    BIGINT  NOT NULL,
	last_event_type TEXT    NOT NULL,
	PRIMARY KEY (consumer_name, aggregate_id)
)`, revisionStateTable)); err != nil {
		t.Fatalf("create revision state table: %v", err)
	}
}

// t057ConsumerName is the test effect's consumer identity (each test boots its
// own database, so one name is enough for the revision-aware effect).
const t057ConsumerName = "t057-revision-consumer"

// revisionStateEffect applies the catalog revision semantics as a consumer
// effect: the effective state follows status_changed / revision.applied
// to_state, reinstated revives the original object, and canonical is exactly
// "the effective state is not Orphaned". It is idempotent per event because
// the T4 transaction runs it at most once.
type revisionStateEffect struct{ consumerName string }

func (e revisionStateEffect) Apply(ctx context.Context, tx pgx.Tx, env Envelope) error {
	state := ""
	switch env.EventType {
	case EventTypeDepositObservationCreated:
		state = "pending"
	case EventTypeDepositObservationStatusChanged, EventTypeDepositRevisionApplied:
		state = fmt.Sprint(env.Payload["to_state"])
	case EventTypeDepositObservationReinstated:
		state = "pending"
	default:
		state = env.EventType
	}
	canonical := state != "orphaned"
	_, err := tx.Exec(ctx, fmt.Sprintf(`
INSERT INTO %s (consumer_name, aggregate_id, effective_state, canonical, last_version, last_event_type)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (consumer_name, aggregate_id) DO UPDATE
SET effective_state = EXCLUDED.effective_state,
    canonical       = EXCLUDED.canonical,
    last_version    = GREATEST(%s.last_version, EXCLUDED.last_version),
    last_event_type = EXCLUDED.last_event_type`, revisionStateTable, revisionStateTable),
		e.consumerName, env.AggregateID, state, canonical, env.AggregateVersion, env.EventType)
	return err
}

// revisionStateRow is one tracked effective state.
type revisionStateRow struct {
	State     string
	Canonical bool
	Version   int64
	EventType string
	Objects   int
}

func readRevisionState(t *testing.T, pool *pgxpool.Pool, consumerName, aggregateID string) revisionStateRow {
	t.Helper()
	ctx := context.Background()
	var row revisionStateRow
	if err := pool.QueryRow(ctx, fmt.Sprintf(`
SELECT effective_state, canonical, last_version, last_event_type,
       (SELECT count(*) FROM %s WHERE consumer_name = $1)
FROM %s WHERE consumer_name = $1 AND aggregate_id = $2`,
		revisionStateTable, revisionStateTable), consumerName, aggregateID).
		Scan(&row.State, &row.Canonical, &row.Version, &row.EventType, &row.Objects); err != nil {
		t.Fatalf("read revision state for %s: %v", aggregateID, err)
	}
	return row
}

// t057CreatedEvent builds one deposit.observation.created fact for the
// aggregate (evm_log identity; version 1 by construction).
func t057CreatedEvent(t *testing.T, observationID, blockHash, txHash string) Event {
	t.Helper()
	ev, err := NewEvent(Event{
		EventType:     EventTypeDepositObservationCreated,
		SchemaVersion: SchemaVersionV1,
		IdentityKind:  IdentityKindEVMLog,
		AggregateType: "deposit_observation",
		AggregateID:   observationID,
		Payload: map[string]any{
			"observation_id": observationID, "state": "pending", "chain_id": int64(1),
			"block_number": int64(10), "block_hash": blockHash, "tx_hash": txHash, "log_index": int64(0),
		},
		OccurredAt: time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC),
		ChainID:    1, BlockNumber: 10, BlockHash: blockHash, TxHash: txHash, LogIndex: 0,
	})
	if err != nil {
		t.Fatalf("build created event: %v", err)
	}
	return ev
}

// t057StatusChangedEvent builds one observation status transition fact.
func t057StatusChangedEvent(t *testing.T, observationID, from, to, reason string) Event {
	t.Helper()
	ev, err := NewEvent(Event{
		EventType:     EventTypeDepositObservationStatusChanged,
		SchemaVersion: SchemaVersionV1,
		IdentityKind:  IdentityKindBusinessObject,
		AggregateType: "deposit_observation",
		AggregateID:   observationID,
		Payload: map[string]any{
			"from_state": from, "to_state": to, "reason": reason,
			"observation_id": observationID, "chain_id": int64(1), "block_number": int64(10),
		},
		OccurredAt: time.Date(2026, 9, 24, 12, 0, 1, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("build status_changed event: %v", err)
	}
	return ev
}

// t057ReinstatedEvent builds one FR-08 revival fact for the original
// observation.
func t057ReinstatedEvent(t *testing.T, observationID string) Event {
	t.Helper()
	ev, err := NewEvent(Event{
		EventType:     EventTypeDepositObservationReinstated,
		SchemaVersion: SchemaVersionV1,
		IdentityKind:  IdentityKindBusinessObject,
		AggregateType: "deposit_observation",
		AggregateID:   observationID,
		Payload: map[string]any{
			"observation_id": observationID, "reason": "reorg_revived",
			"revive_basis": "same_block_hash_recanonicalized",
			"chain_id":     int64(1), "block_number": int64(10),
		},
		OccurredAt: time.Date(2026, 9, 24, 12, 0, 3, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("build reinstated event: %v", err)
	}
	return ev
}

// t057RevisionAppliedEvent builds one deposit.revision.applied fact carrying
// the complete revision envelope (superseded event, old block identity,
// disposition, reason, recovery version).
func t057RevisionAppliedEvent(t *testing.T, observationID string, revises uuid.UUID, toState, reason string) Event {
	t.Helper()
	blockHash := "0x" + strings.Repeat("a1", 32)
	ev, err := NewEvent(Event{
		EventType:     EventTypeDepositRevisionApplied,
		SchemaVersion: SchemaVersionV1,
		IdentityKind:  IdentityKindBusinessObject,
		AggregateType: "deposit_observation",
		AggregateID:   observationID,
		Payload: map[string]any{
			"superseded_identity": map[string]any{
				"event_id": revises.String(), "event_type": EventTypeDepositObservationStatusChanged,
				"block_hash": blockHash,
			},
			"to_state": toState, "reason": reason, "observation_id": observationID,
			"chain_id": int64(1), "block_number": int64(10), "block_hash": blockHash,
		},
		OccurredAt:      time.Date(2026, 9, 24, 12, 0, 2, 0, time.UTC),
		ChainID:         1,
		BlockNumber:     10,
		BlockHash:       blockHash,
		RecoveryVersion: 1,
		RevisesEventID:  revises,
	})
	if err != nil {
		t.Fatalf("build revision.applied event: %v", err)
	}
	return ev
}

// mutateRevisionWire rewrites one JSON field of a rendered wire envelope.
func mutateRevisionWire(t *testing.T, raw []byte, mutate func(wire map[string]any)) []byte {
	t.Helper()
	var wire map[string]any
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatalf("decode wire envelope: %v", err)
	}
	mutate(wire)
	body, err := json.Marshal(wire)
	if err != nil {
		t.Fatalf("encode wire envelope: %v", err)
	}
	return body
}

// TestT057RevisionConvergenceAndNoOrphanCanonical drives created -> orphan ->
// revision -> reinstated -> revision through the real T4 consumer and asserts
// convergence, idempotency, out-of-order safety, single-object revival and
// zero payment effects.
func TestT057RevisionConvergenceAndNoOrphanCanonical(t *testing.T) {
	dsn := startMigratedPostgres(t)
	pool := openEventsPool(t, dsn)
	createRevisionStateTable(t, pool)
	seedUpstreamFixture(t, pool)

	const obsID = "obs-t057-a"
	blockHash := "0x" + strings.Repeat("a2", 32)
	txHash := "0x" + strings.Repeat("b3", 32)

	createdID, _ := appendConsumerEvent(t, pool, t057CreatedEvent(t, obsID, blockHash, txHash))
	orphanID, _ := appendConsumerEvent(t, pool, t057StatusChangedEvent(t, obsID, "pending", "orphaned", "reorg_invalidated"))
	revisionID, _ := appendConsumerEvent(t, pool, t057RevisionAppliedEvent(t, obsID, createdID, "orphaned", "reorg_invalidated"))
	reinstatedID, _ := appendConsumerEvent(t, pool, t057ReinstatedEvent(t, obsID))
	revivalRevisionID, _ := appendConsumerEvent(t, pool, t057RevisionAppliedEvent(t, obsID, revisionID, "pending", "reorg_revived"))

	consumer := newTestConsumer(t, pool, t057ConsumerName,
		revisionStateEffect{consumerName: t057ConsumerName}, nil, 200*time.Millisecond)
	offset := int64(0)
	deliver := func(eventID uuid.UUID) ProcessResult {
		result := mustProcess(t, consumer, Message{
			Topic: "txharbor.events.v1", Partition: 0, Offset: offset,
			Value: wireEnvelope(t, pool, eventID),
		})
		offset++
		return result
	}

	paymentsBefore := countUpstreamRows(t, pool)

	if result := deliver(createdID); result.Outcome != OutcomeApplied {
		t.Fatalf("created outcome = %s, want applied", result.Outcome)
	}
	if result := deliver(orphanID); result.Outcome != OutcomeApplied {
		t.Fatalf("orphan outcome = %s, want applied", result.Outcome)
	}
	if result := deliver(revisionID); result.Outcome != OutcomeApplied {
		t.Fatalf("revision outcome = %s, want applied", result.Outcome)
	}
	state := readRevisionState(t, pool, t057ConsumerName, obsID)
	if state.State != "orphaned" || state.Canonical {
		t.Fatalf("state after orphan revision = %+v, want orphaned and NOT valid canonical", state)
	}
	if state.Version != 3 || state.EventType != EventTypeDepositRevisionApplied {
		t.Fatalf("state version/type = %d/%s, want 3/revision.applied", state.Version, state.EventType)
	}

	// Duplicate deliveries (the same revision and the already-applied
	// original facts): the inbox absorbs them, no second effect runs and the
	// newer state is never overwritten.
	duplicate := mustProcess(t, consumer, Message{
		Topic: "txharbor.events.v1", Partition: 0, Offset: 0, Value: wireEnvelope(t, pool, revisionID),
	})
	if duplicate.Outcome != OutcomeDuplicate {
		t.Fatalf("duplicate revision outcome = %s, want duplicate", duplicate.Outcome)
	}
	if result := deliver(createdID); result.Outcome != OutcomeDuplicate {
		t.Fatalf("late original outcome = %s, want duplicate (inbox dedup, no overwrite)", result.Outcome)
	}
	if result := deliver(orphanID); result.Outcome != OutcomeDuplicate {
		t.Fatalf("late orphan outcome = %s, want duplicate (inbox dedup, no overwrite)", result.Outcome)
	}
	state = readRevisionState(t, pool, t057ConsumerName, obsID)
	if state.State != "orphaned" || state.Canonical || state.Version != 3 || state.Objects != 1 {
		t.Fatalf("state after duplicates/out-of-order = %+v, want orphaned/v3 and one object row", state)
	}

	// FR-08 revival: the reinstated fact revives the SAME object and the
	// revision of the Orphaned disposition converges to pending.
	if result := deliver(reinstatedID); result.Outcome != OutcomeApplied {
		t.Fatalf("reinstated outcome = %s, want applied", result.Outcome)
	}
	if result := deliver(revivalRevisionID); result.Outcome != OutcomeApplied {
		t.Fatalf("revival revision outcome = %s, want applied", result.Outcome)
	}
	state = readRevisionState(t, pool, t057ConsumerName, obsID)
	if state.State != "pending" || !state.Canonical || state.Version != 5 {
		t.Fatalf("state after revival = %+v, want pending/valid canonical at v5", state)
	}
	if state.Objects != 1 {
		t.Fatalf("tracked objects = %d, want 1 (the revival reuses the original observation)", state.Objects)
	}

	// No revision delivery reaches 007/008/009/010/011: zero new intents,
	// nonces, signatures or broadcasts.
	paymentsAfter := countUpstreamRows(t, pool)
	for _, table := range upstreamCounterTables {
		if paymentsBefore[table] != paymentsAfter[table] {
			t.Fatalf("%s rows changed on revision delivery: %d -> %d", table, paymentsBefore[table], paymentsAfter[table])
		}
	}
	t.Logf("T057 revision matrix PASS: obs=%s orphaned(non-canonical) -> revision v3 -> reinstated -> revision v5 pending; 0 new payment rows", obsID)
}

// TestT057RevisionBeforeOriginalAndMalformedIsolation covers the ordering edge
// (the revision arrives before its original) and the fail-closed revision
// field validation: the revision is applied as the object's baseline without
// being misused as a new observation, later originals skip, and malformed or
// chain-mismatched revision envelopes are quarantined without blocking the
// partition.
func TestT057RevisionBeforeOriginalAndMalformedIsolation(t *testing.T) {
	dsn := startMigratedPostgres(t)
	pool := openEventsPool(t, dsn)
	createRevisionStateTable(t, pool)

	const obsID = "obs-t057-b"
	blockHash := "0x" + strings.Repeat("c4", 32)
	txHash := "0x" + strings.Repeat("d5", 32)
	createdID, _ := appendConsumerEvent(t, pool, t057CreatedEvent(t, obsID, blockHash, txHash))
	orphanID, _ := appendConsumerEvent(t, pool, t057StatusChangedEvent(t, obsID, "pending", "orphaned", "reorg_invalidated"))
	revisionID, _ := appendConsumerEvent(t, pool, t057RevisionAppliedEvent(t, obsID, createdID, "orphaned", "reorg_invalidated"))

	consumer := newTestConsumer(t, pool, "t057-revision-first",
		revisionStateEffect{consumerName: "t057-revision-first"}, nil, 200*time.Millisecond)
	// The revision arrives first: it becomes the baseline and is applied as
	// the revised fact (orphaned), never as a fresh pending observation.
	if result := mustProcess(t, consumer, Message{
		Topic: "txharbor.events.v1", Partition: 0, Offset: 0, Value: wireEnvelope(t, pool, revisionID),
	}); result.Outcome != OutcomeApplied {
		t.Fatalf("revision-first outcome = %s, want applied", result.Outcome)
	}
	for i, eventID := range []uuid.UUID{createdID, orphanID} {
		result := mustProcess(t, consumer, Message{
			Topic: "txharbor.events.v1", Partition: 0, Offset: int64(i + 1), Value: wireEnvelope(t, pool, eventID),
		})
		if result.Outcome != OutcomeVersionSkip {
			t.Fatalf("late original[%d] outcome = %s, want version_skip (never overwrite)", i, result.Outcome)
		}
	}
	state := readRevisionState(t, pool, "t057-revision-first", "obs-t057-b")
	if state.State != "orphaned" || state.Canonical || state.Version != 3 {
		t.Fatalf("revision-first state = %+v, want orphaned/non-canonical at v3", state)
	}

	// Malformed revision envelopes fail closed into the persistent quarantine
	// with the right failure class, and the partition keeps advancing.
	const obsMalformed = "obs-t057-c"
	malformedID, _ := appendConsumerEvent(t, pool, t057RevisionAppliedEvent(t, obsMalformed, createdID, "orphaned", "reorg_invalidated"))
	validWire := wireEnvelope(t, pool, malformedID)
	if _, err := ParseEnvelope(validWire); err != nil {
		t.Fatalf("valid revision wire refused: %v", err)
	}

	cases := []struct {
		name   string
		mutate func(map[string]any)
		reason QuarantineReason
	}{
		{"missing revises_event_id", func(w map[string]any) { delete(w, "revises_event_id") }, QuarantineNonRetryable},
		{"missing recovery_version", func(w map[string]any) { delete(w, "recovery_version") }, QuarantineNonRetryable},
		{"chain mismatch", func(w map[string]any) { w["chain_id"] = 2 }, QuarantineIdentityMismatch},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wire := mutateRevisionWire(t, validWire, tc.mutate)
			result := mustProcess(t, consumer, Message{
				Topic: "txharbor.events.v1", Partition: 0, Offset: int64(100 + i), Value: wire,
			})
			if result.Outcome != OutcomeQuarantined || result.Reason != tc.reason {
				t.Fatalf("outcome = %+v, want quarantined/%s", result, tc.reason)
			}
		})
	}
	// None of the malformed deliveries applied an effect; the partition
	// progress advanced past the last quarantined offset.
	var tracked int
	if err := pool.QueryRow(context.Background(), fmt.Sprintf(
		`SELECT count(*) FROM %s WHERE aggregate_id = $1`, revisionStateTable), obsMalformed).Scan(&tracked); err != nil {
		t.Fatalf("count malformed state rows: %v", err)
	}
	if tracked != 0 {
		t.Fatalf("malformed revision produced %d state effects, want 0", tracked)
	}
	if next, found := readProgressRow(t, pool, "t057-revision-first", "txharbor.events.v1", 0); !found || next != 103 {
		t.Fatalf("progress = (%d, found=%v), want (103, true): the partition continues past quarantined events", next, found)
	}
	t.Logf("T057 revision matrix PASS: revision-first baseline + quarantine classes (non_retryable/identity_mismatch), partition continued")
}
