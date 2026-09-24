package events

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// AppendResult is the outcome of one Append.
type AppendResult struct {
	// EventID is the derived deterministic identity of the event.
	EventID uuid.UUID
	// OutboxID is the outbox_events.id of the written (or existing) row.
	OutboxID int64
	// AggregateVersion is the emission-stream version of the row: the derived
	// coalesce(max,0)+1 on insert, or the existing row's version on a no-op.
	AggregateVersion int64
	// Noop reports that the row already existed with the same identity and
	// the same payload_hash: nothing was written, no conflict was raised and
	// no alert fired (FR-09; data-model §10).
	Noop bool
}

// IdentityConflictObserver observes same-identity/different-content refusals
// (FR-09/SC-03). *metrics.Metrics satisfies it
// (ObserveEventsIdentityConflict). A nil observer disables observation
// (unit/integration tests).
type IdentityConflictObserver interface {
	ObserveEventsIdentityConflict(eventType string)
}

type identityConflictObserverHolder struct{ observer IdentityConflictObserver }

var identityConflictObserver atomic.Pointer[identityConflictObserverHolder]

// SetIdentityConflictObserver installs the metrics sink for identity-conflict
// alerts. Call during process wiring, before Append runs concurrently; nil
// disables observation.
func SetIdentityConflictObserver(obs IdentityConflictObserver) {
	if obs == nil {
		identityConflictObserver.Store(nil)
		return
	}
	identityConflictObserver.Store(&identityConflictObserverHolder{observer: obs})
}

func observeIdentityConflict(eventType string) {
	if holder := identityConflictObserver.Load(); holder != nil {
		holder.observer.ObserveEventsIdentityConflict(eventType)
	}
}

// aggregateVersionQuery computes the emission-stream high-water mark for one
// business object inside the caller's transaction. The caller holds the source
// row lock; the UNIQUE index below is the concurrency backstop (data-model
// §3.2/§3.3).
const aggregateVersionQuery = `
SELECT coalesce(max(aggregate_version), 0)
FROM outbox_events
WHERE aggregate_type = $1 AND aggregate_id = $2`

// appendLogSQL inserts an evm_log event. The conflict target is the partial
// log-identity index: same log triple + same payload_hash is an idempotent
// no-op (the row is returned, nothing changes); same triple + different
// payload_hash updates 0 rows and the caller refuses the transaction
// (data-model §2 Table 1 write protocol; research R2).
const appendLogSQL = `
INSERT INTO outbox_events (
	event_id, identity_kind, event_type, schema_version,
	aggregate_type, aggregate_id, aggregate_version,
	payload, payload_hash, occurred_at,
	chain_id, block_number, block_hash, tx_hash, log_index,
	recovery_version, revises_event_id,
	source_kind, source_id, source_version
) VALUES (
	$1, 'evm_log', $2, $3,
	$4, $5, $6,
	$7::jsonb, $8, $9,
	$10, $11, $12, $13, $14,
	$15, $16,
	$17, $18, $19
)
ON CONFLICT (chain_id, block_hash, tx_hash, log_index) WHERE identity_kind = 'evm_log'
DO UPDATE SET attempt_count = outbox_events.attempt_count
WHERE outbox_events.payload_hash = EXCLUDED.payload_hash
RETURNING id, aggregate_version, (xmax = 0) AS inserted`

// appendObjectSQL inserts a business_object event; the conflict target is the
// partial object-identity index (aggregate_type, aggregate_id,
// aggregate_version). The version is derived in-transaction, so a concurrent
// emission of the same transition collides here and is either an idempotent
// no-op or an identity conflict (never a silent overwrite).
const appendObjectSQL = `
INSERT INTO outbox_events (
	event_id, identity_kind, event_type, schema_version,
	aggregate_type, aggregate_id, aggregate_version,
	payload, payload_hash, occurred_at,
	chain_id, block_number, block_hash, tx_hash, log_index,
	recovery_version, revises_event_id,
	source_kind, source_id, source_version
) VALUES (
	$1, 'business_object', $2, $3,
	$4, $5, $6,
	$7::jsonb, $8, $9,
	$10, $11, $12, $13, $14,
	$15, $16,
	$17, $18, $19
)
ON CONFLICT (aggregate_type, aggregate_id, aggregate_version) WHERE identity_kind = 'business_object'
DO UPDATE SET attempt_count = outbox_events.attempt_count
WHERE outbox_events.payload_hash = EXCLUDED.payload_hash
RETURNING id, aggregate_version, (xmax = 0) AS inserted`

// Append writes ev inside the caller's already-open PostgreSQL transaction
// (T1: business transition + outbox row commit or roll back together; FR-07;
// data-model §5). It never opens its own transaction and never performs a
// network call, so there is no "write the event after commit" shape.
//
// The aggregate_version is derived in-transaction as coalesce(max,0)+1; the
// partial UNIQUE index is the concurrency backstop and conflicts are
// classified, never retried blindly (data-model §3.3/§10).
//
// Producer contract: one Append per business transition. Callers must not
// re-Append for an idempotent replay of an already-committed transition.
func Append(ctx context.Context, tx pgx.Tx, ev Event) (AppendResult, error) {
	if tx == nil {
		return AppendResult{}, contractErrorf("Append requires an in-progress PostgreSQL transaction")
	}
	prepared, err := ev.prepare()
	if err != nil {
		return AppendResult{}, err
	}

	var maxVersion int64
	if err := tx.QueryRow(ctx, aggregateVersionQuery, prepared.AggregateType, prepared.AggregateID).Scan(&maxVersion); err != nil {
		return AppendResult{}, ClassifyPGError(err)
	}
	if maxVersion < 0 {
		return AppendResult{}, contractErrorf("negative aggregate_version high-water mark %d", maxVersion)
	}
	version := maxVersion + 1
	prepared.AggregateVersion = version

	eventID := NewEventID(naturalKeyFor(prepared, version))
	insertSQL := appendObjectSQL
	if prepared.IdentityKind == IdentityKindEVMLog {
		insertSQL = appendLogSQL
	}

	var outboxID, storedVersion int64
	var inserted bool
	if err := tx.QueryRow(ctx, insertSQL, appendArgs(prepared, eventID)...).Scan(&outboxID, &storedVersion, &inserted); err != nil {
		classified := ClassifyPGError(err)
		if errors.Is(err, pgx.ErrNoRows) || errors.Is(classified, ErrIdentityConflict) {
			observeIdentityConflict(prepared.EventType)
			return AppendResult{}, fmt.Errorf("%w: %s: %v", ErrIdentityConflict, prepared.EventType, err)
		}
		return AppendResult{}, classified
	}
	return AppendResult{
		EventID:          eventID,
		OutboxID:         outboxID,
		AggregateVersion: storedVersion,
		Noop:             !inserted,
	}, nil
}

// naturalKeyFor builds the documented natural key of an event (data-model §3).
func naturalKeyFor(ev Event, version int64) string {
	if ev.IdentityKind == IdentityKindEVMLog {
		return EVMLogNaturalKey(ev.ChainID, ev.BlockHash, ev.TxHash, ev.LogIndex)
	}
	return BusinessObjectNaturalKey(ev.AggregateType, ev.AggregateID, version)
}

// appendArgs maps an event to the shared 19-parameter insert shape. UUIDs are
// passed as canonical strings and the payload as the frozen canonical JSON
// text cast to jsonb. Optional columns are NULL when unset; tx_hash/log_index
// are only meaningful for evm_log events (contracts/events.md §1).
func appendArgs(ev Event, eventID uuid.UUID) []any {
	var txHash, logIndex any
	if ev.IdentityKind == IdentityKindEVMLog {
		txHash = ev.TxHash
		logIndex = ev.LogIndex
	}
	return []any{
		eventID.String(),
		ev.EventType,
		ev.SchemaVersion,
		ev.AggregateType,
		ev.AggregateID,
		ev.AggregateVersion,
		string(ev.payloadJSON),
		ev.payloadHash,
		ev.OccurredAt,
		nullableInt64(ev.ChainID),
		nullableInt64(ev.BlockNumber),
		nullableString(ev.BlockHash),
		txHash,
		logIndex,
		nullableInt64(ev.RecoveryVersion),
		nullableUUID(ev.RevisesEventID),
		nullableString(ev.SourceKind),
		nullableString(ev.SourceID),
		nullableInt64(ev.SourceVersion),
	}
}

func nullableInt64(v int64) any {
	if v == 0 {
		return nil
	}
	return v
}

func nullableString(v string) any {
	if v == "" {
		return nil
	}
	return v
}

func nullableUUID(v uuid.UUID) any {
	if v == uuid.Nil {
		return nil
	}
	return v.String()
}
