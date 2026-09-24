// outbox_events.go owns the 013 producer helpers for the indexer domain
// (T026/T027/T028; data-model.md §4 integration points). Every helper builds
// one catalog v1 event and calls events.Append inside the caller's already-open
// business transaction, so the business transition and its event commit or
// roll back together (FR-07; T1).
//
// The dependency arrow is indexer -> events only: internal/events never
// imports this package (T016 import boundary). Payloads stay minimal
// (data-model §9): identity, chain identity, states and reasons; never keys,
// credentials or raw signature bytes.
package indexer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/xtianxx/txharbor/internal/events"
)

// Observation aggregate/state vocabulary emitted to the catalog (contracts/
// events.md §3). The DB status strings are the source facts; these constants
// name them for the payload only and never reinterpret 004/005/006 semantics.
const (
	depositObservationAggregateType = "deposit_observation"

	// The pending state reuses the 004 write-path constant
	// (depositObservationStatusPending) so the confinement gate keeps exactly
	// one "pending" literal site in the package. The other states are
	// read-only names of values the 005/006 schemas already CHECK.
	observationStateConfirmed = "confirmed"
	observationStateOrphaned  = "orphaned"

	// observationReasonReorgInvalidated mirrors 006's orphan_reason persisted
	// on the source row (deposit_observations.orphan_reason).
	observationReasonReorgInvalidated = "reorg_invalidated"
	// observationReasonReorgRevived names the 006 FR-08 in-place revival
	// semantics (orphaned -> pending when the same old block hash becomes
	// canonical again) in the reinstated and revision facts (T052).
	observationReasonReorgRevived = "reorg_revived"

	// observationReviveBasisRecanonicalized is the rebirth basis carried by
	// deposit.observation.reinstated: the same old block_hash was restored to
	// canonical (006 FR-08), never a new source observation.
	observationReviveBasisRecanonicalized = "same_block_hash_recanonicalized"

	// observationSourceKind tags every indexer-domain outbox row for the
	// reconciliation audit (data-model §4).
	observationSourceKind = "deposit_observer"
)

// depositObservationID is the stable business identity of one 004 observation
// (the aggregate_id of its emitted events). The (chain_id, block_hash,
// tx_hash, log_index) quadruple is the observation PK and never depends on
// delivery attempts, timestamps or producer instances (contracts/events.md §2;
// height alone is never an identity component).
func depositObservationID(chainID int64, blockHash, txHash string, logIndex uint64) string {
	return fmt.Sprintf("%d/%s/%s/%d", chainID, blockHash, txHash, logIndex)
}

// appendDepositObservationCreatedEvent emits deposit.observation.created for
// one newly inserted observation (T026). The identity kind is evm_log: the
// natural key is the source log triple and the aggregate carries the
// observation identity with emission-stream version 1 (contracts/events.md §3).
// Callers only append when the observation row was actually inserted; an
// idempotent replay of an already-committed transition appends nothing.
func appendDepositObservationCreatedEvent(
	ctx context.Context,
	tx pgx.Tx,
	chainID int64,
	blockNumber uint64,
	blockHash, txHash string,
	logIndex uint64,
) error {
	observationID := depositObservationID(chainID, blockHash, txHash, logIndex)
	ev := events.Event{
		EventType:     events.EventTypeDepositObservationCreated,
		SchemaVersion: events.SchemaVersionV1,
		IdentityKind:  events.IdentityKindEVMLog,
		AggregateType: depositObservationAggregateType,
		AggregateID:   observationID,
		Payload: map[string]any{
			"observation_id": observationID,
			"state":          depositObservationStatusPending,
			"chain_id":       chainID,
			"block_number":   int64(blockNumber),
			"block_hash":     blockHash,
			"tx_hash":        txHash,
			"log_index":      int64(logIndex),
		},
		OccurredAt:  time.Now().UTC(),
		ChainID:     chainID,
		BlockNumber: int64(blockNumber),
		BlockHash:   blockHash,
		TxHash:      txHash,
		LogIndex:    int(logIndex),
		SourceKind:  observationSourceKind,
		SourceID:    observationID,
	}
	if _, err := events.Append(ctx, tx, ev); err != nil {
		return fmt.Errorf("append deposit observation created event %s: %w", observationID, err)
	}
	return nil
}

// appendDepositObservationStatusChangedEvent emits
// deposit.observation.status_changed for one committed observation status
// conversion (T028). fromState is the true pre-conversion status
// (pending/confirmed) or orphaned for a revival; toState is the post status.
// The event expresses a fact, never a permission (FR-03/FR-05).
func appendDepositObservationStatusChangedEvent(
	ctx context.Context,
	tx pgx.Tx,
	chainID int64,
	blockNumber uint64,
	blockHash, txHash string,
	logIndex uint64,
	fromState, toState, reason string,
) error {
	observationID := depositObservationID(chainID, blockHash, txHash, logIndex)
	ev := events.Event{
		EventType:     events.EventTypeDepositObservationStatusChanged,
		SchemaVersion: events.SchemaVersionV1,
		IdentityKind:  events.IdentityKindBusinessObject,
		AggregateType: depositObservationAggregateType,
		AggregateID:   observationID,
		Payload: map[string]any{
			"from_state":     fromState,
			"to_state":       toState,
			"reason":         reason,
			"observation_id": observationID,
			"chain_id":       chainID,
			"block_number":   int64(blockNumber),
			"block_hash":     blockHash,
			"tx_hash":        txHash,
			"log_index":      int64(logIndex),
		},
		OccurredAt: time.Now().UTC(),
		SourceKind: observationSourceKind,
		SourceID:   observationID,
	}
	if _, err := events.Append(ctx, tx, ev); err != nil {
		return fmt.Errorf("append deposit observation status_changed event %s: %w", observationID, err)
	}
	return nil
}

// appendDepositConfirmationConfirmedEvent emits deposit.confirmation.confirmed
// for one committed Pending -> Confirmed conversion (T027). confirmedHeight/
// confirmedHash are the confirmed observation's block (h, bh); policyVersion
// is the 005 policy_seq the confirmation was committed under and is carried as
// the upstream version context (research R3). The event means "the project
// confirmation policy was reached", never upstream ledger credit
// (contracts/events.md §3).
func appendDepositConfirmationConfirmedEvent(
	ctx context.Context,
	tx pgx.Tx,
	chainID int64,
	confirmedHeight uint64,
	confirmedHash, txHash string,
	logIndex uint64,
	policyVersion int64,
) error {
	observationID := depositObservationID(chainID, confirmedHash, txHash, logIndex)
	ev := events.Event{
		EventType:     events.EventTypeDepositConfirmationConfirmed,
		SchemaVersion: events.SchemaVersionV1,
		IdentityKind:  events.IdentityKindBusinessObject,
		AggregateType: depositObservationAggregateType,
		AggregateID:   observationID,
		Payload: map[string]any{
			"policy_version":         policyVersion,
			"confirmed_block_number": int64(confirmedHeight),
			"confirmed_block_hash":   confirmedHash,
			"observation_id":         observationID,
			"chain_id":               chainID,
			"block_number":           int64(confirmedHeight),
			"block_hash":             confirmedHash,
			"tx_hash":                txHash,
			"log_index":              int64(logIndex),
		},
		OccurredAt: time.Now().UTC(),
		SourceKind: observationSourceKind,
		SourceID:   observationID,
	}
	if _, err := events.Append(ctx, tx, ev); err != nil {
		return fmt.Errorf("append deposit confirmation confirmed event %s: %w", observationID, err)
	}
	return nil
}

// supersededEventRef identifies the aggregate's most recent committed event
// before a reorg revision: it becomes the revision event's revises_event_id
// and the payload's superseded_identity reference (T052; contracts/events.md
// §5.1; data-model §3.4).
type supersededEventRef struct {
	EventID          uuid.UUID
	EventType        string
	AggregateVersion int64
}

// readDepositObservationHeadEvent reads the observation aggregate's latest
// committed outbox event inside the caller's transaction and BEFORE any event
// of this transaction is appended, so the reference is the pre-reorg fact
// being revised. found=false means the object's fact stream predates the
// cutover (events.md §7: revision semantics apply only to post-cutover
// facts); the caller then emits no revision event rather than fabricating a
// superseded identity. The read is transaction-local and read-only.
func readDepositObservationHeadEvent(ctx context.Context, tx pgx.Tx, observationID string) (supersededEventRef, bool, error) {
	var ref supersededEventRef
	err := tx.QueryRow(ctx, readObservationHeadEventSQL, depositObservationAggregateType, observationID).
		Scan(&ref.EventID, &ref.EventType, &ref.AggregateVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		return supersededEventRef{}, false, nil
	}
	if err != nil {
		return supersededEventRef{}, false, fmt.Errorf("read deposit observation head event: %w", err)
	}
	return ref, true, nil
}

// readObservationHeadEventSQL reads the highest-version committed event of one
// aggregate. identity_kind is not filtered: the first emitted fact of an
// observation is its evm_log created event, and a revision of a pending
// observation supersedes exactly that fact.
const readObservationHeadEventSQL = `
SELECT event_id, event_type, aggregate_version
FROM outbox_events
WHERE aggregate_type = $1 AND aggregate_id = $2
ORDER BY aggregate_version DESC, id DESC
LIMIT 1`

// appendDepositObservationReinstatedEvent emits
// deposit.observation.reinstated for the 006 FR-08 in-place revival (T052):
// the same old block_hash became canonical again and the ORIGINAL observation
// is reused. It is strictly distinguished from deposit.observation.created (a
// new source observation) and never converts a second time; the payload
// carries the original observation id, the revival basis and the chain
// identity (contracts/events.md §3/§5.4). The event is a fact, never a new
// deposit or confirmation.
func appendDepositObservationReinstatedEvent(
	ctx context.Context,
	tx pgx.Tx,
	chainID int64,
	blockNumber uint64,
	blockHash, txHash string,
	logIndex uint64,
) error {
	observationID := depositObservationID(chainID, blockHash, txHash, logIndex)
	ev := events.Event{
		EventType:     events.EventTypeDepositObservationReinstated,
		SchemaVersion: events.SchemaVersionV1,
		IdentityKind:  events.IdentityKindBusinessObject,
		AggregateType: depositObservationAggregateType,
		AggregateID:   observationID,
		Payload: map[string]any{
			"observation_id": observationID,
			"reason":         observationReasonReorgRevived,
			"revive_basis":   observationReviveBasisRecanonicalized,
			"chain_id":       chainID,
			"block_number":   int64(blockNumber),
			"block_hash":     blockHash,
			"tx_hash":        txHash,
			"log_index":      int64(logIndex),
		},
		OccurredAt:  time.Now().UTC(),
		ChainID:     chainID,
		BlockNumber: int64(blockNumber),
		BlockHash:   blockHash,
		TxHash:      txHash,
		LogIndex:    int(logIndex),
		SourceKind:  observationSourceKind,
		SourceID:    observationID,
	}
	if _, err := events.Append(ctx, tx, ev); err != nil {
		return fmt.Errorf("append deposit observation reinstated event %s: %w", observationID, err)
	}
	return nil
}

// appendDepositRevisionAppliedEvent emits deposit.revision.applied for one
// reorg revision of an existing observation fact (T052): superseded is the
// pre-reorg event the revision supersedes (written to revises_event_id and to
// superseded_identity together with the old block identity), toState is the
// new canonical state or the Orphaned disposition, reason names the 006 fact,
// and recoveryVersion is the 006 recovery version that owns the revision
// (contracts/events.md §5.1; data-model §3.4). The event is idempotent by
// construction: it is appended only when the 006 conversion actually commits,
// and repeat execution converts zero rows and appends zero events.
func appendDepositRevisionAppliedEvent(
	ctx context.Context,
	tx pgx.Tx,
	chainID int64,
	blockNumber uint64,
	blockHash, txHash string,
	logIndex uint64,
	recoveryVersion int64,
	superseded supersededEventRef,
	toState, reason string,
) error {
	observationID := depositObservationID(chainID, blockHash, txHash, logIndex)
	chainIdentity := map[string]any{
		"chain_id":     chainID,
		"block_number": int64(blockNumber),
		"block_hash":   blockHash,
		"tx_hash":      txHash,
		"log_index":    int64(logIndex),
	}
	supersededIdentity := map[string]any{
		"event_id":          superseded.EventID.String(),
		"event_type":        superseded.EventType,
		"aggregate_version": superseded.AggregateVersion,
		"chain_id":          chainID,
		"block_number":      int64(blockNumber),
		"block_hash":        blockHash,
		"tx_hash":           txHash,
		"log_index":         int64(logIndex),
	}
	payload := map[string]any{
		"superseded_identity": supersededIdentity,
		"to_state":            toState,
		"reason":              reason,
		"observation_id":      observationID,
	}
	for key, value := range chainIdentity {
		payload[key] = value
	}
	ev := events.Event{
		EventType:       events.EventTypeDepositRevisionApplied,
		SchemaVersion:   events.SchemaVersionV1,
		IdentityKind:    events.IdentityKindBusinessObject,
		AggregateType:   depositObservationAggregateType,
		AggregateID:     observationID,
		Payload:         payload,
		OccurredAt:      time.Now().UTC(),
		ChainID:         chainID,
		BlockNumber:     int64(blockNumber),
		BlockHash:       blockHash,
		TxHash:          txHash,
		LogIndex:        int(logIndex),
		RecoveryVersion: recoveryVersion,
		RevisesEventID:  superseded.EventID,
		SourceKind:      observationSourceKind,
		SourceID:        observationID,
	}
	if _, err := events.Append(ctx, tx, ev); err != nil {
		return fmt.Errorf("append deposit revision applied event %s: %w", observationID, err)
	}
	return nil
}
