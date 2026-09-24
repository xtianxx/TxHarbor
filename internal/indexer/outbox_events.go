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
	"fmt"
	"time"

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
	// observationReasonReorgRevived names the 006 FR-08 in-place revival in
	// the status_changed fact; the dedicated `reinstated` semantics are US4
	// (T052) and are not emitted by this batch.
	observationReasonReorgRevived = "reorg_revived"

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
