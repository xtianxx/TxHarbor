//go:build integration

// revision_events_integration_test.go is the B6/T055 V-REVISION producer layer
// on real PostgreSQL: a shallow reorg affecting Pending and Confirmed deposits
// emits, in the SAME transaction as the 006 conversion, the generic status
// change AND deposit.revision.applied carrying the superseded pre-reorg event,
// the old block identity, the Orphaned disposition and the 006 recovery
// version; the FR-08 same-block_hash revival emits the dedicated
// deposit.observation.reinstated fact (the ORIGINAL observation reused, never
// a second created) plus the revision of the Orphaned disposition. Repeat
// execution converges with zero new events, and a stale-capture refusal writes
// neither the conversion nor any event. It reuses the package integration
// fixtures (startIndexerPostgres, the reorg/deposit/confirmation seeds,
// outboxRowsForAggregate) against the real 000015 schema.
//
// Chain IDs 906019 and 906020 extend the 906001-906018 reservation block
// documented in reorgcommit_test.go.
package indexer

import (
	"context"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/events"
)

// t055SeedChain plants the shared mini base: canonical 10..20, the seq-1
// history row (observation FK), the deposit checkpoint and zero observations.
func t055SeedChain(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64) {
	t.Helper()
	cfgHash := strings.Repeat("aa", 32)
	reorgSeedChain(t, ctx, pool, chainID, 10, 20, 10)
	depositSeedHistory(t, ctx, pool, chainID, 1, 10, cfgHash)
	if _, err := pool.Exec(ctx, `
INSERT INTO deposit_checkpoint (chain_id, start_block, config_hash, next_block)
VALUES ($1, 10, $2, 20)`, chainID, cfgHash); err != nil {
		t.Fatalf("seed deposit_checkpoint: %v", err)
	}
}

// t055AssertRevision asserts the shared revision contract of one
// deposit.revision.applied row: superseded identity (event + old block),
// disposition, reason, recovery version and revises_event_id.
func t055AssertRevision(t *testing.T, row outboxEventRow, wantToState, wantReason string,
	wantRecoveryVersion int64, wantSupersededEventID string, wantChainID int64, wantBlock uint64, obsBH, obsTx string) {
	t.Helper()
	if row.EventType != events.EventTypeDepositRevisionApplied {
		t.Fatalf("revision event type = %s, want %s", row.EventType, events.EventTypeDepositRevisionApplied)
	}
	if row.SchemaVersion != events.SchemaVersionV1 || row.IdentityKind != string(events.IdentityKindBusinessObject) {
		t.Fatalf("revision envelope = schema %d/%s, want v1/business_object", row.SchemaVersion, row.IdentityKind)
	}
	if row.Payload["to_state"] != wantToState || row.Payload["reason"] != wantReason {
		t.Fatalf("revision payload = %v, want to_state %s reason %s", row.Payload, wantToState, wantReason)
	}
	if row.RevisesEventID == nil || *row.RevisesEventID != wantSupersededEventID {
		t.Fatalf("revision revises_event_id = %v, want %s", row.RevisesEventID, wantSupersededEventID)
	}
	if row.RecoveryVersion == nil || *row.RecoveryVersion != wantRecoveryVersion {
		t.Fatalf("revision recovery_version = %v, want %d", row.RecoveryVersion, wantRecoveryVersion)
	}
	if row.ChainID != wantChainID || row.BlockNumber != int64(wantBlock) || row.BlockHash != obsBH {
		t.Fatalf("revision chain identity = chain %d block %d %s, want chain %d block %d %s",
			row.ChainID, row.BlockNumber, row.BlockHash, wantChainID, wantBlock, obsBH)
	}
	superseded, ok := row.Payload["superseded_identity"].(map[string]any)
	if !ok {
		t.Fatalf("revision superseded_identity = %v, want an object", row.Payload["superseded_identity"])
	}
	if superseded["event_id"] != wantSupersededEventID {
		t.Fatalf("superseded_identity.event_id = %v, want %s", superseded["event_id"], wantSupersededEventID)
	}
	if superseded["block_hash"] != obsBH || superseded["block_number"] != float64(wantBlock) || superseded["tx_hash"] != obsTx {
		t.Fatalf("superseded_identity block identity = %v, want %s/%d/%s", superseded, obsBH, wantBlock, obsTx)
	}
	if row.Payload["chain_id"] != float64(wantChainID) {
		t.Fatalf("revision payload chain_id = %v, want %d", row.Payload["chain_id"], wantChainID)
	}
	outboxWantForbiddenFree(t, row.EventID, row.Payload)
}

// TestT055RevisionAppliedOnOrphanAndReinstatedRevival drives the full
// Pending-deposit cycle: shallow reorg -> orphan conversion + revision, stale
// capture refusal (zero writes), FR-08 same-block_hash revival -> reinstated +
// revision, repeat convergence, version continuity.
func TestT055RevisionAppliedOnOrphanAndReinstatedRevival(t *testing.T) {
	pool := reorgTestPool(t)
	defer pool.Close()
	ctx := context.Background()
	const chainID = int64(906019)
	lease := depositITLease(t, pool, chainID)
	t055SeedChain(t, ctx, pool, chainID)

	obsBH, obsTx := depositBlockHash(17), depositTxHash(17, 0)
	depositSeedObservation(t, ctx, pool, chainID, 17, obsBH, obsTx, 0, "50", 1)
	// The revival re-verifies the source log binding (006 six rules), so the
	// 004 log row must exist for the FR-08 arm.
	depositSeedTransferRow(t, ctx, pool, chainID, 17, obsBH, obsTx, 0,
		common.HexToAddress(testContractA), common.HexToAddress(testContractB), common.HexToAddress(depositWatchAddr), big.NewInt(50))
	outboxSeedCreatedEvent(t, ctx, pool, chainID, 17, obsBH, obsTx, 0)

	res := reorgEstablishOne(t, ctx, pool, lease, chainID, 20)
	owned := RecoveryCapture{Seq: res.Seq, Owned: &RecoveryOwned{RecoveryID: res.RecoveryID}}
	if err := ConfirmRecoveryAncestor(ctx, pool, lease, chainID, owned, 15, depositBlockHash(15), "t055"); err != nil {
		t.Fatalf("confirm ancestor: %v", err)
	}
	if _, _, _, err := InvalidateRecoveryBlocks(ctx, pool, lease, chainID, owned); err != nil {
		t.Fatalf("invalidate blocks: %v", err)
	}
	orphaned, err := InvalidateRecoveryObservations(ctx, pool, lease, chainID, owned)
	if err != nil || orphaned != 1 {
		t.Fatalf("invalidate observations = %d (err %v), want 1", orphaned, err)
	}

	aggregateID := depositObservationID(chainID, obsBH, obsTx, 0)
	rows := outboxRowsForAggregate(t, ctx, pool, aggregateID)
	if len(rows) != 3 {
		t.Fatalf("outbox rows after orphan = %d, want 3 (created + status_changed + revision.applied)", len(rows))
	}
	if rows[1].EventType != events.EventTypeDepositObservationStatusChanged ||
		rows[1].Payload["from_state"] != depositObservationStatusPending ||
		rows[1].Payload["to_state"] != observationStateOrphaned {
		t.Fatalf("orphan status_changed = %+v, want pending->orphaned", rows[1])
	}
	t055AssertRevision(t, rows[2], observationStateOrphaned, observationReasonReorgInvalidated,
		res.Seq, rows[0].EventID, chainID, 17, obsBH, obsTx)

	// The conversion and its events are one fate: a stale capture refuses the
	// whole transaction with zero new writes and zero new events.
	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM deposit_observations
WHERE chain_id = $1 AND block_hash = $2 AND tx_hash = $3 AND log_index = 0`, chainID, obsBH, obsTx).Scan(&status); err != nil {
		t.Fatalf("read observation status: %v", err)
	}
	if status != observationStateOrphaned {
		t.Fatalf("observation status = %s, want orphaned", status)
	}
	stale := RecoveryCapture{Seq: res.Seq + 1, Owned: &RecoveryOwned{RecoveryID: res.RecoveryID}}
	if n, err := InvalidateRecoveryObservations(ctx, pool, lease, chainID, stale); err == nil || n != 0 {
		t.Fatalf("stale-capture invalidate = %d (err %v), want a gate refusal with zero writes", n, err)
	}
	if n := outboxCountForAggregate(t, ctx, pool, aggregateID); n != 3 {
		t.Fatalf("outbox rows after refused stale invalidate = %d, want 3", n)
	}
	// Repeat execution of the same conversion converges: zero conversions,
	// zero new events, zero new versions.
	if repeat, err := InvalidateRecoveryObservations(ctx, pool, lease, chainID, owned); err != nil || repeat != 0 {
		t.Fatalf("repeat invalidate = %d (err %v), want 0", repeat, err)
	}
	if n := outboxCountForAggregate(t, ctx, pool, aggregateID); n != 3 {
		t.Fatalf("outbox rows after repeat invalidate = %d, want 3", n)
	}

	// FR-08: the same old block_hash becomes canonical again; the original
	// observation is revived in place (never a second observation/created).
	if err := RecanonicalizeRecoveryBlock(ctx, pool, lease, chainID, owned, 17, obsBH); err != nil {
		t.Fatalf("recanonicalize 17: %v", err)
	}
	converted, err := ReviveRecoveryObservation(ctx, pool, lease, chainID, owned, obsBH, obsTx, 0, "t055 revive")
	if err != nil || !converted {
		t.Fatalf("revive = %v (err %v), want converted", converted, err)
	}
	if err := pool.QueryRow(ctx, `SELECT status FROM deposit_observations
WHERE chain_id = $1 AND block_hash = $2 AND tx_hash = $3 AND log_index = 0`, chainID, obsBH, obsTx).Scan(&status); err != nil {
		t.Fatalf("re-read observation status: %v", err)
	}
	if status != depositObservationStatusPending {
		t.Fatalf("observation status after revival = %s, want pending", status)
	}
	if n := reorgCount(t, ctx, pool, "deposit_observations", chainID); n != 1 {
		t.Fatalf("observation rows = %d, want 1 (the original row is reused, not duplicated)", n)
	}

	rows = outboxRowsForAggregate(t, ctx, pool, aggregateID)
	if len(rows) != 5 {
		t.Fatalf("outbox rows after revival = %d, want 5 (created + orphan + revision + reinstated + revision)", len(rows))
	}
	reinstated := rows[3]
	if reinstated.EventType != events.EventTypeDepositObservationReinstated || reinstated.AggregateVersion != 4 {
		t.Fatalf("revival event = %+v, want deposit.observation.reinstated v4", reinstated)
	}
	if reinstated.EventType == events.EventTypeDepositObservationCreated ||
		reinstated.IdentityKind != string(events.IdentityKindBusinessObject) {
		t.Fatalf("revival was conflated with a new source observation: %+v", reinstated)
	}
	if reinstated.Payload["observation_id"] != aggregateID ||
		reinstated.Payload["reason"] != observationReasonReorgRevived ||
		reinstated.Payload["revive_basis"] != observationReviveBasisRecanonicalized {
		t.Fatalf("reinstated payload = %v, want the original observation %s revived", reinstated.Payload, aggregateID)
	}
	if reinstated.Payload["block_hash"] != obsBH || reinstated.ChainID != chainID {
		t.Fatalf("reinstated chain identity = %+v, want chain %d block %s", reinstated, chainID, obsBH)
	}
	outboxWantForbiddenFree(t, reinstated.EventID, reinstated.Payload)

	t055AssertRevision(t, rows[4], depositObservationStatusPending, observationReasonReorgRevived,
		res.Seq, rows[2].EventID, chainID, 17, obsBH, obsTx)

	// Repeat execution converges with zero new events and zero new versions.
	if repeat, err := ReviveRecoveryObservation(ctx, pool, lease, chainID, owned, obsBH, obsTx, 0, "t055 revive repeat"); err != nil || repeat {
		t.Fatalf("repeat revive = %v (err %v), want converged false", repeat, err)
	}
	rows = outboxRowsForAggregate(t, ctx, pool, aggregateID)
	if len(rows) != 5 {
		t.Fatalf("outbox rows after repeats = %d, want 5", len(rows))
	}
	for i, row := range rows {
		if row.AggregateVersion != int64(i+1) {
			t.Fatalf("aggregate version sequence = %v, want 1..5 (no duplicate conversion, no wrong version)", row.AggregateVersion)
		}
	}
	t.Logf("T055 revision matrix PASS pending-cycle: events=%d orphan_revision=orphaned revive_revision=pending reinstated=1",
		len(rows))
}

// TestT055ConfirmedObservationOrphanRevision is the Confirmed-deposit arm: the
// revocation of a confirmed observation by the shallow reorg supersedes the
// confirmation event itself (revises_event_id points at
// deposit.confirmation.confirmed) with the Orphaned disposition and the 006
// recovery version.
func TestT055ConfirmedObservationOrphanRevision(t *testing.T) {
	pool := reorgTestPool(t)
	defer pool.Close()
	ctx := context.Background()
	const chainID = int64(906020)
	const obsHeight, n = uint64(18), uint64(3)
	t055SeedChain(t, ctx, pool, chainID)

	obsBH, obsTx := depositBlockHash(obsHeight), depositTxHash(obsHeight, 0)
	depositSeedObservation(t, ctx, pool, chainID, obsHeight, obsBH, obsTx, 0, "1", 1)
	outboxSeedCreatedEvent(t, ctx, pool, chainID, obsHeight, obsBH, obsTx, 0)

	// Confirm through the real 005 commit path so the pre-reorg fact stream is
	// created -> confirmed. The committer's lease is the one shared by the
	// whole test (a second Acquire on the same chain would fail by design).
	confirmSeedPolicyRow(t, ctx, pool, chainID, 1, int64(n), nil, "bootstrap", nil)
	c, lease := confirmCommitter(t, pool, chainID, n)
	rcap := testRecoveryCap(t, ctx, pool, chainID)
	basis := ConfirmBasis{BlockHash: obsBH, TxHash: obsTx, Height: obsHeight,
		TipNumber: 20, TipHash: depositBlockHash(20), PolicySeq: 1, ThresholdN: n}
	if err := c.ConfirmDepositUnit(ctx, lease, basis, rcap); err != nil {
		t.Fatalf("ConfirmDepositUnit(): %v", err)
	}

	aggregateID := depositObservationID(chainID, obsBH, obsTx, 0)
	rows := outboxRowsForAggregate(t, ctx, pool, aggregateID)
	if len(rows) != 2 || rows[1].EventType != events.EventTypeDepositConfirmationConfirmed {
		t.Fatalf("pre-reorg events = %+v, want created + confirmed", rows)
	}
	confirmedEventID := rows[1].EventID
	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM deposit_observations
WHERE chain_id = $1 AND block_hash = $2 AND tx_hash = $3 AND log_index = 0`, chainID, obsBH, obsTx).Scan(&status); err != nil {
		t.Fatalf("read confirmed status: %v", err)
	}
	if status != observationStateConfirmed {
		t.Fatalf("observation status = %s, want confirmed", status)
	}

	res := reorgEstablishOne(t, ctx, pool, lease, chainID, 20)
	owned := RecoveryCapture{Seq: res.Seq, Owned: &RecoveryOwned{RecoveryID: res.RecoveryID}}
	if err := ConfirmRecoveryAncestor(ctx, pool, lease, chainID, owned, 15, depositBlockHash(15), "t055 confirmed"); err != nil {
		t.Fatalf("confirm ancestor: %v", err)
	}
	if _, _, _, err := InvalidateRecoveryBlocks(ctx, pool, lease, chainID, owned); err != nil {
		t.Fatalf("invalidate blocks: %v", err)
	}
	orphaned, err := InvalidateRecoveryObservations(ctx, pool, lease, chainID, owned)
	if err != nil || orphaned != 1 {
		t.Fatalf("invalidate observations = %d (err %v), want 1", orphaned, err)
	}

	rows = outboxRowsForAggregate(t, ctx, pool, aggregateID)
	if len(rows) != 4 {
		t.Fatalf("outbox rows after confirmed orphan = %d, want 4 (created + confirmed + status_changed + revision)", len(rows))
	}
	if rows[2].EventType != events.EventTypeDepositObservationStatusChanged ||
		rows[2].Payload["from_state"] != observationStateConfirmed ||
		rows[2].Payload["to_state"] != observationStateOrphaned {
		t.Fatalf("confirmed orphan status_changed = %+v, want confirmed->orphaned", rows[2])
	}
	// The revision supersedes the confirmation fact itself.
	t055AssertRevision(t, rows[3], observationStateOrphaned, observationReasonReorgInvalidated,
		res.Seq, confirmedEventID, chainID, obsHeight, obsBH, obsTx)
	if err := pool.QueryRow(ctx, `SELECT status FROM deposit_observations
WHERE chain_id = $1 AND block_hash = $2 AND tx_hash = $3 AND log_index = 0`, chainID, obsBH, obsTx).Scan(&status); err != nil {
		t.Fatalf("re-read status: %v", err)
	}
	if status != observationStateOrphaned {
		t.Fatalf("observation status after confirmed orphan = %s, want orphaned", status)
	}
	t.Logf("T055 revision matrix PASS confirmed-arm: revision supersedes confirmation event %s (orphaned)", confirmedEventID)
}
