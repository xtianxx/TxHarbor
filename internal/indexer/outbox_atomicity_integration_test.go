//go:build integration

// outbox_atomicity_integration_test.go is the B3/T038 atomicity probe for the
// indexer-domain 013 integration points (T026/T027/T028; data-model §4):
//
//   - deposit.observation.created: transaction commit writes the observation
//     and its event together, rollback leaves neither, a replay of the same
//     fact appends no second event, and the identity is the source log triple
//     (height never an identity component);
//   - deposit.confirmation.confirmed: the committed conversion carries the
//     005 policy version and the confirmed block identity, a refused conversion
//     writes no event;
//   - deposit.observation.status_changed: the 006 orphan conversion emits
//     exactly one event with the true from_state and a continuous aggregate
//     version; the in-place revival emits the dedicated
//     deposit.observation.reinstated fact instead of a generic status change
//     (T052 re-point: the revival is NOT a created and NOT a second
//     conversion). Each reorg conversion additionally emits its
//     deposit.revision.applied revision fact (Orphaned disposition / new
//     canonical pending state) in the same transaction, superseding the
//     pre-reorg event. Repeat execution converges with zero new events and
//     zero wrong versions.
//
// It reuses the package integration fixtures (startIndexerPostgres and the
// deposit/confirmation/reorg seed helpers) against the real 000015 schema.
package indexer

import (
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/events"
)

// outboxEventRow is one decoded deposit-domain outbox row.
type outboxEventRow struct {
	EventID          string
	EventType        string
	SchemaVersion    int
	IdentityKind     string
	AggregateType    string
	AggregateID      string
	AggregateVersion int64
	ChainID          int64
	BlockNumber      int64
	BlockHash        string
	TxHash           string
	LogIndex         int
	RecoveryVersion  *int64
	RevisesEventID   *string
	PublishState     string
	Payload          map[string]any
}

// outboxRowsForAggregate reads every outbox row of one aggregate in version
// order and decodes the payload.
func outboxRowsForAggregate(t *testing.T, ctx context.Context, pool *pgxpool.Pool, aggregateID string) []outboxEventRow {
	t.Helper()
	rows, err := pool.Query(ctx, `
SELECT event_id::text, event_type, schema_version, identity_kind, aggregate_type, aggregate_id,
       aggregate_version, COALESCE(chain_id, 0), COALESCE(block_number, 0), COALESCE(block_hash, ''),
       COALESCE(tx_hash, ''), COALESCE(log_index, 0), recovery_version, revises_event_id::text,
       publish_state, payload
FROM outbox_events
WHERE aggregate_id = $1
ORDER BY aggregate_version`, aggregateID)
	if err != nil {
		t.Fatalf("query outbox rows for %s: %v", aggregateID, err)
	}
	defer rows.Close()
	var out []outboxEventRow
	for rows.Next() {
		var (
			row     outboxEventRow
			payload []byte
		)
		if err := rows.Scan(&row.EventID, &row.EventType, &row.SchemaVersion, &row.IdentityKind,
			&row.AggregateType, &row.AggregateID, &row.AggregateVersion, &row.ChainID,
			&row.BlockNumber, &row.BlockHash, &row.TxHash, &row.LogIndex,
			&row.RecoveryVersion, &row.RevisesEventID, &row.PublishState, &payload); err != nil {
			t.Fatalf("scan outbox row: %v", err)
		}
		if err := json.Unmarshal(payload, &row.Payload); err != nil {
			t.Fatalf("decode payload of %s: %v", row.EventID, err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate outbox rows: %v", err)
	}
	return out
}

func outboxCountForAggregate(t *testing.T, ctx context.Context, pool *pgxpool.Pool, aggregateID string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE aggregate_id = $1`, aggregateID).Scan(&n); err != nil {
		t.Fatalf("count outbox rows for %s: %v", aggregateID, err)
	}
	return n
}

// outboxSeedCreatedEvent commits one deposit.observation.created through the
// exact producer helper (no producer path exists for a pre-seeded observation
// in these fixtures) so version continuity from 1 is exercised.
func outboxSeedCreatedEvent(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64,
	blockNumber uint64, blockHash, txHash string, logIndex uint64) {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin seed created event: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := appendDepositObservationCreatedEvent(ctx, tx, chainID, blockNumber, blockHash, txHash, logIndex); err != nil {
		t.Fatalf("append seed created event: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit seed created event: %v", err)
	}
}

// outboxWantForbiddenFree fails when a payload carries forbidden material
// (data-model §9) by reusing the frozen runtime scan.
func outboxWantForbiddenFree(t *testing.T, eventID string, payload map[string]any) {
	t.Helper()
	if key, found := events.ForbiddenPayload(payload); found {
		t.Fatalf("event %s payload carries forbidden material %q", eventID, key)
	}
}

// TestOutboxDepositCreatedCommitReplayRollback is the T026 probe: the commit
// path emits exactly one evm_log event with a continuous version and the log
// triple identity; a replay of the same fact appends nothing; an identity
// conflict rolls the batch back without any event.
func TestOutboxDepositCreatedCommitReplayRollback(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	const chainID = int64(913001)
	cfg := depositITConfig(t, chainID, testContractA)
	depositSeedCanonical(t, ctx, pool, chainID, 10, 20, true)
	depositSeedUpstream(t, ctx, pool, chainID, 0, cfg.LogConfigHash, 21)
	depositSeedTransferRow(t, ctx, pool, chainID, 12, depositBlockHash(12), depositTxHash(12, 0), 0,
		common.HexToAddress(testContractA), common.HexToAddress(testContractB), common.HexToAddress(depositWatchAddr), big.NewInt(1))

	sc := depositITScanner(t, pool, cfg)
	lease := depositITLease(t, pool, chainID)
	rcap := testRecoveryCap(t, ctx, pool, chainID)
	unit, batch, progress := depositITPrepareUnit(t, ctx, sc, 10, 20)
	if err := sc.commitDepositUnit(ctx, lease, unit, batch, progress, 10, 20, rcap); err != nil {
		t.Fatalf("commitDepositUnit(): %v", err)
	}

	aggregateID := depositObservationID(chainID, depositBlockHash(12), depositTxHash(12, 0), 0)
	rows := outboxRowsForAggregate(t, ctx, pool, aggregateID)
	if len(rows) != 1 {
		t.Fatalf("outbox rows for %s = %d, want 1", aggregateID, len(rows))
	}
	created := rows[0]
	if created.EventType != events.EventTypeDepositObservationCreated || created.SchemaVersion != events.SchemaVersionV1 ||
		created.IdentityKind != string(events.IdentityKindEVMLog) || created.AggregateType != depositObservationAggregateType ||
		created.AggregateVersion != 1 || created.PublishState != "pending" {
		t.Fatalf("created event = %+v, want created/v1/evm_log/deposit_observation/v1/pending", created)
	}
	// Height is never an identity component: the event_id is the deterministic
	// UUIDv5 of the (chain, block_hash, tx_hash, log_index) triple.
	wantID := events.NewEventID(events.EVMLogNaturalKey(chainID, depositBlockHash(12), depositTxHash(12, 0), 0))
	if created.EventID != wantID.String() {
		t.Fatalf("event_id = %s, want UUIDv5 of the log triple %s", created.EventID, wantID)
	}
	if created.ChainID != chainID || created.BlockNumber != 12 ||
		created.BlockHash != depositBlockHash(12) || created.TxHash != depositTxHash(12, 0) || created.LogIndex != 0 {
		t.Fatalf("created event chain identity = %+v, want chain %d block 12 %s/%s/0",
			created, chainID, depositBlockHash(12), depositTxHash(12, 0))
	}
	if created.Payload["observation_id"] != aggregateID || created.Payload["state"] != depositObservationStatusPending {
		t.Fatalf("created payload = %v, want observation_id %s state pending", created.Payload, aggregateID)
	}
	outboxWantForbiddenFree(t, created.EventID, created.Payload)

	// Replay: rewind the durable progress to the captured basis so the exact
	// same fact is processed again. The observation already exists, so the
	// producer appends nothing and no version moves (one Append per committed
	// business transition).
	if _, err := pool.Exec(ctx, `UPDATE deposit_checkpoint SET next_block = 10 WHERE chain_id = $1`, chainID); err != nil {
		t.Fatalf("rewind checkpoint: %v", err)
	}
	unit2, batch2, captured2 := depositITPrepareUnit(t, ctx, sc, 10, 20)
	if err := sc.commitDepositUnit(ctx, lease, unit2, batch2, captured2, 10, 20, rcap); err != nil {
		t.Fatalf("replay commitDepositUnit(): %v", err)
	}
	if n := depositCountRows(t, ctx, pool, "deposit_observations", chainID); n != 1 {
		t.Fatalf("observations after replay = %d, want 1", n)
	}
	if n := outboxCountForAggregate(t, ctx, pool, aggregateID); n != 1 {
		t.Fatalf("outbox rows after replay = %d, want 1 (no duplicate event)", n)
	}
	if got := outboxRowsForAggregate(t, ctx, pool, aggregateID)[0].AggregateVersion; got != 1 {
		t.Fatalf("aggregate_version after replay = %d, want 1", got)
	}

	// Rollback: an identity conflict (same log identity, different content)
	// refuses the whole batch. Neither the pre-existing observation nor any
	// event row for the conflicting batch may exist.
	const conflictChain = int64(913002)
	conflictCfg := depositITConfig(t, conflictChain, testContractA)
	depositSeedCanonical(t, ctx, pool, conflictChain, 10, 20, true)
	depositSeedUpstream(t, ctx, pool, conflictChain, 0, conflictCfg.LogConfigHash, 21)
	depositSeedCheckpoint(t, ctx, pool, conflictChain, 10, conflictCfg.ConfigHash, 10)
	depositSeedHistory(t, ctx, pool, conflictChain, 1, 10, conflictCfg.ConfigHash)
	conflictBH, conflictTx := depositBlockHash(12), depositTxHash(12, 0)
	depositSeedObservation(t, ctx, pool, conflictChain, 12, conflictBH, conflictTx, 0, "999", 1)
	depositSeedTransferRow(t, ctx, pool, conflictChain, 12, conflictBH, conflictTx, 0,
		common.HexToAddress(testContractA), common.HexToAddress(testContractB), common.HexToAddress(depositWatchAddr), big.NewInt(1))

	conflictScanner := depositITScanner(t, pool, conflictCfg)
	conflictLease := depositITLease(t, pool, conflictChain)
	conflictCap := testRecoveryCap(t, ctx, pool, conflictChain)
	conflictUnit, conflictBatch, conflictCaptured := depositITPrepareUnit(t, ctx, conflictScanner, 10, 20)
	err := conflictScanner.commitDepositUnit(ctx, conflictLease, conflictUnit, conflictBatch, conflictCaptured, 10, 20, conflictCap)
	var conflict *depositIdentityConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("conflicting commit = %v, want *depositIdentityConflictError", err)
	}
	if n := outboxCountForAggregate(t, ctx, pool, depositObservationID(conflictChain, conflictBH, conflictTx, 0)); n != 0 {
		t.Fatalf("outbox rows after refused commit = %d, want 0 (rollback leaves no event)", n)
	}
	if n := depositCountRows(t, ctx, pool, "deposit_observations", conflictChain); n != 1 {
		t.Fatalf("observations after refused commit = %d, want 1 (no overwrite)", n)
	}
}

// TestOutboxConfirmationConfirmedAtomicity is the T027 probe: the committed
// conversion emits one version-2 event carrying the policy version and the
// confirmed block identity; a refused conversion writes no event.
func TestOutboxConfirmationConfirmedAtomicity(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	const (
		chainID = int64(913003)
		h       = uint64(100)
		tip     = uint64(109)
		n       = uint64(10)
	)
	depositSeedCanonical(t, ctx, pool, chainID, h, tip, true)
	bh, txHash := confirmSeedPending(t, ctx, pool, chainID, h)
	// Pre-seed the created event so the confirmation is the second version of
	// the same aggregate (version continuity from 1).
	outboxSeedCreatedEvent(t, ctx, pool, chainID, h, bh, txHash, 0)
	confirmSeedPolicyRow(t, ctx, pool, chainID, 1, int64(n), nil, "bootstrap", nil)

	c, lease := confirmCommitter(t, pool, chainID, n)
	rcap := testRecoveryCap(t, ctx, pool, chainID)
	basis := ConfirmBasis{BlockHash: bh, TxHash: txHash, Height: h,
		TipNumber: tip, TipHash: depositBlockHash(tip), PolicySeq: 1, ThresholdN: n}
	if err := c.ConfirmDepositUnit(ctx, lease, basis, rcap); err != nil {
		t.Fatalf("ConfirmDepositUnit(): %v", err)
	}

	aggregateID := depositObservationID(chainID, bh, txHash, 0)
	rows := outboxRowsForAggregate(t, ctx, pool, aggregateID)
	if len(rows) != 2 {
		t.Fatalf("outbox rows after confirmation = %d, want 2 (created + confirmed)", len(rows))
	}
	confirmation := rows[1]
	if confirmation.EventType != events.EventTypeDepositConfirmationConfirmed ||
		confirmation.SchemaVersion != events.SchemaVersionV1 ||
		confirmation.IdentityKind != string(events.IdentityKindBusinessObject) ||
		confirmation.AggregateVersion != 2 {
		t.Fatalf("confirmation event = %+v, want confirmed/v1/business_object/v2", confirmation)
	}
	if confirmation.Payload["policy_version"] != float64(1) ||
		confirmation.Payload["confirmed_block_number"] != float64(h) ||
		confirmation.Payload["confirmed_block_hash"] != bh {
		t.Fatalf("confirmation payload = %v, want policy_version 1 height %d hash %s", confirmation.Payload, h, bh)
	}
	if confirmation.Payload["chain_id"] != float64(chainID) {
		t.Fatalf("confirmation payload chain_id = %v, want %d", confirmation.Payload["chain_id"], chainID)
	}
	outboxWantForbiddenFree(t, confirmation.EventID, confirmation.Payload)

	// Refused conversion: a mismatched captured tip refuses with zero writes
	// and therefore no event.
	const refusedH = uint64(101)
	refusedBH, refusedTx := depositBlockHash(refusedH), depositTxHash(refusedH, 0)
	depositSeedObservation(t, ctx, pool, chainID, refusedH, refusedBH, refusedTx, 0, "1", 1)
	refusedBasis := ConfirmBasis{BlockHash: refusedBH, TxHash: refusedTx, Height: refusedH,
		TipNumber: tip, TipHash: strings.Repeat("bb", 32), PolicySeq: 1, ThresholdN: n}
	if err := c.ConfirmDepositUnit(ctx, lease, refusedBasis, rcap); err == nil {
		t.Fatal("refused conversion unexpectedly succeeded")
	}
	if n := outboxCountForAggregate(t, ctx, pool, depositObservationID(chainID, refusedBH, refusedTx, 0)); n != 0 {
		t.Fatalf("outbox rows after refused conversion = %d, want 0", n)
	}
}

// TestOutboxStatusChangedOrphanReviveAtomicity is the T028 probe: the orphan
// conversion and the in-place revival each emit exactly one status_changed
// event with the true from_state and a continuous version; repeat execution
// converges with zero new events.
func TestOutboxStatusChangedOrphanReviveAtomicity(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	const chainID = int64(913004)
	lease := depositITLease(t, pool, chainID)
	reorgSeedChain(t, ctx, pool, chainID, 10, 20, 10)
	depositSeedHistory(t, ctx, pool, chainID, 1, 10, strings.Repeat("aa", 32))
	if _, err := pool.Exec(ctx, `
INSERT INTO deposit_checkpoint (chain_id, start_block, config_hash, next_block)
VALUES ($1, 10, $2, 20)`, chainID, strings.Repeat("aa", 32)); err != nil {
		t.Fatalf("seed deposit_checkpoint: %v", err)
	}
	obsBH, obsTx := depositBlockHash(17), depositTxHash(17, 0)
	depositSeedObservation(t, ctx, pool, chainID, 17, obsBH, obsTx, 0, "50", 1)
	depositSeedTransferRow(t, ctx, pool, chainID, 17, obsBH, obsTx, 0,
		common.HexToAddress(testContractA), common.HexToAddress(testContractB), common.HexToAddress(depositWatchAddr), big.NewInt(50))
	outboxSeedCreatedEvent(t, ctx, pool, chainID, 17, obsBH, obsTx, 0)

	res := reorgEstablishOne(t, ctx, pool, lease, chainID, 20)
	owned := RecoveryCapture{Seq: res.Seq, Owned: &RecoveryOwned{RecoveryID: res.RecoveryID}}
	if err := ConfirmRecoveryAncestor(ctx, pool, lease, chainID, owned, 15, depositBlockHash(15), "t038"); err != nil {
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
	orphanEvent := rows[1]
	if orphanEvent.EventType != events.EventTypeDepositObservationStatusChanged || orphanEvent.AggregateVersion != 2 {
		t.Fatalf("orphan event = %+v, want status_changed v2", orphanEvent)
	}
	if orphanEvent.Payload["from_state"] != depositObservationStatusPending ||
		orphanEvent.Payload["to_state"] != observationStateOrphaned ||
		orphanEvent.Payload["reason"] != observationReasonReorgInvalidated {
		t.Fatalf("orphan payload = %v, want pending->orphaned reorg_invalidated", orphanEvent.Payload)
	}
	if orphanEvent.Payload["chain_id"] != float64(chainID) || orphanEvent.Payload["block_number"] != float64(17) ||
		orphanEvent.Payload["block_hash"] != obsBH || orphanEvent.Payload["tx_hash"] != obsTx {
		t.Fatalf("orphan payload chain identity = %v, want chain %d height 17 %s/%s", orphanEvent.Payload, chainID, obsBH, obsTx)
	}
	outboxWantForbiddenFree(t, orphanEvent.EventID, orphanEvent.Payload)

	// T052: the same conversion carries the revision fact — Orphaned
	// disposition, the pre-reorg event superseded, the 006 recovery version
	// and the old block identity.
	orphanRevision := rows[2]
	if orphanRevision.EventType != events.EventTypeDepositRevisionApplied || orphanRevision.AggregateVersion != 3 {
		t.Fatalf("orphan revision = %+v, want deposit.revision.applied v3", orphanRevision)
	}
	if orphanRevision.Payload["to_state"] != observationStateOrphaned ||
		orphanRevision.Payload["reason"] != observationReasonReorgInvalidated {
		t.Fatalf("orphan revision payload = %v, want to_state orphaned reorg_invalidated", orphanRevision.Payload)
	}
	if orphanRevision.RevisesEventID == nil || *orphanRevision.RevisesEventID != rows[0].EventID {
		t.Fatalf("orphan revision revises_event_id = %v, want the pre-reorg created event %s",
			orphanRevision.RevisesEventID, rows[0].EventID)
	}
	if orphanRevision.RecoveryVersion == nil || *orphanRevision.RecoveryVersion != res.Seq {
		t.Fatalf("orphan revision recovery_version = %v, want %d", orphanRevision.RecoveryVersion, res.Seq)
	}
	superseded, ok := orphanRevision.Payload["superseded_identity"].(map[string]any)
	if !ok || superseded["event_id"] != rows[0].EventID || superseded["event_type"] != events.EventTypeDepositObservationCreated {
		t.Fatalf("orphan revision superseded_identity = %v, want the created event %s", orphanRevision.Payload["superseded_identity"], rows[0].EventID)
	}
	if superseded["block_hash"] != obsBH || superseded["block_number"] != float64(17) {
		t.Fatalf("orphan revision superseded_identity block identity = %v, want %s/%d", superseded, obsBH, 17)
	}
	outboxWantForbiddenFree(t, orphanRevision.EventID, orphanRevision.Payload)

	// Repeat execution converges: zero new transitions, zero new events and no
	// version bump (a retry can never fabricate a duplicate business
	// transition or a wrong version).
	if repeat, err := InvalidateRecoveryObservations(ctx, pool, lease, chainID, owned); err != nil || repeat != 0 {
		t.Fatalf("repeat invalidate = %d (err %v), want 0", repeat, err)
	}
	if n := outboxCountForAggregate(t, ctx, pool, aggregateID); n != 3 {
		t.Fatalf("outbox rows after repeat invalidate = %d, want 3", n)
	}

	// Revival: re-canonicalize the old fork height, then revive in place. The
	// dedicated reinstated fact reuses the original observation (never a
	// second created) and the revision fact supersedes the invalidation.
	if err := RecanonicalizeRecoveryBlock(ctx, pool, lease, chainID, owned, 17, obsBH); err != nil {
		t.Fatalf("recanonicalize 17: %v", err)
	}
	converted, err := ReviveRecoveryObservation(ctx, pool, lease, chainID, owned, obsBH, obsTx, 0, "t038 revive")
	if err != nil || !converted {
		t.Fatalf("revive = %v (err %v), want converted", converted, err)
	}
	rows = outboxRowsForAggregate(t, ctx, pool, aggregateID)
	if len(rows) != 5 {
		t.Fatalf("outbox rows after revive = %d, want 5 (created + orphan + revision + reinstated + revision)", len(rows))
	}
	reviveEvent := rows[3]
	if reviveEvent.EventType != events.EventTypeDepositObservationReinstated || reviveEvent.AggregateVersion != 4 {
		t.Fatalf("revive event = %+v, want deposit.observation.reinstated v4", reviveEvent)
	}
	if reviveEvent.Payload["observation_id"] != aggregateID ||
		reviveEvent.Payload["reason"] != observationReasonReorgRevived ||
		reviveEvent.Payload["revive_basis"] != observationReviveBasisRecanonicalized {
		t.Fatalf("reinstated payload = %v, want observation_id %s reason %s", reviveEvent.Payload, aggregateID, observationReasonReorgRevived)
	}
	if reviveEvent.Payload["block_hash"] != obsBH || reviveEvent.Payload["chain_id"] != float64(chainID) {
		t.Fatalf("reinstated chain identity = %v, want chain %d block %s", reviveEvent.Payload, chainID, obsBH)
	}
	if reviveEvent.EventType == events.EventTypeDepositObservationCreated {
		t.Fatal("the revival reused the created type instead of the dedicated reinstated fact")
	}
	outboxWantForbiddenFree(t, reviveEvent.EventID, reviveEvent.Payload)

	reviveRevision := rows[4]
	if reviveRevision.EventType != events.EventTypeDepositRevisionApplied || reviveRevision.AggregateVersion != 5 {
		t.Fatalf("revive revision = %+v, want deposit.revision.applied v5", reviveRevision)
	}
	if reviveRevision.Payload["to_state"] != depositObservationStatusPending ||
		reviveRevision.Payload["reason"] != observationReasonReorgRevived {
		t.Fatalf("revive revision payload = %v, want to_state pending reorg_revived", reviveRevision.Payload)
	}
	if reviveRevision.RevisesEventID == nil || *reviveRevision.RevisesEventID != orphanRevision.EventID {
		t.Fatalf("revive revision revises_event_id = %v, want the invalidation revision %s",
			reviveRevision.RevisesEventID, orphanRevision.EventID)
	}
	if reviveRevision.RecoveryVersion == nil || *reviveRevision.RecoveryVersion != res.Seq {
		t.Fatalf("revive revision recovery_version = %v, want %d", reviveRevision.RecoveryVersion, res.Seq)
	}
	outboxWantForbiddenFree(t, reviveRevision.EventID, reviveRevision.Payload)

	// Repeat revival converges on the transition log with zero new events.
	if repeat, err := ReviveRecoveryObservation(ctx, pool, lease, chainID, owned, obsBH, obsTx, 0, "t038 revive repeat"); err != nil || repeat {
		t.Fatalf("repeat revive = %v (err %v), want converged false", repeat, err)
	}
	if n := outboxCountForAggregate(t, ctx, pool, aggregateID); n != 5 {
		t.Fatalf("outbox rows after repeat revive = %d, want 5", n)
	}
	// The event stream versions are continuous from 1 for the object.
	for i, row := range outboxRowsForAggregate(t, ctx, pool, aggregateID) {
		if row.AggregateVersion != int64(i+1) {
			t.Fatalf("aggregate version sequence = %v, want 1..5", row.AggregateVersion)
		}
	}
}
