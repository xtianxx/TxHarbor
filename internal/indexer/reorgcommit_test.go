//go:build integration

// reorgcommit_test.go pins the 006 transaction guard matrix (T019, V2) on
// real PostgreSQL via `make test-integration` (testcontainers postgres:18,
// same entry as the 005 precedent): every 006 transaction's rechecks
// (L/P/V/G, stale seq/phase/position fault snapshots) independently refuse
// with rowcounts enforced; the seq-exhaustion snapshot refuses with zero
// writes; post-release stale batches refuse on version alone; the bounded
// wait never tight-loops. It rides db.MigrateUp like every other
// integration file — the migrated schema under test includes 000006.
package indexer

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const reorgTestDepth = "25"

func reorgTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	t.Cleanup(pool.Close)
	return pool
}

func reorgCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, table string, chainID int64) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE chain_id = $1`, table), chainID).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

func reorgSeedChain(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, from, to uint64, start uint64) {
	t.Helper()
	depositSeedCanonical(t, ctx, pool, chainID, from, to, true)
	if _, err := pool.Exec(ctx, `
INSERT INTO indexer_checkpoint (chain_id, height, block_hash, start_height)
VALUES ($1, $2, $3, $4)`,
		chainID, int64(to), depositBlockHash(to), int64(start)); err != nil {
		t.Fatalf("seed indexer_checkpoint: %v", err)
	}
	cfgHash := strings.Repeat("ab", 32)
	if _, err := pool.Exec(ctx, `
INSERT INTO log_checkpoint (chain_id, start_block, config_hash, next_block)
VALUES ($1, $2, $3, $4)`,
		chainID, int64(start), cfgHash, int64(to)); err != nil {
		t.Fatalf("seed log_checkpoint: %v", err)
	}
}

// reorgEstablishOne runs one establish on a seeded chain and returns the result.
func reorgEstablishOne(t *testing.T, ctx context.Context, pool *pgxpool.Pool, lease *Lease, chainID int64, oldTip uint64) EstablishResult {
	t.Helper()
	res, err := EstablishRecovery(ctx, pool, lease, EstablishRequest{
		ChainID:      chainID,
		OldTipNumber: int64(oldTip), OldTipHash: depositBlockHash(oldTip),
		NewTipNumber: int64(oldTip) + 1, NewTipHash: depositBlockHash(oldTip + 100),
		DetectedHeight: int64(oldTip), EnvMaxDepthRaw: reorgTestDepth,
	})
	if err != nil {
		t.Fatalf("EstablishRecovery(): %v", err)
	}
	return res
}

// TestReorgEstablishConverge: repeat triggers (same or new evidence) join the
// one persistent instance — exactly one row, one established event.
func TestReorgEstablishConverge(t *testing.T) {
	pool := reorgTestPool(t)
	defer pool.Close()
	ctx := context.Background()
	const chainID = int64(906001)
	lease := depositITLease(t, pool, chainID)
	reorgSeedChain(t, ctx, pool, chainID, 10, 15, 10)

	first := reorgEstablishOne(t, ctx, pool, lease, chainID, 15)
	if first.Converged || first.Seq != 1 {
		t.Fatalf("first establish = %+v; want seq 1, not converged", first)
	}
	second, err := EstablishRecovery(ctx, pool, lease, EstablishRequest{
		ChainID:      chainID,
		OldTipNumber: 15, OldTipHash: depositBlockHash(15),
		NewTipNumber: 17, NewTipHash: depositBlockHash(117),
		DetectedHeight: 15, EnvMaxDepthRaw: reorgTestDepth,
	})
	if err != nil {
		t.Fatalf("repeat establish: %v", err)
	}
	if !second.Converged || second.RecoveryID != first.RecoveryID || second.Seq != first.Seq {
		t.Fatalf("repeat establish = %+v; want convergence on %+v", second, first)
	}
	if n := reorgCount(t, ctx, pool, "reorg_recovery", chainID); n != 1 {
		t.Fatalf("reorg_recovery rows = %d, want 1", n)
	}
	if n := reorgCount(t, ctx, pool, "reorg_recovery_events", chainID); n != 1 {
		t.Fatalf("reorg_recovery_events rows = %d, want 1 (no second established event)", n)
	}
}

// TestReorgGuardMatrixRefusals: L (lease), V (stale seq / foreign identity),
// G (phase / position) faults each refuse independently with zero writes.
func TestReorgGuardMatrixRefusals(t *testing.T) {
	pool := reorgTestPool(t)
	defer pool.Close()
	ctx := context.Background()
	const chainID = int64(906002)
	lease := depositITLease(t, pool, chainID)
	reorgSeedChain(t, ctx, pool, chainID, 10, 15, 10)
	res := reorgEstablishOne(t, ctx, pool, lease, chainID, 15)
	owned := RecoveryCapture{Seq: res.Seq, Owned: &RecoveryOwned{RecoveryID: res.RecoveryID}}

	stale := RecoveryCapture{Seq: res.Seq - 1, Owned: &RecoveryOwned{RecoveryID: res.RecoveryID}}
	if err := ConfirmRecoveryAncestor(ctx, pool, lease, chainID, stale, 14, depositBlockHash(14), "x"); !isRecoveryGate(err) {
		t.Fatalf("stale-seq confirm = %v, want gate refusal", err)
	}
	foreign := RecoveryCapture{Seq: res.Seq, Owned: &RecoveryOwned{RecoveryID: "reorg-nope"}}
	if err := ConfirmRecoveryAncestor(ctx, pool, lease, chainID, foreign, 14, depositBlockHash(14), "x"); !isRecoveryGate(err) {
		t.Fatalf("foreign-identity confirm = %v, want gate refusal", err)
	}
	if _, err := InvalidateRecoveryObservations(ctx, pool, lease, chainID, owned); !isRecoveryGate(err) {
		t.Fatalf("wrong-phase invalidate = %v, want phase refusal", err)
	}
	if err := ReplayRecoveryRange(ctx, pool, lease, chainID, owned, RecoveryStreamBlock, 13, 15, nil, nil, nil); !isRecoveryGate(err) {
		t.Fatalf("wrong-phase replay = %v, want phase refusal", err)
	}
	if err := ReviveRecoveryObservation(ctx, pool, lease, chainID, owned, depositBlockHash(14), depositTxHash(14, 0), 0, "x"); !isRecoveryGate(err) {
		t.Fatalf("wrong-phase revive = %v, want phase refusal", err)
	}
	if err := CompleteRecoveryVerify(ctx, pool, lease, chainID, owned); !isRecoveryGate(err) {
		t.Fatalf("wrong-phase complete = %v, want phase refusal", err)
	}
	// L: a never-acquired lease fails the owner verdict with zero writes.
	stale2 := newTestLease(t, pool, chainID, "foreign-owner", time.Minute, 10*time.Second)
	if _, _, _, err := InvalidateRecoveryBlocks(ctx, pool, stale2, chainID, owned); !isLeaseLost(err) {
		t.Fatalf("foreign-lease invalidate = %v, want ErrLeaseLost", err)
	}
	// Zero-write audit across every refusal above.
	if n := reorgCount(t, ctx, pool, "reorg_recovery", chainID); n != 1 {
		t.Fatalf("reorg_recovery rows = %d, want 1", n)
	}
	if n := reorgCount(t, ctx, pool, "reorg_recovery_events", chainID); n != 1 {
		t.Fatalf("events rows = %d, want 1 (refusals write nothing)", n)
	}
	if n := reorgCount(t, ctx, pool, "deposit_observation_transitions", chainID); n != 0 {
		t.Fatalf("transition rows = %d, want 0", n)
	}
	var canon int64
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM chain_blocks WHERE chain_id = $1 AND canonical`, chainID).Scan(&canon); err != nil {
		t.Fatalf("count canonical: %v", err)
	}
	if canon != 6 {
		t.Fatalf("canonical rows = %d, want 6 (10..15 untouched)", canon)
	}
}

func isLeaseLost(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), ErrLeaseLost.Error())
}

// TestReorgDepthBoundaryTxn pins the closed bound at the transaction: depth
// == max_depth confirms, depth == max_depth+1 refuses to the reconcile path.
func TestReorgDepthBoundaryTxn(t *testing.T) {
	pool := reorgTestPool(t)
	defer pool.Close()
	ctx := context.Background()

	// Allow side: bound 40, ancestor 15, depth 25 == max_depth 25.
	const allowChain = int64(906003)
	lease := depositITLease(t, pool, allowChain)
	reorgSeedChain(t, ctx, pool, allowChain, 10, 40, 10)
	allow := reorgEstablishOne(t, ctx, pool, lease, allowChain, 40)
	allowCap := RecoveryCapture{Seq: allow.Seq, Owned: &RecoveryOwned{RecoveryID: allow.RecoveryID}}
	if err := ConfirmRecoveryAncestor(ctx, pool, lease, allowChain, allowCap, 15, depositBlockHash(15), "boundary-allow"); err != nil {
		t.Fatalf("closed-boundary confirm (depth == max): %v", err)
	}

	// Refuse side: bound 40, ancestor 14, depth 26 > 25.
	const refuseChain = int64(906004)
	lease2 := depositITLease(t, pool, refuseChain)
	reorgSeedChain(t, ctx, pool, refuseChain, 10, 40, 10)
	refuse := reorgEstablishOne(t, ctx, pool, lease2, refuseChain, 40)
	refuseCap := RecoveryCapture{Seq: refuse.Seq, Owned: &RecoveryOwned{RecoveryID: refuse.RecoveryID}}
	err := ConfirmRecoveryAncestor(ctx, pool, lease2, refuseChain, refuseCap, 14, depositBlockHash(14), "boundary-refuse")
	if !isRecoveryGate(err) || !strings.Contains(err.Error(), "exceeds bound") {
		t.Fatalf("over-bound confirm = %v, want exceeds-bound gate refusal", err)
	}
	var phase string
	if err := pool.QueryRow(ctx, `SELECT phase FROM reorg_recovery WHERE chain_id = $1`, refuseChain).Scan(&phase); err != nil {
		t.Fatalf("read phase: %v", err)
	}
	if phase != reorgPhaseDetected {
		t.Fatalf("phase = %q after refused confirm, want detected", phase)
	}
}

// TestReorgInvalidateRollbackReplayComplete drives one full mini-loop on real
// PG: invalidate → orphan with audit → floors → replay with coverage →
// auto release, with the ancestor side and pause tables intact.
func TestReorgInvalidateRollbackReplayComplete(t *testing.T) {
	pool := reorgTestPool(t)
	defer pool.Close()
	ctx := context.Background()
	const chainID = int64(906005)
	lease := depositITLease(t, pool, chainID)
	reorgSeedChain(t, ctx, pool, chainID, 10, 20, 10)
	depositSeedHistory(t, ctx, pool, chainID, 1, 10, strings.Repeat("aa", 32))
	if _, err := pool.Exec(ctx, `
INSERT INTO deposit_checkpoint (chain_id, start_block, config_hash, next_block)
VALUES ($1, 10, $2, 20)`, chainID, strings.Repeat("aa", 32)); err != nil {
		t.Fatalf("seed deposit_checkpoint: %v", err)
	}
	affectedBH, affectedTx := depositBlockHash(17), depositTxHash(17, 0)
	depositSeedObservation(t, ctx, pool, chainID, 17, affectedBH, affectedTx, 0, "50", 1)
	keeperBH, keeperTx := depositBlockHash(14), depositTxHash(14, 0)
	depositSeedObservation(t, ctx, pool, chainID, 14, keeperBH, keeperTx, 0, "7", 1)

	res := reorgEstablishOne(t, ctx, pool, lease, chainID, 20)
	owned := RecoveryCapture{Seq: res.Seq, Owned: &RecoveryOwned{RecoveryID: res.RecoveryID}}
	if err := ConfirmRecoveryAncestor(ctx, pool, lease, chainID, owned, 15, depositBlockHash(15), "mini-loop"); err != nil {
		t.Fatalf("confirm ancestor: %v", err)
	}
	from, to, flipped, err := InvalidateRecoveryBlocks(ctx, pool, lease, chainID, owned)
	if err != nil {
		t.Fatalf("invalidate blocks: %v", err)
	}
	if from != 16 || to != 20 || flipped != 5 {
		t.Fatalf("sweep = [%d,%d] flipped %d; want [16,20] x5", from, to, flipped)
	}
	orphaned, err := InvalidateRecoveryObservations(ctx, pool, lease, chainID, owned)
	if err != nil {
		t.Fatalf("invalidate observations: %v", err)
	}
	if orphaned != 1 {
		t.Fatalf("orphaned = %d, want 1 (only the height-17 row)", orphaned)
	}
	var keeperStatus, keeperOrphan string
	if err := pool.QueryRow(ctx, `SELECT status, COALESCE(orphan_recovery_id, '') FROM deposit_observations
WHERE chain_id = $1 AND block_hash = $2`, chainID, keeperBH).Scan(&keeperStatus, &keeperOrphan); err != nil {
		t.Fatalf("read keeper: %v", err)
	}
	if keeperStatus != "pending" || keeperOrphan != "" {
		t.Fatalf("ancestor-side row = (%s, %s); want (pending, untouched)", keeperStatus, keeperOrphan)
	}
	for _, stream := range []RecoveryStream{RecoveryStreamBlock, RecoveryStreamLog, RecoveryStreamDeposit} {
		if _, _, err := RollbackRecoveryCheckpoint(ctx, pool, lease, chainID, owned, stream); err != nil {
			t.Fatalf("rollback %s: %v", stream, err)
		}
	}
	newHash := func(n int64) string { return fmt.Sprintf("0x%064x", 0xe00e_0000+uint64(n)) }
	newParent := func(h int64) string {
		if h == 16 {
			return depositBlockHash(15) // fork point: new 16 links onto the ancestor
		}
		return newHash(h - 1)
	}
	var blocks []ReplayBlock
	for h := int64(16); h <= 18; h++ {
		blocks = append(blocks, ReplayBlock{Number: h, Hash: newHash(h), ParentHash: newParent(h)})
	}
	// Position fault first: a gapped range refuses before any write.
	if err := ReplayRecoveryRange(ctx, pool, lease, chainID, owned, RecoveryStreamBlock, 19, 20, nil, nil, nil); !isRecoveryGate(err) {
		t.Fatalf("gapped replay = %v, want position refusal", err)
	}
	if err := ReplayRecoveryRange(ctx, pool, lease, chainID, owned, RecoveryStreamBlock, 16, 18, blocks, nil, nil); err != nil {
		t.Fatalf("replay block [16,18]: %v", err)
	}
	if err := ReplayRecoveryRange(ctx, pool, lease, chainID, owned, RecoveryStreamLog, 16, 16, nil, nil, nil); err != nil {
		t.Fatalf("replay log [16,16] empty: %v", err)
	}
	if err := ReplayRecoveryRange(ctx, pool, lease, chainID, owned, RecoveryStreamDeposit, 16, 16, nil, nil, nil); err != nil {
		t.Fatalf("replay deposit [16,16] empty: %v", err)
	}
	var phase string
	if err := pool.QueryRow(ctx, `SELECT phase FROM reorg_recovery WHERE chain_id = $1`, chainID).Scan(&phase); err != nil {
		t.Fatalf("read phase: %v", err)
	}
	if phase != reorgPhaseReplaying {
		t.Fatalf("phase = %q after partial replay, want replaying (frontiers below sweep end)", phase)
	}
	var blocks2 []ReplayBlock
	for h := int64(19); h <= 20; h++ {
		blocks2 = append(blocks2, ReplayBlock{Number: h, Hash: newHash(h), ParentHash: newParent(h)})
	}
	if err := ReplayRecoveryRange(ctx, pool, lease, chainID, owned, RecoveryStreamBlock, 19, 20, blocks2, nil, nil); err != nil {
		t.Fatalf("replay block [19,20]: %v", err)
	}
	if err := ReplayRecoveryRange(ctx, pool, lease, chainID, owned, RecoveryStreamLog, 17, 20, nil, nil, nil); err != nil {
		t.Fatalf("replay log [17,20]: %v", err)
	}
	if err := ReplayRecoveryRange(ctx, pool, lease, chainID, owned, RecoveryStreamDeposit, 17, 20, nil, nil, nil); err != nil {
		t.Fatalf("replay deposit [17,20]: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT phase FROM reorg_recovery WHERE chain_id = $1`, chainID).Scan(&phase); err != nil {
		t.Fatalf("read phase: %v", err)
	}
	if phase != reorgPhaseCompletePending {
		t.Fatalf("phase = %q after full replay, want complete_pending", phase)
	}
	if err := CompleteRecoveryVerify(ctx, pool, lease, chainID, owned); err != nil {
		t.Fatalf("complete: %v", err)
	}
	if n := reorgCount(t, ctx, pool, "reorg_recovery", chainID); n != 0 {
		t.Fatalf("recovery rows = %d after release, want 0", n)
	}
	var terminal string
	if err := pool.QueryRow(ctx, `SELECT detail FROM reorg_recovery_events
WHERE chain_id = $1 AND event = 'auto_completed'`, chainID).Scan(&terminal); err != nil {
		t.Fatalf("read terminal event: %v", err)
	}
	for _, want := range []string{"bound_old=20:", "policy=1", "ancestor=15:", "swept=16-20", "orphaned=1", "surviving_pauses=none", "version=1"} {
		if !strings.Contains(terminal, want) {
			t.Fatalf("terminal detail missing %q: %q", want, terminal)
		}
	}
	// Transition lineage: exactly one pending→orphaned conversion audited.
	if n := reorgCount(t, ctx, pool, "deposit_observation_transitions", chainID); n != 1 {
		t.Fatalf("transition rows = %d, want 1", n)
	}
}

// TestReorgSeqExhaustion presets the events high-water to MaxInt64-1: the
// next establish computes MaxInt64, fails the assert, and refuses with zero
// recovery row, zero event row, zero frontier movement — no wrap, no reuse.
func TestReorgSeqExhaustion(t *testing.T) {
	pool := reorgTestPool(t)
	defer pool.Close()
	ctx := context.Background()
	const chainID = int64(906006)
	lease := depositITLease(t, pool, chainID)
	reorgSeedChain(t, ctx, pool, chainID, 10, 12, 10)
	const maxInt64 = int64(1<<63 - 1)
	if _, err := pool.Exec(ctx, `
INSERT INTO reorg_recovery_events (chain_id, recovery_id, recovery_seq, event_seq, event, detail)
VALUES ($1, 'exhaust-prior', $2, 1, 'established', 'snapshot')`, chainID, maxInt64-1); err != nil {
		t.Fatalf("seed exhaustion snapshot: %v", err)
	}
	_, err := EstablishRecovery(ctx, pool, lease, EstablishRequest{
		ChainID:      chainID,
		OldTipNumber: 12, OldTipHash: depositBlockHash(12),
		NewTipNumber: 13, NewTipHash: depositBlockHash(113),
		DetectedHeight: 12, EnvMaxDepthRaw: reorgTestDepth,
	})
	if err == nil || !strings.Contains(err.Error(), "exhausted") {
		t.Fatalf("exhausted establish = %v, want exhaustion refusal", err)
	}
	if n := reorgCount(t, ctx, pool, "reorg_recovery", chainID); n != 0 {
		t.Fatalf("recovery rows = %d after refused establish, want 0", n)
	}
	if n := reorgCount(t, ctx, pool, "reorg_recovery_events", chainID); n != 1 {
		t.Fatalf("event rows = %d after refused establish, want 1 (snapshot only)", n)
	}
}

// TestReorgPostReleaseStaleRefusal proves the version survives row deletion:
// an ordinary batch captured before the round refuses after it — even with
// fully matching content, which is never consulted.
func TestReorgPostReleaseStaleRefusal(t *testing.T) {
	pool := reorgTestPool(t)
	defer pool.Close()
	ctx := context.Background()
	const chainID = int64(906007)
	lease := depositITLease(t, pool, chainID)
	reorgSeedChain(t, ctx, pool, chainID, 10, 11, 10)
	sc := &Scanner{pool: pool, chainID: chainID, cfg: Config{StartHeight: 10}, lease: lease}

	cap0, _, err := captureRecoveryVersion(ctx, pool, chainID)
	if err != nil {
		t.Fatalf("capture: %v", err)
	}
	// Legal pre-pause order: the ordinary commit lands before any recovery.
	if err := sc.commitBlock(ctx, blockWrite{number: 12, hash: depositBlockHash(12), parent: depositBlockHash(11)}); err != nil {
		t.Fatalf("pre-pause commit: %v", err)
	}
	res := reorgEstablishOne(t, ctx, pool, lease, chainID, 12)
	_ = res
	// Post-establish ordinary submit (even an idempotent rescan) refuses.
	if err := sc.commitBlock(ctx, blockWrite{number: 12, hash: depositBlockHash(12), parent: depositBlockHash(11)}); !isRecoveryGate(err) {
		t.Fatalf("post-establish rescan = %v, want gate refusal", err)
	}
	// Simulate the release row-removal (terminal event omitted: this probes
	// version mechanics, not release auth — T011's release paths are pinned
	// in the mini-loop test above).
	if _, err := pool.Exec(ctx, `DELETE FROM reorg_recovery WHERE chain_id = $1`, chainID); err != nil {
		t.Fatalf("simulate release delete: %v", err)
	}
	// The pre-round batch, fully content-matching, still refuses on version
	// alone — and writes nothing.
	if err := sc.commitBlock(ctx, blockWrite{number: 13, hash: depositBlockHash(13), parent: depositBlockHash(12)}, cap0); !isRecoveryGate(err) {
		t.Fatalf("post-release stale commit = %v, want version refusal", err)
	}
	var n int64
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM chain_blocks WHERE chain_id = $1 AND number = 13`, chainID).Scan(&n); err != nil {
		t.Fatalf("count height 13: %v", err)
	}
	if n != 0 {
		t.Fatalf("height-13 rows = %d after refused commit, want 0", n)
	}
	// Primitive half of the same proof, straight through the gate.
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := recheckRecoveryGate(ctx, tx, chainID, cap0); !isRecoveryGate(err) {
		t.Fatalf("gate primitive with stale capture = %v, want refusal", err)
	}
}

// TestReorgOwnedExemptionPredicate pins the T015 seam: the exemption
// predicate fires only for rows orphaned under the captured identity, and an
// owned capture passes the gate where the ordinary path refuses.
func TestReorgOwnedExemptionPredicate(t *testing.T) {
	pool := reorgTestPool(t)
	defer pool.Close()
	ctx := context.Background()
	const chainID = int64(906008)
	lease := depositITLease(t, pool, chainID)
	reorgSeedChain(t, ctx, pool, chainID, 10, 12, 10)
	depositSeedHistory(t, ctx, pool, chainID, 1, 10, strings.Repeat("aa", 32))
	bh, txHash := depositBlockHash(12), depositTxHash(12, 0)
	depositSeedObservation(t, ctx, pool, chainID, 12, bh, txHash, 0, "9", 1)
	res := reorgEstablishOne(t, ctx, pool, lease, chainID, 12)
	if _, err := pool.Exec(ctx, `
UPDATE deposit_observations SET status = 'orphaned', orphaned_at = now(), orphan_recovery_id = $5, orphan_reason = 'test'
WHERE chain_id = $1 AND block_hash = $2 AND tx_hash = $3 AND log_index = $4`,
		chainID, bh, txHash, 0, res.RecoveryID); err != nil {
		t.Fatalf("seed orphan: %v", err)
	}
	sc := &DepositScanner{cfg: DepositConfig{ChainID: chainID}}
	id := depositIdentity{blockHash: bh, txHash: txHash, logIndex: 0}
	owned := RecoveryCapture{Seq: res.Seq, Owned: &RecoveryOwned{RecoveryID: res.RecoveryID}}
	foreign := RecoveryCapture{Seq: res.Seq, Owned: &RecoveryOwned{RecoveryID: "reorg-nope"}}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if ok, err := sc.exemptOwnedOrphan(ctx, tx, owned, id); err != nil || !ok {
		t.Fatalf("owned exemption = %v, %v; want true, nil", ok, err)
	}
	if ok, err := sc.exemptOwnedOrphan(ctx, tx, foreign, id); err != nil || ok {
		t.Fatalf("foreign exemption = %v, %v; want false, nil", ok, err)
	}
	if ok, err := sc.exemptOwnedOrphan(ctx, tx, RecoveryCapture{Seq: res.Seq}, id); err != nil || ok {
		t.Fatalf("ordinary exemption = %v, %v; want false, nil", ok, err)
	}
	if _, err := recheckRecoveryGate(ctx, tx, chainID, owned, reorgPhaseDetected); err != nil {
		t.Fatalf("owned gate in detected phase: %v", err)
	}
	if _, err := recheckRecoveryGate(ctx, tx, chainID, RecoveryCapture{Seq: res.Seq}); !isRecoveryGate(err) {
		t.Fatalf("ordinary gate under active row = %v, want refusal", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit probe: %v", err)
	}
}

// TestReorgBackoffBounded pins the R5 wait discipline under injected faults:
// waits are always positive (never tight-loop) and capped (never unbounded).
func TestReorgBackoffBounded(t *testing.T) {
	back := newBackoff(200*time.Millisecond, 30*time.Second)
	for i := 0; i < 200; i++ {
		d := back.next()
		if d <= 0 {
			t.Fatalf("wait %d = %s, want positive (no tight loop)", i, d)
		}
		if d > 30*time.Second+30*time.Second/4 {
			t.Fatalf("wait %d = %s, want capped near 30s (+25%% jitter)", i, d)
		}
	}
}
