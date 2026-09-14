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
	"errors"
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

// testRecoveryCap captures the recovery version for a direct test commit.
// Call it before the test builds its batch inputs (006 capture-first
// discipline for calls outside serve loops); the commit then rechecks this
// version under the lock instead of capturing at entry.
func testRecoveryCap(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64) RecoveryCapture {
	t.Helper()
	cap, _, err := captureRecoveryVersion(ctx, pool, chainID)
	if err != nil {
		t.Fatalf("capture recovery version: %v", err)
	}
	return cap
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
	if err := sc.commitBlock(ctx, blockWrite{number: 12, hash: depositBlockHash(12), parent: depositBlockHash(11)}, cap0); err != nil {
		t.Fatalf("pre-pause commit: %v", err)
	}
	res := reorgEstablishOne(t, ctx, pool, lease, chainID, 12)
	_ = res
	// Post-establish ordinary submit (even an idempotent rescan) refuses.
	if err := sc.commitBlock(ctx, blockWrite{number: 12, hash: depositBlockHash(12), parent: depositBlockHash(11)}, cap0); !isRecoveryGate(err) {
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

// --- T023: post-release bulk-orphan non-blocking proof (US2 tail, V4-tail) --
// R8 proof on real PostgreSQL: bulk Orphaned history coexists with live
// confirmation. status='pending' is the only selection key (never column
// non-NULLness); a selected-then-orphaned row halts exactly one tick via the
// commit-time re-read then clears; genuine anomalies still stop-not-skip;
// new Pendings confirm alongside arbitrary Orphaned history with zero
// structural stall. Every test asserts rowcounts/audit — never bare
// nil-error checks. Helper prefix is t023Bulk to avoid collisions.

// t023BulkSeedBase seeds a canonical range plus the 004 version row the
// observation FK needs.
func t023BulkSeedBase(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, from, to uint64) {
	t.Helper()
	depositSeedCanonical(t, ctx, pool, chainID, from, to, true)
	depositSeedHistory(t, ctx, pool, chainID, 1, 10, strings.Repeat("aa", 32))
}

// t023BulkOrphan flips one seeded pending row to orphaned in place (the
// production shape: status + orphan evidence, basis retained as history).
func t023BulkOrphan(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, bh, txHash string, idx int64, recoveryID string) {
	t.Helper()
	tag, err := pool.Exec(ctx, `
UPDATE deposit_observations SET status = 'orphaned', orphaned_at = now(), orphan_recovery_id = $5, orphan_reason = 't023-bulk-history'
WHERE chain_id = $1 AND block_hash = $2 AND tx_hash = $3 AND log_index = $4`,
		chainID, bh, txHash, idx, recoveryID)
	if err != nil {
		t.Fatalf("orphan %s/%s: %v", bh, txHash, err)
	}
	if tag.RowsAffected() != 1 {
		t.Fatalf("orphan %s/%s affected %d rows, want 1", bh, txHash, tag.RowsAffected())
	}
}

// t023BulkSeedOrphans plants count distinct bulk-history orphans at heights
// 10..29 (all inside any eligible window the tests use, so a status-blind
// filter would wrongly select them).
func t023BulkSeedOrphans(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, count int, recoveryID string, base uint64) {
	t.Helper()
	for k := 0; k < count; k++ {
		h := 10 + uint64(k%20)
		bh := fmt.Sprintf("0x%064x", base+uint64(k))
		txHash := fmt.Sprintf("0x%064x", base+0x100000+uint64(k))
		depositSeedObservation(t, ctx, pool, chainID, h, bh, txHash, 0, "3", 1)
		t023BulkOrphan(t, ctx, pool, chainID, bh, txHash, 0, recoveryID)
	}
}

// t023BulkScanner builds the real 005 committer + scanner (nil metrics is
// the detached-observation shape; commits still go through the real txn).
func t023BulkScanner(t *testing.T, pool *pgxpool.Pool, chainID int64, n uint64) (*ConfirmationCommitter, *ConfirmationScanner, *Lease) {
	t.Helper()
	c, lease := confirmCommitter(t, pool, chainID, n)
	sc, err := NewConfirmationScanner(pool, ConfirmationConfig{ChainID: chainID, ThresholdN: n}, c, nil)
	if err != nil {
		t.Fatalf("NewConfirmationScanner(): %v", err)
	}
	return c, sc, lease
}

// t023BulkStatusCount counts observations in one status for the chain.
func t023BulkStatusCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, status string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM deposit_observations WHERE chain_id = $1 AND status = $2`,
		chainID, status).Scan(&n); err != nil {
		t.Fatalf("count status %s: %v", status, err)
	}
	return n
}

// TestT023BulkOrphanCandidateExclusion: the status='pending' filter never
// selects historical Orphaned rows — not pending-history orphans, not even
// ex-Confirmed orphans that still carry non-NULL confirm_* columns (status,
// never column non-NULLness, keys effectiveness).
func TestT023BulkOrphanCandidateExclusion(t *testing.T) {
	pool := reorgTestPool(t)
	defer pool.Close()
	ctx := context.Background()
	const chainID, tip, n = int64(907231), uint64(30), uint64(4)
	t023BulkSeedBase(t, ctx, pool, chainID, 10, tip)

	// Three live pendings, all eligible (tip-h+1 >= 4).
	var wantBH []string
	for _, h := range []uint64{22, 23, 24} {
		bh, txHash := depositBlockHash(h), depositTxHash(h, 0)
		depositSeedObservation(t, ctx, pool, chainID, h, bh, txHash, 0, "1", 1)
		wantBH = append(wantBH, bh)
	}
	// Twelve pending-history orphans at heights 10..21 (inside the window).
	t023BulkSeedOrphans(t, ctx, pool, chainID, 12, "t023-bulk-r1", 0x0b010000)
	// One ex-Confirmed orphan: confirm h=27 on the live basis, then orphan —
	// confirm_* stays non-NULL while status is orphaned.
	exBH, exTx := depositBlockHash(27), depositTxHash(27, 0)
	depositSeedObservation(t, ctx, pool, chainID, 27, exBH, exTx, 0, "1", 1)
	c, sc, lease := t023BulkScanner(t, pool, chainID, n)
	_ = lease
	rcap := testRecoveryCap(t, ctx, pool, chainID)
	if err := c.ConfirmDepositUnit(ctx, lease, ConfirmBasis{
		BlockHash: exBH, TxHash: exTx, LogIndex: 0, Height: 27,
		TipNumber: tip, TipHash: depositBlockHash(tip), PolicySeq: 1, ThresholdN: n,
	}, rcap); err != nil {
		t.Fatalf("seed ex-confirmed confirm: %v", err)
	}
	t023BulkOrphan(t, ctx, pool, chainID, exBH, exTx, 0, "t023-bulk-r1")

	maxEligible, ok := MaxEligibleHeight(tip, n)
	if !ok || maxEligible != 27 {
		t.Fatalf("MaxEligibleHeight(%d,%d) = (%d,%v), want (27,true)", tip, n, maxEligible, ok)
	}
	batch, err := sc.readConfirmationCandidates(ctx, pool, maxEligible)
	if err != nil {
		t.Fatalf("readConfirmationCandidates(): %v", err)
	}
	if len(batch) != len(wantBH) {
		t.Fatalf("candidates = %d rows, want %d (pendings only)", len(batch), len(wantBH))
	}
	want := map[string]bool{}
	for _, bh := range wantBH {
		want[bh] = true
	}
	for _, cd := range batch {
		if !want[cd.blockHash] {
			t.Fatalf("candidate %s/%s selected, want only the 3 live pendings (orphans excluded)", cd.blockHash, cd.txHash)
		}
		delete(want, cd.blockHash)
	}
	if len(want) != 0 {
		t.Fatalf("candidates miss pendings %v", want)
	}
	if n := t023BulkStatusCount(t, ctx, pool, chainID, "pending"); n != 3 {
		t.Fatalf("pending rows = %d, want 3", n)
	}
	if n := t023BulkStatusCount(t, ctx, pool, chainID, "orphaned"); n != 13 {
		t.Fatalf("orphaned rows = %d, want 13 (12 bulk + 1 ex-confirmed)", n)
	}
	pending, err := sc.countConfirmationPending(ctx, pool)
	if err != nil {
		t.Fatalf("countConfirmationPending(): %v", err)
	}
	if pending != 3 {
		t.Fatalf("pending gauge = %d, want 3 (orphan history invisible to selection)", pending)
	}
}

// TestT023BulkOrphanSelectedThenOrphanedOneTickHalt: a candidate selected
// while pending but orphaned before commit halts exactly one tick via the
// commit-time re-read (ChainViewError-stop, never a pause write, zero
// writes), then clears — the next tick excludes it and the surviving
// pending confirms.
func TestT023BulkOrphanSelectedThenOrphanedOneTickHalt(t *testing.T) {
	pool := reorgTestPool(t)
	defer pool.Close()
	ctx := context.Background()
	const chainID, tip, n = int64(907232), uint64(30), uint64(4)
	t023BulkSeedBase(t, ctx, pool, chainID, 10, tip)

	pBH, pTx := depositBlockHash(20), depositTxHash(20, 0)
	depositSeedObservation(t, ctx, pool, chainID, 20, pBH, pTx, 0, "5", 1)
	qBH, qTx := depositBlockHash(21), depositTxHash(21, 0)
	depositSeedObservation(t, ctx, pool, chainID, 21, qBH, qTx, 0, "6", 1)

	c, sc, lease := t023BulkScanner(t, pool, chainID, n)
	rcap := testRecoveryCap(t, ctx, pool, chainID)
	maxEligible, ok := MaxEligibleHeight(tip, n)
	if !ok {
		t.Fatal("MaxEligibleHeight(30,4) not ok")
	}
	sel, err := sc.readConfirmationCandidates(ctx, pool, maxEligible)
	if err != nil {
		t.Fatalf("select candidates: %v", err)
	}
	if len(sel) != 2 {
		t.Fatalf("selected = %d rows, want 2 (P+Q pending)", len(sel))
	}
	// Invalidation lands between selection and commit: P goes orphaned.
	t023BulkOrphan(t, ctx, pool, chainID, pBH, pTx, 0, "t023-bulk-r2")

	pBasis := ConfirmBasis{
		BlockHash: pBH, TxHash: pTx, LogIndex: 0, Height: 20,
		TipNumber: tip, TipHash: depositBlockHash(tip), PolicySeq: 1, ThresholdN: n,
	}
	err = c.ConfirmDepositUnit(ctx, lease, pBasis, rcap)
	var chainView *ConfirmationChainViewError
	if !errors.As(err, &chainView) {
		t.Fatalf("stale-orphan confirm = %v, want ConfirmationChainViewError-stop", err)
	}
	// One-tick halt, stop-not-skip: the driver ends the whole tick.
	if outcome, reason := classifyConfirmationOutcome(err); outcome != confirmHalt || reason != "reference_unverifiable" {
		t.Fatalf("classify = (%d %q), want (confirmHalt reference_unverifiable)", outcome, reason)
	}
	// Zero writes on the halted tick: P orphaned under this round, Q still
	// pending, and even the bootstrap policy insert rolled back.
	if o := t023BulkStatusCount(t, ctx, pool, chainID, "orphaned"); o != 1 {
		t.Fatalf("orphaned rows = %d, want 1 (P only)", o)
	}
	if o := t023BulkStatusCount(t, ctx, pool, chainID, "pending"); o != 1 {
		t.Fatalf("pending rows = %d, want 1 (Q untouched)", o)
	}
	if n := depositCountRows(t, ctx, pool, "confirmation_policy_history", chainID); n != 0 {
		t.Fatalf("policy rows = %d after refused commit, want 0 (single-txn rollback)", n)
	}
	// Next tick clears: the orphan is gone from selection, Q confirms on the
	// live basis with zero stall.
	next, err := sc.readConfirmationCandidates(ctx, pool, maxEligible)
	if err != nil {
		t.Fatalf("re-read candidates: %v", err)
	}
	if len(next) != 1 || next[0].blockHash != qBH {
		t.Fatalf("next-tick candidates = %v, want exactly [Q]", next)
	}
	if err := c.ConfirmDepositUnit(ctx, lease, ConfirmBasis{
		BlockHash: qBH, TxHash: qTx, LogIndex: 0, Height: 21,
		TipNumber: tip, TipHash: depositBlockHash(tip), PolicySeq: 1, ThresholdN: n,
	}, rcap); err != nil {
		t.Fatalf("Q confirm after clear: %v", err)
	}
	status, nullAt, tipN, thr, seq, gotTip, conf := confirmReadBasis(t, ctx, pool, chainID, qBH, qTx)
	if status != "confirmed" || nullAt || tipN != int64(tip) || gotTip != depositBlockHash(tip) ||
		thr != int64(n) || seq != 1 || conf != "10" {
		t.Fatalf("Q basis = (%s null=%v tip %d %s N=%d conf=%s seq=%d), want confirmed tip(30 %s) N=4 conf=10 seq=1",
			status, nullAt, tipN, gotTip, thr, conf, seq, depositBlockHash(tip))
	}
	if o := t023BulkStatusCount(t, ctx, pool, chainID, "confirmed"); o != 1 {
		t.Fatalf("confirmed rows = %d, want 1 (Q only — the halted tick wrote nothing)", o)
	}
}

// TestT023BulkOrphanGenuineAnomalyStillHalts: with bulk orphan history
// present, a genuine chain-view anomaly (reference block de-canonicalized —
// missing/non-canonical under the lock) still halts the tick stop-not-skip
// with zero writes; selection itself stays status-only (the pending row is
// still selected — the stop happens at commit re-read, not by skipping).
func TestT023BulkOrphanGenuineAnomalyStillHalts(t *testing.T) {
	pool := reorgTestPool(t)
	defer pool.Close()
	ctx := context.Background()
	const chainID, tip, n = int64(907233), uint64(30), uint64(4)
	t023BulkSeedBase(t, ctx, pool, chainID, 10, tip)
	t023BulkSeedOrphans(t, ctx, pool, chainID, 10, "t023-bulk-r3", 0x0b030000)

	rBH, rTx := depositBlockHash(20), depositTxHash(20, 0)
	depositSeedObservation(t, ctx, pool, chainID, 20, rBH, rTx, 0, "7", 1)
	if _, err := pool.Exec(ctx, `UPDATE chain_blocks SET canonical = false WHERE chain_id = $1 AND number = 20`, chainID); err != nil {
		t.Fatalf("de-canonicalize reference: %v", err)
	}

	c, sc, lease := t023BulkScanner(t, pool, chainID, n)
	rcap := testRecoveryCap(t, ctx, pool, chainID)
	maxEligible, ok := MaxEligibleHeight(tip, n)
	if !ok {
		t.Fatal("MaxEligibleHeight(30,4) not ok")
	}
	sel, err := sc.readConfirmationCandidates(ctx, pool, maxEligible)
	if err != nil {
		t.Fatalf("select candidates: %v", err)
	}
	if len(sel) != 1 || sel[0].blockHash != rBH {
		t.Fatalf("selected = %v, want exactly [R] (selection is status-only, anomaly does not hide it)", sel)
	}
	err = c.ConfirmDepositUnit(ctx, lease, ConfirmBasis{
		BlockHash: rBH, TxHash: rTx, LogIndex: 0, Height: 20,
		TipNumber: tip, TipHash: depositBlockHash(tip), PolicySeq: 1, ThresholdN: n,
	}, rcap)
	var chainView *ConfirmationChainViewError
	if !errors.As(err, &chainView) {
		t.Fatalf("anomalous confirm = %v, want ConfirmationChainViewError-stop", err)
	}
	if outcome, reason := classifyConfirmationOutcome(err); outcome != confirmHalt || reason != "reference_unverifiable" {
		t.Fatalf("classify = (%d %q), want (confirmHalt reference_unverifiable)", outcome, reason)
	}
	// Zero writes: R still pending, policy untouched, all 10 orphans intact
	// under their round.
	if o := t023BulkStatusCount(t, ctx, pool, chainID, "pending"); o != 1 {
		t.Fatalf("pending rows = %d, want 1 (R, anomaly wrote nothing)", o)
	}
	if n := depositCountRows(t, ctx, pool, "confirmation_policy_history", chainID); n != 0 {
		t.Fatalf("policy rows = %d after refused commit, want 0", n)
	}
	var orphanIDKept int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM deposit_observations
WHERE chain_id = $1 AND status = 'orphaned' AND orphan_recovery_id = 't023-bulk-r3'`, chainID).Scan(&orphanIDKept); err != nil {
		t.Fatalf("count attributed orphans: %v", err)
	}
	if orphanIDKept != 10 {
		t.Fatalf("attributed orphans = %d, want 10 (history untouched by the halt)", orphanIDKept)
	}
}

// TestT023BulkOrphanNewPendingsConfirmNoStall: forty Orphaned history rows
// alongside five new Pendings — every eligible pending confirms in sequence
// on its exact live basis (bootstrap exactly once), history rows keep status
// + attribution, nothing pending remains. Bulk history causes zero
// structural stall.
func TestT023BulkOrphanNewPendingsConfirmNoStall(t *testing.T) {
	pool := reorgTestPool(t)
	defer pool.Close()
	ctx := context.Background()
	const chainID, tip, n = int64(907234), uint64(30), uint64(4)
	t023BulkSeedBase(t, ctx, pool, chainID, 10, tip)
	t023BulkSeedOrphans(t, ctx, pool, chainID, 40, "t023-bulk-r4", 0x0b040000)

	heights := []uint64{22, 23, 24, 25, 26}
	for _, h := range heights {
		depositSeedObservation(t, ctx, pool, chainID, h, depositBlockHash(h), depositTxHash(h, 0), 0, "2", 1)
	}

	c, sc, lease := t023BulkScanner(t, pool, chainID, n)
	rcap := testRecoveryCap(t, ctx, pool, chainID)
	maxEligible, ok := MaxEligibleHeight(tip, n)
	if !ok {
		t.Fatal("MaxEligibleHeight(30,4) not ok")
	}
	batch, err := sc.readConfirmationCandidates(ctx, pool, maxEligible)
	if err != nil {
		t.Fatalf("readConfirmationCandidates(): %v", err)
	}
	if len(batch) != len(heights) {
		t.Fatalf("candidates = %d rows, want %d (new pendings only, 40 orphans excluded)", len(batch), len(heights))
	}
	pendingSet := map[string]uint64{}
	for _, h := range heights {
		pendingSet[depositBlockHash(h)] = h
	}
	for _, cd := range batch {
		h, ok := pendingSet[cd.blockHash]
		if !ok {
			t.Fatalf("candidate %s/%s is not a new pending (orphan leaked into selection)", cd.blockHash, cd.txHash)
		}
		if err := c.ConfirmDepositUnit(ctx, lease, ConfirmBasis{
			BlockHash: cd.blockHash, TxHash: cd.txHash, LogIndex: cd.logIndex, Height: h,
			TipNumber: tip, TipHash: depositBlockHash(tip), PolicySeq: 1, ThresholdN: n,
		}, rcap); err != nil {
			t.Fatalf("confirm h=%d: %v (structural stall alongside orphan history)", h, err)
		}
		status, nullAt, tipN, thr, seq, gotTip, conf := confirmReadBasis(t, ctx, pool, chainID, cd.blockHash, cd.txHash)
		wantConf := fmt.Sprintf("%d", tip-h+1)
		if status != "confirmed" || nullAt || tipN != int64(tip) || gotTip != depositBlockHash(tip) ||
			thr != int64(n) || seq != 1 || conf != wantConf {
			t.Fatalf("h=%d basis = (%s null=%v tip %d %s N=%d conf=%s seq=%d), want confirmed tip(30 %s) N=4 conf=%s seq=1",
				h, status, nullAt, tipN, gotTip, thr, conf, seq, depositBlockHash(tip), wantConf)
		}
	}
	if o := t023BulkStatusCount(t, ctx, pool, chainID, "confirmed"); o != len(heights) {
		t.Fatalf("confirmed rows = %d, want %d", o, len(heights))
	}
	if o := t023BulkStatusCount(t, ctx, pool, chainID, "pending"); o != 0 {
		t.Fatalf("pending rows = %d, want 0 (all new pendings converted)", o)
	}
	var attributed int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM deposit_observations
WHERE chain_id = $1 AND status = 'orphaned' AND orphan_recovery_id = 't023-bulk-r4'`, chainID).Scan(&attributed); err != nil {
		t.Fatalf("count attributed orphans: %v", err)
	}
	if attributed != 40 {
		t.Fatalf("attributed orphans = %d, want 40 (history untouched)", attributed)
	}
	if n := depositCountRows(t, ctx, pool, "confirmation_policy_history", chainID); n != 1 {
		t.Fatalf("policy rows = %d, want 1 (single bootstrap across the bulk run)", n)
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

// --- Batch D: T025/T026/T027 (SC-04/SC-05) ---------------------------------
// ChainID reservation: 906001-906008 and 907231-907234 are taken above; the
// three tests below own 906009 (T025), 906010 (T026), 906011+906012 (T027).
// Every refusal asserts persistent state + audit + side effects, never a bare
// error.

// TestT025DualExecutorSingleAdvance: two executors race on the SAME position
// (one confirm-ancestor slot in the detected phase) with separate *Lease
// instances. The lease CAS admits exactly one holder; the holder advances the
// phase exactly once while the loser fails safe on the lease verdict. The
// loser then re-acquires after expiry, re-reads, and its retry is a clean
// phase refusal — never a second advance. SC-04 green.
func TestT025DualExecutorSingleAdvance(t *testing.T) {
	pool := reorgTestPool(t)
	defer pool.Close()
	ctx := context.Background()
	const chainID = int64(906009)

	leaseA := newTestLease(t, pool, chainID, "t025-exec-a", 3*time.Second, time.Second)
	leaseB := newTestLease(t, pool, chainID, "t025-exec-b", 3*time.Second, time.Second)
	reorgSeedChain(t, ctx, pool, chainID, 10, 15, 10)

	won, _, err := leaseA.Acquire(ctx)
	if err != nil || !won {
		t.Fatalf("executor A initial acquire = (%v, %v), want (true, nil)", won, err)
	}
	res := reorgEstablishOne(t, ctx, pool, leaseA, chainID, 15)
	owned := RecoveryCapture{Seq: res.Seq, Owned: &RecoveryOwned{RecoveryID: res.RecoveryID}}
	if n := reorgCount(t, ctx, pool, "reorg_recovery_events", chainID); n != 1 {
		t.Fatalf("event rows after establish = %d, want 1", n)
	}

	// Let A's lease lapse (DB-clock expiry, polled — never a blind sleep) so
	// the acquire race below starts from a free row.
	waitUntil(t, time.Now().Add(30*time.Second), "executor A lease expiry", func() bool {
		var expired bool
		if err := pool.QueryRow(ctx, `SELECT expires_at < now() FROM indexer_lease WHERE chain_id = $1`, chainID).Scan(&expired); err != nil {
			return false
		}
		return expired
	})

	// Race 1: both executors Acquire behind one barrier — the CAS admits
	// exactly one holder.
	type acquireOut struct {
		name string
		won  bool
		err  error
	}
	start := make(chan struct{})
	acqCh := make(chan acquireOut, 2)
	go func() {
		<-start
		won, _, err := leaseA.Acquire(ctx)
		acqCh <- acquireOut{name: "A", won: won, err: err}
	}()
	go func() {
		<-start
		won, _, err := leaseB.Acquire(ctx)
		acqCh <- acquireOut{name: "B", won: won, err: err}
	}()
	close(start)
	winners := map[string]bool{}
	for i := 0; i < 2; i++ {
		select {
		case o := <-acqCh:
			if o.err != nil {
				t.Fatalf("executor %s acquire: %v", o.name, o.err)
			}
			winners[o.name] = o.won
		case <-time.After(30 * time.Second):
			t.Fatal("acquire race timed out waiting for both executors")
		}
	}
	if (winners["A"] && winners["B"]) || (!winners["A"] && !winners["B"]) {
		t.Fatalf("acquire race winners = %v, want exactly one holder", winners)
	}
	holder, loser := leaseA, leaseB
	holderName, loserName := "A", "B"
	if winners["B"] {
		holder, loser = leaseB, leaseA
		holderName, loserName = "B", "A"
	}

	// Race 2: both executors attempt the SAME confirm-ancestor slot behind one
	// barrier. The holder advances; the dispossessed executor fails safe on
	// the lease verdict before touching recovery state.
	go2 := make(chan struct{})
	confirmCh := make(chan error, 2)
	go func() {
		<-go2
		confirmCh <- ConfirmRecoveryAncestor(ctx, pool, holder, chainID, owned, 14, depositBlockHash(14), "t025-race")
	}()
	go func() {
		<-go2
		confirmCh <- ConfirmRecoveryAncestor(ctx, pool, loser, chainID, owned, 14, depositBlockHash(14), "t025-race")
	}()
	close(go2)
	var confirmErrs []error
	for i := 0; i < 2; i++ {
		select {
		case err := <-confirmCh:
			confirmErrs = append(confirmErrs, err)
		case <-time.After(30 * time.Second):
			t.Fatal("confirm race timed out waiting for both executors")
		}
	}
	var nils, safe int
	for _, err := range confirmErrs {
		switch {
		case err == nil:
			nils++
		case isLeaseLost(err) || isRecoveryGate(err):
			safe++
		default:
			t.Fatalf("confirm race error = %v, want nil or lease/gate refusal", err)
		}
	}
	if nils != 1 || safe != 1 {
		t.Fatalf("confirm race = %d wins + %d safe losses, want exactly 1 + 1", nils, safe)
	}

	// Persistent effects of exactly one advance: phase moved once, exactly one
	// ancestor_confirmed audit row, domain untouched, transitions empty.
	var phase string
	if err := pool.QueryRow(ctx, `SELECT phase FROM reorg_recovery WHERE chain_id = $1`, chainID).Scan(&phase); err != nil {
		t.Fatalf("read phase: %v", err)
	}
	if phase != reorgPhaseAncestorConfirmed {
		t.Fatalf("phase = %q, want ancestor_confirmed (single advance)", phase)
	}
	var confirmedEvents int64
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM reorg_recovery_events WHERE chain_id = $1 AND event = 'ancestor_confirmed'`, chainID).Scan(&confirmedEvents); err != nil {
		t.Fatalf("count ancestor_confirmed events: %v", err)
	}
	if confirmedEvents != 1 {
		t.Fatalf("ancestor_confirmed events = %d, want 1 (never two advances)", confirmedEvents)
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

	// Loser re-reads after re-acquiring (holder's short lease lapses again)
	// and retries: clean phase refusal, zero new audit rows, phase unmoved.
	_ = holderName
	waitUntil(t, time.Now().Add(30*time.Second), "holder lease expiry", func() bool {
		var expired bool
		if err := pool.QueryRow(ctx, `SELECT expires_at < now() FROM indexer_lease WHERE chain_id = $1`, chainID).Scan(&expired); err != nil {
			return false
		}
		return expired
	})
	won, _, err = loser.Acquire(ctx)
	if err != nil || !won {
		t.Fatalf("loser %s re-acquire = (%v, %v), want (true, nil)", loserName, won, err)
	}
	fresh := testRecoveryCap(t, ctx, pool, chainID)
	if fresh.Seq != res.Seq {
		t.Fatalf("re-read version = %d, want active seq %d", fresh.Seq, res.Seq)
	}
	retryOwned := RecoveryCapture{Seq: res.Seq, Owned: &RecoveryOwned{RecoveryID: res.RecoveryID}}
	if err := ConfirmRecoveryAncestor(ctx, pool, loser, chainID, retryOwned, 14, depositBlockHash(14), "t025-retry"); !isRecoveryGate(err) {
		t.Fatalf("loser retry = %v, want clean phase refusal (never a second advance)", err)
	}
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM reorg_recovery_events WHERE chain_id = $1 AND event = 'ancestor_confirmed'`, chainID).Scan(&confirmedEvents); err != nil {
		t.Fatalf("recount ancestor_confirmed events: %v", err)
	}
	if confirmedEvents != 1 {
		t.Fatalf("ancestor_confirmed events after retry = %d, want 1", confirmedEvents)
	}
	if err := pool.QueryRow(ctx, `SELECT phase FROM reorg_recovery WHERE chain_id = $1`, chainID).Scan(&phase); err != nil {
		t.Fatalf("reread phase: %v", err)
	}
	if phase != reorgPhaseAncestorConfirmed {
		t.Fatalf("phase after retry = %q, want ancestor_confirmed (unmoved)", phase)
	}
}

// TestT026DemandedInterleavingRealRelease: capture v -> establish v+1 ->
// drive to complete_pending -> CompleteRecoveryVerify (REAL release, never a
// manual DELETE) -> a stale ordinary commit with fully matching content is
// REFUSED on version alone with zero writes and zero progress; the legal
// pre-pause-then-establish order stays allowed. SC-05 green.
func TestT026DemandedInterleavingRealRelease(t *testing.T) {
	pool := reorgTestPool(t)
	defer pool.Close()
	ctx := context.Background()
	const chainID = int64(906010)
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

	sc := &Scanner{pool: pool, chainID: chainID, cfg: Config{StartHeight: 10}, lease: lease}
	cap0 := testRecoveryCap(t, ctx, pool, chainID)

	// Legal pre-pause order: the ordinary commit lands before any recovery.
	if err := sc.commitBlock(ctx, blockWrite{number: 21, hash: depositBlockHash(21), parent: depositBlockHash(20)}, cap0); err != nil {
		t.Fatalf("pre-pause commit: %v", err)
	}
	var preHeight int64
	var preHash string
	if err := pool.QueryRow(ctx, `SELECT height, block_hash FROM indexer_checkpoint WHERE chain_id = $1`, chainID).Scan(&preHeight, &preHash); err != nil {
		t.Fatalf("read checkpoint after pre-pause commit: %v", err)
	}
	if preHeight != 21 || preHash != depositBlockHash(21) {
		t.Fatalf("checkpoint = (%d %s), want (21 %s)", preHeight, preHash, depositBlockHash(21))
	}

	// Establish: v -> v+1.
	res := reorgEstablishOne(t, ctx, pool, lease, chainID, 21)
	if res.Seq != cap0.Seq+1 {
		t.Fatalf("establish seq = %d, want cap0+1 = %d", res.Seq, cap0.Seq+1)
	}
	owned := RecoveryCapture{Seq: res.Seq, Owned: &RecoveryOwned{RecoveryID: res.RecoveryID}}

	// Drive the mini-loop shape (establish -> confirm-ancestor -> invalidate
	// -> rollback -> replay) to complete_pending. Sweep is [16,21]: the legal
	// block 21 is inside the fork and gets replaced by replay.
	if err := ConfirmRecoveryAncestor(ctx, pool, lease, chainID, owned, 15, depositBlockHash(15), "t026-loop"); err != nil {
		t.Fatalf("confirm ancestor: %v", err)
	}
	if _, _, _, err := InvalidateRecoveryBlocks(ctx, pool, lease, chainID, owned); err != nil {
		t.Fatalf("invalidate blocks: %v", err)
	}
	if _, err := InvalidateRecoveryObservations(ctx, pool, lease, chainID, owned); err != nil {
		t.Fatalf("invalidate observations: %v", err)
	}
	for _, stream := range []RecoveryStream{RecoveryStreamBlock, RecoveryStreamLog, RecoveryStreamDeposit} {
		if _, _, err := RollbackRecoveryCheckpoint(ctx, pool, lease, chainID, owned, stream); err != nil {
			t.Fatalf("rollback %s: %v", stream, err)
		}
	}
	newHash := func(n int64) string { return fmt.Sprintf("0x%064x", 0xe00e_0000+uint64(n)) }
	newParent := func(h int64) string {
		if h == 16 {
			return depositBlockHash(15)
		}
		return newHash(h - 1)
	}
	var blocks []ReplayBlock
	for h := int64(16); h <= 18; h++ {
		blocks = append(blocks, ReplayBlock{Number: h, Hash: newHash(h), ParentHash: newParent(h)})
	}
	if err := ReplayRecoveryRange(ctx, pool, lease, chainID, owned, RecoveryStreamBlock, 16, 18, blocks, nil, nil); err != nil {
		t.Fatalf("replay block [16,18]: %v", err)
	}
	var blocks2 []ReplayBlock
	for h := int64(19); h <= 21; h++ {
		blocks2 = append(blocks2, ReplayBlock{Number: h, Hash: newHash(h), ParentHash: newParent(h)})
	}
	if err := ReplayRecoveryRange(ctx, pool, lease, chainID, owned, RecoveryStreamBlock, 19, 21, blocks2, nil, nil); err != nil {
		t.Fatalf("replay block [19,21]: %v", err)
	}
	if err := ReplayRecoveryRange(ctx, pool, lease, chainID, owned, RecoveryStreamLog, 16, 16, nil, nil, nil); err != nil {
		t.Fatalf("replay log [16,16] empty: %v", err)
	}
	if err := ReplayRecoveryRange(ctx, pool, lease, chainID, owned, RecoveryStreamDeposit, 16, 16, nil, nil, nil); err != nil {
		t.Fatalf("replay deposit [16,16] empty: %v", err)
	}
	if err := ReplayRecoveryRange(ctx, pool, lease, chainID, owned, RecoveryStreamLog, 17, 21, nil, nil, nil); err != nil {
		t.Fatalf("replay log [17,21]: %v", err)
	}
	if err := ReplayRecoveryRange(ctx, pool, lease, chainID, owned, RecoveryStreamDeposit, 17, 21, nil, nil, nil); err != nil {
		t.Fatalf("replay deposit [17,21]: %v", err)
	}
	var phase string
	if err := pool.QueryRow(ctx, `SELECT phase FROM reorg_recovery WHERE chain_id = $1`, chainID).Scan(&phase); err != nil {
		t.Fatalf("read phase: %v", err)
	}
	if phase != reorgPhaseCompletePending {
		t.Fatalf("phase = %q after full replay, want complete_pending", phase)
	}

	// REAL release through CompleteRecoveryVerify (never a manual DELETE).
	if err := CompleteRecoveryVerify(ctx, pool, lease, chainID, owned); err != nil {
		t.Fatalf("complete (real release): %v", err)
	}
	if n := reorgCount(t, ctx, pool, "reorg_recovery", chainID); n != 0 {
		t.Fatalf("recovery rows = %d after release, want 0", n)
	}
	var terminal string
	if err := pool.QueryRow(ctx, `SELECT detail FROM reorg_recovery_events
WHERE chain_id = $1 AND event = 'auto_completed'`, chainID).Scan(&terminal); err != nil {
		t.Fatalf("read terminal event: %v", err)
	}
	for _, want := range []string{"swept=16-21", "version=1"} {
		if !strings.Contains(terminal, want) {
			t.Fatalf("terminal detail missing %q: %q", want, terminal)
		}
	}
	eventsBefore := reorgCount(t, ctx, pool, "reorg_recovery_events", chainID)
	transBefore := reorgCount(t, ctx, pool, "deposit_observation_transitions", chainID)
	var tipHeight int64
	var tipHash string
	if err := pool.QueryRow(ctx, `SELECT height, block_hash FROM indexer_checkpoint WHERE chain_id = $1`, chainID).Scan(&tipHeight, &tipHash); err != nil {
		t.Fatalf("read checkpoint after release: %v", err)
	}

	// The pre-round batch, fully content-matching (correct parent, next
	// height), still refuses on version alone — and writes nothing.
	if err := sc.commitBlock(ctx, blockWrite{number: uint64(tipHeight + 1), hash: depositBlockHash(22), parent: tipHash}, cap0); !isRecoveryGate(err) {
		t.Fatalf("post-release stale commit = %v, want version refusal", err)
	}
	var n22 int64
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM chain_blocks WHERE chain_id = $1 AND number = 22`, chainID).Scan(&n22); err != nil {
		t.Fatalf("count height 22: %v", err)
	}
	if n22 != 0 {
		t.Fatalf("height-22 rows = %d after refused commit, want 0 (zero writes)", n22)
	}
	var afterHeight int64
	var afterHash string
	if err := pool.QueryRow(ctx, `SELECT height, block_hash FROM indexer_checkpoint WHERE chain_id = $1`, chainID).Scan(&afterHeight, &afterHash); err != nil {
		t.Fatalf("reread checkpoint: %v", err)
	}
	if afterHeight != tipHeight || afterHash != tipHash {
		t.Fatalf("checkpoint moved to (%d %s), want still (%d %s) (zero progress)", afterHeight, afterHash, tipHeight, tipHash)
	}
	if n := reorgCount(t, ctx, pool, "reorg_recovery_events", chainID); n != eventsBefore {
		t.Fatalf("event rows = %d after refused commit, want %d (refusal writes nothing)", n, eventsBefore)
	}
	if n := reorgCount(t, ctx, pool, "deposit_observation_transitions", chainID); n != transBefore {
		t.Fatalf("transition rows = %d after refused commit, want %d", n, transBefore)
	}
	if n := reorgCount(t, ctx, pool, "reorg_recovery", chainID); n != 0 {
		t.Fatalf("recovery rows = %d after refused commit, want 0", n)
	}
}

// TestT027VersionRaces: post-establish old confirm commits succeed 0% with
// zero writes; post-policy-switch old-policy confirms succeed 0% with the
// policy version chain unmodified; every submit rolls back on re-read
// mismatch (observation row + policy history unchanged).
func TestT027VersionRaces(t *testing.T) {
	pool := reorgTestPool(t)
	defer pool.Close()
	ctx := context.Background()

	// Half 1: a confirm captured BEFORE establish refuses AFTER establish on
	// version alone (recovery gate fires before any candidate/policy write).
	const chainA, tip, n = int64(906011), uint64(30), uint64(4)
	t023BulkSeedBase(t, ctx, pool, chainA, 10, tip)
	aBH, aTx := depositBlockHash(20), depositTxHash(20, 0)
	depositSeedObservation(t, ctx, pool, chainA, 20, aBH, aTx, 0, "7", 1)
	cA, _, leaseA := t023BulkScanner(t, pool, chainA, n)
	capOld := testRecoveryCap(t, ctx, pool, chainA)
	resA := reorgEstablishOne(t, ctx, pool, leaseA, chainA, 30)
	_ = resA
	oldBasis := ConfirmBasis{
		BlockHash: aBH, TxHash: aTx, LogIndex: 0, Height: 20,
		TipNumber: tip, TipHash: depositBlockHash(tip), PolicySeq: 1, ThresholdN: n,
	}
	if err := cA.ConfirmDepositUnit(ctx, leaseA, oldBasis, capOld); !isRecoveryGate(err) {
		t.Fatalf("post-establish old confirm = %v, want version (gate) refusal", err)
	}
	status, nullAt, tipN, thr, seq, gotTip, conf := confirmReadBasis(t, ctx, pool, chainA, aBH, aTx)
	if status != "pending" || !nullAt || tipN != -1 || thr != -1 || seq != -1 || gotTip != "" || conf != "" {
		t.Fatalf("half-1 zero-write violated: status=%s nullAt=%v tip=%d/%q N=%d conf=%q seq=%d",
			status, nullAt, tipN, gotTip, thr, conf, seq)
	}
	if k := depositCountRows(t, ctx, pool, "confirmation_policy_history", chainA); k != 0 {
		t.Fatalf("half-1 policy rows = %d, want 0 (refusal wrote nothing)", k)
	}
	if k := depositCountRows(t, ctx, pool, "deposit_observation_transitions", chainA); k != 0 {
		t.Fatalf("half-1 transition rows = %d, want 0", k)
	}
	if k := reorgCount(t, ctx, pool, "reorg_recovery", chainA); k != 1 {
		t.Fatalf("half-1 recovery rows = %d, want 1 (refusal moved nothing)", k)
	}

	// Half 2: after an authorized policy switch, a confirm computed under the
	// OLD policy seq refuses on policy identity with the version chain
	// byte-identical to its pre-submit shape.
	const chainB = int64(906012)
	t023BulkSeedBase(t, ctx, pool, chainB, 10, tip)
	bBH, bTx := depositBlockHash(21), depositTxHash(21, 0)
	depositSeedObservation(t, ctx, pool, chainB, 21, bBH, bTx, 0, "8", 1)
	cBH, cTx := depositBlockHash(22), depositTxHash(22, 0)
	depositSeedObservation(t, ctx, pool, chainB, 22, cBH, cTx, 0, "9", 1)
	cB, _, leaseB := t023BulkScanner(t, pool, chainB, n)
	rcapB := testRecoveryCap(t, ctx, pool, chainB)
	if err := cB.ConfirmDepositUnit(ctx, leaseB, ConfirmBasis{
		BlockHash: bBH, TxHash: bTx, LogIndex: 0, Height: 21,
		TipNumber: tip, TipHash: depositBlockHash(tip), PolicySeq: 1, ThresholdN: n,
	}, rcapB); err != nil {
		t.Fatalf("bootstrap confirm h=21: %v", err)
	}
	swRes, err := AuthorizeConfirmationPolicy(ctx, pool, ConfirmAuthRequest{
		ChainID: chainB, RequestID: "t027-sw-half2", ExpectedOldSeq: 1,
		NewThresholdRaw: "20", Operator: "op-t027", Reason: "t027-version-race",
	})
	if err != nil {
		t.Fatalf("AuthorizeConfirmationPolicy(): %v", err)
	}
	if swRes.PolicySeq != 2 || swRes.Threshold != 20 || swRes.Recorded {
		t.Fatalf("switch result = %+v, want {PolicySeq:2 Threshold:20 Recorded:false}", swRes)
	}
	type policyRow struct {
		seq, threshold int64
	}
	readPolicy := func() []policyRow {
		rows, err := pool.Query(ctx, `SELECT policy_seq, threshold FROM confirmation_policy_history
WHERE chain_id = $1 ORDER BY policy_seq`, chainB)
		if err != nil {
			t.Fatalf("read policy history: %v", err)
		}
		defer rows.Close()
		var out []policyRow
		for rows.Next() {
			var r policyRow
			if err := rows.Scan(&r.seq, &r.threshold); err != nil {
				t.Fatalf("scan policy row: %v", err)
			}
			out = append(out, r)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("policy rows: %v", err)
		}
		return out
	}
	before := readPolicy()
	if len(before) != 2 || before[0] != (policyRow{1, 4}) || before[1] != (policyRow{2, 20}) {
		t.Fatalf("policy chain pre-submit = %v, want [{1 4} {2 20}]", before)
	}
	staleBasis := ConfirmBasis{
		BlockHash: cBH, TxHash: cTx, LogIndex: 0, Height: 22,
		TipNumber: tip, TipHash: depositBlockHash(tip), PolicySeq: 1, ThresholdN: n,
	}
	err = cB.ConfirmDepositUnit(ctx, leaseB, staleBasis, rcapB)
	var drift *ConfirmationDriftError
	if !errors.As(err, &drift) {
		t.Fatalf("old-policy confirm = %v, want ConfirmationDriftError refusal", err)
	}
	if after := readPolicy(); len(after) != len(before) || after[0] != before[0] || after[1] != before[1] {
		t.Fatalf("policy chain post-submit = %v, want unchanged %v", after, before)
	}
	status, nullAt, tipN, thr, seq, gotTip, conf = confirmReadBasis(t, ctx, pool, chainB, cBH, cTx)
	if status != "pending" || !nullAt || tipN != -1 || thr != -1 || seq != -1 || gotTip != "" || conf != "" {
		t.Fatalf("half-2 zero-write violated: status=%s nullAt=%v tip=%d/%q N=%d conf=%q seq=%d",
			status, nullAt, tipN, gotTip, thr, conf, seq)
	}
	if k := t023BulkStatusCount(t, ctx, pool, chainB, "confirmed"); k != 1 {
		t.Fatalf("half-2 confirmed rows = %d, want 1 (h=21 only — stale submit converted nothing)", k)
	}
}

// --- Batch D: T030/T031 (manual two-step auth; pause coexistence + tamper) -
// ChainID reservation: 906001-906012 taken above; the two tests below own
// 906013 (T030 early-reconcile refusal demo), 906014 (T030 full manual
// success), 906015 (T031). Every refusal asserts persistent state + audit,
// never a bare error.

// reorgAuditSnap freezes the persistent tables any refusal must leave
// untouched (recovery row + audit + transitions + observations + all three
// pause tables).
type reorgAuditSnap struct {
	rec, ev, trans, obs, dpause, lpause, ipause int64
}

func reorgSnapAudit(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64) reorgAuditSnap {
	t.Helper()
	return reorgAuditSnap{
		rec:    reorgCount(t, ctx, pool, "reorg_recovery", chainID),
		ev:     reorgCount(t, ctx, pool, "reorg_recovery_events", chainID),
		trans:  reorgCount(t, ctx, pool, "deposit_observation_transitions", chainID),
		obs:    reorgCount(t, ctx, pool, "deposit_observations", chainID),
		dpause: reorgCount(t, ctx, pool, "deposit_pause", chainID),
		lpause: reorgCount(t, ctx, pool, "log_pause", chainID),
		ipause: reorgCount(t, ctx, pool, "indexer_pause", chainID),
	}
}

func (s reorgAuditSnap) assertEqual(t *testing.T, got reorgAuditSnap, msg string) {
	t.Helper()
	if s != got {
		t.Fatalf("%s: audit drift:\nbefore=%+v\n after=%+v (refusal must write nothing)", msg, s, got)
	}
}

// t030SeedLoopBase plants the mini-loop base: canonical 10..20, history,
// deposit_checkpoint, one affected (17) + one keeper (14) observation.
func t030SeedLoopBase(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64) {
	t.Helper()
	reorgSeedChain(t, ctx, pool, chainID, 10, 20, 10)
	depositSeedHistory(t, ctx, pool, chainID, 1, 10, strings.Repeat("aa", 32))
	if _, err := pool.Exec(ctx, `
INSERT INTO deposit_checkpoint (chain_id, start_block, config_hash, next_block)
VALUES ($1, 10, $2, 20)`, chainID, strings.Repeat("aa", 32)); err != nil {
		t.Fatalf("seed deposit_checkpoint: %v", err)
	}
	depositSeedObservation(t, ctx, pool, chainID, 17, depositBlockHash(17), depositTxHash(17, 0), 0, "50", 1)
	depositSeedObservation(t, ctx, pool, chainID, 14, depositBlockHash(14), depositTxHash(14, 0), 0, "7", 1)
}

// t030ReplayToComplete runs confirm-ancestor(15) -> invalidate -> rollback ->
// full replay [16,20] on an established instance and demands complete_pending
// (same sweep shape as TestReorgInvalidateRollbackReplayComplete).
func t030ReplayToComplete(t *testing.T, ctx context.Context, pool *pgxpool.Pool, lease *Lease, chainID int64, owned RecoveryCapture) {
	t.Helper()
	if err := ConfirmRecoveryAncestor(ctx, pool, lease, chainID, owned, 15, depositBlockHash(15), "t030-loop"); err != nil {
		t.Fatalf("confirm ancestor: %v", err)
	}
	if _, _, _, err := InvalidateRecoveryBlocks(ctx, pool, lease, chainID, owned); err != nil {
		t.Fatalf("invalidate blocks: %v", err)
	}
	if _, err := InvalidateRecoveryObservations(ctx, pool, lease, chainID, owned); err != nil {
		t.Fatalf("invalidate observations: %v", err)
	}
	for _, stream := range []RecoveryStream{RecoveryStreamBlock, RecoveryStreamLog, RecoveryStreamDeposit} {
		if _, _, err := RollbackRecoveryCheckpoint(ctx, pool, lease, chainID, owned, stream); err != nil {
			t.Fatalf("rollback %s: %v", stream, err)
		}
	}
	newHash := func(n int64) string { return fmt.Sprintf("0x%064x", 0xe00e_0000+uint64(n)) }
	newParent := func(h int64) string {
		if h == 16 {
			return depositBlockHash(15)
		}
		return newHash(h - 1)
	}
	var blocks []ReplayBlock
	for h := int64(16); h <= 18; h++ {
		blocks = append(blocks, ReplayBlock{Number: h, Hash: newHash(h), ParentHash: newParent(h)})
	}
	if err := ReplayRecoveryRange(ctx, pool, lease, chainID, owned, RecoveryStreamBlock, 16, 18, blocks, nil, nil); err != nil {
		t.Fatalf("replay block [16,18]: %v", err)
	}
	var blocks2 []ReplayBlock
	for h := int64(19); h <= 20; h++ {
		blocks2 = append(blocks2, ReplayBlock{Number: h, Hash: newHash(h), ParentHash: newParent(h)})
	}
	if err := ReplayRecoveryRange(ctx, pool, lease, chainID, owned, RecoveryStreamBlock, 19, 20, blocks2, nil, nil); err != nil {
		t.Fatalf("replay block [19,20]: %v", err)
	}
	if err := ReplayRecoveryRange(ctx, pool, lease, chainID, owned, RecoveryStreamLog, 16, 16, nil, nil, nil); err != nil {
		t.Fatalf("replay log [16,16]: %v", err)
	}
	if err := ReplayRecoveryRange(ctx, pool, lease, chainID, owned, RecoveryStreamDeposit, 16, 16, nil, nil, nil); err != nil {
		t.Fatalf("replay deposit [16,16]: %v", err)
	}
	if err := ReplayRecoveryRange(ctx, pool, lease, chainID, owned, RecoveryStreamLog, 17, 20, nil, nil, nil); err != nil {
		t.Fatalf("replay log [17,20]: %v", err)
	}
	if err := ReplayRecoveryRange(ctx, pool, lease, chainID, owned, RecoveryStreamDeposit, 17, 20, nil, nil, nil); err != nil {
		t.Fatalf("replay deposit [17,20]: %v", err)
	}
	var phase string
	if err := pool.QueryRow(ctx, `SELECT phase FROM reorg_recovery WHERE chain_id = $1`, chainID).Scan(&phase); err != nil {
		t.Fatalf("read phase: %v", err)
	}
	if phase != reorgPhaseCompletePending {
		t.Fatalf("phase = %q after full replay, want complete_pending", phase)
	}
}

// t030RefusedBlockCommit attempts the ordinary block commit with a fresh cap
// and demands the recovery-gate refusal with zero progress and zero writes.
func t030RefusedBlockCommit(t *testing.T, ctx context.Context, pool *pgxpool.Pool, lease *Lease, chainID int64) {
	t.Helper()
	sc := &Scanner{pool: pool, chainID: chainID, cfg: Config{StartHeight: 10}, lease: lease}
	capFresh := testRecoveryCap(t, ctx, pool, chainID)
	before := reorgSnapAudit(t, ctx, pool, chainID)
	var hBefore int64
	var bhBefore string
	if err := pool.QueryRow(ctx, `SELECT height, block_hash FROM indexer_checkpoint WHERE chain_id = $1`, chainID).Scan(&hBefore, &bhBefore); err != nil {
		t.Fatalf("read checkpoint: %v", err)
	}
	if err := sc.commitBlock(ctx, blockWrite{number: 21, hash: depositBlockHash(21), parent: depositBlockHash(20)}, capFresh); !isRecoveryGate(err) {
		t.Fatalf("ordinary commit during active recovery = %v, want recovery-gate refusal", err)
	}
	before.assertEqual(t, reorgSnapAudit(t, ctx, pool, chainID), "ordinary-commit refusal")
	var n21 int64
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM chain_blocks WHERE chain_id = $1 AND number = 21`, chainID).Scan(&n21); err != nil {
		t.Fatalf("count height 21: %v", err)
	}
	if n21 != 0 {
		t.Fatalf("height-21 rows = %d after refused commit, want 0", n21)
	}
	var hAfter int64
	var bhAfter string
	if err := pool.QueryRow(ctx, `SELECT height, block_hash FROM indexer_checkpoint WHERE chain_id = $1`, chainID).Scan(&hAfter, &bhAfter); err != nil {
		t.Fatalf("reread checkpoint: %v", err)
	}
	if hAfter != hBefore || bhAfter != bhBefore {
		t.Fatalf("checkpoint moved to (%d %s), want still (%d %s)", hAfter, bhAfter, hBefore, bhBefore)
	}
}

// TestT030ManualTwoStepAuth pins the Q2b manual path on real PG: repair
// records without releasing (row + phase untouched, ordinary commits still
// refused); release before completion-ready refuses with zero writes; the
// full drive + reconcile + repair + release round succeeds atomically with
// operator/time/evidence/cause in the terminal event; forged attempts
// (empty operator/evidence/disposition, wrong phase, no active row) refuse
// 100% with zero writes; the path invents no payment intents (every
// non-recovery table frozen).
func TestT030ManualTwoStepAuth(t *testing.T) {
	pool := reorgTestPool(t)
	defer pool.Close()
	ctx := context.Background()

	// Half 1 (906013): reconcile with NO completion work — repair records
	// without releasing, release refuses on the unmet completion判据.
	const chainR = int64(906013)
	leaseR := depositITLease(t, pool, chainR)
	reorgSeedChain(t, ctx, pool, chainR, 10, 20, 10)
	resR := reorgEstablishOne(t, ctx, pool, leaseR, chainR, 20)
	ownedR := RecoveryCapture{Seq: resR.Seq, Owned: &RecoveryOwned{RecoveryID: resR.RecoveryID}}
	if err := SignalRecoveryReconcile(ctx, pool, leaseR, chainR, ownedR,
		"over-deep", "1-20", "tip-diverged-beyond-depth-25"); err != nil {
		t.Fatalf("signal reconcile: %v", err)
	}
	var phaseR string
	if err := pool.QueryRow(ctx, `SELECT phase FROM reorg_recovery WHERE chain_id = $1`, chainR).Scan(&phaseR); err != nil {
		t.Fatalf("read phase: %v", err)
	}
	if phaseR != reorgPhaseReconcileRequired {
		t.Fatalf("phase = %q, want reconcile_required", phaseR)
	}
	const opR = "op-t030-repair"
	evR := fmt.Sprintf("old_tip=20:%s new_tip=21:%s searched=1-20 cause=over-deep",
		depositBlockHash(20), depositBlockHash(120))
	dispR := "pending=none action=awaiting-manual-repair"
	if err := AuthorizeRecoveryRepair(ctx, pool, chainR, opR, evR, dispR); err != nil {
		t.Fatalf("repair: %v", err)
	}
	var repairDetail string
	if err := pool.QueryRow(ctx, `SELECT detail FROM reorg_recovery_events
WHERE chain_id = $1 AND event = 'repair_authorized'`, chainR).Scan(&repairDetail); err != nil {
		t.Fatalf("read repair_authorized: %v", err)
	}
	for _, want := range []string{opR, evR, dispR} {
		if !strings.Contains(repairDetail, want) {
			t.Fatalf("repair detail missing %q: %q", want, repairDetail)
		}
	}
	// established + reconcile_signaled + exactly one repair_authorized; the
	// row stays in reconcile_required — confirm/sign/broadcast never released.
	if n := reorgCount(t, ctx, pool, "reorg_recovery_events", chainR); n != 3 {
		t.Fatalf("event rows = %d after repair, want 3 (repair writes exactly one)", n)
	}
	if err := pool.QueryRow(ctx, `SELECT phase FROM reorg_recovery WHERE chain_id = $1`, chainR).Scan(&phaseR); err != nil {
		t.Fatalf("read phase: %v", err)
	}
	if phaseR != reorgPhaseReconcileRequired {
		t.Fatalf("phase = %q after repair, want still reconcile_required (repair never releases)", phaseR)
	}
	if n := reorgCount(t, ctx, pool, "reorg_recovery", chainR); n != 1 {
		t.Fatalf("recovery rows = %d after repair, want 1 (row stays)", n)
	}
	t030RefusedBlockCommit(t, ctx, pool, leaseR, chainR)

	// Release BEFORE completion-ready refuses (no ancestor: completion判据
	// unmet) with zero writes and no terminal event.
	snapR := reorgSnapAudit(t, ctx, pool, chainR)
	if err := AuthorizeRecoveryRelease(ctx, pool, chainR, opR, evR, dispR); !isRecoveryGate(err) {
		t.Fatalf("early release = %v, want completion-gate refusal", err)
	}
	snapR.assertEqual(t, reorgSnapAudit(t, ctx, pool, chainR), "early-release refusal")
	var nRel int64
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM reorg_recovery_events
WHERE chain_id = $1 AND event = 'released'`, chainR).Scan(&nRel); err != nil {
		t.Fatalf("count released events: %v", err)
	}
	if nRel != 0 {
		t.Fatalf("released events = %d after refused release, want 0", nRel)
	}

	// Forged step-1/step-2 attempts: empty operator/evidence/disposition are
	// policy refusals with zero writes — 100% refused.
	for _, tc := range []struct{ name, op, ev, disp string }{
		{"repair-empty-operator", "", evR, dispR},
		{"repair-empty-evidence", opR, "", dispR},
		{"repair-empty-disposition", opR, evR, ""},
	} {
		snap := reorgSnapAudit(t, ctx, pool, chainR)
		if err := AuthorizeRecoveryRepair(ctx, pool, chainR, tc.op, tc.ev, tc.disp); !errors.Is(err, ErrReorgPolicyRejected) {
			t.Fatalf("%s = %v, want policy refusal", tc.name, err)
		}
		snap.assertEqual(t, reorgSnapAudit(t, ctx, pool, chainR), tc.name)
	}
	for _, tc := range []struct{ name, op, ev, disp string }{
		{"release-empty-operator", "", evR, dispR},
		{"release-empty-evidence", opR, "", dispR},
		{"release-empty-disposition", opR, evR, ""},
	} {
		snap := reorgSnapAudit(t, ctx, pool, chainR)
		if err := AuthorizeRecoveryRelease(ctx, pool, chainR, tc.op, tc.ev, tc.disp); !errors.Is(err, ErrReorgPolicyRejected) {
			t.Fatalf("%s = %v, want policy refusal", tc.name, err)
		}
		snap.assertEqual(t, reorgSnapAudit(t, ctx, pool, chainR), tc.name)
	}

	// Half 2 (906014): the full manual success round with one INDEPENDENT
	// stream pause that the release must not touch.
	const chainS = int64(906014)
	leaseS := depositITLease(t, pool, chainS)
	t030SeedLoopBase(t, ctx, pool, chainS)
	if _, err := pool.Exec(ctx, `
INSERT INTO deposit_pause (chain_id, height, kind, detail)
VALUES ($1, 11, 'chain_view_changed', 't030-independent-survivor')`, chainS); err != nil {
		t.Fatalf("seed deposit_pause: %v", err)
	}
	resS := reorgEstablishOne(t, ctx, pool, leaseS, chainS, 20)
	ownedS := RecoveryCapture{Seq: resS.Seq, Owned: &RecoveryOwned{RecoveryID: resS.RecoveryID}}
	const opS = "op-t030-release"
	evS := fmt.Sprintf("old_tip=20:%s new_tip=21:%s searched=1-20 cause=over-deep repaired-by=%s",
		depositBlockHash(20), depositBlockHash(120), opR)
	dispS := "orphaned=1 revived=0 replayed=block:20,log:20,deposit:20"
	// Manual auth in the wrong phase (detected) refuses before any write.
	snapW := reorgSnapAudit(t, ctx, pool, chainS)
	if err := AuthorizeRecoveryRepair(ctx, pool, chainS, opS, evS, dispS); !isRecoveryGate(err) {
		t.Fatalf("repair in detected phase = %v, want gate refusal", err)
	}
	snapW.assertEqual(t, reorgSnapAudit(t, ctx, pool, chainS), "wrong-phase repair")
	if err := AuthorizeRecoveryRelease(ctx, pool, chainS, opS, evS, dispS); !isRecoveryGate(err) {
		t.Fatalf("release in detected phase = %v, want gate refusal", err)
	}
	snapW.assertEqual(t, reorgSnapAudit(t, ctx, pool, chainS), "wrong-phase release")

	t030ReplayToComplete(t, ctx, pool, leaseS, chainS, ownedS)
	if err := SignalRecoveryReconcile(ctx, pool, leaseS, chainS, ownedS,
		"over-deep", "1-20", "tip-diverged-beyond-depth-25"); err != nil {
		t.Fatalf("signal reconcile: %v", err)
	}
	var phaseS string
	if err := pool.QueryRow(ctx, `SELECT phase FROM reorg_recovery WHERE chain_id = $1`, chainS).Scan(&phaseS); err != nil {
		t.Fatalf("read phase: %v", err)
	}
	if phaseS != reorgPhaseReconcileRequired {
		t.Fatalf("phase = %q, want reconcile_required", phaseS)
	}
	if err := AuthorizeRecoveryRepair(ctx, pool, chainS, opS, evS, dispS); err != nil {
		t.Fatalf("repair: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT phase FROM reorg_recovery WHERE chain_id = $1`, chainS).Scan(&phaseS); err != nil {
		t.Fatalf("read phase: %v", err)
	}
	if phaseS != reorgPhaseReconcileRequired {
		t.Fatalf("phase = %q after repair, want still reconcile_required", phaseS)
	}
	t030RefusedBlockCommit(t, ctx, pool, leaseS, chainS)

	// Release with re-verified completion: atomic-or-nothing (row gone AND
	// terminal event present), operator/time/evidence/cause recorded, the
	// independent pause row untouched, nothing else invented.
	snapS := reorgSnapAudit(t, ctx, pool, chainS)
	if err := AuthorizeRecoveryRelease(ctx, pool, chainS, opS, evS, dispS); err != nil {
		t.Fatalf("release: %v", err)
	}
	if n := reorgCount(t, ctx, pool, "reorg_recovery", chainS); n != 0 {
		t.Fatalf("recovery rows = %d after release, want 0", n)
	}
	var terminal string
	var at time.Time
	if err := pool.QueryRow(ctx, `SELECT detail, at FROM reorg_recovery_events
WHERE chain_id = $1 AND event = 'released'`, chainS).Scan(&terminal, &at); err != nil {
		t.Fatalf("read terminal event: %v", err)
	}
	for _, want := range []string{"event=released", opS, evS, dispS, "surviving_pauses=deposit_pause", "swept=16-20", "orphaned=1", "version=1"} {
		if !strings.Contains(terminal, want) {
			t.Fatalf("terminal detail missing %q: %q", want, terminal)
		}
	}
	if at.IsZero() {
		t.Fatalf("terminal event time is zero (operator/time must be recorded)")
	}
	var ph int64
	var pkind, pdetail string
	if err := pool.QueryRow(ctx, `SELECT height, kind, detail FROM deposit_pause WHERE chain_id = $1`, chainS).Scan(&ph, &pkind, &pdetail); err != nil {
		t.Fatalf("read surviving deposit_pause: %v", err)
	}
	if ph != 11 || pkind != "chain_view_changed" || pdetail != "t030-independent-survivor" {
		t.Fatalf("survivor = (%d %s %s), want (11 chain_view_changed t030-independent-survivor)", ph, pkind, pdetail)
	}
	got := reorgSnapAudit(t, ctx, pool, chainS)
	if got.rec != 0 || got.ev != snapS.ev+1 || got.trans != snapS.trans || got.obs != snapS.obs ||
		got.dpause != 1 || got.lpause != 0 || got.ipause != 0 {
		t.Fatalf("post-release audit = %+v, want rec=0 ev=%d trans=%d obs=%d pauses=(1,0,0)",
			got, snapS.ev+1, snapS.trans, snapS.obs)
	}
	// No payment-intent invention (011 owns that design): the manual path
	// writes no pause audit, no policy history, no observation side effects.
	if n := reorgCount(t, ctx, pool, "deposit_pause_audit", chainS); n != 0 {
		t.Fatalf("deposit_pause_audit rows = %d, want 0 (release invents nothing)", n)
	}
	if n := reorgCount(t, ctx, pool, "confirmation_policy_history", chainS); n != 0 {
		t.Fatalf("policy rows = %d, want 0 (release invents nothing)", n)
	}

	// Stateless reboot: the same release re-verifies from rebuilt DB state —
	// no row, same refusal verdict, zero writes.
	snapPost := reorgSnapAudit(t, ctx, pool, chainS)
	if err := AuthorizeRecoveryRelease(ctx, pool, chainS, opS, evS, dispS); !isRecoveryGate(err) {
		t.Fatalf("second release = %v, want no-row gate refusal", err)
	}
	snapPost.assertEqual(t, reorgSnapAudit(t, ctx, pool, chainS), "second-release refusal")
	if _, active, err := captureRecoveryVersion(ctx, pool, chainS); err != nil || active {
		t.Fatalf("rebuilt capture = (active=%v, err=%v), want inactive with no error", active, err)
	}

	// The surviving pause still stops ordinary confirmation work (zero writes).
	depositSeedObservation(t, ctx, pool, chainS, 14, depositBlockHash(14), depositTxHash(14, 0), 1, "7", 1)
	confirmSeedPolicyRow(t, ctx, pool, chainS, 1, 4, nil, "bootstrap", nil)
	cS, err := NewConfirmationCommitter(pool, ConfirmationConfig{ChainID: chainS, ThresholdN: 4})
	if err != nil {
		t.Fatalf("NewConfirmationCommitter(): %v", err)
	}
	rcapS := testRecoveryCap(t, ctx, pool, chainS)
	snapC := reorgSnapAudit(t, ctx, pool, chainS)
	cerr := cS.ConfirmDepositUnit(ctx, leaseS, ConfirmBasis{
		BlockHash: depositBlockHash(14), TxHash: depositTxHash(14, 0), LogIndex: 1, Height: 14,
		TipNumber: 20, TipHash: depositBlockHash(20), PolicySeq: 1, ThresholdN: 4,
	}, rcapS)
	var paused *streamPauseError
	if !errors.As(cerr, &paused) || paused.stream != "deposit_pause" {
		t.Fatalf("post-release confirm = %v, want the deposit_pause stop", cerr)
	}
	snapC.assertEqual(t, reorgSnapAudit(t, ctx, pool, chainS), "surviving-pause stop")
	var st string
	if err := pool.QueryRow(ctx, `SELECT status FROM deposit_observations
WHERE chain_id = $1 AND block_hash = $2 AND tx_hash = $3 AND log_index = 1`,
		chainS, depositBlockHash(14), depositTxHash(14, 0)).Scan(&st); err != nil {
		t.Fatalf("read fresh observation: %v", err)
	}
	if st != "pending" {
		t.Fatalf("fresh observation status = %q, want pending (zero writes)", st)
	}
}

// TestT031PauseCoexistenceAndTamper pins T031 on real PG: the release deletes
// ONLY the recovery row — independent pauses in separate tables survive with
// byte-identical content, ride the terminal event, and keep ordinary work
// stopped; a fresh stream pause racing the release is freed for the recovery
// cause only (its row intact); deleting a pause row under an ACTIVE recovery
// row cannot smuggle ordinary commits past the recovery-row gate.
func TestT031PauseCoexistenceAndTamper(t *testing.T) {
	pool := reorgTestPool(t)
	defer pool.Close()
	ctx := context.Background()
	const chainID = int64(906015)
	lease := depositITLease(t, pool, chainID)
	t030SeedLoopBase(t, ctx, pool, chainID)
	// Two INDEPENDENT pauses in separate tables: the deposit-stream survivor
	// and the indexer_pause tamper victim.
	if _, err := pool.Exec(ctx, `
INSERT INTO deposit_pause (chain_id, height, kind, detail)
VALUES ($1, 11, 'chain_view_changed', 't031-independent-survivor')`, chainID); err != nil {
		t.Fatalf("seed deposit_pause: %v", err)
	}
	forkB := fmt.Sprintf("0x%064x", 0xb00b_0000+11)
	if _, err := pool.Exec(ctx, `
INSERT INTO indexer_pause (chain_id, height, expected_hash, actual_hash, kind, detail)
VALUES ($1, 11, $2, $3, 'hash_mismatch', 't031-tamper-victim')`,
		chainID, depositBlockHash(11), forkB); err != nil {
		t.Fatalf("seed indexer_pause: %v", err)
	}
	res := reorgEstablishOne(t, ctx, pool, lease, chainID, 20)
	owned := RecoveryCapture{Seq: res.Seq, Owned: &RecoveryOwned{RecoveryID: res.RecoveryID}}
	// Coexistence starts at establish: both causes ride the established event
	// (readStreamPausesTx order: indexer, log, deposit) with no overwrite.
	var estDetail string
	if err := pool.QueryRow(ctx, `SELECT detail FROM reorg_recovery_events
WHERE chain_id = $1 AND event = 'established'`, chainID).Scan(&estDetail); err != nil {
		t.Fatalf("read established event: %v", err)
	}
	if !strings.Contains(estDetail, "pre_pauses=indexer_pause,deposit_pause") {
		t.Fatalf("established detail missing coexistence evidence: %q", estDetail)
	}
	if n := reorgCount(t, ctx, pool, "deposit_pause", chainID); n != 1 {
		t.Fatalf("deposit_pause rows after establish = %d, want 1 (no overwrite)", n)
	}
	if n := reorgCount(t, ctx, pool, "indexer_pause", chainID); n != 1 {
		t.Fatalf("indexer_pause rows after establish = %d, want 1 (no overwrite)", n)
	}

	t030ReplayToComplete(t, ctx, pool, lease, chainID, owned)

	// TAMPER half: with the recovery row ACTIVE, delete the indexer_pause row
	// (operator error/tamper). The ordinary block commit must STILL refuse via
	// the recovery-row gate — the pause deletion smuggles nothing.
	if _, err := pool.Exec(ctx, `DELETE FROM indexer_pause WHERE chain_id = $1`, chainID); err != nil {
		t.Fatalf("tamper-delete indexer_pause: %v", err)
	}
	t030RefusedBlockCommit(t, ctx, pool, lease, chainID)
	var dh int64
	var dkind, ddetail string
	if err := pool.QueryRow(ctx, `SELECT height, kind, detail FROM deposit_pause WHERE chain_id = $1`, chainID).Scan(&dh, &dkind, &ddetail); err != nil {
		t.Fatalf("read deposit_pause after tamper: %v", err)
	}
	if dh != 11 || dkind != "chain_view_changed" || ddetail != "t031-independent-survivor" {
		t.Fatalf("survivor = (%d %s %s), want untouched (11 chain_view_changed t031-independent-survivor)", dh, dkind, ddetail)
	}

	// RACE half: a fresh stream pause lands between complete and release. The
	// release frees the recovery cause only — the fresh row survives and rides
	// the terminal event.
	if _, err := pool.Exec(ctx, `
INSERT INTO log_pause (chain_id, height, kind, detail)
VALUES ($1, 12, 'chain_view_changed', 't031-race-fresh-pause')`, chainID); err != nil {
		t.Fatalf("seed race log_pause: %v", err)
	}
	if err := SignalRecoveryReconcile(ctx, pool, lease, chainID, owned,
		"over-deep", "1-20", "tip-diverged-beyond-depth-25"); err != nil {
		t.Fatalf("signal reconcile: %v", err)
	}
	const opT = "op-t031-release"
	evT := fmt.Sprintf("old_tip=20:%s new_tip=21:%s searched=1-20 cause=over-deep repaired-by=op-t031-repair",
		depositBlockHash(20), depositBlockHash(120))
	dispT := "orphaned=1 revived=0 replayed=block:20,log:20,deposit:20"
	if err := AuthorizeRecoveryRepair(ctx, pool, chainID, opT, evT, dispT); err != nil {
		t.Fatalf("repair: %v", err)
	}
	snapT := reorgSnapAudit(t, ctx, pool, chainID)
	if err := AuthorizeRecoveryRelease(ctx, pool, chainID, opT, evT, dispT); err != nil {
		t.Fatalf("release: %v", err)
	}
	// Release deleted ONLY the recovery row.
	if n := reorgCount(t, ctx, pool, "reorg_recovery", chainID); n != 0 {
		t.Fatalf("recovery rows = %d after release, want 0", n)
	}
	var terminal string
	if err := pool.QueryRow(ctx, `SELECT detail FROM reorg_recovery_events
WHERE chain_id = $1 AND event = 'released'`, chainID).Scan(&terminal); err != nil {
		t.Fatalf("read terminal event: %v", err)
	}
	for _, want := range []string{"event=released", opT, evT, dispT, "surviving_pauses=log_pause,deposit_pause", "swept=16-20", "version=1"} {
		if !strings.Contains(terminal, want) {
			t.Fatalf("terminal detail missing %q: %q", want, terminal)
		}
	}
	got := reorgSnapAudit(t, ctx, pool, chainID)
	if got.rec != 0 || got.ev != snapT.ev+1 || got.trans != snapT.trans || got.obs != snapT.obs ||
		got.dpause != 1 || got.lpause != 1 || got.ipause != 0 {
		t.Fatalf("post-release audit = %+v, want rec=0 ev=%d pauses=(1,1,0)", got, snapT.ev+1)
	}
	if err := pool.QueryRow(ctx, `SELECT height, kind, detail FROM deposit_pause WHERE chain_id = $1`, chainID).Scan(&dh, &dkind, &ddetail); err != nil {
		t.Fatalf("read surviving deposit_pause: %v", err)
	}
	if dh != 11 || dkind != "chain_view_changed" || ddetail != "t031-independent-survivor" {
		t.Fatalf("deposit survivor = (%d %s %s), want byte-identical content", dh, dkind, ddetail)
	}
	var lh int64
	var lkind, ldetail string
	if err := pool.QueryRow(ctx, `SELECT height, kind, detail FROM log_pause WHERE chain_id = $1`, chainID).Scan(&lh, &lkind, &ldetail); err != nil {
		t.Fatalf("read surviving log_pause: %v", err)
	}
	if lh != 12 || lkind != "chain_view_changed" || ldetail != "t031-race-fresh-pause" {
		t.Fatalf("log survivor = (%d %s %s), want byte-identical content", lh, lkind, ldetail)
	}
	if n := reorgCount(t, ctx, pool, "deposit_pause_audit", chainID); n != 0 {
		t.Fatalf("deposit_pause_audit rows = %d, want 0 (release invents nothing)", n)
	}

	// The survivors keep ordinary confirmation work stopped (zero writes).
	depositSeedObservation(t, ctx, pool, chainID, 14, depositBlockHash(14), depositTxHash(14, 0), 1, "7", 1)
	confirmSeedPolicyRow(t, ctx, pool, chainID, 1, 4, nil, "bootstrap", nil)
	cT, err := NewConfirmationCommitter(pool, ConfirmationConfig{ChainID: chainID, ThresholdN: 4})
	if err != nil {
		t.Fatalf("NewConfirmationCommitter(): %v", err)
	}
	rcapT := testRecoveryCap(t, ctx, pool, chainID)
	snapC := reorgSnapAudit(t, ctx, pool, chainID)
	cerr := cT.ConfirmDepositUnit(ctx, lease, ConfirmBasis{
		BlockHash: depositBlockHash(14), TxHash: depositTxHash(14, 0), LogIndex: 1, Height: 14,
		TipNumber: 20, TipHash: depositBlockHash(20), PolicySeq: 1, ThresholdN: 4,
	}, rcapT)
	var paused *streamPauseError
	if !errors.As(cerr, &paused) || paused.stream != "deposit_pause" {
		t.Fatalf("post-release confirm = %v, want the deposit_pause stop", cerr)
	}
	snapC.assertEqual(t, reorgSnapAudit(t, ctx, pool, chainID), "surviving-pause stop")
}

// ChainID reservation: 906001-906015 taken above (mini-loop, guards,
// depth, T023 bulk, T025-T027, T030-T031); this test owns 906016 (T035).

// TestT035AuditCompleteness pins audit completeness on real PG
// (FR-07/21/22, V12-audit, SC-12) with rowcount assertions throughout.
func TestT035AuditCompleteness(t *testing.T) {
	pool := reorgTestPool(t)
	defer pool.Close()
	ctx := context.Background()
	const chainID = int64(906016)
	lease := depositITLease(t, pool, chainID)
	reorgSeedChain(t, ctx, pool, chainID, 10, 20, 10)
	depositSeedHistory(t, ctx, pool, chainID, 1, 10, strings.Repeat("aa", 32))
	if _, err := pool.Exec(ctx, `
INSERT INTO deposit_checkpoint (chain_id, start_block, config_hash, next_block)
VALUES ($1, 10, $2, 20)`, chainID, strings.Repeat("aa", 32)); err != nil {
		t.Fatalf("seed deposit_checkpoint: %v", err)
	}
	oldBH16, oldTx16 := depositBlockHash(16), depositTxHash(16, 0)
	oldBH18, oldTx18 := depositBlockHash(18), depositTxHash(18, 0)
	keeperBH, keeperTx := depositBlockHash(14), depositTxHash(14, 0)
	depositSeedObservation(t, ctx, pool, chainID, 16, oldBH16, oldTx16, 0, "50", 1)
	depositSeedObservation(t, ctx, pool, chainID, 18, oldBH18, oldTx18, 0, "51", 1)
	depositSeedObservation(t, ctx, pool, chainID, 14, keeperBH, keeperTx, 0, "7", 1)
	depositSeedSourceRow(t, ctx, pool, chainID, 16, oldBH16, oldTx16, 0)

	// One ex-Confirmed observation via the real committer (N=4, tip 20).
	c, err := NewConfirmationCommitter(pool, ConfirmationConfig{ChainID: chainID, ThresholdN: 4})
	if err != nil {
		t.Fatalf("NewConfirmationCommitter(): %v", err)
	}
	rcap0 := testRecoveryCap(t, ctx, pool, chainID)
	if err := c.ConfirmDepositUnit(ctx, lease, ConfirmBasis{
		BlockHash: oldBH16, TxHash: oldTx16, LogIndex: 0, Height: 16,
		TipNumber: 20, TipHash: depositBlockHash(20), PolicySeq: 1, ThresholdN: 4,
	}, rcap0); err != nil {
		t.Fatalf("seed ex-confirmed confirm: %v", err)
	}
	status, nullAt, tipN, thr, seq, gotTip, conf := confirmReadBasis(t, ctx, pool, chainID, oldBH16, oldTx16)
	if status != "confirmed" || nullAt || tipN != 20 || gotTip != depositBlockHash(20) || thr != 4 || seq != 1 || conf != "5" {
		t.Fatalf("old basis = (%s null=%v tip %d %s N=%d conf=%s seq=%d), want confirmed tip(20 %s) N=4 conf=5 seq=1",
			status, nullAt, tipN, gotTip, thr, conf, seq, depositBlockHash(20))
	}

	// ---- Cycle 1: invalidate -> same-hash revive at 16 -> new-hash replay
	// 17..20 linked onto restored 16 -> release.
	res1 := reorgEstablishOne(t, ctx, pool, lease, chainID, 20)
	rec1 := res1.RecoveryID
	owned1 := RecoveryCapture{Seq: res1.Seq, Owned: &RecoveryOwned{RecoveryID: rec1}}
	if err := ConfirmRecoveryAncestor(ctx, pool, lease, chainID, owned1, 15, depositBlockHash(15), "t035-c1"); err != nil {
		t.Fatalf("c1 confirm ancestor: %v", err)
	}
	from, to, flipped, err := InvalidateRecoveryBlocks(ctx, pool, lease, chainID, owned1)
	if err != nil {
		t.Fatalf("c1 invalidate blocks: %v", err)
	}
	if from != 16 || to != 20 || flipped != 5 {
		t.Fatalf("c1 sweep = [%d,%d] flipped %d; want [16,20] x5", from, to, flipped)
	}
	orphaned, err := InvalidateRecoveryObservations(ctx, pool, lease, chainID, owned1)
	if err != nil {
		t.Fatalf("c1 invalidate observations: %v", err)
	}
	if orphaned != 2 {
		t.Fatalf("c1 orphaned = %d, want 2 (ex-confirmed 16 + pending 18)", orphaned)
	}
	var keeperStatus string
	if err := pool.QueryRow(ctx, `SELECT status FROM deposit_observations
WHERE chain_id = $1 AND block_hash = $2`, chainID, keeperBH).Scan(&keeperStatus); err != nil {
		t.Fatalf("read keeper: %v", err)
	}
	if keeperStatus != "pending" {
		t.Fatalf("keeper = %s, want pending (ancestor-side untouched)", keeperStatus)
	}
	// Both conversions audited under rec1 with old source identity + old basis.
	var c1trans int64
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM deposit_observation_transitions
WHERE chain_id = $1 AND recovery_id = $2`, chainID, rec1).Scan(&c1trans); err != nil {
		t.Fatalf("count c1 transitions: %v", err)
	}
	if c1trans != 2 {
		t.Fatalf("c1 transitions = %d, want 2", c1trans)
	}
	var fromSt16, snap16 string
	if err := pool.QueryRow(ctx, `SELECT from_status, basis_snapshot FROM deposit_observation_transitions
WHERE chain_id = $1 AND block_hash = $2 AND recovery_id = $3`, chainID, oldBH16, rec1).Scan(&fromSt16, &snap16); err != nil {
		t.Fatalf("read c1 h16 transition: %v", err)
	}
	if fromSt16 != "confirmed" || !strings.Contains(snap16, depositBlockHash(20)) {
		t.Fatalf("c1 h16 transition = (%s %q), want (confirmed + old tip %s)", fromSt16, snap16, depositBlockHash(20))
	}
	for _, stream := range []RecoveryStream{RecoveryStreamBlock, RecoveryStreamLog, RecoveryStreamDeposit} {
		if _, _, err := RollbackRecoveryCheckpoint(ctx, pool, lease, chainID, owned1, stream); err != nil {
			t.Fatalf("c1 rollback %s: %v", stream, err)
		}
	}
	newHash := func(n int64) string { return fmt.Sprintf("0x%064x", 0xe00e_0000+uint64(n)) }
	// Same-hash prefix is height 16 only: new 17 links onto restored old 16.
	newParent := func(h int64) string {
		if h == 17 {
			return oldBH16
		}
		return newHash(h - 1)
	}
	var blocks []ReplayBlock
	for h := int64(17); h <= 20; h++ {
		blocks = append(blocks, ReplayBlock{Number: h, Hash: newHash(h), ParentHash: newParent(h)})
	}
	var obsBefore int64
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM deposit_observations WHERE chain_id = $1`, chainID).Scan(&obsBefore); err != nil {
		t.Fatalf("count observations: %v", err)
	}
	if err := RecanonicalizeRecoveryBlock(ctx, pool, lease, chainID, owned1, 16, oldBH16); err != nil {
		t.Fatalf("c1 recanonicalize 16: %v", err)
	}
	if err := ReviveRecoveryObservation(ctx, pool, lease, chainID, owned1, oldBH16, oldTx16, 0, "t035-same-hash"); err != nil {
		t.Fatalf("c1 revive h16: %v", err)
	}
	var revStatus, revOrphan string
	if err := pool.QueryRow(ctx, `SELECT status, COALESCE(orphan_recovery_id, '') FROM deposit_observations
WHERE chain_id = $1 AND block_hash = $2`, chainID, oldBH16).Scan(&revStatus, &revOrphan); err != nil {
		t.Fatalf("read revived h16: %v", err)
	}
	if revStatus != "pending" || revOrphan != rec1 {
		t.Fatalf("revived h16 = (%s %s), want (pending %s)", revStatus, revOrphan, rec1)
	}
	var obsAfter int64
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM deposit_observations WHERE chain_id = $1`, chainID).Scan(&obsAfter); err != nil {
		t.Fatalf("recount observations: %v", err)
	}
	if obsAfter != obsBefore {
		t.Fatalf("observations %d -> %d across revive, want unchanged (reuse in place)", obsBefore, obsAfter)
	}
	if err := ReplayRecoveryRange(ctx, pool, lease, chainID, owned1, RecoveryStreamBlock, 16, 20, blocks, nil, nil); err != nil {
		t.Fatalf("c1 replay block [16,20]: %v", err)
	}
	if err := ReplayRecoveryRange(ctx, pool, lease, chainID, owned1, RecoveryStreamLog, 16, 20, nil, nil, nil); err != nil {
		t.Fatalf("c1 replay log [16,20]: %v", err)
	}
	if err := ReplayRecoveryRange(ctx, pool, lease, chainID, owned1, RecoveryStreamDeposit, 16, 20, nil, nil, nil); err != nil {
		t.Fatalf("c1 replay deposit [16,20]: %v", err)
	}
	var phase string
	if err := pool.QueryRow(ctx, `SELECT phase FROM reorg_recovery WHERE chain_id = $1`, chainID).Scan(&phase); err != nil {
		t.Fatalf("read phase: %v", err)
	}
	if phase != reorgPhaseCompletePending {
		t.Fatalf("phase = %q after full replay, want complete_pending", phase)
	}
	if err := CompleteRecoveryVerify(ctx, pool, lease, chainID, owned1); err != nil {
		t.Fatalf("c1 complete: %v", err)
	}
	if n := reorgCount(t, ctx, pool, "reorg_recovery", chainID); n != 0 {
		t.Fatalf("recovery rows = %d after release, want 0", n)
	}
	var terminal string
	if err := pool.QueryRow(ctx, `SELECT detail FROM reorg_recovery_events
WHERE chain_id = $1 AND event = 'auto_completed'`, chainID).Scan(&terminal); err != nil {
		t.Fatalf("read terminal event: %v", err)
	}
	for _, want := range []string{"orphaned=2", "revived=1", "surviving_pauses=none", "swept=16-20", "version=1"} {
		if !strings.Contains(terminal, want) {
			t.Fatalf("terminal detail missing %q: %q", want, terminal)
		}
	}
	// 006 wrote no pause rows anywhere in the loop.
	if got := reorgSnapAudit(t, ctx, pool, chainID); got.dpause != 0 || got.lpause != 0 || got.ipause != 0 {
		t.Fatalf("pause rows = %+v after cycle 1, want all zero", got)
	}

	// ---- (i) every Orphaned row traces to old source + old basis + audit.
	rows, err := pool.Query(ctx, `SELECT block_hash, tx_hash, log_index, orphan_recovery_id FROM deposit_observations
WHERE chain_id = $1 AND status = 'orphaned'`, chainID)
	if err != nil {
		t.Fatalf("read orphaned rows: %v", err)
	}
	type orphanKey struct {
		bh, tx string
		idx    int64
		rec    string
	}
	var orphans []orphanKey
	for rows.Next() {
		var o orphanKey
		if err := rows.Scan(&o.bh, &o.tx, &o.idx, &o.rec); err != nil {
			rows.Close()
			t.Fatalf("scan orphan: %v", err)
		}
		orphans = append(orphans, o)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate orphans: %v", err)
	}
	if len(orphans) != 1 || orphans[0].bh != oldBH18 || orphans[0].rec != rec1 {
		t.Fatalf("orphaned rows = %+v, want exactly [(old18 %s)]", orphans, rec1)
	}
	for _, o := range orphans {
		var snap, fromSt, tRec string
		if err := pool.QueryRow(ctx, `SELECT from_status, basis_snapshot, recovery_id FROM deposit_observation_transitions
WHERE chain_id = $1 AND block_hash = $2 AND tx_hash = $3 AND log_index = $4 AND to_status = 'orphaned'`,
			chainID, o.bh, o.tx, o.idx).Scan(&fromSt, &snap, &tRec); err != nil {
			t.Fatalf("orphan %s transition: %v", o.bh, err)
		}
		if tRec != rec1 {
			t.Fatalf("orphan %s transition recovery = %s, want %s", o.bh, tRec, rec1)
		}
		var evDetail string
		if err := pool.QueryRow(ctx, `SELECT detail FROM reorg_recovery_events
WHERE chain_id = $1 AND recovery_id = $2 AND event = 'observations_invalidated'`, chainID, rec1).Scan(&evDetail); err != nil {
			t.Fatalf("orphan audit event: %v", err)
		}
		if !strings.Contains(evDetail, "orphaned=2") {
			t.Fatalf("orphan event detail missing orphaned=2: %q", evDetail)
		}
	}

	// ---- (ii) revived ex-Confirmed row reconfirms on the new basis only.
	rcapNew := testRecoveryCap(t, ctx, pool, chainID)
	if err := c.ConfirmDepositUnit(ctx, lease, ConfirmBasis{
		BlockHash: oldBH16, TxHash: oldTx16, LogIndex: 0, Height: 16,
		TipNumber: 20, TipHash: newHash(20), PolicySeq: 1, ThresholdN: 4,
	}, rcapNew); err != nil {
		t.Fatalf("new-basis reconfirm: %v", err)
	}
	nStatus, nNull, nTipN, nThr, nSeq, nTipH, nConf := confirmReadBasis(t, ctx, pool, chainID, oldBH16, oldTx16)
	if nStatus != "confirmed" || nNull || nTipN != 20 || nTipH != newHash(20) || nThr != 4 || nSeq != 1 || nConf != "5" {
		t.Fatalf("new basis = (%s null=%v tip %d %s N=%d conf=%s seq=%d), want confirmed tip(20 %s) N=4 conf=5 seq=1",
			nStatus, nNull, nTipN, nTipH, nThr, nConf, nSeq, newHash(20))
	}
	if nTipH == depositBlockHash(20) {
		t.Fatalf("new confirm tip = old tip hash (must be the new-chain hash)")
	}
	var oldTipConfirms int64
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM deposit_observations
WHERE chain_id = $1 AND status = 'confirmed' AND confirm_tip_hash = $2`, chainID, depositBlockHash(20)).Scan(&oldTipConfirms); err != nil {
		t.Fatalf("count old-tip confirms: %v", err)
	}
	if oldTipConfirms != 0 {
		t.Fatalf("confirmed rows on old tip = %d, want 0 (old basis lives only in transition snapshots)", oldTipConfirms)
	}

	// ---- (iii) second Confirmed->Orphaned->Pending cycle; both cycles kept.
	res2, err := EstablishRecovery(ctx, pool, lease, EstablishRequest{
		ChainID:      chainID,
		OldTipNumber: 20, OldTipHash: newHash(20),
		NewTipNumber: 21, NewTipHash: fmt.Sprintf("0x%064x", 0xf001_0000+21),
		DetectedHeight: 20, EnvMaxDepthRaw: reorgTestDepth,
	})
	if err != nil {
		t.Fatalf("c2 establish: %v", err)
	}
	rec2 := res2.RecoveryID
	if rec2 == rec1 {
		t.Fatalf("c2 recovery id reused %s (must be a fresh round)", rec1)
	}
	owned2 := RecoveryCapture{Seq: res2.Seq, Owned: &RecoveryOwned{RecoveryID: rec2}}
	// Refusal leg with zero-write proof: ordinary gate under the active row.
	snapRef := reorgSnapAudit(t, ctx, pool, chainID)
	rtx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin refusal probe: %v", err)
	}
	if _, err := recheckRecoveryGate(ctx, rtx, chainID, testRecoveryCap(t, ctx, pool, chainID)); !isRecoveryGate(err) {
		_ = rtx.Rollback(ctx)
		t.Fatalf("ordinary gate under active c2 row = %v, want refusal", err)
	}
	_ = rtx.Rollback(ctx)
	snapRef.assertEqual(t, reorgSnapAudit(t, ctx, pool, chainID), "c2 ordinary-gate refusal")
	if err := ConfirmRecoveryAncestor(ctx, pool, lease, chainID, owned2, 15, depositBlockHash(15), "t035-c2"); err != nil {
		t.Fatalf("c2 confirm ancestor: %v", err)
	}
	from2, to2, flipped2, err := InvalidateRecoveryBlocks(ctx, pool, lease, chainID, owned2)
	if err != nil {
		t.Fatalf("c2 invalidate blocks: %v", err)
	}
	if from2 != 16 || to2 != 20 || flipped2 != 5 {
		t.Fatalf("c2 sweep = [%d,%d] flipped %d; want [16,20] x5", from2, to2, flipped2)
	}
	orphaned2, err := InvalidateRecoveryObservations(ctx, pool, lease, chainID, owned2)
	if err != nil {
		t.Fatalf("c2 invalidate observations: %v", err)
	}
	if orphaned2 != 1 {
		t.Fatalf("c2 orphaned = %d, want 1 (reconfirmed 16 only)", orphaned2)
	}
	var c2obs int64
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM deposit_observations WHERE chain_id = $1`, chainID).Scan(&c2obs); err != nil {
		t.Fatalf("count observations: %v", err)
	}
	if err := RecanonicalizeRecoveryBlock(ctx, pool, lease, chainID, owned2, 16, oldBH16); err != nil {
		t.Fatalf("c2 recanonicalize 16: %v", err)
	}
	if err := ReviveRecoveryObservation(ctx, pool, lease, chainID, owned2, oldBH16, oldTx16, 0, "t035-c2-16"); err != nil {
		t.Fatalf("c2 revive h16: %v", err)
	}
	var c2obsAfter int64
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM deposit_observations WHERE chain_id = $1`, chainID).Scan(&c2obsAfter); err != nil {
		t.Fatalf("recount observations: %v", err)
	}
	if c2obsAfter != c2obs {
		t.Fatalf("observations %d -> %d across c2 revive, want unchanged", c2obs, c2obsAfter)
	}
	// h16: BOTH Confirmed->Orphaned->Pending cycles fully ordered, no overwrite.
	t16, err := pool.Query(ctx, `SELECT from_status, to_status, recovery_id FROM deposit_observation_transitions
WHERE chain_id = $1 AND block_hash = $2 AND tx_hash = $3 AND log_index = 0 ORDER BY at, recovery_id`, chainID, oldBH16, oldTx16)
	if err != nil {
		t.Fatalf("read h16 transitions: %v", err)
	}
	var got16 [][3]string
	for t16.Next() {
		var f, s, r string
		if err := t16.Scan(&f, &s, &r); err != nil {
			t16.Close()
			t.Fatalf("scan h16 transition: %v", err)
		}
		got16 = append(got16, [3]string{f, s, r})
	}
	t16.Close()
	if len(got16) != 4 || got16[0] != [3]string{"confirmed", "orphaned", rec1} ||
		got16[1] != [3]string{"orphaned", "pending", rec1} ||
		got16[2] != [3]string{"confirmed", "orphaned", rec2} ||
		got16[3] != [3]string{"orphaned", "pending", rec2} {
		t.Fatalf("h16 transitions = %v, want 4 ordered legs across rec1+rec2", got16)
	}

	// ---- (iv) post-release lineage: rec1's row is gone, its audit resolves.
	var rec1rows int64
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM reorg_recovery WHERE chain_id = $1 AND recovery_id = $2`,
		chainID, rec1).Scan(&rec1rows); err != nil {
		t.Fatalf("count rec1 row: %v", err)
	}
	if rec1rows != 0 {
		t.Fatalf("rec1 recovery rows = %d, want 0 (released)", rec1rows)
	}
	var lineage int64
	if err := pool.QueryRow(ctx, `
SELECT COUNT(*) FROM deposit_observations o
JOIN deposit_observation_transitions t ON t.chain_id = o.chain_id
  AND t.block_hash = o.block_hash AND t.tx_hash = o.tx_hash AND t.log_index = o.log_index
  AND t.recovery_id = o.orphan_recovery_id
JOIN reorg_recovery_events e ON e.chain_id = o.chain_id
  AND e.recovery_id = o.orphan_recovery_id AND e.event = 'observations_invalidated'
WHERE o.chain_id = $1 AND o.status = 'orphaned' AND o.orphan_recovery_id = $2`,
		chainID, rec1).Scan(&lineage); err != nil {
		t.Fatalf("post-release lineage join: %v", err)
	}
	if lineage != 1 {
		t.Fatalf("post-release lineage rows = %d, want 1 (old18 -> transition -> event, row deleted)", lineage)
	}
	var rec1trans int64
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM deposit_observation_transitions
WHERE chain_id = $1 AND recovery_id = $2`, chainID, rec1).Scan(&rec1trans); err != nil {
		t.Fatalf("count rec1 transitions: %v", err)
	}
	if rec1trans != 3 {
		t.Fatalf("rec1 transitions = %d, want 3 (2 orphan + 1 revive, surviving release)", rec1trans)
	}
}
