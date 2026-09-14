//go:build integration

// reorgassembly_test.go pins the 006 recovery replay assembly fail-safe
// (B3): detectable anomalies in assembleLogsAndDeposits must never become
// legitimate empty results with frontiers advancing over zero rows.
// FilterLogs RPC errors propagate (loud tick failure, range retried next
// tick); an unbound historicalMatch height holds the range (frontier,
// checkpoints and events untouched, height re-walked next tick); a genuinely
// empty log interval still advances the frontier (legitimate-empty stays
// legal). Class 3 (RPC success with silently omitted logs) is a recorded
// non-goal: undetectable here by construction, covered by comment only.
package indexer

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/eth"
)

// asmLogsClient is a scripted LogsClient: err fails FilterLogs, logs is the
// Transfer-log payload returned on success.
type asmLogsClient struct {
	err  error
	logs []types.Log
}

func (c *asmLogsClient) FilterLogs(context.Context, ethereum.FilterQuery) ([]types.Log, error) {
	return c.logs, c.err
}

// asmFixture is one forked recovery ready to replay streams over [16,20]:
// local rows 10..15 canonical (old chain), 16..20 invalidated (old fork),
// new-fork block rows 16..20 replayed canonical, all checkpoints floored.
type asmFixture struct {
	pool   *pgxpool.Pool
	lease  *Lease
	chain  *scriptChain
	ex     *RecoveryExecutor
	row    *RecoveryRow
	cap    RecoveryCapture
	fork   map[int64]string
	orig15 string
}

// asmSetup builds the fixture: stub chain seeded canonical, recovery
// established/confirmed/invalidated/rolled back, block range [16,20]
// replayed on the fork, executor wired with the given logs client and
// version history starting at versionStart.
func asmSetup(t *testing.T, chainID int64, logs LogsClient, versionStart uint64) *asmFixture {
	t.Helper()
	ctx := context.Background()
	pool := reorgTestPool(t)
	lease := depositITLease(t, pool, chainID)
	chain := newScriptChain(chainID, 20, 0xA5)
	hashOf := func(n uint64) string { return strings.ToLower(chain.heads[n].Hash().Hex()) }
	for n := uint64(10); n <= 20; n++ {
		if _, err := pool.Exec(ctx, `
INSERT INTO chain_blocks (chain_id, number, hash, parent_hash, canonical)
VALUES ($1, $2, $3, $4, true)`,
			chainID, int64(n), hashOf(n), strings.ToLower(chain.heads[n].ParentHash.Hex())); err != nil {
			t.Fatalf("seed chain_blocks %d: %v", n, err)
		}
	}
	cfgHash := strings.Repeat("ab", 32)
	if _, err := pool.Exec(ctx, `
INSERT INTO indexer_checkpoint (chain_id, height, block_hash, start_height)
VALUES ($1, 20, $2, 10)`, chainID, hashOf(20)); err != nil {
		t.Fatalf("seed indexer_checkpoint: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO log_checkpoint (chain_id, start_block, config_hash, next_block)
VALUES ($1, 10, $2, 20)`, chainID, cfgHash); err != nil {
		t.Fatalf("seed log_checkpoint: %v", err)
	}
	depositSeedCheckpoint(t, ctx, pool, chainID, 10, cfgHash, 20)
	depositSeedHistory(t, ctx, pool, chainID, 1, versionStart, cfgHash)
	// Fork the stub chain over the sweep range: new 16 links onto old 15.
	parent := chain.heads[15].Hash()
	for n := uint64(16); n <= 20; n++ {
		h := &types.Header{Number: new(big.Int).SetUint64(n), ParentHash: parent, Extra: []byte{0xF0, byte(n)}, Time: 2000 + n, GasLimit: 30_000_000}
		chain.heads[n] = h
		parent = h.Hash()
	}
	res, err := EstablishRecovery(ctx, pool, lease, EstablishRequest{
		ChainID: chainID, OldTipNumber: 20, OldTipHash: hashOf(20),
		NewTipNumber: 21, NewTipHash: strings.ToLower(chain.heads[20].Hash().Hex()),
		DetectedHeight: 20, EnvMaxDepthRaw: reorgTestDepth,
	})
	if err != nil {
		t.Fatalf("EstablishRecovery(): %v", err)
	}
	cap := RecoveryCapture{Seq: res.Seq, Owned: &RecoveryOwned{RecoveryID: res.RecoveryID}}
	if err := ConfirmRecoveryAncestor(ctx, pool, lease, chainID, cap, 15, hashOf(15), "asm-evidence"); err != nil {
		t.Fatalf("confirm ancestor: %v", err)
	}
	if from, to, flipped, err := InvalidateRecoveryBlocks(ctx, pool, lease, chainID, cap); err != nil || from != 16 || to != 20 || flipped != 5 {
		t.Fatalf("invalidate blocks = [%d,%d] flipped %d err %v; want [16,20] x5", from, to, flipped, err)
	}
	if _, err := InvalidateRecoveryObservations(ctx, pool, lease, chainID, cap); err != nil {
		t.Fatalf("invalidate observations: %v", err)
	}
	for _, stream := range []RecoveryStream{RecoveryStreamBlock, RecoveryStreamLog, RecoveryStreamDeposit} {
		if _, _, err := RollbackRecoveryCheckpoint(ctx, pool, lease, chainID, cap, stream); err != nil {
			t.Fatalf("rollback %s: %v", stream, err)
		}
	}
	fork := map[int64]string{}
	var blocks []ReplayBlock
	for h := int64(16); h <= 20; h++ {
		fh := strings.ToLower(chain.heads[uint64(h)].Hash().Hex())
		fork[h] = fh
		ph := hashOf(15)
		if h > 16 {
			ph = fork[h-1]
		}
		blocks = append(blocks, ReplayBlock{Number: h, Hash: fh, ParentHash: ph})
	}
	if err := ReplayRecoveryRange(ctx, pool, lease, chainID, cap, RecoveryStreamBlock, 16, 20, blocks, nil, nil); err != nil {
		t.Fatalf("replay block [16,20]: %v", err)
	}
	ex, err := NewRecoveryExecutor(pool, lease, chain, logs, RecoveryConfig{
		ChainID: chainID, MaxDepthRaw: reorgTestDepth, ReplayHeights: 10, BlockStartHeight: 10,
	})
	if err != nil {
		t.Fatalf("NewRecoveryExecutor(): %v", err)
	}
	row, err := readRecoveryRow(ctx, pool, chainID)
	if err != nil || row == nil {
		t.Fatalf("readRecoveryRow() = %+v, %v; want replaying row", row, err)
	}
	return &asmFixture{pool: pool, lease: lease, chain: chain, ex: ex,
		row: row, cap: RecoveryCapture{Seq: row.Seq, Owned: &RecoveryOwned{RecoveryID: row.RecoveryID}},
		fork: fork, orig15: hashOf(15)}
}

// asmFrontier reads one stream frontier (nil = untouched).
func asmFrontier(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, column string) *int64 {
	t.Helper()
	var f *int64
	if err := pool.QueryRow(ctx, `SELECT `+column+` FROM reorg_recovery WHERE chain_id = $1`, chainID).Scan(&f); err != nil {
		t.Fatalf("read %s: %v", column, err)
	}
	return f
}

// asmCheckpointNext reads a stream checkpoint's next_block.
func asmCheckpointNext(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, table string) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(ctx, `SELECT next_block FROM `+table+` WHERE chain_id = $1`, chainID).Scan(&n); err != nil {
		t.Fatalf("read %s.next_block: %v", table, err)
	}
	return n
}

// asmReplayProgressCount counts replay_progress events (one per committed range).
func asmReplayProgressCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM reorg_recovery_events
WHERE chain_id = $1 AND event = 'replay_progress'`, chainID).Scan(&n); err != nil {
		t.Fatalf("count replay_progress: %v", err)
	}
	return n
}

// TestReorgAssemblyFilterLogsFailsLoud: a FilterLogs RPC error must fail the
// tick loudly — no frontier, checkpoint or event movement for the range.
func TestReorgAssemblyFilterLogsFailsLoud(t *testing.T) {
	const chainID = int64(907001)
	logs := &asmLogsClient{err: errors.New("asm: rpc boom")}
	fx := asmSetup(t, chainID, logs, 10)
	ctx := context.Background()
	before := asmReplayProgressCount(t, ctx, fx.pool, chainID)
	if _, _, err := fx.ex.assembleLogsAndDeposits(ctx, 16, 20); err == nil {
		t.Fatal("assembleLogsAndDeposits() with failing FilterLogs = nil, want propagated error")
	}
	if err := fx.ex.replayStream(ctx, fx.row, fx.cap, RecoveryStreamLog, 20); err == nil {
		t.Fatal("replayStream(log) with failing FilterLogs = nil, want loud tick failure")
	}
	if f := asmFrontier(t, ctx, fx.pool, chainID, "log_frontier"); f != nil {
		t.Fatalf("log_frontier = %d after failed tick, want NULL (untouched)", *f)
	}
	if n := asmCheckpointNext(t, ctx, fx.pool, chainID, "log_checkpoint"); n != 16 {
		t.Fatalf("log_checkpoint.next_block = %d after failed tick, want 16 (unchanged)", n)
	}
	if n := asmReplayProgressCount(t, ctx, fx.pool, chainID); n != before {
		t.Fatalf("replay_progress events = %d after failed tick, want %d (none for the range)", n, before)
	}
}

// TestReorgAssemblyUnboundHeightHolds: a logged height with no bound deposit
// version (history starts above the replay range) must hold the range — the
// deposit stream returns nil without calling ReplayRecoveryRange, so the
// frontier stays NULL and the height is re-walked next tick, never skipped.
func TestReorgAssemblyUnboundHeightHolds(t *testing.T) {
	const chainID = int64(907002)
	fx := asmSetup(t, chainID, &asmLogsClient{}, 20)
	logAt17 := types.Log{
		BlockNumber: 17, BlockHash: common.HexToHash(fx.fork[17]),
		TxHash: common.HexToHash(depositTxHash(17, 0)), Index: 0,
		Address: common.HexToAddress(testContractA),
		Topics:  []common.Hash{eth.TransferSig},
		Data:    common.BigToHash(big.NewInt(1)).Bytes(),
	}
	fx.ex.logs = &asmLogsClient{logs: []types.Log{logAt17}}
	ctx := context.Background()
	before := asmReplayProgressCount(t, ctx, fx.pool, chainID)
	if _, _, err := fx.ex.assembleLogsAndDeposits(ctx, 16, 20); !errors.Is(err, errRecoveryAssemblyHold) {
		t.Fatalf("assembleLogsAndDeposits() with unbound height = %v, want assembly-hold sentinel", err)
	}
	if err := fx.ex.replayStream(ctx, fx.row, fx.cap, RecoveryStreamDeposit, 20); err != nil {
		t.Fatalf("replayStream(deposit) on hold = %v, want nil (silent hold, no ReplayRecoveryRange)", err)
	}
	if f := asmFrontier(t, ctx, fx.pool, chainID, "deposit_frontier"); f != nil {
		t.Fatalf("deposit_frontier = %d on hold, want NULL (never past the unbound height)", *f)
	}
	if n := asmCheckpointNext(t, ctx, fx.pool, chainID, "deposit_checkpoint"); n != 16 {
		t.Fatalf("deposit_checkpoint.next_block = %d on hold, want 16 (unchanged)", n)
	}
	if n := asmReplayProgressCount(t, ctx, fx.pool, chainID); n != before {
		t.Fatalf("replay_progress events = %d on hold, want %d (none for the range)", n, before)
	}
}

// TestReorgAssemblyEmptyIntervalAdvances: a genuinely empty log interval is
// legitimate — the frontier advances, the checkpoint follows, and every
// replayed height still proves exactly one canonical block row.
func TestReorgAssemblyEmptyIntervalAdvances(t *testing.T) {
	const chainID = int64(907003)
	fx := asmSetup(t, chainID, &asmLogsClient{}, 10)
	ctx := context.Background()
	if err := fx.ex.replayStream(ctx, fx.row, fx.cap, RecoveryStreamLog, 20); err != nil {
		t.Fatalf("replayStream(log) on empty interval = %v, want nil", err)
	}
	if f := asmFrontier(t, ctx, fx.pool, chainID, "log_frontier"); f == nil || *f != 20 {
		t.Fatalf("log_frontier = %v after empty interval, want 20", f)
	}
	if n := asmCheckpointNext(t, ctx, fx.pool, chainID, "log_checkpoint"); n != 21 {
		t.Fatalf("log_checkpoint.next_block = %d after empty interval, want 21", n)
	}
	var canon int64
	if err := fx.pool.QueryRow(ctx, `SELECT COUNT(*) FROM chain_blocks
WHERE chain_id = $1 AND number BETWEEN 16 AND 20 AND canonical`, chainID).Scan(&canon); err != nil {
		t.Fatalf("count canonical: %v", err)
	}
	if canon != 5 {
		t.Fatalf("canonical rows over [16,20] = %d, want 5 (exactly one per height)", canon)
	}
}
