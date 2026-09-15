//go:build integration

// reorg_metrics_wiring_integration_test.go proves the T032 loop wiring on
// real PostgreSQL (no Anvil needed — every step is a transaction or a
// stub-client tick): conversions count exactly once through the executor
// (repeat ticks and crash-resume re-walks add zero), evidence waits count
// per wait with the executor's own classes, snapshots track the durable
// row from establish to terminal release, and release leaves an empty
// snapshot behind. Serve-side gauge mirroring of these snapshots is pinned
// in internal/app/serve_recovery_metrics_test.go.
package indexer

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/eth"
)

type fakeRecoveryMetrics struct {
	orphaned int64
	revived  int64
	waits    map[string]int64
}

func (f *fakeRecoveryMetrics) AddReorgOrphaned(_ int64, n int64) { f.orphaned += n }
func (f *fakeRecoveryMetrics) AddReorgRevived(_ int64, n int64)  { f.revived += n }
func (f *fakeRecoveryMetrics) AddReorgEvidenceWait(_ int64, class string) {
	if f.waits == nil {
		f.waits = map[string]int64{}
	}
	f.waits[class]++
}

type failHeaderClient struct{ err error }

func (f failHeaderClient) ChainID(context.Context) (*big.Int, error) {
	return big.NewInt(1), nil
}
func (f failHeaderClient) HeaderByNumber(context.Context, *big.Int) (*types.Header, error) {
	return nil, f.err
}
func (f failHeaderClient) FilterLogs(context.Context, ethereum.FilterQuery) ([]types.Log, error) {
	return nil, f.err
}

func metricsWiringRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64) (*RecoveryRow, RecoveryCapture) {
	t.Helper()
	row, err := readRecoveryRow(ctx, pool, chainID)
	if err != nil || row == nil {
		t.Fatalf("readRecoveryRow() = %+v, %v; want active row", row, err)
	}
	return row, RecoveryCapture{Seq: row.Seq, Owned: &RecoveryOwned{RecoveryID: row.RecoveryID}}
}

// TestReorgMetricsExecutorCounts: two observations orphan then revive
// through the executor ticks — each counts exactly once, and the repeat
// tick plus the refused second recanonicalize add zero.
func TestReorgMetricsExecutorCounts(t *testing.T) {
	pool := reorgTestPool(t)
	defer pool.Close()
	ctx := context.Background()
	const chainID = int64(907101)
	lease := depositITLease(t, pool, chainID)
	reorgSeedChain(t, ctx, pool, chainID, 10, 15, 10)
	depositSeedHistory(t, ctx, pool, chainID, 1, 10, strings.Repeat("aa", 32))
	bh15 := depositBlockHash(15)
	for i, amount := range []string{"50", "51"} {
		tx := depositTxHash(15, uint64(i))
		depositSeedTransferRow(t, ctx, pool, chainID, 15, bh15, tx, uint64(i),
			common.HexToAddress(testContractA), common.HexToAddress(testContractB),
			common.HexToAddress(depositWatchAddr), big.NewInt(1))
		depositSeedObservation(t, ctx, pool, chainID, 15, bh15, tx, uint64(i), amount, 1)
	}

	res := reorgEstablishOne(t, ctx, pool, lease, chainID, 15)
	owned := RecoveryCapture{Seq: res.Seq, Owned: &RecoveryOwned{RecoveryID: res.RecoveryID}}
	if err := ConfirmRecoveryAncestor(ctx, pool, lease, chainID, owned, 14, depositBlockHash(14), "metrics-evidence"); err != nil {
		t.Fatalf("confirm ancestor: %v", err)
	}

	ex, err := NewRecoveryExecutor(pool, lease,
		&coordStubClient{chainID: big.NewInt(chainID)}, &asmLogsClient{},
		RecoveryConfig{ChainID: chainID, MaxDepthRaw: reorgTestDepth})
	if err != nil {
		t.Fatalf("NewRecoveryExecutor(): %v", err)
	}
	fake := &fakeRecoveryMetrics{}
	ex.SetRecoveryMetrics(fake)

	row, cap := metricsWiringRow(t, ctx, pool, chainID)
	if err := ex.tickInvalidate(ctx, row, cap); err != nil {
		t.Fatalf("tickInvalidate(): %v", err)
	}
	if fake.orphaned != 2 {
		t.Fatalf("orphaned counts = %d, want 2 (one per conversion)", fake.orphaned)
	}
	if n := reorgCount(t, ctx, pool, "deposit_observations", chainID); n != 2 {
		t.Fatalf("observations = %d, want 2 rows retained", n)
	}

	// Repeat tick re-runs the idempotent transactions: zero new
	// conversions, zero new counts.
	row, cap = metricsWiringRow(t, ctx, pool, chainID)
	if err := ex.tickInvalidate(ctx, row, cap); err != nil {
		t.Fatalf("repeat tickInvalidate(): %v", err)
	}
	if fake.orphaned != 2 {
		t.Fatalf("orphaned counts after repeat = %d, want still 2", fake.orphaned)
	}

	mid, err := LoadRecoverySnapshot(ctx, pool, chainID)
	if err != nil || mid.Row == nil {
		t.Fatalf("mid-cycle snapshot = %+v, %v; want active row", mid.Row, err)
	}
	if mid.SweepEnd == nil || *mid.SweepEnd != 15 {
		t.Fatalf("mid-cycle sweep end = %d, want 15", derefInt64(mid.SweepEnd))
	}
	if mid.Depth == nil || *mid.Depth != 1 || mid.Bound != 25 || mid.Reconcile {
		t.Fatalf("mid-cycle snapshot = depth %d bound %d reconcile %v; want 1/25/false",
			derefInt64(mid.Depth), mid.Bound, mid.Reconcile)
	}

	row, cap = metricsWiringRow(t, ctx, pool, chainID)
	if err := ex.recanonicalizeHeight(ctx, row, cap, 15, bh15); err != nil {
		t.Fatalf("recanonicalizeHeight(): %v", err)
	}
	if fake.revived != 2 {
		t.Fatalf("revived counts = %d, want 2", fake.revived)
	}
	// The second flip refuses at the gate (single canonical already back):
	// the tick fails safe and the counter does not move.
	row, cap = metricsWiringRow(t, ctx, pool, chainID)
	if err := ex.recanonicalizeHeight(ctx, row, cap, 15, bh15); err == nil {
		t.Fatal("repeat recanonicalizeHeight() = nil, want gate refusal")
	}
	if fake.revived != 2 {
		t.Fatalf("revived counts after refused repeat = %d, want still 2", fake.revived)
	}
	if len(fake.waits) != 0 {
		t.Fatalf("evidence waits = %v, want none on the healthy path", fake.waits)
	}

	snap, err := LoadRecoverySnapshot(ctx, pool, chainID)
	if err != nil || snap.Row == nil {
		t.Fatalf("LoadRecoverySnapshot() = %+v, %v; want active row", snap.Row, err)
	}
	// The restore removed the last non-canonical row above the ancestor, so
	// the sweep end recomputes to the ancestor itself — the snapshot tracks
	// durable truth, not a cached range.
	if snap.SweepEnd == nil || *snap.SweepEnd != 14 {
		t.Fatalf("sweep end after restore = %d, want 14 (ancestor)", derefInt64(snap.SweepEnd))
	}
	if snap.Depth == nil || *snap.Depth != 1 || snap.Bound != 25 || snap.Reconcile {
		t.Fatalf("snapshot = depth %d bound %d reconcile %v; want 1/25/false",
			derefInt64(snap.Depth), snap.Bound, snap.Reconcile)
	}
}

func derefInt64(p *int64) int64 {
	if p == nil {
		return -1
	}
	return *p
}

// TestReorgMetricsReleaseCycle: the mini close-loop (invalidate → replay →
// auto release) leaves an empty snapshot — the serve observer clears
// active/reconcile/lags from exactly this shape.
func TestReorgMetricsReleaseCycle(t *testing.T) {
	pool := reorgTestPool(t)
	defer pool.Close()
	ctx := context.Background()
	const chainID = int64(907102)
	lease := depositITLease(t, pool, chainID)
	reorgSeedChain(t, ctx, pool, chainID, 10, 20, 10)
	depositSeedHistory(t, ctx, pool, chainID, 1, 10, strings.Repeat("aa", 32))
	if _, err := pool.Exec(ctx, `
INSERT INTO deposit_checkpoint (chain_id, start_block, config_hash, next_block)
VALUES ($1, 10, $2, 20)`, chainID, strings.Repeat("aa", 32)); err != nil {
		t.Fatalf("seed deposit_checkpoint: %v", err)
	}
	depositSeedObservation(t, ctx, pool, chainID, 17, depositBlockHash(17), depositTxHash(17, 0), 0, "50", 1)

	res := reorgEstablishOne(t, ctx, pool, lease, chainID, 20)
	owned := RecoveryCapture{Seq: res.Seq, Owned: &RecoveryOwned{RecoveryID: res.RecoveryID}}
	if err := ConfirmRecoveryAncestor(ctx, pool, lease, chainID, owned, 15, depositBlockHash(15), "metrics-cycle"); err != nil {
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

	mid, err := LoadRecoverySnapshot(ctx, pool, chainID)
	if err != nil || mid.Row == nil {
		t.Fatalf("mid-cycle snapshot = %+v, %v; want active row", mid.Row, err)
	}
	if mid.SweepEnd == nil || *mid.SweepEnd != 20 || mid.Depth == nil || *mid.Depth != 5 {
		t.Fatalf("mid-cycle sweep/depth = %v/%v, want 20/5", mid.SweepEnd, mid.Depth)
	}

	newHash := func(n int64) string { return fmt.Sprintf("0x%064x", 0xe00e_0000+uint64(n)) }
	var blocks []ReplayBlock
	for h := int64(16); h <= 20; h++ {
		parent := depositBlockHash(15)
		if h > 16 {
			parent = newHash(h - 1)
		}
		blocks = append(blocks, ReplayBlock{Number: h, Hash: newHash(h), ParentHash: parent})
	}
	if err := ReplayRecoveryRange(ctx, pool, lease, chainID, owned, RecoveryStreamBlock, 16, 20, blocks, nil, nil); err != nil {
		t.Fatalf("replay block [16,20]: %v", err)
	}
	if err := ReplayRecoveryRange(ctx, pool, lease, chainID, owned, RecoveryStreamLog, 16, 20, nil, nil, nil); err != nil {
		t.Fatalf("replay log [16,20]: %v", err)
	}
	if err := ReplayRecoveryRange(ctx, pool, lease, chainID, owned, RecoveryStreamDeposit, 16, 20, nil, nil, nil); err != nil {
		t.Fatalf("replay deposit [16,20]: %v", err)
	}
	if err := CompleteRecoveryVerify(ctx, pool, lease, chainID, owned); err != nil {
		t.Fatalf("complete: %v", err)
	}

	snap, err := LoadRecoverySnapshot(ctx, pool, chainID)
	if err != nil {
		t.Fatalf("LoadRecoverySnapshot() after release: %v", err)
	}
	if snap.Row != nil {
		t.Fatalf("snapshot row after release = %+v, want nil (observer clears from this shape)", snap.Row)
	}
	if n := reorgCount(t, ctx, pool, "reorg_recovery", chainID); n != 0 {
		t.Fatalf("recovery rows = %d after release, want 0", n)
	}
}

// TestReorgMetricsEvidenceWaits: a detected recovery whose chain reads fail
// counts one wait per bounded wait with the executor's own classes —
// retryable kinds pass through, non-retryable chain failures map to the
// coarse hold classes, and shutdown stays uncounted. Each chain is
// isolated: an unanswered timeout walk legitimately escalates to reconcile
// at the scan floor, which must not leak into the next case.
func TestReorgMetricsEvidenceWaits(t *testing.T) {
	pool := reorgTestPool(t)
	defer pool.Close()
	ctx := context.Background()

	mkChain := func(chainID int64) (*Lease, *RecoveryRow) {
		t.Helper()
		lease := depositITLease(t, pool, chainID)
		reorgSeedChain(t, ctx, pool, chainID, 10, 15, 10)
		reorgEstablishOne(t, ctx, pool, lease, chainID, 15)
		row, err := readRecoveryRow(ctx, pool, chainID)
		if err != nil || row == nil || row.Phase != reorgPhaseDetected {
			t.Fatalf("chain %d row = %+v, %v; want detected", chainID, row, err)
		}
		return lease, row
	}
	mkExecutor := func(chainID int64, lease *Lease, client interface {
		HeaderClient
		LogsClient
	}) (*RecoveryExecutor, *fakeRecoveryMetrics) {
		t.Helper()
		ex, err := NewRecoveryExecutor(pool, lease, client, client, RecoveryConfig{
			ChainID: chainID, MaxDepthRaw: reorgTestDepth,
			PollInterval: 5 * time.Millisecond, RetryInitial: 5 * time.Millisecond, RetryMax: 10 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("NewRecoveryExecutor(): %v", err)
		}
		fake := &fakeRecoveryMetrics{}
		ex.SetRecoveryMetrics(fake)
		return ex, fake
	}

	// Non-retryable chain failure paces the walk with poll waits: a short
	// window stays inside the walk (phase never moves) and every wait
	// counts under its coarse class.
	leaseA, row := mkChain(907103)
	mismatchEx, mismatchFake := mkExecutor(907103, leaseA, failHeaderClient{err: &eth.Error{Kind: eth.KindChainMismatch, Op: "c", Err: context.DeadlineExceeded}})
	mismatchCtx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	if err := mismatchEx.tickDetected(mismatchCtx, row); err != nil {
		t.Fatalf("tickDetected(mismatch chain) = %v, want hold-nil", err)
	}
	if mismatchFake.waits["chain-mismatch"] < 1 {
		t.Fatalf("chain-mismatch waits = %v, want >= 1", mismatchFake.waits)
	}
	if len(mismatchFake.waits) != 1 {
		t.Fatalf("wait classes = %v, want exactly {chain-mismatch}", mismatchFake.waits)
	}
	held, err := readRecoveryRow(ctx, pool, 907103)
	if err != nil || held == nil || held.Phase != reorgPhaseDetected {
		t.Fatalf("row after holds = %+v, %v; want still detected", held, err)
	}

	// An unanswered retryable walk reaches the scan floor and escalates to
	// reconcile_required — waits counted on the way, terminal state clean.
	leaseB, row2 := mkChain(907104)
	timeoutEx, timeoutFake := mkExecutor(907104, leaseB, failHeaderClient{err: &eth.Error{Kind: eth.KindTimeout, Op: "t", Err: context.DeadlineExceeded}})
	timeoutCtx, cancel2 := context.WithTimeout(ctx, 2*time.Second)
	defer cancel2()
	if err := timeoutEx.tickDetected(timeoutCtx, row2); err != nil {
		t.Fatalf("tickDetected(timeout chain) = %v, want reconcile-nil", err)
	}
	if timeoutFake.waits["timeout"] < 1 {
		t.Fatalf("timeout waits = %v, want >= 1", timeoutFake.waits)
	}
	if len(timeoutFake.waits) != 1 {
		t.Fatalf("wait classes = %v, want exactly {timeout}", timeoutFake.waits)
	}
	terminal, err := readRecoveryRow(ctx, pool, 907104)
	if err != nil || terminal == nil || terminal.Phase != reorgPhaseReconcileRequired {
		t.Fatalf("row after unanswered walk = %+v, %v; want reconcile_required", terminal, err)
	}
	snap, err := LoadRecoverySnapshot(ctx, pool, 907104)
	if err != nil || snap.Row == nil || !snap.Reconcile {
		t.Fatalf("terminal snapshot = %+v, %v; want reconcile=true", snap.Row, err)
	}
}
