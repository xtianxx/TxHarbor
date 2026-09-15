//go:build integration

// reorgcapture_test.go banks the B1 regression subset: the four ordinary
// commit paths take a REQUIRED RecoveryCapture (no commit-entry fallback),
// so a batch captured before a recovery round refuses after the round
// releases — on version alone, with fully matching content, zero business
// writes and zero progress. Each test captures the old version, builds its
// inputs from pre-recovery state, establishes a recovery, releases it via
// row DELETE (release mechanics stay pinned in reorgcommit_test.go), then
// commits the stale inputs and demands a recovery-gate refusal; a
// no-recovery control chain proves the same inputs still succeed. It rides
// db.MigrateUp like every other integration file (testcontainers
// postgres:18) and reuses the reorgcommit_test.go harness (same package);
// new helpers carry the cap prefix.
package indexer

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5/pgxpool"
)

// capDeleteRecovery simulates the terminal release (row DELETE) and asserts
// the instance is gone while its event history survives as the version.
func capDeleteRecovery(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64) {
	t.Helper()
	if _, err := pool.Exec(ctx, `DELETE FROM reorg_recovery WHERE chain_id = $1`, chainID); err != nil {
		t.Fatalf("simulate release delete: %v", err)
	}
	if n := reorgCount(t, ctx, pool, "reorg_recovery", chainID); n != 0 {
		t.Fatalf("reorg_recovery rows = %d after release, want 0", n)
	}
}

// TestReorgCaptureBlockStaleRefuses: a block batch captured before the round
// (fully content-matching: checkpoint still at 11 with parent hash 11)
// refuses on version alone after establish+release, writing nothing and
// moving no progress; the no-recovery control still commits.
func TestReorgCaptureBlockStaleRefuses(t *testing.T) {
	pool := reorgTestPool(t)
	defer pool.Close()
	ctx := context.Background()

	const chainID = int64(907011)
	lease := depositITLease(t, pool, chainID)
	reorgSeedChain(t, ctx, pool, chainID, 10, 11, 10)
	sc := &Scanner{pool: pool, chainID: chainID, cfg: Config{StartHeight: 10}, lease: lease}

	stale := testRecoveryCap(t, ctx, pool, chainID)
	w := blockWrite{number: 12, hash: depositBlockHash(12), parent: depositBlockHash(11)}

	reorgEstablishOne(t, ctx, pool, lease, chainID, 11)
	capDeleteRecovery(t, ctx, pool, chainID)

	if err := sc.commitBlock(ctx, w, stale); !isRecoveryGate(err) {
		t.Fatalf("post-release stale commitBlock = %v, want recovery-gate refusal", err)
	}
	var n int64
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM chain_blocks WHERE chain_id = $1 AND number = 12`, chainID).Scan(&n); err != nil {
		t.Fatalf("count height 12: %v", err)
	}
	if n != 0 {
		t.Fatalf("height-12 rows = %d after refused commit, want 0", n)
	}
	cp, ok := readCoordCheckpoint(t, ctx, pool, chainID)
	if !ok || cp.height != 11 || cp.hash != depositBlockHash(11) {
		t.Fatalf("checkpoint = %+v after refused commit, want height 11 hash %s", cp, depositBlockHash(11))
	}

	const controlChain = int64(907012)
	controlLease := depositITLease(t, pool, controlChain)
	reorgSeedChain(t, ctx, pool, controlChain, 10, 11, 10)
	controlSC := &Scanner{pool: pool, chainID: controlChain, cfg: Config{StartHeight: 10}, lease: controlLease}
	fresh := testRecoveryCap(t, ctx, pool, controlChain)
	if err := controlSC.commitBlock(ctx, w, fresh); err != nil {
		t.Fatalf("no-recovery control commitBlock: %v", err)
	}
	cp, ok = readCoordCheckpoint(t, ctx, pool, controlChain)
	if !ok || cp.height != 12 || cp.hash != depositBlockHash(12) {
		t.Fatalf("control checkpoint = %+v, want height 12 hash %s", cp, depositBlockHash(12))
	}
}

// TestReorgCaptureLogRangeStaleRefuses: a log batch captured before the round
// (fully content-matching: checkpoint row still (10, cfg, 12), canonical
// hash 12) refuses on version alone after establish+release, writing no log
// rows and moving no checkpoint; the no-recovery control still commits.
func TestReorgCaptureLogRangeStaleRefuses(t *testing.T) {
	pool := reorgTestPool(t)
	defer pool.Close()
	ctx := context.Background()

	const chainID = int64(907013)
	lease := depositITLease(t, pool, chainID)
	reorgSeedChain(t, ctx, pool, chainID, 10, 12, 10)
	cfgHash := strings.Repeat("ab", 32)
	ls := &LogScanner{pool: pool, chainID: chainID, lease: lease,
		cfg: LogConfig{StartBlock: 10, Contracts: []string{logscanContractA}, ConfigHash: cfgHash, BatchBlocks: 3}}

	stale := testRecoveryCap(t, ctx, pool, chainID)
	coverage := map[uint64]string{12: depositBlockHash(12)}
	rows := []logRow{{
		blockNumber: 12, blockHash: depositBlockHash(12), txHash: depositTxHash(12, 0), logIndex: 0,
		contract: logscanContractA,
		topic0:   "0x" + strings.Repeat("aa", 32),
		topic1:   "0x" + strings.Repeat("bb", 32),
		topic2:   "0x" + strings.Repeat("cc", 32),
		data:     "0x" + strings.Repeat("00", 32),
	}}

	reorgEstablishOne(t, ctx, pool, lease, chainID, 12)
	capDeleteRecovery(t, ctx, pool, chainID)

	if err := ls.commitLogRange(ctx, 12, 12, false, coverage, rows, stale); !isRecoveryGate(err) {
		t.Fatalf("post-release stale commitLogRange = %v, want recovery-gate refusal", err)
	}
	var n int64
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM erc20_transfer_logs WHERE chain_id = $1`, chainID).Scan(&n); err != nil {
		t.Fatalf("count log rows: %v", err)
	}
	if n != 0 {
		t.Fatalf("log rows = %d after refused commit, want 0", n)
	}
	var next int64
	if err := pool.QueryRow(ctx, `SELECT next_block FROM log_checkpoint WHERE chain_id = $1`, chainID).Scan(&next); err != nil {
		t.Fatalf("read log checkpoint: %v", err)
	}
	if next != 12 {
		t.Fatalf("log checkpoint next = %d after refused commit, want 12", next)
	}

	const controlChain = int64(907014)
	controlLease := depositITLease(t, pool, controlChain)
	reorgSeedChain(t, ctx, pool, controlChain, 10, 12, 10)
	controlLS := &LogScanner{pool: pool, chainID: controlChain, lease: controlLease,
		cfg: LogConfig{StartBlock: 10, Contracts: []string{logscanContractA}, ConfigHash: cfgHash, BatchBlocks: 3}}
	fresh := testRecoveryCap(t, ctx, pool, controlChain)
	if err := controlLS.commitLogRange(ctx, 12, 12, false, coverage, rows, fresh); err != nil {
		t.Fatalf("no-recovery control commitLogRange: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT next_block FROM log_checkpoint WHERE chain_id = $1`, controlChain).Scan(&next); err != nil {
		t.Fatalf("read control log checkpoint: %v", err)
	}
	if next != 13 {
		t.Fatalf("control log checkpoint next = %d, want 13", next)
	}
}

// TestReorgCaptureDepositUnitStaleRefuses: a deposit unit captured before the
// round (fully content-matching first unit [10,20]) refuses on version alone
// after establish+release, writing no observation, checkpoint or history
// row; the no-recovery control still commits.
func TestReorgCaptureDepositUnitStaleRefuses(t *testing.T) {
	pool := reorgTestPool(t)
	defer pool.Close()
	ctx := context.Background()

	const chainID = int64(907015)
	cfg := depositITConfig(t, chainID, testContractA)
	depositSeedCanonical(t, ctx, pool, cfg.ChainID, 10, 20, true)
	depositSeedUpstream(t, ctx, pool, cfg.ChainID, 0, cfg.LogConfigHash, 21)
	depositSeedTransferRow(t, ctx, pool, cfg.ChainID, 15, depositBlockHash(15), depositTxHash(15, 0), 0,
		common.HexToAddress(testContractA), common.HexToAddress(testContractB), common.HexToAddress(depositWatchAddr), big.NewInt(1))
	sc := depositITScanner(t, pool, cfg)
	lease := depositITLease(t, pool, cfg.ChainID)

	stale := testRecoveryCap(t, ctx, pool, cfg.ChainID)
	unit, batch, progress := depositITPrepareUnit(t, ctx, sc, 10, 20)
	if progress != nil {
		t.Fatalf("progress = %+v, want empty (first unit)", progress)
	}

	reorgEstablishOne(t, ctx, pool, lease, cfg.ChainID, 20)
	capDeleteRecovery(t, ctx, pool, cfg.ChainID)

	if err := sc.commitDepositUnit(ctx, lease, unit, batch, progress, 10, 20, stale); !isRecoveryGate(err) {
		t.Fatalf("post-release stale commitDepositUnit = %v, want recovery-gate refusal", err)
	}
	if n := depositCountRows(t, ctx, pool, "deposit_observations", cfg.ChainID); n != 0 {
		t.Fatalf("observations = %d after refused commit, want 0", n)
	}
	if _, _, _, ok := depositCheckpointState(t, ctx, pool, cfg.ChainID); ok {
		t.Fatal("deposit checkpoint row exists after refused commit, want none")
	}
	if n := depositCountRows(t, ctx, pool, "deposit_config_history", cfg.ChainID); n != 0 {
		t.Fatalf("history rows = %d after refused commit, want 0", n)
	}

	const controlChain = int64(907016)
	controlCfg := depositITConfig(t, controlChain, testContractA)
	depositSeedCanonical(t, ctx, pool, controlCfg.ChainID, 10, 20, true)
	depositSeedUpstream(t, ctx, pool, controlCfg.ChainID, 0, controlCfg.LogConfigHash, 21)
	depositSeedTransferRow(t, ctx, pool, controlCfg.ChainID, 15, depositBlockHash(15), depositTxHash(15, 0), 0,
		common.HexToAddress(testContractA), common.HexToAddress(testContractB), common.HexToAddress(depositWatchAddr), big.NewInt(1))
	controlSC := depositITScanner(t, pool, controlCfg)
	controlLease := depositITLease(t, pool, controlCfg.ChainID)
	fresh := testRecoveryCap(t, ctx, pool, controlCfg.ChainID)
	controlUnit, controlBatch, controlProgress := depositITPrepareUnit(t, ctx, controlSC, 10, 20)
	if err := controlSC.commitDepositUnit(ctx, controlLease, controlUnit, controlBatch, controlProgress, 10, 20, fresh); err != nil {
		t.Fatalf("no-recovery control commitDepositUnit: %v", err)
	}
	if _, _, next, ok := depositCheckpointState(t, ctx, pool, controlCfg.ChainID); !ok || next != 21 {
		t.Fatalf("control checkpoint next = %d (ok=%v), want 21", next, ok)
	}
}

// TestReorgCaptureConfirmUnitStaleRefuses: a confirmation captured before the
// round refuses on version alone after establish+orphan+release — even
// though the orphaned row would otherwise route to a ChainViewError, the
// refusal must be the recovery gate, never a content verdict — and converts
// nothing; the no-recovery control still converts.
func TestReorgCaptureConfirmUnitStaleRefuses(t *testing.T) {
	pool := reorgTestPool(t)
	defer pool.Close()
	ctx := context.Background()

	const chainID, h, tip, n = int64(907017), uint64(100), uint64(109), uint64(10)
	depositSeedCanonical(t, ctx, pool, chainID, 90, tip, true)
	bh, txHash := confirmSeedPending(t, ctx, pool, chainID, h)
	confirmSeedPolicyRow(t, ctx, pool, chainID, 1, int64(n), nil, "bootstrap", nil)
	c, lease := confirmCommitter(t, pool, chainID, n)

	stale := testRecoveryCap(t, ctx, pool, chainID)
	basis := ConfirmBasis{BlockHash: bh, TxHash: txHash, Height: h,
		TipNumber: tip, TipHash: depositBlockHash(tip), PolicySeq: 1, ThresholdN: n}

	// Drive the round far enough to orphan the candidate (the content path
	// that would otherwise answer ChainViewError), then release.
	res := reorgEstablishOne(t, ctx, pool, lease, chainID, tip)
	owned := RecoveryCapture{Seq: res.Seq, Owned: &RecoveryOwned{RecoveryID: res.RecoveryID}}
	if err := ConfirmRecoveryAncestor(ctx, pool, lease, chainID, owned, 95, depositBlockHash(95), "cap-orphan"); err != nil {
		t.Fatalf("confirm ancestor: %v", err)
	}
	if _, _, _, err := InvalidateRecoveryBlocks(ctx, pool, lease, chainID, owned); err != nil {
		t.Fatalf("invalidate blocks: %v", err)
	}
	if orphaned, err := InvalidateRecoveryObservations(ctx, pool, lease, chainID, owned); err != nil || orphaned != 1 {
		t.Fatalf("invalidate observations = (%d, %v), want exactly the height-100 row orphaned", orphaned, err)
	}
	capDeleteRecovery(t, ctx, pool, chainID)

	err := c.ConfirmDepositUnit(ctx, lease, basis, stale)
	if !isRecoveryGate(err) {
		t.Fatalf("post-release stale ConfirmDepositUnit = %v, want recovery-gate refusal", err)
	}
	var chainView *ConfirmationChainViewError
	if errors.As(err, &chainView) {
		t.Fatalf("post-release stale ConfirmDepositUnit = %v, want gate refusal, not a content verdict", err)
	}
	status, nullAt, tipN, _, _, _, conf := confirmReadBasis(t, ctx, pool, chainID, bh, txHash)
	if status != "orphaned" || !nullAt || tipN != -1 || conf != "" {
		t.Fatalf("candidate = (%s nullAt=%v tip=%d conf=%q), want orphaned with no conversion facts", status, nullAt, tipN, conf)
	}
	if k := depositCountRows(t, ctx, pool, "confirmation_policy_history", chainID); k != 1 {
		t.Fatalf("confirmation_policy_history rows = %d, want 1 (refusal writes nothing)", k)
	}

	const controlChain = int64(907018)
	depositSeedCanonical(t, ctx, pool, controlChain, h, tip, true)
	controlBH, controlTx := confirmSeedPending(t, ctx, pool, controlChain, h)
	confirmSeedPolicyRow(t, ctx, pool, controlChain, 1, int64(n), nil, "bootstrap", nil)
	controlC, controlLease := confirmCommitter(t, pool, controlChain, n)
	fresh := testRecoveryCap(t, ctx, pool, controlChain)
	controlBasis := ConfirmBasis{BlockHash: controlBH, TxHash: controlTx, Height: h,
		TipNumber: tip, TipHash: depositBlockHash(tip), PolicySeq: 1, ThresholdN: n}
	if err := controlC.ConfirmDepositUnit(ctx, controlLease, controlBasis, fresh); err != nil {
		t.Fatalf("no-recovery control ConfirmDepositUnit: %v", err)
	}
	status, _, _, _, _, _, _ = confirmReadBasis(t, ctx, pool, controlChain, controlBH, controlTx)
	if status != "confirmed" {
		t.Fatalf("control status = %s, want confirmed", status)
	}
}
