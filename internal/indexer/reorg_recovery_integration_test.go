//go:build integration

// reorg_recovery_integration_test.go carries the Batch C US1 E2Easchematic (T020,
// T021, V3 + V7-part) on real PostgreSQL + real Anvil (testcontainers,
// foundry v1.8.1, chain-id 31337 — the logscanStartAnvilNode pattern and the
// T007 TestDepositAnvilFullStackPending precedent). Later batches extend THIS
// file: T022 (fork shapes) and T024 (skew/start/empty) add new tests plus
// shared-scene fields, never renames of the rrec helpers below.
//
// Shape under test (US1): shallow fork over Pending P + Confirmed C with an
// ancestor-side Confirmed K. Snapshot → mine fork A → index via the REAL
// 002/003/004/005 loops → revert → mine fork B (same heights, new hashes).
// Every recovery phase transition goes through the REAL RecoveryExecutor
// ticks + the real reorgcommit.go transactions; the test seeds chain +
// initial DB state (and the two tickIdle inputs: a hash_mismatch pause row
// with genuine A/B hashes plus the confirmation tip from indexed fork A)
// but never hand-edits canonical flags, observations, checkpoints, or
// recovery phases to simulate the flow.
package indexer

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/eth"
	"github.com/xtianxx/txharbor/internal/metrics"
)

// --- rrec shared scene -------------------------------------------------------

const (
	rrecMaxDepth   = "25"
	rrecThresholdN = uint64(4)
)

// rrecScene is the US1 Anvil scene shared by T020/T021 (own DB per test;
// chain_id is 31337 to match Anvil — isolation comes from separate
// testcontainers, so both tests stay independently runnable).
type rrecScene struct {
	pool   *pgxpool.Pool
	dsn    string
	node   *logscanAnvilNode
	client *eth.Client
	lease  *Lease

	chainID int64

	watch       common.Address
	tokenK      common.Address
	tokenC      common.Address
	tokenP      common.Address
	tokenB      common.Address
	tokens      []string
	depositHash string
	logHash     string

	hK, hA, hC, hP, hTip uint64
	aHashes              map[uint64]string // fork-A canonical hashes (hA..hTip)
	bHashes              map[uint64]string // fork-B canonical hashes (hA+1..hTip)
	kTx, cTx, pTx, bTx   common.Hash
	kBH, cBH, pBH, bBH   string // deposit source block hashes (A-chain / B-chain)
	snapID               string
}

// rrecFailLogs is a LogsClient that always fails: one tickReplay through it
// must fail loud with zero frontier movement (Batch-B FilterLogs discipline,
// asserted inside the US1 loop — never worked around).
type rrecFailLogs struct{}

func (rrecFailLogs) FilterLogs(_ context.Context, _ ethereum.FilterQuery) ([]types.Log, error) {
	return nil, errors.New("rrec fail-loud: FilterLogs boom")
}

// rrecSetup provisions one PG container + one Anvil node + the eth client.
func rrecSetup(t *testing.T, owner string) *rrecScene {
	t.Helper()
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	t.Cleanup(pool.Close)
	ctx := context.Background()

	node := logscanStartAnvilNode(t)
	client, err := eth.Dial(ctx, node.url, 5*time.Second)
	if err != nil {
		t.Fatalf("dial anvil client: %v", err)
	}
	t.Cleanup(client.Close)

	// One lease owner for the whole test: only 002's Run acquires (the
	// 003/004/005 ServeLoops and the 006 txns present owner+token under the
	// lock). TTL is an hour so no expiry race can interrupt the loop; resume
	// reuses these same credentials on a fresh pool (restart-with-persisted-
	// credentials, never a second owner).
	s := &rrecScene{
		pool: pool, dsn: dsn, node: node, client: client,
		chainID: scanChainID,
		lease:   newTestLease(t, pool, scanChainID, owner, time.Hour, 10*time.Second),
		watch:   common.HexToAddress("0x00000000000000000000000000000000000000d1"),
		tokenK:  common.HexToAddress("0x00000000000000000000000000000000000000a1"),
		tokenC:  common.HexToAddress("0x00000000000000000000000000000000000000a2"),
		tokenP:  common.HexToAddress("0x00000000000000000000000000000000000000a3"),
		tokenB:  common.HexToAddress("0x00000000000000000000000000000000000000a4"),
		aHashes: map[uint64]string{},
		bHashes: map[uint64]string{},
	}
	s.tokens = []string{
		strings.ToLower(s.tokenK.Hex()), strings.ToLower(s.tokenC.Hex()),
		strings.ToLower(s.tokenP.Hex()), strings.ToLower(s.tokenB.Hex()),
	}
	s.depositHash = strings.Repeat("33", 32)
	s.logHash = depositUpstreamHash(t, s.tokens...)
	return s
}

// rrecHeaderHash reads one real Anvil header hash (lowercased Table 1 form).
func rrecHeaderHash(t *testing.T, ctx context.Context, s *rrecScene, h uint64) string {
	t.Helper()
	hdr, err := s.client.HeaderByNumber(ctx, new(big.Int).SetUint64(h))
	if err != nil {
		t.Fatalf("anvil header %d: %v", h, err)
	}
	return hashHex(hdr.Hash())
}

// rrecEmit installs one fallback token whose every call emits exactly one
// Transfer(sender → watch, amount) log (T007 precedent, no forge).
func rrecEmit(t *testing.T, s *rrecScene, token common.Address, amount *big.Int) {
	t.Helper()
	topics := [3]common.Hash{eth.TransferSig}
	topics[1] = common.BytesToHash(s.node.accounts[0].Bytes())
	topics[2] = common.BytesToHash(s.watch.Bytes())
	s.node.setCode(t, token, logscanEmitCode(topics, amount))
}

// rrecMineForkA builds fork A: K (ancestor-side) → empties → snapshot at the
// ancestor → C → empties → P → empties to the tip. Heights come from receipts
// and mined-head reads, never assumptions.
func rrecMineForkA(t *testing.T, ctx context.Context, s *rrecScene) {
	t.Helper()
	rrecEmit(t, s, s.tokenK, big.NewInt(7))
	rrecEmit(t, s, s.tokenC, big.NewInt(5))
	rrecEmit(t, s, s.tokenP, big.NewInt(9))
	rrecEmit(t, s, s.tokenB, big.NewInt(11))

	s.kTx, s.hK = s.node.sendTransferAt(t, s.tokenK)
	if got := s.node.blockNumber(t); got != s.hK {
		t.Fatalf("anvil head = %d after K, want receipt height %d", got, s.hK)
	}
	// Ancestor six blocks above K so K confirms deeply while C/P straddle N.
	s.node.mine(t, (s.hK+6)-s.node.blockNumber(t))
	s.hA = s.node.blockNumber(t)
	if s.hA != s.hK+6 {
		t.Fatalf("ancestor height = %d, want %d", s.hA, s.hK+6)
	}
	var snap string
	s.node.mustCall(t, &snap, "evm_snapshot")
	if snap == "" {
		t.Fatal("evm_snapshot returned an empty id")
	}
	s.snapID = snap

	s.cTx, s.hC = s.node.sendTransferAt(t, s.tokenC)
	if s.hC != s.hA+1 {
		t.Fatalf("hC = %d, want ancestor+1 = %d", s.hC, s.hA+1)
	}
	s.node.mine(t, 2)
	s.pTx, s.hP = s.node.sendTransferAt(t, s.tokenP)
	if s.hP != s.hC+3 {
		t.Fatalf("hP = %d, want hC+3 = %d", s.hP, s.hC+3)
	}
	s.node.mine(t, 2)
	s.hTip = s.node.blockNumber(t)
	if s.hTip != s.hP+2 {
		t.Fatalf("fork-A tip = %d, want hP+2 = %d", s.hTip, s.hP+2)
	}
	for h := s.hA; h <= s.hTip; h++ {
		s.aHashes[h] = rrecHeaderHash(t, ctx, s, h)
	}
	s.kBH = rrecHeaderHash(t, ctx, s, s.hK)
	s.cBH, s.pBH = s.aHashes[s.hC], s.aHashes[s.hP]
}

// rrecIndexForkA runs the REAL 002 → 003 → 004 → 005 loops over fork A and
// pins the pre-fork durable state: K + C Confirmed (exact 005 basis), P
// Pending, checkpoints at the tip, block/log identity against Anvil.
func rrecIndexForkA(t *testing.T, ctx context.Context, s *rrecScene) {
	t.Helper()
	sc002, err := NewScanner(s.pool, s.client, s.lease, Config{
		StartHeight: 0, RPCTimeout: 2 * time.Second, PollInterval: 25 * time.Millisecond,
		RetryInitial: 25 * time.Millisecond, RetryMax: 250 * time.Millisecond,
	}, logscanLogger())
	if err != nil {
		t.Fatalf("NewScanner(): %v", err)
	}
	runScanTo(t, sc002, s.hTip, 60*time.Second)

	ls := logscanNewScanner(t, s.pool, s.client, s.client, s.lease, LogConfig{
		StartBlock: 0, Contracts: s.tokens, ConfigHash: s.logHash, BatchBlocks: 2,
	})
	logscanServeTo(t, ctx, s.pool, s.chainID, ls, uint64(int64(s.hTip)+1), 60*time.Second)

	cfg := rrecDepositCfg(s)
	sc004, err := NewDepositScanner(s.pool, cfg)
	if err != nil {
		t.Fatalf("NewDepositScanner(): %v", err)
	}
	stop004 := depositRunLoop(t, ctx, sc004, s.lease)
	waitUntil(t, time.Now().Add(60*time.Second), "deposit checkpoint reaches tip+1", func() bool {
		_, _, next, ok := depositCheckpointState(t, ctx, s.pool, s.chainID)
		return ok && next >= s.hTip+1
	})
	time.Sleep(100 * time.Millisecond)
	stop004()

	m := metrics.New(func() bool { return true })
	confirmCfg := ConfirmationConfig{
		ChainID:      s.chainID,
		ThresholdN:   rrecThresholdN,
		PollInterval: 25 * time.Millisecond,
		RetryInitial: 25 * time.Millisecond,
		RetryMax:     250 * time.Millisecond,
	}
	committer005, err := NewConfirmationCommitter(s.pool, confirmCfg)
	if err != nil {
		t.Fatalf("NewConfirmationCommitter(): %v", err)
	}
	sc005, err := NewConfirmationScanner(s.pool, confirmCfg, committer005, m)
	if err != nil {
		t.Fatalf("NewConfirmationScanner(): %v", err)
	}
	stop005 := confirm13RunLoop(t, ctx, sc005, s.lease)
	waitUntil(t, time.Now().Add(60*time.Second), "K+C confirmed, P pending", func() bool {
		var kc, cc, pc int
		_ = s.pool.QueryRow(ctx, `SELECT count(*) FROM deposit_observations WHERE chain_id=$1 AND tx_hash=$2 AND status='confirmed'`,
			s.chainID, strings.ToLower(s.kTx.Hex())).Scan(&kc)
		_ = s.pool.QueryRow(ctx, `SELECT count(*) FROM deposit_observations WHERE chain_id=$1 AND tx_hash=$2 AND status='confirmed'`,
			s.chainID, strings.ToLower(s.cTx.Hex())).Scan(&cc)
		_ = s.pool.QueryRow(ctx, `SELECT count(*) FROM deposit_observations WHERE chain_id=$1 AND tx_hash=$2 AND status='pending'`,
			s.chainID, strings.ToLower(s.pTx.Hex())).Scan(&pc)
		return kc == 1 && cc == 1 && pc == 1
	})
	time.Sleep(200 * time.Millisecond)
	stop005()

	// Checkpoint identity: 002 at the tip, 003/004 watermarks at tip+1.
	var cpH int64
	var cpHash string
	if err := s.pool.QueryRow(ctx, `SELECT height, block_hash FROM indexer_checkpoint WHERE chain_id=$1`,
		s.chainID).Scan(&cpH, &cpHash); err != nil {
		t.Fatalf("read indexer_checkpoint: %v", err)
	}
	if cpH != int64(s.hTip) || cpHash != s.aHashes[s.hTip] {
		t.Fatalf("002 checkpoint = (%d %s), want (%d %s)", cpH, cpHash, s.hTip, s.aHashes[s.hTip])
	}
	if next, ok := logscanCheckpointNext(ctx, s.pool, s.chainID); !ok || next != s.hTip+1 {
		t.Fatalf("log checkpoint next = %d (ok=%v), want %d", next, ok, s.hTip+1)
	}
	if _, _, next, ok := depositCheckpointState(t, ctx, s.pool, s.chainID); !ok || next != s.hTip+1 {
		t.Fatalf("deposit checkpoint next = %d (ok=%v), want %d", next, ok, s.hTip+1)
	}
	// Block identity per deposit height against the real Anvil headers.
	for h, bh := range map[uint64]string{s.hK: s.kBH, s.hC: s.cBH, s.hP: s.pBH} {
		var canon string
		if err := s.pool.QueryRow(ctx, `SELECT hash FROM chain_blocks WHERE chain_id=$1 AND number=$2 AND canonical`,
			s.chainID, int64(h)).Scan(&canon); err != nil {
			t.Fatalf("canonical block %d: %v", h, err)
		}
		if canon != bh {
			t.Fatalf("chain_blocks[%d] = %s, want anvil %s", h, canon, bh)
		}
		var n int
		if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM erc20_transfer_logs WHERE chain_id=$1 AND block_number=$2`,
			s.chainID, int64(h)).Scan(&n); err != nil || n != 1 {
			t.Fatalf("003 rows at %d = %d (err=%v), want 1", h, n, err)
		}
	}
	// 005 basis exactness pre-fork (live-policy conditions, then-threshold).
	tipHash := s.aHashes[s.hTip]
	for _, tc := range []struct {
		h  uint64
		tx common.Hash
	}{{s.hK, s.kTx}, {s.hC, s.cTx}} {
		status, nullAt, tipN, thr, seq, gotTip, conf := confirmReadBasis(t, ctx, s.pool, s.chainID,
			rrecHeaderHash(t, ctx, s, tc.h), strings.ToLower(tc.tx.Hex()))
		wantConf := fmt.Sprintf("%d", s.hTip-tc.h+1)
		if status != "confirmed" || nullAt || tipN != int64(s.hTip) || gotTip != tipHash ||
			thr != int64(rrecThresholdN) || seq != 1 || conf != wantConf {
			t.Fatalf("h=%d pre-fork basis = (%s null=%v tip %d %s N=%d conf=%s seq=%d), want confirmed tip(%d %s) N=%d conf=%s seq=1",
				tc.h, status, nullAt, tipN, gotTip, thr, conf, seq, s.hTip, tipHash, rrecThresholdN, wantConf)
		}
	}
}

// rrecForkB reverts to the ancestor snapshot and mines fork B to the SAME tip
// height with all-new hashes (one re-mined watched transfer at hA+1, empties
// after). The divergence is genuine Anvil history, never edited DB rows.
func rrecForkB(t *testing.T, ctx context.Context, s *rrecScene) {
	t.Helper()
	var ok bool
	s.node.mustCall(t, &ok, "evm_revert", s.snapID)
	if !ok {
		t.Fatal("evm_revert refused the ancestor snapshot")
	}
	if got := s.node.blockNumber(t); got != s.hA {
		t.Fatalf("anvil head after revert = %d, want ancestor %d", got, s.hA)
	}
	bTx, bH := s.node.sendTransferAt(t, s.tokenB)
	s.bTx = bTx
	if bH != s.hA+1 {
		t.Fatalf("fork-B transfer height = %d, want %d", bH, s.hA+1)
	}
	s.node.mine(t, s.hTip-bH)
	if got := s.node.blockNumber(t); got != s.hTip {
		t.Fatalf("fork-B tip = %d, want %d", got, s.hTip)
	}
	for h := s.hA + 1; h <= s.hTip; h++ {
		s.bHashes[h] = rrecHeaderHash(t, ctx, s, h)
		if s.bHashes[h] == s.aHashes[h] {
			t.Fatalf("fork-B hash at %d equals fork-A hash %s: no divergence", h, s.aHashes[h])
		}
	}
	s.bBH = s.bHashes[s.hA+1]
	// Prefix above the ancestor is untouched by the revert (ancestor intact).
	if got := rrecHeaderHash(t, ctx, s, s.hA); got != s.aHashes[s.hA] {
		t.Fatalf("ancestor hash moved: %s vs %s", got, s.aHashes[s.hA])
	}
	// Genuine fork evidence from both sources: DB canonical tip (fork A)
	// disagrees with the live chain at the same height.
	var dbTipN int64
	var dbTipHash string
	if err := s.pool.QueryRow(ctx, `SELECT number, hash FROM chain_blocks WHERE chain_id=$1 AND canonical ORDER BY number DESC LIMIT 1`,
		s.chainID).Scan(&dbTipN, &dbTipHash); err != nil {
		t.Fatalf("read db tip: %v", err)
	}
	if dbTipN != int64(s.hTip) || dbTipHash != s.aHashes[s.hTip] {
		t.Fatalf("db tip = (%d %s), want fork-A tip (%d %s)", dbTipN, dbTipHash, s.hTip, s.aHashes[s.hTip])
	}
	if live := rrecHeaderHash(t, ctx, s, s.hTip); live == dbTipHash {
		t.Fatalf("chain tip %s still equals db tip: fork did not land", live)
	}
}

// rrecSeedPauses writes the two tickIdle inputs with genuine values: the
// fork-evidence hash_mismatch pause (expected/fork-A vs actual/fork-B at
// hA+1) plus one INDEPENDENT deposit-stream pause that must survive release.
func rrecSeedPauses(t *testing.T, ctx context.Context, s *rrecScene) {
	t.Helper()
	if _, err := s.pool.Exec(ctx, `
INSERT INTO indexer_pause (chain_id, height, expected_hash, actual_hash, kind, detail)
VALUES ($1, $2, $3, $4, 'hash_mismatch', 'rrec US1 fork evidence: stored fork-A vs live fork-B')`,
		s.chainID, int64(s.hA+1), s.aHashes[s.hA+1], s.bHashes[s.hA+1]); err != nil {
		t.Fatalf("seed indexer_pause: %v", err)
	}
	if _, err := s.pool.Exec(ctx, `
INSERT INTO deposit_pause (chain_id, height, kind, detail)
VALUES ($1, $2, 'chain_view_changed', 'rrec independent stream pause: must survive recovery release')`,
		s.chainID, int64(s.hA+1)); err != nil {
		t.Fatalf("seed deposit_pause: %v", err)
	}
	var kind string
	var height int64
	var expected, actual string
	if err := s.pool.QueryRow(ctx, `SELECT height, expected_hash, actual_hash, kind FROM indexer_pause WHERE chain_id=$1`,
		s.chainID).Scan(&height, &expected, &actual, &kind); err != nil {
		t.Fatalf("read seeded pause: %v", err)
	}
	if kind != "hash_mismatch" || height != int64(s.hA+1) || expected != s.aHashes[s.hA+1] || actual != s.bHashes[s.hA+1] {
		t.Fatalf("pause = (%d %s %s %s), want (%d A B hash_mismatch)", height, expected, actual, kind, s.hA+1)
	}
}

// rrecExecutor builds the REAL recovery executor on the given pool/lease.
func rrecExecutor(t *testing.T, pool *pgxpool.Pool, lease *Lease, s *rrecScene) *RecoveryExecutor {
	t.Helper()
	return rrecExecutorBatch(t, pool, lease, s, 0)
}

// rrecExecutorBatch is rrecExecutor with an explicit replay batch width (0
// = executor default). Narrow widths force multi-tick replays so
// repeat-execution checks can interleave before completion.
func rrecExecutorBatch(t *testing.T, pool *pgxpool.Pool, lease *Lease, s *rrecScene, batch int) *RecoveryExecutor {
	t.Helper()
	ex, err := NewRecoveryExecutor(pool, lease, s.client, s.client, RecoveryConfig{
		ChainID: s.chainID, MaxDepthRaw: rrecMaxDepth, BlockStartHeight: 0, ReplayHeights: batch,
	})
	if err != nil {
		t.Fatalf("NewRecoveryExecutor(): %v", err)
	}
	return ex
}

// rrecRow re-reads the durable recovery row + the owned capture for it
// (capture-first: the capture always comes from a fresh durable read).
func rrecRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64) (*RecoveryRow, RecoveryCapture) {
	t.Helper()
	row, err := LoadRecoveryState(ctx, pool, chainID)
	if err != nil {
		t.Fatalf("LoadRecoveryState(): %v", err)
	}
	if row == nil {
		t.Fatal("no active recovery row where the phase machine must hold one")
	}
	return row, RecoveryCapture{Seq: row.Seq, Owned: &RecoveryOwned{RecoveryID: row.RecoveryID}}
}

// rrecEventCount counts one audit event for the chain (0 allowed).
func rrecEventCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, event string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM reorg_recovery_events WHERE chain_id=$1 AND event=$2`,
		chainID, event).Scan(&n); err != nil {
		t.Fatalf("count event %s: %v", event, err)
	}
	return n
}

// rrecObs reads one observation's status + basis + orphan evidence.
type rrecObs struct {
	status          string
	blockNumber     int64
	amount          string
	version         int64
	confirmedAtNull bool
	tipNumber       int64
	tipHash         string
	threshold       int64
	confirmations   string
	policySeq       int64
	orphanID        string
	orphanedAtNull  bool
}

func rrecReadObs(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, bh, tx string) rrecObs {
	t.Helper()
	return rrecReadObsIdx(t, ctx, pool, chainID, bh, tx, 0)
}

// rrecReadObsIdx reads one observation identity; re-mined variants can live
// at nonzero log indices (the plain helper pins index 0 and would miss
// them, reporting a false absence).
func rrecReadObsIdx(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, bh, tx string, idx int64) rrecObs {
	t.Helper()
	var o rrecObs
	var confirmedAt, orphanedAt *string
	var tipN, thr, pseq *int64
	var tipH, conf *string
	err := pool.QueryRow(ctx, `
SELECT status, block_number, amount::text, version_seq,
       confirmed_at::text, confirm_tip_number, confirm_tip_hash, confirm_threshold,
       confirmations::text, confirm_policy_seq,
       COALESCE(orphan_recovery_id, ''), orphaned_at::text
FROM deposit_observations WHERE chain_id=$1 AND block_hash=$2 AND tx_hash=$3 AND log_index=$4`,
		chainID, bh, tx, idx).Scan(
		&o.status, &o.blockNumber, &o.amount, &o.version,
		&confirmedAt, &tipN, &tipH, &thr, &conf, &pseq,
		&o.orphanID, &orphanedAt)
	if err != nil {
		t.Fatalf("read observation %s/%s: %v", bh, tx, err)
	}
	o.confirmedAtNull, o.orphanedAtNull = confirmedAt == nil, orphanedAt == nil
	if tipN != nil {
		o.tipNumber = *tipN
		o.tipHash = *tipH
		o.threshold = *thr
		o.confirmations = *conf
		o.policySeq = *pseq
	}
	return o
}

// rrecTransitionCount counts conversions for one recovery.
func rrecTransitionCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, from, to, recoveryID string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM deposit_observation_transitions
WHERE chain_id=$1 AND from_status=$2 AND to_status=$3 AND recovery_id=$4`,
		chainID, from, to, recoveryID).Scan(&n); err != nil {
		t.Fatalf("count transitions %s->%s: %v", from, to, err)
	}
	return n
}

// rrecRunScanTo runs a header ServeLoop until the in-memory checkpoint
// reaches want, then cancels and requires a clean nil return. It drives
// ServeLoop (not Run): the test's lease row is already live and owned by
// this scene, and Run's singleton acquire stands by on any unexpired row —
// even its own — so a second Run can never proceed here. All write-path
// adjudication (pause/lease/gate/exact-guard) still runs inside
// commitBlock; only the multi-instance fencing step is vacuous in-test.
// This mirrors logscanServeTo/depositRunLoop/confirm13RunLoop, which all
// drive ServeLoop directly for the same reason.
func rrecRunScanTo(t *testing.T, sc *Scanner, want uint64, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- sc.ServeLoop(ctx, nil) }()
	waitUntil(t, time.Now().Add(timeout), fmt.Sprintf("scan checkpoint reaches %d", want), func() bool {
		h, _, ok := sc.Checkpoint()
		return ok && h >= want
	})
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ServeLoop() after cancel = %v, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("ServeLoop() did not return after cancel")
	}
}

// rrecDepositCfg is the 004 configuration over the scene whitelist (start
// 0, historical version bootstrap owned by the first scanner run).
func rrecDepositCfg(s *rrecScene) DepositConfig {
	return DepositConfig{
		ChainID: s.chainID, StartBlock: 0,
		Assets: []config.DepositEntry{
			{Address: s.tokens[0], Effective: 0}, {Address: s.tokens[1], Effective: 0},
			{Address: s.tokens[2], Effective: 0}, {Address: s.tokens[3], Effective: 0},
		},
		Watches:        []config.DepositEntry{{Address: strings.ToLower(s.watch.Hex()), Effective: 0}},
		ConfigHash:     s.depositHash,
		BatchBlocks:    500,
		PollInterval:   25 * time.Millisecond,
		RetryInitial:   25 * time.Millisecond,
		RetryMax:       250 * time.Millisecond,
		LogContracts:   s.tokens,
		LogConfigHash:  s.logHash,
		LogStartHeight: 0,
	}
}

// --- T020: US1 close loop ----------------------------------------------------

// TestReorgRecoveryUS1CloseLoop is T020 (FR-01/02/05/06/07/09/10/19, V3):
// establish+pause atomic and restart-persistent; affected P+C 100% Orphaned
// with transition + event audit; ancestor-side K untouched with basis intact;
// post-ancestor replay regenerates the fork-B observation under historical
// semantics; 005 reconfirmation meets ALL live-policy conditions with zero
// old-basis reuse; auto-release only after re-verified FR-19判据 with the
// mandatory terminal detail; SC-01 green.
func TestReorgRecoveryUS1CloseLoop(t *testing.T) {
	s := rrecSetup(t, "rrec-us1-loop")
	ctx := context.Background()

	rrecMineForkA(t, ctx, s)
	rrecIndexForkA(t, ctx, s)

	// Capture-first: the pre-recovery version (events MAX = 0) for the
	// in-loop stale-ordinary proof below.
	cap0 := testRecoveryCap(t, ctx, s.pool, s.chainID)
	if cap0.Seq != 0 {
		t.Fatalf("pre-recovery capture seq = %d, want 0", cap0.Seq)
	}
	// Pre-fork K/C basis snapshots for the untouched / zero-reuse proofs.
	kStatus, kNull, kTipN, kThr, kSeq, kTipHash, kConf := confirmReadBasis(t, ctx, s.pool, s.chainID,
		s.kBH, strings.ToLower(s.kTx.Hex()))
	cStatus, cNull, cTipN, cThr, cSeq, cTipHash, cConf := confirmReadBasis(t, ctx, s.pool, s.chainID,
		s.cBH, strings.ToLower(s.cTx.Hex()))
	if kStatus != "confirmed" || cStatus != "confirmed" || kNull || cNull {
		t.Fatalf("pre-fork K=(%s null=%v) C=(%s null=%v), want both confirmed", kStatus, kNull, cStatus, cNull)
	}

	rrecForkB(t, ctx, s)
	rrecSeedPauses(t, ctx, s)

	ex := rrecExecutor(t, s.pool, s.lease, s)

	// establish via the REAL executor trigger (pause + bound tip + live tip).
	done, err := ex.tickIdle(ctx)
	if err != nil {
		t.Fatalf("tickIdle(): %v", err)
	}
	if !done {
		t.Fatal("tickIdle() = false with a hash_mismatch pause + tips present")
	}
	row, cap := rrecRow(t, ctx, s.pool, s.chainID)
	if row.Phase != reorgPhaseDetected {
		t.Fatalf("phase = %q, want detected", row.Phase)
	}
	if row.BoundOldNumber != int64(s.hTip) || row.BoundOldHash != s.aHashes[s.hTip] {
		t.Fatalf("bound tip = (%d %s), want (%d %s)", row.BoundOldNumber, row.BoundOldHash, s.hTip, s.aHashes[s.hTip])
	}
	if row.PolicySeq != 1 || row.MaxDepth != 25 || row.Seq != 1 {
		t.Fatalf("bound = (policy=%d depth=%d seq=%d), want (1 25 1)", row.PolicySeq, row.MaxDepth, row.Seq)
	}
	if n := rrecEventCount(t, ctx, s.pool, s.chainID, "established"); n != 1 {
		t.Fatalf("established events = %d, want 1 (row+event atomic, exactly once)", n)
	}
	// Establish writes NO pause table (R9): the seeded rows are recorded in
	// the event detail, never modified.
	var estDetail string
	if err := s.pool.QueryRow(ctx, `SELECT detail FROM reorg_recovery_events WHERE chain_id=$1 AND event='established'`,
		s.chainID).Scan(&estDetail); err != nil {
		t.Fatalf("read established detail: %v", err)
	}
	for _, want := range []string{"phase=detected", "policy=1", "version=1", "pre_pauses=indexer_pause,deposit_pause"} {
		if !strings.Contains(estDetail, want) {
			t.Fatalf("established detail missing %q: %q", want, estDetail)
		}
	}
	var pauseN int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM indexer_pause WHERE chain_id=$1`, s.chainID).Scan(&pauseN); err != nil || pauseN != 1 {
		t.Fatalf("indexer_pause rows = %d (err=%v), want 1 untouched", pauseN, err)
	}
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM deposit_pause WHERE chain_id=$1`, s.chainID).Scan(&pauseN); err != nil || pauseN != 1 {
		t.Fatalf("deposit_pause rows = %d (err=%v), want 1 untouched", pauseN, err)
	}
	// Restart-persistence: a fresh pool sees the identical persisted phase.
	probe := openIndexerPool(t, s.dsn)
	defer probe.Close()
	prow, err := LoadRecoveryState(ctx, probe, s.chainID)
	if err != nil || prow == nil {
		t.Fatalf("fresh-pool LoadRecoveryState() = %+v, %v; want the detected row", prow, err)
	}
	if prow.RecoveryID != row.RecoveryID || prow.Seq != row.Seq || prow.Phase != reorgPhaseDetected {
		t.Fatalf("fresh-pool row = %+v, want %+v", prow, row)
	}

	// In-loop stale-ordinary proofs (capture-first). (a) Primitive: the
	// pre-round capture refuses straight through the gate although its
	// content still matches — version alone decides, content never
	// consulted. (b) Ordinary 005 wrapper: the same stale batch stays
	// stopped on the seeded stream pauses (first-line fencing order), with
	// zero writes to the P row.
	func() {
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin gate probe: %v", err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if _, err := recheckRecoveryGate(ctx, tx, s.chainID, cap0); !isRecoveryGate(err) {
			t.Fatalf("gate primitive with pre-round capture = %v, want refusal", err)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatalf("rollback gate probe: %v", err)
		}
	}()
	committer, err := NewConfirmationCommitter(s.pool, ConfirmationConfig{ChainID: s.chainID, ThresholdN: rrecThresholdN})
	if err != nil {
		t.Fatalf("NewConfirmationCommitter(): %v", err)
	}
	staleBasis := ConfirmBasis{
		BlockHash: s.pBH, TxHash: strings.ToLower(s.pTx.Hex()), LogIndex: 0, Height: uint64(s.hP),
		TipNumber: uint64(s.hTip), TipHash: s.aHashes[s.hTip], PolicySeq: 1, ThresholdN: rrecThresholdN,
	}
	if err := committer.ConfirmDepositUnit(ctx, s.lease, staleBasis, cap0); err == nil || !strings.Contains(err.Error(), "deposit_pause") {
		t.Fatalf("stale ordinary confirm during recovery = %v, want the deposit_pause stop", err)
	}
	if o := rrecReadObs(t, ctx, s.pool, s.chainID, s.pBH, strings.ToLower(s.pTx.Hex())); o.status != "pending" {
		t.Fatalf("P after refused stale confirm = %s, want pending (zero writes)", o.status)
	}

	// ancestor_confirm via the REAL executor search (local+chain dual
	// equality over the genuine Anvil fork).
	if err := ex.tickDetected(ctx, row); err != nil {
		t.Fatalf("tickDetected(): %v", err)
	}
	row, cap = rrecRow(t, ctx, s.pool, s.chainID)
	if row.Phase != reorgPhaseAncestorConfirmed {
		t.Fatalf("phase = %q, want ancestor_confirmed", row.Phase)
	}
	if row.AncestorNumber == nil || *row.AncestorNumber != int64(s.hA) || row.AncestorHash == nil || *row.AncestorHash != s.aHashes[s.hA] {
		t.Fatalf("ancestor = (%v %v), want (%d %s)", row.AncestorNumber, row.AncestorHash, s.hA, s.aHashes[s.hA])
	}
	if depth, err := reorgDepth(row.BoundOldNumber, *row.AncestorNumber); err != nil || depth != int64(s.hTip-s.hA) {
		t.Fatalf("depth = %d (err=%v), want %d", depth, err, s.hTip-s.hA)
	}
	if n := rrecEventCount(t, ctx, s.pool, s.chainID, "ancestor_confirmed"); n != 1 {
		t.Fatalf("ancestor_confirmed events = %d, want 1", n)
	}
	// FR-18 validity annotation mid-recovery: ancestor-side valid, affected
	// range provisional — the mixed view is never labeled complete.
	if st, v := AnnotateRecoveryHeight(row, false, int64(s.hK)); st != RecoveryStateRecovering || v != ValidityUnaffected {
		t.Fatalf("K annotation = (%s %s), want (recovering valid_unaffected)", st, v)
	}
	if st, v := AnnotateRecoveryHeight(row, false, int64(s.hP)); st != RecoveryStateRecovering || v != ValidityProvisionalReplaying {
		t.Fatalf("P annotation = (%s %s), want (recovering provisional_replaying)", st, v)
	}

	// invalidate + rollback via the REAL executor tick (real transactions).
	if err := ex.tickInvalidate(ctx, row, cap); err != nil {
		t.Fatalf("tickInvalidate(): %v", err)
	}
	row, cap = rrecRow(t, ctx, s.pool, s.chainID)
	if row.Phase != reorgPhaseInvalidated {
		t.Fatalf("phase = %q, want invalidated", row.Phase)
	}
	// SC-01 half 1: swept heights 100% non-canonical, ancestor side intact.
	var flipped, canonInSweep int64
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM chain_blocks WHERE chain_id=$1 AND number>$2 AND NOT canonical`,
		s.chainID, int64(s.hA)).Scan(&flipped); err != nil {
		t.Fatalf("count flipped: %v", err)
	}
	if flipped != int64(s.hTip-s.hA) {
		t.Fatalf("flipped blocks = %d, want %d ([%d,%d])", flipped, s.hTip-s.hA, s.hA+1, s.hTip)
	}
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM chain_blocks WHERE chain_id=$1 AND number>$2 AND number<=$3 AND canonical`,
		s.chainID, int64(s.hA), int64(s.hTip)).Scan(&canonInSweep); err != nil || canonInSweep != 0 {
		t.Fatalf("canonical rows in sweep = %d (err=%v), want 0", canonInSweep, err)
	}
	// SC-01 half 2: P+C 100% Orphaned with transition + event audit.
	pObs := rrecReadObs(t, ctx, s.pool, s.chainID, s.pBH, strings.ToLower(s.pTx.Hex()))
	cObs := rrecReadObs(t, ctx, s.pool, s.chainID, s.cBH, strings.ToLower(s.cTx.Hex()))
	if pObs.status != "orphaned" || cObs.status != "orphaned" {
		t.Fatalf("P=(%s) C=(%s), want both orphaned", pObs.status, cObs.status)
	}
	if pObs.orphanID != row.RecoveryID || cObs.orphanID != row.RecoveryID || pObs.orphanedAtNull || cObs.orphanedAtNull {
		t.Fatalf("orphan evidence P=(%s null=%v) C=(%s null=%v), want this recovery + timestamps",
			pObs.orphanID, pObs.orphanedAtNull, cObs.orphanID, cObs.orphanedAtNull)
	}
	// Old confirm basis retained as history on the ex-Confirmed row; the
	// never-confirmed row never had one.
	if cObs.tipNumber != cTipN || cObs.tipHash != cTipHash || cObs.threshold != cThr || cObs.policySeq != cSeq || cObs.confirmations != cConf {
		t.Fatalf("C retained basis = (tip %d %s N=%d conf=%s seq=%d), want pre-fork (%d %s %d %s %d)",
			cObs.tipNumber, cObs.tipHash, cObs.threshold, cObs.confirmations, cObs.policySeq, cTipN, cTipHash, cThr, cConf, cSeq)
	}
	if !pObs.confirmedAtNull || pObs.tipNumber != 0 || pObs.threshold != 0 {
		t.Fatalf("P basis must stay empty (never confirmed): %+v", pObs)
	}
	if n := rrecTransitionCount(t, ctx, s.pool, s.chainID, "pending", "orphaned", row.RecoveryID); n != 1 {
		t.Fatalf("pending->orphaned transitions = %d, want 1 (P)", n)
	}
	if n := rrecTransitionCount(t, ctx, s.pool, s.chainID, "confirmed", "orphaned", row.RecoveryID); n != 1 {
		t.Fatalf("confirmed->orphaned transitions = %d, want 1 (C)", n)
	}
	var cSnap string
	if err := s.pool.QueryRow(ctx, `SELECT basis_snapshot FROM deposit_observation_transitions
WHERE chain_id=$1 AND block_hash=$2 AND tx_hash=$3 AND from_status='confirmed' AND to_status='orphaned'`,
		s.chainID, s.cBH, strings.ToLower(s.cTx.Hex())).Scan(&cSnap); err != nil {
		t.Fatalf("read C transition snapshot: %v", err)
	}
	if !strings.Contains(cSnap, cTipHash) {
		t.Fatalf("C transition snapshot %q misses old confirm tip %s", cSnap, cTipHash)
	}
	// SC-01 half 3: K 100% untouched with basis intact.
	kObs := rrecReadObs(t, ctx, s.pool, s.chainID, s.kBH, strings.ToLower(s.kTx.Hex()))
	if kObs.status != "confirmed" || kObs.orphanID != "" || !kObs.orphanedAtNull {
		t.Fatalf("K = (%s orphan=%s), want (confirmed, untouched)", kObs.status, kObs.orphanID)
	}
	if kObs.tipNumber != kTipN || kObs.tipHash != kTipHash || kObs.threshold != kThr || kObs.policySeq != kSeq || kObs.confirmations != kConf {
		t.Fatalf("K basis moved: (tip %d %s N=%d conf=%s seq=%d), want pre-fork (%d %s %d %s %d)",
			kObs.tipNumber, kObs.tipHash, kObs.threshold, kObs.confirmations, kObs.policySeq, kTipN, kTipHash, kThr, kConf, kSeq)
	}
	if n := rrecTransitionCount(t, ctx, s.pool, s.chainID, "confirmed", "orphaned", row.RecoveryID); n != 1 {
		t.Fatalf("total orphan conversions = %d, want exactly 2 (P+C); K must have none", n+1)
	}
	// Invalidation audit: one blocks + one observations event; three
	// checkpoint rollbacks with exact floors.
	for _, ev := range []string{"blocks_invalidated", "observations_invalidated"} {
		if n := rrecEventCount(t, ctx, s.pool, s.chainID, ev); n != 1 {
			t.Fatalf("%s events = %d, want 1", ev, n)
		}
	}
	if n := rrecEventCount(t, ctx, s.pool, s.chainID, "checkpoints_rolled_back"); n != 3 {
		t.Fatalf("checkpoints_rolled_back events = %d, want 3 (block+log+deposit)", n)
	}
	var cpH int64
	var cpHash string
	if err := s.pool.QueryRow(ctx, `SELECT height, block_hash FROM indexer_checkpoint WHERE chain_id=$1`,
		s.chainID).Scan(&cpH, &cpHash); err != nil {
		t.Fatalf("read rolled-back 002 checkpoint: %v", err)
	}
	if cpH != int64(s.hA) || cpHash != s.aHashes[s.hA] {
		t.Fatalf("002 checkpoint = (%d %s), want ancestor (%d %s)", cpH, cpHash, s.hA, s.aHashes[s.hA])
	}
	for _, tc := range []struct {
		table string
		want  int64
	}{
		{"log_checkpoint", int64(s.hA + 1)}, {"deposit_checkpoint", int64(s.hA + 1)},
	} {
		var next int64
		if err := s.pool.QueryRow(ctx, fmt.Sprintf(`SELECT next_block FROM %s WHERE chain_id=$1`, tc.table),
			s.chainID).Scan(&next); err != nil {
			t.Fatalf("read %s: %v", tc.table, err)
		}
		if next != tc.want {
			t.Fatalf("%s next = %d, want floor %d", tc.table, next, tc.want)
		}
	}

	// FilterLogs fail-loud inside the loop: a broken logs client fails the
	// tick with zero frontier movement and zero replay audit.
	exFail := rrecExecutor(t, s.pool, s.lease, s)
	exFail.logs = rrecFailLogs{}
	if err := exFail.tickReplay(ctx, row, cap); err == nil || !strings.Contains(err.Error(), "FilterLogs") {
		t.Fatalf("broken-logs tickReplay = %v, want FilterLogs fail-loud", err)
	}
	rowAfterFail, _ := rrecRow(t, ctx, s.pool, s.chainID)
	if rowAfterFail.BlockFrontier != nil || rowAfterFail.LogFrontier != nil || rowAfterFail.DepositFrontier != nil {
		t.Fatalf("frontiers moved on failed tick: %+v, want all nil", rowAfterFail)
	}
	if n := rrecEventCount(t, ctx, s.pool, s.chainID, "replay_progress"); n != 0 {
		t.Fatalf("replay_progress events = %d after failed tick, want 0", n)
	}

	// replay to complete_pending via the REAL executor tick (real chain
	// reads, historical deposit semantics, legitimate-empty advance).
	for i := 0; i < 10; i++ {
		row, cap = rrecRow(t, ctx, s.pool, s.chainID)
		if row.Phase == reorgPhaseCompletePending {
			break
		}
		if err := ex.tickReplay(ctx, row, cap); err != nil {
			t.Fatalf("tickReplay #%d: %v", i, err)
		}
	}
	row, cap = rrecRow(t, ctx, s.pool, s.chainID)
	if row.Phase != reorgPhaseCompletePending {
		t.Fatalf("phase = %q after replay, want complete_pending", row.Phase)
	}
	for _, tc := range []struct {
		name string
		got  *int64
		want int64
	}{
		{"block", row.BlockFrontier, int64(s.hTip)},
		{"log", row.LogFrontier, int64(s.hTip)},
		{"deposit", row.DepositFrontier, int64(s.hTip)},
	} {
		if tc.got == nil || *tc.got != tc.want {
			got := int64(-1)
			if tc.got != nil {
				got = *tc.got
			}
			t.Fatalf("%s frontier = %d, want %d", tc.name, got, tc.want)
		}
	}
	if n := rrecEventCount(t, ctx, s.pool, s.chainID, "replay_progress"); n != 3 {
		t.Fatalf("replay_progress events = %d, want 3 (one per stream)", n)
	}
	// New-chain identity: fork-B heights carry exactly one canonical row
	// each (the B hash), old fork-A rows retained non-canonical.
	for h := s.hA + 1; h <= s.hTip; h++ {
		var canon, total int
		if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM chain_blocks WHERE chain_id=$1 AND number=$2 AND canonical`,
			s.chainID, int64(h)).Scan(&canon); err != nil || canon != 1 {
			t.Fatalf("canonical rows at %d = %d (err=%v), want 1", h, canon, err)
		}
		if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM chain_blocks WHERE chain_id=$1 AND number=$2`,
			s.chainID, int64(h)).Scan(&total); err != nil || total != 2 {
			t.Fatalf("total rows at %d = %d (err=%v), want 2 (A-retained + B-new)", h, total, err)
		}
		var ch string
		if err := s.pool.QueryRow(ctx, `SELECT hash FROM chain_blocks WHERE chain_id=$1 AND number=$2 AND canonical`,
			s.chainID, int64(h)).Scan(&ch); err != nil || ch != s.bHashes[h] {
			t.Fatalf("canonical[%d] = %s (err=%v), want fork-B %s", h, ch, err, s.bHashes[h])
		}
	}
	var bParent string
	if err := s.pool.QueryRow(ctx, `SELECT parent_hash FROM chain_blocks WHERE chain_id=$1 AND hash=$2`,
		s.chainID, s.bHashes[s.hA+1]).Scan(&bParent); err != nil || bParent != s.aHashes[s.hA] {
		t.Fatalf("fork-B base parent = %s (err=%v), want ancestor %s", bParent, err, s.aHashes[s.hA])
	}
	// Regenerated observation under historical semantics: new identity,
	// pending, version_seq=1 (inherited, never current-config), amount 11.
	bObs := rrecReadObs(t, ctx, s.pool, s.chainID, s.bBH, strings.ToLower(s.bTx.Hex()))
	if bObs.status != "pending" || bObs.blockNumber != int64(s.hA+1) || bObs.amount != "11" || bObs.version != 1 {
		t.Fatalf("fork-B observation = %+v, want (pending @%d amount 11 v1)", bObs, s.hA+1)
	}
	if bObs.orphanID != "" || !bObs.orphanedAtNull || !bObs.confirmedAtNull {
		t.Fatalf("fork-B observation carries orphan/confirm residue: %+v", bObs)
	}
	var bLogN int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM erc20_transfer_logs WHERE chain_id=$1 AND block_hash=$2 AND tx_hash=$3`,
		s.chainID, s.bBH, strings.ToLower(s.bTx.Hex())).Scan(&bLogN); err != nil || bLogN != 1 {
		t.Fatalf("fork-B log rows = %d (err=%v), want 1", bLogN, err)
	}
	// P/C stay Orphaned (fork B lacks them); K still Confirmed.
	if o := rrecReadObs(t, ctx, s.pool, s.chainID, s.pBH, strings.ToLower(s.pTx.Hex())); o.status != "orphaned" {
		t.Fatalf("P after replay = %s, want orphaned", o.status)
	}
	if o := rrecReadObs(t, ctx, s.pool, s.chainID, s.cBH, strings.ToLower(s.cTx.Hex())); o.status != "orphaned" {
		t.Fatalf("C after replay = %s, want orphaned", o.status)
	}
	// Replay checkpoints advanced to the new tip.
	if err := s.pool.QueryRow(ctx, `SELECT height, block_hash FROM indexer_checkpoint WHERE chain_id=$1`,
		s.chainID).Scan(&cpH, &cpHash); err != nil {
		t.Fatalf("read replayed 002 checkpoint: %v", err)
	}
	if cpH != int64(s.hTip) || cpHash != s.bHashes[s.hTip] {
		t.Fatalf("002 checkpoint = (%d %s), want fork-B tip (%d %s)", cpH, cpHash, s.hTip, s.bHashes[s.hTip])
	}

	// auto-release via the REAL executor tick: re-verified FR-19判据 +
	// mandatory terminal detail; independent pauses survive.
	if err := ex.tickComplete(ctx, row, cap); err != nil {
		t.Fatalf("tickComplete(): %v", err)
	}
	if prow, err := LoadRecoveryState(ctx, s.pool, s.chainID); err != nil || prow != nil {
		t.Fatalf("recovery row after release = %+v (err=%v), want gone", prow, err)
	}
	var terminal string
	if err := s.pool.QueryRow(ctx, `SELECT detail FROM reorg_recovery_events WHERE chain_id=$1 AND event='auto_completed'`,
		s.chainID).Scan(&terminal); err != nil {
		t.Fatalf("read terminal event: %v", err)
	}
	for _, want := range []string{
		fmt.Sprintf("bound_old=%d:%s", s.hTip, s.aHashes[s.hTip]),
		"policy=1",
		fmt.Sprintf("ancestor=%d:%s", s.hA, s.aHashes[s.hA]),
		fmt.Sprintf("swept=%d-%d", s.hA+1, s.hTip),
		"orphaned=2", "revived=0",
		fmt.Sprintf("replayed=block:%d,log:%d,deposit:%d", s.hTip, s.hTip, s.hTip),
		"surviving_pauses=indexer_pause,deposit_pause",
		"version=1",
	} {
		if !strings.Contains(terminal, want) {
			t.Fatalf("terminal detail missing %q: %q", want, terminal)
		}
	}
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM indexer_pause WHERE chain_id=$1 AND kind='hash_mismatch'`,
		s.chainID).Scan(&pauseN); err != nil || pauseN != 1 {
		t.Fatalf("hash_mismatch pause rows = %d (err=%v), want 1 surviving", pauseN, err)
	}
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM deposit_pause WHERE chain_id=$1`,
		s.chainID).Scan(&pauseN); err != nil || pauseN != 1 {
		t.Fatalf("deposit_pause rows = %d (err=%v), want 1 surviving", pauseN, err)
	}
	if n := rrecEventCount(t, ctx, s.pool, s.chainID, "auto_completed"); n != 1 {
		t.Fatalf("auto_completed events = %d, want exactly 1", n)
	}

	// Post-release ordinary work stays stopped while the independent
	// pauses survive (the designed end state): a fresh-capture 005 commit
	// for B refuses on the deposit_pause with B still pending and zero
	// writes.
	postCap := testRecoveryCap(t, ctx, s.pool, s.chainID)
	committerPost, err := NewConfirmationCommitter(s.pool, ConfirmationConfig{ChainID: s.chainID, ThresholdN: rrecThresholdN})
	if err != nil {
		t.Fatalf("NewConfirmationCommitter(): %v", err)
	}
	bBH0, bTx0 := s.bBH, strings.ToLower(s.bTx.Hex())
	bBasisPost := ConfirmBasis{
		BlockHash: bBH0, TxHash: bTx0, LogIndex: 0, Height: uint64(s.hA + 1),
		TipNumber: uint64(s.hTip), TipHash: s.bHashes[s.hTip], PolicySeq: 1, ThresholdN: rrecThresholdN,
	}
	if err := committerPost.ConfirmDepositUnit(ctx, s.lease, bBasisPost, postCap); err == nil || !strings.Contains(err.Error(), "deposit_pause") {
		t.Fatalf("post-release confirm under surviving pause = %v, want the deposit_pause stop", err)
	}
	if o := rrecReadObs(t, ctx, s.pool, s.chainID, bBH0, bTx0); o.status != "pending" {
		t.Fatalf("B after stopped confirm = %s, want pending (zero writes)", o.status)
	}
	// Operator clears the independent deposit pause through the REAL 004
	// manual path (row delete + audit atomically, consumer state
	// untouched).
	var pauseID, pauseRev int64
	if err := s.pool.QueryRow(ctx, `SELECT pause_id, revision FROM deposit_pause WHERE chain_id=$1`,
		s.chainID).Scan(&pauseID, &pauseRev); err != nil {
		t.Fatalf("read independent pause identity: %v", err)
	}
	scD, err := NewDepositScanner(s.pool, rrecDepositCfg(s))
	if err != nil {
		t.Fatalf("NewDepositScanner(): %v", err)
	}
	freed, err := scD.ReleaseDepositPause(ctx, s.lease, pauseID, pauseRev,
		"rrec-operator", "US1 independent cause cleared after recovery release")
	if err != nil || !freed {
		t.Fatalf("ReleaseDepositPause() = (%v, %v), want (true, nil)", freed, err)
	}
	var relAudit int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM deposit_pause_audit WHERE chain_id=$1 AND pause_id=$2 AND action='release'`,
		s.chainID, pauseID).Scan(&relAudit); err != nil || relAudit != 1 {
		t.Fatalf("deposit release audit rows = %d (err=%v), want 1", relAudit, err)
	}
	// The seeded 002 fork-evidence pause has no production clear path
	// (006 never writes pause tables by R9): it was scaffolding for the
	// tickIdle trigger, already asserted surviving release. Tear the seed
	// down so the live 005 loop can run its full gates — no business row
	// is written by this teardown.
	if _, err := s.pool.Exec(ctx, `DELETE FROM indexer_pause WHERE chain_id=$1`, s.chainID); err != nil {
		t.Fatalf("teardown seeded 002 pause: %v", err)
	}
	var pauseLeft int
	if err := s.pool.QueryRow(ctx, `SELECT
(SELECT count(*) FROM indexer_pause WHERE chain_id=$1) +
(SELECT count(*) FROM deposit_pause WHERE chain_id=$1)`,
		s.chainID).Scan(&pauseLeft); err != nil {
		t.Fatalf("count pauses: %v", err)
	}
	if pauseLeft != 0 {
		t.Fatalf("pause rows after operator clear + seed teardown = %d, want 0", pauseLeft)
	}

	// 005 reconfirmation post-release: the regenerated observation meets
	// ALL live-policy conditions with zero old-basis reuse.
	m2 := metrics.New(func() bool { return true })
	confirmCfg2 := ConfirmationConfig{
		ChainID:      s.chainID,
		ThresholdN:   rrecThresholdN,
		PollInterval: 25 * time.Millisecond,
		RetryInitial: 25 * time.Millisecond,
		RetryMax:     250 * time.Millisecond,
	}
	committer2, err := NewConfirmationCommitter(s.pool, confirmCfg2)
	if err != nil {
		t.Fatalf("NewConfirmationCommitter(): %v", err)
	}
	sc2, err := NewConfirmationScanner(s.pool, confirmCfg2, committer2, m2)
	if err != nil {
		t.Fatalf("NewConfirmationScanner(): %v", err)
	}
	stop2 := confirm13RunLoop(t, ctx, sc2, s.lease)
	bBH, bTx := s.bBH, strings.ToLower(s.bTx.Hex())
	waitUntil(t, time.Now().Add(60*time.Second), "fork-B observation confirmed", func() bool {
		var k int
		_ = s.pool.QueryRow(ctx, `SELECT count(*) FROM deposit_observations WHERE chain_id=$1 AND block_hash=$2 AND tx_hash=$3 AND status='confirmed'`,
			s.chainID, bBH, bTx).Scan(&k)
		return k == 1
	})
	time.Sleep(200 * time.Millisecond)
	stop2()
	newStatus, newNull, newTipN, newThr, newSeq, newTipHash, newConf :=
		confirmReadBasis(t, ctx, s.pool, s.chainID, bBH, bTx)
	wantNewConf := fmt.Sprintf("%d", s.hTip-(s.hA+1)+1)
	if newStatus != "confirmed" || newNull || newTipN != int64(s.hTip) || newTipHash != s.bHashes[s.hTip] ||
		newThr != int64(rrecThresholdN) || newSeq != 1 || newConf != wantNewConf {
		t.Fatalf("fork-B basis = (%s null=%v tip %d %s N=%d conf=%s seq=%d), want confirmed tip(%d %s) N=%d conf=%s seq=1",
			newStatus, newNull, newTipN, newTipHash, newThr, newConf, newSeq, s.hTip, s.bHashes[s.hTip], rrecThresholdN, wantNewConf)
	}
	if newTipHash == cTipHash {
		t.Fatalf("reconfirmation reused the old confirm tip %s", cTipHash)
	}
	// SC-01 close: P+C 100% Orphaned, K Confirmed with original basis, B
	// Confirmed on the new basis; transition log holds both conversions.
	var orphanedAffected, kConfN, bConfN int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM deposit_observations WHERE chain_id=$1 AND status='orphaned' AND block_number>$2`,
		s.chainID, int64(s.hA)).Scan(&orphanedAffected); err != nil || orphanedAffected != 2 {
		t.Fatalf("orphaned affected = %d (err=%v), want 2 (P+C)", orphanedAffected, err)
	}
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM deposit_observations WHERE chain_id=$1 AND tx_hash=$2 AND status='confirmed'
AND confirm_tip_number=$3 AND confirm_tip_hash=$4 AND confirm_threshold=$5 AND confirmations=$6 AND confirm_policy_seq=$7`,
		s.chainID, strings.ToLower(s.kTx.Hex()), kTipN, kTipHash, kThr, kConf, kSeq).Scan(&kConfN); err != nil || kConfN != 1 {
		t.Fatalf("K-on-original-basis rows = %d (err=%v), want 1", kConfN, err)
	}
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM deposit_observations WHERE chain_id=$1 AND tx_hash=$2 AND status='confirmed'`,
		s.chainID, bTx).Scan(&bConfN); err != nil || bConfN != 1 {
		t.Fatalf("B confirmed rows = %d (err=%v), want 1", bConfN, err)
	}
	if total := rrecTransitionCount(t, ctx, s.pool, s.chainID, "pending", "orphaned", row.RecoveryID) +
		rrecTransitionCount(t, ctx, s.pool, s.chainID, "confirmed", "orphaned", row.RecoveryID); total != 2 {
		t.Fatalf("orphan transitions = %d, want 2", total)
	}
}

// --- T021: crash-resume spot check -------------------------------------------

// TestReorgRecoveryUS1CrashResume is T021 (FR-12/13, V7-part, resume drill):
// dropping the executor+pool mid-recovery at two different phase boundaries
// then resuming with a fresh executor on the same DB lands on the persisted
// phase + frontiers with no redone committed phase and no skipped phase; one
// lost-commit-response case triages via re-read to the persisted outcome.
func TestReorgRecoveryUS1CrashResume(t *testing.T) {
	s := rrecSetup(t, "rrec-us1-crash")
	ctx := context.Background()

	rrecMineForkA(t, ctx, s)
	rrecIndexForkA(t, ctx, s)
	rrecForkB(t, ctx, s)
	rrecSeedPauses(t, ctx, s)

	// rrecResume drops everything (the kill -9) and rebuilds the executor on
	// the same DB: the only continuity is durable state. The lease
	// credentials carry over (same owner+token against the untouched DB
	// row); only the pool is replaced.
	resume := func(t *testing.T) (*pgxpool.Pool, *RecoveryExecutor) {
		t.Helper()
		s.pool.Close()
		pool := openIndexerPool(t, s.dsn)
		t.Cleanup(pool.Close)
		s.pool = pool
		return pool, rrecExecutor(t, pool, s.lease, s)
	}

	pool, ex := resume(t)
	done, err := ex.tickIdle(ctx)
	if err != nil || !done {
		t.Fatalf("tickIdle() = (%v, %v), want (true, nil)", done, err)
	}
	firstID, firstSeq := rrecRowID(t, ctx, pool, s.chainID)
	phases := []string{reorgPhaseDetected}

	// Crash 1: between detected and ancestor_confirmed.
	pool, ex = resume(t)
	row, cap := rrecRow(t, ctx, pool, s.chainID)
	if row.RecoveryID != firstID || row.Seq != firstSeq || row.Phase != reorgPhaseDetected {
		t.Fatalf("resume-1 row = (%s %d %s), want (%s %d detected)",
			row.RecoveryID, row.Seq, row.Phase, firstID, firstSeq)
	}
	if n := rrecEventCount(t, ctx, pool, s.chainID, "established"); n != 1 {
		t.Fatalf("established events after crash-1 = %d, want 1 (no redo)", n)
	}
	if row.BlockFrontier != nil || row.LogFrontier != nil || row.DepositFrontier != nil {
		t.Fatalf("frontiers after crash-1 = (%v %v %v), want all nil", row.BlockFrontier, row.LogFrontier, row.DepositFrontier)
	}

	// Lost-commit-response triage on the ancestor confirm: the COMMIT lands
	// server-side while the worker observes an error; the outcome is decided
	// by re-reading durable state, never by memory.
	dropPool, dropCtl := logscanOpenCommitDropPool(t, s.dsn)
	dropCtl.arm.Store(true)
	// The drop pool runs the commit under the carried-over lease
	// credentials (same owner+token the verdict expects); only the COMMIT
	// reply is destroyed.
	dropErr := ConfirmRecoveryAncestor(ctx, dropPool, s.lease, s.chainID, cap,
		int64(s.hA), s.aHashes[s.hA], "rrec crash triage evidence")
	if dropErr == nil {
		t.Fatal("ConfirmRecoveryAncestor through the commit-drop pool = nil, want the lost-response error")
	}
	triRow, triErr := LoadRecoveryState(ctx, pool, s.chainID)
	if triErr != nil || triRow == nil {
		t.Fatalf("triage re-read = (%+v, %v), want the row", triRow, triErr)
	}
	if triRow.Phase != reorgPhaseAncestorConfirmed || triRow.AncestorNumber == nil || *triRow.AncestorNumber != int64(s.hA) {
		t.Fatalf("triage outcome = phase %s ancestor %v, want (ancestor_confirmed %d): re-read must decide, not memory",
			triRow.Phase, triRow.AncestorNumber, s.hA)
	}
	if n := rrecEventCount(t, ctx, pool, s.chainID, "ancestor_confirmed"); n != 1 {
		t.Fatalf("ancestor_confirmed events = %d, want 1 (committed once)", n)
	}
	phases = append(phases, reorgPhaseAncestorConfirmed)
	// No redo of the committed confirm: a second submit refuses on the phase
	// gate instead of appending a duplicate event.
	if err := ConfirmRecoveryAncestor(ctx, pool, s.lease, s.chainID, cap, int64(s.hA), s.aHashes[s.hA], "rrec redo"); !isRecoveryGate(err) {
		t.Fatalf("re-confirm after committed confirm = %v, want gate refusal (no redo)", err)
	}
	if n := rrecEventCount(t, ctx, pool, s.chainID, "ancestor_confirmed"); n != 1 {
		t.Fatalf("ancestor_confirmed events after redo attempt = %d, want still 1", n)
	}
	dropPool.Close()

	// Crash 2: between ancestor_confirmed and invalidated (frontiers still
	// nil, ancestor pinned — resume must keep both with no skip).
	pool, ex = resume(t)
	row, cap = rrecRow(t, ctx, pool, s.chainID)
	if row.RecoveryID != firstID || row.Phase != reorgPhaseAncestorConfirmed ||
		row.AncestorNumber == nil || *row.AncestorNumber != int64(s.hA) ||
		row.AncestorHash == nil || *row.AncestorHash != s.aHashes[s.hA] {
		t.Fatalf("resume-2 row = %+v, want ancestor_confirmed @(%d %s)", row, s.hA, s.aHashes[s.hA])
	}
	if n := rrecEventCount(t, ctx, pool, s.chainID, "ancestor_confirmed"); n != 1 {
		t.Fatalf("ancestor_confirmed events after crash-2 = %d, want 1 (no redo)", n)
	}

	// No skipped phase: the gate chain detected → invalidated → replaying →
	// complete_pending → release advances exactly once per committed step.
	if err := ex.tickInvalidate(ctx, row, cap); err != nil {
		t.Fatalf("post-crash tickInvalidate(): %v", err)
	}
	row, cap = rrecRow(t, ctx, pool, s.chainID)
	if row.Phase != reorgPhaseInvalidated {
		t.Fatalf("phase = %q, want invalidated (no skip)", row.Phase)
	}
	phases = append(phases, reorgPhaseInvalidated)
	var flipped int64
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM chain_blocks WHERE chain_id=$1 AND number>$2 AND NOT canonical`,
		s.chainID, int64(s.hA)).Scan(&flipped); err != nil || flipped != int64(s.hTip-s.hA) {
		t.Fatalf("flipped = %d (err=%v), want %d", flipped, err, s.hTip-s.hA)
	}

	for i := 0; i < 10; i++ {
		row, cap = rrecRow(t, ctx, pool, s.chainID)
		if row.Phase == reorgPhaseCompletePending {
			break
		}
		if row.Phase != reorgPhaseInvalidated && row.Phase != reorgPhaseReplaying {
			t.Fatalf("unexpected phase %q mid-replay (skipped or regressed)", row.Phase)
		}
		if err := ex.tickReplay(ctx, row, cap); err != nil {
			t.Fatalf("post-crash tickReplay #%d: %v", i, err)
		}
	}
	row, cap = rrecRow(t, ctx, pool, s.chainID)
	if row.Phase != reorgPhaseCompletePending {
		t.Fatalf("phase = %q, want complete_pending", row.Phase)
	}
	phases = append(phases, reorgPhaseReplaying, reorgPhaseCompletePending)
	if row.BlockFrontier == nil || *row.BlockFrontier != int64(s.hTip) ||
		row.LogFrontier == nil || *row.LogFrontier != int64(s.hTip) ||
		row.DepositFrontier == nil || *row.DepositFrontier != int64(s.hTip) {
		t.Fatalf("post-crash frontiers = (%v %v %v), want (%d %d %d)",
			row.BlockFrontier, row.LogFrontier, row.DepositFrontier, s.hTip, s.hTip, s.hTip)
	}

	if err := ex.tickComplete(ctx, row, cap); err != nil {
		t.Fatalf("post-crash tickComplete(): %v", err)
	}
	if prow, err := LoadRecoveryState(ctx, pool, s.chainID); err != nil || prow != nil {
		t.Fatalf("recovery row after release = %+v (err=%v), want gone exactly once", prow, err)
	}
	if n := rrecEventCount(t, ctx, pool, s.chainID, "auto_completed"); n != 1 {
		t.Fatalf("auto_completed events = %d, want exactly 1 (exactly-once terminal release)", n)
	}
	// Phase ledger: monotonic, no redo, no skip, one terminal release.
	wantPhases := []string{
		reorgPhaseDetected, reorgPhaseAncestorConfirmed, reorgPhaseInvalidated,
		reorgPhaseReplaying, reorgPhaseCompletePending,
	}
	if fmt.Sprintf("%v", phases) != fmt.Sprintf("%v", wantPhases) {
		t.Fatalf("phase ledger = %v, want %v", phases, wantPhases)
	}
	for _, ev := range []string{"established", "ancestor_confirmed", "blocks_invalidated", "observations_invalidated", "auto_completed"} {
		if n := rrecEventCount(t, ctx, pool, s.chainID, ev); n != 1 {
			t.Fatalf("%s events = %d, want 1 (no redone committed phase)", ev, n)
		}
	}
	if n := rrecEventCount(t, ctx, pool, s.chainID, "checkpoints_rolled_back"); n != 3 {
		t.Fatalf("checkpoints_rolled_back events = %d, want 3", n)
	}
}

// rrecRowID reads the active recovery identity (helper for the crash test's
// same-instance assertions).
func rrecRowID(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64) (string, int64) {
	t.Helper()
	row, err := LoadRecoveryState(ctx, pool, chainID)
	if err != nil || row == nil {
		t.Fatalf("LoadRecoveryState() = (%+v, %v), want the active row", row, err)
	}
	return row.RecoveryID, row.Seq
}

// --- T022 helpers (US2 fork shapes; existing helpers above untouched) --------

// rrecForkEmpty reverts to the ancestor snapshot and mines pure empty blocks
// to the same tip: shape (a), no new logs on the fork.
func rrecForkEmpty(t *testing.T, ctx context.Context, s *rrecScene) {
	t.Helper()
	var ok bool
	s.node.mustCall(t, &ok, "evm_revert", s.snapID)
	if !ok {
		t.Fatal("evm_revert refused the ancestor snapshot")
	}
	if got := s.node.blockNumber(t); got != s.hA {
		t.Fatalf("anvil head after revert = %d, want ancestor %d", got, s.hA)
	}
	s.node.mine(t, s.hTip-s.hA)
	if got := s.node.blockNumber(t); got != s.hTip {
		t.Fatalf("empty-fork tip = %d, want %d", got, s.hTip)
	}
	for h := s.hA + 1; h <= s.hTip; h++ {
		s.bHashes[h] = rrecHeaderHash(t, ctx, s, h)
		if s.bHashes[h] == s.aHashes[h] {
			t.Fatalf("empty-fork hash at %d equals fork-A hash: no divergence", h)
		}
	}
}

// rrecDumpState snapshots the full Anvil state (blocks included) for an
// exact later return via rrecLoadState — shape (d) needs byte-identical
// history, which re-mining can never reproduce (headers carry wall-clock).
func rrecDumpState(t *testing.T, s *rrecScene) string {
	t.Helper()
	var dump string
	s.node.mustCall(t, &dump, "anvil_dumpState")
	if dump == "" {
		t.Fatal("anvil_dumpState returned empty state")
	}
	return dump
}

// rrecLoadState restores a dumped state and pins the tip to wantTip.
func rrecLoadState(t *testing.T, s *rrecScene, dump string, wantTip uint64) {
	t.Helper()
	var ok bool
	s.node.mustCall(t, &ok, "anvil_loadState", dump)
	if !ok {
		t.Fatal("anvil_loadState refused the dump")
	}
	if got := s.node.blockNumber(t); got != wantTip {
		t.Fatalf("anvil head after loadState = %d, want %d", got, wantTip)
	}
}

// rrecSetAutomine toggles Anvil automining (multi-tx single-block assembly
// for the index-change variant needs it off, then back on).
func rrecSetAutomine(t *testing.T, s *rrecScene, on bool) {
	t.Helper()
	var out bool
	s.node.mustCall(t, &out, "evm_setAutomine", on)
}

// rrecReceipt is the minimal receipt view the shape tests need.
type rrecReceipt struct {
	BlockNumber hexutil.Uint64 `json:"blockNumber"`
	Logs        []struct {
		LogIndex hexutil.Uint64 `json:"logIndex"`
		Data     hexutil.Bytes  `json:"data"`
	} `json:"logs"`
}

func rrecWaitReceipt(t *testing.T, s *rrecScene, tx common.Hash) rrecReceipt {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var r *rrecReceipt
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := s.node.rpc.CallContext(ctx, &r, "eth_getTransactionReceipt", tx)
		cancel()
		if err != nil {
			t.Fatalf("eth_getTransactionReceipt(%s): %v", tx.Hex(), err)
		}
		if r != nil {
			return *r
		}
		if time.Now().After(deadline) {
			t.Fatalf("transaction %s was not mined within the deadline", tx.Hex())
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// rrecSubmitRaw sends one raw transaction map without waiting (for
// automine-off block packing); rrecSendRaw submits and waits for mining.
func rrecSubmitRaw(t *testing.T, s *rrecScene, params map[string]any) common.Hash {
	t.Helper()
	var tx common.Hash
	s.node.mustCall(t, &tx, "eth_sendTransaction", params)
	return tx
}

func rrecSendRaw(t *testing.T, s *rrecScene, params map[string]any) (common.Hash, uint64) {
	t.Helper()
	tx := rrecSubmitRaw(t, s, params)
	return tx, uint64(rrecWaitReceipt(t, s, tx).BlockNumber)
}

// rrecReceiptLogIndex reads the first log's index for a mined tx.
func rrecReceiptLogIndex(t *testing.T, s *rrecScene, tx common.Hash) uint64 {
	t.Helper()
	r := rrecWaitReceipt(t, s, tx)
	if len(r.Logs) != 1 {
		t.Fatalf("tx %s has %d logs, want exactly 1", tx.Hex(), len(r.Logs))
	}
	return uint64(r.Logs[0].LogIndex)
}

// rrecTxParams copies a mined tx's exact send fields so a post-revert resend
// reproduces the identical tx hash (same nonce restored by the revert, same
// explicit fees — Anvil-filled 1559 fields included verbatim).
func rrecTxParams(t *testing.T, s *rrecScene, tx common.Hash) map[string]any {
	t.Helper()
	var raw map[string]any
	s.node.mustCall(t, &raw, "eth_getTransactionByHash", tx)
	if len(raw) == 0 {
		t.Fatalf("eth_getTransactionByHash(%s) returned nothing", tx.Hex())
	}
	out := map[string]any{}
	for _, k := range []string{"from", "to", "gas", "value", "nonce", "type", "accessList"} {
		if v, ok := raw[k]; ok {
			out[k] = v
		}
	}
	// eth_getTransactionByHash names calldata `input`; eth_sendTransaction
	// takes `data` (Anvil ignores a bare `input`, which silently changes the
	// tx hash and drops the logs).
	if v, ok := raw["input"]; ok {
		out["data"] = v
	} else if v, ok := raw["data"]; ok {
		out["data"] = v
	}
	if v, ok := raw["gasPrice"]; ok {
		out["gasPrice"] = v
	}
	if v, ok := raw["maxFeePerGas"]; ok {
		out["maxFeePerGas"] = v
	}
	if v, ok := raw["maxPriorityFeePerGas"]; ok {
		out["maxPriorityFeePerGas"] = v
	}
	// A 1559 resend must not carry the legacy gasPrice alongside the
	// max-fee fields: Anvil would type the tx legacy and the hash could
	// never equal the original 1559 (same-tx history line broken).
	if _, ok := out["maxFeePerGas"]; ok {
		delete(out, "gasPrice")
	}
	return out
}

// rrecEmitFrom installs emit-code baked with an explicit sender (the
// index-change variant needs an accounts[1] log source; rrecEmit stays
// frozen for T020/T021).
func rrecEmitFrom(t *testing.T, s *rrecScene, token, sender common.Address, amount *big.Int) {
	t.Helper()
	topics := [3]common.Hash{eth.TransferSig}
	topics[1] = common.BytesToHash(sender.Bytes())
	topics[2] = common.BytesToHash(s.watch.Bytes())
	s.node.setCode(t, token, logscanEmitCode(topics, amount))
}

// rrecReminedIds carries the shape-(c) fork identities.
type rrecReminedIds struct {
	xTx     common.Hash // == scene C tx (same history line), log_index 1
	xBH     string
	yTx     common.Hash // == scene P tx (same history line), amount 99
	yBH     string
	yAmount string
	zTx     common.Hash // extra accounts[1] log in X's block
}

// rrecForkRemined builds shape (c): revert, then re-mine the scene's own
// transfers with identical params (same tx hashes, new block hashes) —
// X packed after an accounts[1] log (index-change), Y under rewritten code
// (content-change: amount 9 → 99) — at the same heights, tip unchanged.
func rrecForkRemined(t *testing.T, ctx context.Context, s *rrecScene) rrecReminedIds {
	t.Helper()
	// Pre-revert param capture: evm_revert drops the fork-A txs from
	// eth_getTransactionByHash, so their resend params must be read while
	// fork A is still the chain.
	cParams := rrecTxParams(t, s, s.cTx)
	pParams := rrecTxParams(t, s, s.pTx)
	var ok bool
	s.node.mustCall(t, &ok, "evm_revert", s.snapID)
	if !ok {
		t.Fatal("evm_revert refused the ancestor snapshot")
	}
	if got := s.node.blockNumber(t); got != s.hA {
		t.Fatalf("anvil head after revert = %d, want ancestor %d", got, s.hA)
	}
	// Content-change lever: rewrite tokenP's emitted amount post-revert.
	// Same call params below therefore keep Y's tx hash with new data.
	rrecEmit(t, s, s.tokenP, big.NewInt(99))
	rrecEmitFrom(t, s, s.tokenB, s.node.accounts[1], big.NewInt(11))

	var ids rrecReminedIds
	rrecSetAutomine(t, s, false)
	zTx := rrecSubmitRaw(t, s, map[string]any{
		"from": s.node.accounts[1], "to": s.tokenB.Hex(), "data": "0x", "gas": "0x30d40",
	})
	xTx := rrecSubmitRaw(t, s, cParams)
	s.node.mine(t, 1)
	rrecSetAutomine(t, s, true)
	if got := s.node.blockNumber(t); got != s.hA+1 {
		t.Fatalf("re-mined X block = %d, want %d", got, s.hA+1)
	}
	if xTx != s.cTx {
		t.Fatalf("re-mined X tx = %s, want identical %s (same history line)", xTx.Hex(), s.cTx.Hex())
	}
	if idx := rrecReceiptLogIndex(t, s, xTx); idx != 1 {
		t.Fatalf("re-mined X log_index = %d, want 1 (index-change variant)", idx)
	}
	ids.xTx, ids.zTx = xTx, zTx

	s.node.mine(t, 2)
	yTx, yH := rrecSendRaw(t, s, pParams)
	if yH != s.hA+4 {
		t.Fatalf("re-mined Y block = %d, want %d", yH, s.hA+4)
	}
	if yTx != s.pTx {
		t.Fatalf("re-mined Y tx = %s, want identical %s (same history line)", yTx.Hex(), s.pTx.Hex())
	}
	s.node.mine(t, 2)
	if got := s.node.blockNumber(t); got != s.hTip {
		t.Fatalf("re-mined tip = %d, want %d", got, s.hTip)
	}
	for h := s.hA + 1; h <= s.hTip; h++ {
		s.bHashes[h] = rrecHeaderHash(t, ctx, s, h)
		if s.bHashes[h] == s.aHashes[h] {
			t.Fatalf("re-mined hash at %d equals fork-A hash: no divergence", h)
		}
	}
	ids.yTx, ids.yBH, ids.yAmount = yTx, s.bHashes[s.hA+4], "99"
	ids.xBH = s.bHashes[s.hA+1]
	return ids
}

// rrecClearPauses releases the seeded deposit pause through the REAL 004
// manual path (row + audit atomically) and tears down the seeded 002
// fork-evidence pause (no production clear path exists by R9; it was
// scaffolding for the tickIdle trigger, asserted surviving first).
func rrecClearPauses(t *testing.T, ctx context.Context, s *rrecScene) {
	t.Helper()
	var pauseID, pauseRev int64
	if err := s.pool.QueryRow(ctx, `SELECT pause_id, revision FROM deposit_pause WHERE chain_id=$1`,
		s.chainID).Scan(&pauseID, &pauseRev); err != nil {
		t.Fatalf("read independent pause identity: %v", err)
	}
	scD, err := NewDepositScanner(s.pool, rrecDepositCfg(s))
	if err != nil {
		t.Fatalf("NewDepositScanner(): %v", err)
	}
	freed, err := scD.ReleaseDepositPause(ctx, s.lease, pauseID, pauseRev,
		"rrec-operator", "US2 independent cause cleared after recovery release")
	if err != nil || !freed {
		t.Fatalf("ReleaseDepositPause() = (%v, %v), want (true, nil)", freed, err)
	}
	if _, err := s.pool.Exec(ctx, `DELETE FROM indexer_pause WHERE chain_id=$1`, s.chainID); err != nil {
		t.Fatalf("teardown seeded 002 pause: %v", err)
	}
	var left int
	if err := s.pool.QueryRow(ctx, `SELECT
(SELECT count(*) FROM indexer_pause WHERE chain_id=$1) +
(SELECT count(*) FROM deposit_pause WHERE chain_id=$1)`,
		s.chainID).Scan(&left); err != nil {
		t.Fatalf("count pauses: %v", err)
	}
	if left != 0 {
		t.Fatalf("pause rows after clear = %d, want 0", left)
	}
}

// rrecDriveRecovery runs the REAL executor chain establish→…→release for one
// round and returns the terminal recovery id (row already deleted).
func rrecDriveRecovery(t *testing.T, ctx context.Context, pool *pgxpool.Pool, ex *RecoveryExecutor, chainID int64) string {
	t.Helper()
	done, err := ex.tickIdle(ctx)
	if err != nil || !done {
		t.Fatalf("tickIdle() = (%v, %v), want (true, nil)", done, err)
	}
	row, cap := rrecRow(t, ctx, pool, chainID)
	if err := ex.tickDetected(ctx, row); err != nil {
		t.Fatalf("tickDetected(): %v", err)
	}
	row, cap = rrecRow(t, ctx, pool, chainID)
	if err := ex.tickInvalidate(ctx, row, cap); err != nil {
		t.Fatalf("tickInvalidate(): %v", err)
	}
	for i := 0; i < 10; i++ {
		row, cap = rrecRow(t, ctx, pool, chainID)
		if row.Phase == reorgPhaseCompletePending {
			break
		}
		if err := ex.tickReplay(ctx, row, cap); err != nil {
			t.Fatalf("tickReplay #%d: %v", i, err)
		}
	}
	row, cap = rrecRow(t, ctx, pool, chainID)
	if row.Phase != reorgPhaseCompletePending {
		t.Fatalf("phase = %q after replay, want complete_pending", row.Phase)
	}
	id := row.RecoveryID
	if err := ex.tickComplete(ctx, row, cap); err != nil {
		t.Fatalf("tickComplete(): %v", err)
	}
	return id
}

// rrecCountObs counts observations in a block range by statuses.
func rrecCountObs(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, from, to uint64, statuses ...string) int {
	t.Helper()
	var n int
	q := `SELECT count(*) FROM deposit_observations WHERE chain_id=$1 AND block_number>$2 AND block_number<=$3`
	args := []any{chainID, int64(from), int64(to)}
	if len(statuses) > 0 {
		ph := make([]string, len(statuses))
		for i, st := range statuses {
			ph[i] = fmt.Sprintf("$%d", 4+i)
			args = append(args, st)
		}
		q += " AND status IN (" + strings.Join(ph, ",") + ")"
	}
	if err := pool.QueryRow(ctx, q, args...).Scan(&n); err != nil {
		t.Fatalf("count observations: %v", err)
	}
	return n
}

// --- T022: US2 fork shapes ---------------------------------------------------

// TestReorgRecoveryUS2EmptyRange is T022 shape (a) (FR-09/10/11, V4-shape):
// the fork carries no new logs; replay advances every stream legitimately
// with zero new observations while P+C still orphan with full audit.
func TestReorgRecoveryUS2EmptyRange(t *testing.T) {
	s := rrecSetup(t, "rrec-us2-empty")
	ctx := context.Background()

	rrecMineForkA(t, ctx, s)
	rrecIndexForkA(t, ctx, s)
	rrecForkEmpty(t, ctx, s)
	rrecSeedPauses(t, ctx, s)

	ex := rrecExecutor(t, s.pool, s.lease, s)
	id := rrecDriveRecovery(t, ctx, s.pool, ex, s.chainID)

	row, err := LoadRecoveryState(ctx, s.pool, s.chainID)
	if err != nil || row != nil {
		t.Fatalf("recovery row after release = %+v (err=%v), want gone", row, err)
	}
	// Zero new observations in the swept range: only P+C, both Orphaned.
	if n := rrecCountObs(t, ctx, s.pool, s.chainID, s.hA, s.hTip); n != 2 {
		t.Fatalf("observations in sweep = %d, want 2 (P+C only, zero minted)", n)
	}
	if n := rrecCountObs(t, ctx, s.pool, s.chainID, s.hA, s.hTip, "orphaned"); n != 2 {
		t.Fatalf("orphaned in sweep = %d, want 2", n)
	}
	if n := rrecCountObs(t, ctx, s.pool, s.chainID, s.hA, s.hTip, "pending", "confirmed"); n != 0 {
		t.Fatalf("effective observations in sweep = %d, want 0", n)
	}
	// No new chain rows beyond the fork-B headers; no new logs at all.
	var totalBlocks int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM chain_blocks WHERE chain_id=$1 AND number>$2 AND number<=$3`,
		s.chainID, int64(s.hA), int64(s.hTip)).Scan(&totalBlocks); err != nil || totalBlocks != 2*int(s.hTip-s.hA) {
		t.Fatalf("sweep block rows = %d (err=%v), want %d (A-retained + B-new)", totalBlocks, err, 2*(s.hTip-s.hA))
	}
	var newLogs int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM erc20_transfer_logs WHERE chain_id=$1 AND block_number>$2 AND block_number<=$3`,
		s.chainID, int64(s.hA), int64(s.hTip)).Scan(&newLogs); err != nil {
		t.Fatalf("count sweep logs: %v", err)
	}
	// Fork-A logs (3: K? no — K is below hA) — C+P logs retained = 2.
	if newLogs != 2 {
		t.Fatalf("sweep log rows = %d, want 2 (C+P retained, zero new)", newLogs)
	}
	// Checkpoints + frontiers advanced across the legitimately-empty range.
	var cpH int64
	var cpHash string
	if err := s.pool.QueryRow(ctx, `SELECT height, block_hash FROM indexer_checkpoint WHERE chain_id=$1`,
		s.chainID).Scan(&cpH, &cpHash); err != nil {
		t.Fatalf("read 002 checkpoint: %v", err)
	}
	if cpH != int64(s.hTip) || cpHash != s.bHashes[s.hTip] {
		t.Fatalf("002 checkpoint = (%d %s), want fork-B tip (%d %s)", cpH, cpHash, s.hTip, s.bHashes[s.hTip])
	}
	for _, tc := range []struct {
		table string
		want  int64
	}{
		{"log_checkpoint", int64(s.hTip + 1)}, {"deposit_checkpoint", int64(s.hTip + 1)},
	} {
		var next int64
		if err := s.pool.QueryRow(ctx, fmt.Sprintf(`SELECT next_block FROM %s WHERE chain_id=$1`, tc.table),
			s.chainID).Scan(&next); err != nil || next != tc.want {
			t.Fatalf("%s next = %d (err=%v), want %d", tc.table, next, err, tc.want)
		}
	}
	// Audit: one terminal release, both conversions, empty replay still
	// emits per-stream progress (legitimate-empty advance, never a stall).
	if n := rrecEventCount(t, ctx, s.pool, s.chainID, "auto_completed"); n != 1 {
		t.Fatalf("auto_completed events = %d, want 1", n)
	}
	if n := rrecEventCount(t, ctx, s.pool, s.chainID, "replay_progress"); n != 3 {
		t.Fatalf("replay_progress events = %d, want 3 (empty ranges advance)", n)
	}
	if total := rrecTransitionCount(t, ctx, s.pool, s.chainID, "pending", "orphaned", id) +
		rrecTransitionCount(t, ctx, s.pool, s.chainID, "confirmed", "orphaned", id); total != 2 {
		t.Fatalf("orphan transitions = %d, want 2", total)
	}
	var terminal string
	if err := s.pool.QueryRow(ctx, `SELECT detail FROM reorg_recovery_events WHERE chain_id=$1 AND event='auto_completed'`,
		s.chainID).Scan(&terminal); err != nil {
		t.Fatalf("read terminal event: %v", err)
	}
	for _, want := range []string{"orphaned=2", "revived=0", "surviving_pauses=indexer_pause,deposit_pause"} {
		if !strings.Contains(terminal, want) {
			t.Fatalf("terminal detail missing %q: %q", want, terminal)
		}
	}
}

// TestReorgRecoveryUS2NewDeposit is T022 shape (b) (FR-08/09/10, V4-shape):
// the fork carries a genuinely new deposit; replay mints exactly one
// new-identity observation (new block_hash + tx_hash) while the old rows
// stay Orphaned unmerged, then 005 confirms it on the new basis.
func TestReorgRecoveryUS2NewDeposit(t *testing.T) {
	s := rrecSetup(t, "rrec-us2-newdep")
	ctx := context.Background()

	rrecMineForkA(t, ctx, s)
	rrecIndexForkA(t, ctx, s)
	rrecForkB(t, ctx, s)
	rrecSeedPauses(t, ctx, s)

	ex := rrecExecutor(t, s.pool, s.lease, s)
	id := rrecDriveRecovery(t, ctx, s.pool, ex, s.chainID)

	// New identity, not a merge: distinct block_hash AND tx_hash from every
	// fork-A observation, pending, version_seq inherited, amount 11.
	bTx := strings.ToLower(s.bTx.Hex())
	bObs := rrecReadObs(t, ctx, s.pool, s.chainID, s.bBH, bTx)
	if bObs.status != "pending" || bObs.blockNumber != int64(s.hA+1) || bObs.amount != "11" || bObs.version != 1 {
		t.Fatalf("new-deposit observation = %+v, want (pending @%d amount 11 v1)", bObs, s.hA+1)
	}
	for _, tc := range []struct {
		name, bh, tx string
	}{
		{"K", s.kBH, strings.ToLower(s.kTx.Hex())},
		{"C", s.cBH, strings.ToLower(s.cTx.Hex())},
		{"P", s.pBH, strings.ToLower(s.pTx.Hex())},
	} {
		if tc.bh == s.bBH || tc.tx == bTx {
			t.Fatalf("new identity collides with %s (%s/%s)", tc.name, tc.bh, tc.tx)
		}
	}
	if n := rrecCountObs(t, ctx, s.pool, s.chainID, s.hA, s.hTip); n != 3 {
		t.Fatalf("observations in sweep = %d, want 3 (P+C orphaned + 1 minted)", n)
	}
	// Old rows unmerged: C still carries its stale basis + orphan evidence.
	cObs := rrecReadObs(t, ctx, s.pool, s.chainID, s.cBH, strings.ToLower(s.cTx.Hex()))
	if cObs.status != "orphaned" || cObs.orphanID != id || cObs.confirmedAtNull {
		t.Fatalf("C after new-deposit fork = %+v, want orphaned with retained basis", cObs)
	}
	// Release + clear, then 005 confirms the minted observation on the live
	// new-chain basis (zero old-basis reuse: new tip hash).
	rrecClearPauses(t, ctx, s)
	m := metrics.New(func() bool { return true })
	confirmCfg := ConfirmationConfig{
		ChainID: s.chainID, ThresholdN: rrecThresholdN,
		PollInterval: 25 * time.Millisecond, RetryInitial: 25 * time.Millisecond, RetryMax: 250 * time.Millisecond,
	}
	committer, err := NewConfirmationCommitter(s.pool, confirmCfg)
	if err != nil {
		t.Fatalf("NewConfirmationCommitter(): %v", err)
	}
	sc, err := NewConfirmationScanner(s.pool, confirmCfg, committer, m)
	if err != nil {
		t.Fatalf("NewConfirmationScanner(): %v", err)
	}
	stop := confirm13RunLoop(t, ctx, sc, s.lease)
	waitUntil(t, time.Now().Add(60*time.Second), "minted observation confirmed", func() bool {
		var k int
		_ = s.pool.QueryRow(ctx, `SELECT count(*) FROM deposit_observations WHERE chain_id=$1 AND block_hash=$2 AND tx_hash=$3 AND status='confirmed'`,
			s.chainID, s.bBH, bTx).Scan(&k)
		return k == 1
	})
	time.Sleep(200 * time.Millisecond)
	stop()
	status, nullAt, tipN, thr, seq, gotTip, conf := confirmReadBasis(t, ctx, s.pool, s.chainID, s.bBH, bTx)
	wantConf := fmt.Sprintf("%d", s.hTip-(s.hA+1)+1)
	if status != "confirmed" || nullAt || tipN != int64(s.hTip) || gotTip != s.bHashes[s.hTip] ||
		thr != int64(rrecThresholdN) || seq != 1 || conf != wantConf {
		t.Fatalf("minted basis = (%s null=%v tip %d %s N=%d conf=%s seq=%d), want confirmed tip(%d %s) N=%d conf=%s seq=1",
			status, nullAt, tipN, gotTip, thr, conf, seq, s.hTip, s.bHashes[s.hTip], rrecThresholdN, wantConf)
	}
}

// TestReorgRecoveryUS2Remined is T022 shape (c) (FR-08, V4-shape): the same
// txs re-mined — X with a shifted log index (extra accounts[1] log packed in
// the same block, same tx hash) and Y with rewritten log content (same tx
// hash, amount 9 → 99). Both mint new-identity rows on their original tx
// history lines; the old rows stay Orphaned unmerged with old amounts.
func TestReorgRecoveryUS2Remined(t *testing.T) {
	s := rrecSetup(t, "rrec-us2-remined")
	ctx := context.Background()

	rrecMineForkA(t, ctx, s)
	rrecIndexForkA(t, ctx, s)
	ids := rrecForkRemined(t, ctx, s)
	rrecSeedPauses(t, ctx, s)

	ex := rrecExecutor(t, s.pool, s.lease, s)
	id := rrecDriveRecovery(t, ctx, s.pool, ex, s.chainID)

	cTx, pTx := strings.ToLower(s.cTx.Hex()), strings.ToLower(s.pTx.Hex())
	// Index-change: same tx hash, new block hash, log_index 0 → 1.
	xNew := rrecReadObsIdx(t, ctx, s.pool, s.chainID, ids.xBH, cTx, 1)
	if xNew.status != "pending" || xNew.amount != "5" || xNew.version != 1 {
		t.Fatalf("X-new = %+v, want (pending amount 5 v1)", xNew)
	}
	var xLogIdx int64
	if err := s.pool.QueryRow(ctx, `SELECT log_index FROM deposit_observations WHERE chain_id=$1 AND block_hash=$2 AND tx_hash=$3`,
		s.chainID, ids.xBH, cTx).Scan(&xLogIdx); err != nil || xLogIdx != 1 {
		t.Fatalf("X-new log_index = %d (err=%v), want 1", xLogIdx, err)
	}
	// Content-change: same tx hash, new block hash, amount 9 → 99.
	yNew := rrecReadObs(t, ctx, s.pool, s.chainID, ids.yBH, pTx)
	if yNew.status != "pending" || yNew.amount != ids.yAmount || yNew.version != 1 {
		t.Fatalf("Y-new = %+v, want (pending amount %s v1)", yNew, ids.yAmount)
	}
	// Old rows unmerged: same tx hashes coexist as Orphaned with OLD
	// block hashes, OLD indices, OLD amounts and this round's evidence.
	xOld := rrecReadObs(t, ctx, s.pool, s.chainID, s.cBH, cTx)
	if xOld.status != "orphaned" || xOld.orphanID != id {
		t.Fatalf("X-old = %+v, want orphaned under this recovery", xOld)
	}
	var xOldIdx int64
	if err := s.pool.QueryRow(ctx, `SELECT log_index FROM deposit_observations WHERE chain_id=$1 AND block_hash=$2 AND tx_hash=$3`,
		s.chainID, s.cBH, cTx).Scan(&xOldIdx); err != nil || xOldIdx != 0 {
		t.Fatalf("X-old log_index = %d (err=%v), want 0 (unmerged)", xOldIdx, err)
	}
	yOld := rrecReadObs(t, ctx, s.pool, s.chainID, s.pBH, pTx)
	if yOld.status != "orphaned" || yOld.orphanID != id || yOld.amount != "9" {
		t.Fatalf("Y-old = %+v, want orphaned with old amount 9", yOld)
	}
	// Same-tx coexistence: two rows per re-mined tx hash, distinct sources.
	for _, tx := range []string{cTx, pTx} {
		var n int
		if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM deposit_observations WHERE chain_id=$1 AND tx_hash=$2`,
			s.chainID, tx).Scan(&n); err != nil || n != 2 {
			t.Fatalf("rows for re-mined tx %s = %d (err=%v), want 2 (old+new, never merged)", tx, n, err)
		}
	}
	// The packing log minted its own observation (full coverage, no loss).
	zObs := rrecReadObs(t, ctx, s.pool, s.chainID, ids.xBH, strings.ToLower(ids.zTx.Hex()))
	if zObs.status != "pending" || zObs.amount != "11" {
		t.Fatalf("Z packing observation = %+v, want (pending amount 11)", zObs)
	}
	// Audit: both conversions per re-mined tx, terminal dispositions.
	if n := rrecTransitionCount(t, ctx, s.pool, s.chainID, "confirmed", "orphaned", id); n != 1 {
		t.Fatalf("confirmed->orphaned = %d, want 1 (X-old)", n)
	}
	if n := rrecTransitionCount(t, ctx, s.pool, s.chainID, "pending", "orphaned", id); n != 1 {
		t.Fatalf("pending->orphaned = %d, want 1 (Y-old)", n)
	}
	var terminal string
	if err := s.pool.QueryRow(ctx, `SELECT detail FROM reorg_recovery_events WHERE chain_id=$1 AND event='auto_completed'`,
		s.chainID).Scan(&terminal); err != nil {
		t.Fatalf("read terminal event: %v", err)
	}
	for _, want := range []string{"orphaned=2", "revived=0", "surviving_pauses=indexer_pause,deposit_pause"} {
		if !strings.Contains(terminal, want) {
			t.Fatalf("terminal detail missing %q: %q", want, terminal)
		}
	}
	// Release + clear, then 005 confirms X-new (now deepest) while Y-new
	// stays pending below threshold and old rows stay Orphaned.
	rrecClearPauses(t, ctx, s)
	m := metrics.New(func() bool { return true })
	confirmCfg := ConfirmationConfig{
		ChainID: s.chainID, ThresholdN: rrecThresholdN,
		PollInterval: 25 * time.Millisecond, RetryInitial: 25 * time.Millisecond, RetryMax: 250 * time.Millisecond,
	}
	committer, err := NewConfirmationCommitter(s.pool, confirmCfg)
	if err != nil {
		t.Fatalf("NewConfirmationCommitter(): %v", err)
	}
	sc, err := NewConfirmationScanner(s.pool, confirmCfg, committer, m)
	if err != nil {
		t.Fatalf("NewConfirmationScanner(): %v", err)
	}
	stop := confirm13RunLoop(t, ctx, sc, s.lease)
	waitUntil(t, time.Now().Add(60*time.Second), "X-new confirmed", func() bool {
		var k int
		_ = s.pool.QueryRow(ctx, `SELECT count(*) FROM deposit_observations WHERE chain_id=$1 AND block_hash=$2 AND tx_hash=$3 AND status='confirmed'`,
			s.chainID, ids.xBH, cTx).Scan(&k)
		return k == 1
	})
	time.Sleep(200 * time.Millisecond)
	stop()
	if o := rrecReadObs(t, ctx, s.pool, s.chainID, ids.yBH, pTx); o.status != "pending" {
		t.Fatalf("Y-new = %s, want pending (below threshold)", o.status)
	}
	if o := rrecReadObs(t, ctx, s.pool, s.chainID, s.cBH, cTx); o.status != "orphaned" {
		t.Fatalf("X-old = %s, want orphaned (never resurrected by 005)", o.status)
	}
	if o := rrecReadObs(t, ctx, s.pool, s.chainID, s.pBH, pTx); o.status != "orphaned" {
		t.Fatalf("Y-old = %s, want orphaned", o.status)
	}
}

// TestReorgRecoveryUS2Recanonicalize is T022 shape (d) (FR-08 Q4, V4-shape)
// plus the V4 tail: mid-recovery the chain returns byte-identically to fork
// A (state restore — re-mining can never reproduce hashes), so replay takes
// the recanonicalize + revive path within the SAME recovery: the ORIGINAL
// rows are reused (zero new observations), Orphaned→Pending only with
// re-verified block/log-binding/history semantics, and the ex-Confirmed C
// passes through Pending for a live 005 reconfirmation. Release retains the
// orphaned history (P + fork-B transfer) while new Pendings (F) arrive and
// confirm; transitions carry version/basis/time; repeat revive converts
// exactly once; stale workers refuse; SC-02 green.
func TestReorgRecoveryUS2Recanonicalize(t *testing.T) {
	s := rrecSetup(t, "rrec-us2-recanon")
	ctx := context.Background()

	rrecMineForkA(t, ctx, s)
	rrecIndexForkA(t, ctx, s)
	aDump := rrecDumpState(t, s)
	rrecForkB(t, ctx, s)
	rrecSeedPauses(t, ctx, s)

	cap0 := testRecoveryCap(t, ctx, s.pool, s.chainID)
	ex := rrecExecutorBatch(t, s.pool, s.lease, s, 1)
	done, err := ex.tickIdle(ctx)
	if err != nil || !done {
		t.Fatalf("tickIdle() = (%v, %v), want (true, nil)", done, err)
	}
	row, cap := rrecRow(t, ctx, s.pool, s.chainID)
	if err := ex.tickDetected(ctx, row); err != nil {
		t.Fatalf("tickDetected(): %v", err)
	}
	row, cap = rrecRow(t, ctx, s.pool, s.chainID)
	if row.AncestorNumber == nil || *row.AncestorNumber != int64(s.hA) {
		t.Fatalf("ancestor = %v, want %d", row.AncestorNumber, s.hA)
	}
	if err := ex.tickInvalidate(ctx, row, cap); err != nil {
		t.Fatalf("tickInvalidate(): %v", err)
	}
	row, cap = rrecRow(t, ctx, s.pool, s.chainID)
	cTx, pTx := strings.ToLower(s.cTx.Hex()), strings.ToLower(s.pTx.Hex())
	if o := rrecReadObs(t, ctx, s.pool, s.chainID, s.cBH, cTx); o.status != "orphaned" {
		t.Fatalf("C post-invalidate = %s, want orphaned", o.status)
	}
	// Mid-recovery return: the chain is byte-identically fork A again.
	rrecLoadState(t, s, aDump, s.hTip)
	if got := rrecHeaderHash(t, ctx, s, s.hTip); got != s.aHashes[s.hTip] {
		t.Fatalf("returned tip = %s, want fork-A %s", got, s.aHashes[s.hTip])
	}
	var blocksBefore int64
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM chain_blocks WHERE chain_id=$1`, s.chainID).Scan(&blocksBefore); err != nil {
		t.Fatalf("count chain_blocks: %v", err)
	}
	var obsBefore int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM deposit_observations WHERE chain_id=$1`, s.chainID).Scan(&obsBefore); err != nil {
		t.Fatalf("count observations: %v", err)
	}
	// First replay tick flips only the first swept height (batch width 1):
	// C recanonicalizes + revives while the phase is still replaying, so
	// repeat-execution converges here instead of against a completed round.
	row, cap = rrecRow(t, ctx, s.pool, s.chainID)
	if err := ex.tickReplay(ctx, row, cap); err != nil {
		t.Fatalf("tickReplay #0: %v", err)
	}
	row, cap = rrecRow(t, ctx, s.pool, s.chainID)
	if row.Phase != reorgPhaseReplaying {
		t.Fatalf("phase after first replay tick = %q, want replaying", row.Phase)
	}
	if cRev0 := rrecReadObs(t, ctx, s.pool, s.chainID, s.cBH, cTx); cRev0.status != "pending" {
		t.Fatalf("C after first tick = %s, want pending (revived)", cRev0.status)
	}
	if err := ReviveRecoveryObservation(ctx, s.pool, s.lease, s.chainID, cap, s.cBH, cTx, 0, "rrec repeat"); err != nil {
		t.Fatalf("repeat revive = %v, want idempotent nil", err)
	}
	if n := rrecTransitionCount(t, ctx, s.pool, s.chainID, "orphaned", "pending", row.RecoveryID); n != 1 {
		t.Fatalf("orphaned->pending transitions after repeat = %d, want still 1", n)
	}
	if err := RecanonicalizeRecoveryBlock(ctx, s.pool, s.lease, s.chainID, cap, int64(s.hA+1), s.aHashes[s.hA+1]); !isRecoveryGate(err) {
		t.Fatalf("double recanonicalize = %v, want gate refusal (flips happen once)", err)
	}
	for i := 1; i < 32; i++ {
		row, cap = rrecRow(t, ctx, s.pool, s.chainID)
		if row.Phase == reorgPhaseCompletePending {
			break
		}
		if err := ex.tickReplay(ctx, row, cap); err != nil {
			t.Fatalf("tickReplay #%d: %v", i, err)
		}
	}
	row, cap = rrecRow(t, ctx, s.pool, s.chainID)
	if row.Phase != reorgPhaseCompletePending {
		t.Fatalf("phase = %q after replay, want complete_pending", row.Phase)
	}
	// Zero new rows: recanonicalize flips in place, revive reuses in place.
	var blocksAfter int64
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM chain_blocks WHERE chain_id=$1`, s.chainID).Scan(&blocksAfter); err != nil {
		t.Fatalf("count chain_blocks: %v", err)
	}
	if blocksAfter != blocksBefore {
		t.Fatalf("chain_blocks rows %d → %d, want unchanged (flip, never insert)", blocksBefore, blocksAfter)
	}
	var obsAfter int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM deposit_observations WHERE chain_id=$1`, s.chainID).Scan(&obsAfter); err != nil {
		t.Fatalf("count observations: %v", err)
	}
	if obsAfter != obsBefore {
		t.Fatalf("observation rows %d → %d, want unchanged (revive reuses)", obsBefore, obsAfter)
	}
	// Heights canonical again with the ORIGINAL hashes; orphans revived to
	// Pending (C passes through Pending — never straight to Confirmed).
	for h := s.hA + 1; h <= s.hTip; h++ {
		var ch string
		if err := s.pool.QueryRow(ctx, `SELECT hash FROM chain_blocks WHERE chain_id=$1 AND number=$2 AND canonical`,
			s.chainID, int64(h)).Scan(&ch); err != nil || ch != s.aHashes[h] {
			t.Fatalf("canonical[%d] = %s (err=%v), want original %s", h, ch, err, s.aHashes[h])
		}
	}
	cRev := rrecReadObs(t, ctx, s.pool, s.chainID, s.cBH, cTx)
	if cRev.status != "pending" {
		t.Fatalf("C post-replay = %s, want pending (ex-Confirmed via revive)", cRev.status)
	}
	pRev := rrecReadObs(t, ctx, s.pool, s.chainID, s.pBH, pTx)
	if pRev.status != "pending" {
		t.Fatalf("P post-replay = %s, want pending (revived)", pRev.status)
	}
	// Revive audit: one orphaned→pending conversion per observation, with
	// re-verify evidence; repeat execution converges (exactly once).
	if n := rrecTransitionCount(t, ctx, s.pool, s.chainID, "orphaned", "pending", row.RecoveryID); n != 2 {
		t.Fatalf("orphaned->pending transitions = %d, want 2 (C+P)", n)
	}
	var cSnap string
	if err := s.pool.QueryRow(ctx, `SELECT basis_snapshot FROM deposit_observation_transitions
WHERE chain_id=$1 AND block_hash=$2 AND tx_hash=$3 AND from_status='orphaned' AND to_status='pending' AND recovery_id=$4`,
		s.chainID, s.cBH, cTx, row.RecoveryID).Scan(&cSnap); err != nil {
		t.Fatalf("read C revive snapshot: %v", err)
	}
	if !strings.Contains(cSnap, "canonical_reverified") && !strings.Contains(cSnap, "reverified") && !strings.Contains(cSnap, "evidence=") {
		t.Fatalf("C revive snapshot %q carries no re-verify evidence", cSnap)
	}
	// Release, then the tail: surviving pauses, teardown, fresh transfer F,
	// chain extension, live 005 reconfirmation of ex-Confirmed C + F.
	if err := ex.tickComplete(ctx, row, cap); err != nil {
		t.Fatalf("tickComplete(): %v", err)
	}
	if prow, err := LoadRecoveryState(ctx, s.pool, s.chainID); err != nil || prow != nil {
		t.Fatalf("recovery row after release = %+v (err=%v), want gone", prow, err)
	}
	var terminal string
	if err := s.pool.QueryRow(ctx, `SELECT detail FROM reorg_recovery_events WHERE chain_id=$1 AND event='auto_completed'`,
		s.chainID).Scan(&terminal); err != nil {
		t.Fatalf("read terminal event: %v", err)
	}
	for _, want := range []string{"revived=2", "surviving_pauses=indexer_pause,deposit_pause"} {
		if !strings.Contains(terminal, want) {
			t.Fatalf("terminal detail missing %q: %q", want, terminal)
		}
	}
	// Tail: surviving pauses, teardown, fresh transfer F, chain extension,
	// stale-worker refusal, live 005 reconfirmation of ex-Confirmed C + F.
	rrecClearPauses(t, ctx, s)
	fTx, fH := rrecSendRaw(t, s, map[string]any{
		"from": s.node.accounts[0], "to": s.tokenK.Hex(), "data": "0x", "gas": "0x30d40",
	})
	if fH != s.hTip+1 {
		t.Fatalf("F height = %d, want %d", fH, s.hTip+1)
	}
	s.node.mine(t, 3)
	newTip := s.node.blockNumber(t)
	if newTip != s.hTip+4 {
		t.Fatalf("extended tip = %d, want %d", newTip, s.hTip+4)
	}
	fBH := rrecHeaderHash(t, ctx, s, fH)
	fTxHex := strings.ToLower(fTx.Hex())

	sc002b, err := NewScanner(s.pool, s.client, s.lease, Config{
		StartHeight: 0, RPCTimeout: 2 * time.Second, PollInterval: 25 * time.Millisecond,
		RetryInitial: 25 * time.Millisecond, RetryMax: 250 * time.Millisecond,
	}, logscanLogger())
	if err != nil {
		t.Fatalf("NewScanner(): %v", err)
	}
	rrecRunScanTo(t, sc002b, newTip, 60*time.Second)
	lsb := logscanNewScanner(t, s.pool, s.client, s.client, s.lease, LogConfig{
		StartBlock: 0, Contracts: s.tokens, ConfigHash: s.logHash, BatchBlocks: 2,
	})
	logscanServeTo(t, ctx, s.pool, s.chainID, lsb, newTip+1, 60*time.Second)
	sc004b, err := NewDepositScanner(s.pool, rrecDepositCfg(s))
	if err != nil {
		t.Fatalf("NewDepositScanner(): %v", err)
	}
	stop004b := depositRunLoop(t, ctx, sc004b, s.lease)
	waitUntil(t, time.Now().Add(60*time.Second), "deposit checkpoint reaches new tip+1", func() bool {
		_, _, next, ok := depositCheckpointState(t, ctx, s.pool, s.chainID)
		return ok && next >= newTip+1
	})
	time.Sleep(100 * time.Millisecond)
	stop004b()
	if o := rrecReadObs(t, ctx, s.pool, s.chainID, fBH, fTxHex); o.status != "pending" {
		t.Fatalf("F after indexing = %s, want pending", o.status)
	}

	// Old worker with the pre-round capture cannot overwrite new state:
	// pauses are gone, so the stale batch reaches the version gate itself.
	committerTail, err := NewConfirmationCommitter(s.pool, ConfirmationConfig{ChainID: s.chainID, ThresholdN: rrecThresholdN})
	if err != nil {
		t.Fatalf("NewConfirmationCommitter(): %v", err)
	}
	staleF := ConfirmBasis{
		BlockHash: fBH, TxHash: fTxHex, LogIndex: 0, Height: fH,
		TipNumber: newTip, TipHash: rrecHeaderHash(t, ctx, s, newTip), PolicySeq: 1, ThresholdN: rrecThresholdN,
	}
	if err := committerTail.ConfirmDepositUnit(ctx, s.lease, staleF, cap0); !isRecoveryGate(err) {
		t.Fatalf("stale pre-round confirm of F = %v, want gate refusal", err)
	}
	if o := rrecReadObs(t, ctx, s.pool, s.chainID, fBH, fTxHex); o.status != "pending" {
		t.Fatalf("F after refused stale confirm = %s, want pending (zero writes)", o.status)
	}

	m := metrics.New(func() bool { return true })
	confirmCfg := ConfirmationConfig{
		ChainID: s.chainID, ThresholdN: rrecThresholdN,
		PollInterval: 25 * time.Millisecond, RetryInitial: 25 * time.Millisecond, RetryMax: 250 * time.Millisecond,
	}
	committer, err := NewConfirmationCommitter(s.pool, confirmCfg)
	if err != nil {
		t.Fatalf("NewConfirmationCommitter(): %v", err)
	}
	sc, err := NewConfirmationScanner(s.pool, confirmCfg, committer, m)
	if err != nil {
		t.Fatalf("NewConfirmationScanner(): %v", err)
	}
	stop := confirm13RunLoop(t, ctx, sc, s.lease)
	waitUntil(t, time.Now().Add(60*time.Second), "C and F confirmed", func() bool {
		var k, f int
		_ = s.pool.QueryRow(ctx, `SELECT count(*) FROM deposit_observations WHERE chain_id=$1 AND block_hash=$2 AND tx_hash=$3 AND status='confirmed'`,
			s.chainID, s.cBH, cTx).Scan(&k)
		_ = s.pool.QueryRow(ctx, `SELECT count(*) FROM deposit_observations WHERE chain_id=$1 AND block_hash=$2 AND tx_hash=$3 AND status='confirmed'`,
			s.chainID, fBH, fTxHex).Scan(&f)
		return k == 1 && f == 1
	})
	time.Sleep(200 * time.Millisecond)
	stop()

	// Ex-Confirmed C reconfirmed on the live basis with a FRESH timestamp
	// (recomputation, never old-basis reuse); exactly one row for its tx.
	status, nullAt, tipN, thr, seq, gotTip, conf := confirmReadBasis(t, ctx, s.pool, s.chainID, s.cBH, cTx)
	wantConf := fmt.Sprintf("%d", newTip-s.hC+1)
	newTipHash := rrecHeaderHash(t, ctx, s, newTip)
	if status != "confirmed" || nullAt || tipN != int64(newTip) || gotTip != newTipHash ||
		thr != int64(rrecThresholdN) || seq != 1 || conf != wantConf {
		t.Fatalf("C re-basis = (%s null=%v tip %d %s N=%d conf=%s seq=%d), want confirmed tip(%d %s) N=%d conf=%s seq=1",
			status, nullAt, tipN, gotTip, thr, conf, seq, newTip, newTipHash, rrecThresholdN, wantConf)
	}
	// Freshness against the revive audit (the revive pass-through clears
	// confirmed_at/orphaned_at per the approved CHECK, so the old orphan
	// timestamp is not the baseline — the orphaned→pending transition is).
	var reviveAt time.Time
	if err := s.pool.QueryRow(ctx, `SELECT at FROM deposit_observation_transitions
WHERE chain_id=$1 AND block_hash=$2 AND tx_hash=$3 AND from_status='orphaned' AND to_status='pending' AND recovery_id=$4`,
		s.chainID, s.cBH, cTx, row.RecoveryID).Scan(&reviveAt); err != nil {
		t.Fatalf("read C revive transition time: %v", err)
	}
	var reconfAt time.Time
	if err := s.pool.QueryRow(ctx, `SELECT confirmed_at FROM deposit_observations
WHERE chain_id=$1 AND block_hash=$2 AND tx_hash=$3`,
		s.chainID, s.cBH, cTx).Scan(&reconfAt); err != nil || !reconfAt.After(reviveAt) {
		t.Fatalf("C confirmed_at = %v (err=%v), want after revive transition %v", reconfAt, err, reviveAt)
	}
	var cRows int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM deposit_observations WHERE chain_id=$1 AND tx_hash=$2`,
		s.chainID, cTx).Scan(&cRows); err != nil || cRows != 1 {
		t.Fatalf("rows for C tx = %d (err=%v), want 1 (revive reuses, zero new)", cRows, err)
	}
	// Revived P reconfirms on the live chain too (same-hash revival is a
	// valid observation, not a historical orphan): pending → confirmed with
	// a fresh basis. Historical-orphan exclusion is proven by T023, not by
	// starving a revived row here.
	pStatus, pNullAt, pTipN, pThr, pSeq, pGotTip, pConf := confirmReadBasis(t, ctx, s.pool, s.chainID, s.pBH, pTx)
	wantPConf := fmt.Sprintf("%d", newTip-s.hP+1)
	if pStatus != "confirmed" || pNullAt || pTipN != int64(newTip) || pGotTip != newTipHash ||
		pThr != int64(rrecThresholdN) || pSeq != 1 || pConf != wantPConf {
		t.Fatalf("P re-basis = (%s null=%v tip %d %s N=%d conf=%s seq=%d), want confirmed tip(%d %s) N=%d conf=%s seq=1",
			pStatus, pNullAt, pTipN, pGotTip, pThr, pConf, pSeq, newTip, newTipHash, rrecThresholdN, wantPConf)
	}
	// The fork-B transfer was never indexed (B bytes never passed the
	// ordinary loops), so it owns no observation row — history is retained
	// as blocks + audit, never as phantom observations.
	var bRows int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM deposit_observations WHERE chain_id=$1 AND tx_hash=$2`,
		s.chainID, strings.ToLower(s.bTx.Hex())).Scan(&bRows); err != nil || bRows != 0 {
		t.Fatalf("rows for fork-B tx = %d (err=%v), want 0 (never indexed)", bRows, err)
	}
	var confirmedN int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM deposit_observations WHERE chain_id=$1 AND status='confirmed'`,
		s.chainID).Scan(&confirmedN); err != nil || confirmedN != 4 {
		t.Fatalf("confirmed rows = %d (err=%v), want 4 (K+C+F+P)", confirmedN, err)
	}
	// Transition audit: all four conversions carry version/basis/time.
	var badTransitions int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM deposit_observation_transitions
WHERE chain_id=$1 AND recovery_id=$2 AND (basis_snapshot IS NULL OR basis_snapshot = '' OR at IS NULL)`,
		s.chainID, row.RecoveryID).Scan(&badTransitions); err != nil || badTransitions != 0 {
		t.Fatalf("transitions missing version/basis/time = %d (err=%v), want 0", badTransitions, err)
	}
	if total := rrecTransitionCount(t, ctx, s.pool, s.chainID, "confirmed", "orphaned", row.RecoveryID) +
		rrecTransitionCount(t, ctx, s.pool, s.chainID, "pending", "orphaned", row.RecoveryID) +
		rrecTransitionCount(t, ctx, s.pool, s.chainID, "orphaned", "pending", row.RecoveryID); total != 4 {
		t.Fatalf("total transitions = %d, want 4 (C conf→orph, P pend→orph, C+P orph→pend)", total)
	}
	// SC-02 no-omission: every height carries exactly one canonical row.
	var canonN int64
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM chain_blocks WHERE chain_id=$1 AND canonical`,
		s.chainID).Scan(&canonN); err != nil || canonN != int64(newTip)+1 {
		t.Fatalf("canonical rows = %d (err=%v), want %d (genesis..tip, no gaps)", canonN, err, newTip+1)
	}
}

// --- T024: US3 skew/start/empty (FR-04/11, V5; SC-03) ------------------------
//
// Three Anvil E2E scenes on the same rrec harness (snapshot/revert/mine,
// real executor ticks, never hand-edited rows or hand-advanced phases):
// (a) skewed checkpoints — log behind block, deposit behind log, every lag
// point above the ancestor — roll back to the minimum affected point;
// (b) ancestor before the log/deposit scan starts — each stream floors at its
// own start with no backfill of below-start history;
// (c) a legitimately-empty fork tail advances every stream with zero new
// observations and no gap markers.
// Every scene asserts checkpoint/frontier/identity/status/audit plus the
// preserved CHECK (next_block >= start_block) with no DDL change. US1/US2
// helpers above are untouched; only new rrec helpers are added here.

// rrecLogCfgStart is the 003 configuration over the scene whitelist with an
// explicit scan start + batch width (T024 skew/floor scenes; the T020 shape
// stays frozen in rrecIndexForkA).
func rrecLogCfgStart(s *rrecScene, start, batch uint64) LogConfig {
	return LogConfig{
		StartBlock: start, Contracts: s.tokens, ConfigHash: s.logHash, BatchBlocks: batch,
	}
}

// rrecDepositCfgStart is rrecDepositCfg with explicit 004 + upstream starts
// and batch width (a deposit start below the upstream start refuses
// structural — never silently drops — so the floor scene keeps start >=
// logStart).
func rrecDepositCfgStart(s *rrecScene, start, logStart, batch uint64) DepositConfig {
	cfg := rrecDepositCfg(s)
	cfg.StartBlock = start
	cfg.LogStartHeight = logStart
	cfg.BatchBlocks = batch
	return cfg
}

// rrecRunDepositTo runs the REAL 004 loop until next_block reaches want, then
// stops IMMEDIATELY with no settle sleep: the caller owns the lag ceiling,
// and any overshoot is bounded by one in-flight unit plus the persisted log
// watermark (deposit units never run past log next_block).
func rrecRunDepositTo(t *testing.T, ctx context.Context, s *rrecScene, cfg DepositConfig, want uint64) uint64 {
	t.Helper()
	sc, err := NewDepositScanner(s.pool, cfg)
	if err != nil {
		t.Fatalf("NewDepositScanner(): %v", err)
	}
	stop := depositRunLoop(t, ctx, sc, s.lease)
	waitUntil(t, time.Now().Add(60*time.Second), fmt.Sprintf("deposit checkpoint reaches %d", want), func() bool {
		_, _, next, ok := depositCheckpointState(t, ctx, s.pool, s.chainID)
		return ok && next >= want
	})
	stop()
	_, _, next, ok := depositCheckpointState(t, ctx, s.pool, s.chainID)
	if !ok || next < want {
		t.Fatalf("deposit checkpoint next = %d (ok=%v), want >= %d", next, ok, want)
	}
	return next
}

// rrecConfirmLoop starts the REAL 005 loop with the scene cadence; the caller
// waits its own condition, settles, then stops (the rrecIndexForkA shape).
func rrecConfirmLoop(t *testing.T, ctx context.Context, s *rrecScene) func() {
	t.Helper()
	m := metrics.New(func() bool { return true })
	confirmCfg := ConfirmationConfig{
		ChainID: s.chainID, ThresholdN: rrecThresholdN,
		PollInterval: 25 * time.Millisecond, RetryInitial: 25 * time.Millisecond, RetryMax: 250 * time.Millisecond,
	}
	committer, err := NewConfirmationCommitter(s.pool, confirmCfg)
	if err != nil {
		t.Fatalf("NewConfirmationCommitter(): %v", err)
	}
	sc, err := NewConfirmationScanner(s.pool, confirmCfg, committer, m)
	if err != nil {
		t.Fatalf("NewConfirmationScanner(): %v", err)
	}
	return confirm13RunLoop(t, ctx, sc, s.lease)
}

// rrecCheckpoints is the durable checkpoint triple read in one place.
type rrecCheckpoints struct {
	blockH, blockStart int64
	blockHash          string
	logStart, logNext  int64
	depStart, depNext  int64
}

func rrecReadCheckpoints(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64) rrecCheckpoints {
	t.Helper()
	var c rrecCheckpoints
	if err := pool.QueryRow(ctx, `SELECT height, block_hash, start_height FROM indexer_checkpoint WHERE chain_id=$1`,
		chainID).Scan(&c.blockH, &c.blockHash, &c.blockStart); err != nil {
		t.Fatalf("read indexer_checkpoint: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT start_block, next_block FROM log_checkpoint WHERE chain_id=$1`,
		chainID).Scan(&c.logStart, &c.logNext); err != nil {
		t.Fatalf("read log_checkpoint: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT start_block, next_block FROM deposit_checkpoint WHERE chain_id=$1`,
		chainID).Scan(&c.depStart, &c.depNext); err != nil {
		t.Fatalf("read deposit_checkpoint: %v", err)
	}
	return c
}

// rrecDriveToReady runs establish → ancestor → invalidate → replay to
// complete_pending through the REAL executor and returns the pre-release row
// so the test asserts frontiers before the terminal release deletes it.
func rrecDriveToReady(t *testing.T, ctx context.Context, pool *pgxpool.Pool, ex *RecoveryExecutor, chainID int64) (*RecoveryRow, RecoveryCapture) {
	t.Helper()
	done, err := ex.tickIdle(ctx)
	if err != nil || !done {
		t.Fatalf("tickIdle() = (%v, %v), want (true, nil)", done, err)
	}
	row, cap := rrecRow(t, ctx, pool, chainID)
	if row.Phase != reorgPhaseDetected {
		t.Fatalf("phase = %q, want detected", row.Phase)
	}
	if err := ex.tickDetected(ctx, row); err != nil {
		t.Fatalf("tickDetected(): %v", err)
	}
	row, cap = rrecRow(t, ctx, pool, chainID)
	if row.Phase != reorgPhaseAncestorConfirmed {
		t.Fatalf("phase = %q, want ancestor_confirmed (search must pin, never hold here)", row.Phase)
	}
	if err := ex.tickInvalidate(ctx, row, cap); err != nil {
		t.Fatalf("tickInvalidate(): %v", err)
	}
	for i := 0; i < 10; i++ {
		row, cap = rrecRow(t, ctx, pool, chainID)
		if row.Phase == reorgPhaseCompletePending {
			return row, cap
		}
		if err := ex.tickReplay(ctx, row, cap); err != nil {
			t.Fatalf("tickReplay #%d: %v", i, err)
		}
	}
	row, cap = rrecRow(t, ctx, pool, chainID)
	if row.Phase != reorgPhaseCompletePending {
		t.Fatalf("phase = %q after replay, want complete_pending", row.Phase)
	}
	return row, cap
}

// rrecRelease runs the REAL terminal release and returns the released id.
func rrecRelease(t *testing.T, ctx context.Context, pool *pgxpool.Pool, ex *RecoveryExecutor, chainID int64, row *RecoveryRow, cap RecoveryCapture) string {
	t.Helper()
	id := row.RecoveryID
	if err := ex.tickComplete(ctx, row, cap); err != nil {
		t.Fatalf("tickComplete(): %v", err)
	}
	if prow, err := LoadRecoveryState(ctx, pool, chainID); err != nil || prow != nil {
		t.Fatalf("recovery row after release = %+v (err=%v), want gone", prow, err)
	}
	if n := rrecEventCount(t, ctx, pool, chainID, "auto_completed"); n != 1 {
		t.Fatalf("auto_completed events = %d, want exactly 1", n)
	}
	return id
}

// rrecRollbackDetail returns the checkpoints_rolled_back audit detail for one
// stream (the from/to proof of the min-point rollback).
func rrecRollbackDetail(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, stream string) string {
	t.Helper()
	var detail string
	if err := pool.QueryRow(ctx, `SELECT detail FROM reorg_recovery_events
WHERE chain_id=$1 AND event='checkpoints_rolled_back' AND detail LIKE $2`,
		chainID, "%stream="+stream+" %").Scan(&detail); err != nil {
		t.Fatalf("read %s rollback detail: %v", stream, err)
	}
	return detail
}

// rrecReplayFirstRange returns the first replayed range per stream: replay
// must start exactly at the rolled-back floor — never above it (skip) and
// never below it (backfill).
func rrecReplayFirstRange(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, stream string) (int64, int64) {
	t.Helper()
	rows, err := pool.Query(ctx, `SELECT detail FROM reorg_recovery_events
WHERE chain_id=$1 AND event='replay_progress' AND detail LIKE $2`,
		chainID, "%stream="+stream+" %")
	if err != nil {
		t.Fatalf("read %s replay details: %v", stream, err)
	}
	defer rows.Close()
	var firstFrom, firstTo int64
	first := true
	for rows.Next() {
		var d string
		if err := rows.Scan(&d); err != nil {
			t.Fatalf("scan replay detail: %v", err)
		}
		var rec, st string
		var from, to int64
		var ins, ver int
		if _, err := fmt.Sscanf(d, "recovery=%s stream=%s range=%d-%d inserted=%d version=%d",
			&rec, &st, &from, &to, &ins, &ver); err != nil {
			t.Fatalf("parse replay detail %q: %v", d, err)
		}
		if first || from < firstFrom {
			firstFrom, firstTo = from, to
			first = false
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate replay details: %v", err)
	}
	if first {
		t.Fatalf("no replay_progress rows for stream %s", stream)
	}
	return firstFrom, firstTo
}

// rrecAssertCheckpointChecks guards the T024 storage invariant: the CHECK
// (next_block >= start_block) still exists on both stream tables (no DDL
// change) with every row satisfying it, and the header checkpoint still names
// a live canonical row (the composite FK target — rollbacks land on persisted
// state, never air).
func rrecAssertCheckpointChecks(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64) {
	t.Helper()
	for _, tbl := range []string{"log_checkpoint", "deposit_checkpoint"} {
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_constraint
WHERE conrelid = $1::regclass AND contype = 'c'
AND pg_get_constraintdef(oid) LIKE '%next_block >= start_block%'`, tbl).Scan(&n); err != nil || n != 1 {
			t.Fatalf("%s CHECK(next_block >= start_block) constraints = %d (err=%v), want 1 (no DDL change)", tbl, n, err)
		}
		var bad int
		if err := pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s WHERE chain_id=$1 AND next_block < start_block`, tbl),
			chainID).Scan(&bad); err != nil || bad != 0 {
			t.Fatalf("%s rows violating next>=start = %d (err=%v), want 0", tbl, bad, err)
		}
	}
	var cpH int64
	var cpHash string
	if err := pool.QueryRow(ctx, `SELECT height, block_hash FROM indexer_checkpoint WHERE chain_id=$1`,
		chainID).Scan(&cpH, &cpHash); err != nil {
		t.Fatalf("read indexer_checkpoint: %v", err)
	}
	var canon int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM chain_blocks WHERE chain_id=$1 AND number=$2 AND hash=$3 AND canonical`,
		chainID, cpH, cpHash).Scan(&canon); err != nil || canon != 1 {
		t.Fatalf("block checkpoint (%d %s) canonical matches = %d (err=%v), want 1", cpH, cpHash, canon, err)
	}
}

// rrecAssertSeededPausesOnly proves no gap marker landed anywhere: the only
// pause rows are the two seeded inputs (fork-evidence hash_mismatch + the
// independent chain_view_changed) and log_pause stays empty.
func rrecAssertSeededPausesOnly(t *testing.T, ctx context.Context, pool *pgxpool.Pool, s *rrecScene) {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM indexer_pause WHERE chain_id=$1`, s.chainID).Scan(&n); err != nil || n != 1 {
		t.Fatalf("indexer_pause rows = %d (err=%v), want exactly the seeded 1", n, err)
	}
	var height int64
	var expected, actual, kind string
	if err := pool.QueryRow(ctx, `SELECT height, expected_hash, actual_hash, kind FROM indexer_pause WHERE chain_id=$1`,
		s.chainID).Scan(&height, &expected, &actual, &kind); err != nil {
		t.Fatalf("read indexer_pause: %v", err)
	}
	if kind != "hash_mismatch" || height != int64(s.hA+1) || expected != s.aHashes[s.hA+1] || actual != s.bHashes[s.hA+1] {
		t.Fatalf("indexer_pause = (%d %s %s %s), want seeded fork evidence", height, expected, actual, kind)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM deposit_pause WHERE chain_id=$1`, s.chainID).Scan(&n); err != nil || n != 1 {
		t.Fatalf("deposit_pause rows = %d (err=%v), want exactly the seeded 1", n, err)
	}
	var dheight int64
	var dkind string
	if err := pool.QueryRow(ctx, `SELECT height, kind FROM deposit_pause WHERE chain_id=$1`,
		s.chainID).Scan(&dheight, &dkind); err != nil {
		t.Fatalf("read deposit_pause: %v", err)
	}
	if dkind != "chain_view_changed" || dheight != int64(s.hA+1) {
		t.Fatalf("deposit_pause = (%d %s), want seeded independent pause", dheight, dkind)
	}
	var logPauses int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM log_pause WHERE chain_id=$1`, s.chainID).Scan(&logPauses); err != nil || logPauses != 0 {
		t.Fatalf("log_pause rows = %d (err=%v), want 0 (no gap markers)", logPauses, err)
	}
}

// rrecCountTxObs counts every observation row for one tx hash (any status).
func rrecCountTxObs(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, tx string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM deposit_observations WHERE chain_id=$1 AND tx_hash=$2`,
		chainID, tx).Scan(&n); err != nil {
		t.Fatalf("count observations for tx %s: %v", tx, err)
	}
	return n
}

// rrecAssertNoNewChainFootprint asserts the new-chain tail heights carry no
// log or observation rows: the interval was legitimately empty, and progress
// advanced over air — never over dropped data.
func rrecAssertNoNewChainFootprint(t *testing.T, ctx context.Context, pool *pgxpool.Pool, s *rrecScene, from, to uint64) {
	t.Helper()
	for h := from; h <= to; h++ {
		var ln, on int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM erc20_transfer_logs WHERE chain_id=$1 AND block_number=$2 AND block_hash=$3`,
			s.chainID, int64(h), s.bHashes[h]).Scan(&ln); err != nil || ln != 0 {
			t.Fatalf("new-chain logs at %d = %d (err=%v), want 0 (legitimately empty)", h, ln, err)
		}
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM deposit_observations WHERE chain_id=$1 AND block_number=$2 AND block_hash=$3`,
			s.chainID, int64(h), s.bHashes[h]).Scan(&on); err != nil || on != 0 {
			t.Fatalf("new-chain observations at %d = %d (err=%v), want 0 (legitimately empty)", h, on, err)
		}
	}
}

// rrecAssertFrontiers pins all three stream frontiers pre-release: exactly
// the swept tip — full failed-interval coverage with no forward jump past it.
func rrecAssertFrontiers(t *testing.T, row *RecoveryRow, tip uint64) {
	t.Helper()
	for _, tc := range []struct {
		name string
		got  *int64
	}{
		{"block", row.BlockFrontier}, {"log", row.LogFrontier}, {"deposit", row.DepositFrontier},
	} {
		if tc.got == nil || *tc.got != int64(tip) {
			got := int64(-1)
			if tc.got != nil {
				got = *tc.got
			}
			t.Fatalf("%s frontier = %d, want swept tip %d (no shortfall, no jump)", tc.name, got, tip)
		}
	}
}

// TestReorgRecoveryUS3SkewedRollback is T024 scene (a) (FR-11, V5-skew): the
// block checkpoint sits at the tip while the log checkpoint lags behind it
// and the deposit checkpoint lags behind the log — every lag point still
// above the ancestor. Invalidation rolls each stream back to the minimum
// affected point (ancestor+1) with exact from/to audit; replay covers the
// whole failed interval from the floors with no forward jump of the
// never-indexed P position; SC-03 green.
func TestReorgRecoveryUS3SkewedRollback(t *testing.T) {
	s := rrecSetup(t, "rrec-us3-skew")
	ctx := context.Background()

	rrecMineForkA(t, ctx, s)

	// 002 runs full to the tip (block checkpoint at the head).
	sc002, err := NewScanner(s.pool, s.client, s.lease, Config{
		StartHeight: 0, RPCTimeout: 2 * time.Second, PollInterval: 25 * time.Millisecond,
		RetryInitial: 25 * time.Millisecond, RetryMax: 250 * time.Millisecond,
	}, logscanLogger())
	if err != nil {
		t.Fatalf("NewScanner(): %v", err)
	}
	runScanTo(t, sc002, s.hTip, 60*time.Second)

	// 003 runs short (batch 1, exact stop): covers C, never P's height.
	ls1 := logscanNewScanner(t, s.pool, s.client, s.client, s.lease, rrecLogCfgStart(s, 0, 1))
	logscanServeTo(t, ctx, s.pool, s.chainID, ls1, uint64(s.hC+1), 60*time.Second)
	logShort, ok := logscanCheckpointNext(ctx, s.pool, s.chainID)
	if !ok || logShort < uint64(s.hC+1) || logShort > uint64(s.hC+2) {
		t.Fatalf("log checkpoint after short run = %d, want [%d,%d] (covers C, never P)", logShort, s.hC+1, s.hC+2)
	}
	// 004 runs short (batch 1, immediate stop): observes K+C, never P, and
	// can never pass the persisted log watermark.
	depPre := rrecRunDepositTo(t, ctx, s, rrecDepositCfgStart(s, 0, 0, 1), uint64(s.hC+1))
	if depPre < uint64(s.hC+1) || depPre > logShort {
		t.Fatalf("deposit next = %d, want [%d,%d] (covers C, bounded by log)", depPre, s.hC+1, logShort)
	}
	// 003 advances again over P's height (batch 1): strict log-behind-block
	// AND deposit-behind-log with every lag point above the coming floor.
	ls2 := logscanNewScanner(t, s.pool, s.client, s.client, s.lease, rrecLogCfgStart(s, 0, 1))
	logscanServeTo(t, ctx, s.pool, s.chainID, ls2, uint64(s.hP), 60*time.Second)
	logPre, ok := logscanCheckpointNext(ctx, s.pool, s.chainID)
	if !ok || logPre < uint64(s.hP) || logPre > uint64(s.hP+1) {
		t.Fatalf("log checkpoint after extend = %d, want [%d,%d]", logPre, s.hP, s.hP+1)
	}
	if !(depPre < logPre && logPre < uint64(s.hTip+1)) {
		t.Fatalf("skew order broken: deposit=%d log=%d block=%d", depPre, logPre, s.hTip+1)
	}
	if depPre <= uint64(s.hA+1) || logPre <= uint64(s.hA+1) {
		t.Fatalf("lag points must sit above the floor %d: deposit=%d log=%d", s.hA+1, depPre, logPre)
	}
	// Coverage identity pre-fork: exactly K+C observed; P's height never
	// reached the deposit watermark so P owns no row at all.
	var obsN int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM deposit_observations WHERE chain_id=$1`,
		s.chainID).Scan(&obsN); err != nil || obsN != 2 {
		t.Fatalf("pre-fork observations = %d (err=%v), want 2 (K+C)", obsN, err)
	}
	if n := rrecCountTxObs(t, ctx, s.pool, s.chainID, strings.ToLower(s.pTx.Hex())); n != 0 {
		t.Fatalf("pre-fork P observation rows = %d, want 0 (never indexed)", n)
	}
	// 005 confirms K+C on the fork-A basis; snapshots for the untouched /
	// zero-reuse proofs below.
	stop005 := rrecConfirmLoop(t, ctx, s)
	waitUntil(t, time.Now().Add(60*time.Second), "K+C confirmed, P still absent", func() bool {
		var kc, cc, pc int
		_ = s.pool.QueryRow(ctx, `SELECT count(*) FROM deposit_observations WHERE chain_id=$1 AND tx_hash=$2 AND status='confirmed'`,
			s.chainID, strings.ToLower(s.kTx.Hex())).Scan(&kc)
		_ = s.pool.QueryRow(ctx, `SELECT count(*) FROM deposit_observations WHERE chain_id=$1 AND tx_hash=$2 AND status='confirmed'`,
			s.chainID, strings.ToLower(s.cTx.Hex())).Scan(&cc)
		_ = s.pool.QueryRow(ctx, `SELECT count(*) FROM deposit_observations WHERE chain_id=$1 AND tx_hash=$2`,
			s.chainID, strings.ToLower(s.pTx.Hex())).Scan(&pc)
		return kc == 1 && cc == 1 && pc == 0
	})
	time.Sleep(200 * time.Millisecond)
	stop005()
	kStatus, _, kTipN, kThr, kSeq, kTipHash, kConf := confirmReadBasis(t, ctx, s.pool, s.chainID,
		s.kBH, strings.ToLower(s.kTx.Hex()))
	cStatus, _, cTipN, cThr, cSeq, cTipHash, cConf := confirmReadBasis(t, ctx, s.pool, s.chainID,
		s.cBH, strings.ToLower(s.cTx.Hex()))
	if kStatus != "confirmed" || cStatus != "confirmed" {
		t.Fatalf("pre-fork K=(%s) C=(%s), want both confirmed", kStatus, cStatus)
	}

	rrecForkB(t, ctx, s)
	rrecSeedPauses(t, ctx, s)

	ex := rrecExecutor(t, s.pool, s.lease, s)
	row, cap := rrecDriveToReady(t, ctx, s.pool, ex, s.chainID)
	if row.AncestorNumber == nil || *row.AncestorNumber != int64(s.hA) ||
		row.AncestorHash == nil || *row.AncestorHash != s.aHashes[s.hA] {
		t.Fatalf("ancestor = (%v %v), want (%d %s)", row.AncestorNumber, row.AncestorHash, s.hA, s.aHashes[s.hA])
	}
	// Min-point rollback: exact from (the lagged positions) to the floor.
	for _, tc := range []struct {
		stream   string
		from, to int64
	}{
		{"block", int64(s.hTip), int64(s.hA)},
		{"log", int64(logPre), int64(s.hA + 1)},
		{"deposit", int64(depPre), int64(s.hA + 1)},
	} {
		detail := rrecRollbackDetail(t, ctx, s.pool, s.chainID, tc.stream)
		if want := fmt.Sprintf("stream=%s from=%d to=%d", tc.stream, tc.from, tc.to); !strings.Contains(detail, want) {
			t.Fatalf("%s rollback detail %q misses %q (min-point rollback)", tc.stream, detail, want)
		}
	}
	if n := rrecEventCount(t, ctx, s.pool, s.chainID, "checkpoints_rolled_back"); n != 3 {
		t.Fatalf("checkpoints_rolled_back events = %d, want 3 (block+log+deposit)", n)
	}
	// Replay starts exactly at the floors and covers to the swept tip.
	for _, stream := range []string{"block", "log", "deposit"} {
		if from, _ := rrecReplayFirstRange(t, ctx, s.pool, s.chainID, stream); from != int64(s.hA+1) {
			t.Fatalf("%s first replay range starts at %d, want floor %d", stream, from, s.hA+1)
		}
	}
	rrecAssertFrontiers(t, row, s.hTip)
	if n := rrecEventCount(t, ctx, s.pool, s.chainID, "replay_progress"); n != 3 {
		t.Fatalf("replay_progress events = %d, want 3 (one per stream)", n)
	}
	if n := rrecEventCount(t, ctx, s.pool, s.chainID, "reconcile_signaled"); n != 0 {
		t.Fatalf("reconcile_signaled events = %d, want 0 (in-bound skew auto-recovers)", n)
	}
	// Status: K confirmed with basis intact; C orphaned with retained basis
	// + this round's evidence; P still absent (no forward jump of its
	// never-indexed position); B minted pending under historical semantics.
	kObs := rrecReadObs(t, ctx, s.pool, s.chainID, s.kBH, strings.ToLower(s.kTx.Hex()))
	if kObs.status != "confirmed" || kObs.orphanID != "" || !kObs.orphanedAtNull {
		t.Fatalf("K = (%s orphan=%s), want (confirmed, untouched)", kObs.status, kObs.orphanID)
	}
	if kObs.tipNumber != kTipN || kObs.tipHash != kTipHash || kObs.threshold != kThr || kObs.policySeq != kSeq || kObs.confirmations != kConf {
		t.Fatalf("K basis moved under skew rollback: %+v", kObs)
	}
	cObs := rrecReadObs(t, ctx, s.pool, s.chainID, s.cBH, strings.ToLower(s.cTx.Hex()))
	if cObs.status != "orphaned" || cObs.orphanID != row.RecoveryID || cObs.orphanedAtNull {
		t.Fatalf("C = (%s orphan=%s null=%v), want orphaned under this recovery", cObs.status, cObs.orphanID, cObs.orphanedAtNull)
	}
	if cObs.tipNumber != cTipN || cObs.tipHash != cTipHash || cObs.threshold != cThr || cObs.policySeq != cSeq || cObs.confirmations != cConf {
		t.Fatalf("C retained basis = (tip %d %s N=%d conf=%s seq=%d), want pre-fork (%d %s %d %s %d)",
			cObs.tipNumber, cObs.tipHash, cObs.threshold, cObs.confirmations, cObs.policySeq, cTipN, cTipHash, cThr, cConf, cSeq)
	}
	if n := rrecCountTxObs(t, ctx, s.pool, s.chainID, strings.ToLower(s.pTx.Hex())); n != 0 {
		t.Fatalf("P observation rows = %d, want 0 (never-indexed position never jumped)", n)
	}
	bObs := rrecReadObs(t, ctx, s.pool, s.chainID, s.bBH, strings.ToLower(s.bTx.Hex()))
	if bObs.status != "pending" || bObs.blockNumber != int64(s.hA+1) || bObs.amount != "11" || bObs.version != 1 {
		t.Fatalf("fork-B observation = %+v, want (pending @%d amount 11 v1)", bObs, s.hA+1)
	}
	// Identity: swept heights carry exactly the fork-B canonical rows.
	for h := s.hA + 1; h <= s.hTip; h++ {
		var ch string
		var total int
		if err := s.pool.QueryRow(ctx, `SELECT hash FROM chain_blocks WHERE chain_id=$1 AND number=$2 AND canonical`,
			s.chainID, int64(h)).Scan(&ch); err != nil || ch != s.bHashes[h] {
			t.Fatalf("canonical[%d] = %s (err=%v), want fork-B %s", h, ch, err, s.bHashes[h])
		}
		if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM chain_blocks WHERE chain_id=$1 AND number=$2`,
			s.chainID, int64(h)).Scan(&total); err != nil || total != 2 {
			t.Fatalf("total rows at %d = %d (err=%v), want 2 (A-retained + B-new)", h, total, err)
		}
	}
	// The fork-B tail past the mint height is legitimately empty.
	rrecAssertNoNewChainFootprint(t, ctx, s.pool, s, s.hA+2, s.hTip)
	// Checkpoints post-replay: block at the new tip, streams at tip+1.
	cp := rrecReadCheckpoints(t, ctx, s.pool, s.chainID)
	if cp.blockH != int64(s.hTip) || cp.blockHash != s.bHashes[s.hTip] {
		t.Fatalf("002 checkpoint = (%d %s), want fork-B tip (%d %s)", cp.blockH, cp.blockHash, s.hTip, s.bHashes[s.hTip])
	}
	if cp.logNext != int64(s.hTip+1) || cp.logStart != 0 || cp.depNext != int64(s.hTip+1) || cp.depStart != 0 {
		t.Fatalf("stream checkpoints = (log %d/%d deposit %d/%d), want (0/%d 0/%d)",
			cp.logStart, cp.logNext, cp.depStart, cp.depNext, s.hTip+1, s.hTip+1)
	}
	rrecAssertCheckpointChecks(t, ctx, s.pool, s.chainID)
	rrecAssertSeededPausesOnly(t, ctx, s.pool, s)

	id := rrecRelease(t, ctx, s.pool, ex, s.chainID, row, cap)
	var terminal string
	if err := s.pool.QueryRow(ctx, `SELECT detail FROM reorg_recovery_events WHERE chain_id=$1 AND event='auto_completed'`,
		s.chainID).Scan(&terminal); err != nil {
		t.Fatalf("read terminal event: %v", err)
	}
	for _, want := range []string{
		fmt.Sprintf("bound_old=%d:%s", s.hTip, s.aHashes[s.hTip]),
		"policy=1",
		fmt.Sprintf("ancestor=%d:%s", s.hA, s.aHashes[s.hA]),
		fmt.Sprintf("swept=%d-%d", s.hA+1, s.hTip),
		"orphaned=1", "revived=0",
		fmt.Sprintf("replayed=block:%d,log:%d,deposit:%d", s.hTip, s.hTip, s.hTip),
		"surviving_pauses=indexer_pause,deposit_pause",
		"version=1",
	} {
		if !strings.Contains(terminal, want) {
			t.Fatalf("terminal detail missing %q: %q", want, terminal)
		}
	}
	if n := rrecTransitionCount(t, ctx, s.pool, s.chainID, "confirmed", "orphaned", id); n != 1 {
		t.Fatalf("confirmed->orphaned transitions = %d, want 1 (C)", n)
	}
	if n := rrecTransitionCount(t, ctx, s.pool, s.chainID, "pending", "orphaned", id); n != 0 {
		t.Fatalf("pending->orphaned transitions = %d, want 0 (P never existed)", n)
	}
	// Post-release the streams are usable again: clear the independent
	// pauses, and 005 confirms the replay-minted B on the new basis with zero
	// old-basis reuse (K keeps its original basis throughout).
	rrecClearPauses(t, ctx, s)
	stopB := rrecConfirmLoop(t, ctx, s)
	bTxHex := strings.ToLower(s.bTx.Hex())
	waitUntil(t, time.Now().Add(60*time.Second), "minted B confirmed", func() bool {
		var k int
		_ = s.pool.QueryRow(ctx, `SELECT count(*) FROM deposit_observations WHERE chain_id=$1 AND block_hash=$2 AND tx_hash=$3 AND status='confirmed'`,
			s.chainID, s.bBH, bTxHex).Scan(&k)
		return k == 1
	})
	time.Sleep(200 * time.Millisecond)
	stopB()
	bStatus, bNull, bTipN, bThr, bSeq, bTipHash, bConf := confirmReadBasis(t, ctx, s.pool, s.chainID, s.bBH, bTxHex)
	wantBConf := fmt.Sprintf("%d", s.hTip-(s.hA+1)+1)
	if bStatus != "confirmed" || bNull || bTipN != int64(s.hTip) || bTipHash != s.bHashes[s.hTip] ||
		bThr != int64(rrecThresholdN) || bSeq != 1 || bConf != wantBConf {
		t.Fatalf("B basis = (%s null=%v tip %d %s N=%d conf=%s seq=%d), want confirmed tip(%d %s) N=%d conf=%s seq=1",
			bStatus, bNull, bTipN, bTipHash, bThr, bConf, bSeq, s.hTip, s.bHashes[s.hTip], rrecThresholdN, wantBConf)
	}
	if bTipHash == cTipHash {
		t.Fatalf("B reconfirmation reused the old confirm tip %s", cTipHash)
	}
	kStatus2, _, kTipN2, _, _, kTipHash2, kConf2 := confirmReadBasis(t, ctx, s.pool, s.chainID,
		s.kBH, strings.ToLower(s.kTx.Hex()))
	if kStatus2 != "confirmed" || kTipN2 != kTipN || kTipHash2 != kTipHash || kConf2 != kConf {
		t.Fatalf("K basis moved post-release: (%s tip %d %s conf %s)", kStatus2, kTipN2, kTipHash2, kConf2)
	}
}

// TestReorgRecoveryUS3AncestorBeforeStart is T024 scene (b) (FR-04/11,
// V5-floor): the log scan starts above the ancestor and the deposit scan
// starts above the log start. Recovery floors each stream at its own start
// (block → ancestor, log/deposit → their starts) with exact from/to audit
// and replays from those floors with no backfill of below-start history.
// The fork is empty on purpose: a new-chain log below the deposit start
// holds block replay inside the shared log/deposit assembly (no version is
// bound there — deliberate fail-closed hold, out of T024 scope), so the
// floor/no-backfill proof runs over legitimately-empty new history.
func TestReorgRecoveryUS3AncestorBeforeStart(t *testing.T) {
	s := rrecSetup(t, "rrec-us3-floor")
	ctx := context.Background()

	rrecMineForkA(t, ctx, s)
	l1 := s.hA + 2 // log start: above the ancestor, below P
	l2 := s.hA + 3 // deposit start: above the log start, at/below P

	// 002 runs full from genesis (the ancestor stays searchable); 003/004 run
	// late-started to the tip.
	sc002, err := NewScanner(s.pool, s.client, s.lease, Config{
		StartHeight: 0, RPCTimeout: 2 * time.Second, PollInterval: 25 * time.Millisecond,
		RetryInitial: 25 * time.Millisecond, RetryMax: 250 * time.Millisecond,
	}, logscanLogger())
	if err != nil {
		t.Fatalf("NewScanner(): %v", err)
	}
	runScanTo(t, sc002, s.hTip, 60*time.Second)
	ls := logscanNewScanner(t, s.pool, s.client, s.client, s.lease, rrecLogCfgStart(s, l1, 2))
	logscanServeTo(t, ctx, s.pool, s.chainID, ls, uint64(s.hTip+1), 60*time.Second)
	sc004, err := NewDepositScanner(s.pool, rrecDepositCfgStart(s, l2, l1, 500))
	if err != nil {
		t.Fatalf("NewDepositScanner(): %v", err)
	}
	stop004 := depositRunLoop(t, ctx, sc004, s.lease)
	waitUntil(t, time.Now().Add(60*time.Second), "deposit checkpoint reaches tip+1", func() bool {
		_, _, next, ok := depositCheckpointState(t, ctx, s.pool, s.chainID)
		return ok && next >= s.hTip+1
	})
	time.Sleep(100 * time.Millisecond)
	stop004()
	cp := rrecReadCheckpoints(t, ctx, s.pool, s.chainID)
	if cp.blockH != int64(s.hTip) || cp.blockHash != s.aHashes[s.hTip] {
		t.Fatalf("002 checkpoint = (%d %s), want fork-A tip (%d %s)", cp.blockH, cp.blockHash, s.hTip, s.aHashes[s.hTip])
	}
	if cp.logStart != int64(l1) || cp.logNext != int64(s.hTip+1) {
		t.Fatalf("log checkpoint = (start %d next %d), want (%d %d)", cp.logStart, cp.logNext, l1, s.hTip+1)
	}
	if cp.depStart != int64(l2) || cp.depNext != int64(s.hTip+1) {
		t.Fatalf("deposit checkpoint = (start %d next %d), want (%d %d)", cp.depStart, cp.depNext, l2, s.hTip+1)
	}
	// Below-start history was never indexed: no logs in (ancestor, l1), only
	// P observed, C/K own no rows at all. No 005 runs: nothing is confirmable
	// (P sits 3 deep against N=4) and recovery must still close.
	var belowLogs int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM erc20_transfer_logs WHERE chain_id=$1 AND block_number>$2 AND block_number<$3`,
		s.chainID, int64(s.hA), int64(l1)).Scan(&belowLogs); err != nil || belowLogs != 0 {
		t.Fatalf("logs below the log start = %d (err=%v), want 0 (no backfill source)", belowLogs, err)
	}
	var obsN int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM deposit_observations WHERE chain_id=$1`,
		s.chainID).Scan(&obsN); err != nil || obsN != 1 {
		t.Fatalf("pre-fork observations = %d (err=%v), want 1 (P only)", obsN, err)
	}
	pPre := rrecReadObs(t, ctx, s.pool, s.chainID, s.pBH, strings.ToLower(s.pTx.Hex()))
	if pPre.status != "pending" || pPre.blockNumber != int64(s.hP) {
		t.Fatalf("pre-fork P = %+v, want (pending @%d)", pPre, s.hP)
	}
	for _, tc := range []struct {
		name string
		tx   common.Hash
	}{
		{"K", s.kTx}, {"C", s.cTx},
	} {
		if n := rrecCountTxObs(t, ctx, s.pool, s.chainID, strings.ToLower(tc.tx.Hex())); n != 0 {
			t.Fatalf("pre-fork %s observation rows = %d, want 0 (below starts)", tc.name, n)
		}
	}

	rrecForkEmpty(t, ctx, s)
	rrecSeedPauses(t, ctx, s)

	ex := rrecExecutor(t, s.pool, s.lease, s)
	row, cap := rrecDriveToReady(t, ctx, s.pool, ex, s.chainID)
	if row.AncestorNumber == nil || *row.AncestorNumber != int64(s.hA) ||
		row.AncestorHash == nil || *row.AncestorHash != s.aHashes[s.hA] {
		t.Fatalf("ancestor = (%v %v), want (%d %s)", row.AncestorNumber, row.AncestorHash, s.hA, s.aHashes[s.hA])
	}
	// Each stream floors at its own start: block onto the ancestor, log and
	// deposit onto their scan starts.
	for _, tc := range []struct {
		stream   string
		from, to int64
	}{
		{"block", int64(s.hTip), int64(s.hA)},
		{"log", int64(s.hTip + 1), int64(l1)},
		{"deposit", int64(s.hTip + 1), int64(l2)},
	} {
		detail := rrecRollbackDetail(t, ctx, s.pool, s.chainID, tc.stream)
		if want := fmt.Sprintf("stream=%s from=%d to=%d", tc.stream, tc.from, tc.to); !strings.Contains(detail, want) {
			t.Fatalf("%s rollback detail %q misses %q (own-start floor)", tc.stream, detail, want)
		}
	}
	// Replay starts at the floors — never below (no backfill demanded).
	for _, tc := range []struct {
		stream string
		floor  int64
	}{
		{"block", int64(s.hA + 1)}, {"log", int64(l1)}, {"deposit", int64(l2)},
	} {
		if from, _ := rrecReplayFirstRange(t, ctx, s.pool, s.chainID, tc.stream); from != tc.floor {
			t.Fatalf("%s first replay range starts at %d, want own-start floor %d", tc.stream, from, tc.floor)
		}
	}
	rrecAssertFrontiers(t, row, s.hTip)
	if n := rrecEventCount(t, ctx, s.pool, s.chainID, "replay_progress"); n != 3 {
		t.Fatalf("replay_progress events = %d, want 3 (one per stream)", n)
	}
	if n := rrecEventCount(t, ctx, s.pool, s.chainID, "reconcile_signaled"); n != 0 {
		t.Fatalf("reconcile_signaled events = %d, want 0 (floors are in-bound, never holds)", n)
	}
	// Status: P orphaned with this round's evidence; below-start history
	// still owns no rows (K/C never backfilled) and the empty new chain
	// minted nothing anywhere in the sweep.
	pObs := rrecReadObs(t, ctx, s.pool, s.chainID, s.pBH, strings.ToLower(s.pTx.Hex()))
	if pObs.status != "orphaned" || pObs.orphanID != row.RecoveryID || pObs.orphanedAtNull {
		t.Fatalf("P = (%s orphan=%s null=%v), want orphaned under this recovery", pObs.status, pObs.orphanID, pObs.orphanedAtNull)
	}
	for _, tc := range []struct {
		name string
		tx   common.Hash
	}{
		{"K", s.kTx}, {"C", s.cTx},
	} {
		if n := rrecCountTxObs(t, ctx, s.pool, s.chainID, strings.ToLower(tc.tx.Hex())); n != 0 {
			t.Fatalf("%s observation rows = %d, want 0 (no backfill below starts)", tc.name, n)
		}
	}
	rrecAssertNoNewChainFootprint(t, ctx, s.pool, s, l1, s.hTip)
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM erc20_transfer_logs WHERE chain_id=$1 AND block_number>$2 AND block_number<$3`,
		s.chainID, int64(s.hA), int64(l1)).Scan(&belowLogs); err != nil || belowLogs != 0 {
		t.Fatalf("logs below the log start after replay = %d (err=%v), want 0", belowLogs, err)
	}
	// Identity: swept heights carry the fork-B canonical rows (two rows each:
	// old retained + new); the ancestor-side K header stays canonical.
	for h := s.hA + 1; h <= s.hTip; h++ {
		var ch string
		var total int
		if err := s.pool.QueryRow(ctx, `SELECT hash FROM chain_blocks WHERE chain_id=$1 AND number=$2 AND canonical`,
			s.chainID, int64(h)).Scan(&ch); err != nil || ch != s.bHashes[h] {
			t.Fatalf("canonical[%d] = %s (err=%v), want fork-B %s", h, ch, err, s.bHashes[h])
		}
		if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM chain_blocks WHERE chain_id=$1 AND number=$2`,
			s.chainID, int64(h)).Scan(&total); err != nil || total != 2 {
			t.Fatalf("total rows at %d = %d (err=%v), want 2", h, total, err)
		}
	}
	var kCanon string
	if err := s.pool.QueryRow(ctx, `SELECT hash FROM chain_blocks WHERE chain_id=$1 AND number=$2 AND canonical`,
		s.chainID, int64(s.hK)).Scan(&kCanon); err != nil || kCanon != s.kBH {
		t.Fatalf("ancestor-side K header = %s (err=%v), want %s canonical", kCanon, err, s.kBH)
	}
	// Checkpoints post-replay keep their own starts and reach the new tip.
	cp = rrecReadCheckpoints(t, ctx, s.pool, s.chainID)
	if cp.blockH != int64(s.hTip) || cp.blockHash != s.bHashes[s.hTip] {
		t.Fatalf("002 checkpoint = (%d %s), want fork-B tip (%d %s)", cp.blockH, cp.blockHash, s.hTip, s.bHashes[s.hTip])
	}
	if cp.logStart != int64(l1) || cp.logNext != int64(s.hTip+1) || cp.depStart != int64(l2) || cp.depNext != int64(s.hTip+1) {
		t.Fatalf("stream checkpoints = (log %d/%d deposit %d/%d), want (%d/%d %d/%d)",
			cp.logStart, cp.logNext, cp.depStart, cp.depNext, l1, s.hTip+1, l2, s.hTip+1)
	}
	rrecAssertCheckpointChecks(t, ctx, s.pool, s.chainID)
	rrecAssertSeededPausesOnly(t, ctx, s.pool, s)

	id := rrecRelease(t, ctx, s.pool, ex, s.chainID, row, cap)
	var terminal string
	if err := s.pool.QueryRow(ctx, `SELECT detail FROM reorg_recovery_events WHERE chain_id=$1 AND event='auto_completed'`,
		s.chainID).Scan(&terminal); err != nil {
		t.Fatalf("read terminal event: %v", err)
	}
	for _, want := range []string{
		fmt.Sprintf("bound_old=%d:%s", s.hTip, s.aHashes[s.hTip]),
		"policy=1",
		fmt.Sprintf("ancestor=%d:%s", s.hA, s.aHashes[s.hA]),
		fmt.Sprintf("swept=%d-%d", s.hA+1, s.hTip),
		"orphaned=1", "revived=0",
		fmt.Sprintf("replayed=block:%d,log:%d,deposit:%d", s.hTip, s.hTip, s.hTip),
		"surviving_pauses=indexer_pause,deposit_pause",
		"version=1",
	} {
		if !strings.Contains(terminal, want) {
			t.Fatalf("terminal detail missing %q: %q", want, terminal)
		}
	}
	if n := rrecTransitionCount(t, ctx, s.pool, s.chainID, "pending", "orphaned", id); n != 1 {
		t.Fatalf("pending->orphaned transitions = %d, want 1 (P)", n)
	}
	if n := rrecTransitionCount(t, ctx, s.pool, s.chainID, "confirmed", "orphaned", id); n != 0 {
		t.Fatalf("confirmed->orphaned transitions = %d, want 0 (nothing confirmed pre-fork)", n)
	}
}

// TestReorgRecoveryUS3EmptyInterval is T024 scene (c) (FR-11, V5-empty): the
// fork carries a single transfer at the sweep base and pure empties after.
// Replay advances every stream across the empty tail with zero new log or
// observation rows there and no gap markers anywhere; SC-03 green.
func TestReorgRecoveryUS3EmptyInterval(t *testing.T) {
	s := rrecSetup(t, "rrec-us3-empty")
	ctx := context.Background()

	rrecMineForkA(t, ctx, s)
	rrecIndexForkA(t, ctx, s)
	kStatus, _, kTipN, kThr, kSeq, kTipHash, kConf := confirmReadBasis(t, ctx, s.pool, s.chainID,
		s.kBH, strings.ToLower(s.kTx.Hex()))
	if kStatus != "confirmed" {
		t.Fatalf("pre-fork K = %s, want confirmed", kStatus)
	}

	rrecForkB(t, ctx, s)
	rrecSeedPauses(t, ctx, s)

	ex := rrecExecutor(t, s.pool, s.lease, s)
	row, cap := rrecDriveToReady(t, ctx, s.pool, ex, s.chainID)
	if row.AncestorNumber == nil || *row.AncestorNumber != int64(s.hA) {
		t.Fatalf("ancestor = %v, want %d", row.AncestorNumber, s.hA)
	}
	// The fork-B tail past the mint height is legitimately empty: no new log
	// or observation rows on any new-chain tail hash.
	rrecAssertNoNewChainFootprint(t, ctx, s.pool, s, s.hA+2, s.hTip)
	// Progress still advanced across the empties on every stream.
	for _, stream := range []string{"block", "log", "deposit"} {
		if from, _ := rrecReplayFirstRange(t, ctx, s.pool, s.chainID, stream); from != int64(s.hA+1) {
			t.Fatalf("%s first replay range starts at %d, want floor %d", stream, from, s.hA+1)
		}
	}
	rrecAssertFrontiers(t, row, s.hTip)
	if n := rrecEventCount(t, ctx, s.pool, s.chainID, "replay_progress"); n != 3 {
		t.Fatalf("replay_progress events = %d, want 3 (empty ranges advance, never stall)", n)
	}
	cp := rrecReadCheckpoints(t, ctx, s.pool, s.chainID)
	if cp.blockH != int64(s.hTip) || cp.blockHash != s.bHashes[s.hTip] {
		t.Fatalf("002 checkpoint = (%d %s), want fork-B tip (%d %s)", cp.blockH, cp.blockHash, s.hTip, s.bHashes[s.hTip])
	}
	if cp.logNext != int64(s.hTip+1) || cp.depNext != int64(s.hTip+1) {
		t.Fatalf("stream nexts = (log %d deposit %d), want both %d (advanced over empties)", cp.logNext, cp.depNext, s.hTip+1)
	}
	// Status: only the sweep-base mint exists; P+C orphaned, K intact, and no
	// row is effective in the empty tail.
	var tailEff int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM deposit_observations
WHERE chain_id=$1 AND block_number>$2 AND block_number<=$3 AND status IN ('pending','confirmed')`,
		s.chainID, int64(s.hA+1), int64(s.hTip)).Scan(&tailEff); err != nil || tailEff != 0 {
		t.Fatalf("effective observations in empty tail = %d (err=%v), want 0", tailEff, err)
	}
	bObs := rrecReadObs(t, ctx, s.pool, s.chainID, s.bBH, strings.ToLower(s.bTx.Hex()))
	if bObs.status != "pending" || bObs.amount != "11" || bObs.version != 1 {
		t.Fatalf("sweep-base mint = %+v, want (pending amount 11 v1)", bObs)
	}
	for _, tc := range []struct {
		name, bh, tx string
	}{
		{"P", s.pBH, strings.ToLower(s.pTx.Hex())},
		{"C", s.cBH, strings.ToLower(s.cTx.Hex())},
	} {
		if o := rrecReadObs(t, ctx, s.pool, s.chainID, tc.bh, tc.tx); o.status != "orphaned" || o.orphanID != row.RecoveryID {
			t.Fatalf("%s = (%s orphan=%s), want orphaned under this recovery", tc.name, o.status, o.orphanID)
		}
	}
	kObs := rrecReadObs(t, ctx, s.pool, s.chainID, s.kBH, strings.ToLower(s.kTx.Hex()))
	if kObs.status != "confirmed" || kObs.orphanID != "" {
		t.Fatalf("K = (%s orphan=%s), want (confirmed, untouched)", kObs.status, kObs.orphanID)
	}
	if kObs.tipNumber != kTipN || kObs.tipHash != kTipHash || kObs.threshold != kThr || kObs.policySeq != kSeq || kObs.confirmations != kConf {
		t.Fatalf("K basis moved over empty replay: %+v", kObs)
	}
	rrecAssertCheckpointChecks(t, ctx, s.pool, s.chainID)
	rrecAssertSeededPausesOnly(t, ctx, s.pool, s)

	id := rrecRelease(t, ctx, s.pool, ex, s.chainID, row, cap)
	var terminal string
	if err := s.pool.QueryRow(ctx, `SELECT detail FROM reorg_recovery_events WHERE chain_id=$1 AND event='auto_completed'`,
		s.chainID).Scan(&terminal); err != nil {
		t.Fatalf("read terminal event: %v", err)
	}
	for _, want := range []string{"orphaned=2", "revived=0", "surviving_pauses=indexer_pause,deposit_pause"} {
		if !strings.Contains(terminal, want) {
			t.Fatalf("terminal detail missing %q: %q", want, terminal)
		}
	}
	if total := rrecTransitionCount(t, ctx, s.pool, s.chainID, "pending", "orphaned", id) +
		rrecTransitionCount(t, ctx, s.pool, s.chainID, "confirmed", "orphaned", id); total != 2 {
		t.Fatalf("orphan transitions = %d, want 2 (P+C)", total)
	}
}

// --- T026: Anvil stale-worker refusal (Batch D, FR-14/20) --------------------

// rrecT26Snap is the zero-write fingerprint for the stale-worker test:
// business tables, checkpoints, and audit counts must be identical before and
// after every refused submit.
type rrecT26Snap struct {
	blocks, canon, noncanon   int
	kStatus, cStatus, pStatus string
	kBasis, cBasis            string
	cpH                       int64
	cpHash                    string
	logNext, depNext          int64
	events, trans             int
}

func rrecT26Snapshot(t *testing.T, ctx context.Context, s *rrecScene) rrecT26Snap {
	t.Helper()
	var snap rrecT26Snap
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM chain_blocks WHERE chain_id=$1`,
		s.chainID).Scan(&snap.blocks); err != nil {
		t.Fatalf("count chain_blocks: %v", err)
	}
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM chain_blocks WHERE chain_id=$1 AND canonical`,
		s.chainID).Scan(&snap.canon); err != nil {
		t.Fatalf("count canonical: %v", err)
	}
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM chain_blocks WHERE chain_id=$1 AND NOT canonical`,
		s.chainID).Scan(&snap.noncanon); err != nil {
		t.Fatalf("count non-canonical: %v", err)
	}
	snap.kStatus, snap.cStatus, snap.pStatus = rrecReadObs(t, ctx, s.pool, s.chainID,
		s.kBH, strings.ToLower(s.kTx.Hex())).status,
		rrecReadObs(t, ctx, s.pool, s.chainID, s.cBH, strings.ToLower(s.cTx.Hex())).status,
		rrecReadObs(t, ctx, s.pool, s.chainID, s.pBH, strings.ToLower(s.pTx.Hex())).status
	kSt, kNull, kTipN, kThr, kSeq, kTipHash, kConf := confirmReadBasis(t, ctx, s.pool, s.chainID,
		s.kBH, strings.ToLower(s.kTx.Hex()))
	cSt, cNull, cTipN, cThr, cSeq, cTipHash, cConf := confirmReadBasis(t, ctx, s.pool, s.chainID,
		s.cBH, strings.ToLower(s.cTx.Hex()))
	snap.kBasis = fmt.Sprintf("%s null=%v tip=%d:%s N=%d seq=%d conf=%s", kSt, kNull, kTipN, kTipHash, kThr, kSeq, kConf)
	snap.cBasis = fmt.Sprintf("%s null=%v tip=%d:%s N=%d seq=%d conf=%s", cSt, cNull, cTipN, cTipHash, cThr, cSeq, cConf)
	cp := rrecReadCheckpoints(t, ctx, s.pool, s.chainID)
	snap.cpH, snap.cpHash, snap.logNext, snap.depNext = cp.blockH, cp.blockHash, cp.logNext, cp.depNext
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM reorg_recovery_events WHERE chain_id=$1`,
		s.chainID).Scan(&snap.events); err != nil {
		t.Fatalf("count recovery events: %v", err)
	}
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM deposit_observation_transitions WHERE chain_id=$1`,
		s.chainID).Scan(&snap.trans); err != nil {
		t.Fatalf("count transitions: %v", err)
	}
	return snap
}

func rrecT26AssertSame(t *testing.T, want, got rrecT26Snap, where string) {
	t.Helper()
	if want != got {
		t.Fatalf("state moved across refused %s:\n want %+v\n got  %+v", where, want, got)
	}
}

// TestT026AnvilStaleWorkerRefusal is T026-Anvil (FR-14/20, V6-isolation): on a
// live Anvil fork, ordinary-path captures taken BEFORE the recovery version
// change refuse 100% after it — old scan (002 rescan), old identify (004 unit
// commit), old confirm (005 unit commit) — with zero business writes and zero
// progress. The version-alone proof runs pauseless (direct establish, so no
// pause row can shadow the gate); the live-fork proof then lands fork B with
// the seeded pauses and shows the same delayed submits still refuse
// (pause-first fencing) while the legal pre-pause commits stand.
func TestT026AnvilStaleWorkerRefusal(t *testing.T) {
	s := rrecSetup(t, "rrec-t026-stale")
	ctx := context.Background()

	rrecMineForkA(t, ctx, s)
	rrecIndexForkA(t, ctx, s)

	// Delayed-worker inputs: everything captured BEFORE the version change.
	cap0 := testRecoveryCap(t, ctx, s.pool, s.chainID)
	if cap0.Seq != 0 {
		t.Fatalf("pre-recovery capture seq = %d, want 0", cap0.Seq)
	}
	sc002, err := NewScanner(s.pool, s.client, s.lease, Config{
		StartHeight: 0, RPCTimeout: 2 * time.Second, PollInterval: 25 * time.Millisecond,
		RetryInitial: 25 * time.Millisecond, RetryMax: 250 * time.Millisecond,
	}, logscanLogger())
	if err != nil {
		t.Fatalf("NewScanner(): %v", err)
	}
	rescan := blockWrite{number: s.hTip, hash: s.aHashes[s.hTip], parent: s.aHashes[s.hTip-1]}
	sc004, err := NewDepositScanner(s.pool, rrecDepositCfg(s))
	if err != nil {
		t.Fatalf("NewDepositScanner(): %v", err)
	}
	unit0, batch0 := depositITReadUnit(t, ctx, sc004, 0, 0)
	committer, err := NewConfirmationCommitter(s.pool, ConfirmationConfig{ChainID: s.chainID, ThresholdN: rrecThresholdN})
	if err != nil {
		t.Fatalf("NewConfirmationCommitter(): %v", err)
	}
	stalePBasis := ConfirmBasis{
		BlockHash: s.pBH, TxHash: strings.ToLower(s.pTx.Hex()), LogIndex: 0, Height: s.hP,
		TipNumber: s.hTip, TipHash: s.aHashes[s.hTip], PolicySeq: 1, ThresholdN: rrecThresholdN,
	}

	// Legal ordinary-first order: the K/C confirmations + P pending above ARE
	// the pre-pause commits (real 002/003/004/005 loops); establish-after-them
	// must stay allowed, and the baseline below pins the post-version state
	// every refused submit must preserve.
	if _, err := EstablishRecovery(ctx, s.pool, s.lease, EstablishRequest{
		ChainID:      s.chainID,
		OldTipNumber: int64(s.hTip), OldTipHash: s.aHashes[s.hTip],
		NewTipNumber: int64(s.hTip), NewTipHash: s.aHashes[s.hTip],
		DetectedHeight: int64(s.hA + 1), EnvMaxDepthRaw: rrecMaxDepth,
	}); err != nil {
		t.Fatalf("pauseless establish: %v", err)
	}
	est, err := func() (EstablishResult, error) {
		row, err := LoadRecoveryState(ctx, s.pool, s.chainID)
		if err != nil || row == nil {
			return EstablishResult{}, err
		}
		return EstablishResult{RecoveryID: row.RecoveryID, Seq: row.Seq}, nil
	}()
	if err != nil {
		t.Fatalf("read established row: %v", err)
	}
	if est.Seq != 1 {
		t.Fatalf("established seq = %d, want 1", est.Seq)
	}
	base := rrecT26Snapshot(t, ctx, s)
	if base.kStatus != "confirmed" || base.cStatus != "confirmed" || base.pStatus != "pending" {
		t.Fatalf("legal baseline = K:%s C:%s P:%s, want confirmed/confirmed/pending",
			base.kStatus, base.cStatus, base.pStatus)
	}
	if base.noncanon != 0 || base.trans != 0 || base.events != 1 {
		t.Fatalf("legal baseline = (noncanon=%d trans=%d events=%d), want (0 0 1-established)",
			base.noncanon, base.trans, base.events)
	}

	// Delayed old scan: the tip rescan refuses on version alone.
	if err := sc002.commitBlock(ctx, rescan, cap0); !isRecoveryGate(err) {
		t.Fatalf("stale scan rescan post-version = %v, want gate refusal", err)
	}
	rrecT26AssertSame(t, base, rrecT26Snapshot(t, ctx, s), "stale scan")
	// Delayed old identify: a well-formed first-unit submit refuses on version
	// alone (gate runs before progress guards).
	if err := sc004.commitDepositUnit(ctx, s.lease, unit0, batch0, nil, 0, 0, cap0); !isRecoveryGate(err) {
		t.Fatalf("stale identify commit post-version = %v, want gate refusal", err)
	}
	rrecT26AssertSame(t, base, rrecT26Snapshot(t, ctx, s), "stale identify")
	// Delayed old confirm: fully content-matching P basis refuses on version.
	if err := committer.ConfirmDepositUnit(ctx, s.lease, stalePBasis, cap0); !isRecoveryGate(err) {
		t.Fatalf("stale confirm post-version = %v, want gate refusal", err)
	}
	rrecT26AssertSame(t, base, rrecT26Snapshot(t, ctx, s), "stale confirm")
	// Primitive half: straight through the gate, content never consulted.
	func() {
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin gate probe: %v", err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if _, err := recheckRecoveryGate(ctx, tx, s.chainID, cap0); !isRecoveryGate(err) {
			t.Fatalf("gate primitive with pre-round capture = %v, want refusal", err)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatalf("rollback gate probe: %v", err)
		}
	}()
	rrecT26AssertSame(t, base, rrecT26Snapshot(t, ctx, s), "gate probe")

	// The live fork lands: revert + fork B with all-new hashes, seeded pauses,
	// and the executor converges onto the one persistent instance (no second
	// established event, same seq).
	rrecForkB(t, ctx, s)
	rrecSeedPauses(t, ctx, s)
	ex := rrecExecutor(t, s.pool, s.lease, s)
	done, err := ex.tickIdle(ctx)
	if err != nil || !done {
		t.Fatalf("tickIdle() on live fork = (%v, %v), want (true, nil) via convergence", done, err)
	}
	row, _ := rrecRow(t, ctx, s.pool, s.chainID)
	if row.RecoveryID == "" || row.Seq != 1 || row.Phase != reorgPhaseDetected {
		t.Fatalf("post-fork row = (%s %d %s), want (same id 1 detected)",
			row.RecoveryID, row.Seq, row.Phase)
	}
	if n := rrecEventCount(t, ctx, s.pool, s.chainID, "established"); n != 1 {
		t.Fatalf("established events after convergence = %d, want 1 (no second instance)", n)
	}
	var estDetail string
	if err := s.pool.QueryRow(ctx, `SELECT detail FROM reorg_recovery_events WHERE chain_id=$1 AND event='established'`,
		s.chainID).Scan(&estDetail); err != nil {
		t.Fatalf("read established detail: %v", err)
	}
	for _, want := range []string{
		fmt.Sprintf("old_tip=%d:%s", s.hTip, s.aHashes[s.hTip]),
		fmt.Sprintf("new_tip=%d:%s", s.hTip, s.aHashes[s.hTip]),
		"policy=1", "version=1",
	} {
		if !strings.Contains(estDetail, want) {
			t.Fatalf("established detail missing %q: %q", want, estDetail)
		}
	}

	// Same delayed workers under the live fork + pauses: still refused 100%
	// (pause-first fencing now shadows the gate), zero writes each.
	liveBase := rrecT26Snapshot(t, ctx, s)
	if err := sc002.commitBlock(ctx, rescan, cap0); !errors.Is(err, errPaused) {
		t.Fatalf("stale scan on live fork = %v, want the indexer_pause stop", err)
	}
	rrecT26AssertSame(t, liveBase, rrecT26Snapshot(t, ctx, s), "live-fork stale scan")
	if err := sc004.commitDepositUnit(ctx, s.lease, unit0, batch0, nil, 0, 0, cap0); err == nil ||
		!strings.Contains(strings.ToLower(err.Error()), "pause") {
		t.Fatalf("stale identify on live fork = %v, want a pause refusal", err)
	}
	rrecT26AssertSame(t, liveBase, rrecT26Snapshot(t, ctx, s), "live-fork stale identify")
	if err := committer.ConfirmDepositUnit(ctx, s.lease, stalePBasis, cap0); err == nil ||
		!strings.Contains(err.Error(), "deposit_pause") {
		t.Fatalf("stale confirm on live fork = %v, want the deposit_pause stop", err)
	}
	rrecT26AssertSame(t, liveBase, rrecT26Snapshot(t, ctx, s), "live-fork stale confirm")

	// Legal pre-pause commits stand: K/C confirmed on the ORIGINAL basis, P
	// pending; A-chain canonical everywhere (no invalidate ran); checkpoints
	// frozen at the tip (zero progress); exactly the established event.
	final := rrecT26Snapshot(t, ctx, s)
	if final.kStatus != "confirmed" || final.cStatus != "confirmed" || final.pStatus != "pending" {
		t.Fatalf("post-refusal statuses = K:%s C:%s P:%s, want confirmed/confirmed/pending",
			final.kStatus, final.cStatus, final.pStatus)
	}
	if final.kBasis != base.kBasis || final.cBasis != base.cBasis {
		t.Fatalf("legal basis moved:\n K %+v -> %+v\n C %+v -> %+v", base.kBasis, final.kBasis, base.cBasis, final.cBasis)
	}
	if final.noncanon != 0 || final.blocks != base.blocks || final.canon != base.canon {
		t.Fatalf("chain blocks moved: base (total=%d canon=%d noncanon=%d) vs final (total=%d canon=%d noncanon=%d)",
			base.blocks, base.canon, base.noncanon, final.blocks, final.canon, final.noncanon)
	}
	if final.cpH != int64(s.hTip) || final.cpHash != s.aHashes[s.hTip] ||
		final.logNext != int64(s.hTip+1) || final.depNext != int64(s.hTip+1) {
		t.Fatalf("checkpoints moved: block=(%d %s) logNext=%d depNext=%d, want tip (%d %s) +1/+1",
			final.cpH, final.cpHash, final.logNext, final.depNext, s.hTip, s.aHashes[s.hTip])
	}
	if row.BlockFrontier != nil || row.LogFrontier != nil || row.DepositFrontier != nil {
		t.Fatalf("frontiers moved: (%v %v %v), want all nil (zero progress)",
			row.BlockFrontier, row.LogFrontier, row.DepositFrontier)
	}
	if final.events != 1 || final.trans != 0 {
		t.Fatalf("audit = (events=%d trans=%d), want (1 established, 0 conversions)", final.events, final.trans)
	}
	rrecAssertSeededPausesOnly(t, ctx, s.pool, s)
}

// --- T028: crash drill + re-fork E2E (Batch D, FR-12/13/16/19) ---------------

// rrecT28Frontiers reads the three stream frontiers (-1 for nil) for the
// monotonic-frontier proof.
func rrecT28Frontiers(t *testing.T, row *RecoveryRow) [3]int64 {
	t.Helper()
	out := [3]int64{-1, -1, -1}
	for i, f := range []*int64{row.BlockFrontier, row.LogFrontier, row.DepositFrontier} {
		if f != nil {
			out[i] = *f
		}
	}
	return out
}

func rrecT28AssertFrontiers(t *testing.T, prev, got [3]int64, tip uint64, where string) {
	t.Helper()
	for i, name := range []string{"block", "log", "deposit"} {
		if got[i] < prev[i] {
			t.Fatalf("%s frontier regressed at %s: %v -> %v", name, where, prev, got)
		}
		if got[i] > int64(tip) {
			t.Fatalf("%s frontier %d past bound tip %d at %s", name, got[i], tip, where)
		}
	}
}

// TestT028CrashDrillAndRefork is T028 (FR-12/13/16/19, V7-crash + V8) in two
// scenes on the shared harness shape: (1) a kill -9 drill between EVERY phase
// pair with commit-response-loss triage in all three persisted outcomes and an
// exactly-once terminal release over monotonic frontiers; (2) a mid-replay
// second fork that never falsely releases, re-validates the ancestor, and
// holds completion against the bound tip + swept range, never the live head.
func TestT028CrashDrillAndRefork(t *testing.T) {
	ctx := context.Background()

	// Scene 1: the crash drill on fork B (narrow batch 2 so replaying is
	// observable across three ticks over the six-height sweep).
	s := rrecSetup(t, "rrec-t028-drill")
	rrecMineForkA(t, ctx, s)
	rrecIndexForkA(t, ctx, s)
	rrecForkB(t, ctx, s)
	rrecSeedPauses(t, ctx, s)

	// rrecT28Resume drops everything (the kill -9) and rebuilds the executor
	// on the same DB + same Anvil endpoint: durable state is the only
	// continuity. Lease credentials carry over; only the pool is replaced.
	resume := func(t *testing.T) (*pgxpool.Pool, *RecoveryExecutor) {
		t.Helper()
		s.pool.Close()
		pool := openIndexerPool(t, s.dsn)
		t.Cleanup(pool.Close)
		s.pool = pool
		return pool, rrecExecutorBatch(t, pool, s.lease, s, 2)
	}

	pool, ex := resume(t)
	done, err := ex.tickIdle(ctx)
	if err != nil || !done {
		t.Fatalf("tickIdle() = (%v, %v), want (true, nil)", done, err)
	}
	firstID, firstSeq := rrecRowID(t, ctx, pool, s.chainID)
	phases := []string{reorgPhaseDetected}

	// Crash 1: between establish and ancestor confirm (unknown outcome, but
	// nothing was in flight — resume must land on detected with no redo).
	pool, ex = resume(t)
	row, cap := rrecRow(t, ctx, pool, s.chainID)
	if row.RecoveryID != firstID || row.Seq != firstSeq || row.Phase != reorgPhaseDetected {
		t.Fatalf("resume-1 row = (%s %d %s), want (%s %d detected)",
			row.RecoveryID, row.Seq, row.Phase, firstID, firstSeq)
	}
	if n := rrecEventCount(t, ctx, pool, s.chainID, "established"); n != 1 {
		t.Fatalf("established events after crash-1 = %d, want 1 (no redo)", n)
	}
	if f := rrecT28Frontiers(t, row); f != [3]int64{-1, -1, -1} {
		t.Fatalf("frontiers after crash-1 = %v, want all nil", f)
	}

	// Triage (a) uncommitted: a refused confirm fails clean; re-read decides
	// still-detected with zero ancestor events, and the retry stays safe.
	wrongHash := "0x" + strings.Repeat("00", 32)
	if err := ConfirmRecoveryAncestor(ctx, pool, s.lease, s.chainID, cap,
		int64(s.hA), wrongHash, "t028 triage uncommitted"); !isRecoveryGate(err) {
		t.Fatalf("wrong-ancestor confirm = %v, want gate refusal (uncommitted)", err)
	}
	if triRow, triErr := LoadRecoveryState(ctx, pool, s.chainID); triErr != nil || triRow == nil ||
		triRow.Phase != reorgPhaseDetected {
		t.Fatalf("triage re-read = (%+v, %v), want still detected", triRow, triErr)
	}
	if n := rrecEventCount(t, ctx, pool, s.chainID, "ancestor_confirmed"); n != 0 {
		t.Fatalf("ancestor_confirmed events after refused confirm = %d, want 0", n)
	}

	// Triage (b) committed: the COMMIT lands server-side while the worker
	// observes the lost response; re-read decides ancestor_confirmed, and the
	// immediate retry refuses instead of duplicating the event.
	dropPool, dropCtl := logscanOpenCommitDropPool(t, s.dsn)
	dropCtl.arm.Store(true)
	lostErr := ConfirmRecoveryAncestor(ctx, dropPool, s.lease, s.chainID, cap,
		int64(s.hA), s.aHashes[s.hA], "t028 triage lost-response evidence")
	if lostErr == nil {
		t.Fatal("ConfirmRecoveryAncestor through the commit-drop pool = nil, want the lost-response error")
	}
	triRow, triErr := LoadRecoveryState(ctx, pool, s.chainID)
	if triErr != nil || triRow == nil {
		t.Fatalf("triage re-read = (%+v, %v), want the row", triRow, triErr)
	}
	if triRow.Phase != reorgPhaseAncestorConfirmed || triRow.AncestorNumber == nil ||
		*triRow.AncestorNumber != int64(s.hA) {
		t.Fatalf("triage outcome = phase %s ancestor %v, want (ancestor_confirmed %d): re-read decides, not memory",
			triRow.Phase, triRow.AncestorNumber, s.hA)
	}
	if n := rrecEventCount(t, ctx, pool, s.chainID, "ancestor_confirmed"); n != 1 {
		t.Fatalf("ancestor_confirmed events = %d, want 1 (committed once)", n)
	}
	row, cap = rrecRow(t, ctx, pool, s.chainID)
	if err := ConfirmRecoveryAncestor(ctx, pool, s.lease, s.chainID, cap,
		int64(s.hA), s.aHashes[s.hA], "t028 redo"); !isRecoveryGate(err) {
		t.Fatalf("re-confirm after committed confirm = %v, want gate refusal (no redo)", err)
	}
	if n := rrecEventCount(t, ctx, pool, s.chainID, "ancestor_confirmed"); n != 1 {
		t.Fatalf("ancestor_confirmed events after redo attempt = %d, want still 1", n)
	}
	phases = append(phases, reorgPhaseAncestorConfirmed)

	// Crash 2: between ancestor_confirmed and invalidated (ancestor pinned,
	// frontiers still nil — resume keeps both with no skip).
	pool, ex = resume(t)
	row, cap = rrecRow(t, ctx, pool, s.chainID)
	if row.RecoveryID != firstID || row.Phase != reorgPhaseAncestorConfirmed ||
		row.AncestorNumber == nil || *row.AncestorNumber != int64(s.hA) ||
		row.AncestorHash == nil || *row.AncestorHash != s.aHashes[s.hA] {
		t.Fatalf("resume-2 row = %+v, want ancestor_confirmed @(%d %s)", row, s.hA, s.aHashes[s.hA])
	}

	// Triage (c) unknown-at-crash: the invalidate tick runs on the commit-drop
	// pool (first COMMIT lands, reply lost — outcome truly unknown), then the
	// process dies before reading anything; restart re-reads: blocks flipped
	// + phase invalidated + exactly one event means committed, so the drill
	// continues with the REMAINING steps only (never re-runs the committed
	// blocks step).
	exDrop := rrecExecutorBatch(t, dropPool, s.lease, s, 2)
	dropCtl.arm.Store(true)
	if err := exDrop.tickInvalidate(ctx, row, cap); err == nil {
		t.Fatal("tickInvalidate through the commit-drop pool = nil, want the lost-response error")
	}
	pool, ex = resume(t)
	row, cap = rrecRow(t, ctx, pool, s.chainID)
	if row.RecoveryID != firstID || row.Phase != reorgPhaseInvalidated {
		t.Fatalf("post-crash row = (%s %s), want (same id invalidated): re-read triages committed",
			row.RecoveryID, row.Phase)
	}
	var flipped int64
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM chain_blocks WHERE chain_id=$1 AND number>$2 AND NOT canonical`,
		s.chainID, int64(s.hA)).Scan(&flipped); err != nil || flipped != int64(s.hTip-s.hA) {
		t.Fatalf("flipped = %d (err=%v), want %d (blocks step committed despite lost response)",
			flipped, err, s.hTip-s.hA)
	}
	if n := rrecEventCount(t, ctx, pool, s.chainID, "blocks_invalidated"); n != 1 {
		t.Fatalf("blocks_invalidated events = %d, want 1 (committed once, never redone)", n)
	}
	// Continue forward without redo: observations + the three checkpoint
	// rollbacks only.
	if _, err := InvalidateRecoveryObservations(ctx, pool, s.lease, s.chainID, cap); err != nil {
		t.Fatalf("continued invalidate observations: %v", err)
	}
	for _, stream := range []RecoveryStream{RecoveryStreamBlock, RecoveryStreamLog, RecoveryStreamDeposit} {
		if _, _, err := RollbackRecoveryCheckpoint(ctx, pool, s.lease, s.chainID, cap, stream); err != nil {
			t.Fatalf("continued rollback %s: %v", stream, err)
		}
	}
	if n := rrecEventCount(t, ctx, pool, s.chainID, "blocks_invalidated"); n != 1 {
		t.Fatalf("blocks_invalidated events after continuation = %d, want still 1 (no redo)", n)
	}
	if n := rrecEventCount(t, ctx, pool, s.chainID, "observations_invalidated"); n != 1 {
		t.Fatalf("observations_invalidated events = %d, want 1", n)
	}
	if n := rrecEventCount(t, ctx, pool, s.chainID, "checkpoints_rolled_back"); n != 3 {
		t.Fatalf("checkpoints_rolled_back events = %d, want 3", n)
	}
	phases = append(phases, reorgPhaseInvalidated)

	// Crash 3: between invalidated and replaying (sweep durable, frontiers
	// nil — resume must not skip the replay).
	pool, ex = resume(t)
	row, cap = rrecRow(t, ctx, pool, s.chainID)
	if row.RecoveryID != firstID || row.Phase != reorgPhaseInvalidated {
		t.Fatalf("resume-3 row = (%s %s), want (same id invalidated)", row.RecoveryID, row.Phase)
	}
	if f := rrecT28Frontiers(t, row); f != [3]int64{-1, -1, -1} {
		t.Fatalf("frontiers after crash-3 = %v, want all nil", f)
	}

	// Replay tick 1 (batch 2 over the six-height sweep): phase replaying,
	// frontiers at hA+2 on every stream.
	if err := ex.tickReplay(ctx, row, cap); err != nil {
		t.Fatalf("tickReplay #1: %v", err)
	}
	row, cap = rrecRow(t, ctx, pool, s.chainID)
	if row.Phase != reorgPhaseReplaying {
		t.Fatalf("phase after tick #1 = %q, want replaying", row.Phase)
	}
	prev := [3]int64{-1, -1, -1}
	got := rrecT28Frontiers(t, row)
	rrecT28AssertFrontiers(t, prev, got, s.hTip, "tick #1")
	for _, f := range got {
		if f != int64(s.hA+2) {
			t.Fatalf("frontiers after tick #1 = %v, want all %d (one batch of 2)", got, s.hA+2)
		}
	}
	prev = got
	progAfterTick1 := rrecEventCount(t, ctx, pool, s.chainID, "replay_progress")
	if progAfterTick1 != 3 {
		t.Fatalf("replay_progress events after tick #1 = %d, want 3 (one per stream)", progAfterTick1)
	}
	phases = append(phases, reorgPhaseReplaying)

	// Crash 4: mid-replay (partial frontiers — resume keeps them with no
	// replayed range redone: event count frozen, frontiers identical).
	pool, ex = resume(t)
	row, cap = rrecRow(t, ctx, pool, s.chainID)
	if row.RecoveryID != firstID || row.Phase != reorgPhaseReplaying {
		t.Fatalf("resume-4 row = (%s %s), want (same id replaying)", row.RecoveryID, row.Phase)
	}
	rrecT28AssertFrontiers(t, prev, rrecT28Frontiers(t, row), s.hTip, "resume-4")
	if n := rrecEventCount(t, ctx, pool, s.chainID, "replay_progress"); n != progAfterTick1 {
		t.Fatalf("replay_progress events after crash-4 = %d, want %d (no redone range)", n, progAfterTick1)
	}

	// Replay to complete_pending: frontiers monotonic every tick, never past
	// the bound tip.
	for i := 0; i < 10; i++ {
		row, cap = rrecRow(t, ctx, pool, s.chainID)
		if row.Phase == reorgPhaseCompletePending {
			break
		}
		if row.Phase != reorgPhaseInvalidated && row.Phase != reorgPhaseReplaying {
			t.Fatalf("unexpected phase %q mid-replay (skipped or regressed)", row.Phase)
		}
		if err := ex.tickReplay(ctx, row, cap); err != nil {
			t.Fatalf("tickReplay #%d: %v", i+2, err)
		}
		row, cap = rrecRow(t, ctx, pool, s.chainID)
		got = rrecT28Frontiers(t, row)
		rrecT28AssertFrontiers(t, prev, got, s.hTip, fmt.Sprintf("replay tick #%d", i+2))
		prev = got
	}
	row, cap = rrecRow(t, ctx, pool, s.chainID)
	if row.Phase != reorgPhaseCompletePending {
		t.Fatalf("phase = %q after replay, want complete_pending", row.Phase)
	}
	rrecT28AssertFrontiers(t, [3]int64{int64(s.hTip), int64(s.hTip), int64(s.hTip)},
		rrecT28Frontiers(t, row), s.hTip, "complete_pending")
	phases = append(phases, reorgPhaseCompletePending)

	// Crash 5: between complete_pending and release (full frontiers durable —
	// resume must release exactly once, never twice).
	pool, ex = resume(t)
	row, cap = rrecRow(t, ctx, pool, s.chainID)
	if row.RecoveryID != firstID || row.Phase != reorgPhaseCompletePending {
		t.Fatalf("resume-5 row = (%s %s), want (same id complete_pending)", row.RecoveryID, row.Phase)
	}
	rrecT28AssertFrontiers(t, [3]int64{int64(s.hTip), int64(s.hTip), int64(s.hTip)},
		rrecT28Frontiers(t, row), s.hTip, "resume-5")
	if err := ex.tickComplete(ctx, row, cap); err != nil {
		t.Fatalf("post-crash tickComplete(): %v", err)
	}
	if prow, err := LoadRecoveryState(ctx, pool, s.chainID); err != nil || prow != nil {
		t.Fatalf("recovery row after release = %+v (err=%v), want gone exactly once", prow, err)
	}
	if n := rrecEventCount(t, ctx, pool, s.chainID, "auto_completed"); n != 1 {
		t.Fatalf("auto_completed events = %d, want exactly 1 (exactly-once terminal release)", n)
	}
	// A second release with the spent capture refuses on the gate (no row to
	// release); the terminal count stays exactly one.
	if err := CompleteRecoveryVerify(ctx, pool, s.lease, s.chainID, cap); !isRecoveryGate(err) {
		t.Fatalf("second release = %v, want gate refusal", err)
	}
	if n := rrecEventCount(t, ctx, pool, s.chainID, "auto_completed"); n != 1 {
		t.Fatalf("auto_completed events after second attempt = %d, want still 1", n)
	}

	// Phase ledger: monotonic, no redo, no skip, one terminal release.
	wantPhases := []string{
		reorgPhaseDetected, reorgPhaseAncestorConfirmed, reorgPhaseInvalidated,
		reorgPhaseReplaying, reorgPhaseCompletePending,
	}
	if fmt.Sprintf("%v", phases) != fmt.Sprintf("%v", wantPhases) {
		t.Fatalf("phase ledger = %v, want %v", phases, wantPhases)
	}
	for _, ev := range []string{"established", "ancestor_confirmed", "blocks_invalidated", "observations_invalidated", "auto_completed"} {
		if n := rrecEventCount(t, ctx, pool, s.chainID, ev); n != 1 {
			t.Fatalf("%s events = %d, want 1 (no redone committed phase)", ev, n)
		}
	}
	if n := rrecEventCount(t, ctx, pool, s.chainID, "checkpoints_rolled_back"); n != 3 {
		t.Fatalf("checkpoints_rolled_back events = %d, want 3", n)
	}
	if n := rrecEventCount(t, ctx, pool, s.chainID, "replay_progress"); n != 9 {
		t.Fatalf("replay_progress events = %d, want 9 (3 ticks x 3 streams)", n)
	}
	dropPool.Close()
}

// --- T029: depth boundary + reconcile evidence E2E (Batch D, FR-04/17) ------

// rrecT29ExtendTip mines empties so the fork depth (tip - ancestor) equals
// wantDepth exactly, extending the fork-A hash record over the new heights.
func rrecT29ExtendTip(t *testing.T, ctx context.Context, s *rrecScene, wantDepth uint64) {
	t.Helper()
	have := s.hTip - s.hA
	if wantDepth < have {
		t.Fatalf("want depth %d below harness depth %d", wantDepth, have)
	}
	if wantDepth > have {
		s.node.mine(t, wantDepth-have)
		s.hTip = s.node.blockNumber(t)
	}
	if s.hTip-s.hA != wantDepth {
		t.Fatalf("fork depth = %d, want %d", s.hTip-s.hA, wantDepth)
	}
	for h := s.hA; h <= s.hTip; h++ {
		if _, ok := s.aHashes[h]; !ok {
			s.aHashes[h] = rrecHeaderHash(t, ctx, s, h)
		}
	}
}

// rrecT29IndexDeep mirrors rrecIndexForkA over a deep tip where P is already
// confirmed (the standard helper waits for P pending, which never holds
// here): K+C+P confirmed via the REAL 002/003/004/005 loops.
func rrecT29IndexDeep(t *testing.T, ctx context.Context, s *rrecScene) {
	t.Helper()
	sc002, err := NewScanner(s.pool, s.client, s.lease, Config{
		StartHeight: 0, RPCTimeout: 2 * time.Second, PollInterval: 25 * time.Millisecond,
		RetryInitial: 25 * time.Millisecond, RetryMax: 250 * time.Millisecond,
	}, logscanLogger())
	if err != nil {
		t.Fatalf("NewScanner(): %v", err)
	}
	runScanTo(t, sc002, s.hTip, 60*time.Second)

	ls := logscanNewScanner(t, s.pool, s.client, s.client, s.lease, LogConfig{
		StartBlock: 0, Contracts: s.tokens, ConfigHash: s.logHash, BatchBlocks: 2,
	})
	logscanServeTo(t, ctx, s.pool, s.chainID, ls, uint64(int64(s.hTip)+1), 60*time.Second)

	sc004, err := NewDepositScanner(s.pool, rrecDepositCfg(s))
	if err != nil {
		t.Fatalf("NewDepositScanner(): %v", err)
	}
	stop004 := depositRunLoop(t, ctx, sc004, s.lease)
	waitUntil(t, time.Now().Add(60*time.Second), "deposit checkpoint reaches tip+1", func() bool {
		_, _, next, ok := depositCheckpointState(t, ctx, s.pool, s.chainID)
		return ok && next >= s.hTip+1
	})
	time.Sleep(100 * time.Millisecond)
	stop004()

	m := metrics.New(func() bool { return true })
	confirmCfg := ConfirmationConfig{
		ChainID:      s.chainID,
		ThresholdN:   rrecThresholdN,
		PollInterval: 25 * time.Millisecond,
		RetryInitial: 25 * time.Millisecond,
		RetryMax:     250 * time.Millisecond,
	}
	committer005, err := NewConfirmationCommitter(s.pool, confirmCfg)
	if err != nil {
		t.Fatalf("NewConfirmationCommitter(): %v", err)
	}
	sc005, err := NewConfirmationScanner(s.pool, confirmCfg, committer005, m)
	if err != nil {
		t.Fatalf("NewConfirmationScanner(): %v", err)
	}
	stop005 := confirm13RunLoop(t, ctx, sc005, s.lease)
	waitUntil(t, time.Now().Add(60*time.Second), "K+C+P confirmed", func() bool {
		var n int
		_ = s.pool.QueryRow(ctx, `SELECT count(*) FROM deposit_observations WHERE chain_id=$1 AND status='confirmed'`,
			s.chainID).Scan(&n)
		return n == 3
	})
	time.Sleep(200 * time.Millisecond)
	stop005()

	var cpH int64
	var cpHash string
	if err := s.pool.QueryRow(ctx, `SELECT height, block_hash FROM indexer_checkpoint WHERE chain_id=$1`,
		s.chainID).Scan(&cpH, &cpHash); err != nil {
		t.Fatalf("read indexer_checkpoint: %v", err)
	}
	if cpH != int64(s.hTip) || cpHash != s.aHashes[s.hTip] {
		t.Fatalf("002 checkpoint = (%d %s), want (%d %s)", cpH, cpHash, s.hTip, s.aHashes[s.hTip])
	}
	tipHash := s.aHashes[s.hTip]
	for _, tc := range []struct {
		h  uint64
		tx common.Hash
	}{
		{s.hK, s.kTx}, {s.hC, s.cTx}, {s.hP, s.pTx},
	} {
		status, nullAt, tipN, thr, seq, gotTip, conf := confirmReadBasis(t, ctx, s.pool, s.chainID,
			rrecHeaderHash(t, ctx, s, tc.h), strings.ToLower(tc.tx.Hex()))
		wantConf := fmt.Sprintf("%d", s.hTip-tc.h+1)
		if status != "confirmed" || nullAt || tipN != int64(s.hTip) || gotTip != tipHash ||
			thr != int64(rrecThresholdN) || seq != 1 || conf != wantConf {
			t.Fatalf("h=%d deep pre-fork basis = (%s null=%v tip %d %s N=%d conf=%s seq=%d), want confirmed tip(%d %s) N=%d conf=%s seq=1",
				tc.h, status, nullAt, tipN, gotTip, thr, conf, seq, s.hTip, tipHash, rrecThresholdN, wantConf)
		}
	}
}

// rrecT29IndexHeaders runs the 002 header scan only (bound tip + local
// history for the search; hold-path scenes need no observations).
func rrecT29IndexHeaders(t *testing.T, s *rrecScene, start uint64) {
	t.Helper()
	sc, err := NewScanner(s.pool, s.client, s.lease, Config{
		StartHeight: start, RPCTimeout: 2 * time.Second, PollInterval: 25 * time.Millisecond,
		RetryInitial: 25 * time.Millisecond, RetryMax: 250 * time.Millisecond,
	}, logscanLogger())
	if err != nil {
		t.Fatalf("NewScanner(): %v", err)
	}
	runScanTo(t, sc, s.hTip, 60*time.Second)
}

// rrecT29BlockCheckpoint reads the 002 checkpoint height + hash.
func rrecT29BlockCheckpoint(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64) (int64, string) {
	t.Helper()
	var h int64
	var hash string
	if err := pool.QueryRow(ctx, `SELECT height, block_hash FROM indexer_checkpoint WHERE chain_id=$1`,
		chainID).Scan(&h, &hash); err != nil {
		t.Fatalf("read indexer_checkpoint: %v", err)
	}
	return h, hash
}

// rrecT29EventDetail returns one recovery audit event detail.
func rrecT29EventDetail(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, event string) string {
	t.Helper()
	var detail string
	if err := pool.QueryRow(ctx, `SELECT detail FROM reorg_recovery_events WHERE chain_id=$1 AND event=$2`,
		chainID, event).Scan(&detail); err != nil {
		t.Fatalf("read %s detail: %v", event, err)
	}
	return detail
}

// rrecT29ZeroHistory asserts zero history revocations: no canonical flips,
// no observation conversions.
func rrecT29ZeroHistory(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, where string) {
	t.Helper()
	var noncanon, trans int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM chain_blocks WHERE chain_id=$1 AND NOT canonical`,
		chainID).Scan(&noncanon); err != nil || noncanon != 0 {
		t.Fatalf("%s: non-canonical blocks = %d (err=%v), want 0 (no flips)", where, noncanon, err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM deposit_observation_transitions WHERE chain_id=$1`,
		chainID).Scan(&trans); err != nil || trans != 0 {
		t.Fatalf("%s: observation transitions = %d (err=%v), want 0 (no revocations)", where, trans, err)
	}
}

// TestT029DepthBoundaryAndEvidence is T029 (FR-04/17, bound + reconcile +
// unobtainable + bad-RPC): depth exactly D=25 fully recovers; depth D+1
// refuses to reconcile_required with the searched range + both tip identities
// + cause class and zero flips/transitions/releases over frozen checkpoints;
// a fork point below the scan start holds with no head chase; a lagging chain
// yields zero revocations + zero releases until evidence suffices, then
// resumes to a full green release.
func TestT029DepthBoundaryAndEvidence(t *testing.T) {
	ctx := context.Background()

	t.Run("allow-bound", func(t *testing.T) {
		s := rrecSetup(t, "rrec-t029-bound")
		rrecMineForkA(t, ctx, s)
		rrecT29ExtendTip(t, ctx, s, 25)
		rrecT29IndexDeep(t, ctx, s)
		rrecForkB(t, ctx, s)
		rrecSeedPauses(t, ctx, s)

		ex := rrecExecutor(t, s.pool, s.lease, s)
		row, cap := rrecDriveToReady(t, ctx, s.pool, ex, s.chainID)
		if row.AncestorNumber == nil || *row.AncestorNumber != int64(s.hA) ||
			row.AncestorHash == nil || *row.AncestorHash != s.aHashes[s.hA] {
			t.Fatalf("ancestor = (%v %v), want (%d %s)", row.AncestorNumber, row.AncestorHash, s.hA, s.aHashes[s.hA])
		}
		if depth, err := reorgDepth(row.BoundOldNumber, *row.AncestorNumber); err != nil || depth != 25 {
			t.Fatalf("depth = %d (err=%v), want exactly 25 (bound)", depth, err)
		}
		var flipped int64
		if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM chain_blocks WHERE chain_id=$1 AND number>$2 AND NOT canonical`,
			s.chainID, int64(s.hA)).Scan(&flipped); err != nil || flipped != 25 {
			t.Fatalf("flipped blocks = %d (err=%v), want 25 (100%% of the sweep)", flipped, err)
		}
		// (Post-replay the sweep carries the fork-B canonical rows alongside
		// the retained fork-A rows, so per-height identity below owns the
		// canonical proof — no zero-canonical assert here.)
		// K ancestor-side untouched; C+P (both confirmed pre-fork) orphaned.
		kObs := rrecReadObs(t, ctx, s.pool, s.chainID, s.kBH, strings.ToLower(s.kTx.Hex()))
		if kObs.status != "confirmed" || kObs.orphanID != "" || !kObs.orphanedAtNull {
			t.Fatalf("K = (%s orphan=%s), want (confirmed, untouched)", kObs.status, kObs.orphanID)
		}
		for _, tc := range []struct {
			name string
			bh   string
			tx   common.Hash
		}{
			{"C", s.cBH, s.cTx}, {"P", s.pBH, s.pTx},
		} {
			if o := rrecReadObs(t, ctx, s.pool, s.chainID, tc.bh, strings.ToLower(tc.tx.Hex())); o.status != "orphaned" || o.orphanID != row.RecoveryID {
				t.Fatalf("%s = (%s orphan=%s), want orphaned under this recovery", tc.name, o.status, o.orphanID)
			}
		}
		if n := rrecTransitionCount(t, ctx, s.pool, s.chainID, "confirmed", "orphaned", row.RecoveryID); n != 2 {
			t.Fatalf("confirmed->orphaned transitions = %d, want 2 (C+P)", n)
		}
		if n := rrecTransitionCount(t, ctx, s.pool, s.chainID, "pending", "orphaned", row.RecoveryID); n != 0 {
			t.Fatalf("pending->orphaned transitions = %d, want 0", n)
		}
		rrecAssertFrontiers(t, row, s.hTip)
		if n := rrecEventCount(t, ctx, s.pool, s.chainID, "reconcile_signaled"); n != 0 {
			t.Fatalf("reconcile_signaled events = %d, want 0 (bound recovers, never holds)", n)
		}
		for h := s.hA + 1; h <= s.hTip; h++ {
			var ch string
			var total int
			if err := s.pool.QueryRow(ctx, `SELECT hash FROM chain_blocks WHERE chain_id=$1 AND number=$2 AND canonical`,
				s.chainID, int64(h)).Scan(&ch); err != nil || ch != s.bHashes[h] {
				t.Fatalf("canonical[%d] = %s (err=%v), want fork-B %s", h, ch, err, s.bHashes[h])
			}
			if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM chain_blocks WHERE chain_id=$1 AND number=$2`,
				s.chainID, int64(h)).Scan(&total); err != nil || total != 2 {
				t.Fatalf("total rows at %d = %d (err=%v), want 2 (A-retained + B-new)", h, total, err)
			}
		}
		bObs := rrecReadObs(t, ctx, s.pool, s.chainID, s.bBH, strings.ToLower(s.bTx.Hex()))
		if bObs.status != "pending" || bObs.blockNumber != int64(s.hA+1) || bObs.amount != "11" || bObs.version != 1 {
			t.Fatalf("fork-B observation = %+v, want (pending @%d amount 11 v1)", bObs, s.hA+1)
		}
		id := rrecRelease(t, ctx, s.pool, ex, s.chainID, row, cap)
		terminal := rrecT29EventDetail(t, ctx, s.pool, s.chainID, "auto_completed")
		for _, want := range []string{
			fmt.Sprintf("bound_old=%d:%s", s.hTip, s.aHashes[s.hTip]),
			fmt.Sprintf("ancestor=%d:%s", s.hA, s.aHashes[s.hA]),
			fmt.Sprintf("swept=%d-%d", s.hA+1, s.hTip),
			"orphaned=2",
			fmt.Sprintf("replayed=block:%d,log:%d,deposit:%d", s.hTip, s.hTip, s.hTip),
			"version=1",
		} {
			if !strings.Contains(terminal, want) {
				t.Fatalf("terminal detail missing %q: %q", want, terminal)
			}
		}
		_ = id
	})

	t.Run("over-bound", func(t *testing.T) {
		s := rrecSetup(t, "rrec-t029-over")
		rrecMineForkA(t, ctx, s)
		rrecT29ExtendTip(t, ctx, s, 26)
		rrecT29IndexHeaders(t, s, 0)
		rrecForkB(t, ctx, s)
		rrecSeedPauses(t, ctx, s)

		ex := rrecExecutor(t, s.pool, s.lease, s)
		done, err := ex.tickIdle(ctx)
		if err != nil || !done {
			t.Fatalf("tickIdle() = (%v, %v), want (true, nil)", done, err)
		}
		preH, preHash := rrecT29BlockCheckpoint(t, ctx, s.pool, s.chainID)
		row, _ := rrecRow(t, ctx, s.pool, s.chainID)
		if row.Phase != reorgPhaseDetected {
			t.Fatalf("phase = %q, want detected", row.Phase)
		}
		if err := ex.tickDetected(ctx, row); err != nil {
			t.Fatalf("tickDetected(): %v", err)
		}
		row, _ = rrecRow(t, ctx, s.pool, s.chainID)
		if row.Phase != reorgPhaseReconcileRequired {
			t.Fatalf("phase = %q, want reconcile_required (bound+1 refuses)", row.Phase)
		}
		// Rich signal: cause class + searched range + bound tip + max depth;
		// both tip identities ride the established event.
		sig := rrecT29EventDetail(t, ctx, s.pool, s.chainID, "reconcile_signaled")
		for _, want := range []string{
			"cause=over_depth",
			fmt.Sprintf("walked=%d-%d", int64(s.hTip)-25, int64(s.hTip)),
			fmt.Sprintf("bound=%d:%s", s.hTip, s.aHashes[s.hTip]),
			"max_depth=25",
			"version=1",
		} {
			if !strings.Contains(sig, want) {
				t.Fatalf("reconcile detail missing %q: %q", want, sig)
			}
		}
		est := rrecT29EventDetail(t, ctx, s.pool, s.chainID, "established")
		for _, want := range []string{
			fmt.Sprintf("old_tip=%d:%s", s.hTip, s.aHashes[s.hTip]),
			fmt.Sprintf("new_tip=%d:%s", s.hTip, s.bHashes[s.hTip]),
		} {
			if !strings.Contains(est, want) {
				t.Fatalf("established detail missing %q: %q", want, est)
			}
		}
		if n := rrecEventCount(t, ctx, s.pool, s.chainID, "reconcile_signaled"); n != 1 {
			t.Fatalf("reconcile_signaled events = %d, want exactly 1", n)
		}
		// Zero flips / transitions / releases; checkpoints frozen (no head
		// chase onto the live fork-B tip hash).
		rrecT29ZeroHistory(t, ctx, s.pool, s.chainID, "over-bound hold")
		if n := rrecEventCount(t, ctx, s.pool, s.chainID, "auto_completed"); n != 0 {
			t.Fatalf("auto_completed events = %d, want 0 (held, never released)", n)
		}
		if gotH, gotHash := rrecT29BlockCheckpoint(t, ctx, s.pool, s.chainID); gotH != preH || gotHash != preHash {
			t.Fatalf("checkpoint moved: (%d %s) vs (%d %s), want frozen", gotH, gotHash, preH, preHash)
		}
		if _, gotHash := rrecT29BlockCheckpoint(t, ctx, s.pool, s.chainID); gotHash != s.aHashes[s.hTip] {
			t.Fatalf("checkpoint hash = %s, want fork-A %s (no head chase)", gotHash, s.aHashes[s.hTip])
		}
		if row.BlockFrontier != nil || row.LogFrontier != nil || row.DepositFrontier != nil {
			t.Fatalf("frontiers moved: (%v %v %v), want all nil (zero progress)",
				row.BlockFrontier, row.LogFrontier, row.DepositFrontier)
		}
		if st, v := AnnotateRecoveryHeight(row, false, int64(s.hTip)); st != RecoveryStatePausedReconcile || v != ValidityUnknownPaused {
			t.Fatalf("held annotation = (%s %s), want (paused_reconcile unknown_paused)", st, v)
		}
	})

	t.Run("unobtainable", func(t *testing.T) {
		s := rrecSetup(t, "rrec-t029-unobtain")
		rrecMineForkA(t, ctx, s)
		// Genuinely late-started 002: the ancestor (hA) was never indexed,
		// so the fork point sits below available history AND the scan start.
		rrecT29IndexHeaders(t, s, s.hA+1)
		rrecForkB(t, ctx, s)
		rrecSeedPauses(t, ctx, s)

		ex := rrecExecutor(t, s.pool, s.lease, s)
		done, err := ex.tickIdle(ctx)
		if err != nil || !done {
			t.Fatalf("tickIdle() = (%v, %v), want (true, nil)", done, err)
		}
		preH, preHash := rrecT29BlockCheckpoint(t, ctx, s.pool, s.chainID)
		if preHash != s.aHashes[s.hTip] {
			t.Fatalf("pre-hold checkpoint hash = %s, want fork-A %s", preHash, s.aHashes[s.hTip])
		}
		row, _ := rrecRow(t, ctx, s.pool, s.chainID)
		if err := ex.tickDetected(ctx, row); err != nil {
			t.Fatalf("tickDetected(): %v", err)
		}
		row, _ = rrecRow(t, ctx, s.pool, s.chainID)
		if row.Phase != reorgPhaseReconcileRequired {
			t.Fatalf("phase = %q, want reconcile_required (unobtainable holds)", row.Phase)
		}
		sig := rrecT29EventDetail(t, ctx, s.pool, s.chainID, "reconcile_signaled")
		for _, want := range []string{
			"cause=below_scan_start",
			fmt.Sprintf("start=%d", s.hA+1),
			fmt.Sprintf("bound=%d:%s", s.hTip, s.aHashes[s.hTip]),
			"version=1",
		} {
			if !strings.Contains(sig, want) {
				t.Fatalf("reconcile detail missing %q: %q", want, sig)
			}
		}
		rrecT29ZeroHistory(t, ctx, s.pool, s.chainID, "unobtainable hold")
		if n := rrecEventCount(t, ctx, s.pool, s.chainID, "auto_completed"); n != 0 {
			t.Fatalf("auto_completed events = %d, want 0 (held, never released)", n)
		}
		if gotH, gotHash := rrecT29BlockCheckpoint(t, ctx, s.pool, s.chainID); gotH != preH || gotHash != preHash {
			t.Fatalf("checkpoint moved: (%d %s) vs (%d %s), want frozen (no head chase)", gotH, gotHash, preH, preHash)
		}
	})

	t.Run("stalled-rpc", func(t *testing.T) {
		s := rrecSetup(t, "rrec-t029-stall")
		rrecMineForkA(t, ctx, s)
		rrecIndexForkA(t, ctx, s)

		// Lagging chain: revert to the ancestor, mine exactly one fork-B
		// block, then freeze automining — bound tip (hTip) towers above the
		// live tip (hA+1), so the ancestor search has insufficient evidence.
		var ok bool
		s.node.mustCall(t, &ok, "evm_revert", s.snapID)
		if !ok {
			t.Fatal("evm_revert refused the ancestor snapshot")
		}
		bTx, bH := s.node.sendTransferAt(t, s.tokenB)
		s.bTx = bTx
		if bH != s.hA+1 {
			t.Fatalf("fork-B transfer height = %d, want %d", bH, s.hA+1)
		}
		s.bHashes[s.hA+1] = rrecHeaderHash(t, ctx, s, s.hA+1)
		s.bBH = s.bHashes[s.hA+1]
		if s.bHashes[s.hA+1] == s.aHashes[s.hA+1] {
			t.Fatalf("fork-B hash at %d equals fork-A hash: no divergence", s.hA+1)
		}
		if got := rrecHeaderHash(t, ctx, s, s.hA); got != s.aHashes[s.hA] {
			t.Fatalf("ancestor hash moved: %s vs %s", got, s.aHashes[s.hA])
		}
		rrecSetAutomine(t, s, false)
		rrecSeedPauses(t, ctx, s)

		ex := rrecExecutor(t, s.pool, s.lease, s)
		done, err := ex.tickIdle(ctx)
		if err != nil || !done {
			t.Fatalf("tickIdle() = (%v, %v), want (true, nil)", done, err)
		}
		preCP := rrecReadCheckpoints(t, ctx, s.pool, s.chainID)

		// While stalled the search holds inside a short bounded wait: the
		// ancestor walk spends ~1s per undecidable height (backoff/poll
		// waits), so a 4s window keeps it mid-descent with no terminal
		// verdict possible — no error, no phase move, zero revocations,
		// zero releases. (A longer stall would let the walk descend the
		// intact prefix to below_scan_start and reconcile; the short
		// window pins the pure-hold case, and the resume below proves
		// the same recovery converges once evidence suffices.)
		stalled, cancel := context.WithTimeout(ctx, 4*time.Second)
		defer cancel()
		row, _ := rrecRow(t, ctx, s.pool, s.chainID)
		if err := ex.tickDetected(stalled, row); err != nil {
			t.Fatalf("stalled tickDetected() = %v, want nil (evidence hold)", err)
		}
		row, _ = rrecRow(t, ctx, s.pool, s.chainID)
		if row.Phase != reorgPhaseDetected {
			t.Fatalf("stalled phase = %q, want detected (hold position)", row.Phase)
		}
		if n := rrecEventCount(t, ctx, s.pool, s.chainID, "ancestor_confirmed"); n != 0 {
			t.Fatalf("ancestor_confirmed events while stalled = %d, want 0", n)
		}
		if n := rrecEventCount(t, ctx, s.pool, s.chainID, "reconcile_signaled"); n != 0 {
			t.Fatalf("reconcile_signaled events while stalled = %d, want 0 (hold, not terminal)", n)
		}
		if n := rrecEventCount(t, ctx, s.pool, s.chainID, "auto_completed"); n != 0 {
			t.Fatalf("auto_completed events while stalled = %d, want 0", n)
		}
		rrecT29ZeroHistory(t, ctx, s.pool, s.chainID, "stalled hold")
		if got := rrecReadCheckpoints(t, ctx, s.pool, s.chainID); got != preCP {
			t.Fatalf("checkpoints moved while stalled: %+v vs %+v, want frozen", got, preCP)
		}

		// Evidence suffices again: mine the rest of fork B and the SAME
		// recovery converges to a full green release.
		rrecSetAutomine(t, s, true)
		s.node.mine(t, s.hTip-bH)
		if got := s.node.blockNumber(t); got != s.hTip {
			t.Fatalf("fork-B tip = %d, want %d", got, s.hTip)
		}
		for h := s.hA + 2; h <= s.hTip; h++ {
			s.bHashes[h] = rrecHeaderHash(t, ctx, s, h)
			if s.bHashes[h] == s.aHashes[h] {
				t.Fatalf("fork-B hash at %d equals fork-A hash %s: no divergence", h, s.aHashes[h])
			}
		}
		if got := rrecHeaderHash(t, ctx, s, s.hA); got != s.aHashes[s.hA] {
			t.Fatalf("ancestor hash moved: %s vs %s", got, s.aHashes[s.hA])
		}
		row, cap := rrecDriveToReady(t, ctx, s.pool, ex, s.chainID)
		if row.AncestorNumber == nil || *row.AncestorNumber != int64(s.hA) ||
			row.AncestorHash == nil || *row.AncestorHash != s.aHashes[s.hA] {
			t.Fatalf("ancestor = (%v %v), want (%d %s)", row.AncestorNumber, row.AncestorHash, s.hA, s.aHashes[s.hA])
		}
		rrecAssertFrontiers(t, row, s.hTip)
		rrecRelease(t, ctx, s.pool, ex, s.chainID, row, cap)
	})
}
