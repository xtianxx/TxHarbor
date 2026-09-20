//go:build integration

// Batch B integration tests (T010/T006) on a real PostgreSQL via
// startIndexerPostgres — new assertions only; no production file is touched
// and no existing test is modified. The helpers reused here live in
// confirmcommit_integration_test.go (confirmSeedPending, confirmSeedPolicyRow,
// confirmReadBasis, confirmAssertZeroWrite, confirmCommitter), lease_
// integration_test.go (startIndexerPostgres, openIndexerPool), deposit_
// integration_test.go (depositITConfig, depositITScanner, depositSeedCanonical,
// depositSeedUpstream, depositSeedCheckpoint, depositSeedHistory,
// depositSeedTransferRow, depositITLease, depositITPrepareUnit,
// depositCheckpointState, depositCountRows) and reorgcommit_test.go
// (testRecoveryCap). Every test carries defect/invariant/gap/level/criteria
// and every refusal path asserts durable snapshot equality (zero writes).
package indexer

import (
	"context"
	"errors"
	"math/big"
	"sync"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestConfirmCommitExactNPlusOne closes the exact N+1 confirmation boundary:
// tip - h == N converts once with confirmations = N+1 (the spec value
// max(0,tip-h+1)), not N.
//
// Defect: none known — this is a numeric-boundary coverage gap.
// Invariant: 005 data-model §确认数计算 writes the exact NUMERIC value
// (confirmations = tip - block_number + 1) at the only conversion write
// (confirmDepositObservationSQL); the R1-equivalent gate only decides
// eligibility. A clamp to N or an off-by-one would pass every existing
// happy-path test (they all use tip-h == N-1).
// Gap: TestConfirmCommitHappyPath pins tip=109,h=100,N=10 -> "10"; no test
// pins the first step past the threshold (tip=110 -> "11"), the value the
// audit trail is supposed to carry one block after eligibility.
// Level: integration (real PostgreSQL, real migration 000005 schema, real
// committer transaction).
// Criteria: one ConfirmDepositUnit at tip=110/h=100/N=10 stores
// confirmations::text == "11" exactly, confirm_tip_number=110,
// confirm_tip_hash=hash(110), exactly one policy row and exactly one
// observation; the SQL-level recomputation
// (confirm_tip_number - block_number + 1)::TEXT equals the stored text
// (confirmnumeric pattern).
func TestConfirmCommitExactNPlusOne(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	const chainID, h, tip, n = int64(91), uint64(100), uint64(110), uint64(10)
	depositSeedCanonical(t, ctx, pool, chainID, h, tip, true)
	bh, txHash := confirmSeedPending(t, ctx, pool, chainID, h)
	confirmSeedPolicyRow(t, ctx, pool, chainID, 1, int64(n), nil, "bootstrap", nil)

	// Pre-state: the candidate is pending with no conversion facts.
	status, nullAt, _, _, _, _, conf := confirmReadBasis(t, ctx, pool, chainID, bh, txHash)
	if status != "pending" || !nullAt || conf != "" {
		t.Fatalf("pre-commit candidate = status %s nullAt %v conf %q, want pending/no facts", status, nullAt, conf)
	}

	c, lease := confirmCommitter(t, pool, chainID, n)
	rcap := testRecoveryCap(t, ctx, pool, chainID)
	basis := ConfirmBasis{BlockHash: bh, TxHash: txHash, Height: h,
		TipNumber: tip, TipHash: depositBlockHash(tip), PolicySeq: 1, ThresholdN: n}
	if err := c.ConfirmDepositUnit(ctx, lease, basis, rcap); err != nil {
		t.Fatalf("ConfirmDepositUnit(tip-h == N): %v", err)
	}

	status, nullAt, tipN, thr, seq, tipH, conf := confirmReadBasis(t, ctx, pool, chainID, bh, txHash)
	if status != "confirmed" || nullAt {
		t.Fatalf("status=%s nullAt=%v, want confirmed/non-null confirmed_at", status, nullAt)
	}
	if tipN != int64(tip) || tipH != depositBlockHash(tip) || thr != int64(n) || seq != 1 {
		t.Fatalf("basis = tip(%d %s) N=%d seq=%d, want tip(110 %s) N=10 seq=1",
			tipN, tipH, thr, seq, depositBlockHash(tip))
	}
	if conf != "11" {
		t.Fatalf("confirmations = %q, want exact decimal text \"11\" (N+1, not N)", conf)
	}

	// Raw SQL text read (never through int64/float64 transit) plus the
	// confirmnumeric audit recomputation in exact NUMERIC arithmetic.
	var raw, recomputed string
	if err := pool.QueryRow(ctx, `
SELECT confirmations::TEXT, (confirm_tip_number::NUMERIC - block_number::NUMERIC + 1)::TEXT
FROM deposit_observations
WHERE chain_id = $1 AND block_hash = $2 AND tx_hash = $3 AND log_index = 0`,
		chainID, bh, txHash).Scan(&raw, &recomputed); err != nil {
		t.Fatalf("SQL exact-text/recompute read: %v", err)
	}
	if raw != "11" || recomputed != "11" {
		t.Fatalf("SQL audit = stored %q recomputed %q, want both \"11\"", raw, recomputed)
	}
	if k := depositCountRows(t, ctx, pool, "confirmation_policy_history", chainID); k != 1 {
		t.Fatalf("policy rows = %d, want exactly 1", k)
	}
	if k := depositCountRows(t, ctx, pool, "deposit_observations", chainID); k != 1 {
		t.Fatalf("observations = %d, want exactly 1 (exactly-once conversion)", k)
	}
}

// TestConfirmCommitBelowDepthThenRevive closes the below-depth refusal ->
// chain-extension -> re-commit lifecycle on the same pending candidate.
//
// Defect: none known — this is a revival-path coverage gap.
// Invariant: a below-depth candidate is never written (the under-lock gate
// recomputation refuses with zero writes, typed — no string sniffing); once
// the canonical chain reaches tip-h == N-1 exactly, the same still-pending
// row converts exactly once with confirmations = N, and the earlier refusal
// left no policy/pause/transition residue.
// Gap: TestConfirmCommitBelowDepth stops at the refusal. Nothing proves the
// refused candidate later converts after the chain extends (the loop's
// "wait" is only encoded by the caller's retry classification), nor that the
// refusal wrote zero pause/transition rows.
// Level: integration.
// Criteria: refusal #1 is *ConfirmationChainViewError with
// confirmAssertZeroWrite(...,1); after seeding canonical block 109 and
// re-committing with a fresh basis, status=confirmed, conf="10",
// confirmation_policy_history==1, and deposit_pause/log_pause/indexer_pause/
// deposit_observation_transitions all 0.
func TestConfirmCommitBelowDepthThenRevive(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	const chainID, h, tip, n = int64(92), uint64(100), uint64(108), uint64(10)
	depositSeedCanonical(t, ctx, pool, chainID, h, tip, true)
	bh, txHash := confirmSeedPending(t, ctx, pool, chainID, h)
	confirmSeedPolicyRow(t, ctx, pool, chainID, 1, int64(n), nil, "bootstrap", nil)

	c, lease := confirmCommitter(t, pool, chainID, n)
	rcap := testRecoveryCap(t, ctx, pool, chainID)
	belowDepth := ConfirmBasis{BlockHash: bh, TxHash: txHash, Height: h,
		TipNumber: tip, TipHash: depositBlockHash(tip), PolicySeq: 1, ThresholdN: n}
	err := c.ConfirmDepositUnit(ctx, lease, belowDepth, rcap)
	var chainView *ConfirmationChainViewError
	if !errors.As(err, &chainView) {
		t.Fatalf("below-depth commit = %v (%T), want *ConfirmationChainViewError", err, err)
	}
	// The refusal is the under-lock gate recomputation (tip-h == 8 < N-1),
	// not the tip/candidate/block readjudication: tip 108 was canonical and
	// hash-matched when the gate ran.
	confirmAssertZeroWrite(t, ctx, pool, chainID, bh, txHash, 1)

	// Extend the canonical chain by exactly one block: tip-h becomes N-1=9.
	depositSeedCanonical(t, ctx, pool, chainID, tip+1, tip+1, true)
	revivedTip := tip + 1
	rcap2 := testRecoveryCap(t, ctx, pool, chainID)
	revived := ConfirmBasis{BlockHash: bh, TxHash: txHash, Height: h,
		TipNumber: revivedTip, TipHash: depositBlockHash(revivedTip), PolicySeq: 1, ThresholdN: n}
	if err := c.ConfirmDepositUnit(ctx, lease, revived, rcap2); err != nil {
		t.Fatalf("re-commit after chain extension to tip=%d: %v", revivedTip, err)
	}

	status, nullAt, tipN, thr, seq, tipH, conf := confirmReadBasis(t, ctx, pool, chainID, bh, txHash)
	if status != "confirmed" || nullAt {
		t.Fatalf("status=%s nullAt=%v, want confirmed/non-null confirmed_at", status, nullAt)
	}
	if tipN != int64(revivedTip) || tipH != depositBlockHash(revivedTip) || thr != int64(n) || seq != 1 || conf != "10" {
		t.Fatalf("basis = tip(%d %s) N=%d conf=%s seq=%d, want tip(109 %s) N=10 conf=10 seq=1",
			tipN, tipH, thr, conf, seq, depositBlockHash(revivedTip))
	}
	if k := depositCountRows(t, ctx, pool, "confirmation_policy_history", chainID); k != 1 {
		t.Fatalf("policy rows = %d, want exactly 1", k)
	}
	for _, table := range []string{"deposit_pause", "log_pause", "indexer_pause", "deposit_observation_transitions"} {
		if k := depositCountRows(t, ctx, pool, table, chainID); k != 0 {
			t.Fatalf("%s rows = %d, want 0 (refusal/commit write no pause or transition row)", table, k)
		}
	}
	if k := depositCountRows(t, ctx, pool, "deposit_observations", chainID); k != 1 {
		t.Fatalf("observations = %d, want exactly 1", k)
	}
}

// batchBRaceWorkers is the fixed fan-out of the same-unit commit race (8 ==
// the pool's MaxConns, so every worker holds one real connection).
const batchBRaceWorkers = 8

// batchBSeedSameUnit seeds canonical [10,20], the upstream coverage proof and
// three matched watched transfers, so one unit [10,20] carries
// len(batch.matched)==3 rows for the race assertions.
func batchBSeedSameUnit(t *testing.T, ctx context.Context, pool *pgxpool.Pool, cfg DepositConfig) {
	t.Helper()
	depositSeedCanonical(t, ctx, pool, cfg.ChainID, 10, 20, true)
	depositSeedUpstream(t, ctx, pool, cfg.ChainID, 0, cfg.LogConfigHash, 21)
	for _, h := range []uint64{12, 15, 18} {
		depositSeedTransferRow(t, ctx, pool, cfg.ChainID, h, depositBlockHash(h), depositTxHash(h, 0), 0,
			common.HexToAddress(testContractA), common.HexToAddress(testContractB),
			common.HexToAddress(depositWatchAddr), big.NewInt(1))
	}
}

// batchBRunSameUnitCommits releases batchBRaceWorkers goroutines from one
// close(start) barrier (no sleeps): every worker commits the same unit [10,20]
// with the same captured basis through one shared lease.
func batchBRunSameUnitCommits(ctx context.Context, sc *DepositScanner, lease *Lease,
	unit *depositUnit, batch depositBatch, captured *depositProgress, rcap RecoveryCapture) []error {
	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make([]error, batchBRaceWorkers)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			errs[i] = sc.commitDepositUnit(ctx, lease, unit, batch, captured, 10, 20, rcap)
		}(i)
	}
	close(start)
	wg.Wait()
	return errs
}

// batchBAssertCommitRace proves the exactly-once outcome: one nil winner, all
// losers refused with the variant's documented sentinel, the checkpoint
// advanced exactly once to b+1=21, observations == len(matched) with
// version_seq=1, exactly one bootstrap history row, and zero pause/duplicate
// rows.
func batchBAssertCommitRace(t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	cfg DepositConfig, batch depositBatch, errs []error, loser func(error) bool, loserName string) {
	t.Helper()
	if len(errs) != batchBRaceWorkers {
		t.Fatalf("race outcome count = %d, want %d", len(errs), batchBRaceWorkers)
	}
	wins, losses := 0, 0
	for i, err := range errs {
		switch {
		case err == nil:
			wins++
		case loser(err):
			losses++
		default:
			t.Fatalf("worker %d error = %v (%T), want nil or %s", i, err, err, loserName)
		}
	}
	if wins != 1 || losses != batchBRaceWorkers-1 {
		t.Fatalf("race wins=%d losses=%d (%v), want exactly 1 win and %d %s losses",
			wins, losses, errs, batchBRaceWorkers-1, loserName)
	}

	start, hash, next, ok := depositCheckpointState(t, ctx, pool, cfg.ChainID)
	if !ok || start != 10 || hash != cfg.ConfigHash || next != 21 {
		t.Fatalf("checkpoint = (%d,%s,%d,%v), want (10,%s,21,true) — one advance only",
			start, hash, next, ok, cfg.ConfigHash)
	}
	if n := depositCountRows(t, ctx, pool, "deposit_checkpoint", cfg.ChainID); n != 1 {
		t.Fatalf("checkpoint rows = %d, want exactly 1", n)
	}
	if n := depositCountRows(t, ctx, pool, "deposit_observations", cfg.ChainID); n != len(batch.matched) {
		t.Fatalf("observations = %d, want exactly len(matched)=%d", n, len(batch.matched))
	}
	var versioned int
	if err := pool.QueryRow(ctx, `
SELECT COUNT(*) FROM deposit_observations
WHERE chain_id = $1 AND version_seq = 1`, cfg.ChainID).Scan(&versioned); err != nil {
		t.Fatalf("count version_seq=1 observations: %v", err)
	}
	if versioned != len(batch.matched) {
		t.Fatalf("version_seq=1 observations = %d, want %d (captured version, not a loopback)",
			versioned, len(batch.matched))
	}
	var duplicates int
	if err := pool.QueryRow(ctx, `
SELECT COUNT(*) FROM (
    SELECT 1 FROM deposit_observations WHERE chain_id = $1
    GROUP BY block_hash, tx_hash, log_index HAVING COUNT(*) > 1
) d`, cfg.ChainID).Scan(&duplicates); err != nil {
		t.Fatalf("count duplicate observation identities: %v", err)
	}
	if duplicates != 0 {
		t.Fatalf("duplicate observation identities = %d, want 0", duplicates)
	}
	if n := depositCountRows(t, ctx, pool, "deposit_config_history", cfg.ChainID); n != 1 {
		t.Fatalf("config history rows = %d, want exactly 1 (a single bootstrap)", n)
	}
	for _, table := range []string{"deposit_pause", "log_pause", "indexer_pause", "deposit_observation_transitions"} {
		if n := depositCountRows(t, ctx, pool, table, cfg.ChainID); n != 0 {
			t.Fatalf("%s rows = %d, want 0 (a commit writes no pause/transition row)", table, n)
		}
	}
}

// TestDepositCommitConcurrentSameUnitExactlyOneExecutes closes the 8-way
// same-unit deposit commit race (depositcommit.go:186-427) for both captured
// bases the loop can present.
//
// Defect: none known — serialization is the coordination row lock; this is a
// concurrency coverage gap.
// Invariant: for one unit [10,20] at most one commit may write; exactly one
// worker gets nil, the checkpoint advances exactly once to b+1=21 (guard
// next_block == a), observations carry the captured version_seq (never
// "latest at commit"), and one bootstrap history row exists.
// Gap: the existing concurrent coverage is a 2-worker race (and a cross-pool
// variant); an 8-way race on the SAME prepared unit with the SAME basis, for
// both the empty-captured first unit and the version_seq=1 non-first unit,
// was never exercised.
// Flagged inconsistency (encoded, not resolved): the loser verdict is
// asymmetric by design of the implemented guards — variant A (captured=nil)
// losers refuse at depositcommit.go:270-273 with errDepositVersionMismatch
// because a checkpoint has since appeared; variant B (captured version_seq=1)
// losers refuse at depositcommit.go:289-293 with errStaleState because the
// checkpoint moved to 21. Both are asserted exactly as implemented.
// Level: integration.
// Criteria: N=8 goroutines released by one close(start) barrier from one
// pool; exactly 1 nil; the other 7 all the variant sentinel; checkpoint
// (10, config_hash, 21) with one row; observations == len(matched), all
// version_seq=1; config_history == 1; zero pause rows and zero duplicate
// identities.
func TestDepositCommitConcurrentSameUnitExactlyOneExecutes(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	t.Run("captured_nil_losers_version_mismatch", func(t *testing.T) {
		const chainID = int64(93)
		cfg := depositITConfig(t, chainID, testContractA)
		batchBSeedSameUnit(t, ctx, pool, cfg)

		sc := depositITScanner(t, pool, cfg)
		lease := depositITLease(t, pool, chainID)
		rcap := testRecoveryCap(t, ctx, pool, chainID)
		unit, batch, captured := depositITPrepareUnit(t, ctx, sc, 10, 20)
		if captured != nil {
			t.Fatalf("captured progress = %+v, want nil (first unit)", captured)
		}
		if len(batch.matched) != 3 || batch.zero != 0 || batch.nomatch != 0 {
			t.Fatalf("batch = matched %d zero %d nomatch %d, want 3/0/0", len(batch.matched), batch.zero, batch.nomatch)
		}

		errs := batchBRunSameUnitCommits(ctx, sc, lease, unit, batch, captured, rcap)
		batchBAssertCommitRace(t, ctx, pool, cfg, batch, errs,
			func(err error) bool { return errors.Is(err, errDepositVersionMismatch) },
			"errDepositVersionMismatch")
	})

	t.Run("captured_version_one_losers_stale_state", func(t *testing.T) {
		const chainID = int64(94)
		cfg := depositITConfig(t, chainID, testContractA)
		batchBSeedSameUnit(t, ctx, pool, cfg)
		depositSeedCheckpoint(t, ctx, pool, cfg.ChainID, 10, cfg.ConfigHash, 10)
		depositSeedHistory(t, ctx, pool, cfg.ChainID, 1, 10, cfg.ConfigHash)

		sc := depositITScanner(t, pool, cfg)
		lease := depositITLease(t, pool, chainID)
		rcap := testRecoveryCap(t, ctx, pool, chainID)
		unit, batch, captured := depositITPrepareUnit(t, ctx, sc, 10, 20)
		if captured == nil || captured.startBlock != 10 || captured.configHash != cfg.ConfigHash ||
			captured.nextBlock != 10 || captured.versionSeq != 1 {
			t.Fatalf("captured progress = %+v, want version 1 at next_block 10", captured)
		}
		if len(batch.matched) != 3 {
			t.Fatalf("batch matched = %d, want 3", len(batch.matched))
		}

		errs := batchBRunSameUnitCommits(ctx, sc, lease, unit, batch, captured, rcap)
		batchBAssertCommitRace(t, ctx, pool, cfg, batch, errs,
			func(err error) bool { return errors.Is(err, errStaleState) },
			"errStaleState")
	})
}
