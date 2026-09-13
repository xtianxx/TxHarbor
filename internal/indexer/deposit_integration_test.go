//go:build integration

// T005 acceptance tests on a real PostgreSQL: upstream identity alignment,
// R5 gap classification, the coverage proof (watermark, chain identity,
// upstream start, three pause rows, block-by-block canonical binding) and the
// deposit-state integrity reads. No Anvil here: the upstream truth is seeded
// as rows (chain_blocks, erc20_transfer_logs, log_checkpoint); the full-stack
// Anvil path is T007. Assertions read durable rows back through a pool.
//
// T006 extends this file with the unit atomic commit assertions: the first
// unit's bootstrap row, the exact guard, version isolation (including the
// same-hash loopback), captured-version attribution, conflict rollback,
// one-sided corruption, canonical abort, empty-interval advance and the
// uncertain-COMMIT recovery.
package indexer

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/eth"
)

// depositBlockHash builds the canonical hash for height n; depositTxHash a
// deterministic transaction hash.
func depositBlockHash(n uint64) string { return fmt.Sprintf("0x%064x", 0xd00d_0000+n) }
func depositTxHash(n, index uint64) string {
	return fmt.Sprintf("0x%064x", 0xdead_0000+n*16+index)
}

// depositITConfig is the valid configuration shape used by the integration
// tests; contracts are the shared upstream whitelist.
func depositITConfig(t *testing.T, chainID int64, contracts ...string) DepositConfig {
	t.Helper()
	cfg := DepositConfig{
		ChainID:        chainID,
		StartBlock:     10,
		Assets:         []config.DepositEntry{{Address: testContractA, Effective: 10}},
		Watches:        []config.DepositEntry{{Address: depositWatchAddr, Effective: 10}},
		ConfigHash:     strings.Repeat("aa", 32),
		BatchBlocks:    500,
		LogContracts:   contracts,
		LogStartHeight: 0,
	}
	cfg.LogConfigHash = depositUpstreamHash(t, contracts...)
	return cfg
}

func depositITScanner(t *testing.T, pool *pgxpool.Pool, cfg DepositConfig) *DepositScanner {
	t.Helper()
	sc, err := NewDepositScanner(pool, cfg)
	if err != nil {
		t.Fatalf("NewDepositScanner(): %v", err)
	}
	return sc
}

func depositSeedCanonical(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, from, to uint64, canonical bool) {
	t.Helper()
	for n := from; n <= to; n++ {
		parent := "0x" + strings.Repeat("00", 32)
		if n > 0 {
			parent = depositBlockHash(n - 1)
		}
		if _, err := pool.Exec(ctx, `
INSERT INTO chain_blocks (chain_id, number, hash, parent_hash, canonical)
VALUES ($1, $2, $3, $4, $5)`, chainID, int64(n), depositBlockHash(n), parent, canonical); err != nil {
			t.Fatalf("seed chain_blocks %d/%d: %v", chainID, n, err)
		}
	}
}

func depositSeedUpstream(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, start uint64, hash string, next uint64) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
INSERT INTO log_checkpoint (chain_id, start_block, config_hash, next_block)
VALUES ($1, $2, $3, $4)`, chainID, int64(start), hash, int64(next)); err != nil {
		t.Fatalf("seed log_checkpoint: %v", err)
	}
}

// depositSeedSourceRow plants one stored Transfer row for a watched address;
// its block_hash is caller-controlled so canonical mismatches can be built.
func depositSeedSourceRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, block uint64, blockHash, txHash string, logIndex uint64) {
	t.Helper()
	topic1 := common.BytesToHash(common.HexToAddress(testContractB).Bytes()).Hex()
	topic2 := common.BytesToHash(common.HexToAddress(depositWatchAddr).Bytes()).Hex()
	if _, err := pool.Exec(ctx, `
INSERT INTO erc20_transfer_logs
    (chain_id, block_number, block_hash, tx_hash, log_index, contract, topic0, topic1, topic2, data)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		chainID, int64(block), blockHash, txHash, int64(logIndex),
		testContractA, eth.TransferSig.Hex(), topic1, topic2,
		common.BigToHash(big.NewInt(1)).Hex()); err != nil {
		t.Fatalf("seed erc20_transfer_logs: %v", err)
	}
}

func depositSeedCheckpoint(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, start uint64, hash string, next uint64) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
INSERT INTO deposit_checkpoint (chain_id, start_block, config_hash, next_block)
VALUES ($1, $2, $3, $4)`, chainID, int64(start), hash, int64(next)); err != nil {
		t.Fatalf("seed deposit_checkpoint: %v", err)
	}
}

func depositSeedHistory(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, seq int64, start uint64, hash string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
INSERT INTO deposit_config_history
    (chain_id, version_seq, config_hash, start_block, assets, watches, replay_from, operator)
VALUES ($1, $2, $3, $4, $5, $6, $7, 'bootstrap')`,
		chainID, seq, hash, int64(start),
		testContractA+":10", depositWatchAddr+":10", int64(start)); err != nil {
		t.Fatalf("seed deposit_config_history: %v", err)
	}
}

// TestDepositUpstreamDriftRefused: the env whitelist recomputation disagrees
// with the persisted log_checkpoint identity, so consumption refuses before
// reading any source row (R5 step 1, upstream_drift).
func TestDepositUpstreamDriftRefused(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	cfg := depositITConfig(t, 7, testContractA)
	depositSeedCanonical(t, ctx, pool, cfg.ChainID, 10, 20, true)
	depositSeedUpstream(t, ctx, pool, cfg.ChainID, 0, strings.Repeat("ff", 32), 21)

	sc := depositITScanner(t, pool, cfg)
	_, err := sc.readCoveredUnit(ctx, pool, 10, 20)
	var drift *upstreamDriftError
	if !errors.As(err, &drift) {
		t.Fatalf("readCoveredUnit() error = %v (%T), want *upstreamDriftError", err, err)
	}
	if drift.persisted != strings.Repeat("ff", 32) || drift.expected != cfg.LogConfigHash {
		t.Fatalf("drift = %+v, want persisted=%s expected=%s", drift, strings.Repeat("ff", 32), cfg.LogConfigHash)
	}
	if !strings.Contains(err.Error(), "upstream_drift") {
		t.Fatalf("error %q does not name upstream_drift", err.Error())
	}
}

// TestDepositCoveredUnitProof: a covered unit returns the aligned upstream
// state, the canonical binding of every height and the source rows in
// consumption order; rows from another chain never leak in (chain identity).
func TestDepositCoveredUnitProof(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	cfg := depositITConfig(t, 7, testContractA)
	depositSeedCanonical(t, ctx, pool, cfg.ChainID, 10, 20, true)
	depositSeedUpstream(t, ctx, pool, cfg.ChainID, 0, cfg.LogConfigHash, 21)
	// Reverse insertion order: the read must impose (block_number, log_index).
	depositSeedSourceRow(t, ctx, pool, cfg.ChainID, 15, depositBlockHash(15), depositTxHash(15, 2), 2)
	depositSeedSourceRow(t, ctx, pool, cfg.ChainID, 12, depositBlockHash(12), depositTxHash(12, 0), 0)
	depositSeedSourceRow(t, ctx, pool, cfg.ChainID, 15, depositBlockHash(15), depositTxHash(15, 1), 1)
	// Foreign-chain decoys with the same heights must not appear.
	otherChain := cfg.ChainID + 1
	depositSeedCanonical(t, ctx, pool, otherChain, 10, 20, true)
	depositSeedUpstream(t, ctx, pool, otherChain, 0, cfg.LogConfigHash, 21)
	depositSeedSourceRow(t, ctx, pool, otherChain, 12, depositBlockHash(12), depositTxHash(12, 0), 0)

	sc := depositITScanner(t, pool, cfg)
	unit, err := sc.readCoveredUnit(ctx, pool, 10, 20)
	if err != nil {
		t.Fatalf("readCoveredUnit() error = %v, want nil", err)
	}
	if len(unit.canonical) != 11 || unit.canonical[15] != depositBlockHash(15) {
		t.Fatalf("canonical = %d entries, [15]=%q; want 11 entries and %q",
			len(unit.canonical), unit.canonical[15], depositBlockHash(15))
	}
	if unit.upstream.startBlock != 0 || unit.upstream.nextBlock != 21 || unit.upstream.configHash != cfg.LogConfigHash {
		t.Fatalf("upstream = %+v, want start=0 next=21 hash=%s", unit.upstream, cfg.LogConfigHash)
	}
	if len(unit.rows) != 3 {
		t.Fatalf("rows = %d, want 3 (foreign chain decoy must be excluded)", len(unit.rows))
	}
	assert := func(i int, block, index uint64) {
		t.Helper()
		row := unit.rows[i]
		if row.blockNumber != block || row.logIndex != index {
			t.Fatalf("rows[%d] = block %d index %d, want block %d index %d", i, row.blockNumber, row.logIndex, block, index)
		}
		if row.blockHash != depositBlockHash(block) || row.txHash != depositTxHash(block, index) {
			t.Fatalf("rows[%d] identity = %s/%s, want %s/%s", i, row.blockHash, row.txHash,
				depositBlockHash(block), depositTxHash(block, index))
		}
	}
	assert(0, 12, 0)
	assert(1, 15, 1)
	assert(2, 15, 2)
	if row := unit.rows[0]; row.contract != testContractA || row.topic0 != eth.TransferSig.Hex() {
		t.Fatalf("rows[0] source = %s topic0=%s", row.contract, row.topic0)
	}
}

// TestDepositGapClassificationIntegration covers the R5 verdicts against
// durable upstream rows: transient wait, structural below upstream start and
// structural asset not indexed.
func TestDepositGapClassificationIntegration(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	t.Run("transient_behind_head", func(t *testing.T) {
		cfg := depositITConfig(t, 7, testContractA)
		depositSeedUpstream(t, ctx, pool, cfg.ChainID, 0, cfg.LogConfigHash, 15)
		sc := depositITScanner(t, pool, cfg)
		_, err := sc.readCoveredUnit(ctx, pool, 10, 20)
		var gap *depositGap
		if !errors.As(err, &gap) {
			t.Fatalf("error = %v (%T), want *depositGap", err, err)
		}
		if gap.class != depositGapTransient || gap.cause != gapCauseBehindHead {
			t.Fatalf("gap = %+v, want transient/behind_head", gap)
		}
		if gap.from != 10 || gap.to != 20 || gap.configHash != cfg.ConfigHash {
			t.Fatalf("gap range/config = %+v", gap)
		}
	})

	t.Run("structural_below_upstream_start", func(t *testing.T) {
		cfg := depositITConfig(t, 8, testContractA)
		depositSeedUpstream(t, ctx, pool, cfg.ChainID, 12, cfg.LogConfigHash, 100)
		sc := depositITScanner(t, pool, cfg)
		_, err := sc.readCoveredUnit(ctx, pool, 10, 20)
		var gap *depositGap
		if !errors.As(err, &gap) {
			t.Fatalf("error = %v (%T), want *depositGap", err, err)
		}
		if gap.class != depositGapStructural || gap.cause != gapCauseBelowUpstreamStart {
			t.Fatalf("gap = %+v, want structural/below_upstream_start", gap)
		}
	})

	t.Run("structural_asset_not_indexed", func(t *testing.T) {
		cfg := depositITConfig(t, 9, testContractA)
		// The second asset is effective inside the unit but the upstream
		// whitelist (env recomputation) never indexed it.
		cfg.Assets = append(cfg.Assets, config.DepositEntry{Address: testContractB, Effective: 10})
		depositSeedCanonical(t, ctx, pool, cfg.ChainID, 10, 20, true)
		depositSeedUpstream(t, ctx, pool, cfg.ChainID, 0, cfg.LogConfigHash, 21)
		sc := depositITScanner(t, pool, cfg)
		_, err := sc.readCoveredUnit(ctx, pool, 10, 20)
		var gap *depositGap
		if !errors.As(err, &gap) {
			t.Fatalf("error = %v (%T), want *depositGap", err, err)
		}
		if gap.class != depositGapStructural || gap.cause != gapCauseAssetNotIndexed {
			t.Fatalf("gap = %+v, want structural/asset_not_indexed", gap)
		}
	})
}

// TestDepositPauseStreamsBlockProof: each of the three pause streams blocks
// the proof on its own (FR-12; no commit may cross an active pause).
func TestDepositPauseStreamsBlockProof(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	tests := []struct {
		name string
		seed func(chainID int64)
	}{
		{"deposit_pause", func(chainID int64) {
			if _, err := pool.Exec(ctx, `
INSERT INTO deposit_pause (chain_id, height, kind, detail) VALUES ($1, 10, 'upstream_gap', 'test')`,
				chainID); err != nil {
				t.Fatalf("seed deposit_pause: %v", err)
			}
		}},
		{"log_pause", func(chainID int64) {
			if _, err := pool.Exec(ctx, `
INSERT INTO log_pause (chain_id, height, kind, detail) VALUES ($1, 10, 'chain_view_changed', 'test')`,
				chainID); err != nil {
				t.Fatalf("seed log_pause: %v", err)
			}
		}},
		{"indexer_pause", func(chainID int64) {
			if _, err := pool.Exec(ctx, `
INSERT INTO indexer_pause (chain_id, height, expected_hash, actual_hash, kind, detail)
VALUES ($1, 10, $2, $2, 'hash_mismatch', 'test')`,
				chainID, depositBlockHash(10)); err != nil {
				t.Fatalf("seed indexer_pause: %v", err)
			}
		}},
	}
	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := depositITConfig(t, int64(7+i), testContractA)
			depositSeedCanonical(t, ctx, pool, cfg.ChainID, 10, 20, true)
			depositSeedUpstream(t, ctx, pool, cfg.ChainID, 0, cfg.LogConfigHash, 21)
			tc.seed(cfg.ChainID)

			sc := depositITScanner(t, pool, cfg)
			_, err := sc.readCoveredUnit(ctx, pool, 10, 20)
			var paused *streamPauseError
			if !errors.As(err, &paused) {
				t.Fatalf("error = %v (%T), want *streamPauseError", err, err)
			}
			if paused.stream != tc.name {
				t.Fatalf("paused stream = %q, want %q", paused.stream, tc.name)
			}
		})
	}
}

// TestDepositCanonicalProofFails: a missing, non-canonical or hash-mismatched
// referenced block stops the proof (I4; the watermark alone is never enough).
func TestDepositCanonicalProofFails(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	t.Run("missing_height", func(t *testing.T) {
		cfg := depositITConfig(t, 7, testContractA)
		depositSeedCanonical(t, ctx, pool, cfg.ChainID, 10, 19, true)
		depositSeedUpstream(t, ctx, pool, cfg.ChainID, 0, cfg.LogConfigHash, 21)
		sc := depositITScanner(t, pool, cfg)
		_, err := sc.readCoveredUnit(ctx, pool, 10, 20)
		var cv *chainViewError
		if !errors.As(err, &cv) {
			t.Fatalf("error = %v (%T), want *chainViewError", err, err)
		}
		if !cv.absent || cv.height != 20 {
			t.Fatalf("chainViewError = %+v, want absent at height 20", cv)
		}
	})

	t.Run("non_canonical_height", func(t *testing.T) {
		cfg := depositITConfig(t, 8, testContractA)
		depositSeedCanonical(t, ctx, pool, cfg.ChainID, 10, 20, true)
		if _, err := pool.Exec(ctx,
			`UPDATE chain_blocks SET canonical = FALSE WHERE chain_id = $1 AND number = 15`,
			cfg.ChainID); err != nil {
			t.Fatalf("flip canonical: %v", err)
		}
		depositSeedUpstream(t, ctx, pool, cfg.ChainID, 0, cfg.LogConfigHash, 21)
		sc := depositITScanner(t, pool, cfg)
		_, err := sc.readCoveredUnit(ctx, pool, 10, 20)
		var cv *chainViewError
		if !errors.As(err, &cv) {
			t.Fatalf("error = %v (%T), want *chainViewError", err, err)
		}
		if !cv.absent || cv.height != 15 {
			t.Fatalf("chainViewError = %+v, want absent at height 15", cv)
		}
	})

	t.Run("source_hash_mismatch", func(t *testing.T) {
		cfg := depositITConfig(t, 9, testContractA)
		depositSeedCanonical(t, ctx, pool, cfg.ChainID, 10, 20, true)
		depositSeedUpstream(t, ctx, pool, cfg.ChainID, 0, cfg.LogConfigHash, 21)
		depositSeedSourceRow(t, ctx, pool, cfg.ChainID, 15, depositBlockHash(999), depositTxHash(15, 0), 0)
		sc := depositITScanner(t, pool, cfg)
		_, err := sc.readCoveredUnit(ctx, pool, 10, 20)
		var cv *chainViewError
		if !errors.As(err, &cv) {
			t.Fatalf("error = %v (%T), want *chainViewError", err, err)
		}
		if cv.absent || cv.height != 15 ||
			cv.expected != depositBlockHash(15) || cv.actual != depositBlockHash(999) {
			t.Fatalf("chainViewError = %+v, want height 15 expected=%s actual=%s",
				cv, depositBlockHash(15), depositBlockHash(999))
		}
	})
}

// TestDepositEmptyIntervalProof: zero source rows over a covered interval is a
// legal empty interval — the proof passes and returns no rows (never "no
// coverage").
func TestDepositEmptyIntervalProof(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	cfg := depositITConfig(t, 7, testContractA)
	depositSeedCanonical(t, ctx, pool, cfg.ChainID, 10, 20, true)
	depositSeedUpstream(t, ctx, pool, cfg.ChainID, 0, cfg.LogConfigHash, 21)

	sc := depositITScanner(t, pool, cfg)
	unit, err := sc.readCoveredUnit(ctx, pool, 10, 20)
	if err != nil {
		t.Fatalf("readCoveredUnit() error = %v, want nil for a covered empty interval", err)
	}
	if len(unit.rows) != 0 {
		t.Fatalf("rows = %d, want 0", len(unit.rows))
	}
	if len(unit.canonical) != 11 {
		t.Fatalf("canonical = %d entries, want 11", len(unit.canonical))
	}
}

// TestDepositProgressIntegrityIntegration: checkpoint/history must exist on
// both sides with matching (start_block, config_hash); a one-sided or
// mismatched state is corruption and is never repaired.
func TestDepositProgressIntegrityIntegration(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	hashA := strings.Repeat("aa", 32)

	t.Run("both_absent_is_empty_progress", func(t *testing.T) {
		cfg := depositITConfig(t, 7, testContractA)
		sc := depositITScanner(t, pool, cfg)
		progress, err := sc.readProgress(ctx, pool)
		if err != nil || progress != nil {
			t.Fatalf("readProgress() = %+v, %v; want nil, nil", progress, err)
		}
	})

	t.Run("checkpoint_without_history", func(t *testing.T) {
		cfg := depositITConfig(t, 8, testContractA)
		depositSeedCheckpoint(t, ctx, pool, cfg.ChainID, 0, hashA, 10)
		sc := depositITScanner(t, pool, cfg)
		_, err := sc.readProgress(ctx, pool)
		var corrupt *depositCorruptStateError
		if !errors.As(err, &corrupt) {
			t.Fatalf("error = %v (%T), want *depositCorruptStateError", err, err)
		}
	})

	t.Run("history_without_checkpoint", func(t *testing.T) {
		cfg := depositITConfig(t, 9, testContractA)
		depositSeedHistory(t, ctx, pool, cfg.ChainID, 1, 0, hashA)
		sc := depositITScanner(t, pool, cfg)
		_, err := sc.readProgress(ctx, pool)
		var corrupt *depositCorruptStateError
		if !errors.As(err, &corrupt) {
			t.Fatalf("error = %v (%T), want *depositCorruptStateError", err, err)
		}
	})

	t.Run("start_block_disagrees", func(t *testing.T) {
		cfg := depositITConfig(t, 10, testContractA)
		depositSeedCheckpoint(t, ctx, pool, cfg.ChainID, 0, hashA, 10)
		depositSeedHistory(t, ctx, pool, cfg.ChainID, 1, 5, hashA)
		sc := depositITScanner(t, pool, cfg)
		_, err := sc.readProgress(ctx, pool)
		var corrupt *depositCorruptStateError
		if !errors.As(err, &corrupt) {
			t.Fatalf("error = %v (%T), want *depositCorruptStateError", err, err)
		}
	})

	t.Run("consistent_reads_progress_and_version", func(t *testing.T) {
		cfg := depositITConfig(t, 11, testContractA)
		depositSeedCheckpoint(t, ctx, pool, cfg.ChainID, 0, hashA, 10)
		depositSeedHistory(t, ctx, pool, cfg.ChainID, 4, 0, hashA)
		sc := depositITScanner(t, pool, cfg)
		progress, err := sc.readProgress(ctx, pool)
		if err != nil {
			t.Fatalf("readProgress() error = %v", err)
		}
		want := depositProgress{startBlock: 0, configHash: hashA, nextBlock: 10, versionSeq: 4}
		if progress == nil || *progress != want {
			t.Fatalf("progress = %+v, want %+v", progress, want)
		}
	})
}

// --- T006: unit atomic commit ----------------------------------------------

// depositITLease acquires a real lease for the chain so the commit's
// owner/token/expiry verdict has a real row to adjudicate.
func depositITLease(t *testing.T, pool *pgxpool.Pool, chainID int64) *Lease {
	t.Helper()
	l := newTestLease(t, pool, chainID, fmt.Sprintf("deposit-it-%d", chainID), time.Minute, 10*time.Second)
	won, token, err := l.Acquire(context.Background())
	if err != nil || !won {
		t.Fatalf("acquire deposit test lease: won=%v token=%d err=%v", won, token, err)
	}
	return l
}

// depositSeedTransferRow plants one stored Transfer row with full control over
// contract, sender, recipient and amount (0 = zero-value, non-watch = nomatch).
func depositSeedTransferRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64,
	block uint64, blockHash, txHash string, logIndex uint64, contract, sender, recipient common.Address, amount *big.Int) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
INSERT INTO erc20_transfer_logs
    (chain_id, block_number, block_hash, tx_hash, log_index, contract, topic0, topic1, topic2, data)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		chainID, int64(block), blockHash, txHash, int64(logIndex),
		strings.ToLower(contract.Hex()), eth.TransferSig.Hex(),
		common.BytesToHash(sender.Bytes()).Hex(), common.BytesToHash(recipient.Bytes()).Hex(),
		common.BigToHash(amount).Hex()); err != nil {
		t.Fatalf("seed erc20_transfer_logs: %v", err)
	}
}

// depositSeedHistoryVersion plants one non-bootstrap history version row
// (request_id non-null); the same hash may repeat across seqs (H1 loopback).
func depositSeedHistoryVersion(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64,
	seq, prevSeq int64, start uint64, hash, requestID string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
INSERT INTO deposit_config_history
    (chain_id, version_seq, config_hash, prev_seq, start_block, assets, watches, replay_from, operator, request_id)
VALUES ($1, $2, $3, $4, $5, $6, $7, $5, 'operator', $8)`,
		chainID, seq, hash, prevSeq, int64(start),
		testContractA+":10", depositWatchAddr+":10", requestID); err != nil {
		t.Fatalf("seed deposit_config_history version %d: %v", seq, err)
	}
}

// depositSeedObservation plants one durable observation exactly as a commit
// for the depositSeedTransferRow shape would.
func depositSeedObservation(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64,
	block uint64, blockHash, txHash string, logIndex uint64, amount string, versionSeq int64) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
INSERT INTO deposit_observations
    (chain_id, block_hash, tx_hash, log_index, block_number, contract, sender, recipient, amount, version_seq)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		chainID, blockHash, txHash, int64(logIndex), int64(block),
		testContractA, testContractB, depositWatchAddr, amount, versionSeq); err != nil {
		t.Fatalf("seed deposit_observations: %v", err)
	}
}

// depositITPrepareUnit runs the T005 read plus the T004 parse for [a,b]. The
// progress read is included so callers get the captured version basis.
func depositITPrepareUnit(t *testing.T, ctx context.Context, sc *DepositScanner, a, b uint64) (*depositUnit, depositBatch, *depositProgress) {
	t.Helper()
	progress, err := sc.readProgress(ctx, sc.pool)
	if err != nil {
		t.Fatalf("readProgress(): %v", err)
	}
	unit, batch := depositITReadUnit(t, ctx, sc, a, b)
	return unit, batch, progress
}

// depositITReadUnit reads and parses the unit only; the corrupt-state tests
// need it while the durable progress read itself is the expected failure.
func depositITReadUnit(t *testing.T, ctx context.Context, sc *DepositScanner, a, b uint64) (*depositUnit, depositBatch) {
	t.Helper()
	unit, err := sc.readCoveredUnit(ctx, sc.pool, a, b)
	if err != nil {
		t.Fatalf("readCoveredUnit(%d,%d): %v", a, b, err)
	}
	batch, err := parseDepositLogs(sc.matchConfig(), unit.rows)
	if err != nil {
		t.Fatalf("parseDepositLogs(): %v", err)
	}
	return unit, batch
}

func depositCountRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, table string, chainID int64) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+table+" WHERE chain_id = $1", chainID).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

func depositCheckpointState(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64) (start uint64, hash string, next uint64, ok bool) {
	t.Helper()
	var s, n int64
	err := pool.QueryRow(ctx, `SELECT start_block, config_hash, next_block FROM deposit_checkpoint WHERE chain_id = $1`, chainID).Scan(&s, &hash, &n)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, "", 0, false
	}
	if err != nil {
		t.Fatalf("read deposit_checkpoint: %v", err)
	}
	return uint64(s), hash, uint64(n), true
}

// TestDepositCommitFirstUnitAtomic: a first unit creates the checkpoint and
// the version_seq=1 bootstrap history row (request_id NULL, operator
// bootstrap) in one transaction, writes the matched observation with the
// captured version and accounts for zero/nomatch rows without rows.
func TestDepositCommitFirstUnitAtomic(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	cfg := depositITConfig(t, 30, testContractA)
	depositSeedCanonical(t, ctx, pool, cfg.ChainID, 10, 20, true)
	depositSeedUpstream(t, ctx, pool, cfg.ChainID, 0, cfg.LogConfigHash, 21)
	// One match, one zero value, one non-matching recipient.
	depositSeedTransferRow(t, ctx, pool, cfg.ChainID, 12, depositBlockHash(12), depositTxHash(12, 0), 0,
		common.HexToAddress(testContractA), common.HexToAddress(testContractB), common.HexToAddress(depositWatchAddr), big.NewInt(1))
	depositSeedTransferRow(t, ctx, pool, cfg.ChainID, 13, depositBlockHash(13), depositTxHash(13, 0), 0,
		common.HexToAddress(testContractA), common.HexToAddress(testContractB), common.HexToAddress(depositWatchAddr), big.NewInt(0))
	depositSeedTransferRow(t, ctx, pool, cfg.ChainID, 14, depositBlockHash(14), depositTxHash(14, 0), 0,
		common.HexToAddress(testContractA), common.HexToAddress(testContractB), common.HexToAddress(depositOtherAddr), big.NewInt(1))

	sc := depositITScanner(t, pool, cfg)
	lease := depositITLease(t, pool, cfg.ChainID)
	unit, batch, progress := depositITPrepareUnit(t, ctx, sc, 10, 20)
	if progress != nil {
		t.Fatalf("progress = %+v, want empty (first unit)", progress)
	}
	if len(batch.matched) != 1 || batch.zero != 1 || batch.nomatch != 1 {
		t.Fatalf("batch = matched %d zero %d nomatch %d, want 1/1/1", len(batch.matched), batch.zero, batch.nomatch)
	}
	if err := sc.commitDepositUnit(ctx, lease, unit, batch, progress, 10, 20); err != nil {
		t.Fatalf("commitDepositUnit(): %v", err)
	}

	start, hash, next, ok := depositCheckpointState(t, ctx, pool, cfg.ChainID)
	if !ok || start != 10 || hash != cfg.ConfigHash || next != 21 {
		t.Fatalf("checkpoint = (%d,%s,%d,%v), want (10,%s,21,true)", start, hash, next, ok, cfg.ConfigHash)
	}

	var (
		versionSeq, hStart, replayFrom int64
		hHash, assets, watches         string
		prevSeq                        *int64
		operator                       string
		requestID                      *string
	)
	err := pool.QueryRow(ctx, `
SELECT version_seq, config_hash, prev_seq, start_block, assets, watches, replay_from, operator, request_id
FROM deposit_config_history WHERE chain_id = $1`, cfg.ChainID).
		Scan(&versionSeq, &hHash, &prevSeq, &hStart, &assets, &watches, &replayFrom, &operator, &requestID)
	if err != nil {
		t.Fatalf("read history row: %v", err)
	}
	if versionSeq != 1 || hHash != cfg.ConfigHash || prevSeq != nil || hStart != 10 ||
		replayFrom != 10 || operator != "bootstrap" || requestID != nil {
		t.Fatalf("history = seq %d hash %s prev %v start %d replay %d operator %q request %v",
			versionSeq, hHash, prevSeq, hStart, replayFrom, operator, requestID)
	}
	if assets != testContractA+":10" || watches != depositWatchAddr+":10" {
		t.Fatalf("history snapshots = assets %q watches %q", assets, watches)
	}
	if n := depositCountRows(t, ctx, pool, "deposit_config_history", cfg.ChainID); n != 1 {
		t.Fatalf("history rows = %d, want 1", n)
	}

	if n := depositCountRows(t, ctx, pool, "deposit_observations", cfg.ChainID); n != 1 {
		t.Fatalf("observations = %d, want exactly 1", n)
	}
	var (
		obsBlockHash, obsTxHash, contract, sender, recipient, amount, status string
		obsLogIndex, obsBlockNumber, obsVersion                              int64
	)
	err = pool.QueryRow(ctx, `
SELECT block_hash, tx_hash, log_index, block_number, contract, sender, recipient, amount::text, status, version_seq
FROM deposit_observations WHERE chain_id = $1`, cfg.ChainID).
		Scan(&obsBlockHash, &obsTxHash, &obsLogIndex, &obsBlockNumber, &contract, &sender, &recipient, &amount, &status, &obsVersion)
	if err != nil {
		t.Fatalf("read observation: %v", err)
	}
	if obsBlockHash != depositBlockHash(12) || obsTxHash != depositTxHash(12, 0) || obsLogIndex != 0 ||
		obsBlockNumber != 12 || contract != testContractA || sender != testContractB ||
		recipient != depositWatchAddr || amount != "1" || status != "pending" || obsVersion != 1 {
		t.Fatalf("observation = %s/%s/%d block %d contract %s sender %s recipient %s amount %s status %s version %d",
			obsBlockHash, obsTxHash, obsLogIndex, obsBlockNumber, contract, sender, recipient, amount, status, obsVersion)
	}
}

// TestDepositCommitAdvanceExactGuard: a successful advance moves next_block to
// exactly b+1; replaying the same unit with the captured (now stale) basis is
// refused with zero writes.
func TestDepositCommitAdvanceExactGuard(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	cfg := depositITConfig(t, 31, testContractA)
	depositSeedCanonical(t, ctx, pool, cfg.ChainID, 10, 20, true)
	depositSeedUpstream(t, ctx, pool, cfg.ChainID, 0, cfg.LogConfigHash, 21)
	depositSeedCheckpoint(t, ctx, pool, cfg.ChainID, 10, cfg.ConfigHash, 10)
	depositSeedHistory(t, ctx, pool, cfg.ChainID, 1, 10, cfg.ConfigHash)
	depositSeedTransferRow(t, ctx, pool, cfg.ChainID, 15, depositBlockHash(15), depositTxHash(15, 0), 0,
		common.HexToAddress(testContractA), common.HexToAddress(testContractB), common.HexToAddress(depositWatchAddr), big.NewInt(7))

	sc := depositITScanner(t, pool, cfg)
	lease := depositITLease(t, pool, cfg.ChainID)
	unit, batch, captured := depositITPrepareUnit(t, ctx, sc, 10, 20)
	if captured == nil || captured.nextBlock != 10 || captured.versionSeq != 1 {
		t.Fatalf("captured progress = %+v, want next 10 version 1", captured)
	}
	if err := sc.commitDepositUnit(ctx, lease, unit, batch, captured, 10, 20); err != nil {
		t.Fatalf("first advance: %v", err)
	}
	if _, _, next, ok := depositCheckpointState(t, ctx, pool, cfg.ChainID); !ok || next != 21 {
		t.Fatalf("checkpoint next = %d (ok=%v), want exactly 21", next, ok)
	}
	if n := depositCountRows(t, ctx, pool, "deposit_observations", cfg.ChainID); n != 1 {
		t.Fatalf("observations = %d, want 1", n)
	}

	// Replay the same unit with the captured basis: the durable next_block is
	// 21, so the exact guard refuses and nothing is written twice.
	unit2, batch2, _ := depositITPrepareUnit(t, ctx, sc, 10, 20)
	err := sc.commitDepositUnit(ctx, lease, unit2, batch2, captured, 10, 20)
	if !errors.Is(err, errStaleState) {
		t.Fatalf("stale replay = %v, want errStaleState", err)
	}
	if _, _, next, _ := depositCheckpointState(t, ctx, pool, cfg.ChainID); next != 21 {
		t.Fatalf("checkpoint next = %d, want 21 unchanged", next)
	}
	if n := depositCountRows(t, ctx, pool, "deposit_observations", cfg.ChainID); n != 1 {
		t.Fatalf("observations = %d after stale replay, want 1", n)
	}
}

// TestDepositCommitConfigMismatch: the durable (start_block, config_hash) is
// frozen; a scanner whose configuration differs is refused with zero writes.
func TestDepositCommitConfigMismatch(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	cfg := depositITConfig(t, 32, testContractA)
	dbHash := cfg.ConfigHash
	depositSeedCanonical(t, ctx, pool, cfg.ChainID, 10, 20, true)
	depositSeedUpstream(t, ctx, pool, cfg.ChainID, 0, cfg.LogConfigHash, 21)
	depositSeedCheckpoint(t, ctx, pool, cfg.ChainID, 10, dbHash, 10)
	depositSeedHistory(t, ctx, pool, cfg.ChainID, 1, 10, dbHash)
	depositSeedTransferRow(t, ctx, pool, cfg.ChainID, 15, depositBlockHash(15), depositTxHash(15, 0), 0,
		common.HexToAddress(testContractA), common.HexToAddress(testContractB), common.HexToAddress(depositWatchAddr), big.NewInt(1))

	// A changed configuration (new hash) against the frozen durable row.
	cfg.ConfigHash = strings.Repeat("bb", 32)
	sc := depositITScanner(t, pool, cfg)
	lease := depositITLease(t, pool, cfg.ChainID)
	unit, batch, captured := depositITPrepareUnit(t, ctx, sc, 10, 20)
	if captured == nil || captured.configHash != dbHash {
		t.Fatalf("captured = %+v, want hash %s", captured, dbHash)
	}
	err := sc.commitDepositUnit(ctx, lease, unit, batch, captured, 10, 20)
	var mismatch *depositConfigMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("commit = %v (%T), want *depositConfigMismatchError", err, err)
	}
	if _, _, next, _ := depositCheckpointState(t, ctx, pool, cfg.ChainID); next != 10 {
		t.Fatalf("checkpoint next = %d, want 10 unchanged", next)
	}
	if n := depositCountRows(t, ctx, pool, "deposit_observations", cfg.ChainID); n != 0 {
		t.Fatalf("observations = %d, want 0 after config mismatch", n)
	}
}

// TestDepositCommitVersionIsolationLoopback: the captured version_seq must
// still be current under the lock; a newer seq with the SAME content hash (H1
// loopback) is rejected on seq identity and the unit is abandoned untouched.
func TestDepositCommitVersionIsolationLoopback(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	cfg := depositITConfig(t, 33, testContractA)
	depositSeedCanonical(t, ctx, pool, cfg.ChainID, 10, 20, true)
	depositSeedUpstream(t, ctx, pool, cfg.ChainID, 0, cfg.LogConfigHash, 21)
	depositSeedHistory(t, ctx, pool, cfg.ChainID, 1, 10, cfg.ConfigHash)
	depositSeedHistoryVersion(t, ctx, pool, cfg.ChainID, 2, 1, 10, cfg.ConfigHash, "req-2")
	depositSeedCheckpoint(t, ctx, pool, cfg.ChainID, 10, cfg.ConfigHash, 10)
	depositSeedTransferRow(t, ctx, pool, cfg.ChainID, 15, depositBlockHash(15), depositTxHash(15, 0), 0,
		common.HexToAddress(testContractA), common.HexToAddress(testContractB), common.HexToAddress(depositWatchAddr), big.NewInt(1))

	sc := depositITScanner(t, pool, cfg)
	lease := depositITLease(t, pool, cfg.ChainID)
	unit, batch, captured := depositITPrepareUnit(t, ctx, sc, 10, 20)
	if captured == nil || captured.versionSeq != 2 {
		t.Fatalf("captured = %+v, want version_seq 2", captured)
	}

	// The H1 loopback: a newer version with identical content arrives while the
	// unit result is in flight.
	depositSeedHistoryVersion(t, ctx, pool, cfg.ChainID, 3, 2, 10, cfg.ConfigHash, "req-3")
	err := sc.commitDepositUnit(ctx, lease, unit, batch, captured, 10, 20)
	if !errors.Is(err, errDepositVersionMismatch) {
		t.Fatalf("commit = %v, want errDepositVersionMismatch", err)
	}
	if _, _, next, _ := depositCheckpointState(t, ctx, pool, cfg.ChainID); next != 10 {
		t.Fatalf("checkpoint next = %d, want 10 unchanged", next)
	}
	if n := depositCountRows(t, ctx, pool, "deposit_observations", cfg.ChainID); n != 0 {
		t.Fatalf("observations = %d, want 0 after version mismatch", n)
	}
	if n := depositCountRows(t, ctx, pool, "deposit_config_history", cfg.ChainID); n != 3 {
		t.Fatalf("history rows = %d, want 3", n)
	}
}

// TestDepositCommitReplayConvergesOriginalVersion: a replayed identity with
// identical content converges without rewriting, keeps its original
// version_seq, and a genuinely new observation is written with the captured
// version (never the latest at commit).
func TestDepositCommitReplayConvergesOriginalVersion(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	cfg := depositITConfig(t, 34, testContractA)
	depositSeedCanonical(t, ctx, pool, cfg.ChainID, 10, 20, true)
	depositSeedUpstream(t, ctx, pool, cfg.ChainID, 0, cfg.LogConfigHash, 21)
	depositSeedHistory(t, ctx, pool, cfg.ChainID, 1, 10, cfg.ConfigHash)
	depositSeedHistoryVersion(t, ctx, pool, cfg.ChainID, 2, 1, 10, cfg.ConfigHash, "req-2")
	depositSeedCheckpoint(t, ctx, pool, cfg.ChainID, 10, cfg.ConfigHash, 10)
	// Identity I already exists with the same content but the original version
	// (a replay under a newer captured version must not rewrite it).
	depositSeedTransferRow(t, ctx, pool, cfg.ChainID, 15, depositBlockHash(15), depositTxHash(15, 0), 0,
		common.HexToAddress(testContractA), common.HexToAddress(testContractB), common.HexToAddress(depositWatchAddr), big.NewInt(1))
	depositSeedObservation(t, ctx, pool, cfg.ChainID, 15, depositBlockHash(15), depositTxHash(15, 0), 0, "1", 1)
	// Identity J is new in this unit.
	depositSeedTransferRow(t, ctx, pool, cfg.ChainID, 16, depositBlockHash(16), depositTxHash(16, 0), 0,
		common.HexToAddress(testContractA), common.HexToAddress(testContractB), common.HexToAddress(depositWatchAddr), big.NewInt(2))

	sc := depositITScanner(t, pool, cfg)
	lease := depositITLease(t, pool, cfg.ChainID)
	unit, batch, captured := depositITPrepareUnit(t, ctx, sc, 10, 20)
	if captured == nil || captured.versionSeq != 2 {
		t.Fatalf("captured = %+v, want version_seq 2", captured)
	}
	if err := sc.commitDepositUnit(ctx, lease, unit, batch, captured, 10, 20); err != nil {
		t.Fatalf("commitDepositUnit(): %v", err)
	}

	if n := depositCountRows(t, ctx, pool, "deposit_observations", cfg.ChainID); n != 2 {
		t.Fatalf("observations = %d, want 2", n)
	}
	var (
		versionI, versionJ int64
		amountI, amountJ   string
	)
	if err := pool.QueryRow(ctx, `SELECT amount::text, version_seq FROM deposit_observations
WHERE chain_id = $1 AND block_hash = $2 AND tx_hash = $3 AND log_index = $4`,
		cfg.ChainID, depositBlockHash(15), depositTxHash(15, 0), 0).Scan(&amountI, &versionI); err != nil {
		t.Fatalf("read identity I: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT amount::text, version_seq FROM deposit_observations
WHERE chain_id = $1 AND block_hash = $2 AND tx_hash = $3 AND log_index = $4`,
		cfg.ChainID, depositBlockHash(16), depositTxHash(16, 0), 0).Scan(&amountJ, &versionJ); err != nil {
		t.Fatalf("read identity J: %v", err)
	}
	if amountI != "1" || versionI != 1 {
		t.Fatalf("replayed identity = amount %s version %d, want amount 1 version 1 (original preserved)", amountI, versionI)
	}
	if amountJ != "2" || versionJ != 2 {
		t.Fatalf("new identity = amount %s version %d, want amount 2 version 2 (captured version)", amountJ, versionJ)
	}
	if _, _, next, _ := depositCheckpointState(t, ctx, pool, cfg.ChainID); next != 21 {
		t.Fatalf("checkpoint next = %d, want 21", next)
	}
}

// TestDepositCommitConflictRollsBackWholeBatch: an existing identity with
// different content fails the whole batch (identity_conflict), the existing
// row is untouched and a new observation inserted earlier in the same
// transaction is rolled back.
func TestDepositCommitConflictRollsBackWholeBatch(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	cfg := depositITConfig(t, 35, testContractA)
	depositSeedCanonical(t, ctx, pool, cfg.ChainID, 10, 20, true)
	depositSeedUpstream(t, ctx, pool, cfg.ChainID, 0, cfg.LogConfigHash, 21)
	depositSeedHistory(t, ctx, pool, cfg.ChainID, 1, 10, cfg.ConfigHash)
	depositSeedCheckpoint(t, ctx, pool, cfg.ChainID, 10, cfg.ConfigHash, 10)
	// Identity I: source amount 1, stored amount 2 → conflict.
	depositSeedTransferRow(t, ctx, pool, cfg.ChainID, 15, depositBlockHash(15), depositTxHash(15, 0), 0,
		common.HexToAddress(testContractA), common.HexToAddress(testContractB), common.HexToAddress(depositWatchAddr), big.NewInt(1))
	depositSeedObservation(t, ctx, pool, cfg.ChainID, 15, depositBlockHash(15), depositTxHash(15, 0), 0, "2", 1)
	// Identity J is new and would be inserted first in source order (block 14).
	depositSeedTransferRow(t, ctx, pool, cfg.ChainID, 14, depositBlockHash(14), depositTxHash(14, 0), 0,
		common.HexToAddress(testContractA), common.HexToAddress(testContractB), common.HexToAddress(depositWatchAddr), big.NewInt(3))

	sc := depositITScanner(t, pool, cfg)
	lease := depositITLease(t, pool, cfg.ChainID)
	unit, batch, captured := depositITPrepareUnit(t, ctx, sc, 10, 20)
	if len(batch.matched) != 2 {
		t.Fatalf("matched = %d, want 2", len(batch.matched))
	}
	err := sc.commitDepositUnit(ctx, lease, unit, batch, captured, 10, 20)
	var conflict *depositIdentityConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("commit = %v (%T), want *depositIdentityConflictError", err, err)
	}

	if n := depositCountRows(t, ctx, pool, "deposit_observations", cfg.ChainID); n != 1 {
		t.Fatalf("observations = %d, want 1 (new row rolled back)", n)
	}
	var amount string
	var version int64
	if err := pool.QueryRow(ctx, `SELECT amount::text, version_seq FROM deposit_observations
WHERE chain_id = $1 AND block_hash = $2 AND tx_hash = $3 AND log_index = $4`,
		cfg.ChainID, depositBlockHash(15), depositTxHash(15, 0), 0).Scan(&amount, &version); err != nil {
		t.Fatalf("read identity I: %v", err)
	}
	if amount != "2" || version != 1 {
		t.Fatalf("existing identity = amount %s version %d, want untouched amount 2 version 1", amount, version)
	}
	if _, _, next, _ := depositCheckpointState(t, ctx, pool, cfg.ChainID); next != 10 {
		t.Fatalf("checkpoint next = %d, want 10 (failed batch must not advance)", next)
	}
}

// TestDepositCommitCorruptOneSidedState: checkpoint and history must exist on
// both sides or neither; the commit refuses a one-sided state and never
// bootstraps over it.
func TestDepositCommitCorruptOneSidedState(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	t.Run("checkpoint_without_history", func(t *testing.T) {
		cfg := depositITConfig(t, 36, testContractA)
		depositSeedCanonical(t, ctx, pool, cfg.ChainID, 10, 20, true)
		depositSeedUpstream(t, ctx, pool, cfg.ChainID, 0, cfg.LogConfigHash, 21)
		depositSeedCheckpoint(t, ctx, pool, cfg.ChainID, 10, cfg.ConfigHash, 10)
		depositSeedTransferRow(t, ctx, pool, cfg.ChainID, 15, depositBlockHash(15), depositTxHash(15, 0), 0,
			common.HexToAddress(testContractA), common.HexToAddress(testContractB), common.HexToAddress(depositWatchAddr), big.NewInt(1))

		sc := depositITScanner(t, pool, cfg)
		lease := depositITLease(t, pool, cfg.ChainID)
		unit, batch := depositITReadUnit(t, ctx, sc, 10, 20)
		err := sc.commitDepositUnit(ctx, lease, unit, batch, nil, 10, 20)
		var corrupt *depositCorruptStateError
		if !errors.As(err, &corrupt) {
			t.Fatalf("commit = %v (%T), want *depositCorruptStateError", err, err)
		}
		if n := depositCountRows(t, ctx, pool, "deposit_config_history", cfg.ChainID); n != 0 {
			t.Fatalf("history rows = %d, want 0 (never bootstrapped over corruption)", n)
		}
		if _, _, next, _ := depositCheckpointState(t, ctx, pool, cfg.ChainID); next != 10 {
			t.Fatalf("checkpoint next = %d, want 10 unchanged", next)
		}
		if n := depositCountRows(t, ctx, pool, "deposit_observations", cfg.ChainID); n != 0 {
			t.Fatalf("observations = %d, want 0", n)
		}
	})

	t.Run("history_without_checkpoint", func(t *testing.T) {
		cfg := depositITConfig(t, 37, testContractA)
		depositSeedCanonical(t, ctx, pool, cfg.ChainID, 10, 20, true)
		depositSeedUpstream(t, ctx, pool, cfg.ChainID, 0, cfg.LogConfigHash, 21)
		depositSeedHistory(t, ctx, pool, cfg.ChainID, 1, 10, cfg.ConfigHash)
		depositSeedTransferRow(t, ctx, pool, cfg.ChainID, 15, depositBlockHash(15), depositTxHash(15, 0), 0,
			common.HexToAddress(testContractA), common.HexToAddress(testContractB), common.HexToAddress(depositWatchAddr), big.NewInt(1))

		sc := depositITScanner(t, pool, cfg)
		lease := depositITLease(t, pool, cfg.ChainID)
		unit, batch := depositITReadUnit(t, ctx, sc, 10, 20)
		err := sc.commitDepositUnit(ctx, lease, unit, batch, nil, 10, 20)
		var corrupt *depositCorruptStateError
		if !errors.As(err, &corrupt) {
			t.Fatalf("commit = %v (%T), want *depositCorruptStateError", err, err)
		}
		if _, _, _, ok := depositCheckpointState(t, ctx, pool, cfg.ChainID); ok {
			t.Fatal("checkpoint row created over corruption, want none")
		}
		if n := depositCountRows(t, ctx, pool, "deposit_observations", cfg.ChainID); n != 0 {
			t.Fatalf("observations = %d, want 0", n)
		}
	})
}

// TestDepositCommitCanonicalRevertAborts: a canonical flip between the read
// and the lock stops the commit with a chain-view divergence and zero writes.
func TestDepositCommitCanonicalRevertAborts(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	cfg := depositITConfig(t, 38, testContractA)
	depositSeedCanonical(t, ctx, pool, cfg.ChainID, 10, 20, true)
	depositSeedUpstream(t, ctx, pool, cfg.ChainID, 0, cfg.LogConfigHash, 21)
	depositSeedHistory(t, ctx, pool, cfg.ChainID, 1, 10, cfg.ConfigHash)
	depositSeedCheckpoint(t, ctx, pool, cfg.ChainID, 10, cfg.ConfigHash, 10)
	depositSeedTransferRow(t, ctx, pool, cfg.ChainID, 15, depositBlockHash(15), depositTxHash(15, 0), 0,
		common.HexToAddress(testContractA), common.HexToAddress(testContractB), common.HexToAddress(depositWatchAddr), big.NewInt(1))

	sc := depositITScanner(t, pool, cfg)
	lease := depositITLease(t, pool, cfg.ChainID)
	unit, batch, captured := depositITPrepareUnit(t, ctx, sc, 10, 20)
	if _, err := pool.Exec(ctx, `UPDATE chain_blocks SET canonical = FALSE WHERE chain_id = $1 AND number = 15`, cfg.ChainID); err != nil {
		t.Fatalf("flip canonical: %v", err)
	}
	err := sc.commitDepositUnit(ctx, lease, unit, batch, captured, 10, 20)
	var cv *chainViewError
	if !errors.As(err, &cv) {
		t.Fatalf("commit = %v (%T), want *chainViewError", err, err)
	}
	if !cv.absent || cv.height != 15 {
		t.Fatalf("chainViewError = %+v, want absent at height 15", cv)
	}
	if n := depositCountRows(t, ctx, pool, "deposit_observations", cfg.ChainID); n != 0 {
		t.Fatalf("observations = %d, want 0", n)
	}
	if _, _, next, _ := depositCheckpointState(t, ctx, pool, cfg.ChainID); next != 10 {
		t.Fatalf("checkpoint next = %d, want 10 unchanged", next)
	}
}

// TestDepositCommitEmptyIntervalAdvances: a covered interval with zero source
// rows is legal and advances to exactly b+1 with no observation.
func TestDepositCommitEmptyIntervalAdvances(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	cfg := depositITConfig(t, 39, testContractA)
	depositSeedCanonical(t, ctx, pool, cfg.ChainID, 10, 20, true)
	depositSeedUpstream(t, ctx, pool, cfg.ChainID, 0, cfg.LogConfigHash, 21)
	depositSeedHistory(t, ctx, pool, cfg.ChainID, 1, 10, cfg.ConfigHash)
	depositSeedCheckpoint(t, ctx, pool, cfg.ChainID, 10, cfg.ConfigHash, 10)

	sc := depositITScanner(t, pool, cfg)
	lease := depositITLease(t, pool, cfg.ChainID)
	unit, batch, captured := depositITPrepareUnit(t, ctx, sc, 10, 20)
	if len(unit.rows) != 0 || len(batch.matched) != 0 {
		t.Fatalf("empty interval returned %d rows / %d matched", len(unit.rows), len(batch.matched))
	}
	if err := sc.commitDepositUnit(ctx, lease, unit, batch, captured, 10, 20); err != nil {
		t.Fatalf("commitDepositUnit(): %v", err)
	}
	if _, _, next, _ := depositCheckpointState(t, ctx, pool, cfg.ChainID); next != 21 {
		t.Fatalf("checkpoint next = %d, want exactly 21", next)
	}
	if n := depositCountRows(t, ctx, pool, "deposit_observations", cfg.ChainID); n != 0 {
		t.Fatalf("observations = %d, want 0", n)
	}
}

// TestDepositCommitUnknownOutcomeRereadsDB: the COMMIT reply is dropped after
// PostgreSQL accepted it; the commit re-reads the durable progress and
// continues as committed, and a stale replay is still refused.
func TestDepositCommitUnknownOutcomeRereadsDB(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool, drop := logscanOpenCommitDropPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	cfg := depositITConfig(t, 40, testContractA)
	depositSeedCanonical(t, ctx, pool, cfg.ChainID, 10, 20, true)
	depositSeedUpstream(t, ctx, pool, cfg.ChainID, 0, cfg.LogConfigHash, 21)
	depositSeedTransferRow(t, ctx, pool, cfg.ChainID, 12, depositBlockHash(12), depositTxHash(12, 0), 0,
		common.HexToAddress(testContractA), common.HexToAddress(testContractB), common.HexToAddress(depositWatchAddr), big.NewInt(1))

	sc := depositITScanner(t, pool, cfg)
	lease := depositITLease(t, pool, cfg.ChainID)
	unit, batch, captured := depositITPrepareUnit(t, ctx, sc, 10, 20)
	if captured != nil {
		t.Fatalf("captured = %+v, want empty progress", captured)
	}
	drop.arm.Store(true)
	if err := sc.commitDepositUnit(ctx, lease, unit, batch, captured, 10, 20); err != nil {
		t.Fatalf("uncertain commit = %v, want nil after the durable re-read", err)
	}
	if got := drop.dropped.Load(); got != 1 {
		t.Fatalf("commit drops = %d, want exactly 1 (fault injection did not fire)", got)
	}
	if _, _, next, ok := depositCheckpointState(t, ctx, pool, cfg.ChainID); !ok || next != 21 {
		t.Fatalf("checkpoint next = %d (ok=%v), want 21 after the lost reply", next, ok)
	}
	if n := depositCountRows(t, ctx, pool, "deposit_observations", cfg.ChainID); n != 1 {
		t.Fatalf("observations = %d, want exactly 1 (no partial commit)", n)
	}

	// The old captured basis is now behind the durable state; a replay is
	// abandoned (version isolation), never committed twice.
	unit2, batch2, _ := depositITPrepareUnit(t, ctx, sc, 10, 20)
	err := sc.commitDepositUnit(ctx, lease, unit2, batch2, captured, 10, 20)
	if !errors.Is(err, errDepositVersionMismatch) {
		t.Fatalf("stale replay after uncertain commit = %v, want errDepositVersionMismatch", err)
	}
	if n := depositCountRows(t, ctx, pool, "deposit_observations", cfg.ChainID); n != 1 {
		t.Fatalf("observations = %d after replay, want 1", n)
	}
}

// TestDepositServeLoopAdvancesAndWaits: the T006 loop skeleton bootstraps,
// advances a full and an empty unit to exactly b+1 and then waits on the
// upstream watermark without advancing, until cancelled.
func TestDepositServeLoopAdvancesAndWaits(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	cfg := depositITConfig(t, 41, testContractA)
	cfg.BatchBlocks = 11
	cfg.PollInterval = 20 * time.Millisecond
	cfg.RetryInitial = 20 * time.Millisecond
	cfg.RetryMax = 100 * time.Millisecond
	depositSeedCanonical(t, ctx, pool, cfg.ChainID, 10, 31, true)
	depositSeedUpstream(t, ctx, pool, cfg.ChainID, 0, cfg.LogConfigHash, 32)
	depositSeedTransferRow(t, ctx, pool, cfg.ChainID, 12, depositBlockHash(12), depositTxHash(12, 0), 0,
		common.HexToAddress(testContractA), common.HexToAddress(testContractB), common.HexToAddress(depositWatchAddr), big.NewInt(1))

	sc := depositITScanner(t, pool, cfg)
	lease := depositITLease(t, pool, cfg.ChainID)
	loopCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- sc.ServeLoop(loopCtx, lease, nil) }()

	// Unit [10,20] bootstraps to 21, unit [21,31] advances to 32, then a=32
	// with upstream next=32 is a transient gap: the loop waits at 32.
	waitUntil(t, time.Now().Add(10*time.Second), "deposit next_block to reach 32", func() bool {
		_, _, next, ok := depositCheckpointState(t, ctx, pool, cfg.ChainID)
		return ok && next == 32
	})
	time.Sleep(100 * time.Millisecond) // let any erroneous extra advance surface
	if _, _, next, _ := depositCheckpointState(t, ctx, pool, cfg.ChainID); next != 32 {
		t.Fatalf("checkpoint next = %d, want 32 (must wait, not advance past coverage)", next)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("ServeLoop() = %v, want nil on cancellation", err)
	}
	if n := depositCountRows(t, ctx, pool, "deposit_observations", cfg.ChainID); n != 1 {
		t.Fatalf("observations = %d, want 1", n)
	}
	if n := depositCountRows(t, ctx, pool, "deposit_config_history", cfg.ChainID); n != 1 {
		t.Fatalf("history rows = %d, want 1 bootstrap row", n)
	}
	var operator string
	var requestID *string
	if err := pool.QueryRow(ctx, `SELECT operator, request_id FROM deposit_config_history WHERE chain_id = $1`, cfg.ChainID).
		Scan(&operator, &requestID); err != nil {
		t.Fatalf("read bootstrap history: %v", err)
	}
	if operator != "bootstrap" || requestID != nil {
		t.Fatalf("bootstrap history = operator %q request %v", operator, requestID)
	}
}

// depositRunLoop starts ServeLoop in a goroutine and returns a stop function
// that cancels it, joins it and fails the test if the loop returned an error
// instead of stopping on cancellation. It is registered as cleanup too.
func depositRunLoop(t *testing.T, parent context.Context, sc *DepositScanner, lease *Lease) func() {
	t.Helper()
	loopCtx, cancel := context.WithCancel(parent)
	done := make(chan error, 1)
	go func() { done <- sc.ServeLoop(loopCtx, lease, nil) }()
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("ServeLoop() = %v, want nil on cancellation", err)
			}
		case <-time.After(5 * time.Second):
			t.Errorf("ServeLoop did not stop after cancellation")
		}
	}
	t.Cleanup(stop)
	return stop
}

// TestDepositServeLoopTrimsUnitToWatermark covers the batch-as-upper-bound
// sizing (research R7/R5): the batch size never delays available work, the
// unit never runs past N_u-1, and only a watermark at or below the position
// waits with zero advance.
func TestDepositServeLoopTrimsUnitToWatermark(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	t.Run("V1_full_batch_only_one_covered_block", func(t *testing.T) {
		cfg := depositITConfig(t, 50, testContractA) // batch 500
		depositSeedCanonical(t, ctx, pool, cfg.ChainID, 10, 10, true)
		depositSeedUpstream(t, ctx, pool, cfg.ChainID, 0, cfg.LogConfigHash, 11)
		depositSeedTransferRow(t, ctx, pool, cfg.ChainID, 10, depositBlockHash(10), depositTxHash(10, 0), 0,
			common.HexToAddress(testContractA), common.HexToAddress(testContractB), common.HexToAddress(depositWatchAddr), big.NewInt(1))

		sc := depositITScanner(t, pool, cfg)
		stop := depositRunLoop(t, ctx, sc, depositITLease(t, pool, cfg.ChainID))
		waitUntil(t, time.Now().Add(10*time.Second), "V1 next_block 11", func() bool {
			_, _, next, ok := depositCheckpointState(t, ctx, pool, cfg.ChainID)
			return ok && next == 11
		})
		time.Sleep(100 * time.Millisecond)
		stop()
		if _, _, next, _ := depositCheckpointState(t, ctx, pool, cfg.ChainID); next != 11 {
			t.Fatalf("checkpoint next = %d, want 11 (one covered block must be processed under a 500-block batch)", next)
		}
		if n := depositCountRows(t, ctx, pool, "deposit_observations", cfg.ChainID); n != 1 {
			t.Fatalf("observations = %d, want 1", n)
		}
	})

	t.Run("V2_partial_batch_tail", func(t *testing.T) {
		cfg := depositITConfig(t, 51, testContractA)
		cfg.BatchBlocks = 3 // watermark covers only 2 blocks
		depositSeedCanonical(t, ctx, pool, cfg.ChainID, 10, 11, true)
		depositSeedUpstream(t, ctx, pool, cfg.ChainID, 0, cfg.LogConfigHash, 12)
		depositSeedTransferRow(t, ctx, pool, cfg.ChainID, 11, depositBlockHash(11), depositTxHash(11, 0), 0,
			common.HexToAddress(testContractA), common.HexToAddress(testContractB), common.HexToAddress(depositWatchAddr), big.NewInt(2))

		sc := depositITScanner(t, pool, cfg)
		stop := depositRunLoop(t, ctx, sc, depositITLease(t, pool, cfg.ChainID))
		waitUntil(t, time.Now().Add(10*time.Second), "V2 next_block 12", func() bool {
			_, _, next, ok := depositCheckpointState(t, ctx, pool, cfg.ChainID)
			return ok && next == 12
		})
		time.Sleep(100 * time.Millisecond)
		stop()
		if _, _, next, _ := depositCheckpointState(t, ctx, pool, cfg.ChainID); next != 12 {
			t.Fatalf("checkpoint next = %d, want 12 (watermark-bounded tail must process)", next)
		}
		if n := depositCountRows(t, ctx, pool, "deposit_observations", cfg.ChainID); n != 1 {
			t.Fatalf("observations = %d, want 1", n)
		}
	})

	t.Run("V3_complete_tail_not_stranded_when_upstream_stops", func(t *testing.T) {
		cfg := depositITConfig(t, 52, testContractA) // batch 500
		depositSeedCanonical(t, ctx, pool, cfg.ChainID, 10, 20, true)
		depositSeedUpstream(t, ctx, pool, cfg.ChainID, 0, cfg.LogConfigHash, 15) // stops at next=15
		depositSeedTransferRow(t, ctx, pool, cfg.ChainID, 12, depositBlockHash(12), depositTxHash(12, 0), 0,
			common.HexToAddress(testContractA), common.HexToAddress(testContractB), common.HexToAddress(depositWatchAddr), big.NewInt(1))
		depositSeedTransferRow(t, ctx, pool, cfg.ChainID, 14, depositBlockHash(14), depositTxHash(14, 0), 0,
			common.HexToAddress(testContractA), common.HexToAddress(testContractB), common.HexToAddress(depositWatchAddr), big.NewInt(3))

		sc := depositITScanner(t, pool, cfg)
		stop := depositRunLoop(t, ctx, sc, depositITLease(t, pool, cfg.ChainID))
		waitUntil(t, time.Now().Add(10*time.Second), "V3 next_block 15", func() bool {
			_, _, next, ok := depositCheckpointState(t, ctx, pool, cfg.ChainID)
			return ok && next == 15
		})
		time.Sleep(100 * time.Millisecond)
		stop()
		if _, _, next, _ := depositCheckpointState(t, ctx, pool, cfg.ChainID); next != 15 {
			t.Fatalf("checkpoint next = %d, want 15 (complete tail must not wait for filling the batch)", next)
		}
		if n := depositCountRows(t, ctx, pool, "deposit_observations", cfg.ChainID); n != 2 {
			t.Fatalf("observations = %d, want 2", n)
		}
	})

	t.Run("V4_watermark_at_position_waits", func(t *testing.T) {
		cfg := depositITConfig(t, 53, testContractA)
		depositSeedCanonical(t, ctx, pool, cfg.ChainID, 10, 20, true)
		depositSeedUpstream(t, ctx, pool, cfg.ChainID, 0, cfg.LogConfigHash, 10) // N_u == a

		sc := depositITScanner(t, pool, cfg)
		stop := depositRunLoop(t, ctx, sc, depositITLease(t, pool, cfg.ChainID))
		time.Sleep(200 * time.Millisecond)
		stop()
		if _, _, _, ok := depositCheckpointState(t, ctx, pool, cfg.ChainID); ok {
			t.Fatal("checkpoint created although N_u == a; want a wait with zero advance")
		}
		if n := depositCountRows(t, ctx, pool, "deposit_observations", cfg.ChainID); n != 0 {
			t.Fatalf("observations = %d, want 0", n)
		}
	})
}

// TestDepositServeLoopStopsOnDurableConditions confirms that the trimmed unit
// still stops on real coverage faults: structural gaps, missing or
// non-canonical blocks and upstream identity drift never become waits.
func TestDepositServeLoopStopsOnDurableConditions(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	run := func(t *testing.T, cfg DepositConfig) error {
		t.Helper()
		sc := depositITScanner(t, pool, cfg)
		lease := depositITLease(t, pool, cfg.ChainID)
		loopCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		return sc.ServeLoop(loopCtx, lease, nil)
	}
	assertNoWrites := func(t *testing.T, chainID int64) {
		t.Helper()
		if _, _, _, ok := depositCheckpointState(t, ctx, pool, chainID); ok {
			t.Fatal("checkpoint row created despite the durable stop")
		}
		if n := depositCountRows(t, ctx, pool, "deposit_observations", chainID); n != 0 {
			t.Fatalf("observations = %d, want 0", n)
		}
	}

	t.Run("structural_below_upstream_start", func(t *testing.T) {
		cfg := depositITConfig(t, 54, testContractA)
		depositSeedCanonical(t, ctx, pool, cfg.ChainID, 10, 20, true)
		depositSeedUpstream(t, ctx, pool, cfg.ChainID, 20, cfg.LogConfigHash, 30)
		err := run(t, cfg)
		var gap *depositGap
		if !errors.As(err, &gap) || gap.class != depositGapStructural || gap.cause != gapCauseBelowUpstreamStart {
			t.Fatalf("ServeLoop() = %v, want structural below_upstream_start gap", err)
		}
		assertNoWrites(t, cfg.ChainID)
	})

	t.Run("canonical_missing", func(t *testing.T) {
		cfg := depositITConfig(t, 55, testContractA)
		depositSeedCanonical(t, ctx, pool, cfg.ChainID, 15, 20, true) // 10..14 absent
		depositSeedUpstream(t, ctx, pool, cfg.ChainID, 0, cfg.LogConfigHash, 21)
		err := run(t, cfg)
		var cv *chainViewError
		if !errors.As(err, &cv) || !cv.absent || cv.height != 10 {
			t.Fatalf("ServeLoop() = %v, want chainViewError absent at 10", err)
		}
		assertNoWrites(t, cfg.ChainID)
	})

	t.Run("non_canonical", func(t *testing.T) {
		cfg := depositITConfig(t, 56, testContractA)
		depositSeedCanonical(t, ctx, pool, cfg.ChainID, 10, 20, true)
		if _, err := pool.Exec(ctx, `UPDATE chain_blocks SET canonical = FALSE WHERE chain_id = $1 AND number = 15`, cfg.ChainID); err != nil {
			t.Fatalf("flip canonical: %v", err)
		}
		depositSeedUpstream(t, ctx, pool, cfg.ChainID, 0, cfg.LogConfigHash, 21)
		err := run(t, cfg)
		var cv *chainViewError
		if !errors.As(err, &cv) || !cv.absent || cv.height != 15 {
			t.Fatalf("ServeLoop() = %v, want chainViewError absent at 15", err)
		}
		assertNoWrites(t, cfg.ChainID)
	})

	t.Run("upstream_drift", func(t *testing.T) {
		cfg := depositITConfig(t, 57, testContractA)
		depositSeedCanonical(t, ctx, pool, cfg.ChainID, 10, 20, true)
		depositSeedUpstream(t, ctx, pool, cfg.ChainID, 0, strings.Repeat("ff", 32), 21)
		err := run(t, cfg)
		var drift *upstreamDriftError
		if !errors.As(err, &drift) {
			t.Fatalf("ServeLoop() = %v (%T), want *upstreamDriftError", err, err)
		}
		assertNoWrites(t, cfg.ChainID)
	})
}

// TestDepositCommitReverifiesUnderLock mutates upstream coverage, the upstream
// row or the pause state after the unit was read and confirms the locked
// re-verification still aborts the commit with zero partial writes.
func TestDepositCommitReverifiesUnderLock(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	prepare := func(t *testing.T, chainID int64) (*DepositScanner, *Lease, *depositUnit, depositBatch, *depositProgress) {
		t.Helper()
		cfg := depositITConfig(t, chainID, testContractA)
		depositSeedCanonical(t, ctx, pool, cfg.ChainID, 10, 20, true)
		depositSeedUpstream(t, ctx, pool, cfg.ChainID, 0, cfg.LogConfigHash, 21)
		depositSeedHistory(t, ctx, pool, cfg.ChainID, 1, 10, cfg.ConfigHash)
		depositSeedCheckpoint(t, ctx, pool, cfg.ChainID, 10, cfg.ConfigHash, 10)
		depositSeedTransferRow(t, ctx, pool, cfg.ChainID, 15, depositBlockHash(15), depositTxHash(15, 0), 0,
			common.HexToAddress(testContractA), common.HexToAddress(testContractB), common.HexToAddress(depositWatchAddr), big.NewInt(1))
		sc := depositITScanner(t, pool, cfg)
		lease := depositITLease(t, pool, cfg.ChainID)
		unit, batch, captured := depositITPrepareUnit(t, ctx, sc, 10, 20)
		return sc, lease, unit, batch, captured
	}
	assertAborted := func(t *testing.T, chainID int64) {
		t.Helper()
		if n := depositCountRows(t, ctx, pool, "deposit_observations", chainID); n != 0 {
			t.Fatalf("observations = %d, want 0 (aborted commit must not leave partial writes)", n)
		}
		if _, _, next, _ := depositCheckpointState(t, ctx, pool, chainID); next != 10 {
			t.Fatalf("checkpoint next = %d, want 10 unchanged", next)
		}
	}

	t.Run("upstream_watermark_lowered", func(t *testing.T) {
		sc, lease, unit, batch, captured := prepare(t, 58)
		if _, err := pool.Exec(ctx, `UPDATE log_checkpoint SET next_block = 15 WHERE chain_id = $1`, int64(58)); err != nil {
			t.Fatalf("lower watermark: %v", err)
		}
		err := sc.commitDepositUnit(ctx, lease, unit, batch, captured, 10, 20)
		if !errors.Is(err, errDepositCoverageLost) {
			t.Fatalf("commit = %v, want errDepositCoverageLost", err)
		}
		assertAborted(t, 58)
	})

	t.Run("upstream_row_deleted", func(t *testing.T) {
		sc, lease, unit, batch, captured := prepare(t, 59)
		if _, err := pool.Exec(ctx, `DELETE FROM log_checkpoint WHERE chain_id = $1`, int64(59)); err != nil {
			t.Fatalf("delete upstream row: %v", err)
		}
		err := sc.commitDepositUnit(ctx, lease, unit, batch, captured, 10, 20)
		if !errors.Is(err, errDepositCoverageLost) {
			t.Fatalf("commit = %v, want errDepositCoverageLost", err)
		}
		assertAborted(t, 59)
	})

	t.Run("pause_row_added", func(t *testing.T) {
		sc, lease, unit, batch, captured := prepare(t, 60)
		if _, err := pool.Exec(ctx, `
INSERT INTO deposit_pause (chain_id, height, kind, detail) VALUES ($1, 10, 'chain_view_changed', 'test')`,
			int64(60)); err != nil {
			t.Fatalf("insert deposit pause: %v", err)
		}
		err := sc.commitDepositUnit(ctx, lease, unit, batch, captured, 10, 20)
		var paused *streamPauseError
		if !errors.As(err, &paused) || paused.stream != "deposit_pause" {
			t.Fatalf("commit = %v (%T), want deposit_pause streamPauseError", err, err)
		}
		assertAborted(t, 60)
	})
}
