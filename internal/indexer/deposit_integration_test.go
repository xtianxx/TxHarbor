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
//
// T007 is the full-stack Anvil path (quickstart D1): real token transfers are
// indexed by the real 002 header scan and 003 log scan, then consumed by the
// 004 proof + commit directly (serve wiring is T018). No 004-side source rows
// are seeded.
//
// T009 extends the file with the idempotent-replay and identity-conflict
// ladder (fully duplicate convergence proven on row content, per-field
// conflict whole-batch failures). T010 extends it with deterministic
// connection-level fault injection over a real PostgreSQL session (crash
// after the unit queries, mid-transaction failure, lost COMMIT reply, the
// committed/not-committed verdict pair and bounded-backoff transient database
// errors, each followed by a restart from the durable progress).
package indexer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/big"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/eth"
	"github.com/xtianxx/txharbor/internal/metrics"
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

// TestDepositAnvilFullStackPending is T007 / quickstart D1 (SC-01,
// FR-01/FR-02/FR-11): real Anvil token transfers are indexed by the real 002
// header scan and 003 log scan into chain_blocks and erc20_transfer_logs, then
// the 004 scanner proves coverage over the freshly persisted rows and consumes
// them into Pending observations. Every source row 004 reads was written by
// the 003 scanner from real chain data; this test seeds no 004-side rows and
// no erc20_transfer_logs.
//
// T018 is not implemented (serve wiring): the test composes the already-built
// components directly — Scanner.Run wins the lease, then LogScanner.ServeLoop
// and DepositScanner.ServeLoop share the same lease handle, exactly as one
// process would after wiring. A pass proves the recognition chain end to end;
// it does NOT prove that the production serve path registers the deposit loop.
//
// Environment: Anvil ghcr.io/foundry-rs/foundry:v1.8.1 with --chain-id 31337
// (scanChainID) and PostgreSQL postgres:18.6-trixie (startIndexerPostgres).
func TestDepositAnvilFullStackPending(t *testing.T) {
	dsn := startIndexerPostgres(t)
	ctx := context.Background()
	pool := openIndexerPool(t, dsn)
	defer pool.Close()

	node := logscanStartAnvilNode(t)
	client, err := eth.Dial(ctx, node.url, 5*time.Second)
	if err != nil {
		t.Fatalf("dial anvil client: %v", err)
	}
	defer client.Close()

	const chainID = scanChainID // 31337, matching the Anvil chain id

	// Real token contracts: anvil_setCode gives each address fallback bytecode
	// that emits exactly one standard Transfer log (logscanEmitCode, no forge).
	watchAddr := common.HexToAddress("0x00000000000000000000000000000000000000d1")
	otherAddr := common.HexToAddress("0x00000000000000000000000000000000000000d2")
	tokenA := common.HexToAddress("0x00000000000000000000000000000000000000a1")
	tokenB := common.HexToAddress("0x00000000000000000000000000000000000000a2")
	tokenNM := common.HexToAddress("0x00000000000000000000000000000000000000b1")

	sender := node.accounts[0]
	topics := [3]common.Hash{eth.TransferSig}
	topics[1] = common.BytesToHash(sender.Bytes())
	topics[2] = common.BytesToHash(watchAddr.Bytes())
	node.setCode(t, tokenA, logscanEmitCode(topics, big.NewInt(1)))
	node.setCode(t, tokenB, logscanEmitCode(topics, big.NewInt(2)))
	topics[2] = common.BytesToHash(otherAddr.Bytes())
	node.setCode(t, tokenNM, logscanEmitCode(topics, big.NewInt(3)))

	// Two matching transfers (each exactly one Pending) and one non-matching
	// transfer to an unwatched recipient (tracked by 003, zero observations),
	// separated by mined empty intervals. Heights come from the receipts.
	txA, heightA := node.sendTransferAt(t, tokenA)
	node.mine(t, 1)
	txB, heightB := node.sendTransferAt(t, tokenB)
	node.mine(t, 2)
	txNM, heightNM := node.sendTransferAt(t, tokenNM)
	head := node.blockNumber(t)
	if heightA >= heightB || heightB >= heightNM || head < heightNM {
		t.Fatalf("anvil heights hA=%d hB=%d hNM=%d head=%d out of order", heightA, heightB, heightNM, head)
	}

	// Shared 003 whitelist identity: the log scanner persists it and 004
	// recomputes it from the same contract list (R5).
	tokens := []string{
		strings.ToLower(tokenA.Hex()),
		strings.ToLower(tokenB.Hex()),
		strings.ToLower(tokenNM.Hex()),
	}
	logHash := depositUpstreamHash(t, tokens...)

	// 002 indexes the real headers first; they are the log scanner's coverage.
	// Scanner.Run acquires the lease itself, so the handle must not be
	// pre-acquired; afterwards it holds the winning token for 003 and 004.
	lease := newTestLease(t, pool, chainID, "anvil-004-t007", time.Minute, time.Second)
	sc002, err := NewScanner(pool, client, lease, Config{
		StartHeight: 0, RPCTimeout: 2 * time.Second, PollInterval: 25 * time.Millisecond,
		RetryInitial: 25 * time.Millisecond, RetryMax: 250 * time.Millisecond,
	}, logscanLogger())
	if err != nil {
		t.Fatalf("NewScanner(): %v", err)
	}
	runScanTo(t, sc002, head, 60*time.Second)

	// 003 indexes the real Transfer logs into erc20_transfer_logs.
	ls := logscanNewScanner(t, pool, client, client, lease, LogConfig{
		StartBlock: 0, Contracts: tokens, ConfigHash: logHash, BatchBlocks: 2,
	})
	logscanServeTo(t, ctx, pool, chainID, ls, head+1, 60*time.Second)
	logCP, ok := logscanReadCheckpoint(t, ctx, pool, chainID)
	if !ok || logCP.start != 0 || logCP.hash != logHash || logCP.next != int64(head)+1 {
		t.Fatalf("log checkpoint = %+v (ok=%v), want start=0 hash=%s next=%d", logCP, ok, logHash, head+1)
	}

	// 004: the shared lease handle drives the deposit loop directly (T018
	// pending). BatchBlocks is far larger than the chain, so the single unit is
	// a partial batch tail: b = min(a+BatchBlocks-1, N_u-1) = N_u-1.
	const depositHash = "1111111111111111111111111111111111111111111111111111111111111111"
	cfg := DepositConfig{
		ChainID:    chainID,
		StartBlock: 0,
		Assets: []config.DepositEntry{
			{Address: tokens[0], Effective: 0},
			{Address: tokens[1], Effective: 0},
			{Address: tokens[2], Effective: 0},
		},
		Watches:        []config.DepositEntry{{Address: strings.ToLower(watchAddr.Hex()), Effective: 0}},
		ConfigHash:     depositHash,
		BatchBlocks:    500,
		PollInterval:   25 * time.Millisecond,
		RetryInitial:   25 * time.Millisecond,
		RetryMax:       250 * time.Millisecond,
		LogContracts:   tokens,
		LogConfigHash:  logHash,
		LogStartHeight: 0,
	}
	if uint64(logCP.next) >= cfg.BatchBlocks {
		t.Fatalf("chain coverage %d blocks is not smaller than the batch (%d); partial-tail path not exercised",
			logCP.next, cfg.BatchBlocks)
	}
	sc004, err := NewDepositScanner(pool, cfg)
	if err != nil {
		t.Fatalf("NewDepositScanner(): %v", err)
	}

	stop := depositRunLoop(t, ctx, sc004, lease)
	waitUntil(t, time.Now().Add(60*time.Second), "deposit checkpoint reaches the upstream watermark", func() bool {
		_, _, next, ok := depositCheckpointState(t, ctx, pool, chainID)
		return ok && next >= uint64(logCP.next)
	})
	time.Sleep(100 * time.Millisecond) // surface any erroneous extra advance
	stop()

	// Coverage advance: the whole partial batch tail is consumed to N_u.
	start, cfgHash, next, ok := depositCheckpointState(t, ctx, pool, chainID)
	if !ok || start != 0 || cfgHash != depositHash || next != uint64(logCP.next) {
		t.Fatalf("deposit checkpoint = (%d,%s,%d,%v), want (0,%s,%d,true)", start, cfgHash, next, ok, depositHash, logCP.next)
	}
	if next != head+1 {
		t.Fatalf("deposit next = %d, want exactly head+1 = %d (one partial-batch unit)", next, head+1)
	}

	// assertObservation pins one matching transfer's observation against the
	// real chain: 003's stored row identity, the Anvil header hash, the
	// canonical chain_blocks hash and every Table 1 field.
	assertObservation := func(t *testing.T, txHash common.Hash, height uint64, amount *big.Int, token common.Address) {
		t.Helper()
		stored, ok := logscanReadRowAt(t, ctx, pool, chainID, height)
		if !ok {
			t.Fatalf("003 stored no log at height %d (tx %s)", height, txHash.Hex())
		}
		hdr, err := client.HeaderByNumber(ctx, new(big.Int).SetUint64(height))
		if err != nil {
			t.Fatalf("anvil header %d: %v", height, err)
		}
		wantBlockHash := hashHex(hdr.Hash())
		wantTxHash := strings.ToLower(txHash.Hex())
		if stored.blockHash != wantBlockHash || stored.txHash != wantTxHash || stored.logIndex != 0 {
			t.Fatalf("003 row at %d = %s/%s/%d, want %s/%s/0",
				height, stored.blockHash, stored.txHash, stored.logIndex, wantBlockHash, wantTxHash)
		}
		if wantData := common.BigToHash(amount).Hex(); stored.data != wantData {
			t.Fatalf("003 data at %d = %s, want %s", height, stored.data, wantData)
		}
		var canonicalHash string
		if err := pool.QueryRow(ctx, `
SELECT hash FROM chain_blocks WHERE chain_id = $1 AND number = $2 AND canonical`,
			chainID, int64(height)).Scan(&canonicalHash); err != nil {
			t.Fatalf("canonical block %d: %v", height, err)
		}
		if canonicalHash != wantBlockHash {
			t.Fatalf("chain_blocks[%d] = %s, want %s", height, canonicalHash, wantBlockHash)
		}

		var (
			obsBlockHash, obsTxHash, obsContract, obsSender, obsRecipient, obsAmount, obsStatus string
			obsBlockNumber, obsLogIndex, obsVersion                                             int64
		)
		err = pool.QueryRow(ctx, `
SELECT block_hash, tx_hash, log_index, block_number, contract, sender, recipient, amount::text, status, version_seq
FROM deposit_observations
WHERE chain_id = $1 AND block_hash = $2 AND tx_hash = $3 AND log_index = $4`,
			chainID, wantBlockHash, wantTxHash, stored.logIndex).
			Scan(&obsBlockHash, &obsTxHash, &obsLogIndex, &obsBlockNumber, &obsContract,
				&obsSender, &obsRecipient, &obsAmount, &obsStatus, &obsVersion)
		if err != nil {
			t.Fatalf("observation for tx %s: %v", wantTxHash, err)
		}
		wantContract := strings.ToLower(token.Hex())
		wantSender := strings.ToLower(sender.Hex())
		wantRecipient := strings.ToLower(watchAddr.Hex())
		wantAmount := amount.String()
		if obsBlockHash != wantBlockHash || obsTxHash != wantTxHash || obsLogIndex != stored.logIndex ||
			obsBlockNumber != int64(height) || obsContract != wantContract ||
			obsSender != wantSender || obsRecipient != wantRecipient ||
			obsAmount != wantAmount || obsStatus != "pending" || obsVersion != 1 {
			t.Fatalf("observation = %s/%s/%d number %d contract %s sender %s recipient %s amount %s status %s version %d; want %s/%s/%d/%d/%s/%s/%s/%s/pending/1",
				obsBlockHash, obsTxHash, obsLogIndex, obsBlockNumber, obsContract, obsSender, obsRecipient,
				obsAmount, obsStatus, obsVersion,
				wantBlockHash, wantTxHash, stored.logIndex, height, wantContract, wantSender, wantRecipient, wantAmount)
		}
	}
	assertObservation(t, txA, heightA, big.NewInt(1), tokenA)
	assertObservation(t, txB, heightB, big.NewInt(2), tokenB)

	// Exactly one observation per matching transfer, and none for the
	// non-matching one; the non-match source row is durably present (tracked
	// and advanced over, not missed).
	if n := depositCountRows(t, ctx, pool, "deposit_observations", chainID); n != 2 {
		t.Fatalf("observations = %d, want exactly 2 (one per matching transfer, zero for the non-match)", n)
	}
	for _, tx := range []common.Hash{txA, txB} {
		var n int
		if err := pool.QueryRow(ctx, `
SELECT count(*) FROM deposit_observations WHERE chain_id = $1 AND tx_hash = $2`,
			chainID, strings.ToLower(tx.Hex())).Scan(&n); err != nil {
			t.Fatalf("count observations for %s: %v", tx.Hex(), err)
		}
		if n != 1 {
			t.Fatalf("observations for matching tx %s = %d, want exactly 1", tx.Hex(), n)
		}
	}
	storedNM, ok := logscanReadRowAt(t, ctx, pool, chainID, heightNM)
	if !ok || storedNM.txHash != strings.ToLower(txNM.Hex()) {
		t.Fatalf("003 row for the non-match tx missing at %d: %+v (ok=%v)", heightNM, storedNM, ok)
	}
	var nonMatched int
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM deposit_observations WHERE chain_id = $1 AND tx_hash = $2`,
		chainID, strings.ToLower(txNM.Hex())).Scan(&nonMatched); err != nil {
		t.Fatalf("count non-match observations: %v", err)
	}
	if nonMatched != 0 {
		t.Fatalf("non-matching transfer produced %d observations, want 0", nonMatched)
	}

	// Version attribution: the first unit bootstrapped version_seq=1 with the
	// full T002 snapshots, and every observation joins that version/hash.
	var (
		versionSeq, hStart, replayFrom         int64
		historyHash, operator, assets, watches string
		requestID                              *string
	)
	err = pool.QueryRow(ctx, `
SELECT version_seq, config_hash, operator, request_id, start_block, replay_from, assets, watches
FROM deposit_config_history WHERE chain_id = $1`, chainID).
		Scan(&versionSeq, &historyHash, &operator, &requestID, &hStart, &replayFrom, &assets, &watches)
	if err != nil {
		t.Fatalf("read history row: %v", err)
	}
	if versionSeq != 1 || historyHash != depositHash || operator != "bootstrap" ||
		requestID != nil || hStart != 0 || replayFrom != 0 {
		t.Fatalf("history = seq %d hash %s operator %q request %v start %d replay %d, want v1/bootstrap/%s",
			versionSeq, historyHash, operator, requestID, hStart, replayFrom, depositHash)
	}
	for _, tok := range tokens {
		if !strings.Contains(assets, tok+":0") {
			t.Fatalf("history assets snapshot %q misses %s:0", assets, tok)
		}
	}
	if wantWatches := strings.ToLower(watchAddr.Hex()) + ":0"; watches != wantWatches {
		t.Fatalf("history watches snapshot = %q, want %q", watches, wantWatches)
	}
	var attributed int
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM deposit_observations o
JOIN deposit_config_history h ON h.chain_id = o.chain_id AND h.version_seq = o.version_seq
WHERE o.chain_id = $1 AND h.config_hash = $2`, chainID, depositHash).Scan(&attributed); err != nil {
		t.Fatalf("version attribution join: %v", err)
	}
	if attributed != 2 {
		t.Fatalf("observations attributed to version hash %s = %d, want 2", depositHash, attributed)
	}
}

// TestDepositMixedIntervalZeroGeneration is T008 / quickstart D2 (SC-01,
// FR-01/FR-03/FR-05): a mixed interval holding a transfer below every
// effective height, a non-whitelisted asset, a non-monitored recipient and a
// zero-value transfer generates exactly zero observations while the progress
// advances over every one of them, including a legal empty tail unit that
// advances to b+1. The four txharbor_deposit_observations_total result classes
// are asserted on the real registry.
//
// Method, recorded as required: this test plants the upstream rows directly
// (canonical chain_blocks + erc20_transfer_logs) as the "upstream already
// committed" prerequisite state (research R9, quickstart D2). No Anvil is
// started here; the real-Anvil path is T007's TestDepositAnvilFullStackPending.
// Every planted row is a structurally valid Transfer, so the invalid class is
// asserted at zero; its non-zero path is T004 unit scope plus T014.
func TestDepositMixedIntervalZeroGeneration(t *testing.T) {
	dsn := startIndexerPostgres(t)
	ctx := context.Background()
	pool := openIndexerPool(t, dsn)
	defer pool.Close()

	// Effective height 20 for both entries: the height-12 row is below every
	// effective height while still inside [StartBlock, N_u-1]. BatchBlocks=5
	// slices the interval into [10,14],[15,19],[20,24],[25,29]; the last unit
	// is a legal empty interval advancing to b+1 = N_u.
	cfg := depositITConfig(t, 88, testContractA)
	cfg.Assets = []config.DepositEntry{{Address: testContractA, Effective: 20}}
	cfg.Watches = []config.DepositEntry{{Address: depositWatchAddr, Effective: 20}}
	cfg.BatchBlocks = 5

	const (
		first, last = uint64(10), uint64(29) // N_u = last + 1 = 30
	)
	depositSeedCanonical(t, ctx, pool, cfg.ChainID, first, last, true)
	depositSeedUpstream(t, ctx, pool, cfg.ChainID, 0, cfg.LogConfigHash, last+1)

	asset := common.HexToAddress(testContractA)
	foreign := common.HexToAddress(testContractB)
	watch := common.HexToAddress(depositWatchAddr)
	other := common.HexToAddress("0x00000000000000000000000000000000000000b1")
	sender := common.HexToAddress("0x00000000000000000000000000000000000000e1")

	rows := []struct {
		height    uint64
		contract  common.Address
		recipient common.Address
		amount    *big.Int
	}{
		{12, asset, watch, big.NewInt(5)},   // below every effective height
		{15, foreign, watch, big.NewInt(5)}, // asset not whitelisted
		{18, asset, other, big.NewInt(5)},   // recipient not monitored
		{22, asset, watch, big.NewInt(0)},   // zero value (all checks pass)
	}
	for _, row := range rows {
		depositSeedTransferRow(t, ctx, pool, cfg.ChainID, row.height,
			depositBlockHash(row.height), depositTxHash(row.height, 0), 0,
			row.contract, sender, row.recipient, row.amount)
	}

	m := metrics.New(func() bool { return true })
	sc := depositITScanner(t, pool, cfg)
	sc.SetResultObserver(func(result string) { m.ObserveDepositObservation(cfg.ChainID, result) })

	stop := depositRunLoop(t, ctx, sc, depositITLease(t, pool, cfg.ChainID))
	waitUntil(t, time.Now().Add(60*time.Second), "mixed interval next_block 30", func() bool {
		_, _, next, ok := depositCheckpointState(t, ctx, pool, cfg.ChainID)
		return ok && next == last+1
	})
	time.Sleep(100 * time.Millisecond) // surface any erroneous extra advance
	stop()

	// Progress crossed every case exactly to the watermark: the final next is
	// the empty tail unit's b+1, no overshoot and no skipped unit.
	start, cfgHash, next, ok := depositCheckpointState(t, ctx, pool, cfg.ChainID)
	if !ok || start != first || cfgHash != cfg.ConfigHash || next != last+1 {
		t.Fatalf("checkpoint = (%d,%s,%d,%v), want (%d,%s,%d,true)",
			start, cfgHash, next, ok, first, cfg.ConfigHash, last+1)
	}
	if n := depositCountRows(t, ctx, pool, "deposit_observations", cfg.ChainID); n != 0 {
		t.Fatalf("observations = %d, want 0 (below-effective, non-whitelist, non-monitored and zero all generate nothing)", n)
	}

	// Source identities: no new rows, the planted ones unchanged, and the
	// progress crossed every seeded height.
	if n := logscanCountLogs(t, ctx, pool, cfg.ChainID); n != len(rows) {
		t.Fatalf("erc20_transfer_logs = %d, want the %d planted rows (no new source rows)", n, len(rows))
	}
	for _, row := range rows {
		if row.height >= next {
			t.Fatalf("progress next=%d did not cross seeded height %d", next, row.height)
		}
		stored, found := logscanReadRowAt(t, ctx, pool, cfg.ChainID, row.height)
		if !found || stored.txHash != depositTxHash(row.height, 0) ||
			stored.blockHash != depositBlockHash(row.height) ||
			stored.contract != strings.ToLower(row.contract.Hex()) {
			t.Fatalf("source row at %d = %+v (found=%v), want tx %s contract %s",
				row.height, stored, found, depositTxHash(row.height, 0), strings.ToLower(row.contract.Hex()))
		}
		var generated int
		if err := pool.QueryRow(ctx, `
SELECT count(*) FROM deposit_observations WHERE chain_id = $1 AND tx_hash = $2`,
			cfg.ChainID, depositTxHash(row.height, 0)).Scan(&generated); err != nil {
			t.Fatalf("count observations for height %d: %v", row.height, err)
		}
		if generated != 0 {
			t.Fatalf("height %d generated %d observations, want 0", row.height, generated)
		}
	}

	// Four-class counter: three valid non-matches (below-effective counts as
	// nomatch), one zero, no match and no structurally invalid row.
	for _, tc := range []struct {
		result string
		want   float64
	}{
		{"matched", 0}, {"nomatch", 3}, {"zero", 1}, {"invalid", 0},
	} {
		if got := depositResultCount(t, m, cfg.ChainID, tc.result); got != tc.want {
			t.Fatalf("%s{result=%s} = %v, want %v", metrics.DepositObservationsMetricName, tc.result, got, tc.want)
		}
	}
}

// depositResultCount reads one txharbor_deposit_observations_total label set
// from a real registry; an absent series reads as 0.
func depositResultCount(t *testing.T, m *metrics.Metrics, chainID int64, result string) float64 {
	t.Helper()
	families, err := m.Gatherer().Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	chain := strconv.FormatInt(chainID, 10)
	for _, family := range families {
		if family.GetName() != metrics.DepositObservationsMetricName {
			continue
		}
		for _, metric := range family.GetMetric() {
			labels := make(map[string]string, len(metric.GetLabel()))
			for _, pair := range metric.GetLabel() {
				labels[pair.GetName()] = pair.GetValue()
			}
			if labels["chain"] == chain && labels["result"] == result {
				return metric.GetCounter().GetValue()
			}
		}
	}
	return 0
}

// --- T009: idempotent replay and identity-conflict comparison ----------------

// depositObservationRow is one full deposit_observations snapshot: the source
// identity plus every content and association column the write protocol
// compares. It is comparable, so a whole-row equality assertion covers
// "content, status and version association unchanged".
type depositObservationRow struct {
	blockHash   string
	txHash      string
	logIndex    int64
	blockNumber int64
	contract    string
	sender      string
	recipient   string
	amount      string
	status      string
	versionSeq  int64
}

// depositSeedObservationRow plants one full observation row; the caller
// controls every field, block_number and version_seq included.
func depositSeedObservationRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, row depositObservationRow) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
INSERT INTO deposit_observations
    (chain_id, block_hash, tx_hash, log_index, block_number, contract, sender, recipient, amount, status, version_seq)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		chainID, row.blockHash, row.txHash, row.logIndex, row.blockNumber,
		row.contract, row.sender, row.recipient, row.amount, row.status, row.versionSeq); err != nil {
		t.Fatalf("seed deposit_observations %s/%s/%d: %v", row.blockHash, row.txHash, row.logIndex, err)
	}
}

// depositReadObservation reads one full observation row snapshot; ok=false
// means no row.
func depositReadObservation(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64,
	blockHash, txHash string, logIndex uint64) (depositObservationRow, bool) {
	t.Helper()
	var row depositObservationRow
	err := pool.QueryRow(ctx, `
SELECT block_hash, tx_hash, log_index, block_number, contract, sender, recipient, amount::text, status, version_seq
FROM deposit_observations
WHERE chain_id = $1 AND block_hash = $2 AND tx_hash = $3 AND log_index = $4`,
		chainID, blockHash, txHash, int64(logIndex)).
		Scan(&row.blockHash, &row.txHash, &row.logIndex, &row.blockNumber, &row.contract,
			&row.sender, &row.recipient, &row.amount, &row.status, &row.versionSeq)
	if errors.Is(err, pgx.ErrNoRows) {
		return row, false
	}
	if err != nil {
		t.Fatalf("read deposit_observations %s/%s/%d: %v", blockHash, txHash, logIndex, err)
	}
	return row, true
}

// TestDepositCommitReplayFullyDuplicateConverges: T009 / SC-02, FR-09/I1. A
// unit whose every matched identity is already stored with identical content
// is replayed under a newer captured version and must converge with zero new
// rows: the durable row count is unchanged and each row's content, status and
// original version association are byte-identical (a replay never rewrites or
// re-versions an existing observation). Convergence is asserted on row
// content, not merely on the absence of a unique-constraint error.
func TestDepositCommitReplayFullyDuplicateConverges(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	const chainID = 89
	cfg := depositITConfig(t, chainID, testContractA)
	depositSeedCanonical(t, ctx, pool, chainID, 10, 20, true)
	depositSeedUpstream(t, ctx, pool, chainID, 0, cfg.LogConfigHash, 21)
	depositSeedHistory(t, ctx, pool, chainID, 1, 10, cfg.ConfigHash)
	depositSeedHistoryVersion(t, ctx, pool, chainID, 2, 1, 10, cfg.ConfigHash, "req-2")
	depositSeedCheckpoint(t, ctx, pool, chainID, 10, cfg.ConfigHash, 10)
	// Both identities were generated under version 1 and are replayed under
	// the captured version 2: the replay must converge without touching them.
	for _, h := range []uint64{15, 16} {
		depositSeedTransferRow(t, ctx, pool, chainID, h, depositBlockHash(h), depositTxHash(h, 0), 0,
			common.HexToAddress(testContractA), common.HexToAddress(testContractB),
			common.HexToAddress(depositWatchAddr), big.NewInt(int64(h-14)))
		depositSeedObservationRow(t, ctx, pool, chainID, depositObservationRow{
			blockHash: depositBlockHash(h), txHash: depositTxHash(h, 0), logIndex: 0,
			blockNumber: int64(h), contract: testContractA, sender: testContractB,
			recipient: depositWatchAddr, amount: strconv.FormatUint(h-14, 10),
			status: "pending", versionSeq: 1,
		})
	}

	before := make([]depositObservationRow, 0, 2)
	for _, h := range []uint64{15, 16} {
		row, ok := depositReadObservation(t, ctx, pool, chainID, depositBlockHash(h), depositTxHash(h, 0), 0)
		if !ok {
			t.Fatalf("precondition: observation at height %d missing", h)
		}
		before = append(before, row)
	}

	sc := depositITScanner(t, pool, cfg)
	lease := depositITLease(t, pool, chainID)
	unit, batch, captured := depositITPrepareUnit(t, ctx, sc, 10, 20)
	if captured == nil || captured.versionSeq != 2 {
		t.Fatalf("captured = %+v, want version_seq 2", captured)
	}
	if len(batch.matched) != 2 {
		t.Fatalf("matched = %d, want 2", len(batch.matched))
	}
	if err := sc.commitDepositUnit(ctx, lease, unit, batch, captured, 10, 20); err != nil {
		t.Fatalf("replay commit: %v", err)
	}

	if n := depositCountRows(t, ctx, pool, "deposit_observations", chainID); n != 2 {
		t.Fatalf("observations = %d, want 2 (a fully duplicate replay inserts zero rows)", n)
	}
	if n := depositCountRows(t, ctx, pool, "deposit_config_history", chainID); n != 2 {
		t.Fatalf("history rows = %d, want 2 (the replay must not create versions)", n)
	}
	for i, h := range []uint64{15, 16} {
		after, ok := depositReadObservation(t, ctx, pool, chainID, depositBlockHash(h), depositTxHash(h, 0), 0)
		if !ok {
			t.Fatalf("observation at height %d disappeared", h)
		}
		if after != before[i] {
			t.Fatalf("replayed row at height %d changed: %+v -> %+v", h, before[i], after)
		}
		if after.versionSeq != 1 {
			t.Fatalf("row at height %d version_seq = %d, want the original 1 (never the commit-time latest)",
				h, after.versionSeq)
		}
	}
	if _, _, next, _ := depositCheckpointState(t, ctx, pool, chainID); next != 21 {
		t.Fatalf("checkpoint next = %d, want exactly 21", next)
	}
}

// TestDepositCommitFieldConflictFailsWholeBatch: T009 / SC-05, FR-10/I1. For
// every representable content field of an existing observation identity, a
// stored value that differs from the parsed source row fails the whole batch
// with identity_conflict: the progress stays put, the new identity inserted
// earlier in the same transaction is rolled back, the stored row is never
// overwritten and no pause row is written (conflicts are never ignored via
// ON CONFLICT DO NOTHING).
//
// status is single-valued ('pending') by 004's own CHECK, so a differing
// status row cannot exist in 004; the equal-status branch is asserted by
// TestDepositCommitReplayFullyDuplicateConverges.
func TestDepositCommitFieldConflictFailsWholeBatch(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	altSender := strings.ToLower(common.HexToAddress("0x00000000000000000000000000000000000000c1").Hex())
	altRecipient := strings.ToLower(common.HexToAddress("0x00000000000000000000000000000000000000c2").Hex())

	cases := []struct {
		name   string
		mutate func(*depositObservationRow)
	}{
		{"contract", func(r *depositObservationRow) {
			r.contract = strings.ToLower(common.HexToAddress(testContractB).Hex())
		}},
		{"block_number", func(r *depositObservationRow) { r.blockNumber = 14 }},
		{"sender", func(r *depositObservationRow) { r.sender = altSender }},
		{"recipient", func(r *depositObservationRow) { r.recipient = altRecipient }},
		{"amount", func(r *depositObservationRow) { r.amount = "2" }},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			chainID := int64(90 + i)
			cfg := depositITConfig(t, chainID, testContractA)
			depositSeedCanonical(t, ctx, pool, chainID, 10, 20, true)
			depositSeedUpstream(t, ctx, pool, chainID, 0, cfg.LogConfigHash, 21)
			depositSeedHistory(t, ctx, pool, chainID, 1, 10, cfg.ConfigHash)
			depositSeedCheckpoint(t, ctx, pool, chainID, 10, cfg.ConfigHash, 10)
			// Identity I at height 15 conflicts with the stored row; identity J
			// at height 14 is new and sorts first, so a partial batch would
			// leave J behind.
			depositSeedTransferRow(t, ctx, pool, chainID, 15, depositBlockHash(15), depositTxHash(15, 0), 0,
				common.HexToAddress(testContractA), common.HexToAddress(testContractB),
				common.HexToAddress(depositWatchAddr), big.NewInt(1))
			depositSeedTransferRow(t, ctx, pool, chainID, 14, depositBlockHash(14), depositTxHash(14, 0), 0,
				common.HexToAddress(testContractA), common.HexToAddress(testContractB),
				common.HexToAddress(depositWatchAddr), big.NewInt(3))

			conflicting := depositObservationRow{
				blockHash: depositBlockHash(15), txHash: depositTxHash(15, 0), logIndex: 0,
				blockNumber: 15, contract: testContractA, sender: testContractB,
				recipient: depositWatchAddr, amount: "1", status: "pending", versionSeq: 1,
			}
			tc.mutate(&conflicting)
			depositSeedObservationRow(t, ctx, pool, chainID, conflicting)
			planted, ok := depositReadObservation(t, ctx, pool, chainID, depositBlockHash(15), depositTxHash(15, 0), 0)
			if !ok {
				t.Fatal("precondition: conflicting observation missing")
			}

			sc := depositITScanner(t, pool, cfg)
			lease := depositITLease(t, pool, chainID)
			unit, batch, captured := depositITPrepareUnit(t, ctx, sc, 10, 20)
			if len(batch.matched) != 2 {
				t.Fatalf("matched = %d, want 2", len(batch.matched))
			}
			err := sc.commitDepositUnit(ctx, lease, unit, batch, captured, 10, 20)
			var conflict *depositIdentityConflictError
			if !errors.As(err, &conflict) {
				t.Fatalf("commit = %v (%T), want *depositIdentityConflictError", err, err)
			}
			if want := depositBlockHash(15) + "/" + depositTxHash(15, 0) + "/0"; conflict.identity != want {
				t.Fatalf("conflict identity = %s, want %s", conflict.identity, want)
			}

			// Whole batch failed: J rolled back, the stored row untouched and
			// the progress unchanged.
			if n := depositCountRows(t, ctx, pool, "deposit_observations", chainID); n != 1 {
				t.Fatalf("observations = %d, want 1 (the new identity J must roll back)", n)
			}
			if _, found := depositReadObservation(t, ctx, pool, chainID, depositBlockHash(14), depositTxHash(14, 0), 0); found {
				t.Fatal("new identity J committed despite the whole-batch conflict failure")
			}
			after, ok := depositReadObservation(t, ctx, pool, chainID, depositBlockHash(15), depositTxHash(15, 0), 0)
			if !ok {
				t.Fatal("stored conflicting observation disappeared")
			}
			if after != planted {
				t.Fatalf("conflicting row was modified: %+v -> %+v", planted, after)
			}
			if _, _, next, _ := depositCheckpointState(t, ctx, pool, chainID); next != 10 {
				t.Fatalf("checkpoint next = %d, want 10 (a failed batch must not advance)", next)
			}
			if n := depositCountRows(t, ctx, pool, "deposit_config_history", chainID); n != 1 {
				t.Fatalf("history rows = %d, want 1", n)
			}
			if n := depositCountRows(t, ctx, pool, "deposit_pause", chainID); n != 0 {
				t.Fatalf("deposit_pause rows = %d, want 0 (the commit writes no pause row; T014 owns persistence)", n)
			}
		})
	}
}

// --- T010: crash recovery and uncertain-commit verdicts ----------------------

// Fault-injection triggers, matched against the SQL text pgx writes to the
// connection. The markers name the exact protocol points:
const (
	// depositFaultAfterReads is the first statement of the commit transaction:
	// once it is written every unit query has completed, so a connection death
	// here is the deterministic "process exits after the queries, before any
	// write" state.
	depositFaultAfterReads = "SET LOCAL statement_timeout"
	// depositFaultMidTransaction is the progress UPDATE of an advancing unit,
	// written after the observations: failing there must roll them all back
	// (no partial commit). The first unit's INSERT checkpoint path is covered
	// by depositFaultAfterReads.
	depositFaultMidTransaction = "UPDATE deposit_checkpoint"
	// depositFaultCommit is the COMMIT statement itself (the reply-lost and
	// never-reached cases).
	depositFaultCommit = "commit"
)

// Deposit-fault verdicts returned by depositFault.match.
const (
	depositFaultNone = iota
	depositFaultDropReply
	depositFaultFailWrite
)

// depositFault injects deterministic faults into every connection of a fault
// pool, through a real dial wrapper (never by simulating return values). A SQL
// trigger matches a client write containing marker: dropReply forwards the
// statement and closes the connection before the reply can be read (the
// statement lands, the worker observes a connection error: the process-exit /
// uncertain-commit state); failWrite closes before forwarding (the statement
// never lands: a mid-transaction connection failure). sticky makes every
// matching write fire so a worker that must stay dead cannot slip a commit
// through while the test stops it. failDials fails every dial while set
// (transient database unavailability). fired closes on the first trigger so
// tests synchronize on the fault itself, never on sleeps.
type depositFault struct {
	marker    string
	dropReply bool
	sticky    bool
	armed     atomic.Bool
	fires     atomic.Int32
	dialFails atomic.Bool
	dialTries atomic.Int32
	fired     chan struct{}
	once      sync.Once
}

func newDepositFault() *depositFault { return &depositFault{fired: make(chan struct{})} }

// armSQL arms a SQL-text trigger.
func (f *depositFault) armSQL(marker string, dropReply, sticky bool) {
	f.marker = marker
	f.dropReply = dropReply
	f.sticky = sticky
	f.armed.Store(true)
}

func (f *depositFault) disarm() { f.armed.Store(false) }

// match classifies one client write and records the trigger.
func (f *depositFault) match(b []byte) int {
	if !f.armed.Load() || f.marker == "" || !bytes.Contains(b, []byte(f.marker)) {
		return depositFaultNone
	}
	if !f.sticky && !f.armed.CompareAndSwap(true, false) {
		return depositFaultNone
	}
	f.fires.Add(1)
	f.once.Do(func() { close(f.fired) })
	if f.dropReply {
		return depositFaultDropReply
	}
	return depositFaultFailWrite
}

// waitFired blocks until the trigger fires at least once.
func (f *depositFault) waitFired(t *testing.T, what string) {
	t.Helper()
	select {
	case <-f.fired:
	case <-time.After(15 * time.Second):
		t.Fatalf("timed out waiting for the injected fault: %s", what)
	}
}

type depositFaultConn struct {
	net.Conn
	f *depositFault
}

func (c *depositFaultConn) Write(b []byte) (int, error) {
	switch c.f.match(b) {
	case depositFaultFailWrite:
		_ = c.Conn.Close()
		return 0, fmt.Errorf("injected connection failure at %q", c.f.marker)
	case depositFaultDropReply:
		n, err := c.Conn.Write(b)
		_ = c.Conn.Close() // reply lost; a forwarded COMMIT still lands
		return n, err
	default:
		return c.Conn.Write(b)
	}
}

// depositOpenFaultPool dials every connection through the injected fault. The
// pool stays lazy (MinConns 0) so no connection exists until the worker asks
// for one; the test's own pool (seeding and durable assertions) is separate
// and never faulted.
func depositOpenFaultPool(t *testing.T, dsn string, f *depositFault) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse dsn for fault pool: %v", err)
	}
	cfg.MaxConns = 8
	cfg.MinConns = 0
	cfg.MaxConnLifetime = time.Hour
	cfg.MaxConnIdleTime = 30 * time.Minute
	cfg.ConnConfig.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
		if f.dialFails.Load() {
			f.dialTries.Add(1)
			return nil, errors.New("injected dial failure")
		}
		c, err := (&net.Dialer{}).DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		return &depositFaultConn{Conn: c, f: f}, nil
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("create fault pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// depositITOwnedLease acquires a lease under a caller-chosen owner and test
// cadence; the win is required.
func depositITOwnedLease(t *testing.T, pool *pgxpool.Pool, chainID int64, owner string, ttl, heartbeat time.Duration) *Lease {
	t.Helper()
	l := newTestLease(t, pool, chainID, owner, ttl, heartbeat)
	won, token, err := l.Acquire(context.Background())
	if err != nil || !won {
		t.Fatalf("lease Acquire(%s) = (won=%v token=%d err=%v), want win", owner, won, token, err)
	}
	return l
}

// depositWaitLeaseExpired waits on the database clock until the chain's
// coordination row is expired (the crashed worker's lease), so a restarted
// owner can acquire it deterministically.
func depositWaitLeaseExpired(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64) {
	t.Helper()
	waitUntil(t, time.Now().Add(15*time.Second), "the crashed worker's lease to expire (DB clock)", func() bool {
		var expired bool
		if err := pool.QueryRow(ctx, `SELECT expires_at < now() FROM indexer_lease WHERE chain_id = $1`, chainID).Scan(&expired); err != nil {
			return false
		}
		return expired
	})
}

// depositAssertCommittedOnce asserts the post-recovery durable state of one
// two-row unit: checkpoint exactly [10,21), two observations (one per matched
// source row, no missed and no duplicate detection), each with its source
// fields and original version association, and exactly one history row.
func depositAssertCommittedOnce(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, cfg DepositConfig) {
	t.Helper()
	start, hash, next, ok := depositCheckpointState(t, ctx, pool, chainID)
	if !ok || start != 10 || hash != cfg.ConfigHash || next != 21 {
		t.Fatalf("checkpoint = (%d,%s,%d,%v), want (10,%s,21,true)", start, hash, next, ok, cfg.ConfigHash)
	}
	if n := depositCountRows(t, ctx, pool, "deposit_observations", chainID); n != 2 {
		t.Fatalf("observations = %d, want exactly 2 (no missed and no duplicated detection)", n)
	}
	for _, h := range []uint64{14, 15} {
		row, found := depositReadObservation(t, ctx, pool, chainID, depositBlockHash(h), depositTxHash(h, 0), 0)
		if !found {
			t.Fatalf("observation for the source row at height %d is missing (missed detection)", h)
		}
		want := depositObservationRow{
			blockHash: depositBlockHash(h), txHash: depositTxHash(h, 0), logIndex: 0,
			blockNumber: int64(h), contract: testContractA, sender: testContractB,
			recipient: depositWatchAddr, amount: strconv.FormatUint(h-13, 10),
			status: "pending", versionSeq: 1,
		}
		if row != want {
			t.Fatalf("observation at height %d = %+v, want %+v", h, row, want)
		}
	}
	if n := depositCountRows(t, ctx, pool, "deposit_config_history", chainID); n != 1 {
		t.Fatalf("history rows = %d, want exactly 1 bootstrap row", n)
	}
}

// TestDepositCrashRecoveryInjectedFaults: T010 / D3+D4 (SC-03/SC-04,
// FR-09/I2, research R9). Three deterministic connection-level faults are
// injected into real PostgreSQL sessions through a dial wrapper and the worker
// is stopped (process exit). A restarted worker with a fresh lease owner
// acquires after the dead worker's lease expires on the DB clock and continues
// from the durable progress: every phase asserts zero missed, zero duplicated
// and zero partial commits. No fault is simulated by return values and no step
// is sequenced by sleeps; the tests synchronize on the injected trigger and on
// durable state.
func TestDepositCrashRecoveryInjectedFaults(t *testing.T) {
	dsn := startIndexerPostgres(t)
	ctx := context.Background()
	pool := openIndexerPool(t, dsn)
	defer pool.Close()

	const leaseTTL = time.Second
	const leaseHeartbeat = 250 * time.Millisecond

	seed := func(t *testing.T, chainID int64) DepositConfig {
		t.Helper()
		cfg := depositITConfig(t, chainID, testContractA)
		cfg.PollInterval = 10 * time.Millisecond
		cfg.RetryInitial = 10 * time.Millisecond
		cfg.RetryMax = 50 * time.Millisecond
		depositSeedCanonical(t, ctx, pool, chainID, 10, 20, true)
		depositSeedUpstream(t, ctx, pool, chainID, 0, cfg.LogConfigHash, 21)
		for _, h := range []uint64{14, 15} {
			depositSeedTransferRow(t, ctx, pool, chainID, h, depositBlockHash(h), depositTxHash(h, 0), 0,
				common.HexToAddress(testContractA), common.HexToAddress(testContractB),
				common.HexToAddress(depositWatchAddr), big.NewInt(int64(h-13)))
		}
		return cfg
	}
	assertZeroFootprint := func(t *testing.T, chainID int64) {
		t.Helper()
		if _, _, _, ok := depositCheckpointState(t, ctx, pool, chainID); ok {
			t.Fatal("crashed worker created a checkpoint row")
		}
		if n := depositCountRows(t, ctx, pool, "deposit_observations", chainID); n != 0 {
			t.Fatalf("observations = %d, want 0 (no partial commit)", n)
		}
		if n := depositCountRows(t, ctx, pool, "deposit_config_history", chainID); n != 0 {
			t.Fatalf("history rows = %d, want 0 (no partial commit)", n)
		}
	}
	// restart waits for the dead worker's lease to expire on the DB clock,
	// acquires a fresh owner and runs a fresh scanner; the durable progress is
	// re-read from the database, never from pre-crash memory.
	restart := func(t *testing.T, chainID int64, cfg DepositConfig, owner string) func() {
		t.Helper()
		depositWaitLeaseExpired(t, ctx, pool, chainID)
		lease := depositITOwnedLease(t, pool, chainID, owner, leaseTTL, leaseHeartbeat)
		sc := depositITScanner(t, pool, cfg)
		return depositRunLoop(t, ctx, sc, lease)
	}
	waitRecovered := func(t *testing.T, chainID int64) {
		t.Helper()
		waitUntil(t, time.Now().Add(10*time.Second), "restarted worker reaches next_block 21", func() bool {
			_, _, next, ok := depositCheckpointState(t, ctx, pool, chainID)
			return ok && next == 21
		})
	}

	t.Run("query_then_exit", func(t *testing.T) {
		const chainID = 100
		cfg := seed(t, chainID)
		fault := newDepositFault()
		faultPool := depositOpenFaultPool(t, dsn, fault)
		lease := depositITOwnedLease(t, pool, chainID, "crashed-100", leaseTTL, leaseHeartbeat)
		sc := depositITScanner(t, faultPool, cfg)

		// Every unit query has completed once the commit transaction opens;
		// killing the connection at its first statement is the deterministic
		// "process exits after the queries" state. sticky keeps the stopped
		// worker from slipping any write through before the test cancels it.
		fault.armSQL(depositFaultAfterReads, true, true)
		stop := depositRunLoop(t, ctx, sc, lease)
		fault.waitFired(t, "connection death after the unit queries")
		stop()
		fault.disarm()
		assertZeroFootprint(t, chainID)

		stop2 := restart(t, chainID, cfg, "restarted-100")
		waitRecovered(t, chainID)
		stop2()
		depositAssertCommittedOnce(t, ctx, pool, chainID, cfg)
	})

	t.Run("mid_transaction_failure", func(t *testing.T) {
		const chainID = 101
		cfg := seed(t, chainID)
		// An advancing unit (checkpoint/history already exist) so the commit
		// reaches the progress UPDATE after inserting the observations; the
		// first unit's INSERT checkpoint path is covered by query_then_exit.
		depositSeedHistory(t, ctx, pool, chainID, 1, 10, cfg.ConfigHash)
		depositSeedCheckpoint(t, ctx, pool, chainID, 10, cfg.ConfigHash, 10)
		fault := newDepositFault()
		faultPool := depositOpenFaultPool(t, dsn, fault)
		lease := depositITOwnedLease(t, pool, chainID, "crashed-101", leaseTTL, leaseHeartbeat)
		sc := depositITScanner(t, faultPool, cfg)

		// The attempt has already inserted both observations when the progress
		// UPDATE dies: closing the connection mid-transaction must roll them
		// all back, with the progress untouched.
		fault.armSQL(depositFaultMidTransaction, false, true)
		stop := depositRunLoop(t, ctx, sc, lease)
		fault.waitFired(t, "connection death at the progress UPDATE")
		stop()
		fault.disarm()
		if _, _, next, ok := depositCheckpointState(t, ctx, pool, chainID); !ok || next != 10 {
			t.Fatalf("checkpoint next = %d (ok=%v), want 10 (no partial advance)", next, ok)
		}
		if n := depositCountRows(t, ctx, pool, "deposit_observations", chainID); n != 0 {
			t.Fatalf("observations = %d, want 0 (mid-transaction failure must roll the inserts back)", n)
		}
		if n := depositCountRows(t, ctx, pool, "deposit_config_history", chainID); n != 1 {
			t.Fatalf("history rows = %d, want the seeded 1 (no partial commit)", n)
		}

		stop2 := restart(t, chainID, cfg, "restarted-101")
		waitRecovered(t, chainID)
		stop2()
		depositAssertCommittedOnce(t, ctx, pool, chainID, cfg)
	})

	t.Run("commit_reply_lost", func(t *testing.T) {
		const chainID = 102
		cfg := seed(t, chainID)
		fault := newDepositFault()
		faultPool := depositOpenFaultPool(t, dsn, fault)
		lease := depositITOwnedLease(t, pool, chainID, "crashed-102", leaseTTL, leaseHeartbeat)
		sc := depositITScanner(t, faultPool, cfg)

		// One drop, not sticky: the server accepts the COMMIT, the client
		// never reads the reply. The commit's re-read must resolve the unit as
		// committed; no retry may generate a second time.
		fault.armSQL(depositFaultCommit, true, false)
		stop := depositRunLoop(t, ctx, sc, lease)
		fault.waitFired(t, "COMMIT reply loss")
		waitRecovered(t, chainID)
		stop()
		if got := fault.fires.Load(); got != 1 {
			t.Fatalf("commit drops = %d, want exactly 1 (a second commit attempt would be a duplicate)", got)
		}
		depositAssertCommittedOnce(t, ctx, pool, chainID, cfg)
	})

	t.Run("transient_db_error_bounded_backoff", func(t *testing.T) {
		const chainID = 103
		cfg := seed(t, chainID)
		fault := newDepositFault()
		faultPool := depositOpenFaultPool(t, dsn, fault)
		lease := depositITOwnedLease(t, pool, chainID, "transient-103", time.Minute, 10*time.Second)
		sc := depositITScanner(t, faultPool, cfg)

		// Every dial fails until the gate opens: the loop must retry with
		// bounded backoff and advance nothing while the database is
		// unreachable.
		fault.dialFails.Store(true)
		stop := depositRunLoop(t, ctx, sc, lease)
		waitUntil(t, time.Now().Add(10*time.Second), "at least 3 failed database dials", func() bool {
			return fault.dialTries.Load() >= 3
		})
		if _, _, _, ok := depositCheckpointState(t, ctx, pool, chainID); ok {
			t.Fatal("checkpoint created while every DB dial was failing")
		}
		if n := depositCountRows(t, ctx, pool, "deposit_observations", chainID); n != 0 {
			t.Fatalf("observations = %d, want 0 while every DB dial was failing", n)
		}
		fault.dialFails.Store(false)
		waitRecovered(t, chainID)
		stop()
		if got := fault.dialTries.Load(); got < 3 {
			t.Fatalf("failed dials = %d, want at least 3 (bounded retry kept dialing before recovery)", got)
		}
		depositAssertCommittedOnce(t, ctx, pool, chainID, cfg)
	})
}

// TestDepositCommitUnknownOutcomeVerdicts: T010 dedicated assertion pair. The
// two possible truths behind an unknown COMMIT must be distinguished by
// re-reading the database, never by interpreting the connection error:
//
//   - reply lost after the statement landed: the re-read proves b+1 durable,
//     so the commit resolves as committed (nil) exactly once;
//   - the COMMIT never reached PostgreSQL: the re-read proves the progress
//     unchanged, so the commit reports an explicit unknown-outcome error (a
//     connection error is not proof of rollback) and a retry on a healthy
//     session converges exactly once.
func TestDepositCommitUnknownOutcomeVerdicts(t *testing.T) {
	dsn := startIndexerPostgres(t)
	ctx := context.Background()
	pool := openIndexerPool(t, dsn)
	defer pool.Close()

	seed := func(t *testing.T, chainID int64) DepositConfig {
		t.Helper()
		cfg := depositITConfig(t, chainID, testContractA)
		depositSeedCanonical(t, ctx, pool, chainID, 10, 20, true)
		depositSeedUpstream(t, ctx, pool, chainID, 0, cfg.LogConfigHash, 21)
		depositSeedTransferRow(t, ctx, pool, chainID, 15, depositBlockHash(15), depositTxHash(15, 0), 0,
			common.HexToAddress(testContractA), common.HexToAddress(testContractB),
			common.HexToAddress(depositWatchAddr), big.NewInt(1))
		return cfg
	}

	t.Run("reply_lost_but_committed", func(t *testing.T) {
		const chainID = 104
		cfg := seed(t, chainID)
		fault := newDepositFault()
		faultPool := depositOpenFaultPool(t, dsn, fault)
		sc := depositITScanner(t, faultPool, cfg)
		lease := depositITLease(t, pool, chainID)
		unit, batch, captured := depositITPrepareUnit(t, ctx, sc, 10, 20)

		fault.armSQL(depositFaultCommit, true, false)
		err := sc.commitDepositUnit(ctx, lease, unit, batch, captured, 10, 20)
		if err != nil {
			t.Fatalf("commit = %v, want nil (the re-read must accept the landed COMMIT)", err)
		}
		if got := fault.fires.Load(); got != 1 {
			t.Fatalf("commit drops = %d, want exactly 1", got)
		}
		if _, _, next, ok := depositCheckpointState(t, ctx, pool, chainID); !ok || next != 21 {
			t.Fatalf("checkpoint next = %d (ok=%v), want 21 durable", next, ok)
		}
		if n := depositCountRows(t, ctx, pool, "deposit_observations", chainID); n != 1 {
			t.Fatalf("observations = %d, want exactly 1 (an uncertain outcome must not duplicate)", n)
		}
		// The pre-commit basis (empty progress) is stale now: a replay is
		// refused by the version/progress adjudication, so no path can
		// generate a second time.
		unit2, batch2, _ := depositITPrepareUnit(t, ctx, sc, 10, 20)
		err = sc.commitDepositUnit(ctx, lease, unit2, batch2, captured, 10, 20)
		if !errors.Is(err, errDepositVersionMismatch) {
			t.Fatalf("stale replay = %v, want errDepositVersionMismatch", err)
		}
		if n := depositCountRows(t, ctx, pool, "deposit_observations", chainID); n != 1 {
			t.Fatalf("observations = %d after the stale replay, want 1", n)
		}
	})

	t.Run("commit_never_reached_db", func(t *testing.T) {
		const chainID = 105
		cfg := seed(t, chainID)
		fault := newDepositFault()
		faultPool := depositOpenFaultPool(t, dsn, fault)
		sc := depositITScanner(t, faultPool, cfg)
		lease := depositITLease(t, pool, chainID)
		unit, batch, captured := depositITPrepareUnit(t, ctx, sc, 10, 20)

		// Close before forwarding: PostgreSQL never sees the COMMIT and the
		// open transaction rolls back. The client error alone must not be
		// treated as success, and must not be assumed to be a rollback either:
		// the verdict comes from the durable re-read.
		fault.armSQL(depositFaultCommit, false, false)
		err := sc.commitDepositUnit(ctx, lease, unit, batch, captured, 10, 20)
		if err == nil {
			t.Fatal("commit = nil, want an explicit unknown-outcome error (a connection error is not proof of rollback)")
		}
		if got := fault.fires.Load(); got != 1 {
			t.Fatalf("commit failures = %d, want exactly 1", got)
		}
		if _, _, _, ok := depositCheckpointState(t, ctx, pool, chainID); ok {
			t.Fatal("checkpoint row exists although the COMMIT never reached PostgreSQL")
		}
		if n := depositCountRows(t, ctx, pool, "deposit_observations", chainID); n != 0 {
			t.Fatalf("observations = %d, want 0 (rolled back, no partial commit)", n)
		}
		if n := depositCountRows(t, ctx, pool, "deposit_config_history", chainID); n != 0 {
			t.Fatalf("history rows = %d, want 0 (rolled back, no partial commit)", n)
		}

		// Retry on a healthy session: exactly one generation, progress b+1.
		sc2 := depositITScanner(t, pool, cfg)
		unit2, batch2, captured2 := depositITPrepareUnit(t, ctx, sc2, 10, 20)
		if captured2 != nil {
			t.Fatalf("captured = %+v, want empty progress (nothing committed)", captured2)
		}
		if err := sc2.commitDepositUnit(ctx, lease, unit2, batch2, captured2, 10, 20); err != nil {
			t.Fatalf("retry commit: %v", err)
		}
		if _, _, next, ok := depositCheckpointState(t, ctx, pool, chainID); !ok || next != 21 {
			t.Fatalf("checkpoint next = %d (ok=%v), want 21 after the retry", next, ok)
		}
		if n := depositCountRows(t, ctx, pool, "deposit_observations", chainID); n != 1 {
			t.Fatalf("observations = %d, want exactly 1 after the retry", n)
		}
	})
}

// TestDepositRestartConfigComparison is T011 (US4): a restart whose
// configuration differs from the durable row refuses before any write, a
// blank whitelist refuses inside the constructor (no I/O, hence no upstream
// read), env/upstream drift refuses, and restoring the original configuration
// resumes from the durable progress with pre-existing observations
// byte-identical. Startup asserts both-sides presence plus row-vs-latest
// linkage; one-sided state is corruption, never repaired. The refusal error
// returned here is what the serve path maps to a non-zero exit (exit mapping
// itself is serve wiring, T018); this test locks the refusal + zero-damage
// contract at the component level.
func TestDepositRestartConfigComparison(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	// seedRestart plants the durable progress (checkpoint next=15 + history
	// seq1), canonical 10..20, upstream next=21, one matched transfer row at
	// 15 and one orphan pre-existing observation at block 9 (no source row)
	// used as the zero-change snapshot anchor.
	seedRestart := func(t *testing.T, chainID int64) DepositConfig {
		t.Helper()
		cfg := depositITConfig(t, chainID, testContractA)
		depositSeedCanonical(t, ctx, pool, chainID, 10, 20, true)
		depositSeedUpstream(t, ctx, pool, chainID, 0, cfg.LogConfigHash, 21)
		depositSeedCheckpoint(t, ctx, pool, chainID, 10, cfg.ConfigHash, 15)
		depositSeedHistory(t, ctx, pool, chainID, 1, 10, cfg.ConfigHash)
		depositSeedTransferRow(t, ctx, pool, chainID, 15, depositBlockHash(15), depositTxHash(15, 0), 0,
			common.HexToAddress(testContractA), common.HexToAddress(testContractB), common.HexToAddress(depositWatchAddr), big.NewInt(1))
		depositSeedObservation(t, ctx, pool, chainID, 9, depositBlockHash(9), depositTxHash(9, 0), 0, "1", 1)
		return cfg
	}
	snapshotObservations := func(t *testing.T, chainID int64) string {
		t.Helper()
		rows, err := pool.Query(ctx, `SELECT t::text FROM deposit_observations t WHERE chain_id = $1 ORDER BY block_number, log_index`, chainID)
		if err != nil {
			t.Fatalf("snapshot observations: %v", err)
		}
		defer rows.Close()
		var sb strings.Builder
		for rows.Next() {
			var s string
			if err := rows.Scan(&s); err != nil {
				t.Fatalf("scan observation snapshot: %v", err)
			}
			sb.WriteString(s + "\n")
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("snapshot observations: %v", err)
		}
		return sb.String()
	}
	// runLoopOnce runs ServeLoop until it returns (refusals return fast) and
	// fails on timeout; cancellable loops must use depositRunLoop instead.
	runLoopOnce := func(t *testing.T, sc *DepositScanner, lease *Lease) error {
		t.Helper()
		loopCtx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- sc.ServeLoop(loopCtx, lease, nil) }()
		select {
		case err := <-done:
			return err
		case <-time.After(15 * time.Second):
			t.Fatalf("ServeLoop did not return within 15s")
			return nil
		}
	}
	assertZeroDamage := func(t *testing.T, chainID int64, before string) {
		t.Helper()
		if _, _, next, ok := depositCheckpointState(t, ctx, pool, chainID); !ok || next != 15 {
			t.Fatalf("checkpoint next = %d (ok=%v), want 15 unchanged", next, ok)
		}
		if got := snapshotObservations(t, chainID); got != before {
			t.Fatalf("observations changed by a refused restart:\nbefore %q\nafter  %q", before, got)
		}
		if n := depositCountRows(t, ctx, pool, "deposit_config_history", chainID); n != 1 {
			t.Fatalf("history rows = %d, want 1 unchanged", n)
		}
	}

	t.Run("changed_asset_refuses", func(t *testing.T) {
		const chainID = 60
		cfg := seedRestart(t, chainID)
		before := snapshotObservations(t, chainID)
		cfg.Assets = []config.DepositEntry{{Address: testContractB, Effective: 10}}
		cfg.ConfigHash = strings.Repeat("bb", 32)
		sc := depositITScanner(t, pool, cfg)
		err := runLoopOnce(t, sc, depositITLease(t, pool, chainID))
		var mismatch *depositConfigMismatchError
		if !errors.As(err, &mismatch) {
			t.Fatalf("ServeLoop() = %v (%T), want *depositConfigMismatchError", err, err)
		}
		assertZeroDamage(t, chainID, before)
	})

	t.Run("changed_watch_address_refuses", func(t *testing.T) {
		const chainID = 61
		cfg := seedRestart(t, chainID)
		before := snapshotObservations(t, chainID)
		cfg.Watches = []config.DepositEntry{{Address: testContractB, Effective: 10}}
		cfg.ConfigHash = strings.Repeat("bb", 32)
		sc := depositITScanner(t, pool, cfg)
		err := runLoopOnce(t, sc, depositITLease(t, pool, chainID))
		var mismatch *depositConfigMismatchError
		if !errors.As(err, &mismatch) {
			t.Fatalf("ServeLoop() = %v (%T), want *depositConfigMismatchError", err, err)
		}
		assertZeroDamage(t, chainID, before)
	})

	t.Run("changed_start_block_refuses", func(t *testing.T) {
		const chainID = 62
		cfg := seedRestart(t, chainID)
		before := snapshotObservations(t, chainID)
		// Same hash, different start: proves the comparison object is the row
		// (start_block, config_hash), not the hash alone.
		cfg.StartBlock = 11
		sc := depositITScanner(t, pool, cfg)
		err := runLoopOnce(t, sc, depositITLease(t, pool, chainID))
		var mismatch *depositConfigMismatchError
		if !errors.As(err, &mismatch) {
			t.Fatalf("ServeLoop() = %v (%T), want *depositConfigMismatchError", err, err)
		}
		assertZeroDamage(t, chainID, before)
	})

	t.Run("changed_effective_height_refuses", func(t *testing.T) {
		const chainID = 63
		cfg := seedRestart(t, chainID)
		before := snapshotObservations(t, chainID)
		cfg.Assets = []config.DepositEntry{{Address: testContractA, Effective: 12}}
		cfg.ConfigHash = strings.Repeat("cc", 32)
		sc := depositITScanner(t, pool, cfg)
		err := runLoopOnce(t, sc, depositITLease(t, pool, chainID))
		var mismatch *depositConfigMismatchError
		if !errors.As(err, &mismatch) {
			t.Fatalf("ServeLoop() = %v (%T), want *depositConfigMismatchError", err, err)
		}
		assertZeroDamage(t, chainID, before)
	})

	t.Run("blank_whitelists_refuse_without_reads", func(t *testing.T) {
		const chainID = 64
		cfg := depositITConfig(t, chainID, testContractA)
		// The constructor is pure validation: it returns synchronously without
		// issuing any query, so no upstream read is possible on this path.
		blank := cfg
		blank.Assets = nil
		if _, err := NewDepositScanner(pool, blank); err == nil || !strings.Contains(err.Error(), "empty asset") {
			t.Fatalf("blank assets err = %v, want empty-set refusal", err)
		}
		blank = cfg
		blank.Watches = nil
		if _, err := NewDepositScanner(pool, blank); err == nil || !strings.Contains(err.Error(), "empty watch") {
			t.Fatalf("blank watches err = %v, want empty-set refusal", err)
		}
		blank = cfg
		blank.LogContracts = nil
		if _, err := NewDepositScanner(pool, blank); err == nil || !strings.Contains(err.Error(), "empty upstream") {
			t.Fatalf("blank upstream whitelist err = %v, want empty-set refusal", err)
		}
		for _, table := range []string{"deposit_checkpoint", "deposit_config_history", "deposit_observations"} {
			if n := depositCountRows(t, ctx, pool, table, chainID); n != 0 {
				t.Fatalf("%s rows = %d, want 0 (refusal must precede any read or write)", table, n)
			}
		}
	})

	t.Run("upstream_drift_refuses", func(t *testing.T) {
		const chainID = 65
		cfg := depositITConfig(t, chainID, testContractA)
		depositSeedCanonical(t, ctx, pool, chainID, 10, 20, true)
		// Persisted identity disagrees with the env recomputation.
		depositSeedUpstream(t, ctx, pool, chainID, 0, strings.Repeat("ff", 32), 21)
		sc := depositITScanner(t, pool, cfg)
		err := runLoopOnce(t, sc, depositITLease(t, pool, chainID))
		var drift *upstreamDriftError
		if !errors.As(err, &drift) {
			t.Fatalf("ServeLoop() = %v (%T), want *upstreamDriftError", err, err)
		}
		if !strings.Contains(err.Error(), "upstream_drift") {
			t.Fatalf("error %q does not name upstream_drift", err.Error())
		}
		if _, _, _, ok := depositCheckpointState(t, ctx, pool, chainID); ok {
			t.Fatal("checkpoint row exists after an upstream_drift refusal")
		}
		if n := depositCountRows(t, ctx, pool, "deposit_observations", chainID); n != 0 {
			t.Fatalf("observations = %d, want 0 after drift refusal", n)
		}
	})

	t.Run("startup_integrity_one_sided_is_corrupt", func(t *testing.T) {
		// Checkpoint without history.
		cfgA := depositITConfig(t, 66, testContractA)
		depositSeedCheckpoint(t, ctx, pool, cfgA.ChainID, 10, cfgA.ConfigHash, 15)
		scA := depositITScanner(t, pool, cfgA)
		if _, err := scA.readProgress(ctx, pool); !isDepositCorrupt(err) {
			t.Fatalf("checkpoint-only readProgress = %v, want *depositCorruptStateError", err)
		}
		if err := runLoopOnce(t, scA, depositITLease(t, pool, cfgA.ChainID)); !isDepositCorrupt(err) {
			t.Fatalf("checkpoint-only ServeLoop = %v, want *depositCorruptStateError", err)
		}
		// History without checkpoint.
		cfgB := depositITConfig(t, 67, testContractA)
		depositSeedHistory(t, ctx, pool, cfgB.ChainID, 1, 10, cfgB.ConfigHash)
		scB := depositITScanner(t, pool, cfgB)
		if _, err := scB.readProgress(ctx, pool); !isDepositCorrupt(err) {
			t.Fatalf("history-only readProgress = %v, want *depositCorruptStateError", err)
		}
		// Checkpoint disagreeing with the latest history row.
		const chainID int64 = 68
		cfgC := depositITConfig(t, chainID, testContractA)
		depositSeedCheckpoint(t, ctx, pool, chainID, 10, cfgC.ConfigHash, 15)
		depositSeedHistory(t, ctx, pool, chainID, 1, 10, cfgC.ConfigHash)
		depositSeedHistoryVersion(t, ctx, pool, chainID, 2, 1, 10, strings.Repeat("dd", 32), "req-2")
		scC := depositITScanner(t, pool, cfgC)
		if _, err := scC.readProgress(ctx, pool); !isDepositCorrupt(err) {
			t.Fatalf("disagreeing readProgress = %v, want *depositCorruptStateError", err)
		}
	})

	t.Run("restore_original_config_resumes", func(t *testing.T) {
		const chainID = 69
		cfg := seedRestart(t, chainID)
		before := snapshotObservations(t, chainID)
		// A changed configuration refuses first (zero damage).
		bad := cfg
		bad.ConfigHash = strings.Repeat("bb", 32)
		lease := depositITLease(t, pool, chainID)
		if err := runLoopOnce(t, depositITScanner(t, pool, bad), lease); !isDepositMismatch(err) {
			t.Fatalf("changed config ServeLoop = %v, want *depositConfigMismatchError", err)
		}
		assertZeroDamage(t, chainID, before)
		// Restoring the original configuration passes the exact guard and
		// continues from the durable next=15 to 21.
		cfg.PollInterval = 20 * time.Millisecond
		cfg.RetryInitial = 20 * time.Millisecond
		cfg.RetryMax = 100 * time.Millisecond
		sc := depositITScanner(t, pool, cfg)
		stop := depositRunLoop(t, ctx, sc, lease)
		waitUntil(t, time.Now().Add(10*time.Second), "restored next_block 21", func() bool {
			_, _, next, ok := depositCheckpointState(t, ctx, pool, chainID)
			return ok && next == 21
		})
		time.Sleep(100 * time.Millisecond)
		stop()
		start, hash, next, ok := depositCheckpointState(t, ctx, pool, chainID)
		if !ok || start != 10 || hash != cfg.ConfigHash || next != 21 {
			t.Fatalf("checkpoint = (%d,%s,%d,%v), want (10,%s,21,true)", start, hash, next, ok, cfg.ConfigHash)
		}
		after := snapshotObservations(t, chainID)
		if !strings.Contains(after, before) || after == before {
			t.Fatalf("observations must keep the pre-existing row and add exactly the new unit:\nbefore %q\nafter  %q", before, after)
		}
		var (
			amount  string
			version int64
		)
		if err := pool.QueryRow(ctx, `SELECT amount::text, version_seq FROM deposit_observations
WHERE chain_id = $1 AND block_number = 15`, chainID).Scan(&amount, &version); err != nil {
			t.Fatalf("read new observation: %v", err)
		}
		if amount != "1" || version != 1 {
			t.Fatalf("new observation = amount %s version %d, want 1/1", amount, version)
		}
	})
}

func isDepositCorrupt(err error) bool {
	var corrupt *depositCorruptStateError
	return errors.As(err, &corrupt)
}

func isDepositMismatch(err error) bool {
	var mismatch *depositConfigMismatchError
	return errors.As(err, &mismatch)
}

// depositRunLoopOnce runs ServeLoop until it returns (refusals return fast)
// and fails on timeout; loops expected to keep running must use
// depositRunLoop instead.
func depositRunLoopOnce(t *testing.T, sc *DepositScanner, lease *Lease) error {
	t.Helper()
	loopCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- sc.ServeLoop(loopCtx, lease, nil) }()
	select {
	case err := <-done:
		return err
	case <-time.After(15 * time.Second):
		t.Fatalf("ServeLoop did not return within 15s")
		return nil
	}
}

// TestDepositGapRecoveryBehavior is T012 (US4): an in-range watermark wait
// advances nothing, survives a slow upstream without a timeout verdict, and
// resumes idempotently from the original gap once coverage is backfilled;
// required history below the upstream start and assets the upstream never
// indexed stop with a structural gap carrying range/cause/config version.
// Identity adoption for structural gaps is the T025 authorization path and is
// asserted there, not here; this test locks classification, wait/stop
// behavior, no-skip, no phantom zero-marks and no silent start/height shifts.
func TestDepositGapRecoveryBehavior(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	t.Run("transient_wait_then_backfill_resumes", func(t *testing.T) {
		const chainID = 70
		cfg := depositITConfig(t, chainID, testContractA)
		cfg.PollInterval = 20 * time.Millisecond
		cfg.RetryInitial = 20 * time.Millisecond
		cfg.RetryMax = 100 * time.Millisecond
		// Watermark covers nothing at the position: N_u=10 <= a=10.
		depositSeedUpstream(t, ctx, pool, chainID, 0, cfg.LogConfigHash, 10)

		sc := depositITScanner(t, pool, cfg)
		stop := depositRunLoop(t, ctx, sc, depositITLease(t, pool, chainID))
		// Slow upstream: a full second behind must still be a wait, never a
		// timeout verdict and never an advance.
		time.Sleep(time.Second)
		if _, _, _, ok := depositCheckpointState(t, ctx, pool, chainID); ok {
			t.Fatal("checkpoint row exists while the watermark covers nothing (must wait with zero advance)")
		}
		if n := depositCountRows(t, ctx, pool, "deposit_observations", chainID); n != 0 {
			t.Fatalf("observations = %d while waiting, want 0", n)
		}
		// Manual repair backfill: canonical history, the watermark advance and
		// the source row arrive together.
		depositSeedCanonical(t, ctx, pool, chainID, 10, 20, true)
		if _, err := pool.Exec(ctx, `UPDATE log_checkpoint SET next_block = 21 WHERE chain_id = $1`, chainID); err != nil {
			t.Fatalf("advance upstream watermark: %v", err)
		}
		depositSeedTransferRow(t, ctx, pool, chainID, 15, depositBlockHash(15), depositTxHash(15, 0), 0,
			common.HexToAddress(testContractA), common.HexToAddress(testContractB), common.HexToAddress(depositWatchAddr), big.NewInt(1))
		// The loop resumes from the original gap and commits exactly [10,20].
		waitUntil(t, time.Now().Add(10*time.Second), "gap resume next_block 21", func() bool {
			_, _, next, ok := depositCheckpointState(t, ctx, pool, chainID)
			return ok && next == 21
		})
		time.Sleep(100 * time.Millisecond)
		stop()
		start, hash, next, ok := depositCheckpointState(t, ctx, pool, chainID)
		if !ok || start != 10 || hash != cfg.ConfigHash || next != 21 {
			t.Fatalf("checkpoint = (%d,%s,%d,%v), want (10,%s,21,true): no skip, no start shift", start, hash, next, ok, cfg.ConfigHash)
		}
		if n := depositCountRows(t, ctx, pool, "deposit_observations", chainID); n != 1 {
			t.Fatalf("observations = %d, want exactly 1 (no phantom marks, no duplicate)", n)
		}
		if n := depositCountRows(t, ctx, pool, "deposit_config_history", chainID); n != 1 {
			t.Fatalf("history rows = %d, want 1 bootstrap row", n)
		}
	})

	t.Run("structural_below_upstream_start_stops", func(t *testing.T) {
		const chainID = 71
		cfg := depositITConfig(t, chainID, testContractA)
		// Required history [10,..] starts below the upstream start S_u=15.
		depositSeedUpstream(t, ctx, pool, chainID, 15, cfg.LogConfigHash, 25)
		sc := depositITScanner(t, pool, cfg)
		err := depositRunLoopOnce(t, sc, depositITLease(t, pool, chainID))
		var gap *depositGap
		if !errors.As(err, &gap) {
			t.Fatalf("ServeLoop() = %v (%T), want *depositGap", err, err)
		}
		if gap.class != depositGapStructural || gap.cause != gapCauseBelowUpstreamStart {
			t.Fatalf("gap = class %q cause %q, want structural/below_upstream_start", gap.class, gap.cause)
		}
		if gap.from != 10 || gap.to != 24 || gap.configHash != cfg.ConfigHash {
			t.Fatalf("gap = %d-%d config %s, want 10-24 with the current config version", gap.from, gap.to, gap.configHash)
		}
		for _, want := range []string{"structural", "10-24", gapCauseBelowUpstreamStart, cfg.ConfigHash} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("gap line %q misses %q (range/cause/config version must be complete)", err.Error(), want)
			}
		}
		if _, _, _, ok := depositCheckpointState(t, ctx, pool, chainID); ok {
			t.Fatal("checkpoint row exists after a structural gap (must stop, never trim the start)")
		}
		if n := depositCountRows(t, ctx, pool, "deposit_observations", chainID); n != 0 {
			t.Fatalf("observations = %d, want 0 after a structural stop", n)
		}
	})

	t.Run("structural_asset_not_indexed_stops", func(t *testing.T) {
		const chainID = 72
		// Upstream whitelist {B} never indexed asset A (effective 10 <= b).
		cfg := depositITConfig(t, chainID, testContractB)
		depositSeedUpstream(t, ctx, pool, chainID, 0, cfg.LogConfigHash, 21)
		sc := depositITScanner(t, pool, cfg)
		err := depositRunLoopOnce(t, sc, depositITLease(t, pool, chainID))
		var gap *depositGap
		if !errors.As(err, &gap) {
			t.Fatalf("ServeLoop() = %v (%T), want *depositGap", err, err)
		}
		if gap.class != depositGapStructural || gap.cause != gapCauseAssetNotIndexed {
			t.Fatalf("gap = class %q cause %q, want structural/asset_not_indexed", gap.class, gap.cause)
		}
		if gap.from != 10 || gap.to != 20 || gap.configHash != cfg.ConfigHash {
			t.Fatalf("gap = %d-%d config %s, want 10-20 with the current config version", gap.from, gap.to, gap.configHash)
		}
		if _, _, _, ok := depositCheckpointState(t, ctx, pool, chainID); ok {
			t.Fatal("checkpoint row exists after a structural gap")
		}
		if n := depositCountRows(t, ctx, pool, "deposit_observations", chainID); n != 0 {
			t.Fatalf("observations = %d, want 0 after a structural stop", n)
		}
	})

	t.Run("no_timeout_verdict_over_time", func(t *testing.T) {
		cfg := depositITConfig(t, 73, testContractA)
		up := &upstreamState{chainID: cfg.ChainID, startBlock: 0, configHash: cfg.LogConfigHash, nextBlock: 10}
		first := classifyUpstreamGap(cfg, up, 10, 20)
		time.Sleep(300 * time.Millisecond)
		second := classifyUpstreamGap(cfg, up, 10, 20)
		if first == nil || second == nil {
			t.Fatalf("gaps = %v/%v, want transient verdicts (unit uncovered at N_u=10)", first, second)
		}
		if *first != *second {
			t.Fatalf("verdict changed over time: %+v vs %+v (elapsed time is never an input)", first, second)
		}
		if second.class != depositGapTransient || second.cause != gapCauseBehindHead {
			t.Fatalf("gap = %+v, want transient/behind_head", second)
		}
	})
}

// TestDepositCoverageProofElements is T013 (US4): the loop-level对照 the
// read-level proof tests do not cover. An empty but covered interval advances
// while a missing watermark stays; a new asset required inside the unit stops
// while one effective beyond it stays covered; a pause row stops the loop;
// foreign-chain-only coverage is never consumed. Config inconsistency is
// locked by T011 (four restart variants) and the mid-commit canonical flip by
// TestDepositCommitCanonicalRevertAborts — referenced, not duplicated.
func TestDepositCoverageProofElements(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	t.Run("empty_advances_vs_missing_waits", func(t *testing.T) {
		// Covered but zero source rows: legal empty interval, advances to b+1.
		const coveredID = 80
		cfg := depositITConfig(t, coveredID, testContractA)
		cfg.PollInterval = 20 * time.Millisecond
		cfg.RetryInitial = 20 * time.Millisecond
		cfg.RetryMax = 100 * time.Millisecond
		depositSeedCanonical(t, ctx, pool, coveredID, 10, 20, true)
		depositSeedUpstream(t, ctx, pool, coveredID, 0, cfg.LogConfigHash, 21)
		stop := depositRunLoop(t, ctx, depositITScanner(t, pool, cfg), depositITLease(t, pool, coveredID))
		waitUntil(t, time.Now().Add(10*time.Second), "empty interval next_block 21", func() bool {
			_, _, next, ok := depositCheckpointState(t, ctx, pool, coveredID)
			return ok && next == 21
		})
		time.Sleep(100 * time.Millisecond)
		stop()
		if _, _, next, _ := depositCheckpointState(t, ctx, pool, coveredID); next != 21 {
			t.Fatalf("checkpoint next = %d, want 21 (empty interval must advance)", next)
		}
		if n := depositCountRows(t, ctx, pool, "deposit_observations", coveredID); n != 0 {
			t.Fatalf("observations = %d, want 0 for the empty interval", n)
		}
		// Missing watermark at the same position: stays, no row, no advance.
		const missingID = 81
		cfgM := depositITConfig(t, missingID, testContractA)
		cfgM.PollInterval = 20 * time.Millisecond
		depositSeedUpstream(t, ctx, pool, missingID, 0, cfgM.LogConfigHash, 10)
		stopM := depositRunLoop(t, ctx, depositITScanner(t, pool, cfgM), depositITLease(t, pool, missingID))
		time.Sleep(300 * time.Millisecond)
		if _, _, _, ok := depositCheckpointState(t, ctx, pool, missingID); ok {
			t.Fatal("checkpoint row exists although N_u <= b (must stay, never treat missing as empty)")
		}
		stopM()
	})

	t.Run("new_asset_inside_vs_beyond_unit", func(t *testing.T) {
		// Asset B is unknown to the upstream whitelist {A}. Effective 12 (at
		// or below b=20) makes it required by this unit: structural stop.
		const insideID = 82
		cfg := depositITConfig(t, insideID, testContractA)
		cfg.Assets = append(append([]config.DepositEntry(nil), cfg.Assets...),
			config.DepositEntry{Address: testContractB, Effective: 12})
		depositSeedCanonical(t, ctx, pool, insideID, 10, 20, true)
		depositSeedUpstream(t, ctx, pool, insideID, 0, cfg.LogConfigHash, 21)
		err := depositRunLoopOnce(t, depositITScanner(t, pool, cfg), depositITLease(t, pool, insideID))
		var gap *depositGap
		if !errors.As(err, &gap) || gap.class != depositGapStructural || gap.cause != gapCauseAssetNotIndexed {
			t.Fatalf("ServeLoop() = %v, want structural asset_not_indexed gap", err)
		}
		if _, _, _, ok := depositCheckpointState(t, ctx, pool, insideID); ok {
			t.Fatal("checkpoint row exists after the in-unit asset stop")
		}
		// Same unknown asset effective at 25, beyond b=20: the current unit
		// stays covered and commits; the loop then stops fail-fast with the
		// structural verdict instead of waiting past a position the upstream
		// can never serve (structural fires before the transient wait).
		const beyondID = 83
		cfgB := depositITConfig(t, beyondID, testContractA)
		cfgB.Assets = append(append([]config.DepositEntry(nil), cfgB.Assets...),
			config.DepositEntry{Address: testContractB, Effective: 25})
		depositSeedCanonical(t, ctx, pool, beyondID, 10, 20, true)
		depositSeedUpstream(t, ctx, pool, beyondID, 0, cfgB.LogConfigHash, 21)
		errB := depositRunLoopOnce(t, depositITScanner(t, pool, cfgB), depositITLease(t, pool, beyondID))
		var gapB *depositGap
		if !errors.As(errB, &gapB) || gapB.class != depositGapStructural || gapB.cause != gapCauseAssetNotIndexed {
			t.Fatalf("ServeLoop() = %v, want structural asset_not_indexed fail-fast after the covered unit", errB)
		}
		if _, _, next, ok := depositCheckpointState(t, ctx, pool, beyondID); !ok || next != 21 {
			t.Fatalf("checkpoint next = %d (ok=%v), want 21 (covered unit commits before the stop)", next, ok)
		}
		if n := depositCountRows(t, ctx, pool, "deposit_observations", beyondID); n != 0 {
			t.Fatalf("observations = %d, want 0 (beyond-unit asset needs no row)", n)
		}
	})

	t.Run("pause_row_stops_loop", func(t *testing.T) {
		const chainID = 84
		cfg := depositITConfig(t, chainID, testContractA)
		depositSeedCanonical(t, ctx, pool, chainID, 10, 20, true)
		depositSeedUpstream(t, ctx, pool, chainID, 0, cfg.LogConfigHash, 21)
		if _, err := pool.Exec(ctx, `
INSERT INTO deposit_pause (chain_id, height, kind, detail) VALUES ($1, 10, 'upstream_gap', 'test')`,
			chainID); err != nil {
			t.Fatalf("seed deposit_pause: %v", err)
		}
		err := depositRunLoopOnce(t, depositITScanner(t, pool, cfg), depositITLease(t, pool, chainID))
		var paused *streamPauseError
		if !errors.As(err, &paused) || paused.stream != "deposit_pause" {
			t.Fatalf("ServeLoop() = %v (%T), want *streamPauseError for deposit_pause", err, err)
		}
		if _, _, _, ok := depositCheckpointState(t, ctx, pool, chainID); ok {
			t.Fatal("checkpoint row created despite the pause stop")
		}
		if n := depositCountRows(t, ctx, pool, "deposit_observations", chainID); n != 0 {
			t.Fatalf("observations = %d, want 0 under pause", n)
		}
	})

	t.Run("foreign_chain_coverage_isolated", func(t *testing.T) {
		const foreignID = 99
		cfgF := depositITConfig(t, foreignID, testContractA)
		depositSeedCanonical(t, ctx, pool, foreignID, 10, 20, true)
		depositSeedUpstream(t, ctx, pool, foreignID, 0, cfgF.LogConfigHash, 21)
		depositSeedTransferRow(t, ctx, pool, foreignID, 15, depositBlockHash(15), depositTxHash(15, 0), 0,
			common.HexToAddress(testContractA), common.HexToAddress(testContractB), common.HexToAddress(depositWatchAddr), big.NewInt(1))
		// This chain has no upstream row at all: the foreign coverage must
		// not leak in, the loop waits with zero state.
		const chainID = 85
		cfg := depositITConfig(t, chainID, testContractA)
		cfg.PollInterval = 20 * time.Millisecond
		stop := depositRunLoop(t, ctx, depositITScanner(t, pool, cfg), depositITLease(t, pool, chainID))
		time.Sleep(300 * time.Millisecond)
		if _, _, _, ok := depositCheckpointState(t, ctx, pool, chainID); ok {
			t.Fatal("checkpoint row exists without this chain's coverage (foreign rows must not leak)")
		}
		if n := depositCountRows(t, ctx, pool, "deposit_observations", chainID); n != 0 {
			t.Fatalf("observations = %d, want 0 (chain identity)", n)
		}
		stop()
	})
}

// TestDepositReplayConsumerConverges is T026 (US4) consumer side: after an
// authorization rewinds the position, the loop replays the range without
// adding or resetting observations (original version references kept) and
// then continues past the old position.
func TestDepositReplayConsumerConverges(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	const chainID = 130
	cfg1 := depositITConfig(t, chainID, testContractA, testContractB)
	cfg1.PollInterval = 20 * time.Millisecond
	cfg1.RetryInitial = 20 * time.Millisecond
	cfg1.RetryMax = 100 * time.Millisecond
	depositSeedCanonical(t, ctx, pool, chainID, 10, 20, true)
	depositSeedUpstream(t, ctx, pool, chainID, 0, cfg1.LogConfigHash, 21)
	depositSeedTransferRow(t, ctx, pool, chainID, 12, depositBlockHash(12), depositTxHash(12, 0), 0,
		common.HexToAddress(testContractA), common.HexToAddress(testContractB), common.HexToAddress(depositWatchAddr), big.NewInt(1))
	depositSeedTransferRow(t, ctx, pool, chainID, 15, depositBlockHash(15), depositTxHash(15, 0), 0,
		common.HexToAddress(testContractA), common.HexToAddress(testContractB), common.HexToAddress(depositWatchAddr), big.NewInt(2))

	lease := depositITLease(t, pool, chainID)
	stop1 := depositRunLoop(t, ctx, depositITScanner(t, pool, cfg1), lease)
	waitUntil(t, time.Now().Add(10*time.Second), "pre-auth next_block 21", func() bool {
		_, _, next, ok := depositCheckpointState(t, ctx, pool, chainID)
		return ok && next == 21
	})
	time.Sleep(100 * time.Millisecond)
	stop1()
	if n := depositCountRows(t, ctx, pool, "deposit_observations", chainID); n != 2 {
		t.Fatalf("observations = %d, want 2 before the authorization", n)
	}

	// Authorize H1->H2 (add asset B effective 10): replay rewinds 21->10.
	watches := depositAuthLine(depositWatchAddr, 10)
	h2Assets := depositAuthSnapshot(depositAuthLine(testContractA, 10), depositAuthLine(testContractB, 10))
	h2 := depositAuthHash(t, 10, h2Assets, watches)
	req := depositAuthBaseReq(t, chainID, "replay-auth-1", 1)
	req.NewConfigHash, req.NewStartBlock, req.NewAssets, req.NewWatches = h2, 10, h2Assets, watches
	res, err := AuthorizeDepositConfig(ctx, pool, req)
	if err != nil {
		t.Fatalf("authorize H2: %v", err)
	}
	if want := (DepositAuthResult{VersionSeq: 2, ReplayFrom: 10, Pause: DepositPauseRetained}); res != want {
		t.Fatalf("result = %+v, want %+v", res, want)
	}

	// Replay under H2: the two identities converge with version_seq=1 kept,
	// the position returns to 21 with no new rows.
	cfg2 := cfg1
	cfg2.Assets = append(append([]config.DepositEntry(nil), cfg1.Assets...),
		config.DepositEntry{Address: testContractB, Effective: 10})
	cfg2.ConfigHash = h2
	stop2 := depositRunLoop(t, ctx, depositITScanner(t, pool, cfg2), lease)
	waitUntil(t, time.Now().Add(10*time.Second), "replay next_block 21", func() bool {
		_, _, next, ok := depositCheckpointState(t, ctx, pool, chainID)
		return ok && next == 21
	})
	time.Sleep(100 * time.Millisecond)
	stop2()
	if n := depositCountRows(t, ctx, pool, "deposit_observations", chainID); n != 2 {
		t.Fatalf("observations = %d after replay, want 2 (replay adds nothing, resets nothing)", n)
	}
	for _, tc := range []struct {
		block         uint64
		amount        string
		version       int64
	}{
		{12, "1", 1},
		{15, "2", 1},
	} {
		var amount string
		var version int64
		if err := pool.QueryRow(ctx, `SELECT amount::text, version_seq FROM deposit_observations
WHERE chain_id = $1 AND block_number = $2`, chainID, int64(tc.block)).Scan(&amount, &version); err != nil {
			t.Fatalf("read replayed observation at %d: %v", tc.block, err)
		}
		if amount != tc.amount || version != tc.version {
			t.Fatalf("replayed observation at %d = (%s,%d), want (%s,%d)", tc.block, amount, version, tc.amount, tc.version)
		}
	}
	if n := depositCountRows(t, ctx, pool, "deposit_config_history", chainID); n != 2 {
		t.Fatalf("history rows = %d, want 2 (replay adds no versions)", n)
	}

	// Past the old position: new coverage generates under the current version.
	depositSeedCanonical(t, ctx, pool, chainID, 21, 25, true)
	if _, err := pool.Exec(ctx, `UPDATE log_checkpoint SET next_block = 26 WHERE chain_id = $1`, chainID); err != nil {
		t.Fatalf("extend upstream watermark: %v", err)
	}
	depositSeedTransferRow(t, ctx, pool, chainID, 22, depositBlockHash(22), depositTxHash(22, 0), 0,
		common.HexToAddress(testContractA), common.HexToAddress(testContractB), common.HexToAddress(depositWatchAddr), big.NewInt(3))
	stop3 := depositRunLoop(t, ctx, depositITScanner(t, pool, cfg2), lease)
	waitUntil(t, time.Now().Add(10*time.Second), "post-replay next_block 26", func() bool {
		_, _, next, ok := depositCheckpointState(t, ctx, pool, chainID)
		return ok && next == 26
	})
	time.Sleep(100 * time.Millisecond)
	stop3()
	if n := depositCountRows(t, ctx, pool, "deposit_observations", chainID); n != 3 {
		t.Fatalf("observations = %d, want 3 after continuing", n)
	}
	var amount string
	var version int64
	if err := pool.QueryRow(ctx, `SELECT amount::text, version_seq FROM deposit_observations
WHERE chain_id = $1 AND block_number = 22`, chainID).Scan(&amount, &version); err != nil {
		t.Fatalf("read new observation: %v", err)
	}
	if amount != "3" || version != 2 {
		t.Fatalf("new observation = (%s,%d), want (3,2) under the current version", amount, version)
	}
}

// depositPauseRow reads one live deposit_pause row for assertions.
type depositPauseRow struct {
	id, rev, height int64
	kind, detail    string
}

func depositReadPause(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64) (depositPauseRow, bool) {
	t.Helper()
	var row depositPauseRow
	err := pool.QueryRow(ctx, `
SELECT pause_id, revision, height, kind, detail FROM deposit_pause WHERE chain_id = $1`, chainID).
		Scan(&row.id, &row.rev, &row.height, &row.kind, &row.detail)
	if errors.Is(err, pgx.ErrNoRows) {
		return row, false
	}
	if err != nil {
		t.Fatalf("read deposit_pause: %v", err)
	}
	return row, true
}

// TestDepositPauseWriteAndStop is T014 (US5): chain-view re-check failures,
// structural gaps, deterministic validation failures and identity conflicts
// stop without advancing and persist exactly one pause row (instance identity
// + revision 1 + version tag); drift stops bare with zero damage; a pause
// survives restarts with the same instance identity.
func TestDepositPauseWriteAndStop(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	assertNoProgress := func(t *testing.T, chainID int64) {
		t.Helper()
		if _, _, _, ok := depositCheckpointState(t, ctx, pool, chainID); ok {
			t.Fatal("checkpoint row created despite the pause stop")
		}
		if n := depositCountRows(t, ctx, pool, "deposit_config_history", chainID); n != 0 {
			t.Fatalf("history rows = %d, want 0", n)
		}
	}

	t.Run("structural_gap_writes_pause", func(t *testing.T) {
		const chainID = 160
		cfg := depositITConfig(t, chainID, testContractA)
		depositSeedUpstream(t, ctx, pool, chainID, 15, cfg.LogConfigHash, 25)
		err := depositRunLoopOnce(t, depositITScanner(t, pool, cfg), depositITLease(t, pool, chainID))
		var gap *depositGap
		if !errors.As(err, &gap) || gap.class != depositGapStructural {
			t.Fatalf("ServeLoop() = %v, want a structural gap stop", err)
		}
		row, ok := depositReadPause(t, ctx, pool, chainID)
		if !ok {
			t.Fatal("no deposit_pause row after the structural stop")
		}
		if row.kind != "upstream_gap" || row.height != 10 || row.rev != 1 || row.id <= 0 {
			t.Fatalf("pause = %+v, want upstream_gap at 10 rev 1 with a sequence identity", row)
		}
		for _, want := range []string{"class=structural", "gap=10-24", "cause=below_upstream_start", cfg.ConfigHash, "version=0"} {
			if !strings.Contains(row.detail, want) {
				t.Fatalf("pause detail %q misses %q", row.detail, want)
			}
		}
		assertNoProgress(t, chainID)
		if n := depositCountRows(t, ctx, pool, "deposit_observations", chainID); n != 0 {
			t.Fatalf("observations = %d, want 0", n)
		}
	})

	t.Run("chain_view_writes_pause", func(t *testing.T) {
		const chainID = 161
		cfg := depositITConfig(t, chainID, testContractA)
		depositSeedCanonical(t, ctx, pool, chainID, 10, 14, true)
		depositSeedCanonical(t, ctx, pool, chainID, 16, 20, true)
		depositSeedUpstream(t, ctx, pool, chainID, 0, cfg.LogConfigHash, 21)
		err := depositRunLoopOnce(t, depositITScanner(t, pool, cfg), depositITLease(t, pool, chainID))
		var cv *chainViewError
		if !errors.As(err, &cv) || !cv.absent || cv.height != 15 {
			t.Fatalf("ServeLoop() = %v, want chainViewError absent at 15", err)
		}
		row, ok := depositReadPause(t, ctx, pool, chainID)
		if !ok {
			t.Fatal("no deposit_pause row after the chain-view stop")
		}
		if row.kind != "chain_view_changed" || row.height != 15 || row.rev != 1 {
			t.Fatalf("pause = %+v, want chain_view_changed at 15 rev 1", row)
		}
		for _, want := range []string{"class=chain_view_changed", "version=0"} {
			if !strings.Contains(row.detail, want) {
				t.Fatalf("pause detail %q misses %q", row.detail, want)
			}
		}
		assertNoProgress(t, chainID)
	})

	t.Run("validation_failure_writes_pause", func(t *testing.T) {
		const chainID = 162
		cfg := depositITConfig(t, chainID, testContractA)
		depositSeedCanonical(t, ctx, pool, chainID, 10, 20, true)
		depositSeedUpstream(t, ctx, pool, chainID, 0, cfg.LogConfigHash, 21)
		topic1 := common.BytesToHash(common.HexToAddress(testContractB).Bytes()).Hex()
		topic2 := common.BytesToHash(common.HexToAddress(depositWatchAddr).Bytes()).Hex()
		if _, err := pool.Exec(ctx, `
INSERT INTO erc20_transfer_logs
    (chain_id, block_number, block_hash, tx_hash, log_index, contract, topic0, topic1, topic2, data)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
			chainID, 15, depositBlockHash(15), depositTxHash(15, 0), 0,
			testContractA, "0x"+strings.Repeat("00", 32), topic1, topic2,
			common.BigToHash(big.NewInt(1)).Hex()); err != nil {
			t.Fatalf("seed invalid transfer row: %v", err)
		}
		err := depositRunLoopOnce(t, depositITScanner(t, pool, cfg), depositITLease(t, pool, chainID))
		var pe *depositParseError
		if !errors.As(err, &pe) {
			t.Fatalf("ServeLoop() = %v (%T), want *depositParseError", err, err)
		}
		switch pe.class {
		case "bad_address", "bad_topics", "bad_amount", "out_of_range", "missing_field":
		default:
			t.Fatalf("parse class = %q, want a Table 3 validation class", pe.class)
		}
		row, ok := depositReadPause(t, ctx, pool, chainID)
		if !ok {
			t.Fatal("no deposit_pause row after the validation stop")
		}
		if row.kind != "validation_failed" || row.height != 15 || row.rev != 1 {
			t.Fatalf("pause = %+v, want validation_failed at 15 rev 1", row)
		}
		for _, want := range []string{"class=" + pe.class, "unit=10-20", "version=0"} {
			if !strings.Contains(row.detail, want) {
				t.Fatalf("pause detail %q misses %q", row.detail, want)
			}
		}
		assertNoProgress(t, chainID)
		if n := depositCountRows(t, ctx, pool, "deposit_observations", chainID); n != 0 {
			t.Fatalf("observations = %d, want 0", n)
		}
	})

	t.Run("conflict_writes_pause_with_version", func(t *testing.T) {
		const chainID = 163
		cfg := depositITConfig(t, chainID, testContractA)
		depositSeedCanonical(t, ctx, pool, chainID, 10, 20, true)
		depositSeedUpstream(t, ctx, pool, chainID, 0, cfg.LogConfigHash, 21)
		depositSeedCheckpoint(t, ctx, pool, chainID, 10, cfg.ConfigHash, 10)
		depositSeedHistory(t, ctx, pool, chainID, 1, 10, cfg.ConfigHash)
		depositSeedTransferRow(t, ctx, pool, chainID, 15, depositBlockHash(15), depositTxHash(15, 0), 0,
			common.HexToAddress(testContractA), common.HexToAddress(testContractB), common.HexToAddress(depositWatchAddr), big.NewInt(1))
		depositSeedObservation(t, ctx, pool, chainID, 15, depositBlockHash(15), depositTxHash(15, 0), 0, "99", 1)
		err := depositRunLoopOnce(t, depositITScanner(t, pool, cfg), depositITLease(t, pool, chainID))
		var conflict *depositIdentityConflictError
		if !errors.As(err, &conflict) {
			t.Fatalf("ServeLoop() = %v (%T), want *depositIdentityConflictError", err, err)
		}
		row, ok := depositReadPause(t, ctx, pool, chainID)
		if !ok {
			t.Fatal("no deposit_pause row after the conflict stop")
		}
		if row.kind != "validation_failed" || row.rev != 1 {
			t.Fatalf("pause = %+v, want validation_failed rev 1", row)
		}
		for _, want := range []string{"class=identity_conflict", "unit=10-20", "version=1"} {
			if !strings.Contains(row.detail, want) {
				t.Fatalf("pause detail %q misses %q (the locked current version)", row.detail, want)
			}
		}
		if _, _, next, _ := depositCheckpointState(t, ctx, pool, chainID); next != 10 {
			t.Fatalf("checkpoint next = %d, want 10 unchanged", next)
		}
		var amount string
		if err := pool.QueryRow(ctx, `SELECT amount::text FROM deposit_observations
WHERE chain_id = $1 AND block_number = 15`, chainID).Scan(&amount); err != nil || amount != "99" {
			t.Fatalf("conflicting observation = (%s,%v), want (99,nil) intact", amount, err)
		}
	})

	t.Run("drift_writes_no_pause", func(t *testing.T) {
		const chainID = 164
		cfg := depositITConfig(t, chainID, testContractA)
		depositSeedCanonical(t, ctx, pool, chainID, 10, 20, true)
		depositSeedUpstream(t, ctx, pool, chainID, 0, strings.Repeat("ff", 32), 21)
		err := depositRunLoopOnce(t, depositITScanner(t, pool, cfg), depositITLease(t, pool, chainID))
		var drift *upstreamDriftError
		if !errors.As(err, &drift) {
			t.Fatalf("ServeLoop() = %v (%T), want *upstreamDriftError", err, err)
		}
		if _, ok := depositReadPause(t, ctx, pool, chainID); ok {
			t.Fatal("pause row written for drift (operator domain stops bare)")
		}
		assertNoProgress(t, chainID)
	})

	t.Run("restart_keeps_pause", func(t *testing.T) {
		const chainID = 165
		cfg := depositITConfig(t, chainID, testContractA)
		depositSeedCanonical(t, ctx, pool, chainID, 10, 14, true)
		depositSeedCanonical(t, ctx, pool, chainID, 16, 20, true)
		depositSeedUpstream(t, ctx, pool, chainID, 0, cfg.LogConfigHash, 21)
		lease := depositITLease(t, pool, chainID)
		if err := depositRunLoopOnce(t, depositITScanner(t, pool, cfg), lease); !isChainView(err) {
			t.Fatalf("first run = %v, want a chain-view stop", err)
		}
		first, ok := depositReadPause(t, ctx, pool, chainID)
		if !ok {
			t.Fatal("no pause row after the first stop")
		}
		// Restart with a fresh scanner: the pause pre-check stops the loop
		// before any proof, and the row keeps its instance identity.
		if err := depositRunLoopOnce(t, depositITScanner(t, pool, cfg), lease); !isStreamPause(err) {
			t.Fatalf("second run = %v, want *streamPauseError (pause still effective)", err)
		}
		second, ok := depositReadPause(t, ctx, pool, chainID)
		if !ok || second != first {
			t.Fatalf("pause = %+v (ok=%v), want unchanged %+v", second, ok, first)
		}
		assertNoProgress(t, chainID)
	})
}

func isChainView(err error) bool {
	var cv *chainViewError
	return errors.As(err, &cv)
}

func isStreamPause(err error) bool {
	var paused *streamPauseError
	return errors.As(err, &paused)
}

// TestDepositPauseLayeredRecovery is T015 (US5) with a fixed batch (exactly 5
// runs, all recorded; any failure fails the batch): upstream-pause auto
// resume without 004 ever clearing an upstream row, manual instance release
// with a same-transaction audit and zero consumer writes, release-then-still
// broken rebuilds with a new instance identity, stale release fenced to zero
// rows with audit-lookup定性, re-release returning the original audit facts
// without rewriting, cross-version release audited at the current version,
// and a needs_006 pause holding across runs. Manual release is covered by
// ReleaseDepositPause; re-verification after release is the loop itself.
func TestDepositPauseLayeredRecovery(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	t.Run("upstream_auto_resumes", func(t *testing.T) {
		const chainID = 170
		cfg := depositITConfig(t, chainID, testContractA)
		cfg.PollInterval = 20 * time.Millisecond
		cfg.RetryInitial = 20 * time.Millisecond
		cfg.RetryMax = 100 * time.Millisecond
		depositSeedCanonical(t, ctx, pool, chainID, 10, 20, true)
		depositSeedUpstream(t, ctx, pool, chainID, 0, cfg.LogConfigHash, 21)
		depositSeedTransferRow(t, ctx, pool, chainID, 12, depositBlockHash(12), depositTxHash(12, 0), 0,
			common.HexToAddress(testContractA), common.HexToAddress(testContractB), common.HexToAddress(depositWatchAddr), big.NewInt(1))
		if _, err := pool.Exec(ctx, `
INSERT INTO log_pause (chain_id, height, kind, detail) VALUES ($1, 10, 'chain_view_changed', 'test')`, chainID); err != nil {
			t.Fatalf("seed log_pause: %v", err)
		}
		const decoy = 999
		if _, err := pool.Exec(ctx, `
INSERT INTO log_pause (chain_id, height, kind, detail) VALUES ($1, 10, 'chain_view_changed', 'decoy')`, decoy); err != nil {
			t.Fatalf("seed decoy log_pause: %v", err)
		}
		lease := depositITLease(t, pool, chainID)
		if err := depositRunLoopOnce(t, depositITScanner(t, pool, cfg), lease); !isStreamPause(err) {
			t.Fatalf("first run = %v, want *streamPauseError (upstream pause obeyed)", err)
		}
		if _, _, _, ok := depositCheckpointState(t, ctx, pool, chainID); ok {
			t.Fatal("checkpoint row created while paused")
		}
		// The upstream lifts its own row (004 never writes log_pause); the
		// next run continues from the original position with no 004-side
		// release step.
		if _, err := pool.Exec(ctx, `DELETE FROM log_pause WHERE chain_id = $1`, chainID); err != nil {
			t.Fatalf("lift upstream pause: %v", err)
		}
		stop := depositRunLoop(t, ctx, depositITScanner(t, pool, cfg), lease)
		waitUntil(t, time.Now().Add(10*time.Second), "post-pause next_block 21", func() bool {
			_, _, next, ok := depositCheckpointState(t, ctx, pool, chainID)
			return ok && next == 21
		})
		time.Sleep(100 * time.Millisecond)
		stop()
		if n := depositCountRows(t, ctx, pool, "deposit_observations", chainID); n != 1 {
			t.Fatalf("observations = %d, want 1 after auto-resume", n)
		}
		var detail string
		if err := pool.QueryRow(ctx, `SELECT detail FROM log_pause WHERE chain_id = $1`, decoy).Scan(&detail); err != nil || detail != "decoy" {
			t.Fatalf("decoy log_pause = (%q,%v), want untouched 'decoy'", detail, err)
		}
		if n := depositCountRows(t, ctx, pool, "deposit_pause", chainID); n != 0 {
			t.Fatalf("deposit_pause rows = %d, want 0 (upstream waits never persist)", n)
		}
	})

	t.Run("manual_release_with_audit", func(t *testing.T) {
		const chainID = 171
		cfg := depositITConfig(t, chainID, testContractA)
		depositSeedCanonical(t, ctx, pool, chainID, 10, 14, true)
		depositSeedCanonical(t, ctx, pool, chainID, 16, 20, true)
		depositSeedUpstream(t, ctx, pool, chainID, 0, cfg.LogConfigHash, 21)
		lease := depositITLease(t, pool, chainID)
		sc := depositITScanner(t, pool, cfg)
		if err := depositRunLoopOnce(t, sc, lease); !isChainView(err) {
			t.Fatalf("first run = %v, want a chain-view stop", err)
		}
		row, ok := depositReadPause(t, ctx, pool, chainID)
		if !ok {
			t.Fatal("no pause row after the stop")
		}
		released, err := sc.ReleaseDepositPause(ctx, lease, row.id, row.rev, "operator-r", "manual recovery")
		if err != nil || !released {
			t.Fatalf("release = (%v,%v), want (true,nil)", released, err)
		}
		if _, ok := depositReadPause(t, ctx, pool, chainID); ok {
			t.Fatal("pause row still present after release")
		}
		var action, operator, reason string
		var version int64
		if err := pool.QueryRow(ctx, `
SELECT action, operator, reason, version_seq FROM deposit_pause_audit
WHERE chain_id = $1 AND pause_id = $2 AND revision = $3`, chainID, row.id, row.rev).
			Scan(&action, &operator, &reason, &version); err != nil {
			t.Fatalf("read release audit: %v", err)
		}
		if action != "release" || operator != "operator-r" || reason != "manual recovery" || version != 0 {
			t.Fatalf("audit = (%s,%s,%s,%d), want (release,operator-r,manual recovery,0 pre-bootstrap)", action, operator, reason, version)
		}
		// Release writes no consumer state.
		if _, _, _, ok := depositCheckpointState(t, ctx, pool, chainID); ok {
			t.Fatal("checkpoint row created by a release (release is not authorization)")
		}
		if n := depositCountRows(t, ctx, pool, "deposit_config_history", chainID); n != 0 {
			t.Fatalf("history rows = %d, want 0 after a pure release", n)
		}
	})

	t.Run("release_then_reverify_fails_rebuilds", func(t *testing.T) {
		const chainID = 172
		cfg := depositITConfig(t, chainID, testContractA)
		cfg.PollInterval = 20 * time.Millisecond
		cfg.RetryInitial = 20 * time.Millisecond
		cfg.RetryMax = 100 * time.Millisecond
		depositSeedCanonical(t, ctx, pool, chainID, 10, 14, true)
		depositSeedCanonical(t, ctx, pool, chainID, 16, 20, true)
		depositSeedUpstream(t, ctx, pool, chainID, 0, cfg.LogConfigHash, 21)
		lease := depositITLease(t, pool, chainID)
		sc := depositITScanner(t, pool, cfg)
		if err := depositRunLoopOnce(t, sc, lease); !isChainView(err) {
			t.Fatalf("first run = %v, want a chain-view stop", err)
		}
		p1, ok := depositReadPause(t, ctx, pool, chainID)
		if !ok {
			t.Fatal("no pause row after the first stop")
		}
		if released, err := sc.ReleaseDepositPause(ctx, lease, p1.id, p1.rev, "op", "why"); err != nil || !released {
			t.Fatalf("release = (%v,%v), want (true,nil)", released, err)
		}
		// The evidence is still broken: the loop rebuilds with a NEW
		// instance identity and stays stopped.
		if err := depositRunLoopOnce(t, depositITScanner(t, pool, cfg), lease); !isChainView(err) {
			t.Fatalf("second run = %v, want a chain-view stop again", err)
		}
		p2, ok := depositReadPause(t, ctx, pool, chainID)
		if !ok {
			t.Fatal("no rebuilt pause row")
		}
		if p2.id == p1.id {
			t.Fatalf("rebuilt pause id = %d, want a new instance (sequence never reused)", p2.id)
		}
		if p2.rev != 1 {
			t.Fatalf("rebuilt pause rev = %d, want 1", p2.rev)
		}
		// Repair the evidence and release the rebuilt instance: the loop
		// recovers from the original position.
		depositSeedCanonical(t, ctx, pool, chainID, 15, 15, true)
		if released, err := sc.ReleaseDepositPause(ctx, lease, p2.id, p2.rev, "op", "repaired"); err != nil || !released {
			t.Fatalf("release P2 = (%v,%v), want (true,nil)", released, err)
		}
		stop := depositRunLoop(t, ctx, depositITScanner(t, pool, cfg), lease)
		waitUntil(t, time.Now().Add(10*time.Second), "repaired next_block 21", func() bool {
			_, _, next, ok := depositCheckpointState(t, ctx, pool, chainID)
			return ok && next == 21
		})
		time.Sleep(100 * time.Millisecond)
		stop()
	})

	t.Run("stale_release_zero_rows", func(t *testing.T) {
		const chainID = 173
		cfg := depositITConfig(t, chainID, testContractA)
		depositSeedCanonical(t, ctx, pool, chainID, 10, 14, true)
		depositSeedCanonical(t, ctx, pool, chainID, 16, 20, true)
		depositSeedUpstream(t, ctx, pool, chainID, 0, cfg.LogConfigHash, 21)
		lease := depositITLease(t, pool, chainID)
		sc := depositITScanner(t, pool, cfg)
		if err := depositRunLoopOnce(t, sc, lease); !isChainView(err) {
			t.Fatalf("first run = %v, want a chain-view stop", err)
		}
		p1, _ := depositReadPause(t, ctx, pool, chainID)
		if released, err := sc.ReleaseDepositPause(ctx, lease, p1.id, p1.rev, "op", "why"); err != nil || !released {
			t.Fatalf("release P1 = (%v,%v), want (true,nil)", released, err)
		}
		p2id, p2rev := depositAuthSeedPause(t, ctx, pool, chainID, "upstream_gap", "class=structural gap=10-14 cause=below_upstream_start config=x", 10)
		if p2id == p1.id {
			t.Fatalf("P2 id = %d, want a never-reused identity", p2id)
		}
		before := depositAuthStateOf(t, ctx, pool, chainID)
		released, err := sc.ReleaseDepositPause(ctx, lease, p1.id, p1.rev, "op-late", "stale")
		if err != nil || released {
			t.Fatalf("stale release = (%v,%v), want (false,nil) with zero rows", released, err)
		}
		var one int
		if err := pool.QueryRow(ctx, `
SELECT 1 FROM deposit_pause_audit WHERE chain_id = $1 AND pause_id = $2 AND action = 'release'`,
			chainID, p2id).Scan(&one); !errors.Is(err, pgx.ErrNoRows) {
			t.Fatalf("P2 audit lookup = %v, want a miss (stale定性, no release record)", err)
		}
		live, ok := depositReadPause(t, ctx, pool, chainID)
		if !ok || live.id != p2id || live.rev != p2rev {
			t.Fatalf("live pause = %+v (ok=%v), want untouched P2 (%d,%d)", live, ok, p2id, p2rev)
		}
		if after := depositAuthStateOf(t, ctx, pool, chainID); after.history != before.history || after.obs != before.obs {
			t.Fatalf("state changed by the stale release: %+v -> %+v", before, after)
		}
	})

	t.Run("rerelease_returns_original_facts", func(t *testing.T) {
		const chainID = 174
		cfg := depositITConfig(t, chainID, testContractA)
		depositSeedCanonical(t, ctx, pool, chainID, 10, 14, true)
		depositSeedCanonical(t, ctx, pool, chainID, 16, 20, true)
		depositSeedUpstream(t, ctx, pool, chainID, 0, cfg.LogConfigHash, 21)
		lease := depositITLease(t, pool, chainID)
		sc := depositITScanner(t, pool, cfg)
		if err := depositRunLoopOnce(t, sc, lease); !isChainView(err) {
			t.Fatalf("first run = %v, want a chain-view stop", err)
		}
		p1, _ := depositReadPause(t, ctx, pool, chainID)
		if released, err := sc.ReleaseDepositPause(ctx, lease, p1.id, p1.rev, "operator-a", "reason-a"); err != nil || !released {
			t.Fatalf("release P1 = (%v,%v), want (true,nil)", released, err)
		}
		p2id, _ := depositAuthSeedPause(t, ctx, pool, chainID, "upstream_gap", "gap-test", 10)
		// Releasing the old identity again: zero rows, and the audit lookup
		// returns the ORIGINAL facts without rewriting or touching P2.
		released, err := sc.ReleaseDepositPause(ctx, lease, p1.id, p1.rev, "operator-b", "reason-b")
		if err != nil || released {
			t.Fatalf("re-release = (%v,%v), want (false,nil)", released, err)
		}
		var operator, reason string
		var at time.Time
		if err := pool.QueryRow(ctx, `
SELECT operator, reason, at FROM deposit_pause_audit
WHERE chain_id = $1 AND pause_id = $2 AND revision = $3`, chainID, p1.id, p1.rev).
			Scan(&operator, &reason, &at); err != nil {
			t.Fatalf("original audit lookup: %v", err)
		}
		if operator != "operator-a" || reason != "reason-a" {
			t.Fatalf("audit facts = (%s,%s), want the original (operator-a,reason-a), not the late caller", operator, reason)
		}
		var releases int
		if err := pool.QueryRow(ctx, `
SELECT count(*) FROM deposit_pause_audit WHERE chain_id = $1 AND pause_id = $2 AND action = 'release'`,
			chainID, p1.id).Scan(&releases); err != nil || releases != 1 {
			t.Fatalf("release rows for P1 = (%d,%v), want exactly 1 (no rewrite)", releases, err)
		}
		live, _ := depositReadPause(t, ctx, pool, chainID)
		if live.id != p2id {
			t.Fatalf("live pause = %+v, want untouched P2 (%d)", live, p2id)
		}
	})

	t.Run("cross_version_release", func(t *testing.T) {
		const chainID = 175
		h1 := depositAuthHash(t, 10, depositAuthLine(testContractA, 10), depositAuthLine(depositWatchAddr, 10))
		cfg := depositITConfig(t, chainID, testContractA, testContractB)
		cfg.PollInterval = 20 * time.Millisecond
		cfg.RetryInitial = 20 * time.Millisecond
		cfg.RetryMax = 100 * time.Millisecond
		cfg.ConfigHash = h1
		depositSeedCanonical(t, ctx, pool, chainID, 10, 20, true)
		depositSeedUpstream(t, ctx, pool, chainID, 0, cfg.LogConfigHash, 21)
		depositSeedTransferRow(t, ctx, pool, chainID, 15, depositBlockHash(15), depositTxHash(15, 0), 0,
			common.HexToAddress(testContractA), common.HexToAddress(testContractB), common.HexToAddress(depositWatchAddr), big.NewInt(1))
		lease := depositITLease(t, pool, chainID)
		stop := depositRunLoop(t, ctx, depositITScanner(t, pool, cfg), lease)
		waitUntil(t, time.Now().Add(10*time.Second), "cross-version next_block 21", func() bool {
			_, _, next, ok := depositCheckpointState(t, ctx, pool, chainID)
			return ok && next == 21
		})
		time.Sleep(100 * time.Millisecond)
		stop()
		// A retained needs_006 pause tagged at version 1 survives two
		// authorizations, then releases by instance at the current version 3.
		pid, prev := depositAuthSeedPause(t, ctx, pool, chainID, "upstream_gap",
			"class=structural gap=10-14 cause=below_upstream_start config="+h1+" needs_006=true version=1", 10)
		watches := depositAuthLine(depositWatchAddr, 10)
		withB := depositAuthSnapshot(depositAuthLine(testContractA, 10), depositAuthLine(testContractB, 10))
		add := depositAuthBaseReq(t, chainID, "xv-add", 1)
		add.NewConfigHash, add.NewStartBlock, add.NewAssets, add.NewWatches =
			depositAuthHash(t, 10, withB, watches), 10, withB, watches
		if _, err := AuthorizeDepositConfig(ctx, pool, add); err != nil {
			t.Fatalf("auth v2: %v", err)
		}
		postponed := depositAuthSnapshot(depositAuthLine(testContractA, 15))
		adv := depositAuthBaseReq(t, chainID, "xv-postpone", 2)
		adv.NewConfigHash, adv.NewStartBlock, adv.NewAssets, adv.NewWatches =
			depositAuthHash(t, 10, postponed, watches), 10, postponed, watches
		if _, err := AuthorizeDepositConfig(ctx, pool, adv); err != nil {
			t.Fatalf("auth v3: %v", err)
		}
		live, ok := depositReadPause(t, ctx, pool, chainID)
		if !ok || live.id != pid || live.rev != prev {
			t.Fatalf("retained pause = %+v (ok=%v), want untouched (%d,%d)", live, ok, pid, prev)
		}
		sc := depositITScanner(t, pool, cfg)
		released, err := sc.ReleaseDepositPause(ctx, lease, pid, prev, "operator-xv", "recovered")
		if err != nil || !released {
			t.Fatalf("cross-version release = (%v,%v), want (true,nil)", released, err)
		}
		var version int64
		if err := pool.QueryRow(ctx, `
SELECT version_seq FROM deposit_pause_audit WHERE chain_id = $1 AND pause_id = $2 AND revision = $3`,
			chainID, pid, prev).Scan(&version); err != nil || version != 3 {
			t.Fatalf("release audit version = (%d,%v), want 3 (locked current, tag was 1)", version, err)
		}
		// Re-verification under the current version replays convergently.
		cfg3 := cfg
		cfg3.Assets = []config.DepositEntry{{Address: testContractA, Effective: 15}}
		cfg3.ConfigHash = adv.NewConfigHash
		stop3 := depositRunLoop(t, ctx, depositITScanner(t, pool, cfg3), lease)
		waitUntil(t, time.Now().Add(10*time.Second), "cross-version replay next_block 21", func() bool {
			_, _, next, ok := depositCheckpointState(t, ctx, pool, chainID)
			return ok && next == 21
		})
		time.Sleep(100 * time.Millisecond)
		stop3()
		if n := depositCountRows(t, ctx, pool, "deposit_observations", chainID); n != 1 {
			t.Fatalf("observations = %d, want 1 converged at version 1", n)
		}
		var kept int64
		if err := pool.QueryRow(ctx, `SELECT version_seq FROM deposit_observations WHERE chain_id = $1`,
			chainID).Scan(&kept); err != nil || kept != 1 {
			t.Fatalf("observation version = (%d,%v), want 1 kept", kept, err)
		}
	})

	t.Run("needs_006_holds_pause", func(t *testing.T) {
		const chainID = 176
		cfg := depositITConfig(t, chainID, testContractA)
		depositSeedCanonical(t, ctx, pool, chainID, 10, 20, true)
		depositSeedUpstream(t, ctx, pool, chainID, 0, cfg.LogConfigHash, 21)
		detail := "class=structural gap=10-14 cause=below_upstream_start config=" + cfg.ConfigHash + " needs_006=true version=0"
		pid, prev := depositAuthSeedPause(t, ctx, pool, chainID, "upstream_gap", detail, 10)
		lease := depositITLease(t, pool, chainID)
		for i := 0; i < 3; i++ {
			if err := depositRunLoopOnce(t, depositITScanner(t, pool, cfg), lease); !isStreamPause(err) {
				t.Fatalf("run %d = %v, want *streamPauseError (needs_006 holds)", i, err)
			}
		}
		live, ok := depositReadPause(t, ctx, pool, chainID)
		if !ok || live.id != pid || live.rev != prev || live.detail != detail {
			t.Fatalf("pause = %+v (ok=%v), want byte-identical hold %q", live, ok, detail)
		}
		var held int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM deposit_pause WHERE chain_id = $1 AND detail LIKE '%needs_006=true%'`,
			chainID).Scan(&held); err != nil || held != 1 {
			t.Fatalf("needs_006 rows = (%d,%v), want 1 queryable", held, err)
		}
		if _, _, _, ok := depositCheckpointState(t, ctx, pool, chainID); ok {
			t.Fatal("checkpoint row created while needs_006 holds")
		}
	})
}

// TestDepositPauseConcurrency is T016 (US5) with a fixed batch (exactly 5
// runs, all recorded; any failure fails the batch): concurrent pause writers
// converge on exactly one row (first wins), moved progress, vanished
// first-unit state and a dispossessed lease all abandon with zero writes, and
// a rolled-back batch transaction itself never writes a pause row (the single
// row always comes from the Table 3 pause transaction).
func TestDepositPauseConcurrency(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	pauseEvidence := &pauseEvidence{kind: "upstream_gap", height: 10,
		detail: "class=structural gap=10-14 cause=below_upstream_start config=x"}

	t.Run("concurrent_first_wins_one_row", func(t *testing.T) {
		const chainID = 180
		cfg := depositITConfig(t, chainID, testContractA)
		sc := depositITScanner(t, pool, cfg)
		lease := depositITLease(t, pool, chainID)
		const writers = 8
		var wg sync.WaitGroup
		for i := 0; i < writers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				// First-unit basis (nil progress): all writers race the same
				// empty state; only the outcome converges, never the row.
				sc.tryPersistPause(ctx, lease, pauseEvidence, nil)
			}()
		}
		wg.Wait()
		row, ok := depositReadPause(t, ctx, pool, chainID)
		if !ok {
			t.Fatal("no pause row after the concurrent race")
		}
		if row.rev != 1 || row.kind != "upstream_gap" || row.height != 10 {
			t.Fatalf("pause = %+v, want a single first-wins row", row)
		}
		if n := depositCountRows(t, ctx, pool, "deposit_pause", chainID); n != 1 {
			t.Fatalf("pause rows = %d, want exactly 1", n)
		}
	})

	t.Run("moved_progress_abandons", func(t *testing.T) {
		const chainID = 181
		cfg := depositITConfig(t, chainID, testContractA)
		depositSeedCheckpoint(t, ctx, pool, chainID, 10, cfg.ConfigHash, 15)
		depositSeedHistory(t, ctx, pool, chainID, 1, 10, cfg.ConfigHash)
		sc := depositITScanner(t, pool, cfg)
		lease := depositITLease(t, pool, chainID)
		stale := &depositProgress{startBlock: 10, configHash: cfg.ConfigHash, nextBlock: 10, versionSeq: 1}
		if !sc.tryPersistPause(ctx, lease, pauseEvidence, stale) {
			t.Fatal("tryPersistPause asked for a retry on converged evidence")
		}
		if _, ok := depositReadPause(t, ctx, pool, chainID); ok {
			t.Fatal("pause row written on moved progress (evidence expired)")
		}
		// First-unit basis against existing progress is equally stale.
		if !sc.tryPersistPause(ctx, lease, pauseEvidence, nil) {
			t.Fatal("tryPersistPause asked for a retry on vanished empty basis")
		}
		if _, ok := depositReadPause(t, ctx, pool, chainID); ok {
			t.Fatal("pause row written on a vanished empty basis")
		}
	})

	t.Run("dispossessed_lease_abandons", func(t *testing.T) {
		const chainID = 182
		cfg := depositITConfig(t, chainID, testContractA)
		sc := depositITScanner(t, pool, cfg)
		staleLease := depositITLease(t, pool, chainID)
		// Expire the first lease on the DB clock so the takeover wins
		// deterministically (no sleep guessing).
		if _, err := pool.Exec(ctx, `UPDATE indexer_lease SET expires_at = now() - make_interval(secs => 1) WHERE chain_id = $1`, chainID); err != nil {
			t.Fatalf("expire first lease: %v", err)
		}
		takeover := newTestLease(t, pool, chainID, "takeover-owner", time.Minute, 10*time.Second)
		won, _, err := takeover.Acquire(context.Background())
		if err != nil || !won {
			t.Fatalf("takeover Acquire() = (%v,%v), want a win", won, err)
		}
		if !sc.tryPersistPause(ctx, staleLease, pauseEvidence, nil) {
			t.Fatal("tryPersistPause asked for a retry on a lost lease")
		}
		if _, ok := depositReadPause(t, ctx, pool, chainID); ok {
			t.Fatal("pause row written by a dispossessed lease")
		}
	})

	t.Run("batch_rollback_writes_no_pause", func(t *testing.T) {
		const chainID = 183
		cfg := depositITConfig(t, chainID, testContractA)
		depositSeedCanonical(t, ctx, pool, chainID, 10, 20, true)
		depositSeedUpstream(t, ctx, pool, chainID, 0, cfg.LogConfigHash, 21)
		depositSeedCheckpoint(t, ctx, pool, chainID, 10, cfg.ConfigHash, 10)
		depositSeedHistory(t, ctx, pool, chainID, 1, 10, cfg.ConfigHash)
		depositSeedTransferRow(t, ctx, pool, chainID, 15, depositBlockHash(15), depositTxHash(15, 0), 0,
			common.HexToAddress(testContractA), common.HexToAddress(testContractB), common.HexToAddress(depositWatchAddr), big.NewInt(1))
		sc := depositITScanner(t, pool, cfg)
		lease := depositITLease(t, pool, chainID)
		unit, batch, captured := depositITPrepareUnit(t, ctx, sc, 10, 20)
		if _, err := pool.Exec(ctx, `UPDATE chain_blocks SET canonical = FALSE WHERE chain_id = $1 AND number = 15`, chainID); err != nil {
			t.Fatalf("flip canonical: %v", err)
		}
		// The batch transaction itself rolls back with zero pause writes.
		if err := sc.commitDepositUnit(ctx, lease, unit, batch, captured, 10, 20); !isChainView(err) {
			t.Fatalf("commit = %v, want a chain-view abort", err)
		}
		if _, ok := depositReadPause(t, ctx, pool, chainID); ok {
			t.Fatal("pause row written by the rolled-back batch transaction")
		}
		if n := depositCountRows(t, ctx, pool, "deposit_observations", chainID); n != 0 {
			t.Fatalf("observations = %d, want 0 after the rollback", n)
		}
		// The loop path persists exactly one row from the pause transaction.
		if err := depositRunLoopOnce(t, depositITScanner(t, pool, cfg), lease); !isChainView(err) {
			t.Fatalf("loop = %v, want a chain-view stop", err)
		}
		row, ok := depositReadPause(t, ctx, pool, chainID)
		if !ok || row.kind != "chain_view_changed" || row.rev != 1 {
			t.Fatalf("pause = %+v (ok=%v), want the single pause-transaction row", row, ok)
		}
		if n := depositCountRows(t, ctx, pool, "deposit_pause", chainID); n != 1 {
			t.Fatalf("pause rows = %d, want exactly 1", n)
		}
	})
}

// TestDepositDualWorkers is T017 (US5) with a fixed batch (exactly 5 runs,
// all recorded; any failure fails the batch): two genuine workers (separate
// pools, separate lease handles) race the same first unit and exactly one
// effective advance lands; a dispossessed worker's delayed commit and a stale
// basis resubmission both write zero rows. Final state is always asserted
// across workers (reads through the other pool), never by -race alone.
func TestDepositDualWorkers(t *testing.T) {
	dsn := startIndexerPostgres(t)
	poolA := openIndexerPool(t, dsn)
	defer poolA.Close()
	poolB := openIndexerPool(t, dsn)
	defer poolB.Close()
	ctx := context.Background()

	// seedFirstUnit plants the covered first unit (canonical 10..20, upstream
	// next 21 over {A}, one matched transfer at 15) on the given pool.
	// seedFirstUnit plants the covered first unit (canonical 10..20, upstream
	// next 21 over {A}, one matched transfer at 15) on the given pool.
	seedFirstUnit := func(t *testing.T, pool *pgxpool.Pool, chainID int64) {
		t.Helper()
		depositSeedCanonical(t, ctx, pool, chainID, 10, 20, true)
		depositSeedUpstream(t, ctx, pool, chainID, 0, depositUpstreamHash(t, testContractA), 21)
		depositSeedTransferRow(t, ctx, pool, chainID, 15, depositBlockHash(15), depositTxHash(15, 0), 0,
			common.HexToAddress(testContractA), common.HexToAddress(testContractB), common.HexToAddress(depositWatchAddr), big.NewInt(1))
	}
	// depositUpstreamHash is keyed by the whitelist alone; the scanner config
	// carries the same hash for LogContracts {A}.
	_ = seedFirstUnit

	t.Run("concurrent_first_unit_exactly_once", func(t *testing.T) {
		const chainID = 190
		seedFirstUnit(t, poolA, chainID)
		cfgA := depositITConfig(t, chainID, testContractA)
		cfgB := depositITConfig(t, chainID, testContractA)
		scA := depositITScanner(t, poolA, cfgA)
		scB := depositITScanner(t, poolB, cfgB)
		leaseA := depositITLease(t, poolA, chainID)
		leaseB, err := NewLease(poolB, chainID, Params{OwnerID: "worker-b", TTL: time.Minute, Heartbeat: 10 * time.Second})
		if err != nil {
			t.Fatalf("NewLease(worker-b): %v", err)
		}
		unitA, batchA, capturedA := depositITPrepareUnit(t, ctx, scA, 10, 20)
		unitB, batchB, capturedB := depositITPrepareUnit(t, ctx, scB, 10, 20)
		gate := make(chan struct{})
		var wg sync.WaitGroup
		errs := make([]error, 2)
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-gate
			errs[0] = scA.commitDepositUnit(ctx, leaseA, unitA, batchA, capturedA, 10, 20)
		}()
		go func() {
			defer wg.Done()
			<-gate
			// Worker B never acquired: it races only with what it captured.
			// Either it wins the bootstrap or it loses with zero writes.
			errs[1] = scB.commitDepositUnit(ctx, leaseB, unitB, batchB, capturedB, 10, 20)
		}()
		close(gate)
		wg.Wait()
		// Exactly one worker advanced; the loser wrote nothing. Reads go
		// through the other pool (cross-worker database assertions).
		wins := 0
		for i, err := range errs {
			if err == nil {
				wins++
				continue
			}
			if !errors.Is(err, ErrLeaseLost) && !errors.Is(err, errStaleState) &&
				!errors.Is(err, errDepositVersionMismatch) {
				t.Fatalf("worker %d error = %v, want nil or a fenced/stale refusal", i, err)
			}
		}
		if wins != 1 {
			t.Fatalf("winners = %d (%v), want exactly 1", wins, errs)
		}
		if _, _, next, ok := depositCheckpointState(t, ctx, poolB, chainID); !ok || next != 21 {
			t.Fatalf("checkpoint via poolB = %d (ok=%v), want exactly 21", next, ok)
		}
		if n := depositCountRows(t, ctx, poolB, "deposit_observations", chainID); n != 1 {
			t.Fatalf("observations via poolB = %d, want exactly 1", n)
		}
		if n := depositCountRows(t, ctx, poolA, "deposit_config_history", chainID); n != 1 {
			t.Fatalf("history rows via poolA = %d, want exactly the bootstrap row", n)
		}
	})

	t.Run("delayed_commit_fenced", func(t *testing.T) {
		const chainID = 191
		seedFirstUnit(t, poolA, chainID)
		cfg := depositITConfig(t, chainID, testContractA)
		scA := depositITScanner(t, poolA, cfg)
		scB := depositITScanner(t, poolB, cfg)
		leaseA := depositITLease(t, poolA, chainID)
		unitA, batchA, capturedA := depositITPrepareUnit(t, ctx, scA, 10, 20)
		// Worker B takes over while A holds a prepared unit, then commits.
		if _, err := poolA.Exec(ctx, `UPDATE indexer_lease SET expires_at = now() - make_interval(secs => 1) WHERE chain_id = $1`, chainID); err != nil {
			t.Fatalf("expire worker A: %v", err)
		}
		leaseB := depositITLease(t, poolB, chainID)
		unitB, batchB, capturedB := depositITPrepareUnit(t, ctx, scB, 10, 20)
		if err := scB.commitDepositUnit(ctx, leaseB, unitB, batchB, capturedB, 10, 20); err != nil {
			t.Fatalf("worker B commit: %v", err)
		}
		// A's delayed commit presents an old token against B's row: fenced
		// with zero writes.
		if err := scA.commitDepositUnit(ctx, leaseA, unitA, batchA, capturedA, 10, 20); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("delayed commit = %v, want ErrLeaseLost", err)
		}
		if _, _, next, ok := depositCheckpointState(t, ctx, poolB, chainID); !ok || next != 21 {
			t.Fatalf("checkpoint via poolB = %d (ok=%v), want 21 from worker B only", next, ok)
		}
		if n := depositCountRows(t, ctx, poolB, "deposit_observations", chainID); n != 1 {
			t.Fatalf("observations via poolB = %d, want 1", n)
		}
	})

	t.Run("stale_basis_resubmission", func(t *testing.T) {
		const chainID = 192
		seedFirstUnit(t, poolA, chainID)
		cfg := depositITConfig(t, chainID, testContractA)
		sc := depositITScanner(t, poolA, cfg)
		lease := depositITLease(t, poolA, chainID)
		unit, batch, captured := depositITPrepareUnit(t, ctx, sc, 10, 20)
		if err := sc.commitDepositUnit(ctx, lease, unit, batch, captured, 10, 20); err != nil {
			t.Fatalf("first commit: %v", err)
		}
		// The same prepared triple resubmitted against moved progress is
		// refused by version isolation with zero new writes.
		if err := sc.commitDepositUnit(ctx, lease, unit, batch, captured, 10, 20); !errors.Is(err, errDepositVersionMismatch) {
			t.Fatalf("resubmission = %v, want errDepositVersionMismatch", err)
		}
		if n := depositCountRows(t, ctx, poolB, "deposit_observations", chainID); n != 1 {
			t.Fatalf("observations via poolB = %d, want 1", n)
		}
		if n := depositCountRows(t, ctx, poolB, "deposit_config_history", chainID); n != 1 {
			t.Fatalf("history via poolB = %d, want 1", n)
		}
	})
}
