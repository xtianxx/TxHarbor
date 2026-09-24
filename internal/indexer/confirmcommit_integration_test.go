//go:build integration

// T010 commit-transaction tests on a real PostgreSQL: the happy-path
// conversion with full basis columns, the first-confirmation bootstrap, and
// every mismatch path rolling back with zero writes (observation still
// pending, no new policy row, no basis columns). The uncertain-COMMIT and
// lost-race recoveries converge by observation-PK re-read. Harness and seed
// helpers mirror internal/indexer/*_integration_test.go headers
// (testcontainers postgres:18 via startIndexerPostgres).
package indexer

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// confirmSeedPolicyRow plants one confirmation_policy_history row; nil prevSeq
// / requestID write SQL NULL (bootstrap shape), non-nil write values.
func confirmSeedPolicyRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID, seq, threshold int64, prevSeq any, operator string, requestID any) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
INSERT INTO confirmation_policy_history
    (chain_id, policy_seq, threshold, prev_seq, operator, request_id)
VALUES ($1, $2, $3, $4, $5, $6)`,
		chainID, seq, threshold, prevSeq, operator, requestID); err != nil {
		t.Fatalf("seed confirmation_policy_history (%d,%d): %v", chainID, seq, err)
	}
}

// confirmSeedPending plants one pending observation at height h with the
// canonical test hash, satisfying the 004 version_seq FK with a seq-1 history
// row. It returns the observation identity parts.
func confirmSeedPending(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, h uint64) (bh, txHash string) {
	t.Helper()
	bh, txHash = depositBlockHash(h), depositTxHash(h, 0)
	depositSeedHistory(t, ctx, pool, chainID, 1, 10, strings.Repeat("aa", 32))
	depositSeedObservation(t, ctx, pool, chainID, h, bh, txHash, 0, "1", 1)
	return bh, txHash
}

// confirmCommitter builds the T010 committer plus a real acquired lease.
func confirmCommitter(t *testing.T, pool *pgxpool.Pool, chainID int64, n uint64) (*ConfirmationCommitter, *Lease) {
	t.Helper()
	c, err := NewConfirmationCommitter(pool, ConfirmationConfig{ChainID: chainID, ThresholdN: n})
	if err != nil {
		t.Fatalf("NewConfirmationCommitter(): %v", err)
	}
	return c, depositITLease(t, pool, chainID)
}

// confirmReadBasis reads the observation row back: status plus the six
// conversion facts (confirmations as exact decimal text).
func confirmReadBasis(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, bh, txHash string) (status string, confirmedAtNull bool, tipNumber, threshold, policySeq int64, tipHash, confirmations string) {
	t.Helper()
	var confirmedAtNullScan bool
	err := pool.QueryRow(ctx, `
SELECT status, (confirmed_at IS NULL),
       COALESCE(confirm_tip_number, -1), COALESCE(confirm_tip_hash, ''),
       COALESCE(confirm_threshold, -1), COALESCE(confirmations::text, ''),
       COALESCE(confirm_policy_seq, -1)
FROM deposit_observations
WHERE chain_id = $1 AND block_hash = $2 AND tx_hash = $3 AND log_index = 0`,
		chainID, bh, txHash).Scan(
		&status, &confirmedAtNullScan, &tipNumber, &tipHash, &threshold, &confirmations, &policySeq)
	if err != nil {
		t.Fatalf("read observation basis: %v", err)
	}
	return status, confirmedAtNullScan, tipNumber, threshold, policySeq, tipHash, confirmations
}

// confirmAssertZeroWrite proves the mismatch rollback left nothing behind:
// the candidate is still pending with no conversion facts, and the policy
// table holds exactly wantPolicy rows.
func confirmAssertZeroWrite(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, bh, txHash string, wantPolicy int) {
	t.Helper()
	status, nullAt, tipN, thr, seq, tipH, conf := confirmReadBasis(t, ctx, pool, chainID, bh, txHash)
	if status != "pending" || !nullAt || tipN != -1 || thr != -1 || seq != -1 || tipH != "" || conf != "" {
		t.Fatalf("zero-write violated: status=%s nullAt=%v tip=%d/%q N=%d conf=%q seq=%d",
			status, nullAt, tipN, tipH, thr, conf, seq)
	}
	if n := depositCountRows(t, ctx, pool, "confirmation_policy_history", chainID); n != wantPolicy {
		t.Fatalf("confirmation_policy_history rows = %d, want %d (mismatch must write nothing)", n, wantPolicy)
	}
}

// TestConfirmCommitHappyPath: tip=109, h=100, N=10 converts exactly once with
// all five basis columns plus confirmed_at.
func TestConfirmCommitHappyPath(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	const chainID, h, tip, n = int64(71), uint64(100), uint64(109), uint64(10)
	depositSeedCanonical(t, ctx, pool, chainID, h, tip, true)
	bh, txHash := confirmSeedPending(t, ctx, pool, chainID, h)
	confirmSeedPolicyRow(t, ctx, pool, chainID, 1, int64(n), nil, "bootstrap", nil)

	c, lease := confirmCommitter(t, pool, chainID, n)
	rcap := testRecoveryCap(t, ctx, pool, chainID)
	basis := ConfirmBasis{BlockHash: bh, TxHash: txHash, Height: h,
		TipNumber: tip, TipHash: depositBlockHash(tip), PolicySeq: 1, ThresholdN: n}
	if err := c.ConfirmDepositUnit(ctx, lease, basis, rcap); err != nil {
		t.Fatalf("ConfirmDepositUnit(): %v", err)
	}

	status, nullAt, tipN, thr, seq, tipH, conf :=
		confirmReadBasis(t, ctx, pool, chainID, bh, txHash)
	if status != "confirmed" || nullAt {
		t.Fatalf("status=%s nullAt=%v, want confirmed/non-null confirmed_at", status, nullAt)
	}
	if tipN != int64(tip) || tipH != depositBlockHash(tip) || thr != int64(n) || seq != 1 || conf != "10" {
		t.Fatalf("basis = tip(%d %s) N=%d conf=%s seq=%d, want tip(109 %s) N=10 conf=10 seq=1",
			tipN, tipH, thr, conf, seq, depositBlockHash(tip))
	}
	if n := depositCountRows(t, ctx, pool, "confirmation_policy_history", chainID); n != 1 {
		t.Fatalf("policy rows = %d, want 1 (no bootstrap on the normal path)", n)
	}
}

// TestConfirmCommitBootstrap: zero policy rows -> the same transaction inserts
// (chain_id, 1, N, NULL, 'bootstrap', NULL) and converts.
func TestConfirmCommitBootstrap(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	const chainID, h, tip, n = int64(72), uint64(50), uint64(59), uint64(10)
	depositSeedCanonical(t, ctx, pool, chainID, h, tip, true)
	bh, txHash := confirmSeedPending(t, ctx, pool, chainID, h)

	c, lease := confirmCommitter(t, pool, chainID, n)
	rcap := testRecoveryCap(t, ctx, pool, chainID)
	basis := ConfirmBasis{BlockHash: bh, TxHash: txHash, Height: h,
		TipNumber: tip, TipHash: depositBlockHash(tip), PolicySeq: 1, ThresholdN: n}
	if err := c.ConfirmDepositUnit(ctx, lease, basis, rcap); err != nil {
		t.Fatalf("ConfirmDepositUnit() bootstrap: %v", err)
	}

	var seq, thr int64
	var op string
	var reqID *string
	err := pool.QueryRow(ctx, `
SELECT policy_seq, threshold, operator, request_id FROM confirmation_policy_history
WHERE chain_id = $1`, chainID).Scan(&seq, &thr, &op, &reqID)
	if err != nil {
		t.Fatalf("read bootstrap policy row: %v", err)
	}
	if seq != 1 || thr != int64(n) || op != "bootstrap" || reqID != nil {
		t.Fatalf("bootstrap row = (%d %d %q %v), want (1 10 bootstrap NULL)", seq, thr, op, reqID)
	}
	status, _, _, _, _, _, conf := confirmReadBasis(t, ctx, pool, chainID, bh, txHash)
	if status != "confirmed" || conf != "10" {
		t.Fatalf("status=%s conf=%s, want confirmed/10", status, conf)
	}
}

// TestConfirmCommitPolicyDrift: effective (2, 20) vs captured (1, 10) ->
// ConfirmationDriftError with zero writes (concurrency-timing case 3).
func TestConfirmCommitPolicyDrift(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	const chainID, h, tip, n = int64(73), uint64(100), uint64(109), uint64(10)
	depositSeedCanonical(t, ctx, pool, chainID, h, tip, true)
	bh, txHash := confirmSeedPending(t, ctx, pool, chainID, h)
	confirmSeedPolicyRow(t, ctx, pool, chainID, 1, int64(n), nil, "bootstrap", nil)
	confirmSeedPolicyRow(t, ctx, pool, chainID, 2, 20, 1, "operator", "req-1")

	c, lease := confirmCommitter(t, pool, chainID, n)
	rcap := testRecoveryCap(t, ctx, pool, chainID)
	basis := ConfirmBasis{BlockHash: bh, TxHash: txHash, Height: h,
		TipNumber: tip, TipHash: depositBlockHash(tip), PolicySeq: 1, ThresholdN: n}
	err := c.ConfirmDepositUnit(ctx, lease, basis, rcap)
	var drift *ConfirmationDriftError
	if !errors.As(err, &drift) {
		t.Fatalf("ConfirmDepositUnit() = %v (%T), want *ConfirmationDriftError", err, err)
	}
	confirmAssertZeroWrite(t, ctx, pool, chainID, bh, txHash, 2)
}

// TestConfirmCommitTipMismatch: the two tip-mismatch shapes are distinct
// (B10 defect fix; data-model §候选分类). A pure forward advance (case 1: the
// captured tip block is still canonical and only the canonical tip moved up)
// is the row-level selection race -> errStaleState with zero writes. A tip
// replacement (the captured tip row left the canonical chain) stays a
// chain-view refusal -> *ConfirmationChainViewError with zero writes.
func TestConfirmCommitTipMismatch(t *testing.T) {
	t.Run("pure advance is the row-level race", func(t *testing.T) {
		dsn := startIndexerPostgres(t)
		pool := openIndexerPool(t, dsn)
		defer pool.Close()
		ctx := context.Background()

		const chainID, h, tip, n = int64(74), uint64(100), uint64(109), uint64(10)
		depositSeedCanonical(t, ctx, pool, chainID, h, tip+1, true)
		bh, txHash := confirmSeedPending(t, ctx, pool, chainID, h)
		confirmSeedPolicyRow(t, ctx, pool, chainID, 1, int64(n), nil, "bootstrap", nil)

		c, lease := confirmCommitter(t, pool, chainID, n)
		rcap := testRecoveryCap(t, ctx, pool, chainID)
		basis := ConfirmBasis{BlockHash: bh, TxHash: txHash, Height: h,
			TipNumber: tip, TipHash: depositBlockHash(tip), PolicySeq: 1, ThresholdN: n}
		err := c.ConfirmDepositUnit(ctx, lease, basis, rcap)
		if !errors.Is(err, errStaleState) {
			t.Fatalf("ConfirmDepositUnit() = %v (%T), want errStaleState (pure advance is a retry, not a halt)", err, err)
		}
		confirmAssertZeroWrite(t, ctx, pool, chainID, bh, txHash, 1)
	})

	t.Run("tip replacement halts chain-view", func(t *testing.T) {
		dsn := startIndexerPostgres(t)
		pool := openIndexerPool(t, dsn)
		defer pool.Close()
		ctx := context.Background()

		const chainID, h, tip, n = int64(78), uint64(100), uint64(109), uint64(10)
		depositSeedCanonical(t, ctx, pool, chainID, h, tip, true)
		bh, txHash := confirmSeedPending(t, ctx, pool, chainID, h)
		confirmSeedPolicyRow(t, ctx, pool, chainID, 1, int64(n), nil, "bootstrap", nil)
		// The captured tip block leaves the canonical chain (same height,
		// different canonical hash): a chain-view anomaly, not a race.
		if _, err := pool.Exec(ctx,
			`UPDATE chain_blocks SET canonical = false WHERE chain_id = $1 AND number = $2`, chainID, int64(tip)); err != nil {
			t.Fatalf("de-canonicalize captured tip: %v", err)
		}
		if _, err := pool.Exec(ctx, `
INSERT INTO chain_blocks (chain_id, number, hash, parent_hash, canonical)
VALUES ($1, $2, $3, $4, true)`,
			chainID, int64(tip), depositBlockHash(tip+1000), depositBlockHash(tip-1)); err != nil {
			t.Fatalf("insert replacement tip: %v", err)
		}

		c, lease := confirmCommitter(t, pool, chainID, n)
		rcap := testRecoveryCap(t, ctx, pool, chainID)
		basis := ConfirmBasis{BlockHash: bh, TxHash: txHash, Height: h,
			TipNumber: tip, TipHash: depositBlockHash(tip), PolicySeq: 1, ThresholdN: n}
		err := c.ConfirmDepositUnit(ctx, lease, basis, rcap)
		var chainView *ConfirmationChainViewError
		if !errors.As(err, &chainView) {
			t.Fatalf("ConfirmDepositUnit() = %v (%T), want *ConfirmationChainViewError (tip replacement)", err, err)
		}
		confirmAssertZeroWrite(t, ctx, pool, chainID, bh, txHash, 1)
	})
}

// TestConfirmCommitPauseStops: a present deposit_pause row stops the commit
// with zero writes (the tx builds no pause row itself).
func TestConfirmCommitPauseStops(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	const chainID, h, tip, n = int64(75), uint64(100), uint64(109), uint64(10)
	depositSeedCanonical(t, ctx, pool, chainID, h, tip, true)
	bh, txHash := confirmSeedPending(t, ctx, pool, chainID, h)
	confirmSeedPolicyRow(t, ctx, pool, chainID, 1, int64(n), nil, "bootstrap", nil)
	if _, err := pool.Exec(ctx, `
INSERT INTO deposit_pause (chain_id, height, kind, detail) VALUES ($1, 10, 'upstream_gap', 'test')`,
		chainID); err != nil {
		t.Fatalf("seed deposit_pause: %v", err)
	}

	c, lease := confirmCommitter(t, pool, chainID, n)
	rcap := testRecoveryCap(t, ctx, pool, chainID)
	basis := ConfirmBasis{BlockHash: bh, TxHash: txHash, Height: h,
		TipNumber: tip, TipHash: depositBlockHash(tip), PolicySeq: 1, ThresholdN: n}
	err := c.ConfirmDepositUnit(ctx, lease, basis, rcap)
	var paused *streamPauseError
	if !errors.As(err, &paused) || paused.stream != "deposit_pause" {
		t.Fatalf("ConfirmDepositUnit() = %v (%T), want *streamPauseError for deposit_pause", err, err)
	}
	confirmAssertZeroWrite(t, ctx, pool, chainID, bh, txHash, 1)
	if n := depositCountRows(t, ctx, pool, "deposit_pause", chainID); n != 1 {
		t.Fatalf("deposit_pause rows = %d, want 1 (commit writes no pause row)", n)
	}
}

// TestConfirmCommitBelowDepth: 9 < N=10 re-computes false under the lock ->
// row-level wait refusal with zero writes.
func TestConfirmCommitBelowDepth(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	const chainID, h, tip, n = int64(76), uint64(100), uint64(108), uint64(10)
	depositSeedCanonical(t, ctx, pool, chainID, h, tip, true)
	bh, txHash := confirmSeedPending(t, ctx, pool, chainID, h)
	confirmSeedPolicyRow(t, ctx, pool, chainID, 1, int64(n), nil, "bootstrap", nil)

	c, lease := confirmCommitter(t, pool, chainID, n)
	rcap := testRecoveryCap(t, ctx, pool, chainID)
	basis := ConfirmBasis{BlockHash: bh, TxHash: txHash, Height: h,
		TipNumber: tip, TipHash: depositBlockHash(tip), PolicySeq: 1, ThresholdN: n}
	err := c.ConfirmDepositUnit(ctx, lease, basis, rcap)
	var chainView *ConfirmationChainViewError
	if !errors.As(err, &chainView) {
		t.Fatalf("ConfirmDepositUnit() = %v (%T), want *ConfirmationChainViewError (below depth)", err, err)
	}
	confirmAssertZeroWrite(t, ctx, pool, chainID, bh, txHash, 1)
}

// TestConfirmCommitReferenceDivergence: candidate bh != canonical hash at h
// (Edge-170) -> chain-view refusal with zero writes.
func TestConfirmCommitReferenceDivergence(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	const chainID, h, tip, n = int64(77), uint64(100), uint64(109), uint64(10)
	depositSeedCanonical(t, ctx, pool, chainID, h, tip, true)
	depositSeedHistory(t, ctx, pool, chainID, 1, 10, strings.Repeat("aa", 32))
	forged := "0x" + strings.Repeat("be", 32)
	depositSeedObservation(t, ctx, pool, chainID, h, forged, depositTxHash(h, 0), 0, "1", 1)
	confirmSeedPolicyRow(t, ctx, pool, chainID, 1, int64(n), nil, "bootstrap", nil)

	c, lease := confirmCommitter(t, pool, chainID, n)
	rcap := testRecoveryCap(t, ctx, pool, chainID)
	basis := ConfirmBasis{BlockHash: forged, TxHash: depositTxHash(h, 0), Height: h,
		TipNumber: tip, TipHash: depositBlockHash(tip), PolicySeq: 1, ThresholdN: n}
	err := c.ConfirmDepositUnit(ctx, lease, basis, rcap)
	var chainView *ConfirmationChainViewError
	if !errors.As(err, &chainView) {
		t.Fatalf("ConfirmDepositUnit() = %v (%T), want *ConfirmationChainViewError (hash divergence)", err, err)
	}
	confirmAssertZeroWrite(t, ctx, pool, chainID, forged, depositTxHash(h, 0), 1)
}

// TestConfirmCommitConvergesWhenAlreadyConfirmed: the winner's conversion is
// final (case 2) — a repeat commit returns nil and keeps the winner's basis.
func TestConfirmCommitConvergesWhenAlreadyConfirmed(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	const chainID, h, tip, n = int64(78), uint64(100), uint64(109), uint64(10)
	depositSeedCanonical(t, ctx, pool, chainID, h, tip, true)
	bh, txHash := confirmSeedPending(t, ctx, pool, chainID, h)
	confirmSeedPolicyRow(t, ctx, pool, chainID, 1, int64(n), nil, "bootstrap", nil)

	c, lease := confirmCommitter(t, pool, chainID, n)
	rcap := testRecoveryCap(t, ctx, pool, chainID)
	basis := ConfirmBasis{BlockHash: bh, TxHash: txHash, Height: h,
		TipNumber: tip, TipHash: depositBlockHash(tip), PolicySeq: 1, ThresholdN: n}
	if err := c.ConfirmDepositUnit(ctx, lease, basis, rcap); err != nil {
		t.Fatalf("first ConfirmDepositUnit(): %v", err)
	}
	if err := c.ConfirmDepositUnit(ctx, lease, basis, rcap); err != nil {
		t.Fatalf("repeat ConfirmDepositUnit() = %v, want nil (converge)", err)
	}
	status, _, tipN, _, seq, _, conf := confirmReadBasis(t, ctx, pool, chainID, bh, txHash)
	if status != "confirmed" || tipN != int64(tip) || seq != 1 || conf != "10" {
		t.Fatalf("winner basis rewritten: status=%s tip=%d conf=%s seq=%d", status, tipN, conf, seq)
	}
}

// TestConfirmCommitBootstrapConflict: the bootstrap INSERT lost a concurrent
// double-first-write. Same winner N -> stale (retry takes the normal path);
// divergent winner N -> drift (never silently follow).
func TestConfirmCommitBootstrapConflict(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	const chainID, n = int64(79), uint64(10)
	c, _ := confirmCommitter(t, pool, chainID, n)
	basis := ConfirmBasis{PolicySeq: 1, ThresholdN: n}

	confirmSeedPolicyRow(t, ctx, pool, chainID, 1, int64(n), nil, "bootstrap", nil)
	if err := c.resolveBootstrapConflict(ctx, basis); !errors.Is(err, errStaleState) {
		t.Fatalf("resolveBootstrapConflict() same-N = %v, want errStaleState", err)
	}

	const chainID2 = int64(80)
	c2, _ := confirmCommitter(t, pool, chainID2, n)
	confirmSeedPolicyRow(t, ctx, pool, chainID2, 1, 20, nil, "bootstrap", nil)
	var drift *ConfirmationDriftError
	if err := c2.resolveBootstrapConflict(ctx, basis); !errors.As(err, &drift) {
		t.Fatalf("resolveBootstrapConflict() divergent-N = %v (%T), want *ConfirmationDriftError", err, err)
	}
}
