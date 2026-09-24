//go:build integration

// B10 defect fix regression (013 T077 benchmark; specs/005-confirmation-tracking
// data-model.md §候选分类 "行级等待：选中后 tip 推进/重算不足。纯竞态，下 tick
// 自动重估"): a PURE forward tip advance after selection — the captured tip
// block is still canonical and only a new canonical block was appended on top —
// must stay the row-level race: the commit refuses with zero writes and the
// scanner re-captures on the fresh basis, never a whole-loop halt.
//
// The existing T026 race tests cover the reorg-shaped advance (the old tip row
// is de-canonicalized -> tip_untrusted halt); this test pins the benign
// advance distinction introduced by the B10 fix and proves the fresh-basis
// retry still converts exactly once.
package indexer

import (
	"context"
	"errors"
	"testing"
)

// TestConfirmationPureTipAdvanceRetriesWithoutHalt injects a pure advance
// between the candidate read and the confirm commit (lock-wait sync, no
// sleeps): the racer appends canonical tip+1 WITHOUT de-canonicalizing the
// captured tip. The confirmer's post-lock reread must refuse with
// errStaleState (retry tick, zero writes) instead of a chain-view halt, and a
// fresh-basis commit on the advanced tip must land exactly once.
func TestConfirmationPureTipAdvanceRetriesWithoutHalt(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	const chainID, h, tip, n = int64(305), uint64(100), uint64(109), uint64(10)
	const newTip = tip + 1
	rcap := testRecoveryCap(t, ctx, pool, chainID)
	bh, txHash, basis, lease := race26SeedBase(t, ctx, pool, chainID, h, tip, n)
	hold := race26BeginHolder(t, ctx, pool, chainID)

	racerPool := race26SecondPool(t, ctx, dsn, "race26-tip-c-racer")
	confirmerPool := race26SecondPool(t, ctx, dsn, "race26-tip-c-conf")
	confirmer, err := NewConfirmationCommitter(confirmerPool, ConfirmationConfig{ChainID: chainID, ThresholdN: n})
	if err != nil {
		t.Fatalf("NewConfirmationCommitter(): %v", err)
	}

	type outcome struct{ err error }
	racerDone := make(chan outcome, 1)
	go func() {
		tx, err := racerPool.Begin(ctx)
		if err != nil {
			racerDone <- outcome{err: err}
			return
		}
		defer func() { _ = tx.Rollback(ctx) }()
		var (
			owner string
			token int64
			valid bool
		)
		if err := tx.QueryRow(ctx, lockCoordSQL, chainID).Scan(&owner, &token, &valid); err != nil {
			racerDone <- outcome{err: err}
			return
		}
		// Pure advance: the captured tip row (tip, depositBlockHash(tip))
		// stays canonical; only a new canonical block extends the chain.
		if _, err := tx.Exec(ctx, `
INSERT INTO chain_blocks (chain_id, number, hash, parent_hash, canonical)
VALUES ($1, $2, $3, $4, true)`,
			chainID, int64(newTip), depositBlockHash(newTip), depositBlockHash(tip)); err != nil {
			racerDone <- outcome{err: err}
			return
		}
		racerDone <- outcome{err: tx.Commit(ctx)}
	}()
	race26WaitBlocked(t, ctx, pool, "race26-tip-c-racer", "tip racer to park on the coordination lock")

	confDone := make(chan outcome, 1)
	go func() { confDone <- outcome{err: confirmer.ConfirmDepositUnit(ctx, lease, basis, rcap)} }()
	race26WaitBlocked(t, ctx, pool, "race26-tip-c-conf", "confirmer to park on the coordination lock")

	if err := hold.Rollback(ctx); err != nil {
		t.Fatalf("release holder: %v", err)
	}
	if got := <-racerDone; got.err != nil {
		t.Fatalf("tip racer commit: %v", got.err)
	}
	got := <-confDone
	if !errors.Is(got.err, errStaleState) {
		t.Fatalf("ConfirmDepositUnit() = %v (%T), want errStaleState (pure advance is the row-level race)", got.err, got.err)
	}
	if outcome, reason := classifyConfirmationOutcome(got.err); outcome != confirmRetryTick || reason != "" {
		t.Fatalf("classify(pure advance) = (%d, %q), want (%d, %q)", outcome, reason, confirmRetryTick, "")
	}
	confirmAssertZeroWrite(t, ctx, pool, chainID, bh, txHash, 1)

	// The scanner's next tick re-captures the advanced tip; the same
	// candidate must then convert exactly once on the fresh basis.
	fresh := basis
	fresh.TipNumber, fresh.TipHash = newTip, depositBlockHash(newTip)
	if err := confirmer.ConfirmDepositUnit(ctx, lease, fresh, rcap); err != nil {
		t.Fatalf("fresh-basis ConfirmDepositUnit(): %v", err)
	}
	status, _, tipN, _, _, _, conf := confirmReadBasis(t, ctx, pool, chainID, bh, txHash)
	if status != "confirmed" || tipN != int64(newTip) || conf != "11" {
		t.Fatalf("fresh commit = status %s tip %d conf %s, want confirmed/%d/11", status, tipN, conf, newTip)
	}
}
