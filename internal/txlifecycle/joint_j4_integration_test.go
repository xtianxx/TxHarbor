//go:build integration

package txlifecycle

import (
	"context"
	"errors"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/xtianxx/txharbor/internal/eth"
	"github.com/xtianxx/txharbor/internal/execution"
)

type scriptedReader struct {
	facts execution.LifecycleFacts
	err   error
}

func (s scriptedReader) Read(ctx context.Context, intentID string) (execution.LifecycleFacts, error) {
	return s.facts, s.err
}

// jointReorgRPC delegates to the real Anvil node but can serve a reorged
// receipt at a new block/hash (the canonical-view change a reorg produces),
// mirroring how the 010 V9 scenario models a reorg on a chain double.
type jointReorgRPC struct {
	real    *eth.Client
	mu      sync.Mutex
	receipt *types.Receipt
}

func (r *jointReorgRPC) setReceipt(rec *types.Receipt) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.receipt = rec
}

func (r *jointReorgRPC) SendSignedTransaction(ctx context.Context, raw []byte, expected common.Hash) (common.Hash, error) {
	return r.real.SendSignedTransaction(ctx, raw, expected)
}

func (r *jointReorgRPC) TransactionByHash(ctx context.Context, h common.Hash) (*types.Transaction, bool, error) {
	return r.real.TransactionByHash(ctx, h)
}

func (r *jointReorgRPC) TransactionReceipt(ctx context.Context, h common.Hash) (*types.Receipt, error) {
	r.mu.Lock()
	rec := r.receipt
	r.mu.Unlock()
	if rec != nil {
		return rec, nil
	}
	return r.real.TransactionReceipt(ctx, h)
}

func (r *jointReorgRPC) BlockNumber(ctx context.Context) (uint64, error) {
	r.mu.Lock()
	rec := r.receipt
	r.mu.Unlock()
	if rec != nil {
		return rec.BlockNumber.Uint64(), nil
	}
	return r.real.BlockNumber(ctx)
}

// TestJointJ4ReorgRevisionAndProjectionOrder is T049/J4: a joint reorg after
// confirmation revises 010's receipt/revision chain (orphaned then
// reconfirmed) without rebuilding the payment; 011's projection consumes the
// lifecycle revision in order (a stale revision never overwrites a newer one;
// an unconfirmable read is marked possibly stale).
func TestJointJ4ReorgRevisionAndProjectionOrder(t *testing.T) {
	ctx := context.Background()
	j := newJointEnv(t)
	jj := j.admit()
	j.installTransferEmit(1000)
	chain := &jointReorgRPC{real: j.eth}
	j.store.WithChain(chain, 15*time.Second)

	res, err := j.store.Send(ctx, &SendRequest{AttemptID: jj.attemptID, Kind: SendInitial, Claim: j.claim(jj)})
	if err != nil || res.Outcome != "accepted" {
		t.Fatalf("joint send = %+v %v", res, err)
	}
	j.waitMined(res.TxHash)
	j.seedChainTruth()
	if _, err := j.store.Reconcile(ctx, jj.attemptID, ""); err != nil {
		t.Fatalf("initial reconcile: %v", err)
	}
	var originalBlock int64
	if err := j.pool.QueryRow(ctx,
		`SELECT block_number FROM tx_receipts WHERE attempt_id = $1`, jj.attemptID).Scan(&originalBlock); err != nil {
		t.Fatalf("read receipt block: %v", err)
	}

	// Reorg: the original block is non-canonical and the tx appears at a new
	// canonical block/hash.
	j.mustExec(`UPDATE chain_blocks SET canonical = FALSE WHERE chain_id = $1 AND number = $2`, jointChainID, originalBlock)
	reorgBlock := originalBlock + 500
	reorgHash := blockHashHex(uint64(reorgBlock))
	j.mustExec(`INSERT INTO chain_blocks (chain_id, number, hash, parent_hash, canonical)
		VALUES ($1,$2,$3,$3,TRUE) ON CONFLICT DO NOTHING`, jointChainID, reorgBlock, reorgHash)
	chain.setReceipt(&types.Receipt{
		Status: 1, BlockNumber: big.NewInt(reorgBlock), BlockHash: common.HexToHash(reorgHash),
		Logs: []*types.Log{xferLog(j.asset, j.sender, j.recipient, 1000)},
	})
	if _, err := j.store.Reconcile(ctx, jj.attemptID, ""); err != nil {
		t.Fatalf("reconcile after reorg: %v", err)
	}
	var orphanedReceipts int
	if err := j.pool.QueryRow(ctx,
		`SELECT count(*) FROM tx_receipts WHERE attempt_id = $1 AND canonicality = 'orphaned'`, jj.attemptID).Scan(&orphanedReceipts); err != nil {
		t.Fatal(err)
	}
	if orphanedReceipts == 0 {
		t.Fatal("reorg did not orphan the original receipt")
	}
	att, err := j.store.AttemptByID(ctx, jj.attemptID)
	if err != nil || att.State != "orphaned" {
		t.Fatalf("attempt after reorg = %+v %v, want orphaned", att, err)
	}
	var orphanEvents int
	if err := j.pool.QueryRow(ctx,
		`SELECT count(*) FROM tx_attempt_events WHERE attempt_id = $1 AND event = 'orphaned'`, jj.attemptID).Scan(&orphanEvents); err != nil {
		t.Fatal(err)
	}
	if orphanEvents == 0 {
		t.Fatal("no orphaned revision event recorded")
	}

	// Re-inclusion at a new canonical height -> reconfirmed, never rebuilt.
	reincBlock := reorgBlock + 100
	reincHash := blockHashHex(uint64(reincBlock))
	j.mustExec(`INSERT INTO chain_blocks (chain_id, number, hash, parent_hash, canonical)
		VALUES ($1,$2,$3,$3,TRUE) ON CONFLICT DO NOTHING`, jointChainID, reincBlock, reincHash)
	chain.setReceipt(&types.Receipt{
		Status: 1, BlockNumber: big.NewInt(reincBlock), BlockHash: common.HexToHash(reincHash),
		Logs: []*types.Log{xferLog(j.asset, j.sender, j.recipient, 1000)},
	})
	if _, err := j.store.Reconcile(ctx, jj.attemptID, ""); err != nil {
		t.Fatalf("reconcile after re-inclusion: %v", err)
	}
	var reconfirmed int
	if err := j.pool.QueryRow(ctx,
		`SELECT count(*) FROM tx_attempt_events WHERE attempt_id = $1 AND event = 'reconfirmed'`, jj.attemptID).Scan(&reconfirmed); err != nil {
		t.Fatal(err)
	}
	if reconfirmed == 0 {
		t.Fatal("no reconfirmed revision event after re-inclusion")
	}
	var attempts, intents int
	if err := j.pool.QueryRow(ctx, `SELECT count(*) FROM tx_attempts WHERE intent_id = $1`, jj.intentID).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if err := j.pool.QueryRow(ctx, `SELECT count(*) FROM payment_intents WHERE intent_id = $1`, jj.intentID).Scan(&intents); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 || intents != 1 {
		t.Fatalf("reorg rebuilt the payment: attempts=%d intents=%d, want 1/1", attempts, intents)
	}

	// 011 projection consumes lifecycle revisions in order.
	reconciler := &execution.Reconciler{Pool: j.pool, Reader: scriptedReader{facts: execution.LifecycleFacts{RevisionVersion: 100, Basis: "scripted"}}}
	if _, err := reconciler.ReconcileIntent(ctx, jj.intentID, "", 0); err != nil {
		t.Fatalf("projection apply new: %v", err)
	}
	row, found, err := execution.ReadProjection(ctx, j.pool, jj.requestID)
	if err != nil || !found {
		t.Fatalf("ReadProjection: %v found=%v", err, found)
	}
	if row.LifecycleVersion != 100 {
		t.Fatalf("projection lifecycle_version = %d, want 100", row.LifecycleVersion)
	}
	reconciler.Reader = scriptedReader{facts: execution.LifecycleFacts{RevisionVersion: 50, Basis: "scripted-stale"}}
	if _, err := reconciler.ReconcileIntent(ctx, jj.intentID, "", 0); err != nil {
		t.Fatalf("projection apply stale: %v", err)
	}
	row, found, err = execution.ReadProjection(ctx, j.pool, jj.requestID)
	if err != nil || !found {
		t.Fatalf("ReadProjection after stale: %v found=%v", err, found)
	}
	if row.LifecycleVersion != 100 {
		t.Fatalf("stale revision overwrote newer: lifecycle_version = %d, want 100", row.LifecycleVersion)
	}

	reconciler.Reader = scriptedReader{err: errors.New("authority unavailable")}
	if _, err := reconciler.ReconcileIntent(ctx, jj.intentID, "", 0); err != nil {
		t.Fatalf("projection apply unavailable: %v", err)
	}
	row, found, err = execution.ReadProjection(ctx, j.pool, jj.requestID)
	if err != nil || !found {
		t.Fatalf("ReadProjection after unavailable: %v found=%v", err, found)
	}
	if row.Freshness != execution.FreshnessPossiblyStale {
		t.Fatalf("freshness = %s, want possibly_stale", row.Freshness)
	}
}
