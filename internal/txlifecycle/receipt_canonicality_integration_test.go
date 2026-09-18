//go:build integration

// receipt_canonicality_integration_test.go is Lane-F3/B1: the tx_receipts
// canonicality lifecycle on repeat observations (010 data-model Table 5).
//
// Contract under test (no semantic change):
//   - canonicality starts `unverified` and moves to `canonical` only when the
//     current indexer chain view contains the receipt's (block_number,
//     block_hash); a lagging view must not strand a receipt as unverified
//     forever once truth catches up, and a non-canonical observation must
//     never overwrite newer progress;
//   - `orphaned` is terminal for that row identity: the old block is never
//     re-canonicalized, and it never drives a re-confirmation; a genuine
//     re-inclusion carries a new block hash and writes a new row;
//   - confirmation progress/basis and `confirmed_at` advance only while the
//     row is canonical, and repeat scans of a confirmed attempt are
//     idempotent (no revision bump, no duplicate `confirmed` event).
package txlifecycle

import (
	"context"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
)

// stubIncluded switches the stub chain probe to "included with this receipt".
func stubIncluded(e *env, receipt *types.Receipt) {
	e.rpc.mu.Lock()
	e.rpc.txFound = true
	e.rpc.txPending = false
	e.rpc.receipt = receipt
	e.rpc.mu.Unlock()
}

// stubProbeOff restores the "not found / pending" default probe.
func stubProbeOff(e *env) {
	e.rpc.mu.Lock()
	e.rpc.txFound = false
	e.rpc.receipt = nil
	e.rpc.mu.Unlock()
}

type receiptFacts struct {
	canonicality  string
	confirmations int64
	threshold     int64
	confirmedAt   *time.Time
}

func readReceiptFacts(t *testing.T, e *env, attemptID string) receiptFacts {
	t.Helper()
	var f receiptFacts
	if err := e.pool.QueryRow(context.Background(),
		`SELECT canonicality, confirmations, confirm_threshold, confirmed_at
		   FROM tx_receipts WHERE attempt_id = $1`, attemptID).
		Scan(&f.canonicality, &f.confirmations, &f.threshold, &f.confirmedAt); err != nil {
		t.Fatalf("read receipt facts: %v", err)
	}
	return f
}

func attemptState(t *testing.T, e *env, attemptID string) string {
	t.Helper()
	a, err := e.store.AttemptByID(context.Background(), attemptID)
	if err != nil {
		t.Fatalf("AttemptByID: %v", err)
	}
	return a.State
}

func reconcileOnce(t *testing.T, e *env, attemptID string) ReconcileResult {
	t.Helper()
	res, err := e.store.Reconcile(context.Background(), attemptID, "")
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	return res
}

// TestV8UnverifiedPromotesWithChainTruth is the B1 core regression: a receipt
// first observed while the indexer view had not reached its block stays
// unverified; when the protected chain facts catch up, the SAME receipt row is
// promoted to canonical and only then can Transfer/effect + depth confirm the
// attempt. Repeat scans afterwards are idempotent.
func TestV8UnverifiedPromotesWithChainTruth(t *testing.T) {
	e := newEnv(t)
	f := e.seed()
	f.sign()
	stubIncluded(e, receiptAt(150, blockHashHex(150), 1, []*types.Log{xferLog(fxAsset, f.sender, fxRecipient, 1000)}))
	t.Cleanup(func() { stubProbeOff(e) })

	// First observation: chain truth has no row at 150 -> unverified, no
	// progress, no confirmation (and no attempt transition).
	res := reconcileOnce(t, e, f.attemptID)
	if res.Classification != "included" || res.ReceiptEffect != "effective" {
		t.Fatalf("first reconcile = %+v, want included/effective", res)
	}
	first := readReceiptFacts(t, e, f.attemptID)
	if first.canonicality != "unverified" || first.confirmations != 0 || first.confirmedAt != nil {
		t.Fatalf("lagging observation = %+v, want unverified/0/no confirmed_at", first)
	}
	if got := attemptState(t, e, f.attemptID); got != "signed" {
		t.Fatalf("attempt after lagging observation = %s, want signed (no canonical fact yet)", got)
	}

	// Chain truth catches up: promotion is driven by the chain view, not by
	// the RPC classification alone (the classification was identical above).
	e.addBlock(150, blockHashHex(150), true)
	reconcileOnce(t, e, f.attemptID)
	promoted := readReceiptFacts(t, e, f.attemptID)
	if promoted.canonicality != "canonical" {
		t.Fatalf("canonicality after truth catch-up = %s, want canonical", promoted.canonicality)
	}
	if promoted.confirmations != 1 || promoted.threshold != 3 {
		t.Fatalf("progress = %d/%d, want 1/3", promoted.confirmations, promoted.threshold)
	}
	if promoted.confirmedAt != nil {
		t.Fatal("confirmed_at set below the threshold")
	}
	if got := attemptState(t, e, f.attemptID); got != "effective" {
		t.Fatalf("attempt after promotion = %s, want effective", got)
	}

	// Depth reaches the policy threshold: confirmation applies exactly once.
	e.addBlock(151, blockHashHex(151), true)
	e.addBlock(152, blockHashHex(152), true)
	reconcileOnce(t, e, f.attemptID)
	confirmed := readReceiptFacts(t, e, f.attemptID)
	if confirmed.canonicality != "canonical" || confirmed.confirmations != 3 || confirmed.confirmedAt == nil {
		t.Fatalf("confirmed receipt = %+v, want canonical/3/confirmed_at", confirmed)
	}
	if got := attemptState(t, e, f.attemptID); got != "confirmed" {
		t.Fatalf("attempt at threshold = %s, want confirmed", got)
	}
	revision := e.revisionOf(t, f.attemptID)
	var confirmedEvents int
	if err := e.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM tx_attempt_events WHERE attempt_id = $1 AND event = 'confirmed'`, f.attemptID).
		Scan(&confirmedEvents); err != nil {
		t.Fatal(err)
	}
	if confirmedEvents != 1 {
		t.Fatalf("confirmed events = %d, want exactly 1", confirmedEvents)
	}

	// Repeat scans: idempotent — stored progress, revision and events stable.
	for i := 0; i < 3; i++ {
		reconcileOnce(t, e, f.attemptID)
	}
	repeat := readReceiptFacts(t, e, f.attemptID)
	if repeat.canonicality != "canonical" || repeat.confirmations != 3 {
		t.Fatalf("receipt after repeat scans = %+v, want canonical/3", repeat)
	}
	if got := e.revisionOf(t, f.attemptID); got != revision {
		t.Fatalf("revision after repeat scans = %d, want %d (idempotent scans do not mutate)", got, revision)
	}
	if err := e.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM tx_attempt_events WHERE attempt_id = $1 AND event = 'confirmed'`, f.attemptID).
		Scan(&confirmedEvents); err != nil {
		t.Fatal(err)
	}
	if confirmedEvents != 1 {
		t.Fatalf("confirmed events after repeat scans = %d, want 1", confirmedEvents)
	}
}

// TestV8OrphanedReceiptIdentityIsTerminal is the B1 counterexample: once a
// receipt row is revised to orphaned, the old block identity never returns to
// canonical and never re-confirms the attempt, even if chain truth and the RPC
// view later report the old block again; a non-canonical observation also
// never rewrites newer confirmation progress.
func TestV8OrphanedReceiptIdentityIsTerminal(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	f := e.seed()
	f.sign()
	e.addBlock(150, blockHashHex(150), true)
	e.addBlock(151, blockHashHex(151), true)
	e.addBlock(152, blockHashHex(152), true)
	stubIncluded(e, receiptAt(150, blockHashHex(150), 1, []*types.Log{xferLog(fxAsset, f.sender, fxRecipient, 1000)}))
	t.Cleanup(func() { stubProbeOff(e) })

	reconcileOnce(t, e, f.attemptID)
	if got := attemptState(t, e, f.attemptID); got != "confirmed" {
		t.Fatalf("attempt before reorg = %s, want confirmed", got)
	}

	// Reorg: the old block loses canonicality and the tx re-includes at a new
	// block -> old row orphaned, new row canonical, attempt orphaned.
	e.exec(`UPDATE chain_blocks SET canonical = FALSE WHERE chain_id = $1 AND number = 150`, e.chainID)
	e.addBlock(160, blockHashHex(160), true)
	stubIncluded(e, receiptAt(160, blockHashHex(160), 1, []*types.Log{xferLog(fxAsset, f.sender, fxRecipient, 1000)}))
	reconcileOnce(t, e, f.attemptID)
	if got := attemptState(t, e, f.attemptID); got != "orphaned" {
		t.Fatalf("attempt after reorg = %s, want orphaned", got)
	}

	// Counterexample: chain truth and the RPC view report the OLD block again.
	// The orphaned identity is terminal — the row must not flip canonical and
	// must not drive a re-confirmation.
	e.exec(`UPDATE chain_blocks SET canonical = TRUE WHERE chain_id = $1 AND number = 150`, e.chainID)
	stubIncluded(e, receiptAt(150, blockHashHex(150), 1, []*types.Log{xferLog(fxAsset, f.sender, fxRecipient, 1000)}))
	reconcileOnce(t, e, f.attemptID)

	var oldRow struct {
		canonicality  string
		confirmations int64
	}
	if err := e.pool.QueryRow(ctx,
		`SELECT canonicality, confirmations FROM tx_receipts
		  WHERE attempt_id = $1 AND block_number = 150`, f.attemptID).
		Scan(&oldRow.canonicality, &oldRow.confirmations); err != nil {
		t.Fatal(err)
	}
	if oldRow.canonicality != "orphaned" {
		t.Fatalf("old receipt canonicality = %s, want orphaned (terminal for the row)", oldRow.canonicality)
	}
	if oldRow.confirmations != 3 {
		t.Fatalf("old receipt confirmations = %d, want the newer 3 preserved (stale observation must not rewrite)", oldRow.confirmations)
	}
	if got := attemptState(t, e, f.attemptID); got != "orphaned" {
		t.Fatalf("attempt after old-block observation = %s, want orphaned (no re-canonicalization from the old block)", got)
	}
	var reconfirmed int
	if err := e.pool.QueryRow(ctx,
		`SELECT count(*) FROM tx_attempt_events WHERE attempt_id = $1 AND event = 'reconfirmed'`, f.attemptID).
		Scan(&reconfirmed); err != nil {
		t.Fatal(err)
	}
	if reconfirmed != 0 {
		t.Fatalf("reconfirmed events = %d, want 0 (the old identity must not reconfirm)", reconfirmed)
	}

	// A non-canonical observation of a still-canonical row must not rewrite its
	// stored progress: observe the new row while its block is absent from the
	// chain view (truth head falls back below it) — the newer progress stands.
	stubIncluded(e, receiptAt(160, blockHashHex(160), 1, []*types.Log{xferLog(fxAsset, f.sender, fxRecipient, 1000)}))
	e.exec(`UPDATE chain_blocks SET canonical = FALSE WHERE chain_id = $1 AND number IN (151, 152, 160)`, e.chainID)
	var newConfirmations int64
	if err := e.pool.QueryRow(ctx,
		`SELECT confirmations FROM tx_receipts WHERE attempt_id = $1 AND block_number = 160`, f.attemptID).
		Scan(&newConfirmations); err != nil {
		t.Fatal(err)
	}
	reconcileOnce(t, e, f.attemptID)
	var afterNewConfirmations int64
	if err := e.pool.QueryRow(ctx,
		`SELECT confirmations FROM tx_receipts WHERE attempt_id = $1 AND block_number = 160`, f.attemptID).
		Scan(&afterNewConfirmations); err != nil {
		t.Fatal(err)
	}
	if afterNewConfirmations != newConfirmations {
		t.Fatalf("new-row confirmations after non-canonical observation = %d, want %d preserved", afterNewConfirmations, newConfirmations)
	}
}
