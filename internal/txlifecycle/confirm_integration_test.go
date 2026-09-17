//go:build integration

package txlifecycle

import (
	"context"
	"testing"

	"github.com/ethereum/go-ethereum/core/types"
)

// TestV9ConfirmationReorgRevision is quickstart V9: confirmation to the 005
// threshold, reorg orphaning with a revision event, re-inclusion creating a
// new receipt row + reconfirmed, and no rebuilt intent/binding.
func TestV9ConfirmationReorgRevision(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	e.addBlock(98, blockHashHex(98), true)
	f := e.seed()

	confirmed := e.reconcileIncluded(t, f, receiptAt(98, blockHashHex(98), 1, []*types.Log{xferLog(fxAsset, f.sender, fxRecipient, 1000)}))
	if confirmed.AttemptState != "confirmed" {
		t.Fatalf("state = %s, want confirmed (confirmations=3)", confirmed.AttemptState)
	}
	var threshold, policy, tip int64
	if err := e.pool.QueryRow(ctx,
		`SELECT confirm_threshold, confirm_policy_seq, confirm_tip_number FROM tx_receipts WHERE attempt_id = $1`, f.attemptID).
		Scan(&threshold, &policy, &tip); err != nil {
		t.Fatal(err)
	}
	if threshold != 3 || policy != 1 || tip != 100 {
		t.Fatalf("confirmation basis = threshold %d policy %d tip %d", threshold, policy, tip)
	}
	var confirmedEvents int
	if err := e.pool.QueryRow(ctx,
		`SELECT count(*) FROM tx_attempt_events WHERE attempt_id = $1 AND event = 'confirmed'`, f.attemptID).Scan(&confirmedEvents); err != nil {
		t.Fatal(err)
	}
	if confirmedEvents == 0 {
		t.Fatal("confirmed event missing")
	}

	// Reorg: block 98 is no longer canonical; a new canonical receipt appears.
	e.exec(`UPDATE chain_blocks SET canonical = FALSE WHERE chain_id = $1 AND number = 98`, e.chainID)
	e.rpc.mu.Lock()
	e.rpc.receipt = receiptAt(100, blockHashHex(100), 1, []*types.Log{xferLog(fxAsset, f.sender, fxRecipient, 1000)})
	e.rpc.mu.Unlock()
	if _, err := e.store.Reconcile(ctx, f.attemptID, ""); err != nil {
		t.Fatal(err)
	}
	var orphaned int
	if err := e.pool.QueryRow(ctx,
		`SELECT count(*) FROM tx_receipts WHERE attempt_id = $1 AND canonicality = 'orphaned'`, f.attemptID).Scan(&orphaned); err != nil {
		t.Fatal(err)
	}
	if orphaned != 1 {
		t.Fatalf("orphaned receipts = %d, want 1", orphaned)
	}
	var orphanEvents, recoveryVersionEvents int
	if err := e.pool.QueryRow(ctx,
		`SELECT count(*), count(*) FILTER (WHERE recovery_version IS NOT NULL)
		   FROM tx_attempt_events WHERE attempt_id = $1 AND event = 'orphaned'`, f.attemptID).
		Scan(&orphanEvents, &recoveryVersionEvents); err != nil {
		t.Fatal(err)
	}
	if orphanEvents == 0 || recoveryVersionEvents == 0 {
		t.Fatalf("orphaned events = %d (with recovery_version %d)", orphanEvents, recoveryVersionEvents)
	}
	attempt, err := e.store.AttemptByID(ctx, f.attemptID)
	if err != nil {
		t.Fatal(err)
	}
	if attempt.State != "orphaned" {
		t.Fatalf("attempt state after reorg = %s, want orphaned", attempt.State)
	}

	// Re-inclusion: a new canonical block at height 98 with a new hash.
	e.addBlock(98, blockHashHex(1098), true)
	e.rpc.mu.Lock()
	e.rpc.receipt = receiptAt(98, blockHashHex(1098), 1, []*types.Log{xferLog(fxAsset, f.sender, fxRecipient, 1000)})
	e.rpc.mu.Unlock()
	if _, err := e.store.Reconcile(ctx, f.attemptID, ""); err != nil {
		t.Fatal(err)
	}
	var reconfirmed int
	if err := e.pool.QueryRow(ctx,
		`SELECT count(*) FROM tx_attempt_events WHERE attempt_id = $1 AND event = 'reconfirmed'`, f.attemptID).Scan(&reconfirmed); err != nil {
		t.Fatal(err)
	}
	if reconfirmed == 0 {
		t.Fatal("reconfirmed event missing after re-inclusion")
	}
	var receiptRows int
	if err := e.pool.QueryRow(ctx, `SELECT count(*) FROM tx_receipts WHERE attempt_id = $1`, f.attemptID).Scan(&receiptRows); err != nil {
		t.Fatal(err)
	}
	if receiptRows < 3 {
		t.Fatalf("receipt rows = %d, want >= 3 (orphaned + two canonical)", receiptRows)
	}
	if attempt, err = e.store.AttemptByID(ctx, f.attemptID); err != nil {
		t.Fatal(err)
	}
	if attempt.State != "confirmed" {
		t.Fatalf("attempt state after re-inclusion = %s, want confirmed", attempt.State)
	}

	// No rebuilt intent/binding/payment.
	var attempts, bindings int
	if err := e.pool.QueryRow(ctx, `SELECT count(*) FROM tx_attempts WHERE intent_id = $1`, f.intentID).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if err := e.pool.QueryRow(ctx, `SELECT count(*) FROM nonce_bindings WHERE intent_id = $1`, f.intentID).Scan(&bindings); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 || bindings != 1 {
		t.Fatalf("rebuild detected: attempts=%d bindings=%d", attempts, bindings)
	}
}

// TestV9SiblingReplaced is V9's replaced-marking: a confirmed attempt sharing
// a binding revises its siblings to replaced.
func TestV9SiblingReplaced(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	f := e.seed()

	replacement := *f.request
	replacement.AttemptID = f.attemptID + "-rep"
	replacement.SigningRequestID = f.signingRequestID + "-rep"
	replacement.ReplacementOf = f.attemptID
	replacement.MaxPriorityFeePerGas = "50000000"
	if _, err := e.store.PrepareAttempt(ctx, &replacement); err != nil {
		t.Fatalf("replacement prepare: %v", err)
	}

	res := e.reconcileIncluded(t, f, receiptAt(100, blockHashHex(100), 1, []*types.Log{xferLog(fxAsset, f.sender, fxRecipient, 1000)}))
	if res.AttemptState != "effective" {
		t.Fatalf("anchor state = %s, want effective", res.AttemptState)
	}
	sibling, err := e.store.AttemptByID(ctx, replacement.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	if sibling.State != "replaced" {
		t.Fatalf("sibling state = %s, want replaced", sibling.State)
	}
	var replacedEvents int
	if err := e.pool.QueryRow(ctx,
		`SELECT count(*) FROM tx_attempt_events WHERE attempt_id = $1 AND event = 'replaced'`, replacement.AttemptID).Scan(&replacedEvents); err != nil {
		t.Fatal(err)
	}
	if replacedEvents == 0 {
		t.Fatal("replaced event missing on sibling")
	}
}
