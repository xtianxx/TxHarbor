//go:build integration

package txlifecycle

import (
	"context"
	"testing"

	"github.com/xtianxx/txharbor/internal/eth"
)

// TestV5ReconcileProtocol is quickstart V5: probe classifications, no
// not_found_yet failure verdict, dispatch-from-unknown needs the observation
// first, and repetition converges without duplicate receipts.
func TestV5ReconcileProtocol(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()

	t.Run("not_found_yet_is_not_failure", func(t *testing.T) {
		f := e.unknownAttempt(t)
		res, err := e.store.Reconcile(ctx, f.attemptID, "")
		if err != nil {
			t.Fatal(err)
		}
		if res.Classification != "not_found_yet" {
			t.Fatalf("classification = %s", res.Classification)
		}
		if res.AttemptState != "unknown" {
			t.Fatalf("attempt state = %s, want unknown (never a failure)", res.AttemptState)
		}
	})

	t.Run("found_pending_moves_to_sent", func(t *testing.T) {
		f := e.unknownAttempt(t)
		e.rpc.mu.Lock()
		e.rpc.txFound = true
		e.rpc.txPending = true
		e.rpc.mu.Unlock()
		defer func() {
			e.rpc.mu.Lock()
			e.rpc.txFound = false
			e.rpc.txPending = false
			e.rpc.mu.Unlock()
		}()
		res, err := e.store.Reconcile(ctx, f.attemptID, "")
		if err != nil {
			t.Fatal(err)
		}
		if res.Classification != "found_pending" || res.AttemptState != "sent" {
			t.Fatalf("found_pending = %+v, want sent", res)
		}
	})

	t.Run("included_verifies_receipt", func(t *testing.T) {
		f := e.unknownAttempt(t)
		hash, _, err := e.store.signingRow(ctx, f.attemptID)
		if err != nil {
			t.Fatal(err)
		}
		e.rpc.mu.Lock()
		e.rpc.txFound = true
		e.rpc.txPending = false
		e.rpc.receipt = synthReceipt(f.sender, 100)
		e.rpc.mu.Unlock()
		defer func() {
			e.rpc.mu.Lock()
			e.rpc.txFound = false
			e.rpc.receipt = nil
			e.rpc.mu.Unlock()
		}()
		res, err := e.store.Reconcile(ctx, f.attemptID, hash)
		if err != nil {
			t.Fatal(err)
		}
		if res.Classification != "included" || res.ReceiptEffect != "effective" {
			t.Fatalf("included = %+v, want effective", res)
		}
		if res.AttemptState != "effective" {
			t.Fatalf("attempt state = %s, want effective", res.AttemptState)
		}
		// Repetition converges: no duplicate receipt row.
		if _, err := e.store.Reconcile(ctx, f.attemptID, hash); err != nil {
			t.Fatal(err)
		}
		var receipts int
		if err := e.pool.QueryRow(ctx, `SELECT count(*) FROM tx_receipts WHERE attempt_id = $1`, f.attemptID).Scan(&receipts); err != nil {
			t.Fatal(err)
		}
		if receipts != 1 {
			t.Fatalf("receipt rows = %d, want 1", receipts)
		}
	})

	t.Run("unavailable_fails_closed", func(t *testing.T) {
		f := e.unknownAttempt(t)
		e.rpc.mu.Lock()
		e.rpc.probeErr = &eth.Error{Kind: eth.KindTransport, Op: "stub"}
		e.rpc.mu.Unlock()
		defer func() {
			e.rpc.mu.Lock()
			e.rpc.probeErr = nil
			e.rpc.mu.Unlock()
		}()
		res, err := e.store.Reconcile(ctx, f.attemptID, "")
		if err != nil {
			t.Fatal(err)
		}
		if res.Classification != "unavailable" || res.AttemptState != "unknown" {
			t.Fatalf("unavailable = %+v, want unchanged unknown", res)
		}
	})

	t.Run("dispatch_from_unknown_requires_observation", func(t *testing.T) {
		f := e.unknownAttempt(t)
		before := e.rpc.dispatchCount()
		res, err := e.send(f, SendReplay, nil)
		if err != nil || res.Outcome != "accepted" {
			t.Fatalf("replay from unknown = %+v %v", res, err)
		}
		if e.rpc.dispatchCount() != before+1 {
			t.Fatal("replay from unknown did not dispatch")
		}
		var observations int
		if err := e.pool.QueryRow(ctx,
			`SELECT count(*) FROM tx_reconciliations WHERE attempt_id = $1`, f.attemptID).Scan(&observations); err != nil {
			t.Fatal(err)
		}
		if observations == 0 {
			t.Fatal("dispatch from unknown happened without a reconcile observation")
		}
	})

	t.Run("unavailable_blocks_dispatch", func(t *testing.T) {
		f := e.unknownAttempt(t)
		e.rpc.mu.Lock()
		e.rpc.probeErr = &eth.Error{Kind: eth.KindTransport, Op: "stub"}
		e.rpc.mu.Unlock()
		defer func() {
			e.rpc.mu.Lock()
			e.rpc.probeErr = nil
			e.rpc.mu.Unlock()
		}()
		before := e.rpc.dispatchCount()
		res, err := e.send(f, SendReplay, nil)
		if got := refusalClass(err); got != ClassGateReadFailed {
			t.Fatalf("refusal = %s, want gate_read_failed", got)
		}
		if res.Outcome != "blocked" {
			t.Fatalf("result = %+v, want blocked", res)
		}
		if got := e.rpc.dispatchCount(); got != before {
			t.Fatalf("unavailable probe dispatched: %d -> %d", before, got)
		}
		var observed int
		if err := e.pool.QueryRow(ctx,
			`SELECT count(*) FROM tx_reconciliations WHERE attempt_id = $1 AND classification = 'unavailable'`, f.attemptID).Scan(&observed); err != nil {
			t.Fatal(err)
		}
		if observed == 0 {
			t.Fatal("unavailable observation not recorded")
		}
	})
}

// TestV5UnknownRecoveryInterface covers T025: facts + recovery conditions,
// never a rebuild and never a send.
func TestV5UnknownRecoveryInterface(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	f := e.unknownAttempt(t)

	before := e.rpc.dispatchCount()
	u, err := e.store.UnknownRecovery(ctx, f.attemptID, "")
	if err != nil {
		t.Fatal(err)
	}
	if !u.SignedBytesPresent {
		t.Fatal("unknown recovery lost the persisted signed bytes")
	}
	if len(u.Dispatches) == 0 || u.Dispatches[len(u.Dispatches)-1].Outcome != "unknown" {
		t.Fatalf("dispatch facts = %+v", u.Dispatches)
	}
	if len(u.RecoveryConditions) == 0 {
		t.Fatal("no recovery conditions stated")
	}
	if got := e.rpc.dispatchCount(); got != before {
		t.Fatalf("unknown recovery sent: dispatch count %d -> %d", before, got)
	}
	var attempts int
	if err := e.pool.QueryRow(ctx, `SELECT count(*) FROM tx_attempts WHERE intent_id = $1`, f.intentID).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 {
		t.Fatalf("unknown recovery rebuilt attempts: %d", attempts)
	}
}
