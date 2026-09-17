//go:build integration

package txlifecycle

import (
	"context"
	"testing"
)

// TestJointJ1IntentAuthorizationWiring is T046/J1: a real 007 HTTP request
// creates the withdrawal request, real 011 HTTP admission creates the intent
// and the real 011 execution claim, and 010 consumes the real claim + grant +
// scope to send on a real Anvil node and verify the receipt.
func TestJointJ1IntentAuthorizationWiring(t *testing.T) {
	ctx := context.Background()
	j := newJointEnv(t)
	jj := j.admit()
	j.installTransferEmit(1000)

	res, err := j.store.Send(ctx, &SendRequest{
		AttemptID: jj.attemptID, Kind: SendInitial, Claim: j.claim(jj),
	})
	if err != nil {
		t.Fatalf("joint send: %v", err)
	}
	if res.Outcome != "accepted" {
		t.Fatalf("joint send outcome=%s class=%s hash=%s", res.Outcome, res.RPCClass, res.TxHash)
	}

	j.waitMined(res.TxHash)
	j.seedChainTruth()
	rec, err := j.store.Reconcile(ctx, jj.attemptID, "")
	if err != nil {
		t.Fatalf("joint reconcile: %v", err)
	}
	if rec.Classification != "included" || rec.ReceiptEffect != "effective" {
		t.Fatalf("reconcile = %+v, want included/effective", rec)
	}

	var effect, canonicality string
	var confirmations int64
	if err := j.pool.QueryRow(ctx,
		`SELECT effect, canonicality, confirmations FROM tx_receipts WHERE attempt_id = $1`,
		jj.attemptID).Scan(&effect, &canonicality, &confirmations); err != nil {
		t.Fatalf("read receipt: %v", err)
	}
	if effect != "effective" || canonicality != "canonical" || confirmations < 1 {
		t.Fatalf("receipt = %s/%s confirmations=%d, want effective/canonical >=1", effect, canonicality, confirmations)
	}

	var ownerID string
	var lease int64
	if err := j.pool.QueryRow(ctx,
		`SELECT owner_id, lease_version FROM execution_claims WHERE intent_id = $1`, jj.intentID).
		Scan(&ownerID, &lease); err != nil {
		t.Fatalf("read real claim: %v", err)
	}
	if ownerID != jj.ownerID || lease != jj.claimVersion {
		t.Fatalf("claim owner/version = %s/%d, want %s/%d", ownerID, lease, jj.ownerID, jj.claimVersion)
	}

	var attempts, intents int
	if err := j.pool.QueryRow(ctx, `SELECT count(*) FROM tx_attempts WHERE intent_id = $1`, jj.intentID).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if err := j.pool.QueryRow(ctx, `SELECT count(*) FROM payment_intents WHERE intent_id = $1`, jj.intentID).Scan(&intents); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 || intents != 1 {
		t.Fatalf("attempts=%d intents=%d, want exactly 1/1 (no second intent)", attempts, intents)
	}

	att, err := j.store.AttemptByID(ctx, jj.attemptID)
	if err != nil {
		t.Fatalf("AttemptByID: %v", err)
	}
	if att.IntentID != jj.intentID || att.State != "confirmed" {
		t.Fatalf("attempt intent=%s state=%s, want %s/confirmed", att.IntentID, att.State, jj.intentID)
	}
}
