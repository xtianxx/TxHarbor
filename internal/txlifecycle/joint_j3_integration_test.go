//go:build integration

package txlifecycle

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/xtianxx/txharbor/internal/eth"
	"github.com/xtianxx/txharbor/internal/execution"
)

// jointDropAfterAccept forwards the dispatch to the real node (so the tx is
// genuinely accepted) and then loses the response: the joint "response drop
// after node acceptance" fault.
type jointDropAfterAccept struct{ real *eth.Client }

func (d jointDropAfterAccept) SendSignedTransaction(ctx context.Context, raw []byte, expected common.Hash) (common.Hash, error) {
	_, _ = d.real.SendSignedTransaction(ctx, raw, expected)
	return common.Hash{}, &eth.Error{Kind: eth.KindTimeout, Op: "joint-drop-after-accept"}
}

func (d jointDropAfterAccept) TransactionByHash(ctx context.Context, h common.Hash) (*types.Transaction, bool, error) {
	return d.real.TransactionByHash(ctx, h)
}

func (d jointDropAfterAccept) TransactionReceipt(ctx context.Context, h common.Hash) (*types.Receipt, error) {
	return d.real.TransactionReceipt(ctx, h)
}

func (d jointDropAfterAccept) BlockNumber(ctx context.Context) (uint64, error) {
	return d.real.BlockNumber(ctx)
}

// TestJointJ3UnknownReconciliation is T048/J3 SUPPLEMENTARY shared-state
// coverage: it drives the real 010 Store directly and validates the durable
// unknown facts the two lanes share. The worker-Driver J3 acceptance is
// TestJointDriverJ3UnknownReconciliation (joint_driver_integration_test.go).
//
// The scenario: a real dispatch response loss after node acceptance ends 010
// `unknown`; the joint reconcile loop (011 Reconciler over the real 010
// authority reader) honestly persists it and then resolves it once the chain
// evidence is definite — never repaying.
func TestJointJ3UnknownReconciliation(t *testing.T) {
	t.Log("supplementary shared-state J3: direct 010 Store drive, not the worker-Driver link")
	ctx := context.Background()
	j := newJointEnv(t)
	jj := j.admit()
	j.installTransferEmit(1000)

	j.store.WithChain(jointDropAfterAccept{real: j.eth}, 15*time.Second)
	res, err := j.store.Send(ctx, &SendRequest{AttemptID: jj.attemptID, Kind: SendInitial, Claim: j.claim(jj)})
	if err != nil {
		t.Fatalf("lost-response send: %v", err)
	}
	if res.Outcome != "unknown" {
		t.Fatalf("lost-response outcome = %s/%s, want unknown", res.Outcome, res.RPCClass)
	}
	j.store.WithChain(j.eth, 15*time.Second)

	att, err := j.store.AttemptByID(ctx, jj.attemptID)
	if err != nil || att.State != "unknown" {
		t.Fatalf("attempt = %+v %v, want unknown", att, err)
	}
	var outcome, rpcClass string
	if err := j.pool.QueryRow(ctx,
		`SELECT outcome, rpc_class FROM tx_send_attempts WHERE attempt_id = $1 ORDER BY send_id DESC LIMIT 1`, jj.attemptID).
		Scan(&outcome, &rpcClass); err != nil {
		t.Fatal(err)
	}
	if outcome != "unknown" {
		t.Fatalf("durable send outcome = %s, want unknown", outcome)
	}
	hash, raw, err := j.store.signingRow(ctx, jj.attemptID)
	if err != nil || len(raw) == 0 {
		t.Fatalf("bytes/hash lost: %v (%d bytes)", err, len(raw))
	}

	reader, err := NewLifecycleLive(j.pool, j.store)
	if err != nil {
		t.Fatalf("010 lifecycle adapter: %v", err)
	}
	reconciler := &execution.Reconciler{Pool: j.pool, Reader: reader}
	// Joint loop while the chain is still uncleared: honest persist, no failure.
	rec, err := reconciler.ReconcileIntent(ctx, jj.intentID, jj.ownerID, jj.claimVersion)
	if err != nil {
		t.Fatalf("joint reconcile while unknown: %v", err)
	}
	if rec.OutcomeClass == string(execution.OutcomeSent) && rec.Converged {
		t.Fatalf("unknown wrongly converged as sent: %+v", rec)
	}

	j.waitMined(hash)
	j.seedChainTruth()
	rec010, err := j.store.Reconcile(ctx, jj.attemptID, "")
	if err != nil || rec010.Classification != "included" || rec010.ReceiptEffect != "effective" {
		t.Fatalf("010 reconcile = %+v %v, want included/effective", rec010, err)
	}

	// Joint loop after the authority fact is definite: completes, no re-dispatch.
	rec2, err := reconciler.ReconcileIntent(ctx, jj.intentID, jj.ownerID, jj.claimVersion)
	if err != nil {
		t.Fatalf("joint reconcile after resolution: %v", err)
	}
	if !strings.Contains(rec2.Basis, "authority confirmed sent") {
		t.Fatalf("joint reconcile did not consume the confirmed-sent fact: %+v", rec2)
	}

	var attempts, intents, sends int
	if err := j.pool.QueryRow(ctx, `SELECT count(*) FROM tx_attempts WHERE intent_id = $1`, jj.intentID).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if err := j.pool.QueryRow(ctx, `SELECT count(*) FROM payment_intents WHERE intent_id = $1`, jj.intentID).Scan(&intents); err != nil {
		t.Fatal(err)
	}
	if err := j.pool.QueryRow(ctx, `SELECT count(*) FROM tx_send_attempts WHERE attempt_id = $1`, jj.attemptID).Scan(&sends); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 || intents != 1 || sends != 1 {
		t.Fatalf("repaying detected: attempts=%d intents=%d sends=%d, want 1/1/1", attempts, intents, sends)
	}
}
