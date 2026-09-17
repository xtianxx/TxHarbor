//go:build integration

package txlifecycle

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/eth"
	"github.com/xtianxx/txharbor/internal/execution"
)

// jointLifecycleReader is the real 010 authority reader 011 consumes: it reads
// the attempt rows, the persisted tx_hash, the current revision and the unknown
// recovery condition from 010's durable state.
type jointLifecycleReader struct{ pool *pgxpool.Pool }

func (r jointLifecycleReader) Read(ctx context.Context, intentID string) (execution.LifecycleFacts, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT a.attempt_id, a.state, a.revision_seq, COALESCE(s.tx_hash, '')
		   FROM tx_attempts a LEFT JOIN tx_attempt_signings s ON s.attempt_id = a.attempt_id
		  WHERE a.intent_id = $1 ORDER BY a.created_at, a.attempt_id`, intentID)
	if err != nil {
		return execution.LifecycleFacts{}, err
	}
	defer rows.Close()
	facts := execution.LifecycleFacts{Basis: "010 authority read"}
	for rows.Next() {
		var id, state, hash string
		var rev int64
		if err := rows.Scan(&id, &state, &rev, &hash); err != nil {
			return execution.LifecycleFacts{}, err
		}
		facts.Attempts = append(facts.Attempts, execution.AttemptRef{AttemptID: id, State: state})
		facts.CurrentAttemptID = id
		if rev > facts.RevisionVersion {
			facts.RevisionVersion = rev
		}
		if state == "unknown" {
			facts.Unknown = &execution.UnknownRef{
				AttemptID: id, TxHash: hash,
				RecoveryCondition: "probe the same tx_hash; reconcile before any re-dispatch",
			}
		}
	}
	return facts, rows.Err()
}

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

// TestJointJ3UnknownReconciliation is T048/J3: a real dispatch response loss
// after node acceptance ends 010 `unknown`; the joint reconcile loop (011
// Reconciler over the real 010 authority reader) honestly persists it and then
// resolves it once the chain evidence is definite — never repaying.
func TestJointJ3UnknownReconciliation(t *testing.T) {
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

	reconciler := &execution.Reconciler{Pool: j.pool, Reader: jointLifecycleReader{pool: j.pool}}
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
