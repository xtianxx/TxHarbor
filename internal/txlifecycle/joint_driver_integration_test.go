//go:build integration

// joint_driver_integration_test.go is the FINAL joint acceptance path for
// 010:T046–T050 (J1–J5): every send-class flow is driven through the
// production-wired 011 worker
// (`app.NewJointWithdrawalWorker(...)`, `worker.Driver.IssueAndAdvance`) over
// the real 010 adapter (`txlifecycle.NewLifecycleLive`), the real 008
// Allocator, the real 009 signer-serve and a real Anvil node with a test
// ERC-20 Transfer emitter. The reconciler legs run through
// `worker.Reconciler.ReconcileIntent` (the 011 loop) over the real 010
// authority reader.
//
// The sibling `joint_j*_integration_test.go` files remain as SUPPLEMENTARY
// shared-state coverage: they call the 010 Store directly to validate the
// durable rows two lanes share. They do not, by themselves, prove the
// worker-Driver link; that link is proven here.
package txlifecycle

import (
	"context"
	"errors"
	"log/slog"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/xtianxx/txharbor/internal/app"
	"github.com/xtianxx/txharbor/internal/execution"
	"github.com/xtianxx/txharbor/internal/nonce"
)

// productionWorker builds the worker exactly as the joint deployment does:
// the 010-owned LifecycleLive adapter over the real Store, the real 011
// StepDriver/Reconciler boundary, and the real 008 binding reader.
func (j *jointEnv) productionWorker() (*app.WithdrawalWorker, *LifecycleLive) {
	j.t.Helper()
	adapter, err := NewLifecycleLive(j.pool, j.store)
	if err != nil {
		j.t.Fatalf("010 lifecycle adapter: %v", err)
	}
	worker, err := app.NewJointWithdrawalWorker(j.pool, jointWorkerConfig(), nil, slog.Default(), app.JointDeps{
		Advancer: adapter,
		Reader:   adapter,
		Binding:  execution.NewLiveBindingReader(nonce.NewReadProvider(j.pool, "")),
	})
	if err != nil {
		j.t.Fatalf("joint worker: %v", err)
	}
	if worker.Driver == nil || worker.Reconciler == nil {
		j.t.Fatalf("joint worker not wired: driver=%v reconciler=%v", worker.Driver, worker.Reconciler)
	}
	return worker, adapter
}

// markClaimed applies the production admitted->claimed edge (the worker's
// serveIntent advanceIntentToClaimed) so the driver runs against the state the
// 011 loop maintains. The claim row itself was created by the real 011
// ClaimStore in admitIntent.
func (j *jointEnv) markClaimed(jj *jointIntent) {
	j.t.Helper()
	tx, err := j.pool.Begin(j.ctx)
	if err != nil {
		j.t.Fatalf("begin claimed transition: %v", err)
	}
	defer func() { _ = tx.Rollback(j.ctx) }()
	intent, found, err := execution.ReadIntent(j.ctx, tx, jj.intentID)
	if err != nil || !found {
		j.t.Fatalf("read intent: %v found=%v", err, found)
	}
	if intent.State == execution.IntentAdmitted {
		if err := execution.TransitionIntent(j.ctx, tx, jj.intentID,
			execution.IntentAdmitted, intent.StateVersion, execution.IntentClaimed, jj.claimVersion); err != nil {
			j.t.Fatalf("admitted->claimed: %v", err)
		}
	}
	if err := tx.Commit(j.ctx); err != nil {
		j.t.Fatalf("commit claimed transition: %v", err)
	}
}

func (j *jointEnv) stepCount(intentID string) int {
	j.t.Helper()
	var n int
	if err := j.pool.QueryRow(j.ctx, `SELECT count(*) FROM execution_steps WHERE intent_id = $1`, intentID).Scan(&n); err != nil {
		j.t.Fatal(err)
	}
	return n
}

func (j *jointEnv) intentState(intentID string) string {
	j.t.Helper()
	var state string
	if err := j.pool.QueryRow(j.ctx, `SELECT state FROM payment_intents WHERE intent_id = $1`, intentID).Scan(&state); err != nil {
		j.t.Fatal(err)
	}
	return state
}

// TestJointDriverJ1IntentAuthorizationWiring is 010:T046/J1 on the production
// worker path: a real 007 HTTP request creates the withdrawal request, real
// 011 HTTP admission creates the intent plus the real 011 claim, and the
// worker Driver drives the first broadcast through 010 (real grant/scope
// gates, real 009 signing), sends on the real Anvil node and verifies the
// receipt; the 011 reconcile loop then completes the intent from 010 facts.
func TestJointDriverJ1IntentAuthorizationWiring(t *testing.T) {
	ctx := context.Background()
	j := newJointEnv(t)
	jj := j.admitIntent()
	j.markClaimed(jj)
	j.installTransferEmit(1000)
	worker, _ := j.productionWorker()

	out, err := worker.Driver.IssueAndAdvance(ctx, execution.StepRequest{
		IntentID: jj.intentID, RequestID: jj.requestID, CallerID: j.callerID,
		OwnerID: jj.ownerID, LeaseVersion: jj.claimVersion, Action: execution.ActionFirstBroadcast,
	})
	if err != nil {
		t.Fatalf("driver first broadcast: %v", err)
	}
	if out.OutcomeClass != execution.OutcomeSent || out.FinalStepState != execution.StepConverged ||
		out.AttemptID == "" || out.TxHash == "" {
		t.Fatalf("driver outcome = %+v, want sent/converged with attempt and hash", out)
	}

	var stepState, stepAttempt string
	if err := j.pool.QueryRow(ctx,
		`SELECT state, COALESCE(attempt_id, '') FROM execution_steps WHERE step_id = $1`, out.StepID).
		Scan(&stepState, &stepAttempt); err != nil {
		t.Fatal(err)
	}
	if stepState != execution.StepConverged || stepAttempt != out.AttemptID {
		t.Fatalf("step = %s/%s, want converged/%s", stepState, stepAttempt, out.AttemptID)
	}

	j.waitMined(out.TxHash)
	j.seedChainTruth()
	rec, err := j.store.Reconcile(ctx, out.AttemptID, "")
	if err != nil {
		t.Fatalf("010 reconcile: %v", err)
	}
	if rec.Classification != "included" || rec.ReceiptEffect != "effective" {
		t.Fatalf("reconcile = %+v, want included/effective", rec)
	}
	var effect, canonicality string
	var confirmations int64
	if err := j.pool.QueryRow(ctx,
		`SELECT effect, canonicality, confirmations FROM tx_receipts WHERE attempt_id = $1`, out.AttemptID).
		Scan(&effect, &canonicality, &confirmations); err != nil {
		t.Fatalf("read receipt: %v", err)
	}
	if effect != "effective" || canonicality != "canonical" || confirmations < 1 {
		t.Fatalf("receipt = %s/%s confirmations=%d, want effective/canonical >=1", effect, canonicality, confirmations)
	}

	// The real 011 claim is untouched by the send path (read-only fence).
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

	// The 011 loop consumes the confirmed-sent authority fact and completes
	// the same intent (fact transition, no new payment).
	if _, err := worker.Reconciler.ReconcileIntent(ctx, jj.intentID, jj.ownerID, jj.claimVersion); err != nil {
		t.Fatalf("joint reconcile: %v", err)
	}
	if state := j.intentState(jj.intentID); state != execution.IntentCompleted {
		t.Fatalf("intent state = %s, want completed", state)
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
}

// TestJointDriverJ2ExpiredWorkerIsolationAndTakeover is 010:T047/J2 on the
// production worker path: the fenced worker's three send kinds are all
// refused before any dispatch through the worker Driver, 010's own claim gate
// still refuses the same claim through the production adapter, a legitimate
// 011 taker takes over the same intent (monotonic lease_version) and its
// replay dispatches on the same attempt history. The fee-replacement leg is
// recorded as blocked-with-reason: 010 has no fee-construction policy
// (`Advance(ActionReplace)` returns refused_basis), so the scenario is not
// faked here; the direct-Store supplementary test covers the 010 claim fence
// for a prepared replacement attempt.
func TestJointDriverJ2ExpiredWorkerIsolationAndTakeover(t *testing.T) {
	ctx := context.Background()
	j := newJointEnv(t)
	jj := j.admitIntent()
	j.markClaimed(jj)
	j.installTransferEmit(1000)
	worker, adapter := j.productionWorker()

	first, err := worker.Driver.IssueAndAdvance(ctx, execution.StepRequest{
		IntentID: jj.intentID, RequestID: jj.requestID, CallerID: j.callerID,
		OwnerID: jj.ownerID, LeaseVersion: jj.claimVersion, Action: execution.ActionFirstBroadcast,
	})
	if err != nil || first.OutcomeClass != execution.OutcomeSent {
		t.Fatalf("worker-1 first broadcast = %+v %v", first, err)
	}

	j.mustExec(`UPDATE execution_claims SET acquired_at = now() - interval '2 hours',
		expires_at = now() - interval '1 hour' WHERE intent_id = $1`, jj.intentID)
	stepsBefore, sendsBefore := j.stepCount(jj.intentID), j.sendRowCount(first.AttemptID)

	// The fenced worker's three send kinds, through the worker Driver.
	for _, action := range []execution.AdvanceAction{
		execution.ActionFirstBroadcast, execution.ActionReplay, execution.ActionReplace,
	} {
		out, err := worker.Driver.IssueAndAdvance(ctx, execution.StepRequest{
			IntentID: jj.intentID, RequestID: jj.requestID, CallerID: j.callerID,
			OwnerID: jj.ownerID, LeaseVersion: jj.claimVersion, Action: action,
			AnchorAttemptID: first.AttemptID, ExpectedTxHash: first.TxHash,
		})
		if err != nil {
			t.Fatalf("fenced %s: %v", action, err)
		}
		if out.Refusal != execution.ClassClaimNotCurrent {
			t.Fatalf("fenced %s refusal = %s (%s), want %s", action, out.Refusal, out.Basis, execution.ClassClaimNotCurrent)
		}
	}
	if got := j.stepCount(jj.intentID); got != stepsBefore {
		t.Fatalf("fenced worker wrote a step: %d -> %d", stepsBefore, got)
	}
	if got := j.sendRowCount(first.AttemptID); got != sendsBefore {
		t.Fatalf("fenced worker dispatched: %d -> %d", sendsBefore, got)
	}

	// 010's own fence through the production adapter: the same expired claim
	// is refused at 010's claim gate with zero dispatch.
	adv, err := adapter.Advance(ctx, execution.AdvanceRequest{
		IntentID: jj.intentID, RequestID: jj.requestID, CallerID: j.callerID,
		OwnerID: jj.ownerID, LeaseVersion: jj.claimVersion, StepID: "step-j2-fence-010",
		Action: execution.ActionReplay, AnchorAttemptID: first.AttemptID, ExpectedTxHash: first.TxHash,
	})
	if err != nil {
		t.Fatalf("010 fenced replay: %v", err)
	}
	if adv.Class != execution.OutcomeRefusedGate || !strings.Contains(adv.Basis, string(ClassClaimExpired)) {
		t.Fatalf("010 fenced replay = %+v, want refused_gate with %s", adv, ClassClaimExpired)
	}
	if got := j.sendRowCount(first.AttemptID); got != sendsBefore {
		t.Fatalf("010 fenced replay dispatched: %d -> %d", sendsBefore, got)
	}

	// Legitimate taker: real 011 claim takeover after expiry.
	claims, err := execution.NewClaimStore(j.pool, 30*time.Second, 5*time.Minute)
	if err != nil {
		t.Fatalf("NewClaimStore: %v", err)
	}
	taker, err := claims.Claim(ctx, jj.intentID, "worker-j2-taker")
	if err != nil || !taker.Acquired || taker.Version <= jj.claimVersion {
		t.Fatalf("takeover claim = %+v %v, want version > %d", taker, err, jj.claimVersion)
	}

	// The taker replays through the worker Driver: it passes every 011 gate and
	// 010 dispatches; the chain verdict (the nonce is already consumed) is a
	// dispatch outcome, never a claim refusal.
	takerOut, err := worker.Driver.IssueAndAdvance(ctx, execution.StepRequest{
		IntentID: jj.intentID, RequestID: jj.requestID, CallerID: j.callerID,
		OwnerID: "worker-j2-taker", LeaseVersion: taker.Version, Action: execution.ActionReplay,
		AnchorAttemptID: first.AttemptID, ExpectedTxHash: first.TxHash,
	})
	if err != nil {
		t.Fatalf("taker replay: %v", err)
	}
	if takerOut.Refusal != "" || takerOut.FinalStepState == execution.StepRefused {
		t.Fatalf("taker replay refused = %+v, want a dispatched verdict", takerOut)
	}
	if takerOut.OutcomeClass != execution.OutcomeReconcileRequired &&
		takerOut.OutcomeClass != execution.OutcomeSent && takerOut.OutcomeClass != execution.OutcomePendingUnknown {
		t.Fatalf("taker replay class = %s, want a dispatched verdict", takerOut.OutcomeClass)
	}
	var seq int
	var outcome string
	if err := j.pool.QueryRow(ctx,
		`SELECT send_seq, outcome FROM tx_send_attempts WHERE attempt_id = $1 ORDER BY send_seq DESC LIMIT 1`,
		first.AttemptID).Scan(&seq, &outcome); err != nil {
		t.Fatalf("read taker dispatch row: %v", err)
	}
	if seq != 2 || (outcome != "rejected" && outcome != "unknown") {
		t.Fatalf("taker dispatch = seq %d outcome %s, want seq 2 rejected/unknown", seq, outcome)
	}
	if got := j.sendRowCount(first.AttemptID); got != 2 {
		t.Fatalf("send rows = %d, want 2 (same attempt history)", got)
	}

	// Replacement through the worker Driver: blocked with reason — no 010 fee
	// policy exists to construct a differing candidate, so the adapter refuses
	// before any write (deliberate gap, recorded, never faked).
	rep, err := worker.Driver.IssueAndAdvance(ctx, execution.StepRequest{
		IntentID: jj.intentID, RequestID: jj.requestID, CallerID: j.callerID,
		OwnerID: "worker-j2-taker", LeaseVersion: taker.Version, Action: execution.ActionReplace,
		AnchorAttemptID: first.AttemptID,
	})
	if err != nil {
		t.Fatalf("driver replace: %v", err)
	}
	if rep.OutcomeClass != execution.OutcomeRefusedBasis || rep.FinalStepState != execution.StepConverged {
		t.Fatalf("driver replace = %+v, want refused_basis/converged", rep)
	}
	repAdv, err := adapter.Advance(ctx, execution.AdvanceRequest{
		IntentID: jj.intentID, RequestID: jj.requestID, CallerID: j.callerID,
		OwnerID: "worker-j2-taker", LeaseVersion: taker.Version, StepID: "step-j2-replace-gap",
		Action: execution.ActionReplace, AnchorAttemptID: first.AttemptID,
	})
	if err != nil || repAdv.Class != execution.OutcomeRefusedBasis ||
		!strings.Contains(repAdv.Basis, "no 010 fee policy") {
		t.Fatalf("replace gap outcome = %+v %v, want refused_basis naming the missing fee policy", repAdv, err)
	}

	// Same intent / nonce / attempt history; no second intent.
	var intents, attempts int
	if err := j.pool.QueryRow(ctx, `SELECT count(*) FROM payment_intents WHERE intent_id = $1`, jj.intentID).Scan(&intents); err != nil {
		t.Fatal(err)
	}
	if err := j.pool.QueryRow(ctx, `SELECT count(*) FROM tx_attempts WHERE intent_id = $1`, jj.intentID).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if intents != 1 || attempts != 1 {
		t.Fatalf("intents=%d attempts=%d, want 1/1 (no second intent, no second attempt)", intents, attempts)
	}
	var nonce string
	if err := j.pool.QueryRow(ctx, `SELECT nonce::text FROM tx_attempts WHERE attempt_id = $1`, first.AttemptID).Scan(&nonce); err != nil {
		t.Fatal(err)
	}
	if nonce != "0" {
		t.Fatalf("nonce changed to %s", nonce)
	}
	var owner string
	var lease int64
	if err := j.pool.QueryRow(ctx, `SELECT owner_id, lease_version FROM execution_claims WHERE intent_id = $1`, jj.intentID).Scan(&owner, &lease); err != nil {
		t.Fatal(err)
	}
	if owner != "worker-j2-taker" || lease != taker.Version {
		t.Fatalf("claim row = %s/%d, want taker/%d", owner, lease, taker.Version)
	}
}

// TestJointDriverJ3UnknownReconciliation is 010:T048/J3 on the production
// worker path: a real dispatch response loss after node acceptance converges
// the worker step as unknown and the intent as reconciling; the joint 011
// reconcile loop persists the unknown honestly (never failure) and then
// completes on the confirmed-sent authority fact — no repaying.
func TestJointDriverJ3UnknownReconciliation(t *testing.T) {
	ctx := context.Background()
	j := newJointEnv(t)
	jj := j.admitIntent()
	j.markClaimed(jj)
	j.installTransferEmit(1000)

	j.store.WithChain(jointDropAfterAccept{real: j.eth}, 15*time.Second)
	worker, _ := j.productionWorker()

	out, err := worker.Driver.IssueAndAdvance(ctx, execution.StepRequest{
		IntentID: jj.intentID, RequestID: jj.requestID, CallerID: j.callerID,
		OwnerID: jj.ownerID, LeaseVersion: jj.claimVersion, Action: execution.ActionFirstBroadcast,
	})
	if err != nil {
		t.Fatalf("driver lost-response send: %v", err)
	}
	if out.OutcomeClass != execution.OutcomePendingUnknown || out.FinalStepState != execution.StepUnknown ||
		!out.Reconciling || out.AttemptID == "" {
		t.Fatalf("driver outcome = %+v, want pending_unknown/unknown/reconciling", out)
	}
	j.store.WithChain(j.eth, 15*time.Second)

	att, err := j.store.AttemptByID(ctx, out.AttemptID)
	if err != nil || att.State != "unknown" {
		t.Fatalf("attempt = %+v %v, want unknown", att, err)
	}
	var outcome string
	if err := j.pool.QueryRow(ctx,
		`SELECT outcome FROM tx_send_attempts WHERE attempt_id = $1 ORDER BY send_id DESC LIMIT 1`, out.AttemptID).
		Scan(&outcome); err != nil {
		t.Fatal(err)
	}
	if outcome != "unknown" {
		t.Fatalf("durable send outcome = %s, want unknown", outcome)
	}
	hash, raw, err := j.store.signingRow(ctx, out.AttemptID)
	if err != nil || len(raw) == 0 {
		t.Fatalf("bytes/hash lost: %v (%d bytes)", err, len(raw))
	}

	// Joint loop while the chain is still uncleared: honest persist, no failure
	// claim and no renewed dispatch.
	rec, err := worker.Reconciler.ReconcileIntent(ctx, jj.intentID, jj.ownerID, jj.claimVersion)
	if err != nil {
		t.Fatalf("joint reconcile while unknown: %v", err)
	}
	if rec.OutcomeClass == execution.OutcomeSent && rec.Converged {
		t.Fatalf("unknown wrongly converged as sent: %+v", rec)
	}
	if state := j.intentState(jj.intentID); state != execution.IntentReconciling {
		t.Fatalf("intent state while unknown = %s, want reconciling", state)
	}

	j.waitMined(hash)
	j.seedChainTruth()
	rec010, err := j.store.Reconcile(ctx, out.AttemptID, "")
	if err != nil || rec010.Classification != "included" || rec010.ReceiptEffect != "effective" {
		t.Fatalf("010 reconcile = %+v %v, want included/effective", rec010, err)
	}

	rec2, err := worker.Reconciler.ReconcileIntent(ctx, jj.intentID, jj.ownerID, jj.claimVersion)
	if err != nil {
		t.Fatalf("joint reconcile after resolution: %v", err)
	}
	if !strings.Contains(rec2.Basis, "authority confirmed sent") {
		t.Fatalf("joint reconcile did not consume the confirmed-sent fact: %+v", rec2)
	}
	if state := j.intentState(jj.intentID); state != execution.IntentCompleted {
		t.Fatalf("intent state = %s, want completed", state)
	}

	var attempts, intents, sends int
	if err := j.pool.QueryRow(ctx, `SELECT count(*) FROM tx_attempts WHERE intent_id = $1`, jj.intentID).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if err := j.pool.QueryRow(ctx, `SELECT count(*) FROM payment_intents WHERE intent_id = $1`, jj.intentID).Scan(&intents); err != nil {
		t.Fatal(err)
	}
	if err := j.pool.QueryRow(ctx, `SELECT count(*) FROM tx_send_attempts WHERE attempt_id = $1`, out.AttemptID).Scan(&sends); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 || intents != 1 || sends != 1 {
		t.Fatalf("repaying detected: attempts=%d intents=%d sends=%d, want 1/1/1", attempts, intents, sends)
	}
}

// TestJointDriverJ4ReorgRevisionAndProjectionOrder is 010:T049/J4 on the
// production worker path: the worker Driver broadcasts on the real node, the
// joint reorg after confirmation revises 010's receipt chain (orphaned then
// reconfirmed) without rebuilding the payment, and the 011 projection
// consumes 010's lifecycle revision in order (stale never overwrites newer;
// an unconfirmable read is marked possibly stale).
func TestJointDriverJ4ReorgRevisionAndProjectionOrder(t *testing.T) {
	ctx := context.Background()
	j := newJointEnv(t)
	jj := j.admitIntent()
	j.markClaimed(jj)
	j.installTransferEmit(1000)
	chain := &jointReorgRPC{real: j.eth}
	j.store.WithChain(chain, 15*time.Second)
	worker, _ := j.productionWorker()

	out, err := worker.Driver.IssueAndAdvance(ctx, execution.StepRequest{
		IntentID: jj.intentID, RequestID: jj.requestID, CallerID: j.callerID,
		OwnerID: jj.ownerID, LeaseVersion: jj.claimVersion, Action: execution.ActionFirstBroadcast,
	})
	if err != nil || out.OutcomeClass != execution.OutcomeSent {
		t.Fatalf("driver send = %+v %v", out, err)
	}
	j.waitMined(out.TxHash)
	j.seedChainTruth()
	if _, err := j.store.Reconcile(ctx, out.AttemptID, ""); err != nil {
		t.Fatalf("initial reconcile: %v", err)
	}
	var originalBlock int64
	if err := j.pool.QueryRow(ctx,
		`SELECT block_number FROM tx_receipts WHERE attempt_id = $1`, out.AttemptID).Scan(&originalBlock); err != nil {
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
	if _, err := j.store.Reconcile(ctx, out.AttemptID, ""); err != nil {
		t.Fatalf("reconcile after reorg: %v", err)
	}
	var orphanedReceipts int
	if err := j.pool.QueryRow(ctx,
		`SELECT count(*) FROM tx_receipts WHERE attempt_id = $1 AND canonicality = 'orphaned'`, out.AttemptID).Scan(&orphanedReceipts); err != nil {
		t.Fatal(err)
	}
	if orphanedReceipts == 0 {
		t.Fatal("reorg did not orphan the original receipt")
	}
	att, err := j.store.AttemptByID(ctx, out.AttemptID)
	if err != nil || att.State != "orphaned" {
		t.Fatalf("attempt after reorg = %+v %v, want orphaned", att, err)
	}
	var orphanEvents int
	if err := j.pool.QueryRow(ctx,
		`SELECT count(*) FROM tx_attempt_events WHERE attempt_id = $1 AND event = 'orphaned'`, out.AttemptID).Scan(&orphanEvents); err != nil {
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
	if _, err := j.store.Reconcile(ctx, out.AttemptID, ""); err != nil {
		t.Fatalf("reconcile after re-inclusion: %v", err)
	}
	var reconfirmed int
	if err := j.pool.QueryRow(ctx,
		`SELECT count(*) FROM tx_attempt_events WHERE attempt_id = $1 AND event = 'reconfirmed'`, out.AttemptID).Scan(&reconfirmed); err != nil {
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

	// 011 projection consumes the real 010 lifecycle revision.
	att, err = j.store.AttemptByID(ctx, out.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := worker.Reconciler.ReconcileIntent(ctx, jj.intentID, "", 0); err != nil {
		t.Fatalf("joint projection apply: %v", err)
	}
	row, found, err := execution.ReadProjection(ctx, j.pool, jj.requestID)
	if err != nil || !found {
		t.Fatalf("ReadProjection: %v found=%v", err, found)
	}
	if row.LifecycleVersion != att.RevisionSeq || row.LifecycleAttemptID == nil || *row.LifecycleAttemptID != out.AttemptID {
		t.Fatalf("projection = v%d/%v, want v%d/%s (010 authority)", row.LifecycleVersion, row.LifecycleAttemptID, att.RevisionSeq, out.AttemptID)
	}

	// Version order: a newer revision applies, an older one never overwrites.
	worker.Reconciler.Reader = scriptedReader{facts: execution.LifecycleFacts{RevisionVersion: 1000, Basis: "scripted"}}
	if _, err := worker.Reconciler.ReconcileIntent(ctx, jj.intentID, "", 0); err != nil {
		t.Fatalf("projection apply new: %v", err)
	}
	row, found, err = execution.ReadProjection(ctx, j.pool, jj.requestID)
	if err != nil || !found || row.LifecycleVersion != 1000 {
		t.Fatalf("projection = %+v found=%v err=%v, want lifecycle_version 1000", row, found, err)
	}
	worker.Reconciler.Reader = scriptedReader{facts: execution.LifecycleFacts{RevisionVersion: 50, Basis: "scripted-stale"}}
	if _, err := worker.Reconciler.ReconcileIntent(ctx, jj.intentID, "", 0); err != nil {
		t.Fatalf("projection apply stale: %v", err)
	}
	row, found, err = execution.ReadProjection(ctx, j.pool, jj.requestID)
	if err != nil || !found {
		t.Fatalf("ReadProjection after stale: %v found=%v", err, found)
	}
	if row.LifecycleVersion != 1000 {
		t.Fatalf("stale revision overwrote newer: lifecycle_version = %d, want 1000", row.LifecycleVersion)
	}

	worker.Reconciler.Reader = scriptedReader{err: errors.New("authority unavailable")}
	if _, err := worker.Reconciler.ReconcileIntent(ctx, jj.intentID, "", 0); err != nil {
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

// TestJointDriverJ5LockLossDetectablePath is 010:T050/J5 on the production
// worker path, detectable path only: a pause committed before a dispatch that
// recorded pause=none proves a stale basis, so 010 freezes the intent's
// further sends (011 observes the freeze), chain observation/reconcile stay
// available, and a controlled manual release lifts only the freeze cause while
// a resend still re-verifies every gate.
//
// It asserts what is NOT claimed: no "window is tiny/rare" property, no
// detection guarantee for unobservable faults.
func TestJointDriverJ5LockLossDetectablePath(t *testing.T) {
	ctx := context.Background()
	j := newJointEnv(t)
	jj := j.admitIntent()
	j.markClaimed(jj)
	j.installTransferEmit(1000)
	worker, adapter := j.productionWorker()

	out, err := worker.Driver.IssueAndAdvance(ctx, execution.StepRequest{
		IntentID: jj.intentID, RequestID: jj.requestID, CallerID: j.callerID,
		OwnerID: jj.ownerID, LeaseVersion: jj.claimVersion, Action: execution.ActionFirstBroadcast,
	})
	if err != nil || out.OutcomeClass != execution.OutcomeSent {
		t.Fatalf("driver send = %+v %v", out, err)
	}

	// The unperceived lock-loss evidence: a real pause whose created_at
	// precedes the dispatch that recorded pause=none.
	j.mustExec(`INSERT INTO indexer_pause (chain_id, height, expected_hash, actual_hash, kind, created_at)
		VALUES ($1,100,$2,$3,'hash_mismatch', now() - interval '1 hour')`,
		jointChainID, blockHashHex(100), blockHashHex(101))

	// Reconcile runs the version-vs-change-evidence comparison. Chain
	// observation is allowed even while frozen.
	if _, err := j.store.Reconcile(ctx, out.AttemptID, ""); err != nil {
		t.Fatalf("reconcile (observation still allowed): %v", err)
	}
	var freezeCause, freezeEvidence string
	if err := j.pool.QueryRow(ctx,
		`SELECT cause, evidence FROM tx_intent_freezes WHERE intent_id = $1 AND released_at IS NULL`, jj.intentID).
		Scan(&freezeCause, &freezeEvidence); err != nil {
		t.Fatalf("freeze row missing: %v", err)
	}
	if freezeCause != "protection_loss_residual" || freezeEvidence == "" {
		t.Fatalf("freeze = %s/%q, want protection_loss_residual with evidence", freezeCause, freezeEvidence)
	}
	var frozenEvents int
	if err := j.pool.QueryRow(ctx,
		`SELECT count(*) FROM tx_attempt_events WHERE attempt_id = $1 AND event = 'frozen'`, out.AttemptID).Scan(&frozenEvents); err != nil {
		t.Fatal(err)
	}
	if frozenEvents == 0 {
		t.Fatal("no frozen event recorded")
	}

	// The 011 loop consumes 010's freeze fact and records its own marker.
	if _, err := worker.Reconciler.ReconcileIntent(ctx, jj.intentID, jj.ownerID, jj.claimVersion); err != nil {
		t.Fatalf("joint freeze reconcile: %v", err)
	}
	frozen, class, err := execution.IsFrozen(ctx, j.pool, jj.intentID)
	if err != nil || !frozen || class == "" {
		t.Fatalf("011 IsFrozen = %v/%s %v, want frozen", frozen, class, err)
	}

	// Every further send refuses; zero dispatch; observation/reconcile allowed.
	sendsBefore := j.sendRowCount(out.AttemptID)
	stepsBefore := j.stepCount(jj.intentID)
	if replay, err := worker.Driver.IssueAndAdvance(ctx, execution.StepRequest{
		IntentID: jj.intentID, RequestID: jj.requestID, CallerID: j.callerID,
		OwnerID: jj.ownerID, LeaseVersion: jj.claimVersion, Action: execution.ActionReplay,
		AnchorAttemptID: out.AttemptID, ExpectedTxHash: out.TxHash,
	}); err != nil {
		t.Fatalf("driver replay while frozen: %v", err)
	} else if replay.Refusal == "" {
		t.Fatalf("driver replay while frozen was not refused: %+v", replay)
	}
	// 010's own freeze gate through the production adapter (freeze beats the
	// pause gate in 010's order).
	adv, err := adapter.Advance(ctx, execution.AdvanceRequest{
		IntentID: jj.intentID, RequestID: jj.requestID, CallerID: j.callerID,
		OwnerID: jj.ownerID, LeaseVersion: jj.claimVersion, StepID: "step-j5-frozen-010",
		Action: execution.ActionReplay, AnchorAttemptID: out.AttemptID, ExpectedTxHash: out.TxHash,
	})
	if err != nil || adv.Class != execution.OutcomeRefusedGate || !strings.Contains(adv.Basis, string(ClassIntentFrozen)) {
		t.Fatalf("010 frozen replay = %+v %v, want refused_gate with %s", adv, err, ClassIntentFrozen)
	}
	if got := j.sendRowCount(out.AttemptID); got != sendsBefore {
		t.Fatalf("frozen send dispatched: %d -> %d", sendsBefore, got)
	}
	if got := j.stepCount(jj.intentID); got != stepsBefore {
		t.Fatalf("frozen send wrote a step: %d -> %d", stepsBefore, got)
	}
	if _, err := j.store.Reconcile(ctx, out.AttemptID, ""); err != nil {
		t.Fatalf("reconcile while frozen: %v", err)
	}

	// Controlled manual release lifts ONLY the freeze cause.
	j.store.WithReleaseToken("joint-release")
	if _, err := j.store.Release(ctx, &ReleaseRequest{IntentID: jj.intentID, Permission: "wrong"}); refusalClass(err) != ClassReleaseNotPermitted {
		t.Fatalf("wrong release permission = %v", err)
	}
	ok, err := j.store.Release(ctx, &ReleaseRequest{
		IntentID: jj.intentID, Permission: "joint-release", Operator: "ops-j5", Reason: "reviewed", Basis: "evidence-j5",
	})
	if err != nil || !ok {
		t.Fatalf("controlled release = %v %v", ok, err)
	}
	var releasedAt *time.Time
	if err := j.pool.QueryRow(ctx,
		`SELECT released_at FROM tx_intent_freezes WHERE intent_id = $1`, jj.intentID).Scan(&releasedAt); err != nil {
		t.Fatal(err)
	}
	if releasedAt == nil {
		t.Fatal("010 freeze cause not released")
	}
	reader, err := NewLifecycleLive(j.pool, j.store)
	if err != nil {
		t.Fatal(err)
	}
	facts, err := reader.Read(ctx, jj.intentID)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := execution.FreezeClass(facts); ok {
		t.Fatalf("joint reader still surfaces the released freeze: %+v", facts.Unknown)
	}

	// The pause is still present: release never overrides another gate, and a
	// resend re-verifies the full gate sequence on both sides.
	res, err := adapter.Advance(ctx, execution.AdvanceRequest{
		IntentID: jj.intentID, RequestID: jj.requestID, CallerID: j.callerID,
		OwnerID: jj.ownerID, LeaseVersion: jj.claimVersion, StepID: "step-j5-post-release-010",
		Action: execution.ActionReplay, AnchorAttemptID: out.AttemptID, ExpectedTxHash: out.TxHash,
	})
	if err != nil || res.Class != execution.OutcomeRefusedGate || !strings.Contains(res.Basis, string(ClassPausePresent)) {
		t.Fatalf("post-release 010 resend = %+v %v, want refused_gate with %s", res, err, ClassPausePresent)
	}
	if replay, err := worker.Driver.IssueAndAdvance(ctx, execution.StepRequest{
		IntentID: jj.intentID, RequestID: jj.requestID, CallerID: j.callerID,
		OwnerID: jj.ownerID, LeaseVersion: jj.claimVersion, Action: execution.ActionReplay,
		AnchorAttemptID: out.AttemptID, ExpectedTxHash: out.TxHash,
	}); err != nil {
		t.Fatalf("driver post-release resend: %v", err)
	} else if replay.Refusal != execution.ClassRecoveryPaused {
		t.Fatalf("driver post-release refusal = %s (%s), want %s", replay.Refusal, replay.Basis, execution.ClassRecoveryPaused)
	}
	if got := j.sendRowCount(out.AttemptID); got != sendsBefore {
		t.Fatalf("post-release resend wrote a row: %d -> %d", sendsBefore, got)
	}
	var pauses int
	if err := j.pool.QueryRow(ctx, `SELECT count(*) FROM indexer_pause WHERE chain_id = $1`, jointChainID).Scan(&pauses); err != nil {
		t.Fatal(err)
	}
	if pauses == 0 {
		t.Fatal("release cleared an unrelated pause (it must not)")
	}
	var releasedEvents int
	if err := j.pool.QueryRow(ctx,
		`SELECT count(*) FROM tx_attempt_events WHERE attempt_id = $1 AND event = 'released'`, out.AttemptID).Scan(&releasedEvents); err != nil {
		t.Fatal(err)
	}
	if releasedEvents == 0 {
		t.Fatal("release audit event missing")
	}
}
