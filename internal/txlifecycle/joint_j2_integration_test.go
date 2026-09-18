//go:build integration

package txlifecycle

import (
	"context"
	"testing"
	"time"

	"github.com/xtianxx/txharbor/internal/execution"
)

// TestJointJ2ExpiredWorkerIsolationAndTakeover is T047/J2 SUPPLEMENTARY
// shared-state coverage: it drives the real 010 Store directly so the 010
// claim gate is exercised for all three prepared send kinds, including a
// prepared replacement attempt (which the production adapter cannot construct
// without a fee policy). The worker-Driver J2 acceptance is
// TestJointDriverJ2ExpiredWorkerIsolationAndTakeover
// (joint_driver_integration_test.go).
//
// The scenario: an expired/fenced worker's three send kinds are refused by
// 010's real claim gate (zero dispatch, zero new send rows), then a legitimate
// taker re-verifies the same intent/claim and resumes on the same intent,
// nonce and attempt history.
func TestJointJ2ExpiredWorkerIsolationAndTakeover(t *testing.T) {
	t.Log("supplementary shared-state J2: direct 010 Store drive, not the worker-Driver link")
	ctx := context.Background()
	j := newJointEnv(t)
	jj := j.admit()
	j.installTransferEmit(1000)

	if res, err := j.store.Send(ctx, &SendRequest{AttemptID: jj.attemptID, Kind: SendInitial, Claim: j.claim(jj)}); err != nil || res.Outcome != "accepted" {
		t.Fatalf("worker-1 initial send = %+v %v", res, err)
	}

	j.mustExec(`UPDATE execution_claims SET acquired_at = now() - interval '2 hours', expires_at = now() - interval '1 hour' WHERE intent_id = $1`, jj.intentID)

	sendRows := func() int {
		t.Helper()
		var n int
		if err := j.pool.QueryRow(ctx, `SELECT count(*) FROM tx_send_attempts WHERE attempt_id = $1`, jj.attemptID).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	before := sendRows()

	// The fenced worker's three send kinds all refuse at the claim gate.
	res, err := j.store.Send(ctx, &SendRequest{AttemptID: jj.attemptID, Kind: SendInitial, Claim: j.claim(jj)})
	assertJointRefusal(t, res, err, ClassClaimExpired)
	if got := sendRows(); got != before {
		t.Fatalf("fenced initial wrote a send row: %d -> %d", before, got)
	}
	res, err = j.store.Send(ctx, &SendRequest{AttemptID: jj.attemptID, Kind: SendReplay, Claim: j.claim(jj)})
	assertJointRefusal(t, res, err, ClassClaimExpired)
	if got := sendRows(); got != before {
		t.Fatalf("fenced replay wrote a send row: %d -> %d", before, got)
	}

	// Replacement attempt under the same intent/binding with a fee change. The
	// replacement uses a FRESH grant: the current 009 submit path admits a
	// reused-grant replacement through EvaluateGrantReuse but then re-applies
	// EvaluateGrantScope, whose request_id == signing_request_id check rejects
	// every new signing identity under the anchor's scope (recorded finding).
	replAuth := "wa-j2-fresh"
	replSR := "sr-j2-repl"
	j.mustExec(`INSERT INTO withdrawal_authorizations (authorization_id, caller_id, chain_id, asset, recipient, amount, state)
		VALUES ($1,$2,$3,$4,$5,$6,'active')`, replAuth, j.callerID, jointChainID, j.asset, j.recipient, j.amount)
	j.mustExec(`INSERT INTO withdrawal_authorization_scopes
		(authorization_id, intent_id, request_id, sender, fee_max_total, fee_max_per_gas, fee_max_priority, allows_fee_replacement, authorization_version, attested_by)
		VALUES ($1,$2,$3,$4,$5,$6,$7,TRUE,1,'joint')`, replAuth, jj.intentID, replSR, j.sender, int64(1e15), int64(2e9), int64(15e8))
	replReq := &PrepareRequest{
		AttemptID: "att-j2-repl", SigningRequestID: replSR, ReplacementOf: jj.attemptID,
		IntentID: jj.intentID, BindingRef: jj.bindingID, AuthorizationID: replAuth,
		AuthorizationVersion: 1, RecoveryVersion: 0, ChainID: uint64(jointChainID), Sender: j.sender, Nonce: jj.nonce,
		TxType: TxTypeDynamicFee, GasLimit: "100000",
		MaxFeePerGas: "1200000000", MaxPriorityFeePerGas: "100000000",
		Asset: j.asset, Recipient: j.recipient, Amount: j.amount,
	}
	if _, err := j.store.PrepareAttempt(ctx, replReq); err != nil {
		t.Fatalf("replacement prepare: %v", err)
	}
	res, err = j.store.Send(ctx, &SendRequest{AttemptID: "att-j2-repl", Kind: SendInitial, Claim: j.claim(jj)})
	assertJointRefusal(t, res, err, ClassClaimExpired)
	var replSends int
	if err := j.pool.QueryRow(ctx, `SELECT count(*) FROM tx_send_attempts WHERE attempt_id = 'att-j2-repl'`).Scan(&replSends); err != nil {
		t.Fatal(err)
	}
	if replSends != 0 {
		t.Fatalf("fenced replacement wrote %d send row(s)", replSends)
	}

	// Legitimate taker: real 011 claim takeover after expiry.
	claims, err := execution.NewClaimStore(j.pool, 30*time.Second, 5*time.Minute)
	if err != nil {
		t.Fatalf("NewClaimStore: %v", err)
	}
	taker, err := claims.Claim(ctx, jj.intentID, "worker-j2-taker")
	if err != nil || !taker.Acquired {
		t.Fatalf("takeover claim = %+v %v", taker, err)
	}
	if taker.Version <= jj.claimVersion {
		t.Fatalf("takeover version %d not monotonic over %d", taker.Version, jj.claimVersion)
	}

	takerRef := ClaimRef{IntentID: jj.intentID, WorkerID: "worker-j2-taker", LeaseVersion: taker.Version}
	res, err = j.store.Send(ctx, &SendRequest{AttemptID: jj.attemptID, Kind: SendReplay, Claim: takerRef})
	// The taker passes every gate and the dispatch is made; the chain verdict
	// (the original nonce is already consumed -> anvil nonce_too_low) is a
	// dispatch outcome, never a claim refusal.
	if err != nil {
		t.Fatalf("taker replay error = %v", err)
	}
	if res.Outcome == "blocked" || res.RefusalClass != "" {
		t.Fatalf("taker replay blocked = %+v, want a dispatched verdict", res)
	}
	if res.SendSeq != 2 {
		t.Fatalf("taker replay send_seq = %d, want 2 (same attempt history)", res.SendSeq)
	}

	// Same intent / nonce / attempt history; no second intent.
	var intents, attempts int
	if err := j.pool.QueryRow(ctx, `SELECT count(*) FROM payment_intents WHERE intent_id = $1`, jj.intentID).Scan(&intents); err != nil {
		t.Fatal(err)
	}
	if err := j.pool.QueryRow(ctx, `SELECT count(*) FROM tx_attempts WHERE intent_id = $1`, jj.intentID).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if intents != 1 {
		t.Fatalf("second intent created: %d", intents)
	}
	if attempts != 2 {
		t.Fatalf("attempt history = %d, want 2 (original + replacement)", attempts)
	}
	var nonce string
	if err := j.pool.QueryRow(ctx, `SELECT nonce::text FROM tx_attempts WHERE attempt_id = $1`, jj.attemptID).Scan(&nonce); err != nil {
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

func assertJointRefusal(t *testing.T, res SendResult, err error, want RefusalClass) {
	t.Helper()
	if got := refusalClass(err); got != want {
		t.Fatalf("refusal = %s (%v), want %s", got, err, want)
	}
	if res.Outcome != "blocked" || res.RefusalClass != string(want) {
		t.Fatalf("result = %+v, want blocked/%s", res, want)
	}
}
