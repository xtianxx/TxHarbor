//go:build integration

// Batch C2: the N-way concurrent shared-request cover for the 005 privileged
// policy switch (T024), complementing the sequential request_id idempotency
// tests in confirmauth_integration_test.go.
//
// Defect guarded: N operators fire the SAME authorization request (identical
// (chain_id, request_id) intent, expected_old_seq=1, new threshold 25) at once.
// Under the pre-tx classify / in-tx UNIQUE / post-lock re-verify ordering
// (confirmauth.go steps 1-5) a race could append a second policy version, bind
// the request_id twice, or strand a loser on a non-converging error.
//
// Invariant (005 data-model.md §授权切换协议 steps 1-5; Table 2 request_id row;
// migrations/000005_confirmation_tracking.sql line 37
// UNIQUE (chain_id, request_id)): policy_seq advances exactly once (1 -> 2),
// exactly one history row carries the request_id, and every loser either reads
// back the recorded result or is refused as expired — never a second append.
//
// Gap: the pre-existing suite proved same-param replay and different-param
// refusal sequentially only; the N-way race on one shared request_id across
// the classify/UNIQUE/re-verify branches was uncovered.
//
// Level: integration (testcontainers PostgreSQL 18, real BEGIN..COMMIT).
//
// Criteria: at least one caller succeeds on row (2, 25); every loss is either a
// Recorded replay of that row or ErrConfirmAuthExpired; exactly 2 policy rows
// exist, MAX(policy_seq)=2, exactly one request_id row exists, the seq-1
// bootstrap row is untouched, and zero pause rows were written.
package indexer

import (
	"errors"
	"sync"
	"testing"
)

// TestConfirmAuthSameRequestIDNWayConverges releases N goroutines through one
// start barrier, all carrying the same ConfirmAuthRequest, and proves the
// request identity converges to exactly one appended policy version.
func TestConfirmAuthSameRequestIDNWayConverges(t *testing.T) {
	pool, ctx := confirmAuthTestPool(t)
	const (
		chainID = int64(88)
		callers = 8
	)
	confirmSeedPolicyRow(t, ctx, pool, chainID, 1, 10, nil, "bootstrap", nil)

	req := ConfirmAuthRequest{
		ChainID: chainID, RequestID: "sw-nway", ExpectedOldSeq: 1,
		NewThresholdRaw: "25", Operator: "op-nway", Reason: "converge one request id",
	}

	// Barrier + WaitGroup: every worker blocks on start, then fires together;
	// one shared request value is copied into each call.
	type outcome struct {
		res ConfirmAuthResult
		err error
	}
	start := make(chan struct{})
	outcomes := make([]outcome, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			res, err := AuthorizeConfirmationPolicy(ctx, pool, req)
			outcomes[i] = outcome{res: res, err: err}
		}(i)
	}
	close(start)
	wg.Wait()

	// Loser alphabet: a Recorded replay of the winner's row or an expiry
	// refusal (a goroutine whose pre-tx/locked read already saw seq 2). Any
	// other outcome is a convergence defect.
	ok, replayed, expired := 0, 0, 0
	for i, out := range outcomes {
		if out.err != nil {
			if !errors.Is(out.err, ErrConfirmAuthExpired) {
				t.Fatalf("caller %d lost with %v, want only %v or a recorded replay", i, out.err, ErrConfirmAuthExpired)
			}
			expired++
			continue
		}
		ok++
		if out.res.PolicySeq != 2 || out.res.Threshold != 25 {
			t.Fatalf("caller %d result = %+v, want the winner row {PolicySeq:2 Threshold:25}", i, out.res)
		}
		if out.res.Recorded {
			replayed++
		}
	}
	if ok < 1 {
		t.Fatalf("no caller succeeded (replayed=%d expired=%d); the shared request must converge for at least one",
			replayed, expired)
	}

	// Exactly one request_id row (the UNIQUE identity), one appended version,
	// and no third seq — never a second row/seq.
	var requestRows, maxSeq int64
	if err := pool.QueryRow(ctx,
		`SELECT count(*), COALESCE(MAX(policy_seq), 0) FROM confirmation_policy_history
		  WHERE chain_id = $1 AND request_id = $2`, chainID, req.RequestID).Scan(&requestRows, &maxSeq); err != nil {
		t.Fatalf("read request_id rows: %v", err)
	}
	if requestRows != 1 {
		t.Fatalf("request_id rows = %d, want exactly 1 (UNIQUE (chain_id, request_id))", requestRows)
	}
	if n := depositCountRows(t, ctx, pool, "confirmation_policy_history", chainID); n != 2 {
		t.Fatalf("policy rows = %d, want 2 (bootstrap + exactly one switch)", n)
	}
	if maxSeq != 2 {
		t.Fatalf("MAX(policy_seq) = %d, want 2 (seq advances exactly once)", maxSeq)
	}

	// The appended row carries the full audit set with expected_old_seq=1.
	thr, prev, prevNull, op, reason, reqID, expOld := confirmAuthReadRow(t, ctx, pool, chainID, 2)
	if thr != 25 || prevNull || prev != 1 || op != "op-nway" ||
		reason != "converge one request id" || reqID != req.RequestID || expOld != 1 {
		t.Fatalf("row seq2 = (thr=%d prev=%d null=%v op=%q reason=%q req=%q exp=%d), "+
			"want (25 1 false op-nway \"converge one request id\" %q 1)",
			thr, prev, prevNull, op, reason, reqID, expOld, req.RequestID)
	}
	var seq1Thr int64
	var seq1ReqNull bool
	if err := pool.QueryRow(ctx,
		`SELECT threshold, request_id IS NULL FROM confirmation_policy_history
		  WHERE chain_id = $1 AND policy_seq = 1`, chainID).Scan(&seq1Thr, &seq1ReqNull); err != nil {
		t.Fatalf("read seq-1 row: %v", err)
	}
	if seq1Thr != 10 || !seq1ReqNull {
		t.Fatalf("seq-1 row = (threshold=%d request_id_null=%v), want the untouched bootstrap (10 true)", seq1Thr, seq1ReqNull)
	}
	confirmAuthAssertZeroPause(t, ctx, pool, chainID)
}
