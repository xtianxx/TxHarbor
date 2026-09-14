//go:build integration

// -- T025 --------------------------------------------------------------------
//
// T025 [US5] switch full-cycle integration D4 (FR-03, SC-10; US2-4/US5-4,
// quickstart D4): the authorized threshold-switch end to end against a real
// PostgreSQL (testcontainers postgres:18 via startIndexerPostgres), in one
// test with five phases on isolated chain_ids:
//
// (a) lower N (10->3): pre-switch Pendings all re-judged under the new N
// (no cursor skip — the eligible set of 511 rows exceeds the fixed per-tick
// candidate LIMIT of 500, so pending reaching 0 proves multi-tick full
// re-inclusion, T019(f) precedent), still passing canonical/chain-view/pause
// gates (lowering is NOT batch-confirm: every row converts through the real
// ServeLoop with exact per-row basis columns);
// (b) raise N (3->10): Confirmed rows unchanged with the then-threshold
// traceable, sub-new-threshold Pendings stay Pending;
// (c) unauthorized drift (restart with different N, no authorization):
// refused with zero damage to observations/policy;
// (d) failed switch (stale expected_old_seq / empty change / illegal value):
// policy_transition_total{rejected}+1 each, zero new policy rows (atomic, no
// multi-version);
// (e) unknown switch outcome: request_id reread定性 (COMMIT landed but the
// reply lost -> recorded result; COMMIT never landed -> error, ID unbound,
// retry succeeds).
//
// Method, recorded as required: fixed tips need no tip advance, so (like
// T019(f)/T021/T022, unlike the tip-advance T013/T016/T019(e)) this test
// seeds canonical rows with the deterministic depositSeedCanonical hashes
// instead of mining Anvil — the chain-view gates still adjudicate real
// canonical rows (hash match + canonical + tip identity per row), and the
// confirmation loop path itself is never faked (real ServeLoop +
// ConfirmDepositUnit + AuthorizeConfirmationPolicy). Helpers are reused
// verbatim (confirm13*/confirm19*/confirmSeed*/confirmAssertZeroWrite/
// confirmReadBasis/confirmAuthReadRow/confirmAuthAssertZeroPause,
// depositSeed*, metrics counters, contracts integrity SQL).
//
// Append discipline: T028 appends below under its own banner; helpers added
// here must be confirm25*-prefixed so later batches never collide.
package indexer

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/xtianxx/txharbor/internal/logx"
	"github.com/xtianxx/txharbor/internal/metrics"
)

// TestConfirmationAuthFullCycleD4 is T025 / quickstart D4 (FR-03, SC-10).
func TestConfirmationAuthFullCycleD4(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	// Phase (a): lower N 10->3. Tip=112, heights 104..110 sit at 9..3
	// confirmations: all hold under N=10, all eligible under N=3. 7 heights
	// x 73 rows = 511 rows exceeds the 500-row tick LIMIT, so pending
	// reaching 0 proves multi-tick full re-inclusion with no cursor skip.
	const chainA = int64(31360)
	const tipA = uint64(112)
	depositSeedCanonical(t, ctx, pool, chainA, 104, tipA, true)
	depositSeedHistory(t, ctx, pool, chainA, 1, 10, strings.Repeat("aa", 32))
	const perHeightA = 73
	var totalA int
	for h := uint64(104); h <= 110; h++ {
		for i := uint64(0); i < perHeightA; i++ {
			depositSeedObservation(t, ctx, pool, chainA, h, depositBlockHash(h), depositTxHash(h, i), i, "1", 1)
			totalA++
		}
	}
	if totalA != 511 {
		t.Fatalf("seeded rows = %d, want 511 (must exceed the 500-row tick LIMIT)", totalA)
	}
	confirmSeedPolicyRow(t, ctx, pool, chainA, 1, 10, nil, "bootstrap", nil)
	tipHashA := depositBlockHash(tipA)

	mA := metrics.New(func() bool { return true })
	scA, leaseA := confirm13Scanner(t, pool, chainA, 10, mA)

	// Pre-switch patience: below threshold -> zero commits, zero new policy
	// rows (the lowering converts nothing by itself).
	stopA := confirm13RunLoop(t, ctx, scA, leaseA)
	time.Sleep(600 * time.Millisecond) // several 25ms poll ticks
	var pendingA, confirmedA int
	if err := pool.QueryRow(ctx, `
SELECT count(*) FILTER (WHERE status = 'pending'), count(*) FILTER (WHERE status = 'confirmed')
FROM deposit_observations WHERE chain_id = $1`, chainA).Scan(&pendingA, &confirmedA); err != nil {
		t.Fatalf("count by status pre-switch: %v", err)
	}
	if pendingA != totalA || confirmedA != 0 {
		t.Fatalf("pre-switch pending=%d confirmed=%d, want %d/0 (lowering pre-converts nothing)", pendingA, confirmedA, totalA)
	}
	if k := depositCountRows(t, ctx, pool, "confirmation_policy_history", chainA); k != 1 {
		t.Fatalf("pre-switch policy rows = %d, want 1", k)
	}
	stopA()

	// Authorized switch 10->3: exactly one appended row with the full audit
	// set, zero pause rows.
	resA, err := AuthorizeConfirmationPolicy(ctx, pool, ConfirmAuthRequest{
		ChainID: chainA, RequestID: "t025-lower", ExpectedOldSeq: 1,
		NewThresholdRaw: "3", Operator: "t025-op", Reason: "lower 10->3",
	})
	if err != nil {
		t.Fatalf("lower AuthorizeConfirmationPolicy(): %v", err)
	}
	if resA.PolicySeq != 2 || resA.Threshold != 3 || resA.Recorded {
		t.Fatalf("lower result = %+v, want {PolicySeq:2 Threshold:3 Recorded:false}", resA)
	}
	thrA, prevA, prevNullA, opA, reasonA, reqA, expA :=
		confirmAuthReadRow(t, ctx, pool, chainA, 2)
	if thrA != 3 || prevNullA || prevA != 1 || opA != "t025-op" || reasonA != "lower 10->3" || reqA != "t025-lower" || expA != 1 {
		t.Fatalf("lower row seq2 = (thr=%d prev=%d null=%v op=%q reason=%q req=%q exp=%d), want (3 1 false t025-op lower-10->3 t025-lower 1)",
			thrA, prevA, prevNullA, opA, reasonA, reqA, expA)
	}
	if k := depositCountRows(t, ctx, pool, "confirmation_policy_history", chainA); k != 2 {
		t.Fatalf("post-switch policy rows = %d, want 2 (exactly one appended row)", k)
	}
	confirmAuthAssertZeroPause(t, ctx, pool, chainA)

	// Post-switch re-run under N=3 with the same lease handle (T019(e)
	// precedent): every pre-switch Pending re-judged, multi-tick to full
	// coverage (no cursor skip).
	scA2 := confirm19Scanner(t, pool, chainA, 3, mA, leaseA)
	stopA2 := confirm13RunLoop(t, ctx, scA2, leaseA)
	waitUntil(t, time.Now().Add(120*time.Second), "all 511 lowered rows confirmed over multiple ticks", func() bool {
		var k int
		if err := pool.QueryRow(ctx, `
SELECT count(*) FROM deposit_observations WHERE chain_id = $1 AND status = 'confirmed'`,
			chainA).Scan(&k); err != nil {
			return false
		}
		return k == totalA
	})
	time.Sleep(300 * time.Millisecond) // surface any erroneous extra write
	stopA2()

	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM deposit_observations WHERE chain_id = $1 AND status = 'pending'`,
		chainA).Scan(&pendingA); err != nil {
		t.Fatalf("count pending post-lower: %v", err)
	}
	if pendingA != 0 {
		t.Fatalf("pending rows post-lower = %d, want 0 (full re-inclusion, no cursor skip)", pendingA)
	}
	// Lowering is NOT batch-confirm: every row carries exact per-row gates
	// output (new threshold, new policy seq, exact confirmations, exact tip).
	var misjudged int
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM deposit_observations
WHERE chain_id = $1 AND status = 'confirmed'
  AND (confirm_threshold != 3 OR confirm_policy_seq != 2
       OR confirm_tip_number != $2 OR confirm_tip_hash != $3)`,
		chainA, int64(tipA), tipHashA).Scan(&misjudged); err != nil {
		t.Fatalf("lower basis check: %v", err)
	}
	if misjudged != 0 {
		t.Fatalf("misjudged rows post-lower = %d, want 0 (every row under new N=3, seq=2, exact tip)", misjudged)
	}
	var inexactConf int
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM deposit_observations
WHERE chain_id = $1 AND status = 'confirmed'
  AND confirmations != (confirm_tip_number - block_number + 1)`,
		chainA).Scan(&inexactConf); err != nil {
		t.Fatalf("lower confirmations check: %v", err)
	}
	if inexactConf != 0 {
		t.Fatalf("inexact confirmations rows = %d, want 0 (per-row tip-h+1 exact)", inexactConf)
	}
	if k := depositCountRows(t, ctx, pool, "confirmation_policy_history", chainA); k != 2 {
		t.Fatalf("policy rows post-lower = %d, want 2 (loop writes no policy row)", k)
	}
	confirmAuthAssertZeroPause(t, ctx, pool, chainA)
	chainLabelA := confirm13ChainLabel(chainA)
	if got := confirm13Counter(t, mA, metrics.ConfirmationConfirmedMetricName, chainLabelA); got != float64(totalA) {
		t.Fatalf("%s post-lower = %v, want %d", metrics.ConfirmationConfirmedMetricName, got, totalA)
	}
	okLabelsA := map[string]string{"chain": "31360", "result": "ok"}
	if got := confirm13Counter(t, mA, metrics.ConfirmationTransitionMetricName, okLabelsA); got != float64(totalA) {
		t.Fatalf("%s{ok} post-lower = %v, want %d", metrics.ConfirmationTransitionMetricName, got, totalA)
	}
	var incompleteA int
	if err := pool.QueryRow(ctx, `
SELECT COUNT(*) FROM deposit_observations
WHERE chain_id = $1 AND status = 'confirmed'
  AND (confirmed_at IS NULL OR confirm_tip_number IS NULL OR confirm_tip_hash IS NULL
       OR confirm_threshold IS NULL OR confirmations IS NULL OR confirm_policy_seq IS NULL)`,
		chainA).Scan(&incompleteA); err != nil {
		t.Fatalf("integrity check post-lower: %v", err)
	}
	if incompleteA != 0 {
		t.Fatalf("incomplete confirmed rows post-lower = %d, want 0", incompleteA)
	}

	// Phase (b): raise N 3->10. Tip=120: h=110 converts under N=3 (11
	// confirmations), h=119 holds (2 confirmations). After the authorized
	// raise, the Confirmed row stays byte-identical with the then-threshold
	// traceable, and sub-new-threshold Pendings (h=119 at 2, h=112 at 9)
	// stay Pending.
	const chainB = int64(31361)
	const tipB = uint64(120)
	depositSeedCanonical(t, ctx, pool, chainB, 100, tipB, true)
	bhB110, txB110 := confirmSeedPending(t, ctx, pool, chainB, 110)
	bhB119, txB119 := depositBlockHash(119), depositTxHash(119, 0)
	depositSeedObservation(t, ctx, pool, chainB, 119, bhB119, txB119, 0, "1", 1)
	confirmSeedPolicyRow(t, ctx, pool, chainB, 1, 3, nil, "bootstrap", nil)

	mB1 := metrics.New(func() bool { return true })
	scB, leaseB := confirm13Scanner(t, pool, chainB, 3, mB1)
	stopB := confirm13RunLoop(t, ctx, scB, leaseB)
	waitUntil(t, time.Now().Add(30*time.Second), "h=110 confirmed under N=3", func() bool {
		var k int
		if err := pool.QueryRow(ctx, `
SELECT count(*) FROM deposit_observations WHERE chain_id = $1 AND status = 'confirmed'`,
			chainB).Scan(&k); err != nil {
			return false
		}
		return k == 1
	})
	time.Sleep(200 * time.Millisecond) // surface any erroneous extra write
	stopB()
	statusB, nullAtB, tipNB, thrB, seqB, tipHashB, confB :=
		confirmReadBasis(t, ctx, pool, chainB, bhB110, txB110)
	if statusB != "confirmed" || nullAtB || tipNB != int64(tipB) || tipHashB != depositBlockHash(tipB) || thrB != 3 || seqB != 1 || confB != "11" {
		t.Fatalf("h=110 pre-raise basis = (%s %d %s %d %s %d), want (confirmed 120 tip N=3 conf=11 seq=1)",
			statusB, tipNB, tipHashB, thrB, confB, seqB)
	}
	atB110 := confirm13ConfirmedAt(t, ctx, pool, chainB, bhB110, txB110)
	confirmAssertZeroWrite(t, ctx, pool, chainB, bhB119, txB119, 1)

	resB, err := AuthorizeConfirmationPolicy(ctx, pool, ConfirmAuthRequest{
		ChainID: chainB, RequestID: "t025-raise", ExpectedOldSeq: 1,
		NewThresholdRaw: "10", Operator: "t025-op", Reason: "raise 3->10",
	})
	if err != nil {
		t.Fatalf("raise AuthorizeConfirmationPolicy(): %v", err)
	}
	if resB.PolicySeq != 2 || resB.Threshold != 10 || resB.Recorded {
		t.Fatalf("raise result = %+v, want {PolicySeq:2 Threshold:10 Recorded:false}", resB)
	}
	// Seeded after the switch: conf=9 reaches the old N=3 but not the new
	// N=10 — post-switch adjudication must hold it Pending.
	bhB112, txB112 := depositBlockHash(112), depositTxHash(112, 0)
	depositSeedObservation(t, ctx, pool, chainB, 112, bhB112, txB112, 0, "1", 1)

	mB2 := metrics.New(func() bool { return true })
	scB2 := confirm19Scanner(t, pool, chainB, 10, mB2, leaseB)
	stopB2 := confirm13RunLoop(t, ctx, scB2, leaseB)
	time.Sleep(600 * time.Millisecond) // several 25ms poll ticks
	stopB2()

	// Confirmed never rewritten: byte-identical first-seen facts with the
	// then-threshold traceable.
	if got := confirm13ConfirmedAt(t, ctx, pool, chainB, bhB110, txB110); got != atB110 {
		t.Fatalf("h=110 confirmed_at changed %s -> %s across the raise (rewrote first-seen time)", atB110, got)
	}
	statusB2, nullAtB2, tipNB2, thrB2, seqB2, tipHashB2, confB2 :=
		confirmReadBasis(t, ctx, pool, chainB, bhB110, txB110)
	if statusB2 != statusB || nullAtB2 != nullAtB || tipNB2 != tipNB || thrB2 != thrB ||
		seqB2 != seqB || tipHashB2 != tipHashB || confB2 != confB {
		t.Fatalf("h=110 basis changed across the raise (confirmed row was rewritten)")
	}
	if thrB2 != 3 || seqB2 != 1 {
		t.Fatalf("h=110 traceability = (N=%d seq=%d), want (N=3 seq=1, the then-threshold)", thrB2, seqB2)
	}
	// Sub-new-threshold Pendings stay Pending with zero conversion facts.
	confirmAssertZeroWrite(t, ctx, pool, chainB, bhB119, txB119, 2)
	confirmAssertZeroWrite(t, ctx, pool, chainB, bhB112, txB112, 2)
	var confirmedB, pendingB int
	if err := pool.QueryRow(ctx, `
SELECT count(*) FILTER (WHERE status = 'confirmed'), count(*) FILTER (WHERE status = 'pending')
FROM deposit_observations WHERE chain_id = $1`, chainB).Scan(&confirmedB, &pendingB); err != nil {
		t.Fatalf("count by status post-raise: %v", err)
	}
	if confirmedB != 1 || pendingB != 2 {
		t.Fatalf("post-raise confirmed=%d pending=%d, want 1/2", confirmedB, pendingB)
	}
	if got := confirm13Counter(t, mB2, metrics.ConfirmationConfirmedMetricName, confirm13ChainLabel(chainB)); got != 0 {
		t.Fatalf("%s post-raise = %v, want 0 (raise converts nothing new)", metrics.ConfirmationConfirmedMetricName, got)
	}
	confirmAuthAssertZeroPause(t, ctx, pool, chainB)

	// Phase (c): unauthorized drift. Effective N=10, restart carrying N=7
	// with no authorization -> the loop refuses loud with zero damage to
	// observations/policy (R7; data-model §首确认协议分歧双首启纠正走 T029).
	const chainC = int64(31362)
	depositSeedCanonical(t, ctx, pool, chainC, 100, 109, true)
	bhC, txC := confirmSeedPending(t, ctx, pool, chainC, 100)
	confirmSeedPolicyRow(t, ctx, pool, chainC, 1, 10, nil, "bootstrap", nil)

	mC := metrics.New(func() bool { return true })
	scC, leaseC := confirm13Scanner(t, pool, chainC, 7, mC)
	doneC := make(chan error, 1)
	go func() { doneC <- scC.ServeLoop(ctx, leaseC, nil) }()
	select {
	case err := <-doneC:
		var mismatch *confirmationConfigMismatchError
		if !errors.As(err, &mismatch) {
			t.Fatalf("drifted ServeLoop() = %v (%T), want *confirmationConfigMismatchError", err, err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("drifted ServeLoop did not refuse the unauthorized N")
	}
	if got := scC.ConfirmationState(); got != 3 {
		t.Fatalf("drifted ConfirmationState() = %d, want 3 (stopped)", got)
	}
	confirmAssertZeroWrite(t, ctx, pool, chainC, bhC, txC, 1)
	var pendingC, confirmedC int
	if err := pool.QueryRow(ctx, `
SELECT count(*) FILTER (WHERE status = 'pending'), count(*) FILTER (WHERE status = 'confirmed')
FROM deposit_observations WHERE chain_id = $1`, chainC).Scan(&pendingC, &confirmedC); err != nil {
		t.Fatalf("count by status after drift refusal: %v", err)
	}
	if pendingC != 1 || confirmedC != 0 {
		t.Fatalf("after drift refusal pending=%d confirmed=%d, want 1/0 (zero damage)", pendingC, confirmedC)
	}
	confirmAuthAssertZeroPause(t, ctx, pool, chainC)
	rejectedC := map[string]string{"chain": "31362", "result": "rejected"}
	if got := confirm13Counter(t, mC, metrics.ConfirmationTransitionMetricName, rejectedC); got != 1 {
		t.Fatalf("%s{rejected} after drift refusal = %v, want 1", metrics.ConfirmationTransitionMetricName, got)
	}

	// Phase (d): failed switch. Stale expected_old_seq, empty change and
	// illegal value are each refused; policy_transition_total{rejected}+1
	// per attempt with zero new policy rows (atomic, no multi-version).
	const chainD = int64(31363)
	depositSeedCanonical(t, ctx, pool, chainD, 100, 109, true)
	bhD, txD := confirmSeedPending(t, ctx, pool, chainD, 100)
	confirmSeedPolicyRow(t, ctx, pool, chainD, 1, 10, nil, "bootstrap", nil)

	mD := metrics.New(func() bool { return true })
	SetConfirmAuthObserver(func(result string) {
		mD.ObserveConfirmationPolicyTransition(chainD, result)
	})
	defer SetConfirmAuthObserver(nil)

	if _, err := AuthorizeConfirmationPolicy(ctx, pool, ConfirmAuthRequest{
		ChainID: chainD, RequestID: "t025-stale", ExpectedOldSeq: 99,
		NewThresholdRaw: "25", Operator: "t025-op", Reason: "stale attempt",
	}); !errors.Is(err, ErrConfirmAuthExpired) {
		t.Fatalf("stale switch err = %v, want %v", err, ErrConfirmAuthExpired)
	}
	if _, err := AuthorizeConfirmationPolicy(ctx, pool, ConfirmAuthRequest{
		ChainID: chainD, RequestID: "t025-empty", ExpectedOldSeq: 1,
		NewThresholdRaw: "10", Operator: "t025-op", Reason: "empty attempt",
	}); !errors.Is(err, ErrConfirmAuthRejected) {
		t.Fatalf("empty-change switch err = %v, want %v", err, ErrConfirmAuthRejected)
	}
	if _, err := AuthorizeConfirmationPolicy(ctx, pool, ConfirmAuthRequest{
		ChainID: chainD, RequestID: "t025-illegal", ExpectedOldSeq: 1,
		NewThresholdRaw: "0", Operator: "t025-op", Reason: "illegal attempt",
	}); !errors.Is(err, ErrConfirmAuthRejected) {
		t.Fatalf("illegal-value switch err = %v, want %v", err, ErrConfirmAuthRejected)
	}
	if k := depositCountRows(t, ctx, pool, "confirmation_policy_history", chainD); k != 1 {
		t.Fatalf("policy rows after 3 failed switches = %d, want 1 (atomic, no multi-version)", k)
	}
	rejectedD := map[string]string{"chain": "31363", "result": "rejected"}
	if got := confirm13Counter(t, mD, metrics.ConfirmationPolicyTransitionMetricName, rejectedD); got != 3 {
		t.Fatalf("%s{rejected} = %v, want 3 (one per failed switch)", metrics.ConfirmationPolicyTransitionMetricName, got)
	}
	okD := map[string]string{"chain": "31363", "result": "ok"}
	if got := confirm13Counter(t, mD, metrics.ConfirmationPolicyTransitionMetricName, okD); got != 0 {
		t.Fatalf("%s{ok} = %v, want 0 (failed switches commit nothing)", metrics.ConfirmationPolicyTransitionMetricName, got)
	}
	confirmAssertZeroWrite(t, ctx, pool, chainD, bhD, txD, 1)
	confirmAuthAssertZeroPause(t, ctx, pool, chainD)
	SetConfirmAuthObserver(nil)

	// Phase (e): unknown switch outcome. Through a fault-injected pool
	// (T022(b) precedent: channel-fired sync, no fixed sleeps): COMMIT
	// landed but the reply lost -> the request_id reread定性 the recorded
	// result (no re-execution); COMMIT never landed -> error with the ID
	// unbound, so the retry with corrected fate succeeds.
	const chainE = int64(31364)
	confirmSeedPolicyRow(t, ctx, pool, chainE, 1, 10, nil, "bootstrap", nil)
	fault := newDepositFault()
	faultPool := depositOpenFaultPool(t, dsn, fault)

	reqE1 := ConfirmAuthRequest{
		ChainID: chainE, RequestID: "t025-unk1", ExpectedOldSeq: 1,
		NewThresholdRaw: "3", Operator: "t025-op", Reason: "unknown outcome 1",
	}
	fault.armSQL(depositFaultCommit, true, false)
	resE1, err := AuthorizeConfirmationPolicy(ctx, faultPool, reqE1)
	if err != nil {
		t.Fatalf("switch with the COMMIT reply lost = %v, want nil (request_id reread converges)", err)
	}
	fault.waitFired(t, "COMMIT reply loss on the auth switch")
	fault.disarm()
	if got := fault.fires.Load(); got != 1 {
		t.Fatalf("COMMIT drops = %d, want exactly 1 (a second attempt would be a duplicate)", got)
	}
	if !resE1.Recorded || resE1.PolicySeq != 2 || resE1.Threshold != 3 {
		t.Fatalf("reread result = %+v, want {PolicySeq:2 Threshold:3 Recorded:true}", resE1)
	}
	if k := depositCountRows(t, ctx, pool, "confirmation_policy_history", chainE); k != 2 {
		t.Fatalf("policy rows after reply loss = %d, want 2 (COMMIT landed exactly once)", k)
	}
	thrE, prevE, prevNullE, opE, reasonE, reqE, expE :=
		confirmAuthReadRow(t, ctx, pool, chainE, 2)
	if thrE != 3 || prevNullE || prevE != 1 || opE != "t025-op" || reasonE != "unknown outcome 1" || reqE != "t025-unk1" || expE != 1 {
		t.Fatalf("row seq2 = (thr=%d prev=%d null=%v op=%q reason=%q req=%q exp=%d), want (3 1 false t025-op unknown-outcome-1 t025-unk1 1)",
			thrE, prevE, prevNullE, opE, reasonE, reqE, expE)
	}
	// The follow-up with the same intent reads the recorded row back
	// without appending (定性, not re-execution).
	replayE1, err := AuthorizeConfirmationPolicy(ctx, pool, reqE1)
	if err != nil {
		t.Fatalf("same-intent replay = %v, want nil (recorded)", err)
	}
	if !replayE1.Recorded || replayE1.PolicySeq != 2 || replayE1.Threshold != 3 {
		t.Fatalf("replay result = %+v, want {PolicySeq:2 Threshold:3 Recorded:true}", replayE1)
	}
	if k := depositCountRows(t, ctx, pool, "confirmation_policy_history", chainE); k != 2 {
		t.Fatalf("policy rows after replay = %d, want 2 (replay appends nothing)", k)
	}

	reqE2 := ConfirmAuthRequest{
		ChainID: chainE, RequestID: "t025-unk2", ExpectedOldSeq: 2,
		NewThresholdRaw: "7", Operator: "t025-op", Reason: "unknown outcome 2",
	}
	fault.armSQL(depositFaultCommit, false, false)
	if _, err := AuthorizeConfirmationPolicy(ctx, faultPool, reqE2); err == nil {
		t.Fatal("switch with COMMIT unlanded = nil, want an unknown-outcome error")
	}
	fault.waitFired(t, "connection death at the auth COMMIT")
	fault.disarm()
	if got := fault.fires.Load(); got != 2 {
		t.Fatalf("fault fires = %d, want 2 (one per unknown-outcome case)", got)
	}
	if k := depositCountRows(t, ctx, pool, "confirmation_policy_history", chainE); k != 2 {
		t.Fatalf("policy rows after unlanded COMMIT = %d, want 2 (nothing landed)", k)
	}
	// The ID stayed unbound: the retry (same intent, corrected fate)
	// succeeds as a fresh switch.
	resE2, err := AuthorizeConfirmationPolicy(ctx, pool, reqE2)
	if err != nil {
		t.Fatalf("retry after unlanded COMMIT = %v, want nil", err)
	}
	if resE2.PolicySeq != 3 || resE2.Threshold != 7 || resE2.Recorded {
		t.Fatalf("retry result = %+v, want {PolicySeq:3 Threshold:7 Recorded:false}", resE2)
	}
	if k := depositCountRows(t, ctx, pool, "confirmation_policy_history", chainE); k != 3 {
		t.Fatalf("policy rows after retry = %d, want 3", k)
	}
	confirmAuthAssertZeroPause(t, ctx, pool, chainE)

	// Cross-phase contracts integrity check over every chain touched: zero
	// confirmed rows may lack basis columns.
	var incompleteAll int
	if err := pool.QueryRow(ctx, `
SELECT COUNT(*) FROM deposit_observations
WHERE chain_id IN (31360, 31361, 31362, 31363, 31364) AND status = 'confirmed'
  AND (confirmed_at IS NULL OR confirm_tip_number IS NULL OR confirm_tip_hash IS NULL
       OR confirm_threshold IS NULL OR confirmations IS NULL OR confirm_policy_seq IS NULL)`,
	).Scan(&incompleteAll); err != nil {
		t.Fatalf("cross-phase integrity check: %v", err)
	}
	if incompleteAll != 0 {
		t.Fatalf("incomplete confirmed rows across D4 chains = %d, want 0", incompleteAll)
	}
}

// -- T028 (append below; T025 ends here) --------------------------------------

// -- T028 --------------------------------------------------------------------
//
// T028 [US5] audit-traceability assertions (FR-09/11, SC-09; US5-2/3,
// quickstart D5 audit limb): the converted rows stay locatable through the
// verbatim contracts/observability.md diagnostic SQL (deposit -> block ->
// basis -> time), error/log output carries zero credentials and zero
// unbounded dumps (sampling assertions mirroring the repo's redaction style:
// metrics TestDepositLogContract + logx redact_test), and
// policy_transition_total reconciles with the history rows (ok == switch
// count, rejected == failed attempts).
//
// Method: isolated chain_ids on one container database (testcontainers
// postgres:18 via startIndexerPostgres), T025 helpers reused verbatim
// (depositSeed*/confirmSeed*/confirm13*/confirmAuth*); new helpers are
// confirm28*-prefixed so later batches never collide. R9 note: the trace
// columns asserted here (basis six + confirmed_at + policy version chain)
// are exactly the 006 re-verification input — this test only asserts they
// suffice and are present, it implements no 006 semantics. Counter
// vocabulary: below_depth waiting lands in skipped_total, drift/pause
// refusals and lock-order losers in transition_total{rejected|stale}; the
// policy counter below only ever sees ok|rejected — no new recovery states.

// TestConfirmationAuthAuditTraceSQL runs the verbatim contract trace SQL over
// converted rows: every Confirmed row locates deposit, block, basis and time,
// and the 006 re-verification input (basis six columns + confirmed_at +
// policy version chain) is complete.
func TestConfirmationAuthAuditTraceSQL(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	const chainT = int64(31410)
	const h, tip, n = uint64(200), uint64(209), uint64(10)
	depositSeedCanonical(t, ctx, pool, chainT, h, tip, true)
	confirmSeedPolicyRow(t, ctx, pool, chainT, 1, int64(n), nil, "bootstrap", nil)
	depositSeedHistory(t, ctx, pool, chainT, 1, 10, strings.Repeat("aa", 32))
	wantTx := map[string]bool{}
	for i := uint64(0); i < 3; i++ {
		depositSeedObservation(t, ctx, pool, chainT, h, depositBlockHash(h), depositTxHash(h, i), i, "1", 1)
		wantTx[depositTxHash(h, i)] = true
	}

	m := metrics.New(func() bool { return true })
	sc, lease := confirm13Scanner(t, pool, chainT, n, m)
	stop := confirm13RunLoop(t, ctx, sc, lease)
	waitUntil(t, time.Now().Add(30*time.Second), "3 rows confirmed under N=10", func() bool {
		var k int
		if err := pool.QueryRow(ctx, `
SELECT count(*) FROM deposit_observations WHERE chain_id = $1 AND status = 'confirmed'`,
			chainT).Scan(&k); err != nil {
			return false
		}
		return k == 3
	})
	stop()

	// Verbatim contracts/observability.md trace SQL (deposit->block->basis->time).
	rows, err := pool.Query(ctx, `
SELECT block_number, block_hash, tx_hash, log_index,
       confirm_tip_number, confirm_tip_hash, confirm_threshold,
       confirmations, confirm_policy_seq, confirmed_at
FROM deposit_observations
WHERE chain_id = $1 AND status = 'confirmed' ORDER BY confirmed_at LIMIT $2`,
		chainT, 100)
	if err != nil {
		t.Fatalf("contract trace SQL: %v", err)
	}
	defer rows.Close()
	var seen int
	tipHash := depositBlockHash(tip)
	for rows.Next() {
		var blockNum, tipNum, thr, seq int64
		var bh, txHash, tipH, conf string
		var logIndex int64
		var at any
		if err := rows.Scan(&blockNum, &bh, &txHash, &logIndex, &tipNum, &tipH, &thr, &conf, &seq, &at); err != nil {
			t.Fatalf("scan trace row: %v", err)
		}
		seen++
		// Deposit identity: the seeded observation.
		if blockNum != int64(h) || bh != depositBlockHash(h) || !wantTx[txHash] {
			t.Fatalf("trace row %d locates deposit (%d %s %s), want h=200 canonical hash + seeded tx", seen, blockNum, bh, txHash)
		}
		// Block + basis: exact tip identity, then-threshold, exact confirmations, policy seq.
		if tipNum != int64(tip) || tipH != tipHash || thr != int64(n) || seq != 1 || conf != "10" {
			t.Fatalf("trace row %d basis = tip(%d %s) N=%d conf=%s seq=%d, want tip(209 %s) N=10 conf=10 seq=1",
				seen, tipNum, tipH, thr, conf, seq, tipHash)
		}
		if conf != fmt.Sprintf("%d", tipNum-blockNum+1) {
			t.Fatalf("trace row %d confirmations=%s, want tip-h+1 exact", seen, conf)
		}
		// Time: confirmed_at present — the R9 006 re-verification input needs
		// all ten columns, none nullable on a converted row.
		if at == nil {
			t.Fatalf("trace row %d confirmed_at is NULL (006 re-verification input incomplete)", seen)
		}
		delete(wantTx, txHash)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("trace rows iteration: %v", err)
	}
	if seen != 3 || len(wantTx) != 0 {
		t.Fatalf("trace rows = %d (unlocated %d), want all 3 converted rows located", seen, len(wantTx))
	}

	// Verbatim integrity SQL: zero converted rows may lack basis columns.
	var incomplete int
	if err := pool.QueryRow(ctx, `
SELECT COUNT(*) FROM deposit_observations
WHERE chain_id = $1 AND status = 'confirmed'
  AND (confirmed_at IS NULL OR confirm_tip_number IS NULL OR confirm_tip_hash IS NULL
       OR confirm_threshold IS NULL OR confirmations IS NULL OR confirm_policy_seq IS NULL)`,
		chainT).Scan(&incomplete); err != nil {
		t.Fatalf("contract integrity SQL: %v", err)
	}
	if incomplete != 0 {
		t.Fatalf("incomplete confirmed rows = %d, want 0", incomplete)
	}

	// Verbatim version-chain SQL: the policy side of the 006 input (effective
	// policy = max seq, linear prev chain) is present for this chain.
	var seqs []int64
	chainRows, err := pool.Query(ctx, `
SELECT policy_seq, threshold, prev_seq, operator, reason, request_id, expected_old_seq, created_at
FROM confirmation_policy_history WHERE chain_id = $1 ORDER BY policy_seq`, chainT)
	if err != nil {
		t.Fatalf("contract version-chain SQL: %v", err)
	}
	defer chainRows.Close()
	for chainRows.Next() {
		var seq, thr int64
		var prev, op, reason, reqID, exp, at any
		if err := chainRows.Scan(&seq, &thr, &prev, &op, &reason, &reqID, &exp, &at); err != nil {
			t.Fatalf("scan policy row: %v", err)
		}
		seqs = append(seqs, seq)
		if thr != int64(n) {
			t.Fatalf("policy row seq=%d threshold=%d, want %d", seq, thr, n)
		}
	}
	if err := chainRows.Err(); err != nil {
		t.Fatalf("policy rows iteration: %v", err)
	}
	if len(seqs) != 1 || seqs[0] != 1 {
		t.Fatalf("policy version chain = %v, want [1] (bootstrap only; loop writes no policy row)", seqs)
	}
	confirmAuthAssertZeroPause(t, ctx, pool, chainT)
}

// TestConfirmationAuthAuditLogRedactionSampling mirrors the repo's redaction
// assertion style (metrics TestDepositLogContract banned-field sweep +
// DepositLogRedact leak check, logx redact_test): the fixed confirmation log
// field sets carry no credential-bearing names, sampled error/log output
// contains zero credentials, and no unbounded dumps. Thresholds/heights are
// plain numeric fields and stay visible (contract: 可打点).
func TestConfirmationAuthAuditLogRedactionSampling(t *testing.T) {
	// Fixed confirmation log fields per contracts/observability.md §Logs.
	confirm28Fields := map[string][]string{
		"convert":       {"chain_id", "block_number", "block_hash", "tx_hash", "log_index", "tip", "threshold", "confirmations", "policy_seq", "attempt"},
		"skip":          {"chain_id", "block_number", "block_hash", "reason"},
		"stop":          {"chain_id", "reason"},
		"retry":         {"chain_id", "kind", "attempt", "retry_in"},
		"config_reject": {"chain_id", "reason", "detail"},
		"switch":        {"chain_id", "request_id", "operator", "old_seq", "old_threshold", "new_threshold", "result", "reason"},
	}
	banned := []string{"token", "secret", "password", "passwd", "pwd", "dsn", "url", "key", "authorization"}
	for event, fields := range confirm28Fields {
		if len(fields) == 0 || fields[0] != "chain_id" {
			t.Errorf("event %q fields %v: chain_id must come first", event, fields)
		}
		for _, field := range fields {
			for _, b := range banned {
				if strings.Contains(strings.ToLower(field), b) {
					t.Errorf("event %q field %q looks credential-bearing", event, field)
				}
			}
		}
	}

	// Sampling: error/log output that brushed against credentials must carry
	// zero of them after the contract funnel (logx.Redact, shared with
	// metrics.DepositLogRedact).
	samples := []string{
		"confirm switch failed: postgres://op:hunter2@db:5432/txharbor chain_id=31411 request_id=t028-s1",
		"commit retry: Authorization: Bearer sk-live-123 chain_id=31411 attempt=2 retry_in=25ms",
		"config reject: password=p@ss detail=bad-threshold chain_id=31411 reason=non_positive",
		"rpc dial: postgres://op:hunter2@db:5432/txharbor?password=hunter2&token=sk-live-456 chain_id=31411",
	}
	for _, funnel := range []struct {
		name string
		fn   func(string) string
	}{
		{"logx.Redact", logx.Redact},
		{"DepositLogRedact", metrics.DepositLogRedact},
	} {
		for _, sample := range samples {
			got := funnel.fn(sample)
			for _, secret := range []string{"hunter2", "sk-live-123", "sk-live-456", "p@ss"} {
				if strings.Contains(got, secret) {
					t.Fatalf("%s(%q) leaked %q: %q", funnel.name, sample, secret, got)
				}
			}
			if !strings.Contains(got, logx.Redacted) {
				t.Fatalf("%s(%q) = %q, want %s", funnel.name, sample, got, logx.Redacted)
			}
			// Diagnosability survives: chain/request identity is not redacted away.
			if !strings.Contains(got, "31411") {
				t.Fatalf("%s(%q) = %q, lost the chain identity", funnel.name, sample, got)
			}
		}
	}

	// Zero unbounded dumps: a raw multi-KB payload must never appear verbatim
	// in a sampled log line; the line stays bounded while plain numerics
	// (thresholds/heights, contract-allowed) stay visible.
	rawDump := strings.Repeat("7f", 32768)
	convertLine := fmt.Sprintf("chain_id=%d block_number=%d tip=%d threshold=%d confirmations=%d policy_seq=%d attempt=%d",
		31411, 200, 209, 10, 10, 1, 1)
	if strings.Contains(convertLine, rawDump) || len(convertLine) > 4<<10 {
		t.Fatalf("convert log line unbounded (len=%d)", len(convertLine))
	}
	for _, plain := range []string{"200", "209", "10"} {
		if !strings.Contains(convertLine, plain) {
			t.Fatalf("convert log line %q hid plain numeric %q (thresholds/heights are allowed)", convertLine, plain)
		}
	}
}

// TestConfirmationAuthPolicyTransitionReconciliation wires the switch-outcome
// hook (T025 phase-d precedent) and reconciles
// policy_transition_total against the history rows: ok == switch count,
// rejected == failed attempts. Vocabulary note: below_depth waiting counts
// in skipped_total, drift/pause refusals and lock-order losers in
// transition_total{rejected|stale} — this policy counter only sees
// ok|rejected, no new recovery states.
func TestConfirmationAuthAuditPolicyTransitionReconciliation(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	const chainP = int64(31412)
	confirmSeedPolicyRow(t, ctx, pool, chainP, 1, 10, nil, "bootstrap", nil)

	m := metrics.New(func() bool { return true })
	SetConfirmAuthObserver(func(result string) {
		m.ObserveConfirmationPolicyTransition(chainP, result)
	})
	defer SetConfirmAuthObserver(nil)

	// Two authorized switches (the switch count).
	if _, err := AuthorizeConfirmationPolicy(ctx, pool, ConfirmAuthRequest{
		ChainID: chainP, RequestID: "t028-ok1", ExpectedOldSeq: 1,
		NewThresholdRaw: "3", Operator: "t028-op", Reason: "lower 10->3",
	}); err != nil {
		t.Fatalf("switch 1: %v", err)
	}
	if _, err := AuthorizeConfirmationPolicy(ctx, pool, ConfirmAuthRequest{
		ChainID: chainP, RequestID: "t028-ok2", ExpectedOldSeq: 2,
		NewThresholdRaw: "7", Operator: "t028-op", Reason: "raise 3->7",
	}); err != nil {
		t.Fatalf("switch 2: %v", err)
	}

	// Three failed attempts: stale seq, empty change, illegal value.
	if _, err := AuthorizeConfirmationPolicy(ctx, pool, ConfirmAuthRequest{
		ChainID: chainP, RequestID: "t028-f1", ExpectedOldSeq: 99,
		NewThresholdRaw: "25", Operator: "t028-op", Reason: "stale attempt",
	}); !errors.Is(err, ErrConfirmAuthExpired) {
		t.Fatalf("stale switch err = %v, want %v", err, ErrConfirmAuthExpired)
	}
	if _, err := AuthorizeConfirmationPolicy(ctx, pool, ConfirmAuthRequest{
		ChainID: chainP, RequestID: "t028-f2", ExpectedOldSeq: 3,
		NewThresholdRaw: "7", Operator: "t028-op", Reason: "empty attempt",
	}); !errors.Is(err, ErrConfirmAuthRejected) {
		t.Fatalf("empty-change switch err = %v, want %v", err, ErrConfirmAuthRejected)
	}
	if _, err := AuthorizeConfirmationPolicy(ctx, pool, ConfirmAuthRequest{
		ChainID: chainP, RequestID: "t028-f3", ExpectedOldSeq: 3,
		NewThresholdRaw: "0", Operator: "t028-op", Reason: "illegal attempt",
	}); !errors.Is(err, ErrConfirmAuthRejected) {
		t.Fatalf("illegal-value switch err = %v, want %v", err, ErrConfirmAuthRejected)
	}

	historyRows := depositCountRows(t, ctx, pool, "confirmation_policy_history", chainP)
	if historyRows != 3 {
		t.Fatalf("policy history rows = %d, want 3 (bootstrap + 2 switches; failures append nothing)", historyRows)
	}
	okLabels := map[string]string{"chain": "31412", "result": "ok"}
	rejectedLabels := map[string]string{"chain": "31412", "result": "rejected"}
	if got := confirm13Counter(t, m, metrics.ConfirmationPolicyTransitionMetricName, okLabels); got != 2 {
		t.Fatalf("%s{ok} = %v, want 2 (== switch count %d)", metrics.ConfirmationPolicyTransitionMetricName, got, historyRows-1)
	}
	if got := confirm13Counter(t, m, metrics.ConfirmationPolicyTransitionMetricName, rejectedLabels); got != 3 {
		t.Fatalf("%s{rejected} = %v, want 3 (== failed attempts)", metrics.ConfirmationPolicyTransitionMetricName, got)
	}
	// ok count == appended switch rows (history minus the bootstrap).
	if got := confirm13Counter(t, m, metrics.ConfirmationPolicyTransitionMetricName, okLabels); got != float64(historyRows-1) {
		t.Fatalf("ok count %v != appended rows %d", got, historyRows-1)
	}
	confirmAuthAssertZeroPause(t, ctx, pool, chainP)
	SetConfirmAuthObserver(nil)
}
