//go:build integration

// T024 switch-transaction tests on a real PostgreSQL: the happy-path single
// append with its full audit set, every guard refusing with zero state
// change (stale seq, empty change, empty table), the request_id idempotency
// branches (same-param recorded, different-param refused, failed-attempt ID
// unbound and retryable), and the zero-pause-row invariant. Harness mirrors
// internal/indexer/*_integration_test.go headers (testcontainers postgres:18
// via startIndexerPostgres). The full-cycle D4 coverage (pending
// re-adjudication under old/new policy) belongs to T025, not here.
package indexer

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// confirmAuthTestPool boots one container database with all migrations.
func confirmAuthTestPool(t *testing.T) (*pgxpool.Pool, context.Context) {
	t.Helper()
	pool := openIndexerPool(t, startIndexerPostgres(t))
	t.Cleanup(pool.Close)
	return pool, context.Background()
}

// confirmAuthReadRow reads one policy row's full audit set.
func confirmAuthReadRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID, seq int64) (threshold, prev int64, prevNull bool, operator, reason, requestID string, expectedOld int64) {
	t.Helper()
	var prevScan *int64
	err := pool.QueryRow(ctx, `
SELECT threshold, prev_seq, operator, reason, request_id, expected_old_seq
FROM confirmation_policy_history WHERE chain_id = $1 AND policy_seq = $2`,
		chainID, seq).Scan(&threshold, &prevScan, &operator, &reason, &requestID, &expectedOld)
	if err != nil {
		t.Fatalf("read policy row (%d,%d): %v", chainID, seq, err)
	}
	if prevScan == nil {
		return threshold, 0, true, operator, reason, requestID, expectedOld
	}
	return threshold, *prevScan, false, operator, reason, requestID, expectedOld
}

// confirmAuthAssertZeroPause proves the switch wrote no pause row on any
// stream (005 builds no pause row itself; switches especially must not).
func confirmAuthAssertZeroPause(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64) {
	t.Helper()
	for _, table := range []string{"deposit_pause", "indexer_pause", "log_pause"} {
		if n := depositCountRows(t, ctx, pool, table, chainID); n != 0 {
			t.Fatalf("%s rows = %d, want 0 (switch writes zero pause rows)", table, n)
		}
	}
}

// TestConfirmAuthSwitchHappyPath: seq 1/N=10 -> single INSERT seq 2/N=25 with
// prev_seq=1 and the full audit set; no pause rows.
func TestConfirmAuthSwitchHappyPath(t *testing.T) {
	pool, ctx := confirmAuthTestPool(t)
	const chainID = int64(81)
	confirmSeedPolicyRow(t, ctx, pool, chainID, 1, 10, nil, "bootstrap", nil)

	res, err := AuthorizeConfirmationPolicy(ctx, pool, ConfirmAuthRequest{
		ChainID: chainID, RequestID: "sw-1", ExpectedOldSeq: 1,
		NewThresholdRaw: "25", Operator: "op-a", Reason: "raise depth",
	})
	if err != nil {
		t.Fatalf("AuthorizeConfirmationPolicy(): %v", err)
	}
	if res.PolicySeq != 2 || res.Threshold != 25 || res.Recorded {
		t.Fatalf("result = %+v, want {PolicySeq:2 Threshold:25 Recorded:false}", res)
	}

	thr, prev, prevNull, op, reason, reqID, expOld :=
		confirmAuthReadRow(t, ctx, pool, chainID, 2)
	if thr != 25 || prevNull || prev != 1 || op != "op-a" || reason != "raise depth" || reqID != "sw-1" || expOld != 1 {
		t.Fatalf("row seq2 = (thr=%d prev=%d null=%v op=%q reason=%q req=%q exp=%d), want (25 1 false op-a raise-depth sw-1 1)",
			thr, prev, prevNull, op, reason, reqID, expOld)
	}
	if n := depositCountRows(t, ctx, pool, "confirmation_policy_history", chainID); n != 2 {
		t.Fatalf("policy rows = %d, want 2 (exactly one appended row)", n)
	}
	confirmAuthAssertZeroPause(t, ctx, pool, chainID)
}

// TestConfirmAuthStaleRefused: expected_old_seq != max seq -> expired, zero
// state change.
func TestConfirmAuthStaleRefused(t *testing.T) {
	pool, ctx := confirmAuthTestPool(t)
	const chainID = int64(82)
	confirmSeedPolicyRow(t, ctx, pool, chainID, 1, 10, nil, "bootstrap", nil)

	_, err := AuthorizeConfirmationPolicy(ctx, pool, ConfirmAuthRequest{
		ChainID: chainID, RequestID: "sw-stale", ExpectedOldSeq: 0,
		NewThresholdRaw: "25", Operator: "op", Reason: "why",
	})
	if !errors.Is(err, ErrConfirmAuthExpired) {
		t.Fatalf("stale switch err = %v, want %v", err, ErrConfirmAuthExpired)
	}
	if n := depositCountRows(t, ctx, pool, "confirmation_policy_history", chainID); n != 1 {
		t.Fatalf("policy rows = %d, want 1 (stale refusal writes nothing)", n)
	}
	confirmAuthAssertZeroPause(t, ctx, pool, chainID)
}

// TestConfirmAuthEmptyChangeRefused: new threshold == current -> empty
// authorization, zero state change (every seq advance implies a change).
func TestConfirmAuthEmptyChangeRefused(t *testing.T) {
	pool, ctx := confirmAuthTestPool(t)
	const chainID = int64(83)
	confirmSeedPolicyRow(t, ctx, pool, chainID, 1, 10, nil, "bootstrap", nil)

	_, err := AuthorizeConfirmationPolicy(ctx, pool, ConfirmAuthRequest{
		ChainID: chainID, RequestID: "sw-same", ExpectedOldSeq: 1,
		NewThresholdRaw: "10", Operator: "op", Reason: "why",
	})
	if !errors.Is(err, ErrConfirmAuthRejected) {
		t.Fatalf("empty-change switch err = %v, want %v", err, ErrConfirmAuthRejected)
	}
	if n := depositCountRows(t, ctx, pool, "confirmation_policy_history", chainID); n != 1 {
		t.Fatalf("policy rows = %d, want 1 (empty authorization writes nothing)", n)
	}
}

// TestConfirmAuthEmptyTableRefused: no source version -> switch refused; the
// first row belongs to the first-confirm transaction, never to a switch.
func TestConfirmAuthEmptyTableRefused(t *testing.T) {
	pool, ctx := confirmAuthTestPool(t)
	const chainID = int64(84)

	_, err := AuthorizeConfirmationPolicy(ctx, pool, ConfirmAuthRequest{
		ChainID: chainID, RequestID: "sw-early", ExpectedOldSeq: 0,
		NewThresholdRaw: "10", Operator: "op", Reason: "why",
	})
	if !errors.Is(err, ErrConfirmAuthRejected) {
		t.Fatalf("empty-table switch err = %v, want %v", err, ErrConfirmAuthRejected)
	}
	if n := depositCountRows(t, ctx, pool, "confirmation_policy_history", chainID); n != 0 {
		t.Fatalf("policy rows = %d, want 0 (bootstrap-via-switch refused)", n)
	}
}

// TestConfirmAuthIdempotentSameParam: repeating the identical request returns
// the recorded result without appending a row.
func TestConfirmAuthIdempotentSameParam(t *testing.T) {
	pool, ctx := confirmAuthTestPool(t)
	const chainID = int64(85)
	confirmSeedPolicyRow(t, ctx, pool, chainID, 1, 10, nil, "bootstrap", nil)

	req := ConfirmAuthRequest{
		ChainID: chainID, RequestID: "sw-idem", ExpectedOldSeq: 1,
		NewThresholdRaw: "25", Operator: "op", Reason: "why",
	}
	first, err := AuthorizeConfirmationPolicy(ctx, pool, req)
	if err != nil {
		t.Fatalf("first switch: %v", err)
	}
	second, err := AuthorizeConfirmationPolicy(ctx, pool, req)
	if err != nil {
		t.Fatalf("same-param replay: %v", err)
	}
	if !second.Recorded || second.PolicySeq != first.PolicySeq || second.Threshold != first.Threshold {
		t.Fatalf("replay = %+v, want recorded %+v", second, first)
	}
	if n := depositCountRows(t, ctx, pool, "confirmation_policy_history", chainID); n != 2 {
		t.Fatalf("policy rows = %d, want 2 (replay appends nothing)", n)
	}
}

// TestConfirmAuthIdempotentDifferentParamRefused: the same request_id with a
// different intent is refused with zero state change.
func TestConfirmAuthIdempotentDifferentParamRefused(t *testing.T) {
	pool, ctx := confirmAuthTestPool(t)
	const chainID = int64(86)
	confirmSeedPolicyRow(t, ctx, pool, chainID, 1, 10, nil, "bootstrap", nil)

	base := ConfirmAuthRequest{
		ChainID: chainID, RequestID: "sw-clash", ExpectedOldSeq: 1,
		NewThresholdRaw: "25", Operator: "op", Reason: "why",
	}
	if _, err := AuthorizeConfirmationPolicy(ctx, pool, base); err != nil {
		t.Fatalf("first switch: %v", err)
	}
	clash := base
	clash.NewThresholdRaw = "30"
	_, err := AuthorizeConfirmationPolicy(ctx, pool, clash)
	if !errors.Is(err, ErrConfirmAuthRejected) {
		t.Fatalf("different-param replay err = %v, want %v", err, ErrConfirmAuthRejected)
	}
	if n := depositCountRows(t, ctx, pool, "confirmation_policy_history", chainID); n != 2 {
		t.Fatalf("policy rows = %d, want 2 (clash writes nothing)", n)
	}
	confirmAuthAssertZeroPause(t, ctx, pool, chainID)
}

// TestConfirmAuthFailedAttemptLeavesIDUnbound: an explicitly failed switch
// binds nothing, so the same request_id retried with corrected params
// succeeds (mirrors the 004 binding rule).
func TestConfirmAuthFailedAttemptLeavesIDUnbound(t *testing.T) {
	pool, ctx := confirmAuthTestPool(t)
	const chainID = int64(87)
	confirmSeedPolicyRow(t, ctx, pool, chainID, 1, 10, nil, "bootstrap", nil)

	_, err := AuthorizeConfirmationPolicy(ctx, pool, ConfirmAuthRequest{
		ChainID: chainID, RequestID: "sw-retry", ExpectedOldSeq: 99,
		NewThresholdRaw: "25", Operator: "op", Reason: "why",
	})
	if !errors.Is(err, ErrConfirmAuthExpired) {
		t.Fatalf("stale attempt err = %v, want %v", err, ErrConfirmAuthExpired)
	}
	res, err := AuthorizeConfirmationPolicy(ctx, pool, ConfirmAuthRequest{
		ChainID: chainID, RequestID: "sw-retry", ExpectedOldSeq: 1,
		NewThresholdRaw: "25", Operator: "op", Reason: "why",
	})
	if err != nil {
		t.Fatalf("retry with corrected params: %v", err)
	}
	if res.PolicySeq != 2 || res.Threshold != 25 || res.Recorded {
		t.Fatalf("retry result = %+v, want {PolicySeq:2 Threshold:25 Recorded:false}", res)
	}
	if n := depositCountRows(t, ctx, pool, "confirmation_policy_history", chainID); n != 2 {
		t.Fatalf("policy rows = %d, want 2", n)
	}
}
