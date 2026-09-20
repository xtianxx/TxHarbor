//go:build integration

// Batch B integration tests (T030 extension) on a real PostgreSQL: the
// manual repair -> release path must invent nothing downstream. These are new
// test functions in a new file only; reorgcommit_test.go and every other
// existing file stay untouched. Helpers reused directly: reorgTestPool,
// reorgCount, reorgEstablishOne, reorgSnapAudit, t030SeedLoopBase,
// t030ReplayToComplete, testRecoveryCap (reorgcommit_test.go),
// depositITLease (deposit_integration_test.go) and the release/repair
// functions themselves. ChainID reservation: 906017 (B3-a) and 906018 (B3-b)
// extend the 906001-906016 block documented in reorgcommit_test.go.
//
// Flagged inconsistencies (encoded as implemented, never resolved here):
// AuthorizeRecoveryRelease creates no payment intent by construction (011 owns
// admission; the release only deletes the recovery row + appends the terminal
// event); the assertions below pin the absence with row counts, not with
// intent-shape logic.
package indexer

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// t030GlobalCount counts rows in a downstream table with no chain_id column
// (011/012 intent-scoped tables: execution_steps, execution_events,
// tx_intent_freezes). Each test runs on a fresh per-test container, so the
// global zero is the strongest available claim; reorgCount cannot be used
// without a chain_id column.
func t030GlobalCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, table string) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

// t030DownstreamCounts is the row-count image of the six downstream 011/012
// tables the T030 release must never populate: payment_intents,
// execution_steps, execution_events, withdrawal_requests, tx_attempts and
// tx_intent_freezes. payment_intents / withdrawal_requests / tx_attempts are
// chain-scoped (reorgCount); execution_steps / execution_events /
// tx_intent_freezes carry no chain_id and are counted globally.
type t030DownstreamCounts struct {
	intents, steps, events, requests, attempts, freezes int64
}

func t030ReadDownstreamCounts(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64) t030DownstreamCounts {
	t.Helper()
	return t030DownstreamCounts{
		intents:  reorgCount(t, ctx, pool, "payment_intents", chainID),
		steps:    t030GlobalCount(t, ctx, pool, "execution_steps"),
		events:   t030GlobalCount(t, ctx, pool, "execution_events"),
		requests: reorgCount(t, ctx, pool, "withdrawal_requests", chainID),
		attempts: reorgCount(t, ctx, pool, "tx_attempts", chainID),
		freezes:  t030GlobalCount(t, ctx, pool, "tx_intent_freezes"),
	}
}

func (c t030DownstreamCounts) assertEmpty(t *testing.T, msg string) {
	t.Helper()
	if c != (t030DownstreamCounts{}) {
		t.Fatalf("%s: downstream rows invented: %+v (want all six zero)", msg, c)
	}
}

// t030BatchBReleaseCycle drives the exact T030 manual cycle up to (but not
// including) the release: establish on the seeded mini-loop, full replay to
// complete_pending, reconcile signal, then repair authorization. The release
// stays in each test so pre-release snapshots are taken at the right point.
func t030BatchBReleaseCycle(t *testing.T, ctx context.Context, pool *pgxpool.Pool, lease *Lease,
	chainID int64, op, ev, disp string) {
	t.Helper()
	res := reorgEstablishOne(t, ctx, pool, lease, chainID, 20)
	owned := RecoveryCapture{Seq: res.Seq, Owned: &RecoveryOwned{RecoveryID: res.RecoveryID}}
	t030ReplayToComplete(t, ctx, pool, lease, chainID, owned)
	if err := SignalRecoveryReconcile(ctx, pool, lease, chainID, owned,
		"over-deep", "1-20", "tip-diverged-beyond-depth-25"); err != nil {
		t.Fatalf("signal reconcile: %v", err)
	}
	if err := AuthorizeRecoveryRepair(ctx, pool, chainID, op, ev, disp); err != nil {
		t.Fatalf("repair: %v", err)
	}
}

// TestT030ReleaseInventsNoPaymentIntents closes the "release invents no
// payment intents" gap for the T030 manual path.
//
// Defect: none known — this is a cross-subsystem coverage gap.
// Invariant: AuthorizeRecoveryRelease (Q2b step 2) deletes only the recovery
// row and appends one terminal event; it creates no payment_intent,
// execution_step, execution_event, withdrawal_request, tx_attempt or
// tx_intent_freeze row (011 owns admission; unknown withdrawal outcomes keep
// their待对账 semantics).
// Gap: TestT030ManualTwoStepAuth freezes only 006/004 tables
// (reorgSnapAudit); the six downstream 011/012 tables were never asserted
// empty across the release.
// Level: integration.
// Criteria: after t030SeedLoopBase + establish + replay-to-complete +
// reconcile + repair, the six downstream tables are empty at baseline and
// still empty after AuthorizeRecoveryRelease succeeds; the existing snap
// invariants hold (rec=0, ev=snap.ev+1, transitions/observations/pauses
// unchanged) and the terminal event carries operator/evidence/disposition,
// swept=16-20 and version=1.
func TestT030ReleaseInventsNoPaymentIntents(t *testing.T) {
	pool := reorgTestPool(t)
	defer pool.Close()
	ctx := context.Background()
	const chainID = int64(906017)
	lease := depositITLease(t, pool, chainID)
	t030SeedLoopBase(t, ctx, pool, chainID)

	const op = "op-t030-b3a-release"
	ev := fmt.Sprintf("old_tip=20:%s new_tip=21:%s searched=1-20 cause=over-deep",
		depositBlockHash(20), depositBlockHash(120))
	disp := "orphaned=1 revived=0 replayed=block:20,log:20,deposit:20"
	t030BatchBReleaseCycle(t, ctx, pool, lease, chainID, op, ev, disp)

	// Baseline: the downstream tables are empty before the release, so a
	// post-release zero proves the release itself wrote none of them.
	t030ReadDownstreamCounts(t, ctx, pool, chainID).assertEmpty(t, "pre-release baseline")

	snap := reorgSnapAudit(t, ctx, pool, chainID)
	if err := AuthorizeRecoveryRelease(ctx, pool, chainID, op, ev, disp); err != nil {
		t.Fatalf("release: %v", err)
	}
	t030ReadDownstreamCounts(t, ctx, pool, chainID).assertEmpty(t, "post-release")

	// Existing T030 release invariants: only the recovery row is deleted and
	// exactly one terminal event is appended; nothing else moves.
	if n := reorgCount(t, ctx, pool, "reorg_recovery", chainID); n != 0 {
		t.Fatalf("recovery rows = %d after release, want 0", n)
	}
	got := reorgSnapAudit(t, ctx, pool, chainID)
	if got.rec != 0 || got.ev != snap.ev+1 || got.trans != snap.trans || got.obs != snap.obs ||
		got.dpause != 0 || got.lpause != 0 || got.ipause != 0 {
		t.Fatalf("post-release audit = %+v, want rec=0 ev=%d trans=%d obs=%d pauses=(0,0,0)",
			got, snap.ev+1, snap.trans, snap.obs)
	}
	var terminal string
	if err := pool.QueryRow(ctx, `SELECT detail FROM reorg_recovery_events
WHERE chain_id = $1 AND event = 'released'`, chainID).Scan(&terminal); err != nil {
		t.Fatalf("read terminal event: %v", err)
	}
	for _, want := range []string{"event=released", op, ev, disp, "swept=16-20", "version=1"} {
		if !strings.Contains(terminal, want) {
			t.Fatalf("terminal detail missing %q: %q", want, terminal)
		}
	}
}

// t030SeedDormantWithdrawalRequest plants one accepted withdrawal_requests row
// (plus its caller FK row) that no intent was ever admitted for (dormant:
// payment_intents stays 0) so the release cycle must not touch it.
func t030SeedDormantWithdrawalRequest(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64) {
	t.Helper()
	callerID := chainID
	if _, err := pool.Exec(ctx, `INSERT INTO caller (caller_id, label) VALUES ($1, 't030-b3b-caller')`,
		callerID); err != nil {
		t.Fatalf("seed caller: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO withdrawal_requests
    (request_id, caller_id, idempotency_key, authorization_id, chain_id, asset, recipient, amount)
VALUES ($1, $2, $3, $4, $5, $6, $7, 100)`,
		fmt.Sprintf("t030-b3b-req-%d", chainID), callerID,
		fmt.Sprintf("t030-b3b-idem-%d", chainID), fmt.Sprintf("t030-b3b-auth-%d", chainID),
		chainID, testContractA, depositWatchAddr); err != nil {
		t.Fatalf("seed withdrawal_request: %v", err)
	}
}

// t030WithdrawalSnapshot freezes every column of the chain's
// withdrawal_requests rows as one byte-exact text image (to_jsonb renders all
// columns; ORDER BY id fixes row order) plus the row count.
func t030WithdrawalSnapshot(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64) (int64, string) {
	t.Helper()
	var (
		count int64
		snap  string
	)
	if err := pool.QueryRow(ctx, `
SELECT COUNT(*), COALESCE(string_agg(to_jsonb(r)::text, E'\n' ORDER BY r.id), '')
FROM withdrawal_requests r
WHERE r.chain_id = $1`, chainID).Scan(&count, &snap); err != nil {
		t.Fatalf("snapshot withdrawal_requests: %v", err)
	}
	return count, snap
}

// TestT030ReleaseLeavesDownstreamRowsByteIdentical closes the "release leaves
// downstream rows byte-identical" gap for the T030 manual path.
//
// Defect: none known — this is a cross-subsystem coverage gap.
// Invariant: the release cycle never reads-then-writes downstream 007/011
// state; a dormant accepted withdrawal request that predates the recovery
// keeps its row count and every column byte-for-byte.
// Gap: TestT030ManualTwoStepAuth/T031 assert table counts only; no test
// seeds a real downstream row before establish and proves the release path
// leaves it byte-identical.
// Level: integration.
// Criteria: seed the request pre-establish (payment_intents==0 proves it is
// dormant), run the full cycle (establish + replay-to-complete + reconcile +
// repair + release), then re-read the byte-exact snapshot: row count == 1 and
// the to_jsonb text image is unchanged; the recovery row is gone, exactly one
// terminal event was appended and downstream 011/012 tables stay empty.
func TestT030ReleaseLeavesDownstreamRowsByteIdentical(t *testing.T) {
	pool := reorgTestPool(t)
	defer pool.Close()
	ctx := context.Background()
	const chainID = int64(906018)
	lease := depositITLease(t, pool, chainID)
	t030SeedLoopBase(t, ctx, pool, chainID)
	t030SeedDormantWithdrawalRequest(t, ctx, pool, chainID)

	// Dormant baseline: the request exists but no intent was ever admitted.
	if n := reorgCount(t, ctx, pool, "payment_intents", chainID); n != 0 {
		t.Fatalf("payment_intents rows = %d before establish, want 0 (dormant request)", n)
	}
	beforeCount, beforeSnap := t030WithdrawalSnapshot(t, ctx, pool, chainID)
	if beforeCount != 1 {
		t.Fatalf("withdrawal_requests rows = %d, want the one seeded dormant request", beforeCount)
	}

	const op = "op-t030-b3b-release"
	ev := fmt.Sprintf("old_tip=20:%s new_tip=21:%s searched=1-20 cause=over-deep",
		depositBlockHash(20), depositBlockHash(120))
	disp := "orphaned=1 revived=0 replayed=block:20,log:20,deposit:20"
	t030BatchBReleaseCycle(t, ctx, pool, lease, chainID, op, ev, disp)

	snap := reorgSnapAudit(t, ctx, pool, chainID)
	if err := AuthorizeRecoveryRelease(ctx, pool, chainID, op, ev, disp); err != nil {
		t.Fatalf("release: %v", err)
	}

	// Byte-identical: same row count, same to_jsonb text image for every
	// column of every row.
	afterCount, afterSnap := t030WithdrawalSnapshot(t, ctx, pool, chainID)
	if afterCount != beforeCount || afterSnap != beforeSnap {
		t.Fatalf("withdrawal_requests changed across the release cycle:\nbefore count=%d %s\nafter  count=%d %s",
			beforeCount, beforeSnap, afterCount, afterSnap)
	}

	// The release still deleted only the recovery row and appended exactly one
	// terminal event; downstream stays empty.
	if n := reorgCount(t, ctx, pool, "reorg_recovery", chainID); n != 0 {
		t.Fatalf("recovery rows = %d after release, want 0", n)
	}
	got := reorgSnapAudit(t, ctx, pool, chainID)
	if got.rec != 0 || got.ev != snap.ev+1 || got.trans != snap.trans || got.obs != snap.obs ||
		got.dpause != 0 || got.lpause != 0 || got.ipause != 0 {
		t.Fatalf("post-release audit = %+v, want rec=0 ev=%d trans=%d obs=%d pauses=(0,0,0)",
			got, snap.ev+1, snap.trans, snap.obs)
	}
	var terminal string
	if err := pool.QueryRow(ctx, `SELECT detail FROM reorg_recovery_events
WHERE chain_id = $1 AND event = 'released'`, chainID).Scan(&terminal); err != nil {
		t.Fatalf("read terminal event: %v", err)
	}
	for _, want := range []string{"event=released", op, ev, disp, "swept=16-20", "version=1"} {
		if !strings.Contains(terminal, want) {
			t.Fatalf("terminal detail missing %q: %q", want, terminal)
		}
	}
	// The only downstream row is the seeded dormant request (already pinned
	// byte-identical above): zero the request column and require the other
	// five 011/012 tables to stay empty across the release.
	postDownstream := t030ReadDownstreamCounts(t, ctx, pool, chainID)
	if postDownstream.requests != beforeCount {
		t.Fatalf("withdrawal_requests rows = %d after release, want %d (unchanged)", postDownstream.requests, beforeCount)
	}
	postDownstream.requests = 0
	postDownstream.assertEmpty(t, "post-release downstream")
}
