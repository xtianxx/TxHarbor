//go:build integration

// Batch A — 006 reorg policy (depth-config) authorization tests (T009):
// internal/indexer/reorgpolicy.go's AuthorizeReorgPolicy state machine on real
// PostgreSQL through the package harness (startIndexerPostgres + db.MigrateUp,
// testcontainers postgres:18), no sleeps — every wait is an event or a
// bounded condition poll (waitUntil / pg_stat_activity / the fault channel).
//
// Why this file exists: AuthorizeReorgPolicy has zero callers in the tree
// (production only uses ParseReorgMaxDepth and the establish-time bind), so no
// test at any level exercised the switch transaction end to end before this
// file; reorgpolicy_test.go covers only the parse/verify helpers. Every test
// below asserts durable state — policy rows with their full audit set,
// request_id binding, recovery/event/pause rowcounts — never a bare return
// code, and every refusal compares a full before/after snapshot of
// reorg_policy_history.
//
// Derivation: rules were read from reorgpolicy.go (classify:381,
// resolveRace:400, bind:190, SQL:415-437), migrations/000006_reorg_recovery.sql
// (115-137: PK (chain_id, policy_seq), UNIQUE (chain_id, request_id),
// bootstrap partial-unique), and specs/006-reorg-recovery/data-model.md Table 3
// (82-85). The four intent fields compared on a duplicate are exactly
// expected_old_seq / max_depth / operator / reason; derived results
// (policy_seq, prev_seq, created_at) are never part of that comparison.
//
// Known inconsistency (flagged, deliberately NOT resolved here): research R4
// ("no row + env set follows the 005 drift rule — loud refuse") versus the
// implemented VerifyReorgPolicyIdentity returning nil on an absent row, with
// the first establish transaction bootstrapping instead. These tests encode
// the implemented behavior; TestReorgBindDriftRefusedZeroWrite's success phase
// pins the bootstrap-on-absent-row behavior but does not adjudicate R4.
package indexer

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// --- helpers ---------------------------------------------------------------

// reorgAuthRow is one durable policy-history row with its whole audit set.
type reorgAuthRow struct {
	policySeq      int64
	maxDepth       int64
	prevSeq        *int64
	operator       string
	reason         string
	requestID      *string
	expectedOldSeq int64
	createdAt      time.Time
}

func reorgPtrInt64(v int64) *int64    { return &v }
func reorgPtrString(v string) *string { return &v }

// reorgAuthSeedBootstrap creates the first policy row through the production
// bind path (the same call the first establish transaction makes), so the
// switch tests start from the real bootstrap shape: seq 1, prev_seq NULL,
// operator 'bootstrap', request_id NULL, expected_old_seq 0.
func reorgAuthSeedBootstrap(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, depthRaw string) {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin bootstrap transaction: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	seq, err := bindReorgPolicy(ctx, tx, chainID, depthRaw)
	if err != nil {
		t.Fatalf("bind bootstrap policy (depth %q): %v", depthRaw, err)
	}
	if seq != 1 {
		t.Fatalf("bootstrap bind seq = %d, want 1", seq)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit bootstrap transaction: %v", err)
	}
}

// reorgAuthRows reads every policy row for the chain in version order.
func reorgAuthRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64) []reorgAuthRow {
	t.Helper()
	rows, err := pool.Query(ctx, `
SELECT policy_seq, max_depth, prev_seq, operator, reason, request_id, expected_old_seq, created_at
FROM reorg_policy_history WHERE chain_id = $1 ORDER BY policy_seq`, chainID)
	if err != nil {
		t.Fatalf("read reorg_policy_history: %v", err)
	}
	defer rows.Close()
	var out []reorgAuthRow
	for rows.Next() {
		var r reorgAuthRow
		if err := rows.Scan(&r.policySeq, &r.maxDepth, &r.prevSeq, &r.operator, &r.reason,
			&r.requestID, &r.expectedOldSeq, &r.createdAt); err != nil {
			t.Fatalf("scan reorg_policy_history row: %v", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate reorg_policy_history: %v", err)
	}
	return out
}

// reorgAuthSnapshot serializes the full chain history (every column, including
// created_at) so refusals and replays are compared as whole-state equality.
func reorgAuthSnapshot(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64) string {
	t.Helper()
	var b strings.Builder
	for _, r := range reorgAuthRows(t, ctx, pool, chainID) {
		prev := "NULL"
		if r.prevSeq != nil {
			prev = fmt.Sprintf("%d", *r.prevSeq)
		}
		req := "NULL"
		if r.requestID != nil {
			req = *r.requestID
		}
		fmt.Fprintf(&b, "seq=%d depth=%d prev=%s operator=%q reason=%q request_id=%s expected_old_seq=%d created_at=%s\n",
			r.policySeq, r.maxDepth, prev, r.operator, r.reason, req, r.expectedOldSeq,
			r.createdAt.UTC().Format(time.RFC3339Nano))
	}
	return b.String()
}

// reorgAuthAssertSnapshotUnchanged is the zero-state-change proof for refusals
// and replays: the entire history must be byte-identical afterwards.
func reorgAuthAssertSnapshotUnchanged(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, before, what string) {
	t.Helper()
	if after := reorgAuthSnapshot(t, ctx, pool, chainID); after != before {
		t.Fatalf("%s changed reorg_policy_history:\nbefore:\n%s\nafter:\n%s", what, before, after)
	}
}

// reorgAuthAssertRow compares one version row against expected values; a zero
// created_at is a failure (the audit timestamp must be set by the DB).
func reorgAuthAssertRow(t *testing.T, rows []reorgAuthRow, seq int64, want reorgAuthRow) {
	t.Helper()
	for _, got := range rows {
		if got.policySeq != seq {
			continue
		}
		gotPrev, wantPrev := "NULL", "NULL"
		if got.prevSeq != nil {
			gotPrev = fmt.Sprintf("%d", *got.prevSeq)
		}
		if want.prevSeq != nil {
			wantPrev = fmt.Sprintf("%d", *want.prevSeq)
		}
		gotReq, wantReq := "NULL", "NULL"
		if got.requestID != nil {
			gotReq = *got.requestID
		}
		if want.requestID != nil {
			wantReq = *want.requestID
		}
		if got.maxDepth != want.maxDepth || gotPrev != wantPrev || got.operator != want.operator ||
			got.reason != want.reason || gotReq != wantReq || got.expectedOldSeq != want.expectedOldSeq ||
			got.createdAt.IsZero() {
			t.Fatalf("row seq %d = (depth=%d prev=%s operator=%q reason=%q request_id=%s expected_old_seq=%d created_at=%s), want (depth=%d prev=%s operator=%q reason=%q request_id=%s expected_old_seq=%d, non-zero created_at)",
				seq, got.maxDepth, gotPrev, got.operator, got.reason, gotReq, got.expectedOldSeq, got.createdAt.UTC().Format(time.RFC3339Nano),
				want.maxDepth, wantPrev, want.operator, want.reason, wantReq, want.expectedOldSeq)
		}
		return
	}
	t.Fatalf("row seq %d missing from %d policy row(s)", seq, len(rows))
}

// reorgAuthCountRequest counts rows carrying one request identity.
func reorgAuthCountRequest(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, requestID string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM reorg_policy_history WHERE chain_id = $1 AND request_id = $2`,
		chainID, requestID).Scan(&n); err != nil {
		t.Fatalf("count policy rows for request_id %q: %v", requestID, err)
	}
	return n
}

// reorgAuthAssertZeroSideEffects proves the switch/refusal wrote no recovery
// row, no recovery event row, and no pause row on any stream (006 R9: policy
// authorization never touches those tables).
func reorgAuthAssertZeroSideEffects(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64) {
	t.Helper()
	for _, table := range []string{"reorg_recovery", "reorg_recovery_events"} {
		if n := reorgCount(t, ctx, pool, table, chainID); n != 0 {
			t.Fatalf("%s rows = %d, want 0 (policy authorization writes no recovery state)", table, n)
		}
	}
	confirmAuthAssertZeroPause(t, ctx, pool, chainID)
}

// reorgAuthTestPool boots one container database with all migrations for this
// file's authorization tests.
func reorgAuthTestPool(t *testing.T) (*pgxpool.Pool, context.Context) {
	t.Helper()
	pool := reorgTestPool(t)
	return pool, context.Background()
}

// --- tests -----------------------------------------------------------------

// TestReorgAuthFirstGrantHappyPathAndAudit
//
// Defect prevented: the first authorized depth change silently mis-records its
// audit set or writes more than the single appended version row.
// Invariant: from bootstrap seq 1 the switch appends exactly one row
// (seq 2, prev_seq 1) carrying max_depth, operator, reason, request_id and
// expected_old_seq verbatim; no recovery/event/pause row exists afterwards.
// Why existing tests are insufficient: AuthorizeReorgPolicy has zero callers
// anywhere in the tree, so this transaction had no end-to-end coverage.
// Level: integration, real PostgreSQL (migrations applied).
// Pass/fail: result == {2, new depth, Recorded:false}; both rows read back with
// the exact audit set; rowcount exactly 2; zero side effects.
func TestReorgAuthFirstGrantHappyPathAndAudit(t *testing.T) {
	pool, ctx := reorgAuthTestPool(t)
	const chainID = int64(907301)
	reorgAuthSeedBootstrap(t, ctx, pool, chainID, "25")

	res, err := AuthorizeReorgPolicy(ctx, pool, ReorgAuthRequest{
		ChainID: chainID, RequestID: "ra-first", ExpectedOldSeq: 1,
		NewMaxDepthRaw: "30", Operator: "op-alice", Reason: "raise depth for incident",
	})
	if err != nil {
		t.Fatalf("first authorized grant: %v", err)
	}
	if res.PolicySeq != 2 || res.MaxDepth != 30 || res.Recorded {
		t.Fatalf("result = %+v, want {PolicySeq:2 MaxDepth:30 Recorded:false}", res)
	}

	rows := reorgAuthRows(t, ctx, pool, chainID)
	if len(rows) != 2 {
		t.Fatalf("policy rows = %d, want 2 (bootstrap + one grant)", len(rows))
	}
	reorgAuthAssertRow(t, rows, 1, reorgAuthRow{
		maxDepth: 25, prevSeq: nil, operator: "bootstrap", reason: "",
		requestID: nil, expectedOldSeq: 0,
	})
	reorgAuthAssertRow(t, rows, 2, reorgAuthRow{
		maxDepth: 30, prevSeq: reorgPtrInt64(1), operator: "op-alice",
		reason: "raise depth for incident", requestID: reorgPtrString("ra-first"), expectedOldSeq: 1,
	})

	// The switch rides the shared coordination protocol: the lock row exists,
	// and nothing else was written for this chain.
	var leases int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM indexer_lease WHERE chain_id = $1`, chainID).Scan(&leases); err != nil {
		t.Fatalf("count indexer_lease: %v", err)
	}
	if leases != 1 {
		t.Fatalf("indexer_lease rows = %d, want 1 (write-protocol coordination row)", leases)
	}
	reorgAuthAssertZeroSideEffects(t, ctx, pool, chainID)
}

// TestReorgAuthSameRequestReplayRecorded
//
// Defect prevented: a repeated request_id re-executes (double-advances the
// version chain) or a recorded read-back is invalidated by a later version.
// Invariant: a same-intent replay returns the recorded result with
// Recorded=true and appends nothing; the duplicate comparison uses the four
// recorded intent fields, never the current effective version, so a replay
// still reads its own row back after the chain advanced (derived policy_seq is
// not part of the comparison).
// Why existing tests are insufficient: no integration caller of
// AuthorizeReorgPolicy existed; the unit file only tests helpers.
// Level: integration, real PostgreSQL.
// Pass/fail: replay result equals the executed result with Recorded=true;
// full history snapshot unchanged across each replay; 3 rows after the advance.
func TestReorgAuthSameRequestReplayRecorded(t *testing.T) {
	pool, ctx := reorgAuthTestPool(t)
	const chainID = int64(907302)
	reorgAuthSeedBootstrap(t, ctx, pool, chainID, "25")

	req := ReorgAuthRequest{
		ChainID: chainID, RequestID: "ra-replay", ExpectedOldSeq: 1,
		NewMaxDepthRaw: "30", Operator: "op-a", Reason: "first raise",
	}
	first, err := AuthorizeReorgPolicy(ctx, pool, req)
	if err != nil {
		t.Fatalf("first switch: %v", err)
	}
	if first.PolicySeq != 2 || first.MaxDepth != 30 || first.Recorded {
		t.Fatalf("first result = %+v, want {2 30 false}", first)
	}
	afterFirst := reorgAuthSnapshot(t, ctx, pool, chainID)

	second, err := AuthorizeReorgPolicy(ctx, pool, req)
	if err != nil {
		t.Fatalf("same-intent replay: %v", err)
	}
	if !second.Recorded || second.PolicySeq != 2 || second.MaxDepth != 30 {
		t.Fatalf("replay result = %+v, want recorded {2 30}", second)
	}
	reorgAuthAssertSnapshotUnchanged(t, ctx, pool, chainID, afterFirst, "same-intent replay")

	// Advance the chain; the original recorded request must still read its own
	// row back: the comparison never consults the derived/current version.
	advance, err := AuthorizeReorgPolicy(ctx, pool, ReorgAuthRequest{
		ChainID: chainID, RequestID: "ra-replay-advance", ExpectedOldSeq: 2,
		NewMaxDepthRaw: "35", Operator: "op-b", Reason: "second raise",
	})
	if err != nil {
		t.Fatalf("advancing switch: %v", err)
	}
	if advance.PolicySeq != 3 || advance.Recorded {
		t.Fatalf("advancing result = %+v, want {3 35 false}", advance)
	}
	afterAdvance := reorgAuthSnapshot(t, ctx, pool, chainID)
	if rows := reorgAuthRows(t, ctx, pool, chainID); len(rows) != 3 {
		t.Fatalf("policy rows after advance = %d, want 3", len(rows))
	}

	third, err := AuthorizeReorgPolicy(ctx, pool, req)
	if err != nil {
		t.Fatalf("cross-version replay: %v", err)
	}
	if !third.Recorded || third.PolicySeq != 2 || third.MaxDepth != 30 {
		t.Fatalf("cross-version replay result = %+v, want recorded {2 30}", third)
	}
	reorgAuthAssertSnapshotUnchanged(t, ctx, pool, chainID, afterAdvance, "cross-version replay")
	reorgAuthAssertZeroSideEffects(t, ctx, pool, chainID)
}

// TestReorgAuthSameRequestIDDifferentIntent
//
// Defect prevented: a reused request_id with mutated intent executes as a
// second version (identity collision), or the clash is detected by only some
// of the four compared fields.
// Invariant: each of expected_old_seq / max_depth / operator / reason changing
// under the same request_id is refused with ErrReorgPolicyRejected and zero
// state change; the recorded row stays byte-identical.
// Why existing tests are insufficient: the four-field comparison in
// classifyReorgAuthRequest had no integration-level coverage.
// Level: integration, real PostgreSQL.
// Pass/fail: every subcase refuses with ErrReorgPolicyRejected; full snapshot
// equality after each subcase; the rejected request_id remains bound to its
// original row only.
func TestReorgAuthSameRequestIDDifferentIntent(t *testing.T) {
	pool, ctx := reorgAuthTestPool(t)
	const chainID = int64(907303)
	reorgAuthSeedBootstrap(t, ctx, pool, chainID, "25")

	base := ReorgAuthRequest{
		ChainID: chainID, RequestID: "ra-clash", ExpectedOldSeq: 1,
		NewMaxDepthRaw: "30", Operator: "op-a", Reason: "original intent",
	}
	if _, err := AuthorizeReorgPolicy(ctx, pool, base); err != nil {
		t.Fatalf("original switch: %v", err)
	}
	before := reorgAuthSnapshot(t, ctx, pool, chainID)

	cases := []struct {
		field  string
		mutate func(*ReorgAuthRequest)
	}{
		{"max_depth", func(r *ReorgAuthRequest) { r.NewMaxDepthRaw = "31" }},
		{"expected_old_seq", func(r *ReorgAuthRequest) { r.ExpectedOldSeq = 2 }},
		{"operator", func(r *ReorgAuthRequest) { r.Operator = "op-b" }},
		{"reason", func(r *ReorgAuthRequest) { r.Reason = "mutated intent" }},
	}
	for _, tc := range cases {
		t.Run(tc.field+"_changed", func(t *testing.T) {
			clash := base
			tc.mutate(&clash)
			_, err := AuthorizeReorgPolicy(ctx, pool, clash)
			if !errors.Is(err, ErrReorgPolicyRejected) {
				t.Fatalf("same request_id with different %s = %v, want %v", tc.field, err, ErrReorgPolicyRejected)
			}
			if !strings.Contains(err.Error(), "different intent") {
				t.Fatalf("clash error = %v, want it to report the recorded different intent", err)
			}
			reorgAuthAssertSnapshotUnchanged(t, ctx, pool, chainID, before, "different-"+tc.field+" replay")
		})
	}
	if n := reorgAuthCountRequest(t, ctx, pool, chainID, "ra-clash"); n != 1 {
		t.Fatalf("ra-clash rows = %d, want 1 (the original binding)", n)
	}
	reorgAuthAssertZeroSideEffects(t, ctx, pool, chainID)
}

// TestReorgAuthStaleSeqRefused
//
// Defect prevented: an authorization based on a version that is not the
// current maximum silently advances the chain (lost update), whether the
// caller sent the pre-bootstrap default 0, a superseded seq, or a
// never-established future seq.
// Invariant: expectedOldSeq != current max policy_seq refuses with
// ErrReorgPolicyExpired before any transaction, reports both versions, binds
// no request_id, and changes no row.
// Why existing tests are insufficient: no integration test drove the switch
// gates at all.
// Level: integration, real PostgreSQL.
// Pass/fail: each variant returns ErrReorgPolicyExpired naming expected and
// latest; snapshot equality; every attempted request_id unbound; zero side
// effects.
func TestReorgAuthStaleSeqRefused(t *testing.T) {
	pool, ctx := reorgAuthTestPool(t)
	const chainID = int64(907304)
	reorgAuthSeedBootstrap(t, ctx, pool, chainID, "25")

	// Variant 1: the pre-bootstrap default 0 never matches an existing source
	// version once the bootstrap row is present.
	_, err := AuthorizeReorgPolicy(ctx, pool, ReorgAuthRequest{
		ChainID: chainID, RequestID: "ra-stale-zero", ExpectedOldSeq: 0,
		NewMaxDepthRaw: "30", Operator: "op", Reason: "stale against bootstrap",
	})
	if !errors.Is(err, ErrReorgPolicyExpired) {
		t.Fatalf("expected_old_seq 0 against seq 1 = %v, want %v", err, ErrReorgPolicyExpired)
	}
	if !strings.Contains(err.Error(), "expected policy_seq 0 but the latest is 1") {
		t.Fatalf("stale-zero error = %v, want it to name 0 vs 1", err)
	}
	before := reorgAuthSnapshot(t, ctx, pool, chainID)
	reorgAuthAssertSnapshotUnchanged(t, ctx, pool, chainID, before, "stale-zero refusal")

	// Bump the chain to seq 2 with a real grant.
	if _, err := AuthorizeReorgPolicy(ctx, pool, ReorgAuthRequest{
		ChainID: chainID, RequestID: "ra-stale-advance", ExpectedOldSeq: 1,
		NewMaxDepthRaw: "30", Operator: "op", Reason: "advance",
	}); err != nil {
		t.Fatalf("advancing switch: %v", err)
	}
	after := reorgAuthSnapshot(t, ctx, pool, chainID)

	// Variant 2 (behind): a request still based on the superseded seq 1.
	_, err = AuthorizeReorgPolicy(ctx, pool, ReorgAuthRequest{
		ChainID: chainID, RequestID: "ra-stale-behind", ExpectedOldSeq: 1,
		NewMaxDepthRaw: "35", Operator: "op", Reason: "based on superseded version",
	})
	if !errors.Is(err, ErrReorgPolicyExpired) {
		t.Fatalf("superseded expected_old_seq = %v, want %v", err, ErrReorgPolicyExpired)
	}
	if !strings.Contains(err.Error(), "expected policy_seq 1 but the latest is 2") {
		t.Fatalf("stale-behind error = %v, want it to name 1 vs 2", err)
	}
	reorgAuthAssertSnapshotUnchanged(t, ctx, pool, chainID, after, "stale-behind refusal")

	// Variant 3 (ahead): a version that was never established.
	_, err = AuthorizeReorgPolicy(ctx, pool, ReorgAuthRequest{
		ChainID: chainID, RequestID: "ra-stale-ahead", ExpectedOldSeq: 3,
		NewMaxDepthRaw: "40", Operator: "op", Reason: "based on a future version",
	})
	if !errors.Is(err, ErrReorgPolicyExpired) {
		t.Fatalf("future expected_old_seq = %v, want %v", err, ErrReorgPolicyExpired)
	}
	if !strings.Contains(err.Error(), "expected policy_seq 3 but the latest is 2") {
		t.Fatalf("stale-ahead error = %v, want it to name 3 vs 2", err)
	}
	reorgAuthAssertSnapshotUnchanged(t, ctx, pool, chainID, after, "stale-ahead refusal")

	for _, id := range []string{"ra-stale-zero", "ra-stale-behind", "ra-stale-ahead"} {
		if n := reorgAuthCountRequest(t, ctx, pool, chainID, id); n != 0 {
			t.Fatalf("%s rows = %d, want 0 (refusals bind no request identity)", id, n)
		}
	}
	reorgAuthAssertZeroSideEffects(t, ctx, pool, chainID)
}

// TestReorgAuthEmptyTableBootstrapRefused
//
// Defect prevented: a switch on a chain with no policy row creates the first
// version, letting an authorization bypass the establish-time bootstrap.
// Invariant: with zero source versions the switch refuses with
// ErrReorgPolicyRejected before opening any transaction; the table stays empty
// and the request_id stays unbound, so a later establish bootstrap plus a
// corrected retry with the SAME id executes normally.
// Why existing tests are insufficient: the bootstrap-via-switch guard had no
// integration coverage.
// Level: integration, real PostgreSQL.
// Pass/fail: refusal on the empty table; empty snapshot unchanged; retry after
// bind executes to seq 2 with the full audit set.
func TestReorgAuthEmptyTableBootstrapRefused(t *testing.T) {
	pool, ctx := reorgAuthTestPool(t)
	const chainID = int64(907305)

	_, err := AuthorizeReorgPolicy(ctx, pool, ReorgAuthRequest{
		ChainID: chainID, RequestID: "ra-bootstrap", ExpectedOldSeq: 0,
		NewMaxDepthRaw: "25", Operator: "op", Reason: "switch before any version",
	})
	if !errors.Is(err, ErrReorgPolicyRejected) {
		t.Fatalf("empty-table switch = %v, want %v", err, ErrReorgPolicyRejected)
	}
	if !strings.Contains(err.Error(), "no source version") {
		t.Fatalf("empty-table error = %v, want the no-source-version refusal", err)
	}
	if snap := reorgAuthSnapshot(t, ctx, pool, chainID); snap != "" {
		t.Fatalf("empty-table refusal wrote policy rows:\n%s", snap)
	}
	if n := reorgAuthCountRequest(t, ctx, pool, chainID, "ra-bootstrap"); n != 0 {
		t.Fatalf("ra-bootstrap rows = %d, want 0 (refusal binds nothing)", n)
	}

	// The refused id was left retryable: after the establish-style bootstrap
	// the same id with corrected params (base seq 1, a depth that changes the
	// effective 25) is a normal first grant.
	reorgAuthSeedBootstrap(t, ctx, pool, chainID, "25")
	res, err := AuthorizeReorgPolicy(ctx, pool, ReorgAuthRequest{
		ChainID: chainID, RequestID: "ra-bootstrap", ExpectedOldSeq: 1,
		NewMaxDepthRaw: "30", Operator: "op", Reason: "retry after bootstrap",
	})
	if err != nil {
		t.Fatalf("retry after bootstrap: %v", err)
	}
	if res.PolicySeq != 2 || res.MaxDepth != 30 || res.Recorded {
		t.Fatalf("retry result = %+v, want executed {2 30 false}", res)
	}
	rows := reorgAuthRows(t, ctx, pool, chainID)
	if len(rows) != 2 {
		t.Fatalf("policy rows = %d, want 2", len(rows))
	}
	reorgAuthAssertRow(t, rows, 2, reorgAuthRow{
		maxDepth: 30, prevSeq: reorgPtrInt64(1), operator: "op",
		reason: "retry after bootstrap", requestID: reorgPtrString("ra-bootstrap"), expectedOldSeq: 1,
	})
	reorgAuthAssertZeroSideEffects(t, ctx, pool, chainID)
}

// TestReorgAuthEmptyChangeRefused
//
// Defect prevented: an authorization that changes nothing still appends a
// version row (version inflation with no effective change).
// Invariant: new max_depth == current max_depth refuses with
// ErrReorgPolicyRejected and zero state change.
// Why existing tests are insufficient: no integration test drove the
// empty-change gate.
// Level: integration, real PostgreSQL.
// Pass/fail: refusal reporting the empty authorization; snapshot equality;
// request_id unbound; zero side effects.
func TestReorgAuthEmptyChangeRefused(t *testing.T) {
	pool, ctx := reorgAuthTestPool(t)
	const chainID = int64(907306)
	reorgAuthSeedBootstrap(t, ctx, pool, chainID, "25")
	before := reorgAuthSnapshot(t, ctx, pool, chainID)

	_, err := AuthorizeReorgPolicy(ctx, pool, ReorgAuthRequest{
		ChainID: chainID, RequestID: "ra-empty", ExpectedOldSeq: 1,
		NewMaxDepthRaw: "25", Operator: "op", Reason: "same depth",
	})
	if !errors.Is(err, ErrReorgPolicyRejected) {
		t.Fatalf("empty-change switch = %v, want %v", err, ErrReorgPolicyRejected)
	}
	if !strings.Contains(err.Error(), "empty authorization") {
		t.Fatalf("empty-change error = %v, want the empty-authorization refusal", err)
	}
	reorgAuthAssertSnapshotUnchanged(t, ctx, pool, chainID, before, "empty-change refusal")
	if n := reorgAuthCountRequest(t, ctx, pool, chainID, "ra-empty"); n != 0 {
		t.Fatalf("ra-empty rows = %d, want 0 (refusal binds nothing)", n)
	}
	reorgAuthAssertZeroSideEffects(t, ctx, pool, chainID)
}

// TestReorgAuthUnderLockReverifyExpires
//
// Defect prevented: the check-then-act gap — a request whose pre-tx read
// passed (expected seq still current) but whose version advanced before it
// acquired the coordination lock commits on the stale basis.
// Invariant: the under-lock re-read is authoritative; a version that moved
// between the pre-tx gate and the lock refuses with ErrReorgPolicyExpired
// naming the NEW latest version, writes no row, and binds no request_id.
// Why existing tests are insufficient: no test forced the window between the
// pre-tx gate and the lock; the concurrency test alone cannot distinguish the
// under-lock refusal from the pre-tx one.
// Level: integration, real PostgreSQL, deterministic barrier: a holder
// transaction takes the coordination lock exactly as a winning switch does and
// commits a seq-3 row through the production INSERT, then the loser (already
// blocked on the lock with a pre-tx read of seq 2) is released.
// Pass/fail: loser returns ErrReorgPolicyExpired ("latest is 3", i.e. the
// under-lock read, not the pre-tx "2"); final history is exactly the three
// winner rows; loser request_id unbound; zero side effects.
func TestReorgAuthUnderLockReverifyExpires(t *testing.T) {
	pool, ctx := reorgAuthTestPool(t)
	const chainID = int64(907307)
	reorgAuthSeedBootstrap(t, ctx, pool, chainID, "25")

	if _, err := AuthorizeReorgPolicy(ctx, pool, ReorgAuthRequest{
		ChainID: chainID, RequestID: "ra-underlock-setup", ExpectedOldSeq: 1,
		NewMaxDepthRaw: "30", Operator: "op-setup", Reason: "establish seq 2",
	}); err != nil {
		t.Fatalf("setup switch: %v", err)
	}

	// Holder transaction: coordination lock first, then the production INSERT
	// of the version a winning concurrent switch would have committed.
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin holder transaction: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, ensureLeaseSQL, chainID, "underlock-holder", 0, float64(3600)); err != nil {
		t.Fatalf("holder ensure coordination row: %v", err)
	}
	var (
		holderOwner string
		holderToken int64
		holderValid bool
	)
	if err := tx.QueryRow(ctx, lockCoordSQL, chainID).Scan(&holderOwner, &holderToken, &holderValid); err != nil {
		t.Fatalf("holder take coordination lock: %v", err)
	}

	type outcome struct {
		res ReorgAuthResult
		err error
	}
	done := make(chan outcome, 1)
	go func() {
		res, err := AuthorizeReorgPolicy(ctx, pool, ReorgAuthRequest{
			ChainID: chainID, RequestID: "ra-underlock-loser", ExpectedOldSeq: 2,
			NewMaxDepthRaw: "50", Operator: "op-loser", Reason: "pre-tx read saw seq 2",
		})
		done <- outcome{res: res, err: err}
	}()

	// Wait until the loser is genuinely blocked on the coordination lock: its
	// pre-tx gate has passed and its under-lock read has not run yet.
	waitUntil(t, time.Now().Add(10*time.Second), "loser blocked on the coordination lock", func() bool {
		var waiting int
		if err := pool.QueryRow(ctx, `
SELECT count(*) FROM pg_stat_activity
WHERE wait_event_type = 'Lock' AND query LIKE '%FROM indexer_lease%FOR UPDATE%'`).Scan(&waiting); err != nil {
			return false
		}
		return waiting > 0
	})

	// The winner commits seq 3 while the loser waits (production statement).
	tag, err := tx.Exec(ctx, insertReorgPolicySQL,
		chainID, int64(3), int64(40), int64(2), "op-winner", "won the lock first", "ra-underlock-winner", int64(2))
	if err != nil {
		t.Fatalf("winner insert: %v", err)
	}
	if tag.RowsAffected() != 1 {
		t.Fatalf("winner insert affected %d rows, want 1", tag.RowsAffected())
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("winner commit: %v", err)
	}

	out := <-done
	if !errors.Is(out.err, ErrReorgPolicyExpired) {
		t.Fatalf("under-lock reverify = (%+v, %v), want ErrReorgPolicyExpired", out.res, out.err)
	}
	if !strings.Contains(out.err.Error(), "expected policy_seq 2 but the latest is 3") {
		t.Fatalf("under-lock expiry = %v, want it to name 2 vs 3 (proving the post-lock re-read)", out.err)
	}

	rows := reorgAuthRows(t, ctx, pool, chainID)
	if len(rows) != 3 {
		t.Fatalf("policy rows = %d, want 3 (bootstrap, setup, winner only)", len(rows))
	}
	reorgAuthAssertRow(t, rows, 3, reorgAuthRow{
		maxDepth: 40, prevSeq: reorgPtrInt64(2), operator: "op-winner",
		reason: "won the lock first", requestID: reorgPtrString("ra-underlock-winner"), expectedOldSeq: 2,
	})
	if n := reorgAuthCountRequest(t, ctx, pool, chainID, "ra-underlock-loser"); n != 0 {
		t.Fatalf("ra-underlock-loser rows = %d, want 0 (loser wrote and bound nothing)", n)
	}
	if n := reorgAuthCountRequest(t, ctx, pool, chainID, "ra-underlock-winner"); n != 1 {
		t.Fatalf("ra-underlock-winner rows = %d, want 1", n)
	}
	reorgAuthAssertZeroSideEffects(t, ctx, pool, chainID)
}

// TestReorgBindDriftRefusedZeroWrite
//
// Defect prevented: the establish-time policy bind silently follows an
// effective policy that differs from the configured env depth (R4 drift), or
// an illegal env depth still creates a recovery instance.
// Invariant: with a diverging effective row, establish refuses with
// *ReorgPolicyDriftError and rolls back the whole transaction (zero recovery
// row, zero event row, policy history untouched); an illegal env depth refuses
// with ErrReorgPolicyRejected; the same establish with the matching env depth
// then succeeds on the existing bootstrap version.
// Why existing tests are insufficient: the bind/drift path was only unit
// covered via VerifyReorgPolicyIdentity, never through EstablishRecovery with
// durable-state assertions.
// Level: integration, real PostgreSQL, real lease.
// Known inconsistency encoded deliberately (not resolved): absent row + env
// set bootstraps here, while research R4 describes a loud startup refusal.
// Pass/fail: drift error type/message; zero writes on both refusals; snapshot
// equality; matching-depths establish binds seq 1 and writes exactly one
// recovery row + one established event.
func TestReorgBindDriftRefusedZeroWrite(t *testing.T) {
	pool, ctx := reorgAuthTestPool(t)
	const chainID = int64(907308)
	lease := depositITLease(t, pool, chainID)
	reorgSeedChain(t, ctx, pool, chainID, 10, 15, 10)
	reorgAuthSeedBootstrap(t, ctx, pool, chainID, "25")
	before := reorgAuthSnapshot(t, ctx, pool, chainID)

	req := EstablishRequest{
		ChainID:      chainID,
		OldTipNumber: 15, OldTipHash: depositBlockHash(15),
		NewTipNumber: 16, NewTipHash: depositBlockHash(115),
		DetectedHeight: 15, EnvMaxDepthRaw: "26",
	}
	_, err := EstablishRecovery(ctx, pool, lease, req)
	var drift *ReorgPolicyDriftError
	if !errors.As(err, &drift) {
		t.Fatalf("establish with drifting env depth = %v, want *ReorgPolicyDriftError", err)
	}
	if !strings.Contains(err.Error(), "max_depth=25") || !strings.Contains(err.Error(), "configured max_depth=26") {
		t.Fatalf("drift error = %v, want it to name effective 25 vs configured 26", err)
	}
	if n := reorgCount(t, ctx, pool, "reorg_recovery", chainID); n != 0 {
		t.Fatalf("reorg_recovery rows = %d after drift refusal, want 0", n)
	}
	if n := reorgCount(t, ctx, pool, "reorg_recovery_events", chainID); n != 0 {
		t.Fatalf("reorg_recovery_events rows = %d after drift refusal, want 0", n)
	}
	reorgAuthAssertSnapshotUnchanged(t, ctx, pool, chainID, before, "drift refusal")

	// Illegal env depth: Q1 refusal, whole transaction rolled back.
	req.EnvMaxDepthRaw = "0"
	if _, err := EstablishRecovery(ctx, pool, lease, req); !errors.Is(err, ErrReorgPolicyRejected) {
		t.Fatalf("establish with illegal env depth = %v, want %v", err, ErrReorgPolicyRejected)
	}
	if n := reorgCount(t, ctx, pool, "reorg_recovery", chainID); n != 0 {
		t.Fatalf("reorg_recovery rows = %d after illegal-depth refusal, want 0", n)
	}
	if n := reorgCount(t, ctx, pool, "reorg_recovery_events", chainID); n != 0 {
		t.Fatalf("reorg_recovery_events rows = %d after illegal-depth refusal, want 0", n)
	}
	reorgAuthAssertSnapshotUnchanged(t, ctx, pool, chainID, before, "illegal-depth refusal")

	// Matching env depth: establish binds the existing bootstrap version and
	// writes the recovery instance atomically; the refusals poisoned nothing.
	req.EnvMaxDepthRaw = "25"
	res, err := EstablishRecovery(ctx, pool, lease, req)
	if err != nil {
		t.Fatalf("matching-depth establish after refusals: %v", err)
	}
	if res.Converged || res.Seq != 1 || res.RecoveryID == "" {
		t.Fatalf("establish result = %+v, want a fresh instance at seq 1", res)
	}
	if n := reorgCount(t, ctx, pool, "reorg_recovery", chainID); n != 1 {
		t.Fatalf("reorg_recovery rows = %d after establish, want 1", n)
	}
	if n := reorgCount(t, ctx, pool, "reorg_recovery_events", chainID); n != 1 {
		t.Fatalf("reorg_recovery_events rows = %d after establish, want 1", n)
	}
	reorgAuthAssertSnapshotUnchanged(t, ctx, pool, chainID, before, "successful bind (existing bootstrap reused, never re-created)")
}

// TestReorgAuthFailureLeavesIDUnboundAndUnknownCommitResolves
//
// Defect prevented: a failed or unknown-outcome switch leaves the request_id
// bound (making a corrected retry impossible), or an unknown COMMIT is
// re-executed / assumed rather than resolved from the database.
// Invariant: (a) an explicit gate refusal binds nothing and the corrected
// retry with the same id executes; (b) when the COMMIT reply is lost after
// PostgreSQL accepted it, the recorded row resolves the request as
// Recorded=true with exactly one COMMIT attempt; (c) when the COMMIT never
// lands, the request returns an unknown-outcome error, writes nothing, leaves
// the id unbound, and the retry executes.
// Why existing tests are insufficient: no fault-injected authorization test
// existed for the 006 switch (005 has the analogous D11 test).
// Level: integration, real PostgreSQL, real dial-wrapper fault pool.
// Pass/fail: per-phase error classes, fault fire counts (exactly 1 per armed
// COMMIT), row counts/audit sets, request_id binding, snapshot equality.
func TestReorgAuthFailureLeavesIDUnboundAndUnknownCommitResolves(t *testing.T) {
	dsn := startIndexerPostgres(t)
	ctx := context.Background()
	pool := openIndexerPool(t, dsn)
	t.Cleanup(pool.Close)
	const chainID = int64(907309)
	reorgAuthSeedBootstrap(t, ctx, pool, chainID, "25")

	// (a) An explicitly failed switch binds nothing; the corrected retry with
	// the same request_id executes as a fresh grant.
	stale := ReorgAuthRequest{
		ChainID: chainID, RequestID: "ra-retry", ExpectedOldSeq: 99,
		NewMaxDepthRaw: "30", Operator: "op", Reason: "stale attempt",
	}
	if _, err := AuthorizeReorgPolicy(ctx, pool, stale); !errors.Is(err, ErrReorgPolicyExpired) {
		t.Fatalf("stale attempt = %v, want %v", err, ErrReorgPolicyExpired)
	}
	if n := reorgAuthCountRequest(t, ctx, pool, chainID, "ra-retry"); n != 0 {
		t.Fatalf("ra-retry rows = %d, want 0 (failed attempt binds nothing)", n)
	}
	retry := stale
	retry.ExpectedOldSeq = 1
	resRetry, err := AuthorizeReorgPolicy(ctx, pool, retry)
	if err != nil {
		t.Fatalf("corrected retry: %v", err)
	}
	if resRetry.PolicySeq != 2 || resRetry.MaxDepth != 30 || resRetry.Recorded {
		t.Fatalf("retry result = %+v, want executed {2 30 false}", resRetry)
	}
	if n := reorgAuthCountRequest(t, ctx, pool, chainID, "ra-retry"); n != 1 {
		t.Fatalf("ra-retry rows = %d, want 1 (the executed retry)", n)
	}

	// (b) COMMIT reply lost: the statement landed once and the database
	// re-read resolves the request as recorded (never a second INSERT).
	faultDrop := newDepositFault()
	dropPool := depositOpenFaultPool(t, dsn, faultDrop)
	faultDrop.armSQL(depositFaultCommit, true, false)
	reqDrop := ReorgAuthRequest{
		ChainID: chainID, RequestID: "ra-unknown-reply", ExpectedOldSeq: 2,
		NewMaxDepthRaw: "35", Operator: "op", Reason: "reply dropped after acceptance",
	}
	resDrop, err := AuthorizeReorgPolicy(ctx, dropPool, reqDrop)
	if err != nil {
		t.Fatalf("switch with the COMMIT reply dropped = %v, want the recorded result read from the DB", err)
	}
	if got := faultDrop.fires.Load(); got != 1 {
		t.Fatalf("COMMIT drops = %d, want exactly 1 (a retry would double-insert)", got)
	}
	if !resDrop.Recorded || resDrop.PolicySeq != 3 || resDrop.MaxDepth != 35 {
		t.Fatalf("dropped-reply result = %+v, want recorded {3 35}", resDrop)
	}
	rows := reorgAuthRows(t, ctx, pool, chainID)
	if len(rows) != 3 {
		t.Fatalf("policy rows = %d after dropped reply, want 3 (COMMIT landed exactly once)", len(rows))
	}
	reorgAuthAssertRow(t, rows, 3, reorgAuthRow{
		maxDepth: 35, prevSeq: reorgPtrInt64(2), operator: "op",
		reason: "reply dropped after acceptance", requestID: reorgPtrString("ra-unknown-reply"), expectedOldSeq: 2,
	})
	afterDrop := reorgAuthSnapshot(t, ctx, pool, chainID)
	replayDrop, err := AuthorizeReorgPolicy(ctx, pool, reqDrop)
	if err != nil {
		t.Fatalf("joined replay after dropped reply: %v", err)
	}
	if !replayDrop.Recorded || replayDrop.PolicySeq != 3 {
		t.Fatalf("joined replay = %+v, want recorded {3 35}", replayDrop)
	}
	reorgAuthAssertSnapshotUnchanged(t, ctx, pool, chainID, afterDrop, "joined replay after dropped reply")
	if got := faultDrop.fires.Load(); got != 1 {
		t.Fatalf("COMMIT drops after replay = %d, want still 1", got)
	}
	faultDrop.disarm()

	// (c) COMMIT never landed: the connection dies before the statement
	// reaches the server, so the row was rolled back, the id is unbound, and
	// the retry executes as a fresh grant.
	faultLost := newDepositFault()
	lostPool := depositOpenFaultPool(t, dsn, faultLost)
	faultLost.armSQL(depositFaultCommit, false, false)
	reqLost := ReorgAuthRequest{
		ChainID: chainID, RequestID: "ra-unknown-noland", ExpectedOldSeq: 3,
		NewMaxDepthRaw: "40", Operator: "op", Reason: "connection death before the statement landed",
	}
	if _, err := AuthorizeReorgPolicy(ctx, lostPool, reqLost); err == nil {
		t.Fatal("switch with a never-landed COMMIT = nil, want an unknown-outcome error")
	} else if !strings.Contains(err.Error(), "outcome unknown") {
		t.Fatalf("never-landed COMMIT error = %v, want the unknown-outcome classification", err)
	}
	if got := faultLost.fires.Load(); got != 1 {
		t.Fatalf("connection deaths at COMMIT = %d, want exactly 1", got)
	}
	if rows := reorgAuthRows(t, ctx, pool, chainID); len(rows) != 3 {
		t.Fatalf("policy rows after never-landed COMMIT = %d, want 3 (nothing landed)", len(rows))
	}
	if n := reorgAuthCountRequest(t, ctx, pool, chainID, "ra-unknown-noland"); n != 0 {
		t.Fatalf("ra-unknown-noland rows = %d, want 0 (unlanded COMMIT binds nothing)", n)
	}
	resLost, err := AuthorizeReorgPolicy(ctx, pool, reqLost)
	if err != nil {
		t.Fatalf("retry after never-landed COMMIT: %v", err)
	}
	if resLost.PolicySeq != 4 || resLost.MaxDepth != 40 || resLost.Recorded {
		t.Fatalf("retry result = %+v, want executed {4 40 false}", resLost)
	}
	rows = reorgAuthRows(t, ctx, pool, chainID)
	if len(rows) != 4 {
		t.Fatalf("policy rows = %d after retry, want 4", len(rows))
	}
	reorgAuthAssertRow(t, rows, 4, reorgAuthRow{
		maxDepth: 40, prevSeq: reorgPtrInt64(3), operator: "op",
		reason: "connection death before the statement landed", requestID: reorgPtrString("ra-unknown-noland"), expectedOldSeq: 3,
	})
	reorgAuthAssertZeroSideEffects(t, ctx, pool, chainID)
}

// TestReorgAuthConcurrentDuplicateSingleAppend
//
// Defect prevented: two executors racing on the same request identity append
// two versions (or lose the identity binding) because the pre-tx
// classification is not reconciled under the coordination lock.
// Invariant: the chain-wide lock serializes the transactions, so exactly one
// call executes and appends seq 2; the other converges by reading the recorded
// row back (Recorded=true) or by the documented basis-moved expiry; a joined
// retry is always Recorded and changes nothing.
// Why existing tests are insufficient: the switch had no concurrent-execution
// test; the 005 D11 precedent does not exercise this transaction's under-lock
// gates.
// Level: integration, real PostgreSQL, start-close barrier + WaitGroup.
// Pass/fail: trichotomy counts exactly one executed + one converging outcome;
// one bound row with the full audit set; snapshot equality around the joined
// retry; zero recovery/event/pause rows.
func TestReorgAuthConcurrentDuplicateSingleAppend(t *testing.T) {
	pool, ctx := reorgAuthTestPool(t)
	const chainID = int64(907310)
	reorgAuthSeedBootstrap(t, ctx, pool, chainID, "25")

	req := ReorgAuthRequest{
		ChainID: chainID, RequestID: "ra-concurrent", ExpectedOldSeq: 1,
		NewMaxDepthRaw: "30", Operator: "op-a", Reason: "concurrent duplicate",
	}
	type outcome struct {
		res ReorgAuthResult
		err error
	}
	outcomes := make([]outcome, 2)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range outcomes {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			res, err := AuthorizeReorgPolicy(ctx, pool, req)
			outcomes[i] = outcome{res: res, err: err}
		}(i)
	}
	close(start)
	wg.Wait()

	wantExecuted := ReorgAuthResult{PolicySeq: 2, MaxDepth: 30}
	wantRecorded := wantExecuted
	wantRecorded.Recorded = true
	executed, recorded, expired := 0, 0, 0
	for i, o := range outcomes {
		switch {
		case o.err == nil && o.res == wantExecuted:
			executed++
		case o.err == nil && o.res == wantRecorded:
			recorded++
		case o.err != nil && errors.Is(o.err, ErrReorgPolicyExpired):
			expired++
		default:
			t.Fatalf("outcome %d = (%+v, %v), want the executed, recorded or expired verdict", i, o.res, o.err)
		}
	}
	if executed != 1 || recorded+expired != 1 {
		t.Fatalf("outcomes = executed %d recorded %d expired %d, want exactly one executed and one converging duplicate",
			executed, recorded, expired)
	}

	rows := reorgAuthRows(t, ctx, pool, chainID)
	if len(rows) != 2 {
		t.Fatalf("policy rows = %d, want 2 (the duplicate never executes twice)", len(rows))
	}
	reorgAuthAssertRow(t, rows, 2, reorgAuthRow{
		maxDepth: 30, prevSeq: reorgPtrInt64(1), operator: "op-a",
		reason: "concurrent duplicate", requestID: reorgPtrString("ra-concurrent"), expectedOldSeq: 1,
	})
	if n := reorgAuthCountRequest(t, ctx, pool, chainID, "ra-concurrent"); n != 1 {
		t.Fatalf("ra-concurrent rows = %d, want 1 (single identity binding)", n)
	}

	after := reorgAuthSnapshot(t, ctx, pool, chainID)
	joined, err := AuthorizeReorgPolicy(ctx, pool, req)
	if err != nil {
		t.Fatalf("joined retry after the race: %v", err)
	}
	if joined != wantRecorded {
		t.Fatalf("joined retry = %+v, want %+v", joined, wantRecorded)
	}
	reorgAuthAssertSnapshotUnchanged(t, ctx, pool, chainID, after, "joined retry after the race")
	reorgAuthAssertZeroSideEffects(t, ctx, pool, chainID)
}

// TestReorgAuthFieldRefusals
//
// Defect prevented: pre-transaction field validation lets a malformed request
// through (including the nil-pool/unusable-chain shapes), or a rejected
// request mutates durable state.
// Invariant: nil pool, non-positive chain id, empty request_id, missing
// operator/reason, and every Q1-illegal depth (blank, zero, negative,
// non-decimal, fractional, over MaxInt64) are refused; the policy table is
// untouched throughout.
// Why existing tests are insufficient: only ParseReorgMaxDepth's string rule
// was unit tested; none of the request-shape refusals nor their zero-write
// guarantee had integration coverage.
// Level: integration, real PostgreSQL (one bootstrapped chain for all cases).
// Pass/fail: each subcase returns an error (ErrReorgPolicyRejected where the
// implementation wraps it), and the full snapshot is unchanged after all
// subcases.
func TestReorgAuthFieldRefusals(t *testing.T) {
	pool, ctx := reorgAuthTestPool(t)
	const chainID = int64(907311)
	reorgAuthSeedBootstrap(t, ctx, pool, chainID, "25")
	before := reorgAuthSnapshot(t, ctx, pool, chainID)

	valid := func() ReorgAuthRequest {
		return ReorgAuthRequest{
			ChainID: chainID, RequestID: "ra-field", ExpectedOldSeq: 1,
			NewMaxDepthRaw: "30", Operator: "op", Reason: "why",
		}
	}
	cases := []struct {
		name         string
		req          ReorgAuthRequest
		nilPool      bool
		wantRejected bool
		wantContains string
	}{
		{"nil_pool", valid(), true, false, "nil pool"},
		{"zero_chain_id", func() ReorgAuthRequest { r := valid(); r.ChainID = 0; return r }(), false, true, "chain id 0"},
		{"negative_chain_id", func() ReorgAuthRequest { r := valid(); r.ChainID = -7; return r }(), false, true, "chain id -7"},
		{"empty_request_id", func() ReorgAuthRequest { r := valid(); r.RequestID = ""; return r }(), false, true, "empty request_id"},
		{"empty_operator", func() ReorgAuthRequest { r := valid(); r.Operator = ""; return r }(), false, true, "operator and reason"},
		{"empty_reason", func() ReorgAuthRequest { r := valid(); r.Reason = ""; return r }(), false, true, "operator and reason"},
		{"blank_depth", func() ReorgAuthRequest { r := valid(); r.NewMaxDepthRaw = ""; return r }(), false, true, "no default"},
		{"zero_depth", func() ReorgAuthRequest { r := valid(); r.NewMaxDepthRaw = "0"; return r }(), false, true, "positive"},
		{"negative_depth", func() ReorgAuthRequest { r := valid(); r.NewMaxDepthRaw = "-1"; return r }(), false, true, "decimal integer"},
		{"non_numeric_depth", func() ReorgAuthRequest { r := valid(); r.NewMaxDepthRaw = "abc"; return r }(), false, true, "decimal integer"},
		{"fractional_depth", func() ReorgAuthRequest { r := valid(); r.NewMaxDepthRaw = "1.5"; return r }(), false, true, "decimal integer"},
		{"depth_over_maxint64", func() ReorgAuthRequest { r := valid(); r.NewMaxDepthRaw = "9223372036854775808"; return r }(), false, true, "system-supported"},
		{"depth_maxuint64", func() ReorgAuthRequest { r := valid(); r.NewMaxDepthRaw = "18446744073709551615"; return r }(), false, true, "system-supported"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			usePool := pool
			if tc.nilPool {
				usePool = nil
			}
			_, err := AuthorizeReorgPolicy(ctx, usePool, tc.req)
			if err == nil {
				t.Fatalf("request %+v accepted, want refusal", tc.req)
			}
			if tc.wantRejected && !errors.Is(err, ErrReorgPolicyRejected) {
				t.Fatalf("refusal = %v, want %v", err, ErrReorgPolicyRejected)
			}
			if !strings.Contains(err.Error(), tc.wantContains) {
				t.Fatalf("refusal = %v, want it to mention %q", err, tc.wantContains)
			}
			reorgAuthAssertSnapshotUnchanged(t, ctx, pool, chainID, before, "field refusal "+tc.name)
		})
	}
	reorgAuthAssertZeroSideEffects(t, ctx, pool, chainID)
}
