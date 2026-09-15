//go:build integration

// idempotency_integration_test.go owns the T021 [US4] same-key N-way
// concurrency test against a real PostgreSQL 18 container (testcontainers).
//
// T021 OWNS this file: T022 appends the cross-key same-grant and cross-caller
// isolation cases here afterwards (no [P] with T021). Every helper below is
// deliberately generic and `idempotency`-prefixed so T022 can reuse it without
// redefining anything.
//
// It reuses the T006 container/migration helper (grantSetup) and the T010
// intake seeding helpers (intakeKey, intakeSupplyGrant, intakeReq,
// intakeRequestCount, intakeAuditActions, intakeWantNoAudit, intakeWantIntent,
// intakeRequestIDShape) unchanged; it adds only idempotency-prefixed helpers.
//
// Coverage: V5 (same key + equal params -> 200 same request_id, row count
// unchanged) plus the first half of V6 (N-way parallel same-key POSTs ->
// exactly one row, all callers see the same request_id).
package withdrawal

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// idempotencyConcurrency is N, the number of same-key callers raced by T021.
// grantSetup -> db.OpenPool pins the pool's MaxConns to 8 explicitly, so N=8 is
// exactly the number of connections required to hold every caller in flight.
const idempotencyConcurrency = 8

// idempotencySeed issues a credential for callerID and seeds one active grant
// bound to authorizationID through the reviewed T008 supply path. It returns
// the plaintext key. Generic on purpose: T022 reuses it for its own callers.
func idempotencySeed(t *testing.T, ctx context.Context, pool *pgxpool.Pool, callerID int64, authorizationID string) string {
	t.Helper()
	key := intakeKey(t, ctx, pool, callerID)
	intakeSupplyGrant(t, ctx, pool, callerID, authorizationID, intakeAmount)
	return key
}

// idempotencyProvePoolCapacity makes the "all N callers are genuinely in
// flight" claim falsifiable instead of hopeful. It fails unless the pool is
// configured to allow N simultaneous connections, then actually holds N
// connections at once before releasing them. If MaxConns < N the Acquire loop
// would block until a connection freed — the explicit MaxConns check reports
// that failure instead of letting the race silently serialize.
func idempotencyProvePoolCapacity(t *testing.T, ctx context.Context, pool *pgxpool.Pool, n int) {
	t.Helper()
	if got := pool.Stat().MaxConns(); got < int32(n) {
		t.Fatalf("pool MaxConns = %d, want >= %d so all callers can be genuinely in flight", got, n)
	}
	held := make([]*pgxpool.Conn, 0, n)
	for i := 0; i < n; i++ {
		conn, err := pool.Acquire(ctx)
		if err != nil {
			t.Fatalf("acquire connection %d/%d: %v", i+1, n, err)
		}
		held = append(held, conn)
	}
	for _, conn := range held {
		conn.Release()
	}
}

// idempotencyNWay releases n identical submits from a single start barrier and
// returns their results in caller-index order.
//
// The barrier is the only synchronization: every goroutine parks at <-start
// after signalling ready, so close(start) launches all n together with no
// sleeps and no chance-based pacing. Paired with idempotencyProvePoolCapacity
// (N connections provably available) and the pool sizing it asserts, all n
// receipt transactions can be genuinely concurrent. The assertions built on the
// results are order-independent, so a caller that happens to run ahead of
// another cannot make the test pass or fail for the wrong reason — it fails
// only if the idempotency constraint or its classify is broken.
func idempotencyNWay(t *testing.T, ctx context.Context, pool *pgxpool.Pool, req SubmitRequest, n int) []*SubmitResult {
	t.Helper()
	results := make([]*SubmitResult, n)
	errs := make([]error, n)
	var ready, done sync.WaitGroup
	start := make(chan struct{})
	ready.Add(n)
	done.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer done.Done()
			ready.Done()
			<-start
			results[i], errs[i] = SubmitWithdrawal(ctx, pool, req)
		}(i)
	}
	ready.Wait() // every caller is parked at the barrier
	close(start) // release them together
	done.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent submit %d: %v", i, err)
		}
	}
	return results
}

// idempotencyCountByKey reads the persistent row count for one (caller, key)
// scope — a table read, not a response code.
func idempotencyCountByKey(t *testing.T, ctx context.Context, pool *pgxpool.Pool, callerID int64, key string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM withdrawal_requests WHERE caller_id = $1 AND idempotency_key = $2`,
		callerID, key).Scan(&n); err != nil {
		t.Fatalf("count withdrawal_requests by (caller, key): %v", err)
	}
	return n
}

// idempotencyDistinctRequestIDs reads the DISTINCT request_id values the table
// actually holds for one (caller, key) scope — the persistent-state proof that
// the race minted a single id.
func idempotencyDistinctRequestIDs(t *testing.T, ctx context.Context, pool *pgxpool.Pool, callerID int64, key string) []string {
	t.Helper()
	rows, err := pool.Query(ctx,
		`SELECT DISTINCT request_id FROM withdrawal_requests WHERE caller_id = $1 AND idempotency_key = $2`,
		callerID, key)
	if err != nil {
		t.Fatalf("select distinct request_id by (caller, key): %v", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan distinct request_id: %v", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate distinct request_id: %v", err)
	}
	return ids
}

// TestWithdrawalIdempotencySameKeyNWayConcurrent covers T021 / V5 + V6-first-
// half: N identical same-key same-params submits released from one barrier MUST
// converge on exactly one persisted row and one request_id for every caller.
// Exactly one caller is the creator (201); the rest are replays (200) carrying
// replay intent. Never 409/403/503.
func TestWithdrawalIdempotencySameKeyNWayConcurrent(t *testing.T) {
	// Given a caller with an active grant and a pool able to hold all callers.
	ctx, pool := grantSetup(t)
	const callerID = int64(7201)
	const key = "idem-nway-1"
	const authorizationID = "auth-idem-nway-1"
	presented := idempotencySeed(t, ctx, pool, callerID, authorizationID)
	idempotencyProvePoolCapacity(t, ctx, pool, idempotencyConcurrency)

	// When n identical same-key same-params submits are released together.
	results := idempotencyNWay(t, ctx, pool, intakeReq(presented, key, authorizationID), idempotencyConcurrency)

	// Then exactly one is the creator: 201 with no pre-tx audit intent (its
	// `created` row is written in-tx), and every other caller is a 200 replay
	// of that same id. No status outside {200, 201} is ever acceptable.
	created := 0
	distinct := map[string]struct{}{}
	for i, res := range results {
		switch res.Status {
		case 201:
			created++
			intakeWantNoAudit(t, res)
		case 200:
			// checked below once the single id is known
		default:
			t.Fatalf("submit %d = %+v, want 200 or 201 (never 409/403/503)", i, res)
		}
		if res.RequestID == "" {
			t.Fatalf("submit %d returned an empty request_id", i)
		}
		if !intakeRequestIDShape.MatchString(res.RequestID) {
			t.Fatalf("submit %d request_id = %q, want wr- + 32 hex", i, res.RequestID)
		}
		distinct[res.RequestID] = struct{}{}
	}
	if created != 1 {
		t.Fatalf("201 count = %d, want exactly 1", created)
	}
	if len(distinct) != 1 {
		t.Fatalf("distinct request_ids across results = %v, want exactly 1", distinct)
	}

	var winner string
	for id := range distinct {
		winner = id
	}
	for _, res := range results {
		if res.Status == 200 {
			// Replay intent: the loser must report the winner's id and the
			// `replayed` action for the same caller (response-first rule).
			intakeWantIntent(t, res, auditActionReplayed, callerID, winner)
		}
	}

	// And the persistent state — COUNT(*) plus DISTINCT request_id read from
	// the table, not the responses — proves exactly one row and one id.
	if n := idempotencyCountByKey(t, ctx, pool, callerID, key); n != 1 {
		t.Fatalf("persisted rows for (caller %d, key %q) = %d, want 1", callerID, key, n)
	}
	if n := intakeRequestCount(t, ctx, pool); n != 1 {
		t.Fatalf("total withdrawal_requests rows = %d, want 1", n)
	}
	ids := idempotencyDistinctRequestIDs(t, ctx, pool, callerID, key)
	if len(ids) != 1 || ids[0] != winner {
		t.Fatalf("distinct persisted request_ids = %v, want [%q]", ids, winner)
	}

	// And the in-tx `created` audit was written exactly once; the replay
	// outcomes carry intent only (the core persists no replay row).
	intakeWantActions(t, intakeAuditActions(t, ctx, pool), []string{auditActionCreated})
}

// TestWithdrawalIdempotencySequentialReplayBaseline covers T021's second
// scenario: the same key submitted strictly after the first has committed is
// the 200-same-id outcome the N-way race converges to. It pins that baseline
// and proves the converged state is durable.
func TestWithdrawalIdempotencySequentialReplayBaseline(t *testing.T) {
	// Given a caller with an active grant.
	ctx, pool := grantSetup(t)
	const callerID = int64(7202)
	const key = "idem-baseline-1"
	const authorizationID = "auth-idem-baseline-1"
	presented := idempotencySeed(t, ctx, pool, callerID, authorizationID)

	// When the first submit commits and a later same-key same-params submit
	// arrives after that commit (sequential — the race's converged baseline).
	first, err := SubmitWithdrawal(ctx, pool, intakeReq(presented, key, authorizationID))
	if err != nil {
		t.Fatalf("first SubmitWithdrawal: %v", err)
	}
	if first.Status != 201 {
		t.Fatalf("first status = %d (%+v), want 201", first.Status, first)
	}
	intakeWantNoAudit(t, first)

	replay, err := SubmitWithdrawal(ctx, pool, intakeReq(presented, key, authorizationID))
	if err != nil {
		t.Fatalf("later SubmitWithdrawal: %v", err)
	}

	// Then the later arrival is a 200 replay of the committed id with no new
	// row, and the `created` row is still exactly one.
	if replay.Status != 200 || replay.RequestID != first.RequestID {
		t.Fatalf("later submit = %+v, want 200 with request_id %q", replay, first.RequestID)
	}
	intakeWantIntent(t, replay, auditActionReplayed, callerID, first.RequestID)
	if n := idempotencyCountByKey(t, ctx, pool, callerID, key); n != 1 {
		t.Fatalf("persisted rows for (caller %d, key %q) = %d, want 1", callerID, key, n)
	}
	ids := idempotencyDistinctRequestIDs(t, ctx, pool, callerID, key)
	if len(ids) != 1 || ids[0] != first.RequestID {
		t.Fatalf("distinct persisted request_ids = %v, want [%q]", ids, first.RequestID)
	}
	intakeWantActions(t, intakeAuditActions(t, ctx, pool), []string{auditActionCreated})
}

// ---------------------------------------------------------------------------
// T022 [US4] — appended here, after T021, per task order (no [P] with T021).
//
// Covers the second half of V6: cross-key same-grant rejection (a different
// key reusing a consumed grant is 403 with the original row untouched) and the
// (caller_id, idempotency_key) scoping that makes two callers' identical key
// strings independent. Shares T021's idempotency-prefixed helpers and the
// T006/T010 container + seed helpers; adds only idempotencyRowSnapshot.
// ---------------------------------------------------------------------------

// idempotencyRowSnapshot is every persisted withdrawal_requests column except
// the surrogate identity `id`, captured so a before/after comparison proves the
// row is byte-identical. All fields are comparable, so == is the whole check.
type idempotencyRowSnapshot struct {
	requestID       string
	callerID        int64
	idempotencyKey  string
	authorizationID string
	chainID         int64
	asset           string
	recipient       string
	amount          string
	status          string
	createdAt       time.Time
}

// idempotencyRowSnapshotOf reads the full persisted row for requestID. It fails
// the test rather than returning a zero value, so a missing row cannot pass a
// "unchanged" comparison by accident.
func idempotencyRowSnapshotOf(t *testing.T, ctx context.Context, pool *pgxpool.Pool, requestID string) idempotencyRowSnapshot {
	t.Helper()
	var s idempotencyRowSnapshot
	if err := pool.QueryRow(ctx, `
SELECT request_id, caller_id, idempotency_key, authorization_id, chain_id, asset, recipient, amount::text, status, created_at
FROM withdrawal_requests WHERE request_id = $1`, requestID).
		Scan(&s.requestID, &s.callerID, &s.idempotencyKey, &s.authorizationID, &s.chainID,
			&s.asset, &s.recipient, &s.amount, &s.status, &s.createdAt); err != nil {
		t.Fatalf("snapshot withdrawal_requests row %q: %v", requestID, err)
	}
	return s
}

// TestWithdrawalIdempotencyCrossKeySameGrantRejected covers T022 / V6-second-
// half: once key K1 has consumed grant G, the same caller resubmitting the same
// grant under a DIFFERENT key K2 is the auth-bound 403 — never a second receipt.
// The original K1 row must be byte-identical before and after, no row may exist
// for K2, and no `created` row beyond K1's own may appear.
func TestWithdrawalIdempotencyCrossKeySameGrantRejected(t *testing.T) {
	// Given a caller whose grant is already consumed by a first key K1.
	ctx, pool := grantSetup(t)
	const callerID = int64(7301)
	const firstKey = "idem-crosskey-1"
	const secondKey = "idem-crosskey-2"
	const authorizationID = "auth-idem-crosskey-1"
	presented := idempotencySeed(t, ctx, pool, callerID, authorizationID)

	first, err := SubmitWithdrawal(ctx, pool, intakeReq(presented, firstKey, authorizationID))
	if err != nil {
		t.Fatalf("first SubmitWithdrawal: %v", err)
	}
	if first.Status != 201 {
		t.Fatalf("first status = %d (%+v), want 201", first.Status, first)
	}
	intakeWantNoAudit(t, first)
	before := idempotencyRowSnapshotOf(t, ctx, pool, first.RequestID)

	// When the same caller reuses the consumed grant under a different key.
	second, err := SubmitWithdrawal(ctx, pool, intakeReq(presented, secondKey, authorizationID))
	if err != nil {
		t.Fatalf("cross-key SubmitWithdrawal: %v", err)
	}

	// Then it is the auth-bound 403 with no request_id (never the original's id)
	// and `auth_failed` intent attributed to the caller, not to the original row.
	if second.Status != 403 || second.Code != CodeAuthorizationInvalid {
		t.Fatalf("cross-key result = %+v, want 403/%s", second, CodeAuthorizationInvalid)
	}
	if second.RequestID != "" {
		t.Fatalf("cross-key result request_id = %q, want empty", second.RequestID)
	}
	intakeWantIntent(t, second, auditActionAuthFailed, callerID, "")

	// And persistent state proves the original K1 row is untouched: same content,
	// still exactly one K1 row, zero rows for the rejected K2, one table row.
	after := idempotencyRowSnapshotOf(t, ctx, pool, first.RequestID)
	if after != before {
		t.Fatalf("original row changed across cross-key attempt:\n before %+v\n after  %+v", before, after)
	}
	if n := idempotencyCountByKey(t, ctx, pool, callerID, secondKey); n != 0 {
		t.Fatalf("persisted rows for rejected key %q = %d, want 0", secondKey, n)
	}
	if n := idempotencyCountByKey(t, ctx, pool, callerID, firstKey); n != 1 {
		t.Fatalf("persisted rows for original key %q = %d, want 1", firstKey, n)
	}
	if n := intakeRequestCount(t, ctx, pool); n != 1 {
		t.Fatalf("total withdrawal_requests rows = %d, want 1", n)
	}
	if ids := idempotencyDistinctRequestIDs(t, ctx, pool, callerID, firstKey); len(ids) != 1 || ids[0] != first.RequestID {
		t.Fatalf("distinct persisted request_ids for %q = %v, want [%q]", firstKey, ids, first.RequestID)
	}
	// And the rejected attempt wrote no core row: the only audit is K1's in-tx
	// `created` (the auth_failed reject is response-first intent, persisted by
	// the transport, not by the core).
	intakeWantActions(t, intakeAuditActions(t, ctx, pool), []string{auditActionCreated})
}

// TestWithdrawalIdempotencyCrossCallerScoping covers T022 / caller scoping: the
// UNIQUE carrier is (caller_id, idempotency_key), so two callers submitting the
// SAME key string get two independent 201s with different request_ids, A's
// replay is unaffected by B's row, and B cannot read A's request.
func TestWithdrawalIdempotencyCrossCallerScoping(t *testing.T) {
	// Given two callers, each with its own grant, sharing one key string.
	ctx, pool := grantSetup(t)
	const callerA = int64(7302)
	const callerB = int64(7303)
	const sharedKey = "idem-shared-scope-1"
	const authorizationA = "auth-idem-scope-a"
	const authorizationB = "auth-idem-scope-b"
	presentedA := idempotencySeed(t, ctx, pool, callerA, authorizationA)
	presentedB := idempotencySeed(t, ctx, pool, callerB, authorizationB)

	// When both create under the identical key string.
	a, err := SubmitWithdrawal(ctx, pool, intakeReq(presentedA, sharedKey, authorizationA))
	if err != nil {
		t.Fatalf("caller A submit: %v", err)
	}
	b, err := SubmitWithdrawal(ctx, pool, intakeReq(presentedB, sharedKey, authorizationB))
	if err != nil {
		t.Fatalf("caller B submit: %v", err)
	}

	// Then each is an independent 201 with its own request_id and its own row.
	if a.Status != 201 || b.Status != 201 {
		t.Fatalf("statuses = %d/%d, want 201/201", a.Status, b.Status)
	}
	if a.RequestID == b.RequestID {
		t.Fatalf("cross-caller request ids collided at %q", a.RequestID)
	}
	intakeWantNoAudit(t, a)
	intakeWantNoAudit(t, b)
	if n := idempotencyCountByKey(t, ctx, pool, callerA, sharedKey); n != 1 {
		t.Fatalf("rows for caller A key %q = %d, want 1", sharedKey, n)
	}
	if n := idempotencyCountByKey(t, ctx, pool, callerB, sharedKey); n != 1 {
		t.Fatalf("rows for caller B key %q = %d, want 1", sharedKey, n)
	}
	if n := intakeRequestCount(t, ctx, pool); n != 2 {
		t.Fatalf("total withdrawal_requests rows = %d, want 2", n)
	}
	if ids := idempotencyDistinctRequestIDs(t, ctx, pool, callerA, sharedKey); len(ids) != 1 || ids[0] != a.RequestID {
		t.Fatalf("caller A distinct ids = %v, want [%q]", ids, a.RequestID)
	}
	if ids := idempotencyDistinctRequestIDs(t, ctx, pool, callerB, sharedKey); len(ids) != 1 || ids[0] != b.RequestID {
		t.Fatalf("caller B distinct ids = %v, want [%q]", ids, b.RequestID)
	}

	// And A's replay resolves to A's own row, unaffected by B's same-key row.
	replayA, err := SubmitWithdrawal(ctx, pool, intakeReq(presentedA, sharedKey, authorizationA))
	if err != nil {
		t.Fatalf("caller A replay: %v", err)
	}
	if replayA.Status != 200 || replayA.RequestID != a.RequestID {
		t.Fatalf("caller A replay = %+v, want 200 with request_id %q", replayA, a.RequestID)
	}
	intakeWantIntent(t, replayA, auditActionReplayed, callerA, a.RequestID)

	// And ownership is enforced across the shared key: B reading A's request is
	// the same not_found as a random id (no existence leakage), while A can.
	_, foreignErr := GetWithdrawal(ctx, pool, callerB, a.RequestID)
	if foreignErr == nil {
		t.Fatal("caller B read caller A's request, want not_found")
	}
	var notFound *Error
	if !errors.As(foreignErr, &notFound) || notFound.Code != CodeNotFound {
		t.Fatalf("caller B foreign read error = %v, want *Error{%s}", foreignErr, CodeNotFound)
	}
	if _, err := GetWithdrawal(ctx, pool, callerA, a.RequestID); err != nil {
		t.Fatalf("caller A owner read: %v (row must exist)", err)
	}

	// And no replay wrote a core row: exactly the two in-tx `created` rows.
	intakeWantActions(t, intakeAuditActions(t, ctx, pool), []string{auditActionCreated, auditActionCreated})
}
