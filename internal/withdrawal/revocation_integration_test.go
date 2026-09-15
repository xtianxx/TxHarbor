//go:build integration

// revocation_integration_test.go owns the T025 [US5] revocation-interleave and
// grant-expiry tests against a real PostgreSQL 18 container (testcontainers).
//
// It proves the R7 three-case interleave between the receipt transaction
// (SubmitWithdrawal: FOR SHARE on the one grant row → post-lock
// clock_timestamp() → INSERT → COMMIT) and the revoke transaction (RevokeGrant:
// FOR UPDATE/UPDATE on that same row) with CONTROLLED interleaving — lock-queue
// barriers and database-observed wait states, never sleeps used as thread
// synchronization. The one sleep in this file models elapsed wall time for the
// expiry path (expires_at must actually pass), which is a time domain, not a
// thread-sync barrier.
//
// It also proves the R8 grant-expiry clock protocol: validity is decided by a
// post-lock `SELECT clock_timestamp()` (never now()/transaction_timestamp(),
// which are fixed at tx start), the strict `expires_at > t_check` boundary
// (`expires_at == t_check` is expired), and the lock-wait expiry counter-example
// (tx-start-valid → lock wait → expiry passes mid-wait → 403, zero rows).
//
// It reuses the T006 container/migration helper (grantSetup), the T008 grant
// helpers (grantTestOp, SupplyGrant, RevokeGrant), and the T010 intake helpers
// (intakeKey, intakeSupplyGrant, intakeReq, intakeRequestCount) and adds only
// revocation-prefixed helpers, so no name collides with any existing test file.
package withdrawal

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// revocationWaitGrantLockWaiters is a database-observed barrier: it blocks until
// at least `want` backends in this isolated database are waiting on a lock. It
// observes real lock-manager state (pg_stat_activity), never a fixed delay, so
// ordering between a receipt's FOR SHARE and a revoke's FOR UPDATE is
// established by the database, not by lucky scheduling. A blocked row lock
// waits on the holder's transaction id (pg_locks relation is 0), so the session
// wait state is the scoped, reliable signal.
func revocationWaitGrantLockWaiters(t *testing.T, ctx context.Context, pool *pgxpool.Pool, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var waiting int
		if err := pool.QueryRow(ctx, `
SELECT count(*)
FROM pg_stat_activity
WHERE wait_event_type = 'Lock' AND datname = current_database()`).Scan(&waiting); err != nil {
			t.Fatalf("count waiting sessions: %v", err)
		}
		if waiting >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d queued grant-row lock(s), observed %d", want, waiting)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// revocationBlockGrant opens the transaction that owns the grant row FOR UPDATE
// and returns it; the caller releases it to control the lock queue. It is the
// deterministic holder that lets a receipt's FOR SHARE queue behind it.
func revocationBlockGrant(t *testing.T, ctx context.Context, pool *pgxpool.Pool, authorizationID string) pgx.Tx {
	t.Helper()
	blocker, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin blocker tx: %v", err)
	}
	var one int
	if err := blocker.QueryRow(ctx,
		`SELECT 1 FROM withdrawal_authorizations WHERE authorization_id = $1 FOR UPDATE`,
		authorizationID).Scan(&one); err != nil {
		_ = blocker.Rollback(ctx)
		t.Fatalf("blocker lock grant %q: %v", authorizationID, err)
	}
	// Always release the row lock, even if the test fails before its explicit
	// release, so a queued receipt/revoke cannot deadlock pool.Close.
	t.Cleanup(func() { _ = blocker.Rollback(ctx) })
	return blocker
}

// revocationRelease releases the blocker transaction, unblocking the queued
// receipt/revoke in lock-queue order.
func revocationRelease(t *testing.T, ctx context.Context, blocker pgx.Tx) {
	t.Helper()
	if err := blocker.Rollback(ctx); err != nil {
		t.Fatalf("release blocker tx: %v", err)
	}
}

// revocationSeedGrantWithExpiry inserts one active grant directly, bound to the
// canonical request parameters, with an explicit expires_at (DB clock relative).
func revocationSeedGrantWithExpiry(t *testing.T, ctx context.Context, pool *pgxpool.Pool, callerID int64, authorizationID, amount string, expiresAt time.Time) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
INSERT INTO withdrawal_authorizations
    (authorization_id, caller_id, chain_id, asset, recipient, amount, state, expires_at, supplied_by)
VALUES ($1, $2, $3, $4, $5, $6::numeric, 'active', $7, 'seed')`,
		authorizationID, callerID, intakeChainID, intakeAsset, intakeRecipient, amount, expiresAt); err != nil {
		t.Fatalf("seed expiring grant %q: %v", authorizationID, err)
	}
}

// revocationWantRejected asserts a 403 authorization_invalid outcome with no
// request identity.
func revocationWantRejected(t *testing.T, res *SubmitResult) {
	t.Helper()
	if res.Status != 403 || res.Code != CodeAuthorizationInvalid || res.RequestID != "" {
		t.Fatalf("result = %+v, want 403/%s with no request_id", res, CodeAuthorizationInvalid)
	}
}

// TestWithdrawalRevocationCaseARevokeCommitsBeforeReceipt covers R7 Case A: the
// revoke commits BEFORE the receipt takes its grant lock, so the post-lock read
// observes `revoked` → 403 and zero request rows. Revocation is effective for a
// receipt that has not yet linearized on the grant row.
func TestWithdrawalRevocationCaseARevokeCommitsBeforeReceipt(t *testing.T) {
	// Given an active grant for a caller.
	ctx, pool := grantSetup(t)
	const callerID = int64(7201)
	key := intakeKey(t, ctx, pool, callerID)
	intakeSupplyGrant(t, ctx, pool, callerID, "auth-rev-a", intakeAmount)

	// When the revoke commits first (the returned outcome is the ordering
	// barrier: V has committed before any receipt lock request exists)...
	revoked, err := RevokeGrant(ctx, pool, "revoke-a", "auth-rev-a", "revocation-test", "revoke before receipt")
	if err != nil {
		t.Fatalf("RevokeGrant: %v", err)
	}
	if revoked.Action != grantOutcomeRevoked {
		t.Fatalf("revoke outcome = %+v, want %s", revoked, grantOutcomeRevoked)
	}

	// ...and only then the receipt runs.
	res, err := SubmitWithdrawal(ctx, pool, intakeReq(key, "idem-rev-a", "auth-rev-a"))
	if err != nil {
		t.Fatalf("SubmitWithdrawal after revoke: %v", err)
	}

	// Then it is 403 with zero request rows, and the grant stays revoked.
	revocationWantRejected(t, res)
	if n := intakeRequestCount(t, ctx, pool); n != 0 {
		t.Fatalf("request rows after Case A = %d, want 0", n)
	}
	if state, _ := grantStateAndAmount(t, ctx, pool, "auth-rev-a"); state != "revoked" {
		t.Fatalf("grant state = %q, want revoked", state)
	}
}

// TestWithdrawalRevocationCaseBReceiptCommitsBeforeRevoke covers R7 Case B: the
// receipt commits BEFORE the revoke's lock, so the Accepted row stands, the
// revoke applies only to later receipts (a new key against the same grant is
// 403), and the original key replays as 200 — never an implicit cancel (Q2/Q5).
func TestWithdrawalRevocationCaseBReceiptCommitsBeforeRevoke(t *testing.T) {
	// Given an active grant for a caller.
	ctx, pool := grantSetup(t)
	const callerID = int64(7202)
	key := intakeKey(t, ctx, pool, callerID)
	const authID = "auth-rev-b"
	intakeSupplyGrant(t, ctx, pool, callerID, authID, intakeAmount)

	// When the receipt commits first (R-COMMIT precedes V-lock by construction).
	first, err := SubmitWithdrawal(ctx, pool, intakeReq(key, "idem-rev-b", authID))
	if err != nil {
		t.Fatalf("SubmitWithdrawal: %v", err)
	}
	if first.Status != 201 {
		t.Fatalf("first status = %d (%+v), want 201", first.Status, first)
	}
	revoked, err := RevokeGrant(ctx, pool, "revoke-b", authID, "revocation-test", "revoke after receipt")
	if err != nil {
		t.Fatalf("RevokeGrant after receipt: %v", err)
	}
	if revoked.Action != grantOutcomeRevoked {
		t.Fatalf("revoke outcome = %+v, want %s", revoked, grantOutcomeRevoked)
	}

	// Then the Accepted row stands, a same-key replay is still 200 with the
	// original id, and the revoke blocks only a NEW receipt against the grant.
	replay, err := SubmitWithdrawal(ctx, pool, intakeReq(key, "idem-rev-b", authID))
	if err != nil {
		t.Fatalf("replay after revoke: %v", err)
	}
	if replay.Status != 200 || replay.RequestID != first.RequestID {
		t.Fatalf("replay = %+v, want 200 with request_id %q (no implicit cancel)", replay, first.RequestID)
	}

	later, err := SubmitWithdrawal(ctx, pool, intakeReq(key, "idem-rev-b-later", authID))
	if err != nil {
		t.Fatalf("later-receipt submit: %v", err)
	}
	revocationWantRejected(t, later)

	if n := intakeRequestCount(t, ctx, pool); n != 1 {
		t.Fatalf("request rows after Case B = %d, want 1 (Accepted stands, no second request)", n)
	}
	if state, _ := grantStateAndAmount(t, ctx, pool, authID); state != "revoked" {
		t.Fatalf("grant state = %q, want revoked", state)
	}
}

// TestWithdrawalRevocationCaseCRevokeWaitsOnReceiptShareLock covers R7 Case C:
// the receipt holds the FOR SHARE lock and the revoke queues behind it. The
// interleave is forced with a database-observed lock-queue barrier — a blocker
// transaction owns the row FOR UPDATE, the receipt queues on FOR SHARE first,
// the revoke queues on FOR UPDATE second, then the blocker releases and the
// lock manager grants in FIFO order. The outcome is deterministically
// Case-B-equivalent: no second request for the grant, no implicit cancel.
func TestWithdrawalRevocationCaseCRevokeWaitsOnReceiptShareLock(t *testing.T) {
	// Given an active grant and a blocker holding it FOR UPDATE.
	ctx, pool := grantSetup(t)
	const callerID = int64(7203)
	key := intakeKey(t, ctx, pool, callerID)
	const authID = "auth-rev-c"
	intakeSupplyGrant(t, ctx, pool, callerID, authID, intakeAmount)
	blocker := revocationBlockGrant(t, ctx, pool, authID)

	// When the receipt's FOR SHARE is queued behind the blocker...
	receipt := make(chan *SubmitResult, 1)
	go func() {
		res, err := SubmitWithdrawal(ctx, pool, intakeReq(key, "idem-rev-c", authID))
		if err != nil {
			res = &SubmitResult{Status: -1, Message: "error: " + err.Error()}
		}
		receipt <- res
	}()
	revocationWaitGrantLockWaiters(t, ctx, pool, 1)

	// ...and the revoke then queues behind the receipt's share lock.
	revokeOut := make(chan *GrantOutcome, 1)
	revokeErr := make(chan error, 1)
	go func() {
		out, err := RevokeGrant(ctx, pool, "revoke-c", authID, "revocation-test", "wait on receipt share lock")
		revokeOut <- out
		revokeErr <- err
	}()
	revocationWaitGrantLockWaiters(t, ctx, pool, 2)

	// Release the blocker: the receipt is granted its share lock first (FIFO),
	// commits, and only then the revoke acquires the row.
	revocationRelease(t, ctx, blocker)

	// Then the receipt is Accepted, the revoke lands after it, there is exactly
	// one request row, and the replay stands while the grant is revoked.
	res := <-receipt
	out := <-revokeOut
	if err := <-revokeErr; err != nil {
		t.Fatalf("Case C revoke: %v", err)
	}
	if res.Status != 201 {
		t.Fatalf("Case C receipt = %+v, want 201 (receipt linearized before revoke)", res)
	}
	if out.Action != grantOutcomeRevoked {
		t.Fatalf("Case C revoke outcome = %+v, want %s", out, grantOutcomeRevoked)
	}
	if n := intakeRequestCount(t, ctx, pool); n != 1 {
		t.Fatalf("request rows after Case C = %d, want 1 (no second request)", n)
	}
	if state, _ := grantStateAndAmount(t, ctx, pool, authID); state != "revoked" {
		t.Fatalf("grant state = %q, want revoked", state)
	}
	replay, err := SubmitWithdrawal(ctx, pool, intakeReq(key, "idem-rev-c", authID))
	if err != nil {
		t.Fatalf("Case C replay: %v", err)
	}
	if replay.Status != 200 || replay.RequestID != res.RequestID {
		t.Fatalf("Case C replay = %+v, want 200 with request_id %q (no implicit cancel)", replay, res.RequestID)
	}
}

// TestWithdrawalRevocationLockWaitExpiry covers the R8 counter-example: a grant
// valid at transaction start whose expires_at passes WHILE the receipt waits for
// its grant lock. The wait is forced deterministically (blocker owns the row,
// receipt queues on FOR SHARE), the elapsed wall time is modeled by one sleep —
// the sleep is the expiry mechanism, not thread synchronization — and the
// post-lock `clock_timestamp()` then reads past expires_at → 403, zero rows.
func TestWithdrawalRevocationLockWaitExpiry(t *testing.T) {
	// Given an active grant that expires one second from now (DB clock).
	ctx, pool := grantSetup(t)
	const callerID = int64(7204)
	key := intakeKey(t, ctx, pool, callerID)
	const authID = "auth-rev-expiry"
	var expiresAt time.Time
	if err := pool.QueryRow(ctx, `SELECT clock_timestamp() + interval '1 second'`).Scan(&expiresAt); err != nil {
		t.Fatalf("compute expires_at: %v", err)
	}
	revocationSeedGrantWithExpiry(t, ctx, pool, callerID, authID, intakeAmount, expiresAt)

	// When the receipt queues on the grant lock behind a blocker, wall time
	// passes beyond expires_at, and the blocker releases so the lock is granted.
	blocker := revocationBlockGrant(t, ctx, pool, authID)
	receipt := make(chan *SubmitResult, 1)
	go func() {
		res, err := SubmitWithdrawal(ctx, pool, intakeReq(key, "idem-rev-expiry", authID))
		if err != nil {
			res = &SubmitResult{Status: -1, Message: "error: " + err.Error()}
		}
		receipt <- res
	}()
	revocationWaitGrantLockWaiters(t, ctx, pool, 1)
	time.Sleep(1500 * time.Millisecond) // models time passing past expires_at
	revocationRelease(t, ctx, blocker)
	res := <-receipt

	// Then the post-lock clock reads expired → 403 with zero request rows.
	revocationWantRejected(t, res)
	if n := intakeRequestCount(t, ctx, pool); n != 0 {
		t.Fatalf("request rows after lock-wait expiry = %d, want 0", n)
	}
}

// TestWithdrawalRevocationExpiryBoundary locks the strict `expires_at > t_check`
// rule (`==` is expired) two ways. The precise boundary is asserted directly on
// the pure decision function. The database path cannot hit exact equality
// reliably — any elapsed time makes it past — so it is exercised with a
// DB-computed expires_at captured from `clock_timestamp()`: by the time the
// receipt's post-lock clock reads, time has moved strictly past it, which is the
// `equality-or-past ⇒ expired` branch that the strict `>` guarantees.
func TestWithdrawalRevocationExpiryBoundary(t *testing.T) {
	// Given a well-formed active grant bound to the canonical request params.
	const callerID = int64(7205)
	t0 := time.Now().UTC().Truncate(time.Microsecond)
	p := submitParams{
		chainID:         intakeChainID,
		asset:           intakeAsset,
		recipient:       intakeRecipient,
		amount:          intakeAmount,
		authorizationID: "auth-boundary",
	}
	base := grantRow{
		callerID:  callerID,
		chainID:   intakeChainID,
		asset:     intakeAsset,
		recipient: intakeRecipient,
		amount:    intakeAmount,
		state:     "active",
		expiresAt: &t0,
	}

	// Then the decision function treats equality as expired and only a strictly
	// later expiry as valid.
	for _, tc := range []struct {
		name   string
		g      grantRow
		tCheck time.Time
		want   bool
	}{
		{"expires_at == t_check is expired", base, t0, false},
		{"expires_at one microsecond after t_check is valid", base, t0.Add(-time.Microsecond), true},
		{"expires_at one microsecond before t_check is expired", base, t0.Add(time.Microsecond), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := intakeGrantValid(tc.g, callerID, p, tc.tCheck); got != tc.want {
				t.Fatalf("intakeGrantValid = %v, want %v", got, tc.want)
			}
		})
	}
	// A NULL expiry never expires, and a non-active state never validates.
	noExpiry := base
	noExpiry.expiresAt = nil
	if !intakeGrantValid(noExpiry, callerID, p, t0) {
		t.Fatal("NULL expires_at rejected, want valid")
	}
	revoked := base
	revoked.state = "revoked"
	if intakeGrantValid(revoked, callerID, p, t0) {
		t.Fatal("revoked grant validated, want expired")
	}

	// And on the real database path: expires_at is captured from the DB clock
	// and the receipt reads a strictly later clock → 403, zero rows.
	ctx, pool := grantSetup(t)
	key := intakeKey(t, ctx, pool, callerID)
	var dbNow time.Time
	if err := pool.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&dbNow); err != nil {
		t.Fatalf("capture DB clock: %v", err)
	}
	revocationSeedGrantWithExpiry(t, ctx, pool, callerID, "auth-eq", intakeAmount, dbNow)

	res, err := SubmitWithdrawal(ctx, pool, intakeReq(key, "idem-rev-eq", "auth-eq"))
	if err != nil {
		t.Fatalf("SubmitWithdrawal at expiry boundary: %v", err)
	}
	revocationWantRejected(t, res)
	if n := intakeRequestCount(t, ctx, pool); n != 0 {
		t.Fatalf("request rows at expiry boundary = %d, want 0", n)
	}
}
