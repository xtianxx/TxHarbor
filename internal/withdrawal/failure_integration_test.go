//go:build integration

// failure_integration_test.go owns spec task T026 (US5): the storage-failure and
// unknown-commit fault-injection tests for the withdrawal receipt path, against
// a real PostgreSQL 18 container (testcontainers). It covers:
//
//   - the 503 shape for a dead store (closed pool): temporary_unavailable with
//     the same-key retry instruction, never "definitely not created", never
//     "Accepted" (contracts/api.md §1-2);
//   - a held FOR UPDATE grant lock that makes the receipt FOR SHARE wait until
//     writeGuard's per-STATEMENT 5s statement_timeout fires -> the non-23505
//     branch -> retryable 503 (never a 23505 replay/conflict);
//   - a raw-SQL opposite-lock-order deadlock proving PostgreSQL emits 40P01 as
//     a class distinct from 23505;
//   - the Table 5 C3/N1 audit rules: unauthenticated rejects write ZERO rows,
//     an authenticated pre-tx reject writes exactly one best-effort row on the
//     success path, and the writer is zero-or-one (never duplicated) on a write
//     failure and on a cancelled request context.
//
// Honest boundary: the receipt transaction takes exactly ONE lock (FOR SHARE on
// the single grant row), so SubmitWithdrawal cannot deadlock against itself. The
// 40P01 case therefore drives raw SQL; it proves the error class exists and is
// distinct from 23505, while the timeout test proves the receipt path maps a
// non-23505/23514 pg error to the retryable 503 branch end to end. writeGuard is
// a per-statement timeout (SET LOCAL), NOT a transaction-total bound.
//
// It reuses the T006 container/migration helpers and the T007/T008/T010 suite
// helpers (intakeKey, intakeSupplyGrant, intakeReq, intake* counts) and adds
// only failure-prefixed helpers so no name collides.
package withdrawal

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/db"
)

// failureSetup boots a migrated scratch PostgreSQL and returns the pool plus the
// DSN (the DSN lets a test re-open a fresh reader after the original pool is
// deliberately killed).
func failureSetup(t *testing.T) (context.Context, *pgxpool.Pool, string) {
	t.Helper()
	dsn := withdrawalStartPostgres(t)
	ctx := context.Background()
	if err := db.MigrateUp(ctx, withdrawalMigrateOptions(dsn), io.Discard); err != nil {
		t.Fatalf("migrate up: %v", err)
	}
	pool, err := db.OpenPool(ctx, dsn, 5*time.Second)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return ctx, pool, dsn
}

// failureVerifyPool opens an independent reader over dsn, used to inspect the
// store after the pool under test has been closed.
func failureVerifyPool(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()
	pool, err := db.OpenPool(context.Background(), dsn, 5*time.Second)
	if err != nil {
		t.Fatalf("open verify pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// TestWithdrawalFailureStorageFailureIs503Retryable covers the storage-down
// 503 shape: with the store unreachable, the submit is a 503
// temporary_unavailable carrying the same-key retry instruction, and it never
// claims a definite non-creation or acceptance.
func TestWithdrawalFailureStorageFailureIs503Retryable(t *testing.T) {
	ctx, pool, dsn := failureSetup(t)
	key := intakeKey(t, ctx, pool, 7201)
	intakeSupplyGrant(t, ctx, pool, 7201, "auth-failure-503", intakeAmount)

	// Given: a credential and grant were issued while the store was healthy.
	// When: the store is gone (pool closed) and the same submit is attempted.
	pool.Close()
	res, err := SubmitWithdrawal(ctx, pool, intakeReq(key, "idem-failure-503", "auth-failure-503"))
	if err != nil {
		t.Fatalf("closed-pool submit = defect error %v, want a 503 outcome", err)
	}

	// Then: the contract 503 shape, and none of the forbidden claims.
	if res.Status != 503 || res.Code != CodeTemporarilyUnavailable {
		t.Fatalf("result = %+v, want 503/%s", res, CodeTemporarilyUnavailable)
	}
	if res.RequestID != "" {
		t.Fatalf("503 request_id = %q, want empty", res.RequestID)
	}
	intakeWantNoAudit(t, res) // no verifiable caller identity on the auth-failure path
	if !strings.Contains(res.Message, "retry with the same idempotency key") {
		t.Fatalf("503 message %q omits the same-key retry instruction", res.Message)
	}
	if !strings.Contains(res.Message, "never rotate the key") {
		t.Fatalf("503 message %q omits the never-rotate instruction", res.Message)
	}
	if strings.Contains(res.Message, "definitely not created") {
		t.Fatalf("503 message %q claims a definite non-creation", res.Message)
	}
	if strings.Contains(strings.ToLower(res.Message), "accepted") {
		t.Fatalf("503 message %q claims acceptance", res.Message)
	}

	// Then: a fresh reader confirms nothing was persisted.
	verify := failureVerifyPool(t, dsn)
	if n := intakeRequestCount(t, ctx, verify); n != 0 {
		t.Fatalf("request rows after 503 = %d, want 0", n)
	}
	if n := intakeAuditCount(t, ctx, verify); n != 0 {
		t.Fatalf("audit rows after 503 = %d, want 0", n)
	}
}

// TestWithdrawalFailureGrantLockTimeoutIs503NotUniqueViolation covers the
// deadlock/timeout -> 503 boundary: a held FOR UPDATE on the single grant row
// makes the receipt FOR SHARE wait until writeGuard's 5s statement_timeout
// fires, which MUST map to the retryable 503, never to a 23505 replay/conflict.
func TestWithdrawalFailureGrantLockTimeoutIs503NotUniqueViolation(t *testing.T) {
	ctx, pool, _ := failureSetup(t)
	key := intakeKey(t, ctx, pool, 7202)
	intakeSupplyGrant(t, ctx, pool, 7202, "auth-failure-lock", intakeAmount)

	// Given: another session holds FOR UPDATE on that grant row.
	blocker, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin blocker tx: %v", err)
	}
	defer func() { _ = blocker.Rollback(context.Background()) }()
	if _, err := blocker.Exec(ctx,
		`SELECT 1 FROM withdrawal_authorizations WHERE authorization_id = $1 FOR UPDATE`,
		"auth-failure-lock"); err != nil {
		t.Fatalf("blocker FOR UPDATE: %v", err)
	}

	// When: the receipt path waits on FOR SHARE until the statement timeout.
	start := time.Now()
	res, err := SubmitWithdrawal(ctx, pool, intakeReq(key, "idem-failure-lock", "auth-failure-lock"))
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("lock-wait submit = defect error %v, want a 503 outcome", err)
	}

	// Then: a retryable 503 with the unavailable intent, never a 23505 outcome.
	if res.Status != 503 || res.Code != CodeTemporarilyUnavailable {
		t.Fatalf("result = %+v, want 503/%s", res, CodeTemporarilyUnavailable)
	}
	intakeWantIntent(t, res, auditActionUnavailable, 7202, "")
	if !strings.Contains(res.Message, "retry with the same idempotency key") {
		t.Fatalf("503 message %q omits the same-key retry instruction", res.Message)
	}
	if elapsed < 4*time.Second {
		t.Fatalf("lock wait returned in %s; the blocker was held, so writeGuard's per-statement 5s timeout did not fire", elapsed)
	}

	// Then: the aborted receipt tx rolled back with no rows.
	if n := intakeRequestCount(t, ctx, pool); n != 0 {
		t.Fatalf("request rows after timeout = %d, want 0", n)
	}
	if n := intakeAuditCount(t, ctx, pool); n != 0 {
		t.Fatalf("audit rows after timeout = %d, want 0", n)
	}
}

// TestWithdrawalFailureDeadlockSQLStateDistinctFromUnique drives a real
// opposite-lock-order deadlock in raw SQL and asserts PostgreSQL reports 40P01
// (a class distinct from the 23505 UNIQUE class the receipt path classifies),
// with no constraint name.
func TestWithdrawalFailureDeadlockSQLStateDistinctFromUnique(t *testing.T) {
	ctx, pool, _ := failureSetup(t)
	_ = intakeKey(t, ctx, pool, 7203) // create the caller row for the grants
	intakeSupplyGrant(t, ctx, pool, 7203, "auth-deadlock-a", intakeAmount)
	intakeSupplyGrant(t, ctx, pool, 7203, "auth-deadlock-b", intakeAmount)

	const lockSQL = `SELECT 1 FROM withdrawal_authorizations WHERE authorization_id = $1 FOR UPDATE`
	bg := context.Background()

	txA, err := pool.Begin(bg)
	if err != nil {
		t.Fatalf("begin tx A: %v", err)
	}
	defer func() { _ = txA.Rollback(bg) }()
	txB, err := pool.Begin(bg)
	if err != nil {
		t.Fatalf("begin tx B: %v", err)
	}
	defer func() { _ = txB.Rollback(bg) }()

	// Given: T1 holds A and T2 holds B — no cycle yet.
	if _, err := txA.Exec(bg, lockSQL, "auth-deadlock-a"); err != nil {
		t.Fatalf("T1 lock A: %v", err)
	}
	if _, err := txB.Exec(bg, lockSQL, "auth-deadlock-b"); err != nil {
		t.Fatalf("T2 lock B: %v", err)
	}

	// When: each requests the other's row concurrently.
	errCh := make(chan error, 2)
	go func() { _, e := txA.Exec(bg, lockSQL, "auth-deadlock-b"); errCh <- e }()
	go func() { _, e := txB.Exec(bg, lockSQL, "auth-deadlock-a"); errCh <- e }()
	first, second := <-errCh, <-errCh

	// Then: exactly one victim is aborted with 40P01, never 23505.
	deadlocks := 0
	for _, e := range []error{first, second} {
		if e == nil {
			continue
		}
		var pgErr *pgconn.PgError
		if !errors.As(e, &pgErr) {
			t.Fatalf("deadlock probe error %v is not a *pgconn.PgError", e)
		}
		if pgErr.Code == "23505" {
			t.Fatalf("deadlock probe produced 23505 (%s), want a distinct class", pgErr.ConstraintName)
		}
		if pgErr.Code == "40P01" {
			deadlocks++
			if pgErr.ConstraintName != "" {
				t.Fatalf("deadlock ConstraintName = %q, want empty", pgErr.ConstraintName)
			}
		}
	}
	if deadlocks != 1 {
		t.Fatalf("deadlock victims = %d, want exactly 1 (errors: %v, %v)", deadlocks, first, second)
	}
}

// TestWithdrawalFailureUnauthenticatedWritesZeroAuditRows covers the C3/N1 401
// rule: a key matching no credential writes no Table 5 row, across repeated
// unauthenticated submits.
func TestWithdrawalFailureUnauthenticatedWritesZeroAuditRows(t *testing.T) {
	ctx, pool, _ := failureSetup(t)
	unknown := keyPrefix + base64.RawURLEncoding.EncodeToString(make([]byte, keyEntropyBytes))

	// When: three unauthenticated submits fire.
	for i := 0; i < 3; i++ {
		res, err := SubmitWithdrawal(ctx, pool,
			intakeReq(unknown, fmt.Sprintf("idem-failure-401-%d", i), "auth-failure-401"))
		if err != nil {
			t.Fatalf("unauthenticated submit %d: %v", i, err)
		}
		if res.Status != 401 || res.Code != CodeUnauthenticated {
			t.Fatalf("result %d = %+v, want 401/%s", i, res, CodeUnauthenticated)
		}
		intakeWantNoAudit(t, res)
	}

	// Then: Table 5 (and the request table) stay empty — no caller, no row.
	if n := intakeAuditCount(t, ctx, pool); n != 0 {
		t.Fatalf("audit rows after 401s = %d, want 0", n)
	}
	if n := intakeRequestCount(t, ctx, pool); n != 0 {
		t.Fatalf("request rows after 401s = %d, want 0", n)
	}
}

// TestWithdrawalFailurePreTxRejectAuditExactlyOne covers the C3 success path:
// an authenticated pre-tx reject returns exactly one audit intent, and
// persisting it writes exactly one best-effort row under a rej- marker.
func TestWithdrawalFailurePreTxRejectAuditExactlyOne(t *testing.T) {
	ctx, pool, _ := failureSetup(t)
	key := intakeKey(t, ctx, pool, 7205)
	intakeSupplyGrant(t, ctx, pool, 7205, "auth-failure-reject", intakeAmount)

	// Given: an authenticated but invalid submit (amount shape).
	req := intakeReq(key, "idem-failure-reject", "auth-failure-reject")
	req.Amount = "1.5"
	res, err := SubmitWithdrawal(ctx, pool, req)
	if err != nil {
		t.Fatalf("invalid submit: %v", err)
	}
	if res.Status != 422 || res.Code != CodeValidationFailed {
		t.Fatalf("result = %+v, want 422/%s", res, CodeValidationFailed)
	}
	intakeWantIntent(t, res, auditActionRejected, 7205, "")

	// When: the transport persists the returned intent once.
	if err := WriteRejectAudit(ctx, pool, res.Audit.CallerID, res.Audit.RequestID, res.Audit.Action, res.Audit.Detail); err != nil {
		t.Fatalf("WriteRejectAudit: %v", err)
	}

	// Then: exactly one row, the `rejected` action, and a rej- marker id.
	if n := intakeAuditCount(t, ctx, pool); n != 1 {
		t.Fatalf("audit rows after reject = %d, want exactly 1", n)
	}
	intakeWantActions(t, intakeAuditActions(t, ctx, pool), []string{auditActionRejected})
	var storedID string
	if err := pool.QueryRow(ctx, `SELECT request_id FROM withdrawal_request_audit`).Scan(&storedID); err != nil {
		t.Fatalf("read reject audit request_id: %v", err)
	}
	if !strings.HasPrefix(storedID, intakeRejectMarkerPrefix) {
		t.Fatalf("reject audit request_id = %q, want a %q marker", storedID, intakeRejectMarkerPrefix)
	}
	if strings.HasPrefix(storedID, intakeRequestIDPrefix) {
		t.Fatalf("reject audit minted a fabricated %q identity", storedID)
	}
}

// TestWithdrawalFailureRejectAuditWriteFailureAndCancel covers the N1 failure
// boundary of the detached writer: a write failure is reported but leaves zero
// rows (single attempt, no retry), and a cancelled request context is ignored
// so the write is still attempted exactly once.
func TestWithdrawalFailureRejectAuditWriteFailureAndCancel(t *testing.T) {
	t.Run("write failure leaves zero rows", func(t *testing.T) {
		ctx, pool, dsn := failureSetup(t)
		_ = intakeKey(t, ctx, pool, 7206) // caller exists; store was reachable

		// When: the store is gone and a best-effort write is attempted once.
		pool.Close()
		err := WriteRejectAudit(ctx, pool, 7206, "", auditActionRejected, "write failure probe")

		// Then: the writer reports the failure for observability...
		var typed *Error
		if !errors.As(err, &typed) || typed.Code != CodeTemporarilyUnavailable {
			t.Fatalf("WriteRejectAudit on closed pool = %v, want a %s error", err, CodeTemporarilyUnavailable)
		}
		// ...with zero rows left behind (no retry, no queue).
		verify := failureVerifyPool(t, dsn)
		if n := intakeAuditCount(t, ctx, verify); n != 0 {
			t.Fatalf("audit rows after write failure = %d, want 0", n)
		}
	})

	t.Run("cancelled request context still attempts once", func(t *testing.T) {
		ctx, pool, _ := failureSetup(t)
		_ = intakeKey(t, ctx, pool, 7207)

		// Given: the REQUEST context is already cancelled.
		cancelled, cancel := context.WithCancel(context.Background())
		cancel()

		// When: the detached writer runs with that cancelled context.
		if err := WriteRejectAudit(cancelled, pool, 7207, "", auditActionRejected, "cancelled ctx probe"); err != nil {
			t.Fatalf("WriteRejectAudit with cancelled request ctx = %v, want a detached success", err)
		}

		// Then: exactly one row — attempted once, never duplicated.
		if n := intakeAuditCount(t, ctx, pool); n != 1 {
			t.Fatalf("audit rows after cancelled-ctx attempt = %d, want exactly 1", n)
		}
	})
}
