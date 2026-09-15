//go:build integration

// auth_integration_test.go owns the T007 credential core's real-PostgreSQL
// verification: issuance, authentication, rotation grace dual-accept,
// revocation, permission carriage, cross-caller isolation, and the
// full-uniqueness guarantee that a revoked digest can never be re-registered.
//
// Revocation is time-based (revoked_at vs now()), so every assertion here runs
// against a real server; the container harness is the T006 one. Each test boots
// its own isolated scratch database and terminates it in t.Cleanup.
package withdrawal

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/db"
)

// withdrawalAuthPool boots one migrated scratch database and returns a pgx pool
// over it. It reuses the T006 container helper (withdrawalStartPostgres) and
// terminates the container after the pool is closed.
func withdrawalAuthPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := withdrawalStartPostgres(t)
	ctx := context.Background()
	if err := db.MigrateUp(ctx, db.MigrateOptions{
		DSN:            dsn,
		LockTimeout:    5 * time.Second,
		ConnectTimeout: 5 * time.Second,
	}, io.Discard); err != nil {
		t.Fatalf("migrate up: %v", err)
	}
	pool, err := db.OpenPool(ctx, dsn, 5*time.Second)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// withdrawalAuthWantCode asserts err is a *Error carrying exactly code.
func withdrawalAuthWantCode(t *testing.T, err error, code Code) {
	t.Helper()
	var typed *Error
	if !errors.As(err, &typed) {
		t.Fatalf("error = %v (%T), want *Error with code %q", err, err, code)
	}
	if typed.Code != code {
		t.Fatalf("error code = %q, want %q (error %v)", typed.Code, code, err)
	}
}

// withdrawalAuthCallerExists reports whether the stable caller row is present.
func withdrawalAuthCallerExists(t *testing.T, ctx context.Context, pool *pgxpool.Pool, callerID int64) bool {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM caller WHERE caller_id = $1`, callerID).Scan(&n); err != nil {
		t.Fatalf("count caller %d: %v", callerID, err)
	}
	return n == 1
}

// TestWithdrawalAuthIssueAuthenticate covers the baseline issue-or-reuse flow:
// every key byte is inside the returned plaintext/ref, the caller row is
// created on first issue, authentication derives the caller identity, and
// malformed input is rejected generically.
func TestWithdrawalAuthIssueAuthenticate(t *testing.T) {
	pool := withdrawalAuthPool(t)
	ctx := context.Background()

	plaintext, ref, err := IssueKey(ctx, pool, 42, "issuer-label")
	if err != nil {
		t.Fatalf("IssueKey() error = %v", err)
	}
	if ref.CallerID != 42 || ref.KeyID <= 0 {
		t.Fatalf("IssueKey() ref = %+v, want caller_id=42 and key_id>0", ref)
	}
	if ref.Prefix != plaintext[:8] {
		t.Fatalf("IssueKey() prefix = %q, want %q", ref.Prefix, plaintext[:8])
	}
	if !withdrawalAuthCallerExists(t, ctx, pool, 42) {
		t.Fatal("IssueKey() did not create the caller row")
	}

	result, err := Authenticate(ctx, pool, plaintext)
	if err != nil {
		t.Fatalf("Authenticate() error = %v", err)
	}
	if result.Caller.ID != 42 || result.Caller.Label != "issuer-label" || !result.Caller.CanCreate {
		t.Fatalf("Authenticate() caller = %+v, want id=42 label=issuer-label can_create=true", result.Caller)
	}
	if result.Key != ref {
		t.Fatalf("Authenticate() key = %+v, want issued ref %+v", result.Key, ref)
	}

	_, authErr := Authenticate(ctx, pool, "garbage")
	withdrawalAuthWantCode(t, authErr, CodeUnauthenticated)
	_, authErr = Authenticate(ctx, pool, "")
	withdrawalAuthWantCode(t, authErr, CodeUnauthenticated)

	// A fresh caller id is created by issue-or-reuse (no pre-seeded row).
	freshPlaintext, freshRef, err := IssueKey(ctx, pool, 9001, "fresh")
	if err != nil {
		t.Fatalf("IssueKey(fresh caller) error = %v", err)
	}
	if freshRef.CallerID != 9001 {
		t.Fatalf("fresh IssueKey() caller_id = %d, want 9001", freshRef.CallerID)
	}
	if !withdrawalAuthCallerExists(t, ctx, pool, 9001) {
		t.Fatal("IssueKey() did not create the fresh caller row")
	}
	freshResult, err := Authenticate(ctx, pool, freshPlaintext)
	if err != nil {
		t.Fatalf("Authenticate(fresh) error = %v", err)
	}
	if freshResult.Caller.ID != 9001 || freshResult.Caller.Label != "fresh" || !freshResult.Caller.CanCreate {
		t.Fatalf("fresh Authenticate() caller = %+v, want id=9001 label=fresh can_create=true", freshResult.Caller)
	}
}

// TestWithdrawalAuthRotationGracePreservesIdentity covers rotation with a
// positive grace window: the predecessor stays accepted (dual-accept), the
// successor is accepted, caller_id and scope are unchanged, and RevokeKey
// touches only currently active credentials.
func TestWithdrawalAuthRotationGracePreservesIdentity(t *testing.T) {
	pool := withdrawalAuthPool(t)
	ctx := context.Background()

	oldPlaintext, oldRef, err := IssueKey(ctx, pool, 7, "rotate")
	if err != nil {
		t.Fatalf("IssueKey() error = %v", err)
	}
	newPlaintext, newRef, err := RotateKey(ctx, pool, 7, 60)
	if err != nil {
		t.Fatalf("RotateKey() error = %v", err)
	}
	if newPlaintext == oldPlaintext {
		t.Fatal("RotateKey() returned the predecessor plaintext")
	}
	if newRef.CallerID != oldRef.CallerID || newRef.CallerID != 7 {
		t.Fatalf("RotateKey() caller_id = %d, want %d (rotation must preserve identity)", newRef.CallerID, oldRef.CallerID)
	}
	if newRef.KeyID == oldRef.KeyID {
		t.Fatal("RotateKey() reused the predecessor key_id")
	}

	// Dual-accept: both credentials resolve to the same caller/scope.
	oldResult, err := Authenticate(ctx, pool, oldPlaintext)
	if err != nil {
		t.Fatalf("Authenticate(predecessor within grace) error = %v", err)
	}
	newResult, err := Authenticate(ctx, pool, newPlaintext)
	if err != nil {
		t.Fatalf("Authenticate(successor) error = %v", err)
	}
	if oldResult.Key.KeyID != oldRef.KeyID || newResult.Key.KeyID != newRef.KeyID {
		t.Fatalf("authenticated key ids = %d/%d, want %d/%d",
			oldResult.Key.KeyID, newResult.Key.KeyID, oldRef.KeyID, newRef.KeyID)
	}
	for _, r := range []*AuthResult{oldResult, newResult} {
		if r.Caller.ID != 7 || r.Caller.Label != "rotate" || !r.Caller.CanCreate {
			t.Fatalf("rotated Authenticate() caller = %+v, want id=7 label=rotate can_create=true", r.Caller)
		}
	}

	// An explicit revoke terminates a still-valid graced predecessor: the old
	// key is 401 on the next authentication while the successor keeps working
	// under the same caller identity. The revoke always wins over a grace stamp.
	if err := RevokeKey(ctx, pool, oldRef.KeyID); err != nil {
		t.Fatalf("RevokeKey(predecessor in grace) error = %v, want nil", err)
	}
	_, err = Authenticate(ctx, pool, oldPlaintext)
	withdrawalAuthWantCode(t, err, CodeUnauthenticated)
	if _, err := Authenticate(ctx, pool, newPlaintext); err != nil {
		t.Fatalf("Authenticate(successor) after predecessor revoke error = %v", err)
	}
	// Revoking an already-terminated credential reports not-found.
	withdrawalAuthWantCode(t, RevokeKey(ctx, pool, oldRef.KeyID), CodeNotFound)

	// Revoking the active successor takes effect on the next authentication.
	if err := RevokeKey(ctx, pool, newRef.KeyID); err != nil {
		t.Fatalf("RevokeKey(successor) error = %v", err)
	}
	_, err = Authenticate(ctx, pool, newPlaintext)
	withdrawalAuthWantCode(t, err, CodeUnauthenticated)
	if again := RevokeKey(ctx, pool, newRef.KeyID); again == nil {
		t.Fatal("second RevokeKey(successor) = nil, want not-found")
	} else {
		withdrawalAuthWantCode(t, again, CodeNotFound)
	}

	// Revocation never deletes the caller row or its scope.
	if !withdrawalAuthCallerExists(t, ctx, pool, 7) {
		t.Fatal("caller row missing after revocation")
	}
}

// TestWithdrawalAuthImmediateRotationRevokesOldKey proves graceSeconds <= 0
// stamps the predecessor at now(), so the old credential is 401 immediately
// while the successor still works.
func TestWithdrawalAuthImmediateRotationRevokesOldKey(t *testing.T) {
	pool := withdrawalAuthPool(t)
	ctx := context.Background()

	oldPlaintext, _, err := IssueKey(ctx, pool, 8, "immediate")
	if err != nil {
		t.Fatalf("IssueKey() error = %v", err)
	}
	newPlaintext, newRef, err := RotateKey(ctx, pool, 8, 0)
	if err != nil {
		t.Fatalf("RotateKey(grace=0) error = %v", err)
	}
	_, err = Authenticate(ctx, pool, oldPlaintext)
	withdrawalAuthWantCode(t, err, CodeUnauthenticated)

	result, err := Authenticate(ctx, pool, newPlaintext)
	if err != nil {
		t.Fatalf("Authenticate(successor) error = %v", err)
	}
	if result.Caller.ID != 8 || result.Key.KeyID != newRef.KeyID {
		t.Fatalf("Authenticate(successor) = %+v, want caller 8 / key %d", result, newRef.KeyID)
	}
}

// TestWithdrawalAuthRevokedHashNeverReRegisters proves the full unique index:
// a revoked digest is retained and cannot be re-inserted (23505 on
// api_key_key_hash_uniq), so a compromised key can never be resurrected.
func TestWithdrawalAuthRevokedHashNeverReRegisters(t *testing.T) {
	pool := withdrawalAuthPool(t)
	ctx := context.Background()

	plaintext, ref, err := IssueKey(ctx, pool, 11, "revoke")
	if err != nil {
		t.Fatalf("IssueKey() error = %v", err)
	}
	if err := RevokeKey(ctx, pool, ref.KeyID); err != nil {
		t.Fatalf("RevokeKey() error = %v", err)
	}
	_, err = Authenticate(ctx, pool, plaintext)
	withdrawalAuthWantCode(t, err, CodeUnauthenticated)

	_, err = pool.Exec(ctx, `
INSERT INTO api_key (caller_id, key_hash, key_prefix) VALUES ($1, $2, $3)`,
		11, HashKey(plaintext), "txh_dead")
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		t.Fatalf("duplicate revoked digest INSERT error = %v (%T), want *pgconn.PgError", err, err)
	}
	if pgErr.Code != "23505" || pgErr.ConstraintName != "api_key_key_hash_uniq" {
		t.Fatalf("duplicate revoked digest INSERT = %s/%s, want 23505/api_key_key_hash_uniq",
			pgErr.Code, pgErr.ConstraintName)
	}
}

// TestWithdrawalAuthPermissionCarried proves Authenticate surfaces can_create
// without deciding authorization itself: a caller with can_create=FALSE still
// authenticates and its AuthResult carries the FALSE permission.
func TestWithdrawalAuthPermissionCarried(t *testing.T) {
	pool := withdrawalAuthPool(t)
	ctx := context.Background()

	plaintext, _, err := IssueKey(ctx, pool, 21, "denied")
	if err != nil {
		t.Fatalf("IssueKey() error = %v", err)
	}
	if _, err := pool.Exec(ctx,
		`UPDATE caller SET can_create = FALSE, updated_at = now() WHERE caller_id = $1`, 21); err != nil {
		t.Fatalf("revoke can_create: %v", err)
	}

	result, err := Authenticate(ctx, pool, plaintext)
	if err != nil {
		t.Fatalf("Authenticate() error = %v, want nil (permission is not decided here)", err)
	}
	if result.Caller.ID != 21 || result.Caller.CanCreate {
		t.Fatalf("Authenticate() caller = %+v, want id=21 can_create=false", result.Caller)
	}
}

// TestWithdrawalAuthCrossCallerIsolation proves a key resolves only to its own
// caller: caller A's key never yields caller B's identity.
func TestWithdrawalAuthCrossCallerIsolation(t *testing.T) {
	pool := withdrawalAuthPool(t)
	ctx := context.Background()

	plaintextA, refA, err := IssueKey(ctx, pool, 31, "caller-a")
	if err != nil {
		t.Fatalf("IssueKey(A) error = %v", err)
	}
	plaintextB, refB, err := IssueKey(ctx, pool, 32, "caller-b")
	if err != nil {
		t.Fatalf("IssueKey(B) error = %v", err)
	}

	resultA, err := Authenticate(ctx, pool, plaintextA)
	if err != nil {
		t.Fatalf("Authenticate(A) error = %v", err)
	}
	resultB, err := Authenticate(ctx, pool, plaintextB)
	if err != nil {
		t.Fatalf("Authenticate(B) error = %v", err)
	}
	if resultA.Caller.ID != 31 || resultA.Key.KeyID != refA.KeyID {
		t.Fatalf("Authenticate(A) = %+v, want caller 31 / key %d", resultA, refA.KeyID)
	}
	if resultB.Caller.ID != 32 || resultB.Key.KeyID != refB.KeyID {
		t.Fatalf("Authenticate(B) = %+v, want caller 32 / key %d", resultB, refB.KeyID)
	}
	if resultA.Caller.ID == resultB.Caller.ID {
		t.Fatal("caller A's key resolved to caller B's identity")
	}
}

// TestWithdrawalAuthRotationFailureLeavesNoPartialState proves rotation is
// atomic: a failed rotation (unknown caller, FK violation) leaves zero
// api_key rows and no caller row behind — never a half-written successor.
func TestWithdrawalAuthRotationFailureLeavesNoPartialState(t *testing.T) {
	pool := withdrawalAuthPool(t)
	ctx := context.Background()

	const missingCaller int64 = 99991
	_, _, err := RotateKey(ctx, pool, missingCaller, 60)
	if err == nil {
		t.Fatal("RotateKey(unknown caller) = nil, want storage error")
	}
	withdrawalAuthWantCode(t, err, CodeTemporarilyUnavailable)

	var apiKeys int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM api_key WHERE caller_id = $1`, missingCaller).Scan(&apiKeys); err != nil {
		t.Fatalf("count api_key rows: %v", err)
	}
	if apiKeys != 0 {
		t.Fatalf("api_key rows for failed rotation = %d, want 0 (no half-written successor)", apiKeys)
	}
	if withdrawalAuthCallerExists(t, ctx, pool, missingCaller) {
		t.Fatal("caller row created by failed rotation, want none")
	}
}

// TestWithdrawalAuthRotationMidTransactionFailureRollsBack proves the rotation
// transaction rolls back past the successor INSERT: with the predecessor row
// locked by another transaction, RotateKey's predecessor UPDATE blocks until
// the caller's context deadline, the whole transaction aborts, and no
// half-written successor remains — the old credential keeps working.
func TestWithdrawalAuthRotationMidTransactionFailureRollsBack(t *testing.T) {
	pool := withdrawalAuthPool(t)
	ctx := context.Background()

	oldPlaintext, oldRef, err := IssueKey(ctx, pool, 55, "rollback")
	if err != nil {
		t.Fatalf("IssueKey() error = %v", err)
	}

	// Hold the predecessor row so RotateKey's UPDATE must wait on the lock;
	// its successor INSERT has already landed in-tx at that point.
	blocker, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin blocker tx: %v", err)
	}
	defer func() { _ = blocker.Rollback(ctx) }()
	var one int
	if err := blocker.QueryRow(ctx, `SELECT 1 FROM api_key WHERE key_id = $1 FOR UPDATE`, oldRef.KeyID).Scan(&one); err != nil {
		t.Fatalf("lock predecessor row: %v", err)
	}

	callCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	_, _, err = RotateKey(callCtx, pool, 55, 60)
	if err == nil {
		t.Fatal("RotateKey(blocked predecessor) = nil, want storage error")
	}
	withdrawalAuthWantCode(t, err, CodeTemporarilyUnavailable)

	var apiKeys int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM api_key WHERE caller_id = $1`, 55).Scan(&apiKeys); err != nil {
		t.Fatalf("count api_key rows: %v", err)
	}
	if apiKeys != 1 {
		t.Fatalf("api_key rows after rolled-back rotation = %d, want 1 (no half-written successor)", apiKeys)
	}
	var revokedAt *time.Time
	if err := pool.QueryRow(ctx, `SELECT revoked_at FROM api_key WHERE key_id = $1`, oldRef.KeyID).Scan(&revokedAt); err != nil {
		t.Fatalf("read predecessor revoked_at: %v", err)
	}
	if revokedAt != nil {
		t.Fatalf("predecessor revoked_at = %v, want NULL (rollback must undo nothing — nothing committed)", *revokedAt)
	}
	if _, err := Authenticate(ctx, pool, oldPlaintext); err != nil {
		t.Fatalf("Authenticate(predecessor) after rolled-back rotation error = %v, want nil", err)
	}
}
