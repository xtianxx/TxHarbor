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

// ---------------------------------------------------------------------------
// T016 [US2] — V2 auth matrix (contracts/api.md §1-2)
// ---------------------------------------------------------------------------
//
// These tests extend the T007 credential-core coverage above with the V2
// rejection matrix at the frozen SubmitWithdrawal boundary: the 401 key paths,
// the 403 interface-permission path, and the 403 grant paths. Each asserts the
// exact status/code, the response-first audit intent shape (401 → nil; 403 →
// non-nil), and that no withdrawal_requests row is ever written.
//
// Rotation's caller_id + scope preservation is already locked by
// TestWithdrawalAuthRotationGracePreservesIdentity and is deliberately NOT
// duplicated here.
//
// Fingerprints: the 401 paths write nothing (no verifiable caller identity), so
// Audit is nil and BOTH Table 3 (withdrawal_requests) and Table 5
// (withdrawal_request_audit) stay at zero rows. The 403 paths carry a
// response-first AuditIntent but the core still writes NOTHING — the intent is
// persisted by the transport AFTER the response (T014/T010). The
// permission-denied case demonstrates that hand-off explicitly by calling
// WriteRejectAudit with the returned intent exactly once, mirroring the
// handler's post-response write; the grant 403 cases stay intent-only.
//
// T018 appends its own R4/R5 startpoint-semantics tests below this section; the
// separator keeps the two additions from interleaving.

// TestWithdrawalAuthMatrixRejectedKeyPaths covers V2's 401 key matrix at the
// library boundary: a missing key, a malformed key, a shape-valid but unissued
// key, and a revoked key each classify to 401 unauthenticated with no audit
// intent and zero Table 3 / Table 5 rows.
func TestWithdrawalAuthMatrixRejectedKeyPaths(t *testing.T) {
	pool := withdrawalAuthPool(t)
	ctx := context.Background()

	// Given a shape-valid credential that was never issued (distinct from both
	// the empty and the malformed shapes)...
	unissued, _, _, err := GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey(unissued): %v", err)
	}
	// ...and a real credential for caller 6001 that is then explicitly revoked.
	revoked, revokedRef, err := IssueKey(ctx, pool, 6001, "v2-revoked")
	if err != nil {
		t.Fatalf("IssueKey(6001): %v", err)
	}
	if err := RevokeKey(ctx, pool, revokedRef.KeyID); err != nil {
		t.Fatalf("RevokeKey(6001 key): %v", err)
	}

	cases := []struct {
		name      string
		presented string
	}{
		{"missing key", ""},
		{"malformed key", "garbage"},
		{"never-issued shape-valid key", unissued},
		{"revoked key", revoked},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// When an otherwise valid request presents the rejected key.
			res, err := SubmitWithdrawal(ctx, pool, intakeReq(tc.presented, "idem-v2-401", "auth-v2-missing"))
			if err != nil {
				t.Fatalf("SubmitWithdrawal(%s): %v", tc.name, err)
			}

			// Then it is 401 unauthenticated with no audit intent.
			if res.Status != 401 || res.Code != CodeUnauthenticated {
				t.Fatalf("result = %+v, want 401/%s", res, CodeUnauthenticated)
			}
			intakeWantNoAudit(t, res)

			// And nothing was written: zero Table 3 rows and zero Table 5 rows.
			if n := intakeRequestCount(t, ctx, pool); n != 0 {
				t.Fatalf("withdrawal_requests rows after 401 = %d, want 0", n)
			}
			if n := intakeAuditCount(t, ctx, pool); n != 0 {
				t.Fatalf("withdrawal_request_audit rows after 401 = %d, want 0", n)
			}
		})
	}
}

// TestWithdrawalAuthMatrixPermissionDenied covers the 403 interface-permission
// path: a caller whose can_create=FALSE authenticates but is refused with
// unauthorized, the result carries a non-nil `rejected` AuditIntent, and the
// core writes no rows. The test then performs the transport's post-response
// write itself (WriteRejectAudit) to prove the intent lands exactly one Table 5
// row — the ONLY Table 5 row this file's V2 matrix expects, since the 401 paths
// are nil-intent and the grant 403 paths stay intent-only.
func TestWithdrawalAuthMatrixPermissionDenied(t *testing.T) {
	pool := withdrawalAuthPool(t)
	ctx := context.Background()

	// Given a caller with a valid key but can_create=FALSE.
	const callerID = int64(6002)
	key := intakeKey(t, ctx, pool, callerID)
	if _, err := pool.Exec(ctx,
		`UPDATE caller SET can_create = FALSE, updated_at = now() WHERE caller_id = $1`, callerID); err != nil {
		t.Fatalf("set can_create=FALSE: %v", err)
	}

	// When a valid request is submitted.
	res, err := SubmitWithdrawal(ctx, pool, intakeReq(key, "idem-v2-denied", "auth-v2-denied"))
	if err != nil {
		t.Fatalf("SubmitWithdrawal(can_create=FALSE): %v", err)
	}

	// Then it is 403 unauthorized with a non-nil `rejected` intent, and the
	// core persisted nothing (intent only).
	if res.Status != 403 || res.Code != CodeUnauthorized {
		t.Fatalf("result = %+v, want 403/%s", res, CodeUnauthorized)
	}
	intakeWantIntent(t, res, auditActionRejected, callerID, "")
	if n := intakeRequestCount(t, ctx, pool); n != 0 {
		t.Fatalf("withdrawal_requests rows after 403 = %d, want 0", n)
	}
	if n := intakeAuditCount(t, ctx, pool); n != 0 {
		t.Fatalf("withdrawal_request_audit rows before the transport write = %d, want 0 (intent only)", n)
	}

	// And when the transport performs its post-response write with the returned
	// intent, exactly one `rejected` row appears.
	if err := WriteRejectAudit(ctx, pool,
		res.Audit.CallerID, res.Audit.RequestID, res.Audit.Action, res.Audit.Detail); err != nil {
		t.Fatalf("WriteRejectAudit(intent): %v", err)
	}
	if got := intakeAuditActions(t, ctx, pool); len(got) != 1 || got[0] != auditActionRejected {
		t.Fatalf("withdrawal_request_audit actions = %v, want [rejected]", got)
	}
}

// TestWithdrawalAuthMatrixGrantRejects covers V2's 403 grant matrix: a valid,
// permitted key presenting an authorization that does not exist, then one whose
// bound amount differs from the request. Each classifies to 403
// authorization_invalid with a non-nil `rejected` intent and zero Table 3 rows;
// neither persists a Table 5 row.
func TestWithdrawalAuthMatrixGrantRejects(t *testing.T) {
	pool := withdrawalAuthPool(t)
	ctx := context.Background()

	// Given a valid key for caller 6003.
	const callerID = int64(6003)
	key := intakeKey(t, ctx, pool, callerID)

	t.Run("unknown grant", func(t *testing.T) {
		// When the authorization_id has no row at all.
		res, err := SubmitWithdrawal(ctx, pool, intakeReq(key, "idem-v2-unknown", "auth-v2-missing"))
		if err != nil {
			t.Fatalf("SubmitWithdrawal(unknown grant): %v", err)
		}
		// Then 403 authorization_invalid, rejected intent, zero Table 3 rows.
		if res.Status != 403 || res.Code != CodeAuthorizationInvalid {
			t.Fatalf("result = %+v, want 403/%s", res, CodeAuthorizationInvalid)
		}
		intakeWantIntent(t, res, auditActionRejected, callerID, "")
		if n := intakeRequestCount(t, ctx, pool); n != 0 {
			t.Fatalf("withdrawal_requests rows after unknown grant = %d, want 0", n)
		}
	})

	t.Run("mismatched grant", func(t *testing.T) {
		// Given an active grant bound to an amount that differs from the request.
		intakeSupplyGrant(t, ctx, pool, callerID, "auth-v2-mismatch", "999")
		// When the request presents the canonical amount.
		res, err := SubmitWithdrawal(ctx, pool, intakeReq(key, "idem-v2-mismatch", "auth-v2-mismatch"))
		if err != nil {
			t.Fatalf("SubmitWithdrawal(mismatched grant): %v", err)
		}
		// Then 403 authorization_invalid, rejected intent, zero Table 3 rows.
		if res.Status != 403 || res.Code != CodeAuthorizationInvalid {
			t.Fatalf("result = %+v, want 403/%s", res, CodeAuthorizationInvalid)
		}
		intakeWantIntent(t, res, auditActionRejected, callerID, "")
		if n := intakeRequestCount(t, ctx, pool); n != 0 {
			t.Fatalf("withdrawal_requests rows after mismatched grant = %d, want 0", n)
		}
	})

	// And neither grant 403 path persisted a Table 5 row (intent only).
	if n := intakeAuditCount(t, ctx, pool); n != 0 {
		t.Fatalf("withdrawal_request_audit rows after grant rejects = %d, want 0 (intent only)", n)
	}
}

// ---------------------------------------------------------------------------
// T018 [US2] — startpoint semantics (R4/R5)
// ---------------------------------------------------------------------------
//
// The auth predicate (R4) is evaluated at each attempt's start; there is no
// cache, so the staleness window never outlives the attempt. These tests lock
// that boundary: a revoke is observed by the NEXT Authenticate call, never by
// an attempt that already read its verdict, and a rotation's grace stamp is a
// future wall-clock instant, so the predecessor's acceptance ends exactly when
// the window elapses.

// TestWithdrawalAuthStartpointRevokeAffectsNextAttemptOnly covers R4's
// pre/post-startpoint boundary: a revoke committing after a successful
// startpoint is refused on the next fresh Authenticate call, while a revoke
// committing before any attempt makes that first attempt 401.
func TestWithdrawalAuthStartpointRevokeAffectsNextAttemptOnly(t *testing.T) {
	pool := withdrawalAuthPool(t)
	ctx := context.Background()

	// Given a valid key whose first attempt completes (its startpoint verdict
	// is already read)...
	plaintext, ref, err := IssueKey(ctx, pool, 81, "startpoint-after")
	if err != nil {
		t.Fatalf("IssueKey(81) error = %v", err)
	}
	first, err := Authenticate(ctx, pool, plaintext)
	if err != nil {
		t.Fatalf("Authenticate(81, attempt 1) error = %v", err)
	}
	if first.Caller.ID != 81 || first.Key.KeyID != ref.KeyID {
		t.Fatalf("Authenticate(81, attempt 1) = %+v, want caller 81 / key %d", first, ref.KeyID)
	}

	// When the key is revoked after that startpoint...
	if err := RevokeKey(ctx, pool, ref.KeyID); err != nil {
		t.Fatalf("RevokeKey(81) error = %v", err)
	}

	// Then the same logical retry as a NEW Authenticate call re-evaluates the
	// startpoint and is refused; the caller row survives.
	_, err = Authenticate(ctx, pool, plaintext)
	withdrawalAuthWantCode(t, err, CodeUnauthenticated)
	if !withdrawalAuthCallerExists(t, ctx, pool, 81) {
		t.Fatal("caller row missing after startpoint-boundary revoke")
	}

	// And the mirror case: a revoke committing BEFORE any startpoint is observed
	// by that very first attempt.
	prePlaintext, preRef, err := IssueKey(ctx, pool, 82, "startpoint-before")
	if err != nil {
		t.Fatalf("IssueKey(82) error = %v", err)
	}
	if err := RevokeKey(ctx, pool, preRef.KeyID); err != nil {
		t.Fatalf("RevokeKey(82) error = %v", err)
	}
	_, err = Authenticate(ctx, pool, prePlaintext)
	withdrawalAuthWantCode(t, err, CodeUnauthenticated)
}

// TestWithdrawalAuthStartpointGraceWindowExpiry covers R4's time-based grace
// stamp: with a one-second grace the predecessor and successor both accept
// while the window is open, and once wall-clock passes the stamp the
// predecessor's next startpoint check is refused while the successor keeps
// working. The bounded poll below is itself the subject of the assertion (the
// grace window IS time) — no other step waits.
func TestWithdrawalAuthStartpointGraceWindowExpiry(t *testing.T) {
	pool := withdrawalAuthPool(t)
	ctx := context.Background()

	oldPlaintext, oldRef, err := IssueKey(ctx, pool, 83, "grace-window")
	if err != nil {
		t.Fatalf("IssueKey(83) error = %v", err)
	}
	newPlaintext, newRef, err := RotateKey(ctx, pool, 83, 1)
	if err != nil {
		t.Fatalf("RotateKey(83, grace=1) error = %v", err)
	}
	if newRef.KeyID == oldRef.KeyID {
		t.Fatal("RotateKey reused the predecessor key_id")
	}

	// During the grace window both credentials accept (dual-accept).
	oldResult, err := Authenticate(ctx, pool, oldPlaintext)
	if err != nil {
		t.Fatalf("Authenticate(predecessor within grace) error = %v", err)
	}
	newResult, err := Authenticate(ctx, pool, newPlaintext)
	if err != nil {
		t.Fatalf("Authenticate(successor) error = %v", err)
	}
	if oldResult.Caller.ID != 83 || newResult.Caller.ID != 83 {
		t.Fatalf("grace dual-accept caller ids = %d/%d, want 83/83", oldResult.Caller.ID, newResult.Caller.ID)
	}

	// After the window elapses the predecessor's next attempt is refused.
	deadline := time.Now().Add(15 * time.Second)
	for {
		if _, err := Authenticate(ctx, pool, oldPlaintext); err != nil {
			withdrawalAuthWantCode(t, err, CodeUnauthenticated)
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("predecessor still authenticates after the grace window elapsed")
		}
		time.Sleep(100 * time.Millisecond)
	}
	if _, err := Authenticate(ctx, pool, newPlaintext); err != nil {
		t.Fatalf("Authenticate(successor) after predecessor grace expiry error = %v", err)
	}
	if !withdrawalAuthCallerExists(t, ctx, pool, 83) {
		t.Fatal("caller row missing after grace expiry")
	}
}
