//go:build integration

// intake_integration_test.go owns the T010 receipt-path integration tests
// against a real PostgreSQL 18 container (testcontainers): first persist +
// replay, conflict, cross-caller isolation, grant rejection (unknown, revoked,
// expired, mismatched), T-auth-bound, bad parameters, unauthenticated, and the
// receive-only behavior during an active 006 recovery.
//
// It reuses the T006 container/migration helper (grantSetup) and the T008
// grant-supply helper (grantTestOp + SupplyGrant), and adds intake-prefixed
// helpers so no name collides with the existing T007/T008/T006 test files.
package withdrawal

import (
	"context"
	"encoding/base64"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// intakeChainID/intakeAsset/intakeRecipient must match grantTestOp's bind
// values so a grant seeded through the reviewed supply path is submit-ready.
const (
	intakeChainID   int64 = 31337
	intakeAsset           = "0x1111111111111111111111111111111111111111"
	intakeRecipient       = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	intakeAmount          = "100"
)

// intakeRequestIDShape is the public id format: "wr-" + 32 lowercase hex.
var intakeRequestIDShape = regexp.MustCompile(`^wr-[0-9a-f]{32}$`)

// intakeKey issues one credential for callerID (creating the caller row) and
// returns its plaintext.
func intakeKey(t *testing.T, ctx context.Context, pool *pgxpool.Pool, callerID int64) string {
	t.Helper()
	plaintext, _, err := IssueKey(ctx, pool, callerID, "intake-test")
	if err != nil {
		t.Fatalf("IssueKey(caller %d): %v", callerID, err)
	}
	return plaintext
}

// intakeSupplyGrant seeds one active grant bound to callerID through the
// reviewed T008 supply path.
func intakeSupplyGrant(t *testing.T, ctx context.Context, pool *pgxpool.Pool, callerID int64, authorizationID, amount string) {
	t.Helper()
	op := grantTestOp("seed-"+authorizationID, authorizationID, callerID, amount)
	out, err := SupplyGrant(ctx, pool, op, "intake-test", "seed grant")
	if err != nil {
		t.Fatalf("SupplyGrant(%s): %v", authorizationID, err)
	}
	if out.Action != grantOutcomeSupplied {
		t.Fatalf("SupplyGrant(%s) action = %s, want %s", authorizationID, out.Action, grantOutcomeSupplied)
	}
}

// intakeReq builds a fully valid submit request over the canonical params.
func intakeReq(presented, key, authorizationID string) SubmitRequest {
	return SubmitRequest{
		PresentedKey:    presented,
		IdempotencyKey:  key,
		ChainID:         intakeChainID,
		ExpectedChainID: intakeChainID,
		Asset:           intakeAsset,
		Recipient:       intakeRecipient,
		Amount:          intakeAmount,
		AuthorizationID: authorizationID,
		Allowlist:       []string{intakeAsset},
	}
}

func intakeRequestCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM withdrawal_requests`).Scan(&n); err != nil {
		t.Fatalf("count withdrawal_requests: %v", err)
	}
	return n
}

func intakeAuditCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM withdrawal_request_audit`).Scan(&n); err != nil {
		t.Fatalf("count withdrawal_request_audit: %v", err)
	}
	return n
}

func intakeAuditActions(t *testing.T, ctx context.Context, pool *pgxpool.Pool) []string {
	t.Helper()
	rows, err := pool.Query(ctx, `SELECT action FROM withdrawal_request_audit ORDER BY audit_id`)
	if err != nil {
		t.Fatalf("query withdrawal_request_audit: %v", err)
	}
	defer rows.Close()
	var actions []string
	for rows.Next() {
		var a string
		if err := rows.Scan(&a); err != nil {
			t.Fatalf("scan audit action: %v", err)
		}
		actions = append(actions, a)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate audit actions: %v", err)
	}
	return actions
}

func intakeAmountOf(t *testing.T, ctx context.Context, pool *pgxpool.Pool, requestID string) string {
	t.Helper()
	var amount string
	if err := pool.QueryRow(ctx,
		`SELECT amount::text FROM withdrawal_requests WHERE request_id = $1`, requestID).Scan(&amount); err != nil {
		t.Fatalf("read amount for %q: %v", requestID, err)
	}
	return amount
}

func intakeWantActions(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("audit actions = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("audit actions = %v, want %v", got, want)
		}
	}
}

// intakeWantIntent asserts the response-first audit intent a result carries
// (the transport persists it after the response); no row is written by the
// core for it.
func intakeWantIntent(t *testing.T, res *SubmitResult, action string, callerID int64, requestID string) {
	t.Helper()
	if res.Audit == nil {
		t.Fatalf("result %+v has nil Audit, want action %q", res, action)
	}
	if res.Audit.Action != action || res.Audit.CallerID != callerID || res.Audit.RequestID != requestID {
		t.Fatalf("Audit = %+v, want {caller %d request %q action %q}", res.Audit, callerID, requestID, action)
	}
}

// intakeWantNoAudit asserts a result carries no audit intent (201 in-tx row or
// a 401 with no verifiable caller identity).
func intakeWantNoAudit(t *testing.T, res *SubmitResult) {
	t.Helper()
	if res.Audit != nil {
		t.Fatalf("result %+v carries Audit %+v, want nil", res, res.Audit)
	}
}

// TestWithdrawalIntakeFirstPersistAndReplay covers T-accept persistence, the
// 200 same-key-equal replay, and Q5 replay immunity to a later grant revoke.
func TestWithdrawalIntakeFirstPersistAndReplay(t *testing.T) {
	ctx, pool := grantSetup(t)
	key := intakeKey(t, ctx, pool, 7101)
	intakeSupplyGrant(t, ctx, pool, 7101, "auth-intake-1", intakeAmount)

	first, err := SubmitWithdrawal(ctx, pool, intakeReq(key, "idem-1", "auth-intake-1"))
	if err != nil {
		t.Fatalf("first SubmitWithdrawal: %v", err)
	}
	if first.Status != 201 {
		t.Fatalf("first status = %d (%+v), want 201", first.Status, first)
	}
	if !intakeRequestIDShape.MatchString(first.RequestID) {
		t.Fatalf("request_id = %q, want wr- + 32 hex", first.RequestID)
	}
	if n := intakeRequestCount(t, ctx, pool); n != 1 {
		t.Fatalf("request rows after first persist = %d, want 1", n)
	}
	if got := intakeAmountOf(t, ctx, pool, first.RequestID); got != intakeAmount {
		t.Fatalf("persisted amount = %q, want %q (exact string round-trip)", got, intakeAmount)
	}
	intakeWantNoAudit(t, first)
	intakeWantActions(t, intakeAuditActions(t, ctx, pool), []string{auditActionCreated})

	replay, err := SubmitWithdrawal(ctx, pool, intakeReq(key, "idem-1", "auth-intake-1"))
	if err != nil {
		t.Fatalf("replay SubmitWithdrawal: %v", err)
	}
	if replay.Status != 200 || replay.RequestID != first.RequestID {
		t.Fatalf("replay = %+v, want 200 with request_id %q", replay, first.RequestID)
	}
	if n := intakeRequestCount(t, ctx, pool); n != 1 {
		t.Fatalf("request rows after replay = %d, want 1 (no new row)", n)
	}
	intakeWantIntent(t, replay, auditActionReplayed, 7101, first.RequestID)
	intakeWantActions(t, intakeAuditActions(t, ctx, pool), []string{auditActionCreated})

	// Q5: a replay re-checks only current key-auth + interface permission; the
	// original grant's later revocation MUST NOT turn it into a failure.
	if _, err := RevokeGrant(ctx, pool, "revoke-op-intake-1", "auth-intake-1", "intake-test", "post-accept revoke"); err != nil {
		t.Fatalf("RevokeGrant: %v", err)
	}
	afterRevoke, err := SubmitWithdrawal(ctx, pool, intakeReq(key, "idem-1", "auth-intake-1"))
	if err != nil {
		t.Fatalf("replay after revoke: %v", err)
	}
	if afterRevoke.Status != 200 || afterRevoke.RequestID != first.RequestID {
		t.Fatalf("replay after revoke = %+v, want 200 with the original request_id", afterRevoke)
	}
	intakeWantIntent(t, afterRevoke, auditActionReplayed, 7101, first.RequestID)
	intakeWantActions(t, intakeAuditActions(t, ctx, pool), []string{auditActionCreated})
}

// TestWithdrawalIntakeConflict covers T-conflict: same key with a differing
// business parameter returns 409, leaves the original untouched, and records a
// `conflict` audit row.
func TestWithdrawalIntakeConflict(t *testing.T) {
	ctx, pool := grantSetup(t)
	key := intakeKey(t, ctx, pool, 7102)
	intakeSupplyGrant(t, ctx, pool, 7102, "auth-intake-2", intakeAmount)

	first, err := SubmitWithdrawal(ctx, pool, intakeReq(key, "idem-2", "auth-intake-2"))
	if err != nil || first.Status != 201 {
		t.Fatalf("first submit = (%+v, %v), want 201", first, err)
	}

	differ := intakeReq(key, "idem-2", "auth-intake-2")
	differ.Amount = "101"
	conflict, err := SubmitWithdrawal(ctx, pool, differ)
	if err != nil {
		t.Fatalf("conflicting submit: %v", err)
	}
	if conflict.Status != 409 || conflict.Code != CodeIdempotencyConflict {
		t.Fatalf("conflict result = %+v, want 409/%s", conflict, CodeIdempotencyConflict)
	}
	if conflict.RequestID != first.RequestID {
		t.Fatalf("conflict request_id = %q, want the original %q", conflict.RequestID, first.RequestID)
	}
	if n := intakeRequestCount(t, ctx, pool); n != 1 {
		t.Fatalf("request rows after conflict = %d, want 1 (original untouched)", n)
	}
	if got := intakeAmountOf(t, ctx, pool, first.RequestID); got != intakeAmount {
		t.Fatalf("original amount = %q, want %q (unchanged)", got, intakeAmount)
	}
	intakeWantIntent(t, conflict, auditActionConflict, 7102, first.RequestID)
	intakeWantActions(t, intakeAuditActions(t, ctx, pool), []string{auditActionCreated})
}

// TestWithdrawalIntakeAuthorizationBound covers T-auth-bound: a different key
// reusing an already-consumed grant hits the authorization UNIQUE, classifies
// to 403 (auth_failed), and creates zero new rows.
func TestWithdrawalIntakeAuthorizationBound(t *testing.T) {
	ctx, pool := grantSetup(t)
	key := intakeKey(t, ctx, pool, 7103)
	intakeSupplyGrant(t, ctx, pool, 7103, "auth-intake-bound", intakeAmount)

	if first, err := SubmitWithdrawal(ctx, pool, intakeReq(key, "idem-bound-1", "auth-intake-bound")); err != nil || first.Status != 201 {
		t.Fatalf("first submit = (%+v, %v), want 201", first, err)
	}

	second, err := SubmitWithdrawal(ctx, pool, intakeReq(key, "idem-bound-2", "auth-intake-bound"))
	if err != nil {
		t.Fatalf("second-key submit: %v", err)
	}
	if second.Status != 403 || second.Code != CodeAuthorizationInvalid {
		t.Fatalf("auth-bound result = %+v, want 403/%s", second, CodeAuthorizationInvalid)
	}
	if n := intakeRequestCount(t, ctx, pool); n != 1 {
		t.Fatalf("request rows after auth-bound = %d, want 1 (zero new rows)", n)
	}
	intakeWantIntent(t, second, auditActionAuthFailed, 7103, "")
	intakeWantActions(t, intakeAuditActions(t, ctx, pool), []string{auditActionCreated})
}

// TestWithdrawalIntakeCrossCallerIndependent proves the (caller_id,
// idempotency_key) scope: two callers may use the same key independently.
func TestWithdrawalIntakeCrossCallerIndependent(t *testing.T) {
	ctx, pool := grantSetup(t)
	keyA := intakeKey(t, ctx, pool, 7104)
	keyB := intakeKey(t, ctx, pool, 7105)
	intakeSupplyGrant(t, ctx, pool, 7104, "auth-cross-a", intakeAmount)
	intakeSupplyGrant(t, ctx, pool, 7105, "auth-cross-b", intakeAmount)

	a, err := SubmitWithdrawal(ctx, pool, intakeReq(keyA, "shared-key", "auth-cross-a"))
	if err != nil || a.Status != 201 {
		t.Fatalf("caller A submit = (%+v, %v), want 201", a, err)
	}
	b, err := SubmitWithdrawal(ctx, pool, intakeReq(keyB, "shared-key", "auth-cross-b"))
	if err != nil || b.Status != 201 {
		t.Fatalf("caller B submit = (%+v, %v), want 201", b, err)
	}
	if a.RequestID == b.RequestID {
		t.Fatalf("cross-caller request ids collided: %q", a.RequestID)
	}
	if n := intakeRequestCount(t, ctx, pool); n != 2 {
		t.Fatalf("request rows = %d, want 2 (independent scopes)", n)
	}
}

// TestWithdrawalIntakeUnknownGrant covers a valid request against a grant that
// does not exist: 403, zero request rows, nil RequestID, `rejected` audit.
func TestWithdrawalIntakeUnknownGrant(t *testing.T) {
	ctx, pool := grantSetup(t)
	key := intakeKey(t, ctx, pool, 7106)

	res, err := SubmitWithdrawal(ctx, pool, intakeReq(key, "idem-unknown", "auth-missing"))
	if err != nil {
		t.Fatalf("unknown-grant submit: %v", err)
	}
	if res.Status != 403 || res.Code != CodeAuthorizationInvalid || res.RequestID != "" {
		t.Fatalf("unknown-grant result = %+v, want 403/%s with no request_id", res, CodeAuthorizationInvalid)
	}
	if n := intakeRequestCount(t, ctx, pool); n != 0 {
		t.Fatalf("request rows after unknown grant = %d, want 0", n)
	}
	intakeWantIntent(t, res, auditActionRejected, 7106, "")
	if n := intakeAuditCount(t, ctx, pool); n != 0 {
		t.Fatalf("audit rows after unknown grant = %d, want 0 (intent only)", n)
	}
}

// TestWithdrawalIntakeRevokedAndExpiredGrant covers both non-active grant
// states, including the strict expires_at > t_check boundary (a past instant).
func TestWithdrawalIntakeRevokedAndExpiredGrant(t *testing.T) {
	ctx, pool := grantSetup(t)

	t.Run("revoked", func(t *testing.T) {
		key := intakeKey(t, ctx, pool, 7107)
		intakeSupplyGrant(t, ctx, pool, 7107, "auth-revoked", intakeAmount)
		if _, err := RevokeGrant(ctx, pool, "revoke-op-revoked", "auth-revoked", "intake-test", "revoke"); err != nil {
			t.Fatalf("RevokeGrant: %v", err)
		}
		res, err := SubmitWithdrawal(ctx, pool, intakeReq(key, "idem-revoked", "auth-revoked"))
		if err != nil || res.Status != 403 || res.Code != CodeAuthorizationInvalid {
			t.Fatalf("revoked-grant submit = (%+v, %v), want 403/%s", res, err, CodeAuthorizationInvalid)
		}
		if n := intakeRequestCount(t, ctx, pool); n != 0 {
			t.Fatalf("request rows after revoked grant = %d, want 0", n)
		}
		intakeWantIntent(t, res, auditActionRejected, 7107, "")
	})

	t.Run("expired", func(t *testing.T) {
		key := intakeKey(t, ctx, pool, 7108)
		if _, err := pool.Exec(ctx, `
INSERT INTO withdrawal_authorizations
    (authorization_id, caller_id, chain_id, asset, recipient, amount, state, expires_at, supplied_by)
VALUES ($1, $2, $3, $4, $5, $6::numeric, 'active', now() - interval '1 hour', 'seed')`,
			"auth-expired", int64(7108), intakeChainID, intakeAsset, intakeRecipient, intakeAmount); err != nil {
			t.Fatalf("seed expired grant: %v", err)
		}
		res, err := SubmitWithdrawal(ctx, pool, intakeReq(key, "idem-expired", "auth-expired"))
		if err != nil || res.Status != 403 || res.Code != CodeAuthorizationInvalid {
			t.Fatalf("expired-grant submit = (%+v, %v), want 403/%s", res, err, CodeAuthorizationInvalid)
		}
		if n := intakeRequestCount(t, ctx, pool); n != 0 {
			t.Fatalf("request rows after expired grant = %d, want 0", n)
		}
		intakeWantIntent(t, res, auditActionRejected, 7108, "")
	})
}

// TestWithdrawalIntakeMismatchedGrant covers a grant whose bound amount differs
// from the request: in-tx validation fails, zero rows.
func TestWithdrawalIntakeMismatchedGrant(t *testing.T) {
	ctx, pool := grantSetup(t)
	key := intakeKey(t, ctx, pool, 7109)
	intakeSupplyGrant(t, ctx, pool, 7109, "auth-mismatch", "999")

	res, err := SubmitWithdrawal(ctx, pool, intakeReq(key, "idem-mismatch", "auth-mismatch"))
	if err != nil || res.Status != 403 || res.Code != CodeAuthorizationInvalid {
		t.Fatalf("mismatched-grant submit = (%+v, %v), want 403/%s", res, err, CodeAuthorizationInvalid)
	}
	if n := intakeRequestCount(t, ctx, pool); n != 0 {
		t.Fatalf("request rows after mismatched grant = %d, want 0", n)
	}
	intakeWantIntent(t, res, auditActionRejected, 7109, "")
}

// TestWithdrawalIntakeBadParams covers the step-3 semantic rejects: every case
// returns 422 validation_failed with zero request rows.
func TestWithdrawalIntakeBadParams(t *testing.T) {
	ctx, pool := grantSetup(t)
	key := intakeKey(t, ctx, pool, 7110)
	intakeSupplyGrant(t, ctx, pool, 7110, "auth-bad", intakeAmount)

	cases := []struct {
		name   string
		mutate func(*SubmitRequest)
	}{
		{"amount shape", func(r *SubmitRequest) { r.Amount = "1.5" }},
		{"asset shape", func(r *SubmitRequest) { r.Asset = "not-an-address" }},
		{"asset not whitelisted", func(r *SubmitRequest) { r.Asset = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" }},
		{"recipient shape", func(r *SubmitRequest) { r.Recipient = "0x1234" }},
		{"chain mismatch", func(r *SubmitRequest) { r.ChainID = 1 }},
		{"bad idempotency key", func(r *SubmitRequest) { r.IdempotencyKey = "has space" }},
		{"missing authorization_id", func(r *SubmitRequest) { r.AuthorizationID = "" }},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := intakeReq(key, fmt.Sprintf("idem-bad-%d", i), "auth-bad")
			tc.mutate(&req)
			res, err := SubmitWithdrawal(ctx, pool, req)
			if err != nil {
				t.Fatalf("bad-param submit: %v", err)
			}
			if res.Status != 422 || res.Code != CodeValidationFailed {
				t.Fatalf("bad-param result = %+v, want 422/%s", res, CodeValidationFailed)
			}
			intakeWantIntent(t, res, auditActionRejected, 7110, "")
		})
	}
	if n := intakeRequestCount(t, ctx, pool); n != 0 {
		t.Fatalf("request rows after bad params = %d, want 0", n)
	}
	if n := intakeAuditCount(t, ctx, pool); n != 0 {
		t.Fatalf("audit rows after bad params = %d, want 0 (intent only)", n)
	}
}

// TestWithdrawalIntakeUnauthenticated covers the 401 path: a shape-valid key
// that matches no credential writes zero request rows and zero Table 5 audit
// rows (there is no caller identity to attribute).
func TestWithdrawalIntakeUnauthenticated(t *testing.T) {
	ctx, pool := grantSetup(t)
	unknown := keyPrefix + base64.RawURLEncoding.EncodeToString(make([]byte, keyEntropyBytes))

	res, err := SubmitWithdrawal(ctx, pool, intakeReq(unknown, "idem-401", "auth-x"))
	if err != nil {
		t.Fatalf("unauthenticated submit: %v", err)
	}
	if res.Status != 401 || res.Code != CodeUnauthenticated {
		t.Fatalf("unauthenticated result = %+v, want 401/%s", res, CodeUnauthenticated)
	}
	intakeWantNoAudit(t, res)
	if n := intakeRequestCount(t, ctx, pool); n != 0 {
		t.Fatalf("request rows after 401 = %d, want 0", n)
	}
	if n := intakeAuditCount(t, ctx, pool); n != 0 {
		t.Fatalf("audit rows after 401 = %d, want 0", n)
	}
}

// TestWithdrawalIntakeActiveRecoveryPersist proves FR-16 receive-only behavior
// during an active 006 recovery: a compliant create still persists, and the
// intake path writes no execution artefacts (there are none by construction; it
// leaves the recovery row and its event log untouched).
func TestWithdrawalIntakeActiveRecoveryPersist(t *testing.T) {
	ctx, pool := grantSetup(t)
	key := intakeKey(t, ctx, pool, 7111)
	intakeSupplyGrant(t, ctx, pool, 7111, "auth-recovery", intakeAmount)

	// Seed one active 006 recovery instance (chain 1), exactly as T006 does.
	if _, err := pool.Exec(ctx, `INSERT INTO reorg_policy_history
		(chain_id, policy_seq, max_depth, operator) VALUES (1, 1, 64, 'bootstrap')`); err != nil {
		t.Fatalf("seed reorg policy: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO reorg_recovery
		(chain_id, recovery_id, phase, policy_seq, max_depth, bound_old_number, bound_old_hash, recovery_seq)
		VALUES (1, 'rec-intake', 'detected', 1, 64, 100, $1, 1)`,
		"0x"+strings.Repeat("aa", 32)); err != nil {
		t.Fatalf("seed active recovery: %v", err)
	}

	res, err := SubmitWithdrawal(ctx, pool, intakeReq(key, "idem-recovery", "auth-recovery"))
	if err != nil || res.Status != 201 {
		t.Fatalf("submit during active recovery = (%+v, %v), want 201", res, err)
	}
	intakeWantNoAudit(t, res)
	if n := intakeRequestCount(t, ctx, pool); n != 1 {
		t.Fatalf("request rows during recovery = %d, want 1", n)
	}
	var recoveries, events int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM reorg_recovery`).Scan(&recoveries); err != nil {
		t.Fatalf("count reorg_recovery: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM reorg_recovery_events`).Scan(&events); err != nil {
		t.Fatalf("count reorg_recovery_events: %v", err)
	}
	if recoveries != 1 || events != 0 {
		t.Fatalf("recovery rows after intake = (%d, %d), want (1, 0) untouched", recoveries, events)
	}
}

// TestWithdrawalIntakeStorageDown is the explicit hand-off placeholder: fault
// injection (storage down, uncertain commit, deadlock/timeout mapping) is owned
// by T026's failure_integration_test.go, which this batch does not have.
func TestWithdrawalIntakeStorageDown(t *testing.T) {
	t.Skip("storage-failure and unknown-commit fault injection are owned by T026")
}

// TestWithdrawalIntakeV1AmountRoundTripMaxUint256 covers SC-08's exactness
// requirement at the uint256 ceiling: a max-bound compliant create persists the
// amount string verbatim and a self-query reads it back byte-identical — the
// integer never passes through a float and is never re-formatted.
func TestWithdrawalIntakeV1AmountRoundTripMaxUint256(t *testing.T) {
	// Given a caller with an active grant bound to the uint256 maximum.
	ctx, pool := grantSetup(t)
	const callerID = int64(7150)
	key := intakeKey(t, ctx, pool, callerID)
	intakeSupplyGrant(t, ctx, pool, callerID, "auth-v1-max", withdrawalMaxUint256)

	// When the compliant max-amount request is persisted.
	req := intakeReq(key, "idem-v1-max", "auth-v1-max")
	req.Amount = withdrawalMaxUint256
	first, err := SubmitWithdrawal(ctx, pool, req)
	if err != nil {
		t.Fatalf("max-amount SubmitWithdrawal: %v", err)
	}

	// Then it is accepted with no pre-tx audit intent, and BOTH the stored row
	// and the self-query carry the byte-identical max string.
	if first.Status != 201 {
		t.Fatalf("max-amount status = %d (%+v), want 201", first.Status, first)
	}
	intakeWantNoAudit(t, first)
	if got := intakeAmountOf(t, ctx, pool, first.RequestID); got != withdrawalMaxUint256 {
		t.Fatalf("persisted amount = %q, want byte-identical %q", got, withdrawalMaxUint256)
	}
	view, err := GetWithdrawal(ctx, pool, callerID, first.RequestID)
	if err != nil {
		t.Fatalf("GetWithdrawal(max-amount): %v", err)
	}
	if view.Amount != withdrawalMaxUint256 {
		t.Fatalf("self-query amount = %q, want byte-identical %q", view.Amount, withdrawalMaxUint256)
	}
	intakeWantActions(t, intakeAuditActions(t, ctx, pool), []string{auditActionCreated})
}

// TestWithdrawalIntakeV1CreateSelfQueryIdentity covers SC-01 end to end on the
// 007 path (not a seeded row): the create response and the owner's self-query
// agree on every field, status is accepted, created_at is a real timestamp, and
// the recovery signal is the standing none/not_started.
func TestWithdrawalIntakeV1CreateSelfQueryIdentity(t *testing.T) {
	// Given a caller with an active grant.
	ctx, pool := grantSetup(t)
	const callerID = int64(7151)
	key := intakeKey(t, ctx, pool, callerID)
	intakeSupplyGrant(t, ctx, pool, callerID, "auth-v1-identity", intakeAmount)

	// When the compliant create is persisted and the owner queries it by the
	// request_id it returned.
	first, err := SubmitWithdrawal(ctx, pool, intakeReq(key, "idem-v1-identity", "auth-v1-identity"))
	if err != nil {
		t.Fatalf("SubmitWithdrawal: %v", err)
	}
	if first.Status != 201 {
		t.Fatalf("create status = %d (%+v), want 201", first.Status, first)
	}
	intakeWantNoAudit(t, first)

	view, err := GetWithdrawal(ctx, pool, callerID, first.RequestID)
	if err != nil {
		t.Fatalf("GetWithdrawal(self): %v", err)
	}

	// Then every field of the self-query matches the create, the in-tx
	// `created` row exists (no pre-tx rows), and the standing signal holds.
	if view.RequestID != first.RequestID || view.CallerID != callerID {
		t.Fatalf("identity = %q/%d, want %q/%d", view.RequestID, view.CallerID, first.RequestID, callerID)
	}
	if view.ChainID != intakeChainID || view.Asset != intakeAsset || view.Recipient != intakeRecipient {
		t.Fatalf("bound fields = chain %d asset %q recipient %q, want %d/%q/%q",
			view.ChainID, view.Asset, view.Recipient, intakeChainID, intakeAsset, intakeRecipient)
	}
	if view.Amount != intakeAmount {
		t.Fatalf("amount = %q, want %q", view.Amount, intakeAmount)
	}
	if view.Status != "accepted" {
		t.Fatalf("status = %q, want accepted", view.Status)
	}
	if view.CreatedAt.IsZero() {
		t.Fatal("created_at is zero, want a real timestamp")
	}
	if view.Recovery.State != "none" || view.Recovery.Execution != "not_started" {
		t.Fatalf("recovery = %+v, want {none not_started}", view.Recovery)
	}
	intakeWantActions(t, intakeAuditActions(t, ctx, pool), []string{auditActionCreated})
}

// TestWithdrawalIntakeV1FieldCompleteness covers the SC-08/FR-07 persistence
// spot checks the replay tests do not: a legal mixed-case request persists every
// Table 3 column, with asset/recipient canonicalized to lowercase, and the
// self-query returns that canonical content.
func TestWithdrawalIntakeV1FieldCompleteness(t *testing.T) {
	// Given a caller with an active grant and a legal mixed-case request.
	ctx, pool := grantSetup(t)
	const callerID = int64(7152)
	key := intakeKey(t, ctx, pool, callerID)
	intakeSupplyGrant(t, ctx, pool, callerID, "auth-v1-complete", intakeAmount)

	req := intakeReq(key, "idem-v1-complete", "auth-v1-complete")
	req.Asset = "0x" + strings.ToUpper(intakeAsset[2:])
	req.Recipient = "0x" + strings.ToUpper(intakeRecipient[2:])

	// When it is persisted.
	first, err := SubmitWithdrawal(ctx, pool, req)
	if err != nil {
		t.Fatalf("mixed-case SubmitWithdrawal: %v", err)
	}
	if first.Status != 201 {
		t.Fatalf("mixed-case status = %d (%+v), want 201", first.Status, first)
	}

	// Then every persisted column is present and canonical.
	var (
		asset, recipient, status, idemKey, authID string
		chainID                                   int64
	)
	if err := pool.QueryRow(ctx, `
SELECT asset, recipient, chain_id, status, idempotency_key, authorization_id
FROM withdrawal_requests WHERE request_id = $1`, first.RequestID).
		Scan(&asset, &recipient, &chainID, &status, &idemKey, &authID); err != nil {
		t.Fatalf("read persisted columns: %v", err)
	}
	if asset != intakeAsset || recipient != intakeRecipient {
		t.Fatalf("persisted asset/recipient = %q/%q, want canonical %q/%q",
			asset, recipient, intakeAsset, intakeRecipient)
	}
	if chainID != intakeChainID || status != "accepted" {
		t.Fatalf("persisted chain/status = %d/%q, want %d/accepted", chainID, status, intakeChainID)
	}
	if idemKey != "idem-v1-complete" || authID != "auth-v1-complete" {
		t.Fatalf("persisted idem/auth = %q/%q, want verbatim key + bound authorization", idemKey, authID)
	}
	view, err := GetWithdrawal(ctx, pool, callerID, first.RequestID)
	if err != nil {
		t.Fatalf("GetWithdrawal(mixed-case): %v", err)
	}
	if view.Asset != intakeAsset || view.Recipient != intakeRecipient {
		t.Fatalf("self-query asset/recipient = %q/%q, want canonical %q/%q",
			view.Asset, view.Recipient, intakeAsset, intakeRecipient)
	}
}
