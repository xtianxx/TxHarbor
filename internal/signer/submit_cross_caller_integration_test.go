//go:build integration

// Batch C3: the cross-caller grant boundary on the 009 submit path.
//
// Defect guarded: caller A presents a body naming caller B's active grant. If
// the caller-scoped ownership check were skipped (gates.go:226-228), A would
// consume B's authorization. Symmetrically, if request identity were global
// instead of caller-scoped (data-model.md identity columns: (caller_id,
// signing_request_id); OC-4), A reusing B's already-bound signing_request_id
// would misfire as a request_conflict instead of a fresh, independently
// refused identity.
//
// Invariant: EvaluateGrant refuses a grant whose caller_id differs from the
// authenticated caller as authorization_invalid (contracts/api.md §2 403
// mismatch row); the refusal is a terminal submitRefusal recorded on A's own
// row plus audit (submit.go:233, 401-404, 440-450), zero signatures, and every
// 007 read is SELECT-only (gates.go:226-228; readGrantForShare FOR SHARE) so
// B's request row and grant row stay byte-identical.
//
// Gap: V3 proved credential/permission/sender-policy, and V6's grant matrix
// ran a single caller; the A-credential x B-grant refusal and the OC-4
// caller-scoped same-request-id boundary were uncovered.
//
// Level: integration (testcontainers PostgreSQL 18, real submit transaction).
//
// Criteria: ClassAuthorizationInvalid mapped to HTTP 403; zero
// signature_results; A's row state=rejected/refusal_class=authorization_invalid;
// the audit row names caller A; B's request row and B's grant row are
// byte-identical (md5 of row::text) across the attempt. Deliberately NOT
// asserted: any A-replay-of-B's-id refusal — the caller-scoped identity makes
// A's row legitimately fresh (OC-4), so the refusal must be the grant-owner
// mismatch, never request_conflict.
package signer

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// crossAuthIDB is B's own separate grant: B's request row must sit on it so
// the shared-grant anchor UNIQUE (signing_requests_authorization_anchor_uniq,
// non-replacement rows) cannot intercept A's insert before the grant gate — A
// must reach EvaluateGrant to be refused as authorization_invalid.
const crossAuthIDB = "wa-gate-b"

// crossSeedGrant inserts a B-owned active grant in the same shape as
// gateSeedGrant (gates_integration_test.go), parameterized by caller so the
// non-target grant of the fixture is coherent.
func crossSeedGrant(t *testing.T, pool *pgxpool.Pool, callerID int64, authorizationID string) {
	t.Helper()
	gateExec(t, pool, `INSERT INTO caller (caller_id, can_create) VALUES ($1, TRUE)
		ON CONFLICT (caller_id) DO NOTHING`, callerID)
	gateExec(t, pool, `INSERT INTO withdrawal_authorizations
		(authorization_id, caller_id, chain_id, asset, recipient, amount, state, expires_at, supplied_by)
		VALUES ($1, $2, $3, $4, $5, $6, 'active', now() + interval '1 hour', 'cross-caller-test')
		ON CONFLICT (authorization_id) DO UPDATE
		SET state = 'active', expires_at = EXCLUDED.expires_at`,
		authorizationID, callerID, gateChainID, gateAsset, gateRecipient, gateAmount)
}

// crossRowDigest snapshots one row wholesale (md5 over its full text), so the
// before/after comparison is byte-identical and cannot miss a column.
func crossRowDigest(t *testing.T, pool *pgxpool.Pool, query string, args ...any) string {
	t.Helper()
	var digest string
	if err := pool.QueryRow(context.Background(), query, args...).Scan(&digest); err != nil {
		t.Fatalf("snapshot row (%s): %v", query, err)
	}
	return digest
}

// TestSignerSubmitCrossCallerGrantRefused drives A's credential against B's
// grant (A's body reusing B's signing_request_id) and proves the refusal is
// the caller-ownership mismatch with B's state untouched and A's own fresh row
// recorded.
func TestSignerSubmitCrossCallerGrantRefused(t *testing.T) {
	pool := signerAuthStartPG(t)
	ctx := context.Background()
	const callerA = int64(8101)

	credA, err := IssueCredential(ctx, pool, callerA, "cross-caller-a")
	if err != nil {
		t.Fatalf("IssueCredential(caller A): %v", err)
	}
	// Caller B is the 007 grant owner; B holds a credential too (unused), so
	// both real caller rows exist for the fixture.
	if _, err := IssueCredential(ctx, pool, gateCallerID, "cross-caller-b"); err != nil {
		t.Fatalf("IssueCredential(caller B): %v", err)
	}
	gateReset006(t, pool)
	gateSeedGrant(t, pool) // B owns the target grant gateAuthID

	// B's own request row: B's signing_request_id is the id A will present,
	// over B's separate grant (see crossAuthIDB) with a distinct attempt_id.
	bRowBody := strings.Replace(submitGrantBody(),
		`"authorization_id": "`+gateAuthID+`"`, `"authorization_id": "`+crossAuthIDB+`"`, 1)
	bRowBody = strings.Replace(bRowBody, `"attempt_id": "at-9c02"`, `"attempt_id": "at-cross-b"`, 1)
	crossSeedGrant(t, pool, gateCallerID, crossAuthIDB)
	bRowID := submitSeedRow(t, pool, gateCallerID, bRowBody, string(StateReceived))

	bRowBefore := crossRowDigest(t, pool,
		`SELECT md5(x::text) FROM signing_requests x WHERE id = $1`, bRowID)
	bGrantBefore := crossRowDigest(t, pool,
		`SELECT md5(x::text) FROM withdrawal_authorizations x WHERE authorization_id = $1`, gateAuthID)

	// When: A authenticates and names B's grant with B's request id.
	body := submitGrantBody()
	resp, err := Submit(ctx, submitTestDeps(t, pool), credA, []byte(body))
	if resp != nil {
		t.Fatalf("cross-caller submit returned a signature response: %+v", resp)
	}

	// Then: the caller-ownership refusal, 403, zero signatures.
	re := signerAuthRefusal(t, err, ClassAuthorizationInvalid)
	if got := signerPolicyHTTPStatus(re.Class); got != 403 {
		t.Fatalf("authorization_invalid maps to HTTP %d, want 403", got)
	}
	if got := signerAuthSignatureCount(t, pool); got != 0 {
		t.Fatalf("cross-caller attempt produced %d signature result(s), want 0", got)
	}

	// A's own fresh row (never B's): terminal rejected with the grant class.
	var state, refusalClass, authzID string
	if err := pool.QueryRow(ctx,
		`SELECT state, refusal_class, authorization_id FROM signing_requests
		  WHERE caller_id = $1 AND signing_request_id = $2`,
		callerA, "sr-7f3a").Scan(&state, &refusalClass, &authzID); err != nil {
		t.Fatalf("read caller A's row: %v", err)
	}
	if state != string(StateRejected) || refusalClass != string(ClassAuthorizationInvalid) || authzID != gateAuthID {
		t.Fatalf("caller A row = (state=%q class=%q authz=%q), want (rejected %s %s)",
			state, refusalClass, authzID, ClassAuthorizationInvalid, gateAuthID)
	}

	// The refusal audit is attributed to caller A, not to B's identity.
	var action, reason string
	var auditCaller int64
	if err := pool.QueryRow(ctx,
		`SELECT action, reason_class, caller_id FROM signing_request_audit
		  WHERE signing_request_id = $1 AND caller_id = $2 ORDER BY audit_id DESC LIMIT 1`,
		"sr-7f3a", callerA).Scan(&action, &reason, &auditCaller); err != nil {
		t.Fatalf("read caller A's refusal audit: %v", err)
	}
	if action != "authorization_refused" || reason != string(ClassAuthorizationInvalid) || auditCaller != callerA {
		t.Fatalf("audit = (action=%q reason=%q caller=%d), want (authorization_refused %s %d)",
			action, reason, auditCaller, ClassAuthorizationInvalid, callerA)
	}

	// B's request row and B's grant row are byte-identical: the FOR SHARE read
	// never writes upstream, and the refusal landed on A's identity only.
	if got := crossRowDigest(t, pool,
		`SELECT md5(x::text) FROM signing_requests x WHERE id = $1`, bRowID); got != bRowBefore {
		t.Fatalf("B's request row changed: before=%s after=%s", bRowBefore, got)
	}
	if got := crossRowDigest(t, pool,
		`SELECT md5(x::text) FROM withdrawal_authorizations x WHERE authorization_id = $1`, gateAuthID); got != bGrantBefore {
		t.Fatalf("B's grant row changed: before=%s after=%s", bGrantBefore, got)
	}
}
