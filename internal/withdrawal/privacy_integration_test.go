//go:build integration

// privacy_integration_test.go owns T017 (US2): the V9 cross-caller privacy
// integration test on a real PostgreSQL 18 container (testcontainers).
//
// V9 is the ownership-enforced read contract (contracts/api.md §1 GET, FR-15):
// caller B probing caller A's real request_id and caller B probing an id that
// never existed MUST receive the same `not_found` code/message/shape — the
// response must not reveal whether the id exists, and must leak no
// caller/amount/asset fact.
//
// The byte-equality asserted here is at the library level: both probes return a
// *Error with identical (Code, Error() string). HTTP responses additionally
// carry a per-request trace id by design (contracts/api.md §1 "request trace
// id"), so independent requests legitimately carry different trace ids; the
// contract normalizes code/message/shape, not the trace id. This test compares
// exactly those normalized fields and nothing request-scoped.
//
// It reuses the T006 container/migration helper (grantSetup) and seeds through
// the reviewed paths only — IssueKey for credentials, SupplyGrant +
// SubmitWithdrawal for the request — never a raw INSERT. Its helpers are
// privacy-prefixed so no name collides with the other T00x test files.
package withdrawal

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// privacyIssueKey issues one credential for callerID through the reviewed
// IssueKey path and returns its plaintext.
func privacyIssueKey(t *testing.T, ctx context.Context, pool *pgxpool.Pool, callerID int64) string {
	t.Helper()
	plaintext, _, err := IssueKey(ctx, pool, callerID, "privacy-test")
	if err != nil {
		t.Fatalf("IssueKey(caller %d): %v", callerID, err)
	}
	return plaintext
}

// privacySeedRequest supplies one active grant bound to callerID through the
// reviewed SupplyGrant path, then creates the request through the reviewed
// SubmitWithdrawal path and returns its request_id. No row is inserted raw, so
// the seed exercises the same reviewed path production traffic does.
func privacySeedRequest(t *testing.T, ctx context.Context, pool *pgxpool.Pool, presented, authorizationID string, callerID int64) string {
	t.Helper()
	op := grantTestOp("seed-"+authorizationID, authorizationID, callerID, intakeAmount)
	out, err := SupplyGrant(ctx, pool, op, "privacy-test", "seed grant")
	if err != nil {
		t.Fatalf("SupplyGrant(%s): %v", authorizationID, err)
	}
	if out.Action != grantOutcomeSupplied {
		t.Fatalf("SupplyGrant(%s) action = %s, want %s", authorizationID, out.Action, grantOutcomeSupplied)
	}
	res, err := SubmitWithdrawal(ctx, pool, intakeReq(presented, "idem-"+authorizationID, authorizationID))
	if err != nil {
		t.Fatalf("SubmitWithdrawal(%s): %v", authorizationID, err)
	}
	if res.Status != 201 {
		t.Fatalf("SubmitWithdrawal(%s) status = %d (%+v), want 201", authorizationID, res.Status, res)
	}
	return res.RequestID
}

// privacyWantNotFound asserts err is a *Error whose Code is CodeNotFound.
func privacyWantNotFound(t *testing.T, err error) {
	t.Helper()
	var e *Error
	if !errors.As(err, &e) {
		t.Fatalf("error = %v (%T), want *Error code %q", err, err, CodeNotFound)
	}
	if e.Code != CodeNotFound {
		t.Fatalf("error code = %q, want %q (%v)", e.Code, CodeNotFound, e)
	}
}

// TestWithdrawalPrivacyCrossCallerNotFound is T017/V9: a valid caller B probing
// caller A's real request_id receives the byte-identical not_found error it
// receives for a random nonexistent id, and no request view.
func TestWithdrawalPrivacyCrossCallerNotFound(t *testing.T) {
	// Given caller A (valid key + active grant) whose request is created through
	// the reviewed SubmitWithdrawal path, and caller B with a valid key for a
	// different caller_id.
	ctx, pool := grantSetup(t)
	const (
		callerA = int64(7601)
		callerB = int64(7602)
	)
	keyA := privacyIssueKey(t, ctx, pool, callerA)
	keyB := privacyIssueKey(t, ctx, pool, callerB)
	if keyA == keyB {
		t.Fatal("keys A and B are identical, want distinct credentials")
	}
	requestID := privacySeedRequest(t, ctx, pool, keyA, "auth-privacy-a", callerA)

	// The random id is well-formed (wr- + 32 hex) yet never created.
	randomID := "wr-" + strings.Repeat("0", 32)

	// When caller B queries A's real request_id and the random nonexistent id.
	foreignView, foreignErr := GetWithdrawal(ctx, pool, callerB, requestID)
	randomView, randomErr := GetWithdrawal(ctx, pool, callerB, randomID)

	// Then both probes are the same not_found code ...
	privacyWantNotFound(t, foreignErr)
	privacyWantNotFound(t, randomErr)

	// ... byte-identical in the normalized fields: same Error() message/shape.
	// (Library-level equality; HTTP trace ids differ per request by design and
	// are deliberately excluded from this comparison.)
	if foreignErr.Error() != randomErr.Error() {
		t.Fatalf("foreign error %q differs from random error %q (existence leakage)",
			foreignErr.Error(), randomErr.Error())
	}

	// ... and B's probe leaks no view: no caller, amount, or asset is returned.
	if foreignView != nil || randomView != nil {
		t.Fatalf("views = %+v/%+v, want nil (error-only, no field leakage)", foreignView, randomView)
	}

	// The owner can still read the row B was denied, proving the foreign probe
	// hit a real request rather than an absent one.
	if _, err := GetWithdrawal(ctx, pool, callerA, requestID); err != nil {
		t.Fatalf("owner read after B's 404 probes: %v (row must exist)", err)
	}
}
