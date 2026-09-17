//go:build integration

// binding_integration_test.go owns spec task T019 for 009-signer-service: the
// V4 binding/conflict/determinism integration test on a real, isolated
// PostgreSQL (quickstart V4; FR-13/FR-14/FR-15; contracts/persistence.md §3;
// SC-03). It pins the retry-determinism table against durable state:
//
//   - same identity + same envelope, retried sequentially after a committed-
//     but-response-lost result and then N≥2 times concurrently → exactly one
//     persisted result and exactly one observable signature: signature_results
//     rows == 1 AND exactly one distinct signature payload (persistence §3 hard
//     rule; §1 invariant 3 "no second observable signature");
//   - same identity + different envelope → request_conflict (api.md §2: 409),
//     the original row/result untouched, zero second signature;
//   - concurrent first-receipts of one never-seen identity → exactly one
//     signing_requests row via the insert-first 23505 classify + replay path,
//     never a duplicate identity or a signature.
//
// "Same identity" is the persistence.md §3 definition: the same caller +
// signing_request_id, with the request content bound by the persisted
// canonical_envelope that the different-envelope branch must mismatch.
//
// PB-gate: the fixtures here seed scopeless stock grants, so submit refuses
// authorization_unverifiable from the observed carrier read (H4/T039,
// PB-FR-04). The post-COMMIT durable state is therefore reached the same way
// T016's replay surface reaches it — by seeding the committed row + result that
// the contract's retry determinism is defined against. Nothing here is a
// legal-path sign-off.
package signer

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// bindConcurrency is the N of persistence §3's "N≥2 concurrent" determinism
// claim.
const bindConcurrency = 6

// bindDeps builds one independent submit collaborator set per concurrent
// caller. The 008 contract-shape double mutates per read
// (gateBindingDouble.calls), so a shared deps would race under -race; each
// caller gets its own pool-backed set. Built on the test goroutine so
// submitTestDeps' t.Fatalf never runs off it.
func bindDeps(t *testing.T, pool *pgxpool.Pool, n int) []SubmitDeps {
	t.Helper()
	deps := make([]SubmitDeps, n)
	for i := range deps {
		deps[i] = submitTestDeps(t, pool)
	}
	return deps
}

// bindConverge fires one same-identity Submit per deps entry concurrently and
// returns the per-caller outcome. Each caller writes only its own slice slot.
func bindConverge(ctx context.Context, deps []SubmitDeps, cred string, body []byte) ([]*SubmitResponse, []error) {
	resps := make([]*SubmitResponse, len(deps))
	errs := make([]error, len(deps))
	var wg sync.WaitGroup
	for i := range deps {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resps[i], errs[i] = Submit(ctx, deps[i], cred, body)
		}(i)
	}
	wg.Wait()
	return resps, errs
}

// bindSeedCommittedResult writes the durable post-COMMIT state of a submit
// whose response was lost: the request row is `signed` and the signature_results
// row is committed. The caller never saw that response — exactly persistence
// §3's "Row + result, signed (crash after COMMIT, response lost)" retry row.
func bindSeedCommittedResult(t *testing.T, pool *pgxpool.Pool, callerID int64, body string) (rowID int64, signature, txHash string) {
	t.Helper()
	rowID = submitSeedRow(t, pool, callerID, body, string(StateSigned))
	signature, txHash = signerMigrationSignature("ab"), signerMigrationHash("e1")
	gateExec(t, pool, `INSERT INTO signature_results (signing_request_row, signature, tx_hash) VALUES ($1, $2, $3)`,
		rowID, signature, txHash)
	return rowID, signature, txHash
}

// bindDistinctSignatureCount counts distinct persisted signature payloads: the
// "exactly one observable signature" half of the determinism claim. A distinct
// value here means a second signature was produced.
func bindDistinctSignatureCount(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(DISTINCT signature) FROM signature_results`).Scan(&n); err != nil {
		t.Fatalf("count distinct signature_results.signature: %v", err)
	}
	return n
}

// bindHTTPStatus is the api.md §2 status for the classes T019 asserts. The
// shared V5 helper maps the out-of-policy refusals only and predates the
// conflict class; api.md §2 fixes same-identity/different-envelope at 409, so
// pin it here rather than widen a sibling slice's helper.
func bindHTTPStatus(class RefusalClass) int {
	if class == ClassRequestConflict {
		return 409
	}
	return signerPolicyHTTPStatus(class)
}

// TestSignerBindingSameContentOneSignature is T019's determinism core: a
// committed-but-response-lost result is retried (sequentially, then N≥2
// concurrently) with the same identity + same content and converges on the one
// persisted result — never a second signature. Identity = same caller + same
// signing_request_id + same canonical_envelope (persistence §3).
func TestSignerBindingSameContentOneSignature(t *testing.T) {
	pool := signerAuthStartPG(t)
	ctx := context.Background()
	const callerID = int64(7101)

	cred, err := IssueCredential(ctx, pool, callerID, "bind-determinism")
	if err != nil {
		t.Fatalf("IssueCredential: %v", err)
	}
	body := submitGrantBody()

	_, signature, txHash := bindSeedCommittedResult(t, pool, callerID, body)
	if got := submitRowCount(t, pool, callerID, "sr-7f3a"); got != 1 {
		t.Fatalf("seeded identity rows = %d, want 1", got)
	}

	// Response loss: the first submit COMMITted the result, so a same-identity
	// same-content retry returns that persisted result without re-signing. The
	// returned response stands in for the bytes the caller never received.
	retry, err := Submit(ctx, submitTestDeps(t, pool), cred, []byte(body))
	if err != nil {
		t.Fatalf("retry of a signed identity: %v", err)
	}
	if retry.Signature != signature || retry.TxHash != txHash {
		t.Fatalf("retry = %q/%q, want the persisted %q/%q", retry.Signature, retry.TxHash, signature, txHash)
	}

	// Concurrent same-identity same-content retries converge on that one result.
	resps, errs := bindConverge(ctx, bindDeps(t, pool, bindConcurrency), cred, []byte(body))
	for i := range resps {
		if errs[i] != nil {
			t.Fatalf("concurrent retry %d: %v", i, errs[i])
		}
		if resps[i].Signature != signature || resps[i].TxHash != txHash {
			t.Fatalf("concurrent retry %d = %q/%q, want the persisted %q/%q",
				i, resps[i].Signature, resps[i].TxHash, signature, txHash)
		}
	}

	if got := submitRowCount(t, pool, callerID, "sr-7f3a"); got != 1 {
		t.Fatalf("same-identity retries created %d identity rows, want 1", got)
	}
	if got := signerAuthSignatureCount(t, pool); got != 1 {
		t.Fatalf("signature_results rows = %d, want exactly 1", got)
	}
	if got := bindDistinctSignatureCount(t, pool); got != 1 {
		t.Fatalf("distinct signature payloads = %d, want exactly 1", got)
	}
}

// TestSignerBindingSameIdentityDifferentContentConflict is T019's conflict
// branch: the same caller + signing_request_id with a changed envelope is a
// request_conflict (api.md §2: 409), the original row/result stay untouched,
// and the second signature count is zero.
func TestSignerBindingSameIdentityDifferentContentConflict(t *testing.T) {
	pool := signerAuthStartPG(t)
	ctx := context.Background()
	const callerID = int64(7202)

	cred, err := IssueCredential(ctx, pool, callerID, "bind-conflict")
	if err != nil {
		t.Fatalf("IssueCredential: %v", err)
	}
	body := submitGrantBody()

	rowID, signature, _ := bindSeedCommittedResult(t, pool, callerID, body)
	before := signerAuthSignatureCount(t, pool)

	// Same identity, one bound field changed (nonce 42→43) → a different
	// canonical_envelope over the same caller+signing_request_id.
	changed := strings.Replace(body, `"nonce": "42"`, `"nonce": "43"`, 1)
	_, err = Submit(ctx, submitTestDeps(t, pool), cred, []byte(changed))
	re := signerAuthRefusal(t, err, ClassRequestConflict)
	if got := bindHTTPStatus(re.Class); got != 409 {
		t.Fatalf("request_conflict maps to HTTP %d, want 409 (api.md §2)", got)
	}

	// Second signature count 0: the conflict produced no new result, and the
	// original persisted result is untouched.
	if got := signerAuthSignatureCount(t, pool) - before; got != 0 {
		t.Fatalf("conflict produced %d new signature result(s), want 0", got)
	}
	if got := signerAuthSignatureCount(t, pool); got != 1 {
		t.Fatalf("signature_results rows = %d, want the original 1", got)
	}
	if got := bindDistinctSignatureCount(t, pool); got != 1 {
		t.Fatalf("distinct signature payloads = %d, want the original 1", got)
	}
	if got := submitRowCount(t, pool, callerID, "sr-7f3a"); got != 1 {
		t.Fatalf("conflict created %d identity rows, want the original 1", got)
	}

	var persistedNonce, persistedSignature string
	if err := pool.QueryRow(ctx, `SELECT nonce::text FROM signing_requests WHERE id = $1`, rowID).Scan(&persistedNonce); err != nil {
		t.Fatalf("re-read original row: %v", err)
	}
	if persistedNonce != "42" {
		t.Fatalf("conflict rewrote the original row: nonce = %q, want 42", persistedNonce)
	}
	if err := pool.QueryRow(ctx, `SELECT signature FROM signature_results WHERE signing_request_row = $1`, rowID).Scan(&persistedSignature); err != nil {
		t.Fatalf("re-read original result: %v", err)
	}
	if persistedSignature != signature {
		t.Fatalf("conflict rewrote the persisted result: %q, want %q", persistedSignature, signature)
	}
}

// TestSignerBindingConcurrentFirstReceiptOneIdentity is T019's concurrent
// first-receipt branch: N callers submit one never-seen identity at once. The
// insert-first 23505 classify lets exactly one durable identity row exist; the
// losers replay it and converge on the same refusal class. Under the PB gate
// the legal path cannot sign, so the deterministic outcome is that one class
// with zero signatures — never a duplicate row, never a second identity.
func TestSignerBindingConcurrentFirstReceiptOneIdentity(t *testing.T) {
	pool := signerAuthStartPG(t)
	ctx := context.Background()
	const callerID = int64(gateCallerID)

	cred, err := IssueCredential(ctx, pool, callerID, "bind-first-receipt")
	if err != nil {
		t.Fatalf("IssueCredential: %v", err)
	}
	gateReset006(t, pool)
	gateSeedGrant(t, pool)
	body := submitGrantBody()

	if got := submitRowCount(t, pool, callerID, "sr-7f3a"); got != 0 {
		t.Fatalf("identity pre-exists: %d rows, want 0", got)
	}

	resps, errs := bindConverge(ctx, bindDeps(t, pool, bindConcurrency), cred, []byte(body))
	for i := range resps {
		if resps[i] != nil {
			t.Fatalf("caller %d got a signature response on the gated path: %+v", i, resps[i])
		}
		// Every caller — the insert winner and each 23505 loser replaying the
		// committed refusal — converges on the same class.
		re := signerAuthRefusal(t, errs[i], ClassAuthorizationUnverifiable)
		if got := bindHTTPStatus(re.Class); got != 403 {
			t.Fatalf("caller %d status = %d, want 403", i, got)
		}
	}

	if got := submitRowCount(t, pool, callerID, "sr-7f3a"); got != 1 {
		t.Fatalf("concurrent first-receipts created %d identity rows, want exactly 1", got)
	}
	if got := signerAuthSignatureCount(t, pool); got != 0 {
		t.Fatalf("gated concurrent submit produced %d signature result(s), want 0", got)
	}
}
