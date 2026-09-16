//go:build integration

// replacement_integration_test.go owns spec task T021 for 009-signer-service:
// OC-5 conditional fee replacement on a real, isolated PostgreSQL (FR-05;
// contracts/gates.md §2 condition 4; research R7/R11; OC-4/OC-5).
//
// A fee replacement is a NEW attempt: new attempt_id + new signing_request_id
// over the same intent_id + binding_ref (R7/OC-4). Authorization use is
// conditional (OC-5):
//
//	branch 1 — the grant explicitly permits the fee-replacement purpose and the
//	  fee is in scope → the same authorization_id MAY be reused (still a new
//	  request identity; the original row is never modified/rebound);
//	branch 2 — otherwise → a fresh authorization with the new identity.
//
// PB-gate: the 007 scope/version carrier (PB-01, withdrawal_authorization_
// scopes) is not in this tree, so "explicitly permits" is not verifiable from
// the row — branch 1 is unreachable and a reuse that cannot be verified is
// refused (R7/R11). This pins the runnable acceptance now: the partial anchor
// index + "persisted rows are never rebound" carrier, the fresh-grant + new
// identity branch, and the mandatory fail-closed scopeless-grant behavior. The
// full legal reuse acceptance is retained for after the PB carrier lands.
// Nothing here is a legal-path sign-off.
package signer

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// repInsertRequestSQL writes one signing_requests row with replacement_of under
// caller control (submit.go cannot set it — Submit always inserts an anchor).
// A type-0 row mirrors the migration baseline so the insert exercises the
// carrier, not the fee-shape CHECK.
const repInsertRequestSQL = `INSERT INTO signing_requests
	(caller_id, signing_request_id, attempt_id, replacement_of, intent_id, binding_ref,
	 chain_id, sender, nonce, tx_type, to_addr, value, data, gas_limit, gas_price,
	 asset, recipient, amount, canonical_envelope, content_hash,
	 authorization_id, authorization_fingerprint, authorization_state, policy_version, state)
	VALUES ($1,$2,$3,$4,'pi-1b44','nb-1',
	 31337,$5,'42',0,$6,'0',$13,'65000','1',
	 $7,$8,'1000000','0x00',$9,
	 $10,$11,'active','policy-v1',$12)
	RETURNING id`

// repBody derives a fee-replacement body from the gate-aligned baseline
// (submitGrantBody): a new request/attempt identity (OC-4) over the same
// intent_id + binding_ref, with the fee raised in place (still within the
// policy cap) and the authorization named by the caller. Every other bound
// field is byte-identical, so the new envelope differs from the anchor's only
// where OC-4/OC-5 say it must.
func repBody(requestID, attemptID, authID string) string {
	b := submitGrantBody()
	b = strings.Replace(b, `"signing_request_id": "sr-7f3a"`, `"signing_request_id": "`+requestID+`"`, 1)
	b = strings.Replace(b, `"attempt_id": "at-9c02"`, `"attempt_id": "`+attemptID+`"`, 1)
	b = strings.Replace(b, `"authorization_id": "`+gateAuthID+`"`, `"authorization_id": "`+authID+`"`, 1)
	b = strings.Replace(b, `"max_fee_per_gas": "1500000000"`, `"max_fee_per_gas": "1800000000"`, 1)
	b = strings.Replace(b, `"max_priority_fee_per_gas": "1000000000"`, `"max_priority_fee_per_gas": "1200000000"`, 1)
	return b
}

// repSeedGrant seeds one 007 grant for the gate caller under a named
// authorization_id, so a fresh-grant replacement (branch 2) has a distinct
// grant to point at. Fields match the body baseline.
func repSeedGrant(t *testing.T, pool *pgxpool.Pool, authID string) {
	t.Helper()
	gateExec(t, pool, `INSERT INTO caller (caller_id, can_create) VALUES ($1, TRUE)
		ON CONFLICT (caller_id) DO NOTHING`, gateCallerID)
	gateExec(t, pool, `INSERT INTO withdrawal_authorizations
		(authorization_id, caller_id, chain_id, asset, recipient, amount, state, expires_at, supplied_by)
		VALUES ($1, $2, $3, $4, $5, $6, 'active', now() + interval '1 hour', 'rep-test')
		ON CONFLICT (authorization_id) DO NOTHING`,
		authID, gateCallerID, gateChainID, gateAsset, gateRecipient, gateAmount)
}

// repInsertRow writes one signing_requests row directly; replacementOf nil is
// an anchor, non-nil a replacement naming its predecessor. It returns the raw
// error so the anchor-index violation can be asserted on its named constraint.
func repInsertRow(t *testing.T, pool *pgxpool.Pool, callerID int64, requestID, attemptID, authID string, replacementOf *int64, state string) (int64, error) {
	t.Helper()
	var id int64
	err := pool.QueryRow(context.Background(), repInsertRequestSQL,
		callerID, requestID, attemptID, replacementOf,
		signerMigrationAddr("11"), signerMigrationAddr("aa"), signerMigrationAddr("aa"),
		signerMigrationAddr("bb"), signerMigrationHash("cc"), authID,
		signerMigrationFingerprint("dd"), state, []byte{},
	).Scan(&id)
	return id, err
}

// repRowFacts reads one persisted row's identity facts: row id, the grant it is
// bound to, and replacement_of (nil = anchor). The grant is read back to prove
// a persisted row is never rebound.
func repRowFacts(t *testing.T, pool *pgxpool.Pool, callerID int64, requestID string) (int64, string, *int64) {
	t.Helper()
	var id int64
	var authID string
	var replacementOf *int64
	if err := pool.QueryRow(context.Background(),
		`SELECT id, authorization_id, replacement_of FROM signing_requests
		  WHERE caller_id = $1 AND signing_request_id = $2`,
		callerID, requestID).Scan(&id, &authID, &replacementOf); err != nil {
		t.Fatalf("read persisted row %s: %v", requestID, err)
	}
	return id, authID, replacementOf
}

// repNonAnchorCount counts the anchor (non-replacement) requests bound to one
// authorization: the partial index's "at most one" claim.
func repNonAnchorCount(t *testing.T, pool *pgxpool.Pool, authID string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM signing_requests WHERE authorization_id = $1 AND replacement_of IS NULL`,
		authID).Scan(&n); err != nil {
		t.Fatalf("count anchor requests for %s: %v", authID, err)
	}
	return n
}

// repLastAudit reads the latest audit row for one request identity.
func repLastAudit(t *testing.T, pool *pgxpool.Pool, requestID string) (action, reason, detail string) {
	t.Helper()
	if err := pool.QueryRow(context.Background(),
		`SELECT action, reason_class, detail FROM signing_request_audit
		  WHERE signing_request_id = $1 ORDER BY audit_id DESC LIMIT 1`,
		requestID).Scan(&action, &reason, &detail); err != nil {
		t.Fatalf("read last audit for %s: %v", requestID, err)
	}
	return action, reason, detail
}

// TestSignerReplacementReuseRefusedWithoutVerifiablePermit is the branch-1
// fail-closed path: a replacement naming the anchor's grant is refused while no
// carrier can express "explicitly permits the fee-replacement purpose". The
// partial anchor index admits at most one non-replacement request per grant, so
// the unverifiable reuse lands on authorization_invalid (403, fresh-
// authorization instruction) — never a second signable object on the one grant
// — and the persisted anchor row is never rebound.
func TestSignerReplacementReuseRefusedWithoutVerifiablePermit(t *testing.T) {
	pool := signerAuthStartPG(t)
	ctx := context.Background()
	const callerID = int64(gateCallerID)

	cred, err := IssueCredential(ctx, pool, callerID, "rep-reuse")
	if err != nil {
		t.Fatalf("IssueCredential: %v", err)
	}
	gateReset006(t, pool)
	gateSeedGrant(t, pool)

	anchorID := submitSeedRow(t, pool, callerID, submitGrantBody(), string(StateReceived))

	// The replacement is a new identity (OC-4) over the same intent + binding,
	// reusing the anchor's grant without a verifiable explicit permit.
	reuseBody := repBody("sr-rep-reuse", "at-rep-reuse", gateAuthID)
	resp, err := Submit(ctx, submitTestDeps(t, pool), cred, []byte(reuseBody))
	if resp != nil {
		t.Fatalf("permit-less reuse returned a signature response: %+v", resp)
	}
	re := signerAuthRefusal(t, err, ClassAuthorizationInvalid)
	if got := signerPolicyHTTPStatus(re.Class); got != 403 {
		t.Fatalf("authorization_invalid maps to HTTP %d, want 403", got)
	}
	if got := signerAuthSignatureCount(t, pool); got != 0 {
		t.Fatalf("permit-less reuse produced %d signature result(s), want 0", got)
	}
	if got := submitRowCount(t, pool, callerID, "sr-rep-reuse"); got != 0 {
		t.Fatalf("refused reuse persisted %d rows for the new identity, want 0", got)
	}

	// "旧请求换绑禁止": the anchor keeps its id, identity and grant.
	id, authID, replacementOf := repRowFacts(t, pool, callerID, "sr-7f3a")
	if id != anchorID || authID != gateAuthID || replacementOf != nil {
		t.Fatalf("anchor rebound: id=%d auth=%s replacement_of=%v, want %d/%s/nil",
			id, authID, replacementOf, anchorID, gateAuthID)
	}
	if got := repNonAnchorCount(t, pool, gateAuthID); got != 1 {
		t.Fatalf("anchor requests on the grant = %d, want exactly 1", got)
	}
}

// TestSignerReplacementFreshGrantFailsClosedScopeless is OC-5 branch 2 (fresh
// authorization + new identity) under the PB gate, and the mandated explicit
// scopeless-grant → authorization_unverifiable behavior assertion: a
// replacement pointing at a fresh, active, field-matching grant is refused
// before any signature because the grant carries no verifiable scope/version
// (R11). The refusal is recorded (rejected row + authorization_refused audit)
// against the new identity with zero signatures.
func TestSignerReplacementFreshGrantFailsClosedScopeless(t *testing.T) {
	pool := signerAuthStartPG(t)
	ctx := context.Background()
	const callerID = int64(gateCallerID)

	cred, err := IssueCredential(ctx, pool, callerID, "rep-fresh")
	if err != nil {
		t.Fatalf("IssueCredential: %v", err)
	}
	gateReset006(t, pool)
	gateSeedGrant(t, pool)

	const freshAuthID = "wa-rep-fresh-2"
	repSeedGrant(t, pool, freshAuthID)

	// The original request is the anchor on the old grant and must stay put.
	anchorID := submitSeedRow(t, pool, callerID, submitGrantBody(), string(StateReceived))

	replacementBody := repBody("sr-rep-fresh", "at-rep-fresh", freshAuthID)
	resp, err := Submit(ctx, submitTestDeps(t, pool), cred, []byte(replacementBody))
	if resp != nil {
		t.Fatalf("scopeless fresh-grant replacement returned a signature response: %+v", resp)
	}
	re := signerAuthRefusal(t, err, ClassAuthorizationUnverifiable)
	if got := signerPolicyHTTPStatus(re.Class); got != 403 {
		t.Fatalf("authorization_unverifiable maps to HTTP %d, want 403", got)
	}
	if RetryabilityOf(re.Class) != RetryNever {
		t.Fatalf("authorization_unverifiable retryability = %v, want RetryNever (fail closed)", RetryabilityOf(re.Class))
	}
	if got := signerAuthSignatureCount(t, pool); got != 0 {
		t.Fatalf("scopeless fresh-grant replacement produced %d signature result(s), want 0", got)
	}

	// Refusal recorded on the NEW identity, pointing at the fresh grant (new
	// identity + fresh authorization — never a rebind of the anchor).
	var state, refusalClass, authzState, authzID string
	if err := pool.QueryRow(ctx,
		`SELECT state, refusal_class, authorization_state, authorization_id
		   FROM signing_requests WHERE caller_id = $1 AND signing_request_id = $2`,
		callerID, "sr-rep-fresh").Scan(&state, &refusalClass, &authzState, &authzID); err != nil {
		t.Fatalf("read persisted replacement row: %v", err)
	}
	if state != string(StateRejected) || refusalClass != string(ClassAuthorizationUnverifiable) {
		t.Fatalf("persisted replacement = %q/%q, want rejected/authorization_unverifiable", state, refusalClass)
	}
	if authzState != "active" || authzID != freshAuthID {
		t.Fatalf("persisted grant evidence = state %q id %q, want active/%s", authzState, authzID, freshAuthID)
	}

	action, reason, detail := repLastAudit(t, pool, "sr-rep-fresh")
	if action != "authorization_refused" || reason != string(ClassAuthorizationUnverifiable) || detail == "" {
		t.Fatalf("audit = %q/%q/%q, want authorization_refused/%s with a basis",
			action, reason, detail, ClassAuthorizationUnverifiable)
	}

	// The anchor row on the old grant is untouched and never rebound.
	id, authID, replacementOf := repRowFacts(t, pool, callerID, "sr-7f3a")
	if id != anchorID || authID != gateAuthID || replacementOf != nil {
		t.Fatalf("anchor row changed: id=%d auth=%s replacement_of=%v, want %d/%s/nil",
			id, authID, replacementOf, anchorID, gateAuthID)
	}
}

// TestSignerReplacementAnchorIndexAllowsReplacementReuse pins the OC-5 carrier
// (R7): the partial anchor index keeps exactly one NON-replacement request per
// authorization while a replacement row (replacement_of set) may share the
// anchor's grant. A replacement is always a new row/identity — the persisted
// anchor's authorization_id is never updated ("旧请求换绑禁止") — and a second
// non-replacement request on the same grant is rejected by the named index.
func TestSignerReplacementAnchorIndexAllowsReplacementReuse(t *testing.T) {
	pool := signerAuthStartPG(t)
	ctx := context.Background()
	const callerID = int64(8403)

	if _, err := IssueCredential(ctx, pool, callerID, "rep-anchor"); err != nil {
		t.Fatalf("IssueCredential: %v", err)
	}

	anchorID, err := repInsertRow(t, pool, callerID, "sr-rep-anchor", "at-rep-anchor", gateAuthID, nil, string(StateReceived))
	if err != nil {
		t.Fatalf("seed anchor request: %v", err)
	}

	// A replacement row may name the same grant: the index only guards
	// replacement_of IS NULL.
	replacementID, err := repInsertRow(t, pool, callerID, "sr-rep-child", "at-rep-child", gateAuthID, &anchorID, string(StateReceived))
	if err != nil {
		t.Fatalf("replacement row sharing the anchor grant was rejected: %v", err)
	}
	if replacementID == anchorID {
		t.Fatalf("replacement reused the anchor row id %d; a replacement is a new row/identity", anchorID)
	}

	// The anchor's grant is shared, not rebound.
	gotAnchorID, anchorAuth, anchorReplacementOf := repRowFacts(t, pool, callerID, "sr-rep-anchor")
	if gotAnchorID != anchorID || anchorAuth != gateAuthID || anchorReplacementOf != nil {
		t.Fatalf("anchor rebound: id=%d auth=%s replacement_of=%v, want %d/%s/nil",
			gotAnchorID, anchorAuth, anchorReplacementOf, anchorID, gateAuthID)
	}
	gotReplID, replAuth, replReplacementOf := repRowFacts(t, pool, callerID, "sr-rep-child")
	if gotReplID != replacementID || replAuth != gateAuthID {
		t.Fatalf("replacement row = id=%d auth=%s, want %d/%s", gotReplID, replAuth, replacementID, gateAuthID)
	}
	if replReplacementOf == nil || *replReplacementOf != anchorID {
		t.Fatalf("replacement replacement_of = %v, want %d", replReplacementOf, anchorID)
	}
	if got := repNonAnchorCount(t, pool, gateAuthID); got != 1 {
		t.Fatalf("anchor requests on the grant = %d, want exactly 1", got)
	}

	// A second non-replacement request on the same grant is rejected by the
	// named partial index (the submit path classifies it authorization_invalid).
	_, err = repInsertRow(t, pool, callerID, "sr-rep-anchor-2", "at-rep-anchor-2", gateAuthID, nil, string(StateReceived))
	signerMigrationWantPgError(t, err, "23505", "signing_requests_authorization_anchor_uniq")
}
