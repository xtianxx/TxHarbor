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
// H3/T038 threads the reusable branch: submit.go writes replacement_of = the
// anchor row id when a fee replacement names its anchor's grant, and admits the
// reuse only when the scope explicitly permits the fee-replacement purpose and
// the raised fee stays in range (PB-FR-05, R7/R11); otherwise a fresh
// authorization + PB re-issue is required and the reuse is refused with a
// recorded refusal. The still-hardcoded scopeless refusal (T039) refuses every
// submission at this tip, so the permitted branch's sign-off is the joint
// legal-path case (T040); the admission itself is a persistence fact asserted
// here. Nothing here is a legal-path sign-off.
package signer

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// repInsertRequestSQL writes one signing_requests row with replacement_of under
// caller control, for the carrier/index assertions that need an anchor and a
// replacement side by side without the Submit path. A type-0 row mirrors the
// migration baseline so the insert exercises the carrier, not the fee-shape
// CHECK.
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

// repSeedScope inserts the PB carrier row for a grant with the given purpose
// token and fee caps. Identity columns derive from the anchor body so the row
// stays coherent; the H3/T038 gate consumes the purpose token + fee caps.
func repSeedScope(t *testing.T, pool *pgxpool.Pool, authID string, allows bool, feeMaxTotal, feeMaxPerGas, feeMaxPriority int64) {
	t.Helper()
	req := mustDecode(t, submitGrantBody())
	gateExec(t, pool, `INSERT INTO withdrawal_authorization_scopes
		(authorization_id, intent_id, request_id, sender, fee_max_total, fee_max_per_gas,
		 fee_max_priority, allows_fee_replacement, authorization_version, attested_by)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 1, 'rep-test')
		ON CONFLICT (authorization_id) DO UPDATE SET
		  allows_fee_replacement = EXCLUDED.allows_fee_replacement,
		  fee_max_total = EXCLUDED.fee_max_total,
		  fee_max_per_gas = EXCLUDED.fee_max_per_gas,
		  fee_max_priority = EXCLUDED.fee_max_priority`,
		authID, req.IntentID, req.SigningRequestID, strings.ToLower(req.Sender),
		feeMaxTotal, feeMaxPerGas, feeMaxPriority, allows)
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

// repWantRecordedReuseRefusal asserts the forbidden-reuse outcome: the new
// identity is persisted as a rejected replacement of the anchor under the
// anchor's grant, with the authorization_refused audit naming the anchor —
// never a second non-replacement row, never a rebind of the anchor.
func repWantRecordedReuseRefusal(t *testing.T, pool *pgxpool.Pool, callerID int64, requestID string, anchorID int64, authID string) {
	t.Helper()
	var state, class, rowAuth string
	var replacementOf *int64
	if err := pool.QueryRow(context.Background(),
		`SELECT state, refusal_class, authorization_id, replacement_of FROM signing_requests
		  WHERE caller_id = $1 AND signing_request_id = $2`,
		callerID, requestID).Scan(&state, &class, &rowAuth, &replacementOf); err != nil {
		t.Fatalf("read persisted refusal %s: %v", requestID, err)
	}
	if state != string(StateRejected) || class != string(ClassAuthorizationInvalid) {
		t.Fatalf("%s persisted %q/%q, want rejected/%s", requestID, state, class, ClassAuthorizationInvalid)
	}
	if rowAuth != authID || replacementOf == nil || *replacementOf != anchorID {
		t.Fatalf("%s bound to auth=%s replacement_of=%v, want %s/%d", requestID, rowAuth, replacementOf, authID, anchorID)
	}
	action, reason, detail := repLastAudit(t, pool, requestID)
	if action != "authorization_refused" || reason != string(ClassAuthorizationInvalid) ||
		!strings.Contains(detail, "anchor="+strconv.FormatInt(anchorID, 10)) {
		t.Fatalf("%s audit = %q/%q/%q, want authorization_refused/%s naming anchor %d",
			requestID, action, reason, detail, ClassAuthorizationInvalid, anchorID)
	}
}

// TestSignerReplacementReuseRefusedWithoutVerifiablePermit is the branch-1
// fail-closed path: a replacement naming the anchor's grant is refused while no
// carrier can express "explicitly permits the fee-replacement purpose". The
// partial anchor index admits at most one non-replacement request per grant, so
// the unverifiable reuse lands on authorization_invalid (403, fresh-
// authorization instruction) — never a second signable object on the one grant
// — and the persisted anchor row is never rebound. H3/T038 records that
// refusal on the new identity (a rejected replacement row + audit naming the
// anchor) instead of aborting at the index.
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
	// H3/T038: the refusal is recorded on the new identity as a rejected
	// replacement of the anchor — the reuse gate refuses before any signature,
	// and the anchor index never sees a second non-replacement row.
	repWantRecordedReuseRefusal(t, pool, callerID, "sr-rep-reuse", anchorID, gateAuthID)

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

// TestSignerReplacementReuseRefusedByPurposeAndFeeGate pins PB-FR-05's two
// refusal halves with the carrier present: a scope whose fee-replacement
// purpose token is off refuses the reuse (fresh authorization required), and a
// scope that permits the purpose but caps the fee below the replacement's raise
// refuses it too. Both refusals are recorded on the new identity with zero
// signatures; the anchor row and the index are untouched.
func TestSignerReplacementReuseRefusedByPurposeAndFeeGate(t *testing.T) {
	pool := signerAuthStartPG(t)
	ctx := context.Background()
	const callerID = int64(gateCallerID)

	cred, err := IssueCredential(ctx, pool, callerID, "rep-gate")
	if err != nil {
		t.Fatalf("IssueCredential: %v", err)
	}
	gateReset006(t, pool)
	gateSeedGrant(t, pool)

	anchorID := submitSeedRow(t, pool, callerID, submitGrantBody(), string(StateReceived))

	// Purpose token off (caps cover the raised fee): the reuse must refuse.
	repSeedScope(t, pool, gateAuthID, false, 117000000000000, 1800000000, 1200000000)
	body := repBody("sr-rep-purpose", "at-rep-purpose", gateAuthID)
	resp, err := Submit(ctx, submitTestDeps(t, pool), cred, []byte(body))
	if resp != nil {
		t.Fatalf("purpose-denied reuse returned a signature response: %+v", resp)
	}
	re := signerAuthRefusal(t, err, ClassAuthorizationInvalid)
	if got := signerPolicyHTTPStatus(re.Class); got != 403 {
		t.Fatalf("authorization_invalid maps to HTTP %d, want 403", got)
	}
	if got := signerAuthSignatureCount(t, pool); got != 0 {
		t.Fatalf("purpose-denied reuse produced %d signature result(s), want 0", got)
	}
	repWantRecordedReuseRefusal(t, pool, callerID, "sr-rep-purpose", anchorID, gateAuthID)

	// Purpose permitted but the per-gas cap sits below the replacement's raise
	// (1.5 gwei cap vs 1.8 gwei request): the fee half refuses.
	gateExec(t, pool, `UPDATE withdrawal_authorization_scopes
		SET allows_fee_replacement = TRUE, fee_max_per_gas = 1500000000
		WHERE authorization_id = $1`, gateAuthID)
	body = repBody("sr-rep-feecap", "at-rep-feecap", gateAuthID)
	resp, err = Submit(ctx, submitTestDeps(t, pool), cred, []byte(body))
	if resp != nil {
		t.Fatalf("out-of-range-fee reuse returned a signature response: %+v", resp)
	}
	signerAuthRefusal(t, err, ClassAuthorizationInvalid)
	if got := signerAuthSignatureCount(t, pool); got != 0 {
		t.Fatalf("out-of-range-fee reuse produced %d signature result(s), want 0", got)
	}
	repWantRecordedReuseRefusal(t, pool, callerID, "sr-rep-feecap", anchorID, gateAuthID)

	// Both refused identities are replacements: exactly one non-replacement row
	// on the grant, and the anchor itself is never rebound.
	if got := repNonAnchorCount(t, pool, gateAuthID); got != 1 {
		t.Fatalf("anchor requests on the grant = %d, want exactly 1", got)
	}
	id, authID, replacementOf := repRowFacts(t, pool, callerID, "sr-7f3a")
	if id != anchorID || authID != gateAuthID || replacementOf != nil {
		t.Fatalf("anchor rebound: id=%d auth=%s replacement_of=%v, want %d/%s/nil",
			id, authID, replacementOf, anchorID, gateAuthID)
	}
}

// TestSignerReplacementPermittedScopeAdmitsReuse is OC-5 branch 1's admission
// (H3/T038): with a scope that explicitly permits the fee-replacement purpose
// and covers the raised fee, the fee replacement is persisted as a replacement
// row under the anchor's grant — a new identity, the anchor partial-unique
// keeping exactly one non-replacement row, the anchor never rebound, and no
// reuse-gate ("fresh authorization required") refusal recorded. The outcome
// past this admission is the joint legal-path case (T040): the still-hardcoded
// scopeless refusal (T039) governs it at this tip, so no signature is asserted
// here.
func TestSignerReplacementPermittedScopeAdmitsReuse(t *testing.T) {
	pool := signerAuthStartPG(t)
	ctx := context.Background()
	const callerID = int64(gateCallerID)

	cred, err := IssueCredential(ctx, pool, callerID, "rep-permit")
	if err != nil {
		t.Fatalf("IssueCredential: %v", err)
	}
	gateReset006(t, pool)
	gateSeedGrant(t, pool)

	anchorID := submitSeedRow(t, pool, callerID, submitGrantBody(), string(StateReceived))

	// Caps cover the raised fee: 65000 gas × 1.8 gwei max fee, 1.2 gwei tip.
	repSeedScope(t, pool, gateAuthID, true, 117000000000000, 1800000000, 1200000000)

	body := repBody("sr-rep-permit", "at-rep-permit", gateAuthID)
	_, _ = Submit(ctx, submitTestDeps(t, pool), cred, []byte(body))

	// The reuse was admitted: the new identity is bound as a replacement of the
	// anchor under the same grant, not refused with the fresh-authorization
	// instruction.
	rowID, rowAuth, replacementOf := repRowFacts(t, pool, callerID, "sr-rep-permit")
	if rowID == anchorID {
		t.Fatalf("reuse reused the anchor row id %d; a replacement is a new row/identity", anchorID)
	}
	if rowAuth != gateAuthID || replacementOf == nil || *replacementOf != anchorID {
		t.Fatalf("permitted reuse bound to auth=%s replacement_of=%v, want %s/%d", rowAuth, replacementOf, gateAuthID, anchorID)
	}
	var reuseRefusals int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM signing_request_audit
		  WHERE signing_request_id = $1 AND detail LIKE '%fresh_authorization_required%'`,
		"sr-rep-permit").Scan(&reuseRefusals); err != nil {
		t.Fatalf("count reuse-gate refusals: %v", err)
	}
	if reuseRefusals != 0 {
		t.Fatalf("permitted reuse recorded %d reuse-gate refusal(s), want 0", reuseRefusals)
	}

	// The anchor keeps its id, identity and grant; the index keeps exactly one
	// non-replacement row on the grant.
	id, authID, anchorReplacementOf := repRowFacts(t, pool, callerID, "sr-7f3a")
	if id != anchorID || authID != gateAuthID || anchorReplacementOf != nil {
		t.Fatalf("anchor rebound: id=%d auth=%s replacement_of=%v, want %d/%s/nil",
			id, authID, anchorReplacementOf, anchorID, gateAuthID)
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
