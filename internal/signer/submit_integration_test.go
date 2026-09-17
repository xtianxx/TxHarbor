//go:build integration

// submit_integration_test.go owns spec tasks T014 (V1 happy path) and the
// T016 replay surface for 009-signer-service on a real, isolated PostgreSQL
// (quickstart V1; FR-01/FR-03/FR-13/FR-14/FR-15/FR-16; contracts/api.md §4,
// persistence §§1/3).
//
// PB-gate: the legal happy path (compliant request → persisted signature+hash)
// requires a present-and-verifiable scope on the 007 grant. The fixture body
// targets a scopeless stock grant, so the runnable V1 branch here is the
// fail-closed one: a fully compliant request with a matching 008 binding and an
// active 007 grant is refused `authorization_unverifiable` (403) by the
// observed carrier read (H4/T039, PB-FR-04) — persist-first evidence (row +
// audit) proves the submit transaction ran its gates before any signing. No
// legal-path sign-off is claimed here; the replay surface is exercised by
// seeding durable rows directly, which is exactly the state the contract's
// retry determinism (persistence §3) is defined against.
package signer

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5/pgxpool"
)

// submitGrantBody is validBody aligned with the 007 grant gateSeedGrant seeds
// (authorization_id gateAuthID) and the clean 006 version (0). The PB carrier
// is absent, so this compliant body must still fail closed.
func submitGrantBody() string {
	b := strings.Replace(validBody(), `"recovery_version": 12`, `"recovery_version": 0`, 1)
	return strings.Replace(b, `"authorization_id": "wa-1"`, `"authorization_id": "`+gateAuthID+`"`, 1)
}

// submitTestDeps builds the submit collaborators over a dev-test-key provider
// and the contract-shape 008 binding double returning BindingMatches for
// validBody's intent. The provider is never reached on the fail-closed branch;
// it exists so a regression that reaches signing fails on a real sign, not a
// nil dereference.
func submitTestDeps(t *testing.T, pool *pgxpool.Pool) SubmitDeps {
	t.Helper()
	hexKey, _ := devKeyHex(t)
	provider, err := NewDevKeyProvider(ModeDevelopment, writeTestKey(t, hexKey), 0)
	if err != nil {
		t.Fatalf("NewDevKeyProvider: %v", err)
	}
	return SubmitDeps{
		DB:        pool,
		Policy:    testPolicy(t),
		Provider:  provider,
		Binding:   &gateBindingDouble{results: map[string]BindingResult{"pi-1b44": BindingMatches}},
		ScopeLock: &dlvScope{},
	}
}

// submitSeedRequestSQL seeds one signing_requests row from a decoded body so a
// later Submit of the same identity exercises the replay path without needing
// the (unreachable) legal signing path. canonical_envelope/content_hash are
// computed the same way Submit computes them, so equality is exact.
const submitSeedRequestSQL = `INSERT INTO signing_requests
	(caller_id, signing_request_id, attempt_id, intent_id, binding_ref, recovery_version,
	 chain_id, sender, nonce, tx_type, to_addr, value, data, gas_limit,
	 max_fee_per_gas, max_priority_fee_per_gas, asset, recipient, amount,
	 canonical_envelope, content_hash, authorization_id, authorization_fingerprint,
	 authorization_state, policy_version, state)
	VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,'active',$24,$25)
	RETURNING id`

func submitSeedRow(t *testing.T, pool *pgxpool.Pool, callerID int64, body, state string) int64 {
	t.Helper()
	req := mustDecode(t, body)
	env, err := req.CanonicalEnvelope()
	if err != nil {
		t.Fatalf("CanonicalEnvelope: %v", err)
	}
	ch, err := req.ContentHash()
	if err != nil {
		t.Fatalf("ContentHash: %v", err)
	}
	var id int64
	err = pool.QueryRow(context.Background(), submitSeedRequestSQL,
		callerID, req.SigningRequestID, req.AttemptID, req.IntentID, req.BindingRef, int64(req.RecoveryVersion),
		int64(req.ChainID), strings.ToLower(req.Sender), req.Nonce, int(req.TxType),
		strings.ToLower(req.To), req.Value, common.FromHex(req.Data), req.GasLimit,
		req.MaxFeePerGas, req.MaxPriorityFeePerGas, strings.ToLower(req.Asset),
		strings.ToLower(req.Recipient), req.Amount,
		envelopeText(env), ch.Hex(), req.AuthorizationID, strings.Repeat("d", 64),
		testPolicy(t).Version(), state).Scan(&id)
	if err != nil {
		t.Fatalf("seed signing_requests row: %v", err)
	}
	return id
}

func submitRowCount(t *testing.T, pool *pgxpool.Pool, callerID int64, requestID string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM signing_requests WHERE caller_id = $1 AND signing_request_id = $2`,
		callerID, requestID).Scan(&n); err != nil {
		t.Fatalf("count signing_requests: %v", err)
	}
	return n
}

// TestSignerSubmitV1FailClosedScopelessGrant is V1 under the PB gate: a fully
// compliant request (clean 006, matching 008 binding, active matching 007
// grant) is refused `authorization_unverifiable` (4xx) because the grant has no
// verifiable carrier, and the persist-first transaction leaves a `rejected` row
// plus an authorization_refused audit — never a signature.
func TestSignerSubmitV1FailClosedScopelessGrant(t *testing.T) {
	pool := signerAuthStartPG(t)
	ctx := context.Background()
	const callerID = int64(gateCallerID)

	cred, err := IssueCredential(ctx, pool, callerID, "submit")
	if err != nil {
		t.Fatalf("IssueCredential: %v", err)
	}
	gateReset006(t, pool)
	gateSeedGrant(t, pool)

	body := submitGrantBody()
	resp, err := Submit(ctx, submitTestDeps(t, pool), cred, []byte(body))
	if resp != nil {
		t.Fatalf("fail-closed branch returned a signature response: %+v", resp)
	}
	re := signerAuthRefusal(t, err, ClassAuthorizationUnverifiable)
	if got := signerPolicyHTTPStatus(re.Class); got != 403 {
		t.Fatalf("authorization_unverifiable maps to HTTP %d, want 403", got)
	}

	if got := signerAuthSignatureCount(t, pool); got != 0 {
		t.Fatalf("fail-closed submit produced %d signature result(s), want 0", got)
	}

	// Persist-first evidence: the identity row exists, terminal 'rejected', with
	// the observed grant state recorded from the same transaction (the grant was
	// read before the scope check refused).
	var state, refusalClass, authzState, authzID string
	if err := pool.QueryRow(ctx,
		`SELECT state, refusal_class, authorization_state, authorization_id
		   FROM signing_requests WHERE caller_id = $1 AND signing_request_id = $2`,
		callerID, "sr-7f3a").Scan(&state, &refusalClass, &authzState, &authzID); err != nil {
		t.Fatalf("read persisted refusal row: %v", err)
	}
	if state != string(StateRejected) || refusalClass != string(ClassAuthorizationUnverifiable) {
		t.Fatalf("persisted row = state %q class %q, want rejected/authorization_unverifiable", state, refusalClass)
	}
	if authzState != "active" || authzID != gateAuthID {
		t.Fatalf("persisted grant evidence = state %q id %q, want active/%s", authzState, authzID, gateAuthID)
	}

	var action, reason string
	if err := pool.QueryRow(ctx,
		`SELECT action, reason_class FROM signing_request_audit
		  WHERE signing_request_id = $1 ORDER BY audit_id DESC LIMIT 1`,
		"sr-7f3a").Scan(&action, &reason); err != nil {
		t.Fatalf("read refusal audit: %v", err)
	}
	if action != "authorization_refused" || reason != string(ClassAuthorizationUnverifiable) {
		t.Fatalf("audit = action %q reason %q, want authorization_refused/%s", action, reason, ClassAuthorizationUnverifiable)
	}
}

// TestSignerSubmitReplayPersistedResult is T-submit-replay: a duplicate
// identity whose envelope equals the persisted row's replays the persisted
// signature+hash (never a second signature), while a different envelope on the
// same identity is a 409 request_conflict with the original untouched.
func TestSignerSubmitReplayPersistedResult(t *testing.T) {
	pool := signerAuthStartPG(t)
	ctx := context.Background()
	const callerID = int64(5001)

	cred, err := IssueCredential(ctx, pool, callerID, "submit-replay")
	if err != nil {
		t.Fatalf("IssueCredential: %v", err)
	}
	body := submitGrantBody()
	rowID := submitSeedRow(t, pool, callerID, body, string(StateSigned))
	sig, txHash := signerMigrationSignature("ab"), signerMigrationHash("e1")
	gateExec(t, pool, `INSERT INTO signature_results (signing_request_row, signature, tx_hash) VALUES ($1, $2, $3)`,
		rowID, sig, txHash)

	resp, err := Submit(ctx, submitTestDeps(t, pool), cred, []byte(body))
	if err != nil {
		t.Fatalf("same-identity same-content Submit: %v", err)
	}
	if resp.Signature != sig || resp.TxHash != txHash {
		t.Fatalf("replay = signature %q tx_hash %q, want the persisted %q/%q", resp.Signature, resp.TxHash, sig, txHash)
	}
	if got := signerAuthSignatureCount(t, pool); got != 1 {
		t.Fatalf("replay persisted %d signature result(s), want exactly 1", got)
	}

	// Same identity, one bound field changed (nonce 42→43) → conflict; the
	// original row/result must be untouched and no second signature appears.
	changed := strings.Replace(body, `"nonce": "42"`, `"nonce": "43"`, 1)
	if _, err := Submit(ctx, submitTestDeps(t, pool), cred, []byte(changed)); err == nil {
		t.Fatal("same identity with a different envelope was accepted")
	} else {
		signerAuthRefusal(t, err, ClassRequestConflict)
	}
	if got := signerAuthSignatureCount(t, pool); got != 1 {
		t.Fatalf("conflict produced %d signature result(s), want the original 1", got)
	}
	var persistedNonce string
	if err := pool.QueryRow(ctx, `SELECT nonce::text FROM signing_requests WHERE id = $1`, rowID).Scan(&persistedNonce); err != nil {
		t.Fatalf("re-read original row: %v", err)
	}
	if persistedNonce != "42" {
		t.Fatalf("conflict rewrote the original row: nonce = %q, want 42", persistedNonce)
	}
}

// TestSignerSubmitReplayOutcomeNotYetVisible is T-submit-replay's in-flight
// branch: the identity row exists but no durable result is visible, so the
// caller is told to retry the same identity — never re-signed, never a new
// identity, zero signatures.
func TestSignerSubmitReplayOutcomeNotYetVisible(t *testing.T) {
	pool := signerAuthStartPG(t)
	ctx := context.Background()
	const callerID = int64(6001)

	cred, err := IssueCredential(ctx, pool, callerID, "submit-inflight")
	if err != nil {
		t.Fatalf("IssueCredential: %v", err)
	}
	body := submitGrantBody()
	submitSeedRow(t, pool, callerID, body, string(StateReceived))

	_, err = Submit(ctx, submitTestDeps(t, pool), cred, []byte(body))
	if err == nil {
		t.Fatal("a row without a durable result reported success")
	}
	var re *RefusalError
	if !errors.As(err, &re) || re.Class != ClassOutcomeNotYetVisible {
		t.Fatalf("error = %v, want outcome_not_yet_visible", err)
	}
	if RetryabilityOf(re.Class) != RetryNow {
		t.Fatalf("outcome_not_yet_visible retryability = %v, want RetryNow", RetryabilityOf(re.Class))
	}
	if got := signerAuthSignatureCount(t, pool); got != 0 {
		t.Fatalf("in-flight replay produced %d signature result(s), want 0", got)
	}
	if got := submitRowCount(t, pool, callerID, "sr-7f3a"); got != 1 {
		t.Fatalf("in-flight replay created %d rows, want the original 1", got)
	}
}
