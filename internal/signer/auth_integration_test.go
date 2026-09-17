//go:build integration

// auth_integration_test.go owns spec task T017 for 009-signer-service: the V3
// authentication/permission/ownership matrix on a real, isolated PostgreSQL
// (quickstart V3; FR-04/FR-05; contracts/api.md §4; SC-02). It proves that
// missing/invalid/revoked credentials all collapse to one identical generic
// unauthenticated refusal (the 401 is indistinguishable), that an authenticated
// caller without can_sign is refused as signing_not_permitted (403) and never
// signs, and that a request's sender is validated against the pinned
// policy/registry rather than trusted from, or derived from, the
// credential-derived caller identity. Each test boots its own testcontainer
// database named per the 009 isolation scheme and drops it in t.Cleanup.
package signer

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/xtianxx/txharbor/internal/db"
)

// signerAuthGeneric401 is the exact refusal text every credential failure must
// produce: api.md §4 step 1 renders one generic 401 with no distinction.
const signerAuthGeneric401 = "unauthenticated: credential rejected"

// signerAuthStartPG boots an isolated scratch PostgreSQL named per the 009
// isolation scheme, migrates it to the full schema, and returns a pool.
func signerAuthStartPG(t *testing.T) *pgxpool.Pool {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()
	ctr, err := postgres.Run(ctx, "postgres:18.6-trixie",
		postgres.WithDatabase(isolationDBName),
		postgres.WithUsername("txharbor"),
		postgres.WithPassword("txharbor"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2)),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })
	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres connection string: %v", err)
	}
	var out bytes.Buffer
	if err := db.MigrateUp(ctx, signerMigrationOptions(dsn), &out); err != nil {
		t.Fatalf("MigrateUp() error = %v (output %q)", err, out.String())
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("open pgx pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// signerAuthRefusal asserts err is a refusal of wantClass.
func signerAuthRefusal(t *testing.T, err error, wantClass RefusalClass) *RefusalError {
	t.Helper()
	var re *RefusalError
	if !errors.As(err, &re) {
		t.Fatalf("error %v is not a *RefusalError", err)
	}
	if re.Class != wantClass {
		t.Fatalf("refusal class = %s, want %s (%v)", re.Class, wantClass, err)
	}
	return re
}

func signerAuthSignatureCount(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM signature_results`).Scan(&n); err != nil {
		t.Fatalf("count signature_results: %v", err)
	}
	return n
}

// TestSignerAuthCredentialFailuresAreIdentical401 is V3 step 1: missing,
// malformed, unknown and revoked credentials are one indistinguishable generic
// unauthenticated refusal, and no signing result is produced.
func TestSignerAuthCredentialFailuresAreIdentical401(t *testing.T) {
	pool := signerAuthStartPG(t)
	ctx := context.Background()
	const callerID = int64(1001)

	valid, err := IssueCredential(ctx, pool, callerID, "ops")
	if err != nil {
		t.Fatalf("IssueCredential: %v", err)
	}
	res, err := Authenticate(ctx, pool, valid)
	if err != nil {
		t.Fatalf("Authenticate(valid): %v", err)
	}
	if res.Caller.ID != callerID || !res.Caller.CanSign {
		t.Fatalf("valid credential loaded caller %+v, want id=%d can_sign=true", res.Caller, callerID)
	}

	// A second credential is revoked; the first must stay usable.
	revocable, err := IssueCredential(ctx, pool, callerID, "ops-rotating")
	if err != nil {
		t.Fatalf("IssueCredential(revocable): %v", err)
	}
	revocableRes, err := Authenticate(ctx, pool, revocable)
	if err != nil {
		t.Fatalf("Authenticate(revocable): %v", err)
	}
	if err := RevokeCredential(ctx, pool, revocableRes.CredentialID); err != nil {
		t.Fatalf("RevokeCredential: %v", err)
	}
	unknown, err := GenerateCredential()
	if err != nil {
		t.Fatalf("GenerateCredential: %v", err)
	}

	cases := []struct {
		name      string
		presented string
	}{
		{"missing", ""},
		{"malformed", "txs_short"},
		{"unknown", unknown},
		{"revoked", revocable},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Authenticate(ctx, pool, tc.presented)
			re := signerAuthRefusal(t, err, ClassUnauthenticated)
			if re.Field != "" || re.Msg != "credential rejected" {
				t.Fatalf("refusal %q leaks detail (field=%q msg=%q); the 401 must be generic", err, re.Field, re.Msg)
			}
			if err.Error() != signerAuthGeneric401 {
				t.Fatalf("refusal %q != the identical generic 401 %q", err, signerAuthGeneric401)
			}
		})
	}

	if _, err := Authenticate(ctx, pool, valid); err != nil {
		t.Fatalf("unrevoked credential rejected after a sibling revoke: %v", err)
	}
	if got := signerAuthSignatureCount(t, pool); got != 0 {
		t.Fatalf("credential failures produced %d signature result(s), want 0", got)
	}
}

// TestSignerAuthCanSignFalseIsForbidden403 is V3 step 2: can_sign=false still
// authenticates (never 401) but the permission gate refuses it as
// signing_not_permitted (403), zero signatures; a permission flip never
// changes the identity.
func TestSignerAuthCanSignFalseIsForbidden403(t *testing.T) {
	pool := signerAuthStartPG(t)
	ctx := context.Background()
	const callerID = int64(2001)

	plaintext, err := IssueCredential(ctx, pool, callerID, "ops")
	if err != nil {
		t.Fatalf("IssueCredential: %v", err)
	}
	if err := SetCanSign(ctx, pool, callerID, false); err != nil {
		t.Fatalf("SetCanSign(false): %v", err)
	}

	res, err := Authenticate(ctx, pool, plaintext)
	if err != nil {
		t.Fatalf("authenticated caller must not be refused as unauthenticated: %v", err)
	}
	if res.Caller.CanSign {
		t.Fatal("can_sign=false caller reported can_sign=true")
	}
	if re := signerAuthRefusal(t, PermitSigning(res.Caller), ClassSigningNotPermitted); re.Msg == "" {
		t.Fatal("signing_not_permitted refusal carries no message")
	}
	if ClassSigningNotPermitted == ClassUnauthenticated {
		t.Fatal("permission class collides with the unauthenticated 401 class")
	}
	if got := signerAuthSignatureCount(t, pool); got != 0 {
		t.Fatalf("unpermitted caller produced %d signature result(s), want 0", got)
	}

	if err := SetCanSign(ctx, pool, callerID, true); err != nil {
		t.Fatalf("SetCanSign(true): %v", err)
	}
	res, err = Authenticate(ctx, pool, plaintext)
	if err != nil {
		t.Fatalf("Authenticate after re-enable: %v", err)
	}
	if !res.Caller.CanSign || res.Caller.ID != callerID {
		t.Fatalf("after re-enable caller = %+v, want same id=%d with can_sign=true", res.Caller, callerID)
	}
	if err := PermitSigning(res.Caller); err != nil {
		t.Fatalf("PermitSigning after re-enable: %v", err)
	}
}

// TestSignerAuthSenderValidatedAgainstPolicyNotIdentity is V3 step 3: the
// credential fixes the caller identity; the request sender is separately
// checked against the policy/registry, so a fully valid permitted caller still
// cannot choose an off-policy sender, and no body field can assert identity.
func TestSignerAuthSenderValidatedAgainstPolicyNotIdentity(t *testing.T) {
	pool := signerAuthStartPG(t)
	ctx := context.Background()
	const callerID = int64(3001)

	plaintext, err := IssueCredential(ctx, pool, callerID, "ops")
	if err != nil {
		t.Fatalf("IssueCredential: %v", err)
	}
	res, err := Authenticate(ctx, pool, plaintext)
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if res.Caller.ID != callerID {
		t.Fatalf("caller identity = %d, want the credential's %d (never a body field)", res.Caller.ID, callerID)
	}
	if err := PermitSigning(res.Caller); err != nil {
		t.Fatalf("PermitSigning: %v", err)
	}

	p := testPolicy(t) // senders 0x1111…, assets 0xaa…, recipient 0xbb…, chain 31337

	inPolicy := mustDecode(t, validBody())
	if err := Validate(&inPolicy); err != nil {
		t.Fatalf("Validate(in-policy): %v", err)
	}
	if err := p.Check(&inPolicy); err != nil {
		t.Fatalf("policy refused the allowlisted sender: %v", err)
	}

	offPolicy := mustDecode(t, strings.Replace(validBody(),
		"0x1111111111111111111111111111111111111111",
		"0x2222222222222222222222222222222222222222", 1))
	if err := Validate(&offPolicy); err != nil {
		t.Fatalf("off-policy sender must stay shape-valid before the policy gate: %v", err)
	}
	re := signerAuthRefusal(t, p.Check(&offPolicy), ClassPolicyRefused)
	if re.Field != "sender" || !strings.HasPrefix(re.Msg, "policy_sender") {
		t.Fatalf("off-policy sender refusal = %s/%s %q, want policy_refused/sender policy_sender*", re.Class, re.Field, re.Msg)
	}

	smuggled := strings.Replace(validBody(), `"sender":`, `"caller_id": 999999, "sender":`, 1)
	_, err = DecodeRequest([]byte(smuggled))
	if err == nil {
		t.Fatal("body with a self-asserted caller_id was accepted")
	}
	if re := signerAuthRefusal(t, err, ClassMalformedRequest); re.Field != "caller_id" {
		t.Fatalf("smuggled identity field refused as %q, want caller_id", re.Field)
	}

	if got := signerAuthSignatureCount(t, pool); got != 0 {
		t.Fatalf("no signing path ran but %d signature result(s) exist, want 0", got)
	}
}
