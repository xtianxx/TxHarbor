//go:build integration

// credential_lifecycle_integration_test.go owns spec task T018 for
// 009-signer-service: the operator credential lifecycle carried by the
// `signer-auth` subcommand (FR-04; deps T013/T017). It drives issue → rotate →
// revoke through the real subcommand handler in-process against a real,
// isolated PostgreSQL and asserts every transition at the library auth path.
//
// Rotation rule asserted here is the one that actually exists: 009 research R6
// and signerauth.go pin rotation as an atomic successor-insert + predecessor
// revoke with NO grace window ("Rotation is immediate ... there is no grace
// window"). T018's phrase "incl. rotation grace" is therefore locked as the
// real rule — a predecessor is refused at once, with the identical generic 401
// — not a fictional dual-accept. If a grace window is ever added, this test is
// the tripwire that must change with it.
//
// The file lives in signer_test (external test package), mirroring
// keymode_test.go: the operator handler lives in internal/app, which imports
// internal/signer, so only an external test package can import both without
// the app -> signer test cycle. The isolated-PG helpers below reuse the
// auth_integration_test.go harness shape under `cred*` names and the 009
// isolation database name.
package signer_test

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/xtianxx/txharbor/internal/app"
	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/signer"
)

// credGeneric401 is the exact refusal text every credential failure must
// produce: api.md §4 step 1 renders one generic 401 with no distinction.
const credGeneric401 = "unauthenticated: credential rejected"

// credDBName is the 009 isolation database, matching isolation_test.go.
const credDBName = "txharbor_009"

// credStartPG boots an isolated scratch PostgreSQL, migrates it to the full
// schema, and returns a pool plus the DSN the operator handler must be pointed
// at (it opens its own pool from config.Load).
func credStartPG(t *testing.T) (*pgxpool.Pool, string) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()
	ctr, err := postgres.Run(ctx, "postgres:18.6-trixie",
		postgres.WithDatabase(credDBName),
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
	if err := db.MigrateUp(ctx, db.MigrateOptions{
		DSN:            dsn,
		LockTimeout:    5 * time.Second,
		ConnectTimeout: 5 * time.Second,
	}, &out); err != nil {
		t.Fatalf("MigrateUp() error = %v (output %q)", err, out.String())
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("open pgx pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool, dsn
}

// credEnv is a fully valid environment for config.Load with the 009 scratch
// DSN spliced in (mirrors app's fullServeEnv + confirmAuthEnv).
func credEnv(dsn string) map[string]string {
	return map[string]string{
		config.EnvPGDSN:                 dsn,
		config.EnvRPCURL:                "http://127.0.0.1:8545",
		config.EnvChainID:               "31337",
		config.EnvStartHeight:           "0",
		config.EnvLogStartHeight:        "0",
		config.EnvLogContracts:          "0x1111111111111111111111111111111111111111",
		config.EnvDepositStartHeight:    "0",
		config.EnvDepositContracts:      "0x1111111111111111111111111111111111111111",
		config.EnvDepositWatchAddresses: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		config.EnvConfirmationDepth:     "10",
		config.EnvReorgMaxDepth:         "100",
		config.EnvHTTPAddr:              "127.0.0.1:0",
	}
}

// credRun drives the signer-auth subcommand handler in-process (no shell-out),
// mirroring signerauth_test.go's Deps shape.
func credRun(ctx context.Context, env map[string]string, args ...string) (int, string, string) {
	var stdout, stderr bytes.Buffer
	code := app.SignerAuth(ctx, args, app.Deps{
		Getenv: func(k string) (string, bool) { v, ok := env[k]; return v, ok },
		Stdout: &stdout,
		Stderr: &stderr,
	})
	return code, stdout.String(), stderr.String()
}

// credField extracts a `name=value` field from an operator success line.
func credField(t *testing.T, out, name string) string {
	t.Helper()
	for _, f := range strings.Fields(out) {
		if v, ok := strings.CutPrefix(f, name+"="); ok {
			return v
		}
	}
	t.Fatalf("stdout %q has no %s= field", out, name)
	return ""
}

// credRefusal asserts err is a *signer.RefusalError of wantClass.
func credRefusal(t *testing.T, err error, wantClass signer.RefusalClass) *signer.RefusalError {
	t.Helper()
	var re *signer.RefusalError
	if !errors.As(err, &re) {
		t.Fatalf("error %v is not a *signer.RefusalError", err)
	}
	if re.Class != wantClass {
		t.Fatalf("refusal class = %s, want %s (%v)", re.Class, wantClass, err)
	}
	return re
}

// credUnauth asserts err is the identical generic 401 every credential failure
// shares: one class, no field, no detail, exact text.
func credUnauth(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("credential accepted, want the identical generic unauthenticated refusal")
	}
	re := credRefusal(t, err, signer.ClassUnauthenticated)
	if re.Field != "" || re.Msg != "credential rejected" {
		t.Fatalf("refusal %q leaks detail (field=%q msg=%q); the 401 must be generic", err, re.Field, re.Msg)
	}
	if err.Error() != credGeneric401 {
		t.Fatalf("refusal %q != the identical generic 401 %q", err, credGeneric401)
	}
}

// TestSignerCredentialLifecycleOperatorPath is T018: the whole lifecycle is
// driven ONLY through the signer-auth handler against the real database, and
// every transition is asserted at the library auth path.
func TestSignerCredentialLifecycleOperatorPath(t *testing.T) {
	pool, dsn := credStartPG(t)
	ctx := context.Background()
	env := credEnv(dsn)
	const callerID = int64(18001)

	// issue: the plaintext is shown once, the paper trail is echoed, the
	// typed prefix is present.
	code, out, errOut := credRun(ctx, env,
		"issue", "--caller-id", "18001", "--label", "life", "--operator", "op-1", "--reason", "bootstrap")
	if code != 0 {
		t.Fatalf("issue exit code = %d, want 0; stderr=%s", code, errOut)
	}
	if errOut != "" {
		t.Errorf("issue wrote stderr %q, want empty", errOut)
	}
	key1 := credField(t, out, "key")
	if !strings.HasPrefix(key1, signer.CredentialPrefix) {
		t.Fatalf("issued plaintext %q lacks the %s prefix", key1, signer.CredentialPrefix)
	}
	if !strings.Contains(out, "issued caller_id=18001") || credField(t, out, "operator") != "op-1" ||
		credField(t, out, "reason") != "bootstrap" {
		t.Errorf("issue stdout %q lacks the caller/operator/reason paper trail", out)
	}

	// The issued credential authenticates and resolves to the caller row.
	res1, err := signer.Authenticate(ctx, pool, key1)
	if err != nil {
		t.Fatalf("Authenticate(issued) error = %v", err)
	}
	if res1.Caller.ID != callerID || res1.Caller.Label != "life" || !res1.Caller.CanSign {
		t.Fatalf("Authenticate(issued) caller = %+v, want id=18001 label=life can_sign=true", res1.Caller)
	}
	cred1 := res1.CredentialID
	if cred1 <= 0 {
		t.Fatalf("issued credential_id = %d, want positive", cred1)
	}

	// A sibling credential for the same caller: separate row, same identity.
	code, out, errOut = credRun(ctx, env,
		"issue", "--caller-id", "18001", "--label", "life", "--operator", "op-1", "--reason", "sibling")
	if code != 0 {
		t.Fatalf("sibling issue exit code = %d, want 0; stderr=%s", code, errOut)
	}
	key2 := credField(t, out, "key")
	res2, err := signer.Authenticate(ctx, pool, key2)
	if err != nil {
		t.Fatalf("Authenticate(sibling) error = %v", err)
	}
	if res2.Caller.ID != callerID || res2.CredentialID == cred1 {
		t.Fatalf("sibling = caller %d cred %d, want caller %d with a distinct credential id", res2.Caller.ID, res2.CredentialID, callerID)
	}

	// rotate: successor is minted and the predecessor is revoked in the same
	// transaction. The real rule is IMMEDIATE (no grace window), so key2 is
	// refused at once with the identical generic 401 — no dual-accept.
	code, out, errOut = credRun(ctx, env,
		"rotate", "--caller-id", "18001", "--credential-id", strconv.FormatInt(res2.CredentialID, 10), "--operator", "op-2", "--reason", "scheduled rotation")
	if code != 0 {
		t.Fatalf("rotate exit code = %d, want 0; stderr=%s", code, errOut)
	}
	key3 := credField(t, out, "key")
	if key3 == key2 || key3 == key1 {
		t.Fatal("rotate returned a predecessor plaintext")
	}
	if _, err := signer.Authenticate(ctx, pool, key2); err == nil {
		t.Fatal("predecessor still authenticates after rotation; rotation must be immediate (no grace window)")
	} else {
		credUnauth(t, err)
	}
	if _, err := signer.Authenticate(ctx, pool, key3); err != nil {
		t.Fatalf("successor invalid after rotation: %v", err)
	}
	// The sibling (never rotated) still works while its peer is revoked.
	if _, err := signer.Authenticate(ctx, pool, key1); err != nil {
		t.Fatalf("sibling credential rejected after a peer rotation: %v", err)
	}

	// rotate refuses an already-revoked predecessor (exit 1, redacted).
	code, _, errOut = credRun(ctx, env,
		"rotate", "--caller-id", "18001", "--credential-id", strconv.FormatInt(res2.CredentialID, 10), "--operator", "op-2", "--reason", "again")
	if code != 1 {
		t.Fatalf("rotate revoked predecessor exit code = %d, want 1; stderr=%s", code, errOut)
	}
	if strings.Contains(errOut, "txs_") || strings.Contains(errOut, ":txharbor@") {
		t.Errorf("stderr %q leaks credential material or the DSN password", errOut)
	}

	// revoke the sibling: it 401s at once, never again, while the successor of
	// the rotated peer keeps authenticating under the same caller.
	code, out, errOut = credRun(ctx, env,
		"revoke", "--credential-id", strconv.FormatInt(cred1, 10), "--operator", "op-3", "--reason", "retire sibling")
	if code != 0 {
		t.Fatalf("revoke exit code = %d, want 0; stderr=%s", code, errOut)
	}
	if !strings.Contains(out, "revoked credential_id="+strconv.FormatInt(cred1, 10)) {
		t.Errorf("revoke stdout %q lacks revoked credential_id=%d", out, cred1)
	}
	if _, err := signer.Authenticate(ctx, pool, key1); err == nil {
		t.Fatal("revoked credential still authenticates")
	} else {
		credUnauth(t, err)
	}
	// "never again": a second look at the revoked credential is still the same
	// generic 401, and the caller's other credential is untouched.
	if _, err := signer.Authenticate(ctx, pool, key1); err == nil {
		t.Fatal("revoked credential authenticated on a later attempt")
	} else {
		credUnauth(t, err)
	}
	if _, err := signer.Authenticate(ctx, pool, key3); err != nil {
		t.Fatalf("sibling credential rejected after a peer revoke: %v", err)
	}

	// A second revoke of the same credential is not-found (exit 1).
	code, _, errOut = credRun(ctx, env,
		"revoke", "--credential-id", strconv.FormatInt(cred1, 10), "--operator", "op-3", "--reason", "retire again")
	if code != 1 {
		t.Fatalf("second revoke exit code = %d, want 1; stderr=%s", code, errOut)
	}

	// Revoking an unknown credential fails naming the field, without leaking
	// secrets (the handler prints the redacted refusal, never the raw id).
	code, _, errOut = credRun(ctx, env,
		"revoke", "--credential-id", "990000001", "--operator", "op-3", "--reason", "cleanup")
	if code != 1 {
		t.Fatalf("revoke missing exit code = %d, want 1; stderr=%s", code, errOut)
	}
	if !strings.Contains(errOut, "credential_id") {
		t.Errorf("stderr %q does not name the credential_id field", errOut)
	}
	if strings.Contains(errOut, "txs_") || strings.Contains(errOut, ":txharbor@") {
		t.Errorf("stderr %q leaks credential material or the DSN password", errOut)
	}
}

// TestSignerCredentialDirectRoundTrip covers the same lifecycle through
// auth.go directly (the operator handler is only a carrier): round-trip,
// immediate predecessor refusal, and the refusal classes for rotate/revoke
// misuse, with a sibling surviving every revoke of its peer.
func TestSignerCredentialDirectRoundTrip(t *testing.T) {
	pool, _ := credStartPG(t)
	ctx := context.Background()
	const callerID = int64(18101)

	key1, err := signer.IssueCredential(ctx, pool, callerID, "direct")
	if err != nil {
		t.Fatalf("IssueCredential: %v", err)
	}
	res1, err := signer.Authenticate(ctx, pool, key1)
	if err != nil {
		t.Fatalf("Authenticate(issued): %v", err)
	}
	if res1.Caller.ID != callerID || res1.Caller.Label != "direct" || !res1.Caller.CanSign {
		t.Fatalf("issued caller = %+v, want id=%d label=direct can_sign=true", res1.Caller, callerID)
	}

	// rotate: successor valid, predecessor refused immediately.
	key2, err := signer.RotateCredential(ctx, pool, callerID, res1.CredentialID)
	if err != nil {
		t.Fatalf("RotateCredential: %v", err)
	}
	if key2 == key1 {
		t.Fatal("rotation returned the predecessor plaintext")
	}
	if _, err := signer.Authenticate(ctx, pool, key1); err == nil {
		t.Fatal("predecessor still authenticates after rotation")
	} else {
		credUnauth(t, err)
	}
	res2, err := signer.Authenticate(ctx, pool, key2)
	if err != nil {
		t.Fatalf("Authenticate(successor): %v", err)
	}
	if res2.Caller.ID != callerID {
		t.Fatalf("rotation changed caller_id to %d, want %d", res2.Caller.ID, callerID)
	}

	// Misuse: rotating an already-revoked predecessor or one owned by another
	// caller is validation_failed, not a silent no-op.
	if _, err := signer.RotateCredential(ctx, pool, callerID, res1.CredentialID); err == nil {
		t.Fatal("rotating a revoked predecessor succeeded")
	} else if re := credRefusal(t, err, signer.ClassValidationFailed); re.Field != "credential_id" {
		t.Fatalf("re-rotate refusal field = %q, want credential_id", re.Field)
	}
	if _, err := signer.RotateCredential(ctx, pool, callerID+777, res2.CredentialID); err == nil {
		t.Fatal("rotating another caller's credential succeeded")
	} else {
		credRefusal(t, err, signer.ClassValidationFailed)
	}

	// A sibling survives its peer's revoke.
	sibling, err := signer.IssueCredential(ctx, pool, callerID, "direct")
	if err != nil {
		t.Fatalf("IssueCredential(sibling): %v", err)
	}
	if err := signer.RevokeCredential(ctx, pool, res2.CredentialID); err != nil {
		t.Fatalf("RevokeCredential(successor): %v", err)
	}
	if _, err := signer.Authenticate(ctx, pool, key2); err == nil {
		t.Fatal("revoked successor still authenticates")
	} else {
		credUnauth(t, err)
	}
	if _, err := signer.Authenticate(ctx, pool, sibling); err != nil {
		t.Fatalf("sibling credential rejected after a peer revoke: %v", err)
	}
	if err := signer.RevokeCredential(ctx, pool, res2.CredentialID); err == nil {
		t.Fatal("double revoke succeeded")
	} else {
		credRefusal(t, err, signer.ClassValidationFailed)
	}
}
