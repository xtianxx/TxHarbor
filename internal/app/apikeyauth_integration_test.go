//go:build integration

package app

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/withdrawal"
)

// apiKeyAuthRun drives the subcommand function in-process (no shell-out),
// mirroring the confirm-auth carrier tests.
func apiKeyAuthRun(ctx context.Context, env map[string]string, args ...string) (int, string, string) {
	var stdout, stderr bytes.Buffer
	code := APIKeyAuth(ctx, args, Deps{
		Getenv: fakeEnv(env),
		Stdout: &stdout,
		Stderr: &stderr,
	})
	return code, stdout.String(), stderr.String()
}

// apiKeyAuthField extracts a `name=value` field from a success line.
func apiKeyAuthField(t *testing.T, out, name string) string {
	t.Helper()
	for _, f := range strings.Fields(out) {
		if v, ok := strings.CutPrefix(f, name+"="); ok {
			return v
		}
	}
	t.Fatalf("stdout %q has no %s= field", out, name)
	return ""
}

// apiKeyAuthWantCode asserts err is a *withdrawal.Error carrying exactly code.
func apiKeyAuthWantCode(t *testing.T, err error, code withdrawal.Code) {
	t.Helper()
	var typed *withdrawal.Error
	if !errors.As(err, &typed) {
		t.Fatalf("error = %v (%T), want *withdrawal.Error with code %q", err, err, code)
	}
	if typed.Code != code {
		t.Fatalf("error code = %q, want %q (error %v)", typed.Code, code, err)
	}
}

// apiKeyAuthCallerScope reads the stable caller row's label and permission.
func apiKeyAuthCallerScope(t *testing.T, ctx context.Context, pool *pgxpool.Pool, callerID int64) (string, bool) {
	t.Helper()
	var label string
	var canCreate bool
	if err := pool.QueryRow(ctx,
		`SELECT label, can_create FROM caller WHERE caller_id = $1`, callerID).
		Scan(&label, &canCreate); err != nil {
		t.Fatalf("read caller %d scope: %v", callerID, err)
	}
	return label, canCreate
}

// TestAPIKeyAuthLifecycle covers T034 against a real PostgreSQL: issue prints
// the plaintext once and persists the digest; the library authenticates it;
// rotate(grace 60) dual-accepts while preserving caller_id + scope; an
// explicit revoke wins over the grace stamp; revoking the successor 401s it
// and leaves the caller row intact.
func TestAPIKeyAuthLifecycle(t *testing.T) {
	dsn := startConfirmAuthPostgres(t)
	ctx := context.Background()
	env := confirmAuthEnv(dsn)

	pool, err := db.OpenPool(ctx, dsn, 5*time.Second)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	defer pool.Close()

	const callerID = int64(7001)

	// issue
	code, out, errOut := apiKeyAuthRun(ctx, env,
		"issue", "--caller-id", "7001", "--label", "life", "--operator", "op-1", "--reason", "bootstrap")
	if code != 0 {
		t.Fatalf("issue exit code = %d, want 0; stderr=%s", code, errOut)
	}
	if errOut != "" {
		t.Errorf("issue wrote stderr %q, want empty", errOut)
	}
	oldKey := apiKeyAuthField(t, out, "key")
	oldKeyID := apiKeyAuthField(t, out, "key_id")
	if !strings.HasPrefix(oldKey, "txh_") {
		t.Fatalf("issued plaintext %q lacks the txh_ typed prefix", oldKey)
	}
	if !strings.Contains(out, "issued caller_id=7001") {
		t.Errorf("issue stdout %q lacks the caller identity", out)
	}
	if got := apiKeyAuthField(t, out, "operator"); got != "op-1" {
		t.Errorf("issue stdout operator = %q, want op-1", got)
	}

	// The credential authenticates through the library and resolves to the
	// caller with the scope the row was created with.
	if label, canCreate := apiKeyAuthCallerScope(t, ctx, pool, callerID); label != "life" || !canCreate {
		t.Fatalf("caller scope after issue = (%q,%t), want (life,true)", label, canCreate)
	}
	res, err := withdrawal.Authenticate(ctx, pool, oldKey)
	if err != nil {
		t.Fatalf("Authenticate(issued) error = %v", err)
	}
	if res.Caller.ID != callerID || res.Caller.Label != "life" || !res.Caller.CanCreate {
		t.Fatalf("Authenticate(issued) caller = %+v, want id=7001 label=life can_create=true", res.Caller)
	}
	if res.Key.Prefix != oldKey[:8] {
		t.Fatalf("Authenticate(issued) prefix = %q, want %q", res.Key.Prefix, oldKey[:8])
	}

	// rotate with a 60s grace window
	code, out, errOut = apiKeyAuthRun(ctx, env,
		"rotate", "--caller-id", "7001", "--grace-seconds", "60", "--operator", "op-2", "--reason", "scheduled rotation")
	if code != 0 {
		t.Fatalf("rotate exit code = %d, want 0; stderr=%s", code, errOut)
	}
	newKey := apiKeyAuthField(t, out, "key")
	newKeyID := apiKeyAuthField(t, out, "key_id")
	if newKey == oldKey {
		t.Fatal("rotate returned the predecessor plaintext")
	}
	if newKeyID == oldKeyID {
		t.Fatal("rotate reused the predecessor key_id")
	}

	// Dual-accept: both credentials resolve to the same caller and scope;
	// rotation never mutates caller_id or the caller row.
	oldRes, err := withdrawal.Authenticate(ctx, pool, oldKey)
	if err != nil {
		t.Fatalf("Authenticate(predecessor within grace) error = %v", err)
	}
	newRes, err := withdrawal.Authenticate(ctx, pool, newKey)
	if err != nil {
		t.Fatalf("Authenticate(successor) error = %v", err)
	}
	if oldRes.Caller.ID != callerID || newRes.Caller.ID != callerID {
		t.Fatalf("rotation changed caller_id: old=%d new=%d, want %d", oldRes.Caller.ID, newRes.Caller.ID, callerID)
	}
	if label, canCreate := apiKeyAuthCallerScope(t, ctx, pool, callerID); label != "life" || !canCreate {
		t.Fatalf("caller scope after rotate = (%q,%t), want (life,true) preserved", label, canCreate)
	}

	// An explicit revoke terminates the still-valid graced predecessor.
	code, out, errOut = apiKeyAuthRun(ctx, env,
		"revoke", "--key-id", oldKeyID, "--operator", "op-2", "--reason", "retire predecessor")
	if code != 0 {
		t.Fatalf("revoke predecessor exit code = %d, want 0; stderr=%s", code, errOut)
	}
	if !strings.Contains(out, "revoked key_id="+oldKeyID) {
		t.Errorf("revoke stdout %q lacks revoked key_id=%s", out, oldKeyID)
	}
	if _, err := withdrawal.Authenticate(ctx, pool, oldKey); err == nil {
		t.Fatal("predecessor still authenticates after explicit revoke")
	} else {
		apiKeyAuthWantCode(t, err, withdrawal.CodeUnauthenticated)
	}
	if _, err := withdrawal.Authenticate(ctx, pool, newKey); err != nil {
		t.Fatalf("successor invalid after predecessor revoke: %v", err)
	}

	// A second revoke of the same key_id is not-found (exit 1).
	code, _, errOut = apiKeyAuthRun(ctx, env,
		"revoke", "--key-id", oldKeyID, "--operator", "op-2", "--reason", "retire again")
	if code != 1 {
		t.Fatalf("second revoke exit code = %d, want 1; stderr=%s", code, errOut)
	}

	// Revoke the successor: it 401s while the caller row survives untouched.
	code, _, errOut = apiKeyAuthRun(ctx, env,
		"revoke", "--key-id", newKeyID, "--operator", "op-2", "--reason", "decommission caller")
	if code != 0 {
		t.Fatalf("revoke successor exit code = %d, want 0; stderr=%s", code, errOut)
	}
	if _, err := withdrawal.Authenticate(ctx, pool, newKey); err == nil {
		t.Fatal("successor still authenticates after revoke")
	} else {
		apiKeyAuthWantCode(t, err, withdrawal.CodeUnauthenticated)
	}
	if label, canCreate := apiKeyAuthCallerScope(t, ctx, pool, callerID); label != "life" || !canCreate {
		t.Fatalf("caller scope after revocation = (%q,%t), want (life,true) intact", label, canCreate)
	}
}

// TestAPIKeyAuthRevokeMissingKey covers the not-found path against a real
// database: exit 1, stderr names the key_id, and no secret (DSN password or
// key material) leaks.
func TestAPIKeyAuthRevokeMissingKey(t *testing.T) {
	dsn := startConfirmAuthPostgres(t)
	ctx := context.Background()
	env := confirmAuthEnv(dsn)

	code, _, errOut := apiKeyAuthRun(ctx, env,
		"revoke", "--key-id", "999999999", "--operator", "op-3", "--reason", "cleanup")
	if code != 1 {
		t.Fatalf("revoke missing exit code = %d, want 1; stderr=%s", code, errOut)
	}
	if !strings.Contains(errOut, "999999999") {
		t.Errorf("stderr %q does not name the missing key_id", errOut)
	}
	if strings.Contains(errOut, ":txharbor@") {
		t.Errorf("stderr %q leaks the DSN password", errOut)
	}
	if strings.Contains(errOut, "txh_") {
		t.Errorf("stderr %q carries key material", errOut)
	}
}
