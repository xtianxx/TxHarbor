//go:build integration

// authorization_integration_test.go executes quickstart V6 (T032): after
// admission the per-send authorization is re-verified every step, so revoke /
// expiry / scope re-supply refuse with zero calls to the 010 boundary, while
// re-authorized sends continue the same intent. It also asserts structurally
// that no worker/CLI code path supplies or revokes a 007 grant (scheduling is
// never issuance).
package execution

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAuthorizationLifecycleRefusesSends(t *testing.T) {
	ctx, pool := executionPool(t)
	store := execClaimStore(t, pool)
	double := newLifecycleDouble()
	driver := &StepDriver{Pool: pool, Claims: store, Binding: bindingDouble{result: BindingMatches}, Advancer: double}
	version := seedExecClaimedIntent(t, ctx, pool, "req-v6", "authz-v6", "intent-v6", "owner-a", store)
	req := StepRequest{IntentID: "intent-v6", RequestID: "req-v6", CallerID: 1, OwnerID: "owner-a", LeaseVersion: version, Action: ActionFirstBroadcast}

	if _, err := pool.Exec(ctx, `UPDATE withdrawal_authorizations SET state = 'revoked' WHERE authorization_id = 'authz-v6'`); err != nil {
		t.Fatalf("revoke grant: %v", err)
	}
	if out, err := driver.IssueAndAdvance(ctx, req); err != nil || out.Refusal != ClassAuthorizationRevoked {
		t.Fatalf("revoked grant refusal = %+v (err %v), want authorization_revoked", out, err)
	}

	if _, err := pool.Exec(ctx, `UPDATE withdrawal_authorizations
		SET state = 'active', expires_at = now() - interval '1 hour' WHERE authorization_id = 'authz-v6'`); err != nil {
		t.Fatalf("expire grant: %v", err)
	}
	if out, err := driver.IssueAndAdvance(ctx, req); err != nil || out.Refusal != ClassAuthorizationExpired {
		t.Fatalf("expired grant refusal = %+v (err %v), want authorization_expired", out, err)
	}

	if _, err := pool.Exec(ctx, `UPDATE withdrawal_authorizations SET expires_at = NULL WHERE authorization_id = 'authz-v6'`); err != nil {
		t.Fatalf("restore grant: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE withdrawal_authorization_scopes
		SET authorization_version = 2 WHERE authorization_id = 'authz-v6'`); err != nil {
		t.Fatalf("re-supply scope: %v", err)
	}
	if out, err := driver.IssueAndAdvance(ctx, req); err != nil || out.Refusal != ClassAuthorizationChanged {
		t.Fatalf("scope re-supply refusal = %+v (err %v), want authorization_changed", out, err)
	}

	if double.totalCalls() != 0 {
		t.Fatalf("advance calls before any successful step = %d, want 0", double.totalCalls())
	}
	if got := countIntentSteps(t, ctx, pool, "intent-v6"); got != 0 {
		t.Fatalf("steps written on refusals = %d, want 0", got)
	}

	// Re-established basis: a new step may be issued, never a second intent.
	if _, err := pool.Exec(ctx, `UPDATE withdrawal_authorization_scopes
		SET authorization_version = 1 WHERE authorization_id = 'authz-v6'`); err != nil {
		t.Fatalf("re-establish scope: %v", err)
	}
	if out, err := driver.IssueAndAdvance(ctx, req); err != nil || out.FinalStepState != StepConverged {
		t.Fatalf("re-authorized step = %+v (err %v), want converged", out, err)
	}
	var intents int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM payment_intents`).Scan(&intents); err != nil || intents != 1 {
		t.Fatalf("intent rows = %d (err %v), want 1", intents, err)
	}
}

func TestNoGrantSupplyPathInScheduling(t *testing.T) {
	var files []string
	execFiles, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("glob execution sources: %v", err)
	}
	files = append(files, execFiles...)
	files = append(files, filepath.Join("..", "app", "withdrawalworker.go"), filepath.Join("..", "app", "withdrawalexec.go"))

	forbidden := []string{
		"INSERT INTO withdrawal_authorizations",
		"UPDATE withdrawal_authorizations",
		"DELETE FROM withdrawal_authorizations",
		"withdrawal_authorizations SET",
	}
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		src := string(raw)
		for _, bad := range forbidden {
			if strings.Contains(src, bad) {
				t.Fatalf("%s contains a 007 grant write %q; scheduling must never supply/revoke authorization", f, bad)
			}
		}
	}
}
