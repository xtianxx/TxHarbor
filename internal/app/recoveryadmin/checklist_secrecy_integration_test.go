//go:build integration

package recoveryadmin

import (
	"strings"
	"testing"
)

func TestChecklistCLIRejectsCredentialCanariesBeforeAuditOrOutput(t *testing.T) {
	f := newCLIFixture(t)
	const uriPassword = "cli-checklist-password-6d12"
	const keywordToken = "cli-checklist-token-7f24"
	const hexKey = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	const privateKeyHex = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	cases := []struct {
		name   string
		args   []string
		canary string
	}{
		{"evidence URI", []string{"checklist-set", "--instance", f.instanceID, "--item", "old_writers_stopped", "--evidence-ref", "postgres://operator:" + uriPassword + "@db.example.test/app"}, uriPassword},
		{"nested summary token", []string{"checklist-set", "--instance", f.instanceID, "--item", "old_writers_stopped", "--evidence-ref", "evidence://015/clean", "--checkpoint-summary", `{"nested":{"token":"` + keywordToken + `"}}`}, keywordToken},
		{"private PEM reason", []string{"checklist-set", "--instance", f.instanceID, "--item", "old_writers_stopped", "--evidence-ref", "evidence://015/clean", "--reason", "-----BEGIN PRIVATE KEY-----\ncanary\n-----END PRIVATE KEY-----"}, "BEGIN PRIVATE KEY"},
		{"64-hex credential field in summary", []string{"checklist-set", "--instance", f.instanceID, "--item", "old_writers_stopped", "--evidence-ref", "evidence://015/clean", "--checkpoint-summary", `{"private_key":"` + hexKey + `"}`}, hexKey},
		{"nested authorization field", []string{"checklist-set", "--instance", f.instanceID, "--item", "old_writers_stopped", "--evidence-ref", "evidence://015/auth", "--checkpoint-summary", `{"checks":[{"authorization":"Basic cli-canary"}]}`}, "Basic cli-canary"},
		{"nested privatekey field", []string{"checklist-set", "--instance", f.instanceID, "--item", "old_writers_stopped", "--evidence-ref", "evidence://015/privatekey", "--checkpoint-summary", `{"checks":[{"privatekey":"` + privateKeyHex + `"}]}`}, privateKeyHex},
		{"private-key alias field", []string{"checklist-set", "--instance", f.instanceID, "--item", "old_writers_stopped", "--evidence-ref", "evidence://015/private-key", "--checkpoint-summary", `{"nested":{"private-key":"` + privateKeyHex + `"}}`}, privateKeyHex},
		{"whitespace padded private_key key", []string{"checklist-set", "--instance", f.instanceID, "--item", "old_writers_stopped", "--evidence-ref", "evidence://015/padded-private-key", "--checkpoint-summary", `{"checks":[{" private_key ":"padded-cli-private-key-canary"}]}`}, "padded-cli-private-key-canary"},
		{"whitespace padded authorization key", []string{"checklist-set", "--instance", f.instanceID, "--item", "old_writers_stopped", "--evidence-ref", "evidence://015/padded-authorization", "--checkpoint-summary", `{"checks":[{" authorization ":"padded-cli-authorization-canary"}]}`}, "padded-cli-authorization-canary"},
		{"whitespace padded client_secret key", []string{"checklist-set", "--instance", f.instanceID, "--item", "old_writers_stopped", "--evidence-ref", "evidence://015/padded-client-secret", "--checkpoint-summary", `{"checks":[{" client_secret ":"padded-cli-client-secret-canary"}]}`}, "padded-cli-client-secret-canary"},
		{"verify rejection reason", []string{"checklist-verify", "--instance", f.instanceID, "--item", "old_writers_stopped", "--reject", "--reason", "password=" + uriPassword}, uriPassword},
	}
	var beforeAudits, beforeItems int
	if err := f.control.QueryRow(f.ctx, `SELECT count(*) FROM recovery_audit WHERE instance_id=$1`, f.instanceID).Scan(&beforeAudits); err != nil {
		t.Fatal(err)
	}
	if err := f.control.QueryRow(f.ctx, `SELECT count(*) FROM recovery_isolation_check WHERE instance_id=$1`, f.instanceID).Scan(&beforeItems); err != nil {
		t.Fatal(err)
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			principal := "deploy:executor"
			if strings.HasPrefix(tc.name, "verify") {
				principal = "auth:verifier"
			}
			code, stdout, stderr := runRecoveryAdmin(t, tc.args, f.env(principal))
			if code == 0 || !strings.Contains(stderr, "credential-shaped material") {
				t.Fatalf("unsafe checklist input not refused generically: exit=%d stdout=%q stderr=%q", code, stdout, stderr)
			}
			if strings.Contains(stdout, tc.canary) || strings.Contains(stderr, tc.canary) {
				t.Fatalf("CLI output leaked canary: stdout=%q stderr=%q", stdout, stderr)
			}
			var audits, items int
			if err := f.control.QueryRow(f.ctx, `SELECT count(*) FROM recovery_audit WHERE instance_id=$1`, f.instanceID).Scan(&audits); err != nil {
				t.Fatal(err)
			}
			if err := f.control.QueryRow(f.ctx, `SELECT count(*) FROM recovery_isolation_check WHERE instance_id=$1`, f.instanceID).Scan(&items); err != nil {
				t.Fatal(err)
			}
			if audits != beforeAudits || items != beforeItems {
				t.Fatalf("unsafe input changed persisted state: audit %d→%d, checklist %d→%d", beforeAudits, audits, beforeItems, items)
			}
			persisted := cliChecklistPersistedText(t, f)
			if strings.Contains(persisted, tc.canary) {
				t.Fatalf("database leaked canary %q", tc.canary)
			}
		})
	}
	code, stdout, stderr := runRecoveryAdmin(t, []string{"checklist-set", "--instance", f.instanceID, "--item", "old_writers_stopped", "--evidence-ref", "evidence://015/padded-safe", "--checkpoint-summary", `{" status ":"healthy"," key_fingerprint ":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`}, f.env("deploy:executor"))
	if code != 0 {
		t.Fatalf("ordinary padded status/fingerprint fields refused: exit=%d stdout=%q stderr=%q", code, stdout, stderr)
	}
}

func cliChecklistPersistedText(t *testing.T, f *cliFixture) string {
	t.Helper()
	var audits, checklist string
	if err := f.control.QueryRow(f.ctx, `SELECT COALESCE(string_agg(to_jsonb(a)::text, ' '), '') FROM recovery_audit a WHERE instance_id=$1`, f.instanceID).Scan(&audits); err != nil {
		t.Fatal(err)
	}
	if err := f.control.QueryRow(f.ctx, `SELECT COALESCE(string_agg(to_jsonb(c)::text, ' '), '') FROM recovery_isolation_check c WHERE instance_id=$1`, f.instanceID).Scan(&checklist); err != nil {
		t.Fatal(err)
	}
	return audits + " " + checklist
}
