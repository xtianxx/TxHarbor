//go:build integration

package recoveryadmin

import (
	"net/url"
	"strings"
	"testing"

	"github.com/xtianxx/txharbor/internal/config"
)

func TestRestoreCLIRejectsCredentialShapedReasonWithoutDisclosure(t *testing.T) {
	requireNativePGRestore(t)
	f := newCLIFixture(t)
	manifestPath, backupID := f.verifiedBackup(t)

	const canary = "fake-restore-reason-canary"
	reasons := []string{
		"postgres://operator:" + canary + "@db.example.test/app",
		"-----BEGIN PRIVATE KEY-----\n" + canary + "\n-----END PRIVATE KEY-----",
		"password=" + canary,
	}
	markersBefore := f.markerCount(t, backupID)
	probesBefore := f.evidenceCount(t, "restore_probe", backupID)
	callsBefore := f.toolCalls(t)
	for i, reason := range reasons {
		args := append(cliRestoreArgs(manifestPath, f.dataDSN, f.instanceID, ""),
			"--declaration", "production_main", "--reason", reason)
		code, stdout, stderr := runRecoveryAdmin(t, args, f.env("deploy:executor"))
		if code == 0 || !strings.Contains(stderr, "target reason contains credential-shaped material") {
			t.Fatalf("reason %d was not refused generically: exit=%d stdout=%q stderr=%q", i, code, stdout, stderr)
		}
		if strings.Contains(stdout, canary) || strings.Contains(stderr, canary) {
			t.Fatalf("reason %d canary escaped in CLI output: stdout=%q stderr=%q", i, stdout, stderr)
		}
	}
	if got := f.markerCount(t, backupID); got != markersBefore {
		t.Fatalf("restore_started markers = %d, before attempts %d", got, markersBefore)
	}
	if got := f.evidenceCount(t, "restore_probe", backupID); got != probesBefore {
		t.Fatalf("restore_probe evidence = %d, before attempts %d", got, probesBefore)
	}
	if got := f.toolCalls(t); got != callsBefore {
		t.Fatalf("pg_dump shim calls = %d, before attempts %d", got, callsBefore)
	}
	if text := cliRestoreReasonPersistedText(t, f); strings.Contains(text, canary) {
		t.Fatalf("credential canary persisted in control-store JSON/text: %q", text)
	}
}

func TestRestoreCLIRejectsMissingObserverAndUntrustedTargetsBeforeEffects(t *testing.T) {
	f := newCLIFixture(t)
	// Restore preflight must run before manifest acceptance, so a captured
	// backup artifact is sufficient here; no verification/restore success is
	// asserted by this negative-only test.
	manifestPath := f.cliBackup(t, "op-restore-preflight-backup")
	backupID := f.manifestBackupID(t, manifestPath)
	markersBefore := f.markerCount(t, backupID)
	callsBefore := f.toolCalls(t)

	missingObserverEnv := f.env("deploy:executor")
	delete(missingObserverEnv, config.EnvRecoveryObserverDSN)
	code, stdout, stderr := runRecoveryAdmin(t, cliRestoreArgs(manifestPath, f.createDB(t, "missing_observer"), f.instanceID, ""), missingObserverEnv)
	if code != 1 || !strings.Contains(stderr, config.EnvRecoveryObserverDSN+" is required") {
		t.Fatalf("missing observer was not rejected: exit=%d stdout=%q stderr=%q", code, stdout, stderr)
	}

	// Preserve endpoint identity while changing only the login role. The
	// endpoint guard key alone is insufficient authority for a restore.
	wrongRoleURL, err := url.Parse(f.dataDSN)
	if err != nil {
		t.Fatalf("parse fixture target DSN: %v", err)
	}
	wrongRoleURL.User = url.UserPassword("different-role", "not-used")
	wrongRole := wrongRoleURL.String()
	wrongRoleArgs := append(cliRestoreArgs(manifestPath, wrongRole, f.instanceID, ""),
		"--declaration", "production_main", "--reason", "configured recovery")
	code, stdout, stderr = runRecoveryAdmin(t, wrongRoleArgs, f.env("deploy:executor"))
	if code != 1 || !strings.Contains(stderr, "does not match deployment-configured authoritative endpoint and role") {
		t.Fatalf("same-key wrong-role target was not refused: exit=%d stdout=%q stderr=%q", code, stdout, stderr)
	}

	// A declaration label cannot make the deployment's authoritative database
	// isolated. The refusal must happen before the invalidation marker and
	// before invoking pg_restore.
	code, stdout, stderr = runRecoveryAdmin(t,
		cliRestoreArgs(manifestPath, f.dataDSN, f.instanceID, ""), f.env("deploy:executor"))
	if code != 1 || !strings.Contains(stderr, "isolated declaration refused") {
		t.Fatalf("authoritative target accepted as isolated: exit=%d stdout=%q stderr=%q", code, stdout, stderr)
	}
	if got := f.markerCount(t, backupID); got != markersBefore {
		t.Fatalf("restore_started markers = %d, before refusals %d", got, markersBefore)
	}
	if got := f.toolCalls(t); got != callsBefore {
		t.Fatalf("pg_dump shim calls = %d, before refusals %d", got, callsBefore)
	}
}

func cliRestoreReasonPersistedText(t *testing.T, f *cliFixture) string {
	t.Helper()
	var audits, evidence string
	if err := f.control.QueryRow(f.ctx, `
SELECT COALESCE(string_agg(to_jsonb(a)::text, ' '), '')
FROM recovery_audit AS a WHERE instance_id = $1`, f.instanceID).Scan(&audits); err != nil {
		t.Fatalf("read recovery audit JSON/text: %v", err)
	}
	if err := f.control.QueryRow(f.ctx, `
SELECT COALESCE(string_agg(to_jsonb(e)::text, ' '), '')
FROM recovery_evidence AS e WHERE instance_id = $1`, f.instanceID).Scan(&evidence); err != nil {
		t.Fatalf("read recovery evidence JSON/text: %v", err)
	}
	return audits + " " + evidence
}
