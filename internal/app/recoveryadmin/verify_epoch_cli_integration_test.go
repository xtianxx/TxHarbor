//go:build integration

package recoveryadmin

import (
	"os"
	"strings"
	"testing"

	"github.com/xtianxx/txharbor/internal/recovery"
)

func TestVerifyBackupRevokedSuccessCannotReplayAsVerified(t *testing.T) {
	requireNativePGRestore(t)
	f := newCLIFixture(t)
	manifestPath := f.cliBackup(t, "op-bkp-epoch-replay")
	backupID := f.manifestBackupID(t, manifestPath)
	target := f.createDB(t, "epoch_cli_replay_target")
	code, out, errOut := f.cliVerify(t, manifestPath, target, "op-ver-epoch-replay")
	if code != 0 || !strings.Contains(out, "verification=verified") {
		t.Fatalf("initial real CLI verification: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}

	data, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read verified manifest: %v", err)
	}
	manifest, err := recovery.ParseManifest(data)
	if err != nil {
		t.Fatalf("parse verified manifest: %v", err)
	}
	manifest.Program.MinCompatible = "999.0"
	data, err = manifest.CanonicalJSON()
	if err != nil {
		t.Fatalf("encode incompatible manifest: %v", err)
	}
	if err := os.WriteFile(manifestPath, data, 0o600); err != nil {
		t.Fatalf("write incompatible manifest: %v", err)
	}
	code, out, errOut = f.cliVerify(t, manifestPath, target, "op-ver-epoch-refusal")
	if code != 1 || !strings.Contains(errOut, "compatible") {
		t.Fatalf("repeat compatibility refusal: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}

	callsBeforeReplay := f.toolCalls(t)
	code, out, errOut = f.cliVerify(t, manifestPath, target, "op-ver-epoch-replay")
	if code != 1 || !strings.Contains(errOut, "cached verification manifest no longer matches its accepted receipt") {
		t.Fatalf("revoked successful operation replay was not refused: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}
	if strings.Contains(out+errOut, "verification=verified") || f.toolCalls(t) != callsBeforeReplay {
		t.Fatalf("stale replay overclaimed or invoked tools: calls_before=%d calls_after=%d stdout=%q stderr=%q",
			callsBeforeReplay, f.toolCalls(t), out, errOut)
	}
	if got := f.evidenceCount(t, "backup_manifest", backupID); got != 1 {
		t.Fatalf("immutable history rows=%d, want original accepted receipt only", got)
	}
}

func TestVerifyBackupCachedReplayRequiresCurrentManifest(t *testing.T) {
	requireNativePGRestore(t)
	f := newCLIFixture(t)
	manifestPath := f.cliBackup(t, "op-bkp-manifest-replay")
	backupID := f.manifestBackupID(t, manifestPath)
	target := f.createDB(t, "epoch_cli_manifest_replay_target")
	code, out, errOut := f.cliVerify(t, manifestPath, target, "op-ver-manifest-missing")
	if code != 0 || !strings.Contains(out, "verification=verified") {
		t.Fatalf("initial real CLI verification: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}
	acceptedBytes, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read accepted manifest: %v", err)
	}

	if err := os.Remove(manifestPath); err != nil {
		t.Fatalf("remove cached manifest: %v", err)
	}
	callsBefore := f.toolCalls(t)
	code, out, errOut = f.cliVerify(t, manifestPath, target, "op-ver-manifest-missing")
	if code == 0 || !strings.Contains(errOut, "cached verification manifest is unavailable or invalid") ||
		strings.Contains(out+errOut, "verification=verified") || f.toolCalls(t) != callsBefore {
		t.Fatalf("missing cached manifest was replayed as verified or ran tools: exit=%d calls=%d/%d stdout=%q stderr=%q",
			code, callsBefore, f.toolCalls(t), out, errOut)
	}
	var missingReplayReason string
	if err := f.control.QueryRow(f.ctx, `SELECT detail->>'reason' FROM recovery_audit
WHERE operation_id='op-ver-manifest-missing' AND result='refused' ORDER BY audit_id DESC LIMIT 1`).Scan(&missingReplayReason); err != nil {
		t.Fatalf("read missing-manifest replay refusal audit: %v", err)
	}
	if missingReplayReason != "cached verification manifest is unavailable or invalid" {
		t.Fatalf("missing-manifest replay audit reason is not safe and specific: %q", missingReplayReason)
	}
	if err := os.WriteFile(manifestPath, acceptedBytes, 0o600); err != nil {
		t.Fatalf("restore accepted manifest copy: %v", err)
	}

	code, out, errOut = f.cliVerify(t, manifestPath, target, "op-ver-manifest-edited")
	if code != 0 || !strings.Contains(out, "verification=verified") {
		t.Fatalf("second real CLI verification: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}
	callsBefore = f.toolCalls(t)
	manifest, err := recovery.ParseManifest(acceptedBytes)
	if err != nil {
		t.Fatalf("parse accepted manifest: %v", err)
	}
	manifest.Verification.State = recovery.VerificationUnverified
	manifest.Verification.EvidenceRef = ""
	edited, err := manifest.CanonicalJSON()
	if err != nil {
		t.Fatalf("encode edited cached manifest: %v", err)
	}
	if err := os.WriteFile(manifestPath, edited, 0o600); err != nil {
		t.Fatalf("write edited cached manifest: %v", err)
	}
	code, out, errOut = f.cliVerify(t, manifestPath, target, "op-ver-manifest-edited")
	if code == 0 || !strings.Contains(errOut, "cached verification manifest no longer matches its accepted receipt") ||
		strings.Contains(out+errOut, "verification=verified") || f.toolCalls(t) != callsBefore {
		t.Fatalf("edited cached manifest was replayed as verified or ran tools: exit=%d calls=%d/%d stdout=%q stderr=%q",
			code, callsBefore, f.toolCalls(t), out, errOut)
	}
	var editedReplayReason string
	if err := f.control.QueryRow(f.ctx, `SELECT detail->>'reason' FROM recovery_audit
WHERE operation_id='op-ver-manifest-edited' AND result='refused' ORDER BY audit_id DESC LIMIT 1`).Scan(&editedReplayReason); err != nil {
		t.Fatalf("read edited-manifest replay refusal audit: %v", err)
	}
	if editedReplayReason != "cached verification manifest no longer matches its accepted receipt" {
		t.Fatalf("edited-manifest replay audit reason is not safe and specific: %q", editedReplayReason)
	}
	if got := f.evidenceCount(t, "backup_manifest", backupID); got != 2 {
		t.Fatalf("read-only replay refusals changed accepted evidence history: rows=%d, want 2", got)
	}
}
