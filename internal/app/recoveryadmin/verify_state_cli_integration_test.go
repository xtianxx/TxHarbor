//go:build integration

package recoveryadmin

import (
	"os"
	"strings"
	"testing"

	"github.com/xtianxx/txharbor/internal/recovery"
)

func TestVerifyBackupCLIRecordsDeterminateIntegrityRefusal(t *testing.T) {
	requireNativePGRestore(t)
	f := newCLIFixture(t)
	manifestPath := f.cliBackup(t, "op-bkp-integrity-refusal")
	backupID := f.manifestBackupID(t, manifestPath)

	data, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read backup manifest: %v", err)
	}
	manifest, err := recovery.ParseManifest(data)
	if err != nil {
		t.Fatalf("parse backup manifest: %v", err)
	}
	manifest.Artifacts[0].SHA256 = "sha256:0000000000000000000000000000000000000000000000000000000000000000"
	data, err = manifest.CanonicalJSON()
	if err != nil {
		t.Fatalf("encode corrupted manifest: %v", err)
	}
	if err := os.WriteFile(manifestPath, data, 0o600); err != nil {
		t.Fatalf("write corrupted manifest: %v", err)
	}

	target := f.createDB(t, "verify_integrity_refusal")
	code, out, errOut := f.cliVerify(t, manifestPath, target, "op-ver-integrity-refusal")
	if code == 0 {
		t.Fatalf("integrity refusal must be nonzero: stdout=%q stderr=%q", out, errOut)
	}
	if !strings.Contains(out, "verification=rejected") || !strings.Contains(errOut, "sha256") {
		t.Fatalf("CLI did not report the determinate refusal basis: stdout=%q stderr=%q", out, errOut)
	}
	if strings.Contains(out+errOut, "verification=verified") {
		t.Fatalf("CLI overclaimed verification: stdout=%q stderr=%q", out, errOut)
	}
	writtenBytes, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read refused manifest: %v", err)
	}
	written, err := recovery.ParseManifest(writtenBytes)
	if err != nil {
		t.Fatalf("parse refused manifest: %v", err)
	}
	if written.Verification.State != recovery.VerificationRejected || written.Verification.EvidenceRef != "" {
		t.Fatalf("manifest refusal record = %+v, want rejected without evidence", written.Verification)
	}
	if got := f.evidenceCount(t, "backup_manifest", backupID); got != 0 {
		t.Fatalf("rejected verification accepted %d backup_manifest evidence rows", got)
	}
}
