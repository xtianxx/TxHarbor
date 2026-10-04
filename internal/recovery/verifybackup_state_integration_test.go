//go:build integration

package recovery

import (
	"strings"
	"testing"

	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

func TestVerifyBackupEvidenceInsertFailureRevokesProvisionalManifest(t *testing.T) {
	requireLocalPGRestore(t)
	f := newBkpFixture(t)
	f.seedProbeTable(t)
	backup := f.backup(t, nil)
	target := f.createDatabase(t, "tgt_verify_evidence_denied")
	binding := f.bindTestVerifyTarget(t, target)

	if _, err := f.ctrl.Exec(f.ctx, `CREATE FUNCTION deny_backup_manifest_evidence() RETURNS trigger
LANGUAGE plpgsql AS $$ BEGIN
  IF NEW.kind = 'backup_manifest' THEN RAISE EXCEPTION 'controlled evidence acceptance failure'; END IF;
  RETURN NEW;
END $$`); err != nil {
		t.Fatalf("create controlled evidence failure function: %v", err)
	}
	if _, err := f.ctrl.Exec(f.ctx, `CREATE TRIGGER deny_backup_manifest_evidence
BEFORE INSERT ON recovery_evidence FOR EACH ROW EXECUTE FUNCTION deny_backup_manifest_evidence()`); err != nil {
		t.Fatalf("create controlled evidence failure trigger: %v", err)
	}
	t.Cleanup(func() {
		_, _ = f.ctrl.Exec(f.ctx, `DROP TRIGGER IF EXISTS deny_backup_manifest_evidence ON recovery_evidence`)
		_, _ = f.ctrl.Exec(f.ctx, `DROP FUNCTION IF EXISTS deny_backup_manifest_evidence()`)
	})

	result, err := ExecuteVerifyBackup(f.ctx, VerifyBackupOptions{
		ManifestPath: backup.ManifestPath, Binding: binding, TargetDSN: target,
		Verifier: "auth:verifier", InstanceID: f.instanceID, ControlStore: f.store,
		ControlDSN: f.ctrlDSN, AuthoritativeDSN: f.srcDSN, ObserverDSN: target,
		ProgramVersion: bkpProgramVersion, PG: f.pg,
	})
	if err == nil {
		t.Fatalf("evidence insertion failure unexpectedly succeeded: %+v", result)
	}
	if result.State != VerificationUnverified || result.Checks != (ManifestChecks{
		Readable: true, StructureConstraints: true, BusinessStateProbes: true, VerificationExecutable: true,
	}) {
		t.Fatalf("failed evidence acceptance must retain observed checks but stay unverified: result=%+v", result)
	}
	if strings.TrimSpace(result.Reason) == "" || !strings.Contains(result.Reason, "accept") {
		t.Fatalf("unverified result lacks the evidence-acceptance failure reason: %+v", result)
	}
	written := bkpReadManifest(t, backup.ManifestPath)
	if written.Verification.State != VerificationUnverified || written.Verification.EvidenceRef != "" {
		t.Fatalf("failed evidence acceptance left a reusable manifest: %+v", written.Verification)
	}
	if got := f.controlEvidenceCount(t, "backup_manifest", backup.Manifest.BackupID); got != 0 {
		t.Fatalf("failed evidence acceptance inserted %d backup_manifest rows", got)
	}
	guard, found, guardErr := controlstore.ReadTargetGuard(f.ctx, f.ctrl, binding.TargetGuardKey())
	if guardErr != nil || !found || guard.State != controlstore.TargetGuardRebuildRequired || guard.ActiveWriter {
		t.Fatalf("failed evidence acceptance must leave the target dirty: found=%t guard=%+v err=%v", found, guard, guardErr)
	}
}
