//go:build integration

package recovery

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

// A verified backup is bound to its immutable open instance target. A copied
// manifest and valid backup_manifest evidence must not authorize restoring it
// into a different database; refusal must precede the marker and all probes.
func TestRestoreRejectsTargetOutsideImmutableInstanceBinding(t *testing.T) {
	f := newBkpFixture(t)
	f.seedProbeTable(t)
	backup := f.backup(t, nil)

	verifyTarget := f.createDatabase(t, "tgt_verify_wrong_restore_target")
	if verified := f.verifyBackup(t, backup.ManifestPath, verifyTarget); verified.State != VerificationVerified {
		t.Fatalf("verification state = %q, want verified", verified.State)
	}

	manifestCopy := filepath.Join(f.artDir, "copied-manifest.json")
	bkpCopyFile(t, backup.ManifestPath, manifestCopy)
	wrongTarget := f.createDatabase(t, "tgt_wrong_restore_binding")
	wrongIdentity, err := controlstore.ParseDSNTarget(wrongTarget)
	if err != nil {
		t.Fatalf("parse wrong restore target: %v", err)
	}
	boundIdentity, err := controlstore.ParseDSNTarget(f.restoreTargetDSN)
	if err != nil {
		t.Fatalf("parse immutable instance target: %v", err)
	}
	if wrongIdentity.SameDatabase(boundIdentity) {
		t.Fatal("negative fixture accidentally uses the immutable instance target")
	}

	result, err := ExecuteRestore(f.ctx, RestoreOptions{
		ManifestPath:      manifestCopy,
		InstanceID:        f.instanceID,
		ControlStore:      f.store,
		ControlDSN:        f.ctrlDSN,
		TargetDSN:         wrongTarget,
		ObserverDSN:       wrongTarget,
		TargetDeclaration: TargetProductionMain,
		TargetReason:      "controlled wrong-target integration case",
		Actor:             "deploy:executor",
		ProgramVersion:    bkpProgramVersion,
		PG:                f.pg,
	})
	if err == nil || result.Restored {
		t.Fatalf("restore to an unbound target was accepted: result=%+v err=%v", result, err)
	}
	if !strings.Contains(err.Error(), "target binding") {
		t.Fatalf("wrong-target refusal did not identify the binding precondition: %v", err)
	}
	if got := f.userTableCount(t, wrongTarget); got != 0 {
		t.Fatalf("wrong-target refusal created %d user tables", got)
	}
	if n := rsiMarkerAuditCount(t, f, backup.Manifest.BackupID); n != 0 {
		t.Fatalf("wrong-target refusal wrote %d restore_started markers, want 0", n)
	}
	if n := f.controlEvidenceCount(t, "restore_probe", backup.Manifest.BackupID); n != 0 {
		t.Fatalf("wrong-target refusal wrote %d restore_probe rows, want 0", n)
	}
}
