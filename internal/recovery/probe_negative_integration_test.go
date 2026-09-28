//go:build integration

// probe_negative_integration_test.go is the G2 real negative case: a required
// probe cannot complete because one probe-representative authoritative object
// is not in the restored target (the source table is renamed before the dump).
// It runs the real backup entry, the real verify-backup entry (real isolated
// pg_restore + the four checks) and the real restore entry against a fresh
// isolated target, and asserts:
//
//   - the affected FR-002 coverage category is recorded unknown/failed and
//     only that category loses its proven state; the other eight categories
//     and the readable dimension stay as the contract expects;
//   - no verified/restored success evidence is produced (0-row assertions);
//   - the restore entry refuses with an explicit blocked state and leaves the
//     target untouched (no best-effort restore, no pre-write marker);
//   - the CLI-level "no success beyond the evidence" assertion lives in
//     internal/app/recoveryadmin/backup_restore_integration_test.go, where the
//     real CLI runs with the real pg_restore from the pinned image.
//
// Scope note: this test verifies the restore/probe coverage machinery (the
// four checks and the per-category coverage states). It is NOT a V1-V9
// acceptance run: no V1-V9 verification item is executed or concluded here.
package recovery

import (
	"strings"
	"testing"
)

// probeMissingObject is the representative object renamed away from the
// source before the backup: the manifest still declares it (the canonical
// coverage list) but the dump cannot restore a relation with that name.
const probeMissingObject = "consumer_inbox"

const probeMissingObjectCategory = "consumer_idempotency_progress"

func TestVerifyBackupRejectsMissingAuthoritativeObject(t *testing.T) {
	f := newBkpFixture(t)
	f.seedProbeTable(t)

	// Damage the authoritative object set at the source: the table is renamed,
	// so the dump carries a different name and the restored target cannot
	// satisfy the declared object.
	if _, err := f.src.Exec(f.ctx,
		`ALTER TABLE `+probeMissingObject+` RENAME TO `+probeMissingObject+`_015_damaged`); err != nil {
		t.Fatalf("rename %s in the source: %v", probeMissingObject, err)
	}
	backup := f.backup(t, nil)

	// Real isolated verify: a real pg_restore plus the four checks, concluded
	// rejected (never verified) because a required probe cannot complete.
	verifyTarget := f.createDatabase(t, "tgt_probe_negative")
	result, err := ExecuteVerifyBackup(f.ctx, VerifyBackupOptions{
		ManifestPath:   backup.ManifestPath,
		TargetDSN:      verifyTarget,
		Verifier:       "auth:verifier",
		InstanceID:     f.instanceID,
		ControlStore:   f.store,
		ProgramVersion: bkpProgramVersion,
		PG:             f.pg,
	})
	if err != nil {
		t.Fatalf("ExecuteVerifyBackup returned an unexpected error: %v", err)
	}
	if result.State != VerificationRejected {
		t.Fatalf("verify state = %q, want %q (checks=%+v)", result.State, VerificationRejected, result.Checks)
	}
	// The readable dimension is retained; the object-dependent dimensions fail.
	if !result.Checks.Readable {
		t.Fatalf("readable dimension must be retained, got %+v", result.Checks)
	}
	if result.Checks.StructureConstraints || result.Checks.BusinessStateProbes || result.Checks.VerificationExecutable {
		t.Fatalf("object-dependent dimensions must fail, got %+v", result.Checks)
	}

	// No success evidence of any kind, and the manifest write-back is a
	// rejected conclusion (no certified manifest to reuse).
	if n := f.controlEvidenceCount(t, "backup_manifest", backup.Manifest.BackupID); n != 0 {
		t.Fatalf("backup_manifest evidence rows = %d, want 0", n)
	}
	if n := f.controlEvidenceCount(t, "restore_probe", backup.Manifest.BackupID); n != 0 {
		t.Fatalf("restore_probe evidence rows = %d, want 0", n)
	}
	written := bkpReadManifest(t, backup.ManifestPath)
	if written.Verification.State != VerificationRejected {
		t.Fatalf("manifest write-back state = %q, want %q", written.Verification.State, VerificationRejected)
	}

	// Per-dimension detail: exactly the affected FR-002 category is unknown;
	// the other eight categories stay proven in both probe views. (The
	// rejected verify-backup already ran the real pg_restore, so the target
	// carries the restored-but-incomplete data set.)
	outcome := probeRestoredTarget(f.ctx, f.pg, backup.ManifestPath, written, verifyTarget)
	if !outcome.Checks.Readable || outcome.Checks.StructureConstraints ||
		outcome.Checks.BusinessStateProbes || outcome.Checks.VerificationExecutable {
		t.Fatalf("probe outcome checks = %+v, want readable-only", outcome.Checks)
	}
	assertProbeCoverage(t, "business_state_coverage", outcome.Business, true)
	assertProbeCoverage(t, "verification_coverage", outcome.Verification, false)

	// Real restore entry: refused with an explicit blocked state; a fresh
	// target is untouched, no restored evidence and no pre-write marker.
	restoreTarget := f.createDatabase(t, "tgt_probe_negative_restore")
	refused := f.refusedRestore(t, backup.ManifestPath, restoreTarget)
	if len(refused.Blocked) == 0 {
		t.Fatal("refusal did not report the missing preconditions")
	}
	if got := f.userTableCount(t, restoreTarget); got != 0 {
		t.Fatalf("refused restore created %d tables in the target (best-effort restore)", got)
	}
	if n := f.controlEvidenceCount(t, "restore_probe", backup.Manifest.BackupID); n != 0 {
		t.Fatalf("restore_probe evidence rows after the refusal = %d, want 0", n)
	}
	if n := rsiMarkerAuditCount(t, f, backup.Manifest.BackupID); n != 0 {
		t.Fatalf("a restore that never started wrote %d pre-write marker rows, want 0", n)
	}
}

// assertProbeCoverage asserts one probe coverage view: the affected category is
// unknown with a reason (naming the missing object in the business view,
// where the reason is per-object), every other category is proven.
func assertProbeCoverage(t *testing.T, view string, items []probeItem, wantObjectNamed bool) {
	t.Helper()
	if len(items) == 0 {
		t.Fatalf("%s is empty; the probe coverage was not recorded", view)
	}
	affected := 0
	for _, item := range items {
		if item.Category == probeMissingObjectCategory {
			affected++
			if item.State != "unknown" {
				t.Fatalf("%s: affected category %s state = %q, want unknown", view, item.Category, item.State)
			}
			if strings.TrimSpace(item.Reason) == "" {
				t.Fatalf("%s: affected category %s must carry a reason", view, item.Category)
			}
			if wantObjectNamed && !strings.Contains(item.Reason, probeMissingObject) {
				t.Fatalf("%s: affected category reason = %q, want it to name %s",
					view, item.Reason, probeMissingObject)
			}
			continue
		}
		if item.State != "proven" {
			t.Fatalf("%s: unaffected category %s state = %q (%s), want proven",
				view, item.Category, item.State, item.Reason)
		}
	}
	if affected != 1 {
		t.Fatalf("%s: affected category occurrences = %d, want exactly 1", view, affected)
	}
}
