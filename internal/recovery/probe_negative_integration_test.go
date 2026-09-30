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
//   - no accepted backup/restore success evidence is produced (0-row assertions);
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

	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
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

	// Real isolated verify: a real pg_restore plus the four checks. The failed
	// post-restore probe is unaccepted, so this is unverified rather than a
	// rejected conclusion (which would claim a conclusion the checks did not
	// establish).
	requireLocalPGRestore(t)
	verifyTarget := f.createDatabase(t, "tgt_probe_negative")
	binding := f.bindTestVerifyTarget(t, verifyTarget)
	result, err := ExecuteVerifyBackup(f.ctx, VerifyBackupOptions{
		ManifestPath:     backup.ManifestPath,
		Binding:          binding,
		TargetDSN:        verifyTarget,
		Verifier:         "auth:verifier",
		InstanceID:       f.instanceID,
		ControlStore:     f.store,
		ControlDSN:       f.ctrlDSN,
		AuthoritativeDSN: f.srcDSN,
		ObserverDSN:      verifyTarget,
		ProgramVersion:   bkpProgramVersion,
		PG:               f.pg,
	})
	if err == nil {
		t.Fatalf("ExecuteVerifyBackup returned success for a missing authoritative object: %+v", result)
	}
	if !strings.Contains(err.Error(), "isolated verification was not accepted") {
		t.Fatalf("missing-object verification failed for an unexpected reason: %v", err)
	}
	if result.State != VerificationUnverified {
		t.Fatalf("verify state = %q, want %q (checks=%+v)", result.State, VerificationUnverified, result.Checks)
	}
	if result.Checks.Readable || result.Checks.StructureConstraints ||
		result.Checks.BusinessStateProbes || result.Checks.VerificationExecutable {
		t.Fatalf("unaccepted post-restore probe must not claim any successful check, got %+v", result.Checks)
	}

	// No accepted evidence of any kind, and the manifest write-back remains
	// unverified (no certified manifest to reuse).
	if n := f.controlEvidenceCount(t, "backup_manifest", backup.Manifest.BackupID); n != 0 {
		t.Fatalf("backup_manifest evidence rows = %d, want 0", n)
	}
	if n := f.controlEvidenceCount(t, "restore_probe", backup.Manifest.BackupID); n != 0 {
		t.Fatalf("restore_probe evidence rows = %d, want 0", n)
	}
	guard, found, guardErr := controlstore.ReadTargetGuard(f.ctx, f.ctrl, binding.TargetGuardKey())
	if guardErr != nil || !found || guard.State != controlstore.TargetGuardRebuildRequired || guard.ActiveWriter {
		t.Fatalf("failed missing-object verification must leave its target dirty: found=%t guard=%+v err=%v", found, guard, guardErr)
	}
	written := bkpReadManifest(t, backup.ManifestPath)
	if written.Verification.State != VerificationUnverified {
		t.Fatalf("manifest write-back state = %q, want %q", written.Verification.State, VerificationUnverified)
	}

	// Per-dimension detail: the failed real probe identifies the missing
	// authoritative object and affected FR-002 category. The target carries the
	// restored-but-incomplete data set; this detail is not an accepted proof.
	outcome := probeRestoredTarget(f.ctx, f.pg, backup.ManifestPath, written, verifyTarget)
	if !outcome.Checks.Readable || outcome.Checks.StructureConstraints ||
		outcome.Checks.BusinessStateProbes || outcome.Checks.VerificationExecutable {
		t.Fatalf("probe coverage detail checks = %+v, want readable-only detail", outcome.Checks)
	}
	assertProbeCoverage(t, "business_state_coverage", outcome.Business, true)
	assertProbeCoverage(t, "verification_coverage", outcome.Verification, false)
	if !coverageNamesMissingObject(outcome.Business, probeMissingObject, probeMissingObjectCategory) {
		t.Fatalf("probe evidence did not identify missing authoritative object %q in category %q: %+v", probeMissingObject, probeMissingObjectCategory, outcome.Business)
	}

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

func coverageNamesMissingObject(items []probeItem, object, category string) bool {
	for _, item := range items {
		if item.Category == category && item.State != "proven" && strings.Contains(item.Reason, object) {
			return true
		}
	}
	return false
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
