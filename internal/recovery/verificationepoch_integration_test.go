//go:build integration

package recovery

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

func TestVerifyBackupFailedRepeatRevokesSavedVerifiedCopy(t *testing.T) {
	requireLocalPGRestore(t)
	f := newBkpFixture(t)
	f.seedProbeTable(t)
	backup := f.backup(t, nil)
	firstTarget := f.createDatabase(t, "epoch_repeat_first")
	firstVerification := f.verifyBackup(t, backup.ManifestPath, firstTarget)
	if firstVerification.State != VerificationVerified {
		t.Fatalf("initial verification state=%q, want verified", firstVerification.State)
	}
	oldCopy := filepath.Join(f.artDir, "saved-verified-copy.json")
	if err := os.WriteFile(oldCopy, mustReadFile(t, backup.ManifestPath), 0o600); err != nil {
		t.Fatalf("save original verified manifest: %v", err)
	}

	badCopy := filepath.Join(f.artDir, "compatibility-refusal.json")
	refusedManifest := bkpReadManifest(t, backup.ManifestPath)
	refusedManifest.Program.MinCompatible = "999.0"
	bkpWriteManifest(t, badCopy, refusedManifest)
	secondTarget := f.createDatabase(t, "epoch_repeat_second")
	binding := f.bindTestVerifyTarget(t, secondTarget)
	result, err := ExecuteVerifyBackup(f.ctx, bkpVerifyOptions(f, badCopy, secondTarget, binding, f.instanceID))
	if err != nil || result.State != VerificationRejected || !strings.Contains(result.Reason, "compatible") {
		t.Fatalf("repeat compatibility refusal = (%+v, %v), want rejected with basis", result, err)
	}
	before := probeSnapshot(t, f, f.restoreTargetDSN)
	refused := f.refusedRestore(t, oldCopy, f.restoreTargetDSN)
	if !blockedContains(refused, "no current verified receipt") {
		t.Fatalf("saved prior verified copy did not fail specifically on its revoked receipt: %+v", refused)
	}
	if after := probeSnapshot(t, f, f.restoreTargetDSN); after != before {
		t.Fatalf("refused restore changed authoritative target data: before=%v after=%v", before, after)
	}
	if got := rsiMarkerAuditCount(t, f, backup.Manifest.BackupID); got != 0 {
		t.Fatalf("stale receipt refusal wrote %d restore_started markers", got)
	}
	if got := f.controlEvidenceCount(t, "restore_probe", backup.Manifest.BackupID); got != 0 {
		t.Fatalf("stale receipt refusal accepted %d restore_probe rows", got)
	}
	if got := f.controlEvidenceCount(t, "backup_manifest", backup.Manifest.BackupID); got != 1 {
		t.Fatalf("repeat failure changed immutable accepted evidence history: rows=%d, want 1", got)
	}
}

func TestVerifyBackupCrashAfterStartedCommitRevokesOldProof(t *testing.T) {
	requireLocalPGRestore(t)
	f := newBkpFixture(t)
	f.seedProbeTable(t)
	backup := f.backup(t, nil)
	verifyTarget := f.createDatabase(t, "epoch_crash_first")
	firstVerification := f.verifyBackup(t, backup.ManifestPath, verifyTarget)
	if firstVerification.State != VerificationVerified {
		t.Fatalf("initial verification state=%q, want verified", firstVerification.State)
	}
	oldCopy := filepath.Join(f.artDir, "saved-before-crash.json")
	if err := os.WriteFile(oldCopy, mustReadFile(t, backup.ManifestPath), 0o600); err != nil {
		t.Fatalf("save verified manifest: %v", err)
	}
	manifest := bkpReadManifest(t, oldCopy)
	binding := f.bindTestVerifyTarget(t, f.createDatabase(t, "epoch_crash_unstarted"))
	epoch, err := beginBackupVerificationEpoch(f.ctx, f.store, manifest, "auth:verifier", "crash-after-start",
		f.instanceID, binding.TargetFingerprint(), binding.RoleFingerprint(), bkpProgramVersion)
	if err != nil || epoch.AuditID <= 0 {
		t.Fatalf("commit START epoch before simulated crash: epoch=%+v err=%v", epoch, err)
	}
	before := probeSnapshot(t, f, f.restoreTargetDSN)
	refused := f.refusedRestore(t, oldCopy, f.restoreTargetDSN)
	if !blockedContains(refused, "no current verified receipt") {
		t.Fatalf("old proof did not fail specifically on its revoked receipt after START: %+v", refused)
	}
	if after := probeSnapshot(t, f, f.restoreTargetDSN); after != before {
		t.Fatalf("refused restore changed authoritative target data: before=%v after=%v", before, after)
	}
	if got := rsiMarkerAuditCount(t, f, backup.Manifest.BackupID); got != 0 {
		t.Fatalf("crash-after-START refusal wrote %d restore_started markers", got)
	}
	if got := f.controlEvidenceCount(t, "restore_probe", backup.Manifest.BackupID); got != 0 {
		t.Fatalf("crash-after-START refusal accepted %d restore_probe rows", got)
	}
}

func TestVerifyBackupConcurrentTargetsRejectSupersededEpoch(t *testing.T) {
	requireLocalPGRestore(t)
	f := newBkpFixture(t)
	f.seedProbeTable(t)
	backup := f.backup(t, nil)
	targetA := f.createDatabase(t, "epoch_concurrent_a")
	targetB := f.createDatabase(t, "epoch_concurrent_b")
	bindingA := bindEpochVerifyTarget(t, f, targetA, "epoch-concurrent-a")
	bindingB := bindEpochVerifyTarget(t, f, targetB, "epoch-concurrent-b")
	locker := bkpConnect(t, targetA)
	defer locker.Close(context.Background())
	if _, err := locker.Exec(f.ctx, `CREATE TABLE `+bkpProbeTable+` (id bigint, note text)`); err != nil {
		t.Fatalf("prepare real restore barrier table: %v", err)
	}
	if _, err := locker.Exec(f.ctx, `BEGIN`); err != nil {
		t.Fatalf("begin real restore barrier: %v", err)
	}
	if _, err := locker.Exec(f.ctx, `LOCK TABLE `+bkpProbeTable+` IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatalf("lock first verification target: %v", err)
	}
	type verifyOutcome struct {
		result VerifyBackupResult
		err    error
	}
	first := make(chan verifyOutcome, 1)
	go func() {
		firstResult, firstErr := ExecuteVerifyBackup(f.ctx, bkpVerifyOptions(f, backup.ManifestPath, targetA, bindingA, ""))
		first <- verifyOutcome{firstResult, firstErr}
	}()
	targetADB := bkpDBNameOf(t, f.adminDSN, targetA)
	deadline := time.Now().Add(30 * time.Second)
	for {
		select {
		case outcome := <-first:
			t.Fatalf("first verification finished before target lock barrier: result=%+v err=%v", outcome.result, outcome.err)
		default:
		}
		rows, err := f.admin.Query(f.ctx, `SELECT pid FROM pg_stat_activity
WHERE datname=$1 AND wait_event_type='Lock' AND pid<>pg_backend_pid()`, targetADB)
		if err != nil {
			t.Fatalf("look up first verification lock waiter: %v", err)
		}
		waiting := rows.Next()
		rows.Close()
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("first verification did not reach target lock barrier in %s", targetADB)
		}
		time.Sleep(50 * time.Millisecond)
	}
	second, secondErr := ExecuteVerifyBackup(f.ctx, bkpVerifyOptions(f, backup.ManifestPath, targetB, bindingB, ""))
	if secondErr != nil || second.State != VerificationVerified || second.VerificationEpoch <= 0 || second.AcceptedAuditID <= 0 {
		t.Fatalf("second target verification did not accept its current epoch: result=%+v err=%v", second, secondErr)
	}
	if _, err := locker.Exec(f.ctx, `ROLLBACK`); err != nil {
		t.Fatalf("release first verification target barrier: %v", err)
	}
	firstResult := <-first
	if firstResult.err == nil || firstResult.result.State != VerificationUnverified {
		t.Fatalf("superseded concurrent attempt = (%+v, %v), want unverified refusal", firstResult.result, firstResult.err)
	}
	current, err := BackupVerificationReceiptCurrent(f.ctx, f.store, backup.Manifest.BackupID, "",
		backup.Manifest.ManifestVersion, second.ManifestDigest, second.EvidenceRef, second.AcceptedAuditID)
	if err != nil || !current {
		t.Fatalf("latest attempt receipt is not current: current=%t err=%v", current, err)
	}
	written := bkpReadManifest(t, backup.ManifestPath)
	if written.Verification.State != VerificationVerified || written.Verification.EvidenceRef != second.EvidenceRef {
		t.Fatalf("older failed attempt overwrote newer accepted manifest: %+v", written.Verification)
	}
}

func TestRestoreRechecksVerificationEpochBeforeLaunchAndAcceptance(t *testing.T) {
	requireLocalPGRestore(t)
	t.Run("prelaunch", func(t *testing.T) {
		f := newBkpFixture(t)
		f.seedProbeTable(t)
		backup := f.backup(t, nil)
		verifyTarget := f.createDatabase(t, "epoch_restore_prelaunch_verify")
		if got := f.verifyBackup(t, backup.ManifestPath, verifyTarget); got.State != VerificationVerified {
			t.Fatalf("initial verification state=%q, want verified", got.State)
		}
		manifest := bkpReadManifest(t, backup.ManifestPath)
		lockTx, err := f.ctrl.Begin(f.ctx)
		if err != nil {
			t.Fatalf("begin instance-row barrier: %v", err)
		}
		if _, err := controlstore.LockInstance(f.ctx, lockTx, f.instanceID); err != nil {
			t.Fatalf("hold instance-row barrier: %v", err)
		}
		type restoreOutcome struct {
			result RestoreResult
			err    error
		}
		done := make(chan restoreOutcome, 1)
		prelaunchTarget := f.restoreTargetDSN
		beforeData := probeSnapshot(t, f, prelaunchTarget)
		go func() {
			result, restoreErr := ExecuteRestore(f.ctx, bkpRestoreOptions(f, backup.ManifestPath, prelaunchTarget, f.pg))
			done <- restoreOutcome{result, restoreErr}
		}()
		waitForControlLockWaiter(t, f, bkpDBNameOf(t, f.adminDSN, f.ctrlDSN), 30*time.Second)
		newEpoch := startTestBackupEpoch(t, f, manifest, f.instanceID, "prelaunch-superseder")
		if err := lockTx.Commit(f.ctx); err != nil {
			t.Fatalf("release instance-row barrier: %v", err)
		}
		outcome := <-done
		if outcome.err == nil || outcome.result.Restored || !blockedContains(outcome.result, "backup verification receipt was superseded before restore launch") {
			t.Fatalf("prelaunch epoch race was accepted: result=%+v err=%v", outcome.result, outcome.err)
		}
		if !strings.Contains(outcome.err.Error(), "backup verification receipt was superseded before restore launch") {
			t.Fatalf("prelaunch epoch refusal did not surface the specific epoch cause: err=%v result=%+v", outcome.err, outcome.result)
		}
		// The refusal must come from the bound target path, not from the
		// instance-less evidence lookup: the result carries the authoritative
		// target fingerprint and never the generic missing-receipt reason.
		authoritative, err := controlstore.ParseDSNTarget(f.srcDSN)
		if err != nil {
			t.Fatalf("parse authoritative fixture target: %v", err)
		}
		if outcome.result.TargetFingerprint != authoritative.DataTargetFingerprint().TargetFingerprint {
			t.Fatalf("prelaunch refusal did not run against the bound authoritative target: fingerprint=%q want=%q",
				outcome.result.TargetFingerprint, authoritative.DataTargetFingerprint().TargetFingerprint)
		}
		if blockedContains(outcome.result, "no current verified receipt") {
			t.Fatalf("prelaunch refusal was the unbound/missing-receipt rejection, not the superseded epoch: %+v", outcome.result)
		}
		latest, err := latestBackupVerificationEpoch(f.ctx, f.store.Pool(), backup.Manifest.BackupID)
		if err != nil || latest != newEpoch.AuditID {
			t.Fatalf("superseding epoch was not the committed cause: latest=%d epoch=%d err=%v", latest, newEpoch.AuditID, err)
		}
		if afterData := probeSnapshot(t, f, prelaunchTarget); afterData != beforeData {
			t.Fatalf("prelaunch epoch refusal changed authoritative target data: before=%v after=%v", beforeData, afterData)
		}
		if newEpoch.AuditID == 0 || rsiMarkerAuditCount(t, f, backup.Manifest.BackupID) != 0 ||
			f.controlEvidenceCount(t, "restore_probe", backup.Manifest.BackupID) != 0 {
			t.Fatalf("prelaunch refusal started restore or omitted committed epoch: result=%+v epoch=%+v", outcome.result, newEpoch)
		}
	})

	t.Run("acceptance", func(t *testing.T) {
		f := newBkpFixture(t)
		f.seedProbeTable(t)
		backup := f.backup(t, nil)
		verifyTarget := f.createDatabase(t, "epoch_restore_accept_verify")
		if got := f.verifyBackup(t, backup.ManifestPath, verifyTarget); got.State != VerificationVerified {
			t.Fatalf("initial verification state=%q, want verified", got.State)
		}
		manifest := bkpReadManifest(t, backup.ManifestPath)
		barrier := &epochProbeBarrier{inner: f.pg, reached: make(chan struct{}), release: make(chan struct{})}
		done := make(chan struct {
			result RestoreResult
			err    error
		}, 1)
		go func() {
			result, restoreErr := ExecuteRestore(f.ctx, bkpRestoreOptions(f, backup.ManifestPath, f.restoreTargetDSN, barrier))
			done <- struct {
				result RestoreResult
				err    error
			}{result, restoreErr}
		}()
		select {
		case <-barrier.reached:
		case <-time.After(60 * time.Second):
			t.Fatal("restore did not reach the real post-restore probe barrier")
		}
		epoch := startTestBackupEpoch(t, f, manifest, f.instanceID, "acceptance-superseder")
		close(barrier.release)
		outcome := <-done
		if outcome.err == nil || outcome.result.Restored || epoch.AuditID == 0 {
			t.Fatalf("acceptance epoch race was accepted: result=%+v err=%v epoch=%+v", outcome.result, outcome.err, epoch)
		}
		if got := f.controlEvidenceCount(t, "restore_probe", backup.Manifest.BackupID); got != 0 {
			t.Fatalf("superseded restore accepted %d restore_probe rows", got)
		}
	})
}

func blockedContains(result RestoreResult, fragment string) bool {
	for _, reason := range result.Blocked {
		if strings.Contains(reason, fragment) {
			return true
		}
	}
	return false
}

func probeSnapshot(t *testing.T, f *bkpFixture, dsn string) [2]int {
	t.Helper()
	before, after := f.probeCount(t, dsn)
	return [2]int{before, after}
}

func TestVerifyBackupRevokedSuccessCannotReplayAsVerified(t *testing.T) {
	f := newBkpFixture(t)
	f.seedProbeTable(t)
	backup := f.backup(t, nil)
	target := f.createDatabase(t, "epoch_replay_target")
	verified := f.verifyBackup(t, backup.ManifestPath, target)
	if verified.State != VerificationVerified {
		t.Fatalf("initial verification state=%q, want verified", verified.State)
	}
	manifest := bkpReadManifest(t, backup.ManifestPath)
	startTestBackupEpoch(t, f, manifest, f.instanceID, "replay-revoker")
	current, err := BackupVerificationReceiptCurrent(f.ctx, f.store, backup.Manifest.BackupID, f.instanceID,
		manifest.ManifestVersion, verified.ManifestDigest, verified.EvidenceRef, verified.AcceptedAuditID)
	if err != nil || current {
		t.Fatalf("revoked verification replay is still current: current=%t err=%v", current, err)
	}
}

// epochIONonRootOutcome is the fixture-to-parent report of one non-root
// verification helper run. It carries only credential-free facts: whether the
// artifact open was actually denied, the verification state/reason, and the
// helper's own failure text.
type epochIONonRootOutcome struct {
	OK             bool              `json:"ok"`
	Failure        string            `json:"failure,omitempty"`
	UID            int               `json:"uid"`
	GID            int               `json:"gid"`
	ArtifactDenied bool              `json:"artifact_denied"`
	State          VerificationState `json:"state,omitempty"`
	Reason         string            `json:"reason,omitempty"`
	Err            string            `json:"err,omitempty"`
}

// epochIONonRootVerify is assigned by the linux helper file. It stages exactly
// this test's own temporary paths for uid/gid 65534, re-executes the real test
// binary as that identity, and returns the child's report. On platforms where
// the integration suite cannot spawn the helper it stays nil, and the caller
// refuses instead of accepting a root-readable artifact as IO uncertainty.
var epochIONonRootVerify func(t *testing.T, opts VerifyBackupOptions, artifactPath string) epochIONonRootOutcome

// epochIONonRootUID/GID are the unprivileged identity of the helper child used
// when this test runs as root. They are declared here (not in the linux helper
// file) so this test file stays buildable on every GOOS.
const (
	epochIONonRootUID = 65534
	epochIONonRootGID = 65534
)

func TestVerifyBackupIntegrityIOUncertaintyRemainsUnverified(t *testing.T) {
	requireLocalPGRestore(t)
	f := newBkpFixture(t)
	f.seedProbeTable(t)
	backup := f.backup(t, nil)
	target := f.createDatabase(t, "epoch_integrity_io_target")
	initial := f.verifyBackup(t, backup.ManifestPath, target)
	if initial.State != VerificationVerified {
		t.Fatalf("initial verification state=%q, want verified", initial.State)
	}
	manifest := bkpReadManifest(t, backup.ManifestPath)
	artifactPath := resolveArtifactPath(backup.ManifestPath, manifest.Artifacts[0].Path)
	info, err := os.Stat(artifactPath)
	if err != nil {
		t.Fatalf("stat artifact before permission denial: %v", err)
	}
	if err := os.Chmod(artifactPath, 0); err != nil {
		t.Fatalf("make artifact unreadable to this test process: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(artifactPath, info.Mode().Perm()) })

	openBindings, err := readOpenIsolatedBindings(f.ctx, f.store)
	if err != nil {
		t.Fatalf("read open target bindings: %v", err)
	}
	binding, err := BindIsolatedTarget(f.srcDSN, f.ctrlDSN, target, openBindings)
	if err != nil {
		t.Fatalf("bind existing isolated target: %v", err)
	}
	opts := bkpVerifyOptions(f, backup.ManifestPath, target, binding, f.instanceID)

	var result VerifyBackupResult
	var verifyErr error
	if os.Geteuid() == 0 {
		// Root bypasses the mode-000 artifact, so the uncertainty
		// counterexample can only be produced by a real non-root child process
		// that opens the artifact, proves the actual EACCES, and then runs the
		// real ExecuteVerifyBackup against the fixture control store and
		// binding. The helper's own report is asserted below.
		if epochIONonRootVerify == nil {
			t.Fatalf("non-root verification helper is unavailable on this platform; refusing to accept a root-readable artifact as IO uncertainty")
		}
		outcome := epochIONonRootVerify(t, opts, artifactPath)
		if !outcome.OK {
			t.Fatalf("non-root verification helper did not complete: %s", outcome.Failure)
		}
		if outcome.UID != epochIONonRootUID || outcome.GID != epochIONonRootGID {
			t.Fatalf("non-root verification helper did not run as uid/gid %d:%d: uid=%d gid=%d",
				epochIONonRootUID, epochIONonRootGID, outcome.UID, outcome.GID)
		}
		if !outcome.ArtifactDenied {
			t.Fatalf("non-root verification helper did not prove the artifact EACCES: %+v", outcome)
		}
		if outcome.Err == "" {
			t.Fatalf("non-root verification helper returned no error for the denied artifact: %+v", outcome)
		}
		result = VerifyBackupResult{State: outcome.State, Reason: outcome.Reason}
		verifyErr = errors.New(outcome.Err)
	} else {
		// Not root: the current identity is really blocked by mode 000. Prove
		// the actual EACCES before asserting IO uncertainty (no generic
		// failure and no not-reached shortcut).
		probe, probeErr := os.Open(artifactPath)
		if probeErr == nil {
			_ = probe.Close()
			t.Fatalf("artifact mode 000 is still readable by uid %d; refusing to assert IO uncertainty", os.Geteuid())
		}
		if !errors.Is(probeErr, fs.ErrPermission) {
			t.Fatalf("artifact open error=%v, want permission denied", probeErr)
		}
		result, verifyErr = ExecuteVerifyBackup(f.ctx, opts)
	}
	if verifyErr == nil || result.State != VerificationUnverified ||
		!strings.Contains(result.Reason, "integrity could not be determined") || !strings.Contains(result.Reason, "permission denied") {
		t.Fatalf("permission-denied artifact read = (%+v, %v), want unverified with safe uncertainty reason", result, verifyErr)
	}
	written := bkpReadManifest(t, backup.ManifestPath)
	if written.Verification.State != VerificationUnverified || written.Verification.EvidenceRef != "" {
		t.Fatalf("indeterminate read did not leave manifest unverified: %+v", written.Verification)
	}
	if got := f.controlEvidenceCount(t, "backup_manifest", backup.Manifest.BackupID); got != 1 {
		t.Fatalf("indeterminate read changed immutable accepted evidence history: rows=%d, want 1", got)
	}
	var unboundRows int
	if err := f.ctrl.QueryRow(f.ctx, `SELECT count(*) FROM recovery_evidence
WHERE kind='backup_manifest' AND instance_id IS NULL AND scope->>'backup_id'=$1`, backup.Manifest.BackupID).Scan(&unboundRows); err != nil {
		t.Fatalf("count unbound backup_manifest rows: %v", err)
	}
	if unboundRows != 0 {
		t.Fatalf("indeterminate read recorded an unbound (instance_id NULL) proof: rows=%d", unboundRows)
	}
	current, err := BackupVerificationReceiptCurrent(f.ctx, f.store, backup.Manifest.BackupID, f.instanceID,
		manifest.ManifestVersion, initial.ManifestDigest, initial.EvidenceRef, initial.AcceptedAuditID)
	if err != nil || current {
		t.Fatalf("prior receipt remained current after indeterminate repeat: current=%t err=%v", current, err)
	}
}

func TestCheckManifestIntegrityRejectsDeterminateCorruption(t *testing.T) {
	requireLocalPGRestore(t)
	f := newBkpFixture(t)
	f.seedProbeTable(t)
	backup := f.backup(t, nil)
	original := bkpArtifactPath(t, backup.ManifestPath, backup.Manifest.Artifacts[0])
	originalBytes := mustReadFile(t, original)
	bitflipPath := filepath.Join(f.artDir, "integrity-bitflip.pgcustom")
	bitflip := append([]byte(nil), originalBytes...)
	bitflip[len(bitflip)/2] ^= 0xff
	if err := os.WriteFile(bitflipPath, bitflip, 0o600); err != nil {
		t.Fatalf("write bit-flipped artifact: %v", err)
	}
	truncatedPath := filepath.Join(f.artDir, "integrity-truncated.pgcustom")
	if err := os.WriteFile(truncatedPath, originalBytes[:len(originalBytes)/2], 0o600); err != nil {
		t.Fatalf("write truncated artifact: %v", err)
	}

	cases := []struct {
		name   string
		mutate func(*Manifest)
	}{
		{"missing artifact", func(m *Manifest) { m.Artifacts[0].Path = "absent-integrity-test.pgcustom" }},
		{"truncated artifact", func(m *Manifest) { m.Artifacts[0].Path = filepath.Base(truncatedPath) }},
		{"bit-flipped artifact", func(m *Manifest) { m.Artifacts[0].Path = filepath.Base(bitflipPath) }},
		{"byte-count mismatch", func(m *Manifest) { m.Artifacts[0].Bytes++ }},
		{"sha256 mismatch", func(m *Manifest) { m.Artifacts[0].SHA256 = "sha256:" + strings.Repeat("0", 64) }},
		{"missing artifact entry", func(m *Manifest) { m.Artifacts = nil }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := *backup.Manifest
			m.Artifacts = append([]ManifestArtifact(nil), backup.Manifest.Artifacts...)
			tc.mutate(&m)
			if _, err := checkManifestIntegrity(backup.ManifestPath, &m); err == nil || !definiteIntegrityRefusal(err) {
				t.Fatalf("checkManifestIntegrity error=%v, want determinate refusal", err)
			}
		})
	}
}

func TestUnboundVerificationReceiptCannotBePromotedAfterRevocation(t *testing.T) {
	requireLocalPGRestore(t)
	f := newBkpFixture(t)
	f.seedProbeTable(t)
	backup := f.backup(t, nil)
	target := f.createDatabase(t, "epoch_unbound_target")
	binding := f.bindTestVerifyTarget(t, target)
	result, err := ExecuteVerifyBackup(f.ctx, bkpVerifyOptions(f, backup.ManifestPath, target, binding, ""))
	if err != nil || result.State != VerificationVerified {
		t.Fatalf("unbound verification setup: result=%+v err=%v", result, err)
	}
	manifest := bkpReadManifest(t, backup.ManifestPath)
	startTestBackupEpoch(t, f, manifest, "", "unbound-revoker")
	digest, err := manifest.Digest()
	if err != nil {
		t.Fatal(err)
	}
	_, current, err := currentBackupVerificationBinding(f.ctx, f.store.Pool(), f.instanceID, manifest, digest)
	if err != nil || current {
		t.Fatalf("unbound or revoked receipt was promotable into instance %s: current=%t err=%v", f.instanceID, current, err)
	}
	var boundRows int
	if err := f.ctrl.QueryRow(f.ctx, `SELECT count(*) FROM recovery_evidence WHERE kind='backup_manifest' AND instance_id=$1 AND scope->>'backup_id'=$2`, f.instanceID, backup.Manifest.BackupID).Scan(&boundRows); err != nil {
		t.Fatal(err)
	}
	if boundRows != 0 {
		t.Fatalf("unexpected fabricated bound proof rows=%d", boundRows)
	}
}

func TestBackupVerificationSurvivesUnrelatedGenerationAdvance(t *testing.T) {
	requireLocalPGRestore(t)
	f := newBkpFixture(t)
	f.seedProbeTable(t)
	backup := f.backup(t, nil)
	verifyTarget := f.createDatabase(t, "epoch_generation_target")
	verified := f.verifyBackup(t, backup.ManifestPath, verifyTarget)
	if verified.State != VerificationVerified {
		t.Fatalf("initial verification state=%q, want verified", verified.State)
	}
	manifest := bkpReadManifest(t, backup.ManifestPath)
	token, err := CaptureEvidenceToken(f.ctx, f.store.Pool(), f.instanceID)
	if err != nil {
		t.Fatal(err)
	}
	write, err := CommitEvidenceWrite(f.ctx, f.store, EvidenceWriteRequest{
		InstanceID: f.instanceID, Token: token, Kind: MutationEvidenceSnapshotAccepted,
		Actor: "deploy:executor", Reason: "unrelated accepted evidence generation advance",
		OperationID: "unrelated-verification-epoch-test", ResultDigest: []byte("unrelated"),
		Apply: func(ctx context.Context, tx pgx.Tx, accepted EvidenceToken) error {
			scope := []byte(`{"purpose":"unrelated-generation-advance"}`)
			return insertEvidence(ctx, tx, newUUIDString(), accepted.InstanceID, accepted.Generation,
				"verification_batch", scope, "sha256:unrelated", "test://unrelated", "deploy:executor")
		},
	})
	if err != nil || write.Discarded || write.Token.Generation <= token.Generation {
		t.Fatalf("advance unrelated instance generation: outcome=%+v err=%v", write, err)
	}
	digest, err := manifest.Digest()
	if err != nil {
		t.Fatal(err)
	}
	run := &restoreRun{opts: RestoreOptions{ControlStore: f.store}}
	current := controlEvidenceMatches(f.ctx, f.store, f.instanceID, manifest, digest, run)
	if !current {
		t.Fatalf("unrelated generation advance invalidated epoch-bound backup proof: blocked=%v result=%+v", run.blocked, verified)
	}
}

func bkpVerifyOptions(f *bkpFixture, manifestPath, target string, binding IsolatedTarget, instanceID string) VerifyBackupOptions {
	return VerifyBackupOptions{
		ManifestPath: manifestPath, Binding: binding, TargetDSN: target,
		Verifier: "auth:verifier", InstanceID: instanceID, ControlStore: f.store,
		ControlDSN: f.ctrlDSN, AuthoritativeDSN: f.srcDSN, ObserverDSN: target,
		ProgramVersion: bkpProgramVersion,
		OperationID: "epoch-test-" + strings.TrimSuffix(filepath.Base(manifestPath), filepath.Ext(manifestPath)) + "-" +
			strings.TrimPrefix(binding.TargetFingerprint(), "sha256:")[:12],
		PG: f.pg,
	}
}

func bkpRestoreOptions(f *bkpFixture, manifestPath, target string, pg PGCommand) RestoreOptions {
	return RestoreOptions{
		ManifestPath: manifestPath, InstanceID: f.instanceID, ControlStore: f.store,
		ControlDSN: f.ctrlDSN, TargetDSN: target, ObserverDSN: target,
		TargetDeclaration: TargetProductionMain, TargetReason: "verification epoch integration test",
		Actor: "deploy:executor", ProgramVersion: bkpProgramVersion, PG: pg,
		Convergence: f.deploymentConvergence(target),
	}
}

func startTestBackupEpoch(t *testing.T, f *bkpFixture, m *Manifest, instanceID, operationID string) BackupVerificationEpoch {
	t.Helper()
	epoch, err := beginBackupVerificationEpoch(f.ctx, f.store, m, "auth:verifier", operationID,
		instanceID, "test-target-fingerprint", "test-role-fingerprint", bkpProgramVersion)
	if err != nil {
		t.Fatalf("start backup verification epoch: %v", err)
	}
	return epoch
}

func bindEpochVerifyTarget(t *testing.T, f *bkpFixture, targetDSN, label string) IsolatedTarget {
	t.Helper()
	open, err := readOpenIsolatedBindings(f.ctx, f.store)
	if err != nil {
		t.Fatalf("read open target bindings: %v", err)
	}
	binding, err := BindIsolatedTarget(f.srcDSN, f.ctrlDSN, targetDSN, open)
	if err != nil {
		t.Fatalf("bind epoch test target: %v", err)
	}
	tx, err := f.ctrl.Begin(f.ctx)
	if err != nil {
		t.Fatalf("begin epoch target guard setup: %v", err)
	}
	defer func() { _ = tx.Rollback(f.ctx) }()
	if err := controlstore.InitializeTargetGuard(f.ctx, tx, binding.TargetGuardKey(), label+"-initialize"); err != nil {
		t.Fatalf("initialize epoch target guard: %v", err)
	}
	evidence, err := json.Marshal(map[string]string{
		"test_fixture": "verification-epoch", "target_guard_key": binding.TargetGuardKey(),
		"target_role_fingerprint": binding.RoleFingerprint(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := controlstore.ResolveTargetGuardClean(f.ctx, tx, binding.TargetGuardKey(), label+"-clean", evidence); err != nil {
		t.Fatalf("resolve epoch target guard clean: %v", err)
	}
	if err := tx.Commit(f.ctx); err != nil {
		t.Fatalf("commit epoch target guard setup: %v", err)
	}
	return binding
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return data
}

func waitForControlLockWaiter(t *testing.T, f *bkpFixture, database string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var count int
		err := f.admin.QueryRow(f.ctx, `SELECT count(*) FROM pg_stat_activity
WHERE datname=$1 AND wait_event_type='Lock' AND query LIKE '%recovery_instance%'`, database).Scan(&count)
		if err != nil {
			t.Fatalf("observe instance-lock waiter: %v", err)
		}
		if count > 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("no real control-store recovery_instance lock waiter observed in %s", database)
}

type epochProbeBarrier struct {
	inner   PGCommand
	reached chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *epochProbeBarrier) Run(ctx context.Context, name string, args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	if name == "pg_restore" && len(args) == 1 && args[0] == "--list" {
		b.once.Do(func() { close(b.reached) })
		select {
		case <-b.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return b.inner.Run(ctx, name, args, stdin, stdout, stderr)
}
