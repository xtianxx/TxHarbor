//go:build integration

package recovery

import (
	"context"
	"io"
	"strings"
	"sync/atomic"
	"testing"
)

type restoreReasonCountingPG struct {
	inner PGCommand
	calls atomic.Int64
}

func (pg *restoreReasonCountingPG) Run(ctx context.Context, name string, args []string,
	stdin io.Reader, stdout, stderr io.Writer) error {
	pg.calls.Add(1)
	return pg.inner.Run(ctx, name, args, stdin, stdout, stderr)
}

func TestExecuteRestoreRejectsCredentialShapedReasonBeforeAnyWork(t *testing.T) {
	const canary = "restore-reason-canary"
	tests := []struct {
		name   string
		reason string
	}{
		{name: "dsn", reason: "postgres://operator:" + canary + "@db.example.test/app"},
		{name: "pem", reason: "-----BEGIN PRIVATE KEY-----\n" + canary + "\n-----END PRIVATE KEY-----"},
		{name: "password", reason: "password=" + canary},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ExecuteRestore(context.Background(), RestoreOptions{TargetReason: tt.reason})
			if err == nil {
				t.Fatal("ExecuteRestore succeeded, want credential-shaped reason refusal")
			}
			if strings.Contains(err.Error(), canary) || strings.Contains(strings.Join(result.Blocked, " "), canary) {
				t.Fatalf("credential canary escaped in refusal: err=%q blocked=%q", err, result.Blocked)
			}
			if !strings.Contains(err.Error(), "target reason contains credential-shaped material") {
				t.Fatalf("refusal is not the generic reason error: %v", err)
			}
			if result.Restored || len(result.Blocked) != 1 {
				t.Fatalf("result=%+v, want one early generic refusal", result)
			}
		})
	}
}

func TestValidateRestoreTargetReasonAllowsOrdinaryReason(t *testing.T) {
	if err := ValidateRestoreTargetReason("approved maintenance-window recovery"); err != nil {
		t.Fatalf("safe reason rejected: %v", err)
	}
}

func TestRestoreCredentialShapedReasonDoesNotPersistOrTouchTarget(t *testing.T) {
	f := newBkpFixture(t)
	backup := f.backup(t, nil)
	verifyTarget := f.createDatabase(t, "reason_verify")
	if got := f.verifyBackup(t, backup.ManifestPath, verifyTarget); got.State != VerificationVerified {
		t.Fatalf("backup verification state = %q, want verified", got.State)
	}

	// Preserve a known target object so the refusal can prove that pg_restore
	// did not run and the target was not modified.
	target := f.createDatabase(t, "reason_refusal")
	targetPool := bkpOpenPool(t, target)
	t.Cleanup(targetPool.Close)
	if _, err := targetPool.Exec(f.ctx, `CREATE TABLE public.reason_guard_sentinel(value text);
INSERT INTO public.reason_guard_sentinel(value) VALUES ('unchanged')`); err != nil {
		t.Fatalf("seed refusal target: %v", err)
	}
	countingPG := &restoreReasonCountingPG{inner: f.pg}

	const canary = "fake-restore-reason-canary"
	reasons := []string{
		"postgres://operator:" + canary + "@db.example.test/app",
		"-----BEGIN PRIVATE KEY-----\n" + canary + "\n-----END PRIVATE KEY-----",
		"password=" + canary,
	}
	startedBefore := restoreReasonAuditCount(t, f)
	probesBefore := f.controlEvidenceCount(t, "restore_probe", backup.Manifest.BackupID)
	for _, reason := range reasons {
		result, err := ExecuteRestore(f.ctx, RestoreOptions{
			ManifestPath: backup.ManifestPath, InstanceID: f.instanceID,
			ControlStore: f.store, ControlDSN: f.ctrlDSN, TargetDSN: target,
			TargetDeclaration: TargetProductionMain, TargetReason: reason,
			Actor: "deploy:executor", ProgramVersion: bkpProgramVersion,
			PG: countingPG,
		})
		if err == nil || result.Restored {
			t.Fatalf("credential-shaped reason accepted: result=%+v err=%v", result, err)
		}
		if strings.Contains(err.Error(), canary) || strings.Contains(strings.Join(result.Blocked, " "), canary) {
			t.Fatalf("canary echoed by restore refusal: err=%q blocked=%q", err, result.Blocked)
		}
		if !strings.Contains(err.Error(), "target reason contains credential-shaped material") {
			t.Fatalf("unexpected refusal: %v", err)
		}
	}
	if got := restoreReasonAuditCount(t, f); got != startedBefore {
		t.Fatalf("restore_started audit count = %d, before attempts %d", got, startedBefore)
	}
	if got := f.controlEvidenceCount(t, "restore_probe", backup.Manifest.BackupID); got != probesBefore {
		t.Fatalf("restore_probe evidence count = %d, before attempts %d", got, probesBefore)
	}
	if got := countingPG.calls.Load(); got != 0 {
		t.Fatalf("PGCommand calls = %d, want zero (no archive/restore logs or target tool activity)", got)
	}
	var sentinel string
	if err := targetPool.QueryRow(f.ctx, `SELECT value FROM public.reason_guard_sentinel`).Scan(&sentinel); err != nil {
		t.Fatalf("read refusal target sentinel: %v", err)
	}
	if sentinel != "unchanged" {
		t.Fatalf("target sentinel = %q, want unchanged", sentinel)
	}
	if text := restoreReasonPersistedText(t, f); strings.Contains(text, canary) {
		t.Fatalf("credential canary persisted in control-store JSON/text: %q", text)
	}

	// A benign annotation follows the normal real restore path and is recorded
	// as expected, proving the guard is limited to credential-shaped text.
	safeTarget := f.restoreTargetDSN
	if got := f.restore(t, backup.ManifestPath, safeTarget, TargetProductionMain,
		"approved maintenance-window recovery"); !got.Restored {
		t.Fatalf("safe-reason restore result = %+v, want restored", got)
	}
	if got := restoreReasonAuditCount(t, f); got != startedBefore+1 {
		t.Fatalf("restore_started audit count after safe restore = %d, want %d", got, startedBefore+1)
	}
	if got := f.controlEvidenceCount(t, "restore_probe", backup.Manifest.BackupID); got != probesBefore+1 {
		t.Fatalf("restore_probe evidence count after safe restore = %d, want %d", got, probesBefore+1)
	}
	if text := restoreReasonPersistedText(t, f); strings.Contains(text, canary) {
		t.Fatalf("credential canary persisted after safe restore: %q", text)
	}
}

func restoreReasonAuditCount(t *testing.T, f *bkpFixture) int {
	t.Helper()
	var count int
	if err := f.ctrl.QueryRow(f.ctx,
		`SELECT count(*) FROM recovery_audit WHERE instance_id = $1 AND action = $2`,
		f.instanceID, ActionRestoreStarted).Scan(&count); err != nil {
		t.Fatalf("count restore_started audits: %v", err)
	}
	return count
}

func restoreReasonPersistedText(t *testing.T, f *bkpFixture) string {
	t.Helper()
	var audits, evidence string
	if err := f.ctrl.QueryRow(f.ctx, `
SELECT COALESCE(string_agg(to_jsonb(a)::text, ' '), '')
FROM recovery_audit AS a WHERE instance_id = $1`, f.instanceID).Scan(&audits); err != nil {
		t.Fatalf("read recovery audit text: %v", err)
	}
	if err := f.ctrl.QueryRow(f.ctx, `
SELECT COALESCE(string_agg(to_jsonb(e)::text, ' '), '')
FROM recovery_evidence AS e WHERE instance_id = $1`, f.instanceID).Scan(&evidence); err != nil {
		t.Fatalf("read recovery evidence text: %v", err)
	}
	return audits + " " + evidence
}
