//go:build integration

package recoveryadmin

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/xtianxx/txharbor/internal/config"
)

// These tests seed restore_probe-shaped rows only to exercise the CLI's
// negative integrity boundary. They do not represent a real restore or
// establish a positive record-only/full-recovery result.
func seedDrillBindingEvidence(t *testing.T, f *cliFixture, instanceID string, manifestPath, targetDSN string) {
	t.Helper()
	manifest, err := recoveryDrillReadManifest(manifestPath)
	if err != nil {
		t.Fatalf("read test manifest: %v", err)
	}
	digest, err := manifest.Digest()
	if err != nil {
		t.Fatalf("digest test manifest: %v", err)
	}
	var generation int64
	if err := f.control.QueryRow(f.ctx,
		`SELECT evidence_generation FROM recovery_instance WHERE instance_id = $1`, instanceID).Scan(&generation); err != nil {
		t.Fatalf("read test instance generation: %v", err)
	}
	scope, err := json.Marshal(map[string]string{
		"backup_id":          manifest.BackupID,
		"manifest_digest":    digest,
		"target_fingerprint": recoveryTargetFingerprint(targetDSN),
	})
	if err != nil {
		t.Fatalf("encode test restore-probe scope: %v", err)
	}
	_, err = f.control.Exec(f.ctx, `
INSERT INTO recovery_evidence
    (evidence_id, instance_id, generation, kind, scope, artifact_hash,
     artifact_ref, observed_at, collected_by)
VALUES ($1, $2, $3, 'restore_probe', $4, $5, 'negative-integrity-test-setup', now(), 'test-fixture')`,
		uuid.NewString(), instanceID, generation, scope, digest)
	if err != nil {
		t.Fatalf("seed test-only restore-probe-shaped evidence: %v", err)
	}
}

func drillBindingCLISetup(t *testing.T) (*cliFixture, string, string, string, map[string]string) {
	t.Helper()
	f := newCLIFixture(t)
	// The real CLI backup produces a well-formed manifest, but these negative
	// integrity tests do not need or claim a restore-positive chain.
	manifestPath := f.cliBackup(t, "drill-binding-negative-test")
	targetDSN := f.createDB(t, "drill_binding_target")
	env := f.env("deploy:executor")
	env[config.EnvPGDSN] = targetDSN
	env[config.EnvRecoveryInstance] = f.instanceID
	env[config.EnvRecoveryGateTTL] = "30s"
	return f, manifestPath, targetDSN, t.TempDir(), env
}

func assertDrillBindingRefusalHasNoSuccessWrites(t *testing.T, f *cliFixture, outDir string, callsBefore int) {
	t.Helper()
	assertDrillRowCount(t, f, 0)
	var successfulAudits int
	if err := f.control.QueryRow(f.ctx, `
SELECT count(*) FROM recovery_audit
WHERE instance_id = $1 AND action IN ('drill', 'drill_run') AND result = 'ok'`, f.instanceID).Scan(&successfulAudits); err != nil {
		t.Fatalf("count successful drill audits: %v", err)
	}
	if successfulAudits != 0 {
		t.Fatalf("successful drill audit rows=%d, want zero", successfulAudits)
	}
	archives, err := filepath.Glob(filepath.Join(outDir, "drill-*.json"))
	if err != nil {
		t.Fatalf("list drill archives: %v", err)
	}
	if len(archives) != 0 {
		t.Fatalf("refused record-only invocation left drill archives: %v", archives)
	}
	if got := f.toolCalls(t); got != callsBefore {
		t.Fatalf("record-only refusal invoked target/backup tool shim: calls=%d, before=%d", got, callsBefore)
	}
}

func TestDrillCLIRecordOnlyRejectsWrongTargetFingerprint(t *testing.T) {
	f, manifestPath, targetDSN, outDir, env := drillBindingCLISetup(t)
	otherTarget := f.createDB(t, "drill_binding_wrong_target")
	seedDrillBindingEvidence(t, f, f.instanceID, manifestPath, otherTarget)
	callsBefore := f.toolCalls(t)

	code, stdout, stderr := runRecoveryAdmin(t, drillCLIArgs(manifestPath, targetDSN, f.instanceID, outDir, "--record-only"), env)
	if code != 1 || !strings.Contains(stderr, "record-only requires an accepted restore_probe evidence row") {
		t.Fatalf("wrong target-fingerprint probe must refuse: exit=%d stdout=%q stderr=%q", code, stdout, stderr)
	}
	assertDrillBindingRefusalHasNoSuccessWrites(t, f, outDir, callsBefore)
}

func TestDrillCLIRecordOnlyRejectsProbeBoundToAnotherInstance(t *testing.T) {
	f, manifestPath, targetDSN, outDir, env := drillBindingCLISetup(t)
	otherInstanceID := uuid.NewString()
	// At most one recovery instance may be open. Create a closed historical
	// recovery instance in the negative-test fixture and bind the synthetic
	// probe-shaped row to it; this is not evidence of an actual restore.
	if _, err := f.control.Exec(f.ctx, `
INSERT INTO recovery_instance
    (instance_id, kind, state, evidence_generation, evidence_hash, opened_by,
     closed_by, closed_at, target_guard_key, target_role_fingerprint,
     entry_chain_inventory, entry_chain_inventory_version)
SELECT $1, kind, 'closed', 0, evidence_hash, opened_by, opened_by, now(),
       target_guard_key, target_role_fingerprint, entry_chain_inventory,
       entry_chain_inventory_version
FROM recovery_instance WHERE instance_id = $2`, otherInstanceID, f.instanceID); err != nil {
		t.Fatalf("seed closed historical fixture instance: %v", err)
	}
	seedDrillBindingEvidence(t, f, otherInstanceID, manifestPath, targetDSN)
	callsBefore := f.toolCalls(t)

	code, stdout, stderr := runRecoveryAdmin(t, drillCLIArgs(manifestPath, targetDSN, f.instanceID, outDir, "--record-only"), env)
	if code != 1 || !strings.Contains(stderr, "record-only requires an accepted restore_probe evidence row") {
		t.Fatalf("another instance's probe must refuse: exit=%d stdout=%q stderr=%q", code, stdout, stderr)
	}
	assertDrillBindingRefusalHasNoSuccessWrites(t, f, outDir, callsBefore)
}

func TestDrillCLIRecordOnlyRejectsGenerationChangeBeforeRecord(t *testing.T) {
	f, manifestPath, targetDSN, outDir, env := drillBindingCLISetup(t)
	seedDrillBindingEvidence(t, f, f.instanceID, manifestPath, targetDSN)
	callsBefore := f.toolCalls(t)

	// Hold the real instance row lock before launching the CLI. Its evidence
	// check is read-only and succeeds; observing its later FOR UPDATE wait
	// proves it has reached RecordDrillRun before advancing the generation.
	lockTx, err := f.control.Begin(f.ctx)
	if err != nil {
		t.Fatalf("begin generation interleaving transaction: %v", err)
	}
	defer func() { _ = lockTx.Rollback(context.Background()) }()
	var generation int64
	if err := lockTx.QueryRow(f.ctx,
		`SELECT evidence_generation FROM recovery_instance WHERE instance_id = $1 FOR UPDATE`, f.instanceID).Scan(&generation); err != nil {
		t.Fatalf("lock test instance: %v", err)
	}

	type cliResult struct {
		code   int
		stdout string
		stderr string
	}
	result := make(chan cliResult, 1)
	go func() {
		code, stdout, stderr := runRecoveryAdmin(t,
			drillCLIArgs(manifestPath, targetDSN, f.instanceID, outDir, "--record-only"), env)
		result <- cliResult{code: code, stdout: stdout, stderr: stderr}
	}()

	deadline := time.Now().Add(10 * time.Second)
	for {
		var blocked bool
		if err := f.control.QueryRow(f.ctx, `
SELECT EXISTS (
    SELECT 1 FROM pg_stat_activity
	WHERE datname = current_database() AND wait_event_type = 'Lock'
	  AND query ILIKE '%recovery_instance%' AND query ILIKE '%FOR UPDATE%'
)`).Scan(&blocked); err != nil {
			t.Fatalf("observe CLI blocked at record transaction: %v", err)
		}
		if blocked {
			break
		}
		if time.Now().After(deadline) {
			var active []string
			rows, err := f.control.Query(f.ctx, `SELECT wait_event_type || ':' || query FROM pg_stat_activity WHERE datname = current_database() AND state = 'active'`)
			if err == nil {
				for rows.Next() {
					var query string
					if rows.Scan(&query) == nil {
						active = append(active, query)
					}
				}
				rows.Close()
			}
			t.Fatalf("record-only CLI did not reach the locked RecordDrillRun instance query; active=%v", active)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := lockTx.Exec(f.ctx,
		`UPDATE recovery_instance SET evidence_generation = evidence_generation + 1 WHERE instance_id = $1`, f.instanceID); err != nil {
		t.Fatalf("advance evidence generation while holding lock: %v", err)
	}
	if err := lockTx.Commit(f.ctx); err != nil {
		t.Fatalf("commit generation advance: %v", err)
	}

	select {
	case got := <-result:
		if got.code != 1 || !strings.Contains(got.stderr, "became stale before the drill row was inserted") {
			t.Fatalf("generation race must refuse before insert: exit=%d stdout=%q stderr=%q", got.code, got.stdout, got.stderr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("record-only CLI did not finish after generation transaction committed")
	}
	assertDrillBindingRefusalHasNoSuccessWrites(t, f, outDir, callsBefore)

	var generationAfter int64
	if err := f.control.QueryRow(f.ctx,
		`SELECT evidence_generation FROM recovery_instance WHERE instance_id = $1`, f.instanceID).Scan(&generationAfter); err != nil {
		t.Fatalf("read generation after refusal: %v", err)
	}
	if generationAfter != generation+1 {
		t.Fatalf("test interleaving generation=%d, want %d", generationAfter, generation+1)
	}
}
