//go:build integration

// drill_integration_test.go runs the real `recovery-admin drill` command
// (in-process Run with the deployment env) against the real PostgreSQL 18.6
// fixture and the real pg_dump/pg_restore binaries: a real restore into the
// recovery environment's own isolated database, the archived S12 record with
// the separate timing scopes, the explicit unconfigured-constraint annotation,
// record-only over accepted restore evidence, operation-id replay/conflict and
// the refusal to record a declared failure injection that was not observed.
//
// Docker provider missing: the package fails under CI=true or
// TXHARBOR_REQUIRE_DOCKER=1 and reports NOT RUN locally (exit 0).
package recoveryadmin

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/recovery"
)

// drillCLIArgs is the real CLI drill invocation under test.
func drillCLIArgs(manifestPath, targetDSN, instanceID, outDir string, extra ...string) []string {
	args := []string{"drill",
		"--manifest", manifestPath,
		"--target-dsn", targetDSN,
		"--instance", instanceID,
		"--chain-id", "1",
		"--out", outDir,
	}
	return append(args, extra...)
}

func TestDrillCLIRealRestoreRecordAndArchive(t *testing.T) {
	requireNativePGRestore(t)
	f := newCLIFixture(t)
	manifestPath, _ := f.verifiedBackup(t)
	drillEnv := f.createDB(t, "drill_env")
	outDir := t.TempDir()
	env := f.env("deploy:executor")
	env[config.EnvPGDSN] = drillEnv
	env[config.EnvRecoveryInstance] = f.instanceID
	// Missing privileged observer must fail before restore markers or child
	// process effects. The real invocation below also proves this configured
	// observer is passed through to ExecuteRestore.
	markersBefore := f.markerCount(t, f.manifestBackupID(t, manifestPath))
	callsBefore := f.toolCalls(t)
	withoutObserver := make(map[string]string, len(env))
	for key, value := range env {
		withoutObserver[key] = value
	}
	delete(withoutObserver, config.EnvRecoveryObserverDSN)
	code, stdout, stderr := runRecoveryAdmin(t, drillCLIArgs(manifestPath, drillEnv, f.instanceID, outDir), withoutObserver)
	if code != 1 || !strings.Contains(stderr, config.EnvRecoveryObserverDSN+" is required") {
		t.Fatalf("missing observer must refuse explicitly: exit=%d stdout=%q stderr=%q", code, stdout, stderr)
	}
	if got := f.markerCount(t, f.manifestBackupID(t, manifestPath)); got != markersBefore {
		t.Fatalf("missing observer wrote %d restore markers, want no change from %d", got, markersBefore)
	}
	if got := f.toolCalls(t); got != callsBefore {
		t.Fatalf("missing observer invoked restore tools: calls=%d, before=%d", got, callsBefore)
	}
	env[config.EnvRecoveryObserverDSN] = f.dataDSN

	// The gate TTL is required by exact key name (the derived release
	// observation has no default TTL): nothing may run without it.
	code, stdout, stderr = runRecoveryAdmin(t, drillCLIArgs(manifestPath, drillEnv, f.instanceID, outDir), env)
	if code != 1 || !strings.Contains(stderr, config.EnvRecoveryGateTTL) {
		t.Fatalf("missing gate TTL must refuse by key name: exit=%d stdout=%q stderr=%q", code, stdout, stderr)
	}
	if strings.Contains(stdout, "drill_id=") {
		t.Fatalf("a refused drill must not claim a run: %q", stdout)
	}

	env[config.EnvRecoveryGateTTL] = "30s"

	// Real drill: real pg_restore into the environment's own data database.
	code, stdout, stderr = runRecoveryAdmin(t, drillCLIArgs(manifestPath, drillEnv, f.instanceID, outDir), env)
	if code != 1 {
		t.Fatalf("an incomplete full recovery must persist its refusal and return nonzero: exit=%d stdout=%q stderr=%q", code, stdout, stderr)
	}
	drillID := cliField(stdout, "drill_id")
	if drillID == "" || cliField(stdout, "result") != "refused_safe" {
		t.Fatalf("restore without observed verification/release stages must not claim a complete drill: %q", stdout)
	}
	if got := cliField(stdout, "db_restore_seconds"); got == "" || got == "not_measured" {
		t.Fatalf("a real restore must measure db_restore_seconds: %q", stdout)
	}
	if got := cliField(stdout, "verification_state"); got != "not_configured" {
		t.Fatalf("verification state = %q, want not_configured (no RPC configured; never a pass)", got)
	}
	if got := cliField(stdout, "constraints_configured"); got != "false" {
		t.Fatalf("constraints_configured = %q, want false with no constraint env", got)
	}
	if !strings.Contains(stdout, "rto status=not_configured") {
		t.Fatalf("with no configured target the RTO verdict must be not_configured (nothing claimed): %q", stdout)
	}
	if strings.Contains(stdout, "txharbor:txharbor") {
		t.Fatalf("drill output leaked a DSN credential: %q", stdout)
	}

	// The recorded row carries the separate scopes and the explicit
	// unconfigured-constraint annotation.
	runs, err := recovery.DrillRuns(f.ctx, f.store, f.instanceID)
	if err != nil {
		t.Fatalf("DrillRuns: %v", err)
	}
	if len(runs) != 1 {
		t.Fatalf("drill rows = %d, want 1", len(runs))
	}
	run := runs[0]
	if run.DrillID != drillID || run.Result != recovery.DrillResultRefusedSafe || run.Scenario != recovery.DrillScenarioFullRecovery {
		t.Fatalf("recorded run differs: %+v", run)
	}
	if run.DBRestoreSeconds == nil || *run.DBRestoreSeconds <= 0 {
		t.Fatalf("db_restore_seconds not recorded: %v", run.DBRestoreSeconds)
	}
	if run.VerificationSeconds != nil {
		t.Fatalf("verification_seconds must stay not-measured when the phase is not_configured, got %v", *run.VerificationSeconds)
	}
	if run.ConstraintsConfigured {
		t.Fatalf("constraints_configured must be false with no constraint env")
	}
	var inputs struct {
		Purpose                 string   `json:"purpose"`
		UnconfiguredConstraints []string `json:"unconfigured_constraints"`
	}
	if err := json.Unmarshal(run.TestInputs, &inputs); err != nil {
		t.Fatalf("decode test_inputs: %v", err)
	}
	if !strings.Contains(inputs.Purpose, "not production thresholds") || len(inputs.UnconfiguredConstraints) != 4 {
		t.Fatalf("test_inputs annotation incomplete: %+v", inputs)
	}
	// The per-capability payload is explicit for all seven capabilities.
	var payloadText string
	if err := f.control.QueryRow(f.ctx,
		`SELECT capability_release_seconds::text FROM recovery_drill_run WHERE drill_id = $1`, drillID).Scan(&payloadText); err != nil {
		t.Fatalf("read capability payload: %v", err)
	}
	var releases map[string]any
	if err := json.Unmarshal([]byte(payloadText), &releases); err != nil {
		t.Fatalf("decode capability payload: %v", err)
	}
	if len(releases) != 7 {
		t.Fatalf("capability payload has %d keys, want 7: %s", len(releases), payloadText)
	}

	// The archive exists and references the recorded row.
	archivePath := filepath.Join(outDir, "drill-"+drillID+".json")
	if cliField(stdout, "archive") != archivePath {
		t.Fatalf("archive reference = %q, want %q", cliField(stdout, "archive"), archivePath)
	}
	archiveBytes, err := os.ReadFile(archivePath)
	if err != nil {
		t.Fatalf("read archive: %v", err)
	}
	var archiveDoc map[string]any
	if err := json.Unmarshal(archiveBytes, &archiveDoc); err != nil {
		t.Fatalf("decode archive: %v", err)
	}
	if archiveDoc["drill_id"] != drillID || archiveDoc["result"] != "refused_safe" {
		t.Fatalf("archive identity differs: %v", archiveDoc)
	}
	if archiveDoc["db_restore_seconds"] == nil || archiveDoc["local_values_only"] != true {
		t.Fatalf("archive S12 sections incomplete: %v", archiveDoc)
	}

	// Operation-id replay: same input reads the recorded outcome back with
	// zero new rows; a changed input conflicts with zero writes.
	opArgs := drillCLIArgs(manifestPath, drillEnv, f.instanceID, outDir, "--operation-id", "drill-op-1")
	code, stdout, stderr = runRecoveryAdmin(t, opArgs, env)
	if code != 1 {
		t.Fatalf("incomplete drill with operation id must return nonzero: exit=%d stdout=%q stderr=%q", code, stdout, stderr)
	}
	code, stdout, stderr = runRecoveryAdmin(t, opArgs, env)
	if code != 1 || !strings.Contains(stdout, "replayed=true") || !strings.Contains(stdout, "result_class=refused_safe") {
		t.Fatalf("same operation id + input must replay: exit=%d stdout=%q stderr=%q", code, stdout, stderr)
	}
	conflict := append(append([]string{}, opArgs...), "--backup-lag-seconds", "5")
	code, _, stderr = runRecoveryAdmin(t, conflict, env)
	if code != 1 || !strings.Contains(stderr, "operation_conflict") {
		t.Fatalf("changed input under the same operation id must conflict: exit=%d stderr=%q", code, stderr)
	}
	assertDrillRowCount(t, f, 2)

	// Record-only records from accepted restore evidence without claiming a
	// restore of its own.
	code, stdout, stderr = runRecoveryAdmin(t, drillCLIArgs(manifestPath, drillEnv, f.instanceID, outDir,
		"--record-only", "--operation-id", "drill-op-2"), env)
	if code != 1 || cliField(stdout, "result") != "refused_safe" {
		t.Fatalf("record-only must persist incomplete stages and return nonzero: exit=%d stdout=%q stderr=%q", code, stdout, stderr)
	}
	if got := cliField(stdout, "db_restore_seconds"); got != "not_measured" {
		t.Fatalf("record-only db_restore_seconds = %q, want not_measured", got)
	}
	assertDrillRowCount(t, f, 3)

	// A declared failure injection that was not observed is never recorded as
	// a false injection claim.
	code, _, stderr = runRecoveryAdmin(t, drillCLIArgs(manifestPath, drillEnv, f.instanceID, outDir,
		"--scenario", "f1_backup_unusable_or_unverified", "--operation-id", "drill-op-inj"), env)
	if code != 1 || !strings.Contains(stderr, "was not observed") {
		t.Fatalf("an unobserved injection must be refused: exit=%d stderr=%q", code, stderr)
	}
	assertDrillRowCount(t, f, 3)
}

// assertDrillRowCount counts the recorded drill rows of the fixture instance.
func assertDrillRowCount(t *testing.T, f *cliFixture, want int) {
	t.Helper()
	var rows int
	if err := f.control.QueryRow(f.ctx,
		`SELECT count(*) FROM recovery_drill_run WHERE instance_id = $1`, f.instanceID).Scan(&rows); err != nil {
		t.Fatalf("count drill rows: %v", err)
	}
	if rows != want {
		t.Fatalf("drill rows = %d, want %d", rows, want)
	}
}

// TestDrillCLIRefusesUnverifiedDrillTargets pins the pre-restore guards: a
// target that is not the environment's own data database and a record-only
// run without accepted restore evidence both refuse before writing anything.
func TestDrillCLIRefusesUnverifiedDrillTargets(t *testing.T) {
	requireNativePGRestore(t)
	f := newCLIFixture(t)
	manifestPath, _ := f.verifiedBackup(t)
	outDir := t.TempDir()
	env := f.env("deploy:executor")
	env[config.EnvRecoveryGateTTL] = "30s"

	// A target that is not TXHARBOR_PG_DSN is refused.
	other := f.createDB(t, "other_env")
	code, _, stderr := runRecoveryAdmin(t, drillCLIArgs(manifestPath, other, f.instanceID, outDir), env)
	if code != 1 || !strings.Contains(stderr, config.EnvPGDSN) {
		t.Fatalf("a mismatched target must refuse by key name: exit=%d stderr=%q", code, stderr)
	}

	// record-only without accepted restore evidence (none exists yet) is
	// refused: a re-initialized empty database is never a drill.
	env[config.EnvPGDSN] = other
	code, _, stderr = runRecoveryAdmin(t, drillCLIArgs(manifestPath, other, f.instanceID, outDir, "--record-only"), env)
	if code != 1 || !strings.Contains(stderr, "record-only requires an accepted restore_probe evidence row") {
		t.Fatalf("record-only without restore evidence must refuse: exit=%d stderr=%q", code, stderr)
	}
	var rows int
	if err := f.control.QueryRow(f.ctx,
		`SELECT count(*) FROM recovery_drill_run WHERE instance_id = $1`, f.instanceID).Scan(&rows); err != nil {
		t.Fatalf("count drill rows: %v", err)
	}
	if rows != 0 {
		t.Fatalf("refused drills wrote %d rows, want 0", rows)
	}
}

// A restore probe is not a generic instance-wide permission: record-only must
// reject a different backup and a probe that has fallen behind the current
// evidence generation.
func TestDrillCLIRecordOnlyRejectsMismatchedAndStaleProbe(t *testing.T) {
	requireNativePGRestore(t)
	f := newCLIFixture(t)
	manifestA, _ := f.verifiedBackup(t)
	drillEnv := f.createDB(t, "drill_record_binding")
	outDir := t.TempDir()
	env := f.env("deploy:executor")
	env[config.EnvPGDSN] = drillEnv
	env[config.EnvRecoveryInstance] = f.instanceID
	env[config.EnvRecoveryGateTTL] = "30s"

	code, stdout, stderr := runRecoveryAdmin(t, drillCLIArgs(manifestA, drillEnv, f.instanceID, outDir), env)
	if code != 1 || cliField(stdout, "result") != "refused_safe" {
		t.Fatalf("restore should persist incomplete-stage refusal: exit=%d stdout=%q stderr=%q", code, stdout, stderr)
	}
	assertDrillRowCount(t, f, 1)

	// A new verified backup advances the instance generation and has no
	// restore_probe of its own. It must not borrow manifest A's probe.
	manifestB := f.cliBackup(t, "drill-binding-backup-b")
	verifyTarget := f.createDB(t, "drill_record_verify_b")
	code, stdout, stderr = f.cliVerify(t, manifestB, verifyTarget, "drill-binding-verify-b")
	if code != 0 || !strings.Contains(stdout, "verification=verified") {
		t.Fatalf("verify backup B: exit=%d stdout=%q stderr=%q", code, stdout, stderr)
	}

	for name, manifest := range map[string]string{"backup B without probe A": manifestB, "stale probe A": manifestA} {
		t.Run(name, func(t *testing.T) {
			code, stdout, stderr := runRecoveryAdmin(t, drillCLIArgs(manifest, drillEnv, f.instanceID, outDir, "--record-only"), env)
			if code != 1 || !strings.Contains(stderr, "record-only requires an accepted restore_probe evidence row") {
				t.Fatalf("mismatched/stale restore evidence must refuse: exit=%d stdout=%q stderr=%q", code, stdout, stderr)
			}
		})
	}
	assertDrillRowCount(t, f, 1)
}

// TestDrillCLILocalInputAnnotations pins the explicit local-input annotation:
// operator-provided lag/interval values are recorded as test inputs, never as
// production measurements, and the RTO target env is judged end-to-end only.
func TestDrillCLILocalInputAnnotations(t *testing.T) {
	requireNativePGRestore(t)
	f := newCLIFixture(t)
	manifestPath, _ := f.verifiedBackup(t)
	drillEnv := f.createDB(t, "drill_annot")
	outDir := t.TempDir()
	env := f.env("deploy:executor")
	env[config.EnvPGDSN] = drillEnv
	env[config.EnvRecoveryInstance] = f.instanceID
	env[config.EnvRecoveryGateTTL] = "30s"
	env[config.EnvRecoveryRPOTarget] = "1h"
	env[config.EnvRecoveryRTOTarget] = "1m"

	code, stdout, stderr := runRecoveryAdmin(t, drillCLIArgs(manifestPath, drillEnv, f.instanceID, outDir,
		"--backup-lag-seconds", "42.5", "--uncovered-interval-seconds", "7"), env)
	if code != 1 || cliField(stdout, "result") != "refused_safe" {
		t.Fatalf("incomplete annotated drill must persist refused_safe and return nonzero: exit=%d stdout=%q stderr=%q", code, stdout, stderr)
	}
	if got := cliField(stdout, "constraints_configured"); got != "false" {
		t.Fatalf("constraints_configured = %q, want false (frequency/retention unconfigured)", got)
	}
	if !strings.Contains(stdout, "backup_lag state=provided seconds=42.5") {
		t.Fatalf("operator-provided lag must be recorded as a test input: %q", stdout)
	}
	// A configured RTO target with no end-to-end measurement stays
	// not_measured: the db restore scope alone is never a verdict.
	if !strings.Contains(stdout, "rto status=not_measured target_configured=true") {
		t.Fatalf("db_restore_seconds alone must never produce an RTO verdict: %q", stdout)
	}
	if strings.Contains(stdout, "txharbor:txharbor") {
		t.Fatalf("drill output leaked a DSN credential: %q", stdout)
	}
	runs, err := recovery.DrillRuns(f.ctx, f.store, f.instanceID)
	if err != nil || len(runs) != 1 {
		t.Fatalf("DrillRuns = %v / %v", runs, err)
	}
	var lag struct {
		State   string  `json:"state"`
		Seconds float64 `json:"seconds"`
		Purpose string  `json:"purpose"`
	}
	if err := json.Unmarshal(runs[0].BackupLag, &lag); err != nil {
		t.Fatalf("decode backup lag: %v", err)
	}
	if lag.State != "provided" || lag.Seconds != 42.5 || !strings.Contains(lag.Purpose, "local test input") {
		t.Fatalf("backup lag annotation differs: %+v", lag)
	}
	var interval struct {
		State   string  `json:"state"`
		Seconds float64 `json:"seconds"`
		Purpose string  `json:"purpose"`
	}
	if err := json.Unmarshal(runs[0].UncoveredInterval, &interval); err != nil {
		t.Fatalf("decode uncovered interval: %v", err)
	}
	if interval.State != "provided" || interval.Seconds != 7 || !strings.Contains(interval.Purpose, "local test input") {
		t.Fatalf("uncovered interval annotation differs: %+v", interval)
	}
	var testInputs struct {
		Purpose         string `json:"purpose"`
		LocalValuesOnly bool   `json:"local_values_only"`
		RTO             struct {
			Status        string `json:"status"`
			Target        int64  `json:"target"`
			MeasuredKnown bool   `json:"measured_known"`
		} `json:"rto"`
		RTOTargetConfigured bool `json:"rto_target_configured"`
	}
	if err := json.Unmarshal(runs[0].TestInputs, &testInputs); err != nil {
		t.Fatalf("decode test inputs: %v", err)
	}
	if !strings.Contains(testInputs.Purpose, "not production thresholds") || !testInputs.LocalValuesOnly ||
		testInputs.RTO.Status != "not_measured" || testInputs.RTO.Target != int64(60*1000000000) ||
		testInputs.RTO.MeasuredKnown || !testInputs.RTOTargetConfigured {
		t.Fatalf("recorded test-input/RTO annotation differs: %+v", testInputs)
	}
	archivePath := cliField(stdout, "archive")
	archiveBytes, err := os.ReadFile(archivePath)
	if err != nil {
		t.Fatalf("read archive %q: %v", archivePath, err)
	}
	var archiveDoc struct {
		DrillID           string         `json:"drill_id"`
		Result            string         `json:"result"`
		BackupLag         map[string]any `json:"backup_lag"`
		UncoveredInterval map[string]any `json:"uncovered_interval"`
		LocalValuesOnly   bool           `json:"local_values_only"`
		RTO               struct {
			Status        string `json:"status"`
			MeasuredKnown bool   `json:"measured_known"`
		} `json:"rto"`
	}
	if err := json.Unmarshal(archiveBytes, &archiveDoc); err != nil {
		t.Fatalf("decode archive: %v", err)
	}
	if archiveDoc.DrillID != runs[0].DrillID || archiveDoc.Result != "refused_safe" || !archiveDoc.LocalValuesOnly ||
		archiveDoc.BackupLag["state"] != "provided" || archiveDoc.BackupLag["seconds"] != 42.5 ||
		archiveDoc.UncoveredInterval["state"] != "provided" || archiveDoc.UncoveredInterval["seconds"] != float64(7) ||
		archiveDoc.RTO.Status != "not_measured" || archiveDoc.RTO.MeasuredKnown {
		t.Fatalf("archive local-input/RTO annotation differs: %+v", archiveDoc)
	}
	if _, err := strconv.ParseFloat(cliField(stdout, "db_restore_seconds"), 64); err != nil {
		t.Fatalf("db_restore_seconds is not numeric: %q", stdout)
	}
}
