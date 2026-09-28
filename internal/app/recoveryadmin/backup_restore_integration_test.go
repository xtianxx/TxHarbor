//go:build integration

// backup_restore_integration_test.go runs the real `recovery-admin
// backup|verify-backup|restore` CLI (in-process Run with the deployment env)
// against a real PostgreSQL 18.6 fixture (testcontainers) and isolated target
// databases, with the real pg_dump/pg_restore binaries of the pinned image.
//
// Tool path: the host carries no PostgreSQL client. The test puts a PATH shim
// named pg_dump/pg_restore in front of PATH; the shim executes the real binary
// inside the fixture container (`docker exec -i`) and rewrites the --dbname
// DSN to the container loopback (one server, same database). The shim also
// appends every tool invocation to a counter file: that counter is the
// call-count instrument only — it never substitutes for the real tool, and
// every positive path below performs a real dump/restore.
//
// Covered:
//
//   - S1/S2: real CLI backup (unverified manifest) and verify-backup (real
//     isolated pg_restore + four checks, verified conclusion + evidence row);
//   - operation_id semantics on the real CLI: same id + same input replays
//     with zero tool calls, zero new evidence and an untouched target; same id
//     with a changed target conflicts with zero writes anywhere; an omitted id
//     is a real rerun (non-replay) that appends a new evidence row through the
//     generation protocol;
//   - interruption re-entry: an interrupted real restore never says restored,
//     writes no restored evidence, keeps the accepted pre-write invalidation
//     marker (old releases/approvals stay stale) and does not reuse failed
//     evidence: the same id replays the recorded refusal, a fresh id reruns
//     for real after the target is rebuilt;
//   - the G2 negative CLI assertion: a backup whose authoritative object set
//     is incomplete is verified as rejected and restored as refused, and the
//     CLI prints no success beyond the evidence (no over-evidence success).
//
// Fixture note: the approval/release/isolation write paths of T048-T051 are not
// delivered in this batch; this file only needs the T021/T022 preconditions
// (open instance, executor/verifier bindings, verified evidence chain), which
// it seeds through the real controlstore write paths.
//
// Docker provider missing: the package fails under CI=true or
// TXHARBOR_REQUIRE_DOCKER=1 and reports NOT RUN locally (exit 0).
package recoveryadmin

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/recovery"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore/schema"
)

const cliBkpPGImage = "postgres:18.6-trixie"

// cliProbeTable is the data probe table seeded before the backup: it is absent
// from the repository migrations, so its presence in a restored target proves
// dump-derived data (same fixture idea as internal/recovery).
const cliProbeTable = "snapshot_probe_015"

// cliPGShim executes the real tool of the same name inside the fixture
// container. It is test scaffolding around the real binary: the archive
// streams over stdin/stdout exactly like the production path.
const cliPGShim = `#!/usr/bin/env bash
set -euo pipefail
tool="$(basename "$0")"
ctr="${TXHARBOR_TEST_PG_CONTAINER:?TXHARBOR_TEST_PG_CONTAINER must name the fixture container}"
if [[ -n "${TXHARBOR_TEST_CALLS:-}" ]]; then
  printf '%s\n' "$tool" >> "$TXHARBOR_TEST_CALLS"
fi
args=()
for a in "$@"; do
  if [[ "$a" == --dbname=* ]]; then
    dsn="${a#--dbname=}"
    scheme="${dsn%%://*}"
    rest="${dsn#*://}"
    if [[ "$rest" == *@* ]]; then
      userinfo="${rest%%@*}"
      hostpath="${rest#*@}"
      path="${hostpath#*/}"
      a="--dbname=${scheme}://${userinfo}@127.0.0.1:5432/${path}"
    fi
  fi
  args+=("$a")
done
exec docker exec -i "$ctr" "$tool" "${args[@]}"
`

type cliFixture struct {
	t          *testing.T
	ctx        context.Context
	ctrID      string
	baseDSN    string
	admin      *pgxpool.Pool
	dataDSN    string
	data       *pgxpool.Pool
	controlDSN string
	control    *pgxpool.Pool
	store      *controlstore.Store
	instanceID string
	artDir     string
	callsFile  string
	seq        int
}

func newCLIFixture(t *testing.T) *cliFixture {
	t.Helper()
	ctx := context.Background()
	ctr, err := postgres.Run(ctx, cliBkpPGImage,
		postgres.WithDatabase("txharbor"),
		postgres.WithUsername("txharbor"),
		postgres.WithPassword("txharbor"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2)),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })
	baseDSN, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres connection string: %v", err)
	}
	f := &cliFixture{
		t: t, ctx: ctx, ctrID: ctr.GetContainerID(), baseDSN: baseDSN,
		admin: migrateTestPool(t, baseDSN), artDir: t.TempDir(),
		callsFile: filepath.Join(t.TempDir(), "tool-calls.log"),
	}

	// Data database: real repository migrations + the probe table.
	f.dataDSN = f.createDB(t, "data")
	if err := db.MigrateUp(ctx, db.MigrateOptions{
		DSN: f.dataDSN, LockTimeout: 30 * time.Second, ConnectTimeout: 5 * time.Second,
	}, io.Discard); err != nil {
		t.Fatalf("migrate data database: %v", err)
	}
	f.data = migrateTestPool(t, f.dataDSN)
	if _, err := f.data.Exec(ctx,
		`CREATE TABLE `+cliProbeTable+` (id bigserial PRIMARY KEY, note text NOT NULL)`); err != nil {
		t.Fatalf("seed probe table: %v", err)
	}
	if _, err := f.data.Exec(ctx,
		`INSERT INTO `+cliProbeTable+` (note) VALUES ('cli')`); err != nil {
		t.Fatalf("seed probe row: %v", err)
	}

	// Control store: independent database with the 015 schema.
	f.controlDSN = f.createDB(t, "ctrl")
	if err := db.MigrateUp(ctx, db.MigrateOptions{
		DSN: f.controlDSN, LockTimeout: 10 * time.Second, ConnectTimeout: 5 * time.Second, FS: schema.FS,
	}, io.Discard); err != nil {
		t.Fatalf("migrate control database: %v", err)
	}
	f.control = migrateTestPool(t, f.controlDSN)
	store, err := controlstore.NewStore(ctx, f.control)
	if err != nil {
		t.Fatalf("controlstore.NewStore: %v", err)
	}
	f.store = store

	// One open recovery instance with an executor and a verifier, through the
	// real control-store write paths.
	opened, err := store.OpenInstance(ctx, controlstore.OpenInstanceRequest{
		Kind: "recovery", OpenedBy: "deploy:executor",
	})
	if err != nil {
		t.Fatalf("open recovery instance: %v", err)
	}
	f.instanceID = opened.InstanceID
	f.mapIdentity(t, "deploy:executor", "person-executor")
	f.mapIdentity(t, "auth:verifier", "person-verifier")
	f.register(t, "deploy:executor", "executor")
	f.register(t, "auth:verifier", "verifier")

	// PATH shim for the real client binaries inside the pinned container.
	binDir := t.TempDir()
	for _, tool := range []string{"pg_dump", "pg_restore"} {
		if err := os.WriteFile(filepath.Join(binDir, tool), []byte(cliPGShim), 0o755); err != nil {
			t.Fatalf("write %s shim: %v", tool, err)
		}
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("TXHARBOR_TEST_PG_CONTAINER", f.ctrID)
	t.Setenv("TXHARBOR_TEST_CALLS", f.callsFile)
	return f
}

func (f *cliFixture) mapIdentity(t *testing.T, principal, person string) {
	t.Helper()
	f.seq++
	if _, err := f.store.SetIdentityMapping(f.ctx, controlstore.SetIdentityMappingRequest{
		Principal: principal, PersonID: person, RecordedBy: "deploy:admin",
		OperationID: fmt.Sprintf("cli-map-%d", f.seq),
	}); err != nil {
		t.Fatalf("map identity %s -> %s: %v", principal, person, err)
	}
}

func (f *cliFixture) register(t *testing.T, principal, role string) {
	t.Helper()
	f.seq++
	if _, err := f.store.RegisterParticipant(f.ctx, controlstore.RegisterParticipantRequest{
		InstanceID: f.instanceID, Principal: principal, Role: role,
		Actor: "deploy:admin", OperationID: fmt.Sprintf("cli-reg-%d", f.seq),
	}); err != nil {
		t.Fatalf("register %s as %s: %v", principal, role, err)
	}
}

// createDB creates a fresh database inside the fixture container.
func (f *cliFixture) createDB(t *testing.T, label string) string {
	t.Helper()
	f.seq++
	name := fmt.Sprintf("cli_%s_%d", label, f.seq)
	if _, err := f.admin.Exec(f.ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatalf("create database %s: %v", name, err)
	}
	return migrateWithDatabase(t, f.baseDSN, name)
}

// rebuildDB drops the database WITH (FORCE) and recreates it empty (the
// documented retry after an interruption).
func (f *cliFixture) rebuildDB(t *testing.T, dsn string) string {
	t.Helper()
	name := cliDBNameOf(t, dsn)
	if _, err := f.admin.Exec(f.ctx,
		"DROP DATABASE IF EXISTS "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)"); err != nil {
		t.Fatalf("drop %s: %v", name, err)
	}
	if _, err := f.admin.Exec(f.ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatalf("recreate %s: %v", name, err)
	}
	return dsn
}

func cliDBNameOf(t *testing.T, dsn string) string {
	t.Helper()
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	return strings.TrimPrefix(u.Path, "/")
}

// env builds the deployment environment for one CLI invocation; the principal
// is per-command because the CLI binds it from the environment.
func (f *cliFixture) env(principal string) map[string]string {
	return map[string]string{
		config.EnvRecoveryControlDSN:  f.controlDSN,
		config.EnvPGDSN:               f.dataDSN,
		config.EnvRecoveryPrincipal:   principal,
		config.EnvRecoveryArtifactDir: f.artDir,
	}
}

// toolCalls counts the real client invocations recorded by the PATH shim.
func (f *cliFixture) toolCalls(t *testing.T) int {
	t.Helper()
	file, err := os.Open(f.callsFile)
	if os.IsNotExist(err) {
		return 0
	}
	if err != nil {
		t.Fatalf("open tool call counter: %v", err)
	}
	defer file.Close()
	n := 0
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		if strings.TrimSpace(scanner.Text()) != "" {
			n++
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("read tool call counter: %v", err)
	}
	return n
}

func (f *cliFixture) evidenceCount(t *testing.T, kind, backupID string) int {
	t.Helper()
	var n int
	if err := f.control.QueryRow(f.ctx,
		`SELECT count(*) FROM recovery_evidence
		  WHERE instance_id = $1 AND kind = $2 AND scope->>'backup_id' = $3`,
		f.instanceID, kind, backupID).Scan(&n); err != nil {
		t.Fatalf("count evidence (%s/%s): %v", kind, backupID, err)
	}
	return n
}

// evidenceGenerations returns the row count and the distinct-generation count
// of one evidence kind: appended rows must each carry their own accepted
// generation (data-model §5).
func (f *cliFixture) evidenceGenerations(t *testing.T, kind, backupID string) (rows, generations int) {
	t.Helper()
	if err := f.control.QueryRow(f.ctx,
		`SELECT count(*), count(DISTINCT generation) FROM recovery_evidence
		  WHERE instance_id = $1 AND kind = $2 AND scope->>'backup_id' = $3`,
		f.instanceID, kind, backupID).Scan(&rows, &generations); err != nil {
		t.Fatalf("read evidence generations (%s/%s): %v", kind, backupID, err)
	}
	return rows, generations
}

func (f *cliFixture) markerCount(t *testing.T, backupID string) int {
	t.Helper()
	var n int
	if err := f.control.QueryRow(f.ctx,
		`SELECT count(*) FROM recovery_audit
		  WHERE instance_id = $1 AND action = 'restore_started' AND result = 'ok'
		    AND target->>'backup_id' = $2`,
		f.instanceID, backupID).Scan(&n); err != nil {
		t.Fatalf("count restore_started markers: %v", err)
	}
	return n
}

func (f *cliFixture) userTableCount(t *testing.T, dsn string) int {
	t.Helper()
	conn, err := pgx.Connect(f.ctx, dsn)
	if err != nil {
		t.Fatalf("connect %s: %v", cliDBNameOf(t, dsn), err)
	}
	defer func() { _ = conn.Close(f.ctx) }()
	var n int
	if err := conn.QueryRow(f.ctx,
		`SELECT count(*) FROM pg_class
		  WHERE relkind = 'r' AND relnamespace = 'public'::regnamespace
		    AND relname NOT LIKE 'pg_%'`).Scan(&n); err != nil {
		t.Fatalf("count user tables: %v", err)
	}
	return n
}

func (f *cliFixture) probeRowCount(t *testing.T, dsn string) int {
	t.Helper()
	conn, err := pgx.Connect(f.ctx, dsn)
	if err != nil {
		t.Fatalf("connect %s: %v", cliDBNameOf(t, dsn), err)
	}
	defer func() { _ = conn.Close(f.ctx) }()
	var exists bool
	if err := conn.QueryRow(f.ctx,
		`SELECT to_regclass($1) IS NOT NULL`, "public."+cliProbeTable).Scan(&exists); err != nil {
		t.Fatalf("probe table lookup: %v", err)
	}
	if !exists {
		return 0
	}
	var n int
	if err := conn.QueryRow(f.ctx, `SELECT count(*) FROM `+cliProbeTable).Scan(&n); err != nil {
		t.Fatalf("count probe rows: %v", err)
	}
	return n
}

// waitForLockWaiter waits until a pg_restore backend is parked on a lock in
// the named target database (the same deterministic barrier as the library
// integration tests).
func (f *cliFixture) waitForLockWaiter(t *testing.T, dbName string, timeout time.Duration) []int32 {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		rows, err := f.admin.Query(f.ctx,
			`SELECT pid FROM pg_stat_activity
			  WHERE datname = $1 AND wait_event_type = 'Lock' AND pid <> pg_backend_pid()
			  ORDER BY pid`, dbName)
		if err != nil {
			t.Fatalf("look up lock waiter: %v", err)
		}
		var pids []int32
		for rows.Next() {
			var pid int32
			if err := rows.Scan(&pid); err != nil {
				rows.Close()
				t.Fatalf("scan lock waiter: %v", err)
			}
			pids = append(pids, pid)
		}
		rows.Close()
		if len(pids) > 0 {
			return pids
		}
		if time.Now().After(deadline) {
			t.Fatalf("no pg_restore lock waiter in %s after %s (the restore never reached the target)", dbName, timeout)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// cliField extracts one `key=value` token from a CLI output line.
func cliField(output, key string) string {
	for _, token := range strings.Fields(output) {
		if strings.HasPrefix(token, key+"=") {
			return strings.TrimPrefix(token, key+"=")
		}
	}
	return ""
}

func (f *cliFixture) manifestBackupID(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read manifest %s: %v", path, err)
	}
	m, err := recovery.ParseManifest(data)
	if err != nil {
		t.Fatalf("parse manifest %s: %v", path, err)
	}
	return m.BackupID
}

// cliBackup runs the real CLI backup and returns the manifest path.
func (f *cliFixture) cliBackup(t *testing.T, operationID string) string {
	t.Helper()
	args := []string{"backup", "--chain-id", "1", "--out", f.artDir}
	if operationID != "" {
		args = append(args, "--operation-id", operationID)
	}
	code, out, errOut := runRecoveryAdmin(t, args, f.env("deploy:executor"))
	if code != 0 {
		t.Fatalf("CLI backup: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}
	manifestPath := cliField(out, "manifest")
	if manifestPath == "" {
		t.Fatalf("CLI backup printed no manifest path: %q", out)
	}
	return manifestPath
}

// cliVerify runs the real CLI verify-backup.
func (f *cliFixture) cliVerify(t *testing.T, manifestPath, targetDSN, operationID string) (int, string, string) {
	t.Helper()
	args := []string{"verify-backup", "--manifest", manifestPath, "--target-dsn", targetDSN, "--instance", f.instanceID}
	if operationID != "" {
		args = append(args, "--operation-id", operationID)
	}
	return runRecoveryAdmin(t, args, f.env("auth:verifier"))
}

// cliRestoreArgs is the real CLI restore invocation under test.
func cliRestoreArgs(manifestPath, targetDSN, instanceID, operationID string) []string {
	args := []string{"restore", "--manifest", manifestPath, "--target-dsn", targetDSN, "--instance", instanceID}
	if operationID != "" {
		args = append(args, "--operation-id", operationID)
	}
	return args
}

// cliRestore runs the real CLI restore.
func (f *cliFixture) cliRestore(t *testing.T, manifestPath, targetDSN, operationID string) (int, string, string) {
	t.Helper()
	return runRecoveryAdmin(t, cliRestoreArgs(manifestPath, targetDSN, f.instanceID, operationID), f.env("deploy:executor"))
}

// verifiedBackup is the S1/S2 setup: real backup + real isolated verify.
func (f *cliFixture) verifiedBackup(t *testing.T) (manifestPath, backupID string) {
	t.Helper()
	manifestPath = f.cliBackup(t, "op-bkp-1")
	backupID = f.manifestBackupID(t, manifestPath)
	target := f.createDB(t, "verify_seed")
	code, out, errOut := f.cliVerify(t, manifestPath, target, "op-ver-1")
	if code != 0 || !strings.Contains(out, "verification=verified") {
		t.Fatalf("CLI verify-backup: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}
	if got := f.evidenceCount(t, "backup_manifest", backupID); got != 1 {
		t.Fatalf("backup_manifest evidence rows after verify = %d, want 1", got)
	}
	return manifestPath, backupID
}

// TestVerifyBackupCLIOperationIDSemantics covers G3 on the real CLI: replay,
// conflict and the explicit non-replay rerun.
func TestVerifyBackupCLIOperationIDSemantics(t *testing.T) {
	f := newCLIFixture(t)
	manifestPath := f.cliBackup(t, "op-bkp-1")
	backupID := f.manifestBackupID(t, manifestPath)

	target := f.createDB(t, "verify")
	callsBefore := f.toolCalls(t)
	code, out, errOut := f.cliVerify(t, manifestPath, target, "op-ver-1")
	if code != 0 || !strings.Contains(out, "verification=verified") {
		t.Fatalf("verify-backup: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}
	if f.toolCalls(t) <= callsBefore {
		t.Fatal("verify-backup reported verified without invoking the real tool")
	}
	if got := f.evidenceCount(t, "backup_manifest", backupID); got != 1 {
		t.Fatalf("backup_manifest evidence rows = %d, want 1", got)
	}
	tablesAfterVerify := f.userTableCount(t, target)
	if tablesAfterVerify == 0 {
		t.Fatal("the isolated verify target was not actually restored")
	}

	// Replay: same operation id, same input -> recorded outcome, zero tool
	// calls, zero new evidence, untouched target.
	calls := f.toolCalls(t)
	code, out, errOut = f.cliVerify(t, manifestPath, target, "op-ver-1")
	if code != 0 || !strings.Contains(out, "replayed=true") {
		t.Fatalf("verify-backup replay: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}
	if got := f.toolCalls(t); got != calls {
		t.Fatalf("a replayed verify-backup invoked the tool %d more time(s)", got-calls)
	}
	if got := f.evidenceCount(t, "backup_manifest", backupID); got != 1 {
		t.Fatalf("a replayed verify-backup wrote evidence: %d rows", got)
	}
	if got := f.userTableCount(t, target); got != tablesAfterVerify {
		t.Fatalf("a replayed verify-backup changed the target: %d -> %d tables", tablesAfterVerify, got)
	}

	// Conflict: same operation id, different target -> refused with zero
	// writes anywhere.
	other := f.createDB(t, "verify_conflict")
	code, _, errOut = f.cliVerify(t, manifestPath, other, "op-ver-1")
	if code != 1 || !strings.Contains(errOut, "operation_conflict") {
		t.Fatalf("operation_id conflict must refuse: exit=%d stderr=%q", code, errOut)
	}
	if got := f.toolCalls(t); got != calls {
		t.Fatalf("a conflicting operation invoked the tool %d time(s)", got-calls)
	}
	if got := f.evidenceCount(t, "backup_manifest", backupID); got != 1 {
		t.Fatalf("a conflicting operation wrote evidence: %d rows", got)
	}
	if got := f.userTableCount(t, other); got != 0 {
		t.Fatalf("a conflicting operation wrote %d tables into its target", got)
	}

	// Omitted operation id: a real rerun (non-replay) that appends a new
	// evidence row through the generation protocol.
	rerun := f.createDB(t, "verify_rerun")
	calls = f.toolCalls(t)
	code, out, errOut = f.cliVerify(t, manifestPath, rerun, "")
	if code != 0 || !strings.Contains(out, "verification=verified") {
		t.Fatalf("verify-backup rerun: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}
	if strings.Contains(out, "replayed=true") {
		t.Fatalf("an omitted operation id must be a real rerun, not a replay: %q", out)
	}
	if f.toolCalls(t) <= calls {
		t.Fatal("the omitted-id rerun did not invoke the real tool")
	}
	if got := f.evidenceCount(t, "backup_manifest", backupID); got != 2 {
		t.Fatalf("backup_manifest evidence rows after the rerun = %d, want 2 (append)", got)
	}
	if rows, generations := f.evidenceGenerations(t, "backup_manifest", backupID); rows != 2 || generations != 2 {
		t.Fatalf("appended backup_manifest rows = %d with %d distinct generations, want 2/2", rows, generations)
	}
}

// TestRestoreCLIOperationIDReplayRerunAndInterruption covers G3 on the real
// CLI restore path: successful replay does not touch the target, conflicts
// write nothing, an omitted id is a real rerun with appended evidence, and an
// interrupted attempt is never reused as success.
func TestRestoreCLIOperationIDReplayRerunAndInterruption(t *testing.T) {
	f := newCLIFixture(t)
	manifestPath, backupID := f.verifiedBackup(t)

	// Positive real restore through the CLI.
	target := f.createDB(t, "restore")
	calls := f.toolCalls(t)
	code, out, errOut := f.cliRestore(t, manifestPath, target, "op-rst-1")
	if code != 0 || !strings.Contains(out, "restored=true") {
		t.Fatalf("restore: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}
	if f.toolCalls(t) <= calls {
		t.Fatal("restore reported restored=true without invoking the real tool")
	}
	if got := f.evidenceCount(t, "restore_probe", backupID); got != 1 {
		t.Fatalf("restore_probe evidence rows = %d, want 1", got)
	}
	tables := f.userTableCount(t, target)
	rows := f.probeRowCount(t, target)
	if tables == 0 || rows == 0 {
		t.Fatalf("the restore target was not actually restored (tables=%d probe rows=%d)", tables, rows)
	}

	// Successful replay (scenario 1): recorded outcome, zero tool calls, zero
	// new evidence, target untouched.
	calls = f.toolCalls(t)
	code, out, errOut = f.cliRestore(t, manifestPath, target, "op-rst-1")
	if code != 0 || !strings.Contains(out, "replayed=true") {
		t.Fatalf("restore replay: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}
	if got := f.toolCalls(t); got != calls {
		t.Fatalf("a replayed restore invoked the tool %d more time(s)", got-calls)
	}
	if got := f.evidenceCount(t, "restore_probe", backupID); got != 1 {
		t.Fatalf("a replayed restore wrote evidence: %d rows", got)
	}
	if got := f.userTableCount(t, target); got != tables || f.probeRowCount(t, target) != rows {
		t.Fatalf("a replayed restore changed the target (tables %d->%d, probe rows %d->%d)",
			tables, got, rows, f.probeRowCount(t, target))
	}

	// Conflict: same operation id, different target -> zero writes anywhere.
	conflict := f.createDB(t, "restore_conflict")
	code, _, errOut = f.cliRestore(t, manifestPath, conflict, "op-rst-1")
	if code != 1 || !strings.Contains(errOut, "operation_conflict") {
		t.Fatalf("restore operation_id conflict must refuse: exit=%d stderr=%q", code, errOut)
	}
	if got := f.toolCalls(t); got != calls {
		t.Fatalf("a conflicting restore invoked the tool %d time(s)", got-calls)
	}
	if got := f.evidenceCount(t, "restore_probe", backupID); got != 1 {
		t.Fatalf("a conflicting restore wrote evidence: %d rows", got)
	}
	if got := f.userTableCount(t, conflict); got != 0 {
		t.Fatalf("a conflicting restore wrote %d tables into its target", got)
	}

	// Omitted operation id: explicit real rerun, evidence appended.
	rerun := f.createDB(t, "restore_rerun")
	calls = f.toolCalls(t)
	code, out, errOut = f.cliRestore(t, manifestPath, rerun, "")
	if code != 0 || !strings.Contains(out, "restored=true") {
		t.Fatalf("restore rerun: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}
	if strings.Contains(out, "replayed=true") {
		t.Fatalf("an omitted operation id must be a real rerun, not a replay: %q", out)
	}
	if f.toolCalls(t) <= calls {
		t.Fatal("the omitted-id restore rerun did not invoke the real tool")
	}
	if got := f.evidenceCount(t, "restore_probe", backupID); got != 2 {
		t.Fatalf("restore_probe evidence rows after the rerun = %d, want 2 (append)", got)
	}
	if rows, generations := f.evidenceGenerations(t, "restore_probe", backupID); rows != 2 || generations != 2 {
		t.Fatalf("appended restore_probe rows = %d with %d distinct generations, want 2/2 (each acceptance records its own generation)",
			rows, generations)
	}

	// Interruption: park the real pg_restore on its first write, terminate it,
	// and verify the CLI never claims success and never writes restored
	// evidence.
	interrupted := f.createDB(t, "restore_interrupt")
	locker, err := pgx.Connect(f.ctx, interrupted)
	if err != nil {
		t.Fatalf("connect interruption target: %v", err)
	}
	if _, err := locker.Exec(f.ctx, `CREATE TABLE `+cliProbeTable+` (dummy text)`); err != nil {
		t.Fatalf("create conflicting table: %v", err)
	}
	if _, err := locker.Exec(f.ctx, `BEGIN`); err != nil {
		t.Fatalf("begin locker: %v", err)
	}
	if _, err := locker.Exec(f.ctx, `LOCK TABLE `+cliProbeTable+` IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatalf("lock conflicting table: %v", err)
	}
	type cliResult struct {
		code   int
		stdout string
		stderr string
	}
	done := make(chan cliResult, 1)
	go func() {
		code, stdout, stderr := f.cliRestore(t, manifestPath, interrupted, "op-rst-int")
		done <- cliResult{code: code, stdout: stdout, stderr: stderr}
	}()
	pids := f.waitForLockWaiter(t, cliDBNameOf(t, interrupted), 30*time.Second)
	for _, pid := range pids {
		var ok bool
		if err := f.admin.QueryRow(f.ctx, `SELECT pg_terminate_backend($1)`, pid).Scan(&ok); err != nil || !ok {
			t.Fatalf("terminate backend %d: ok=%t err=%v", pid, ok, err)
		}
	}
	if _, err := locker.Exec(f.ctx, `ROLLBACK`); err != nil {
		t.Fatalf("release locker: %v", err)
	}
	if err := locker.Close(f.ctx); err != nil {
		t.Fatalf("close locker: %v", err)
	}
	var interruptedResult cliResult
	select {
	case interruptedResult = <-done:
	case <-time.After(2 * time.Minute):
		t.Fatal("the interrupted CLI restore did not return")
	}
	if interruptedResult.code != 1 || !strings.Contains(interruptedResult.stderr, "restored=false") {
		t.Fatalf("interrupted restore must report restored=false on stderr: exit=%d stdout=%q stderr=%q",
			interruptedResult.code, interruptedResult.stdout, interruptedResult.stderr)
	}
	if strings.Contains(interruptedResult.stdout, "restored=true") {
		t.Fatalf("interrupted restore claimed success: %q", interruptedResult.stdout)
	}
	if got := f.evidenceCount(t, "restore_probe", backupID); got != 2 {
		t.Fatalf("interrupted restore wrote restored evidence: %d rows, want the previous 2", got)
	}
	// The pre-write marker of the interrupted attempt is durable: the old
	// releases/approvals cannot come back (the library test asserts the gate
	// refusals; here the marker itself is asserted).
	if got := f.markerCount(t, backupID); got != 3 {
		t.Fatalf("restore_started markers = %d, want 3 (two successful runs + the interrupted attempt)", got)
	}

	// Retry with the same failed operation id: the recorded refusal replays
	// (not success), zero tool calls and zero new evidence.
	calls = f.toolCalls(t)
	code, out, errOut = f.cliRestore(t, manifestPath, interrupted, "op-rst-int")
	if code != 1 || !strings.Contains(errOut, "replayed=true") {
		t.Fatalf("failed operation id must replay the recorded refusal: exit=%d stdout=%q stderr=%q",
			code, out, errOut)
	}
	if strings.Contains(out, "restored=true") {
		t.Fatalf("failed evidence was reused as success: %q", out)
	}
	if got := f.toolCalls(t); got != calls {
		t.Fatalf("replaying the failed operation invoked the tool %d time(s)", got-calls)
	}
	if got := f.evidenceCount(t, "restore_probe", backupID); got != 2 {
		t.Fatalf("replaying the failed operation wrote evidence: %d rows", got)
	}

	// Retry with a fresh operation id after rebuilding the target: a real
	// rerun that succeeds and appends its own evidence (no reuse of the failed
	// attempt's state).
	rebuilt := f.rebuildDB(t, interrupted)
	calls = f.toolCalls(t)
	code, out, errOut = f.cliRestore(t, manifestPath, rebuilt, "op-rst-int-2")
	if code != 0 || !strings.Contains(out, "restored=true") {
		t.Fatalf("retry after rebuild: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}
	if f.toolCalls(t) <= calls {
		t.Fatal("the retry after rebuild did not invoke the real tool")
	}
	if got := f.evidenceCount(t, "restore_probe", backupID); got != 3 {
		t.Fatalf("restore_probe evidence rows after the retry = %d, want 3", got)
	}
	if rows, generations := f.evidenceGenerations(t, "restore_probe", backupID); rows != 3 || generations != 3 {
		t.Fatalf("restore_probe rows = %d with %d distinct generations after the retry, want 3/3", rows, generations)
	}
}

// TestVerifyBackupCLINoOverEvidenceSuccessOnMissingObject is the CLI half of
// G2: with an authoritative object missing, verify-backup concludes rejected,
// restore refuses, and neither command prints a success beyond its evidence.
func TestVerifyBackupCLINoOverEvidenceSuccessOnMissingObject(t *testing.T) {
	f := newCLIFixture(t)
	// Damage the authoritative object set at the source before the dump.
	if _, err := f.data.Exec(f.ctx,
		`ALTER TABLE consumer_inbox RENAME TO consumer_inbox_015_damaged`); err != nil {
		t.Fatalf("rename consumer_inbox in the source: %v", err)
	}
	manifestPath := f.cliBackup(t, "op-bkp-neg")
	backupID := f.manifestBackupID(t, manifestPath)

	target := f.createDB(t, "verify_negative")
	code, out, errOut := f.cliVerify(t, manifestPath, target, "op-ver-neg")
	if code != 1 {
		t.Fatalf("verify-backup over a missing object must exit 1, got %d (stdout=%q stderr=%q)", code, out, errOut)
	}
	if !strings.Contains(out, "verification=rejected") {
		t.Fatalf("verify-backup must report rejected: %q", out)
	}
	if strings.Contains(out, "verification=verified") || strings.Contains(out, "verified=true") {
		t.Fatalf("verify-backup printed a success beyond its evidence: %q", out)
	}
	if !strings.Contains(out, "readable=true") ||
		!strings.Contains(out, "structure_constraints=false") ||
		!strings.Contains(out, "business_state_probes=false") ||
		!strings.Contains(out, "verification_executable=false") {
		t.Fatalf("verify-backup must expose the retained/failed dimensions: %q", out)
	}
	if got := f.evidenceCount(t, "backup_manifest", backupID); got != 0 {
		t.Fatalf("rejected verify-backup wrote backup_manifest evidence: %d rows", got)
	}

	restoreTarget := f.createDB(t, "restore_negative")
	calls := f.toolCalls(t)
	code, out, errOut = f.cliRestore(t, manifestPath, restoreTarget, "op-rst-neg")
	if code != 1 {
		t.Fatalf("restore over a rejected manifest must exit 1, got %d (stdout=%q stderr=%q)", code, out, errOut)
	}
	if !strings.Contains(errOut, "restored=false") {
		t.Fatalf("restore must report restored=false: stdout=%q stderr=%q", out, errOut)
	}
	if strings.Contains(out, "restored=true") {
		t.Fatalf("restore printed a success beyond its evidence: %q", out)
	}
	if got := f.toolCalls(t); got != calls {
		t.Fatalf("a refused restore still invoked the tool %d time(s)", got-calls)
	}
	if got := f.userTableCount(t, restoreTarget); got != 0 {
		t.Fatalf("refused restore wrote %d tables into the target", got)
	}
	if got := f.evidenceCount(t, "restore_probe", backupID); got != 0 {
		t.Fatalf("refused restore wrote restore_probe evidence: %d rows", got)
	}
	if got := f.markerCount(t, backupID); got != 0 {
		t.Fatalf("a restore that never started wrote %d pre-write markers", got)
	}
}
