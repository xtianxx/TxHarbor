//go:build integration

// backup_restore_integration_test.go runs the real `recovery-admin
// backup|verify-backup|restore` CLI (in-process Run with the deployment env)
// against a real PostgreSQL 18.6 fixture (testcontainers) and isolated target
// databases, with native pg_restore and the real pg_dump binary in the fixture.
//
// Tool path: pg_dump uses a test-only PATH counter shim that execs the resolved
// native PostgreSQL 18.6 client with the original host-mapped DSN unchanged.
// pg_restore must be native and direct: the production guard rejects PATH
// shell shims. Missing native tools report NOT RUN locally and fail in the
// required CI environment. The counter instruments actual pg_dump calls;
// positive restore assertions inspect actual restored data.
//
// Covered:
//
//   - S1/S2: real CLI backup (unverified manifest) and verify-backup (real
//     isolated pg_restore + four checks, verified conclusion + evidence row);
//   - operation_id semantics on the real CLI: same id + same input replays
//     with zero recorded pg_dump shim calls, zero new evidence and an untouched target; same id
//     with a changed target conflicts with zero writes anywhere; an omitted id
//     is a real rerun (non-replay) that appends a new evidence row through the
//     generation protocol;
//   - interruption re-entry: an interrupted real restore never says restored,
//     writes no restored evidence, keeps the accepted pre-write invalidation
//     marker (old releases/approvals stay stale) and does not reuse failed
//     evidence: the same id replays the recorded refusal, a fresh id reruns
//     for real after the target is rebuilt;
//   - the G2 negative CLI assertion: a missing authoritative object is actually
//     absent from the restored target, verification leaves its guard dirty and
//     writes no accepted evidence, and restore is refused without overclaiming.
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
	"os/exec"
	"path/filepath"
	"slices"
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

// cliPGShim counts an invocation and execs the fixture's resolved native
// pg_dump. It deliberately reads no ambient test-only variables: production's
// PostgreSQL child environment is allowlisted and strips them.
const cliPGShim = `#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' 'pg_dump' >> __CALLS_FILE__
exec __PG_DUMP__ "$@"
`

type cliFixture struct {
	t          *testing.T
	ctx        context.Context
	baseDSN    string
	admin      *pgxpool.Pool
	dataDSN    string
	restoreDSN string
	data       *pgxpool.Pool
	controlDSN string
	control    *pgxpool.Pool
	store      *controlstore.Store
	instanceID string
	artDir     string
	callsFile  string
	seq        int
}

// requireNativePGRestore checks the host PATH before the fixture adds any
// test-only PATH entries. A real restore executable is required for positive
// restore evidence; successful stubs are not acceptable substitutes.
func requireNativePGRestore(t *testing.T) {
	t.Helper()
	path, err := exec.LookPath("pg_restore")
	if err != nil {
		requireNativeFixtureTool(t, "native direct PostgreSQL 18.6 pg_restore unavailable")
		return
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		requireNativeFixtureTool(t, fmt.Sprintf("native direct PostgreSQL 18.6 pg_restore unavailable: resolve executable: %v", err))
		return
	}
	binary, err := os.Open(resolved)
	if err != nil {
		requireNativeFixtureTool(t, fmt.Sprintf("native direct PostgreSQL 18.6 pg_restore unavailable: open executable: %v", err))
		return
	}
	magic := make([]byte, 4)
	_, readErr := io.ReadFull(binary, magic)
	closeErr := binary.Close()
	if readErr != nil || closeErr != nil || string(magic) != "\x7fELF" {
		requireNativeFixtureTool(t, "native direct PostgreSQL 18.6 pg_restore unavailable: executable is not a native ELF binary")
		return
	}
	version, err := exec.Command(path, "--version").CombinedOutput()
	if err != nil {
		requireNativeFixtureTool(t, fmt.Sprintf("native direct PostgreSQL 18.6 pg_restore unavailable: version command failed: %v", err))
		return
	}
	// Accept packaging suffixes, but require an exact PostgreSQL 18.6 version.
	versionText := strings.TrimSpace(string(version))
	versionFields := strings.Fields(versionText)
	if len(versionFields) < 3 || versionFields[0] != "pg_restore" ||
		versionFields[1] != "(PostgreSQL)" || versionFields[2] != "18.6" {
		requireNativeFixtureTool(t, fmt.Sprintf("native direct PostgreSQL 18.6 pg_restore unavailable: version=%q", versionText))
	}
}

// nativePGDumpPath resolves and validates the real 18.6 client before the
// fixture prepends its counter shim to PATH. The fixture server and dump client
// must match; accepting another major/minor can produce misleading format or
// compatibility failures.
func nativePGDumpPath(t *testing.T) string {
	t.Helper()
	path, err := exec.LookPath("pg_dump")
	if err != nil {
		requireNativeFixtureTool(t, "native PostgreSQL 18.6 pg_dump unavailable: executable not found")
		return ""
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		requireNativeFixtureTool(t, fmt.Sprintf("native PostgreSQL 18.6 pg_dump unavailable: resolve executable: %v", err))
		return ""
	}
	binary, err := os.Open(resolved)
	if err != nil {
		requireNativeFixtureTool(t, fmt.Sprintf("native PostgreSQL 18.6 pg_dump unavailable: open executable: %v", err))
		return ""
	}
	magic := make([]byte, 4)
	_, readErr := io.ReadFull(binary, magic)
	closeErr := binary.Close()
	if readErr != nil || closeErr != nil || string(magic) != "\x7fELF" {
		requireNativeFixtureTool(t, "native PostgreSQL 18.6 pg_dump unavailable: executable is not a native ELF binary")
		return ""
	}
	version, err := exec.Command(resolved, "--version").CombinedOutput()
	versionText := strings.TrimSpace(string(version))
	versionFields := strings.Fields(versionText)
	if err != nil || len(versionFields) < 3 || versionFields[0] != "pg_dump" ||
		versionFields[1] != "(PostgreSQL)" || versionFields[2] != "18.6" {
		requireNativeFixtureTool(t, fmt.Sprintf("native PostgreSQL 18.6 pg_dump unavailable: version=%q", versionText))
		return ""
	}
	return resolved
}

func requireNativeFixtureTool(t *testing.T, reason string) {
	t.Helper()
	if strings.EqualFold(os.Getenv("CI"), "true") || os.Getenv("TXHARBOR_REQUIRE_DOCKER") == "1" {
		t.Fatalf("NOT RUN: %s (required integration environment)", reason)
	}
	t.Skip("NOT RUN: " + reason)
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
}

func newCLIFixture(t *testing.T) *cliFixture {
	t.Helper()
	pgDumpPath := nativePGDumpPath(t)
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
		t: t, ctx: ctx, baseDSN: baseDSN,
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
	// The recovery instance is bound to one dedicated restore target. Backup
	// source data remains separate; restore authority must not be retargeted to
	// arbitrary per-test databases.
	f.restoreDSN = f.createDB(t, "restore_target")
	targetBinding := cliTargetBinding(t, f.restoreDSN)
	opened, err := store.OpenInstance(ctx, controlstore.OpenInstanceRequest{
		Kind: "recovery", OpenedBy: "deploy:executor", EntryChainInventory: []uint64{31337},
		TargetGuardKey:        targetBinding.TargetGuardKey,
		TargetRoleFingerprint: targetBinding.TargetRoleFingerprint,
	})
	if err != nil {
		t.Fatalf("open recovery instance: %v", err)
	}
	f.instanceID = opened.InstanceID
	cliResolveFixtureTargetGuard(t, f, targetBinding.TargetGuardKey)
	f.mapIdentity(t, "deploy:executor", "person-executor")
	f.mapIdentity(t, "auth:verifier", "person-verifier")
	f.register(t, "deploy:executor", "executor")
	f.register(t, "auth:verifier", "verifier")

	// PATH shim for pg_dump only. It counts the actual native client invocation
	// and then execs that binary unchanged; pg_restore continues resolving
	// directly to the native executable checked by real-restore tests.
	binDir := t.TempDir()
	shim := strings.ReplaceAll(cliPGShim, "__CALLS_FILE__", shellQuote(f.callsFile))
	shim = strings.ReplaceAll(shim, "__PG_DUMP__", shellQuote(pgDumpPath))
	if err := os.WriteFile(filepath.Join(binDir, "pg_dump"), []byte(shim), 0o755); err != nil {
		t.Fatalf("write pg_dump shim: %v", err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return f
}

// cliResolveFixtureTargetGuard models the explicit deployment-controlled
// clean baseline required before the fixture can exercise release behavior.
// It records the rebuild evidence and state transition atomically; production
// code remains fail-closed when no such evidence exists.
func cliResolveFixtureTargetGuard(t *testing.T, f *cliFixture, key string) {
	t.Helper()
	ctx := f.ctx
	tx, err := f.control.Begin(ctx)
	if err != nil {
		t.Fatalf("begin fixture target-guard resolution: %v", err)
	}
	defer tx.Rollback(ctx)
	guard, found, err := controlstore.ReadTargetGuard(ctx, tx, key)
	if err != nil {
		t.Fatalf("read fixture target guard: %v", err)
	}
	if found && guard.State == controlstore.TargetGuardClean && !guard.ActiveWriter {
		return
	}
	operationID := "cli-fixture-clean-" + key[:16]
	if !found {
		if err := controlstore.InitializeTargetGuard(ctx, tx, key, operationID); err != nil {
			t.Fatalf("initialize fixture target guard: %v", err)
		}
	}
	evidence := []byte(`{"fixture":"isolated PostgreSQL instance; no target writer was launched","disposition":"clean"}`)
	if err := controlstore.RecordTargetGuardRebuild(ctx, tx, f.instanceID, key,
		"deploy:test-fixture", operationID, evidence); err != nil {
		t.Fatalf("record fixture target-guard evidence: %v", err)
	}
	if err := controlstore.ResolveTargetGuardClean(ctx, tx, key, operationID, evidence); err != nil {
		guard, found, readErr := controlstore.ReadTargetGuard(ctx, tx, key)
		t.Fatalf("resolve fixture target guard clean: %v (found=%t guard=%+v read_err=%v)", err, found, guard, readErr)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit fixture target-guard resolution: %v", err)
	}
}

// cliTargetBinding derives the same credential-free endpoint and role
// identities used by the production instance-open path.
func cliTargetBinding(t *testing.T, dsn string) recovery.IsolatedInstanceBinding {
	t.Helper()
	target, err := controlstore.ParseDSNTarget(dsn)
	if err != nil {
		t.Fatalf("parse data target: %v", err)
	}
	key, err := controlstore.TargetGuardKey(target)
	if err != nil {
		t.Fatalf("derive target guard key: %v", err)
	}
	return recovery.IsolatedInstanceBinding{
		TargetGuardKey:        key,
		TargetRoleFingerprint: target.DataTargetFingerprint().RoleFingerprint,
	}
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

// rebuildInterruptedTarget proves the failed attempt is inactive, then
// rebuilds the exact bound target without FORCE while holding its shared
// target lock. The clean transition and rebuild audit are committed on the
// lock-owning session, not seeded through the generic fixture baseline.
func (f *cliFixture) rebuildInterruptedTarget(t *testing.T, dsn, interruptedOperation string) string {
	t.Helper()
	name := cliDBNameOf(t, dsn)
	target, err := controlstore.ParseDSNTarget(dsn)
	if err != nil {
		t.Fatalf("parse interrupted restore target: %v", err)
	}
	guardKey, err := controlstore.TargetGuardKey(target)
	if err != nil {
		t.Fatalf("derive interrupted restore target guard: %v", err)
	}
	lockKey, err := recovery.CanonicalTargetKey(target)
	if err != nil {
		t.Fatalf("derive interrupted restore target lock: %v", err)
	}
	lock, err := recovery.AcquireTargetLock(f.ctx, f.controlDSN, lockKey, 10*time.Second, 20*time.Millisecond)
	if err != nil {
		t.Fatalf("acquire shared interrupted-target lock: %v", err)
	}
	defer func() {
		if err := lock.Release(context.Background()); err != nil {
			t.Errorf("release interrupted-target lock: %v", err)
		}
	}()
	if err := lock.Health(f.ctx); err != nil {
		t.Fatalf("verify shared interrupted-target lock: %v", err)
	}

	var prior controlstore.TargetGuard
	var priorFound bool
	if err := lock.WithTransaction(f.ctx, func(ctx context.Context, tx pgx.Tx) error {
		var err error
		prior, priorFound, err = controlstore.ReadTargetGuard(ctx, tx, guardKey)
		return err
	}); err != nil {
		t.Fatalf("read prior interrupted-target guard on lock owner: %v", err)
	}
	if !priorFound || prior.State != controlstore.TargetGuardRebuildRequired || prior.ActiveWriter ||
		prior.OperationID != interruptedOperation {
		t.Fatalf("refuse rebuild without the exact inactive interrupted attempt: found=%t guard=%+v want_operation=%q",
			priorFound, prior, interruptedOperation)
	}
	f.waitForNoTargetSessions(t, name, 30*time.Second)
	if _, err := f.admin.Exec(f.ctx, "DROP DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatalf("drop %s: %v", name, err)
	}
	if _, err := f.admin.Exec(f.ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatalf("recreate %s: %v", name, err)
	}
	if tables := f.userTableCount(t, dsn); tables != 0 {
		t.Fatalf("rebuilt target is not empty: found %d user tables", tables)
	}
	if rows := f.probeRowCount(t, dsn); rows != 0 {
		t.Fatalf("rebuilt target contains %d probe rows", rows)
	}

	const rebuildOperation = "cli-interrupted-target-rebuild"
	evidence := []byte(fmt.Sprintf(
		`{"fixture":"controlled rebuild after owned CLI cancellation","target_database":%q,"prior_operation_id":%q,"prior_guard_state":%q,"prior_active_writer":false,"target_sessions_empty":true,"database_dropped_without_force":true,"database_recreated_empty":true,"process_supervisor_returned":true}`,
		name, prior.OperationID, prior.State))
	if err := lock.WithTransaction(f.ctx, func(ctx context.Context, tx pgx.Tx) error {
		current, found, err := controlstore.ReadTargetGuard(ctx, tx, guardKey)
		if err != nil {
			return err
		}
		if !found || current.State != controlstore.TargetGuardRebuildRequired || current.ActiveWriter ||
			current.OperationID != interruptedOperation {
			return fmt.Errorf("target guard changed during controlled rebuild: found=%t guard=%+v", found, current)
		}
		if err := controlstore.RecordTargetGuardRebuild(ctx, tx, f.instanceID, guardKey,
			"deploy:test-fixture", rebuildOperation, evidence); err != nil {
			return fmt.Errorf("record observed target rebuild: %w", err)
		}
		if err := controlstore.ResolveTargetGuardClean(ctx, tx, guardKey, rebuildOperation, evidence); err != nil {
			return fmt.Errorf("resolve observed target guard clean: %w", err)
		}
		return nil
	}); err != nil {
		t.Fatalf("commit lock-owner interrupted-target rebuild audit: %v", err)
	}
	return dsn
}

func (f *cliFixture) waitForNoTargetSessions(t *testing.T, dbName string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		var sessions int
		if err := f.admin.QueryRow(f.ctx,
			`SELECT count(*) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()`, dbName).Scan(&sessions); err != nil {
			t.Fatalf("count sessions in target %s: %v", dbName, err)
		}
		if sessions == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("target %s still has %d session(s) after %s", dbName, sessions, timeout)
		}
		time.Sleep(20 * time.Millisecond)
	}
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
		config.EnvRecoveryControlDSN:         f.controlDSN,
		config.EnvRecoveryObserverDSN:        f.baseDSN,
		config.EnvPGDSN:                      f.dataDSN,
		config.EnvRecoveryPrincipal:          principal,
		config.EnvRecoveryArtifactDir:        f.artDir,
		config.EnvRecoveryEntryChains:        "31337",
		config.EnvRecoveryDeploymentAdminDSN: f.baseDSN,
	}
}

// toolCalls counts pg_dump invocations recorded by the test-only PATH shim.
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

func (f *cliFixture) relationExists(t *testing.T, dsn, relation string) bool {
	t.Helper()
	conn, err := pgx.Connect(f.ctx, dsn)
	if err != nil {
		t.Fatalf("connect %s: %v", cliDBNameOf(t, dsn), err)
	}
	defer func() { _ = conn.Close(f.ctx) }()
	var exists bool
	if err := conn.QueryRow(f.ctx, `SELECT to_regclass($1) IS NOT NULL`, "public."+relation).Scan(&exists); err != nil {
		t.Fatalf("look up relation %s: %v", relation, err)
	}
	return exists
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
	// No operator assertion is needed: the CLI must use its opaque deployment
	// binding even when the isolated database differs from the authoritative DB.
	return f.cliVerifyWithConfiguredTarget(t, manifestPath, "", targetDSN, operationID)
}

func (f *cliFixture) cliVerifyWithConfiguredTarget(t *testing.T, manifestPath, assertedDSN, configuredDSN, operationID string) (int, string, string) {
	t.Helper()
	// Verification restores into its isolated target before inspecting it. Seed
	// the target's independently controlled clean baseline through the real
	// target-guard paths; the authoritative instance guard alone is not evidence
	// that this separate database is clean.
	target, err := controlstore.ParseDSNTarget(configuredDSN)
	if err != nil {
		t.Fatalf("parse configured verification target: %v", err)
	}
	targetKey, err := controlstore.TargetGuardKey(target)
	if err != nil {
		t.Fatalf("derive configured verification target guard: %v", err)
	}
	cliResolveFixtureTargetGuard(t, f, targetKey)
	args := []string{"verify-backup", "--manifest", manifestPath, "--instance", f.instanceID}
	if strings.TrimSpace(assertedDSN) != "" {
		args = append(args, "--target-dsn", assertedDSN)
	}
	if operationID != "" {
		args = append(args, "--operation-id", operationID)
	}
	env := f.env("auth:verifier")
	env[config.EnvPGDSN] = f.restoreDSN
	env[config.EnvRecoveryIsolatedTargetDSN] = configuredDSN
	// The fixture's PostgreSQL owner is privileged and observes the same target
	// endpoint, independently of the opaque restore binding.
	env[config.EnvRecoveryObserverDSN] = configuredDSN
	return runRecoveryAdmin(t, args, env)
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
	env := f.env("deploy:executor")
	env[config.EnvPGDSN] = targetDSN
	env[config.EnvRecoveryObserverDSN] = targetDSN
	args := append(cliRestoreArgs(manifestPath, targetDSN, f.instanceID, operationID),
		"--declaration", "production_main", "--reason", "controlled isolated PostgreSQL recovery fixture")
	return runRecoveryAdmin(t, args, env)
}

func (f *cliFixture) cliRestoreContext(ctx context.Context, manifestPath, targetDSN, operationID string) (int, string, string) {
	env := f.env("deploy:executor")
	env[config.EnvPGDSN] = targetDSN
	env[config.EnvRecoveryObserverDSN] = targetDSN
	args := append(cliRestoreArgs(manifestPath, targetDSN, f.instanceID, operationID),
		"--declaration", "production_main", "--reason", "controlled isolated PostgreSQL recovery fixture")
	var stdout, stderr strings.Builder
	code := Run(ctx, args, Deps{
		Getenv: migrateTestEnv(env),
		Stdout: &stdout,
		Stderr: &stderr,
	})
	return code, stdout.String(), stderr.String()
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
	requireNativePGRestore(t)
	f := newCLIFixture(t)
	manifestPath := f.cliBackup(t, "op-bkp-1")
	backupID := f.manifestBackupID(t, manifestPath)

	target := f.createDB(t, "verify")
	code, out, errOut := f.cliVerify(t, manifestPath, target, "op-ver-1")
	if code != 0 || !strings.Contains(out, "verification=verified") {
		t.Fatalf("verify-backup: exit=%d stdout=%q stderr=%q", code, out, errOut)
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
		t.Fatalf("a replayed verify-backup invoked the pg_dump shim %d more time(s)", got-calls)
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
		t.Fatalf("a conflicting operation invoked the pg_dump shim %d time(s)", got-calls)
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
	code, out, errOut = f.cliVerify(t, manifestPath, rerun, "")
	if code != 0 || !strings.Contains(out, "verification=verified") {
		t.Fatalf("verify-backup rerun: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}
	if strings.Contains(out, "replayed=true") {
		t.Fatalf("an omitted operation id must be a real rerun, not a replay: %q", out)
	}
	if got := f.evidenceCount(t, "backup_manifest", backupID); got != 2 {
		t.Fatalf("backup_manifest evidence rows after the rerun = %d, want 2 (append)", got)
	}
	if rows, generations := f.evidenceGenerations(t, "backup_manifest", backupID); rows != 2 || generations != 2 {
		t.Fatalf("appended backup_manifest rows = %d with %d distinct generations, want 2/2", rows, generations)
	}
}

// A caller-provided target DSN is only an assertion. A mismatch against the
// deployment-configured endpoint must refuse before invoking pg_restore or
// writing verification evidence.
func TestVerifyBackupCLIRejectsTargetAssertionMismatch(t *testing.T) {
	f := newCLIFixture(t)
	manifestPath := f.cliBackup(t, "op-bkp-target-assertion")
	backupID := f.manifestBackupID(t, manifestPath)
	configured := f.createDB(t, "trusted_verify")
	asserted := f.createDB(t, "untrusted_verify")
	callsBefore := f.toolCalls(t)
	code, stdout, stderr := f.cliVerifyWithConfiguredTarget(t, manifestPath, asserted, configured, "")
	if code != 1 || !strings.Contains(stderr, "assertion refused") {
		t.Fatalf("mismatched target assertion must refuse: exit=%d stdout=%q stderr=%q", code, stdout, stderr)
	}
	if got := f.toolCalls(t); got != callsBefore {
		t.Fatalf("mismatched target assertion invoked PostgreSQL tools: calls %d -> %d", callsBefore, got)
	}
	if got := f.evidenceCount(t, "backup_manifest", backupID); got != 0 {
		t.Fatalf("mismatched target assertion wrote verification evidence: %d rows", got)
	}
	for _, output := range []string{stdout, stderr} {
		if strings.Contains(output, "secret") || strings.Contains(output, "postgres://") {
			t.Fatalf("target assertion refusal leaked DSN material: stdout=%q stderr=%q", stdout, stderr)
		}
	}
}

// TestRestoreCLIOperationIDReplayRerunAndInterruption covers G3 on the real
// CLI restore path: successful replay does not touch the target, conflicts
// write nothing, an omitted id is a real rerun with appended evidence, and an
// interrupted attempt is never reused as success.
func TestRestoreCLIOperationIDReplayRerunAndInterruption(t *testing.T) {
	requireNativePGRestore(t)
	f := newCLIFixture(t)
	manifestPath, backupID := f.verifiedBackup(t)

	// Positive real restore through the CLI.
	target := f.restoreDSN
	calls := f.toolCalls(t)
	code, out, errOut := f.cliRestore(t, manifestPath, target, "op-rst-1")
	if code != 0 || !strings.Contains(out, "restored=true") {
		t.Fatalf("restore: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}
	if got := f.evidenceCount(t, "restore_probe", backupID); got != 1 {
		t.Fatalf("restore_probe evidence rows = %d, want 1", got)
	}
	tables := f.userTableCount(t, target)
	rows := f.probeRowCount(t, target)
	if tables == 0 || rows == 0 {
		t.Fatalf("the restore target was not actually restored (tables=%d probe rows=%d)", tables, rows)
	}

	// Successful replay (scenario 1): recorded outcome, zero pg_dump shim calls, zero
	// new evidence, target untouched.
	calls = f.toolCalls(t)
	code, out, errOut = f.cliRestore(t, manifestPath, target, "op-rst-1")
	if code != 0 || !strings.Contains(out, "replayed=true") {
		t.Fatalf("restore replay: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}
	if got := f.toolCalls(t); got != calls {
		t.Fatalf("a replayed restore invoked the pg_dump shim %d more time(s)", got-calls)
	}
	if got := f.evidenceCount(t, "restore_probe", backupID); got != 1 {
		t.Fatalf("a replayed restore wrote evidence: %d rows", got)
	}
	if got := f.userTableCount(t, target); got != tables || f.probeRowCount(t, target) != rows {
		t.Fatalf("a replayed restore changed the target (tables %d->%d, probe rows %d->%d)",
			tables, got, rows, f.probeRowCount(t, target))
	}

	// A different database is outside the immutable instance binding and is
	// refused before a restore can write anywhere.
	conflict := f.createDB(t, "restore_conflict")
	code, _, errOut = f.cliRestore(t, manifestPath, conflict, "op-rst-1")
	if code != 1 || !strings.Contains(errOut, "operation_conflict") {
		t.Fatalf("restore operation_id conflict must refuse: exit=%d stderr=%q", code, errOut)
	}
	if got := f.toolCalls(t); got != calls {
		t.Fatalf("a conflicting restore invoked the pg_dump shim %d time(s)", got-calls)
	}
	if got := f.evidenceCount(t, "restore_probe", backupID); got != 1 {
		t.Fatalf("a conflicting restore wrote evidence: %d rows", got)
	}
	if got := f.userTableCount(t, conflict); got != 0 {
		t.Fatalf("a refused restore wrote %d tables into its target", got)
	}

	// Omitted operation id: explicit real rerun, evidence appended.
	rerun := f.restoreDSN
	calls = f.toolCalls(t)
	code, out, errOut = f.cliRestore(t, manifestPath, rerun, "")
	if code != 0 || !strings.Contains(out, "restored=true") {
		t.Fatalf("restore rerun: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}
	if strings.Contains(out, "replayed=true") {
		t.Fatalf("an omitted operation id must be a real rerun, not a replay: %q", out)
	}
	if got := f.evidenceCount(t, "restore_probe", backupID); got != 2 {
		t.Fatalf("restore_probe evidence rows after the rerun = %d, want 2 (append)", got)
	}
	if rows, generations := f.evidenceGenerations(t, "restore_probe", backupID); rows != 2 || generations != 2 {
		t.Fatalf("appended restore_probe rows = %d with %d distinct generations, want 2/2 (each acceptance records its own generation)",
			rows, generations)
	}

	// Interruption: park the real pg_restore on its first write, cancel the
	// owning CLI command, and verify its supervisor drains the
	// process group before the target can be rebuilt.
	interrupted := f.restoreDSN
	locker, err := pgx.Connect(f.ctx, interrupted)
	if err != nil {
		t.Fatalf("connect interruption target: %v", err)
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
	cliCtx, cancelCLI := context.WithCancel(f.ctx)
	defer cancelCLI()
	go func() {
		code, stdout, stderr := f.cliRestoreContext(cliCtx, manifestPath, interrupted, "op-rst-int")
		done <- cliResult{code: code, stdout: stdout, stderr: stderr}
	}()
	_ = f.waitForLockWaiter(t, cliDBNameOf(t, interrupted), 30*time.Second)
	cancelCLI()
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
	// The cancelled command returned only after the PostgreSQL process-group
	// supervisor drained its child. Independently prove there are no remaining
	// sessions anywhere in the immutable target before rebuilding it.
	f.waitForNoTargetSessions(t, cliDBNameOf(t, interrupted), 30*time.Second)

	// Cancellation leaves the target guard dirty before the CLI can persist its
	// operation receipt. The first post-interruption retry therefore records a
	// refusal against that exact guard; it is never allowed to reuse the failed
	// attempt as success.
	calls = f.toolCalls(t)
	code, out, errOut = f.cliRestore(t, manifestPath, interrupted, "op-rst-int")
	if code != 1 || !strings.Contains(errOut, "restored=false") || strings.Contains(out, "restored=true") {
		t.Fatalf("retry of interrupted operation must refuse, not reuse it as success: exit=%d stdout=%q stderr=%q",
			code, out, errOut)
	}
	if got := f.toolCalls(t); got != calls {
		t.Fatalf("retrying the failed operation invoked the pg_dump shim %d time(s)", got-calls)
	}
	if got := f.evidenceCount(t, "restore_probe", backupID); got != 2 {
		t.Fatalf("retrying the failed operation wrote evidence: %d rows", got)
	}
	// Once that refusal is durably recorded, replaying the same failed operation
	// id returns the refusal with no tool calls or evidence writes.
	calls = f.toolCalls(t)
	code, out, errOut = f.cliRestore(t, manifestPath, interrupted, "op-rst-int")
	if code != 1 || !strings.Contains(errOut, "replayed=true") || strings.Contains(out, "restored=true") {
		t.Fatalf("recorded failed operation must replay its refusal: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}
	if got := f.toolCalls(t); got != calls {
		t.Fatalf("replaying the failed operation invoked the pg_dump shim %d time(s)", got-calls)
	}
	if got := f.evidenceCount(t, "restore_probe", backupID); got != 2 {
		t.Fatalf("replaying the failed operation wrote evidence: %d rows", got)
	}
	// A different operation id is an ordinary retry, not a replay. It must
	// still refuse while the interrupted target is dirty; only the controlled
	// same-target recreation below can make a subsequent attempt eligible.
	calls = f.toolCalls(t)
	code, out, errOut = f.cliRestore(t, manifestPath, interrupted, "op-rst-int-retry-too-soon")
	if code != 1 || !strings.Contains(errOut, "restored=false") || strings.Contains(out, "restored=true") {
		t.Fatalf("ordinary retry before controlled rebuild must refuse: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}
	if got := f.toolCalls(t); got != calls {
		t.Fatalf("ordinary retry against the dirty target invoked the pg_dump shim %d time(s)", got-calls)
	}
	if got := f.evidenceCount(t, "restore_probe", backupID); got != 2 {
		t.Fatalf("ordinary retry against the dirty target wrote restore evidence: %d rows", got)
	}
	interruptedBinding := cliTargetBinding(t, interrupted)
	guard, found, guardErr := controlstore.ReadTargetGuard(f.ctx, f.control, interruptedBinding.TargetGuardKey)
	if guardErr != nil || !found || guard.State != controlstore.TargetGuardRebuildRequired || guard.ActiveWriter ||
		guard.OperationID != "op-rst-int" {
		t.Fatalf("ordinary retry must preserve the exact inactive interrupted guard: found=%t guard=%+v err=%v", found, guard, guardErr)
	}

	// Retry only after a controlled rebuild of the same immutable target. Hold
	// the shared target lock across the non-FORCE drop/create and the transaction
	// that records the observed rebuild and resolves its guard.
	rebuilt := f.rebuildInterruptedTarget(t, interrupted, "op-rst-int")
	calls = f.toolCalls(t)
	code, out, errOut = f.cliRestore(t, manifestPath, rebuilt, "op-rst-int-2")
	if code != 0 || !strings.Contains(out, "restored=true") {
		t.Fatalf("retry after rebuild: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}
	if got := f.evidenceCount(t, "restore_probe", backupID); got != 3 {
		t.Fatalf("restore_probe evidence rows after the retry = %d, want 3", got)
	}
	if rows, generations := f.evidenceGenerations(t, "restore_probe", backupID); rows != 3 || generations != 3 {
		t.Fatalf("restore_probe rows = %d with %d distinct generations after the retry, want 3/3", rows, generations)
	}
}

// TestVerifyBackupCLINoOverEvidenceSuccessOnMissingObject is the CLI half of
// G2: with an authoritative object missing, verify-backup fails before
// acceptance, records no success evidence and leaves its target dirty; restore
// refuses, while the restored target and manifest retain causal failure proof.
func TestVerifyBackupCLINoOverEvidenceSuccessOnMissingObject(t *testing.T) {
	requireNativePGRestore(t)
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
	if strings.Contains(out+errOut, "verification=verified") || strings.Contains(out+errOut, "verified=true") ||
		strings.Contains(out+errOut, "backup_manifest") {
		t.Fatalf("verify-backup overclaimed acceptance before its probes passed: stdout=%q stderr=%q", out, errOut)
	}
	if !strings.Contains(errOut, "required table consumer_inbox is missing") {
		t.Fatalf("verify-backup failed for an unexpected reason instead of the failed isolated probe: stdout=%q stderr=%q", out, errOut)
	}
	if f.relationExists(t, target, "consumer_inbox") || !f.relationExists(t, target, "consumer_inbox_015_damaged") {
		t.Fatalf("verification target does not prove the causal missing consumer_inbox object (missing=%t damaged_copy=%t)",
			!f.relationExists(t, target, "consumer_inbox"), f.relationExists(t, target, "consumer_inbox_015_damaged"))
	}
	if !f.relationExists(t, target, "consumer_progress") {
		t.Fatal("verification target lost the surviving consumer_idempotency_progress category object")
	}
	manifestBytes, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read rejected manifest after CLI probe failure: %v", err)
	}
	manifest, err := recovery.ParseManifest(manifestBytes)
	if err != nil {
		t.Fatalf("parse manifest after CLI probe failure: %v", err)
	}
	if manifest.Verification.State != recovery.VerificationUnverified {
		t.Fatalf("failed missing-object probe must not write an accepted verification conclusion, got %q", manifest.Verification.State)
	}
	if manifest.Verification.Checks.AllTrue() || manifest.Verification.Checks.StructureConstraints ||
		manifest.Verification.Checks.BusinessStateProbes || manifest.Verification.Checks.VerificationExecutable {
		t.Fatalf("missing consumer_inbox in category consumer_idempotency_progress must not carry successful probe checks: %+v",
			manifest.Verification.Checks)
	}
	if !slices.Contains(manifest.Coverage.Authoritative, "consumer_inbox") {
		t.Fatalf("the failed consumer_inbox object is not declared authoritative in the rejected manifest: %v", manifest.Coverage.Authoritative)
	}
	if got := f.evidenceCount(t, "backup_manifest", backupID); got != 0 {
		t.Fatalf("rejected verify-backup wrote backup_manifest evidence: %d rows", got)
	}
	configuredTarget, err := controlstore.ParseDSNTarget(target)
	if err != nil {
		t.Fatalf("parse failed verification target: %v", err)
	}
	guardKey, err := controlstore.TargetGuardKey(configuredTarget)
	if err != nil {
		t.Fatalf("derive failed verification target guard: %v", err)
	}
	guard, found, err := controlstore.ReadTargetGuard(f.ctx, f.control, guardKey)
	if err != nil || !found || guard.State != controlstore.TargetGuardRebuildRequired || guard.ActiveWriter {
		t.Fatalf("failed missing-object verification must leave its target dirty and inactive: found=%t guard=%+v err=%v", found, guard, err)
	}

	restoreTarget := f.restoreDSN
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
		t.Fatalf("a refused restore still invoked the pg_dump shim %d time(s)", got-calls)
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

// TestBackupVerifyRestoreCLIRedactCredentialShapedFreeText checks the paths
// that cross from free-text CLI inputs/results into operator output and the
// persistent operation audit. Redaction is applied before the audit write;
// output-only redaction would leave the canary in recovery_audit.
func TestBackupVerifyRestoreCLIRedactCredentialShapedFreeText(t *testing.T) {
	f := newCLIFixture(t)
	const canary = "t068-fictional-path-canary"
	secretPath := filepath.Join(f.artDir, "password="+canary, "manifest.json")
	secretDir := filepath.Dir(secretPath)

	code, out, errOut := runRecoveryAdmin(t,
		[]string{"backup", "--chain-id", "1", "--out", secretDir, "--operation-id", "t068-backup"},
		f.env("deploy:executor"))
	if code != 0 {
		t.Fatalf("backup: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}
	if strings.Contains(out+errOut, canary) {
		t.Fatalf("backup output leaked credential-shaped artifact path: stdout=%q stderr=%q", out, errOut)
	}

	missingManifest := filepath.Join(f.artDir, "token="+canary, "missing.json")
	target := f.createDB(t, "t068_verify")
	code, out, errOut = f.cliVerify(t, missingManifest, target, "t068-verify")
	if code != 1 {
		t.Fatalf("verify-backup missing manifest: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}
	if strings.Contains(out+errOut, canary) {
		t.Fatalf("verify-backup refusal leaked credential-shaped input: stdout=%q stderr=%q", out, errOut)
	}

	restoreTarget := f.createDB(t, "t068_restore")
	restorePath := filepath.Join(f.artDir, "authorization="+canary, "missing.json")
	args := []string{"restore", "--manifest", restorePath, "--target-dsn", restoreTarget,
		"--instance", f.instanceID, "--declaration", "production_main",
		"--reason", "password=" + canary, "--operation-id", "t068-restore"}
	code, out, errOut = runRecoveryAdmin(t, args, f.env("deploy:executor"))
	if code != 1 {
		t.Fatalf("restore credential-shaped reason: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}
	if strings.Contains(out+errOut, canary) {
		t.Fatalf("restore refusal leaked credential-shaped input: stdout=%q stderr=%q", out, errOut)
	}

	var auditText string
	if err := f.control.QueryRow(f.ctx, `
SELECT COALESCE(string_agg(to_jsonb(a)::text, ' '), '')
FROM recovery_audit AS a
WHERE operation_id IN ('t068-backup', 't068-verify', 't068-restore')`).Scan(&auditText); err != nil {
		t.Fatalf("read T068 operation audit: %v", err)
	}
	if strings.Contains(auditText, canary) {
		t.Fatalf("credential-shaped free text persisted in refusal/success audit: %s", auditText)
	}
}
