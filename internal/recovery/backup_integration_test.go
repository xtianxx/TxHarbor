//go:build integration

// backup_integration_test.go is T015: the real backup/restore PostgreSQL
// integration layer (tags: integration; pinned postgres:18.6-trixie
// testcontainers; FR-003/FR-004/FR-005/FR-006/FR-008; quickstart S1/S2 + F1/F2/F3).
//
// Docker discipline: the package TestMain (generation_integration_test.go)
// reports NOT RUN (exit 0) locally when no Docker provider is healthy and
// fails the package under CI=true or TXHARBOR_REQUIRE_DOCKER=1. An unrun PG
// layer is never a pass.
//
// Real tools only: every dump/restore below runs the actual pg_dump/pg_restore
// binaries from the pinned image through the PGCommand injected in the
// request. The host carries no PostgreSQL client, so the test implements
// PGCommand by executing the real binaries inside the PostgreSQL container
// (`docker exec -i`). Archives stream over stdout/stdin (pg_dump writes the
// custom format to stdout; pg_restore reads the archive from stdin), so no
// host path ever needs to be visible inside the container. There is no fake
// dumper, no in-process substitute and no hand-written archive anywhere in
// this file.
//
// Covered (mapped to tasks.md T015):
//
//  1. real small pg_dump --format=custom --snapshot -> isolated pg_restore:
//     the exported snapshot is independently imported with SET TRANSACTION
//     SNAPSHOT in a second session, its tuple must equal
//     manifest.recovery_point.snapshot; a row committed AFTER the export by a
//     concurrent writer must be absent from the dump (proving the dump was
//     taken on the exported snapshot, not a later one); artifacts[] bytes and
//     sha256 are bound to the real file;
//  2. verify-backup runs a real isolated pg_restore, writes the manifest-level
//     verification back and records control-store evidence (backup_manifest);
//     restore refuses without that evidence even when the manifest file
//     claims verified (manifest file flags are not proof);
//  3. F1: corrupt / truncated / partial / hash-mismatch / bytes-mismatch /
//     unverified -> refused; a refused restore leaves the target database with
//     zero user tables (0 "best effort" recoveries) and writes no
//     restore_probe evidence;
//  4. F2: a real pg_restore interrupted mid-flight (parked on a table lock and
//     terminated server-side) is never marked restored; after rebuilding the
//     target database the rerun is idempotent - exactly one probe row, the
//     exact goose set, no duplicated/mixed state;
//  5. F3: manifest schema/program mismatch is refused with zero silent
//     downgrade/rewrite (target untouched);
//  6. snapshot invalidation / export failure produce no success manifest and
//     leave the artifact directory untouched;
//  7. restore targets: the control-store DSN can never be a restore target,
//     production_main requires an explicit recorded reason, and the default
//     declaration is isolated-only.
//
// TDD-first: this file references the B6/T018-T020 API - ExecuteBackup /
// BackupOptions / PGCommand / SnapshotExport (internal/recovery/backup.go),
// ExecuteRestore / RestoreOptions / TargetDeclaration
// (internal/recovery/restore.go; RestoreOptions carries ControlDSN so the
// control-store target guard is checkable) and ExecuteVerifyBackup /
// VerifyBackupOptions (internal/recovery/verifybackup.go) - none of which
// exists yet, so the integration candidate fails to build until B6 lands.
// Every helper is bkp-prefixed so it cannot collide with the existing
// gate/generation integration helpers in this package.
package recovery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore/schema"
)

const (
	// bkpPGImage is the pinned carrier image (ADR-002); the same image also
	// supplies the real pg_dump/pg_restore clients executed by the test's
	// PGCommand implementation.
	bkpPGImage      = "postgres:18.6-trixie"
	bkpBaseDB       = "txharbor"
	bkpBaseUser     = "txharbor"
	bkpBasePass     = "txharbor"
	bkpInternalPort = "5432"

	// bkpProgramVersion is the running program identity used by backup and
	// restore; the manifest floor is the same value, so the default fixture
	// manifest is compatible.
	bkpProgramVersion = "018.0"

	// bkpProbeTable is the small user table seeded before the backup. It is
	// deliberately absent from the repository migrations so its presence in a
	// restored target is unambiguous evidence of dump-derived data.
	bkpProbeTable = "snapshot_probe_015"

	bkpClockSkew = time.Hour
)

// ---------------------------------------------------------------------------
// Fixture: one PostgreSQL container, one data DB, one control DB, per-test
// isolated target DBs.
// ---------------------------------------------------------------------------

type bkpFixture struct {
	t     *testing.T
	ctx   context.Context
	ctr   *postgres.PostgresContainer
	ctrID string

	adminDSN string
	admin    *pgxpool.Pool

	srcDSN string
	src    *pgxpool.Pool

	ctrlDSN string
	ctrl    *pgxpool.Pool
	store   *controlstore.Store

	instanceID         string
	restoreTargetDSN   string
	restoreTargetGuard string
	restoreTargetRole  string
	artDir             string
	pg                 PGCommand

	seq int
}

type bkpTarget struct {
	name string
	dsn  string
}

func newBkpFixture(t *testing.T) *bkpFixture {
	t.Helper()
	ctx := context.Background()
	ctr, err := postgres.Run(ctx, bkpPGImage,
		postgres.WithDatabase(bkpBaseDB),
		postgres.WithUsername(bkpBaseUser),
		postgres.WithPassword(bkpBasePass),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2)),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })
	adminDSN, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres connection string: %v", err)
	}
	f := &bkpFixture{
		t:        t,
		ctx:      ctx,
		ctr:      ctr,
		ctrID:    ctr.GetContainerID(),
		adminDSN: adminDSN,
		artDir:   t.TempDir(),
	}
	f.admin = bkpOpenPool(t, adminDSN)
	f.pg = &bkpContainerPGCommand{t: t, ctrID: f.ctrID}

	// Data database: the real repository migrations (the manifest records the
	// exact goose set; restore probes compare against it).
	f.srcDSN = f.createDatabase(t, "bkp_src")
	if err := db.MigrateUp(ctx, db.MigrateOptions{
		DSN: f.srcDSN, LockTimeout: 30 * time.Second, ConnectTimeout: 5 * time.Second,
	}, io.Discard); err != nil {
		t.Fatalf("migrate data database: %v", err)
	}
	f.src = bkpOpenPool(t, f.srcDSN)

	// Control store: independent database with its own goose sequence
	// (ADR-001); the data DB backup never contains recovery_* tables.
	f.ctrlDSN = f.createDatabase(t, "bkp_ctrl")
	if err := db.MigrateUp(ctx, db.MigrateOptions{
		DSN: f.ctrlDSN, LockTimeout: 10 * time.Second, ConnectTimeout: 5 * time.Second, FS: schema.FS,
	}, io.Discard); err != nil {
		t.Fatalf("migrate control database: %v", err)
	}
	f.ctrl = bkpOpenPool(t, f.ctrlDSN)
	store, err := controlstore.NewStore(ctx, f.ctrl)
	if err != nil {
		t.Fatalf("controlstore.NewStore: %v", err)
	}
	f.store = store

	// This fixture's configured authoritative target is the migrated data
	// database. Bind the recovery instance to its immutable endpoint and role.
	authoritative, err := controlstore.ParseDSNTarget(f.srcDSN)
	if err != nil {
		t.Fatalf("parse authoritative target: %v", err)
	}
	f.restoreTargetGuard, err = controlstore.TargetGuardKey(authoritative)
	if err != nil {
		t.Fatalf("derive authoritative target guard: %v", err)
	}
	f.restoreTargetRole = authoritative.DataTargetFingerprint().RoleFingerprint
	f.restoreTargetDSN = f.srcDSN
	guardTx, err := f.ctrl.Begin(ctx)
	if err != nil {
		t.Fatalf("begin test target guard initialization: %v", err)
	}
	if err := controlstore.InitializeTargetGuard(ctx, guardTx, f.restoreTargetGuard, "bkp-fixture-target-init"); err != nil {
		_ = guardTx.Rollback(ctx)
		t.Fatalf("initialize test target guard: %v", err)
	}
	if err := guardTx.Commit(ctx); err != nil {
		t.Fatalf("commit test target guard initialization: %v", err)
	}

	// One open recovery instance with an executor and a verifier, registered
	// through the real write paths. The test-only clean baseline below is
	// explicit controlled evidence; tests never bootstrap a production guard.
	opened, err := store.OpenInstance(ctx, controlstore.OpenInstanceRequest{
		Kind: "recovery", OpenedBy: "deploy:executor", EntryChainInventory: []uint64{1},
		TargetGuardKey: f.restoreTargetGuard, TargetRoleFingerprint: f.restoreTargetRole,
	})
	if err != nil {
		t.Fatalf("open recovery instance: %v", err)
	}
	f.instanceID = opened.InstanceID
	baselineTx, err := f.ctrl.Begin(ctx)
	if err != nil {
		t.Fatalf("begin test-only authoritative target baseline: %v", err)
	}
	baselineEvidence := []byte(fmt.Sprintf(
		`{"controlled_test_baseline":true,"fixture":"backup-restore","target_guard_key":%q,"target_role_fingerprint":%q}`,
		f.restoreTargetGuard, f.restoreTargetRole))
	if err := controlstore.RecordTargetGuardRebuild(ctx, baselineTx, f.instanceID, f.restoreTargetGuard,
		"deploy:test", "bkp-fixture-target-baseline", baselineEvidence); err != nil {
		_ = baselineTx.Rollback(ctx)
		t.Fatalf("record test-only target baseline evidence: %v", err)
	}
	if err := controlstore.ResolveTargetGuardClean(ctx, baselineTx, f.restoreTargetGuard,
		"bkp-fixture-target-baseline", baselineEvidence); err != nil {
		_ = baselineTx.Rollback(ctx)
		t.Fatalf("resolve test-only target baseline: %v", err)
	}
	if err := baselineTx.Commit(ctx); err != nil {
		t.Fatalf("commit test-only target baseline: %v", err)
	}
	f.mapIdentity(t, "deploy:executor", "person-executor")
	f.mapIdentity(t, "auth:verifier", "person-verifier")
	f.register(t, "deploy:executor", "executor")
	f.register(t, "auth:verifier", "verifier")
	return f
}

func (f *bkpFixture) mapIdentity(t *testing.T, principal, person string) {
	t.Helper()
	f.seq++
	if _, err := f.store.SetIdentityMapping(f.ctx, controlstore.SetIdentityMappingRequest{
		Principal: principal, PersonID: person, RecordedBy: "deploy:admin",
		OperationID: fmt.Sprintf("bkp-map-%d", f.seq),
	}); err != nil {
		t.Fatalf("map identity %s -> %s: %v", principal, person, err)
	}
}

func (f *bkpFixture) register(t *testing.T, principal, role string) {
	t.Helper()
	f.seq++
	if _, err := f.store.RegisterParticipant(f.ctx, controlstore.RegisterParticipantRequest{
		InstanceID: f.instanceID, Principal: principal, Role: role,
		Actor: "deploy:admin", OperationID: fmt.Sprintf("bkp-reg-%d", f.seq),
	}); err != nil {
		t.Fatalf("register %s as %s: %v", principal, role, err)
	}
}

// createDatabase creates a fresh database inside the fixture container and
// returns its DSN.
func (f *bkpFixture) createDatabase(t *testing.T, label string) string {
	t.Helper()
	f.seq++
	name := fmt.Sprintf("bkp_t_%s_%d", label, f.seq)
	if _, err := f.admin.Exec(f.ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatalf("create database %s: %v", name, err)
	}
	return bkpDSNFor(t, f.adminDSN, name)
}

// rebuildTarget drops the target database WITH (FORCE) and recreates it empty
// (F2: retry = rebuild the target database, then rerun).
func (f *bkpFixture) rebuildTarget(t *testing.T, target bkpTarget) string {
	t.Helper()
	if _, err := f.admin.Exec(f.ctx,
		"DROP DATABASE IF EXISTS "+pgx.Identifier{target.name}.Sanitize()+" WITH (FORCE)"); err != nil {
		t.Fatalf("drop target %s: %v", target.name, err)
	}
	if _, err := f.admin.Exec(f.ctx, "CREATE DATABASE "+pgx.Identifier{target.name}.Sanitize()); err != nil {
		t.Fatalf("recreate target %s: %v", target.name, err)
	}
	if target.name == bkpDBNameOf(t, f.adminDSN, f.restoreTargetDSN) {
		f.resolveTestTargetBaseline(t, "after-rebuild")
	}
	return bkpDSNFor(t, f.adminDSN, target.name)
}

// resolveTestTargetBaseline is test-only controlled evidence after this
// fixture has physically dropped and recreated its authoritative target.
func (f *bkpFixture) resolveTestTargetBaseline(t *testing.T, suffix string) {
	t.Helper()
	op := "bkp-fixture-target-baseline-" + suffix
	evidence := []byte(fmt.Sprintf(
		`{"controlled_test_baseline":true,"fixture":"backup-restore","target":"recreated-authoritative","target_guard_key":%q,"target_role_fingerprint":%q}`,
		f.restoreTargetGuard, f.restoreTargetRole))
	tx, err := f.ctrl.Begin(f.ctx)
	if err != nil {
		t.Fatalf("begin controlled test target baseline: %v", err)
	}
	defer func() { _ = tx.Rollback(f.ctx) }()
	if err := controlstore.RecordTargetGuardRebuild(f.ctx, tx, f.instanceID, f.restoreTargetGuard,
		"deploy:test", op, evidence); err != nil {
		t.Fatalf("record controlled test target rebuild evidence: %v", err)
	}
	if err := controlstore.ResolveTargetGuardClean(f.ctx, tx, f.restoreTargetGuard, op, evidence); err != nil {
		t.Fatalf("resolve controlled test target baseline: %v", err)
	}
	if err := tx.Commit(f.ctx); err != nil {
		t.Fatalf("commit controlled test target baseline: %v", err)
	}
}

func TestBkpFixtureVerifyTargetBindingIsDistinctAndTestControlled(t *testing.T) {
	f := newBkpFixture(t)
	verifyDSN := f.createDatabase(t, "verify_binding")
	binding := f.bindTestVerifyTarget(t, verifyDSN)
	authoritative, err := controlstore.ParseDSNTarget(f.srcDSN)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := controlstore.ParseDSNTarget(binding.BoundDSN())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.EqualFold(authoritative.Host, verified.Host) || authoritative.Port != verified.Port || authoritative.Database == verified.Database {
		t.Fatalf("verification target must be a distinct database on the authoritative endpoint: authoritative=%+v verify=%+v", authoritative, verified)
	}
	guard, found, err := controlstore.ReadTargetGuard(f.ctx, f.ctrl, binding.TargetGuardKey())
	if err != nil || !found || guard.State != controlstore.TargetGuardClean {
		t.Fatalf("isolated verification guard is not clean: guard=%+v found=%t err=%v", guard, found, err)
	}
	var baseline struct {
		ControlledTestBaseline bool `json:"controlled_test_baseline"`
	}
	if err := json.Unmarshal(guard.RebuildEvidence, &baseline); err != nil || !baseline.ControlledTestBaseline {
		t.Fatalf("verify guard clean state lacks explicit test-only baseline evidence: evidence=%s err=%v", guard.RebuildEvidence, err)
	}
}

// controlEvidenceCount counts control-store evidence rows for one backup and
// kind. scope->>'backup_id' is the manifest binding (data-model §1.4).
func (f *bkpFixture) controlEvidenceCount(t *testing.T, kind, backupID string) int {
	t.Helper()
	var n int
	if err := f.ctrl.QueryRow(f.ctx,
		`SELECT count(*) FROM recovery_evidence
		  WHERE instance_id = $1 AND kind = $2 AND scope->>'backup_id' = $3`,
		f.instanceID, kind, backupID).Scan(&n); err != nil {
		t.Fatalf("count control evidence (%s/%s): %v", kind, backupID, err)
	}
	return n
}

// seedProbeTable creates the small probe table and returns nothing; callers
// insert before/after rows at the exact snapshot boundaries.
func (f *bkpFixture) seedProbeTable(t *testing.T) {
	t.Helper()
	if _, err := f.src.Exec(f.ctx,
		`CREATE TABLE `+bkpProbeTable+` (id bigserial PRIMARY KEY, note text NOT NULL)`); err != nil {
		t.Fatalf("create probe table: %v", err)
	}
	if _, err := f.src.Exec(f.ctx,
		`INSERT INTO `+bkpProbeTable+` (note) VALUES ('before')`); err != nil {
		t.Fatalf("seed before row: %v", err)
	}
}

// probeCount counts the before/after probe rows visible through dsn.
func (f *bkpFixture) probeCount(t *testing.T, dsn string) (before, after int) {
	t.Helper()
	conn := bkpConnect(t, dsn)
	defer conn.Close(f.ctx)
	var exists bool
	if err := conn.QueryRow(f.ctx,
		`SELECT to_regclass($1) IS NOT NULL`, "public."+bkpProbeTable).Scan(&exists); err != nil {
		t.Fatalf("probe table lookup: %v", err)
	}
	if !exists {
		return 0, 0
	}
	if err := conn.QueryRow(f.ctx,
		`SELECT count(*) FILTER (WHERE note = 'before'), count(*) FILTER (WHERE note = 'after')
		   FROM `+bkpProbeTable).Scan(&before, &after); err != nil {
		t.Fatalf("probe counts: %v", err)
	}
	return before, after
}

// userTableCount counts ordinary user tables in the public schema of a
// database; a refused restore must leave it at zero (0 best-effort).
func (f *bkpFixture) userTableCount(t *testing.T, dsn string) int {
	t.Helper()
	conn := bkpConnect(t, dsn)
	defer conn.Close(f.ctx)
	var n int
	if err := conn.QueryRow(f.ctx,
		`SELECT count(*) FROM pg_class
		  WHERE relkind = 'r' AND relnamespace = 'public'::regnamespace
		    AND relname NOT LIKE 'pg_%'`).Scan(&n); err != nil {
		t.Fatalf("user table count: %v", err)
	}
	return n
}

// gooseVersions reads the exact applied goose set of a database.
func (f *bkpFixture) gooseVersions(t *testing.T, dsn string) []int64 {
	t.Helper()
	conn := bkpConnect(t, dsn)
	defer conn.Close(f.ctx)
	rows, err := conn.Query(f.ctx,
		`SELECT version_id FROM goose_db_version WHERE is_applied AND version_id > 0 ORDER BY version_id`)
	if err != nil {
		t.Fatalf("read goose versions: %v", err)
	}
	defer rows.Close()
	var versions []int64
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("scan goose version: %v", err)
		}
		versions = append(versions, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read goose versions: %v", err)
	}
	return versions
}

// importSnapshotTuple independently reads the exported snapshot in a second
// session (SET TRANSACTION SNAPSHOT) and returns the server-side tuple as
// "xmin:xmax:xip_list" - the ground truth the manifest must match.
func (f *bkpFixture) importSnapshotTuple(t *testing.T, snapshotID string) string {
	t.Helper()
	if strings.ContainsAny(snapshotID, "'\\;") || snapshotID == "" {
		t.Fatalf("exported snapshot id %q is not safe to interpolate", snapshotID)
	}
	conn := bkpConnect(t, f.srcDSN)
	defer conn.Close(f.ctx)
	tx, err := conn.BeginTx(f.ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		t.Fatalf("begin snapshot import tx: %v", err)
	}
	defer func() { _ = tx.Rollback(f.ctx) }()
	if _, err := tx.Exec(f.ctx, "SET TRANSACTION SNAPSHOT '"+snapshotID+"'"); err != nil {
		t.Fatalf("SET TRANSACTION SNAPSHOT %s: %v (the backup transaction must still be open)", snapshotID, err)
	}
	var tuple string
	if err := tx.QueryRow(f.ctx, `SELECT pg_current_snapshot()::text`).Scan(&tuple); err != nil {
		t.Fatalf("read imported snapshot tuple: %v", err)
	}
	return tuple
}

// terminateExporters kills the backup transaction's backend while it is idle
// in transaction after pg_export_snapshot, so the exported snapshot ceases to
// exist before pg_dump runs (snapshot invalidation, no success manifest).
func (f *bkpFixture) terminateExporters(ctx context.Context) error {
	var killed int
	if err := f.src.QueryRow(ctx,
		`SELECT count(*) FILTER (WHERE pg_terminate_backend(pid))
		   FROM pg_stat_activity
		  WHERE datname = current_database() AND pid <> pg_backend_pid()
		    AND state = 'idle in transaction' AND query ILIKE '%pg_export_snapshot%'`).Scan(&killed); err != nil {
		return fmt.Errorf("terminate exporter backend: %w", err)
	}
	if killed == 0 {
		return errors.New("no idle-in-transaction exporter backend found; cannot invalidate the snapshot")
	}
	return nil
}

// waitForLockWaiter waits until a pg_restore backend is parked on the lock the
// test holds in the target database, and returns its pid(s).
func (f *bkpFixture) waitForLockWaiter(t *testing.T, targetDB string, timeout time.Duration) []int32 {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		rows, err := f.admin.Query(f.ctx,
			`SELECT pid FROM pg_stat_activity
			  WHERE datname = $1 AND wait_event_type = 'Lock' AND pid <> pg_backend_pid()
			  ORDER BY pid`, targetDB)
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
			t.Fatalf("no pg_restore lock waiter in %s after %s (the restore never reached the target)", targetDB, timeout)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// terminateBackends terminates the given backends (real mid-flight
// interruption of the real pg_restore).
func (f *bkpFixture) terminateBackends(t *testing.T, pids []int32) {
	t.Helper()
	for _, pid := range pids {
		var ok bool
		if err := f.admin.QueryRow(f.ctx, `SELECT pg_terminate_backend($1)`, pid).Scan(&ok); err != nil {
			t.Fatalf("terminate backend %d: %v", pid, err)
		}
		if !ok {
			t.Fatalf("pg_terminate_backend(%d) = false", pid)
		}
	}
}

// ---------------------------------------------------------------------------
// Container-backed PGCommand: the real pg_dump/pg_restore binaries from the
// pinned image, with archives on stdin/stdout.
// ---------------------------------------------------------------------------

// bkpContainerPGCommand implements PGCommand by executing the real
// PostgreSQL client binaries inside the fixture container. DSNs that address
// the container through its host-mapped port are rewritten to the container's
// loopback (127.0.0.1:5432), which is the same server and database.
type bkpContainerPGCommand struct {
	t     *testing.T
	ctrID string
}

func (c *bkpContainerPGCommand) Run(ctx context.Context, name string, args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	dockerArgs := append([]string{"exec", "-i", c.ctrID, name}, bkpRewriteDSNArgs(args)...)
	cmd := exec.CommandContext(ctx, "docker", dockerArgs...)
	cmd.Stdin = stdin
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("run %s in pinned container: %w", name, err)
	}
	return nil
}

func bkpRewriteDSNArgs(args []string) []string {
	out := make([]string, 0, len(args))
	for _, arg := range args {
		out = append(out, bkpRewriteDSNArg(arg))
	}
	return out
}

func bkpRewriteDSNArg(arg string) string {
	for _, scheme := range []string{"postgres://", "postgresql://"} {
		idx := strings.Index(arg, scheme)
		if idx < 0 {
			continue
		}
		u, err := url.Parse(arg[idx:])
		if err != nil {
			return arg
		}
		u.Host = net.JoinHostPort("127.0.0.1", bkpInternalPort)
		return arg[:idx] + u.String()
	}
	return arg
}

// ---------------------------------------------------------------------------
// Probe-exit synchronization
// ---------------------------------------------------------------------------

// Probe-exit synchronization bounds: the local exit-window experiment
// (docs/evidence/015-fix-session-census/) measured a p95 exit lag of ~27ms and
// a max of ~28ms over 300 Close->observe iterations; 10s stays orders of
// magnitude above the observed window while bounded, and 25ms matches the
// fixture's existing drain-poll cadence (waitForLockWaiter).
const (
	bkpProbeExitTimeout  = 10 * time.Second
	bkpProbeExitInterval = 25 * time.Millisecond

	// bkpTargetSessionDumpTimeout is the dump's own short, explicit budget. The
	// dump is best-effort evidence appended to an already-decided error, so a
	// blocked diagnostic round trip must never extend the bounded decision
	// (which runs inside the rebuild lock's acceptance transaction) nor replace
	// its primary error.
	bkpTargetSessionDumpTimeout = 2 * time.Second
)

// bkpAwaitProbeExit performs bounded lifecycle synchronization on the test's
// own just-closed probe backend: it polls until the exact (pid, backend_start)
// tuple is absent from pg_stat_activity. PostgreSQL has no wait-for-foreign-
// backend primitive and pgx Close returns no server-side terminal event (it is
// a client-local close), so a bounded poll is the strongest available sync. It
// waits for that one identity only: it never excludes by role or
// application_name, never terminates other sessions, and never weakens the
// authoritative target-wide census that follows it.
//
// The window is enforced on the wire, not only between round trips: every pool
// acquisition, QueryRow/Scan and poll sleep runs on a context derived from the
// caller's with the same timeout, and the caller's cancellation propagates into
// it. A hung connect/acquire/query therefore ends the wait at the deadline with
// the probe tuple and the remaining-session diagnostics instead of pinning the
// caller's rebuild lock forever.
func bkpAwaitProbeExit(ctx context.Context, admin *pgxpool.Pool, targetDB string, probePID int32, probeBackendStart time.Time, timeout, interval time.Duration) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("probe exit wait canceled: %w", err)
	}
	waitCtx, cancelWait := context.WithTimeout(ctx, timeout)
	defer cancelWait()
	deadline := time.Now().Add(timeout)
	for {
		var n int
		if err := admin.QueryRow(waitCtx,
			`SELECT count(*) FROM pg_stat_activity WHERE pid=$1 AND backend_start=$2 AND datname=$3`,
			probePID, probeBackendStart, targetDB).Scan(&n); err != nil {
			switch {
			case ctx.Err() != nil:
				return fmt.Errorf("probe exit wait canceled: %w", ctx.Err())
			case waitCtx.Err() != nil:
				return bkpProbeExitTimedOut(ctx, admin, targetDB, probePID, probeBackendStart, timeout)
			default:
				return fmt.Errorf("observe probe exit: %w", err)
			}
		}
		if n == 0 {
			return nil
		}
		if !time.Now().Before(deadline) {
			return bkpProbeExitTimedOut(ctx, admin, targetDB, probePID, probeBackendStart, timeout)
		}
		select {
		case <-waitCtx.Done():
			if ctxErr := ctx.Err(); ctxErr != nil {
				return fmt.Errorf("probe exit wait canceled: %w", ctxErr)
			}
			return bkpProbeExitTimedOut(ctx, admin, targetDB, probePID, probeBackendStart, timeout)
		case <-time.After(interval):
		}
	}
}

// bkpProbeExitTimedOut reports the bounded window's expiry. The probe tuple and
// the timeout dominate the message; the best-effort session dump is appended and
// carries its own short budget, and a failed/blocked dump is reported inline as
// an additional line instead of masking the primary timeout.
func bkpProbeExitTimedOut(ctx context.Context, admin *pgxpool.Pool, targetDB string, probePID int32, probeBackendStart time.Time, timeout time.Duration) error {
	return fmt.Errorf("probe backend (pid=%d backend_start=%s) still visible on %s after %s: %s",
		probePID, probeBackendStart.Format(time.RFC3339Nano), targetDB, timeout,
		bkpTargetSessionDump(ctx, admin, targetDB))
}

// bkpTargetSessionDump renders identifying diagnostics for every remaining
// session on the target database (server-side columns only). It runs on its own
// short budget derived from the caller's context, so a blocked diagnostic query
// cannot extend a bounded decision or hold the caller's rebuild lock.
func bkpTargetSessionDump(ctx context.Context, admin *pgxpool.Pool, targetDB string) string {
	dumpCtx, cancelDump := context.WithTimeout(ctx, bkpTargetSessionDumpTimeout)
	defer cancelDump()
	rows, err := admin.Query(dumpCtx,
		`SELECT pid, COALESCE(backend_start::text,''), COALESCE(application_name,''),
		        COALESCE(usename,''), COALESCE(state,''), COALESCE(wait_event_type,''), COALESCE(wait_event,''),
		        COALESCE(left(query,120),'')
		 FROM pg_stat_activity WHERE datname=$1 ORDER BY pid`, targetDB)
	if err != nil {
		return fmt.Sprintf("\n  (session dump unavailable: %v)", err)
	}
	defer rows.Close()
	var b strings.Builder
	for rows.Next() {
		var pid int32
		var start, app, user, state, waitType, waitEvent, query string
		if err := rows.Scan(&pid, &start, &app, &user, &state, &waitType, &waitEvent, &query); err != nil {
			return fmt.Sprintf("\n  (session dump scan: %v)", err)
		}
		fmt.Fprintf(&b, "  remaining session pid=%d backend_start=%s application_name=%q usename=%q state=%q wait_event=(%s,%s) query=%q\n",
			pid, start, app, user, state, waitType, waitEvent, query)
	}
	if err := rows.Err(); err != nil {
		// The dump was cut short by its own budget (or the caller's context):
		// report the truncation instead of silently presenting partial evidence.
		fmt.Fprintf(&b, "  (session dump incomplete: %v)\n", err)
	}
	if b.Len() == 0 {
		return "  (no remaining sessions on dump)"
	}
	return strings.TrimRight(b.String(), "\n")
}

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

func bkpOpenPool(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()
	pool, err := db.OpenPool(context.Background(), dsn, 5*time.Second)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func bkpConnect(t *testing.T, dsn string) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		t.Fatalf("connect %s: %v", bkpRedactDSN(dsn), err)
	}
	return conn
}

func bkpDSNFor(t *testing.T, baseDSN, dbName string) string {
	t.Helper()
	u, err := url.Parse(baseDSN)
	if err != nil {
		t.Fatalf("parse base dsn: %v", err)
	}
	u.Path = "/" + dbName
	return u.String()
}

func bkpDBNameOf(t *testing.T, baseDSN, dsn string) string {
	t.Helper()
	base, err := url.Parse(baseDSN)
	if err != nil {
		t.Fatalf("parse base dsn: %v", err)
	}
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	if u.Host != base.Host {
		t.Fatalf("dsn %s is not on the fixture server", bkpRedactDSN(dsn))
	}
	return strings.TrimPrefix(u.Path, "/")
}

func bkpRedactDSN(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		return "<unparsable-dsn>"
	}
	u.User = url.User(u.User.Username())
	return u.String()
}

func bkpFileSHA256(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read artifact %s: %v", path, err)
	}
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func bkpArtifactPath(t *testing.T, manifestPath string, artifact ManifestArtifact) string {
	t.Helper()
	if filepath.IsAbs(artifact.Path) {
		return artifact.Path
	}
	return filepath.Join(filepath.Dir(manifestPath), artifact.Path)
}

func bkpWriteManifest(t *testing.T, path string, m *Manifest) {
	t.Helper()
	canonical, err := m.CanonicalJSON()
	if err != nil {
		t.Fatalf("CanonicalJSON: %v", err)
	}
	if err := os.WriteFile(path, canonical, 0o600); err != nil {
		t.Fatalf("write manifest %s: %v", path, err)
	}
}

func bkpReadManifest(t *testing.T, path string) *Manifest {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read manifest %s: %v", path, err)
	}
	m, err := ParseManifest(data)
	if err != nil {
		t.Fatalf("ParseManifest(%s): %v", path, err)
	}
	return m
}

func bkpCopyFile(t *testing.T, src, dst string) {
	t.Helper()
	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatalf("read %s: %v", src, err)
	}
	if err := os.WriteFile(dst, data, 0o600); err != nil {
		t.Fatalf("write %s: %v", dst, err)
	}
}

// bkpAssertArtifactDirUnchanged asserts a failed backup left the artifact
// directory byte-identical: no partial dump, no manifest, no mixing with the
// pre-existing artifacts.
func bkpAssertArtifactDirUnchanged(t *testing.T, dir, sentinel string, sentinelBefore []byte) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read artifact dir: %v", err)
	}
	for _, entry := range entries {
		if entry.Name() != filepath.Base(sentinel) {
			t.Fatalf("failed backup left unexpected artifact %q in %s (fail must not mix with existing artifacts)", entry.Name(), dir)
		}
	}
	if len(entries) != 1 {
		t.Fatalf("artifact dir has %d entries, want only the sentinel", len(entries))
	}
	after, err := os.ReadFile(sentinel)
	if err != nil {
		t.Fatalf("read sentinel: %v", err)
	}
	if !bytes.Equal(sentinelBefore, after) {
		t.Fatalf("sentinel changed: before %d bytes, after %d bytes", len(sentinelBefore), len(after))
	}
}

// bkpSnapshotTuple renders a manifest snapshot in the server's
// pg_current_snapshot() text form so the two can be compared verbatim.
func bkpSnapshotTuple(s ManifestSnapshot) string {
	parts := make([]string, 0, len(s.Xip))
	for _, xid := range s.Xip {
		parts = append(parts, fmt.Sprint(xid))
	}
	return fmt.Sprintf("%d:%d:%s", s.Xmin, s.Xmax, strings.Join(parts, ","))
}

// bkpSortedInt64 returns a sorted copy for set comparison.
func bkpSortedInt64(values []int64) []int64 {
	out := append([]int64(nil), values...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func bkpEqualInt64(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	a, b = bkpSortedInt64(a), bkpSortedInt64(b)
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// bkpVerifyBackup is the shared happy-path verify call.
func (f *bkpFixture) verifyBackup(t *testing.T, manifestPath, targetDSN string) VerifyBackupResult {
	t.Helper()
	requireLocalPGRestore(t)
	binding := f.bindTestVerifyTarget(t, targetDSN)
	// Fixture target DSNs use the container superuser, which can observe the
	// exact tagged restore session used by quiescence checks.
	result, err := ExecuteVerifyBackup(f.ctx, VerifyBackupOptions{
		ManifestPath:     manifestPath,
		Binding:          binding,
		TargetDSN:        targetDSN,
		Verifier:         "auth:verifier",
		InstanceID:       f.instanceID,
		ControlStore:     f.store,
		ControlDSN:       f.ctrlDSN,
		AuthoritativeDSN: f.srcDSN,
		ObserverDSN:      targetDSN,
		ProgramVersion:   bkpProgramVersion,
		PG:               f.pg,
	})
	if err != nil {
		t.Fatalf("ExecuteVerifyBackup(%s): %v", filepath.Base(manifestPath), err)
	}
	return result
}

// bindTestVerifyTarget models the deployment-configured isolated database:
// it is a distinct database on the authoritative cluster, and collision
// checks use the current open-instance inventory. Its clean row is only
// controlled test-baseline evidence, never production proof.
func (f *bkpFixture) bindTestVerifyTarget(t *testing.T, targetDSN string) IsolatedTarget {
	t.Helper()
	open, err := readOpenIsolatedBindings(f.ctx, f.store)
	if err != nil {
		t.Fatalf("read current open target bindings: %v", err)
	}
	binding, err := BindIsolatedTarget(f.srcDSN, f.ctrlDSN, targetDSN, open)
	if err != nil {
		t.Fatalf("bind isolated test verification target: %v", err)
	}
	tx, err := f.ctrl.Begin(f.ctx)
	if err != nil {
		t.Fatalf("begin isolated test target guard baseline: %v", err)
	}
	defer func() { _ = tx.Rollback(f.ctx) }()
	evidence := []byte(fmt.Sprintf(
		`{"controlled_test_baseline":true,"fixture":"backup-restore","purpose":"isolated-verification-target","target_guard_key":%q,"target_role_fingerprint":%q}`,
		binding.TargetGuardKey(), binding.RoleFingerprint()))
	if err := controlstore.InitializeTargetGuard(f.ctx, tx, binding.TargetGuardKey(), "bkp-fixture-verify-init"); err != nil {
		t.Fatalf("initialize isolated test target guard: %v", err)
	}
	if err := controlstore.ResolveTargetGuardClean(f.ctx, tx, binding.TargetGuardKey(),
		"bkp-fixture-verify-baseline", evidence); err != nil {
		t.Fatalf("resolve isolated test target baseline: %v", err)
	}
	if err := tx.Commit(f.ctx); err != nil {
		t.Fatalf("commit isolated test target baseline: %v", err)
	}
	return binding
}

// deploymentConvergence builds the fixture deployment-lane step for a bound
// restore: the isolated fixture's admin identity converges public relation
// ownership to the target's role and records the audited prerequisite.
func (f *bkpFixture) deploymentConvergence(targetDSN string) ConvergenceStep {
	target, err := controlstore.ParseDSNTarget(targetDSN)
	if err != nil {
		f.t.Fatalf("parse convergence target: %v", err)
	}
	step := DeploymentConvergence{
		AdminDSN:     f.adminDSN,
		TargetDSN:    targetDSN,
		OriginalRole: target.Role,
		Actor:        "deploy:convergence",
		Store:        f.store,
	}
	return step.Converge
}

// restore is the shared happy-path restore call.
func (f *bkpFixture) restore(t *testing.T, manifestPath, targetDSN string, declaration TargetDeclaration, reason string) RestoreResult {
	t.Helper()
	requireLocalPGRestore(t)
	result, err := ExecuteRestore(f.ctx, RestoreOptions{
		ManifestPath:      manifestPath,
		InstanceID:        f.instanceID,
		ControlStore:      f.store,
		ControlDSN:        f.ctrlDSN,
		TargetDSN:         targetDSN,
		ObserverDSN:       targetDSN,
		TargetDeclaration: declaration,
		TargetReason:      reason,
		Actor:             "deploy:executor",
		ProgramVersion:    bkpProgramVersion,
		PG:                f.pg,
		Convergence:       f.deploymentConvergence(targetDSN),
	})
	if err != nil {
		t.Fatalf("ExecuteRestore(%s): %v", filepath.Base(manifestPath), err)
	}
	return result
}

func requireLocalPGRestore(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("pg_restore"); err != nil {
		t.Skip("NOT RUN: supervised restore requires a local pg_restore executable; the injected container PGCommand only lists archives and is not the supervised child")
	}
}

// bkpBackup runs the real backup and returns its result.
func (f *bkpFixture) backup(t *testing.T, mutate func(*BackupOptions)) BackupResult {
	t.Helper()
	opts := BackupOptions{
		DSN:                  f.srcDSN,
		ArtifactDir:          f.artDir,
		CreatedBy:            "deploy:executor",
		ProgramVersion:       bkpProgramVersion,
		ProgramMinCompatible: bkpProgramVersion,
		PG:                   f.pg,
	}
	if mutate != nil {
		mutate(&opts)
	}
	result, err := ExecuteBackup(f.ctx, opts)
	if err != nil {
		t.Fatalf("ExecuteBackup: %v", err)
	}
	return result
}

func (f *bkpFixture) refusedRestore(t *testing.T, manifestPath, targetDSN string) RestoreResult {
	t.Helper()
	result, err := ExecuteRestore(f.ctx, RestoreOptions{
		ManifestPath:      manifestPath,
		InstanceID:        f.instanceID,
		ControlStore:      f.store,
		ControlDSN:        f.ctrlDSN,
		TargetDSN:         targetDSN,
		ObserverDSN:       targetDSN,
		TargetDeclaration: TargetIsolated,
		Actor:             "deploy:executor",
		ProgramVersion:    bkpProgramVersion,
		PG:                f.pg,
	})
	if err == nil {
		t.Fatal("ExecuteRestore succeeded, want refusal")
	}
	if result.Restored {
		t.Fatal("refused restore reports Restored=true")
	}
	return result
}

// ---------------------------------------------------------------------------
// Test 1: snapshot-bound dump -> isolated verify -> isolated restore.
// ---------------------------------------------------------------------------

func TestBackupVerifyRestoreSnapshotBoundIsolated(t *testing.T) {
	f := newBkpFixture(t)
	f.seedProbeTable(t)

	var exported SnapshotExport
	var importedTuple string
	backup := f.backup(t, func(opts *BackupOptions) {
		opts.SnapshotBarrier = func(ctx context.Context, export SnapshotExport) error {
			if export.SnapshotID == "" {
				return errors.New("exported snapshot id is empty")
			}
			exported = export
			// Independently read the exported snapshot from a second session.
			importedTuple = f.importSnapshotTuple(t, export.SnapshotID)
			// Commit a write after the export: it must never appear in the
			// dump, so its absence from the restored target proves the dump
			// used this exact snapshot.
			_, err := f.src.Exec(ctx, `INSERT INTO `+bkpProbeTable+` (note) VALUES ('after')`)
			return err
		}
	})

	m := backup.Manifest
	if m == nil {
		t.Fatal("ExecuteBackup returned no manifest")
	}
	// S1: a fresh backup is unverified.
	if m.Verification.State != VerificationUnverified {
		t.Fatalf("fresh backup state = %q, want %q", m.Verification.State, VerificationUnverified)
	}
	// The manifest recovery point equals the independently imported snapshot
	// tuple and matches what the barrier observed.
	if got := bkpSnapshotTuple(m.RecoveryPoint.Snapshot); got != importedTuple {
		t.Fatalf("manifest snapshot tuple = %q, want exported tuple %q", got, importedTuple)
	}
	if bkpSnapshotTuple(exported.RecoveryPoint.Snapshot) != importedTuple {
		t.Fatalf("barrier recovery point = %+v, want tuple %q", exported.RecoveryPoint, importedTuple)
	}
	if _, err := time.Parse(time.RFC3339, m.RecoveryPoint.WallClock); err != nil {
		t.Fatalf("wall_clock %q is not RFC3339: %v", m.RecoveryPoint.WallClock, err)
	}
	if time.Since(mustParseRFC3339(t, m.RecoveryPoint.WallClock)) > bkpClockSkew {
		t.Fatalf("wall_clock %q is not within the test window", m.RecoveryPoint.WallClock)
	}
	if m.RecoveryPoint.LSN == "" || m.RecoveryPoint.Server == "" || m.RecoveryPoint.Database == "" {
		t.Fatalf("recovery point incomplete: %+v", m.RecoveryPoint)
	}

	// artifacts[] binding: the recorded bytes and sha256 match the real file.
	if len(m.Artifacts) == 0 {
		t.Fatal("manifest has no artifacts")
	}
	for _, artifact := range m.Artifacts {
		path := bkpArtifactPath(t, backup.ManifestPath, artifact)
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("artifact %s: %v", path, err)
		}
		if info.Size() != artifact.Bytes {
			t.Fatalf("artifact %s bytes = %d, manifest says %d", path, info.Size(), artifact.Bytes)
		}
		if got := bkpFileSHA256(t, path); got != artifact.SHA256 {
			t.Fatalf("artifact %s sha256 = %s, manifest says %s", path, got, artifact.SHA256)
		}
	}
	if onDisk := bkpReadManifest(t, backup.ManifestPath); onDisk.BackupID != m.BackupID {
		t.Fatalf("manifest file backup_id = %s, want %s", onDisk.BackupID, m.BackupID)
	}

	// verify-backup: real isolated pg_restore + four checks.
	verifyTarget := f.createDatabase(t, "tgt_verify")
	verified := f.verifyBackup(t, backup.ManifestPath, verifyTarget)
	if verified.State != VerificationVerified {
		t.Fatalf("verify state = %q, want %q (detailed: %+v)", verified.State, VerificationVerified, verified)
	}
	checks := verified.Checks
	if !checks.Readable || !checks.StructureConstraints || !checks.BusinessStateProbes || !checks.VerificationExecutable {
		t.Fatalf("verify checks = %+v, want all four true", checks)
	}
	written := bkpReadManifest(t, backup.ManifestPath)
	if written.Verification.State != VerificationVerified || written.Verification.Target != "isolated" ||
		written.Verification.VerifiedAt == "" || written.Verification.Verifier == "" || written.Verification.EvidenceRef == "" {
		t.Fatalf("manifest write-back = %+v", written.Verification)
	}
	if got := f.controlEvidenceCount(t, "backup_manifest", m.BackupID); got != 1 {
		t.Fatalf("backup_manifest evidence rows = %d, want 1 (bound to backup_id + instance)", got)
	}

	// The dump really used the exported snapshot: 'before' is restored,
	// 'after' (committed after export) is not.
	before, after := f.probeCount(t, verifyTarget)
	if before != 1 || after != 0 {
		t.Fatalf("verify target probe rows before=%d after=%d, want 1/0 (dump must use the exported snapshot)", before, after)
	}
	if got, want := f.gooseVersions(t, verifyTarget), m.Schema.GooseDBVersion; !bkpEqualInt64(got, want) {
		t.Fatalf("verify target goose set = %v, want manifest set %v", got, want)
	}

	// Restore targets the fixture's configured authoritative database.
	restoreTarget := f.restoreTargetDSN
	restored := f.restore(t, backup.ManifestPath, restoreTarget, TargetProductionMain, "controlled integration restore")
	if !restored.Restored {
		t.Fatalf("restore result = %+v, want Restored=true", restored)
	}
	before, after = f.probeCount(t, restoreTarget)
	if before != 1 || after != 0 {
		t.Fatalf("restore target probe rows before=%d after=%d, want 1/0", before, after)
	}
	if got := f.controlEvidenceCount(t, "restore_probe", m.BackupID); got != 1 {
		t.Fatalf("restore_probe evidence rows = %d, want 1", got)
	}
	if got := f.controlEvidenceCount(t, "backup_manifest", m.BackupID); got != 1 {
		t.Fatalf("backup_manifest evidence rows after restore = %d, want 1", got)
	}
}

func mustParseRFC3339(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		t.Fatalf("parse RFC3339 %q: %v", value, err)
	}
	return parsed
}

// ---------------------------------------------------------------------------
// Test 2: export failure / snapshot invalidation -> no success manifest.
// ---------------------------------------------------------------------------

func TestBackupFailureProducesNoSuccessManifest(t *testing.T) {
	f := newBkpFixture(t)
	f.seedProbeTable(t)

	sentinel := filepath.Join(f.artDir, "preexisting.artifact")
	sentinelBefore := []byte("pre-existing artifact from an earlier run")
	if err := os.WriteFile(sentinel, sentinelBefore, 0o600); err != nil {
		t.Fatalf("write sentinel: %v", err)
	}

	// (a) export failure: the exporter database does not exist.
	missingDSN := bkpDSNFor(t, f.adminDSN, "bkp_no_such_db")
	if _, err := ExecuteBackup(f.ctx, BackupOptions{
		DSN: missingDSN, ArtifactDir: f.artDir, CreatedBy: "deploy:executor",
		ProgramVersion: bkpProgramVersion, ProgramMinCompatible: bkpProgramVersion, PG: f.pg,
	}); err == nil {
		t.Fatal("ExecuteBackup against a missing database succeeded, want explicit failure")
	}
	bkpAssertArtifactDirUnchanged(t, f.artDir, sentinel, sentinelBefore)

	// (b) snapshot invalidation: the export transaction dies before pg_dump.
	if _, err := ExecuteBackup(f.ctx, BackupOptions{
		DSN: f.srcDSN, ArtifactDir: f.artDir, CreatedBy: "deploy:executor",
		ProgramVersion: bkpProgramVersion, ProgramMinCompatible: bkpProgramVersion, PG: f.pg,
		SnapshotBarrier: func(ctx context.Context, export SnapshotExport) error {
			return f.terminateExporters(ctx)
		},
	}); err == nil {
		t.Fatal("ExecuteBackup succeeded after the exported snapshot was invalidated, want explicit failure")
	}
	bkpAssertArtifactDirUnchanged(t, f.artDir, sentinel, sentinelBefore)
}

// ---------------------------------------------------------------------------
// Test 3: F1 - corrupt/truncated/partial/hash-mismatch/unverified are refused
// with zero best-effort restores.
// ---------------------------------------------------------------------------

func TestBackupCorruptionRejectedAndRestoreRefused(t *testing.T) {
	f := newBkpFixture(t)
	f.seedProbeTable(t)

	// A real backup, deliberately left unverified.
	backup := f.backup(t, nil)
	m := backup.Manifest
	if len(m.Artifacts) == 0 {
		t.Fatal("backup manifest has no artifacts")
	}
	originalDump := bkpArtifactPath(t, backup.ManifestPath, m.Artifacts[0])

	// (0) "unverified" is not usable: a real restore is refused and the
	// target is untouched (0 best-effort).
	unverifiedTarget := f.createDatabase(t, "tgt_unverified")
	refused := f.refusedRestore(t, backup.ManifestPath, unverifiedTarget)
	if len(refused.Blocked) == 0 {
		t.Fatal("refusal did not report the missing preconditions (Blocked is empty)")
	}
	if got := f.userTableCount(t, unverifiedTarget); got != 0 {
		t.Fatalf("unverified refusal created %d tables in the target (best-effort restore)", got)
	}

	// (1) a manifest file flag claiming verified is not proof: without the
	// control-store evidence row, restore stays refused (F1/F7).
	forged := bkpReadManifest(t, backup.ManifestPath)
	forged.Verification = ManifestVerification{
		State:       VerificationVerified,
		VerifiedAt:  time.Now().UTC().Format(time.RFC3339),
		Verifier:    "auth:verifier",
		Target:      "isolated",
		EvidenceRef: "forged://no-such-evidence",
		Checks: ManifestChecks{
			Readable: true, StructureConstraints: true,
			BusinessStateProbes: true, VerificationExecutable: true,
		},
	}
	forgedPath := filepath.Join(f.artDir, "forged.manifest.json")
	bkpWriteManifest(t, forgedPath, forged)
	forgedTarget := f.createDatabase(t, "tgt_forged")
	refused = f.refusedRestore(t, forgedPath, forgedTarget)
	if len(refused.Blocked) == 0 {
		t.Fatal("forged-verified refusal did not report missing control-store evidence")
	}
	if got := f.userTableCount(t, forgedTarget); got != 0 {
		t.Fatalf("forged-verified refusal created %d tables in the target", got)
	}
	if got := f.controlEvidenceCount(t, "restore_probe", m.BackupID); got != 0 {
		t.Fatalf("restore_probe evidence rows after refusals = %d, want 0", got)
	}

	// (2) corruption matrix: each variant is a real file/copy; verify-backup
	// must conclude rejected and restore must refuse.
	type corruption struct {
		name   string
		mutate func(t *testing.T, manifestPath string) // writes the variant manifest
	}
	cases := []corruption{
		{"missing artifact", func(t *testing.T, manifestPath string) {
			variant := bkpReadManifest(t, backup.ManifestPath)
			variant.Artifacts[0].Path = "no-such-dump.pgcustom"
			bkpWriteManifest(t, manifestPath, variant)
		}},
		{"truncated artifact", func(t *testing.T, manifestPath string) {
			copied := filepath.Join(f.artDir, "truncated.pgcustom")
			bkpCopyFile(t, originalDump, copied)
			data, err := os.ReadFile(copied)
			if err != nil {
				t.Fatalf("read copy: %v", err)
			}
			if err := os.WriteFile(copied, data[:len(data)/2], 0o600); err != nil {
				t.Fatalf("truncate copy: %v", err)
			}
			variant := bkpReadManifest(t, backup.ManifestPath)
			variant.Artifacts[0].Path = filepath.Base(copied)
			bkpWriteManifest(t, manifestPath, variant)
		}},
		{"partial write / bit flip", func(t *testing.T, manifestPath string) {
			copied := filepath.Join(f.artDir, "bitflip.pgcustom")
			bkpCopyFile(t, originalDump, copied)
			data, err := os.ReadFile(copied)
			if err != nil {
				t.Fatalf("read copy: %v", err)
			}
			data[len(data)/2] ^= 0xFF
			if err := os.WriteFile(copied, data, 0o600); err != nil {
				t.Fatalf("write flipped copy: %v", err)
			}
			variant := bkpReadManifest(t, backup.ManifestPath)
			variant.Artifacts[0].Path = filepath.Base(copied)
			bkpWriteManifest(t, manifestPath, variant)
		}},
		{"bytes mismatch", func(t *testing.T, manifestPath string) {
			variant := bkpReadManifest(t, backup.ManifestPath)
			variant.Artifacts[0].Bytes++
			bkpWriteManifest(t, manifestPath, variant)
		}},
		{"sha256 mismatch", func(t *testing.T, manifestPath string) {
			variant := bkpReadManifest(t, backup.ManifestPath)
			variant.Artifacts[0].SHA256 = "sha256:0000000000000000000000000000000000000000000000000000000000000000"
			bkpWriteManifest(t, manifestPath, variant)
		}},
		{"unparsable manifest", func(t *testing.T, manifestPath string) {
			if err := os.WriteFile(manifestPath, []byte("not a manifest"), 0o600); err != nil {
				t.Fatalf("write garbage manifest: %v", err)
			}
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			label := strings.NewReplacer(" ", "_", "/", "_").Replace(tc.name)
			manifestPath := filepath.Join(f.artDir, "variant-"+label+".json")
			tc.mutate(t, manifestPath)

			// verify-backup must not certify a corrupt/unusable manifest.
			verifyTarget := f.createDatabase(t, "tgt_corrupt")
			verifyResult, err := ExecuteVerifyBackup(f.ctx, VerifyBackupOptions{
				ManifestPath:   manifestPath,
				TargetDSN:      verifyTarget,
				Verifier:       "auth:verifier",
				InstanceID:     f.instanceID,
				ControlStore:   f.store,
				ProgramVersion: bkpProgramVersion,
				PG:             f.pg,
			})
			if err == nil && verifyResult.State != VerificationRejected {
				t.Fatalf("verify %s = state %q, want rejected (or an explicit refusal error)", tc.name, verifyResult.State)
			}
			if err != nil && verifyResult.State == VerificationVerified {
				t.Fatalf("verify %s returned an error but state verified", tc.name)
			}

			// restore must refuse and leave a fresh target untouched.
			restoreTarget := f.createDatabase(t, "tgt_refuse")
			refused := f.refusedRestore(t, manifestPath, restoreTarget)
			if len(refused.Blocked) == 0 {
				t.Fatal("refusal did not report missing preconditions")
			}
			if got := f.userTableCount(t, restoreTarget); got != 0 {
				t.Fatalf("refused restore created %d tables in the target (0 best-effort required)", got)
			}
		})
	}

	// No rejected variant may leave a usable backup_manifest evidence row.
	if got := f.controlEvidenceCount(t, "backup_manifest", m.BackupID); got != 0 {
		t.Fatalf("backup_manifest evidence rows for an unverified backup = %d, want 0", got)
	}
}

// ---------------------------------------------------------------------------
// Test 4: F2 - interruption is never restored; rebuild + rerun is idempotent.
// ---------------------------------------------------------------------------

func TestRestoreInterruptionNotRestoredAndRerunIdempotent(t *testing.T) {
	f := newBkpFixture(t)
	f.seedProbeTable(t)

	backup := f.backup(t, nil)
	m := backup.Manifest
	verifyTarget := f.createDatabase(t, "tgt_verify_f2")
	if got := f.verifyBackup(t, backup.ManifestPath, verifyTarget); got.State != VerificationVerified {
		t.Fatalf("verify state = %q, want verified", got.State)
	}

	target := f.restoreTargetDSN
	// Park pg_restore on a lock the test holds: the target carries a
	// conflicting probe table and the locker holds ACCESS EXCLUSIVE.
	locker := bkpConnect(t, target)
	if _, err := locker.Exec(f.ctx, `BEGIN`); err != nil {
		t.Fatalf("begin locker: %v", err)
	}
	if _, err := locker.Exec(f.ctx, `LOCK TABLE `+bkpProbeTable+` IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatalf("lock conflicting table: %v", err)
	}

	type restoreOutcome struct {
		result RestoreResult
		err    error
	}
	done := make(chan restoreOutcome, 1)
	go func() {
		result, err := ExecuteRestore(f.ctx, RestoreOptions{
			ManifestPath:      backup.ManifestPath,
			InstanceID:        f.instanceID,
			ControlStore:      f.store,
			ControlDSN:        f.ctrlDSN,
			TargetDSN:         target,
			ObserverDSN:       target,
			TargetDeclaration: TargetProductionMain,
			TargetReason:      "controlled restore interruption integration",
			Actor:             "deploy:executor",
			ProgramVersion:    bkpProgramVersion,
			PG:                f.pg,
		})
		done <- restoreOutcome{result: result, err: err}
	}()

	// Interrupt the real pg_restore mid-flight: it is parked on DROP TABLE.
	pids := f.waitForLockWaiter(t, bkpDBNameOf(t, f.adminDSN, target), 30*time.Second)
	f.terminateBackends(t, pids)
	// Release the lock and the locker connection.
	if _, err := locker.Exec(f.ctx, `ROLLBACK`); err != nil {
		t.Fatalf("release blocker lock: %v", err)
	}
	if err := locker.Close(f.ctx); err != nil {
		t.Fatalf("close locker: %v", err)
	}

	var interrupted restoreOutcome
	select {
	case interrupted = <-done:
	case <-time.After(2 * time.Minute):
		t.Fatal("interrupted restore did not return")
	}
	if interrupted.err == nil {
		t.Fatal("interrupted restore returned no error")
	}
	if interrupted.result.Restored {
		t.Fatal("interrupted restore was marked restored")
	}
	if got := f.controlEvidenceCount(t, "restore_probe", m.BackupID); got != 0 {
		t.Fatalf("restore_probe evidence rows after interruption = %d, want 0", got)
	}

	// Retry = rebuild the target database, then rerun (idempotent; no
	// duplicated or mixed state).
	rebuilt := f.rebuildInterruptedRestoreTarget(t, bkpTarget{name: bkpDBNameOf(t, f.adminDSN, target), dsn: target}, m.BackupID)
	rerun := f.restore(t, backup.ManifestPath, rebuilt, TargetProductionMain, "controlled restore retry after target rebuild")
	if !rerun.Restored {
		t.Fatalf("rerun after rebuild = %+v, want Restored=true", rerun)
	}
	before, after := f.probeCount(t, rebuilt)
	if before != 1 || after != 0 {
		t.Fatalf("rerun target probe rows before=%d after=%d, want 1/0 (no duplicated rows)", before, after)
	}
	var ids int
	conn := bkpConnect(t, rebuilt)
	if err := conn.QueryRow(f.ctx, `SELECT count(*) FROM `+bkpProbeTable).Scan(&ids); err != nil {
		t.Fatalf("count probe rows: %v", err)
	}
	_ = conn.Close(f.ctx)
	if ids != 1 {
		t.Fatalf("probe row count = %d, want exactly 1 (idempotent rerun, zero double/mixed state)", ids)
	}
	if got, want := f.gooseVersions(t, rebuilt), m.Schema.GooseDBVersion; !bkpEqualInt64(got, want) {
		t.Fatalf("rerun target goose set = %v, want %v", got, want)
	}
	if got := f.controlEvidenceCount(t, "restore_probe", m.BackupID); got != 1 {
		t.Fatalf("restore_probe evidence rows after successful rerun = %d, want 1", got)
	}
}

// rebuildInterruptedRestoreTarget only resolves the authoritative target guard
// after the supervised restore has returned, the exact interrupted attempt is
// inactive/rebuild-required, the executor's restore-start audit is present,
// all target sessions have drained, and a no-FORCE drop/create is observed
// under the target's shared advisory lock.
func (f *bkpFixture) rebuildInterruptedRestoreTarget(t *testing.T, target bkpTarget, backupID string) string {
	t.Helper()
	if target.name == "" || target.dsn == "" || target.name != bkpDBNameOf(t, f.adminDSN, f.restoreTargetDSN) {
		t.Fatal("interrupted restore rebuild must name the exact bound authoritative target")
	}
	key, err := CanonicalTargetKey(mustBkpDSNTarget(t, target.dsn))
	if err != nil {
		t.Fatalf("derive interrupted target lock key: %v", err)
	}
	lock, err := AcquireTargetLock(f.ctx, f.ctrlDSN, key, 10*time.Second, 25*time.Millisecond)
	if err != nil {
		t.Fatalf("acquire interrupted target rebuild lock: %v", err)
	}
	defer func() {
		if err := lock.Release(context.Background()); err != nil {
			t.Errorf("release interrupted target rebuild lock: %v", err)
		}
	}()

	// Drop the fixture's persistent source pool so the session inventory is
	// target-wide rather than mistaken for cleanliness while a session remains.
	if f.src != nil {
		f.src.Close()
		f.src = nil
	}
	guard, found, err := controlstore.ReadTargetGuard(f.ctx, f.ctrl, f.restoreTargetGuard)
	if err != nil || !found || guard.State != controlstore.TargetGuardRebuildRequired || guard.ActiveWriter ||
		!guard.LaunchIntent || guard.LaunchedAt == nil || guard.OperationID == "" || guard.AttemptAppName == "" {
		t.Fatalf("interrupted restore attempt is not drained/rebuild-required: found=%t guard=%+v err=%v", found, guard, err)
	}
	var executorMarkerCount int
	if err := f.ctrl.QueryRow(f.ctx, `SELECT count(*) FROM recovery_audit
WHERE instance_id=$1 AND actor='deploy:executor' AND action=$2 AND result='ok'
  AND target->>'backup_id'=$3`, f.instanceID, ActionRestoreStarted, backupID).Scan(&executorMarkerCount); err != nil {
		t.Fatalf("verify interrupted restore executor audit: %v", err)
	}
	if executorMarkerCount != 1 {
		t.Fatalf("interrupted restore executor marker audits = %d, want exactly one", executorMarkerCount)
	}

	var emptyBaseline bool
	if err := lock.WithTransaction(f.ctx, func(ctx context.Context, tx pgx.Tx) error {
		current, found, err := controlstore.ReadTargetGuard(ctx, tx, f.restoreTargetGuard)
		if err != nil || !found || current.State != controlstore.TargetGuardRebuildRequired || current.ActiveWriter ||
			current.OperationID != guard.OperationID || current.AttemptAppName != guard.AttemptAppName || current.LaunchedAt == nil {
			return fmt.Errorf("interrupted target attempt changed before rebuild: found=%t guard=%+v err=%v", found, current, err)
		}
		var sessions int
		if err := f.admin.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname=$1`, target.name).Scan(&sessions); err != nil {
			return fmt.Errorf("observe target sessions before rebuild: %w", err)
		}
		if sessions != 0 {
			return fmt.Errorf("target still has %d sessions; refusing destructive rebuild", sessions)
		}
		if _, err := f.admin.Exec(ctx, "DROP DATABASE "+pgx.Identifier{target.name}.Sanitize()); err != nil {
			return fmt.Errorf("drop drained target %s without FORCE: %w", target.name, err)
		}
		if _, err := f.admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{target.name}.Sanitize()); err != nil {
			return fmt.Errorf("recreate target %s: %w", target.name, err)
		}
		var exists bool
		if err := f.admin.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=$1)`, target.name).Scan(&exists); err != nil || !exists {
			return fmt.Errorf("observe recreated target baseline: exists=%t err=%v", exists, err)
		}
		fresh, err := pgx.Connect(ctx, bkpDSNFor(t, f.adminDSN, target.name))
		if err != nil {
			return fmt.Errorf("connect to recreated target baseline: %w", err)
		}
		var probePID int32
		var probeBackendStart time.Time
		if err := fresh.QueryRow(ctx, `SELECT pg_backend_pid(), backend_start FROM pg_stat_activity WHERE pid = pg_backend_pid()`).Scan(&probePID, &probeBackendStart); err != nil {
			_ = fresh.Close(ctx)
			return fmt.Errorf("observe recreated target probe identity: %w", err)
		}
		var userTables int
		err = fresh.QueryRow(ctx, `SELECT count(*) FROM pg_class
WHERE relkind='r' AND relnamespace='public'::regnamespace AND relname NOT LIKE 'pg_%'`).Scan(&userTables)
		closeErr := fresh.Close(ctx)
		if err != nil || closeErr != nil || userTables != 0 {
			return fmt.Errorf("recreated target is not an observed empty baseline: user_tables=%d query_err=%v close_err=%v", userTables, err, closeErr)
		}
		// pgx Close is client-local (Terminate + flush + socket close; no
		// server acknowledgement), so the probe's backend may still be visible
		// in pg_stat_activity after Close returns. Synchronize on the probe's
		// own (pid, backend_start) identity with a bounded wait, then run the
		// authoritative target-wide zero-session census unchanged. This only
		// ever waits for OUR just-closed probe; any other session - unknown
		// writer, old attempt tag, recovery tool - is not absorbed by this
		// wait and still fails the census below.
		if err := bkpAwaitProbeExit(ctx, f.admin, target.name, probePID, probeBackendStart,
			bkpProbeExitTimeout, bkpProbeExitInterval); err != nil {
			return fmt.Errorf("probe session did not leave the recreated target: %w", err)
		}
		var postSessions int
		if err := f.admin.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname=$1`, target.name).Scan(&postSessions); err != nil || postSessions != 0 {
			return fmt.Errorf("recreated target is not empty of sessions: count=%d err=%v\n%s",
				postSessions, err, bkpTargetSessionDump(ctx, f.admin, target.name))
		}
		emptyBaseline = true
		operationID := "bkp-fixture-interrupted-rebuild-" + backupID
		evidence, err := json.Marshal(map[string]any{
			"controlled_test_baseline": true, "fixture": "backup-restore", "observation": "drop-create-empty",
			"target_database": target.name, "target_guard_key": f.restoreTargetGuard,
			"target_role_fingerprint": f.restoreTargetRole, "old_attempt": guard.OperationID,
			"old_attempt_app_name": guard.AttemptAppName, "old_attempt_state": guard.State,
			"executor_restore_started_audit_rows": executorMarkerCount,
		})
		if err != nil {
			return err
		}
		if err := controlstore.RecordTargetGuardRebuild(ctx, tx, f.instanceID, f.restoreTargetGuard, "deploy:executor", operationID, evidence); err != nil {
			return fmt.Errorf("record observed interrupted target rebuild: %w", err)
		}
		return controlstore.ResolveTargetGuardClean(ctx, tx, f.restoreTargetGuard, operationID, evidence)
	}); err != nil {
		t.Fatalf("controlled interrupted target rebuild: %v", err)
	}
	if !emptyBaseline {
		t.Fatal("controlled rebuild did not prove the empty target baseline")
	}
	return bkpDSNFor(t, f.adminDSN, target.name)
}

func mustBkpDSNTarget(t *testing.T, dsn string) controlstore.DSNTarget {
	t.Helper()
	target, err := controlstore.ParseDSNTarget(dsn)
	if err != nil {
		t.Fatalf("parse target DSN: %v", err)
	}
	return target
}

// ---------------------------------------------------------------------------
// Test 5: F3 + target guards - incompatible versions and illegal targets are
// refused with zero silent downgrade/rewrite.
// ---------------------------------------------------------------------------

func TestRestoreTargetGuardsAndIncompatibleManifestRefused(t *testing.T) {
	f := newBkpFixture(t)
	f.seedProbeTable(t)

	backup := f.backup(t, nil)
	m := backup.Manifest
	verifyTarget := f.createDatabase(t, "tgt_verify_guards")
	if got := f.verifyBackup(t, backup.ManifestPath, verifyTarget); got.State != VerificationVerified {
		t.Fatalf("verify state = %q, want verified", got.State)
	}

	// (a) the control-store DSN can never be a restore target.
	controlDBTablesBefore := f.userTableCount(t, f.ctrlDSN)
	refused := f.refusedRestore(t, backup.ManifestPath, f.ctrlDSN)
	if len(refused.Blocked) == 0 {
		t.Fatal("control-store-as-target refusal reported no blocked items")
	}
	if got := f.userTableCount(t, f.ctrlDSN); got != controlDBTablesBefore {
		t.Fatalf("control database table count changed: %d -> %d", controlDBTablesBefore, got)
	}

	// (b) production_main requires an explicit, recorded reason; the default
	// declaration is isolated-only.
	prodTarget := f.createDatabase(t, "tgt_prod_guard")
	result, err := ExecuteRestore(f.ctx, RestoreOptions{
		ManifestPath:      backup.ManifestPath,
		InstanceID:        f.instanceID,
		ControlStore:      f.store,
		ControlDSN:        f.ctrlDSN,
		TargetDSN:         prodTarget,
		ObserverDSN:       prodTarget,
		TargetDeclaration: TargetProductionMain,
		TargetReason:      "",
		Actor:             "deploy:executor",
		ProgramVersion:    bkpProgramVersion,
		PG:                f.pg,
	})
	if err == nil || result.Restored {
		t.Fatal("production_main without an explicit reason was accepted, want refusal")
	}
	if got := f.userTableCount(t, prodTarget); got != 0 {
		t.Fatalf("production guard refusal created %d tables", got)
	}

	// (c) F3 schema mismatch: a manifest recording an unknown migration is
	// refused and never silently downgraded/rewritten.
	schemaVariant := bkpReadManifest(t, backup.ManifestPath)
	schemaVariant.Schema.GooseDBVersion = append(append([]int64(nil), schemaVariant.Schema.GooseDBVersion...), 9999)
	schemaPath := filepath.Join(f.artDir, "schema-mismatch.manifest.json")
	bkpWriteManifest(t, schemaPath, schemaVariant)
	schemaTarget := f.createDatabase(t, "tgt_schema_guard")
	refused = f.refusedRestore(t, schemaPath, schemaTarget)
	if len(refused.Blocked) == 0 {
		t.Fatal("schema mismatch refusal reported no blocked items")
	}
	if got := f.userTableCount(t, schemaTarget); got != 0 {
		t.Fatalf("schema mismatch refusal created %d tables (silent downgrade)", got)
	}

	// (d) F3 program mismatch: the manifest demands a newer minimum program
	// version than the running one; refused, target untouched.
	programVariant := bkpReadManifest(t, backup.ManifestPath)
	programVariant.Program.MinCompatible = "999.0"
	programPath := filepath.Join(f.artDir, "program-mismatch.manifest.json")
	bkpWriteManifest(t, programPath, programVariant)
	programTarget := f.createDatabase(t, "tgt_program_guard")
	refused = f.refusedRestore(t, programPath, programTarget)
	if len(refused.Blocked) == 0 {
		t.Fatal("program mismatch refusal reported no blocked items")
	}
	if got := f.userTableCount(t, programTarget); got != 0 {
		t.Fatalf("program mismatch refusal created %d tables", got)
	}

	// No refusal produced restored evidence or rewrote the verified manifest.
	if got := f.controlEvidenceCount(t, "restore_probe", m.BackupID); got != 0 {
		t.Fatalf("restore_probe evidence rows after refusals = %d, want 0", got)
	}
	if reread := bkpReadManifest(t, backup.ManifestPath); !bkpEqualInt64(reread.Schema.GooseDBVersion, m.Schema.GooseDBVersion) {
		t.Fatalf("verified manifest was rewritten after refusals: %v -> %v", m.Schema.GooseDBVersion, reread.Schema.GooseDBVersion)
	}
}
