//go:build integration

// isolation_integration_test.go is T025 [US2]: the control-store trust
// boundary against a real PostgreSQL 18.6 (testcontainers), with the real
// recovery-admin command surface, the real control store and the real gate
// (tags: integration; FR-009/016/023/026/032; data-model.md §1/§3/§4.4;
// contracts/resumption-gate.md §1–§4; research.md §3/§4; ADR-001; tasks.md
// T025; DG-3/DG-4).
//
// TDD-first. The supported control-store recovery path (stop-isolation +
// explicit rebuild/supersede + audit, F6) requires the B8/T028 isolation
// checklist write path: the rebuild step below attests each isolation item
// through recovery.NewChecklist / Set / Verify. Those symbols do not exist
// yet, so the integration candidate fails to build until B8 lands; the
// expected API surface is exactly the one documented in
// internal/recovery/isolation_integration_test.go (T024). The trust-boundary
// assertions that only need the existing store/gate write paths were verified
// in a scratch run before this file landed (recorded in the batch report);
// once T028 exists, every test below runs as written.
//
// Scope and non-claims (DG-3/DG-4; do not widen in a test comment or an
// assertion):
//
//   - this fixture deliberately hosts the data DB and the control DB in ONE
//     PostgreSQL instance (same storage). That is the normal deployment
//     topology and the *logical* rollback domain. No instance-level
//     independence is provided or claimed: an instance-level loss takes both,
//     and the rule is fail-closed until the control facts are rebuilt. Off-host
//     storage/backup of the artifacts remains a deployment ruling.
//   - blind restore of the control store is NOT supported and there is no
//     automatic detection of a rolled-back control copy (F6/DG-1/DG-4). The
//     tests below pin the supported-tool refusal (restore never targets the
//     control store), the worthlessness of a directly self-written supersede
//     row (no audit, no release path) and the instance binding of approvals
//     (old-instance approvals never apply to a new instance). They do NOT
//     claim a universal "old approvals never re-activate": inside a
//     blind-restored control copy the old facts are self-consistent and
//     undetectable by 015.
//
// Docker discipline: the package TestMain (store_integration_test.go, package
// controlstore) guards the whole test binary: missing Docker reports NOT RUN
// (exit 0) locally and fails under CI=true or TXHARBOR_REQUIRE_DOCKER=1. An
// unrun PG layer is never a pass.
//
// This file is an external test package (controlstore_test) because it imports
// the recovery gate and the recovery-admin command surface; the internal
// controlstore test package cannot import a package that depends on itself.
package controlstore_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os/exec"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/xtianxx/txharbor/internal/app/recoveryadmin"
	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/recovery"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore/schema"
)

const (
	trustPGImage  = "postgres:18.6-trixie"
	trustBaseDB   = "txharbor"
	trustBaseUser = "txharbor"
	trustBasePass = "txharbor"
)

// trustScope returns a canonical capability-scoped expression. Older fixture
// strings omitted the required capability dimension, so gate admission
// refused them as scope_mismatch before these tests reached their trust-boundary
// assertions.
func trustScope(capability recovery.Capability) string {
	return fmt.Sprintf("asset=usdc;capability=%s;chain=31337;kind=withdrawal", capability)
}

type trustFixture struct {
	t     *testing.T
	ctx   context.Context
	ctr   *postgres.PostgresContainer
	ctrID string

	adminDSN string
	admin    *pgxpool.Pool
	seq      int

	dataName string
	dataDSN  string
	dataPool *pgxpool.Pool

	ctrlName string
	ctrlDSN  string
	ctrlPool *pgxpool.Pool
	store    *controlstore.Store
	gate     *recovery.Gate
}

func newTrustFixture(t *testing.T) *trustFixture {
	t.Helper()
	ctx := context.Background()
	ctr, err := postgres.Run(ctx, trustPGImage,
		postgres.WithDatabase(trustBaseDB),
		postgres.WithUsername(trustBaseUser),
		postgres.WithPassword(trustBasePass),
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
	f := &trustFixture{
		t:        t,
		ctx:      ctx,
		ctr:      ctr,
		ctrID:    ctr.GetContainerID(),
		adminDSN: adminDSN,
	}
	f.admin = trustOpenPool(t, adminDSN)

	// Data DB: the real repository migrations (015 adds no data-DB schema).
	f.dataName = "trust_data"
	f.dataDSN = f.createDatabase(t, f.dataName)
	if err := db.MigrateUp(ctx, db.MigrateOptions{
		DSN: f.dataDSN, LockTimeout: 30 * time.Second, ConnectTimeout: 5 * time.Second,
	}, io.Discard); err != nil {
		t.Fatalf("migrate data database: %v", err)
	}
	f.dataPool = trustOpenPool(t, f.dataDSN)

	// Control store: an independent database with its own goose sequence.
	f.ctrlName = "trust_ctrl"
	f.ctrlDSN = f.createDatabase(t, f.ctrlName)
	f.migrateControl(t, f.ctrlDSN)
	f.ctrlPool = trustOpenPool(t, f.ctrlDSN)
	store, err := controlstore.NewStore(ctx, f.ctrlPool)
	if err != nil {
		t.Fatalf("controlstore.NewStore: %v", err)
	}
	f.store = store
	target, err := recovery.GateTargetBindingFromDSN(f.dataDSN)
	if err != nil {
		t.Fatalf("GateTargetBindingFromDSN: %v", err)
	}
	gate, err := recovery.NewGate(store, recovery.GateOptions{TTL: time.Minute, TrustedTarget: target})
	if err != nil {
		t.Fatalf("recovery.NewGate: %v", err)
	}
	f.gate = gate
	return f
}

func trustOpenPool(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()
	pool, err := db.OpenPool(context.Background(), dsn, 10*time.Second)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func (f *trustFixture) migrateControl(t *testing.T, dsn string) {
	t.Helper()
	if err := db.MigrateUp(f.ctx, db.MigrateOptions{
		DSN: dsn, LockTimeout: 10 * time.Second, ConnectTimeout: 5 * time.Second, FS: schema.FS,
	}, io.Discard); err != nil {
		t.Fatalf("migrate control schema: %v", err)
	}
}

func (f *trustFixture) createDatabase(t *testing.T, name string) string {
	t.Helper()
	if _, err := f.admin.Exec(f.ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatalf("create database %s: %v", name, err)
	}
	return trustDSNFor(t, f.adminDSN, name)
}

// dropAndRecreate drops a database WITH (FORCE) and returns its DSN. It models
// a lost/rolled-back database inside the same PostgreSQL instance.
func (f *trustFixture) dropAndRecreate(t *testing.T, name string) string {
	t.Helper()
	if _, err := f.admin.Exec(f.ctx,
		"DROP DATABASE IF EXISTS "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)"); err != nil {
		t.Fatalf("drop database %s: %v", name, err)
	}
	if _, err := f.admin.Exec(f.ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatalf("recreate database %s: %v", name, err)
	}
	return trustDSNFor(t, f.adminDSN, name)
}

func (f *trustFixture) op(prefix string) string {
	f.seq++
	return fmt.Sprintf("trust-%s-%d", prefix, f.seq)
}

// ---------------------------------------------------------------------------
// Real PG client tools inside the pinned container (no host PostgreSQL client)
// ---------------------------------------------------------------------------

func trustContainerDSN(t *testing.T, dsn string) string {
	t.Helper()
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	u.Host = net.JoinHostPort("127.0.0.1", "5432")
	return u.String()
}

func (f *trustFixture) pgTool(t *testing.T, stdin []byte, name string, args ...string) ([]byte, error) {
	t.Helper()
	cmd := exec.CommandContext(f.ctx, "docker", append([]string{"exec", "-i", f.ctrID, name}, args...)...)
	if stdin != nil {
		cmd.Stdin = bytes.NewReader(stdin)
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("%s failed: %w (stderr: %s)", name, err, stderr.String())
	}
	return stdout.Bytes(), nil
}

// dumpDatabase produces a real pg_dump custom-format archive of dsn.
func (f *trustFixture) dumpDatabase(t *testing.T, dsn string) []byte {
	t.Helper()
	out, err := f.pgTool(t, nil, "pg_dump", "--format=custom", trustContainerDSN(t, dsn))
	if err != nil {
		t.Fatalf("pg_dump: %v", err)
	}
	if len(out) == 0 {
		t.Fatal("pg_dump produced an empty archive")
	}
	return out
}

// archiveListing runs pg_restore -l on a custom-format archive (real tool).
func (f *trustFixture) archiveListing(t *testing.T, archive []byte) string {
	t.Helper()
	out, err := f.pgTool(t, archive, "pg_restore", "-l")
	if err != nil {
		t.Fatalf("pg_restore -l: %v", err)
	}
	return string(out)
}

// restoreInto runs a real pg_restore of archive into dsn.
func (f *trustFixture) restoreInto(t *testing.T, archive []byte, dsn string) {
	t.Helper()
	if _, err := f.pgTool(t, archive, "pg_restore",
		"--dbname="+trustContainerDSN(t, dsn), "--no-owner", "--no-privileges", "--exit-on-error"); err != nil {
		t.Fatalf("pg_restore into target: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Control-store fixture helpers (real write paths)
// ---------------------------------------------------------------------------

func (f *trustFixture) openInstance(t *testing.T) string {
	t.Helper()
	target, err := f.targetBinding(t)
	if err != nil {
		t.Fatalf("GateTargetBindingFromDSN: %v", err)
	}
	result, err := f.store.OpenInstance(f.ctx, controlstore.OpenInstanceRequest{
		Kind: "recovery", OpenedBy: "deploy:executor", Reason: "trust-boundary fixture", EntryChainInventory: []uint64{1},
		TargetGuardKey: target.TargetGuardKey, TargetRoleFingerprint: target.TargetRoleFingerprint,
	})
	if err != nil {
		t.Fatalf("open recovery instance: %v", err)
	}
	return result.InstanceID
}

func (f *trustFixture) targetBinding(t *testing.T) (recovery.GateTargetBinding, error) {
	t.Helper()
	return recovery.GateTargetBindingFromDSN(f.dataDSN)
}

func (f *trustFixture) mapIdentity(t *testing.T, principal, person string) {
	t.Helper()
	if _, err := f.store.SetIdentityMapping(f.ctx, controlstore.SetIdentityMappingRequest{
		Principal: principal, PersonID: person, RecordedBy: "deploy:admin", OperationID: f.op("map"),
	}); err != nil {
		t.Fatalf("map identity %s -> %s: %v", principal, person, err)
	}
}

func (f *trustFixture) register(t *testing.T, instanceID, principal, role string) {
	t.Helper()
	if _, err := f.store.RegisterParticipant(f.ctx, controlstore.RegisterParticipantRequest{
		InstanceID: instanceID, Principal: principal, Role: role,
		Actor: "deploy:admin", OperationID: f.op("reg"),
	}); err != nil {
		t.Fatalf("register %s as %s: %v", principal, role, err)
	}
}

// baseFacts opens one recovery instance and binds an executor, verifier and
// approver with distinct people through the real identity/participant paths.
func (f *trustFixture) baseFacts(t *testing.T) string {
	t.Helper()
	instanceID := f.openInstance(t)
	f.mapIdentity(t, "deploy:executor", "person-executor")
	f.mapIdentity(t, "auth:verifier", "person-verifier")
	f.mapIdentity(t, "auth:approver", "person-approver")
	f.register(t, instanceID, "deploy:executor", "executor")
	f.register(t, instanceID, "auth:verifier", "verifier")
	f.register(t, instanceID, "auth:approver", "approver")
	return instanceID
}

func (f *trustFixture) token(t *testing.T, instanceID string) recovery.EvidenceToken {
	t.Helper()
	token, err := recovery.CaptureEvidenceToken(f.ctx, f.ctrlPool, instanceID)
	if err != nil {
		t.Fatalf("capture evidence token: %v", err)
	}
	return token
}

// seedIsolationVerified installs the capability's isolation dependency set as
// verified through the real generation protocol (T013). It is the fixture's
// stand-in for the T028 checklist persistence hook where the test subject is
// the store/gate trust boundary rather than the checklist state machine.
func (f *trustFixture) seedIsolationVerified(t *testing.T, instanceID string, capability recovery.Capability) {
	t.Helper()
	items, err := recovery.IsolationDependencySet(capability)
	if err != nil {
		t.Fatalf("IsolationDependencySet(%s): %v", capability, err)
	}
	token := f.token(t, instanceID)
	outcome, err := recovery.CommitEvidenceWrite(f.ctx, f.store, recovery.EvidenceWriteRequest{
		InstanceID: instanceID,
		Token:      token,
		Kind:       recovery.MutationIsolationVerified,
		Actor:      "auth:verifier",
		Reason:     "fixture: isolation verified through the real generation protocol",
		OperationID: func() string {
			f.seq++
			return fmt.Sprintf("trust-iso-%d", f.seq)
		}(),
		Apply: func(ctx context.Context, tx pgx.Tx, accepted recovery.EvidenceToken) error {
			for _, item := range items {
				if _, err := tx.Exec(ctx, `
INSERT INTO recovery_isolation_check
    (check_id, instance_id, item_key, state, evidence_ref, checkpoint_summary,
     checked_by, checked_at, verified_by, verified_at)
VALUES (gen_random_uuid(), $1, $2, 'verified', $3, '{}'::jsonb, $4, now(), $5, now())
ON CONFLICT (instance_id, item_key) DO UPDATE
   SET state = 'verified', evidence_ref = EXCLUDED.evidence_ref,
       verified_by = EXCLUDED.verified_by, verified_at = now()`,
					accepted.InstanceID, string(item), "evidence://015/trust/"+string(item),
					"deploy:executor", "auth:verifier"); err != nil {
					return err
				}
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("seed verified isolation: %v", err)
	}
	if outcome.Discarded {
		t.Fatal("fixture isolation write was discarded; the token must have been fresh")
	}
}

func (f *trustFixture) approve(t *testing.T, instanceID string, capability recovery.Capability, principal, person string, class recovery.ApprovalClass) string {
	t.Helper()
	token := f.token(t, instanceID)
	result, err := f.store.AppendApprovalDecision(f.ctx, controlstore.ApprovalDecisionRequest{
		InstanceID: instanceID, Capability: string(capability), ScopeHash: trustScope(capability),
		Decision: "approve", ApprovalClassSnapshot: string(class),
		Principal: principal, PersonID: person,
		EvidenceGeneration: token.Generation, EvidenceHash: token.Hash,
		OperationID: f.op("approve"),
	})
	if err != nil {
		t.Fatalf("append approval for %s: %v", capability, err)
	}
	return result.DecisionID
}

func (f *trustFixture) release(t *testing.T, instanceID string, capability recovery.Capability, refs []string) string {
	t.Helper()
	token := f.token(t, instanceID)
	result, err := f.store.AppendReleaseDecision(f.ctx, controlstore.ReleaseDecisionRequest{
		InstanceID: instanceID, Capability: string(capability), ScopeHash: trustScope(capability),
		Decision: "release", ApprovalRefs: refs,
		EvidenceGeneration: token.Generation, EvidenceHash: token.Hash,
		Actor: "deploy:executor", OperationID: f.op("release"),
	})
	if err != nil {
		t.Fatalf("append release for %s: %v", capability, err)
	}
	return result.DecisionID
}

func (f *trustFixture) admit(t *testing.T, gate *recovery.Gate, instanceID string, capability recovery.Capability) recovery.GateDecision {
	t.Helper()
	d, err := gate.Admit(f.ctx, recovery.GateRequest{
		InstanceID: instanceID, Capability: capability, ScopeHash: trustScope(capability),
		Actor: "deploy:executor", OperationID: f.op("admit"),
	})
	if err != nil {
		t.Fatalf("gate.Admit(%s): %v", capability, err)
	}
	return d
}

// releaseQueryFully makes query fully release-valid (isolation verified,
// single non-executor approval, release at the current generation) and
// asserts the positive control, so the tests that follow have a store the
// gate has really observed as allowing.
func (f *trustFixture) releaseQueryFully(t *testing.T, gate *recovery.Gate, instanceID string) {
	t.Helper()
	f.seedTargetGuardClean(t, instanceID)
	f.seedIsolationVerified(t, instanceID, recovery.CapabilityQuery)
	approval := f.approve(t, instanceID, recovery.CapabilityQuery, "auth:approver", "person-approver", recovery.ApprovalClassSingleNonExecutor)
	f.release(t, instanceID, recovery.CapabilityQuery, []string{approval})
	if d := f.admit(t, gate, instanceID, recovery.CapabilityQuery); !d.Allowed {
		t.Fatalf("positive control: query must be released, got %+v", d)
	}
}

// seedTargetGuardClean is a controlled test-fixture baseline: positive gate
// controls need the persisted target guard to be explicitly clean. Instance
// open intentionally creates only unknown inventory, so this fixture records
// synthetic, clearly-labeled test evidence through the supported audit and
// resolution operations rather than weakening gate assertions.
func (f *trustFixture) seedTargetGuardClean(t *testing.T, instanceID string) {
	t.Helper()
	target, err := f.targetBinding(t)
	if err != nil {
		t.Fatalf("GateTargetBindingFromDSN: %v", err)
	}
	guard, found, err := controlstore.ReadTargetGuard(f.ctx, f.ctrlPool, target.TargetGuardKey)
	if err != nil {
		t.Fatalf("read target guard: %v", err)
	}
	if !found {
		t.Fatal("instance open did not create target guard inventory")
	}
	if guard.State == controlstore.TargetGuardClean && !guard.ActiveWriter {
		return
	}
	if guard.State != controlstore.TargetGuardUnknown || guard.ActiveWriter || guard.LaunchIntent {
		t.Fatalf("cannot establish fixture clean target from guard %+v", guard)
	}
	tx, err := f.ctrlPool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(f.ctx) }()
	op := f.op("target-clean")
	evidence := []byte(`{"fixture":"synthetic controlled baseline; test-only"}`)
	if err := controlstore.RecordTargetGuardRebuild(f.ctx, tx, instanceID, target.TargetGuardKey, "deploy:executor", op, evidence); err != nil {
		t.Fatalf("record fixture target baseline: %v", err)
	}
	if err := controlstore.ResolveTargetGuardClean(f.ctx, tx, target.TargetGuardKey, op, evidence); err != nil {
		t.Fatalf("resolve fixture target guard clean: %v", err)
	}
	if err := tx.Commit(f.ctx); err != nil {
		t.Fatalf("commit fixture target baseline: %v", err)
	}
}

func trustDBTableCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, like string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE $1`,
		like).Scan(&n); err != nil {
		t.Fatalf("count tables like %s: %v", like, err)
	}
	return n
}

func trustControlSnapshot(t *testing.T, ctx context.Context, pool *pgxpool.Pool, instanceID string) map[string]int {
	t.Helper()
	snapshot := make(map[string]int, len(controlstore.ControlTableNames())+2)
	for _, table := range controlstore.ControlTableNames() {
		var n int
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+pgx.Identifier{table}.Sanitize()).Scan(&n); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		snapshot[table] = n
	}
	var approvals, releases int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM recovery_approval WHERE instance_id = $1`, instanceID).Scan(&approvals); err != nil {
		t.Fatalf("count approvals: %v", err)
	}
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM recovery_release WHERE instance_id = $1`, instanceID).Scan(&releases); err != nil {
		t.Fatalf("count releases: %v", err)
	}
	snapshot["approvals_for_instance"] = approvals
	snapshot["releases_for_instance"] = releases
	return snapshot
}

func trustCLI(t *testing.T, env map[string]string, args ...string) (int, string, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := recoveryadmin.Run(context.Background(), args, recoveryadmin.Deps{
		Getenv: func(key string) (string, bool) {
			value, ok := env[key]
			return value, ok
		},
		Stdout: &stdout,
		Stderr: &stderr,
	})
	return code, stdout.String(), stderr.String()
}

// trustNoopRestorePG is a PGCommand that fails the test if it is ever invoked.
// It exists to prove that the control-store target guard refuses before any
// restore tooling runs.
type trustNoopRestorePG struct {
	calls int
}

func (p *trustNoopRestorePG) Run(ctx context.Context, name string, args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	p.calls++
	return errors.New("restore tooling must not run for a control-store target")
}

// ---------------------------------------------------------------------------
// 1. Control DSN = data DSN is refused by the real command, with zero writes
// ---------------------------------------------------------------------------

func TestControlTargetIndependenceRefusedByRealCommand(t *testing.T) {
	f := newTrustFixture(t)
	shared := f.createDatabase(t, "trust_shared")

	controlTarget, err := controlstore.ParseDSNTarget(shared)
	if err != nil {
		t.Fatalf("parse shared DSN: %v", err)
	}
	dataTarget, err := controlstore.ParseDSNTarget(f.dataDSN)
	if err != nil {
		t.Fatalf("parse data DSN: %v", err)
	}
	if controlTarget.SameDatabase(dataTarget) {
		t.Fatal("control and data databases must be different targets")
	}
	if !controlTarget.SameDatabase(controlTarget) {
		t.Fatal("SameDatabase must be reflexive")
	}
	sameDBCredentials, err := controlstore.ParseDSNTarget(strings.Replace(shared, "txharbor:txharbor@", "txharbor_other:other@", 1))
	if err != nil {
		t.Fatalf("parse same-db/different-credential DSN: %v", err)
	}
	if !controlTarget.SameDatabase(sameDBCredentials) {
		t.Fatal("SameDatabase must ignore credentials: the database target identity is host/port/database")
	}

	// The real migrate command refuses an equal control/data target before any
	// connection, for both phases.
	for _, phase := range []string{"up", "status"} {
		code, _, stderr := trustCLI(t, map[string]string{
			"TXHARBOR_RECOVERY_CONTROL_DSN": shared,
			"TXHARBOR_PG_DSN":               shared,
		}, "migrate", phase)
		if code != 1 || !strings.Contains(stderr, "same database target") {
			t.Fatalf("migrate %s with equal DSNs = (%d, %q), want exit 1 naming the same-database refusal", phase, code, stderr)
		}
	}
	// Same database, different credentials: still refused (role is not part of
	// the target identity).
	code, _, stderr := trustCLI(t, map[string]string{
		"TXHARBOR_RECOVERY_CONTROL_DSN": shared,
		"TXHARBOR_PG_DSN":               strings.Replace(shared, "txharbor:txharbor@", "txharbor_other:other@", 1),
	}, "migrate", "up")
	if code != 1 || !strings.Contains(stderr, "same database target") {
		t.Fatalf("migrate up with same-db/different-credentials = (%d, %q), want exit 1", code, stderr)
	}
	// Missing data DSN: independence is unprovable, refuse by name.
	code, _, stderr = trustCLI(t, map[string]string{"TXHARBOR_RECOVERY_CONTROL_DSN": shared}, "migrate", "up")
	if code != 1 || !strings.Contains(stderr, "TXHARBOR_PG_DSN") {
		t.Fatalf("migrate up without data DSN = (%d, %q), want exit 1 naming TXHARBOR_PG_DSN", code, stderr)
	}
	// Missing control DSN: refuse by name.
	code, _, stderr = trustCLI(t, map[string]string{"TXHARBOR_PG_DSN": f.dataDSN}, "migrate", "up")
	if code != 1 || !strings.Contains(stderr, "TXHARBOR_RECOVERY_CONTROL_DSN") {
		t.Fatalf("migrate up without control DSN = (%d, %q), want exit 1 naming TXHARBOR_RECOVERY_CONTROL_DSN", code, stderr)
	}

	// Zero writes: the shared database is still pristine — no version table,
	// no recovery objects, no partial wiring (a pristine database reports the
	// pending target version; that is its unmigrated state, not a write).
	pool := trustOpenPool(t, shared)
	state, err := controlstore.InspectSchema(f.ctx, pool)
	if err != nil {
		t.Fatalf("inspect shared database: %v", err)
	}
	if state.VersionTable || len(state.RecoveryObjects) != 0 || len(state.Applied) != 0 {
		t.Fatalf("refused migrate must leave the shared database pristine, got %+v", state)
	}
}

// ---------------------------------------------------------------------------
// 2. The data-DB backup set contains no control-store objects
// ---------------------------------------------------------------------------

func TestDataDBBackupCarriesNoRecoveryObjects(t *testing.T) {
	f := newTrustFixture(t)

	// Fixture sanity: the control store was migrated with all 12 entities, so
	// a "recovery_" match below would be detectable if it ever leaked.
	if got, want := trustDBTableCount(t, f.ctx, f.ctrlPool, "recovery\\_%"), len(controlstore.ControlTableNames()); got != want {
		t.Fatalf("control database has %d recovery_* tables, want %d", got, want)
	}
	// Positive control for the namespace check: the data DB really does carry
	// data-side objects whose names contain "recovery" (000006
	// reorg_recovery*), so a substring scan would be wrong and a
	// recovery_-namespace leak would be detectable.
	if got := trustDBTableCount(t, f.ctx, f.dataPool, "recovery\\_%"); got != 0 {
		t.Fatalf("data database unexpectedly carries %d recovery_* table(s)", got)
	}
	if got := trustDBTableCount(t, f.ctx, f.dataPool, "%recovery%"); got < 2 {
		t.Fatalf("expected the data-side recovery-related tables (000006), found %d", got)
	}

	archive := f.dumpDatabase(t, f.dataDSN)
	listing := f.archiveListing(t, archive)
	if !strings.Contains(listing, "TABLE") {
		t.Fatalf("pg_restore -l output does not look like a real data-DB archive:\n%s", listing)
	}
	if !strings.Contains(listing, "reorg_recovery") {
		t.Fatalf("the archive listing must show the data-side reorg_recovery objects (positive control):\n%s", listing)
	}
	// The control namespace is the recovery_* prefix; data-side names such as
	// reorg_recovery_events must not trip this check.
	controlObject := regexp.MustCompile(`(^|[^0-9A-Za-z_])recovery_[a-z]`)
	if match := controlObject.FindString(listing); match != "" {
		t.Fatalf("the data-DB backup archive lists a control-store object (%q); the control store must never enter the data backup set:\n%s", match, listing)
	}
	// The dump is restorable into a fresh database (the listing is not a
	// truncation artefact).
	targetDSN := f.createDatabase(t, "trust_listing_target")
	f.restoreInto(t, archive, targetDSN)
	restoredPool := trustOpenPool(t, targetDSN)
	if got := trustDBTableCount(t, f.ctx, restoredPool, "recovery\\_%"); got != 0 {
		t.Fatalf("a restored data-DB copy carries %d recovery_* table(s)", got)
	}
}

// ---------------------------------------------------------------------------
// 3. Restoring the data DB leaves every control fact untouched
// ---------------------------------------------------------------------------

func TestDataDBRestoreLeavesControlFactsUntouched(t *testing.T) {
	f := newTrustFixture(t)
	instanceID := f.baseFacts(t)
	f.seedIsolationVerified(t, instanceID, recovery.CapabilityQuery)
	approval := f.approve(t, instanceID, recovery.CapabilityQuery, "auth:approver", "person-approver", recovery.ApprovalClassSingleNonExecutor)

	before := trustControlSnapshot(t, f.ctx, f.ctrlPool, instanceID)
	beforeToken := f.token(t, instanceID)

	// A data-side marker so the restore is proven to have really happened.
	if _, err := f.dataPool.Exec(f.ctx,
		`CREATE TABLE trust_restore_marker (id bigserial PRIMARY KEY, note text NOT NULL)`); err != nil {
		t.Fatalf("create marker table: %v", err)
	}
	if _, err := f.dataPool.Exec(f.ctx, `INSERT INTO trust_restore_marker (note) VALUES ('before-restore')`); err != nil {
		t.Fatalf("seed marker row: %v", err)
	}

	archive := f.dumpDatabase(t, f.dataDSN)
	f.dropAndRecreate(t, f.dataName)
	f.dataPool.Close()
	restoredDSN := trustDSNFor(t, f.adminDSN, f.dataName)
	f.restoreInto(t, archive, restoredDSN)
	f.dataPool = trustOpenPool(t, restoredDSN)

	var markers int
	if err := f.dataPool.QueryRow(f.ctx, `SELECT count(*) FROM trust_restore_marker`).Scan(&markers); err != nil {
		t.Fatalf("marker count after restore: %v", err)
	}
	if markers != 1 {
		t.Fatalf("data restore did not produce the marker row (got %d)", markers)
	}

	// The control store is a different database: the data restore cannot have
	// touched it. Re-read through the same store paths.
	after := trustControlSnapshot(t, f.ctx, f.ctrlPool, instanceID)
	if len(before) != len(after) {
		t.Fatalf("control snapshot shape changed: %v -> %v", before, after)
	}
	for key, want := range before {
		if got := after[key]; got != want {
			t.Fatalf("control fact %s changed across the data restore: %d -> %d", key, want, got)
		}
	}
	afterToken := f.token(t, instanceID)
	if afterToken.Generation != beforeToken.Generation || afterToken.Hash != beforeToken.Hash || afterToken.State != "open" {
		t.Fatalf("instance token changed across the data restore: %+v -> %+v", beforeToken, afterToken)
	}
	// The approval row is still there and still bound to this instance; it is
	// never read from the data DB (research §3).
	decision, err := f.store.CurrentApprovalDecision(f.ctx, nil, controlstore.DecisionKey{
		InstanceID: instanceID, Capability: string(recovery.CapabilityQuery), ScopeHash: trustScope(recovery.CapabilityQuery),
	}, "auth:approver")
	if err != nil {
		t.Fatalf("read approval decision: %v", err)
	}
	if !decision.Found || decision.DecisionID != approval || decision.Decision != "approve" {
		t.Fatalf("approval decision after data restore = %+v, want the recorded approve %s", decision, approval)
	}
}

// ---------------------------------------------------------------------------
// 4. An unreachable/lost control store refuses the gate (fail-closed)
// ---------------------------------------------------------------------------

func TestControlStoreLossRefusesGate(t *testing.T) {
	f := newTrustFixture(t)
	instanceID := f.baseFacts(t)

	// Warm the gate's derivation cache with a real admission first.
	d := f.admit(t, f.gate, instanceID, recovery.CapabilityQuery)
	if d.Allowed || d.RefusalClass != recovery.RefusalIsolationUnproven {
		t.Fatalf("baseline admission = %+v, want isolation_unproven", d)
	}

	// (a) Unreachable: the control pool is closed; no cache may allow.
	lostGate := func() *recovery.Gate {
		dsn := f.createDatabase(t, "trust_ctrl_unreachable")
		f.migrateControl(t, dsn)
		pool := trustOpenPool(t, dsn)
		store, err := controlstore.NewStore(f.ctx, pool)
		if err != nil {
			t.Fatalf("NewStore over reachable control database: %v", err)
		}
		target, err := recovery.GateTargetBindingFromDSN(f.dataDSN)
		if err != nil {
			t.Fatalf("GateTargetBindingFromDSN: %v", err)
		}
		gate, err := recovery.NewGate(store, recovery.GateOptions{TTL: time.Minute, TrustedTarget: target})
		if err != nil {
			t.Fatalf("NewGate: %v", err)
		}
		pool.Close()
		return gate
	}()
	d, err := lostGate.Admit(f.ctx, recovery.GateRequest{
		InstanceID: instanceID, Capability: recovery.CapabilityNewWithdrawalCreation, ScopeHash: trustScope(recovery.CapabilityNewWithdrawalCreation),
		Actor: "deploy:executor", OperationID: f.op("admit-unreachable"),
	})
	if err == nil || !errors.Is(err, recovery.ErrGateControlStoreUnavailable) {
		t.Fatalf("closed-pool admission error = %v, want ErrGateControlStoreUnavailable", err)
	}
	if d.Allowed || d.RefusalClass != recovery.RefusalControlStoreUnavailable {
		t.Fatalf("closed-pool admission = %+v, want a control_store_unavailable refusal", d)
	}

	// (b) Lost: the control database is dropped while the store/gate still
	// hold their pool. Every subsequent admission refuses.
	f.dropAndRecreate(t, f.ctrlName)
	d, err = f.gate.Admit(f.ctx, recovery.GateRequest{
		InstanceID: instanceID, Capability: recovery.CapabilityNewWithdrawalCreation, ScopeHash: trustScope(recovery.CapabilityNewWithdrawalCreation),
		Actor: "deploy:executor", OperationID: f.op("admit-lost"),
	})
	if err == nil || !errors.Is(err, recovery.ErrGateControlStoreUnavailable) {
		t.Fatalf("lost-control admission error = %v, want ErrGateControlStoreUnavailable", err)
	}
	if d.Allowed || d.RefusalClass != recovery.RefusalControlStoreUnavailable {
		t.Fatalf("lost-control admission = %+v, want a control_store_unavailable refusal", d)
	}
	// A dropped control store can never be re-opened into a store.
	rebuiltDSN := trustDSNFor(t, f.adminDSN, f.ctrlName)
	rebuiltPool := trustOpenPool(t, rebuiltDSN)
	if store, err := controlstore.NewStore(f.ctx, rebuiltPool); err == nil || store != nil {
		t.Fatal("NewStore over a dropped/empty control database must refuse")
	}
}

// ---------------------------------------------------------------------------
// 5. Same-instance disaster (DG-3): only rebuilt control facts allow release
// ---------------------------------------------------------------------------

func TestSameInstanceDisasterRequiresControlRebuild(t *testing.T) {
	// Deliberate topology (DG-3): one PostgreSQL instance hosts the data DB
	// and the control DB. This is the logical rollback domain; no
	// instance-level independence exists and none is claimed. The test only
	// requires the artifacts + control rebuild discipline to be scoped to a
	// single storage instance.
	f := newTrustFixture(t)
	firstInstance := f.baseFacts(t)
	f.releaseQueryFully(t, f.gate, firstInstance)

	// Disaster: the single PG instance's control database is lost (same
	// storage as the data DB; both are gone as far as the rollback domain is
	// concerned).
	f.dropAndRecreate(t, f.ctrlName)
	if d, err := f.gate.Admit(f.ctx, recovery.GateRequest{
		InstanceID: firstInstance, Capability: recovery.CapabilityQuery, ScopeHash: trustScope(recovery.CapabilityQuery),
		OperationID: f.op("post-disaster"),
	}); err == nil || !errors.Is(err, recovery.ErrGateControlStoreUnavailable) || d.Allowed {
		t.Fatalf("post-disaster admission = (%+v, %v), want a fail-closed control_store_unavailable refusal", d, err)
	}

	// Supported rebuild: a NEW control database (new name/binding), explicit
	// instance open (audited) and the supported isolation path — every
	// dependency item is attested by the executor and verified by a
	// non-executor (T028/T029; the checklist write path is B8's job and does
	// not exist yet, so this file fails to build until it lands).
	rebuiltDSN := trustDSNFor(t, f.adminDSN, f.ctrlName)
	f.migrateControl(t, rebuiltDSN)
	rebuiltPool := trustOpenPool(t, rebuiltDSN)
	rebuiltStore, err := controlstore.NewStore(f.ctx, rebuiltPool)
	if err != nil {
		t.Fatalf("NewStore over the rebuilt control database: %v", err)
	}
	target, err := recovery.GateTargetBindingFromDSN(f.dataDSN)
	if err != nil {
		t.Fatalf("GateTargetBindingFromDSN: %v", err)
	}
	rebuiltGate, err := recovery.NewGate(rebuiltStore, recovery.GateOptions{TTL: time.Minute, TrustedTarget: target})
	if err != nil {
		t.Fatalf("NewGate over the rebuilt store: %v", err)
	}
	rebuilt := &trustFixture{
		t: f.t, ctx: f.ctx, ctr: f.ctr, ctrID: f.ctrID, adminDSN: f.adminDSN, admin: f.admin,
		dataName: f.dataName, dataDSN: f.dataDSN, dataPool: f.dataPool,
		ctrlName: f.ctrlName, ctrlDSN: rebuiltDSN, ctrlPool: rebuiltPool,
		store: rebuiltStore, gate: rebuiltGate,
	}
	newInstance := rebuilt.baseFacts(t)

	checklist, err := recovery.NewChecklist(rebuiltStore)
	if err != nil {
		t.Fatalf("recovery.NewChecklist over the rebuilt store: %v", err)
	}
	items, err := recovery.IsolationDependencySet(recovery.CapabilityQuery)
	if err != nil {
		t.Fatalf("IsolationDependencySet(query): %v", err)
	}
	for _, item := range items {
		if _, err := checklist.Set(f.ctx, recovery.ChecklistEvidenceRequest{
			InstanceID:        newInstance,
			ItemKey:           item,
			State:             recovery.ChecklistStateEvidenced,
			EvidenceRef:       "evidence://015/trust-rebuild/" + string(item),
			CheckpointSummary: []byte(`{"step":"stop-isolation executed"}`),
			Actor:             "deploy:executor",
			OperationID:       rebuilt.op("rebuild-set"),
		}); err != nil {
			t.Fatalf("checklist.Set(%s) on the rebuilt instance: %v", item, err)
		}
		if _, err := checklist.Verify(f.ctx, recovery.ChecklistVerifyRequest{
			InstanceID: newInstance, ItemKey: item, Actor: "auth:verifier",
			OperationID: rebuilt.op("rebuild-verify"),
		}); err != nil {
			t.Fatalf("checklist.Verify(%s) on the rebuilt instance: %v", item, err)
		}
	}
	// The rebuilt data target also requires an explicit controlled clean
	// transition; opening provisioned an unknown target-guard row only.
	rebuilt.seedTargetGuardClean(t, newInstance)
	approval := rebuilt.approve(t, newInstance, recovery.CapabilityQuery, "auth:approver", "person-approver", recovery.ApprovalClassSingleNonExecutor)
	rebuilt.release(t, newInstance, recovery.CapabilityQuery, []string{approval})
	if d := rebuilt.admit(t, rebuiltGate, newInstance, recovery.CapabilityQuery); !d.Allowed {
		t.Fatalf("rebuilt control facts must allow the capability after the explicit rebuild, got %+v", d)
	}

	// The rebuild is explicit and audited: the instance open wrote an ok audit
	// row under the control store's own action name.
	var openAudits int
	if err := rebuiltPool.QueryRow(f.ctx,
		`SELECT count(*) FROM recovery_audit WHERE instance_id = $1 AND action = $2 AND result = 'ok'`,
		newInstance, controlstore.ActionInstanceOpen).Scan(&openAudits); err != nil {
		t.Fatalf("read rebuild audit: %v", err)
	}
	if openAudits == 0 {
		t.Fatal("the rebuilt instance open must be audited (explicit rebuild + audit)")
	}
	// The old instance's facts are gone with the old store; only the new
	// instance is open (INV-1), and nothing was inherited (no instance-level
	// independence is invented).
	var openRows int
	if err := rebuiltPool.QueryRow(f.ctx,
		`SELECT count(*) FROM recovery_instance WHERE state = 'open'`).Scan(&openRows); err != nil {
		t.Fatalf("count open instances: %v", err)
	}
	if openRows != 1 {
		t.Fatalf("rebuilt control store has %d open instances, want exactly 1", openRows)
	}
	oldReleases, err := rebuiltStore.CurrentReleaseDecision(f.ctx, nil, controlstore.DecisionKey{
		InstanceID: firstInstance, Capability: string(recovery.CapabilityQuery), ScopeHash: trustScope(recovery.CapabilityQuery),
	})
	if err != nil {
		t.Fatalf("read old release from rebuilt store: %v", err)
	}
	if oldReleases.Found {
		t.Fatal("the rebuilt control store must not inherit the old instance's release")
	}
}

// ---------------------------------------------------------------------------
// 6. F6 control-rollback negative (blind restore unsupported; self-written
//    supersede is not a proof; old approvals are instance-bound)
// ---------------------------------------------------------------------------

func TestControlRollbackNegativeF6(t *testing.T) {
	f := newTrustFixture(t)
	instanceA := f.baseFacts(t)
	f.releaseQueryFully(t, f.gate, instanceA)
	approvalA := func() string {
		decision, err := f.store.CurrentApprovalDecision(f.ctx, nil, controlstore.DecisionKey{
			InstanceID: instanceA, Capability: string(recovery.CapabilityQuery), ScopeHash: trustScope(recovery.CapabilityQuery),
		}, "auth:approver")
		if err != nil || !decision.Found {
			t.Fatalf("approval A lookup = (%+v, %v), want found", decision, err)
		}
		return decision.DecisionID
	}()

	// (a) Blind restore of the control store is not a supported path: the
	// restore executor refuses the control database as a target before any
	// tooling runs, even with a missing manifest.
	archive := f.dumpDatabase(t, f.ctrlDSN)
	f.dropAndRecreate(t, f.ctrlName)
	restoredDSN := trustDSNFor(t, f.adminDSN, f.ctrlName)
	f.restoreInto(t, archive, restoredDSN)
	restoredPool := trustOpenPool(t, restoredDSN)
	restoredStore, err := controlstore.NewStore(f.ctx, restoredPool)
	if err != nil {
		t.Fatalf("NewStore over the blind-restored control database: %v", err)
	}
	target, err := recovery.GateTargetBindingFromDSN(f.dataDSN)
	if err != nil {
		t.Fatalf("GateTargetBindingFromDSN: %v", err)
	}
	restoredGate, err := recovery.NewGate(restoredStore, recovery.GateOptions{TTL: time.Minute, TrustedTarget: target})
	if err != nil {
		t.Fatalf("NewGate over the blind-restored store: %v", err)
	}
	pg := &trustNoopRestorePG{}
	result, err := recovery.ExecuteRestore(f.ctx, recovery.RestoreOptions{
		ManifestPath:   "does-not-exist-manifest.json",
		InstanceID:     instanceA,
		ControlStore:   restoredStore,
		ControlDSN:     restoredDSN,
		TargetDSN:      restoredDSN, // the control store itself
		ObserverDSN:    restoredDSN,
		Actor:          "deploy:executor",
		ProgramVersion: "018.0",
		PG:             pg,
	})
	if err == nil || result.Restored {
		t.Fatalf("restore targeting the control store = (%+v, %v), want a refusal", result, err)
	}
	if len(result.Blocked) == 0 || !strings.Contains(strings.Join(result.Blocked, "; "), "control-store database") {
		t.Fatalf("refusal must name the control-store target guard, got %v", result.Blocked)
	}
	if pg.calls != 0 {
		t.Fatalf("restore tooling ran %d time(s) for a control-store target; it must be refused first", pg.calls)
	}

	// (b) A directly self-written supersede row is not a rebuild proof. The
	// rolled-back control store still holds instance A (open); an operator
	// writes the closure and the successor row directly in SQL (no audit, no
	// decision rows). It must grant nothing and count as nothing.
	var auditRowsBefore int
	if err := restoredPool.QueryRow(f.ctx, `SELECT count(*) FROM recovery_audit`).Scan(&auditRowsBefore); err != nil {
		t.Fatalf("count audit rows: %v", err)
	}
	if _, err := restoredPool.Exec(f.ctx,
		`UPDATE recovery_instance SET state = 'closed', closed_by = 'deploy:operator', closed_at = now()
		  WHERE instance_id = $1`, instanceA); err != nil {
		t.Fatalf("self-written close: %v", err)
	}
	instanceB := "22222222-3333-4444-8555-666666666666"
	target, err = f.targetBinding(t)
	if err != nil {
		t.Fatalf("GateTargetBindingFromDSN: %v", err)
	}
	if _, err := restoredPool.Exec(f.ctx,
		`INSERT INTO recovery_instance
		     (instance_id, kind, state, supersedes_instance_id, evidence_generation, evidence_hash, opened_by, reason, target_guard_key, target_role_fingerprint)
		 VALUES ($1, 'recovery', 'open', $2, 0, $3, 'deploy:operator', 'self-written supersede row (unsupported)', $4, $5)`,
		instanceB, instanceA, controlstore.EmptyEvidenceHash, target.TargetGuardKey, target.TargetRoleFingerprint); err != nil {
		t.Fatalf("self-written supersede insert: %v", err)
	}
	var auditRowsAfter int
	if err := restoredPool.QueryRow(f.ctx, `SELECT count(*) FROM recovery_audit`).Scan(&auditRowsAfter); err != nil {
		t.Fatalf("count audit rows after: %v", err)
	}
	if auditRowsAfter != auditRowsBefore {
		t.Fatalf("a self-written supersede/close bypassed the audited path (audit rows %d -> %d); it must leave no audit row and no proof",
			auditRowsBefore, auditRowsAfter)
	}
	// The successor has no release/approval/isolation facts; the self-written
	// row cannot manufacture them.
	var releasesForB, approvalsForB, isolationForB int
	if err := restoredPool.QueryRow(f.ctx, `SELECT count(*) FROM recovery_release WHERE instance_id = $1`, instanceB).Scan(&releasesForB); err != nil {
		t.Fatalf("count releases for B: %v", err)
	}
	if err := restoredPool.QueryRow(f.ctx, `SELECT count(*) FROM recovery_approval WHERE instance_id = $1`, instanceB).Scan(&approvalsForB); err != nil {
		t.Fatalf("count approvals for B: %v", err)
	}
	if err := restoredPool.QueryRow(f.ctx, `SELECT count(*) FROM recovery_isolation_check WHERE instance_id = $1`, instanceB).Scan(&isolationForB); err != nil {
		t.Fatalf("count isolation rows for B: %v", err)
	}
	if releasesForB != 0 || approvalsForB != 0 || isolationForB != 0 {
		t.Fatalf("self-written supersede produced decision facts for B (releases=%d approvals=%d isolation=%d)", releasesForB, approvalsForB, isolationForB)
	}

	// Old approvals are instance-bound (inert for the new instance): the
	// recorded approval of A is not the current decision of B...
	decisionB, err := restoredStore.CurrentApprovalDecision(f.ctx, nil, controlstore.DecisionKey{
		InstanceID: instanceB, Capability: string(recovery.CapabilityQuery), ScopeHash: trustScope(recovery.CapabilityQuery),
	}, "auth:approver")
	if err != nil {
		t.Fatalf("read B approval decision: %v", err)
	}
	if decisionB.Found {
		t.Fatalf("A's approval must not appear as B's decision: %+v", decisionB)
	}
	// ...and even a release forged for B that references A's approval is
	// refused by the gate: the approval belongs to another instance.
	// Seed B's isolation through the real generation protocol so the refusal
	// reaches the approval binding instead of stopping at isolation.
	f2 := &trustFixture{t: f.t, ctx: f.ctx, adminDSN: f.adminDSN, admin: f.admin,
		ctrlPool: restoredPool, store: restoredStore, gate: restoredGate, seq: f.seq}
	f2.mapIdentity(t, "deploy:executor", "person-executor")
	f2.mapIdentity(t, "auth:verifier", "person-verifier")
	f2.mapIdentity(t, "auth:approver", "person-approver")
	f2.register(t, instanceB, "auth:approver", "approver")
	f2.seedIsolationVerified(t, instanceB, recovery.CapabilityQuery)
	releaseB, err := restoredStore.AppendReleaseDecision(f.ctx, controlstore.ReleaseDecisionRequest{
		InstanceID: instanceB, Capability: string(recovery.CapabilityQuery), ScopeHash: trustScope(recovery.CapabilityQuery),
		Decision: "release", ApprovalRefs: []string{approvalA},
		EvidenceGeneration: f2.token(t, instanceB).Generation, EvidenceHash: f2.token(t, instanceB).Hash,
		Actor: "deploy:executor", OperationID: f2.op("forged-release"),
	})
	if err != nil {
		t.Fatalf("append release referencing the old instance's approval: %v", err)
	}
	d, err := restoredGate.Admit(f.ctx, recovery.GateRequest{
		InstanceID: instanceB, Capability: recovery.CapabilityQuery, ScopeHash: trustScope(recovery.CapabilityQuery),
		Actor: "deploy:executor", OperationID: f2.op("forged-admit"),
	})
	if err != nil {
		t.Fatalf("gate admission for the forged release: %v", err)
	}
	if d.Allowed || d.RefusalClass != recovery.RefusalScopeMismatch {
		t.Fatalf("forged release referencing A's approval = %+v (release %s), want scope_mismatch", d, releaseB.DecisionID)
	}

	// Non-claim (DG-4): this test does not and cannot assert that a blind
	// control rollback is detected — 015 has no such detection, and inside the
	// rolled-back copy the old facts stay self-consistent for the old instance
	// id. What is pinned: no supported blind-restore path, no proof from a
	// self-written supersede row, and no cross-instance approval reuse.
}

// trustDSNFor rewrites a DSN to address another database on the same server.
func trustDSNFor(t *testing.T, baseDSN, name string) string {
	t.Helper()
	u, err := url.Parse(baseDSN)
	if err != nil {
		t.Fatalf("parse base dsn: %v", err)
	}
	u.Path = "/" + name
	return u.String()
}
