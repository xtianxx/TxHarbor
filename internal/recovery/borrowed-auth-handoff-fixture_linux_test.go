//go:build linux && drill

// borrowed-auth-handoff-fixture_linux_test.go is the FRESH restricted-writer
// auth-handoff fixture. It establishes a real W (writer) login with restricted
// role flags (including NOINHERIT) and no privileged memberships, a W-owned
// target/source database, a protected-but-distinct observer and control role,
// a public-locked control database, a genuine gate endpoint and direct
// container-IP routes BEFORE any original DSN/key/fingerprint/lock/factory
// work, and an authentic borrowed coordinator run whose writer role is W
// (unlike the older gate fixture, which used the admin DSN). AF01: every
// credential (rotated administrator, W, protected observer, protected control)
// is an independent crypto/rand value, and the gate's retained observer
// authenticates as the fresh protected observer role. AF02: the control
// database denial is proven as a real SQLSTATE 42501 permission refusal while
// the same W credential completes a genuine target authentication. It is
// read-only with respect to existing fixtures and creates no acceptance/receipt
// authority.
package recovery_test

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/recovery"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore/schema"
)

type borrowedAuthRetainedConn struct {
	Role            string
	BackendPID      int
	ApplicationName string
	Database        string
	BackendType     string
	UsesysID        uint32
}

type borrowedAuthHandoffFixture struct {
	fx   *originGateFixture
	gate *originGate

	run    *recovery.DrillBorrowedWriterRun
	prefix *borrowedIdentityPrefix
	lock   *recovery.TargetLock
	tools  recovery.DrillNativePGTools

	writerRole     string
	writerPassword string
	observerRole   string
	observerPass   string
	controlRole    string
	controlPass    string
	adminRole      string

	// adminDSN/adminPassword are the fresh private administrator credential
	// created by this fixture (diagnostic fields; never printed).
	adminDSN      string
	adminPassword string
	nano          int64

	writerTargetDSN   string
	observerTargetDSN string
	writerSourceDSN   string
	controlDSN        string

	writerRoleOID   uint32
	observerRoleOID uint32
	controlRoleOID  uint32
	adminRoleOID    uint32

	targetDB       string
	sourceDB       string
	controlDB      string
	targetDBOID    uint32
	table          string
	operation      string
	guardKey       string
	pristineDigest string
	instanceID     string
	bound          bool

	controlPool *pgxpool.Pool
	archive     *os.File

	probeCalls      int32
	acceptanceCalls int32

	// authEntrySlot is the fixture-owned one-time shared entry slot (type and
	// bind-once semantics in borrowed-receipt-auth-entry_linux_test.go). The
	// original fixture retains the pointer and shallow copies share it, so a
	// genuine auth-entry loss permanently binds the original fixture/run-bound
	// capability instead of a per-constructor copy.
	authEntrySlot *borrowedReceiptAuthEntrySlot
}

// borrowedAuthInheritedAdminPassword is the fixed public credential of the
// shared container's bootstrap administrator. It is never used for a fixture
// role; it exists only to prove the rotation invalidated it.
const borrowedAuthInheritedAdminPassword = "txharbor"

// borrowedAuthCredential returns a fresh unpredictable credential: exactly 32
// bytes from crypto/rand (one separate random read per credential; any read
// failure refuses the fixture) encoded as URL-safe base64. It derives nothing
// from role names, purposes, timestamps or templates.
func borrowedAuthCredential(t *testing.T, purpose string) string {
	t.Helper()
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		t.Fatalf("crypto/rand credential read (%s) refused: %v", purpose, err)
	}
	credential := base64.RawURLEncoding.EncodeToString(raw[:])
	if len(credential) != 43 {
		t.Fatalf("credential for %s has unexpected encoding length", purpose)
	}
	return credential
}

// borrowedAuthSQLState extracts the SQLSTATE of a real server-side failure
// without ever rendering credential material or the connection string.
func borrowedAuthSQLState(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return "no-sqlstate"
}

// assertAuthRejected proves a real authentication attempt with a candidate
// credential completed server-side and was rejected with the expected
// SQLSTATE. The candidate credential is never printed; only the role OID and
// the safe SQLSTATE appear in diagnostics.
func (f *borrowedAuthHandoffFixture) assertAuthRejected(t *testing.T, ctx context.Context, dsn, role, wantCode, what string) {
	t.Helper()
	conn, err := pgx.Connect(ctx, dsn)
	if err == nil {
		_ = conn.Close(ctx)
		t.Fatalf("%s was accepted (role OID %d)", what, borrowedAuthRoleOID(t, f.fx.admin, role))
	}
	if code := borrowedAuthSQLState(err); code != wantCode {
		t.Fatalf("%s was refused without SQLSTATE %s (sqlstate=%s, role OID %d)", what, wantCode, code, borrowedAuthRoleOID(t, f.fx.admin, role))
	}
}

func borrowedAuthRoleDSN(t *testing.T, baseDSN, role, password string) string {
	t.Helper()
	parsed, err := url.Parse(baseDSN)
	if err != nil {
		t.Fatal("parse base DSN refused")
	}
	parsed.User = url.UserPassword(role, password)
	return parsed.String()
}

func borrowedAuthRoleOID(t *testing.T, admin *pgx.Conn, role string) uint32 {
	t.Helper()
	var oid uint32
	if err := admin.QueryRow(context.Background(), `SELECT oid FROM pg_roles WHERE rolname=$1`, role).Scan(&oid); err != nil {
		t.Fatalf("read role OID for %s: %v", role, err)
	}
	return oid
}

func newBorrowedAuthHandoffFixture(t *testing.T) *borrowedAuthHandoffFixture {
	t.Helper()
	// The default constructor is the ONLY unbound entrypoint: it routes
	// directly through the shared setup and never through the bound contract.
	return newBorrowedAuthHandoffFixtureShared(t, nil, nil)
}

// borrowedAuthHandoffBoundOptions parameterizes the BOUND fixture construction
// variant. The default constructor stays unbound.
type borrowedAuthHandoffBoundOptions struct {
	Kind                string
	OpenedBy            string
	Reason              string
	EntryChainInventory []uint64
	// GuardInitMode selects the REAL guard row state the constructor leaves for
	// the real exact validator: "" (fixture clean init), "absent" (no row) or
	// "unknown" (real unknown inventory row). No synthetic error hook is used.
	GuardInitMode string
	// InjectBootstrapReleaseError injects a drill-only release uncertainty AFTER
	// the real bounded release ran; it is labelled injected and is never
	// credited as an observed PG uncertainty.
	InjectBootstrapReleaseError error
}

// borrowedAuthHandoffBoundResult captures bound-stage construction failures so
// an error-returning variant can refuse without aborting the process.
type borrowedAuthHandoffBoundResult struct {
	err error
}

// newBorrowedAuthHandoffFixtureBoundResult is the error-returning bound
// constructor: real absent/unknown guard rows are refused by the REAL
// validator, a failed/uncertain bootstrap release returns nil/error BEFORE the
// retained owner acquisition and factory publication, and every failure keeps
// the bounded cleanup mandatory. The fatal public constructors stay unchanged.
func newBorrowedAuthHandoffFixtureBoundResult(t *testing.T, bound *borrowedAuthHandoffBoundOptions) (*borrowedAuthHandoffFixture, error) {
	t.Helper()
	if bound == nil {
		return nil, errors.New("bound auth fixture requires explicit bound options")
	}
	{
		if strings.TrimSpace(bound.Kind) == "" || strings.TrimSpace(bound.OpenedBy) == "" || strings.TrimSpace(bound.Reason) == "" {
			return nil, errors.New("bound auth fixture requires kind, opened_by and reason")
		}
		for index, id := range bound.EntryChainInventory {
			if id == 0 || (index > 0 && bound.EntryChainInventory[index-1] >= id) {
				return nil, errors.New("bound auth fixture entry chain inventory must be positive and strictly ascending")
			}
		}
		switch bound.GuardInitMode {
		case "", "absent", "unknown":
		default:
			return nil, fmt.Errorf("bound auth fixture guard init mode %q is unsupported", bound.GuardInitMode)
		}
	}
	result := &borrowedAuthHandoffBoundResult{}
	fixture := newBorrowedAuthHandoffFixtureShared(t, bound, result)
	if result.err != nil {
		return nil, result.err
	}
	return fixture, nil
}

// newBorrowedAuthHandoffFixtureBound constructs a genuinely instance-bound
// fixture through the authorized bound-only ordering: bounded BOOTSTRAP target
// lock first -> pristine proof -> insert-only fixture guard init -> exact
// clean/provenance validation -> authentic Store.OpenInstance -> CONFIRMED
// bounded bootstrap release -> retained owner acquisition -> the same factory
// call with the authentic instance id and the transactional restore_started
// marker. It refuses on any failure and never opens over a later dirty guard.
func newBorrowedAuthHandoffFixtureBound(t *testing.T, bound *borrowedAuthHandoffBoundOptions) *borrowedAuthHandoffFixture {
	t.Helper()
	fixture, err := newBorrowedAuthHandoffFixtureBoundResult(t, bound)
	if err != nil {
		t.Fatalf("bound auth fixture construction refused: %v", err)
	}
	return fixture
}

func newBorrowedAuthHandoffFixtureWithBound(t *testing.T, bound *borrowedAuthHandoffBoundOptions) *borrowedAuthHandoffFixture {
	t.Helper()
	fixture, err := newBorrowedAuthHandoffFixtureBoundResult(t, bound)
	if err != nil {
		t.Fatalf("bound auth fixture construction refused: %v", err)
	}
	return fixture
}

func newBorrowedAuthHandoffFixtureShared(t *testing.T, bound *borrowedAuthHandoffBoundOptions, result *borrowedAuthHandoffBoundResult) *borrowedAuthHandoffFixture {
	t.Helper()
	ctx := context.Background()
	fx := newOriginGateFixture(t)
	nano := time.Now().UnixNano()
	baseDB := databaseOf(t, fx.dsn)
	adminRole := "txharbor"

	// AF01: rotate the inherited fixture administrator verifier FIRST, to a
	// fresh crypto/rand credential generated before the W role exists, before
	// any gate observer session and before any original writer DSN/key/lock/
	// factory. Only this fresh owned fixture is touched; the retained fx.admin
	// connection remains the original authenticated administrator session.
	adminPassword := borrowedAuthCredential(t, "administrator")
	if _, err := fx.admin.Exec(ctx, `ALTER ROLE `+pgx.Identifier{adminRole}.Sanitize()+` PASSWORD `+sqlLiteral(adminPassword)); err != nil {
		t.Fatalf("rotate inherited administrator verifier: %v", err)
	}
	// The inherited fixed administrator credential must no longer authenticate
	// BEFORE W is created or any original binding exists.
	oldAdminConn, err := pgx.Connect(ctx, fx.dsn)
	if err == nil {
		_ = oldAdminConn.Close(ctx)
		t.Fatal("the inherited fixed administrator credential still authenticates after rotation")
	}
	if code := borrowedAuthSQLState(err); code != "28P01" {
		t.Fatalf("old inherited administrator credential was not a real 28P01 rejection (sqlstate=%s)", code)
	}
	// Repoint only this fresh fixture's base DSN at the new private
	// administrator credential; no shared/global fixture behavior changes.
	fx.dsn = fx.dsnAs(baseDB, adminRole, adminPassword)

	// Distinct protected observer and control roles, each with its own
	// independent crypto/rand credential (privileged fixture-only identities,
	// never W, never logged or passed to the native writer run).
	observerRole := fmt.Sprintf("borrowed_auth_obs_%d", nano)
	observerPass := borrowedAuthCredential(t, "protected observer")
	controlRole := fmt.Sprintf("borrowed_auth_ctl_%d", nano)
	controlPass := borrowedAuthCredential(t, "protected control")
	for _, spec := range []struct{ role, password string }{{observerRole, observerPass}, {controlRole, controlPass}} {
		if _, err := fx.admin.Exec(ctx, `CREATE ROLE `+pgx.Identifier{spec.role}.Sanitize()+
			` LOGIN SUPERUSER PASSWORD `+sqlLiteral(spec.password)); err != nil {
			t.Fatalf("create protected fixture role %s: %v", spec.role, err)
		}
		role := spec.role
		t.Cleanup(func() {
			_, _ = fx.admin.Exec(context.Background(), `DROP ROLE IF EXISTS `+pgx.Identifier{role}.Sanitize())
		})
	}

	// The gate observer (and the gate's independent control session) must
	// authenticate as the fresh protected observer role; fx.dsn is set to that
	// DSN BEFORE the genuine gate endpoint is created, and the endpoint stays
	// BEFORE any original writer DSN, key, fingerprint or lock.
	fx.dsn = fx.dsnAs(baseDB, observerRole, observerPass)
	gate := newOriginGate(t, ctx, fx)
	strictHelper := buildBorrowedIdentityStrictHelper(t)
	if err := fx.container.CopyFileToContainer(ctx, strictHelper, borrowedIdentityStrictHelperPath, 0o700); err != nil {
		t.Fatalf("copy strict namespace helper into the auth fixture: %v", err)
	}

	// Real restricted W login with its own independent crypto/rand credential:
	// LOGIN, NOINHERIT, NOSUPERUSER, NOCREATEDB, NOCREATEROLE, NOREPLICATION,
	// NOBYPASSRLS, and no membership in any role.
	writerRole := fmt.Sprintf("borrowed_auth_w_%d", nano)
	writerPassword := borrowedAuthCredential(t, "writer")
	createWriter := `CREATE ROLE ` + pgx.Identifier{writerRole}.Sanitize() +
		` LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD ` + sqlLiteral(writerPassword)
	if _, err := fx.admin.Exec(ctx, createWriter); err != nil {
		t.Fatalf("create restricted writer W: %v", err)
	}
	t.Cleanup(func() {
		_, _ = fx.admin.Exec(context.Background(), `DROP ROLE IF EXISTS `+pgx.Identifier{writerRole}.Sanitize())
	})

	sourceDB := fmt.Sprintf("borrowed_auth_source_%d", nano)
	targetDB := fmt.Sprintf("borrowed_auth_target_%d", nano)
	controlDB := fmt.Sprintf("borrowed_auth_ctrl_%d", nano)
	for _, spec := range []struct{ name, owner string }{
		{sourceDB, writerRole}, {targetDB, writerRole}, {controlDB, adminRole},
	} {
		if _, err := fx.admin.Exec(ctx, `CREATE DATABASE `+pgx.Identifier{spec.name}.Sanitize()+
			` OWNER `+pgx.Identifier{spec.owner}.Sanitize()); err != nil {
			t.Fatalf("create %s database owned by %s: %v", spec.name, spec.owner, err)
		}
	}

	// Direct container-IP routes for every identity, derived after the roles
	// and databases but before the original key/lock/factory. The migration
	// connection is explicitly the administrator with the NEW private
	// credential, never the observer credential carried by fx.dsn.
	controlAdminBase := borrowedIdentityRoute(t, ctx, fx, controlDB)
	controlAdminDSN := borrowedAuthRoleDSN(t, controlAdminBase, adminRole, adminPassword)
	if err := db.MigrateUp(ctx, db.MigrateOptions{
		DSN: controlAdminDSN, LockTimeout: 10 * time.Second, ConnectTimeout: 5 * time.Second, FS: schema.FS,
	}, io.Discard); err != nil {
		t.Fatalf("migrate auth control database: %v", err)
	}
	// Control database public lock-down: only the authorized control role may
	// connect/create; a real W attempt must fail with a permission denial.
	for _, statement := range []string{
		`REVOKE ALL ON DATABASE ` + pgx.Identifier{controlDB}.Sanitize() + ` FROM PUBLIC`,
		`GRANT CONNECT ON DATABASE ` + pgx.Identifier{controlDB}.Sanitize() + ` TO ` + pgx.Identifier{controlRole}.Sanitize(),
		`REVOKE CREATE, TEMPORARY ON DATABASE ` + pgx.Identifier{controlDB}.Sanitize() + ` FROM PUBLIC`,
	} {
		if _, err := fx.admin.Exec(ctx, statement); err != nil {
			t.Fatalf("control database privilege lock-down: %v", err)
		}
	}
	controlDSN := borrowedAuthRoleDSN(t, controlAdminBase, controlRole, controlPass)
	controlPool, err := pgxpool.New(ctx, controlDSN)
	if err != nil {
		t.Fatalf("open control-role pool: %v", err)
	}
	t.Cleanup(controlPool.Close)
	store, err := controlstore.NewStore(ctx, controlPool)
	if err != nil {
		t.Fatalf("control-role store: %v", err)
	}

	writerTargetBase := borrowedIdentityRoute(t, ctx, fx, targetDB)
	writerTargetDSN := borrowedAuthRoleDSN(t, writerTargetBase, writerRole, writerPassword)
	observerTargetDSN := borrowedAuthRoleDSN(t, writerTargetBase, observerRole, observerPass)
	writerSourceDSN := borrowedAuthRoleDSN(t, borrowedIdentityRoute(t, ctx, fx, sourceDB), writerRole, writerPassword)

	// Real W-owned source table and three rows.
	sourceAdmin, err := pgx.Connect(ctx, writerSourceDSN)
	if err != nil {
		t.Fatalf("connect W-owned source: %v", err)
	}
	table := fmt.Sprintf("borrowed_auth_rows_%d", nano)
	if _, err := sourceAdmin.Exec(ctx, `CREATE TABLE public.`+pgx.Identifier{table}.Sanitize()+` (ref text NOT NULL)`); err != nil {
		_ = sourceAdmin.Close(ctx)
		t.Fatalf("create W-owned source table: %v", err)
	}
	for row := 1; row <= 3; row++ {
		if _, err := sourceAdmin.Exec(ctx, `INSERT INTO public.`+pgx.Identifier{table}.Sanitize()+` (ref) VALUES ($1)`, fmt.Sprintf("auth-row-%d", row)); err != nil {
			_ = sourceAdmin.Close(ctx)
			t.Fatalf("seed W-owned source row: %v", err)
		}
	}
	if err := sourceAdmin.Close(ctx); err != nil {
		t.Fatalf("close source connection: %v", err)
	}

	// Real custom archive dumped with W login (native pg_restore will also run
	// as W; --no-owner --no-privileges avoids ROLE switches). The W DSN in the
	// dump argv is the existing accepted fixture-tool mechanics; no protected
	// credential appears in any CLI argument.
	tools, err := recovery.DrillProvisionNativePGTools(t)
	if err != nil {
		t.Fatalf("provision sealed tools: %v", err)
	}
	dumpPath, ok := tools.DumpPath()
	if !ok {
		t.Fatal("sealed dump tool is unavailable")
	}
	archivePath := filepath.Join(t.TempDir(), "borrowed-auth-source.dump")
	dump := exec.CommandContext(ctx, dumpPath, "--format=custom", "--no-owner", "--no-privileges", "--file="+archivePath, writerSourceDSN)
	if err := dump.Run(); err != nil {
		t.Fatalf("real W custom source dump failed: %v", err)
	}
	archive, err := os.Open(archivePath)
	if err != nil {
		t.Fatalf("open real W custom archive: %v", err)
	}
	t.Cleanup(func() { _ = archive.Close() })

	// Original W target identity: whole-catalog pristine proof BEFORE guard INIT.
	targetTarget, err := controlstore.ParseDSNTarget(writerTargetDSN)
	if err != nil {
		t.Fatalf("parse W target identity: %v", err)
	}
	targetKey, err := recovery.CanonicalTargetKey(targetTarget)
	if err != nil {
		t.Fatalf("W target key: %v", err)
	}
	var bootstrapLock *recovery.TargetLock
	if bound != nil {
		// Bounded BOOTSTRAP target lock FIRST: a distinct session owner holds
		// the target serialization the authentic instance open requires. It is
		// released (confirmed) before the retained lineage owner lock.
		bootstrapLock = acquireBorrowedAuthFixtureBootstrapLock(t, ctx, controlDSN, targetKey)
	}
	targetAdmin, err := pgx.Connect(ctx, writerTargetDSN)
	if err != nil {
		t.Fatalf("connect W-owned pristine target: %v", err)
	}
	// P2-A2: the pristine-check connection is disposed boundedly on EVERY
	// construction return path; the successful path keeps its explicit close
	// (the idempotent cleanup is a no-op then).
	t.Cleanup(func() {
		closeCtx, cancelClose := context.WithTimeout(context.Background(), borrowedAuthFixtureBootstrapBudget)
		_ = targetAdmin.Close(closeCtx)
		cancelClose()
	})
	pristineDigest, err := verifyBorrowedGatePristineCatalog(ctx, targetAdmin, targetDB, writerRole)
	if err != nil {
		t.Fatalf("W-owned target is not pristine: %v", err)
	}

	boundFail := func(err error) *borrowedAuthHandoffFixture {
		if result != nil {
			result.err = err
			return nil
		}
		t.Fatal(err)
		return nil
	}
	operation := fmt.Sprintf("borrowed-auth-%d", nano)
	switch {
	case bound != nil && bound.GuardInitMode == "absent":
		// REAL absent inventory: no guard row is created and the real exact
		// validator below must refuse.
	case bound != nil && bound.GuardInitMode == "unknown":
		if err := insertBorrowedFixtureUnknownGuard(ctx, controlPool, targetKey.String(), operation); err != nil {
			t.Fatalf("auth fixture unknown guard inventory init refused: %v", err)
		}
	default:
		if err := initBorrowedGateGuard(ctx, controlPool, targetKey.String(),
			targetTarget.DataTargetFingerprint().RoleFingerprint, "fixture-admin:"+adminRole, operation, pristineDigest); err != nil {
			t.Fatalf("auth fixture guard INSERT-only init refused: %v", err)
		}
	}
	var instanceID string
	if bound != nil {
		// EXPLICIT exact clean/provenance validation of the pre-existing guard
		// through the REAL validator: absent/unknown/dirty/active/inconsistent
		// rows are refused, never normalized.
		if validateErr := validateBorrowedAuthFixtureGuardExactResult(ctx, controlPool, targetKey.String(),
			targetTarget.DataTargetFingerprint().RoleFingerprint, operation); validateErr != nil {
			return boundFail(validateErr)
		}
		// Authentic instance open under the bootstrap lock, while the guard is
		// exactly clean; a failure refuses construction.
		opened, openErr := store.OpenInstance(ctx, controlstore.OpenInstanceRequest{
			Kind:                  bound.Kind,
			OpenedBy:              bound.OpenedBy,
			Reason:                bound.Reason,
			EntryChainInventory:   bound.EntryChainInventory,
			TargetGuardKey:        targetKey.String(),
			TargetRoleFingerprint: targetTarget.DataTargetFingerprint().RoleFingerprint,
		})
		if openErr != nil {
			return boundFail(fmt.Errorf("bound fixture authentic instance open refused: %w", openErr))
		}
		instanceID = opened.InstanceID
		// CONFIRMED bounded bootstrap release BEFORE the retained owner
		// acquisition and factory publication; an injected drill-only
		// uncertainty (labelled) is never credited as an observed PG failure.
		releaseErr := releaseBorrowedAuthFixtureBootstrapLock(ctx, bootstrapLock)
		if releaseErr == nil && bound.InjectBootstrapReleaseError != nil {
			releaseErr = fmt.Errorf("drill-injected bootstrap release uncertainty (not an observed PG failure): %w", bound.InjectBootstrapReleaseError)
		}
		if releaseErr != nil {
			return boundFail(fmt.Errorf("bound fixture bootstrap target lock release was not confirmed: %w", releaseErr))
		}
	}

	// Retained real borrowed lock on the ORIGINAL W target key with the control
	// role's dedicated session as ControlDSN.
	lock, err := recovery.AcquireTargetLock(ctx, controlDSN, targetKey, 3*time.Second, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("acquire auth fixture control-role lock: %v", err)
	}
	t.Cleanup(func() { _ = lock.Release(context.Background()) })

	fixture := &borrowedAuthHandoffFixture{
		fx: fx, gate: gate, lock: lock, tools: tools,
		instanceID: instanceID, bound: bound != nil,
		writerRole: writerRole, writerPassword: writerPassword,
		observerRole: observerRole, observerPass: observerPass,
		controlRole: controlRole, controlPass: controlPass, adminRole: adminRole,
		adminDSN: controlAdminDSN, adminPassword: adminPassword, nano: nano,
		writerTargetDSN: writerTargetDSN, observerTargetDSN: observerTargetDSN,
		writerSourceDSN: writerSourceDSN, controlDSN: controlDSN,
		targetDB: targetDB, sourceDB: sourceDB, controlDB: controlDB, table: table, operation: operation,
		guardKey: targetKey.String(), pristineDigest: pristineDigest,
		controlPool: controlPool, archive: archive,
		authEntrySlot: &borrowedReceiptAuthEntrySlot{},
	}
	writerOptions := recovery.TargetWriterOptions{
		OperationKind: recovery.TargetWriterOperationRestore,
		Store:         store, ControlDSN: controlDSN, TargetDSN: writerTargetDSN,
		ObserverDSN: observerTargetDSN, TrustedTarget: targetTarget,
		OperationID: operation, Archive: archive,
		Probe: func(context.Context, recovery.TargetWriterProof) (recovery.TargetWriterProbeResult, error) {
			atomic.AddInt32(&fixture.probeCalls, 1)
			return recovery.TargetWriterProbeResult{}, fmt.Errorf("borrowed auth fixture probe intentionally rejects")
		},
		Acceptance: func(context.Context, pgx.Tx, recovery.TargetWriterProof) (recovery.TargetWriterAcceptance, error) {
			atomic.AddInt32(&fixture.acceptanceCalls, 1)
			return recovery.TargetWriterAcceptance{}, fmt.Errorf("borrowed auth fixture acceptance must never run")
		},
		QuiescenceTimeout: 5 * time.Second, QuiescenceInterval: 50 * time.Millisecond,
	}
	if bound != nil {
		writerOptions.InstanceID = instanceID
		writerOptions.Prelaunch = borrowedAuthFixtureRestoreStartedMarker(operation, adminRole)
	}
	run, err := recovery.DrillNewBorrowedWriterRun(ctx, writerOptions, lock, gate.endpoint, tools)
	if err != nil {
		t.Fatalf("auth fixture borrowed run factory: %v", err)
	}
	fixture.run = run
	setup := newBorrowedIdentityCandidateSetup(t, fx, writerTargetDSN)
	prefix, err := setup.capture(ctx, run)
	if err != nil {
		t.Fatalf("auth fixture identity prefix capture: %v", err)
	}
	fixture.prefix = prefix
	if err := gate.UseBorrowedOrigin(run, prefix); err != nil {
		t.Fatalf("register auth fixture borrowed origin: %v", err)
	}

	fixture.writerRoleOID = borrowedAuthRoleOID(t, fx.admin, writerRole)
	fixture.observerRoleOID = borrowedAuthRoleOID(t, fx.admin, observerRole)
	fixture.controlRoleOID = borrowedAuthRoleOID(t, fx.admin, controlRole)
	fixture.adminRoleOID = borrowedAuthRoleOID(t, fx.admin, adminRole)
	var targetDBOID uint32
	if err := fx.admin.QueryRow(ctx, `SELECT oid FROM pg_database WHERE datname=$1`, targetDB).Scan(&targetDBOID); err != nil {
		t.Fatalf("auth fixture target database OID: %v", err)
	}
	fixture.targetDBOID = targetDBOID
	// The W-owned target connection is not needed after the pristine proof and
	// archive setup; close it so the retained-connection inventory stays exact.
	_ = targetAdmin.Close(context.Background())
	return fixture
}

// borrowedAuthFixtureBootstrapBudget bounds the bootstrap lock acquisition,
// release and the exact guard validation.
const borrowedAuthFixtureBootstrapBudget = 10 * time.Second

// acquireBorrowedAuthFixtureBootstrapLock holds the bounded bootstrap target
// lock required by the authentic instance open; an idempotent cleanup release
// is registered immediately so a fatal can never strand it.
func acquireBorrowedAuthFixtureBootstrapLock(t *testing.T, ctx context.Context, controlDSN string, key recovery.TargetKey) *recovery.TargetLock {
	t.Helper()
	acquireCtx, cancelAcquire := context.WithTimeout(ctx, borrowedAuthFixtureBootstrapBudget)
	lock, err := recovery.AcquireTargetLock(acquireCtx, controlDSN, key, 3*time.Second, 10*time.Millisecond)
	cancelAcquire()
	if err != nil {
		t.Fatalf("bound fixture bootstrap target lock refused: %v", err)
	}
	t.Cleanup(func() {
		releaseCtx, cancelRelease := context.WithTimeout(context.Background(), borrowedAuthFixtureBootstrapBudget)
		_ = lock.Release(releaseCtx)
		cancelRelease()
	})
	return lock
}

// releaseBorrowedAuthFixtureBootstrapLock performs the CONFIRMED bounded
// bootstrap release; any error or uncertainty is returned.
func releaseBorrowedAuthFixtureBootstrapLock(ctx context.Context, lock *recovery.TargetLock) error {
	if lock == nil {
		return errors.New("bound fixture has no bootstrap target lock")
	}
	releaseCtx, cancelRelease := context.WithTimeout(ctx, borrowedAuthFixtureBootstrapBudget)
	defer cancelRelease()
	return lock.Release(releaseCtx)
}

// validateBorrowedAuthFixtureGuardExactResult requires the EXACT pre-existing
// fixture-initialized clean guard matching key, role provenance and operation
// through the REAL validator; absent/unknown/dirty/active/inconsistent rows
// refuse with an error and are never normalized.
func validateBorrowedAuthFixtureGuardExactResult(ctx context.Context, pool *pgxpool.Pool, key, roleFingerprint, operation string) error {
	validateCtx, cancelValidate := context.WithTimeout(ctx, borrowedAuthFixtureBootstrapBudget)
	defer cancelValidate()
	var exact bool
	err := pool.QueryRow(validateCtx, `
SELECT (disposition = 'clean' AND NOT active_writer AND NOT launch_intent
        AND operation_id = $2
        AND COALESCE(rebuild_evidence->>'bound_target_key','') = $1
        AND COALESCE(rebuild_evidence->>'role_fingerprint','') = $3)
FROM recovery_target_guard WHERE target_guard_key = $1`, key, operation, roleFingerprint).Scan(&exact)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("bound fixture guard row is absent; refusing construction (no inventory is created): %w", err)
	}
	if err != nil {
		return fmt.Errorf("bound fixture exact guard validation refused (unreadable row): %w", err)
	}
	if !exact {
		return errors.New("bound fixture requires the exact pre-existing fixture-initialized clean guard matching key/role/provenance")
	}
	return nil
}

// insertBorrowedFixtureUnknownGuard creates a REAL unknown inventory row for
// the real exact validator to refuse.
func insertBorrowedFixtureUnknownGuard(ctx context.Context, pool *pgxpool.Pool, key, operation string) error {
	insertCtx, cancelInsert := context.WithTimeout(ctx, borrowedAuthFixtureBootstrapBudget)
	defer cancelInsert()
	_, err := pool.Exec(insertCtx, `
INSERT INTO recovery_target_guard (target_guard_key, disposition, operation_id)
VALUES ($1, 'unknown', $2)`, key, operation)
	return err
}

// borrowedAuthFixtureRestoreStartedMarker is the genuine transactional
// restore_started marker path of the bound fixture: it writes the marker inside
// the passed prepare transaction, rejects a discarded/stale outcome and returns
// the ACTUAL accepted protocol token.
func borrowedAuthFixtureRestoreStartedMarker(operation, actorRole string) func(context.Context, pgx.Tx, controlstore.InstanceToken) (recovery.EvidenceToken, error) {
	return func(markerCtx context.Context, tx pgx.Tx, locked controlstore.InstanceToken) (recovery.EvidenceToken, error) {
		write, err := recovery.CommitEvidenceWriteTx(markerCtx, tx, recovery.EvidenceWriteRequest{
			InstanceID:  locked.InstanceID,
			Token:       recovery.LockedEvidenceToken(locked),
			Kind:        recovery.MutationRestoreStarted,
			Actor:       "fixture-admin:" + actorRole,
			OperationID: operation,
			Apply:       func(context.Context, pgx.Tx, recovery.EvidenceToken) error { return nil },
		})
		if err != nil {
			return recovery.EvidenceToken{}, fmt.Errorf("bound fixture restore_started marker write: %w", err)
		}
		if write.Discarded {
			return recovery.EvidenceToken{}, errors.New("bound fixture restore_started marker was discarded as stale")
		}
		return write.Token, nil
	}
}

// LiveConnections is the retained-connection inventory (safe metadata only):
// every backend in the cluster is represented, including background and
// unassigned sessions whose role/database are NULL (they are reported as
// "(unassigned)" rather than dropped by a role allowlist). It manufactures no
// admission or registration facts and treats no idle boolean as evidence; any
// future admission must strictly SQL/OS/socket correlate the actual retained
// clients itself.
func (f *borrowedAuthHandoffFixture) LiveConnections(t *testing.T) []borrowedAuthRetainedConn {
	t.Helper()
	rows, err := f.fx.admin.Query(context.Background(), `
SELECT coalesce(r.rolname, '(unassigned)'), a.pid::int, coalesce(a.application_name, ''),
       coalesce(a.datname, '(unassigned)'), coalesce(a.backend_type, ''), coalesce(a.usesysid, 0)::oid
FROM pg_stat_activity a LEFT JOIN pg_roles r ON r.oid = a.usesysid
ORDER BY a.pid`)
	if err != nil {
		t.Fatalf("retained connection inventory: %v", err)
	}
	defer rows.Close()
	var out []borrowedAuthRetainedConn
	for rows.Next() {
		var conn borrowedAuthRetainedConn
		if err := rows.Scan(&conn.Role, &conn.BackendPID, &conn.ApplicationName, &conn.Database, &conn.BackendType, &conn.UsesysID); err != nil {
			t.Fatalf("scan retained connection inventory: %v", err)
		}
		out = append(out, conn)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate retained connection inventory: %v", err)
	}
	return out
}

// runProtectedAuthCheck runs the owned static auth helper inside the protected
// container and performs the FULL SCRAM-SHA-256 exchange for the supplied real
// credential on the requested transport ("loopback" or "unix"), printing the
// public AUTHCHECK outcome line. Reaching AuthenticationSASLContinue is not
// login proof; only the helper's completed exchange is. The credential is
// supplied privately on stdin and is never echoed by the helper.
func (f *borrowedAuthHandoffFixture) runProtectedAuthCheck(t *testing.T, transport, role, password string) (string, error) {
	t.Helper()
	cmd := exec.Command("docker", "exec", "-i", f.fx.containerID,
		"/tmp/drill-auth-boundary-client", "authcheck", transport, role)
	cmd.Stdin = strings.NewReader(password + "\n")
	output, err := cmd.CombinedOutput()
	return string(output), err
}
