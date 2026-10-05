//go:build linux && drill

// borrowed-transport-phase1_linux_test.go proves the phase-1 borrowed-writer
// transport bridge: SQL-runtime capture of the original cluster/database/role
// and control-lock-owner identities (no caller scalars), distinct transport vs
// original target keys, opaque one-use handle semantics, and the private hook
// validating the EXACT canonical coordinator invocation plus sealed tool and
// listener capabilities before Start.
package recovery

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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

const borrowedTransportPGImage = "postgres@sha256:4ef4dbc939d61acea57712655ddb4b4ab27419c913f94cca0cd57cb3ea3c2280"

type borrowedTransportPG struct {
	ctx       context.Context
	adminDSN  string
	admin     *pgxpool.Pool
	ctrlDSN   string
	ctrl      *pgxpool.Pool
	targetDSN string
	store     *controlstore.Store
	target    controlstore.DSNTarget
}

func borrowedTransportDatabaseDSN(t *testing.T, baseDSN, name string) string {
	t.Helper()
	parsed, err := url.Parse(baseDSN)
	if err != nil {
		t.Fatalf("parse fixture DSN: %v", err)
	}
	parsed.Path = "/" + name
	return parsed.String()
}

func borrowedTransportObserverDSN(t *testing.T, adminDSN, targetDSN string) string {
	t.Helper()
	adminURL, err := url.Parse(adminDSN)
	if err != nil {
		t.Fatalf("parse observer base DSN: %v", err)
	}
	targetURL, err := url.Parse(targetDSN)
	if err != nil {
		t.Fatalf("parse observer target DSN: %v", err)
	}
	adminURL.Path = targetURL.Path
	query := adminURL.Query()
	query.Set("application_name", "borrowed_transport_observer")
	adminURL.RawQuery = query.Encode()
	return adminURL.String()
}

// borrowedTransportObserverDSNForDatabase builds an observer DSN for one
// explicit database (same host/port/role) so a wrong-database observer can be
// exercised without touching the production code path.
func borrowedTransportObserverDSNForDatabase(t *testing.T, baseDSN, database string) string {
	t.Helper()
	parsed, err := url.Parse(baseDSN)
	if err != nil {
		t.Fatalf("parse observer base DSN: %v", err)
	}
	parsed.Path = "/" + database
	query := parsed.Query()
	query.Set("application_name", "borrowed_transport_observer")
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

// borrowedTransportObserverDSNWithRole builds a privileged observer DSN whose
// role is deliberately different from the original writer role.
func borrowedTransportObserverDSNWithRole(t *testing.T, baseDSN, database, role, password string) string {
	t.Helper()
	parsed, err := url.Parse(baseDSN)
	if err != nil {
		t.Fatalf("parse observer base DSN: %v", err)
	}
	parsed.User = url.UserPassword(role, password)
	parsed.Path = "/" + database
	query := parsed.Query()
	query.Set("application_name", "borrowed_transport_observer")
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

// unsetBorrowedTransportPGEnvironment removes inherited PG* variables for the
// duration of the test (restored at cleanup). The supervised child refuses an
// ambient PostgreSQL environment by design, so a real native launch requires a
// clean parent environment rather than a silently stripped one.
func unsetBorrowedTransportPGEnvironment(t *testing.T) {
	t.Helper()
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if len(key) < 2 || !strings.EqualFold(key[:2], "PG") {
			continue
		}
		value, hadValue := os.LookupEnv(key)
		if err := os.Unsetenv(key); err != nil {
			t.Fatalf("could not unset inherited PostgreSQL environment key")
		}
		if hadValue {
			restoreKey, restoreValue := key, value
			t.Cleanup(func() { _ = os.Setenv(restoreKey, restoreValue) })
		}
	}
}

func newBorrowedTransportPG(t *testing.T) *borrowedTransportPG {
	t.Helper()
	ctx := context.Background()
	ctr, err := postgres.Run(ctx, borrowedTransportPGImage,
		postgres.WithDatabase("txharbor"),
		postgres.WithUsername("txharbor"),
		postgres.WithPassword("txharbor"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2)))
	if err != nil {
		t.Fatalf("start pinned postgres container: %v", err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })
	adminDSN, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("container connection string: %v", err)
	}
	e := &borrowedTransportPG{ctx: ctx, adminDSN: adminDSN}
	e.admin = openBorrowedTransportPool(t, e.ctx, adminDSN)
	for _, name := range []string{"ctrl", "target"} {
		if _, err := e.admin.Exec(ctx, `CREATE DATABASE `+pgx.Identifier{name}.Sanitize()); err != nil {
			t.Fatalf("create %s database: %v", name, err)
		}
	}
	e.ctrlDSN = borrowedTransportDatabaseDSN(t, adminDSN, "ctrl")
	if err := db.MigrateUp(ctx, db.MigrateOptions{
		DSN: e.ctrlDSN, LockTimeout: 10 * time.Second, ConnectTimeout: 5 * time.Second, FS: schema.FS,
	}, io.Discard); err != nil {
		t.Fatalf("migrate control database: %v", err)
	}
	e.ctrl = openBorrowedTransportPool(t, e.ctx, e.ctrlDSN)
	store, err := controlstore.NewStore(ctx, e.ctrl)
	if err != nil {
		t.Fatalf("controlstore.NewStore: %v", err)
	}
	e.store = store
	e.targetDSN = borrowedTransportDatabaseDSN(t, adminDSN, "target")
	target, err := controlstore.ParseDSNTarget(e.targetDSN)
	if err != nil {
		t.Fatalf("parse fixture target: %v", err)
	}
	e.target = target
	return e
}

func openBorrowedTransportPool(t *testing.T, ctx context.Context, dsn string) *pgxpool.Pool {
	t.Helper()
	pool, err := db.OpenPool(ctx, dsn, 5*time.Second)
	if err != nil {
		t.Fatalf("open fixture pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// borrowedTransportSecondCluster starts a real second PostgreSQL container
// (same pinned image, same-looking database and role names) so a cross-cluster
// control-owner/observer pair can be exercised against a genuinely different
// system_identifier and postmaster incarnation.
func borrowedTransportSecondCluster(t *testing.T) (admin *pgxpool.Pool, targetDSN string, target controlstore.DSNTarget) {
	t.Helper()
	ctx := context.Background()
	ctr, err := postgres.Run(ctx, borrowedTransportPGImage,
		postgres.WithDatabase("txharbor"),
		postgres.WithUsername("txharbor"),
		postgres.WithPassword("txharbor"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2)))
	if err != nil {
		t.Fatalf("start second pinned postgres container: %v", err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })
	adminDSN, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("second container connection string: %v", err)
	}
	admin = openBorrowedTransportPool(t, ctx, adminDSN)
	if _, err := admin.Exec(ctx, `CREATE DATABASE `+pgx.Identifier{"target"}.Sanitize()); err != nil {
		t.Fatalf("create second cluster target database: %v", err)
	}
	targetDSN = borrowedTransportDatabaseDSN(t, adminDSN, "target")
	target, err = controlstore.ParseDSNTarget(targetDSN)
	if err != nil {
		t.Fatalf("parse second cluster target: %v", err)
	}
	return admin, targetDSN, target
}

// borrowedTransportAuditCount returns the control store's audit row count so a
// refusal can prove it wrote no audit/launch facts.
func borrowedTransportAuditCount(t *testing.T, e *borrowedTransportPG) int64 {
	t.Helper()
	var count int64
	if err := e.ctrl.QueryRow(e.ctx, `SELECT count(*) FROM recovery_audit`).Scan(&count); err != nil {
		t.Fatalf("count recovery_audit: %v", err)
	}
	return count
}

func TestBorrowedTransportPhase1CaptureAndRefusals(t *testing.T) {
	e := newBorrowedTransportPG(t)
	tools, err := DrillProvisionNativePGTools(t)
	if err != nil {
		t.Fatalf("provision sealed tools: %v", err)
	}
	endpoint, err := DrillOpenOriginEndpoint()
	if err != nil {
		t.Fatalf("open factory endpoint: %v", err)
	}
	originalKey, err := CanonicalTargetKey(e.target)
	if err != nil {
		t.Fatalf("original target key: %v", err)
	}
	controlTarget, err := controlstore.ParseDSNTarget(e.ctrlDSN)
	if err != nil {
		t.Fatalf("parse control DSN: %v", err)
	}
	controlKey, err := CanonicalTargetKey(controlTarget)
	if err != nil {
		t.Fatalf("control key: %v", err)
	}
	lock, err := AcquireTargetLock(e.ctx, e.ctrlDSN, originalKey, 3*time.Second, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("acquire real borrowed lock: %v", err)
	}
	defer lock.Release(context.Background())
	opts := TargetWriterOptions{
		OperationKind: TargetWriterOperationRestore,
		Store:         e.store, ControlDSN: e.ctrlDSN, TargetDSN: e.targetDSN,
		ObserverDSN: borrowedTransportObserverDSN(t, e.adminDSN, e.targetDSN), TrustedTarget: e.target,
		OperationID: "borrow-phase1-1",
		Archive:     strings.NewReader("archive"),
		Probe: func(context.Context, TargetWriterProof) (TargetWriterProbeResult, error) {
			return TargetWriterProbeResult{}, errors.New("phase 1 probe intentionally refuses")
		},
		Acceptance: func(context.Context, pgx.Tx, TargetWriterProof) (TargetWriterAcceptance, error) {
			return TargetWriterAcceptance{}, errors.New("phase 1 acceptance intentionally refuses")
		},
	}
	run, err := DrillNewBorrowedWriterRun(e.ctx, opts, lock, endpoint, tools)
	if err != nil {
		t.Fatalf("borrowed writer run factory: %v", err)
	}
	binding := run.Binding()
	if binding.ClusterSystemIdentifier() == "" || binding.PostmasterStartTime().IsZero() {
		t.Fatalf("target cluster SQL identity missing: %+v", binding)
	}
	if binding.TargetDatabaseOID() == 0 || binding.WriterRoleOID() == 0 {
		t.Fatalf("target database/role OID missing: %+v", binding)
	}
	if binding.ControlBackendPID() <= 0 || binding.ControlBackendStart().IsZero() || binding.ControlDatabaseOID() == 0 {
		t.Fatalf("control lock owner identity missing: %+v", binding)
	}
	if binding.ControlPostmasterStart().IsZero() {
		t.Fatal("control owner postmaster incarnation is missing")
	}
	if binding.ControlTargetKey() != controlKey || binding.OriginalTargetKey() != originalKey {
		t.Fatalf("captured control/original keys are wrong: %+v", binding)
	}
	if binding.TransportTargetKey() == originalKey || binding.TransportTargetKey() == (TargetKey{}) {
		t.Fatalf("transport key must be distinct from the original direct target key: %+v", binding)
	}
	if !binding.SQLFactsComplete() {
		t.Fatalf("phase-1 SQL capture incomplete: missing=%v", binding.MissingSQLFacts())
	}
	if state := binding.Phase2OSBindingState(); state != "OPEN" {
		t.Fatalf("phase-2 OS binding must be explicitly OPEN in phase 1, got %q", state)
	}

	// Independent cross-check: the captured control facts name the real lock
	// owner session holding the advisory lock.
	var (
		ownerPID     int
		ownerDB      string
		ownerStart   time.Time
		controlLocks int
	)
	if err := e.ctrl.QueryRow(e.ctx, `
SELECT a.pid::int, current_database(), a.backend_start,
       (SELECT count(*) FROM pg_locks l WHERE l.pid=a.pid AND l.locktype='advisory' AND l.granted AND l.objsubid=2)
FROM pg_stat_activity a WHERE a.pid=$1`, binding.ControlBackendPID()).
		Scan(&ownerPID, &ownerDB, &ownerStart, &controlLocks); err != nil {
		t.Fatalf("cross-check control lock owner: %v", err)
	}
	if ownerPID != binding.ControlBackendPID() || !ownerStart.Equal(binding.ControlBackendStart()) || controlLocks == 0 {
		t.Fatalf("captured control owner does not name the real advisory-lock session: pid=%d start=%s locks=%d", ownerPID, ownerStart, controlLocks)
	}
	var observedSystemID string
	var observedPostmaster time.Time
	if err := e.admin.QueryRow(e.ctx, `SELECT system_identifier::text, pg_postmaster_start_time() FROM pg_control_system()`).Scan(&observedSystemID, &observedPostmaster); err != nil {
		t.Fatalf("independent cluster identity read: %v", err)
	}
	if binding.ClusterSystemIdentifier() != observedSystemID || !binding.PostmasterStartTime().Equal(observedPostmaster) {
		t.Fatalf("captured cluster identity is not the SQL runtime original: captured=%q/%s observed=%q/%s",
			binding.ClusterSystemIdentifier(), binding.PostmasterStartTime(), observedSystemID, observedPostmaster)
	}
	if !binding.ControlPostmasterStart().Equal(observedPostmaster) {
		t.Fatalf("captured control postmaster incarnation is not the SQL runtime original: captured=%s observed=%s",
			binding.ControlPostmasterStart(), observedPostmaster)
	}
	if binding.ControlPostmasterStart().IsZero() {
		t.Fatal("control postmaster incarnation must be captured, not defaulted")
	}

	// Opaque handle: no started facts before a real run, and no claim.
	handle := run.Observation()
	if !handle.Valid() {
		t.Fatal("borrowed observation handle is not backed by the private observation")
	}
	if identity := handle.StartedIdentity(); identity.Started {
		t.Fatalf("handle reports a started child before any run: %+v", identity)
	}
	if _, err := handle.ClaimForEndpoint(endpoint); err == nil {
		t.Fatal("unstarted borrowed handle issued an origin claim")
	}
	if _, err := handle.ClaimForEndpoint(DrillOriginEndpoint{}); err == nil {
		t.Fatal("zero endpoint capability issued an origin claim")
	}

	// Reserve-once: a canceled-context run fails before any write and still
	// consumes the single reservation; a second run is refused.
	canceled, cancelRun := context.WithCancel(e.ctx)
	cancelRun()
	if _, _, err := run.Run(canceled); err == nil {
		t.Fatal("canceled borrowed run unexpectedly succeeded")
	}
	if _, _, err := run.Run(e.ctx); err == nil || !strings.Contains(err.Error(), "already reserved") {
		t.Fatalf("second borrowed run was not refused by the one-use reservation: %v", err)
	}

	t.Run("hook validates canonical invocation and rewrites only host port", func(t *testing.T) {
		appName, err := NewAttemptApplicationName()
		if err != nil {
			t.Fatal(err)
		}
		tagged, err := ConninfoWithAttemptApplicationName(opts.TargetDSN, appName)
		if err != nil {
			t.Fatal(err)
		}
		canonical := []string{"--clean", "--if-exists", "--no-owner", "--no-privileges", "--dbname=" + tagged}
		rewritten, err := run.prepareChildArgs(targetWriterChildArgPreflight, tools.sealed.restore.path, append([]string(nil), canonical...))
		if err != nil {
			t.Fatalf("valid canonical invocation refused: %v", err)
		}
		if len(rewritten) != 5 || rewritten[0] != "--clean" || rewritten[1] != "--if-exists" || rewritten[2] != "--no-owner" || rewritten[3] != "--no-privileges" || !strings.HasPrefix(rewritten[4], "--dbname=") {
			t.Fatalf("rewritten vector changed the closed flags: %q", rewritten)
		}
		original, _ := url.Parse(tagged)
		transport, err := url.Parse(strings.TrimPrefix(rewritten[4], "--dbname="))
		if err != nil {
			t.Fatalf("parse rewritten transport DSN: %v", err)
		}
		if transport.Host != endpoint.Addr() {
			t.Fatalf("transport host/port not rewritten to the armed listener: %q want %q", transport.Host, endpoint.Addr())
		}
		if transport.User == nil || transport.User.String() != original.User.String() || transport.Path != original.Path {
			t.Fatalf("role/password/database not preserved: %q", transport)
		}
		if transport.Query().Get("application_name") != appName || transport.Query().Get("sslmode") != original.Query().Get("sslmode") {
			t.Fatalf("application tag/settings not preserved: %q", transport)
		}

		refuse := func(name string, executable string, args []string) {
			t.Helper()
			if _, err := run.prepareChildArgs(targetWriterChildArgPreflight, executable, args); err == nil {
				t.Fatalf("%s was not refused", name)
			} else {
				var refusal *targetWriterChildTransportRefusal
				if !errors.As(err, &refusal) || refusal.reason == "" {
					t.Fatalf("%s refusal is not the safe typed transport cause: %v", name, err)
				}
			}
		}
		refuse("wrong executable", "/bin/true", append([]string(nil), canonical...))
		refuse("wrong argument shape", tools.sealed.restore.path, []string{"--clean", "--dbname=" + tagged})
		refuse("positional override", tools.sealed.restore.path, append(append([]string(nil), canonical[:2]...), canonical[2], "/tmp/archive.dump"))
		wrongHost, err := ConninfoWithAttemptApplicationName(
			"postgres://"+url.UserPassword(e.target.Role, "pw").String()+"@127.0.0.1:1/"+e.target.Database+"?sslmode=disable", appName)
		if err != nil {
			t.Fatal(err)
		}
		refuse("foreign host", tools.sealed.restore.path, []string{"--clean", "--if-exists", "--dbname=" + wrongHost})
		wrongRole, err := ConninfoWithAttemptApplicationName(
			"postgres://"+url.UserPassword("other_role", "pw").String()+"@"+e.target.Host+"/"+e.target.Database+"?sslmode=disable", appName)
		if err != nil {
			t.Fatal(err)
		}
		refuse("foreign role", tools.sealed.restore.path, []string{"--clean", "--if-exists", "--dbname=" + wrongRole})
		wrongDatabase, err := ConninfoWithAttemptApplicationName(
			"postgres://"+url.UserPassword(e.target.Role, "pw").String()+"@"+e.target.Host+"/other_database?sslmode=disable", appName)
		if err != nil {
			t.Fatal(err)
		}
		refuse("foreign database", tools.sealed.restore.path, []string{"--clean", "--if-exists", "--dbname=" + wrongDatabase})
		alternate, err := DrillOpenOriginEndpoint()
		if err != nil {
			t.Fatal(err)
		}
		defer alternate.Listener().Close()
		alternateDSN, err := ConninfoWithAttemptApplicationName(
			"postgres://"+url.UserPassword(e.target.Role, "pw").String()+"@"+alternate.Addr()+"/"+e.target.Database+"?sslmode=disable", appName)
		if err != nil {
			t.Fatal(err)
		}
		refuse("alternate real listener", tools.sealed.restore.path, []string{"--clean", "--if-exists", "--dbname=" + alternateDSN})
		refuse("service routing", tools.sealed.restore.path, []string{"--clean", "--if-exists", "--dbname=postgres://u:p@" + endpoint.Addr() + "/db?service=carrier&application_name=" + appName})
		refuse("hostaddr routing", tools.sealed.restore.path, []string{"--clean", "--if-exists", "--dbname=postgres://u:p@" + endpoint.Addr() + "/db?hostaddr=10.0.0.1&application_name=" + appName})
		refuse("multi-host routing", tools.sealed.restore.path, []string{"--clean", "--if-exists", "--dbname=postgres://u:p@h1,h2/db?application_name=" + appName})
		refuse("unix routing", tools.sealed.restore.path, []string{"--clean", "--if-exists", "--dbname=postgres://u:p@/db?application_name=" + appName})
	})

	t.Run("wrong borrowed lock control store and key are refused", func(t *testing.T) {
		wrongControl, err := AcquireTargetLock(e.ctx, e.adminDSN, originalKey, 2*time.Second, 10*time.Millisecond)
		if err != nil {
			t.Fatalf("acquire wrong-control lock: %v", err)
		}
		defer wrongControl.Release(context.Background())
		if _, err := DrillNewBorrowedWriterRun(e.ctx, opts, wrongControl, endpoint, tools); err == nil {
			t.Fatal("factory accepted a lock from a different control store")
		}
		adminTarget, err := controlstore.ParseDSNTarget(e.adminDSN)
		if err != nil {
			t.Fatal(err)
		}
		adminKey, err := CanonicalTargetKey(adminTarget)
		if err != nil {
			t.Fatal(err)
		}
		wrongKey, err := AcquireTargetLock(e.ctx, e.ctrlDSN, adminKey, 2*time.Second, 10*time.Millisecond)
		if err != nil {
			t.Fatalf("acquire wrong-key lock: %v", err)
		}
		defer wrongKey.Release(context.Background())
		if _, err := DrillNewBorrowedWriterRun(e.ctx, opts, wrongKey, endpoint, tools); err == nil {
			t.Fatal("factory accepted a lock for a different target key")
		}
	})

	t.Run("privileged observer role may differ from the writer role", func(t *testing.T) {
		observerRole := "borrowed_observer_privileged"
		if _, err := e.admin.Exec(e.ctx, `CREATE ROLE `+pgx.Identifier{observerRole}.Sanitize()+` LOGIN SUPERUSER PASSWORD 'borrowed-observer-password'`); err != nil {
			t.Fatalf("create privileged observer role: %v", err)
		}
		if observerRole == opts.TrustedTarget.Role {
			t.Fatal("observer fixture must not be the writer role")
		}
		roleOpts := opts
		roleOpts.ObserverDSN = borrowedTransportObserverDSNWithRole(t, e.adminDSN, e.target.Database, observerRole, "borrowed-observer-password")
		roleOpts.OperationID = "borrow-phase1-observer-role"
		observerRun, err := DrillNewBorrowedWriterRun(e.ctx, roleOpts, lock, endpoint, tools)
		if err != nil {
			t.Fatalf("factory refused a privileged observer role that is not the writer role: %v", err)
		}
		observerBinding := observerRun.Binding()
		if !observerBinding.SQLFactsComplete() {
			t.Fatalf("observer-role capture is incomplete: %v", observerBinding.MissingSQLFacts())
		}
		if observerBinding.WriterRoleOID() != binding.WriterRoleOID() {
			t.Fatal("writer role fingerprint must resolve to the original writer role, not the observer role")
		}
		if observerBinding.ClusterSystemIdentifier() != binding.ClusterSystemIdentifier() {
			t.Fatal("observer-role capture must name the same original cluster")
		}
		if observerBinding.OriginalTargetKey() != originalKey || observerBinding.OriginalRoleFingerprint() != binding.OriginalRoleFingerprint() {
			t.Fatal("observer-role capture changed the original target/key/fingerprint binding")
		}
	})

	t.Run("wrong observer identity is refused before connecting", func(t *testing.T) {
		auditBefore := borrowedTransportAuditCount(t, e)
		var guardBefore int64
		if err := e.ctrl.QueryRow(e.ctx, `SELECT count(*) FROM recovery_target_guard WHERE target_guard_key=$1`, originalKey.String()).Scan(&guardBefore); err != nil {
			t.Fatalf("count target guard rows: %v", err)
		}
		refuse := func(name, observerDSN string) {
			t.Helper()
			observerOpts := opts
			observerOpts.ObserverDSN = observerDSN
			if run, err := DrillNewBorrowedWriterRun(e.ctx, observerOpts, lock, endpoint, tools); err == nil || run != nil {
				t.Fatalf("%s observer was not refused by the factory", name)
			} else if !strings.Contains(err.Error(), "observer identity does not match") {
				t.Fatalf("%s refusal is not the canonical identity mismatch: %v", name, err)
			}
		}
		// Canonical host/port/database mismatch, same reachable host:port.
		refuse("wrong database", borrowedTransportObserverDSNForDatabase(t, e.adminDSN, "ctrl"))
		// Canonical host/port mismatch against a live tarpit: the factory must
		// refuse without ever connecting (no accepted connection follows).
		tarpit, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("open observer tarpit: %v", err)
		}
		defer tarpit.Close()
		tarpitDSN := borrowedTransportObserverDSNForDatabase(t, "postgres://observer:pw@"+tarpit.Addr().String()+"/txharbor", e.target.Database)
		started := time.Now()
		refuse("wrong host/port", tarpitDSN)
		if elapsed := time.Since(started); elapsed > 2*time.Second {
			t.Fatalf("wrong observer refusal took %s: a connection attempt was made before validation", elapsed)
		}
		if tcp, ok := tarpit.(*net.TCPListener); ok {
			_ = tcp.SetDeadline(time.Now().Add(300 * time.Millisecond))
		}
		if conn, err := tarpit.Accept(); err == nil {
			_ = conn.Close()
			t.Fatal("factory connected to the wrong observer despite the canonical identity mismatch")
		}
		if after := borrowedTransportAuditCount(t, e); after != auditBefore {
			t.Fatal("wrong observer refusal wrote audit rows")
		}
		var guardAfter int64
		if err := e.ctrl.QueryRow(e.ctx, `SELECT count(*) FROM recovery_target_guard WHERE target_guard_key=$1`, originalKey.String()).Scan(&guardAfter); err != nil {
			t.Fatalf("recount target guard rows: %v", err)
		}
		if guardAfter != guardBefore {
			t.Fatal("wrong observer refusal wrote target guard/launch facts")
		}
	})

	t.Run("closed endpoint is refused by the hook and the factory", func(t *testing.T) {
		closed, err := DrillOpenOriginEndpoint()
		if err != nil {
			t.Fatal(err)
		}
		if err := closed.Listener().Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := DrillNewBorrowedWriterRun(e.ctx, opts, lock, closed, tools); err == nil {
			t.Fatal("factory accepted a closed endpoint")
		}
		endpoint.Listener().Close()
		if _, err := run.prepareChildArgs(targetWriterChildArgPreflight, tools.sealed.restore.path, []string{"--clean", "--if-exists", "--dbname=x"}); err == nil {
			t.Fatal("hook accepted a closed endpoint capability")
		}
	})
}

// TestBorrowedTransportPhase1RefusesCrossClusterObserver proves the factory
// refuses a known control-owner/observer cluster mismatch before publishing
// any facts-complete binding: the control lock owner stays on the first real
// cluster while the observer DSN targets a genuinely different real cluster
// whose database and role names look identical.
func TestBorrowedTransportPhase1RefusesCrossClusterObserver(t *testing.T) {
	e := newBorrowedTransportPG(t)
	secondAdmin, secondTargetDSN, secondTarget := borrowedTransportSecondCluster(t)
	if secondTarget.Database != e.target.Database || secondTarget.Role != e.target.Role {
		t.Fatal("second cluster target is not same-looking (database/role names)")
	}
	var firstSystemID, secondSystemID string
	if err := e.admin.QueryRow(e.ctx, `SELECT system_identifier::text FROM pg_control_system()`).Scan(&firstSystemID); err != nil {
		t.Fatalf("first cluster identity: %v", err)
	}
	if err := secondAdmin.QueryRow(e.ctx, `SELECT system_identifier::text FROM pg_control_system()`).Scan(&secondSystemID); err != nil {
		t.Fatalf("second cluster identity: %v", err)
	}
	if firstSystemID == "" || secondSystemID == "" || firstSystemID == secondSystemID {
		t.Fatal("fixture did not produce two distinct real clusters")
	}
	tools, err := DrillProvisionNativePGTools(t)
	if err != nil {
		t.Fatalf("provision sealed tools: %v", err)
	}
	endpoint, err := DrillOpenOriginEndpoint()
	if err != nil {
		t.Fatalf("open factory endpoint: %v", err)
	}
	defer endpoint.Listener().Close()
	originalKey, err := CanonicalTargetKey(secondTarget)
	if err != nil {
		t.Fatalf("second target key: %v", err)
	}
	lock, err := AcquireTargetLock(e.ctx, e.ctrlDSN, originalKey, 3*time.Second, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("acquire real borrowed lock: %v", err)
	}
	defer lock.Release(context.Background())
	opts := TargetWriterOptions{
		OperationKind: TargetWriterOperationRestore,
		Store:         e.store, ControlDSN: e.ctrlDSN, TargetDSN: secondTargetDSN,
		ObserverDSN: borrowedTransportObserverDSN(t, secondTargetDSN, secondTargetDSN), TrustedTarget: secondTarget,
		OperationID: "borrow-phase1-crosscluster-1",
		Archive:     strings.NewReader("archive"),
		Probe: func(context.Context, TargetWriterProof) (TargetWriterProbeResult, error) {
			return TargetWriterProbeResult{}, errors.New("phase 1 probe intentionally refuses")
		},
		Acceptance: func(context.Context, pgx.Tx, TargetWriterProof) (TargetWriterAcceptance, error) {
			return TargetWriterAcceptance{}, errors.New("phase 1 acceptance intentionally refuses")
		},
	}
	run, err := DrillNewBorrowedWriterRun(e.ctx, opts, lock, endpoint, tools)
	if err == nil || run != nil {
		t.Fatal("factory published a cross-cluster control/observer capture")
	}
	if !strings.Contains(err.Error(), "not the same PostgreSQL cluster") {
		t.Fatalf("cross-cluster refusal is not the cluster mismatch cause: %v", err)
	}
	if err := lock.Health(e.ctx); err != nil {
		t.Fatal("cross-cluster refusal disturbed the borrowed control lock")
	}
}

// borrowedTransportRunOutcome carries one factory Run result off the test
// goroutine. It grants no additional authority over the run or its receipt.
type borrowedTransportRunOutcome struct {
	result  TargetWriterResult
	receipt DrillTargetProcessReceipt
	err     error
}

// borrowedTransportStallingPeer is the real reader peer of the factory
// endpoint listener: it accepts actual TCP connections and reads their
// startup bytes, then never answers, so a native child blocks at startup
// authentication instead of any simulated PostgreSQL success.
type borrowedTransportStallingPeer struct {
	listener net.Listener
	mu       sync.Mutex
	accepted int
	read     int
	closed   sync.Once
}

func newBorrowedTransportStallingPeer(listener net.Listener) *borrowedTransportStallingPeer {
	peer := &borrowedTransportStallingPeer{listener: listener}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			peer.mu.Lock()
			peer.accepted++
			peer.mu.Unlock()
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 4096)
				for {
					n, err := c.Read(buf)
					if n > 0 {
						peer.mu.Lock()
						peer.read += n
						peer.mu.Unlock()
					}
					if err != nil {
						return
					}
				}
			}(conn)
		}
	}()
	return peer
}

func (p *borrowedTransportStallingPeer) Close() {
	p.closed.Do(func() { _ = p.listener.Close() })
}

func (p *borrowedTransportStallingPeer) Accepted() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.accepted
}

func (p *borrowedTransportStallingPeer) ReadBytes() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.read
}

func (p *borrowedTransportStallingPeer) waitAccepted(t *testing.T, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if p.Accepted() > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("native child never connected to the armed origin endpoint")
}

// borrowedTransportProcessCountWithExecutable counts live processes whose
// resolved executable is the sealed tool file, proving at most one supervised
// native child exists at a time.
func borrowedTransportProcessCountWithExecutable(t *testing.T, executablePath string) int {
	t.Helper()
	sealed, err := os.Stat(executablePath)
	if err != nil {
		t.Fatalf("stat sealed executable: %v", err)
	}
	entries, err := os.ReadDir("/proc")
	if err != nil {
		t.Fatalf("read proc: %v", err)
	}
	count := 0
	for _, entry := range entries {
		if _, err := strconv.Atoi(entry.Name()); err != nil {
			continue
		}
		info, err := os.Stat(filepath.Join("/proc", entry.Name(), "exe"))
		if err != nil {
			continue
		}
		if os.SameFile(info, sealed) {
			count++
		}
	}
	return count
}

// borrowedTransportSeedCleanGuard seeds the positively-clean target guard the
// real coordinator requires before it may record launch intent.
func borrowedTransportSeedCleanGuard(t *testing.T, e *borrowedTransportPG, key TargetKey, operationID string) {
	t.Helper()
	if _, err := e.ctrl.Exec(e.ctx, `INSERT INTO recovery_target_guard
		(target_guard_key, disposition, operation_id, clean_at, rebuild_evidence)
		VALUES ($1, 'clean', $2, now(), '{"fixture":"borrowed-transport-native"}')
		ON CONFLICT (target_guard_key) DO UPDATE SET disposition='clean', active_writer=FALSE,
		launch_intent=FALSE, attempt_app_name=NULL, launch_intent_at=NULL,
		launched_at=NULL, rebuild_required_at=NULL, clean_at=now(),
		rebuild_evidence='{"fixture":"borrowed-transport-native"}'`, key.String(), operationID); err != nil {
		t.Fatalf("seed clean target guard: %v", err)
	}
}

// TestBorrowedTransportPhase1FactoryNativeLaunchBlockedChild exercises the
// authentic factory through the ACTUAL coordinator into a native launch: a
// genuine custom dump is restored toward the armed endpoint, whose real reader
// peer stalls startup authentication so the child is provably the sealed
// private ELF (never a PATH-resolved ordinary pg_restore) before the only
// owner's cancellation terminates it. The probe intentionally refuses; no
// acceptance, verified or restored state is ever reached.
func TestBorrowedTransportPhase1FactoryNativeLaunchBlockedChild(t *testing.T) {
	unsetBorrowedTransportPGEnvironment(t)
	e := newBorrowedTransportPG(t)
	tools, err := DrillProvisionNativePGTools(t)
	if err != nil {
		t.Fatalf("provision sealed tools: %v", err)
	}
	restorePath := tools.sealed.restore.path
	dumpPath, ok := tools.DumpPath()
	if !ok {
		t.Fatal("sealed dump tool is unavailable for the genuine archive")
	}
	endpoint, err := DrillOpenOriginEndpoint()
	if err != nil {
		t.Fatalf("open factory endpoint: %v", err)
	}
	defer endpoint.Listener().Close()
	originalKey, err := CanonicalTargetKey(e.target)
	if err != nil {
		t.Fatalf("original target key: %v", err)
	}
	lock, err := AcquireTargetLock(e.ctx, e.ctrlDSN, originalKey, 3*time.Second, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("acquire real borrowed lock: %v", err)
	}
	defer lock.Release(context.Background())
	operationID := "borrow-phase1-native-1"
	borrowedTransportSeedCleanGuard(t, e, originalKey, operationID)

	// A genuine custom archive from the real fixture source database owned by
	// the writer role; never a hand-written stub archive.
	sourcePool := openBorrowedTransportPool(t, e.ctx, e.targetDSN)
	if _, err := sourcePool.Exec(e.ctx, `CREATE TABLE IF NOT EXISTS borrowed_source_table (id integer PRIMARY KEY, note text)`); err != nil {
		t.Fatalf("create fixture source table: %v", err)
	}
	if _, err := sourcePool.Exec(e.ctx, `INSERT INTO borrowed_source_table (id, note) VALUES (1, 'phase1') ON CONFLICT (id) DO NOTHING`); err != nil {
		t.Fatalf("seed fixture source table: %v", err)
	}
	archivePath := filepath.Join(t.TempDir(), "borrowed-source.dump")
	dump := exec.CommandContext(e.ctx, dumpPath, "--format=custom", "--no-owner", "--no-privileges", "--file="+archivePath, e.targetDSN)
	if err := dump.Run(); err != nil {
		t.Fatalf("fixture source pg_dump failed: %v", err)
	}
	list := exec.CommandContext(e.ctx, restorePath, "--list", archivePath)
	listed, err := list.Output()
	if err != nil || !strings.Contains(string(listed), "borrowed_source_table") {
		t.Fatal("fixture archive is not a genuine custom dump of the source database")
	}
	archive, err := os.Open(archivePath)
	if err != nil {
		t.Fatalf("open fixture archive: %v", err)
	}
	defer archive.Close()

	// A wrong ordinary pg_restore earlier in PATH must never be selected or
	// touched: only the factory-selected sealed private path may run.
	canaryDir := t.TempDir()
	canaryMarker := filepath.Join(canaryDir, "canary-pg-restore-ran")
	canary := "#!/bin/sh\ntouch '" + canaryMarker + "'\nexit 0\n"
	if err := os.WriteFile(filepath.Join(canaryDir, "pg_restore"), []byte(canary), 0o755); err != nil {
		t.Fatalf("write PATH canary: %v", err)
	}
	t.Setenv("PATH", canaryDir+":"+os.Getenv("PATH"))

	var probeCalls, acceptanceCalls int32
	opts := TargetWriterOptions{
		OperationKind: TargetWriterOperationRestore,
		Store:         e.store, ControlDSN: e.ctrlDSN, TargetDSN: e.targetDSN,
		ObserverDSN: borrowedTransportObserverDSN(t, e.adminDSN, e.targetDSN), TrustedTarget: e.target,
		OperationID: operationID, Archive: archive,
		QuiescenceTimeout: 5 * time.Second, QuiescenceInterval: 50 * time.Millisecond,
		Probe: func(context.Context, TargetWriterProof) (TargetWriterProbeResult, error) {
			atomic.AddInt32(&probeCalls, 1)
			return TargetWriterProbeResult{}, errors.New("phase 1 probe intentionally refuses")
		},
		Acceptance: func(context.Context, pgx.Tx, TargetWriterProof) (TargetWriterAcceptance, error) {
			atomic.AddInt32(&acceptanceCalls, 1)
			return TargetWriterAcceptance{}, errors.New("phase 1 acceptance intentionally refuses")
		},
	}
	run, err := DrillNewBorrowedWriterRun(e.ctx, opts, lock, endpoint, tools)
	if err != nil {
		t.Fatalf("borrowed writer run factory: %v", err)
	}
	if run.opts.executable != restorePath {
		t.Fatal("factory did not select the private sealed restore executable")
	}
	// Actual struct copy, not a pointer alias, BEFORE the first invocation.
	copied := *run
	peer := newBorrowedTransportStallingPeer(endpoint.Listener())
	defer peer.Close()
	handle := run.Observation()
	runCtx, cancelRun := context.WithCancel(e.ctx)
	defer cancelRun()
	outcome := make(chan borrowedTransportRunOutcome, 1)
	go func() {
		result, receipt, err := run.Run(runCtx)
		outcome <- borrowedTransportRunOutcome{result: result, receipt: receipt, err: err}
	}()
	startCtx, cancelStart := context.WithTimeout(e.ctx, 60*time.Second)
	defer cancelStart()
	if err := handle.AwaitStarted(startCtx); err != nil {
		cancelRun()
		t.Fatalf("native child was not started by the actual coordinator: %v", err)
	}
	startIdentity := handle.StartedIdentity()
	if startIdentity.PID <= 0 || startIdentity.StartID == 0 {
		t.Fatal("native start identity is incomplete")
	}
	exeInfo, err := os.Stat(fmt.Sprintf("/proc/%d/exe", startIdentity.PID))
	if err != nil {
		t.Fatalf("stat native child executable: %v", err)
	}
	sealedInfo, err := os.Stat(restorePath)
	if err != nil {
		t.Fatalf("stat sealed executable: %v", err)
	}
	if !os.SameFile(exeInfo, sealedInfo) {
		t.Fatal("running child is not the sealed private pg_restore ELF")
	}
	rawCmdline, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", startIdentity.PID))
	if err != nil {
		t.Fatalf("read native child argv: %v", err)
	}
	argv := strings.Split(strings.TrimRight(string(rawCmdline), "\x00"), "\x00")
	if len(argv) != 6 || argv[0] != restorePath || argv[1] != "--clean" || argv[2] != "--if-exists" || argv[3] != "--no-owner" || argv[4] != "--no-privileges" || !strings.HasPrefix(argv[5], "--dbname=") {
		t.Fatal("native child argv is not the exact closed transport invocation on the sealed executable")
	}
	transportDSN, err := url.Parse(strings.TrimPrefix(argv[5], "--dbname="))
	if err != nil || transportDSN.Host != endpoint.Addr() || transportDSN.Path != "/"+e.target.Database {
		t.Fatal("native child was not routed to the armed endpoint with the original database")
	}
	if _, hasPassword := transportDSN.User.Password(); hasPassword {
		t.Fatal("native child argv carries a password")
	}
	rawEnviron, err := os.ReadFile(fmt.Sprintf("/proc/%d/environ", startIdentity.PID))
	if err != nil {
		t.Fatalf("read native child environment: %v", err)
	}
	if !strings.Contains(string(rawEnviron), "PGPASSFILE=/proc/self/fd/") {
		t.Fatal("native child did not receive the private descriptor-backed passfile")
	}
	peer.waitAccepted(t, 15*time.Second)
	if peer.ReadBytes() == 0 {
		t.Fatal("the real endpoint peer accepted but read no startup bytes")
	}
	claimed, err := handle.ClaimForEndpoint(endpoint)
	if err != nil {
		t.Fatal("started origin could not be claimed with the armed endpoint capability")
	}
	if claimedIdentity := claimed.StartedIdentity(); claimedIdentity.PID != startIdentity.PID || claimedIdentity.StartID != startIdentity.StartID {
		t.Fatal("claimed origin identity does not match the started child")
	}
	if !claimed.MatchesEndpoint(endpoint) {
		t.Fatal("claimed origin does not match the armed endpoint capability")
	}
	if _, err := handle.ClaimForEndpoint(endpoint); err == nil {
		t.Fatal("origin was claimed twice")
	}
	// A copy retry while the real child is held is refused and touches nothing:
	// the shared lifetime allows exactly one entry, the observation is not
	// overwritten and still no terminal Wait fact exists.
	canceledCtx, cancelCanceled := context.WithCancel(e.ctx)
	cancelCanceled()
	if _, _, err := copied.Run(canceledCtx); err == nil || !strings.Contains(err.Error(), "already reserved") {
		t.Fatal("copy retry was not refused by the shared lifetime reservation")
	}
	if identity := handle.StartedIdentity(); identity.PID != startIdentity.PID || identity.StartID != startIdentity.StartID {
		t.Fatal("copy retry overwrote the real start facts")
	}
	if terminal := handle.WaitTerminal(); terminal.Terminal {
		t.Fatal("copy retry fabricated terminal wait facts")
	}
	if count := borrowedTransportProcessCountWithExecutable(t, restorePath); count != 1 {
		t.Fatalf("expected exactly one supervised native child while held, found %d", count)
	}
	if atomic.LoadInt32(&probeCalls) != 0 || atomic.LoadInt32(&acceptanceCalls) != 0 {
		t.Fatal("probe/acceptance ran before any accepted outcome")
	}
	// Cancellation is the only terminator; the pinned owner's authentic Wait
	// and the frozen receipt remain the real child facts.
	cancelRun()
	select {
	case finished := <-outcome:
		if finished.err == nil {
			t.Fatal("canceled native run was reported as a clean success")
		}
		if finished.result.TargetKey != originalKey {
			t.Fatal("run result target key changed")
		}
		if finished.result.Application == "" || !finished.result.Command.Started ||
			finished.result.Command.Outcome != PGCommandCanceled || !finished.result.Command.ProcessGroupDrained {
			t.Fatal("canceled run did not report a real started/canceled/drained child")
		}
		if finished.result.Probe.Outcome == TargetWriterProbePassed {
			t.Fatal("canceled run reported a passed probe")
		}
		facts, err := finished.receipt.ConsumeFacts()
		if err != nil {
			t.Fatalf("process receipt: %v", err)
		}
		if !facts.Started || !facts.Terminal || facts.PID != startIdentity.PID {
			t.Fatal("process receipt did not retain the real terminal child facts")
		}
		if facts.TargetKey != originalKey || facts.OperationID != operationID ||
			facts.RoleFingerprint != e.target.DataTargetFingerprint().RoleFingerprint {
			t.Fatal("process receipt lost the original target/operation/role binding")
		}
		if terminal := handle.WaitTerminal(); !terminal.ChildWaitCompleted || !terminal.Terminal {
			t.Fatal("the pinned owner did not record the authentic sole-Wait terminal fact")
		}
	case <-time.After(60 * time.Second):
		t.Fatal("canceled native run did not return within the bounded wait")
	}
	if atomic.LoadInt32(&probeCalls) != 0 || atomic.LoadInt32(&acceptanceCalls) != 0 {
		t.Fatal("probe/acceptance ran on the canceled attempt")
	}
	if _, err := os.Stat(canaryMarker); err == nil {
		t.Fatal("PATH canary pg_restore was executed: the private sealed executable was not selected")
	}
	goneDeadline := time.Now().Add(15 * time.Second)
	for borrowedTransportProcessCountWithExecutable(t, restorePath) != 0 {
		if time.Now().After(goneDeadline) {
			t.Fatal("native child did not disappear after the pinned owner's sole Wait")
		}
		time.Sleep(20 * time.Millisecond)
	}
	k1, k2 := originalKey.AdvisoryLockKey()
	if lock.key1 != k1 || lock.key2 != k2 {
		t.Fatal("borrowed lock namespace key changed during the native run")
	}
	if err := lock.Health(e.ctx); err != nil {
		t.Fatal("borrowed lock was released or lost during the native run")
	}
	guard, found, err := controlstore.ReadTargetGuard(e.ctx, e.ctrl, originalKey.String())
	if err != nil || !found || guard.State == controlstore.TargetGuardClean {
		t.Fatal("failed native attempt left a clean target guard: no verified/restored state was reached")
	}
	if _, _, err := copied.Run(canceledCtx); err == nil || !strings.Contains(err.Error(), "already reserved") {
		t.Fatal("copy replay after terminal was not refused")
	}
	if _, _, err := run.Run(e.ctx); err == nil || !strings.Contains(err.Error(), "already reserved") {
		t.Fatal("run replay after terminal was not refused")
	}
}

// borrowedTransportDSNWithRole rewrites the login role/password of an existing
// DSN without touching its host, port, database or parameters.
func borrowedTransportDSNWithRole(t *testing.T, baseDSN, role, password string) string {
	t.Helper()
	parsed, err := url.Parse(baseDSN)
	if err != nil {
		t.Fatalf("parse fixture DSN: %v", err)
	}
	parsed.User = url.UserPassword(role, password)
	return parsed.String()
}
