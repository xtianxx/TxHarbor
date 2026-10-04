//go:build linux && drill

// borrowed-auth-fixture-mini_linux_test.go validates the fresh restricted-writer
// auth-handoff fixture on real PostgreSQL: restricted W catalog facts (including
// NOINHERIT and no memberships) and the control-database 42501 boundary, an
// authentic native restore through the borrowed gate running AS W (not admin),
// a real SCRAM P0 authentication on the direct TCP/Unix routes using the owned
// helper, and the AF01 credential-causality negatives (timestamp/template and
// fixed-administrator candidates rejected with 28P01). No rotation, postmaster
// stop, acceptance or evidence authority is created; InstanceID stays empty
// because the run is isolated.
package recovery_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/xtianxx/txharbor/internal/recovery"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

func newBorrowedAuthRunOutcome() chan struct {
	result  recovery.TargetWriterResult
	receipt recovery.DrillTargetProcessReceipt
	err     error
} {
	return make(chan struct {
		result  recovery.TargetWriterResult
		receipt recovery.DrillTargetProcessReceipt
		err     error
	}, 1)
}

func TestBorrowedAuthFixtureCatalogAndControlBoundary(t *testing.T) {
	f := newBorrowedAuthHandoffFixture(t)
	ctx := context.Background()

	// SCRAM-only HBA on every reachable transport of the protected container.
	assertProtectedScramOnlyHBA(t, ctx, f.fx.admin)

	// Real restricted W catalog facts: exact flags, NOINHERIT, LOGIN, SCRAM
	// verifier, no memberships, W-owned target database.
	var (
		super, createdb, createrole, replication, bypassRLS, canLogin, canInherit bool
		password                                                                  string
	)
	if err := f.fx.admin.QueryRow(ctx, `
SELECT rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolbypassrls, rolcanlogin, rolinherit, coalesce(rolpassword,'')
FROM pg_authid WHERE rolname=$1`, f.writerRole).
		Scan(&super, &createdb, &createrole, &replication, &bypassRLS, &canLogin, &canInherit, &password); err != nil {
		t.Fatalf("read W role flags: %v", err)
	}
	if super || createdb || createrole || replication || bypassRLS || !canLogin || canInherit {
		t.Fatalf("W role flags are not the restricted NOINHERIT login: super=%t createdb=%t createrole=%t replication=%t bypassrls=%t login=%t inherit=%t",
			super, createdb, createrole, replication, bypassRLS, canLogin, canInherit)
	}
	if !strings.HasPrefix(password, "SCRAM-SHA-256$") {
		t.Fatalf("W rolpassword is not a SCRAM verifier")
	}
	var memberships int
	if err := f.fx.admin.QueryRow(ctx, `
SELECT count(*) FROM pg_auth_members m JOIN pg_roles g ON g.oid = m.member WHERE g.rolname = $1`, f.writerRole).Scan(&memberships); err != nil {
		t.Fatalf("read W memberships: %v", err)
	}
	if memberships != 0 {
		t.Fatalf("W holds %d role memberships, want none", memberships)
	}
	var owner string
	if err := f.fx.admin.QueryRow(ctx, `SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname=$1`, f.targetDB).Scan(&owner); err != nil || owner != f.writerRole {
		t.Fatalf("target database owner is not W: owner=%q err=%v", owner, err)
	}
	var controlOwner string
	if err := f.fx.admin.QueryRow(ctx, `SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname=$1`, f.controlDB).Scan(&controlOwner); err != nil || controlOwner != f.adminRole {
		t.Fatalf("control database owner is not the administrator: owner=%q err=%v", controlOwner, err)
	}

	// The inherited fixed fixture role (fx.role, created by the shared fixture
	// with a public fixed credential) is itself a restricted NOINHERIT login
	// with no memberships and no membership edge to the administrator: its
	// known credential can never reach admin/observer/control authority.
	var inhSuper, inhCreateDB, inhCreateRole, inhRepl, inhBypassRLS, inhLogin, inhInherit bool
	if err := f.fx.admin.QueryRow(ctx, `
SELECT rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolbypassrls, rolcanlogin, rolinherit
FROM pg_roles WHERE rolname=$1`, f.fx.role).
		Scan(&inhSuper, &inhCreateDB, &inhCreateRole, &inhRepl, &inhBypassRLS, &inhLogin, &inhInherit); err != nil {
		t.Fatalf("read inherited fixture role flags: %v", err)
	}
	if inhSuper || inhCreateDB || inhCreateRole || inhRepl || inhBypassRLS || !inhLogin || inhInherit {
		t.Fatalf("inherited fixed fixture role is not the restricted NOINHERIT login: super=%t createdb=%t createrole=%t replication=%t bypassrls=%t login=%t inherit=%t",
			inhSuper, inhCreateDB, inhCreateRole, inhRepl, inhBypassRLS, inhLogin, inhInherit)
	}
	var inhMemberships int
	if err := f.fx.admin.QueryRow(ctx, `
SELECT count(*) FROM pg_auth_members m JOIN pg_roles g ON g.oid = m.member WHERE g.rolname = $1`, f.fx.role).Scan(&inhMemberships); err != nil {
		t.Fatalf("read inherited fixture role memberships: %v", err)
	}
	if inhMemberships != 0 {
		t.Fatalf("inherited fixed fixture role holds %d memberships, want none", inhMemberships)
	}
	var inhAdminEdges int
	if err := f.fx.admin.QueryRow(ctx, `
SELECT count(*) FROM pg_auth_members m
JOIN pg_roles a ON a.oid = m.roleid JOIN pg_roles b ON b.oid = m.member
WHERE (a.rolname=$1 AND b.rolname=$2) OR (a.rolname=$2 AND b.rolname=$1)`, f.fx.role, f.adminRole).Scan(&inhAdminEdges); err != nil {
		t.Fatalf("read inherited fixture role/admin membership edges: %v", err)
	}
	if inhAdminEdges != 0 {
		t.Fatalf("inherited fixed fixture role has %d membership edges with the administrator, want none", inhAdminEdges)
	}
	if inhOID := borrowedAuthRoleOID(t, f.fx.admin, f.fx.role); inhOID == f.adminRoleOID || inhOID == f.observerRoleOID || inhOID == f.controlRoleOID || inhOID == f.writerRoleOID {
		t.Fatalf("inherited fixed fixture role OID %d collides with a fixture identity", inhOID)
	}

	// AF02: the protected control database must refuse the real W login with a
	// genuine server-side permission denial (SQLSTATE 42501) — never an
	// authentication failure or a transport failure — while the authorized
	// control role may connect. The same W credential must independently
	// complete a real authenticated session on the W-owned target database,
	// proving the refusal above is a legitimate permission boundary.
	wControlDSN := borrowedAuthRoleDSN(t, f.controlDSN, f.writerRole, f.writerPassword)
	if conn, err := pgx.Connect(ctx, wControlDSN); err == nil {
		_ = conn.Close(ctx)
		t.Fatal("real W connected to the protected control database")
	} else if code := borrowedAuthSQLState(err); code != "42501" {
		t.Fatalf("real W control connect was not a database permission denial (sqlstate=%s, W role OID %d, control role OID %d)",
			code, f.writerRoleOID, f.controlRoleOID)
	}
	controlConn, err := pgx.Connect(ctx, f.controlDSN)
	if err != nil {
		t.Fatalf("authorized control role cannot connect (sqlstate=%s, control role OID %d)", borrowedAuthSQLState(err), f.controlRoleOID)
	}
	_ = controlConn.Close(ctx)
	wTargetAuth, err := pgx.Connect(ctx, f.writerTargetDSN)
	if err != nil {
		t.Fatalf("W credential did not complete target authentication (sqlstate=%s, W role OID %d)", borrowedAuthSQLState(err), f.writerRoleOID)
	}
	var currentUser string
	if err := wTargetAuth.QueryRow(ctx, `SELECT current_user`).Scan(&currentUser); err != nil {
		_ = wTargetAuth.Close(ctx)
		t.Fatalf("W target authenticated session query failed (W role OID %d)", f.writerRoleOID)
	}
	_ = wTargetAuth.Close(ctx)
	if currentUser != f.writerRole {
		t.Fatalf("W target authenticated session runs as %q, want W role OID %d", currentUser, f.writerRoleOID)
	}

	// Distinct protected identities.
	if f.writerRoleOID == f.observerRoleOID || f.writerRoleOID == f.controlRoleOID || f.writerRoleOID == f.adminRoleOID ||
		f.observerRoleOID == f.controlRoleOID || f.observerRoleOID == f.adminRoleOID || f.controlRoleOID == f.adminRoleOID {
		t.Fatalf("fixture roles are not distinct: W=%d observer=%d control=%d admin=%d",
			f.writerRoleOID, f.observerRoleOID, f.controlRoleOID, f.adminRoleOID)
	}
	// The protected observer and control roles are privileged logins with real
	// SCRAM verifiers (independent random credentials created by the fixture).
	for _, spec := range []struct {
		role string
		oid  uint32
	}{{f.observerRole, f.observerRoleOID}, {f.controlRole, f.controlRoleOID}} {
		var super, login bool
		var verifier string
		if err := f.fx.admin.QueryRow(ctx, `
SELECT rolsuper, rolcanlogin, coalesce(rolpassword,'') FROM pg_authid WHERE rolname=$1`, spec.role).
			Scan(&super, &login, &verifier); err != nil {
			t.Fatalf("read protected role facts (OID %d): %v", spec.oid, err)
		}
		if !super || !login || !strings.HasPrefix(verifier, "SCRAM-SHA-256$") {
			t.Fatalf("protected role OID %d is not a privileged SCRAM login: super=%t login=%t", spec.oid, super, login)
		}
	}
	// The actual gate's retained observer must be the fresh protected observer
	// login — not the administrator — and distinct from W/control/admin.
	var gateObserverSysID uint32
	if err := f.fx.admin.QueryRow(ctx, `SELECT usesysid::oid FROM pg_stat_activity WHERE pid=$1`, f.gate.observerPID).Scan(&gateObserverSysID); err != nil {
		t.Fatalf("gate observer session row: %v", err)
	}
	if gateObserverSysID != f.observerRoleOID || gateObserverSysID == f.adminRoleOID || gateObserverSysID == f.writerRoleOID || gateObserverSysID == f.controlRoleOID {
		t.Fatalf("gate observer session usesysid=%d, want protected observer OID %d distinct from admin=%d W=%d control=%d",
			gateObserverSysID, f.observerRoleOID, f.adminRoleOID, f.writerRoleOID, f.controlRoleOID)
	}
	// The retained bootstrap connection is still the real administrator
	// session authenticated with the fresh private credential.
	var bootstrapSysID uint32
	if err := f.fx.admin.QueryRow(ctx, `SELECT usesysid::oid FROM pg_stat_activity WHERE pid=$1`, int32(f.fx.admin.PgConn().PID())).Scan(&bootstrapSysID); err != nil {
		t.Fatalf("bootstrap administrator session row: %v", err)
	}
	if bootstrapSysID != f.adminRoleOID {
		t.Fatalf("bootstrap session usesysid=%d, want administrator OID %d", bootstrapSysID, f.adminRoleOID)
	}

	// Real original lock binding: W target key/role/op, control-role namespace
	// and complete binding facts.
	wTarget, err := controlstore.ParseDSNTarget(f.writerTargetDSN)
	if err != nil {
		t.Fatalf("parse W target: %v", err)
	}
	wKey, err := recovery.CanonicalTargetKey(wTarget)
	if err != nil {
		t.Fatalf("W target key: %v", err)
	}
	controlTarget, err := controlstore.ParseDSNTarget(f.controlDSN)
	if err != nil {
		t.Fatalf("parse control DSN: %v", err)
	}
	controlKey, err := recovery.CanonicalTargetKey(controlTarget)
	if err != nil {
		t.Fatalf("control key: %v", err)
	}
	binding := f.run.Binding()
	if binding.OriginalTargetKey() != wKey || binding.ControlTargetKey() != controlKey {
		t.Fatalf("original/control keys are wrong: %+v", binding)
	}
	if binding.WriterRoleOID() != f.writerRoleOID {
		t.Fatalf("binding writer role OID=%d, want W=%d", binding.WriterRoleOID(), f.writerRoleOID)
	}
	if binding.TargetDatabaseOID() != f.targetDBOID {
		t.Fatalf("binding target database OID=%d, want %d", binding.TargetDatabaseOID(), f.targetDBOID)
	}
	if !binding.SQLFactsComplete() {
		t.Fatalf("binding SQL facts incomplete: %v", binding.MissingSQLFacts())
	}

	// Actual control lock owner: real PID/start/usesysid + exact granted
	// advisory pair for the W target key.
	var (
		ownerStart time.Time
		ownerSysID uint32
	)
	if err := f.controlPool.QueryRow(ctx, `
SELECT backend_start, usesysid::oid FROM pg_stat_activity WHERE pid=$1`, binding.ControlBackendPID()).
		Scan(&ownerStart, &ownerSysID); err != nil {
		t.Fatalf("control owner session row: %v", err)
	}
	if !ownerStart.Equal(binding.ControlBackendStart()) || ownerSysID != f.controlRoleOID {
		t.Fatalf("control owner is not the control role session: start=%s/%s usesysid=%d/%d",
			ownerStart, binding.ControlBackendStart(), ownerSysID, f.controlRoleOID)
	}
	k1, k2 := wKey.AdvisoryLockKey()
	var granted int
	if err := f.controlPool.QueryRow(ctx, `
SELECT count(*) FROM pg_locks WHERE pid=$1 AND locktype='advisory' AND granted AND objsubid=2
  AND classid=$2::oid AND objid=$3::oid`, binding.ControlBackendPID(), uint32(k1), uint32(k2)).Scan(&granted); err != nil || granted != 1 {
		t.Fatalf("original advisory lock is not granted to the control owner: count=%d err=%v", granted, err)
	}

	// Retained-connection inventory for a future staged driver: safe metadata
	// only, and no run has started.
	inventory := f.LiveConnections(t)
	if len(inventory) == 0 {
		t.Fatal("retained-connection inventory is empty")
	}
	sawControl := false
	sawUnassigned := false
	for _, conn := range inventory {
		if conn.BackendPID <= 0 || conn.BackendType == "" {
			t.Fatalf("inventory row is not a real backend: %+v", conn)
		}
		if conn.Role == f.controlRole && conn.BackendPID == binding.ControlBackendPID() {
			if conn.UsesysID != f.controlRoleOID {
				t.Fatalf("control inventory row usesysid=%d, want %d", conn.UsesysID, f.controlRoleOID)
			}
			sawControl = true
		}
		if conn.Role == "(unassigned)" || conn.Database == "(unassigned)" {
			// A role-unknown row cannot carry a role OID. A database-unknown
			// background process (e.g. the logical replication launcher) may
			// still run under a real role OID; that is safe diagnostic
			// metadata, never an admission fact.
			if conn.Role == "(unassigned)" && conn.UsesysID != 0 {
				t.Fatalf("unassigned inventory row carries a role OID: %+v", conn)
			}
			sawUnassigned = true
		}
	}
	if !sawControl {
		t.Fatalf("inventory does not name the real control lock owner: %+v", inventory)
	}
	if !sawUnassigned {
		t.Fatalf("inventory omitted background/unassigned sessions: %+v", inventory)
	}
	if identity := f.run.Observation().StartedIdentity(); identity.Started {
		t.Fatalf("fixture run started before any admission: %+v", identity)
	}
}

func TestBorrowedAuthFixtureNativeRestoreAsWriter(t *testing.T) {
	f := newBorrowedAuthHandoffFixture(t)
	ctx := context.Background()

	// The native child inherits only the runner's small runtime allowlist
	// derived from this process environment (ambient PG* settings are refused
	// and everything else is filtered); no protected credential may exist in
	// this environment at all.
	protectedSecrets := []string{f.adminPassword, f.observerPass, f.controlPass}
	for _, entry := range os.Environ() {
		if key, _, ok := strings.Cut(entry, "="); ok && len(key) >= 2 && strings.EqualFold(key[:2], "PG") {
			t.Fatalf("ambient PostgreSQL environment variable %q is present", key)
		}
		for _, secret := range protectedSecrets {
			if strings.Contains(entry, secret) {
				t.Fatal("protected credential material is present in the inherited process environment")
			}
		}
	}

	f.gate.HoldRegistration()

	resultCh := newBorrowedAuthRunOutcome()
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	go func() {
		result, receipt, err := f.run.Run(runCtx)
		resultCh <- struct {
			result  recovery.TargetWriterResult
			receipt recovery.DrillTargetProcessReceipt
			err     error
		}{result, receipt, err}
	}()
	admitCtx, cancelAdmit := context.WithTimeout(ctx, 90*time.Second)
	defer cancelAdmit()
	session, err := f.gate.AdmitBorrowed(admitCtx, f.run, f.prefix)
	if err != nil {
		cancelRun()
		t.Fatalf("auth fixture borrowed admission: %v", err)
	}
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) && !(session.BackendPID() > 0 && session.BufferedFrames() > 0) {
		time.Sleep(20 * time.Millisecond)
	}
	if !(session.BackendPID() > 0 && session.BufferedFrames() > 0) {
		cancelRun()
		t.Fatal("auth fixture never reached the held first executable frame")
	}
	// The registered backend must be the real W login, not the admin.
	var backendRoleOID uint32
	if err := f.fx.admin.QueryRow(ctx, `SELECT usesysid::oid FROM pg_stat_activity WHERE pid=$1`, session.BackendPID()).Scan(&backendRoleOID); err != nil {
		t.Fatalf("registered backend identity: %v", err)
	}
	if backendRoleOID != f.writerRoleOID {
		t.Fatalf("registered backend usesysid=%d, want W=%d (admin=%d)", backendRoleOID, f.writerRoleOID, f.adminRoleOID)
	}
	// Best-effort actual child inspection: while the native child is held at
	// its first frame, read the real argv/environ of the observed child. If the
	// process is opaque, report that explicitly and claim nothing.
	if identity := f.run.Observation().StartedIdentity(); identity.Started && identity.PID > 0 {
		argv, argvErr := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", identity.PID))
		environ, envErr := os.ReadFile(fmt.Sprintf("/proc/%d/environ", identity.PID))
		if argvErr != nil || envErr != nil {
			t.Logf("native child process inspection opaque (pid %d); no argv/environ secret-free claim is made", identity.PID)
		} else {
			for _, secret := range protectedSecrets {
				if strings.Contains(string(argv), secret) || strings.Contains(string(environ), secret) {
					t.Fatal("native child argv/environ exposed a protected credential")
				}
			}
			if strings.Contains(string(argv), f.writerPassword) || strings.Contains(string(environ), f.writerPassword) {
				t.Fatal("native child argv/environ exposed the writer credential instead of the private passfile")
			}
			t.Logf("native child argv/environ carry no protected or writer credential material (pid %d)", identity.PID)
		}
	} else {
		t.Logf("native child is not observed as started at the held frame; argv/environ inspection not claimed")
	}
	// Target relation must still be absent while the first frame is held.
	targetConn, err := pgx.Connect(ctx, f.writerTargetDSN)
	if err != nil {
		cancelRun()
		t.Fatalf("connect W target during hold: %v", err)
	}
	var absent bool
	if err := targetConn.QueryRow(ctx, `SELECT to_regclass($1) IS NULL`, "public."+f.table).Scan(&absent); err != nil || !absent {
		_ = targetConn.Close(ctx)
		cancelRun()
		t.Fatalf("target relation present during hold: absent=%t err=%v", absent, err)
	}
	f.gate.ReleaseRegistration()
	releaseDeadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(releaseDeadline) && !session.WatcherReady() {
		time.Sleep(20 * time.Millisecond)
	}
	if !session.WatcherReady() {
		_ = targetConn.Close(ctx)
		cancelRun()
		t.Fatal("auth fixture watcher/admission checks never became ready")
	}
	var outcome struct {
		result  recovery.TargetWriterResult
		receipt recovery.DrillTargetProcessReceipt
		err     error
	}
	select {
	case outcome = <-resultCh:
	case <-time.After(150 * time.Second):
		_ = targetConn.Close(ctx)
		cancelRun()
		t.Fatal("auth fixture coordinator run did not return")
	}
	if outcome.err == nil {
		t.Fatal("probe rejection did not fail the coordinator run")
	}
	facts, err := outcome.receipt.ConsumeFacts()
	if err != nil {
		t.Fatalf("auth fixture frozen receipt: %v", err)
	}
	if !facts.Started || !facts.Terminal || facts.Command.Outcome != recovery.PGCommandSucceeded ||
		facts.Command.ExitCode != 0 || !facts.Command.ProcessGroupDrained {
		t.Fatalf("auth fixture native child did not complete with sole-wait drain: %+v", facts)
	}
	if facts.TargetKey != bindingKeyOf(t, f) || facts.RoleFingerprint != f.run.Binding().OriginalRoleFingerprint() || facts.OperationID != f.operation {
		t.Fatal("auth fixture receipt lost the original bound key/role/operation")
	}
	if atomic.LoadInt32(&f.probeCalls) != 1 || atomic.LoadInt32(&f.acceptanceCalls) != 0 {
		t.Fatalf("auth fixture probe/acceptance counts: probe=%d acceptance=%d", f.probeCalls, f.acceptanceCalls)
	}
	var count int
	if err := targetConn.QueryRow(ctx, `SELECT count(*) FROM public.`+pgx.Identifier{f.table}.Sanitize()).Scan(&count); err != nil || count != 3 {
		_ = targetConn.Close(ctx)
		t.Fatalf("auth fixture W restore rows=%d err=%v, want 3", count, err)
	}
	_ = targetConn.Close(ctx)
	var disposition string
	if err := f.controlPool.QueryRow(ctx, `SELECT disposition FROM recovery_target_guard WHERE target_guard_key=$1`, f.guardKey).Scan(&disposition); err != nil {
		t.Fatalf("auth fixture guard state: %v", err)
	}
	if disposition == "clean" {
		t.Fatal("failed auth fixture probe left a clean guard")
	}
	if err := f.lock.Health(ctx); err != nil {
		t.Fatalf("auth fixture original control lock no longer healthy: %v", err)
	}
}

func bindingKeyOf(t *testing.T, f *borrowedAuthHandoffFixture) recovery.TargetKey {
	t.Helper()
	target, err := controlstore.ParseDSNTarget(f.writerTargetDSN)
	if err != nil {
		t.Fatalf("parse W target for binding key: %v", err)
	}
	key, err := recovery.CanonicalTargetKey(target)
	if err != nil {
		t.Fatalf("W target binding key: %v", err)
	}
	return key
}

func TestBorrowedAuthFixtureP0ProtectedSCRAMHelpers(t *testing.T) {
	f := newBorrowedAuthHandoffFixture(t)
	ctx := context.Background()

	// P0: FULL SASL authentication via the owned helper on the direct
	// container-loopback TCP route and on the protected Unix route, before any
	// auth-drain staging. P1 is deliberately not rotated in this lane, and no
	// postmaster STOP or census completion is claimed.
	tcpOutput, err := f.runProtectedAuthCheck(t, "loopback", f.writerRole, f.writerPassword)
	if err != nil || !strings.Contains(tcpOutput, "AUTHCHECK state=AUTH_OK") {
		t.Fatalf("P0 W direct TCP authentication did not complete: err=%v output=%s", err, tcpOutput)
	}
	unixOutput, err := f.runProtectedAuthCheck(t, "unix", f.writerRole, f.writerPassword)
	if err != nil || !strings.Contains(unixOutput, "AUTHCHECK state=AUTH_OK") {
		t.Fatalf("P0 W protected Unix authentication did not complete: err=%v output=%s", err, unixOutput)
	}
	observerOutput, err := f.runProtectedAuthCheck(t, "loopback", f.observerRole, f.observerPass)
	if err != nil || !strings.Contains(observerOutput, "AUTHCHECK state=AUTH_OK") {
		t.Fatalf("P0 observer authentication did not complete: err=%v output=%s", err, observerOutput)
	}
	controlOutput, err := f.runProtectedAuthCheck(t, "loopback", f.controlRole, f.controlPass)
	if err != nil || !strings.Contains(controlOutput, "AUTHCHECK state=AUTH_OK") {
		t.Fatalf("P0 control authentication did not complete: err=%v output=%s", err, controlOutput)
	}

	// Negative: a wrong password must complete the real exchange as a REJECT
	// with SQLSTATE 28P01 — never a trust/fallback success and never merely
	// reaching AuthenticationSASLContinue. The owned static helper classifies
	// the completed exchange and exits zero even for a rejection (only
	// argument/input errors are non-zero), so a successful helper process
	// (err==nil) together with the public REJECT + SQLSTATE classification is
	// the proof; an unexpectedly failed process can never pass even with
	// matching output.
	wrongOutput, wrongErr := f.runProtectedAuthCheck(t, "loopback", f.writerRole, f.writerPassword+"-wrong")
	if wrongErr != nil || !strings.Contains(wrongOutput, "state=REJECT") || !strings.Contains(wrongOutput, "sqlstate=28P01") {
		t.Fatalf("wrong-password helper exchange was not a completed REJECT 28P01 (err=%v): %s", wrongErr, wrongOutput)
	}
	// The same negative through an actual pgx connection: real SASL exchange,
	// real SQLSTATE 28P01.
	wrongDSN := borrowedAuthRoleDSN(t, f.writerTargetDSN, f.writerRole, f.writerPassword+"-wrong")
	conn, connectErr := pgx.Connect(ctx, wrongDSN)
	if connectErr == nil {
		_ = conn.Close(ctx)
		t.Fatal("actual pgx connection accepted the wrong W password")
	}
	var pgErr *pgconn.PgError
	if !errors.As(connectErr, &pgErr) || pgErr.Code != "28P01" {
		t.Fatalf("actual pgx wrong-password error is not SQLSTATE 28P01 (sqlstate=%s)", borrowedAuthSQLState(connectErr))
	}

	// AF01 causal negatives: candidates reconstructed from the PUBLIC W role
	// suffix with the pre-fix timestamp/template derivations, and the fixed
	// inherited administrator credential, must fail real PostgreSQL
	// authentication with 28P01; the new correct protected credentials already
	// completed AUTH_OK above. Candidate values are never printed.
	suffix := strings.TrimPrefix(f.writerRole, "borrowed_auth_w_")
	if suffix == f.writerRole || suffix == "" {
		t.Fatal("W role name does not expose the reconstructible public suffix")
	}
	for _, candidate := range []struct{ what, role, password string }{
		{"old timestamp-derived W candidate", f.writerRole, "borrowed_auth_w_pw_" + suffix},
		{"old timestamp-derived observer candidate", f.observerRole, "borrowed_auth_obs_pw_" + suffix},
		{"old timestamp-derived control candidate", f.controlRole, "borrowed_auth_ctl_pw_" + suffix},
		{"old fixed administrator candidate", f.adminRole, borrowedAuthInheritedAdminPassword},
	} {
		dsn := borrowedAuthRoleDSN(t, f.writerTargetDSN, candidate.role, candidate.password)
		f.assertAuthRejected(t, ctx, dsn, candidate.role, "28P01", candidate.what)
	}

	for _, output := range []string{tcpOutput, unixOutput, observerOutput, controlOutput, wrongOutput} {
		if strings.Contains(output, f.writerPassword) || strings.Contains(output, f.observerPass) ||
			strings.Contains(output, f.controlPass) || strings.Contains(output, f.adminPassword) {
			t.Fatal("auth helper output echoed credential material")
		}
	}
}
