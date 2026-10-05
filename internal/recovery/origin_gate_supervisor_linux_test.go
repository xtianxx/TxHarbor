//go:build linux && drill

// origin_gate_supervisor_linux_test.go binds the continuous-origin gate to the
// real pinned launch+soleWait owner through the OG01 phase-2 sealed API:
// factory origin endpoint + privately provisioned pinned PostgreSQL 18.6
// pg_restore/pg_dump ELFs, armed once per fresh handle with a single-endpoint
// DSN equal to the exact armed listener, and run with a private closed flag
// set. There is no caller args, executable path, role or routing authority.
//
// Scope: test-only transport/admission primitive. No durable target/control
// namespace, guard, probe or acceptance; nothing is marked Restored.
package recovery_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xtianxx/txharbor/internal/recovery"
)

// ---------------------------------------------------------------------------
// Fixture helpers for owned databases, archives and target state.
// ---------------------------------------------------------------------------

func (f *originGateFixture) createOwnedDatabase(ctx context.Context, name string) error {
	_, err := f.admin.Exec(ctx, `CREATE DATABASE `+pgx.Identifier{name}.Sanitize()+` OWNER `+pgx.Identifier{f.role}.Sanitize())
	return err
}

func (f *originGateFixture) dsnAs(database, role, password string) string {
	parsed, err := url.Parse(f.dsn)
	if err != nil {
		return ""
	}
	parsed.User = url.UserPassword(role, password)
	parsed.Path = "/" + database
	query := parsed.Query()
	query.Set("sslmode", "disable")
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

// dsnAsLibpq is the libpq client form: GSS encryption is disabled explicitly
// so the pinned client never starts a GSS handshake against the SCRAM-only
// fixture. This parameter must not be used for pgx connections. It is retained
// for the launch-binding lane's archive helper.
func (f *originGateFixture) dsnAsLibpq(database, role, password string) string {
	return f.dsnAs(database, role, password) + "&gssencmode=disable"
}

func supervisorTableState(t *testing.T, ctx context.Context, conn *pgx.Conn, table string) (bool, int) {
	t.Helper()
	var relations int
	if err := conn.QueryRow(ctx,
		`SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relname=$1`,
		table).Scan(&relations); err != nil {
		t.Fatalf("inspect target relation presence: %v", err)
	}
	if relations == 0 {
		return false, 0
	}
	var count int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM public.`+pgx.Identifier{table}.Sanitize()).Scan(&count); err != nil {
		t.Fatalf("count restored target rows: %v", err)
	}
	return true, count
}

// supervisorArchiveFromTools creates a real custom-format archive with the
// sealed pinned pg_dump path. The dump DSN carries the credential only through
// the protected anonymous passfile path of LocalPGCommand.
func supervisorArchiveFromTools(t *testing.T, ctx context.Context, fx *originGateFixture, tools recovery.DrillNativePGTools, sourceDB, table string, rows int, dataOnly bool) string {
	t.Helper()
	dumpPath, ok := tools.DumpPath()
	if !ok {
		t.Fatalf("sealed native tools expose no pg_dump path")
	}
	symlinkDir := t.TempDir()
	if err := os.Symlink(dumpPath, filepath.Join(symlinkDir, "pg_dump")); err != nil {
		t.Fatalf("stage sealed pg_dump for LocalPGCommand: %v", err)
	}
	t.Setenv("PATH", symlinkDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	sourceDSN := fx.dsnAsLibpq(sourceDB, fx.role, fx.password)
	source, err := pgx.Connect(ctx, fx.dsnAs(sourceDB, fx.role, fx.password))
	if err != nil {
		t.Fatalf("connect restricted W source database: %v", err)
	}
	if _, err := source.Exec(ctx, `CREATE TABLE `+pgx.Identifier{table}.Sanitize()+` (id int PRIMARY KEY, note text NOT NULL)`); err != nil {
		source.Close(context.Background())
		t.Fatalf("create restricted W source table: %v", err)
	}
	if _, err := source.Exec(ctx, `INSERT INTO `+pgx.Identifier{table}.Sanitize()+` (id, note) SELECT g, 'supervised-origin-' || g FROM generate_series(1, $1) AS g`, rows); err != nil {
		source.Close(context.Background())
		t.Fatalf("seed restricted W source table: %v", err)
	}
	if err := source.Close(ctx); err != nil {
		t.Fatalf("close source session: %v", err)
	}
	archivePath := filepath.Join(t.TempDir(), table+".dump")
	archive, err := os.Create(archivePath)
	if err != nil {
		t.Fatalf("create archive file: %v", err)
	}
	args := []string{"-Fc", "--no-owner", "--no-privileges"}
	if dataOnly {
		args = append(args, "--data-only")
	}
	args = append(args, "--dbname="+sourceDSN)
	if err := (recovery.LocalPGCommand{}).Run(ctx, "pg_dump", args, nil, archive, &bytes.Buffer{}); err != nil {
		_ = archive.Close()
		t.Fatalf("sealed pinned pg_dump failed: %v", err)
	}
	if err := archive.Close(); err != nil {
		t.Fatalf("close archive: %v", err)
	}
	info, err := os.Stat(archivePath)
	if err != nil || info.Size() == 0 {
		t.Fatalf("sealed pinned pg_dump produced no archive (err=%v)", err)
	}
	return archivePath
}

func armedGateDSN(gate *originGate, database, role, password string) string {
	return fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable&gssencmode=disable", role, password, gate.Endpoint(), database)
}

func supervisorRegisteredBackendWaiting(t *testing.T, ctx context.Context, fx *originGateFixture, pid int) error {
	t.Helper()
	return originGateWaitFor(ctx, "registered backend waiting on the real lock", func() bool {
		var waitType *string
		if err := fx.admin.QueryRow(ctx, `SELECT wait_event_type FROM pg_stat_activity WHERE pid=$1`, pid).Scan(&waitType); err != nil {
			return false
		}
		return waitType != nil && *waitType == "Lock"
	})
}

func supervisorOpenArchive(t *testing.T, path string) *os.File {
	t.Helper()
	archive, err := os.Open(path)
	if err != nil {
		t.Fatalf("open archive: %v", err)
	}
	t.Cleanup(func() { _ = archive.Close() })
	return archive
}

// ---------------------------------------------------------------------------
// Tests.
// ---------------------------------------------------------------------------

// TestDrillOriginGateSupervisorNativePgRestoreOwnedOrigin is the adopted
// native positive: the sealed arm/run path launches the genuine pinned
// pg_restore behind the exact factory endpoint; the gate holds the first
// executable frame until protected registration, then the real archive
// restores its rows and the session retires only on sole-wait terminal plus
// authoritative drain.
func TestDrillOriginGateSupervisorNativePgRestoreOwnedOrigin(t *testing.T) {
	detail := "armed factory endpoint plus sealed pinned tools; genuine custom archive restored behind the held first frame"
	recordOriginGateResult(t, "TestDrillOriginGateSupervisorNativePgRestoreOwnedOrigin", &detail)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	fx := newOriginGateFixture(t)
	tools, err := recovery.DrillProvisionNativePGTools(t)
	if err != nil {
		t.Fatalf("provision sealed native tools: %v", err)
	}
	nano := time.Now().UnixNano()
	sourceDB := fmt.Sprintf("origin_sup_src_%d", nano)
	targetDB := fmt.Sprintf("origin_sup_tgt_%d", nano)
	table := "origin_supervisor_rows"
	if err := fx.createOwnedDatabase(ctx, sourceDB); err != nil {
		t.Fatalf("create restricted W source database: %v", err)
	}
	if err := fx.createOwnedDatabase(ctx, targetDB); err != nil {
		t.Fatalf("create restricted W target database: %v", err)
	}
	archivePath := supervisorArchiveFromTools(t, ctx, fx, tools, sourceDB, table, 3, false)
	gate := newOriginGate(t, ctx, fx)
	gate.HoldRegistration()
	handle := recovery.DrillNewTargetProcessObservation()
	if err := gate.ArmObserved(handle, tools, recovery.DrillPGRestoreArmOptions{
		TargetDSN:   armedGateDSN(gate, targetDB, fx.role, fx.password),
		OperationID: fmt.Sprintf("origin-supervisor-%d", nano),
	}); err != nil {
		t.Fatalf("arm the supervised native restore: %v", err)
	}
	var stdout, stderr bytes.Buffer
	runner := recovery.TargetProcessRunner{DrainTimeout: 15 * time.Second}
	runDone := make(chan struct{})
	var result recovery.PGCommandResult
	var runErr error
	go func() {
		defer close(runDone)
		result, runErr = recovery.DrillRunObservedPGRestore(ctx, runner, handle, supervisorOpenArchive(t, archivePath), &stdout, &stderr)
	}()
	if err := handle.AwaitBound(ctx); err != nil {
		t.Fatalf("armed operation was never bound: %v", err)
	}
	session, err := gate.AdmitObserved(ctx, handle)
	if err != nil {
		t.Fatalf("admit the armed supervised origin: %v", err)
	}
	identity := handle.StartedIdentity()
	if !identity.Started || identity.PID <= 0 || identity.StartID == 0 || identity.StartErr != nil {
		t.Fatalf("live origin start identity is not usable: %+v", identity)
	}
	if err := originGateWaitFor(ctx, "gate to hold the first executable frame", func() bool {
		return session.BufferedFrames() >= 1
	}); err != nil {
		t.Fatalf("%v", err)
	}
	if _, registered := session.Registration(); registered {
		t.Fatalf("registration completed before the test released it")
	}
	if !session.ServerAuthOK() {
		t.Fatalf("gate never observed the genuine server AuthenticationOk for the armed child")
	}
	report := session.Report()
	if !report.Supervisor || report.ObservedPID != identity.PID || report.ObservedStartID != identity.StartID {
		t.Fatalf("session is not bound to the real supervised child: %+v identity=%+v", report, identity)
	}
	if report.BoundDatabase != targetDB || report.BoundRole != fx.role {
		t.Fatalf("session bound identity is not the armed operation identity: db=%q role=%q", report.BoundDatabase, report.BoundRole)
	}
	if gate.SupervisorOriginCount() != 0 {
		t.Fatalf("publisher origin registry published an origin before registration completed")
	}
	targetConn, err := pgx.Connect(ctx, fx.dsnAs(targetDB, "txharbor", "txharbor"))
	if err != nil {
		t.Fatalf("connect target database for barrier verification: %v", err)
	}
	defer targetConn.Close(context.Background())
	if exists, count := supervisorTableState(t, ctx, targetConn, table); exists || count != 0 {
		t.Fatalf("target changed while the first executable frame was held: exists=%t rows=%d", exists, count)
	}
	t.Logf("barrier held: buffered=%d AuthenticationOk=%t target absent; armed child PID %d start %d", session.BufferedFrames(), session.ServerAuthOK(), identity.PID, identity.StartID)

	gate.ReleaseRegistration()
	registration, err := originGateWaitRegistration(ctx, session)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if gate.SupervisorOriginCount() != 1 {
		t.Fatalf("publisher origin registry does not hold exactly one published origin: %d", gate.SupervisorOriginCount())
	}
	if err := originGateWaitFor(ctx, "native archive data restore", func() bool {
		exists, count := supervisorTableState(t, ctx, targetConn, table)
		return exists && count == 3
	}); err != nil {
		t.Fatalf("%v (stderr bytes=%d)", err, stderr.Len())
	}
	select {
	case <-runDone:
	case <-ctx.Done():
		t.Fatalf("armed native restore did not return: %v", ctx.Err())
	}
	if runErr != nil {
		t.Fatalf("armed native pg_restore run failed: %v", runErr)
	}
	if result.Outcome != recovery.PGCommandSucceeded || !result.Started || !result.ProcessGroupDrained || result.ExitCode != 0 {
		t.Fatalf("armed native pg_restore did not complete with a proven process-group drain: %+v", result)
	}
	terminal := handle.WaitTerminal()
	if !terminal.Terminal || !terminal.ChildWaitCompleted || terminal.WaitExitCode != 0 {
		t.Fatalf("sole cmd.Wait terminal facts are not authentic: %+v", terminal)
	}
	if err := supervisorChildReapError(identity.PID, identity.StartID, 5*time.Second); err != nil {
		t.Fatalf("armed child reaping verdict after the sole wait: %v", err)
	}
	if err := originGateWaitFor(ctx, "drain-verified retirement", func() bool {
		state := gate.DrainState()
		return state.Verified && !state.Unknown
	}); err != nil {
		t.Fatalf("%v (state %+v)", err, gate.DrainState())
	}
	if latched, reason := gate.Latched(); latched {
		t.Fatalf("gate latched during a clean armed restore: %s", reason)
	}
	if _, err := handle.ClaimForEndpoint(gate.endpoint); err == nil || !strings.Contains(err.Error(), "claimed") {
		t.Fatalf("replayed armed origin handle was not denied: %v", err)
	}
	detail = fmt.Sprintf("factory endpoint %s armed with sealed pinned pg_restore 18.6 (PID %d start %d, db=%s op=%s); accepted peer %s:%d bound to its own fd socket inode %s; first executable frame held with the target relation absent; backend PID %d registered; real archive restored 3 rows; ExitCode 0, group drained, sole-wait terminal 0, PID reaped, drain verified, replay denied, gate not latched",
		gate.Endpoint(), identity.PID, identity.StartID, targetDB, fmt.Sprintf("origin-supervisor-%d", nano),
		report.PeerIP, report.PeerPort, report.OriginInode, registration.PID)
}

// supervisorChildReapError is the integrated strict reaping verdict of the
// armed native positive. It returns nil only when the preserved OG05
// classifier proves the exact start identity authoritatively gone: an ENOENT
// or a successfully read, different start ID. EACCES, EIO, malformed or
// partial reads are refused with their cause, a same-identity zombie or live
// process stays not-gone, and timeout expiry is never reaping evidence. The
// positive call site consumes this verdict directly; no scalar boolean can
// bypass it.
func supervisorChildReapError(pid int, startID uint64, timeout time.Duration) error {
	return originSupervisorReapError(pid, startID, timeout)
}

// TestDrillOriginGateSupervisorRejectsInertAndWrongIdentity proves the armed
// authority cannot be forged, swapped, replayed or pointed at anything except
// the exact armed listener, and that a legitimate protocol-layer wrong startup
// is refused before any SQL without pretending to be the native path.
func TestDrillOriginGateSupervisorRejectsInertAndWrongIdentity(t *testing.T) {
	detail := "refusing forged endpoints/handles, wrong or other-gate DSNs, arm replay, closed listeners, altered tools and a legitimate wrong startup"
	recordOriginGateResult(t, "TestDrillOriginGateSupervisorRejectsInertAndWrongIdentity", &detail)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	fx := newOriginGateFixture(t)
	nano := time.Now().UnixNano()
	sourceDB := fmt.Sprintf("origin_sup_rej_src_%d", nano)
	targetDB := fmt.Sprintf("origin_sup_rej_tgt_%d", nano)
	expectedOtherDB := fmt.Sprintf("origin_sup_rej_other_%d", nano)
	for _, name := range []string{sourceDB, targetDB, expectedOtherDB} {
		if err := fx.createOwnedDatabase(ctx, name); err != nil {
			t.Fatalf("create owned database %s: %v", name, err)
		}
	}
	tools, err := recovery.DrillProvisionNativePGTools(t)
	if err != nil {
		t.Fatalf("provision sealed native tools: %v", err)
	}
	archivePath := supervisorArchiveFromTools(t, ctx, fx, tools, sourceDB, "origin_supervisor_rej_rows", 2, false)
	gate := newOriginGate(t, ctx, fx)
	otherGate := newOriginGate(t, ctx, fx)

	// Inert handle, inert forged endpoint JSON.
	var zeroHandle recovery.DrillTargetProcessObservation
	if zeroHandle.Valid() {
		t.Fatalf("zero-value observation handle reports itself valid")
	}
	if _, err := zeroHandle.ClaimOrigin(); err == nil {
		t.Fatalf("zero-value handle issued an origin")
	}
	serialized, err := json.Marshal(gate.endpoint)
	if err != nil || string(serialized) != "{}" {
		t.Fatalf("origin endpoint serialized authority material: %v %q", err, serialized)
	}
	var forgedEndpoint recovery.DrillOriginEndpoint
	if err := json.Unmarshal([]byte(`{"listener":{"addr":"127.0.0.1:1"},"addr":"127.0.0.1:1","inode":7}`), &forgedEndpoint); err != nil {
		t.Fatalf("JSON round trip of an inert endpoint: %v", err)
	}
	if forgedEndpoint.Valid() {
		t.Fatalf("JSON document constructed a live origin endpoint capability")
	}
	if err := recovery.DrillArmObservedPGRestore(recovery.DrillNewTargetProcessObservation(), forgedEndpoint, tools,
		recovery.DrillPGRestoreArmOptions{TargetDSN: armedGateDSN(gate, targetDB, fx.role, fx.password), OperationID: "forged-endpoint"}); err == nil {
		t.Fatalf("forged endpoint capability was accepted for arm")
	}
	if latched, reason := gate.Latched(); latched {
		t.Fatalf("inert input refusals latched the gate: %s", reason)
	}

	// Arm authority: direct server DSN, other live gate DSN, unsupported routing.
	directHit := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable&gssencmode=disable", fx.role, fx.password, fx.serverAddr, targetDB)
	if err := gate.ArmObserved(recovery.DrillNewTargetProcessObservation(), tools,
		recovery.DrillPGRestoreArmOptions{TargetDSN: directHit, OperationID: "direct-server"}); err == nil {
		t.Fatalf("arm accepted a direct protected-server DSN instead of the exact armed listener")
	}
	otherGateHit := armedGateDSN(otherGate, targetDB, fx.role, fx.password)
	if err := gate.ArmObserved(recovery.DrillNewTargetProcessObservation(), tools,
		recovery.DrillPGRestoreArmOptions{TargetDSN: otherGateHit, OperationID: "other-gate"}); err == nil {
		t.Fatalf("arm accepted the DSN of a different live gate listener")
	}
	unsupported := armedGateDSN(gate, targetDB, fx.role, fx.password) + "&options=-c%20statement_timeout%3D1"
	if err := gate.ArmObserved(recovery.DrillNewTargetProcessObservation(), tools,
		recovery.DrillPGRestoreArmOptions{TargetDSN: unsupported, OperationID: "unsupported-routing"}); err == nil {
		t.Fatalf("arm accepted unsupported routing parameters")
	}
	if latched, reason := gate.Latched(); latched {
		t.Fatalf("arm refusals latched the gate: %s", reason)
	}

	// Arm replay and endpoint claim swap on separate gates/handles.
	handleA := recovery.DrillNewTargetProcessObservation()
	if err := gate.ArmObserved(handleA, tools, recovery.DrillPGRestoreArmOptions{
		TargetDSN: armedGateDSN(gate, targetDB, fx.role, fx.password), OperationID: "arm-replay",
	}); err != nil {
		t.Fatalf("first arm failed: %v", err)
	}
	if err := gate.ArmObserved(handleA, tools, recovery.DrillPGRestoreArmOptions{
		TargetDSN: armedGateDSN(gate, targetDB, fx.role, fx.password), OperationID: "arm-replay",
	}); err == nil {
		t.Fatalf("second arm on the same handle was accepted")
	}
	if err := gate.ArmObserved(recovery.DrillNewTargetProcessObservation(), tools, recovery.DrillPGRestoreArmOptions{
		TargetDSN: armedGateDSN(gate, targetDB, fx.role, fx.password), OperationID: "second-arm",
	}); err == nil {
		t.Fatalf("second armed restore on the same gate was accepted")
	}
	if _, err := handleA.ClaimForEndpoint(otherGate.endpoint); err == nil {
		t.Fatalf("claim with a swapped endpoint capability was accepted")
	}
	if _, err := otherGate.AdmitObserved(ctx, handleA); err == nil {
		t.Fatalf("a foreign gate admitted a handle armed to another endpoint")
	}
	if _, err := handleA.ClaimOrigin(); err == nil {
		t.Fatalf("plain claim bypassed the endpoint binding")
	}

	// Closed original listener after arm refuses before start.
	closedGate := newOriginGate(t, ctx, fx)
	closedHandle := recovery.DrillNewTargetProcessObservation()
	if err := closedGate.ArmObserved(closedHandle, tools, recovery.DrillPGRestoreArmOptions{
		TargetDSN: armedGateDSN(closedGate, targetDB, fx.role, fx.password), OperationID: "closed-listener",
	}); err != nil {
		t.Fatalf("arm before closing the listener failed: %v", err)
	}
	if err := closedGate.endpoint.Listener().Close(); err != nil {
		t.Fatalf("close the armed listener: %v", err)
	}
	if _, err := recovery.DrillRunObservedPGRestore(ctx, recovery.TargetProcessRunner{}, closedHandle, supervisorOpenArchive(t, archivePath), nil, nil); err == nil {
		t.Fatalf("run started although the armed listener was closed")
	}
	closedCtx, closedCancel := context.WithTimeout(ctx, 3*time.Second)
	_, closedErr := closedGate.AdmitObserved(closedCtx, closedHandle)
	closedCancel()
	if closedErr == nil {
		t.Fatalf("admission succeeded after the armed listener was closed")
	}

	// Altered sealed tool content refuses before start.
	alteredTools, err := recovery.DrillProvisionNativePGTools(t)
	if err != nil {
		t.Fatalf("provision tools for the altered-tool negative: %v", err)
	}
	dumpPath, ok := alteredTools.DumpPath()
	if !ok {
		t.Fatalf("altered-tool tools expose no pg_dump path")
	}
	data, err := os.ReadFile(dumpPath)
	if err != nil {
		t.Fatalf("read sealed pg_dump: %v", err)
	}
	if len(data) == 0 {
		t.Fatalf("sealed pg_dump is empty")
	}
	data[0] ^= 0xff
	if err := os.WriteFile(dumpPath, data, 0o700); err != nil {
		t.Fatalf("alter sealed pg_dump: %v", err)
	}
	alteredGate := newOriginGate(t, ctx, fx)
	alteredHandle := recovery.DrillNewTargetProcessObservation()
	if err := alteredGate.ArmObserved(alteredHandle, alteredTools, recovery.DrillPGRestoreArmOptions{
		TargetDSN: armedGateDSN(alteredGate, targetDB, fx.role, fx.password), OperationID: "altered-tool",
	}); err != nil {
		// Arm-time revalidation may already refuse; run-time refusal is also valid.
		t.Logf("altered tools refused at arm time: %v", err)
	}
	if _, err := recovery.DrillRunObservedPGRestore(ctx, recovery.TargetProcessRunner{}, alteredHandle, supervisorOpenArchive(t, archivePath), nil, nil); err == nil {
		t.Fatalf("run started with altered sealed tool content")
	}

	// Legitimate protocol-layer wrong startup via the fixture-owned helper and
	// the existing gate capability (not the native path).
	wrongStartupGate := newOriginGate(t, ctx, fx)
	launcher := newOriginGateLauncher(t)
	wrongClient, wrongCap, err := launcher.launch(t, ctx, wrongStartupGate,
		[]string{"write", wrongStartupGate.Endpoint(), fx.role, fx.table, "origin-gate-wrong-startup", "none", originGateAppName}, fx.password)
	if err != nil {
		t.Fatalf("spawn wrong-startup fixture client: %v", err)
	}
	wrongCap.expectedRole = fx.role
	wrongCap.expectedDatabase = expectedOtherDB
	if _, err := wrongStartupGate.Admit(ctx, wrongCap); err != nil {
		t.Fatalf("admit wrong-startup fixture origin: %v", err)
	}
	if _, err := wrongClient.waitEvent(ctx, "wrong-startup channel close", func(line string) bool {
		return strings.HasPrefix(line, "ORIGIN_CLIENT_EXIT")
	}); err != nil {
		t.Fatalf("%v", err)
	}
	if err := wrongClient.waitRefusalExit(ctx); err != nil {
		t.Fatalf("wrong-startup client did not exit: %v", err)
	}
	wrongReason, err := originGateWaitLatch(ctx, wrongStartupGate)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if !strings.Contains(wrongReason, "does not match the bound operation identity") {
		t.Fatalf("wrong startup was not refused as a bound-identity mismatch: %s", wrongReason)
	}
	checkConn, err := pgx.Connect(ctx, fx.dsn)
	if err != nil {
		t.Fatalf("connect check session: %v", err)
	}
	defer checkConn.Close(context.Background())
	var wrongSQL int
	if err := checkConn.QueryRow(ctx, `SELECT count(*) FROM `+pgx.Identifier{fx.table}.Sanitize()+` WHERE ref=$1`, "origin-gate-wrong-startup").Scan(&wrongSQL); err != nil || wrongSQL != 0 {
		t.Fatalf("wrong-startup fixture client executed SQL: rows=%d err=%v", wrongSQL, err)
	}
	targetConn, err := pgx.Connect(ctx, fx.dsnAs(targetDB, "txharbor", "txharbor"))
	if err != nil {
		t.Fatalf("connect target database: %v", err)
	}
	defer targetConn.Close(context.Background())
	if exists, _ := supervisorTableState(t, ctx, targetConn, "origin_supervisor_rej_rows"); exists {
		t.Fatalf("refused cases executed SQL in the target database")
	}
	detail = fmt.Sprintf("forged/inert handles and endpoint JSON refused; direct-server, other-live-gate and unsupported-routing DSNs refused before start; arm replay, second arm, endpoint claim swap, plain-claim bypass and foreign-gate admission refused; closed armed listener and altered sealed tools refused before start; legitimate fixture-helper wrong startup latched (%s) with no SQL; target unchanged", wrongReason)
}

// TestDrillOriginGateSupervisorObserverLossDrains proves a genuine
// post-registration observer loss during an in-flight blocked native restore
// terminates the exact backend and drains it: no blocked effect commits after
// the row lock is released.
func TestDrillOriginGateSupervisorObserverLossDrains(t *testing.T) {
	detail := "post-registration observer loss during a blocked sealed native restore must terminate the exact backend and leave no committed effect"
	recordOriginGateResult(t, "TestDrillOriginGateSupervisorObserverLossDrains", &detail)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	fx := newOriginGateFixture(t)
	tools, err := recovery.DrillProvisionNativePGTools(t)
	if err != nil {
		t.Fatalf("provision sealed native tools: %v", err)
	}
	nano := time.Now().UnixNano()
	sourceDB := fmt.Sprintf("origin_sup_obs_src_%d", nano)
	targetDB := fmt.Sprintf("origin_sup_obs_tgt_%d", nano)
	table := "origin_supervisor_obs_rows"
	if err := fx.createOwnedDatabase(ctx, sourceDB); err != nil {
		t.Fatalf("create owned source database: %v", err)
	}
	if err := fx.createOwnedDatabase(ctx, targetDB); err != nil {
		t.Fatalf("create owned target database: %v", err)
	}
	archivePath := supervisorArchiveFromTools(t, ctx, fx, tools, sourceDB, table, 3, true)
	targetAdmin, err := pgx.Connect(ctx, fx.dsnAs(targetDB, "txharbor", "txharbor"))
	if err != nil {
		t.Fatalf("connect supervised target database: %v", err)
	}
	defer targetAdmin.Close(context.Background())
	if _, err := targetAdmin.Exec(ctx, `CREATE TABLE `+pgx.Identifier{table}.Sanitize()+` (id int PRIMARY KEY, note text NOT NULL)`); err != nil {
		t.Fatalf("pre-create target table: %v", err)
	}
	lockTx, err := targetAdmin.Begin(ctx)
	if err != nil {
		t.Fatalf("begin target lock transaction: %v", err)
	}
	defer func() { _ = lockTx.Rollback(context.Background()) }()
	if _, err := lockTx.Exec(ctx, `LOCK TABLE `+pgx.Identifier{table}.Sanitize()+` IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatalf("hold target table lock: %v", err)
	}
	gate := newOriginGate(t, ctx, fx)
	handle := recovery.DrillNewTargetProcessObservation()
	if err := gate.ArmObserved(handle, tools, recovery.DrillPGRestoreArmOptions{
		TargetDSN:   armedGateDSN(gate, targetDB, fx.role, fx.password),
		OperationID: fmt.Sprintf("origin-supervisor-observer-loss-%d", nano),
	}); err != nil {
		t.Fatalf("arm observer-loss restore: %v", err)
	}
	var stderr bytes.Buffer
	runDone := make(chan struct{})
	var result recovery.PGCommandResult
	var runErr error
	go func() {
		defer close(runDone)
		result, runErr = recovery.DrillRunObservedPGRestore(ctx, recovery.TargetProcessRunner{DrainTimeout: 15 * time.Second}, handle, supervisorOpenArchive(t, archivePath), nil, &stderr)
	}()
	if err := handle.AwaitBound(ctx); err != nil {
		t.Fatalf("observer-loss operation was never bound: %v", err)
	}
	session, err := gate.AdmitObserved(ctx, handle)
	if err != nil {
		t.Fatalf("admit observer-loss origin: %v", err)
	}
	registration, err := originGateWaitRegistration(ctx, session)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if err := supervisorRegisteredBackendWaiting(t, ctx, fx, registration.PID); err != nil {
		t.Fatalf("%v (stderr bytes=%d)", err, stderr.Len())
	}
	var terminated bool
	if err := fx.admin.QueryRow(ctx, `SELECT pg_terminate_backend($1)`, gate.observerPID).Scan(&terminated); err != nil || !terminated {
		t.Fatalf("terminate observer: terminated=%t err=%v", terminated, err)
	}
	if err := originGateWaitFor(ctx, "observer-loss latch", func() bool {
		latched, _ := gate.Latched()
		return latched
	}); err != nil {
		t.Fatalf("%v", err)
	}
	if err := originGateWaitFor(ctx, "observer-loss drain", func() bool {
		return gate.DrainState().Verified
	}); err != nil {
		t.Fatalf("%v (state %+v)", err, gate.DrainState())
	}
	select {
	case <-runDone:
	case <-ctx.Done():
		t.Fatalf("observer-loss restore did not return: %v", ctx.Err())
	}
	if runErr == nil || result.Outcome == recovery.PGCommandSucceeded {
		t.Fatalf("observer-loss restore reported success: %+v err=%v", result, runErr)
	}
	if err := lockTx.Rollback(ctx); err != nil {
		t.Fatalf("release target lock: %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	if exists, count := supervisorTableState(t, ctx, targetAdmin, table); !exists || count != 0 {
		t.Fatalf("observer-loss blocked effect committed after lock release: exists=%t rows=%d", exists, count)
	}
	if _, err := handle.ClaimForEndpoint(gate.endpoint); err == nil || !strings.Contains(err.Error(), "claimed") {
		t.Fatalf("used observer-loss handle was not denied on replay: %v", err)
	}
	successor := recovery.DrillNewTargetProcessObservation()
	if err := gate.ArmObserved(successor, tools, recovery.DrillPGRestoreArmOptions{
		TargetDSN: armedGateDSN(gate, targetDB, fx.role, fx.password), OperationID: "observer-loss-successor",
	}); err == nil {
		t.Fatalf("successor arm was accepted after the observer-loss latch")
	}
	detail = fmt.Sprintf("blocked sealed native restore on backend PID %d; observer loss latched (%s) and drained (%s); zero committed rows after lock release; replay and successor refused", registration.PID, reasonOf(gate), gate.DrainState().Report)
}
