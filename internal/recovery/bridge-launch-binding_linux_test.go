//go:build linux && drill

// bridge-launch-binding_linux_test.go proves the OG01 Phase 1 arming and launch
// contract: the factory listener and sealed tools are the only authority, the
// arm step refuses every non-listener route before any start, and the run step
// constructs its child only from the private arm configuration.
package recovery_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xtianxx/txharbor/internal/recovery"
)

func TestDrillLegacyObservedCommandIsPermanentlyClosed(t *testing.T) {
	handle := recovery.DrillNewTargetProcessObservation()
	result, err := recovery.DrillRunObservedPGCommand(context.Background(), recovery.TargetProcessRunner{}, handle,
		recovery.DrillObservedRunnerOptions{TargetDSN: "postgres://user:pass@127.0.0.1:1/db", ExpectedRole: "user", OperationID: "legacy", Executable: "/bin/pg_restore"},
		nil, nil, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "permanently closed") {
		t.Fatalf("legacy observed command is still usable: %+v err=%v", result, err)
	}
	if result.Started || result.Outcome != recovery.PGCommandNotStarted {
		t.Fatalf("legacy observed command reported a start: %+v", result)
	}
	if _, bound := handle.BoundOperation(); bound {
		t.Fatal("legacy observed command bound an operation on the handle")
	}
}

func TestDrillOriginEndpointFactoryRetainsListenerIdentity(t *testing.T) {
	endpoint := mustOpenDrillOriginEndpoint(t)
	if endpoint.Addr() == "" || endpoint.Listener() == nil {
		t.Fatalf("factory endpoint is not backed by an actual listener: addr=%q", endpoint.Addr())
	}
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := endpoint.Listener().Accept()
		if err == nil {
			accepted <- conn
		}
	}()
	dialed, err := net.Dial("tcp", endpoint.Addr())
	if err != nil {
		t.Fatalf("factory listener does not accept real TCP connections: %v", err)
	}
	defer dialed.Close()
	select {
	case held := <-accepted:
		defer held.Close()
	case <-time.After(5 * time.Second):
		t.Fatal("factory listener accepted no real connection")
	}

	// Neither JSON nor a copied scalar address carries the capability.
	serialized, err := json.Marshal(endpoint)
	if err != nil || string(serialized) != "{}" {
		t.Fatalf("endpoint serialized authority material: err=%v doc=%q", err, serialized)
	}
	var forged recovery.DrillOriginEndpoint
	if err := json.Unmarshal([]byte(`{"listener":{},"addr":"127.0.0.1:1","inode":1}`), &forged); err != nil {
		t.Fatalf("forged endpoint document: %v", err)
	}
	if forged.Valid() {
		t.Fatal("JSON document constructed a live origin endpoint")
	}
}

func TestDrillArmObservedPGRestoreRefusesBeforeStart(t *testing.T) {
	fx := newOriginGateFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	tools := mustProvisionDrillNativePGTools(t)
	nano := time.Now().UnixNano()
	targetDB := fmt.Sprintf("origin_arm_refuse_%d", nano)
	table := fmt.Sprintf("origin_arm_rows_%d", nano)
	if err := fx.createOwnedDatabase(ctx, targetDB); err != nil {
		t.Fatalf("create arm-refusal target database: %v", err)
	}
	targetConn, err := pgx.Connect(ctx, fx.dsnAs(targetDB, "txharbor", "txharbor"))
	if err != nil {
		t.Fatalf("connect arm-refusal target: %v", err)
	}
	defer targetConn.Close(context.Background())
	if _, err := targetConn.Exec(ctx, `CREATE TABLE public.`+pgx.Identifier{table}.Sanitize()+` (id int PRIMARY KEY, note text NOT NULL)`); err != nil {
		t.Fatalf("seed arm-refusal target table: %v", err)
	}
	if _, err := targetConn.Exec(ctx, `INSERT INTO public.`+pgx.Identifier{table}.Sanitize()+` (id, note) VALUES (1, 'unchanged')`); err != nil {
		t.Fatalf("seed arm-refusal target row: %v", err)
	}
	rowCount := func() int {
		var count int
		if err := targetConn.QueryRow(ctx, `SELECT count(*) FROM public.`+pgx.Identifier{table}.Sanitize()).Scan(&count); err != nil {
			t.Fatalf("count arm-refusal rows: %v", err)
		}
		return count
	}

	endpoint := mustOpenDrillOriginEndpoint(t)
	alternate := mustOpenDrillOriginEndpoint(t)
	closed := mustOpenDrillOriginEndpoint(t)
	if err := closed.Listener().Close(); err != nil {
		t.Fatalf("close origin endpoint for the closed-listener case: %v", err)
	}
	validDSN := func(ep recovery.DrillOriginEndpoint) string {
		return fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable&gssencmode=disable", fx.role, fx.password, ep.Addr(), targetDB)
	}
	validDSNExtra := func(extra string) string {
		return fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable&gssencmode=disable&%s", fx.role, fx.password, endpoint.Addr(), targetDB, extra)
	}
	validDSNRaw := func(rawQuery string) string {
		return fmt.Sprintf("postgres://%s:%s@%s/%s?%s", fx.role, fx.password, endpoint.Addr(), targetDB, rawQuery)
	}
	directServerDSN := fx.dsnAs(targetDB, "txharbor", "txharbor")

	cases := []struct {
		name     string
		endpoint recovery.DrillOriginEndpoint
		tools    recovery.DrillNativePGTools
		dsn      string
		op       string
	}{
		{"direct server DSN is not the armed listener", endpoint, tools, directServerDSN, "arm-direct"},
		{"alternate real listener is not the armed listener", endpoint, tools, validDSN(alternate), "arm-alternate"},
		{"armed listener is not the alternate DSN endpoint", alternate, tools, validDSN(endpoint), "arm-alternate-reverse"},
		{"closed listener refuses", closed, tools, validDSN(closed), "arm-closed"},
		{"service query routing refuses", endpoint, tools, validDSNExtra("service=carrier"), "arm-service"},
		{"keyword service DSN refuses", endpoint, tools, "service=carrier dbname=" + targetDB, "arm-keyword"},
		{"passfile routing refuses", endpoint, tools, validDSNExtra("passfile=/tmp/forged"), "arm-passfile"},
		{"multi-host routing refuses", endpoint, tools, fmt.Sprintf("postgres://%s:%s@%s,%s/%s", fx.role, fx.password, endpoint.Addr(), alternate.Addr(), targetDB), "arm-multihost"},
		{"unix socket routing refuses", endpoint, tools, "postgres://" + fx.role + ":" + fx.password + "@/db?host=/var/run/postgresql", "arm-unix"},
		{"hostaddr routing refuses", endpoint, tools, validDSNExtra("hostaddr=127.0.0.1"), "arm-hostaddr"},
		{"load-balance routing refuses", endpoint, tools, validDSNExtra("load_balance_hosts=random"), "arm-loadbalance"},
		{"invalid port refuses", endpoint, tools, fmt.Sprintf("postgres://%s:%s@127.0.0.1:70000/%s", fx.role, fx.password, targetDB), "arm-port"},
		{"empty operation refuses", endpoint, tools, validDSN(endpoint), ""},
		{"empty role refuses", endpoint, tools, "postgres://:pass@" + endpoint.Addr() + "/" + targetDB, "arm-norole"},
		{"empty database refuses", endpoint, tools, "postgres://" + fx.role + ":" + fx.password + "@" + endpoint.Addr() + "/", "arm-nodb"},
		{"zero endpoint refuses", recovery.DrillOriginEndpoint{}, tools, validDSN(endpoint), "arm-zero-endpoint"},
		{"zero tools refuse", endpoint, recovery.DrillNativePGTools{}, validDSN(endpoint), "arm-zero-tools"},
		{"malformed percent encoding refuses", endpoint, tools, validDSNRaw("sslmode=%zz"), "arm-badpercent"},
		{"raw semicolon separator refuses", endpoint, tools, validDSNRaw("sslmode=disable;gssencmode=disable"), "arm-semicolon"},
		{"duplicate sslmode refuses", endpoint, tools, validDSNRaw("sslmode=disable&sslmode=require"), "arm-dup-sslmode"},
		{"duplicate encoded name refuses", endpoint, tools, validDSNRaw("sslmode=disable&%73slmode=disable"), "arm-dup-encoded"},
		{"empty supported value refuses", endpoint, tools, validDSNRaw("sslmode="), "arm-empty-sslmode"},
		{"duplicate gssencmode refuses", endpoint, tools, validDSNRaw("gssencmode=disable&gssencmode=disable"), "arm-dup-gss"},
		{"duplicate empty gssencmode refuses", endpoint, tools, validDSNRaw("gssencmode=&gssencmode=disable"), "arm-dup-empty-gss"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			handle := recovery.DrillNewTargetProcessObservation()
			if err := recovery.DrillArmObservedPGRestore(handle, tc.endpoint, tc.tools, recovery.DrillPGRestoreArmOptions{TargetDSN: tc.dsn, OperationID: tc.op}); err == nil {
				t.Fatalf("%s: arm accepted a non-authoritative input", tc.name)
			}
			result, err := recovery.DrillRunObservedPGRestore(ctx, recovery.TargetProcessRunner{}, handle, nil, nil, nil)
			if err == nil || result.Started || result.Outcome != recovery.PGCommandNotStarted {
				t.Fatalf("%s: refused input still started a run: %+v err=%v", tc.name, result, err)
			}
		})
	}

	// Inert/zero and JSON-forged handles can never arm.
	var forgedHandle recovery.DrillTargetProcessObservation
	if err := json.Unmarshal([]byte(`{"observation":{"pid":7},"state":{}}`), &forgedHandle); err != nil {
		t.Fatalf("forged handle document: %v", err)
	}
	if err := recovery.DrillArmObservedPGRestore(recovery.DrillTargetProcessObservation{}, endpoint, tools, recovery.DrillPGRestoreArmOptions{TargetDSN: validDSN(endpoint), OperationID: "arm-zero-handle"}); err == nil {
		t.Fatal("zero handle was armed")
	}
	if err := recovery.DrillArmObservedPGRestore(forgedHandle, endpoint, tools, recovery.DrillPGRestoreArmOptions{TargetDSN: validDSN(endpoint), OperationID: "arm-forged-handle"}); err == nil {
		t.Fatal("JSON-forged handle was armed")
	}

	// Once armed, no swap or replay: a second arm with another endpoint is
	// refused and the original binding is unchanged.
	armed := recovery.DrillNewTargetProcessObservation()
	if err := recovery.DrillArmObservedPGRestore(armed, endpoint, tools, recovery.DrillPGRestoreArmOptions{TargetDSN: validDSN(endpoint), OperationID: "arm-once"}); err != nil {
		t.Fatalf("initial arm failed: %v", err)
	}
	if err := recovery.DrillArmObservedPGRestore(armed, alternate, tools, recovery.DrillPGRestoreArmOptions{TargetDSN: validDSN(alternate), OperationID: "arm-swap"}); err == nil {
		t.Fatal("armed handle accepted a swap")
	}
	bound, ok := armed.BoundOperation()
	if !ok || bound.OperationID != "arm-once" || bound.Database != targetDB || bound.Role != fx.role || bound.Host != "127.0.0.1" {
		t.Fatalf("armed binding changed or is incomplete: %+v ok=%t", bound, ok)
	}
	if bound.Port == 0 || fmt.Sprintf("127.0.0.1:%d", bound.Port) != endpoint.Addr() {
		t.Fatalf("armed binding port does not match the factory listener: %+v", bound)
	}

	// Nothing started: the seeded target rows are exactly unchanged.
	if got := rowCount(); got != 1 {
		t.Fatalf("refused arm/run attempts changed target rows: count=%d want=1", got)
	}
}

func TestDrillRunObservedPGRestoreRefusesAmbientPGRoute(t *testing.T) {
	tools := mustProvisionDrillNativePGTools(t)
	endpoint := mustOpenDrillOriginEndpoint(t)
	handle := recovery.DrillNewTargetProcessObservation()
	opts := recovery.DrillPGRestoreArmOptions{
		TargetDSN:   "postgres://txharbor:txharbor@" + endpoint.Addr() + "/origin_ambient_route?sslmode=disable",
		OperationID: "ambient-route",
	}
	if err := recovery.DrillArmObservedPGRestore(handle, endpoint, tools, opts); err != nil {
		t.Fatalf("arm before the ambient-route attempt: %v", err)
	}
	t.Setenv("PGHOST", "ambient-route-canary")
	result, err := recovery.DrillRunObservedPGRestore(context.Background(), recovery.TargetProcessRunner{}, handle, nil, nil, nil)
	if err == nil || result.Started || result.Outcome != recovery.PGCommandNotStarted {
		t.Fatalf("ambient PG route was not refused before start: %+v err=%v", result, err)
	}
}

func TestDrillRunObservedPGRestoreBarrierHoldsWithoutSQL(t *testing.T) {
	fx := newOriginGateFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	drillUnsetAmbientPGRoute(t)
	tools := mustProvisionDrillNativePGTools(t)
	dumpPath, ok := tools.DumpPath()
	if !ok {
		t.Fatal("sealed tools did not expose the dump path diagnostic")
	}
	t.Setenv("PATH", filepath.Dir(dumpPath)+string(os.PathListSeparator)+os.Getenv("PATH"))

	nano := time.Now().UnixNano()
	sourceDB := fmt.Sprintf("origin_bridge_src_%d", nano)
	targetDB := fmt.Sprintf("origin_bridge_tgt_%d", nano)
	table := fmt.Sprintf("origin_bridge_rows_%d", nano)
	if err := fx.createOwnedDatabase(ctx, sourceDB); err != nil {
		t.Fatalf("create bridge source database: %v", err)
	}
	if err := fx.createOwnedDatabase(ctx, targetDB); err != nil {
		t.Fatalf("create bridge target database: %v", err)
	}
	sourceConn, err := pgx.Connect(ctx, fx.dsnAs(sourceDB, fx.role, fx.password))
	if err != nil {
		t.Fatalf("connect bridge source: %v", err)
	}
	defer sourceConn.Close(context.Background())
	if _, err := sourceConn.Exec(ctx, `CREATE TABLE public.`+pgx.Identifier{table}.Sanitize()+` (id int PRIMARY KEY, note text NOT NULL)`); err != nil {
		t.Fatalf("create bridge source table: %v", err)
	}
	if _, err := sourceConn.Exec(ctx, `INSERT INTO public.`+pgx.Identifier{table}.Sanitize()+` (id, note) VALUES (1, 'bridge'), (2, 'barrier')`); err != nil {
		t.Fatalf("seed bridge source rows: %v", err)
	}
	archivePath := filepath.Join(t.TempDir(), "origin-bridge.dump")
	archiveFile, err := os.Create(archivePath)
	if err != nil {
		t.Fatalf("create bridge archive file: %v", err)
	}
	var dumpDiagnostics bytes.Buffer
	if err := (recovery.LocalPGCommand{}).Run(ctx, "pg_dump",
		[]string{"-Fc", "--no-owner", "--no-privileges", "--dbname=" + fx.dsnAsLibpq(sourceDB, fx.role, fx.password)},
		nil, archiveFile, &dumpDiagnostics); err != nil {
		_ = archiveFile.Close()
		diagnostic := strings.ToLower(dumpDiagnostics.String())
		t.Fatalf("real pinned pg_dump failed: %v (stderr_connection=%t stderr_permission=%t stderr_table=%t stderr_pgclient=%t)",
			err, strings.Contains(diagnostic, "connection"), strings.Contains(diagnostic, "permission"),
			strings.Contains(diagnostic, "table"), strings.Contains(diagnostic, "pg_dump: error"))
	}
	if err := archiveFile.Close(); err != nil {
		t.Fatalf("close bridge archive: %v", err)
	}
	if info, err := os.Stat(archivePath); err != nil || info.Size() == 0 {
		t.Fatalf("bridge archive is empty: err=%v", err)
	}
	targetConn, err := pgx.Connect(ctx, fx.dsnAs(targetDB, "txharbor", "txharbor"))
	if err != nil {
		t.Fatalf("connect bridge target: %v", err)
	}
	defer targetConn.Close(context.Background())

	endpoint := mustOpenDrillOriginEndpoint(t)
	hold := make(chan struct{})
	defer close(hold)
	accepted := make(chan net.Conn, 1)
	go func() {
		for {
			conn, err := endpoint.Listener().Accept()
			if err != nil {
				return
			}
			select {
			case accepted <- conn:
			default:
				_ = conn.Close()
			}
		}
	}()

	handle := recovery.DrillNewTargetProcessObservation()
	dsn := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable&gssencmode=disable", fx.role, fx.password, endpoint.Addr(), targetDB)
	operationID := fmt.Sprintf("origin-bridge-%d", nano)
	if err := recovery.DrillArmObservedPGRestore(handle, endpoint, tools, recovery.DrillPGRestoreArmOptions{TargetDSN: dsn, OperationID: operationID}); err != nil {
		t.Fatalf("arm bridge restore: %v", err)
	}
	bound, ok := handle.BoundOperation()
	if !ok || bound.OperationID != operationID || bound.Database != targetDB || bound.Role != fx.role {
		t.Fatalf("bridge arm binding is incomplete: %+v ok=%t", bound, ok)
	}
	archiveReader, err := os.Open(archivePath)
	if err != nil {
		t.Fatalf("open bridge archive for restore: %v", err)
	}
	defer archiveReader.Close()
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	var stderr bytes.Buffer
	runDone := make(chan struct{})
	var result recovery.PGCommandResult
	var runErr error
	go func() {
		defer close(runDone)
		result, runErr = recovery.DrillRunObservedPGRestore(runCtx,
			recovery.TargetProcessRunner{DrainTimeout: 20 * time.Second}, handle, archiveReader, nil, &stderr)
	}()
	if err := handle.AwaitStarted(ctx); err != nil {
		t.Fatalf("native bridge restore never started: %v (stderr bytes=%d)", err, stderr.Len())
	}
	identity := handle.StartedIdentity()

	// The child must reach the actual armed listener; the acceptor holds the
	// connection so the barrier stays before any SQL.
	var held net.Conn
	select {
	case held = <-accepted:
		defer held.Close()
	case <-time.After(30 * time.Second):
		t.Fatalf("native restore never connected to the armed origin listener (stderr bytes=%d)", stderr.Len())
	}
	if exists, _ := supervisorTableState(t, ctx, targetConn, table); exists {
		t.Fatal("barrier was bypassed: the target relation exists before any release")
	}

	// The child is built only from the private arm configuration: canonical
	// flags plus the credential-protected DSN, and no ambient PG route.
	argv, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", identity.PID))
	if err != nil {
		t.Fatalf("read native bridge argv: %v", err)
	}
	argvText := strings.ReplaceAll(string(argv), "\x00", " ")
	for _, required := range []string{"--no-owner", "--no-privileges", "--dbname="} {
		if !strings.Contains(argvText, required) {
			t.Fatalf("native bridge argv is missing the closed flag %q", required)
		}
	}
	if strings.Contains(argvText, fx.password) {
		t.Fatal("native bridge argv contains the DSN password")
	}
	environ, err := os.ReadFile(fmt.Sprintf("/proc/%d/environ", identity.PID))
	if err != nil {
		t.Fatalf("read native bridge environment: %v", err)
	}
	if bytes.Contains(environ, []byte("PGHOST=")) || bytes.Contains(environ, []byte("ambient-route-canary")) {
		t.Fatal("native bridge child inherited an ambient PG route")
	}
	if !bytes.Contains(environ, []byte("PGPASSFILE=/proc/self/fd/")) {
		t.Fatal("native bridge child is missing the protected passfile descriptor environment")
	}

	// Endpoint identity is an opaque private-pointer comparison: the wrong
	// capability (even the right scalar address shape) cannot claim.
	wrong := mustOpenDrillOriginEndpoint(t)
	if _, err := handle.ClaimForEndpoint(wrong); err == nil {
		t.Fatal("a different endpoint capability claimed the origin")
	}
	claim, err := handle.ClaimForEndpoint(endpoint)
	if err != nil {
		t.Fatalf("the armed endpoint capability could not claim: %v", err)
	}
	if !claim.MatchesEndpoint(endpoint) || claim.MatchesEndpoint(wrong) {
		t.Fatal("claimed origin does not match the armed endpoint capability")
	}

	cancelRun()
	select {
	case <-runDone:
	case <-ctx.Done():
		t.Fatalf("canceled bridge run did not return: %v", ctx.Err())
	}
	if runErr == nil || result.Outcome == recovery.PGCommandSucceeded {
		t.Fatalf("canceled barrier run reported success: %+v err=%v", result, runErr)
	}
	requireDrillBridgeReaped(t, identity.PID, identity.StartID)
	if exists, _ := supervisorTableState(t, ctx, targetConn, table); exists {
		t.Fatal("canceled barrier run executed SQL against the target")
	}
}

// ---------------------------------------------------------------------------
// OG01 phase 1 follow-up: one start per handle (P1) causal tests.
// ---------------------------------------------------------------------------

// drillBridgeNativeFixture is a real pinned PG18.6 archive + empty target +
// sealed tools, shared by the concurrent-launch causal test.
type drillBridgeNativeFixture struct {
	fx          *originGateFixture
	ctx         context.Context
	tools       recovery.DrillNativePGTools
	targetDB    string
	table       string
	archivePath string
	targetConn  *pgx.Conn
}

func newDrillBridgeNativeFixture(t *testing.T) *drillBridgeNativeFixture {
	t.Helper()
	fx := newOriginGateFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	t.Cleanup(cancel)
	drillUnsetAmbientPGRoute(t)
	tools := mustProvisionDrillNativePGTools(t)
	dumpPath, ok := tools.DumpPath()
	if !ok {
		t.Fatal("sealed tools did not expose the dump path diagnostic")
	}
	t.Setenv("PATH", filepath.Dir(dumpPath)+string(os.PathListSeparator)+os.Getenv("PATH"))
	nano := time.Now().UnixNano()
	sourceDB := fmt.Sprintf("origin_once_src_%d", nano)
	targetDB := fmt.Sprintf("origin_once_tgt_%d", nano)
	table := fmt.Sprintf("origin_once_rows_%d", nano)
	if err := fx.createOwnedDatabase(ctx, sourceDB); err != nil {
		t.Fatalf("create once source database: %v", err)
	}
	if err := fx.createOwnedDatabase(ctx, targetDB); err != nil {
		t.Fatalf("create once target database: %v", err)
	}
	sourceConn, err := pgx.Connect(ctx, fx.dsnAs(sourceDB, fx.role, fx.password))
	if err != nil {
		t.Fatalf("connect once source: %v", err)
	}
	defer sourceConn.Close(context.Background())
	if _, err := sourceConn.Exec(ctx, `CREATE TABLE public.`+pgx.Identifier{table}.Sanitize()+` (id int PRIMARY KEY, note text NOT NULL)`); err != nil {
		t.Fatalf("create once source table: %v", err)
	}
	if _, err := sourceConn.Exec(ctx, `INSERT INTO public.`+pgx.Identifier{table}.Sanitize()+` (id, note) VALUES (1, 'once'), (2, 'start')`); err != nil {
		t.Fatalf("seed once source rows: %v", err)
	}
	archivePath := filepath.Join(t.TempDir(), "origin-once.dump")
	archiveFile, err := os.Create(archivePath)
	if err != nil {
		t.Fatalf("create once archive file: %v", err)
	}
	var dumpDiagnostics bytes.Buffer
	if err := (recovery.LocalPGCommand{}).Run(ctx, "pg_dump",
		[]string{"-Fc", "--no-owner", "--no-privileges", "--dbname=" + fx.dsnAsLibpq(sourceDB, fx.role, fx.password)},
		nil, archiveFile, &dumpDiagnostics); err != nil {
		_ = archiveFile.Close()
		diagnostic := strings.ToLower(dumpDiagnostics.String())
		t.Fatalf("real pinned pg_dump failed: %v (stderr_connection=%t stderr_permission=%t stderr_table=%t stderr_pgclient=%t)",
			err, strings.Contains(diagnostic, "connection"), strings.Contains(diagnostic, "permission"),
			strings.Contains(diagnostic, "table"), strings.Contains(diagnostic, "pg_dump: error"))
	}
	if err := archiveFile.Close(); err != nil {
		t.Fatalf("close once archive: %v", err)
	}
	if info, err := os.Stat(archivePath); err != nil || info.Size() == 0 {
		t.Fatalf("once archive is empty: err=%v", err)
	}
	targetConn, err := pgx.Connect(ctx, fx.dsnAs(targetDB, "txharbor", "txharbor"))
	if err != nil {
		t.Fatalf("connect once target: %v", err)
	}
	t.Cleanup(func() { _ = targetConn.Close(context.Background()) })
	return &drillBridgeNativeFixture{
		fx: fx, ctx: ctx, tools: tools, targetDB: targetDB, table: table,
		archivePath: archivePath, targetConn: targetConn,
	}
}

func (f *drillBridgeNativeFixture) armDSN(endpoint recovery.DrillOriginEndpoint) string {
	return fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable&gssencmode=disable", f.fx.role, f.fx.password, endpoint.Addr(), f.targetDB)
}

// requireDrillBridgeReaped proves the exact child start identity is
// authoritatively gone using the preserved OG05 strict verdict; an unreadable
// /proc is a refusal with its cause, never reaping evidence.
func requireDrillBridgeReaped(t *testing.T, pid int, startID uint64) {
	t.Helper()
	if err := originSupervisorReapError(pid, startID, 5*time.Second); err != nil {
		t.Fatal(err)
	}
}

// drillDirectNativeRestoreChildren counts the test process's direct children
// that are actual native restores by their canonical closed flags.
func drillDirectNativeRestoreChildren(t *testing.T) []int {
	t.Helper()
	entries, err := os.ReadDir("/proc")
	if err != nil {
		t.Fatalf("read /proc for native child count: %v", err)
	}
	self := os.Getpid()
	var pids []int
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid == self {
			continue
		}
		stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
		if err != nil {
			continue
		}
		closeIndex := strings.LastIndexByte(string(stat), ')')
		if closeIndex < 0 {
			continue
		}
		fields := strings.Fields(string(stat[closeIndex+1:]))
		if len(fields) < 2 {
			continue
		}
		ppid, err := strconv.Atoi(fields[1])
		if err != nil || ppid != self {
			continue
		}
		cmdline, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid))
		if err != nil {
			continue
		}
		if bytes.Contains(cmdline, []byte("--no-owner")) && bytes.Contains(cmdline, []byte("--no-privileges")) && bytes.Contains(cmdline, []byte("--dbname=")) {
			pids = append(pids, pid)
		}
	}
	return pids
}

func TestDrillRunObservedPGRestoreReservesOneStartPerHandle(t *testing.T) {
	fx := newDrillBridgeNativeFixture(t)
	ctx := fx.ctx
	endpoint := mustOpenDrillOriginEndpoint(t)
	accepted := make(chan net.Conn, 1)
	go func() {
		for {
			conn, err := endpoint.Listener().Accept()
			if err != nil {
				return
			}
			select {
			case accepted <- conn:
			default:
				_ = conn.Close()
			}
		}
	}()
	handle := recovery.DrillNewTargetProcessObservation()
	operationID := fmt.Sprintf("origin-once-%d", time.Now().UnixNano())
	if err := recovery.DrillArmObservedPGRestore(handle, endpoint, fx.tools, recovery.DrillPGRestoreArmOptions{TargetDSN: fx.armDSN(endpoint), OperationID: operationID}); err != nil {
		t.Fatalf("arm once restore: %v", err)
	}
	archiveReader, err := os.Open(fx.archivePath)
	if err != nil {
		t.Fatalf("open once archive: %v", err)
	}
	defer archiveReader.Close()
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	var stderr bytes.Buffer
	runDone := make(chan struct{})
	var result recovery.PGCommandResult
	var runErr error
	go func() {
		defer close(runDone)
		result, runErr = recovery.DrillRunObservedPGRestore(runCtx,
			recovery.TargetProcessRunner{DrainTimeout: 20 * time.Second}, handle, archiveReader, nil, &stderr)
	}()
	if err := handle.AwaitStarted(ctx); err != nil {
		t.Fatalf("native once restore never started: %v (stderr bytes=%d)", err, stderr.Len())
	}
	identity := handle.StartedIdentity()

	// The transport barrier must be reached by exactly one actual child.
	var held net.Conn
	select {
	case held = <-accepted:
		t.Cleanup(func() { _ = held.Close() })
	case <-time.After(30 * time.Second):
		t.Fatalf("native restore never connected to the armed origin listener (stderr bytes=%d)", stderr.Len())
	}
	children := drillDirectNativeRestoreChildren(t)
	if len(children) != 1 || children[0] != identity.PID {
		t.Fatalf("actual native restore children (!=1 or not the observed identity): pids=%v identity=%+v", children, identity)
	}
	t.Logf("causal: actual_native_children_while_running=%d pid=%d start=%d", len(children), identity.PID, identity.StartID)

	// A concurrent duplicate invocation on the same handle must be refused
	// before any launch, with the specific reservation reason, and it must not
	// overwrite any PID/Wait fact of the running child.
	dupResult, dupErr := recovery.DrillRunObservedPGRestore(context.Background(), recovery.TargetProcessRunner{}, handle, nil, nil, nil)
	if dupErr == nil || dupResult.Started || dupResult.Outcome != recovery.PGCommandNotStarted {
		t.Fatalf("concurrent duplicate invocation was not refused before start: %+v err=%v", dupResult, dupErr)
	}
	if !strings.Contains(dupErr.Error(), "already invoked") {
		t.Fatalf("duplicate invocation refusal lacks the specific reservation reason: %v", dupErr)
	}
	identityAfter := handle.StartedIdentity()
	if identityAfter.Started != identity.Started || identityAfter.PID != identity.PID || identityAfter.StartID != identity.StartID {
		t.Fatalf("duplicate invocation overwrote the started identity: before=%+v after=%+v", identity, identityAfter)
	}
	if disposition := handle.RunnerDisposition(); disposition.Returned {
		t.Fatalf("duplicate invocation recorded a runner disposition: %+v", disposition)
	}
	if terminal := handle.WaitTerminal(); terminal.Terminal || terminal.ChildWaitCompleted {
		t.Fatalf("duplicate invocation recorded terminal wait facts: %+v", terminal)
	}
	if again := drillDirectNativeRestoreChildren(t); len(again) != 1 || again[0] != identity.PID {
		t.Fatalf("duplicate invocation launched another actual child: pids=%v", again)
	}
	t.Logf("causal: concurrent_start_refusals=1 reason=%q actual_children_after_duplicate=%d facts_unchanged=true", dupErr, len(children))

	// The legitimate gate claim runs after Started even though the run
	// reservation is taken; a wrong capability refuses without consuming.
	wrong := mustOpenDrillOriginEndpoint(t)
	if _, err := handle.ClaimForEndpoint(wrong); err == nil {
		t.Fatal("a different endpoint capability claimed the reserved origin")
	}
	claim, err := handle.ClaimForEndpoint(endpoint)
	if err != nil {
		t.Fatalf("the armed endpoint could not claim after the reserved start: %v", err)
	}
	if !claim.MatchesEndpoint(endpoint) || claim.MatchesEndpoint(wrong) {
		t.Fatal("claimed origin does not match the armed endpoint capability")
	}

	cancelRun()
	select {
	case <-runDone:
	case <-ctx.Done():
		t.Fatalf("canceled once run did not return: %v", ctx.Err())
	}
	if runErr == nil || result.Outcome == recovery.PGCommandSucceeded {
		t.Fatalf("canceled once run reported success: %+v err=%v", result, runErr)
	}
	requireDrillBridgeReaped(t, identity.PID, identity.StartID)
	kids := drillDirectNativeRestoreChildren(t)
	if len(kids) != 0 {
		t.Fatalf("canceled once run left actual children: pids=%v", kids)
	}
	terminalBefore := handle.WaitTerminal()
	if !terminalBefore.Terminal || !terminalBefore.ChildWaitCompleted {
		t.Fatalf("sole wait terminal facts are not authentic after cancel: %+v", terminalBefore)
	}
	t.Logf("causal: children_after_cancel_and_reap=%d terminal=%+v", len(kids), terminalBefore)
	if exists, _ := supervisorTableState(t, ctx, fx.targetConn, fx.table); exists {
		t.Fatal("reserved once run executed SQL against the target")
	}

	// A post-terminal attempt on the same handle is refused and cannot attach
	// to the finished child's ambiguous/late wait facts.
	postResult, postErr := recovery.DrillRunObservedPGRestore(context.Background(), recovery.TargetProcessRunner{}, handle, nil, nil, nil)
	if postErr == nil || postResult.Started || postResult.Outcome != recovery.PGCommandNotStarted {
		t.Fatalf("post-terminal attempt was not refused: %+v err=%v", postResult, postErr)
	}
	if !strings.Contains(postErr.Error(), "already invoked") {
		t.Fatalf("post-terminal refusal lacks the specific reservation reason: %v", postErr)
	}
	t.Logf("causal: post_terminal_refusals=1 reason=%q", postErr)
	identityFinal := handle.StartedIdentity()
	terminalFinal := handle.WaitTerminal()
	if identityFinal.PID != identity.PID || identityFinal.StartID != identity.StartID {
		t.Fatalf("post-terminal attempt overwrote the identity: before=%+v after=%+v", identity, identityFinal)
	}
	if terminalFinal.Terminal != terminalBefore.Terminal || terminalFinal.ChildWaitCompleted != terminalBefore.ChildWaitCompleted || terminalFinal.WaitExitCode != terminalBefore.WaitExitCode {
		t.Fatalf("post-terminal attempt overwrote wait facts: before=%+v after=%+v", terminalBefore, terminalFinal)
	}
	if kids := drillDirectNativeRestoreChildren(t); len(kids) != 0 {
		t.Fatalf("post-terminal refusal launched a child: pids=%v", kids)
	}

	// Retry requires a new handle: arming one succeeds with the same sealed
	// endpoint/tools authority.
	fresh := recovery.DrillNewTargetProcessObservation()
	if err := recovery.DrillArmObservedPGRestore(fresh, endpoint, fx.tools, recovery.DrillPGRestoreArmOptions{TargetDSN: fx.armDSN(endpoint), OperationID: operationID + "-retry"}); err != nil {
		t.Fatalf("a new handle could not arm for retry: %v", err)
	}
}

func TestDrillRunObservedPGRestoreInvalidCallDoesNotConsumeHandle(t *testing.T) {
	handle := recovery.DrillNewTargetProcessObservation()
	if result, err := recovery.DrillRunObservedPGRestore(context.Background(), recovery.TargetProcessRunner{}, handle, nil, nil, nil); err == nil || result.Started {
		t.Fatalf("unarmed handle was not refused: %+v err=%v", result, err)
	}
	if result, err := recovery.DrillRunObservedPGRestore(context.Background(), recovery.TargetProcessRunner{}, recovery.DrillTargetProcessObservation{}, nil, nil, nil); err == nil || result.Started {
		t.Fatalf("zero handle was not refused: %+v err=%v", result, err)
	}
	tools := mustProvisionDrillNativePGTools(t)
	endpoint := mustOpenDrillOriginEndpoint(t)
	opts := recovery.DrillPGRestoreArmOptions{
		TargetDSN:   "postgres://txharbor:txharbor@" + endpoint.Addr() + "/origin_not_consumed?sslmode=disable",
		OperationID: "not-consumed",
	}
	if err := recovery.DrillArmObservedPGRestore(handle, endpoint, tools, opts); err != nil {
		t.Fatalf("invalid run calls consumed the handle: %v", err)
	}
	if err := recovery.DrillArmObservedPGRestore(handle, endpoint, tools, opts); err == nil {
		t.Fatal("a second arm was accepted on the same handle")
	}
}

func TestDrillRunRefusalAfterReservationConsumesHandle(t *testing.T) {
	tools := mustProvisionDrillNativePGTools(t)
	endpoint := mustOpenDrillOriginEndpoint(t)
	handle := recovery.DrillNewTargetProcessObservation()
	opts := recovery.DrillPGRestoreArmOptions{
		TargetDSN:   "postgres://txharbor:txharbor@" + endpoint.Addr() + "/origin_consumed?sslmode=disable",
		OperationID: "consumed",
	}
	if err := recovery.DrillArmObservedPGRestore(handle, endpoint, tools, opts); err != nil {
		t.Fatalf("arm before the consumed-attempt test: %v", err)
	}
	// Invalidate the listener after arm: the run reserves first and then fails
	// the pre-start revalidation. That failure must consume the attempt.
	if err := endpoint.Listener().Close(); err != nil {
		t.Fatalf("close armed listener: %v", err)
	}
	first, firstErr := recovery.DrillRunObservedPGRestore(context.Background(), recovery.TargetProcessRunner{}, handle, nil, nil, nil)
	if firstErr == nil || first.Started || first.Outcome != recovery.PGCommandNotStarted {
		t.Fatalf("invalid pre-start revalidation was not refused: %+v err=%v", first, firstErr)
	}
	if !strings.Contains(firstErr.Error(), "before start") {
		t.Fatalf("pre-start refusal lacks the revalidation reason: %v", firstErr)
	}
	second, secondErr := recovery.DrillRunObservedPGRestore(context.Background(), recovery.TargetProcessRunner{}, handle, nil, nil, nil)
	if secondErr == nil || second.Started || second.Outcome != recovery.PGCommandNotStarted {
		t.Fatalf("consumed handle retried a start: %+v err=%v", second, secondErr)
	}
	if !strings.Contains(secondErr.Error(), "already invoked") {
		t.Fatalf("consumed retry refusal lacks the specific reservation reason: %v", secondErr)
	}
	if kids := drillDirectNativeRestoreChildren(t); len(kids) != 0 {
		t.Fatalf("failed-before-start attempts launched children: pids=%v", kids)
	}
	t.Logf("causal: pre_start_refusal=%q consumed_retry_refusal=%q children_started=0", firstErr, secondErr)
}

// drillUnsetAmbientPGRoute removes PG-prefixed ambient variables that the
// product's protected child environment must reject, and restores them after
// the owning test.
func drillUnsetAmbientPGRoute(t *testing.T) {
	t.Helper()
	for _, entry := range os.Environ() {
		key, _, ok := strings.Cut(entry, "=")
		if !ok || !strings.HasPrefix(strings.ToUpper(key), "PG") {
			continue
		}
		previous, had := os.LookupEnv(key)
		_ = os.Unsetenv(key)
		if had {
			restoreKey, restoreValue := key, previous
			t.Cleanup(func() { _ = os.Setenv(restoreKey, restoreValue) })
		}
	}
}
