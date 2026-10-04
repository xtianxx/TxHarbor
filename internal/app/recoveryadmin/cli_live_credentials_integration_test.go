//go:build integration && linux

package recoveryadmin

import (
	"bytes"
	"context"
	"debug/elf"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/xtianxx/txharbor/internal/config"
)

// The exact operation ids carried by the two real invocations of the shipped
// CLI below. Command audit rows are written with a NULL instance_id and bind
// the fixture instance through target JSON, so the audit evidence query must
// select these exact ids (and the actual backup id) instead of assuming an
// instance_id-only binding.
const (
	cliLiveBackupOperation    = "live-success"
	cliLiveInterruptOperation = "live-interrupt"
)

// This is intentionally a process-level test: the data-table lock holds a real
// authenticated PostgreSQL 18 pg_dump in the kernel while /proc is inspected.
func TestRecoveryAdminCLIUsesLiveCredentialPrivateNativeChild(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("requires Linux /proc process inspection")
	}
	nativeDump := nativePGDumpPath(t)
	resolvedDump, err := filepath.EvalSymlinks(nativeDump)
	if err != nil {
		t.Fatalf("resolve native pg_dump: %v", err)
	}
	binInfo, err := os.Stat(resolvedDump)
	if err != nil {
		t.Fatalf("stat native pg_dump: %v", err)
	}
	elfFile, err := elf.Open(resolvedDump)
	if err != nil {
		t.Fatalf("open native pg_dump ELF: %v", err)
	}
	_ = elfFile.Close()

	// Preserve the unshimmed PATH for the production child, and build the actual
	// shipped binary once for both the successful and interruption executions.
	path := os.Getenv("PATH")
	f := newCLIFixture(t)
	bin := filepath.Join(t.TempDir(), "txharbor")
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "../../../"))
	build := exec.Command("go", "build", "-o", bin, "./cmd/txharbor")
	build.Dir = repoRoot
	build.Env = append(os.Environ(), "PATH="+path)
	if _, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build production CLI: %v (output omitted)", err)
	}

	canaries := []string{
		"live-db-password-canary", "live-target-canary", "live-control-canary", "live-data-canary",
		"https://rpc.invalid/live-rpc-api-key", "live-signer-token", "live-vault-token",
		"postgres://broker:live-broker-password@broker.invalid/db", "live-custom-secret",
	}
	// Replace the throwaway container role password with a unique test-only
	// canary and update only the DSN strings used by this test. Existing fixture
	// pools are already authenticated; all newly launched production processes
	// must authenticate with this disposable credential.
	if _, err := f.admin.Exec(f.ctx, `ALTER ROLE txharbor PASSWORD 'live-db-password-canary'`); err != nil {
		t.Fatalf("set disposable database credential canary: %v", err)
	}
	f.baseDSN = dsnWithPassword(t, f.baseDSN, "live-db-password-canary")
	f.dataDSN = dsnWithPassword(t, f.dataDSN, "live-db-password-canary")
	f.controlDSN = dsnWithPassword(t, f.controlDSN, "live-db-password-canary")
	f.restoreDSN = dsnWithPassword(t, f.restoreDSN, "live-db-password-canary")
	env := f.env("deploy:executor")
	env["TXHARBOR_RECOVERY_TARGET_DSN"] = f.restoreDSN
	env[config.EnvRecoveryObserverDSN] = f.baseDSN
	env["TXHARBOR_RECOVERY_TARGET_DSN"] = f.restoreDSN
	env["TXHARBOR_RECOVERY_TARGET_CANARY"] = "live-target-canary"
	env["TXHARBOR_RECOVERY_CONTROL_CANARY"] = "live-control-canary"
	env["TXHARBOR_PG_CANARY"] = "live-data-canary"
	env["TXHARBOR_RPC_URL"] = "https://rpc.invalid/live-rpc-api-key"
	env["TXHARBOR_SIGNER_TOKEN"] = "live-signer-token"
	env["VAULT_TOKEN"] = "live-vault-token"
	env["BROKER_DSN"] = "postgres://broker:live-broker-password@broker.invalid/db"
	env["TESTSECRET"] = "live-custom-secret"
	childEnv := []string{"PATH=" + path}
	if home := os.Getenv("HOME"); home != "" {
		childEnv = append(childEnv, "HOME="+home)
	}
	for key, value := range env {
		childEnv = append(childEnv, key+"="+value)
	}

	// Hold an ACCESS EXCLUSIVE table lock. pg_dump must authenticate and wait
	// for ACCESS SHARE before the real CLI can finish writing its archive.
	lockConn, err := pgx.Connect(f.ctx, f.dataDSN)
	if err != nil {
		t.Fatalf("connect lock owner: %v", err)
	}
	defer lockConn.Close(context.Background())
	if _, err := lockConn.Exec(f.ctx, "BEGIN"); err != nil {
		t.Fatalf("begin table lock: %v", err)
	}
	defer lockConn.Exec(context.Background(), "ROLLBACK")
	if _, err := lockConn.Exec(f.ctx, "LOCK TABLE "+cliProbeTable+" IN ACCESS EXCLUSIVE MODE"); err != nil {
		t.Fatalf("hold source-table lock: %v", err)
	}

	var startedCommands []*exec.Cmd
	var nativeChildren []processIdentity
	defer func() {
		for _, cmd := range startedCommands {
			if cmd.Process != nil && cmd.ProcessState == nil {
				nativeChildren = append(nativeChildren, directNativeChildren(cmd.Process.Pid, resolvedDump)...)
			}
		}
		for _, child := range nativeChildren {
			// A cleanup kill requires a fresh observation of the exact start
			// identity; an unreadable or reused PID is never killed.
			if presence, _ := linuxTestProcessPresence(child.pid, child.start); presence == linuxTestPresenceAlive {
				_ = syscall.Kill(child.pid, syscall.SIGKILL)
				_ = waitForProcessGone(child.pid, child.start, 5*time.Second)
			}
		}
		for _, cmd := range startedCommands {
			if cmd.Process != nil && cmd.ProcessState == nil {
				_ = cmd.Process.Kill()
				_ = cmd.Wait()
			}
		}
	}()
	startCLI := func(operation string) (*exec.Cmd, *bytes.Buffer, *bytes.Buffer) {
		args := []string{"recovery-admin", "backup", "--chain-id", "31337", "--out", filepath.Join(f.artDir, operation), "--operation-id", operation}
		cmd := exec.Command(bin, args...)
		cmd.Env = childEnv
		stdout, stderr := &bytes.Buffer{}, &bytes.Buffer{}
		cmd.Stdout, cmd.Stderr = stdout, stderr
		if err := cmd.Start(); err != nil {
			t.Fatalf("start production CLI: %v", err)
		}
		startedCommands = append(startedCommands, cmd)
		return cmd, stdout, stderr
	}

	first, firstOut, firstErr := startCLI(cliLiveBackupOperation)
	firstStart, err := linuxTestStartIdentity(first.Process.Pid)
	if err != nil {
		t.Fatalf("read CLI start identity: %v", err)
	}
	firstNativePID, firstNativeStart := waitForNativeDump(t, first.Process.Pid, firstStart, resolvedDump, 30*time.Second)
	registerObservedNativeChild(&nativeChildren, processIdentity{pid: firstNativePID, start: firstNativeStart})
	assertCLIArgvPrivate(t, first.Process.Pid, canaries)
	nativePID, nativeStart := nativeChildren[len(nativeChildren)-1].pid, nativeChildren[len(nativeChildren)-1].start
	assertPrivatePGChild(t, first.Process.Pid, nativePID, canaries, binInfo)
	assertAuthenticatedDumpBlocked(t, f, nativePID)
	if _, err := lockConn.Exec(f.ctx, "COMMIT"); err != nil {
		t.Fatalf("release source-table lock: %v", err)
	}
	if err := first.Wait(); err != nil {
		t.Fatalf("production backup failed (output suppressed): %v", err)
	}
	assertNoCanary(t, firstOut.String()+firstErr.String(), canaries, "CLI output")
	assertCLIOutputClean(t, filepath.Join(f.artDir, cliLiveBackupOperation), canaries)
	assertControlAuditClean(t, f, firstOut.String()+firstErr.String(), filepath.Join(f.artDir, cliLiveBackupOperation), canaries)
	requireLinuxTestProcessGone(t, nativePID, nativeStart, "native pg_dump after successful CLI completion")

	// Repeat with a fresh real child, then SIGKILL only the authenticated CLI
	// parent. Observe natural child termination before cleanup; an orphan is a
	// negative result, not something the test may silently repair and call pass.
	lockConn2, err := pgx.Connect(f.ctx, f.dataDSN)
	if err != nil {
		t.Fatalf("connect second lock owner: %v", err)
	}
	defer lockConn2.Close(context.Background())
	if _, err := lockConn2.Exec(f.ctx, "BEGIN"); err != nil {
		t.Fatalf("begin second lock: %v", err)
	}
	defer lockConn2.Exec(context.Background(), "ROLLBACK")
	if _, err := lockConn2.Exec(f.ctx, "LOCK TABLE "+cliProbeTable+" IN ACCESS EXCLUSIVE MODE"); err != nil {
		t.Fatalf("hold second source-table lock: %v", err)
	}
	second, _, _ := startCLI(cliLiveInterruptOperation)
	secondStart, err := linuxTestStartIdentity(second.Process.Pid)
	if err != nil {
		t.Fatalf("read second CLI start identity: %v", err)
	}
	secondNativePID, secondNativeStart := waitForNativeDump(t, second.Process.Pid, secondStart, resolvedDump, 30*time.Second)
	registerObservedNativeChild(&nativeChildren, processIdentity{pid: secondNativePID, start: secondNativeStart})
	assertCLIArgvPrivate(t, second.Process.Pid, canaries)
	childPID, childStart := nativeChildren[len(nativeChildren)-1].pid, nativeChildren[len(nativeChildren)-1].start
	assertPrivatePGChild(t, second.Process.Pid, childPID, canaries, binInfo)
	assertAuthenticatedDumpBlocked(t, f, childPID)
	if err := second.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatalf("SIGKILL CLI parent: %v", err)
	}
	assertCLIParentWasKilled(t, second.Wait())
	drainPresence, drainErr := awaitLinuxTestProcessGone(childPID, childStart, 5*time.Second)
	if drainPresence == linuxTestPresenceUnknown {
		// An unreadable /proc is not disappearance evidence, and the PID is
		// not killed because its identity could not be re-verified.
		t.Fatalf("FAIL: SIGKILL outcome could not be proven: process inspection refused (pid=%d start=%d): %v", childPID, childStart, drainErr)
	}
	if drainPresence == linuxTestPresenceAlive {
		state, ppid, alive := linuxTestProcessObservation(childPID, childStart)
		// Cleanup after recording the failed observation so this negative test
		// never leaves a database session or native client behind. The kill is
		// issued only after the exact start identity was verified alive.
		cleanupDrained := false
		if presence, _ := linuxTestProcessPresence(childPID, childStart); presence == linuxTestPresenceAlive {
			if err := syscall.Kill(childPID, syscall.SIGKILL); err == nil {
				cleanupPresence, _ := awaitLinuxTestProcessGone(childPID, childStart, 5*time.Second)
				cleanupDrained = cleanupPresence == linuxTestPresenceGone
			}
		}
		t.Fatalf("FAIL: SIGKILL left owned native pg_dump alive after bounded wait (pid=%d start=%d ppid=%d state=%s alive_count=%d); cleanup_drained=%t", childPID, childStart, ppid, state, alive, cleanupDrained)
	}
	assertInterruptedOperationNeverRecordedSuccess(t, f, canaries)
	requireLinuxTestProcessGone(t, first.Process.Pid, firstStart, "first CLI process after the interrupted run")
}

// assertInterruptedOperationNeverRecordedSuccess proves the SIGKILLed
// invocation wrote no success audit row after the interruption was observed.
// Any row that does exist under that exact operation id is swept for canaries
// through the same safe text outputs; an empty set is only the negative claim
// that no audit write happened before SIGKILL, never positive coverage.
func assertInterruptedOperationNeverRecordedSuccess(t *testing.T, f *cliFixture, canaries []string) {
	t.Helper()
	var successRows int
	if err := f.control.QueryRow(f.ctx,
		`SELECT count(*) FROM recovery_audit WHERE operation_id=$1 AND result='ok'`,
		cliLiveInterruptOperation).Scan(&successRows); err != nil {
		t.Fatalf("read the interrupted operation audit rows: %v", err)
	}
	if successRows != 0 {
		t.Fatalf("the SIGKILLed invocation %q recorded %d success audit row(s); an interrupted operation must not claim success",
			cliLiveInterruptOperation, successRows)
	}
	rows, err := f.control.Query(f.ctx, `
SELECT coalesce(target::text,'') || ' ' || coalesce(detail::text,'') || ' ' ||
       actor || ' ' || action || ' ' || coalesce(operation_id,'')
FROM recovery_audit WHERE operation_id=$1 ORDER BY audit_id`, cliLiveInterruptOperation)
	if err != nil {
		t.Fatalf("read the interrupted operation rows for canary scan: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var row string
		if err := rows.Scan(&row); err != nil {
			t.Fatalf("scan the interrupted operation rows for canary scan: %v", err)
		}
		for _, canary := range canaries {
			if strings.Contains(row, canary) {
				t.Fatal("interrupted operation audit row contains an application canary")
			}
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate the interrupted operation canary scan: %v", err)
	}
}

func assertCLIParentWasKilled(t *testing.T, err error) {
	t.Helper()
	exitErr, ok := err.(*exec.ExitError)
	if !ok {
		t.Fatalf("CLI Wait did not report expected SIGKILL exit: %v", err)
	}
	waitStatus, ok := exitErr.ProcessState.Sys().(syscall.WaitStatus)
	if !ok || waitStatus.Signal() != syscall.SIGKILL {
		t.Fatal("CLI parent Wait status was not SIGKILL")
	}
}

type processIdentity struct {
	pid   int
	start uint64
}

func registerObservedNativeChild(children *[]processIdentity, identity processIdentity) {
	*children = append(*children, identity)
}

func directNativeChildren(parentPID int, native string) []processIdentity {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil
	}
	want, err := os.Stat(native)
	if err != nil {
		return nil
	}
	var children []processIdentity
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid == parentPID {
			continue
		}
		ppid, err := linuxTestParentPID(pid)
		if err != nil || ppid != parentPID {
			continue
		}
		exe, err := os.Stat(fmt.Sprintf("/proc/%d/exe", pid))
		if err != nil || !os.SameFile(exe, want) {
			continue
		}
		start, err := linuxTestStartIdentity(pid)
		if err == nil {
			children = append(children, processIdentity{pid: pid, start: start})
		}
	}
	return children
}

func assertNoCanary(t *testing.T, value string, canaries []string, location string) {
	t.Helper()
	for _, canary := range canaries {
		if strings.Contains(value, canary) {
			t.Fatalf("%s contains a credential canary", location)
		}
	}
}

func dsnWithPassword(t *testing.T, dsn, password string) string {
	t.Helper()
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse disposable test DSN: %v", err)
	}
	username := u.User.Username()
	u.User = url.UserPassword(username, password)
	return u.String()
}

func assertPrivatePGChild(t *testing.T, parentPID, pid int, canaries []string, native os.FileInfo) {
	t.Helper()
	argv, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid))
	if err != nil {
		t.Fatalf("read native child argv: %v", err)
	}
	for _, canary := range canaries {
		if bytes.Contains(argv, []byte(canary)) {
			t.Fatal("native PostgreSQL child argv contains an application canary")
		}
	}
	assertArgvCredentialFree(t, argv, "native PostgreSQL child")
	var environ []byte
	var readErr error
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		environ, readErr = os.ReadFile(fmt.Sprintf("/proc/%d/environ", pid))
		if readErr != nil {
			t.Fatalf("read native child environment: %v", readErr)
		}
		if bytes.Contains(environ, []byte("PGPASSFILE=")) {
			break
		}
		switch presence, presenceErr := linuxTestProcessPresence(pid, 0); presence {
		case linuxTestPresenceAlive:
		case linuxTestPresenceGone:
			t.Fatal("native child exited before private passfile environment was observable")
		default:
			t.Fatalf("native child inspection refused before private passfile environment was observable: %v", presenceErr)
		}
		time.Sleep(10 * time.Millisecond)
	}
	entries := strings.Split(string(environ), "\x00")
	passfile := ""
	for _, entry := range entries {
		key, value, ok := strings.Cut(entry, "=")
		if !ok {
			continue
		}
		if key == "PGPASSFILE" {
			passfile = value
		}
		if key != "PATH" && key != "HOME" && key != "TMPDIR" && key != "LANG" && key != "PGPASSFILE" {
			t.Fatalf("native child has unexpected environment key %q", key)
		}
		for _, canary := range canaries {
			if strings.Contains(value, canary) {
				t.Fatal("native child environment contains an application canary")
			}
		}
	}
	if !strings.HasPrefix(passfile, "/proc/self/fd/") {
		t.Fatalf("native child PGPASSFILE is not an anonymous descriptor reference: %q", passfile)
	}
	fd := filepath.Base(passfile)
	fdInfo, err := os.Stat(fmt.Sprintf("/proc/%d/fd/%s", pid, fd))
	if err != nil {
		t.Fatalf("stat child anonymous passfile: %v", err)
	}
	stat, ok := fdInfo.Sys().(*syscall.Stat_t)
	if !ok || !fdInfo.Mode().IsRegular() || fdInfo.Mode().Perm() != 0o600 || stat.Nlink != 0 {
		t.Fatal("native child passfile descriptor is not regular mode 0600 with link count zero")
	}
	childExe, err := os.Stat(fmt.Sprintf("/proc/%d/exe", pid))
	if err != nil || !os.SameFile(native, childExe) {
		t.Fatalf("owned descendant PID %d is not the resolved native pg_dump ELF", pid)
	}
	ppid, err := linuxTestParentPID(pid)
	if err != nil || ppid != parentPID {
		t.Fatalf("native pg_dump is not an owned direct child (pid=%d ppid=%d)", pid, ppid)
	}
}

func assertCLIArgvPrivate(t *testing.T, pid int, canaries []string) {
	t.Helper()
	argv, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid))
	if err != nil {
		t.Fatalf("read CLI argv: %v", err)
	}
	for _, canary := range canaries {
		if bytes.Contains(argv, []byte(canary)) {
			t.Fatal("production CLI argv contains an application canary")
		}
	}
	assertArgvCredentialFree(t, argv, "production CLI")
}

func assertArgvCredentialFree(t *testing.T, raw []byte, process string) {
	t.Helper()
	for _, arg := range strings.Split(string(raw), "\x00") {
		if strings.HasPrefix(strings.ToLower(arg), "postgres://") || strings.HasPrefix(strings.ToLower(arg), "postgresql://") || strings.Contains(strings.ToLower(arg), "--dbname=postgres") {
			candidate := strings.TrimPrefix(arg, "--dbname=")
			u, err := url.Parse(candidate)
			if err == nil && u.User != nil {
				if _, hasPassword := u.User.Password(); hasPassword {
					t.Fatalf("%s argv contains a PostgreSQL URI with a password", process)
				}
			}
		}
		for _, field := range strings.Fields(arg) {
			key, _, found := strings.Cut(strings.ToLower(field), "=")
			if found && key == "password" {
				t.Fatalf("%s argv contains a keyword DSN password", process)
			}
		}
	}
}

func waitForNativeDump(t *testing.T, parent int, parentStart uint64, native string, timeout time.Duration) (int, uint64) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		switch presence, presenceErr := linuxTestProcessPresence(parent, parentStart); presence {
		case linuxTestPresenceAlive:
		case linuxTestPresenceGone:
			t.Fatal("CLI exited before an authenticated native pg_dump was observed")
		default:
			t.Fatalf("CLI process inspection refused before an authenticated native pg_dump was observed: %v", presenceErr)
		}
		entries, _ := os.ReadDir("/proc")
		for _, entry := range entries {
			pid, err := strconv.Atoi(entry.Name())
			if err != nil || pid == parent {
				continue
			}
			ppid, err := linuxTestParentPID(pid)
			if err != nil || ppid != parent {
				continue
			}
			exe, err := os.Stat(fmt.Sprintf("/proc/%d/exe", pid))
			want, statErr := os.Stat(native)
			if err != nil || statErr != nil || !os.SameFile(exe, want) {
				continue
			}
			// LocalPGCommand may first run the native client's --version probe.
			// Only select the credential-bearing dump invocation, not that short
			// lived version child.
			argv, argvErr := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid))
			if argvErr != nil || !bytes.Contains(argv, []byte("--dbname=")) {
				continue
			}
			start, err := linuxTestStartIdentity(pid)
			if err == nil {
				return pid, start
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for owned native pg_dump process")
	return 0, 0
}

func assertAuthenticatedDumpBlocked(t *testing.T, f *cliFixture, nativePID int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var count int
		err := f.admin.QueryRow(f.ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname=$1 AND usename='txharbor' AND wait_event_type='Lock' AND application_name LIKE 'pg_dump%'`, cliDBNameOf(t, f.dataDSN)).Scan(&count)
		if err == nil && count > 0 {
			return
		}
		// The native process is held by the table's ACCESS EXCLUSIVE lock; keep
		// polling server evidence rather than inferring from process liveness.
		switch presence, presenceErr := linuxTestProcessPresence(nativePID, 0); presence {
		case linuxTestPresenceAlive:
		case linuxTestPresenceGone:
			t.Fatal("native pg_dump exited before server-side authentication/lock evidence")
		default:
			t.Fatalf("native pg_dump inspection refused before server-side authentication/lock evidence: %v", presenceErr)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no authenticated pg_dump backend observed waiting on the real source-table lock")
}

func assertCLIOutputClean(t *testing.T, dir string, canaries []string) {
	t.Helper()
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, canary := range canaries {
			if bytes.Contains(data, []byte(canary)) {
				return fmt.Errorf("artifact contains canary")
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("backup artifact/manifest canary scan failed: %v", err)
	}
}

// cliBackupSuccess is the identity one successful backup CLI invocation
// reported in its own output line.
type cliBackupSuccess struct {
	backupID     string
	manifestPath string
	verification string
	operationID  string
	instanceID   string
}

// parseCLIBackupSuccess reads the shipped command's own success line. Any
// missing field is refused: the audit binding assertions must never run
// against an unbound or partially observed result.
func parseCLIBackupSuccess(t *testing.T, output string) cliBackupSuccess {
	t.Helper()
	var parsed cliBackupSuccess
	for _, field := range strings.Fields(output) {
		key, value, ok := strings.Cut(field, "=")
		if !ok {
			continue
		}
		switch key {
		case "backup_id":
			parsed.backupID = value
		case "manifest":
			parsed.manifestPath = value
		case "verification":
			parsed.verification = value
		case "operation_id":
			parsed.operationID = value
		case "instance":
			parsed.instanceID = value
		}
	}
	if parsed.backupID == "" || parsed.manifestPath == "" || parsed.verification == "" ||
		parsed.operationID == "" || parsed.instanceID == "" {
		t.Fatal("successful backup line did not report backup_id/manifest/verification/operation_id/instance; audit binding cannot be proven")
	}
	return parsed
}

// assertActualBackupManifest binds the command audit evidence to the actual
// manifest document this invocation wrote: same backup_id, the pg_dump_custom
// carrier kind, and the verification state the CLI itself reported. A backup
// manifest is accepted evidence of what was written, not proof of process
// cleanup or of restoration.
func assertActualBackupManifest(t *testing.T, success cliBackupSuccess, artifactDir string) {
	t.Helper()
	manifestPath := success.manifestPath
	if !filepath.IsAbs(manifestPath) {
		manifestPath = filepath.Join(artifactDir, manifestPath)
	}
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read the actual backup manifest reported by the CLI: %v", err)
	}
	var manifest struct {
		ManifestVersion string `json:"manifest_version"`
		BackupID        string `json:"backup_id"`
		Carrier         struct {
			Kind string `json:"kind"`
		} `json:"carrier"`
		Verification struct {
			State string `json:"state"`
		} `json:"verification"`
	}
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatal("actual backup manifest is not valid JSON")
	}
	if manifest.BackupID != success.backupID {
		t.Fatal("CLI success line backup_id does not match the actual backup manifest")
	}
	if manifest.Carrier.Kind != "pg_dump_custom" {
		t.Fatalf("actual backup manifest carrier kind = %q, want pg_dump_custom", manifest.Carrier.Kind)
	}
	if manifest.Verification.State != success.verification {
		t.Fatal("CLI success line verification state does not match the actual backup manifest")
	}
	switch manifest.Verification.State {
	case "unverified", "verified", "rejected":
	default:
		t.Fatalf("actual backup manifest verification state %q is not in the closed state set", manifest.Verification.State)
	}
	if manifest.ManifestVersion == "" {
		t.Fatal("actual backup manifest has no manifest_version")
	}
}

// assertControlAuditClean binds the exact command audit rows written by the
// tested invocations and scans those actual rows for credential canaries.
// Command audit rows carry a NULL instance_id (the instance binding lives in
// target JSON), so a WHERE instance_id=$fixture scan alone would miss them.
// Row contents are never echoed: only operation ids and counts appear in
// failure messages.
func assertControlAuditClean(t *testing.T, f *cliFixture, cliOutput, artifactDir string, canaries []string) {
	t.Helper()
	success := parseCLIBackupSuccess(t, cliOutput)
	if success.operationID != cliLiveBackupOperation {
		t.Fatalf("CLI reported operation_id %q, want %q", success.operationID, cliLiveBackupOperation)
	}
	if success.instanceID != f.instanceID {
		t.Fatal("CLI success line instance does not match the fixture's open recovery instance")
	}
	assertActualBackupManifest(t, success, artifactDir)

	// Positive evidence: the expected success command row must actually exist
	// with the exact tested operation id and the actual backup/instance
	// binding. A zero-row selection is a failed evidence claim, not a pass.
	var successRows, boundRows int
	if err := f.control.QueryRow(f.ctx, `
SELECT
  count(*) FILTER (WHERE action='backup' AND result='ok' AND operation_id=$1
                     AND target->>'instance_id'=$3::text AND target->>'backup_id'=$4::text),
  count(*)
FROM recovery_audit
WHERE instance_id=$3::text::uuid
   OR (instance_id IS NULL AND (operation_id IN ($1,$2)
        OR target->>'instance_id'=$3::text OR target->>'backup_id'=$4::text))`,
		cliLiveBackupOperation, cliLiveInterruptOperation, f.instanceID, success.backupID).
		Scan(&successRows, &boundRows); err != nil {
		t.Fatalf("read the tested command audit rows: %v", err)
	}
	if successRows < 1 {
		t.Fatalf("expected success command audit row for operation %q bound to instance %s and backup %s was not inspected (count=%d)",
			cliLiveBackupOperation, f.instanceID, success.backupID, successRows)
	}
	if boundRows == 0 {
		t.Fatal("audit evidence selection inspected zero rows; a zero-row selection proves nothing")
	}

	// Finite canary sweep over exactly those rows. Only safe text outputs are
	// read, and no row content is printed.
	rows, err := f.control.Query(f.ctx, `
SELECT coalesce(target::text,'') || ' ' || coalesce(detail::text,'') || ' ' ||
       actor || ' ' || action || ' ' || coalesce(operation_id,'')
FROM recovery_audit
WHERE instance_id=$1::text::uuid
   OR (instance_id IS NULL AND (operation_id IN ($2,$3)
        OR target->>'instance_id'=$1::text OR target->>'backup_id'=$4::text))
ORDER BY audit_id`,
		f.instanceID, cliLiveBackupOperation, cliLiveInterruptOperation, success.backupID)
	if err != nil {
		t.Fatalf("read recovery audit for canary scan: %v", err)
	}
	defer rows.Close()
	scanned := 0
	for rows.Next() {
		var row string
		if err := rows.Scan(&row); err != nil {
			t.Fatalf("scan recovery audit for canary scan: %v", err)
		}
		scanned++
		for _, canary := range canaries {
			if strings.Contains(row, canary) {
				t.Fatal("control-store audit row written by the tested CLI invocation contains an application canary")
			}
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate recovery audit canary scan: %v", err)
	}
	if scanned == 0 {
		t.Fatal("audit canary scan inspected zero rows; the finite sweep is not evidence")
	}
}
