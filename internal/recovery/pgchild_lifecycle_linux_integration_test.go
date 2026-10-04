//go:build integration && linux

package recovery

import (
	"context"
	"debug/elf"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	pgLifecycleHelperEnv  = "TXHARBOR_TEST_SUPERVISED_RESTORE_HELPER"
	pgLifecycleTargetEnv  = "TXHARBOR_TEST_SUPERVISED_RESTORE_TARGET"
	pgLifecycleArchiveEnv = "TXHARBOR_TEST_SUPERVISED_RESTORE_ARCHIVE"
	pgLifecycleBlockerEnv = "TXHARBOR_TEST_SUPERVISED_RESTORE_BLOCKER_PID"
)

// TestPGChildLifecycleSupervisedNativeRestoreParentDeathHelper is the child
// test process that owns TargetProcessRunner and a real native pg_restore.
func TestPGChildLifecycleSupervisedNativeRestoreParentDeathHelper(t *testing.T) {
	if os.Getenv(pgLifecycleHelperEnv) != "1" {
		return
	}
	ctx := context.Background()
	targetDSN := os.Getenv(pgLifecycleTargetEnv)
	archivePath := os.Getenv(pgLifecycleArchiveEnv)
	blockerPID, err := strconv.Atoi(os.Getenv(pgLifecycleBlockerEnv))
	if err != nil || blockerPID <= 0 || targetDSN == "" || archivePath == "" {
		os.Exit(2)
	}
	archive, err := os.Open(archivePath)
	if err != nil {
		os.Exit(3)
	}
	defer archive.Close()
	healthConn, err := pgx.Connect(ctx, targetDSN)
	if err != nil {
		os.Exit(4)
	}
	defer healthConn.Close(ctx)
	lockHealth := func(ctx context.Context) error {
		var held bool
		err := healthConn.QueryRow(ctx, `SELECT EXISTS (
			SELECT 1 FROM pg_locks
			WHERE pid=$1 AND relation='public.snapshot_probe_015'::regclass
			  AND mode='AccessExclusiveLock' AND granted)`, blockerPID).Scan(&held)
		if err != nil || !held {
			return errors.New("test blocker lock is not held")
		}
		return nil
	}
	_, runErr := (TargetProcessRunner{HealthCheckInterval: 20 * time.Millisecond}).RunPGCommandWithEnv(
		ctx, "pg_restore", []string{"--clean", "--if-exists", "--dbname=" + targetDSN},
		archive, io.Discard, io.Discard, os.Environ(), lockHealth)
	if runErr != nil {
		t.Fatal("supervised native restore command failed")
	}
}

// TestSupervisedNativePGRestoreDiesWhenItsOwningParentIsSIGKILLed exercises
// the direct pg_restore path only (not CLI restore's surrounding authorization
// or audit workflow). A real lock owner blocks a real archive restore while
// the process owning TargetProcessRunner is killed.
func TestSupervisedNativePGRestoreDiesWhenItsOwningParentIsSIGKILLed(t *testing.T) {
	native := requireNativePG18Restore(t)
	f := newBkpFixture(t)
	f.seedProbeTable(t)
	archivePath := filepath.Join(t.TempDir(), "restore-parent-death.dump")
	archiveFile, err := os.Create(archivePath)
	if err != nil {
		t.Fatal("create temporary archive")
	}
	var dumpStderr strings.Builder
	var sourceTable string
	if err := f.src.QueryRow(f.ctx, `SELECT to_regclass('public.snapshot_probe_015')::text`).Scan(&sourceTable); err != nil || sourceTable != "snapshot_probe_015" {
		t.Fatal("fixture source table is unavailable for archive export")
	}
	err = (LocalPGCommand{}).Run(f.ctx, "pg_dump", []string{
		"--format=custom", "--table=public.snapshot_probe_015", "--dbname=" + f.srcDSN,
	}, nil, archiveFile, &dumpStderr)
	closeErr := archiveFile.Close()
	if err != nil || closeErr != nil {
		diagnostic := strings.ToLower(dumpStderr.String())
		t.Fatalf("create real test archive: command_failed=%t file_close_failed=%t stderr_connection=%t stderr_table=%t stderr_permission=%t stderr_pgclient=%t",
			err != nil, closeErr != nil, strings.Contains(diagnostic, "connection"),
			strings.Contains(diagnostic, "table"), strings.Contains(diagnostic, "permission"), strings.Contains(diagnostic, "pg_dump: error"))
	}
	if info, err := os.Stat(archivePath); err != nil || info.Size() == 0 {
		t.Fatal("native pg_dump did not produce a non-empty custom archive")
	}
	targetDSN := f.createDatabase(t, "parent_death_restore")
	blocker, err := pgx.Connect(f.ctx, targetDSN)
	if err != nil {
		t.Fatal("connect restore target lock owner")
	}
	defer blocker.Close(context.Background())
	if _, err := blocker.Exec(f.ctx, `CREATE TABLE public.snapshot_probe_015 (id bigserial PRIMARY KEY, note text NOT NULL)`); err != nil {
		t.Fatal("seed conflicting restore table")
	}
	if _, err := blocker.Exec(f.ctx, "BEGIN"); err != nil {
		t.Fatal("begin target lock transaction")
	}
	defer blocker.Exec(context.Background(), "ROLLBACK")
	if _, err := blocker.Exec(f.ctx, `LOCK TABLE public.snapshot_probe_015 IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal("hold target table lock")
	}
	var blockerPID int
	if err := blocker.QueryRow(f.ctx, "SELECT pg_backend_pid()").Scan(&blockerPID); err != nil {
		t.Fatal("identify target lock owner backend")
	}

	helperExecutable, err := os.Executable()
	if err != nil {
		t.Fatal("locate integration test executable")
	}
	helper := exec.Command(helperExecutable, "-test.run=^TestPGChildLifecycleSupervisedNativeRestoreParentDeathHelper$")
	helper.Env = append(os.Environ(),
		pgLifecycleHelperEnv+"=1",
		pgLifecycleTargetEnv+"="+targetDSN,
		pgLifecycleArchiveEnv+"="+archivePath,
		pgLifecycleBlockerEnv+"="+strconv.Itoa(blockerPID),
	)
	helper.Stdout, helper.Stderr = io.Discard, io.Discard
	if err := helper.Start(); err != nil {
		t.Fatal("start supervised restore owner")
	}
	helperStart, err := linuxProcessStartIdentity(helper.Process.Pid)
	if err != nil {
		_ = helper.Process.Kill()
		_ = helper.Wait()
		t.Fatal("read supervised restore owner identity")
	}
	var child pgLifecycleProcessIdentity
	defer func() {
		if child.pid == 0 {
			child = findNativePGChild(helper.Process.Pid, native)
		}
		if helper.ProcessState == nil {
			_ = helper.Process.Kill()
			_ = helper.Wait()
		}
		if child.pid != 0 && pgLifecycleProcessAlive(child.pid, child.start) {
			_ = syscall.Kill(child.pid, syscall.SIGKILL)
			_ = waitForLinuxProcessGone(child.pid, child.start, 5*time.Second)
		}
	}()
	child = waitForNativePGChild(t, helper.Process.Pid, native, 30*time.Second)
	backendPID := waitForSupervisedRestoreLockWait(t, f, targetDSN, child, 30*time.Second)
	if err := helper.Process.Kill(); err != nil {
		t.Fatalf("SIGKILL supervised restore owner: %v", err)
	}
	assertPGLifecycleParentWasKilled(t, helper.Wait())
	childDrained := waitForLinuxProcessGone(child.pid, child.start, 5*time.Second)
	// The server backend may remain in its heavyweight-lock wait even after the
	// client socket closes. Release the real blocker only after recording that
	// the owning process is gone, then require the database session to drain.
	if childDrained {
		if _, err := blocker.Exec(context.Background(), "ROLLBACK"); err != nil {
			t.Fatal("release target blocker after native restore termination")
		}
		if err := blocker.Close(context.Background()); err != nil {
			t.Fatal("close target blocker after native restore termination")
		}
	}
	backendDrained := childDrained && waitForRestoreBackendGone(f, backendPID, 15*time.Second)
	if !childDrained || !backendDrained {
		ppid, state := linuxChildObservation(child.pid, child.start)
		backendState := supervisedRestoreBackendState(f, backendPID)
		if pgLifecycleProcessAlive(child.pid, child.start) {
			_ = syscall.Kill(child.pid, syscall.SIGKILL)
			_ = waitForLinuxProcessGone(child.pid, child.start, 5*time.Second)
		}
		t.Fatalf("supervised native restore parent death incomplete (child_pid=%d child_start=%d child_ppid=%d child_state=%s child_drained=%t backend_pid=%d backend_state=%s backend_drained=%t owner_start=%d)", child.pid, child.start, ppid, state, childDrained, backendPID, backendState, backendDrained, helperStart)
	}
}

func requireNativePG18Restore(t *testing.T) string {
	t.Helper()
	path, err := exec.LookPath("pg_restore")
	if err != nil {
		requireNativePG18Tool(t, "native pg_restore unavailable")
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		t.Fatalf("resolve native pg_restore")
	}
	file, err := elf.Open(resolved)
	if err != nil {
		t.Fatalf("native pg_restore is not an ELF executable")
	}
	_ = file.Close()
	version, err := exec.Command(resolved, "--version").Output()
	if err != nil || !strings.Contains(string(version), "pg_restore (PostgreSQL) 18.6") {
		requireNativePG18Tool(t, "native pg_restore is not PostgreSQL 18.6")
	}
	return resolved
}

func requireNativePG18Tool(t *testing.T, reason string) {
	t.Helper()
	if strings.EqualFold(os.Getenv("CI"), "true") || os.Getenv("TXHARBOR_REQUIRE_DOCKER") == "1" {
		t.Fatalf("required native tool unavailable: %s", reason)
	}
	t.Skip("NOT RUN: " + reason)
}

type pgLifecycleProcessIdentity struct {
	pid   int
	start uint64
}

func waitForNativePGChild(t *testing.T, parent int, native string, timeout time.Duration) pgLifecycleProcessIdentity {
	t.Helper()
	deadline := time.Now().Add(timeout)
	want, err := os.Stat(native)
	if err != nil {
		t.Fatal("stat pinned native pg_restore")
	}
	for time.Now().Before(deadline) {
		entries, _ := os.ReadDir("/proc")
		for _, entry := range entries {
			pid, err := strconv.Atoi(entry.Name())
			if err != nil || pid == parent {
				continue
			}
			ppid, err := pgLifecycleParentPID(pid)
			if err != nil || ppid != parent {
				continue
			}
			exe, err := os.Stat(fmt.Sprintf("/proc/%d/exe", pid))
			if err != nil || !os.SameFile(exe, want) {
				continue
			}
			argv, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid))
			if err != nil || !bytesContains(argv, []byte("--dbname=")) {
				continue
			}
			start, err := linuxProcessStartIdentity(pid)
			if err == nil {
				return pgLifecycleProcessIdentity{pid: pid, start: start}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for the owned native pg_restore client")
	return pgLifecycleProcessIdentity{}
}

func waitForSupervisedRestoreLockWait(t *testing.T, f *bkpFixture, targetDSN string, child pgLifecycleProcessIdentity, timeout time.Duration) int32 {
	t.Helper()
	deadline := time.Now().Add(timeout)
	dbName := bkpDBNameOf(t, f.adminDSN, targetDSN)
	for time.Now().Before(deadline) {
		var pid int32
		err := f.admin.QueryRow(f.ctx, `SELECT pid FROM pg_stat_activity
			WHERE datname=$1 AND usename='txharbor' AND application_name LIKE 'pg_restore%'
			  AND wait_event_type='Lock' ORDER BY pid LIMIT 1`, dbName).Scan(&pid)
		if err == nil && pid > 0 {
			return pid
		}
		if !pgLifecycleProcessAlive(child.pid, child.start) {
			t.Fatal("native pg_restore exited before server-side target-lock wait was observed")
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no authenticated native pg_restore backend observed waiting on the held target lock")
	return 0
}

func waitForRestoreBackendGone(f *bkpFixture, backendPID int32, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var exists bool
		err := f.admin.QueryRow(f.ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1)`, backendPID).Scan(&exists)
		if err == nil && !exists {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

func supervisedRestoreBackendState(f *bkpFixture, backendPID int32) string {
	var state string
	err := f.admin.QueryRow(f.ctx, `SELECT coalesce(state,'') || '/' || coalesce(wait_event_type,'') || '/' || coalesce(wait_event,'') FROM pg_stat_activity WHERE pid=$1`, backendPID).Scan(&state)
	if err != nil {
		return "gone"
	}
	return state
}

func bytesContains(value, part []byte) bool {
	return strings.Contains(string(value), string(part))
}

func findNativePGChild(parent int, native string) pgLifecycleProcessIdentity {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return pgLifecycleProcessIdentity{}
	}
	want, err := os.Stat(native)
	if err != nil {
		return pgLifecycleProcessIdentity{}
	}
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid == parent {
			continue
		}
		ppid, err := pgLifecycleParentPID(pid)
		if err != nil || ppid != parent {
			continue
		}
		exe, err := os.Stat(fmt.Sprintf("/proc/%d/exe", pid))
		if err != nil || !os.SameFile(exe, want) {
			continue
		}
		start, err := linuxProcessStartIdentity(pid)
		if err == nil {
			return pgLifecycleProcessIdentity{pid: pid, start: start}
		}
	}
	return pgLifecycleProcessIdentity{}
}

func pgLifecycleParentPID(pid int) (int, error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, err
	}
	close := strings.LastIndexByte(string(data), ')')
	if close < 0 {
		return 0, errors.New("malformed process stat")
	}
	fields := strings.Fields(string(data[close+1:]))
	if len(fields) < 2 {
		return 0, errors.New("short process stat")
	}
	return strconv.Atoi(fields[1])
}

func pgLifecycleProcessAlive(pid int, start uint64) bool {
	actual, err := linuxProcessStartIdentity(pid)
	return err == nil && actual == start
}
