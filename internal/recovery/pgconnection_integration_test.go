//go:build integration && linux

package recovery

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// This uses the actual PostgreSQL 18.6 ELF clients supplied by the pinned
// integration runner, not a fake client. It verifies native libpq's passfile
// handling as well as successful dump and restore authentication.
func TestNativePG18AnonymousCredentialFDAuthenticatesDumpAndRestore(t *testing.T) {
	for _, tool := range []string{"pg_dump", "pg_restore"} {
		out, err := exec.Command(tool, "--version").CombinedOutput()
		if err != nil || !strings.Contains(string(out), "(PostgreSQL) 18.6") {
			t.Fatalf("native %s must be PostgreSQL 18.6: %q err=%v", tool, out, err)
		}
	}
	ctx := context.Background()
	ctr, err := postgres.Run(ctx, "postgres:18.6-trixie",
		postgres.WithDatabase("txh_credential"), postgres.WithUsername("txh_credential"),
		postgres.WithPassword("native-secret"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })
	connDSN, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	admin, err := pgxpool.New(ctx, connDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if _, err := admin.Exec(ctx, `CREATE TABLE credential_fd_probe (value text NOT NULL); INSERT INTO credential_fd_probe VALUES ('native-auth-ok')`); err != nil {
		t.Fatal(err)
	}

	host, err := ctr.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port := ctrMappedPort(t, ctx, ctr)
	sourceDSN := fmt.Sprintf("postgres://txh_credential:native-secret@%s:%s/txh_credential?sslmode=disable", host, port)
	archive := filepath.Join(t.TempDir(), "native.dump")
	runPGWithAnonymousCredential(t, ctx, "pg_dump", []string{"--format=custom", "--file=" + archive, "--dbname=" + sourceDSN}, sourceDSN, nil)
	if _, err := admin.Exec(ctx, `CREATE DATABASE credential_restore_target`); err != nil {
		t.Fatal(err)
	}
	targetDSN := fmt.Sprintf("postgres://txh_credential:native-secret@%s:%s/credential_restore_target?sslmode=disable", host, port)
	archiveFile, err := os.Open(archive)
	if err != nil {
		t.Fatal(err)
	}
	defer archiveFile.Close()
	runPGWithAnonymousCredential(t, ctx, "pg_restore", []string{"--dbname=" + targetDSN}, targetDSN, archiveFile)
	target, err := pgxpool.New(ctx, targetDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	var restored string
	if err := target.QueryRow(ctx, `SELECT value FROM credential_fd_probe`).Scan(&restored); err != nil {
		t.Fatal(err)
	}
	if restored != "native-auth-ok" {
		t.Fatalf("restored value=%q", restored)
	}
	testPGCredentialParentSIGKILL(t, ctx, admin, sourceDSN+"&application_name=pg_dump-fd-canary")
}

// TestPGCredentialFDHelperProcess is the deliberately blocked intermediate
// parent used by the SIGKILL canary. The outer test owns both process IDs and
// guarantees child cleanup even after killing this helper.
func TestPGCredentialFDHelperProcess(t *testing.T) {
	if os.Getenv("TXHARBOR_PG_CREDENTIAL_HELPER") != "1" {
		return
	}
	dsnBytes, err := io.ReadAll(os.Stdin)
	if err != nil {
		os.Exit(21)
	}
	cmd := exec.Command("pg_dump", "--dbname="+string(dsnBytes))
	cleanup, err := protectPGChildArgsWithEnvironment(cmd, "pg_dump", cmd.Args[1:], []string{"PATH=" + os.Getenv("PATH"), "LANG=C"})
	if err != nil {
		os.Exit(22)
	}
	if len(cmd.ExtraFiles) == 0 {
		cleanup()
		os.Exit(23)
	}
	credential := cmd.ExtraFiles[len(cmd.ExtraFiles)-1]
	fd := 3 + uintptr(len(cmd.ExtraFiles)) - 1
	beforeStart, err := credential.Stat()
	if err != nil || !beforeStart.Mode().IsRegular() || beforeStart.Mode().Perm() != 0o600 {
		cleanup()
		os.Exit(25)
	}
	stat, ok := beforeStart.Sys().(*syscall.Stat_t)
	if !ok || stat.Nlink != 0 {
		cleanup()
		os.Exit(26)
	}
	if _, err := os.Stat(credential.Name()); !os.IsNotExist(err) {
		cleanup()
		os.Exit(27)
	}
	if err := cmd.Start(); err != nil {
		cleanup()
		os.Exit(24)
	}
	_, _ = fmt.Fprintf(os.Stdout, "%d %d\n", cmd.Process.Pid, fd)
	// Intentionally remain as the owner until the external parent SIGKILLs us.
	select {}
}

func testPGCredentialParentSIGKILL(t *testing.T, ctx context.Context, admin *pgxpool.Pool, dsn string) {
	t.Helper()
	lockConn, err := admin.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lockConn.Exec(ctx, `BEGIN; LOCK TABLE credential_fd_probe IN ACCESS EXCLUSIVE MODE`); err != nil {
		lockConn.Release()
		t.Fatal(err)
	}
	defer func() { _, _ = lockConn.Exec(context.Background(), `ROLLBACK`); lockConn.Release() }()

	helper := exec.Command(os.Args[0], "-test.run=^TestPGCredentialFDHelperProcess$")
	helper.Env = append(os.Environ(), "TXHARBOR_PG_CREDENTIAL_HELPER=1")
	helper.Stdin = strings.NewReader(dsn)
	helper.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stdout, err := helper.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var helperStderr strings.Builder
	helper.Stderr = &helperStderr
	if err := helper.Start(); err != nil {
		t.Fatal(err)
	}
	var childPID, fd uint64
	line := make(chan string, 1)
	go func() { got, _ := bufio.NewReader(stdout).ReadString('\n'); line <- got }()
	select {
	case got := <-line:
		if _, err := fmt.Sscanf(got, "%d %d", &childPID, &fd); err != nil || childPID == 0 || fd < 3 {
			_ = helper.Process.Kill()
			_ = helper.Wait()
			t.Fatalf("helper did not report child descriptor: line=%q stderr=%q err=%v", got, helperStderr.String(), err)
		}
	case <-time.After(10 * time.Second):
		_ = helper.Process.Kill()
		_ = helper.Wait()
		t.Fatalf("helper startup timed out: %s", helperStderr.String())
	}
	childAlive := true
	t.Cleanup(func() {
		if helper.ProcessState == nil {
			_ = helper.Process.Kill()
			_ = helper.Wait()
		}
		if childAlive {
			_ = syscall.Kill(int(childPID), syscall.SIGKILL)
			waitProcGone(int(childPID))
		}
	})

	assertProcHasNoSecret := func(pid int) {
		t.Helper()
		for _, leaf := range []string{"cmdline", "environ"} {
			data, err := os.ReadFile(fmt.Sprintf("/proc/%d/%s", pid, leaf))
			if err != nil {
				t.Fatalf("inspect %s of pid %d: %v", leaf, pid, err)
			}
			text := strings.ReplaceAll(string(data), "\x00", " ")
			if strings.Contains(text, "native-secret") || strings.Contains(text, dsn) || strings.Contains(text, "PGPASSWORD=") {
				t.Fatalf("credential visible in pid %d %s: %q", pid, leaf, text)
			}
		}
	}
	assertProcHasNoSecret(helper.Process.Pid)
	assertProcHasNoSecret(int(childPID))
	childEnv, err := os.ReadFile(fmt.Sprintf("/proc/%d/environ", childPID))
	if err != nil || !strings.Contains(string(childEnv), fmt.Sprintf("PGPASSFILE=/proc/self/fd/%d", fd)) {
		t.Fatalf("child did not receive the inherited passfile descriptor: env=%q err=%v", childEnv, err)
	}
	passfileLink := fmt.Sprintf("/proc/%d/fd/%d", childPID, fd)
	info, err := os.Stat(passfileLink)
	if err != nil {
		t.Fatalf("child passfile fd unavailable: %v", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		t.Fatalf("child passfile fd mode/type=%v", info.Mode())
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Nlink != 0 {
		t.Fatalf("child passfile fd is not anonymous: %#v", info.Sys())
	}
	link, err := os.Readlink(passfileLink)
	if err != nil || !strings.HasSuffix(link, " (deleted)") {
		t.Fatalf("child fd does not name an unlinked inode: link=%q err=%v", link, err)
	}
	if !waitForPGDumpLock(ctx, admin) {
		t.Fatal("blocked native pg_dump did not authenticate and reach the table lock")
	}

	if err := helper.Process.Kill(); err != nil {
		t.Fatalf("SIGKILL helper parent: %v", err)
	}
	if err := helper.Wait(); err == nil {
		t.Fatal("helper unexpectedly survived SIGKILL")
	}
	info, err = os.Stat(passfileLink)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("anonymous credential fd did not survive parent death: mode=%v err=%v", info, err)
	}
	if err := syscall.Kill(int(childPID), syscall.SIGKILL); err != nil {
		t.Fatalf("kill tracked pg_dump child: %v", err)
	}
	if !waitProcGone(int(childPID)) {
		t.Fatalf("pg_dump child %d remained after kill", childPID)
	}
	childAlive = false
	if _, err := os.Stat(passfileLink); err == nil {
		t.Fatal("credential descriptor path remains after child exit")
	}
}

func waitForPGDumpLock(ctx context.Context, admin *pgxpool.Pool) bool {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var waiting bool
		_ = admin.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE application_name='pg_dump-fd-canary' AND wait_event_type='Lock')`).Scan(&waiting)
		if waiting {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

func waitProcGone(pid int) bool {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(fmt.Sprintf("/proc/%d", pid)); os.IsNotExist(err) {
			return true
		}
		time.Sleep(25 * time.Millisecond)
	}
	return false
}

func ctrMappedPort(t *testing.T, ctx context.Context, ctr *postgres.PostgresContainer) string {
	t.Helper()
	port, err := ctr.MappedPort(ctx, "5432/tcp")
	if err != nil {
		t.Fatal(err)
	}
	return port.Port()
}

func runPGWithAnonymousCredential(t *testing.T, ctx context.Context, tool string, args []string, dsn string, stdin io.Reader) {
	t.Helper()
	cmd := exec.CommandContext(ctx, tool, args...)
	cleanup, err := protectPGChildArgsWithEnvironment(cmd, tool, args, []string{"PATH=" + os.Getenv("PATH"), "LANG=C"})
	if err != nil {
		t.Fatalf("protect %s arguments: %v", tool, err)
	}
	if len(cmd.ExtraFiles) == 0 {
		cleanup()
		t.Fatalf("%s did not receive a credential descriptor", tool)
	}
	credential := cmd.ExtraFiles[len(cmd.ExtraFiles)-1]
	fdInfo, err := credential.Stat()
	if err != nil {
		cleanup()
		t.Fatal(err)
	}
	if !fdInfo.Mode().IsRegular() || fdInfo.Mode().Perm() != 0o600 {
		cleanup()
		t.Fatalf("passfile mode/type=%v", fdInfo.Mode())
	}
	stat, ok := fdInfo.Sys().(*syscall.Stat_t)
	if !ok || stat.Nlink != 0 {
		cleanup()
		t.Fatalf("passfile is not anonymous (stat=%#v)", fdInfo.Sys())
	}
	if _, err := os.Stat(credential.Name()); !os.IsNotExist(err) {
		cleanup()
		t.Fatalf("credential pathname remains: %v", err)
	}
	if strings.Contains(strings.Join(cmd.Args, " "), "native-secret") || strings.Contains(strings.Join(cmd.Env, "\n"), "native-secret") || strings.Contains(strings.Join(cmd.Env, "\n"), dsn) {
		cleanup()
		t.Fatalf("credential appeared in child argv/environment: argv=%q env=%q", cmd.Args, cmd.Env)
	}
	var stderr strings.Builder
	cmd.Stderr = &stderr
	cmd.Stdin = stdin
	if err := cmd.Run(); err != nil {
		cleanup()
		t.Fatalf("native %s failed: %v stderr=%q", tool, err, stderr.String())
	}
	cleanup()
	if _, err := credential.Stat(); err == nil {
		t.Fatal("credential descriptor remained open after child completion")
	}
}
