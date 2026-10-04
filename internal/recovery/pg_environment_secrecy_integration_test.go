//go:build integration && linux

package recovery

import (
	"context"
	"fmt"
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

// Exercises the real PG18 libpq child while it is alive and authenticated, not
// just the environment filter in isolation.
func TestNativePG18ChildEnvironmentContainsNoApplicationSecrets(t *testing.T) {
	if out, err := exec.Command("pg_dump", "--version").CombinedOutput(); err != nil || !strings.Contains(string(out), "(PostgreSQL) 18.6") {
		if os.Getenv("CI") == "true" || os.Getenv("TXHARBOR_REQUIRE_DOCKER") == "1" {
			t.Fatalf("native pg_dump must be PostgreSQL 18.6: %q err=%v", out, err)
		}
		t.Skipf("NOT RUN: native PostgreSQL 18.6 pg_dump unavailable: %v", err)
	}
	ctx := context.Background()
	ctr, err := postgres.Run(ctx, "postgres:18.6-trixie",
		postgres.WithDatabase("txh_env_probe"), postgres.WithUsername("txh_env_probe"), postgres.WithPassword("pg18-env-password"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })
	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if _, err := admin.Exec(ctx, `CREATE TABLE env_probe (value text); INSERT INTO env_probe VALUES ('ok')`); err != nil {
		t.Fatal(err)
	}
	lockConn, err := admin.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lockConn.Exec(ctx, `BEGIN; LOCK TABLE env_probe IN ACCESS EXCLUSIVE MODE`); err != nil {
		lockConn.Release()
		t.Fatal(err)
	}
	defer func() { _, _ = lockConn.Exec(context.Background(), `ROLLBACK`); lockConn.Release() }()

	canaries := []string{
		"postgres://u:target-canary@db/target", "postgres://u:control-canary@db/control",
		"rpc-canary", "signer-canary", "vault-canary", "broker-canary", "custom-canary",
		"gate-canary", "deploy-canary",
	}
	envKeys := []string{"TXHARBOR_RECOVERY_TARGET_DSN", "TXHARBOR_RECOVERY_CONTROL_DSN", "TXHARBOR_PG_DSN", "TXHARBOR_RPC_URL", "TXHARBOR_SIGNER_TOKEN", "VAULT_TOKEN", "BROKER_DSN", "TESTSECRET", "TXHARBOR_RECOVERY_GATE_DSN", "TXHARBOR_RECOVERY_DEPLOYMENT_ADMIN_DSN"}
	old := make(map[string]string, len(envKeys))
	wasSet := make(map[string]bool, len(envKeys))
	for i, key := range envKeys {
		old[key], wasSet[key] = os.LookupEnv(key)
		_ = os.Setenv(key, canaries[i%len(canaries)])
	}
	t.Cleanup(func() {
		for _, key := range envKeys {
			if wasSet[key] {
				_ = os.Setenv(key, old[key])
			} else {
				_ = os.Unsetenv(key)
			}
		}
	})

	host, err := ctr.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port := ctrMappedPort(t, ctx, ctr)
	pgDSN := fmt.Sprintf("postgres://txh_env_probe:pg18-env-password@%s:%s/txh_env_probe?sslmode=disable&application_name=pg_dump-fd-canary", host, port)
	archive := filepath.Join(t.TempDir(), "blocked.dump")
	cmd := exec.Command("pg_dump", "--format=custom", "--file="+archive, "--dbname="+pgDSN)
	cleanup, err := protectPGChildArgsWithEnvironment(cmd, "pg_dump", cmd.Args[1:], os.Environ())
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		cleanup()
		t.Fatal(err)
	}
	pid := cmd.Process.Pid
	t.Cleanup(func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_, _ = cmd.Process.Wait()
		}
		cleanup()
	})
	if !waitForPGDumpLock(ctx, admin) {
		t.Fatal("native pg_dump did not authenticate and block on the locked table")
	}
	for _, leaf := range []string{"environ", "cmdline"} {
		data, err := os.ReadFile(fmt.Sprintf("/proc/%d/%s", pid, leaf))
		if err != nil {
			t.Fatalf("inspect native pg_dump %s: %v", leaf, err)
		}
		text := strings.ReplaceAll(string(data), "\x00", " ")
		for _, canary := range canaries {
			if strings.Contains(text, canary) {
				t.Fatalf("application canary present in pg_dump %s", leaf)
			}
		}
		if strings.Contains(text, "pg18-env-password") || strings.Contains(text, pgDSN) || strings.Contains(text, "PGPASSWORD=") {
			t.Fatalf("database credential visible in native pg_dump %s", leaf)
		}
	}
	childEnv, err := os.ReadFile(fmt.Sprintf("/proc/%d/environ", pid))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(childEnv), "PATH=") || !strings.Contains(string(childEnv), "LANG=") {
		t.Fatalf("runtime environment not retained: %q", childEnv)
	}
	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	cleanup()
	time.Sleep(10 * time.Millisecond)
}
