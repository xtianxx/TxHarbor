//go:build integration

package db

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pressly/goose/v3/lock"
)

// repoRoot locates the module root regardless of the test working directory.
func repoRoot(t *testing.T) string {
	t.Helper()
	out, err := exec.Command("go", "env", "GOMOD").Output()
	if err != nil {
		t.Fatalf("go env GOMOD: %v", err)
	}
	modFile := strings.TrimSpace(string(out))
	if modFile == "" || modFile == os.DevNull {
		t.Fatalf("cannot locate go.mod: %q", modFile)
	}
	return filepath.Dir(modFile)
}

// buildTxharbor compiles the real CLI so concurrency is exercised across
// processes, not goroutines.
func buildTxharbor(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "txharbor")
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/txharbor")
	cmd.Dir = repoRoot(t)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build txharbor: %v\n%s", err, out)
	}
	return bin
}

func cliEnv(dsn string, lockTimeout time.Duration) []string {
	return append(os.Environ(),
		"TXHARBOR_PG_DSN="+dsn,
		"TXHARBOR_RPC_URL=http://127.0.0.1:1",
		"TXHARBOR_CHAIN_ID=31337",
		"TXHARBOR_START_HEIGHT=0",
		"TXHARBOR_LOG_START_HEIGHT=0",
		"TXHARBOR_LOG_CONTRACTS=0x1111111111111111111111111111111111111111",
		"TXHARBOR_DEPOSIT_START_HEIGHT=0",
		"TXHARBOR_DEPOSIT_CONTRACTS=0x1111111111111111111111111111111111111111:0",
		"TXHARBOR_DEPOSIT_WATCH_ADDRESSES=0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0",
		"TXHARBOR_CONFIRMATION_DEPTH=10",
		fmt.Sprintf("TXHARBOR_MIGRATE_LOCK_TIMEOUT=%s", lockTimeout),
	)
}

// TestConcurrentMigrateProcessesSerialize covers FR-006 (SC-005, US4-4):
// concurrent migrate processes against one database serialize on the goose
// session lock, apply each version exactly once and never fork the version
// table.
func TestConcurrentMigrateProcessesSerialize(t *testing.T) {
	dsn := startPostgres(t)
	bin := buildTxharbor(t)

	const processes = 3
	var wg sync.WaitGroup
	type result struct {
		code   int
		stdout string
		stderr string
	}
	results := make([]result, processes)
	start := make(chan struct{})

	for i := 0; i < processes; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cmd := exec.Command(bin, "migrate", "up")
			cmd.Env = cliEnv(dsn, 30*time.Second)
			var stdout, stderr bytes.Buffer
			cmd.Stdout = &stdout
			cmd.Stderr = &stderr
			<-start
			err := cmd.Run()
			code := 0
			if exitErr, ok := err.(*exec.ExitError); ok {
				code = exitErr.ExitCode()
			} else if err != nil {
				t.Errorf("process %d: %v", i, err)
				code = -1
			}
			results[i] = result{code: code, stdout: stdout.String(), stderr: stderr.String()}
		}(i)
	}
	close(start)
	wg.Wait()

	applied := 0
	for i, res := range results {
		if res.code != 0 {
			t.Fatalf("process %d exit code = %d, stderr=%s", i, res.code, res.stderr)
		}
		if !strings.Contains(res.stdout, "applied=") {
			t.Fatalf("process %d output lacks contract summary: %q", i, res.stdout)
		}
		var n int
		if _, err := fmt.Sscanf(strings.Fields(res.stdout)[0], "applied=%d", &n); err != nil {
			t.Fatalf("process %d output %q: %v", i, res.stdout, err)
		}
		applied += n
	}
	files, err := MigrationFiles(Migrations)
	if err != nil {
		t.Fatalf("list embedded migrations: %v", err)
	}
	if applied != len(files) {
		t.Fatalf("total applied = %d across %d processes, want exactly %d (one process applies every version once)", applied, processes, len(files))
	}

	sqlDB := openTestSQL(t, dsn)
	for _, f := range files {
		var rows int
		if err := sqlDB.QueryRowContext(context.Background(),
			"SELECT count(*) FROM goose_db_version WHERE version_id = $1 AND is_applied", f.Version).Scan(&rows); err != nil {
			t.Fatalf("count version rows: %v", err)
		}
		if rows != 1 {
			t.Fatalf("version table forked: %d applied rows for version %d, want 1", rows, f.Version)
		}
	}
}

// TestMigrateLockWaitTimeoutExitsNonZero covers FR-006: when the lock cannot
// be acquired within TXHARBOR_MIGRATE_LOCK_TIMEOUT the CLI must fail loudly
// with a non-zero exit instead of waiting forever.
func TestMigrateLockWaitTimeoutExitsNonZero(t *testing.T) {
	dsn := startPostgres(t)
	ctx := context.Background()

	// Hold the goose advisory lock from the test itself.
	holder, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open lock holder: %v", err)
	}
	defer holder.Close()
	if _, err := holder.ExecContext(ctx, "SELECT pg_advisory_lock($1)", lock.DefaultLockID); err != nil {
		t.Fatalf("acquire advisory lock: %v", err)
	}
	defer holder.ExecContext(ctx, "SELECT pg_advisory_unlock($1)", lock.DefaultLockID)

	bin := buildTxharbor(t)
	cmd := exec.Command(bin, "migrate", "up")
	cmd.Env = cliEnv(dsn, time.Second)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	start := time.Now()
	err = cmd.Run()
	elapsed := time.Since(start)

	exitErr, ok := err.(*exec.ExitError)
	if !ok || exitErr.ExitCode() == 0 {
		t.Fatalf("migrate with held lock: exit = %v (stdout=%s), want non-zero", err, stdout.String())
	}
	if elapsed > 30*time.Second {
		t.Fatalf("lock wait took %s, expected bounded wait", elapsed)
	}
	if !strings.Contains(stderr.String(), "lock") {
		t.Fatalf("stderr %q does not report the lock failure", stderr.String())
	}
}
