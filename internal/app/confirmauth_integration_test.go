//go:build integration

package app

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/db"
)

// startConfirmAuthPostgres boots one container database with all migrations
// applied (mirrors the indexer *_integration_test.go harness). It skips
// (never passes) when no Docker provider is available.
func startConfirmAuthPostgres(t *testing.T) string {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()
	ctr, err := postgres.Run(ctx, "postgres:18.6-trixie",
		postgres.WithDatabase("txharbor"),
		postgres.WithUsername("txharbor"),
		postgres.WithPassword("txharbor"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2)),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })
	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres connection string: %v", err)
	}
	if err := db.MigrateUp(ctx, db.MigrateOptions{
		DSN:            dsn,
		LockTimeout:    5 * time.Second,
		ConnectTimeout: 5 * time.Second,
	}, io.Discard); err != nil {
		t.Fatalf("migrate up: %v", err)
	}
	return dsn
}

func confirmAuthEnv(dsn string) map[string]string {
	env := fullServeEnv("127.0.0.1:0")
	env[config.EnvPGDSN] = dsn
	env[config.EnvChainID] = "31337"
	return env
}

func confirmAuthSeedPolicy(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
INSERT INTO confirmation_policy_history
    (chain_id, policy_seq, threshold, prev_seq, operator, request_id)
VALUES ($1, 1, 10, NULL, 'bootstrap', NULL)`, chainID); err != nil {
		t.Fatalf("seed confirmation_policy_history: %v", err)
	}
}

func confirmAuthPolicyRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64) int {
	t.Helper()
	var k int
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM confirmation_policy_history WHERE chain_id = $1`, chainID).Scan(&k); err != nil {
		t.Fatalf("count policy rows: %v", err)
	}
	return k
}

func confirmAuthRun(ctx context.Context, env map[string]string, args ...string) (int, string, string) {
	var stdout, stderr bytes.Buffer
	code := ConfirmAuth(ctx, args, Deps{
		Getenv: fakeEnv(env),
		Stdout: &stdout,
		Stderr: &stderr,
	})
	return code, stdout.String(), stderr.String()
}

// TestConfirmAuthCmdOkReplayRefused covers the T029 carrier against a real
// database: a guarded switch exits 0 and prints the recorded policy_seq; the
// same request_id replays deterministically (0, recorded, zero new rows);
// a stale expected_old_seq is refused with exit 1, redacted stderr, and
// zero new rows.
func TestConfirmAuthCmdOkReplayRefused(t *testing.T) {
	dsn := startConfirmAuthPostgres(t)
	ctx := context.Background()
	env := confirmAuthEnv(dsn)
	const chainID = int64(31337)

	pool, err := db.OpenPool(ctx, dsn, 5*time.Second)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	defer pool.Close()
	confirmAuthSeedPolicy(t, ctx, pool, chainID)

	sw := []string{
		"--request-id", "cmd-ok-1",
		"--expected-old-seq", "1",
		"--new-threshold", "25",
		"--operator", "op-cmd",
		"--reason", "cmd switch",
	}
	code, stdout, stderr := confirmAuthRun(ctx, env, sw...)
	if code != 0 {
		t.Fatalf("switch exit code = %d, want 0; stderr=%s", code, stderr)
	}
	for _, want := range []string{"policy_seq=2", "threshold=25", "recorded=false"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("stdout %q lacks %q", stdout, want)
		}
	}
	if k := confirmAuthPolicyRows(t, ctx, pool, chainID); k != 2 {
		t.Fatalf("policy rows after switch = %d, want 2", k)
	}

	// Same intent replays deterministically: same recorded result, no row.
	code, stdout, stderr = confirmAuthRun(ctx, env, sw...)
	if code != 0 {
		t.Fatalf("replay exit code = %d, want 0; stderr=%s", code, stderr)
	}
	for _, want := range []string{"policy_seq=2", "threshold=25", "recorded=true"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("replay stdout %q lacks %q", stdout, want)
		}
	}
	if k := confirmAuthPolicyRows(t, ctx, pool, chainID); k != 2 {
		t.Fatalf("policy rows after replay = %d, want 2 (replay appends nothing)", k)
	}

	// Stale expected_old_seq is refused: exit 1, redacted stderr, zero rows.
	stale := []string{
		"--request-id", "cmd-stale-1",
		"--expected-old-seq", "1",
		"--new-threshold", "30",
		"--operator", "op-cmd",
		"--reason", "stale attempt",
	}
	code, _, stderr = confirmAuthRun(ctx, env, stale...)
	if code != 1 {
		t.Fatalf("stale switch exit code = %d, want 1; stderr=%s", code, stderr)
	}
	if strings.Contains(stderr, ":txharbor@") {
		t.Errorf("stderr %q leaks the DSN password", stderr)
	}
	if k := confirmAuthPolicyRows(t, ctx, pool, chainID); k != 2 {
		t.Fatalf("policy rows after refused switch = %d, want 2 (zero state change)", k)
	}
}
