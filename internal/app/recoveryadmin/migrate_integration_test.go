//go:build integration

// migrate_integration_test.go runs `recovery-admin migrate up|status` against
// a real PostgreSQL 18.6 (testcontainers): the positive initialize/status/
// idempotent-up lifecycle, the same-target and connectivity refusals, and the
// T069 version-guard negative paths (unknown version, recovery objects
// without a version table) with their audit annotation.
//
// Docker provider missing: the package fails under CI=true or
// TXHARBOR_REQUIRE_DOCKER=1 and reports NOT RUN locally (exit 0).
package recoveryadmin

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

const migratePGImage = "postgres:18.6-trixie"

func TestMain(m *testing.M) {
	os.Exit(runRecoveryAdminIntegration(m))
}

func runRecoveryAdminIntegration(m *testing.M) int {
	ctx := context.Background()
	if !migrateDockerHealthy(ctx) {
		if os.Getenv("CI") == "true" || os.Getenv("TXHARBOR_REQUIRE_DOCKER") == "1" {
			fmt.Fprintln(os.Stderr, "recoveryadmin integration: docker provider unavailable; failing package (CI=true or TXHARBOR_REQUIRE_DOCKER=1): exit 1")
			return 1
		}
		fmt.Fprintln(os.Stderr, "recoveryadmin integration: docker provider unavailable; skipping package (NOT RUN, exit 0)")
		return 0
	}
	return m.Run()
}

func migrateDockerHealthy(ctx context.Context) (healthy bool) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "recoveryadmin integration: docker provider check panicked: %v\n", r)
			healthy = false
		}
	}()
	provider, err := testcontainers.ProviderDocker.GetProvider()
	if err != nil {
		fmt.Fprintf(os.Stderr, "recoveryadmin integration: docker provider: %v\n", err)
		return false
	}
	if err := provider.Health(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "recoveryadmin integration: docker health: %v\n", err)
		return false
	}
	return true
}

func startMigratePostgres(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	ctr, err := postgres.Run(ctx, migratePGImage,
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
	return dsn
}

func migrateWithDatabase(t *testing.T, dsn, database string) string {
	t.Helper()
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	u.Path = "/" + database
	return u.String()
}

func migrateWithPassword(t *testing.T, dsn, password string) string {
	t.Helper()
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	u.User = url.UserPassword(u.User.Username(), password)
	return u.String()
}

func migrateTestPool(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()
	pool, err := db.OpenPool(context.Background(), dsn, 10*time.Second)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func migrateCountRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, sql, args...).Scan(&n); err != nil {
		t.Fatalf("count (%s): %v", sql, err)
	}
	return n
}

func TestMigrateIntegrationPositiveLifecycle(t *testing.T) {
	baseDSN := startMigratePostgres(t)
	env := map[string]string{
		config.EnvRecoveryControlDSN: baseDSN,
		config.EnvPGDSN:              migrateWithDatabase(t, baseDSN, "txharbor_data"),
	}

	code, stdout, stderr := runMigrate(t, []string{"status"}, env)
	if code != 0 {
		t.Fatalf("fresh status: exit=%d stderr=%q", code, stderr)
	}
	for _, want := range []string{"control_store=uninitialized", "current_version=0", "target_version=1", "pending=1"} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("fresh status must report %q: %q", want, stdout)
		}
	}

	code, upOut, upErr := runMigrate(t, []string{"up"}, env)
	if code != 0 {
		t.Fatalf("migrate up: exit=%d stdout=%q stderr=%q", code, upOut, upErr)
	}
	if !strings.Contains(upOut, "applied=1") || !strings.Contains(upOut, "current_version=1") {
		t.Fatalf("up must report the applied migration: %q", upOut)
	}

	ctx := context.Background()
	pool := migrateTestPool(t, baseDSN)
	if got := migrateCountRows(t, ctx, pool,
		"SELECT count(*) FROM goose_db_version WHERE is_applied AND version_id = 1"); got != 1 {
		t.Fatalf("control store must be at version 1, found %d rows", got)
	}
	if got := migrateCountRows(t, ctx, pool,
		"SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE 'recovery\\_%'"); got != len(controlstore.ControlTableNames()) {
		t.Fatalf("expected %d recovery tables after migration, got %d", len(controlstore.ControlTableNames()), got)
	}
	if _, err := controlstore.NewStore(ctx, pool); err != nil {
		t.Fatalf("NewStore over the migrated control store: %v", err)
	}
	if got := migrateCountRows(t, ctx, pool,
		"SELECT count(*) FROM recovery_audit WHERE action = 'migrate_up' AND result = 'ok'"); got != 1 {
		t.Fatalf("exactly one migrate_up ok audit note expected, got %d", got)
	}

	code, secondOut, _ := runMigrate(t, []string{"up"}, env)
	if code != 0 || !strings.Contains(secondOut, "already_current=true") {
		t.Fatalf("second up must be an idempotent no-op: exit=%d stdout=%q", code, secondOut)
	}
	if got := migrateCountRows(t, ctx, pool,
		"SELECT count(*) FROM recovery_audit WHERE action = 'migrate_up'"); got != 1 {
		t.Fatalf("no-op up must not add audit rows, got %d", got)
	}

	code, statusOut, _ := runMigrate(t, []string{"status"}, env)
	if code != 0 {
		t.Fatalf("status after up: exit=%d", code)
	}
	for _, want := range []string{"control_store=initialized", "current_version=1", "target_version=1", "pending=none"} {
		if !strings.Contains(statusOut, want) {
			t.Fatalf("status after up must report %q: %q", want, statusOut)
		}
	}
	for _, out := range []string{stdout, upOut, upErr, secondOut, statusOut} {
		if strings.Contains(out, "postgres://") || strings.Contains(out, "password=") {
			t.Fatalf("command output must never carry a plaintext DSN: %q", out)
		}
	}
}

func TestMigrateIntegrationRefusesSameTargetWithoutWrites(t *testing.T) {
	baseDSN := startMigratePostgres(t)
	env := map[string]string{
		config.EnvRecoveryControlDSN: baseDSN,
		config.EnvPGDSN:              baseDSN,
	}
	code, _, stderr := runMigrate(t, []string{"up"}, env)
	if code != 1 || !strings.Contains(stderr, "same database target") {
		t.Fatalf("same target must refuse: exit=%d stderr=%q", code, stderr)
	}
	pool := migrateTestPool(t, baseDSN)
	var exists bool
	if err := pool.QueryRow(context.Background(), "SELECT to_regclass('goose_db_version') IS NOT NULL").Scan(&exists); err != nil {
		t.Fatalf("check version table: %v", err)
	}
	if exists {
		t.Fatal("a refused migrate up must not create the control-store version table")
	}
}

func TestMigrateIntegrationRefusesUnreachableControlStore(t *testing.T) {
	baseDSN := startMigratePostgres(t)
	env := map[string]string{
		config.EnvRecoveryControlDSN: "postgres://txharbor:controlsecret@127.0.0.1:1/control?sslmode=disable",
		config.EnvPGDSN:              baseDSN,
	}
	code, stdout, stderr := runMigrate(t, []string{"up"}, env)
	if code != 1 || !strings.Contains(stderr, "control store unavailable") {
		t.Fatalf("unreachable control store must refuse: exit=%d stderr=%q", code, stderr)
	}
	for _, secret := range []string{"controlsecret", "postgres://"} {
		if strings.Contains(stdout, secret) || strings.Contains(stderr, secret) {
			t.Fatalf("refusal must not leak %q: stdout=%q stderr=%q", secret, stdout, stderr)
		}
	}
}

func TestMigrateIntegrationRefusesBadCredentials(t *testing.T) {
	baseDSN := startMigratePostgres(t)
	env := map[string]string{
		config.EnvRecoveryControlDSN: migrateWithPassword(t, baseDSN, "wrongsecret"),
		config.EnvPGDSN:              migrateWithDatabase(t, baseDSN, "txharbor_data"),
	}
	code, stdout, stderr := runMigrate(t, []string{"status"}, env)
	if code != 1 {
		t.Fatalf("bad credentials must refuse with exit 1, got %d (stderr=%q)", code, stderr)
	}
	if strings.Contains(stdout, "wrongsecret") || strings.Contains(stderr, "wrongsecret") {
		t.Fatalf("refusal must not leak the password: stdout=%q stderr=%q", stdout, stderr)
	}
}

func TestMigrateIntegrationRefusesUnknownVersion(t *testing.T) {
	baseDSN := startMigratePostgres(t)
	env := map[string]string{
		config.EnvRecoveryControlDSN: baseDSN,
		config.EnvPGDSN:              migrateWithDatabase(t, baseDSN, "txharbor_data"),
	}
	if code, _, stderr := runMigrate(t, []string{"up"}, env); code != 0 {
		t.Fatalf("initial up: exit=%d stderr=%q", code, stderr)
	}
	ctx := context.Background()
	pool := migrateTestPool(t, baseDSN)
	if _, err := pool.Exec(ctx, "UPDATE goose_db_version SET version_id = 999 WHERE is_applied"); err != nil {
		t.Fatalf("forge unknown version: %v", err)
	}

	code, _, stderr := runMigrate(t, []string{"status"}, env)
	if code != 1 {
		t.Fatalf("status over an unknown version must refuse with exit 1, got %d", code)
	}
	for _, want := range []string{"control_store_unavailable", "observed_version=999", "target_version=1"} {
		if !strings.Contains(stderr, want) {
			t.Fatalf("refusal must contain %q: %q", want, stderr)
		}
	}
	var result, refusalClass, observed string
	if err := pool.QueryRow(ctx, `
SELECT result, refusal_class, detail->>'observed_version'
FROM recovery_audit WHERE action = 'migrate_status' ORDER BY audit_id DESC LIMIT 1`).
		Scan(&result, &refusalClass, &observed); err != nil {
		t.Fatalf("read version refusal audit: %v", err)
	}
	if result != "refused" || refusalClass != "control_store_unavailable" || observed != "999" {
		t.Fatalf("unexpected refusal audit row: result=%s class=%s observed=%s", result, refusalClass, observed)
	}

	code, _, upErr := runMigrate(t, []string{"up"}, env)
	if code != 1 {
		t.Fatalf("up over an unknown version must refuse with exit 1, got %d", code)
	}
	if !strings.Contains(upErr, "control_store_unavailable") {
		t.Fatalf("up refusal must be expressed as control_store_unavailable: %q", upErr)
	}
	if got := migrateCountRows(t, ctx, pool,
		"SELECT count(*) FROM goose_db_version WHERE is_applied AND version_id = 1"); got != 0 {
		t.Fatalf("up must not silently rewrite the unknown version back to 0001, got %d rows at version 1", got)
	}
	var maxVersion int64
	if err := pool.QueryRow(ctx,
		"SELECT max(version_id) FROM goose_db_version WHERE is_applied").Scan(&maxVersion); err != nil {
		t.Fatalf("read max version: %v", err)
	}
	if maxVersion != 999 {
		t.Fatalf("unknown version must be preserved, max=%d", maxVersion)
	}
}

func TestMigrateIntegrationRefusesObjectsWithoutVersionTable(t *testing.T) {
	baseDSN := startMigratePostgres(t)
	env := map[string]string{
		config.EnvRecoveryControlDSN: baseDSN,
		config.EnvPGDSN:              migrateWithDatabase(t, baseDSN, "txharbor_data"),
	}
	ctx := context.Background()
	pool := migrateTestPool(t, baseDSN)
	if _, err := pool.Exec(ctx, "CREATE TABLE recovery_instance (placeholder int)"); err != nil {
		t.Fatalf("create stray recovery object: %v", err)
	}
	code, _, stderr := runMigrate(t, []string{"status"}, env)
	if code != 1 {
		t.Fatalf("objects without a version table must refuse with exit 1, got %d", code)
	}
	for _, want := range []string{"without a goose_db_version version table", "audit_note=not_attempted"} {
		if !strings.Contains(stderr, want) {
			t.Fatalf("refusal must contain %q: %q", want, stderr)
		}
	}
	if code, _, _ := runMigrate(t, []string{"up"}, env); code != 1 {
		t.Fatalf("up must also refuse, got exit %d", code)
	}
	if got := migrateCountRows(t, ctx, pool,
		"SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'goose_db_version'"); got != 0 {
		t.Fatalf("refused migrate must not create the version table, got %d", got)
	}
}
