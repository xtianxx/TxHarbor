//go:build integration

// db_shared_pg_test.go owns the package-wide PostgreSQL lifecycle for the
// internal/db integration suite (Phase A container-lifecycle optimization;
// same pattern as internal/indexer/indexer_shared_pg_test.go,
// internal/withdrawal/withdrawal_shared_pg_test.go and
// internal/txlifecycle/txlifecycle_shared_pg_test.go).
//
// Before: every startPostgres call booted its own postgres:18.6-trixie
// container (38 callers), so one `go test -tags integration ./internal/db/`
// run created one container per integration test.
//
// After: TestMain boots exactly one shared container and startPostgres derives
// a uniquely named, EMPTY database inside it for each non-whitelisted test.
// Isolation is preserved at the database level: every call gets a fresh
// database (CREATE DATABASE; DROP DATABASE ... WITH (FORCE) in t.Cleanup), so
// no test can observe another test's rows, schema or goose_db_version state.
//
// The derived database is deliberately NOT pre-migrated. This package's tests
// call MigrateUp/MigrateStatus/Inspect themselves — including from zero
// (empty-DB assertions), with partial and overlay migration FS sets, and with
// failing migrations. Each test therefore still runs real migrations against
// its own fresh database: full migration coverage per test (no TEMPLATE
// cloning) and no template1 lock contention. Every test entry name,
// assertion, build tag and timeout is untouched.
//
// Audit of the migration/global-state candidates named by the plan:
//   - migrate x3 (migrate_integration_test.go) and the non-down scratch lanes
//     (T040, T041, T036 A/B) assert only their own database's state
//     (goose_db_version rows, applied sets, CheckCompatibility); the embedded
//     migrations FS is read-only and newProvider disables goose's global
//     registry.
//   - migrate_concurrent x2 relies on goose's session advisory lock, which
//     PostgreSQL scopes per database (the advisory lock tag embeds
//     MyDatabaseId), and this package has no t.Parallel test, so
//     cross-database interference is impossible.
//   - The down/destructive lanes run goose Down/DownTo and DROP schema on
//     purpose; they are whitelisted to a dedicated container per test
//     (dbDedicatedContainerTests), mirroring internal/txlifecycle's
//     schema-destructive whitelist (testfixture_test.go:78-102): the
//     whitelist keeps their blast radius out of the shared container even
//     though per-test databases would already isolate them.
//
// Deliberate non-goals and follow-ups (documented, not implemented here):
//   - No cluster-wide pg_stat_activity probe exists in this package today. If
//     one is added, it must filter `datname = current_database()` or move to a
//     dedicated container before any test gains t.Parallel.
//   - TXHARBOR_KEEP_DB=1 (or a failed test) keeps that test's database for
//     triage; the container still terminates with the test binary.
package db

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	dbPGImage      = "postgres:18.6-trixie"
	dbBaseDatabase = "txharbor"
	dbBaseUser     = "txharbor"
	dbBasePassword = "txharbor"

	// dbPGMaxConnections raises the image default (100): the concurrency lanes
	// run real migrate CLI processes on top of the test's own pools. This is a
	// test-harness knob only; db.poolMaxConns stays untouched.
	dbPGMaxConnections = 200

	// dbAdminPoolMaxConns bounds the maintenance pool connected to the base
	// database (CREATE/DROP DATABASE only).
	dbAdminPoolMaxConns = 4

	// PostgreSQL identifiers are limited to 63 bytes; keep a strict upper
	// bound of 62 so every derived name is unambiguously < 63 bytes.
	dbTestDBPrefix   = "db_t_"
	dbTestDBMaxBytes = 62

	dbKeepDBEnv = "TXHARBOR_KEEP_DB"

	dbAdminPingTimeout = 10 * time.Second
	dbDropTimeout      = 30 * time.Second
)

var (
	// dbSharedBaseDSN points at the base database inside the shared container.
	// It is package-internal only: startPostgres returns the derived per-test
	// DSN, never this one.
	dbSharedBaseDSN string
	dbSharedCtr     *postgres.PostgresContainer
	dbAdminPool     *pgxpool.Pool

	// dbDBMu serializes CREATE DATABASE: PostgreSQL serializes on the template
	// database, so concurrent creation is best kept ordered here.
	dbDBMu sync.Mutex
	// dbDBSeq keeps derived database names unique across the run.
	dbDBSeq atomic.Uint64
)

// dbDedicatedContainerTests are the audited down/destructive lanes that keep a
// dedicated container (startPostgresDedicated) instead of a database inside
// the package-wide one. Every entry runs goose Down/DownTo and drops schema on
// purpose; the whitelist is a blast-radius policy, not a correctness
// requirement (per-test databases already isolate them). The keys are exact
// top-level t.Name() values; test bodies call startPostgres unchanged.
var dbDedicatedContainerTests = map[string]bool{
	"TestDepositMigrationDowngradeFrom004RemovesOnly004":    true, // DownTo(3)
	"TestAuthzScopeMigrationDownRemovesOnlyItself":          true, // DownTo(9)
	"TestWithdrawalExecutionMigrationDownRemovesOnlyItself": true, // DownTo(11)
	"TestConfirmationMigrationDowngradeTo4RemovesAbove4":    true, // DownTo(4)
	"TestEventInfrastructureMigrationUpDownUp":              true, // Down 18,17,16,15
	"TestIntentFKRepairDownAndReUp":                         true, // Down of 000014
	"TestT036SequenceCDownThenReUp":                         true, // Down of 000010
	"TestT042RollbackRevertsTenBeforeNine":                  true, // Down 18..11,10,9
	"TestT042DownOfAppliedThenRenumberedNumberForbidden":    true, // refused Down
}

// TestMain is the only TestMain in package db. It owns the shared container
// lifecycle:
//   - Docker provider missing/unhealthy: exit 1 under CI (CI=true) or when
//     TXHARBOR_REQUIRE_DOCKER=1 — a silently skipped integration suite must
//     fail the pipeline — and exit 0 locally (package skipped, no test runs)
//   - setup failure: terminate whatever started, exit 1
//   - otherwise: run the tests, then close the admin pool, terminate the
//     container, and exit with the test result code (pass or fail).
func TestMain(m *testing.M) {
	os.Exit(runDBIntegrationTests(m))
}

func runDBIntegrationTests(m *testing.M) int {
	ctx := context.Background()
	if !dbDockerProviderHealthy(ctx) {
		if os.Getenv("CI") == "true" || os.Getenv("TXHARBOR_REQUIRE_DOCKER") == "1" {
			fmt.Fprintln(os.Stderr, "db integration: docker provider unavailable; failing package (CI=true or TXHARBOR_REQUIRE_DOCKER=1): exit 1")
			return 1
		}
		fmt.Fprintln(os.Stderr, "db integration: docker provider unavailable; skipping package (exit 0)")
		return 0
	}
	if err := startSharedDBPostgres(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "db integration: shared postgres setup failed: %v\n", err)
		teardownSharedDBPostgres()
		return 1
	}
	code := m.Run()
	teardownSharedDBPostgres()
	return code
}

// dbDockerProviderHealthy mirrors testcontainers.SkipIfProviderIsNotHealthy
// without a *testing.T (TestMain runs before m.Run): recover provider panics
// and report health.
func dbDockerProviderHealthy(ctx context.Context) (healthy bool) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "db integration: docker provider check panicked: %v\n", r)
			healthy = false
		}
	}()
	provider, err := testcontainers.ProviderDocker.GetProvider()
	if err != nil {
		fmt.Fprintf(os.Stderr, "db integration: docker provider: %v\n", err)
		return false
	}
	if err := provider.Health(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "db integration: docker health: %v\n", err)
		return false
	}
	return true
}

// startSharedDBPostgres boots the single postgres container for the package
// and opens the admin pool used to create/drop per-test databases.
func startSharedDBPostgres(ctx context.Context) error {
	ctr, err := postgres.Run(ctx, dbPGImage,
		postgres.WithDatabase(dbBaseDatabase),
		postgres.WithUsername(dbBaseUser),
		postgres.WithPassword(dbBasePassword),
		testcontainers.WithCmdArgs("-c", fmt.Sprintf("max_connections=%d", dbPGMaxConnections)),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2)),
	)
	if err != nil {
		return fmt.Errorf("start postgres container: %w", err)
	}
	dbSharedCtr = ctr

	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return fmt.Errorf("postgres connection string: %w", err)
	}
	dbSharedBaseDSN = dsn

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return fmt.Errorf("parse base dsn: %w", err)
	}
	cfg.MaxConns = dbAdminPoolMaxConns
	cfg.MinConns = 1
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return fmt.Errorf("create admin pool: %w", err)
	}
	dbAdminPool = pool

	pingCtx, cancel := context.WithTimeout(ctx, dbAdminPingTimeout)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		return fmt.Errorf("ping admin pool: %w", err)
	}
	return nil
}

// teardownSharedDBPostgres closes the admin pool and terminates the shared
// container. It runs on the normal and the setup-failure path.
func teardownSharedDBPostgres() {
	if dbAdminPool != nil {
		dbAdminPool.Close()
		dbAdminPool = nil
	}
	if dbSharedCtr != nil {
		if err := dbSharedCtr.Terminate(context.Background()); err != nil {
			fmt.Fprintf(os.Stderr, "db integration: terminate shared postgres container: %v\n", err)
		}
		dbSharedCtr = nil
	}
	dbSharedBaseDSN = ""
}

// startPostgresDerived creates one fresh, uniquely named, EMPTY database in
// the shared container and returns its DSN. The caller keeps its own
// MigrateUp calls, exactly as before the shared-container optimization. The
// database is dropped in t.Cleanup (WITH (FORCE)) unless TXHARBOR_KEEP_DB=1
// or the test failed, in which case its name and DSN are logged for triage.
func startPostgresDerived(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	if dbAdminPool == nil || dbSharedBaseDSN == "" {
		t.Fatal("shared postgres not initialized: TestMain must run this package with -tags integration")
	}

	dbName := uniqueDBTestDBName(t)
	// Serialize CREATE DATABASE: PostgreSQL serializes on the template
	// database, so ordered creation avoids template1 lock contention.
	dbDBMu.Lock()
	_, err := dbAdminPool.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{dbName}.Sanitize())
	dbDBMu.Unlock()
	if err != nil {
		t.Fatalf("create test database %s: %v", dbName, err)
	}

	dsn, err := deriveDBDSN(dbSharedBaseDSN, dbName)
	if err != nil {
		_ = dropDBTestDB(ctx, dbName) // never leak a database we cannot address
		t.Fatalf("derive dsn for %s: %v", dbName, err)
	}

	t.Cleanup(func() { cleanupDBTestDB(t, dbName, dsn) })
	return dsn
}

// startPostgresDedicated boots a dedicated PostgreSQL container for the
// whitelisted down/destructive lanes and returns the DSN of its EMPTY base
// database (the exact pre-optimization contract: the caller runs its own
// migrations). Skips (never passes) when no Docker provider is available.
func startPostgresDedicated(t *testing.T) string {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()
	ctr, err := postgres.Run(ctx, dbPGImage,
		postgres.WithDatabase(dbBaseDatabase),
		postgres.WithUsername(dbBaseUser),
		postgres.WithPassword(dbBasePassword),
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

// uniqueDBTestDBName returns a lowercase PostgreSQL identifier unique per
// call: db_t_<sanitized test name>_<monotonic sequence>, always < 63 bytes.
func uniqueDBTestDBName(t *testing.T) string {
	t.Helper()
	suffix := fmt.Sprintf("_%d", dbDBSeq.Add(1))
	limit := dbTestDBMaxBytes - len(dbTestDBPrefix) - len(suffix)
	if limit < 1 {
		limit = 1
	}
	return dbTestDBPrefix + sanitizeDBTestDBName(t.Name(), limit) + suffix
}

// sanitizeDBTestDBName lowercases raw, maps every byte outside [a-z0-9_] to
// '_', and truncates to limit bytes. Test names are ASCII, so byte slicing
// stays valid.
func sanitizeDBTestDBName(raw string, limit int) string {
	var b strings.Builder
	b.Grow(len(raw))
	for _, r := range strings.ToLower(raw) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	name := b.String()
	if len(name) > limit {
		name = name[:limit]
	}
	if name == "" {
		name = "test"
	}
	return name
}

// deriveDBDSN swaps only the database name of the shared base DSN, preserving
// user/password/host/port/query parameters. It works on the URL form produced
// by PostgresContainer.ConnectionString; note that pgxpool.Config.ConnString()
// would return the original string unchanged, so the URL is edited directly.
func deriveDBDSN(baseDSN, dbName string) (string, error) {
	u, err := url.Parse(baseDSN)
	if err != nil {
		return "", fmt.Errorf("parse base dsn: %w", err)
	}
	if u.Scheme != "postgres" && u.Scheme != "postgresql" {
		return "", fmt.Errorf("parse base dsn: unsupported scheme %q", u.Scheme)
	}
	u.Path = "/" + dbName
	return u.String(), nil
}

// dropDBTestDB drops a per-test database, terminating straggler connections.
// Used on failure paths and from t.Cleanup.
func dropDBTestDB(ctx context.Context, dbName string) error {
	dctx, cancel := context.WithTimeout(ctx, dbDropTimeout)
	defer cancel()
	_, err := dbAdminPool.Exec(dctx, "DROP DATABASE "+pgx.Identifier{dbName}.Sanitize()+" WITH (FORCE)")
	return err
}

// cleanupDBTestDB is the single t.Cleanup registered per derived database:
// keep it for triage when TXHARBOR_KEEP_DB=1 or the test failed, otherwise
// drop it with FORCE. A drop failure is logged, never fatal (the container is
// disposable).
//
// Invariant: callers must close their pools first (a defer or a
// later-registered t.Cleanup, which run before this one). DROP ... WITH
// (FORCE) is only a fallback that terminates straggler backends; it is not
// the connection lifecycle.
func cleanupDBTestDB(t *testing.T, dbName, dsn string) {
	t.Helper()
	if os.Getenv(dbKeepDBEnv) == "1" || t.Failed() {
		// Honest triage semantics: the kept database is not durable. It dies
		// with the test binary when the shared container is Terminated, so it
		// must be inspected while the process is still alive; the DSN is only
		// reachable during that window.
		t.Logf("kept test database %q for triage (%s=%q, failed=%v); DSN: %s; note: the database dies with this test binary (shared container Terminate), connect while the process is alive",
			dbName, dbKeepDBEnv, os.Getenv(dbKeepDBEnv), t.Failed(), dsn)
		return
	}
	if dbAdminPool == nil {
		return
	}
	if err := dropDBTestDB(context.Background(), dbName); err != nil {
		t.Logf("drop test database %q: %v", dbName, err)
	}
}
