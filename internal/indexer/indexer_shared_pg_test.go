//go:build integration

// indexer_shared_pg_test.go owns the package-wide PostgreSQL lifecycle for the
// indexer integration suite (Phase A container-lifecycle optimization, Phase B
// template-DB clone fast path).
//
// Before: every startIndexerPostgres call booted its own postgres:18.6-trixie
// container and migrated it, so a single `go test -tags integration` run
// created dozens of containers; the first optimization left one container but
// still re-ran the embedded migrations per test database.
//
// After (both phases): TestMain boots exactly one container for the whole
// package and builds one migrated template database (indexerTemplateDBName) by
// running the full db.MigrateUp exactly once, single-threaded. Every
// startIndexerPostgres call then clones that template with
// CREATE DATABASE ... WITH TEMPLATE and drops its clone in t.Cleanup
// (DROP DATABASE ... WITH (FORCE)). Isolation is preserved at the database
// level: every call gets a fresh database, so no test can observe another
// test's rows. Anvil containers, sleeps/TTLs/polls and every test entry
// name/assertion are untouched.
//
// Migration coverage: the template build is the package-wide canary — it runs
// the full migration path once and aborts the package loudly on any error
// before m.Run. In addition, exactly one test,
// TestLeaseExactlyOneHolderAndExpiryTakeover (lease_integration_test.go),
// calls startIndexerPostgresMigrated, which keeps the legacy empty-database +
// full db.MigrateUp path alive on every run, so a broken migration cannot hide
// behind a template that was only built earlier in the same binary (see the
// helper comment for why that test).
//
// Deliberate non-goals and follow-ups (documented, not implemented here):
//   - The pg_stat_activity barriers in deposit_ctxguard, confirmation_race and
//     deposit_auth read cluster-wide state. That is safe while the indexer
//     package runs serially against its own per-test databases, but it must be
//     revisited before another package shares this container or any test gains
//     t.Parallel (TODO markers at each site).
//   - TXHARBOR_KEEP_DB=1 (or a failed test) keeps that test's database for
//     triage; the template and the container still terminate with the test
//     binary.
package indexer

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

	"github.com/xtianxx/txharbor/internal/db"
)

const (
	indexerPGImage      = "postgres:18.6-trixie"
	indexerBaseDatabase = "txharbor"
	indexerBaseUser     = "txharbor"
	indexerBasePassword = "txharbor"

	// indexerPGMaxConnections raises the image default (100): per-test pools
	// open up to 8 connections each and several tests add independent pools.
	// This is a test-harness knob only; db.poolMaxConns stays untouched.
	indexerPGMaxConnections = 200

	// indexerAdminPoolMaxConns bounds the maintenance pool connected to the
	// base database (CREATE/DROP DATABASE only).
	indexerAdminPoolMaxConns = 4

	// PostgreSQL identifiers are limited to 63 bytes; keep a strict upper
	// bound of 62 so every derived name is unambiguously < 63 bytes.
	indexerTestDBPrefix   = "idx_t_"
	indexerTestDBMaxBytes = 62

	// indexerTemplateDBName is the one migrated template database built by
	// TestMain; every per-test database is cloned from it. It deliberately
	// does not use indexerTestDBPrefix so it can never collide with a derived
	// per-test name.
	indexerTemplateDBName = "idx_template_migrated"

	indexerKeepDBEnv = "TXHARBOR_KEEP_DB"

	indexerAdminPingTimeout = 10 * time.Second
	indexerDropTimeout      = 30 * time.Second

	// indexerTemplateConnDrainTimeout bounds the wait for the MigrateUp
	// session's backends to disappear from the template database before the
	// package starts cloning it.
	indexerTemplateConnDrainTimeout = 10 * time.Second
)

var (
	// sharedBaseDSN points at the base database inside the shared container.
	// It is package-internal only: startIndexerPostgres returns the derived
	// per-test DSN, never this one.
	sharedBaseDSN string
	sharedCtr     *postgres.PostgresContainer
	adminPool     *pgxpool.Pool

	// sharedTemplateDB is indexerTemplateDBName once the migrated template
	// database exists, and "" before that (and after teardown). It is written
	// once before m.Run and only read afterwards.
	sharedTemplateDB string

	// indexerDBMu serializes CREATE DATABASE: PostgreSQL serializes on the
	// template database, so concurrent creation is best kept ordered here.
	indexerDBMu sync.Mutex
	// indexerDBSeq keeps derived database names unique across the run.
	indexerDBSeq atomic.Uint64
)

// TestMain is the only TestMain in package indexer. It owns the shared
// container lifecycle:
//   - Docker provider missing/unhealthy: exit 1 under CI (CI=true) or when
//     TXHARBOR_REQUIRE_DOCKER=1 — a silently skipped integration suite must
//     fail the pipeline — and exit 0 locally (package skipped, no test runs)
//   - setup failure: terminate whatever started, exit 1
//   - otherwise: run the tests, then close the admin pool, terminate the
//     container, and exit with the test result code (pass or fail).
func TestMain(m *testing.M) {
	os.Exit(runIndexerIntegrationTests(m))
}

func runIndexerIntegrationTests(m *testing.M) int {
	ctx := context.Background()
	if !indexerDockerProviderHealthy(ctx) {
		if os.Getenv("CI") == "true" || os.Getenv("TXHARBOR_REQUIRE_DOCKER") == "1" {
			fmt.Fprintln(os.Stderr, "indexer integration: docker provider unavailable; failing package (CI=true or TXHARBOR_REQUIRE_DOCKER=1): exit 1")
			return 1
		}
		fmt.Fprintln(os.Stderr, "indexer integration: docker provider unavailable; skipping package (exit 0)")
		return 0
	}
	if err := startSharedIndexerPostgres(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "indexer integration: shared postgres setup failed: %v\n", err)
		teardownSharedIndexerPostgres()
		return 1
	}
	code := m.Run()
	teardownSharedIndexerPostgres()
	return code
}

// indexerDockerProviderHealthy mirrors testcontainers.SkipIfProviderIsNotHealthy
// without a *testing.T (TestMain runs before m.Run): recover provider panics
// and report health.
func indexerDockerProviderHealthy(ctx context.Context) (healthy bool) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "indexer integration: docker provider check panicked: %v\n", r)
			healthy = false
		}
	}()
	provider, err := testcontainers.ProviderDocker.GetProvider()
	if err != nil {
		fmt.Fprintf(os.Stderr, "indexer integration: docker provider: %v\n", err)
		return false
	}
	if err := provider.Health(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "indexer integration: docker health: %v\n", err)
		return false
	}
	return true
}

// startSharedIndexerPostgres boots the single postgres container for the
// package, opens the admin pool used to create/drop per-test databases, and
// builds the migrated template database every startIndexerPostgres call
// clones.
func startSharedIndexerPostgres(ctx context.Context) error {
	ctr, err := postgres.Run(ctx, indexerPGImage,
		postgres.WithDatabase(indexerBaseDatabase),
		postgres.WithUsername(indexerBaseUser),
		postgres.WithPassword(indexerBasePassword),
		testcontainers.WithCmdArgs("-c", fmt.Sprintf("max_connections=%d", indexerPGMaxConnections)),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2)),
	)
	if err != nil {
		return fmt.Errorf("start postgres container: %w", err)
	}
	sharedCtr = ctr

	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return fmt.Errorf("postgres connection string: %w", err)
	}
	sharedBaseDSN = dsn

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return fmt.Errorf("parse base dsn: %w", err)
	}
	cfg.MaxConns = indexerAdminPoolMaxConns
	cfg.MinConns = 1
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return fmt.Errorf("create admin pool: %w", err)
	}
	adminPool = pool

	pingCtx, cancel := context.WithTimeout(ctx, indexerAdminPingTimeout)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		return fmt.Errorf("ping admin pool: %w", err)
	}

	// Build the migrated template database once, here in TestMain, before any
	// test can run; every startIndexerPostgres call clones it afterwards.
	return buildSharedIndexerTemplate(ctx)
}

// buildSharedIndexerTemplate creates the migrated template database once,
// single-threaded, before m.Run: one full db.MigrateUp over an empty database.
//
// db.MigrateUp closes its own connections before returning, and
// waitForZeroIndexerTemplateConnections proves the server also sees zero open
// connections to the template. That is what CREATE DATABASE ... WITH TEMPLATE
// requires; without the drain wait the first clone could race the asynchronous
// server-side teardown of the migration session and fail with "source database
// is being accessed by other users". Nothing ever connects to the template
// after this point, so once the drain wait returns, zero stays zero for the
// rest of the run.
//
// This build is also the package-wide migration canary: any migration failure
// aborts the package from TestMain (exit 1) before a single test runs, which is
// loud and can never look like a skip.
func buildSharedIndexerTemplate(ctx context.Context) error {
	start := time.Now()
	if _, err := adminPool.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{indexerTemplateDBName}.Sanitize()); err != nil {
		return fmt.Errorf("create template database %s: %w", indexerTemplateDBName, err)
	}
	// From here on the template exists: teardownSharedIndexerPostgres drops it
	// on every path, including a partially migrated build.
	sharedTemplateDB = indexerTemplateDBName

	dsn, err := deriveIndexerDSN(sharedBaseDSN, indexerTemplateDBName)
	if err != nil {
		return fmt.Errorf("derive template database dsn: %w", err)
	}

	var summary strings.Builder
	if err := db.MigrateUp(ctx, db.MigrateOptions{
		DSN:            dsn,
		LockTimeout:    5 * time.Second,
		ConnectTimeout: 5 * time.Second,
	}, &summary); err != nil {
		return fmt.Errorf("migrate template database %s: %w", indexerTemplateDBName, err)
	}
	if err := waitForZeroIndexerTemplateConnections(ctx, indexerTemplateDBName); err != nil {
		return err
	}

	fmt.Fprintf(os.Stderr, "indexer integration: template database %s migrated in %s (%s)\n",
		indexerTemplateDBName, time.Since(start).Round(time.Millisecond), strings.TrimSpace(summary.String()))
	return nil
}

// waitForZeroIndexerTemplateConnections polls pg_stat_activity until no server
// backend is attached to dbName. The admin pool itself is connected to the base
// database, so it never counts against the template.
func waitForZeroIndexerTemplateConnections(ctx context.Context, dbName string) error {
	deadline := time.Now().Add(indexerTemplateConnDrainTimeout)
	for {
		var n int
		if err := adminPool.QueryRow(ctx,
			"SELECT count(*) FROM pg_stat_activity WHERE datname = $1", dbName).Scan(&n); err != nil {
			return fmt.Errorf("count connections to template database %s: %w", dbName, err)
		}
		if n == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("template database %s still has %d open connection(s) after %s; CREATE DATABASE ... WITH TEMPLATE would fail",
				dbName, n, indexerTemplateConnDrainTimeout)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// teardownSharedIndexerPostgres drops the migrated template database, closes
// the admin pool and terminates the shared container. It runs on the normal and
// the setup-failure path.
func teardownSharedIndexerPostgres() {
	if adminPool != nil && sharedTemplateDB != "" {
		if err := dropIndexerTestDB(context.Background(), sharedTemplateDB); err != nil {
			fmt.Fprintf(os.Stderr, "indexer integration: drop template database %s: %v\n", sharedTemplateDB, err)
		}
		sharedTemplateDB = ""
	}
	if adminPool != nil {
		adminPool.Close()
		adminPool = nil
	}
	if sharedCtr != nil {
		if err := sharedCtr.Terminate(context.Background()); err != nil {
			fmt.Fprintf(os.Stderr, "indexer integration: terminate shared postgres container: %v\n", err)
		}
		sharedCtr = nil
	}
	sharedBaseDSN = ""
}

// uniqueIndexerTestDBName returns a lowercase PostgreSQL identifier unique per
// call: idx_t_<sanitized test name>_<monotonic sequence>, always < 63 bytes.
func uniqueIndexerTestDBName(t *testing.T) string {
	t.Helper()
	suffix := fmt.Sprintf("_%d", indexerDBSeq.Add(1))
	limit := indexerTestDBMaxBytes - len(indexerTestDBPrefix) - len(suffix)
	if limit < 1 {
		limit = 1
	}
	return indexerTestDBPrefix + sanitizeIndexerDBName(t.Name(), limit) + suffix
}

// sanitizeIndexerDBName lowercases raw, maps every byte outside [a-z0-9_] to
// '_', and truncates to limit bytes. Test names are ASCII, so byte slicing
// stays valid.
func sanitizeIndexerDBName(raw string, limit int) string {
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

// deriveIndexerDSN swaps only the database name of the shared base DSN,
// preserving user/password/host/port/query parameters. It works on the URL
// form produced by PostgresContainer.ConnectionString; note that
// pgxpool.Config.ConnString() would return the original string unchanged, so
// the URL is edited directly.
func deriveIndexerDSN(baseDSN, dbName string) (string, error) {
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

// dropIndexerTestDB drops a per-test database, terminating straggler
// connections. Used on failure paths and from t.Cleanup.
func dropIndexerTestDB(ctx context.Context, dbName string) error {
	dctx, cancel := context.WithTimeout(ctx, indexerDropTimeout)
	defer cancel()
	_, err := adminPool.Exec(dctx, "DROP DATABASE "+pgx.Identifier{dbName}.Sanitize()+" WITH (FORCE)")
	return err
}

// cleanupIndexerTestDB is the single t.Cleanup registered per derived
// database: keep it for triage when TXHARBOR_KEEP_DB=1 or the test failed,
// otherwise drop it with FORCE. A drop failure is logged, never fatal (the
// container is disposable).
//
// Invariant: callers must close their pool first (a defer or a
// later-registered t.Cleanup, which run before this one). DROP ... WITH
// (FORCE) is only a fallback that terminates straggler backends; it is not
// the connection lifecycle.
func cleanupIndexerTestDB(t *testing.T, dbName, dsn string) {
	t.Helper()
	if os.Getenv(indexerKeepDBEnv) == "1" || t.Failed() {
		// Honest triage semantics: the kept database is not durable. It dies
		// with the test binary when the shared container is Terminated, so it
		// must be inspected while the process is still alive; the DSN is only
		// reachable during that window.
		t.Logf("kept test database %q for triage (%s=%q, failed=%v); DSN: %s; note: the database dies with this test binary (shared container Terminate), connect while the process is alive",
			dbName, indexerKeepDBEnv, os.Getenv(indexerKeepDBEnv), t.Failed(), dsn)
		return
	}
	if adminPool == nil {
		return
	}
	if err := dropIndexerTestDB(context.Background(), dbName); err != nil {
		t.Logf("drop test database %q: %v", dbName, err)
	}
}
