//go:build integration

// app_shared_pg_test.go owns the package-wide PostgreSQL lifecycle for the
// integration fixtures in package app that need a migrated scratch database
// (Phase A container-lifecycle + Phase B template-DB clone, adapted from
// internal/indexer/indexer_shared_pg_test.go).
//
// Scope: the shared container exists ONLY for withdrawalHTTPSetup
// (withdrawalhttp_integration_test.go), whose 24 uniform callers
// (withdrawalhttp, execution_http, execution_view, privacy_http,
// withdrawalmetrics) all need a migrated scratch database. Every other app
// integration test keeps booting its own container via startPostgresContainer:
// serve_pause's pinned ports, the serve:107 pending-migration test, the
// Anvil-backed serve/e2e tests and every other MigrateUp call site are
// deliberately untouched.
//
// Before: every withdrawalHTTPSetup call booted its own postgres:18.6-trixie
// container and ran the full embedded migration set on it, so one package run
// created two dozen containers and re-ran the migrations two dozen times.
//
// After: TestMain boots exactly one container for the whole package and builds
// one migrated template database (appTemplateDBName) by running the full
// db.MigrateUp exactly once, single-threaded. Every newTestDB call then clones
// that template with CREATE DATABASE ... WITH TEMPLATE and drops its clone in
// t.Cleanup (DROP DATABASE ... WITH (FORCE)). Isolation is preserved at the
// database level: every call gets a fresh database, so no test can observe
// another test's rows. Test bodies, assertions and timeouts are untouched.
//
// Migration coverage: the template build is a package-wide canary — it runs
// the full migration path once and aborts the package loudly on any error
// before m.Run. It is not the only migration coverage: the untouched app tests
// that boot their own container and run db.MigrateUp (serve_integration_test.go,
// serve_pause_integration_test.go, capacity_wiring_integration_test.go,
// eventpublisher_integration_test.go, serve_sc09_integration_test.go, ...)
// keep the legacy empty-database + MigrateUp path alive on every run.
//
// Deliberate non-goals and follow-ups (documented, not implemented here):
//   - The pg_stat_activity barriers in allowlist_switchover_integration_test.go
//     filter `datname = current_database()`, so they stay scoped to that test's
//     own database. This package has no t.Parallel test; that must stay true
//     for the shared container to remain safe.
//   - TXHARBOR_KEEP_DB=1 (or a failed test) keeps that test's clone database
//     for triage; the template and the container still terminate with the test
//     binary.
package app

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
	appPGImage      = "postgres:18.6-trixie"
	appBaseDatabase = "txharbor"
	appBaseUser     = "txharbor"
	appBasePassword = "txharbor"

	// appPGMaxConnections raises the image default (100): per-test pools open
	// up to 8 connections each and several tests add independent pools. This
	// is a test-harness knob only; db.poolMaxConns stays untouched.
	appPGMaxConnections = 200

	// appAdminPoolMaxConns bounds the maintenance pool connected to the base
	// database (CREATE/DROP DATABASE only).
	appAdminPoolMaxConns = 4

	// PostgreSQL identifiers are limited to 63 bytes; keep a strict upper
	// bound of 62 so every derived name is unambiguously < 63 bytes.
	appTestDBPrefix   = "app_t_"
	appTestDBMaxBytes = 62

	// appTemplateDBName is the one migrated template database built by
	// TestMain; every per-test database is cloned from it. It deliberately
	// does not use appTestDBPrefix so it can never collide with a derived
	// per-test name.
	appTemplateDBName = "app_template_migrated"

	appKeepDBEnv = "TXHARBOR_KEEP_DB"

	appAdminPingTimeout = 10 * time.Second
	appDropTimeout      = 30 * time.Second

	// appTemplateConnDrainTimeout bounds the wait for the MigrateUp session's
	// backends to disappear from the template database before the package
	// starts cloning it.
	appTemplateConnDrainTimeout = 10 * time.Second
)

var (
	// sharedBaseDSN points at the base database inside the shared container.
	// It is package-internal only: newTestDB returns the derived per-test DSN,
	// never this one. None of these names is used by the per-test containers
	// started via startPostgresContainer.
	sharedBaseDSN string
	sharedCtr     *postgres.PostgresContainer
	adminPool     *pgxpool.Pool

	// sharedTemplateDB is appTemplateDBName once the migrated template
	// database exists, and "" before that (and after teardown). It is written
	// once before m.Run and only read afterwards.
	sharedTemplateDB string

	// appDBMu serializes CREATE DATABASE: PostgreSQL serializes on the
	// template database, so concurrent creation is best kept ordered here.
	appDBMu sync.Mutex
	// appDBSeq keeps derived database names unique across the run.
	appDBSeq atomic.Uint64
)

// TestMain is the only TestMain in package app. It owns the shared container
// lifecycle:
//   - Docker provider missing/unhealthy: exit 1 under CI (CI=true) or when
//     TXHARBOR_REQUIRE_DOCKER=1 — a silently skipped integration suite must
//     fail the pipeline — and exit 0 locally (package skipped, no test runs)
//   - setup failure: terminate whatever started, exit 1
//   - otherwise: run the tests, then close the admin pool, terminate the
//     container, and exit with the test result code (pass or fail).
func TestMain(m *testing.M) {
	os.Exit(runAppIntegrationTests(m))
}

func runAppIntegrationTests(m *testing.M) int {
	ctx := context.Background()
	if !appDockerProviderHealthy(ctx) {
		if os.Getenv("CI") == "true" || os.Getenv("TXHARBOR_REQUIRE_DOCKER") == "1" {
			fmt.Fprintln(os.Stderr, "app integration: docker provider unavailable; failing package (CI=true or TXHARBOR_REQUIRE_DOCKER=1): exit 1")
			return 1
		}
		fmt.Fprintln(os.Stderr, "app integration: docker provider unavailable; skipping package (exit 0)")
		return 0
	}
	if err := startSharedAppPostgres(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "app integration: shared postgres setup failed: %v\n", err)
		teardownSharedAppPostgres()
		return 1
	}
	code := m.Run()
	teardownSharedAppPostgres()
	return code
}

// appDockerProviderHealthy mirrors testcontainers.SkipIfProviderIsNotHealthy
// without a *testing.T (TestMain runs before m.Run): recover provider panics
// and report health.
func appDockerProviderHealthy(ctx context.Context) (healthy bool) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "app integration: docker provider check panicked: %v\n", r)
			healthy = false
		}
	}()
	provider, err := testcontainers.ProviderDocker.GetProvider()
	if err != nil {
		fmt.Fprintf(os.Stderr, "app integration: docker provider: %v\n", err)
		return false
	}
	if err := provider.Health(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "app integration: docker health: %v\n", err)
		return false
	}
	return true
}

// startSharedAppPostgres boots the single postgres container for the package,
// opens the admin pool used to create/drop per-test databases, and builds the
// migrated template database every newTestDB call clones.
func startSharedAppPostgres(ctx context.Context) error {
	ctr, err := postgres.Run(ctx, appPGImage,
		postgres.WithDatabase(appBaseDatabase),
		postgres.WithUsername(appBaseUser),
		postgres.WithPassword(appBasePassword),
		testcontainers.WithCmdArgs("-c", fmt.Sprintf("max_connections=%d", appPGMaxConnections)),
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
	cfg.MaxConns = appAdminPoolMaxConns
	cfg.MinConns = 1
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return fmt.Errorf("create admin pool: %w", err)
	}
	adminPool = pool

	pingCtx, cancel := context.WithTimeout(ctx, appAdminPingTimeout)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		return fmt.Errorf("ping admin pool: %w", err)
	}

	// Build the migrated template database once, here in TestMain, before any
	// test can run; every newTestDB call clones it afterwards.
	return buildSharedAppTemplate(ctx)
}

// buildSharedAppTemplate creates the migrated template database once,
// single-threaded, before m.Run: one full db.MigrateUp over an empty database.
//
// db.MigrateUp closes its own connections before returning, and
// waitForZeroAppTemplateConnections proves the server also sees zero open
// connections to the template. That is what CREATE DATABASE ... WITH TEMPLATE
// requires; without the drain wait the first clone could race the asynchronous
// server-side teardown of the migration session and fail with "source database
// is being accessed by other users". Nothing ever connects to the template
// after this point, so once the drain wait returns, zero stays zero for the
// rest of the run.
//
// This build is also a package-wide migration canary: any migration failure
// aborts the package from TestMain (exit 1) before a single test runs, which is
// loud and can never look like a skip.
func buildSharedAppTemplate(ctx context.Context) error {
	start := time.Now()
	if _, err := adminPool.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{appTemplateDBName}.Sanitize()); err != nil {
		return fmt.Errorf("create template database %s: %w", appTemplateDBName, err)
	}
	// From here on the template exists: teardownSharedAppPostgres drops it on
	// every path, including a partially migrated build.
	sharedTemplateDB = appTemplateDBName

	dsn, err := deriveAppDSN(sharedBaseDSN, appTemplateDBName)
	if err != nil {
		return fmt.Errorf("derive template database dsn: %w", err)
	}

	var summary strings.Builder
	if err := db.MigrateUp(ctx, db.MigrateOptions{
		DSN:            dsn,
		LockTimeout:    5 * time.Second,
		ConnectTimeout: 5 * time.Second,
	}, &summary); err != nil {
		return fmt.Errorf("migrate template database %s: %w", appTemplateDBName, err)
	}
	if err := waitForZeroAppTemplateConnections(ctx, appTemplateDBName); err != nil {
		return err
	}

	fmt.Fprintf(os.Stderr, "app integration: template database %s migrated in %s (%s)\n",
		appTemplateDBName, time.Since(start).Round(time.Millisecond), strings.TrimSpace(summary.String()))
	return nil
}

// waitForZeroAppTemplateConnections polls pg_stat_activity until no server
// backend is attached to dbName. The admin pool itself is connected to the base
// database, so it never counts against the template.
func waitForZeroAppTemplateConnections(ctx context.Context, dbName string) error {
	deadline := time.Now().Add(appTemplateConnDrainTimeout)
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
				dbName, n, appTemplateConnDrainTimeout)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// teardownSharedAppPostgres drops the migrated template database, closes the
// admin pool and terminates the shared container. It runs on the normal and the
// setup-failure path.
func teardownSharedAppPostgres() {
	if adminPool != nil && sharedTemplateDB != "" {
		if err := dropAppTestDB(context.Background(), sharedTemplateDB); err != nil {
			fmt.Fprintf(os.Stderr, "app integration: drop template database %s: %v\n", sharedTemplateDB, err)
		}
		sharedTemplateDB = ""
	}
	if adminPool != nil {
		adminPool.Close()
		adminPool = nil
	}
	if sharedCtr != nil {
		if err := sharedCtr.Terminate(context.Background()); err != nil {
			fmt.Fprintf(os.Stderr, "app integration: terminate shared postgres container: %v\n", err)
		}
		sharedCtr = nil
	}
	sharedBaseDSN = ""
}

// newTestDB clones the migrated template database into a fresh, uniquely named
// database inside the shared container (CREATE DATABASE ... WITH TEMPLATE) and
// returns the derived DSN; it never returns sharedBaseDSN. The clone is a full
// copy of the template, so it starts at the template's schema version without
// re-running the embedded migrations.
//
// Docker gating moved to TestMain: when no provider is healthy TestMain exits
// 0 before m.Run, so the package is skipped (never silently passed) without a
// per-test container. The returned DSN is private to this call: the database is
// dropped in t.Cleanup (WITH (FORCE)) unless TXHARBOR_KEEP_DB=1 or the test
// failed, in which case its name and DSN are logged for triage.
func newTestDB(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	if adminPool == nil || sharedBaseDSN == "" || sharedTemplateDB == "" {
		t.Fatal("shared postgres not initialized: TestMain must run this package with -tags integration")
	}

	dbName := uniqueAppTestDBName(t)
	// Serialize CREATE DATABASE ... WITH TEMPLATE: PostgreSQL serializes on
	// the template database, so ordered creation avoids lock contention.
	appDBMu.Lock()
	_, err := adminPool.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{dbName}.Sanitize()+
		" WITH TEMPLATE "+pgx.Identifier{sharedTemplateDB}.Sanitize())
	appDBMu.Unlock()
	if err != nil {
		t.Fatalf("create test database %s from template %s: %v", dbName, sharedTemplateDB, err)
	}

	dsn, err := deriveAppDSN(sharedBaseDSN, dbName)
	if err != nil {
		_ = dropAppTestDB(ctx, dbName)
		t.Fatalf("derive dsn for %s: %v", dbName, err)
	}

	t.Cleanup(func() { cleanupAppTestDB(t, dbName, dsn) })
	return dsn
}

// uniqueAppTestDBName returns a lowercase PostgreSQL identifier unique per
// call: app_t_<sanitized test name>_<monotonic sequence>, always < 63 bytes.
func uniqueAppTestDBName(t *testing.T) string {
	t.Helper()
	suffix := fmt.Sprintf("_%d", appDBSeq.Add(1))
	limit := appTestDBMaxBytes - len(appTestDBPrefix) - len(suffix)
	if limit < 1 {
		limit = 1
	}
	return appTestDBPrefix + sanitizeAppDBName(t.Name(), limit) + suffix
}

// sanitizeAppDBName lowercases raw, maps every byte outside [a-z0-9_] to '_',
// and truncates to limit bytes. Test names are ASCII, so byte slicing stays
// valid.
func sanitizeAppDBName(raw string, limit int) string {
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

// deriveAppDSN swaps only the database name of the shared base DSN, preserving
// user/password/host/port/query parameters. It works on the URL form produced
// by PostgresContainer.ConnectionString; note that pgxpool.Config.ConnString()
// would return the original string unchanged, so the URL is edited directly.
func deriveAppDSN(baseDSN, dbName string) (string, error) {
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

// dropAppTestDB drops a per-test database, terminating straggler connections.
// Used on failure paths and from t.Cleanup.
func dropAppTestDB(ctx context.Context, dbName string) error {
	dctx, cancel := context.WithTimeout(ctx, appDropTimeout)
	defer cancel()
	_, err := adminPool.Exec(dctx, "DROP DATABASE "+pgx.Identifier{dbName}.Sanitize()+" WITH (FORCE)")
	return err
}

// cleanupAppTestDB is the single t.Cleanup registered per derived database:
// keep it for triage when TXHARBOR_KEEP_DB=1 or the test failed, otherwise drop
// it with FORCE. A drop failure is logged, never fatal (the container is
// disposable).
//
// Invariant: callers must close their pool first (a defer or a later-registered
// t.Cleanup, which run before this one). DROP ... WITH (FORCE) is only a
// fallback that terminates straggler backends; it is not the connection
// lifecycle.
func cleanupAppTestDB(t *testing.T, dbName, dsn string) {
	t.Helper()
	if os.Getenv(appKeepDBEnv) == "1" || t.Failed() {
		// Honest triage semantics: the kept database is not durable. It dies
		// with the test binary when the shared container is Terminated, so it
		// must be inspected while the process is still alive; the DSN is only
		// reachable during that window.
		t.Logf("kept test database %q for triage (%s=%q, failed=%v); DSN: %s; note: the database dies with this test binary (shared container Terminate), connect while the process is alive",
			dbName, appKeepDBEnv, os.Getenv(appKeepDBEnv), t.Failed(), dsn)
		return
	}
	if adminPool == nil {
		return
	}
	if err := dropAppTestDB(context.Background(), dbName); err != nil {
		t.Logf("drop test database %q: %v", dbName, err)
	}
}
