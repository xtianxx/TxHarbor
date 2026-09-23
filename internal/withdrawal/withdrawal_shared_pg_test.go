//go:build integration

// withdrawal_shared_pg_test.go owns the package-wide PostgreSQL lifecycle for
// the withdrawal integration suite (same Phase A container-lifecycle
// optimization as internal/indexer/indexer_shared_pg_test.go, applied to this
// package).
//
// Before: every withdrawalStartPostgres call booted its own
// postgres:18.6-trixie container, so a single `go test -tags integration` run
// created one container per test.
//
// After: TestMain boots exactly one container for the whole package and
// withdrawalStartPostgres derives a uniquely named database inside it.
// Isolation is preserved at the database level: every call gets a fresh
// database (CREATE DATABASE; DROP DATABASE ... WITH (FORCE) in t.Cleanup), so
// no test can observe another test's rows. Sleeps/TTLs/polls, every test entry
// name and every assertion are untouched.
//
// Deliberate non-goals and follow-ups (documented, not implemented here):
//   - No TEMPLATE cloning: each database runs the embedded migrations, which
//     keeps migration coverage per test and avoids template1 lock contention.
//   - The recovery_kill / carrier_kill harness (package withdrawal_test) keeps
//     its own dedicated kill container (killSetup) and a real child re-exec of
//     this test binary. Those children run with TXHARBOR_RECOVERY_KILL_CHILD=1
//     or TXHARBOR_CARRIER_KILL_CHILD=1 and never touch the shared pool, so
//     withdrawalChildProcess makes their TestMain a no-op: a SIGKILLed child
//     could not terminate a container it started, and every child run would
//     otherwise leak one container until Ryuk reaped it.
//   - The pg_stat_activity barrier in revocation_integration_test.go filters
//     `datname = current_database()`, so it stays scoped to its own derived
//     database. This package has no t.Parallel test; that must stay true for
//     the cluster-wide probes to remain safe.
//   - TXHARBOR_KEEP_DB=1 (or a failed test) keeps that test's database for
//     triage; the container still terminates with the test binary.
package withdrawal

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
	withdrawalPGImage      = "postgres:18.6-trixie"
	withdrawalBaseDatabase = "txharbor"
	withdrawalBaseUser     = "txharbor"
	withdrawalBasePassword = "txharbor"

	// withdrawalPGMaxConnections raises the image default (100): per-test pools
	// open up to 8 connections each and several tests add independent pools.
	// This is a test-harness knob only; db.poolMaxConns stays untouched.
	withdrawalPGMaxConnections = 200

	// withdrawalAdminPoolMaxConns bounds the maintenance pool connected to the
	// base database (CREATE/DROP DATABASE only).
	withdrawalAdminPoolMaxConns = 4

	// PostgreSQL identifiers are limited to 63 bytes; keep a strict upper
	// bound of 62 so every derived name is unambiguously < 63 bytes.
	withdrawalTestDBPrefix   = "wd_t_"
	withdrawalTestDBMaxBytes = 62

	withdrawalKeepDBEnv = "TXHARBOR_KEEP_DB"

	// Re-exec child flags owned by recovery_kill_integration_test.go and
	// carrier_kill_integration_test.go (package withdrawal_test). They are
	// duplicated as literals because those constants live in the external test
	// package and cannot be referenced from here; withdrawalChildProcess is
	// the only consumer.
	withdrawalKillChildEnv        = "TXHARBOR_RECOVERY_KILL_CHILD"
	withdrawalCarrierKillChildEnv = "TXHARBOR_CARRIER_KILL_CHILD"

	withdrawalAdminPingTimeout = 10 * time.Second
	withdrawalDropTimeout      = 30 * time.Second
)

var (
	// sharedBaseDSN points at the base database inside the shared container.
	// It is package-internal only: withdrawalStartPostgres returns the derived
	// per-test DSN, never this one.
	sharedBaseDSN string
	sharedCtr     *postgres.PostgresContainer
	adminPool     *pgxpool.Pool

	// withdrawalDBMu serializes CREATE DATABASE: PostgreSQL serializes on the
	// template database, so concurrent creation is best kept ordered here.
	withdrawalDBMu sync.Mutex
	// withdrawalDBSeq keeps derived database names unique across the run.
	withdrawalDBSeq atomic.Uint64
)

// TestMain is the only TestMain in package withdrawal. It owns the shared
// container lifecycle:
//   - re-exec'd kill/carrier children: run the tests immediately, no container
//     (they use the parent's dedicated kill container, see withdrawalChildProcess)
//   - Docker provider missing/unhealthy: exit 1 under CI (CI=true) or when
//     TXHARBOR_REQUIRE_DOCKER=1 — a silently skipped integration suite must
//     fail the pipeline — and exit 0 locally (package skipped, no test runs)
//   - setup failure: terminate whatever started, exit 1
//   - otherwise: run the tests, then close the admin pool, terminate the
//     container, and exit with the test result code (pass or fail).
func TestMain(m *testing.M) {
	os.Exit(runWithdrawalIntegrationTests(m))
}

// withdrawalChildProcess reports whether this binary was re-exec'd as a
// kill-harness child process (recovery_kill_integration_test.go /
// carrier_kill_integration_test.go, package withdrawal_test). Those children
// connect to the parent's independent kill container and must not boot the
// package-wide container.
func withdrawalChildProcess() bool {
	return os.Getenv(withdrawalKillChildEnv) == "1" || os.Getenv(withdrawalCarrierKillChildEnv) == "1"
}

func runWithdrawalIntegrationTests(m *testing.M) int {
	ctx := context.Background()
	if withdrawalChildProcess() {
		return m.Run()
	}
	if !withdrawalDockerProviderHealthy(ctx) {
		if os.Getenv("CI") == "true" || os.Getenv("TXHARBOR_REQUIRE_DOCKER") == "1" {
			fmt.Fprintln(os.Stderr, "withdrawal integration: docker provider unavailable; failing package (CI=true or TXHARBOR_REQUIRE_DOCKER=1): exit 1")
			return 1
		}
		fmt.Fprintln(os.Stderr, "withdrawal integration: docker provider unavailable; skipping package (exit 0)")
		return 0
	}
	if err := startSharedWithdrawalPostgres(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "withdrawal integration: shared postgres setup failed: %v\n", err)
		teardownSharedWithdrawalPostgres()
		return 1
	}
	code := m.Run()
	teardownSharedWithdrawalPostgres()
	return code
}

// withdrawalDockerProviderHealthy mirrors testcontainers.SkipIfProviderIsNotHealthy
// without a *testing.T (TestMain runs before m.Run): recover provider panics
// and report health.
func withdrawalDockerProviderHealthy(ctx context.Context) (healthy bool) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "withdrawal integration: docker provider check panicked: %v\n", r)
			healthy = false
		}
	}()
	provider, err := testcontainers.ProviderDocker.GetProvider()
	if err != nil {
		fmt.Fprintf(os.Stderr, "withdrawal integration: docker provider: %v\n", err)
		return false
	}
	if err := provider.Health(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "withdrawal integration: docker health: %v\n", err)
		return false
	}
	return true
}

// startSharedWithdrawalPostgres boots the single postgres container for the
// package and opens the admin pool used to create/drop per-test databases.
func startSharedWithdrawalPostgres(ctx context.Context) error {
	ctr, err := postgres.Run(ctx, withdrawalPGImage,
		postgres.WithDatabase(withdrawalBaseDatabase),
		postgres.WithUsername(withdrawalBaseUser),
		postgres.WithPassword(withdrawalBasePassword),
		testcontainers.WithCmdArgs("-c", fmt.Sprintf("max_connections=%d", withdrawalPGMaxConnections)),
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
	cfg.MaxConns = withdrawalAdminPoolMaxConns
	cfg.MinConns = 1
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return fmt.Errorf("create admin pool: %w", err)
	}
	adminPool = pool

	pingCtx, cancel := context.WithTimeout(ctx, withdrawalAdminPingTimeout)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		return fmt.Errorf("ping admin pool: %w", err)
	}
	return nil
}

// teardownSharedWithdrawalPostgres closes the admin pool and terminates the
// shared container. It runs on the normal and the setup-failure path.
func teardownSharedWithdrawalPostgres() {
	if adminPool != nil {
		adminPool.Close()
		adminPool = nil
	}
	if sharedCtr != nil {
		if err := sharedCtr.Terminate(context.Background()); err != nil {
			fmt.Fprintf(os.Stderr, "withdrawal integration: terminate shared postgres container: %v\n", err)
		}
		sharedCtr = nil
	}
	sharedBaseDSN = ""
}

// uniqueWithdrawalTestDBName returns a lowercase PostgreSQL identifier unique
// per call: wd_t_<sanitized test name>_<monotonic sequence>, always < 63 bytes.
func uniqueWithdrawalTestDBName(t *testing.T) string {
	t.Helper()
	suffix := fmt.Sprintf("_%d", withdrawalDBSeq.Add(1))
	limit := withdrawalTestDBMaxBytes - len(withdrawalTestDBPrefix) - len(suffix)
	if limit < 1 {
		limit = 1
	}
	return withdrawalTestDBPrefix + sanitizeWithdrawalDBName(t.Name(), limit) + suffix
}

// sanitizeWithdrawalDBName lowercases raw, maps every byte outside [a-z0-9_]
// to '_', and truncates to limit bytes. Test names are ASCII, so byte slicing
// stays valid.
func sanitizeWithdrawalDBName(raw string, limit int) string {
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

// deriveWithdrawalDSN swaps only the database name of the shared base DSN,
// preserving user/password/host/port/query parameters. It works on the URL
// form produced by PostgresContainer.ConnectionString; note that
// pgxpool.Config.ConnString() would return the original string unchanged, so
// the URL is edited directly.
func deriveWithdrawalDSN(baseDSN, dbName string) (string, error) {
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

// dropWithdrawalTestDB drops a per-test database, terminating straggler
// connections. Used on failure paths and from t.Cleanup.
func dropWithdrawalTestDB(ctx context.Context, dbName string) error {
	dctx, cancel := context.WithTimeout(ctx, withdrawalDropTimeout)
	defer cancel()
	_, err := adminPool.Exec(dctx, "DROP DATABASE "+pgx.Identifier{dbName}.Sanitize()+" WITH (FORCE)")
	return err
}

// cleanupWithdrawalTestDB is the single t.Cleanup registered per derived
// database: keep it for triage when TXHARBOR_KEEP_DB=1 or the test failed,
// otherwise drop it with FORCE. A drop failure is logged, never fatal (the
// container is disposable).
//
// Invariant: callers must close their pool first (a defer or a
// later-registered t.Cleanup, which run before this one). DROP ... WITH
// (FORCE) is only a fallback that terminates straggler backends; it is not
// the connection lifecycle.
func cleanupWithdrawalTestDB(t *testing.T, dbName, dsn string) {
	t.Helper()
	if os.Getenv(withdrawalKeepDBEnv) == "1" || t.Failed() {
		// Honest triage semantics: the kept database is not durable. It dies
		// with the test binary when the shared container is Terminated, so it
		// must be inspected while the process is still alive; the DSN is only
		// reachable during that window.
		t.Logf("kept test database %q for triage (%s=%q, failed=%v); DSN: %s; note: the database dies with this test binary (shared container Terminate), connect while the process is alive",
			dbName, withdrawalKeepDBEnv, os.Getenv(withdrawalKeepDBEnv), t.Failed(), dsn)
		return
	}
	if adminPool == nil {
		return
	}
	if err := dropWithdrawalTestDB(context.Background(), dbName); err != nil {
		t.Logf("drop test database %q: %v", dbName, err)
	}
}
