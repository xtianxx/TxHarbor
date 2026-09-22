//go:build integration

// txlifecycle_shared_pg_test.go owns the package-wide PostgreSQL lifecycle for
// the txlifecycle integration suite (the same container-lifecycle optimization
// as internal/indexer/indexer_shared_pg_test.go and
// internal/withdrawal/withdrawal_shared_pg_test.go).
//
// Before: every startPG call booted its own postgres:18.6-trixie container and
// migrated it, so a single `go test -tags integration` run created one
// container per ordinary test, per joint test and per crash-matrix subtest.
//
// After: TestMain boots exactly one container for the whole package and
// startPG derives a uniquely named, migrated database inside it. Isolation is
// preserved at the database level: every call gets a fresh database
// (CREATE DATABASE; DROP DATABASE ... WITH (FORCE) in t.Cleanup), so no test
// can observe another test's rows. Anvil containers, sleeps/TTLs/polls,
// t.Parallel (none exists here) and every test entry name/assertion are
// untouched.
//
// Whitelisted lanes that keep independent containers (audit classification):
//   - Crash matrix (TestV9bCrashMatrix): a dedicated container per subtest via
//     startPGDedicated. The lane re-execs a helper subprocess and hard-kills it
//     (os.Exit) at each T-boundary, so it never shares the pool's lifecycle.
//     The re-exec'd child (TestCrashHelper) carries TX_CRASH_DSN/TX_CRASH_POINT
//     and bypasses TestMain (txlifecycleCrashChildProcess), exactly like the
//     withdrawal kill/carrier children: a child that exited hard could not
//     terminate a container it started, and every child run would otherwise
//     leak one container until Ryuk reaped it.
//   - Schema-destructive tests (residual's region_aborted_no_dispatch and the
//     claim_failclosed matrix) use newDedicatedEnv -> startPGDedicated: they
//     DROP/ALTER tables on purpose, and the whitelist keeps their blast radius
//     out of the shared container even though per-test databases would already
//     isolate them.
//   - Joint tests keep their per-test Anvil container (chain state is never
//     shared); only their PostgreSQL database is derived from the shared
//     container, through the unchanged newJointEnv construction.
//
// Deliberate non-goals and follow-ups (documented, not implemented here):
//   - No TEMPLATE cloning: each database runs the embedded migrations, which
//     keeps migration coverage per test and avoids template1 lock contention.
//   - The revoke_gate_race pg_stat_activity probes read cluster-wide state
//     (pid <> pg_backend_pid(), no datname filter). That is safe while this
//     package runs serially against its own per-test databases, but it must be
//     revisited before another package shares this container or any test
//     gains t.Parallel (TODO marker at the probe).
//   - TXHARBOR_KEEP_DB=1 (or a failed test) keeps that test's database for
//     triage; the container still terminates with the test binary.
package txlifecycle

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
	txlifecyclePGImage      = "postgres:18.6-trixie"
	txlifecycleBaseDatabase = "txharbor"
	txlifecycleBaseUser     = "txharbor"
	txlifecycleBasePassword = "txharbor"

	// txlifecyclePGMaxConnections raises the image default (100): per-test
	// pools open several connections each and joint tests add worker-process
	// pools. This is a test-harness knob only; db.poolMaxConns stays untouched.
	txlifecyclePGMaxConnections = 200

	// txlifecycleAdminPoolMaxConns bounds the maintenance pool connected to
	// the base database (CREATE/DROP DATABASE only).
	txlifecycleAdminPoolMaxConns = 4

	// PostgreSQL identifiers are limited to 63 bytes; keep a strict upper
	// bound of 62 so every derived name is unambiguously < 63 bytes.
	txlifecycleTestDBPrefix   = "tx_t_"
	txlifecycleTestDBMaxBytes = 62

	txlifecycleKeepDBEnv = "TXHARBOR_KEEP_DB"

	// Crash-helper child flags owned by crash_integration_test.go. The child
	// is this test binary re-exec'd with both set; it connects to the parent's
	// dedicated container and must not boot the package-wide one.
	txCrashDSNEnv   = "TX_CRASH_DSN"
	txCrashPointEnv = "TX_CRASH_POINT"

	txlifecycleAdminPingTimeout = 10 * time.Second
	txlifecycleDropTimeout      = 30 * time.Second
)

var (
	// sharedBaseDSN points at the base database inside the shared container.
	// It is package-internal only: startPG returns the derived per-test DSN,
	// never this one.
	sharedBaseDSN string
	sharedCtr     *postgres.PostgresContainer
	adminPool     *pgxpool.Pool

	// txlifecycleDBMu serializes CREATE DATABASE: PostgreSQL serializes on the
	// template database, so concurrent creation is best kept ordered here.
	txlifecycleDBMu sync.Mutex
	// txlifecycleDBSeq keeps derived database names unique across the run.
	txlifecycleDBSeq atomic.Uint64
)

// TestMain is the only TestMain in package txlifecycle. It owns the shared
// container lifecycle:
//   - re-exec'd crash-matrix children: run the tests immediately, no container
//     (they use the parent's dedicated container, see txlifecycleCrashChildProcess)
//   - Docker provider missing/unhealthy: exit 1 under CI (CI=true) or when
//     TXHARBOR_REQUIRE_DOCKER=1 — a silently skipped integration suite must
//     fail the pipeline — and exit 0 locally (package skipped, no test runs)
//   - setup failure: terminate whatever started, exit 1
//   - otherwise: run the tests, then close the admin pool, terminate the
//     container, and exit with the test result code (pass or fail).
func TestMain(m *testing.M) {
	os.Exit(runTxLifecycleIntegrationTests(m))
}

// txlifecycleCrashChildProcess reports whether this binary was re-exec'd as a
// crash-matrix helper child (crash_integration_test.go). Those children
// connect to the parent's dedicated container and must not boot the
// package-wide container.
func txlifecycleCrashChildProcess() bool {
	return os.Getenv(txCrashDSNEnv) != "" && os.Getenv(txCrashPointEnv) != ""
}

func runTxLifecycleIntegrationTests(m *testing.M) int {
	ctx := context.Background()
	if txlifecycleCrashChildProcess() {
		return m.Run()
	}
	if !txlifecycleDockerProviderHealthy(ctx) {
		if os.Getenv("CI") == "true" || os.Getenv("TXHARBOR_REQUIRE_DOCKER") == "1" {
			fmt.Fprintln(os.Stderr, "txlifecycle integration: docker provider unavailable; failing package (CI=true or TXHARBOR_REQUIRE_DOCKER=1): exit 1")
			return 1
		}
		fmt.Fprintln(os.Stderr, "txlifecycle integration: docker provider unavailable; skipping package (exit 0)")
		return 0
	}
	if err := startSharedTxLifecyclePostgres(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "txlifecycle integration: shared postgres setup failed: %v\n", err)
		teardownSharedTxLifecyclePostgres()
		return 1
	}
	code := m.Run()
	teardownSharedTxLifecyclePostgres()
	return code
}

// txlifecycleDockerProviderHealthy mirrors testcontainers.SkipIfProviderIsNotHealthy
// without a *testing.T (TestMain runs before m.Run): recover provider panics
// and report health.
func txlifecycleDockerProviderHealthy(ctx context.Context) (healthy bool) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "txlifecycle integration: docker provider check panicked: %v\n", r)
			healthy = false
		}
	}()
	provider, err := testcontainers.ProviderDocker.GetProvider()
	if err != nil {
		fmt.Fprintf(os.Stderr, "txlifecycle integration: docker provider: %v\n", err)
		return false
	}
	if err := provider.Health(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "txlifecycle integration: docker health: %v\n", err)
		return false
	}
	return true
}

// startSharedTxLifecyclePostgres boots the single postgres container for the
// package and opens the admin pool used to create/drop per-test databases.
func startSharedTxLifecyclePostgres(ctx context.Context) error {
	ctr, err := postgres.Run(ctx, txlifecyclePGImage,
		postgres.WithDatabase(txlifecycleBaseDatabase),
		postgres.WithUsername(txlifecycleBaseUser),
		postgres.WithPassword(txlifecycleBasePassword),
		testcontainers.WithCmdArgs("-c", fmt.Sprintf("max_connections=%d", txlifecyclePGMaxConnections)),
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
	cfg.MaxConns = txlifecycleAdminPoolMaxConns
	cfg.MinConns = 1
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return fmt.Errorf("create admin pool: %w", err)
	}
	adminPool = pool

	pingCtx, cancel := context.WithTimeout(ctx, txlifecycleAdminPingTimeout)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		return fmt.Errorf("ping admin pool: %w", err)
	}
	return nil
}

// teardownSharedTxLifecyclePostgres closes the admin pool and terminates the
// shared container. It runs on the normal and the setup-failure path.
func teardownSharedTxLifecyclePostgres() {
	if adminPool != nil {
		adminPool.Close()
		adminPool = nil
	}
	if sharedCtr != nil {
		if err := sharedCtr.Terminate(context.Background()); err != nil {
			fmt.Fprintf(os.Stderr, "txlifecycle integration: terminate shared postgres container: %v\n", err)
		}
		sharedCtr = nil
	}
	sharedBaseDSN = ""
}

// uniqueTxLifecycleTestDBName returns a lowercase PostgreSQL identifier unique
// per call: tx_t_<sanitized test name>_<monotonic sequence>, always < 63 bytes.
func uniqueTxLifecycleTestDBName(t *testing.T) string {
	t.Helper()
	suffix := fmt.Sprintf("_%d", txlifecycleDBSeq.Add(1))
	limit := txlifecycleTestDBMaxBytes - len(txlifecycleTestDBPrefix) - len(suffix)
	if limit < 1 {
		limit = 1
	}
	return txlifecycleTestDBPrefix + sanitizeTxLifecycleDBName(t.Name(), limit) + suffix
}

// sanitizeTxLifecycleDBName lowercases raw, maps every byte outside [a-z0-9_]
// to '_', and truncates to limit bytes. Test names are ASCII, so byte slicing
// stays valid.
func sanitizeTxLifecycleDBName(raw string, limit int) string {
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

// deriveTxLifecycleDSN swaps only the database name of the shared base DSN,
// preserving user/password/host/port/query parameters. It works on the URL
// form produced by PostgresContainer.ConnectionString; note that
// pgxpool.Config.ConnString() would return the original string unchanged, so
// the URL is edited directly.
func deriveTxLifecycleDSN(baseDSN, dbName string) (string, error) {
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

// dropTxLifecycleTestDB drops a per-test database, terminating straggler
// connections. Used on failure paths and from t.Cleanup.
func dropTxLifecycleTestDB(ctx context.Context, dbName string) error {
	dctx, cancel := context.WithTimeout(ctx, txlifecycleDropTimeout)
	defer cancel()
	_, err := adminPool.Exec(dctx, "DROP DATABASE "+pgx.Identifier{dbName}.Sanitize()+" WITH (FORCE)")
	return err
}

// cleanupTxLifecycleTestDB is the single t.Cleanup registered per derived
// database: keep it for triage when TXHARBOR_KEEP_DB=1 or the test failed,
// otherwise drop it with FORCE. A drop failure is logged, never fatal (the
// container is disposable).
//
// Invariant: callers must close their pool first (a defer or a
// later-registered t.Cleanup, which run before this one). DROP ... WITH
// (FORCE) is only a fallback that terminates straggler backends; it is not
// the connection lifecycle.
func cleanupTxLifecycleTestDB(t *testing.T, dbName, dsn string) {
	t.Helper()
	if os.Getenv(txlifecycleKeepDBEnv) == "1" || t.Failed() {
		// Honest triage semantics: the kept database is not durable. It dies
		// with the test binary when the shared container is Terminated, so it
		// must be inspected while the process is still alive; the DSN is only
		// reachable during that window.
		t.Logf("kept test database %q for triage (%s=%q, failed=%v); DSN: %s; note: the database dies with this test binary (shared container Terminate), connect while the process is alive",
			dbName, txlifecycleKeepDBEnv, os.Getenv(txlifecycleKeepDBEnv), t.Failed(), dsn)
		return
	}
	if adminPool == nil {
		return
	}
	if err := dropTxLifecycleTestDB(context.Background(), dbName); err != nil {
		t.Logf("drop test database %q: %v", dbName, err)
	}
}
