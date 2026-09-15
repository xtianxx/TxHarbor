//go:build integration

// migration_integration_test.go owns spec task T006 for 007-withdrawal-creation:
// it proves migration 000007 applies and rolls back cleanly over a real 006
// database and that its named constraints carry the exact declared names.
//
// Every database assertion here is a raw-SQL probe; no withdrawal intake/grant
// classifier (ValidateAmount, CanonicalAddress, ...) is exercised. Each test
// boots its own isolated scratch PostgreSQL container via testcontainers and
// terminates it in t.Cleanup.
package withdrawal

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io/fs"
	"os/exec"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib" // database/sql driver for the raw probes
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/migrations"
)

const (
	// withdrawalMaxUint256 is the declared upper bound enforced by 000007's
	// amount CHECK constraints (2^256-1). withdrawalOverUint256 is 2^256.
	withdrawalMaxUint256  = "115792089237316195423570985008687907853269984665640564039457584007913129639935"
	withdrawalOverUint256 = "115792089237316195423570985008687907853269984665640564039457584007913129639936"
)

// withdrawalSevenTables are the six objects created by 000007, dropped by its
// Down, in the exact set the migration declares.
var withdrawalSevenTables = []string{
	"caller",
	"api_key",
	"withdrawal_requests",
	"withdrawal_authorizations",
	"withdrawal_request_audit",
	"withdrawal_grant_audit",
}

// withdrawalHistoricalMigrations are the frozen migrations T006 must never
// modify; the history-untouched probe diffs exactly this set.
var withdrawalHistoricalMigrations = []string{
	"migrations/000001_baseline.sql",
	"migrations/000002_chain_indexer.sql",
	"migrations/000003_event_indexing.sql",
	"migrations/000004_deposit_detection.sql",
	"migrations/000005_confirmation_tracking.sql",
	"migrations/000006_reorg_recovery.sql",
}

// withdrawalStartPostgres boots a real PostgreSQL container and returns its
// DSN. Skips (never passes) when no Docker provider is available.
func withdrawalStartPostgres(t *testing.T) string {
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
	return dsn
}

func withdrawalOpenSQL(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	sqlDB, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open sql db: %v", err)
	}
	t.Cleanup(func() { sqlDB.Close() })
	return sqlDB
}

func withdrawalMigrateOptions(dsn string) db.MigrateOptions {
	return db.MigrateOptions{DSN: dsn, LockTimeout: 5 * time.Second, ConnectTimeout: 5 * time.Second}
}

// withdrawalSubsetFS returns the embedded migrations up to maxVersion
// inclusive as a standalone FS, so a real database pinned at version N can be
// built before testing the N+1 upgrade.
func withdrawalSubsetFS(t *testing.T, maxVersion int64) fstest.MapFS {
	t.Helper()
	files, err := db.MigrationFiles(migrations.FS)
	if err != nil {
		t.Fatalf("list embedded migrations: %v", err)
	}
	fsys := fstest.MapFS{}
	for _, f := range files {
		if f.Version > maxVersion {
			continue
		}
		data, err := fs.ReadFile(migrations.FS, f.Name)
		if err != nil {
			t.Fatalf("read embedded %s: %v", f.Name, err)
		}
		fsys[f.Name] = &fstest.MapFile{Data: data}
	}
	return fsys
}

// withdrawalNewProvider replicates internal/db's unexported newProvider (same
// locker, same options) since that constructor is not importable.
func withdrawalNewProvider(t *testing.T, sqlDB *sql.DB, fsys fs.FS) *goose.Provider {
	t.Helper()
	locker, err := lock.NewPostgresSessionLocker(lock.WithLockTimeout(1, 5))
	if err != nil {
		t.Fatalf("create migration session locker: %v", err)
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, sqlDB, fsys,
		goose.WithSessionLocker(locker),
		goose.WithDisableGlobalRegistry(true),
	)
	if err != nil {
		t.Fatalf("create goose provider: %v", err)
	}
	return provider
}

func withdrawalMustExec(t *testing.T, sqlDB *sql.DB, query string, args ...any) {
	t.Helper()
	if _, err := sqlDB.ExecContext(context.Background(), query, args...); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

// withdrawalWantPgError asserts err is a *pgconn.PgError with the exact
// SQLSTATE code and the exact declared constraint name.
func withdrawalWantPgError(t *testing.T, err error, code, constraint string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected PostgreSQL error %s on constraint %s, got nil", code, constraint)
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		t.Fatalf("expected *pgconn.PgError %s/%s, got %T: %v", code, constraint, err, err)
	}
	if pgErr.Code != code {
		t.Fatalf("SQLSTATE = %s (%q), want %s", pgErr.Code, pgErr.Message, code)
	}
	if pgErr.ConstraintName != constraint {
		t.Fatalf("constraint = %q, want %q (message %q)", pgErr.ConstraintName, constraint, pgErr.Message)
	}
}

func withdrawalMatchesPgError(err error, code, constraint string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == code && pgErr.ConstraintName == constraint
}

func withdrawalRelationExists(t *testing.T, sqlDB *sql.DB, name string) bool {
	t.Helper()
	var exists bool
	if err := sqlDB.QueryRowContext(context.Background(),
		"SELECT to_regclass($1) IS NOT NULL", name).Scan(&exists); err != nil {
		t.Fatalf("to_regclass(%s): %v", name, err)
	}
	return exists
}

func withdrawalCount(t *testing.T, sqlDB *sql.DB, table string) int {
	t.Helper()
	var n int
	if err := sqlDB.QueryRowContext(context.Background(),
		"SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

func withdrawalHash(fill string) string { return "0x" + strings.Repeat(fill, 32) }
func withdrawalAddr(fill string) string { return "0x" + strings.Repeat(fill, 20) }

// withdrawalGit runs git in dir (or the process CWD when empty) and returns
// trimmed stdout, failing the test on any git error.
func withdrawalGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	if dir != "" {
		cmd.Dir = dir
	}
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("git %s: %v", strings.Join(args, " "), err)
	}
	return strings.TrimSpace(string(out))
}

// withdrawalSeedUpstreamRows inserts one representative minimal row per
// migration 002-006 object, valid at version 6 and at version 7 alike. Used
// both as the upgrade seed (rows must survive 007 Down) and as the post-upgrade
// smoke write. Fails the test if any write is rejected.
func withdrawalSeedUpstreamRows(t *testing.T, sqlDB *sql.DB) {
	t.Helper()
	blockHash := withdrawalHash("aa")
	txHash := withdrawalHash("cc")
	topic := withdrawalHash("dd")
	contract := withdrawalAddr("11")
	sender := withdrawalAddr("22")
	recipient := withdrawalAddr("33")

	withdrawalMustExec(t, sqlDB, `INSERT INTO chain_blocks
		(chain_id, number, hash, parent_hash, canonical)
		VALUES (1, 100, $1, $2, TRUE)`, blockHash, withdrawalHash("bb"))
	withdrawalMustExec(t, sqlDB, `INSERT INTO erc20_transfer_logs
		(chain_id, block_number, block_hash, tx_hash, log_index, contract, topic0, topic1, topic2, data)
		VALUES (1, 100, $1, $2, 0, $3, $4, $4, $4, $4)`, blockHash, txHash, contract, topic)
	withdrawalMustExec(t, sqlDB, `INSERT INTO deposit_config_history
		(chain_id, version_seq, config_hash, start_block, assets, watches, replay_from, operator)
		VALUES (1, 1, $1, 0, 'asset:x', 'watch:y', 0, 'bootstrap')`, strings.Repeat("a", 64))
	withdrawalMustExec(t, sqlDB, `INSERT INTO deposit_observations
		(chain_id, block_hash, tx_hash, log_index, block_number, contract, sender, recipient, amount, version_seq)
		VALUES (1, $1, $2, 0, 100, $3, $4, $5, 100, 1)`, blockHash, txHash, contract, sender, recipient)
	withdrawalMustExec(t, sqlDB, `INSERT INTO confirmation_policy_history
		(chain_id, policy_seq, threshold, operator) VALUES (1, 1, 12, 'bootstrap')`)
	withdrawalMustExec(t, sqlDB, `INSERT INTO reorg_policy_history
		(chain_id, policy_seq, max_depth, operator) VALUES (1, 1, 64, 'bootstrap')`)
	withdrawalMustExec(t, sqlDB, `INSERT INTO reorg_recovery
		(chain_id, recovery_id, phase, policy_seq, max_depth, bound_old_number, bound_old_hash, recovery_seq)
		VALUES (1, 'rec-1', 'detected', 1, 64, 100, $1, 1)`, blockHash)
}

// upstreamRowCounts is the expected surviving count per 002-006 table after a
// withdrawalSeedUpstreamRows call.
var upstreamRowCounts = map[string]int{
	"chain_blocks":                1,
	"erc20_transfer_logs":         1,
	"deposit_config_history":      1,
	"deposit_observations":        1,
	"confirmation_policy_history": 1,
	"reorg_policy_history":        1,
	"reorg_recovery":              1,
}

// TestWithdrawalMigrationHistoryUntouched proves T006 modified no historical
// migration: replaying 000001-000006 is allowed, editing them is not.
func TestWithdrawalMigrationHistoryUntouched(t *testing.T) {
	root := withdrawalGit(t, "", "rev-parse", "--show-toplevel")
	if root == "" {
		t.Fatal("git rev-parse --show-toplevel returned no repository root")
	}
	args := append([]string{"diff", "--name-only", "--"}, withdrawalHistoricalMigrations...)
	if out := withdrawalGit(t, root, args...); out != "" {
		t.Fatalf("historical migrations 000001-000006 were modified (T006 requires zero changes):\n%s", out)
	}
}

// TestWithdrawalMigrationUpgradeDowngradeFrom006 covers the T006 upgrade path
// on an isolated scratch database: a real 006 database gains ONLY 000007, the
// status reports 7/clean, 000007 Down drops exactly the six 007 tables while
// 002-006 rows survive, and re-up reproduces the same state.
func TestWithdrawalMigrationUpgradeDowngradeFrom006(t *testing.T) {
	dsn := withdrawalStartPostgres(t)
	ctx := context.Background()

	// Build a real database at 006, then seed representative 002-006 rows.
	subset := withdrawalSubsetFS(t, 6)
	subsetOpts := withdrawalMigrateOptions(dsn)
	subsetOpts.FS = subset
	var out bytes.Buffer
	if err := db.MigrateUp(ctx, subsetOpts, &out); err != nil {
		t.Fatalf("MigrateUp(through 6) error = %v (output %q)", err, out.String())
	}
	sqlDB := withdrawalOpenSQL(t, dsn)
	withdrawalSeedUpstreamRows(t, sqlDB)

	// Full embedded set applies ONLY 000007.
	fullOpts := withdrawalMigrateOptions(dsn)
	out.Reset()
	if err := db.MigrateUp(ctx, fullOpts, &out); err != nil {
		t.Fatalf("upgrade MigrateUp() error = %v (output %q)", err, out.String())
	}
	if !strings.Contains(out.String(), "applied=1 skipped=6 pending=0") {
		t.Fatalf("upgrade output = %q, want applied=1 skipped=6 pending=0", out.String())
	}

	out.Reset()
	if err := db.MigrateStatus(ctx, fullOpts, &out); err != nil {
		t.Fatalf("MigrateStatus() after upgrade error = %v", err)
	}
	if !strings.Contains(out.String(), "current_version=7") || !strings.Contains(out.String(), "pending=none") {
		t.Fatalf("status after upgrade = %q, want current_version=7 and pending=none", out.String())
	}
	for _, rel := range withdrawalSevenTables {
		if !withdrawalRelationExists(t, sqlDB, rel) {
			t.Errorf("%s missing after 000007 upgrade", rel)
		}
	}

	// 000007 Down via a provider mirroring internal/db: exactly version 7.
	provider := withdrawalNewProvider(t, sqlDB, migrations.FS)
	results, err := provider.DownTo(ctx, 6)
	if err != nil {
		t.Fatalf("DownTo(6): %v", err)
	}
	if len(results) != 1 || results[0].Source.Version != 7 {
		t.Fatalf("DownTo(6) rolled back %d migration(s), want exactly version 7", len(results))
	}
	for _, rel := range withdrawalSevenTables {
		if withdrawalRelationExists(t, sqlDB, rel) {
			t.Errorf("%s still exists after 000007 Down", rel)
		}
	}
	for table, want := range upstreamRowCounts {
		if got := withdrawalCount(t, sqlDB, table); got != want {
			t.Errorf("%s rows after 000007 Down = %d, want %d (002-006 data must survive)", table, got, want)
		}
	}

	out.Reset()
	if err := db.MigrateStatus(ctx, fullOpts, &out); err != nil {
		t.Fatalf("MigrateStatus() after Down error = %v", err)
	}
	if !strings.Contains(out.String(), "current_version=6") || !strings.Contains(out.String(), "pending=1") {
		t.Fatalf("status after Down = %q, want current_version=6 and pending=1", out.String())
	}

	// Re-up reproduces the same clean state.
	out.Reset()
	if err := db.MigrateUp(ctx, fullOpts, &out); err != nil {
		t.Fatalf("re-up MigrateUp() error = %v (output %q)", err, out.String())
	}
	if !strings.Contains(out.String(), "applied=1 skipped=6 pending=0") {
		t.Fatalf("re-up output = %q, want applied=1 skipped=6 pending=0", out.String())
	}
	out.Reset()
	if err := db.MigrateStatus(ctx, fullOpts, &out); err != nil {
		t.Fatalf("MigrateStatus() after re-up error = %v", err)
	}
	if !strings.Contains(out.String(), "current_version=7") || !strings.Contains(out.String(), "pending=none") {
		t.Fatalf("status after re-up = %q, want current_version=7 and pending=none", out.String())
	}
	for _, rel := range withdrawalSevenTables {
		if !withdrawalRelationExists(t, sqlDB, rel) {
			t.Errorf("%s missing after 000007 re-up", rel)
		}
	}
}

// TestWithdrawalMigrationConstraintNames probes the declared constraint names
// with raw SQL: duplicate-key conflicts must raise 23505 on the exact named
// unique/PK constraint, value-format violations must raise 23514 (never
// 23505), and the positive counterparts must not over-reject.
func TestWithdrawalMigrationConstraintNames(t *testing.T) {
	dsn := withdrawalStartPostgres(t)
	ctx := context.Background()
	var out bytes.Buffer
	if err := db.MigrateUp(ctx, withdrawalMigrateOptions(dsn), &out); err != nil {
		t.Fatalf("MigrateUp() error = %v (output %q)", err, out.String())
	}
	sqlDB := withdrawalOpenSQL(t, dsn)

	asset := withdrawalAddr("11")
	recipient := withdrawalAddr("22")
	withdrawalMustExec(t, sqlDB, `INSERT INTO caller (caller_id, label) VALUES (1001, 'probe')`)
	withdrawalMustExec(t, sqlDB, `INSERT INTO withdrawal_authorizations
		(authorization_id, caller_id, chain_id, asset, recipient, amount, state)
		VALUES ('auth-1', 1001, 1, $1, $2, 100, 'active')`, asset, recipient)

	const insertRequest = `INSERT INTO withdrawal_requests
		(request_id, caller_id, idempotency_key, authorization_id, chain_id, asset, recipient, amount)
		VALUES ($1, 1001, $2, $3, 1, $4, $5, $6)`
	const insertGrantAudit = `INSERT INTO withdrawal_grant_audit
		(operation_id, authorization_id, caller_id, action) VALUES ($1, $2, 1001, $3)`
	// insertRequestStatus is insertRequest plus an explicit status: 007 writes
	// only 'accepted' (the column DEFAULT), so any other value is a CHECK
	// violation — the negative probe below uses it to name
	// withdrawal_requests_status_check exactly.
	const insertRequestStatus = `INSERT INTO withdrawal_requests
		(request_id, caller_id, idempotency_key, authorization_id, chain_id, asset, recipient, amount, status)
		VALUES ($1, 1001, $2, $3, 1, $4, $5, $6, $7)`
	withdrawalMustExec(t, sqlDB, insertRequest, "req-1", "key-1", "auth-1", asset, recipient, "100")
	withdrawalMustExec(t, sqlDB, insertGrantAudit, "op-1", "auth-g1", "supplied")

	violations := []struct {
		name       string
		query      string
		args       []any
		code       string
		constraint string
	}{
		{"duplicate caller+idempotency key", insertRequest,
			[]any{"req-2", "key-1", "auth-2", asset, recipient, "100"}, "23505", "withdrawal_requests_caller_key_uniq"},
		{"duplicate authorization id", insertRequest,
			[]any{"req-3", "key-3", "auth-1", asset, recipient, "100"}, "23505", "withdrawal_requests_authorization_uniq"},
		{"duplicate operation id", insertGrantAudit,
			[]any{"op-1", "auth-g2", "supplied"}, "23505", "withdrawal_grant_audit_operation_id_uniq"},
		{"amount zero", insertRequest,
			[]any{"req-4", "key-4", "auth-4", asset, recipient, "0"}, "23514", "withdrawal_requests_amount_check"},
		{"amount over uint256", insertRequest,
			[]any{"req-5", "key-5", "auth-5", asset, recipient, withdrawalOverUint256}, "23514", "withdrawal_requests_amount_check"},
		{"uppercase asset", insertRequest,
			[]any{"req-6", "key-6", "auth-6", "0x" + strings.Repeat("AB", 20), recipient, "100"}, "23514", "withdrawal_requests_asset_check"},
		{"short recipient", insertRequest,
			[]any{"req-7", "key-7", "auth-7", asset, "0x" + strings.Repeat("a", 39), "100"}, "23514", "withdrawal_requests_recipient_check"},
		// status='pending' only ever violates the status CHECK: the row is
		// otherwise fresh, so no unique carrier can fire. This pins 23514 on
		// withdrawal_requests_status_check, distinct from the 23505
		// idempotency/authorization conflicts above.
		{"status pending is not accepted", insertRequestStatus,
			[]any{"req-status", "key-status", "auth-status", asset, recipient, "100", "pending"}, "23514", "withdrawal_requests_status_check"},
	}
	for _, tc := range violations {
		t.Run(tc.name, func(t *testing.T) {
			_, err := sqlDB.ExecContext(ctx, tc.query, tc.args...)
			withdrawalWantPgError(t, err, tc.code, tc.constraint)
		})
	}

	// Concurrent first-supply PK race: two goroutines, one start barrier,
	// both inserting the same authorization_id. Exactly one INSERT wins and
	// the loser names the declared primary key.
	t.Run("concurrent first-supply PK race", func(t *testing.T) {
		const insertAuthorization = `INSERT INTO withdrawal_authorizations
			(authorization_id, caller_id, chain_id, asset, recipient, amount, state)
			VALUES ('auth-race', 1001, 1, $1, $2, 100, 'active')`
		start := make(chan struct{})
		errs := make(chan error, 2)
		for i := 0; i < 2; i++ {
			go func() {
				<-start
				_, err := sqlDB.ExecContext(ctx, insertAuthorization, asset, recipient)
				errs <- err
			}()
		}
		close(start)
		var wins, dupes int
		for i := 0; i < 2; i++ {
			switch err := <-errs; {
			case err == nil:
				wins++
			case withdrawalMatchesPgError(err, "23505", "withdrawal_authorizations_pkey"):
				dupes++
			default:
				t.Fatalf("concurrent insert error = %v, want nil or 23505 on withdrawal_authorizations_pkey", err)
			}
		}
		if wins != 1 || dupes != 1 {
			t.Fatalf("concurrent PK race = %d wins / %d duplicates, want exactly 1/1", wins, dupes)
		}
	})

	// Positive counterparts: the constraints above must not over-reject.
	positives := []struct {
		name  string
		query string
		args  []any
	}{
		{"max uint256 amount accepted", insertRequest,
			[]any{"req-max", "key-max", "auth-max", asset, recipient, withdrawalMaxUint256}},
		{"distinct authorization accepted", insertRequest,
			[]any{"req-pos", "key-pos", "auth-pos", asset, recipient, "1"}},
	}
	for _, tc := range positives {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := sqlDB.ExecContext(ctx, tc.query, tc.args...); err != nil {
				t.Fatalf("expected insert to succeed: %v", err)
			}
		})
	}

	// Zero illegal rows: the rejected status='pending' INSERT left no
	// non-'accepted' row behind, so the CHECK is a true rejection and not a
	// silent rewrite. This is the persistence half of the pending probe.
	var illegalStatus int
	if err := sqlDB.QueryRowContext(ctx,
		`SELECT count(*) FROM withdrawal_requests WHERE status <> 'accepted'`).Scan(&illegalStatus); err != nil {
		t.Fatalf("count non-accepted withdrawal_requests: %v", err)
	}
	if illegalStatus != 0 {
		t.Fatalf("withdrawal_requests rows with status <> 'accepted' = %d, want 0", illegalStatus)
	}
}

// TestWithdrawalMigrationUpstreamSmokeAfterUpgrade covers T006 item (e): after
// a full upgrade to 000007, one representative write into each 002-006 table
// still succeeds (upstream storage is not disturbed by 007).
func TestWithdrawalMigrationUpstreamSmokeAfterUpgrade(t *testing.T) {
	dsn := withdrawalStartPostgres(t)
	var out bytes.Buffer
	if err := db.MigrateUp(context.Background(), withdrawalMigrateOptions(dsn), &out); err != nil {
		t.Fatalf("MigrateUp() error = %v (output %q)", err, out.String())
	}
	sqlDB := withdrawalOpenSQL(t, dsn)

	withdrawalSeedUpstreamRows(t, sqlDB)

	for table, want := range upstreamRowCounts {
		if got := withdrawalCount(t, sqlDB, table); got != want {
			t.Errorf("%s rows after post-upgrade write = %d, want %d", table, got, want)
		}
	}
}
