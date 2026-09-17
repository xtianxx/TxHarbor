//go:build integration

// migration_integration_test.go owns spec task T005 for 008-nonce-manager:
// it proves migration 000008 applies and rolls back cleanly over a real
// 000001-000007 baseline and that its named constraints carry the exact
// declared names.
//
// Every database assertion here is a raw-SQL probe; no internal/nonce
// classifier (binding, classify, numeric, hold, ...) is imported or
// exercised -- app-level classification belongs to T012/T022/T038. Each test
// boots its own isolated scratch PostgreSQL container via testcontainers and
// terminates it in t.Cleanup.
package nonce_test

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io/fs"
	"os/exec"
	"strconv"
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
	// nonceMaxUint64 is the declared upper bound enforced by 000008's nonce
	// CHECK constraints (2^64-1; R13). nonceOverUint64 is 2^64.
	nonceMaxUint64  = "18446744073709551615"
	nonceOverUint64 = "18446744073709551616"
)

// nonceEightTables are the seven objects created by 000008 and dropped by its
// Down, in the exact set the migration declares.
var nonceEightTables = []string{
	"nonce_wallet_registry",
	"nonce_scope_state",
	"nonce_bindings",
	"nonce_binding_events",
	"nonce_observations",
	"nonce_scope_holds",
	"nonce_ops_audit",
}

// nonceHistoricalMigrations are the frozen migrations T005 must never modify;
// the history-untouched probe diffs exactly this set.
var nonceHistoricalMigrations = []string{
	"migrations/000001_baseline.sql",
	"migrations/000002_chain_indexer.sql",
	"migrations/000003_event_indexing.sql",
	"migrations/000004_deposit_detection.sql",
	"migrations/000005_confirmation_tracking.sql",
	"migrations/000006_reorg_recovery.sql",
	"migrations/000007_withdrawal_creation.sql",
}

// nonceDeclaredConstraints are the exact ConstraintName values T004's line
// requires 000008 to declare explicitly. The catalog probe proves each one
// exists in the migrated schema; the negative probes prove the ones consumed
// by classification are additionally elicited with those exact names.
var nonceDeclaredConstraints = []string{
	"nonce_bindings_pkey",
	"nonce_bindings_intent_uniq",
	"nonce_bindings_scope_nonce_uniq",
	"nonce_bindings_registry_fkey",
	"nonce_bindings_state_check",
	"nonce_bindings_terminal_consistency",
	"nonce_bindings_nonce_range",
	"nonce_bindings_intent_shape",
	"nonce_wallet_registry_pkey",
	"nonce_scope_state_pkey",
	"nonce_scope_state_registry_fkey",
	"nonce_binding_events_binding_to_uniq",
	"nonce_scope_holds_status_consistency",
	"nonce_scope_holds_registry_fkey",
	"nonce_ops_audit_pkey",
	"nonce_ops_audit_operation_id_uniq",
}

// nonceStartPostgres boots a real PostgreSQL container and returns its DSN.
// Skips (never passes) when no Docker provider is available.
func nonceStartPostgres(t *testing.T) string {
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

func nonceOpenSQL(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	sqlDB, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open sql db: %v", err)
	}
	t.Cleanup(func() { sqlDB.Close() })
	return sqlDB
}

func nonceMigrateOptions(dsn string) db.MigrateOptions {
	return db.MigrateOptions{DSN: dsn, LockTimeout: 5 * time.Second, ConnectTimeout: 5 * time.Second}
}

// nonceMigrationsThrough returns the lane's embedded migrations capped at
// maxVersion. 008-era tests pin their baseline meaning (000001-000007 plus
// 000008) even after later planning numbers (e.g. PB 000010) land in the lane
// tree; the cap is test scope, not a migration edit.
func nonceMigrationsThrough(t *testing.T, maxVersion int64) fstest.MapFS {
	t.Helper()
	names, err := fs.Glob(migrations.FS, "*.sql")
	if err != nil {
		t.Fatalf("glob embedded migrations: %v", err)
	}
	out := make(fstest.MapFS, len(names))
	for _, name := range names {
		i := strings.IndexByte(name, '_')
		if i <= 0 {
			t.Fatalf("migration %q lacks NNNNNN_ prefix", name)
		}
		v, err := strconv.ParseInt(name[:i], 10, 64)
		if err != nil {
			t.Fatalf("migration %q version: %v", name, err)
		}
		if v > maxVersion {
			continue
		}
		data, err := fs.ReadFile(migrations.FS, name)
		if err != nil {
			t.Fatalf("read embedded migration %s: %v", name, err)
		}
		out[name] = &fstest.MapFile{Data: data}
	}
	return out
}

// nonceNewProvider replicates internal/db's unexported newProvider (same
// locker, same options, same embedded FS) since that constructor is not
// importable.
func nonceNewProvider(t *testing.T, sqlDB *sql.DB) *goose.Provider {
	return nonceNewProviderFS(t, sqlDB, migrations.FS)
}

// nonceNewProviderFS is nonceNewProvider over an explicit migrations FS, so a
// test can pin its FS scope (e.g. through 000008) without touching the lane.
func nonceNewProviderFS(t *testing.T, sqlDB *sql.DB, fsys fs.FS) *goose.Provider {
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

func nonceMustExec(t *testing.T, sqlDB *sql.DB, query string, args ...any) {
	t.Helper()
	if _, err := sqlDB.ExecContext(context.Background(), query, args...); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

// nonceWantPgError asserts err is a *pgconn.PgError with the exact SQLSTATE
// code and the exact declared constraint name.
func nonceWantPgError(t *testing.T, err error, code, constraint string) {
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

func nonceRelationExists(t *testing.T, sqlDB *sql.DB, name string) bool {
	t.Helper()
	var exists bool
	if err := sqlDB.QueryRowContext(context.Background(),
		"SELECT to_regclass($1) IS NOT NULL", name).Scan(&exists); err != nil {
		t.Fatalf("to_regclass(%s): %v", name, err)
	}
	return exists
}

func nonceAddr(fill string) string { return "0x" + strings.Repeat(fill, 20) }

// nonceGit runs git in dir (or the process CWD when empty) and returns
// trimmed stdout, failing the test on any git error.
func nonceGit(t *testing.T, dir string, args ...string) string {
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

// TestNonceMigrationHistoryUntouched proves T005 modified no historical
// migration: replaying 000001-000007 is allowed, editing them is not.
func TestNonceMigrationHistoryUntouched(t *testing.T) {
	root := nonceGit(t, "", "rev-parse", "--show-toplevel")
	if root == "" {
		t.Fatal("git rev-parse --show-toplevel returned no repository root")
	}
	args := append([]string{"diff", "--name-only", "--"}, nonceHistoricalMigrations...)
	if out := nonceGit(t, root, args...); out != "" {
		t.Fatalf("historical migrations 000001-000007 were modified (T005 requires zero changes):\n%s", out)
	}
}

// TestNonceMigrationUpStatusDownUp covers the T005 upgrade path on an
// isolated scratch database: baseline through 000008 applies as one goose up,
// the status reports 8/clean, 000008 Down drops exactly the seven 008 tables,
// and re-up reproduces the same state.
func TestNonceMigrationUpStatusDownUp(t *testing.T) {
	dsn := nonceStartPostgres(t)
	ctx := context.Background()
	opts := nonceMigrateOptions(dsn)
	// Pin the FS through 000008: the lane tree may carry later planning
	// numbers (PB 000010) that this 008-era baseline test must not absorb.
	opts.FS = nonceMigrationsThrough(t, 8)

	var out bytes.Buffer
	if err := db.MigrateUp(ctx, opts, &out); err != nil {
		t.Fatalf("MigrateUp() error = %v (output %q)", err, out.String())
	}
	if !strings.Contains(out.String(), "applied=8 skipped=0 pending=0") {
		t.Fatalf("fresh MigrateUp output = %q, want applied=8 skipped=0 pending=0", out.String())
	}

	out.Reset()
	if err := db.MigrateStatus(ctx, opts, &out); err != nil {
		t.Fatalf("MigrateStatus() error = %v", err)
	}
	if !strings.Contains(out.String(), "current_version=8") || !strings.Contains(out.String(), "pending=none") {
		t.Fatalf("status after up = %q, want current_version=8 and pending=none", out.String())
	}

	sqlDB := nonceOpenSQL(t, dsn)
	for _, rel := range nonceEightTables {
		if !nonceRelationExists(t, sqlDB, rel) {
			t.Errorf("%s missing after 000008 up", rel)
		}
	}

	// 000008 Down via a provider mirroring internal/db: exactly version 8.
	provider := nonceNewProviderFS(t, sqlDB, opts.FS)
	results, err := provider.DownTo(ctx, 7)
	if err != nil {
		t.Fatalf("DownTo(7): %v", err)
	}
	if len(results) != 1 || results[0].Source.Version != 8 {
		t.Fatalf("DownTo(7) rolled back %d migration(s), want exactly version 8", len(results))
	}
	for _, rel := range nonceEightTables {
		if nonceRelationExists(t, sqlDB, rel) {
			t.Errorf("%s still exists after 000008 Down", rel)
		}
	}

	out.Reset()
	if err := db.MigrateStatus(ctx, opts, &out); err != nil {
		t.Fatalf("MigrateStatus() after Down error = %v", err)
	}
	if !strings.Contains(out.String(), "current_version=7") || !strings.Contains(out.String(), "pending=1") {
		t.Fatalf("status after Down = %q, want current_version=7 and pending=1", out.String())
	}

	// Re-up reproduces the same clean state.
	results, err = provider.Up(ctx)
	if err != nil {
		t.Fatalf("Up() after Down: %v", err)
	}
	if len(results) != 1 || results[0].Source.Version != 8 {
		t.Fatalf("re-up applied %d migration(s), want exactly version 8", len(results))
	}
	for _, rel := range nonceEightTables {
		if !nonceRelationExists(t, sqlDB, rel) {
			t.Errorf("%s missing after 000008 re-up", rel)
		}
	}

	out.Reset()
	if err := db.MigrateStatus(ctx, opts, &out); err != nil {
		t.Fatalf("MigrateStatus() after re-up error = %v", err)
	}
	if !strings.Contains(out.String(), "current_version=8") || !strings.Contains(out.String(), "pending=none") {
		t.Fatalf("status after re-up = %q, want current_version=8 and pending=none", out.String())
	}
}

// TestNonceMigrationConstraintNames probes the declared constraint names with
// raw SQL: every T004-declared ConstraintName exists in the catalog, duplicate-
// key conflicts raise 23505 on the exact named unique/PK constraint, an
// unregistered sender raises 23503 on the exact named FK, and CHECK violations
// raise 23514 on the exact named CHECK (never 23505). Positive counterparts
// prove the constraints do not over-reject.
func TestNonceMigrationConstraintNames(t *testing.T) {
	dsn := nonceStartPostgres(t)
	ctx := context.Background()
	var out bytes.Buffer
	if err := db.MigrateUp(ctx, nonceMigrateOptions(dsn), &out); err != nil {
		t.Fatalf("MigrateUp() error = %v (output %q)", err, out.String())
	}
	sqlDB := nonceOpenSQL(t, dsn)

	// Catalog probe: all 16 declared names exist exactly as written.
	rows, err := sqlDB.QueryContext(ctx,
		`SELECT conname FROM pg_constraint WHERE conname = ANY($1::text[])`, nonceDeclaredConstraints)
	if err != nil {
		t.Fatalf("query pg_constraint: %v", err)
	}
	found := map[string]int{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan conname: %v", err)
		}
		found[name]++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate pg_constraint: %v", err)
	}
	for _, name := range nonceDeclaredConstraints {
		if found[name] != 1 {
			t.Errorf("declared constraint %s present %d times in pg_constraint, want exactly 1", name, found[name])
		}
	}

	registered := nonceAddr("22")
	unregistered := nonceAddr("33")
	authorizationVersion := strings.Repeat("a", 64)

	nonceMustExec(t, sqlDB, `INSERT INTO nonce_wallet_registry
		(chain_id, sender, state, registry_seq) VALUES (1, $1, 'active', 1)`, registered)

	const insertBinding = `INSERT INTO nonce_bindings
		(binding_id, intent_id, chain_id, sender, nonce, state, authorization_id,
		 authorization_version, registry_seq, allocation_observation_id)
		VALUES ($1, $2, 1, $3, $4, $5, $6, $7, 1, $8)`
	const insertBindingConsumed = `INSERT INTO nonce_bindings
		(binding_id, intent_id, chain_id, sender, nonce, state, authorization_id,
		 authorization_version, registry_seq, allocation_observation_id, consumed_at)
		VALUES ($1, $2, 1, $3, $4, $5, $6, $7, 1, $8, $9)`
	const insertEvent = `INSERT INTO nonce_binding_events (binding_id, to_state) VALUES ($1, $2)`
	const insertAudit = `INSERT INTO nonce_ops_audit
		(operation_id, action, chain_id, sender, outcome) VALUES ($1, $2, 1, $3, $4)`

	nonceMustExec(t, sqlDB, insertBinding,
		"nb-1", "intent-1", registered, "1", "allocated", "auth-1", authorizationVersion, "no-1")
	nonceMustExec(t, sqlDB, insertEvent, "nb-1", "allocated")
	nonceMustExec(t, sqlDB, insertAudit, "op-1", "registry_register", registered, "applied")

	violations := []struct {
		name       string
		query      string
		args       []any
		code       string
		constraint string
	}{
		{"duplicate binding_id", insertBinding,
			[]any{"nb-1", "intent-pk", registered, "10", "allocated", "auth-pk", authorizationVersion, "no-pk"}, "23505", "nonce_bindings_pkey"},
		{"duplicate intent_id", insertBinding,
			[]any{"nb-2", "intent-1", registered, "2", "allocated", "auth-2", authorizationVersion, "no-2"}, "23505", "nonce_bindings_intent_uniq"},
		{"duplicate chain+sender+nonce", insertBinding,
			[]any{"nb-3", "intent-3", registered, "1", "allocated", "auth-3", authorizationVersion, "no-3"}, "23505", "nonce_bindings_scope_nonce_uniq"},
		{"duplicate operation_id", insertAudit,
			[]any{"op-1", "registry_disable", registered, "nop"}, "23505", "nonce_ops_audit_operation_id_uniq"},
		{"duplicate binding+to_state", insertEvent,
			[]any{"nb-1", "allocated"}, "23505", "nonce_binding_events_binding_to_uniq"},
		// The sender below is format-valid but absent from the registry: the
		// named FK is the only violated carrier.
		{"unregistered sender", insertBinding,
			[]any{"nb-fk", "intent-fk", unregistered, "4", "allocated", "auth-fk", authorizationVersion, "no-fk"}, "23503", "nonce_bindings_registry_fkey"},
		// nonce = 2^64 only ever violates the range CHECK: the row is
		// otherwise fresh, so no unique carrier can fire (23514, not 23505).
		{"nonce over 2^64-1", insertBinding,
			[]any{"nb-range", "intent-range", registered, nonceOverUint64, "allocated", "auth-range", authorizationVersion, "no-range"}, "23514", "nonce_bindings_nonce_range"},
		// state='consumed' without consumed_at only ever violates the
		// terminal-consistency CHECK (23514, not 23505).
		{"consumed without consumed_at", insertBindingConsumed,
			[]any{"nb-term", "intent-term", registered, "5", "consumed", "auth-term", authorizationVersion, "no-term", nil}, "23514", "nonce_bindings_terminal_consistency"},
	}
	for _, tc := range violations {
		t.Run(tc.name, func(t *testing.T) {
			_, err := sqlDB.ExecContext(ctx, tc.query, tc.args...)
			nonceWantPgError(t, err, tc.code, tc.constraint)
		})
	}

	// Positive counterparts: the constraints above must not over-reject.
	positives := []struct {
		name  string
		query string
		args  []any
	}{
		{"max uint64 nonce accepted", insertBinding,
			[]any{"nb-max", "intent-max", registered, nonceMaxUint64, "allocated", "auth-max", authorizationVersion, "no-max"}},
		{"consumed with consumed_at accepted", insertBindingConsumed,
			[]any{"nb-consumed", "intent-consumed", registered, "6", "consumed", "auth-consumed", authorizationVersion, "no-consumed", time.Now().UTC()}},
		{"distinct to_state accepted", insertEvent,
			[]any{"nb-1", "in_flight"}},
	}
	for _, tc := range positives {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := sqlDB.ExecContext(ctx, tc.query, tc.args...); err != nil {
				t.Fatalf("expected insert to succeed: %v", err)
			}
		})
	}

	// Zero illegal rows: the rejected range/terminal INSERTs left no trace,
	// so the CHECKs are true rejections and not silent rewrites.
	var illegalNonce, illegalTerminal int
	if err := sqlDB.QueryRowContext(ctx, `SELECT
		count(*) FILTER (WHERE nonce > 18446744073709551615),
		count(*) FILTER (WHERE (state = 'consumed') <> (consumed_at IS NOT NULL))
		FROM nonce_bindings`).Scan(&illegalNonce, &illegalTerminal); err != nil {
		t.Fatalf("count illegal nonce_bindings rows: %v", err)
	}
	if illegalNonce != 0 || illegalTerminal != 0 {
		t.Fatalf("illegal nonce_bindings rows = %d out-of-range / %d terminal-inconsistent, want 0/0",
			illegalNonce, illegalTerminal)
	}
}
