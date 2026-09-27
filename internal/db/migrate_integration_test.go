//go:build integration

package db

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"testing/fstest"
	"time"
)

// startPostgres returns the DSN of a test-private, EMPTY PostgreSQL database
// and is the single entry point every integration test in this package uses.
//
// Non-whitelisted tests get a freshly created database inside the package-wide
// container booted by TestMain (db_shared_pg_test.go): creation and
// DROP DATABASE ... WITH (FORCE) cleanup are per test, so database-level
// isolation is preserved. The audited down/destructive lanes keep a dedicated
// container (dbDedicatedContainerTests -> startPostgresDedicated). Either way
// the database starts empty and the caller runs its own migrations, so every
// existing empty-DB, overlay-FS and partial-FS assertion stays exact and
// migration coverage is unchanged.
//
// Docker gating moved to TestMain: when no provider is healthy TestMain exits
// 0 before m.Run locally (package skipped, never silently passed) and exits 1
// under CI=true or TXHARBOR_REQUIRE_DOCKER=1. The dedicated whitelist path
// keeps testcontainers.SkipIfProviderIsNotHealthy as a second line of defense.
func startPostgres(t *testing.T) string {
	t.Helper()
	if dbDedicatedContainerTests[t.Name()] {
		return startPostgresDedicated(t)
	}
	return startPostgresDerived(t)
}

func openTestSQL(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	// The "pgx" driver is registered by db/migrate.go's stdlib import.
	sqlDB, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open sql db: %v", err)
	}
	t.Cleanup(func() { sqlDB.Close() })
	return sqlDB
}

func testMigrateOptions(dsn string) MigrateOptions {
	return MigrateOptions{DSN: dsn, LockTimeout: 5 * time.Second, ConnectTimeout: 5 * time.Second}
}

// TestMigrateUpStatusAndRepeatOnEmptyDatabase covers FR-005/006 (SC-005):
// empty DB -> full init, status reports the target, a second run skips
// everything without side effects.
func TestMigrateUpStatusAndRepeatOnEmptyDatabase(t *testing.T) {
	dsn := startPostgres(t)
	ctx := context.Background()
	opts := testMigrateOptions(dsn)

	// Derive every count from the embedded set so adding a migration never
	// invalidates this test (003 added 000003).
	files, err := MigrationFiles(Migrations)
	if err != nil {
		t.Fatalf("list embedded migrations: %v", err)
	}
	target := files[len(files)-1].Version

	var out bytes.Buffer
	if err := MigrateUp(ctx, opts, &out); err != nil {
		t.Fatalf("first MigrateUp() error = %v", err)
	}
	if want := fmt.Sprintf("applied=%d skipped=0 pending=0", len(files)); !strings.Contains(out.String(), want) {
		t.Fatalf("first MigrateUp() output = %q, want %q", out.String(), want)
	}

	out.Reset()
	if err := MigrateStatus(ctx, opts, &out); err != nil {
		t.Fatalf("MigrateStatus() error = %v", err)
	}
	if want := fmt.Sprintf("current_version=%d", target); !strings.Contains(out.String(), want) || !strings.Contains(out.String(), "pending=none") {
		t.Fatalf("MigrateStatus() output = %q, want %q and pending=none", out.String(), want)
	}

	if _, err := CheckCompatibility(ctx, opts); err != nil {
		t.Fatalf("CheckCompatibility() after migrate error = %v", err)
	}

	out.Reset()
	if err := MigrateUp(ctx, opts, &out); err != nil {
		t.Fatalf("second MigrateUp() error = %v", err)
	}
	if want := fmt.Sprintf("applied=0 skipped=%d pending=0", len(files)); !strings.Contains(out.String(), want) {
		t.Fatalf("second MigrateUp() output = %q, want %q", out.String(), want)
	}

	// Exactly one applied row per version: no duplicate application.
	sqlDB := openTestSQL(t, dsn)
	for _, f := range files {
		var rows int
		if err := sqlDB.QueryRowContext(ctx,
			"SELECT count(*) FROM goose_db_version WHERE version_id = $1 AND is_applied", f.Version).Scan(&rows); err != nil {
			t.Fatalf("count version rows: %v", err)
		}
		if rows != 1 {
			t.Fatalf("goose_db_version rows for version %d = %d, want 1", f.Version, rows)
		}
	}
}

// TestMigrateFailureRollsBackAndRetries covers FR-007 (SC-005): a failing
// migration is not recorded as successful and can be retried after it is
// fixed, with a clean rollback in between.
func TestMigrateFailureRollsBackAndRetries(t *testing.T) {
	dsn := startPostgres(t)
	ctx := context.Background()

	fsys := fstest.MapFS{
		"000001_ok.sql": &fstest.MapFile{Data: []byte(
			"-- +goose Up\nCREATE TABLE t001 (id int);\n-- +goose Down\nDROP TABLE t001;\n")},
		"000002_bad.sql": &fstest.MapFile{Data: []byte(
			"-- +goose Up\nCREATE TABLE t002 (id int);\nTHIS IS NOT VALID SQL;\n-- +goose Down\nDROP TABLE t002;\n")},
	}
	opts := testMigrateOptions(dsn)
	opts.FS = fsys

	var out bytes.Buffer
	if err := MigrateUp(ctx, opts, &out); err == nil {
		t.Fatal("MigrateUp() with broken migration: expected error")
	}
	if !strings.Contains(out.String(), "failed_version=2") {
		t.Fatalf("failure output = %q, want failed_version=2", out.String())
	}

	sqlDB := openTestSQL(t, dsn)
	var applied int
	if err := sqlDB.QueryRowContext(ctx,
		"SELECT count(*) FROM goose_db_version WHERE version_id = 2 AND is_applied").Scan(&applied); err != nil {
		t.Fatalf("count failed version rows: %v", err)
	}
	if applied != 0 {
		t.Fatalf("failed version 2 recorded as applied %d time(s), want 0", applied)
	}
	var t002 *string
	if err := sqlDB.QueryRowContext(ctx, "SELECT to_regclass('t002')").Scan(&t002); err != nil {
		t.Fatalf("to_regclass(t002): %v", err)
	}
	if t002 != nil {
		t.Fatalf("failed migration left table t002 behind: %v", *t002)
	}

	// Fix the migration and retry: only the pending version applies.
	fsys["000002_bad.sql"] = &fstest.MapFile{Data: []byte(
		"-- +goose Up\nCREATE TABLE t002 (id int);\n-- +goose Down\nDROP TABLE t002;\n")}
	out.Reset()
	if err := MigrateUp(ctx, opts, &out); err != nil {
		t.Fatalf("retry MigrateUp() error = %v", err)
	}
	if !strings.Contains(out.String(), "applied=1 skipped=1 pending=0") {
		t.Fatalf("retry output = %q", out.String())
	}
}

// TestCheckCompatibilityDetectsPendingMigration covers FR-008 (US4): serve
// must refuse while the database is behind the embedded target.
func TestCheckCompatibilityDetectsPendingMigration(t *testing.T) {
	dsn := startPostgres(t)
	ctx := context.Background()
	opts := testMigrateOptions(dsn)

	_, err := CheckCompatibility(ctx, opts)
	if err == nil {
		t.Fatal("CheckCompatibility() on empty database: expected pending-migration error")
	}
	if !strings.Contains(err.Error(), "pending") || !strings.Contains(err.Error(), "migrate up") {
		t.Fatalf("compatibility error lacks actionable text: %v", err)
	}
}
