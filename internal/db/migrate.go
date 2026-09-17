// Package db owns the PostgreSQL pool and the goose migration integration.
//
// 001 has exactly one tool-managed table (goose_db_version); the baseline
// migration creates no schema objects (FR-017/FR-018).
package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // database/sql driver for goose
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"

	"github.com/xtianxx/txharbor/internal/logx"
	"github.com/xtianxx/txharbor/migrations"
)

// Migrations is the embedded migration filesystem (see migrations/embed.go).
var Migrations fs.FS = migrations.FS

// versionTable is goose's bookkeeping table. Kept as a constant so the
// read-only compatibility check never depends on goose internals.
const versionTable = "goose_db_version"

// MigrateOptions configures one migrate/status/compatibility operation.
type MigrateOptions struct {
	DSN            string
	LockTimeout    time.Duration // bounded wait for the DB-level migration lock
	ConnectTimeout time.Duration // bounded connect/ping for this operation
	FS             fs.FS         // optional; defaults to embedded migrations
}

func (o MigrateOptions) fsys() fs.FS {
	if o.FS == nil {
		return Migrations
	}
	return o.FS
}

func (o MigrateOptions) connectTimeout() time.Duration {
	if o.ConnectTimeout <= 0 {
		return 5 * time.Second
	}
	return o.ConnectTimeout
}

// MigrationFile is one versioned migration file.
type MigrationFile struct {
	Version int64
	Name    string
}

// MigrationFiles lists the SQL migration files in ascending version order and
// rejects malformed names, non-positive versions and duplicates.
func MigrationFiles(fsys fs.FS) ([]MigrationFile, error) {
	names, err := fs.Glob(fsys, "*.sql")
	if err != nil {
		return nil, fmt.Errorf("list migration files: %w", err)
	}
	if len(names) == 0 {
		return nil, errors.New("no migration files found")
	}
	seen := make(map[int64]string, len(names))
	files := make([]MigrationFile, 0, len(names))
	for _, name := range names {
		base := path.Base(name)
		idx := strings.IndexByte(base, '_')
		if idx <= 0 {
			return nil, fmt.Errorf("migration file %q must be named NNNNNN_name.sql", base)
		}
		version, err := strconv.ParseInt(base[:idx], 10, 64)
		if err != nil || version <= 0 {
			return nil, fmt.Errorf("migration file %q has an invalid version prefix %q", base, base[:idx])
		}
		if other, dup := seen[version]; dup {
			return nil, fmt.Errorf("duplicate migration version %d (%s and %s)", version, other, base)
		}
		seen[version] = base
		files = append(files, MigrationFile{Version: version, Name: base})
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Version < files[j].Version })
	return files, nil
}

// SchemaState is a read-only snapshot of the migration bookkeeping.
type SchemaState struct {
	TableExists bool
	Current     int64
	Applied     []int64
	Pending     []int64
}

// Inspect reads the migration state without creating or changing anything.
// On an empty database it reports all embedded versions as pending.
func Inspect(ctx context.Context, opts MigrateOptions) (SchemaState, error) {
	files, err := MigrationFiles(opts.fsys())
	if err != nil {
		return SchemaState{}, err
	}
	sqlDB, err := openSQL(ctx, opts)
	if err != nil {
		return SchemaState{}, err
	}
	defer sqlDB.Close()
	return inspect(ctx, sqlDB, files)
}

func inspect(ctx context.Context, sqlDB *sql.DB, files []MigrationFile) (SchemaState, error) {
	state := SchemaState{}
	if err := sqlDB.QueryRowContext(ctx, "SELECT to_regclass($1) IS NOT NULL", versionTable).Scan(&state.TableExists); err != nil {
		return SchemaState{}, fmt.Errorf("check migration version table: %w", err)
	}
	applied := make(map[int64]bool)
	if state.TableExists {
		rows, err := sqlDB.QueryContext(ctx,
			"SELECT version_id FROM "+versionTable+" WHERE is_applied AND version_id > 0 ORDER BY version_id")
		if err != nil {
			return SchemaState{}, fmt.Errorf("read applied migrations: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var v int64
			if err := rows.Scan(&v); err != nil {
				return SchemaState{}, fmt.Errorf("scan applied migration: %w", err)
			}
			applied[v] = true
			state.Applied = append(state.Applied, v)
		}
		if err := rows.Err(); err != nil {
			return SchemaState{}, fmt.Errorf("read applied migrations: %w", err)
		}
	}
	if len(state.Applied) > 0 {
		state.Current = state.Applied[len(state.Applied)-1]
	}
	for _, f := range files {
		if !applied[f.Version] {
			state.Pending = append(state.Pending, f.Version)
		}
	}
	return state, nil
}

// Compatibility is the outcome of the serve-time migration check.
type Compatibility struct {
	Current int64
	Target  int64
	Pending int
}

// CheckCompatibility rejects serving unless the database is exactly at the
// program's target version: no pending migrations and no unknown/newer
// version (FR-008, US4). It never creates or changes anything.
func CheckCompatibility(ctx context.Context, opts MigrateOptions) (Compatibility, error) {
	files, err := MigrationFiles(opts.fsys())
	if err != nil {
		return Compatibility{}, err
	}
	target := files[len(files)-1].Version
	known := make(map[int64]bool, len(files))
	for _, f := range files {
		known[f.Version] = true
	}

	state, err := Inspect(ctx, opts)
	if err != nil {
		return Compatibility{}, err
	}
	result := Compatibility{Current: state.Current, Target: target, Pending: len(state.Pending)}

	if state.Current > target {
		return result, fmt.Errorf("database schema version %d is newer than the program expected %d; refusing to serve",
			state.Current, target)
	}
	if state.Current != 0 && !known[state.Current] {
		return result, fmt.Errorf("database schema version %d is not a known migration (program knows up to %d); refusing to serve",
			state.Current, target)
	}
	if len(state.Pending) > 0 {
		return result, fmt.Errorf("database has %d pending migration(s), current=%d target=%d; run %q first",
			len(state.Pending), state.Current, target, "txharbor migrate up")
	}
	return result, nil
}

// MigrateUp applies all pending migrations under the DB-level session lock and
// prints the contract summary line: applied=N skipped=M pending=K.
//
// Concurrent callers serialize on the advisory lock; a waiter gives up after
// LockTimeout and the caller exits non-zero (FR-006).
func MigrateUp(ctx context.Context, opts MigrateOptions, out io.Writer) error {
	files, err := MigrationFiles(opts.fsys())
	if err != nil {
		return err
	}
	sqlDB, err := openSQL(ctx, opts)
	if err != nil {
		return err
	}
	defer sqlDB.Close()

	provider, err := newProvider(sqlDB, opts)
	if err != nil {
		return err
	}
	results, err := provider.Up(ctx)
	if err != nil {
		var partial *goose.PartialError
		if errors.As(err, &partial) && partial.Failed != nil && partial.Failed.Source != nil {
			fmt.Fprintf(out, "failed_version=%d reason=%s\n",
				partial.Failed.Source.Version, logx.Redact(partial.Err.Error()))
		}
		return err
	}

	state, err := inspect(ctx, sqlDB, files)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "applied=%d skipped=%d pending=%d\n", len(results), len(files)-len(results), len(state.Pending))
	return nil
}

// MigrateStatus prints the current version and the pending version list.
func MigrateStatus(ctx context.Context, opts MigrateOptions, out io.Writer) error {
	files, err := MigrationFiles(opts.fsys())
	if err != nil {
		return err
	}
	names := make(map[int64]string, len(files))
	for _, f := range files {
		names[f.Version] = f.Name
	}
	state, err := Inspect(ctx, opts)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "current_version=%d\n", state.Current)
	if len(state.Pending) == 0 {
		fmt.Fprintln(out, "pending=none")
		return nil
	}
	fmt.Fprintf(out, "pending=%d\n", len(state.Pending))
	for _, v := range state.Pending {
		fmt.Fprintf(out, "pending_version=%d name=%s\n", v, names[v])
	}
	return nil
}

func newProvider(sqlDB *sql.DB, opts MigrateOptions) (*goose.Provider, error) {
	// goose's session locker waits in whole-second retry intervals; round the
	// configured timeout up so the bounded wait is never shorter than asked.
	seconds := uint64(opts.LockTimeout / time.Second)
	if opts.LockTimeout%time.Second != 0 {
		seconds++
	}
	if seconds < 1 {
		seconds = 1
	}
	locker, err := lock.NewPostgresSessionLocker(lock.WithLockTimeout(1, seconds))
	if err != nil {
		return nil, fmt.Errorf("create migration session locker: %w", err)
	}
	return goose.NewProvider(goose.DialectPostgres, sqlDB, opts.fsys(),
		goose.WithSessionLocker(locker),
		goose.WithDisableGlobalRegistry(true),
		// R-PB8 merge order: the carrier merges first as 000010, 009 lands
		// later as 000009 (the gap at 9 is reserved, no renumbering). A
		// database already at {1..8,10} must therefore fill 9 with a plain
		// `migrate up`. goose refuses out-of-order ("missing") migrations by
		// default, so the gap-fill proof (T041, PB-05 sequence (d)) needs an
		// allow-missing provider — no special CLI option at the call site.
		// This never weakens the serve gate: CheckCompatibility still refuses
		// while any version is pending and refuses unknown/newer versions.
		goose.WithAllowOutofOrder(true),
	)
}

func openSQL(ctx context.Context, opts MigrateOptions) (*sql.DB, error) {
	sqlDB, err := sql.Open("pgx", opts.DSN)
	if err != nil {
		return nil, fmt.Errorf("open postgres connection: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, opts.connectTimeout())
	defer cancel()
	if err := sqlDB.PingContext(pingCtx); err != nil {
		sqlDB.Close()
		return nil, fmt.Errorf("database ping failed: %w", err)
	}
	return sqlDB, nil
}
