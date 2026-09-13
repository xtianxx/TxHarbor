//go:build integration

package db

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/jackc/pgx/v5/pgconn"
)

// hash64/addr40 produce lowercase 0x-prefixed hex fixtures matching the
// migration format checks (fill must be one or two lowercase hex chars).
func hash64(fill string) string { return "0x" + strings.Repeat(fill, 32) }
func addr40(fill string) string { return "0x" + strings.Repeat(fill, 20) }

// migrateUpAll applies the full embedded migration set to dsn.
func migrateUpAll(t *testing.T, dsn string) {
	t.Helper()
	var out bytes.Buffer
	if err := MigrateUp(context.Background(), testMigrateOptions(dsn), &out); err != nil {
		t.Fatalf("MigrateUp() error = %v (output %q)", err, out.String())
	}
}

// migrateUpThrough applies embedded migrations up to maxVersion inclusive and
// returns how many files that subset contains. Used to build a real "database
// at 003" before testing the 004 upgrade.
func migrateUpThrough(t *testing.T, dsn string, maxVersion int64) int {
	t.Helper()
	files, err := MigrationFiles(Migrations)
	if err != nil {
		t.Fatalf("list embedded migrations: %v", err)
	}
	fsys := fstest.MapFS{}
	applied := 0
	for _, f := range files {
		if f.Version > maxVersion {
			continue
		}
		data, err := fs.ReadFile(Migrations, f.Name)
		if err != nil {
			t.Fatalf("read embedded %s: %v", f.Name, err)
		}
		fsys[f.Name] = &fstest.MapFile{Data: data}
		applied++
	}
	opts := testMigrateOptions(dsn)
	opts.FS = fsys
	var out bytes.Buffer
	if err := MigrateUp(context.Background(), opts, &out); err != nil {
		t.Fatalf("MigrateUp(through %d) error = %v (output %q)", maxVersion, err, out.String())
	}
	if !strings.Contains(out.String(), fmt.Sprintf("applied=%d", applied)) {
		t.Fatalf("MigrateUp(through %d) output = %q, want applied=%d", maxVersion, out.String(), applied)
	}
	return applied
}

func mustExec(t *testing.T, sqlDB *sql.DB, query string, args ...any) {
	t.Helper()
	if _, err := sqlDB.ExecContext(context.Background(), query, args...); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

func wantPgErrorCode(t *testing.T, err error, code string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected PostgreSQL error %s, got nil", code)
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		t.Fatalf("expected *pgconn.PgError %s, got %T: %v", code, err, err)
	}
	if pgErr.Code != code {
		t.Fatalf("PostgreSQL error = %s %q, want %s", pgErr.Code, pgErr.Message, code)
	}
}

func relationExists(t *testing.T, sqlDB *sql.DB, name string) bool {
	t.Helper()
	var exists bool
	if err := sqlDB.QueryRowContext(context.Background(),
		"SELECT to_regclass($1) IS NOT NULL", name).Scan(&exists); err != nil {
		t.Fatalf("to_regclass(%s): %v", name, err)
	}
	return exists
}

func indexDef(t *testing.T, sqlDB *sql.DB, name string) string {
	t.Helper()
	var def string
	if err := sqlDB.QueryRowContext(context.Background(),
		"SELECT indexdef FROM pg_indexes WHERE indexname = $1", name).Scan(&def); err != nil {
		t.Fatalf("indexdef(%s): %v", name, err)
	}
	return def
}

func nonUniqueIndexCount(t *testing.T, sqlDB *sql.DB, table string) int {
	t.Helper()
	var n int
	if err := sqlDB.QueryRowContext(context.Background(), `
		SELECT count(*) FROM pg_index i
		JOIN pg_class c ON c.oid = i.indrelid
		WHERE c.relname = $1 AND NOT i.indisunique AND NOT i.indisprimary`, table).Scan(&n); err != nil {
		t.Fatalf("count indexes on %s: %v", table, err)
	}
	return n
}

// TestDepositMigrationEmptyDatabaseAndRepeat covers the T001 "empty database
// migration" and "repeat migration" conditions: full init reaches 004, the
// second run applies nothing, and all five tables plus the pause sequence
// exist afterwards.
func TestDepositMigrationEmptyDatabaseAndRepeat(t *testing.T) {
	dsn := startPostgres(t)
	ctx := context.Background()
	opts := testMigrateOptions(dsn)

	files, err := MigrationFiles(Migrations)
	if err != nil {
		t.Fatalf("list embedded migrations: %v", err)
	}
	target := files[len(files)-1].Version
	if target < 4 {
		t.Fatalf("embedded target version = %d, want >= 4 (000004 missing)", target)
	}

	var out bytes.Buffer
	if err := MigrateUp(ctx, opts, &out); err != nil {
		t.Fatalf("first MigrateUp() error = %v (output %q)", err, out.String())
	}
	if want := fmt.Sprintf("applied=%d skipped=0 pending=0", len(files)); !strings.Contains(out.String(), want) {
		t.Fatalf("first MigrateUp() output = %q, want %q", out.String(), want)
	}

	out.Reset()
	if err := MigrateStatus(ctx, opts, &out); err != nil {
		t.Fatalf("MigrateStatus() error = %v", err)
	}
	if want := fmt.Sprintf("current_version=%d", target); !strings.Contains(out.String(), want) ||
		!strings.Contains(out.String(), "pending=none") {
		t.Fatalf("MigrateStatus() output = %q, want %q and pending=none", out.String(), want)
	}
	if _, err := CheckCompatibility(ctx, opts); err != nil {
		t.Fatalf("CheckCompatibility() after migrate error = %v", err)
	}

	out.Reset()
	if err := MigrateUp(ctx, opts, &out); err != nil {
		t.Fatalf("second MigrateUp() error = %v (output %q)", err, out.String())
	}
	if want := fmt.Sprintf("applied=0 skipped=%d pending=0", len(files)); !strings.Contains(out.String(), want) {
		t.Fatalf("second MigrateUp() output = %q, want %q", out.String(), want)
	}

	sqlDB := openTestSQL(t, dsn)
	for _, rel := range []string{
		"deposit_config_history",
		"deposit_observations",
		"deposit_checkpoint",
		"deposit_pause",
		"deposit_pause_audit",
		"deposit_pause_id_seq",
	} {
		if !relationExists(t, sqlDB, rel) {
			t.Errorf("%s missing after full migration", rel)
		}
	}
}

// TestDepositMigrationUpgradeFrom003PreservesExistingData covers the T001
// "upgrade from a 003 database" condition: only 000004 applies, the 002/003
// rows are untouched (FR-09 zero disruption) and the new tables appear.
func TestDepositMigrationUpgradeFrom003PreservesExistingData(t *testing.T) {
	dsn := startPostgres(t)
	ctx := context.Background()

	at003 := migrateUpThrough(t, dsn, 3)
	sqlDB := openTestSQL(t, dsn)
	if relationExists(t, sqlDB, "deposit_config_history") {
		t.Fatal("deposit tables must not exist before 000004 is applied")
	}
	if !relationExists(t, sqlDB, "log_checkpoint") {
		t.Fatal("003 schema missing from the pre-upgrade database")
	}

	blockHash := hash64("ab")
	txHash := hash64("cd")
	configHash := strings.Repeat("a", 64)
	mustExec(t, sqlDB, `INSERT INTO chain_blocks (chain_id, number, hash, parent_hash, canonical)
		VALUES (1, 0, $1, $2, TRUE)`, blockHash, hash64("ee"))
	mustExec(t, sqlDB, `INSERT INTO indexer_checkpoint (chain_id, height, block_hash, start_height)
		VALUES (1, 0, $1, 0)`, blockHash)
	mustExec(t, sqlDB, `INSERT INTO erc20_transfer_logs
		(chain_id, block_number, block_hash, tx_hash, log_index, contract, topic0, topic1, topic2, data)
		VALUES (1, 0, $1, $2, 0, $3, $4, $5, $6, $7)`,
		blockHash, txHash, addr40("11"), hash64("00"), hash64("aa"), hash64("bb"), hash64("cc"))
	mustExec(t, sqlDB, `INSERT INTO log_checkpoint (chain_id, start_block, config_hash, next_block)
		VALUES (1, 0, $1, 1)`, configHash)

	files, err := MigrationFiles(Migrations)
	if err != nil {
		t.Fatalf("list embedded migrations: %v", err)
	}
	var out bytes.Buffer
	if err := MigrateUp(ctx, testMigrateOptions(dsn), &out); err != nil {
		t.Fatalf("upgrade MigrateUp() error = %v (output %q)", err, out.String())
	}
	if want := fmt.Sprintf("applied=%d skipped=%d pending=0", len(files)-at003, at003); !strings.Contains(out.String(), want) {
		t.Fatalf("upgrade MigrateUp() output = %q, want %q", out.String(), want)
	}
	if _, err := CheckCompatibility(ctx, testMigrateOptions(dsn)); err != nil {
		t.Fatalf("CheckCompatibility() after upgrade error = %v", err)
	}
	for _, rel := range []string{
		"deposit_config_history", "deposit_observations", "deposit_checkpoint",
		"deposit_pause", "deposit_pause_audit", "deposit_pause_id_seq",
	} {
		if !relationExists(t, sqlDB, rel) {
			t.Errorf("%s missing after upgrade", rel)
		}
	}

	// 002/003 rows survive the upgrade byte for byte.
	var blocks, logs, height int64
	var nextBlock, startHeight int64
	var storedHash, storedConfig string
	if err := sqlDB.QueryRowContext(ctx, `SELECT count(*) FROM chain_blocks WHERE chain_id = 1`).Scan(&blocks); err != nil {
		t.Fatalf("count chain_blocks: %v", err)
	}
	if err := sqlDB.QueryRowContext(ctx, `SELECT count(*) FROM erc20_transfer_logs WHERE chain_id = 1`).Scan(&logs); err != nil {
		t.Fatalf("count erc20_transfer_logs: %v", err)
	}
	if err := sqlDB.QueryRowContext(ctx, `SELECT height, block_hash FROM indexer_checkpoint WHERE chain_id = 1`).Scan(&height, &storedHash); err != nil {
		t.Fatalf("read indexer_checkpoint: %v", err)
	}
	if err := sqlDB.QueryRowContext(ctx, `SELECT start_height FROM indexer_checkpoint WHERE chain_id = 1`).Scan(&startHeight); err != nil {
		t.Fatalf("read indexer_checkpoint start: %v", err)
	}
	if err := sqlDB.QueryRowContext(ctx, `SELECT next_block, config_hash FROM log_checkpoint WHERE chain_id = 1`).Scan(&nextBlock, &storedConfig); err != nil {
		t.Fatalf("read log_checkpoint: %v", err)
	}
	if blocks != 1 || logs != 1 || height != 0 || startHeight != 0 || nextBlock != 1 {
		t.Fatalf("002/003 progress changed: blocks=%d logs=%d height=%d start=%d next=%d",
			blocks, logs, height, startHeight, nextBlock)
	}
	if storedHash != blockHash || strings.TrimSpace(storedConfig) != configHash {
		t.Fatalf("002/003 identity changed: hash=%q config=%q", storedHash, storedConfig)
	}
}

// TestDepositMigrationSchemaConstraints pins the storage-enforced pieces of
// data-model Tables 1-5: source-identity PK, amount>0, status=pending,
// lowercase config hashes, progress monotonicity, version identity and
// request_id uniqueness, the version FK, pause kind and audit action enums.
func TestDepositMigrationSchemaConstraints(t *testing.T) {
	dsn := startPostgres(t)
	ctx := context.Background()
	migrateUpAll(t, dsn)
	sqlDB := openTestSQL(t, dsn)

	blockHash := hash64("11")
	txHash := hash64("22")
	contract := addr40("33")
	sender := addr40("44")
	recipient := addr40("55")
	configA := strings.Repeat("a", 64)
	configB := strings.Repeat("b", 64)

	// Bootstrap version 1 (first version, no request identity) plus a valid
	// observation and two later versions to make duplicate cases meaningful.
	mustExec(t, sqlDB, `INSERT INTO deposit_config_history
		(chain_id, version_seq, config_hash, start_block, assets, watches, replay_from, operator)
		VALUES (1, 1, $1, 0, 'asset:a:0', 'watch:b:0', 0, 'bootstrap')`, configA)
	mustExec(t, sqlDB, `INSERT INTO deposit_observations
		(chain_id, block_hash, tx_hash, log_index, block_number, contract, sender, recipient, amount, version_seq)
		VALUES (1, $1, $2, 0, 7, $3, $4, $5, 100, 1)`, blockHash, txHash, contract, sender, recipient)
	mustExec(t, sqlDB, `INSERT INTO deposit_config_history
		(chain_id, version_seq, config_hash, prev_seq, start_block, assets, watches, replay_from, request_id)
		VALUES (1, 2, $1, 1, 0, 'a', 'w', 0, 'req-A')`, configB)
	mustExec(t, sqlDB, `INSERT INTO deposit_config_history
		(chain_id, version_seq, config_hash, prev_seq, start_block, assets, watches, replay_from, request_id)
		VALUES (1, 3, $1, 2, 0, 'a', 'w', 0, 'req-B')`, configA)

	const insertObservation = `INSERT INTO deposit_observations
		(chain_id, block_hash, tx_hash, log_index, block_number, contract, sender, recipient, amount, status, version_seq)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`
	obs := func(chainID int64, bh, th string, logIndex, blockNumber int64, amount, status string, versionSeq int64) []any {
		return []any{chainID, bh, th, logIndex, blockNumber, contract, sender, recipient, amount, status, versionSeq}
	}
	const insertHistory = `INSERT INTO deposit_config_history
		(chain_id, version_seq, config_hash, prev_seq, start_block, assets, watches, replay_from, request_id,
		 expected_pause_id, expected_pause_revision)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`

	cases := []struct {
		name string
		sql  string
		args []any
		code string // PostgreSQL SQLSTATE: 23505 unique, 23514 check, 23503 foreign key
	}{
		{"observation duplicate source identity", insertObservation,
			obs(1, blockHash, txHash, 0, 7, "100", "pending", 1), "23505"},
		{"observation amount zero", insertObservation,
			obs(1, hash64("66"), hash64("77"), 0, 7, "0", "pending", 1), "23514"},
		{"observation amount negative", insertObservation,
			obs(1, hash64("66"), hash64("77"), 0, 7, "-1", "pending", 1), "23514"},
		{"observation status not pending", insertObservation,
			obs(1, hash64("66"), hash64("77"), 0, 7, "100", "confirmed", 1), "23514"},
		{"observation version seq missing", insertObservation,
			obs(1, hash64("66"), hash64("77"), 0, 7, "100", "pending", 99), "23503"},
		{"observation uppercase block hash", insertObservation,
			obs(1, "0x"+strings.Repeat("AB", 32), hash64("77"), 0, 7, "100", "pending", 1), "23514"},
		{"observation short contract", insertObservation,
			[]any{1, hash64("66"), hash64("77"), 0, 7, "0x" + strings.Repeat("a", 39), sender, recipient, "100", "pending", 1}, "23514"},
		{"observation chain id zero", insertObservation,
			obs(0, hash64("66"), hash64("77"), 0, 7, "100", "pending", 1), "23514"},
		{"observation negative log index", insertObservation,
			obs(1, hash64("66"), hash64("77"), -1, 7, "100", "pending", 1), "23514"},
		{"observation negative block number", insertObservation,
			obs(1, hash64("66"), hash64("77"), 0, -1, "100", "pending", 1), "23514"},
		{"checkpoint next before start", `INSERT INTO deposit_checkpoint
			(chain_id, start_block, config_hash, next_block) VALUES (1, 10, $1, 9)`, []any{configA}, "23514"},
		{"checkpoint uppercase config hash", `INSERT INTO deposit_checkpoint
			(chain_id, start_block, config_hash, next_block) VALUES (1, 0, $1, 0)`,
			[]any{strings.Repeat("A", 64)}, "23514"},
		{"checkpoint chain id zero", `INSERT INTO deposit_checkpoint
			(chain_id, start_block, config_hash, next_block) VALUES (0, 0, $1, 0)`, []any{configA}, "23514"},
		{"history duplicate version", insertHistory,
			[]any{1, 1, configA, nil, 0, "a", "w", 0, nil, nil, nil}, "23505"},
		{"history duplicate request id", insertHistory,
			[]any{1, 4, configB, 3, 0, "a", "w", 0, "req-A", nil, nil}, "23505"},
		{"history second bootstrap row", insertHistory,
			[]any{1, 5, configB, 3, 0, "a", "w", 0, nil, nil, nil}, "23505"},
		{"history dangling prev_seq", insertHistory,
			[]any{1, 6, configB, 999, 0, "a", "w", 0, "req-Z", nil, nil}, "23503"},
		{"history expected pause id without revision", insertHistory,
			[]any{1, 7, configB, 3, 0, "a", "w", 0, "req-C", 7, nil}, "23514"},
		{"history expected pause revision without id", insertHistory,
			[]any{1, 8, configB, 3, 0, "a", "w", 0, "req-D", nil, 1}, "23514"},
		{"history version seq zero", insertHistory,
			[]any{1, 0, configB, nil, 0, "a", "w", 0, nil, nil, nil}, "23514"},
		{"history uppercase config hash", insertHistory,
			[]any{1, 9, strings.Repeat("A", 64), 3, 0, "a", "w", 0, "req-E", nil, nil}, "23514"},
		{"pause kind not in enum", `INSERT INTO deposit_pause (chain_id, height, kind)
			VALUES (1, 10, 'structural')`, nil, "23514"},
		{"audit action not in enum", `INSERT INTO deposit_pause_audit
			(chain_id, pause_id, revision, action, version_seq, kind, height)
			VALUES (1, 1, 1, 'delete', 1, 'upstream_gap', 10)`, nil, "23514"},
		{"audit revision zero", `INSERT INTO deposit_pause_audit
			(chain_id, pause_id, revision, action, version_seq, kind, height)
			VALUES (1, 1, 0, 'release', 1, 'upstream_gap', 10)`, nil, "23514"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := sqlDB.ExecContext(ctx, tc.sql, tc.args...)
			wantPgErrorCode(t, err, tc.code)
		})
	}

	// Positive counterparts: the constraints above must not over-reject.
	positives := []struct {
		name string
		sql  string
		args []any
	}{
		{"observation distinct source identity", insertObservation,
			obs(1, hash64("66"), hash64("77"), 0, 7, "100", "pending", 1)},
		{"observation amount one", insertObservation,
			obs(1, hash64("98"), hash64("99"), 0, 8, "1", "pending", 1)},
		{"checkpoint next equal start", `INSERT INTO deposit_checkpoint
			(chain_id, start_block, config_hash, next_block) VALUES (1, 5, $1, 5)`, []any{configA}},
		{"history both expected pause columns", insertHistory,
			[]any{1, 10, configB, 3, 0, "a", "w", 0, "req-F", 7, 1}},
		{"history same request id on another chain", insertHistory,
			[]any{2, 1, configB, nil, 0, "a", "w", 0, "req-A", nil, nil}},
	}
	for _, tc := range positives {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := sqlDB.ExecContext(ctx, tc.sql, tc.args...); err != nil {
				t.Fatalf("expected insert to succeed: %v", err)
			}
		})
	}
}

// TestDepositPauseSequenceAndAuditRetention pins the pause instance identity
// rules: pause_id comes from a never-reused sequence, revision defaults to 1,
// and audit rows survive the active row (no FK) with one event per
// (chain_id, pause_id, revision, action).
func TestDepositPauseSequenceAndAuditRetention(t *testing.T) {
	dsn := startPostgres(t)
	ctx := context.Background()
	migrateUpAll(t, dsn)
	sqlDB := openTestSQL(t, dsn)

	var firstPauseID, firstRevision int64
	if err := sqlDB.QueryRowContext(ctx, `INSERT INTO deposit_pause (chain_id, height, kind, detail)
		VALUES (1, 42, 'validation_failed', 'class=bad_amount')
		RETURNING pause_id, revision`).Scan(&firstPauseID, &firstRevision); err != nil {
		t.Fatalf("insert first pause: %v", err)
	}
	if firstPauseID <= 0 || firstRevision != 1 {
		t.Fatalf("first pause identity = (%d, revision %d), want id > 0 and revision 1", firstPauseID, firstRevision)
	}

	mustExec(t, sqlDB, `INSERT INTO deposit_pause_audit
		(chain_id, pause_id, revision, action, operator, reason, version_seq, kind, height, detail)
		VALUES (1, $1, $2, 'release', 'ops', 'fixed', 1, 'validation_failed', 42, 'class=bad_amount')`,
		firstPauseID, firstRevision)
	mustExec(t, sqlDB, `DELETE FROM deposit_pause WHERE chain_id = 1 AND pause_id = $1 AND revision = $2`,
		firstPauseID, firstRevision)

	var auditRows int
	if err := sqlDB.QueryRowContext(ctx, `SELECT count(*) FROM deposit_pause_audit
		WHERE chain_id = 1 AND pause_id = $1`, firstPauseID).Scan(&auditRows); err != nil {
		t.Fatalf("count pause audit: %v", err)
	}
	if auditRows != 1 {
		t.Fatalf("audit rows after release = %d, want 1 (audit must outlive the pause row)", auditRows)
	}

	// Same instance/revision/action is recorded once.
	_, err := sqlDB.ExecContext(ctx, `INSERT INTO deposit_pause_audit
		(chain_id, pause_id, revision, action, version_seq, kind, height)
		VALUES (1, $1, $2, 'release', 1, 'validation_failed', 42)`, firstPauseID, firstRevision)
	wantPgErrorCode(t, err, "23505")

	// A different action for the same instance/revision is a distinct event.
	mustExec(t, sqlDB, `INSERT INTO deposit_pause_audit
		(chain_id, pause_id, revision, action, version_seq, kind, height)
		VALUES (1, $1, $2, 'merge', 1, 'validation_failed', 42)`, firstPauseID, firstRevision)

	// The next pause instance never reuses the released identity.
	var secondPauseID int64
	if err := sqlDB.QueryRowContext(ctx, `INSERT INTO deposit_pause (chain_id, height, kind)
		VALUES (1, 43, 'upstream_gap')
		RETURNING pause_id`).Scan(&secondPauseID); err != nil {
		t.Fatalf("insert second pause: %v", err)
	}
	if secondPauseID <= firstPauseID {
		t.Fatalf("pause_id reused: first=%d second=%d", firstPauseID, secondPauseID)
	}
}

// TestDepositMigrationIndexes pins the two required observations indexes and
// asserts history/audit/checkpoint/pause carry no secondary index beyond the
// constraint-backed ones.
func TestDepositMigrationIndexes(t *testing.T) {
	dsn := startPostgres(t)
	ctx := context.Background()
	migrateUpAll(t, dsn)
	sqlDB := openTestSQL(t, dsn)

	height := indexDef(t, sqlDB, "deposit_observations_height_idx")
	if !strings.Contains(height, "(chain_id, block_number)") {
		t.Errorf("deposit_observations_height_idx = %q, want (chain_id, block_number)", height)
	}
	recipient := indexDef(t, sqlDB, "deposit_observations_recipient_height_idx")
	if !strings.Contains(recipient, "(chain_id, recipient, block_number)") {
		t.Errorf("deposit_observations_recipient_height_idx = %q, want (chain_id, recipient, block_number)", recipient)
	}
	if got := nonUniqueIndexCount(t, sqlDB, "deposit_observations"); got != 2 {
		t.Errorf("deposit_observations secondary indexes = %d, want 2", got)
	}
	for _, table := range []string{
		"deposit_config_history", "deposit_checkpoint", "deposit_pause", "deposit_pause_audit",
	} {
		if got := nonUniqueIndexCount(t, sqlDB, table); got != 0 {
			t.Errorf("%s has %d secondary index(es), want 0", table, got)
		}
	}

	// The bootstrap-row partial unique is constraint-backed (unique), not a
	// secondary lookup index, but it must exist.
	if def := indexDef(t, sqlDB, "deposit_config_history_bootstrap_uniq"); !strings.Contains(def, "WHERE") {
		t.Errorf("bootstrap unique index = %q, want a partial (WHERE request_id IS NULL) index", def)
	}
	var uniqueIdx int
	if err := sqlDB.QueryRowContext(ctx, `SELECT count(*) FROM pg_index i
		JOIN pg_class c ON c.oid = i.indrelid
		WHERE c.relname = 'deposit_config_history' AND i.indisunique AND NOT i.indisprimary`).Scan(&uniqueIdx); err != nil {
		t.Fatalf("count history unique indexes: %v", err)
	}
	if uniqueIdx != 2 {
		t.Fatalf("deposit_config_history unique indexes = %d, want 2 (request_id UNIQUE + bootstrap partial)", uniqueIdx)
	}
}

// TestDepositMigrationDowngradeFrom004RemovesOnly004 covers the T001 DOWN
// condition: rolling back 000004 drops the five tables and the sequence while
// 002/003 schema and rows stay intact, and re-applying works.
func TestDepositMigrationDowngradeFrom004RemovesOnly004(t *testing.T) {
	dsn := startPostgres(t)
	ctx := context.Background()
	migrateUpAll(t, dsn)

	sqlDB := openTestSQL(t, dsn)
	blockHash := hash64("ab")
	configHash := strings.Repeat("a", 64)
	mustExec(t, sqlDB, `INSERT INTO chain_blocks (chain_id, number, hash, parent_hash, canonical)
		VALUES (1, 0, $1, $2, TRUE)`, blockHash, hash64("ee"))
	mustExec(t, sqlDB, `INSERT INTO log_checkpoint (chain_id, start_block, config_hash, next_block)
		VALUES (1, 0, $1, 1)`, configHash)
	// A 004 row must disappear together with its table.
	mustExec(t, sqlDB, `INSERT INTO deposit_config_history
		(chain_id, version_seq, config_hash, start_block, assets, watches, replay_from, operator)
		VALUES (1, 1, $1, 0, 'a', 'w', 0, 'bootstrap')`, configHash)

	opts := testMigrateOptions(dsn)
	provider, err := newProvider(sqlDB, opts)
	if err != nil {
		t.Fatalf("newProvider: %v", err)
	}
	results, err := provider.DownTo(ctx, 3)
	if err != nil {
		t.Fatalf("DownTo(3): %v", err)
	}
	if len(results) != 1 || results[0].Source.Version != 4 {
		t.Fatalf("DownTo(3) rolled back %d migration(s), want exactly version 4", len(results))
	}

	for _, rel := range []string{
		"deposit_config_history", "deposit_observations", "deposit_checkpoint",
		"deposit_pause", "deposit_pause_audit", "deposit_pause_id_seq",
	} {
		if relationExists(t, sqlDB, rel) {
			t.Errorf("%s still exists after DOWN of 000004", rel)
		}
	}

	var blocks, nextBlock int64
	if err := sqlDB.QueryRowContext(ctx, `SELECT count(*) FROM chain_blocks WHERE chain_id = 1`).Scan(&blocks); err != nil {
		t.Fatalf("count chain_blocks after down: %v", err)
	}
	if err := sqlDB.QueryRowContext(ctx, `SELECT next_block FROM log_checkpoint WHERE chain_id = 1`).Scan(&nextBlock); err != nil {
		t.Fatalf("read log_checkpoint after down: %v", err)
	}
	if blocks != 1 || nextBlock != 1 {
		t.Fatalf("002/003 data changed by DOWN: blocks=%d next=%d", blocks, nextBlock)
	}

	var out bytes.Buffer
	if err := MigrateStatus(ctx, opts, &out); err != nil {
		t.Fatalf("MigrateStatus() after down error = %v", err)
	}
	if !strings.Contains(out.String(), "current_version=3") || !strings.Contains(out.String(), "pending=1") {
		t.Fatalf("status after down = %q, want current_version=3 and pending=1", out.String())
	}

	// Re-applying the migration behaves like the 003 upgrade again.
	files, err := MigrationFiles(Migrations)
	if err != nil {
		t.Fatalf("list embedded migrations: %v", err)
	}
	out.Reset()
	if err := MigrateUp(ctx, opts, &out); err != nil {
		t.Fatalf("re-up MigrateUp() error = %v (output %q)", err, out.String())
	}
	if want := fmt.Sprintf("applied=%d skipped=3 pending=0", len(files)-3); !strings.Contains(out.String(), want) {
		t.Fatalf("re-up MigrateUp() output = %q, want %q", out.String(), want)
	}
	if !relationExists(t, sqlDB, "deposit_config_history") {
		t.Fatal("deposit_config_history missing after re-up")
	}
}
