//go:build integration

package db

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/pressly/goose/v3"
)

// tableDef captures the column and constraint definitions of one table: the
// diff evidence for the T001 "002/003/004 table definitions zero change"
// condition.
type tableDef struct {
	columns     string
	constraints map[string]string
}

func snapshotTableDef(t *testing.T, sqlDB *sql.DB, table string) tableDef {
	t.Helper()
	ctx := context.Background()
	def := tableDef{constraints: map[string]string{}}

	rows, err := sqlDB.QueryContext(ctx, `
		SELECT column_name || '|' || data_type || '|' || COALESCE(character_maximum_length::text, '')
			|| '|' || COALESCE(numeric_precision::text, '') || '|' || COALESCE(numeric_scale::text, '')
			|| '|' || CASE WHEN is_nullable = 'YES' THEN 'null' ELSE 'notnull' END
			|| '|' || COALESCE(column_default, '')
		FROM information_schema.columns WHERE table_name = $1 ORDER BY ordinal_position`, table)
	if err != nil {
		t.Fatalf("snapshot columns of %s: %v", table, err)
	}
	defer rows.Close()
	var sb strings.Builder
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatalf("scan column of %s: %v", table, err)
		}
		sb.WriteString(line + "\n")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read columns of %s: %v", table, err)
	}
	def.columns = sb.String()

	crows, err := sqlDB.QueryContext(ctx, `
		SELECT conname, pg_get_constraintdef(oid) FROM pg_constraint
		WHERE conrelid = to_regclass($1) ORDER BY conname`, table)
	if err != nil {
		t.Fatalf("snapshot constraints of %s: %v", table, err)
	}
	defer crows.Close()
	for crows.Next() {
		var name, body string
		if err := crows.Scan(&name, &body); err != nil {
			t.Fatalf("scan constraint of %s: %v", table, err)
		}
		def.constraints[name] = body
	}
	if err := crows.Err(); err != nil {
		t.Fatalf("read constraints of %s: %v", table, err)
	}
	return def
}

// frozenTables are the 002/003 tables plus the 004 tables that 005 must not
// touch at all. deposit_observations is intentionally additive and is
// compared separately.
var confirmationFrozenTables = []string{
	"chain_blocks", "indexer_checkpoint", "indexer_lease", "indexer_pause",
	"erc20_transfer_logs", "log_checkpoint", "log_pause",
	"deposit_config_history", "deposit_checkpoint", "deposit_pause", "deposit_pause_audit",
}

func snapshotFrozen(t *testing.T, sqlDB *sql.DB) map[string]tableDef {
	t.Helper()
	out := make(map[string]tableDef, len(confirmationFrozenTables)+1)
	for _, tbl := range confirmationFrozenTables {
		out[tbl] = snapshotTableDef(t, sqlDB, tbl)
	}
	out["deposit_observations"] = snapshotTableDef(t, sqlDB, "deposit_observations")
	return out
}

// TestConfirmationMigrationEmptyDatabaseAndRepeat covers the T001
// "empty database migration" and "repeat migration" conditions: full init
// reaches 005, the second run applies nothing, and the new table, columns,
// and pending index exist afterwards.
func TestConfirmationMigrationEmptyDatabaseAndRepeat(t *testing.T) {
	dsn := startPostgres(t)
	ctx := context.Background()
	opts := testMigrateOptions(dsn)

	files, err := MigrationFiles(Migrations)
	if err != nil {
		t.Fatalf("list embedded migrations: %v", err)
	}
	target := files[len(files)-1].Version
	if target < 5 {
		t.Fatalf("embedded target version = %d, want >= 5 (000005 missing)", target)
	}

	var out bytes.Buffer
	if err := MigrateUp(ctx, opts, &out); err != nil {
		t.Fatalf("first MigrateUp() error = %v (output %q)", err, out.String())
	}
	if want := fmt.Sprintf("applied=%d skipped=0 pending=0", len(files)); !strings.Contains(out.String(), want) {
		t.Fatalf("first MigrateUp() output = %q, want %q", out.String(), want)
	}

	out.Reset()
	if err := MigrateUp(ctx, opts, &out); err != nil {
		t.Fatalf("second MigrateUp() error = %v (output %q)", err, out.String())
	}
	if want := fmt.Sprintf("applied=0 skipped=%d pending=0", len(files)); !strings.Contains(out.String(), want) {
		t.Fatalf("second MigrateUp() output = %q, want %q", out.String(), want)
	}
	if _, err := CheckCompatibility(ctx, opts); err != nil {
		t.Fatalf("CheckCompatibility() after migrate error = %v", err)
	}

	sqlDB := openTestSQL(t, dsn)
	if !relationExists(t, sqlDB, "confirmation_policy_history") {
		t.Error("confirmation_policy_history missing after full migration")
	}
	for _, col := range []string{
		"confirmed_at", "confirm_tip_number", "confirm_tip_hash",
		"confirm_threshold", "confirmations", "confirm_policy_seq",
	} {
		var n int
		if err := sqlDB.QueryRowContext(ctx, `
			SELECT count(*) FROM information_schema.columns
			WHERE table_name = 'deposit_observations' AND column_name = $1`, col).Scan(&n); err != nil {
			t.Fatalf("column lookup %s: %v", col, err)
		}
		if n != 1 {
			t.Errorf("deposit_observations.%s missing after full migration", col)
		}
	}
	pending := indexDef(t, sqlDB, "deposit_observations_pending_height_idx")
	if !strings.Contains(pending, "(chain_id, block_number)") || !strings.Contains(pending, "WHERE") {
		t.Errorf("deposit_observations_pending_height_idx = %q, want (chain_id, block_number) partial index", pending)
	}
	if def := indexDef(t, sqlDB, "confirmation_policy_history_bootstrap_uniq"); !strings.Contains(def, "WHERE") {
		t.Errorf("confirmation_policy_history_bootstrap_uniq = %q, want a partial (WHERE request_id IS NULL) index", def)
	}
}

// TestConfirmationMigrationUpgradeFrom004PreservesExistingData covers the
// T001 "upgrade from a 004 database" condition in two legs: 004 -> 000005
// (only 000005 applies; the 002/003/004 table definitions are byte-identical
// except for the additive 005 changes on deposit_observations), then
// 000005 -> 000006 (the approved reorg-recovery deltas land: fork-identity
// PK, canonical partial-unique, 3-state status + orphan-evidence columns,
// four new tables) while 005 rows/columns stay intact in meaning.
func TestConfirmationMigrationUpgradeFrom004PreservesExistingData(t *testing.T) {
	dsn := startPostgres(t)
	ctx := context.Background()

	migrateUpThrough(t, dsn, 4)
	sqlDB := openTestSQL(t, dsn)
	if relationExists(t, sqlDB, "confirmation_policy_history") {
		t.Fatal("confirmation_policy_history must not exist before 000005 is applied")
	}

	configHash := strings.Repeat("a", 64)
	blockHash := hash64("ab")
	txHash := hash64("cd")
	mustExec(t, sqlDB, `INSERT INTO chain_blocks (chain_id, number, hash, parent_hash, canonical)
		VALUES (1, 0, $1, $2, TRUE)`, blockHash, hash64("ee"))
	mustExec(t, sqlDB, `INSERT INTO indexer_checkpoint (chain_id, height, block_hash, start_height)
		VALUES (1, 0, $1, 0)`, blockHash)
	mustExec(t, sqlDB, `INSERT INTO deposit_config_history
		(chain_id, version_seq, config_hash, start_block, assets, watches, replay_from, operator)
		VALUES (1, 1, $1, 0, 'a', 'w', 0, 'bootstrap')`, configHash)
	mustExec(t, sqlDB, `INSERT INTO deposit_observations
		(chain_id, block_hash, tx_hash, log_index, block_number, contract, sender, recipient, amount, version_seq)
		VALUES (1, $1, $2, 0, 7, $3, $4, $5, 100, 1)`,
		blockHash, txHash, addr40("11"), addr40("22"), addr40("33"))

	before := snapshotFrozen(t, sqlDB)

	// Leg 1: 004 -> 000005. Only 000005 may apply here (the DB already sits
	// at 4, so exactly one file applies).
	leg1Opts := testMigrateOptions(dsn)
	leg1Opts.FS = migrationSubsetFS(t, 5)
	var leg1Out bytes.Buffer
	if err := MigrateUp(ctx, leg1Opts, &leg1Out); err != nil {
		t.Fatalf("leg-1 MigrateUp() error = %v (output %q)", err, leg1Out.String())
	}
	if want := "applied=1 skipped=4 pending=0"; !strings.Contains(leg1Out.String(), want) {
		t.Fatalf("leg-1 MigrateUp() output = %q, want %q", leg1Out.String(), want)
	}

	mid := snapshotFrozen(t, sqlDB)
	for _, tbl := range confirmationFrozenTables {
		if before[tbl].columns != mid[tbl].columns {
			t.Errorf("%s columns changed by 000005:\nbefore:\n%smid:\n%s", tbl, before[tbl].columns, mid[tbl].columns)
		}
		if len(before[tbl].constraints) != len(mid[tbl].constraints) {
			t.Errorf("%s constraint count changed by 000005: before=%v mid=%v",
				tbl, before[tbl].constraints, mid[tbl].constraints)
		}
		for name, body := range before[tbl].constraints {
			if mid[tbl].constraints[name] != body {
				t.Errorf("%s constraint %s changed by 000005: before=%q mid=%q",
					tbl, name, body, mid[tbl].constraints[name])
			}
		}
	}

	// deposit_observations: every pre-existing column and every pre-existing
	// constraint except the widened status set must be untouched by 000005.
	bObs, aObs := before["deposit_observations"], mid["deposit_observations"]
	for _, line := range strings.Split(strings.TrimSpace(bObs.columns), "\n") {
		if !strings.Contains(aObs.columns, line) {
			t.Errorf("deposit_observations lost pre-existing column def %q", line)
		}
	}
	for name, body := range bObs.constraints {
		if name == "deposit_observations_status_check" {
			continue
		}
		if aObs.constraints[name] != body {
			t.Errorf("deposit_observations constraint %s changed by 000005: before=%q after=%q",
				name, body, aObs.constraints[name])
		}
	}
	if def := aObs.constraints["deposit_observations_status_check"]; !strings.Contains(def, "confirmed") {
		t.Errorf("widened status check = %q, want IN ('pending','confirmed')", def)
	}
	if def, ok := aObs.constraints["deposit_observations_confirmation_consistency"]; !ok || !strings.Contains(def, "confirmed_at") {
		t.Errorf("confirmation consistency constraint missing or wrong: %v %q", ok, def)
	}

	// The pre-existing pending row keeps its identity with NULL basis columns.
	var status string
	var confirmedAt sql.NullTime
	if err := sqlDB.QueryRowContext(ctx, `SELECT status, confirmed_at FROM deposit_observations
		WHERE chain_id = 1 AND block_hash = $1`, blockHash).Scan(&status, &confirmedAt); err != nil {
		t.Fatalf("read pre-upgrade observation: %v", err)
	}
	if status != "pending" || confirmedAt.Valid {
		t.Fatalf("pre-upgrade observation changed: status=%q confirmed_at=%v", status, confirmedAt)
	}

	// Leg 2: 000005 -> 000006. Exactly one migration applies; the approved
	// reorg-recovery deltas land while 005 rows/columns stay intact.
	files, err := MigrationFiles(Migrations)
	if err != nil {
		t.Fatalf("list embedded migrations: %v", err)
	}
	var out bytes.Buffer
	if err := MigrateUp(ctx, testMigrateOptions(dsn), &out); err != nil {
		t.Fatalf("leg-2 MigrateUp() error = %v (output %q)", err, out.String())
	}
	if want := fmt.Sprintf("applied=1 skipped=%d pending=0", len(files)-1); !strings.Contains(out.String(), want) {
		t.Fatalf("leg-2 MigrateUp() output = %q, want %q", out.String(), want)
	}
	if _, err := CheckCompatibility(ctx, testMigrateOptions(dsn)); err != nil {
		t.Fatalf("CheckCompatibility() after 000006 error = %v", err)
	}

	after := snapshotFrozen(t, sqlDB)
	pkey6 := after["chain_blocks"].constraints["chain_blocks_pkey"]
	if pkey6 == before["chain_blocks"].constraints["chain_blocks_pkey"] ||
		!strings.Contains(pkey6, "chain_id") || !strings.Contains(pkey6, "number") ||
		!strings.Contains(pkey6, "hash") {
		t.Errorf("chain_blocks_pkey after 000006 = %q, want the fork-identity (chain_id, number, hash) key", pkey6)
	}
	for _, tbl := range confirmationFrozenTables {
		if tbl == "chain_blocks" {
			continue
		}
		if mid[tbl].columns != after[tbl].columns {
			t.Errorf("%s columns changed by 000006:\nmid:\n%safter:\n%s", tbl, mid[tbl].columns, after[tbl].columns)
		}
		if len(mid[tbl].constraints) != len(after[tbl].constraints) {
			t.Errorf("%s constraint count changed by 000006: mid=%v after=%v",
				tbl, mid[tbl].constraints, after[tbl].constraints)
		}
	}
	mObs, aObs6 := mid["deposit_observations"], after["deposit_observations"]
	for _, line := range strings.Split(strings.TrimSpace(mObs.columns), "\n") {
		if !strings.Contains(aObs6.columns, line) {
			t.Errorf("deposit_observations lost 005 column def %q under 000006", line)
		}
	}
	for _, col := range []string{"orphaned_at|", "orphan_recovery_id|", "orphan_reason|"} {
		if !strings.Contains(aObs6.columns, col) {
			t.Errorf("deposit_observations missing 000006 orphan-evidence column %q", col)
		}
	}
	if def := aObs6.constraints["deposit_observations_status_check"]; !strings.Contains(def, "orphaned") {
		t.Errorf("3-state status check = %q, want IN ('pending','confirmed','orphaned')", def)
	}
	if def := aObs6.constraints["deposit_observations_confirmation_consistency"]; !strings.Contains(def, "orphaned") {
		t.Errorf("3-state consistency check = %q, want the orphaned branch", def)
	}
	for _, tbl := range []string{
		"reorg_policy_history", "reorg_recovery", "reorg_recovery_events", "deposit_observation_transitions",
	} {
		if !relationExists(t, sqlDB, tbl) {
			t.Errorf("%s missing after 000006", tbl)
		}
	}
	if err := sqlDB.QueryRowContext(ctx, `SELECT status, confirmed_at FROM deposit_observations
		WHERE chain_id = 1 AND block_hash = $1`, blockHash).Scan(&status, &confirmedAt); err != nil {
		t.Fatalf("read pre-upgrade observation after 000006: %v", err)
	}
	if status != "pending" || confirmedAt.Valid {
		t.Fatalf("pre-upgrade observation changed by 000006: status=%q confirmed_at=%v", status, confirmedAt)
	}
}

// seedConfirmationParents inserts the 004 config version plus the 005 policy
// version that confirmed-observation fixtures reference.
func seedConfirmationParents(t *testing.T, sqlDB *sql.DB, chainID int64, configHash string, threshold int64) {
	t.Helper()
	mustExec(t, sqlDB, `INSERT INTO deposit_config_history
		(chain_id, version_seq, config_hash, start_block, assets, watches, replay_from, operator)
		VALUES ($1, 1, $2, 0, 'a', 'w', 0, 'bootstrap')
		ON CONFLICT DO NOTHING`, chainID, configHash)
	mustExec(t, sqlDB, `INSERT INTO confirmation_policy_history
		(chain_id, policy_seq, threshold, operator)
		VALUES ($1, 1, $2, 'bootstrap')
		ON CONFLICT DO NOTHING`, chainID, threshold)
}

// TestConfirmationMigrationSchemaConstraints pins every T001 constraint
// individually: policy PK/threshold/request-uniqueness/bootstrap-singleton/
// prev-seq FK, observation basis ranges and formats, NUMERIC integer and
// non-negative checks (decimals and negatives rejected), the policy FK, the
// widened status set, and the pending/confirmed consistency rule.
func TestConfirmationMigrationSchemaConstraints(t *testing.T) {
	dsn := startPostgres(t)
	ctx := context.Background()
	migrateUpAll(t, dsn)
	sqlDB := openTestSQL(t, dsn)

	configHash := strings.Repeat("c", 64)
	seedConfirmationParents(t, sqlDB, 1, configHash, 10)

	const insertPolicy = `INSERT INTO confirmation_policy_history
		(chain_id, policy_seq, threshold, prev_seq, operator, reason, request_id, expected_old_seq)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`
	pol := func(chainID, seq, threshold int64, prev any, req any) []any {
		return []any{chainID, seq, threshold, prev, "ops", "why", req, 1}
	}

	contract := addr40("33")
	sender := addr40("44")
	recipient := addr40("55")
	const insertObservation = `INSERT INTO deposit_observations
		(chain_id, block_hash, tx_hash, log_index, block_number, contract, sender, recipient, amount,
		 status, version_seq, confirmed_at, confirm_tip_number, confirm_tip_hash,
		 confirm_threshold, confirmations, confirm_policy_seq)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)`
	obs := func(chainID int64, bh, th string, logIndex int64, status string, confirmedAt any,
		tipNum any, tipHash any, threshold, confirmations any, policySeq any) []any {
		return []any{chainID, bh, th, logIndex, 7, contract, sender, recipient, "100",
			status, 1, confirmedAt, tipNum, tipHash, threshold, confirmations, policySeq}
	}
	pending := func(chainID int64, bh, th string, logIndex int64) []any {
		return obs(chainID, bh, th, logIndex, "pending", nil, nil, nil, nil, nil, nil)
	}
	confirmed := func(chainID int64, bh, th string, logIndex int64, tipNum any, tipHash any,
		threshold, confirmations any, policySeq any) []any {
		return obs(chainID, bh, th, logIndex, "confirmed", "2026-09-14T00:00:00Z",
			tipNum, tipHash, threshold, confirmations, policySeq)
	}
	goodTip := hash64("aa")

	cases := []struct {
		name string
		sql  string
		args []any
		code string // PostgreSQL SQLSTATE: 23505 unique, 23514 check, 23503 foreign key
	}{
		{"policy threshold zero", insertPolicy, pol(1, 2, 0, 1, "req-A"), "23514"},
		{"policy threshold negative", insertPolicy, pol(1, 2, -3, 1, "req-A"), "23514"},
		{"policy chain id zero", insertPolicy, pol(0, 2, 10, nil, "req-A"), "23514"},
		{"policy seq zero", insertPolicy, pol(1, 0, 10, nil, "req-A"), "23514"},
		{"policy duplicate primary key", insertPolicy, pol(1, 1, 10, nil, "req-B"), "23505"},
		{"policy second bootstrap row", insertPolicy, pol(1, 9, 10, 1, nil), "23505"},
		{"policy dangling prev_seq", insertPolicy, pol(1, 9, 10, 999, "req-C"), "23503"},
		{"observation tip number negative",
			insertObservation, confirmed(1, hash64("01"), hash64("02"), 0, -1, goodTip, 10, "10", 1), "23514"},
		{"observation tip hash not hex",
			insertObservation, confirmed(1, hash64("01"), hash64("02"), 0, 12, "not-a-hash", 10, "10", 1), "23514"},
		{"observation tip hash short",
			insertObservation, confirmed(1, hash64("01"), hash64("02"), 0, 12, "0x"+strings.Repeat("a", 63), 10, "10", 1), "23514"},
		{"observation threshold zero",
			insertObservation, confirmed(1, hash64("01"), hash64("02"), 0, 12, goodTip, 0, "10", 1), "23514"},
		{"observation confirmations decimal",
			insertObservation, confirmed(1, hash64("01"), hash64("02"), 0, 12, goodTip, 10, "10.5", 1), "23514"},
		{"observation confirmations negative",
			insertObservation, confirmed(1, hash64("01"), hash64("02"), 0, 12, goodTip, 10, "-1", 1), "23514"},
		{"observation policy seq zero",
			insertObservation, confirmed(1, hash64("01"), hash64("02"), 0, 12, goodTip, 10, "10", 0), "23514"},
		{"observation policy seq missing",
			insertObservation, confirmed(1, hash64("01"), hash64("02"), 0, 12, goodTip, 10, "10", 99), "23503"},
		{"observation status outside pending/confirmed",
			insertObservation, obs(1, hash64("01"), hash64("02"), 0, "archived", nil, nil, nil, nil, nil, nil), "23514"},
		{"observation pending with confirmed_at set",
			insertObservation, obs(1, hash64("01"), hash64("02"), 0, "pending", "2026-09-14T00:00:00Z", nil, nil, nil, nil, nil), "23514"},
		{"observation confirmed missing confirmations",
			insertObservation, confirmed(1, hash64("01"), hash64("02"), 0, 12, goodTip, 10, nil, 1), "23514"},
		{"observation confirmed missing policy seq",
			insertObservation, confirmed(1, hash64("01"), hash64("02"), 0, 12, goodTip, 10, "10", nil), "23514"},
		{"observation confirmed missing tip hash",
			insertObservation, confirmed(1, hash64("01"), hash64("02"), 0, 12, nil, 10, "10", 1), "23514"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := sqlDB.ExecContext(ctx, tc.sql, tc.args...)
			wantPgErrorCode(t, err, tc.code)
		})
	}

	// Request uniqueness binds per chain: seed one request row, then assert
	// the duplicate and the cross-chain counterpart behave oppositely.
	mustExec(t, sqlDB, insertPolicy, pol(1, 2, 20, 1, "req-A")...)
	if _, err := sqlDB.ExecContext(ctx, insertPolicy, pol(1, 3, 30, 2, "req-A")...); err == nil {
		t.Fatal("duplicate (chain_id, request_id) accepted")
	} else {
		wantPgErrorCode(t, err, "23505")
	}

	positives := []struct {
		name string
		sql  string
		args []any
	}{
		{"policy same request id on another chain", insertPolicy, pol(2, 1, 10, nil, "req-A")},
		{"policy second version chained on prev", insertPolicy, pol(2, 2, 20, 1, "req-B")},
		{"observation pending plain", insertObservation, pending(1, hash64("01"), hash64("02"), 10)},
		{"observation confirmed full basis",
			insertObservation, confirmed(1, hash64("03"), hash64("04"), 0, 12, goodTip, 10, "10", 1)},
		{"observation confirmed zero confirmations",
			insertObservation, confirmed(1, hash64("05"), hash64("06"), 1, 0, goodTip, 1, "0", 1)},
	}
	for _, tc := range positives {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := sqlDB.ExecContext(ctx, tc.sql, tc.args...); err != nil {
				t.Fatalf("expected insert to succeed: %v", err)
			}
		})
	}
}

func versionsOf(results []*goose.MigrationResult) []int64 {
	out := make([]int64, 0, len(results))
	for _, r := range results {
		out = append(out, r.Source.Version)
	}
	return out
}

// TestConfirmationMigrationDowngradeFrom005RemovesOnly005 covers the Down
// section: rolling back to 4 removes 000006 (scratch-only shape: no fork
// history rows exist here, per the T007 outage boundary) then 000005 — the
// history table, the basis columns, the pending index, and the widened status
// set — while 002/003/004 schema and rows stay intact, and re-applying works.
func TestConfirmationMigrationDowngradeFrom005RemovesOnly005(t *testing.T) {
	dsn := startPostgres(t)
	ctx := context.Background()
	migrateUpAll(t, dsn)

	sqlDB := openTestSQL(t, dsn)
	configHash := strings.Repeat("d", 64)
	blockHash := hash64("ab")
	mustExec(t, sqlDB, `INSERT INTO chain_blocks (chain_id, number, hash, parent_hash, canonical)
		VALUES (1, 0, $1, $2, TRUE)`, blockHash, hash64("ee"))
	mustExec(t, sqlDB, `INSERT INTO log_checkpoint (chain_id, start_block, config_hash, next_block)
		VALUES (1, 0, $1, 1)`, configHash)
	seedConfirmationParents(t, sqlDB, 1, configHash, 10)
	mustExec(t, sqlDB, `INSERT INTO deposit_observations
		(chain_id, block_hash, tx_hash, log_index, block_number, contract, sender, recipient, amount, version_seq)
		VALUES (1, $1, $2, 0, 7, $3, $4, $5, 100, 1)`,
		blockHash, hash64("cd"), addr40("11"), addr40("22"), addr40("33"))

	opts := testMigrateOptions(dsn)
	provider, err := newProvider(sqlDB, opts)
	if err != nil {
		t.Fatalf("newProvider: %v", err)
	}
	results, err := provider.DownTo(ctx, 4)
	if err != nil {
		t.Fatalf("DownTo(4): %v", err)
	}
	if len(results) != 2 || results[0].Source.Version != 6 || results[1].Source.Version != 5 {
		t.Fatalf("DownTo(4) rolled back %v, want exactly versions [6 5] in order", versionsOf(results))
	}

	for _, rel := range []string{
		"reorg_policy_history", "reorg_recovery", "reorg_recovery_events", "deposit_observation_transitions",
		"confirmation_policy_history", "deposit_observations_pending_height_idx",
	} {
		if relationExists(t, sqlDB, rel) {
			t.Errorf("%s still exists after DOWN to 4", rel)
		}
	}
	var basisCols int
	if err := sqlDB.QueryRowContext(ctx, `
		SELECT count(*) FROM information_schema.columns
		WHERE table_name = 'deposit_observations' AND column_name LIKE 'confirm%'`).Scan(&basisCols); err != nil {
		t.Fatalf("count basis columns after down: %v", err)
	}
	if basisCols != 0 {
		t.Fatalf("deposit_observations still has %d confirm* columns after DOWN", basisCols)
	}
	// The narrowed status set rejects 'confirmed' again (004 shape restored).
	if _, err := sqlDB.ExecContext(ctx, `INSERT INTO deposit_observations
		(chain_id, block_hash, tx_hash, log_index, block_number, contract, sender, recipient, amount, status, version_seq)
		VALUES (1, $1, $2, 9, 7, $3, $4, $5, 100, 'confirmed', 1)`,
		hash64("91"), hash64("92"), addr40("11"), addr40("22"), addr40("33")); err == nil {
		t.Fatal("status='confirmed' accepted after DOWN; want the 004 pending-only check")
	} else {
		wantPgErrorCode(t, err, "23514")
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
	var obs int
	if err := sqlDB.QueryRowContext(ctx, `SELECT count(*) FROM deposit_observations WHERE chain_id = 1`).Scan(&obs); err != nil {
		t.Fatalf("count deposit_observations after down: %v", err)
	}
	if obs != 1 {
		t.Fatalf("004 observation lost by DOWN: rows=%d, want 1", obs)
	}

	var out bytes.Buffer
	if err := MigrateStatus(ctx, opts, &out); err != nil {
		t.Fatalf("MigrateStatus() after down error = %v", err)
	}
	if !strings.Contains(out.String(), "current_version=4") || !strings.Contains(out.String(), "pending=2") {
		t.Fatalf("status after down = %q, want current_version=4 and pending=2", out.String())
	}

	files, err := MigrationFiles(Migrations)
	if err != nil {
		t.Fatalf("list embedded migrations: %v", err)
	}
	out.Reset()
	if err := MigrateUp(ctx, opts, &out); err != nil {
		t.Fatalf("re-up MigrateUp() error = %v (output %q)", err, out.String())
	}
	if want := fmt.Sprintf("applied=%d skipped=4 pending=0", len(files)-4); !strings.Contains(out.String(), want) {
		t.Fatalf("re-up MigrateUp() output = %q, want %q", out.String(), want)
	}
	if !relationExists(t, sqlDB, "confirmation_policy_history") {
		t.Fatal("confirmation_policy_history missing after re-up")
	}
	if !relationExists(t, sqlDB, "reorg_recovery") {
		t.Fatal("reorg_recovery missing after re-up")
	}
}
