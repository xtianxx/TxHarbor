//go:build integration

// withdrawal_execution_migration_integration_test.go proves migration 000012
// on a real PostgreSQL (T008): up/down/up is clean, each required negative
// probe is refused by the exact named constraint (23505/23514 classified by
// ConstraintName only, never by message text), the migration is additive-only
// against 000001-000010, and the provisional 000012 number is re-verified
// against the actual embedded set (C12/V12.1/V12.2). Real container, no
// sleeps-as-proof.
package db

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

var execTables = []string{
	"payment_intents",
	"execution_claims",
	"execution_steps",
	"execution_events",
	"request_status_projection",
	"execution_caller_permission",
	"execution_ops_audit",
}

// wantPgConstraint asserts the error is the expected SQLSTATE AND that it was
// raised by the expected named constraint/index.
func wantPgConstraint(t *testing.T, err error, code, constraint string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected PostgreSQL error %s on %s, got nil", code, constraint)
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		t.Fatalf("expected *pgconn.PgError %s on %s, got %T: %v", code, constraint, err, err)
	}
	if pgErr.Code != code {
		t.Fatalf("PostgreSQL error = %s %q, want %s", pgErr.Code, pgErr.Message, code)
	}
	if pgErr.ConstraintName != constraint {
		t.Fatalf("constraint = %q, want %q (message %q)", pgErr.ConstraintName, constraint, pgErr.Message)
	}
}

// seedExecPrerequisites creates the 007/registry rows the 011 FK targets need.
func seedExecPrerequisites(t *testing.T, sqlDB *sql.DB) {
	t.Helper()
	mustExec(t, sqlDB, `INSERT INTO caller (caller_id, label) VALUES (1, 'ops')`)
	mustExec(t, sqlDB, `INSERT INTO withdrawal_requests
		(request_id, caller_id, idempotency_key, authorization_id, chain_id, asset, recipient, amount)
		VALUES ('req-1', 1, 'idem-1', 'authz-1', 1, $1, $2, 100)`, addr40("aa"), addr40("bb"))
	mustExec(t, sqlDB, `INSERT INTO withdrawal_requests
		(request_id, caller_id, idempotency_key, authorization_id, chain_id, asset, recipient, amount)
		VALUES ('req-2', 1, 'idem-2', 'authz-2', 1, $1, $2, 100)`, addr40("aa"), addr40("bb"))
}

func TestWithdrawalExecutionMigrationAppliesAndEnforcesConstraints(t *testing.T) {
	dsn := startPostgres(t)
	ctx := context.Background()
	migrateUpAll(t, dsn)
	sqlDB := openTestSQL(t, dsn)

	for _, rel := range execTables {
		if !relationExists(t, sqlDB, rel) {
			t.Fatalf("%s missing after full migration", rel)
		}
	}
	seedExecPrerequisites(t, sqlDB)

	// Positive counterparts: one intent, one claim, one open step, one event,
	// one projection, one permission row, one audit row.
	mustExec(t, sqlDB, `INSERT INTO payment_intents
		(intent_id, request_id, chain_id, sender, authorization_id, authorization_version,
		 state, admitted_recovery_version)
		VALUES ('intent-1', 'req-1', 1, $1, 'authz-1', 1, 'admitted', 0)`, addr40("cc"))
	mustExec(t, sqlDB, `INSERT INTO execution_claims
		(intent_id, owner_id, lease_version, state, expires_at)
		VALUES ('intent-1', '0011223344556677', 1, 'active', now() + interval '30 seconds')`)
	mustExec(t, sqlDB, `INSERT INTO execution_steps
		(step_id, intent_id, action, state, owner_id, lease_version, recovery_version)
		VALUES ('00112233445566778899aabbccddeeff', 'intent-1', 'first_broadcast', 'issued',
		        '0011223344556677', 1, 0)`)
	mustExec(t, sqlDB, `INSERT INTO execution_events (intent_id, kind) VALUES ('intent-1', 'admitted')`)
	mustExec(t, sqlDB, `INSERT INTO request_status_projection
		(request_id, intent_id, execution_state, state_version, freshness)
		VALUES ('req-1', 'intent-1', 'admitted', 1, 'confirmed')`)
	mustExec(t, sqlDB, `INSERT INTO execution_caller_permission (caller_id, can_execute)
		VALUES (1, TRUE)`)
	mustExec(t, sqlDB, `INSERT INTO execution_ops_audit
		(operation_id, action, outcome, operator)
		VALUES ('op-1', 'permission_set', 'applied', 'alice')`)

	const insertIntent = `INSERT INTO payment_intents
		(intent_id, request_id, chain_id, sender, authorization_id, authorization_version,
		 state, admitted_recovery_version)
		VALUES ($1,$2,1,$3,$4,1,'admitted',0)`

	cases := []struct {
		name       string
		sql        string
		args       []any
		code       string
		constraint string
	}{
		{"payment_intents_request_uniq", insertIntent,
			[]any{"intent-2", "req-1", addr40("cc"), "authz-9"}, "23505", "payment_intents_request_uniq"},
		{"payment_intents_authorization_uniq", insertIntent,
			[]any{"intent-3", "req-2", addr40("cc"), "authz-1"}, "23505", "payment_intents_authorization_uniq"},
		{"execution_claims_pkey",
			`INSERT INTO execution_claims (intent_id, owner_id, lease_version, state, expires_at)
			 VALUES ('intent-1', '8899aabbccddeeff', 2, 'active', now() + interval '30 seconds')`,
			nil, "23505", "execution_claims_pkey"},
		{"execution_claims_state_consistency",
			`UPDATE execution_claims SET state = 'released' WHERE intent_id = 'intent-1'`,
			nil, "23514", "execution_claims_state_consistency"},
		{"execution_steps_open_uniq",
			`INSERT INTO execution_steps (step_id, intent_id, action, state, owner_id, lease_version)
			 VALUES ('ffeeddccbbaa99887766554433221100', 'intent-1', 'replay', 'issued',
			         '0011223344556677', 1)`,
			nil, "23505", "execution_steps_open_uniq"},
		{"execution_ops_audit_operation_id_uniq",
			`INSERT INTO execution_ops_audit (operation_id, action, outcome, operator)
			 VALUES ('op-1', 'permission_revoke', 'applied', 'bob')`,
			nil, "23505", "execution_ops_audit_operation_id_uniq"},
		{"request_status_projection_stale_consistency",
			`UPDATE request_status_projection SET freshness = 'possibly_stale' WHERE request_id = 'req-1'`,
			nil, "23514", "request_status_projection_stale_consistency"},
		{"execution_events_kind_check",
			`INSERT INTO execution_events (intent_id, kind) VALUES ('intent-1', 'bogus_kind')`,
			nil, "23514", "execution_events_kind_check"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := sqlDB.ExecContext(ctx, tc.sql, tc.args...)
			wantPgConstraint(t, err, tc.code, tc.constraint)
		})
	}
}

func TestWithdrawalExecutionMigrationDownRemovesOnlyItself(t *testing.T) {
	dsn := startPostgres(t)
	ctx := context.Background()
	opts := testMigrateOptions(dsn)

	// Build the database at exactly {1..12}: 000012 is the highest version
	// this test owns. The full embedded set would also carry 010's 000013/
	// 000014, and DownTo(11) would revert those too, which is not 000012's
	// down-scope assertion.
	migrateUpThrough(t, dsn, 12)
	sqlDB := openTestSQL(t, dsn)
	for _, rel := range execTables {
		if !relationExists(t, sqlDB, rel) {
			t.Fatalf("%s missing after up", rel)
		}
	}

	provider, err := newProvider(sqlDB, opts)
	if err != nil {
		t.Fatalf("newProvider: %v", err)
	}
	// The fixture stops at 12, so DownTo(11) rolls back exactly 000012.
	results, err := provider.DownTo(ctx, 11)
	if err != nil {
		t.Fatalf("DownTo(11): %v", err)
	}
	if len(results) != 1 || results[0].Source.Version != 12 {
		t.Fatalf("DownTo(11) rolled back %d migration(s), want exactly version 12", len(results))
	}
	for _, rel := range execTables {
		if relationExists(t, sqlDB, rel) {
			t.Errorf("%s still exists after DOWN of 000012", rel)
		}
	}
	for _, rel := range []string{"withdrawal_requests", "withdrawal_authorizations", "caller", "nonce_bindings"} {
		if !relationExists(t, sqlDB, rel) {
			t.Errorf("%s must survive the 000012 DOWN", rel)
		}
	}

	var out bytes.Buffer
	if err := MigrateUp(ctx, opts, &out); err != nil {
		t.Fatalf("re-up MigrateUp() error = %v (output %q)", err, out.String())
	}
	for _, rel := range execTables {
		if !relationExists(t, sqlDB, rel) {
			t.Fatalf("%s missing after re-up", rel)
		}
	}
}

// execSchemaFingerprint renders the public base tables and their columns,
// excluding the 011 tables, so any upstream schema mutation shows up as a diff.
func execSchemaFingerprint(t *testing.T, sqlDB *sql.DB) (string, map[string]bool) {
	t.Helper()
	ctx := context.Background()
	rows, err := sqlDB.QueryContext(ctx, `SELECT table_name FROM information_schema.tables
		WHERE table_schema = 'public' AND table_type = 'BASE TABLE' ORDER BY table_name`)
	if err != nil {
		t.Fatalf("list tables: %v", err)
	}
	defer rows.Close()
	tables := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan table: %v", err)
		}
		tables[name] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate tables: %v", err)
	}

	skip := map[string]bool{}
	for _, rel := range execTables {
		skip[rel] = true
	}
	crows, err := sqlDB.QueryContext(ctx, `SELECT table_name, column_name, data_type, is_nullable
		FROM information_schema.columns WHERE table_schema = 'public'
		ORDER BY table_name, ordinal_position`)
	if err != nil {
		t.Fatalf("list columns: %v", err)
	}
	defer crows.Close()
	var b strings.Builder
	for crows.Next() {
		var table, column, dataType, nullable string
		if err := crows.Scan(&table, &column, &dataType, &nullable); err != nil {
			t.Fatalf("scan column: %v", err)
		}
		if skip[table] {
			continue
		}
		b.WriteString(table + "." + column + ":" + dataType + ":" + nullable + "\n")
	}
	if err := crows.Err(); err != nil {
		t.Fatalf("iterate columns: %v", err)
	}
	return b.String(), tables
}

// TestWithdrawalExecutionMigrationIsAdditiveOnly builds a real database at
// 000011, snapshots the upstream schema, applies exactly 000012 (subset FS),
// and asserts no upstream object changed while 000012's tables are the only
// new base tables. The full embedded set would also carry 010's 000013/000014
// follow-ups, whose tables are outside this test's additivity claim.
func TestWithdrawalExecutionMigrationIsAdditiveOnly(t *testing.T) {
	dsn := startPostgres(t)
	ctx := context.Background()
	migrateUpThrough(t, dsn, 11)
	sqlDB := openTestSQL(t, dsn)

	before, tablesBefore := execSchemaFingerprint(t, sqlDB)

	opts := testMigrateOptions(dsn)
	opts.FS = migrationSubsetFS(t, 12) // apply exactly 000012
	var out bytes.Buffer
	if err := MigrateUp(ctx, opts, &out); err != nil {
		t.Fatalf("MigrateUp() error = %v (output %q)", err, out.String())
	}
	after, tablesAfter := execSchemaFingerprint(t, sqlDB)

	if before != after {
		t.Errorf("000012 mutated the upstream schema:\n--- before ---\n%s\n--- after ---\n%s", before, after)
	}
	var added []string
	for name := range tablesAfter {
		if !tablesBefore[name] {
			added = append(added, name)
		}
	}
	want := map[string]bool{}
	for _, rel := range execTables {
		want[rel] = true
	}
	if len(added) != len(want) {
		t.Fatalf("000012 added %v base tables, want exactly %v", added, execTables)
	}
	for _, name := range added {
		if !want[name] {
			t.Errorf("000012 added unexpected base table %q", name)
		}
	}
}

// TestWithdrawalExecutionMigrationNumberIsProvisional verifies the merged
// version set against the actual embedded set on the joint branch: 000010 is
// PB, 000011 is 010, 000012 is 011, 000013 is 010's guarded intent-FK
// follow-up and 000014 is 010's intent-FK repair — no version duplicated and
// no gap inside 1..14 (C12).
//
// The lane-local "000011 must be absent" form was recorded stale at 3d8556e
// when the joint branch deliberately carries 010's 000011; the 000014 repair
// unit (fix(010), joint branch only) owns the post-repair set.
func TestWithdrawalExecutionMigrationNumberIsProvisional(t *testing.T) {
	files, err := MigrationFiles(Migrations)
	if err != nil {
		t.Fatalf("MigrationFiles: %v", err)
	}
	byVersion := map[int64]string{}
	for _, f := range files {
		if _, dup := byVersion[f.Version]; dup {
			t.Fatalf("duplicate migration version %d", f.Version)
		}
		byVersion[f.Version] = f.Name
	}
	want := map[int64]string{
		10: "000010_withdrawal_authorization_scopes.sql",
		11: "000011_tx_lifecycle.sql",
		12: "000012_withdrawal_execution.sql",
		13: "000013_tx_lifecycle_intent_fk.sql",
		14: "000014_intent_fk_repair.sql",
	}
	for version, name := range want {
		if got := byVersion[version]; got != name {
			t.Errorf("version %d = %q, want %q", version, got, name)
		}
	}
	if len(files) != len(want)+9 {
		t.Fatalf("embedded migration count = %d, want %d (versions 1..14)", len(files), len(want)+9)
	}
	for i, f := range files {
		if f.Version != int64(i+1) {
			t.Fatalf("embedded versions %v are not exactly 1..14", files)
		}
	}
}
