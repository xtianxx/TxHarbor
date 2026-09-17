//go:build integration

// authz_scope_migration_integration_test.go proves migration 000010 on a real
// PostgreSQL: the carrier applies and rolls back cleanly, and its PK/FK plus
// fee CHECK constraints are enforced by the server (23505/23503/23514), never
// merely declared in text. 007 rows and the rest of the chain are untouched by
// the Down leg. Real container, no sleeps-as-proof.
package db

import (
	"bytes"
	"context"
	"testing"
)

func TestAuthzScopeMigrationAppliesAndEnforcesConstraints(t *testing.T) {
	dsn := startPostgres(t)
	ctx := context.Background()
	migrateUpAll(t, dsn)
	sqlDB := openTestSQL(t, dsn)

	if !relationExists(t, sqlDB, "withdrawal_authorization_scopes") {
		t.Fatal("withdrawal_authorization_scopes missing after full migration")
	}

	// 007 FK targets: one caller, one active grant.
	mustExec(t, sqlDB, `INSERT INTO caller (caller_id, label) VALUES (1, 'ops')`)
	mustExec(t, sqlDB, `INSERT INTO withdrawal_authorizations
		(authorization_id, caller_id, chain_id, asset, recipient, amount, state, supplied_by)
		VALUES ('authz-1', 1, 1, $1, $2, 100, 'active', 'principal')`,
		addr40("aa"), addr40("bb"))

	const insertScope = `INSERT INTO withdrawal_authorization_scopes
		(authorization_id, intent_id, request_id, sender, fee_max_total,
		 fee_max_per_gas, fee_max_priority, allows_fee_replacement,
		 authorization_version, attested_by)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`
	scopeArgs := func(authID, intent, request, sender string, total, perGas, priority int64, replacement bool, version int64) []any {
		return []any{authID, intent, request, sender, total, perGas, priority, replacement, version, "principal"}
	}

	// Positive counterpart: a fully populated row, priority == max_fee.
	mustExec(t, sqlDB, insertScope,
		scopeArgs("authz-1", "intent-1", "req-1", addr40("cc"), 1000, 10, 10, false, 1)...)

	cases := []struct {
		name string
		sql  string
		args []any
		code string // PostgreSQL SQLSTATE: 23505 unique/PK, 23503 FK, 23514 check
	}{
		{"duplicate PK", insertScope,
			scopeArgs("authz-1", "intent-2", "req-2", addr40("dd"), 1, 1, 0, false, 1), "23505"},
		{"orphan scope (no grant)", insertScope,
			scopeArgs("authz-missing", "intent-3", "req-3", addr40("dd"), 1, 1, 0, false, 1), "23503"},
		{"negative total", insertScope,
			scopeArgs("authz-1", "intent-4", "req-4", addr40("dd"), -1, 1, 0, false, 1), "23514"},
		{"negative per gas", insertScope,
			scopeArgs("authz-1", "intent-5", "req-5", addr40("dd"), 1, -1, 0, false, 1), "23514"},
		{"negative priority", insertScope,
			scopeArgs("authz-1", "intent-6", "req-6", addr40("dd"), 1, 1, -1, false, 1), "23514"},
		{"priority above per-gas max", insertScope,
			scopeArgs("authz-1", "intent-7", "req-7", addr40("dd"), 1, 10, 11, false, 1), "23514"},
		{"version zero", insertScope,
			scopeArgs("authz-1", "intent-8", "req-8", addr40("dd"), 1, 1, 0, false, 0), "23514"},
		{"uppercase sender", insertScope,
			scopeArgs("authz-1", "intent-9", "req-9", addr40("DD"), 1, 1, 0, false, 1), "23514"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := sqlDB.ExecContext(ctx, tc.sql, tc.args...)
			wantPgErrorCode(t, err, tc.code)
		})
	}
}

func TestAuthzScopeMigrationDownRemovesOnlyItself(t *testing.T) {
	dsn := startPostgres(t)
	ctx := context.Background()
	opts := testMigrateOptions(dsn)

	var out bytes.Buffer
	if err := MigrateUp(ctx, opts, &out); err != nil {
		t.Fatalf("MigrateUp() error = %v (output %q)", err, out.String())
	}
	sqlDB := openTestSQL(t, dsn)
	if !relationExists(t, sqlDB, "withdrawal_authorization_scopes") {
		t.Fatal("withdrawal_authorization_scopes missing after up")
	}

	provider, err := newProvider(sqlDB, opts)
	if err != nil {
		t.Fatalf("newProvider: %v", err)
	}
	// Target 9 so exactly the carrier rolls back: on the pre-merge PB tree
	// 000010 was the only applied version above 8, and on the merged tree the
	// signer lane's 000009 is applied below it, so 9 (not 8) keeps the "000010
	// down removes only itself" claim exact in both trees.
	results, err := provider.DownTo(ctx, 9)
	if err != nil {
		t.Fatalf("DownTo(9): %v", err)
	}
	if len(results) != 1 || results[0].Source.Version != 10 {
		t.Fatalf("DownTo(9) rolled back %d migration(s), want exactly version 10", len(results))
	}

	if relationExists(t, sqlDB, "withdrawal_authorization_scopes") {
		t.Error("withdrawal_authorization_scopes still exists after DOWN of 000010")
	}
	for _, rel := range []string{"withdrawal_authorizations", "caller", "withdrawal_requests"} {
		if !relationExists(t, sqlDB, rel) {
			t.Errorf("%s must survive the 000010 DOWN", rel)
		}
	}

	// Re-applying behaves like a fresh 007-era upgrade.
	out.Reset()
	if err := MigrateUp(ctx, opts, &out); err != nil {
		t.Fatalf("re-up MigrateUp() error = %v (output %q)", err, out.String())
	}
	if !relationExists(t, sqlDB, "withdrawal_authorization_scopes") {
		t.Fatal("withdrawal_authorization_scopes missing after re-up")
	}
}
