//go:build integration

// intent_fk_repair_integration_test.go proves migration 000014 on a real
// PostgreSQL: the guarded-000013 defect (no-op recorded while 000012 was
// absent, applied versions never re-run, FK never built) is reproduced, the
// repair closes it, and every failure mode stays loud (orphan rows fail
// 23503, a missing 000012 raises instead of silently skipping). Testcontainers
// PostgreSQL, no mocks, no sleeps-as-proof.
//
// Paths covered:
//
//	(i)   incremental defected path: {1..11,13} -> no-op FK absent, then
//	      {12,14} pending -> FK present and validated;
//	(ii)  fresh install: full embedded set from zero -> FK present;
//	(iii) re-run: second MigrateUp is a no-op success, FK untouched;
//	(iv)  negative probe: absent intent_id -> 23503 tx_attempts_intent_fkey;
//	(v)   orphan tx_attempts row seeded before the repair -> repair FAILS,
//	      no silent delete, no fabricated intent;
//	(vi)  down the repair step -> FK gone, re-up -> FK present;
//	(vii) 000012 absent -> repair RAISEs (never a silent skip).
package db

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io/fs"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/jackc/pgx/v5/pgconn"
)

const (
	repairIntentID  = "repair-intent-1"
	repairBindingID = "repair-binding-1"
	repairAuthID    = "repair-authz-1"
	repairSender    = "0xcccccccccccccccccccccccccccccccccccccccc"
)

// txAttemptInsertSQL is the shared tx_attempts fixture shape (same column set
// as the T044 probe): a valid 007/008-referenced attempt whose intent_id is
// supplied by the caller.
const txAttemptInsertSQL = `INSERT INTO tx_attempts (
	attempt_id, signing_request_id, intent_id, binding_ref, authorization_id,
	authorization_version, recovery_version, chain_id, sender, nonce, tx_type,
	to_addr, value, data, gas_limit, gas_price, max_fee_per_gas, max_priority_fee_per_gas,
	asset, recipient, amount, canonical_envelope, content_hash)
	VALUES ($1, $2, $3, $4, $5, 1, 0, 1, $6, 0, 2,
		$7, 0, '\x', 21000, NULL, 1000000000, 100000000, $7, $8, '1000', '{}',
		'0x0000000000000000000000000000000000000000000000000000000000000001')`

// repairSetFS returns the embedded migration set minus the given versions.
// Stage-1 fixtures use it to build the defected state {1..11,13} (12 and 14
// not yet in the tree) before letting 000012/000014 land.
func repairSetFS(t *testing.T, drop ...int64) fstest.MapFS {
	t.Helper()
	files, err := MigrationFiles(Migrations)
	if err != nil {
		t.Fatalf("list embedded migrations: %v", err)
	}
	skip := make(map[int64]bool, len(drop))
	for _, v := range drop {
		skip[v] = true
	}
	fsys := make(fstest.MapFS, len(files))
	for _, f := range files {
		if skip[f.Version] {
			continue
		}
		data, err := fs.ReadFile(Migrations, f.Name)
		if err != nil {
			t.Fatalf("read embedded %s: %v", f.Name, err)
		}
		fsys[f.Name] = &fstest.MapFile{Data: data}
	}
	return fsys
}

// intentFKState reports whether tx_attempts_intent_fkey exists on tx_attempts
// and whether PostgreSQL validated it.
func intentFKState(t *testing.T, sqlDB *sql.DB) (exists, validated bool) {
	t.Helper()
	err := sqlDB.QueryRowContext(context.Background(), `
		SELECT convalidated
		  FROM pg_constraint
		 WHERE conname = 'tx_attempts_intent_fkey'
		   AND conrelid = 'public.tx_attempts'::regclass
		   AND contype = 'f'`).Scan(&validated)
	if errors.Is(err, sql.ErrNoRows) {
		return false, false
	}
	if err != nil {
		t.Fatalf("read tx_attempts_intent_fkey state: %v", err)
	}
	return true, validated
}

// assertVersionApplied pins one goose_db_version row's is_applied flag.
func assertVersionApplied(t *testing.T, sqlDB *sql.DB, version int64, want bool) {
	t.Helper()
	var n int
	if err := sqlDB.QueryRowContext(context.Background(),
		"SELECT count(*) FROM goose_db_version WHERE version_id = $1 AND is_applied", version).Scan(&n); err != nil {
		t.Fatalf("count goose_db_version rows for version %d: %v", version, err)
	}
	if got := n > 0; got != want {
		t.Fatalf("goose_db_version version %d applied = %v, want %v", version, got, want)
	}
}

// seedRepairPrerequisites creates the 007/008 rows the tx_attempts FKs need.
// No payment_intents row is created: each case decides whether the intent is
// present, absent (negative probe) or orphan (repair-refusal case).
func seedRepairPrerequisites(t *testing.T, sqlDB *sql.DB) {
	t.Helper()
	mustExec(t, sqlDB, `INSERT INTO caller (caller_id, label) VALUES (1, 'ops')`)
	mustExec(t, sqlDB, `INSERT INTO withdrawal_requests
		(request_id, caller_id, idempotency_key, authorization_id, chain_id, asset, recipient, amount)
		VALUES ('repair-req-1', 1, 'repair-idem-1', $1, 1, $2, $3, 100)`,
		repairAuthID, addr40("aa"), addr40("bb"))
	mustExec(t, sqlDB, `INSERT INTO withdrawal_authorizations
		(authorization_id, caller_id, chain_id, asset, recipient, amount, state)
		VALUES ($1, 1, 1, $2, $3, 100, 'active')`, repairAuthID, addr40("aa"), addr40("bb"))
	mustExec(t, sqlDB, `INSERT INTO nonce_wallet_registry (chain_id, sender, state, registry_seq)
		VALUES (1, $1, 'active', 1)`, repairSender)
	mustExec(t, sqlDB, `INSERT INTO nonce_bindings
		(binding_id, intent_id, chain_id, sender, nonce, state, authorization_id,
		 authorization_version, registry_seq, allocation_observation_id)
		VALUES ($1, 'repair-registry-intent', 1, $2, 0, 'allocated', $3, $4, 1, 'repair-obs-1')`,
		repairBindingID, repairSender, repairAuthID, strings.Repeat("ab", 32))
}

func seedRepairIntent(t *testing.T, sqlDB *sql.DB, intentID string) {
	t.Helper()
	mustExec(t, sqlDB, `INSERT INTO payment_intents
		(intent_id, request_id, chain_id, sender, authorization_id, authorization_version,
		 state, admitted_recovery_version)
		VALUES ($1, 'repair-req-1', 1, $2, $3, 1, 'admitted', 0)`, intentID, repairSender, repairAuthID)
}

// TestIntentFKRepairIncrementalGuardedNoOpThenRepair is path (i): reproduce
// the defect (guarded 000013 records a no-op while 000012 is absent, goose
// never re-runs it) and prove 000014 builds the FK once 000012 lands.
func TestIntentFKRepairIncrementalGuardedNoOpThenRepair(t *testing.T) {
	dsn := startPostgres(t)
	ctx := context.Background()

	// Stage 1: the defected database: {1..11} + guarded 000013, no 000012/000014.
	pre := testMigrateOptions(dsn)
	pre.FS = repairSetFS(t, 12, 14)
	var out bytes.Buffer
	if err := MigrateUp(ctx, pre, &out); err != nil {
		t.Fatalf("stage 1 MigrateUp() error = %v (output %q)", err, out.String())
	}
	t.Logf("stage 1 (000012 absent, guarded 000013): %s", strings.TrimSpace(out.String()))
	if !strings.Contains(out.String(), "applied=12 skipped=0 pending=0") {
		t.Fatalf("stage 1 output = %q, want applied=12 skipped=0 pending=0", out.String())
	}

	sqlDB := openTestSQL(t, dsn)
	if relationExists(t, sqlDB, "payment_intents") {
		t.Fatal("payment_intents exists before 000012; stage-1 fixture invalid")
	}
	if exists, _ := intentFKState(t, sqlDB); exists {
		t.Fatal("intent FK present before the repair; defect premise invalid")
	}
	// The no-op was recorded complete: 13 applied, 12/14 not applied.
	assertVersionApplied(t, sqlDB, 12, false)
	assertVersionApplied(t, sqlDB, 13, true)
	assertVersionApplied(t, sqlDB, 14, false)

	// Stage 2: 000012 lands, repair 000014 closes the gap.
	out.Reset()
	if err := MigrateUp(ctx, testMigrateOptions(dsn), &out); err != nil {
		t.Fatalf("stage 2 MigrateUp() error = %v (output %q)", err, out.String())
	}
	t.Logf("stage 2 (000012 + repair 000014): %s", strings.TrimSpace(out.String()))
	if !strings.Contains(out.String(), "applied=2 skipped=12 pending=0") {
		t.Fatalf("stage 2 output = %q, want applied=2 skipped=12 pending=0", out.String())
	}
	exists, validated := intentFKState(t, sqlDB)
	if !exists || !validated {
		t.Fatalf("intent FK after repair = exists:%v validated:%v, want true/true", exists, validated)
	}
	assertVersionApplied(t, sqlDB, 14, true)
}

// TestIntentFKRepairFreshInstall is path (ii): a full chain from zero gets the
// FK from guarded 000013 and the repair is a no-op.
func TestIntentFKRepairFreshInstall(t *testing.T) {
	dsn := startPostgres(t)
	migrateUpAll(t, dsn)
	sqlDB := openTestSQL(t, dsn)

	exists, validated := intentFKState(t, sqlDB)
	if !exists {
		t.Fatal("intent FK missing after fresh full migration")
	}
	if !validated {
		t.Fatal("intent FK is NOT VALID after fresh full migration")
	}

	var refSchema, refTable, refColumn string
	if err := sqlDB.QueryRowContext(context.Background(),
		`SELECT n.nspname, c.relname, a.attname
		   FROM pg_constraint con
		   JOIN pg_class c ON c.oid = con.confrelid
		   JOIN pg_namespace n ON n.oid = c.relnamespace
		   JOIN unnest(con.confkey) AS k(attnum) ON TRUE
		   JOIN pg_attribute a ON a.attrelid = con.confrelid AND a.attnum = k.attnum
		  WHERE con.conname = 'tx_attempts_intent_fkey' AND con.contype = 'f'`).
		Scan(&refSchema, &refTable, &refColumn); err != nil {
		t.Fatalf("read FK target: %v", err)
	}
	if refSchema != "public" || refTable != "payment_intents" || refColumn != "intent_id" {
		t.Fatalf("intent FK references %s.%s(%s), want payment_intents(intent_id)", refSchema, refTable, refColumn)
	}
	assertVersionApplied(t, sqlDB, 13, true)
	assertVersionApplied(t, sqlDB, 14, true)
}

// TestIntentFKRepairRerunIsNoOpSuccess is path (iii): a second migrate up
// applies nothing and leaves the FK untouched.
func TestIntentFKRepairRerunIsNoOpSuccess(t *testing.T) {
	dsn := startPostgres(t)
	migrateUpAll(t, dsn)
	ctx := context.Background()
	sqlDB := openTestSQL(t, dsn)

	var out bytes.Buffer
	if err := MigrateUp(ctx, testMigrateOptions(dsn), &out); err != nil {
		t.Fatalf("re-run MigrateUp() error = %v (output %q)", err, out.String())
	}
	if !strings.Contains(out.String(), "applied=0 skipped=14 pending=0") {
		t.Fatalf("re-run output = %q, want applied=0 skipped=14 pending=0", out.String())
	}
	exists, validated := intentFKState(t, sqlDB)
	if !exists || !validated {
		t.Fatalf("intent FK after re-run = exists:%v validated:%v, want true/true", exists, validated)
	}
}

// TestIntentFKRepairNegativeProbeAndValidInsert is path (iv): the repaired FK
// refuses an absent intent_id with 23503 on the exact constraint name, and a
// real intent is accepted.
func TestIntentFKRepairNegativeProbeAndValidInsert(t *testing.T) {
	dsn := startPostgres(t)
	migrateUpAll(t, dsn)
	ctx := context.Background()
	sqlDB := openTestSQL(t, dsn)
	seedRepairPrerequisites(t, sqlDB)

	_, err := sqlDB.ExecContext(ctx, txAttemptInsertSQL,
		"repair-probe-missing", "repair-probe-sr-missing", "missing-intent-fk-probe",
		repairBindingID, repairAuthID, repairSender, addr40("aa"), addr40("bb"))
	wantPgConstraint(t, err, "23503", "tx_attempts_intent_fkey")

	seedRepairIntent(t, sqlDB, repairIntentID)
	mustExec(t, sqlDB, txAttemptInsertSQL,
		"repair-probe-ok", "repair-probe-sr-ok", repairIntentID,
		repairBindingID, repairAuthID, repairSender, addr40("aa"), addr40("bb"))
}

// TestIntentFKRepairOrphanFailsLoud is path (v): an orphan tx_attempts row
// seeded before the repair makes the validated ADD CONSTRAINT fail. The goose
// transaction rolls back, the orphan rows survive intact and no intent is
// fabricated.
func TestIntentFKRepairOrphanFailsLoud(t *testing.T) {
	dsn := startPostgres(t)
	ctx := context.Background()

	pre := testMigrateOptions(dsn)
	pre.FS = repairSetFS(t, 12, 14)
	var out bytes.Buffer
	if err := MigrateUp(ctx, pre, &out); err != nil {
		t.Fatalf("stage 1 MigrateUp() error = %v (output %q)", err, out.String())
	}

	sqlDB := openTestSQL(t, dsn)
	seedRepairPrerequisites(t, sqlDB)
	mustExec(t, sqlDB, txAttemptInsertSQL,
		"repair-probe-orphan", "repair-probe-sr-orphan", "orphan-intent-1",
		repairBindingID, repairAuthID, repairSender, addr40("aa"), addr40("bb"))

	// 000012 creates payment_intents, then 000014 must fail on the orphan.
	out.Reset()
	err := MigrateUp(ctx, testMigrateOptions(dsn), &out)
	if err == nil {
		t.Fatalf("MigrateUp() with an orphan tx_attempts row: expected repair failure (output %q)", out.String())
	}
	t.Logf("orphan failure (expected): %s", strings.TrimSpace(err.Error()))
	if !strings.Contains(out.String(), "failed_version=14") {
		t.Fatalf("failure output = %q, want failed_version=14", out.String())
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23503" || pgErr.ConstraintName != "tx_attempts_intent_fkey" {
		t.Fatalf("orphan failure = %v, want 23503 tx_attempts_intent_fkey", err)
	}

	// No silent data fix: orphan row intact, no intent fabricated, FK absent.
	assertRowCount(t, sqlDB, "SELECT count(*) FROM tx_attempts WHERE intent_id = 'orphan-intent-1'", 1)
	assertRowCount(t, sqlDB, "SELECT count(*) FROM payment_intents", 0)
	if exists, _ := intentFKState(t, sqlDB); exists {
		t.Fatal("intent FK exists after the failed repair")
	}
	assertVersionApplied(t, sqlDB, 12, true)
	assertVersionApplied(t, sqlDB, 13, true)
	assertVersionApplied(t, sqlDB, 14, false)
}

// TestIntentFKRepairMissing012Raises is path (vii): if 000012 is absent the
// repair RAISEs a clear exception (never records a silent no-op).
func TestIntentFKRepairMissing012Raises(t *testing.T) {
	dsn := startPostgres(t)
	ctx := context.Background()

	pre := testMigrateOptions(dsn)
	pre.FS = repairSetFS(t, 12, 14)
	var out bytes.Buffer
	if err := MigrateUp(ctx, pre, &out); err != nil {
		t.Fatalf("stage 1 MigrateUp() error = %v (output %q)", err, out.String())
	}

	// Only the repair is pending: 000012 is still absent.
	stageTwo := testMigrateOptions(dsn)
	stageTwo.FS = repairSetFS(t, 12)
	out.Reset()
	err := MigrateUp(ctx, stageTwo, &out)
	if err == nil {
		t.Fatalf("MigrateUp() without 000012: expected a loud repair refusal (output %q)", out.String())
	}
	t.Logf("missing-000012 refusal (expected): %s", strings.TrimSpace(err.Error()))
	if !strings.Contains(out.String(), "failed_version=14") {
		t.Fatalf("refusal output = %q, want failed_version=14", out.String())
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "P0001" ||
		!strings.Contains(pgErr.Message, "requires 011 table payment_intents") {
		t.Fatalf("refusal = %v, want a P0001 RAISE naming the missing payment_intents table", err)
	}

	sqlDB := openTestSQL(t, dsn)
	if relationExists(t, sqlDB, "payment_intents") {
		t.Fatal("payment_intents exists although 000012 never ran")
	}
	if exists, _ := intentFKState(t, sqlDB); exists {
		t.Fatal("intent FK exists although the repair refused")
	}
	assertVersionApplied(t, sqlDB, 14, false)
}

// TestIntentFKRepairDownAndReUp is path (vi): the repair's Down drops the FK
// and the next migrate up rebuilds it.
func TestIntentFKRepairDownAndReUp(t *testing.T) {
	dsn := startPostgres(t)
	migrateUpAll(t, dsn)
	ctx := context.Background()
	sqlDB := openTestSQL(t, dsn)

	provider, err := newProvider(sqlDB, testMigrateOptions(dsn))
	if err != nil {
		t.Fatalf("newProvider: %v", err)
	}
	result, err := provider.Down(ctx)
	if err != nil {
		t.Fatalf("Down() of the repair step: %v", err)
	}
	if result.Source.Version != 14 {
		t.Fatalf("Down() reverted version %d, want 14", result.Source.Version)
	}
	if exists, _ := intentFKState(t, sqlDB); exists {
		t.Fatal("intent FK still exists after the repair DOWN")
	}
	if !relationExists(t, sqlDB, "payment_intents") {
		t.Fatal("payment_intents lost by the repair DOWN")
	}
	assertVersionApplied(t, sqlDB, 14, false)

	var out bytes.Buffer
	if err := MigrateUp(ctx, testMigrateOptions(dsn), &out); err != nil {
		t.Fatalf("re-up MigrateUp() error = %v (output %q)", err, out.String())
	}
	t.Logf("re-up (repair rebuild): %s", strings.TrimSpace(out.String()))
	if !strings.Contains(out.String(), "applied=1 skipped=13 pending=0") {
		t.Fatalf("re-up output = %q, want applied=1 skipped=13 pending=0", out.String())
	}
	exists, validated := intentFKState(t, sqlDB)
	if !exists || !validated {
		t.Fatalf("intent FK after re-up = exists:%v validated:%v, want true/true", exists, validated)
	}
}

func assertRowCount(t *testing.T, sqlDB *sql.DB, query string, want int) {
	t.Helper()
	var got int
	if err := sqlDB.QueryRowContext(context.Background(), query).Scan(&got); err != nil {
		t.Fatalf("count query %q: %v", query, err)
	}
	if got != want {
		t.Fatalf("row count for %q = %d, want %d", query, got, want)
	}
}
