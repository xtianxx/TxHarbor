//go:build integration

// migration_integration_test.go owns spec tasks T004/T005/T041 for
// 009-signer-service: it proves migration 000009 applies and rolls back
// cleanly over a real 007 database, that its named constraints carry the exact
// declared names, that historical migrations stay untouched (merged-tree
// allowlist: every embedded migration except the lane-owned 000009 is frozen),
// that the post-sync merged chain 000001-000010 and its 9-gap fill behave
// (T041), and that no TTL/grace column exists. Every database assertion here
// is a raw-SQL probe; no signer business code is exercised. Each test boots
// its own isolated scratch PostgreSQL container via testcontainers and
// terminates it in t.Cleanup.
package signer

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
	// signerMigrationCurrentVersion is the version 000009 brings the schema to.
	signerMigrationCurrentVersion = 9
	// signerMigrationBaselineVersion is the highest migration before 000009.
	signerMigrationBaselineVersion = 7
	// signerMigrationMergedHeadVersion is the merged-tree chain head: PB's
	// carrier 000010 landed on main ahead of 009 (R-PB8 merge order), so the
	// merged chain is 000001-000008 + 000010 with 000009 filling the gap at 9.
	signerMigrationMergedHeadVersion = 10
	// signerMaxUint256 is the declared upper bound of the value/amount CHECKs
	// (2^256-1); signerOverUint256 is 2^256.
	signerMaxUint256  = "115792089237316195423570985008687907853269984665640564039457584007913129639935"
	signerOverUint256 = "115792089237316195423570985008687907853269984665640564039457584007913129639936"
	// signerOverMaxNonce is 2^64, one past the declared nonce upper bound.
	signerOverMaxNonce = "18446744073709551616"
)

// signerMigrationTables are the six objects created by 000009, dropped by its
// Down, in the exact set the migration declares.
var signerMigrationTables = []string{
	"signer_caller",
	"signer_credential",
	"signing_requests",
	"signature_results",
	"signing_request_audit",
	"delivery_admissions",
}

// signerMigrationStartPostgres boots a real PostgreSQL container and returns
// its DSN. Skips (never passes) when no Docker provider is available.
func signerMigrationStartPostgres(t *testing.T) string {
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

func signerMigrationOpenSQL(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	sqlDB, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open sql db: %v", err)
	}
	t.Cleanup(func() { sqlDB.Close() })
	return sqlDB
}

func signerMigrationOptions(dsn string) db.MigrateOptions {
	return db.MigrateOptions{DSN: dsn, LockTimeout: 5 * time.Second, ConnectTimeout: 5 * time.Second}
}

// signerMigrationSubsetFS returns the embedded migrations up to maxVersion
// inclusive as a standalone FS, so a real database pinned at version N can be
// built before testing the N+1 upgrade.
func signerMigrationSubsetFS(t *testing.T, maxVersion int64) fstest.MapFS {
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

// signerMigrationFSExcept returns the embedded migrations minus dropVersion as
// a standalone FS: embedded minus 9 is main's PB-era {000001..000008, 000010}
// state, where 9 is reserved but unmerged (T041 gap-fill sequence).
func signerMigrationFSExcept(t *testing.T, dropVersion int64) fstest.MapFS {
	t.Helper()
	files, err := db.MigrationFiles(migrations.FS)
	if err != nil {
		t.Fatalf("list embedded migrations: %v", err)
	}
	fsys := fstest.MapFS{}
	for _, f := range files {
		if f.Version == dropVersion {
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

// signerMigrationNewProvider replicates internal/db's unexported newProvider
// (same locker, same options) since that constructor is not importable.
func signerMigrationNewProvider(t *testing.T, sqlDB *sql.DB, fsys fs.FS) *goose.Provider {
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

// signerMigrationMigrateUp applies the full embedded set and returns an open
// raw-SQL handle on the migrated database.
func signerMigrationMigrateUp(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	var out bytes.Buffer
	if err := db.MigrateUp(context.Background(), signerMigrationOptions(dsn), &out); err != nil {
		t.Fatalf("MigrateUp() error = %v (output %q)", err, out.String())
	}
	return signerMigrationOpenSQL(t, dsn)
}

func signerMigrationMustExec(t *testing.T, sqlDB *sql.DB, query string, args ...any) {
	t.Helper()
	if _, err := sqlDB.ExecContext(context.Background(), query, args...); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

// signerMigrationWantPgError asserts err is a *pgconn.PgError with the exact
// SQLSTATE code and the exact declared constraint name.
func signerMigrationWantPgError(t *testing.T, err error, code, constraint string) {
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

func signerMigrationRelationExists(t *testing.T, sqlDB *sql.DB, name string) bool {
	t.Helper()
	var exists bool
	if err := sqlDB.QueryRowContext(context.Background(),
		"SELECT to_regclass($1) IS NOT NULL", name).Scan(&exists); err != nil {
		t.Fatalf("to_regclass(%s): %v", name, err)
	}
	return exists
}

func signerMigrationCount(t *testing.T, sqlDB *sql.DB, table string) int {
	t.Helper()
	var n int
	if err := sqlDB.QueryRowContext(context.Background(),
		"SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

// signerMigrationGit runs git in dir (or the process CWD when empty) and
// returns trimmed stdout, failing the test on any git error.
func signerMigrationGit(t *testing.T, dir string, args ...string) string {
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

func signerMigrationAddr(fill string) string     { return "0x" + strings.Repeat(fill, 20) }
func signerMigrationHash(fill string) string     { return "0x" + strings.Repeat(fill, 32) }
func signerMigrationFingerprint(f string) string { return strings.Repeat(f, 64/len(f)) }
func signerMigrationSecret(fill string) string   { return strings.Repeat(fill, 64/len(fill)) }

// signerMigrationSignature is a syntactically valid secp256k1 signature value:
// 0x-prefixed, 130 lowercase hex characters.
func signerMigrationSignature(fill string) string { return "0x" + strings.Repeat(fill, 65) }

// signerMigrationWithArg overrides one positional argument of a fresh
// signerMigrationRequestArgs slice, isolating a probe to a single constraint.
func signerMigrationWithArg(args []any, index int, value any) []any {
	args[index] = value
	return args
}

// signerMigrationRequestArgs returns one full valid type-0 signing_requests
// argument list for signerMigrationInsertRequest; callers override single
// positions to isolate a probe to exactly one named constraint. $20 is the
// submit-time scope-version snapshot (1 = observed scope version).
func signerMigrationRequestArgs(callerID int64, requestID, attemptID, authID string) []any {
	return []any{
		callerID, requestID, attemptID, nil, // caller, signing_request_id, attempt_id, replacement_of
		int64(1), signerMigrationAddr("11"), "0", 0, // chain_id, sender, nonce, tx_type
		signerMigrationAddr("11"), "0", []byte{}, // to_addr, value, data
		"21000", "1", // gas_limit, gas_price
		signerMigrationAddr("11"), signerMigrationAddr("22"), "100", // asset, recipient, amount
		signerMigrationHash("cc"), authID, signerMigrationFingerprint("dd"), // content_hash, authorization_id, authorization_fingerprint
		int64(1), // authorization_version
	}
}

// signerMigrationInsertRequest inserts one signing_requests row from the
// signerMigrationRequestArgs positional argument list ($1..$20 in slice order;
// $9 is both to_addr and asset for the valid baseline, callers override single
// positions to isolate a probe).
const signerMigrationInsertRequest = `INSERT INTO signing_requests
	(caller_id, signing_request_id, attempt_id, replacement_of, intent_id, binding_ref, chain_id,
	 sender, nonce, tx_type, to_addr, value, data, gas_limit, gas_price,
	 asset, recipient, amount, canonical_envelope, content_hash,
	 authorization_id, authorization_fingerprint, authorization_state, authorization_version, policy_version)
	VALUES ($1, $2, $3, $4, 'intent-1', 'bind-1', $5,
	 $6, $7, $8, $9, $10, $11, $12, $13,
	 $14, $15, $16, 'envelope', $17,
	 $18, $19, 'active', $20, 'policy-v1')`

// TestSignerMigrationHistoryUntouched proves the merged-tree diff allowlist:
// every embedded migration except the lane-owned 000009 is frozen (git diff
// over each path is empty), git status under migrations/ mentions only 000009,
// version 9 carries the declared name, the PB carrier 000010 is the chain head,
// and no embedded version exceeds it.
//
// T041 baseline update (T035 sync, merged tree): the pre-sync expectation was
// the 000001-000009 allowlist with nothing above 9. Main now legitimately
// embeds 000008 (008-nonce-manager) and PB's carrier 000010, so the frozen set
// grew to 000001-000008 + 000010 and the upper bound moved from 9 to 10. Old:
// any v > 9 failed. New: any v > 10 fails. The guarantee is unchanged: any edit
// to an applied-history migration still fails the git diff below, and 000009
// stays the single lane-owned migration.
func TestSignerMigrationHistoryUntouched(t *testing.T) {
	root := signerMigrationGit(t, "", "rev-parse", "--show-toplevel")
	if root == "" {
		t.Fatal("git rev-parse --show-toplevel returned no repository root")
	}

	files, err := db.MigrationFiles(migrations.FS)
	if err != nil {
		t.Fatalf("list embedded migrations: %v", err)
	}
	frozen := make([]string, 0, len(files))
	seen := map[int64]string{}
	for _, f := range files {
		seen[f.Version] = f.Name
		if f.Version == signerMigrationCurrentVersion {
			continue // 000009 is lane-owned: the only editable migration
		}
		frozen = append(frozen, "migrations/"+f.Name)
	}
	if name, ok := seen[signerMigrationCurrentVersion]; !ok || name != "000009_signer_service.sql" {
		t.Fatalf("embedded migration 9 = %q (present=%v), want 000009_signer_service.sql", name, ok)
	}
	if name, ok := seen[signerMigrationMergedHeadVersion]; !ok || name != "000010_withdrawal_authorization_scopes.sql" {
		t.Fatalf("embedded migration 10 = %q (present=%v), want the PB carrier 000010_withdrawal_authorization_scopes.sql", name, ok)
	}
	for v := range seen {
		if v > signerMigrationMergedHeadVersion {
			t.Fatalf("embedded migration version %d exceeds the merged chain head %d", v, signerMigrationMergedHeadVersion)
		}
	}
	if len(frozen) == 0 {
		t.Fatal("no frozen historical migrations listed; allowlist probe is vacuous")
	}

	args := append([]string{"diff", "--name-only", "--"}, frozen...)
	if out := signerMigrationGit(t, root, args...); out != "" {
		t.Fatalf("historical migrations were modified (T004 requires zero changes):\n%s", out)
	}

	status := signerMigrationGit(t, root, "status", "--porcelain", "--", "migrations")
	for _, line := range strings.Split(status, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.Contains(line, "000009_signer_service.sql") {
			continue
		}
		t.Fatalf("migrations/ diff allowlist is the new 000009 file only; unexpected entry %q", line)
	}
}

// TestSignerMigrationUpgradeDowngradeFrom007 covers the T004/T005 upgrade path
// on an isolated scratch database: a real 007 database gains every merged-tree
// version above 007 — 000008 (008-nonce-manager), the lane-owned 000009, and
// PB's carrier 000010 — the status reports 10/clean, the applied-descending
// DownTo(7) reverts 000010, 000009, 000008 in that order (000009's Down drops
// exactly the six 009 tables) while the upstream 002 row survives, and re-up
// reproduces the same state.
//
// T041 baseline update (T035 sync, merged tree): the pre-sync expectation was
// applied=1 skipped=7 — only 000009 above 007, DownTo rolling back exactly 9.
// Main now carries 000008 and 000010, so the same 007 database legitimately
// applies 000008, 000009 and 000010 (applied=3 skipped=7) and DownTo(7) rolls
// back exactly [10, 9, 8]. Applied history is still never rewritten: the lane
// fills the reserved 9 and touches nothing below it.
func TestSignerMigrationUpgradeDowngradeFrom007(t *testing.T) {
	dsn := signerMigrationStartPostgres(t)
	ctx := context.Background()

	// Build a real database at 007, then seed one representative 002 row.
	subset := signerMigrationSubsetFS(t, signerMigrationBaselineVersion)
	subsetOpts := signerMigrationOptions(dsn)
	subsetOpts.FS = subset
	var out bytes.Buffer
	if err := db.MigrateUp(ctx, subsetOpts, &out); err != nil {
		t.Fatalf("MigrateUp(through %d) error = %v (output %q)", signerMigrationBaselineVersion, err, out.String())
	}
	sqlDB := signerMigrationOpenSQL(t, dsn)
	signerMigrationMustExec(t, sqlDB, `INSERT INTO chain_blocks
		(chain_id, number, hash, parent_hash, canonical)
		VALUES (1, 100, $1, $2, TRUE)`, signerMigrationHash("aa"), signerMigrationHash("bb"))

	// Full embedded set applies every merged-tree version above 007:
	// 000008, the lane-owned 000009, and PB's carrier 000010.
	fullOpts := signerMigrationOptions(dsn)
	out.Reset()
	if err := db.MigrateUp(ctx, fullOpts, &out); err != nil {
		t.Fatalf("upgrade MigrateUp() error = %v (output %q)", err, out.String())
	}
	if !strings.Contains(out.String(), "applied=3 skipped=7 pending=0") {
		t.Fatalf("upgrade output = %q, want applied=3 skipped=7 pending=0 (000008, 000009, 000010)", out.String())
	}

	out.Reset()
	if err := db.MigrateStatus(ctx, fullOpts, &out); err != nil {
		t.Fatalf("MigrateStatus() after upgrade error = %v", err)
	}
	if !strings.Contains(out.String(), "current_version=10") || !strings.Contains(out.String(), "pending=none") {
		t.Fatalf("status after upgrade = %q, want current_version=10 and pending=none", out.String())
	}
	for _, rel := range signerMigrationTables {
		if !signerMigrationRelationExists(t, sqlDB, rel) {
			t.Errorf("%s missing after 000009 upgrade", rel)
		}
	}

	// Applied-descending DownTo(7) via a provider mirroring internal/db: every
	// merged-tree version above 007, carrier first — 000010, 000009, 000008.
	provider := signerMigrationNewProvider(t, sqlDB, migrations.FS)
	results, err := provider.DownTo(ctx, signerMigrationBaselineVersion)
	if err != nil {
		t.Fatalf("DownTo(%d): %v", signerMigrationBaselineVersion, err)
	}
	gotDown := make([]int64, 0, len(results))
	for _, r := range results {
		gotDown = append(gotDown, r.Source.Version)
	}
	wantDown := []int64{
		signerMigrationMergedHeadVersion,   // 000010 PB carrier
		signerMigrationCurrentVersion,      // 000009 lane-owned
		signerMigrationBaselineVersion + 1, // 000008
	}
	if len(gotDown) != len(wantDown) {
		t.Fatalf("DownTo(%d) rolled back %v, want %v", signerMigrationBaselineVersion, gotDown, wantDown)
	}
	for i, want := range wantDown {
		if gotDown[i] != want {
			t.Fatalf("DownTo(%d) rolled back %v, want %v", signerMigrationBaselineVersion, gotDown, wantDown)
		}
	}
	for _, rel := range signerMigrationTables {
		if signerMigrationRelationExists(t, sqlDB, rel) {
			t.Errorf("%s still exists after 000009 Down", rel)
		}
	}
	if got := signerMigrationCount(t, sqlDB, "chain_blocks"); got != 1 {
		t.Errorf("chain_blocks rows after 000009 Down = %d, want 1 (upstream data must survive)", got)
	}

	out.Reset()
	if err := db.MigrateStatus(ctx, fullOpts, &out); err != nil {
		t.Fatalf("MigrateStatus() after Down error = %v", err)
	}
	if !strings.Contains(out.String(), "current_version=7") || !strings.Contains(out.String(), "pending=3") {
		t.Fatalf("status after Down = %q, want current_version=7 and pending=3", out.String())
	}

	// Re-up reproduces the same clean state.
	out.Reset()
	if err := db.MigrateUp(ctx, fullOpts, &out); err != nil {
		t.Fatalf("re-up MigrateUp() error = %v (output %q)", err, out.String())
	}
	if !strings.Contains(out.String(), "applied=3 skipped=7 pending=0") {
		t.Fatalf("re-up output = %q, want applied=3 skipped=7 pending=0", out.String())
	}
	out.Reset()
	if err := db.MigrateStatus(ctx, fullOpts, &out); err != nil {
		t.Fatalf("MigrateStatus() after re-up error = %v", err)
	}
	if !strings.Contains(out.String(), "current_version=10") || !strings.Contains(out.String(), "pending=none") {
		t.Fatalf("status after re-up = %q, want current_version=10 and pending=none", out.String())
	}
	for _, rel := range signerMigrationTables {
		if !signerMigrationRelationExists(t, sqlDB, rel) {
			t.Errorf("%s missing after 000009 re-up", rel)
		}
	}
	if got := signerMigrationCount(t, sqlDB, "chain_blocks"); got != 1 {
		t.Errorf("chain_blocks rows after re-up = %d, want 1", got)
	}
}

// signerMigrationWantApplied asserts the applied set is exactly want, in order,
// and that the chain head equals the last entry with nothing pending.
func signerMigrationWantApplied(t *testing.T, state db.SchemaState, want ...int64) {
	t.Helper()
	if len(state.Applied) != len(want) {
		t.Fatalf("applied versions = %v, want %v", state.Applied, want)
	}
	for i, v := range want {
		if state.Applied[i] != v {
			t.Fatalf("applied versions = %v, want %v", state.Applied, want)
		}
	}
	if state.Current != want[len(want)-1] {
		t.Fatalf("current version = %d, want %d", state.Current, want[len(want)-1])
	}
	if len(state.Pending) != 0 {
		t.Fatalf("pending versions = %v, want none", state.Pending)
	}
}

// signerMigrationWantGateGreen asserts the serve gate accepts the chain head.
func signerMigrationWantGateGreen(t *testing.T, ctx context.Context, opts db.MigrateOptions) {
	t.Helper()
	compat, err := db.CheckCompatibility(ctx, opts)
	if err != nil {
		t.Fatalf("CheckCompatibility() = %v, want green", err)
	}
	if compat.Current != signerMigrationMergedHeadVersion ||
		compat.Target != signerMigrationMergedHeadVersion || compat.Pending != 0 {
		t.Fatalf("compatibility = %+v, want current=target=%d pending=0", compat, signerMigrationMergedHeadVersion)
	}
}

// signerMigrationWantTables asserts the six 000009 tables exist.
func signerMigrationWantTables(t *testing.T, sqlDB *sql.DB) {
	t.Helper()
	for _, rel := range signerMigrationTables {
		if !signerMigrationRelationExists(t, sqlDB, rel) {
			t.Errorf("%s missing after the merged chain", rel)
		}
	}
}

// signerMigrationWantAuthorizationVersion pins that the 000009 that applied is
// the T037-amended revision: signing_requests.authorization_version exists.
func signerMigrationWantAuthorizationVersion(t *testing.T, sqlDB *sql.DB) {
	t.Helper()
	var columns int
	if err := sqlDB.QueryRowContext(context.Background(), `
		SELECT count(*) FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'signing_requests'
		  AND column_name = 'authorization_version'`).Scan(&columns); err != nil {
		t.Fatalf("probe signing_requests.authorization_version: %v", err)
	}
	if columns != 1 {
		t.Fatal("applied 000009 lacks authorization_version: the T037 amendment did not ship at version 9")
	}
}

// TestSignerMigrationMergedChain is the T041 post-sync chain proof on the
// merged tree, driving the lane's real embedded 000009 (T037 amended it with
// the authorization_version snapshot) rather than the PB lane's pinned
// pre-T037 fixture. Three eras, one scratch PostgreSQL each:
//
//	(a) empty             -> full chain 000001-000010: applied=10, gate green
//	(b) 008-era {1..8}     -> applies exactly 000009 + 000010
//	(c) PB-era {1..8,10}   -> gate refuses on pending 9, then the
//	    WithAllowOutofOrder provider fills the gap with exactly 000009
//
// T041 baseline note: pre-sync the chain ended at 9 and (b)/(c) did not exist.
// The merged tree legitimately carries 000008 and the PB carrier 000010; this
// pins that 009 fills the reserved gap in every direction and that the amended
// 000009 is what actually applies.
func TestSignerMigrationMergedChain(t *testing.T) {
	t.Run("empty to full chain", func(t *testing.T) {
		dsn := signerMigrationStartPostgres(t)
		ctx := context.Background()
		opts := signerMigrationOptions(dsn)
		var out bytes.Buffer
		if err := db.MigrateUp(ctx, opts, &out); err != nil {
			t.Fatalf("MigrateUp(full chain) error = %v (output %q)", err, out.String())
		}
		if !strings.Contains(out.String(), "applied=10 skipped=0 pending=0") {
			t.Fatalf("MigrateUp(full chain) output = %q, want applied=10 skipped=0 pending=0", out.String())
		}
		state, err := db.Inspect(ctx, opts)
		if err != nil {
			t.Fatalf("Inspect() error = %v", err)
		}
		signerMigrationWantApplied(t, state, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		signerMigrationWantGateGreen(t, ctx, opts)
		sqlDB := signerMigrationOpenSQL(t, dsn)
		signerMigrationWantTables(t, sqlDB)
		signerMigrationWantAuthorizationVersion(t, sqlDB)
	})

	t.Run("008-era upgrade adds lane and carrier migrations", func(t *testing.T) {
		dsn := signerMigrationStartPostgres(t)
		ctx := context.Background()
		era := signerMigrationOptions(dsn)
		era.FS = signerMigrationSubsetFS(t, signerMigrationBaselineVersion+1)
		var out bytes.Buffer
		if err := db.MigrateUp(ctx, era, &out); err != nil {
			t.Fatalf("MigrateUp(008-era) error = %v (output %q)", err, out.String())
		}
		if !strings.Contains(out.String(), "applied=8 skipped=0 pending=0") {
			t.Fatalf("MigrateUp(008-era) output = %q, want applied=8 skipped=0 pending=0", out.String())
		}
		state, err := db.Inspect(ctx, era)
		if err != nil {
			t.Fatalf("Inspect(008-era) error = %v", err)
		}
		if state.Current != signerMigrationBaselineVersion+1 {
			t.Fatalf("008-era current = %d, want %d", state.Current, signerMigrationBaselineVersion+1)
		}

		full := signerMigrationOptions(dsn)
		out.Reset()
		if err := db.MigrateUp(ctx, full, &out); err != nil {
			t.Fatalf("upgrade MigrateUp() error = %v (output %q)", err, out.String())
		}
		if !strings.Contains(out.String(), "applied=2 skipped=8 pending=0") {
			t.Fatalf("upgrade output = %q, want applied=2 skipped=8 pending=0 (000009, 000010)", out.String())
		}
		state, err = db.Inspect(ctx, full)
		if err != nil {
			t.Fatalf("Inspect() after upgrade error = %v", err)
		}
		signerMigrationWantApplied(t, state, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		signerMigrationWantGateGreen(t, ctx, full)
		sqlDB := signerMigrationOpenSQL(t, dsn)
		signerMigrationWantTables(t, sqlDB)
		signerMigrationWantAuthorizationVersion(t, sqlDB)
	})

	t.Run("PB-era gap fill applies only the lane migration", func(t *testing.T) {
		dsn := signerMigrationStartPostgres(t)
		ctx := context.Background()
		pbEra := signerMigrationOptions(dsn)
		pbEra.FS = signerMigrationFSExcept(t, signerMigrationCurrentVersion)
		var out bytes.Buffer
		if err := db.MigrateUp(ctx, pbEra, &out); err != nil {
			t.Fatalf("MigrateUp(PB-era) error = %v (output %q)", err, out.String())
		}
		if !strings.Contains(out.String(), "applied=9 skipped=0 pending=0") {
			t.Fatalf("MigrateUp(PB-era) output = %q, want applied=9 skipped=0 pending=0", out.String())
		}

		full := signerMigrationOptions(dsn)
		state, err := db.Inspect(ctx, full)
		if err != nil {
			t.Fatalf("Inspect(PB-era) error = %v", err)
		}
		if state.Current != signerMigrationMergedHeadVersion || len(state.Pending) != 1 ||
			state.Pending[0] != signerMigrationCurrentVersion {
			t.Fatalf("PB-era state = current %d pending %v, want current 10 with only version 9 pending",
				state.Current, state.Pending)
		}

		// The serve gate must refuse while the reserved 9 is pending.
		if _, err := db.CheckCompatibility(ctx, full); err == nil {
			t.Fatal("serve gate: expected refusal while version 9 is pending")
		} else if !strings.Contains(err.Error(), "pending") || !strings.Contains(err.Error(), "migrate up") {
			t.Fatalf("refusal lacks actionable text: %v", err)
		}

		// Gap fill: the WithAllowOutofOrder provider applies exactly 000009.
		out.Reset()
		if err := db.MigrateUp(ctx, full, &out); err != nil {
			t.Fatalf("gap-fill MigrateUp() error = %v (output %q)", err, out.String())
		}
		if !strings.Contains(out.String(), "applied=1 skipped=9 pending=0") {
			t.Fatalf("gap-fill output = %q, want applied=1 skipped=9 pending=0", out.String())
		}
		state, err = db.Inspect(ctx, full)
		if err != nil {
			t.Fatalf("Inspect() after gap fill error = %v", err)
		}
		signerMigrationWantApplied(t, state, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		signerMigrationWantGateGreen(t, ctx, full)
		sqlDB := signerMigrationOpenSQL(t, dsn)
		signerMigrationWantTables(t, sqlDB)
		signerMigrationWantAuthorizationVersion(t, sqlDB)
	})
}

// TestSignerMigrationConstraintNames probes the declared constraint names with
// raw SQL: duplicate-key conflicts must raise 23505 on the exact named
// unique/PK/index constraint, format and bound violations must raise 23514,
// dangling references must raise 23503 — all on the exact declared constraint
// name — and the positive counterparts must not over-reject.
func TestSignerMigrationConstraintNames(t *testing.T) {
	dsn := signerMigrationStartPostgres(t)
	sqlDB := signerMigrationMigrateUp(t, dsn)
	ctx := context.Background()

	const insertCaller = `INSERT INTO signer_caller (caller_id, label) VALUES ($1, $2)`
	const insertCredential = `INSERT INTO signer_credential (caller_id, secret_hash, secret_prefix)
		VALUES ($1, $2, $3)`
	const insertResult = `INSERT INTO signature_results (signing_request_row, signature, tx_hash)
		VALUES ($1, $2, $3)`
	const insertAdmission = `INSERT INTO delivery_admissions
		(signing_request_row, attempt_seq, verdict, authorization_id, authorization_fingerprint,
		 authorization_state, binding_class, recovery_version)
		VALUES ($1, $2, $3, 'auth-anchor-1', $4, 'active', $5, 0)`
	const insertAudit = `INSERT INTO signing_request_audit (signing_request_id, caller_id, action)
		VALUES ($1, $2, $3)`

	// Valid baseline: caller, credential, two anchor requests (distinct
	// authorizations), one result, one admission, one audit row.
	signerMigrationMustExec(t, sqlDB, insertCaller, int64(1001), "probe")
	signerMigrationMustExec(t, sqlDB, insertCredential, int64(1001), signerMigrationSecret("a"), "txh_alpha")
	anchorArgs := signerMigrationRequestArgs(1001, "req-1", "att-1", "auth-anchor-1")
	var anchorID int64
	if err := sqlDB.QueryRowContext(ctx, signerMigrationInsertRequest+" RETURNING id", anchorArgs...).Scan(&anchorID); err != nil {
		t.Fatalf("seed anchor request: %v", err)
	}
	secondArgs := signerMigrationRequestArgs(1001, "req-2", "att-2", "auth-two")
	var secondID int64
	if err := sqlDB.QueryRowContext(ctx, signerMigrationInsertRequest+" RETURNING id", secondArgs...).Scan(&secondID); err != nil {
		t.Fatalf("seed second request: %v", err)
	}
	signerMigrationMustExec(t, sqlDB, insertResult, anchorID, signerMigrationSignature("ab"), signerMigrationHash("e1"))
	signerMigrationMustExec(t, sqlDB, insertAdmission, anchorID, 1, "admitted", signerMigrationFingerprint("dd"), "")
	signerMigrationMustExec(t, sqlDB, insertAudit, "req-1", int64(1001), "received")

	requestProbe := func(req, att, auth string) []any {
		return signerMigrationRequestArgs(1001, req, att, auth)
	}
	violations := []struct {
		name       string
		query      string
		args       []any
		code       string
		constraint string
	}{
		{"duplicate caller", insertCaller,
			[]any{int64(1001), "dup"}, "23505", "signer_caller_pkey"},
		{"caller id zero", insertCaller,
			[]any{int64(0), "zero"}, "23514", "signer_caller_caller_id_check"},
		{"duplicate credential hash", insertCredential,
			[]any{int64(1001), signerMigrationSecret("a"), "txh_dup"}, "23505", "signer_credential_secret_hash_uniq"},
		{"uppercase credential hash", insertCredential,
			[]any{int64(1001), strings.Repeat("A", 64), "txh_bad"}, "23514", "signer_credential_secret_hash_check"},
		{"credential unknown caller", insertCredential,
			[]any{int64(999999), signerMigrationSecret("f"), "txh_fk"}, "23503", "signer_credential_caller_id_fkey"},
		{"duplicate caller+request identity", signerMigrationInsertRequest,
			requestProbe("req-1", "att-dup", "auth-dup"), "23505", "signing_requests_caller_request_uniq"},
		{"duplicate attempt identity", signerMigrationInsertRequest,
			requestProbe("req-dup", "att-1", "auth-dup2"), "23505", "signing_requests_attempt_uniq"},
		{"second anchor for one authorization", signerMigrationInsertRequest,
			requestProbe("req-an-2", "att-an-2", "auth-anchor-1"), "23505", "signing_requests_authorization_anchor_uniq"},
		{"request unknown caller", signerMigrationInsertRequest,
			signerMigrationRequestArgs(999999, "req-nocaller", "att-nocaller", "auth-nocaller"), "23503", "signing_requests_caller_id_fkey"},
		{"unknown replacement predecessor", signerMigrationInsertRequest,
			signerMigrationWithArg(requestProbe("req-repl-fk", "att-repl-fk", "auth-repl-fk"), 3, int64(999999)), "23503", "signing_requests_replacement_fkey"},
		{"amount zero", signerMigrationInsertRequest,
			signerMigrationWithArg(requestProbe("req-a0", "att-a0", "auth-a0"), 15, "0"), "23514", "signing_requests_amount_check"},
		{"amount over uint256", signerMigrationInsertRequest,
			signerMigrationWithArg(requestProbe("req-a1", "att-a1", "auth-a1"), 15, signerOverUint256), "23514", "signing_requests_amount_check"},
		{"value over uint256", signerMigrationInsertRequest,
			signerMigrationWithArg(requestProbe("req-v1", "att-v1", "auth-v1"), 9, signerOverUint256), "23514", "signing_requests_value_check"},
		{"nonce over uint64", signerMigrationInsertRequest,
			signerMigrationWithArg(requestProbe("req-n1", "att-n1", "auth-n1"), 6, signerOverMaxNonce), "23514", "signing_requests_nonce_check"},
		{"gas limit zero", signerMigrationInsertRequest,
			signerMigrationWithArg(requestProbe("req-g0", "att-g0", "auth-g0"), 11, "0"), "23514", "signing_requests_gas_limit_check"},
		{"chain id zero", signerMigrationInsertRequest,
			signerMigrationWithArg(requestProbe("req-c0", "att-c0", "auth-c0"), 4, int64(0)), "23514", "signing_requests_chain_id_check"},
		{"short sender", signerMigrationInsertRequest,
			signerMigrationWithArg(requestProbe("req-s1", "att-s1", "auth-s1"), 5, "0x"+strings.Repeat("1", 39)), "23514", "signing_requests_sender_check"},
		{"uppercase to_addr", signerMigrationInsertRequest,
			signerMigrationWithArg(requestProbe("req-t1", "att-t1", "auth-t1"), 8, "0x"+strings.Repeat("AB", 20)), "23514", "signing_requests_to_check"},
		{"short recipient", signerMigrationInsertRequest,
			signerMigrationWithArg(requestProbe("req-r1", "att-r1", "auth-r1"), 14, "0x"+strings.Repeat("2", 39)), "23514", "signing_requests_recipient_check"},
		{"uppercase asset", signerMigrationInsertRequest,
			signerMigrationWithArg(requestProbe("req-as1", "att-as1", "auth-as1"), 13, "0x"+strings.Repeat("CD", 20)), "23514", "signing_requests_asset_check"},
		{"bad content hash", signerMigrationInsertRequest,
			signerMigrationWithArg(requestProbe("req-h1", "att-h1", "auth-h1"), 16, "0xnothex"), "23514", "signing_requests_content_hash_check"},
		{"bad authorization fingerprint", signerMigrationInsertRequest,
			signerMigrationWithArg(requestProbe("req-f1", "att-f1", "auth-f1"), 18, "not-a-fingerprint"), "23514", "signing_requests_authorization_fingerprint_check"},
		{"authorization version below one", signerMigrationInsertRequest,
			signerMigrationWithArg(requestProbe("req-av0", "att-av0", "auth-av0"), 19, int64(0)), "23514", "signing_requests_authorization_version_check"},
		{"eip1559 shape with gas_price", signerMigrationInsertRequest,
			signerMigrationWithArg(signerMigrationWithArg(requestProbe("req-f2", "att-f2", "auth-f2"), 7, 2), 12, nil), "23514", "signing_requests_fee_shape_check"},
		{"unknown tx type", signerMigrationInsertRequest,
			signerMigrationWithArg(signerMigrationWithArg(requestProbe("req-f9", "att-f9", "auth-f9"), 7, 9), 12, nil), "23514", "signing_requests_fee_shape_check"},
		{"duplicate signature result", insertResult,
			[]any{anchorID, signerMigrationSignature("cd"), signerMigrationHash("e2")}, "23505", "signature_results_pkey"},
		{"duplicate tx hash", insertResult,
			[]any{secondID, signerMigrationSignature("ef"), signerMigrationHash("e1")}, "23505", "signature_results_tx_hash_uniq"},
		{"short signature", insertResult,
			[]any{secondID, "0xdead", signerMigrationHash("e3")}, "23514", "signature_results_signature_check"},
		{"bad tx hash", insertResult,
			[]any{secondID, signerMigrationSignature("12"), "0xnothex"}, "23514", "signature_results_tx_hash_check"},
		{"signature for unknown request", insertResult,
			[]any{int64(999999), signerMigrationSignature("34"), signerMigrationHash("e4")}, "23503", "signature_results_request_fkey"},
		{"duplicate delivery attempt", insertAdmission,
			[]any{anchorID, 1, "admitted", signerMigrationFingerprint("de"), ""}, "23505", "delivery_admissions_request_attempt_uniq"},
		{"delivery attempt seq zero", insertAdmission,
			[]any{anchorID, 0, "admitted", signerMigrationFingerprint("df"), ""}, "23514", "delivery_admissions_attempt_seq_check"},
		{"unknown delivery verdict", insertAdmission,
			[]any{anchorID, 2, "shrug", signerMigrationFingerprint("d0"), ""}, "23514", "delivery_admissions_verdict_check"},
		{"unknown binding class", insertAdmission,
			[]any{anchorID, 3, "admitted", signerMigrationFingerprint("d1"), "guessing"}, "23514", "delivery_admissions_binding_class_check"},
		{"delivery for unknown request", insertAdmission,
			[]any{int64(999999), 1, "admitted", signerMigrationFingerprint("d2"), ""}, "23503", "delivery_admissions_request_fkey"},
		{"unknown audit action", insertAudit,
			[]any{"req-1", int64(1001), "teleported"}, "23514", "signing_request_audit_action_check"},
	}
	for _, tc := range violations {
		t.Run(tc.name, func(t *testing.T) {
			_, err := sqlDB.ExecContext(ctx, tc.query, tc.args...)
			signerMigrationWantPgError(t, err, tc.code, tc.constraint)
		})
	}

	// UPDATE-time CHECK probes: mutating the anchor row is refused and the row
	// keeps its persisted values (a true rejection, not a silent rewrite).
	updates := []struct {
		name       string
		query      string
		code       string
		constraint string
	}{
		{"negative recovery version", `UPDATE signing_requests SET recovery_version = -1 WHERE id = $1`,
			"23514", "signing_requests_recovery_version_check"},
		{"authorization version below one", `UPDATE signing_requests SET authorization_version = 0 WHERE id = $1`,
			"23514", "signing_requests_authorization_version_check"},
		{"state outside closed set", `UPDATE signing_requests SET state = 'bogus' WHERE id = $1`,
			"23514", "signing_requests_state_check"},
		{"non-empty access list", `UPDATE signing_requests SET access_list = '[{"k":1}]'::jsonb WHERE id = $1`,
			"23514", "signing_requests_access_list_check"},
	}
	for _, tc := range updates {
		t.Run(tc.name, func(t *testing.T) {
			_, err := sqlDB.ExecContext(ctx, tc.query, anchorID)
			signerMigrationWantPgError(t, err, tc.code, tc.constraint)
		})
	}
	var state, accessList string
	var recoveryVersion int64
	if err := sqlDB.QueryRowContext(ctx,
		`SELECT state, recovery_version, access_list::text FROM signing_requests WHERE id = $1`,
		anchorID).Scan(&state, &recoveryVersion, &accessList); err != nil {
		t.Fatalf("re-read anchor row: %v", err)
	}
	if state != "received" || recoveryVersion != 0 || accessList != "[]" {
		t.Fatalf("anchor row rewritten by failed UPDATE probes: state=%q recovery_version=%d access_list=%q",
			state, recoveryVersion, accessList)
	}

	// Positive counterparts: the constraints above must not over-reject.
	positives := []struct {
		name  string
		query string
		args  []any
	}{
		{"replacement shares the anchor grant", signerMigrationInsertRequest,
			signerMigrationWithArg(requestProbe("req-repl", "att-repl", "auth-anchor-1"), 3, anchorID)},
		{"distinct caller accepted", insertCaller,
			[]any{int64(1002), "second"}},
		{"second credential per caller", insertCredential,
			[]any{int64(1001), signerMigrationSecret("b"), "txh_beta"}},
		{"max uint256 amount accepted", signerMigrationInsertRequest,
			signerMigrationWithArg(requestProbe("req-max", "att-max", "auth-max"), 15, signerMaxUint256)},
		{"no scope observed is a NULL version, not a default", signerMigrationInsertRequest,
			signerMigrationWithArg(requestProbe("req-avn", "att-avn", "auth-avn"), 19, nil)},
		{"audit outlives a never-created request", insertAudit,
			[]any{"rej-never-created", int64(1001), "gate_refused"}},
	}
	for _, tc := range positives {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := sqlDB.ExecContext(ctx, tc.query, tc.args...); err != nil {
				t.Fatalf("expected insert to succeed: %v", err)
			}
		})
	}
}

// TestSignerMigrationNoTTLColumns pins the withdrawn TTL revision and the
// declared table shapes: none of the six 009 tables may carry a TTL/grace
// column, delivery_admissions keeps its exact 15-column shape, and (T037)
// signing_requests keeps its exact 34-column shape with authorization_version
// a nullable BIGINT and no default.
func TestSignerMigrationNoTTLColumns(t *testing.T) {
	dsn := signerMigrationStartPostgres(t)
	sqlDB := signerMigrationMigrateUp(t, dsn)

	var forbidden int
	if err := sqlDB.QueryRowContext(context.Background(), `
		SELECT count(*) FROM information_schema.columns
		WHERE table_schema = 'public'
		  AND table_name IN ('signer_caller', 'signer_credential', 'signing_requests',
		                     'signature_results', 'signing_request_audit', 'delivery_admissions')
		  AND (column_name LIKE '%valid%until%' OR column_name LIKE '%ttl%' OR column_name LIKE '%grace%')
	`).Scan(&forbidden); err != nil {
		t.Fatalf("probe information_schema.columns: %v", err)
	}
	if forbidden != 0 {
		t.Fatalf("009 tables carry %d TTL/grace column(s); the withdrawn revision MUST NOT return", forbidden)
	}

	var admissionColumns int
	if err := sqlDB.QueryRowContext(context.Background(), `
		SELECT count(*) FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'delivery_admissions'
	`).Scan(&admissionColumns); err != nil {
		t.Fatalf("count delivery_admissions columns: %v", err)
	}
	if admissionColumns != 15 {
		t.Fatalf("delivery_admissions column count = %d, want exactly 15", admissionColumns)
	}

	// T037 storage shape: signing_requests gains the nullable submit-time
	// scope-version snapshot (NULL = no scope observed, never a defaulted
	// version), so the column set is pinned exactly.
	var signingRequestColumns int
	if err := sqlDB.QueryRowContext(context.Background(), `
		SELECT count(*) FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'signing_requests'
	`).Scan(&signingRequestColumns); err != nil {
		t.Fatalf("count signing_requests columns: %v", err)
	}
	if signingRequestColumns != 34 {
		t.Fatalf("signing_requests column count = %d, want exactly 34", signingRequestColumns)
	}
	var versionType, versionNullable, versionDefault string
	if err := sqlDB.QueryRowContext(context.Background(), `
		SELECT data_type, is_nullable, COALESCE(column_default, '')
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'signing_requests'
		  AND column_name = 'authorization_version'`).Scan(&versionType, &versionNullable, &versionDefault); err != nil {
		t.Fatalf("probe signing_requests.authorization_version: %v", err)
	}
	if versionType != "bigint" || versionNullable != "YES" || versionDefault != "" {
		t.Fatalf("authorization_version = %s nullable=%s default=%q, want a nullable bigint with no default",
			versionType, versionNullable, versionDefault)
	}
}
