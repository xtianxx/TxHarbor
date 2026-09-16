//go:build integration

// scratch_pb05_upgrade_integration_test.go owns spec task T036
// (012-007-authorization-carrier): PB-05 upgrade verification, sequences
//
//	(a) empty -> full chain (000001..000009 + carrier 000010)
//	(b) 007-era (000001..000007, pre-PB) -> carrier upgrade
//	(c) carrier down -> re-up (rollback / re-upgrade order asserted)
//
// Sequence (d), the 009 gap-fill, is proven by TestT041GapFillSequenceD and is
// deliberately not repeated here.
//
// Scratch PostgreSQL only (testcontainers), one container per sequence. The
// T040 harness helpers are reused verbatim — startPostgres, testMigrateOptions,
// pb009Overlay, appliedContains, CheckCompatibility — so the overlay logic
// lives in exactly one place. The lane tree never carries 009's file: it enters
// only through pb009Overlay's in-memory FS. Every claim below is a real runner
// run; code reading is not evidence.
package db

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

// t036Seed007Grant inserts a caller + one 007-era grant, so an upgrade can be
// shown to preserve pre-existing rows (PB-FR-06: additive-only DDL). The
// authorization id is the caller's chosen fixture id.
func t036Seed007Grant(t *testing.T, dsn, authorizationID string) {
	t.Helper()
	sqlDB := openTestSQL(t, dsn)
	mustExec(t, sqlDB, `INSERT INTO caller (caller_id, label) VALUES (1, 'ops')`)
	mustExec(t, sqlDB, `INSERT INTO withdrawal_authorizations
		(authorization_id, caller_id, chain_id, asset, recipient, amount, state, supplied_by)
		VALUES ($1, 1, 1, $2, $3, 100, 'active', 'principal')`,
		authorizationID, addr40("aa"), addr40("bb"))
}

// t036AssertGrantIntact reads the fixture grant back and fails if the upgrade
// changed its business content.
func t036AssertGrantIntact(t *testing.T, dsn, authorizationID string) {
	t.Helper()
	sqlDB := openTestSQL(t, dsn)
	var (
		callerID   int64
		chainID    int64
		state      string
		suppliedBy string
	)
	if err := sqlDB.QueryRowContext(context.Background(),
		`SELECT caller_id, chain_id, state, supplied_by FROM withdrawal_authorizations
		 WHERE authorization_id = $1`, authorizationID).
		Scan(&callerID, &chainID, &state, &suppliedBy); err != nil {
		t.Fatalf("read back grant %q after upgrade: %v", authorizationID, err)
	}
	if callerID != 1 || chainID != 1 || state != "active" || suppliedBy != "principal" {
		t.Fatalf("grant %q mutated by upgrade: caller=%d chain=%d state=%q supplied_by=%q",
			authorizationID, callerID, chainID, state, suppliedBy)
	}
}

// TestT036SequenceAEmptyToFull: a fresh scratch database plus the full chain
// {000001..000010} (carrier 000010 + the scratch 009 file) migrates from empty,
// serves, and a repeat `migrate up` is a no-op — applied numbers untouched.
func TestT036SequenceAEmptyToFull(t *testing.T) {
	dsn := startPostgres(t)
	ctx := context.Background()
	opts := testMigrateOptions(dsn)
	opts.FS = pb009Overlay(t, true) // lane {1..8,10} + scratch 009 => {1..10}

	var out bytes.Buffer
	if err := MigrateUp(ctx, opts, &out); err != nil {
		t.Fatalf("MigrateUp(full chain) error = %v (output %q)", err, out.String())
	}
	t.Logf("T036 sequence (a) empty->full: %s", strings.TrimSpace(out.String()))
	if want := "applied=10 skipped=0 pending=0"; !strings.Contains(out.String(), want) {
		t.Fatalf("MigrateUp(full chain) output = %q, want %q", out.String(), want)
	}

	state, err := Inspect(ctx, opts)
	if err != nil {
		t.Fatalf("Inspect() error = %v", err)
	}
	appliedContains(t, state.Applied, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	if state.Current != 10 || len(state.Pending) != 0 {
		t.Fatalf("schema state = current %d pending %v, want 10/none", state.Current, state.Pending)
	}
	compat, err := CheckCompatibility(ctx, opts)
	if err != nil {
		t.Fatalf("CheckCompatibility(full chain) = %v", err)
	}
	t.Logf("T036 sequence (a) serve gate green: current=%d target=%d pending=%d",
		compat.Current, compat.Target, compat.Pending)

	sqlDB := openTestSQL(t, dsn)
	for _, rel := range []string{
		"withdrawal_authorizations",         // 007
		"withdrawal_authorization_scopes",   // PB carrier 000010
		"signer_caller", "signing_requests", // 009
	} {
		if !relationExists(t, sqlDB, rel) {
			t.Errorf("%s missing after the full chain", rel)
		}
	}

	// Re-run: nothing pending, no number rewritten, no side effect.
	out.Reset()
	if err := MigrateUp(ctx, opts, &out); err != nil {
		t.Fatalf("repeat MigrateUp() error = %v (output %q)", err, out.String())
	}
	t.Logf("T036 sequence (a) repeat migrate up: %s", strings.TrimSpace(out.String()))
	if want := "applied=0 skipped=10 pending=0"; !strings.Contains(out.String(), want) {
		t.Fatalf("repeat MigrateUp() output = %q, want %q", out.String(), want)
	}
	state, err = Inspect(ctx, opts)
	if err != nil {
		t.Fatalf("Inspect() after repeat error = %v", err)
	}
	appliedContains(t, state.Applied, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	t.Logf("T036 sequence (a) applied numbers untouched: %v", state.Applied)
}

// TestT036SequenceB007EraToCarrier: a scratch database at the 007-era schema
// {000001..000007} upgrades to the PB carrier. 009 is absent from the target
// set, proving PB merges and serves without 009 (R-PB8 independence); the
// pre-existing 007 grant survives byte-for-row and no scope is backfilled.
func TestT036SequenceB007EraToCarrier(t *testing.T) {
	dsn := startPostgres(t)
	ctx := context.Background()

	// Build the 007-era schema: lane files {1..7}, i.e. pre-carrier and
	// pre-009 (omit 8 and 10 to materialise the era set in the overlay FS).
	era := testMigrateOptions(dsn)
	era.FS = pb009Overlay(t, false, 8, 10)
	var out bytes.Buffer
	if err := MigrateUp(ctx, era, &out); err != nil {
		t.Fatalf("MigrateUp(007-era) error = %v (output %q)", err, out.String())
	}
	t.Logf("T036 sequence (b) 007-era up: %s", strings.TrimSpace(out.String()))
	if want := "applied=7 skipped=0 pending=0"; !strings.Contains(out.String(), want) {
		t.Fatalf("MigrateUp(007-era) output = %q, want %q", out.String(), want)
	}
	state, err := Inspect(ctx, era)
	if err != nil {
		t.Fatalf("Inspect(007-era) error = %v", err)
	}
	appliedContains(t, state.Applied, 1, 2, 3, 4, 5, 6, 7)
	if state.Current != 7 {
		t.Fatalf("007-era current = %d, want 7", state.Current)
	}

	const fixtureID = "authz-007era"
	t036Seed007Grant(t, dsn, fixtureID)

	sqlDB := openTestSQL(t, dsn)
	if relationExists(t, sqlDB, "withdrawal_authorization_scopes") {
		t.Fatal("carrier table must not exist in the 007 era")
	}
	if relationExists(t, sqlDB, "signer_caller") {
		t.Fatal("009 signer table must not exist in the 007 era")
	}

	// Upgrade to the lane head {1..8,10}: applies 000008 and the carrier
	// 000010. 000009 is still unmerged (009 absent).
	carrier := testMigrateOptions(dsn)
	carrier.FS = pb009Overlay(t, false)
	out.Reset()
	if err := MigrateUp(ctx, carrier, &out); err != nil {
		t.Fatalf("MigrateUp(carrier) error = %v (output %q)", err, out.String())
	}
	t.Logf("T036 sequence (b) 007-era->carrier upgrade: %s", strings.TrimSpace(out.String()))
	if want := "applied=2 skipped=7 pending=0"; !strings.Contains(out.String(), want) {
		t.Fatalf("MigrateUp(carrier) output = %q, want %q", out.String(), want)
	}

	state, err = Inspect(ctx, carrier)
	if err != nil {
		t.Fatalf("Inspect(carrier) error = %v", err)
	}
	appliedContains(t, state.Applied, 1, 2, 3, 4, 5, 6, 7, 8, 10)
	if state.Current != 10 {
		t.Fatalf("post-carrier current = %d, want 10", state.Current)
	}
	compat, err := CheckCompatibility(ctx, carrier)
	if err != nil {
		t.Fatalf("CheckCompatibility(carrier) = %v", err)
	}
	t.Logf("T036 sequence (b) serve gate green (009 absent): current=%d target=%d pending=%d",
		compat.Current, compat.Target, compat.Pending)

	if !relationExists(t, sqlDB, "withdrawal_authorization_scopes") {
		t.Fatal("carrier table missing after the 007-era upgrade")
	}
	if relationExists(t, sqlDB, "signer_caller") {
		t.Fatal("009 signer table appeared without 009: PB must serve with 009 absent")
	}
	t036AssertGrantIntact(t, dsn, fixtureID)
	var scopes int
	if err := sqlDB.QueryRowContext(ctx,
		"SELECT count(*) FROM withdrawal_authorization_scopes").Scan(&scopes); err != nil {
		t.Fatalf("count carrier rows: %v", err)
	}
	if scopes != 0 {
		t.Fatalf("carrier upgrade must not backfill scopes, got %d row(s)", scopes)
	}
	t.Logf("T036 sequence (b) pre-existing 007 grant intact, no scope backfill")
}

// TestT036SequenceCDownThenReUp: from the full chain, `down` reverts the
// carrier 000010 before 000009 (applied-descending), then a plain `migrate up`
// re-applies exactly 000010 without rewriting any applied number. The 007 grant
// survives; 009 tables are untouched by the carrier's down.
func TestT036SequenceCDownThenReUp(t *testing.T) {
	dsn := startPostgres(t)
	ctx := context.Background()
	opts := testMigrateOptions(dsn)
	opts.FS = pb009Overlay(t, true)

	var out bytes.Buffer
	if err := MigrateUp(ctx, opts, &out); err != nil {
		t.Fatalf("MigrateUp(full chain) error = %v (output %q)", err, out.String())
	}
	stateBefore, err := Inspect(ctx, opts)
	if err != nil {
		t.Fatalf("Inspect(before) error = %v", err)
	}
	appliedContains(t, stateBefore.Applied, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

	const fixtureID = "authz-downup"
	t036Seed007Grant(t, dsn, fixtureID)

	sqlDB := openTestSQL(t, dsn)
	provider, err := newProvider(sqlDB, opts)
	if err != nil {
		t.Fatalf("newProvider: %v", err)
	}
	first, err := provider.Down(ctx)
	if err != nil {
		t.Fatalf("Down() error = %v", err)
	}
	if first.Source.Version != 10 {
		t.Fatalf("Down() reverted version %d, want 10 (applied-descending)", first.Source.Version)
	}
	t.Logf("T036 sequence (c) down reverted version %d (applied-descending)", first.Source.Version)
	if relationExists(t, sqlDB, "withdrawal_authorization_scopes") {
		t.Error("carrier table must be gone after 000010 DOWN")
	}
	if !relationExists(t, sqlDB, "signer_caller") {
		t.Error("009 signer table must survive the carrier DOWN (000009 still applied)")
	}

	// Re-up: exactly the pending carrier re-applies; applied numbers unchanged.
	out.Reset()
	if err := MigrateUp(ctx, opts, &out); err != nil {
		t.Fatalf("re-up MigrateUp() error = %v (output %q)", err, out.String())
	}
	t.Logf("T036 sequence (c) re-up after down: %s", strings.TrimSpace(out.String()))
	if want := "applied=1 skipped=9 pending=0"; !strings.Contains(out.String(), want) {
		t.Fatalf("re-up MigrateUp() output = %q, want %q", out.String(), want)
	}

	stateAfter, err := Inspect(ctx, opts)
	if err != nil {
		t.Fatalf("Inspect(after) error = %v", err)
	}
	appliedContains(t, stateAfter.Applied, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	t.Logf("T036 sequence (c) applied numbers unchanged after down->re-up: %v", stateAfter.Applied)

	compat, err := CheckCompatibility(ctx, opts)
	if err != nil {
		t.Fatalf("CheckCompatibility(after re-up) = %v", err)
	}
	t.Logf("T036 sequence (c) serve gate green after re-up: current=%d target=%d pending=%d",
		compat.Current, compat.Target, compat.Pending)
	if !relationExists(t, sqlDB, "withdrawal_authorization_scopes") {
		t.Fatal("carrier table missing after re-up")
	}
	t036AssertGrantIntact(t, dsn, fixtureID)
}
