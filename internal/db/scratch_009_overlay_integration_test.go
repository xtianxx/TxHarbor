//go:build integration

// T040/T041/T042 scratch harness for 009-file integration (tasks.md Phase 6;
// research R-PB8). It proves the PB carrier chain and 009's signer-service
// migration compose WITHOUT 009's file ever entering this lane's tree.
//
// SOURCE (T002 pin, testdata fixture): internal/db/testdata/
// 000009_signer_service.sql — a byte-exact read-only copy of the 009 lane at
// git 8f75450 (see testdata/README.md for provenance). Embedded, so a clean
// checkout runs with no outside-tree fixture (this replaced the /tmp/pb-009ref
// scratch dependency that broke remote CI on PR #11). The file's sha256 is
// asserted below, so any 009 re-pin is loud and forces a re-judge of T040/T041
// scope (T002).
//
// ISOLATION: one scratch PostgreSQL per sequence (testcontainers). The 009
// file is read from the scratch dir and copied into an in-memory migrations
// FS overlay (fstest.MapFS) — migrate.go's fs.FS seam (MigrateOptions.FS) is
// what makes that possible. The lane's embedded FS is asserted 009-free by
// TestLaneMigrationsCarryReal009 (runs without Docker).
//
// OVERLAY FORMAT consumed by sibling verification (T036, PB-05 sequences):
//
//	fsys := pb009HistoricOverlay(t, true, pb009HistoricLane...) // {1..10}
//	opts := testMigrateOptions(dsn)
//	opts.FS = fsys
//	db.MigrateUp(ctx, opts, &out)                 // on an empty DB: applied=10
//
// pb009Overlay(t, ...) is the unpinned overlay built from whatever the
// embedded lane set currently carries (the joint lane's {1..14}); omit lets a
// caller simulate a renumber-at-merge that removes an applied number's file
// (T042's down tests, which exercise the current joint head). PB-history
// sequences that assert an era-exact applied count pin the historical set
// through pb009HistoricOverlay instead.
package db

import (
	"bytes"
	"context"
	"crypto/sha256"
	_ "embed"
	"fmt"
	"io/fs"
	"strconv"
	"strings"
	"testing"
	"testing/fstest"
	"time"
)

const (
	pb009FileName     = "000009_signer_service.sql"
	pb009Version      = 9
	pb009PinnedGitSHA = "9f029ee"
	pb009FileSHA256   = "cd77bffd2b434f0dcd0bf8bf63e169b1e2db75bcf5beda8edc05f0901b1ae859"
)

// pb009Fixture is the pinned 009 migration, embedded from testdata so tests
// run on a clean checkout with no external fixture (testdata/README.md).
//
//go:embed testdata/000009_signer_service.sql
var pb009Fixture []byte

// pb009File returns the pinned 009 file bytes and asserts the pinned content
// hash. A mismatch means 009 advanced past the pin: re-pin the testdata copy
// and re-judge T040/T041 (T002), never edit this lane to make it pass.
func pb009File(t *testing.T) []byte {
	t.Helper()
	data := bytes.Clone(pb009Fixture)
	sum := fmt.Sprintf("%x", sha256.Sum256(data))
	if sum != pb009FileSHA256 {
		t.Fatalf("009 %s sha256 = %s, want pinned %s (git %s); 009 re-pinned -> re-judge T040/T041 scope (T002)",
			pb009FileName, sum, pb009FileSHA256, pb009PinnedGitSHA)
	}
	return data
}

func laneVersion(t *testing.T, name string) int64 {
	t.Helper()
	i := strings.IndexByte(name, '_')
	if i <= 0 {
		t.Fatalf("lane migration %q lacks the NNNNNN_ prefix", name)
	}
	v, err := strconv.ParseInt(name[:i], 10, 64)
	if err != nil {
		t.Fatalf("lane migration %q version prefix: %v", name, err)
	}
	return v
}

// pb009Overlay builds the in-memory migrations FS from the embedded lane set:
// every embedded file minus omit, with any embedded 000009 dropped — 9 is the
// signer lane's own number and enters this PB harness only through the pinned
// fixture. On the joint branch the embedded set is {1..15}, so callers that
// need the PB/009-era set pin it with pb009HistoricOverlay; pb009Overlay is the
// unpinned set T042's down tests exercise. omit drops further lane versions
// (renumber simulation). The lane tree and the embedded FS are never touched.
func pb009Overlay(t *testing.T, include009 bool, omit ...int64) fstest.MapFS {
	t.Helper()
	names, err := fs.Glob(Migrations, "*.sql")
	if err != nil {
		t.Fatalf("glob lane migrations: %v", err)
	}
	drop := make(map[int64]bool, len(omit))
	for _, v := range omit {
		drop[v] = true
	}
	fsys := make(fstest.MapFS, len(names)+1)
	for _, name := range names {
		version := laneVersion(t, name)
		if drop[version] || version == pb009Version {
			continue
		}
		data, err := fs.ReadFile(Migrations, name)
		if err != nil {
			t.Fatalf("read lane migration %s: %v", name, err)
		}
		fsys[name] = &fstest.MapFile{Data: data}
	}
	if include009 {
		fsys[pb009FileName] = &fstest.MapFile{Data: pb009File(t)}
	}
	return fsys
}

// pb009HistoricLane is the frozen lane set the PB/009-history sequences were
// written against: main's {1..8,10}. The signer lane's own 9 enters only as
// the scratch fixture (include009), never from the embedded set.
var pb009HistoricLane = []int64{1, 2, 3, 4, 5, 6, 7, 8, 10}

// pb009HistoricOverlay builds the overlay FS from exactly the named historical
// lane versions, never from whatever the embedded joint set currently carries.
// It is the pin the T036/T040/T041 sequences use: their claims (007-era ->
// carrier, empty -> full, the 009 gap-fill, overlay-green) are exact over the
// {1..10} era set, so joint-lane migrations 000011+ must not leak in and shift
// the applied counts. The scratch 009 fixture still enters only through
// include009, and a named version missing from the lane FS fails here, loudly.
func pb009HistoricOverlay(t *testing.T, include009 bool, versions ...int64) fstest.MapFS {
	t.Helper()
	files, err := MigrationFiles(Migrations)
	if err != nil {
		t.Fatalf("list lane migrations: %v", err)
	}
	want := make(map[int64]bool, len(versions))
	for _, v := range versions {
		want[v] = true
	}
	fsys := make(fstest.MapFS, len(versions)+1)
	for _, f := range files {
		if f.Version == pb009Version || !want[f.Version] {
			continue
		}
		data, err := fs.ReadFile(Migrations, f.Name)
		if err != nil {
			t.Fatalf("read lane migration %s: %v", f.Name, err)
		}
		fsys[f.Name] = &fstest.MapFile{Data: data}
		delete(want, f.Version)
	}
	if len(want) != 0 {
		t.Fatalf("historically pinned lane versions missing from the embedded FS: %v", want)
	}
	if include009 {
		fsys[pb009FileName] = &fstest.MapFile{Data: pb009File(t)}
	}
	return fsys
}

// serveGateDryCheck mirrors internal/app/serve.go's startup schema gate
// (serve.go:125-136): the serve gate IS db.CheckCompatibility with the
// serve-shaped MigrateOptions, no listener and no RPC. It is a dry check of
// that exact call — importing package app here would be an import cycle, and
// the real binary's embedded FS cannot carry the 009 overlay (which is the
// point of the overlay).
func serveGateDryCheck(ctx context.Context, opts MigrateOptions) error {
	gateCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := CheckCompatibility(gateCtx, opts)
	return err
}

// appliedContains reports whether the applied set holds every wanted version
// and nothing else.
func appliedContains(t *testing.T, applied []int64, want ...int64) {
	t.Helper()
	if len(applied) != len(want) {
		t.Fatalf("applied versions = %v, want %v", applied, want)
	}
	for i, v := range want {
		if applied[i] != v {
			t.Fatalf("applied versions = %v, want %v", applied, want)
		}
	}
}

// TestT040OverlayGreenOnEmptySequence is T040's "green on empty sequence": a
// fresh scratch PostgreSQL plus the pinned PB/009-era overlay {1..10}
// (pb009HistoricLane + scratch 009; joint-lane 000011+ postdates this era and
// is deliberately outside the sequence) migrates to 10 and serves
// (CheckCompatibility green) with both the 007 carrier and the 009 signer
// tables in place. Real container, real runner.
func TestT040OverlayGreenOnEmptySequence(t *testing.T) {
	dsn := startPostgres(t)
	ctx := context.Background()
	opts := testMigrateOptions(dsn)
	opts.FS = pb009HistoricOverlay(t, true, pb009HistoricLane...) // pinned {1..10}

	files, err := MigrationFiles(opts.FS)
	if err != nil {
		t.Fatalf("list overlay migrations: %v", err)
	}
	if len(files) != 10 || files[9].Version != 10 {
		t.Fatalf("overlay = %v, want 10 files ending at 10", files)
	}
	var hasNine bool
	for _, f := range files {
		if f.Version == 9 {
			hasNine = true
		}
	}
	if !hasNine {
		t.Fatal("overlay must carry the scratch 009 file")
	}

	var out bytes.Buffer
	if err := MigrateUp(ctx, opts, &out); err != nil {
		t.Fatalf("MigrateUp(overlay) error = %v (output %q)", err, out.String())
	}
	t.Logf("T040 sequence (a) empty->full: %s", strings.TrimSpace(out.String()))
	if want := fmt.Sprintf("applied=%d skipped=0 pending=0", len(files)); !strings.Contains(out.String(), want) {
		t.Fatalf("MigrateUp(overlay) output = %q, want %q", out.String(), want)
	}

	state, err := Inspect(ctx, opts)
	if err != nil {
		t.Fatalf("Inspect() error = %v", err)
	}
	if state.Current != 10 || len(state.Pending) != 0 {
		t.Fatalf("schema state = current %d pending %v, want 10/none", state.Current, state.Pending)
	}
	if _, err := CheckCompatibility(ctx, opts); err != nil {
		t.Fatalf("CheckCompatibility() after overlay migrate = %v", err)
	}

	sqlDB := openTestSQL(t, dsn)
	for _, rel := range []string{"withdrawal_authorization_scopes", "signer_caller", "signing_requests"} {
		if !relationExists(t, sqlDB, rel) {
			t.Errorf("%s missing after overlay migration", rel)
		}
	}
}

// TestT041GapFillSequenceD is PB-05 sequence (d): a PB-only database at the
// pinned historical {1..8,10} plus the pinned {1..10} must, on plain
// `migrate up`, apply exactly version 9; the serve gate must refuse before
// that and accept after. Both gate outputs are logged (T041 log pointers live
// outside the repo). The historical pin keeps the gap-fill arithmetic exact
// over the 009 era (joint-lane 000011+ is outside this sequence).
func TestT041GapFillSequenceD(t *testing.T) {
	dsn := startPostgres(t)
	ctx := context.Background()
	preMerge := testMigrateOptions(dsn)
	preMerge.FS = pb009HistoricOverlay(t, false, pb009HistoricLane...) // lane {1..8,10}: 9 has not merged
	postMerge := testMigrateOptions(dsn)
	postMerge.FS = pb009HistoricOverlay(t, true, pb009HistoricLane...) // files {1..10}: 009 has landed

	// Phase 1: an already-served PB database at {1..8,10}.
	var out bytes.Buffer
	if err := MigrateUp(ctx, preMerge, &out); err != nil {
		t.Fatalf("MigrateUp(pre-merge) error = %v (output %q)", err, out.String())
	}
	t.Logf("T041 phase 1 (PB before 009 merge): %s", strings.TrimSpace(out.String()))
	state, err := Inspect(ctx, preMerge)
	if err != nil {
		t.Fatalf("Inspect(pre-merge) error = %v", err)
	}
	appliedContains(t, state.Applied, 1, 2, 3, 4, 5, 6, 7, 8, 10)
	if state.Current != 10 {
		t.Fatalf("pre-merge current = %d, want 10", state.Current)
	}

	// Pre-fill serve gate must be RED: 9 is pending, serve refuses.
	err = serveGateDryCheck(ctx, postMerge)
	if err == nil {
		t.Fatal("pre-fill serve gate: expected refusal while version 9 is pending")
	}
	if _, rawErr := CheckCompatibility(ctx, postMerge); rawErr == nil {
		t.Fatal("pre-fill CheckCompatibility: expected pending-migration refusal")
	} else if !strings.Contains(rawErr.Error(), "pending") || !strings.Contains(rawErr.Error(), "migrate up") {
		t.Fatalf("pre-fill refusal lacks actionable text: %v", rawErr)
	}
	t.Logf("T041 pre-fill serve gate RED (expected): %s", strings.TrimSpace(err.Error()))

	// Gap-fill: plain migrate up with files {1..10} applies exactly 9.
	out.Reset()
	if err := MigrateUp(ctx, postMerge, &out); err != nil {
		t.Fatalf("gap-fill MigrateUp() error = %v (output %q)", err, out.String())
	}
	t.Logf("T041 gap-fill migrate up: %s", strings.TrimSpace(out.String()))
	if !strings.Contains(out.String(), "applied=1 skipped=9 pending=0") {
		t.Fatalf("gap-fill output = %q, want applied=1 skipped=9 pending=0", out.String())
	}

	// Post-fill: CheckCompatibility green and the serve-gate dry check green.
	compat, err := CheckCompatibility(ctx, postMerge)
	if err != nil {
		t.Fatalf("post-fill CheckCompatibility() = %v", err)
	}
	if compat.Current != 10 || compat.Target != 10 || compat.Pending != 0 {
		t.Fatalf("post-fill compatibility = %+v, want current=10 target=10 pending=0", compat)
	}
	t.Logf("T041 post-fill CheckCompatibility green: current=%d target=%d pending=%d",
		compat.Current, compat.Target, compat.Pending)
	if err := serveGateDryCheck(ctx, postMerge); err != nil {
		t.Fatalf("post-fill serve-gate dry check = %v", err)
	}
	t.Logf("T041 post-fill serve-gate dry check green")

	sqlDB := openTestSQL(t, dsn)
	if !relationExists(t, sqlDB, "signer_caller") {
		t.Fatal("signer_caller missing: version 9 did not apply")
	}
	for _, v := range []int64{9, 10} {
		var rows int
		if err := sqlDB.QueryRowContext(ctx,
			"SELECT count(*) FROM goose_db_version WHERE version_id = $1 AND is_applied", v).Scan(&rows); err != nil {
			t.Fatalf("count applied rows for version %d: %v", v, err)
		}
		if rows != 1 {
			t.Fatalf("goose_db_version applied rows for version %d = %d, want 1", v, rows)
		}
	}
}

// TestT042RollbackRevertsTenBeforeNine asserts the applied-descending rollback
// order from the full joint chain {1..15}: `down` walks the joint head
// 15,14,13,12,11 first, then reverts 10 before 9, dropping the carrier while the
// signer tables survive until 9's own down. The 10-down drops
// withdrawal_authorization_scopes AND its rows — that is the designed-for-
// scratch limit (T042): carrier rollback is never a production operation.
func TestT042RollbackRevertsTenBeforeNine(t *testing.T) {
	dsn := startPostgres(t)
	ctx := context.Background()
	opts := testMigrateOptions(dsn)
	opts.FS = pb009Overlay(t, true) // unpinned joint head {1..15}

	var out bytes.Buffer
	if err := MigrateUp(ctx, opts, &out); err != nil {
		t.Fatalf("MigrateUp() error = %v (output %q)", err, out.String())
	}
	sqlDB := openTestSQL(t, dsn)
	provider, err := newProvider(sqlDB, opts)
	if err != nil {
		t.Fatalf("newProvider: %v", err)
	}

	// The joint head reverts strictly descending (15 -> 11) before the PB
	// carrier reaches the 10-before-9 assertion: every applied number, exact.
	for _, want := range []int64{15, 14, 13, 12, 11} {
		result, err := provider.Down(ctx)
		if err != nil {
			t.Fatalf("Down() of version %d: %v", want, err)
		}
		if result.Source.Version != want {
			t.Fatalf("Down() reverted version %d, want %d (applied-descending)", result.Source.Version, want)
		}
	}

	first, err := provider.Down(ctx)
	if err != nil {
		t.Fatalf("first Down() error = %v", err)
	}
	if first.Source.Version != 10 {
		t.Fatalf("first Down() reverted version %d, want 10 (applied-descending)", first.Source.Version)
	}
	if relationExists(t, sqlDB, "withdrawal_authorization_scopes") {
		t.Error("carrier table still exists after 000010 DOWN")
	}
	if !relationExists(t, sqlDB, "signer_caller") {
		t.Error("signer_caller must survive 000010 DOWN (9 is still applied)")
	}

	second, err := provider.Down(ctx)
	if err != nil {
		t.Fatalf("second Down() error = %v", err)
	}
	if second.Source.Version != 9 {
		t.Fatalf("second Down() reverted version %d, want 9", second.Source.Version)
	}
	if relationExists(t, sqlDB, "signer_caller") {
		t.Error("signer_caller still exists after 000009 DOWN")
	}
}

// TestT042DownOfAppliedThenRenumberedNumberForbidden pins the renumber rule:
// migration numbers are immutable once applied. Simulate a merge-time
// renumber that removes the applied head's file (FS {1..14} while the DB
// still has 15 applied): `down` must refuse rather than silently roll back a
// different version, the DB must be untouched, and the serve gate must refuse
// (unknown/newer applied version).
func TestT042DownOfAppliedThenRenumberedNumberForbidden(t *testing.T) {
	dsn := startPostgres(t)
	ctx := context.Background()
	applied := testMigrateOptions(dsn)
	applied.FS = pb009Overlay(t, true)

	var out bytes.Buffer
	if err := MigrateUp(ctx, applied, &out); err != nil {
		t.Fatalf("MigrateUp() error = %v (output %q)", err, out.String())
	}
	sqlDB := openTestSQL(t, dsn)

	renumbered := testMigrateOptions(dsn)
	renumbered.FS = pb009Overlay(t, true, 15) // applied head 15's file is gone
	files, err := MigrationFiles(renumbered.FS)
	if err != nil {
		t.Fatalf("list renumbered migrations: %v", err)
	}
	if got := files[len(files)-1].Version; got != 14 {
		t.Fatalf("renumbered FS target = %d, want 14", got)
	}

	provider, err := newProvider(sqlDB, renumbered)
	if err != nil {
		t.Fatalf("newProvider(renumbered): %v", err)
	}
	if _, err := provider.Down(ctx); err == nil {
		t.Fatal("down of an applied-then-renumbered number must be forbidden")
	} else {
		t.Logf("T042 renumber guard: down refused as designed: %s", strings.TrimSpace(err.Error()))
	}

	if !relationExists(t, sqlDB, "withdrawal_authorization_scopes") {
		t.Error("carrier table lost by a forbidden down")
	}
	state, err := Inspect(ctx, applied)
	if err != nil {
		t.Fatalf("Inspect(applied) error = %v", err)
	}
	if state.Current != 15 {
		t.Fatalf("applied state changed by a forbidden down: current = %d, want 15", state.Current)
	}
	if _, err := CheckCompatibility(ctx, renumbered); err == nil {
		t.Fatal("serve gate must refuse an applied version with no resolvable file")
	} else if !strings.Contains(err.Error(), "newer") && !strings.Contains(err.Error(), "not a known migration") {
		t.Fatalf("renumber refusal lacks actionable text: %v", err)
	}
}
