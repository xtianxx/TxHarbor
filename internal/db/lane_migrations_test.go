package db

import (
	"bytes"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestLaneMigrationsCarryReal009 pins the 009-lane production/test-set
// isolation rule (retargeted 2026-09-17 from the PB-lane "exclude 009" guard:
// this lane legitimately OWNS 000009, so exclusion is the wrong invariant
// here; the main-side exclusion is restored when 009 merges).
//
// Production/test-set boundary, all asserted in the no-Docker unit run:
//  1. the embedded 000009 is byte-identical to the lane's real migration file
//     (no fixture copy may leak into the production set);
//  2. no "testdata" path segment exists at ANY depth of the embedded FS
//     (WalkDir over the whole tree, not just a root-level *.sql glob);
//  3. the full embedded chain still declares versions 1..10 with the 9-gap
//     fillable (chain shape itself is owned by TestSignerMigrationMergedChain
//     and the scratch overlay tests; here only the set boundary is pinned).
func TestLaneMigrationsCarryReal009(t *testing.T) {
	assertNoTestdata(t, Migrations)
	want, err := os.ReadFile(filepath.Join("..", "..", "migrations", "000009_signer_service.sql"))
	if err != nil {
		t.Fatalf("read lane migration 000009: %v", err)
	}
	got, err := fs.ReadFile(Migrations, "000009_signer_service.sql")
	if err != nil {
		t.Fatalf("read embedded 000009: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("embedded 000009 diverges from migrations/000009_signer_service.sql")
	}
}

// assertNoTestdata walks the whole fsys tree and fails on any "testdata"
// path segment at any depth, so a fixture copy cannot hide below the glob.
func assertNoTestdata(t *testing.T, fsys fs.FS) {
	t.Helper()
	err := fs.WalkDir(fsys, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		for _, seg := range strings.Split(path, "/") {
			if seg == "testdata" {
				t.Errorf("embedded migrations must not contain testdata, found %q", path)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk embedded migrations: %v", err)
	}
}
