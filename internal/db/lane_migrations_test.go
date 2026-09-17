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
//  2. nothing from internal/db/testdata is reachable through the embedded FS;
//  3. the full embedded chain still declares versions 1..10 with the 9-gap
//     fillable (chain shape itself is owned by TestSignerMigrationMergedChain
//     and the scratch overlay tests; here only the set boundary is pinned).
func TestLaneMigrationsCarryReal009(t *testing.T) {
	names, err := fs.Glob(Migrations, "*.sql")
	if err != nil {
		t.Fatalf("glob embedded migrations: %v", err)
	}
	for _, name := range names {
		if strings.Contains(name, "testdata") {
			t.Fatalf("embedded migrations must not contain testdata, found %q", name)
		}
	}
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
