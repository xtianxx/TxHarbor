package db

import (
	"io/fs"
	"strings"
	"testing"
)

// TestLaneMigrationsExclude009 pins the migration-chain isolation rule of
// specs/012-007-authorization-carrier tasks T002/T040: this PB lane must never
// carry 009's signer-service migration. 009's file is pinned as a test-only
// fixture (internal/db/testdata, git 8f75450; see testdata/README.md) and is
// only ever overlaid in memory by the T040 harness; a stray 000009 in the lane tree would both violate T002 and
// fake the gap-fill proof (the pending version would stop being pending), so
// it fails here in the no-Docker unit run too.
//
// Sibling: TestMigrateGapFillSequenceD (integration) proves the gap closes
// when the 009 file is present in the overlay.
func TestLaneMigrationsExclude009(t *testing.T) {
	names, err := fs.Glob(Migrations, "*.sql")
	if err != nil {
		t.Fatalf("glob embedded migrations: %v", err)
	}
	for _, name := range names {
		if strings.HasPrefix(name, "000009") {
			t.Fatalf("lane embedded migrations must not contain 009's file, found %q", name)
		}
	}
	files, err := MigrationFiles(Migrations)
	if err != nil {
		t.Fatalf("list embedded migrations: %v", err)
	}
	for _, f := range files {
		if f.Version == 9 {
			t.Fatalf("lane embedded migrations must not declare version 9, found %s", f.Name)
		}
	}
}
