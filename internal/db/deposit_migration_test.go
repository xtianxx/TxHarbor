package db

import (
	"io/fs"
	"strings"
	"testing"
)

// TestDepositMigrationFilePresent pins the 004 migration name and its goose
// transactional markers (the embedded FS is captured by migrations/embed.go).
func TestDepositMigrationFilePresent(t *testing.T) {
	files, err := MigrationFiles(Migrations)
	if err != nil {
		t.Fatalf("list embedded migrations: %v", err)
	}
	var found bool
	for _, f := range files {
		if f.Version == 4 {
			found = true
			if f.Name != "000004_deposit_detection.sql" {
				t.Fatalf("migration 4 name = %q, want 000004_deposit_detection.sql", f.Name)
			}
		}
	}
	if !found {
		t.Fatalf("migration 000004 missing from %v", files)
	}

	data, err := fs.ReadFile(Migrations, "000004_deposit_detection.sql")
	if err != nil {
		t.Fatalf("read 000004: %v", err)
	}
	for _, want := range []string{"-- +goose Up", "-- +goose Down"} {
		if !strings.Contains(string(data), want) {
			t.Errorf("000004 missing %q", want)
		}
	}
	if strings.Contains(string(data), "NO TRANSACTION") {
		t.Error("000004 must not disable goose transactions")
	}
}

// TestDepositMigrationShape locks the storage objects T001 requires: five
// tables, the pause identity sequence, and a DOWN that removes all of them
// table-first (the sequence is dropped after the table that defaults from it).
// The Up section is asserted append-only: no UPDATE/DELETE statements exist in
// the schema definition (history/audit rows are never rewritten by storage).
func TestDepositMigrationShape(t *testing.T) {
	data, err := fs.ReadFile(Migrations, "000004_deposit_detection.sql")
	if err != nil {
		t.Fatalf("read 000004: %v", err)
	}
	up, down, ok := strings.Cut(string(data), "-- +goose Down")
	if !ok {
		t.Fatal("000004 missing goose Down section")
	}

	tables := []string{
		"deposit_config_history",
		"deposit_observations",
		"deposit_checkpoint",
		"deposit_pause",
		"deposit_pause_audit",
	}
	for _, table := range tables {
		if !strings.Contains(up, "CREATE TABLE "+table+" ") {
			t.Errorf("Up section missing CREATE TABLE %s", table)
		}
		if !strings.Contains(down, "DROP TABLE IF EXISTS "+table+";") {
			t.Errorf("Down section missing DROP TABLE IF EXISTS %s", table)
		}
	}
	if !strings.Contains(up, "CREATE SEQUENCE deposit_pause_id_seq") {
		t.Error("Up section missing CREATE SEQUENCE deposit_pause_id_seq")
	}
	if !strings.Contains(up, "NO CYCLE") {
		t.Error("deposit_pause_id_seq must be NO CYCLE (identities are never reused)")
	}
	if !strings.Contains(down, "DROP SEQUENCE IF EXISTS deposit_pause_id_seq;") {
		t.Error("Down section missing DROP SEQUENCE IF EXISTS deposit_pause_id_seq")
	}

	// Observations reference history: the table must go before its FK target.
	obsDrop := strings.Index(down, "DROP TABLE IF EXISTS deposit_observations;")
	histDrop := strings.Index(down, "DROP TABLE IF EXISTS deposit_config_history;")
	if obsDrop < 0 || histDrop < 0 || obsDrop > histDrop {
		t.Error("Down must drop deposit_observations before deposit_config_history (FK order)")
	}
	// The sequence must go after the pause table whose default uses it.
	pauseDrop := strings.Index(down, "DROP TABLE IF EXISTS deposit_pause;")
	seqDrop := strings.Index(down, "DROP SEQUENCE IF EXISTS deposit_pause_id_seq;")
	if pauseDrop < 0 || seqDrop < 0 || pauseDrop > seqDrop {
		t.Error("Down must drop deposit_pause before its sequence")
	}

	for _, forbidden := range []string{"UPDATE ", "DELETE FROM"} {
		if strings.Contains(up, forbidden) {
			t.Errorf("Up section contains %q; the 004 schema is append-only", forbidden)
		}
	}
}
