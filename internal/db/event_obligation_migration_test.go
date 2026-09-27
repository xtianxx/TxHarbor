package db

import (
	"io/fs"
	"strings"
	"testing"
)

// TestEventObligationMigrationFilePresent pins the T040 expectation-carrier
// migration name and its goose markers. The carrier is additive: version 17
// follows the joint tip 000016 and is never renumbered into an applied file.
func TestEventObligationMigrationFilePresent(t *testing.T) {
	files, err := MigrationFiles(Migrations)
	if err != nil {
		t.Fatalf("list embedded migrations: %v", err)
	}
	var found bool
	for _, f := range files {
		if f.Version == 17 {
			found = true
			if f.Name != "000017_event_obligations.sql" {
				t.Fatalf("migration 17 name = %q, want 000017_event_obligations.sql", f.Name)
			}
		}
	}
	if !found {
		t.Fatalf("migration 000017 missing from %v", files)
	}

	data, err := fs.ReadFile(Migrations, "000017_event_obligations.sql")
	if err != nil {
		t.Fatalf("read 000017: %v", err)
	}
	for _, want := range []string{"-- +goose Up", "-- +goose Down"} {
		if !strings.Contains(string(data), want) {
			t.Errorf("000017 missing %q", want)
		}
	}
	if strings.Contains(string(data), "NO TRANSACTION") {
		t.Error("000017 must not disable goose transactions")
	}
}

// TestEventObligationMigrationShape locks the carrier DDL surface of the T040
// expectation marker (expected-event-discriminator.md §3): the aggregate
// triple, expected_event_type, obligated_at and the producer source triple,
// the named identity UNIQUE, the closed (aggregate_type, expected_event_type)
// mapping, append-only inserts (no UPDATE/DELETE/TRIGGER), no seed and no
// backfill, and a Down that drops only this table.
func TestEventObligationMigrationShape(t *testing.T) {
	data, err := fs.ReadFile(Migrations, "000017_event_obligations.sql")
	if err != nil {
		t.Fatalf("read 000017: %v", err)
	}
	up, down, ok := strings.Cut(string(data), "-- +goose Down")
	if !ok {
		t.Fatal("000017 missing goose Down section")
	}

	for _, want := range []string{
		"CREATE TABLE event_obligation (",
		"aggregate_type      TEXT        NOT NULL",
		"aggregate_id        TEXT        NOT NULL",
		"aggregate_version   BIGINT      NOT NULL",
		"expected_event_type TEXT        NOT NULL",
		"obligated_at        TIMESTAMPTZ NOT NULL",
		"source_kind         TEXT        NOT NULL DEFAULT ''",
		"source_id           TEXT        NOT NULL DEFAULT ''",
		"source_version      BIGINT",
		"CONSTRAINT event_obligation_pkey PRIMARY KEY (obligation_id)",
		"CONSTRAINT event_obligation_identity_uniq UNIQUE (",
		"aggregate_type, aggregate_id, aggregate_version, expected_event_type)",
		"CONSTRAINT event_obligation_aggregate_version_check CHECK (aggregate_version > 0)",
		"CONSTRAINT event_obligation_source_version_check CHECK (",
		"CONSTRAINT event_obligation_mapping_check CHECK (",
		"'deposit.observation.created'",
		"'deposit.observation.status_changed'",
		"'deposit.observation.reinstated'",
		"'deposit.confirmation.confirmed'",
		"'deposit.revision.applied'",
		"'withdrawal.request.received'",
		"'withdrawal.execution.state_changed'",
		"'withdrawal.execution.revised'",
		"CREATE INDEX event_obligation_aggregate_idx",
		"CREATE INDEX event_obligation_obligated_at_idx",
	} {
		if !strings.Contains(up, want) {
			t.Errorf("Up section missing %q", want)
		}
	}
	for _, forbidden := range []string{"UPDATE ", "DELETE FROM", "TRIGGER", "INSERT INTO", "ALTER TABLE", "DROP TABLE"} {
		if strings.Contains(up, forbidden) {
			t.Errorf("Up section contains %q; the carrier DDL is pure CREATE TABLE/INDEX", forbidden)
		}
	}
	// Exactly one table is created: the carrier is pure additive DDL.
	if n := strings.Count(up, "CREATE TABLE "); n != 1 {
		t.Errorf("Up section creates %d tables, want exactly 1", n)
	}

	if !strings.Contains(down, "DROP TABLE IF EXISTS event_obligation;") {
		t.Error("Down section missing DROP TABLE IF EXISTS event_obligation;")
	}
	if strings.Count(down, "DROP ") != 1 {
		t.Error("Down section must drop exactly the carrier table")
	}
}
