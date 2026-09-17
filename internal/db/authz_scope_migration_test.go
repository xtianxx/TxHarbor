package db

import (
	"io/fs"
	"strings"
	"testing"
)

// TestAuthzScopeMigrationFilePresent pins the PB carrier migration name and its
// goose transactional markers (the embedded FS is captured by
// migrations/embed.go). The planning number 000010 is fixed here; renumbering
// happens only at merge (renumber-at-merge), so this name must survive until
// then.
func TestAuthzScopeMigrationFilePresent(t *testing.T) {
	files, err := MigrationFiles(Migrations)
	if err != nil {
		t.Fatalf("list embedded migrations: %v", err)
	}
	var found bool
	for _, f := range files {
		if f.Version == 10 {
			found = true
			if f.Name != "000010_withdrawal_authorization_scopes.sql" {
				t.Fatalf("migration 10 name = %q, want 000010_withdrawal_authorization_scopes.sql", f.Name)
			}
		}
	}
	if !found {
		t.Fatalf("migration 000010 missing from %v", files)
	}

	data, err := fs.ReadFile(Migrations, "000010_withdrawal_authorization_scopes.sql")
	if err != nil {
		t.Fatalf("read 000010: %v", err)
	}
	for _, want := range []string{"-- +goose Up", "-- +goose Down"} {
		if !strings.Contains(string(data), want) {
			t.Errorf("000010 missing %q", want)
		}
	}
	if strings.Contains(string(data), "NO TRANSACTION") {
		t.Error("000010 must not disable goose transactions")
	}
}

// TestAuthzScopeMigrationShape locks the carrier DDL surface fixed by
// specs/012-007-authorization-carrier/data-model.md (R-PB3/R-PB4): the ten
// columns, the explicitly named PK/FK, the fee triple with the frozen
// priority <= max_fee cross-check, version >= 1, the purpose boolean, and a
// Down that drops only this table. The Up section carries no data rewrites and
// no triggers; there is no signature column (Q-A).
func TestAuthzScopeMigrationShape(t *testing.T) {
	data, err := fs.ReadFile(Migrations, "000010_withdrawal_authorization_scopes.sql")
	if err != nil {
		t.Fatalf("read 000010: %v", err)
	}
	up, down, ok := strings.Cut(string(data), "-- +goose Down")
	if !ok {
		t.Fatal("000010 missing goose Down section")
	}

	for _, want := range []string{
		"CREATE TABLE withdrawal_authorization_scopes (",
		"authorization_id        TEXT    NOT NULL",
		"intent_id               TEXT    NOT NULL",
		"request_id              TEXT    NOT NULL",
		"sender                  TEXT    NOT NULL",
		"fee_max_total           BIGINT  NOT NULL",
		"fee_max_per_gas         BIGINT  NOT NULL",
		"fee_max_priority        BIGINT  NOT NULL",
		"allows_fee_replacement  BOOLEAN NOT NULL DEFAULT FALSE",
		"authorization_version   BIGINT  NOT NULL",
		"attested_by             TEXT    NOT NULL",
		"CONSTRAINT withdrawal_authorization_scopes_pkey PRIMARY KEY (authorization_id)",
		"CONSTRAINT withdrawal_authorization_scopes_authorization_fkey FOREIGN KEY (authorization_id)",
		"REFERENCES withdrawal_authorizations (authorization_id)",
		"CONSTRAINT withdrawal_authorization_scopes_sender_check CHECK (sender ~ '^0x[0-9a-f]{40}$')",
		"CONSTRAINT withdrawal_authorization_scopes_fee_max_total_check CHECK (fee_max_total >= 0)",
		"CONSTRAINT withdrawal_authorization_scopes_fee_max_per_gas_check CHECK (fee_max_per_gas >= 0)",
		"CONSTRAINT withdrawal_authorization_scopes_fee_max_priority_check CHECK (fee_max_priority >= 0)",
		"CONSTRAINT withdrawal_authorization_scopes_fee_priority_within_max_check CHECK (fee_max_priority <= fee_max_per_gas)",
		"CONSTRAINT withdrawal_authorization_scopes_authorization_version_check CHECK (authorization_version >= 1)",
	} {
		if !strings.Contains(up, want) {
			t.Errorf("Up section missing %q", want)
		}
	}
	// Q-A: no per-grant cryptography on this path. Scoped to the table body so
	// the header prose ("No signature column") does not trip the check.
	if _, table, found := strings.Cut(up, "CREATE TABLE withdrawal_authorization_scopes ("); found {
		body, _, _ := strings.Cut(table, ");")
		if strings.Contains(strings.ToLower(body), "signature") {
			t.Error("scope table must carry no signature column (Q-A)")
		}
	}
	for _, forbidden := range []string{"UPDATE ", "DELETE FROM", "TRIGGER"} {
		if strings.Contains(up, forbidden) {
			t.Errorf("Up section contains %q; the carrier DDL is pure CREATE TABLE", forbidden)
		}
	}

	if !strings.Contains(down, "DROP TABLE IF EXISTS withdrawal_authorization_scopes;") {
		t.Error("Down section missing DROP TABLE IF EXISTS withdrawal_authorization_scopes;")
	}
}
