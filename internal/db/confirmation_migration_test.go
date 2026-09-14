package db

import (
	"io/fs"
	"strings"
	"testing"
)

// TestConfirmationMigrationFilePresent pins the 005 migration name and its
// goose transactional markers (the embedded FS is captured by
// migrations/embed.go). T017 amends this same file later for the 2^63 audit
// path; the name and markers pinned here must survive that amendment.
func TestConfirmationMigrationFilePresent(t *testing.T) {
	files, err := MigrationFiles(Migrations)
	if err != nil {
		t.Fatalf("list embedded migrations: %v", err)
	}
	var found bool
	for _, f := range files {
		if f.Version == 5 {
			found = true
			if f.Name != "000005_confirmation_tracking.sql" {
				t.Fatalf("migration 5 name = %q, want 000005_confirmation_tracking.sql", f.Name)
			}
		}
	}
	if !found {
		t.Fatalf("migration 000005 missing from %v", files)
	}

	data, err := fs.ReadFile(Migrations, "000005_confirmation_tracking.sql")
	if err != nil {
		t.Fatalf("read 000005: %v", err)
	}
	for _, want := range []string{"-- +goose Up", "-- +goose Down"} {
		if !strings.Contains(string(data), want) {
			t.Errorf("000005 missing %q", want)
		}
	}
	if strings.Contains(string(data), "NO TRANSACTION") {
		t.Error("000005 must not disable goose transactions")
	}
}

// TestConfirmationMigrationShape locks the T001 DDL surface from data-model
// Tables 1-2: the policy history table (PK, threshold check, request
// uniqueness, bootstrap partial unique, self-referencing prev_seq), the six
// additive observation columns (NUMERIC confirmations per OI-1, no BIGINT
// alternative), the widened status set, the consistency rule, the pending
// partial index, the zero-non-pending upgrade assertion, and a Down section
// that unwinds in reverse dependency order. The Up section carries no
// triggers, no third status value, and no UPDATE/DELETE statements.
func TestConfirmationMigrationShape(t *testing.T) {
	data, err := fs.ReadFile(Migrations, "000005_confirmation_tracking.sql")
	if err != nil {
		t.Fatalf("read 000005: %v", err)
	}
	up, down, ok := strings.Cut(string(data), "-- +goose Down")
	if !ok {
		t.Fatal("000005 missing goose Down section")
	}

	for _, want := range []string{
		"CREATE TABLE confirmation_policy_history",
		"PRIMARY KEY (chain_id, policy_seq)",
		"CHECK (threshold > 0)",
		"UNIQUE (chain_id, request_id)",
		"CREATE UNIQUE INDEX confirmation_policy_history_bootstrap_uniq",
		"WHERE request_id IS NULL",
		"FOREIGN KEY (chain_id, prev_seq)",
		"REFERENCES confirmation_policy_history (chain_id, policy_seq)",
		"ADD COLUMN confirmed_at TIMESTAMPTZ",
		"ADD COLUMN confirm_tip_number BIGINT CHECK (confirm_tip_number >= 0)",
		"ADD COLUMN confirm_tip_hash TEXT",
		"ADD COLUMN confirm_threshold BIGINT CHECK (confirm_threshold > 0)",
		"ADD COLUMN confirmations NUMERIC CHECK (confirmations >= 0 AND confirmations = floor(confirmations))",
		"ADD COLUMN confirm_policy_seq BIGINT CHECK (confirm_policy_seq > 0)",
		"FOREIGN KEY (chain_id, confirm_policy_seq)",
		"REFERENCES confirmation_policy_history (chain_id, policy_seq)",
		"CHECK (status IN ('pending', 'confirmed'))",
		"deposit_observations_confirmation_consistency",
		"(status = 'pending') = (confirmed_at IS NULL)",
		"CREATE INDEX deposit_observations_pending_height_idx",
		"ON deposit_observations (chain_id, block_number)",
		"WHERE status = 'pending'",
		"WHERE status <> 'pending'",
		"RAISE EXCEPTION",
	} {
		if !strings.Contains(up, want) {
			t.Errorf("Up section missing %q", want)
		}
	}
	// OI-1 resolved: confirmations is NUMERIC; the BIGINT alternative must
	// not appear anywhere in this migration.
	if strings.Contains(string(data), "confirmations BIGINT") {
		t.Error("confirmations must be NUMERIC (OI-1); found a BIGINT alternative")
	}
	for _, forbidden := range []string{"orphaned", "TRIGGER", "UPDATE ", "DELETE FROM"} {
		if strings.Contains(up, forbidden) {
			t.Errorf("Up section contains %q; 005 carries no triggers, no third status, no data rewrites", forbidden)
		}
	}

	for _, want := range []string{
		"DROP INDEX IF EXISTS deposit_observations_pending_height_idx;",
		"DROP CONSTRAINT IF EXISTS deposit_observations_confirmation_consistency;",
		"DROP CONSTRAINT IF EXISTS deposit_observations_confirm_policy_seq_fkey;",
		"CHECK (status = 'pending')",
		"DROP COLUMN IF EXISTS confirm_policy_seq;",
		"DROP COLUMN IF EXISTS confirmations;",
		"DROP COLUMN IF EXISTS confirm_threshold;",
		"DROP COLUMN IF EXISTS confirm_tip_hash;",
		"DROP COLUMN IF EXISTS confirm_tip_number;",
		"DROP COLUMN IF EXISTS confirmed_at;",
		"DROP TABLE IF EXISTS confirmation_policy_history;",
	} {
		if !strings.Contains(down, want) {
			t.Errorf("Down section missing %q", want)
		}
	}

	// Down unwinds in reverse dependency order: index and confirmation
	// constraints first, status narrowing next, basis columns next, the
	// referenced history table last.
	order := []string{
		"DROP INDEX IF EXISTS deposit_observations_pending_height_idx;",
		"DROP CONSTRAINT IF EXISTS deposit_observations_confirmation_consistency;",
		"CHECK (status = 'pending')",
		"DROP COLUMN IF EXISTS confirmed_at;",
		"DROP TABLE IF EXISTS confirmation_policy_history;",
	}
	last := -1
	for _, step := range order {
		pos := strings.Index(down, step)
		if pos < 0 {
			continue // already reported missing above
		}
		if pos < last {
			t.Errorf("Down section out of dependency order at %q", step)
		}
		last = pos
	}
}
