//go:build integration

package db

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"sort"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// eventInfrastructureTables are the seven objects 000015 adds (data-model §1).
var eventInfrastructureTables = []string{
	"outbox_events",
	"consumer_progress",
	"consumer_inbox",
	"consumer_versions",
	"consumer_quarantine",
	"event_ops_audit",
	"event_system_state",
}

// TestEventInfrastructureMigrationUpDownUp covers T009 (data-model §7):
// 000015 applies on a scratch database, reverts cleanly with `down`, and
// applies again; the seven tables and the single-row cutover seed exist.
func TestEventInfrastructureMigrationUpDownUp(t *testing.T) {
	dsn := startPostgres(t)
	ctx := context.Background()
	opts := testMigrateOptions(dsn)

	var out bytes.Buffer
	if err := MigrateUp(ctx, opts, &out); err != nil {
		t.Fatalf("MigrateUp() error = %v", err)
	}
	sqlDB := openTestSQL(t, dsn)
	assertEventInfrastructureTables(t, sqlDB)

	provider, err := newProvider(sqlDB, opts)
	if err != nil {
		t.Fatalf("newProvider() error = %v", err)
	}
	result, err := provider.Down(ctx)
	if err != nil {
		t.Fatalf("Down() error = %v", err)
	}
	if result.Source.Version != 15 {
		t.Fatalf("Down() reverted version %d, want 15 (the last migration)", result.Source.Version)
	}
	for _, table := range eventInfrastructureTables {
		var reg *string
		if err := sqlDB.QueryRowContext(ctx, "SELECT to_regclass($1)::text", "public."+table).Scan(&reg); err != nil {
			t.Fatalf("to_regclass(%s): %v", table, err)
		}
		if reg != nil {
			t.Fatalf("table %s still exists after down", table)
		}
	}

	out.Reset()
	if err := MigrateUp(ctx, opts, &out); err != nil {
		t.Fatalf("MigrateUp() after down error = %v", err)
	}
	assertEventInfrastructureTables(t, sqlDB)
}

// assertEventInfrastructureTables checks the seven tables plus the seeded
// single row (id=1, catalog_version=1).
func assertEventInfrastructureTables(t *testing.T, sqlDB *sql.DB) {
	t.Helper()
	ctx := context.Background()
	for _, table := range eventInfrastructureTables {
		var reg *string
		if err := sqlDB.QueryRowContext(ctx, "SELECT to_regclass($1)::text", "public."+table).Scan(&reg); err != nil {
			t.Fatalf("to_regclass(%s): %v", table, err)
		}
		if reg == nil {
			t.Fatalf("table %s missing after up", table)
		}
	}
	var id int
	var catalogVersion int
	if err := sqlDB.QueryRowContext(ctx,
		"SELECT id, catalog_version FROM event_system_state").Scan(&id, &catalogVersion); err != nil {
		t.Fatalf("read event_system_state: %v", err)
	}
	if id != 1 || catalogVersion != 1 {
		t.Fatalf("event_system_state = (id=%d, catalog_version=%d), want (1, 1)", id, catalogVersion)
	}
}

// TestEventInfrastructureConstraintProbes is the T009 negative-probe matrix:
// every named constraint must reject its violating row with the documented
// SQLSTATE and be classifiable by ConstraintName alone (data-model §2/§8/§10).
func TestEventInfrastructureConstraintProbes(t *testing.T) {
	dsn := startPostgres(t)
	ctx := context.Background()
	opts := testMigrateOptions(dsn)
	if err := MigrateUp(ctx, opts, io.Discard); err != nil {
		t.Fatalf("MigrateUp() error = %v", err)
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("pgx.Connect() error = %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })

	// A valid evm_log row every duplicate probe can collide with.
	evmLogRow := func(eventID uuid.UUID, payloadHash string) string {
		return fmt.Sprintf(`INSERT INTO outbox_events
			(event_id, identity_kind, event_type, schema_version, aggregate_type, aggregate_id,
			 aggregate_version, payload, payload_hash, occurred_at, chain_id, block_number, block_hash, tx_hash, log_index)
			VALUES ('%s','evm_log','deposit.observation.created',1,'deposit_observation','obs-1',
			 1,'{}','%s',now(),1,10,'0xblock','0xtx',0)`, eventID, payloadHash)
	}
	objectRow := func(eventID uuid.UUID) string {
		return fmt.Sprintf(`INSERT INTO outbox_events
			(event_id, identity_kind, event_type, schema_version, aggregate_type, aggregate_id,
			 aggregate_version, payload, payload_hash, occurred_at)
			VALUES ('%s','business_object','withdrawal.request.received',1,'withdrawal_request','req-1',
			 1,'{}','h-obj',now())`, eventID)
	}
	quarantineRow := func(eventID uuid.UUID) string {
		return fmt.Sprintf(`INSERT INTO consumer_quarantine
			(consumer_name, event_id, event_snapshot, failure_class, reason, attempt_count, first_seen_at, last_seen_at)
			VALUES ('ref','%s','{}','version_gap','gap',1,now(),now())`, eventID)
	}
	// One event id shared by the duplicate-open-quarantine probe pair: the
	// partial unique is on (consumer_name, event_id) WHERE status='open'.
	quarantineEventID := uuid.New()

	cases := []struct {
		name     string
		setup    []string
		stmt     string
		wantCode string
		wantName string
	}{
		{
			name:     "duplicate evm_log identity",
			setup:    []string{evmLogRow(uuid.New(), "h1")},
			stmt:     evmLogRow(uuid.New(), "h2"),
			wantCode: "23505", wantName: "outbox_events_log_identity_uniq",
		},
		{
			name:     "duplicate business_object identity",
			setup:    []string{objectRow(uuid.New())},
			stmt:     objectRow(uuid.New()),
			wantCode: "23505", wantName: "outbox_events_object_identity_uniq",
		},
		{
			name: "published without published_at",
			stmt: `INSERT INTO outbox_events
				(event_id, identity_kind, event_type, schema_version, aggregate_type, aggregate_id,
				 aggregate_version, payload, payload_hash, occurred_at, publish_state)
				VALUES ('` + uuid.New().String() + `','business_object','withdrawal.request.received',1,'withdrawal_request','req-pub',
				 1,'{}','h-pub',now(),'published')`,
			wantCode: "23514", wantName: "outbox_events_state_consistency",
		},
		{
			name: "blocked without last_error_class",
			stmt: `INSERT INTO outbox_events
				(event_id, identity_kind, event_type, schema_version, aggregate_type, aggregate_id,
				 aggregate_version, payload, payload_hash, occurred_at, publish_state)
				VALUES ('` + uuid.New().String() + `','business_object','withdrawal.request.received',1,'withdrawal_request','req-blk',
				 1,'{}','h-blk',now(),'blocked')`,
			wantCode: "23514", wantName: "outbox_events_state_consistency",
		},
		{
			name: "revision without recovery_version",
			stmt: `INSERT INTO outbox_events
				(event_id, identity_kind, event_type, schema_version, aggregate_type, aggregate_id,
				 aggregate_version, payload, payload_hash, occurred_at, revises_event_id)
				VALUES ('` + uuid.New().String() + `','business_object','deposit.revision.applied',1,'deposit_observation','obs-rev',
				 2,'{}','h-rev',now(),'` + uuid.New().String() + `')`,
			wantCode: "23514", wantName: "outbox_events_revision_shape",
		},
		{
			name: "evm_log identity shape missing chain fields",
			stmt: `INSERT INTO outbox_events
				(event_id, identity_kind, event_type, schema_version, aggregate_type, aggregate_id,
				 aggregate_version, payload, payload_hash, occurred_at)
				VALUES ('` + uuid.New().String() + `','evm_log','deposit.observation.created',1,'deposit_observation','obs-shape',
				 1,'{}','h-shape',now())`,
			wantCode: "23514", wantName: "outbox_events_log_identity_shape",
		},
		{
			name: "negative consumer offset",
			stmt: `INSERT INTO consumer_progress (consumer_name, topic, partition, next_offset)
				VALUES ('ref','txharbor.events.v1',0,-1)`,
			wantCode: "23514", wantName: "consumer_progress_next_offset_check",
		},
		{
			name:     "duplicate open quarantine entry",
			setup:    []string{quarantineRow(quarantineEventID)},
			stmt:     quarantineRow(quarantineEventID),
			wantCode: "23505", wantName: "consumer_quarantine_open_uniq",
		},
		{
			name: "duplicate operation_id",
			setup: []string{`INSERT INTO event_ops_audit (operation_id, op_kind, operator, scope, reason, result)
				VALUES ('op-1','replay','alice','{}','drill','ok')`},
			stmt: `INSERT INTO event_ops_audit (operation_id, op_kind, operator, scope, reason, result)
				VALUES ('op-1','unblock','alice','{}','drill','ok')`,
			wantCode: "23505", wantName: "event_ops_audit_operation_id_key",
		},
		{
			name:     "event_system_state id other than 1",
			stmt:     `INSERT INTO event_system_state (id, cutover_at, catalog_version) VALUES (2, now(), 1)`,
			wantCode: "23514", wantName: "event_system_state_id_check",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, setup := range tc.setup {
				if _, err := conn.Exec(ctx, setup); err != nil {
					t.Fatalf("setup failed: %v\n%s", err, setup)
				}
			}
			_, err := conn.Exec(ctx, tc.stmt)
			if err == nil {
				t.Fatalf("probe accepted the violating row; want %s/%s", tc.wantCode, tc.wantName)
			}
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) {
				t.Fatalf("error %v is not a PgError", err)
			}
			if pgErr.Code != tc.wantCode {
				t.Fatalf("SQLSTATE = %s, want %s (constraint %s)", pgErr.Code, tc.wantCode, pgErr.ConstraintName)
			}
			if pgErr.ConstraintName != tc.wantName {
				t.Fatalf("ConstraintName = %q, want %q (classification must work by name alone)",
					pgErr.ConstraintName, tc.wantName)
			}
		})
	}
}

// TestEventInfrastructureAdditiveOnly covers the T009 additive-only diff:
// migrate to 000014, snapshot the schema, apply 000015, snapshot again and
// require that nothing pre-existing changed and every new object belongs to
// the seven 013 tables.
func TestEventInfrastructureAdditiveOnly(t *testing.T) {
	dsn := startPostgres(t)
	ctx := context.Background()

	// Filtered filesystem with 000001-000014 only.
	all, err := MigrationFiles(Migrations)
	if err != nil {
		t.Fatalf("MigrationFiles() error = %v", err)
	}
	filtered := fstest.MapFS{}
	for _, f := range all {
		if f.Version > 14 {
			continue
		}
		data, err := fs.ReadFile(Migrations, f.Name)
		if err != nil {
			t.Fatalf("read %s: %v", f.Name, err)
		}
		filtered[f.Name] = &fstest.MapFile{Data: data}
	}

	opts14 := testMigrateOptions(dsn)
	opts14.FS = filtered
	if err := MigrateUp(ctx, opts14, io.Discard); err != nil {
		t.Fatalf("MigrateUp(<=000014) error = %v", err)
	}
	sqlDB := openTestSQL(t, dsn)
	before := schemaSnapshot(t, sqlDB)

	if err := MigrateUp(ctx, testMigrateOptions(dsn), io.Discard); err != nil {
		t.Fatalf("MigrateUp(000015) error = %v", err)
	}
	after := schemaSnapshot(t, sqlDB)

	for key, beforeVal := range before {
		afterVal, ok := after[key]
		if !ok {
			t.Errorf("000015 removed or renamed a pre-existing object: %s", key)
			continue
		}
		if afterVal != beforeVal {
			t.Errorf("000015 changed a pre-existing object: %s\n  before: %s\n  after:  %s", key, beforeVal, afterVal)
		}
	}
	for key := range after {
		if _, existed := before[key]; existed {
			continue
		}
		if !isEventInfrastructureObject(key) {
			t.Errorf("000015 added an object outside the 013 tables: %s", key)
		}
	}
}

// isEventInfrastructureObject reports whether a snapshot key names an object
// of one of the seven 013 tables (columns, constraints, indexes, sequences).
func isEventInfrastructureObject(key string) bool {
	for _, table := range eventInfrastructureTables {
		if strings.Contains(key, table) {
			return true
		}
	}
	return false
}

// schemaSnapshot captures public-schema objects (columns, constraints,
// indexes, sequences) as kind+name -> definition strings. Data rows are
// deliberately excluded: 000015 only adds schema objects.
func schemaSnapshot(t *testing.T, sqlDB *sql.DB) map[string]string {
	t.Helper()
	ctx := context.Background()
	snap := map[string]string{}

	columnRows, err := sqlDB.QueryContext(ctx, `
		SELECT table_name, column_name, data_type, is_nullable, COALESCE(column_default, '')
		FROM information_schema.columns
		WHERE table_schema = 'public'`)
	if err != nil {
		t.Fatalf("snapshot columns: %v", err)
	}
	defer columnRows.Close()
	for columnRows.Next() {
		var table, column, dataType, nullable, def string
		if err := columnRows.Scan(&table, &column, &dataType, &nullable, &def); err != nil {
			t.Fatalf("scan column: %v", err)
		}
		snap["column:"+table+"."+column] = dataType + "|" + nullable + "|" + def
	}
	if err := columnRows.Err(); err != nil {
		t.Fatalf("snapshot columns: %v", err)
	}

	constraintRows, err := sqlDB.QueryContext(ctx, `
		SELECT c.relname, con.conname, pg_get_constraintdef(con.oid)
		FROM pg_constraint con
		JOIN pg_class c ON c.oid = con.conrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'public'`)
	if err != nil {
		t.Fatalf("snapshot constraints: %v", err)
	}
	defer constraintRows.Close()
	for constraintRows.Next() {
		var table, name, def string
		if err := constraintRows.Scan(&table, &name, &def); err != nil {
			t.Fatalf("scan constraint: %v", err)
		}
		snap["constraint:"+table+"."+name] = def
	}
	if err := constraintRows.Err(); err != nil {
		t.Fatalf("snapshot constraints: %v", err)
	}

	indexRows, err := sqlDB.QueryContext(ctx, `
		SELECT tablename, indexname, indexdef FROM pg_indexes WHERE schemaname = 'public'`)
	if err != nil {
		t.Fatalf("snapshot indexes: %v", err)
	}
	defer indexRows.Close()
	for indexRows.Next() {
		var table, name, def string
		if err := indexRows.Scan(&table, &name, &def); err != nil {
			t.Fatalf("scan index: %v", err)
		}
		snap["index:"+table+"."+name] = def
	}
	if err := indexRows.Err(); err != nil {
		t.Fatalf("snapshot indexes: %v", err)
	}

	sequenceRows, err := sqlDB.QueryContext(ctx, `
		SELECT sequence_name FROM information_schema.sequences WHERE sequence_schema = 'public'`)
	if err != nil {
		t.Fatalf("snapshot sequences: %v", err)
	}
	defer sequenceRows.Close()
	for sequenceRows.Next() {
		var name string
		if err := sequenceRows.Scan(&name); err != nil {
			t.Fatalf("scan sequence: %v", err)
		}
		snap["sequence:"+name] = ""
	}
	if err := sequenceRows.Err(); err != nil {
		t.Fatalf("snapshot sequences: %v", err)
	}

	// Deterministic order for failure messages.
	keys := make([]string, 0, len(snap))
	for k := range snap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	ordered := make(map[string]string, len(snap))
	for _, k := range keys {
		ordered[k] = snap[k]
	}
	return ordered
}
