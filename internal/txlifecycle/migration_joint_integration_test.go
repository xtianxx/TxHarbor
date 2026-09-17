//go:build integration

package txlifecycle

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/xtianxx/txharbor/internal/db"
)

// TestT043MigrationSetMergeOrder re-verifies the applied migration set and the
// provisional numbers in 010→011 merge order on the joint scratch DB (PLAN-1;
// T043): 000011 (010) precedes 000012 (011) precedes 000013 (010 guarded
// follow-up) precedes 000014 (010 intent-FK repair), and no applied version is
// missing or duplicated.
func TestT043MigrationSetMergeOrder(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	dsn := e.dsn

	files, err := db.MigrationFiles(db.Migrations)
	if err != nil {
		t.Fatalf("list migrations: %v", err)
	}
	want := map[int64]string{
		11: "000011_tx_lifecycle.sql",
		12: "000012_withdrawal_execution.sql",
		13: "000013_tx_lifecycle_intent_fk.sql",
		14: "000014_intent_fk_repair.sql",
	}
	byVersion := make(map[int64]string, len(files))
	var versions []int64
	for _, f := range files {
		byVersion[f.Version] = f.Name
		versions = append(versions, f.Version)
	}
	for v, name := range want {
		if byVersion[v] != name {
			t.Errorf("version %d = %q, want %q", v, byVersion[v], name)
		}
	}
	if last := versions[len(versions)-1]; last != 14 {
		t.Errorf("highest migration = %d, want 14", last)
	}

	opts := db.MigrateOptions{DSN: dsn, LockTimeout: 10 * time.Second, ConnectTimeout: 10 * time.Second}
	state, err := db.Inspect(ctx, opts)
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if len(state.Pending) != 0 {
		t.Fatalf("pending migrations: %v", state.Pending)
	}
	if state.Current != 14 {
		t.Fatalf("current migration = %d, want 14", state.Current)
	}
	for _, v := range []int64{11, 12, 13, 14} {
		found := false
		for _, a := range state.Applied {
			if a == v {
				found = true
			}
		}
		if !found {
			t.Errorf("migration %d not applied", v)
		}
	}

	for _, table := range []string{
		"tx_attempts", "tx_attempt_signings", "tx_send_attempts", "tx_reconciliations",
		"tx_receipts", "tx_attempt_events", "tx_intent_freezes",
		"payment_intents", "execution_claims", "execution_steps", "execution_events",
		"request_status_projection", "execution_caller_permission", "execution_ops_audit",
	} {
		if !e.tableExists(table) {
			t.Errorf("table %s missing after migrate", table)
		}
	}

	names := []string{
		"tx_attempts_pkey", "tx_attempts_signing_request_uniq", "tx_attempts_binding_fkey",
		"tx_attempts_authorization_fkey", "tx_attempts_intent_fkey",
		"tx_attempt_signings_tx_hash_uniq", "tx_send_attempts_attempt_seq_uniq",
		"tx_attempt_events_attempt_seq_uniq", "tx_receipts_tx_block_uniq",
		"execution_claims_pkey", "payment_intents_pkey",
	}
	var missing []string
	for _, n := range names {
		var exists bool
		if err := e.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_constraint WHERE conname = $1)`, n).Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if !exists {
			missing = append(missing, n)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("named constraints missing: %v", missing)
	}
}

// TestT044IntentFK verifies the additive intent-FK follow-up: the named
// constraint exists and is validated (not NOT VALID), an absent intent_id
// raises 23503 on exactly tx_attempts_intent_fkey, and a seeded attempt
// (existing data) stays valid (G-010-3; PLAN-1; J6).
func TestT044IntentFK(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()

	var refSchema, refTable, refColumn string
	var convalidated bool
	if err := e.pool.QueryRow(ctx,
		`SELECT n.nspname, c.relname, a.attname, con.convalidated
		   FROM pg_constraint con
		   JOIN pg_class c ON c.oid = con.confrelid
		   JOIN pg_namespace n ON n.oid = c.relnamespace
		   JOIN unnest(con.confkey) AS k(attnum) ON TRUE
		   JOIN pg_attribute a ON a.attrelid = con.confrelid AND a.attnum = k.attnum
		  WHERE con.conname = 'tx_attempts_intent_fkey' AND con.contype = 'f'`).
		Scan(&refSchema, &refTable, &refColumn, &convalidated); err != nil {
		t.Fatalf("intent FK missing: %v", err)
	}
	if refSchema != "public" || refTable != "payment_intents" || refColumn != "intent_id" {
		t.Fatalf("intent FK references %s.%s(%s), want payment_intents(intent_id)", refSchema, refTable, refColumn)
	}
	if !convalidated {
		t.Fatal("intent FK is NOT VALID; existing rows were not checked")
	}

	// Existing-data compatibility: the seeded attempt (valid intent chain)
	// already proves the ALTER validated a populated table without rewriting.
	f := e.seed()

	// Negative 23503 probe: valid binding/authorization, absent intent_id.
	_, err := e.pool.Exec(ctx,
		`INSERT INTO tx_attempts (
		   attempt_id, signing_request_id, intent_id, binding_ref, authorization_id,
		   authorization_version, recovery_version, chain_id, sender, nonce, tx_type,
		   to_addr, value, data, gas_limit, gas_price, max_fee_per_gas, max_priority_fee_per_gas,
		   asset, recipient, amount, canonical_envelope, content_hash)
		 VALUES ('fk-probe-att','fk-probe-sr','missing-intent-fk-probe',$1,$2,1,0,$3,$4,0,2,
		   $5,0,'\x',21000,NULL,1000000000,100000000,$5,$6,'1000','{}',
		   '0x0000000000000000000000000000000000000000000000000000000000000000')`,
		f.bindingID, f.authID, e.chainID, f.sender, fxAsset, fxRecipient)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23503" || pgErr.ConstraintName != "tx_attempts_intent_fkey" {
		t.Fatalf("absent-intent insert error = %v, want 23503 tx_attempts_intent_fkey", err)
	}

	// Positive probe: a second attempt on the seeded intent is accepted.
	if _, err := e.pool.Exec(ctx,
		`INSERT INTO tx_attempts (
		   attempt_id, signing_request_id, intent_id, binding_ref, authorization_id,
		   authorization_version, recovery_version, chain_id, sender, nonce, tx_type,
		   to_addr, value, data, gas_limit, gas_price, max_fee_per_gas, max_priority_fee_per_gas,
		   asset, recipient, amount, canonical_envelope, content_hash)
		 VALUES ('fk-probe-ok','fk-probe-ok-sr',$1,$2,$3,1,0,$4,$5,0,2,
		   $6,0,'\x',21000,NULL,1000000000,100000000,$6,$7,'1000','{}',
		   '0x0000000000000000000000000000000000000000000000000000000000000001')`,
		f.intentID, f.bindingID, f.authID, e.chainID, f.sender, fxAsset, fxRecipient); err != nil {
		t.Fatalf("valid-intent insert refused: %v", err)
	}
}
