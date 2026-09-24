//go:build integration

// eventsadmin_b3_integration_test.go is the B3/T031 cutover-path evidence:
// bootstrap-export is a read-only snapshot (business table counts unchanged,
// no fabricated event rows), cutover reports the migration-seeded marker and
// fails closed when it is missing, and rollback-inventory lists pending|blocked
// outbox rows without touching them.
package app

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/events"
)

// eventsAdminEnv is baseEnv + the complete 013 runtime set, pointed at the
// scratch container's DSN. Values are test inputs, not proposed thresholds.
func eventsAdminEnv(dsn string) map[string]string {
	return map[string]string{
		"TXHARBOR_PG_DSN":                              dsn,
		"TXHARBOR_RPC_URL":                             "http://127.0.0.1:8545",
		"TXHARBOR_CHAIN_ID":                            "31337",
		"TXHARBOR_START_HEIGHT":                        "0",
		"TXHARBOR_LOG_START_HEIGHT":                    "0",
		"TXHARBOR_LOG_CONTRACTS":                       "0x1111111111111111111111111111111111111111",
		"TXHARBOR_DEPOSIT_START_HEIGHT":                "0",
		"TXHARBOR_DEPOSIT_CONTRACTS":                   "0x1111111111111111111111111111111111111111",
		"TXHARBOR_DEPOSIT_WATCH_ADDRESSES":             "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"TXHARBOR_CONFIRMATION_DEPTH":                  "10",
		"TXHARBOR_EVENTS_ENABLED":                      "true",
		"TXHARBOR_KAFKA_BROKERS":                       "127.0.0.1:9092",
		"TXHARBOR_REDIS_ADDR":                          "127.0.0.1:6379",
		"TXHARBOR_RATELIMIT_NEW_WITHDRAWAL":            "10/20",
		"TXHARBOR_RATELIMIT_WRITE":                     "50/100",
		"TXHARBOR_RATELIMIT_QUERY":                     "100/200",
		"TXHARBOR_RATELIMIT_OPERATOR":                  "5/10",
		"TXHARBOR_RATELIMIT_RPC":                       "20/40",
		"TXHARBOR_EVENTS_CAPACITY_SOFT_LIMIT":          "1000",
		"TXHARBOR_EVENTS_CAPACITY_HARD_LIMIT":          "2000",
		"TXHARBOR_EVENTS_CAPACITY_RESERVE":             "100",
		"TXHARBOR_EVENTS_CAPACITY_RETENTION":           "24h",
		"TXHARBOR_EVENTS_CAPACITY_MAX_SHUTDOWN_WINDOW": "1h",
		"TXHARBOR_EVENTS_CAPACITY_DRAIN_TARGET_WINDOW": "30m",
	}
}

func eventsAdminRun(t *testing.T, env map[string]string, args ...string) (int, string, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := EventsAdmin(context.Background(), args, Deps{
		Getenv: envGetter(env),
		Stdout: &stdout,
		Stderr: &stderr,
	})
	return code, stdout.String(), stderr.String()
}

// TestEventsAdminCutoverPath is the T031 evidence.
func TestEventsAdminCutoverPath(t *testing.T) {
	ctx := context.Background()
	pgCtr := startPostgresContainer(t)
	dsn := postgresDSN(t, pgCtr)
	if err := db.MigrateUp(ctx, db.MigrateOptions{DSN: dsn, LockTimeout: 5 * time.Second, ConnectTimeout: 5 * time.Second}, io.Discard); err != nil {
		t.Fatalf("migrate up: %v", err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)

	// Seed one object of each snapshot class (real FKs).
	seed := []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO deposit_config_history
		    (chain_id, version_seq, config_hash, start_block, assets, watches, replay_from, operator)
		  VALUES (31337, 1, repeat('aa', 32), 10, '0x1111111111111111111111111111111111111111:10',
		          '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:10', 10, 'bootstrap')`, nil},
		{`INSERT INTO deposit_observations
		    (chain_id, block_hash, tx_hash, log_index, block_number, contract, sender, recipient, amount, version_seq)
		  VALUES (31337, '0x' || repeat('ab', 32), '0x' || repeat('cd', 32), 0, 12,
		          '0x1111111111111111111111111111111111111111', '0x2222222222222222222222222222222222222222',
		          '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 5, 1)`, nil},
		{`INSERT INTO caller (caller_id, label) VALUES (1, 'ops')`, nil},
		{`INSERT INTO withdrawal_requests
		    (request_id, caller_id, idempotency_key, authorization_id, chain_id, asset, recipient, amount)
		  VALUES ('wr-t031', 1, 'idem-t031', 'authz-t031', 31337,
		          '0x1111111111111111111111111111111111111111', '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 5)`, nil},
		{`INSERT INTO payment_intents
		    (intent_id, request_id, chain_id, sender, authorization_id, authorization_version, state, admitted_recovery_version)
		  VALUES ('intent-t031', 'wr-t031', 31337, '0x2222222222222222222222222222222222222222', 'authz-t031', 1, 'admitted', 0)`, nil},
	}
	for _, stmt := range seed {
		if _, err := pool.Exec(ctx, stmt.sql, stmt.args...); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	counts := func() map[string]int {
		out := map[string]int{}
		for _, table := range []string{"deposit_observations", "withdrawal_requests", "payment_intents", "outbox_events"} {
			var n int
			if err := pool.QueryRow(ctx, `SELECT count(*) FROM `+table).Scan(&n); err != nil {
				t.Fatalf("count %s: %v", table, err)
			}
			out[table] = n
		}
		return out
	}
	before := counts()

	// 1. bootstrap-export: read-only JSONL snapshot; business tables and the
	// outbox are untouched and no event row is fabricated.
	exportPath := filepath.Join(t.TempDir(), "snapshot.jsonl")
	code, _, stderr := eventsAdminRun(t, eventsAdminEnv(dsn), "bootstrap-export", "--out", exportPath)
	if code != 0 {
		t.Fatalf("bootstrap-export exit = %d, stderr=%s", code, stderr)
	}
	body, err := os.ReadFile(exportPath)
	if err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(body)), "\n")
	kinds := map[string]int{}
	cutoverSeen := false
	for _, line := range lines {
		var obj map[string]any
		if err := json.Unmarshal([]byte(line), &obj); err != nil {
			t.Fatalf("snapshot line is not JSON: %v (%q)", err, line)
		}
		snapshot, _ := obj["snapshot"].(string)
		kinds[snapshot]++
		if snapshot == "event_system_state" {
			cutoverSeen = true
			if obj["catalog_version"] != float64(1) {
				t.Fatalf("snapshot catalog_version = %v, want 1", obj["catalog_version"])
			}
		}
	}
	if !cutoverSeen || kinds["deposit_observation"] != 1 || kinds["withdrawal_request"] != 1 || kinds["withdrawal_intent"] != 1 {
		t.Fatalf("snapshot kinds = %v, want event_system_state + one of each object class", kinds)
	}
	after := counts()
	for table, want := range before {
		if after[table] != want {
			t.Fatalf("bootstrap-export changed %s rows: %d -> %d (must be read-only)", table, want, after[table])
		}
	}
	var fabricated int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE occurred_at < (SELECT cutover_at FROM event_system_state WHERE id = 1)`).Scan(&fabricated); err != nil {
		t.Fatalf("count fabricated history: %v", err)
	}
	if fabricated != 0 {
		t.Fatalf("fabricated pre-cutover event rows = %d, want 0", fabricated)
	}

	// 2. cutover: reports the migration-seeded marker; refuses when missing.
	code, stdout, stderr := eventsAdminRun(t, eventsAdminEnv(dsn), "cutover")
	if code != 0 || !strings.Contains(stdout, "catalog_version=1") {
		t.Fatalf("cutover exit = %d stdout=%q stderr=%q, want seeded marker", code, stdout, stderr)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM event_system_state WHERE id = 1`); err != nil {
		t.Fatalf("delete marker: %v", err)
	}
	code, _, _ = eventsAdminRun(t, eventsAdminEnv(dsn), "cutover")
	if code != 1 {
		t.Fatalf("cutover without marker exit = %d, want 1 (fail-closed)", code)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO event_system_state (id, cutover_at, catalog_version) VALUES (1, now(), 1)`); err != nil {
		t.Fatalf("restore marker: %v", err)
	}

	// 3. rollback-inventory: pending + blocked rows are listed and exported,
	// published rows are not, and no row changes state.
	appendEvent := func(requestID string) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin append: %v", err)
		}
		_, err = events.Append(ctx, tx, events.Event{
			EventType: events.EventTypeWithdrawalRequestReceived, SchemaVersion: events.SchemaVersionV1,
			IdentityKind:  events.IdentityKindBusinessObject,
			AggregateType: "withdrawal_request", AggregateID: requestID,
			Payload:    map[string]any{"request_id": requestID, "caller": int64(1), "state": "accepted", "chain_id": int64(31337)},
			OccurredAt: time.Now().UTC(),
		})
		if err != nil {
			_ = tx.Rollback(ctx)
			t.Fatalf("append: %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("commit append: %v", err)
		}
	}
	appendEvent("wr-t031-pending")
	appendEvent("wr-t031-blocked")
	appendEvent("wr-t031-published")
	if _, err := pool.Exec(ctx, `UPDATE outbox_events SET publish_state = 'blocked', last_error_class = 'contract'
		WHERE aggregate_id = 'wr-t031-blocked'`); err != nil {
		t.Fatalf("block row: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE outbox_events SET publish_state = 'published', published_at = now()
		WHERE aggregate_id = 'wr-t031-published'`); err != nil {
		t.Fatalf("publish row: %v", err)
	}
	inventoryPath := filepath.Join(t.TempDir(), "inventory.jsonl")
	code, stdout, stderr = eventsAdminRun(t, eventsAdminEnv(dsn), "rollback-inventory", "--out", inventoryPath)
	if code != 0 {
		t.Fatalf("rollback-inventory exit = %d stderr=%s", code, stderr)
	}
	if !strings.Contains(stdout, "pending=1 blocked=1") {
		t.Fatalf("rollback-inventory summary = %q, want pending=1 blocked=1", stdout)
	}
	inventory, err := os.ReadFile(inventoryPath)
	if err != nil {
		t.Fatalf("read inventory: %v", err)
	}
	text := string(inventory)
	if !strings.Contains(text, "wr-t031-pending") || !strings.Contains(text, "wr-t031-blocked") {
		t.Fatalf("inventory does not list pending/blocked rows: %s", text)
	}
	if strings.Contains(text, "wr-t031-published") {
		t.Fatalf("inventory lists a published row: %s", text)
	}
	var publishedPending int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE publish_state = 'pending'`).Scan(&publishedPending); err != nil {
		t.Fatalf("count pending: %v", err)
	}
	if publishedPending != 1 {
		t.Fatalf("pending rows after inventory = %d, want 1 (inventory must not mutate state)", publishedPending)
	}
	// The snapshot export never wrote an outbox row either.
	if row := countOutboxForAggregate(t, ctx, pool, "wr-t031"); row != 0 {
		t.Fatalf("snapshot export appended an event for a seeded request: %d", row)
	}
}

func countOutboxForAggregate(t *testing.T, ctx context.Context, pool *pgxpool.Pool, aggregateID string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE aggregate_id = $1`, aggregateID).Scan(&n); err != nil {
		t.Fatalf("count outbox for %s: %v", aggregateID, err)
	}
	return n
}
