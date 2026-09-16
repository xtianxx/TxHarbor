//go:build integration

// allocate_concurrency_integration_test.go owns spec task T018
// (008-nonce-manager, US1 / V1 / SC-01): the real-PostgreSQL concurrency
// acceptance for the admission core. N>=2 different intents race on one
// (chain_id, sender) while a different sender allocates in parallel.
//
// PostgreSQL is real (testcontainers, the migration_integration_test.go
// pattern). Anvil is deliberately NOT required (T018): the chain view is
// served by an httptest JSON-RPC stub whose latest/pending counters stay 0, so
// classification is always `consistent` and every race is resolved by durable
// state plus the named UNIQUE carriers, never by chain data. Assertions read
// durable rows back; the outcome is never decided by sleeping (a start barrier
// plus a WaitGroup are the only sync points).
//
// This file never imports an internal/nonce classifier for its assertions:
// every database invariant is a raw-SQL probe, and the admission surface is
// the public nonce.NewAllocator / Allocate API.
package nonce_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	gethrpc "github.com/ethereum/go-ethereum/rpc"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/nonce"
)

// nonceT018ChainID is the scope chain every request in this test binds to.
const nonceT018ChainID = int64(1)

// nonceT018Concurrency is the V1 fan-out: N intents on the contended scope and
// M intents on a second, independent scope, all in flight at once.
const (
	nonceT018IntentsA = 6
	nonceT018IntentsB = 3
)

// nonceT018Target is one parallel admission: the scope it belongs to plus its
// canonical allocation request.
type nonceT018Target struct {
	scope string
	req   nonce.AllocationRequest
}

// nonceT018Result captures one goroutine's Allocate result.
type nonceT018Result struct {
	binding *nonce.Binding
	outcome nonce.Outcome
	err     error
}

// nonceT018StubRPC serves the three observation reads the Observer performs
// (eth_getTransactionCount latest/pending + eth_getBlockByNumber latest) from
// a fixed head at count 0. It is a transport-level double, not an Anvil
// replacement: the concurrency proof lives in PostgreSQL.
func nonceT018StubRPC(t *testing.T) *gethrpc.Client {
	t.Helper()
	hash := "0x" + strings.Repeat("ab", 32)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		if err := json.Unmarshal(body, &req); err != nil {
			http.Error(w, "malformed JSON-RPC request", http.StatusBadRequest)
			return
		}
		var result any
		switch req.Method {
		case "eth_getTransactionCount":
			result = "0x0"
		case "eth_getBlockByNumber":
			result = map[string]any{"number": "0x0", "hash": hash}
		default:
			http.Error(w, "unscripted method "+req.Method, http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      json.RawMessage(req.ID),
			"result":  result,
		})
	}))
	t.Cleanup(srv.Close)
	client, err := gethrpc.Dial(srv.URL)
	if err != nil {
		t.Fatalf("dial stub JSON-RPC: %v", err)
	}
	t.Cleanup(client.Close)
	return client
}

// nonceT018Nonces reads one scope's committed nonces in ascending order.
func nonceT018Nonces(t *testing.T, sqlDB *sql.DB, sender string) []int64 {
	t.Helper()
	rows, err := sqlDB.QueryContext(context.Background(),
		`SELECT nonce::bigint FROM nonce_bindings
		 WHERE chain_id = $1 AND sender = $2 ORDER BY nonce`, nonceT018ChainID, sender)
	if err != nil {
		t.Fatalf("read scope nonces: %v", err)
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var n int64
		if err := rows.Scan(&n); err != nil {
			t.Fatalf("scan scope nonce: %v", err)
		}
		out = append(out, n)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate scope nonces: %v", err)
	}
	return out
}

// nonceT018WantNonces returns 0..count-1, the distinct low nonce range a
// healthy serialized allocator hands out to a fresh scope.
func nonceT018WantNonces(count int) []int64 {
	out := make([]int64, count)
	for i := range out {
		out[i] = int64(i)
	}
	return out
}

func nonceT018SameInt64(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestAllocateConcurrent is the T018 V1/SC-01 acceptance: N>=2 parallel
// different intents on one (chain_id, sender) plus a parallel different
// sender. It asserts each intent has at most one binding, no active binding
// shares a nonce within a scope, the other sender is unaffected, and the named
// DB UNIQUE carriers exist and are enforced.
func TestAllocateConcurrent(t *testing.T) {
	ctx := context.Background()

	dsn := nonceStartPostgres(t)
	var migrateOut bytes.Buffer
	if err := db.MigrateUp(ctx, nonceMigrateOptions(dsn), &migrateOut); err != nil {
		t.Fatalf("MigrateUp() error = %v (output %q)", err, migrateOut.String())
	}
	sqlDB := nonceOpenSQL(t, dsn)

	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse pool config: %v", err)
	}
	// Every allocation trivially serializes on the coordination row; the extra
	// connections keep waiters off the pool frontier so a failure surfaces as
	// a domain outcome, never pool starvation.
	poolCfg.MaxConns = int32(nonceT018IntentsA + nonceT018IntentsB + 2)
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)

	senderA := nonceAddr("aa")
	senderB := nonceAddr("bb")
	asset := nonceAddr("11")
	recipient := nonceAddr("22")

	// FK prerequisites: a registered (active) sender per scope, and one caller
	// for the 007 authorizations each intent binds.
	nonceMustExec(t, sqlDB, `INSERT INTO caller (caller_id, label) VALUES (1, 't018')`)
	for _, sender := range []string{senderA, senderB} {
		nonceMustExec(t, sqlDB, `INSERT INTO nonce_wallet_registry
			(chain_id, sender, state, registry_seq) VALUES ($1, $2, 'active', 1)`,
			nonceT018ChainID, sender)
	}

	var targets []nonceT018Target
	for i := 0; i < nonceT018IntentsA; i++ {
		targets = append(targets, nonceT018Request(t, sqlDB, "a", i, senderA, asset, recipient))
	}
	for i := 0; i < nonceT018IntentsB; i++ {
		targets = append(targets, nonceT018Request(t, sqlDB, "b", i, senderB, asset, recipient))
	}

	observer := nonce.NewObserver(nonceT018StubRPC(t), nonce.ObserverConfig{
		RPCTimeout:   5 * time.Second,
		RetryInitial: time.Millisecond,
		RetryMax:     5 * time.Millisecond,
	})
	allocator := nonce.NewAllocator(pool, observer)

	results := make([]nonceT018Result, len(targets))
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range targets {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			binding, outcome, err := allocator.Allocate(ctx, targets[i].req)
			results[i] = nonceT018Result{binding: binding, outcome: outcome, err: err}
		}(i)
	}
	close(start)
	wg.Wait()

	// Every distinct intent must have won exactly one new binding; a refusal
	// or a replay here is a real serialization defect, not a flake.
	for i, res := range results {
		if res.err != nil || res.outcome != nonce.OutcomeAllocated || res.binding == nil {
			t.Fatalf("intent %s: outcome=%q err=%v binding=%v, want allocated with a binding",
				targets[i].req.IntentID, res.outcome, res.err, res.binding)
		}
		if res.binding.Sender != targets[i].scope || res.binding.IntentID != targets[i].req.IntentID {
			t.Fatalf("binding %s identity mismatch: got (sender=%s intent=%s), want (sender=%s intent=%s)",
				res.binding.BindingID, res.binding.Sender, res.binding.IntentID,
				targets[i].scope, targets[i].req.IntentID)
		}
	}

	// Each intent <= 1 binding: a second binding for any intent is impossible.
	var duplicateIntents int
	if err := sqlDB.QueryRowContext(ctx, `SELECT count(*) FROM (
		SELECT intent_id FROM nonce_bindings GROUP BY intent_id HAVING count(*) > 1) d`).Scan(&duplicateIntents); err != nil {
		t.Fatalf("count duplicate intents: %v", err)
	}
	if duplicateIntents != 0 {
		t.Fatalf("intents with more than one binding = %d, want 0", duplicateIntents)
	}

	// No duplicate nonce among active (non-terminal) bindings in any scope.
	var duplicateActiveNonces int
	if err := sqlDB.QueryRowContext(ctx, `SELECT count(*) FROM (
		SELECT chain_id, sender, nonce FROM nonce_bindings
		WHERE state IN ('allocated', 'in_flight')
		GROUP BY chain_id, sender, nonce HAVING count(*) > 1) d`).Scan(&duplicateActiveNonces); err != nil {
		t.Fatalf("count duplicate active nonces: %v", err)
	}
	if duplicateActiveNonces != 0 {
		t.Fatalf("active bindings sharing a nonce within a scope = %d, want 0", duplicateActiveNonces)
	}

	// Contended scope A and independent scope B each own a clean 0..N-1 nonce
	// range: the parallel different sender is unaffected by A's contention.
	gotA := nonceT018Nonces(t, sqlDB, senderA)
	if !nonceT018SameInt64(gotA, nonceT018WantNonces(nonceT018IntentsA)) {
		t.Fatalf("sender A nonces = %v, want %v", gotA, nonceT018WantNonces(nonceT018IntentsA))
	}
	gotB := nonceT018Nonces(t, sqlDB, senderB)
	if !nonceT018SameInt64(gotB, nonceT018WantNonces(nonceT018IntentsB)) {
		t.Fatalf("sender B nonces = %v, want %v", gotB, nonceT018WantNonces(nonceT018IntentsB))
	}

	// The named DB UNIQUE carriers exist with the exact declared name...
	for _, name := range []string{"nonce_bindings_intent_uniq", "nonce_bindings_scope_nonce_uniq"} {
		var contype string
		if err := sqlDB.QueryRowContext(ctx,
			`SELECT contype FROM pg_constraint WHERE conname = $1`, name).Scan(&contype); err != nil {
			t.Fatalf("named unique carrier %s is absent: %v", name, err)
		}
		if contype != "u" {
			t.Fatalf("carrier %s contype = %q, want %q", name, contype, "u")
		}
	}
	// ...and they are enforced: a second binding on an existing nonce, and a
	// second binding for an existing intent, are both refused with the exact
	// constraint name (never a silent overwrite).
	const insertBinding = `INSERT INTO nonce_bindings
		(binding_id, intent_id, chain_id, sender, nonce, state, authorization_id,
		 authorization_version, registry_seq, allocation_observation_id)
		VALUES ($1, $2, $3, $4, $5, 'allocated', 't018-dup-auth', $6, 1, 'no-t018-dup')`
	authVersion := strings.Repeat("a", 64)
	_, err = sqlDB.ExecContext(ctx, insertBinding,
		"nb-t018-dup-nonce", "t018-dup-nonce", nonceT018ChainID, senderA, gotA[0], authVersion)
	nonceWantPgError(t, err, "23505", "nonce_bindings_scope_nonce_uniq")
	_, err = sqlDB.ExecContext(ctx, insertBinding,
		"nb-t018-dup-intent", targets[0].req.IntentID, nonceT018ChainID, senderA, int64(999999), authVersion)
	nonceWantPgError(t, err, "23505", "nonce_bindings_intent_uniq")
}

// nonceT018Request creates the per-intent FK prerequisites (one active 007
// authorization) and returns the target carrying the canonical request.
func nonceT018Request(t *testing.T, sqlDB *sql.DB, scope string, i int, sender, asset, recipient string) nonceT018Target {
	t.Helper()
	intentID := fmt.Sprintf("t018-%s-intent-%02d", scope, i)
	authorizationID := fmt.Sprintf("t018-%s-auth-%02d", scope, i)
	nonceMustExec(t, sqlDB, `INSERT INTO withdrawal_authorizations
		(authorization_id, caller_id, chain_id, asset, recipient, amount, state)
		VALUES ($1, 1, $2, $3, $4, 1, 'active')`,
		authorizationID, nonceT018ChainID, asset, recipient)
	return nonceT018Target{
		scope: sender,
		req: nonce.AllocationRequest{
			IntentID:        intentID,
			ChainID:         nonceT018ChainID,
			Sender:          sender,
			AuthorizationID: authorizationID,
		},
	}
}
