//go:build integration

// rebuild_integration_test.go owns spec task T023 for 008-nonce-manager: it
// proves the startup rebuild gate (R5/FR-13) is fail-closed over a real
// PostgreSQL. While the gate is closed every admission refuses
// rebuild_incomplete with the recorded cause and performs zero RPC/DB work; a
// successful VerifyRebuild is the only condition that opens it; a failed
// verification keeps it closed and repairs nothing; and a database that is
// unavailable fails closed (never a memory-derived nonce).
//
// The gate is driven at the carrier level — a real nonce.Allocator over a
// migrated testcontainers database — so no Serve process is needed. Each test
// boots its own isolated scratch PostgreSQL container via the T005 harness and
// terminates it in t.Cleanup.
package nonce_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/nonce"
)

const (
	rebuildChainID = int64(1)
	// rebuildDeclaredCarriers is len(nonce.requiredConstraintNames): the
	// migration declares all 49 explicitly and VerifyRebuild probes exactly
	// that set.
	rebuildDeclaredCarriers = 49
)

// rebuildMigratedPool boots an isolated scratch PostgreSQL, applies
// 000001-000008 as one goose up, and returns a pool over the migrated schema.
func rebuildMigratedPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := nonceStartPostgres(t)
	var out bytes.Buffer
	if err := db.MigrateUp(context.Background(), nonceMigrateOptions(dsn), &out); err != nil {
		t.Fatalf("MigrateUp: %v (output %q)", err, out.String())
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open pgxpool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func rebuildMustExec(t *testing.T, pool *pgxpool.Pool, query string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), query, args...); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

func rebuildCount(t *testing.T, pool *pgxpool.Pool, query string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	return n
}

func rebuildSender() string { return nonceAddr("22") }

// rebuildSeedScope registers an active wallet and an active 007 authorization
// so a gate-open admission can complete end to end.
func rebuildSeedScope(t *testing.T, pool *pgxpool.Pool, authorizationID string) {
	t.Helper()
	rebuildMustExec(t, pool, `INSERT INTO caller (caller_id, label) VALUES (1, 'rebuild-gate')`)
	rebuildMustExec(t, pool, `INSERT INTO nonce_wallet_registry (chain_id, sender, state, registry_seq)
		VALUES ($1, $2, 'active', 1)`, rebuildChainID, rebuildSender())
	rebuildMustExec(t, pool, `INSERT INTO withdrawal_authorizations
		(authorization_id, caller_id, chain_id, asset, recipient, amount, state, expires_at)
		VALUES ($1, 1, $2, $3, $4, '1', 'active', now() + interval '1 hour')`,
		authorizationID, rebuildChainID, nonceAddr("aa"), nonceAddr("bb"))
}

// rebuildRPC is a scripted chain view: latest = pending = 0 and a valid head,
// so the first admission is consistent and derives candidate nonce 0 without
// any Anvil node.
type rebuildRPC struct{ calls int }

func (r *rebuildRPC) CallContext(_ context.Context, result any, method string, _ ...any) error {
	r.calls++
	switch method {
	case "eth_getTransactionCount":
		return json.Unmarshal([]byte(`"0x0"`), result)
	case "eth_getBlockByNumber":
		head := `{"number":"0x5","hash":"0x` + strings.Repeat("ab", 32) + `"}`
		return json.Unmarshal([]byte(head), result)
	default:
		return fmt.Errorf("rebuildRPC: unexpected method %q", method)
	}
}

func rebuildAllocator(pool *pgxpool.Pool, rpc *rebuildRPC, gate *nonce.RebuildGate) *nonce.Allocator {
	return nonce.NewAllocator(pool, nonce.NewObserver(rpc, nonce.ObserverConfig{}), gate)
}

func rebuildRequest(intentID, authorizationID string) nonce.AllocationRequest {
	return nonce.AllocationRequest{
		IntentID:        intentID,
		ChainID:         rebuildChainID,
		Sender:          rebuildSender(),
		AuthorizationID: authorizationID,
	}
}

func TestNonceRebuildGateClosedRefusesAllocation(t *testing.T) {
	pool := rebuildMigratedPool(t)
	rebuildSeedScope(t, pool, "auth-rebuild-closed")

	gate := nonce.NewRebuildGate()
	gate.KeepClosed("scope integrity unreadable")
	rpc := &rebuildRPC{}
	alloc := rebuildAllocator(pool, rpc, gate)

	binding, outcome, err := alloc.Allocate(context.Background(),
		rebuildRequest("intent-rebuild-closed", "auth-rebuild-closed"))
	if binding != nil {
		t.Fatalf("closed gate returned a binding: %+v", binding)
	}
	if outcome != nonce.OutcomeRebuildIncomplete || !nonce.IsOutcome(err, nonce.OutcomeRebuildIncomplete) {
		t.Fatalf("Allocate = (%v, %q, %v), want rebuild_incomplete", binding, outcome, err)
	}
	if !strings.Contains(err.Error(), "scope integrity unreadable") {
		t.Fatalf("refusal %q does not record the gate cause", err)
	}
	if rpc.calls != 0 {
		t.Fatalf("gate-closed admission performed %d RPC calls, want 0", rpc.calls)
	}
	if n := rebuildCount(t, pool, `SELECT count(*) FROM nonce_bindings`); n != 0 {
		t.Fatalf("gate-closed admission wrote %d bindings, want 0", n)
	}
	if n := rebuildCount(t, pool, `SELECT count(*) FROM nonce_observations`); n != 0 {
		t.Fatalf("gate-closed admission wrote %d observations, want 0", n)
	}
}

func TestNonceRebuildVerificationSuccessOpensGate(t *testing.T) {
	pool := rebuildMigratedPool(t)
	rebuildSeedScope(t, pool, "auth-rebuild-open")

	gate := nonce.NewRebuildGate()
	alloc := rebuildAllocator(pool, &rebuildRPC{}, gate)
	req := rebuildRequest("intent-rebuild-open", "auth-rebuild-open")

	if _, outcome, err := alloc.Allocate(context.Background(), req); outcome != nonce.OutcomeRebuildIncomplete {
		t.Fatalf("closed gate outcome = %q (%v), want rebuild_incomplete", outcome, err)
	}

	report, err := nonce.VerifyRebuild(context.Background(), pool, rebuildChainID)
	if err != nil {
		t.Fatalf("VerifyRebuild: %v", err)
	}
	if report.ConstraintsFound != rebuildDeclaredCarriers {
		t.Fatalf("ConstraintsFound = %d, want %d", report.ConstraintsFound, rebuildDeclaredCarriers)
	}
	gate.Open()
	if !gate.IsOpen() {
		t.Fatal("gate stayed closed after verification success")
	}

	binding, outcome, err := alloc.Allocate(context.Background(), req)
	if err != nil {
		t.Fatalf("Allocate after verification success: %v", err)
	}
	if outcome != nonce.OutcomeAllocated || binding == nil {
		t.Fatalf("Allocate = (%+v, %q), want allocated", binding, outcome)
	}
	if binding.Nonce == nil || binding.Nonce.Sign() != 0 {
		t.Fatalf("binding nonce = %v, want 0", binding.Nonce)
	}
	if n := rebuildCount(t, pool, `SELECT count(*) FROM nonce_bindings WHERE intent_id = $1`, req.IntentID); n != 1 {
		t.Fatalf("persisted bindings for the intent = %d, want 1", n)
	}
}

func TestNonceRebuildVerificationFailureKeepsGateClosed(t *testing.T) {
	pool := rebuildMigratedPool(t)
	rebuildSeedScope(t, pool, "auth-rebuild-fail")
	rebuildMustExec(t, pool, `ALTER TABLE nonce_bindings DROP CONSTRAINT nonce_bindings_nonce_range`)

	gate := nonce.NewRebuildGate()
	if _, err := nonce.VerifyRebuild(context.Background(), pool, rebuildChainID); err == nil {
		t.Fatal("VerifyRebuild succeeded on a schema missing a named carrier")
	} else {
		gate.KeepClosed("rebuild_incomplete: " + err.Error())
		if !strings.Contains(err.Error(), "named carriers") {
			t.Fatalf("verification error = %v, want named-carrier refusal", err)
		}
	}
	if gate.IsOpen() {
		t.Fatal("failed verification opened the gate")
	}
	if n := rebuildCount(t, pool, `SELECT count(*) FROM pg_constraint WHERE conname = 'nonce_bindings_nonce_range'`); n != 0 {
		t.Fatal("verification silently repaired the dropped carrier")
	}

	rpc := &rebuildRPC{}
	alloc := rebuildAllocator(pool, rpc, gate)
	binding, outcome, err := alloc.Allocate(context.Background(),
		rebuildRequest("intent-rebuild-fail", "auth-rebuild-fail"))
	if binding != nil || !nonce.IsOutcome(err, nonce.OutcomeRebuildIncomplete) {
		t.Fatalf("Allocate = (%v, %q, %v), want rebuild_incomplete", binding, outcome, err)
	}
	if rpc.calls != 0 {
		t.Fatalf("gate-closed admission performed %d RPC calls, want 0", rpc.calls)
	}
	if n := rebuildCount(t, pool, `SELECT count(*) FROM nonce_bindings`); n != 0 {
		t.Fatalf("gate-closed admission wrote %d bindings, want 0", n)
	}
}

func TestNonceRebuildGateDBUnavailableFailsClosed(t *testing.T) {
	pool := rebuildMigratedPool(t)
	pool.Close()

	gate := nonce.NewRebuildGate()
	if _, err := nonce.VerifyRebuild(context.Background(), pool, rebuildChainID); err == nil {
		t.Fatal("VerifyRebuild succeeded with the database unavailable")
	} else {
		gate.KeepClosed("rebuild_incomplete: " + err.Error())
	}

	rpc := &rebuildRPC{}
	alloc := rebuildAllocator(pool, rpc, gate)
	req := rebuildRequest("intent-rebuild-unavail", "auth-rebuild-unavail")

	binding, outcome, err := alloc.Allocate(context.Background(), req)
	if binding != nil || !nonce.IsOutcome(err, nonce.OutcomeRebuildIncomplete) {
		t.Fatalf("Allocate = (%v, %q, %v), want rebuild_incomplete", binding, outcome, err)
	}
	if rpc.calls != 0 {
		t.Fatalf("gate-closed admission performed %d RPC calls, want 0", rpc.calls)
	}

	gate.Open()
	binding, outcome, err = alloc.Allocate(context.Background(), req)
	if binding != nil {
		t.Fatalf("unavailable database produced a binding from memory: %+v", binding)
	}
	if outcome != nonce.OutcomeTemporarilyUnavailable || !nonce.IsOutcome(err, nonce.OutcomeTemporarilyUnavailable) {
		t.Fatalf("Allocate = (%q, %v), want temporarily_unavailable", outcome, err)
	}
}
