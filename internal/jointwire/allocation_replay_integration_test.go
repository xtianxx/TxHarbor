//go:build integration

// allocation_replay_integration_test.go is Lane-F3 evidence closure: the REAL
// production assembly (jointwire.Worker) exposes the 008 allocator outcome, so
// the replay path is observed directly ("allocated" then "replayed") with the
// same intent, the same binding and the same nonce — not inferred from row
// counts alone. The outcome is returned as evidence only; the worker never
// branches on it.
package jointwire

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/nonce"
)

func replayMustExec(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sql string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(ctx, sql, args...); err != nil {
		t.Fatalf("seed exec failed: %v\n%s", err, sql)
	}
}

func TestWorkerAssemblyAllocationReplayOutcome(t *testing.T) {
	dsn := startMigratedPG(t)
	anvilURL := startAnvil(t)
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)

	cfg := entryConfig(dsn, anvilURL)
	deps, err := Worker(ctx, cfg, pool)
	if err != nil {
		t.Fatalf("Worker: %v", err)
	}
	if deps.AllocBinding == nil {
		t.Fatal("assembly left the 008 binding allocator nil")
	}

	const (
		intentID = "intent-assembly-replay"
		authID   = "auth-assembly-replay"
		sender   = "0xcccccccccccccccccccccccccccccccccccccccc"
		asset    = "0x1111111111111111111111111111111111111111"
		recp     = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	)
	replayMustExec(t, ctx, pool, `INSERT INTO caller (caller_id, label) VALUES (1, 'assembly-replay')`)
	replayMustExec(t, ctx, pool, `INSERT INTO withdrawal_authorizations
		(authorization_id, caller_id, chain_id, asset, recipient, amount, state)
		VALUES ($1, 1, 31337, $2, $3, 1000, 'active')`, authID, asset, recp)
	replayMustExec(t, ctx, pool, `INSERT INTO withdrawal_requests
		(request_id, caller_id, idempotency_key, authorization_id, chain_id, asset, recipient, amount)
		VALUES ('req-assembly-replay', 1, 'idem-assembly-replay', $1, 31337, $2, $3, 1000)`, authID, asset, recp)
	replayMustExec(t, ctx, pool, `INSERT INTO payment_intents
		(intent_id, request_id, chain_id, sender, authorization_id, authorization_version,
		 state, admitted_recovery_version)
		VALUES ($1, 'req-assembly-replay', 31337, $2, $3, 1, 'admitted', 0)`, intentID, sender, authID)
	replayMustExec(t, ctx, pool, `INSERT INTO nonce_wallet_registry (chain_id, sender, state, registry_seq)
		VALUES (31337, $1, 'active', 1)`, sender)
	replayMustExec(t, ctx, pool, `INSERT INTO nonce_scope_state (chain_id, sender) VALUES (31337, $1)`, sender)

	first, err := deps.AllocBinding(ctx, intentID)
	if err != nil {
		t.Fatalf("first AllocBinding: %v", err)
	}
	if first != string(nonce.OutcomeAllocated) {
		t.Fatalf("first AllocBinding outcome = %q, want %q", first, string(nonce.OutcomeAllocated))
	}
	var bindingID, bindingNonce string
	if err := pool.QueryRow(ctx,
		`SELECT binding_id, nonce::text FROM nonce_bindings WHERE intent_id = $1`, intentID).
		Scan(&bindingID, &bindingNonce); err != nil {
		t.Fatalf("read binding: %v", err)
	}

	// Identical re-request through the same assembly closure: the allocator
	// replays, keeping the same intent/binding/nonce with no second row.
	second, err := deps.AllocBinding(ctx, intentID)
	if err != nil {
		t.Fatalf("replay AllocBinding: %v", err)
	}
	if second != string(nonce.OutcomeReplayed) {
		t.Fatalf("replay AllocBinding outcome = %q, want %q", second, string(nonce.OutcomeReplayed))
	}
	var bindings, intents int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM nonce_bindings WHERE intent_id = $1`, intentID).Scan(&bindings); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM payment_intents WHERE request_id = 'req-assembly-replay'`).Scan(&intents); err != nil {
		t.Fatal(err)
	}
	if bindings != 1 || intents != 1 {
		t.Fatalf("replay grew the durable set: bindings=%d intents=%d, want 1/1", bindings, intents)
	}
	var replayBindingID, replayNonce string
	if err := pool.QueryRow(ctx,
		`SELECT binding_id, nonce::text FROM nonce_bindings WHERE intent_id = $1`, intentID).
		Scan(&replayBindingID, &replayNonce); err != nil {
		t.Fatal(err)
	}
	if replayBindingID != bindingID || replayNonce != bindingNonce {
		t.Fatalf("replay changed identity: binding %s->%s nonce %s->%s",
			bindingID, replayBindingID, bindingNonce, replayNonce)
	}
}
