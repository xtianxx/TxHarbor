//go:build integration

// converge_integration_test.go owns spec task T022 for 008-nonce-manager: the
// T-converge contract on a real PostgreSQL 18 container (testcontainers).
//
// It drives the production admission core (`Allocator.Allocate`) and its
// fixed-order post-23505 diagnosis (`convergeAfterRace` -> `resolveAdmissionRace`)
// against durable rows, never against scripted doubles. Two insert races are
// staged: a concurrent same-intent insert and a concurrent same-scope
// different-intent insert. On the resulting 23505 the fixed-order classify must
// return the winner as a replay or a retryable outcome, and must never hand
// another intent's binding to the caller. A third case injects a commit-unknown
// (the durable commit succeeds, the caller sees an error) and proves a retry
// with the same allocation identity converges on the original binding.
//
// The test lives in package nonce (not nonce_test) because the converge path
// and the transaction opener are package-private: the injection is exactly the
// production branch under test (FR-02/05/06, V2, SC-02).
//
// Unlike migration_integration_test.go (a raw-SQL probe that must not import
// the classifiers), this file exercises the app-level classifiers directly.
package nonce

import (
	"bytes"
	"context"
	"errors"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/xtianxx/txharbor/internal/db"
)

const (
	convergeChain  = int64(4242)
	convergeSender = "0x4444444444444444444444444444444444444444"
	convergeAuth   = "wa-converge"
	convergeHash   = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
)

// convergeRPC answers the three R4 reads with an empty, healthy scope view
// (latest == pending == 0) so classification admits at the candidate derived
// from the durable frontier.
type convergeRPC struct{}

var _ rpcCaller = convergeRPC{}

func (convergeRPC) CallContext(_ context.Context, result any, method string, _ ...any) error {
	switch method {
	case methodTransactionCount:
		*(result.(*string)) = "0x0"
	case methodBlockByNumber:
		head := result.(**rpcHead)
		number := "0x1"
		*head = &rpcHead{Number: &number, Hash: "0x" + strings.Repeat("ab", 32)}
	default:
		return errors.New("convergeRPC: unexpected method " + method)
	}
	return nil
}

// convergeGateTx wraps the real admission transaction and, the first time the
// binding insert is about to execute, runs onBindingInsert. That hook commits a
// competing row on a second pooled connection while this transaction is open —
// a genuine concurrent insert race, staged deterministically.
type convergeGateTx struct {
	allocTx
	onBindingInsert func()
}

var _ allocTx = (*convergeGateTx)(nil)

func (t *convergeGateTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if t.onBindingInsert != nil && strings.Contains(sql, "INSERT INTO nonce_bindings ") {
		hook := t.onBindingInsert
		t.onBindingInsert = nil
		hook()
	}
	return t.allocTx.Exec(ctx, sql, args...)
}

// convergeCommitUnknownTx commits the real transaction and then reports an
// error to the caller: the durable commit happened, the caller cannot tell.
type convergeCommitUnknownTx struct {
	allocTx
	failNext bool
}

var _ allocTx = (*convergeCommitUnknownTx)(nil)

func (t *convergeCommitUnknownTx) Commit(ctx context.Context) error {
	if err := t.allocTx.Commit(ctx); err != nil {
		return err
	}
	if t.failNext {
		t.failNext = false
		return errors.New("injected commit-unknown after a durable commit")
	}
	return nil
}

// convergeSetup boots an isolated PostgreSQL container, applies the embedded
// migrations through 000008, and returns a pool bound to it. Skips (never
// passes) when no Docker provider is healthy.
func convergeSetup(t *testing.T) (context.Context, *pgxpool.Pool) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()
	ctr, err := postgres.Run(ctx, "postgres:18.6-trixie",
		postgres.WithDatabase("txharbor"),
		postgres.WithUsername("txharbor"),
		postgres.WithPassword("txharbor"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2)),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })

	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres connection string: %v", err)
	}
	var out bytes.Buffer
	if err := db.MigrateUp(ctx, db.MigrateOptions{
		DSN: dsn, LockTimeout: 5 * time.Second, ConnectTimeout: 5 * time.Second,
	}, &out); err != nil {
		t.Fatalf("MigrateUp() error = %v (output %q)", err, out.String())
	}
	pool, err := db.OpenPool(ctx, dsn, 5*time.Second)
	if err != nil {
		t.Fatalf("OpenPool: %v", err)
	}
	t.Cleanup(pool.Close)
	return ctx, pool
}

// convergeSeedScope registers the sender and supplies one active 007
// authorization so admission reaches the binding insert.
func convergeSeedScope(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	mustExec := func(query string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, query, args...); err != nil {
			t.Fatalf("exec %q: %v", query, err)
		}
	}
	mustExec(`INSERT INTO nonce_wallet_registry (chain_id, sender, state, registry_seq)
		VALUES ($1, $2, 'active', 1)`, convergeChain, convergeSender)
	mustExec(`INSERT INTO caller (caller_id, label, can_create) VALUES (1, 'converge', TRUE)`)
	mustExec(`INSERT INTO withdrawal_authorizations
		(authorization_id, caller_id, chain_id, asset, recipient, amount, state)
		VALUES ($1, 1, $2, $3, $4, 1, 'active')`,
		convergeAuth, convergeChain, convergeSender, convergeSender)
}

// convergeInsertBinding writes one committed binding directly (the competing
// writer of the staged race). The observation id carries no FK, so no evidence
// row is required.
func convergeInsertBinding(t *testing.T, ctx context.Context, pool *pgxpool.Pool, bindingID, intentID string, nonce int64) {
	t.Helper()
	if _, err := pool.Exec(ctx, `INSERT INTO nonce_bindings
		(binding_id, intent_id, chain_id, sender, nonce, state, authorization_id,
		 authorization_version, registry_seq, allocation_observation_id)
		VALUES ($1, $2, $3, $4, $5::numeric, 'allocated', $6, $7, 1, 'no-race')`,
		bindingID, intentID, convergeChain, convergeSender, nonce, convergeAuth, convergeHash); err != nil {
		t.Fatalf("concurrent insert %s: %v", bindingID, err)
	}
}

// convergeAllocator wires the real admission core with a scripted opener.
func convergeAllocator(pool *pgxpool.Pool, begin func(context.Context) (allocTx, error)) *Allocator {
	if begin == nil {
		begin = func(ctx context.Context) (allocTx, error) { return pool.Begin(ctx) }
	}
	return &Allocator{observer: NewObserver(convergeRPC{}, ObserverConfig{}), begin: begin}
}

// convergeCountBindings counts durable bindings for one intent.
func convergeCountBindings(t *testing.T, ctx context.Context, pool *pgxpool.Pool, intentID string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM nonce_bindings WHERE intent_id = $1`, intentID).Scan(&n); err != nil {
		t.Fatalf("count bindings for %s: %v", intentID, err)
	}
	return n
}

func convergeRequest(intentID string) AllocationRequest {
	return AllocationRequest{IntentID: intentID, ChainID: convergeChain, Sender: convergeSender, AuthorizationID: convergeAuth}
}

// TestNonceConvergeSameIntentRaceReplays stages a concurrent same-intent insert:
// while admission holds its open transaction just before the binding insert, a
// competing writer commits the same intent at a different nonce. The insert then
// raises 23505 on nonce_bindings_intent_uniq; the fixed-order classify must find
// the winner by intent, confirm input equality, and return it as a replay. No
// second binding is ever created.
func TestNonceConvergeSameIntentRaceReplays(t *testing.T) {
	ctx, pool := convergeSetup(t)
	convergeSeedScope(t, ctx, pool)
	req := convergeRequest("intent-race-same")

	begin := func(ctx context.Context) (allocTx, error) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			return nil, err
		}
		return &convergeGateTx{allocTx: tx, onBindingInsert: func() {
			convergeInsertBinding(t, ctx, pool, "nb-race-winner", req.IntentID, 7)
		}}, nil
	}

	binding, outcome, err := convergeAllocator(pool, begin).Allocate(ctx, req)
	if err != nil {
		t.Fatalf("Allocate same-intent race error = %v, want converged replay", err)
	}
	if outcome != OutcomeReplayed {
		t.Fatalf("outcome = %q, want %q", outcome, OutcomeReplayed)
	}
	if binding == nil || binding.BindingID != "nb-race-winner" || binding.Nonce.Cmp(big.NewInt(7)) != 0 {
		t.Fatalf("replay binding = %+v, want the raced winner nb-race-winner/7", binding)
	}
	if got := convergeCountBindings(t, ctx, pool, req.IntentID); got != 1 {
		t.Fatalf("bindings for %s = %d, want exactly 1", req.IntentID, got)
	}
}

// TestNonceConvergeScopeNonceRaceRetryable stages the same-scope different-intent
// race: the competing writer commits at the very nonce admission selected. The
// insert raises 23505 on nonce_bindings_scope_nonce_uniq; the fixed-order
// classify misses the intent probe, hits the scope+nonce carrier owned by the
// other intent, and must return a retryable outcome without adopting that
// binding.
func TestNonceConvergeScopeNonceRaceRetryable(t *testing.T) {
	ctx, pool := convergeSetup(t)
	convergeSeedScope(t, ctx, pool)
	req := convergeRequest("intent-race-scope")
	otherIntent := "intent-race-other"

	begin := func(ctx context.Context) (allocTx, error) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			return nil, err
		}
		return &convergeGateTx{allocTx: tx, onBindingInsert: func() {
			// The candidate is 0 (scope frontier empty); the other intent takes it.
			convergeInsertBinding(t, ctx, pool, "nb-race-other", otherIntent, 0)
		}}, nil
	}

	binding, outcome, err := convergeAllocator(pool, begin).Allocate(ctx, req)
	if !IsOutcome(err, OutcomeTemporarilyUnavailable) || outcome != OutcomeTemporarilyUnavailable {
		t.Fatalf("scope+nonce race = (%v, %q), want temporarily_unavailable", err, outcome)
	}
	if binding != nil {
		t.Fatalf("race adopted another intent's binding %+v", binding)
	}
	if got := convergeCountBindings(t, ctx, pool, req.IntentID); got != 0 {
		t.Fatalf("bindings for raced intent = %d, want 0", got)
	}
	if got := convergeCountBindings(t, ctx, pool, otherIntent); got != 1 {
		t.Fatalf("bindings for owning intent = %d, want its 1", got)
	}
}

// TestNonceConvergeCommitUnknownRetryConverges injects a commit-unknown: the
// durable commit lands but admission reports temporarily_unavailable. Retrying
// the same allocation identity must converge on the original binding with no
// second row and no reassignment.
func TestNonceConvergeCommitUnknownRetryConverges(t *testing.T) {
	ctx, pool := convergeSetup(t)
	convergeSeedScope(t, ctx, pool)
	req := convergeRequest("intent-commit-unknown")

	first := true
	begin := func(ctx context.Context) (allocTx, error) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			return nil, err
		}
		if first {
			first = false
			return &convergeCommitUnknownTx{allocTx: tx, failNext: true}, nil
		}
		return tx, nil
	}
	alloc := convergeAllocator(pool, begin)

	binding, outcome, err := alloc.Allocate(ctx, req)
	if !IsOutcome(err, OutcomeTemporarilyUnavailable) || outcome != OutcomeTemporarilyUnavailable {
		t.Fatalf("commit-unknown = (%v, %q), want temporarily_unavailable", err, outcome)
	}
	if binding != nil {
		t.Fatalf("commit-unknown returned binding %+v, want none", binding)
	}
	if got := convergeCountBindings(t, ctx, pool, req.IntentID); got != 1 {
		t.Fatalf("bindings after commit-unknown = %d, want the 1 durable row", got)
	}
	var winnerID, winnerNonce string
	if err := pool.QueryRow(ctx,
		`SELECT binding_id, nonce::text FROM nonce_bindings WHERE intent_id = $1`, req.IntentID).
		Scan(&winnerID, &winnerNonce); err != nil {
		t.Fatalf("read committed winner: %v", err)
	}

	binding, outcome, err = alloc.Allocate(ctx, req)
	if err != nil {
		t.Fatalf("retry after commit-unknown error = %v", err)
	}
	if outcome != OutcomeReplayed {
		t.Fatalf("retry outcome = %q, want %q", outcome, OutcomeReplayed)
	}
	if binding == nil || binding.BindingID != winnerID || FormatDecimal(binding.Nonce) != winnerNonce {
		t.Fatalf("retry binding = %+v, want the original %s/%s", binding, winnerID, winnerNonce)
	}
	if got := convergeCountBindings(t, ctx, pool, req.IntentID); got != 1 {
		t.Fatalf("bindings after retry = %d, want still 1 (no double allocation)", got)
	}
}
