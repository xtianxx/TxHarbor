//go:build integration

// allocate_replay_integration_test.go owns spec task T019 for 008-nonce-manager
// (V2, FR-04/FR-05, SC-02): replay and conflict behavior over a real
// PostgreSQL, exercised through the exported Allocator against a scripted JSON-
// RPC observation.
//
// Scenarios:
//   - sequential retry: an equal-input Allocate returns the original
//     binding_id/nonce and writes zero new rows;
//   - concurrent duplicate: N same-intent admissions yield exactly one
//     allocation and N-1 replays of that one binding;
//   - retry after restart: a fresh pool/handle against the same DB (process-
//     level teardown simulated at the pooled-handle boundary, NOT a real kill —
//     that is T021's job) still replays the persisted original;
//   - same intent, differing chain_id / sender / authorization_id: each yields
//     allocation_conflict with the original untouched and zero second binding.
//
// The container/migration pattern is reused from migration_integration_test.go
// (isolated scratch DB per test function, T005 helpers). Only exported 008 API
// is touched; the block-result shape is unexported, so the RPC double answers
// through JSON exactly the way the real geth client would.
package nonce_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/nonce"
)

const (
	replayChain1 = int64(80001)
	replayChain2 = int64(80002)

	replayAuth1 = "wa-replay-1" // chain1, base scope
	replayAuth2 = "wa-replay-2" // chain1, differing-authorization case
	replayAuth3 = "wa-replay-3" // chain2, differing-chain case (auth PK is global)
)

var (
	replaySender1 = nonceAddr("11")
	replaySender2 = nonceAddr("22")
)

// replayRPC is the scripted JSON-RPC double: a frozen, self-consistent chain
// view (latest == pending == 0, head 0). The values are JSON-encoded because
// the observer's block-result type is package-private.
type replayRPC struct {
	countJSON  string // a JSON string literal, e.g. `"0x0"`
	headNumber string
	headHash   string
}

// replayConsistentRPC is the healthy-view double used by every T019 scenario:
// with no bindings it classifies consistent at candidate 0.
func replayConsistentRPC() replayRPC {
	return replayRPC{
		countJSON:  `"0x0"`,
		headNumber: "0x0",
		headHash:   "0x" + strings.Repeat("ab", 32),
	}
}

func (r replayRPC) CallContext(_ context.Context, result any, method string, args ...any) error {
	switch method {
	case "eth_getTransactionCount":
		return json.Unmarshal([]byte(r.countJSON), result)
	case "eth_getBlockByNumber":
		payload := fmt.Sprintf(`{"number":%q,"hash":%q}`, r.headNumber, r.headHash)
		return json.Unmarshal([]byte(payload), result)
	default:
		return fmt.Errorf("replayRPC: unexpected method %q", method)
	}
}

func replayAllocator(t *testing.T, pool *pgxpool.Pool) *nonce.Allocator {
	t.Helper()
	observer := nonce.NewObserver(replayConsistentRPC(), nonce.ObserverConfig{
		RPCTimeout:   2 * time.Second,
		RetryInitial: time.Millisecond,
		RetryMax:     5 * time.Millisecond,
	})
	return nonce.NewAllocator(pool, observer)
}

// replayOpenPool opens one fresh pooled handle for the given DSN; Close is
// idempotent, so the t.Cleanup registration is safe even when a scenario
// closes the pool early to simulate a restart.
func replayOpenPool(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := db.OpenPool(ctx, dsn, 5*time.Second)
	if err != nil {
		t.Fatalf("open pgx pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// replaySeed inserts the 007 caller/authorization rows and the 008 wallet
// registry rows every T019 scenario needs to pass the admission gates. The
// scrubbed single caller keeps the 007 FK satisfied without granting anything.
func replaySeed(t *testing.T, sqlDB *sql.DB) {
	t.Helper()
	nonceMustExec(t, sqlDB, `INSERT INTO caller (caller_id, label) VALUES (1, 'replay-it')`)

	for _, reg := range []struct {
		chain  int64
		sender string
	}{
		{replayChain1, replaySender1},
		{replayChain1, replaySender2},
		{replayChain2, replaySender1},
	} {
		nonceMustExec(t, sqlDB, `INSERT INTO nonce_wallet_registry
			(chain_id, sender, state, registry_seq) VALUES ($1, $2, 'active', 1)`,
			reg.chain, reg.sender)
	}

	for _, a := range []struct {
		id    string
		chain int64
	}{
		{replayAuth1, replayChain1},
		{replayAuth2, replayChain1},
		{replayAuth3, replayChain2},
	} {
		nonceMustExec(t, sqlDB, `INSERT INTO withdrawal_authorizations
			(authorization_id, caller_id, chain_id, asset, recipient, amount, state)
			VALUES ($1, 1, $2, $3, $4, 1, 'active')`,
			a.id, a.chain, nonceAddr("aa"), nonceAddr("bb"))
	}
}

// replayRows is the whole-DB row census used to prove "zero new rows".
type replayRows struct {
	bindings     int
	events       int
	observations int
	holds        int
	scopes       int
}

func replayCountRows(t *testing.T, sqlDB *sql.DB) replayRows {
	t.Helper()
	var r replayRows
	if err := sqlDB.QueryRow(`SELECT
		(SELECT count(*) FROM nonce_bindings),
		(SELECT count(*) FROM nonce_binding_events),
		(SELECT count(*) FROM nonce_observations),
		(SELECT count(*) FROM nonce_scope_holds),
		(SELECT count(*) FROM nonce_scope_state)`).
		Scan(&r.bindings, &r.events, &r.observations, &r.holds, &r.scopes); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	return r
}

type replayBinding struct {
	bindingID string
	nonce     string
	state     string
	chainID   int64
	sender    string
	authID    string
}

func replayBindingByIntent(t *testing.T, sqlDB *sql.DB, intentID string) replayBinding {
	t.Helper()
	var b replayBinding
	if err := sqlDB.QueryRow(`SELECT binding_id, nonce::text, state, chain_id, sender, authorization_id
		FROM nonce_bindings WHERE intent_id = $1`, intentID).
		Scan(&b.bindingID, &b.nonce, &b.state, &b.chainID, &b.sender, &b.authID); err != nil {
		t.Fatalf("read binding by intent %q: %v", intentID, err)
	}
	return b
}

func replayIntentBindingCount(t *testing.T, sqlDB *sql.DB, intentID string) int {
	t.Helper()
	var n int
	if err := sqlDB.QueryRow(`SELECT count(*) FROM nonce_bindings WHERE intent_id = $1`, intentID).
		Scan(&n); err != nil {
		t.Fatalf("count bindings for %q: %v", intentID, err)
	}
	return n
}

// TestNonceAllocateReplayConflictIntegration is the T019 V2 acceptance: every
// scenario runs against one migrated scratch database with isolated scopes.
func TestNonceAllocateReplayConflictIntegration(t *testing.T) {
	dsn := nonceStartPostgres(t)
	ctx := context.Background()

	var out bytes.Buffer
	if err := db.MigrateUp(ctx, nonceMigrateOptions(dsn), &out); err != nil {
		t.Fatalf("MigrateUp() error = %v (output %q)", err, out.String())
	}
	sqlDB := nonceOpenSQL(t, dsn)
	replaySeed(t, sqlDB)

	pool := replayOpenPool(t, dsn)
	alloc := replayAllocator(t, pool)

	base := nonce.AllocationRequest{
		IntentID:        "replay-intent-sequential",
		ChainID:         replayChain1,
		Sender:          replaySender1,
		AuthorizationID: replayAuth1,
	}

	// --- sequential retry -------------------------------------------------
	first, outcome, err := alloc.Allocate(ctx, base)
	if err != nil || outcome != nonce.OutcomeAllocated || first == nil {
		t.Fatalf("first allocate = (%+v, %q, %v), want a new binding", first, outcome, err)
	}
	if first.Nonce.String() != "0" || first.State != nonce.StateAllocated {
		t.Fatalf("first binding = nonce %s/state %s, want 0/allocated", first.Nonce, first.State)
	}
	afterAlloc := replayCountRows(t, sqlDB)

	replay, outcome, err := alloc.Allocate(ctx, base)
	if err != nil || outcome != nonce.OutcomeReplayed {
		t.Fatalf("sequential retry = (%+v, %q, %v), want replayed", replay, outcome, err)
	}
	if replay == nil || replay.BindingID != first.BindingID || replay.Nonce.Cmp(first.Nonce) != 0 {
		t.Fatalf("replay = %+v, want original %s/%s", replay, first.BindingID, first.Nonce)
	}
	if got := replayCountRows(t, sqlDB); got != afterAlloc {
		t.Fatalf("sequential replay wrote rows: %+v, want %+v", got, afterAlloc)
	}

	// --- concurrent duplicate --------------------------------------------
	concurrent := base
	concurrent.IntentID = "replay-intent-concurrent"
	beforeDup := replayCountRows(t, sqlDB)

	type dupResult struct {
		binding *nonce.Binding
		outcome nonce.Outcome
		err     error
	}
	const dupN = 4
	results := make([]dupResult, dupN)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < dupN; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			b, o, e := alloc.Allocate(ctx, concurrent)
			results[i] = dupResult{binding: b, outcome: o, err: e}
		}(i)
	}
	close(start)
	wg.Wait()

	allocated, replayed := 0, 0
	var winner *nonce.Binding
	for _, r := range results {
		switch r.outcome {
		case nonce.OutcomeAllocated:
			allocated++
			winner = r.binding
		case nonce.OutcomeReplayed:
			replayed++
		default:
			t.Fatalf("concurrent duplicate outcome = %q (err %v), want allocated/replayed", r.outcome, r.err)
		}
		if r.err != nil {
			t.Fatalf("concurrent duplicate returned error %v", r.err)
		}
	}
	if allocated != 1 || replayed != dupN-1 {
		t.Fatalf("concurrent duplicate = %d allocated / %d replayed, want 1 / %d", allocated, replayed, dupN-1)
	}
	if winner == nil {
		t.Fatal("concurrent duplicate produced no winning binding")
	}
	for i, r := range results {
		if r.binding == nil || r.binding.BindingID != winner.BindingID || r.binding.Nonce.Cmp(winner.Nonce) != 0 {
			t.Fatalf("result %d = %+v, want the winner %s/%s", i, r.binding, winner.BindingID, winner.Nonce)
		}
	}
	afterDup := replayCountRows(t, sqlDB)
	if afterDup.bindings != beforeDup.bindings+1 ||
		afterDup.events != beforeDup.events+1 ||
		afterDup.observations != beforeDup.observations+1 ||
		afterDup.holds != beforeDup.holds ||
		afterDup.scopes != beforeDup.scopes {
		t.Fatalf("concurrent duplicate rows = %+v, want one binding/event/observation over %+v", afterDup, beforeDup)
	}

	// --- retry after restart ---------------------------------------------
	beforeRestart := replayCountRows(t, sqlDB)
	pool.Close() // pool-level teardown; the durable rows are the only basis
	restarted := replayOpenPool(t, dsn)
	alloc = replayAllocator(t, restarted)

	replay, outcome, err = alloc.Allocate(ctx, concurrent)
	if err != nil || outcome != nonce.OutcomeReplayed {
		t.Fatalf("post-restart retry = (%+v, %q, %v), want replayed", replay, outcome, err)
	}
	if replay == nil || replay.BindingID != winner.BindingID || replay.Nonce.Cmp(winner.Nonce) != 0 {
		t.Fatalf("post-restart replay = %+v, want original %s/%s", replay, winner.BindingID, winner.Nonce)
	}
	if got := replayCountRows(t, sqlDB); got != beforeRestart {
		t.Fatalf("post-restart replay wrote rows: %+v, want %+v", got, beforeRestart)
	}

	// --- same intent, differing input ------------------------------------
	beforeDiff := replayCountRows(t, sqlDB)
	original := replayBindingByIntent(t, sqlDB, base.IntentID)

	differing := []struct {
		name string
		req  nonce.AllocationRequest
	}{
		// The authorization_id PK is global, so a differing chain necessarily
		// carries the chain-2 authorization; the chain field is still the first
		// equality mismatch the classifier sees.
		{"chain_id", nonce.AllocationRequest{
			IntentID: base.IntentID, ChainID: replayChain2,
			Sender: replaySender1, AuthorizationID: replayAuth3,
		}},
		{"sender", nonce.AllocationRequest{
			IntentID: base.IntentID, ChainID: replayChain1,
			Sender: replaySender2, AuthorizationID: replayAuth1,
		}},
		{"authorization_id", nonce.AllocationRequest{
			IntentID: base.IntentID, ChainID: replayChain1,
			Sender: replaySender1, AuthorizationID: replayAuth2,
		}},
	}
	for _, tc := range differing {
		t.Run("differ-"+tc.name, func(t *testing.T) {
			binding, outcome, err := alloc.Allocate(ctx, tc.req)
			if !nonce.IsOutcome(err, nonce.OutcomeAllocationConflict) || outcome != nonce.OutcomeAllocationConflict {
				t.Fatalf("differing %s = (%+v, %q, %v), want allocation_conflict", tc.name, binding, outcome, err)
			}
			if binding != nil {
				t.Fatalf("conflict returned binding %+v, want none", binding)
			}
			if got := replayCountRows(t, sqlDB); got != beforeDiff {
				t.Fatalf("conflict wrote rows: %+v, want %+v", got, beforeDiff)
			}
			if got := replayIntentBindingCount(t, sqlDB, base.IntentID); got != 1 {
				t.Fatalf("intent has %d bindings after conflict, want 1", got)
			}
			kept := replayBindingByIntent(t, sqlDB, base.IntentID)
			if kept != original {
				t.Fatalf("original binding changed: %+v, want %+v", kept, original)
			}
		})
	}
}
