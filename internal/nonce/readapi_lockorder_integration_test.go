//go:build integration

// readapi_lockorder_integration_test.go owns spec task T035 for
// 008-nonce-manager (US5; FR-14/15; read-api.md §4 bilateral; V9): the
// provider-side scope-row SHARE participation over a real PostgreSQL.
//
// What it proves:
//   - a read takes SELECT … FOR SHARE on the scope nonce_scope_state row and is
//     ordered after a concurrent writer's FOR UPDATE on that same row: the read
//     blocks until the writer commits, then completes. Reads never take the
//     reverse order;
//   - a 009-side consumer simulated with raw SQL — SELECT … FOR SHARE on the
//     same scope row, held into its own COMMIT (ordering only, no write) —
//     blocks an 008 writer (coordination row, then that scope row FOR UPDATE)
//     only until that commit and introduces no lock cycle (no deadlock);
//   - a read completes while a consumer holds the same scope row FOR SHARE
//     (share/share compatibility), so the read's lock is SHARE, never UPDATE;
//   - every 008 writer leaves the 006/007 tables byte-identical.
//
// The 009 client itself is out of scope (executed by 009); this file only
// simulates the consumer's ordering-only SHARE participation against a real
// PostgreSQL. Every helper this file owns is `lo`-prefixed so it cannot
// collide with a sibling integration test.
package nonce_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/nonce"
)

const (
	loChain = int64(80091)
	loAuth  = "wa-lo-1"
	loToken = "lo-read-token"
)

var (
	loSenderRead   = nonceAddr("e1")
	loSenderWriter = nonceAddr("e2")
)

// loUpstreamTables are every 006 and 007 domain table 008 reads but must never
// write. indexer_lease is deliberately excluded: it is the shared coordination
// row 008 serializes on (ensureCoordRowSQL materialises it), not 006-owned
// domain data.
var loUpstreamTables = []string{
	// 006
	"reorg_recovery", "reorg_recovery_events", "reorg_policy_history",
	"indexer_pause", "log_pause", "deposit_pause", "deposit_pause_audit",
	"chain_blocks", "indexer_checkpoint", "log_checkpoint",
	"erc20_transfer_logs", "deposit_observations", "deposit_checkpoint",
	"deposit_config_history", "confirmation_policy_history",
	"deposit_observation_transitions",
	// 007
	"caller", "api_key", "withdrawal_requests", "withdrawal_authorizations",
	"withdrawal_request_audit", "withdrawal_grant_audit",
}

// loRPC is the scripted JSON-RPC double: a self-consistent healthy chain view
// (latest == pending == 0, head 0) so each admission is a consistent allocation
// of the next nonce.
type loRPC struct{}

func (loRPC) CallContext(_ context.Context, result any, method string, args ...any) error {
	switch method {
	case "eth_getTransactionCount":
		return json.Unmarshal([]byte(`"0x0"`), result)
	case "eth_getBlockByNumber":
		payload := fmt.Sprintf(`{"number":"0x0","hash":%q}`, "0x"+strings.Repeat("ef", 32))
		return json.Unmarshal([]byte(payload), result)
	default:
		return fmt.Errorf("loRPC: unexpected method %q", method)
	}
}

func loAllocator(pool *pgxpool.Pool) *nonce.Allocator {
	observer := nonce.NewObserver(loRPC{}, nonce.ObserverConfig{
		RPCTimeout:   2 * time.Second,
		RetryInitial: time.Millisecond,
		RetryMax:     5 * time.Millisecond,
	})
	admitGate := nonce.NewRebuildGate()
	admitGate.Open()
	return nonce.NewAllocator(pool, observer, admitGate)
}

// loSeed inserts the 007 caller/authorization rows plus one registry + scope
// row per scenario sender, and one non-gate 006 history row so the upstream
// snapshot is not trivially empty.
func loSeed(t *testing.T, sqlDB *sql.DB, senders ...string) {
	t.Helper()
	nonceMustExec(t, sqlDB, `INSERT INTO caller (caller_id, label) VALUES (1, 'lo-it')`)
	nonceMustExec(t, sqlDB, `INSERT INTO withdrawal_authorizations
		(authorization_id, caller_id, chain_id, asset, recipient, amount, state)
		VALUES ($1, 1, $2, $3, $4, 1, 'active')`, loAuth, loChain, nonceAddr("c1"), nonceAddr("d1"))
	for _, sender := range senders {
		nonceMustExec(t, sqlDB, `INSERT INTO nonce_wallet_registry
			(chain_id, sender, state, registry_seq) VALUES ($1, $2, 'active', 1)`, loChain, sender)
		nonceMustExec(t, sqlDB, `INSERT INTO nonce_scope_state (chain_id, sender) VALUES ($1, $2)`, loChain, sender)
	}
	nonceMustExec(t, sqlDB, `INSERT INTO reorg_policy_history
		(chain_id, policy_seq, max_depth, prev_seq, operator, request_id)
		VALUES ($1, 1, 64, NULL, 'bootstrap', NULL)`, loChain)
}

// loSnapshot renders each 006/007 table's full row set as one canonical string;
// equality of the returned maps is the byte-identity assertion.
func loSnapshot(t *testing.T, sqlDB *sql.DB) map[string]string {
	t.Helper()
	out := make(map[string]string, len(loUpstreamTables))
	for _, table := range loUpstreamTables {
		var blob string
		query := fmt.Sprintf(
			`SELECT COALESCE(string_agg(t::text, E'\n' ORDER BY t::text), '<empty>') FROM %s t`, table)
		if err := sqlDB.QueryRow(query).Scan(&blob); err != nil {
			t.Fatalf("snapshot %s: %v", table, err)
		}
		out[table] = blob
	}
	return out
}

func loWantUpstreamUnchanged(t *testing.T, before, after map[string]string) {
	t.Helper()
	for _, table := range loUpstreamTables {
		if before[table] != after[table] {
			t.Fatalf("008 wrote a 006/007 row in %s:\nbefore=%q\nafter =%q",
				table, before[table], after[table])
		}
	}
}

// loLockScopeRowRaw opens one raw-SQL transaction that takes the named lock
// mode on the scope row and holds it until the caller commits (the simulated
// 009-side consumer, or a concurrent writer). SET LOCAL lock_timeout bounds the
// acquisition so a lock cycle surfaces as an error, never a hang.
func loLockScopeRowRaw(t *testing.T, sqlDB *sql.DB, mode, sender string) *sql.Tx {
	t.Helper()
	ctx := context.Background()
	tx, err := sqlDB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin raw %s lock tx: %v", mode, err)
	}
	t.Cleanup(func() { _ = tx.Rollback() }) // no-op after an explicit Commit
	if _, err := tx.ExecContext(ctx, `SET LOCAL lock_timeout = '4s'`); err != nil {
		t.Fatalf("set lock_timeout: %v", err)
	}
	q := fmt.Sprintf(`SELECT chain_id FROM nonce_scope_state
		WHERE chain_id = $1 AND sender = $2 FOR %s`, mode)
	var cid int64
	if err := tx.QueryRowContext(ctx, q, loChain, sender).Scan(&cid); err != nil {
		t.Fatalf("take scope row FOR %s: %v", mode, err)
	}
	return tx
}

func loCommit(t *testing.T, tx *sql.Tx) {
	t.Helper()
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit raw lock tx: %v", err)
	}
}

// loConsumerGateReads performs the 009 consumer's read-only gate reads under
// the held scope SHARE lock (fixed cross-transaction order: scope row → 006
// gate tables → 007 grant row). Reads only; the consumer writes nothing.
func loConsumerGateReads(t *testing.T, tx *sql.Tx) {
	t.Helper()
	ctx := context.Background()
	var n int
	if err := tx.QueryRowContext(ctx,
		`SELECT count(*) FROM reorg_recovery WHERE chain_id = $1`, loChain).Scan(&n); err != nil {
		t.Fatalf("consumer 006 gate read: %v", err)
	}
	if err := tx.QueryRowContext(ctx,
		`SELECT count(*) FROM withdrawal_authorizations WHERE authorization_id = $1`, loAuth).Scan(&n); err != nil {
		t.Fatalf("consumer 007 grant read: %v", err)
	}
}

type loReadResult struct {
	resp nonce.ReadResponse
	err  error
}

type loWriteResult struct {
	binding *nonce.Binding
	outcome nonce.Outcome
	err     error
}

// TestNonceReadAPILockOrderIntegration is the T035 acceptance over one migrated
// scratch database. Real PostgreSQL; no Anvil (the chain view is scripted).
func TestNonceReadAPILockOrderIntegration(t *testing.T) {
	dsn := nonceStartPostgres(t)
	ctx := context.Background()

	var out bytes.Buffer
	if err := db.MigrateUp(ctx, nonceMigrateOptions(dsn), &out); err != nil {
		t.Fatalf("MigrateUp() error = %v (output %q)", err, out.String())
	}
	sqlDB := nonceOpenSQL(t, dsn)
	loSeed(t, sqlDB, loSenderRead, loSenderWriter)

	pool := replayOpenPool(t, dsn)
	alloc := loAllocator(pool)
	reader := nonce.NewReadProvider(pool, loToken)

	// Upstream (006/007) rows before any 008 writer runs.
	upstreamBefore := loSnapshot(t, sqlDB)

	// Seed one committed binding per scenario scope (itself an 008 writer).
	readBinding, outcome, err := alloc.Allocate(ctx, nonce.AllocationRequest{
		IntentID: "lo-intent-read", ChainID: loChain, Sender: loSenderRead, AuthorizationID: loAuth,
	})
	if err != nil || outcome != nonce.OutcomeAllocated || readBinding == nil {
		t.Fatalf("seed binding for read scope = (%v, %q, %v), want allocated", readBinding, outcome, err)
	}
	firstWrite, outcome, err := alloc.Allocate(ctx, nonce.AllocationRequest{
		IntentID: "lo-intent-write-0", ChainID: loChain, Sender: loSenderWriter, AuthorizationID: loAuth,
	})
	if err != nil || outcome != nonce.OutcomeAllocated || firstWrite == nil {
		t.Fatalf("seed binding for writer scope = (%v, %q, %v), want allocated", firstWrite, outcome, err)
	}

	// --- reads are ordered after a writer's FOR UPDATE on the scope row -----
	t.Run("read_ordered_after_writer_for_update", func(t *testing.T) {
		holder := loLockScopeRowRaw(t, sqlDB, "UPDATE", loSenderRead)

		rctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		done := make(chan loReadResult, 1)
		go func() {
			resp, rerr := reader.ReadByBindingID(rctx, readBinding.BindingID)
			done <- loReadResult{resp, rerr}
		}()

		select {
		case res := <-done:
			t.Fatalf("read returned while the scope row was held FOR UPDATE (no ordering): outcome=%q err=%v",
				res.resp.Outcome, res.err)
		case <-time.After(300 * time.Millisecond):
		}

		loCommit(t, holder)
		select {
		case res := <-done:
			if res.err != nil || res.resp.Outcome != nonce.ReadBound {
				t.Fatalf("read after the writer committed = (%q, %v), want bound", res.resp.Outcome, res.err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("read did not complete after the scope FOR UPDATE committed")
		}
	})

	// --- a consumer's SHARE held into its own commit blocks 008 writers -----
	t.Run("consumer_share_blocks_writer_until_commit", func(t *testing.T) {
		consumer := loLockScopeRowRaw(t, sqlDB, "SHARE", loSenderWriter)
		loConsumerGateReads(t, consumer)

		// Share/share compatible: the read must complete promptly under the
		// consumer's SHARE lock (so the read holds SHARE, never UPDATE).
		rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		resp, rerr := reader.ReadByBindingID(rctx, firstWrite.BindingID)
		if rerr != nil || resp.Outcome != nonce.ReadBound {
			t.Fatalf("read under the consumer's SHARE = (%q, %v), want bound (share/share compatible)",
				resp.Outcome, rerr)
		}

		// The 008 writer takes the coordination row, then the same scope row
		// FOR UPDATE: it must block only until the consumer commits, no cycle.
		wctx, wcancel := context.WithTimeout(ctx, 15*time.Second)
		defer wcancel()
		wdone := make(chan loWriteResult, 1)
		go func() {
			b, o, werr := alloc.Allocate(wctx, nonce.AllocationRequest{
				IntentID: "lo-intent-write-1", ChainID: loChain, Sender: loSenderWriter, AuthorizationID: loAuth,
			})
			wdone <- loWriteResult{b, o, werr}
		}()

		select {
		case res := <-wdone:
			t.Fatalf("008 writer returned while the consumer held SHARE: outcome=%q err=%v", res.outcome, res.err)
		case <-time.After(300 * time.Millisecond):
		}

		loCommit(t, consumer)
		select {
		case res := <-wdone:
			if res.err != nil || res.outcome != nonce.OutcomeAllocated || res.binding == nil {
				t.Fatalf("008 writer after the consumer committed = (%v, %q, %v), want allocated",
					res.binding, res.outcome, res.err)
			}
			if got := res.binding.Nonce.String(); got != "1" {
				t.Fatalf("blocked writer nonce = %s, want 1 (ordered after the first binding)", got)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("008 writer did not complete after the consumer committed")
		}
	})

	// --- a read blocked past the lock bound is unavailable, then releases ----
	// read-api.md §4: a read that cannot take the scope FOR SHARE because a
	// writer holds FOR UPDATE fails closed as the retryable `unavailable`
	// (never a partial/hung answer), and the pooled transaction is released so
	// the next read succeeds.
	t.Run("read_lock_timeout_is_unavailable_then_releases", func(t *testing.T) {
		holder := loLockScopeRowRaw(t, sqlDB, "UPDATE", loSenderRead)

		rctx, cancel := context.WithTimeout(ctx, 25*time.Second)
		defer cancel()
		start := time.Now()
		done := make(chan loReadResult, 1)
		go func() {
			resp, rerr := reader.ReadByBindingID(rctx, readBinding.BindingID)
			done <- loReadResult{resp, rerr}
		}()

		select {
		case res := <-done:
			if res.err == nil || !nonce.IsOutcome(res.err, nonce.ReadUnavailable) ||
				res.resp.Outcome != nonce.ReadUnavailable {
				t.Fatalf("blocked read = (%q, %v), want unavailable", res.resp.Outcome, res.err)
			}
			if elapsed := time.Since(start); elapsed < 4*time.Second {
				t.Fatalf("blocked read returned after %s; want the DB lock bound to wait, not the caller ctx", elapsed)
			}
		case <-time.After(20 * time.Second):
			t.Fatal("blocked read never returned; the lock bound did not fire")
		}

		loCommit(t, holder)

		fctx, fcancel := context.WithTimeout(ctx, 10*time.Second)
		defer fcancel()
		resp, rerr := reader.ReadByBindingID(fctx, readBinding.BindingID)
		if rerr != nil || resp.Outcome != nonce.ReadBound {
			t.Fatalf("post-timeout read = (%q, %v), want bound (tx released)", resp.Outcome, rerr)
		}
	})

	// --- 008 writers never write 006/007 rows ------------------------------
	if !t.Failed() {
		loWantUpstreamUnchanged(t, upstreamBefore, loSnapshot(t, sqlDB))
	}
}
