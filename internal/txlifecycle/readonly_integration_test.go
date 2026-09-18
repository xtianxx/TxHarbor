//go:build integration

package txlifecycle

import (
	"context"
	"fmt"
	"testing"

	"github.com/ethereum/go-ethereum/core/types"
)

// upstreamTables are the tables 010 only ever reads (FR-07/FR-12/FR-13/FR-14;
// persistence.md §6). V10 diffs their content across a lifecycle run.
var upstreamTables = []string{
	"indexer_pause", "log_pause", "deposit_pause",
	"reorg_recovery", "reorg_recovery_events", "chain_blocks",
	"withdrawal_authorizations", "withdrawal_authorization_scopes",
	"nonce_bindings", "nonce_scope_state", "nonce_scope_holds",
	"nonce_wallet_registry", "confirmation_policy_history", "execution_claims",
}

// snapshotUpstream content-hashes every read-only consumer table; row order is
// fixed by the text ordering, so a changed byte changes the digest.
func snapshotUpstream(t *testing.T, e *env) map[string]string {
	t.Helper()
	out := make(map[string]string, len(upstreamTables))
	for _, tbl := range upstreamTables {
		var h string
		if err := e.pool.QueryRow(context.Background(),
			fmt.Sprintf(`SELECT COALESCE(md5(string_agg(t::text, E'\n' ORDER BY t::text)), '<empty>') FROM %s t`, tbl)).
			Scan(&h); err != nil {
			t.Fatalf("snapshot %s: %v", tbl, err)
		}
		out[tbl] = h
	}
	return out
}

func attemptRevision(t *testing.T, e *env, attemptID string) int64 {
	t.Helper()
	var rev int64
	if err := e.pool.QueryRow(context.Background(),
		`SELECT revision_seq FROM tx_attempts WHERE attempt_id = $1`, attemptID).Scan(&rev); err != nil {
		t.Fatal(err)
	}
	return rev
}

// TestV10ReadOnlyUpstreamDiff is T052/V10: a lifecycle run over the read-only
// consumers leaves every upstream table unchanged, the attempt's revision_seq
// is monotonic (+1 per mutation), and Status never dispatches (FR-12/FR-13;
// persistence.md §6).
func TestV10ReadOnlyUpstreamDiff(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	f := e.seed()
	f.sign()
	revisionAfterSign := attemptRevision(t, e, f.attemptID)

	before := snapshotUpstream(t, e)

	// Dispatch accepted: reads claim/lease/pause/recovery/binding/grant/scope.
	if res, err := e.send(f, SendInitial, nil); err != nil || res.Outcome != "accepted" {
		t.Fatalf("send = %+v %v", res, err)
	}
	revisionAfterSend := attemptRevision(t, e, f.attemptID)
	if revisionAfterSend != revisionAfterSign+1 {
		t.Fatalf("revision after send = %d, want %d", revisionAfterSend, revisionAfterSign+1)
	}

	// Reconcile to an effective canonical receipt: reads chain_blocks + 005.
	e.rpc.mu.Lock()
	e.rpc.txFound = true
	e.rpc.txPending = false
	e.rpc.receipt = receiptAt(100, blockHashHex(100), 1, []*types.Log{xferLog(fxAsset, f.sender, fxRecipient, 1000)})
	e.rpc.mu.Unlock()
	res, err := e.store.Reconcile(ctx, f.attemptID, "")
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.ReceiptEffect != "effective" {
		t.Fatalf("receipt effect = %s, want effective", res.ReceiptEffect)
	}
	revisionAfterReceipt := attemptRevision(t, e, f.attemptID)
	if revisionAfterReceipt != revisionAfterSend+1 {
		t.Fatalf("revision after receipt = %d, want %d", revisionAfterReceipt, revisionAfterSend+1)
	}

	// A zero-dispatch refusal (stale revision) reads the same consumers.
	stale := int64(999)
	dispatchBeforeRefusal := e.rpc.dispatchCount()
	rres, err := e.send(f, SendReplay, &stale)
	if rres.RefusalClass != string(ClassSendStale) {
		t.Fatalf("stale replay class = %s (%v), want send_stale", rres.RefusalClass, err)
	}
	if e.rpc.dispatchCount() != dispatchBeforeRefusal {
		t.Fatal("zero-dispatch refusal dispatched")
	}

	// Status is a pure read: no dispatch, no revision bump.
	revBeforeStatus := attemptRevision(t, e, f.attemptID)
	dispatchBeforeStatus := e.rpc.dispatchCount()
	if _, err := e.store.Status(ctx, f.attemptID); err != nil {
		t.Fatalf("Status: %v", err)
	}
	if e.rpc.dispatchCount() != dispatchBeforeStatus {
		t.Fatal("Status triggered a dispatch")
	}
	if rev := attemptRevision(t, e, f.attemptID); rev != revBeforeStatus {
		t.Fatalf("Status bumped revision %d -> %d", revBeforeStatus, rev)
	}

	after := snapshotUpstream(t, e)
	for _, tbl := range upstreamTables {
		if before[tbl] != after[tbl] {
			t.Errorf("upstream table %s changed: %s -> %s", tbl, before[tbl], after[tbl])
		}
	}
}
