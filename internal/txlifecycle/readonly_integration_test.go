//go:build integration

package txlifecycle

import (
	"context"
	"testing"

	"github.com/xtianxx/txharbor/internal/eth"
)

// readonlyTables are the upstream tables (005/006/007/008 state plus 011's
// claim carrier) that V10 diffs across the whole matrix: 010 must stay read
// only over every one of them — no INSERT, no UPDATE, no DELETE (FR-12/FR-13;
// persistence.md §6).
var readonlyTables = []string{
	"indexer_pause", "log_pause", "deposit_pause",
	"reorg_recovery", "reorg_recovery_events", "chain_blocks",
	"withdrawal_authorizations", "withdrawal_authorization_scopes",
	"nonce_bindings", "nonce_scope_state", "nonce_scope_holds",
	"nonce_wallet_registry", "confirmation_policy_history",
	"execution_claims",
}

// fingerprintTables serializes each table's full current content (not just
// row counts) into a deterministic digest, so an in-place UPDATE that keeps
// the row count identical is still caught as a diff.
func (e *env) fingerprintTables(t *testing.T) map[string]string {
	t.Helper()
	out := make(map[string]string, len(readonlyTables))
	for _, tb := range readonlyTables {
		var fp string
		if err := e.pool.QueryRow(context.Background(),
			`SELECT COALESCE(string_agg(row_text, E'\n' ORDER BY row_text), '')
			   FROM (SELECT to_jsonb(x)::text AS row_text FROM `+tb+` x) s`).Scan(&fp); err != nil {
			t.Fatalf("fingerprint %s: %v", tb, err)
		}
		out[tb] = fp
	}
	return out
}

// revisionOf reads one attempt's current revision_seq.
func (e *env) revisionOf(t *testing.T, attemptID string) int64 {
	t.Helper()
	var revision int64
	if err := e.pool.QueryRow(context.Background(),
		`SELECT revision_seq FROM tx_attempts WHERE attempt_id = $1`, attemptID).Scan(&revision); err != nil {
		t.Fatalf("revision_seq %s: %v", attemptID, err)
	}
	return revision
}

// expectRevision pins the revision state after one step: every mutation must
// advance it by exactly +1 (revision_seq monotonicity, persistence.md §1) and
// every read-only operation must leave it untouched.
func (e *env) expectRevision(t *testing.T, f *fixture, want int64) {
	t.Helper()
	if got := e.revisionOf(t, f.attemptID); got != want {
		t.Fatalf("attempt %s revision_seq = %d, want %d (+1 per mutation, monotonic)", f.attemptID, got, want)
	}
}

// TestV10ReadOnlyBoundary is quickstart V10 / T052: seed all upstream state,
// fingerprint the 14 upstream tables, then drive the 010 V-matrix over that
// frozen upstream state (prepare + idempotent converge, sign, a V6 pause
// refusal, an accepted dispatch, a timeout unknown, a reconcile + receipt
// apply, Status/AttemptByID/UnknownRecovery reads) and diff the fingerprint.
// The diff must be empty: 010 writes only its own tables. Every step also
// pins revision_seq (+1 per mutation, never more, never less) and every read
// — Status above all — provably sends nothing (FR-13).
func TestV10ReadOnlyBoundary(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()

	// Seed three attempts' full upstream fixtures BEFORE the fingerprint: the
	// seeding is test-controlled upstream state, not a 010 operation.
	fA := e.seed()
	fB := e.seed()
	fC := e.seed()
	before := e.fingerprintTables(t)

	// V1/T1: the prepare persists revision_seq=1; an identical re-prepare
	// converges without a write.
	ref, err := e.store.PrepareAttempt(ctx, fA.request)
	if err != nil || ref.AttemptID != fA.attemptID || ref.RevisionSeq != 1 {
		t.Fatalf("prepare = %+v %v, want revision 1", ref, err)
	}
	e.expectRevision(t, fA, 1)
	if ref, err = e.store.PrepareAttempt(ctx, fA.request); err != nil || !ref.Converged {
		t.Fatalf("prepare replay: %+v %v, want converged", ref, err)
	}
	e.expectRevision(t, fA, 1)

	// T2 for A, B and C: signed at revision 2.
	fA.sign()
	e.expectRevision(t, fA, 2)
	fB.sign()
	e.expectRevision(t, fB, 2)
	fC.sign()
	e.expectRevision(t, fC, 2)

	// V6 refusal: a pause present blocks the send with zero dispatch, a
	// committed gate_refused event and no revision movement.
	e.exec(`INSERT INTO indexer_pause (chain_id, height, expected_hash, actual_hash, kind)
		VALUES ($1,100,$2,$3,'hash_mismatch')`, e.chainID, blockHashHex(100), blockHashHex(101))
	beforeDispatch := e.rpc.dispatchCount()
	res, err := e.send(fC, SendInitial, nil)
	assertBlocked(t, e, fC.attemptID, res, err, ClassPausePresent, beforeDispatch)
	e.expectRevision(t, fC, 2)
	e.exec(`DELETE FROM indexer_pause WHERE chain_id = $1`, e.chainID)

	// V3 accepted dispatch: exactly one dispatch, revision 2 -> 3.
	res, err = e.send(fA, SendInitial, nil)
	if err != nil || res.Outcome != "accepted" {
		t.Fatalf("send A = %+v %v, want accepted", res, err)
	}
	if got := e.rpc.dispatchCount(); got != 1 {
		t.Fatalf("dispatch count = %d, want 1", got)
	}
	e.expectRevision(t, fA, 3)

	// V4 unknown: a timeout dispatch leaves the business effect undetermined
	// (revision 2 -> 3) without ever inferring not-sent.
	e.rpc.mu.Lock()
	e.rpc.txErr = &eth.Error{Kind: eth.KindTimeout, Op: "stub"}
	e.rpc.mu.Unlock()
	res, err = e.send(fB, SendInitial, nil)
	if err != nil || res.Outcome != "unknown" {
		t.Fatalf("send B = %+v %v, want unknown", res, err)
	}
	e.rpc.mu.Lock()
	e.rpc.txErr = nil
	e.rpc.mu.Unlock()
	if got := e.rpc.dispatchCount(); got != 2 {
		t.Fatalf("dispatch count = %d, want 2", got)
	}
	e.expectRevision(t, fB, 3)

	// V5/V7: reconcile observes A included with the expected Transfer; the
	// receipt effect is effective and the attempt revises sent -> effective.
	e.rpc.mu.Lock()
	e.rpc.txFound = true
	e.rpc.txPending = false
	e.rpc.receipt = synthReceipt(fA.sender, 100)
	e.rpc.mu.Unlock()
	t.Cleanup(func() {
		e.rpc.mu.Lock()
		e.rpc.txFound = false
		e.rpc.receipt = nil
		e.rpc.mu.Unlock()
	})
	rec, err := e.store.Reconcile(ctx, fA.attemptID, "")
	if err != nil || rec.Classification != "included" || rec.ReceiptEffect != "effective" {
		t.Fatalf("reconcile A = %+v %v, want included/effective", rec, err)
	}
	e.expectRevision(t, fA, 4)

	// The read-only sweep. Every observation below must move zero dispatch and
	// zero revision — Status never triggers a send (FR-13/Q1).
	st, err := e.store.Status(ctx, fA.attemptID)
	if err != nil {
		t.Fatalf("status A: %v", err)
	}
	if st.State != "effective" || st.RevisionSeq != 4 ||
		st.LatestDispatchOutcome != "accepted" || st.LatestReconcileClass != "included" ||
		st.LatestReceiptEffect != "effective" {
		t.Fatalf("status A = %+v, want effective/4 with accepted dispatch + included/effective facts", st)
	}
	if stB, err := e.store.Status(ctx, fB.attemptID); err != nil ||
		stB.State != "unknown" || stB.RevisionSeq != 3 {
		t.Fatalf("status B = %+v %v, want unknown at revision 3", stB, err)
	}
	if stC, err := e.store.Status(ctx, fC.attemptID); err != nil ||
		stC.State != "signed" || stC.RevisionSeq != 2 {
		t.Fatalf("status C = %+v %v, want signed at revision 2", stC, err)
	}
	if stMissing, err := e.store.Status(ctx, "att-no-such"); refusalClass(err) != ClassAttemptNotFound {
		t.Fatalf("status missing = %v, want attempt_not_found", err)
	} else if stMissing != nil {
		t.Fatalf("status missing returned data %+v", stMissing)
	}
	if _, err := e.store.UnknownRecovery(ctx, fB.attemptID, ""); err != nil {
		t.Fatalf("unknown recovery read: %v", err)
	}
	if att, err := e.store.AttemptByID(ctx, fA.attemptID); err != nil || att.State != "effective" {
		t.Fatalf("attempt read = %+v %v", att, err)
	}
	if got := e.rpc.dispatchCount(); got != 2 {
		t.Fatalf("dispatch count after read sweep = %d, want 2 (Status never sends)", got)
	}
	for _, f := range []*fixture{fA, fB, fC} {
		e.expectRevision(t, f, map[string]int64{fA.attemptID: 4, fB.attemptID: 3, fC.attemptID: 2}[f.attemptID])
	}

	// The diff: upstream tables identical before/after the whole matrix.
	after := e.fingerprintTables(t)
	for _, tb := range readonlyTables {
		if before[tb] != after[tb] {
			t.Errorf("upstream table %s changed during the V-matrix (read-only boundary violated)\nbefore: %s\nafter:  %s",
				tb, before[tb], after[tb])
		}
	}
}
