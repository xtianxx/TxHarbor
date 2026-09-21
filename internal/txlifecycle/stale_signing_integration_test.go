//go:build integration

// stale_signing_integration_test.go closes the T2 stale-signing gap: an
// attempt that has moved out of `prepared` must never be (re)written to
// `signed`, even when a locally valid 009 result for it arrives late.
//
// Defect: none known - coverage gap (the guard itself is correct). Zero tests
// exercised the ClassSendStale arm of SignAndPersist, so the hardcoded
// `{"prepared"}` from-state filter in applyStateTx (signing.go:261) could be
// dropped (store.go:77 `len(fromStates) > 0` -> `<= 0`) with the suite still
// green: signing a moved attempt would then succeed instead of refusing.
//
// Reachability note: the obvious "sign once, then sign again" flow can never
// reach signing.go:263 - tx_attempt_signings is PRIMARY KEY (attempt_id)
// (000011_tx_lifecycle.sql:128), so a second T2 for the same attempt fails
// 23505 on the pkey inside the INSERT, before applyStateTx, and surfaces
// coordination_unavailable. The reachable moved-out-of-prepared-without-a-
// signing-row state is `replaced`: markSiblingsReplaced (confirm.go:116-152)
// revises every non-terminal sibling, including a still-`prepared` one, once a
// sibling attempt is observed effective. A late 009 signature for that replaced
// attempt is exactly the stale T2 this test drives.
//
// Invariant: T2 is guarded prepared->signed with the state re-checked inside
// the write (persistence.md §1; revision machinery). A stale result for a
// replaced attempt is refused ClassSendStale with zero durable trace: no
// signing row, no signature_persisted event, no state/revision move, no
// dispatch (no send/attempt anywhere).
// Gap: no test asserted the stale T2 refusal class, the rollback of the signing
// insert, or the frozen replaced state.
// Level: integration (real PostgreSQL scratch DB + lane-local stubRPC; never
// joint evidence).
// Criteria: A is prepared with no T2 row; sibling S over the same binding is
// signed and observed included/effective, revising A to replaced; a valid
// 009 result for A then returns ClassSendStale; A's signing-row count (0),
// attempt-event count, dispatch count, state (replaced) and revision are
// byte-identical afterwards, and no signature_persisted evidence exists.
package txlifecycle

import (
	"context"
	"testing"

	"github.com/ethereum/go-ethereum/core/types"
)

func TestV2StaleSigningRefusedAfterReplaced(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()

	// A: prepared, no T2 row, so the stale INSERT below is not pre-empted by
	// the tx_attempt_signings primary key.
	f := e.seed()

	// S: sibling replacement over A's binding, fee-bumped so the replacement
	// anchor validates. S becomes effective, which revises A to replaced
	// through markSiblingsReplaced (the production writer, not a hand-set
	// state).
	sib := *f.request
	sib.AttemptID = f.attemptID + "-sib"
	sib.SigningRequestID = f.signingRequestID + "-sib"
	sib.ReplacementOf = f.attemptID
	sib.MaxPriorityFeePerGas = "50000000"
	if _, err := e.store.PrepareAttempt(ctx, &sib); err != nil {
		t.Fatalf("sibling prepare: %v", err)
	}
	f.signRequest(&sib)
	e.rpc.mu.Lock()
	e.rpc.txFound = true
	e.rpc.txPending = false
	e.rpc.receipt = receiptAt(100, blockHashHex(100), 1, []*types.Log{xferLog(fxAsset, f.sender, fxRecipient, 1000)})
	e.rpc.mu.Unlock()
	if _, err := e.store.Reconcile(ctx, sib.AttemptID, ""); err != nil {
		t.Fatalf("reconcile sibling: %v", err)
	}
	sibAfter, err := e.store.AttemptByID(ctx, sib.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	if sibAfter.State != "effective" {
		t.Fatalf("sibling state = %s, want effective (setup)", sibAfter.State)
	}

	stale, err := e.store.AttemptByID(ctx, f.attemptID)
	if err != nil {
		t.Fatal(err)
	}
	if stale.State != "replaced" {
		t.Fatalf("attempt state = %s, want replaced (setup must move it out of prepared)", stale.State)
	}

	// Frozen history before the stale T2. The signing-row count is the
	// observable T2-persist count for A: A never had one, and a refused stale
	// persist must roll its INSERT back.
	signingsBefore := envCount(t, e, `SELECT count(*) FROM tx_attempt_signings WHERE attempt_id = $1`, f.attemptID)
	eventsBefore := envCount(t, e, `SELECT count(*) FROM tx_attempt_events WHERE attempt_id = $1`, f.attemptID)
	sendsBefore := envCount(t, e, `SELECT count(*) FROM tx_send_attempts WHERE attempt_id = $1`, f.attemptID)
	revBefore := stale.RevisionSeq
	dispatchBefore := e.rpc.dispatchCount()
	if signingsBefore != 0 {
		t.Fatalf("attempt already has %d signing row(s); the stale path needs none", signingsBefore)
	}

	// The valid 009 result for A arrives after A was replaced.
	result := f.signOnly()
	if _, err := e.store.SignAndPersist(ctx, stale, result); refusalClass(err) != ClassSendStale {
		t.Fatalf("stale T2 refusal = %s (%v), want %s (a moved attempt must not be re-signed)",
			refusalClass(err), err, ClassSendStale)
	}

	// Zero durable trace: no signing row, no signature_persisted event, no
	// state/revision move, no dispatch.
	if got := envCount(t, e, `SELECT count(*) FROM tx_attempt_signings WHERE attempt_id = $1`, f.attemptID); got != signingsBefore {
		t.Fatalf("signing rows moved: %d -> %d, want no new signing row for the stale attempt", signingsBefore, got)
	}
	if got := envCount(t, e, `SELECT count(*) FROM tx_attempt_events WHERE attempt_id = $1 AND event = 'signature_persisted'`, f.attemptID); got != 0 {
		t.Fatalf("signature_persisted events = %d, want 0", got)
	}
	if got := envCount(t, e, `SELECT count(*) FROM tx_attempt_events WHERE attempt_id = $1`, f.attemptID); got != eventsBefore {
		t.Fatalf("attempt events moved: %d -> %d", eventsBefore, got)
	}
	if got := envCount(t, e, `SELECT count(*) FROM tx_send_attempts WHERE attempt_id = $1`, f.attemptID); got != sendsBefore {
		t.Fatalf("send rows moved: %d -> %d", sendsBefore, got)
	}
	if got := e.rpc.dispatchCount(); got != dispatchBefore {
		t.Fatalf("dispatch count moved: %d -> %d (a stale T2 must never dispatch)", dispatchBefore, got)
	}
	after, err := e.store.AttemptByID(ctx, f.attemptID)
	if err != nil {
		t.Fatal(err)
	}
	if after.State != "replaced" || after.RevisionSeq != revBefore {
		t.Fatalf("attempt moved: state=%s revision=%d, want replaced/%d", after.State, after.RevisionSeq, revBefore)
	}
}
