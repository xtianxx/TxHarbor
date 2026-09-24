//go:build integration

// execution_revision_events_integration_test.go is the B6/T056 V-REVISION
// producer layer for the 010/011 chain-fact revision path on real PostgreSQL:
// RevisionConsumer.Apply (completed -> revised) emits
// withdrawal.execution.revised in the SAME transaction as the state edge,
// carrying the superseded pre-revision event, the old chain block identity,
// the revised post state, the authority reason and the driving revision
// version; the revision creates no intent, nonce binding, signature, send or
// step (send counters unchanged), repeat and older revisions converge with
// zero new rows, and a send after a revision still re-runs the full gate set
// (a revision is never a send permission). It reuses the execution package
// fixtures (executionPool, the completed-intent driver, lifecycleDouble,
// countRows) against the real migrations.
package execution

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/events"
)

// t056SeedChainReceipt plants one real 010 receipt fact for the attempt: the
// chain block identity the revision event carries (canonicality=orphaned is
// the receipt-invalidation basis the revision consumes).
func t056SeedChainReceipt(t *testing.T, ctx context.Context, pool *pgxpool.Pool, attemptID string) (blockNumber int64, blockHash, txHash string) {
	t.Helper()
	blockNumber = 42
	blockHash = "0x" + strings.Repeat("ab", 32)
	txHash = "0x" + strings.Repeat("ef", 32)
	if _, err := pool.Exec(ctx, `
INSERT INTO tx_receipts
    (attempt_id, tx_hash, status, block_number, block_hash, effect, canonicality,
     confirmations, confirm_threshold, confirm_policy_seq, orphaned_at)
VALUES ($1, $2, 1, $3, $4, 'effective', 'orphaned', 1, 1, 1, now())`,
		attemptID, txHash, blockNumber, blockHash); err != nil {
		t.Fatalf("seed tx_receipts: %v", err)
	}
	return blockNumber, blockHash, txHash
}

// t056ExecutionCounts is the row-count image of the 007/008/009/010/011
// authority tables a revision must never add to.
type t056ExecutionCounts struct {
	intents, steps, execEvents, bindings, attempts, signings, sends int
}

func t056ReadCounts(t *testing.T, ctx context.Context, pool *pgxpool.Pool) t056ExecutionCounts {
	t.Helper()
	return t056ExecutionCounts{
		intents:    countRows(t, ctx, pool, `SELECT count(*) FROM payment_intents`),
		steps:      countRows(t, ctx, pool, `SELECT count(*) FROM execution_steps`),
		execEvents: countRows(t, ctx, pool, `SELECT count(*) FROM execution_events`),
		bindings:   countRows(t, ctx, pool, `SELECT count(*) FROM nonce_bindings`),
		attempts:   countRows(t, ctx, pool, `SELECT count(*) FROM tx_attempts`),
		signings:   countRows(t, ctx, pool, `SELECT count(*) FROM tx_attempt_signings`),
		sends:      countRows(t, ctx, pool, `SELECT count(*) FROM tx_send_attempts`),
	}
}

// TestT056WithdrawalRevisionEventEmittedWithoutNewSend drives one real chain
// fact revision on a completed intent and pins the revision contract plus the
// no-new-payment invariants.
func TestT056WithdrawalRevisionEventEmittedWithoutNewSend(t *testing.T) {
	const requestID, intentID = "req-t056-a", "intent-t056-a"
	pool, consumer, attemptID := completedIntentV10(t, requestID, intentID)
	ctx := context.Background()

	// The 011->010 boundary in this test is the labeled test-only double
	// (lifecycle_double_test.go): seed the real 010 attempt fact (and its
	// binding parent) that the revision's receipt and chain evidence attach
	// to, so the FK-backed fixture stays real.
	seedExecutionAttemptFixture(t, ctx, pool, attemptID, intentID, "binding-t056", "authz-"+intentID)

	blockNumber, blockHash, txHash := t056SeedChainReceipt(t, ctx, pool, attemptID)

	// Pre-revision fact stream: three committed state_changed events
	// (admitted->claimed, claimed->executing, executing->completed).
	before := executionOutboxRows(t, ctx, pool, intentID)
	if len(before) != 3 || before[2].Payload["to_state"] != IntentCompleted {
		t.Fatalf("pre-revision events = %+v, want three ending in completed", before)
	}
	completedEventID := before[2].EventID

	countsBefore := t056ReadCounts(t, ctx, pool)

	res, err := consumer.Apply(ctx, RevisionFact{
		IntentID: intentID, RevisionVersion: 2, AttemptID: attemptID,
		TargetState: IntentRevised, Basis: "authority_invalidated state=orphaned",
	})
	if err != nil {
		t.Fatalf("RevisionConsumer.Apply: %v", err)
	}
	if !res.Applied || res.FromState != IntentCompleted || res.ToState != IntentRevised {
		t.Fatalf("revision result = %+v, want completed->revised applied", res)
	}

	// The revision fact stream: the transition's state_changed plus the
	// withdrawal.execution.revised revision of the pre-revision event.
	after := executionOutboxRows(t, ctx, pool, intentID)
	if len(after) != 5 {
		t.Fatalf("post-revision events = %d, want 5 (3 + state_changed + revised)", len(after))
	}
	if after[3].EventType != events.EventTypeWithdrawalExecutionStateChanged ||
		after[3].Payload["from_state"] != IntentCompleted || after[3].Payload["to_state"] != IntentRevised {
		t.Fatalf("revision transition event = %+v, want state_changed completed->revised", after[3])
	}
	revised := after[4]
	if revised.EventType != events.EventTypeWithdrawalExecutionRevised ||
		revised.SchemaVersion != events.SchemaVersionV1 ||
		revised.IdentityKind != string(events.IdentityKindBusinessObject) ||
		revised.AggregateType != withdrawalIntentAggregateType ||
		revised.AggregateID != intentID ||
		revised.AggregateVersion != 5 {
		t.Fatalf("revised envelope = %+v, want revised/v1/business_object/withdrawal_intent/%s/v5", revised, intentID)
	}
	if revised.Payload["from_state"] != IntentCompleted || revised.Payload["to_state"] != IntentRevised {
		t.Fatalf("revised payload = %v, want completed->revised", revised.Payload)
	}
	if revised.Payload["reason"] != "authority_invalidated state=orphaned" ||
		revised.Payload["attempt_id"] != attemptID {
		t.Fatalf("revised payload context = %v, want the authority reason and attempt %s", revised.Payload, attemptID)
	}
	superseded, ok := revised.Payload["superseded_identity"].(map[string]any)
	if !ok {
		t.Fatalf("revised superseded_identity = %v, want an object", revised.Payload["superseded_identity"])
	}
	if superseded["event_id"] != completedEventID || superseded["event_type"] != events.EventTypeWithdrawalExecutionStateChanged {
		t.Fatalf("superseded_identity = %v, want the pre-revision completed event %s", superseded, completedEventID)
	}
	if superseded["block_hash"] != blockHash || superseded["block_number"] != float64(blockNumber) ||
		superseded["tx_hash"] != txHash {
		t.Fatalf("superseded_identity chain identity = %v, want %s/%d/%s", superseded, blockHash, blockNumber, txHash)
	}
	var (
		revisesID       string
		recoveryVersion int64
		chainID         int64
		storedBlock     int64
		storedHash      string
	)
	if err := pool.QueryRow(ctx, `
SELECT revises_event_id::text, recovery_version, chain_id, block_number, block_hash
FROM outbox_events WHERE aggregate_id = $1 AND event_type = $2`,
		intentID, events.EventTypeWithdrawalExecutionRevised).
		Scan(&revisesID, &recoveryVersion, &chainID, &storedBlock, &storedHash); err != nil {
		t.Fatalf("read revised row: %v", err)
	}
	if revisesID != completedEventID {
		t.Fatalf("revised revises_event_id = %s, want %s", revisesID, completedEventID)
	}
	if recoveryVersion != 2 {
		t.Fatalf("revised recovery_version = %d, want the driving revision version 2", recoveryVersion)
	}
	if chainID != execChainID || storedBlock != blockNumber || storedHash != blockHash {
		t.Fatalf("revised chain identity = (%d,%d,%s), want (%d,%d,%s)", chainID, storedBlock, storedHash, execChainID, blockNumber, blockHash)
	}
	if key, found := events.ForbiddenPayload(revised.Payload); found {
		t.Fatalf("revised payload carries forbidden material %q", key)
	}

	// Zero new payment effects: the revision adds exactly two execution events
	// (the completed->revised state_changed plus its revision_applied) and no
	// intent, step, binding, attempt, signature or send.
	countsAfter := t056ReadCounts(t, ctx, pool)
	if countsAfter.intents != countsBefore.intents || countsAfter.steps != countsBefore.steps ||
		countsAfter.bindings != countsBefore.bindings || countsAfter.attempts != countsBefore.attempts ||
		countsAfter.signings != countsBefore.signings || countsAfter.sends != countsBefore.sends {
		t.Fatalf("revision created payment effects: before=%+v after=%+v", countsBefore, countsAfter)
	}
	if countsAfter.execEvents != countsBefore.execEvents+2 {
		t.Fatalf("execution_events = %d, want %d (state_changed + revision_applied)",
			countsAfter.execEvents, countsBefore.execEvents+2)
	}
	if n := countIntents(t, ctx, pool, requestID); n != 1 {
		t.Fatalf("intent rows = %d, want 1 (revision never rebuilds or compensates)", n)
	}

	// Repeat and older revisions converge: zero new rows and zero new events.
	repeat, err := consumer.Apply(ctx, RevisionFact{
		IntentID: intentID, RevisionVersion: 2, AttemptID: attemptID,
		TargetState: IntentRevised, Basis: "authority_invalidated state=orphaned",
	})
	if err != nil || repeat.Applied {
		t.Fatalf("repeat revision = %+v (err %v), want zero rows", repeat, err)
	}
	older, err := consumer.Apply(ctx, RevisionFact{
		IntentID: intentID, RevisionVersion: 1, AttemptID: attemptID,
		TargetState: IntentRevised, Basis: "stale",
	})
	if err != nil || older.Applied {
		t.Fatalf("older revision = %+v (err %v), want zero rows", older, err)
	}
	if n := len(executionOutboxRows(t, ctx, pool, intentID)); n != 5 {
		t.Fatalf("outbox rows after repeats = %d, want 5", n)
	}
	if state := intentState(t, ctx, pool, intentID); state != IntentRevised {
		t.Fatalf("intent state = %s, want revised", state)
	}

	// A revision is not a send permission: an attempted send without the
	// current claim is refused with zero 010 boundary calls and zero new rows.
	store := execClaimStore(t, pool)
	double := newLifecycleDouble()
	driver := &StepDriver{Pool: pool, Claims: store, Binding: bindingDouble{result: BindingMatches}, Advancer: double}
	out, err := driver.IssueAndAdvance(ctx, StepRequest{
		IntentID: intentID, RequestID: requestID, CallerID: 1,
		OwnerID: "intruder", LeaseVersion: 1, Action: ActionFirstBroadcast,
	})
	if err != nil {
		t.Fatalf("send after revision: %v", err)
	}
	if out.Refusal != ClassClaimNotCurrent {
		t.Fatalf("send after revision = %+v, want claim_not_current (full gate re-verification)", out)
	}
	if double.totalCalls() != 0 {
		t.Fatal("a revision granted a send path to the 010 boundary")
	}
	final := t056ReadCounts(t, ctx, pool)
	if final != countsAfter {
		t.Fatalf("refused send changed row counts: after revision=%+v after refusal=%+v", countsAfter, final)
	}
	t.Logf("T056 revision matrix PASS: revised v5 supersedes %s (chain %d block %d), 0 new intent/nonce/signature/send",
		completedEventID, chainID, blockNumber)
}
