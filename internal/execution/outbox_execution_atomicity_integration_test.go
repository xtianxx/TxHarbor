//go:build integration

// outbox_execution_atomicity_integration_test.go is the B3/T040 atomicity and
// referential-integrity probe for the 011 execution integration points (T030;
// data-model §4):
//
//   - every committed TransitionIntent carries its withdrawal.execution
//     state_changed event in the same transaction; a rolled-back transition
//     emits nothing, a lost CAS emits nothing, an illegal edge emits nothing;
//   - the unknown-result transition (executing -> reconciling) carries the 010
//     attempt reference in the payload, and that reference 100% resolves to an
//     existing tx_attempts fact (attempt-level referential integrity);
//   - a repeated delivery of the same unknown outcome creates no new intent,
//     attempt, nonce binding, signature request or broadcast (call/row
//     counts unchanged).
//
// The lifecycleDouble is the labeled test-only 011->010 boundary double and is
// never joint evidence (lifecycle_double_test.go); the attempt fact is seeded
// as an explicit tx_attempts row so the referential check is against the real
// 010 authority table.
package execution

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/events"
)

// executionOutboxRow is one decoded withdrawal_intent outbox row.
type executionOutboxRow struct {
	EventID          string
	EventType        string
	SchemaVersion    int
	IdentityKind     string
	AggregateType    string
	AggregateID      string
	AggregateVersion int64
	PublishState     string
	Payload          map[string]any
}

func executionOutboxRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, intentID string) []executionOutboxRow {
	t.Helper()
	rows, err := pool.Query(ctx, `
SELECT event_id::text, event_type, schema_version, identity_kind, aggregate_type, aggregate_id,
       aggregate_version, publish_state, payload
FROM outbox_events
WHERE aggregate_id = $1
ORDER BY aggregate_version`, intentID)
	if err != nil {
		t.Fatalf("query outbox rows for %s: %v", intentID, err)
	}
	defer rows.Close()
	var out []executionOutboxRow
	for rows.Next() {
		var (
			row     executionOutboxRow
			payload []byte
		)
		if err := rows.Scan(&row.EventID, &row.EventType, &row.SchemaVersion, &row.IdentityKind,
			&row.AggregateType, &row.AggregateID, &row.AggregateVersion, &row.PublishState, &payload); err != nil {
			t.Fatalf("scan outbox row: %v", err)
		}
		if err := json.Unmarshal(payload, &row.Payload); err != nil {
			t.Fatalf("decode payload of %s: %v", row.EventID, err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate outbox rows: %v", err)
	}
	return out
}

// seedExecutionAttemptFixture plants one real tx_attempts fact (and its
// nonce_bindings FK parent) so the emitted attempt reference resolves against
// the 010 authority table.
func seedExecutionAttemptFixture(t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	attemptID, intentID, bindingID, authorizationID string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
INSERT INTO nonce_bindings
    (binding_id, intent_id, chain_id, sender, nonce, state, authorization_id,
     authorization_version, registry_seq, allocation_observation_id)
VALUES ($1, $2, $3, $4, 1, 'allocated', $5, $6, 1, 'obs-t040')`,
		bindingID, intentID, execChainID, execSender, authorizationID,
		strings.Repeat("ab", 32)); err != nil {
		t.Fatalf("seed nonce_bindings: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO tx_attempts
    (attempt_id, signing_request_id, intent_id, binding_ref, authorization_id,
     authorization_version, recovery_version, chain_id, sender, nonce, tx_type,
     to_addr, value, data, gas_limit, gas_price, asset, recipient, amount,
     canonical_envelope, content_hash)
VALUES ($1, $2, $3, $4, $5, 1, 0, $6, $7, 1, 0, $8, 0, $9, 21000, 1, $8, $10, 1, '{}', $11)`,
		attemptID, attemptID+"-sr", intentID, bindingID, authorizationID,
		execChainID, execSender, execAsset, []byte{0x01}, execRecipient,
		"0x"+strings.Repeat("cd", 32)); err != nil {
		t.Fatalf("seed tx_attempts: %v", err)
	}
}

func countRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, sql, args...).Scan(&n); err != nil {
		t.Fatalf("count rows (%s): %v", sql, err)
	}
	return n
}

// TestOutboxExecutionTransitionAtomicity is the T030 commit/rollback/CAS
// probe on the shared TransitionIntent path.
func TestOutboxExecutionTransitionAtomicity(t *testing.T) {
	ctx, pool := executionPool(t)
	store := execClaimStore(t, pool)
	leaseVersion := seedExecClaimedIntent(t, ctx, pool, "req-b3-a", "authz-b3-a", "intent-b3-a", "owner-a", store)
	var stateVersion int64
	if err := pool.QueryRow(ctx, `SELECT state_version FROM payment_intents WHERE intent_id = 'intent-b3-a'`).Scan(&stateVersion); err != nil {
		t.Fatalf("read claimed state_version: %v", err)
	}

	// The admitted -> claimed transition committed by the fixture already
	// emitted version 1.
	rows := executionOutboxRows(t, ctx, pool, "intent-b3-a")
	if len(rows) != 1 || rows[0].AggregateVersion != 1 || rows[0].Payload["to_state"] != IntentClaimed {
		t.Fatalf("claim transition events = %+v, want one v1 claimed event", rows)
	}

	// Rollback path: the transition and its event disappear together.
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin rollback transition: %v", err)
	}
	if err := TransitionIntent(ctx, tx, "intent-b3-a", IntentClaimed, stateVersion, IntentExecuting, leaseVersion); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("in-tx TransitionIntent: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback transition: %v", err)
	}
	var state string
	if err := pool.QueryRow(ctx, `SELECT state FROM payment_intents WHERE intent_id = 'intent-b3-a'`).Scan(&state); err != nil {
		t.Fatalf("read intent state: %v", err)
	}
	if state != IntentClaimed {
		t.Fatalf("state after rollback = %s, want claimed", state)
	}
	if n := len(executionOutboxRows(t, ctx, pool, "intent-b3-a")); n != 1 {
		t.Fatalf("outbox rows after rolled-back transition = %d, want 1", n)
	}

	// Commit path: version 2 with the factual from/to states.
	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin commit transition: %v", err)
	}
	if err := TransitionIntent(ctx, tx, "intent-b3-a", IntentClaimed, stateVersion, IntentExecuting, leaseVersion); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("commit TransitionIntent: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit transition: %v", err)
	}
	rows = executionOutboxRows(t, ctx, pool, "intent-b3-a")
	if len(rows) != 2 || rows[1].AggregateVersion != 2 ||
		rows[1].Payload["from_state"] != IntentClaimed || rows[1].Payload["to_state"] != IntentExecuting {
		t.Fatalf("commit transition events = %+v, want v2 claimed->executing", rows)
	}
	if key, found := events.ForbiddenPayload(rows[1].Payload); found {
		t.Fatalf("state_changed payload carries forbidden material %q", key)
	}

	// Lost CAS: repeating the same transition is refused and emits nothing.
	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin lost-CAS transition: %v", err)
	}
	err = TransitionIntent(ctx, tx, "intent-b3-a", IntentClaimed, stateVersion, IntentExecuting, leaseVersion)
	_ = tx.Rollback(ctx)
	if !errors.Is(err, ErrTransitionRefused) {
		t.Fatalf("repeated transition = %v, want ErrTransitionRefused", err)
	}
	if n := len(executionOutboxRows(t, ctx, pool, "intent-b3-a")); n != 2 {
		t.Fatalf("outbox rows after lost CAS = %d, want 2", n)
	}

	// Illegal edge: refused before any write.
	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin illegal transition: %v", err)
	}
	err = TransitionIntent(ctx, tx, "intent-b3-a", IntentClaimed, stateVersion, IntentCompleted, leaseVersion)
	_ = tx.Rollback(ctx)
	if !errors.Is(err, ErrIllegalTransition) {
		t.Fatalf("illegal transition = %v, want ErrIllegalTransition", err)
	}
	if n := len(executionOutboxRows(t, ctx, pool, "intent-b3-a")); n != 2 {
		t.Fatalf("outbox rows after illegal edge = %d, want 2", n)
	}
}

// TestOutboxExecutionUnknownAttemptReference is the T030/T040 unknown-result
// arm: the state_changed event of the executing -> reconciling transition
// references the 010 attempt, the reference resolves, and a repeated delivery
// of the same outcome creates no new business effects.
func TestOutboxExecutionUnknownAttemptReference(t *testing.T) {
	ctx, pool := executionPool(t)
	store := execClaimStore(t, pool)
	version := seedExecClaimedIntent(t, ctx, pool, "req-b3-u", "authz-b3-u", "intent-b3-u", "owner-a", store)

	// One real 010 attempt fact, persisted before the boundary call (T1
	// persist-before-call shape).
	const attemptID = "attempt-1"
	seedExecutionAttemptFixture(t, ctx, pool, attemptID, "intent-b3-u", "binding-b3-u", "authz-b3-u")

	double := newLifecycleDouble()
	double.resultClass = OutcomePendingUnknown
	driver := &StepDriver{Pool: pool, Claims: store, Binding: bindingDouble{result: BindingMatches}, Advancer: double}
	req := StepRequest{IntentID: "intent-b3-u", RequestID: "req-b3-u", CallerID: 1,
		OwnerID: "owner-a", LeaseVersion: version, Action: ActionFirstBroadcast}

	out, err := driver.IssueAndAdvance(ctx, req)
	if err != nil {
		t.Fatalf("IssueAndAdvance: %v", err)
	}
	if out.FinalStepState != StepUnknown || !out.Reconciling || out.AttemptID != attemptID {
		t.Fatalf("unknown outcome = %+v, want unknown/reconciling with attempt %s", out, attemptID)
	}

	rows := executionOutboxRows(t, ctx, pool, "intent-b3-u")
	if len(rows) != 3 {
		t.Fatalf("outbox rows = %d, want 3 (claimed, executing, reconciling)", len(rows))
	}
	reconciling := rows[2]
	if reconciling.EventType != events.EventTypeWithdrawalExecutionStateChanged ||
		reconciling.AggregateVersion != 3 ||
		reconciling.Payload["from_state"] != IntentExecuting ||
		reconciling.Payload["to_state"] != IntentReconciling {
		t.Fatalf("reconciling event = %+v, want state_changed v3 executing->reconciling", reconciling)
	}
	if reconciling.Payload["attempt_id"] != attemptID {
		t.Fatalf("reconciling payload attempt_id = %v, want %s", reconciling.Payload["attempt_id"], attemptID)
	}
	// Attempt-level referential integrity: the reference exists in the 010
	// authority table, and no state_changed event in the database carries a
	// dangling attempt reference.
	var resolved int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM tx_attempts WHERE attempt_id = $1`, attemptID).Scan(&resolved); err != nil || resolved != 1 {
		t.Fatalf("tx_attempts rows for %s = %d (err %v), want 1", attemptID, resolved, err)
	}
	dangling := countRows(t, ctx, pool, `
SELECT count(*) FROM outbox_events e
WHERE e.event_type = 'withdrawal.execution.state_changed'
  AND e.payload ? 'attempt_id'
  AND NOT EXISTS (SELECT 1 FROM tx_attempts a WHERE a.attempt_id = e.payload->>'attempt_id')`)
	if dangling != 0 {
		t.Fatalf("state_changed events with dangling attempt references = %d, want 0", dangling)
	}
	referencing := countRows(t, ctx, pool, `
SELECT count(*) FROM outbox_events e
WHERE e.event_type = 'withdrawal.execution.state_changed' AND e.payload->>'attempt_id' = $1`, attemptID)
	if referencing != 1 {
		t.Fatalf("events referencing %s = %d, want 1", attemptID, referencing)
	}

	// Repeated delivery of the same unknown outcome: the fenced converge
	// refuses and no new intent/attempt/nonce/signature/broadcast is created.
	before := map[string]int{
		"intents":      countRows(t, ctx, pool, `SELECT count(*) FROM payment_intents`),
		"bindings":     countRows(t, ctx, pool, `SELECT count(*) FROM nonce_bindings`),
		"attempts":     countRows(t, ctx, pool, `SELECT count(*) FROM tx_attempts`),
		"signings":     countRows(t, ctx, pool, `SELECT count(*) FROM tx_attempt_signings`),
		"sendAttempts": countRows(t, ctx, pool, `SELECT count(*) FROM tx_send_attempts`),
	}
	repeat, err := driver.converge(ctx, req, out.StepID, advanceResult{class: OutcomePendingUnknown, attemptID: attemptID})
	if err != nil {
		t.Fatalf("repeat converge: %v", err)
	}
	if repeat.Refusal != ClassClaimNotCurrent {
		t.Fatalf("repeat converge = %+v, want a fenced refusal (step no longer issued)", repeat)
	}
	after := map[string]int{
		"intents":      countRows(t, ctx, pool, `SELECT count(*) FROM payment_intents`),
		"bindings":     countRows(t, ctx, pool, `SELECT count(*) FROM nonce_bindings`),
		"attempts":     countRows(t, ctx, pool, `SELECT count(*) FROM tx_attempts`),
		"signings":     countRows(t, ctx, pool, `SELECT count(*) FROM tx_attempt_signings`),
		"sendAttempts": countRows(t, ctx, pool, `SELECT count(*) FROM tx_send_attempts`),
	}
	for name, want := range before {
		if after[name] != want {
			t.Fatalf("%s rows changed on repeated delivery: %d -> %d (want no new effect)", name, want, after[name])
		}
	}
	if n := len(executionOutboxRows(t, ctx, pool, "intent-b3-u")); n != 3 {
		t.Fatalf("outbox rows after repeated delivery = %d, want 3", n)
	}
	double.mu.Lock()
	attemptsRecorded := len(double.attempts)
	double.mu.Unlock()
	if attemptsRecorded != 1 {
		t.Fatalf("double recorded %d attempts, want 1 (no second attempt on repeat)", attemptsRecorded)
	}
}
