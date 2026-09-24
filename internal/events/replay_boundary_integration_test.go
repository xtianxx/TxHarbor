//go:build integration

// replay_boundary_integration_test.go is the T049 Integration-PG layer: the
// PD-4 counterexample verification. Replaying withdrawal request / execution
// / revision events and unblocking blocked rows MUST NOT create a withdrawal
// intent, allocate a nonce, sign or broadcast: the upstream 007/008/009/010/011
// tables are counted before and after, and the audited operator path is
// checked for its complete operator/scope/reason/result record and its
// operation-id convergence. Manual replay and automatic retry are separated
// in the observations.
package events

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// upstreamCounterTables are the 007/008/009/010/011 authority tables a replay
// must never add a row to.
var upstreamCounterTables = []string{
	"withdrawal_requests",
	"payment_intents",
	"nonce_bindings",
	"tx_attempts",
	"tx_attempt_signings",
	"tx_send_attempts",
	"signing_requests",
}

func countUpstreamRows(t *testing.T, pool *pgxpool.Pool) map[string]int64 {
	t.Helper()
	out := make(map[string]int64, len(upstreamCounterTables))
	for _, table := range upstreamCounterTables {
		var n int64
		if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM "+table).Scan(&n); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		out[table] = n
	}
	return out
}

// seedUpstreamFixture plants one row in every upstream authority table so the
// count-unchanged assertion is meaningful (a zero-to-zero comparison would
// not distinguish "nothing happened" from "the tables were empty").
func seedUpstreamFixture(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	const (
		sender    = "0x5151305151305151305151305151305151305151"
		asset     = "0x1111111111111111111111111111111111111111"
		recipient = "0x3333333333333333333333333333333333333333"
		chainID   = int64(1)
	)
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("seed fixture (%s): %v", sql, err)
		}
	}
	exec(`INSERT INTO caller (caller_id, label, can_create) VALUES (1, 't049', TRUE)`)
	exec(`INSERT INTO signer_caller (caller_id, label, can_sign) VALUES (1, 't049', TRUE)`)
	exec(`INSERT INTO withdrawal_authorizations
		(authorization_id, caller_id, chain_id, asset, recipient, amount, state)
		VALUES ('authz-t049', 1, $1, $2, $3, 1, 'active')`, chainID, asset, recipient)
	exec(`INSERT INTO withdrawal_requests
		(request_id, caller_id, idempotency_key, authorization_id, chain_id, asset, recipient, amount, status)
		VALUES ('req-t049', 1, 'idem-t049', 'authz-t049', $1, $2, $3, 1, 'accepted')`, chainID, asset, recipient)
	exec(`INSERT INTO payment_intents
		(intent_id, request_id, chain_id, sender, authorization_id, authorization_version, state, state_version, admitted_recovery_version)
		VALUES ('intent-t049', 'req-t049', $1, $2, 'authz-t049', 1, 'admitted', 1, 0)`, chainID, sender)
	exec(`INSERT INTO nonce_wallet_registry (chain_id, sender, state, registry_seq)
		VALUES ($1, $2, 'active', 1)`, chainID, sender)
	exec(`INSERT INTO nonce_bindings
		(binding_id, intent_id, chain_id, sender, nonce, state, authorization_id, authorization_version, registry_seq, allocation_observation_id)
		VALUES ('binding-t049', 'intent-t049', $1, $2, 1, 'allocated', 'authz-t049', $3, 1, 'obs-t049')`,
		chainID, sender, strings.Repeat("ab", 32))
	exec(`INSERT INTO tx_attempts
		(attempt_id, signing_request_id, intent_id, binding_ref, authorization_id,
		 authorization_version, recovery_version, chain_id, sender, nonce, tx_type,
		 to_addr, value, data, gas_limit, gas_price, asset, recipient, amount,
		 canonical_envelope, content_hash)
		VALUES ('attempt-t049', 'sr-t049', 'intent-t049', 'binding-t049', 'authz-t049',
		 1, 0, $1, $2, 1, 0, $5, 0, $4, 21000, 1, $5, $3, 1, '{}', $6)`,
		chainID, sender, recipient, []byte{0x01}, asset, "0x"+strings.Repeat("cd", 32))
	exec(`INSERT INTO signing_requests
		(caller_id, signing_request_id, attempt_id, intent_id, binding_ref, recovery_version,
		 chain_id, sender, nonce, tx_type, to_addr, value, data, gas_limit, gas_price,
		 asset, recipient, amount, canonical_envelope, content_hash,
		 authorization_id, authorization_fingerprint, authorization_state, policy_version)
		VALUES (1, 'sr-t049', 'attempt-t049', 'intent-t049', 'binding-t049', 0,
		 $1, $2, 1, 0, $5, 0, $4, 21000, 1, $5, $3, 1, '{}', $6,
		 'authz-t049', $7, 'active', 'policy-v1')`,
		chainID, sender, recipient, []byte{0x01}, asset, "0x"+strings.Repeat("ef", 32), strings.Repeat("ab", 32))
	exec(`INSERT INTO tx_attempt_signings (attempt_id, signature, signed_tx_bytes, tx_hash)
		VALUES ('attempt-t049', $1, $2, $3)`, "0x"+strings.Repeat("ab", 65), []byte{0x02}, "0x"+strings.Repeat("12", 32))
	exec(`INSERT INTO tx_send_attempts
		(attempt_id, send_seq, kind, outcome, observed_recovery_version, observed_now, dispatched_at)
		VALUES ('attempt-t049', 1, 'initial', 'accepted', 0, now(), now())`)
}

// withdrawalReceivedReplayEvent builds one withdrawal.request.received event.
func withdrawalReceivedReplayEvent(t *testing.T, requestID string) Event {
	t.Helper()
	ev, err := NewEvent(Event{
		EventType:     EventTypeWithdrawalRequestReceived,
		SchemaVersion: SchemaVersionV1,
		IdentityKind:  IdentityKindBusinessObject,
		AggregateType: "withdrawal_request",
		AggregateID:   requestID,
		Payload: map[string]any{
			"request_id": requestID,
			"caller":     1,
			"state":      "accepted",
		},
		OccurredAt: time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("build withdrawal.request.received: %v", err)
	}
	return ev
}

// executionStateChangedReplayEvent builds one
// withdrawal.execution.state_changed event.
func executionStateChangedReplayEvent(t *testing.T, intentID, from, to string) Event {
	t.Helper()
	ev, err := NewEvent(Event{
		EventType:     EventTypeWithdrawalExecutionStateChanged,
		SchemaVersion: SchemaVersionV1,
		IdentityKind:  IdentityKindBusinessObject,
		AggregateType: "withdrawal_intent",
		AggregateID:   intentID,
		Payload: map[string]any{
			"from_state": from,
			"to_state":   to,
			"intent_id":  intentID,
		},
		OccurredAt: time.Date(2026, 9, 24, 12, 0, 1, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("build withdrawal.execution.state_changed: %v", err)
	}
	return ev
}

// revisionAppliedReplayEvent builds one deposit.revision.applied event.
func revisionAppliedReplayEvent(t *testing.T, observationID string, revises uuid.UUID) Event {
	t.Helper()
	ev, err := NewEvent(Event{
		EventType:     EventTypeDepositRevisionApplied,
		SchemaVersion: SchemaVersionV1,
		IdentityKind:  IdentityKindBusinessObject,
		AggregateType: "deposit_observation",
		AggregateID:   observationID,
		Payload: map[string]any{
			"superseded_identity": map[string]any{
				"event_id":   revises.String(),
				"block_hash": "0x" + strings.Repeat("aa", 32),
			},
			"to_state": "orphaned",
			"reason":   "t049 shallow reorg",
		},
		OccurredAt:      time.Date(2026, 9, 24, 12, 0, 2, 0, time.UTC),
		ChainID:         1,
		BlockNumber:     100,
		BlockHash:       "0x" + strings.Repeat("bb", 32),
		RecoveryVersion: 1,
		RevisesEventID:  revises,
	})
	if err != nil {
		t.Fatalf("build deposit.revision.applied: %v", err)
	}
	return ev
}

// TestReplayBoundaryNeverReachesUpstreamAuthority is the PD-4 counterexample:
// replaying withdrawal and revision events applies the wired consumer effect
// only; the 007/008/009/010/011 authority tables gain zero rows, no new
// withdrawal intent appears, and the audited operation converges on its
// operation id.
func TestReplayBoundaryNeverReachesUpstreamAuthority(t *testing.T) {
	dsn := startMigratedPostgres(t)
	pool := openEventsPool(t, dsn)
	createConsumerEffectTable(t, pool)
	seedUpstreamFixture(t, pool)

	// The events under replay, appended through the real outbox path.
	requestID, _ := appendConsumerEvent(t, pool, withdrawalReceivedReplayEvent(t, "req-t049"))
	revisesID, _ := appendConsumerEvent(t, pool, statusChangedConsumerEvent(t, "obs-t049", "pending"))
	revisionID, _ := appendConsumerEvent(t, pool, revisionAppliedReplayEvent(t, "obs-t049", revisesID))
	executionID, _ := appendConsumerEvent(t, pool, executionStateChangedReplayEvent(t, "intent-t049", "executing", "completed"))

	observer := newConsumerTestObserver()
	reference, err := NewReferenceConsumer(pool, ConsumerOptions{
		GapWait:     200 * time.Millisecond,
		BackoffBase: 10 * time.Millisecond,
		BackoffMax:  50 * time.Millisecond,
		RetryLimit:  3,
		ChainID:     1,
		Jitter:      func() float64 { return 0 },
		Observer:    observer,
	})
	if err != nil {
		t.Fatalf("NewReferenceConsumer: %v", err)
	}
	if err := reference.EnsureLedgerSchema(context.Background()); err != nil {
		t.Fatalf("EnsureLedgerSchema: %v", err)
	}

	before := countUpstreamRows(t, pool)
	result, err := Replay(context.Background(), pool, reference.Consumer, ReplayOptions{
		ConsumerName: RefConsumerName,
		Scope: ReplayScope{Kind: ReplayScopeEventIDs, EventIDs: []uuid.UUID{
			requestID, revisionID, executionID,
		}},
		Reason:   "PD-4 counterexample replay",
		Operator: "t049-operator",
		Topic:    "txharbor.events.v1",
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if result.Replayed != 3 || result.Total != 3 {
		t.Fatalf("replay result = %+v, want 3 applied", result)
	}
	after := countUpstreamRows(t, pool)
	for _, table := range upstreamCounterTables {
		if before[table] != after[table] {
			t.Fatalf("%s rows changed on replay: %d -> %d (want 0 new intents/nonces/signatures/broadcasts)",
				table, before[table], after[table])
		}
	}
	if n, err := reference.LedgerApplications(context.Background(), requestID); err != nil || n != 1 {
		t.Fatalf("reference ledger rows for %s = %d (err %v), want 1", requestID, n, err)
	}

	// The audit row carries the operator, scope, reason and result.
	var (
		opKind, operator, reason, stored string
		scope                            []byte
	)
	if err := pool.QueryRow(context.Background(), `
SELECT op_kind, operator, reason, result, scope FROM event_ops_audit WHERE operation_id = $1`,
		result.OperationID).Scan(&opKind, &operator, &reason, &stored, &scope); err != nil {
		t.Fatalf("read replay audit row: %v", err)
	}
	if opKind != "replay" || operator != "t049-operator" || reason == "" || stored == "" || len(scope) == 0 {
		t.Fatalf("replay audit row incomplete: kind=%s operator=%s reason=%q result=%q scope=%s",
			opKind, operator, reason, stored, scope)
	}
	if !strings.Contains(stored, "replayed=3") {
		t.Fatalf("stored result %q does not record the replay count", stored)
	}

	// A repeated CLI invocation with identical parameters converges on the
	// stored result and adds no row anywhere.
	repeat, err := Replay(context.Background(), pool, reference.Consumer, ReplayOptions{
		ConsumerName: RefConsumerName,
		Scope: ReplayScope{Kind: ReplayScopeEventIDs, EventIDs: []uuid.UUID{
			requestID, revisionID, executionID,
		}},
		Reason:   "PD-4 counterexample replay",
		Operator: "t049-operator",
		Topic:    "txharbor.events.v1",
	})
	if err != nil {
		t.Fatalf("repeated Replay: %v", err)
	}
	if !repeat.Deduplicated || repeat.StoredResult != stored {
		t.Fatalf("repeated replay = %+v, want the stored result %q", repeat, stored)
	}
	var auditRows int64
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM event_ops_audit WHERE op_kind = 'replay'`).Scan(&auditRows); err != nil {
		t.Fatalf("count replay audit rows: %v", err)
	}
	if auditRows != 1 {
		t.Fatalf("replay audit rows = %d, want 1 (operation-id convergence)", auditRows)
	}
	after = countUpstreamRows(t, pool)
	for _, table := range upstreamCounterTables {
		if before[table] != after[table] {
			t.Fatalf("%s rows changed on the repeated replay: %d -> %d", table, before[table], after[table])
		}
	}

	// Manual replay and automatic retry are separate observation channels:
	// no automatic retry was recorded for these deliveries.
	applied, retries, _, replays := observer.snapshot()
	if applied != 3 || retries != 0 || replays != 1 {
		t.Fatalf("observations = (applied=%d, retries=%d, replays=%d), want (3, 0, 1)", applied, retries, replays)
	}
}

// TestUnblockIsAuditedAndConverges covers the operator unblock: blocked ->
// pending only, complete audit, and a repeated operation converges on the
// stored result.
func TestUnblockIsAuditedAndConverges(t *testing.T) {
	dsn := startMigratedPostgres(t)
	pool := openEventsPool(t, dsn)
	eventID, outboxID := appendConsumerEvent(t, pool, statusChangedConsumerEvent(t, "obs-t049-unblock", "pending"))
	_ = eventID
	if _, err := pool.Exec(context.Background(), `
UPDATE outbox_events SET publish_state = 'blocked', last_error_class = 'permanent' WHERE id = $1`, outboxID); err != nil {
		t.Fatalf("block outbox row: %v", err)
	}

	result, err := Unblock(context.Background(), pool, UnblockOptions{
		OutboxID: outboxID, Reason: "operator reviewed the permanent class", Operator: "t049-operator",
	})
	if err != nil {
		t.Fatalf("Unblock: %v", err)
	}
	if !result.Unblocked {
		t.Fatalf("Unblock result = %+v, want unblocked", result)
	}
	var state string
	if err := pool.QueryRow(context.Background(),
		`SELECT publish_state FROM outbox_events WHERE id = $1`, outboxID).Scan(&state); err != nil {
		t.Fatalf("read outbox state: %v", err)
	}
	if state != string(PublishStatePending) {
		t.Fatalf("outbox state = %s, want pending", state)
	}
	var opKind, operator, reason string
	if err := pool.QueryRow(context.Background(), `
SELECT op_kind, operator, reason FROM event_ops_audit WHERE operation_id = $1`, result.OperationID).
		Scan(&opKind, &operator, &reason); err != nil {
		t.Fatalf("read unblock audit row: %v", err)
	}
	if opKind != "unblock" || operator != "t049-operator" || reason == "" {
		t.Fatalf("unblock audit row incomplete: kind=%s operator=%s reason=%q", opKind, operator, reason)
	}
	repeat, err := Unblock(context.Background(), pool, UnblockOptions{
		OutboxID: outboxID, Reason: "operator reviewed the permanent class", Operator: "t049-operator",
	})
	if err != nil {
		t.Fatalf("repeated Unblock: %v", err)
	}
	if !repeat.Deduplicated || repeat.StoredResult == "" {
		t.Fatalf("repeated unblock = %+v, want a deduplicated stored result", repeat)
	}
	// A non-blocked row is refused with zero rows, never a silent transition.
	refused, err := Unblock(context.Background(), pool, UnblockOptions{
		OutboxID: outboxID, Reason: "second review", Operator: "t049-operator", OperationID: "t049-unblock-2",
	})
	if err != nil {
		t.Fatalf("second Unblock: %v", err)
	}
	if refused.Unblocked {
		t.Fatalf("unblocking a pending row = %+v, want zero rows updated", refused)
	}
}

// TestRetentionPruneAuditedAndPublishedOnly covers the audited retention
// prune: only published rows older than the window are deleted, pending and
// blocked rows are never touched, and a repeated operation converges.
func TestRetentionPruneAuditedAndPublishedOnly(t *testing.T) {
	dsn := startMigratedPostgres(t)
	pool := openEventsPool(t, dsn)
	ctx := context.Background()

	_, publishedID := appendConsumerEvent(t, pool, statusChangedConsumerEvent(t, "obs-t049-prune-old", "pending"))
	_, pendingID := appendConsumerEvent(t, pool, statusChangedConsumerEvent(t, "obs-t049-prune-pending", "pending"))
	_, blockedID := appendConsumerEvent(t, pool, statusChangedConsumerEvent(t, "obs-t049-prune-blocked", "pending"))
	if _, err := pool.Exec(ctx, `
UPDATE outbox_events SET publish_state = 'published', published_at = now() - interval '2 hours'
WHERE id = $1`, publishedID); err != nil {
		t.Fatalf("mark published: %v", err)
	}
	if _, err := pool.Exec(ctx, `
UPDATE outbox_events SET publish_state = 'blocked', last_error_class = 'permanent' WHERE id = $1`, blockedID); err != nil {
		t.Fatalf("mark blocked: %v", err)
	}

	result, err := RetentionPrune(ctx, pool, PruneOptions{
		Retention: time.Hour, Reason: "weekly retention pass", Operator: "t049-operator",
	})
	if err != nil {
		t.Fatalf("RetentionPrune: %v", err)
	}
	if result.Deleted != 1 {
		t.Fatalf("pruned rows = %d, want 1 (only the old published row)", result.Deleted)
	}
	var published, pending, blocked int64
	if err := pool.QueryRow(ctx, `
SELECT count(*) FILTER (WHERE id = $1), count(*) FILTER (WHERE id = $2), count(*) FILTER (WHERE id = $3)
FROM outbox_events`, publishedID, pendingID, blockedID).Scan(&published, &pending, &blocked); err != nil {
		t.Fatalf("read prune survivors: %v", err)
	}
	if published != 0 || pending != 1 || blocked != 1 {
		t.Fatalf("prune survivors = (published=%d pending=%d blocked=%d), want (0,1,1)", published, pending, blocked)
	}
	var opKind, stored string
	if err := pool.QueryRow(ctx, `
SELECT op_kind, result FROM event_ops_audit WHERE operation_id = $1`, result.OperationID).
		Scan(&opKind, &stored); err != nil {
		t.Fatalf("read prune audit row: %v", err)
	}
	if opKind != "retention_prune" || !strings.Contains(stored, "deleted=1") {
		t.Fatalf("prune audit row = (%s, %q), want retention_prune with deleted=1", opKind, stored)
	}
	repeat, err := RetentionPrune(ctx, pool, PruneOptions{
		Retention: time.Hour, Reason: "weekly retention pass", Operator: "t049-operator",
	})
	if err != nil {
		t.Fatalf("repeated RetentionPrune: %v", err)
	}
	if !repeat.Deduplicated || repeat.StoredResult != stored {
		t.Fatalf("repeated prune = %+v, want the stored result %q", repeat, stored)
	}
}

// TestReplayRefusesUnknownConsumer pins the fail-closed wiring: a replay for
// a consumer that is not wired in this batch is refused before any event is
// touched.
func TestReplayRefusesUnknownConsumer(t *testing.T) {
	dsn := startMigratedPostgres(t)
	pool := openEventsPool(t, dsn)
	reference, err := NewReferenceConsumer(pool, ConsumerOptions{
		GapWait: 200 * time.Millisecond, BackoffBase: 10 * time.Millisecond, BackoffMax: 50 * time.Millisecond,
		RetryLimit: 3, ChainID: 1,
	})
	if err != nil {
		t.Fatalf("NewReferenceConsumer: %v", err)
	}
	_, err = Replay(context.Background(), pool, reference.Consumer, ReplayOptions{
		ConsumerName: "some-other-consumer",
		Scope:        ReplayScope{Kind: ReplayScopeEventIDs, EventIDs: []uuid.UUID{uuid.New()}},
		Reason:       "should refuse", Operator: "t049-operator",
	})
	if err == nil {
		t.Fatal("replay for an unwired consumer was accepted")
	}
	if !errors.Is(err, ErrContract) {
		t.Fatalf("refusal = %v, want ErrContract", err)
	}
}
