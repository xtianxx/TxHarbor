//go:build integration

// catalog_conformance_integration_test.go is the B3/T032 conformance matrix
// for the five non-revision catalog v1 event types implemented by T026–T030
// (deposit.observation.created/status_changed,
// deposit.confirmation.confirmed, withdrawal.request.received,
// withdrawal.execution.state_changed). The three revision types are closed by
// T058 (US4).
//
// The matrix asserts, per type: event_type, schema_version=1, identity kind,
// version monotonicity from 1 per object, chain identity presence where the
// catalog requires it, payload minimality (no forbidden material) and
// identity uniqueness (same fact = no-op, same identity with different content
// = refusal). It also runs the producer-wiring scan (each integration point
// file calls its emission helper) and the 010 attempt-level referential
// integrity audit over state_changed attempt references.
//
// Division of evidence for the attempt arm: this file proves the audit and a
// resolved reference against real seeded 010 authority rows; T040
// (internal/execution/outbox_execution_atomicity_integration_test.go) proves
// the same invariant on the real converge path (unknown result -> reconciling).
package events

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// depositObservationID mirrors the producer-side stable business identity of
// one 004 observation (the aggregate_id of its emitted events).
func depositObservationID(chainID int64, blockHash, txHash string, logIndex uint64) string {
	return fmt.Sprintf("%d/%s/%s/%d", chainID, blockHash, txHash, logIndex)
}

// t032Append commits one event through the public Append inside its own
// transaction.
func t032Append(t *testing.T, ctx context.Context, pool *pgxpool.Pool, ev Event) AppendResult {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin append tx: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	res, err := Append(ctx, tx, ev)
	if err != nil {
		t.Fatalf("Append(%s): %v", ev.EventType, err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit append: %v", err)
	}
	return res
}

// t032Row reads one stored outbox row by event_id.
func t032Row(t *testing.T, ctx context.Context, pool *pgxpool.Pool, eventID uuid.UUID) (string, int, string, string, string, int64, int64, string, string, int, map[string]any) {
	t.Helper()
	rows, err := pool.Query(ctx, `
SELECT event_type, schema_version, identity_kind, aggregate_type, aggregate_id, aggregate_version,
       COALESCE(chain_id, 0), COALESCE(block_hash, ''), COALESCE(tx_hash, ''), COALESCE(log_index, 0), payload
FROM outbox_events WHERE event_id = $1`, eventID.String())
	if err != nil {
		t.Fatalf("query event %s: %v", eventID, err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatalf("event %s not found", eventID)
	}
	var (
		eventType, identityKind, aggregateType, aggregateID, blockHash, txHash string
		schemaVersion, logIndex                                                int
		aggregateVersion, chainID                                              int64
		payload                                                                []byte
	)
	if err := rows.Scan(&eventType, &schemaVersion, &identityKind, &aggregateType, &aggregateID,
		&aggregateVersion, &chainID, &blockHash, &txHash, &logIndex, &payload); err != nil {
		t.Fatalf("scan event %s: %v", eventID, err)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate event %s: %v", eventID, err)
	}
	decoded := map[string]any{}
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("decode payload of %s: %v", eventID, err)
	}
	return eventType, schemaVersion, identityKind, aggregateType, aggregateID, aggregateVersion, chainID, blockHash, txHash, logIndex, decoded
}

func t032Count(t *testing.T, ctx context.Context, pool *pgxpool.Pool, aggregateID string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE aggregate_id = $1`, aggregateID).Scan(&n); err != nil {
		t.Fatalf("count events for %s: %v", aggregateID, err)
	}
	return n
}

// TestT032CatalogConformanceMatrix drives one representative event per
// implemented non-revision type through Append and asserts the stored row.
func TestT032CatalogConformanceMatrix(t *testing.T) {
	dsn := startMigratedPostgres(t)
	pool := openEventsPool(t, dsn)
	ctx := context.Background()

	const chainID = int64(31337)
	blockHash := "0x" + strings.Repeat("ab", 32)
	txHash := "0x" + strings.Repeat("cd", 32)
	observationA := depositObservationID(chainID, blockHash, txHash, 0)
	blockHashB := "0x" + strings.Repeat("ef", 32)
	txHashB := "0x" + strings.Repeat("12", 32)
	observationB := depositObservationID(chainID, blockHashB, txHashB, 1)

	// #1 deposit.observation.created, object version 1 (evm_log identity).
	created := Event{
		EventType: EventTypeDepositObservationCreated, SchemaVersion: SchemaVersionV1,
		IdentityKind: IdentityKindEVMLog, AggregateType: "deposit_observation", AggregateID: observationA,
		Payload: map[string]any{
			"observation_id": observationA, "state": "pending", "chain_id": chainID,
			"block_number": int64(100), "block_hash": blockHash, "tx_hash": txHash, "log_index": int64(0),
		},
		OccurredAt: time.Now().UTC(), ChainID: chainID, BlockNumber: 100, BlockHash: blockHash, TxHash: txHash, LogIndex: 0,
	}
	createdRes := t032Append(t, ctx, pool, created)
	t032WantRow(t, ctx, pool, createdRes.EventID, EventTypeDepositObservationCreated, string(IdentityKindEVMLog),
		"deposit_observation", observationA, 1, map[string]any{
			"observation_id": observationA, "state": "pending", "chain_id": chainID,
			"block_number": int64(100), "block_hash": blockHash, "tx_hash": txHash, "log_index": int64(0),
		})

	// Identity uniqueness: the same fact re-appends as an idempotent no-op
	// (same identity, same bytes), never a second row or a version bump.
	createdAgain := t032Append(t, ctx, pool, created)
	if !createdAgain.Noop || createdAgain.AggregateVersion != 1 || createdAgain.EventID != createdRes.EventID {
		t.Fatalf("duplicate created = %+v, want no-op version 1", createdAgain)
	}
	if n := t032Count(t, ctx, pool, observationA); n != 1 {
		t.Fatalf("created rows after duplicate = %d, want 1", n)
	}
	// Identity is the log triple: the same block height under a different hash
	// is a different fact (height never an identity).
	createdOther := created
	createdOther.AggregateID = observationB
	createdOther.BlockHash = blockHashB
	createdOther.TxHash = txHashB
	createdOther.LogIndex = 1
	createdOther.Payload = map[string]any{
		"observation_id": observationB, "state": "pending", "chain_id": chainID,
		"block_number": int64(100), "block_hash": blockHashB, "tx_hash": txHashB, "log_index": int64(1),
	}
	otherRes := t032Append(t, ctx, pool, createdOther)
	if otherRes.EventID == createdRes.EventID {
		t.Fatal("different log triple derived the same event identity")
	}

	// #2 deposit.observation.status_changed, object version 2.
	statusChanged := Event{
		EventType: EventTypeDepositObservationStatusChanged, SchemaVersion: SchemaVersionV1,
		IdentityKind: IdentityKindBusinessObject, AggregateType: "deposit_observation", AggregateID: observationA,
		Payload: map[string]any{
			"from_state": "pending", "to_state": "orphaned", "reason": "reorg_invalidated",
			"observation_id": observationA, "chain_id": chainID, "block_number": int64(100),
			"block_hash": blockHash, "tx_hash": txHash, "log_index": int64(0),
		},
		OccurredAt: time.Now().UTC(),
	}
	statusRes := t032Append(t, ctx, pool, statusChanged)
	t032WantRow(t, ctx, pool, statusRes.EventID, EventTypeDepositObservationStatusChanged, string(IdentityKindBusinessObject),
		"deposit_observation", observationA, 2, map[string]any{
			"from_state": "pending", "to_state": "orphaned", "reason": "reorg_invalidated",
		})
	// Same identity with different content is refused and alerts (never a
	// silent overwrite). Two concurrent producers derive the same next
	// version for a fresh object; the loser refuses.
	conflictObserver := &countingConflictObserver{}
	SetIdentityConflictObserver(conflictObserver)
	t.Cleanup(func() { SetIdentityConflictObserver(nil) })
	observationC := depositObservationID(chainID, "0x"+strings.Repeat("34", 32), "0x"+strings.Repeat("56", 32), 0)
	winnerEvent := statusChangedEvent(t, observationC, "pending", "orphaned", "winner")
	loserEvent := statusChangedEvent(t, observationC, "pending", "orphaned", "loser")
	conn1 := connectNamed(t, ctx, dsn, "t032race1")
	conn2 := connectNamed(t, ctx, dsn, "t032race2")
	watcher := connectNamed(t, ctx, dsn, "t032watch")
	tx1, err := conn1.Begin(ctx)
	if err != nil {
		t.Fatalf("begin conflict tx1: %v", err)
	}
	if _, err := Append(ctx, tx1, winnerEvent); err != nil {
		_ = tx1.Rollback(ctx)
		t.Fatalf("conflict tx1 append: %v", err)
	}
	tx2, err := conn2.Begin(ctx)
	if err != nil {
		t.Fatalf("begin conflict tx2: %v", err)
	}
	outcomes := make(chan appendOutcome, 1)
	go func() {
		_, err := Append(ctx, tx2, loserEvent)
		outcomes <- appendOutcome{err: err}
	}()
	waitForBlockedStatement(t, ctx, watcher, "t032race2")
	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("conflict tx1 commit: %v", err)
	}
	got := <-outcomes
	_ = tx2.Rollback(ctx)
	if !errors.Is(got.err, ErrIdentityConflict) {
		t.Fatalf("conflicting append = %v, want ErrIdentityConflict", got.err)
	}
	if conflictObserver.count() == 0 {
		t.Fatal("identity conflict raised no alert")
	}
	if n := t032Count(t, ctx, pool, observationC); n != 1 {
		t.Fatalf("rows after conflict = %d, want 1 (winner only, no overwrite)", n)
	}

	// #3 deposit.confirmation.confirmed on the second object (created v1
	// above, confirmed v2).
	confirmed := Event{
		EventType: EventTypeDepositConfirmationConfirmed, SchemaVersion: SchemaVersionV1,
		IdentityKind: IdentityKindBusinessObject, AggregateType: "deposit_observation", AggregateID: observationB,
		Payload: map[string]any{
			"policy_version": int64(1), "confirmed_block_number": int64(100), "confirmed_block_hash": blockHashB,
			"observation_id": observationB, "chain_id": chainID, "block_number": int64(100),
			"block_hash": blockHashB, "tx_hash": txHashB, "log_index": int64(1),
		},
		OccurredAt: time.Now().UTC(),
	}
	confirmedRes := t032Append(t, ctx, pool, confirmed)
	t032WantRow(t, ctx, pool, confirmedRes.EventID, EventTypeDepositConfirmationConfirmed, string(IdentityKindBusinessObject),
		"deposit_observation", observationB, 2, map[string]any{
			"policy_version": int64(1), "confirmed_block_number": int64(100), "confirmed_block_hash": blockHashB,
		})

	// #4 withdrawal.request.received, object version 1.
	requestID := "wr-0123456789abcdef0123456789abcdef"
	received := Event{
		EventType: EventTypeWithdrawalRequestReceived, SchemaVersion: SchemaVersionV1,
		IdentityKind: IdentityKindBusinessObject, AggregateType: "withdrawal_request", AggregateID: requestID,
		Payload:    map[string]any{"request_id": requestID, "caller": int64(7), "state": "accepted", "chain_id": chainID},
		OccurredAt: time.Now().UTC(),
	}
	receivedRes := t032Append(t, ctx, pool, received)
	t032WantRow(t, ctx, pool, receivedRes.EventID, EventTypeWithdrawalRequestReceived, string(IdentityKindBusinessObject),
		"withdrawal_request", requestID, 1, map[string]any{
			"request_id": requestID, "caller": int64(7), "state": "accepted", "chain_id": chainID,
		})
	// Unknown type / unsupported version are not routable (fail-closed).
	if _, err := RouteEvent(EventTypeWithdrawalRequestReceived, 99); !errors.Is(err, ErrSchemaUnsupported) {
		t.Fatalf("unsupported schema version route = %v, want ErrSchemaUnsupported", err)
	}
	if _, err := RouteEvent("withdrawal.request.unknown", 1); !errors.Is(err, ErrContract) {
		t.Fatalf("unknown event type route = %v, want ErrContract", err)
	}

	// #5 withdrawal.execution.state_changed, object version 1, with the 010
	// attempt reference. A real attempt fact is seeded first so the reference
	// resolves (T032 attempt-level referential-integrity evidence).
	intentID := "intent-t032"
	t032SeedAttemptFixture(t, ctx, pool, "attempt-t032", intentID)
	stateChanged := Event{
		EventType: EventTypeWithdrawalExecutionStateChanged, SchemaVersion: SchemaVersionV1,
		IdentityKind: IdentityKindBusinessObject, AggregateType: "withdrawal_intent", AggregateID: intentID,
		Payload: map[string]any{
			"from_state": "executing", "to_state": "reconciling", "intent_id": intentID,
			"attempt_id": "attempt-t032", "outcome_class": "pending_unknown",
		},
		OccurredAt: time.Now().UTC(),
	}
	stateRes := t032Append(t, ctx, pool, stateChanged)
	t032WantRow(t, ctx, pool, stateRes.EventID, EventTypeWithdrawalExecutionStateChanged, string(IdentityKindBusinessObject),
		"withdrawal_intent", intentID, 1, map[string]any{
			"from_state": "executing", "to_state": "reconciling", "intent_id": intentID,
		})

	// 010 attempt-level referential integrity audit: every state_changed event
	// carrying an attempt reference resolves to a tx_attempts fact.
	var referencing, dangling int
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM outbox_events
WHERE event_type = $1 AND payload ? 'attempt_id'`, EventTypeWithdrawalExecutionStateChanged).Scan(&referencing); err != nil {
		t.Fatalf("count referencing events: %v", err)
	}
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM outbox_events e
WHERE e.event_type = $1 AND e.payload ? 'attempt_id'
  AND NOT EXISTS (SELECT 1 FROM tx_attempts a WHERE a.attempt_id = e.payload->>'attempt_id')`,
		EventTypeWithdrawalExecutionStateChanged).Scan(&dangling); err != nil {
		t.Fatalf("audit dangling attempt references: %v", err)
	}
	if referencing != 1 || dangling != 0 {
		t.Fatalf("attempt reference audit = %d referencing / %d dangling, want 1/0", referencing, dangling)
	}
	// Matrix summary (printed so the run output carries the 5/5 row).
	for _, row := range []struct {
		eventType, identity, aggregate string
		version                        int64
	}{
		{EventTypeDepositObservationCreated, string(IdentityKindEVMLog), "deposit_observation", 1},
		{EventTypeDepositObservationStatusChanged, string(IdentityKindBusinessObject), "deposit_observation", 2},
		{EventTypeDepositConfirmationConfirmed, string(IdentityKindBusinessObject), "deposit_observation", 2},
		{EventTypeWithdrawalRequestReceived, string(IdentityKindBusinessObject), "withdrawal_request", 1},
		{EventTypeWithdrawalExecutionStateChanged, string(IdentityKindBusinessObject), "withdrawal_intent", 1},
	} {
		t.Logf("matrix PASS type=%s identity=%s aggregate=%s version=%d schema_version=1",
			row.eventType, row.identity, row.aggregate, row.version)
	}
}

// t032WantRow asserts one stored row's contract fields plus the required
// payload keys, payload minimality and version monotonicity from 1.
func t032WantRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, eventID uuid.UUID,
	eventType, identityKind, aggregateType, aggregateID string, version int64, wantPayload map[string]any) {
	t.Helper()
	spec, ok := LookupEventSpec(eventType)
	if !ok {
		t.Fatalf("catalog has no spec for %s", eventType)
	}
	gotType, gotSchema, gotKind, gotAggregateType, gotAggregateID, gotVersion, _, _, _, _, payload :=
		t032Row(t, ctx, pool, eventID)
	if gotType != eventType || gotSchema != SchemaVersionV1 || gotKind != identityKind ||
		gotAggregateType != aggregateType || gotAggregateID != aggregateID || gotVersion != version {
		t.Fatalf("row = %s/%d/%s/%s/%s/v%d, want %s/%d/%s/%s/%s/v%d",
			gotType, gotSchema, gotKind, gotAggregateType, gotAggregateID, gotVersion,
			eventType, SchemaVersionV1, identityKind, aggregateType, aggregateID, version)
	}
	for _, key := range spec.RequiredPayloadKeys {
		if _, present := payload[key]; !present {
			t.Fatalf("%s payload missing required key %q: %v", eventType, key, payload)
		}
	}
	for key, want := range wantPayload {
		// The payload round-trips through JSONB, so numbers come back as
		// float64; compare by value rendering.
		if got := payload[key]; fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("%s payload[%s] = %v (%T), want %v (%T)", eventType, key, got, got, want, want)
		}
	}
	if forbidden, found := ForbiddenPayload(payload); found {
		t.Fatalf("%s payload carries forbidden material %q", eventType, forbidden)
	}
	if version > 1 {
		// Version continuity: the predecessor version exists for the object.
		var predecessor int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE aggregate_type = $1 AND aggregate_id = $2 AND aggregate_version = $3`,
			aggregateType, aggregateID, version-1).Scan(&predecessor); err != nil || predecessor != 1 {
			t.Fatalf("%s version %d has no predecessor version %d (err %v)", eventType, version, version-1, err)
		}
	}
}

// TestT032ProducerWiring statically pins the T026–T030 integration points:
// each producer file calls its emission helper (which appends in the same
// transaction) and the execution transition carries the attempt context.
func TestT032ProducerWiring(t *testing.T) {
	cases := []struct {
		file string
		want []string
	}{
		{"../indexer/depositcommit.go", []string{"appendDepositObservationCreatedEvent(ctx, tx"}},
		{"../indexer/confirmcommit.go", []string{"appendDepositConfirmationConfirmedEvent(ctx, tx"}},
		{"../indexer/reorgcommit.go", []string{"appendDepositObservationStatusChangedEvent(ctx, tx"}},
		{"../indexer/outbox_events.go", []string{
			"EventTypeDepositObservationCreated", "EventTypeDepositObservationStatusChanged",
			"EventTypeDepositConfirmationConfirmed", "events.Append(ctx, tx",
		}},
		{"../withdrawal/intake.go", []string{"appendWithdrawalRequestReceivedEvent(ctx, tx"}},
		{"../execution/intent.go", []string{
			"EventTypeWithdrawalExecutionStateChanged", "appendIntentStateChangedEvent(ctx, tx",
		}},
		{"../execution/advance.go", []string{"TransitionIntentWithContext(ctx, tx", "AttemptID:    res.attemptID"}},
	}
	for _, tc := range cases {
		body, err := os.ReadFile(tc.file)
		if err != nil {
			t.Fatalf("read %s: %v", tc.file, err)
		}
		for _, want := range tc.want {
			if !strings.Contains(string(body), want) {
				t.Errorf("%s does not contain %q (integration point missing)", tc.file, want)
			}
		}
	}
}

// t032SeedAttemptFixture plants the minimal 010 authority rows so the attempt
// reference resolves against tx_attempts (real FKs, real CHECKs).
func t032SeedAttemptFixture(t *testing.T, ctx context.Context, pool *pgxpool.Pool, attemptID, intentID string) {
	t.Helper()
	const (
		chainID   = int64(31337)
		sender    = "0xcccccccccccccccccccccccccccccccccccccccc"
		recipient = "0xdddddddddddddddddddddddddddddddddddddddd"
		callerID  = int64(7)
		authzID   = "authz-t032"
		bindingID = "binding-t032"
	)
	contentHash := "0x" + strings.Repeat("cd", 32)
	for _, stmt := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO caller (caller_id, label) VALUES ($1, 't032') ON CONFLICT (caller_id) DO NOTHING`, []any{callerID}},
		{`INSERT INTO withdrawal_authorizations
		    (authorization_id, caller_id, chain_id, asset, recipient, amount, state)
		  VALUES ($1, $2, $3, $4, $5, $6, 'active')`,
			[]any{authzID, callerID, chainID, sender, recipient, "100"}},
		{`INSERT INTO withdrawal_requests
		    (request_id, caller_id, idempotency_key, authorization_id, chain_id, asset, recipient, amount)
		  VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
			[]any{"req-t032", callerID, "idem-t032", authzID, chainID, sender, recipient, "100"}},
		{`INSERT INTO payment_intents
		    (intent_id, request_id, chain_id, sender, authorization_id, authorization_version, state, admitted_recovery_version)
		  VALUES ($1, $2, $3, $4, $5, 1, 'admitted', 0)`,
			[]any{intentID, "req-t032", chainID, sender, authzID}},
		{`INSERT INTO nonce_wallet_registry (chain_id, sender, state, registry_seq)
		  VALUES ($1, $2, 'active', 1)`, []any{chainID, sender}},
		{`INSERT INTO nonce_bindings
		    (binding_id, intent_id, chain_id, sender, nonce, state, authorization_id,
		     authorization_version, registry_seq, allocation_observation_id)
		  VALUES ($1, $2, $3, $4, 1, 'allocated', $5, $6, 1, 'obs-t032')`,
			[]any{bindingID, intentID, chainID, sender, authzID, strings.Repeat("ab", 32)}},
		{`INSERT INTO tx_attempts
		    (attempt_id, signing_request_id, intent_id, binding_ref, authorization_id,
		     authorization_version, recovery_version, chain_id, sender, nonce, tx_type,
		     to_addr, value, data, gas_limit, gas_price, asset, recipient, amount,
		     canonical_envelope, content_hash)
		  VALUES ($1, $2, $3, $4, $5, 1, 0, $6, $7, 1, 0, $8, 0, $9, 21000, 1, $8, $10, 1, '{}', $11)`,
			[]any{attemptID, attemptID + "-sr", intentID, bindingID, authzID, chainID, sender, sender,
				[]byte{0x01}, recipient, contentHash}},
	} {
		if _, err := pool.Exec(ctx, stmt.sql, stmt.args...); err != nil {
			t.Fatalf("seed 010 fixture: %v", err)
		}
	}
}
