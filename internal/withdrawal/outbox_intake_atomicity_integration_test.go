//go:build integration

// outbox_intake_atomicity_integration_test.go is the B3/T039 atomicity probe
// for the 007 intake integration point (T029; data-model §4):
//
//   - a first receipt commits the withdrawal_requests row and its
//     withdrawal.request.received event together (Accepted semantics
//     unchanged, delivery not part of the receive contract);
//   - a same-key replay writes no second request and no second event;
//   - refused outcomes (409 conflict) write neither;
//   - the exact receipt-transaction shape rolled back leaves neither the
//     business row nor the event (no half commit).
//
// It reuses the package integration fixtures (grantSetup + the T010/T008
// helpers) against the real 000015 schema.
package withdrawal

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/events"
)

// withdrawalOutboxRows reads every outbox row of one withdrawal_request
// aggregate in version order.
type withdrawalOutboxRow struct {
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

func withdrawalOutboxRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, aggregateID string) []withdrawalOutboxRow {
	t.Helper()
	rows, err := pool.Query(ctx, `
SELECT event_id::text, event_type, schema_version, identity_kind, aggregate_type, aggregate_id,
       aggregate_version, publish_state, payload
FROM outbox_events
WHERE aggregate_id = $1
ORDER BY aggregate_version`, aggregateID)
	if err != nil {
		t.Fatalf("query outbox rows for %s: %v", aggregateID, err)
	}
	defer rows.Close()
	var out []withdrawalOutboxRow
	for rows.Next() {
		var (
			row     withdrawalOutboxRow
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

func withdrawalOutboxTotal(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events`).Scan(&n); err != nil {
		t.Fatalf("count outbox_events: %v", err)
	}
	return n
}

// TestOutboxWithdrawalRequestReceivedAtomicity is the T029 probe.
func TestOutboxWithdrawalRequestReceivedAtomicity(t *testing.T) {
	ctx, pool := grantSetup(t)
	key := intakeKey(t, ctx, pool, 7201)
	intakeSupplyGrant(t, ctx, pool, 7201, "auth-b3-1", intakeAmount)

	first, err := SubmitWithdrawal(ctx, pool, intakeReq(key, "idem-b3-1", "auth-b3-1"))
	if err != nil || first.Status != 201 {
		t.Fatalf("first SubmitWithdrawal = %+v (err %v), want 201", first, err)
	}
	rows := withdrawalOutboxRows(t, ctx, pool, first.RequestID)
	if len(rows) != 1 {
		t.Fatalf("outbox rows for %s = %d, want 1", first.RequestID, len(rows))
	}
	received := rows[0]
	if received.EventType != events.EventTypeWithdrawalRequestReceived ||
		received.SchemaVersion != events.SchemaVersionV1 ||
		received.IdentityKind != string(events.IdentityKindBusinessObject) ||
		received.AggregateType != withdrawalRequestAggregateType ||
		received.AggregateVersion != 1 || received.PublishState != "pending" {
		t.Fatalf("received event = %+v, want withdrawal.request.received/v1/business_object/v1", received)
	}
	if received.Payload["request_id"] != first.RequestID ||
		received.Payload["caller"] != float64(7201) ||
		received.Payload["state"] != withdrawalRequestStateAccepted ||
		received.Payload["chain_id"] != float64(intakeChainID) {
		t.Fatalf("received payload = %v, want request_id/caller/state=accepted/chain_id", received.Payload)
	}
	if key, found := events.ForbiddenPayload(received.Payload); found {
		t.Fatalf("received payload carries forbidden material %q", key)
	}

	// Same-key replay: 200 with the same request_id, no second event and no
	// second request row.
	replay, err := SubmitWithdrawal(ctx, pool, intakeReq(key, "idem-b3-1", "auth-b3-1"))
	if err != nil || replay.Status != 200 || replay.RequestID != first.RequestID {
		t.Fatalf("replay = %+v (err %v), want 200 with request_id %s", replay, err, first.RequestID)
	}
	if n := intakeRequestCount(t, ctx, pool); n != 1 {
		t.Fatalf("request rows after replay = %d, want 1", n)
	}
	if n := len(withdrawalOutboxRows(t, ctx, pool, first.RequestID)); n != 1 {
		t.Fatalf("outbox rows after replay = %d, want 1 (replay emits nothing)", n)
	}

	// Conflict (same key, different parameters): 409 writes no request and no
	// event.
	conflictReq := intakeReq(key, "idem-b3-1", "auth-b3-1")
	conflictReq.Amount = "101"
	conflict, err := SubmitWithdrawal(ctx, pool, conflictReq)
	if err != nil || conflict.Status != 409 {
		t.Fatalf("conflict = %+v (err %v), want 409", conflict, err)
	}
	if n := intakeRequestCount(t, ctx, pool); n != 1 {
		t.Fatalf("request rows after conflict = %d, want 1", n)
	}
	if n := withdrawalOutboxTotal(t, ctx, pool); n != 1 {
		t.Fatalf("outbox rows after conflict = %d, want 1", n)
	}

	// Rollback probe: run the exact receipt-transaction shape (request INSERT
	// + receipt audit + event append) with a known identity and ROLLBACK. Both
	// the business row and the event must be absent afterwards — there is no
	// half-committed receipt and no orphan event.
	const rollbackRequestID = "wr-b3rollback0000000000000000000000"
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin rollback probe: %v", err)
	}
	if _, err := tx.Exec(ctx, intakeInsertRequestSQL,
		rollbackRequestID, int64(7201), "idem-b3-rollback", "auth-b3-rollback",
		intakeChainID, intakeAsset, intakeRecipient, intakeAmount); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("insert request in rollback probe: %v", err)
	}
	if err := insertReceiptAudit(ctx, tx, rollbackRequestID, 7201, auditActionCreated, "rollback probe"); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("receipt audit in rollback probe: %v", err)
	}
	if err := appendWithdrawalRequestReceivedEvent(ctx, tx, rollbackRequestID, 7201, intakeChainID); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("append event in rollback probe: %v", err)
	}
	// The event is visible inside the transaction before rollback.
	if n := withdrawalOutboxTotalTx(t, ctx, tx, rollbackRequestID); n != 1 {
		t.Fatalf("in-tx outbox rows = %d, want 1 before rollback", n)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback probe: %v", err)
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM withdrawal_requests WHERE request_id = $1`, rollbackRequestID).Scan(&count); err != nil {
		t.Fatalf("count rolled-back request: %v", err)
	}
	if count != 0 {
		t.Fatalf("rolled-back request rows = %d, want 0", count)
	}
	if n := len(withdrawalOutboxRows(t, ctx, pool, rollbackRequestID)); n != 0 {
		t.Fatalf("rolled-back outbox rows = %d, want 0", n)
	}
}

// withdrawalOutboxTotalTx counts one aggregate's outbox rows inside an open
// transaction (the rollback-probe visibility check).
func withdrawalOutboxTotalTx(t *testing.T, ctx context.Context, tx pgx.Tx, aggregateID string) int {
	t.Helper()
	var n int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE aggregate_id = $1`, aggregateID).Scan(&n); err != nil {
		t.Fatalf("count in-tx outbox rows: %v", err)
	}
	return n
}
