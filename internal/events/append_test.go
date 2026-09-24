package events

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// scriptedRow is a pgx.Row whose Scan fills the destinations from a script.
type scriptedRow struct {
	err  error
	fill func(dest ...any) error
}

func (r scriptedRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if r.fill == nil {
		return errors.New("scripted row has no fill function")
	}
	return r.fill(dest...)
}

type txStep struct {
	contains string
	row      pgx.Row
}

type txCall struct {
	sql  string
	args []any
}

// scriptedTx is a pgx.Tx stand-in: the embedded nil interface satisfies the
// full pgx.Tx surface while QueryRow answers from the script. Only QueryRow is
// ever called by Append, so no other method needs an implementation.
type scriptedTx struct {
	pgx.Tx
	mu    sync.Mutex
	calls []txCall
	steps []txStep
}

func (tx *scriptedTx) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	tx.mu.Lock()
	tx.calls = append(tx.calls, txCall{sql: sql, args: args})
	steps := tx.steps
	tx.mu.Unlock()
	for _, step := range steps {
		if strings.Contains(sql, step.contains) {
			return step.row
		}
	}
	return scriptedRow{err: fmt.Errorf("unexpected query: %s", sql)}
}

func (tx *scriptedTx) callCount() int {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return len(tx.calls)
}

func (tx *scriptedTx) call(t *testing.T, index int) txCall {
	t.Helper()
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if index >= len(tx.calls) {
		t.Fatalf("query %d not issued; %d queries total", index, len(tx.calls))
	}
	return tx.calls[index]
}

// fillValues returns a Scan filler for the scripted destinations.
func fillValues(values ...any) func(dest ...any) error {
	return func(dest ...any) error {
		if len(dest) != len(values) {
			return fmt.Errorf("scripted row has %d values, Scan got %d destinations", len(values), len(dest))
		}
		for i, value := range values {
			switch target := dest[i].(type) {
			case *int64:
				v, ok := value.(int64)
				if !ok {
					return fmt.Errorf("dest %d is *int64, script has %T", i, value)
				}
				*target = v
			case *bool:
				v, ok := value.(bool)
				if !ok {
					return fmt.Errorf("dest %d is *bool, script has %T", i, value)
				}
				*target = v
			default:
				return fmt.Errorf("unsupported destination %d (%T)", i, dest[i])
			}
		}
		return nil
	}
}

// countingConflictObserver records identity-conflict observations.
type countingConflictObserver struct {
	mu    sync.Mutex
	types []string
}

func (o *countingConflictObserver) ObserveEventsIdentityConflict(eventType string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.types = append(o.types, eventType)
}

func (o *countingConflictObserver) count() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.types)
}

func (o *countingConflictObserver) last() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.types) == 0 {
		return ""
	}
	return o.types[len(o.types)-1]
}

// newVersionTx scripts the version high-water query and the insert row.
func newVersionTx(maxVersion int64, insert pgx.Row) *scriptedTx {
	return &scriptedTx{steps: []txStep{
		{contains: "coalesce(max(aggregate_version)", row: scriptedRow{fill: fillValues(maxVersion)}},
		{contains: "INSERT INTO outbox_events", row: insert},
	}}
}

func TestAppendInsertsNewEvent(t *testing.T) {
	ctx := context.Background()
	ev := mustNewEvent(t, validEvent(t, EventTypeDepositObservationCreated))
	tx := newVersionTx(0, scriptedRow{fill: fillValues(int64(42), int64(1), true)})

	res, err := Append(ctx, tx, ev)
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if res.OutboxID != 42 || res.AggregateVersion != 1 || res.Noop {
		t.Fatalf("Append() = %#v, want id=42 version=1 noop=false", res)
	}
	wantID := NewEventID(EVMLogNaturalKey(1, "0xblock", "0xtx", 0))
	if res.EventID != wantID {
		t.Fatalf("EventID = %s, want %s", res.EventID, wantID)
	}
	if tx.callCount() != 2 {
		t.Fatalf("Append issued %d queries, want 2", tx.callCount())
	}
	versionCall := tx.call(t, 0)
	if !strings.Contains(versionCall.sql, "coalesce(max(aggregate_version)") {
		t.Fatalf("first query is not the version query: %s", versionCall.sql)
	}
	if versionCall.args[0] != "deposit_observation" || versionCall.args[1] != "obs-1" {
		t.Fatalf("version query args = %v", versionCall.args)
	}

	insertCall := tx.call(t, 1)
	if !strings.Contains(insertCall.sql, "ON CONFLICT (chain_id, block_hash, tx_hash, log_index) WHERE identity_kind = 'evm_log'") {
		t.Fatalf("insert does not target the evm_log identity index: %s", insertCall.sql)
	}
	args := insertCall.args
	if len(args) != 19 {
		t.Fatalf("insert has %d args, want 19", len(args))
	}
	if args[0] != wantID.String() {
		t.Fatalf("insert event_id arg = %v, want %s", args[0], wantID)
	}
	if args[1] != EventTypeDepositObservationCreated || args[2] != SchemaVersionV1 {
		t.Fatalf("insert type/version args = %v/%v", args[1], args[2])
	}
	if args[5] != int64(1) {
		t.Fatalf("insert aggregate_version arg = %v, want 1", args[5])
	}
	if args[6] != string(ev.PayloadBytes()) {
		t.Fatalf("insert payload arg = %v, want frozen canonical bytes", args[6])
	}
	if args[7] != ev.PayloadHash() {
		t.Fatalf("insert payload_hash arg = %v, want %s", args[7], ev.PayloadHash())
	}
	if args[12] != "0xtx" || args[13] != 0 {
		t.Fatalf("insert tx_hash/log_index args = %v/%v", args[12], args[13])
	}
	if args[14] != nil || args[15] != nil {
		t.Fatalf("revision args on a created event = %v/%v, want nil", args[14], args[15])
	}
}

func TestAppendDerivesContinuationVersion(t *testing.T) {
	ctx := context.Background()
	ev := mustNewEvent(t, validEvent(t, EventTypeWithdrawalRequestReceived))
	tx := newVersionTx(7, scriptedRow{fill: fillValues(int64(9), int64(8), true)})

	res, err := Append(ctx, tx, ev)
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if res.AggregateVersion != 8 {
		t.Fatalf("AggregateVersion = %d, want 8", res.AggregateVersion)
	}
	insertCall := tx.call(t, 1)
	if !strings.Contains(insertCall.sql, "ON CONFLICT (aggregate_type, aggregate_id, aggregate_version) WHERE identity_kind = 'business_object'") {
		t.Fatalf("insert does not target the business_object identity index: %s", insertCall.sql)
	}
	wantID := NewEventID(BusinessObjectNaturalKey("deposit_observation", "obs-1", 8))
	if insertCall.args[0] != wantID.String() {
		t.Fatalf("insert event_id arg = %v, want %s", insertCall.args[0], wantID)
	}
	if insertCall.args[5] != int64(8) {
		t.Fatalf("insert aggregate_version arg = %v, want 8", insertCall.args[5])
	}
}

func TestAppendSameIdentitySameContentIsNoop(t *testing.T) {
	observer := &countingConflictObserver{}
	SetIdentityConflictObserver(observer)
	t.Cleanup(func() { SetIdentityConflictObserver(nil) })

	ctx := context.Background()
	ev := mustNewEvent(t, validEvent(t, EventTypeDepositObservationCreated))
	tx := newVersionTx(1, scriptedRow{fill: fillValues(int64(42), int64(1), false)})

	res, err := Append(ctx, tx, ev)
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if !res.Noop {
		t.Fatal("Append() did not report the idempotent no-op")
	}
	if res.AggregateVersion != 1 {
		t.Fatalf("no-op AggregateVersion = %d, want the existing row version 1", res.AggregateVersion)
	}
	if observer.count() != 0 {
		t.Fatalf("no-op raised %d conflict alerts, want 0", observer.count())
	}
}

func TestAppendSameIdentityDifferentContentIsConflict(t *testing.T) {
	observer := &countingConflictObserver{}
	SetIdentityConflictObserver(observer)
	t.Cleanup(func() { SetIdentityConflictObserver(nil) })

	ctx := context.Background()
	ev := mustNewEvent(t, validEvent(t, EventTypeDepositObservationCreated))
	tx := newVersionTx(1, scriptedRow{err: pgx.ErrNoRows})

	_, err := Append(ctx, tx, ev)
	if !errors.Is(err, ErrIdentityConflict) {
		t.Fatalf("Append() error = %v, want ErrIdentityConflict", err)
	}
	if observer.count() != 1 || observer.last() != EventTypeDepositObservationCreated {
		t.Fatalf("conflict alerts = %v, want one for %s", observer.types, EventTypeDepositObservationCreated)
	}
}

func TestAppendClassifiesPGErrors(t *testing.T) {
	observer := &countingConflictObserver{}
	SetIdentityConflictObserver(observer)
	t.Cleanup(func() { SetIdentityConflictObserver(nil) })

	ctx := context.Background()
	ev := mustNewEvent(t, validEvent(t, EventTypeWithdrawalRequestReceived))

	tx := newVersionTx(0, scriptedRow{err: &pgconn.PgError{
		Code:           "23505",
		ConstraintName: "outbox_events_object_identity_uniq",
	}})
	if _, err := Append(ctx, tx, ev); !errors.Is(err, ErrIdentityConflict) {
		t.Fatalf("23505 identity index error = %v, want ErrIdentityConflict", err)
	}
	if observer.count() != 1 {
		t.Fatalf("identity-index 23505 raised %d alerts, want 1", observer.count())
	}

	tx = newVersionTx(0, scriptedRow{err: &pgconn.PgError{
		Code:           "23514",
		ConstraintName: "outbox_events_state_consistency",
	}})
	if _, err := Append(ctx, tx, ev); !errors.Is(err, ErrContract) {
		t.Fatalf("23514 error = %v, want ErrContract", err)
	}
	if observer.count() != 1 {
		t.Fatalf("CHECK violation raised %d identity-conflict alerts, want 0 more", observer.count()-1)
	}

	tx = newVersionTx(0, scriptedRow{err: &pgconn.PgError{
		Code:           "23505",
		ConstraintName: "outbox_events_event_id_uniq",
	}})
	if _, err := Append(ctx, tx, ev); !errors.Is(err, ErrIdentityConflict) {
		t.Fatalf("event_id collision = %v, want ErrIdentityConflict", err)
	}
	if observer.count() != 2 {
		t.Fatalf("event_id collision alerts = %d, want 2", observer.count())
	}
}

func TestAppendRejectsInvalidEventWithoutQuery(t *testing.T) {
	ctx := context.Background()
	ev := validEvent(t, EventTypeDepositObservationCreated)
	ev.EventType = "deposit.unknown.fact"
	tx := &scriptedTx{}

	if _, err := Append(ctx, tx, ev); !errors.Is(err, ErrContract) {
		t.Fatalf("Append(unknown type) error = %v, want ErrContract", err)
	}
	if tx.callCount() != 0 {
		t.Fatalf("invalid event issued %d queries, want 0", tx.callCount())
	}

	ev = validEvent(t, EventTypeDepositObservationCreated)
	delete(ev.Payload, "state")
	if _, err := Append(ctx, tx, ev); !errors.Is(err, ErrContract) {
		t.Fatalf("Append(missing payload key) error = %v, want ErrContract", err)
	}
	if tx.callCount() != 0 {
		t.Fatalf("invalid payload issued %d queries, want 0", tx.callCount())
	}
}

func TestAppendRequiresTransaction(t *testing.T) {
	ev := mustNewEvent(t, validEvent(t, EventTypeDepositObservationCreated))
	if _, err := Append(context.Background(), nil, ev); !errors.Is(err, ErrContract) {
		t.Fatalf("Append(nil tx) error = %v, want ErrContract", err)
	}
}

func TestAppendRejectsNegativeHighWaterMark(t *testing.T) {
	ctx := context.Background()
	ev := mustNewEvent(t, validEvent(t, EventTypeDepositObservationCreated))
	tx := newVersionTx(-1, scriptedRow{fill: fillValues(int64(1), int64(1), true)})

	if _, err := Append(ctx, tx, ev); !errors.Is(err, ErrContract) {
		t.Fatalf("Append(negative max) error = %v, want ErrContract", err)
	}
	if tx.callCount() != 1 {
		t.Fatalf("negative high-water mark issued %d queries, want 1 (no insert)", tx.callCount())
	}
}
