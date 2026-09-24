// refconsumer.go owns the reference consumer (T043): the acceptance-evidence
// consumer that demonstrates "the same event is effectively applied exactly
// once" and "the simulated ledger effect is never duplicated" through the
// persistent inbox + version guard of the T4 transaction.
//
// Boundary statement (FR-16/SC-12; contracts/consumer.md §8): the reference
// consumer is NOT a production ledger and NOT authoritative. It holds no user
// balances. Its evidence covers only this project's event identity/version/
// delivery semantics plus the consumer idempotency contract, and it MUST NOT
// be presented as a guarantee about any external real ledger.
package events

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// RefConsumerName is the reference consumer's independent consumer name: it
// never shares an inbox, version table, progress or consumer group with any
// other consumer (contracts/consumer.md §8).
const RefConsumerName = "txharbor.ref-consumer.v1"

// RefLedgerTable is the simulated upstream ledger the reference consumer
// writes as its effect. It is created by EnsureLedgerSchema at runtime (it is
// deliberately not part of the authoritative migration): it is evidence
// material, rebuildable at any time, and never a production ledger.
const RefLedgerTable = "ref_consumer_ledger"

// ReferenceBoundaryStatement is the FR-16 verification boundary the evidence
// must carry verbatim.
const ReferenceBoundaryStatement = "The reference consumer is not a production ledger and is not authoritative; " +
	"it holds no user balances. Its evidence covers only this project's event identity/version/delivery semantics " +
	"plus the consumer idempotency contract (persistent inbox, version guard, quarantine and progress). " +
	"It is not a guarantee about any external real ledger."

// ReferenceConsumer is the reference consumer runtime.
type ReferenceConsumer struct {
	Consumer *Consumer
	Pool     *pgxpool.Pool
}

// NewReferenceConsumer builds the reference consumer over a pool with the
// given bounds and the simulated-ledger effect. EnsureLedgerSchema must run
// before the first Process.
func NewReferenceConsumer(pool *pgxpool.Pool, opts ConsumerOptions) (*ReferenceConsumer, error) {
	effect := &refLedgerEffect{consumerName: RefConsumerName}
	consumer, err := NewConsumer(pool, RefConsumerName, effect, opts)
	if err != nil {
		return nil, err
	}
	return &ReferenceConsumer{Consumer: consumer, Pool: pool}, nil
}

// EnsureLedgerSchema creates the simulated ledger table when absent. The
// table deliberately has no unique constraint on event_id: the persistent
// inbox is the dedup mechanism under test, and the ledger row count per event
// is the evidence (a second row would prove a duplicate effect).
func (r *ReferenceConsumer) EnsureLedgerSchema(ctx context.Context) error {
	if r == nil || r.Pool == nil {
		return contractErrorf("reference consumer requires a database pool")
	}
	_, err := r.Pool.Exec(ctx, `
CREATE TABLE IF NOT EXISTS `+RefLedgerTable+` (
	consumer_name     TEXT        NOT NULL,
	event_id          UUID        NOT NULL,
	event_type        TEXT        NOT NULL,
	aggregate_type    TEXT        NOT NULL,
	aggregate_id      TEXT        NOT NULL,
	aggregate_version BIGINT      NOT NULL,
	applied_at        TIMESTAMPTZ NOT NULL DEFAULT now()
)`)
	if err != nil {
		return fmt.Errorf("create reference ledger schema: %w", err)
	}
	_, err = r.Pool.Exec(ctx, `CREATE INDEX IF NOT EXISTS ref_consumer_ledger_event_idx
		ON `+RefLedgerTable+` (consumer_name, event_id)`)
	if err != nil {
		return fmt.Errorf("create reference ledger index: %w", err)
	}
	return nil
}

// Process delegates one delivered message to the T4 consumer.
func (r *ReferenceConsumer) Process(ctx context.Context, msg Message) (ProcessResult, error) {
	if r == nil || r.Consumer == nil {
		return ProcessResult{}, contractErrorf("reference consumer is not built")
	}
	return r.Consumer.Process(ctx, msg)
}

// LedgerApplications counts the simulated-ledger rows of one event for the
// reference consumer. Exactly 1 is the "effective application = 1" evidence.
func (r *ReferenceConsumer) LedgerApplications(ctx context.Context, eventID uuid.UUID) (int64, error) {
	var n int64
	err := r.Pool.QueryRow(ctx,
		`SELECT count(*) FROM `+RefLedgerTable+` WHERE consumer_name = $1 AND event_id = $2`,
		RefConsumerName, eventID).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("count reference ledger rows: %w", err)
	}
	return n, nil
}

// LedgerCount counts every simulated-ledger row of the reference consumer.
func (r *ReferenceConsumer) LedgerCount(ctx context.Context) (int64, error) {
	var n int64
	err := r.Pool.QueryRow(ctx,
		`SELECT count(*) FROM `+RefLedgerTable+` WHERE consumer_name = $1`, RefConsumerName).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("count reference ledger rows: %w", err)
	}
	return n, nil
}

// BoundaryStatement returns the FR-16 boundary declaration the evidence
// outputs must carry.
func (r *ReferenceConsumer) BoundaryStatement() string { return ReferenceBoundaryStatement }

// refLedgerEffect inserts one simulated-ledger row inside the T4 transaction:
// the row and the inbox row commit together, so a redelivery never inserts a
// second row (the inbox dedup returns before the effect runs).
type refLedgerEffect struct{ consumerName string }

// Apply implements Effect.
func (e *refLedgerEffect) Apply(ctx context.Context, tx pgx.Tx, env Envelope) error {
	if tx == nil {
		return contractErrorf("reference ledger effect requires an in-progress transaction")
	}
	if strings.TrimSpace(e.consumerName) == "" {
		return contractErrorf("reference ledger effect requires a consumer name")
	}
	_, err := tx.Exec(ctx, `
INSERT INTO `+RefLedgerTable+` (
	consumer_name, event_id, event_type, aggregate_type, aggregate_id, aggregate_version
) VALUES ($1, $2, $3, $4, $5, $6)`,
		e.consumerName, env.EventID, env.EventType, env.AggregateType, env.AggregateID, env.AggregateVersion)
	if err != nil {
		return ClassifyPGError(err)
	}
	return nil
}
