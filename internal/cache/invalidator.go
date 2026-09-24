// invalidator.go implements the T060 event-driven cache invalidation:
// authority-transition outbox events are the invalidation signal (research
// R10) — no dual write, no cache-write alongside a PostgreSQL transaction.
// The invalidator reuses the consumer runtime as an events.Effect
// (Apply), so it rides the same delivery, dedup and retry discipline as every
// other consumer; deletion is idempotent, so at-least-once delivery is
// harmless.
//
// Failure discipline: the cache is non-authoritative. A Redis failure during
// invalidation is logged and counted, never returned as an effect failure
// (returning one would quarantine a perfectly valid authority event because a
// cache was down) and never blocks the partition. Staleness stays bounded by
// the TTL and by epoch rotation on recovery; rebuilding is lazy through
// GetOrLoad's bounded fallback, so recovery never reopens into unbounded
// traffic.
package cache

import (
	"context"
	"errors"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/xtianxx/txharbor/internal/events"
	"github.com/xtianxx/txharbor/internal/logx"
)

// Ref identifies one affected cache key range. An empty ID means the whole
// family (a family-level/aggregate view).
type Ref struct {
	Family Family
	ID     string
}

// AffectedRefs maps one delivered event to the cache key ranges it
// invalidates. Unknown event types map to nothing: invalidation never guesses
// a family from an unknown fact.
func AffectedRefs(env events.Envelope) []Ref {
	family, ok := familyForEventType(env.EventType)
	if !ok {
		return nil
	}
	return []Ref{{Family: family, ID: env.AggregateID}}
}

// familyForEventType is the closed event-type → family mapping (catalog v1).
func familyForEventType(eventType string) (Family, bool) {
	switch {
	case strings.HasPrefix(eventType, "deposit."):
		return FamilyDeposit, true
	case eventType == events.EventTypeWithdrawalRequestReceived:
		return FamilyWithdrawal, true
	case strings.HasPrefix(eventType, "withdrawal.execution."):
		return FamilyExecution, true
	}
	return "", false
}

// Invalidator deletes affected cache ranges for authority-transition events.
// It is safe for concurrent use and holds no durable state.
type Invalidator struct {
	client *Client
	logger *slog.Logger
}

// NewInvalidator builds the invalidator for one cache client.
func NewInvalidator(client *Client) (*Invalidator, error) {
	if client == nil {
		return nil, errNilClient
	}
	return &Invalidator{client: client, logger: slog.Default()}, nil
}

// SetLogger overrides the diagnostic logger.
func (inv *Invalidator) SetLogger(logger *slog.Logger) {
	if logger != nil {
		inv.logger = logger
	}
}

// Invalidate deletes every ref's keys. Exact refs delete the exact key;
// family-wide refs SCAN+DEL the family range. Redis failures are logged, never
// returned: staleness is bounded by TTL and epoch rotation.
func (inv *Invalidator) Invalidate(ctx context.Context, refs []Ref) {
	for _, ref := range refs {
		if !ref.Family.Valid() {
			inv.logger.Warn("cache invalidation skipped: unknown family",
				"family", string(ref.Family))
			continue
		}
		var err error
		if ref.ID == "" {
			err = inv.client.InvalidateFamily(ctx, ref.Family)
		} else {
			err = inv.client.Invalidate(ctx, ref.Family, ref.ID)
		}
		if err != nil {
			inv.logger.Warn("cache invalidation failed; TTL and epoch bound the staleness",
				"family", string(ref.Family),
				"error", logx.Redact(err.Error()))
		}
	}
}

// Handle invalidates the cache ranges affected by one event. It is the direct
// entry point for a lightweight invalidation loop; Apply is the
// consumer-runtime form.
func (inv *Invalidator) Handle(ctx context.Context, env events.Envelope) {
	inv.Invalidate(ctx, AffectedRefs(env))
}

// Apply implements events.Effect so the invalidator can reuse the consumer
// runtime. The PostgreSQL transaction is deliberately untouched: the effect
// is a non-authoritative deletion, and a Redis outage must never turn a valid
// authority event into a quarantine. An inbox row still records the event as
// applied (the consumer runtime owns that), so a redelivery neither repeats
// work nor fails.
func (inv *Invalidator) Apply(ctx context.Context, _ pgx.Tx, env events.Envelope) error {
	inv.Handle(ctx, env)
	return nil
}

// errNilClient keeps the constructor fail-closed.
var errNilClient = errors.New("cache: invalidator requires a client")
