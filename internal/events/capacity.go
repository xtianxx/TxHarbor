// capacity.go owns the outbox capacity guard (T069; PD-2; contracts/
// capacity.md §1–§3): the PostgreSQL-only backlog observation
// (outbox_pending_count / outbox_pending_oldest_age_seconds by event family),
// the fail-closed configuration formula, the soft/hard classification and the
// admission decision the controllable-write gate (T070) and the chain-side
// pause decision (T071) consume.
//
// Boundary rules (MUST NOT be weakened):
//
//   - Redis never participates: the observation reads the PostgreSQL partial
//     index only (contracts/capacity.md §1/§3).
//   - The guard only decides about admitting NEW controllable work, before
//     that work is accepted. It opens no transaction of its own, and it never
//     writes, overwrites or removes a row, never re-publishes and never
//     inspects business state.
//   - Accepted units are never refused: once a unit was accepted, its business
//     state and its required events commit in one transaction and this file is
//     never consulted by Append (I-CAP, contracts/capacity.md §2).
//   - hard_limit is an admission gate + pause trigger, not a physical capacity
//     limit. Accepted writes are never refused because of capacity, and
//     on-chain facts are never rejected: while persistence is still safe they
//     keep entering the Outbox and pending may exceed hard_limit; when
//     persistence is not safe the 003/004 loops pause from their reliable
//     progress and rescan after recovery (T071). Nothing here guarantees that
//     physical storage is never exhausted, and the guard MUST NOT be described
//     that way.
//   - The pending|blocked red line: nothing in this file deletes, drops or
//     silently discards an event; retention remains the only remover and it
//     only ever touches published rows.
package events

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// CapacityLimits is the capacity guard configuration (contracts/capacity.md
// §1). Every value is a measured input (contracts §5 method); the zero value
// is not a valid configuration (Configured reports false). Thresholds are
// never invented here: the caller passes measured/configured values.
type CapacityLimits struct {
	// Reserve is the headroom kept for admitted units: the events of every
	// in-flight unit must still fit (reserve >= E_max x F_max).
	Reserve int64
	// SoftLimit is the admission boundary: from here new controllable funding
	// writes are refused and non-critical features degrade.
	SoftLimit int64
	// HardLimit is the pause-trigger boundary for chain processing when
	// persistence is not safe. It is not a physical capacity limit.
	HardLimit int64
}

// Configured reports whether every limit carries a positive value.
func (l CapacityLimits) Configured() bool {
	return l.Reserve > 0 && l.SoftLimit > 0 && l.HardLimit > 0
}

// Validate enforces the fail-closed formula 0 < reserve < soft_limit <
// hard_limit (contracts/capacity.md §1). A violation refuses construction and
// startup; there is no unbounded fallback and no silent default.
func (l CapacityLimits) Validate() error {
	if !l.Configured() {
		return errors.New("capacity limits must all be positive (reserve, soft_limit, hard_limit)")
	}
	if !(l.Reserve < l.SoftLimit && l.SoftLimit < l.HardLimit) {
		return fmt.Errorf("capacity limits violate 0 < reserve (%d) < soft_limit (%d) < hard_limit (%d)",
			l.Reserve, l.SoftLimit, l.HardLimit)
	}
	return nil
}

// CapacityLevel is the closed backlog classification (contracts/capacity.md
// §3 behavior matrix).
type CapacityLevel string

const (
	// CapacityNormal: pending < soft_limit. Every flow behaves normally.
	CapacityNormal CapacityLevel = "normal"
	// CapacitySoft: soft_limit <= pending < hard_limit. New controllable
	// funding writes are refused with the retryable channel (T062 style);
	// chain observation/confirmation/revision and in-flight withdrawals
	// continue under their reserved headroom.
	CapacitySoft CapacityLevel = "soft"
	// CapacityHard: pending >= hard_limit. The admission gate keeps refusing
	// new controllable work; on-chain facts are still never rejected while
	// persistence is safe, and the chain-processing pause trigger arms
	// (T071).
	CapacityHard CapacityLevel = "hard"
)

// RefusesNewControllable reports whether the level refuses a new controllable
// work item. It is fail-closed: every value other than CapacityNormal refuses,
// so an unclassified observation never admits.
func (l CapacityLevel) RefusesNewControllable() bool { return l != CapacityNormal }

// ClassifyCapacityLevel maps an observed pending total onto the closed level
// set. A negative count (impossible for a SQL count, guarded anyway) is
// treated as zero; the classification never fabricates a value. An
// unconfigured limit set classifies as hard (fail closed).
func ClassifyCapacityLevel(pending int64, limits CapacityLimits) CapacityLevel {
	if pending < 0 {
		pending = 0
	}
	switch {
	case pending >= limits.HardLimit:
		return CapacityHard
	case pending >= limits.SoftLimit:
		return CapacitySoft
	default:
		return CapacityNormal
	}
}

// CapacityFamilyPending is one event family's pending observation.
type CapacityFamilyPending struct {
	EventFamily      string
	PendingCount     int64
	OldestAgeSeconds float64
}

// CapacitySnapshot is one read-only observation of the pending backlog.
type CapacitySnapshot struct {
	Families         []CapacityFamilyPending
	PendingTotal     int64
	OldestAgeSeconds float64
	Level            CapacityLevel
}

// CapacityObserver receives the observation metrics. *metrics.Metrics
// satisfies it; a nil observer disables observation (unit tests).
type CapacityObserver interface {
	SetOutboxPending(eventFamily string, count int)
	SetOutboxPendingOldestAge(eventFamily string, seconds float64)
	ObserveCapacitySoftBreach()
	ObserveCapacityHardBreach()
	ObserveCapacityRefusal(opClass string)
}

// Capacity op-class vocabulary: the low-cardinality label values of
// capacity_refusals_total{op_class}. Only new controllable work classes ever
// appear here; already-accepted work is never refused and is never counted.
const (
	// CapacityOpWithdrawalCreate is new withdrawal creation (007 receive-only
	// intake; PD-2).
	CapacityOpWithdrawalCreate = "withdrawal_create"
)

// CapacityGuard evaluates the PostgreSQL-only pending backlog before new
// controllable work is admitted. It holds no authority: it never
// authenticates, never authorizes, never gates an accepted unit and never
// participates in any funding decision for an existing request.
type CapacityGuard struct {
	pool     *pgxpool.Pool
	limits   CapacityLimits
	observer CapacityObserver
}

// NewCapacityGuard validates the fail-closed configuration and returns the
// guard. A nil pool or an invalid formula refuses construction (never a
// silently disabled guard).
func NewCapacityGuard(pool *pgxpool.Pool, limits CapacityLimits, observer CapacityObserver) (*CapacityGuard, error) {
	if pool == nil {
		return nil, errors.New("capacity guard requires a database pool")
	}
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	return &CapacityGuard{pool: pool, limits: limits, observer: observer}, nil
}

// Limits returns a copy of the configured limits (read-only surface).
func (g *CapacityGuard) Limits() CapacityLimits {
	if g == nil {
		return CapacityLimits{}
	}
	return g.limits
}

// Observe reads the pending backlog through CapacitySQL (PG partial index,
// grouped by event family; Redis never participates), refreshes the pending
// gauges and counts one soft/hard breach when the level is not normal. The
// observation is read-only: no event row is written, overwritten or removed.
func (g *CapacityGuard) Observe(ctx context.Context) (CapacitySnapshot, error) {
	if g == nil || g.pool == nil {
		return CapacitySnapshot{}, errors.New("capacity guard is not configured")
	}
	rows, err := g.pool.Query(ctx, CapacitySQL)
	if err != nil {
		return CapacitySnapshot{}, fmt.Errorf("observe outbox capacity: %w", err)
	}
	defer rows.Close()

	var snap CapacitySnapshot
	for rows.Next() {
		var family CapacityFamilyPending
		if err := rows.Scan(&family.EventFamily, &family.PendingCount, &family.OldestAgeSeconds); err != nil {
			return CapacitySnapshot{}, fmt.Errorf("scan outbox capacity: %w", err)
		}
		snap.Families = append(snap.Families, family)
		snap.PendingTotal += family.PendingCount
		if family.OldestAgeSeconds > snap.OldestAgeSeconds {
			snap.OldestAgeSeconds = family.OldestAgeSeconds
		}
		if g.observer != nil {
			g.observer.SetOutboxPending(family.EventFamily, int(family.PendingCount))
			g.observer.SetOutboxPendingOldestAge(family.EventFamily, family.OldestAgeSeconds)
		}
	}
	if err := rows.Err(); err != nil {
		return CapacitySnapshot{}, fmt.Errorf("read outbox capacity: %w", err)
	}

	snap.Level = ClassifyCapacityLevel(snap.PendingTotal, g.limits)
	if g.observer != nil {
		switch snap.Level {
		case CapacitySoft:
			g.observer.ObserveCapacitySoftBreach()
		case CapacityHard:
			g.observer.ObserveCapacityHardBreach()
		}
	}
	return snap, nil
}

// CapacityAdmission is one admission evaluation for a new controllable work
// item. Refused is true from the soft boundary onward: the guard never admits
// new controllable work at or above soft_limit.
type CapacityAdmission struct {
	Level            CapacityLevel
	PendingTotal     int64
	OldestAgeSeconds float64
	Refused          bool
}

// DecideAdmission is the pure admission rule over one observation (no I/O).
func DecideAdmission(snapshot CapacitySnapshot) CapacityAdmission {
	return CapacityAdmission{
		Level:            snapshot.Level,
		PendingTotal:     snapshot.PendingTotal,
		OldestAgeSeconds: snapshot.OldestAgeSeconds,
		Refused:          snapshot.Level.RefusesNewControllable(),
	}
}

// AdmitNewControllable evaluates the backlog BEFORE a new controllable work
// item is accepted and counts one capacity_refusals_total{op_class} when it
// refuses. An observation failure returns an error and admits nothing:
// callers fail closed for the new write and never treat an unreadable state as
// capacity available. Already-accepted units and replays of accepted units
// never reach this call.
func (g *CapacityGuard) AdmitNewControllable(ctx context.Context, opClass string) (CapacityAdmission, error) {
	snapshot, err := g.Observe(ctx)
	if err != nil {
		return CapacityAdmission{}, err
	}
	admission := DecideAdmission(snapshot)
	if admission.Refused && g.observer != nil {
		g.observer.ObserveCapacityRefusal(opClass)
	}
	return admission, nil
}
