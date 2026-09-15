// reorgmetrics.go wires the 006 recovery execution path to the metrics
// surface (T032, contracts/observability.md) with zero constructor churn:
//
//   - Counters (orphaned/revived/evidence-wait) are at-most-once
//     observations, not audit-exact counts: only the executor sees
//     conversions and waits, so it reports them through the narrow
//     RecoveryMetrics interface (nil = disabled; unit paths stay
//     metric-free). Orphaned counts ride the idempotent transaction's own
//     rowcount (repeat execution converts 0 → counts 0); revived counts
//     ride ReviveRecoveryObservation's converted flag (converged repeats
//     report false → never double-counted, including crash-resume
//     re-walks). The count happens after the transaction commits, so a
//     crash (or lost commit response) between commit and counting may
//     undercount; process restart resets counters to zero. The durable
//     deposit_observation_transitions / reorg_recovery_events rows are the
//     audit authority, never these counters. Evidence-wait classes are the
//     executor's own cause taxonomy (transport/timeout/rate-limited/
//     invalid-response pass-through; chain-mismatch/contradictory/
//     insufficient holds) — no new taxonomy, no heights/hashes in labels.
//   - Gauges (active/depth/bound/frontier-lag/reconcile) are state
//     snapshots derived from the durable row at read time, so they belong
//     to the serve-side observer (same "re-read, don't trust memory"
//     discipline as T012), not to the executor: RecoverySnapshot carries
//     everything the observer needs in one point read.
package indexer

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// RecoveryMetrics is the executor-to-registry funnel for the three 006
// monotonic counters. Implementations must be repeat-safe on their own
// terms (Prometheus counters only increase); the executor never reports
// the same conversion twice, but it may report it zero times on the
// commit-to-count crash window — never twice, possibly once or not at all.
type RecoveryMetrics interface {
	AddReorgOrphaned(chain int64, n int64)
	AddReorgRevived(chain int64, n int64)
	AddReorgEvidenceWait(chain int64, class string)
}

// RecoverySnapshot is one point-in-time gauge input: the durable row plus
// the derived sweep end, depth and reconcile flag. Depth/SweepEnd are nil
// while the ancestor is unconfirmed (unknown, never zero-valued).
type RecoverySnapshot struct {
	Row       *RecoveryRow
	SweepEnd  *int64
	Depth     *int64
	Bound     int64
	Reconcile bool
}

// LoadRecoverySnapshot reads the gauge input in one off-lock pass: the
// active row (nil = no recovery) plus the invalidated sweep end above the
// confirmed ancestor. A read error is reported (the observer keeps its
// last state); nil row with nil error is genuine idle.
func LoadRecoverySnapshot(ctx context.Context, pool *pgxpool.Pool, chainID int64) (RecoverySnapshot, error) {
	row, err := readRecoveryRow(ctx, pool, chainID)
	if err != nil || row == nil {
		return RecoverySnapshot{}, err
	}
	snap := RecoverySnapshot{Row: row, Bound: row.MaxDepth, Reconcile: row.Phase == reorgPhaseReconcileRequired}
	if row.AncestorNumber == nil {
		return snap, nil
	}
	if d, derr := reorgDepth(row.BoundOldNumber, *row.AncestorNumber); derr == nil {
		dup := d
		snap.Depth = &dup
	}
	var end *int64
	if err := pool.QueryRow(ctx, maxInvalidatedHeightSQL, chainID, *row.AncestorNumber).Scan(&end); err != nil {
		return RecoverySnapshot{}, fmt.Errorf("read recovery sweep end: %w", err)
	}
	if end == nil {
		end = row.AncestorNumber // nothing swept yet: sweep == ancestor
	}
	snap.SweepEnd = end
	return snap, nil
}
