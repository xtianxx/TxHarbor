// reorgquery.go implements validity-annotated readers (T012): the query
// half of contracts/observability.md §Query validity annotation (R11, FR-18).
// Validity is derived from durable state at read time, never from a cache
// flag — the same "re-read, don't trust memory" discipline as the write
// paths. Enforcement lives in readers, not writers.
package indexer

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// RecoveryState and Validity are the annotated pair every affected-range
// answer carries (contracts/observability.md).
type RecoveryState string

const (
	RecoveryStateNone            RecoveryState = "none"
	RecoveryStateRecovering      RecoveryState = "recovering"
	RecoveryStatePausedReconcile RecoveryState = "paused_reconcile"
	RecoveryStateReleased        RecoveryState = "released"
)

// Validity names what the accompanying data may be used for.
type Validity string

const (
	ValidityUnaffected           Validity = "valid_unaffected"
	ValidityProvisionalReplaying Validity = "provisional_replaying"
	ValidityUnknownPaused        Validity = "unknown_paused"
)

// AnnotateRecoveryHeight derives (recovery_state, validity) for one height:
// no row (and no terminal release on record) → current answers as today; an
// active row → every affected-range answer carries its state; heights at or
// below the ancestor stay valid-unaffected; heights above it are provisional
// while replaying and unknown while reconcile-held. Mixed views are never
// labeled complete; with no trusted data the caller returns explicit
// unavailable/unknown (Q3), never "always available".
func AnnotateRecoveryHeight(row *RecoveryRow, released bool, height int64) (RecoveryState, Validity) {
	if row == nil {
		if released {
			return RecoveryStateReleased, ValidityUnaffected
		}
		return RecoveryStateNone, ValidityUnaffected
	}
	if row.Phase == reorgPhaseReconcileRequired {
		return RecoveryStatePausedReconcile, ValidityUnknownPaused
	}
	if row.AncestorNumber != nil && height <= *row.AncestorNumber {
		return RecoveryStateRecovering, ValidityUnaffected
	}
	return RecoveryStateRecovering, ValidityProvisionalReplaying
}

// RecoveryReleased reports whether the chain's latest terminal recovery
// event exists (the row is gone but history distinguishes "released" from
// "never recovered"). Readers use it only for the annotation above.
func RecoveryReleased(ctx context.Context, q depositQuerier, chainID int64) (bool, error) {
	var one int
	err := q.QueryRow(ctx, readRecoveryReleasedSQL, chainID).Scan(&one)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, pgx.ErrNoRows):
		return false, nil
	default:
		return false, fmt.Errorf("read recovery release record: %w", err)
	}
}

// LoadRecoveryState is the startup/loop/health reader (T017): it observes
// the recovery row the same way serve loops read pause rows today — one
// point read, nil-able, no side effects.
func LoadRecoveryState(ctx context.Context, q depositQuerier, chainID int64) (*RecoveryRow, error) {
	return readRecoveryRow(ctx, q, chainID)
}

const (
	// readRecoveryReleasedSQL finds the latest terminal release for a chain
	// (auto_completed or released survive the row DELETE in the events log).
	readRecoveryReleasedSQL = `
SELECT 1 FROM reorg_recovery_events
WHERE chain_id = $1 AND event IN ('auto_completed', 'released')
ORDER BY at DESC LIMIT 1`
)
