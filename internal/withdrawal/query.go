// query.go owns the 007 query core (T011): an ownership-enforced read of one
// withdrawal request annotated with the 006 recovery signal read at one
// snapshot (specs/007-withdrawal-creation/contracts/api.md §3). It is a pure
// read path — no execution state, no chain height, no writes to any table.
//
// The caller identity is authenticated by the transport layer (401 is T014's
// concern); GetWithdrawal receives an already-verified callerID and enforces
// ownership itself. A missing id and another caller's id render identically:
// both return the same *Error{Code: CodeNotFound} (FR-15).
package withdrawal

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/indexer"
)

// RecoverySignal is the 006 recovery annotation carried by a withdrawal view.
//
// State is the 006 state at one read snapshot (contracts/api.md §3, 006 FR-18
// subset): none, recovering, paused_reconcile, released, or unknown. Execution
// is always not_started in 007: a persisted request has not been executed — a
// standing fact (Q8), never an unknown outcome.
type RecoverySignal struct {
	State     string
	Execution string
}

// WithdrawalView is one withdrawal request as returned to its owner by
// GetWithdrawal. Amount is the canonical decimal string (NUMERIC(78,0) read as
// text, never a float). Recovery is the 006 signal described above; it is read
// separately from the request row, so no cross-table atomicity is claimed.
type WithdrawalView struct {
	RequestID string
	CallerID  int64
	ChainID   int64
	Asset     string
	Recipient string
	Amount    string
	Status    string
	CreatedAt time.Time
	Recovery  RecoverySignal
}

// Recovery-state vocabulary exposed by WithdrawalView.Recovery.State
// (contracts/api.md §3, the 006 FR-18 subset 007 presents).
const (
	queryStateNone            = "none"
	queryStateRecovering      = "recovering"
	queryStatePausedReconcile = "paused_reconcile"
	queryStateReleased        = "released"
	queryStateUnknown         = "unknown"
)

// queryExecutionNotStarted is the constant execution fact of every 007 row:
// "not yet executed", never "unknown outcome" (Q8).
const queryExecutionNotStarted = "not_started"

// queryPhaseReconcileRequired is the one 006 reorg_recovery phase that maps to
// paused_reconcile; every other persisted phase maps to recovering.
const queryPhaseReconcileRequired = "reconcile_required"

// querySelectRequestSQL is the ownership-enforced request read: a row comes
// back only when BOTH the public id and the verified caller agree, so a
// foreign id is a miss indistinguishable from a nonexistent one (FR-15).
const querySelectRequestSQL = `
SELECT request_id, caller_id, chain_id, asset, recipient, amount::text, status, created_at
FROM withdrawal_requests
WHERE request_id = $1 AND caller_id = $2`

// GetWithdrawal reads one withdrawal request owned by callerID and annotates
// it with the 006 recovery signal. It is ownership-enforced: a request that
// does not exist OR belongs to another caller returns the identical
// *Error{Code: CodeNotFound} (same code/message/shape; FR-15, no existence
// leakage). callerID MUST already be authenticated by the transport layer.
//
// A genuine storage failure on the request read returns
// *Error{Code: CodeTemporarilyUnavailable} — never a false not_found.
//
// Recovery.State is read at one REPEATABLE READ, read-only snapshot shared by
// the active-row read and the terminal-event read; a failure in either yields
// unknown while the request facts are still returned (a failed read is never
// none or released). Recovery.Execution is always not_started.
func GetWithdrawal(ctx context.Context, pool *pgxpool.Pool, callerID int64, requestID string) (*WithdrawalView, error) {
	view, err := queryReadRequest(ctx, pool, callerID, requestID)
	if err != nil {
		return nil, err
	}
	view.Recovery = queryReadRecoverySignal(ctx, pool, view.ChainID)
	return view, nil
}

// queryReadRequest performs the ownership-enforced request-row read. The row
// carries no recovery or execution data; the caller annotates it separately.
func queryReadRequest(ctx context.Context, pool *pgxpool.Pool, callerID int64, requestID string) (*WithdrawalView, error) {
	var v WithdrawalView
	err := pool.QueryRow(ctx, querySelectRequestSQL, requestID, callerID).
		Scan(&v.RequestID, &v.CallerID, &v.ChainID, &v.Asset, &v.Recipient, &v.Amount, &v.Status, &v.CreatedAt)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return nil, New(CodeNotFound, "withdrawal not found")
	case err != nil:
		return nil, storageUnavailable("read withdrawal request", err)
	}
	return &v, nil
}

// queryReadRecoverySignal reads the two 006 recovery signals inside one
// REPEATABLE READ, read-only transaction so both observe the same snapshot: a
// release-then-re-establish landing between them is impossible. The active-row
// read and the terminal-event read are the exported 006 readers (read-only
// reuse); a failure in either rolls the snapshot back and yields unknown.
func queryReadRecoverySignal(ctx context.Context, pool *pgxpool.Pool, chainID int64) RecoverySignal {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return queryRecoverySignal(queryStateUnknown)
	}
	defer func() { _ = tx.Rollback(ctx) }() // read-only snapshot: rollback is the release

	row, rowErr := indexer.LoadRecoveryState(ctx, tx, chainID)
	var released bool
	var releasedErr error
	if rowErr == nil {
		released, releasedErr = indexer.RecoveryReleased(ctx, tx, chainID)
	}
	phase := ""
	if row != nil {
		phase = row.Phase
	}
	return queryRecoverySignal(combineRecoveryState(row != nil, phase, released, errors.Join(rowErr, releasedErr)))
}

// queryRecoverySignal builds the signal with the standing not_started
// execution fact; only State varies.
func queryRecoverySignal(state string) RecoverySignal {
	return RecoverySignal{State: state, Execution: queryExecutionNotStarted}
}

// combineRecoveryState applies the contracts/api.md §3 precedence to the two
// same-snapshot recovery signals. Either read failing yields unknown (never a
// failed read mapped to none or released). An active row wins over a terminal
// event and maps its phase (reconcile_required → paused_reconcile, any other
// persisted phase → recovering); with no row, a terminal event is released and
// no event is none.
func combineRecoveryState(rowPresent bool, phase string, released bool, readErr error) string {
	if readErr != nil {
		return queryStateUnknown
	}
	if rowPresent {
		if phase == queryPhaseReconcileRequired {
			return queryStatePausedReconcile
		}
		return queryStateRecovering
	}
	if released {
		return queryStateReleased
	}
	return queryStateNone
}
