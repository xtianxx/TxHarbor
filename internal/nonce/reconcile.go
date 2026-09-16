// reconcile.go owns startup rebuild verification and floor reconciliation:
// re-deriving the durable frontier from nonce_bindings, advancing
// reconciled_floor only on evidence, and gating allocation until the rebuild
// passes (FR-12/FR-13, R7). It also owns the T-observe binding transition
// applier (R6, FR-07/09/20) and the per-known-scope reconcile observer loop
// (T029, R4): each tick samples every known (chain_id, sender) scope outside
// any DB transaction, then submits one T-observe transaction per scope under
// the shared coordination lock (coord.go lockChain).
//
// The loop never affects service readiness: a scope-list, RPC or transaction
// failure is logged and skipped, never fatal, and nothing here touches the
// health aggregate — mirroring 006's recovery observer (contracts/observation.md §5).
package nonce

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/logx"
)

// observeTransitionTarget is the T-observe decision for one binding under one
// observation: the single automatic edge R6 allows, or "" for no transition.
// It is pure over the durable row and the observed latest(L)/pending(P):
//
//	nonce < L        -> consumed (mined, ours or external)
//	L <= nonce < P   -> in_flight (pending, outcome unknown)  [allocated only]
//	otherwise        -> no transition
//
// A terminal binding (consumed/released) has no path out, and a nil count
// (an unavailable read) is never treated as a value. released is structurally
// unreachable here: reconcile never emits it (operator-only, admin.go).
func observeTransitionTarget(b Binding, latest, pending *big.Int) string {
	if b.Nonce == nil || latest == nil || pending == nil || IsTerminal(b.State) {
		return ""
	}
	switch {
	case b.Nonce.Cmp(latest) < 0:
		return StateConsumed
	case b.State == StateAllocated && b.Nonce.Cmp(pending) < 0:
		return StateInFlight
	default:
		return ""
	}
}

// applyObservationTransitionTx applies at most one automatic transition for
// one binding under one classified observation, appending at most one event.
// It is a pure carrier over durable rows: the caller owns the tx/lock
// discipline (T029) and passes the persisted observation as data.
//
// Idempotency: the event insert converges on
// nonce_binding_events_binding_to_uniq, and the state advance is guarded by
// the expected current state, so a repeat observation (or a crash between the
// event write and the state advance) appends zero further history. An
// unavailable observation and a terminal binding are left untouched. The
// return reports whether any row was written.
func applyObservationTransitionTx(ctx context.Context, tx txQuerier, b Binding, obs Observation) (bool, error) {
	if obs.Classification == ClassificationUnavailable {
		return false, nil
	}
	to := observeTransitionTarget(b, obs.LatestCount, obs.PendingCount)
	if to == "" || !CanAutoTransition(b.State, to) {
		return false, nil
	}
	from := b.State
	created, err := insertBindingEventTx(ctx, tx, BindingEvent{
		BindingID:     b.BindingID,
		FromState:     &from,
		ToState:       to,
		ObservationID: obs.ObservationID,
		Detail:        "reconcile",
	})
	if err != nil {
		return false, err
	}
	moved, err := advanceBindingStateTx(ctx, tx, b.BindingID, to, "", from)
	if err != nil {
		return false, err
	}
	return created || moved, nil
}

// listKnownScopesSQL lists the chain's known scopes: every registered sender
// is a scope (nonce_wallet_registry is the authority for scope existence, and
// its FK guarantees no binding or scope row can exist without a registry row).
// The disabled state is deliberately not filtered: a disabled sender's
// existing bindings still converge (FR-07/FR-09).
const listKnownScopesSQL = `
SELECT sender FROM nonce_wallet_registry
WHERE chain_id = $1 ORDER BY sender`

// ReconcileLoop is the periodic reconcile observer (T029). Each tick lists the
// known scopes and submits one T-observe transaction per scope; the chain view
// is sampled outside the transaction (R4) and the write path reuses the
// T024/T026 applier under the shared coordination lock.
type ReconcileLoop struct {
	chainID  int64
	interval time.Duration
	// listScopes and reconcile are the unit seams; NewReconcileLoop wires the
	// durable production implementations.
	listScopes func(ctx context.Context) ([]string, error)
	reconcile  func(ctx context.Context, sender string) error
	logger     *slog.Logger
}

// NewReconcileLoop wires the serve pool, the raw-RPC observer and the existing
// IndexPollInterval cadence (no new timing knob). It opens one pooled
// transaction per scope list and one per scope tick, never a long-lived session.
func NewReconcileLoop(pool *pgxpool.Pool, observer *Observer, chainID int64, interval time.Duration, logger *slog.Logger) *ReconcileLoop {
	l := &ReconcileLoop{chainID: chainID, interval: interval, logger: logger}
	begin := func(ctx context.Context) (allocTx, error) { return pool.Begin(ctx) }
	l.listScopes = func(ctx context.Context) ([]string, error) { return listKnownScopes(ctx, begin, chainID) }
	l.reconcile = func(ctx context.Context, sender string) error {
		return reconcileScope(ctx, begin, observer, chainID, sender)
	}
	return l
}

// Run drives the loop until ctx is cancelled, then returns. The first pass
// runs immediately (the metric observers sample at startup too), then one pass
// per tick. The loop holds no state between passes: every decision re-reads the
// durable rows, so a restart loses nothing (R5).
func (l *ReconcileLoop) Run(ctx context.Context) {
	if l.interval <= 0 {
		l.log().Warn("nonce reconcile loop disabled: non-positive interval", "interval", l.interval)
		return
	}
	ticker := time.NewTicker(l.interval)
	defer ticker.Stop()
	l.tick(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			l.tick(ctx)
		}
	}
}

func (l *ReconcileLoop) log() *slog.Logger {
	if l.logger == nil {
		return slog.Default()
	}
	return l.logger
}

// tick runs one pass: list the known scopes, then reconcile each in turn. A
// list failure skips the pass and a per-scope failure skips that scope; neither
// is fatal, and neither touches service readiness.
func (l *ReconcileLoop) tick(ctx context.Context) {
	scopes, err := l.listScopes(ctx)
	if err != nil {
		if ctx.Err() == nil {
			l.log().Warn("nonce reconcile scope list failed", "error", logx.Redact(err.Error()))
		}
		return
	}
	for _, sender := range scopes {
		if ctx.Err() != nil {
			return
		}
		if err := l.reconcile(ctx, sender); err != nil {
			if ctx.Err() != nil {
				return
			}
			l.log().Warn("nonce reconcile scope failed",
				"sender", logx.Redact(sender), "error", logx.Redact(err.Error()))
		}
	}
}

// listKnownScopes reads the chain's registered scopes over its own read-only
// transaction: a plain read of the stable registry table, so no coordination
// lock is taken and nothing is written.
func listKnownScopes(ctx context.Context, begin func(context.Context) (allocTx, error), chainID int64) ([]string, error) {
	tx, err := begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("open scope list transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rowRows, err := tx.Query(ctx, listKnownScopesSQL, chainID)
	if err != nil {
		return nil, fmt.Errorf("list known scopes: %w", err)
	}
	defer rowRows.Close()
	var senders []string
	for rowRows.Next() {
		var sender string
		if err := rowRows.Scan(&sender); err != nil {
			return nil, fmt.Errorf("scan known scope: %w", err)
		}
		senders = append(senders, sender)
	}
	if err := rowRows.Err(); err != nil {
		return nil, fmt.Errorf("list known scopes: %w", err)
	}
	return senders, nil
}

// reconcileScope performs one T-observe tick for one scope: the chain view is
// sampled OUTSIDE the transaction, then one transaction takes the shared
// coordination lock and re-reads the durable facts under the scope row lock.
// Failures are returned to the caller (which logs and moves on); the loop never
// stops on them.
func reconcileScope(ctx context.Context, begin func(context.Context) (allocTx, error), observer *Observer, chainID int64, sender string) error {
	obs := observer.Observe(ctx, chainID, sender, ObservationKindReconcile)
	tx, err := begin(ctx)
	if err != nil {
		return fmt.Errorf("open reconcile transaction: %w", err)
	}
	if err := reconcileScopeInTx(ctx, tx, chainID, sender, obs); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit reconcile transaction: %w", err)
	}
	return nil
}

// reconcileScopeInTx runs the data-model T-observe steps inside the caller's
// transaction: lock, re-read the scope's bindings and frontier, classify,
// persist the observation, apply the automatic transitions, establish a hold
// when the matrix demands one, and record the fresh last_* facts. It writes no
// floor movement: only an operator release advances reconciled_floor.
func reconcileScopeInTx(ctx context.Context, tx txQuerier, chainID int64, sender string, obs Observation) error {
	if err := lockChain(ctx, tx, chainID); err != nil {
		return err
	}
	if err := ensureScopeRowTx(ctx, tx, chainID, sender); err != nil {
		return err
	}
	if err := lockScopeRowTx(ctx, tx, chainID, sender); err != nil {
		return err
	}
	bindings, err := readBindingsByScopeTx(ctx, tx, chainID, sender)
	if err != nil {
		return err
	}
	scope, err := readScopeStateTx(ctx, tx, chainID, sender)
	if err != nil {
		return err
	}

	decision := Classify(ClassificationInput{
		ReadFailed:      obs.Classification == ClassificationUnavailable,
		Latest:          obs.LatestCount,
		Pending:         obs.PendingCount,
		PendingPrev:     scopeLastPending(scope),
		MaxBindingNonce: MaxBoundNonce(bindings),
		HasBindings:     len(bindings) > 0,
		ReconciledFloor: scopeFloor(scope),
	})
	obs.Classification = decision.Classification
	observationID, err := insertObservationTx(ctx, tx, obs)
	if err != nil {
		return err
	}
	obs.ObservationID = observationID

	for i := range bindings {
		if _, err := applyObservationTransitionTx(ctx, tx, bindings[i], obs); err != nil {
			return fmt.Errorf("apply reconcile transition for %s: %w", bindings[i].BindingID, err)
		}
	}
	if decision.HoldCause != "" {
		if _, err := establishHoldTx(ctx, tx, HoldEstablishment{
			ChainID:       chainID,
			Sender:        sender,
			Cause:         decision.HoldCause,
			ObservationID: observationID,
			Detail:        decision.Classification,
		}); err != nil {
			return err
		}
	}
	if obs.LatestCount != nil && obs.PendingCount != nil {
		if err := updateScopeFrontierTx(ctx, tx, chainID, sender, obs.LatestCount, obs.PendingCount, observationID); err != nil {
			return err
		}
	}
	return nil
}
