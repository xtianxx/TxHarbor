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
// A persistently failing scope is retried under a bounded per-sender backoff
// and counted through the optional low-cardinality ReconcileSink, so one bad
// scope neither tight-loops nor starves the healthy ones.
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
// The caller additionally refuses to call this for a divergent observation,
// whose contradictory counts are likewise not a value.
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
//
// Divergence is excluded exactly like unavailability: a contradictory (L > P)
// or regressing view is NOT a value (classify.go checks it before every
// candidate branch and holds + refuses), so it must never drive any automatic
// edge — in particular it must never consume a binding from a contradictory
// view (observation.md §2.1: a stale/contradictory observation MUST NEVER
// reassign a binding). The classification still records its anomaly observation
// and establishes its chain_view_divergence hold in the caller.
func applyObservationTransitionTx(ctx context.Context, tx txQuerier, b Binding, obs Observation) (bool, error) {
	if obs.Classification == ClassificationUnavailable || obs.Classification == ClassificationDivergence {
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
//
// A scope that keeps failing is retried under a bounded exponential backoff
// (never a tight per-tick retry), and healthy scopes are never delayed by it:
// a backed-off scope is skipped in the pass, not awaited.
type ReconcileLoop struct {
	chainID  int64
	interval time.Duration
	// listScopes and reconcile are the unit seams; NewReconcileLoop wires the
	// durable production implementations.
	listScopes func(ctx context.Context) ([]string, error)
	reconcile  func(ctx context.Context, sender string) error
	logger     *slog.Logger
	// sink is the optional low-cardinality failure counter (see ReconcileSink);
	// nil skips counting.
	sink ReconcileSink
	// failures holds each failing scope's consecutive-failure streak and the
	// earliest time it may be retried. Only Run/tick touch it and they run on
	// one goroutine, so no lock is needed. The map is pruned to the live scope
	// set every pass, so it is bounded by the registry, never by sender churn.
	failures map[string]*reconcileFailure
}

// reconcileFailure is one scope's bounded-backoff state.
type reconcileFailure struct {
	streak      int
	nextAttempt time.Time
}

// reconcileBackoffMaxMultiplier bounds the per-sender retry backoff: the
// streak-th consecutive failure defers the scope by interval<<(streak-1),
// capped at interval*reconcileBackoffMaxMultiplier. The cap makes retries
// bounded (never an unbounded doubling) and keeps a permanently failing scope
// from monopolizing passes.
const reconcileBackoffMaxMultiplier = 8

// ReconcileSink is the optional reconcile-loop failure seam: one label-free
// count per failed scope tick. Counting failures through the observations
// counter would mislabel them as persisted rows, so the seam is its own
// method; the *metrics.Metrics registry satisfies it structurally, so this
// package never imports the registry and the app layer stays the only wiring
// point.
type ReconcileSink interface {
	ObserveNonceReconcileFailure()
}

// NewReconcileLoop wires the serve pool, the raw-RPC observer and the existing
// IndexPollInterval cadence (no new timing knob). It opens one pooled
// transaction per scope list and one per scope tick, never a long-lived session.
// An optional ReconcileSink counts failed scope ticks.
func NewReconcileLoop(pool *pgxpool.Pool, observer *Observer, chainID int64, interval time.Duration, logger *slog.Logger, sinks ...ReconcileSink) *ReconcileLoop {
	l := &ReconcileLoop{
		chainID:  chainID,
		interval: interval,
		logger:   logger,
		failures: make(map[string]*reconcileFailure),
	}
	if len(sinks) > 0 {
		l.sink = sinks[0]
	}
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
	l.failures = make(map[string]*reconcileFailure)
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
// is fatal, and neither touches service readiness. A scope inside its failure
// backoff window is skipped without blocking the pass, so healthy scopes keep
// converging at the configured cadence.
func (l *ReconcileLoop) tick(ctx context.Context) {
	scopes, err := l.listScopes(ctx)
	if err != nil {
		if ctx.Err() == nil {
			l.log().Warn("nonce reconcile scope list failed", "error", logx.Redact(err.Error()))
		}
		return
	}
	now := time.Now()
	for _, sender := range scopes {
		if ctx.Err() != nil {
			return
		}
		if f := l.failures[sender]; f != nil && now.Before(f.nextAttempt) {
			continue
		}
		if err := l.reconcile(ctx, sender); err != nil {
			if ctx.Err() != nil {
				return
			}
			streak := l.recordFailure(sender, now)
			l.log().Warn("nonce reconcile scope failed",
				"sender", logx.Redact(sender), "error", logx.Redact(err.Error()), "streak", streak)
			continue
		}
		delete(l.failures, sender)
	}
	l.pruneFailures(scopes)
}

// recordFailure advances one scope's consecutive-failure streak, defers its
// next attempt by the bounded backoff, and counts the failure through the
// fixed-vocabulary sink (never the sender). It returns the new streak.
func (l *ReconcileLoop) recordFailure(sender string, now time.Time) int {
	if l.failures == nil {
		l.failures = make(map[string]*reconcileFailure)
	}
	f := l.failures[sender]
	if f == nil {
		f = &reconcileFailure{}
		l.failures[sender] = f
	}
	f.streak++
	f.nextAttempt = now.Add(reconcileBackoff(l.interval, f.streak))
	if l.sink != nil {
		l.sink.ObserveNonceReconcileFailure()
	}
	return f.streak
}

// reconcileBackoff returns the deferral for the streak-th consecutive failure:
// interval<<(streak-1), capped at interval*reconcileBackoffMaxMultiplier. It is
// finite and monotone, so a persistently failing scope retries at most every
// cap and never on every tick.
func reconcileBackoff(interval time.Duration, streak int) time.Duration {
	if interval <= 0 {
		return 0
	}
	max := interval * reconcileBackoffMaxMultiplier
	if max <= 0 { // overflow guard: fall back to the un-amplified interval
		max = interval
	}
	delay := interval
	for i := 1; i < streak && delay < max; i++ {
		delay *= 2
	}
	if delay > max || delay <= 0 {
		delay = max
	}
	return delay
}

// pruneFailures drops backoff state for senders no longer in the known scope
// set: the map stays bounded by the live registry instead of growing with
// sender churn. A re-appearing sender starts a fresh streak.
func (l *ReconcileLoop) pruneFailures(scopes []string) {
	if len(l.failures) == 0 {
		return
	}
	live := make(map[string]struct{}, len(scopes))
	for _, s := range scopes {
		live[s] = struct{}{}
	}
	for sender := range l.failures {
		if _, ok := live[sender]; !ok {
			delete(l.failures, sender)
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
	// Record the fresh last_* facts. Only a trusted view advances the
	// waterline: an unavailable read carries no counts, and a contradictory
	// (divergent) view is not a value — persisting it would let one anomaly
	// move the PendingPrev baseline and mask a later real regression. The
	// anomaly observation and its hold are persisted above regardless, so
	// skipping the waterline never drops evidence. Both durable writers
	// (this tick and the admission in allocate.go) apply the same exclusion.
	// last_* are "last observed" facts, deliberately NOT monotonic — a chain
	// reorg may legitimately lower them; monotonicity is a property of
	// reconciled_floor alone (hold.go). Floor movement goes through
	// advanceScopeFloorTx only.
	if obs.LatestCount != nil && obs.PendingCount != nil &&
		obs.Classification != ClassificationUnavailable &&
		obs.Classification != ClassificationDivergence {
		if err := updateScopeFrontierTx(ctx, tx, chainID, sender, obs.LatestCount, obs.PendingCount, observationID); err != nil {
			return err
		}
	}
	return nil
}
