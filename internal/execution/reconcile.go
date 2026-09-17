package execution

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ReconcileResult is one T-reconcile outcome. A reconcile never converts an
// unknown business effect to failure/not-paid; it either converges a step from
// positive authority evidence or leaves it pending.
type ReconcileResult struct {
	IntentID       string
	Converged      bool
	FinalStepState string
	OutcomeClass   string
	Reconciling    bool
	Retryable      bool
	Basis          string
}

// Reconciler converges open steps from 010's authority facts. It takes no
// gate-table locks and makes no send decision (fact-only path, gates.md §5).
type Reconciler struct {
	Pool   *pgxpool.Pool
	Reader LifecycleReader
	// MaxReadRetries bounds reader-unavailable retries within one call.
	MaxReadRetries int
}

func (r *Reconciler) maxReadRetries() int {
	if r.MaxReadRetries > 0 {
		return r.MaxReadRetries
	}
	return 2
}

// ReconcileIntent reconciles the intent's open step. ownerID/leaseVersion, when
// non-empty, let the caller's held qualification advance the progress
// watermark; a fact-only caller passes "".
func (r *Reconciler) ReconcileIntent(ctx context.Context, intentID, ownerID string, leaseVersion int64) (ReconcileResult, error) {
	intent, found, err := ReadIntent(ctx, r.Pool, intentID)
	if err != nil {
		return ReconcileResult{}, err
	}
	if !found {
		return ReconcileResult{IntentID: intentID, Basis: "intent absent"}, nil
	}
	open, hasOpen, err := ReadOpenStep(ctx, r.Pool, intentID)
	if err != nil {
		return ReconcileResult{}, err
	}

	facts, readErr := r.readFacts(ctx, intentID)
	if readErr != nil {
		// A failed read marks the projection possibly-stale and never rewrites
		// business state; the caller retries boundedly.
		_ = markProjectionStale(ctx, r.Pool, intent.RequestID)
		return ReconcileResult{IntentID: intentID, Retryable: true, Basis: "lifecycle reader unavailable"}, nil
	}

	if !hasOpen {
		// No open step: still consume a definitive sent/confirmed fact to
		// complete the intent, and reconcile a positive no-attempt retry.
		return r.reconcileWithoutOpenStep(ctx, intent, facts, ownerID, leaseVersion)
	}

	if facts.Unknown != nil {
		if err := r.convergeOpenStep(ctx, intent, open, StepUnknown, OutcomeReconcileRequired,
			facts.Unknown.AttemptID, facts.Unknown.TxHash, facts.RevisionVersion,
			"still_unknown condition="+facts.Unknown.RecoveryCondition); err != nil {
			return ReconcileResult{}, err
		}
		if err := r.ensureReconciling(ctx, intent, ownerID, leaseVersion); err != nil {
			return ReconcileResult{}, err
		}
		return ReconcileResult{
			IntentID: intentID, Converged: true, FinalStepState: StepUnknown,
			OutcomeClass: OutcomeReconcileRequired, Reconciling: true,
			Basis: "unknown persists; never failure",
		}, nil
	}

	if len(facts.Attempts) == 0 && facts.CurrentAttemptID == "" {
		// Positive "no attempt persisted" evidence: the open step can be
		// refused and a fresh step issued under current gates. Never inferred
		// from an absent read.
		if err := r.convergeOpenStep(ctx, intent, open, StepRefused, "",
			"", "", facts.RevisionVersion, "no_attempt_persisted basis="+facts.Basis); err != nil {
			return ReconcileResult{}, err
		}
		if err := r.toExecuting(ctx, intent, ownerID, leaseVersion); err != nil {
			return ReconcileResult{}, err
		}
		return ReconcileResult{
			IntentID: intentID, Converged: true, FinalStepState: StepRefused,
			Basis: "positive no-attempt evidence; re-issue under current gates",
		}, nil
	}

	if facts.CurrentAttemptID != "" && isConfirmedSent(facts) {
		if err := r.convergeOpenStep(ctx, intent, open, StepConverged, OutcomeSent,
			facts.CurrentAttemptID, "", facts.RevisionVersion, "authority_confirmed_sent"); err != nil {
			return ReconcileResult{}, err
		}
		if err := r.completeIntent(ctx, intent, ownerID, leaseVersion); err != nil {
			return ReconcileResult{}, err
		}
		return ReconcileResult{
			IntentID: intentID, Converged: true, FinalStepState: StepConverged,
			OutcomeClass: OutcomeSent, Basis: "authority confirmed sent",
		}, nil
	}

	// Inconclusive facts: record the observation and keep reconciling. This is
	// never a failure and never a silent "not sent".
	if err := r.recordObservation(ctx, intent, open, facts); err != nil {
		return ReconcileResult{}, err
	}
	return ReconcileResult{IntentID: intentID, Reconciling: true, Retryable: true, Basis: "observation recorded"}, nil
}

func (r *Reconciler) readFacts(ctx context.Context, intentID string) (LifecycleFacts, error) {
	if r.Reader == nil {
		return LifecycleFacts{}, ErrAdvanceUnavailable
	}
	var lastErr error
	for i := 0; i <= r.maxReadRetries(); i++ {
		facts, err := r.Reader.Read(ctx, intentID)
		if err == nil {
			return facts, nil
		}
		lastErr = err
	}
	return LifecycleFacts{}, lastErr
}

// isConfirmedSent reports a confirmed on-chain success for the current attempt.
func isConfirmedSent(facts LifecycleFacts) bool {
	for _, a := range facts.Attempts {
		if a.AttemptID != facts.CurrentAttemptID {
			continue
		}
		switch a.State {
		case "sent", "confirmed", "mined", "included":
			return true
		}
	}
	return false
}

func (r *Reconciler) reconcileWithoutOpenStep(ctx context.Context, intent Intent, facts LifecycleFacts, ownerID string, version int64) (ReconcileResult, error) {
	if facts.CurrentAttemptID != "" && isConfirmedSent(facts) {
		if err := r.completeIntent(ctx, intent, ownerID, version); err != nil {
			return ReconcileResult{}, err
		}
		return ReconcileResult{IntentID: intent.IntentID, Basis: "authority confirmed sent (no open step)"}, nil
	}
	if facts.Unknown == nil && len(facts.Attempts) == 0 && intent.State == IntentReconciling {
		if err := r.toExecuting(ctx, intent, ownerID, version); err != nil {
			return ReconcileResult{}, err
		}
		return ReconcileResult{IntentID: intent.IntentID, Basis: "no attempt persisted; ready to re-issue"}, nil
	}
	return ReconcileResult{IntentID: intent.IntentID}, nil
}

const reconcileConvergeSQL = `UPDATE execution_steps
  SET state = $2, attempt_id = NULLIF($3, ''), tx_hash = NULLIF($4, ''),
      outcome_class = NULLIF($5, ''), revision_version = NULLIF($6, 0),
      evidence = $7, updated_at = now()
  WHERE step_id = $1 AND state = 'issued'`

// convergeOpenStep converges an issued step from authority facts. It is
// exact-state (issued -> terminal) and not owner-fenced: the reconciler may be
// a different, legitimate holder than the issuing one (persistence.md §2.3).
func (r *Reconciler) convergeOpenStep(ctx context.Context, intent Intent, step Step, state, class, attemptID, txHash string, revisionVersion int64, evidence string) error {
	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin reconcile converge: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	tag, err := tx.Exec(ctx, reconcileConvergeSQL, step.StepID, state, attemptID, txHash, class, revisionVersion, evidence)
	if err != nil {
		return fmt.Errorf("reconcile converge: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return nil // already terminal: idempotent
	}
	kind := EventStepConverged
	switch state {
	case StepUnknown:
		kind = EventStepUnknown
	case StepRefused:
		kind = EventStepRefused
	}
	if err := AppendEvent(ctx, tx, Event{
		IntentID: intent.IntentID, Kind: kind, StepID: step.StepID, AttemptID: attemptID,
		RevisionVersion: revisionVersion, Detail: "reconcile " + evidence,
	}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (r *Reconciler) recordObservation(ctx context.Context, intent Intent, step Step, facts LifecycleFacts) error {
	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin reconcile observation: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := AppendEvent(ctx, tx, Event{
		IntentID: intent.IntentID, Kind: EventReconcileObserved, StepID: step.StepID,
		AttemptID: facts.CurrentAttemptID, RevisionVersion: facts.RevisionVersion,
		Detail: "inconclusive basis=" + facts.Basis,
	}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ensureReconciling puts the intent into reconciling if it is executing (fact
// transition, M2) and records the observation.
func (r *Reconciler) ensureReconciling(ctx context.Context, intent Intent, ownerID string, version int64) error {
	if intent.State != IntentExecuting && intent.State != IntentClaimed {
		return nil
	}
	return r.factTransition(ctx, intent, IntentReconciling, version)
}

// toExecuting moves reconciling -> executing only on positive evidence. The
// edge is send-enabling: only a caller holding a verified qualification may
// apply it, so the fact-only startup catch-up (ownerID == "") leaves the intent
// reconciling for the next claimant to advance.
func (r *Reconciler) toExecuting(ctx context.Context, intent Intent, ownerID string, version int64) error {
	if intent.State != IntentReconciling || ownerID == "" {
		return nil
	}
	return r.factTransition(ctx, intent, IntentExecuting, version)
}

// completeIntent applies the authority-driven ->completed fact transition.
func (r *Reconciler) completeIntent(ctx context.Context, intent Intent, ownerID string, version int64) error {
	if intent.State == IntentCompleted {
		return nil
	}
	if ClassifyTransition(intent.State, IntentCompleted) != TransitionFact {
		return nil
	}
	return r.factTransition(ctx, intent, IntentCompleted, version)
}

func (r *Reconciler) factTransition(ctx context.Context, intent Intent, to string, version int64) error {
	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin reconcile transition: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := TransitionIntent(ctx, tx, intent.IntentID, intent.State, intent.StateVersion, to, version); err != nil {
		if errors.Is(err, ErrTransitionRefused) || errors.Is(err, ErrIllegalTransition) {
			return nil
		}
		return err
	}
	_, _ = applyStateProjection(ctx, tx, intent.RequestID, to, intent.StateVersion+1)
	return tx.Commit(ctx)
}

// ReconcileAllOpenSteps reconciles every intent that has an open step; the
// worker's startup catch-up runs this before the loop.
func (r *Reconciler) ReconcileAllOpenSteps(ctx context.Context) error {
	rows, err := r.Pool.Query(ctx, `SELECT DISTINCT intent_id FROM execution_steps WHERE state = 'issued'`)
	if err != nil {
		return err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, id := range ids {
		if _, err := r.ReconcileIntent(ctx, id, "", 0); err != nil {
			return err
		}
	}
	return nil
}

// ProjectionCatchUp011 applies the 011 state stream to every projection row in
// version order (the display plane; never read by decisions).
func (r *Reconciler) ProjectionCatchUp011(ctx context.Context) error {
	rows, err := r.Pool.Query(ctx, `SELECT request_id, state, state_version FROM payment_intents ORDER BY state_version ASC`)
	if err != nil {
		return err
	}
	defer rows.Close()
	type row struct {
		requestID string
		state     string
		version   int64
	}
	var all []row
	for rows.Next() {
		var rr row
		if err := rows.Scan(&rr.requestID, &rr.state, &rr.version); err != nil {
			return err
		}
		all = append(all, rr)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, rr := range all {
		if _, err := applyStateProjection(ctx, r.Pool, rr.requestID, rr.state, rr.version); err != nil {
			return err
		}
	}
	return nil
}

// ApplyStateProjection exposes the version-guarded 011 state apply to the
// worker cycle.
func (r *Reconciler) ApplyStateProjection(ctx context.Context, requestID, state string, version int64) error {
	_, err := applyStateProjection(ctx, r.Pool, requestID, state, version)
	return err
}

// applyStateProjection applies the 011-side state stream only when the incoming
// version is greater; an older arrival affects zero rows and never overwrites a
// newer one (data-model Table 5).
func applyStateProjection(ctx context.Context, q Queryer, requestID, state string, version int64) (bool, error) {
	tag, err := q.Exec(ctx, `UPDATE request_status_projection
		SET execution_state = $2, state_version = $3, updated_at = now()
		WHERE request_id = $1 AND state_version < $3`, requestID, state, version)
	if err != nil {
		return false, fmt.Errorf("apply state projection: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// markProjectionStale records a possibly-stale freshness without rewriting any
// stored version or converting a known result.
func markProjectionStale(ctx context.Context, q Queryer, requestID string) error {
	if _, err := q.Exec(ctx, `UPDATE request_status_projection
		SET freshness = 'possibly_stale', stale_since = coalesce(stale_since, now()), updated_at = now()
		WHERE request_id = $1 AND freshness = 'confirmed'`, requestID); err != nil {
		return fmt.Errorf("mark projection stale: %w", err)
	}
	return nil
}
