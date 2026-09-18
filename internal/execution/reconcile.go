package execution

import (
	"context"
	"errors"
	"fmt"
	"strings"

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

	// Automatic display consumption: a successful authority read refreshes the
	// lifecycle stream (write-only; no decision reads it back).
	if _, err := applyLifecycleProjection(ctx, r.Pool, intent.RequestID, facts.CurrentAttemptID, facts.RevisionVersion); err != nil {
		return ReconcileResult{}, err
	}

	if frozen, freezeClass, err := r.frozen(ctx, intentID, facts); err != nil {
		return ReconcileResult{}, err
	} else if frozen {
		if err := r.recordFreeze(ctx, intent, facts, freezeClass); err != nil {
			return ReconcileResult{}, err
		}
		return r.reconcileFrozen(ctx, intent, open, hasOpen, facts, ownerID, leaseVersion, freezeClass)
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

// FreezeLockLoss is the G-010-2 class (c) unperceived lock-loss freeze. 011
// consumes the class as input and refuses sends while frozen; it claims no
// detection guarantee and no "window is tiny/rare" property.
const FreezeLockLoss = "lock_loss"

// FreezeClass reports the freeze class the unknown-recovery condition carries.
// The exact 010-side token is joint wiring (010:T039); an explicit "freeze:"
// prefix and the lock-loss condition names are both recognised.
func FreezeClass(facts LifecycleFacts) (string, bool) {
	if facts.Unknown == nil {
		return "", false
	}
	cond := strings.ToLower(facts.Unknown.RecoveryCondition)
	if cond == "" {
		return "", false
	}
	if i := strings.Index(cond, "freeze:"); i >= 0 {
		if rest := strings.TrimSpace(cond[i+len("freeze:"):]); rest != "" {
			return rest, true
		}
		return FreezeLockLoss, true
	}
	if strings.Contains(cond, "lock_loss") || strings.Contains(cond, "lock-loss") ||
		strings.Contains(cond, "lockloss") || strings.Contains(cond, "freeze") {
		return FreezeLockLoss, true
	}
	return "", false
}

// frozen reports the freeze state from the authority facts or an already
// recorded marker. A recorded marker is never cleared here: only the 010-side
// controlled manual release (010:T040) can end the freeze.
func (r *Reconciler) frozen(ctx context.Context, intentID string, facts LifecycleFacts) (bool, string, error) {
	if class, ok := FreezeClass(facts); ok {
		return true, class, nil
	}
	marked, class, err := IsFrozen(ctx, r.Pool, intentID)
	if err != nil {
		return false, "", err
	}
	return marked, class, nil
}

// recordFreeze appends the freeze class as durable evidence exactly once.
func (r *Reconciler) recordFreeze(ctx context.Context, intent Intent, facts LifecycleFacts, class string) error {
	already, _, err := IsFrozen(ctx, r.Pool, intent.IntentID)
	if err != nil {
		return err
	}
	if already {
		return nil
	}
	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin freeze evidence: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	detail := "freeze:" + class
	if facts.Unknown != nil {
		detail += " condition=" + facts.Unknown.RecoveryCondition
	}
	if err := AppendEvent(ctx, tx, Event{
		IntentID: intent.IntentID, Kind: EventReconcileObserved,
		AttemptID: facts.CurrentAttemptID, RevisionVersion: facts.RevisionVersion, Detail: detail,
	}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// reconcileFrozen observes a frozen intent without resolving it: the open step
// stays unknown/reconciling and no terminal or executing transition is applied.
func (r *Reconciler) reconcileFrozen(ctx context.Context, intent Intent, open Step, hasOpen bool, facts LifecycleFacts, ownerID string, leaseVersion int64, class string) (ReconcileResult, error) {
	basis := "frozen:" + class + "; reconcile observes only"
	if !hasOpen {
		return ReconcileResult{IntentID: intent.IntentID, Reconciling: true, Retryable: true, Basis: basis}, nil
	}
	if err := r.convergeOpenStep(ctx, intent, open, StepUnknown, OutcomeReconcileRequired,
		facts.CurrentAttemptID, "", facts.RevisionVersion, "frozen "+class); err != nil {
		return ReconcileResult{}, err
	}
	if err := r.ensureReconciling(ctx, intent, ownerID, leaseVersion); err != nil {
		return ReconcileResult{}, err
	}
	return ReconcileResult{
		IntentID: intent.IntentID, Converged: true, FinalStepState: StepUnknown,
		OutcomeClass: OutcomeReconcileRequired, Reconciling: true, Retryable: true, Basis: basis,
	}, nil
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

// The projection SQL and its version-guarded applies live in projection.go
// (the single writer of the display-only projection).
