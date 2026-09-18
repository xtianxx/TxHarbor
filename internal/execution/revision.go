package execution

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5/pgxpool"
)

// T-revision (contracts/lifecycle.md §6, data-model T-revision): consume 010's
// authority revision rows in version order. A revision is an authority-driven
// fact transition (`completed→revised`, `failed→revised`) guarded only by the
// (state, state_version) CAS + version monotonicity: it consumes no
// authorization and needs no claim (M2 — chain-fact observation, state/projection
// revision and reconcile bookkeeping are not new asset execution). A revision
// never rebuilds an intent and never creates a compensation payment; any
// subsequent send re-runs the full current gate set, because a fact transition
// grants no send authority.

// RevisionFact is one 010 revision-chain observation: identity, source version
// and the fact target. It carries references only, never a copy of 010's facts.
type RevisionFact struct {
	IntentID        string
	RevisionVersion int64
	AttemptID       string
	TargetState     string
	Basis           string
}

// RevisionResult records one revision consumption.
type RevisionResult struct {
	IntentID        string
	Applied         bool
	FromState       string
	ToState         string
	RevisionVersion int64
	Basis           string
}

// RevisionConsumer consumes authority revision rows. M2 is the fence: no
// balance ledger, no relief from normal authentication or operator permissions,
// no new intent and no compensation payment.
type RevisionConsumer struct {
	Pool *pgxpool.Pool
}

const appliedRevisionSQL = `SELECT COALESCE(MAX(revision_version), 0) FROM execution_events
  WHERE intent_id = $1 AND kind = 'revision_applied'`

// AppliedRevision returns the last consumed authority revision version for the
// intent (0 when none). The watermark is event-sourced, so this decision path
// never reads the display projection.
func AppliedRevision(ctx context.Context, q Queryer, intentID string) (int64, error) {
	var v int64
	if err := q.QueryRow(ctx, appliedRevisionSQL, intentID).Scan(&v); err != nil {
		return 0, fmt.Errorf("read applied revision: %w", err)
	}
	return v, nil
}

// Apply consumes one authority revision. An older or equal revision affects
// zero rows and is recorded as evidence only; an edge outside the fact class is
// left untouched rather than forced.
func (c *RevisionConsumer) Apply(ctx context.Context, fact RevisionFact) (RevisionResult, error) {
	if fact.IntentID == "" {
		return RevisionResult{}, errors.New("revision fact has no intent id")
	}
	if fact.RevisionVersion <= 0 {
		return RevisionResult{IntentID: fact.IntentID, Basis: "no source version"}, nil
	}
	intent, found, err := ReadIntent(ctx, c.Pool, fact.IntentID)
	if err != nil {
		return RevisionResult{}, err
	}
	if !found {
		return RevisionResult{IntentID: fact.IntentID, RevisionVersion: fact.RevisionVersion, Basis: "intent absent"}, nil
	}
	stored, err := AppliedRevision(ctx, c.Pool, fact.IntentID)
	if err != nil {
		return RevisionResult{}, err
	}
	if fact.RevisionVersion <= stored {
		return RevisionResult{
			IntentID: fact.IntentID, RevisionVersion: fact.RevisionVersion,
			Basis: fmt.Sprintf("older_or_equal revision %d <= %d", fact.RevisionVersion, stored),
		}, nil
	}
	target := fact.TargetState
	if target == "" {
		target = IntentRevised
	}
	if intent.State == target {
		return RevisionResult{IntentID: fact.IntentID, RevisionVersion: fact.RevisionVersion, Basis: "already " + target}, nil
	}
	if ClassifyTransition(intent.State, target) != TransitionFact {
		return RevisionResult{
			IntentID: fact.IntentID, RevisionVersion: fact.RevisionVersion,
			Basis: fmt.Sprintf("no fact edge %s -> %s", intent.State, target),
		}, nil
	}

	tx, err := c.Pool.Begin(ctx)
	if err != nil {
		return RevisionResult{}, fmt.Errorf("begin revision apply: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := TransitionIntent(ctx, tx, intent.IntentID, intent.State, intent.StateVersion, target, 0); err != nil {
		if errors.Is(err, ErrTransitionRefused) || errors.Is(err, ErrIllegalTransition) {
			return RevisionResult{IntentID: fact.IntentID, RevisionVersion: fact.RevisionVersion, Basis: "cas refused"}, nil
		}
		return RevisionResult{}, err
	}
	if err := AppendEvent(ctx, tx, Event{
		IntentID: intent.IntentID, Kind: EventRevisionApplied,
		FromState: intent.State, ToState: target,
		AttemptID: fact.AttemptID, RevisionVersion: fact.RevisionVersion,
		Detail: "basis=" + fact.Basis,
	}); err != nil {
		return RevisionResult{}, err
	}
	if _, err := applyLifecycleProjection(ctx, tx, intent.RequestID, fact.AttemptID, fact.RevisionVersion); err != nil {
		return RevisionResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return RevisionResult{}, fmt.Errorf("commit revision apply: %w", err)
	}
	return RevisionResult{
		IntentID: intent.IntentID, Applied: true, FromState: intent.State, ToState: target,
		RevisionVersion: fact.RevisionVersion, Basis: fact.Basis,
	}, nil
}

// ApplyAll consumes the given revisions in ascending version order.
func (c *RevisionConsumer) ApplyAll(ctx context.Context, facts []RevisionFact) ([]RevisionResult, error) {
	ordered := append([]RevisionFact(nil), facts...)
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].RevisionVersion < ordered[j].RevisionVersion })
	out := make([]RevisionResult, 0, len(ordered))
	for _, f := range ordered {
		res, err := c.Apply(ctx, f)
		if err != nil {
			return out, err
		}
		out = append(out, res)
	}
	return out, nil
}
