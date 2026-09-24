package execution

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/events"
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
		// The state edge is already applied (e.g. a reconfirm revision after
		// completed->revised). The newer authority version is still consumed
		// into the projection so the display follows the revision chain in
		// order; no state is rewritten and no second event is fabricated.
		advanced, err := c.consumeProjectionVersion(ctx, intent, fact)
		if err != nil {
			return RevisionResult{}, err
		}
		basis := "already " + target
		if advanced {
			basis += "; projection advanced"
		}
		return RevisionResult{IntentID: fact.IntentID, RevisionVersion: fact.RevisionVersion, Basis: basis}, nil
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
	// T053: read the superseded event and the 010 chain evidence before the
	// transition appends anything, so revises_event_id references the
	// pre-revision fact. A revision whose object has no post-cutover event or
	// whose attempt has no chain block fact emits no revision event (there is
	// no truthful superseded/chain identity to carry; events.md §7); the
	// existing 011 revision semantics are unchanged.
	superseded, supersedes, err := readWithdrawalHeadEvent(ctx, tx, intent.IntentID)
	if err != nil {
		return RevisionResult{}, err
	}
	evidence, hasEvidence, err := readWithdrawalChainEvidence(ctx, tx, intent.IntentID, fact.AttemptID)
	if err != nil {
		return RevisionResult{}, err
	}
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
	if supersedes && hasEvidence {
		if err := appendWithdrawalExecutionRevisedEvent(ctx, tx, intent.IntentID,
			intent.State, target, fact, evidence, superseded); err != nil {
			return RevisionResult{}, err
		}
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

// consumeProjectionVersion advances the display projection to a strictly
// newer authority revision version and records exactly one
// projection_refreshed event when the row actually moved. It is the
// projection half of revision consumption: no state edge, no authorization,
// no claim, no send; repeat and older inputs affect zero rows.
func (c *RevisionConsumer) consumeProjectionVersion(ctx context.Context, intent Intent, fact RevisionFact) (bool, error) {
	tx, err := c.Pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin projection revision: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	advanced, err := advanceLifecycleProjection(ctx, tx, intent.RequestID, fact.AttemptID, fact.RevisionVersion)
	if err != nil {
		return false, err
	}
	if !advanced {
		return false, nil
	}
	if err := AppendEvent(ctx, tx, Event{
		IntentID: intent.IntentID, Kind: EventProjectionRefreshed,
		AttemptID: fact.AttemptID, RevisionVersion: fact.RevisionVersion,
		Detail: "authority revision observed basis=" + fact.Basis,
	}); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit projection revision: %w", err)
	}
	return true, nil
}

// Observe consumes one authority revision version into the display projection
// without a state transition: the authority moved (so the stored reference
// version must follow in order) while the current facts justify no revision
// fact edge. Version-guarded by the projection's own lifecycle_version (never
// rewritten downward); no send, allocation or claim is involved.
func (c *RevisionConsumer) Observe(ctx context.Context, fact RevisionFact) (RevisionResult, error) {
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
	advanced, err := c.consumeProjectionVersion(ctx, intent, fact)
	if err != nil {
		return RevisionResult{}, err
	}
	basis := "authority revision observed"
	if !advanced {
		basis = "older_or_equal projection"
	}
	return RevisionResult{IntentID: fact.IntentID, RevisionVersion: fact.RevisionVersion, Basis: basis}, nil
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

// executionSupersededRef identifies the intent's most recent committed outbox
// event before a revision: it becomes the revision event's revises_event_id
// and the payload's superseded_identity event reference (T053;
// contracts/events.md §5.1).
type executionSupersededRef struct {
	EventID          uuid.UUID
	EventType        string
	AggregateVersion int64
}

// withdrawalChainEvidence is the 010 chain fact that justifies a revision: the
// attempt's most recent receipt/reconciliation block identity, read-only from
// the 010 authority tables. It supplies the chain identity the revision
// envelope and superseded_identity carry (contracts/events.md §1/§5).
type withdrawalChainEvidence struct {
	ChainID     int64
	BlockNumber int64
	BlockHash   string
	TxHash      string
}

// readWithdrawalHeadEvent reads the intent aggregate's latest committed outbox
// event inside the caller's transaction and BEFORE the revision transition
// appends anything, so the reference is the pre-revision fact. found=false
// means the object's fact stream predates the cutover (events.md §7); the
// caller then emits no revision event rather than fabricating an identity.
func readWithdrawalHeadEvent(ctx context.Context, tx pgx.Tx, intentID string) (executionSupersededRef, bool, error) {
	var ref executionSupersededRef
	err := tx.QueryRow(ctx, readWithdrawalHeadEventSQL, withdrawalIntentAggregateType, intentID).
		Scan(&ref.EventID, &ref.EventType, &ref.AggregateVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		return executionSupersededRef{}, false, nil
	}
	if err != nil {
		return executionSupersededRef{}, false, fmt.Errorf("read withdrawal head event: %w", err)
	}
	return ref, true, nil
}

// readWithdrawalChainEvidence reads the attempt's most recent chain block fact
// from the 010 receipt/reconciliation tables (read-only; 010 owns them). A
// missing attempt or a chain fact without a positive block is reported as
// found=false: the revision event cannot carry a truthful chain identity and
// is then not emitted.
func readWithdrawalChainEvidence(ctx context.Context, tx pgx.Tx, intentID, attemptID string) (withdrawalChainEvidence, bool, error) {
	if attemptID == "" {
		return withdrawalChainEvidence{}, false, nil
	}
	var ev withdrawalChainEvidence
	err := tx.QueryRow(ctx, readWithdrawalChainEvidenceSQL, intentID, attemptID).
		Scan(&ev.ChainID, &ev.BlockNumber, &ev.BlockHash, &ev.TxHash)
	if errors.Is(err, pgx.ErrNoRows) {
		return withdrawalChainEvidence{}, false, nil
	}
	if err != nil {
		return withdrawalChainEvidence{}, false, fmt.Errorf("read withdrawal chain evidence: %w", err)
	}
	return ev, true, nil
}

// appendWithdrawalExecutionRevisedEvent emits withdrawal.execution.revised for
// one applied chain-fact revision (T053): superseded is the pre-revision event
// (revises_event_id + payload superseded_identity with the old chain block
// identity), toState is the revision's post state, reason is the authority
// basis, and the envelope recovery_version carries the driving authority
// revision version (the revision-chain version this project's 010/011
// boundary exposes; 013 revision events always carry one; contracts/events.md
// §1/§5). The event is a fact notification: it is never an execution
// permission and repeated delivery MUST NOT trigger a new send (FR-05).
func appendWithdrawalExecutionRevisedEvent(
	ctx context.Context,
	tx pgx.Tx,
	intentID, fromState, toState string,
	fact RevisionFact,
	evidence withdrawalChainEvidence,
	superseded executionSupersededRef,
) error {
	payload := map[string]any{
		"superseded_identity": map[string]any{
			"event_id":          superseded.EventID.String(),
			"event_type":        superseded.EventType,
			"aggregate_version": superseded.AggregateVersion,
			"chain_id":          evidence.ChainID,
			"block_number":      evidence.BlockNumber,
			"block_hash":        evidence.BlockHash,
			"tx_hash":           evidence.TxHash,
		},
		"from_state":       fromState,
		"to_state":         toState,
		"reason":           fact.Basis,
		"intent_id":        intentID,
		"revision_version": fact.RevisionVersion,
	}
	if fact.AttemptID != "" {
		payload["attempt_id"] = fact.AttemptID
	}
	ev := events.Event{
		EventType:       events.EventTypeWithdrawalExecutionRevised,
		SchemaVersion:   events.SchemaVersionV1,
		IdentityKind:    events.IdentityKindBusinessObject,
		AggregateType:   withdrawalIntentAggregateType,
		AggregateID:     intentID,
		Payload:         payload,
		OccurredAt:      time.Now().UTC(),
		ChainID:         evidence.ChainID,
		BlockNumber:     evidence.BlockNumber,
		BlockHash:       evidence.BlockHash,
		RecoveryVersion: fact.RevisionVersion,
		RevisesEventID:  superseded.EventID,
		SourceKind:      withdrawalExecutionSourceKind,
		SourceID:        intentID,
	}
	if _, err := events.Append(ctx, tx, ev); err != nil {
		return fmt.Errorf("append withdrawal execution revised event %s: %w", intentID, err)
	}
	return nil
}

// readWithdrawalHeadEventSQL reads the highest-version committed event of one
// withdrawal intent.
const readWithdrawalHeadEventSQL = `
SELECT event_id, event_type, aggregate_version
FROM outbox_events
WHERE aggregate_type = $1 AND aggregate_id = $2
ORDER BY aggregate_version DESC, id DESC
LIMIT 1`

// readWithdrawalChainEvidenceSQL reads the attempt's most recent chain block
// fact across the 010 receipt and reconciliation tables (both append-only;
// the newest observation wins). Only rows with a positive block number are
// considered: without one the revision has no truthful chain identity.
const readWithdrawalChainEvidenceSQL = `
SELECT i.chain_id, e.block_number, e.block_hash, e.tx_hash
FROM payment_intents i
JOIN LATERAL (
    SELECT r.block_number, r.block_hash, r.tx_hash, r.observed_at
    FROM tx_receipts r
    WHERE r.attempt_id = $2 AND r.block_number > 0
    UNION ALL
    SELECT c.block_number, c.block_hash, c.tx_hash, c.observed_at
    FROM tx_reconciliations c
    WHERE c.attempt_id = $2 AND c.block_number > 0
    ORDER BY observed_at DESC
    LIMIT 1
) e ON TRUE
WHERE i.intent_id = $1`
