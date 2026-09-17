package execution

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// Intent state values (data-model state machine A).
const (
	IntentAdmitted    = "admitted"
	IntentClaimed     = "claimed"
	IntentExecuting   = "executing"
	IntentCompleted   = "completed"
	IntentFailed      = "failed"
	IntentReconciling = "reconciling"
	IntentRevised     = "revised"
)

// IntentStates is the closed state list.
var IntentStates = []string{
	IntentAdmitted, IntentClaimed, IntentExecuting, IntentCompleted,
	IntentFailed, IntentReconciling, IntentRevised,
}

// ValidIntentState reports whether s is in the closed state list.
func ValidIntentState(s string) bool {
	for _, want := range IntentStates {
		if s == want {
			return true
		}
	}
	return false
}

// Intent is one payment_intents row.
type Intent struct {
	IntentID                string
	RequestID               string
	ChainID                 int64
	Sender                  string
	AuthorizationID         string
	AuthorizationVersion    int64
	State                   string
	StateVersion            int64
	AdmittedRecoveryVersion int64
	AdmittedAt              time.Time
	UpdatedAt               time.Time
}

const readIntentSQL = `SELECT intent_id, request_id, chain_id, sender, authorization_id,
  authorization_version, state, state_version, admitted_recovery_version, admitted_at, updated_at
  FROM payment_intents WHERE intent_id = $1`

// ReadIntent reads one intent row; found=false when it does not exist.
func ReadIntent(ctx context.Context, q Queryer, intentID string) (Intent, bool, error) {
	var in Intent
	err := q.QueryRow(ctx, readIntentSQL, intentID).Scan(
		&in.IntentID, &in.RequestID, &in.ChainID, &in.Sender, &in.AuthorizationID,
		&in.AuthorizationVersion, &in.State, &in.StateVersion,
		&in.AdmittedRecoveryVersion, &in.AdmittedAt, &in.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return in, false, nil
	}
	if err != nil {
		return in, false, fmt.Errorf("read payment intent: %w", err)
	}
	return in, true, nil
}

// TransitionKind is the physical separation of the two guard classes (M2).
type TransitionKind int

const (
	// TransitionNone: not a legal edge; it is a refusal, never a transition.
	TransitionNone TransitionKind = iota
	// TransitionSendEnabling: admitted->claimed, claimed->executing,
	// reconciling->executing, revised->executing. The caller MUST have
	// verified a valid claim (owner_id + lease_version + active + unexpired)
	// and the full current gate set before applying it.
	TransitionSendEnabling
	// TransitionFact: ->reconciling, ->completed, ->failed, ->revised. Guarded
	// only by (state, state_version) CAS + version monotonicity; it consumes
	// no authorization and requires no claim (M2). completed may be revised by
	// authority; failed is terminal for automatic execution (no auto-repay),
	// so only an authority revision may leave it.
	TransitionFact
)

var sendEnablingTransitions = map[[2]string]bool{
	{IntentAdmitted, IntentClaimed}:      true,
	{IntentClaimed, IntentExecuting}:     true,
	{IntentReconciling, IntentExecuting}: true,
	{IntentRevised, IntentExecuting}:     true,
}

var factTransitions = map[[2]string]bool{
	{IntentExecuting, IntentReconciling}: true,
	{IntentExecuting, IntentCompleted}:   true,
	{IntentReconciling, IntentCompleted}: true,
	{IntentExecuting, IntentFailed}:      true,
	{IntentReconciling, IntentFailed}:    true,
	{IntentCompleted, IntentRevised}:     true,
	{IntentFailed, IntentRevised}:        true,
}

// ClassifyTransition returns the kind of the from->to edge, or TransitionNone
// when the edge is illegal (or unknown states). An illegal edge is a refusal:
// it produces zero writes and is recorded as an error, never as a transition.
func ClassifyTransition(from, to string) TransitionKind {
	if !ValidIntentState(from) || !ValidIntentState(to) {
		return TransitionNone
	}
	if sendEnablingTransitions[[2]string{from, to}] {
		return TransitionSendEnabling
	}
	if factTransitions[[2]string{from, to}] {
		return TransitionFact
	}
	return TransitionNone
}

// ErrIllegalTransition is returned for an edge outside state machine A.
var ErrIllegalTransition = errors.New("illegal intent state transition")

// ErrTransitionRefused means a legal edge lost its (state, state_version) CAS
// (concurrent writer, stale version) and wrote zero rows.
var ErrTransitionRefused = errors.New("intent transition refused")

const transitionIntentSQL = `UPDATE payment_intents
  SET state = $4, state_version = state_version + 1, updated_at = now()
  WHERE intent_id = $1 AND state = $2 AND state_version = $3`

// TransitionIntent applies one legal transition with a (state, state_version)
// CAS and appends the state_changed event in the same transaction. Callers
// applying a send-enabling edge MUST have verified the claim and the full gate
// set first; fact edges need neither (M2).
func TransitionIntent(ctx context.Context, tx pgx.Tx, intentID, from string, fromVersion int64, to string, leaseVersion int64) error {
	if ClassifyTransition(from, to) == TransitionNone {
		return fmt.Errorf("%w: %s -> %s", ErrIllegalTransition, from, to)
	}
	tag, err := tx.Exec(ctx, transitionIntentSQL, intentID, from, fromVersion, to)
	if err != nil {
		return fmt.Errorf("apply intent transition: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: %s -> %s (version %d)", ErrTransitionRefused, from, to, fromVersion)
	}
	return AppendEvent(ctx, tx, Event{
		IntentID:     intentID,
		Kind:         EventStateChanged,
		FromState:    from,
		ToState:      to,
		LeaseVersion: leaseVersion,
	})
}

// Event is one append-only execution_events row.
type Event struct {
	IntentID        string
	Kind            string
	FromState       string
	ToState         string
	LeaseVersion    int64
	StepID          string
	AttemptID       string
	RevisionVersion int64
	Detail          string
}

const insertEventSQL = `INSERT INTO execution_events
  (intent_id, kind, from_state, to_state, lease_version, step_id, attempt_id, revision_version, detail)
  VALUES ($1, $2, NULLIF($3, ''), NULLIF($4, ''), NULLIF($5, 0), NULLIF($6, ''), NULLIF($7, ''), NULLIF($8, 0), $9)`

// AppendEvent writes one execution_events row. The kind must be in the closed
// list (the migration CHECK also enforces it); identity/version values are
// evidence and never carry secrets or raw bytes.
func AppendEvent(ctx context.Context, tx pgx.Tx, e Event) error {
	if !ValidEventKind(e.Kind) {
		return fmt.Errorf("execution event kind %q is not in the closed list", e.Kind)
	}
	_, err := tx.Exec(ctx, insertEventSQL, e.IntentID, e.Kind, e.FromState, e.ToState,
		e.LeaseVersion, e.StepID, e.AttemptID, e.RevisionVersion, e.Detail)
	if err != nil {
		return fmt.Errorf("append execution event: %w", err)
	}
	return nil
}
