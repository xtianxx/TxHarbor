package execution

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// StepIDBytes is the preallocated step identity width (16 bytes -> 32 hex).
const StepIDBytes = 16

// NewStepID preallocates a 16-byte hex step identity. 011 generates it before
// the issue insert; retries reuse the same id (010 converges on
// (intent_id, step_id)).
func NewStepID() (string, error) {
	var b [StepIDBytes]byte
	if _, err := cryptorand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate step id: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// ErrOpenStepExists means an open (issued) step already exists for the intent:
// reconcile before issuing a new step (execution_steps_open_uniq).
var ErrOpenStepExists = errors.New("an open execution step already exists for this intent")

// ErrStepConvergeRefused means the fenced converge matched no open step owned
// by this qualification (state or owner/version fence mismatch).
var ErrStepConvergeRefused = errors.New("execution step converge refused")

// Step is one execution_steps row.
type Step struct {
	StepID          string
	IntentID        string
	Action          AdvanceAction
	State           string
	OwnerID         string
	LeaseVersion    int64
	AttemptID       *string
	AnchorAttemptID *string
	TxHash          *string
	OutcomeClass    *string
	RecoveryVersion *int64
	RevisionVersion *int64
	Evidence        string
	IssuedAt        time.Time
	UpdatedAt       time.Time
}

const stepColumns = `step_id, intent_id, action, state, owner_id, lease_version,
  attempt_id, anchor_attempt_id, tx_hash, outcome_class, recovery_version,
  revision_version, evidence, issued_at, updated_at`

const readStepSQL = `SELECT ` + stepColumns + ` FROM execution_steps WHERE step_id = $1`
const readOpenStepSQL = `SELECT ` + stepColumns + ` FROM execution_steps
  WHERE intent_id = $1 AND state = 'issued'`

func scanStep(row pgx.Row) (Step, error) {
	var s Step
	var action, state string
	err := row.Scan(&s.StepID, &s.IntentID, &action, &state, &s.OwnerID, &s.LeaseVersion,
		&s.AttemptID, &s.AnchorAttemptID, &s.TxHash, &s.OutcomeClass, &s.RecoveryVersion,
		&s.RevisionVersion, &s.Evidence, &s.IssuedAt, &s.UpdatedAt)
	s.Action = AdvanceAction(action)
	s.State = state
	return s, err
}

// ReadStep reads one step row; found=false when it does not exist.
func ReadStep(ctx context.Context, q Queryer, stepID string) (Step, bool, error) {
	s, err := scanStep(q.QueryRow(ctx, readStepSQL, stepID))
	if errors.Is(err, pgx.ErrNoRows) {
		return s, false, nil
	}
	if err != nil {
		return s, false, fmt.Errorf("read execution step: %w", err)
	}
	return s, true, nil
}

// ReadOpenStep reads the intent's open (issued) step; found=false when none
// exists.
func ReadOpenStep(ctx context.Context, q Queryer, intentID string) (Step, bool, error) {
	s, err := scanStep(q.QueryRow(ctx, readOpenStepSQL, intentID))
	if errors.Is(err, pgx.ErrNoRows) {
		return s, false, nil
	}
	if err != nil {
		return s, false, fmt.Errorf("read open execution step: %w", err)
	}
	return s, true, nil
}

// StepIssue is the insert-first step position persisted before the 010 call.
type StepIssue struct {
	StepID          string
	IntentID        string
	Action          AdvanceAction
	OwnerID         string
	LeaseVersion    int64
	RecoveryVersion *int64
}

const issueStepSQL = `INSERT INTO execution_steps
  (step_id, intent_id, action, state, owner_id, lease_version, recovery_version)
  VALUES ($1, $2, $3, 'issued', $4, $5, $6)`

// IssueStep persists one open step. A second open step for the same intent is
// refused by execution_steps_open_uniq (ErrOpenStepExists), making
// reconcile-before-decision structural.
func IssueStep(ctx context.Context, q Queryer, in StepIssue) error {
	switch in.Action {
	case ActionFirstBroadcast, ActionReplay, ActionReplace:
	default:
		return fmt.Errorf("step action %q is not a send-class action", in.Action)
	}
	_, err := q.Exec(ctx, issueStepSQL, in.StepID, in.IntentID, string(in.Action),
		in.OwnerID, in.LeaseVersion, in.RecoveryVersion)
	if err != nil {
		if isConstraint(err, "execution_steps_open_uniq") {
			return ErrOpenStepExists
		}
		return fmt.Errorf("issue execution step: %w", err)
	}
	return nil
}

// StepConverge is the fenced terminal update of an open step.
type StepConverge struct {
	StepID          string
	State           string
	AttemptID       *string
	TxHash          *string
	OutcomeClass    *string
	RevisionVersion *int64
	Evidence        string
	OwnerID         string
	LeaseVersion    int64
}

const convergeStepSQL = `UPDATE execution_steps
  SET state = $2, attempt_id = $3, tx_hash = $4, outcome_class = $5,
      revision_version = $6, evidence = $7, updated_at = now()
  WHERE step_id = $1 AND state = 'issued' AND owner_id = $8 AND lease_version = $9`

// ConvergeStep moves an open step to a terminal state with an exact-version
// fence (owner_id + lease_version). If the qualification advanced, the old
// holder's converge matches zero rows (ErrStepConvergeRefused) and the step
// stays durable evidence for the new holder to reconcile.
func ConvergeStep(ctx context.Context, q Queryer, in StepConverge) error {
	switch in.State {
	case StepConverged, StepRefused, StepUnknown:
	default:
		return fmt.Errorf("step terminal state %q is not converged/refused/unknown", in.State)
	}
	tag, err := q.Exec(ctx, convergeStepSQL, in.StepID, in.State, in.AttemptID, in.TxHash,
		in.OutcomeClass, in.RevisionVersion, in.Evidence, in.OwnerID, in.LeaseVersion)
	if err != nil {
		return fmt.Errorf("converge execution step: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrStepConvergeRefused
	}
	return nil
}

func isConstraint(err error, name string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.ConstraintName == name
}
