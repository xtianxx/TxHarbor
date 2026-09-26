// scan.go implements the T006 task/checkpoint/gap state machine skeleton:
// the task lifecycle of contracts/task-lifecycle.md, the gap vocabulary of
// data-model.md §1.3, the checkpoint pointer rule of §1.2/§5, and the
// claim-execute-commit entry point (claim in a short transaction with
// `SELECT recon_task ... FOR UPDATE`, no RPC inside; §5.1). ScanOnce itself
// (T016) builds on these primitives; this file owns only the state machine
// and the claim half of the protocol.
package reconciliation

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// timeScopeStep is the position granularity of time-scoped ranges and
// checkpoints: one microsecond, the resolution of PostgreSQL timestamptz
// (migration 000016 types scope_start_at/covered_through_at/range_start_at as
// TIMESTAMPTZ). Using the column resolution prevents any uncovered sliver at
// a range boundary.
const timeScopeStep = time.Microsecond

// maxClaimSpan bounds one claim interval at the app boundary. Real budgets are
// far smaller (T008); the bound only prevents arithmetic overflow.
const maxClaimSpan = 1 << 32

// RangeBound is one inclusive boundary of a height or time range. Exactly one
// of Height/Time is meaningful according to Kind (the migration stores the two
// pairs separately). Construct with HeightBound or TimeBound so the SQL
// conversion helpers round-trip the value.
type RangeBound struct {
	Kind   ScopeKind
	Height int64
	Time   time.Time
}

// HeightBound returns an inclusive height boundary.
func HeightBound(h int64) RangeBound { return RangeBound{Kind: ScopeHeight, Height: h} }

// TimeBound returns an inclusive time boundary truncated to the time-scope
// step (one microsecond, UTC) so checkpoint positions are stable.
func TimeBound(t time.Time) RangeBound {
	return RangeBound{Kind: ScopeTime, Time: t.UTC().Truncate(timeScopeStep)}
}

// Valid reports whether the bound carries a usable value for its kind.
func (b RangeBound) Valid() bool {
	switch b.Kind {
	case ScopeHeight:
		return true
	case ScopeTime:
		return !b.Time.IsZero()
	}
	return false
}

// Compare orders two bounds of the same kind: -1, 0, 1. Mismatched kinds and
// invalid bounds are contract violations (unknown shapes are never guessed).
func (b RangeBound) Compare(o RangeBound) (int, error) {
	if !b.Valid() || !o.Valid() {
		return 0, contractErrorf("invalid range bound (kind %q)", b.Kind)
	}
	if b.Kind != o.Kind {
		return 0, contractErrorf("range bound kind mismatch: %q vs %q", b.Kind, o.Kind)
	}
	switch b.Kind {
	case ScopeHeight:
		switch {
		case b.Height < o.Height:
			return -1, nil
		case b.Height > o.Height:
			return 1, nil
		}
		return 0, nil
	case ScopeTime:
		switch {
		case b.Time.Before(o.Time):
			return -1, nil
		case b.Time.After(o.Time):
			return 1, nil
		}
		return 0, nil
	}
	return 0, contractErrorf("unknown scope kind %q", b.Kind)
}

// Successor returns the next position after b (data-model.md §1.2/§5.1: the
// claim pointer starts after the persisted prefix). Heights advance by one
// block; time scopes advance by one microsecond.
func (b RangeBound) Successor() (RangeBound, error) {
	switch b.Kind {
	case ScopeHeight:
		if b.Height == math.MaxInt64 {
			return RangeBound{}, contractErrorf("height range overflow at %d", b.Height)
		}
		return HeightBound(b.Height + 1), nil
	case ScopeTime:
		if b.Time.IsZero() {
			return RangeBound{}, contractErrorf("time range successor on zero bound")
		}
		if b.Time.Add(timeScopeStep).Equal(b.Time) {
			return RangeBound{}, contractErrorf("time range overflow at %s", b.Time)
		}
		return TimeBound(b.Time.Add(timeScopeStep)), nil
	}
	return RangeBound{}, contractErrorf("unknown scope kind %q", b.Kind)
}

// rangePairArgs splits a validated bound into the (height, at) column pair the
// migration requires: exactly one is non-NULL.
func rangePairArgs(b RangeBound) (any, any) {
	if b.Kind == ScopeTime {
		return nil, b.Time
	}
	return b.Height, nil
}

// boundFromPair decodes the two typed columns of one scope/range boundary
// (BIGINT + TIMESTAMPTZ) according to the task's scope kind. Exactly the pair
// matching the kind must be non-NULL; anything else is a schema contract
// violation.
func boundFromPair(kind ScopeKind, height *int64, at *time.Time) (RangeBound, error) {
	switch kind {
	case ScopeHeight:
		if height == nil || at != nil {
			return RangeBound{}, contractErrorf("height scope must carry exactly the BIGINT pair")
		}
		return HeightBound(*height), nil
	case ScopeTime:
		if at == nil || height != nil {
			return RangeBound{}, contractErrorf("time scope must carry exactly the TIMESTAMPTZ pair")
		}
		return TimeBound(*at), nil
	}
	return RangeBound{}, contractErrorf("unknown scope kind %q", kind)
}

// rangeAuditValue renders a bound for recon_audit.target JSONB.
func rangeAuditValue(b RangeBound) any {
	if b.Kind == ScopeTime {
		return b.Time.UTC().Format(time.RFC3339Nano)
	}
	return b.Height
}

// TaskState mirrors recon_task.state (data-model.md §1.1;
// contracts/task-lifecycle.md `created → running ⇄ paused | suspended_budget
// → running → done | cancelled`).
type TaskState string

const (
	TaskStateCreated         TaskState = "created"
	TaskStateRunning         TaskState = "running"
	TaskStatePaused          TaskState = "paused"
	TaskStateSuspendedBudget TaskState = "suspended_budget"
	TaskStateDone            TaskState = "done"
	TaskStateCancelled       TaskState = "cancelled"
)

// Valid reports whether s is one of the closed task states.
func (s TaskState) Valid() bool {
	switch s {
	case TaskStateCreated, TaskStateRunning, TaskStatePaused,
		TaskStateSuspendedBudget, TaskStateDone, TaskStateCancelled:
		return true
	}
	return false
}

// Terminal reports whether s is a terminal task state.
func (s TaskState) Terminal() bool { return s == TaskStateDone || s == TaskStateCancelled }

// CanTransitionTask reports whether the task lifecycle allows from -> to.
// Paused and budget-suspended tasks return to running before any terminal
// transition; illegal jumps are refused and audited by TransitionTask.
func CanTransitionTask(from, to TaskState) bool {
	switch from {
	case TaskStateCreated:
		return to == TaskStateRunning
	case TaskStateRunning:
		switch to {
		case TaskStatePaused, TaskStateSuspendedBudget, TaskStateDone, TaskStateCancelled:
			return true
		}
	case TaskStatePaused, TaskStateSuspendedBudget:
		return to == TaskStateRunning
	}
	return false
}

// GapReason mirrors recon_gap.reason (data-model.md §1.3): the closed
// vocabulary of uncovered-range causes. Paused/suspended/cancelled scans MUST
// leave gap rows so "fully consistent" can never be claimed over them.
type GapReason string

const (
	GapNotStarted          GapReason = "not_started"
	GapInterrupted         GapReason = "interrupted"
	GapBudgetExhausted     GapReason = "budget_exhausted"
	GapPaused              GapReason = "paused"
	GapFreshnessHold       GapReason = "freshness_hold"
	GapUpstreamUnconnected GapReason = "upstream_unconnected"
	GapQueryFailed         GapReason = "query_failed"
)

// Valid reports whether r is one of the closed gap reasons.
func (r GapReason) Valid() bool {
	switch r {
	case GapNotStarted, GapInterrupted, GapBudgetExhausted, GapPaused,
		GapFreshnessHold, GapUpstreamUnconnected, GapQueryFailed:
		return true
	}
	return false
}

// Checkpoint is one recon_checkpoint row (data-model.md §1.2). Seq is
// monotonic per task; the task pointer is the highest seq. The pointer never
// advances past unpersisted ranges: ResultPersistedThrough MUST be <=
// CoveredThrough, and only the contiguous persisted prefix is ever used to
// plan the next claim.
type Checkpoint struct {
	TaskID                 string
	Seq                    int64
	CoveredThrough         RangeBound
	ResultPersistedThrough RangeBound
	CreatedAt              time.Time
}

// Validate enforces the checkpoint invariant of data-model.md §1.2.
func (c Checkpoint) Validate() error {
	if c.TaskID == "" {
		return contractErrorf("checkpoint has no task_id")
	}
	if c.Seq <= 0 {
		return contractErrorf("checkpoint seq must be positive")
	}
	if !c.CoveredThrough.Valid() || !c.ResultPersistedThrough.Valid() {
		return contractErrorf("checkpoint has an invalid range bound")
	}
	if c.CoveredThrough.Kind != c.ResultPersistedThrough.Kind {
		return contractErrorf("checkpoint range kinds differ")
	}
	cmp, err := c.ResultPersistedThrough.Compare(c.CoveredThrough)
	if err != nil {
		return err
	}
	if cmp > 0 {
		return contractErrorf("checkpoint persisted_through %v is past covered_through %v",
			c.ResultPersistedThrough, c.CoveredThrough)
	}
	return nil
}

// Task is the subset of recon_task the state machine needs. Budget/policy
// JSONB fields stay with the T008/T016 callers.
type Task struct {
	TaskID      string
	ScopeKind   ScopeKind
	ScopeStart  RangeBound
	ScopeEnd    RangeBound
	State       TaskState
	PauseReason string
}

// NextClaimStart returns the first position a new claim may cover: the
// successor of the checkpoint's persisted prefix, or the scope start when no
// checkpoint exists. It never returns a position inside a range that is not
// yet persisted (data-model.md §5.1).
func NextClaimStart(head *Checkpoint, scopeStart RangeBound) (RangeBound, error) {
	if head == nil {
		if !scopeStart.Valid() {
			return RangeBound{}, contractErrorf("scope start bound is invalid")
		}
		return scopeStart, nil
	}
	if err := head.Validate(); err != nil {
		return RangeBound{}, err
	}
	if head.ResultPersistedThrough.Kind != scopeStart.Kind {
		return RangeBound{}, contractErrorf("checkpoint kind %q does not match scope kind %q",
			head.ResultPersistedThrough.Kind, scopeStart.Kind)
	}
	return head.ResultPersistedThrough.Successor()
}

// ClaimAttemptRequest asks for the next budgeted interval of one running task.
// Span bounds the number of positions (heights or microseconds) in the
// claimed interval; it comes from the task budget (T008/T016).
type ClaimAttemptRequest struct {
	TaskID   string
	Owner    string
	Span     int64
	LeaseTTL time.Duration
}

// ClaimAttemptResult is one claimed attempt.
type ClaimAttemptResult struct {
	AttemptID      string
	RangeStart     RangeBound
	RangeEnd       RangeBound
	LeaseExpiresAt time.Time
}

// ClaimScanAttempt is the claim half of the claim-execute-commit protocol
// (data-model.md §5.1, contracts/task-lifecycle.md). It runs in a short
// transaction only: `SELECT recon_task ... FOR UPDATE`, verify
// state='running', compute the interval from the persisted checkpoint pointer
// plus span, insert recon_scan_attempt(state='claimed'), audit, COMMIT. No
// RPC and no long-lived lock ever happen here; the attempt row (plus its
// unexpired lease) is the ownership proof during the out-of-transaction
// execution phase.
//
// The partial unique index recon_scan_attempt_claimed_uniq admits one claimed
// attempt per task; the loser of a concurrent claim gets ErrClaimTaken and
// retries the next interval. The transaction takes recon_task first and
// recon_checkpoint second, the package-wide lock order (task ->
// attempt/checkpoint/gap), so claim/commit/settle serialize per task instead
// of deadlocking.
func (s *Store) ClaimScanAttempt(ctx context.Context, req ClaimAttemptRequest) (ClaimAttemptResult, error) {
	if s == nil || s.db == nil {
		return ClaimAttemptResult{}, contractErrorf("store has no database")
	}
	if strings.TrimSpace(req.TaskID) == "" {
		return ClaimAttemptResult{}, contractErrorf("claim requires a task_id")
	}
	if strings.TrimSpace(req.Owner) == "" {
		return ClaimAttemptResult{}, contractErrorf("claim requires an owner")
	}
	if req.Span <= 0 || req.Span > maxClaimSpan {
		return ClaimAttemptResult{}, contractErrorf("claim span %d is out of bounds", req.Span)
	}
	if req.LeaseTTL <= 0 {
		return ClaimAttemptResult{}, contractErrorf("claim lease ttl must be positive")
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return ClaimAttemptResult{}, fmt.Errorf("begin claim: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	task, err := lockTaskTx(ctx, tx, req.TaskID)
	if err != nil {
		return ClaimAttemptResult{}, err
	}
	if task.State != TaskStateRunning {
		return ClaimAttemptResult{}, fmt.Errorf("%w: state=%s", ErrTaskNotRunning, task.State)
	}

	head, err := readCheckpointHeadTx(ctx, tx, task.TaskID, task.ScopeKind)
	if err != nil {
		return ClaimAttemptResult{}, err
	}
	start, err := NextClaimStart(head, task.ScopeStart)
	if err != nil {
		return ClaimAttemptResult{}, err
	}
	if cmp, err := start.Compare(task.ScopeEnd); err != nil {
		return ClaimAttemptResult{}, err
	} else if cmp > 0 {
		return ClaimAttemptResult{}, fmt.Errorf("%w: next start past scope end", ErrScopeExhausted)
	}
	end, err := planClaimEnd(start, task.ScopeEnd, req.Span)
	if err != nil {
		return ClaimAttemptResult{}, err
	}

	startHeight, startAt := rangePairArgs(start)
	endHeight, endAt := rangePairArgs(end)
	attemptID := uuid.NewString()
	var leaseExpiresAt time.Time
	err = tx.QueryRow(ctx, insertAttemptSQL,
		attemptID, task.TaskID, startHeight, endHeight, startAt, endAt,
		req.Owner, req.LeaseTTL.Seconds()).
		Scan(&leaseExpiresAt)
	if constraint := uniqueViolationConstraint(err); constraint != "" {
		// recon_scan_attempt_claimed_uniq: another invocation holds the
		// task's single claimed interval; retry the next interval.
		return ClaimAttemptResult{}, fmt.Errorf("%w: %s", ErrClaimTaken, constraint)
	}
	if err != nil {
		return ClaimAttemptResult{}, fmt.Errorf("insert scan attempt: %w", err)
	}

	if err := insertAuditTx(ctx, tx, AuditRecord{
		Actor:  req.Owner,
		Action: AuditActionClaim,
		Target: map[string]any{
			"task_id":     task.TaskID,
			"attempt_id":  attemptID,
			"range_start": rangeAuditValue(start),
			"range_end":   rangeAuditValue(end),
		},
		Reason:   "scan interval claimed",
		Evidence: fmt.Sprintf("lease_ttl=%s", req.LeaseTTL),
		Result:   "claimed",
	}); err != nil {
		return ClaimAttemptResult{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return ClaimAttemptResult{}, fmt.Errorf("commit claim: %w", err)
	}
	return ClaimAttemptResult{
		AttemptID:      attemptID,
		RangeStart:     start,
		RangeEnd:       end,
		LeaseExpiresAt: leaseExpiresAt,
	}, nil
}

// planClaimEnd computes the inclusive end of one claimed interval: at most
// span positions, never past scopeEnd.
func planClaimEnd(start, scopeEnd RangeBound, span int64) (RangeBound, error) {
	if span <= 0 {
		return RangeBound{}, contractErrorf("claim span must be positive")
	}
	switch start.Kind {
	case ScopeHeight:
		end := start.Height
		if span-1 > math.MaxInt64-start.Height {
			end = math.MaxInt64
		} else {
			end = start.Height + span - 1
		}
		if end > scopeEnd.Height {
			end = scopeEnd.Height
		}
		return HeightBound(end), nil
	case ScopeTime:
		step := time.Duration(span-1) * timeScopeStep
		end := start.Time.Add(step)
		if end.After(scopeEnd.Time) {
			end = scopeEnd.Time
		}
		return TimeBound(end), nil
	}
	return RangeBound{}, contractErrorf("unknown scope kind %q", start.Kind)
}

// TaskTransitionRequest is one explicit task-state transition. Reason is
// required for paused/suspended_budget (recorded as pause_reason; Q3 records
// the pause cause) and is audit-only otherwise.
type TaskTransitionRequest struct {
	TaskID string
	To     TaskState
	Reason string
	Actor  string
}

// TaskTransitionResult reports the applied transition and the checkpoint head
// observed in the same transaction (paused/suspended visibility, Q3-5).
type TaskTransitionResult struct {
	From       TaskState
	To         TaskState
	Checkpoint *Checkpoint
}

// TransitionTask applies one task-state transition in a short transaction:
// lock recon_task, validate the transition and its guards, write the state,
// leave gap rows for pause/budget/cancel (never for done), audit, COMMIT.
// Illegal transitions (including done over open gaps or a pointer short of
// the scope end) are refused and audited, never partially applied.
func (s *Store) TransitionTask(ctx context.Context, req TaskTransitionRequest) (TaskTransitionResult, error) {
	if s == nil || s.db == nil {
		return TaskTransitionResult{}, contractErrorf("store has no database")
	}
	if strings.TrimSpace(req.TaskID) == "" {
		return TaskTransitionResult{}, contractErrorf("transition requires a task_id")
	}
	if strings.TrimSpace(req.Actor) == "" {
		return TaskTransitionResult{}, contractErrorf("transition requires an actor")
	}
	if !req.To.Valid() {
		return TaskTransitionResult{}, contractErrorf("unknown target task state %q", req.To)
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return TaskTransitionResult{}, fmt.Errorf("begin task transition: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	task, err := lockTaskTx(ctx, tx, req.TaskID)
	if err != nil {
		return TaskTransitionResult{}, err
	}
	result := TaskTransitionResult{From: task.State, To: req.To}

	guardErr := validateTaskTransition(ctx, tx, task, req)
	if guardErr != nil {
		if err := insertAuditTx(ctx, tx, AuditRecord{
			Actor:  req.Actor,
			Action: AuditActionRefuse,
			Target: taskAuditTarget(task, req.To),
			Reason: guardErr.Error(),
			Result: "refused",
		}); err != nil {
			return result, err
		}
		if err := tx.Commit(ctx); err != nil {
			return result, fmt.Errorf("commit refusal audit: %w", err)
		}
		return result, guardErr
	}

	// Pause/budget/cancel MUST leave the uncovered remainder visible; done
	// requires zero open gaps, so it never appends one.
	switch req.To {
	case TaskStatePaused, TaskStateSuspendedBudget, TaskStateCancelled:
		reason := GapPaused
		if req.To == TaskStateSuspendedBudget {
			reason = GapBudgetExhausted
		} else if req.To == TaskStateCancelled {
			reason = GapNotStarted
		}
		head, err := readCheckpointHeadTx(ctx, tx, task.TaskID, task.ScopeKind)
		if err != nil {
			return result, err
		}
		start, err := NextClaimStart(head, task.ScopeStart)
		if err != nil {
			return result, err
		}
		if err := insertGapTx(ctx, tx, task.TaskID, start, task.ScopeEnd, reason); err != nil {
			return result, err
		}
	}

	tag, err := tx.Exec(ctx, updateTaskStateSQL, task.TaskID, req.To, req.Reason, task.State)
	if err != nil {
		return result, fmt.Errorf("update task state: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return result, fmt.Errorf("%w: concurrent task state change", ErrIllegalTaskTransition)
	}

	action, auditResult := auditActionForTaskTransition(task.State, req.To)
	if err := insertAuditTx(ctx, tx, AuditRecord{
		Actor:  req.Actor,
		Action: action,
		Target: taskAuditTarget(task, req.To),
		Reason: req.Reason,
		Result: auditResult,
	}); err != nil {
		return result, err
	}

	head, err := readCheckpointHeadTx(ctx, tx, task.TaskID, task.ScopeKind)
	if err != nil {
		return result, err
	}
	result.Checkpoint = head

	if err := tx.Commit(ctx); err != nil {
		return result, fmt.Errorf("commit task transition: %w", err)
	}
	return result, nil
}

// validateTaskTransition applies the structural transition rule plus the
// state-specific guards: pause/budget require a reason; done requires the
// persisted prefix to reach the scope end and zero open gaps
// (contracts/task-lifecycle.md `done`).
func validateTaskTransition(ctx context.Context, tx pgx.Tx, task *Task, req TaskTransitionRequest) error {
	if !CanTransitionTask(task.State, req.To) {
		return fmt.Errorf("%w: %s -> %s", ErrIllegalTaskTransition, task.State, req.To)
	}
	if (req.To == TaskStatePaused || req.To == TaskStateSuspendedBudget) &&
		strings.TrimSpace(req.Reason) == "" {
		return contractErrorf("transition to %s requires a reason", req.To)
	}
	if req.To != TaskStateDone {
		return nil
	}

	open, err := openGapCountTx(ctx, tx, task.TaskID)
	if err != nil {
		return err
	}
	if open > 0 {
		return fmt.Errorf("%w: %d open gap(s)", ErrTaskNotComplete, open)
	}
	head, err := readCheckpointHeadTx(ctx, tx, task.TaskID, task.ScopeKind)
	if err != nil {
		return err
	}
	if head == nil {
		// A scope with no persisted checkpoint cannot prove coverage; an
		// empty range still needs the task row plus its checkpoint span
		// (data-model.md §1.1 Edge).
		return fmt.Errorf("%w: no checkpoint covers the scope", ErrTaskNotComplete)
	}
	if err := head.Validate(); err != nil {
		return err
	}
	cmp, err := head.ResultPersistedThrough.Compare(task.ScopeEnd)
	if err != nil {
		return err
	}
	if cmp < 0 {
		return fmt.Errorf("%w: persisted through %v, scope ends at %v",
			ErrTaskNotComplete, head.ResultPersistedThrough, task.ScopeEnd)
	}
	return nil
}

// auditActionForTaskTransition maps a legal task transition onto the closed
// recon_audit.action vocabulary (data-model.md §1.8). done/cancelled are both
// terminal closures of the task and differ only in the recorded result.
func auditActionForTaskTransition(from, to TaskState) (AuditAction, string) {
	switch to {
	case TaskStateRunning:
		if from == TaskStateCreated {
			return AuditActionStart, "started"
		}
		return AuditActionResume, "resumed"
	case TaskStatePaused:
		return AuditActionPause, "paused"
	case TaskStateSuspendedBudget:
		return AuditActionPause, "suspended_budget"
	case TaskStateDone:
		return AuditActionClose, "done"
	case TaskStateCancelled:
		return AuditActionClose, "cancelled"
	}
	return AuditActionRefuse, "refused"
}

// taskAuditTarget renders the audit target for one transition.
func taskAuditTarget(task *Task, to TaskState) map[string]any {
	return map[string]any{
		"task_id": task.TaskID,
		"from":    string(task.State),
		"to":      string(to),
	}
}

// TaskByID loads one task row for observation (no lock).
func (s *Store) TaskByID(ctx context.Context, taskID string) (*Task, error) {
	if s == nil || s.db == nil {
		return nil, contractErrorf("store has no database")
	}
	task := &Task{TaskID: taskID}
	var (
		rawStart, rawEnd     *int64
		rawStartAt, rawEndAt *time.Time
	)
	err := s.db.QueryRow(ctx, taskSelectSQL, taskID).
		Scan(&task.TaskID, &task.ScopeKind, &rawStart, &rawStartAt, &rawEnd, &rawEndAt,
			&task.State, &task.PauseReason)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: task_id %s", ErrTaskNotFound, taskID)
	}
	if err != nil {
		return nil, fmt.Errorf("load task: %w", err)
	}
	if !task.ScopeKind.Known() {
		return nil, contractErrorf("task %s has unknown scope kind %q", taskID, task.ScopeKind)
	}
	if !task.State.Valid() {
		return nil, contractErrorf("task %s has unknown state %q", taskID, task.State)
	}
	if task.ScopeStart, err = boundFromPair(task.ScopeKind, rawStart, rawStartAt); err != nil {
		return nil, err
	}
	if task.ScopeEnd, err = boundFromPair(task.ScopeKind, rawEnd, rawEndAt); err != nil {
		return nil, err
	}
	return task, nil
}

// CheckpointHead returns the latest checkpoint row of a task, or nil when the
// task has no checkpoint yet (an empty scope may legitimately have none).
func (s *Store) CheckpointHead(ctx context.Context, taskID string) (*Checkpoint, error) {
	if s == nil || s.db == nil {
		return nil, contractErrorf("store has no database")
	}
	var kind ScopeKind
	cp := &Checkpoint{TaskID: taskID}
	var (
		rawCovered, rawPersisted     *int64
		rawCoveredAt, rawPersistedAt *time.Time
	)
	err := s.db.QueryRow(ctx, checkpointHeadJoinSQL, taskID).
		Scan(&kind, &cp.Seq, &rawCovered, &rawCoveredAt, &rawPersisted, &rawPersistedAt, &cp.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load checkpoint head: %w", err)
	}
	if !kind.Known() {
		return nil, contractErrorf("task %s has unknown scope kind %q", taskID, kind)
	}
	if cp.CoveredThrough, err = boundFromPair(kind, rawCovered, rawCoveredAt); err != nil {
		return nil, err
	}
	if cp.ResultPersistedThrough, err = boundFromPair(kind, rawPersisted, rawPersistedAt); err != nil {
		return nil, err
	}
	if err := cp.Validate(); err != nil {
		return nil, err
	}
	return cp, nil
}

// OpenGapCount counts the uncovered-range rows of a task. "Fully consistent"
// and `done` both require zero open gaps (data-model.md §1.3).
func (s *Store) OpenGapCount(ctx context.Context, taskID string) (int64, error) {
	if s == nil || s.db == nil {
		return 0, contractErrorf("store has no database")
	}
	var count int64
	if err := s.db.QueryRow(ctx, countGapsSQL, taskID).Scan(&count); err != nil {
		return 0, fmt.Errorf("count gaps: %w", err)
	}
	return count, nil
}

// lockTaskTx locks one recon_task row FOR UPDATE and materializes it. It is
// the first lock of every mutating 014 helper (package lock order:
// recon_task -> recon_scan_attempt/recon_checkpoint/recon_gap).
func lockTaskTx(ctx context.Context, tx pgx.Tx, taskID string) (*Task, error) {
	task := &Task{TaskID: taskID}
	var (
		rawStart, rawEnd     *int64
		rawStartAt, rawEndAt *time.Time
	)
	err := tx.QueryRow(ctx, lockTaskSQL, taskID).
		Scan(&task.TaskID, &task.ScopeKind, &rawStart, &rawStartAt, &rawEnd, &rawEndAt,
			&task.State, &task.PauseReason)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: task_id %s", ErrTaskNotFound, taskID)
	}
	if err != nil {
		return nil, fmt.Errorf("lock task: %w", err)
	}
	if !task.ScopeKind.Known() {
		return nil, contractErrorf("task %s has unknown scope kind %q", taskID, task.ScopeKind)
	}
	if !task.State.Valid() {
		return nil, contractErrorf("task %s has unknown state %q", taskID, task.State)
	}
	if task.ScopeStart, err = boundFromPair(task.ScopeKind, rawStart, rawStartAt); err != nil {
		return nil, err
	}
	if task.ScopeEnd, err = boundFromPair(task.ScopeKind, rawEnd, rawEndAt); err != nil {
		return nil, err
	}
	return task, nil
}

// readCheckpointHeadTx reads the latest checkpoint row inside a transaction
// that already holds the task row lock. nil means "no checkpoint yet".
func readCheckpointHeadTx(ctx context.Context, tx pgx.Tx, taskID string, kind ScopeKind) (*Checkpoint, error) {
	cp := &Checkpoint{TaskID: taskID}
	var (
		rawCovered, rawPersisted     *int64
		rawCoveredAt, rawPersistedAt *time.Time
	)
	err := tx.QueryRow(ctx, checkpointHeadSQL, taskID).
		Scan(&cp.Seq, &rawCovered, &rawCoveredAt, &rawPersisted, &rawPersistedAt, &cp.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read checkpoint: %w", err)
	}
	if cp.CoveredThrough, err = boundFromPair(kind, rawCovered, rawCoveredAt); err != nil {
		return nil, err
	}
	if cp.ResultPersistedThrough, err = boundFromPair(kind, rawPersisted, rawPersistedAt); err != nil {
		return nil, err
	}
	if err := cp.Validate(); err != nil {
		return nil, err
	}
	return cp, nil
}

// openGapCountTx counts gap rows inside the caller's transaction.
func openGapCountTx(ctx context.Context, tx pgx.Tx, taskID string) (int64, error) {
	var count int64
	if err := tx.QueryRow(ctx, countGapsSQL, taskID).Scan(&count); err != nil {
		return 0, fmt.Errorf("count gaps: %w", err)
	}
	return count, nil
}

// insertGapTx appends one uncovered-range row. An empty range (start past
// end) records nothing.
func insertGapTx(ctx context.Context, tx pgx.Tx, taskID string, start, end RangeBound, reason GapReason) error {
	if !reason.Valid() {
		return contractErrorf("unknown gap reason %q", reason)
	}
	cmp, err := start.Compare(end)
	if err != nil {
		return err
	}
	if cmp > 0 {
		return nil
	}
	startHeight, startAt := rangePairArgs(start)
	endHeight, endAt := rangePairArgs(end)
	if _, err := tx.Exec(ctx, insertGapSQL,
		taskID, startHeight, endHeight, startAt, endAt, reason); err != nil {
		return fmt.Errorf("insert gap: %w", err)
	}
	return nil
}
