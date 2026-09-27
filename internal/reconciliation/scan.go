// scan.go implements the 014 task state machine and the scan compare loop:
//
//   - T006: the task lifecycle of contracts/task-lifecycle.md, the gap
//     vocabulary of data-model.md §1.3, the checkpoint pointer rule of
//     §1.2/§5, and the claim half of the claim-execute-commit protocol
//     (claim in a short transaction with `SELECT recon_task ... FOR UPDATE`,
//     no RPC inside; §5.1).
//   - T016: ScanOnce, the budgeted compare loop that builds on those
//     primitives, reads the three parties through the T013–T015 read-only
//     adapters, classifies with T017 and commits results + checkpoint in one
//     short transaction (T010).
package reconciliation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
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

// Task is the recon_task row materialized by the state machine and the T016
// compare loop. ScopeChainID, BusinessTypes, UpstreamReceipts and PolicyRefs
// feed the scan's identity scope and the read-only adapters; the budget JSONB
// stays with the T008 caller (BudgetLimits are passed per invocation).
type Task struct {
	TaskID       string
	ScopeChainID string
	ScopeKind    ScopeKind
	ScopeStart   RangeBound
	ScopeEnd     RangeBound
	State        TaskState
	PauseReason  string
	// BusinessTypes is the validated, canonical (sorted, deduplicated)
	// business-type set of the task scope.
	BusinessTypes []BusinessType
	// UpstreamReceipts is the parsed upstream_receipt_source declaration of
	// the task (empty means every business type counts as unconnected).
	UpstreamReceipts []ChainUpstreamReceiptSource
	// PolicyRefs is the raw policy_refs JSONB snapshot (confirm policy seq,
	// cutover/catalog versions); scanTaskConfirmThresholdN reads it.
	PolicyRefs []byte
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

// scanTaskRow materializes one recon_task row. It is shared by the unlocked
// read (TaskByID) and the locked read (lockTaskTx) so both see the same shape,
// including the scope identity and upstream receipt declaration the T016
// compare loop needs.
func scanTaskRow(row pgx.Row) (*Task, error) {
	task := &Task{}
	var (
		rawStart, rawEnd     *int64
		rawStartAt, rawEndAt *time.Time
		rawTypes             []string
		rawUpstream          []byte
	)
	err := row.Scan(&task.TaskID, &task.ScopeChainID, &task.ScopeKind,
		&rawStart, &rawStartAt, &rawEnd, &rawEndAt,
		&task.State, &task.PauseReason, &rawTypes, &rawUpstream, &task.PolicyRefs)
	if err != nil {
		return nil, err
	}
	if !task.ScopeKind.Known() {
		return nil, contractErrorf("task %s has unknown scope kind %q", task.TaskID, task.ScopeKind)
	}
	if !task.State.Valid() {
		return nil, contractErrorf("task %s has unknown state %q", task.TaskID, task.State)
	}
	if task.ScopeStart, err = boundFromPair(task.ScopeKind, rawStart, rawStartAt); err != nil {
		return nil, err
	}
	if task.ScopeEnd, err = boundFromPair(task.ScopeKind, rawEnd, rawEndAt); err != nil {
		return nil, err
	}
	types := make([]BusinessType, 0, len(rawTypes))
	for _, raw := range rawTypes {
		types = append(types, BusinessType(raw))
	}
	if task.BusinessTypes, err = canonicalBusinessTypes(types); err != nil {
		return nil, err
	}
	var parseErr error
	if task.UpstreamReceipts, parseErr = ParseChainUpstreamReceiptSources(rawUpstream); parseErr != nil {
		return nil, parseErr
	}
	return task, nil
}

// TaskByID loads one task row for observation (no lock).
func (s *Store) TaskByID(ctx context.Context, taskID string) (*Task, error) {
	if s == nil || s.db == nil {
		return nil, contractErrorf("store has no database")
	}
	task, err := scanTaskRow(s.db.QueryRow(ctx, taskSelectSQL, taskID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: task_id %s", ErrTaskNotFound, taskID)
	}
	if err != nil {
		return nil, fmt.Errorf("load task: %w", err)
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
	task, err := scanTaskRow(tx.QueryRow(ctx, lockTaskSQL, taskID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: task_id %s", ErrTaskNotFound, taskID)
	}
	if err != nil {
		return nil, fmt.Errorf("lock task: %w", err)
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

// ===========================================================================
// T016: ScanOnce compare loop
// ===========================================================================
//
// ScanOnce is the US1 core: one budgeted invocation claims the next interval
// of a running task (short transaction, `SELECT recon_task ... FOR UPDATE`,
// no RPC inside; data-model.md §5.1), reads the three parties outside any
// database transaction through the T013–T015 read-only adapters, classifies
// every enumerated candidate with T017, and commits the persisted results and
// the checkpoint pointer in one short transaction (T010 CommitScanBatch;
// data-model.md §5).
//
// Hard boundaries encoded here:
//
//   - Missing evidence never becomes a conclusion: an unreadable party, an
//     incomplete/stale/unknown bundle, an orphaned fact, an unconnected or
//     unavailable upstream receipt source, or an unproven freshness gate all
//     yield incomplete/pending, never consistent (FR-004/005/006/017/018).
//     The upstream receipt declaration is the one input that is not a coverage
//     gate: it is carried by Observation.Upstream (ExternalCredit) exactly as
//     the T017 classifier owns it, so an unconnected upstream keeps the
//     external-credit dimension unverified and still forbids a consistent
//     verdict, but it never masks a fully proven local fact (a chain present
//     without its business row, an absent chain fact, a divergence). See
//     ChainFactsBundle.CoverageClosed / EventStateEvidence.CoverageClosed.
//   - Height-scoped intervals resolve their chain-time window
//     (OccurredFrom/To) from chain block times before the event read (endpoint
//     heights exact, header second precision, right seam closed at the next
//     height), so the event adapter can attribute blockless business-object
//     rows without guessing; an unprovable mapping is an explicit gap, an
//     unclosable right seam is an explicit gap declaration, and a
//     reorg-invalidated mapping stays pending. A height interval without the
//     resolved window keeps the event party incomplete, never an absence.
//   - Legal absorbed duplicates (Q4) create no ticket: metrics/audit only.
//   - Divergences dedup by stable identity: the same identity reuses the
//     original ticket row and appends a discrepancy_occurrence evidence row
//     (FR-007); a new identity creates one ticket. Reopen/invalidation is the
//     lifecycle owner's job (T026) and is deliberately not decided here.
//   - Budget exhaustion suspends observably: new work stops, the in-flight
//     attempt is settled boundedly, a gap row and a suspended_budget task
//     transition (014 tasks only) record the uncovered remainder; there is no
//     busy loop and no unbounded retry (FR-019, Q3).
//   - A crash/cancel/timeout between claim and commit advances nothing: the
//     pointer only moves inside CommitScanBatch over the contiguous persisted
//     prefix.
//   - Only 014-owned tables are written. No recovery, replay/unblock or
//     payment path is ever invoked or triggered (FR-014/015/023).
//   - Owner is the authenticated principal for attempt ownership and audit
//     only. operator/reason/operation_id text never authorizes anything here;
//     authorization happens before ScanOnce (authz.go, T009).
//
// Enumeration is the one caller-supplied seam: ScanCandidateSource lists the
// business identities of an interval and owns enumeration completeness. An
// empty candidate list is taken as "nothing to compare in this interval" and
// is never used to hide an unwired adapter; an enumeration error is committed
// with a query_failed gap so the range can never close silently.

// ScanInterval is one inclusive interval claimed from the task pointer.
type ScanInterval struct {
	From RangeBound
	To   RangeBound
}

// Validate checks the interval shape conservatively.
func (i ScanInterval) Validate() error {
	if !i.From.Valid() || !i.To.Valid() {
		return contractErrorf("scan interval has an invalid range bound")
	}
	if i.From.Kind != i.To.Kind {
		return contractErrorf("scan interval kinds differ: %q vs %q", i.From.Kind, i.To.Kind)
	}
	cmp, err := i.From.Compare(i.To)
	if err != nil {
		return err
	}
	if cmp > 0 {
		return contractErrorf("scan interval is not ascending")
	}
	return nil
}

// ChainFactRef names one candidate's chain-side fact inside the claimed
// interval. BlockNumber (+ optional BlockHash) anchors a canonical block;
// TxHash (+ optional LogIndex) anchors indexed transfer-log facts. An empty
// ref (Declared() == false) cannot attribute the chain party: it stays
// unknown/pending instead of becoming a guess.
type ChainFactRef struct {
	BlockNumber int64
	BlockHash   string
	TxHash      string
	LogIndex    *int64
}

// Declared reports whether the ref names any chain-side fact.
func (r ChainFactRef) Declared() bool {
	return r.BlockNumber > 0 || strings.TrimSpace(r.TxHash) != ""
}

// ScanCandidate is one business identity to reconcile in a claimed interval.
type ScanCandidate struct {
	// BusinessType must belong to the task scope's closed business-type set.
	BusinessType BusinessType
	// BusinessKey is the stable business identity of the candidate.
	BusinessKey BusinessKey
	// ChainFact names the candidate's chain-side fact; an empty ref keeps
	// the chain party unknown (never absent-by-guess).
	ChainFact ChainFactRef
	// Members carries the member log facts of a tx-aggregate candidate
	// (T035): they are enumerated in the occurrence evidence reference and
	// never split the tx into several tickets.
	Members []TxAggregateMember
	// EventKey optionally overrides the event identity matched against the
	// interval's event evidence; empty means BusinessKey is used.
	EventKey BusinessKey
	// Mismatch is the caller-computed verdict of the present required
	// parties (their facts were compared and disagree). It is never inferred
	// from availability alone.
	Mismatch bool
	// PGDuplicate carries the PG-side repeated business-effect facts
	// (repeated effect / repeated withdrawal intent) that only the PG side
	// can evidence; the event adapter supplies the delivery-side facts.
	PGDuplicate DuplicateEvidence
	// EvidenceRef optionally references the evidence bundle for occurrence
	// rows (bounded to 512 bytes); an empty or oversized value is replaced by
	// a synthesized scan reference.
	EvidenceRef string
}

// ScanEnumeration is one interval's enumeration result: the business
// candidates, the coverage reasons the enumerator could not prove (committed
// as gap rows, never silently dropped), and the count of chain facts the
// enumerator proved outside the project attribution (metrics-only; T033).
type ScanEnumeration struct {
	Candidates []ScanCandidate
	// GapReasons are unproven-coverage reasons for the interval. Every reason
	// must belong to the closed gap vocabulary; an unknown reason is a wiring
	// defect and fails the invocation.
	GapReasons []GapReason
	// MetricsOnly counts chain facts proven not to belong to the project
	// (attribution decided and negative). They are metrics/audit-only and
	// MUST NOT become tickets.
	MetricsOnly int
}

// ScanCandidateSource enumerates the business identities of one claimed
// interval. An enumeration error is evidence-missing: the interval is
// committed with a query_failed gap (never consistent), never silently
// skipped.
type ScanCandidateSource interface {
	ScanCandidates(ctx context.Context, interval ScanInterval) ([]ScanCandidate, error)
}

// ScanCandidateEnumerator is the enumeration seam of one scan invocation:
// implemented by ScanCandidateSource (legacy per-call charge), by
// BudgetedScanCandidateSource (per-query charging, T038), or by both. The
// compare loop refuses an enumerator that implements neither.
type ScanCandidateEnumerator interface{}

// BudgetedScanCandidateSource is the T038 enumeration seam: the enumerator
// charges every internal read through the budget callback as it executes, and
// reports the coverage it could not prove. It takes precedence over the legacy
// ScanCandidateSource when a caller-implemented source satisfies both. A
// returned ErrBudgetExhausted aborts the scan boundedly (gap + honest
// checkpoint); it is never converted into a silent empty enumeration.
type BudgetedScanCandidateSource interface {
	EnumerateCandidates(ctx context.Context, interval ScanInterval, budget ScanQueryBudget) (ScanEnumeration, error)
}

// ScanChainWindowResolver optionally resolves the block-height window of a
// claimed interval for the chain-facts adapter. A height-scoped interval
// resolves locally; a time-scoped task needs this resolver because the local
// index stores no chain timestamps. An unresolved window leaves the chain
// party unknown/pending, never consistent.
type ScanChainWindowResolver interface {
	ResolveChainWindow(ctx context.Context, interval ScanInterval) (from, to int64, ok bool, err error)
}

// The read-only adapter surfaces ScanOnce consumes. The T013–T015 adapters
// satisfy them; bounded fakes may substitute in tests.
type (
	// ChainFactsReader is the T013 chain-facts surface.
	ChainFactsReader interface {
		Observe(ctx context.Context, q ChainFactsQuery) (ChainFactsBundle, error)
	}
	// PGStateReader is the T014 PG-state surface.
	PGStateReader interface {
		Read(ctx context.Context, req PGReadRequest) (PGStateRecord, error)
	}
	// EventStateReader is the T015 event-delivery surface.
	EventStateReader interface {
		Observe(ctx context.Context, q EventStateQuery) (EventStateEvidence, error)
	}
)

// ScanSources groups the three read-only adapters. A nil adapter leaves its
// party unknown/pending; it is never read as permission to conclude.
type ScanSources struct {
	Chain  ChainFactsReader
	PG     PGStateReader
	Events EventStateReader
	// Obligations is the T040 read-only expectation-carrier surface
	// (event_obligation, migration 000017). A nil reader leaves every
	// decisive event-only absence R3 pending: the discriminator never
	// guesses an obligation from missing evidence and never upgrades 标记缺席
	// into N/A.
	Obligations EventObligationReader
}

// ScanOnceRequest is one budgeted scan invocation.
type ScanOnceRequest struct {
	TaskID string
	// Owner is the authenticated principal bound by the caller before the
	// call: attempt ownership + audit actor, never an authorization source.
	Owner string
	// LeaseTTL bounds the claimed attempt lease; a crashed invocation's
	// attempt is abandoned after expiry (RecoverStaleAttempts).
	LeaseTTL time.Duration
	// Limits are the hard budget bounds; every field must be positive.
	Limits BudgetLimits
	// FreshnessTolerance is the classification freshness window. Zero keeps
	// every conclusion pending (freshness cannot be proven; fail-closed).
	FreshnessTolerance time.Duration
	// Sources are the three read-only adapters.
	Sources ScanSources
	// Candidates enumerates the per-interval business identities. A source
	// that implements BudgetedScanCandidateSource charges every internal read
	// through the budget seam; a legacy source is charged one PG per call (or
	// its declared proven statement cap). An enumerator implementing neither
	// interface is refused.
	Candidates ScanCandidateEnumerator
	// WindowResolver resolves time-scoped intervals to height windows (T036).
	// Nil keeps time scopes pending-by-design (chain party unknown, explicit
	// gap from the pending classification): an unmapped range is never read as
	// proof that no chain fact exists.
	WindowResolver ScanTimeWindowResolver
	// HeightWindowResolver resolves height-scoped intervals to the chain-time
	// window (OccurredFrom/To) the event adapter needs to attribute blockless
	// business-object rows: endpoint heights exact, header second precision,
	// right seam closed at the next height. Nil keeps height scopes
	// pending-by-design (the event adapter refuses to cover blockless rows):
	// an unresolved event-time coverage is never an absence claim.
	HeightWindowResolver ScanHeightTimeWindowResolver
	// EventConsumers/EventQuarantine are passed through to the event
	// adapter. The event adapter requires a quarantine reader whenever
	// consumers are registered; ScanOnce refuses the mismatched shape up
	// front instead of reading a poisoned event as "not yet consumed".
	EventConsumers  []EventConsumerRegistration
	EventQuarantine EventQuarantineReader
}

// ScanStop names why an invocation stopped claiming intervals.
type ScanStop string

// The recognized stop reasons. The zero value means "not stopped" (the loop
// only exits through one of the values below).
const (
	ScanStopScopeExhausted   ScanStop = "scope_exhausted"
	ScanStopTaskNotRunning   ScanStop = "task_not_running"
	ScanStopClaimHeld        ScanStop = "claim_held"
	ScanStopAttemptDiscarded ScanStop = "attempt_discarded"
)

// ScanOnceResult is the observable outcome of one invocation. Suspended is
// true when the budget stopped new work; the task was moved to
// suspended_budget (unless an operator already paused/cancelled it) and a gap
// row records the uncovered remainder.
type ScanOnceResult struct {
	TaskID      string
	Attempts    int
	Committed   int
	Discarded   int
	Candidates  int
	Tickets     int
	Merged      int
	Occurrences int
	Pending     int
	Absorbed    int
	// Unattributed counts chain facts the enumerator proved outside the
	// project address/asset attribution: metrics-only, never tickets (T033).
	Unattributed int
	// WindowUnmappable/WindowInvalidated count intervals whose chain-time
	// mapping could not be proven (explicit gap) or was reorg-invalidated
	// (pending): the T036 time→height mapping for time scopes and the
	// height→time event window for height scopes. Both stay pending
	// observations.
	WindowUnmappable  int
	WindowInvalidated int
	Gaps              int
	Stop              ScanStop
	StoppedState      TaskState
	DiscardReason     string
	LastCheckpoint    *Checkpoint
	Suspended         bool
	SuspendResource   BudgetResource
	SuspendDetail     string
	BudgetUsage       BudgetUsage
}

// ScanOnce runs one budgeted scan invocation (T016). It never panics on
// malformed input: every shape violation is a typed refusal.
func (s *Store) ScanOnce(ctx context.Context, req ScanOnceRequest) (result ScanOnceResult, err error) {
	result = ScanOnceResult{TaskID: req.TaskID}
	if s == nil || s.db == nil {
		return result, contractErrorf("store has no database")
	}
	if strings.TrimSpace(req.TaskID) == "" {
		return result, contractErrorf("scan requires a task_id")
	}
	if strings.TrimSpace(req.Owner) == "" {
		return result, contractErrorf("scan requires an attempt owner (authenticated principal)")
	}
	if req.LeaseTTL <= 0 {
		return result, contractErrorf("scan lease ttl must be positive")
	}
	if err := req.Limits.Validate(); err != nil {
		return result, err
	}
	if req.Candidates == nil {
		return result, contractErrorf("scan requires a candidate source")
	}
	if _, legacy := req.Candidates.(ScanCandidateSource); !legacy {
		if _, budgeted := req.Candidates.(BudgetedScanCandidateSource); !budgeted {
			return result, contractErrorf("candidate enumerator implements neither ScanCandidateSource nor BudgetedScanCandidateSource")
		}
	}
	if len(req.EventConsumers) > 0 && req.EventQuarantine == nil {
		return result, contractErrorf("event consumers require a quarantine reader")
	}

	task, err := s.TaskByID(ctx, req.TaskID)
	if err != nil {
		return result, err
	}
	if task.State != TaskStateRunning {
		result.Stop = ScanStopTaskNotRunning
		result.StoppedState = task.State
		return result, fmt.Errorf("%w: state=%s", ErrTaskNotRunning, task.State)
	}
	scope, err := scanTaskIdentityScope(task)
	if err != nil {
		return result, err
	}

	budget, err := NewBudget(req.Limits)
	if err != nil {
		return result, err
	}
	defer func() { result.BudgetUsage = budget.Usage() }()

	// Crash-safe resume: expired claims from a crashed invocation are
	// abandoned (gap + audit, pointer untouched) before new work is claimed.
	if err := budget.ConsumePG(ctx, 1); err != nil {
		if errors.Is(err, ErrBudgetExhausted) {
			return s.suspendScanOnce(ctx, &req, budget, result, false)
		}
		return result, err
	}
	if _, err := s.RecoverStaleAttempts(ctx, RecoverStaleAttemptsRequest{
		TaskID: req.TaskID,
		Limit:  scanPositiveLimit(req.Limits.MaxConcurrency),
		Actor:  req.Owner,
	}); err != nil {
		return result, err
	}

	for {
		if err := ctx.Err(); err != nil {
			// Interrupted between intervals: nothing in flight, nothing
			// committed, the pointer is exactly where the last commit left it.
			return result, err
		}
		release, err := budget.Acquire(ctx)
		if err != nil {
			if errors.Is(err, ErrBudgetExhausted) {
				return s.suspendScanOnce(ctx, &req, budget, result, false)
			}
			return result, err
		}

		if err := budget.ConsumePG(ctx, 1); err != nil {
			release()
			if errors.Is(err, ErrBudgetExhausted) {
				return s.suspendScanOnce(ctx, &req, budget, result, false)
			}
			return result, err
		}
		claim, claimErr := s.ClaimScanAttempt(ctx, ClaimAttemptRequest{
			TaskID:   req.TaskID,
			Owner:    req.Owner,
			Span:     req.Limits.MaxSpanPerClaim,
			LeaseTTL: req.LeaseTTL,
		})
		if claimErr != nil {
			release()
			switch {
			case errors.Is(claimErr, ErrScopeExhausted):
				// Scope fully persisted: no claimable interval remains. The
				// task is not auto-closed here (closing is an operator action
				// that requires zero open gaps).
				result.Stop = ScanStopScopeExhausted
				return result, nil
			case errors.Is(claimErr, ErrTaskNotRunning):
				result.Stop = ScanStopTaskNotRunning
				if current, loadErr := s.TaskByID(ctx, req.TaskID); loadErr == nil {
					result.StoppedState = current.State
				}
				return result, nil
			case errors.Is(claimErr, ErrClaimTaken):
				// Another invocation holds the task's single claimed
				// interval; this one did no work and must not spin.
				result.Stop = ScanStopClaimHeld
				return result, nil
			default:
				return result, claimErr
			}
		}
		result.Attempts++

		interval := ScanInterval{From: claim.RangeStart, To: claim.RangeEnd}
		outcome, compareErr := compareScanInterval(ctx, &req, task, scope, interval, budget)
		if compareErr != nil {
			release()
			if errors.Is(compareErr, ErrBudgetExhausted) {
				// The attempt is still claimed: settle it (gap, no pointer
				// move) and suspend the task observably.
				return s.suspendScanOnce(ctx, &req, budget, result, true)
			}
			// ctx/contract errors: the attempt stays claimed until its lease
			// expires (RecoverStaleAttempts); nothing was committed.
			return result, compareErr
		}
		outcome.taskID = req.TaskID
		outcome.attemptID = claim.AttemptID
		outcome.owner = req.Owner

		if err := budget.ConsumePG(ctx, 1); err != nil {
			release()
			if errors.Is(err, ErrBudgetExhausted) {
				return s.suspendScanOnce(ctx, &req, budget, result, true)
			}
			return result, err
		}
		commit, commitErr := s.CommitScanBatch(ctx, CommitScanBatchRequest{
			TaskID:         req.TaskID,
			AttemptID:      claim.AttemptID,
			PersistResults: outcome.persist,
		})
		release()
		switch {
		case commitErr == nil:
			if commit.Checkpoint != nil {
				result.LastCheckpoint = commit.Checkpoint
			}
			result.Committed++
			outcome.applyTo(&result)
		case errors.Is(commitErr, ErrAttemptDiscarded):
			// Late submitter: audited inside the discard transaction, pointer
			// untouched, results dropped. Stop instead of racing the next
			// interval.
			result.Discarded++
			result.DiscardReason = commit.DiscardReason
			result.Stop = ScanStopAttemptDiscarded
			if current, loadErr := s.TaskByID(ctx, req.TaskID); loadErr == nil {
				result.StoppedState = current.State
			}
			return result, nil
		default:
			return result, commitErr
		}
	}
}

// scanTaskIdentityScope builds the stable identity scope of a task from its
// recorded scope (chain + kind + inclusive bounds + business types).
func scanTaskIdentityScope(task *Task) (IdentityScope, error) {
	if task == nil {
		return IdentityScope{}, contractErrorf("scan requires a task")
	}
	types, err := canonicalBusinessTypes(task.BusinessTypes)
	if err != nil {
		return IdentityScope{}, err
	}
	scope := IdentityScope{
		ChainID:       task.ScopeChainID,
		Kind:          task.ScopeKind,
		BusinessTypes: types,
	}
	switch task.ScopeKind {
	case ScopeHeight:
		scope.From = task.ScopeStart.Height
		scope.To = task.ScopeEnd.Height
	case ScopeTime:
		scope.From = task.ScopeStart.Time.UnixMicro()
		scope.To = task.ScopeEnd.Time.UnixMicro()
	default:
		return IdentityScope{}, contractErrorf("task %s has unknown scope kind %q", task.TaskID, task.ScopeKind)
	}
	if err := scope.Validate(); err != nil {
		return IdentityScope{}, err
	}
	return scope, nil
}

// scanPositiveLimit returns v, or 1 when v is non-positive. It is used for
// bounded settle/recovery limits that must be positive.
func scanPositiveLimit(v int) int {
	if v < 1 {
		return 1
	}
	return v
}

// scanNumericChainID converts the task's chain identity into the numeric
// chain id the adapters require. A non-numeric chain identity cannot be read
// by the current adapters and fails closed (parties stay unknown/pending).
func scanNumericChainID(task *Task) (int64, bool) {
	if task == nil {
		return 0, false
	}
	id, err := strconv.ParseInt(strings.TrimSpace(task.ScopeChainID), 10, 64)
	if err != nil || id <= 0 {
		return 0, false
	}
	return id, true
}

// scanTaskConfirmThresholdN reads the confirm policy depth N from the task's
// policy_refs snapshot. A missing/malformed value is 0: the chain evidence is
// then incomplete and can never support a consistent verdict.
func scanTaskConfirmThresholdN(task *Task) uint64 {
	if task == nil || len(task.PolicyRefs) == 0 {
		return 0
	}
	var refs struct {
		ConfirmThresholdN uint64 `json:"confirm_threshold_n"`
	}
	if err := json.Unmarshal(task.PolicyRefs, &refs); err != nil {
		return 0
	}
	return refs.ConfirmThresholdN
}

// scanContextError returns err when it is a context cancellation/deadline,
// nil otherwise. It lets the compare loop treat adapter read failures as
// evidence-unknown while still aborting promptly on interruption.
func scanContextError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return err
	default:
		return nil
	}
}

// scanEnumerateCandidates runs the caller's enumeration seam under the T038
// accounting rules: a BudgetedScanCandidateSource charges each internal read
// through the budget callback as it executes; a legacy source is charged its
// declared proven statement cap (conservative worst case, charged before the
// call), or one PG when no cap is declared. Post-hoc accounting never happens.
func scanEnumerateCandidates(ctx context.Context, req *ScanOnceRequest, interval ScanInterval, budget *Budget) (ScanEnumeration, error) {
	if source, ok := req.Candidates.(BudgetedScanCandidateSource); ok && source != nil {
		return source.EnumerateCandidates(ctx, interval, budget)
	}
	if capSource, ok := req.Candidates.(ScanStatementCapSource); ok && capSource != nil {
		if err := capSource.ScanStatementCap().Charge(ctx, budget); err != nil {
			return ScanEnumeration{}, err
		}
	} else if err := budget.ConsumePG(ctx, 1); err != nil {
		return ScanEnumeration{}, err
	}
	source, ok := req.Candidates.(ScanCandidateSource)
	if !ok || source == nil {
		return ScanEnumeration{}, contractErrorf("candidate enumerator implements no enumeration interface")
	}
	candidates, err := source.ScanCandidates(ctx, interval)
	if err != nil {
		return ScanEnumeration{}, err
	}
	return ScanEnumeration{Candidates: candidates}, nil
}

// chainWindowResult is the outcome of resolving one claimed interval's chain
// window. resolved means from..to is observable; otherwise the chain party
// stays unknown and the status/reason are recorded (unmappable => explicit
// gap, invalidated => pending reverify; T036/FR-017).
type chainWindowResult struct {
	from     int64
	to       int64
	resolved bool
	status   WindowStatus
	reason   string
	gap      bool
}

// scanChainWindow resolves the height window of one claimed interval for the
// chain-facts adapter. Height scopes resolve locally; time scopes need the
// T036 ScanTimeWindowResolver (the local index stores no chain timestamps). A
// legacy ScanChainWindowResolver is still honored for compatibility; without
// either seam the window stays unresolved (chain party unknown, pending).
func scanChainWindow(ctx context.Context, req *ScanOnceRequest, task *Task, interval ScanInterval, budget *Budget) (chainWindowResult, error) {
	if req.Sources.Chain == nil {
		return chainWindowResult{}, nil
	}
	if interval.From.Kind == ScopeHeight {
		return chainWindowResult{from: interval.From.Height, to: interval.To.Height, resolved: true}, nil
	}
	chainID, ok := scanNumericChainID(task)
	if !ok {
		// A symbolic chain identity cannot be read by the resolver; the time
		// scope stays pending-by-design (no absence claim).
		return chainWindowResult{}, nil
	}
	if req.WindowResolver != nil {
		resolution, err := req.WindowResolver.ResolveScanWindow(ctx, chainID, interval, budget)
		if err != nil {
			return chainWindowResult{}, err
		}
		if err := resolution.Validate(); err != nil {
			return chainWindowResult{}, err
		}
		switch resolution.Status {
		case WindowResolved:
			return chainWindowResult{from: resolution.From, to: resolution.To, resolved: true}, nil
		default:
			return chainWindowResult{
				status: resolution.Status,
				reason: resolution.Reason,
				gap:    resolution.Status == WindowUnmappable,
			}, nil
		}
	}
	resolver, ok := req.Candidates.(ScanChainWindowResolver)
	if !ok || resolver == nil {
		return chainWindowResult{}, nil
	}
	from, to, resolved, err := resolver.ResolveChainWindow(ctx, interval)
	if err != nil {
		if ctxErr := scanContextError(err); ctxErr != nil {
			return chainWindowResult{}, ctxErr
		}
		return chainWindowResult{}, nil
	}
	if !resolved || from < 0 || to < from {
		return chainWindowResult{}, nil
	}
	return chainWindowResult{from: from, to: to, resolved: true}, nil
}

// eventTimeWindowResult is the outcome of resolving one height interval's
// chain-time window for the event adapter's OccurredFrom/To boundary. from/to
// are set only when the window was proven; gap marks an unmappable resolution
// (explicit uncovered range); boundaryUnproven marks a resolved window whose
// right seam to the next height could not be closed (explicit gap, the window
// itself still usable); status/reason record the outcome for the invocation
// counters and audit evidence.
type eventTimeWindowResult struct {
	from             *time.Time
	to               *time.Time
	gap              bool
	boundaryUnproven bool
	status           WindowStatus
	reason           string
}

// scanEventTimeWindow resolves the chain-time window of a height-scoped
// interval so the event adapter can attribute blockless business-object rows
// without guessing. It is a no-op for time-scoped intervals (their own
// occurred_at window already is the query boundary), without an event adapter,
// and without the T036 height window seam — in those cases the event adapter
// keeps the interval incomplete instead of the compare loop fabricating
// coverage. The resolution comes from chain block times with canonical
// endpoint verification (reorg => invalidated/pending, unprovable =>
// unmappable/gap); every mapping query is charged to the budget as it
// executes.
func scanEventTimeWindow(ctx context.Context, req *ScanOnceRequest, task *Task,
	interval ScanInterval, budget *Budget) (eventTimeWindowResult, error) {
	if req.Sources.Events == nil || interval.From.Kind != ScopeHeight {
		return eventTimeWindowResult{}, nil
	}
	if req.HeightWindowResolver == nil {
		return eventTimeWindowResult{}, nil
	}
	chainID, ok := scanNumericChainID(task)
	if !ok {
		// A symbolic chain identity cannot be read by the resolver; the event
		// window stays unresolved (pending-by-design, no absence claim).
		return eventTimeWindowResult{}, nil
	}
	resolution, err := req.HeightWindowResolver.ResolveHeightTimeWindow(ctx, chainID, interval, budget)
	if err != nil {
		return eventTimeWindowResult{}, err
	}
	if err := resolution.Validate(); err != nil {
		return eventTimeWindowResult{}, err
	}
	switch resolution.Status {
	case WindowResolved:
		from, to := resolution.From.UTC(), resolution.To.UTC()
		return eventTimeWindowResult{
			from:             &from,
			to:               &to,
			boundaryUnproven: resolution.BoundaryUnproven,
			reason:           resolution.Reason,
		}, nil
	case WindowUnmappable:
		// Unproven chain-time coverage is an explicit uncovered range: the
		// event party stays unknown and the interval cannot close.
		return eventTimeWindowResult{gap: true, status: resolution.Status, reason: resolution.Reason}, nil
	default:
		// Reorg-invalidated mapping: pending reverify only (FR-017).
		return eventTimeWindowResult{status: resolution.Status, reason: resolution.Reason}, nil
	}
}

// detectionIntervalOf projects one claimed scan interval onto the persisted
// detection interval metadata (the re-verification window).
func detectionIntervalOf(interval ScanInterval) *DetectionInterval {
	detection := &DetectionInterval{Kind: interval.From.Kind}
	switch interval.From.Kind {
	case ScopeHeight:
		detection.From, detection.To = interval.From.Height, interval.To.Height
	case ScopeTime:
		detection.From, detection.To = interval.From.Time.UnixMicro(), interval.To.Time.UnixMicro()
	}
	return detection
}

// ScanInterval projects a persisted detection interval back onto a scan
// interval; invalid shapes are refused.
func (d DetectionInterval) ScanInterval() (ScanInterval, error) {
	if err := d.Validate(); err != nil {
		return ScanInterval{}, err
	}
	interval := ScanInterval{}
	switch d.Kind {
	case ScopeHeight:
		interval.From, interval.To = HeightBound(d.From), HeightBound(d.To)
	case ScopeTime:
		interval.From, interval.To = TimeBound(time.UnixMicro(d.From)), TimeBound(time.UnixMicro(d.To))
	}
	return interval, nil
}

// scanIntervalOutcome accumulates one interval's classifications plus the
// bounded side-effect counters that are applied to the invocation result only
// after the commit transaction succeeds.
type scanIntervalOutcome struct {
	taskID    string
	attemptID string
	owner     string
	interval  ScanInterval
	scope     IdentityScope

	classifications []Classification
	gapReasons      []GapReason
	pending         int
	absorbed        int
	candidates      int
	metricsOnly     int

	windowUnmappable  int
	windowInvalidated int

	tickets     int
	merged      int
	occurrences int
	gaps        int
}

// addGapReason records one distinct uncovered-range reason for the interval.
func (o *scanIntervalOutcome) addGapReason(reason GapReason) {
	for _, existing := range o.gapReasons {
		if existing == reason {
			return
		}
	}
	o.gapReasons = append(o.gapReasons, reason)
}

// applyTo copies the committed side effects onto the invocation result.
func (o *scanIntervalOutcome) applyTo(result *ScanOnceResult) {
	result.Candidates += o.candidates
	result.Tickets += o.tickets
	result.Merged += o.merged
	result.Occurrences += o.occurrences
	result.Pending += o.pending
	result.Absorbed += o.absorbed
	result.Unattributed += o.metricsOnly
	result.WindowUnmappable += o.windowUnmappable
	result.WindowInvalidated += o.windowInvalidated
	result.Gaps += o.gaps
}

// compareScanInterval reads the three parties for one claimed interval and
// classifies every enumerated candidate. It performs no database transaction
// and no write: all reads happen outside the claim/commit transactions
// (data-model.md §5.1).
func compareScanInterval(ctx context.Context, req *ScanOnceRequest, task *Task, scope IdentityScope,
	interval ScanInterval, budget *Budget) (*scanIntervalOutcome, error) {
	if err := interval.Validate(); err != nil {
		return nil, err
	}
	outcome := &scanIntervalOutcome{interval: interval, scope: scope}
	now := time.Now().UTC()

	chainID, chainIDOK := scanNumericChainID(task)
	window, err := scanChainWindow(ctx, req, task, interval, budget)
	if err != nil {
		return nil, err
	}
	if window.gap {
		// An unmappable time window is an explicit uncovered range: the chain
		// party stays unknown and the interval cannot close (T036/FR-003).
		outcome.addGapReason(GapQueryFailed)
	}
	switch window.status {
	case WindowUnmappable:
		outcome.windowUnmappable++
	case WindowInvalidated:
		// The mapping was reorg-invalidated: the observation stays pending
		// (FR-017), never consistent.
		outcome.windowInvalidated++
	}
	from, to, haveWindow := window.from, window.to, window.resolved

	// Event-time window (height scopes only): the chain-time window of the
	// claimed interval, resolved from chain block times and closed at the next
	// height so adjacent intervals tile without a seam. Without it the event
	// adapter refuses to cover blockless business-object rows, so a chain-first
	// fact could never become a ticket on a height scan.
	eventWindow, err := scanEventTimeWindow(ctx, req, task, interval, budget)
	if err != nil {
		return nil, err
	}
	if eventWindow.gap {
		// An unmappable event-time window is an explicit uncovered range: the
		// event party stays unknown and the interval cannot close (T036).
		outcome.addGapReason(GapQueryFailed)
	}
	if eventWindow.boundaryUnproven {
		// The window is usable but its right seam to the next height could not
		// be closed: the interval must carry a visible gap so no following
		// interval can close the seam silently (fail-closed no-miss rule).
		outcome.addGapReason(GapQueryFailed)
	}
	switch eventWindow.status {
	case WindowUnmappable:
		outcome.windowUnmappable++
	case WindowInvalidated:
		// The window mapping was reorg-invalidated: the observation stays
		// pending (FR-017), never consistent.
		outcome.windowInvalidated++
	}

	// Chain facts (one interval-level read).
	var (
		chainBundle *ChainFactsBundle
		chainUsable bool
	)
	if req.Sources.Chain != nil && chainIDOK && haveWindow {
		if err := budget.ConsumePG(ctx, 1); err != nil {
			return nil, err
		}
		if err := budget.ConsumeRPC(ctx, 1); err != nil {
			return nil, err
		}
		bundle, observeErr := req.Sources.Chain.Observe(ctx, ChainFactsQuery{
			ChainID:           chainID,
			From:              from,
			To:                to,
			ConfirmThresholdN: scanTaskConfirmThresholdN(task),
			// This compare-loop read is the coverage-bearing chain read and
			// pins NeedTransferLogs=true: only then does the adapter run its
			// log-stream coverage checks, so CoverageClosed can never read a
			// header-only bundle (empty Logs) as "no transfer happened".
			NeedTransferLogs: true,
			UpstreamReceipts: append([]ChainUpstreamReceiptSource(nil), task.UpstreamReceipts...),
		})
		if ctxErr := scanContextError(observeErr); ctxErr != nil {
			return nil, ctxErr
		}
		chainBundle = &bundle
		// CoverageClosed, not CanSupportConsistent: an unconnected upstream
		// receipt declaration downgrades the bundle status (FR-006) but is the
		// classifier's upstream-credit input, not a chain-coverage defect.
		// Missing evidence still never becomes a conclusion: every non-upstream
		// downgrade keeps this false.
		chainUsable = observeErr == nil && bundle.CoverageClosed()
	}

	// Event delivery evidence (one interval-level read).
	var (
		eventEvidence *EventStateEvidence
		eventUsable   bool
	)
	if req.Sources.Events != nil {
		if err := budget.ConsumePG(ctx, 1); err != nil {
			return nil, err
		}
		evidence, observeErr := req.Sources.Events.Observe(ctx, EventStateQuery{
			Scope:            scope,
			Interval:         EventStateInterval{From: interval.From, To: interval.To},
			OccurredFrom:     eventWindow.from,
			OccurredTo:       eventWindow.to,
			Consumers:        append([]EventConsumerRegistration(nil), req.EventConsumers...),
			Quarantine:       req.EventQuarantine,
			UpstreamReceipts: append([]ChainUpstreamReceiptSource(nil), task.UpstreamReceipts...),
		})
		if ctxErr := scanContextError(observeErr); ctxErr != nil {
			return nil, ctxErr
		}
		eventEvidence = &evidence
		// CoverageClosed, not CanSupportConsistent: the upstream receipt
		// declaration is carried by Observation.Upstream/ExternalCredit;
		// every delivery-coverage downgrade (truncation, unclosed delivery,
		// quarantine, missing time window, read failure) keeps this false.
		eventUsable = observeErr == nil && evidence.CoverageClosed()
	}

	// Candidate enumeration. Its error is evidence-missing: the interval is
	// committed with a query_failed gap so it can never close silently. A
	// budgeted source charges every internal read itself (T038); a legacy
	// source is charged its declared proven statement cap, or one PG by
	// default. An ErrBudgetExhausted charge aborts the scan boundedly instead
	// of being mistaken for a failed enumeration.
	enumeration, enumerateErr := scanEnumerateCandidates(ctx, req, interval, budget)
	if errors.Is(enumerateErr, ErrBudgetExhausted) {
		return nil, enumerateErr
	}
	if ctxErr := scanContextError(enumerateErr); ctxErr != nil {
		return nil, ctxErr
	}
	if enumerateErr != nil {
		outcome.pending++
		outcome.addGapReason(GapQueryFailed)
		return outcome, nil
	}
	for _, reason := range enumeration.GapReasons {
		if !reason.Valid() {
			return nil, contractErrorf("candidate enumeration reported unknown gap reason %q", reason)
		}
		outcome.addGapReason(reason)
	}
	outcome.metricsOnly += enumeration.MetricsOnly
	candidates := enumeration.Candidates
	outcome.candidates = len(candidates)

	// T040 expectation evidence: one bounded interval-level read over the
	// catalog event aggregates the candidates name. The read is charged two
	// PG statements (markers + audited retention history). A nil reader or a
	// failed read leaves the evidence unusable, and every decisive event-only
	// absence stays R3 pending with a visible uncovered range — missing
	// evidence is never "no obligation".
	var obligationEvidence EventObligationEvidence
	if req.Sources.Obligations != nil {
		aggregates := make([]EventObligationAggregate, 0, len(candidates))
		for i := range candidates {
			eventKey := candidates[i].EventKey
			if strings.TrimSpace(eventKey.Value) == "" {
				eventKey = candidates[i].BusinessKey
			}
			if aggregate, ok := EventObligationAggregateOf(eventKey); ok {
				aggregates = append(aggregates, aggregate)
			}
		}
		if len(aggregates) > 0 {
			if err := budget.ConsumePG(ctx, 2); err != nil {
				return nil, err
			}
			evidence, readErr := req.Sources.Obligations.ReadObligations(ctx,
				EventObligationQuery{Aggregates: aggregates})
			if ctxErr := scanContextError(readErr); ctxErr != nil {
				return nil, ctxErr
			}
			if readErr != nil {
				// The expectation carrier could not be read: the interval
				// records the failed read as an uncovered range instead of
				// concluding anything from it.
				outcome.addGapReason(GapQueryFailed)
			} else {
				obligationEvidence = evidence
			}
		}
	}

	compare := &scanCandidateCompare{
		sources: req.Sources,
		upstreamFor: func(businessType BusinessType) UpstreamReceiptSource {
			return scanUpstreamReceiptSource(task, businessType)
		},
		freshness:     req.FreshnessTolerance,
		scope:         scope,
		chainID:       chainID,
		chainIDOK:     chainIDOK,
		now:           now,
		chainBundle:   chainBundle,
		chainUsable:   chainUsable,
		eventEvidence: eventEvidence,
		eventUsable:   eventUsable,
		obligation:    obligationEvidence,
	}
	for i := range candidates {
		candidate := candidates[i]
		if err := scanValidateCandidate(&candidate, scope); err != nil {
			return nil, err
		}
		classification, _, err := compareScanCandidate(ctx, compare, candidate, budget)
		if err != nil {
			return nil, err
		}
		outcome.classifications = append(outcome.classifications, classification)

		switch {
		case classification.Ticket:
			// Persisted (dedup + occurrence) inside the commit transaction.
		case classification.MetricsOnly() && classification.Conclusion == ConclusionConsistent:
			outcome.absorbed++
		case classification.Conclusion == ConclusionPending || classification.Category == CategoryIncomplete:
			outcome.pending++
			outcome.addGapReason(gapReasonForClassification(classification, scanUpstreamReceiptSource(task, candidate.BusinessType)))
		default:
			// Consistent on complete, fresh evidence.
		}
	}
	return outcome, nil
}

// scanCandidateCompare is the interval-level read context one candidate
// comparison consumes: the three read-only adapters, the resolved interval
// reads and the per-invocation knobs. The scan compare loop and the production
// pending_verify re-verification both build it, so a candidate is classified
// through exactly one three-way path (T017 classifier + T040 discriminator).
type scanCandidateCompare struct {
	sources       ScanSources
	upstreamFor   func(BusinessType) UpstreamReceiptSource
	freshness     time.Duration
	scope         IdentityScope
	chainID       int64
	chainIDOK     bool
	now           time.Time
	chainBundle   *ChainFactsBundle
	chainUsable   bool
	eventEvidence *EventStateEvidence
	eventUsable   bool
	obligation    EventObligationEvidence
}

// compareScanCandidate classifies one candidate against the interval-level
// reads (chain bundle, event evidence, expectation carrier) plus its own PG
// read. It performs one charged PG read and no write. An adapter read failure
// keeps the affected party unknown (evidence-unknown, never a conclusion); a
// context cancellation is returned as such. A nil PG adapter leaves the PG
// party unknown.
func compareScanCandidate(ctx context.Context, cc *scanCandidateCompare, candidate ScanCandidate, budget *Budget) (Classification, *PGStateRecord, error) {
	if cc == nil {
		return Classification{}, nil, contractErrorf("candidate comparison has no context")
	}
	eventKey := candidate.EventKey
	if strings.TrimSpace(eventKey.Value) == "" {
		eventKey = candidate.BusinessKey
	}

	// Chain party.
	var (
		chainParty PartyObservation
		chainBlock *ChainFactBlock
		chainLogs  []ChainFactLog
	)
	switch {
	case cc.chainBundle == nil:
		chainParty = PartyObservation{Status: PartyUnknown, Content: chainUnavailableSnapshot(candidate.ChainFact)}
	default:
		match := matchChainCandidateFacts(cc.chainBundle, candidate.ChainFact)
		switch {
		case match.orphaned:
			chainParty = PartyObservation{Status: PartyAbsent, Orphaned: true,
				Content: chainAbsenceSnapshot(cc.chainID, candidate.ChainFact)}
		case match.ambiguous:
			chainParty = PartyObservation{Status: PartyUnknown, Content: chainUnavailableSnapshot(candidate.ChainFact)}
		case match.present:
			chainParty = PartyObservation{Status: PartyPresent,
				Content: chainCandidateSnapshot(cc.chainID, candidate.ChainFact, match.block, match.logs)}
			chainBlock = match.block
			chainLogs = match.logs
		case !cc.chainUsable:
			chainParty = PartyObservation{Status: PartyUnknown, Content: chainUnavailableSnapshot(candidate.ChainFact)}
		default:
			chainParty = PartyObservation{Status: PartyAbsent,
				Content: chainAbsenceSnapshot(cc.chainID, candidate.ChainFact)}
		}
	}

	// Event party.
	var (
		eventParty PartyObservation
		eventObs   *EventDeliveryObservation
	)
	if cc.eventEvidence == nil {
		eventParty = PartyObservation{Status: PartyUnknown, Content: eventUnavailableSnapshot(eventKey)}
	} else {
		observation, match := matchCandidateEventObservation(cc.eventEvidence, eventKey)
		switch match {
		case eventMatchFound:
			eventParty = observation.PartyObservation()
			eventObs = observation
		case eventMatchAmbiguous:
			eventParty = PartyObservation{Status: PartyUnknown, Content: eventUnavailableSnapshot(eventKey)}
		default:
			if cc.eventUsable {
				eventParty = PartyObservation{Status: PartyAbsent, Content: EventAbsenceSnapshot(eventKey)}
			} else {
				eventParty = PartyObservation{Status: PartyUnknown, Content: eventUnavailableSnapshot(eventKey)}
			}
		}
	}

	// PG party (one read per candidate; outside any transaction).
	var pgRecord *PGStateRecord
	pgParty := PartyObservation{Status: PartyUnknown, Content: pgUnavailableSnapshot(candidate.BusinessKey)}
	if cc.sources.PG != nil && cc.chainIDOK {
		if err := budget.ConsumePG(ctx, 1); err != nil {
			return Classification{}, nil, err
		}
		record, readErr := cc.sources.PG.Read(ctx, PGReadRequest{
			ChainID:      cc.chainID,
			BusinessType: candidate.BusinessType,
			Key:          candidate.BusinessKey,
		})
		if ctxErr := scanContextError(readErr); ctxErr != nil {
			return Classification{}, nil, ctxErr
		}
		pgRecord = &record
		switch record.Status {
		case PGStateComplete:
			pgParty = PartyObservation{Status: PartyPresent, Content: record.CanonicalBytes()}
		case PGStateAbsent:
			pgParty = PartyObservation{Status: PartyAbsent, Content: record.CanonicalBytes()}
		default:
			pgParty = PartyObservation{Status: PartyUnknown, Content: record.CanonicalBytes()}
		}
	}

	// T040 expected-event discriminator: only the decisive event-only
	// absence (chain fact and PG business record both present, event
	// delivery absent) consults the expectation carrier. Where another
	// party's divergence already owns the ticket, the event party status
	// and its canonical content are left untouched, so no identity
	// content hash and no existing ticket is perturbed:
	//   - proven obligation   -> keep absent (R1, existing missing ticket);
	//   - provably no catalog event aggregate -> N/A (R2), chain/PG
	//     differences still classify independently;
	//   - unproven obligation -> unknown/pending (R3), alert-only.
	var eventObligation EventObligationState
	if eventParty.Status == PartyAbsent && chainParty.Status == PartyPresent && pgParty.Status == PartyPresent {
		var aggregate *EventObligationAggregate
		if bound, ok := EventObligationAggregateOf(eventKey); ok {
			aggregate = &bound
		}
		verdict := DiscriminateEventObligation(aggregate, cc.obligation)
		switch verdict.State {
		case EventObligationProven:
			eventObligation = EventObligationProven
		case EventObligationNotApplicable:
			eventObligation = EventObligationNotApplicable
			eventParty = PartyObservation{Status: PartyNotApplicable,
				Content: EventNotApplicableSnapshot(eventKey)}
		case EventObligationUnproven:
			eventObligation = EventObligationUnproven
			eventParty = PartyObservation{Status: PartyUnknown,
				Content: eventUnavailableSnapshot(eventKey)}
		default:
			return Classification{}, nil, contractErrorf("event obligation discriminator returned unknown state %q", verdict.State)
		}
	}

	// Q4 duplicate evidence: the event adapter owns the delivery-side
	// facts, the candidate owns the PG-side repeated-effect facts.
	duplicates := candidate.PGDuplicate
	if eventObs != nil {
		duplicates = mergeDuplicateEvidence(eventObs.DuplicateEvidence(), candidate.PGDuplicate)
	}
	if eventParty.Status != PartyPresent && duplicates.Deliveries > 1 {
		// The classifier refuses duplicate evidence without a present
		// event party; keep the shape valid instead of dropping the
		// divergence facts.
		duplicates.Deliveries = 1
	}

	evidenceAt := stableScanEvidenceInstant(chainBlock, eventObs, pgRecord, cc.now)
	upstream := UpstreamReceiptSource{}
	if cc.upstreamFor != nil {
		upstream = cc.upstreamFor(candidate.BusinessType)
	}
	classification := Classify(Observation{
		Scope:        cc.scope,
		BusinessKey:  candidate.BusinessKey,
		BusinessType: candidate.BusinessType,
		Chain:        chainParty,
		PG:           pgParty,
		Event:        eventParty,
		Mismatch:     candidate.Mismatch,
		Duplicates:   duplicates,
		Coverage: Coverage{
			ScanComplete:       cc.chainUsable && cc.eventUsable,
			OpenGaps:           0,
			EvidenceAt:         evidenceAt,
			Now:                cc.now,
			FreshnessTolerance: cc.freshness,
		},
		Upstream:        upstream,
		Version:         mergeScanVersionDomain(cc.chainBundle, chainBlock, chainLogs, eventObs, pgRecord, candidate.ChainFact, cc.now),
		EvidenceRef:     candidate.EvidenceRef,
		EventObligation: eventObligation,
	})
	if members := scanCandidateMembers(candidate, chainLogs); len(members) > 0 {
		classification.Members = members
	}
	return classification, pgRecord, nil
}

// scanCandidateMembers returns the canonical member facts of one candidate:
// the explicitly enumerated members (chain-first enumeration, T033/T034) take
// precedence; otherwise the member logs matched inside the interval bundle are
// used. Member facts never split a tx into several tickets (T035).
func scanCandidateMembers(candidate ScanCandidate, matched []ChainFactLog) []TxAggregateMember {
	source := candidate.Members
	if len(source) == 0 {
		source = make([]TxAggregateMember, 0, len(matched))
		for i := range matched {
			source = append(source, TxAggregateMember{
				BlockNumber: matched[i].BlockNumber,
				BlockHash:   matched[i].BlockHash,
				TxHash:      matched[i].TxHash,
				LogIndex:    matched[i].LogIndex,
				Contract:    matched[i].Contract,
				Topic0:      matched[i].Topic0,
			})
		}
	}
	canonical, err := CanonicalizeTxAggregateMembers(source)
	if err != nil {
		return nil
	}
	return canonical
}

// scanValidateCandidate refuses malformed or out-of-scope candidates before
// any comparison: they are wiring defects, not evidence, so they fail the
// invocation without writing anything.
func scanValidateCandidate(candidate *ScanCandidate, scope IdentityScope) error {
	if candidate == nil {
		return contractErrorf("nil scan candidate")
	}
	if !candidate.BusinessType.Known() {
		return contractErrorf("candidate has unknown business type %q", candidate.BusinessType)
	}
	if err := candidate.BusinessKey.Validate(); err != nil {
		return err
	}
	if len(scanBusinessKeyRecord(candidate.BusinessKey)) > scanBusinessKeyMax {
		return contractErrorf("candidate business key exceeds %d bytes", scanBusinessKeyMax)
	}
	if strings.TrimSpace(candidate.EventKey.Value) != "" {
		if err := candidate.EventKey.Validate(); err != nil {
			return err
		}
	}
	for _, businessType := range scope.BusinessTypes {
		if businessType == candidate.BusinessType {
			return nil
		}
	}
	return contractErrorf("candidate business type %q is outside the task scope", candidate.BusinessType)
}

// scanBusinessKeyRecord renders the persisted discrepancy.business_key text:
// kind and value are both part of the identity, never truncated silently.
func scanBusinessKeyRecord(key BusinessKey) string {
	return string(key.Kind) + "=" + key.Value
}

// scanUpstreamReceiptSource looks up the task's declared upstream receipt
// source of one business type. A missing declaration counts as unconnected
// (FR-006): no consistent conclusion is possible from it.
func scanUpstreamReceiptSource(task *Task, businessType BusinessType) UpstreamReceiptSource {
	if task == nil {
		return UpstreamReceiptSource{}
	}
	for _, declaration := range task.UpstreamReceipts {
		if declaration.BusinessType != businessType {
			continue
		}
		return UpstreamReceiptSource{
			Source:    declaration.Source,
			Connected: declaration.Connected,
			// A connected declaration is an available read path. A connected
			// flag alone still never proves upstream success: the classifier
			// keeps ExternalCredit unverified without a positive receipt
			// (FR-006).
			Available: declaration.Connected,
		}
	}
	return UpstreamReceiptSource{}
}

// gapReasonForClassification maps a pending classification onto the closed
// uncovered-range vocabulary so the interval cannot close over unproven
// evidence.
func gapReasonForClassification(classification Classification, upstream UpstreamReceiptSource) GapReason {
	switch classification.Reason {
	case ReasonUpstreamUnconnected, ReasonUpstreamUnavailable:
		return GapUpstreamUnconnected
	case ReasonFreshnessExpired, ReasonFreshnessUnproven, ReasonEvidenceTrimmed:
		return GapFreshnessHold
	default:
		if !upstream.Connected {
			return GapUpstreamUnconnected
		}
		return GapQueryFailed
	}
}

// chainCandidateMatch is the chain-facts membership verdict of one candidate
// ref. ambiguous means the ref cannot be located/attributed (chain party
// stays unknown); orphaned means the durable facts contradict the ref (reorg;
// only pending handling is possible, FR-017).
type chainCandidateMatch struct {
	declared  bool
	ambiguous bool
	orphaned  bool
	present   bool
	block     *ChainFactBlock
	logs      []ChainFactLog
}

// matchChainCandidateFacts checks one candidate ref against the interval's
// chain bundle. Absence is only reported when the ref declares a locatable
// block and the bundle can support a conclusion.
func matchChainCandidateFacts(bundle *ChainFactsBundle, ref ChainFactRef) chainCandidateMatch {
	match := chainCandidateMatch{declared: ref.Declared()}
	if bundle == nil || !match.declared {
		match.ambiguous = true
		return match
	}
	if ref.BlockNumber > 0 {
		for i := range bundle.Blocks {
			block := &bundle.Blocks[i]
			if block.Number != ref.BlockNumber {
				continue
			}
			if ref.BlockHash != "" && !strings.EqualFold(block.Hash, ref.BlockHash) {
				// The height carries a different canonical hash: the
				// referenced fact was reorged away.
				match.orphaned = true
				return match
			}
			match.block = block
			break
		}
	} else if ref.BlockHash != "" {
		for i := range bundle.Blocks {
			if strings.EqualFold(bundle.Blocks[i].Hash, ref.BlockHash) {
				match.block = &bundle.Blocks[i]
				break
			}
		}
		if match.block == nil {
			// A hash without a number cannot be located inside a height
			// range; absence is not provable from this bundle.
			match.ambiguous = true
		}
	}
	if ref.TxHash != "" {
		for i := range bundle.Logs {
			log := &bundle.Logs[i]
			if !strings.EqualFold(log.TxHash, ref.TxHash) {
				continue
			}
			if ref.LogIndex != nil && log.LogIndex != *ref.LogIndex {
				continue
			}
			match.logs = append(match.logs, *log)
		}
	}
	match.present = match.block != nil || len(match.logs) > 0
	if !match.present && ref.BlockNumber == 0 {
		// Without a declared height the ref cannot be located by this
		// adapter's complete height index (the transfer-log index only
		// carries ERC-20 transfers), so absence is not provable.
		match.ambiguous = true
	}
	return match
}

// candidateEventMatch is the event-identity membership verdict of one
// candidate key.
type candidateEventMatch int

const (
	eventMatchNone candidateEventMatch = iota
	eventMatchFound
	eventMatchAmbiguous
)

// matchCandidateEventObservation finds the single event observation whose
// event id or aggregate identity equals key. More than one distinct event id
// is ambiguous and must stay pending instead of guessing one version.
func matchCandidateEventObservation(evidence *EventStateEvidence, key BusinessKey) (*EventDeliveryObservation, candidateEventMatch) {
	if evidence == nil {
		return nil, eventMatchNone
	}
	var (
		found    *EventDeliveryObservation
		distinct = make(map[uuid.UUID]struct{})
	)
	for i := range evidence.Observations {
		observation := &evidence.Observations[i]
		matches := observation.EventBusinessKey() == key
		if !matches {
			if aggregate := observation.AggregateBusinessKey(); aggregate.Value != "" {
				matches = aggregate == key
			}
		}
		if !matches {
			continue
		}
		distinct[observation.EventID] = struct{}{}
		if found == nil {
			found = observation
		}
	}
	switch len(distinct) {
	case 0:
		return nil, eventMatchNone
	case 1:
		return found, eventMatchFound
	default:
		return nil, eventMatchAmbiguous
	}
}

// mergeDuplicateEvidence folds the event-delivery facts and the PG-side
// repeated-effect facts into one conservative Q4 evidence package: every
// divergence flag is an OR and every count is the observed maximum — a
// duplicate is never absorbed by merging.
func mergeDuplicateEvidence(base, extra DuplicateEvidence) DuplicateEvidence {
	merged := base
	if extra.Deliveries > merged.Deliveries {
		merged.Deliveries = extra.Deliveries
	}
	merged.ContentChecked = base.ContentChecked || extra.ContentChecked
	merged.ContentDivergent = base.ContentDivergent || extra.ContentDivergent
	merged.VersionGuardIgnoredLegalOld = base.VersionGuardIgnoredLegalOld || extra.VersionGuardIgnoredLegalOld
	merged.VersionRuleViolated = base.VersionRuleViolated || extra.VersionRuleViolated
	merged.IdempotencyRecorded = base.IdempotencyRecorded || extra.IdempotencyRecorded
	merged.EffectEvidencePresent = base.EffectEvidencePresent || extra.EffectEvidencePresent
	if extra.EffectCount > merged.EffectCount {
		merged.EffectCount = extra.EffectCount
	}
	merged.RepeatedBusinessEffect = base.RepeatedBusinessEffect || extra.RepeatedBusinessEffect
	merged.RepeatedWithdrawalIntent = base.RepeatedWithdrawalIntent || extra.RepeatedWithdrawalIntent
	return merged
}

// stableScanEvidenceInstant derives the stable evidence time of one candidate
// from observed facts only (never the scan's wall clock), so re-observing the
// same facts keeps the same identity and cross-scan dedup works.
func stableScanEvidenceInstant(block *ChainFactBlock, event *EventDeliveryObservation, pg *PGStateRecord, fallback time.Time) time.Time {
	instant := time.Time{}
	if block != nil && !block.IndexedAt.IsZero() {
		instant = block.IndexedAt.UTC()
	}
	if event != nil {
		for _, at := range []time.Time{event.OccurredAt, event.EmittedAt} {
			if !at.IsZero() && at.UTC().After(instant) {
				instant = at.UTC()
			}
		}
	}
	if pg != nil && !pg.Freshness.NewestObservedAt.IsZero() {
		if at := pg.Freshness.NewestObservedAt.UTC(); at.After(instant) {
			instant = at
		}
	}
	if instant.IsZero() {
		return fallback.UTC()
	}
	return instant
}

// mergeScanVersionDomain folds the three parties' version contributions into
// the identity evidence version domain (research §3): block identity anchors
// the domain, recovery/authorization/state versions make a later change
// re-enter verification, and the evidence time stays fact-derived.
func mergeScanVersionDomain(chain *ChainFactsBundle, block *ChainFactBlock, logs []ChainFactLog,
	event *EventDeliveryObservation, pg *PGStateRecord, ref ChainFactRef, fallback time.Time) VersionDomain {
	domain := VersionDomain{}
	switch {
	case block != nil && block.Number >= 0 && block.Hash != "":
		domain.BlockNumber = uint64(block.Number)
		domain.BlockHash = block.Hash
	case len(logs) > 0 && logs[0].BlockHash != "":
		domain.BlockNumber = uint64(logs[0].BlockNumber)
		domain.BlockHash = logs[0].BlockHash
	case ref.BlockNumber > 0 && ref.BlockHash != "":
		domain.BlockNumber = uint64(ref.BlockNumber)
		domain.BlockHash = ref.BlockHash
	case event != nil && event.BlockNumber != nil && *event.BlockNumber > 0 &&
		event.BlockHash != nil && *event.BlockHash != "":
		domain.BlockNumber = uint64(*event.BlockNumber)
		domain.BlockHash = *event.BlockHash
	}
	switch {
	case chain != nil && chain.Recovery.RecoveryID != "":
		domain.RecoveryVersion = fmt.Sprintf("%s/%d", chain.Recovery.RecoveryID, chain.Recovery.Seq)
	case pg != nil && pg.Version.RecoveryVersion > 0:
		domain.RecoveryVersion = strconv.FormatInt(pg.Version.RecoveryVersion, 10)
	case event != nil && event.RecoveryVersion != nil && *event.RecoveryVersion > 0:
		domain.RecoveryVersion = strconv.FormatInt(*event.RecoveryVersion, 10)
	}
	if pg != nil && pg.Version.AuthorizationVersion > 0 {
		domain.AuthorizationVersion = strconv.FormatInt(pg.Version.AuthorizationVersion, 10)
	}
	if pg != nil && pg.Version.StateVersion > 0 {
		domain.StateVersion = pg.Version.StateVersion
	}
	domain.EvidenceAt = stableScanEvidenceInstant(block, event, pg, fallback)
	return domain
}

// The compare loop's canonical snapshot versions. Changing any of them
// changes every derived content hash and is a dedup-breaking change.
const (
	chainCandidateCanonicalVersion   = "txharbor.reconciliation.chaincandidate.v1"
	chainAbsenceCanonicalVersion     = "txharbor.reconciliation.chainabsence.v1"
	chainUnavailableCanonicalVersion = "txharbor.reconciliation.chainunavailable.v1"
	eventUnavailableCanonicalVersion = "txharbor.reconciliation.eventunavailable.v1"
	pgUnavailableCanonicalVersion    = "txharbor.reconciliation.pgunavailable.v1"

	// scanEvidenceRefMax / scanBusinessKeyMax mirror the migration 000016
	// CHECK bounds of discrepancy_occurrence.evidence_ref and
	// discrepancy.business_key.
	scanEvidenceRefMax = 512
	scanBusinessKeyMax = 512
)

// chainCandidateSnapshot encodes only the candidate's matched chain facts,
// never the whole interval bundle: the content hash must be stable when the
// same fact is re-observed under a different claim size.
func chainCandidateSnapshot(chainID int64, ref ChainFactRef, block *ChainFactBlock, logs []ChainFactLog) []byte {
	w := &identityCanonWriter{}
	w.bytesField("chaincandidate.version", []byte(chainCandidateCanonicalVersion))
	w.int64Field("chaincandidate.chain_id", chainID)
	w.int64Field("chaincandidate.ref.block_number", ref.BlockNumber)
	w.stringField("chaincandidate.ref.block_hash", ref.BlockHash)
	w.stringField("chaincandidate.ref.tx_hash", ref.TxHash)
	if ref.LogIndex != nil {
		w.uint64Field("chaincandidate.ref.log_index.present", 1)
		w.int64Field("chaincandidate.ref.log_index.value", *ref.LogIndex)
	} else {
		w.uint64Field("chaincandidate.ref.log_index.present", 0)
	}
	if block != nil {
		w.uint64Field("chaincandidate.block.present", 1)
		w.int64Field("chaincandidate.block.number", block.Number)
		w.stringField("chaincandidate.block.hash", block.Hash)
		w.stringField("chaincandidate.block.parent_hash", block.ParentHash)
		w.bytesField("chaincandidate.block.canonical", chainFactsBoolByte(block.Canonical))
		w.int64Field("chaincandidate.block.indexed_at_unix_nano", block.IndexedAt.UnixNano())
	} else {
		w.uint64Field("chaincandidate.block.present", 0)
	}
	ordered := append([]ChainFactLog(nil), logs...)
	sort.Slice(ordered, func(i, j int) bool {
		a, z := ordered[i], ordered[j]
		switch {
		case a.BlockNumber != z.BlockNumber:
			return a.BlockNumber < z.BlockNumber
		case a.BlockHash != z.BlockHash:
			return a.BlockHash < z.BlockHash
		case a.TxHash != z.TxHash:
			return a.TxHash < z.TxHash
		default:
			return a.LogIndex < z.LogIndex
		}
	})
	w.uint64Field("chaincandidate.logs.count", uint64(len(ordered)))
	for i := range ordered {
		prefix := fmt.Sprintf("chaincandidate.logs.%d", i)
		w.int64Field(prefix+".block_number", ordered[i].BlockNumber)
		w.stringField(prefix+".block_hash", ordered[i].BlockHash)
		w.stringField(prefix+".tx_hash", ordered[i].TxHash)
		w.int64Field(prefix+".log_index", ordered[i].LogIndex)
		w.stringField(prefix+".contract", ordered[i].Contract)
		w.stringField(prefix+".topic0", ordered[i].Topic0)
		w.stringField(prefix+".data", ordered[i].Data)
	}
	return w.buf.Bytes()
}

// chainAbsenceSnapshot is the canonical marker of a definitively absent chain
// fact inside a complete interval (a non-empty marker keeps the three-party
// snapshot hashable; an empty slice would only downgrade the observation).
func chainAbsenceSnapshot(chainID int64, ref ChainFactRef) []byte {
	w := &identityCanonWriter{}
	w.bytesField("chainabsence.version", []byte(chainAbsenceCanonicalVersion))
	w.int64Field("chainabsence.chain_id", chainID)
	w.int64Field("chainabsence.ref.block_number", ref.BlockNumber)
	w.stringField("chainabsence.ref.block_hash", ref.BlockHash)
	w.stringField("chainabsence.ref.tx_hash", ref.TxHash)
	return w.buf.Bytes()
}

// chainUnavailableSnapshot is the canonical marker of an un-attributable
// chain party (unconfigured adapter, unresolved window, unknown bundle).
func chainUnavailableSnapshot(ref ChainFactRef) []byte {
	w := &identityCanonWriter{}
	w.bytesField("chainunavailable.version", []byte(chainUnavailableCanonicalVersion))
	w.int64Field("chainunavailable.ref.block_number", ref.BlockNumber)
	w.stringField("chainunavailable.ref.block_hash", ref.BlockHash)
	w.stringField("chainunavailable.ref.tx_hash", ref.TxHash)
	return w.buf.Bytes()
}

// eventUnavailableSnapshot is the canonical marker of an event party that
// could not be determined (adapter unwired, evidence incomplete, ambiguous
// match). It is never a claim of absence.
func eventUnavailableSnapshot(key BusinessKey) []byte {
	w := &identityCanonWriter{}
	w.bytesField("eventunavailable.version", []byte(eventUnavailableCanonicalVersion))
	w.stringField("eventunavailable.business_key.kind", string(key.Kind))
	w.stringField("eventunavailable.business_key.value", key.Value)
	return w.buf.Bytes()
}

// pgUnavailableSnapshot is the canonical marker of a PG party that could not
// be read (adapter unwired, non-numeric chain identity).
func pgUnavailableSnapshot(key BusinessKey) []byte {
	w := &identityCanonWriter{}
	w.bytesField("pgunavailable.version", []byte(pgUnavailableCanonicalVersion))
	w.stringField("pgunavailable.business_key.kind", string(key.Kind))
	w.stringField("pgunavailable.business_key.value", key.Value)
	return w.buf.Bytes()
}

// SQL for the batch results (persisted inside CommitScanBatch's transaction,
// ordering: discrepancy -> occurrence -> audit -> gaps, then checkpoint by
// CommitScanBatch itself). Table/column names follow migration 000016.
const insertDiscrepancySQL = `
INSERT INTO discrepancy (
    discrepancy_id, category, business_key, content_hash, evidence_version_domain, state)
VALUES ($1, $2, $3, $4, $5::jsonb, 'open_claimable')
ON CONFLICT (discrepancy_id) DO NOTHING`

const insertDiscrepancyOccurrenceSQL = `
INSERT INTO discrepancy_occurrence (discrepancy_id, observed_at, evidence_ref, scan_task_id)
VALUES ($1, now(), $2, $3)`

// scanAggregateRootsSQL locates the tx-aggregate ticket rows of one business
// key (bounded); the scope marker inside evidence_version_domain selects the
// row of the observing scope (T035).
const scanAggregateRootsSQL = `
SELECT discrepancy_id, state, encode(content_hash, 'hex'), evidence_version_domain
FROM discrepancy
WHERE category = $1 AND business_key = $2
ORDER BY created_at, discrepancy_id
LIMIT 64`

// invalidateTxAggregateSQL applies the Q5 invalidation to an existing ticket:
// the latest evidence replaces the recorded evidence and a conclusion-bearing
// state moves to pending_verify. claimed/disposing rows keep their state (only
// the lifecycle owner may settle a claim); an absent conclusion (open_claimable/
// reopened/pending_verify) also moves to pending_verify per the T035 rule that a
// reorg replacement never mints a new ticket.
const invalidateTxAggregateSQL = `
UPDATE discrepancy
SET state = CASE WHEN state IN ('open_claimable', 'reopened', 'closed', 'pending_verify')
                 THEN 'pending_verify' ELSE state END,
    content_hash = $2,
    evidence_version_domain = $3::jsonb,
    reverify_generation = reverify_generation + 1,
    updated_at = now()
WHERE discrepancy_id = $1`

// persist writes one interval's results inside the commit transaction. It
// never re-locks recon_task and never performs network I/O (CommitScanBatch
// contract; data-model.md §5). Detections are grouped before persistence:
//
//   - a tx-aggregate detection (`missing` under a tx_hash business key) is one
//     ticket per tx (T035): member logs are enumerated in the occurrence
//     evidence and a reorg replacement (same tx_hash, new block) follows the Q5
//     invalidation path on the same ticket (pending_verify + occurrence), never
//     a new ticket;
//   - every other detection dedups by its stable identity id, and a same-batch
//     repeat appends no second occurrence row.
func (o *scanIntervalOutcome) persist(ctx context.Context, tx pgx.Tx) error {
	groups := make([]*scanTicketGroup, 0, len(o.classifications))
	index := make(map[string]*scanTicketGroup, len(o.classifications))
	for i := range o.classifications {
		classification := o.classifications[i]
		if !classification.Ticket || !classification.Identity.Valid() {
			continue
		}
		key, aggregate := scanTicketGroupKey(classification)
		if !aggregate {
			key = "identity:" + classification.Identity.ID().String()
		}
		group, ok := index[key]
		if !ok {
			group = &scanTicketGroup{aggregate: aggregate,
				businessKey: scanBusinessKeyRecord(classification.Identity.BusinessKey())}
			index[key] = group
			groups = append(groups, group)
		}
		if !group.primary.Identity.Valid() {
			group.primary = classification
		}
		group.classifications = append(group.classifications, classification)
	}
	for _, group := range groups {
		if err := o.persistTicketGroup(ctx, tx, group); err != nil {
			return err
		}
	}

	// Q4: absorbed duplicates and pending/incomplete evidence are
	// metrics/audit-only — never tickets. One bounded audit row per batch
	// keeps the trail queryable without unbounded writes.
	if o.pending > 0 || o.absorbed > 0 || o.metricsOnly > 0 {
		result := "incomplete"
		if o.pending == 0 {
			result = "absorbed_duplicates"
			if o.metricsOnly > 0 {
				result = "unattributed"
			}
		}
		if err := insertAuditTx(ctx, tx, AuditRecord{
			Actor:  o.owner,
			Action: AuditActionQuery,
			Target: map[string]any{
				"task_id":             o.taskID,
				"attempt_id":          o.attemptID,
				"range_start":         rangeAuditValue(o.interval.From),
				"range_end":           rangeAuditValue(o.interval.To),
				"pending":             o.pending,
				"absorbed_duplicates": o.absorbed,
				"unattributed":        o.metricsOnly,
				"ticketable":          o.tickets + o.merged,
			},
			Reason: scanAuditReason(o),
			Result: result,
		}); err != nil {
			return err
		}
	}

	// Uncovered/unproven remainder: pending classifications keep the interval
	// visible as a gap so `done` can never close over them (FR-004/019).
	for _, reason := range o.gapReasons {
		if err := insertGapTx(ctx, tx, o.taskID, o.interval.From, o.interval.To, reason); err != nil {
			return err
		}
		o.gaps++
	}
	return nil
}

// scanTicketGroup collects the detections that belong to one persisted ticket
// within one interval: either the same stable identity, or the same
// tx-aggregate key (T035). Member facts are unioned so one transaction never
// splits into several tickets.
type scanTicketGroup struct {
	aggregate       bool
	businessKey     string
	primary         Classification
	classifications []Classification
}

// members returns the deduplicated canonical member facts of the group.
func (g *scanTicketGroup) members() []TxAggregateMember {
	var out []TxAggregateMember
	for i := range g.classifications {
		out = append(out, g.classifications[i].Members...)
	}
	canonical, err := CanonicalizeTxAggregateMembers(out)
	if err != nil {
		return nil
	}
	return canonical
}

// scanTicketGroupKey reports whether one detection belongs to a tx-aggregate
// ticket (missing under a tx_hash business key) and its grouping key.
func scanTicketGroupKey(c Classification) (string, bool) {
	if c.Category != CategoryMissing {
		return "", false
	}
	key := c.Identity.BusinessKey()
	if key.Kind != BusinessKeyTxHash {
		return "", false
	}
	return "txagg:" + scanBusinessKeyRecord(key), true
}

// persistTicketGroup persists one group of same-ticket detections.
func (o *scanIntervalOutcome) persistTicketGroup(ctx context.Context, tx pgx.Tx, group *scanTicketGroup) error {
	if !group.aggregate {
		inserted, err := o.insertScanDiscrepancyTx(ctx, tx, group.primary, nil)
		if err != nil {
			return err
		}
		if err := o.appendTicketOccurrences(ctx, tx, group.primary.Identity.ID(), nil, &group.primary); err != nil {
			return err
		}
		if inserted {
			o.tickets++
		} else {
			o.merged++
		}
		return nil
	}

	members := group.members()
	root, err := findTxAggregateRootTx(ctx, tx, group)
	if err != nil {
		return err
	}
	if root == nil {
		inserted, err := o.insertScanDiscrepancyTx(ctx, tx, group.primary, members)
		if err != nil {
			return err
		}
		if err := o.appendTicketOccurrences(ctx, tx, group.primary.Identity.ID(), members, &group.primary); err != nil {
			return err
		}
		if inserted {
			o.tickets++
		} else {
			o.merged++
		}
		return nil
	}

	// The ticket already exists: this detection is a merge. A changed evidence
	// version (reorg replacement to a new block, new member evidence) follows
	// the Q5 invalidation path on the same ticket — pending_verify plus an
	// occurrence append — never a new ticket (T035/FR-007/FR-017).
	changed, err := txAggregateEvidenceChanged(root, group.primary)
	if err != nil {
		return err
	}
	if changed {
		if err := o.invalidateTxAggregateTx(ctx, tx, root, group.primary, members); err != nil {
			return err
		}
	}
	if err := o.appendTicketOccurrences(ctx, tx, root.ID, members, &group.primary); err != nil {
		return err
	}
	o.merged++
	return nil
}

// appendTicketOccurrences appends one occurrence row per member fact (T035:
// each member log is individually recorded), or one row for the detection when
// no member facts exist. The reference is bounded; an oversized member list is
// digested, never truncated silently.
func (o *scanIntervalOutcome) appendTicketOccurrences(ctx context.Context, tx pgx.Tx,
	id uuid.UUID, members []TxAggregateMember, classification *Classification) error {
	if len(members) == 0 {
		if _, err := tx.Exec(ctx, insertDiscrepancyOccurrenceSQL,
			id, scanEvidenceRef(classification, o.attemptID), o.taskID); err != nil {
			return fmt.Errorf("insert discrepancy occurrence: %w", err)
		}
		o.occurrences++
		return nil
	}
	for _, member := range members {
		if _, err := tx.Exec(ctx, insertDiscrepancyOccurrenceSQL,
			id, scanMemberEvidenceRef(member, o.attemptID), o.taskID); err != nil {
			return fmt.Errorf("insert discrepancy occurrence: %w", err)
		}
		o.occurrences++
	}
	return nil
}

// scanAggregateRoot is the existing tx-aggregate ticket row located by
// (category, business key): the row whose recorded scope matches the observing
// task scope, or (compatibility) the first legacy row that carries no scope
// marker.
type scanAggregateRoot struct {
	ID             uuid.UUID
	State          DiscrepancyState
	ContentHashHex string
	Domain         PersistedEvidenceDomain
}

// findTxAggregateRootTx locates the aggregate ticket of one tx-aggregate group
// inside the commit transaction. A same-key row under a different scope is a
// different identity and never a merge target.
func findTxAggregateRootTx(ctx context.Context, tx pgx.Tx, group *scanTicketGroup) (*scanAggregateRoot, error) {
	rows, err := tx.Query(ctx, scanAggregateRootsSQL,
		string(group.primary.Category), group.businessKey)
	if err != nil {
		return nil, fmt.Errorf("find tx aggregate ticket: %w", err)
	}
	defer rows.Close()
	var legacy *scanAggregateRoot
	for rows.Next() {
		var (
			id          uuid.UUID
			state       DiscrepancyState
			hashHex     string
			domainBytes []byte
		)
		if err := rows.Scan(&id, &state, &hashHex, &domainBytes); err != nil {
			return nil, fmt.Errorf("scan tx aggregate ticket: %w", err)
		}
		if !state.Valid() {
			return nil, contractErrorf("discrepancy %s has unknown state %q", id, state)
		}
		root := &scanAggregateRoot{ID: id, State: state, ContentHashHex: strings.ToLower(hashHex)}
		domain, parseErr := ParsePersistedEvidenceDomain(domainBytes)
		if parseErr == nil {
			root.Domain = domain
		}
		switch {
		case root.Domain.Scope != nil && SameIdentityScope(*root.Domain.Scope, group.primary.Identity.Scope()):
			return root, nil
		case root.Domain.Scope == nil && legacy == nil:
			// Compatibility: a row detected before scope markers existed can
			// still be the aggregate root (same chain/business key).
			legacy = root
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("find tx aggregate ticket: %w", err)
	}
	return legacy, nil
}

// txAggregateEvidenceChanged reports whether the new detection's evidence
// contradicts the recorded ticket (content hash or block identity). A reorg
// replacement to a new block is a change; an identical re-observation is not.
func txAggregateEvidenceChanged(root *scanAggregateRoot, primary Classification) (bool, error) {
	if root == nil {
		return false, contractErrorf("tx aggregate evidence change requires a recorded ticket")
	}
	if !primary.Identity.Valid() {
		return false, contractErrorf("tx aggregate evidence change requires a minted identity")
	}
	if !strings.EqualFold(root.ContentHashHex, primary.Identity.ContentHash().Hex()) {
		return true, nil
	}
	recordedBlock := strings.TrimSpace(root.Domain.BlockHash)
	observedBlock := strings.TrimSpace(primary.Identity.VersionDomain().BlockHash)
	return !strings.EqualFold(recordedBlock, observedBlock), nil
}

// invalidateTxAggregateTx applies the Q5 invalidation to an existing
// tx-aggregate ticket: the row moves to pending_verify (from the conclusion/
// conclusion-less states; claimed/disposing rows keep their state because only
// the lifecycle owner may settle a claim), the latest evidence replaces the
// recorded evidence, and an append-only reverify audit row records the
// invalidation. No automatic disposal/recovery/payment is ever triggered.
func (o *scanIntervalOutcome) invalidateTxAggregateTx(ctx context.Context, tx pgx.Tx,
	root *scanAggregateRoot, primary Classification, members []TxAggregateMember) error {
	domain, err := PersistedEvidenceDomainJSONFor(primary.Identity.VersionDomain(), &o.scope,
		primary.BusinessType, detectionIntervalOf(o.interval), members)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, invalidateTxAggregateSQL,
		root.ID, primary.Identity.ContentHash().Bytes(), string(domain)); err != nil {
		return fmt.Errorf("invalidate tx aggregate ticket: %w", err)
	}
	return insertAuditTx(ctx, tx, AuditRecord{
		Actor:  o.owner,
		Action: AuditActionReverify,
		Target: map[string]any{
			"discrepancy_id": root.ID.String(),
			"task_id":        o.taskID,
			"attempt_id":     o.attemptID,
			"business_key":   o.scanBusinessKey(primary),
			"recorded_block": strings.TrimSpace(root.Domain.BlockHash),
			"observed_block": strings.TrimSpace(primary.Identity.VersionDomain().BlockHash),
			"trigger":        string(InvalidationReorg),
		},
		Reason: "tx aggregate evidence changed (reorg replacement or new evidence); pending reverify",
		Result: "invalidated",
	})
}

// scanBusinessKey renders the persisted business-key record of a detection.
func (o *scanIntervalOutcome) scanBusinessKey(c Classification) string {
	return scanBusinessKeyRecord(c.Identity.BusinessKey())
}

// insertScanDiscrepancyTx inserts the stable-identity ticket row if it does
// not exist yet; a conflict means the same identity was already recorded
// (dedup) and the existing lifecycle state is left untouched. The persisted
// evidence domain carries the detection scope (T035 aggregate-root matching),
// the detection business type and claimed interval, and (for tx-aggregate
// groups) the member log set, so the production pending_verify
// re-verification can re-read exactly the same window and prove member
// completeness.
func (o *scanIntervalOutcome) insertScanDiscrepancyTx(ctx context.Context, tx pgx.Tx,
	classification Classification, members []TxAggregateMember) (bool, error) {
	identity := classification.Identity
	versionDomain, err := PersistedEvidenceDomainJSONFor(identity.VersionDomain(), &o.scope,
		classification.BusinessType, detectionIntervalOf(o.interval), members)
	if err != nil {
		return false, err
	}
	tag, err := tx.Exec(ctx, insertDiscrepancySQL,
		identity.ID(), string(classification.Category), scanBusinessKeyRecord(identity.BusinessKey()),
		identity.ContentHash().Bytes(), string(versionDomain))
	if err != nil {
		return false, fmt.Errorf("insert discrepancy: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// scanEvidenceRef bounds the occurrence evidence reference to the migration
// CHECK; an empty/oversized candidate ref is replaced by a synthesized one.
func scanEvidenceRef(classification *Classification, attemptID string) string {
	if classification != nil {
		if ref := strings.TrimSpace(classification.EvidenceRef); ref != "" && len(ref) <= scanEvidenceRefMax {
			return ref
		}
		return fmt.Sprintf("scan:v1 attempt=%s reason=%s", attemptID, classification.Reason)
	}
	return fmt.Sprintf("scan:v1 attempt=%s", attemptID)
}

// scanMemberEvidenceRef bounds one member's occurrence evidence to the
// migration CHECK (the attempt identity is appended when it still fits).
func scanMemberEvidenceRef(member TxAggregateMember, attemptID string) string {
	ref, err := TxAggregateEvidenceRef([]TxAggregateMember{member}, scanEvidenceRefMax)
	if err != nil {
		return fmt.Sprintf("scan:v1 attempt=%s", attemptID)
	}
	suffix := " attempt=" + attemptID
	if len(ref)+len(suffix) <= scanEvidenceRefMax {
		return ref + suffix
	}
	return ref
}

// scanAuditReason summarizes the pending reasons of one batch (bounded,
// secret-free).
func scanAuditReason(o *scanIntervalOutcome) string {
	reasons := make([]string, 0, len(o.gapReasons))
	for _, reason := range o.gapReasons {
		reasons = append(reasons, string(reason))
	}
	sort.Strings(reasons)
	return strings.Join(reasons, ",")
}

// suspendScanOnce stops new work on budget exhaustion and makes the
// suspension observable and safe (FR-019, Q3): the in-flight attempt (when
// one is held) is settled boundedly as superseded with a gap row, the task
// moves running -> suspended_budget with the budget reason recorded, and the
// pointer never moves. A concurrent operator pause/cancel wins: the refused
// transition is audited and the task's actual state is reported.
func (s *Store) suspendScanOnce(ctx context.Context, req *ScanOnceRequest, budget *Budget,
	result ScanOnceResult, hasClaim bool) (ScanOnceResult, error) {
	resource, detail, suspended := budget.Suspended()
	if !suspended {
		resource, detail = ResourceDuration, durationLimitDetail
	}
	result.Suspended = true
	result.SuspendResource = resource
	result.SuspendDetail = detail
	reason := fmt.Sprintf("reconciliation budget exhausted (%s): %s", resource, detail)

	if hasClaim {
		if _, err := s.SettleInFlight(ctx, SettleInFlightRequest{
			TaskID: req.TaskID,
			Mode:   SettleModePause,
			Limit:  scanPositiveLimit(req.Limits.MaxConcurrency),
			Reason: reason,
			Actor:  req.Owner,
		}); err != nil {
			return result, err
		}
	}

	if _, err := s.TransitionTask(ctx, TaskTransitionRequest{
		TaskID: req.TaskID,
		To:     TaskStateSuspendedBudget,
		Reason: reason,
		Actor:  req.Owner,
	}); err != nil {
		if errors.Is(err, ErrIllegalTaskTransition) {
			// The task already left running (operator pause/cancel): the
			// refusal was audited by TransitionTask and the operator state
			// wins. The suspension remains observable in the result.
			if current, loadErr := s.TaskByID(ctx, req.TaskID); loadErr == nil {
				result.StoppedState = current.State
			} else {
				result.StoppedState = TaskStatePaused
			}
			return result, nil
		}
		return result, err
	}
	result.StoppedState = TaskStateSuspendedBudget
	return result, nil
}
