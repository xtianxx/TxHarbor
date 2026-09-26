// store.go implements the T010 result+checkpoint atomic commit helper
// (data-model.md §5/§5.1, contracts/task-lifecycle.md Claim–Execute–Commit):
//
//   - one short transaction persists the batch results, appends the
//     checkpoint row and marks the attempt done, in that order;
//   - the pointer only advances over the contiguous persisted prefix;
//   - a late submitter whose attempt is superseded/abandoned (or whose lease
//     expired, or whose task left running) is discarded and audited, and the
//     pointer never moves;
//   - crash recovery abandons stale claimed attempts (leaving gap rows,
//     pointer untouched) and pause/cancel settle in-flight attempts within a
//     bounded limit;
//   - no DB transaction is ever held across a slow RPC: the commit
//     transaction only ever runs the DB-only PersistScanResults callback.
//
// The pattern follows internal/events/publisher.go:224 ClaimBatch: claim in a
// short locked transaction, then execute and commit without holding the lock,
// and only then a separate short transaction.
package reconciliation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// sqlStateUniqueViolation is PostgreSQL's unique-violation SQLSTATE.
const sqlStateUniqueViolation = "23505"

// StoreDB is the minimal pgx surface the 014 store needs; *pgxpool.Pool
// satisfies it. Read helpers use QueryRow; every mutation runs inside Begin.
type StoreDB interface {
	Begin(ctx context.Context) (pgx.Tx, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Store owns the 014 persistence helpers (T006/T007/T010). The migration that
// creates the tables is 000016 (T004); table and column names follow
// data-model.md §1.1/§1.2/§1.3/§1.8/§1.9 verbatim.
type Store struct {
	db StoreDB
}

// NewStore builds a store over db. A nil database is refused fail-closed.
func NewStore(db StoreDB) (*Store, error) {
	if db == nil {
		return nil, contractErrorf("store requires a database")
	}
	return &Store{db: db}, nil
}

// Sentinel classification errors. Illegal transitions, late submitters and
// unpersisted pointers are refusals: they are audited, never silently applied.
var (
	// ErrContract is a structural/contract violation (bad input, unknown
	// enum value, broken invariant): non-retryable, must alert.
	ErrContract = errors.New("reconciliation: contract violation")

	// ErrTaskNotFound means no recon_task row has the requested id.
	ErrTaskNotFound = errors.New("reconciliation: task not found")
	// ErrTaskNotRunning means the task left running before a claim/commit.
	ErrTaskNotRunning = errors.New("reconciliation: task is not running")
	// ErrTaskNotComplete means `done` was requested while the pointer still
	// sits short of the scope end or open gap rows remain.
	ErrTaskNotComplete = errors.New("reconciliation: task scope is not fully persisted or still has open gaps")
	// ErrIllegalTaskTransition means the requested task-state edge is not in
	// the task lifecycle (contracts/task-lifecycle.md); it is refused and
	// audited.
	ErrIllegalTaskTransition = errors.New("reconciliation: illegal task transition")

	// ErrScopeExhausted means the persisted prefix already reaches the scope
	// end: no claimable interval remains.
	ErrScopeExhausted = errors.New("reconciliation: scope has no uncovered range left")
	// ErrClaimTaken means another attempt already holds the task's claimed
	// interval (recon_scan_attempt_claimed_uniq); retry the next interval.
	ErrClaimTaken = errors.New("reconciliation: scan interval already claimed")
	// ErrAttemptNotFound means no recon_scan_attempt row matches.
	ErrAttemptNotFound = errors.New("reconciliation: scan attempt not found")
	// ErrAttemptDiscarded means the submitter is late: the attempt is
	// superseded/abandoned/expired (or the task is no longer running), the
	// results were discarded and an audit row was written. The returned
	// CommitScanBatchResult carries the discard reason.
	ErrAttemptDiscarded = errors.New("reconciliation: scan attempt is not current; results discarded")

	// ErrDiscrepancyNotFound means no discrepancy row matches the id.
	ErrDiscrepancyNotFound = errors.New("reconciliation: discrepancy not found")
	// ErrIllegalDiscrepancyTransition means the requested discrepancy edge is
	// not in the lifecycle or its guard failed; it is refused and audited.
	ErrIllegalDiscrepancyTransition = errors.New("reconciliation: illegal discrepancy transition")
	// ErrDispositionRequired means dispose -> pending_verify was attempted
	// without a recorded effective disposition (disposed ≠ reverified).
	ErrDispositionRequired = errors.New("reconciliation: a recorded effective disposition is required before verification")
	// ErrReverifyRequired means close was attempted without a fresh,
	// consistent reverify row (stale/incomplete/unknown evidence can never
	// close; FR-010/Q5).
	ErrReverifyRequired = errors.New("reconciliation: a fresh consistent reverify is required to close")
	// ErrInvalidationIgnored means the change signal does not affect the
	// recorded conclusion (unrelated writes must not reopen; Q5).
	ErrInvalidationIgnored = errors.New("reconciliation: change signal does not affect the recorded conclusion")
	// ErrRecurrenceUnconfirmed means reopen was attempted without confirmed
	// recurrence evidence.
	ErrRecurrenceUnconfirmed = errors.New("reconciliation: recurrence is not confirmed")
)

// contractErrorf wraps a contract violation with context while keeping
// errors.Is(err, ErrContract) true.
func contractErrorf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrContract, fmt.Sprintf(format, args...))
}

// uniqueViolationConstraint returns the violated constraint name when err is a
// PostgreSQL unique violation, or "" otherwise.
func uniqueViolationConstraint(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == sqlStateUniqueViolation {
		return pgErr.ConstraintName
	}
	return ""
}

// AuditAction mirrors the closed recon_audit.action ENUM (data-model.md §1.8).
type AuditAction string

const (
	AuditActionQuery    AuditAction = "query"
	AuditActionStart    AuditAction = "start"
	AuditActionPause    AuditAction = "pause"
	AuditActionResume   AuditAction = "resume"
	AuditActionClaim    AuditAction = "claim"
	AuditActionDispose  AuditAction = "dispose"
	AuditActionReverify AuditAction = "reverify"
	AuditActionClose    AuditAction = "close"
	AuditActionReopen   AuditAction = "reopen"
	AuditActionRefuse   AuditAction = "refuse"
)

// Valid reports whether a is one of the closed audit actions.
func (a AuditAction) Valid() bool {
	switch a {
	case AuditActionQuery, AuditActionStart, AuditActionPause, AuditActionResume,
		AuditActionClaim, AuditActionDispose, AuditActionReverify, AuditActionClose,
		AuditActionReopen, AuditActionRefuse:
		return true
	}
	return false
}

// AuditRecord is one append-only recon_audit row (data-model.md §1.8).
// Target is marshaled to JSONB; Reason/Evidence/Result are free text.
type AuditRecord struct {
	Actor    string
	Action   AuditAction
	Target   any
	Reason   string
	Evidence string
	Result   string
}

// insertAuditTx appends one audit row inside the caller's transaction. Refused
// and discarded operations are audited with their observed basis; the row is
// part of the same atomic decision, never a best-effort afterthought.
func insertAuditTx(ctx context.Context, tx pgx.Tx, rec AuditRecord) error {
	if strings.TrimSpace(rec.Actor) == "" {
		return contractErrorf("audit record requires an actor")
	}
	if !rec.Action.Valid() {
		return contractErrorf("unknown audit action %q", rec.Action)
	}
	target := []byte(`{}`)
	if rec.Target != nil {
		encoded, err := json.Marshal(rec.Target)
		if err != nil {
			return fmt.Errorf("marshal audit target: %w", err)
		}
		target = encoded
	}
	if _, err := tx.Exec(ctx, insertAuditSQL,
		rec.Actor, rec.Action, string(target), rec.Reason, rec.Evidence, rec.Result); err != nil {
		return fmt.Errorf("insert audit row: %w", err)
	}
	return nil
}

// AttemptState mirrors recon_scan_attempt.state (data-model.md §1.9).
type AttemptState string

const (
	// AttemptStateClaimed: the current owner of the task's claimed interval.
	AttemptStateClaimed AttemptState = "claimed"
	// AttemptStateDone: results and checkpoint were committed.
	AttemptStateDone AttemptState = "done"
	// AttemptStateAbandoned: the lease expired or the attempt was cancelled;
	// a late submit is discarded.
	AttemptStateAbandoned AttemptState = "abandoned"
	// AttemptStateSuperseded: pause/cancel/a newer claim replaced it; a late
	// submit is discarded.
	AttemptStateSuperseded AttemptState = "superseded"
)

// Valid reports whether s is one of the closed attempt states.
func (s AttemptState) Valid() bool {
	switch s {
	case AttemptStateClaimed, AttemptStateDone, AttemptStateAbandoned, AttemptStateSuperseded:
		return true
	}
	return false
}

// ScanAttempt is one recon_scan_attempt ownership record.
type ScanAttempt struct {
	AttemptID    string
	TaskID       string
	State        AttemptState
	Owner        string
	RangeStart   RangeBound
	RangeEnd     RangeBound
	LeaseExpired bool
}

// Discard reasons recorded on late-submitter audit rows.
const (
	// DiscardReasonTaskNotRunning: paused/suspended/cancelled/done tasks can
	// no longer commit work.
	DiscardReasonTaskNotRunning = "task_not_running"
	// DiscardReasonLeaseExpired: the claim lease lapsed; ownership is gone.
	DiscardReasonLeaseExpired = "lease_expired"
	// DiscardReasonAttemptNotCurrent: the attempt is already
	// superseded/abandoned.
	DiscardReasonAttemptNotCurrent = "attempt_not_current"
	// DiscardReasonAttemptAlreadyDone: the attempt already committed its
	// batch; a repeat submit must not append a second checkpoint.
	DiscardReasonAttemptAlreadyDone = "attempt_already_done"
	// DiscardReasonNonContiguous: the attempt range no longer starts right
	// after the persisted prefix (claim protocol violation/overlap).
	DiscardReasonNonContiguous = "non_contiguous_range"
)

// SQL statement constants. Table and column names follow migration
// 000016_reconciliation_handling.sql (T004) exactly: height ranges use the
// BIGINT pair (scope_start/scope_end, covered_through,
// result_persisted_through, range_start/range_end) and time ranges use the
// TIMESTAMPTZ pair (*_at). Every mutation locks recon_task first (package lock
// order: task -> attempt/checkpoint/gap) before touching its rows.

const taskSelectSQL = `
SELECT task_id, scope_chain_id, scope_kind, scope_start, scope_start_at, scope_end, scope_end_at,
       state, COALESCE(pause_reason, ''), business_types, upstream_receipt_source, policy_refs
FROM recon_task
WHERE task_id = $1`

const lockTaskSQL = taskSelectSQL + `
FOR UPDATE`

const checkpointHeadSQL = `
SELECT seq, covered_through, covered_through_at,
       result_persisted_through, result_persisted_through_at, created_at
FROM recon_checkpoint
WHERE task_id = $1
ORDER BY seq DESC
LIMIT 1`

const checkpointHeadJoinSQL = `
SELECT t.scope_kind, c.seq, c.covered_through, c.covered_through_at,
       c.result_persisted_through, c.result_persisted_through_at, c.created_at
FROM recon_checkpoint c
JOIN recon_task t ON t.task_id = c.task_id
WHERE c.task_id = $1
ORDER BY c.seq DESC
LIMIT 1`

const insertAttemptSQL = `
INSERT INTO recon_scan_attempt (
    attempt_id, task_id, range_start, range_end, range_start_at, range_end_at,
    state, owner, lease_expires_at)
VALUES ($1, $2, $3, $4, $5, $6, 'claimed', $7,
        now() + make_interval(secs => $8::double precision))
RETURNING lease_expires_at`

const lockAttemptSQL = `
SELECT state, owner, range_start, range_start_at, range_end, range_end_at,
       COALESCE(lease_expires_at <= now(), false)
FROM recon_scan_attempt
WHERE attempt_id = $1 AND task_id = $2
FOR UPDATE`

const markAttemptSQL = `
UPDATE recon_scan_attempt
SET state = $2, updated_at = now()
WHERE attempt_id = $1 AND state = 'claimed'`

const insertCheckpointSQL = `
INSERT INTO recon_checkpoint (
    task_id, seq, covered_through, covered_through_at,
    result_persisted_through, result_persisted_through_at)
VALUES ($1, (SELECT COALESCE(MAX(seq), 0) + 1 FROM recon_checkpoint WHERE task_id = $1),
        $2, $3, $2, $3)
RETURNING seq, created_at`

const insertGapSQL = `
INSERT INTO recon_gap (task_id, range_start, range_end, range_start_at, range_end_at, reason)
VALUES ($1, $2, $3, $4, $5, $6)`

const countGapsSQL = `SELECT count(*)::bigint FROM recon_gap WHERE task_id = $1`

const insertAuditSQL = `
INSERT INTO recon_audit (actor, action, target, reason, evidence, result)
VALUES ($1, $2, $3::jsonb, $4, $5, $6)`

const updateTaskStateSQL = `
UPDATE recon_task
SET state = $2, pause_reason = NULLIF($3, ''), updated_at = now()
WHERE task_id = $1 AND state = $4`

const recoverStaleAttemptsSQL = `
UPDATE recon_scan_attempt
SET state = 'abandoned', updated_at = now()
WHERE attempt_id IN (
    SELECT attempt_id FROM recon_scan_attempt
    WHERE task_id = $1 AND state = 'claimed'
      AND lease_expires_at IS NOT NULL AND lease_expires_at <= now()
    ORDER BY lease_expires_at, attempt_id
    LIMIT $2
)
RETURNING attempt_id, range_start, range_start_at, range_end, range_end_at`

const settleInFlightSQL = `
UPDATE recon_scan_attempt
SET state = $2, updated_at = now()
WHERE attempt_id IN (
    SELECT attempt_id FROM recon_scan_attempt
    WHERE task_id = $1 AND state = 'claimed'
    ORDER BY created_at, attempt_id
    LIMIT $3
)
RETURNING attempt_id, range_start, range_start_at, range_end, range_end_at`

// PersistScanResults writes one batch's comparison results (discrepancy /
// occurrence / reverify rows) inside the commit transaction. It MUST NOT
// perform RPC or any other network I/O and MUST NOT re-lock recon_task: the
// commit transaction is short by contract and the slow reads already happened
// outside it (data-model.md §5.1).
type PersistScanResults func(ctx context.Context, tx pgx.Tx) error

// CommitScanBatchRequest commits one finished execution phase.
type CommitScanBatchRequest struct {
	TaskID    string
	AttemptID string
	// PersistResults is mandatory: the same transaction must persist the
	// batch results before the pointer moves (data-model.md §5 ordering:
	// results -> result_persisted_through -> checkpoint row).
	PersistResults PersistScanResults
}

// CommitScanBatchResult reports the outcome. Discarded is true for late
// submitters; the audit row was written and the pointer did not move.
type CommitScanBatchResult struct {
	Committed     bool
	Discarded     bool
	DiscardReason string
	Checkpoint    *Checkpoint
}

// CommitScanBatch is the commit half of the claim-execute-commit protocol. In
// one short transaction it locks the task and the attempt, refuses (discards +
// audits) late submitters, requires the attempt range to start exactly at the
// contiguous persisted prefix, runs PersistResults, appends the checkpoint row
// and marks the attempt done. Any error rolls the whole transaction back: the
// pointer never advances past unpersisted results.
func (s *Store) CommitScanBatch(ctx context.Context, req CommitScanBatchRequest) (CommitScanBatchResult, error) {
	if s == nil || s.db == nil {
		return CommitScanBatchResult{}, contractErrorf("store has no database")
	}
	if strings.TrimSpace(req.TaskID) == "" || strings.TrimSpace(req.AttemptID) == "" {
		return CommitScanBatchResult{}, contractErrorf("commit requires task_id and attempt_id")
	}
	if req.PersistResults == nil {
		return CommitScanBatchResult{}, contractErrorf("commit requires a results persistence callback")
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return CommitScanBatchResult{}, fmt.Errorf("begin scan commit: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	task, err := lockTaskTx(ctx, tx, req.TaskID)
	if err != nil {
		return CommitScanBatchResult{}, err
	}
	attempt, err := lockAttemptTx(ctx, tx, task, req.AttemptID)
	if err != nil {
		return CommitScanBatchResult{}, err
	}

	// Validity: only the current claimed attempt of a running, unexpired
	// task may persist results and move the pointer (§5.1).
	reason := ""
	switch {
	case attempt.State != AttemptStateClaimed:
		if attempt.State == AttemptStateDone {
			reason = DiscardReasonAttemptAlreadyDone
		} else {
			reason = DiscardReasonAttemptNotCurrent
		}
	case task.State != TaskStateRunning:
		reason = DiscardReasonTaskNotRunning
	case attempt.LeaseExpired:
		reason = DiscardReasonLeaseExpired
	}
	if reason != "" {
		return s.discardScanBatchTx(ctx, tx, task, attempt, reason)
	}

	// The attempt's interval must begin exactly where the persisted prefix
	// ends. Anything else is a claim-protocol violation: discard, leave a gap
	// and audit rather than moving the pointer over an unpersisted range.
	head, err := readCheckpointHeadTx(ctx, tx, task.TaskID, task.ScopeKind)
	if err != nil {
		return CommitScanBatchResult{}, err
	}
	expected, err := NextClaimStart(head, task.ScopeStart)
	if err != nil {
		return CommitScanBatchResult{}, err
	}
	if cmp, err := attempt.RangeStart.Compare(expected); err != nil {
		return CommitScanBatchResult{}, err
	} else if cmp != 0 {
		return s.discardScanBatchTx(ctx, tx, task, attempt, DiscardReasonNonContiguous)
	}
	if cmp, err := attempt.RangeEnd.Compare(task.ScopeEnd); err != nil {
		return CommitScanBatchResult{}, err
	} else if cmp > 0 {
		return CommitScanBatchResult{}, contractErrorf("attempt range end %v is past scope end %v",
			attempt.RangeEnd, task.ScopeEnd)
	}

	if err := req.PersistResults(ctx, tx); err != nil {
		// The whole transaction rolls back: results, pointer and attempt
		// state stay untouched and the batch can be re-executed.
		return CommitScanBatchResult{}, fmt.Errorf("persist scan results: %w", err)
	}

	height, at := rangePairArgs(attempt.RangeEnd)
	var (
		seq       int64
		createdAt time.Time
	)
	if err := tx.QueryRow(ctx, insertCheckpointSQL, task.TaskID, height, at).
		Scan(&seq, &createdAt); err != nil {
		return CommitScanBatchResult{}, fmt.Errorf("append checkpoint: %w", err)
	}
	tag, err := tx.Exec(ctx, markAttemptSQL, attempt.AttemptID, AttemptStateDone)
	if err != nil {
		return CommitScanBatchResult{}, fmt.Errorf("mark attempt done: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return CommitScanBatchResult{}, fmt.Errorf("%w: attempt changed under lock", ErrAttemptDiscarded)
	}

	if err := tx.Commit(ctx); err != nil {
		return CommitScanBatchResult{}, fmt.Errorf("commit scan batch: %w", err)
	}
	return CommitScanBatchResult{
		Committed: true,
		Checkpoint: &Checkpoint{
			TaskID:                 task.TaskID,
			Seq:                    seq,
			CoveredThrough:         attempt.RangeEnd,
			ResultPersistedThrough: attempt.RangeEnd,
			CreatedAt:              createdAt,
		},
	}, nil
}

// discardScanBatchTx settles an invalid attempt, leaves its interval visible
// as a gap, writes the refuse audit row and commits the discard. It returns
// the discard result together with ErrAttemptDiscarded.
func (s *Store) discardScanBatchTx(ctx context.Context, tx pgx.Tx, task *Task, attempt *ScanAttempt, reason string) (CommitScanBatchResult, error) {
	if attempt.State == AttemptStateClaimed {
		next := AttemptStateAbandoned
		gapReason := GapInterrupted
		switch task.State {
		case TaskStatePaused:
			next, gapReason = AttemptStateSuperseded, GapPaused
		case TaskStateSuspendedBudget:
			next, gapReason = AttemptStateSuperseded, GapBudgetExhausted
		}
		if _, err := tx.Exec(ctx, markAttemptSQL, attempt.AttemptID, next); err != nil {
			return CommitScanBatchResult{}, fmt.Errorf("settle attempt: %w", err)
		}
		if err := insertGapTx(ctx, tx, task.TaskID, attempt.RangeStart, attempt.RangeEnd, gapReason); err != nil {
			return CommitScanBatchResult{}, err
		}
	}

	actor := attempt.Owner
	if strings.TrimSpace(actor) == "" {
		actor = "system"
	}
	if err := insertAuditTx(ctx, tx, AuditRecord{
		Actor:  actor,
		Action: AuditActionRefuse,
		Target: attemptAuditTarget(attempt, task.State),
		Reason: reason,
		Result: "discarded",
	}); err != nil {
		return CommitScanBatchResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return CommitScanBatchResult{}, fmt.Errorf("commit discard audit: %w", err)
	}
	return CommitScanBatchResult{Discarded: true, DiscardReason: reason},
		fmt.Errorf("%w: %s", ErrAttemptDiscarded, reason)
}

// lockAttemptTx locks one attempt row of the task FOR UPDATE and materializes
// it. Range columns are decoded against the task's scope kind.
func lockAttemptTx(ctx context.Context, tx pgx.Tx, task *Task, attemptID string) (*ScanAttempt, error) {
	attempt := &ScanAttempt{AttemptID: attemptID, TaskID: task.TaskID}
	var (
		rawStart, rawEnd     *int64
		rawStartAt, rawEndAt *time.Time
	)
	err := tx.QueryRow(ctx, lockAttemptSQL, attemptID, task.TaskID).
		Scan(&attempt.State, &attempt.Owner, &rawStart, &rawStartAt, &rawEnd, &rawEndAt,
			&attempt.LeaseExpired)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: attempt_id %s", ErrAttemptNotFound, attemptID)
	}
	if err != nil {
		return nil, fmt.Errorf("lock attempt: %w", err)
	}
	if !attempt.State.Valid() {
		return nil, contractErrorf("attempt %s has unknown state %q", attemptID, attempt.State)
	}
	if attempt.RangeStart, err = boundFromPair(task.ScopeKind, rawStart, rawStartAt); err != nil {
		return nil, err
	}
	if attempt.RangeEnd, err = boundFromPair(task.ScopeKind, rawEnd, rawEndAt); err != nil {
		return nil, err
	}
	return attempt, nil
}

// attemptAuditTarget renders the audit target of one attempt settlement.
func attemptAuditTarget(attempt *ScanAttempt, taskState TaskState) map[string]any {
	return map[string]any{
		"task_id":       attempt.TaskID,
		"attempt_id":    attempt.AttemptID,
		"attempt_state": string(attempt.State),
		"task_state":    string(taskState),
		"range_start":   rangeAuditValue(attempt.RangeStart),
		"range_end":     rangeAuditValue(attempt.RangeEnd),
	}
}

// RecoverStaleAttemptsRequest bounds one crash-recovery pass.
type RecoverStaleAttemptsRequest struct {
	TaskID string
	// Limit bounds how many stale attempts are settled per pass (no unbounded
	// work, FR-019). Must be positive.
	Limit int
	// Actor is the audit actor; empty defaults to "system:recovery".
	Actor string
}

// RecoverStaleAttemptsResult lists the abandoned attempts.
type RecoverStaleAttemptsResult struct {
	Abandoned []string
}

// RecoverStaleAttempts implements crash-safe resume (§5.1): after a restart,
// claimed attempts whose lease expired are moved to abandoned, their intervals
// are recorded as interrupted gap rows, and an audit row documents each
// abandonment. The pointer never moves; the scanner re-claims from the
// persisted prefix and rescans covered ranges idempotently.
func (s *Store) RecoverStaleAttempts(ctx context.Context, req RecoverStaleAttemptsRequest) (RecoverStaleAttemptsResult, error) {
	if s == nil || s.db == nil {
		return RecoverStaleAttemptsResult{}, contractErrorf("store has no database")
	}
	if strings.TrimSpace(req.TaskID) == "" {
		return RecoverStaleAttemptsResult{}, contractErrorf("recovery requires a task_id")
	}
	if req.Limit <= 0 {
		return RecoverStaleAttemptsResult{}, contractErrorf("recovery limit must be positive")
	}
	actor := strings.TrimSpace(req.Actor)
	if actor == "" {
		actor = "system:recovery"
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return RecoverStaleAttemptsResult{}, fmt.Errorf("begin stale recovery: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	task, err := lockTaskTx(ctx, tx, req.TaskID)
	if err != nil {
		return RecoverStaleAttemptsResult{}, err
	}
	abandoned, err := settleAttemptsTx(ctx, tx, task, recoverStaleAttemptsSQL, "", req.TaskID, req.Limit, GapInterrupted, actor, "lease_expired")
	if err != nil {
		return RecoverStaleAttemptsResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return RecoverStaleAttemptsResult{}, fmt.Errorf("commit stale recovery: %w", err)
	}
	return RecoverStaleAttemptsResult{Abandoned: abandoned}, nil
}

// SettleMode selects how in-flight attempts are settled.
type SettleMode string

const (
	// SettleModePause: pause settles claimed attempts as superseded.
	SettleModePause SettleMode = "pause"
	// SettleModeCancel: cancel settles claimed attempts as abandoned.
	SettleModeCancel SettleMode = "cancel"
)

// Valid reports whether m is a known settle mode.
func (m SettleMode) Valid() bool { return m == SettleModePause || m == SettleModeCancel }

// SettleInFlightRequest bounds one pause/cancel settle pass (FR-003/FR-025:
// in-flight work completes or cancels boundedly; paused scans claim nothing).
type SettleInFlightRequest struct {
	TaskID string
	Mode   SettleMode
	// Limit bounds how many in-flight attempts are settled per pass. Must be
	// positive.
	Limit  int
	Reason string
	Actor  string
}

// SettleInFlightResult lists the settled attempts.
type SettleInFlightResult struct {
	Settled []string
}

// SettleInFlight settles the claimed attempts of a task within a bounded
// limit: pause marks them superseded (gap reason paused), cancel marks them
// abandoned (gap reason interrupted). Late submissions from settled attempts
// are discarded and audited by CommitScanBatch; the pointer never moves here.
func (s *Store) SettleInFlight(ctx context.Context, req SettleInFlightRequest) (SettleInFlightResult, error) {
	if s == nil || s.db == nil {
		return SettleInFlightResult{}, contractErrorf("store has no database")
	}
	if strings.TrimSpace(req.TaskID) == "" {
		return SettleInFlightResult{}, contractErrorf("settle requires a task_id")
	}
	if !req.Mode.Valid() {
		return SettleInFlightResult{}, contractErrorf("unknown settle mode %q", req.Mode)
	}
	if req.Limit <= 0 {
		return SettleInFlightResult{}, contractErrorf("settle limit must be positive")
	}
	if strings.TrimSpace(req.Actor) == "" {
		return SettleInFlightResult{}, contractErrorf("settle requires an actor")
	}
	next, gapReason := AttemptStateSuperseded, GapPaused
	if req.Mode == SettleModeCancel {
		next, gapReason = AttemptStateAbandoned, GapInterrupted
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return SettleInFlightResult{}, fmt.Errorf("begin in-flight settle: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	task, err := lockTaskTx(ctx, tx, req.TaskID)
	if err != nil {
		return SettleInFlightResult{}, err
	}
	settled, err := settleAttemptsTx(ctx, tx, task, settleInFlightSQL, next, req.TaskID, req.Limit, gapReason, req.Actor, strings.TrimSpace(req.Reason))
	if err != nil {
		return SettleInFlightResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return SettleInFlightResult{}, fmt.Errorf("commit in-flight settle: %w", err)
	}
	return SettleInFlightResult{Settled: settled}, nil
}

// settleAttemptsTx runs one bounded settle statement, then records a gap row
// and a claim-lifecycle audit row per settled attempt. next is empty for the
// recovery statement (it sets 'abandoned' itself).
func settleAttemptsTx(ctx context.Context, tx pgx.Tx, task *Task, sql string, next AttemptState,
	taskID string, limit int, gapReason GapReason, actor, reason string) ([]string, error) {
	args := []any{taskID}
	if next != "" {
		args = append(args, next)
	}
	args = append(args, limit)

	rows, err := tx.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("settle attempts: %w", err)
	}
	type settledAttempt struct {
		id         string
		rangeStart RangeBound
		rangeEnd   RangeBound
	}
	var settled []settledAttempt
	for rows.Next() {
		var (
			rawStart, rawEnd     *int64
			rawStartAt, rawEndAt *time.Time
			item                 settledAttempt
		)
		if err := rows.Scan(&item.id, &rawStart, &rawStartAt, &rawEnd, &rawEndAt); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan settled attempt: %w", err)
		}
		if item.rangeStart, err = boundFromPair(task.ScopeKind, rawStart, rawStartAt); err != nil {
			rows.Close()
			return nil, err
		}
		if item.rangeEnd, err = boundFromPair(task.ScopeKind, rawEnd, rawEndAt); err != nil {
			rows.Close()
			return nil, err
		}
		settled = append(settled, item)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("settle attempt rows: %w", err)
	}

	ids := make([]string, 0, len(settled))
	auditResult := string(next)
	if auditResult == "" {
		auditResult = string(AttemptStateAbandoned)
	}
	for _, item := range settled {
		if err := insertGapTx(ctx, tx, taskID, item.rangeStart, item.rangeEnd, gapReason); err != nil {
			return nil, err
		}
		if err := insertAuditTx(ctx, tx, AuditRecord{
			Actor:  actor,
			Action: AuditActionClaim,
			Target: map[string]any{
				"task_id":     taskID,
				"attempt_id":  item.id,
				"range_start": rangeAuditValue(item.rangeStart),
				"range_end":   rangeAuditValue(item.rangeEnd),
			},
			Reason: reason,
			Result: auditResult,
		}); err != nil {
			return nil, err
		}
		ids = append(ids, item.id)
	}
	return ids, nil
}
