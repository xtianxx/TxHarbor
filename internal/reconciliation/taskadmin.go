// taskadmin.go implements the T018 task-row half of the reconcile-admin
// surface: `start(scope, budget) -> created` (contracts/task-lifecycle.md
// Operations) with its append-only audit row. Activation is a separate,
// auditable step (`resume` applies created -> running through the T006 state
// machine in scan.go); creation and activation are deliberately not fused so
// the recorded state machine stays exactly the contract's.
//
// This file is additive: the T006/T016 state machine, claim protocol and
// compare loop are untouched. Task rows are the only object created here, and
// only the 014-owned recon_task/recon_audit tables are written.
package reconciliation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// TaskCreateRequest is one start(scope, budget): the recorded scope, the
// upstream receipt declaration of the task, and the budget snapshot. The
// concrete invocation budget (T008 BudgetLimits) is passed to each `scan`
// call; the task row carries an operator-visible snapshot for reproducibility
// (FR-001), never an authorization source.
type TaskCreateRequest struct {
	// TaskID is the caller-supplied UUID; empty generates one. A malformed
	// value is refused (the column is UUID).
	TaskID string
	// ChainID is the scope chain identity (numeric for the current read
	// adapters; a symbolic value is accepted but leaves parties unknown).
	ChainID string
	// Kind is the scope dimension; Start/End must match it.
	Kind ScopeKind
	// Start/End are the inclusive, reproducible scope bounds.
	Start RangeBound
	End   RangeBound
	// BusinessTypes is the closed business-type set of the scope. Unknown or
	// duplicate types are refused.
	BusinessTypes []BusinessType
	// UpstreamReceipts is the per-business-type upstream receipt source
	// declaration (data-model.md §1.1). A missing declaration counts as
	// unconnected: the classifier can then never claim external success
	// (FR-006).
	UpstreamReceipts []ChainUpstreamReceiptSource
	// PolicyRefs is the optional raw policy_refs JSONB snapshot (confirm
	// policy seq, cutover/catalog versions). Empty records `{}`.
	PolicyRefs []byte
	// Budget is the optional raw budget JSONB snapshot. Empty records `{}`.
	Budget []byte
	// CreatedBy is the authenticated principal recorded as the audit actor.
	CreatedBy string
	// Reason is an audit-only free-text annotation.
	Reason string
}

// Validate checks the create request conservatively before any write.
func (r TaskCreateRequest) Validate() error {
	if strings.TrimSpace(r.ChainID) == "" || len(r.ChainID) > 128 ||
		strings.ContainsAny(r.ChainID, "\x00\n\r\t") {
		return contractErrorf("task scope requires a chain id of 1..128 characters")
	}
	if !r.Kind.Known() {
		return contractErrorf("unknown task scope kind %q", r.Kind)
	}
	if !r.Start.Valid() || !r.End.Valid() {
		return contractErrorf("task scope requires valid start and end bounds")
	}
	if r.Start.Kind != r.Kind || r.End.Kind != r.Kind {
		return contractErrorf("task scope bounds do not match scope kind %q", r.Kind)
	}
	cmp, err := r.Start.Compare(r.End)
	if err != nil {
		return err
	}
	if cmp > 0 {
		return contractErrorf("task scope is not ascending")
	}
	types, err := canonicalBusinessTypes(r.BusinessTypes)
	if err != nil {
		return err
	}
	if len(types) == 0 {
		return contractErrorf("task scope requires at least one business type")
	}
	seen := make(map[BusinessType]struct{}, len(r.UpstreamReceipts))
	for _, receipt := range r.UpstreamReceipts {
		if err := receipt.Validate(); err != nil {
			return err
		}
		if _, dup := seen[receipt.BusinessType]; dup {
			return contractErrorf("duplicate upstream receipt declaration for business type %q", receipt.BusinessType)
		}
		seen[receipt.BusinessType] = struct{}{}
	}
	if strings.TrimSpace(r.TaskID) != "" {
		if _, err := uuid.Parse(strings.TrimSpace(r.TaskID)); err != nil {
			return contractErrorf("task id %q is not a UUID", r.TaskID)
		}
	}
	if strings.TrimSpace(r.CreatedBy) == "" || len(r.CreatedBy) > 128 {
		return contractErrorf("task creation requires an authenticated creator of 1..128 characters")
	}
	if strings.ContainsRune(r.Reason, 0) || len(r.Reason) > 1024 {
		return contractErrorf("task creation reason is malformed")
	}
	return nil
}

// TaskCreateResult reports the created task.
type TaskCreateResult struct {
	TaskID    string
	State     TaskState
	CreatedAt time.Time
}

// insertTaskSQL creates the `created` task row. The upstream receipt
// declaration is stored in the data-model.md §1.1 shape
// {business_type: {source, connected}}.
const insertTaskSQL = `
INSERT INTO recon_task (
    task_id, scope_chain_id, scope_kind, scope_start, scope_end,
    scope_start_at, scope_end_at, business_types, upstream_receipt_source,
    policy_refs, state, budget, created_by)
VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8::text[], $9::jsonb,
        $10::jsonb, 'created', $11::jsonb, $12)
RETURNING created_at`

// CreateTask records one task in state `created` and appends its audit row in
// the same short transaction (contracts/task-lifecycle.md: start(scope, budget)
// -> created; illegal shapes are refused before any write). The task does not
// claim or scan anything until `resume` activates it.
func (s *Store) CreateTask(ctx context.Context, req TaskCreateRequest) (TaskCreateResult, error) {
	if s == nil || s.db == nil {
		return TaskCreateResult{}, contractErrorf("store has no database")
	}
	if err := req.Validate(); err != nil {
		return TaskCreateResult{}, err
	}

	types, err := canonicalBusinessTypes(req.BusinessTypes)
	if err != nil {
		return TaskCreateResult{}, err
	}
	taskID := strings.TrimSpace(req.TaskID)
	if taskID == "" {
		taskID = uuid.NewString()
	}
	receipts, err := upstreamReceiptsJSON(req.UpstreamReceipts)
	if err != nil {
		return TaskCreateResult{}, err
	}
	policyRefs, err := taskJSONObject(req.PolicyRefs, "policy_refs")
	if err != nil {
		return TaskCreateResult{}, err
	}
	budget, err := taskJSONObject(req.Budget, "budget")
	if err != nil {
		return TaskCreateResult{}, err
	}

	startHeight, startAt := rangePairArgs(req.Start)
	endHeight, endAt := rangePairArgs(req.End)

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return TaskCreateResult{}, fmt.Errorf("begin task create: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	var createdAt time.Time
	if err := tx.QueryRow(ctx, insertTaskSQL,
		taskID, req.ChainID, req.Kind, startHeight, endHeight, startAt, endAt,
		businessTypeTexts(types), receipts, policyRefs, budget, req.CreatedBy).
		Scan(&createdAt); err != nil {
		return TaskCreateResult{}, fmt.Errorf("insert task: %w", err)
	}

	if err := insertAuditTx(ctx, tx, AuditRecord{
		Actor:  req.CreatedBy,
		Action: AuditActionStart,
		Target: map[string]any{
			"task_id":        taskID,
			"scope_chain":    req.ChainID,
			"scope_kind":     string(req.Kind),
			"range_start":    rangeAuditValue(req.Start),
			"range_end":      rangeAuditValue(req.End),
			"business_types": businessTypeTexts(types),
		},
		Reason:   req.Reason,
		Evidence: fmt.Sprintf("upstream_receipts=%s", receipts),
		Result:   "created",
	}); err != nil {
		return TaskCreateResult{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return TaskCreateResult{}, fmt.Errorf("commit task create: %w", err)
	}
	return TaskCreateResult{TaskID: taskID, State: TaskStateCreated, CreatedAt: createdAt}, nil
}

// upstreamReceiptsJSON renders the declaration map in the stored
// {business_type: {source, connected}} shape.
func upstreamReceiptsJSON(receipts []ChainUpstreamReceiptSource) ([]byte, error) {
	encoded := make(map[string]struct {
		Source    string `json:"source"`
		Connected bool   `json:"connected"`
	}, len(receipts))
	for _, receipt := range receipts {
		encoded[string(receipt.BusinessType)] = struct {
			Source    string `json:"source"`
			Connected bool   `json:"connected"`
		}{Source: receipt.Source, Connected: receipt.Connected}
	}
	raw, err := json.Marshal(encoded)
	if err != nil {
		return nil, fmt.Errorf("marshal upstream receipt source: %w", err)
	}
	return raw, nil
}

// taskJSONObject normalizes an optional raw JSONB snapshot: empty records
// `{}`; a non-object or malformed value is refused (the schema requires a JSON
// object).
func taskJSONObject(raw []byte, name string) ([]byte, error) {
	if len(strings.TrimSpace(string(raw))) == 0 {
		return []byte(`{}`), nil
	}
	var probe map[string]any
	if err := json.Unmarshal(raw, &probe); err != nil {
		return nil, contractErrorf("task %s is not a JSON object: %v", name, err)
	}
	return raw, nil
}

// businessTypeTexts renders canonical business types for the TEXT[] column.
func businessTypeTexts(types []BusinessType) []string {
	out := make([]string, len(types))
	for i, t := range types {
		out[i] = string(t)
	}
	return out
}

// TaskCoverage is the read-only coverage view of one task: the row, the
// checkpoint pointer and the open (uncovered) gap count. It lets an operator
// see, without any write, whether the scope is fully persisted; an incomplete
// or paused task can never be rendered as fully consistent (Q3-5).
type TaskCoverage struct {
	Task     *Task
	Head     *Checkpoint
	OpenGaps int64
	// UncoveredFrom is the first position after the persisted prefix, or nil
	// when the persisted prefix reaches the scope end.
	UncoveredFrom *RangeBound
}

// TaskCoverageByID loads one task with its checkpoint pointer and open gap
// count. It is strictly read-only (no lock, no audit row).
func (s *Store) TaskCoverageByID(ctx context.Context, taskID string) (TaskCoverage, error) {
	if s == nil || s.db == nil {
		return TaskCoverage{}, contractErrorf("store has no database")
	}
	if strings.TrimSpace(taskID) == "" {
		return TaskCoverage{}, contractErrorf("coverage requires a task_id")
	}
	task, err := s.TaskByID(ctx, taskID)
	if err != nil {
		return TaskCoverage{}, err
	}
	head, err := s.CheckpointHead(ctx, taskID)
	if err != nil {
		return TaskCoverage{}, err
	}
	openGaps, err := s.OpenGapCount(ctx, taskID)
	if err != nil {
		return TaskCoverage{}, err
	}
	coverage := TaskCoverage{Task: task, Head: head, OpenGaps: openGaps}
	if head == nil {
		from := task.ScopeStart
		coverage.UncoveredFrom = &from
		return coverage, nil
	}
	cmp, err := head.ResultPersistedThrough.Compare(task.ScopeEnd)
	if err != nil {
		return TaskCoverage{}, err
	}
	if cmp < 0 {
		from, err := head.ResultPersistedThrough.Successor()
		if err != nil {
			return TaskCoverage{}, err
		}
		coverage.UncoveredFrom = &from
	}
	return coverage, nil
}

// ErrReconTaskIDRequired is returned when a task-scoped operation is invoked
// without a task id.
var ErrReconTaskIDRequired = errors.New("reconciliation: task id is required")

// RequireTaskID validates a CLI-supplied task id and returns its canonical
// form. It performs no I/O.
func RequireTaskID(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", ErrReconTaskIDRequired
	}
	parsed, err := uuid.Parse(trimmed)
	if err != nil {
		return "", contractErrorf("task id %q is not a UUID", raw)
	}
	return parsed.String(), nil
}
