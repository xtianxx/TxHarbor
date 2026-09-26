//go:build contract

// scan_contract_test.go is the T011 contract layer for the 014 task lifecycle
// (contracts/task-lifecycle.md; data-model.md §1.1–1.3/§5/§5.1). It pins:
//
//   - the closed task-state vocabulary and the exact transition relation
//     `created -> running <-> paused | suspended_budget -> running ->
//     done | cancelled`: every other edge (including skipping `running`, or
//     leaving done/cancelled) is refused by the relation;
//   - the audit vocabulary each legal edge records (start/pause/resume/close);
//     the persisted refusal + audit row half needs PostgreSQL and lives in
//     scan_integration_test.go (T012);
//   - checkpoint invariants and the pointer rule (`done` needs the pointer at
//     the scope end; a claim never starts inside the unpersisted prefix);
//   - the closed gap vocabulary that keeps paused/budget/interrupted/
//     upstream-unconnected ranges visible;
//   - bounded-budget observability (Integrity Rules: bounded quotas, exhausted
//     budgets stop new work, waiting is a select, never a busy loop).
//
// `make test-contract` runs this layer with no database and no Docker.
package reconciliation

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"
)

// contractTaskLegalEdges is the frozen task lifecycle edge set. Adding or
// removing an edge is a task-lifecycle contract change (contracts/
// task-lifecycle.md States) and must be deliberate.
func contractTaskLegalEdges() [][2]TaskState {
	return [][2]TaskState{
		{TaskStateCreated, TaskStateRunning},
		{TaskStateRunning, TaskStatePaused},
		{TaskStateRunning, TaskStateSuspendedBudget},
		{TaskStateRunning, TaskStateDone},
		{TaskStateRunning, TaskStateCancelled},
		{TaskStatePaused, TaskStateRunning},
		{TaskStateSuspendedBudget, TaskStateRunning},
	}
}

func TestContractTaskLifecycleMatrixIsFrozen(t *testing.T) {
	states := []TaskState{
		TaskStateCreated, TaskStateRunning, TaskStatePaused,
		TaskStateSuspendedBudget, TaskStateDone, TaskStateCancelled,
	}
	legal := make(map[[2]TaskState]bool)
	for _, edge := range contractTaskLegalEdges() {
		if legal[edge] {
			t.Fatalf("duplicate legal edge %s -> %s", edge[0], edge[1])
		}
		legal[edge] = true
	}
	if len(legal) != 7 {
		t.Fatalf("task lifecycle has %d legal edges, want the frozen 7", len(legal))
	}

	for _, from := range states {
		for _, to := range states {
			want := legal[[2]TaskState{from, to}]
			if got := CanTransitionTask(from, to); got != want {
				t.Errorf("CanTransitionTask(%s, %s) = %v, want %v", from, to, got, want)
			}
		}
	}

	// No shortcut into done: only a running task can close, so a paused or
	// budget-suspended task must resume first (the zero-gap/pointer guards are
	// evaluated on that edge; DB half in scan_integration_test.go).
	for _, from := range states {
		if from != TaskStateRunning && CanTransitionTask(from, TaskStateDone) {
			t.Errorf("CanTransitionTask(%s, done) = true, want false (done is only reachable from running)", from)
		}
	}
	// done/cancelled are absorbing: a terminal task never transitions again.
	for _, from := range []TaskState{TaskStateDone, TaskStateCancelled} {
		for _, to := range states {
			if CanTransitionTask(from, to) {
				t.Errorf("CanTransitionTask(%s, %s) = true, want false (terminal state)", from, to)
			}
		}
	}
	// paused/suspended_budget return to running before any terminal edge.
	for _, from := range []TaskState{TaskStatePaused, TaskStateSuspendedBudget} {
		for _, to := range []TaskState{TaskStateDone, TaskStateCancelled} {
			if CanTransitionTask(from, to) {
				t.Errorf("CanTransitionTask(%s, %s) = true, want false", from, to)
			}
		}
	}
}

func TestContractTaskStateVocabularyIsClosed(t *testing.T) {
	for _, s := range []TaskState{
		TaskStateCreated, TaskStateRunning, TaskStatePaused,
		TaskStateSuspendedBudget, TaskStateDone, TaskStateCancelled,
	} {
		if !s.Valid() {
			t.Errorf("TaskState(%q).Valid() = false, want true", s)
		}
	}
	for _, raw := range []string{"", "Created", "RUNNING", "running ", "terminated", "suspendedBudget"} {
		if TaskState(raw).Valid() {
			t.Errorf("TaskState(%q).Valid() = true, want false (closed vocabulary)", raw)
		}
	}
	for _, s := range []TaskState{
		TaskStateCreated, TaskStateRunning, TaskStatePaused, TaskStateSuspendedBudget,
	} {
		if s.Terminal() {
			t.Errorf("TaskState(%q).Terminal() = true, want false", s)
		}
	}
	for _, s := range []TaskState{TaskStateDone, TaskStateCancelled} {
		if !s.Terminal() {
			t.Errorf("TaskState(%q).Terminal() = false, want true", s)
		}
	}
}

func TestContractTaskTransitionAuditVocabulary(t *testing.T) {
	want := map[[2]TaskState]struct {
		action AuditAction
		result string
	}{
		{TaskStateCreated, TaskStateRunning}:         {AuditActionStart, "started"},
		{TaskStateRunning, TaskStatePaused}:          {AuditActionPause, "paused"},
		{TaskStateRunning, TaskStateSuspendedBudget}: {AuditActionPause, "suspended_budget"},
		{TaskStateRunning, TaskStateDone}:            {AuditActionClose, "done"},
		{TaskStateRunning, TaskStateCancelled}:       {AuditActionClose, "cancelled"},
		{TaskStatePaused, TaskStateRunning}:          {AuditActionResume, "resumed"},
		{TaskStateSuspendedBudget, TaskStateRunning}: {AuditActionResume, "resumed"},
	}
	for _, edge := range contractTaskLegalEdges() {
		expected, ok := want[edge]
		if !ok {
			t.Fatalf("legal edge %s -> %s has no pinned audit mapping", edge[0], edge[1])
		}
		action, result := auditActionForTaskTransition(edge[0], edge[1])
		if action != expected.action || result != expected.result {
			t.Errorf("audit mapping %s -> %s = (%s, %q), want (%s, %q)",
				edge[0], edge[1], action, result, expected.action, expected.result)
		}
		if !action.Valid() {
			t.Errorf("audit mapping %s -> %s returns unknown action %q", edge[0], edge[1], action)
		}
		if action == AuditActionRefuse {
			t.Errorf("audit mapping %s -> %s maps a legal edge to refuse", edge[0], edge[1])
		}
	}
	if len(want) != len(contractTaskLegalEdges()) {
		t.Fatalf("audit mapping covers %d edges, want %d", len(want), len(contractTaskLegalEdges()))
	}
}

func TestContractCheckpointInvariantsAndPointerRule(t *testing.T) {
	valid := Checkpoint{
		TaskID:                 "task-contract",
		Seq:                    1,
		CoveredThrough:         HeightBound(110),
		ResultPersistedThrough: HeightBound(103),
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid checkpoint refused: %v", err)
	}
	if err := (Checkpoint{TaskID: "task-contract", Seq: 1,
		CoveredThrough: HeightBound(103), ResultPersistedThrough: HeightBound(103),
	}).Validate(); err != nil {
		t.Fatalf("persisted == covered must be valid: %v", err)
	}

	base := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	refusals := []struct {
		name string
		cp   Checkpoint
	}{
		{"persisted past covered", Checkpoint{TaskID: "t", Seq: 1,
			CoveredThrough: HeightBound(103), ResultPersistedThrough: HeightBound(104)}},
		{"range kind mismatch", Checkpoint{TaskID: "t", Seq: 1,
			CoveredThrough: HeightBound(103), ResultPersistedThrough: TimeBound(base)}},
		{"non-positive seq", Checkpoint{TaskID: "t", Seq: 0,
			CoveredThrough: HeightBound(103), ResultPersistedThrough: HeightBound(103)}},
		{"missing task id", Checkpoint{Seq: 1,
			CoveredThrough: HeightBound(103), ResultPersistedThrough: HeightBound(103)}},
		{"zero time bound", Checkpoint{TaskID: "t", Seq: 1,
			CoveredThrough: TimeBound(time.Time{}), ResultPersistedThrough: TimeBound(time.Time{})}},
	}
	for _, tc := range refusals {
		err := tc.cp.Validate()
		if err == nil {
			t.Errorf("%s: Validate() = nil, want refusal", tc.name)
			continue
		}
		if !errors.Is(err, ErrContract) {
			t.Errorf("%s: err = %v, want ErrContract", tc.name, err)
		}
	}

	// The pointer plans the next claim strictly after the persisted prefix,
	// never inside the not-yet-persisted covered range (103 persisted of
	// 110 covered -> the next claim starts at 104).
	head := Checkpoint{TaskID: "t", Seq: 2,
		CoveredThrough: HeightBound(110), ResultPersistedThrough: HeightBound(103)}
	start, err := NextClaimStart(&head, HeightBound(100))
	if err != nil {
		t.Fatalf("NextClaimStart(head) = %v", err)
	}
	if start.Height != 104 {
		t.Fatalf("NextClaimStart(head) = %d, want 104 (successor of the persisted prefix)", start.Height)
	}
	// No checkpoint yet: the scope start is claimable.
	start, err = NextClaimStart(nil, HeightBound(100))
	if err != nil || start.Height != 100 {
		t.Fatalf("NextClaimStart(nil, 100) = %v (err %v), want 100", start, err)
	}
	// An invalid scope start is a contract violation, never a silent guess.
	if _, err := NextClaimStart(nil, TimeBound(time.Time{})); !errors.Is(err, ErrContract) {
		t.Fatalf("NextClaimStart(nil, zero time) err = %v, want ErrContract", err)
	}
	// A checkpoint whose kind does not match the scope is refused.
	timeHead := Checkpoint{TaskID: "t", Seq: 1,
		CoveredThrough: TimeBound(base), ResultPersistedThrough: TimeBound(base)}
	if _, err := NextClaimStart(&timeHead, HeightBound(100)); !errors.Is(err, ErrContract) {
		t.Fatalf("NextClaimStart(kind mismatch) err = %v, want ErrContract", err)
	}
	// Once the persisted prefix reaches the scope end there is no claimable
	// interval left (the DB half is ErrScopeExhausted in the integration test).
	full := Checkpoint{TaskID: "t", Seq: 3,
		CoveredThrough: HeightBound(112), ResultPersistedThrough: HeightBound(112)}
	start, err = NextClaimStart(&full, HeightBound(100))
	if err != nil {
		t.Fatalf("NextClaimStart(full) = %v", err)
	}
	if cmp, err := start.Compare(HeightBound(112)); err != nil || cmp <= 0 {
		t.Fatalf("next claim start after full coverage = %v (cmp %d, err %v), want past scope end", start, cmp, err)
	}
}

func TestContractClaimIntervalPlanning(t *testing.T) {
	end, err := planClaimEnd(HeightBound(100), HeightBound(112), 4)
	if err != nil || end.Height != 103 {
		t.Fatalf("planClaimEnd(100, 112, 4) = %v (err %v), want 103", end, err)
	}
	end, err = planClaimEnd(HeightBound(110), HeightBound(112), 4)
	if err != nil || end.Height != 112 {
		t.Fatalf("planClaimEnd clamps to scope end: %v (err %v), want 112", end, err)
	}
	end, err = planClaimEnd(HeightBound(112), HeightBound(112), 1)
	if err != nil || end.Height != 112 {
		t.Fatalf("planClaimEnd(112, 112, 1) = %v (err %v), want 112", end, err)
	}
	end, err = planClaimEnd(HeightBound(math.MaxInt64-1), HeightBound(math.MaxInt64), 10)
	if err != nil || end.Height != math.MaxInt64 {
		t.Fatalf("planClaimEnd saturates at MaxInt64: %v (err %v)", end, err)
	}
	if _, err := planClaimEnd(HeightBound(100), HeightBound(112), 0); !errors.Is(err, ErrContract) {
		t.Fatalf("planClaimEnd(span 0) err = %v, want ErrContract", err)
	}
	if _, err := planClaimEnd(HeightBound(100), HeightBound(112), -1); !errors.Is(err, ErrContract) {
		t.Fatalf("planClaimEnd(span -1) err = %v, want ErrContract", err)
	}

	base := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	scopeEnd := TimeBound(base.Add(3 * time.Microsecond))
	end, err = planClaimEnd(TimeBound(base), scopeEnd, 2)
	if err != nil || !end.Time.Equal(base.Add(time.Microsecond)) {
		t.Fatalf("planClaimEnd(time, span 2) = %v (err %v), want %v", end, err, base.Add(time.Microsecond))
	}
	end, err = planClaimEnd(TimeBound(base), scopeEnd, int64(maxClaimSpan))
	if err != nil || !end.Time.Equal(scopeEnd.Time) {
		t.Fatalf("planClaimEnd(time, max span) = %v (err %v), want the scope end %v", end, err, scopeEnd.Time)
	}
}

func TestContractRangeBoundOrderingAndSuccessors(t *testing.T) {
	cmp, err := HeightBound(103).Compare(HeightBound(104))
	if err != nil || cmp != -1 {
		t.Fatalf("Compare(103, 104) = %d (err %v), want -1", cmp, err)
	}
	if cmp, err := HeightBound(104).Compare(HeightBound(104)); err != nil || cmp != 0 {
		t.Fatalf("Compare(104, 104) = %d (err %v), want 0", cmp, err)
	}
	if _, err := HeightBound(1).Compare(TimeBound(time.Now())); !errors.Is(err, ErrContract) {
		t.Fatalf("Compare(kind mismatch) err = %v, want ErrContract", err)
	}
	if _, err := TimeBound(time.Time{}).Compare(TimeBound(time.Now())); !errors.Is(err, ErrContract) {
		t.Fatalf("Compare(invalid bound) err = %v, want ErrContract", err)
	}
	if next, err := HeightBound(103).Successor(); err != nil || next.Height != 104 {
		t.Fatalf("Successor(103) = %v (err %v), want 104", next, err)
	}
	if _, err := HeightBound(math.MaxInt64).Successor(); !errors.Is(err, ErrContract) {
		t.Fatalf("Successor(MaxInt64) err = %v, want ErrContract", err)
	}
	base := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	if next, err := TimeBound(base).Successor(); err != nil || !next.Time.Equal(base.Add(time.Microsecond)) {
		t.Fatalf("Successor(time) = %v (err %v), want +1µs", next, err)
	}
	if _, err := TimeBound(time.Time{}).Successor(); !errors.Is(err, ErrContract) {
		t.Fatalf("Successor(zero time) err = %v, want ErrContract", err)
	}
}

func TestContractGapVocabularyKeepsStopsVisible(t *testing.T) {
	all := []GapReason{
		GapNotStarted, GapInterrupted, GapBudgetExhausted, GapPaused,
		GapFreshnessHold, GapUpstreamUnconnected, GapQueryFailed,
	}
	for _, r := range all {
		if !r.Valid() {
			t.Errorf("GapReason(%q).Valid() = false, want true", r)
		}
	}
	for _, raw := range []string{"", "Paused", "paused ", "budget", "upstream-unconnected", "interrupted_reorg"} {
		if GapReason(raw).Valid() {
			t.Errorf("GapReason(%q).Valid() = true, want false (closed vocabulary)", raw)
		}
	}
	// The reasons TransitionTask must leave for pause/budget/cancel (no
	// premature "fully consistent") and the reason the classifier must leave
	// for incomplete upstream evidence are all part of the visible vocabulary.
	for _, r := range []GapReason{GapPaused, GapBudgetExhausted, GapNotStarted, GapUpstreamUnconnected} {
		if !r.Valid() {
			t.Errorf("required visible reason %q is not part of the vocabulary", r)
		}
	}
}

func TestContractBudgetBoundsSuspendObservably(t *testing.T) {
	ctx := context.Background()
	base := BudgetLimits{
		MaxConcurrency:  1,
		MaxSpanPerClaim: 10,
		MaxDuration:     time.Minute,
		MaxPGRequests:   2,
		MaxRPCRequests:  2,
	}
	if err := base.Validate(); err != nil {
		t.Fatalf("valid limits refused: %v", err)
	}
	mutations := []struct {
		name   string
		mutate func(*BudgetLimits)
	}{
		{"concurrency", func(l *BudgetLimits) { l.MaxConcurrency = 0 }},
		{"span per claim", func(l *BudgetLimits) { l.MaxSpanPerClaim = 0 }},
		{"duration", func(l *BudgetLimits) { l.MaxDuration = 0 }},
		{"pg requests", func(l *BudgetLimits) { l.MaxPGRequests = -1 }},
		{"rpc requests", func(l *BudgetLimits) { l.MaxRPCRequests = 0 }},
	}
	for _, m := range mutations {
		limits := base
		m.mutate(&limits)
		if err := limits.Validate(); !errors.Is(err, ErrInvalidBudget) {
			t.Errorf("%s: unbounded limits accepted: %v", m.name, err)
		}
	}
	if _, err := NewBudget(BudgetLimits{}); !errors.Is(err, ErrInvalidBudget) {
		t.Fatalf("NewBudget(zero limits) err = %v, want ErrInvalidBudget", err)
	}

	if got := base.ClampSpan(5); got != 5 {
		t.Fatalf("ClampSpan(5) = %d, want 5", got)
	}
	if got := base.ClampSpan(50); got != 10 {
		t.Fatalf("ClampSpan(50) = %d, want the per-claim bound 10", got)
	}
	if got := base.ClampSpan(0); got != 0 {
		t.Fatalf("ClampSpan(0) = %d, want 0 (no interval)", got)
	}

	b, err := NewBudget(base)
	if err != nil {
		t.Fatalf("NewBudget: %v", err)
	}
	release, err := b.Acquire(ctx)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	// While the only slot is held a second Acquire waits (bounded by ctx);
	// it must not spin or ignore the invoker's deadline.
	blocked, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if _, err := b.Acquire(blocked); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Acquire with all slots busy err = %v, want context.DeadlineExceeded", err)
	}
	release()
	releaseAgain, err := b.Acquire(ctx)
	if err != nil {
		t.Fatalf("Acquire after release: %v", err)
	}
	releaseAgain()

	if err := b.ConsumePG(ctx, 2); err != nil {
		t.Fatalf("PG charge within quota: %v", err)
	}
	if err := b.ConsumePG(ctx, 1); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("PG charge over quota err = %v, want ErrBudgetExhausted", err)
	}
	resource, detail, suspended := b.Suspended()
	if !suspended || resource != ResourcePG || detail == "" {
		t.Fatalf("Suspended() = (%q, %q, %v), want a visible PG exhaustion", resource, detail, suspended)
	}
	usage := b.Usage()
	if usage.Suspended != ResourcePG || usage.PGUsed != 2 || usage.SuspendedAt.IsZero() {
		t.Fatalf("Usage() = %+v, want suspended PG counter 2 with an observation time", usage)
	}
	// Once suspended, every charge and acquire is refused without charging.
	if err := b.ConsumeRPC(ctx, 1); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("charge after suspension err = %v, want ErrBudgetExhausted", err)
	}
	if _, err := b.Acquire(ctx); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("Acquire after suspension err = %v, want ErrBudgetExhausted", err)
	}
	waitCtx, waitCancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer waitCancel()
	if err := b.Wait(waitCtx, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Wait on a suspended budget err = %v, want the ctx deadline (one select, no poll)", err)
	}

	durationBudget, err := NewBudget(BudgetLimits{
		MaxConcurrency: 1, MaxSpanPerClaim: 10, MaxDuration: time.Nanosecond,
		MaxPGRequests: 2, MaxRPCRequests: 2,
	})
	if err != nil {
		t.Fatalf("NewBudget(nanosecond duration): %v", err)
	}
	time.Sleep(time.Millisecond)
	if err := durationBudget.ConsumePG(ctx, 1); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("charge past wall-clock deadline err = %v, want ErrBudgetExhausted", err)
	}
	if resource, _, _ := durationBudget.Suspended(); resource != ResourceDuration {
		t.Fatalf("Suspended() resource = %q, want %q", resource, ResourceDuration)
	}

	backoff := BoundedBackoff{
		Initial: 10 * time.Millisecond, Factor: 2,
		Max: 25 * time.Millisecond, MaxAttempts: 3,
	}
	if err := backoff.Validate(); err != nil {
		t.Fatalf("valid backoff refused: %v", err)
	}
	if _, ok := backoff.Delay(0); ok {
		t.Fatalf("Delay(0) permitted, want refusal")
	}
	if d, ok := backoff.Delay(1); !ok || d != 10*time.Millisecond {
		t.Fatalf("Delay(1) = %v (ok %v), want 10ms", d, ok)
	}
	if d, ok := backoff.Delay(3); !ok || d != 25*time.Millisecond {
		t.Fatalf("Delay(3) = %v (ok %v), want the clamped 25ms", d, ok)
	}
	if _, ok := backoff.Delay(4); ok {
		t.Fatalf("Delay(4) permitted past MaxAttempts, want refusal")
	}
	if err := (BoundedBackoff{}).Validate(); !errors.Is(err, ErrInvalidBudget) {
		t.Fatalf("zero backoff accepted: %v", err)
	}
}
