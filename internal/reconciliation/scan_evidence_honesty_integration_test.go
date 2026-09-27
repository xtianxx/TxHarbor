//go:build integration

// scan_evidence_honesty_integration_test.go is the T030 acceptance layer for
// the evidence-honesty pass over the real 000016 schema (quickstart §3; tasks
// T030). It pins the parts of T030 the current contract can decide:
//
//   - an eventless request (chain fact and PG business record both present, no
//     event row) still mints the existing fail-closed `missing` ticket; the
//     ticket is alert-only (no disposition/reverify side effect, no money-path
//     write) and the range is covered without a gap;
//   - a paused task records its uncovered remainder as a visible gap and can
//     never be rendered complete: the coverage view reports the uncovered
//     start, `done` is structurally unavailable while paused, and it stays
//     refused after resume while the gap is open. A suspended task has the
//     same shape (already pinned by the budget-suspension case in
//     scan_chainfirst_integration_test.go).
//
// T030 BLOCKER (recorded, not resolved): T030's third item asks that
// "pre-cutover requests without event rows stay pending/gap and never mint
// event-missing tickets", but the current contract cannot distinguish a
// pre-cutover request from a post-cutover request whose event row was really
// dropped (no cutover instant is plumbed into the scan, no expected-event
// registry exists, and recon_task.policy_refs is a version snapshot only —
// data-model.md §1.1), and the existing acceptance case
// `receipted_withdrawal_without_event_row_mints_missing`
// (internal/app/reconcileadmin) requires the fresh post-cutover shape to keep
// minting. The fail-closed path therefore stays (tasks.md:83 item ③) until an
// independent design item provides the discriminator; T030's third item is NOT
// completed in this round.
//
// PostgreSQL comes from testcontainers via the T012 helpers; without a Docker
// provider the package reports NOT RUN (t.Skip), never a pass.
package reconciliation

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

// scanHonestyRequestKey is the persisted business key of the eventless
// request candidate.
const scanHonestyRequestKey = "request_id=eventless-request"

// scanHonestyEventlessRequestCandidates enumerates the eventless shape: a
// withdrawal request whose chain execution receipt and PG row both exist and
// whose event aggregate identity is declared, but which has no event row.
func scanHonestyEventlessRequestCandidates() staticScanCandidates {
	return staticScanCandidates{candidates: func(interval ScanInterval) []ScanCandidate {
		return []ScanCandidate{{
			BusinessType: BusinessWithdrawal,
			BusinessKey:  BusinessKey{Kind: BusinessKeyRequestID, Value: "eventless-request"},
			EventKey: BusinessKey{
				Kind: EventBusinessKeyAggregate, Value: "withdrawal_request/eventless-request",
			},
			ChainFact: ChainFactRef{
				BlockNumber: interval.From.Height, BlockHash: chainFirstBlockHash, TxHash: chainFirstTxHash,
			},
			EvidenceRef: "scan:test:eventless-request",
		}}
	}}
}

// TestIntegrationEventOnlyAbsenceStillMintsFailClosedTicket pins the reverted
// T030 behavior required by the existing acceptance contract: with the chain
// fact and the PG row present and only the event row absent, the scan mints
// the fail-closed `missing` ticket. The pre-cutover variant cannot be told
// apart (see the blocker note in the file header).
func TestIntegrationEventOnlyAbsenceStillMintsFailClosedTicket(t *testing.T) {
	ctx, pool, store := reconIT(t)

	fixedAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	sources := ScanSources{
		Chain: chainFirstChainIndex{indexedAt: fixedAt, withLogs: true},
		PG: chainFirstPGState{
			present: map[string]bool{"eventless-request": true}, readAt: fixedAt,
		},
		// Complete event coverage with no row for the declared aggregate.
		Events: chainFirstEvents{capturedAt: fixedAt},
	}

	task := chainFirstTask(t, ctx, pool, 1400, 1403)
	result, err := store.ScanOnce(ctx, chainFirstRequest(task, sources, scanHonestyEventlessRequestCandidates()))
	if err != nil {
		t.Fatalf("ScanOnce: %v", err)
	}
	if result.Stop != ScanStopScopeExhausted || result.Committed != 1 {
		t.Fatalf("stop/committed = %q/%d, want scope_exhausted/1", result.Stop, result.Committed)
	}
	if result.Tickets != 1 || result.Merged != 0 {
		t.Fatalf("tickets/merged = %d/%d, want 1/0: the eventless request stays fail-closed missing",
			result.Tickets, result.Merged)
	}
	if result.Pending != 0 || result.Gaps != 0 {
		t.Fatalf("pending/gaps = %d/%d, want 0/0 for the complete-coverage missing ticket",
			result.Pending, result.Gaps)
	}

	var requestTickets int64
	if err := pool.QueryRow(ctx, `
		SELECT count(*)::bigint FROM discrepancy WHERE business_key = $1`,
		scanHonestyRequestKey).Scan(&requestTickets); err != nil {
		t.Fatalf("count request tickets: %v", err)
	}
	if requestTickets != 1 {
		t.Fatalf("request tickets = %d, want 1 (fail-closed missing)", requestTickets)
	}
	var category string
	if err := pool.QueryRow(ctx, `
		SELECT category FROM discrepancy WHERE business_key = $1`,
		scanHonestyRequestKey).Scan(&category); err != nil {
		t.Fatalf("read request ticket: %v", err)
	}
	if category != string(CategoryMissing) {
		t.Fatalf("request ticket category = %s, want missing", category)
	}

	// The ticket is alert-only: no auto-repair path runs off the scan and the
	// money-path tables are untouched (the full funds snapshot is asserted by
	// the chain-first acceptance test; here the 014 side effects are pinned).
	if n := chainFirstCount(t, ctx, pool, "disposition"); n != 0 {
		t.Fatalf("disposition rows = %d, want 0: the ticket is alert-only", n)
	}
	if n := chainFirstCount(t, ctx, pool, "reverify"); n != 0 {
		t.Fatalf("reverify rows = %d, want 0: no auto-repair path may run off a scan", n)
	}
	if got, ok := reconPersistedThrough(t, ctx, pool, task); !ok || got != 1403 {
		t.Fatalf("pointer = %d (present %v), want 1403", got, ok)
	}
	if open, err := store.OpenGapCount(ctx, task); err != nil || open != 0 {
		t.Fatalf("open gaps = %d (err %v), want 0 (complete coverage)", open, err)
	}
}

func TestIntegrationPausedTaskNeverRendersFullyConsistent(t *testing.T) {
	ctx, pool, store := reconIT(t)

	task := uuid.NewString()
	reconSeedHeightTask(t, ctx, pool, task, 1500, 1507, "running", "")

	// Pause before any scan: the whole scope is the uncovered remainder.
	transition, err := store.TransitionTask(ctx, TaskTransitionRequest{
		TaskID: task, To: TaskStatePaused, Reason: "operator pause", Actor: reconITOpsActor,
	})
	if err != nil || transition.From != TaskStateRunning || transition.To != TaskStatePaused {
		t.Fatalf("pause transition = %+v (err %v), want running -> paused", transition, err)
	}
	if transition.Checkpoint != nil {
		t.Fatalf("pause reported a checkpoint for a never-scanned task: %+v", transition.Checkpoint)
	}

	// The uncovered remainder is a visible paused gap, never silence.
	if open, err := store.OpenGapCount(ctx, task); err != nil || open != 1 {
		t.Fatalf("open gaps after pause = %d (err %v), want 1", open, err)
	}
	gaps := reconGapReasons(t, ctx, pool, task)
	if gaps[string(GapPaused)] != 1 {
		t.Fatalf("gaps = %v, want paused=1 for the uncovered remainder", gaps)
	}
	var gapStart, gapEnd int64
	if err := pool.QueryRow(ctx, `
		SELECT range_start, range_end FROM recon_gap
		WHERE task_id = $1::uuid AND reason = 'paused'`, task).Scan(&gapStart, &gapEnd); err != nil {
		t.Fatalf("read paused gap range: %v", err)
	}
	if gapStart != 1500 || gapEnd != 1507 {
		t.Fatalf("paused gap = [%d,%d], want the whole uncovered scope [1500,1507]", gapStart, gapEnd)
	}

	// The coverage view (the `show` rendering input) reports the paused task
	// as incomplete: state != running, no checkpoint, uncovered from the scope
	// start, one open gap.
	coverage, err := store.TaskCoverageByID(ctx, task)
	if err != nil {
		t.Fatalf("TaskCoverageByID: %v", err)
	}
	if coverage.Task.State != TaskStatePaused || coverage.Head != nil ||
		coverage.UncoveredFrom == nil || coverage.OpenGaps != 1 {
		t.Fatalf("paused coverage = state %s head %+v uncovered %+v gaps %d, want paused/nil/[1500,1507]/1",
			coverage.Task.State, coverage.Head, coverage.UncoveredFrom, coverage.OpenGaps)
	}
	if coverage.UncoveredFrom.Height != 1500 {
		t.Fatalf("uncovered start = %d, want 1500", coverage.UncoveredFrom.Height)
	}

	// paused -> done is structurally unavailable; after resume, `done` stays
	// refused while the uncovered range is an open gap (never "fully
	// consistent").
	if _, err := store.TransitionTask(ctx, TaskTransitionRequest{
		TaskID: task, To: TaskStateDone, Reason: "close while paused", Actor: reconITOpsActor,
	}); !errors.Is(err, ErrIllegalTaskTransition) {
		t.Fatalf("paused -> done err = %v, want ErrIllegalTaskTransition", err)
	}
	if _, err := store.TransitionTask(ctx, TaskTransitionRequest{
		TaskID: task, To: TaskStateRunning, Reason: "resume", Actor: reconITOpsActor,
	}); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if _, err := store.TransitionTask(ctx, TaskTransitionRequest{
		TaskID: task, To: TaskStateDone, Reason: "close over the uncovered remainder", Actor: reconITOpsActor,
	}); !errors.Is(err, ErrTaskNotComplete) {
		t.Fatalf("done over the open paused gap err = %v, want ErrTaskNotComplete", err)
	}
}
