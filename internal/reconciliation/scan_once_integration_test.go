//go:build integration

// scan_once_integration_test.go is the T016 integration layer for the ScanOnce
// compare loop over the real 000016 schema (quickstart §1–§3/§8; data-model.md
// §5/§5.1):
//
//   - one budgeted invocation claims contiguous intervals, commits
//     results + checkpoint in the same transaction, and stops at the scope
//     end without auto-closing the task;
//   - incomplete evidence (no adapters wired) yields pending classifications
//     plus visible gaps, never a ticket and never a consistent conclusion;
//   - budget exhaustion suspends observably (suspended_budget + gap row,
//     pointer untouched, no claim taken);
//   - a divergence detected by two scans with the same stable identity keeps
//     ONE ticket and appends occurrence evidence (SC-002): no duplicate
//     ticket, no loss of evidence.
//
// PostgreSQL comes from testcontainers. When no Docker provider is healthy the
// package reports NOT RUN (t.Skip), never a pass.
package reconciliation

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
)

// staticScanCandidates is a per-interval candidate source driven by a
// function; each interval receives its own business key.
type staticScanCandidates struct {
	candidates func(interval ScanInterval) []ScanCandidate
}

func (s staticScanCandidates) ScanCandidates(ctx context.Context, interval ScanInterval) ([]ScanCandidate, error) {
	if s.candidates == nil {
		return nil, nil
	}
	return s.candidates(interval), nil
}

// fakeScanChainFacts returns a complete chain bundle for every observed
// window, with a fixed fact time so identities stay stable across scans.
type fakeScanChainFacts struct {
	indexedAt time.Time
}

func (f fakeScanChainFacts) Observe(ctx context.Context, q ChainFactsQuery) (ChainFactsBundle, error) {
	bundle := ChainFactsBundle{
		ChainID:    q.ChainID,
		From:       q.From,
		To:         q.To,
		Source:     ChainSourceLocalIndex,
		CapturedAt: f.indexedAt,
		Status:     ChainFactsComplete,
	}
	for height := q.From; height <= q.To; height++ {
		bundle.Blocks = append(bundle.Blocks, ChainFactBlock{
			Number:    height,
			Hash:      "0xblock",
			Canonical: true,
			IndexedAt: f.indexedAt,
		})
	}
	return bundle, nil
}

// fakeScanPGState answers every read with the authoritative absence of the
// business row.
type fakeScanPGState struct {
	readAt time.Time
}

func (f fakeScanPGState) Read(ctx context.Context, req PGReadRequest) (PGStateRecord, error) {
	return PGStateRecord{
		BusinessType: req.BusinessType,
		BusinessKey:  req.Key,
		ChainID:      req.ChainID,
		Status:       PGStateAbsent,
		ReadAt:       f.readAt,
	}, nil
}

// fakeScanEventState returns a complete, empty event-delivery bundle.
type fakeScanEventState struct {
	capturedAt time.Time
}

func (f fakeScanEventState) Observe(ctx context.Context, q EventStateQuery) (EventStateEvidence, error) {
	return EventStateEvidence{
		Scope:      q.Scope,
		Interval:   q.Interval,
		CapturedAt: f.capturedAt,
		Status:     EventDeliveryComplete,
	}, nil
}

func TestIntegrationScanOnceBudgetedCommitAndSuspend(t *testing.T) {
	ctx, pool, store := reconIT(t)

	t.Run("budgeted_invocation_commits_contiguous_intervals_and_stops_at_scope_end", func(t *testing.T) {
		taskID := uuid.NewString()
		reconSeedHeightTask(t, ctx, pool, taskID, 600, 607, "running", "")

		result, err := store.ScanOnce(ctx, ScanOnceRequest{
			TaskID: taskID, Owner: reconITOwner, LeaseTTL: time.Minute,
			Limits: BudgetLimits{
				MaxConcurrency: 1, MaxSpanPerClaim: 4, MaxDuration: time.Minute,
				MaxPGRequests: 50, MaxRPCRequests: 1,
			},
			// No adapters: every party is unknown, so the interval results
			// must be pending with visible gaps, never tickets or consistency.
			Candidates: staticScanCandidates{candidates: func(interval ScanInterval) []ScanCandidate {
				return []ScanCandidate{{
					BusinessType: BusinessWithdrawal,
					BusinessKey:  BusinessKey{Kind: BusinessKeyRequestID, Value: fmt.Sprintf("pending-%d", interval.From.Height)},
					ChainFact:    ChainFactRef{BlockNumber: interval.From.Height},
				}}
			}},
		})
		if err != nil {
			t.Fatalf("ScanOnce: %v", err)
		}
		if result.Stop != ScanStopScopeExhausted || result.Suspended {
			t.Fatalf("result stop = %q suspended = %v, want scope_exhausted/not suspended", result.Stop, result.Suspended)
		}
		if result.Committed != 2 || result.Attempts != 2 {
			t.Fatalf("committed/attempts = %d/%d, want 2/2", result.Committed, result.Attempts)
		}
		if result.Pending != 2 || result.Tickets != 0 || result.Absorbed != 0 {
			t.Fatalf("pending/tickets/absorbed = %d/%d/%d, want 2/0/0 (missing evidence is never a ticket)",
				result.Pending, result.Tickets, result.Absorbed)
		}
		if result.Gaps != 2 {
			t.Fatalf("gaps = %d, want 2 visible incomplete markers", result.Gaps)
		}
		if result.LastCheckpoint == nil || result.LastCheckpoint.ResultPersistedThrough.Height != 607 {
			t.Fatalf("last checkpoint = %+v, want persisted through 607", result.LastCheckpoint)
		}
		if got, ok := reconPersistedThrough(t, ctx, pool, taskID); !ok || got != 607 {
			t.Fatalf("pointer = %d (present %v), want 607", got, ok)
		}
		if n := reconCheckpointCount(t, ctx, pool, taskID); n != 2 {
			t.Errorf("checkpoint rows = %d, want 2", n)
		}
		gaps := reconGapReasons(t, ctx, pool, taskID)
		if gaps[string(GapQueryFailed)] != 2 {
			t.Errorf("gaps = %v, want query_failed=2", gaps)
		}
		var discrepancies int64
		if err := pool.QueryRow(ctx, `SELECT count(*)::bigint FROM discrepancy`).Scan(&discrepancies); err != nil {
			t.Fatalf("count discrepancies: %v", err)
		}
		if discrepancies != 0 {
			t.Errorf("discrepancies = %d, want 0 from pending evidence", discrepancies)
		}
		// Not auto-closed: the operator closes; the task is still running.
		if state, _ := reconTaskState(t, ctx, store, taskID); state != TaskStateRunning {
			t.Errorf("task state = %s, want running (done is an operator action)", state)
		}
	})

	t.Run("budget_exhaustion_suspends_observably_without_a_claim", func(t *testing.T) {
		taskID := uuid.NewString()
		reconSeedHeightTask(t, ctx, pool, taskID, 620, 627, "running", "")

		result, err := store.ScanOnce(ctx, ScanOnceRequest{
			TaskID: taskID, Owner: reconITOwner, LeaseTTL: time.Minute,
			Limits: BudgetLimits{
				MaxConcurrency: 1, MaxSpanPerClaim: 4, MaxDuration: time.Minute,
				MaxPGRequests: 1, MaxRPCRequests: 1,
			},
			Candidates: staticScanCandidates{candidates: func(interval ScanInterval) []ScanCandidate {
				return []ScanCandidate{{
					BusinessType: BusinessWithdrawal,
					BusinessKey:  BusinessKey{Kind: BusinessKeyRequestID, Value: fmt.Sprintf("never-%d", interval.From.Height)},
				}}
			}},
		})
		if err != nil {
			t.Fatalf("ScanOnce: %v", err)
		}
		if !result.Suspended || result.SuspendResource != ResourcePG || result.SuspendDetail == "" {
			t.Fatalf("result = %+v, want an observable PG suspension", result)
		}
		if result.Committed != 0 || result.Attempts != 0 {
			t.Fatalf("committed/attempts = %d/%d, want 0/0 (quota refused before the claim)", result.Committed, result.Attempts)
		}
		state, reason := reconTaskState(t, ctx, store, taskID)
		if state != TaskStateSuspendedBudget || reason == "" {
			t.Fatalf("task = %s (reason %q), want suspended_budget with a recorded reason", state, reason)
		}
		gaps := reconGapReasons(t, ctx, pool, taskID)
		if gaps[string(GapBudgetExhausted)] != 1 {
			t.Fatalf("gaps = %v, want budget_exhausted=1 over the uncovered remainder", gaps)
		}
		if _, ok := reconPersistedThrough(t, ctx, pool, taskID); ok {
			t.Errorf("pointer advanced while suspended")
		}
	})
}

func TestIntegrationScanOnceTicketDedupAcrossScans(t *testing.T) {
	ctx, pool, store := reconIT(t)

	fixedAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	sources := ScanSources{
		Chain:  fakeScanChainFacts{indexedAt: fixedAt},
		PG:     fakeScanPGState{readAt: fixedAt},
		Events: fakeScanEventState{capturedAt: fixedAt},
	}
	requestFor := func(taskID string) ScanOnceRequest {
		return ScanOnceRequest{
			TaskID: taskID, Owner: reconITOwner, LeaseTTL: time.Minute,
			Limits: BudgetLimits{
				MaxConcurrency: 1, MaxSpanPerClaim: 4, MaxDuration: time.Minute,
				MaxPGRequests: 50, MaxRPCRequests: 4,
			},
			FreshnessTolerance: time.Hour,
			Sources:            sources,
			Candidates: staticScanCandidates{candidates: func(interval ScanInterval) []ScanCandidate {
				return []ScanCandidate{{
					BusinessType: BusinessWithdrawal,
					BusinessKey:  BusinessKey{Kind: BusinessKeyRequestID, Value: "req-dedup"},
					// Confirmed chain fact with no PG record: a missing
					// business record (US1-1) that must mint a ticket.
					ChainFact: ChainFactRef{BlockNumber: interval.From.Height, BlockHash: "0xblock"},
				}}
			}},
		}
	}

	firstTask := uuid.NewString()
	secondTask := uuid.NewString()
	reconSeedHeightTask(t, ctx, pool, firstTask, 700, 703, "running", "")
	reconSeedHeightTask(t, ctx, pool, secondTask, 700, 703, "running", "")
	// The fake adapters require a numeric on-chain id; the shared seed helper
	// uses a symbolic chain name.
	for _, taskID := range []string{firstTask, secondTask} {
		if _, err := pool.Exec(ctx,
			`UPDATE recon_task SET scope_chain_id = '7' WHERE task_id = $1::uuid`, taskID); err != nil {
			t.Fatalf("set numeric chain id of %s: %v", taskID, err)
		}
	}

	first, err := store.ScanOnce(ctx, requestFor(firstTask))
	if err != nil {
		t.Fatalf("first ScanOnce: %v", err)
	}
	if first.Tickets != 1 || first.Merged != 0 || first.Pending != 0 {
		t.Fatalf("first scan tickets/merged/pending = %d/%d/%d, want 1/0/0", first.Tickets, first.Merged, first.Pending)
	}

	// The same stable identity detected by a second scan reuses the original
	// ticket and appends occurrence evidence (SC-002).
	second, err := store.ScanOnce(ctx, requestFor(secondTask))
	if err != nil {
		t.Fatalf("second ScanOnce: %v", err)
	}
	if second.Tickets != 0 || second.Merged != 1 || second.Occurrences != 1 {
		t.Fatalf("second scan tickets/merged/occurrences = %d/%d/%d, want 0/1/1",
			second.Tickets, second.Merged, second.Occurrences)
	}

	var tickets, occurrences int64
	if err := pool.QueryRow(ctx, `
		SELECT count(*)::bigint FROM discrepancy WHERE business_key = 'request_id=req-dedup'`).Scan(&tickets); err != nil {
		t.Fatalf("count dedup tickets: %v", err)
	}
	if tickets != 1 {
		t.Fatalf("dedup tickets = %d, want exactly 1 for the same identity", tickets)
	}
	if err := pool.QueryRow(ctx, `
		SELECT count(*)::bigint FROM discrepancy_occurrence o
		JOIN discrepancy d ON d.discrepancy_id = o.discrepancy_id
		WHERE d.business_key = 'request_id=req-dedup'`).Scan(&occurrences); err != nil {
		t.Fatalf("count dedup occurrences: %v", err)
	}
	if occurrences != 2 {
		t.Fatalf("dedup occurrences = %d, want 2 (evidence appended, no new ticket)", occurrences)
	}
	var category, state string
	if err := pool.QueryRow(ctx, `
		SELECT category, state FROM discrepancy WHERE business_key = 'request_id=req-dedup'`).Scan(&category, &state); err != nil {
		t.Fatalf("read dedup ticket: %v", err)
	}
	if category != string(CategoryMissing) || state != "open_claimable" {
		t.Fatalf("ticket = %s/%s, want missing/open_claimable", category, state)
	}
	for _, taskID := range []string{firstTask, secondTask} {
		if got, ok := reconPersistedThrough(t, ctx, pool, taskID); !ok || got != 703 {
			t.Fatalf("task %s pointer = %d (present %v), want 703", taskID, got, ok)
		}
	}
}

func TestIntegrationScanOnceTaskStateGuards(t *testing.T) {
	ctx, pool, store := reconIT(t)
	taskID := uuid.NewString()
	reconSeedHeightTask(t, ctx, pool, taskID, 640, 643, "paused", "")

	_, err := store.ScanOnce(ctx, ScanOnceRequest{
		TaskID: taskID, Owner: reconITOwner, LeaseTTL: time.Minute,
		Limits: BudgetLimits{
			MaxConcurrency: 1, MaxSpanPerClaim: 4, MaxDuration: time.Minute,
			MaxPGRequests: 10, MaxRPCRequests: 1,
		},
		Candidates: staticScanCandidates{},
	})
	if !errors.Is(err, ErrTaskNotRunning) {
		t.Fatalf("ScanOnce on a paused task err = %v, want ErrTaskNotRunning", err)
	}
	if _, ok := reconPersistedThrough(t, ctx, pool, taskID); ok {
		t.Errorf("paused task pointer moved")
	}
}
