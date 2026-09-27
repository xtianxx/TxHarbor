//go:build integration

// scan_evidence_honesty_integration_test.go is the T030/T040 acceptance layer
// for the evidence-honesty pass over the real 000016 schema plus the 000017
// expectation carrier (quickstart §3; tasks T030, T040):
//
//   - the T040 discriminator matrix on the decisive event-only absence (chain
//     fact and PG business record both present, event row absent): a durable
//     marker keeps the fail-closed `missing` ticket (R1, alert-only, deduped
//     across scans); no marker stays pending/gap (R3, never a ticket); an
//     audited legal retention prune explains the absence and stays pending; a
//     candidate with no catalog event aggregate is N/A and can never mint an
//     event-missing ticket (R2) while a chain/PG divergence still mints
//     independently;
//   - a paused task records its uncovered remainder as a visible gap and can
//     never be rendered complete: the coverage view reports the uncovered
//     start, `done` is structurally unavailable while paused, and it stays
//     refused after resume while the gap is open. A suspended task has the
//     same shape (already pinned by the budget-suspension case in
//     scan_chainfirst_integration_test.go).
//
// PostgreSQL comes from testcontainers via the T012 helpers; without a Docker
// provider the package reports NOT RUN (t.Skip), never a pass.
package reconciliation

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// scanHonestyEventlessRequestCandidates enumerates the eventless shape: a
// withdrawal request whose chain execution receipt and PG row both exist and
// whose event aggregate identity is declared, but which has no event row.
func scanHonestyEventlessRequestCandidates(requestID string) staticScanCandidates {
	return staticScanCandidates{candidates: func(interval ScanInterval) []ScanCandidate {
		return []ScanCandidate{{
			BusinessType: BusinessWithdrawal,
			BusinessKey:  BusinessKey{Kind: BusinessKeyRequestID, Value: requestID},
			EventKey: BusinessKey{
				Kind: EventBusinessKeyAggregate, Value: "withdrawal_request/" + requestID,
			},
			ChainFact: ChainFactRef{
				BlockNumber: interval.From.Height, BlockHash: chainFirstBlockHash, TxHash: chainFirstTxHash,
			},
			EvidenceRef: "scan:test:" + requestID,
		}}
	}}
}

// scanHonestyTxHashCandidates enumerates the chain-first tx_hash shape: the
// candidate carries no catalog event aggregate at all (R2 evidence).
func scanHonestyTxHashCandidates() staticScanCandidates {
	return staticScanCandidates{candidates: func(interval ScanInterval) []ScanCandidate {
		return []ScanCandidate{{
			BusinessType: BusinessWithdrawal,
			BusinessKey:  BusinessKey{Kind: BusinessKeyTxHash, Value: chainFirstTxHash},
			ChainFact: ChainFactRef{
				BlockNumber: interval.From.Height, BlockHash: chainFirstBlockHash, TxHash: chainFirstTxHash,
			},
			EvidenceRef: "scan:test:tx-hash",
		}}
	}}
}

// scanHonestyObligations builds the real bounded adapter over the migrated
// database.
func scanHonestyObligations(t *testing.T, pool *pgxpool.Pool) *EventObligationAdapter {
	t.Helper()
	adapter, err := NewEventObligationAdapter(pool, EventObligationConfig{
		MaxExpectations: 64, MaxRetentionAudits: 16,
	})
	if err != nil {
		t.Fatalf("NewEventObligationAdapter: %v", err)
	}
	return adapter
}

// scanHonestySeedObligation inserts one producer expectation marker (the row
// internal/events.Append writes inside the producer transaction).
func scanHonestySeedObligation(t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	aggregateType, aggregateID string, version int64, eventType string, obligatedAt time.Time) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO event_obligation (aggregate_type, aggregate_id, aggregate_version,
		    expected_event_type, obligated_at, source_kind, source_id)
		VALUES ($1, $2, $3, $4, $5, 'integration-test', $6)`,
		aggregateType, aggregateID, version, eventType, obligatedAt, aggregateID); err != nil {
		t.Fatalf("seed obligation marker: %v", err)
	}
}

// scanHonestySeedRetentionPrune inserts one audited events-admin retention
// prune (event_ops_audit.scope carries the window).
func scanHonestySeedRetentionPrune(t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	prunedAt time.Time, window string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO event_ops_audit (operation_id, op_kind, operator, scope, reason, result, created_at)
		VALUES ($1, 'retention_prune', 'it-operator', jsonb_build_object('retention', $2::text),
		        'integration test', 'deleted=1', $3)`,
		uuid.NewString(), window, prunedAt); err != nil {
		t.Fatalf("seed retention prune audit: %v", err)
	}
}

// scanHonestySources builds the complete-height eventless fixture with the
// real obligation adapter wired and the listed PG business keys present (every
// other key reads as a definitive PG absence).
func scanHonestySources(t *testing.T, pool *pgxpool.Pool, present ...string) ScanSources {
	t.Helper()
	fixedAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	presentSet := make(map[string]bool, len(present))
	for _, key := range present {
		presentSet[key] = true
	}
	return ScanSources{
		Chain: chainFirstChainIndex{indexedAt: fixedAt, withLogs: true},
		PG: chainFirstPGState{
			present: presentSet, readAt: fixedAt,
		},
		// Complete event coverage with no row for the declared aggregate.
		Events:      chainFirstEvents{capturedAt: fixedAt},
		Obligations: scanHonestyObligations(t, pool),
	}
}

// scanHonestyRequestPGKey renders the PG business key of one request.
func scanHonestyRequestPGKey(requestID string) string { return "request_id=" + requestID }

// scanHonestyTicketCount counts the persisted tickets of one business key.
func scanHonestyTicketCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, businessKey string) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(ctx, `
		SELECT count(*)::bigint FROM discrepancy WHERE business_key = $1`, businessKey).Scan(&n); err != nil {
		t.Fatalf("count tickets %s: %v", businessKey, err)
	}
	return n
}

// TestIntegrationEventObligationDiscriminatorMatrix pins the T040 R1/R2/R3
// matrix through the real scan loop, the real 000017 schema and the real
// bounded adapter (no pure-function-only coverage).
func TestIntegrationEventObligationDiscriminatorMatrix(t *testing.T) {
	ctx, pool, store := reconIT(t)
	fixedAt := time.Now().UTC().Add(-time.Minute)

	t.Run("R1 proven marker keeps the fail-closed missing ticket and dedups", func(t *testing.T) {
		const requestID = "eventless-request"
		// Cross-boundary rule: the marker's obligated_at (-1h) lies outside
		// the scanned interval's chain-time window; the expectation read is
		// aggregate-scoped, so an interval boundary never splits the
		// obligation from its candidate.
		scanHonestySeedObligation(t, ctx, pool, "withdrawal_request", requestID, 1,
			"withdrawal.request.received", fixedAt.Add(-time.Hour))
		sources := scanHonestySources(t, pool, requestID)

		task := chainFirstTask(t, ctx, pool, 1400, 1403)
		result, err := store.ScanOnce(ctx, chainFirstRequest(task, sources, scanHonestyEventlessRequestCandidates(requestID)))
		if err != nil {
			t.Fatalf("ScanOnce: %v", err)
		}
		if result.Stop != ScanStopScopeExhausted || result.Committed != 1 {
			t.Fatalf("stop/committed = %q/%d, want scope_exhausted/1", result.Stop, result.Committed)
		}
		if result.Tickets != 1 || result.Merged != 0 || result.Pending != 0 || result.Gaps != 0 {
			t.Fatalf("tickets/merged/pending/gaps = %d/%d/%d/%d, want 1/0/0/0 (R1 missing)",
				result.Tickets, result.Merged, result.Pending, result.Gaps)
		}
		var category string
		if err := pool.QueryRow(ctx, `
			SELECT category FROM discrepancy WHERE business_key = $1`,
			scanHonestyRequestPGKey(requestID)).Scan(&category); err != nil {
			t.Fatalf("read request ticket: %v", err)
		}
		if category != string(CategoryMissing) {
			t.Fatalf("request ticket category = %s, want missing", category)
		}
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

		// A repeat scan over the same evidence keeps ONE stable identity and
		// appends occurrence evidence: the discriminator does not perturb the
		// R1 ticket identity.
		secondTask := chainFirstTask(t, ctx, pool, 1400, 1403)
		second, err := store.ScanOnce(ctx, chainFirstRequest(secondTask, sources, scanHonestyEventlessRequestCandidates(requestID)))
		if err != nil {
			t.Fatalf("second ScanOnce: %v", err)
		}
		if second.Tickets != 0 || second.Merged != 1 || second.Occurrences != 1 || second.Pending != 0 {
			t.Fatalf("second scan tickets/merged/occurrences/pending = %d/%d/%d/%d, want 0/1/1/0",
				second.Tickets, second.Merged, second.Occurrences, second.Pending)
		}
		if n := chainFirstCount(t, ctx, pool, "discrepancy"); n != 1 {
			t.Fatalf("discrepancy rows = %d, want 1 (stable identity)", n)
		}
	})

	t.Run("R3 no marker stays pending with a visible gap", func(t *testing.T) {
		const requestID = "unmarked-request"
		sources := scanHonestySources(t, pool, requestID)
		task := chainFirstTask(t, ctx, pool, 1410, 1413)
		result, err := store.ScanOnce(ctx, chainFirstRequest(task, sources, scanHonestyEventlessRequestCandidates(requestID)))
		if err != nil {
			t.Fatalf("ScanOnce: %v", err)
		}
		if result.Tickets != 0 || result.Pending != 1 || result.Gaps != 1 {
			t.Fatalf("tickets/pending/gaps = %d/%d/%d, want 0/1/1 (R3 pending, never missing)",
				result.Tickets, result.Pending, result.Gaps)
		}
		if n := scanHonestyTicketCount(t, ctx, pool, scanHonestyRequestPGKey(requestID)); n != 0 {
			t.Fatalf("tickets for the unmarked request = %d, want 0: unproven expectation mints nothing", n)
		}
		if got, ok := reconPersistedThrough(t, ctx, pool, task); !ok || got != 1413 {
			t.Fatalf("pointer = %d (present %v), want 1413 (pending still advances with a gap)", got, ok)
		}
		gaps := reconGapReasons(t, ctx, pool, task)
		if gaps[string(GapQueryFailed)] != 1 {
			t.Fatalf("gaps = %v, want query_failed=1 for the unproven expectation", gaps)
		}
	})

	t.Run("R3 audited legal trim keeps the absence unprovable", func(t *testing.T) {
		const requestID = "trimmed-request"
		now := time.Now().UTC()
		scanHonestySeedObligation(t, ctx, pool, "withdrawal_request", requestID, 1,
			"withdrawal.request.received", now.Add(-4*time.Hour))
		scanHonestySeedRetentionPrune(t, ctx, pool, now.Add(-time.Hour), "1h0m0s")

		sources := scanHonestySources(t, pool, requestID)
		task := chainFirstTask(t, ctx, pool, 1420, 1423)
		result, err := store.ScanOnce(ctx, chainFirstRequest(task, sources, scanHonestyEventlessRequestCandidates(requestID)))
		if err != nil {
			t.Fatalf("ScanOnce: %v", err)
		}
		if result.Tickets != 0 || result.Pending != 1 || result.Gaps != 1 {
			t.Fatalf("tickets/pending/gaps = %d/%d/%d, want 0/1/1 (legal trim cannot prove a loss)",
				result.Tickets, result.Pending, result.Gaps)
		}
	})

	t.Run("R3 never silently closes an existing ticket (old-task compatibility)", func(t *testing.T) {
		// Old-task compatibility: a ticket minted before the discriminator
		// exists must never be deleted, closed or downgraded by a later
		// marker-less scan; it stays observable for re-verification.
		const requestID = "legacy-request"
		discrepancyID := uuid.NewString()
		if _, err := pool.Exec(ctx, `
			INSERT INTO discrepancy (discrepancy_id, category, business_key, content_hash, evidence_version_domain, state)
			VALUES ($1::uuid, 'missing', $2, '\x01'::bytea, '{"evidence_at":"2026-09-26T00:00:00Z"}'::jsonb, 'open_claimable')`,
			discrepancyID, scanHonestyRequestPGKey(requestID)); err != nil {
			t.Fatalf("seed pre-existing ticket: %v", err)
		}
		sources := scanHonestySources(t, pool, requestID)
		task := chainFirstTask(t, ctx, pool, 1450, 1453)
		result, err := store.ScanOnce(ctx, chainFirstRequest(task, sources, scanHonestyEventlessRequestCandidates(requestID)))
		if err != nil {
			t.Fatalf("ScanOnce: %v", err)
		}
		if result.Tickets != 0 || result.Pending != 1 || result.Gaps != 1 {
			t.Fatalf("tickets/pending/gaps = %d/%d/%d, want 0/1/1", result.Tickets, result.Pending, result.Gaps)
		}
		var state string
		if err := pool.QueryRow(ctx, `
			SELECT state FROM discrepancy WHERE discrepancy_id = $1::uuid`, discrepancyID).Scan(&state); err != nil {
			t.Fatalf("read pre-existing ticket: %v", err)
		}
		if state != "open_claimable" {
			t.Fatalf("pre-existing ticket state = %s, want open_claimable (never silently closed)", state)
		}
	})

	t.Run("R2 no catalog event aggregate mints nothing and stays independent", func(t *testing.T) {
		sources := scanHonestySources(t, pool, chainFirstTxHash)
		task := chainFirstTask(t, ctx, pool, 1430, 1433)
		result, err := store.ScanOnce(ctx, chainFirstRequest(task, sources, scanHonestyTxHashCandidates()))
		if err != nil {
			t.Fatalf("ScanOnce: %v", err)
		}
		if result.Tickets != 0 || result.Pending != 0 || result.Gaps != 0 {
			t.Fatalf("tickets/pending/gaps = %d/%d/%d, want 0/0/0 (R2: no catalog event obligation)",
				result.Tickets, result.Pending, result.Gaps)
		}
		if n := scanHonestyTicketCount(t, ctx, pool, "tx_hash="+chainFirstTxHash); n != 0 {
			t.Fatalf("tx-hash tickets = %d, want 0 for the N/A event dimension", n)
		}

		// Independence: the same R2 event dimension must not mask a real
		// chain/PG divergence. With the PG row absent the US1-1 missing ticket
		// still mints exactly as before.
		pgAbsentSources := ScanSources{
			Chain:       chainFirstChainIndex{indexedAt: fixedAt, withLogs: true},
			PG:          chainFirstPGState{readAt: fixedAt},
			Events:      chainFirstEvents{capturedAt: fixedAt},
			Obligations: scanHonestyObligations(t, pool),
		}
		task = chainFirstTask(t, ctx, pool, 1440, 1443)
		result, err = store.ScanOnce(ctx, chainFirstRequest(task, pgAbsentSources, scanHonestyTxHashCandidates()))
		if err != nil {
			t.Fatalf("ScanOnce (PG absent): %v", err)
		}
		if result.Tickets != 1 || result.Pending != 0 || result.Gaps != 0 {
			t.Fatalf("PG-absent tickets/pending/gaps = %d/%d/%d, want 1/0/0 (chain/PG divergence stands)",
				result.Tickets, result.Pending, result.Gaps)
		}
	})
}

// TestIntegrationReverifyEntryUnaffectedByDiscriminatorPending pins the
// reverify-entry half of the T040 acceptance: over a task whose marker-less
// decisive absence stayed R3 pending with a visible gap, the real bounded
// reverify sweep runs unchanged — it rechecks no closed item (there is none),
// fabricates no verdict, closes nothing, and the R3 gap stays open. No T040
// code path participates in reverify.
func TestIntegrationReverifyEntryUnaffectedByDiscriminatorPending(t *testing.T) {
	ctx, pool, store := reconIT(t)
	const requestID = "reverify-boundary-request"

	sources := scanHonestySources(t, pool, requestID)
	task := chainFirstTask(t, ctx, pool, 1460, 1463)
	result, err := store.ScanOnce(ctx, chainFirstRequest(task, sources, scanHonestyEventlessRequestCandidates(requestID)))
	if err != nil {
		t.Fatalf("ScanOnce: %v", err)
	}
	if result.Tickets != 0 || result.Pending != 1 || result.Gaps != 1 {
		t.Fatalf("tickets/pending/gaps = %d/%d/%d, want 0/1/1 (R3 before reverify)",
			result.Tickets, result.Pending, result.Gaps)
	}
	openBefore, err := store.OpenGapCount(ctx, task)
	if err != nil || openBefore != 1 {
		t.Fatalf("open gaps before reverify = %d (err %v), want 1", openBefore, err)
	}

	reverifyBefore := chainFirstCount(t, ctx, pool, "reverify")
	discrepancyBefore := chainFirstCount(t, ctx, pool, "discrepancy")
	sweep, err := store.RunReverifySweep(ctx, ReverifySweepRequest{
		TaskID: task,
		Actor:  reconITOpsActor,
		Slice:  ReverifySlice{MaxItems: 4, MaxPGRequests: 8, MaxItemAttempts: 2},
		Evaluator: ReverifyEvaluatorFunc(func(context.Context, ClosedDiscrepancy, ScanQueryBudget) (ReverifyFinding, error) {
			return ReverifyFinding{}, contractErrorf("no closed item exists for this task; the evaluator must not be called")
		}),
	})
	if err != nil {
		t.Fatalf("RunReverifySweep: %v", err)
	}
	if sweep.Rechecked != 0 || sweep.Consistent != 0 || sweep.Divergent != 0 {
		t.Fatalf("sweep rechecked/consistent/divergent = %d/%d/%d, want 0/0/0",
			sweep.Rechecked, sweep.Consistent, sweep.Divergent)
	}
	if sweep.CursorAdvanced {
		t.Fatalf("sweep cursor advanced=%t, want no cursor movement when no closed item was rechecked", sweep.CursorAdvanced)
	}
	if sweep.VerifiedComplete() {
		t.Fatal("sweep claimed verified completeness over the R3 gap")
	}
	if got := chainFirstCount(t, ctx, pool, "reverify"); got != reverifyBefore {
		t.Fatalf("reverify rows = %d, want unchanged %d (no fabricated verdict)", got, reverifyBefore)
	}
	if got := chainFirstCount(t, ctx, pool, "discrepancy"); got != discrepancyBefore {
		t.Fatalf("discrepancy rows = %d, want unchanged %d (no auto-close/auto-repair)", got, discrepancyBefore)
	}
	openAfter, err := store.OpenGapCount(ctx, task)
	if err != nil || openAfter != 1 {
		t.Fatalf("open gaps after reverify = %d (err %v), want the R3 gap still open", openAfter, err)
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
