//go:build integration

// scan_chainfirst_integration_test.go is the US1 acceptance layer for the
// chain-first reconciliation path over the real 000016 schema (spec.md US1-1,
// FR-002/004/005/007/019, SC-001/002/006):
//
//   - a confirmed chain fact with no PostgreSQL business row is detected as one
//     `missing` discrepancy with a stable identity; a repeated scan of the same
//     scope merges onto that ticket and appends occurrence evidence instead of
//     creating a second ticket;
//   - chain and PG agreeing on a closed, gap-free scope mints no ticket, adds
//     no gap and leaves no pending observation (no false positive);
//   - a chain read failure, a PG read failure, incomplete evidence, or a
//     candidate-enumeration failure stays pending/incomplete with a visible
//     `query_failed` gap and never a consistent conclusion;
//   - budget exhaustion suspends observably with an honest, partial checkpoint:
//     the uncovered remainder is a `budget_exhausted` gap and no checkpoint
//     claims coverage past the persisted prefix;
//   - only 014-owned tables are written: the 007-012 money-path row counts do
//     not move across any scan (no withdrawal request/intent/attempt/payment
//     side effect, FR-014/015).
//
// PostgreSQL comes from testcontainers via the T012 helpers
// (scan_integration_test.go) and the scan loop is the T016 ScanOnce/Store API.
// When no Docker provider is healthy the package reports NOT RUN (t.Skip),
// never a pass.
package reconciliation

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Chain-first acceptance vocabulary. The chain identity must be numeric for
// the T013-T015 adapters to read it.
const (
	chainFirstChainID   = "7"
	chainFirstBlockHash = "0xcfblock"
	chainFirstTxHash    = "0xcftx1"
)

// chainFirstChainIndex is a complete local-index bundle for the queried
// window: one canonical block per height plus (optionally) one confirmed
// transfer-log fact carrying the chain-first business transaction. status may
// be overridden to pin the incomplete-evidence case.
type chainFirstChainIndex struct {
	indexedAt time.Time
	withLogs  bool
	status    ChainFactsStatus
}

func (f chainFirstChainIndex) Observe(ctx context.Context, q ChainFactsQuery) (ChainFactsBundle, error) {
	status := f.status
	if status == "" {
		status = ChainFactsComplete
	}
	bundle := ChainFactsBundle{
		ChainID: q.ChainID, From: q.From, To: q.To,
		Source: ChainSourceLocalIndex, CapturedAt: f.indexedAt, Status: status,
	}
	for height := q.From; height <= q.To; height++ {
		bundle.Blocks = append(bundle.Blocks, ChainFactBlock{
			Number: height, Hash: chainFirstBlockHash, Canonical: true, IndexedAt: f.indexedAt,
		})
	}
	if f.withLogs {
		bundle.Logs = append(bundle.Logs, ChainFactLog{
			BlockNumber: q.From, BlockHash: chainFirstBlockHash, TxHash: chainFirstTxHash,
			LogIndex: 0, Contract: "0xcfcontract", Topic0: "0xcf-transfer", Data: "0x01",
		})
	}
	return bundle, nil
}

// chainFirstChainUnavailable simulates an RPC/index read failure: the adapter
// reports the error alongside a partial bundle, exactly like T013 Observe, so
// the compare loop must keep the chain party unknown.
type chainFirstChainUnavailable struct{ err error }

func (f chainFirstChainUnavailable) Observe(ctx context.Context, q ChainFactsQuery) (ChainFactsBundle, error) {
	return ChainFactsBundle{
		ChainID: q.ChainID, From: q.From, To: q.To,
		Source: ChainSourceLocalIndex, CapturedAt: time.Now().UTC(),
	}, f.err
}

// chainFirstPGState answers every read definitively: complete when the key is
// in present, absent otherwise.
type chainFirstPGState struct {
	present map[string]bool
	readAt  time.Time
}

func (f chainFirstPGState) Read(ctx context.Context, req PGReadRequest) (PGStateRecord, error) {
	rec := PGStateRecord{
		BusinessType: req.BusinessType, BusinessKey: req.Key,
		ChainID: req.ChainID, ReadAt: f.readAt,
		Status: PGStateAbsent,
	}
	if f.present[req.Key.Value] {
		rec.Status = PGStateComplete
	}
	return rec, nil
}

// chainFirstPGUnreachable simulates a failed PG read: the status is
// unreachable and the error is returned alongside the record, so the compare
// loop must keep the party unknown instead of concluding.
type chainFirstPGUnreachable struct {
	err    error
	readAt time.Time
}

func (f chainFirstPGUnreachable) Read(ctx context.Context, req PGReadRequest) (PGStateRecord, error) {
	return PGStateRecord{
		BusinessType: req.BusinessType, BusinessKey: req.Key, ChainID: req.ChainID,
		Status: PGStateUnreachable, Reason: PGReasonReadFailed, ReadAt: f.readAt,
	}, f.err
}

// chainFirstEvents returns a closed (complete) event bundle with the configured
// observations; an empty observation list means "covered and empty".
type chainFirstEvents struct {
	capturedAt   time.Time
	status       EventDeliveryStatus
	observations []EventDeliveryObservation
}

func (f chainFirstEvents) Observe(ctx context.Context, q EventStateQuery) (EventStateEvidence, error) {
	status := f.status
	if status == "" {
		status = EventDeliveryComplete
	}
	return EventStateEvidence{
		Scope: q.Scope, Interval: q.Interval, CapturedAt: f.capturedAt,
		Status: status, Observations: f.observations,
	}, nil
}

// chainFirstFailingCandidates simulates a candidate-enumeration failure: the
// interval must be committed with a query_failed gap, never skipped silently.
type chainFirstFailingCandidates struct{ err error }

func (f chainFirstFailingCandidates) ScanCandidates(ctx context.Context, interval ScanInterval) ([]ScanCandidate, error) {
	return nil, f.err
}

// chainFirstCandidateSource enumerates the same chain-anchored business
// candidate (a confirmed tx hash with its canonical block) in every interval.
// An empty eventKey leaves the event identity at the business key.
func chainFirstCandidateSource(eventKey BusinessKey) ScanCandidateSource {
	return staticScanCandidates{candidates: func(interval ScanInterval) []ScanCandidate {
		return []ScanCandidate{{
			BusinessType: BusinessWithdrawal,
			BusinessKey:  BusinessKey{Kind: BusinessKeyTxHash, Value: chainFirstTxHash},
			ChainFact: ChainFactRef{
				BlockNumber: interval.From.Height,
				BlockHash:   chainFirstBlockHash,
				TxHash:      chainFirstTxHash,
			},
			EventKey: eventKey,
		}}
	}}
}

// chainFirstConsistentEvents is the event party of the agreeing case: one
// delivered withdrawal event for the candidate aggregate.
func chainFirstConsistentEvents(capturedAt time.Time) chainFirstEvents {
	return chainFirstEvents{
		capturedAt: capturedAt,
		observations: []EventDeliveryObservation{{
			EventID:       uuid.New(),
			AggregateType: "withdrawal",
			AggregateID:   "cf-ok",
			OccurredAt:    capturedAt,
			EmittedAt:     capturedAt,
		}},
	}
}

// chainFirstAgreeingEventKey maps the candidate to the delivered event
// aggregate identity.
func chainFirstAgreeingEventKey() BusinessKey {
	return BusinessKey{Kind: EventBusinessKeyAggregate, Value: "withdrawal/cf-ok"}
}

// chainFirstTask seeds a running height task carrying the numeric chain id the
// T013-T015 adapters require.
func chainFirstTask(t *testing.T, ctx context.Context, pool *pgxpool.Pool, from, to int64) string {
	t.Helper()
	taskID := uuid.NewString()
	reconSeedHeightTask(t, ctx, pool, taskID, from, to, "running", "")
	if _, err := pool.Exec(ctx,
		`UPDATE recon_task SET scope_chain_id = $2 WHERE task_id = $1::uuid`, taskID, chainFirstChainID); err != nil {
		t.Fatalf("set numeric chain id of %s: %v", taskID, err)
	}
	return taskID
}

// chainFirstRequest is the shared acceptance invocation shape.
func chainFirstRequest(taskID string, sources ScanSources, candidates ScanCandidateSource) ScanOnceRequest {
	return ScanOnceRequest{
		TaskID: taskID, Owner: reconITOwner, LeaseTTL: time.Minute,
		Limits: BudgetLimits{
			MaxConcurrency: 1, MaxSpanPerClaim: 4, MaxDuration: time.Minute,
			MaxPGRequests: 50, MaxRPCRequests: 4,
		},
		FreshnessTolerance: time.Hour,
		Sources:            sources,
		Candidates:         candidates,
	}
}

// chainFirstTicket is one discrepancy row as observed through SQL.
type chainFirstTicket struct {
	ID       string
	Category string
	State    string
	HashHex  string
}

// chainFirstTicketOf reads the ticket row of one business key.
func chainFirstTicketOf(t *testing.T, ctx context.Context, pool *pgxpool.Pool, businessKey string) chainFirstTicket {
	t.Helper()
	var ticket chainFirstTicket
	if err := pool.QueryRow(ctx, `
		SELECT discrepancy_id::text, category, state, encode(content_hash, 'hex')
		FROM discrepancy WHERE business_key = $1`, businessKey).Scan(
		&ticket.ID, &ticket.Category, &ticket.State, &ticket.HashHex); err != nil {
		t.Fatalf("read ticket %s: %v", businessKey, err)
	}
	return ticket
}

// chainFirstOccurrences counts the occurrence evidence rows of one ticket.
func chainFirstOccurrences(t *testing.T, ctx context.Context, pool *pgxpool.Pool, businessKey string) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(ctx, `
		SELECT count(*)::bigint FROM discrepancy_occurrence o
		JOIN discrepancy d ON d.discrepancy_id = o.discrepancy_id
		WHERE d.business_key = $1`, businessKey).Scan(&n); err != nil {
		t.Fatalf("count occurrences of %s: %v", businessKey, err)
	}
	return n
}

// chainFirstCount counts the rows of one table (read-only).
func chainFirstCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, table string) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(ctx, `SELECT count(*)::bigint FROM `+table).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

// chainFirstFundsTables are the 007-012 money-path tables a 014 scan must never
// write: no withdrawal request/intent/attempt/step/receipt row may be created,
// changed, or deleted (FR-014/015).
var chainFirstFundsTables = []string{
	"withdrawal_requests",
	"withdrawal_authorizations",
	"withdrawal_authorization_scopes",
	"payment_intents",
	"execution_claims",
	"execution_steps",
	"tx_attempts",
	"tx_attempt_signings",
	"tx_send_attempts",
	"tx_reconciliations",
	"tx_receipts",
}

// chainFirstFundsCounts snapshots the money-path row counts of one database.
func chainFirstFundsCounts(t *testing.T, ctx context.Context, pool *pgxpool.Pool) map[string]int64 {
	t.Helper()
	out := make(map[string]int64, len(chainFirstFundsTables))
	for _, table := range chainFirstFundsTables {
		out[table] = chainFirstCount(t, ctx, pool, table)
	}
	return out
}

func TestIntegrationScanOnceChainFirstAcceptance(t *testing.T) {
	ctx, pool, store := reconIT(t)
	fundsBefore := chainFirstFundsCounts(t, ctx, pool)

	fixedAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	chain := chainFirstChainIndex{indexedAt: fixedAt, withLogs: true}
	pgAbsent := chainFirstPGState{readAt: fixedAt}
	eventsEmpty := chainFirstEvents{capturedAt: fixedAt}

	t.Run("chain_fact_without_pg_row_mints_one_missing_ticket_then_merges", func(t *testing.T) {
		sources := ScanSources{Chain: chain, PG: pgAbsent, Events: eventsEmpty}
		candidates := chainFirstCandidateSource(BusinessKey{})

		firstTask := chainFirstTask(t, ctx, pool, 1100, 1103)
		first, err := store.ScanOnce(ctx, chainFirstRequest(firstTask, sources, candidates))
		if err != nil {
			t.Fatalf("first ScanOnce: %v", err)
		}
		if first.Stop != ScanStopScopeExhausted || first.Committed != 1 {
			t.Fatalf("first scan stop/committed = %q/%d, want scope_exhausted/1", first.Stop, first.Committed)
		}
		if first.Tickets != 1 || first.Merged != 0 || first.Pending != 0 || first.Gaps != 0 {
			t.Fatalf("first scan tickets/merged/pending/gaps = %d/%d/%d/%d, want 1/0/0/0: a confirmed chain fact without a PG row is one missing ticket and nothing else",
				first.Tickets, first.Merged, first.Pending, first.Gaps)
		}
		if got, ok := reconPersistedThrough(t, ctx, pool, firstTask); !ok || got != 1103 {
			t.Fatalf("first task pointer = %d (present %v), want 1103", got, ok)
		}
		if n := chainFirstCount(t, ctx, pool, "disposition"); n != 0 {
			t.Fatalf("disposition rows = %d, want 0: the missing ticket is alert-only", n)
		}
		if n := chainFirstCount(t, ctx, pool, "reverify"); n != 0 {
			t.Fatalf("reverify rows = %d, want 0: no auto-repair path may run off a scan", n)
		}

		businessKey := "tx_hash=" + chainFirstTxHash
		ticket := chainFirstTicketOf(t, ctx, pool, businessKey)
		if ticket.Category != string(CategoryMissing) || ticket.State != "open_claimable" {
			t.Fatalf("ticket = %s/%s, want missing/open_claimable", ticket.Category, ticket.State)
		}
		if n := chainFirstCount(t, ctx, pool, "discrepancy"); n != 1 {
			t.Fatalf("discrepancy rows = %d, want exactly 1", n)
		}
		if n := chainFirstOccurrences(t, ctx, pool, businessKey); n != 1 {
			t.Fatalf("occurrences after the first scan = %d, want 1", n)
		}

		// The same scope re-scanned by a second task: same stable identity, one
		// ticket, occurrence evidence appended (SC-002).
		secondTask := chainFirstTask(t, ctx, pool, 1100, 1103)
		second, err := store.ScanOnce(ctx, chainFirstRequest(secondTask, sources, candidates))
		if err != nil {
			t.Fatalf("second ScanOnce: %v", err)
		}
		if second.Tickets != 0 || second.Merged != 1 || second.Occurrences != 1 || second.Pending != 0 {
			t.Fatalf("second scan tickets/merged/occurrences/pending = %d/%d/%d/%d, want 0/1/1/0 (dedup onto the original ticket)",
				second.Tickets, second.Merged, second.Occurrences, second.Pending)
		}
		repeat := chainFirstTicketOf(t, ctx, pool, businessKey)
		if repeat.ID != ticket.ID || repeat.HashHex != ticket.HashHex {
			t.Fatalf("ticket identity drifted across scans: id %s -> %s, content hash %s -> %s",
				ticket.ID, repeat.ID, ticket.HashHex, repeat.HashHex)
		}
		if n := chainFirstCount(t, ctx, pool, "discrepancy"); n != 1 {
			t.Fatalf("discrepancy rows after the repeat scan = %d, want 1 (no duplicate ticket)", n)
		}
		if n := chainFirstOccurrences(t, ctx, pool, businessKey); n != 2 {
			t.Fatalf("occurrences after the repeat scan = %d, want 2 (evidence appended, no new ticket)", n)
		}
		var attributedTasks int64
		if err := pool.QueryRow(ctx, `
			SELECT count(DISTINCT o.scan_task_id)::bigint FROM discrepancy_occurrence o
			JOIN discrepancy d ON d.discrepancy_id = o.discrepancy_id
			WHERE d.business_key = $1`, businessKey).Scan(&attributedTasks); err != nil {
			t.Fatalf("count occurrence scan tasks: %v", err)
		}
		if attributedTasks != 2 {
			t.Fatalf("occurrence scan_task_id values = %d, want 2 (both scans attributed)", attributedTasks)
		}
	})

	t.Run("chain_and_pg_agree_mint_no_ticket_and_no_gap", func(t *testing.T) {
		pgPresent := chainFirstPGState{present: map[string]bool{chainFirstTxHash: true}, readAt: fixedAt}
		events := chainFirstConsistentEvents(fixedAt)
		sources := ScanSources{Chain: chain, PG: pgPresent, Events: events}
		candidates := chainFirstCandidateSource(chainFirstAgreeingEventKey())

		before := chainFirstCount(t, ctx, pool, "discrepancy")
		task := chainFirstTask(t, ctx, pool, 1120, 1123)
		result, err := store.ScanOnce(ctx, chainFirstRequest(task, sources, candidates))
		if err != nil {
			t.Fatalf("ScanOnce: %v", err)
		}
		if result.Tickets != 0 || result.Merged != 0 || result.Pending != 0 ||
			result.Gaps != 0 || result.Absorbed != 0 {
			t.Fatalf("agreeing scan tickets/merged/pending/gaps/absorbed = %d/%d/%d/%d/%d, want all zero (no false positive)",
				result.Tickets, result.Merged, result.Pending, result.Gaps, result.Absorbed)
		}
		if result.Candidates != 1 || result.Committed != 1 || result.Stop != ScanStopScopeExhausted {
			t.Fatalf("agreeing scan candidates/committed/stop = %d/%d/%q, want 1/1/%q",
				result.Candidates, result.Committed, result.Stop, ScanStopScopeExhausted)
		}
		if after := chainFirstCount(t, ctx, pool, "discrepancy"); after != before {
			t.Fatalf("discrepancy rows moved on agreeing evidence: %d -> %d, want no new ticket", before, after)
		}
		if got, ok := reconPersistedThrough(t, ctx, pool, task); !ok || got != 1123 {
			t.Fatalf("agreeing task pointer = %d (present %v), want 1123", got, ok)
		}
		open, err := store.OpenGapCount(ctx, task)
		if err != nil {
			t.Fatalf("OpenGapCount: %v", err)
		}
		if open != 0 {
			t.Fatalf("open gaps after agreeing evidence = %d, want 0 (the range is closed and gap-free)", open)
		}
	})

	t.Run("chain_read_failure_stays_pending_with_query_failed_gaps", func(t *testing.T) {
		sources := ScanSources{
			Chain:  chainFirstChainUnavailable{err: errors.New("chain index read failed")},
			PG:     pgAbsent,
			Events: eventsEmpty,
		}
		before := chainFirstCount(t, ctx, pool, "discrepancy")
		task := chainFirstTask(t, ctx, pool, 1140, 1147)
		result, err := store.ScanOnce(ctx, chainFirstRequest(task, sources, chainFirstCandidateSource(BusinessKey{})))
		if err != nil {
			t.Fatalf("ScanOnce: %v", err)
		}
		chainFirstAssertPendingGaps(t, ctx, pool, store, task, result, before, 1147)
	})

	t.Run("pg_read_failure_stays_pending_with_query_failed_gaps", func(t *testing.T) {
		sources := ScanSources{
			Chain:  chain,
			PG:     chainFirstPGUnreachable{err: errors.New("pg read failed"), readAt: fixedAt},
			Events: eventsEmpty,
		}
		before := chainFirstCount(t, ctx, pool, "discrepancy")
		task := chainFirstTask(t, ctx, pool, 1160, 1167)
		result, err := store.ScanOnce(ctx, chainFirstRequest(task, sources, chainFirstCandidateSource(BusinessKey{})))
		if err != nil {
			t.Fatalf("ScanOnce: %v", err)
		}
		chainFirstAssertPendingGaps(t, ctx, pool, store, task, result, before, 1167)
	})

	t.Run("incomplete_evidence_stays_pending_with_query_failed_gaps", func(t *testing.T) {
		sources := ScanSources{
			Chain:  chainFirstChainIndex{indexedAt: fixedAt, withLogs: true, status: ChainFactsIncomplete},
			PG:     pgAbsent,
			Events: eventsEmpty,
		}
		before := chainFirstCount(t, ctx, pool, "discrepancy")
		task := chainFirstTask(t, ctx, pool, 1180, 1187)
		result, err := store.ScanOnce(ctx, chainFirstRequest(task, sources, chainFirstCandidateSource(BusinessKey{})))
		if err != nil {
			t.Fatalf("ScanOnce: %v", err)
		}
		chainFirstAssertPendingGaps(t, ctx, pool, store, task, result, before, 1187)
	})

	t.Run("candidate_enumeration_failure_commits_query_failed_gaps", func(t *testing.T) {
		sources := ScanSources{Chain: chain, PG: pgAbsent, Events: eventsEmpty}
		before := chainFirstCount(t, ctx, pool, "discrepancy")
		task := chainFirstTask(t, ctx, pool, 1200, 1207)
		result, err := store.ScanOnce(ctx, chainFirstRequest(task, sources,
			chainFirstFailingCandidates{err: errors.New("candidate source unavailable")}))
		if err != nil {
			t.Fatalf("ScanOnce: %v", err)
		}
		if result.Attempts != 2 || result.Committed != 2 {
			t.Fatalf("attempts/committed = %d/%d, want 2/2 (the failing intervals are still committed)", result.Attempts, result.Committed)
		}
		chainFirstAssertPendingGaps(t, ctx, pool, store, task, result, before, 1207)
	})

	t.Run("budget_exhaustion_keeps_the_checkpoint_honest", func(t *testing.T) {
		// Budget arithmetic: 1 PG charge for stale recovery, then interval one
		// spends claim 1 + chain 1 + events 1 + enumeration 1 + PG read 1 +
		// commit 1 = 6. Seven charges let exactly interval one commit; the
		// second interval is refused before its claim, and the uncovered
		// remainder must stay visible as a budget_exhausted gap.
		pgPresent := chainFirstPGState{present: map[string]bool{chainFirstTxHash: true}, readAt: fixedAt}
		events := chainFirstConsistentEvents(fixedAt)
		sources := ScanSources{Chain: chain, PG: pgPresent, Events: events}
		candidates := chainFirstCandidateSource(chainFirstAgreeingEventKey())

		task := chainFirstTask(t, ctx, pool, 1220, 1227)
		req := chainFirstRequest(task, sources, candidates)
		req.Limits.MaxPGRequests = 7
		req.Limits.MaxRPCRequests = 1
		result, err := store.ScanOnce(ctx, req)
		if err != nil {
			t.Fatalf("ScanOnce: %v", err)
		}
		if !result.Suspended || result.SuspendResource != ResourcePG || result.SuspendDetail == "" {
			t.Fatalf("result suspended/resource/detail = %v/%q/%q, want an observable PG suspension",
				result.Suspended, result.SuspendResource, result.SuspendDetail)
		}
		if result.Committed != 1 || result.Attempts != 1 || result.Tickets != 0 {
			t.Fatalf("committed/attempts/tickets = %d/%d/%d, want 1/1/0 (the first interval commits; no ticket)",
				result.Committed, result.Attempts, result.Tickets)
		}
		if result.LastCheckpoint == nil || result.LastCheckpoint.ResultPersistedThrough.Height != 1223 {
			t.Fatalf("last checkpoint = %+v, want persisted through 1223 (interval one only)", result.LastCheckpoint)
		}
		if got, ok := reconPersistedThrough(t, ctx, pool, task); !ok || got != 1223 {
			t.Fatalf("pointer after suspension = %d (present %v), want 1223", got, ok)
		}
		if state, reason := reconTaskState(t, ctx, store, task); state != TaskStateSuspendedBudget || reason == "" {
			t.Fatalf("task = %s (reason %q), want suspended_budget with a recorded reason", state, reason)
		}

		// The uncovered remainder [1224,1227] is a visible budget_exhausted
		// gap, and no checkpoint may claim the tail.
		gaps := reconGapReasons(t, ctx, pool, task)
		if gaps[string(GapBudgetExhausted)] != 1 {
			t.Fatalf("gaps = %v, want budget_exhausted=1 over the uncovered remainder", gaps)
		}
		var gapStart, gapEnd int64
		if err := pool.QueryRow(ctx, `
			SELECT range_start, range_end FROM recon_gap
			WHERE task_id = $1::uuid AND reason = 'budget_exhausted'`, task).Scan(&gapStart, &gapEnd); err != nil {
			t.Fatalf("read budget_exhausted gap range: %v", err)
		}
		if gapStart != 1224 || gapEnd != 1227 {
			t.Fatalf("budget_exhausted gap = [%d,%d], want [1224,1227]", gapStart, gapEnd)
		}
		if n := reconCheckpointCount(t, ctx, pool, task); n != 1 {
			t.Fatalf("checkpoint rows = %d, want 1 (only the committed interval)", n)
		}
		var maxPersisted, maxCovered int64
		if err := pool.QueryRow(ctx, `
			SELECT COALESCE(MAX(result_persisted_through), 0), COALESCE(MAX(covered_through), 0)
			FROM recon_checkpoint WHERE task_id = $1::uuid`, task).Scan(&maxPersisted, &maxCovered); err != nil {
			t.Fatalf("read checkpoint coverage: %v", err)
		}
		if maxPersisted != 1223 || maxCovered != 1223 {
			t.Fatalf("checkpoint coverage = persisted %d / covered %d, want 1223/1223: the uncovered tail must never be marked complete",
				maxPersisted, maxCovered)
		}
		head, err := store.CheckpointHead(ctx, task)
		if err != nil || head == nil {
			t.Fatalf("CheckpointHead = %+v (err %v), want a partial head", head, err)
		}
		if head.ResultPersistedThrough.Height != 1223 || head.CoveredThrough.Height != 1223 {
			t.Fatalf("checkpoint head = %+v, want 1223/1223", head)
		}
		if open, err := store.OpenGapCount(ctx, task); err != nil || open != 1 {
			t.Fatalf("open gaps = %d (err %v), want 1", open, err)
		}
		// Suspended is not "fully consistent": no done edge exists.
		if _, err := store.TransitionTask(ctx, TaskTransitionRequest{
			TaskID: task, To: TaskStateDone, Reason: "budget suspended", Actor: reconITOpsActor,
		}); !errors.Is(err, ErrIllegalTaskTransition) {
			t.Fatalf("suspended_budget -> done err = %v, want ErrIllegalTaskTransition", err)
		}
	})

	fundsAfter := chainFirstFundsCounts(t, ctx, pool)
	for _, table := range chainFirstFundsTables {
		if fundsAfter[table] != fundsBefore[table] {
			t.Fatalf("014 scan wrote the funds table %s: rows %d -> %d; no withdrawal intent or payment side effect is permitted (FR-014/015)",
				table, fundsBefore[table], fundsAfter[table])
		}
	}
}

// chainFirstAssertPendingGaps pins the shared failure shape: every interval is
// committed with a query_failed gap, stays pending with zero tickets, and the
// task can never close over the open gaps.
func chainFirstAssertPendingGaps(t *testing.T, ctx context.Context, pool *pgxpool.Pool, store *Store,
	task string, result ScanOnceResult, discrepanciesBefore, scopeEnd int64) {
	t.Helper()
	if result.Tickets != 0 || result.Merged != 0 {
		t.Fatalf("tickets/merged = %d/%d, want 0/0: missing evidence is never a ticket", result.Tickets, result.Merged)
	}
	if result.Pending != 2 {
		t.Fatalf("pending = %d, want 2 (both intervals stay under observation)", result.Pending)
	}
	if result.Stop != ScanStopScopeExhausted {
		t.Fatalf("stop = %q, want %q", result.Stop, ScanStopScopeExhausted)
	}
	gaps := reconGapReasons(t, ctx, pool, task)
	if gaps[string(GapQueryFailed)] != 2 {
		t.Fatalf("gaps = %v, want query_failed=2 (one visible gap per unproven interval)", gaps)
	}
	if got, ok := reconPersistedThrough(t, ctx, pool, task); !ok || got != scopeEnd {
		t.Fatalf("pointer = %d (present %v), want %d (results persisted with gaps, never skipped)", got, ok, scopeEnd)
	}
	if state, _ := reconTaskState(t, ctx, store, task); state != TaskStateRunning {
		t.Fatalf("task state = %s, want running (pending is not a conclusion)", state)
	}
	if after := chainFirstCount(t, ctx, pool, "discrepancy"); after != discrepanciesBefore {
		t.Fatalf("discrepancy rows moved on unproven evidence: %d -> %d, want no ticket", discrepanciesBefore, after)
	}
	if _, err := store.TransitionTask(ctx, TaskTransitionRequest{
		TaskID: task, To: TaskStateDone, Reason: "close over incomplete evidence", Actor: reconITOpsActor,
	}); !errors.Is(err, ErrTaskNotComplete) {
		t.Fatalf("done over query_failed gaps err = %v, want ErrTaskNotComplete", err)
	}
}
