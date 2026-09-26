//go:build integration

// scan_integration_test.go is the T012 integration layer for US1 (quickstart
// §1–§3/§8; contracts/task-lifecycle.md Claim–Execute–Commit; data-model.md
// §5/§5.1):
//
//   - scoped claim → execute → commit over the real 000016 schema: the
//     single-claimed-attempt rule, contiguous height/time intervals, the
//     ErrScopeExhausted end, and a discarded late submitter that never
//     duplicates a batch or moves the pointer;
//   - checkpoint resume: crash recovery abandons a stale attempt (gap row,
//     pointer untouched) and re-claims from the persisted prefix — no miss, no
//     duplicate side effect; an expired-lease submit is discarded + audited;
//   - budget exhaustion → observable suspension with visible gap rows and no
//     new claims, then resume without missing an interval;
//   - the upstream-unconnected negative case: the classifier verdict is
//     `incomplete`/pending only — never `consistent`, never an external-credit
//     claim — and the persisted incomplete marker keeps the scope unclosable.
//
// PostgreSQL comes from testcontainers. When no Docker provider is healthy the
// package reports NOT RUN (t.Skip), never a pass.
package reconciliation

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/xtianxx/txharbor/internal/db"
)

const (
	reconITChainID    = "recon-it-chain"
	reconITProbeTable = "recon_it_scan_probe"
	reconITOwner      = "it-worker"
	reconITOpsActor   = "it-ops"
)

// startReconPostgres boots a real PostgreSQL container, applies the embedded
// migrations (including 000016) and skips — never passes — when no Docker
// provider is available.
func startReconPostgres(t *testing.T) string {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()
	ctr, err := postgres.Run(ctx, "postgres:18.6-trixie",
		postgres.WithDatabase("txharbor"),
		postgres.WithUsername("txharbor"),
		postgres.WithPassword("txharbor"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2)),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })
	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres connection string: %v", err)
	}
	opts := db.MigrateOptions{DSN: dsn, LockTimeout: 10 * time.Second, ConnectTimeout: 10 * time.Second}
	if err := db.MigrateUp(ctx, opts, io.Discard); err != nil {
		t.Fatalf("MigrateUp() error = %v", err)
	}
	return dsn
}

// reconIT provides one migrated database plus a store and the batch probe
// table used to observe result persistence side effects.
func reconIT(t *testing.T) (context.Context, *pgxpool.Pool, *Store) {
	t.Helper()
	dsn := startReconPostgres(t)
	ctx := context.Background()
	pool, err := db.OpenPool(ctx, dsn, 10*time.Second)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(ctx,
		`CREATE TABLE IF NOT EXISTS `+reconITProbeTable+` (attempt_id UUID PRIMARY KEY)`); err != nil {
		t.Fatalf("create probe table: %v", err)
	}
	store, err := NewStore(pool)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	return ctx, pool, store
}

// reconSeedHeightTask inserts a height-scoped task in the requested state.
// receiptSource empty means a connected withdrawal source.
func reconSeedHeightTask(t *testing.T, ctx context.Context, pool *pgxpool.Pool, taskID string, start, end int64, state, receiptSource string) {
	t.Helper()
	if receiptSource == "" {
		receiptSource = `{"withdrawal":{"source":"test-source","connected":true}}`
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO recon_task (task_id, scope_chain_id, scope_kind, scope_start, scope_end,
		    business_types, upstream_receipt_source, policy_refs, state, budget, created_by)
		VALUES ($1::uuid, $2, 'height', $3, $4, ARRAY['withdrawal']::text[], $5::jsonb,
		        '{}'::jsonb, $6, '{}'::jsonb, 'it-recon')`,
		taskID, reconITChainID, start, end, receiptSource, state); err != nil {
		t.Fatalf("seed height task %s: %v", taskID, err)
	}
}

// reconSeedTimeTask inserts a time-scoped running task.
func reconSeedTimeTask(t *testing.T, ctx context.Context, pool *pgxpool.Pool, taskID string, start, end time.Time) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO recon_task (task_id, scope_chain_id, scope_kind, scope_start_at, scope_end_at,
		    business_types, upstream_receipt_source, policy_refs, state, budget, created_by)
		VALUES ($1::uuid, $2, 'time', $3, $4, ARRAY['withdrawal']::text[],
		        '{"withdrawal":{"source":"test-source","connected":true}}'::jsonb,
		        '{}'::jsonb, 'running', '{}'::jsonb, 'it-recon')`,
		taskID, reconITChainID, start, end); err != nil {
		t.Fatalf("seed time task %s: %v", taskID, err)
	}
}

func reconClaim(t *testing.T, ctx context.Context, store *Store, taskID, owner string, span int64, lease time.Duration) ClaimAttemptResult {
	t.Helper()
	res, err := store.ClaimScanAttempt(ctx, ClaimAttemptRequest{
		TaskID: taskID, Owner: owner, Span: span, LeaseTTL: lease,
	})
	if err != nil {
		t.Fatalf("ClaimScanAttempt(%s) = %v, want a claimed interval", taskID, err)
	}
	return res
}

func reconHeightRange(t *testing.T, res ClaimAttemptResult) (int64, int64) {
	t.Helper()
	if res.RangeStart.Kind != ScopeHeight || res.RangeEnd.Kind != ScopeHeight {
		t.Fatalf("claimed range kinds = %q/%q, want height", res.RangeStart.Kind, res.RangeEnd.Kind)
	}
	return res.RangeStart.Height, res.RangeEnd.Height
}

// reconCommit commits one attempt and records a probe row in the same
// transaction, so duplicate side effects stay observable.
func reconCommit(t *testing.T, ctx context.Context, store *Store, taskID, attemptID string) CommitScanBatchResult {
	t.Helper()
	res, err := store.CommitScanBatch(ctx, CommitScanBatchRequest{
		TaskID: taskID, AttemptID: attemptID,
		PersistResults: func(ctx context.Context, tx pgx.Tx) error {
			_, err := tx.Exec(ctx, `INSERT INTO `+reconITProbeTable+` (attempt_id) VALUES ($1::uuid)`, attemptID)
			return err
		},
	})
	if err != nil {
		t.Fatalf("CommitScanBatch(%s/%s) = %+v, err %v, want committed", taskID, attemptID, res, err)
	}
	if !res.Committed || res.Checkpoint == nil {
		t.Fatalf("CommitScanBatch(%s/%s) not committed: %+v", taskID, attemptID, res)
	}
	return res
}

// reconWaitForLeaseExpiry polls the real clock bounded by a deadline; no test
// sleeps on a fixed guess.
func reconWaitForLeaseExpiry(t *testing.T, ctx context.Context, pool *pgxpool.Pool, attemptID string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var expired bool
		if err := pool.QueryRow(ctx,
			`SELECT lease_expires_at <= now() FROM recon_scan_attempt WHERE attempt_id = $1::uuid`,
			attemptID).Scan(&expired); err != nil {
			t.Fatalf("read lease of %s: %v", attemptID, err)
		}
		if expired {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("lease of %s did not expire within 5s", attemptID)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func reconPersistedThrough(t *testing.T, ctx context.Context, pool *pgxpool.Pool, taskID string) (int64, bool) {
	t.Helper()
	var persisted *int64
	err := pool.QueryRow(ctx, `
		SELECT result_persisted_through FROM recon_checkpoint
		WHERE task_id = $1::uuid ORDER BY seq DESC LIMIT 1`, taskID).Scan(&persisted)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false
	}
	if err != nil {
		t.Fatalf("read pointer of %s: %v", taskID, err)
	}
	if persisted == nil {
		t.Fatalf("task %s is height-scoped but its pointer is not a height", taskID)
	}
	return *persisted, true
}

func reconCheckpointCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, taskID string) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(ctx,
		`SELECT count(*)::bigint FROM recon_checkpoint WHERE task_id = $1::uuid`, taskID).Scan(&n); err != nil {
		t.Fatalf("count checkpoints of %s: %v", taskID, err)
	}
	return n
}

func reconGapReasons(t *testing.T, ctx context.Context, pool *pgxpool.Pool, taskID string) map[string]int64 {
	t.Helper()
	rows, err := pool.Query(ctx,
		`SELECT reason, count(*)::bigint FROM recon_gap WHERE task_id = $1::uuid GROUP BY reason`, taskID)
	if err != nil {
		t.Fatalf("read gaps of %s: %v", taskID, err)
	}
	defer rows.Close()
	out := make(map[string]int64)
	for rows.Next() {
		var reason string
		var n int64
		if err := rows.Scan(&reason, &n); err != nil {
			t.Fatalf("scan gap row of %s: %v", taskID, err)
		}
		out[reason] = n
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("gap rows of %s: %v", taskID, err)
	}
	return out
}

func reconAttemptState(t *testing.T, ctx context.Context, pool *pgxpool.Pool, attemptID string) string {
	t.Helper()
	var state string
	if err := pool.QueryRow(ctx,
		`SELECT state FROM recon_scan_attempt WHERE attempt_id = $1::uuid`, attemptID).Scan(&state); err != nil {
		t.Fatalf("read attempt %s: %v", attemptID, err)
	}
	return state
}

func reconAttemptCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, taskID string) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(ctx,
		`SELECT count(*)::bigint FROM recon_scan_attempt WHERE task_id = $1::uuid`, taskID).Scan(&n); err != nil {
		t.Fatalf("count attempts of %s: %v", taskID, err)
	}
	return n
}

func reconAuditCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, taskID, action string) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(ctx, `
		SELECT count(*)::bigint FROM recon_audit
		WHERE action = $2 AND target->>'task_id' = $1`, taskID, action).Scan(&n); err != nil {
		t.Fatalf("count %s audits of %s: %v", action, taskID, err)
	}
	return n
}

func reconProbeCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(ctx, `SELECT count(*)::bigint FROM `+reconITProbeTable).Scan(&n); err != nil {
		t.Fatalf("count probe rows: %v", err)
	}
	return n
}

func reconTaskState(t *testing.T, ctx context.Context, store *Store, taskID string) (TaskState, string) {
	t.Helper()
	task, err := store.TaskByID(ctx, taskID)
	if err != nil {
		t.Fatalf("TaskByID(%s): %v", taskID, err)
	}
	return task.State, task.PauseReason
}

func TestIntegrationScopedScanCheckpointResumeAndBudget(t *testing.T) {
	ctx, pool, store := reconIT(t)

	t.Run("scoped_claim_commit_advances_pointer_contiguously", func(t *testing.T) {
		taskID := uuid.NewString()
		reconSeedHeightTask(t, ctx, pool, taskID, 100, 112, "running", "")

		first := reconClaim(t, ctx, store, taskID, "worker-a", 4, time.Minute)
		if start, end := reconHeightRange(t, first); start != 100 || end != 103 {
			t.Fatalf("first claim = [%d,%d], want [100,103]", start, end)
		}
		// One claimed attempt per task: the partial unique index must refuse a
		// concurrent second claim (the loser retries the next interval).
		if _, err := store.ClaimScanAttempt(ctx, ClaimAttemptRequest{
			TaskID: taskID, Owner: "worker-b", Span: 4, LeaseTTL: time.Minute,
		}); !errors.Is(err, ErrClaimTaken) {
			t.Fatalf("second claim while one is live err = %v, want ErrClaimTaken", err)
		}
		reconCommit(t, ctx, store, taskID, first.AttemptID)
		if got, ok := reconPersistedThrough(t, ctx, pool, taskID); !ok || got != 103 {
			t.Fatalf("pointer after first commit = %d (present %v), want 103", got, ok)
		}

		for _, want := range [][2]int64{{104, 107}, {108, 111}, {112, 112}} {
			claim := reconClaim(t, ctx, store, taskID, reconITOwner, 4, time.Minute)
			start, end := reconHeightRange(t, claim)
			if start != want[0] || end != want[1] {
				t.Fatalf("claim = [%d,%d], want [%d,%d] (contiguous from the persisted prefix)", start, end, want[0], want[1])
			}
			reconCommit(t, ctx, store, taskID, claim.AttemptID)
			if got, _ := reconPersistedThrough(t, ctx, pool, taskID); got != want[1] {
				t.Fatalf("pointer = %d, want %d", got, want[1])
			}
		}

		if _, err := store.ClaimScanAttempt(ctx, ClaimAttemptRequest{
			TaskID: taskID, Owner: reconITOwner, Span: 4, LeaseTTL: time.Minute,
		}); !errors.Is(err, ErrScopeExhausted) {
			t.Fatalf("claim past the scope end err = %v, want ErrScopeExhausted", err)
		}
		if n := reconCheckpointCount(t, ctx, pool, taskID); n != 4 {
			t.Errorf("checkpoint rows = %d, want 4 (one per committed batch)", n)
		}
		if n := reconAuditCount(t, ctx, pool, taskID, string(AuditActionClaim)); n != 4 {
			t.Errorf("claim audit rows = %d, want 4", n)
		}
		if n := reconProbeCount(t, ctx, pool); n != 4 {
			t.Errorf("persisted batches = %d, want 4 (no duplicate side effect)", n)
		}

		// A late repeat of an already-committed attempt is discarded, audited
		// and cannot append a second checkpoint or a duplicate batch.
		res, err := store.CommitScanBatch(ctx, CommitScanBatchRequest{
			TaskID: taskID, AttemptID: first.AttemptID,
			PersistResults: func(ctx context.Context, tx pgx.Tx) error {
				_, err := tx.Exec(ctx, `INSERT INTO `+reconITProbeTable+` (attempt_id) VALUES ($1::uuid)`, first.AttemptID)
				return err
			},
		})
		if !errors.Is(err, ErrAttemptDiscarded) || !res.Discarded || res.DiscardReason != DiscardReasonAttemptAlreadyDone {
			t.Fatalf("late repeat = %+v, err %v, want discarded/%s", res, err, DiscardReasonAttemptAlreadyDone)
		}
		if got, _ := reconPersistedThrough(t, ctx, pool, taskID); got != 112 {
			t.Errorf("pointer after the late repeat = %d, want 112 (never moves for a discarded attempt)", got)
		}
		if n := reconProbeCount(t, ctx, pool); n != 4 {
			t.Errorf("persisted batches after the late repeat = %d, want 4", n)
		}
	})

	t.Run("time_scope_claims_use_the_microsecond_step", func(t *testing.T) {
		taskID := uuid.NewString()
		base := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
		reconSeedTimeTask(t, ctx, pool, taskID, base, base.Add(3*time.Microsecond))

		first := reconClaim(t, ctx, store, taskID, reconITOwner, 2, time.Minute)
		if !first.RangeStart.Time.Equal(base) || !first.RangeEnd.Time.Equal(base.Add(time.Microsecond)) {
			t.Fatalf("first time claim = %v..%v, want %v..%v",
				first.RangeStart.Time, first.RangeEnd.Time, base, base.Add(time.Microsecond))
		}
		reconCommit(t, ctx, store, taskID, first.AttemptID)

		second := reconClaim(t, ctx, store, taskID, reconITOwner, 2, time.Minute)
		if !second.RangeStart.Time.Equal(base.Add(2*time.Microsecond)) ||
			!second.RangeEnd.Time.Equal(base.Add(3*time.Microsecond)) {
			t.Fatalf("second time claim = %v..%v, want the µs-clamped tail",
				second.RangeStart.Time, second.RangeEnd.Time)
		}
		reconCommit(t, ctx, store, taskID, second.AttemptID)
		if _, err := store.ClaimScanAttempt(ctx, ClaimAttemptRequest{
			TaskID: taskID, Owner: reconITOwner, Span: 2, LeaseTTL: time.Minute,
		}); !errors.Is(err, ErrScopeExhausted) {
			t.Fatalf("claim past the time scope end err = %v, want ErrScopeExhausted", err)
		}
	})

	t.Run("crash_recovery_abandons_stale_attempt_and_resumes", func(t *testing.T) {
		taskID := uuid.NewString()
		reconSeedHeightTask(t, ctx, pool, taskID, 200, 211, "running", "")

		stale := reconClaim(t, ctx, store, taskID, "worker-crashed", 4, 150*time.Millisecond)
		if start, end := reconHeightRange(t, stale); start != 200 || end != 203 {
			t.Fatalf("stale claim = [%d,%d], want [200,203]", start, end)
		}
		reconWaitForLeaseExpiry(t, ctx, pool, stale.AttemptID)

		rec, err := store.RecoverStaleAttempts(ctx, RecoverStaleAttemptsRequest{
			TaskID: taskID, Limit: 10, Actor: "it-recovery",
		})
		if err != nil {
			t.Fatalf("RecoverStaleAttempts: %v", err)
		}
		if len(rec.Abandoned) != 1 || rec.Abandoned[0] != stale.AttemptID {
			t.Fatalf("abandoned = %v, want [%s]", rec.Abandoned, stale.AttemptID)
		}
		if state := reconAttemptState(t, ctx, pool, stale.AttemptID); state != string(AttemptStateAbandoned) {
			t.Errorf("stale attempt state = %q, want %q", state, AttemptStateAbandoned)
		}
		if _, ok := reconPersistedThrough(t, ctx, pool, taskID); ok {
			t.Fatalf("crash recovery moved the pointer; it must stay untouched")
		}
		gaps := reconGapReasons(t, ctx, pool, taskID)
		if gaps[string(GapInterrupted)] != 1 {
			t.Errorf("gaps after crash recovery = %v, want one interrupted gap for the abandoned interval", gaps)
		}
		// Idempotent: a second pass abandons nothing and adds no gap.
		rec, err = store.RecoverStaleAttempts(ctx, RecoverStaleAttemptsRequest{TaskID: taskID, Limit: 10})
		if err != nil || len(rec.Abandoned) != 0 {
			t.Fatalf("second recovery = %+v (err %v), want no-op", rec, err)
		}
		if gaps := reconGapReasons(t, ctx, pool, taskID); gaps[string(GapInterrupted)] != 1 {
			t.Errorf("second recovery changed gaps: %v", gaps)
		}

		// The zombie submitter is discarded + audited; pointer and gaps move
		// nowhere.
		res, err := store.CommitScanBatch(ctx, CommitScanBatchRequest{
			TaskID: taskID, AttemptID: stale.AttemptID,
			PersistResults: func(ctx context.Context, tx pgx.Tx) error { return nil },
		})
		if !errors.Is(err, ErrAttemptDiscarded) || !res.Discarded || res.DiscardReason != DiscardReasonAttemptNotCurrent {
			t.Fatalf("zombie submit = %+v, err %v, want discarded/%s", res, err, DiscardReasonAttemptNotCurrent)
		}
		if _, ok := reconPersistedThrough(t, ctx, pool, taskID); ok {
			t.Errorf("pointer moved for a discarded zombie submit")
		}
		if n := reconAuditCount(t, ctx, pool, taskID, string(AuditActionRefuse)); n != 1 {
			t.Errorf("refuse audit rows = %d, want 1", n)
		}

		// No miss: the re-claim restarts exactly where the pointer stopped and
		// advances it once.
		reclaimed := reconClaim(t, ctx, store, taskID, reconITOwner, 4, time.Minute)
		if start, end := reconHeightRange(t, reclaimed); start != 200 || end != 203 {
			t.Fatalf("resumed claim = [%d,%d], want [200,203] (pointer unchanged by the crash)", start, end)
		}
		reconCommit(t, ctx, store, taskID, reclaimed.AttemptID)
		if got, _ := reconPersistedThrough(t, ctx, pool, taskID); got != 203 {
			t.Fatalf("pointer after resume = %d, want 203", got)
		}
		next := reconClaim(t, ctx, store, taskID, reconITOwner, 4, time.Minute)
		if start, _ := reconHeightRange(t, next); start != 204 {
			t.Fatalf("next claim start = %d, want 204 (no duplicate coverage)", start)
		}
	})

	t.Run("expired_lease_late_submit_is_discarded", func(t *testing.T) {
		taskID := uuid.NewString()
		reconSeedHeightTask(t, ctx, pool, taskID, 250, 255, "running", "")

		stale := reconClaim(t, ctx, store, taskID, "worker-slow", 4, 150*time.Millisecond)
		reconWaitForLeaseExpiry(t, ctx, pool, stale.AttemptID)

		res, err := store.CommitScanBatch(ctx, CommitScanBatchRequest{
			TaskID: taskID, AttemptID: stale.AttemptID,
			PersistResults: func(ctx context.Context, tx pgx.Tx) error { return nil },
		})
		if !errors.Is(err, ErrAttemptDiscarded) || !res.Discarded || res.DiscardReason != DiscardReasonLeaseExpired {
			t.Fatalf("expired-lease submit = %+v, err %v, want discarded/%s", res, err, DiscardReasonLeaseExpired)
		}
		if state := reconAttemptState(t, ctx, pool, stale.AttemptID); state != string(AttemptStateAbandoned) {
			t.Errorf("expired attempt state = %q, want %q", state, AttemptStateAbandoned)
		}
		if _, ok := reconPersistedThrough(t, ctx, pool, taskID); ok {
			t.Errorf("pointer advanced for an expired lease")
		}
		if gaps := reconGapReasons(t, ctx, pool, taskID); gaps[string(GapInterrupted)] != 1 {
			t.Errorf("gaps after expiry discard = %v, want one interrupted gap", gaps)
		}
		if n := reconAuditCount(t, ctx, pool, taskID, string(AuditActionRefuse)); n != 1 {
			t.Errorf("refuse audit rows = %d, want 1", n)
		}

		reclaimed := reconClaim(t, ctx, store, taskID, reconITOwner, 4, time.Minute)
		if start, end := reconHeightRange(t, reclaimed); start != 250 || end != 253 {
			t.Fatalf("resumed claim = [%d,%d], want [250,253]", start, end)
		}
		reconCommit(t, ctx, store, taskID, reclaimed.AttemptID)
		if got, _ := reconPersistedThrough(t, ctx, pool, taskID); got != 253 {
			t.Fatalf("pointer after resume = %d, want 253", got)
		}
	})

	t.Run("budget_exhaustion_suspends_with_visible_gaps_and_resumes_without_miss", func(t *testing.T) {
		limits := BudgetLimits{
			MaxConcurrency: 1, MaxSpanPerClaim: 4, MaxDuration: time.Minute,
			MaxPGRequests: 1, MaxRPCRequests: 1,
		}
		budget, err := NewBudget(limits)
		if err != nil {
			t.Fatalf("NewBudget: %v", err)
		}
		if err := budget.ConsumePG(ctx, 1); err != nil {
			t.Fatalf("first PG charge: %v", err)
		}
		if err := budget.ConsumePG(ctx, 1); !errors.Is(err, ErrBudgetExhausted) {
			t.Fatalf("second PG charge = %v, want ErrBudgetExhausted", err)
		}
		resource, detail, suspended := budget.Suspended()
		if !suspended || resource != ResourcePG || detail == "" {
			t.Fatalf("budget suspension = (%q, %q, %v), want the observable PG quota cause", resource, detail, suspended)
		}

		taskID := uuid.NewString()
		reconSeedHeightTask(t, ctx, pool, taskID, 300, 315, "running", "")
		inFlight := reconClaim(t, ctx, store, taskID, reconITOwner, 4, time.Minute)

		settled, err := store.SettleInFlight(ctx, SettleInFlightRequest{
			TaskID: taskID, Mode: SettleModePause, Limit: 1,
			Reason: "reconciliation budget exhausted", Actor: reconITOpsActor,
		})
		if err != nil {
			t.Fatalf("SettleInFlight: %v", err)
		}
		if len(settled.Settled) != 1 || settled.Settled[0] != inFlight.AttemptID {
			t.Fatalf("settled = %v, want [%s]", settled.Settled, inFlight.AttemptID)
		}
		if state := reconAttemptState(t, ctx, pool, inFlight.AttemptID); state != string(AttemptStateSuperseded) {
			t.Errorf("in-flight attempt state after settle = %q, want %q", state, AttemptStateSuperseded)
		}

		tr, err := store.TransitionTask(ctx, TaskTransitionRequest{
			TaskID: taskID, To: TaskStateSuspendedBudget,
			Reason: "pg request quota exhausted: " + detail, Actor: reconITOpsActor,
		})
		if err != nil {
			t.Fatalf("suspended_budget transition: %v", err)
		}
		if tr.From != TaskStateRunning || tr.To != TaskStateSuspendedBudget {
			t.Fatalf("transition = %s -> %s, want running -> suspended_budget", tr.From, tr.To)
		}
		if state, reason := reconTaskState(t, ctx, store, taskID); state != TaskStateSuspendedBudget || reason == "" {
			t.Errorf("task state = %s (pause_reason %q), want suspended_budget with an observable reason", state, reason)
		}
		if tr.Checkpoint != nil {
			t.Errorf("suspended task reports checkpoint head %+v before any commit; suspension must not fake coverage", tr.Checkpoint)
		}
		gaps := reconGapReasons(t, ctx, pool, taskID)
		if gaps[string(GapPaused)] != 1 || gaps[string(GapBudgetExhausted)] != 1 {
			t.Errorf("gaps = %v, want paused=1 (settled in-flight) and budget_exhausted=1 (uncovered remainder)", gaps)
		}
		if _, ok := reconPersistedThrough(t, ctx, pool, taskID); ok {
			t.Errorf("pointer advanced while budget-suspended")
		}
		if _, err := store.ClaimScanAttempt(ctx, ClaimAttemptRequest{
			TaskID: taskID, Owner: reconITOwner, Span: 4, LeaseTTL: time.Minute,
		}); !errors.Is(err, ErrTaskNotRunning) {
			t.Fatalf("claim on a suspended task err = %v, want ErrTaskNotRunning", err)
		}
		// Suspended/budget is not a closable conclusion: no edge to done.
		if _, err := store.TransitionTask(ctx, TaskTransitionRequest{
			TaskID: taskID, To: TaskStateDone, Reason: "force close", Actor: reconITOpsActor,
		}); !errors.Is(err, ErrIllegalTaskTransition) {
			t.Fatalf("suspended_budget -> done err = %v, want ErrIllegalTaskTransition (never 'fully consistent')", err)
		}

		// Resume: no interval is missed; the settled interval is re-claimed.
		if _, err := store.TransitionTask(ctx, TaskTransitionRequest{
			TaskID: taskID, To: TaskStateRunning, Reason: "quota restored", Actor: reconITOpsActor,
		}); err != nil {
			t.Fatalf("resume transition: %v", err)
		}
		reclaimed := reconClaim(t, ctx, store, taskID, reconITOwner, 4, time.Minute)
		if start, end := reconHeightRange(t, reclaimed); start != 300 || end != 303 {
			t.Fatalf("resumed claim = [%d,%d], want [300,303]", start, end)
		}
		reconCommit(t, ctx, store, taskID, reclaimed.AttemptID)
		if got, _ := reconPersistedThrough(t, ctx, pool, taskID); got != 303 {
			t.Fatalf("pointer after resume = %d, want 303", got)
		}

		// The settled attempt stays discarded and never rewrites the pointer.
		late, err := store.CommitScanBatch(ctx, CommitScanBatchRequest{
			TaskID: taskID, AttemptID: inFlight.AttemptID,
			PersistResults: func(ctx context.Context, tx pgx.Tx) error { return nil },
		})
		if !errors.Is(err, ErrAttemptDiscarded) || !late.Discarded || late.DiscardReason != DiscardReasonAttemptNotCurrent {
			t.Fatalf("settled attempt submit = %+v, err %v, want discarded/%s", late, err, DiscardReasonAttemptNotCurrent)
		}
		if got, _ := reconPersistedThrough(t, ctx, pool, taskID); got != 303 {
			t.Errorf("pointer after the settled-attempt submit = %d, want 303", got)
		}
	})

	t.Run("done_requires_the_pointer_at_the_scope_end", func(t *testing.T) {
		taskID := uuid.NewString()
		reconSeedHeightTask(t, ctx, pool, taskID, 420, 423, "running", "")

		partial := reconClaim(t, ctx, store, taskID, reconITOwner, 2, time.Minute)
		if start, end := reconHeightRange(t, partial); start != 420 || end != 421 {
			t.Fatalf("partial claim = [%d,%d], want [420,421]", start, end)
		}
		reconCommit(t, ctx, store, taskID, partial.AttemptID)
		if _, err := store.TransitionTask(ctx, TaskTransitionRequest{
			TaskID: taskID, To: TaskStateDone, Reason: "close early", Actor: reconITOpsActor,
		}); !errors.Is(err, ErrTaskNotComplete) {
			t.Fatalf("done with a short pointer err = %v, want ErrTaskNotComplete", err)
		}
		if state, _ := reconTaskState(t, ctx, store, taskID); state != TaskStateRunning {
			t.Fatalf("state after refused close = %s, want running", state)
		}

		tail := reconClaim(t, ctx, store, taskID, reconITOwner, 2, time.Minute)
		if start, end := reconHeightRange(t, tail); start != 422 || end != 423 {
			t.Fatalf("tail claim = [%d,%d], want [422,423]", start, end)
		}
		reconCommit(t, ctx, store, taskID, tail.AttemptID)
		tr, err := store.TransitionTask(ctx, TaskTransitionRequest{
			TaskID: taskID, To: TaskStateDone, Reason: "scope fully persisted", Actor: reconITOpsActor,
		})
		if err != nil {
			t.Fatalf("done after full coverage: %v", err)
		}
		if tr.Checkpoint == nil || tr.Checkpoint.ResultPersistedThrough.Height != 423 {
			t.Fatalf("done checkpoint = %+v, want persisted through 423", tr.Checkpoint)
		}
		if state, _ := reconTaskState(t, ctx, store, taskID); state != TaskStateDone {
			t.Fatalf("state = %s, want done", state)
		}
		// Terminal: no claim and no further transition.
		if _, err := store.ClaimScanAttempt(ctx, ClaimAttemptRequest{
			TaskID: taskID, Owner: reconITOwner, Span: 2, LeaseTTL: time.Minute,
		}); !errors.Is(err, ErrTaskNotRunning) {
			t.Errorf("claim on a done task err = %v, want ErrTaskNotRunning", err)
		}
		if _, err := store.TransitionTask(ctx, TaskTransitionRequest{
			TaskID: taskID, To: TaskStateCancelled, Reason: "after done", Actor: reconITOpsActor,
		}); !errors.Is(err, ErrIllegalTaskTransition) {
			t.Errorf("done -> cancelled err = %v, want ErrIllegalTaskTransition", err)
		}
	})
}

func TestIntegrationUpstreamUnconnectedNeverConsistent(t *testing.T) {
	ctx, pool, store := reconIT(t)

	const unconnectedSource = `{"withdrawal":{"source":"receipt-stream-not-wired","connected":false}}`

	t.Run("unconnected_scope_classifies_pending_and_never_closes", func(t *testing.T) {
		taskID := uuid.NewString()
		reconSeedHeightTask(t, ctx, pool, taskID, 500, 503, "running", unconnectedSource)

		// The connectedness input is persisted and readable (data-model §1.1).
		var connected bool
		if err := pool.QueryRow(ctx, `
			SELECT (upstream_receipt_source->'withdrawal'->>'connected')::boolean
			FROM recon_task WHERE task_id = $1::uuid`, taskID).Scan(&connected); err != nil {
			t.Fatalf("read upstream receipt source: %v", err)
		}
		if connected {
			t.Fatalf("seed lost connected=false")
		}

		// The compare loop's verdict for a clean, fully-scanned range whose
		// only missing input is the unconnected upstream receipt must be
		// incomplete/pending: no ticket, no consistent conclusion, and the
		// upstream-credit dimension stays unverified (never an external-credit
		// claim; FR-006/Q4).
		scope, err := HeightIdentityScope(reconITChainID, 500, 503, BusinessWithdrawal)
		if err != nil {
			t.Fatalf("HeightIdentityScope: %v", err)
		}
		evidenceAt := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
		verdict := Classify(Observation{
			Scope:        scope,
			BusinessKey:  BusinessKey{Kind: BusinessKeyRequestID, Value: "unconnected-1"},
			BusinessType: BusinessWithdrawal,
			Chain:        PartyObservation{Status: PartyPresent, Content: []byte(`{"block":500,"hash":"0xabc"}`)},
			PG:           PartyObservation{Status: PartyPresent, Content: []byte(`{"row":"present"}`)},
			Event:        PartyObservation{Status: PartyPresent, Content: []byte(`{"delivery":"present"}`)},
			Coverage: Coverage{
				ScanComplete: true, OpenGaps: 0,
				EvidenceAt: evidenceAt, Now: evidenceAt.Add(time.Second),
				FreshnessTolerance: time.Minute,
			},
			Upstream: UpstreamReceiptSource{Source: "receipt-stream-not-wired", Connected: false},
			Version: VersionDomain{
				BlockNumber: 500, BlockHash: "0xabc", EvidenceAt: evidenceAt,
			},
		})
		if verdict.Conclusion != ConclusionPending || verdict.Category != CategoryIncomplete {
			t.Fatalf("unconnected verdict = conclusion %q category %q, want pending/incomplete", verdict.Conclusion, verdict.Category)
		}
		if verdict.Ticket {
			t.Fatalf("unconnected verdict mints a ticket; incomplete evidence must stay alert-only")
		}
		if verdict.ExternalCredit != ExternalCreditUnverified {
			t.Fatalf("external credit = %q, want %q (never an external-credit claim)", verdict.ExternalCredit, ExternalCreditUnverified)
		}
		if verdict.FullyConsistent() {
			t.Fatalf("unconnected verdict claims fully consistent")
		}
		if verdict.Reason != ReasonUpstreamUnconnected {
			t.Fatalf("verdict reason = %q, want %q", verdict.Reason, ReasonUpstreamUnconnected)
		}

		// The scan persists that incomplete classification as the visible
		// marker in the same transaction that advances the checkpoint
		// (data-model §5 ordering), and the scope stays unclosable afterwards.
		claim := reconClaim(t, ctx, store, taskID, reconITOwner, 4, time.Minute)
		start, end := reconHeightRange(t, claim)
		if start != 500 || end != 503 {
			t.Fatalf("claim = [%d,%d], want [500,503]", start, end)
		}
		res, err := store.CommitScanBatch(ctx, CommitScanBatchRequest{
			TaskID: taskID, AttemptID: claim.AttemptID,
			PersistResults: func(ctx context.Context, tx pgx.Tx) error {
				if _, err := tx.Exec(ctx,
					`INSERT INTO `+reconITProbeTable+` (attempt_id) VALUES ($1::uuid)`, claim.AttemptID); err != nil {
					return err
				}
				_, err := tx.Exec(ctx, `
					INSERT INTO recon_gap (task_id, range_start, range_end, reason)
					VALUES ($1::uuid, $2, $3, 'upstream_unconnected')`, taskID, start, end)
				return err
			},
		})
		if err != nil || !res.Committed {
			t.Fatalf("commit with the incomplete marker = %+v, err %v, want committed", res, err)
		}
		if got, _ := reconPersistedThrough(t, ctx, pool, taskID); got != 503 {
			t.Fatalf("pointer = %d, want 503 (results persisted)", got)
		}
		if gaps := reconGapReasons(t, ctx, pool, taskID); gaps[string(GapUpstreamUnconnected)] != 1 {
			t.Fatalf("gaps = %v, want one upstream_unconnected incomplete marker", gaps)
		}

		// The pointer is complete but the open gap keeps the task unclosable:
		// no "fully consistent" outcome can be rendered over it.
		if _, err := store.TransitionTask(ctx, TaskTransitionRequest{
			TaskID: taskID, To: TaskStateDone,
			Reason: "all scoped ranges processed", Actor: reconITOpsActor,
		}); !errors.Is(err, ErrTaskNotComplete) {
			t.Fatalf("done over an unconnected scope err = %v, want ErrTaskNotComplete: incomplete evidence must never close as fully consistent", err)
		}
		if state, _ := reconTaskState(t, ctx, store, taskID); state != TaskStateRunning {
			t.Fatalf("state after the unconnected scan = %s, want running (pending, never closed)", state)
		}
		if n := reconAuditCount(t, ctx, pool, taskID, string(AuditActionRefuse)); n != 1 {
			t.Errorf("refuse audit rows = %d, want 1 (the refused close is audited)", n)
		}

		// No 014 path may record a consistency verdict or an external-credit
		// claim from unconnected evidence.
		var consistent int64
		if err := pool.QueryRow(ctx, `SELECT count(*)::bigint FROM reverify WHERE verdict = 'consistent'`).Scan(&consistent); err != nil {
			t.Fatalf("count consistent reverify rows: %v", err)
		}
		if consistent != 0 {
			t.Errorf("consistent reverify rows = %d, want 0 for unconnected evidence", consistent)
		}
		var dispositions int64
		if err := pool.QueryRow(ctx, `SELECT count(*)::bigint FROM disposition`).Scan(&dispositions); err != nil {
			t.Fatalf("count disposition rows: %v", err)
		}
		if dispositions != 0 {
			t.Errorf("disposition rows = %d, want 0 (never an external-credit claim)", dispositions)
		}
		var occurrences int64
		if err := pool.QueryRow(ctx,
			`SELECT count(*)::bigint FROM discrepancy_occurrence WHERE scan_task_id = $1::uuid`, taskID).Scan(&occurrences); err != nil {
			t.Fatalf("count occurrences of %s: %v", taskID, err)
		}
		if occurrences != 0 {
			t.Errorf("occurrences from the unconnected scan = %d, want 0", occurrences)
		}
	})

	t.Run("incomplete_evidence_cannot_mint_a_ticketable_identity", func(t *testing.T) {
		if CategoryIncomplete.Ticketable() {
			t.Fatalf("CategoryIncomplete.Ticketable() = true; incomplete must stay alert-only")
		}
		scope, err := HeightIdentityScope(reconITChainID, 500, 503, BusinessWithdrawal)
		if err != nil {
			t.Fatalf("HeightIdentityScope: %v", err)
		}
		// The upstream party is unavailable (connected=false), so the
		// three-party snapshot cannot be hashed; conservative handling is
		// ErrInsufficientEvidence, never a ticket.
		_, err = BuildIdentity(IdentityInput{
			Scope:    scope,
			Category: CategoryIncomplete,
			BusinessKey: BusinessKey{
				Kind: BusinessKeyRequestID, Value: "unconnected-1",
			},
			Snapshot: Snapshot{
				Chain: []byte(`{"block":500}`),
				PG:    []byte(`{"row":"present"}`),
				// Event/upstream party absent.
			},
			VersionDomain: VersionDomain{
				BlockNumber: 500, BlockHash: "0xabc",
				EvidenceAt: time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC),
			},
		})
		if !errors.Is(err, ErrInsufficientEvidence) {
			t.Fatalf("BuildIdentity with the unconnected party missing err = %v, want ErrInsufficientEvidence", err)
		}
	})
}
