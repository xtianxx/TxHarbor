//go:build integration

// recovery_integration_test.go is the T025 integration layer for US3
// (quickstart §7; contracts/task-lifecycle.md Claim–Execute–Commit F3;
// data-model.md §5/§5.1):
//
//   - crash recovery no-miss/no-dup: an aborted commit transaction leaves no
//     result, no checkpoint and no moved pointer; the crashed claim surfaces
//     through its expired lease, recovery abandons it with the abandoned
//     interval visible as a gap and the pointer untouched, and the resume
//     claims exactly the first uncovered position — covered ranges are never
//     re-reported and every persisted batch lands exactly once;
//   - the overlap counterexample (T025): two concurrent invocations claim the
//     same task/interval; the partial unique index (WHERE state='claimed')
//     admits exactly one claimed attempt, the loser gets ErrClaimTaken (its
//     transaction, including the claim audit, rolled back) and retries the
//     next interval after the winner commits;
//   - the late submitter: a superseded (paused), abandoned (cancelled/expired
//     lease) attempt is refused before its results are persisted, discarded +
//     audited, and the pointer never moves;
//   - no DB transaction is held across slow execution: while the first
//     invocation is parked in its out-of-transaction execution phase, an
//     independent session takes the recon_task row lock (a second claim
//     returns the deterministic ErrClaimTaken instead of blocking, and a
//     recovery pass completes) and the next interval claims straight after
//     the commit.
//
// PostgreSQL comes from testcontainers. When no Docker provider is healthy the
// package reports NOT RUN (t.Skip), never a pass.
package reconciliation

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// recoveryITRefusal is one refuse audit row observed by the recovery tests.
type recoveryITRefusal struct {
	Actor  string
	Reason string
	Result string
}

// recoveryITRefusals lists the refuse audit rows written for one task, in
// insertion order. Discarded late submitters MUST leave exactly these rows and
// nothing else.
func recoveryITRefusals(t *testing.T, ctx context.Context, pool *pgxpool.Pool, taskID string) []recoveryITRefusal {
	t.Helper()
	rows, err := pool.Query(ctx, `
		SELECT actor, reason, result FROM recon_audit
		WHERE action = 'refuse' AND target->>'task_id' = $1
		ORDER BY audit_id`, taskID)
	if err != nil {
		t.Fatalf("read refuse audits of %s: %v", taskID, err)
	}
	defer rows.Close()
	var out []recoveryITRefusal
	for rows.Next() {
		var rec recoveryITRefusal
		if err := rows.Scan(&rec.Actor, &rec.Reason, &rec.Result); err != nil {
			t.Fatalf("scan refuse audit of %s: %v", taskID, err)
		}
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("refuse audit rows of %s: %v", taskID, err)
	}
	return out
}

// recoveryITProbeRows counts the persisted batches of one attempt (the probe
// table row is written by the commit transaction's PersistResults callback).
func recoveryITProbeRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, attemptID string) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(ctx,
		`SELECT count(*)::bigint FROM `+reconITProbeTable+` WHERE attempt_id = $1::uuid`,
		attemptID).Scan(&n); err != nil {
		t.Fatalf("count persisted batches of attempt %s: %v", attemptID, err)
	}
	return n
}

// recoveryITPersistedBatches counts the persisted batches of one task across
// its attempts: one row per committed interval, none for refused or crashed
// submissions.
func recoveryITPersistedBatches(t *testing.T, ctx context.Context, pool *pgxpool.Pool, taskID string) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(ctx, `
		SELECT count(*)::bigint
		FROM `+reconITProbeTable+` p
		JOIN recon_scan_attempt a ON a.attempt_id = p.attempt_id
		WHERE a.task_id = $1::uuid`, taskID).Scan(&n); err != nil {
		t.Fatalf("count persisted batches of task %s: %v", taskID, err)
	}
	return n
}

// recoveryITClaimedAttemptCount counts the claimed attempt rows of one task.
// The partial unique index admits at most one.
func recoveryITClaimedAttemptCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, taskID string) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(ctx, `
		SELECT count(*)::bigint FROM recon_scan_attempt
		WHERE task_id = $1::uuid AND state = 'claimed'`, taskID).Scan(&n); err != nil {
		t.Fatalf("count claimed attempts of %s: %v", taskID, err)
	}
	return n
}

// recoveryITClaimedAttemptOwner reads the owner of the task's claimed attempt.
func recoveryITClaimedAttemptOwner(t *testing.T, ctx context.Context, pool *pgxpool.Pool, taskID string) string {
	t.Helper()
	var owner string
	if err := pool.QueryRow(ctx, `
		SELECT owner FROM recon_scan_attempt
		WHERE task_id = $1::uuid AND state = 'claimed'`, taskID).Scan(&owner); err != nil {
		t.Fatalf("read claimed attempt owner of %s: %v", taskID, err)
	}
	return owner
}

// recoveryITGapRanges lists the [start, end] ranges of one gap reason.
func recoveryITGapRanges(t *testing.T, ctx context.Context, pool *pgxpool.Pool, taskID, reason string) [][2]int64 {
	t.Helper()
	rows, err := pool.Query(ctx, `
		SELECT range_start, range_end FROM recon_gap
		WHERE task_id = $1::uuid AND reason = $2
		ORDER BY gap_id`, taskID, reason)
	if err != nil {
		t.Fatalf("read %s gaps of %s: %v", reason, taskID, err)
	}
	defer rows.Close()
	var out [][2]int64
	for rows.Next() {
		var r [2]int64
		if err := rows.Scan(&r[0], &r[1]); err != nil {
			t.Fatalf("scan %s gap of %s: %v", reason, taskID, err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("%s gap rows of %s: %v", reason, taskID, err)
	}
	return out
}

func TestIntegrationRecoveryCrashResumeAndOverlap(t *testing.T) {
	ctx, pool, store := reconIT(t)

	t.Run("crash_recovery_no_miss_no_dup", func(t *testing.T) {
		taskID := uuid.NewString()
		reconSeedHeightTask(t, ctx, pool, taskID, 1000, 1012, "running", "")

		// Batch 1 lands atomically: probe row, checkpoint row and attempt
		// transition in one transaction, so the pointer is exactly the batch's
		// interval end.
		first := reconClaim(t, ctx, store, taskID, reconITOwner, 4, time.Minute)
		if start, end := reconHeightRange(t, first); start != 1000 || end != 1003 {
			t.Fatalf("first claim = [%d,%d], want [1000,1003]", start, end)
		}
		reconCommit(t, ctx, store, taskID, first.AttemptID)
		if got, ok := reconPersistedThrough(t, ctx, pool, taskID); !ok || got != 1003 {
			t.Fatalf("pointer after batch 1 = %d (present %v), want 1003", got, ok)
		}
		head, err := store.CheckpointHead(ctx, taskID)
		if err != nil || head == nil {
			t.Fatalf("CheckpointHead after batch 1 = %+v, err %v, want one checkpoint", head, err)
		}
		if head.Seq != 1 || head.CoveredThrough.Height != 1003 || head.ResultPersistedThrough.Height != 1003 {
			t.Fatalf("checkpoint head = seq %d covered %d persisted %d, want 1/1003/1003",
				head.Seq, head.CoveredThrough.Height, head.ResultPersistedThrough.Height)
		}
		if n := recoveryITProbeRows(t, ctx, pool, first.AttemptID); n != 1 {
			t.Fatalf("persisted batches of batch 1 = %d, want 1", n)
		}

		// Batch 2 claims the next contiguous interval and then crashes: the
		// persistence callback writes a partial result and fails, so the whole
		// commit transaction must roll back — results, checkpoint and pointer
		// stay untouched and the attempt stays claimed for recovery.
		crashed := reconClaim(t, ctx, store, taskID, "worker-crash", 4, 3*time.Second)
		if start, end := reconHeightRange(t, crashed); start != 1004 || end != 1007 {
			t.Fatalf("crashed claim = [%d,%d], want [1004,1007] (contiguous after the pointer)", start, end)
		}
		res, err := store.CommitScanBatch(ctx, CommitScanBatchRequest{
			TaskID: taskID, AttemptID: crashed.AttemptID,
			PersistResults: func(ctx context.Context, tx pgx.Tx) error {
				if _, err := tx.Exec(ctx,
					`INSERT INTO `+reconITProbeTable+` (attempt_id) VALUES ($1::uuid)`,
					crashed.AttemptID); err != nil {
					return err
				}
				return errors.New("injected crash before the commit transaction landed")
			},
		})
		if err == nil || res.Committed || res.Discarded {
			t.Fatalf("aborted commit = %+v, err %v, want a plain failure (no commit, no discard)", res, err)
		}
		if n := recoveryITProbeRows(t, ctx, pool, crashed.AttemptID); n != 0 {
			t.Errorf("partial results of the aborted commit survived the rollback: %d probe row(s)", n)
		}
		if n := reconCheckpointCount(t, ctx, pool, taskID); n != 1 {
			t.Errorf("checkpoint rows after the aborted commit = %d, want 1", n)
		}
		if got, ok := reconPersistedThrough(t, ctx, pool, taskID); !ok || got != 1003 {
			t.Errorf("pointer after the aborted commit = %d (present %v), want 1003 (never past unpersisted ranges)", got, ok)
		}
		if head, err := store.CheckpointHead(ctx, taskID); err != nil || head == nil ||
			head.ResultPersistedThrough.Height != 1003 {
			t.Errorf("checkpoint head after the aborted commit = %+v, err %v, want persisted through 1003", head, err)
		}
		if state := reconAttemptState(t, ctx, pool, crashed.AttemptID); state != string(AttemptStateClaimed) {
			t.Errorf("attempt state after the aborted commit = %q, want %q (rolled back, re-executable)",
				state, AttemptStateClaimed)
		}
		if refusals := recoveryITRefusals(t, ctx, pool, taskID); len(refusals) != 0 {
			t.Errorf("refuse audits after the aborted commit = %+v, want none (a rolled-back failure is not a discard)", refusals)
		}

		// The crash surfaces through the expired lease: recovery abandons the
		// attempt, records its interval as an interrupted gap and never moves
		// the pointer.
		reconWaitForLeaseExpiry(t, ctx, pool, crashed.AttemptID)
		rec, err := store.RecoverStaleAttempts(ctx, RecoverStaleAttemptsRequest{
			TaskID: taskID, Limit: 10, Actor: "it-recovery",
		})
		if err != nil || len(rec.Abandoned) != 1 || rec.Abandoned[0] != crashed.AttemptID {
			t.Fatalf("recovery = %+v, err %v, want the crashed attempt abandoned", rec, err)
		}
		if state := reconAttemptState(t, ctx, pool, crashed.AttemptID); state != string(AttemptStateAbandoned) {
			t.Errorf("recovered attempt state = %q, want %q", state, AttemptStateAbandoned)
		}
		if n := reconCheckpointCount(t, ctx, pool, taskID); n != 1 {
			t.Errorf("checkpoint rows after recovery = %d, want 1 (recovery never moves the pointer)", n)
		}
		if got, ok := reconPersistedThrough(t, ctx, pool, taskID); !ok || got != 1003 {
			t.Errorf("pointer after recovery = %d (present %v), want 1003", got, ok)
		}
		if ranges := recoveryITGapRanges(t, ctx, pool, taskID, string(GapInterrupted)); len(ranges) != 1 ||
			ranges[0] != [2]int64{1004, 1007} {
			t.Errorf("interrupted gaps after recovery = %v, want the abandoned [1004,1007] (uncovered)", ranges)
		}

		// The zombie submit of the crashed attempt is discarded + audited and
		// still cannot append a batch or move the pointer.
		zombie, err := store.CommitScanBatch(ctx, CommitScanBatchRequest{
			TaskID: taskID, AttemptID: crashed.AttemptID,
			PersistResults: func(ctx context.Context, tx pgx.Tx) error {
				_, err := tx.Exec(ctx,
					`INSERT INTO `+reconITProbeTable+` (attempt_id) VALUES ($1::uuid)`,
					crashed.AttemptID)
				return err
			},
		})
		if !errors.Is(err, ErrAttemptDiscarded) || !zombie.Discarded ||
			zombie.DiscardReason != DiscardReasonAttemptNotCurrent {
			t.Fatalf("zombie submit = %+v, err %v, want discarded/%s", zombie, err, DiscardReasonAttemptNotCurrent)
		}
		if n := recoveryITProbeRows(t, ctx, pool, crashed.AttemptID); n != 0 {
			t.Errorf("zombie submit persisted %d batch row(s), want 0", n)
		}
		if got, _ := reconPersistedThrough(t, ctx, pool, taskID); got != 1003 {
			t.Errorf("pointer after the zombie submit = %d, want 1003", got)
		}
		if refusals := recoveryITRefusals(t, ctx, pool, taskID); len(refusals) != 1 ||
			refusals[0].Actor != "worker-crash" ||
			refusals[0].Reason != DiscardReasonAttemptNotCurrent ||
			refusals[0].Result != "discarded" {
			t.Errorf("refuse audits after the zombie submit = %+v, want one %s/discarded by worker-crash",
				refusals, DiscardReasonAttemptNotCurrent)
		}

		// Resume from the pointer: no miss — the resumed claim starts exactly
		// at the first uncovered position, never re-covering 1000..1003 — and
		// no duplicate side effect: one persisted batch per committed interval.
		resumed := reconClaim(t, ctx, store, taskID, reconITOwner, 4, time.Minute)
		if start, end := reconHeightRange(t, resumed); start != 1004 || end != 1007 {
			t.Fatalf("resumed claim = [%d,%d], want [1004,1007] (no re-report of the covered prefix)", start, end)
		}
		reconCommit(t, ctx, store, taskID, resumed.AttemptID)
		if n := recoveryITProbeRows(t, ctx, pool, resumed.AttemptID); n != 1 {
			t.Errorf("persisted batches of the resumed interval = %d, want 1", n)
		}
		if got, _ := reconPersistedThrough(t, ctx, pool, taskID); got != 1007 {
			t.Fatalf("pointer after the resume = %d, want 1007", got)
		}

		for _, want := range [][2]int64{{1008, 1011}, {1012, 1012}} {
			claim := reconClaim(t, ctx, store, taskID, reconITOwner, 4, time.Minute)
			start, end := reconHeightRange(t, claim)
			if start != want[0] || end != want[1] {
				t.Fatalf("claim = [%d,%d], want [%d,%d] (contiguous from the pointer, no gap re-report)",
					start, end, want[0], want[1])
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
		if n := recoveryITPersistedBatches(t, ctx, pool, taskID); n != 4 {
			t.Errorf("persisted batches = %d, want 4 (one per committed interval, none duplicated)", n)
		}
		if n := reconCheckpointCount(t, ctx, pool, taskID); n != 4 {
			t.Errorf("checkpoint rows = %d, want 4", n)
		}
	})

	t.Run("concurrent_overlap_claims_only_one_lands", func(t *testing.T) {
		taskID := uuid.NewString()
		reconSeedHeightTask(t, ctx, pool, taskID, 1100, 1111, "running", "")

		type claimOutcome struct {
			result ClaimAttemptResult
			err    error
		}
		start := make(chan struct{})
		outcomes := make(chan claimOutcome, 2)
		var wg sync.WaitGroup
		for _, owner := range []string{"worker-1", "worker-2"} {
			owner := owner
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				result, err := store.ClaimScanAttempt(ctx, ClaimAttemptRequest{
					TaskID: taskID, Owner: owner, Span: 4, LeaseTTL: time.Minute,
				})
				outcomes <- claimOutcome{result: result, err: err}
			}()
		}
		close(start)
		wg.Wait()
		close(outcomes)

		var winners []ClaimAttemptResult
		losers := 0
		for out := range outcomes {
			switch {
			case out.err == nil:
				winners = append(winners, out.result)
			case errors.Is(out.err, ErrClaimTaken):
				losers++
			default:
				t.Fatalf("concurrent claim err = %v, want nil or ErrClaimTaken", out.err)
			}
		}
		if len(winners) != 1 || losers != 1 {
			t.Fatalf("concurrent claims = %d winner(s), %d ErrClaimTaken loser(s), want exactly 1/1", len(winners), losers)
		}

		// Both invocations computed the same interval from the unmoved pointer;
		// only one claimed attempt row may land (recon_scan_attempt_claimed_uniq,
		// the partial unique index WHERE state='claimed'), and the loser's
		// transaction — including its claim audit — rolled back.
		winner := winners[0]
		if start, end := reconHeightRange(t, winner); start != 1100 || end != 1103 {
			t.Fatalf("winning claim = [%d,%d], want [1100,1103]", start, end)
		}
		if got, ok := reconPersistedThrough(t, ctx, pool, taskID); ok {
			t.Fatalf("pointer moved during the claim race: %d", got)
		}
		if n := recoveryITClaimedAttemptCount(t, ctx, pool, taskID); n != 1 {
			t.Fatalf("claimed attempts = %d, want 1 (only one claimed attempt may land)", n)
		}
		if owner := recoveryITClaimedAttemptOwner(t, ctx, pool, taskID); owner != "worker-1" && owner != "worker-2" {
			t.Fatalf("claimed attempt owner = %q, want one of the two concurrent invocations", owner)
		}
		if n := reconAttemptCount(t, ctx, pool, taskID); n != 1 {
			t.Errorf("attempt rows = %d, want 1 (the loser wrote nothing)", n)
		}
		if n := reconAuditCount(t, ctx, pool, taskID, string(AuditActionClaim)); n != 1 {
			t.Errorf("claim audit rows = %d, want 1 (the loser's audit rolled back)", n)
		}

		// The loser retries the next interval after the winner commits: the
		// race never moved the pointer, so the contested range is the only one
		// re-executed; the retry claims straight after it.
		reconCommit(t, ctx, store, taskID, winner.AttemptID)
		if got, _ := reconPersistedThrough(t, ctx, pool, taskID); got != 1103 {
			t.Fatalf("pointer after the winning commit = %d, want 1103", got)
		}
		retry := reconClaim(t, ctx, store, taskID, "worker-loser", 4, time.Minute)
		if start, end := reconHeightRange(t, retry); start != 1104 || end != 1107 {
			t.Fatalf("retry claim = [%d,%d], want [1104,1107] (the next interval)", start, end)
		}
		reconCommit(t, ctx, store, taskID, retry.AttemptID)
		if got, _ := reconPersistedThrough(t, ctx, pool, taskID); got != 1107 {
			t.Fatalf("pointer after the retry = %d, want 1107", got)
		}
	})

	t.Run("late_submitters_are_discarded_and_audited", func(t *testing.T) {
		t.Run("paused_task_supersedes_the_attempt", func(t *testing.T) {
			taskID := uuid.NewString()
			reconSeedHeightTask(t, ctx, pool, taskID, 1200, 1211, "running", "")

			attempt := reconClaim(t, ctx, store, taskID, "worker-paused", 4, time.Minute)
			if _, err := store.TransitionTask(ctx, TaskTransitionRequest{
				TaskID: taskID, To: TaskStatePaused, Reason: "operator pause", Actor: reconITOpsActor,
			}); err != nil {
				t.Fatalf("pause transition: %v", err)
			}

			res, err := store.CommitScanBatch(ctx, CommitScanBatchRequest{
				TaskID: taskID, AttemptID: attempt.AttemptID,
				PersistResults: func(ctx context.Context, tx pgx.Tx) error {
					_, err := tx.Exec(ctx,
						`INSERT INTO `+reconITProbeTable+` (attempt_id) VALUES ($1::uuid)`, attempt.AttemptID)
					return err
				},
			})
			if !errors.Is(err, ErrAttemptDiscarded) || !res.Discarded ||
				res.DiscardReason != DiscardReasonTaskNotRunning {
				t.Fatalf("paused late submit = %+v, err %v, want discarded/%s", res, err, DiscardReasonTaskNotRunning)
			}
			if state := reconAttemptState(t, ctx, pool, attempt.AttemptID); state != string(AttemptStateSuperseded) {
				t.Errorf("paused attempt state = %q, want %q", state, AttemptStateSuperseded)
			}
			if n := recoveryITProbeRows(t, ctx, pool, attempt.AttemptID); n != 0 {
				t.Errorf("paused late submit persisted %d batch row(s), want 0", n)
			}
			if got, ok := reconPersistedThrough(t, ctx, pool, taskID); ok {
				t.Errorf("pointer moved for a paused late submit: %d", got)
			}
			// The pause gap covers the uncovered remainder; the discarded
			// attempt's interval stays visible as its own paused gap.
			if gaps := reconGapReasons(t, ctx, pool, taskID); gaps[string(GapPaused)] != 2 {
				t.Errorf("gaps = %v, want paused=2 (pause remainder + discarded interval)", gaps)
			}
			if refusals := recoveryITRefusals(t, ctx, pool, taskID); len(refusals) != 1 ||
				refusals[0].Actor != "worker-paused" ||
				refusals[0].Reason != DiscardReasonTaskNotRunning ||
				refusals[0].Result != "discarded" {
				t.Errorf("refuse audits = %+v, want one %s/discarded by worker-paused",
					refusals, DiscardReasonTaskNotRunning)
			}
			if _, err := store.ClaimScanAttempt(ctx, ClaimAttemptRequest{
				TaskID: taskID, Owner: reconITOwner, Span: 4, LeaseTTL: time.Minute,
			}); !errors.Is(err, ErrTaskNotRunning) {
				t.Errorf("claim on the paused task err = %v, want ErrTaskNotRunning", err)
			}
		})

		t.Run("cancelled_task_abandons_the_attempt", func(t *testing.T) {
			taskID := uuid.NewString()
			reconSeedHeightTask(t, ctx, pool, taskID, 1300, 1311, "running", "")

			attempt := reconClaim(t, ctx, store, taskID, "worker-cancelled", 4, time.Minute)
			if _, err := store.TransitionTask(ctx, TaskTransitionRequest{
				TaskID: taskID, To: TaskStateCancelled, Reason: "operator cancel", Actor: reconITOpsActor,
			}); err != nil {
				t.Fatalf("cancel transition: %v", err)
			}

			res, err := store.CommitScanBatch(ctx, CommitScanBatchRequest{
				TaskID: taskID, AttemptID: attempt.AttemptID,
				PersistResults: func(ctx context.Context, tx pgx.Tx) error {
					_, err := tx.Exec(ctx,
						`INSERT INTO `+reconITProbeTable+` (attempt_id) VALUES ($1::uuid)`, attempt.AttemptID)
					return err
				},
			})
			if !errors.Is(err, ErrAttemptDiscarded) || !res.Discarded ||
				res.DiscardReason != DiscardReasonTaskNotRunning {
				t.Fatalf("cancelled late submit = %+v, err %v, want discarded/%s", res, err, DiscardReasonTaskNotRunning)
			}
			if state := reconAttemptState(t, ctx, pool, attempt.AttemptID); state != string(AttemptStateAbandoned) {
				t.Errorf("cancelled attempt state = %q, want %q", state, AttemptStateAbandoned)
			}
			if n := recoveryITProbeRows(t, ctx, pool, attempt.AttemptID); n != 0 {
				t.Errorf("cancelled late submit persisted %d batch row(s), want 0", n)
			}
			if got, ok := reconPersistedThrough(t, ctx, pool, taskID); ok {
				t.Errorf("pointer moved for a cancelled late submit: %d", got)
			}
			gaps := reconGapReasons(t, ctx, pool, taskID)
			if gaps[string(GapNotStarted)] != 1 || gaps[string(GapInterrupted)] != 1 {
				t.Errorf("gaps = %v, want not_started=1 (cancel remainder) and interrupted=1 (discarded interval)", gaps)
			}
			if refusals := recoveryITRefusals(t, ctx, pool, taskID); len(refusals) != 1 ||
				refusals[0].Actor != "worker-cancelled" ||
				refusals[0].Reason != DiscardReasonTaskNotRunning ||
				refusals[0].Result != "discarded" {
				t.Errorf("refuse audits = %+v, want one %s/discarded by worker-cancelled",
					refusals, DiscardReasonTaskNotRunning)
			}
		})

		t.Run("expired_lease_is_discarded", func(t *testing.T) {
			taskID := uuid.NewString()
			reconSeedHeightTask(t, ctx, pool, taskID, 1400, 1411, "running", "")

			attempt := reconClaim(t, ctx, store, taskID, "worker-expired", 4, 150*time.Millisecond)
			reconWaitForLeaseExpiry(t, ctx, pool, attempt.AttemptID)

			res, err := store.CommitScanBatch(ctx, CommitScanBatchRequest{
				TaskID: taskID, AttemptID: attempt.AttemptID,
				PersistResults: func(ctx context.Context, tx pgx.Tx) error {
					_, err := tx.Exec(ctx,
						`INSERT INTO `+reconITProbeTable+` (attempt_id) VALUES ($1::uuid)`, attempt.AttemptID)
					return err
				},
			})
			if !errors.Is(err, ErrAttemptDiscarded) || !res.Discarded ||
				res.DiscardReason != DiscardReasonLeaseExpired {
				t.Fatalf("expired-lease late submit = %+v, err %v, want discarded/%s", res, err, DiscardReasonLeaseExpired)
			}
			if state := reconAttemptState(t, ctx, pool, attempt.AttemptID); state != string(AttemptStateAbandoned) {
				t.Errorf("expired attempt state = %q, want %q", state, AttemptStateAbandoned)
			}
			if n := recoveryITProbeRows(t, ctx, pool, attempt.AttemptID); n != 0 {
				t.Errorf("expired-lease late submit persisted %d batch row(s), want 0", n)
			}
			if got, ok := reconPersistedThrough(t, ctx, pool, taskID); ok {
				t.Errorf("pointer moved for an expired-lease late submit: %d", got)
			}
			if gaps := reconGapReasons(t, ctx, pool, taskID); gaps[string(GapInterrupted)] != 1 {
				t.Errorf("gaps = %v, want interrupted=1 for the abandoned interval", gaps)
			}
			if refusals := recoveryITRefusals(t, ctx, pool, taskID); len(refusals) != 1 ||
				refusals[0].Actor != "worker-expired" ||
				refusals[0].Reason != DiscardReasonLeaseExpired ||
				refusals[0].Result != "discarded" {
				t.Errorf("refuse audits = %+v, want one %s/discarded by worker-expired",
					refusals, DiscardReasonLeaseExpired)
			}
		})
	})

	t.Run("claim_releases_the_task_lock_across_slow_execution", func(t *testing.T) {
		taskID := uuid.NewString()
		reconSeedHeightTask(t, ctx, pool, taskID, 1500, 1511, "running", "")

		type commitOutcome struct {
			result CommitScanBatchResult
			err    error
		}
		claimed := make(chan ClaimAttemptResult, 1)
		claimErr := make(chan error, 1)
		commitOutcomes := make(chan commitOutcome, 1)
		release := make(chan struct{})
		var releaseOnce sync.Once
		releaseExecution := func() { releaseOnce.Do(func() { close(release) }) }

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := store.ClaimScanAttempt(ctx, ClaimAttemptRequest{
				TaskID: taskID, Owner: "worker-slow", Span: 4, LeaseTTL: time.Minute,
			})
			if err != nil {
				claimErr <- err
				return
			}
			claimed <- result
			// Execution phase: outside any DB transaction, arbitrarily slow
			// (this channel wait stands in for the RPC).
			<-release
			commit, commitErr := store.CommitScanBatch(ctx, CommitScanBatchRequest{
				TaskID: taskID, AttemptID: result.AttemptID,
				PersistResults: func(ctx context.Context, tx pgx.Tx) error {
					_, err := tx.Exec(ctx,
						`INSERT INTO `+reconITProbeTable+` (attempt_id) VALUES ($1::uuid)`, result.AttemptID)
					return err
				},
			})
			commitOutcomes <- commitOutcome{result: commit, err: commitErr}
		}()
		// On any early failure, unblock the parked execution and wait for the
		// worker before the pool cleanup runs.
		t.Cleanup(func() {
			releaseExecution()
			wg.Wait()
		})

		var claimedRes ClaimAttemptResult
		select {
		case claimedRes = <-claimed:
		case err := <-claimErr:
			t.Fatalf("claim: %v", err)
		case <-time.After(5 * time.Second):
			t.Fatal("the claim did not return within 5s")
		}

		// The claim transaction already committed: while the invocation is
		// parked in its execution phase, an independent session takes the
		// recon_task row lock. A claim that held its transaction across the
		// slow execution would block here until its context deadline.
		probeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		second, err := store.ClaimScanAttempt(probeCtx, ClaimAttemptRequest{
			TaskID: taskID, Owner: "worker-probe", Span: 4, LeaseTTL: time.Minute,
		})
		if !errors.Is(err, ErrClaimTaken) {
			t.Fatalf("claim while the first invocation executes = %+v, err %v, want ErrClaimTaken (never a lock wait on the parked execution)",
				second, err)
		}
		rec, err := store.RecoverStaleAttempts(probeCtx, RecoverStaleAttemptsRequest{
			TaskID: taskID, Limit: 10, Actor: reconITOpsActor,
		})
		if err != nil || len(rec.Abandoned) != 0 {
			t.Fatalf("task-locking recovery pass during the slow execution = %+v, err %v, want a no-op (unexpired lease)", rec, err)
		}

		// The execution completes in its own short commit transaction and the
		// next interval claims immediately on the released lock.
		releaseExecution()
		select {
		case out := <-commitOutcomes:
			if out.err != nil || !out.result.Committed {
				t.Fatalf("commit after the slow execution = %+v, err %v, want committed", out.result, out.err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("the commit did not complete within 10s")
		}
		if got, _ := reconPersistedThrough(t, ctx, pool, taskID); got != 1503 {
			t.Fatalf("pointer after the commit = %d, want 1503", got)
		}
		if start, end := reconHeightRange(t, claimedRes); start != 1500 || end != 1503 {
			t.Fatalf("parked claim = [%d,%d], want [1500,1503]", start, end)
		}
		next := reconClaim(t, ctx, store, taskID, "worker-next", 4, time.Minute)
		if start, end := reconHeightRange(t, next); start != 1504 || end != 1507 {
			t.Fatalf("next claim = [%d,%d], want [1504,1507]", start, end)
		}
	})
}
