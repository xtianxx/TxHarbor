//go:build integration

// Batch D joint converge-fence coverage. The driver's T-step-converge is
// claim-fenced (owner + lease_version + active + unexpired on the DB clock).
// When the qualification expires in the window between an accepted 010
// dispatch and the converge, the old holder cannot converge; the step stays
// `issued` and the real sent attempt stays durable in 010. A legitimate taker
// re-claims, and the 011 reconciler consumes 010's sent fact, converging the
// SAME step onto the SAME attempt without a second attempt or dispatch and
// without rewriting 010 history.
//
// The fence seam wraps the real 010 adapter (never a mock): the adapter
// performs the real broadcast through the production worker Driver, then the
// seam expires the claim before the driver converges.
package txlifecycle

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/xtianxx/txharbor/internal/app"
	"github.com/xtianxx/txharbor/internal/execution"
	"github.com/xtianxx/txharbor/internal/nonce"
)

// fenceAfterDispatch is the test-only seam around the real 010 adapter: after
// an accepted dispatch has committed, it expires the intent's claim on the
// database clock so the driver's converge fence fails. It performs no send and
// no 011 write of its own.
type fenceAfterDispatch struct {
	execution.LifecycleAdvancer
	j        *jointEnv
	intentID string
}

func (f fenceAfterDispatch) Advance(ctx context.Context, req execution.AdvanceRequest) (execution.AdvanceOutcome, error) {
	out, err := f.LifecycleAdvancer.Advance(ctx, req)
	if err == nil && out.Class == execution.OutcomeSent {
		f.j.mustExec(`UPDATE execution_claims
			SET acquired_at = now() - interval '2 hours', expires_at = now() - interval '1 hour'
			WHERE intent_id = $1`, f.intentID)
	}
	return out, err
}

// productionWorkerFenced builds the production worker exactly as the joint
// deployment does, with the real adapter wrapped by the one test-only fence
// seam above.
func (j *jointEnv) productionWorkerFenced(intentID string) (*app.WithdrawalWorker, *LifecycleLive) {
	j.t.Helper()
	adapter, err := NewLifecycleLive(j.pool, j.store)
	if err != nil {
		j.t.Fatalf("010 lifecycle adapter: %v", err)
	}
	worker, err := app.NewJointWithdrawalWorker(j.pool, jointWorkerConfig(), nil, slog.Default(), app.JointDeps{
		Advancer: fenceAfterDispatch{LifecycleAdvancer: adapter, j: j, intentID: intentID},
		Reader:   adapter,
		Binding:  execution.NewLiveBindingReader(nonce.NewReadProvider(j.pool, "")),
		AllocBinding: func(ctx context.Context, intentID string) (string, error) {
			return "", nil
		},
		ConfirmAttempt: func(ctx context.Context, intentID string) error { return nil },
	})
	if err != nil {
		j.t.Fatalf("fenced joint worker: %v", err)
	}
	if worker.Driver == nil || worker.Reconciler == nil {
		j.t.Fatalf("fenced joint worker not wired: driver=%v reconciler=%v", worker.Driver, worker.Reconciler)
	}
	return worker, adapter
}

// intentRowsForID counts the 011 payment_intents rows for one identity.
func (j *jointEnv) intentRowsForID(intentID string) int {
	j.t.Helper()
	var n int
	if err := j.pool.QueryRow(j.ctx,
		`SELECT count(*) FROM payment_intents WHERE intent_id = $1`, intentID).Scan(&n); err != nil {
		j.t.Fatal(err)
	}
	return n
}

// TestJointConvergeFenceThenReconcileNoDupe fences the claim between the
// accepted 010 dispatch and the driver's T-step-converge, then lets a taker
// reconcile the open step from 010's facts.
//
// Defect: none known — coverage gap.
// Invariant: persistence.md §3.2 ("after 010 returns, before T-step-converge":
// an open `issued` step is reconciled with 010's facts) and §5 claim fencing
// (owner + version + active + unexpired on the DB clock). A fenced converge
// must not write, the step must stay open, and the taker's reconcile must
// converge the same step onto the same durable attempt — never a second
// attempt, dispatch or signing; known 010 results are never rewritten.
// Gap: J2 covers a claim expired before any send; no test fences the claim in
// the dispatch->converge window while a real accepted dispatch is durable.
// Level: joint lane (real PostgreSQL, real Anvil, real 009 signer-serve,
// production worker Driver + real 010 adapter; the fence seam is test-only and
// performs no send).
// Criteria: the fenced driver returns claim_not_current with the step still
// `issued` and no attempt, while 010 holds exactly one `sent` attempt with one
// accepted send; after the taker's real claim takeover, ReconcileIntent
// converges the same step to `converged`/sent on the same attempt_id and
// completes the intent with sends/attempts/intents = 1/1/1; the attempt
// revision, signing row, 009 result row and the accepted send row are
// unchanged.
func TestJointConvergeFenceThenReconcileNoDupe(t *testing.T) {
	ctx := context.Background()
	j := newJointEnv(t)
	j.setAutomine(false)

	jj := j.admitIntent()
	j.markClaimed(jj)
	worker, _ := j.productionWorkerFenced(jj.intentID)

	// The dispatch is real and accepted; the claim expires before the converge.
	out, err := worker.Driver.IssueAndAdvance(ctx, execution.StepRequest{
		IntentID: jj.intentID, RequestID: jj.requestID, CallerID: j.callerID,
		OwnerID: jj.ownerID, LeaseVersion: jj.claimVersion, Action: execution.ActionFirstBroadcast,
	})
	if err != nil {
		t.Fatalf("fenced driver broadcast: %v", err)
	}
	if out.Refusal != execution.ClassClaimNotCurrent || out.FinalStepState != execution.StepIssued || out.StepID == "" {
		t.Fatalf("fenced converge = %+v, want claim_not_current/issued with the step id", out)
	}
	if out.AttemptID != "" {
		t.Fatalf("fenced converge reported attempt id %q; the step must stay unresolved", out.AttemptID)
	}
	if got := j.intentState(jj.intentID); got != execution.IntentExecuting {
		t.Fatalf("intent state = %s, want executing (issue applied, converge fenced)", got)
	}

	// The real sent attempt is durable in 010; the step is open with no attempt.
	var attemptID string
	if err := j.pool.QueryRow(ctx,
		`SELECT attempt_id FROM tx_attempts WHERE intent_id = $1`, jj.intentID).Scan(&attemptID); err != nil {
		t.Fatal(err)
	}
	if got := j.attemptCount(jj.intentID); got != 1 {
		t.Fatalf("attempts = %d, want exactly 1", got)
	}
	if got := j.sendRowCount(attemptID); got != 1 {
		t.Fatalf("send rows = %d, want 1", got)
	}
	var attemptState, sendOutcome string
	if err := j.pool.QueryRow(ctx,
		`SELECT state FROM tx_attempts WHERE attempt_id = $1`, attemptID).Scan(&attemptState); err != nil {
		t.Fatal(err)
	}
	if attemptState != "sent" {
		t.Fatalf("attempt state = %s, want sent", attemptState)
	}
	if err := j.pool.QueryRow(ctx,
		`SELECT outcome FROM tx_send_attempts WHERE attempt_id = $1`, attemptID).Scan(&sendOutcome); err != nil {
		t.Fatal(err)
	}
	if sendOutcome != "accepted" {
		t.Fatalf("send outcome = %s, want accepted", sendOutcome)
	}
	var stepState string
	var stepAttempt *string
	if err := j.pool.QueryRow(ctx,
		`SELECT state, attempt_id FROM execution_steps WHERE step_id = $1`, out.StepID).
		Scan(&stepState, &stepAttempt); err != nil {
		t.Fatal(err)
	}
	if stepState != execution.StepIssued || stepAttempt != nil {
		t.Fatalf("fenced step = %s/%v, want issued with no attempt", stepState, stepAttempt)
	}

	// Snapshot 010/009 history before the taker reconcile.
	attBefore, err := j.store.AttemptByID(ctx, attemptID)
	if err != nil {
		t.Fatal(err)
	}
	revBefore := attBefore.RevisionSeq
	signingsBefore := j.signingRowsForAttempt(attemptID)
	resultsBefore := j.signatureResultRowsForIntent(jj.intentID)
	var (
		seqBefore     int
		kindBefore    string
		outcomeBefore string
		atBefore      time.Time
	)
	if err := j.pool.QueryRow(ctx,
		`SELECT send_seq, kind, outcome, dispatched_at FROM tx_send_attempts WHERE attempt_id = $1`,
		attemptID).Scan(&seqBefore, &kindBefore, &outcomeBefore, &atBefore); err != nil {
		t.Fatal(err)
	}

	// Legitimate taker: real 011 claim takeover after expiry.
	claims, err := execution.NewClaimStore(j.pool, 30*time.Second, 5*time.Minute)
	if err != nil {
		t.Fatalf("NewClaimStore: %v", err)
	}
	taker, err := claims.Claim(ctx, jj.intentID, "batchd-taker")
	if err != nil || !taker.Acquired || taker.Version <= jj.claimVersion {
		t.Fatalf("takeover claim = %+v %v, want a version above %d", taker, err, jj.claimVersion)
	}

	rec, err := worker.Reconciler.ReconcileIntent(ctx, jj.intentID, "batchd-taker", taker.Version)
	if err != nil || !rec.Converged || rec.FinalStepState != execution.StepConverged || rec.OutcomeClass != execution.OutcomeSent {
		t.Fatalf("taker reconcile = %+v (err %v), want converged/sent", rec, err)
	}

	// Same step, same attempt: no duplicate.
	var reconciledState, reconciledAttempt string
	if err := j.pool.QueryRow(ctx,
		`SELECT state, COALESCE(attempt_id, '') FROM execution_steps WHERE step_id = $1`, out.StepID).
		Scan(&reconciledState, &reconciledAttempt); err != nil {
		t.Fatal(err)
	}
	if reconciledState != execution.StepConverged || reconciledAttempt != attemptID {
		t.Fatalf("reconciled step = %s/%s, want converged on the same attempt %s",
			reconciledState, reconciledAttempt, attemptID)
	}
	if got := j.intentState(jj.intentID); got != execution.IntentCompleted {
		t.Fatalf("intent state = %s, want completed", got)
	}
	if got := j.stepCount(jj.intentID); got != 1 {
		t.Fatalf("execution_steps rows = %d, want exactly 1", got)
	}
	if sends, attempts, intents := j.sendRowCount(attemptID), j.attemptCount(jj.intentID), j.intentRowsForID(jj.intentID); sends != 1 || attempts != 1 || intents != 1 {
		t.Fatalf("post-reconcile counts = sends %d attempts %d intents %d, want 1/1/1", sends, attempts, intents)
	}

	// 010 history is unchanged by the reconcile.
	attAfter, err := j.store.AttemptByID(ctx, attemptID)
	if err != nil {
		t.Fatal(err)
	}
	if attAfter.State != "sent" || attAfter.RevisionSeq != revBefore {
		t.Fatalf("010 attempt history moved: state=%s revision=%d, want sent/%d",
			attAfter.State, attAfter.RevisionSeq, revBefore)
	}
	if got := j.signingRowsForAttempt(attemptID); got != signingsBefore {
		t.Fatalf("010 signing rows moved: %d -> %d", signingsBefore, got)
	}
	if got := j.signatureResultRowsForIntent(jj.intentID); got != resultsBefore {
		t.Fatalf("009 result rows moved: %d -> %d", resultsBefore, got)
	}
	var (
		seqAfter     int
		kindAfter    string
		outcomeAfter string
		atAfter      time.Time
	)
	if err := j.pool.QueryRow(ctx,
		`SELECT send_seq, kind, outcome, dispatched_at FROM tx_send_attempts WHERE attempt_id = $1`,
		attemptID).Scan(&seqAfter, &kindAfter, &outcomeAfter, &atAfter); err != nil {
		t.Fatal(err)
	}
	if seqAfter != seqBefore || kindAfter != kindBefore || outcomeAfter != outcomeBefore || !atAfter.Equal(atBefore) {
		t.Fatalf("010 send row changed: %d/%s/%s/%s -> %d/%s/%s/%s",
			seqBefore, kindBefore, outcomeBefore, atBefore, seqAfter, kindAfter, outcomeAfter, atAfter)
	}
}
