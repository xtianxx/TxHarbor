//go:build integration

package txlifecycle

import (
	"context"
	"log/slog"
	"testing"

	"github.com/xtianxx/txharbor/internal/app"
	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/execution"
	"github.com/xtianxx/txharbor/internal/nonce"
)

// jointWorkerConfig carries the worker knobs the joint constructor reads; the
// rest of the config belongs to paths this test never enters.
func jointWorkerConfig() *config.Config {
	return &config.Config{
		WorkerTTL:          config.DefaultWorkerTTL,
		WorkerHeartbeat:    config.DefaultWorkerHeartbeat,
		WorkerStall:        config.DefaultWorkerStall,
		WorkerBackoffBase:  config.DefaultWorkerBackoffBase,
		WorkerBackoffMax:   config.DefaultWorkerBackoffMax,
		WorkerScanInterval: config.DefaultWorkerScanInterval,
		WorkerLabel:        "joint-wiring-test",
	}
}

// TestJointWorkerProductionWiring is the production-wiring proof for the joint
// deployment: the 011 worker constructs a non-nil StepDriver/Reconciler over
// the real 010 adapter (no mocks, real PG/Anvil/009); the adapter converges on
// (intent_id, step_id) so re-invoking the same claim/step yields exactly one
// attempt and one dispatch; the adapter's reader returns facts consistent with
// 010's durable rows; and the worker's own driver runs a full real
// issue -> advance -> converge cycle.
func TestJointWorkerProductionWiring(t *testing.T) {
	ctx := context.Background()
	j := newJointEnv(t)
	jj := j.admitIntent()
	j.installTransferEmit(1000)

	adapter, err := NewLifecycleLive(j.pool, j.store)
	if err != nil {
		t.Fatalf("010 lifecycle adapter: %v", err)
	}
	worker, err := app.NewJointWithdrawalWorker(j.pool, jointWorkerConfig(), nil, slog.Default(), app.JointDeps{
		Advancer:       adapter,
		Reader:         adapter,
		Binding:        execution.NewLiveBindingReader(nonce.NewReadProvider(j.pool, "")),
		AllocBinding:   func(ctx context.Context, intentID string) (string, error) { return "", nil },
		ConfirmAttempt: func(ctx context.Context, intentID string) error { return nil },
	})
	if err != nil {
		t.Fatalf("joint worker: %v", err)
	}
	if worker.Driver == nil || worker.Reconciler == nil {
		t.Fatalf("joint worker not wired: driver=%v reconciler=%v", worker.Driver, worker.Reconciler)
	}
	if worker.Driver.Advancer == nil || worker.Driver.Binding == nil || worker.Reconciler.Reader == nil {
		t.Fatalf("boundary halves missing: driver=%+v reconciler=%+v", worker.Driver, worker.Reconciler)
	}
	if _, err := app.NewJointWithdrawalWorker(j.pool, jointWorkerConfig(), nil, slog.Default(), app.JointDeps{}); err == nil {
		t.Fatal("incomplete joint deps must refuse construction")
	}

	// Idempotency by re-invocation: the same (intent, claim, step) twice.
	req := execution.AdvanceRequest{
		IntentID: jj.intentID, RequestID: jj.requestID, CallerID: j.callerID,
		OwnerID: jj.ownerID, LeaseVersion: jj.claimVersion, RecoveryVersion: 0,
		StepID: "step-wiring-1", Action: execution.ActionFirstBroadcast,
	}
	first, err := adapter.Advance(ctx, req)
	if err != nil {
		t.Fatalf("first advance: %v", err)
	}
	if first.Class != execution.OutcomeSent || first.AttemptID == "" || first.TxHash == "" {
		t.Fatalf("first advance = %+v, want sent with attempt/hash", first)
	}
	second, err := adapter.Advance(ctx, req)
	if err != nil {
		t.Fatalf("second advance: %v", err)
	}
	if second.Class != execution.OutcomeSent || second.AttemptID != first.AttemptID || second.TxHash != first.TxHash {
		t.Fatalf("second advance = %+v, want the recorded sent outcome for %s", second, first.AttemptID)
	}
	if got := j.attemptCount(jj.intentID); got != 1 {
		t.Fatalf("attempts = %d, want exactly 1 for one step", got)
	}
	if got := j.sendRowCount(first.AttemptID); got != 1 {
		t.Fatalf("send rows = %d, want exactly 1 for one step", got)
	}

	// The 011 reconcile facts must equal 010's durable rows.
	facts, err := worker.Reconciler.Reader.Read(ctx, jj.intentID)
	if err != nil {
		t.Fatalf("adapter read: %v", err)
	}
	if len(facts.Attempts) != 1 || facts.Attempts[0].AttemptID != first.AttemptID ||
		facts.Attempts[0].State != "sent" || facts.CurrentAttemptID != first.AttemptID {
		t.Fatalf("facts = %+v, want the single sent attempt %s", facts, first.AttemptID)
	}
	var storedHash string
	var storedRevision int64
	if err := j.pool.QueryRow(ctx,
		`SELECT COALESCE(s.tx_hash, ''), a.revision_seq
		   FROM tx_attempts a LEFT JOIN tx_attempt_signings s ON s.attempt_id = a.attempt_id
		  WHERE a.attempt_id = $1`, first.AttemptID).Scan(&storedHash, &storedRevision); err != nil {
		t.Fatal(err)
	}
	if facts.RevisionVersion != storedRevision {
		t.Fatalf("facts revision = %d, stored = %d", facts.RevisionVersion, storedRevision)
	}
	if facts.Unknown != nil {
		t.Fatalf("sent attempt surfaced as unknown: %+v", facts.Unknown)
	}
	if first.TxHash != storedHash {
		t.Fatalf("outcome hash %s differs from stored %s", first.TxHash, storedHash)
	}

	// The worker's own driver path: a second intent runs the real
	// issue -> advance -> converge cycle with the real adapter.
	jj2 := j.admitIntent()
	out, err := worker.Driver.IssueAndAdvance(ctx, execution.StepRequest{
		IntentID: jj2.intentID, RequestID: jj2.requestID,
		OwnerID: jj2.ownerID, LeaseVersion: jj2.claimVersion, Action: execution.ActionFirstBroadcast,
	})
	if err != nil {
		t.Fatalf("joint driver issue+advance: %v", err)
	}
	if out.OutcomeClass != execution.OutcomeSent || out.FinalStepState != execution.StepConverged || out.AttemptID == "" {
		t.Fatalf("driver outcome = %+v, want sent/converged with an attempt", out)
	}
	var stepState, stepAttempt string
	if err := j.pool.QueryRow(ctx,
		`SELECT state, COALESCE(attempt_id, '') FROM execution_steps WHERE intent_id = $1`,
		jj2.intentID).Scan(&stepState, &stepAttempt); err != nil {
		t.Fatal(err)
	}
	if stepState != execution.StepConverged || stepAttempt != out.AttemptID {
		t.Fatalf("step = %s/%s, want converged/%s", stepState, stepAttempt, out.AttemptID)
	}
	if got := j.attemptCount(jj2.intentID); got != 1 {
		t.Fatalf("driver path attempts = %d, want exactly 1", got)
	}
}

func (j *jointEnv) attemptCount(intentID string) int {
	j.t.Helper()
	var n int
	if err := j.pool.QueryRow(j.ctx, `SELECT count(*) FROM tx_attempts WHERE intent_id = $1`, intentID).Scan(&n); err != nil {
		j.t.Fatal(err)
	}
	return n
}
