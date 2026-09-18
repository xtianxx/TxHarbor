//go:build integration

package txlifecycle

import (
	"context"
	"testing"
	"time"

	"github.com/xtianxx/txharbor/internal/execution"
)

// TestJointJ5LockLossDetectablePath is T050/J5 SUPPLEMENTARY shared-state
// coverage: it drives the real 010 Store directly and validates the freeze
// rows/events the two lanes share. The worker-Driver J5 acceptance is
// TestJointDriverJ5LockLossDetectablePath (joint_driver_integration_test.go).
//
// Detectable path only: a pause committed before a dispatch that recorded
// pause=none proves a stale basis, so 010 freezes the intent's further sends
// (011 observes the freeze), chain observation/reconcile stay available, and a
// controlled manual release lifts only the freeze cause while a resend still
// re-verifies every gate.
//
// It asserts what is NOT claimed: no "window is tiny/rare" property, no
// detection guarantee for unobservable faults.
func TestJointJ5LockLossDetectablePath(t *testing.T) {
	t.Log("supplementary shared-state J5: direct 010 Store drive, not the worker-Driver link")
	ctx := context.Background()
	j := newJointEnv(t)
	jj := j.admit()
	j.installTransferEmit(1000)

	res, err := j.store.Send(ctx, &SendRequest{AttemptID: jj.attemptID, Kind: SendInitial, Claim: j.claim(jj)})
	if err != nil || res.Outcome != "accepted" {
		t.Fatalf("initial send = %+v %v", res, err)
	}

	// The unperceived lock-loss evidence: a real pause whose created_at
	// precedes the dispatch that recorded pause=none.
	j.mustExec(`INSERT INTO indexer_pause (chain_id, height, expected_hash, actual_hash, kind, created_at)
		VALUES ($1,100,$2,$3,'hash_mismatch', now() - interval '1 hour')`,
		jointChainID, blockHashHex(100), blockHashHex(101))

	// Reconcile runs the version-vs-change-evidence comparison. Chain
	// observation is allowed even while frozen.
	if _, err := j.store.Reconcile(ctx, jj.attemptID, ""); err != nil {
		t.Fatalf("reconcile (observation still allowed): %v", err)
	}
	var freezeCause, freezeEvidence string
	if err := j.pool.QueryRow(ctx,
		`SELECT cause, evidence FROM tx_intent_freezes WHERE intent_id = $1 AND released_at IS NULL`, jj.intentID).
		Scan(&freezeCause, &freezeEvidence); err != nil {
		t.Fatalf("freeze row missing: %v", err)
	}
	if freezeCause != "protection_loss_residual" || freezeEvidence == "" {
		t.Fatalf("freeze = %s/%q, want protection_loss_residual with evidence", freezeCause, freezeEvidence)
	}
	var frozenEvents int
	if err := j.pool.QueryRow(ctx,
		`SELECT count(*) FROM tx_attempt_events WHERE attempt_id = $1 AND event = 'frozen'`, jj.attemptID).Scan(&frozenEvents); err != nil {
		t.Fatal(err)
	}
	if frozenEvents == 0 {
		t.Fatal("no frozen event recorded")
	}

	// The joint loop consumes 010's freeze fact and records 011's own marker;
	// 011 observes the freeze as an input and claims no detection guarantee.
	reader, err := NewLifecycleLive(j.pool, j.store)
	if err != nil {
		t.Fatalf("010 lifecycle adapter: %v", err)
	}
	reconciler := &execution.Reconciler{Pool: j.pool, Reader: reader}
	if _, err := reconciler.ReconcileIntent(ctx, jj.intentID, jj.ownerID, jj.claimVersion); err != nil {
		t.Fatalf("joint freeze reconcile: %v", err)
	}
	frozen, class, err := execution.IsFrozen(ctx, j.pool, jj.intentID)
	if err != nil || !frozen || class == "" {
		t.Fatalf("011 IsFrozen = %v/%s %v, want frozen", frozen, class, err)
	}

	// Every further send refuses; zero dispatch; observation/reconcile allowed.
	before := j.sendRowCount(jj.attemptID)
	res, err = j.store.Send(ctx, &SendRequest{AttemptID: jj.attemptID, Kind: SendReplay, Claim: j.claim(jj)})
	assertJointRefusal(t, res, err, ClassIntentFrozen)
	if got := j.sendRowCount(jj.attemptID); got != before {
		t.Fatalf("frozen send wrote a row: %d -> %d", before, got)
	}
	if _, err := j.store.Reconcile(ctx, jj.attemptID, ""); err != nil {
		t.Fatalf("reconcile while frozen: %v", err)
	}

	// Controlled manual release lifts ONLY the freeze cause.
	j.store.WithReleaseToken("joint-release")
	if _, err := j.store.Release(ctx, &ReleaseRequest{IntentID: jj.intentID, Permission: "wrong"}); refusalClass(err) != ClassReleaseNotPermitted {
		t.Fatalf("wrong release permission = %v", err)
	}
	ok, err := j.store.Release(ctx, &ReleaseRequest{
		IntentID: jj.intentID, Permission: "joint-release", Operator: "ops-j5", Reason: "reviewed", Basis: "evidence-j5",
	})
	if err != nil || !ok {
		t.Fatalf("controlled release = %v %v", ok, err)
	}
	// 010's controlled release is authoritative and ends the freeze cause. 011's
	// recorded marker is intentionally sticky (only the 010-side release ends
	// the cause); the joint reader must stop surfacing it.
	var releasedAt *time.Time
	if err := j.pool.QueryRow(ctx,
		`SELECT released_at FROM tx_intent_freezes WHERE intent_id = $1`, jj.intentID).Scan(&releasedAt); err != nil {
		t.Fatal(err)
	}
	if releasedAt == nil {
		t.Fatal("010 freeze cause not released")
	}
	facts, err := reader.Read(ctx, jj.intentID)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := execution.FreezeClass(facts); ok {
		t.Fatalf("joint reader still surfaces the released freeze: %+v", facts.Unknown)
	}

	// The pause is still present: release never overrides another gate, and a
	// resend re-verifies the full gate sequence.
	before = j.sendRowCount(jj.attemptID)
	res, err = j.store.Send(ctx, &SendRequest{AttemptID: jj.attemptID, Kind: SendReplay, Claim: j.claim(jj)})
	assertJointRefusal(t, res, err, ClassPausePresent)
	if got := j.sendRowCount(jj.attemptID); got != before {
		t.Fatalf("post-release resend wrote a row: %d -> %d", before, got)
	}
	var pauses int
	if err := j.pool.QueryRow(ctx, `SELECT count(*) FROM indexer_pause WHERE chain_id = $1`, jointChainID).Scan(&pauses); err != nil {
		t.Fatal(err)
	}
	if pauses == 0 {
		t.Fatal("release cleared an unrelated pause (it must not)")
	}
	var releasedEvents int
	if err := j.pool.QueryRow(ctx,
		`SELECT count(*) FROM tx_attempt_events WHERE attempt_id = $1 AND event = 'released'`, jj.attemptID).Scan(&releasedEvents); err != nil {
		t.Fatal(err)
	}
	if releasedEvents == 0 {
		t.Fatal("release audit event missing")
	}
}

func (j *jointEnv) sendRowCount(attemptID string) int {
	j.t.Helper()
	var n int
	if err := j.pool.QueryRow(j.ctx, `SELECT count(*) FROM tx_send_attempts WHERE attempt_id = $1`, attemptID).Scan(&n); err != nil {
		j.t.Fatal(err)
	}
	return n
}
