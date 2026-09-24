//go:build fault

// state_dual_test.go is the T023 fault-layer acceptance: the dual fault state
// (Redis stopped + Kafka suspended; PostgreSQL and the local chain stay up),
// seven operation classes, verified through the real service entry.
//
//   - new withdrawal creation is refused with the clear retryable error
//     (PD-1 conflict: limiter unavailable and/or capacity boundary) and
//     nothing is persisted;
//   - chain processing continues: deposits become observations and
//     confirmations, their events stay in the durable Outbox (0 loss,
//     0 overwrite), and on-chain facts are never rejected;
//   - an already accepted withdrawal keeps executing under its existing
//     gates (admission reads PostgreSQL), queries degrade honestly and the
//     subscription stops delivering without losing anything;
//   - recovery drains, the consumer catches up from durable progress and
//     every event is applied exactly once; the five FR-06 zero-invariants
//     hold and event delivery changes no payment authority.
package faultdrill

import (
	"context"
	"testing"
	"time"
)

// TestStateDualFault is the T023 acceptance.
func TestStateDualFault(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()

	scene := StartScene(t, ctx, SceneOptions{StartRuntimes: true})
	record := func(kind string, data any) {
		t.Helper()
		if err := scene.Env.Evidence.Record(kind, data); err != nil {
			t.Fatalf("record evidence %s: %v", kind, err)
		}
	}

	// --- normal baseline ------------------------------------------------------
	// An accepted request whose execution continues through the dual fault,
	// and one real deposit delivered normally.
	accepted := scene.CreateFresh("dual-accepted")
	baselineDeposit, _ := scene.ProbeDeposit(3001)
	scene.WaitEventDelivered()
	assertLedgerExactlyOnce(t, ctx, scene.Env, int(scene.Count(`SELECT count(*) FROM outbox_events`)))
	record("dual_normal_baseline", map[string]any{
		"request": accepted.ID, "deposit_tx": baselineDeposit.Hex(),
	})

	// --- state: dual ----------------------------------------------------------
	scene.ApplyState(StateDual)
	scene.WaitDependency("redis", false)
	scene.WaitDependency("kafka", false)

	dual := map[string]string{}
	refusal := scene.CreateRefused("dual-refused")
	dual[ClassWithdrawalCreate] = VerdictRefused
	record("dual_refusal", map[string]any{"message": refusal})

	// On-chain facts are never rejected: a new deposit is observed and
	// confirmed while both non-authoritative dependencies are down.
	dualDeposit, _ := scene.ProbeDeposit(3002)
	dual[ClassDepositProcessing] = VerdictContinue
	dual[ClassConfirmationReorg] = VerdictContinue
	if n := scene.Count(`SELECT count(*) FROM deposit_observations WHERE tx_hash = $1 AND status = 'confirmed'`, dualDeposit.Hex()); n != 1 {
		t.Fatalf("dual deposit confirmations = %d, want 1", n)
	}
	backlog := scene.WaitOutbox(90*time.Second, "dual backlog grows", func(totals OutboxTotals) bool {
		return totals.Pending >= 2
	})
	record("dual_backlog", backlog)

	// The accepted request keeps executing: the execution admission succeeds.
	dual[ClassExistingExecution] = VerdictContinue
	scene.ProbeExecution(accepted.ID, "intent-dual-1", accepted.AuthID)

	dual[ClassQuery] = scene.QueryVerdict(accepted.ID)
	if dual[ClassQuery] != VerdictDegraded {
		t.Fatalf("dual query verdict = %q, want degraded", dual[ClassQuery])
	}
	delivery := scene.EventDelivery()
	if delivery.Delivery != "degraded" || delivery.Pending < 2 {
		t.Fatalf("dual delivery posture = %+v, want degraded with a durable backlog", delivery)
	}
	dual[ClassEventSubscription] = VerdictBacklog
	if Degraded(scene.DegradationNow()) {
		dual[ClassNonCritical] = VerdictDegraded
	} else {
		t.Fatal("dual state did not report non-critical degradation")
	}
	AssertMatrixRow(t, StateDual, dual)
	scene.RecordState(StateDual, dual)

	// The authority fingerprint is captured once every admission is done: the
	// following drain/catch-up must not change a single send-side row.
	before, err := scene.Env.AuthorityFingerprint(ctx)
	if err != nil {
		t.Fatalf("authority fingerprint: %v", err)
	}
	pendingBefore := scene.Count(`SELECT count(*) FROM outbox_events WHERE publish_state = 'pending'`)
	if pendingBefore < 2 {
		t.Fatalf("pending before recovery = %d, want the durable backlog", pendingBefore)
	}

	// --- recovery: catch up from durable facts --------------------------------
	scene.ApplyState(StateCatchup)
	scene.WaitDependency("redis", true)
	scene.WaitDependency("kafka", true)
	scene.WaitEventDelivered()
	assertLedgerExactlyOnce(t, ctx, scene.Env, int(scene.Count(`SELECT count(*) FROM outbox_events`)))

	if rows := scene.ConsumerProgressRows(); rows == 0 {
		t.Fatal("consumer progress rows disappeared (progress must stay durable)")
	}
	gap, err := scene.Env.CatchupGap(ctx)
	if err != nil || gap != 0 {
		t.Fatalf("catch-up gap = %d (err %v), want 0", gap, err)
	}

	after, err := scene.Env.AuthorityFingerprint(ctx)
	if err != nil {
		t.Fatalf("authority fingerprint after: %v", err)
	}
	for table, count := range before {
		if after[table] != count {
			t.Fatalf("%s changed through publish/consume/catch-up: %d -> %d (a processed event is not a send permission)",
				table, count, after[table])
		}
	}
	record("dual_authority_freeze", map[string]any{"before": before, "after": after})

	// Recovery re-admits new controllable work.
	recovered := scene.CreateFresh("dual-recovered")
	if got := scene.QueryVerdict(recovered.ID); got != VerdictContinue {
		t.Fatalf("recovered query verdict = %q, want continue", got)
	}
	scene.WaitEventDelivered()

	report, err := scene.Env.CollectInvariants(ctx)
	if err != nil {
		t.Fatalf("CollectInvariants: %v", err)
	}
	AssertInvariants(t, report)
	record("t023_invariants", report)
}
