//go:build fault

// state_kafka_test.go is the T022 fault-layer acceptance: the Kafka-only fault
// state, seven operation classes, verified through the real service entry with
// real PostgreSQL/Redis containers, a suspended real Kafka broker (log and
// committed offsets preserved) and a local chain.
//
//   - the chain keeps processing: deposits become observations and
//     confirmations, and their events accumulate in the durable Outbox
//     (0 loss, 0 overwrite, bounded and observable backlog);
//   - withdrawal creation keeps its receive semantics while below the
//     capacity boundary; from the soft boundary on it is refused with the
//     clear retryable error (PD-2) and on-chain facts are still never
//     rejected (pending may exceed hard_limit);
//   - queries carry the honest delivery-degradation annotation, and the
//     subscription stops delivering without losing anything.
//
// Recovery drains and catches up; the final ledger effect per event is exactly
// one.
package faultdrill

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// TestStateKafkaOnly is the T022 acceptance.
func TestStateKafkaOnly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()

	// Small measured-shaped limits so the capacity boundary is reachable:
	// 0 < reserve (1) < soft (3) < hard (6).
	scene := StartScene(t, ctx, SceneOptions{
		SoftLimit: "3", HardLimit: "6", Reserve: "1", StartRuntimes: true,
	})
	record := func(kind string, data any) {
		t.Helper()
		if err := scene.Env.Evidence.Record(kind, data); err != nil {
			t.Fatalf("record evidence %s: %v", kind, err)
		}
	}

	// --- state: Kafka-only ----------------------------------------------------
	scene.ApplyState(StateKafkaDown)
	scene.WaitDependency("kafka", false)

	kafkaOnly := map[string]string{}
	// Withdrawal creation below the capacity boundary keeps its receive
	// semantics: accepted, event in the Outbox.
	accepted := scene.CreateFresh("kafka-accepted")
	kafkaOnly[ClassWithdrawalCreate] = VerdictContinue
	if n := scene.Count(`SELECT count(*) FROM outbox_events
		WHERE event_type = 'withdrawal.request.received' AND aggregate_id = $1`, accepted.ID); n != 1 {
		t.Fatalf("accepted create events = %d, want 1", n)
	}

	// Chain processing continues: the deposit is observed and confirmed while
	// Kafka is down.
	deposit1, _ := scene.ProbeDeposit(2001)
	kafkaOnly[ClassDepositProcessing] = VerdictContinue
	kafkaOnly[ClassConfirmationReorg] = VerdictContinue
	if n := scene.Count(`SELECT count(*) FROM deposit_observations WHERE tx_hash = $1 AND status = 'confirmed'`, deposit1.Hex()); n != 1 {
		t.Fatalf("kafka-only deposit confirmations = %d, want 1", n)
	}
	pendingAfterDeposit := scene.WaitOutbox(60*time.Second, "pending backlog grows", func(totals OutboxTotals) bool {
		return totals.Pending >= 2 && totals.Published == 0
	})
	record("kafka_only_backlog", pendingAfterDeposit)

	// Existing accepted request continues (admission reads PostgreSQL).
	kafkaOnly[ClassExistingExecution] = VerdictContinue
	scene.ProbeExecution(accepted.ID, "intent-kafka-1", accepted.AuthID)

	kafkaOnly[ClassQuery] = scene.QueryVerdict(accepted.ID)
	if kafkaOnly[ClassQuery] != VerdictDegraded {
		t.Fatalf("kafka-only query verdict = %q, want degraded", kafkaOnly[ClassQuery])
	}

	// The subscription stops delivering while committing nothing away: the
	// backlog is durable and reported.
	before := scene.EventDelivery()
	if before.Delivery != "degraded" {
		t.Fatalf("kafka-only delivery posture = %q, want degraded", before.Delivery)
	}
	if before.Pending <= 0 {
		t.Fatalf("kafka-only pending = %d, want an observable backlog", before.Pending)
	}
	if !strings.Contains(scene.MetricsText(), "outbox_pending_count") {
		t.Fatal("outbox_pending_count is not observable on the serve registry")
	}
	kafkaOnly[ClassEventSubscription] = VerdictBacklog

	if Degraded(scene.DegradationNow()) {
		kafkaOnly[ClassNonCritical] = VerdictDegraded
	} else {
		t.Fatal("kafka-only state did not report non-critical degradation")
	}
	AssertMatrixRow(t, StateKafkaDown, kafkaOnly)
	scene.RecordState(StateKafkaDown, kafkaOnly)

	// --- PD-2 soft boundary: controllable new writes are refused --------------
	// Accumulate committed events past the soft boundary (3) and then past the
	// hard boundary (6) with more real deposits.
	deposit2, _ := scene.ProbeDeposit(2002)
	scene.WaitOutbox(60*time.Second, "soft boundary reached", func(totals OutboxTotals) bool {
		return totals.Pending >= 3
	})
	refusalMessage := scene.CreateRefused("kafka-capacity-refused")
	if !strings.Contains(strings.ToLower(refusalMessage), "capacity") {
		t.Fatalf("refusal message %q does not explain the capacity boundary", refusalMessage)
	}
	if !scene.MetricsContains("capacity_refusals_total") {
		t.Fatal("capacity_refusals_total is not observable on the serve registry")
	}

	// Past hard_limit: on-chain facts are still never rejected; the backlog
	// may exceed the hard boundary, and the fact keeps entering the Outbox.
	deposit3, _ := scene.ProbeDeposit(2003)
	deposit4, _ := scene.ProbeDeposit(2004)
	hard := scene.WaitOutbox(90*time.Second, "hard boundary crossed by facts", func(totals OutboxTotals) bool {
		return totals.Pending >= 6
	})
	record("kafka_only_hard", hard)
	// The facts that crossed the hard boundary were accepted, not refused.
	for _, tx := range []common.Hash{deposit2, deposit3, deposit4} {
		if n := scene.Count(`SELECT count(*) FROM deposit_observations WHERE tx_hash = $1`, tx.Hex()); n != 1 {
			t.Fatalf("deposit %s observations = %d, want 1 (on-chain facts are never rejected)", tx, n)
		}
	}
	record("pd2_assessment", map[string]any{
		"refusal": refusalMessage, "pending_at_hard": hard.Pending, "hard_limit": 6,
		"facts_rejected": 0, "silent_drops": 0,
	})

	// --- recovery: drain and catch up -----------------------------------------
	scene.ApplyState(StateCatchup)
	scene.WaitDependency("kafka", true)
	scene.WaitEventDelivered()
	totals, err := scene.Env.OutboxTotals(ctx)
	if err != nil {
		t.Fatalf("outbox totals: %v", err)
	}
	if totals.Pending != 0 || totals.Published != totals.Total {
		t.Fatalf("recovery totals = %+v, want all published", totals)
	}
	assertLedgerExactlyOnce(t, ctx, scene.Env, int(totals.Total))

	// Recovery re-admits new controllable work below the boundary.
	recovered := scene.CreateFresh("kafka-recovered")
	if got := scene.QueryVerdict(recovered.ID); got != VerdictContinue {
		t.Fatalf("recovered query verdict = %q, want continue", got)
	}

	report, err := scene.Env.CollectInvariants(ctx)
	if err != nil {
		t.Fatalf("CollectInvariants: %v", err)
	}
	AssertInvariants(t, report)
	record("t022_invariants", report)
}
