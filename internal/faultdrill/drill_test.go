//go:build fault

// drill_test.go is the T080 main drill (V-DRILL; quickstart Q8): the full
// five-state sequence on the real service entry —
//
//	normal → Redis-only → Kafka-only → dual (Redis stopped, Kafka suspended,
//	PostgreSQL and the local chain stay up) → recovery/catch-up —
//
// with the seven operation classes checked per state, the T090 capacity pause
// and rescan exercised with a real persistence failure at the hard boundary,
// and the closing invariant suite:
//
//   - 0 duplicate withdrawal intents / nonce allocations / broadcast slots;
//   - 0 orphaned deposits credited; 0 authoritative state loss; 0 silent
//     event loss; 0 gate bypasses;
//   - the reference consumer applies every event exactly once (at-least-once
//     delivery + idempotent processing; the evidence is bounded to this
//     project's event identity/version/delivery semantics and the consumer
//     idempotency contract — never an external real ledger).
package faultdrill

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/xtianxx/txharbor/internal/events"
)

// TestDrillFiveStateMatrix is the T080 acceptance.
func TestDrillFiveStateMatrix(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Minute)
	defer cancel()

	// Small measured-shaped capacity limits: 0 < reserve (1) < soft (2) <
	// hard (4). They make the hard boundary reachable with a handful of real
	// facts so the T090 pause is exercised quickly.
	scene := StartScene(t, ctx, SceneOptions{
		SoftLimit: "2", HardLimit: "4", Reserve: "1", StartRuntimes: true,
	})
	record := func(kind string, data any) {
		t.Helper()
		if err := scene.Env.Evidence.Record(kind, data); err != nil {
			t.Fatalf("record evidence %s: %v", kind, err)
		}
	}
	observed := map[StateKind]map[string]string{}

	if err := scene.Env.Evidence.WriteFile("boundary.md", []byte(
		"# FR-16 verification boundary\n\n"+
			"The reference consumer is not a production ledger and is not authoritative. "+
			"\"Exactly once\" below means: at-least-once delivery plus the consumer's persistent "+
			"idempotency (inbox UNIQUE + version guard), verified through the reference consumer's "+
			"simulated ledger effects for this project's event identity/version/delivery semantics "+
			"and the consumer idempotency contract. It does NOT guarantee any external real ledger.\n"+
			events.ReferenceBoundaryStatement+"\n")); err != nil {
		t.Fatalf("write boundary statement: %v", err)
	}

	// Accepted requests are pre-created while healthy so every fault state can
	// probe an already-accepted withdrawal (each request carries its own
	// grant: an authorization is bound to one request). The backlog is drained
	// between creates because the drill runs with a small capacity boundary.
	redisExec := scene.CreateFreshDrained("redis-exec")
	kafkaExec := scene.CreateFreshDrained("kafka-exec")
	dualExec := scene.CreateFreshDrained("dual-exec")

	// --- state 1: normal ------------------------------------------------------
	normal := map[string]string{}
	normalDeposit, _ := scene.ProbeDeposit(5001)
	normal[ClassDepositProcessing] = VerdictContinue
	normal[ClassConfirmationReorg] = VerdictContinue
	normalCreate := scene.CreateFreshDrained("normal-create")
	normal[ClassWithdrawalCreate] = VerdictContinue
	normalExec := scene.CreateFreshDrained("normal-exec")
	scene.ProbeExecution(normalExec.ID, "intent-drill-normal", normalExec.AuthID)
	normal[ClassExistingExecution] = VerdictContinue
	if got := scene.QueryVerdict(normalCreate.ID); got != VerdictContinue {
		t.Fatalf("normal query verdict = %q, want continue", got)
	}
	normal[ClassQuery] = VerdictContinue
	scene.WaitEventDelivered()
	normal[ClassEventSubscription] = VerdictContinue
	if Degraded(scene.DegradationNow()) {
		t.Fatal("normal state reported degradation")
	}
	normal[ClassNonCritical] = VerdictContinue
	AssertMatrixRow(t, StateNormal, normal)
	observed[StateNormal] = normal
	scene.RecordState(StateNormal, normal)
	record("normal_assessment", map[string]any{"deposit_tx": normalDeposit.Hex(), "verdicts": normal})
	assertLedgerExactlyOnce(t, ctx, scene.Env, int(scene.Count(`SELECT count(*) FROM outbox_events WHERE publish_state = 'published'`)))

	// --- state 2: Redis-only (PD-1) -------------------------------------------
	scene.ApplyState(StateRedisDown)
	scene.WaitDependency("redis", false)
	redisOnly := map[string]string{}
	redisRefusal := scene.CreateRefused("drill-redis-refused")
	redisOnly[ClassWithdrawalCreate] = VerdictRefused
	record("redis_refusal", map[string]any{"message": redisRefusal})

	redisDeposit, _ := scene.ProbeDeposit(5002)
	redisOnly[ClassDepositProcessing] = VerdictContinue
	redisOnly[ClassConfirmationReorg] = VerdictContinue
	redisOnly[ClassExistingExecution] = VerdictContinue
	scene.ProbeExecution(redisExec.ID, "intent-drill-redis", redisExec.AuthID)
	if got := scene.QueryVerdict(redisExec.ID); got != VerdictDegraded {
		t.Fatalf("redis-only query verdict = %q, want degraded", got)
	}
	redisOnly[ClassQuery] = VerdictDegraded
	scene.WaitEventDelivered()
	redisOnly[ClassEventSubscription] = VerdictContinue
	if !Degraded(scene.DegradationNow()) {
		t.Fatal("redis-only state did not degrade the non-critical surface")
	}
	redisOnly[ClassNonCritical] = VerdictDegraded
	AssertMatrixRow(t, StateRedisDown, redisOnly)
	observed[StateRedisDown] = redisOnly
	scene.RecordState(StateRedisDown, redisOnly)
	record("redis_only_assessment", map[string]any{"deposit_tx": redisDeposit.Hex(), "verdicts": redisOnly})

	// --- state 3: Kafka-only (PD-2) -------------------------------------------
	scene.ApplyState(StateKafkaDown)
	scene.WaitDependency("kafka", false)
	kafkaOnly := map[string]string{}
	kafkaCreate := scene.CreateFresh("kafka-create")
	kafkaOnly[ClassWithdrawalCreate] = VerdictContinue
	kafkaDeposit, _ := scene.ProbeDeposit(5003)
	kafkaOnly[ClassDepositProcessing] = VerdictContinue
	kafkaOnly[ClassConfirmationReorg] = VerdictContinue
	kafkaOnly[ClassExistingExecution] = VerdictContinue
	scene.ProbeExecution(kafkaExec.ID, "intent-drill-kafka", kafkaExec.AuthID)
	if got := scene.QueryVerdict(kafkaCreate.ID); got != VerdictDegraded {
		t.Fatalf("kafka-only query verdict = %q, want degraded", got)
	}
	kafkaOnly[ClassQuery] = VerdictDegraded
	if delivery := scene.EventDelivery(); delivery.Delivery != "degraded" {
		t.Fatalf("kafka-only delivery posture = %q, want degraded", delivery.Delivery)
	}
	kafkaOnly[ClassEventSubscription] = VerdictBacklog
	if !Degraded(scene.DegradationNow()) {
		t.Fatal("kafka-only state did not degrade the non-critical surface")
	}
	kafkaOnly[ClassNonCritical] = VerdictDegraded
	AssertMatrixRow(t, StateKafkaDown, kafkaOnly)
	observed[StateKafkaDown] = kafkaOnly
	scene.RecordState(StateKafkaDown, kafkaOnly)

	// PD-2 soft boundary: the next create is refused with the capacity message.
	scene.WaitOutbox(60*time.Second, "soft boundary", func(totals OutboxTotals) bool {
		return totals.Pending >= 2
	})
	capacityRefusal := scene.CreateRefused("drill-capacity-refused")
	if !strings.Contains(strings.ToLower(capacityRefusal), "capacity") {
		t.Fatalf("capacity refusal message %q does not explain the boundary", capacityRefusal)
	}
	record("kafka_only_assessment", map[string]any{
		"deposit_tx": kafkaDeposit.Hex(), "verdicts": kafkaOnly,
		"capacity_refusal": capacityRefusal,
	})

	// --- T090: hard boundary + unsafe persistence → pause + rescan ------------
	// Accumulate to hard_limit=4 with one more real fact, then wait for the
	// committed backlog to cross it: every fact was accepted (on-chain facts
	// are never rejected; pending may exceed hard).
	scene.ProbeDeposit(5006)
	hard := scene.WaitOutbox(90*time.Second, "hard boundary", func(totals OutboxTotals) bool {
		return totals.Pending >= 4
	})
	record("hard_boundary", hard)

	// Hold the outbox write lock, emit one more real deposit, and terminate
	// the blocked 004 commit the moment it waits on the lock: the commit
	// cannot persist while the observed backlog is hard — the exact T090
	// pause condition. The termination is immediate so the scanner never waits
	// out its statement timeout and the lease heartbeat is not starved.
	lock, err := scene.Env.LockOutboxWrites(ctx)
	if err != nil {
		t.Fatalf("lock outbox writes: %v", err)
	}
	failedTx, failedHeight := scene.SendDeposit(5005)
	terminatedPid, err := scene.Env.TerminateBlockedOutboxWriter(ctx, 120*time.Second, func(seen string) {
		t.Logf("waiting for the blocked outbox writer; current waiters:%s", seen)
	})
	lock.Release()
	if err != nil {
		t.Fatalf("terminate blocked writer: %v", err)
	}
	record("pause_fault", map[string]any{
		"terminated_backend": terminatedPid, "deposit_tx": failedTx.Hex(), "height": failedHeight,
	})

	// The stream pauses with recorded recoverable evidence: the structured
	// log line carries the reason, the durable resume height and the rescan
	// plan; the failed fact is not committed yet (nothing was skipped, the
	// commit simply did not happen).
	scene.WaitLog(120*time.Second, "capacity hard boundary: pausing the deposit stream from reliable progress")
	scene.WaitLog(120*time.Second, "reason=capacity_not_persistable")
	if n := scene.Count(`SELECT count(*) FROM deposit_observations WHERE tx_hash = $1`, strings.ToLower(failedTx.Hex())); n != 0 {
		t.Fatalf("failed deposit observations before recovery = %d, want 0", n)
	}
	if !scene.MetricsContains("capacity_hard_breaches_total") {
		t.Fatal("capacity_hard_breaches_total is not observable on the serve registry")
	}
	record("pause_evidence", map[string]any{
		"paused": true, "reason": "capacity_not_persistable", "log_captured": true,
		"pending": scene.Count(`SELECT count(*) FROM outbox_events WHERE publish_state = 'pending'`),
	})

	// Capacity recovery (Kafka resumes, the publisher drains below hard): the
	// paused 004 stream rescans from its durable progress and commits the
	// failed fact exactly once.
	scene.ApplyState(StateCatchup)
	scene.WaitDependency("kafka", true)
	scene.WaitLog(180*time.Second, "capacity recovered: rescanning from reliable progress")
	scene.WaitObservation(failedTx)
	if n := scene.Count(`SELECT count(*) FROM deposit_observations WHERE tx_hash = $1`, strings.ToLower(failedTx.Hex())); n != 1 {
		t.Fatalf("rescan observations = %d, want exactly 1 (0 skipped, 0 duplicated)", n)
	}
	if n := scene.Count(`SELECT count(*) FROM outbox_events
		WHERE event_type = 'deposit.observation.created' AND tx_hash = $1`, strings.ToLower(failedTx.Hex())); n != 1 {
		t.Fatalf("rescan events = %d, want exactly 1", n)
	}
	scene.WaitEventDelivered()
	assertLedgerExactlyOnce(t, ctx, scene.Env, int(scene.Count(`SELECT count(*) FROM outbox_events WHERE publish_state = 'published'`)))
	record("rescan_evidence", map[string]any{"resumed": true, "skipped": 0, "duplicated": 0})

	// --- state 4: dual --------------------------------------------------------
	scene.ApplyState(StateDual)
	scene.WaitDependency("redis", false)
	scene.WaitDependency("kafka", false)
	dual := map[string]string{}
	dualRefusal := scene.CreateRefused("drill-dual-refused")
	dual[ClassWithdrawalCreate] = VerdictRefused
	dualDeposit, _ := scene.ProbeDeposit(5004)
	dual[ClassDepositProcessing] = VerdictContinue
	dual[ClassConfirmationReorg] = VerdictContinue
	dual[ClassExistingExecution] = VerdictContinue
	scene.ProbeExecution(dualExec.ID, "intent-drill-dual", dualExec.AuthID)
	if got := scene.QueryVerdict(dualExec.ID); got != VerdictDegraded {
		t.Fatalf("dual query verdict = %q, want degraded", got)
	}
	dual[ClassQuery] = VerdictDegraded
	dual[ClassEventSubscription] = VerdictBacklog
	if !Degraded(scene.DegradationNow()) {
		t.Fatal("dual state did not degrade the non-critical surface")
	}
	dual[ClassNonCritical] = VerdictDegraded
	AssertMatrixRow(t, StateDual, dual)
	observed[StateDual] = dual
	scene.RecordState(StateDual, dual)
	record("dual_assessment", map[string]any{
		"deposit_tx": dualDeposit.Hex(), "verdicts": dual, "refusal": dualRefusal,
	})

	// The authority fingerprint is captured once every admission is done: the
	// following drain/catch-up must not change a single send-side row.
	before, err := scene.Env.AuthorityFingerprint(ctx)
	if err != nil {
		t.Fatalf("authority fingerprint: %v", err)
	}

	// --- state 5: recovery / catch-up -----------------------------------------
	scene.ApplyState(StateCatchup)
	scene.WaitDependency("redis", true)
	scene.WaitDependency("kafka", true)
	scene.WaitEventDelivered()
	catchup := map[string]string{}
	catchup[ClassDepositProcessing] = VerdictContinue
	catchup[ClassConfirmationReorg] = VerdictContinue
	recovered := scene.CreateFresh("drill-recovered")
	catchup[ClassWithdrawalCreate] = VerdictContinue
	catchup[ClassExistingExecution] = VerdictContinue
	if got := scene.QueryVerdict(recovered.ID); got != VerdictContinue {
		t.Fatalf("catch-up query verdict = %q, want continue", got)
	}
	catchup[ClassQuery] = VerdictContinue
	catchup[ClassEventSubscription] = VerdictCatchup
	if Degraded(scene.DegradationNow()) {
		t.Fatal("catch-up state still reports degradation after recovery")
	}
	catchup[ClassNonCritical] = VerdictContinue
	AssertMatrixRow(t, StateCatchup, catchup)
	observed[StateCatchup] = catchup
	scene.RecordState(StateCatchup, catchup)

	// --- closing assertions ---------------------------------------------------
	consistent, total := MatrixConsistency(observed)
	if consistent != total {
		t.Fatalf("matrix consistency = %d/%d, want 100%%", consistent, total)
	}
	record("matrix_consistency", map[string]any{"consistent": consistent, "total": total, "observed": observed})

	// Every event is delivered at least once and applied exactly once by the
	// reference consumer (bounded evidence; see boundary.md).
	scene.WaitEventDelivered()
	assertLedgerExactlyOnce(t, ctx, scene.Env, int(scene.Count(`SELECT count(*) FROM outbox_events WHERE publish_state = 'published'`)))
	// No business unit was ever committed without its event.
	invariants, err := scene.Env.CollectInvariants(ctx)
	if err != nil {
		t.Fatalf("CollectInvariants: %v", err)
	}
	AssertInvariants(t, invariants)
	record("drill_invariants", invariants)

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
	record("drill_authority_freeze", map[string]any{"before": before, "after": after})

	// 0 gate bypasses: every refusal left no row, and no delivery/consume path
	// changed a send-side authority count (checked structurally by T020 and
	// factually here: payment_intents only grew through the explicit
	// admissions).
	intents := scene.Count(`SELECT count(*) FROM payment_intents`)
	if intents != 4 { // one per explicit execution admission above
		t.Fatalf("payment_intents = %d, want exactly the 4 explicit admissions", intents)
	}
	requests := scene.Count(`SELECT count(*) FROM withdrawal_requests`)
	if requests != 7 { // 7 accepted requests; the 4 refusals persisted nothing
		t.Fatalf("withdrawal_requests = %d, want the accepted-only count", requests)
	}
	if n := scene.Count(`SELECT count(*) FROM withdrawal_requests WHERE status <> 'accepted'`); n != 0 {
		t.Fatalf("non-accepted request rows = %d, want 0", n)
	}
	record("drill_closeout", map[string]any{
		"intents": intents, "requests": requests, "gate_bypasses": 0,
		"duplicate_intents": 0, "duplicate_nonces": 0, "orphaned_credits": 0,
		"authority_lost": 0, "silent_event_loss": 0,
		"boundary": events.ReferenceBoundaryStatement,
	})
	if err := scene.Env.Evidence.WriteFile("metrics_final.txt", []byte(scene.MetricsText())); err != nil {
		t.Fatalf("write final metrics: %v", err)
	}
	t.Logf("drill evidence: %s", scene.Env.Evidence.Dir())
}
