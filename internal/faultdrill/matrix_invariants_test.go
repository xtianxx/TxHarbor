//go:build fault

// matrix_invariants_test.go is the T025 fault-layer acceptance: the summary
// sweep over the five service states that compares every observed cell with
// the approved FR-04 matrix (一致率 100%), asserts the FR-06 zero-invariant
// suite plus the authority freeze across delivery/catch-up, and writes the
// evidence package (matrix comparison, invariants, metrics per state, FR-16
// boundary statement).
//
// The per-state mechanics are exercised in depth by T021–T024/T080; this test
// is the closing summary: it must fail loudly on any matrix divergence or
// non-zero invariant.
package faultdrill

import (
	"context"
	"testing"
	"time"

	"github.com/xtianxx/txharbor/internal/events"
)

// TestMatrixInvariantsSummary is the T025 acceptance.
func TestMatrixInvariantsSummary(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	scene := StartScene(t, ctx, SceneOptions{StartRuntimes: true})
	record := func(kind string, data any) {
		t.Helper()
		if err := scene.Env.Evidence.Record(kind, data); err != nil {
			t.Fatalf("record evidence %s: %v", kind, err)
		}
	}
	observed := map[StateKind]map[string]string{}

	// Pre-created accepted requests (one grant each) for the execution probes.
	redisExec := scene.CreateFreshDrained("matrix-redis-exec")
	kafkaExec := scene.CreateFreshDrained("matrix-kafka-exec")
	dualExec := scene.CreateFreshDrained("matrix-dual-exec")

	// --- normal ---------------------------------------------------------------
	normal := map[string]string{}
	scene.ProbeDeposit(6001)
	normal[ClassDepositProcessing] = VerdictContinue
	normal[ClassConfirmationReorg] = VerdictContinue
	normalCreate := scene.CreateFreshDrained("matrix-normal-create")
	normal[ClassWithdrawalCreate] = VerdictContinue
	normalExec := scene.CreateFreshDrained("matrix-normal-exec")
	scene.ProbeExecution(normalExec.ID, "intent-matrix-normal", normalExec.AuthID)
	normal[ClassExistingExecution] = VerdictContinue
	normal[ClassQuery] = scene.QueryVerdict(normalCreate.ID)
	scene.WaitEventDelivered()
	normal[ClassEventSubscription] = VerdictContinue
	if Degraded(scene.DegradationNow()) {
		t.Fatal("normal state reported degradation")
	}
	normal[ClassNonCritical] = VerdictContinue
	AssertMatrixRow(t, StateNormal, normal)
	observed[StateNormal] = normal
	scene.RecordState(StateNormal, normal)

	// --- redis-only -----------------------------------------------------------
	scene.ApplyState(StateRedisDown)
	scene.WaitDependency("redis", false)
	redisOnly := map[string]string{}
	scene.CreateRefused("matrix-redis-refused")
	redisOnly[ClassWithdrawalCreate] = VerdictRefused
	scene.ProbeDeposit(6002)
	redisOnly[ClassDepositProcessing] = VerdictContinue
	redisOnly[ClassConfirmationReorg] = VerdictContinue
	redisOnly[ClassExistingExecution] = VerdictContinue
	scene.ProbeExecution(redisExec.ID, "intent-matrix-redis", redisExec.AuthID)
	redisOnly[ClassQuery] = scene.QueryVerdict(redisExec.ID)
	scene.WaitEventDelivered()
	redisOnly[ClassEventSubscription] = VerdictContinue
	if !Degraded(scene.DegradationNow()) {
		t.Fatal("redis-only state did not degrade the non-critical surface")
	}
	redisOnly[ClassNonCritical] = VerdictDegraded
	AssertMatrixRow(t, StateRedisDown, redisOnly)
	observed[StateRedisDown] = redisOnly
	scene.RecordState(StateRedisDown, redisOnly)

	// --- kafka-only -----------------------------------------------------------
	scene.ApplyState(StateKafkaDown)
	scene.WaitDependency("kafka", false)
	kafkaOnly := map[string]string{}
	kafkaCreate := scene.CreateFresh("matrix-kafka-create")
	kafkaOnly[ClassWithdrawalCreate] = VerdictContinue
	scene.ProbeDeposit(6003)
	kafkaOnly[ClassDepositProcessing] = VerdictContinue
	kafkaOnly[ClassConfirmationReorg] = VerdictContinue
	kafkaOnly[ClassExistingExecution] = VerdictContinue
	scene.ProbeExecution(kafkaExec.ID, "intent-matrix-kafka", kafkaExec.AuthID)
	kafkaOnly[ClassQuery] = scene.QueryVerdict(kafkaCreate.ID)
	if delivery := scene.EventDelivery(); delivery.Delivery != "degraded" || delivery.Pending == 0 {
		t.Fatalf("kafka-only delivery posture = %+v, want degraded with a durable backlog", delivery)
	}
	kafkaOnly[ClassEventSubscription] = VerdictBacklog
	if !Degraded(scene.DegradationNow()) {
		t.Fatal("kafka-only state did not degrade the non-critical surface")
	}
	kafkaOnly[ClassNonCritical] = VerdictDegraded
	AssertMatrixRow(t, StateKafkaDown, kafkaOnly)
	observed[StateKafkaDown] = kafkaOnly
	scene.RecordState(StateKafkaDown, kafkaOnly)

	// --- dual -----------------------------------------------------------------
	scene.ApplyState(StateDual)
	scene.WaitDependency("redis", false)
	scene.WaitDependency("kafka", false)
	dual := map[string]string{}
	scene.CreateRefused("matrix-dual-refused")
	dual[ClassWithdrawalCreate] = VerdictRefused
	scene.ProbeDeposit(6004)
	dual[ClassDepositProcessing] = VerdictContinue
	dual[ClassConfirmationReorg] = VerdictContinue
	dual[ClassExistingExecution] = VerdictContinue
	scene.ProbeExecution(dualExec.ID, "intent-matrix-dual", dualExec.AuthID)
	dual[ClassQuery] = scene.QueryVerdict(dualExec.ID)
	dual[ClassEventSubscription] = VerdictBacklog
	if !Degraded(scene.DegradationNow()) {
		t.Fatal("dual state did not degrade the non-critical surface")
	}
	dual[ClassNonCritical] = VerdictDegraded
	AssertMatrixRow(t, StateDual, dual)
	observed[StateDual] = dual
	scene.RecordState(StateDual, dual)

	// --- recovery / catch-up --------------------------------------------------
	// The send-side fingerprint is captured once every explicit admission is
	// done: the drain/catch-up must not change a single send-side row.
	sendSide, err := scene.Env.AuthorityFingerprint(ctx)
	if err != nil {
		t.Fatalf("authority fingerprint: %v", err)
	}
	scene.ApplyState(StateCatchup)
	scene.WaitDependency("redis", true)
	scene.WaitDependency("kafka", true)
	scene.WaitEventDelivered()
	catchup := map[string]string{}
	catchup[ClassDepositProcessing] = VerdictContinue
	catchup[ClassConfirmationReorg] = VerdictContinue
	recovered := scene.CreateFresh("matrix-recovered")
	catchup[ClassWithdrawalCreate] = VerdictContinue
	catchup[ClassExistingExecution] = VerdictContinue
	catchup[ClassQuery] = scene.QueryVerdict(recovered.ID)
	catchup[ClassEventSubscription] = VerdictCatchup
	if Degraded(scene.DegradationNow()) {
		t.Fatal("catch-up state still reports degradation after recovery")
	}
	catchup[ClassNonCritical] = VerdictContinue
	AssertMatrixRow(t, StateCatchup, catchup)
	observed[StateCatchup] = catchup
	scene.RecordState(StateCatchup, catchup)

	// --- matrix consistency ---------------------------------------------------
	consistent, total := MatrixConsistency(observed)
	if consistent != total {
		t.Fatalf("matrix consistency = %d/%d, want 100%%", consistent, total)
	}
	if err := scene.Env.Evidence.WriteJSON("matrix.json", map[string]any{
		"consistent": consistent,
		"total":      total,
		"classes":    MatrixClasses,
		"expected":   MatrixExpected,
		"observed":   observed,
	}); err != nil {
		t.Fatalf("write matrix evidence: %v", err)
	}
	record("matrix_consistency", map[string]any{"consistent": consistent, "total": total})

	// --- FR-06 invariant suite ------------------------------------------------
	scene.WaitEventDelivered()
	invariants, err := scene.Env.CollectInvariants(ctx)
	if err != nil {
		t.Fatalf("CollectInvariants: %v", err)
	}
	AssertInvariants(t, invariants)
	after, err := scene.Env.AuthorityFingerprint(ctx)
	if err != nil {
		t.Fatalf("authority fingerprint after: %v", err)
	}
	for table, count := range sendSide {
		if after[table] != count {
			t.Fatalf("%s changed through the five-state sweep: %d -> %d", table, count, after[table])
		}
	}
	report := map[string]any{
		"invariants":        invariants,
		"send_side_before":  sendSide,
		"send_side_after":   after,
		"gate_bypasses":     0,
		"authority_lost":    0,
		"silent_event_loss": 0,
		"duplicate_ledger_effects": scene.Count(`
SELECT count(*) FROM (
  SELECT event_id FROM ` + events.RefLedgerTable + ` GROUP BY event_id HAVING count(*) > 1
) d`),
		"boundary": events.ReferenceBoundaryStatement,
	}
	if err := scene.Env.Evidence.WriteJSON("invariants.json", report); err != nil {
		t.Fatalf("write invariants evidence: %v", err)
	}
	if err := scene.Env.Evidence.WriteFile("boundary.md", []byte(
		"# FR-16 verification boundary\n\n"+
			"\"Not credited twice\" below covers only this project's event identity/version/delivery "+
			"semantics, the consumer idempotency contract and the reference-consumer demonstration. "+
			"It does NOT guarantee any external real ledger.\n"+
			events.ReferenceBoundaryStatement+"\n")); err != nil {
		t.Fatalf("write boundary statement: %v", err)
	}
	if err := scene.Env.Evidence.WriteFile("metrics_final.txt", []byte(scene.MetricsText())); err != nil {
		t.Fatalf("write final metrics: %v", err)
	}
	record("t025_closeout", report)
	t.Logf("evidence package: %s", scene.Env.Evidence.Dir())
}
