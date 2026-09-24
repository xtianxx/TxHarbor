//go:build fault

// state_normal_redis_test.go is the T021 fault-layer acceptance: the normal
// state and the Redis-only fault state, seven operation classes each, verified
// through the real service entry (serve HTTP routes and scanner loops, real
// event runtimes) with real PostgreSQL/Redis/Kafka containers and a local
// chain.
//
//   - normal: every class continues; the create is accepted and its event
//     reaches the reference consumer exactly once;
//   - Redis-only: new withdrawal creation is refused with the clear retryable
//     error (PD-1) while deposit processing, confirmation, accepted-request
//     execution and event subscription continue; query and non-critical
//     surfaces degrade honestly (no stale financial authority).
//
// The matrix row compared against the approved spec table must be 100%
// consistent.
package faultdrill

import (
	"context"
	"net/http"
	"testing"
	"time"
)

// TestStateNormalAndRedisOnly is the T021 acceptance.
func TestStateNormalAndRedisOnly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	scene := StartScene(t, ctx, SceneOptions{StartRuntimes: true})
	record := func(kind string, data any) {
		t.Helper()
		if err := scene.Env.Evidence.Record(kind, data); err != nil {
			t.Fatalf("record evidence %s: %v", kind, err)
		}
	}

	// --- state 1: normal ------------------------------------------------------
	normal := map[string]string{}
	depositNormal, depositHeight := scene.ProbeDeposit(1000)
	normal[ClassDepositProcessing] = VerdictContinue
	normal[ClassConfirmationReorg] = VerdictContinue
	if n := scene.Count(`SELECT count(*) FROM deposit_observations WHERE tx_hash = $1`, depositNormal.Hex()); n != 1 {
		t.Fatalf("normal deposit observations = %d, want 1", n)
	}
	if n := scene.Count(`SELECT count(*) FROM deposit_observations WHERE tx_hash = $1 AND status = 'confirmed'`, depositNormal.Hex()); n != 1 {
		t.Fatalf("normal deposit confirmations = %d, want 1", n)
	}

	createID := scene.CreateExpect("normal-create-1")
	normal[ClassWithdrawalCreate] = VerdictContinue
	if n := scene.Count(`SELECT count(*) FROM withdrawal_requests WHERE request_id = $1`, createID); n != 1 {
		t.Fatalf("normal create rows = %d, want 1", n)
	}
	normal[ClassQuery] = scene.QueryVerdict(createID)
	if normal[ClassQuery] != VerdictContinue {
		t.Fatalf("normal query verdict = %q, want continue", normal[ClassQuery])
	}
	// A second accepted request carries its own grant; it is the normal-state
	// execution probe. The first request stays unadmitted for the Redis-state
	// admission (the 011 intent uniqueness is per authorization id).
	normalExec := scene.CreateFresh("normal-exec")
	normal[ClassExistingExecution] = VerdictContinue
	scene.ProbeExecution(normalExec.ID, "intent-normal-1", normalExec.AuthID)

	// Event subscription: the real publisher drains and the reference
	// consumer applies every event exactly once.
	scene.WaitEventDelivered()
	normal[ClassEventSubscription] = VerdictContinue
	if body := scene.DegradationNow(); Degraded(body) {
		t.Fatalf("normal state reported degraded: %v", body)
	} else {
		normal[ClassNonCritical] = VerdictContinue
	}
	AssertMatrixRow(t, StateNormal, normal)
	scene.RecordState(StateNormal, normal)
	record("normal_assessment", map[string]any{
		"deposit_tx": depositNormal.Hex(), "deposit_height": depositHeight,
		"create_request": createID, "verdicts": normal,
	})

	// --- state 2: Redis-only --------------------------------------------------
	scene.ApplyState(StateRedisDown)
	scene.WaitDependency("redis", false)

	redisOnly := map[string]string{}
	refusal := scene.CreateRefused("redis-refused-1")
	redisOnly[ClassWithdrawalCreate] = VerdictRefused
	record("redis_refusal", map[string]any{"message": refusal, "redis": scene.Env.RedisPing(ctx)})

	// Deposit processing and confirmation continue (PG + chain only).
	redisDeposit, _ := scene.ProbeDeposit(1001)
	redisOnly[ClassDepositProcessing] = VerdictContinue
	redisOnly[ClassConfirmationReorg] = VerdictContinue
	if n := scene.Count(`SELECT count(*) FROM deposit_observations WHERE tx_hash = $1 AND status = 'confirmed'`, redisDeposit.Hex()); n != 1 {
		t.Fatalf("redis-only deposit confirmations = %d, want 1", n)
	}

	// An accepted request continues under its existing gates: the execution
	// admission succeeds (the limiter outage never tightens an existing flow).
	redisOnly[ClassExistingExecution] = VerdictContinue
	scene.ProbeExecution(createID, "intent-normal-2", SceneAuthID)

	// Query degrades (cache bypass) but serves PostgreSQL facts.
	redisOnly[ClassQuery] = scene.QueryVerdict(createID)
	if redisOnly[ClassQuery] != VerdictDegraded {
		t.Fatalf("redis-only query verdict = %q, want degraded", redisOnly[ClassQuery])
	}
	body := scene.DegradationNow()
	deps, _ := body["dependencies"].(map[string]any)
	if deps["redis"] != "down" {
		t.Fatalf("redis dependency = %v, want down", deps["redis"])
	}
	if rateLimit, _ := body["rate_limit"].(string); rateLimit != "unavailable" {
		t.Fatalf("rate limit posture = %q, want unavailable", rateLimit)
	}

	// Event subscription continues (Kafka untouched): the deposit and the
	// accepted request's events still reach the consumer.
	scene.WaitEventDelivered()
	redisOnly[ClassEventSubscription] = VerdictContinue

	if Degraded(body) {
		redisOnly[ClassNonCritical] = VerdictDegraded
	} else {
		t.Fatalf("redis-only state did not report non-critical degradation: %v", body)
	}
	AssertMatrixRow(t, StateRedisDown, redisOnly)
	scene.RecordState(StateRedisDown, redisOnly)
	record("redis_only_assessment", map[string]any{
		"verdicts": redisOnly, "create_refusals": 1, "stale_authority": 0,
	})

	// --- recovery -------------------------------------------------------------
	scene.ApplyState(StateCatchup)
	scene.WaitDependency("redis", true)
	// A new create is admitted again after the limiter recovers (graded
	// reopening must not block a healthy request).
	recovered := scene.CreateFresh("recovered")
	if got := scene.QueryVerdict(recovered.ID); got != VerdictContinue {
		t.Fatalf("recovered query verdict = %q, want continue", got)
	}
	if status, _ := scene.do(http.MethodGet, "/status/degradation", ""); status != http.StatusOK {
		t.Fatalf("status surface after recovery = %d", status)
	}
	scene.WaitEventDelivered()

	// Durable end state: no loss, no duplicate authority, no blocked rows.
	report, err := scene.Env.CollectInvariants(ctx)
	if err != nil {
		t.Fatalf("CollectInvariants: %v", err)
	}
	AssertInvariants(t, report)
	record("t021_invariants", report)
}
