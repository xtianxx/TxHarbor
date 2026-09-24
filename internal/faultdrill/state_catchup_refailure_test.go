//go:build fault

// state_catchup_refailure_test.go is the T024 fault-layer acceptance: the
// recovery/catch-up state and a re-failure during catch-up, verified through
// the real service entry.
//
//  1. normal: real publisher + consumer deliver a first event set; every
//     event reaches the reference ledger exactly once;
//  2. dual fault: Kafka suspended and Redis stopped; business continues
//     through the real deposit path and the backlog stays durable;
//  3. recovery: the publisher drains with bounded batches while the consumer
//     catches up from durable PostgreSQL progress; the delivery surface
//     reports the backlog honestly (never real-time) until it clears;
//  4. re-failure during catch-up: the drill safely re-pauses (0 loss,
//     0 duplicate effects, progress resumable), then a final recovery applies
//     every event exactly once;
//  5. cache/limiter recovery is lazy and observable: the limiter leaves the
//     unavailable state and a new create is admitted again.
package faultdrill

import (
	"context"
	"testing"
	"time"
)

// TestCatchupStateAndRefailure is the T024 acceptance.
func TestCatchupStateAndRefailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()

	// One-event publish cycles so the mid-catch-up re-failure window is wide
	// enough to be deterministic.
	scene := StartScene(t, ctx, SceneOptions{
		StartRuntimes:  true,
		PublisherBatch: "1",
	})
	record := func(kind string, data any) {
		t.Helper()
		if err := scene.Env.Evidence.Record(kind, data); err != nil {
			t.Fatalf("record evidence %s: %v", kind, err)
		}
	}

	// --- phase 1: normal ------------------------------------------------------
	const initialDeposits = 3
	for i := 0; i < initialDeposits; i++ {
		scene.ProbeDeposit(int64(4000 + i))
	}
	scene.WaitEventDelivered()
	initialPublished := scene.Count(`SELECT count(*) FROM outbox_events WHERE publish_state = 'published'`)
	assertLedgerExactlyOnce(t, ctx, scene.Env, int(initialPublished))
	record("phase1_assessment", map[string]any{"published": initialPublished, "loss": 0, "duplicate_effects": 0})

	// --- phase 2: dual fault, business continues ------------------------------
	scene.ApplyState(StateDual)
	scene.WaitDependency("redis", false)
	scene.WaitDependency("kafka", false)
	const outageDeposits = 8
	for i := 0; i < outageDeposits; i++ {
		scene.ProbeDeposit(int64(4100 + i))
	}
	outage := scene.WaitOutbox(120*time.Second, "outage backlog", func(totals OutboxTotals) bool {
		return totals.Pending >= outageDeposits
	})
	record("phase2_assessment", map[string]any{
		"pending": outage.Pending, "published": outage.Published,
		"oldest_age_seconds": scene.OldestPendingAge(),
		"loss":               0,
	})

	// --- phase 3: recovery with an honest backlog posture ---------------------
	scene.ApplyState(StateCatchup)
	scene.WaitDependency("redis", true)
	scene.WaitDependency("kafka", true)
	if err := WaitFor(ctx, 30*time.Second, func() (bool, error) {
		body := scene.DegradationNow()
		delivery, _ := body["event_delivery"].(string)
		if delivery == "live" {
			// The drain won the race: the backlog was already cleared, which
			// is also honest. Accept it only when nothing is pending.
			totals, err := scene.Env.OutboxTotals(ctx)
			if err != nil {
				return false, err
			}
			return totals.Pending == 0, nil
		}
		return delivery == "backlog", nil
	}); err != nil {
		t.Fatalf("backlog posture during catch-up: %v", err)
	}
	record("phase3_backlog_posture", map[string]any{
		"delivery": scene.EventDelivery().Delivery, "pending": scene.EventDelivery().Pending,
	})

	// Catch-up starts for real, then the dependencies fail again mid-flight.
	publishedAtResume := scene.Count(`SELECT count(*) FROM outbox_events WHERE publish_state = 'published'`)
	if err := WaitFor(ctx, 120*time.Second, func() (bool, error) {
		totals, err := scene.Env.OutboxTotals(ctx)
		if err != nil {
			return false, err
		}
		return totals.Published > publishedAtResume && totals.Pending > 0, nil
	}); err != nil {
		t.Fatalf("catch-up did not start: %v", err)
	}
	if err := scene.Env.SuspendDual(ctx); err != nil {
		t.Fatalf("suspend dual mid-catch-up: %v", err)
	}
	refailed, err := scene.Env.OutboxTotals(ctx)
	if err != nil {
		t.Fatalf("outbox totals after re-failure: %v", err)
	}
	if refailed.Blocked != 0 {
		t.Fatalf("blocked rows after re-failure = %d, want 0", refailed.Blocked)
	}
	if refailed.Pending == 0 {
		t.Fatalf("re-failure totals = %+v, want outstanding pending work (safe re-pause)", refailed)
	}
	if ledger := scene.LedgerRows(); ledger > refailed.Published {
		t.Fatalf("ledger rows %d > published %d (duplicate financial effects)", ledger, refailed.Published)
	}
	if rows := scene.ConsumerProgressRows(); rows == 0 {
		t.Fatal("consumer progress disappeared after the re-failure")
	}
	report, err := scene.Env.CollectInvariants(ctx)
	if err != nil {
		t.Fatalf("CollectInvariants: %v", err)
	}
	if report.MissingEventsForUnits != 0 {
		t.Fatalf("committed units without their event = %d, want 0", report.MissingEventsForUnits)
	}
	record("phase4_assessment", map[string]any{
		"pending": refailed.Pending, "published": refailed.Published, "blocked": refailed.Blocked,
		"ledger": scene.LedgerRows(), "progress_rows": scene.ConsumerProgressRows(),
		"loss": 0, "duplicate_effects": 0,
	})

	// --- phase 4: final recovery ----------------------------------------------
	scene.ApplyState(StateCatchup)
	scene.WaitDependency("redis", true)
	scene.WaitDependency("kafka", true)
	scene.WaitEventDelivered()
	final, err := scene.Env.OutboxTotals(ctx)
	if err != nil {
		t.Fatalf("final outbox totals: %v", err)
	}
	if final.Pending != 0 || final.Blocked != 0 || final.Published != final.Total {
		t.Fatalf("final totals = %+v, want a clean drained state", final)
	}
	assertLedgerExactlyOnce(t, ctx, scene.Env, int(final.Total))

	// Cache/limiter recovery: the limiter leaves the unavailable state and a
	// new controllable write is admitted again (lazy, observable recovery).
	if err := WaitFor(ctx, 60*time.Second, func() (bool, error) {
		body := scene.DegradationNow()
		rateLimit, _ := body["rate_limit"].(string)
		return rateLimit == "available" || rateLimit == "recovering", nil
	}); err != nil {
		t.Fatalf("limiter did not recover: %v", err)
	}
	recovered := scene.CreateFresh("catchup-recovered")
	if got := scene.QueryVerdict(recovered.ID); got != VerdictContinue {
		t.Fatalf("recovered query verdict = %q, want continue", got)
	}
	body := scene.DegradationNow()
	if rateLimit, _ := body["rate_limit"].(string); rateLimit != "available" && rateLimit != "recovering" {
		t.Fatalf("rate limit posture = %q, want a recovered posture", rateLimit)
	}
	if cache, _ := body["cache"].(string); cache != "enabled" {
		t.Fatalf("cache posture = %q, want enabled after recovery", cache)
	}
	record("final_assessment", map[string]any{
		"total_events": final.Total, "loss": 0, "duplicate_effects": 0,
		"pending": 0, "blocked": 0, "progress_rows": scene.ConsumerProgressRows(),
		"rate_limit": body["rate_limit"], "cache": body["cache"],
		"note": "catch-up time is measured, threshold 待测",
	})
	invariants, err := scene.Env.CollectInvariants(ctx)
	if err != nil {
		t.Fatalf("CollectInvariants: %v", err)
	}
	AssertInvariants(t, invariants)
	record("t024_invariants", invariants)
}

// WaitLogCheckpointPast waits until the 003 log stream committed coverage past
// height, so the 004 deposit stream can attempt the unit (the T090 pause drill
// needs the attempt before releasing the outbox lock).
func (s *Scene) WaitLogCheckpointPast(height uint64) {
	s.T.Helper()
	if err := WaitFor(s.Ctx, 120*time.Second, func() (bool, error) {
		var next uint64
		if err := s.Pool.QueryRow(s.Ctx,
			`SELECT next_block FROM log_checkpoint WHERE chain_id = $1`, SceneChainID).Scan(&next); err != nil {
			return false, err
		}
		return next > height, nil
	}); err != nil {
		s.T.Fatalf("log checkpoint past height %d: %v%s", height, err, s.diagnostics())
	}
}

// OldestPendingAge reads the measured oldest pending wait from the outbox (a
// real value, never fabricated).
func (s *Scene) OldestPendingAge() float64 {
	s.T.Helper()
	var age float64
	if err := s.Pool.QueryRow(s.Ctx, `SELECT coalesce(extract(epoch FROM now() - min(created_at)), 0)
		FROM outbox_events WHERE publish_state = 'pending'`).Scan(&age); err != nil {
		s.T.Fatalf("oldest pending age: %v", err)
	}
	return age
}
