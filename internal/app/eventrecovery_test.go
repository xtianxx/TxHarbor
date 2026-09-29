// eventrecovery_test.go is the T033/T034 unit layer for the events-side 015
// gate assembly: the missing-configuration modes of assembleEventsRecovery,
// the two-phase checkpoint mechanics of the gated publisher (claim and settle)
// and the Effect checkpoint of the gated consumer, plus the closed
// refusal_class surface both entries expose. None of these tests need a
// control store or a broker; the real-control-store default-deny behavior of
// the entry points is the T026 integration layer
// (recovery_isolation_integration_test.go).
package app

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/events"
	"github.com/xtianxx/txharbor/internal/recovery"
)

// eventsScriptedAdmitter is a scripted eventsGateAdmitter: it records the
// admission requests and returns pre-seeded decisions/errors in order (a
// missing script entry admits).
type eventsScriptedAdmitter struct {
	mu        sync.Mutex
	requests  []recovery.GateRequest
	decisions []recovery.GateDecision
	errs      []error
}

func (a *eventsScriptedAdmitter) Admit(_ context.Context, req recovery.GateRequest) (recovery.GateDecision, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.requests = append(a.requests, req)
	i := len(a.requests) - 1
	if i < len(a.decisions) {
		var err error
		if i < len(a.errs) {
			err = a.errs[i]
		}
		return a.decisions[i], err
	}
	return recovery.GateDecision{Allowed: true, Capability: req.Capability, ScopeHash: req.ScopeHash}, nil
}

func (a *eventsScriptedAdmitter) actions() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]string, 0, len(a.requests))
	for _, req := range a.requests {
		out = append(out, req.Action)
	}
	return out
}

// stubPhasePublisher counts the two-phase publisher calls the gated wrapper
// drives; the counters prove a refused checkpoint never reached the action.
type stubPhasePublisher struct {
	mu      sync.Mutex
	claims  int
	settles int
}

func (p *stubPhasePublisher) ClaimBatch(context.Context) ([]events.OutboxRecord, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.claims++
	return []events.OutboxRecord{{OutboxID: 1}}, nil
}

func (p *stubPhasePublisher) PublishClaimed(context.Context, []events.OutboxRecord) (events.PublishOutcome, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.settles++
	return events.PublishOutcome{Claimed: 1, Acked: 1}, nil
}

func (p *stubPhasePublisher) RefreshGauges(context.Context) error { return nil }

func (p *stubPhasePublisher) counts() (claims, settles int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.claims, p.settles
}

func refusedEvent(capability recovery.Capability) recovery.GateDecision {
	return recovery.GateDecision{
		Allowed:      false,
		Capability:   capability,
		RefusalClass: recovery.RefusalIsolationUnproven,
		Reason:       "isolation item old_writers_stopped is pending",
	}
}

// TestAssembleEventsRecoveryConfigModes pins the explicit missing-configuration
// behavior of both events entries: no control DSN means normal mode (nil
// wiring, no gate), a bound instance without the control store refuses, and a
// configured control store without the required gate TTL refuses by key name.
func TestAssembleEventsRecoveryConfigModes(t *testing.T) {
	ctx := context.Background()

	t.Run("no_control_dsn_is_normal_mode", func(t *testing.T) {
		env := fullServeEnv("127.0.0.1:0")
		cfg, err := config.Load(fakeEnv(env))
		if err != nil {
			t.Fatalf("config.Load: %v", err)
		}
		wiring, err := assembleEventsRecovery(ctx, cfg, fakeEnv(env), recovery.CapabilityEventPublishing)
		if err != nil {
			t.Fatalf("assembleEventsRecovery: %v", err)
		}
		if wiring != nil {
			t.Fatalf("wiring = %+v, want nil (FR-023 normal mode without %s)", wiring, config.EnvRecoveryControlDSN)
		}
		// A nil wiring passes every admission through: no control store means
		// no recovery instance can be observed.
		if err := wiring.require(ctx, recovery.CapabilityEventPublishing, "claim_batch"); err != nil {
			t.Fatalf("nil wiring must pass through: %v", err)
		}
	})

	t.Run("bound_instance_without_control_dsn_refuses", func(t *testing.T) {
		env := fullServeEnv("127.0.0.1:0")
		env[config.EnvRecoveryInstance] = "1b4e28ba-2fa1-11d2-883f-0016d3cca427"
		cfg, err := config.Load(fakeEnv(env))
		if err != nil {
			t.Fatalf("config.Load: %v", err)
		}
		_, err = assembleEventsRecovery(ctx, cfg, fakeEnv(env), recovery.CapabilityEventConsuming)
		if err == nil {
			t.Fatal("a bound instance without the control store must refuse (fail-closed)")
		}
		for _, want := range []string{config.EnvRecoveryInstance, config.EnvRecoveryControlDSN} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q does not name %s", err.Error(), want)
			}
		}
	})

	t.Run("control_dsn_without_ttl_refuses", func(t *testing.T) {
		env := fullServeEnv("127.0.0.1:0")
		env[config.EnvRecoveryControlDSN] = "postgres://u:p@127.0.0.1:1/control?sslmode=disable"
		cfg, err := config.Load(fakeEnv(env))
		if err != nil {
			t.Fatalf("config.Load: %v", err)
		}
		_, err = assembleEventsRecovery(ctx, cfg, fakeEnv(env), recovery.CapabilityEventPublishing)
		if err == nil {
			t.Fatal("a configured control store without the gate TTL must refuse (the gate has no default)")
		}
		if !strings.Contains(err.Error(), config.EnvRecoveryGateTTL) {
			t.Errorf("error %q does not name %s", err.Error(), config.EnvRecoveryGateTTL)
		}
	})
}

// TestGatedOutboxPublisherRefusesBeforeClaim pins T033's first checkpoint: a
// refused claim admission returns the closed refusal class and ClaimBatch is
// never called (no claim, no settle, no outbox progress).
func TestGatedOutboxPublisherRefusesBeforeClaim(t *testing.T) {
	admitter := &eventsScriptedAdmitter{decisions: []recovery.GateDecision{
		refusedEvent(recovery.CapabilityEventPublishing),
	}}
	wiring := &eventsRecoveryWiring{gate: admitter, scope: "test-scope", actor: "test-actor"}
	inner := &stubPhasePublisher{}
	pub := &gatedOutboxPublisher{inner: inner, gate: wiring}

	_, err := pub.PublishOnce(context.Background())
	var refused *eventsGateRefusedError
	if !errors.As(err, &refused) {
		t.Fatalf("PublishOnce error = %v, want *eventsGateRefusedError", err)
	}
	if refused.Capability != recovery.CapabilityEventPublishing {
		t.Errorf("capability = %q, want %q", refused.Capability, recovery.CapabilityEventPublishing)
	}
	if refused.RefusalClass != recovery.RefusalIsolationUnproven {
		t.Errorf("refusal_class = %q, want %q", refused.RefusalClass, recovery.RefusalIsolationUnproven)
	}
	if !strings.Contains(err.Error(), "refusal_class=") {
		t.Errorf("error %q does not expose the refusal_class marker", err.Error())
	}
	claims, settles := inner.counts()
	if claims != 0 || settles != 0 {
		t.Fatalf("claim=%d settle=%d, want 0/0 (a refusal must not claim or settle)", claims, settles)
	}
	if got := admitter.actions(); len(got) != 1 || got[0] != "claim_batch" {
		t.Fatalf("admission actions = %v, want [claim_batch]", got)
	}
}

// TestGatedOutboxPublisherRefusesBeforeSettle pins T033's second checkpoint: a
// batch admitted for claiming is still not published or settled when the
// settle admission refuses; the already-claimed rows stay untouched (the
// bounded lease returns them to pending) and no outbox progress advances.
func TestGatedOutboxPublisherRefusesBeforeSettle(t *testing.T) {
	admitter := &eventsScriptedAdmitter{decisions: []recovery.GateDecision{
		{Allowed: true},
		refusedEvent(recovery.CapabilityEventPublishing),
	}}
	wiring := &eventsRecoveryWiring{gate: admitter, scope: "test-scope", actor: "test-actor"}
	inner := &stubPhasePublisher{}
	pub := &gatedOutboxPublisher{inner: inner, gate: wiring}

	outcome, err := pub.PublishOnce(context.Background())
	var refused *eventsGateRefusedError
	if !errors.As(err, &refused) {
		t.Fatalf("PublishOnce error = %v, want *eventsGateRefusedError", err)
	}
	if refused.Action != "settle_batch" {
		t.Errorf("action = %q, want settle_batch", refused.Action)
	}
	if outcome.Claimed != 1 {
		t.Errorf("outcome.Claimed = %d, want 1 (the refused batch was claimed once)", outcome.Claimed)
	}
	claims, settles := inner.counts()
	if claims != 1 || settles != 0 {
		t.Fatalf("claim=%d settle=%d, want 1/0 (a settle refusal must not publish or settle)", claims, settles)
	}
	if got := admitter.actions(); len(got) != 2 || got[0] != "claim_batch" || got[1] != "settle_batch" {
		t.Fatalf("admission actions = %v, want [claim_batch settle_batch]", got)
	}
}

// TestGatedOutboxPublisherAllowedCycle pins the pass-through of both
// checkpoints: an allowed cycle claims once, settles once and reports the
// batch.
func TestGatedOutboxPublisherAllowedCycle(t *testing.T) {
	wiring := &eventsRecoveryWiring{gate: &eventsScriptedAdmitter{}, scope: "test-scope"}
	inner := &stubPhasePublisher{}
	pub := &gatedOutboxPublisher{inner: inner, gate: wiring}

	outcome, err := pub.PublishOnce(context.Background())
	if err != nil {
		t.Fatalf("PublishOnce: %v", err)
	}
	if outcome.Claimed != 1 || outcome.Acked != 1 {
		t.Fatalf("outcome = %+v, want claimed=1 acked=1", outcome)
	}
	claims, settles := inner.counts()
	if claims != 1 || settles != 1 {
		t.Fatalf("claim=%d settle=%d, want 1/1", claims, settles)
	}
}

// countingEffect counts Effect applications; the counter must stay zero when
// the gate refuses before the effect.
type countingEffect struct {
	mu sync.Mutex
	n  int
}

func (e *countingEffect) Apply(context.Context, pgx.Tx, events.Envelope) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.n++
	return nil
}

func (e *countingEffect) count() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.n
}

// TestGatedConsumerEffectRefusesBeforeApply pins T034's Effect checkpoint: a
// refusal is returned as the closed refusal class, the underlying effect is
// never applied and the run is cancelled so the T4 transaction rolls back
// without an inbox row or a progress advance.
func TestGatedConsumerEffectRefusesBeforeApply(t *testing.T) {
	admitter := &eventsScriptedAdmitter{decisions: []recovery.GateDecision{
		refusedEvent(recovery.CapabilityEventConsuming),
	}}
	wiring := &eventsRecoveryWiring{gate: admitter, scope: "test-scope", actor: "test-actor"}
	inner := &countingEffect{}
	refusals := 0
	effect := &gatedConsumerEffect{
		inner: inner,
		gate:  wiring,
		onRefusal: func(error) {
			refusals++
		},
	}

	err := effect.Apply(context.Background(), nil, events.Envelope{})
	var refused *eventsGateRefusedError
	if !errors.As(err, &refused) {
		t.Fatalf("Apply error = %v, want *eventsGateRefusedError", err)
	}
	if refused.Capability != recovery.CapabilityEventConsuming {
		t.Errorf("capability = %q, want %q", refused.Capability, recovery.CapabilityEventConsuming)
	}
	if inner.count() != 0 {
		t.Fatalf("the effect was applied %d time(s) after a refusal, want 0", inner.count())
	}
	if refusals != 1 {
		t.Fatalf("onRefusal calls = %d, want 1", refusals)
	}
}

// TestGatedConsumerEffectAppliesWhenAllowed pins the pass-through: an allowed
// admission applies the inner effect exactly once.
func TestGatedConsumerEffectAppliesWhenAllowed(t *testing.T) {
	wiring := &eventsRecoveryWiring{gate: &eventsScriptedAdmitter{}, scope: "test-scope"}
	inner := &countingEffect{}
	effect := &gatedConsumerEffect{inner: inner, gate: wiring, onRefusal: func(error) { t.Error("unexpected refusal") }}

	if err := effect.Apply(context.Background(), nil, events.Envelope{}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if inner.count() != 1 {
		t.Fatalf("effect applications = %d, want 1", inner.count())
	}
}

// refusalPublisher always fails with the configured error.
type refusalPublisher struct{ err error }

func (p refusalPublisher) PublishOnce(context.Context) (events.PublishOutcome, error) {
	return events.PublishOutcome{}, p.err
}

func (refusalPublisher) RefreshGauges(context.Context) error { return nil }

// TestRunEventPublisherLoopStopsOnGateRefusal pins that a gate refusal is
// terminal for the entry (the loop returns it instead of retrying), while
// generic cycle errors keep the loop running (existing unit tests).
func TestRunEventPublisherLoopStopsOnGateRefusal(t *testing.T) {
	want := &eventsGateRefusedError{
		Capability:   recovery.CapabilityEventPublishing,
		Action:       "claim_batch",
		RefusalClass: recovery.RefusalNoRelease,
		Reason:       "no release decision exists",
	}
	done := make(chan error, 1)
	go func() {
		done <- runEventPublisherLoop(context.Background(), refusalPublisher{err: want},
			eventPublisherLoopOptions{PollInterval: time.Millisecond})
	}()
	select {
	case err := <-done:
		var got *eventsGateRefusedError
		if !errors.As(err, &got) || got != want {
			t.Fatalf("loop error = %v, want the gate refusal %v", err, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("loop did not stop on a gate refusal")
	}
}

// TestEventsGateRefusedErrorClosedClass pins the refusal surface both entries
// print: the closed-set class is exposed under the `refusal_class` marker.
func TestEventsGateRefusedErrorClosedClass(t *testing.T) {
	err := &eventsGateRefusedError{
		Capability:   recovery.CapabilityEventConsuming,
		Action:       "apply_effect",
		RefusalClass: recovery.RefusalInstanceMismatch,
		Reason:       "a recovery instance is open and this admission is not bound to it",
	}
	text := err.Error()
	if !strings.Contains(text, "refusal_class="+string(recovery.RefusalInstanceMismatch)) {
		t.Fatalf("refusal text %q does not expose refusal_class=%s", text, recovery.RefusalInstanceMismatch)
	}
	if !recovery.RefusalInstanceMismatch.Known() {
		t.Fatal("the surfaced class is outside the closed set")
	}
}

// TestEventsRecoveryWiringUnavailableControlStoreFailsClosed pins the
// infrastructure-failure direction: when the evaluation cannot complete (the
// gate reports control_store_unavailable with a cause error), require refuses
// with the closed class instead of passing the action through.
func TestEventsRecoveryWiringUnavailableControlStoreFailsClosed(t *testing.T) {
	admitter := &eventsScriptedAdmitter{
		decisions: []recovery.GateDecision{{
			Allowed:      false,
			Capability:   recovery.CapabilityEventPublishing,
			RefusalClass: recovery.RefusalControlStoreUnavailable,
			Reason:       "control store unavailable",
		}},
		errs: []error{errors.New("pool exhausted")},
	}
	wiring := &eventsRecoveryWiring{gate: admitter, scope: "test-scope"}
	err := wiring.require(context.Background(), recovery.CapabilityEventPublishing, "claim_batch")
	var refused *eventsGateRefusedError
	if !errors.As(err, &refused) {
		t.Fatalf("require error = %v, want *eventsGateRefusedError", err)
	}
	if refused.RefusalClass != recovery.RefusalControlStoreUnavailable {
		t.Fatalf("refusal_class = %q, want %q (fail-closed on an unevaluable admission)",
			refused.RefusalClass, recovery.RefusalControlStoreUnavailable)
	}
	if !refused.RefusalClass.Known() {
		t.Fatal("the surfaced class is outside the closed set")
	}
}
