// eventpublisher_command_test.go is the T034 unit layer: the fail-closed
// command gates (configuration, feature switch), the bounded loop cadence with
// its audit entry and graceful stop, and the read-only/cutover-filtered shape
// of the reconciliation probes. The publisher runtime itself is exercised
// against real PostgreSQL/Kafka in the events integration layers (T036/T037).
package app

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/events"
)

// fullEventsEnv is a complete, valid 013 configuration (fail-closed gate).
func fullEventsEnv() map[string]string {
	env := fullServeEnv("127.0.0.1:0")
	env[config.EnvEventsEnabled] = "true"
	env[config.EnvKafkaBrokers] = "127.0.0.1:9092"
	env[config.EnvRedisAddr] = "127.0.0.1:6379"
	env[config.EnvRateLimitNewWithdrawal] = "10/20"
	env[config.EnvRateLimitWrite] = "50/100"
	env[config.EnvRateLimitQuery] = "100/200"
	env[config.EnvRateLimitOperator] = "5/10"
	env[config.EnvRateLimitRPC] = "20/40"
	env[config.EnvEventsCapacitySoftLimit] = "1000"
	env[config.EnvEventsCapacityHardLimit] = "2000"
	env[config.EnvEventsCapacityReserve] = "100"
	env[config.EnvEventsCapacityRetention] = "24h"
	env[config.EnvEventsCapacityMaxShutdown] = "1h"
	env[config.EnvEventsCapacityDrainTarget] = "30m"
	return env
}

// TestEventPublisherCommandRefusesIncompleteConfig pins the configuration
// gate: the refusal happens before any pool or broker connection is opened.
func TestEventPublisherCommandRefusesIncompleteConfig(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := EventPublisher(context.Background(), nil, Deps{
		Getenv: fakeEnv(map[string]string{}),
		Stdout: &stdout,
		Stderr: &stderr,
	})
	if code != 1 {
		t.Fatalf("exit = %d, want 1; stderr=%s", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "configuration error") {
		t.Fatalf("stderr = %q, want the configuration refusal", stderr.String())
	}
}

// TestEventPublisherCommandRefusesDisabledEvents pins the feature gate.
func TestEventPublisherCommandRefusesDisabledEvents(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := EventPublisher(context.Background(), nil, Deps{
		Getenv: fakeEnv(fullServeEnv("127.0.0.1:0")),
		Stdout: &stdout,
		Stderr: &stderr,
	})
	if code != 1 {
		t.Fatalf("exit = %d, want 1; stderr=%s", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "events are disabled") {
		t.Fatalf("stderr = %q, want the disabled-events refusal", stderr.String())
	}
}

// TestEventPublisherCommandUsage pins the help surface (no configuration, no
// database, no broker needed).
func TestEventPublisherCommandUsage(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := EventPublisher(context.Background(), []string{"--help"}, Deps{
		Getenv: fakeEnv(map[string]string{}),
		Stdout: &stdout,
		Stderr: &stderr,
	})
	if code != 0 {
		t.Fatalf("exit = %d, want 0; stderr=%s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "usage: txharbor event-publisher") ||
		!strings.Contains(stdout.String(), "at-least-once") {
		t.Fatalf("usage = %q", stdout.String())
	}
}

// countingPublisher is a scripted outboxPublisher: it counts calls and can
// fail a cycle to prove the loop keeps running.
type countingPublisher struct {
	mu        sync.Mutex
	publishes int
	refreshes int
}

func (p *countingPublisher) PublishOnce(context.Context) (events.PublishOutcome, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.publishes++
	return events.PublishOutcome{Claimed: 1, Acked: 1}, nil
}

func (p *countingPublisher) RefreshGauges(context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.refreshes++
	return nil
}

func (p *countingPublisher) counts() (int, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.publishes, p.refreshes
}

// countingAudit counts reconciliation passes.
type countingAudit struct {
	mu   sync.Mutex
	runs int
}

func (a *countingAudit) Run(context.Context) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.runs++
}

func (a *countingAudit) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.runs
}

// TestRunEventPublisherLoopCadenceAndStop pins the loop: bounded cycles on the
// poll cadence, gauge refresh per cycle, an immediate first audit pass, and a
// prompt return when the context is cancelled (graceful stop).
func TestRunEventPublisherLoopCadenceAndStop(t *testing.T) {
	pub := &countingPublisher{}
	audit := &countingAudit{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runEventPublisherLoop(ctx, pub, eventPublisherLoopOptions{
			PollInterval:  5 * time.Millisecond,
			AuditInterval: time.Hour,
			Audit:         audit,
		})
	}()
	time.Sleep(60 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("loop did not stop on cancellation")
	}
	publishes, refreshes := pub.counts()
	if publishes < 2 {
		t.Fatalf("publishes = %d, want >= 2", publishes)
	}
	if refreshes < publishes {
		t.Fatalf("refreshes = %d, want >= publishes (%d)", refreshes, publishes)
	}
	if audit.count() < 1 {
		t.Fatalf("audit runs = %d, want >= 1 (first pass is immediate)", audit.count())
	}
}

// TestRunEventPublisherLoopSurvivesCycleErrors pins that a failed cycle is
// logged and never stops the loop.
func TestRunEventPublisherLoopSurvivesCycleErrors(t *testing.T) {
	pub := &erroringPublisher{err: context.DeadlineExceeded}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runEventPublisherLoop(ctx, pub, eventPublisherLoopOptions{PollInterval: 5 * time.Millisecond})
	}()
	time.Sleep(40 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("loop did not stop on cancellation")
	}
	if pub.count() < 2 {
		t.Fatalf("cycles = %d, want >= 2 after errors", pub.count())
	}
}

type erroringPublisher struct {
	mu  sync.Mutex
	err error
	n   int
}

func (p *erroringPublisher) PublishOnce(context.Context) (events.PublishOutcome, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.n++
	return events.PublishOutcome{}, p.err
}

func (p *erroringPublisher) RefreshGauges(context.Context) error { return nil }

func (p *erroringPublisher) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.n
}

// TestEventPublisherProbeShape pins the registered probes: the three source
// domains, read-only SQL filtered to the migration-seeded cutover
// (pre-cutover history legitimately has no emitted event) and bounded by the
// limit parameter. The deposit domain reads through internal/indexer (the
// 004-owned table never crosses the confinement boundary).
func TestEventPublisherProbeShape(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, "postgres://u:p@127.0.0.1:1/db?sslmode=disable")
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	defer pool.Close()
	probes, err := eventPublisherProbes(pool)
	if err != nil {
		t.Fatalf("eventPublisherProbes: %v", err)
	}
	wantKinds := []string{"deposit_observer", "withdrawal_intake", "withdrawal_execution"}
	if len(probes) != len(wantKinds) {
		t.Fatalf("probes = %d, want %d", len(probes), len(wantKinds))
	}
	for i, want := range wantKinds {
		if got := probes[i].SourceKind(); got != want {
			t.Fatalf("probe %d kind = %q, want %q", i, got, want)
		}
		sqlProbe, ok := probes[i].(events.SQLProbe)
		if !ok {
			continue // the deposit probe is the indexer-backed adapter
		}
		sql := sqlProbe.Query
		if !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(sql)), "SELECT") {
			t.Fatalf("%s probe is not a SELECT", want)
		}
		if !strings.Contains(sql, "event_system_state") || !strings.Contains(sql, "cutover_at") {
			t.Fatalf("%s probe is not filtered to the cutover baseline", want)
		}
		if !strings.Contains(sql, "LIMIT $1") {
			t.Fatalf("%s probe is not bounded by the limit parameter", want)
		}
	}
}
