// dependencies_test.go is the T017 evidence: probe/runner behavior
// (up/down/timeout determinable, unknown before first probe, recovery) plus
// the static no-gate-reference assertion: no funding-gate package may import
// the dependency signal surface or reference its symbols, so Redis/Kafka
// availability can never become a funding, authorization, idempotency or
// reconciliation input (FR-02/04; contracts/redis.md §1).
package health

import (
	"context"
	"errors"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestDependencySignalsUnknownUntilProbed pins the fail-closed unknown state:
// a dependency with no probe result is never reported available.
func TestDependencySignalsUnknownUntilProbed(t *testing.T) {
	signals := NewDependencySignals()
	if available, known := signals.Availability("redis"); known || available {
		t.Fatalf("Availability before probe = (%v, %v), want (false, false)", available, known)
	}
	if signals.Degraded() {
		t.Fatal("Degraded() = true before any probe, want false (unknown is not degraded)")
	}

	signals.Set("redis", true)
	if available, known := signals.Availability("redis"); !known || !available {
		t.Fatalf("Availability after up = (%v, %v), want (true, true)", available, known)
	}
	if signals.Degraded() {
		t.Fatal("Degraded() = true with a single healthy dependency, want false")
	}

	signals.Set("redis", false)
	if available, known := signals.Availability("redis"); !known || available {
		t.Fatalf("Availability after down = (%v, %v), want (false, true)", available, known)
	}
	if !signals.Degraded() {
		t.Fatal("Degraded() = false with an unavailable dependency, want true")
	}

	signals.Set("redis", true)
	if signals.Degraded() {
		t.Fatal("Degraded() = true after recovery, want false")
	}
}

// TestDependencyRunnerProbeOnceDeterminesUpDownRecovery drives the runner with
// scripted probes: the signal flips exactly with the probe outcome, including
// the timeout path and a nil probe (fail-closed).
func TestDependencyRunnerProbeOnceDeterminesUpDownRecovery(t *testing.T) {
	var up bool
	timeout := false
	signals := NewDependencySignals()
	var observed []string
	runner := &DependencyRunner{
		Interval: time.Millisecond,
		Timeout:  50 * time.Millisecond,
		Signals:  signals,
		Observe: func(name string, available bool) {
			observed = append(observed, name)
		},
		Probes: []DependencyProbe{
			{Name: "redis", Probe: func(ctx context.Context) error {
				if timeout {
					<-ctx.Done()
					return ctx.Err()
				}
				if !up {
					return errors.New("connection refused")
				}
				return nil
			}},
			{Name: "kafka", Probe: nil},
		},
	}

	ctx := context.Background()
	runner.ProbeOnce(ctx)
	if available, known := signals.Availability("redis"); !known || available {
		t.Fatalf("redis after refused probe = (%v, %v), want (false, true)", available, known)
	}
	if available, known := signals.Availability("kafka"); !known || available {
		t.Fatalf("kafka nil probe = (%v, %v), want (false, true)", available, known)
	}
	if !signals.Degraded() {
		t.Fatal("Degraded() = false after failed probes, want true")
	}

	up = true
	runner.ProbeOnce(ctx)
	if available, _ := signals.Availability("redis"); !available {
		t.Fatal("redis did not recover after a successful probe")
	}

	timeout = true
	runner.ProbeOnce(ctx)
	if available, _ := signals.Availability("redis"); available {
		t.Fatal("redis timeout probe did not mark the dependency unavailable")
	}
	if len(observed) != 6 {
		t.Fatalf("observer calls = %d, want 6 (2 probes x 3 rounds)", len(observed))
	}
}

// TestDependencyRunnerRunStopsWithContext pins the lifecycle: Run probes once
// and returns when the context ends.
func TestDependencyRunnerRunStopsWithContext(t *testing.T) {
	signals := NewDependencySignals()
	runner := &DependencyRunner{
		Interval: time.Hour,
		Timeout:  time.Second,
		Signals:  signals,
		Probes:   []DependencyProbe{{Name: "redis", Probe: func(context.Context) error { return nil }}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { runner.Run(ctx); close(done) }()
	deadline := time.After(2 * time.Second)
	for {
		if _, known := signals.Availability("redis"); known {
			break
		}
		select {
		case <-deadline:
			t.Fatal("initial probe never ran")
		case <-time.After(time.Millisecond):
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop with its context")
	}
}

// gatePackages are the funding-decision packages: authorization, idempotency,
// withdrawal/execution gates, reconciliation and the event runtime. None of
// them may read the non-authoritative dependency signals.
var gatePackages = []string{
	"indexer", "withdrawal", "execution", "nonce", "txlifecycle", "events", "eth",
}

// signalSymbols are the identifiers that make the dependency signal surface
// readable; a gate package referencing any of them would be a violation even
// through a local alias.
var signalSymbols = []string{
	"DependencySignals", "DependencyRunner", "DependencyProbe",
	"redis_available", "kafka_available", "RedisAvailableMetricName", "KafkaAvailableMetricName",
}

// TestFundingGatesNeverReferenceDependencySignals is the T017 static
// assertion: funding-gate production sources neither import internal/health
// nor reference any dependency-signal symbol (FR-02; contracts/redis.md §1).
func TestFundingGatesNeverReferenceDependencySignals(t *testing.T) {
	root := filepath.Join("..", "..")
	for _, pkg := range gatePackages {
		dir := filepath.Join(root, "internal", pkg)
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("ReadDir %s: %v", dir, err)
		}
		found := 0
		for _, entry := range entries {
			name := entry.Name()
			if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}
			found++
			path := filepath.Join(dir, name)
			body, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("ReadFile %s: %v", path, err)
			}
			file, err := parser.ParseFile(token.NewFileSet(), path, body, 0)
			if err != nil {
				t.Fatalf("parse %s: %v", path, err)
			}
			for _, imp := range file.Imports {
				importPath := strings.Trim(imp.Path.Value, `"`)
				if importPath == "github.com/xtianxx/txharbor/internal/health" {
					t.Errorf("%s imports internal/health: funding gates must never read dependency signals", path)
				}
			}
			for _, symbol := range signalSymbols {
				if strings.Contains(string(body), symbol) {
					t.Errorf("%s references %s: funding gates must never read dependency signals", path, symbol)
				}
			}
		}
		if found == 0 {
			t.Errorf("package %s has no production sources; scan is stale", pkg)
		}
	}
}
