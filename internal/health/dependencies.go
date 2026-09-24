// dependencies.go owns the 013 non-authoritative dependency signals (T017;
// contracts/redis.md §1; FR-04/24; plan D9): Redis and Kafka availability
// probes whose outcome is exposed only as an informational signal
// (`redis_available`/`kafka_available` metrics, degradation annotations, the
// dependency status endpoint). The signals are NEVER read by a funding gate,
// an authorization, an idempotency or a reconciliation decision: those paths
// are PostgreSQL-authoritative and fail-closed on their own (FR-02; the
// no-gate-reference assertion lives in dependencies_test.go).
//
// The readiness aggregate and this signal set are deliberately separate:
// losing Redis or Kafka is a degraded non-critical state, never a
// not-ready verdict for the funding listener.
package health

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

// DependencyProbe names one non-authoritative dependency probe. Probe must be
// bounded by the caller's context; a timeout error is an unavailable signal,
// never a fatal startup condition.
type DependencyProbe struct {
	Name  string
	Probe func(ctx context.Context) error
}

// DependencySignals carries the last observed availability of the
// non-authoritative dependencies. The zero value is not usable; build one
// with NewDependencySignals.
//
// A name is "known" only after its first probe; before that the signal is
// explicitly unknown and MUST NOT be read as available (the status surfaces
// report "unknown").
type DependencySignals struct {
	mu    sync.RWMutex
	known map[string]bool
}

// NewDependencySignals creates an empty signal set: every dependency is
// unknown until its first probe result arrives.
func NewDependencySignals() *DependencySignals {
	return &DependencySignals{known: make(map[string]bool)}
}

// Set records one probe result.
func (s *DependencySignals) Set(name string, available bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.known[name] = available
}

// Availability returns the last observed availability and whether any probe
// result exists yet.
func (s *DependencySignals) Availability(name string) (bool, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	available, ok := s.known[name]
	return available, ok
}

// Names returns the observed dependency names in deterministic order.
func (s *DependencySignals) Names() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	names := make([]string, 0, len(s.known))
	for name := range s.known {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Degraded reports whether any observed dependency is currently unavailable.
// It is a non-critical degradation signal only: it never gates funding.
func (s *DependencySignals) Degraded() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, available := range s.known {
		if !available {
			return true
		}
	}
	return false
}

// DependencyRunner probes the non-authoritative dependencies on a fixed
// interval with a per-probe timeout and publishes their signals plus the
// observer hook. Unlike Runner it touches no readiness aggregate: a down
// Redis/Kafka is degraded, not not-ready.
type DependencyRunner struct {
	Interval time.Duration
	Timeout  time.Duration
	Probes   []DependencyProbe
	Signals  *DependencySignals
	// Observe, when non-nil, receives every probe result (production wires it
	// to the redis_available/kafka_available gauges).
	Observe func(name string, available bool)
}

// Run probes immediately, then on every tick until ctx is done.
func (r *DependencyRunner) Run(ctx context.Context) {
	r.ProbeOnce(ctx)
	ticker := time.NewTicker(r.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.ProbeOnce(ctx)
		}
	}
}

// ProbeOnce runs every probe exactly once with its own timeout and publishes
// the result. A nil probe function is treated as unavailable (fail-closed
// signal, never a silent "up").
func (r *DependencyRunner) ProbeOnce(ctx context.Context) {
	for _, probe := range r.Probes {
		probeCtx, cancel := context.WithTimeout(ctx, r.Timeout)
		var err error
		if probe.Probe == nil {
			err = fmt.Errorf("dependency %s has no probe", probe.Name)
		} else {
			err = probe.Probe(probeCtx)
		}
		cancel()
		available := err == nil
		if r.Signals != nil {
			r.Signals.Set(probe.Name, available)
		}
		if r.Observe != nil {
			r.Observe(probe.Name, available)
		}
	}
}
