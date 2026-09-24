// rpcbudget.go implements the T064 distributed RPC budget overlay
// (contracts/redis.md §4; FR-19; PD-1; research R12).
//
// The per-process bounds (connect/request timeouts, retry limits, concurrency
// caps, error classification) stay the baseline and never depend on Redis.
// The distributed budget is an overlay on top:
//
//   - when the limiter is available, every class passes through the
//     distributed token bucket (a denial is retryable, never a verdict);
//   - when the limiter is unavailable, classes that remain bounded by their
//     per-process baseline continue at a conservative degraded concurrency;
//     classes whose boundedness depends on the distributed budget are safely
//     PAUSED (never silently unbounded) and resume after recovery;
//   - a pause never skips error classification, chain-identity checks or
//     completeness checks: it refuses before the call, it never fabricates a
//     result after one.
package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
)

// RPCClass is the closed outbound-RPC class vocabulary.
type RPCClass string

const (
	// RPCClassRead covers chain-id, header, log, receipt, transaction and
	// block-number reads: bounded per process by the caller's timeout/retry
	// discipline, so it may continue degraded.
	RPCClassRead RPCClass = "read"
	// RPCClassSend covers eth_sendRawTransaction: the class that must never
	// become unbounded, so an unavailable distributed budget pauses it.
	RPCClassSend RPCClass = "send"
)

// Valid reports whether c is a declared RPC class.
func (c RPCClass) Valid() bool {
	return c == RPCClassRead || c == RPCClassSend
}

// RPCClassPolicy declares one class's degradation posture.
type RPCClassPolicy struct {
	// BaselineBounded: the class stays bounded without Redis, so it continues
	// at DegradedConcurrency while the limiter is unavailable.
	BaselineBounded bool
	// DegradedConcurrency bounds concurrent calls of a baseline-bounded class
	// while the limiter is unavailable (initial value, calibrated after
	// measurement). Required when BaselineBounded.
	DegradedConcurrency int
}

// RPCBudgetObserver mirrors pause transitions (metrics; T003). A nil observer
// disables observation.
type RPCBudgetObserver interface {
	ObserveRPCBudgetPaused(class string)
}

// ErrRPCPaused reports a class that is safely paused because only the
// distributed budget could keep it bounded. It is retryable after recovery;
// it is never a payment verdict and never a reason to skip a check.
var ErrRPCPaused = errors.New("ratelimit: rpc class safely paused (distributed budget unavailable)")

// RPCBudget is the distributed overlay. It is safe for concurrent use.
type RPCBudget struct {
	limiter  *Limiter
	observer RPCBudgetObserver
	classes  map[RPCClass]rpcPolicy

	mu     sync.Mutex
	paused map[RPCClass]bool
}

type rpcPolicy struct {
	baselineBounded bool
	sem             chan struct{}
}

// NewRPCBudget validates the class policies fail-closed and builds the
// overlay.
func NewRPCBudget(limiter *Limiter, observer RPCBudgetObserver, policies map[RPCClass]RPCClassPolicy) (*RPCBudget, error) {
	if limiter == nil {
		return nil, errors.New("ratelimit: rpc budget requires a limiter")
	}
	if len(policies) == 0 {
		return nil, errors.New("ratelimit: rpc budget requires at least one class")
	}
	classes := make(map[RPCClass]rpcPolicy, len(policies))
	for class, policy := range policies {
		if !class.Valid() {
			return nil, fmt.Errorf("ratelimit: unknown rpc class %q", class)
		}
		built := rpcPolicy{baselineBounded: policy.BaselineBounded}
		if policy.BaselineBounded {
			if policy.DegradedConcurrency <= 0 {
				return nil, fmt.Errorf("ratelimit: rpc class %s needs a positive degraded concurrency", class)
			}
			built.sem = make(chan struct{}, policy.DegradedConcurrency)
		}
		classes[class] = built
	}
	return &RPCBudget{
		limiter:  limiter,
		observer: observer,
		classes:  classes,
		paused:   make(map[RPCClass]bool, len(policies)),
	}, nil
}

// Admit gates one outbound call of class. The returned release function is
// always non-nil when err is nil and must be called after the call completes
// (a no-op when no degraded slot was taken).
func (b *RPCBudget) Admit(ctx context.Context, class RPCClass) (func(), error) {
	policy, ok := b.classes[class]
	if !ok {
		return nil, fmt.Errorf("ratelimit: rpc class %q is not configured", class)
	}
	decision, err := b.limiter.Allow(ctx, ClassRPC)
	if err == nil {
		b.setPaused(class, false)
		if !decision.Allowed {
			return nil, fmt.Errorf("%w: rpc class %s", ErrLimited, class)
		}
		return func() {}, nil
	}
	if !errors.Is(err, ErrUnavailable) {
		// A configuration error fails closed, exactly like the limiter.
		return nil, err
	}
	if !policy.baselineBounded {
		// Only the distributed budget could keep this class bounded: pause it
		// safely (never silently unbounded) until the limiter recovers.
		b.setPaused(class, true)
		return nil, fmt.Errorf("%w: rpc class %s", ErrRPCPaused, class)
	}
	// Baseline-bounded class: continue at the degraded concurrency.
	b.setPaused(class, false)
	select {
	case policy.sem <- struct{}{}:
		return func() { <-policy.sem }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Observe refreshes pause transitions from the limiter state without
// attempting a call, so a paused class is observable on the status surfaces
// even when no call of that class is in flight.
func (b *RPCBudget) Observe() {
	unavailable := b.limiter.Unavailable()
	for class, policy := range b.classes {
		b.setPaused(class, unavailable && !policy.baselineBounded)
	}
}

// Status returns each class's posture for the status surfaces:
// "ok" (distributed budget healthy), "degraded" (baseline-bounded and
// continuing without the distributed budget), "paused" (safely paused) or
// "unknown" (no state yet).
func (b *RPCBudget) Status() map[string]string {
	b.Observe()
	unavailable := b.limiter.Unavailable()
	out := make(map[string]string, len(b.classes))
	for class, policy := range b.classes {
		switch {
		case !unavailable:
			out[string(class)] = "ok"
		case policy.baselineBounded:
			out[string(class)] = "degraded"
		default:
			out[string(class)] = "paused"
		}
	}
	return out
}

// setPaused flips one class's pause state and counts the transition once.
func (b *RPCBudget) setPaused(class RPCClass, paused bool) {
	b.mu.Lock()
	was := b.paused[class]
	b.paused[class] = paused
	b.mu.Unlock()
	if paused && !was {
		slog.Warn("rpc class safely paused: distributed budget unavailable",
			"class", string(class))
		if b.observer != nil {
			b.observer.ObserveRPCBudgetPaused(string(class))
		}
	}
	if !paused && was {
		slog.Info("rpc class resumed after distributed budget recovery",
			"class", string(class))
	}
}
