// Package ratelimit implements the 013 distributed rate limiting and its
// failure policy (contracts/redis.md §3–§4; FR-18/19; PD-1).
//
// Redis carries the limiter (an atomic Lua token bucket per interface class);
// PostgreSQL remains the only authority. Limiting is never authentication,
// authorization or idempotency: a limiter decision can only allow or refuse a
// request, never grant one, and the original gates always run afterwards for
// anything that is admitted (PD-1; research R11).
//
// Failure discipline: a connection failure, timeout, script error or an
// untrusted script result means "limiting unavailable" — the limiter makes no
// decision and the policy (policy.go) decides what that means per class. The
// only funding-write fail-closed path is new withdrawal creation (PD-1).
package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Class is the closed interface-class vocabulary (contracts/redis.md §3.1).
type Class string

const (
	// ClassNewWithdrawal is POST /withdrawals (new withdrawal creation): the
	// PD-1 fail-closed class.
	ClassNewWithdrawal Class = "new_withdrawal"
	// ClassWrite is the general write class (e.g. execution admission).
	ClassWrite Class = "write"
	// ClassQuery is the read/query class.
	ClassQuery Class = "query"
	// ClassOperator is the operator/admin class (CLI actions).
	ClassOperator Class = "operator"
	// ClassRPC is the distributed outbound-RPC budget class (T064).
	ClassRPC Class = "rpc"
)

// Valid reports whether c is a declared class.
func (c Class) Valid() bool {
	switch c {
	case ClassNewWithdrawal, ClassWrite, ClassQuery, ClassOperator, ClassRPC:
		return true
	}
	return false
}

// ClassConfig is one class's token-bucket setting. Values are measured inputs
// (config TXHARBOR_RATELIMIT_*), never invented defaults.
type ClassConfig struct {
	RatePerSecond int
	Burst         int
}

// ErrUnavailable reports that the limiter cannot make a trusted decision
// (Redis failure, timeout, script error, untrusted result). It is never a
// denial: the caller's policy maps it per class.
var ErrUnavailable = errors.New("ratelimit: unavailable")

// ErrLimited is a limiter denial (never an authorization decision).
var ErrLimited = errors.New("ratelimit: limited")

// Decision is one limiter outcome.
type Decision struct {
	Allowed bool
	// RetryAfter is the script's suggested wait before a retry is likely to
	// succeed (0 when allowed).
	RetryAfter time.Duration
	// Remaining is the token remainder after the decision.
	Remaining int64
}

// ScriptStore runs one atomic script evaluation (the Redis EVAL seam).
type ScriptStore interface {
	Eval(ctx context.Context, script string, keys []string, args ...any) (any, error)
}

// Observer mirrors limiter outcomes into the 013 metric series (T003);
// *metrics.Metrics satisfies it. A nil observer disables observation.
type Observer interface {
	ObserveRateLimitDenied(interfaceClass string)
	SetRateLimitUnavailable(unavailable bool)
	ObserveRateLimitRecovery(interfaceClass string)
}

// Config bounds one limiter.
type Config struct {
	// Classes are the per-class rate/burst settings; every class the limiter
	// serves must be configured (a missing class fails closed at Allow).
	Classes map[Class]ClassConfig
	// Timeout bounds every Redis round trip.
	Timeout time.Duration
	// RecoveryWindow is the graded-reopening window after an outage: during
	// it the limiter runs at half rate/burst so recovery is never an instant
	// unbounded reopening (initial value, calibrated after measurement).
	RecoveryWindow time.Duration
	// Clock is the time source; nil means time.Now.
	Clock func() time.Time
}

// tokenBucketScript is the atomic token-bucket evaluation. It returns
// {allowed, remaining, retry_after_ms}; the caller treats any other shape as
// untrusted.
const tokenBucketScript = `
local key = KEYS[1]
local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local cost = tonumber(ARGV[4])
local ttl = tonumber(ARGV[5])
local data = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts = tonumber(data[2])
if tokens == nil then tokens = burst end
if ts == nil then ts = now end
local delta = math.max(0, now - ts) / 1000.0
tokens = math.min(burst, tokens + delta * rate)
local allowed = 0
local retry = 0
if tokens >= cost then
  allowed = 1
  tokens = tokens - cost
else
  retry = math.ceil((cost - tokens) / rate * 1000)
end
redis.call('HMSET', key, 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', key, ttl)
return {allowed, math.floor(tokens), retry}
`

// Limiter is one distributed limiter over a ScriptStore. It is safe for
// concurrent use.
type Limiter struct {
	store          ScriptStore
	classes        map[Class]ClassConfig
	timeout        time.Duration
	recoveryWindow time.Duration
	clock          func() time.Time
	observer       Observer

	mu              sync.Mutex
	unavailable     bool
	recoveringUntil time.Time
}

// NewLimiter validates the bounds fail-closed and builds the limiter.
func NewLimiter(store ScriptStore, cfg Config, observer Observer) (*Limiter, error) {
	if store == nil {
		return nil, errors.New("ratelimit: script store is required")
	}
	if cfg.Timeout <= 0 {
		return nil, errors.New("ratelimit: timeout must be positive")
	}
	if cfg.RecoveryWindow <= 0 {
		return nil, errors.New("ratelimit: recovery window must be positive")
	}
	if len(cfg.Classes) == 0 {
		return nil, errors.New("ratelimit: at least one class is required")
	}
	for class, classCfg := range cfg.Classes {
		if !class.Valid() {
			return nil, fmt.Errorf("ratelimit: unknown class %q", class)
		}
		if classCfg.RatePerSecond <= 0 || classCfg.Burst <= 0 {
			return nil, fmt.Errorf("ratelimit: class %s needs a measured rate/burst", class)
		}
	}
	clock := cfg.Clock
	if clock == nil {
		clock = time.Now
	}
	return &Limiter{
		store:          store,
		classes:        cfg.Classes,
		timeout:        cfg.Timeout,
		recoveryWindow: cfg.RecoveryWindow,
		clock:          clock,
		observer:       observer,
	}, nil
}

// SetObserver attaches the metric observer (nil is allowed).
func (l *Limiter) SetObserver(observer Observer) { l.observer = observer }

// Unavailable reports whether the limiter currently cannot make decisions.
func (l *Limiter) Unavailable() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.unavailable
}

// Available reports whether the limiter can currently make decisions.
func (l *Limiter) Available() bool { return !l.Unavailable() }

// Recovering reports whether the limiter is inside its graded-reopening
// window after an outage.
func (l *Limiter) Recovering() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return !l.unavailable && !l.recoveringUntil.IsZero() && l.clock().Before(l.recoveringUntil)
}

// Allow evaluates one class decision. A trusted decision is returned as a
// Decision; an untrusted/failed evaluation returns ErrUnavailable and marks
// the limiter unavailable (never a silent allow).
func (l *Limiter) Allow(ctx context.Context, class Class) (Decision, error) {
	classCfg, ok := l.classes[class]
	if !ok {
		return Decision{}, fmt.Errorf("ratelimit: class %q is not configured", class)
	}
	rate, burst := classCfg.RatePerSecond, classCfg.Burst
	if l.Recovering() || l.Unavailable() {
		// Graded reopening: half rate/burst during the recovery window and on
		// the very first trusted decision that lifts the outage (the probe
		// decision itself is already scaled), never instant full throughput.
		rate = maxInt(1, rate/2)
		burst = maxInt(1, burst/2)
	}

	ctx, cancel := context.WithTimeout(ctx, l.timeout)
	defer cancel()
	now := l.clock()
	result, err := l.store.Eval(ctx, tokenBucketScript,
		[]string{"txharbor:rl:" + string(class)},
		rate, burst, now.UnixMilli(), 1, int64((2 * l.timeout).Milliseconds()),
	)
	if err != nil {
		l.markUnavailable()
		return Decision{}, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	allowed, remaining, retry, ok := parseScriptResult(result)
	if !ok {
		l.markUnavailable()
		return Decision{}, fmt.Errorf("%w: untrusted script result %v", ErrUnavailable, result)
	}
	l.markAvailable(class)
	decision := Decision{Allowed: allowed, Remaining: remaining}
	if !allowed {
		decision.RetryAfter = time.Duration(retry) * time.Millisecond
		if l.observer != nil {
			l.observer.ObserveRateLimitDenied(string(class))
		}
	}
	return decision, nil
}

// parseScriptResult accepts exactly the script's documented three-integer
// shape; anything else is untrusted (fail-closed).
func parseScriptResult(result any) (allowed bool, remaining int64, retryMillis int64, ok bool) {
	values, ok := result.([]any)
	if !ok || len(values) != 3 {
		return false, 0, 0, false
	}
	asInt := func(v any) (int64, bool) {
		switch n := v.(type) {
		case int64:
			return n, true
		case int:
			return int64(n), true
		default:
			return 0, false
		}
	}
	a, okA := asInt(values[0])
	rem, okR := asInt(values[1])
	retry, okT := asInt(values[2])
	if !okA || !okR || !okT || (a != 0 && a != 1) || rem < 0 || retry < 0 {
		return false, 0, 0, false
	}
	return a == 1, rem, retry, true
}

// markUnavailable flips the unavailable state and the availability gauge.
func (l *Limiter) markUnavailable() {
	l.mu.Lock()
	first := !l.unavailable
	l.unavailable = true
	l.recoveringUntil = time.Time{}
	l.mu.Unlock()
	if first && l.observer != nil {
		l.observer.SetRateLimitUnavailable(true)
	}
}

// markAvailable clears the unavailable state on a trusted decision. The first
// trusted decision after an outage opens the graded-reopening window and
// counts the recovery.
func (l *Limiter) markAvailable(class Class) {
	l.mu.Lock()
	wasUnavailable := l.unavailable
	l.unavailable = false
	if wasUnavailable {
		l.recoveringUntil = l.clock().Add(l.recoveryWindow)
	}
	l.mu.Unlock()
	if wasUnavailable && l.observer != nil {
		l.observer.SetRateLimitUnavailable(false)
		l.observer.ObserveRateLimitRecovery(string(class))
	}
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
