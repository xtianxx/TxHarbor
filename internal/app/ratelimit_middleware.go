// ratelimit_middleware.go owns the T063 HTTP wiring of the 013 cache and
// limiter: a per-class admission middleware in front of the existing
// handlers, the PD-1 fail-closed refusal shape (429/503 + Retry-After, 007
// taxonomy code), and the cache-bypass annotation for authority queries.
//
// Ordering discipline: the middleware runs BEFORE the handler and can only
// refuse a request — it never authenticates, authorizes, deduplicates or
// rewrites a request, and every admitted request runs the original handler
// with its gate order unchanged (PD-1; contracts/redis.md §3). A limiter
// denial is a retryable availability outcome, never an authorization denial.
//
// Cache discipline: authority queries on this listener (withdrawal views,
// execution views, nonce reads) never resolve through the cache
// (contracts/redis.md §2.1); the middleware pins the explicit bypass
// annotation. The wired cache client serves non-authoritative read models as
// they are introduced and falls back to PostgreSQL behind its bounded guard
// while Redis is unavailable.
package app

import (
	"context"
	"errors"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/eth"
	"github.com/xtianxx/txharbor/internal/ratelimit"
	"github.com/xtianxx/txharbor/internal/withdrawal"
)

// Initial technical values (calibrated after measurement; never business
// thresholds).
const (
	// cacheFallbackConcurrency bounds concurrent PostgreSQL fallback loads
	// while Redis is unavailable (contracts/redis.md §2.5).
	cacheFallbackConcurrency = 8
	// rateLimitRecoveryWindow is the graded-reopening window after a limiter
	// outage: half rate/burst, never an instant unbounded reopening.
	rateLimitRecoveryWindow = 10 * time.Second
	// rpcDegradedConcurrency bounds baseline-bounded RPC classes while the
	// distributed budget is unavailable (research R12).
	rpcDegradedConcurrency = 4
)

// rateLimitClasses maps the measured configuration onto the limiter classes.
func rateLimitClasses(cfg *config.Config) map[ratelimit.Class]ratelimit.ClassConfig {
	class := func(c config.RateLimitClassConfig) ratelimit.ClassConfig {
		return ratelimit.ClassConfig{RatePerSecond: c.RatePerSecond, Burst: c.Burst}
	}
	return map[ratelimit.Class]ratelimit.ClassConfig{
		ratelimit.ClassNewWithdrawal: class(cfg.RateLimit.NewWithdrawal),
		ratelimit.ClassWrite:         class(cfg.RateLimit.Write),
		ratelimit.ClassQuery:         class(cfg.RateLimit.Query),
		ratelimit.ClassOperator:      class(cfg.RateLimit.Operator),
		ratelimit.ClassRPC:           class(cfg.RateLimit.RPC),
	}
}

// buildLimiter builds the per-class limiter from the measured configuration.
// Serve and the HTTP failure test share this constructor, so both exercise
// the same wiring.
func buildLimiter(store ratelimit.ScriptStore, cfg *config.Config, observer ratelimit.Observer) (*ratelimit.Limiter, error) {
	return ratelimit.NewLimiter(store, ratelimit.Config{
		Classes:        rateLimitClasses(cfg),
		Timeout:        cfg.Redis.Timeout,
		RecoveryWindow: rateLimitRecoveryWindow,
	}, observer)
}

// buildRPCBudget builds the distributed RPC budget overlay with the serve
// class policies: reads stay baseline-bounded (continue degraded), sends are
// only boundable by the distributed budget (safely pause).
func buildRPCBudget(limiter *ratelimit.Limiter, observer ratelimit.RPCBudgetObserver) (*ratelimit.RPCBudget, error) {
	return ratelimit.NewRPCBudget(limiter, observer, map[ratelimit.RPCClass]ratelimit.RPCClassPolicy{
		ratelimit.RPCClassRead: {BaselineBounded: true, DegradedConcurrency: rpcDegradedConcurrency},
		ratelimit.RPCClassSend: {BaselineBounded: false},
	})
}

// rateLimitMiddleware applies one class's admission decision before the
// handler. The GET/POST classes differ (a collection route mixes a
// fail-closed write with a degradable read).
type rateLimitMiddleware struct {
	policy *ratelimit.Policy
	post   ratelimit.Class
	get    ratelimit.Class
	next   http.Handler
}

// ServeHTTP decides the class from the method, refuses with the PD-1 shape
// when the policy refuses, and otherwise runs the original handler unchanged.
func (m *rateLimitMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	class := m.get
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		class = m.post
	}
	if err := m.policy.Admit(r.Context(), class); err != nil {
		writeRetryableRefusal(w, err)
		return
	}
	m.next.ServeHTTP(w, r)
}

// writeRetryableRefusal renders a policy refusal. A *RetryableError carries
// its own status/code/Retry-After; any other error (a configuration defect)
// fails closed with the 007 retryable 503 shape.
func writeRetryableRefusal(w http.ResponseWriter, err error) {
	trace := newWithdrawalTraceID()
	var retryable *ratelimit.RetryableError
	if errors.As(err, &retryable) {
		retryAfter := retryable.RetryAfter
		if retryAfter <= 0 {
			retryAfter = ratelimit.RetryAfterInitial
		}
		w.Header().Set("Retry-After", strconv.Itoa(retryAfterSeconds(retryAfter)))
		withdrawalWriteError(w, retryable.Status, retryable.Code, retryable.Message, "", trace, trace)
		return
	}
	w.Header().Set("Retry-After", strconv.Itoa(retryAfterSeconds(ratelimit.RetryAfterInitial)))
	withdrawalWriteError(w, http.StatusServiceUnavailable, string(withdrawal.CodeTemporarilyUnavailable), withdrawalUnavailableMessage, "", trace, trace)
}

// retryAfterSeconds rounds a Retry-After hint up to whole seconds (the wire
// form), never below 1.
func retryAfterSeconds(d time.Duration) int {
	seconds := int(math.Ceil(d.Seconds()))
	if seconds < 1 {
		return 1
	}
	return seconds
}

// guardRoute composes the 013 middleware around one handler: the rate-limit
// admission (when the 013 wiring is present) wraps the degradation
// annotation. A nil policy leaves the route unguarded by the limiter while
// keeping the annotation.
func guardRoute(policy *ratelimit.Policy, degradation *degradationState, post, get ratelimit.Class, next http.Handler) http.Handler {
	inner := &degradationMiddleware{state: degradation, next: next}
	if policy == nil {
		return inner
	}
	return &rateLimitMiddleware{policy: policy, post: post, get: get, next: inner}
}

// rpcBudgetAdapter maps ratelimit.RPCBudget onto the eth.BudgetGate seam, so
// the chain client keeps no dependency on the limiter package.
type rpcBudgetAdapter struct {
	budget *ratelimit.RPCBudget
}

// Admit implements eth.BudgetGate.
func (a rpcBudgetAdapter) Admit(ctx context.Context, class string) (func(), error) {
	release, err := a.budget.Admit(ctx, ratelimit.RPCClass(class))
	switch {
	case errors.Is(err, ratelimit.ErrRPCPaused):
		return nil, eth.ErrBudgetPaused
	case errors.Is(err, ratelimit.ErrLimited):
		return nil, eth.ErrBudgetLimited
	default:
		return release, err
	}
}
