// policy.go lands PD-1 (decided 2026-09-23; MUST NOT be weakened):
//
//   - when limiting is unavailable, new withdrawal creation (POST
//     /withdrawals) is REFUSED with an explicit retryable error — never
//     admitted unlimited;
//   - deposit observation, confirmation, reorg recovery and withdrawals
//     already accepted by PostgreSQL continue under their original
//     authorization, idempotency, state, pause, nonce, broadcast and
//     reconciliation gates — a Redis outage neither loosens nor tightens
//     them;
//   - queries may fall back to PostgreSQL;
//   - no alternative funding-write limiter is approved or introduced here:
//     the policy never counts, queues or admits a funding write on its own,
//     and it never substitutes a local limiter for the distributed one.
//
// The policy only produces admission decisions; the original gates always run
// afterwards for every admitted request (the middleware order in internal/app
// keeps the 007 handler and its gates unchanged).
package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// CodeTemporarilyUnavailable mirrors the 007 taxonomy code
// (withdrawal.CodeTemporarilyUnavailable): the refusal is a retryable
// availability outcome, never an authorization denial.
const CodeTemporarilyUnavailable = "temporarily_unavailable"

// RetryAfterInitial is the Retry-After hint for a limiter-unavailable
// refusal. Initial value, calibrated after measurement; it is a hint, never a
// guarantee.
const RetryAfterInitial = 1 * time.Second

// RetryableError is the PD-1 refusal: a clear, retryable error carrying the
// HTTP status and the 007 taxonomy code. The message instructs a same-key
// retry and never hints at rotating idempotency keys.
type RetryableError struct {
	Status     int
	Code       string
	Message    string
	RetryAfter time.Duration
	// Reason is the machine-readable cause ("limiter_unavailable",
	// "rate_limited"), useful for logs and tests.
	Reason string
}

// Error renders the refusal for logs.
func (e *RetryableError) Error() string {
	return fmt.Sprintf("%s: %s (%s)", e.Reason, e.Message, e.Code)
}

// Policy maps limiter outcomes onto the PD-1 failure policy.
type Policy struct {
	limiter *Limiter
}

// NewPolicy builds the policy over one limiter.
func NewPolicy(limiter *Limiter) (*Policy, error) {
	if limiter == nil {
		return nil, errors.New("ratelimit: policy requires a limiter")
	}
	return &Policy{limiter: limiter}, nil
}

// Admit evaluates one request against the limiter:
//
//   - a trusted allow returns nil: the request proceeds to its original gates;
//   - a trusted denial returns a 429 RetryableError (the limiter never
//     authorizes anything by itself);
//   - limiting unavailable returns a 503 RetryableError for new withdrawal
//     creation (PD-1 fail closed), and nil for every other class so existing
//     flows continue under their own gates;
//   - a configuration error (unconfigured class) fails closed with the
//     configuration error, never a silent allow.
func (p *Policy) Admit(ctx context.Context, class Class) error {
	decision, err := p.limiter.Allow(ctx, class)
	if err == nil {
		if decision.Allowed {
			return nil
		}
		retryAfter := decision.RetryAfter
		if retryAfter <= 0 {
			retryAfter = RetryAfterInitial
		}
		return &RetryableError{
			Status:     429,
			Code:       CodeTemporarilyUnavailable,
			Message:    "request rate exceeded; retry with the same idempotency key and the same parameters (never rotate the key)",
			RetryAfter: retryAfter,
			Reason:     "rate_limited",
		}
	}
	if errors.Is(err, ErrUnavailable) {
		if class == ClassNewWithdrawal {
			return &RetryableError{
				Status:     503,
				Code:       CodeTemporarilyUnavailable,
				Message:    "withdrawal intake is temporarily unavailable while rate limiting is degraded; retry with the same idempotency key and the same parameters (never rotate the key)",
				RetryAfter: RetryAfterInitial,
				Reason:     "limiter_unavailable",
			}
		}
		// PD-1: every other flow continues under its original gates. The
		// limiter was never a gate, so its outage neither loosens nor
		// tightens them.
		return nil
	}
	return fmt.Errorf("ratelimit policy: %w", err)
}

// Degraded reports whether limiting is currently unavailable.
func (p *Policy) Degraded() bool { return p.limiter.Unavailable() }

// Recovering reports whether the limiter is in its graded-reopening window.
func (p *Policy) Recovering() bool { return p.limiter.Recovering() }

// Limiter exposes the underlying limiter for status surfaces (read-only use).
func (p *Policy) Limiter() *Limiter { return p.limiter }
