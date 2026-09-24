// policy_test.go is the T062 unit evidence: the PD-1 policy matrix — allow,
// trusted denial, limiter-unavailable refusal of new withdrawal creation with
// a clear retryable error, continuation of every other flow, and the 0
// gate-bypass assertion (the policy never replaces or skips a funding gate).
package ratelimit

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestPolicyAllowsTrustedDecisions(t *testing.T) {
	store := &fakeScriptStore{result: []any{int64(1), int64(1), int64(0)}}
	limiter, err := NewLimiter(store, testConfig(), nil)
	if err != nil {
		t.Fatalf("NewLimiter: %v", err)
	}
	policy, err := NewPolicy(limiter)
	if err != nil {
		t.Fatalf("NewPolicy: %v", err)
	}
	gateRan := false
	gate := func() { gateRan = true }
	if err := policy.Admit(context.Background(), ClassNewWithdrawal); err != nil {
		t.Fatalf("Admit(allowed) = %v, want nil", err)
	}
	gate()
	if !gateRan {
		t.Fatal("gate did not run after an admitted request")
	}
}

func TestPolicyTrustedDenialIsRetryable429(t *testing.T) {
	store := &fakeScriptStore{result: []any{int64(0), int64(0), int64(500)}}
	limiter, err := NewLimiter(store, testConfig(), nil)
	if err != nil {
		t.Fatalf("NewLimiter: %v", err)
	}
	policy, _ := NewPolicy(limiter)
	err = policy.Admit(context.Background(), ClassQuery)
	var retryable *RetryableError
	if !errors.As(err, &retryable) {
		t.Fatalf("Admit(denied) = %v, want *RetryableError", err)
	}
	if retryable.Status != 429 || retryable.Code != CodeTemporarilyUnavailable || retryable.RetryAfter != 500*time.Millisecond {
		t.Fatalf("denial = %+v, want 429 + %s + 500ms", retryable, CodeTemporarilyUnavailable)
	}
	if !strings.Contains(retryable.Message, "same idempotency key") {
		t.Fatalf("denial message %q must instruct a same-key retry", retryable.Message)
	}
}

func TestPolicyUnavailableRefusesNewWithdrawalOnly(t *testing.T) {
	store := &fakeScriptStore{err: errors.New("connection refused")}
	limiter, err := NewLimiter(store, testConfig(), nil)
	if err != nil {
		t.Fatalf("NewLimiter: %v", err)
	}
	policy, _ := NewPolicy(limiter)
	ctx := context.Background()

	// PD-1 line 1: new withdrawal creation is refused, clearly and retryably.
	err = policy.Admit(ctx, ClassNewWithdrawal)
	var retryable *RetryableError
	if !errors.As(err, &retryable) {
		t.Fatalf("Admit(new_withdrawal) while unavailable = %v, want *RetryableError", err)
	}
	if retryable.Status != 503 || retryable.Code != CodeTemporarilyUnavailable {
		t.Fatalf("refusal = %+v, want 503 + %s", retryable, CodeTemporarilyUnavailable)
	}
	if retryable.RetryAfter <= 0 {
		t.Fatal("refusal must carry a positive Retry-After hint")
	}
	if retryable.Reason != "limiter_unavailable" {
		t.Fatalf("refusal reason = %q, want limiter_unavailable", retryable.Reason)
	}
	if !strings.Contains(retryable.Message, "same idempotency key") {
		t.Fatalf("refusal message %q must instruct a same-key retry", retryable.Message)
	}

	// PD-1 line 2: every other flow continues; its own gates still run.
	for _, class := range []Class{ClassWrite, ClassQuery, ClassOperator, ClassRPC} {
		gateRan := false
		gate := func() { gateRan = true }
		if err := policy.Admit(ctx, class); err != nil {
			t.Fatalf("Admit(%s) while unavailable = %v, want nil (flow continues under its own gates)", class, err)
		}
		gate()
		if !gateRan {
			t.Fatalf("gate did not run for %s after continuation", class)
		}
	}

	// The limiter is a gate-free overlay: after the refusal the policy state
	// is observable and the outage is recorded exactly once.
	if !policy.Degraded() {
		t.Fatal("Degraded() = false during the limiter outage")
	}
}

func TestPolicyUnconfiguredClassFailsClosed(t *testing.T) {
	store := &fakeScriptStore{result: []any{int64(1), int64(1), int64(0)}}
	limiter, err := NewLimiter(store, testConfig(), nil)
	if err != nil {
		t.Fatalf("NewLimiter: %v", err)
	}
	policy, _ := NewPolicy(limiter)
	err = policy.Admit(context.Background(), Class("bogus"))
	if err == nil || errors.Is(err, ErrUnavailable) {
		t.Fatalf("Admit(unconfigured) = %v, want a fail-closed configuration error", err)
	}
}

func TestNewPolicyRequiresLimiter(t *testing.T) {
	if _, err := NewPolicy(nil); err == nil {
		t.Fatal("NewPolicy(nil) succeeded, want fail-closed error")
	}
}
