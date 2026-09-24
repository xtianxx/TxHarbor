//go:build integration_redis

// ratelimit_integration_test.go is the T066 acceptance (Integration-Redis;
// V-RATELIMIT): token-bucket behavior against a real Redis, the
// unavailable-state classification, the PD-1 policy matrix verified as two
// non-conflicting lines (new creation refused / existing flows continue), and
// the graded reopening after recovery. Limiting never participates in
// authorization: the policy only admits or refuses, and every admitted
// request still runs the caller's own gates.
package ratelimit

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"

	"github.com/xtianxx/txharbor/internal/testutil"
)

// t066Observer records limiter metric calls.
type t066Observer struct {
	mu          sync.Mutex
	denied      int
	unavailable int
	recovered   int
}

func (o *t066Observer) ObserveRateLimitDenied(string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.denied++
}

func (o *t066Observer) SetRateLimitUnavailable(unavailable bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if unavailable {
		o.unavailable++
	}
}

func (o *t066Observer) ObserveRateLimitRecovery(string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.recovered++
}

func (o *t066Observer) snapshot() (denied, unavailable, recovered int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.denied, o.unavailable, o.recovered
}

func TestRatelimitVRatelimitAgainstRealRedis(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	redisCtr, err := testutil.StartRedis(ctx)
	if err != nil {
		t.Fatalf("StartRedis: %v", err)
	}
	t.Cleanup(func() { _ = redisCtr.Close(context.Background()) })

	opt, err := redis.ParseURL(redisCtr.Addr())
	if err != nil {
		t.Fatalf("parse redis addr: %v", err)
	}
	raw := redis.NewClient(opt)
	t.Cleanup(func() { _ = raw.Close() })
	store, err := NewRedisScriptStore(raw)
	if err != nil {
		t.Fatalf("NewRedisScriptStore: %v", err)
	}
	observer := &t066Observer{}
	limiter, err := NewLimiter(store, Config{
		Classes: map[Class]ClassConfig{
			ClassNewWithdrawal: {RatePerSecond: 2, Burst: 2},
			ClassWrite:         {RatePerSecond: 50, Burst: 10},
			ClassQuery:         {RatePerSecond: 100, Burst: 50},
			ClassOperator:      {RatePerSecond: 5, Burst: 2},
			ClassRPC:           {RatePerSecond: 50, Burst: 20},
		},
		Timeout:        2 * time.Second,
		RecoveryWindow: 10 * time.Second,
	}, observer)
	if err != nil {
		t.Fatalf("NewLimiter: %v", err)
	}
	policy, err := NewPolicy(limiter)
	if err != nil {
		t.Fatalf("NewPolicy: %v", err)
	}

	// 1. Normal state: the token bucket allows the burst, then denies with a
	// retry hint; denials are counted.
	for i := 0; i < 2; i++ {
		if err := policy.Admit(ctx, ClassNewWithdrawal); err != nil {
			t.Fatalf("Admit #%d in normal state = %v, want allowed", i+1, err)
		}
	}
	err = policy.Admit(ctx, ClassNewWithdrawal)
	var retryable *RetryableError
	if !errors.As(err, &retryable) || retryable.Status != 429 || retryable.RetryAfter <= 0 {
		t.Fatalf("Admit over burst = %v, want a 429 retryable denial", err)
	}
	if denied, _, _ := observer.snapshot(); denied == 0 {
		t.Fatal("denial was not observed")
	}

	// 2. Redis outage: PD-1's two lines hold simultaneously — new withdrawal
	// creation is refused 100% (0 unlimited pass) while every other class
	// continues under its own gates.
	if err := redisCtr.Stop(ctx); err != nil {
		t.Fatalf("stop redis: %v", err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		_, allowErr := limiter.Allow(ctx, ClassQuery)
		if allowErr != nil && limiter.Unavailable() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("limiter never reported unavailable")
		}
		time.Sleep(100 * time.Millisecond)
	}
	refusals := 0
	for i := 0; i < 20; i++ {
		err := policy.Admit(ctx, ClassNewWithdrawal)
		if err == nil {
			t.Fatalf("Admit(new_withdrawal) #%d during the outage = nil, want a refusal (0 unlimited pass)", i+1)
		}
		if !errors.As(err, &retryable) || retryable.Status != 503 || retryable.Reason != "limiter_unavailable" {
			t.Fatalf("refusal #%d = %v, want the 503 limiter_unavailable shape", i+1, err)
		}
		refusals++
	}
	if refusals != 20 {
		t.Fatalf("refusals = %d, want 20", refusals)
	}
	// The existing flows continue: the limiter never gates them, and the
	// caller's own gates still run.
	for _, class := range []Class{ClassWrite, ClassQuery, ClassOperator} {
		gateRan := false
		gate := func() { gateRan = true }
		if err := policy.Admit(ctx, class); err != nil {
			t.Fatalf("Admit(%s) during the outage = %v, want continuation", class, err)
		}
		gate()
		if !gateRan {
			t.Fatalf("own gate did not run for %s", class)
		}
	}

	// 3. Recovery: graded reopening — the first trusted decision opens the
	// recovery window at half rate/burst, so the next immediate request is
	// denied instead of an instant unbounded reopening.
	if err := redisCtr.Start(ctx); err != nil {
		t.Fatalf("start redis: %v", err)
	}
	recoveryDeadline := time.Now().Add(30 * time.Second)
	for {
		if err := raw.Ping(ctx).Err(); err == nil {
			break
		}
		if time.Now().After(recoveryDeadline) {
			t.Fatal("redis never recovered")
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err := raw.Del(ctx, "txharbor:rl:new_withdrawal").Err(); err != nil {
		t.Fatalf("reset bucket: %v", err)
	}
	if err := policy.Admit(ctx, ClassNewWithdrawal); err != nil {
		t.Fatalf("recovery probe = %v, want allowed at the graded rate", err)
	}
	if !limiter.Recovering() {
		t.Fatal("limiter is not in its graded-reopening window")
	}
	err = policy.Admit(ctx, ClassNewWithdrawal)
	if !errors.As(err, &retryable) || retryable.Status != 429 {
		t.Fatalf("second request during recovery = %v, want denied by the halved burst", err)
	}
	if _, unavailable, recovered := observer.snapshot(); unavailable == 0 || recovered == 0 {
		t.Fatalf("observations = unavailable:%d recovered:%d, want both > 0", unavailable, recovered)
	}
}
