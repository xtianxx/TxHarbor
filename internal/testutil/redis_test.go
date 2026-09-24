//go:build integration_redis

package testutil

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
)

// TestRedisSkeletonStartStopAndRecover is the T006 skeleton: start Redis,
// use it, stop it (fault injection), observe the failure, restart it and
// observe recovery on the same address. Skips (never passes) without Docker.
func TestRedisSkeletonStartStopAndRecover(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	r, err := StartRedis(ctx)
	if err != nil {
		t.Fatalf("StartRedis() error = %v", err)
	}
	t.Cleanup(func() { _ = r.Close(context.Background()) })

	opt, err := redis.ParseURL(r.Addr())
	if err != nil {
		t.Fatalf("parse redis addr %q: %v", r.Addr(), err)
	}
	addrBefore := r.Addr()
	client := redis.NewClient(opt)
	t.Cleanup(func() { _ = client.Close() })

	if err := client.Set(ctx, "skeleton", "1", time.Minute).Err(); err != nil {
		t.Fatalf("SET before stop: %v", err)
	}

	if err := r.Stop(ctx); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	pingCtx, pingCancel := context.WithTimeout(ctx, 3*time.Second)
	defer pingCancel()
	if err := client.Ping(pingCtx).Err(); err == nil {
		t.Fatal("PING succeeded while the container was stopped, want failure")
	}

	if err := r.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	// The helper binds a fixed host port, so the address must survive the
	// restart and the pre-outage client must reconnect on its own.
	if got := r.Addr(); got != addrBefore {
		t.Fatalf("Addr after restart = %q, want %q (stable address)", got, addrBefore)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		if err := client.Ping(ctx).Err(); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("PING never recovered after restart")
		}
		time.Sleep(200 * time.Millisecond)
	}
	if got, err := client.Get(ctx, "skeleton").Result(); err != nil || got != "1" {
		t.Fatalf("GET after restart = %q, %v; want 1, nil (same container instance)", got, err)
	}
}
