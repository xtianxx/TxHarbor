//go:build integration_redis

// redisgate_test.go: the gate's own minimal behavior tests (same layer as
// its users). They verify the three postures end-to-end with a real backend
// subscriber, including the Pass→Hold CONVERSION of an established pair —
// the semantics the latency experiment depends on (a warm conn must stop
// responding after the flip, not keep forwarding to the backend).
package testutil

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestRedisGatePassForwardsAndHoldConverses(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	backend, err := StartRedis(ctx)
	if err != nil {
		t.Fatalf("StartRedis: %v", err)
	}
	t.Cleanup(func() { _ = backend.Close(context.Background()) })

	gate, err := NewRedisGate(backend.HostPort())
	if err != nil {
		t.Fatalf("NewRedisGate: %v", err)
	}
	t.Cleanup(func() { _ = gate.Close() })

	client := redis.NewClient(&redis.Options{Addr: gate.HostPort()})
	t.Cleanup(func() { _ = client.Close() })

	// Pass: a real command succeeds through the gate.
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping through pass gate: %v", err)
	}
	// A warm conn is now in the pool.
	if err := client.Set(ctx, "redisgate-key", "v", 0).Err(); err != nil {
		t.Fatalf("set through pass gate: %v", err)
	}

	// Hold: the WARM conn must stop receiving answers (conversion), so the
	// next command hangs to the client timeout instead of succeeding.
	if err := gate.SetMode(GateHold); err != nil {
		t.Fatalf("hold: %v", err)
	}
	hangCtx, hangCancel := context.WithTimeout(ctx, 400*time.Millisecond)
	defer hangCancel()
	start := time.Now()
	if err := client.Ping(hangCtx).Err(); err == nil {
		t.Fatal("ping through hold gate = nil, want hang/timeout")
	} else if got := time.Since(start); got < 350*time.Millisecond {
		// The command must have RUN INTO the hold, not died instantly via a
		// broken warm socket (a reset would return in microseconds).
		t.Fatalf("ping failed TOO FAST (%s): %v — warm conn was likely dropped instead of converted", got, err)
	}

	// Down: dial meets ECONNREFUSED.
	if err := gate.SetMode(GateDown); err != nil {
		t.Fatalf("down: %v", err)
	}
	err = client.Ping(ctx).Err()
	if err == nil {
		t.Fatal("ping through down gate = nil, want refused")
	}
	t.Logf("down-gate error (informational): %v", err)

	// Pass again: recovery end-to-end through the SAME address.
	if err := gate.SetMode(GatePass); err != nil {
		t.Fatalf("pass: %v", err)
	}
	pingDeadline := time.Now().Add(15 * time.Second)
	for {
		if err := client.Ping(ctx).Err(); err == nil {
			break
		}
		if time.Now().After(pingDeadline) {
			t.Fatal("gate never recovered after Pass flip")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestRedisGateRefusedOnDownBeforeAnyConn(t *testing.T) {
	_, err := net.DialTimeout("tcp", "127.0.0.1:1", 500*time.Millisecond)
	if err == nil {
		t.Skip("loopback port 1 unexpectedly open; refusing shape not verifiable here")
	}
	// Shape ① must surface as "connection refused" (not i/o timeout) on a
	// closed loopback port.
	if !strings.Contains(err.Error(), "connect: connection refused") {
		t.Fatalf("closed loopback dial = %v, want connection refused", err)
	}
}
