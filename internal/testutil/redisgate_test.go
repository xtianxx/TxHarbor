//go:build integration_redis

// redisgate_test.go: the gate's own minimal behavior tests (same layer as
// its users). They verify the postures end-to-end with a real backend
// subscriber, including the Pass→Hold CONVERSION of an established pair —
// the semantics the latency experiment depends on (a warm conn must stop
// responding after the flip, not keep forwarding to the backend) — and the
// GateDelay reply postponement after an EVAL frame.
package testutil

import (
	"context"
	"net"
	"strconv"
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

// TestRedisGateDelayDelaysEvalReplies: GateDelay forwards normally until an
// EVAL frame has crossed the pair, then delays backend reply chunks by
// SetDelay — pre-EVAL (handshake/PING) replies stay immediate, the EVAL reply
// is late but delivered intact, and the delayed_replies counter observes it.
func TestRedisGateDelayDelaysEvalReplies(t *testing.T) {
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
	gate.SetDelay(300 * time.Millisecond)
	if err := gate.SetMode(GateDelay); err != nil {
		t.Fatalf("delay mode: %v", err)
	}

	client := redis.NewClient(&redis.Options{Addr: gate.HostPort()})
	t.Cleanup(func() { _ = client.Close() })

	// Pre-EVAL traffic (handshake + PING) must pass undelayed.
	start := time.Now()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping through delay gate: %v", err)
	}
	if dur := time.Since(start); dur > 250*time.Millisecond {
		t.Fatalf("pre-EVAL ping took %s, want undelayed (<250ms)", dur)
	}

	// The EVAL reply is delayed by ~SetDelay but still delivered.
	before := gate.CounterSnapshot()
	start = time.Now()
	val, err := client.Eval(ctx, "return 41", nil).Result()
	dur := time.Since(start)
	if err != nil {
		t.Fatalf("eval through delay gate: %v", err)
	}
	if n, ok := val.(int64); !ok || n != 41 {
		t.Fatalf("eval result = %v, want 41 (the delayed reply must be forwarded intact)", val)
	}
	if dur < 250*time.Millisecond {
		t.Fatalf("eval took %s, want >= ~300ms (SetDelay)", dur)
	}
	if got := gate.CounterSnapshot().DelayedReplies - before.DelayedReplies; got < 1 {
		t.Fatalf("delayed_replies delta = %d, want >= 1", got)
	}

	// A client-side timeout during the delay window must not park the pair:
	// the forwarding loops drain, the pair is recycled, and the backend
	// connection is released.
	direct := redis.NewClient(&redis.Options{Addr: backend.HostPort(), ReadTimeout: 2 * time.Second})
	t.Cleanup(func() { _ = direct.Close() })
	connected := func() int {
		text, ierr := direct.Do(ctx, "INFO", "clients").Text()
		if ierr != nil {
			t.Fatalf("INFO clients: %v", ierr)
		}
		for _, line := range strings.Split(text, "\n") {
			if v, ok := strings.CutPrefix(line, "connected_clients:"); ok {
				n, perr := strconv.Atoi(strings.TrimSpace(v))
				if perr != nil {
					t.Fatalf("connected_clients %q: %v", v, perr)
				}
				return n
			}
		}
		t.Fatalf("INFO clients lacks connected_clients: %q", text)
		return 0
	}
	baseline := connected() // the first client's pair is established and healthy

	shortClient := redis.NewClient(&redis.Options{
		Addr: gate.HostPort(), ReadTimeout: 150 * time.Millisecond,
	})
	t.Cleanup(func() { _ = shortClient.Close() })
	if err := shortClient.Eval(ctx, "return 1", nil).Err(); err == nil {
		t.Fatal("eval with ReadTimeout<delay = nil, want a client-side timeout")
	}
	cleanupDeadline := time.Now().Add(5 * time.Second)
	for {
		if connected() == baseline {
			break
		}
		if time.Now().After(cleanupDeadline) {
			t.Fatalf("backend connections = %d after the timed-out delay client, want baseline %d (pair parked)", connected(), baseline)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Conversion DURING the delay window: a reply the gate has already read
	// and is holding must be swallowed at the flip, never delivered after the
	// gate entered Hold (the caller must not see a successful command).
	midClient := redis.NewClient(&redis.Options{
		Addr: gate.HostPort(), ReadTimeout: 700 * time.Millisecond,
	})
	t.Cleanup(func() { _ = midClient.Close() })
	midDone := make(chan error, 1)
	go func() {
		_, merr := midClient.Eval(ctx, "return 7", nil).Result()
		midDone <- merr
	}()
	time.Sleep(100 * time.Millisecond) // the gate has read the reply, delay pending
	if err := gate.SetMode(GateHold); err != nil {
		t.Fatalf("hold during delay: %v", err)
	}
	if merr := <-midDone; merr == nil {
		t.Fatal("eval during the delay window succeeded after the Hold flip: the pending reply was delivered (must be swallowed)")
	}

	// The intentional Hold conversion must still convert a warm delay pair.
	if err := gate.SetMode(GateHold); err != nil {
		t.Fatalf("hold: %v", err)
	}
	hangCtx, hangCancel := context.WithTimeout(ctx, 400*time.Millisecond)
	defer hangCancel()
	start = time.Now()
	if err := client.Ping(hangCtx).Err(); err == nil {
		t.Fatal("ping after Hold flip = nil, want hang (delay pair converted)")
	} else if got := time.Since(start); got < 300*time.Millisecond {
		t.Fatalf("ping after Hold flip failed in %s: want a hang, got %v", got, err)
	}
}
