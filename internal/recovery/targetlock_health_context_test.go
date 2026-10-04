//go:build linux

// targetlock_health_context_test.go proves the CA03 context-aware Health
// acquisition with a real sync.Mutex and Ping/Query counters on a unit fake
// (not a PG proof): nil/already-done contexts refuse before any SQL, a held
// serialization makes Health return at the caller deadline without abandoned
// waiters, and the same Ping plus exact granted-advisory checks are preserved.
package recovery

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type healthCountingConn struct {
	mu      sync.Mutex
	pings   int
	queries int
	held    bool
	pingErr error
}

func (c *healthCountingConn) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}

func (c *healthCountingConn) QueryRow(context.Context, string, ...any) pgx.Row {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queries++
	return healthCountingRow{held: c.held}
}

func (c *healthCountingConn) Ping(context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pings++
	return c.pingErr
}

func (c *healthCountingConn) Close(context.Context) error { return nil }

func (c *healthCountingConn) counts() (int, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pings, c.queries
}

type healthCountingRow struct{ held bool }

func (r healthCountingRow) Scan(dest ...any) error {
	if len(dest) != 1 {
		return errors.New("unexpected scan shape")
	}
	b, ok := dest[0].(*bool)
	if !ok {
		return errors.New("unexpected scan destination")
	}
	*b = r.held
	return nil
}

func newHealthTestLock() (*TargetLock, *healthCountingConn) {
	conn := &healthCountingConn{held: true}
	return &TargetLock{conn: conn, key1: 11, key2: 22}, conn
}

func TestTargetLockHealthRefusesNilAndDoneContextsBeforeAnySQL(t *testing.T) {
	lock, conn := newHealthTestLock()

	if err := lock.Health(nil); err == nil {
		t.Fatal("nil context accepted")
	}
	if pings, queries := conn.counts(); pings != 0 || queries != 0 {
		t.Fatalf("nil context ran SQL: pings=%d queries=%d", pings, queries)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	err := lock.Health(canceled)
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled context must refuse with its cause: %v", err)
	}
	if pings, queries := conn.counts(); pings != 0 || queries != 0 {
		t.Fatalf("pre-canceled context ran SQL: pings=%d queries=%d", pings, queries)
	}
	if !lock.mu.TryLock() {
		t.Fatal("already-done context left the serialization mutex held")
	}
	lock.mu.Unlock()

	expired, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelExpired()
	err = lock.Health(expired)
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("already-expired context must refuse with its cause: %v", err)
	}
	if pings, queries := conn.counts(); pings != 0 || queries != 0 {
		t.Fatalf("already-expired context ran SQL: pings=%d queries=%d", pings, queries)
	}

	if err := (*TargetLock)(nil).Health(context.Background()); err == nil {
		t.Fatal("nil lock accepted")
	}
}

func TestTargetLockHealthReturnsAtDeadlineWhileSerializationHeld(t *testing.T) {
	lock, conn := newHealthTestLock()
	lock.mu.Lock()

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	started := time.Now()
	err := lock.Health(ctx)
	elapsed := time.Since(started)
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("held serialization must refuse at the deadline: %v", err)
	}
	if elapsed > time.Second {
		t.Fatalf("held serialization refusal was not bounded: %s", elapsed)
	}
	if pings, queries := conn.counts(); pings != 0 || queries != 0 {
		t.Fatalf("held serialization ran SQL while locked: pings=%d queries=%d", pings, queries)
	}

	// The abandoned waiter must not steal serialization once the holder
	// releases: the mutex is immediately available again.
	lock.mu.Unlock()
	if !lock.mu.TryLock() {
		t.Fatal("a lingering waiter retained serialization after the refusal")
	}
	lock.mu.Unlock()

	if err := lock.Health(context.Background()); err != nil {
		t.Fatalf("healthy lock after bounded refusal: %v", err)
	}
	if pings, queries := conn.counts(); pings != 1 || queries != 1 {
		t.Fatalf("healthy call counts: pings=%d queries=%d, want 1/1", pings, queries)
	}
}

func TestTargetLockHealthAcquiresAfterSerializationRelease(t *testing.T) {
	lock, conn := newHealthTestLock()
	lock.mu.Lock()
	done := make(chan error, 1)
	go func() { done <- lock.Health(context.Background()) }()
	select {
	case err := <-done:
		t.Fatalf("Health returned while serialization was held: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	lock.mu.Unlock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Health after serialization release: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Health did not acquire serialization after release")
	}
	if pings, queries := conn.counts(); pings != 1 || queries != 1 {
		t.Fatalf("healthy call counts: pings=%d queries=%d, want 1/1", pings, queries)
	}
}

func TestTargetLockHealthPreservesExactLossAndFaultChecks(t *testing.T) {
	lock, conn := newHealthTestLock()
	if err := lock.Health(context.Background()); err != nil {
		t.Fatalf("healthy lock: %v", err)
	}
	conn.mu.Lock()
	conn.held = false
	conn.mu.Unlock()
	if err := lock.Health(context.Background()); err == nil || !strings.Contains(err.Error(), "ownership was lost") {
		t.Fatalf("lock loss must fail closed: %v", err)
	}
	conn.mu.Lock()
	conn.held = true
	conn.pingErr = errors.New("connection unavailable")
	conn.mu.Unlock()
	if err := lock.Health(context.Background()); err == nil || !strings.Contains(err.Error(), "control session health") {
		t.Fatalf("unhealthy control connection must fail closed: %v", err)
	}
}
