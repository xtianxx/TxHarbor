//go:build linux && integration

// integration_targetlock_health_context_test.go is the genuine PostgreSQL 18
// regression for the context-aware Health acquisition: real lock sessions,
// real owner loss and real Release, with no SKIP under CI.
package recovery

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

func newRealHealthLock(t *testing.T, f *targetWriterFixture) (*TargetLock, TargetKey) {
	t.Helper()
	targetDSN := f.createTarget(t)
	target, err := controlstore.ParseDSNTarget(targetDSN)
	if err != nil {
		t.Fatal(err)
	}
	key, err := CanonicalTargetKey(target)
	if err != nil {
		t.Fatal(err)
	}
	lock, err := AcquireTargetLock(f.ctx, f.ctrlDSN, key, 3*time.Second, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("acquire real target lock: %v", err)
	}
	return lock, key
}

func TestIntegrationTargetLockHealthContextRealPG(t *testing.T) {
	f := newTargetWriterFixture(t)
	lock, _ := newRealHealthLock(t, f)

	// Healthy real lock.
	if err := lock.Health(context.Background()); err != nil {
		t.Fatalf("healthy real lock: %v", err)
	}

	// nil context refuses without touching the session.
	if err := lock.Health(nil); err == nil || !strings.Contains(err.Error(), "requires a context") {
		t.Fatalf("nil context: %v", err)
	}
	if err := lock.Health(context.Background()); err != nil {
		t.Fatalf("real lock after nil-context refusal: %v", err)
	}

	// Already-canceled context refuses with its cause before any SQL.
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	err := lock.Health(canceled)
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled context must wrap context.Canceled: %v", err)
	}
	if err := lock.Health(context.Background()); err != nil {
		t.Fatalf("real lock after canceled-context refusal: %v", err)
	}

	// Serialization held + short deadline: bounded refusal, no abandoned
	// waiter that would steal the mutex after the holder releases.
	lock.mu.Lock()
	bounded, cancelBounded := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelBounded()
	started := time.Now()
	err = lock.Health(bounded)
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		lock.mu.Unlock()
		t.Fatalf("held serialization must refuse at the deadline: %v", err)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		lock.mu.Unlock()
		t.Fatalf("held serialization refusal was not bounded: %s", elapsed)
	}
	lock.mu.Unlock()
	if err := lock.Health(context.Background()); err != nil {
		t.Fatalf("real lock after bounded refusal: %v", err)
	}

	// Real same-owner loss: terminate the lock session's backend elsewhere.
	var ownerPID int
	if err := lock.conn.QueryRow(f.ctx, `SELECT pg_backend_pid()::int`).Scan(&ownerPID); err != nil {
		t.Fatalf("read lock owner pid: %v", err)
	}
	if _, err := f.ctrl.Exec(f.ctx, `SELECT pg_terminate_backend($1)`, ownerPID); err != nil {
		t.Fatalf("terminate lock owner: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		err := lock.Health(context.Background())
		if err != nil {
			if strings.Contains(err.Error(), "postgres://") {
				t.Fatalf("health error exposed raw DSN material: %v", err)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("terminated real owner still reported healthy")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Real Release refuses later health.
	second, _ := newRealHealthLock(t, f)
	if err := second.Health(context.Background()); err != nil {
		t.Fatalf("second real lock healthy: %v", err)
	}
	if err := second.Release(context.Background()); err != nil {
		t.Fatalf("release second real lock: %v", err)
	}
	if err := second.Health(context.Background()); err == nil || !strings.Contains(err.Error(), "session is closed") {
		t.Fatalf("released real lock must fail closed: %v", err)
	}
	if err := second.Release(context.Background()); err != nil {
		t.Fatalf("idempotent release: %v", err)
	}
}
