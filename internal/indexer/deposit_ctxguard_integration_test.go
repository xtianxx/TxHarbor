//go:build integration

// deposit_ctxguard_integration_test.go owns Lane-F6's regression for the two
// deposit-scanner retry branches (readProgress / readUpstream): a loop
// cancellation that lands while such a read is in flight must exit cleanly
// without recording state 2, while a genuine read error under a live loop ctx
// must still be classified as a retry. Every barrier is deterministic (a held
// table lock plus a pg_stat_activity wait) — no sleep-based window hunting —
// and every wait/cleanup is bounded.
package indexer

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// guardLockTable takes ACCESS EXCLUSIVE on one table from an independent
// session and returns a bounded release (also registered as test cleanup).
func guardLockTable(t *testing.T, ctx context.Context, dsn, table string) func() {
	t.Helper()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("blocker connect: %v", err)
	}
	if _, err := conn.Exec(ctx, "BEGIN"); err != nil {
		_ = conn.Close(ctx)
		t.Fatalf("blocker begin: %v", err)
	}
	if _, err := conn.Exec(ctx, "LOCK TABLE "+table+" IN ACCESS EXCLUSIVE MODE"); err != nil {
		_ = conn.Close(ctx)
		t.Fatalf("blocker lock %s: %v", table, err)
	}
	release := func() {
		rctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = conn.Exec(rctx, "ROLLBACK")
		_ = conn.Close(rctx)
	}
	t.Cleanup(release)
	return release
}

// guardWaitBlocked polls pg_stat_activity until one backend is actively
// waiting on a lock for a query mentioning the fragment (deterministic proof
// that the scanner reached the target read before the cancel).
//
// TODO(shared-pg): this poll reads cluster-wide pg_stat_activity. Safe while
// package indexer runs serially against its own per-test databases; revisit
// before any cross-package container sharing or t.Parallel
// (indexer_shared_pg_test.go).
func guardWaitBlocked(t *testing.T, ctx context.Context, pool *pgxpool.Pool, fragment string) {
	t.Helper()
	waitUntil(t, time.Now().Add(10*time.Second), "scanner blocked on "+fragment, func() bool {
		var n int
		err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity
			WHERE wait_event_type = 'Lock' AND state = 'active'
			  AND query LIKE '%' || $1 || '%'`, fragment).Scan(&n)
		return err == nil && n > 0
	})
}

// newGuardScene starts the isolated stack and returns the scanner/lease over a
// minimal scene (no progress rows needed: an empty progress is valid, and the
// blocked read is what the barriers target).
func newGuardScene(t *testing.T, ctx context.Context, chainID int64) (string, *pgxpool.Pool, *DepositScanner, *Lease) {
	t.Helper()
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	t.Cleanup(pool.Close)
	cfg := depositITConfig(t, chainID, testContractA)
	cfg.PollInterval = 20 * time.Millisecond
	cfg.RetryInitial = 20 * time.Millisecond
	cfg.RetryMax = 100 * time.Millisecond
	sc := depositITScanner(t, pool, cfg)
	lease := depositITLease(t, pool, chainID)
	return dsn, pool, sc, lease
}

// TestDepositState2CtxGuardReadProgress: the readProgress branch must not turn
// a loop-cancelled in-flight read into state 2. Pre-fix this test fails with
// state = 2; post-fix the last meaningful state (0) survives and ServeLoop
// returns nil.
func TestDepositState2CtxGuardReadProgress(t *testing.T) {
	ctx := context.Background()
	dsn, pool, sc, lease := newGuardScene(t, ctx, 24601)
	release := guardLockTable(t, ctx, dsn, "deposit_checkpoint")
	defer release()

	stop := depositRunLoop(t, ctx, sc, lease)
	guardWaitBlocked(t, ctx, pool, "deposit_checkpoint")
	before := sc.DepositState()
	stop()
	after := sc.DepositState()
	if after != before {
		t.Fatalf("state after in-flight cancel = %d, want the pre-cancel %d (cancellation must not record a retry state)", after, before)
	}
	if after == 2 {
		t.Fatal("cancellation of an in-flight readProgress recorded retry state 2")
	}
}

// TestDepositState2CtxGuardReadUpstream: same contract for the readUpstream
// branch (blocked on log_checkpoint, reached after capture + readProgress).
func TestDepositState2CtxGuardReadUpstream(t *testing.T) {
	ctx := context.Background()
	dsn, pool, sc, lease := newGuardScene(t, ctx, 24602)
	release := guardLockTable(t, ctx, dsn, "log_checkpoint")
	defer release()

	stop := depositRunLoop(t, ctx, sc, lease)
	guardWaitBlocked(t, ctx, pool, "log_checkpoint")
	before := sc.DepositState()
	stop()
	after := sc.DepositState()
	if after != before {
		t.Fatalf("state after in-flight cancel = %d, want the pre-cancel %d (cancellation must not record a retry state)", after, before)
	}
	if after == 2 {
		t.Fatal("cancellation of an in-flight readUpstream recorded retry state 2")
	}
}

// TestDepositState2CtxGuardCaptureSemanticsUnchanged: the capture-recovery
// branch already checked the loop ctx before storing 2; this pins that its
// cancellation polarity is untouched (clean nil, no retry state).
func TestDepositState2CtxGuardCaptureSemanticsUnchanged(t *testing.T) {
	ctx := context.Background()
	dsn, pool, sc, lease := newGuardScene(t, ctx, 24603)
	release := guardLockTable(t, ctx, dsn, "reorg_recovery")
	defer release()

	stop := depositRunLoop(t, ctx, sc, lease)
	guardWaitBlocked(t, ctx, pool, "reorg_recovery")
	before := sc.DepositState()
	stop()
	after := sc.DepositState()
	if after != before {
		t.Fatalf("capture cancel changed state from %d to %d", before, after)
	}
	if after == 2 {
		t.Fatal("capture cancellation recorded retry state 2")
	}
}

// TestDepositState2RealReadErrorStillRetries is the non-cancellation
// counterexample: with a live loop ctx, a genuine read failure (the history
// relation hidden) must still be classified as a retry (state 2) and the loop
// must keep running, not exit.
func TestDepositState2RealReadErrorStillRetries(t *testing.T) {
	ctx := context.Background()
	_, pool, sc, lease := newGuardScene(t, ctx, 24604)
	mustExec := func(q string) {
		if _, err := pool.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	mustExec(`ALTER TABLE deposit_config_history RENAME TO deposit_config_history_hidden`)

	loopCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- sc.ServeLoop(loopCtx, lease, nil) }()
	stopped := false
	stopLoop := func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("ServeLoop() = %v, want nil on cancellation", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("ServeLoop did not stop after cancellation")
		}
	}
	defer stopLoop()

	waitUntil(t, time.Now().Add(10*time.Second), "retry state for a real read error", func() bool {
		return sc.DepositState() == 2
	})
	select {
	case err := <-done:
		t.Fatalf("loop exited on a transient read error (state must stay a retry): %v", err)
	default:
	}

	mustExec(`ALTER TABLE deposit_config_history_hidden RENAME TO deposit_config_history`)
	stopLoop()
}
