//go:build integration

package indexer

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/xtianxx/txharbor/internal/db"
)

// startIndexerPostgres boots a real PostgreSQL container and applies the
// embedded migrations. It skips (never passes) when no Docker provider is
// available.
func startIndexerPostgres(t *testing.T) string {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()
	ctr, err := postgres.Run(ctx, "postgres:18.6-trixie",
		postgres.WithDatabase("txharbor"),
		postgres.WithUsername("txharbor"),
		postgres.WithPassword("txharbor"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2)),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })
	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres connection string: %v", err)
	}
	if err := db.MigrateUp(ctx, db.MigrateOptions{
		DSN:            dsn,
		LockTimeout:    5 * time.Second,
		ConnectTimeout: 5 * time.Second,
	}, io.Discard); err != nil {
		t.Fatalf("migrate up: %v", err)
	}
	return dsn
}

func openIndexerPool(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()
	pool, err := db.OpenPool(context.Background(), dsn, 5*time.Second)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	return pool
}

func newTestLease(t *testing.T, pool *pgxpool.Pool, chainID int64, owner string, ttl, heartbeat time.Duration) *Lease {
	t.Helper()
	l, err := NewLease(pool, chainID, Params{OwnerID: owner, TTL: ttl, Heartbeat: heartbeat})
	if err != nil {
		t.Fatalf("NewLease(%s): %v", owner, err)
	}
	return l
}

// waitUntil polls cond until it is true or the wall-clock deadline passes.
// The app clock only bounds how long the test is willing to wait; lease
// validity is never derived from it (data-model.md Table 3).
func waitUntil(t *testing.T, deadline time.Time, describe string, cond func() bool) {
	t.Helper()
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", describe)
}

func leaseRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64) (owner string, token int64, expires time.Time) {
	t.Helper()
	if err := pool.QueryRow(ctx,
		"SELECT owner_id, fencing_token, expires_at FROM indexer_lease WHERE chain_id = $1", chainID).
		Scan(&owner, &token, &expires); err != nil {
		t.Fatalf("read indexer_lease: %v", err)
	}
	return owner, token, expires
}

func countChainBlocks(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID, number int64) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx,
		"SELECT count(*) FROM chain_blocks WHERE chain_id = $1 AND number = $2", chainID, number).Scan(&n); err != nil {
		t.Fatalf("count chain_blocks: %v", err)
	}
	return n
}

// tryWriteWithToken mimics the coordination protocol's fencing check
// (data-model.md 写事务协议 step 4): ensure the coordination row, lock it,
// re-read owner/fencing_token/expiry with a fresh statement, and only then
// perform a marker write. It reports whether that write committed. A stale
// owner/token or an expired lease must return (false, nil) with zero rows
// written.
func tryWriteWithToken(ctx context.Context, pool *pgxpool.Pool, chainID, number int64, hash, parentHash, owner string, token int64) (bool, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx,
		`INSERT INTO indexer_lease (chain_id, owner_id, expires_at, updated_at)
		 VALUES ($1, $2, now(), now()) ON CONFLICT (chain_id) DO NOTHING`, chainID, owner); err != nil {
		return false, err
	}
	var (
		gotOwner  string
		gotToken  int64
		unexpired bool
	)
	err = tx.QueryRow(ctx,
		`SELECT owner_id, fencing_token, expires_at > now()
		   FROM indexer_lease WHERE chain_id = $1 FOR UPDATE`, chainID).Scan(&gotOwner, &gotToken, &unexpired)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if gotOwner != owner || gotToken != token || !unexpired {
		return false, nil // ROLLBACK via defer: stale writers write nothing
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO chain_blocks (chain_id, number, hash, parent_hash)
		 VALUES ($1, $2, $3, $4) ON CONFLICT (chain_id, number) DO NOTHING`,
		chainID, number, hash, parentHash); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}

// TestLeaseExactlyOneHolderAndExpiryTakeover covers FR-08/FR-14 (R2): two
// instances race for one chain, exactly one holds the lease at a time; when
// the holder's connections die, the contender takes over through the real
// expiry condition (DB clock) with a bumped fencing token, and a stale
// owner/token cannot write.
func TestLeaseExactlyOneHolderAndExpiryTakeover(t *testing.T) {
	dsn := startIndexerPostgres(t)
	ctx := context.Background()

	const (
		chainID       = int64(31337)
		ttl           = 1200 * time.Millisecond
		heartbeat     = 300 * time.Millisecond
		takeoverWatch = 20 * time.Second
	)

	holderPool := openIndexerPool(t, dsn)
	contenderPool := openIndexerPool(t, dsn)
	// observerPool is a dedicated read/write-path pool that is never closed
	// mid-test: killing the winner's instance pool must not break observation.
	observerPool := openIndexerPool(t, dsn)
	defer observerPool.Close()
	defer contenderPool.Close()
	defer holderPool.Close() // idempotent; may also be closed mid-test

	instanceA := newTestLease(t, holderPool, chainID, "instance-a", ttl, heartbeat)
	instanceB := newTestLease(t, contenderPool, chainID, "instance-b", ttl, heartbeat)

	// Two instances Acquire on an empty lease row at the same instant:
	// exactly one wins, the other sees the live lease.
	type acquireResult struct {
		lease *Lease
		ok    bool
		token int64
		err   error
	}
	results := make(chan acquireResult, 2)
	start := make(chan struct{})
	for _, candidate := range []*Lease{instanceA, instanceB} {
		go func(l *Lease) {
			<-start
			ok, token, err := l.Acquire(ctx)
			results <- acquireResult{lease: l, ok: ok, token: token, err: err}
		}(candidate)
	}
	close(start)

	var winner, contender *Lease
	var winToken int64
	wins := 0
	for i := 0; i < 2; i++ {
		r := <-results
		if r.err != nil {
			t.Fatalf("Acquire() error = %v", r.err)
		}
		if r.ok {
			wins++
			winner, winToken = r.lease, r.token
		} else {
			contender = r.lease
		}
	}
	if wins != 1 {
		t.Fatalf("Acquire() wins = %d, want exactly 1", wins)
	}
	if winner == nil || contender == nil {
		t.Fatalf("race did not produce one winner and one contender")
	}
	if owner, token, _ := leaseRow(t, ctx, observerPool, chainID); owner != winner.ownerID || token != winToken {
		t.Fatalf("lease after race = (%s, %d), want (%s, %d)", owner, token, winner.ownerID, winToken)
	}
	if ok, _, err := contender.Acquire(ctx); err != nil || ok {
		t.Fatalf("contender Acquire() = (%v, %v), want (false, nil) while holder is live", ok, err)
	}

	// The holder heartbeats and keeps the lease past its original expiry.
	_, _, initialExpiry := leaseRow(t, ctx, observerPool, chainID)
	hbCtx, hbCancel := context.WithCancel(ctx)
	hbErr := make(chan error, 1)
	go func() { hbErr <- winner.Heartbeat(hbCtx) }()
	waitUntil(t, time.Now().Add(10*time.Second), "heartbeat renewal to extend expires_at", func() bool {
		_, _, expires := leaseRow(t, ctx, observerPool, chainID)
		return expires.After(initialExpiry)
	})
	if ok, _, err := contender.Acquire(ctx); err != nil || ok {
		t.Fatalf("contender Acquire() while heartbeat runs = (%v, %v), want (false, nil)", ok, err)
	}

	// Kill the holder: stop its heartbeat, then close the WINNER's pool
	// (connections die, no further renewal happens). The winner is whichever
	// instance won the race above — closing a fixed pool variable would kill
	// the contender instead whenever instance-b wins.
	hbCancel()
	if err := <-hbErr; err != nil {
		t.Fatalf("Heartbeat() after cancel = %v, want nil", err)
	}
	winnerPool := holderPool
	if winner == instanceB {
		winnerPool = contenderPool
	}
	winnerPool.Close()

	// Real-expiry takeover: bounded conditional wait (no bare sleep guessing
	// the completion time), then token must have advanced by exactly one.
	var (
		tookToken   int64
		takeoverErr error
	)
	waitUntil(t, time.Now().Add(takeoverWatch), "expiry takeover by the contender", func() bool {
		if takeoverErr != nil {
			return true
		}
		ok, token, err := contender.Acquire(ctx)
		if err != nil {
			takeoverErr = err
			return true
		}
		if ok {
			tookToken = token
			return true
		}
		return false
	})
	if takeoverErr != nil {
		t.Fatalf("contender Acquire() during takeover: %v", takeoverErr)
	}
	if tookToken != winToken+1 {
		t.Fatalf("takeover token = %d, want %d (winner token + 1)", tookToken, winToken+1)
	}
	if owner, token, expires := leaseRow(t, ctx, observerPool, chainID); owner != contender.ownerID || token != tookToken || !expires.After(time.Now()) {
		t.Fatalf("lease after takeover = (%s, %d, %s), want (%s, %d, unexpired)",
			owner, token, expires, contender.ownerID, tookToken)
	}

	// Renewal of a stale owner is reported as loss; the real holder renews.
	stale := newTestLease(t, observerPool, chainID, winner.ownerID, ttl, heartbeat)
	if err := stale.Renew(ctx); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale Renew() error = %v, want ErrLeaseLost", err)
	}
	if err := contender.Renew(ctx); err != nil {
		t.Fatalf("holder Renew() error = %v, want nil", err)
	}

	// Heartbeat surfaces loss immediately instead of writing on: a ghost with
	// the old owner id fails on its first renewal.
	ghost := newTestLease(t, observerPool, chainID, winner.ownerID, 200*time.Millisecond, 20*time.Millisecond)
	if err := ghost.Heartbeat(ctx); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale Heartbeat() error = %v, want ErrLeaseLost", err)
	}

	// Stale owner/token write attempt is rejected with zero rows written; the
	// current holder's token writes exactly once.
	const number = int64(1)
	hash := "0x" + strings.Repeat("ab", 32)
	parentHash := "0x" + strings.Repeat("cd", 32)
	before := countChainBlocks(t, ctx, observerPool, chainID, number)
	allowed, err := tryWriteWithToken(ctx, observerPool, chainID, number, hash, parentHash, winner.ownerID, winToken)
	if err != nil {
		t.Fatalf("stale write attempt: %v", err)
	}
	if allowed {
		t.Fatal("write with stale owner/token was allowed, want rejected")
	}
	if got := countChainBlocks(t, ctx, observerPool, chainID, number); got != before {
		t.Fatalf("stale write leaked %d row(s), want 0", got-before)
	}
	allowed, err = tryWriteWithToken(ctx, observerPool, chainID, number, hash, parentHash, contender.ownerID, contender.Token())
	if err != nil {
		t.Fatalf("holder write attempt: %v", err)
	}
	if !allowed {
		t.Fatal("write with the current owner/token was rejected, want allowed")
	}
	if got := countChainBlocks(t, ctx, observerPool, chainID, number); got != before+1 {
		t.Fatalf("holder write rows = %d, want %d", got, before+1)
	}
}
