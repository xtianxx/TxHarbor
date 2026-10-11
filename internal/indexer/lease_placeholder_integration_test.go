//go:build integration

// lease_placeholder_integration_test.go owns the regression suite for the
// coordination-row placeholder contract (008 R1/R4 framing over 002's
// `indexer_lease`, data-model.md Table 3).
//
// The contract under test: 008 never acquires, renews, or updates the lease.
// Its 5-step framing may materialize the shared row on first use, but only as
// an already-expired placeholder — never as a live lease — so the indexer's
// `Lease.Acquire` CAS (which takes over only `expires_at < now()` by the
// database clock) wins immediately on a fresh chain.
//
// The suite needs a dedicated, freshly migrated PostgreSQL: the fixture DSN is
// read from TXHARBOR_NONCE_LEASE_FIX_DSN (deliberately not TXHARBOR_PG_DSN),
// and the suite never drops, truncates, deletes, or resets anything. Chain ids
// are unique to these cases so repeated runs on a retained fixture only ever
// add rows.
//
// The suite lives in the indexer test package because internal/indexer
// production code imports internal/nonce (binding_live.go), so a nonce test
// cannot import internal/indexer; here the real Lease, the canonical 5-step
// coordination framing (lockRecoveryChain, the shape 008 copies), and the real
// 008 operator path are all reachable at once.
package indexer

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/nonce"
)

const (
	// leaseFixDSNEnv names the dedicated fixture database.
	leaseFixDSNEnv = "TXHARBOR_NONCE_LEASE_FIX_DSN"

	// leaseFixPlaceholderOwner is the owner_id the 008 framing writes when it
	// materializes the row; it must never be a live lease.
	leaseFixPlaceholderOwner = "nonce-008"
)

// leaseFixChainBase is drawn once per process: every case takes a fresh chain
// id so the suite is repeatable on a retained fixture without deleting
// anything, and the created rows stay queryable as evidence.
var leaseFixChainBase = leaseFixChainRandomBase()

func leaseFixChainRandomBase() int64 {
	var b [8]byte
	if _, err := cryptorand.Read(b[:]); err != nil {
		panic("nonce-lease-fix: random chain base: " + err.Error())
	}
	return 100_000_000 + int64(binary.BigEndian.Uint64(b[:])%800_000_000)
}

var (
	leaseFixMigrateOnce sync.Once
	leaseFixMigrateErr  error
)

// leaseFixPool opens the dedicated fixture pool and applies the embedded
// migrations once per process.
func leaseFixPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv(leaseFixDSNEnv)
	if dsn == "" {
		t.Skipf("%s is not set; the nonce-lease-fix suite needs its dedicated fresh-PG fixture", leaseFixDSNEnv)
	}
	leaseFixMigrateOnce.Do(func() {
		leaseFixMigrateErr = db.MigrateUp(context.Background(), db.MigrateOptions{
			DSN:            dsn,
			LockTimeout:    5 * time.Second,
			ConnectTimeout: 5 * time.Second,
		}, io.Discard)
	})
	if leaseFixMigrateErr != nil {
		t.Fatalf("migrate fixture database: %v", leaseFixMigrateErr)
	}
	pool, err := db.OpenPool(context.Background(), dsn, 5*time.Second)
	if err != nil {
		t.Fatalf("open fixture pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// leaseFixRow is one `indexer_lease` row read back with the database clock.
type leaseFixRow struct {
	owner     string
	token     int64
	expiresAt time.Time
	updatedAt time.Time
	dbNow     time.Time
	expired   bool
}

func (r leaseFixRow) String() string {
	return fmt.Sprintf("owner=%s token=%d expires_at=%s updated_at=%s expired=%t",
		r.owner, r.token,
		r.expiresAt.UTC().Format(time.RFC3339Nano),
		r.updatedAt.UTC().Format(time.RFC3339Nano),
		r.expired)
}

func leaseFixReadRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64) leaseFixRow {
	t.Helper()
	var r leaseFixRow
	err := pool.QueryRow(ctx, `
SELECT owner_id, fencing_token, expires_at, updated_at, expires_at < now(), now()
FROM indexer_lease WHERE chain_id = $1`, chainID).
		Scan(&r.owner, &r.token, &r.expiresAt, &r.updatedAt, &r.expired, &r.dbNow)
	if err != nil {
		t.Fatalf("read indexer_lease row for chain %d: %v", chainID, err)
	}
	return r
}

// leaseFixAssertRowEqual pins a row value-by-value: the 008 coordination write
// must leave owner_id, fencing_token, expires_at, and updated_at exactly as
// they were.
func leaseFixAssertRowEqual(t *testing.T, what string, want, got leaseFixRow) {
	t.Helper()
	if want.owner != got.owner || want.token != got.token ||
		!want.expiresAt.Equal(got.expiresAt) || !want.updatedAt.Equal(got.updatedAt) {
		t.Fatalf("%s: row changed\n want %s\n  got %s", what, want, got)
	}
}

// leaseFixRegister drives the real 008 operator path (AdminRunner.Run →
// adminInTx → the shared coordination framing), i.e. exactly the write the
// demo CLI performs before the serve process starts.
func leaseFixRegister(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, sender, operationID string) {
	t.Helper()
	res, err := nonce.NewAdminRunner(pool, nil).Run(ctx, nonce.AdminRequest{
		Action:      nonce.AdminActionRegistryRegister,
		OperationID: operationID,
		ChainID:     chainID,
		Sender:      sender,
	})
	if err != nil {
		t.Fatalf("registry register on chain %d: %v", chainID, err)
	}
	if res.Outcome != nonce.AdminApplied {
		t.Fatalf("registry register on chain %d outcome = %s (%s), want %s", chainID, res.Outcome, res.Detail, nonce.AdminApplied)
	}
}

// leaseFixWaitExpired waits (bounded, on the database clock only) until the
// chain's coordination row is observably expired. It never infers ordering
// from a sleep: the predicate is the same one Lease.Acquire evaluates.
func leaseFixWaitExpired(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	var expired bool
	var expiresAt, dbNow time.Time
	for {
		if err := pool.QueryRow(ctx, `
SELECT expires_at < now(), expires_at, now() FROM indexer_lease WHERE chain_id = $1`, chainID).
			Scan(&expired, &expiresAt, &dbNow); err != nil {
			t.Fatalf("poll lease expiry on chain %d: %v", chainID, err)
		}
		if expired {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("lease on chain %d still unexpired after %s: expires_at=%s db now=%s",
				chainID, within, expiresAt.UTC().Format(time.RFC3339Nano), dbNow.UTC().Format(time.RFC3339Nano))
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// leaseFixWaitBlockedAcquirers polls PostgreSQL until at least want Acquire
// statements are observably waiting on a lock, proving both callers are
// serialized behind the coordination row lock before the holder commits.
func leaseFixWaitBlockedAcquirers(t *testing.T, ctx context.Context, pool *pgxpool.Pool, want int, within time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(within)
	var last int
	for {
		if err := pool.QueryRow(ctx, `
SELECT count(*) FROM pg_stat_activity
WHERE wait_event_type = 'Lock' AND query LIKE '%INSERT INTO indexer_lease%'`).Scan(&last); err != nil {
			t.Fatalf("probe blocked Acquire sessions: %v", err)
		}
		if last >= want {
			return last
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d Acquire session(s) blocked on the coordination row lock within %s",
				last, want, within)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// TestLeaseFixPlaceholderBornExpiredThenAcquireWins is case 1: on a virgin
// chain the first real 008 write must materialize the coordination row as an
// already-expired placeholder, and the indexer's real Lease.Acquire must win
// it immediately with fencing token 1.
func TestLeaseFixPlaceholderBornExpiredThenAcquireWins(t *testing.T) {
	ctx := context.Background()
	pool := leaseFixPool(t)
	chain := leaseFixChainBase
	t.Logf("case 1 chain id %d", chain)

	var pre int64
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM indexer_lease WHERE chain_id = $1`, chain).Scan(&pre); err != nil {
		t.Fatalf("count pre-state on chain %d: %v", chain, err)
	}
	if pre != 0 {
		t.Fatalf("fixture chain %d already has a coordination row; this case needs a virgin chain id", chain)
	}

	leaseFixRegister(t, ctx, pool, chain, "0x1111111111111111111111111111111111111111", fmt.Sprintf("op-lease-fix-placeholder-%d", chain))

	row := leaseFixReadRow(t, ctx, pool, chain)
	t.Logf("first materialization on chain %d: %s", chain, row)
	if row.owner != leaseFixPlaceholderOwner {
		t.Errorf("placeholder owner = %q, want %q", row.owner, leaseFixPlaceholderOwner)
	}
	if row.token != 0 {
		t.Errorf("placeholder fencing token = %d, want 0 (008 must not fence)", row.token)
	}
	if !row.expired || !row.expiresAt.Before(row.dbNow) {
		t.Errorf("first materialization must be born expired: %s", row)
	}

	lease, err := NewLease(pool, chain, Params{OwnerID: "lease-fix-indexer"})
	if err != nil {
		t.Fatalf("NewLease: %v", err)
	}
	won, token, err := lease.Acquire(ctx)
	if err != nil {
		t.Fatalf("Acquire after the placeholder: %v", err)
	}
	if !won || token != 1 {
		t.Errorf("Acquire after the placeholder = (won=%t token=%d), want (true, 1): the indexer must win immediately on a born-expired placeholder", won, token)
	}
	after := leaseFixReadRow(t, ctx, pool, chain)
	t.Logf("post-Acquire row on chain %d: %s", chain, after)
	if won && (after.owner != "lease-fix-indexer" || after.token != 1 || after.expired) {
		t.Errorf("post-Acquire row = %s, want owner=lease-fix-indexer token=1 expired=false", after)
	}
}

// TestLeaseFixNonceCoordWritePreservesExistingLease is case 2: over an
// existing live and an existing expired lease, the real 008 coordination write
// must leave owner_id/fencing_token/expires_at/updated_at untouched; a live
// lease refuses a second instance, and an expired one is taken over with
// exactly one fence increment.
func TestLeaseFixNonceCoordWritePreservesExistingLease(t *testing.T) {
	ctx := context.Background()
	pool := leaseFixPool(t)
	chain := leaseFixChainBase + 1
	t.Logf("case 2 chain id %d", chain)

	live, err := NewLease(pool, chain, Params{OwnerID: "lease-fix-live", TTL: 6 * time.Second, Heartbeat: 2 * time.Second})
	if err != nil {
		t.Fatalf("NewLease live: %v", err)
	}
	won, token, err := live.Acquire(ctx)
	if err != nil || !won {
		t.Fatalf("seed live lease = (won=%t token=%d err=%v), want a win on the virgin chain", won, token, err)
	}
	t.Logf("seed live lease token %d (insert path)", token)
	beforeLive := leaseFixReadRow(t, ctx, pool, chain)
	if beforeLive.expired {
		t.Fatalf("seeded live lease already expired: %s", beforeLive)
	}

	leaseFixRegister(t, ctx, pool, chain, "0x2222222222222222222222222222222222222222", fmt.Sprintf("op-lease-fix-preserve-live-%d", chain))
	afterLive := leaseFixReadRow(t, ctx, pool, chain)
	if afterLive.expired {
		t.Fatalf("the seeded lease expired before the live-state checks completed: %s", afterLive)
	}
	leaseFixAssertRowEqual(t, "008 write over a live lease", beforeLive, afterLive)

	loser, err := NewLease(pool, chain, Params{OwnerID: "lease-fix-live-loser"})
	if err != nil {
		t.Fatalf("NewLease loser: %v", err)
	}
	wonLoser, tokenLoser, err := loser.Acquire(ctx)
	if err != nil {
		t.Fatalf("Acquire over a live lease: %v", err)
	}
	if wonLoser || tokenLoser != 0 {
		t.Fatalf("Acquire over a live lease = (won=%t token=%d), want (false, 0)", wonLoser, tokenLoser)
	}
	leaseFixAssertRowEqual(t, "refused Acquire over a live lease", afterLive, leaseFixReadRow(t, ctx, pool, chain))

	// The seeded lease expires on the database clock (6s TTL): wait for the
	// same predicate Acquire evaluates, then re-run the 008 write and the
	// takeover against the expired state of the very same row.
	leaseFixWaitExpired(t, ctx, pool, chain, 20*time.Second)

	beforeExpired := leaseFixReadRow(t, ctx, pool, chain)
	leaseFixRegister(t, ctx, pool, chain, "0x3333333333333333333333333333333333333333", fmt.Sprintf("op-lease-fix-preserve-expired-%d", chain))
	leaseFixAssertRowEqual(t, "008 write over an expired lease", beforeExpired, leaseFixReadRow(t, ctx, pool, chain))

	takeover, err := NewLease(pool, chain, Params{OwnerID: "lease-fix-takeover"})
	if err != nil {
		t.Fatalf("NewLease takeover: %v", err)
	}
	wonTakeover, tokenTakeover, err := takeover.Acquire(ctx)
	if err != nil {
		t.Fatalf("takeover Acquire: %v", err)
	}
	if !wonTakeover || tokenTakeover != token+1 {
		t.Fatalf("takeover Acquire = (won=%t token=%d), want (true, %d): the fence must advance exactly once", wonTakeover, tokenTakeover, token+1)
	}
	final := leaseFixReadRow(t, ctx, pool, chain)
	if final.owner != "lease-fix-takeover" || final.token != tokenTakeover || final.expired {
		t.Fatalf("post-takeover row = %s, want owner=lease-fix-takeover token=%d expired=false", final, tokenTakeover)
	}

	third, err := NewLease(pool, chain, Params{OwnerID: "lease-fix-third"})
	if err != nil {
		t.Fatalf("NewLease third: %v", err)
	}
	wonThird, tokenThird, err := third.Acquire(ctx)
	if err != nil {
		t.Fatalf("Acquire over the taken-over lease: %v", err)
	}
	if wonThird || tokenThird != 0 {
		t.Fatalf("Acquire over the taken-over lease = (won=%t token=%d), want (false, 0)", wonThird, tokenThird)
	}
	leaseFixAssertRowEqual(t, "refused Acquire over the taken-over lease", final, leaseFixReadRow(t, ctx, pool, chain))
}

// TestLeaseFixCoordLockSerializesConcurrentAcquire is case 3: a real 008
// coordination transaction holding the row lock serializes two independent
// Lease.Acquire callers. Both are observably waiting on the lock before the
// holder commits; afterwards exactly one wins and the other neither wins nor
// overwrites owner/fence.
func TestLeaseFixCoordLockSerializesConcurrentAcquire(t *testing.T) {
	ctx := context.Background()
	pool := leaseFixPool(t)
	chain := leaseFixChainBase + 2
	t.Logf("case 3 chain id %d", chain)

	leaseFixRegister(t, ctx, pool, chain, "0x4444444444444444444444444444444444444444", fmt.Sprintf("op-lease-fix-lock-%d", chain))

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin coordination transaction: %v", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()
	// The repo's canonical 5-step coordination framing (the exact shape 008
	// copies): statement guard -> ensure the coordination row ->
	// SELECT ... FOR UPDATE (the row lock the two Acquires must queue on).
	if err := lockRecoveryChain(ctx, tx, chain, nil); err != nil {
		t.Fatalf("lockRecoveryChain: %v", err)
	}

	type acquireResult struct {
		owner string
		won   bool
		token int64
		err   error
	}
	results := make(chan acquireResult, 2)
	start := make(chan struct{})
	for _, owner := range []string{"lease-fix-conc-a", "lease-fix-conc-b"} {
		lease, err := NewLease(pool, chain, Params{OwnerID: owner})
		if err != nil {
			t.Fatalf("NewLease %s: %v", owner, err)
		}
		go func(lease *Lease, owner string) {
			<-start
			actx, cancel := context.WithTimeout(ctx, 20*time.Second)
			defer cancel()
			won, token, err := lease.Acquire(actx)
			results <- acquireResult{owner: owner, won: won, token: token, err: err}
		}(lease, owner)
	}
	close(start)

	// Observable synchronization: both callers must be waiting in PostgreSQL
	// before the holder releases. The probe deadline stays under Acquire's own
	// 5s statement bound so the release always lands while both are waiting.
	blocked := leaseFixWaitBlockedAcquirers(t, ctx, pool, 2, 3*time.Second)
	t.Logf("observed %d Acquire session(s) waiting on the coordination row lock", blocked)

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit coordination transaction: %v", err)
	}
	committed = true

	got := make([]acquireResult, 0, 2)
	deadline := time.After(20 * time.Second)
	for len(got) < 2 {
		select {
		case r := <-results:
			if r.err != nil {
				t.Fatalf("concurrent Acquire (%s): %v", r.owner, r.err)
			}
			got = append(got, r)
		case <-deadline:
			t.Fatalf("timed out collecting concurrent Acquire results: have %d of 2", len(got))
		}
	}

	var winner acquireResult
	winners := 0
	for _, r := range got {
		switch {
		case r.won:
			winners++
			winner = r
			if r.token != 1 {
				t.Fatalf("winner %s token = %d, want 1 (placeholder 0 plus one fence bump)", r.owner, r.token)
			}
		case r.token != 0:
			t.Fatalf("loser %s returned token %d, want 0 (no token without a win)", r.owner, r.token)
		}
	}
	if winners != 1 {
		t.Fatalf("concurrent Acquire winners = %d, want exactly 1 (results %+v)", winners, got)
	}

	final := leaseFixReadRow(t, ctx, pool, chain)
	if final.owner != winner.owner || final.token != 1 || final.expired {
		t.Fatalf("final row = %s, want owner=%s token=1 expired=false: the loser must not overwrite owner/fence", final, winner.owner)
	}
}
