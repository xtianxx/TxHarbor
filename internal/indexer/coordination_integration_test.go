//go:build integration

// T014-T017: deterministic coordination races on a real PostgreSQL. Every
// ordering decision is an explicit sync point (start barriers, channel
// hand-off, PostgreSQL's own lock-wait view) and every wait is a bounded
// conditional wait. Nothing is ordered by sleeping.
package indexer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/eth"
)

// coordStubClient satisfies HeaderClient for scanners that only exercise the
// write protocol: commitBlock and pause never call the chain client.
type coordStubClient struct{ chainID *big.Int }

func (c *coordStubClient) ChainID(context.Context) (*big.Int, error) { return c.chainID, nil }

func (c *coordStubClient) HeaderByNumber(context.Context, *big.Int) (*types.Header, error) {
	return nil, &eth.Error{Kind: eth.KindNotFound, Op: "stub", Err: errors.New("no headers in coordination tests")}
}

func newCoordScanner(t *testing.T, pool *pgxpool.Pool, lease *Lease, startHeight uint64) *Scanner {
	t.Helper()
	sc, err := NewScanner(pool, &coordStubClient{chainID: big.NewInt(lease.chainID)}, lease, Config{
		StartHeight:  startHeight,
		RPCTimeout:   time.Second,
		PollInterval: 25 * time.Millisecond,
		RetryInitial: 25 * time.Millisecond,
		RetryMax:     time.Second,
	}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("NewScanner(): %v", err)
	}
	return sc
}

// seedCoordFirstBlock wins the lease for chainID and commits the first block
// through the real write protocol, leaving the chain at height startHeight.
func seedCoordFirstBlock(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, owner string, ttl, heartbeat time.Duration, startHeight uint64, hash string) (*Lease, *Scanner) {
	t.Helper()
	lease := newTestLease(t, pool, chainID, owner, ttl, heartbeat)
	won, token, err := lease.Acquire(ctx)
	if err != nil || !won {
		t.Fatalf("seed lease Acquire() = (%v, %d, %v), want win", won, token, err)
	}
	sc := newCoordScanner(t, pool, lease, startHeight)
	rcap := testRecoveryCap(t, ctx, pool, chainID)
	if err := sc.commitBlock(ctx, blockWrite{number: startHeight, hash: hash, parent: coordHash(0), first: true}, rcap); err != nil {
		t.Fatalf("seed first block at %d: %v", startHeight, err)
	}
	return lease, sc
}

// coordHash builds a distinct, format-valid storage hash from one byte.
func coordHash(tag byte) string { return "0x" + strings.Repeat(fmt.Sprintf("%02x", tag), 32) }

// coordCheckpoint is the full durable checkpoint row. A pause must leave every
// field identical (T014 "逐字段不变", T017 "精确比对").
type coordCheckpoint struct {
	height      int64
	hash        string
	startHeight int64
	updatedAt   time.Time
}

func readCoordCheckpoint(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64) (coordCheckpoint, bool) {
	t.Helper()
	var cp coordCheckpoint
	err := pool.QueryRow(ctx,
		`SELECT height, block_hash, start_height, updated_at FROM indexer_checkpoint WHERE chain_id = $1`, chainID).
		Scan(&cp.height, &cp.hash, &cp.startHeight, &cp.updatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return coordCheckpoint{}, false
	}
	if err != nil {
		t.Fatalf("read checkpoint: %v", err)
	}
	return cp, true
}

func sameCoordCheckpoint(a, b coordCheckpoint) bool {
	return a.height == b.height && a.hash == b.hash && a.startHeight == b.startHeight && a.updatedAt.Equal(b.updatedAt)
}

func requireCoordCheckpoint(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, want coordCheckpoint) {
	t.Helper()
	got, ok := readCoordCheckpoint(t, ctx, pool, chainID)
	if !ok {
		t.Fatalf("checkpoint row for chain %d disappeared", chainID)
	}
	if !sameCoordCheckpoint(got, want) {
		t.Fatalf("checkpoint changed: got %+v, want %+v", got, want)
	}
}

func coordCountPauses(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM indexer_pause WHERE chain_id = $1`, chainID).Scan(&n); err != nil {
		t.Fatalf("count indexer_pause: %v", err)
	}
	return n
}

func coordActivityDump(ctx context.Context, pool *pgxpool.Pool) string {
	var dump string
	err := pool.QueryRow(ctx, `
SELECT coalesce(string_agg(format('pid=%s state=%s wait=%s/%s blockers=%s query=%s', pid, state, wait_event_type, wait_event, pg_blocking_pids(pid), query), E'\n' ORDER BY pid), '')
FROM pg_stat_activity WHERE datname = current_database()`).Scan(&dump)
	if err != nil {
		return fmt.Sprintf("pg_stat_activity dump failed: %v", err)
	}
	return "pg_stat_activity:\n" + dump
}

func coordLockDump(ctx context.Context, pool *pgxpool.Pool) string {
	var dump string
	err := pool.QueryRow(ctx, `
SELECT coalesce(string_agg(format('pid=%s lock=%s relation=%s mode=%s granted=%s', pid, locktype, relation::regclass, mode, granted), E'\n' ORDER BY pid), '')
FROM pg_locks WHERE relation = 'indexer_lease'::regclass OR NOT granted`).Scan(&dump)
	if err != nil {
		return fmt.Sprintf("pg_locks dump failed: %v", err)
	}
	return "pg_locks (indexer_lease / waiting):\n" + dump
}

// waitForBlockedBy returns only once PostgreSQL itself reports a session whose
// lock wait is attributed to blockerPID. The timeout is a test failure with a
// lock-wait diagnostic, never an ordering device.
func waitForBlockedBy(t *testing.T, ctx context.Context, observer *pgxpool.Pool, blockerPID int32, timeout time.Duration) int32 {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		var pid int32
		err := observer.QueryRow(ctx, `
SELECT a.pid FROM pg_stat_activity a
WHERE a.datname = current_database() AND a.pid <> $1 AND $1 = ANY(pg_blocking_pids(a.pid))
LIMIT 1`, blockerPID).Scan(&pid)
		switch {
		case err == nil:
			return pid
		case errors.Is(err, pgx.ErrNoRows):
		default:
			t.Fatalf("inspect pg_stat_activity: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("no session was observed blocked by pid %d within %s\n%s\n%s",
				blockerPID, timeout, coordActivityDump(ctx, observer), coordLockDump(ctx, observer))
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// TestCoordinationPauseHoldsOffAdvance is T014 (data-model case 1): a pause
// transaction holds the coordination lock, an in-flight advance is provably
// blocked, and only after the pause commits does the advance wake up and get
// refused by the verdict step with the checkpoint unchanged field by field.
func TestCoordinationPauseHoldsOffAdvance(t *testing.T) {
	dsn := startIndexerPostgres(t)
	ctx := context.Background()

	const (
		chainID     = int64(31337)
		startHeight = uint64(100)
	)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()

	firstHash := coordHash(0x11)
	nextHash := coordHash(0x22)
	pauseActual := coordHash(0x33)

	lease, scanner := seedCoordFirstBlock(t, ctx, pool, chainID, "pause-session", time.Minute, time.Second, startHeight, firstHash)
	before, ok := readCoordCheckpoint(t, ctx, pool, chainID)
	if !ok {
		t.Fatal("checkpoint missing after the first block")
	}

	// Session A runs the pause write protocol up to the uncommitted INSERT:
	// ensure the coordination row, hold the chain-wide lock, insert the pause.
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire session A: %v", err)
	}
	defer conn.Release()
	var aPID int32
	if err := conn.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&aPID); err != nil {
		t.Fatalf("session A pg_backend_pid: %v", err)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("session A BEGIN: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, ensureLeaseSQL, chainID, lease.ownerID, lease.Token(), lease.ttl.Seconds()); err != nil {
		t.Fatalf("session A ensure coordination row: %v", err)
	}
	var (
		lockedOwner string
		lockedToken int64
		unexpired   bool
	)
	if err := tx.QueryRow(ctx, lockCoordSQL, chainID).Scan(&lockedOwner, &lockedToken, &unexpired); err != nil {
		t.Fatalf("session A lock coordination row: %v", err)
	}
	if !unexpired || lockedOwner != lease.ownerID || lockedToken != lease.Token() {
		t.Fatalf("session A locked lease = (%s, %d, unexpired=%v), want (%s, %d, true)",
			lockedOwner, lockedToken, unexpired, lease.ownerID, lease.Token())
	}
	if _, err := tx.Exec(ctx, insertPauseSQL, chainID, int64(startHeight+1), nextHash, pauseActual, pauseHashMismatch, "T014 pause holds the lock"); err != nil {
		t.Fatalf("session A insert pause: %v", err)
	}

	// Session B advances S+1 and must queue behind A's lock. The sync point is
	// PostgreSQL reporting B blocked by A; no sleep decides the ordering.
	bCtx, cancelB := context.WithTimeout(ctx, 60*time.Second)
	defer cancelB()
	bStarted := make(chan struct{})
	bDone := make(chan error, 1)
	rcapB := testRecoveryCap(t, ctx, pool, chainID)
	go func() {
		close(bStarted)
		bDone <- scanner.commitBlock(bCtx, blockWrite{number: startHeight + 1, hash: nextHash, parent: firstHash, first: false}, rcapB)
	}()
	<-bStarted
	blockedPID := waitForBlockedBy(t, ctx, pool, aPID, 10*time.Second)
	if blockedPID == 0 || blockedPID == aPID {
		t.Fatalf("blocked session pid = %d, want a different pid", blockedPID)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("session A COMMIT: %v", err)
	}
	select {
	case err := <-bDone:
		if !errors.Is(err, errPaused) {
			t.Fatalf("advance after pause commit = %v, want errPaused", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatalf("advance never returned after the pause committed\n%s\n%s", coordActivityDump(ctx, pool), coordLockDump(ctx, pool))
	}

	// Refused at the verdict step: zero block writes, checkpoint untouched.
	requireCoordCheckpoint(t, ctx, pool, chainID, before)
	if got := countChainBlocks(t, ctx, pool, chainID, int64(startHeight+1)); got != 0 {
		t.Fatalf("advance wrote %d block row(s) at S+1, want 0", got)
	}
	if got := countChainBlocks(t, ctx, pool, chainID, int64(startHeight)); got != 1 {
		t.Fatalf("blocks at S = %d, want 1", got)
	}
	if got := coordCountPauses(t, ctx, pool, chainID); got != 1 {
		t.Fatalf("pause rows = %d, want exactly 1", got)
	}
	var (
		pauseHeight   int64
		pauseExpected string
		pauseStored   string
		pauseKind     string
	)
	if err := pool.QueryRow(ctx, `SELECT height, expected_hash, actual_hash, kind FROM indexer_pause WHERE chain_id = $1`, chainID).
		Scan(&pauseHeight, &pauseExpected, &pauseStored, &pauseKind); err != nil {
		t.Fatalf("read pause row: %v", err)
	}
	if pauseHeight != int64(startHeight+1) || pauseKind != pauseHashMismatch || pauseExpected != nextHash || pauseStored != pauseActual {
		t.Fatalf("pause row = (%d, %s, %s, %s), want (%d, %s, %s, %s)",
			pauseHeight, pauseExpected, pauseStored, pauseKind, startHeight+1, nextHash, pauseActual, pauseHashMismatch)
	}
}

// TestCoordinationStaleTokenWritesRefused is T015 (data-model case 2): after a
// real DB-clock expiry takeover bumps owner and fencing token, the old token's
// advance and pause transactions are both refused at verdict step 4 with zero
// writes; the new token still works.
func TestCoordinationStaleTokenWritesRefused(t *testing.T) {
	dsn := startIndexerPostgres(t)
	ctx := context.Background()

	const (
		chainID     = int64(31338)
		startHeight = uint64(200)
		ttl         = 1500 * time.Millisecond
		heartbeat   = 400 * time.Millisecond
	)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()

	firstHash := coordHash(0x41)
	nextHash := coordHash(0x42)

	oldLease, oldScanner := seedCoordFirstBlock(t, ctx, pool, chainID, "stale-owner", ttl, heartbeat, startHeight, firstHash)
	oldToken := oldLease.Token()
	before, ok := readCoordCheckpoint(t, ctx, pool, chainID)
	if !ok {
		t.Fatal("checkpoint missing after the first block")
	}

	// Real expiry on the DB clock, then a real takeover: token + 1, owner
	// replaced (lease_integration_test.go covers the lease mechanics; this
	// test owns the write protocol's reaction).
	waitUntil(t, time.Now().Add(20*time.Second), "the seeded lease to expire on the DB clock", func() bool {
		var expired bool
		if err := pool.QueryRow(ctx, `SELECT expires_at <= now() FROM indexer_lease WHERE chain_id = $1`, chainID).Scan(&expired); err != nil {
			t.Fatalf("read lease expiry: %v", err)
		}
		return expired
	})
	takeover := newTestLease(t, pool, chainID, "new-owner", ttl, heartbeat)
	won, newToken, err := takeover.Acquire(ctx)
	if err != nil || !won {
		t.Fatalf("takeover Acquire() = (%v, %d, %v), want (true, %d, nil)", won, newToken, err, oldToken+1)
	}
	if newToken != oldToken+1 {
		t.Fatalf("takeover fencing token = %d, want %d", newToken, oldToken+1)
	}

	// Old-token advance: refused at the independent verdict statement.
	rcap := testRecoveryCap(t, ctx, pool, chainID)
	err = oldScanner.commitBlock(ctx, blockWrite{number: startHeight + 1, hash: nextHash, parent: firstHash, first: false}, rcap)
	if !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale advance error = %v, want ErrLeaseLost", err)
	}
	// Old-token pause transaction: same lock-first protocol, same refusal.
	err = oldScanner.pause(ctx, pauseInfo{kind: pauseHashMismatch, height: startHeight, expected: firstHash, actual: nextHash, detail: "T015 stale token pause"})
	if !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale pause error = %v, want ErrLeaseLost", err)
	}

	requireCoordCheckpoint(t, ctx, pool, chainID, before)
	if got := countChainBlocks(t, ctx, pool, chainID, int64(startHeight+1)); got != 0 {
		t.Fatalf("stale token leaked %d block row(s), want 0", got)
	}
	if got := coordCountPauses(t, ctx, pool, chainID); got != 0 {
		t.Fatalf("stale token leaked %d pause row(s), want 0", got)
	}

	// Positive control: the takeover token writes normally.
	newScanner := newCoordScanner(t, pool, takeover, startHeight)
	if err := newScanner.commitBlock(ctx, blockWrite{number: startHeight + 1, hash: nextHash, parent: firstHash, first: false}, rcap); err != nil {
		t.Fatalf("new-holder advance: %v", err)
	}
	after, ok := readCoordCheckpoint(t, ctx, pool, chainID)
	if !ok || after.height != int64(startHeight+1) || after.hash != nextHash {
		t.Fatalf("checkpoint after new-holder advance = %+v, want height %d hash %s", after, startHeight+1, nextHash)
	}
}

// coordFirstOutcome is one first-block protocol result.
type coordFirstOutcome struct {
	scanner *Scanner
	err     error
}

// raceCoordFirstBlocks runs the first-block write protocol concurrently from
// two sessions behind a start barrier and returns both outcomes.
func raceCoordFirstBlocks(ctx context.Context, a, b *Scanner, number uint64, hashA, hashB string, rcap RecoveryCapture) []coordFirstOutcome {
	start := make(chan struct{})
	out := make(chan coordFirstOutcome, 2)
	run := func(sc *Scanner, hash string) {
		<-start
		out <- coordFirstOutcome{scanner: sc, err: sc.commitBlock(ctx, blockWrite{number: number, hash: hash, parent: coordHash(0), first: true}, rcap)}
	}
	go run(a, hashA)
	go run(b, hashB)
	close(start)
	return []coordFirstOutcome{<-out, <-out}
}

// splitFirstOutcomes requires exactly one committed run and one run rejected
// with errStaleState (the durable state had moved under it).
func splitFirstOutcomes(t *testing.T, outcomes []coordFirstOutcome) (winner, loser *Scanner) {
	t.Helper()
	if len(outcomes) != 2 {
		t.Fatalf("first-block outcomes = %d, want 2", len(outcomes))
	}
	for i, o := range outcomes {
		switch {
		case o.err == nil:
			if winner != nil {
				t.Fatalf("both first-block runs succeeded: %+v", outcomes)
			}
			winner = o.scanner
		case errors.Is(o.err, errStaleState):
			if loser != nil {
				t.Fatalf("both first-block runs were rejected: %+v", outcomes)
			}
			loser = o.scanner
		default:
			t.Fatalf("first-block run %d error = %v, want nil or errStaleState", i, o.err)
		}
	}
	if winner == nil || loser == nil {
		t.Fatalf("first-block race did not produce one winner and one loser: %+v", outcomes)
	}
	return winner, loser
}

func requireSingleCoordFirstBlock(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, startHeight uint64, wantHash string) {
	t.Helper()
	var checkpoints int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM indexer_checkpoint WHERE chain_id = $1`, chainID).Scan(&checkpoints); err != nil {
		t.Fatalf("count indexer_checkpoint: %v", err)
	}
	if checkpoints != 1 {
		t.Fatalf("checkpoint rows = %d, want exactly 1", checkpoints)
	}
	cp, ok := readCoordCheckpoint(t, ctx, pool, chainID)
	if !ok || cp.height != int64(startHeight) || cp.hash != wantHash || cp.startHeight != int64(startHeight) {
		t.Fatalf("checkpoint = %+v, want height/start %d and hash %s", cp, startHeight, wantHash)
	}
	if got := countChainBlocks(t, ctx, pool, chainID, int64(startHeight)); got != 1 {
		t.Fatalf("blocks at S = %d, want 1", got)
	}
	if got := countChainBlocks(t, ctx, pool, chainID, int64(startHeight+1)); got != 0 {
		t.Fatalf("blocks at S+1 = %d, want 0", got)
	}
	if got := coordCountPauses(t, ctx, pool, chainID); got != 0 {
		t.Fatalf("pause rows = %d, want 0", got)
	}
}

// TestCoordinationConcurrentFirstBlock is T016 (data-model case 3): behind a
// start barrier two sessions run the first-block protocol for the same height;
// exactly one commits, the loser re-reads and moves to S+1. With divergent
// hashes the loser takes the pause path instead.
func TestCoordinationConcurrentFirstBlock(t *testing.T) {
	dsn := startIndexerPostgres(t)
	ctx := context.Background()

	const (
		chainID     = int64(31339)
		startHeight = uint64(300)
	)
	poolA := openIndexerPool(t, dsn)
	defer poolA.Close()
	poolB := openIndexerPool(t, dsn)
	defer poolB.Close()

	t.Run("same hash", func(t *testing.T) {
		hash := coordHash(0x55)
		nextHash := coordHash(0x56)
		lease := newTestLease(t, poolA, chainID, "first-writer", time.Minute, time.Second)
		if won, token, err := lease.Acquire(ctx); err != nil || !won {
			t.Fatalf("first-block lease Acquire() = (%v, %d, %v), want win", won, token, err)
		}
		scA := newCoordScanner(t, poolA, lease, startHeight)
		scB := newCoordScanner(t, poolB, lease, startHeight)

		rcap := testRecoveryCap(t, ctx, poolA, chainID)
		_, loser := splitFirstOutcomes(t, raceCoordFirstBlocks(ctx, scA, scB, startHeight, hash, hash, rcap))
		requireSingleCoordFirstBlock(t, ctx, poolA, chainID, startHeight, hash)

		// Case 3: the loser re-reads the durable state and advances to S+1.
		if err := loser.commitBlock(ctx, blockWrite{number: startHeight + 1, hash: nextHash, parent: hash, first: false}, rcap); err != nil {
			t.Fatalf("loser advance to S+1: %v", err)
		}
		cp, ok := readCoordCheckpoint(t, ctx, poolA, chainID)
		if !ok || cp.height != int64(startHeight+1) || cp.hash != nextHash {
			t.Fatalf("checkpoint after loser advance = %+v, want height %d hash %s", cp, startHeight+1, nextHash)
		}
		for _, n := range []uint64{startHeight, startHeight + 1} {
			if got := countChainBlocks(t, ctx, poolA, chainID, int64(n)); got != 1 {
				t.Fatalf("blocks at %d = %d, want 1", n, got)
			}
		}
	})

	t.Run("divergent hash", func(t *testing.T) {
		divergentChain := chainID + 1
		h1, h2 := coordHash(0x61), coordHash(0x62)
		lease := newTestLease(t, poolA, divergentChain, "first-writer-divergent", time.Minute, time.Second)
		if won, token, err := lease.Acquire(ctx); err != nil || !won {
			t.Fatalf("divergent lease Acquire() = (%v, %d, %v), want win", won, token, err)
		}
		scA := newCoordScanner(t, poolA, lease, startHeight)
		scB := newCoordScanner(t, poolB, lease, startHeight)

		rcapDiv := testRecoveryCap(t, ctx, poolA, divergentChain)
		_, loser := splitFirstOutcomes(t, raceCoordFirstBlocks(ctx, scA, scB, startHeight, h1, h2, rcapDiv))
		stored, ok := readCoordCheckpoint(t, ctx, poolA, divergentChain)
		if !ok {
			t.Fatal("winner did not commit a checkpoint")
		}
		if stored.hash != h1 && stored.hash != h2 {
			t.Fatalf("stored first-block hash = %s, want one of the raced hashes", stored.hash)
		}
		loserHash := h1
		if stored.hash == h1 {
			loserHash = h2
		}
		requireSingleCoordFirstBlock(t, ctx, poolA, divergentChain, startHeight, stored.hash)

		// Replay the serve loop's errStaleState branch (scanner.go): a stored
		// checkpoint at the same height with a different hash pauses.
		cp, paused, err := loser.loadProgress(ctx)
		if err != nil || paused != nil {
			t.Fatalf("loser loadProgress() = (%+v, %+v, %v), want (checkpoint, nil, nil)", cp, paused, err)
		}
		if cp == nil || cp.height != startHeight || cp.hash != stored.hash {
			t.Fatalf("loser re-read checkpoint = %+v, want height %d hash %s", cp, startHeight, stored.hash)
		}
		perr := loser.pause(ctx, pauseInfo{
			kind:     pauseHashMismatch,
			height:   startHeight,
			expected: stored.hash,
			actual:   loserHash,
			detail:   "T016 divergent first block",
		})
		if !errors.Is(perr, errPaused) {
			t.Fatalf("pause after divergent first block = %v, want errPaused", perr)
		}
		var (
			pauseHeight   int64
			pauseExpected string
			pauseActual   string
			pauseKind     string
		)
		if err := poolA.QueryRow(ctx, `SELECT height, expected_hash, actual_hash, kind FROM indexer_pause WHERE chain_id = $1`, divergentChain).
			Scan(&pauseHeight, &pauseExpected, &pauseActual, &pauseKind); err != nil {
			t.Fatalf("read divergent pause row: %v", err)
		}
		if pauseHeight != int64(startHeight) || pauseKind != pauseHashMismatch || pauseExpected != stored.hash || pauseActual != loserHash {
			t.Fatalf("pause row = (%d, %s, %s, %s), want (%d, %s, %s, %s)",
				pauseHeight, pauseExpected, pauseActual, pauseKind, startHeight, stored.hash, loserHash, pauseHashMismatch)
		}
		// Nothing beyond the one first block was written.
		requireCoordCheckpoint(t, ctx, poolA, divergentChain, stored)
		if got := countChainBlocks(t, ctx, poolA, divergentChain, int64(startHeight+1)); got != 0 {
			t.Fatalf("blocks at S+1 = %d, want 0", got)
		}
	})
}

// TestCoordinationPauseFreezesCheckpoint is T017: once a pause row is
// committed, N concurrent first-block and advance attempts from multiple
// sessions are all refused, and the checkpoint row is exactly identical
// before and after (every column, including updated_at).
func TestCoordinationPauseFreezesCheckpoint(t *testing.T) {
	dsn := startIndexerPostgres(t)
	ctx := context.Background()

	const (
		chainID     = int64(31341)
		startHeight = uint64(400)
		attempts    = 6
	)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()

	firstHash := coordHash(0x71)
	nextHash := coordHash(0x72)
	lease, _ := seedCoordFirstBlock(t, ctx, pool, chainID, "pause-holder", time.Minute, time.Second, startHeight, firstHash)
	pre, ok := readCoordCheckpoint(t, ctx, pool, chainID)
	if !ok {
		t.Fatal("checkpoint missing before the pause")
	}

	// Commit the durable pause exactly as the pause transaction writes it.
	if _, err := pool.Exec(ctx, insertPauseSQL, chainID, int64(startHeight+1), nextHash, coordHash(0x73), pauseHashMismatch, "T017 freeze baseline"); err != nil {
		t.Fatalf("commit pause row: %v", err)
	}
	post, ok := readCoordCheckpoint(t, ctx, pool, chainID)
	if !ok || !sameCoordCheckpoint(pre, post) {
		t.Fatalf("checkpoint changed by the pause commit: got %+v, want %+v", post, pre)
	}

	// N attempts from separate pooled sessions behind one start barrier: half
	// replay the first-block protocol, half try to advance S+1. All present the
	// live owner/token, so only the pause verdict can refuse them.
	start := make(chan struct{})
	errs := make(chan error, attempts)
	var wg sync.WaitGroup
	rcap := testRecoveryCap(t, ctx, pool, chainID)
	for i := 0; i < attempts; i++ {
		number, first := startHeight+1, false
		if i%2 == 0 {
			number, first = startHeight, true
		}
		sc := newCoordScanner(t, pool, lease, startHeight)
		wg.Add(1)
		go func(sc *Scanner, number uint64, first bool) {
			defer wg.Done()
			<-start
			errs <- sc.commitBlock(ctx, blockWrite{number: number, hash: nextHash, parent: firstHash, first: first}, rcap)
		}(sc, number, first)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if !errors.Is(err, errPaused) {
			t.Fatalf("paused write attempt = %v, want errPaused", err)
		}
	}

	final, ok := readCoordCheckpoint(t, ctx, pool, chainID)
	if !ok || !sameCoordCheckpoint(pre, final) {
		t.Fatalf("checkpoint changed across %d paused attempts: got %+v, want %+v", attempts, final, pre)
	}
	if got := countChainBlocks(t, ctx, pool, chainID, int64(startHeight+1)); got != 0 {
		t.Fatalf("paused attempts wrote %d block row(s) at S+1, want 0", got)
	}
	if got := coordCountPauses(t, ctx, pool, chainID); got != 1 {
		t.Fatalf("pause rows = %d, want exactly 1", got)
	}
}
