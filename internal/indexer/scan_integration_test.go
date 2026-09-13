//go:build integration

// T007-T013: acceptance-scenario tests for the 002 chain indexer on a real
// PostgreSQL. Chain behavior comes from a scripted in-memory HeaderClient
// (deterministic fault injection: outages, wrong parents, changed hashes,
// wrong chain id); every ordering decision is an explicit sync point
// (channels, start barriers, bounded conditional waits). Nothing is ordered
// by sleeping. Docker-gated via startIndexerPostgres (skips when unhealthy).
package indexer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/eth"
)

const scanChainID = int64(31337)

// scriptChain is a deterministic in-memory chain with programmable faults.
type scriptChain struct {
	mu       sync.Mutex
	id       *big.Int
	idErr    error
	heads    map[uint64]*types.Header
	errQueue map[uint64][]error
	fetches  atomic.Int64
}

func newScriptChain(id int64, upto uint64, tag byte) *scriptChain {
	c := &scriptChain{id: big.NewInt(id), heads: map[uint64]*types.Header{}, errQueue: map[uint64][]error{}}
	parent := common.Hash{}
	for n := uint64(0); n <= upto; n++ {
		h := &types.Header{Number: new(big.Int).SetUint64(n), ParentHash: parent, Extra: []byte{tag, byte(n)}, Time: 1000 + n, GasLimit: 30_000_000}
		c.heads[n] = h
		parent = h.Hash()
	}
	return c
}

func (c *scriptChain) ChainID(context.Context) (*big.Int, error) {
	if c.idErr != nil {
		return nil, c.idErr
	}
	return c.id, nil
}

func (c *scriptChain) HeaderByNumber(_ context.Context, n *big.Int) (*types.Header, error) {
	c.fetches.Add(1)
	c.mu.Lock()
	defer c.mu.Unlock()
	h := n.Uint64()
	if len(c.errQueue[h]) > 0 {
		err := c.errQueue[h][0]
		c.errQueue[h] = c.errQueue[h][1:]
		return nil, err
	}
	hdr, ok := c.heads[h]
	if !ok {
		return nil, &eth.Error{Kind: eth.KindNotFound, Op: "stub", Err: errors.New("stub: no header")}
	}
	return hdr, nil
}

func (c *scriptChain) queueErr(height uint64, errs ...error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.errQueue[height] = append(c.errQueue[height], errs...)
}

func (c *scriptChain) extend(upto uint64, tag byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var parent common.Hash
	var start uint64
	for n, h := range c.heads {
		if n >= start {
			start = n + 1
			parent = h.Hash()
		}
	}
	for n := start; n <= upto; n++ {
		h := &types.Header{Number: new(big.Int).SetUint64(n), ParentHash: parent, Extra: []byte{tag, byte(n)}, Time: 1000 + n, GasLimit: 30_000_000}
		c.heads[n] = h
		parent = h.Hash()
	}
}

func scanTransportErr() error {
	return &eth.Error{Kind: eth.KindTransport, Op: "stub", Err: errors.New("stub: connection refused")}
}

func newScanScanner(t *testing.T, pool *pgxpool.Pool, chain HeaderClient, owner string, startHeight uint64) (*Lease, *Scanner) {
	t.Helper()
	lease := newTestLease(t, pool, scanChainID, owner, 3*time.Second, time.Second)
	sc, err := NewScanner(pool, chain, lease, Config{
		StartHeight:  startHeight,
		RPCTimeout:   2 * time.Second,
		PollInterval: 25 * time.Millisecond,
		RetryInitial: 25 * time.Millisecond,
		RetryMax:     250 * time.Millisecond,
	}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("NewScanner(): %v", err)
	}
	return lease, sc
}

// runScan runs sc until checkpoint reaches want (bounded), then cancels and
// requires a nil Run error (clean shutdown, no partial progress).
func runScanTo(t *testing.T, sc *Scanner, want uint64, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- sc.Run(ctx) }()
	deadline := time.Now().Add(timeout)
	waitUntil(t, deadline, fmt.Sprintf("checkpoint reaches %d", want), func() bool {
		h, _, ok := sc.Checkpoint()
		return ok && h >= want
	})
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() after cancel = %v, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run() did not return after cancel")
	}
}

// seedScanTo deterministically commits heights [S,N] through the real write
// protocol (lease Acquire + commitBlock loop, no Run loop, hence no overshoot
// race). It returns the winning lease, its scanner (whose fencing token
// matches durable truth), and the resulting checkpoint height (always N).
func seedScanTo(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chain *scriptChain, owner string, S, N uint64) (*Lease, *Scanner, uint64) {
	t.Helper()
	lease := newTestLease(t, pool, scanChainID, owner, 3*time.Second, time.Second)
	won, _, err := lease.Acquire(ctx)
	if err != nil || !won {
		t.Fatalf("seed acquire = (%v, %v), want win", won, err)
	}
	sc, err := NewScanner(pool, chain, lease, Config{
		StartHeight: S, RPCTimeout: 2 * time.Second, PollInterval: 25 * time.Millisecond,
		RetryInitial: 25 * time.Millisecond, RetryMax: 250 * time.Millisecond,
	}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("NewScanner(): %v", err)
	}
	type hw struct {
		n uint64
		h string
		p string
	}
	chain.mu.Lock()
	var writes []hw
	for n := S; n <= N; n++ {
		hdr := chain.heads[n]
		if hdr == nil {
			chain.mu.Unlock()
			t.Fatalf("seed chain missing height %d", n)
		}
		writes = append(writes, hw{n, hashHex(hdr.Hash()), hashHex(hdr.ParentHash)})
	}
	chain.mu.Unlock()
	for _, w := range writes {
		if err := sc.commitBlock(ctx, blockWrite{number: w.n, hash: w.h, parent: w.p, first: w.n == S}); err != nil {
			t.Fatalf("seed commit %d: %v", w.n, err)
		}
	}
	return lease, sc, N
}

func scanBlockCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, from, to uint64) int {
	t.Helper()
	var n int
	err := pool.QueryRow(ctx, `SELECT count(*) FROM chain_blocks WHERE chain_id=$1 AND number BETWEEN $2 AND $3`, scanChainID, int64(from), int64(to)).Scan(&n)
	if err != nil {
		t.Fatalf("count blocks: %v", err)
	}
	return n
}

// assertScanGapFree fails when any height in [from,to] is missing (I6/I4).
func assertScanGapFree(t *testing.T, ctx context.Context, pool *pgxpool.Pool, from, to uint64) {
	t.Helper()
	var missing int
	err := pool.QueryRow(ctx, `SELECT count(*) FROM generate_series($2::bigint,$3::bigint) g(n)
		LEFT JOIN chain_blocks b ON b.chain_id=$1 AND b.number=g.n WHERE b.number IS NULL`,
		scanChainID, int64(from), int64(to)).Scan(&missing)
	if err != nil {
		t.Fatalf("gap check: %v", err)
	}
	if missing != 0 {
		t.Fatalf("gap check: %d heights missing in [%d,%d]", missing, from, to)
	}
}

// assertScanLinked fails when any non-boundary block's parent differs from its
// stored predecessor (I3). boundaryZero skips height 0's parent check.
func assertScanLinked(t *testing.T, ctx context.Context, pool *pgxpool.Pool, from, to uint64) {
	t.Helper()
	var bad int
	err := pool.QueryRow(ctx, `SELECT count(*) FROM chain_blocks cur
		JOIN chain_blocks prev ON prev.chain_id=cur.chain_id AND prev.number=cur.number-1
		WHERE cur.chain_id=$1 AND cur.number BETWEEN $2 AND $3 AND cur.parent_hash <> prev.hash`,
		scanChainID, int64(from), int64(to)).Scan(&bad)
	if err != nil {
		t.Fatalf("linkage check: %v", err)
	}
	if bad != 0 {
		t.Fatalf("linkage check: %d blocks with mismatched parent", bad)
	}
}

// assertScanSingleRecord fails on duplicate effective rows (I1).
func assertScanSingleRecord(t *testing.T, ctx context.Context, pool *pgxpool.Pool, from, to uint64) {
	t.Helper()
	var dups int
	err := pool.QueryRow(ctx, `SELECT count(*) FROM (SELECT chain_id, number FROM chain_blocks
		WHERE chain_id=$1 AND number BETWEEN $2 AND $3 GROUP BY 1,2 HAVING count(*)>1) d`,
		scanChainID, int64(from), int64(to)).Scan(&dups)
	if err != nil {
		t.Fatalf("dup check: %v", err)
	}
	if dups != 0 {
		t.Fatalf("dup check: %d heights with duplicate records", dups)
	}
}

func scanPauseRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool) (height int64, expected, actual, kind string, ok bool) {
	t.Helper()
	err := pool.QueryRow(ctx, `SELECT height, expected_hash, actual_hash, kind FROM indexer_pause WHERE chain_id=$1`, scanChainID).Scan(&height, &expected, &actual, &kind)
	if err != nil {
		return 0, "", "", "", false
	}
	return height, expected, actual, kind, true
}

// TestScanFirstScanOrderAndRestart: scenes 1+5, FR-02, SC-01.
func TestScanFirstScanOrderAndRestart(t *testing.T) {
	dsn := startIndexerPostgres(t)
	ctx := context.Background()
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	chain := newScriptChain(31337, 9, 0xA1)
	_, sc := newScanScanner(t, pool, chain, "scan-a", 3)

	runScanTo(t, sc, 6, 15*time.Second)
	// The run may commit one block past the observed target before observing
	// cancel; anchor every assertion on the actual checkpoint, never on 6.
	cp0, ok := readCoordCheckpoint(t, ctx, pool, scanChainID)
	if !ok {
		t.Fatal("no checkpoint after first run")
	}
	H := uint64(cp0.height)
	if H < 6 {
		t.Fatalf("checkpoint = %d, want >= 6", H)
	}
	var minN int64
	if err := pool.QueryRow(ctx, `SELECT min(number) FROM chain_blocks WHERE chain_id=$1`, scanChainID).Scan(&minN); err != nil {
		t.Fatalf("min height: %v", err)
	}
	if minN != 3 {
		t.Fatalf("first persisted height = %d, want S=3", minN)
	}
	assertScanGapFree(t, ctx, pool, 3, H)
	if H >= 4 {
		assertScanLinked(t, ctx, pool, 4, H)
	}
	assertScanSingleRecord(t, ctx, pool, 3, H)

	// Restart with a fresh instance: must continue from N+1 with no gap.
	_, sc2 := newScanScanner(t, pool, chain, "scan-b", 3)
	runScanTo(t, sc2, 9, 15*time.Second)
	assertScanGapFree(t, ctx, pool, 3, 9)
	assertScanLinked(t, ctx, pool, 4, 9)
	assertScanSingleRecord(t, ctx, pool, 3, 9)
	cp, ok := readCoordCheckpoint(t, ctx, pool, scanChainID)
	if !ok || cp.height != 9 {
		t.Fatalf("checkpoint = %+v, want height 9", cp)
	}
}

// TestScanStartHeightChangeRefused: FR-03/US1-AC4 — S change with checkpoint
// refuses startup and leaves blocks/checkpoint/start_height untouched.
func TestScanStartHeightChangeRefused(t *testing.T) {
	dsn := startIndexerPostgres(t)
	ctx := context.Background()
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	chain := newScriptChain(31337, 9, 0xA2)
	_, sc := newScanScanner(t, pool, chain, "scan-a", 3)
	runScanTo(t, sc, 5, 15*time.Second)
	before, ok := readCoordCheckpoint(t, ctx, pool, scanChainID)
	if !ok {
		t.Fatal("no checkpoint after first run")
	}
	H := uint64(before.height)

	_, sc2 := newScanScanner(t, pool, chain, "scan-b", H+10)
	rctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := sc2.Run(rctx)
	if err == nil || !strings.Contains(err.Error(), "start height changed") {
		t.Fatalf("Run() with changed S = %v, want start-height refusal", err)
	}
	after, ok := readCoordCheckpoint(t, ctx, pool, scanChainID)
	if !ok || !sameCoordCheckpoint(before, after) {
		t.Fatalf("checkpoint changed by refused run: before=%+v after=%+v", before, after)
	}
	if got := scanBlockCount(t, ctx, pool, H+1, 100); got != 0 {
		t.Fatalf("refused run wrote %d blocks, want 0", got)
	}
}

// TestScanGenesisBoundary: C3 — S=0 genesis has zero parent, builds checkpoint,
// and S+1 links normally (FR-05).
func TestScanGenesisBoundary(t *testing.T) {
	dsn := startIndexerPostgres(t)
	ctx := context.Background()
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	chain := newScriptChain(31337, 3, 0xA3)
	_, sc := newScanScanner(t, pool, chain, "scan-a", 0)
	runScanTo(t, sc, 2, 15*time.Second)
	var parent string
	if err := pool.QueryRow(ctx, `SELECT parent_hash FROM chain_blocks WHERE chain_id=$1 AND number=0`, scanChainID).Scan(&parent); err != nil {
		t.Fatalf("genesis row: %v", err)
	}
	if parent != "0x"+strings.Repeat("00", 32) {
		t.Fatalf("genesis parent = %s, want all-zero", parent)
	}
	cp, ok := readCoordCheckpoint(t, ctx, pool, scanChainID)
	if !ok || cp.height < 2 || cp.startHeight != 0 {
		t.Fatalf("checkpoint = %+v, want height >= 2 start 0", cp)
	}
	H := uint64(cp.height)
	assertScanLinked(t, ctx, pool, 1, H)
	assertScanSingleRecord(t, ctx, pool, 0, H)
}

// TestScanStartAboveHeadWaits: scene 2, FR-10, clarification #1.
func TestScanStartAboveHeadWaits(t *testing.T) {
	dsn := startIndexerPostgres(t)
	ctx := context.Background()
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	chain := newScriptChain(31337, 9, 0xA4)
	_, sc := newScanScanner(t, pool, chain, "scan-a", 50)
	rctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- sc.Run(rctx) }()
	waitUntil(t, time.Now().Add(10*time.Second), "scanner waiting for S", func() bool {
		return sc.State() == StateWaiting
	})
	select {
	case err := <-done:
		t.Fatalf("Run() returned %v while S above head, want quiet wait", err)
	case <-time.After(300 * time.Millisecond):
	}
	if got := scanBlockCount(t, ctx, pool, 0, 1000); got != 0 {
		t.Fatalf("wrote %d blocks while S above head, want 0", got)
	}
	if _, ok := readCoordCheckpoint(t, ctx, pool, scanChainID); ok {
		t.Fatal("checkpoint created while S above head, want empty progress")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() after cancel = %v, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run() did not return after cancel")
	}
}

// TestScanCatchUpWaitsForNewBlocks: scene 3, FR-10.
func TestScanCatchUpWaitsForNewBlocks(t *testing.T) {
	dsn := startIndexerPostgres(t)
	ctx := context.Background()
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	chain := newScriptChain(31337, 5, 0xA5)
	_, sc := newScanScanner(t, pool, chain, "scan-a", 2)
	rctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- sc.Run(rctx) }()
	waitUntil(t, time.Now().Add(15*time.Second), "checkpoint reaches 5", func() bool {
		h, _, ok := sc.Checkpoint()
		return ok && h >= 5
	})
	waitUntil(t, time.Now().Add(10*time.Second), "scanner waiting at head", func() bool {
		return sc.State() == StateWaiting
	})
	select {
	case err := <-done:
		t.Fatalf("Run() returned %v at head, want quiet wait", err)
	case <-time.After(200 * time.Millisecond):
	}
	chain.extend(7, 0xA5)
	waitUntil(t, time.Now().Add(15*time.Second), "checkpoint resumes to 7", func() bool {
		h, _, ok := sc.Checkpoint()
		return ok && h >= 7
	})
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() after cancel = %v, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run() did not return after cancel")
	}
	assertScanGapFree(t, ctx, pool, 2, 7)
}

// TestScanRPCOutageRecovery: scene 6, FR-09 — transport errors retry with
// backoff, checkpoint frozen, then auto-continue (SC-03).
func TestScanRPCOutageRecovery(t *testing.T) {
	dsn := startIndexerPostgres(t)
	ctx := context.Background()
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	chain := newScriptChain(31337, 8, 0xA6)
	for n := uint64(4); n <= 8; n++ {
		chain.queueErr(n, scanTransportErr(), scanTransportErr(), scanTransportErr())
	}
	_, sc := newScanScanner(t, pool, chain, "scan-a", 4)
	runScanTo(t, sc, 8, 30*time.Second)
	assertScanGapFree(t, ctx, pool, 4, 8)
	assertScanSingleRecord(t, ctx, pool, 4, 8)
}

// TestScanDBFailureNoPartialProgress: scene 7, FR-06/FR-09 — pool failure
// retries without checkpoint-without-block; a fresh pool/instance recovers.
func TestScanDBFailureNoPartialProgress(t *testing.T) {
	dsn := startIndexerPostgres(t)
	ctx := context.Background()
	pool := openIndexerPool(t, dsn)
	chain := newScriptChain(31337, 8, 0xA7)
	_, sc := newScanScanner(t, pool, chain, "scan-a", 2)
	runScanTo(t, sc, 4, 15*time.Second)
	pool.Close() // hard DB failure: in-flight and future statements fail.
	// A fresh instance on the dead pool must surface retrying (never success,
	// never partial progress) and still exit cleanly on cancel.
	_, scDead := newScanScanner(t, pool, chain, "scan-dead", 2)
	drctx, dcancel := context.WithCancel(context.Background())
	defer dcancel()
	deadDone := make(chan error, 1)
	go func() { deadDone <- scDead.Run(drctx) }()
	waitUntil(t, time.Now().Add(10*time.Second), "scanner retrying on DB failure", func() bool {
		return scDead.State() == StateRetrying
	})
	dcancel()
	select {
	case <-deadDone:
	case <-time.After(10 * time.Second):
		t.Fatal("Run() did not return after cancel during DB failure")
	}
	pool2 := openIndexerPool(t, dsn)
	defer pool2.Close()
	_, sc2 := newScanScanner(t, pool2, chain, "scan-b", 2)
	runScanTo(t, sc2, 8, 20*time.Second)
	assertScanGapFree(t, ctx, pool2, 2, 8)
	var orphan int
	if err := pool2.QueryRow(ctx, `SELECT count(*) FROM indexer_checkpoint c
		LEFT JOIN chain_blocks b ON b.chain_id=c.chain_id AND b.number=c.height AND b.hash=c.block_hash
		WHERE c.chain_id=$1 AND b.number IS NULL`, scanChainID).Scan(&orphan); err != nil {
		t.Fatalf("orphan check: %v", err)
	}
	if orphan != 0 {
		t.Fatal("checkpoint without matching block exists")
	}
}

// TestScanUncertainCommitIdempotent: scene 8, FR-07/FR-13 — concurrent
// double-commit of one height converges to a single consistent state.
func TestScanUncertainCommitIdempotent(t *testing.T) {
	dsn := startIndexerPostgres(t)
	ctx := context.Background()
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	chain := newScriptChain(31337, 30, 0xA8)
	// Deterministic setup: seed exactly [4,4] through the real write protocol.
	// No Run loop runs here, so there is no cancel-observation overshoot and
	// next is pinned to 5 by construction (the old runScanTo setup raced with
	// scanner speed against the fixed chain length). The returned scanner's
	// lease token matches durable truth, so the racy pair below exercises the
	// real fencing path.
	_, seedSC, _ := seedScanTo(t, ctx, pool, chain, "scan-a", 4, 4)
	// Anchor on the actual checkpoint and pin the deterministic base: any
	// deviation means the seeding itself regressed.
	base, ok := readCoordCheckpoint(t, ctx, pool, scanChainID)
	if !ok {
		t.Fatal("no checkpoint after seeding")
	}
	if uint64(base.height) != 4 || uint64(base.startHeight) != 4 {
		t.Fatalf("seeded checkpoint = %+v, want height 4 start 4", base)
	}
	next := uint64(base.height) + 1
	hn := hashHex(chain.heads[next].Hash())
	// Parent comes from durable truth, not from a second chain read.
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = seedSC.commitBlock(ctx, blockWrite{number: next, hash: hn, parent: base.hash})
		}(i)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil && !errors.Is(err, errStaleState) {
			t.Fatalf("concurrent commitBlock = %v, want nil or stale-state", err)
		}
	}
	if got := scanBlockCount(t, ctx, pool, next, next); got != 1 {
		t.Fatalf("height %d rows = %d, want 1", next, got)
	}
	cp, ok := readCoordCheckpoint(t, ctx, pool, scanChainID)
	if !ok || uint64(cp.height) != next || cp.hash != hn || uint64(cp.startHeight) != 4 {
		t.Fatalf("checkpoint = %+v, want (%d,%s,4)", cp, next, hn)
	}
	// Idempotent continuation proves no skip/double after doubt; next+5 stays
	// far below the tip by construction, so the Run loop cannot outrun the
	// chain. A fresh owner models the restart after the uncertain commit.
	_, sc2 := newScanScanner(t, pool, chain, "scan-b", 4)
	runScanTo(t, sc2, next+5, 30*time.Second)
	assertScanGapFree(t, ctx, pool, 4, next+5)
	assertScanSingleRecord(t, ctx, pool, 4, next+5)
}

// TestScanSafeExit: scene 13, FR-13 — cancel mid-loop leaves paired state.
func TestScanSafeExit(t *testing.T) {
	dsn := startIndexerPostgres(t)
	ctx := context.Background()
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	chain := newScriptChain(31337, 30, 0xA9)
	_, sc := newScanScanner(t, pool, chain, "scan-a", 0)
	rctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- sc.Run(rctx) }()
	waitUntil(t, time.Now().Add(15*time.Second), "progress starts", func() bool {
		_, _, ok := sc.Checkpoint()
		return ok
	})
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() after cancel = %v, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run() did not exit within shutdown budget")
	}
	if cp, ok := readCoordCheckpoint(t, ctx, pool, scanChainID); ok {
		requireCoordCheckpoint(t, ctx, pool, scanChainID, cp)
		assertScanGapFree(t, ctx, pool, 0, uint64(cp.height))
	}
}

// TestScanDuplicateDeliveryIdempotent: scene 4, FR-07, SC-02.
func TestScanDuplicateDeliveryIdempotent(t *testing.T) {
	dsn := startIndexerPostgres(t)
	ctx := context.Background()
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	chain := newScriptChain(31337, 5, 0xAA)
	_, sc := newScanScanner(t, pool, chain, "scan-a", 4)
	h4 := hashHex(chain.heads[4].Hash())
	p4 := hashHex(chain.heads[4].ParentHash)
	for i := 0; i < 3; i++ {
		if err := sc.commitBlock(ctx, blockWrite{number: 4, hash: h4, parent: p4, first: i == 0}); err != nil && !errors.Is(err, errStaleState) {
			t.Fatalf("delivery %d: %v", i, err)
		}
	}
	if got := scanBlockCount(t, ctx, pool, 4, 4); got != 1 {
		t.Fatalf("height 4 rows = %d, want 1", got)
	}
}

// TestScanTwoInstancesConverge: scene 9, FR-08, SC-05.
func TestScanTwoInstancesConverge(t *testing.T) {
	dsn := startIndexerPostgres(t)
	ctx := context.Background()
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	chain := newScriptChain(31337, 12, 0xAB)
	_, sa := newScanScanner(t, pool, chain, "scan-a", 2)
	_, sb := newScanScanner(t, pool, chain, "scan-b", 2)
	rctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 2)
	go func() { done <- sa.Run(rctx) }()
	go func() { done <- sb.Run(rctx) }()
	waitUntil(t, time.Now().Add(30*time.Second), "dual instances reach 12", func() bool {
		var h int64
		err := pool.QueryRow(ctx, `SELECT height FROM indexer_checkpoint WHERE chain_id=$1`, scanChainID).Scan(&h)
		return err == nil && h >= 12
	})
	cancel()
	for i := 0; i < 2; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Run() after cancel = %v, want nil", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("Run() did not return after cancel")
		}
	}
	assertScanGapFree(t, ctx, pool, 2, 12)
	assertScanLinked(t, ctx, pool, 3, 12)
	assertScanSingleRecord(t, ctx, pool, 2, 12)
}

// TestScanParentBreakPauses: scene 10, FR-12 — wrong parent pauses with
// diagnostics, history intact, restart still refused.
func TestScanParentBreakPauses(t *testing.T) {
	dsn := startIndexerPostgres(t)
	ctx := context.Background()
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	chain := newScriptChain(31337, 7, 0xAC)
	seedScanTo(t, ctx, pool, chain, "scan-seed", 2, 5)
	// Corrupt the next header's parent on the chain side only.
	chain.mu.Lock()
	broken := *chain.heads[6]
	broken.ParentHash = common.HexToHash(coordHash(0xFF))
	chain.heads[6] = &broken
	chain.mu.Unlock()
	_, sc := newScanScanner(t, pool, chain, "scan-a", 2)
	rctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	err := sc.Run(rctx)
	if err == nil || !errors.Is(err, errPaused) {
		t.Fatalf("Run() on parent break = %v, want paused", err)
	}
	h, expected, actual, kind, ok := scanPauseRow(t, ctx, pool)
	if !ok || kind != pauseParentMismatch || h != 6 {
		t.Fatalf("pause row = (%d,%s,%s,%s,%v), want parent_mismatch at 6", h, expected, actual, kind, ok)
	}
	if expected == "" || actual == "" {
		t.Fatal("pause row missing diagnostic hashes")
	}
	cp, _ := readCoordCheckpoint(t, ctx, pool, scanChainID)
	if cp.height != 5 {
		t.Fatalf("checkpoint moved to %d during pause, want 5", cp.height)
	}
	// Restart must still refuse.
	_, sc2 := newScanScanner(t, pool, chain, "scan-b", 2)
	rctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()
	if err := sc2.Run(rctx2); !errors.Is(err, errPaused) {
		t.Fatalf("restart Run() = %v, want paused", err)
	}
}

// TestScanCheckpointChangedPauses: scene 11, FR-11 — chain re-resolves the
// checkpoint height to another hash while down.
func TestScanCheckpointChangedPauses(t *testing.T) {
	dsn := startIndexerPostgres(t)
	ctx := context.Background()
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	chain := newScriptChain(31337, 6, 0xAD)
	seedScanTo(t, ctx, pool, chain, "scan-seed", 2, 4)
	// Simulate downtime reorg: rebuild the chain with a different tag so
	// height 4 (and later) resolve to different hashes.
	chain2 := newScriptChain(31337, 6, 0xAE)
	chain.mu.Lock()
	chain.heads = chain2.heads
	chain.mu.Unlock()
	_, sc2 := newScanScanner(t, pool, chain, "scan-b", 2)
	rctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := sc2.Run(rctx); !errors.Is(err, errPaused) {
		t.Fatalf("Run() after checkpoint change = %v, want paused", err)
	}
	_, _, _, kind, ok := scanPauseRow(t, ctx, pool)
	if !ok || kind != pauseCheckpointChanged {
		t.Fatalf("pause kind = %s, want checkpoint_changed", kind)
	}
	cp, _ := readCoordCheckpoint(t, ctx, pool, scanChainID)
	if cp.height != 4 {
		t.Fatalf("checkpoint moved to %d, want 4", cp.height)
	}
}

// TestScanChainIDMismatchRefuses: scene 12, FR-01 — zero writes, diagnostic.
func TestScanChainIDMismatchRefuses(t *testing.T) {
	dsn := startIndexerPostgres(t)
	ctx := context.Background()
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	chain := newScriptChain(99999, 6, 0xAF)
	_, sc := newScanScanner(t, pool, chain, "scan-a", 2)
	rctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := sc.Run(rctx)
	if err == nil || !strings.Contains(err.Error(), "chain id mismatch") {
		t.Fatalf("Run() on wrong chain = %v, want mismatch refusal", err)
	}
	if got := scanBlockCount(t, ctx, pool, 0, 100); got != 0 {
		t.Fatalf("wrote %d blocks on wrong chain, want 0", got)
	}
	if _, ok := readCoordCheckpoint(t, ctx, pool, scanChainID); ok {
		t.Fatal("checkpoint created on wrong chain, want empty")
	}
}

// TestScanObservabilityContract: T013 — contract names mappable, pause row
// queryable, logs carry no credentials.
func TestScanObservabilityContract(t *testing.T) {
	var _ State = StateRunning | StateWaiting | StateRetrying | StatePaused
	if int(StateRunning) != 0 || int(StateWaiting) != 1 || int(StateRetrying) != 2 || int(StatePaused) != 3 {
		t.Fatal("state values drifted from contracts/observability.md (0/1/2/3)")
	}
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	var buf bytes.Buffer
	chain := newScriptChain(31337, 4, 0xB0)
	lease := newTestLease(t, pool, scanChainID, "scan-a", 3*time.Second, time.Second)
	sc, err := NewScanner(pool, chain, lease, Config{
		StartHeight: 2, RPCTimeout: 2 * time.Second, PollInterval: 25 * time.Millisecond,
		RetryInitial: 25 * time.Millisecond, RetryMax: 250 * time.Millisecond,
	}, slog.New(slog.NewTextHandler(&buf, nil)))
	if err != nil {
		t.Fatalf("NewScanner(): %v", err)
	}
	runScanTo(t, sc, 4, 15*time.Second)
	out := buf.String()
	for _, secret := range []string{"sslmode", "postgres://", "txharbor:txharbor@"} {
		if strings.Contains(out, secret) {
			t.Fatalf("log output leaks credential pattern %q", secret)
		}
	}
}
