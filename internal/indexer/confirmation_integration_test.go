//go:build integration

// T013 [US1] threshold-crossing exactly-once conversion (quickstart D1 main
// path, tasks.md lines 110-115): Anvil is the real chain truth, the real
// wired confirmation stack (NewConfirmationCommitter +
// NewConfirmationScanner.ServeLoop + metrics.New + acquired lease, mirroring
// the serve.go/RunQuatro wiring) converts every eligible Pending exactly
// once with all six basis columns exact.
//
// Method, recorded as required: Anvil (ghcr.io/foundry-rs/foundry:v1.8.1,
// chain-id 31337) is mined for real; every chain_blocks row carries the real
// Anvil header hash (fetched via HeaderByNumber, never forged), so the tip
// the scanner reads IS the Anvil tip and the candidate gate adjudicates real
// chain-view data. Pending observations use the documented direct seed
// (depositSeedHistory + depositSeedObservation, the "upstream already
// committed" prerequisite shape per research R9) because the 004 recognition
// chain is already proven by TestDepositAnvilFullStackPending; re-running
// 002/003/004 here would prove nothing new about confirmation. The
// confirmation loop path itself is never faked.
//
// Flow (N=10): mine Anvil to head=18 (all three candidates at h=10,11,12
// below depth: 9/8/7 < 10) -> run the real loop -> assert zero commits and
// zero policy rows (bootstrap has not happened). Then mine to head=25
// (confirmations 16/15/14), extend chain_blocks with the new real headers,
// re-run the real loop -> each row converts exactly once with
// confirmed_at non-null, tip == (25, anvilHeadHash), threshold == 10,
// confirmations == tip-h+1 exact, policy_seq == 1 bootstrap;
// confirmed_total{ok} and transition_total{ok} +1 per row. One observation
// carries the old 004 version_seq=1 (carryover while history is at seq 2):
// it confirms normally on canonical+threshold. Version-agnostic is NOT gate
// bypass: the pause sub-case proves zero commits under a present pause row
// (chain-view/policy pauses are covered by confirmcommit_integration_test.go
// mismatch paths; the scanner halt on those is asserted here via the pause
// representative).
//
// Empty-tip sub-case: a Pending row with no chain_blocks at all -> the loop
// waits (state=1) with zero commits and zero policy rows.
//
// Append discipline: T016-T027 append below in order (-- T0xx banners --);
// helpers are confirm13*-prefixed so later batches never collide.
package indexer

import (
	"context"
	"errors"
	"math/big"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/eth"
	"github.com/xtianxx/txharbor/internal/metrics"
)

// --- T013 helpers -----------------------------------------------------------

// confirm13SeedCanonicalReal inserts chain_blocks rows carrying the caller's
// hashes (real Anvil header hashes in this file) with exact parent linkage.
func confirm13SeedCanonicalReal(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, hashes map[uint64]string, from, to uint64) {
	t.Helper()
	for n := from; n <= to; n++ {
		h, ok := hashes[n]
		if !ok {
			t.Fatalf("no header hash for height %d", n)
		}
		parent := "0x0000000000000000000000000000000000000000000000000000000000000000"
		if n > 0 {
			p, ok := hashes[n-1]
			if !ok {
				t.Fatalf("no parent hash for height %d", n)
			}
			parent = p
		}
		if _, err := pool.Exec(ctx, `
INSERT INTO chain_blocks (chain_id, number, hash, parent_hash, canonical)
VALUES ($1, $2, $3, $4, $5)`, chainID, int64(n), h, parent, true); err != nil {
			t.Fatalf("seed real chain_blocks %d/%d: %v", chainID, n, err)
		}
	}
}

// confirm13AnvilHashes fetches real header hashes for [from, to] from Anvil.
func confirm13AnvilHashes(t *testing.T, ctx context.Context, client *eth.Client, from, to uint64) map[uint64]string {
	t.Helper()
	out := make(map[uint64]string, to-from+1)
	for n := from; n <= to; n++ {
		hdr, err := client.HeaderByNumber(ctx, new(big.Int).SetUint64(n))
		if err != nil {
			t.Fatalf("anvil header %d: %v", n, err)
		}
		out[n] = hashHex(hdr.Hash())
	}
	return out
}

// confirm13RunLoop starts the real ConfirmationScanner.ServeLoop and returns
// a stop function that cancels it, joins it and fails on a non-nil return
// (mirrors depositRunLoop; cancellation is the only clean exit here).
func confirm13RunLoop(t *testing.T, parent context.Context, sc *ConfirmationScanner, lease *Lease) func() {
	t.Helper()
	loopCtx, cancel := context.WithCancel(parent)
	done := make(chan error, 1)
	go func() { done <- sc.ServeLoop(loopCtx, lease, nil) }()
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case err := <-done:
			// A cancel racing an in-flight tick read surfaces as a
			// context-canceled query error; serve.go treats
			// context.Canceled/DeadlineExceeded as the clean shutdown
			// polarity, and this helper follows it. (Batch A note:
			// confirmscan.go returns the raw read error instead of nil
			// when cancel lands mid-query; reported, not worked around
			// beyond this polarity match.)
			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("ServeLoop() = %v, want nil on cancellation", err)
			}
		case <-time.After(10 * time.Second):
			t.Errorf("ServeLoop did not stop after cancellation")
		}
	}
	t.Cleanup(stop)
	return stop
}

// confirm13Counter reads one confirmation counter label set from a real
// registry; an absent series reads as 0.
func confirm13Counter(t *testing.T, m *metrics.Metrics, name string, want map[string]string) float64 {
	t.Helper()
	families, err := m.Gatherer().Gather()
	if err != nil {
		t.Fatalf("integrity check: %v", err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			labels := make(map[string]string, len(metric.GetLabel()))
			for _, pair := range metric.GetLabel() {
				labels[pair.GetName()] = pair.GetValue()
			}
			match := true
			for k, v := range want {
				if labels[k] != v {
					match = false
					break
				}
			}
			if match {
				return metric.GetCounter().GetValue()
			}
		}
	}
	return 0
}

// confirm13ChainLabel is the chain label value the registry exposes.
func confirm13ChainLabel(chainID int64) map[string]string {
	return map[string]string{"chain": strconv.FormatInt(chainID, 10)}
}

// confirm13ConfirmedAt reads the durable confirmed_at instant as text.
func confirm13ConfirmedAt(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, bh, txHash string) string {
	t.Helper()
	var at string
	if err := pool.QueryRow(ctx, `
SELECT confirmed_at::text FROM deposit_observations
WHERE chain_id = $1 AND block_hash = $2 AND tx_hash = $3 AND log_index = 0`,
		chainID, bh, txHash).Scan(&at); err != nil {
		t.Fatalf("read confirmed_at: %v", err)
	}
	return at
}

// confirm13Scanner builds the real wired stack: committer + scanner + lease.
func confirm13Scanner(t *testing.T, pool *pgxpool.Pool, chainID int64, n uint64, m *metrics.Metrics) (*ConfirmationScanner, *Lease) {
	t.Helper()
	cfg := ConfirmationConfig{
		ChainID:      chainID,
		ThresholdN:   n,
		PollInterval: 25 * time.Millisecond,
		RetryInitial: 25 * time.Millisecond,
		RetryMax:     250 * time.Millisecond,
	}
	committer, err := NewConfirmationCommitter(pool, cfg)
	if err != nil {
		t.Fatalf("NewConfirmationCommitter(): %v", err)
	}
	sc, err := NewConfirmationScanner(pool, cfg, committer, m)
	if err != nil {
		t.Fatalf("NewConfirmationScanner(): %v", err)
	}
	return sc, depositITLease(t, pool, chainID)
}

// --- T013 main path ---------------------------------------------------------

// TestConfirmationThresholdCrossingExactlyOnce is T013 / quickstart D1 main
// path (FR-01/05, SC-01): below-threshold patience, then threshold-crossing
// exactly-once conversion with exact basis columns and counters.
func TestConfirmationThresholdCrossingExactlyOnce(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	node := logscanStartAnvilNode(t)
	client, err := eth.Dial(ctx, node.url, 5*time.Second)
	if err != nil {
		t.Fatalf("dial anvil client: %v", err)
	}
	defer client.Close()

	const chainID = scanChainID // 31337, matching the Anvil chain id
	const n = uint64(10)

	// Phase 0: mine Anvil to head=18 and reflect the real headers into
	// chain_blocks. Candidates at h=10,11,12 sit at 9/8/7 confirmations.
	node.mine(t, 18)
	head := node.blockNumber(t)
	if head < 18 {
		t.Fatalf("anvil head = %d, want >= 18", head)
	}
	hashes := confirm13AnvilHashes(t, ctx, client, 0, head)
	confirm13SeedCanonicalReal(t, ctx, pool, chainID, hashes, 0, head)

	// 004 carryover shape: history at seq 2, one observation still on the
	// old version_seq=1, two on the current version_seq=2.
	const histA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const histB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	depositSeedHistory(t, ctx, pool, chainID, 1, 10, histA)
	depositSeedHistoryVersion(t, ctx, pool, chainID, 2, 1, 10, histB, "t013-req")
	type cand struct {
		h          uint64
		bh, txHash string
		version    int64
	}
	seeds := []cand{
		{10, "", depositTxHash(10, 0), 2},
		{11, "", depositTxHash(11, 0), 2},
		{12, "", depositTxHash(12, 0), 1}, // old version_seq carryover
	}
	for i, s := range seeds {
		seeds[i].bh = hashes[s.h]
		depositSeedObservation(t, ctx, pool, chainID, s.h, hashes[s.h], s.txHash, 0, "1", s.version)
	}

	m := metrics.New(func() bool { return true })
	sc, lease := confirm13Scanner(t, pool, chainID, n, m)

	// Phase 1: below threshold -> zero commits, zero policy rows (no
	// bootstrap before the first eligible conversion).
	stop := confirm13RunLoop(t, ctx, sc, lease)
	time.Sleep(600 * time.Millisecond) // several 25ms poll ticks
	for _, s := range seeds {
		confirmAssertZeroWrite(t, ctx, pool, chainID, s.bh, s.txHash, 0)
	}
	if got := confirm13Counter(t, m, metrics.ConfirmationConfirmedMetricName, confirm13ChainLabel(chainID)); got != 0 {
		t.Fatalf("%s = %v before threshold, want 0", metrics.ConfirmationConfirmedMetricName, got)
	}
	stop()

	// Phase 2: advance the Anvil tip past the threshold (head=25 ->
	// confirmations 16/15/14) and extend chain_blocks with the new real
	// headers. The DB tip must equal the Anvil head exactly.
	node.mine(t, 25-head)
	tip := node.blockNumber(t)
	if tip != 25 {
		t.Fatalf("anvil head = %d, want exactly 25", tip)
	}
	for h := head + 1; h <= tip; h++ {
		hashes[h] = confirm13AnvilHashes(t, ctx, client, h, h)[h]
	}
	confirm13SeedCanonicalReal(t, ctx, pool, chainID, hashes, head+1, tip)
	tipHash := hashes[tip]
	var dbTipHash string
	if err := pool.QueryRow(ctx, `
SELECT hash FROM chain_blocks WHERE chain_id = $1 AND canonical ORDER BY number DESC LIMIT 1`,
		chainID).Scan(&dbTipHash); err != nil {
		t.Fatalf("read db tip: %v", err)
	}
	if dbTipHash != tipHash {
		t.Fatalf("db tip = %s, want anvil head %s", dbTipHash, tipHash)
	}

	// The same lease handle drives the second loop run, exactly as one
	// process does after wiring (T007 precedent).
	sc2cfg := ConfirmationConfig{
		ChainID:      chainID,
		ThresholdN:   n,
		PollInterval: 25 * time.Millisecond,
		RetryInitial: 25 * time.Millisecond,
		RetryMax:     250 * time.Millisecond,
	}
	committer2, err := NewConfirmationCommitter(pool, sc2cfg)
	if err != nil {
		t.Fatalf("NewConfirmationCommitter(): %v", err)
	}
	sc2, err := NewConfirmationScanner(pool, sc2cfg, committer2, m)
	if err != nil {
		t.Fatalf("NewConfirmationScanner(): %v", err)
	}
	stop2 := confirm13RunLoop(t, ctx, sc2, lease)
	waitUntil(t, time.Now().Add(30*time.Second), "all three candidates confirmed", func() bool {
		var k int
		if err := pool.QueryRow(ctx, `
SELECT count(*) FROM deposit_observations WHERE chain_id = $1 AND status = 'confirmed'`,
			chainID).Scan(&k); err != nil {
			return false
		}
		return k == len(seeds)
	})
	time.Sleep(200 * time.Millisecond) // surface any erroneous extra write
	stop2()

	// Six basis columns exact per row (decimal-string compare for NUMERIC).
	for _, s := range seeds {
		status, nullAt, tipN, thr, seq, gotTipHash, conf :=
			confirmReadBasis(t, ctx, pool, chainID, s.bh, s.txHash)
		wantConf := strconv.FormatUint(tip-s.h+1, 10)
		if status != "confirmed" || nullAt {
			t.Fatalf("h=%d: status=%s nullAt=%v, want confirmed/non-null confirmed_at", s.h, status, nullAt)
		}
		if tipN != int64(tip) || gotTipHash != tipHash || thr != int64(n) || seq != 1 || conf != wantConf {
			t.Fatalf("h=%d: basis = tip(%d %s) N=%d conf=%s seq=%d, want tip(%d %s) N=%d conf=%s seq=1",
				s.h, tipN, gotTipHash, thr, conf, seq, tip, tipHash, n, wantConf)
		}
	}
	// Old version_seq row confirms normally AND stays attributed to its
	// 004 history version (004 semantics untouched, confirmation only
	// gates on canonical + threshold).
	var attributed int
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM deposit_observations o
JOIN deposit_config_history h ON h.chain_id = o.chain_id AND h.version_seq = o.version_seq
WHERE o.chain_id = $1 AND o.status = 'confirmed'`, chainID).Scan(&attributed); err != nil {
		t.Fatalf("version attribution join: %v", err)
	}
	if attributed != len(seeds) {
		t.Fatalf("observations attributed to a history version = %d, want %d", attributed, len(seeds))
	}
	// Bootstrap: exactly one policy row, operator bootstrap, request NULL.
	if k := depositCountRows(t, ctx, pool, "confirmation_policy_history", chainID); k != 1 {
		t.Fatalf("confirmation_policy_history rows = %d, want 1 (bootstrap on first conversion)", k)
	}
	var seq, thr int64
	var op string
	var reqID *string
	if err := pool.QueryRow(ctx, `
SELECT policy_seq, threshold, operator, request_id FROM confirmation_policy_history WHERE chain_id = $1`,
		chainID).Scan(&seq, &thr, &op, &reqID); err != nil {
		t.Fatalf("read bootstrap policy row: %v", err)
	}
	if seq != 1 || thr != int64(n) || op != "bootstrap" || reqID != nil {
		t.Fatalf("bootstrap row = (%d %d %q %v), want (1 10 bootstrap NULL)", seq, thr, op, reqID)
	}
	// Counters: +1 per row on both the confirmed counter and the ok
	// adjudication (contracts/observability.md).
	chain := confirm13ChainLabel(chainID)
	if got := confirm13Counter(t, m, metrics.ConfirmationConfirmedMetricName, chain); got != float64(len(seeds)) {
		t.Fatalf("%s = %v, want %d", metrics.ConfirmationConfirmedMetricName, got, len(seeds))
	}
	okLabels := map[string]string{"chain": strconv.FormatInt(chainID, 10), "result": "ok"}
	if got := confirm13Counter(t, m, metrics.ConfirmationTransitionMetricName, okLabels); got != float64(len(seeds)) {
		t.Fatalf("%s{ok} = %v, want %d", metrics.ConfirmationTransitionMetricName, got, len(seeds))
	}
	// Contracts integrity check: zero confirmed rows may lack basis columns.
	var incomplete int
	if err := pool.QueryRow(ctx, `
SELECT COUNT(*) FROM deposit_observations
WHERE chain_id = $1 AND status = 'confirmed'
  AND (confirmed_at IS NULL OR confirm_tip_number IS NULL OR confirm_tip_hash IS NULL
       OR confirm_threshold IS NULL OR confirmations IS NULL OR confirm_policy_seq IS NULL)`,
		chainID).Scan(&incomplete); err != nil {
		t.Fatalf("integrity抽查: %v", err)
	}
	if incomplete != 0 {
		t.Fatalf("incomplete confirmed rows = %d, want 0", incomplete)
	}

	// Exactly-once: a second loop run over the converted rows changes
	// nothing (confirmed_at identical, counters frozen).
	before := make(map[string]string, len(seeds))
	for _, s := range seeds {
		before[s.txHash] = confirm13ConfirmedAt(t, ctx, pool, chainID, s.bh, s.txHash)
	}
	sc3cfg := ConfirmationConfig{
		ChainID:      chainID,
		ThresholdN:   n,
		PollInterval: 25 * time.Millisecond,
		RetryInitial: 25 * time.Millisecond,
		RetryMax:     250 * time.Millisecond,
	}
	committer3, err := NewConfirmationCommitter(pool, sc3cfg)
	if err != nil {
		t.Fatalf("NewConfirmationCommitter(): %v", err)
	}
	sc3, err := NewConfirmationScanner(pool, sc3cfg, committer3, m)
	if err != nil {
		t.Fatalf("NewConfirmationScanner(): %v", err)
	}
	stop3 := confirm13RunLoop(t, ctx, sc3, lease)
	time.Sleep(500 * time.Millisecond)
	stop3()
	for _, s := range seeds {
		if got := confirm13ConfirmedAt(t, ctx, pool, chainID, s.bh, s.txHash); got != before[s.txHash] {
			t.Fatalf("h=%d: confirmed_at changed %s -> %s (repeat run rewrote first-seen facts)",
				s.h, before[s.txHash], got)
		}
	}
	if got := confirm13Counter(t, m, metrics.ConfirmationConfirmedMetricName, chain); got != float64(len(seeds)) {
		t.Fatalf("%s after repeat run = %v, want still %d (exactly once)", metrics.ConfirmationConfirmedMetricName, got, len(seeds))
	}
	if got := confirm13Counter(t, m, metrics.ConfirmationTransitionMetricName, okLabels); got != float64(len(seeds)) {
		t.Fatalf("%s{ok} after repeat run = %v, want still %d", metrics.ConfirmationTransitionMetricName, got, len(seeds))
	}
}

// --- T013 pause gate --------------------------------------------------------

// TestConfirmationPauseGateZeroCommits proves version-agnostic != gate
// bypass: with a deposit_pause row present, the real loop halts with zero
// commits (chain-view and policy-drift refusals are covered per-commit in
// confirmcommit_integration_test.go; the pause representative proves the
// scanner enforces gates instead of converting around them).
func TestConfirmationPauseGateZeroCommits(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	const chainID, h, tip, n = int64(31338), uint64(100), uint64(109), uint64(10)
	depositSeedCanonical(t, ctx, pool, chainID, h, tip, true)
	bh, txHash := confirmSeedPending(t, ctx, pool, chainID, h)
	if _, err := pool.Exec(ctx, `
INSERT INTO deposit_pause (chain_id, height, kind, detail) VALUES ($1, 10, 'upstream_gap', 't013')`,
		chainID); err != nil {
		t.Fatalf("seed deposit_pause: %v", err)
	}

	m := metrics.New(func() bool { return true })
	sc, lease := confirm13Scanner(t, pool, chainID, n, m)

	done := make(chan error, 1)
	go func() { done <- sc.ServeLoop(ctx, lease, nil) }()
	select {
	case err := <-done:
		var paused *streamPauseError
		if !errors.As(err, &paused) || paused.stream != "deposit_pause" {
			t.Fatalf("ServeLoop() = %v (%T), want *streamPauseError for deposit_pause", err, err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("ServeLoop did not halt on the present pause row")
	}
	if got := sc.ConfirmationState(); got != 3 {
		t.Fatalf("ConfirmationState() = %d, want 3 (stopped)", got)
	}
	confirmAssertZeroWrite(t, ctx, pool, chainID, bh, txHash, 0)
	if got := confirm13Counter(t, m, metrics.ConfirmationConfirmedMetricName, confirm13ChainLabel(chainID)); got != 0 {
		t.Fatalf("%s under pause = %v, want 0", metrics.ConfirmationConfirmedMetricName, got)
	}
	rejected := map[string]string{"chain": strconv.FormatInt(chainID, 10), "result": "rejected"}
	if got := confirm13Counter(t, m, metrics.ConfirmationTransitionMetricName, rejected); got != 1 {
		t.Fatalf("%s{rejected} under pause = %v, want 1", metrics.ConfirmationTransitionMetricName, got)
	}
}

// --- T013 empty tip ---------------------------------------------------------

// TestConfirmationEmptyTipZeroCommits is the F3 empty-state assertion for
// T013: a Pending row with no canonical tip at all -> the loop waits for a
// trusted tip (state=1) with zero commits and zero policy rows.
func TestConfirmationEmptyTipZeroCommits(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	const chainID, h, n = int64(31339), uint64(50), uint64(10)
	bh, txHash := confirmSeedPending(t, ctx, pool, chainID, h)
	// No chain_blocks rows: the canonical tip is missing entirely.

	m := metrics.New(func() bool { return true })
	sc, lease := confirm13Scanner(t, pool, chainID, n, m)

	stop := confirm13RunLoop(t, ctx, sc, lease)
	time.Sleep(600 * time.Millisecond) // several 25ms poll ticks
	if got := sc.ConfirmationState(); got != 1 {
		t.Fatalf("ConfirmationState() = %d, want 1 (waiting for trusted tip)", got)
	}
	confirmAssertZeroWrite(t, ctx, pool, chainID, bh, txHash, 0)
	if got := confirm13Counter(t, m, metrics.ConfirmationConfirmedMetricName, confirm13ChainLabel(chainID)); got != 0 {
		t.Fatalf("%s with missing tip = %v, want 0", metrics.ConfirmationConfirmedMetricName, got)
	}
	stop()
}

// --- T016 boundary matrix ---------------------------------------------------

// TestConfirmationBoundaryMatrixN10 is T016 / quickstart D1 boundary (FR-01,
// SC-01): with N=10 and the Anvil tip fixed, confirmations 9/10/11 hold,
// convert, convert. Each conversion carries the exact basis columns and
// converts exactly once. US2-4 switch re-judgment is T025 scope: this test
// asserts only pre-switch behavior under the single wired threshold.
func TestConfirmationBoundaryMatrixN10(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	node := logscanStartAnvilNode(t)
	client, err := eth.Dial(ctx, node.url, 5*time.Second)
	if err != nil {
		t.Fatalf("dial anvil client: %v", err)
	}
	defer client.Close()

	const chainID = scanChainID // 31337, matching the Anvil chain id
	const n = uint64(10)

	// Fixed tip=20: h=12 sits at 9 confirmations (N-1, holds), h=11 at 10
	// (N, converts), h=10 at 11 (N+1, converts).
	node.mine(t, 20)
	tip := node.blockNumber(t)
	if tip != 20 {
		t.Fatalf("anvil head = %d, want exactly 20", tip)
	}
	hashes := confirm13AnvilHashes(t, ctx, client, 0, tip)
	confirm13SeedCanonicalReal(t, ctx, pool, chainID, hashes, 0, tip)

	const hist = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	depositSeedHistory(t, ctx, pool, chainID, 1, 10, hist)
	type cand struct {
		h          uint64
		bh, txHash string
	}
	seeds := []cand{
		{12, "", depositTxHash(12, 0)}, // 9 confirmations: holds
		{11, "", depositTxHash(11, 0)}, // 10 confirmations: converts
		{10, "", depositTxHash(10, 0)}, // 11 confirmations: converts
	}
	for i, s := range seeds {
		seeds[i].bh = hashes[s.h]
		depositSeedObservation(t, ctx, pool, chainID, s.h, hashes[s.h], s.txHash, 0, "1", 1)
	}
	tipHash := hashes[tip]

	m := metrics.New(func() bool { return true })
	sc, lease := confirm13Scanner(t, pool, chainID, n, m)

	stop := confirm13RunLoop(t, ctx, sc, lease)
	waitUntil(t, time.Now().Add(30*time.Second), "N/N+1 candidates confirmed", func() bool {
		var k int
		if err := pool.QueryRow(ctx, `
SELECT count(*) FROM deposit_observations WHERE chain_id = $1 AND status = 'confirmed'`,
			chainID).Scan(&k); err != nil {
			return false
		}
		return k >= 2
	})
	time.Sleep(300 * time.Millisecond) // surface any erroneous extra write (N-1 must never convert)
	stop()

	// N-1 holds: still pending with zero conversion facts. The policy table
	// holds exactly the bootstrap row written by the two conversions.
	confirmAssertZeroWrite(t, ctx, pool, chainID, seeds[0].bh, seeds[0].txHash, 1)
	var pending int
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM deposit_observations WHERE chain_id = $1 AND status = 'pending'`,
		chainID).Scan(&pending); err != nil {
		t.Fatalf("count pending: %v", err)
	}
	if pending != 1 {
		t.Fatalf("pending rows = %d, want 1 (the N-1 candidate)", pending)
	}

	// N and N+1 convert with the exact basis columns (decimal-string
	// compare for NUMERIC, mirroring T013).
	for _, s := range seeds[1:] {
		status, nullAt, tipN, thr, seq, gotTipHash, conf :=
			confirmReadBasis(t, ctx, pool, chainID, s.bh, s.txHash)
		wantConf := strconv.FormatUint(tip-s.h+1, 10)
		if status != "confirmed" || nullAt {
			t.Fatalf("h=%d: status=%s nullAt=%v, want confirmed/non-null confirmed_at", s.h, status, nullAt)
		}
		if tipN != int64(tip) || gotTipHash != tipHash || thr != int64(n) || seq != 1 || conf != wantConf {
			t.Fatalf("h=%d: basis = tip(%d %s) N=%d conf=%s seq=%d, want tip(%d %s) N=%d conf=%s seq=1",
				s.h, tipN, gotTipHash, thr, conf, seq, tip, tipHash, n, wantConf)
		}
	}

	// Counters: +1 per conversion on both the confirmed counter and the ok
	// adjudication (contracts/observability.md).
	chain := confirm13ChainLabel(chainID)
	if got := confirm13Counter(t, m, metrics.ConfirmationConfirmedMetricName, chain); got != 2 {
		t.Fatalf("%s = %v, want 2", metrics.ConfirmationConfirmedMetricName, got)
	}
	okLabels := map[string]string{"chain": strconv.FormatInt(chainID, 10), "result": "ok"}
	if got := confirm13Counter(t, m, metrics.ConfirmationTransitionMetricName, okLabels); got != 2 {
		t.Fatalf("%s{ok} = %v, want 2", metrics.ConfirmationTransitionMetricName, got)
	}

	// Exactly-once: a second loop run changes nothing (confirmed_at
	// identical, counters frozen).
	before := make(map[string]string, len(seeds[1:]))
	for _, s := range seeds[1:] {
		before[s.txHash] = confirm13ConfirmedAt(t, ctx, pool, chainID, s.bh, s.txHash)
	}
	sc2cfg := ConfirmationConfig{
		ChainID:      chainID,
		ThresholdN:   n,
		PollInterval: 25 * time.Millisecond,
		RetryInitial: 25 * time.Millisecond,
		RetryMax:     250 * time.Millisecond,
	}
	committer2, err := NewConfirmationCommitter(pool, sc2cfg)
	if err != nil {
		t.Fatalf("NewConfirmationCommitter(): %v", err)
	}
	sc2, err := NewConfirmationScanner(pool, sc2cfg, committer2, m)
	if err != nil {
		t.Fatalf("NewConfirmationScanner(): %v", err)
	}
	stop2 := confirm13RunLoop(t, ctx, sc2, lease)
	time.Sleep(500 * time.Millisecond)
	stop2()
	for _, s := range seeds[1:] {
		if got := confirm13ConfirmedAt(t, ctx, pool, chainID, s.bh, s.txHash); got != before[s.txHash] {
			t.Fatalf("h=%d: confirmed_at changed %s -> %s (repeat run rewrote first-seen facts)",
				s.h, before[s.txHash], got)
		}
	}
	confirmAssertZeroWrite(t, ctx, pool, chainID, seeds[0].bh, seeds[0].txHash, 1)
	if got := confirm13Counter(t, m, metrics.ConfirmationConfirmedMetricName, chain); got != 2 {
		t.Fatalf("%s after repeat run = %v, want still 2 (exactly once)", metrics.ConfirmationConfirmedMetricName, got)
	}
	if got := confirm13Counter(t, m, metrics.ConfirmationTransitionMetricName, okLabels); got != 2 {
		t.Fatalf("%s{ok} after repeat run = %v, want still 2", metrics.ConfirmationTransitionMetricName, got)
	}
}

// TestConfirmationBoundaryN1TipEqualsHeight is T016 / quickstart D1 (FR-01,
// SC-02): with N=1 and the Anvil tip exactly at the candidate height,
// confirmations == 1 and the row converts exactly once with the exact basis
// columns.
func TestConfirmationBoundaryN1TipEqualsHeight(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	node := logscanStartAnvilNode(t)
	client, err := eth.Dial(ctx, node.url, 5*time.Second)
	if err != nil {
		t.Fatalf("dial anvil client: %v", err)
	}
	defer client.Close()

	const chainID = scanChainID // 31337, matching the Anvil chain id
	const n = uint64(1)

	// tip == h == 5: confirmations = 5-5+1 = 1 = N, converts.
	node.mine(t, 5)
	tip := node.blockNumber(t)
	if tip != 5 {
		t.Fatalf("anvil head = %d, want exactly 5", tip)
	}
	const h = uint64(5)
	hashes := confirm13AnvilHashes(t, ctx, client, 0, tip)
	confirm13SeedCanonicalReal(t, ctx, pool, chainID, hashes, 0, tip)

	const hist = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	depositSeedHistory(t, ctx, pool, chainID, 1, h, hist)
	bh := hashes[h]
	txHash := depositTxHash(h, 0)
	depositSeedObservation(t, ctx, pool, chainID, h, bh, txHash, 0, "1", 1)
	tipHash := hashes[tip]

	m := metrics.New(func() bool { return true })
	sc, lease := confirm13Scanner(t, pool, chainID, n, m)

	stop := confirm13RunLoop(t, ctx, sc, lease)
	waitUntil(t, time.Now().Add(30*time.Second), "N=1 candidate confirmed", func() bool {
		var k int
		if err := pool.QueryRow(ctx, `
SELECT count(*) FROM deposit_observations WHERE chain_id = $1 AND status = 'confirmed'`,
			chainID).Scan(&k); err != nil {
			return false
		}
		return k >= 1
	})
	time.Sleep(200 * time.Millisecond) // surface any erroneous extra write
	stop()

	status, nullAt, tipN, thr, seq, gotTipHash, conf :=
		confirmReadBasis(t, ctx, pool, chainID, bh, txHash)
	if status != "confirmed" || nullAt {
		t.Fatalf("status=%s nullAt=%v, want confirmed/non-null confirmed_at", status, nullAt)
	}
	if tipN != int64(tip) || gotTipHash != tipHash || thr != 1 || seq != 1 || conf != "1" {
		t.Fatalf("basis = tip(%d %s) N=%d conf=%s seq=%d, want tip(%d %s) N=1 conf=1 seq=1",
			tipN, gotTipHash, thr, conf, seq, tip, tipHash)
	}

	chain := confirm13ChainLabel(chainID)
	if got := confirm13Counter(t, m, metrics.ConfirmationConfirmedMetricName, chain); got != 1 {
		t.Fatalf("%s = %v, want 1", metrics.ConfirmationConfirmedMetricName, got)
	}
	okLabels := map[string]string{"chain": strconv.FormatInt(chainID, 10), "result": "ok"}
	if got := confirm13Counter(t, m, metrics.ConfirmationTransitionMetricName, okLabels); got != 1 {
		t.Fatalf("%s{ok} = %v, want 1", metrics.ConfirmationTransitionMetricName, got)
	}
}

// --- T019 anomaly halt / catch-up / small-batch coverage --------------------

// T019 [US3] quickstart D5 anomaly matrix (FR-02/06, SC-03/04, US3-1/2/3/4):
// forged-reference and hash-mismatch rows halt the whole loop (state=3,
// zero commits this tick and after, back eligible rows blocked as required
// behavior per data-model §候选分类 §后果); pause halts; missing tip waits
// (state=1); cleared lag converts everything eligible; a benign set larger
// than confirmationCandidateBatchLimit (500) is covered over multiple ticks.
// Every test owns an isolated pg (+ Anvil where chain truth matters) and
// reuses the confirm13* harness verbatim. One old 004 version_seq=1 row
// rides along wherever a second row exists: version-agnostic never bypasses
// a gate. T018's ServeLoop mapping is NOT re-implemented here, only
// exercised end to end; no drift-exit scenarios (T027 lane).

// TestConfirmationAnomalyReferenceMissingHalts is T019(a) / quickstart D5
// (US3-2): a SQL-seeded forged reference row (deposit_observations row at
// h=100 whose block_number/hash has no canonical chain_blocks row — legal,
// no FK; history row attached for the version_seq FK; the scanner path
// pre-006 cannot produce it) sits front-row ahead of an eligible benign row.
// The loop halts with *ConfirmationChainViewError: state=3, both rows still
// pending with zero conversion facts, zero policy rows, no pause row built,
// confirmed_total 0 and transition_total{stale} 1. A second loop run halts
// the same way (subsequent ticks zero commits).
func TestConfirmationAnomalyReferenceMissingHalts(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	const chainID, tip, n = int64(31340), uint64(110), uint64(10)
	// Canonical rows 101..110 only: h=100 has no reference row at all.
	depositSeedCanonical(t, ctx, pool, chainID, 101, tip, true)
	depositSeedHistory(t, ctx, pool, chainID, 1, 10, histA19)
	depositSeedHistoryVersion(t, ctx, pool, chainID, 2, 1, 10, histB19, "t019-req")

	// Front forged row (old version_seq=1 carryover): tip-h+1 = 11 >= 10,
	// so it reaches the commit path and halts there instead of waiting.
	bhF, txF := depositBlockHash(100), depositTxHash(100, 0)
	depositSeedObservation(t, ctx, pool, chainID, 100, bhF, txF, 0, "1", 1)
	// Back benign row, eligible (110-101+1 = 10) on the current version:
	// blocked by the front-row halt as required behavior, not a bug.
	bhB, txB := depositBlockHash(101), depositTxHash(101, 0)
	depositSeedObservation(t, ctx, pool, chainID, 101, bhB, txB, 0, "1", 2)

	m := metrics.New(func() bool { return true })
	sc, lease := confirm13Scanner(t, pool, chainID, n, m)

	done := make(chan error, 1)
	go func() { done <- sc.ServeLoop(ctx, lease, nil) }()
	select {
	case err := <-done:
		var cv *ConfirmationChainViewError
		if !errors.As(err, &cv) {
			t.Fatalf("ServeLoop() = %v (%T), want *ConfirmationChainViewError (forged reference)", err, err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("ServeLoop did not halt on the forged reference row")
	}
	if got := sc.ConfirmationState(); got != 3 {
		t.Fatalf("ConfirmationState() = %d, want 3 (stopped)", got)
	}
	confirmAssertZeroWrite(t, ctx, pool, chainID, bhF, txF, 0)
	confirmAssertZeroWrite(t, ctx, pool, chainID, bhB, txB, 0)
	var pending int
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM deposit_observations WHERE chain_id = $1 AND status = 'pending'`,
		chainID).Scan(&pending); err != nil {
		t.Fatalf("count pending: %v", err)
	}
	if pending != 2 {
		t.Fatalf("pending rows = %d, want 2 (forged front row + blocked back row, zero commits)", pending)
	}
	if k := depositCountRows(t, ctx, pool, "deposit_pause", chainID); k != 0 {
		t.Fatalf("deposit_pause rows = %d, want 0 (halt builds no pause row)", k)
	}
	chain := confirm13ChainLabel(chainID)
	if got := confirm13Counter(t, m, metrics.ConfirmationConfirmedMetricName, chain); got != 0 {
		t.Fatalf("%s = %v, want 0 (halt commits nothing)", metrics.ConfirmationConfirmedMetricName, got)
	}
	stale := map[string]string{"chain": strconv.FormatInt(chainID, 10), "result": "stale"}
	if got := confirm13Counter(t, m, metrics.ConfirmationTransitionMetricName, stale); got != 1 {
		t.Fatalf("%s{stale} = %v, want 1 (reference_unverifiable halt)", metrics.ConfirmationTransitionMetricName, got)
	}

	// Subsequent ticks: a fresh scanner on the same durable state halts
	// again with zero commits (fresh registry proves no write happened).
	m2 := metrics.New(func() bool { return true })
	sc2 := confirm19Scanner(t, pool, chainID, n, m2, lease)
	done2 := make(chan error, 1)
	go func() { done2 <- sc2.ServeLoop(ctx, lease, nil) }()
	select {
	case err := <-done2:
		var cv *ConfirmationChainViewError
		if !errors.As(err, &cv) {
			t.Fatalf("second ServeLoop() = %v (%T), want *ConfirmationChainViewError", err, err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("second ServeLoop did not halt on the forged reference row")
	}
	if got := sc2.ConfirmationState(); got != 3 {
		t.Fatalf("second ConfirmationState() = %d, want 3", got)
	}
	confirmAssertZeroWrite(t, ctx, pool, chainID, bhF, txF, 0)
	confirmAssertZeroWrite(t, ctx, pool, chainID, bhB, txB, 0)
	if got := confirm13Counter(t, m2, metrics.ConfirmationConfirmedMetricName, confirm13ChainLabel(chainID)); got != 0 {
		t.Fatalf("second run %s = %v, want 0", metrics.ConfirmationConfirmedMetricName, got)
	}
}

// TestConfirmationAnomalyHashMismatchHalts is T019(b) / quickstart D5
// (Edge-170): the canonical row exists at h but carries a different hash
// than the observation reference. Same halt as (a): state=3, zero commits
// this tick and on re-run, back eligible row blocked, stale adjudication.
func TestConfirmationAnomalyHashMismatchHalts(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	const chainID, tip, n = int64(31341), uint64(110), uint64(10)
	depositSeedCanonical(t, ctx, pool, chainID, 100, tip, true)
	depositSeedHistory(t, ctx, pool, chainID, 1, 10, histA19)
	depositSeedHistoryVersion(t, ctx, pool, chainID, 2, 1, 10, histB19, "t019-req")

	// Front row references h=100 with a hash that diverges from the
	// canonical row (old version_seq=1 carryover); eligible so it halts.
	bhM, txM := depositBlockHash(999), depositTxHash(100, 7)
	depositSeedObservation(t, ctx, pool, chainID, 100, bhM, txM, 0, "1", 1)
	bhB, txB := depositBlockHash(101), depositTxHash(101, 0)
	depositSeedObservation(t, ctx, pool, chainID, 101, bhB, txB, 0, "1", 2)

	m := metrics.New(func() bool { return true })
	sc, lease := confirm13Scanner(t, pool, chainID, n, m)

	done := make(chan error, 1)
	go func() { done <- sc.ServeLoop(ctx, lease, nil) }()
	select {
	case err := <-done:
		var cv *ConfirmationChainViewError
		if !errors.As(err, &cv) {
			t.Fatalf("ServeLoop() = %v (%T), want *ConfirmationChainViewError (hash mismatch)", err, err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("ServeLoop did not halt on the hash-mismatched reference")
	}
	if got := sc.ConfirmationState(); got != 3 {
		t.Fatalf("ConfirmationState() = %d, want 3 (stopped)", got)
	}
	confirmAssertZeroWrite(t, ctx, pool, chainID, bhM, txM, 0)
	confirmAssertZeroWrite(t, ctx, pool, chainID, bhB, txB, 0)
	chain := confirm13ChainLabel(chainID)
	if got := confirm13Counter(t, m, metrics.ConfirmationConfirmedMetricName, chain); got != 0 {
		t.Fatalf("%s = %v, want 0 (halt commits nothing)", metrics.ConfirmationConfirmedMetricName, got)
	}
	stale := map[string]string{"chain": strconv.FormatInt(chainID, 10), "result": "stale"}
	if got := confirm13Counter(t, m, metrics.ConfirmationTransitionMetricName, stale); got != 1 {
		t.Fatalf("%s{stale} = %v, want 1 (reference_unverifiable halt)", metrics.ConfirmationTransitionMetricName, got)
	}

	m2 := metrics.New(func() bool { return true })
	sc2 := confirm19Scanner(t, pool, chainID, n, m2, lease)
	done2 := make(chan error, 1)
	go func() { done2 <- sc2.ServeLoop(ctx, lease, nil) }()
	select {
	case err := <-done2:
		var cv *ConfirmationChainViewError
		if !errors.As(err, &cv) {
			t.Fatalf("second ServeLoop() = %v (%T), want *ConfirmationChainViewError", err, err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("second ServeLoop did not halt on the hash-mismatched reference")
	}
	confirmAssertZeroWrite(t, ctx, pool, chainID, bhM, txM, 0)
	confirmAssertZeroWrite(t, ctx, pool, chainID, bhB, txB, 0)
	if got := confirm13Counter(t, m2, metrics.ConfirmationConfirmedMetricName, confirm13ChainLabel(chainID)); got != 0 {
		t.Fatalf("second run %s = %v, want 0", metrics.ConfirmationConfirmedMetricName, got)
	}
}

// TestConfirmationPausePresentZeroCommitsT019 is T019(c) / quickstart D5
// (US3-1): a deposit_pause row is present while an eligible old-version
// carryover row waits. The real loop halts (state=3) with zero commits,
// zero policy rows, confirmed_total 0 and transition_total{rejected} 1:
// gates hold regardless of the observation's 004 version.
func TestConfirmationPausePresentZeroCommitsT019(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	const chainID, h, tip, n = int64(31342), uint64(100), uint64(110), uint64(10)
	depositSeedCanonical(t, ctx, pool, chainID, h, tip, true)
	depositSeedHistory(t, ctx, pool, chainID, 1, 10, histA19)
	depositSeedHistoryVersion(t, ctx, pool, chainID, 2, 1, 10, histB19, "t019-req")
	bh, txHash := depositBlockHash(h), depositTxHash(h, 0)
	depositSeedObservation(t, ctx, pool, chainID, h, bh, txHash, 0, "1", 1) // old version_seq carryover
	if _, err := pool.Exec(ctx, `
INSERT INTO deposit_pause (chain_id, height, kind, detail) VALUES ($1, 10, 'upstream_gap', 't019')`,
		chainID); err != nil {
		t.Fatalf("seed deposit_pause: %v", err)
	}

	m := metrics.New(func() bool { return true })
	sc, lease := confirm13Scanner(t, pool, chainID, n, m)

	done := make(chan error, 1)
	go func() { done <- sc.ServeLoop(ctx, lease, nil) }()
	select {
	case err := <-done:
		var paused *streamPauseError
		if !errors.As(err, &paused) || paused.stream != "deposit_pause" {
			t.Fatalf("ServeLoop() = %v (%T), want *streamPauseError for deposit_pause", err, err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("ServeLoop did not halt on the present pause row")
	}
	if got := sc.ConfirmationState(); got != 3 {
		t.Fatalf("ConfirmationState() = %d, want 3 (stopped)", got)
	}
	confirmAssertZeroWrite(t, ctx, pool, chainID, bh, txHash, 0)
	if got := confirm13Counter(t, m, metrics.ConfirmationConfirmedMetricName, confirm13ChainLabel(chainID)); got != 0 {
		t.Fatalf("%s under pause = %v, want 0", metrics.ConfirmationConfirmedMetricName, got)
	}
	rejected := map[string]string{"chain": strconv.FormatInt(chainID, 10), "result": "rejected"}
	if got := confirm13Counter(t, m, metrics.ConfirmationTransitionMetricName, rejected); got != 1 {
		t.Fatalf("%s{rejected} under pause = %v, want 1", metrics.ConfirmationTransitionMetricName, got)
	}
}

// TestConfirmationTipMissingWaitsZeroCommitsT019 is T019(d) / quickstart D5
// (FR-02): a Pending old-version carryover row with no chain_blocks at all
// -> the loop waits for a trusted tip (state=1) with zero commits and zero
// policy rows. Same wait semantics as the T013 empty-tip case, here with
// the version_seq carryover shape.
func TestConfirmationTipMissingWaitsZeroCommitsT019(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	const chainID, h, n = int64(31343), uint64(50), uint64(10)
	depositSeedHistory(t, ctx, pool, chainID, 1, 10, histA19)
	depositSeedHistoryVersion(t, ctx, pool, chainID, 2, 1, 10, histB19, "t019-req")
	bh, txHash := depositBlockHash(h), depositTxHash(h, 0)
	depositSeedObservation(t, ctx, pool, chainID, h, bh, txHash, 0, "1", 1) // old version_seq carryover
	// No chain_blocks rows: the canonical tip is missing entirely.

	m := metrics.New(func() bool { return true })
	sc, lease := confirm13Scanner(t, pool, chainID, n, m)

	stop := confirm13RunLoop(t, ctx, sc, lease)
	time.Sleep(600 * time.Millisecond) // several 25ms poll ticks
	if got := sc.ConfirmationState(); got != 1 {
		t.Fatalf("ConfirmationState() = %d, want 1 (waiting for trusted tip)", got)
	}
	confirmAssertZeroWrite(t, ctx, pool, chainID, bh, txHash, 0)
	if got := confirm13Counter(t, m, metrics.ConfirmationConfirmedMetricName, confirm13ChainLabel(chainID)); got != 0 {
		t.Fatalf("%s with missing tip = %v, want 0", metrics.ConfirmationConfirmedMetricName, got)
	}
	stop()
}

// TestConfirmationLagClearedCatchupAllEligible is T019(e) / quickstart D5
// (SC-04, US3-4): Anvil is the real chain truth. Below-depth lag waits with
// zero commits and no omission (all three rows still pending); once the tip
// clears the lag, every eligible Pending converts exactly once with the
// exact basis columns (catch-up, no omission). One row rides the old 004
// version_seq=1.
func TestConfirmationLagClearedCatchupAllEligible(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	node := logscanStartAnvilNode(t)
	client, err := eth.Dial(ctx, node.url, 5*time.Second)
	if err != nil {
		t.Fatalf("dial anvil client: %v", err)
	}
	defer client.Close()

	const chainID = scanChainID // 31337, matching the Anvil chain id
	const n = uint64(10)

	// Lagged tip=18: candidates at h=10,11,12 sit at 9/8/7 confirmations.
	node.mine(t, 18)
	head := node.blockNumber(t)
	if head < 18 {
		t.Fatalf("anvil head = %d, want >= 18", head)
	}
	hashes := confirm13AnvilHashes(t, ctx, client, 0, head)
	confirm13SeedCanonicalReal(t, ctx, pool, chainID, hashes, 0, head)

	depositSeedHistory(t, ctx, pool, chainID, 1, 10, histA19)
	depositSeedHistoryVersion(t, ctx, pool, chainID, 2, 1, 10, histB19, "t019-req")
	type cand struct {
		h          uint64
		bh, txHash string
		version    int64
	}
	seeds := []cand{
		{10, "", depositTxHash(10, 0), 2},
		{11, "", depositTxHash(11, 0), 2},
		{12, "", depositTxHash(12, 0), 1}, // old version_seq carryover
	}
	for i, s := range seeds {
		seeds[i].bh = hashes[s.h]
		depositSeedObservation(t, ctx, pool, chainID, s.h, hashes[s.h], s.txHash, 0, "1", s.version)
	}

	m := metrics.New(func() bool { return true })
	sc, lease := confirm13Scanner(t, pool, chainID, n, m)

	// Lagged phase: zero commits, zero policy rows, nothing omitted (all
	// three rows still pending).
	stop := confirm13RunLoop(t, ctx, sc, lease)
	time.Sleep(600 * time.Millisecond) // several 25ms poll ticks
	for _, s := range seeds {
		confirmAssertZeroWrite(t, ctx, pool, chainID, s.bh, s.txHash, 0)
	}
	var pending int
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM deposit_observations WHERE chain_id = $1 AND status = 'pending'`,
		chainID).Scan(&pending); err != nil {
		t.Fatalf("count pending during lag: %v", err)
	}
	if pending != len(seeds) {
		t.Fatalf("pending rows during lag = %d, want %d (no omission, no partial commit)", pending, len(seeds))
	}
	stop()

	// Clear the lag: advance the Anvil tip past the threshold (head=25)
	// and extend chain_blocks with the new real headers.
	node.mine(t, 25-head)
	tip := node.blockNumber(t)
	if tip != 25 {
		t.Fatalf("anvil head = %d, want exactly 25", tip)
	}
	for h := head + 1; h <= tip; h++ {
		hashes[h] = confirm13AnvilHashes(t, ctx, client, h, h)[h]
	}
	confirm13SeedCanonicalReal(t, ctx, pool, chainID, hashes, head+1, tip)
	tipHash := hashes[tip]

	sc2 := confirm19Scanner(t, pool, chainID, n, m, lease)
	stop2 := confirm13RunLoop(t, ctx, sc2, lease)
	waitUntil(t, time.Now().Add(60*time.Second), "all lagged candidates confirmed after catch-up", func() bool {
		var k int
		if err := pool.QueryRow(ctx, `
SELECT count(*) FROM deposit_observations WHERE chain_id = $1 AND status = 'confirmed'`,
			chainID).Scan(&k); err != nil {
			return false
		}
		return k == len(seeds)
	})
	time.Sleep(200 * time.Millisecond) // surface any erroneous extra write
	stop2()

	// Exact basis per row (decimal-string compare for NUMERIC).
	for _, s := range seeds {
		status, nullAt, tipN, thr, seq, gotTipHash, conf :=
			confirmReadBasis(t, ctx, pool, chainID, s.bh, s.txHash)
		wantConf := strconv.FormatUint(tip-s.h+1, 10)
		if status != "confirmed" || nullAt {
			t.Fatalf("h=%d: status=%s nullAt=%v, want confirmed/non-null confirmed_at", s.h, status, nullAt)
		}
		if tipN != int64(tip) || gotTipHash != tipHash || thr != int64(n) || seq != 1 || conf != wantConf {
			t.Fatalf("h=%d: basis = tip(%d %s) N=%d conf=%s seq=%d, want tip(%d %s) N=%d conf=%s seq=1",
				s.h, tipN, gotTipHash, thr, conf, seq, tip, tipHash, n, wantConf)
		}
	}
	chain := confirm13ChainLabel(chainID)
	if got := confirm13Counter(t, m, metrics.ConfirmationConfirmedMetricName, chain); got != float64(len(seeds)) {
		t.Fatalf("%s = %v, want %d (catch-up converts everything eligible)", metrics.ConfirmationConfirmedMetricName, got, len(seeds))
	}
	okLabels := map[string]string{"chain": strconv.FormatInt(chainID, 10), "result": "ok"}
	if got := confirm13Counter(t, m, metrics.ConfirmationTransitionMetricName, okLabels); got != float64(len(seeds)) {
		t.Fatalf("%s{ok} = %v, want %d", metrics.ConfirmationTransitionMetricName, got, len(seeds))
	}
	var incomplete int
	if err := pool.QueryRow(ctx, `
SELECT COUNT(*) FROM deposit_observations
WHERE chain_id = $1 AND status = 'confirmed'
  AND (confirmed_at IS NULL OR confirm_tip_number IS NULL OR confirm_tip_hash IS NULL
       OR confirm_threshold IS NULL OR confirmations IS NULL OR confirm_policy_seq IS NULL)`,
		chainID).Scan(&incomplete); err != nil {
		t.Fatalf("integrity check: %v", err)
	}
	if incomplete != 0 {
		t.Fatalf("incomplete confirmed rows = %d, want 0", incomplete)
	}
}

// TestConfirmationBenignSmallBatchMultiTickCoverage is T019(f) / quickstart
// D5 (SC-04 benign no-starvation): all rows benign and eligible, but the
// eligible set (506 rows across h=100..110) is larger than the per-tick
// candidate LIMIT (500). Multiple ticks cover every row (monotonicity):
// pending reaches 0, confirmed_total and transition_total{ok} equal the
// full set, exactly one bootstrap policy row exists, and the integrity
// check returns 0. Anomalous blocking is excluded here by construction —
// the spec keeps halt for anomalies (cases a/b above).
func TestConfirmationBenignSmallBatchMultiTickCoverage(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	const chainID, tip, n = int64(31344), uint64(119), uint64(10)
	depositSeedCanonical(t, ctx, pool, chainID, 100, tip, true)
	depositSeedHistory(t, ctx, pool, chainID, 1, 10, histA19)

	// maxEligible = 119-10+1 = 110: heights 100..110 (11 heights x 46 rows
	// = 506 rows) are all eligible, exceeding the 500-row tick LIMIT.
	const perHeight = 46
	var total int
	for h := uint64(100); h <= 110; h++ {
		for i := uint64(0); i < perHeight; i++ {
			depositSeedObservation(t, ctx, pool, chainID, h, depositBlockHash(h), depositTxHash(h, i), i, "1", 1)
			total++
		}
	}
	if total != 506 {
		t.Fatalf("seeded rows = %d, want 506 (must exceed the 500-row tick LIMIT)", total)
	}

	m := metrics.New(func() bool { return true })
	sc, lease := confirm13Scanner(t, pool, chainID, n, m)

	stop := confirm13RunLoop(t, ctx, sc, lease)
	waitUntil(t, time.Now().Add(120*time.Second), "all 506 benign rows confirmed over multiple ticks", func() bool {
		var k int
		if err := pool.QueryRow(ctx, `
SELECT count(*) FROM deposit_observations WHERE chain_id = $1 AND status = 'confirmed'`,
			chainID).Scan(&k); err != nil {
			return false
		}
		return k == total
	})
	time.Sleep(300 * time.Millisecond) // surface any erroneous extra write
	stop()

	var pending int
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM deposit_observations WHERE chain_id = $1 AND status = 'pending'`,
		chainID).Scan(&pending); err != nil {
		t.Fatalf("count pending: %v", err)
	}
	if pending != 0 {
		t.Fatalf("pending rows = %d, want 0 (multi-tick full coverage, no starvation)", pending)
	}
	if k := depositCountRows(t, ctx, pool, "confirmation_policy_history", chainID); k != 1 {
		t.Fatalf("confirmation_policy_history rows = %d, want 1 (bootstrap on first conversion)", k)
	}
	chain := confirm13ChainLabel(chainID)
	if got := confirm13Counter(t, m, metrics.ConfirmationConfirmedMetricName, chain); got != float64(total) {
		t.Fatalf("%s = %v, want %d", metrics.ConfirmationConfirmedMetricName, got, total)
	}
	okLabels := map[string]string{"chain": strconv.FormatInt(chainID, 10), "result": "ok"}
	if got := confirm13Counter(t, m, metrics.ConfirmationTransitionMetricName, okLabels); got != float64(total) {
		t.Fatalf("%s{ok} = %v, want %d", metrics.ConfirmationTransitionMetricName, got, total)
	}
	var incomplete int
	if err := pool.QueryRow(ctx, `
SELECT COUNT(*) FROM deposit_observations
WHERE chain_id = $1 AND status = 'confirmed'
  AND (confirmed_at IS NULL OR confirm_tip_number IS NULL OR confirm_tip_hash IS NULL
       OR confirm_threshold IS NULL OR confirmations IS NULL OR confirm_policy_seq IS NULL)`,
		chainID).Scan(&incomplete); err != nil {
		t.Fatalf("integrity check: %v", err)
	}
	if incomplete != 0 {
		t.Fatalf("incomplete confirmed rows = %d, want 0", incomplete)
	}
}

// --- T019 helpers -----------------------------------------------------------

// histA19/histB19 are distinct config hashes for the T019 004-carryover
// shape (history at seq 2, observations on seq 1 or 2).
const (
	histA19 = "e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1"
	histB19 = "e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2"
)

// confirm19Scanner builds a follow-up scanner reusing an already-acquired
// lease handle (same-process second run, T013 sc2/sc3 precedent): the
// confirm13Scanner helper always acquires, which a second run must not redo.
func confirm19Scanner(t *testing.T, pool *pgxpool.Pool, chainID int64, n uint64, m *metrics.Metrics, lease *Lease) *ConfirmationScanner {
	t.Helper()
	cfg := ConfirmationConfig{
		ChainID:      chainID,
		ThresholdN:   n,
		PollInterval: 25 * time.Millisecond,
		RetryInitial: 25 * time.Millisecond,
		RetryMax:     250 * time.Millisecond,
	}
	committer, err := NewConfirmationCommitter(pool, cfg)
	if err != nil {
		t.Fatalf("NewConfirmationCommitter(): %v", err)
	}
	sc, err := NewConfirmationScanner(pool, cfg, committer, m)
	if err != nil {
		t.Fatalf("NewConfirmationScanner(): %v", err)
	}
	_ = lease
	return sc
}

// -- T021 --------------------------------------------------------------------

// T021 [US4] quickstart D2 idempotence + concurrency (FR-05/07, SC-05/06,
// US4-1/2): (a) two REAL database connections (two pgxpool handles to the
// same testcontainers DB, one committer each) race ConfirmDepositUnit on the
// same Pending row — exactly one durable conversion, the loser converges;
// (b) repeat checks never rewrite first-seen facts. T022/T023 append below
// under their own banners; helpers stay confirm13*-prefixed.

// TestConfirmationDualWorkerRaceConverges is T021(a) / quickstart D2
// (data-model §并发时序情形 2, US4-1): two committers on two separate pools
// confirm the same Pending row concurrently. The coordination lock
// (indexer_lease FOR UPDATE) serializes the two transactions: the winner's
// conditional UPDATE converts the row, the loser re-reads status='confirmed'
// under the lock (step 3) and converges via convergeCommitted with zero
// writes — confirmed_at and all five basis columns stay the winner's (I2).
// The channel rendezvous (both goroutines signal ready, main closes the
// gate) is the only sync point — no fixed sleeps; the loser's lock wait is
// the in-DB serialization. Either interleaving (true overlap or
// back-to-back) exercises the same converge path, so the end-state
// assertions hold regardless of which connection wins.
func TestConfirmationDualWorkerRaceConverges(t *testing.T) {
	dsn := startIndexerPostgres(t)
	poolA := openIndexerPool(t, dsn)
	defer poolA.Close()
	poolB := openIndexerPool(t, dsn)
	defer poolB.Close()
	ctx := context.Background()

	const chainID, h, tip, n = int64(31345), uint64(100), uint64(109), uint64(10)
	depositSeedCanonical(t, ctx, poolA, chainID, h, tip, true)
	bh, txHash := confirmSeedPending(t, ctx, poolA, chainID, h)
	confirmSeedPolicyRow(t, ctx, poolA, chainID, 1, int64(n), nil, "bootstrap", nil)

	committerA, err := NewConfirmationCommitter(poolA, ConfirmationConfig{ChainID: chainID, ThresholdN: n})
	if err != nil {
		t.Fatalf("NewConfirmationCommitter(A): %v", err)
	}
	committerB, err := NewConfirmationCommitter(poolB, ConfirmationConfig{ChainID: chainID, ThresholdN: n})
	if err != nil {
		t.Fatalf("NewConfirmationCommitter(B): %v", err)
	}
	// One acquired lease shared by both workers (Token() is an atomic
	// load, safe for concurrent use); both present the same owner/token.
	lease := depositITLease(t, poolA, chainID)
	basis := ConfirmBasis{BlockHash: bh, TxHash: txHash, Height: h,
		TipNumber: tip, TipHash: depositBlockHash(tip), PolicySeq: 1, ThresholdN: n}

	ready := make(chan struct{}, 2)
	gate := make(chan struct{})
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i, c := range []*ConfirmationCommitter{committerA, committerB} {
		wg.Add(1)
		go func(i int, c *ConfirmationCommitter) {
			defer wg.Done()
			ready <- struct{}{}
			<-gate
			errs[i] = c.ConfirmDepositUnit(ctx, lease, basis)
		}(i, c)
	}
	for i := 0; i < 2; i++ {
		select {
		case <-ready:
		case <-time.After(10 * time.Second):
			t.Fatal("race workers did not reach the rendezvous")
		}
	}
	close(gate) // release both real connections at once; the DB lock orders them
	wg.Wait()

	// Winner committed, loser converged (case 2 converge returns nil, not
	// an error): both calls succeed with exactly one durable conversion.
	for i, err := range errs {
		if err != nil {
			t.Fatalf("worker %d ConfirmDepositUnit() = %v, want nil (win or converge)", i, err)
		}
	}
	// Cross-pool reads: the conversion is visible from the other handle.
	var confirmed, pending int
	if err := poolB.QueryRow(ctx, `
SELECT count(*) FROM deposit_observations WHERE chain_id = $1 AND status = 'confirmed'`,
		chainID).Scan(&confirmed); err != nil {
		t.Fatalf("count confirmed via poolB: %v", err)
	}
	if err := poolB.QueryRow(ctx, `
SELECT count(*) FROM deposit_observations WHERE chain_id = $1 AND status = 'pending'`,
		chainID).Scan(&pending); err != nil {
		t.Fatalf("count pending via poolB: %v", err)
	}
	if confirmed != 1 || pending != 0 {
		t.Fatalf("confirmed=%d pending=%d via poolB, want exactly 1 confirmed and 0 pending", confirmed, pending)
	}
	if k := depositCountRows(t, ctx, poolB, "confirmation_policy_history", chainID); k != 1 {
		t.Fatalf("confirmation_policy_history rows via poolB = %d, want 1 (no second bootstrap)", k)
	}
	// Winner's values: the single conversion carries the exact basis.
	status, nullAt, tipN, thr, seq, gotTipHash, conf :=
		confirmReadBasis(t, ctx, poolB, chainID, bh, txHash)
	if status != "confirmed" || nullAt {
		t.Fatalf("status=%s nullAt=%v via poolB, want confirmed/non-null confirmed_at", status, nullAt)
	}
	if tipN != int64(tip) || gotTipHash != depositBlockHash(tip) || thr != int64(n) || seq != 1 || conf != "10" {
		t.Fatalf("basis via poolB = tip(%d %s) N=%d conf=%s seq=%d, want tip(%d %s) N=10 conf=10 seq=1",
			tipN, gotTipHash, thr, conf, seq, tip, depositBlockHash(tip))
	}
	winnerAt := confirm13ConfirmedAt(t, ctx, poolB, chainID, bh, txHash)

	// Loser converges on re-read: a repeat commit through the losing
	// handle returns nil with confirmed_at and every basis column
	// byte-identical to the winner's (first-seen facts immutable, I2).
	if err := committerB.ConfirmDepositUnit(ctx, lease, basis); err != nil {
		t.Fatalf("loser re-read ConfirmDepositUnit() = %v, want nil (converge)", err)
	}
	if got := confirm13ConfirmedAt(t, ctx, poolA, chainID, bh, txHash); got != winnerAt {
		t.Fatalf("confirmed_at changed %s -> %s on loser re-read (rewrote winner facts)", winnerAt, got)
	}
	status2, nullAt2, tipN2, thr2, seq2, gotTipHash2, conf2 :=
		confirmReadBasis(t, ctx, poolA, chainID, bh, txHash)
	if status2 != status || nullAt2 != nullAt || tipN2 != tipN || thr2 != thr ||
		seq2 != seq || gotTipHash2 != gotTipHash || conf2 != conf {
		t.Fatalf("basis changed on loser re-read: was (%s %d %s %d %s %d), now (%s %d %s %d %s %d)",
			status, tipN, gotTipHash, thr, conf, seq, status2, tipN2, gotTipHash2, thr2, conf2, seq2)
	}
	// Integrity: zero confirmed rows may lack basis columns.
	var incomplete int
	if err := poolA.QueryRow(ctx, `
SELECT COUNT(*) FROM deposit_observations
WHERE chain_id = $1 AND status = 'confirmed'
  AND (confirmed_at IS NULL OR confirm_tip_number IS NULL OR confirm_tip_hash IS NULL
       OR confirm_threshold IS NULL OR confirmations IS NULL OR confirm_policy_seq IS NULL)`,
		chainID).Scan(&incomplete); err != nil {
		t.Fatalf("integrity check: %v", err)
	}
	if incomplete != 0 {
		t.Fatalf("incomplete confirmed rows = %d, want 0", incomplete)
	}
}

// TestConfirmationRepeatChecksStaySingleConversion is T021(b) / quickstart D2
// (FR-05, SC-05, US4-2): the same Pending row checked >= 2 times after
// conversion still shows exactly 1 conversion with confirmed_at and basis
// immutable (the single-connection form of case 2 convergence; the
// concurrent form is TestConfirmationDualWorkerRaceConverges above).
func TestConfirmationRepeatChecksStaySingleConversion(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	const chainID, h, tip, n = int64(31346), uint64(100), uint64(109), uint64(10)
	depositSeedCanonical(t, ctx, pool, chainID, h, tip, true)
	bh, txHash := confirmSeedPending(t, ctx, pool, chainID, h)
	confirmSeedPolicyRow(t, ctx, pool, chainID, 1, int64(n), nil, "bootstrap", nil)

	c, lease := confirmCommitter(t, pool, chainID, n)
	basis := ConfirmBasis{BlockHash: bh, TxHash: txHash, Height: h,
		TipNumber: tip, TipHash: depositBlockHash(tip), PolicySeq: 1, ThresholdN: n}
	if err := c.ConfirmDepositUnit(ctx, lease, basis); err != nil {
		t.Fatalf("first ConfirmDepositUnit(): %v", err)
	}
	firstAt := confirm13ConfirmedAt(t, ctx, pool, chainID, bh, txHash)
	firstStatus, firstNullAt, firstTipN, firstThr, firstSeq, firstTipHash, firstConf :=
		confirmReadBasis(t, ctx, pool, chainID, bh, txHash)

	// Repeat checks >= 2: every one converges (nil), none rewrites.
	for i := 0; i < 2; i++ {
		if err := c.ConfirmDepositUnit(ctx, lease, basis); err != nil {
			t.Fatalf("repeat ConfirmDepositUnit() #%d = %v, want nil (converge)", i+1, err)
		}
	}
	if got := confirm13ConfirmedAt(t, ctx, pool, chainID, bh, txHash); got != firstAt {
		t.Fatalf("confirmed_at changed %s -> %s across repeat checks (rewrote first-seen time)", firstAt, got)
	}
	status, nullAt, tipN, thr, seq, gotTipHash, conf :=
		confirmReadBasis(t, ctx, pool, chainID, bh, txHash)
	if status != firstStatus || nullAt != firstNullAt || tipN != firstTipN || thr != firstThr ||
		seq != firstSeq || gotTipHash != firstTipHash || conf != firstConf {
		t.Fatalf("basis changed across repeat checks: was (%s %d %s %d %s %d), now (%s %d %s %d %s %d)",
			firstStatus, firstTipN, firstTipHash, firstThr, firstConf, firstSeq,
			status, tipN, gotTipHash, thr, conf, seq)
	}
	if status != "confirmed" || nullAt {
		t.Fatalf("status=%s nullAt=%v, want confirmed/non-null confirmed_at", status, nullAt)
	}
	if tipN != int64(tip) || gotTipHash != depositBlockHash(tip) || thr != int64(n) || seq != 1 || conf != "10" {
		t.Fatalf("basis = tip(%d %s) N=%d conf=%s seq=%d, want tip(%d %s) N=10 conf=10 seq=1",
			tipN, gotTipHash, thr, conf, seq, tip, depositBlockHash(tip))
	}
	var confirmed int
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM deposit_observations WHERE chain_id = $1 AND status = 'confirmed'`,
		chainID).Scan(&confirmed); err != nil {
		t.Fatalf("count confirmed: %v", err)
	}
	if confirmed != 1 {
		t.Fatalf("confirmed rows = %d, want exactly 1 after repeat checks", confirmed)
	}
	if k := depositCountRows(t, ctx, pool, "confirmation_policy_history", chainID); k != 1 {
		t.Fatalf("confirmation_policy_history rows = %d, want 1 (repeat checks write nothing)", k)
	}
	var incomplete int
	if err := pool.QueryRow(ctx, `
SELECT COUNT(*) FROM deposit_observations
WHERE chain_id = $1 AND status = 'confirmed'
  AND (confirmed_at IS NULL OR confirm_tip_number IS NULL OR confirm_tip_hash IS NULL
       OR confirm_threshold IS NULL OR confirmations IS NULL OR confirm_policy_seq IS NULL)`,
		chainID).Scan(&incomplete); err != nil {
		t.Fatalf("integrity check: %v", err)
	}
	if incomplete != 0 {
		t.Fatalf("incomplete confirmed rows = %d, want 0", incomplete)
	}
}

// -- T022 --------------------------------------------------------------------

// T022 [US4] crash boundary + restart recovery (FR-05, SC-08, US4-3,
// quickstart D2): (a) pre-commit kill retries from durable state with zero
// footprint; (b) unknown COMMIT outcomes are定性 by observation-PK re-read
// (committed -> converge nil, else retry) and never assumed not-committed;
// (c) a fresh-handle restart resumes from durable state (pending processed,
// confirmed untouched). Fault shape mirrors 004
// TestDepositCrashRecoveryInjectedFaults: the TCP-level depositFaultConn on a
// dedicated fault pool (the committer's pool only; seeding and durable
// assertions stay on the clean pool), channel-fired sync, no fixed sleeps.
// T023 appends below under its own banner.

// TestConfirmationPreCommitKillRetriesFromDurableState is T022(a): the
// connection dies at the conditional UPDATE — the last statement before
// COMMIT never lands — so the whole attempt rolls back with zero footprint;
// a retry from durable state converts exactly once (no omission, no
// duplication, no partial row; integrity SQL = 0).
func TestConfirmationPreCommitKillRetriesFromDurableState(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	fault := newDepositFault()
	faultPool := depositOpenFaultPool(t, dsn, fault)
	ctx := context.Background()

	const chainID, h, tip, n = int64(31347), uint64(100), uint64(109), uint64(10)
	depositSeedCanonical(t, ctx, pool, chainID, h, tip, true)
	bh, txHash := confirmSeedPending(t, ctx, pool, chainID, h)
	confirmSeedPolicyRow(t, ctx, pool, chainID, 1, int64(n), nil, "bootstrap", nil)

	committer, err := NewConfirmationCommitter(faultPool, ConfirmationConfig{ChainID: chainID, ThresholdN: n})
	if err != nil {
		t.Fatalf("NewConfirmationCommitter(): %v", err)
	}
	lease := depositITLease(t, pool, chainID)
	basis := ConfirmBasis{BlockHash: bh, TxHash: txHash, Height: h,
		TipNumber: tip, TipHash: depositBlockHash(tip), PolicySeq: 1, ThresholdN: n}

	// Kill the connection at the conditional write: it never lands, so the
	// server rolls the attempt back. The call must fail (never silent
	// success on a killed write).
	fault.armSQL("UPDATE deposit_observations", false, false)
	if err := committer.ConfirmDepositUnit(ctx, lease, basis); err == nil {
		t.Fatal("ConfirmDepositUnit() with the write killed = nil, want a connection error")
	}
	fault.waitFired(t, "connection death at the conditional UPDATE")
	fault.disarm()
	if got := fault.fires.Load(); got != 1 {
		t.Fatalf("fault fires = %d, want exactly 1 (no hidden retry slipped a write through)", got)
	}

	// Zero footprint: still pending with no conversion facts, seeded policy
	// row only.
	confirmAssertZeroWrite(t, ctx, pool, chainID, bh, txHash, 1)

	// Retry from durable state converts exactly once with the exact basis.
	if err := committer.ConfirmDepositUnit(ctx, lease, basis); err != nil {
		t.Fatalf("retry ConfirmDepositUnit() = %v, want nil", err)
	}
	status, nullAt, tipN, thr, seq, gotTipHash, conf :=
		confirmReadBasis(t, ctx, pool, chainID, bh, txHash)
	if status != "confirmed" || nullAt {
		t.Fatalf("status=%s nullAt=%v after retry, want confirmed/non-null confirmed_at", status, nullAt)
	}
	if tipN != int64(tip) || gotTipHash != depositBlockHash(tip) || thr != int64(n) || seq != 1 || conf != "10" {
		t.Fatalf("basis after retry = tip(%d %s) N=%d conf=%s seq=%d, want tip(%d %s) N=10 conf=10 seq=1",
			tipN, gotTipHash, thr, conf, seq, tip, depositBlockHash(tip))
	}
	var confirmed int
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM deposit_observations WHERE chain_id = $1 AND status = 'confirmed'`,
		chainID).Scan(&confirmed); err != nil {
		t.Fatalf("count confirmed: %v", err)
	}
	if confirmed != 1 {
		t.Fatalf("confirmed rows = %d, want exactly 1 after kill + retry", confirmed)
	}
	if k := depositCountRows(t, ctx, pool, "confirmation_policy_history", chainID); k != 1 {
		t.Fatalf("confirmation_policy_history rows = %d, want 1 (retry writes no policy row)", k)
	}
	var incomplete int
	if err := pool.QueryRow(ctx, `
SELECT COUNT(*) FROM deposit_observations
WHERE chain_id = $1 AND status = 'confirmed'
  AND (confirmed_at IS NULL OR confirm_tip_number IS NULL OR confirm_tip_hash IS NULL
       OR confirm_threshold IS NULL OR confirmations IS NULL OR confirm_policy_seq IS NULL)`,
		chainID).Scan(&incomplete); err != nil {
		t.Fatalf("integrity check: %v", err)
	}
	if incomplete != 0 {
		t.Fatalf("incomplete confirmed rows = %d, want 0", incomplete)
	}
}

// TestConfirmationUnknownCommitOutcomeResolvesByPKReread is T022(b) /
// quickstart D2 (FR-05, SC-08, US4-3): a COMMIT whose outcome the caller
// never learns is定性 by re-reading the observation PK — committed means
// converge nil (never treated as not-committed, never rewritten), unlanded
// means error-then-retry converts exactly once. Shape mirrors 004
// TestDepositAuthD11UnknownCommitResolvesByDB (direct call through the fault
// pool, fires==1 pins a single attempt).
func TestConfirmationUnknownCommitOutcomeResolvesByPKReread(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	fault := newDepositFault()
	faultPool := depositOpenFaultPool(t, dsn, fault)
	ctx := context.Background()

	const chainID, h1, h2, tip, n = int64(31348), uint64(100), uint64(101), uint64(110), uint64(10)
	depositSeedCanonical(t, ctx, pool, chainID, h1, tip, true)
	bh1, txHash1 := confirmSeedPending(t, ctx, pool, chainID, h1)
	// A second pending needs its own observation row only: confirmSeedPending
	// plants the shared seq-1 history row, which already exists.
	bh2, txHash2 := depositBlockHash(h2), depositTxHash(h2, 0)
	depositSeedObservation(t, ctx, pool, chainID, h2, bh2, txHash2, 0, "1", 1)
	confirmSeedPolicyRow(t, ctx, pool, chainID, 1, int64(n), nil, "bootstrap", nil)

	committer, err := NewConfirmationCommitter(faultPool, ConfirmationConfig{ChainID: chainID, ThresholdN: n})
	if err != nil {
		t.Fatalf("NewConfirmationCommitter(): %v", err)
	}
	lease := depositITLease(t, pool, chainID)
	basis1 := ConfirmBasis{BlockHash: bh1, TxHash: txHash1, Height: h1,
		TipNumber: tip, TipHash: depositBlockHash(tip), PolicySeq: 1, ThresholdN: n}
	basis2 := ConfirmBasis{BlockHash: bh2, TxHash: txHash2, Height: h2,
		TipNumber: tip, TipHash: depositBlockHash(tip), PolicySeq: 1, ThresholdN: n}

	// Case 1: COMMIT lands at the DB but the reply never reaches the caller.
	// The PK re-read定性 it committed -> nil (not an error, not a blind
	// "not-committed" retry).
	fault.armSQL(depositFaultCommit, true, false)
	if err := committer.ConfirmDepositUnit(ctx, lease, basis1); err != nil {
		t.Fatalf("ConfirmDepositUnit() with the COMMIT reply lost = %v, want nil (PK re-read converges)", err)
	}
	fault.waitFired(t, "COMMIT reply loss")
	fault.disarm()
	if got := fault.fires.Load(); got != 1 {
		t.Fatalf("COMMIT drops = %d, want exactly 1 (a second attempt would be a duplicate)", got)
	}
	status, nullAt, tipN, thr, seq, gotTipHash, conf :=
		confirmReadBasis(t, ctx, pool, chainID, bh1, txHash1)
	if status != "confirmed" || nullAt {
		t.Fatalf("status=%s nullAt=%v after reply loss, want confirmed/non-null confirmed_at", status, nullAt)
	}
	if tipN != int64(tip) || gotTipHash != depositBlockHash(tip) || thr != int64(n) || seq != 1 || conf != "11" {
		t.Fatalf("basis after reply loss = tip(%d %s) N=%d conf=%s seq=%d, want tip(%d %s) N=10 conf=11 seq=1",
			tipN, gotTipHash, thr, conf, seq, tip, depositBlockHash(tip))
	}
	firstAt := confirm13ConfirmedAt(t, ctx, pool, chainID, bh1, txHash1)

	// The follow-up check converges with byte-identical facts: the unknown
	// outcome was committed, so treating it as not-committed would have
	// rewritten first-seen facts (I2).
	if err := committer.ConfirmDepositUnit(ctx, lease, basis1); err != nil {
		t.Fatalf("follow-up ConfirmDepositUnit() = %v, want nil (converge)", err)
	}
	if got := confirm13ConfirmedAt(t, ctx, pool, chainID, bh1, txHash1); got != firstAt {
		t.Fatalf("confirmed_at changed %s -> %s on follow-up (unknown outcome was rewritten)", firstAt, got)
	}
	status2, nullAt2, tipN2, thr2, seq2, gotTipHash2, conf2 :=
		confirmReadBasis(t, ctx, pool, chainID, bh1, txHash1)
	if status2 != status || nullAt2 != nullAt || tipN2 != tipN || thr2 != thr ||
		seq2 != seq || gotTipHash2 != gotTipHash || conf2 != conf {
		t.Fatal("basis changed on follow-up after reply loss (unknown outcome was rewritten)")
	}

	// Case 2: COMMIT never lands (connection dies first). The PK re-read
	//定性 it uncommitted -> error (never silent success, never a phantom
	// row); a retry then converts exactly once.
	fault.armSQL(depositFaultCommit, false, false)
	if err := committer.ConfirmDepositUnit(ctx, lease, basis2); err == nil {
		t.Fatal("ConfirmDepositUnit() with COMMIT unlanded = nil, want an unknown-outcome error")
	}
	fault.waitFired(t, "connection death at COMMIT")
	fault.disarm()
	if got := fault.fires.Load(); got != 2 {
		t.Fatalf("fault fires = %d, want 2 (one per unknown-outcome case)", got)
	}
	confirmAssertZeroWrite(t, ctx, pool, chainID, bh2, txHash2, 1)

	if err := committer.ConfirmDepositUnit(ctx, lease, basis2); err != nil {
		t.Fatalf("retry ConfirmDepositUnit() = %v, want nil", err)
	}
	status3, nullAt3, tipN3, thr3, seq3, gotTipHash3, conf3 :=
		confirmReadBasis(t, ctx, pool, chainID, bh2, txHash2)
	if status3 != "confirmed" || nullAt3 {
		t.Fatalf("status=%s nullAt=%v after retry, want confirmed/non-null confirmed_at", status3, nullAt3)
	}
	if tipN3 != int64(tip) || gotTipHash3 != depositBlockHash(tip) || thr3 != int64(n) || seq3 != 1 || conf3 != "10" {
		t.Fatalf("basis after retry = tip(%d %s) N=%d conf=%s seq=%d, want tip(%d %s) N=10 conf=10 seq=1",
			tipN3, gotTipHash3, thr3, conf3, seq3, tip, depositBlockHash(tip))
	}
	var confirmed int
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM deposit_observations WHERE chain_id = $1 AND status = 'confirmed'`,
		chainID).Scan(&confirmed); err != nil {
		t.Fatalf("count confirmed: %v", err)
	}
	if confirmed != 2 {
		t.Fatalf("confirmed rows = %d, want exactly 2 (one per case, no duplicate)", confirmed)
	}
	if k := depositCountRows(t, ctx, pool, "confirmation_policy_history", chainID); k != 1 {
		t.Fatalf("confirmation_policy_history rows = %d, want 1 (unknown outcomes write no policy row)", k)
	}
	var incomplete int
	if err := pool.QueryRow(ctx, `
SELECT COUNT(*) FROM deposit_observations
WHERE chain_id = $1 AND status = 'confirmed'
  AND (confirmed_at IS NULL OR confirm_tip_number IS NULL OR confirm_tip_hash IS NULL
       OR confirm_threshold IS NULL OR confirmations IS NULL OR confirm_policy_seq IS NULL)`,
		chainID).Scan(&incomplete); err != nil {
		t.Fatalf("integrity check: %v", err)
	}
	if incomplete != 0 {
		t.Fatalf("incomplete confirmed rows = %d, want 0", incomplete)
	}
}

// TestConfirmationRestartRecoveryResumesDurableState is T022(c) / quickstart
// D2 (FR-05, SC-08, US4-3): after a kill -9 (every connection of the crashed
// worker dropped, lease left to expire on the DB clock), a restart with fresh
// handles (new pool, new committer, re-acquired lease) against the same DB
// resumes from durable state: the pre-crash conversion is untouched
// (confirmed_at and all basis columns byte-identical) and the still-pending
// row is processed. Restart shape mirrors 004's lease-expiry + fresh-owner
// recovery.
func TestConfirmationRestartRecoveryResumesDurableState(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	const chainID, h1, h2, tip, n = int64(31349), uint64(100), uint64(101), uint64(110), uint64(10)
	depositSeedCanonical(t, ctx, pool, chainID, h1, tip, true)
	bh1, txHash1 := confirmSeedPending(t, ctx, pool, chainID, h1)
	bh2, txHash2 := depositBlockHash(h2), depositTxHash(h2, 0)
	depositSeedObservation(t, ctx, pool, chainID, h2, bh2, txHash2, 0, "1", 1)
	confirmSeedPolicyRow(t, ctx, pool, chainID, 1, int64(n), nil, "bootstrap", nil)

	basis1 := ConfirmBasis{BlockHash: bh1, TxHash: txHash1, Height: h1,
		TipNumber: tip, TipHash: depositBlockHash(tip), PolicySeq: 1, ThresholdN: n}
	basis2 := ConfirmBasis{BlockHash: bh2, TxHash: txHash2, Height: h2,
		TipNumber: tip, TipHash: depositBlockHash(tip), PolicySeq: 1, ThresholdN: n}

	// Pre-crash worker on its own pool with a short-TTL lease converts h1.
	crashPool := openIndexerPool(t, dsn)
	crasher, err := NewConfirmationCommitter(crashPool, ConfirmationConfig{ChainID: chainID, ThresholdN: n})
	if err != nil {
		t.Fatalf("NewConfirmationCommitter(crasher): %v", err)
	}
	crashLease := depositITOwnedLease(t, pool, chainID, "crasher-31349", time.Second, 250*time.Millisecond)
	if err := crasher.ConfirmDepositUnit(ctx, crashLease, basis1); err != nil {
		t.Fatalf("pre-crash ConfirmDepositUnit() = %v", err)
	}
	at1 := confirm13ConfirmedAt(t, ctx, pool, chainID, bh1, txHash1)
	oldStatus, oldNullAt, oldTipN, oldThr, oldSeq, oldTipHash, oldConf :=
		confirmReadBasis(t, ctx, pool, chainID, bh1, txHash1)

	// kill -9: drop every connection of the crashed worker, then wait for
	// its lease to expire on the DB clock so a fresh owner can take over.
	crashPool.Close()
	depositWaitLeaseExpired(t, ctx, pool, chainID)

	// Restart: fresh pool, fresh committer, freshly acquired lease against
	// the same test DB. Durable progress is re-read, never inherited.
	freshPool := openIndexerPool(t, dsn)
	defer freshPool.Close()
	restarter, err := NewConfirmationCommitter(freshPool, ConfirmationConfig{ChainID: chainID, ThresholdN: n})
	if err != nil {
		t.Fatalf("NewConfirmationCommitter(restarter): %v", err)
	}
	freshLease := depositITOwnedLease(t, pool, chainID, "restarted-31349", time.Minute, 10*time.Second)

	// The pre-crash conversion converges byte-identical (confirmed untouched).
	if err := restarter.ConfirmDepositUnit(ctx, freshLease, basis1); err != nil {
		t.Fatalf("restart re-check of h1 = %v, want nil (converge)", err)
	}
	if got := confirm13ConfirmedAt(t, ctx, pool, chainID, bh1, txHash1); got != at1 {
		t.Fatalf("h1 confirmed_at changed %s -> %s across restart (rewrote first-seen time)", at1, got)
	}
	status, nullAt, tipN, thr, seq, gotTipHash, conf :=
		confirmReadBasis(t, ctx, pool, chainID, bh1, txHash1)
	if status != oldStatus || nullAt != oldNullAt || tipN != oldTipN || thr != oldThr ||
		seq != oldSeq || gotTipHash != oldTipHash || conf != oldConf {
		t.Fatal("h1 basis changed across restart (confirmed row was rewritten)")
	}

	// The still-pending row is processed exactly once with the exact basis.
	if err := restarter.ConfirmDepositUnit(ctx, freshLease, basis2); err != nil {
		t.Fatalf("restart ConfirmDepositUnit(h2) = %v, want nil", err)
	}
	status2, nullAt2, tipN2, thr2, seq2, gotTipHash2, conf2 :=
		confirmReadBasis(t, ctx, pool, chainID, bh2, txHash2)
	if status2 != "confirmed" || nullAt2 {
		t.Fatalf("h2 status=%s nullAt=%v after restart, want confirmed/non-null confirmed_at", status2, nullAt2)
	}
	if tipN2 != int64(tip) || gotTipHash2 != depositBlockHash(tip) || thr2 != int64(n) || seq2 != 1 || conf2 != "10" {
		t.Fatalf("h2 basis after restart = tip(%d %s) N=%d conf=%s seq=%d, want tip(%d %s) N=10 conf=10 seq=1",
			tipN2, gotTipHash2, thr2, conf2, seq2, tip, depositBlockHash(tip))
	}
	var confirmed, pending int
	if err := pool.QueryRow(ctx, `
SELECT count(*) FILTER (WHERE status = 'confirmed'), count(*) FILTER (WHERE status = 'pending')
FROM deposit_observations WHERE chain_id = $1`, chainID).Scan(&confirmed, &pending); err != nil {
		t.Fatalf("count by status: %v", err)
	}
	if confirmed != 2 || pending != 0 {
		t.Fatalf("confirmed=%d pending=%d after restart, want 2/0 (no omission, no duplication)", confirmed, pending)
	}
	if k := depositCountRows(t, ctx, pool, "confirmation_policy_history", chainID); k != 1 {
		t.Fatalf("confirmation_policy_history rows = %d, want 1 (restart writes no policy row)", k)
	}
	var incomplete int
	if err := pool.QueryRow(ctx, `
SELECT COUNT(*) FROM deposit_observations
WHERE chain_id = $1 AND status = 'confirmed'
  AND (confirmed_at IS NULL OR confirm_tip_number IS NULL OR confirm_tip_hash IS NULL
       OR confirm_threshold IS NULL OR confirmations IS NULL OR confirm_policy_seq IS NULL)`,
		chainID).Scan(&incomplete); err != nil {
		t.Fatalf("integrity check: %v", err)
	}
	if incomplete != 0 {
		t.Fatalf("incomplete confirmed rows = %d, want 0", incomplete)
	}
}

// -- T023 --------------------------------------------------------------------

// T023 [US4] no-rewrite regression (FR-05/I2, data-model Table 1 immutability
// mechanism, US4-1 no-rewrite side, SC-08; quickstart D2/D5 behavioral half):
// a confirmed row rejects every rewrite attempt with zero rows affected and
// byte-identical first-seen facts (confirmed_at + all six basis columns),
// and the contracts integrity SQL reports 0.
//
// Write-path list review (static half, NOT re-proven here): the repo-wide 005
// write path to deposit_observations is exactly the single commit-tx
// conditional UPDATE confirmDepositObservationSQL (confirmcommit.go, with the
// status='pending' predicate). That static half is closed by the grep gate
// TestDepositWritePathConfinement (depositscanner_test.go, extended in commit
// 5d43e98: exactly one UPDATE of deposit_observations in confirmcommit.go,
// confirmation_policy_history update-free, pending literals pinned, probe
// interception proof). The tests below prove the behavioral half: the
// zero-row property through every plausible application path. A
// predicate-less UPDATE is deliberately never attempted — it would succeed by
// design (no DB triggers, data-model Table 1: application predicate + tests
// carry the guarantee so 006 inherits zero constraints); the gate above is
// what guarantees no such statement exists in production code.

// TestConfirmationConfirmedRowsRejectEveryRewritePath is T023(a): seed one
// Pending, convert it, then attempt rewrites through every plausible path —
// (1) re-commit of the same basis through ConfirmDepositUnit (converge, nil),
// (2) raw conditional UPDATE of the basis columns with the pending predicate
// on the confirmed PK, (3) raw predicate UPDATE attempting a status/timestamp
// touch on the confirmed PK — and assert each affects 0 rows with
// confirmed_at and every basis column byte-identical afterwards.
func TestConfirmationConfirmedRowsRejectEveryRewritePath(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	const chainID, h, tip, n = int64(31350), uint64(100), uint64(109), uint64(10)
	depositSeedCanonical(t, ctx, pool, chainID, h, tip, true)
	bh, txHash := confirmSeedPending(t, ctx, pool, chainID, h)
	confirmSeedPolicyRow(t, ctx, pool, chainID, 1, int64(n), nil, "bootstrap", nil)

	c, lease := confirmCommitter(t, pool, chainID, n)
	basis := ConfirmBasis{BlockHash: bh, TxHash: txHash, Height: h,
		TipNumber: tip, TipHash: depositBlockHash(tip), PolicySeq: 1, ThresholdN: n}
	if err := c.ConfirmDepositUnit(ctx, lease, basis); err != nil {
		t.Fatalf("ConfirmDepositUnit(): %v", err)
	}

	firstAt := confirm13ConfirmedAt(t, ctx, pool, chainID, bh, txHash)
	firstStatus, firstNullAt, firstTipN, firstThr, firstSeq, firstTipHash, firstConf :=
		confirmReadBasis(t, ctx, pool, chainID, bh, txHash)
	if firstStatus != "confirmed" || firstNullAt {
		t.Fatalf("status=%s nullAt=%v after convert, want confirmed/non-null confirmed_at", firstStatus, firstNullAt)
	}
	assertUnchanged := func(stage string) {
		t.Helper()
		if got := confirm13ConfirmedAt(t, ctx, pool, chainID, bh, txHash); got != firstAt {
			t.Fatalf("%s: confirmed_at changed %s -> %s (rewrote first-seen time)", stage, firstAt, got)
		}
		status, nullAt, tipN, thr, seq, gotTipHash, conf :=
			confirmReadBasis(t, ctx, pool, chainID, bh, txHash)
		if status != firstStatus || nullAt != firstNullAt || tipN != firstTipN || thr != firstThr ||
			seq != firstSeq || gotTipHash != firstTipHash || conf != firstConf {
			t.Fatalf("%s: basis changed: was (%s %d %s %d %s %d), now (%s %d %s %d %s %d)",
				stage, firstStatus, firstTipN, firstTipHash, firstThr, firstConf, firstSeq,
				status, tipN, gotTipHash, thr, conf, seq)
		}
	}

	// Path 1: re-commit of the same basis converges (nil) with zero writes.
	if err := c.ConfirmDepositUnit(ctx, lease, basis); err != nil {
		t.Fatalf("re-commit ConfirmDepositUnit() = %v, want nil (converge)", err)
	}
	assertUnchanged("re-commit same basis")

	// Path 2: raw conditional UPDATE of the basis columns carrying the
	// pending predicate, aimed at the confirmed PK with foreign values.
	tag, err := pool.Exec(ctx, `
UPDATE deposit_observations
SET confirm_tip_number = $5, confirm_tip_hash = $6, confirm_threshold = $7,
    confirmations = $8, confirm_policy_seq = $9
WHERE chain_id = $1 AND block_hash = $2 AND tx_hash = $3 AND log_index = $4
  AND status = 'pending'`,
		chainID, bh, txHash, 0, int64(tip+1), depositBlockHash(tip), int64(n), "99", 1)
	if err != nil {
		t.Fatalf("predicate basis UPDATE: %v", err)
	}
	if got := tag.RowsAffected(); got != 0 {
		t.Fatalf("predicate basis UPDATE affected %d rows, want 0 (confirmed row is unwritable)", got)
	}
	assertUnchanged("predicate basis UPDATE")

	// Path 3: raw predicate UPDATE attempting a status/timestamp touch on
	// the confirmed PK.
	tag, err = pool.Exec(ctx, `
UPDATE deposit_observations
SET status = 'confirmed', confirmed_at = now()
WHERE chain_id = $1 AND block_hash = $2 AND tx_hash = $3 AND log_index = $4
  AND status = 'pending'`,
		chainID, bh, txHash, 0)
	if err != nil {
		t.Fatalf("predicate status-touch UPDATE: %v", err)
	}
	if got := tag.RowsAffected(); got != 0 {
		t.Fatalf("predicate status-touch UPDATE affected %d rows, want 0 (confirmed row is unwritable)", got)
	}
	assertUnchanged("predicate status-touch UPDATE")

	// Exactly one durable conversion; the contracts integrity SQL reports 0.
	var confirmed int
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM deposit_observations WHERE chain_id = $1 AND status = 'confirmed'`,
		chainID).Scan(&confirmed); err != nil {
		t.Fatalf("count confirmed: %v", err)
	}
	if confirmed != 1 {
		t.Fatalf("confirmed rows = %d, want exactly 1 (rewrite attempts wrote nothing)", confirmed)
	}
	var restartIncomplete int
	if err := pool.QueryRow(ctx, `
SELECT COUNT(*) FROM deposit_observations
WHERE chain_id = $1 AND status = 'confirmed'
  AND (confirmed_at IS NULL OR confirm_tip_number IS NULL OR confirm_tip_hash IS NULL
       OR confirm_threshold IS NULL OR confirmations IS NULL OR confirm_policy_seq IS NULL)`,
		chainID).Scan(&restartIncomplete); err != nil {
		t.Fatalf("integrity check: %v", err)
	}
	if restartIncomplete != 0 {
		t.Fatalf("incomplete confirmed rows = %d, want 0", restartIncomplete)
	}
}

// TestConfirmationIntegritySpotCheckSQL is T023(b/c): the write-path list
// review pins the commit-tx UPDATE predicate on the constant itself (the
// repo-wide grep half stays with TestDepositWritePathConfinement per the T023
// banner — this is a constant pin, not a second grep), and the verbatim
// contracts/observability.md integrity SQL asserts 0 over two conversions
// while a still-pending row proves the spot-check only flags confirmed rows.
func TestConfirmationIntegritySpotCheckSQL(t *testing.T) {
	// Write-path list review, constant pin: the single approved 005 write
	// path to deposit_observations carries the status='pending' predicate.
	// (Full-repo confinement is asserted by TestDepositWritePathConfinement;
	// cited, not duplicated.)
	if !strings.Contains(confirmDepositObservationSQL, "status = 'pending'") {
		t.Fatal("confirmDepositObservationSQL lost its status='pending' predicate (sole 005 write path)")
	}

	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	const chainID, h1, h2, h3, tip, n = int64(31351), uint64(100), uint64(101), uint64(102), uint64(111), uint64(10)
	depositSeedCanonical(t, ctx, pool, chainID, h1, tip, true)
	bh1, txHash1 := confirmSeedPending(t, ctx, pool, chainID, h1)
	bh2, txHash2 := depositBlockHash(h2), depositTxHash(h2, 0)
	depositSeedObservation(t, ctx, pool, chainID, h2, bh2, txHash2, 0, "1", 1)
	bh3, txHash3 := depositBlockHash(h3), depositTxHash(h3, 0)
	depositSeedObservation(t, ctx, pool, chainID, h3, bh3, txHash3, 0, "1", 1)
	confirmSeedPolicyRow(t, ctx, pool, chainID, 1, int64(n), nil, "bootstrap", nil)

	c, lease := confirmCommitter(t, pool, chainID, n)
	for i, b := range []ConfirmBasis{
		{BlockHash: bh1, TxHash: txHash1, Height: h1,
			TipNumber: tip, TipHash: depositBlockHash(tip), PolicySeq: 1, ThresholdN: n},
		{BlockHash: bh2, TxHash: txHash2, Height: h2,
			TipNumber: tip, TipHash: depositBlockHash(tip), PolicySeq: 1, ThresholdN: n},
	} {
		if err := c.ConfirmDepositUnit(ctx, lease, b); err != nil {
			t.Fatalf("ConfirmDepositUnit(#%d): %v", i+1, err)
		}
	}

	// Verbatim contracts/observability.md 转换完整性抽查 SQL: must return 0
	// (confirmed rows all-basis-non-null), with a pending row present.
	var incomplete int
	if err := pool.QueryRow(ctx, `
SELECT COUNT(*) FROM deposit_observations
WHERE chain_id = $1 AND status = 'confirmed'
  AND (confirmed_at IS NULL OR confirm_tip_number IS NULL OR confirm_tip_hash IS NULL
       OR confirm_threshold IS NULL OR confirmations IS NULL OR confirm_policy_seq IS NULL)`,
		chainID).Scan(&incomplete); err != nil {
		t.Fatalf("integrity spot-check SQL: %v", err)
	}
	if incomplete != 0 {
		t.Fatalf("incomplete confirmed rows = %d, want 0", incomplete)
	}
	var confirmed, pending int
	if err := pool.QueryRow(ctx, `
SELECT count(*) FILTER (WHERE status = 'confirmed'), count(*) FILTER (WHERE status = 'pending')
FROM deposit_observations WHERE chain_id = $1`, chainID).Scan(&confirmed, &pending); err != nil {
		t.Fatalf("count by status: %v", err)
	}
	if confirmed != 2 || pending != 1 {
		t.Fatalf("confirmed=%d pending=%d, want 2/1 (two conversions, one row left pending)", confirmed, pending)
	}
	if k := depositCountRows(t, ctx, pool, "confirmation_policy_history", chainID); k != 1 {
		t.Fatalf("confirmation_policy_history rows = %d, want 1 (conversions write no policy row)", k)
	}
}
