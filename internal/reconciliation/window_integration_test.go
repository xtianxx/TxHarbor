//go:build integration

// window_integration_test.go is the T036 integration acceptance layer over the
// real durable local index (indexer_checkpoint/chain_blocks) plus the scan
// loop (FR-001 time kind, FR-003/004/017/019, Q3; quickstart §3/§7/§8):
//
//   - the production HeaderBlockTimeReader reads durable bounds and canonical
//     identities and never guesses a chain time from the local clock;
//   - boundary time intervals map to inclusive height windows against real
//     rows, and a reorg-contradicted mapping resolves to `invalidated`;
//   - ScanOnce consumes the resolver: a resolved window drives the chain read
//     with the mapped heights, an unmappable window is an explicit gap with
//     the chain party unknown (never an "on-chain absence" claim), an
//     invalidated mapping stays pending, and the task cannot close over the
//     open gaps;
//   - an exhausted budget stops the scan boundedly with an honest checkpoint
//     (no pointer movement, paused + budget_exhausted gaps) and a resume from
//     the same pointer covers every interval exactly once.
//
// PostgreSQL comes from testcontainers via the shared reconIT helpers. When no
// Docker provider is healthy the package reports NOT RUN (t.Skip), never a pass.
package reconciliation

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// windowTestChainID is the numeric chain id every window test seeds.
const windowTestChainID = int64(31337)

// windowHeaderFor builds the deterministic header of one seeded height. The
// Extra tag varies so a test can request a different header hash at the same
// height (the reorg contradiction case).
func windowHeaderFor(height int64, seconds uint64, tag string) *types.Header {
	return &types.Header{
		Number: big.NewInt(height),
		Time:   seconds,
		Extra:  []byte(tag),
	}
}

// windowSeedHash is the canonical hash of one seeded block: exactly the header
// hash the chain-time reader observes.
func windowSeedHash(height int64, seconds uint64) string {
	return strings.ToLower(windowHeaderFor(height, seconds, windowSeedTag(height)).Hash().Hex())
}

// windowSeedTag is the deterministic block tag used by the seed and the fake
// header client.
func windowSeedTag(height int64) string { return fmt.Sprintf("txharbor-block-%d", height) }

// windowSeedBlocks inserts chain_blocks rows for the given ascending heights
// (one block per second starting at baseUnix) and points indexer_checkpoint at
// the tip. It returns the chain-time map the fake header client must serve.
func windowSeedBlocks(t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	chainID int64, baseUnix int64, heights []int64) map[int64]uint64 {
	t.Helper()
	if len(heights) == 0 {
		t.Fatalf("window seed requires at least one height")
	}
	seconds := make(map[int64]uint64, len(heights))
	parent := "0x" + strings.Repeat("00", 32)
	for i, height := range heights {
		at := uint64(baseUnix + int64(i))
		seconds[height] = at
		hash := windowSeedHash(height, at)
		if _, err := pool.Exec(ctx, `
			INSERT INTO chain_blocks (chain_id, number, hash, parent_hash, canonical, indexed_at)
			VALUES ($1, $2, $3, $4, true, now())`,
			chainID, height, hash, parent); err != nil {
			t.Fatalf("seed chain block %d: %v", height, err)
		}
		parent = hash
	}
	tip := heights[len(heights)-1]
	if _, err := pool.Exec(ctx, `
		INSERT INTO indexer_checkpoint (chain_id, height, block_hash, start_height, updated_at)
		VALUES ($1, $2, $3, $4, now())`,
		chainID, tip, windowSeedHash(tip, seconds[tip]), heights[0]); err != nil {
		t.Fatalf("seed indexer checkpoint: %v", err)
	}
	return seconds
}

// windowHeaderClient is the deterministic HeaderClient of the window tests: no
// chain, no Anvil, only the seeded heights.
type windowHeaderClient struct {
	chainID int64
	times   map[int64]uint64
	extra   map[int64]string
}

func (c windowHeaderClient) ChainID(ctx context.Context) (*big.Int, error) {
	return big.NewInt(c.chainID), nil
}

func (c windowHeaderClient) HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error) {
	height := number.Int64()
	seconds, ok := c.times[height]
	if !ok {
		return nil, nil
	}
	tag := windowSeedTag(height)
	if override, ok := c.extra[height]; ok {
		tag = override
	}
	return windowHeaderFor(height, seconds, tag), nil
}

// windowCountingHeaderClient counts the header RPCs issued, proving the probe
// bound really bounds the reads.
type windowCountingHeaderClient struct {
	inner windowHeaderClient
	calls int
}

func (c *windowCountingHeaderClient) ChainID(ctx context.Context) (*big.Int, error) {
	return c.inner.ChainID(ctx)
}

func (c *windowCountingHeaderClient) HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error) {
	c.calls++
	return c.inner.HeaderByNumber(ctx, number)
}

// windowSeedTimeTask seeds one running time-scoped task with the numeric chain
// id the resolver requires.
func windowSeedTimeTask(t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	taskID string, chainID int64, from, to time.Time) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO recon_task (task_id, scope_chain_id, scope_kind, scope_start_at, scope_end_at,
		    business_types, upstream_receipt_source, policy_refs, state, budget, created_by)
		VALUES ($1::uuid, $2, 'time', $3, $4, ARRAY['withdrawal']::text[],
		        '{"withdrawal":{"source":"it-window","connected":true}}'::jsonb,
		        '{"confirm_threshold_n":3}'::jsonb, 'running', '{}'::jsonb, 'it-recon')`,
		taskID, fmt.Sprintf("%d", chainID), from, to); err != nil {
		t.Fatalf("seed time task %s: %v", taskID, err)
	}
}

// windowPersistedThroughTime reads the time-typed persisted pointer.
func windowPersistedThroughTime(t *testing.T, ctx context.Context, pool *pgxpool.Pool, taskID string) (time.Time, bool) {
	t.Helper()
	var persisted *time.Time
	err := pool.QueryRow(ctx, `
		SELECT result_persisted_through_at FROM recon_checkpoint
		WHERE task_id = $1::uuid ORDER BY seq DESC LIMIT 1`, taskID).Scan(&persisted)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, false
	}
	if err != nil {
		t.Fatalf("read time pointer of %s: %v", taskID, err)
	}
	if persisted == nil {
		return time.Time{}, false
	}
	return persisted.UTC(), true
}

// windowChainSeed is the shared seeded timeline of the scan-level tests.
type windowChainSeed struct {
	base    time.Time
	seconds map[int64]uint64
	client  windowHeaderClient
}

// windowSeedTimeline seeds heights 100..104 (one block per second at base) and
// returns the timeline plus the matching header client.
func windowSeedTimeline(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, base time.Time) windowChainSeed {
	t.Helper()
	seconds := windowSeedBlocks(t, ctx, pool, chainID, base.Unix(), []int64{100, 101, 102, 103, 104})
	return windowChainSeed{
		base:    base,
		seconds: seconds,
		client:  windowHeaderClient{chainID: chainID, times: seconds},
	}
}

// windowScanChain is the compare-loop chain surface: it returns a complete
// bundle for the observed window and records every observed window.
type windowScanChain struct {
	indexedAt time.Time
	windows   [][2]int64
}

func (c *windowScanChain) Observe(ctx context.Context, q ChainFactsQuery) (ChainFactsBundle, error) {
	c.windows = append(c.windows, [2]int64{q.From, q.To})
	bundle := ChainFactsBundle{
		ChainID: q.ChainID, From: q.From, To: q.To,
		Source: ChainSourceLocalIndex, CapturedAt: c.indexedAt, Status: ChainFactsComplete,
	}
	for height := q.From; height <= q.To; height++ {
		bundle.Blocks = append(bundle.Blocks, ChainFactBlock{
			Number: height, Hash: "0x" + strings.Repeat("ab", 32), Canonical: true, IndexedAt: c.indexedAt,
		})
	}
	return bundle, nil
}

// windowScanRequest is the shared time-scope scan invocation shape.
func windowScanRequest(taskID string, resolver ScanTimeWindowResolver,
	chain ChainFactsReader, candidates func(interval ScanInterval) []ScanCandidate,
	maxPG int) ScanOnceRequest {
	return ScanOnceRequest{
		TaskID: taskID, Owner: reconITOwner, LeaseTTL: time.Minute,
		Limits: BudgetLimits{
			MaxConcurrency: 1, MaxSpanPerClaim: 2_000_000, MaxDuration: time.Minute,
			MaxPGRequests: maxPG, MaxRPCRequests: 20,
		},
		FreshnessTolerance: time.Hour,
		Sources: ScanSources{
			Chain:  chain,
			PG:     fakeScanPGState{readAt: time.Now().UTC()},
			Events: fakeScanEventState{capturedAt: time.Now().UTC()},
		},
		Candidates:     staticScanCandidates{candidates: candidates},
		WindowResolver: resolver,
	}
}

func TestIntegrationHeaderBlockTimeReaderOverDurableIndex(t *testing.T) {
	ctx, pool, _ := reconIT(t)
	base := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	timeline := windowSeedTimeline(t, ctx, pool, windowTestChainID, base)

	reader, err := NewHeaderBlockTimeReader(pool, timeline.client, time.Second)
	if err != nil {
		t.Fatalf("NewHeaderBlockTimeReader: %v", err)
	}
	start, tip, ok, err := reader.IndexedBounds(ctx, windowTestChainID)
	if err != nil || !ok || start != 100 || tip != 104 {
		t.Fatalf("IndexedBounds = (%d, %d, %v, %v), want (100, 104, true, nil)", start, tip, ok, err)
	}
	at, hash, ok, err := reader.ChainTimeAt(ctx, windowTestChainID, 101)
	if err != nil || !ok {
		t.Fatalf("ChainTimeAt(101) = (%v, %q, %v, %v), want the seeded header", at, hash, ok, err)
	}
	if !at.Equal(time.Unix(int64(timeline.seconds[101]), 0).UTC()) {
		t.Fatalf("chain time at 101 = %v, want the header time (never the local insert clock)", at)
	}
	if hash != windowSeedHash(101, timeline.seconds[101]) {
		t.Fatalf("chain header hash at 101 = %q, want the seeded canonical hash", hash)
	}
	canonical, ok, err := reader.CanonicalHashAt(ctx, windowTestChainID, 101)
	if err != nil || !ok || canonical != hash {
		t.Fatalf("CanonicalHashAt(101) = (%q, %v, %v), want the seeded hash", canonical, ok, err)
	}

	// A height the node does not serve and a chain without durable coverage
	// both report ok=false, never a guessed value.
	if _, _, ok, err := reader.ChainTimeAt(ctx, windowTestChainID, 200); err != nil || ok {
		t.Fatalf("ChainTimeAt(200) ok = %v (err %v), want unknown", ok, err)
	}
	if _, _, ok, err := reader.IndexedBounds(ctx, 99999); err != nil || ok {
		t.Fatalf("IndexedBounds(unknown chain) ok = %v (err %v), want no coverage", ok, err)
	}
	if _, ok, err := reader.CanonicalHashAt(ctx, 99999, 101); err != nil || ok {
		t.Fatalf("CanonicalHashAt(unknown chain) ok = %v (err %v), want unprovable", ok, err)
	}

	// A non-canonical row is not a provable canonical identity.
	if _, err := pool.Exec(ctx,
		`UPDATE chain_blocks SET canonical = false WHERE chain_id = $1 AND number = 103`, windowTestChainID); err != nil {
		t.Fatalf("mark block 103 non-canonical: %v", err)
	}
	if _, ok, err := reader.CanonicalHashAt(ctx, windowTestChainID, 103); err != nil || ok {
		t.Fatalf("CanonicalHashAt(non-canonical) ok = %v (err %v), want unprovable", ok, err)
	}
}

func TestIntegrationTimeHeightResolverOverDurableIndex(t *testing.T) {
	ctx, pool, _ := reconIT(t)
	base := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	timeline := windowSeedTimeline(t, ctx, pool, windowTestChainID, base)

	reader, err := NewHeaderBlockTimeReader(pool, timeline.client, time.Second)
	if err != nil {
		t.Fatalf("NewHeaderBlockTimeReader: %v", err)
	}
	resolver := windowTestResolver(t, reader, 8)

	t.Run("boundaries_map_against_real_rows", func(t *testing.T) {
		resolution, err := resolver.ResolveScanWindow(ctx, windowTestChainID,
			windowTimeInterval(base, base.Add(4*time.Second)), &windowTestBudget{pgLimit: 64, rpcLimit: 64})
		if err != nil {
			t.Fatalf("ResolveScanWindow: %v", err)
		}
		if resolution.Status != WindowResolved || resolution.From != 100 || resolution.To != 104 {
			t.Fatalf("resolution = %+v, want resolved 100..104", resolution)
		}
		resolution, err = resolver.ResolveScanWindow(ctx, windowTestChainID,
			windowTimeInterval(base.Add(500*time.Millisecond), base.Add(2500*time.Millisecond)),
			&windowTestBudget{pgLimit: 64, rpcLimit: 64})
		if err != nil || resolution.Status != WindowResolved || resolution.From != 101 || resolution.To != 102 {
			t.Fatalf("between-block resolution = %+v (err %v), want 101..102", resolution, err)
		}
	})

	t.Run("reorg_contradiction_invalidates", func(t *testing.T) {
		contradicting := timeline.client
		contradicting.extra = map[int64]string{102: "txharbor-block-102-replaced"}
		reorgReader, err := NewHeaderBlockTimeReader(pool, contradicting, time.Second)
		if err != nil {
			t.Fatalf("NewHeaderBlockTimeReader: %v", err)
		}
		reorgResolver := windowTestResolver(t, reorgReader, 8)
		resolution, err := reorgResolver.ResolveScanWindow(ctx, windowTestChainID,
			windowTimeInterval(base.Add(2*time.Second), base.Add(2*time.Second)),
			&windowTestBudget{pgLimit: 64, rpcLimit: 64})
		if err != nil {
			t.Fatalf("ResolveScanWindow: %v", err)
		}
		if resolution.Status != WindowInvalidated || resolution.Reason != WindowReasonReorgInvalidated {
			t.Fatalf("resolution = %+v, want invalidated/%s", resolution, WindowReasonReorgInvalidated)
		}
	})

	t.Run("unprovable_canonical_identity_is_unmappable", func(t *testing.T) {
		// A height inside the mapping with no durable canonical row can never
		// be treated as mapped.
		if _, err := pool.Exec(ctx,
			`DELETE FROM chain_blocks WHERE chain_id = $1 AND number = 102`, windowTestChainID); err != nil {
			t.Fatalf("delete canonical row 102: %v", err)
		}
		resolution, err := resolver.ResolveScanWindow(ctx, windowTestChainID,
			windowTimeInterval(base.Add(2*time.Second), base.Add(2*time.Second)),
			&windowTestBudget{pgLimit: 64, rpcLimit: 64})
		if err != nil {
			t.Fatalf("ResolveScanWindow: %v", err)
		}
		if resolution.Status != WindowUnmappable || resolution.Reason != WindowReasonIdentityNotCanonical {
			t.Fatalf("resolution = %+v, want unmappable/%s", resolution, WindowReasonIdentityNotCanonical)
		}
	})

	t.Run("no_durable_coverage_never_guesses_a_mapping", func(t *testing.T) {
		resolution, err := resolver.ResolveScanWindow(ctx, 31338,
			windowTimeInterval(base, base.Add(4*time.Second)), &windowTestBudget{pgLimit: 64, rpcLimit: 64})
		if err != nil {
			t.Fatalf("ResolveScanWindow: %v", err)
		}
		if resolution.Status != WindowUnmappable || resolution.Reason != WindowReasonIndexUnavailable {
			t.Fatalf("resolution = %+v, want unmappable/%s", resolution, WindowReasonIndexUnavailable)
		}
	})

	t.Run("probe_bound_bounds_the_header_reads", func(t *testing.T) {
		counting := &windowCountingHeaderClient{inner: timeline.client}
		countingReader, err := NewHeaderBlockTimeReader(pool, counting, time.Second)
		if err != nil {
			t.Fatalf("NewHeaderBlockTimeReader: %v", err)
		}
		boundResolver := windowTestResolver(t, countingReader, 2)
		from := base.Add(time.Hour)
		resolution, err := boundResolver.ResolveScanWindow(ctx, windowTestChainID,
			windowTimeInterval(from, from), &windowTestBudget{pgLimit: 64, rpcLimit: 64})
		if err != nil {
			t.Fatalf("ResolveScanWindow: %v", err)
		}
		if resolution.Status != WindowUnmappable || resolution.Reason != WindowReasonProbeLimit {
			t.Fatalf("resolution = %+v, want unmappable/%s", resolution, WindowReasonProbeLimit)
		}
		if counting.calls > 2 {
			t.Fatalf("probe bound 2 issued %d header reads", counting.calls)
		}
	})

	t.Run("exhausted_budget_aborts_the_mapping", func(t *testing.T) {
		_, err := resolver.ResolveScanWindow(ctx, windowTestChainID,
			windowTimeInterval(base, base.Add(4*time.Second)), &windowTestBudget{pgLimit: 1, rpcLimit: 64})
		if !errors.Is(err, ErrBudgetExhausted) {
			t.Fatalf("exhausted mapping err = %v, want ErrBudgetExhausted", err)
		}
	})
}

func TestIntegrationScanOnceTimeWindowMapping(t *testing.T) {
	ctx, pool, store := reconIT(t)
	base := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	timeline := windowSeedTimeline(t, ctx, pool, windowTestChainID, base)

	reader, err := NewHeaderBlockTimeReader(pool, timeline.client, time.Second)
	if err != nil {
		t.Fatalf("NewHeaderBlockTimeReader: %v", err)
	}
	resolver := windowTestResolver(t, reader, 8)

	t.Run("resolved_windows_drive_the_chain_read", func(t *testing.T) {
		taskID := uuid.NewString()
		windowSeedTimeTask(t, ctx, pool, taskID, windowTestChainID, base, base.Add(4*time.Second))
		chain := &windowScanChain{indexedAt: time.Now().UTC().Add(-time.Minute)}

		result, err := store.ScanOnce(ctx, windowScanRequest(taskID, resolver, chain, nil, 100))
		if err != nil {
			t.Fatalf("ScanOnce: %v", err)
		}
		if result.Stop != ScanStopScopeExhausted || result.Suspended {
			t.Fatalf("result = stop %q suspended %v, want scope_exhausted", result.Stop, result.Suspended)
		}
		if result.Committed != 3 || result.Attempts != 3 {
			t.Fatalf("committed/attempts = %d/%d, want 3/3 (2,000,000-position claims plus the one-position tail)",
				result.Committed, result.Attempts)
		}
		if result.WindowUnmappable != 0 || result.WindowInvalidated != 0 || result.Pending != 0 || result.Gaps != 0 {
			t.Fatalf("window counters/pending/gaps = %d/%d/%d/%d, want all zero",
				result.WindowUnmappable, result.WindowInvalidated, result.Pending, result.Gaps)
		}
		want := [][2]int64{{100, 101}, {102, 103}, {104, 104}}
		if len(chain.windows) != len(want) {
			t.Fatalf("chain windows = %v, want %v", chain.windows, want)
		}
		for i := range want {
			if chain.windows[i] != want[i] {
				t.Fatalf("chain window %d = %v, want %v (boundary mapping)", i, chain.windows[i], want[i])
			}
		}
		if got, ok := windowPersistedThroughTime(t, ctx, pool, taskID); !ok || !got.Equal(base.Add(4*time.Second)) {
			t.Fatalf("time pointer = %v (present %v), want the scope end", got, ok)
		}
	})

	t.Run("unmappable_window_is_a_gap_never_an_absence_claim", func(t *testing.T) {
		taskID := uuid.NewString()
		windowSeedTimeTask(t, ctx, pool, taskID, 31338, base, base.Add(4*time.Second))
		chain := &windowScanChain{indexedAt: time.Now().UTC().Add(-time.Minute)}
		candidates := func(interval ScanInterval) []ScanCandidate {
			return []ScanCandidate{{
				BusinessType: BusinessWithdrawal,
				BusinessKey: BusinessKey{
					Kind:  BusinessKeyRequestID,
					Value: fmt.Sprintf("unmapped-%d", interval.From.Time.UnixMicro()),
				},
				ChainFact: ChainFactRef{BlockNumber: 100, BlockHash: "0x" + strings.Repeat("ab", 32)},
			}}
		}
		result, err := store.ScanOnce(ctx, windowScanRequest(taskID, resolver, chain, candidates, 100))
		if err != nil {
			t.Fatalf("ScanOnce: %v", err)
		}
		if result.WindowUnmappable != 3 {
			t.Fatalf("window_unmappable = %d, want 3", result.WindowUnmappable)
		}
		if result.Tickets != 0 || result.Pending != 3 {
			t.Fatalf("tickets/pending = %d/%d, want 0/3 (an unmapped range is an explicit gap, not a conclusion)",
				result.Tickets, result.Pending)
		}
		if len(chain.windows) != 0 {
			t.Fatalf("chain was read over an unmappable window: %v", chain.windows)
		}
		gaps := reconGapReasons(t, ctx, pool, taskID)
		if gaps[string(GapQueryFailed)] != 3 {
			t.Fatalf("gaps = %v, want query_failed=3", gaps)
		}
		if _, err := store.TransitionTask(ctx, TaskTransitionRequest{
			TaskID: taskID, To: TaskStateDone, Reason: "close over an unmapped window", Actor: reconITOpsActor,
		}); !errors.Is(err, ErrTaskNotComplete) {
			t.Fatalf("done over an unmapped window err = %v, want ErrTaskNotComplete", err)
		}
	})

	t.Run("invalidated_window_stays_pending", func(t *testing.T) {
		contradicting := timeline.client
		contradicting.extra = map[int64]string{104: "txharbor-block-104-replaced"}
		reorgReader, err := NewHeaderBlockTimeReader(pool, contradicting, time.Second)
		if err != nil {
			t.Fatalf("NewHeaderBlockTimeReader: %v", err)
		}
		reorgResolver := windowTestResolver(t, reorgReader, 8)

		taskID := uuid.NewString()
		windowSeedTimeTask(t, ctx, pool, taskID, windowTestChainID, base, base.Add(4*time.Second))
		chain := &windowScanChain{indexedAt: time.Now().UTC().Add(-time.Minute)}
		candidates := func(interval ScanInterval) []ScanCandidate {
			return []ScanCandidate{{
				BusinessType: BusinessWithdrawal,
				BusinessKey: BusinessKey{
					Kind:  BusinessKeyRequestID,
					Value: fmt.Sprintf("reorged-%d", interval.From.Time.UnixMicro()),
				},
				ChainFact: ChainFactRef{BlockNumber: 100, BlockHash: "0x" + strings.Repeat("ab", 32)},
			}}
		}
		// One interval [base, base+4s] mapping to the contradicted 100..104.
		request := windowScanRequest(taskID, reorgResolver, chain, candidates, 100)
		request.Limits.MaxSpanPerClaim = 4_100_000
		result, err := store.ScanOnce(ctx, request)
		if err != nil {
			t.Fatalf("ScanOnce: %v", err)
		}
		if result.WindowInvalidated != 1 || result.WindowUnmappable != 0 {
			t.Fatalf("window counters = invalidated %d / unmappable %d, want 1/0",
				result.WindowInvalidated, result.WindowUnmappable)
		}
		if result.Tickets != 0 || result.Pending != 1 {
			t.Fatalf("tickets/pending = %d/%d, want 0/1 (invalidated evidence never concludes)", result.Tickets, result.Pending)
		}
		if len(chain.windows) != 0 {
			t.Fatalf("chain was read over an invalidated window: %v", chain.windows)
		}
		if state, _ := reconTaskState(t, ctx, store, taskID); state != TaskStateRunning {
			t.Fatalf("task state = %s, want running (pending, never closed)", state)
		}
	})

	t.Run("budget_exhaustion_and_resume_cover_every_interval_once", func(t *testing.T) {
		taskID := uuid.NewString()
		windowSeedTimeTask(t, ctx, pool, taskID, windowTestChainID, base, base.Add(4*time.Second))
		chain := &windowScanChain{indexedAt: time.Now().UTC().Add(-time.Minute)}

		// 1 stale-recovery + 1 claim + 1 indexed-bounds charge fit; the first
		// canonical-identity charge is refused: a bounded stop inside the
		// resolver, not a silent partial mapping.
		suspended, err := store.ScanOnce(ctx, windowScanRequest(taskID, resolver, chain, nil, 3))
		if err != nil {
			t.Fatalf("suspending ScanOnce: %v", err)
		}
		if !suspended.Suspended || suspended.SuspendResource != ResourcePG || suspended.SuspendDetail == "" {
			t.Fatalf("suspension = %+v, want an observable PG suspension", suspended)
		}
		if suspended.Committed != 0 {
			t.Fatalf("committed = %d, want 0 (nothing persisted before the resolver stopped)", suspended.Committed)
		}
		if _, ok := windowPersistedThroughTime(t, ctx, pool, taskID); ok {
			t.Fatalf("pointer moved on a budget-suspended scan")
		}
		if state, reason := reconTaskState(t, ctx, store, taskID); state != TaskStateSuspendedBudget || reason == "" {
			t.Fatalf("task = %s (reason %q), want suspended_budget with a reason", state, reason)
		}
		gaps := reconGapReasons(t, ctx, pool, taskID)
		if gaps[string(GapPaused)] != 1 || gaps[string(GapBudgetExhausted)] != 1 {
			t.Fatalf("gaps = %v, want paused=1 and budget_exhausted=1", gaps)
		}
		if len(chain.windows) != 0 {
			t.Fatalf("chain was read before the mapping resolution completed: %v", chain.windows)
		}

		// A checkpoint head must stay honest: no persisted coverage exists.
		head, err := store.CheckpointHead(ctx, taskID)
		if err != nil || head != nil {
			t.Fatalf("checkpoint head = %+v (err %v), want none after the suspension", head, err)
		}

		// Resume: the same pointer restarts at the scope start and the full
		// budget covers every interval exactly once.
		if _, err := store.TransitionTask(ctx, TaskTransitionRequest{
			TaskID: taskID, To: TaskStateRunning, Reason: "quota restored", Actor: reconITOpsActor,
		}); err != nil {
			t.Fatalf("resume transition: %v", err)
		}
		resumed, err := store.ScanOnce(ctx, windowScanRequest(taskID, resolver, chain, nil, 100))
		if err != nil {
			t.Fatalf("resumed ScanOnce: %v", err)
		}
		if resumed.Stop != ScanStopScopeExhausted || resumed.Committed != 3 {
			t.Fatalf("resumed stop/committed = %q/%d, want scope_exhausted/3 (no interval missed)",
				resumed.Stop, resumed.Committed)
		}
		want := [][2]int64{{100, 101}, {102, 103}, {104, 104}}
		if len(chain.windows) != 3 || chain.windows[0] != want[0] || chain.windows[1] != want[1] || chain.windows[2] != want[2] {
			t.Fatalf("resumed chain windows = %v, want %v", chain.windows, want)
		}
		if got, ok := windowPersistedThroughTime(t, ctx, pool, taskID); !ok || !got.Equal(base.Add(4*time.Second)) {
			t.Fatalf("resumed pointer = %v (present %v), want the scope end", got, ok)
		}
	})
}
