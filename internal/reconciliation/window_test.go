// window_test.go is the T036 unit layer for the time→height window resolver
// (FR-001 time kind, FR-003/017/019, Q3; data-model.md §1.1/§1.2):
//
//   - height intervals resolve locally with zero budget charges;
//   - time intervals map to inclusive height windows with explicit boundary
//     semantics (exact endpoint, between-block narrowing, single instant);
//   - every unmappable shape (no index, outside coverage, probe limit, probe
//     failure, unprovable canonical identity) refuses to resolve and carries a
//     stable machine reason — an incomplete mapping is never an absence claim;
//   - a reorg-invalidated mapping resolves to `invalidated` and may only stay
//     pending;
//   - mapping queries are charged to the budget as they execute and an
//     exhausted budget aborts with ErrBudgetExhausted (never a silent partial
//     answer), with the probe count bounded by the configured maximum;
//   - scanChainWindow keeps the compare-loop seam honest: no resolver means the
//     chain party stays unknown, and an unmappable window records an explicit
//     gap.
//
// No database and no Docker: the reader is an in-memory fake.
package reconciliation

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// windowTestBudget is a charge-counting ScanQueryBudget that refuses past its
// configured limits, mirroring the production Budget's fail-closed semantics.
type windowTestBudget struct {
	pgLimit, rpcLimit int
	pg, rpc           int
}

func (b *windowTestBudget) ConsumePG(ctx context.Context, requests int) error {
	if requests < 0 || b.pg+requests > b.pgLimit {
		return fmt.Errorf("%w: pg %d+%d > %d", ErrBudgetExhausted, b.pg, requests, b.pgLimit)
	}
	b.pg += requests
	return nil
}

func (b *windowTestBudget) ConsumeRPC(ctx context.Context, requests int) error {
	if requests < 0 || b.rpc+requests > b.rpcLimit {
		return fmt.Errorf("%w: rpc %d+%d > %d", ErrBudgetExhausted, b.rpc, requests, b.rpcLimit)
	}
	b.rpc += requests
	return nil
}

// windowTestHash is one deterministic canonical (and header) hash per height.
func windowTestHash(height int64) string {
	return fmt.Sprintf("0x%064x", 0xaa0000+height)
}

// windowFakeReader is a deterministic in-memory BlockTimeReader: heights
// 100..104 carry one block per second, headers and canonical identity match
// unless a test overrides them.
type windowFakeReader struct {
	boundsOK   bool
	start, tip int64

	times     map[int64]time.Time
	headers   map[int64]string
	canonical map[int64]string
	failAt    map[int64]error

	reads []int64
}

func newWindowFakeReader(base time.Time) *windowFakeReader {
	r := &windowFakeReader{
		boundsOK:  true,
		start:     100,
		tip:       104,
		times:     map[int64]time.Time{},
		headers:   map[int64]string{},
		canonical: map[int64]string{},
		failAt:    map[int64]error{},
	}
	for height := int64(100); height <= 104; height++ {
		r.times[height] = base.Add(time.Duration(height-100) * time.Second)
		r.headers[height] = windowTestHash(height)
		r.canonical[height] = windowTestHash(height)
	}
	return r
}

func (r *windowFakeReader) IndexedBounds(ctx context.Context, chainID int64) (int64, int64, bool, error) {
	if !r.boundsOK {
		return 0, 0, false, nil
	}
	return r.start, r.tip, true, nil
}

func (r *windowFakeReader) ChainTimeAt(ctx context.Context, chainID int64, height int64) (time.Time, string, bool, error) {
	r.reads = append(r.reads, height)
	if err := r.failAt[height]; err != nil {
		return time.Time{}, "", false, err
	}
	at, ok := r.times[height]
	if !ok {
		return time.Time{}, "", false, nil
	}
	return at, r.headers[height], true, nil
}

func (r *windowFakeReader) CanonicalHashAt(ctx context.Context, chainID int64, height int64) (string, bool, error) {
	hash, ok := r.canonical[height]
	return hash, ok, nil
}

// windowTestResolver builds a resolver over the fake reader.
func windowTestResolver(t *testing.T, reader BlockTimeReader, maxProbes int) *TimeHeightResolver {
	t.Helper()
	resolver, err := NewTimeHeightResolver(reader, TimeHeightResolverConfig{
		MaxProbes:  maxProbes,
		RPCTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("NewTimeHeightResolver: %v", err)
	}
	return resolver
}

func windowTimeInterval(from, to time.Time) ScanInterval {
	return ScanInterval{From: TimeBound(from), To: TimeBound(to)}
}

func TestTimeHeightResolverHeightScopeResolvesLocally(t *testing.T) {
	reader := newWindowFakeReader(time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC))
	reader.boundsOK = false // a height scope must not even need the index
	resolver := windowTestResolver(t, reader, 8)
	budget := &windowTestBudget{pgLimit: 0, rpcLimit: 0}

	resolution, err := resolver.ResolveScanWindow(context.Background(), 7,
		ScanInterval{From: HeightBound(100), To: HeightBound(103)}, budget)
	if err != nil {
		t.Fatalf("ResolveScanWindow(height): %v", err)
	}
	if resolution.Status != WindowResolved || resolution.From != 100 || resolution.To != 103 {
		t.Fatalf("resolution = %+v, want resolved 100..103", resolution)
	}
	if budget.pg != 0 || budget.rpc != 0 || len(reader.reads) != 0 {
		t.Fatalf("height resolution spent budget/rpc: pg=%d rpc=%d reads=%v", budget.pg, budget.rpc, reader.reads)
	}
}

func TestTimeHeightResolverBoundaryMapping(t *testing.T) {
	base := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name     string
		from, to time.Time
		status   WindowStatus
		reason   string
		wantFrom int64
		wantTo   int64
	}{
		{"exact_span", base, base.Add(4 * time.Second), WindowResolved, "", 100, 104},
		{"exact_single_instant", base.Add(3 * time.Second), base.Add(3 * time.Second), WindowResolved, "", 103, 103},
		{"between_blocks_narrows", base.Add(500 * time.Millisecond), base.Add(2500 * time.Millisecond), WindowResolved, "", 101, 102},
		{"start_between_end_exact", base.Add(1500 * time.Millisecond), base.Add(4 * time.Second), WindowResolved, "", 102, 104},
		{"before_indexed_coverage", base.Add(-time.Second), base.Add(-time.Second), WindowUnmappable, WindowReasonBeforeCoverage, 0, 0},
		{"after_indexed_coverage", base.Add(time.Hour), base.Add(time.Hour), WindowUnmappable, WindowReasonAfterCoverage, 0, 0},
		{"no_block_in_range", base.Add(500 * time.Millisecond), base.Add(600 * time.Millisecond), WindowUnmappable, WindowReasonNoBlockInRange, 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reader := newWindowFakeReader(base)
			resolver := windowTestResolver(t, reader, 8)
			resolution, err := resolver.ResolveScanWindow(context.Background(), 7, windowTimeInterval(tc.from, tc.to),
				&windowTestBudget{pgLimit: 64, rpcLimit: 64})
			if err != nil {
				t.Fatalf("ResolveScanWindow: %v", err)
			}
			if err := resolution.Validate(); err != nil {
				t.Fatalf("resolution invalid: %v", err)
			}
			if resolution.Status != tc.status || resolution.Reason != tc.reason {
				t.Fatalf("resolution = %s/%s, want %s/%s", resolution.Status, resolution.Reason, tc.status, tc.reason)
			}
			if tc.status == WindowResolved && (resolution.From != tc.wantFrom || resolution.To != tc.wantTo) {
				t.Fatalf("resolved %d..%d, want %d..%d", resolution.From, resolution.To, tc.wantFrom, tc.wantTo)
			}
		})
	}
}

func TestTimeHeightResolverUnmappableShapes(t *testing.T) {
	base := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	full := windowTimeInterval(base, base.Add(4*time.Second))

	t.Run("index_unavailable", func(t *testing.T) {
		reader := newWindowFakeReader(base)
		reader.boundsOK = false
		resolution, err := windowTestResolver(t, reader, 8).ResolveScanWindow(context.Background(), 7, full,
			&windowTestBudget{pgLimit: 8, rpcLimit: 8})
		if err != nil {
			t.Fatalf("ResolveScanWindow: %v", err)
		}
		if resolution.Status != WindowUnmappable || resolution.Reason != WindowReasonIndexUnavailable {
			t.Fatalf("resolution = %s/%s, want unmappable/%s", resolution.Status, resolution.Reason, WindowReasonIndexUnavailable)
		}
		if len(reader.reads) != 0 {
			t.Fatalf("index-unavailable resolution probed headers: %v", reader.reads)
		}
	})

	t.Run("probe_failed", func(t *testing.T) {
		reader := newWindowFakeReader(base)
		delete(reader.times, 102)
		resolution, err := windowTestResolver(t, reader, 8).ResolveScanWindow(context.Background(), 7, full,
			&windowTestBudget{pgLimit: 8, rpcLimit: 8})
		if err != nil {
			t.Fatalf("ResolveScanWindow: %v", err)
		}
		if resolution.Status != WindowUnmappable || resolution.Reason != WindowReasonProbeFailed {
			t.Fatalf("resolution = %s/%s, want unmappable/%s", resolution.Status, resolution.Reason, WindowReasonProbeFailed)
		}
	})

	t.Run("identity_not_canonical", func(t *testing.T) {
		reader := newWindowFakeReader(base)
		delete(reader.canonical, 100)
		resolution, err := windowTestResolver(t, reader, 8).ResolveScanWindow(context.Background(), 7, full,
			&windowTestBudget{pgLimit: 8, rpcLimit: 8})
		if err != nil {
			t.Fatalf("ResolveScanWindow: %v", err)
		}
		if resolution.Status != WindowUnmappable || resolution.Reason != WindowReasonIdentityNotCanonical {
			t.Fatalf("resolution = %s/%s, want unmappable/%s", resolution.Status, resolution.Reason, WindowReasonIdentityNotCanonical)
		}
	})

	t.Run("probe_limit_is_bounded", func(t *testing.T) {
		reader := newWindowFakeReader(base)
		resolver := windowTestResolver(t, reader, 1)
		from := base.Add(25 * time.Second)
		resolution, err := resolver.ResolveScanWindow(context.Background(), 7, windowTimeInterval(from, from),
			&windowTestBudget{pgLimit: 8, rpcLimit: 8})
		if err != nil {
			t.Fatalf("ResolveScanWindow: %v", err)
		}
		if resolution.Status != WindowUnmappable || resolution.Reason != WindowReasonProbeLimit {
			t.Fatalf("resolution = %s/%s, want unmappable/%s", resolution.Status, resolution.Reason, WindowReasonProbeLimit)
		}
		if len(reader.reads) > 1 {
			t.Fatalf("probe bound 1 issued %d header reads: %v", len(reader.reads), reader.reads)
		}
	})
}

func TestTimeHeightResolverReorgInvalidated(t *testing.T) {
	base := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	reader := newWindowFakeReader(base)
	reader.headers[101] = "0x" + strings.Repeat("ee", 32)
	resolver := windowTestResolver(t, reader, 8)
	budget := &windowTestBudget{pgLimit: 8, rpcLimit: 8}

	instant := windowTimeInterval(base.Add(time.Second), base.Add(time.Second))
	resolution, err := resolver.ResolveScanWindow(context.Background(), 7, instant, budget)
	if err != nil {
		t.Fatalf("ResolveScanWindow: %v", err)
	}
	if resolution.Status != WindowInvalidated || resolution.Reason != WindowReasonReorgInvalidated {
		t.Fatalf("resolution = %s/%s, want invalidated/%s", resolution.Status, resolution.Reason, WindowReasonReorgInvalidated)
	}
	if resolution.From != 0 || resolution.To != 0 {
		t.Fatalf("invalidated resolution carries heights %d..%d, want none", resolution.From, resolution.To)
	}
	if !strings.Contains(resolution.EvidenceRef, "height=101") {
		t.Fatalf("invalidated evidence ref %q does not name the contradicting height", resolution.EvidenceRef)
	}
	if err := resolution.Validate(); err != nil {
		t.Fatalf("invalidated resolution invalid: %v", err)
	}
}

func TestTimeHeightResolverBudgetChargesAsItExecutes(t *testing.T) {
	base := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	full := windowTimeInterval(base, base.Add(4*time.Second))

	t.Run("charges_counted", func(t *testing.T) {
		reader := newWindowFakeReader(base)
		resolver := windowTestResolver(t, reader, 8)
		budget := &windowTestBudget{pgLimit: 64, rpcLimit: 64}
		resolution, err := resolver.ResolveScanWindow(context.Background(), 7, full, budget)
		if err != nil || resolution.Status != WindowResolved {
			t.Fatalf("ResolveScanWindow = %+v (err %v), want resolved", resolution, err)
		}
		// One PG charge for the indexed bounds plus one canonical-identity
		// read per resolved endpoint; every header probe is one RPC charge.
		if budget.pg != 3 {
			t.Fatalf("PG charges = %d, want 3 (bounds + two canonical endpoints)", budget.pg)
		}
		if budget.rpc == 0 || budget.rpc != len(reader.reads) {
			t.Fatalf("RPC charges = %d, probe reads = %d, want equal and positive", budget.rpc, len(reader.reads))
		}
	})

	t.Run("exhausted_budget_aborts", func(t *testing.T) {
		reader := newWindowFakeReader(base)
		resolver := windowTestResolver(t, reader, 8)
		budget := &windowTestBudget{pgLimit: 1, rpcLimit: 64}
		_, err := resolver.ResolveScanWindow(context.Background(), 7, full, budget)
		if !errors.Is(err, ErrBudgetExhausted) {
			t.Fatalf("exhausted resolution err = %v, want ErrBudgetExhausted", err)
		}
	})

	t.Run("nil_budget_refused", func(t *testing.T) {
		reader := newWindowFakeReader(base)
		resolver := windowTestResolver(t, reader, 8)
		_, err := resolver.ResolveScanWindow(context.Background(), 7, full, nil)
		if err == nil || !errors.Is(err, ErrContract) {
			t.Fatalf("nil budget err = %v, want ErrContract", err)
		}
	})
}

func TestTimeHeightResolverConfigAndResolutionValidation(t *testing.T) {
	reader := newWindowFakeReader(time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC))
	if _, err := NewTimeHeightResolver(nil, TimeHeightResolverConfig{MaxProbes: 1, RPCTimeout: time.Second}); err == nil {
		t.Fatalf("nil reader accepted")
	}
	for _, cfg := range []TimeHeightResolverConfig{
		{MaxProbes: 0, RPCTimeout: time.Second},
		{MaxProbes: -1, RPCTimeout: time.Second},
		{MaxProbes: 1, RPCTimeout: 0},
	} {
		if _, err := NewTimeHeightResolver(reader, cfg); err == nil {
			t.Fatalf("invalid resolver config %+v accepted", cfg)
		}
	}

	for _, resolution := range []ChainWindowResolution{
		{Status: WindowStatus("weird"), Reason: "x"},
		{Status: WindowResolved, From: 3, To: 2},
		{Status: WindowResolved, From: 1, To: 2, Reason: "should-not-be-here"},
		{Status: WindowUnmappable},
		{Status: WindowInvalidated},
		{Status: WindowResolved, From: 1, To: 2, EvidenceRef: strings.Repeat("x", scanEvidenceRefMax+1)},
	} {
		if err := resolution.Validate(); err == nil {
			t.Fatalf("invalid resolution %+v accepted", resolution)
		}
	}
	if err := (ChainWindowResolution{Status: WindowResolved, From: 5, To: 5}).Validate(); err != nil {
		t.Fatalf("valid resolution refused: %v", err)
	}
}

// windowStubChainFacts satisfies the compare loop's chain surface; the seam
// tests below never let it be read.
type windowStubChainFacts struct{}

func (windowStubChainFacts) Observe(ctx context.Context, q ChainFactsQuery) (ChainFactsBundle, error) {
	return ChainFactsBundle{ChainID: q.ChainID, From: q.From, To: q.To, Status: ChainFactsComplete}, nil
}

// windowRecordingResolver records the intervals it was asked to resolve.
type windowRecordingResolver struct {
	resolution ChainWindowResolution
	err        error
	calls      int
	intervals  []ScanInterval
}

func (r *windowRecordingResolver) ResolveScanWindow(ctx context.Context, chainID int64,
	interval ScanInterval, budget ScanQueryBudget) (ChainWindowResolution, error) {
	r.calls++
	r.intervals = append(r.intervals, interval)
	return r.resolution, r.err
}

// windowLegacyResolver is a candidate source that also carries the legacy
// ScanChainWindowResolver seam.
type windowLegacyResolver struct {
	from, to int64
	resolved bool
}

func (windowLegacyResolver) ScanCandidates(ctx context.Context, interval ScanInterval) ([]ScanCandidate, error) {
	return nil, nil
}

func (l windowLegacyResolver) ResolveChainWindow(ctx context.Context, interval ScanInterval) (int64, int64, bool, error) {
	return l.from, l.to, l.resolved, nil
}

func windowTestTask(kind ScopeKind, chainID string) *Task {
	base := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	task := &Task{
		TaskID: "task-1", ScopeChainID: chainID, ScopeKind: kind,
		ScopeStart: HeightBound(100), ScopeEnd: HeightBound(103),
		BusinessTypes: []BusinessType{BusinessWithdrawal},
	}
	if kind == ScopeTime {
		task.ScopeStart = TimeBound(base)
		task.ScopeEnd = TimeBound(base.Add(4 * time.Second))
	}
	return task
}

func windowSeamBudget(t *testing.T) *Budget {
	t.Helper()
	budget, err := NewBudget(BudgetLimits{
		MaxConcurrency: 1, MaxSpanPerClaim: 4, MaxDuration: time.Minute,
		MaxPGRequests: 10, MaxRPCRequests: 10,
	})
	if err != nil {
		t.Fatalf("NewBudget: %v", err)
	}
	return budget
}

func TestScanChainWindowSeams(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)

	t.Run("height_scope_resolves_locally", func(t *testing.T) {
		window, err := scanChainWindow(ctx, &ScanOnceRequest{Sources: ScanSources{Chain: windowStubChainFacts{}}},
			windowTestTask(ScopeHeight, "7"),
			ScanInterval{From: HeightBound(100), To: HeightBound(103)}, windowSeamBudget(t))
		if err != nil {
			t.Fatalf("scanChainWindow: %v", err)
		}
		if !window.resolved || window.from != 100 || window.to != 103 || window.gap || window.status != "" {
			t.Fatalf("window = %+v, want a local 100..103 resolution", window)
		}
	})

	t.Run("nil_chain_reader_keeps_the_party_unknown", func(t *testing.T) {
		window, err := scanChainWindow(ctx, &ScanOnceRequest{}, windowTestTask(ScopeHeight, "7"),
			ScanInterval{From: HeightBound(100), To: HeightBound(103)}, windowSeamBudget(t))
		if err != nil || window.resolved {
			t.Fatalf("window = %+v (err %v), want unresolved", window, err)
		}
	})

	t.Run("time_scope_without_resolver_stays_unresolved", func(t *testing.T) {
		budget := windowSeamBudget(t)
		window, err := scanChainWindow(ctx, &ScanOnceRequest{Sources: ScanSources{Chain: windowStubChainFacts{}}},
			windowTestTask(ScopeTime, "7"),
			windowTimeInterval(base, base.Add(4*time.Second)), budget)
		if err != nil {
			t.Fatalf("scanChainWindow: %v", err)
		}
		if window.resolved || window.gap || window.status != "" {
			t.Fatalf("window = %+v, want an unmapped pending-by-design outcome", window)
		}
		if usage := budget.Usage(); usage.PGUsed != 0 || usage.RPCUsed != 0 {
			t.Fatalf("unresolved time scope spent budget: %+v", usage)
		}
	})

	t.Run("symbolic_chain_id_never_reaches_the_resolver", func(t *testing.T) {
		resolver := &windowRecordingResolver{
			resolution: ChainWindowResolution{Status: WindowResolved, From: 100, To: 103},
		}
		window, err := scanChainWindow(ctx, &ScanOnceRequest{
			Sources:        ScanSources{Chain: windowStubChainFacts{}},
			WindowResolver: resolver,
		}, windowTestTask(ScopeTime, "not-numeric"), windowTimeInterval(base, base.Add(4*time.Second)), windowSeamBudget(t))
		if err != nil {
			t.Fatalf("scanChainWindow: %v", err)
		}
		if window.resolved || resolver.calls != 0 {
			t.Fatalf("symbolic chain resolved = %+v, resolver calls = %d, want none", window, resolver.calls)
		}
	})

	t.Run("unmappable_is_an_explicit_gap", func(t *testing.T) {
		resolver := &windowRecordingResolver{
			resolution: ChainWindowResolution{Status: WindowUnmappable, Reason: WindowReasonIndexUnavailable},
		}
		window, err := scanChainWindow(ctx, &ScanOnceRequest{
			Sources:        ScanSources{Chain: windowStubChainFacts{}},
			WindowResolver: resolver,
		}, windowTestTask(ScopeTime, "7"), windowTimeInterval(base, base.Add(4*time.Second)), windowSeamBudget(t))
		if err != nil {
			t.Fatalf("scanChainWindow: %v", err)
		}
		if window.resolved || !window.gap || window.status != WindowUnmappable || window.reason != WindowReasonIndexUnavailable {
			t.Fatalf("window = %+v, want an explicit unmappable gap", window)
		}
	})

	t.Run("invalidated_stays_pending_without_a_gap_claim", func(t *testing.T) {
		resolver := &windowRecordingResolver{
			resolution: ChainWindowResolution{Status: WindowInvalidated, Reason: WindowReasonReorgInvalidated},
		}
		window, err := scanChainWindow(ctx, &ScanOnceRequest{
			Sources:        ScanSources{Chain: windowStubChainFacts{}},
			WindowResolver: resolver,
		}, windowTestTask(ScopeTime, "7"), windowTimeInterval(base, base.Add(4*time.Second)), windowSeamBudget(t))
		if err != nil {
			t.Fatalf("scanChainWindow: %v", err)
		}
		if window.resolved || window.gap || window.status != WindowInvalidated {
			t.Fatalf("window = %+v, want invalidated without a gap", window)
		}
	})

	t.Run("malformed_resolution_is_refused", func(t *testing.T) {
		resolver := &windowRecordingResolver{
			resolution: ChainWindowResolution{Status: WindowResolved, From: 100, To: 103, Reason: "nope"},
		}
		if _, err := scanChainWindow(ctx, &ScanOnceRequest{
			Sources:        ScanSources{Chain: windowStubChainFacts{}},
			WindowResolver: resolver,
		}, windowTestTask(ScopeTime, "7"), windowTimeInterval(base, base.Add(4*time.Second)), windowSeamBudget(t)); err == nil {
			t.Fatalf("malformed resolver resolution was accepted")
		}
	})

	t.Run("legacy_resolver_seam_is_honored", func(t *testing.T) {
		legacy := windowLegacyResolver{from: 100, to: 103, resolved: true}
		window, err := scanChainWindow(ctx, &ScanOnceRequest{
			Sources:    ScanSources{Chain: windowStubChainFacts{}},
			Candidates: legacy,
		}, windowTestTask(ScopeTime, "7"), windowTimeInterval(base, base.Add(4*time.Second)), windowSeamBudget(t))
		if err != nil {
			t.Fatalf("scanChainWindow: %v", err)
		}
		if !window.resolved || window.from != 100 || window.to != 103 {
			t.Fatalf("window = %+v, want the legacy resolution", window)
		}
	})
}
