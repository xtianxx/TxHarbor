// window.go implements T036: the time-scoped window resolver for 014
// reconciliation (FR-001 time kind, FR-003/017/019, Q3; data-model.md §1.1/§1.2).
//
// A time-scoped reconciliation task claims microsecond intervals. To observe
// chain facts for such an interval the claimed time range must be mapped to an
// inclusive block-height window whose block times bracket it. This file owns
// that mapping seam and its conservative semantics:
//
//   - The mapping is explicit: every resolution carries a status plus a
//     machine-readable reason and a bounded evidence reference.
//   - An unmappable range (no index bounds, time outside the indexed coverage,
//     a probe budget/limit exhausted, a height whose canonical identity cannot
//     be proven) never resolves. The caller commits an explicit gap row and the
//     chain party stays unknown: an incomplete mapping MUST NOT be read as
//     proof that no chain fact exists.
//   - A reorg-invalidated mapping (the live header hash at a mapped height no
//     longer matches the durable canonical identity) resolves to `invalidated`;
//     the caller keeps the observation pending (FR-017), never consistent.
//   - Mapping queries are charged to the scan budget seam as they execute
//     (header probes via ConsumeRPC, local identity reads via ConsumePG), so an
//     exhausted budget stops the resolution boundedly instead of silently
//     under-counting. No after-the-fact accounting exists.
//   - Read-only by construction: this file never writes, never opens a DB
//     transaction, never takes a lock, and never triggers recovery/replay/
//     payment (FR-014/020/023).
//
// The concrete reader (HeaderBlockTimeReader) reads chain times from the node
// header RPC and canonical block identity from the durable local index
// (chain_blocks). The local index stores no chain timestamps, so the header
// RPC is the only chain-time source; when it is unavailable the resolution is
// unmappable (gap), never guessed from the local insert clock.
package reconciliation

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xtianxx/txharbor/internal/indexer"
)

// WindowStatus is the closed outcome vocabulary of one time→height resolution.
type WindowStatus string

const (
	// WindowResolved: both endpoint heights were located and their canonical
	// block identity was verified.
	WindowResolved WindowStatus = "resolved"
	// WindowUnmappable: the range cannot be mapped right now (no local index
	// bounds, outside indexed coverage, probe limit, unprovable identity). The
	// caller records an explicit gap; the chain party stays unknown.
	WindowUnmappable WindowStatus = "unmappable"
	// WindowInvalidated: the mapping was possible but a mapped height's live
	// header contradicts the durable canonical identity (reorg). The caller
	// keeps the observation pending (FR-017); it never becomes consistent.
	WindowInvalidated WindowStatus = "invalidated"
)

// Valid reports whether s is one of the closed window statuses.
func (s WindowStatus) Valid() bool {
	switch s {
	case WindowResolved, WindowUnmappable, WindowInvalidated:
		return true
	}
	return false
}

// Stable machine reasons recorded on a non-resolved resolution. They are
// audit/evidence vocabulary and MUST NOT be reworded once published.
const (
	WindowReasonIndexUnavailable     = "time_window_index_unavailable"
	WindowReasonBeforeCoverage       = "time_window_precedes_indexed_coverage"
	WindowReasonAfterCoverage        = "time_window_beyond_indexed_coverage"
	WindowReasonNoBlockInRange       = "time_window_contains_no_indexed_block"
	WindowReasonProbeLimit           = "time_window_probe_limit"
	WindowReasonProbeFailed          = "time_window_probe_failed"
	WindowReasonIdentityNotCanonical = "time_window_identity_not_canonical"
	WindowReasonReorgInvalidated     = "time_window_reorg_invalidated"
)

// ChainWindowResolution is one window resolution: the inclusive height window
// for a resolved interval, or the refusal reason for an unmappable/invalidated
// one. From/To are only meaningful for WindowResolved.
type ChainWindowResolution struct {
	Status      WindowStatus
	From        int64
	To          int64
	Reason      string
	EvidenceRef string
}

// Validate checks the resolution shape conservatively.
func (r ChainWindowResolution) Validate() error {
	if !r.Status.Valid() {
		return contractErrorf("unknown time window status %q", r.Status)
	}
	switch r.Status {
	case WindowResolved:
		if r.From < 0 || r.To < r.From {
			return contractErrorf("resolved time window %d..%d is not an inclusive ascending height range", r.From, r.To)
		}
		if strings.TrimSpace(r.Reason) != "" {
			return contractErrorf("resolved time window must not carry a refusal reason %q", r.Reason)
		}
	case WindowUnmappable, WindowInvalidated:
		if strings.TrimSpace(r.Reason) == "" {
			return contractErrorf("non-resolved time window requires a machine reason")
		}
	}
	if len(r.EvidenceRef) > scanEvidenceRefMax {
		return contractErrorf("time window evidence ref exceeds %d bytes", scanEvidenceRefMax)
	}
	return nil
}

// ScanTimeWindowResolver is the T036 seam the compare loop consumes for
// time-scoped intervals. chainID is the task scope's numeric chain identity;
// the budget is charged per mapping query as it executes. Returning
// ErrBudgetExhausted aborts the scan boundedly (gap + honest checkpoint). A nil
// resolver leaves time scopes pending-by-design: never an absence claim.
type ScanTimeWindowResolver interface {
	ResolveScanWindow(ctx context.Context, chainID int64, interval ScanInterval, budget ScanQueryBudget) (ChainWindowResolution, error)
}

// BlockTimeReader is the read-only source of the resolver: chain times and
// canonical block identity. Implementations MUST NOT write or take locks.
type BlockTimeReader interface {
	// IndexedBounds returns the durable local height coverage [start, tip].
	// ok=false means no durable coverage exists for the chain.
	IndexedBounds(ctx context.Context, chainID int64) (start, tip int64, ok bool, err error)
	// ChainTimeAt returns the block header time (chain time, never the local
	// insert clock) and header hash at a height. ok=false means the height is
	// not available from the source.
	ChainTimeAt(ctx context.Context, chainID int64, height int64) (at time.Time, hash string, ok bool, err error)
	// CanonicalHashAt returns the durable canonical block hash at a height.
	// ok=false means the local index cannot prove a canonical identity there.
	CanonicalHashAt(ctx context.Context, chainID int64, height int64) (hash string, ok bool, err error)
}

// TimeHeightResolverConfig carries the resolver's bounded probe parameters.
// Every value is a deployment/test parameter: no production threshold is
// invented here, and unbounded is not representable.
type TimeHeightResolverConfig struct {
	// MaxProbes bounds header probes per resolution (positive). Exhausting it
	// is an unmappable resolution, never an unbounded search.
	MaxProbes int
	// RPCTimeout bounds each header probe (positive).
	RPCTimeout time.Duration
}

// Validate fails closed on non-positive bounds.
func (c TimeHeightResolverConfig) Validate() error {
	if c.MaxProbes <= 0 {
		return contractErrorf("time window resolver requires a positive max probe count")
	}
	if c.RPCTimeout <= 0 {
		return contractErrorf("time window resolver requires a positive RPC timeout")
	}
	return nil
}

// TimeHeightResolver maps time intervals to height windows over a
// BlockTimeReader. Concurrent use is safe: every resolution keeps its state
// (probe cache and counter) on the stack.
type TimeHeightResolver struct {
	reader BlockTimeReader
	cfg    TimeHeightResolverConfig
}

// NewTimeHeightResolver validates its dependencies and bounds. It performs no
// I/O.
func NewTimeHeightResolver(reader BlockTimeReader, cfg TimeHeightResolverConfig) (*TimeHeightResolver, error) {
	if reader == nil {
		return nil, contractErrorf("time window resolver requires a block-time reader")
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &TimeHeightResolver{reader: reader, cfg: cfg}, nil
}

// ResolveScanWindow implements ScanTimeWindowResolver. Height intervals are
// resolved locally without any query or charge; time intervals are mapped via
// the reader with every query charged to the budget as it executes.
func (r *TimeHeightResolver) ResolveScanWindow(ctx context.Context, chainID int64, interval ScanInterval, budget ScanQueryBudget) (ChainWindowResolution, error) {
	if r == nil || r.reader == nil {
		return ChainWindowResolution{}, contractErrorf("time window resolver is not wired")
	}
	if err := interval.Validate(); err != nil {
		return ChainWindowResolution{}, err
	}
	if budget == nil {
		return ChainWindowResolution{}, contractErrorf("time window resolution requires a query budget")
	}
	if interval.From.Kind == ScopeHeight {
		return ChainWindowResolution{
			Status: WindowResolved,
			From:   interval.From.Height,
			To:     interval.To.Height,
			Reason: "",
		}, nil
	}
	if interval.From.Kind != ScopeTime {
		return ChainWindowResolution{}, contractErrorf("time window resolver does not know scope kind %q", interval.From.Kind)
	}
	if chainID <= 0 {
		return ChainWindowResolution{}, contractErrorf("time window resolution requires a positive numeric chain id")
	}
	from, to := interval.From.Time, interval.To.Time

	if err := budget.ConsumePG(ctx, 1); err != nil {
		return ChainWindowResolution{}, err
	}
	start, tip, ok, err := r.reader.IndexedBounds(ctx, chainID)
	if err != nil {
		return ChainWindowResolution{}, fmt.Errorf("read indexed bounds: %w", err)
	}
	if !ok || start < 0 || tip < start {
		return r.unmappable(chainID, from, to, WindowReasonIndexUnavailable, 0), nil
	}

	search := newWindowSearch(r, chainID, budget)
	fromHeight, fromHash, found, reason, err := search.lowerBound(ctx, start, tip, from)
	if err != nil {
		return ChainWindowResolution{}, err
	}
	if !found {
		return r.unmappable(chainID, from, to, reason, search.used), nil
	}
	toHeight, toHash, found, reason, err := search.upperBound(ctx, start, tip, to)
	if err != nil {
		return ChainWindowResolution{}, err
	}
	if !found {
		return r.unmappable(chainID, from, to, reason, search.used), nil
	}
	if fromHeight > toHeight {
		return r.unmappable(chainID, from, to, WindowReasonNoBlockInRange, search.used), nil
	}

	// Canonical identity verification of both endpoints: the mapped heights
	// must still carry the canonical block the header probe observed.
	for _, endpoint := range []struct {
		height int64
		hash   string
	}{{fromHeight, fromHash}, {toHeight, toHash}} {
		if endpoint.hash == "" {
			continue
		}
		if err := budget.ConsumePG(ctx, 1); err != nil {
			return ChainWindowResolution{}, err
		}
		localHash, ok, err := r.reader.CanonicalHashAt(ctx, chainID, endpoint.height)
		if err != nil {
			return ChainWindowResolution{}, fmt.Errorf("read canonical identity at %d: %w", endpoint.height, err)
		}
		if !ok || strings.TrimSpace(localHash) == "" {
			return r.unmappable(chainID, from, to, WindowReasonIdentityNotCanonical, search.used), nil
		}
		if !strings.EqualFold(localHash, endpoint.hash) {
			// The live header at a mapped height contradicts the durable
			// canonical identity: the mapping is reorg-invalidated and may
			// only stay pending (FR-017).
			return ChainWindowResolution{
				Status: WindowInvalidated,
				Reason: WindowReasonReorgInvalidated,
				EvidenceRef: fmt.Sprintf("timewindow:v1 chain=%d status=invalidated height=%d header=%s canonical=%s",
					chainID, endpoint.height, strings.ToLower(endpoint.hash), strings.ToLower(localHash)),
			}, nil
		}
	}

	return ChainWindowResolution{
		Status: WindowResolved,
		From:   fromHeight,
		To:     toHeight,
		EvidenceRef: fmt.Sprintf("timewindow:v1 chain=%d heights=%d..%d probes=%d",
			chainID, fromHeight, toHeight, search.used),
	}, nil
}

// unmappable builds the conservative refusal resolution.
func (r *TimeHeightResolver) unmappable(chainID int64, from, to time.Time, reason string, probes int) ChainWindowResolution {
	return ChainWindowResolution{
		Status: WindowUnmappable,
		Reason: reason,
		EvidenceRef: fmt.Sprintf("timewindow:v1 chain=%d status=unmappable reason=%s probes=%d time=%s..%s",
			chainID, reason, probes, from.UTC().Format(time.RFC3339Nano), to.UTC().Format(time.RFC3339Nano)),
	}
}

// windowSearch is one resolution's bounded probe state: a probe cache (so the
// same height is never charged twice) and the max-probe bound.
type windowSearch struct {
	resolver *TimeHeightResolver
	chainID  int64
	budget   ScanQueryBudget
	cache    map[int64]windowProbe
	used     int
	max      int
}

type windowProbe struct {
	at     time.Time
	hash   string
	ok     bool
	reason string
}

// errWindowProbeLimit marks a resolution that hit its probe bound: it is an
// unmappable outcome, never a hard error.
var errWindowProbeLimit = contractErrorf("time window probe limit reached")

// newWindowSearch builds the per-resolution search state.
func newWindowSearch(r *TimeHeightResolver, chainID int64, budget ScanQueryBudget) *windowSearch {
	return &windowSearch{resolver: r, chainID: chainID, budget: budget,
		cache: make(map[int64]windowProbe), max: r.cfg.MaxProbes}
}

// probe reads one height's chain time/hash, charging one RPC per new height.
func (s *windowSearch) probe(ctx context.Context, height int64) (windowProbe, error) {
	if cached, ok := s.cache[height]; ok {
		return cached, nil
	}
	if s.used >= s.max {
		return windowProbe{}, errWindowProbeLimit
	}
	if err := s.budget.ConsumeRPC(ctx, 1); err != nil {
		return windowProbe{}, err
	}
	s.used++
	probeCtx, cancel := context.WithTimeout(ctx, s.resolver.cfg.RPCTimeout)
	defer cancel()
	at, hash, ok, err := s.resolver.reader.ChainTimeAt(probeCtx, s.chainID, height)
	if err != nil {
		return windowProbe{}, fmt.Errorf("chain time probe at %d: %w", height, err)
	}
	result := windowProbe{at: at.UTC(), hash: strings.ToLower(strings.TrimSpace(hash)), ok: ok}
	if !ok {
		result.reason = WindowReasonProbeFailed
	}
	s.cache[height] = result
	return result, nil
}

// lowerBound returns the lowest height in [lo, hi] whose chain time is >= at,
// plus the header hash observed at that height. Times are non-decreasing in
// height on a canonical chain; the search is O(log(hi-lo)) and bounded by the
// probe limit.
func (s *windowSearch) lowerBound(ctx context.Context, lo, hi int64, at time.Time) (int64, string, bool, string, error) {
	foundHeight, foundHash := int64(-1), ""
	for lo <= hi {
		mid := lo + (hi-lo)/2
		probe, err := s.probe(ctx, mid)
		if err != nil {
			if err == errWindowProbeLimit {
				return 0, "", false, WindowReasonProbeLimit, nil
			}
			return 0, "", false, "", err
		}
		if !probe.ok {
			return 0, "", false, WindowReasonProbeFailed, nil
		}
		if !probe.at.Before(at) {
			foundHeight, foundHash = mid, probe.hash
			hi = mid - 1
			continue
		}
		lo = mid + 1
	}
	if foundHeight < 0 {
		// Every indexed block is older than the requested start.
		return 0, "", false, WindowReasonAfterCoverage, nil
	}
	return foundHeight, foundHash, true, "", nil
}

// upperBound returns the highest height in [lo, hi] whose chain time is <= to,
// plus the header hash observed at that height.
func (s *windowSearch) upperBound(ctx context.Context, lo, hi int64, to time.Time) (int64, string, bool, string, error) {
	foundHeight, foundHash := int64(-1), ""
	for lo <= hi {
		mid := lo + (hi-lo)/2
		probe, err := s.probe(ctx, mid)
		if err != nil {
			if err == errWindowProbeLimit {
				return 0, "", false, WindowReasonProbeLimit, nil
			}
			return 0, "", false, "", err
		}
		if !probe.ok {
			return 0, "", false, WindowReasonProbeFailed, nil
		}
		if !probe.at.After(to) {
			foundHeight, foundHash = mid, probe.hash
			lo = mid + 1
			continue
		}
		hi = mid - 1
	}
	if foundHeight < 0 {
		// Every indexed block is newer than the requested end.
		return 0, "", false, WindowReasonBeforeCoverage, nil
	}
	return foundHeight, foundHash, true, "", nil
}

// windowQueryer is the read-only pgx surface the durable reader needs;
// *pgxpool.Pool satisfies it. The reader never opens a transaction.
type windowQueryer interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// HeaderBlockTimeReader is the production BlockTimeReader: chain times come
// from the bounded header RPC, canonical identity from the durable local
// index. It never writes and never holds a DB transaction across the RPC.
type HeaderBlockTimeReader struct {
	pool    windowQueryer
	header  indexer.HeaderClient
	timeout time.Duration
}

// NewHeaderBlockTimeReader validates its dependencies. timeout bounds each
// header probe (the resolver also enforces its own RPCTimeout; the smaller
// wins).
func NewHeaderBlockTimeReader(pool windowQueryer, header indexer.HeaderClient, timeout time.Duration) (*HeaderBlockTimeReader, error) {
	if pool == nil {
		return nil, contractErrorf("block-time reader requires a database pool")
	}
	if header == nil {
		return nil, contractErrorf("block-time reader requires a header client")
	}
	if timeout <= 0 {
		return nil, contractErrorf("block-time reader requires a positive timeout")
	}
	return &HeaderBlockTimeReader{pool: pool, header: header, timeout: timeout}, nil
}

// IndexedBounds reads the durable local coverage of the chain.
func (r *HeaderBlockTimeReader) IndexedBounds(ctx context.Context, chainID int64) (int64, int64, bool, error) {
	var start, tip int64
	err := r.pool.QueryRow(ctx, windowIndexedBoundsSQL, chainID).Scan(&start, &tip)
	switch {
	case err == pgx.ErrNoRows:
		return 0, 0, false, nil
	case err != nil:
		return 0, 0, false, fmt.Errorf("read indexed bounds for chain %d: %w", chainID, err)
	}
	return start, tip, true, nil
}

// ChainTimeAt reads one block header time and hash from the node RPC.
func (r *HeaderBlockTimeReader) ChainTimeAt(ctx context.Context, chainID int64, height int64) (time.Time, string, bool, error) {
	probeCtx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	header, err := r.header.HeaderByNumber(probeCtx, big.NewInt(height))
	if err != nil {
		return time.Time{}, "", false, fmt.Errorf("header at %d: %w", height, err)
	}
	if header == nil || header.Number == nil || header.Number.Int64() != height {
		return time.Time{}, "", false, nil
	}
	return time.Unix(int64(header.Time), 0).UTC(), strings.ToLower(header.Hash().Hex()), true, nil
}

// CanonicalHashAt reads the durable canonical block hash at a height.
func (r *HeaderBlockTimeReader) CanonicalHashAt(ctx context.Context, chainID int64, height int64) (string, bool, error) {
	var hash string
	err := r.pool.QueryRow(ctx, windowCanonicalHashSQL, chainID, height).Scan(&hash)
	switch {
	case err == pgx.ErrNoRows:
		return "", false, nil
	case err != nil:
		return "", false, fmt.Errorf("read canonical hash for chain %d height %d: %w", chainID, height, err)
	}
	return strings.ToLower(strings.TrimSpace(hash)), true, nil
}

// Read-only SQL. Column names follow migrations 000002; no statement writes,
// locks or advances anything.
const (
	windowIndexedBoundsSQL = `
SELECT start_height, height
FROM indexer_checkpoint
WHERE chain_id = $1`

	windowCanonicalHashSQL = `
SELECT hash
FROM chain_blocks
WHERE chain_id = $1 AND number = $2 AND canonical
LIMIT 1`
)
