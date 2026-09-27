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
//
// The same seam serves the reverse direction for height-scoped tasks
// (ResolveHeightTimeWindow): a claimed height interval maps to the inclusive
// chain-time window of its endpoint heights. The boundary heights are exact
// (the endpoint blocks themselves) and the window is closed at the next
// height's chain time so adjacent intervals tile without a seam; the chain
// times carry the header's own second precision (time.Unix), and no
// sub-second precision, local clock or index insert time is ever claimed. The
// endpoint canonical identity keeps reorg-contradicted mappings pending
// (invalidated), unprovable ones gap, and an unclosable right seam stays an
// explicit gap declaration (BoundaryUnproven).
package reconciliation

import (
	"context"
	"errors"
	"fmt"
	"math"
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
	// WindowReasonTimeOrder: the two endpoint heights were readable but their
	// chain times are not ascending, so no inclusive chain-time window exists.
	// The resolution stays unmappable; a negative/zero-length window is never
	// silently normalized into coverage.
	WindowReasonTimeOrder = "height_window_time_not_ascending"
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

// ChainTimeResolution is one height→chain-time resolution: the inclusive
// chain-time window [From, To] of a claimed height interval, or the refusal
// reason for an unmappable/invalidated resolution. From/To are only meaningful
// for WindowResolved. The window is the event-query OccurredFrom/To of the
// interval: both endpoint heights' chain times are probed and their canonical
// block identity is verified. The boundary heights are exact and inclusive,
// and To is closed at the next height's chain time so adjacent claimed
// intervals tile the time axis without a seam (BoundaryUnproven records a
// right seam that could not be closed). The chain times themselves are the
// header's second-precision timestamps (time.Unix); no sub-second precision,
// local clock or index insert time is ever claimed.
type ChainTimeResolution struct {
	Status WindowStatus
	From   time.Time
	To     time.Time

	// Reason is the machine refusal token of a non-resolved resolution, or of
	// a resolved window whose right seam could not be closed
	// (BoundaryUnproven=true).
	Reason string
	// BoundaryUnproven reports that the window is resolved but its right
	// boundary could not be extended to the next height's chain time: the next
	// height is beyond the locally provable coverage/tip, its canonical
	// identity is unprovable or reorg-contradicted, or its chain time is not
	// ascending. The window is still the covered span's window; the caller
	// records an explicit gap so the seam to any following interval can never
	// close silently. Reason carries one of the stable window reasons.
	BoundaryUnproven bool
	EvidenceRef      string
}

// Validate checks the resolution shape conservatively. Resolved requires a
// non-zero ascending inclusive window; a resolved window carries either no
// reason or, when BoundaryUnproven is set, a machine reason for the unclosed
// right seam. Every non-resolved status requires a machine reason.
func (r ChainTimeResolution) Validate() error {
	if !r.Status.Valid() {
		return contractErrorf("unknown height time window status %q", r.Status)
	}
	switch r.Status {
	case WindowResolved:
		if r.From.IsZero() || r.To.IsZero() {
			return contractErrorf("resolved height time window requires two non-zero chain times")
		}
		if r.To.Before(r.From) {
			return contractErrorf("resolved height time window is not an inclusive ascending range")
		}
		if r.BoundaryUnproven {
			if strings.TrimSpace(r.Reason) == "" {
				return contractErrorf("resolved height time window with an unproven right boundary requires a machine reason")
			}
		} else if strings.TrimSpace(r.Reason) != "" {
			return contractErrorf("resolved height time window must not carry a refusal reason %q", r.Reason)
		}
	case WindowUnmappable, WindowInvalidated:
		if strings.TrimSpace(r.Reason) == "" {
			return contractErrorf("non-resolved height time window requires a machine reason")
		}
	}
	if len(r.EvidenceRef) > scanEvidenceRefMax {
		return contractErrorf("height time window evidence ref exceeds %d bytes", scanEvidenceRefMax)
	}
	return nil
}

// ScanHeightTimeWindowResolver is the height→chain-time seam the compare loop
// consumes for height-scoped intervals so the event adapter's OccurredFrom/To
// window is resolved from chain block times (header second precision, endpoint
// heights exact) instead of guessed. Every mapping query is charged to the
// budget as it executes, exactly like the T036 time→height resolver. A nil
// resolver leaves height scopes pending-by-design: the event adapter then
// refuses to cover blockless business-object rows, which is never read as an
// absence claim.
type ScanHeightTimeWindowResolver interface {
	ResolveHeightTimeWindow(ctx context.Context, chainID int64, interval ScanInterval, budget ScanQueryBudget) (ChainTimeResolution, error)
}

// BlockTimeReader is the read-only source of the resolver: chain times and
// canonical block identity. Implementations MUST NOT write or take locks.
type BlockTimeReader interface {
	// IndexedBounds returns the durable local height coverage [start, tip].
	// ok=false means no durable coverage exists for the chain.
	IndexedBounds(ctx context.Context, chainID int64) (start, tip int64, ok bool, err error)
	// ChainTimeAt returns the block header time (chain time, never the local
	// insert clock) and header hash at a height. The time carries the header's
	// own second precision (time.Unix); ok=false means the height is not
	// available from the source.
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

// ResolveHeightTimeWindow implements ScanHeightTimeWindowResolver: it maps a
// claimed height interval to the inclusive chain-time window of its two
// endpoint heights, closed at the next height's chain time so adjacent claimed
// intervals tile the time axis without a seam. The boundary heights are exact
// and the times are the header's second-precision chain timestamps; no
// sub-second precision, local clock or index insert time is ever claimed.
// Both endpoints are header-probed (bounded by MaxProbes and charged as RPC)
// and their canonical block identity is verified against the durable index
// (charged as PG): a contradicted identity is `invalidated` (pending, FR-017)
// and an unprovable one is `unmappable` (explicit gap). Probe read failures on
// an endpoint are evidence-unavailable, not hard errors: they resolve
// `unmappable` so the caller records an explicit gap instead of stalling the
// scan; only budget exhaustion and cancellation/deadline on the caller's
// context abort. When the right seam cannot be closed (the next height's chain
// time is unprovable), the window falls back to the covered endpoint and
// BoundaryUnproven declares the unclosed seam for an explicit caller gap.
func (r *TimeHeightResolver) ResolveHeightTimeWindow(ctx context.Context, chainID int64,
	interval ScanInterval, budget ScanQueryBudget) (ChainTimeResolution, error) {
	if r == nil || r.reader == nil {
		return ChainTimeResolution{}, contractErrorf("height time window resolver is not wired")
	}
	if err := interval.Validate(); err != nil {
		return ChainTimeResolution{}, err
	}
	if budget == nil {
		return ChainTimeResolution{}, contractErrorf("height time window resolution requires a query budget")
	}
	if interval.From.Kind != ScopeHeight {
		return ChainTimeResolution{}, contractErrorf("height time window resolution requires a height interval, got %q",
			interval.From.Kind)
	}
	if chainID <= 0 {
		return ChainTimeResolution{}, contractErrorf("height time window resolution requires a positive numeric chain id")
	}
	fromHeight, toHeight := interval.From.Height, interval.To.Height

	search := newWindowSearch(r, chainID, budget)
	fromAt, fromHash, status, reason, err := search.heightProbeResult(ctx, fromHeight)
	if err != nil {
		return ChainTimeResolution{}, err
	}
	if status != WindowResolved {
		return r.unmappableTime(chainID, fromHeight, toHeight, reason, search.used), nil
	}
	toAt, toHash, status, reason, err := search.heightProbeResult(ctx, toHeight)
	if err != nil {
		return ChainTimeResolution{}, err
	}
	if status != WindowResolved {
		return r.unmappableTime(chainID, fromHeight, toHeight, reason, search.used), nil
	}

	// Canonical identity verification of both endpoint heights: the live header
	// observed at the height must still match the durable canonical block. A
	// contradicted identity is a reorg (`invalidated`, pending only); an
	// unprovable one keeps the window unmappable (explicit gap).
	for _, endpoint := range []struct {
		height int64
		hash   string
	}{{fromHeight, fromHash}, {toHeight, toHash}} {
		verdict, localHash, err := r.verifyCanonicalEndpoint(ctx, chainID, endpoint.height, endpoint.hash, budget)
		if err != nil {
			return ChainTimeResolution{}, err
		}
		switch verdict {
		case canonicalUnprovable:
			return r.unmappableTime(chainID, fromHeight, toHeight, WindowReasonIdentityNotCanonical, search.used), nil
		case canonicalContradicted:
			return ChainTimeResolution{
				Status: WindowInvalidated,
				Reason: WindowReasonReorgInvalidated,
				EvidenceRef: fmt.Sprintf("heightwindow:v1 chain=%d status=invalidated height=%d header=%s canonical=%s",
					chainID, endpoint.height, strings.ToLower(endpoint.hash), strings.ToLower(localHash)),
			}, nil
		}
	}
	if toAt.Before(fromAt) {
		// Non-monotonic chain times cannot form an inclusive window; refuse
		// rather than normalize a negative span into coverage.
		return r.unmappableTime(chainID, fromHeight, toHeight, WindowReasonTimeOrder, search.used), nil
	}

	// Right-seam closure: adjacent claimed intervals [a..b] and [b+1..c] must
	// tile the time axis. Closing [a..b] at t(b) would leave a hole
	// (t(b), t(b+1)) in which a blockless row would be read by neither interval
	// while both claim closed coverage. The window is therefore closed at the
	// next height's chain time, overlapping the next interval's left edge
	// (harmless: candidates match by stable business key and identities dedup).
	// When the next height's chain time is not provable — beyond the locally
	// provable coverage/tip, a probe bound/failure, an unprovable or
	// reorg-contradicted canonical identity, a backwards chain time — the
	// window falls back to [t(from), t(to)] and marks the right boundary
	// unproven; the caller records an explicit gap, so the seam can never close
	// silently. A height that cannot advance has no following interval and
	// needs no extension.
	rightAt := toAt
	boundaryUnproven := false
	boundaryReason := ""
	if toHeight < math.MaxInt64 {
		nextAt, nextHash, ok, tryReason, err := search.tryHeightProbe(ctx, toHeight+1)
		if err != nil {
			return ChainTimeResolution{}, err
		}
		switch {
		case !ok:
			boundaryUnproven, boundaryReason = true, tryReason
		case toAt.After(nextAt):
			boundaryUnproven, boundaryReason = true, WindowReasonTimeOrder
		default:
			verdict, _, err := r.verifyCanonicalEndpoint(ctx, chainID, toHeight+1, nextHash, budget)
			if err != nil {
				return ChainTimeResolution{}, err
			}
			switch verdict {
			case canonicalVerified:
				rightAt = nextAt
			case canonicalUnprovable:
				boundaryUnproven, boundaryReason = true, WindowReasonIdentityNotCanonical
			default:
				boundaryUnproven, boundaryReason = true, WindowReasonReorgInvalidated
			}
		}
	}

	evidenceRef := fmt.Sprintf("heightwindow:v1 chain=%d heights=%d..%d probes=%d",
		chainID, fromHeight, toHeight, search.used)
	if boundaryUnproven {
		evidenceRef = fmt.Sprintf("heightwindow:v1 chain=%d heights=%d..%d probes=%d boundary=to_height_%d_unproven reason=%s",
			chainID, fromHeight, toHeight, search.used, toHeight+1, boundaryReason)
	} else if rightAt.After(toAt) {
		evidenceRef = fmt.Sprintf("heightwindow:v1 chain=%d heights=%d..%d probes=%d boundary_height=%d",
			chainID, fromHeight, toHeight, search.used, toHeight+1)
	}
	return ChainTimeResolution{
		Status:           WindowResolved,
		From:             fromAt,
		To:               rightAt,
		Reason:           boundaryReason,
		BoundaryUnproven: boundaryUnproven,
		EvidenceRef:      evidenceRef,
	}, nil
}

// tryHeightProbe reads one optional boundary height's chain time without
// failing the resolution: an exhausted budget or caller cancellation is a hard
// error, every other probe problem reports ok=false with a stable machine
// reason (probe bound, probe failure, unreadable source).
func (s *windowSearch) tryHeightProbe(ctx context.Context, height int64) (time.Time, string, bool, string, error) {
	probe, err := s.probe(ctx, height)
	if err != nil {
		if errors.Is(err, ErrBudgetExhausted) {
			// An exhausted budget must stop the invocation observably; it is
			// never converted into a silent boundary fallback.
			return time.Time{}, "", false, "", err
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return time.Time{}, "", false, "", ctxErr
		}
		if errors.Is(err, errWindowProbeLimit) {
			return time.Time{}, "", false, WindowReasonProbeLimit, nil
		}
		return time.Time{}, "", false, WindowReasonProbeFailed, nil
	}
	if !probe.ok {
		return time.Time{}, "", false, WindowReasonProbeFailed, nil
	}
	return probe.at, probe.hash, true, "", nil
}

// heightProbeResult reads one endpoint height's chain time within the bounded
// probe state. A probe problem resolves as an unmappable status/reason pair
// (evidence-unavailable); an exhausted budget aborts the scan boundedly
// (ErrBudgetExhausted) and caller-context cancellation is a hard error.
func (s *windowSearch) heightProbeResult(ctx context.Context, height int64) (time.Time, string, WindowStatus, string, error) {
	at, hash, ok, reason, err := s.tryHeightProbe(ctx, height)
	if err != nil {
		return time.Time{}, "", "", "", err
	}
	if !ok {
		return time.Time{}, "", WindowUnmappable, reason, nil
	}
	return at, hash, WindowResolved, "", nil
}

// canonicalVerdict is the outcome of comparing one probed header hash against
// the durable canonical identity.
type canonicalVerdict int

const (
	// canonicalVerified: the header hash and the durable canonical hash agree
	// (or the probe carried no hash to compare).
	canonicalVerified canonicalVerdict = iota
	// canonicalUnprovable: the durable index cannot prove a canonical identity
	// at the height.
	canonicalUnprovable
	// canonicalContradicted: the durable canonical identity contradicts the
	// live header at the height (reorg).
	canonicalContradicted
)

// verifyCanonicalEndpoint checks one probed height's header hash against the
// durable canonical identity, charged as one PG read when the probe carries a
// hash. An empty header hash skips the comparison (nothing to compare).
func (r *TimeHeightResolver) verifyCanonicalEndpoint(ctx context.Context, chainID, height int64,
	headerHash string, budget ScanQueryBudget) (canonicalVerdict, string, error) {
	if headerHash == "" {
		return canonicalVerified, "", nil
	}
	if err := budget.ConsumePG(ctx, 1); err != nil {
		return canonicalVerified, "", err
	}
	localHash, ok, err := r.reader.CanonicalHashAt(ctx, chainID, height)
	if err != nil {
		return canonicalVerified, "", fmt.Errorf("read canonical identity at %d: %w", height, err)
	}
	if !ok || strings.TrimSpace(localHash) == "" {
		return canonicalUnprovable, "", nil
	}
	if !strings.EqualFold(localHash, headerHash) {
		return canonicalContradicted, localHash, nil
	}
	return canonicalVerified, localHash, nil
}

// unmappableTime builds the conservative height→time refusal resolution.
func (r *TimeHeightResolver) unmappableTime(chainID, fromHeight, toHeight int64, reason string, probes int) ChainTimeResolution {
	return ChainTimeResolution{
		Status: WindowUnmappable,
		Reason: reason,
		EvidenceRef: fmt.Sprintf("heightwindow:v1 chain=%d status=unmappable reason=%s probes=%d heights=%d..%d",
			chainID, reason, probes, fromHeight, toHeight),
	}
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

// ChainTimeAt reads one block header time and hash from the node RPC. The time
// is the header's own second-precision timestamp (time.Unix, no nanosecond
// fabrication); a nil/mismatched header reports ok=false, never a guessed
// time.
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
