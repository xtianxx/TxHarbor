// chainfacts.go implements T013: the read-only chain-facts adapter for 014
// reconciliation (FR-001–FR-006, data-model.md §2–§4, research §1/§3).
//
// The adapter answers one question honestly: "what can the local chain index
// prove about this height range right now?" It reuses the durable 002/003
// indexer facts (indexer_checkpoint / log_checkpoint / chain_blocks /
// erc20_transfer_logs / pause rows), the 006 recovery snapshot
// (indexer.LoadRecoverySnapshot) and the 005 confirmation math
// (indexer.ConfirmationReached / ExactConfirmations) without ever writing,
// acquiring a lease, triggering recovery/replay/unblock or a payment.
//
// Hard rules encoded here (spec.md FR-002–FR-006, Q1/Q3/Q4/Q5):
//
//   - Source/version/freshness are explicit fields. A bundle records where the
//     facts came from, which block identity / recovery version they were read
//     under, and how old the source was. Freshness is evidence metadata, never
//     part of the canonical fact bytes (see CanonicalBytes).
//   - The local chain index is NOT the upstream ledger. A `connected`
//     declaration never proves upstream success; ExternalLedgerProvable always
//     reports false. Unconnected/unreachable/pruned/stale/unknown/incomplete
//     coverage can never yield a consistent verdict: only a `complete` bundle
//     without orphaned evidence can support one, and PendingReverify never
//     returns ReverifyConsistent.
//   - One physical source is one evidence item: ChainEvidenceSet dedups by
//     provenance ref, so re-reading or re-wrapping a source never inflates the
//     independent-evidence count.
//   - Read-only by construction: this file never opens a DB transaction and
//     never holds one across the optional slow header RPC; no DB lock is taken
//     and no 001–015 table is written (FR-014/020/023).
//   - Authorization is NOT decided here. Callers must have passed the authz.go
//     evaluation (action × permission × scope) before calling Observe; this
//     file makes no permission decision and self-grants nothing.
//   - Money stays integer: log payloads are returned as raw bytes/hex and are
//     never parsed into floating point; all counts/quotas are integers.
package reconciliation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/indexer"
)

// chainFactsCanonicalVersion freezes the chain-party snapshot canonicalization
// domain. Changing it changes every content hash and is a dedup-breaking change.
// v2 adds the indexed topic1/topic2 transfer parameters (T033 direction
// attribution); the amount stays raw hex in data.
const chainFactsCanonicalVersion = "txharbor.reconciliation.chainfacts.v2"

// ChainFactSource is the closed provenance vocabulary of chain evidence. A
// source identifies one physical read path; the same path re-read (or one read
// wrapped twice) is the same source, never two independent evidence items.
type ChainFactSource string

const (
	// ChainSourceLocalIndex is the bundle's overall read path: the durable
	// local indexer tables. It is a local reference index, not the upstream
	// ledger (FR-006/Q4-5).
	ChainSourceLocalIndex ChainFactSource = "local_chain_index"
	// ChainSourceCanonicalBlock is one chain_blocks row (002).
	ChainSourceCanonicalBlock ChainFactSource = "chain_canonical_block"
	// ChainSourceTransferLog is one erc20_transfer_logs row (003).
	ChainSourceTransferLog ChainFactSource = "chain_transfer_log"
	// ChainSourceHeaderCheckpoint is the durable indexer_checkpoint row (002).
	ChainSourceHeaderCheckpoint ChainFactSource = "chain_header_checkpoint"
	// ChainSourceLogCheckpoint is the durable log_checkpoint row (003).
	ChainSourceLogCheckpoint ChainFactSource = "chain_log_checkpoint"
	// ChainSourceRecoverySnapshot is the 006 recovery snapshot read.
	ChainSourceRecoverySnapshot ChainFactSource = "chain_recovery_snapshot"
	// ChainSourceConfirmationBasis is the 005 confirmation math applied to the
	// observed tip and block.
	ChainSourceConfirmationBasis ChainFactSource = "chain_confirmation_basis"
	// ChainSourceLiveHeader is the optional live header RPC read (freshness
	// verification only; never an independent copy of a durable fact).
	ChainSourceLiveHeader ChainFactSource = "chain_live_header"
)

// Valid reports whether s is part of the closed source vocabulary.
func (s ChainFactSource) Valid() bool {
	switch s {
	case ChainSourceLocalIndex, ChainSourceCanonicalBlock, ChainSourceTransferLog,
		ChainSourceHeaderCheckpoint, ChainSourceLogCheckpoint,
		ChainSourceRecoverySnapshot, ChainSourceConfirmationBasis,
		ChainSourceLiveHeader:
		return true
	}
	return false
}

// ChainFactsStatus is the conservative usability status of one chain-facts
// bundle. Only ChainFactsComplete can support a consistent conclusion; every
// other status maps to pending reverify (stale/unknown/incomplete) and never
// to consistent.
type ChainFactsStatus string

const (
	// ChainFactsComplete: the requested range was fully covered by fresh,
	// canonical, confirm-eligible local index facts with no open pause and no
	// active recovery hazard. Still not proof of upstream success.
	ChainFactsComplete ChainFactsStatus = "complete"
	// ChainFactsIncomplete: coverage is partial (not indexed yet, pruned rows,
	// gap, pause, unconnected upstream, range bound, provisional recovery).
	ChainFactsIncomplete ChainFactsStatus = "incomplete"
	// ChainFactsStale: the source (or the live tip lag) exceeds its tolerance.
	ChainFactsStale ChainFactsStatus = "stale"
	// ChainFactsUnknown: a required source could not be read or the live check
	// failed; nothing may be concluded from this evidence.
	ChainFactsUnknown ChainFactsStatus = "unknown"
)

// Valid reports whether s is one of the closed statuses.
func (s ChainFactsStatus) Valid() bool {
	switch s {
	case ChainFactsComplete, ChainFactsIncomplete, ChainFactsStale, ChainFactsUnknown:
		return true
	}
	return false
}

// CanSupportConsistent reports whether this status may support a consistent
// verdict. Only complete does; unknown/incomplete/stale never can (FR-004/005,
// Q1/Q5).
func (s ChainFactsStatus) CanSupportConsistent() bool { return s == ChainFactsComplete }

// chainFactsSeverity orders statuses so that combining observations keeps the
// most conservative outcome.
func chainFactsSeverity(s ChainFactsStatus) int {
	switch s {
	case ChainFactsUnknown:
		return 3
	case ChainFactsIncomplete:
		return 2
	case ChainFactsStale:
		return 1
	case ChainFactsComplete:
		return 0
	}
	return 0
}

// Stable machine reasons recorded on a bundle. They are audit/evidence
// vocabulary and MUST NOT be reworded once published.
const (
	ChainReasonRangeExceedsBound       = "range_exceeds_configured_bound"
	ChainReasonNoHeaderCheckpoint      = "no_durable_header_checkpoint"
	ChainReasonRangeBeforeStart        = "range_precedes_indexer_start"
	ChainReasonRangeNotIndexed         = "range_not_yet_indexed"
	ChainReasonBlocksMissing           = "block_rows_missing_or_pruned"
	ChainReasonNonCanonicalBlocks      = "non_canonical_block_rows_in_range"
	ChainReasonBlockLinkageBroken      = "block_parent_linkage_broken"
	ChainReasonHeaderStreamPaused      = "header_stream_paused"
	ChainReasonLogStreamPaused         = "log_stream_paused"
	ChainReasonLogNotCovered           = "log_range_not_covered"
	ChainReasonLogRangeBeforeStart     = "log_range_precedes_configured_start"
	ChainReasonSourceReadFailed        = "source_read_failed"
	ChainReasonSourceAgeExceeds        = "source_age_exceeds_tolerance"
	ChainReasonTipLagExceeds           = "tip_lag_exceeds_tolerance"
	ChainReasonLiveCheckFailed         = "live_header_check_failed"
	ChainReasonLiveChainIDMismatch     = "live_chain_id_mismatch"
	ChainReasonLiveHeaderMismatch      = "live_header_hash_mismatch"
	ChainReasonRecoveryProvisional     = "reorg_recovery_provisional"
	ChainReasonRecoveryReconcile       = "reorg_recovery_reconcile_required"
	ChainReasonRecoverySweepIntersects = "reorg_recovery_sweep_intersects_range"
	ChainReasonConfirmBasisMissing     = "confirm_basis_threshold_missing"
	ChainReasonBelowConfirmThreshold   = "block_below_confirm_threshold"
	ChainReasonConfirmTipBelowRange    = "confirm_tip_below_range"
	ChainReasonUpstreamUnconnected     = "upstream_receipt_source_unconnected"
)

// Stream identities used by pause rows.
const (
	chainHeaderStream = "header"
	chainLogStream    = "log"
)

// ChainEvidenceRef is one provenance handle: the physical source plus the
// stable identity of the underlying fact. A ref is self-delimiting and carries
// no secret material.
type ChainEvidenceRef struct {
	Source ChainFactSource `json:"source"`
	Ref    string          `json:"ref"`
}

// Validate checks the ref shape conservatively.
func (r ChainEvidenceRef) Validate() error {
	if !r.Source.Valid() {
		return contractErrorf("unknown chain evidence source %q", r.Source)
	}
	if strings.TrimSpace(r.Ref) == "" || len(r.Ref) > 512 {
		return contractErrorf("chain evidence ref must be 1..512 bytes")
	}
	if strings.ContainsRune(r.Ref, 0) {
		return contractErrorf("chain evidence ref contains NUL")
	}
	return nil
}

// String renders the ref for audit details.
func (r ChainEvidenceRef) String() string { return string(r.Source) + ":" + r.Ref }

// ChainEvidenceSet is a deduplicating provenance set. Adding the same physical
// source ref twice is idempotent: the second add reports false and does not
// inflate the independent-evidence count ("同一来源不得包装成多独立证据").
// The zero value is usable.
type ChainEvidenceSet struct {
	refs []ChainEvidenceRef
	seen map[string]struct{}
}

// Add records one provenance ref. It returns (true, nil) for a new ref,
// (false, nil) when the same source+ref was already recorded, and an error for
// a malformed ref.
func (s *ChainEvidenceSet) Add(ref ChainEvidenceRef) (bool, error) {
	if err := ref.Validate(); err != nil {
		return false, err
	}
	key := ref.String()
	if s.seen == nil {
		s.seen = make(map[string]struct{})
	}
	if _, ok := s.seen[key]; ok {
		return false, nil
	}
	s.seen[key] = struct{}{}
	s.refs = append(s.refs, ref)
	return true, nil
}

// Len returns the number of distinct evidence refs.
func (s *ChainEvidenceSet) Len() int { return len(s.refs) }

// Refs returns a defensive copy of the recorded refs in insertion order.
func (s *ChainEvidenceSet) Refs() []ChainEvidenceRef {
	out := make([]ChainEvidenceRef, len(s.refs))
	copy(out, s.refs)
	return out
}

// Sources returns the distinct physical sources in stable sorted order.
func (s *ChainEvidenceSet) Sources() []ChainFactSource {
	seen := make(map[ChainFactSource]struct{}, len(s.refs))
	out := make([]ChainFactSource, 0, len(s.refs))
	for _, ref := range s.refs {
		if _, ok := seen[ref.Source]; ok {
			continue
		}
		seen[ref.Source] = struct{}{}
		out = append(out, ref.Source)
	}
	slices.Sort(out)
	return out
}

// IndependentSourceCount returns how many distinct physical sources back this
// evidence. Duplicate refs never raise it.
func (s *ChainEvidenceSet) IndependentSourceCount() int { return len(s.Sources()) }

// ChainFactBlock is one canonical-indexed block header fact (chain_blocks).
type ChainFactBlock struct {
	Number     int64     `json:"number"`
	Hash       string    `json:"hash"`
	ParentHash string    `json:"parent_hash"`
	Canonical  bool      `json:"canonical"`
	IndexedAt  time.Time `json:"indexed_at"`
}

// ChainFactLog is one indexed ERC-20 Transfer log fact (erc20_transfer_logs).
// Amount and address payloads stay in their raw on-chain hex form (Data,
// Topic1, Topic2); the adapter never parses an amount into floating point
// (integer/NUMERIC rule). From/To are the normalized lowercase sender/recipient
// addresses decoded from the indexed topic1/topic2 parameters; they are empty
// when the topic is not a zero-padded 20-byte address, so an unknown direction
// is never guessed (T033 direction/asset attribution reads these fields).
type ChainFactLog struct {
	BlockNumber int64  `json:"block_number"`
	BlockHash   string `json:"block_hash"`
	TxHash      string `json:"tx_hash"`
	LogIndex    int64  `json:"log_index"`
	Contract    string `json:"contract"`
	Topic0      string `json:"topic0"`
	Topic1      string `json:"topic1,omitempty"`
	Topic2      string `json:"topic2,omitempty"`
	From        string `json:"from,omitempty"`
	To          string `json:"to,omitempty"`
	Data        string `json:"data"`
}

// ChainFactsObservePGStatementCap / ChainFactsObserveRPCCallCap are the proven
// upper bounds of the internal reads one Observe call issues (T038): ten
// standalone SQL statements (header progress, header meta, log progress, log
// meta, header pause, log pause, block rows, transfer-log rows, recovery
// snapshot, recovery released) and at most three header RPCs (chain id, live
// tip, live anchor). Budget seams charge these caps instead of guessing an
// unbounded read count.
const (
	ChainFactsObservePGStatementCap = 10
	ChainFactsObserveRPCCallCap     = 3
)

// ChainPause is one durable stream-pause row (indexer_pause / log_pause).
type ChainPause struct {
	Stream string `json:"stream"`
	Height int64  `json:"height"`
	Kind   string `json:"kind"`
	Detail string `json:"detail"`
}

// ChainFactCoverage records how much of the requested range the durable index
// actually covers. Coverage is proven, never assumed: missing/pruned rows,
// pauses and below-start ranges are explicit.
type ChainFactCoverage struct {
	From                   int64       `json:"from"`
	To                     int64       `json:"to"`
	HasHeaderCheckpoint    bool        `json:"has_header_checkpoint"`
	HeaderCheckpointHeight int64       `json:"header_checkpoint_height"`
	HeaderCheckpointHash   string      `json:"header_checkpoint_hash"`
	HeaderStartHeight      int64       `json:"header_start_height"`
	HasLogCheckpoint       bool        `json:"has_log_checkpoint"`
	LogCheckpointNext      int64       `json:"log_checkpoint_next"`
	LogCheckpointStart     int64       `json:"log_checkpoint_start"`
	LogConfigHash          string      `json:"log_config_hash"`
	BlockRows              int64       `json:"block_rows"`
	ExpectedBlocks         int64       `json:"expected_blocks"`
	MissingBlocks          int64       `json:"missing_blocks"`
	NonCanonicalBlocks     int64       `json:"non_canonical_blocks"`
	LinkageVerified        bool        `json:"linkage_verified"`
	LinkageBroken          bool        `json:"linkage_broken"`
	HeaderPause            *ChainPause `json:"header_pause,omitempty"`
	LogPause               *ChainPause `json:"log_pause,omitempty"`
	LogRangeCovered        bool        `json:"log_range_covered"`
}

// ChainFactFreshness records the freshness inputs of one observation.
// SourceAge is the worst-case age over the sources the query needs; TipLag is
// relative to the durable checkpoint tip (or the live tip when checked).
type ChainFactFreshness struct {
	CapturedAt      time.Time     `json:"captured_at"`
	HeaderUpdatedAt *time.Time    `json:"header_updated_at,omitempty"`
	LogUpdatedAt    *time.Time    `json:"log_updated_at,omitempty"`
	MaxSourceAge    time.Duration `json:"max_source_age"`
	SourceAge       time.Duration `json:"source_age"`
	Stale           bool          `json:"stale"`
	TipHeight       *int64        `json:"tip_height,omitempty"`
	TipHash         string        `json:"tip_hash,omitempty"`
	TipLagHeights   *int64        `json:"tip_lag_heights,omitempty"`
	MaxTipLag       int64         `json:"max_tip_lag"`
	LiveChecked     bool          `json:"live_checked"`
}

// ChainFactRecovery records the 006 recovery snapshot observed at read time.
// An active recovery never triggers anything here (FR-023); it only downgrades
// evidence that lies in a provisional or reconcile-held region.
type ChainFactRecovery struct {
	Active         bool                  `json:"active"`
	Released       bool                  `json:"released"`
	RecoveryID     string                `json:"recovery_id,omitempty"`
	Phase          string                `json:"phase,omitempty"`
	PolicySeq      int64                 `json:"policy_seq"`
	Seq            int64                 `json:"seq"`
	MaxDepth       int64                 `json:"max_depth"`
	BoundOldNumber int64                 `json:"bound_old_number"`
	BoundOldHash   string                `json:"bound_old_hash,omitempty"`
	AncestorNumber *int64                `json:"ancestor_number,omitempty"`
	SweepEnd       *int64                `json:"sweep_end,omitempty"`
	Depth          *int64                `json:"depth,omitempty"`
	Reconcile      bool                  `json:"reconcile_required"`
	State          indexer.RecoveryState `json:"state,omitempty"`
	Validity       indexer.Validity      `json:"validity,omitempty"`
}

// ChainFactConfirmation is the confirmation basis of the observed block: the
// confirm policy depth N, the tip used, the exact confirmation count and
// whether the gate is reached. An unknown basis (no N) or an unreached gate
// can never support a consistent verdict.
type ChainFactConfirmation struct {
	Known         bool            `json:"known"`
	ThresholdN    uint64          `json:"threshold_n"`
	TipHeight     *int64          `json:"tip_height,omitempty"`
	TipHash       string          `json:"tip_hash,omitempty"`
	TipSource     ChainFactSource `json:"tip_source,omitempty"`
	Confirmations uint64          `json:"confirmations"`
	Reached       bool            `json:"reached"`
}

// ChainFactVersion is the chain-side evidence version: anchor block identity
// plus the durable stream/recovery versions observed at read time.
type ChainFactVersion struct {
	BlockNumber            uint64 `json:"block_number"`
	BlockHash              string `json:"block_hash"`
	RecoveryID             string `json:"recovery_id,omitempty"`
	RecoverySeq            int64  `json:"recovery_seq"`
	RecoveryPolicySeq      int64  `json:"recovery_policy_seq"`
	HeaderCheckpointHeight int64  `json:"header_checkpoint_height"`
	HeaderCheckpointHash   string `json:"header_checkpoint_hash,omitempty"`
	LogCheckpointNext      int64  `json:"log_checkpoint_next"`
	LogConfigHash          string `json:"log_config_hash,omitempty"`
}

// ChainUpstreamReceiptSource is one declared upstream receipt source of the
// task scope (recon_task.upstream_receipt_source). It is a declaration about a
// read path, never evidence.
type ChainUpstreamReceiptSource struct {
	BusinessType BusinessType `json:"business_type"`
	Source       string       `json:"source"`
	Connected    bool         `json:"connected"`
}

// Validate checks the declaration shape: known business type, and a named
// source whenever the declaration claims a connection.
func (u ChainUpstreamReceiptSource) Validate() error {
	if !u.BusinessType.Known() {
		return contractErrorf("unknown upstream receipt business type %q", u.BusinessType)
	}
	if u.Connected && strings.TrimSpace(u.Source) == "" {
		return contractErrorf("connected upstream receipt business type %q requires a source", u.BusinessType)
	}
	if len(u.Source) > 512 || strings.ContainsRune(u.Source, 0) {
		return contractErrorf("upstream receipt source is malformed for business type %q", u.BusinessType)
	}
	return nil
}

// ProvableSuccess reports whether this declaration proves the upstream ledger
// processed the scope successfully. It never does: connected=true only says a
// read path is declared/available, and a connected flag alone never proves
// upstream success (FR-006, Q4-5, data-model.md §4).
func (u ChainUpstreamReceiptSource) ProvableSuccess() bool { return false }

// ParseChainUpstreamReceiptSources decodes the task's upstream_receipt_source
// JSONB shape {business_type: {source, connected}} into a stable ordered
// slice. An empty document yields an empty slice (every scope then counts as
// unconnected). Unknown business types and malformed shapes are refused
// conservatively.
func ParseChainUpstreamReceiptSources(raw []byte) ([]ChainUpstreamReceiptSource, error) {
	if len(strings.TrimSpace(string(raw))) == 0 {
		return nil, nil
	}
	var decoded map[string]struct {
		Source    string `json:"source"`
		Connected bool   `json:"connected"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return nil, fmt.Errorf("%w: upstream receipt source is not a JSON object: %v", ErrContract, err)
	}
	out := make([]ChainUpstreamReceiptSource, 0, len(decoded))
	for name, entry := range decoded {
		item := ChainUpstreamReceiptSource{
			BusinessType: BusinessType(name),
			Source:       entry.Source,
			Connected:    entry.Connected,
		}
		if err := item.Validate(); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].BusinessType < out[j].BusinessType })
	return out, nil
}

// ChainFactsQuery is one read-only observation request over an inclusive block
// height range. The adapter is height-scoped: a time-scoped task must resolve
// its bounds to heights before calling (the local index stores no chain
// timestamps, so this adapter never guesses a time→height mapping).
type ChainFactsQuery struct {
	ChainID int64 `json:"chain_id"`
	From    int64 `json:"from"`
	To      int64 `json:"to"`
	// ConfirmThresholdN is the confirm policy depth (N >= 1) the chain facts
	// must be anchored to. 0 means the task supplied no confirm basis: the
	// evidence is then incomplete and can never support a consistent verdict.
	ConfirmThresholdN uint64 `json:"confirm_threshold_n"`
	// NeedTransferLogs requests the ERC-20 Transfer facts (and the 003 log
	// stream coverage) of the range.
	NeedTransferLogs bool `json:"need_transfer_logs"`
	// UpstreamReceipts is the task's declared per-business-type upstream
	// receipt source. Nil/empty means "not connected", which downgrades the
	// bundle: an external conclusion can never be supported (FR-006).
	UpstreamReceipts []ChainUpstreamReceiptSource `json:"upstream_receipts,omitempty"`
}

// Validate checks the query shape conservatively.
func (q ChainFactsQuery) Validate() error {
	if q.ChainID <= 0 {
		return contractErrorf("chain facts query requires a positive numeric chain id")
	}
	if q.From < 0 || q.To < q.From {
		return contractErrorf("chain facts query range %d..%d is not an inclusive ascending height range", q.From, q.To)
	}
	seenTypes := make(map[BusinessType]struct{}, len(q.UpstreamReceipts))
	for _, item := range q.UpstreamReceipts {
		if err := item.Validate(); err != nil {
			return err
		}
		if _, dup := seenTypes[item.BusinessType]; dup {
			return contractErrorf("duplicate upstream receipt declaration for business type %q", item.BusinessType)
		}
		seenTypes[item.BusinessType] = struct{}{}
	}
	return nil
}

// ChainFactsConfig carries the adapter's bounded read tolerances. Every value
// is a deployment/test parameter (research §7): the adapter never invents a
// production threshold, and unbounded is not representable.
type ChainFactsConfig struct {
	// MaxSourceAge is the freshness tolerance of the durable index sources.
	MaxSourceAge time.Duration
	// MaxTipLag is the freshness tolerance of the live-tip lag, in heights.
	MaxTipLag int64
	// MaxRangeSpan bounds how many heights one observation may load. Callers
	// pass the task budget's per-claim span (T008); larger requests stay
	// incomplete instead of scanning unbounded.
	MaxRangeSpan int64
	// LiveHeader enables the optional read-only live canonical check. It is a
	// verification of freshness/identity only: it never scans, commits or
	// recovers. Nil disables the check (freshness then relies on source age).
	LiveHeader indexer.HeaderClient
	// RPCTimeout bounds every live header read; required when LiveHeader is
	// set. Every RPC runs outside any DB transaction (this adapter opens none).
	RPCTimeout time.Duration
}

// Validate fails closed on missing or non-positive bounds. RPCTimeout is only
// required when a live header client is configured.
func (c ChainFactsConfig) Validate() error {
	if c.MaxSourceAge <= 0 {
		return contractErrorf("chain facts config requires a positive max source age")
	}
	if c.MaxTipLag <= 0 {
		return contractErrorf("chain facts config requires a positive max tip lag")
	}
	if c.MaxRangeSpan <= 0 {
		return contractErrorf("chain facts config requires a positive max range span")
	}
	if c.LiveHeader != nil && c.RPCTimeout <= 0 {
		return contractErrorf("chain facts config requires a positive RPC timeout when a live header client is set")
	}
	return nil
}

// ChainFactsAdapter reads durable chain facts. It is read-only and stateless
// apart from its pool/config; concurrent use is safe.
type ChainFactsAdapter struct {
	pool *pgxpool.Pool
	cfg  ChainFactsConfig
}

// NewChainFactsAdapter validates the dependencies and bounds. A nil pool or
// invalid config is refused fail-closed. It performs no I/O.
func NewChainFactsAdapter(pool *pgxpool.Pool, cfg ChainFactsConfig) (*ChainFactsAdapter, error) {
	if pool == nil {
		return nil, contractErrorf("chain facts adapter requires a database pool")
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &ChainFactsAdapter{pool: pool, cfg: cfg}, nil
}

// ChainFactsBundle is one immutable chain-party evidence bundle. Status and
// Reasons are conservative metadata; CanonicalBytes covers only the observed
// facts so re-observing the same facts stays identity-stable.
type ChainFactsBundle struct {
	ChainID          int64                        `json:"chain_id"`
	From             int64                        `json:"from"`
	To               int64                        `json:"to"`
	Source           ChainFactSource              `json:"source"`
	CapturedAt       time.Time                    `json:"captured_at"`
	Blocks           []ChainFactBlock             `json:"blocks,omitempty"`
	Logs             []ChainFactLog               `json:"logs,omitempty"`
	Anchor           *ChainFactBlock              `json:"anchor,omitempty"`
	Coverage         ChainFactCoverage            `json:"coverage"`
	Freshness        ChainFactFreshness           `json:"freshness"`
	Recovery         ChainFactRecovery            `json:"recovery"`
	Confirmation     ChainFactConfirmation        `json:"confirmation"`
	UpstreamReceipts []ChainUpstreamReceiptSource `json:"upstream_receipts,omitempty"`
	// Orphaned is true when any observed block row in range is non-canonical
	// or the live check contradicts the durable block identity: the evidence
	// may only be handled as pending reverify (FR-017).
	Orphaned bool             `json:"orphaned"`
	Status   ChainFactsStatus `json:"status"`
	Reasons  []string         `json:"reasons,omitempty"`

	evidence ChainEvidenceSet
}

// mark raises the bundle status monotonically and records a reason. Unknown
// always wins over incomplete, which wins over stale, which wins over complete:
// the most conservative observation survives combination.
func (b *ChainFactsBundle) mark(status ChainFactsStatus, reason string) {
	if chainFactsSeverity(status) > chainFactsSeverity(b.Status) {
		b.Status = status
	}
	if reason != "" {
		b.addReason(reason)
	}
}

// addReason appends one reason without duplicates.
func (b *ChainFactsBundle) addReason(reason string) {
	if slices.Contains(b.Reasons, reason) {
		return
	}
	b.Reasons = append(b.Reasons, reason)
}

// addEvidence records one provenance ref, deduplicating by ref.
func (b *ChainFactsBundle) addEvidence(source ChainFactSource, ref string) error {
	_, err := b.evidence.Add(ChainEvidenceRef{Source: source, Ref: ref})
	return err
}

// CanSupportConsistent reports whether this bundle may support a consistent
// verdict: only a complete, non-orphaned bundle can (FR-004/005/017).
func (b *ChainFactsBundle) CanSupportConsistent() bool {
	return b != nil && b.Status == ChainFactsComplete && !b.Orphaned
}

// PendingReverify maps the bundle onto the reverify vocabulary. ok=false means
// the evidence is complete and the caller may proceed to compare; when ok=true
// the verdict is never consistent (FR-004/005/017, Q1/Q5).
func (b *ChainFactsBundle) PendingReverify() (ReverifyVerdict, bool) {
	if b == nil {
		return ReverifyUnknown, true
	}
	if b.Orphaned {
		return ReverifyUnknown, true
	}
	switch b.Status {
	case ChainFactsComplete:
		return "", false
	case ChainFactsStale:
		return ReverifyStale, true
	default:
		return ReverifyUnknown, true
	}
}

// EvidenceRefs returns the deduplicated provenance set of the bundle.
func (b *ChainFactsBundle) EvidenceRefs() []ChainEvidenceRef { return b.evidence.Refs() }

// IndependentSourceCount returns the number of distinct physical sources. A
// duplicated read never raises it.
func (b *ChainFactsBundle) IndependentSourceCount() int { return b.evidence.IndependentSourceCount() }

// Sources returns the distinct physical sources backing the evidence.
func (b *ChainFactsBundle) Sources() []ChainFactSource { return b.evidence.Sources() }

// Version returns the chain-side evidence version observed at read time.
func (b *ChainFactsBundle) Version() ChainFactVersion {
	version := ChainFactVersion{
		RecoveryID:             b.Recovery.RecoveryID,
		RecoverySeq:            b.Recovery.Seq,
		RecoveryPolicySeq:      b.Recovery.PolicySeq,
		HeaderCheckpointHeight: b.Coverage.HeaderCheckpointHeight,
		HeaderCheckpointHash:   b.Coverage.HeaderCheckpointHash,
		LogCheckpointNext:      b.Coverage.LogCheckpointNext,
		LogConfigHash:          b.Coverage.LogConfigHash,
	}
	if b.Anchor != nil && b.Anchor.Number >= 0 {
		version.BlockNumber = uint64(b.Anchor.Number)
		version.BlockHash = b.Anchor.Hash
	}
	return version
}

// VersionDomain derives the chain-side evidence version domain (identity.go
// VersionDomain): anchor block identity, the observed recovery version, and
// the evidence time. PG-side versions (authorization/scope/state) are supplied
// by the other adapters; a bundle without an anchor intentionally leaves the
// block identity empty so Validate classifies it as insufficient evidence.
func (b *ChainFactsBundle) VersionDomain() VersionDomain {
	domain := VersionDomain{EvidenceAt: b.CapturedAt}
	if b.Anchor != nil && b.Anchor.Number >= 0 && b.Anchor.Hash != "" {
		domain.BlockNumber = uint64(b.Anchor.Number)
		domain.BlockHash = b.Anchor.Hash
	}
	if b.Recovery.RecoveryID != "" {
		domain.RecoveryVersion = fmt.Sprintf("%s/%d", b.Recovery.RecoveryID, b.Recovery.Seq)
	}
	return domain
}

// UnconnectedUpstreams lists the declared business types whose upstream
// receipt source is not connected. An empty bundle reports no entries; callers
// treat "no declaration" as unconnected via UpstreamDeclaredConnected.
func (b *ChainFactsBundle) UnconnectedUpstreams() []BusinessType {
	var out []BusinessType
	for _, item := range b.UpstreamReceipts {
		if !item.Connected {
			out = append(out, item.BusinessType)
		}
	}
	return out
}

// UpstreamDeclaredConnected reports whether every needed upstream receipt
// source is declared connected. This is a declaration about a read path, never
// proof of upstream success (FR-006).
func (b *ChainFactsBundle) UpstreamDeclaredConnected() bool {
	if len(b.UpstreamReceipts) == 0 {
		return false
	}
	for _, item := range b.UpstreamReceipts {
		if !item.Connected {
			return false
		}
	}
	return true
}

// ExternalLedgerProvable reports whether this evidence proves the upstream
// ledger processed the scope correctly. Always false: the local chain index
// and local reference consumers are not the upstream ledger, and a connected
// flag alone never proves upstream success (FR-006, Q4-5, data-model.md §4).
func (b *ChainFactsBundle) ExternalLedgerProvable() bool { return false }

// CanonicalBytes returns the deterministic chain-party snapshot bytes for
// SnapshotHash. Only observed facts are encoded (chain id, range, canonical
// block rows, transfer-log rows): freshness, status, reasons, upstream
// declarations and capture time are evidence metadata and are deliberately
// excluded so re-observing the same facts yields the same content hash and
// therefore the same stable identity.
func (b *ChainFactsBundle) CanonicalBytes() []byte {
	w := &identityCanonWriter{}
	w.bytesField("chainfacts.version", []byte(chainFactsCanonicalVersion))
	w.int64Field("chainfacts.chain_id", b.ChainID)
	w.int64Field("chainfacts.from", b.From)
	w.int64Field("chainfacts.to", b.To)

	blocks := append([]ChainFactBlock(nil), b.Blocks...)
	sort.Slice(blocks, func(i, j int) bool { return blocks[i].Number < blocks[j].Number })
	w.uint64Field("chainfacts.block.count", uint64(len(blocks)))
	for i, block := range blocks {
		prefix := fmt.Sprintf("chainfacts.block.%d", i)
		w.int64Field(prefix+".number", block.Number)
		w.stringField(prefix+".hash", block.Hash)
		w.stringField(prefix+".parent_hash", block.ParentHash)
		w.bytesField(prefix+".canonical", chainFactsBoolByte(block.Canonical))
	}

	logs := append([]ChainFactLog(nil), b.Logs...)
	sort.Slice(logs, func(i, j int) bool {
		a, z := logs[i], logs[j]
		switch {
		case a.BlockNumber != z.BlockNumber:
			return a.BlockNumber < z.BlockNumber
		case a.BlockHash != z.BlockHash:
			return a.BlockHash < z.BlockHash
		case a.TxHash != z.TxHash:
			return a.TxHash < z.TxHash
		default:
			return a.LogIndex < z.LogIndex
		}
	})
	w.uint64Field("chainfacts.log.count", uint64(len(logs)))
	for i, log := range logs {
		prefix := fmt.Sprintf("chainfacts.log.%d", i)
		w.int64Field(prefix+".block_number", log.BlockNumber)
		w.stringField(prefix+".block_hash", log.BlockHash)
		w.stringField(prefix+".tx_hash", log.TxHash)
		w.int64Field(prefix+".log_index", log.LogIndex)
		w.stringField(prefix+".contract", log.Contract)
		w.stringField(prefix+".topic0", log.Topic0)
		w.stringField(prefix+".topic1", log.Topic1)
		w.stringField(prefix+".topic2", log.Topic2)
		w.stringField(prefix+".data", log.Data)
	}
	return w.buf.Bytes()
}

// chainFactsBoolByte encodes a boolean as one canonical byte.
func chainFactsBoolByte(v bool) []byte {
	if v {
		return []byte{1}
	}
	return []byte{0}
}

// Observe reads the durable chain facts of one height range. It is strictly
// read-only: no transaction is opened, no lock is taken, no lease is acquired
// and no recovery/replay/payment path is touched (FR-014/020/023).
//
// Failure handling is conservative: a required read failure yields
// ChainFactsUnknown with the error returned alongside the partial bundle (so
// callers can log/audit the cause but can never conclude consistency from an
// ignored error). A range larger than the configured bound stays incomplete.
// The optional live header RPC runs after the DB reads, never inside them.
func (a *ChainFactsAdapter) Observe(ctx context.Context, q ChainFactsQuery) (ChainFactsBundle, error) {
	if a == nil || a.pool == nil {
		return ChainFactsBundle{}, contractErrorf("chain facts adapter has no database")
	}
	if err := q.Validate(); err != nil {
		return ChainFactsBundle{}, err
	}
	if err := a.cfg.Validate(); err != nil {
		return ChainFactsBundle{}, err
	}

	now := time.Now().UTC()
	b := ChainFactsBundle{
		ChainID:          q.ChainID,
		From:             q.From,
		To:               q.To,
		Source:           ChainSourceLocalIndex,
		CapturedAt:       now,
		UpstreamReceipts: append([]ChainUpstreamReceiptSource(nil), q.UpstreamReceipts...),
		Status:           ChainFactsComplete,
	}
	if err := b.addEvidence(ChainSourceLocalIndex,
		fmt.Sprintf("chain=%d/range=%d-%d", q.ChainID, q.From, q.To)); err != nil {
		return b, err
	}

	var errs []error
	recordErr := func(format string, err error) {
		b.mark(ChainFactsUnknown, ChainReasonSourceReadFailed)
		errs = append(errs, fmt.Errorf(format+": %w", err))
	}

	span := uint64(q.To-q.From) + 1
	readFacts := span <= uint64(a.cfg.MaxRangeSpan)
	if !readFacts {
		b.mark(ChainFactsIncomplete, ChainReasonRangeExceedsBound)
	}

	// Durable 002 header progress (scanner.go semantics).
	header, err := indexer.ReadReliableProgress(ctx, a.pool, q.ChainID, indexer.ReliableProgressHeaderSource)
	if err != nil {
		recordErr("read header progress", err)
	}
	headerMeta, err := a.readSourceMeta(ctx, chainHeaderMetaSQL, q.ChainID)
	if err != nil {
		recordErr("read header source age", err)
	}

	// Durable 003 log progress (logscanner.go semantics).
	logProgress, err := indexer.ReadReliableProgress(ctx, a.pool, q.ChainID, indexer.ReliableProgressLogSource)
	if err != nil {
		recordErr("read log progress", err)
	}
	logMeta, err := a.readSourceMeta(ctx, chainLogMetaSQL, q.ChainID)
	if err != nil {
		recordErr("read log source age", err)
	}

	// Durable pause rows: existence means the stream is not current.
	b.Coverage.HeaderPause, err = a.readPause(ctx, chainHeaderPauseSQL, q.ChainID, chainHeaderStream)
	if err != nil {
		recordErr("read header pause", err)
	}
	b.Coverage.LogPause, err = a.readPause(ctx, chainLogPauseSQL, q.ChainID, chainLogStream)
	if err != nil {
		recordErr("read log pause", err)
	}

	// Block facts (bounded by the configured span).
	if readFacts {
		b.Blocks, err = a.readBlocks(ctx, q)
		if err != nil {
			recordErr("read block rows", err)
		}
		if q.NeedTransferLogs {
			b.Logs, err = a.readLogs(ctx, q)
			if err != nil {
				recordErr("read transfer log rows", err)
			}
		}
	}

	// 006 recovery snapshot: read-only observation, never a trigger.
	recovery, err := indexer.LoadRecoverySnapshot(ctx, a.pool, q.ChainID)
	if err != nil {
		recordErr("read recovery snapshot", err)
	}
	released := false
	if err == nil && recovery.Row == nil {
		if released, err = indexer.RecoveryReleased(ctx, a.pool, q.ChainID); err != nil {
			recordErr("read recovery release state", err)
			released = false
		}
	}

	// Optional live canonical check: freshness/identity verification only.
	live := chainLiveObservation{}
	liveChecked := false
	if a.cfg.LiveHeader != nil && readFacts {
		anchorHash := ""
		if block := chainBlockAt(b.Blocks, q.To); block != nil && block.Canonical {
			anchorHash = block.Hash
		}
		live, err = a.liveObserve(ctx, q, anchorHash, header)
		liveChecked = true
		if err != nil {
			b.mark(ChainFactsUnknown, live.reason)
			errs = append(errs, fmt.Errorf("live header check: %w", err))
		} else if live.anchorMismatch {
			// The live chain contradicts the durable identity: orphaned
			// evidence may only go to pending reverify (FR-017).
			b.Orphaned = true
			b.mark(ChainFactsIncomplete, live.reason)
		}
	}

	a.applyHeaderCoverage(&b, header, headerMeta, q)
	if readFacts {
		a.applyBlockCoverage(&b)
		a.applyLogCoverage(&b, q, logProgress, logMeta)
	}
	a.applyRecovery(&b, recovery, released, q)
	a.applyFreshness(&b, headerMeta, logMeta, q, live, liveChecked, now)
	a.applyConfirmation(&b, q, header, live)
	a.applyUpstream(&b, q)
	a.applyEvidenceRefs(&b, header, logProgress, recovery, released, live, liveChecked)

	b.Freshness.CapturedAt = now
	if len(errs) > 0 {
		return b, errors.Join(errs...)
	}
	return b, nil
}

// applyHeaderCoverage materializes the 002 checkpoint facts and their coverage
// checks.
func (a *ChainFactsAdapter) applyHeaderCoverage(b *ChainFactsBundle, header indexer.ReliableProgress, meta chainSourceMeta, q ChainFactsQuery) {
	b.Coverage.From = q.From
	b.Coverage.To = q.To
	b.Coverage.ExpectedBlocks = q.To - q.From + 1
	if header.HasProgress {
		b.Coverage.HasHeaderCheckpoint = true
		b.Coverage.HeaderCheckpointHeight = int64(header.LastHeight)
		b.Coverage.HeaderCheckpointHash = header.Hash
		if meta.present {
			b.Coverage.HeaderStartHeight = meta.start
			switch {
			case q.From < meta.start:
				b.mark(ChainFactsIncomplete, ChainReasonRangeBeforeStart)
			case int64(header.LastHeight) < q.To:
				b.mark(ChainFactsIncomplete, ChainReasonRangeNotIndexed)
			}
		} else if int64(header.LastHeight) < q.To {
			b.mark(ChainFactsIncomplete, ChainReasonRangeNotIndexed)
		}
	} else {
		b.mark(ChainFactsIncomplete, ChainReasonNoHeaderCheckpoint)
	}
	if b.Coverage.HeaderPause != nil {
		b.mark(ChainFactsIncomplete, ChainReasonHeaderStreamPaused)
	}
}

// applyBlockCoverage verifies the observed block rows: presence, canonicality
// and parent linkage. Missing rows mean pruned evidence or an incomplete scan;
// non-canonical rows are orphaned evidence (FR-017).
func (a *ChainFactsAdapter) applyBlockCoverage(b *ChainFactsBundle) {
	b.Coverage.BlockRows = int64(len(b.Blocks))
	if b.Coverage.ExpectedBlocks > 0 {
		b.Coverage.MissingBlocks = b.Coverage.ExpectedBlocks - b.Coverage.BlockRows
	}
	for i := range b.Blocks {
		if !b.Blocks[i].Canonical {
			b.Coverage.NonCanonicalBlocks++
		}
	}
	if b.Coverage.NonCanonicalBlocks > 0 {
		b.Orphaned = true
		b.mark(ChainFactsIncomplete, ChainReasonNonCanonicalBlocks)
	}
	if b.Coverage.HasHeaderCheckpoint && b.Coverage.MissingBlocks != 0 {
		b.mark(ChainFactsIncomplete, ChainReasonBlocksMissing)
	}
	if len(b.Blocks) > 0 && b.Coverage.MissingBlocks == 0 {
		b.Coverage.LinkageVerified = true
		for i := 1; i < len(b.Blocks); i++ {
			if b.Blocks[i].ParentHash != b.Blocks[i-1].Hash {
				b.Coverage.LinkageBroken = true
				b.Orphaned = true
				b.mark(ChainFactsIncomplete, ChainReasonBlockLinkageBroken)
				break
			}
		}
	}
	if block := chainBlockAt(b.Blocks, b.To); block != nil && block.Canonical {
		anchor := *block
		b.Anchor = &anchor
	}
}

// applyLogCoverage checks the 003 stream coverage only when transfer logs are
// needed: next_block must cover the range, the range must not precede the
// configured start, and no log pause may be open. Missing log rows inside a
// covered range are legitimate (no transfer happened), never an error.
func (a *ChainFactsAdapter) applyLogCoverage(b *ChainFactsBundle, q ChainFactsQuery, logProgress indexer.ReliableProgress, meta chainSourceMeta) {
	if !q.NeedTransferLogs {
		return
	}
	if !logProgress.HasProgress {
		b.mark(ChainFactsIncomplete, ChainReasonLogNotCovered)
		return
	}
	b.Coverage.HasLogCheckpoint = true
	b.Coverage.LogCheckpointNext = int64(logProgress.LastHeight) + 1
	b.Coverage.LogCheckpointStart = meta.start
	b.Coverage.LogConfigHash = logProgress.Hash
	beforeStart := meta.present && q.From < meta.start
	switch {
	case beforeStart:
		b.mark(ChainFactsIncomplete, ChainReasonLogRangeBeforeStart)
	case int64(logProgress.LastHeight) < q.To:
		b.mark(ChainFactsIncomplete, ChainReasonLogNotCovered)
	default:
		b.Coverage.LogRangeCovered = true
	}
	if b.Coverage.LogPause != nil {
		b.mark(ChainFactsIncomplete, ChainReasonLogStreamPaused)
	}
}

// applyRecovery downgrades evidence that lies in a provisional or
// reconcile-held recovery region and flags invalidated sweep overlap. It never
// triggers recovery (FR-023).
func (a *ChainFactsAdapter) applyRecovery(b *ChainFactsBundle, snapshot indexer.RecoverySnapshot, released bool, q ChainFactsQuery) {
	b.Recovery.Released = released
	if snapshot.Row == nil {
		return
	}
	row := snapshot.Row
	b.Recovery.Active = true
	b.Recovery.RecoveryID = row.RecoveryID
	b.Recovery.Phase = row.Phase
	b.Recovery.PolicySeq = row.PolicySeq
	b.Recovery.Seq = row.Seq
	b.Recovery.MaxDepth = row.MaxDepth
	b.Recovery.BoundOldNumber = row.BoundOldNumber
	b.Recovery.BoundOldHash = row.BoundOldHash
	b.Recovery.AncestorNumber = row.AncestorNumber
	b.Recovery.SweepEnd = snapshot.SweepEnd
	b.Recovery.Depth = snapshot.Depth
	b.Recovery.Reconcile = snapshot.Reconcile
	b.Recovery.State, b.Recovery.Validity = indexer.AnnotateRecoveryHeight(row, released, q.To)

	switch b.Recovery.Validity {
	case indexer.ValidityUnknownPaused:
		b.mark(ChainFactsUnknown, ChainReasonRecoveryReconcile)
	case indexer.ValidityProvisionalReplaying:
		b.mark(ChainFactsIncomplete, ChainReasonRecoveryProvisional)
	}
	if row.AncestorNumber != nil && snapshot.SweepEnd != nil &&
		q.To > *row.AncestorNumber && q.From <= *snapshot.SweepEnd {
		b.mark(ChainFactsIncomplete, ChainReasonRecoverySweepIntersects)
	}
}

// applyFreshness records source ages and the live tip, then enforces both
// tolerances: an older-than-tolerance source or a lagging tip is stale
// (FR-005), and stale evidence never triggers automatic repair.
func (a *ChainFactsAdapter) applyFreshness(b *ChainFactsBundle, headerMeta, logMeta chainSourceMeta, q ChainFactsQuery,
	live chainLiveObservation, liveChecked bool, now time.Time) {

	b.Freshness.MaxSourceAge = a.cfg.MaxSourceAge
	b.Freshness.MaxTipLag = a.cfg.MaxTipLag
	needLogSource := q.NeedTransferLogs
	b.Freshness.HeaderUpdatedAt = headerMeta.updatedAt
	if needLogSource {
		b.Freshness.LogUpdatedAt = logMeta.updatedAt
	}
	sourceAge := time.Duration(0)
	ageKnown := false
	if headerMeta.present && headerMeta.updatedAt != nil {
		sourceAge = chainAgeAt(*headerMeta.updatedAt, now)
		ageKnown = true
	}
	if needLogSource && logMeta.present && logMeta.updatedAt != nil {
		if age := chainAgeAt(*logMeta.updatedAt, now); age > sourceAge {
			sourceAge = age
		}
		ageKnown = true
	}
	if ageKnown {
		b.Freshness.SourceAge = sourceAge
		if sourceAge > a.cfg.MaxSourceAge {
			b.Freshness.Stale = true
			b.mark(ChainFactsStale, ChainReasonSourceAgeExceeds)
		}
	}

	if liveChecked {
		b.Freshness.LiveChecked = true
		b.Freshness.TipHeight = live.tipHeight
		b.Freshness.TipHash = live.tipHash
		if live.tipHeight != nil && b.Coverage.HasHeaderCheckpoint {
			lag := max(*live.tipHeight-b.Coverage.HeaderCheckpointHeight, 0)
			b.Freshness.TipLagHeights = &lag
			if lag > a.cfg.MaxTipLag {
				b.Freshness.Stale = true
				b.mark(ChainFactsStale, ChainReasonTipLagExceeds)
			}
		}
	}
}

// applyConfirmation records the confirm basis and enforces the gate. Without a
// policy threshold or with the anchor below the threshold the evidence is
// incomplete and can never support consistency.
func (a *ChainFactsAdapter) applyConfirmation(b *ChainFactsBundle, q ChainFactsQuery, header indexer.ReliableProgress, live chainLiveObservation) {
	b.Confirmation.ThresholdN = q.ConfirmThresholdN
	if q.ConfirmThresholdN == 0 {
		b.mark(ChainFactsIncomplete, ChainReasonConfirmBasisMissing)
		return
	}
	tip, tipHash, tipSource, ok := chainConfirmationTip(header, live)
	if !ok {
		b.mark(ChainFactsIncomplete, ChainReasonConfirmBasisMissing)
		return
	}
	b.Confirmation.TipHeight = &tip
	b.Confirmation.TipHash = tipHash
	b.Confirmation.TipSource = tipSource
	if tip < 0 || q.To < 0 {
		// Unrepresentable height: never cast a negative into the unsigned
		// confirm math; refuse conservatively.
		b.mark(ChainFactsUnknown, ChainReasonSourceReadFailed)
		return
	}
	if uint64(q.To) > uint64(tip) {
		b.mark(ChainFactsIncomplete, ChainReasonConfirmTipBelowRange)
		return
	}
	confirmations, err := indexer.ExactConfirmations(uint64(tip), uint64(q.To))
	if err != nil {
		b.mark(ChainFactsUnknown, ChainReasonSourceReadFailed)
		return
	}
	b.Confirmation.Confirmations = confirmations
	b.Confirmation.Reached = indexer.ConfirmationReached(uint64(tip), uint64(q.To), q.ConfirmThresholdN)
	b.Confirmation.Known = true
	if !b.Confirmation.Reached {
		b.mark(ChainFactsIncomplete, ChainReasonBelowConfirmThreshold)
	}
}

// applyUpstream enforces FR-006: with any unconnected (or missing) upstream
// receipt declaration the bundle stays incomplete, and even a fully connected
// declaration never proves upstream success (ExternalLedgerProvable stays
// false).
func (a *ChainFactsAdapter) applyUpstream(b *ChainFactsBundle, q ChainFactsQuery) {
	if len(q.UpstreamReceipts) == 0 {
		b.mark(ChainFactsIncomplete, ChainReasonUpstreamUnconnected)
		return
	}
	for _, item := range q.UpstreamReceipts {
		if !item.Connected {
			b.mark(ChainFactsIncomplete, ChainReasonUpstreamUnconnected)
			return
		}
	}
}

// applyEvidenceRefs records the provenance of the stream/recovery/confirmation
// reads. Block and log refs are recorded per fact; adding the same physical
// source twice is deduplicated by ChainEvidenceSet. Refs are constructed from
// fixed tokens and stored sizes, so a validation failure is impossible here;
// addEvidence still surfaces one if that invariant ever breaks.
func (a *ChainFactsAdapter) applyEvidenceRefs(b *ChainFactsBundle,
	header, logProgress indexer.ReliableProgress, snapshot indexer.RecoverySnapshot, released bool,
	live chainLiveObservation, liveChecked bool) {

	for _, block := range b.Blocks {
		_ = b.addEvidence(ChainSourceCanonicalBlock,
			fmt.Sprintf("chain=%d/block=%d/%s", b.ChainID, block.Number, block.Hash))
	}
	for _, log := range b.Logs {
		_ = b.addEvidence(ChainSourceTransferLog,
			fmt.Sprintf("chain=%d/block=%d/%s/log=%s/%d",
				b.ChainID, log.BlockNumber, log.BlockHash, log.TxHash, log.LogIndex))
	}
	if header.HasProgress {
		_ = b.addEvidence(ChainSourceHeaderCheckpoint,
			fmt.Sprintf("chain=%d/header_checkpoint=%d/%s", b.ChainID, header.LastHeight, header.Hash))
	}
	if logProgress.HasProgress {
		_ = b.addEvidence(ChainSourceLogCheckpoint,
			fmt.Sprintf("chain=%d/log_checkpoint=%d/%s", b.ChainID, logProgress.LastHeight, logProgress.Hash))
	}
	if snapshot.Row != nil {
		_ = b.addEvidence(ChainSourceRecoverySnapshot,
			fmt.Sprintf("chain=%d/recovery=%s/%d/%s", b.ChainID, snapshot.Row.RecoveryID, snapshot.Row.Seq, snapshot.Row.Phase))
	} else {
		_ = b.addEvidence(ChainSourceRecoverySnapshot,
			fmt.Sprintf("chain=%d/recovery=none/released=%t", b.ChainID, released))
	}
	if b.Confirmation.Known && b.Confirmation.TipHeight != nil {
		_ = b.addEvidence(ChainSourceConfirmationBasis,
			fmt.Sprintf("chain=%d/confirm_tip=%d/%s", b.ChainID, *b.Confirmation.TipHeight, b.Confirmation.TipHash))
	}
	if liveChecked && live.tipHeight != nil {
		_ = b.addEvidence(ChainSourceLiveHeader,
			fmt.Sprintf("chain=%d/live_tip=%d/%s", b.ChainID, *live.tipHeight, live.tipHash))
	}
}

// chainSourceMeta is the extra 002/003 checkpoint metadata freshness needs
// (start position and updated_at) that indexer.ReliableProgress does not carry.
type chainSourceMeta struct {
	present   bool
	start     int64
	updatedAt *time.Time
}

// readSourceMeta reads one checkpoint's start position and updated_at.
func (a *ChainFactsAdapter) readSourceMeta(ctx context.Context, sql string, chainID int64) (chainSourceMeta, error) {
	var (
		meta    chainSourceMeta
		updated time.Time
	)
	err := a.pool.QueryRow(ctx, sql, chainID).Scan(&meta.start, &updated)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return chainSourceMeta{}, nil
	case err != nil:
		return chainSourceMeta{}, err
	}
	meta.present = true
	meta.updatedAt = &updated
	return meta, nil
}

// readPause reads one stream's durable pause row; no row means not paused.
func (a *ChainFactsAdapter) readPause(ctx context.Context, sql string, chainID int64, stream string) (*ChainPause, error) {
	pause := ChainPause{Stream: stream}
	err := a.pool.QueryRow(ctx, sql, chainID).Scan(&pause.Height, &pause.Kind, &pause.Detail)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return nil, nil
	case err != nil:
		return nil, err
	}
	return &pause, nil
}

// readBlocks reads the chain_blocks rows of the bounded range in height order.
func (a *ChainFactsAdapter) readBlocks(ctx context.Context, q ChainFactsQuery) ([]ChainFactBlock, error) {
	rows, err := a.pool.Query(ctx, chainFactsBlocksSQL, q.ChainID, q.From, q.To)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ChainFactBlock
	for rows.Next() {
		var block ChainFactBlock
		if err := rows.Scan(&block.Number, &block.Hash, &block.ParentHash, &block.Canonical, &block.IndexedAt); err != nil {
			return nil, err
		}
		out = append(out, block)
	}
	return out, rows.Err()
}

// readLogs reads the erc20_transfer_logs rows of the bounded range in a stable
// order. Payloads stay raw hex; no amount is parsed (integer rule). topic1 and
// topic2 are read as well and their low-20-byte address form is exposed as
// From/To (empty when the topic is not zero-padded, never guessed).
func (a *ChainFactsAdapter) readLogs(ctx context.Context, q ChainFactsQuery) ([]ChainFactLog, error) {
	rows, err := a.pool.Query(ctx, chainFactsLogsSQL, q.ChainID, q.From, q.To)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ChainFactLog
	for rows.Next() {
		var (
			log            ChainFactLog
			topic1, topic2 string
		)
		if err := rows.Scan(&log.BlockNumber, &log.BlockHash, &log.TxHash, &log.LogIndex,
			&log.Contract, &log.Topic0, &topic1, &topic2, &log.Data); err != nil {
			return nil, err
		}
		log.Topic1, log.Topic2 = topic1, topic2
		if from, ok := chainTopicAddress(topic1); ok {
			log.From = from
		}
		if to, ok := chainTopicAddress(topic2); ok {
			log.To = to
		}
		out = append(out, log)
	}
	return out, rows.Err()
}

// chainTopicAddress decodes one 32-byte indexed topic into its canonical
// lowercase 0x-prefixed 20-byte address. It returns ok=false for a malformed
// shape or a non-zero high 12 bytes (a non-address topic) instead of guessing,
// so an unattributable direction can never be read as a project address.
func chainTopicAddress(topic string) (string, bool) {
	trimmed := strings.ToLower(strings.TrimSpace(topic))
	if len(trimmed) != 66 || !strings.HasPrefix(trimmed, "0x") {
		return "", false
	}
	body := trimmed[2:]
	if strings.Trim(body[:24], "0") != "" {
		return "", false
	}
	for _, r := range body {
		switch {
		case r >= '0' && r <= '9', r >= 'a' && r <= 'f':
		default:
			return "", false
		}
	}
	return "0x" + body[24:], true
}

// chainLiveObservation is one optional live header read: the tip plus any
// contradiction between the live chain and the durable identity.
type chainLiveObservation struct {
	tipHeight      *int64
	tipHash        string
	anchorMismatch bool
	reason         string
}

// liveObserve performs the bounded read-only live check: chain-id gate, latest
// tip, and (when a durable anchor hash exists) the anchor header. It never
// scans, commits or triggers recovery, and it runs outside any DB transaction
// (this adapter opens none).
func (a *ChainFactsAdapter) liveObserve(ctx context.Context, q ChainFactsQuery, anchorHash string, header indexer.ReliableProgress) (chainLiveObservation, error) {
	rctx, cancel := context.WithTimeout(ctx, a.cfg.RPCTimeout)
	defer cancel()

	chainID, err := a.cfg.LiveHeader.ChainID(rctx)
	if err != nil {
		return chainLiveObservation{reason: ChainReasonLiveCheckFailed}, err
	}
	if chainID == nil || chainID.Cmp(big.NewInt(q.ChainID)) != 0 {
		return chainLiveObservation{reason: ChainReasonLiveChainIDMismatch},
			fmt.Errorf("live chain id %v does not match configured chain id %d", chainID, q.ChainID)
	}

	tip, err := a.cfg.LiveHeader.HeaderByNumber(rctx, nil)
	if err != nil {
		return chainLiveObservation{reason: ChainReasonLiveCheckFailed}, err
	}
	if tip == nil || tip.Number == nil {
		return chainLiveObservation{reason: ChainReasonLiveCheckFailed},
			errors.New("live header check returned no tip header")
	}
	observation := chainLiveObservation{tipHash: strings.ToLower(tip.Hash().Hex())}
	tipNumber := tip.Number.Int64()
	observation.tipHeight = &tipNumber

	if anchorHash != "" {
		anchor, err := a.cfg.LiveHeader.HeaderByNumber(rctx, big.NewInt(q.To))
		if err != nil {
			return chainLiveObservation{reason: ChainReasonLiveCheckFailed}, err
		}
		if anchor == nil {
			return chainLiveObservation{reason: ChainReasonLiveCheckFailed},
				fmt.Errorf("live header check returned no header at height %d", q.To)
		}
		if liveHash := strings.ToLower(anchor.Hash().Hex()); liveHash != strings.ToLower(anchorHash) {
			observation.anchorMismatch = true
			observation.reason = ChainReasonLiveHeaderMismatch
		}
	}
	// A durable tip at the live tip height with a different hash is a tip-level
	// reorg: the recorded identity is already gone even before a range anchor
	// mismatch would show.
	if header.HasProgress && !observation.anchorMismatch &&
		int64(header.LastHeight) == tipNumber && strings.ToLower(header.Hash) != observation.tipHash {
		observation.anchorMismatch = true
		observation.reason = ChainReasonLiveHeaderMismatch
	}
	return observation, nil
}

// chainConfirmationTip chooses the tip for the confirm gate: the live tip when the
// check succeeded, else the durable header checkpoint tip. ok=false means no
// usable tip exists.
func chainConfirmationTip(header indexer.ReliableProgress, live chainLiveObservation) (int64, string, ChainFactSource, bool) {
	if live.tipHeight != nil {
		return *live.tipHeight, live.tipHash, ChainSourceLiveHeader, true
	}
	if header.HasProgress {
		return int64(header.LastHeight), header.Hash, ChainSourceHeaderCheckpoint, true
	}
	return 0, "", "", false
}

// chainBlockAt returns the row at the requested height, if observed.
func chainBlockAt(blocks []ChainFactBlock, number int64) *ChainFactBlock {
	for i := range blocks {
		if blocks[i].Number == number {
			return &blocks[i]
		}
	}
	return nil
}

// chainAgeAt computes a non-negative source age (future timestamps clamp to zero).
func chainAgeAt(updated, now time.Time) time.Duration {
	if updated.After(now) {
		return 0
	}
	return now.Sub(updated)
}

// Read-only SQL. Column names follow migrations 000002/000003; no statement
// writes, locks or advances anything. The adapter opens no transaction at all,
// so no DB transaction can ever span the optional slow header RPC.
const (
	chainHeaderMetaSQL = `
SELECT start_height, updated_at
FROM indexer_checkpoint
WHERE chain_id = $1`

	chainLogMetaSQL = `
SELECT start_block, updated_at
FROM log_checkpoint
WHERE chain_id = $1`

	chainHeaderPauseSQL = `
SELECT height, kind, COALESCE(detail, '')
FROM indexer_pause
WHERE chain_id = $1`

	chainLogPauseSQL = `
SELECT height, kind, COALESCE(detail, '')
FROM log_pause
WHERE chain_id = $1`

	chainFactsBlocksSQL = `
SELECT number, hash, parent_hash, canonical, indexed_at
FROM chain_blocks
WHERE chain_id = $1 AND number BETWEEN $2 AND $3
ORDER BY number`

	chainFactsLogsSQL = `
SELECT block_number, block_hash, tx_hash, log_index, contract, topic0, topic1, topic2, data
FROM erc20_transfer_logs
WHERE chain_id = $1 AND block_number BETWEEN $2 AND $3
ORDER BY block_number, block_hash, tx_hash, log_index`
)
