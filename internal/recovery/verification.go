// verification.go implements T038 [US3]: the V1-V9 verification orchestration
// and persistence of FR-014/015/016/017/018/020 (data-model.md
// §1.4/§1.5/§4.3/§5; contracts/verification-items.md §1; tasks.md T038).
//
// One bounded `Verify` step
//
//  1. reads the requested read-only sources (the V1-V9 adapters injected as
//     VerificationSource; the heavy adapters live in
//     internal/recovery/sources/, T039/T040, never in this root package),
//  2. groups their SourceObservations per stable object identity
//     (category + object_key) and derives each item's conclusion with the
//     pure EvaluateConclusion rule,
//  3. persists the accepted batch through the T013 generation protocol
//     (CommitEvidenceWrite, MutationVerificationBatch): one
//     recovery_evidence(verification_batch) batch record, one append-only
//     recovery_verification_item row per object, one open recovery_gap row
//     per observation that asks for a gap, and the audit rows.
//
// Hard boundaries (all fail-closed):
//
//   - Read-only sources: the VerificationSource interface exposes exactly
//     Category() and Observe(ctx). There is no send/sign/broadcast/replay/
//     publish/recreate entry point anywhere on this surface, so verification
//     and everything it triggers can never produce a payment, signature,
//     broadcast, replay or real downstream delivery (FR-020, F10). The
//     orchestrator itself touches only the control store; the adapters own
//     the read-only data-DB/chain access.
//   - Conclusion rules (contracts/verification-items.md §1): consistent only
//     when the declared sources agree, the evidence is complete, the coverage
//     is closed and every observation is fresh inside the configured
//     tolerance; any unknown source caps the item at unknown; unknown/stale
//     never pass; a missing record is unknown plus an evidence gap and is
//     never "never happened", "never paid" or safe to re-execute; nothing is
//     filled in from a default, a prior verdict or a clock (FR-015/016/018).
//   - Freshness tolerance and the verification batch bound are deployment
//     configuration (VerificationFreshnessConfigPrefix /
//     VerificationBatchLimitConfigKey): a missing tolerance keeps every
//     conclusion conservative (unknown) and is never replaced by a default;
//     a missing bound refuses construction. This package reads no
//     environment: the assembly layer resolves the values and injects them
//     (ResolveVerificationFreshness / ResolveVerificationBatchLimit).
//   - The control store is reached only through a *controlstore.Store built
//     by controlstore.NewStore, so the T069 schema-version guard (unknown or
//     incompatible version => control_store_unavailable) is inherited with no
//     unguarded read path.
//   - Re-verification is append-only: every accepted batch appends new item
//     rows bound to the accepted generation (latest-row-wins only after the
//     data-model §5 token check); nothing is updated or deleted. A repeated
//     operation_id reads the recorded batch back without a second batch.
package recovery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/xtianxx/txharbor/internal/logx"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

// ActionVerification is the recovery_audit action of every verification
// attempt: accepted batches, refusals (bound/observation/instance) and the
// generation protocol's own discard rows are recorded under their own actions.
const ActionVerification = "verification"

const (
	// VerificationFreshnessConfigPrefix mirrors
	// config.EnvRecoveryEvidenceFreshnessPrefix (keep the two in sync): the
	// per-category freshness tolerance keys are <prefix><CATEGORY>, for example
	// TXHARBOR_RECOVERY_EVIDENCE_FRESHNESS_V1. No package-level read happens
	// here; ResolveVerificationFreshness is the resolution helper the assembly
	// layer uses, and a missing/non-positive value stays conservative
	// (unknown), never a pass.
	VerificationFreshnessConfigPrefix = "TXHARBOR_RECOVERY_EVIDENCE_FRESHNESS_"

	// VerificationBatchLimitConfigKey is the deployment key for the per-batch
	// item bound. The bound has no default: NewVerification refuses a
	// non-positive BatchLimit, and ResolveVerificationBatchLimit refuses a
	// missing/non-positive configured value by name.
	VerificationBatchLimitConfigKey = "TXHARBOR_RECOVERY_VERIFICATION_BATCH_LIMIT"
)

var (
	// ErrUnknownVerificationCategory marks a string outside the closed V1-V9
	// category set. It is a contract error, not a verification refusal: an
	// unknown category must never be mapped onto a default.
	ErrUnknownVerificationCategory = errors.New("unknown verification category")
	// ErrUnknownVerificationConclusion marks a string outside the closed
	// consistent/divergent/unknown/stale conclusion set.
	ErrUnknownVerificationConclusion = errors.New("unknown verification conclusion")
	// ErrVerificationRequest marks a malformed verification request (missing
	// actor, operation_id, scope or sources, or a source reporting an unknown
	// category). The request is never turned into a default; it is refused
	// and, when an instance/actor is identifiable, audited.
	ErrVerificationRequest = errors.New("verification request is invalid")
	// ErrVerificationObservation marks a source observation that violates the
	// read-only observation contract (missing object_key, non-JSON scope or
	// sources payload, or an incomplete FR-019 gap bundle). Nothing is
	// persisted and the batch is refused.
	ErrVerificationObservation = errors.New("verification source observation is invalid")
	// ErrVerificationBound marks a batch refused because the collected
	// observations exceed the configured bound. Nothing is persisted (one
	// refusal audit row); the caller narrows the scope and steps again.
	ErrVerificationBound = errors.New("verification batch exceeds the configured bound")
)

// ---------------------------------------------------------------------------
// Closed category set (V1-V9)
// ---------------------------------------------------------------------------

// VerificationCategory is one member of the closed V1-V9 verification
// catalogue of contracts/verification-items.md §1. Values are frozen
// vocabulary: they appear verbatim in recovery_verification_item.category and
// in audit details, so renaming one is a specification change.
type VerificationCategory string

// The nine verification categories (contracts/verification-items.md §1).
const (
	// VerificationV1: chain facts vs PG (canonical RPC vs chain/deposit
	// tables and 006 reorg recovery).
	VerificationV1 VerificationCategory = "V1"
	// VerificationV2: withdrawal requests / payment intents / authorizations
	// (007 audit included).
	VerificationV2 VerificationCategory = "V2"
	// VerificationV3: nonce allocation and occupancy (008 tables + chain
	// transaction count).
	VerificationV3 VerificationCategory = "V3"
	// VerificationV4: signing / broadcast results including unknown
	// (009/010 tables + chain receipts).
	VerificationV4 VerificationCategory = "V4"
	// VerificationV5: outbox and event obligations (000017) plus publisher
	// progress.
	VerificationV5 VerificationCategory = "V5"
	// VerificationV6: consumer idempotency / progress / quarantine plus the
	// broker committed offset when readable.
	VerificationV6 VerificationCategory = "V6"
	// VerificationV7: 014 reconciliation / disposition / permission / audit
	// reads.
	VerificationV7 VerificationCategory = "V7"
	// VerificationV8: authorization-surface drift evidence.
	VerificationV8 VerificationCategory = "V8"
	// VerificationV9: tool and dependency readiness.
	VerificationV9 VerificationCategory = "V9"
)

// knownVerificationCategories is the canonical V1-V9 order; it is the
// catalogue order of contracts/verification-items.md §1 and display/validation
// only.
var knownVerificationCategories = []VerificationCategory{
	VerificationV1, VerificationV2, VerificationV3, VerificationV4, VerificationV5,
	VerificationV6, VerificationV7, VerificationV8, VerificationV9,
}

// Known reports whether c is one of the nine verification categories.
func (c VerificationCategory) Known() bool {
	return slices.Contains(knownVerificationCategories, c)
}

// KnownVerificationCategories returns the closed V1-V9 set in catalogue order.
// The caller receives a fresh slice and may mutate it freely.
func KnownVerificationCategories() []VerificationCategory {
	return slices.Clone(knownVerificationCategories)
}

// ParseVerificationCategory maps s onto the closed V1-V9 set. Matching is
// exact: unknown, empty, differently-cased or whitespace-padded input is
// refused with ErrUnknownVerificationCategory (never defaulted).
func ParseVerificationCategory(raw string) (VerificationCategory, error) {
	c := VerificationCategory(raw)
	if !c.Known() {
		return "", fmt.Errorf("%w: %q (known: %v)", ErrUnknownVerificationCategory, raw, knownVerificationCategories)
	}
	return c, nil
}

// ---------------------------------------------------------------------------
// Closed conclusion set
// ---------------------------------------------------------------------------

// VerificationConclusion is one member of the closed verification conclusion
// set of data-model.md §1.5. Passes() is the verdict's only pass rule:
// everything except consistent fails closed (FR-018).
type VerificationConclusion string

// The four conclusions (data-model.md §1.5, contracts/verification-items.md §1).
const (
	// ConclusionConsistent: evidence complete, fresh inside the configured
	// tolerance, coverage closed and every source in agreement.
	ConclusionConsistent VerificationConclusion = "consistent"
	// ConclusionDivergent: at least one source explicitly contradicts the
	// local state; the difference must be reported (FR-015).
	ConclusionDivergent VerificationConclusion = "divergent"
	// ConclusionUnknown: a source could not conclude, a record is missing, or
	// complete/fresh/closed evidence cannot be proven. Never a pass.
	ConclusionUnknown VerificationConclusion = "unknown"
	// ConclusionStale: the evidence exists but is older than the configured
	// freshness tolerance. Never a pass.
	ConclusionStale VerificationConclusion = "stale"
)

// knownVerificationConclusions is the canonical order; it is the wording order
// of data-model.md §1.5 and display/validation only.
var knownVerificationConclusions = []VerificationConclusion{
	ConclusionConsistent, ConclusionDivergent, ConclusionUnknown, ConclusionStale,
}

// Known reports whether c is one of the four conclusions.
func (c VerificationConclusion) Known() bool {
	return slices.Contains(knownVerificationConclusions, c)
}

// Passes reports whether the conclusion may count as passed. Only consistent
// passes; unknown and stale never do (FR-018).
func (c VerificationConclusion) Passes() bool {
	return c == ConclusionConsistent
}

// KnownVerificationConclusions returns the closed conclusion set in canonical
// order. The caller receives a fresh slice and may mutate it freely.
func KnownVerificationConclusions() []VerificationConclusion {
	return slices.Clone(knownVerificationConclusions)
}

// ParseVerificationConclusion maps s onto the closed conclusion set. Matching
// is exact: unknown, empty, differently-cased or whitespace-padded input is
// refused with ErrUnknownVerificationConclusion (never defaulted).
func ParseVerificationConclusion(raw string) (VerificationConclusion, error) {
	c := VerificationConclusion(raw)
	if !c.Known() {
		return "", fmt.Errorf("%w: %q (known: %v)", ErrUnknownVerificationConclusion, raw, knownVerificationConclusions)
	}
	return c, nil
}

// ---------------------------------------------------------------------------
// Conclusion rule (pure)
// ---------------------------------------------------------------------------

// SourceObservation is one read-only source's observation of one object. The
// adapter declares its own verdict; EvaluateConclusion re-derives the item
// verdict from the declared observations and can only downgrade it.
//
// Scope and Sources are JSON payloads (validated by Verify when non-empty):
// Scope identifies the observed range, Sources carries the per-source read
// values/locations/times the conclusion was derived from. GapEvidence, when
// non-nil, asks for an FR-019 evidence gap for this object: the item is capped
// at unknown (a missing/unprovable record is never "never happened") and one
// open recovery_gap row is established in the same accepted batch.
type SourceObservation struct {
	ObjectKey    string
	Scope        []byte
	Sources      []byte
	Conclusion   VerificationConclusion
	Reason       string
	EvidenceRefs []string
	ObservedAt   time.Time
	GapEvidence  *GapEvidence
}

// GapEvidence is the FR-019 evidence bundle of one missing/unprovable record.
// Every component except Risk is persisted into the corresponding
// recovery_gap column; Risk has no dedicated column in the 0001 schema and is
// carried inside required_evidence under the reserved layout marker (see
// mergeGapRiskPayload/splitGapRiskPayload) so it round-trips for human
// handling.
type GapEvidence struct {
	Timeline             []byte
	ExistingEvidence     []byte
	RequiredEvidence     []byte
	Risk                 []byte
	AffectedCapabilities []Capability
}

// ConclusionInput is one pure conclusion evaluation. The evaluator consumes
// Now and Tolerance as declared inputs and never reads a clock, a store or the
// network.
type ConclusionInput struct {
	Category VerificationCategory
	// Sources are all observations of the same stable object identity.
	Sources []SourceObservation
	// EvidenceComplete and CoverageClosed are the declared completeness of the
	// consulted evidence. False refuses a consistent verdict; they are never
	// inferred or defaulted.
	EvidenceComplete bool
	CoverageClosed   bool
	// Missing marks an object with no record at the restore point: it is an
	// evidence gap (unknown), never an absence of the underlying event.
	Missing bool
	// Tolerance is the configured freshness tolerance. <= 0 (not configured)
	// keeps the verdict conservative; no default is substituted.
	Tolerance time.Duration
	// Now is the evaluation instant. The zero time means freshness cannot be
	// proven.
	Now time.Time
}

// EvaluateConclusion applies the conclusion rules of
// contracts/verification-items.md §1 to one object:
//
//   - divergent is reported explicitly when any source contradicts the local
//     state (FR-015); it outranks every other signal;
//   - missing records and any unknown source cap the item at unknown; unknown
//     outranks stale;
//   - consistent requires complete evidence, closed coverage, a configured
//     tolerance, an evaluation instant and every observation fresh inside the
//     tolerance with a plausible (non-zero, non-future) timestamp and no stale
//     source;
//   - stale is reported when the only blocking condition is age;
//   - a non-consistent verdict always carries a non-empty reason; nothing is
//     filled in from a default, a prior conclusion or a clock.
//
// The function is pure: the same declared inputs always produce the same
// verdict and reason.
func EvaluateConclusion(in ConclusionInput) (VerificationConclusion, string) {
	if in.Missing {
		return ConclusionUnknown, "the object has no record at the restore point; a missing record is not \"never happened\", " +
			"\"never paid\" or safe to re-execute (FR-016)"
	}
	if !in.Category.Known() {
		return ConclusionUnknown, fmt.Sprintf(
			"category %q is outside the closed V1-V9 set; refusing to conclude", in.Category)
	}
	if len(in.Sources) == 0 {
		return ConclusionUnknown, "no source observations: a conclusion without sources is not evidence"
	}

	var divergent, unknown []string
	for _, obs := range in.Sources {
		key := strings.TrimSpace(obs.ObjectKey)
		if key == "" {
			key = "unnamed object"
		}
		switch obs.Conclusion {
		case ConclusionConsistent, ConclusionStale:
			// Handled below (freshness) / passed through.
		case ConclusionDivergent:
			divergent = append(divergent, key)
		case ConclusionUnknown:
			unknown = append(unknown, key)
		default:
			unknown = append(unknown, fmt.Sprintf("%s (conclusion %q outside the closed set)", key, obs.Conclusion))
		}
	}
	if len(divergent) > 0 {
		return ConclusionDivergent, boundedEvidenceDetail(
			"source(s) " + strings.Join(divergent, ", ") +
				" explicitly contradict the local state; the difference is listed in the conclusion (FR-015)")
	}
	if len(unknown) > 0 {
		return ConclusionUnknown, boundedEvidenceDetail(
			"a source could not conclude (unknown caps the item at unknown): " + strings.Join(unknown, ", "))
	}
	if !in.EvidenceComplete {
		return ConclusionUnknown, "the evidence is not complete; completeness is never assumed"
	}
	if !in.CoverageClosed {
		return ConclusionUnknown, "the coverage is not closed; open coverage is never counted as consistent"
	}
	if in.Tolerance <= 0 {
		return ConclusionUnknown, "the freshness tolerance is not configured; a missing tolerance keeps the conclusion conservative (unknown)"
	}
	if in.Now.IsZero() {
		return ConclusionUnknown, "no evaluation instant was provided; freshness cannot be proven"
	}

	var stale []string
	for _, obs := range in.Sources {
		key := strings.TrimSpace(obs.ObjectKey)
		if key == "" {
			key = "unnamed object"
		}
		if obs.Conclusion == ConclusionStale {
			stale = append(stale, key+" (source reported stale)")
			continue
		}
		if obs.ObservedAt.IsZero() {
			return ConclusionUnknown, fmt.Sprintf(
				"source observation for %s carries no timestamp; it cannot be assumed fresh", key)
		}
		if obs.ObservedAt.After(in.Now) {
			return ConclusionUnknown, fmt.Sprintf(
				"source observation for %s carries a future timestamp; it cannot be trusted as fresh", key)
		}
		if in.Now.Sub(obs.ObservedAt) > in.Tolerance {
			stale = append(stale, fmt.Sprintf("%s (older than the %s freshness tolerance)", key, in.Tolerance))
		}
	}
	if len(stale) > 0 {
		return ConclusionStale, boundedEvidenceDetail(
			"the evidence is older than the configured freshness tolerance: " + strings.Join(stale, ", "))
	}
	return ConclusionConsistent, ""
}

// ---------------------------------------------------------------------------
// Read-only source interface (adapters: internal/recovery/sources/, T039/T040)
// ---------------------------------------------------------------------------

// VerificationSource is the only surface the orchestrator reads sources
// through: a closed category and one read-only observation call. It
// deliberately exposes no effectful method - adding one is a safety contract
// break (F4/FR-020), not an implementation detail. Adapters must never call a
// write path (for example a txlifecycle Reconcile/Release/Send) from Observe.
//
// An adapter MUST return ConclusionUnknown (never consistent) whenever its
// evidence is incomplete, its coverage is open, or a consulted source could
// not be read; EvaluateConclusion re-checks the rules and can only downgrade
// an observation, never upgrade one.
type VerificationSource interface {
	Category() VerificationCategory
	Observe(ctx context.Context) ([]SourceObservation, error)
}

// ---------------------------------------------------------------------------
// Gap record (shared with the T041 gaps service)
// ---------------------------------------------------------------------------

// GapState is one member of the closed recovery_gap.state set.
//
// The GapState vocabulary and the Gap record are declared here because T038's
// VerificationBatch exposes the open gaps it established; gaps.go (T041) must
// build its Gaps service on these same types and must not redeclare them.
type GapState string

// The three gap states (recovery_gap.state).
const (
	GapStateOpen      GapState = "open"
	GapStateClosed    GapState = "closed"
	GapStateEscalated GapState = "escalated"
)

// Known reports whether s is one of the three gap states.
func (s GapState) Known() bool {
	switch s {
	case GapStateOpen, GapStateClosed, GapStateEscalated:
		return true
	}
	return false
}

// Gap is one persisted recovery_gap row (data-model.md §1.6). Risk is carried
// inside the required_evidence JSONB payload (no dedicated column exists in
// the 0001 schema; see mergeGapRiskPayload/splitGapRiskPayload). ClosedAt is
// zero while the gap has no closure facts.
type Gap struct {
	GapID                string
	InstanceID           string
	ObjectKey            string
	Scope                []byte
	Timeline             []byte
	ExistingEvidence     []byte
	RequiredEvidence     []byte
	Risk                 []byte
	AffectedCapabilities []Capability
	DependencyProof      []byte
	State                GapState
	ClosedBy             string
	ClosedAt             time.Time
	ClosureEvidence      []byte
	Owner                string
	EscalationRef        string
	CreatedAt            time.Time
}

// verificationGapBundleLayout is the reserved layout marker under which the
// risk payload of a verification-triggered gap is carried inside the
// required_evidence column. A payload without this marker is the raw required
// evidence (the T041 gaps service should reuse these two helpers so both
// writers agree on one convention).
const verificationGapBundleLayout = "verification_gap_v1"

// mergeGapRiskPayload returns the JSONB value stored in
// recovery_gap.required_evidence: the required evidence payload unchanged when
// no risk payload is supplied, otherwise a documented wrapper
// {"layout":"verification_gap_v1","required_evidence":...,"risk":...}.
func mergeGapRiskPayload(requiredEvidence, risk []byte) ([]byte, error) {
	if len(risk) == 0 {
		return requiredEvidence, nil
	}
	merged, err := json.Marshal(map[string]any{
		"layout":            verificationGapBundleLayout,
		"required_evidence": json.RawMessage(requiredEvidence),
		"risk":              json.RawMessage(risk),
	})
	if err != nil {
		return nil, fmt.Errorf("encode gap risk bundle: %w", err)
	}
	return merged, nil
}

// splitGapRiskPayload reverses mergeGapRiskPayload: a raw required-evidence
// payload is returned unchanged with a nil risk; a marked wrapper is
// unwrapped.
func splitGapRiskPayload(raw []byte) (requiredEvidence, risk []byte) {
	var wrapper map[string]json.RawMessage
	if err := json.Unmarshal(raw, &wrapper); err != nil {
		return raw, nil
	}
	if string(wrapper["layout"]) != `"`+verificationGapBundleLayout+`"` {
		return raw, nil
	}
	return wrapper["required_evidence"], wrapper["risk"]
}

// ---------------------------------------------------------------------------
// Verification service
// ---------------------------------------------------------------------------

// VerificationOptions constructs one Verification. Tolerance and BatchLimit
// are deployment configuration (never defaults): Tolerance <= 0 is accepted
// but keeps every conclusion conservative, BatchLimit <= 0 refuses
// construction (there is no default bound).
type VerificationOptions struct {
	// Tolerance is the freshness tolerance in effect for every category
	// unless Freshness resolves a per-category value.
	Tolerance time.Duration
	// BatchLimit bounds how many source observations one bounded Verify step
	// may collect. It has no default; a non-positive value refuses
	// construction.
	BatchLimit int
	// Now is a test seam for the evaluation instant (nil means time.Now).
	Now func() time.Time
	// Freshness optionally resolves the per-category freshness tolerance
	// family (<prefix><CATEGORY>). A missing or non-positive category value
	// must stay conservative (return 0); an error also stays conservative.
	Freshness func(category VerificationCategory) (time.Duration, error)
}

// Verification is the V1-V9 orchestration service over a version-guarded
// control store. It holds no mutable state and touches only the control store.
type Verification struct {
	store      *controlstore.Store
	tolerance  time.Duration
	batchLimit int
	now        func() time.Time
	freshness  func(category VerificationCategory) (time.Duration, error)
}

// NewVerification builds the verification service over a *controlstore.Store
// (controlstore.NewStore enforces the T069 version guard, so there is no
// unguarded path). A nil store refuses, and a non-positive BatchLimit refuses
// by key name: the batch bound is deployment configuration with no default.
func NewVerification(store *controlstore.Store, opts VerificationOptions) (*Verification, error) {
	if store == nil {
		return nil, errors.New("verification requires a controlstore.Store built by controlstore.NewStore")
	}
	if opts.BatchLimit <= 0 {
		return nil, fmt.Errorf(
			"%s is required (not configured) and must be a positive observation bound; the verification has no default batch bound",
			VerificationBatchLimitConfigKey)
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	return &Verification{
		store:      store,
		tolerance:  opts.Tolerance,
		batchLimit: opts.BatchLimit,
		now:        now,
		freshness:  opts.Freshness,
	}, nil
}

// toleranceFor returns the freshness tolerance of one category. An unresolved
// per-category value stays 0, which EvaluateConclusion treats conservatively
// (unknown) - never a default pass.
func (v *Verification) toleranceFor(category VerificationCategory) time.Duration {
	if v.freshness == nil {
		return v.tolerance
	}
	resolved, err := v.freshness(category)
	if err != nil || resolved <= 0 {
		return 0
	}
	return resolved
}

// ResolveVerificationFreshness resolves the per-category freshness tolerance
// family <prefix><CATEGORY> from a lookup function (os.LookupEnv). A missing
// or blank value returns (0, nil): the category is simply not configured and
// stays conservative. A configured but unparsable/non-positive value is
// returned as an error by exact key name (the caller refuses or keeps the
// category conservative, but never substitutes a default).
func ResolveVerificationFreshness(lookup func(string) (string, bool), category VerificationCategory) (time.Duration, error) {
	if !category.Known() {
		return 0, fmt.Errorf("%w: %q", ErrUnknownVerificationCategory, category)
	}
	if lookup == nil {
		return 0, nil
	}
	key := VerificationFreshnessConfigPrefix + string(category)
	raw, ok := lookup(key)
	if !ok || strings.TrimSpace(raw) == "" {
		return 0, nil
	}
	value, err := time.ParseDuration(strings.TrimSpace(raw))
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s=%q is not a positive duration", key, raw)
	}
	return value, nil
}

// ResolveVerificationBatchLimit resolves VerificationBatchLimitConfigKey from
// a lookup function (os.LookupEnv). A missing, blank or non-positive value is
// refused by key name: the batch bound has no default.
func ResolveVerificationBatchLimit(lookup func(string) (string, bool)) (int, error) {
	if lookup == nil {
		return 0, fmt.Errorf("%s is required (not configured); the verification has no default batch bound",
			VerificationBatchLimitConfigKey)
	}
	raw, ok := lookup(VerificationBatchLimitConfigKey)
	if !ok || strings.TrimSpace(raw) == "" {
		return 0, fmt.Errorf("%s is required (not configured); the verification has no default batch bound",
			VerificationBatchLimitConfigKey)
	}
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s=%q is not a positive integer", VerificationBatchLimitConfigKey, raw)
	}
	return value, nil
}

// VerificationRequest is one bounded verification step. OperationID is the
// idempotency key: the same operation_id reads the recorded batch back
// without a second batch; a different request under the same operation_id
// refuses with controlstore.ErrOperationConflict and zero writes.
type VerificationRequest struct {
	InstanceID  string
	Actor       string
	Scope       []byte
	OperationID string
	Sources     []VerificationSource
}

// VerificationItem is one persisted recovery_verification_item row.
type VerificationItem struct {
	ItemID       string
	InstanceID   string
	Generation   int64
	Category     VerificationCategory
	ObjectKey    string
	Scope        []byte
	Sources      []byte
	Conclusion   VerificationConclusion
	Reason       string
	EvidenceRefs []string
	ObservedAt   time.Time
	CreatedAt    time.Time
}

// VerificationBatch is one bounded step's outcome. Discarded reports that the
// captured evidence token no longer matched the instance (a concurrent writer
// committed first): the protocol wrote only its discard audit row and the
// caller re-runs a fresh step (bounded stepping converges). BatchID is the
// recovery_evidence row of the accepted batch.
type VerificationBatch struct {
	InstanceID string
	BatchID    string
	Generation int64
	Discarded  bool
	Items      []VerificationItem
	Gaps       []Gap
}

// verificationPendingGap is one gap to establish inside the accepted batch.
type verificationPendingGap struct {
	objectKey string
	scope     []byte
	evidence  GapEvidence
}

// verificationPendingItem is one item plus the optional gap it establishes.
type verificationPendingItem struct {
	item VerificationItem
	gap  *verificationPendingGap
}

// Verify runs one bounded read-only verification step and persists the
// accepted batch.
//
// Order of operations (data-model.md §5): the evidence token is captured
// before the sources are read; the sources are read outside any transaction;
// the result is committed through CommitEvidenceWrite, which takes the
// instance row lock, re-validates the token (a mismatch discards the attempt
// with only the protocol's discard audit row) and advances the evidence
// generation by exactly one for the accepted batch. A refused batch (bound,
// malformed observation, closed instance) persists nothing but its refusal
// audit row.
func (v *Verification) Verify(ctx context.Context, req VerificationRequest) (VerificationBatch, error) {
	if v == nil || v.store == nil {
		return VerificationBatch{}, errors.New("verification requires a controlstore.Store built by controlstore.NewStore")
	}
	instanceID, err := verificationInstanceID(req.InstanceID)
	if err != nil {
		return VerificationBatch{}, err
	}
	actor := strings.TrimSpace(req.Actor)
	if actor == "" {
		return VerificationBatch{}, fmt.Errorf("%w: actor is required; the authenticated principal is recorded on every audit row", ErrVerificationRequest)
	}
	operationID := strings.TrimSpace(req.OperationID)
	if operationID == "" {
		return VerificationBatch{}, fmt.Errorf("%w: operation_id is required (idempotent bounded stepping)", ErrVerificationRequest)
	}
	requestScope, err := normalizeVerificationScope(req.Scope)
	if err != nil {
		return VerificationBatch{}, err
	}
	if len(req.Sources) == 0 {
		return VerificationBatch{}, fmt.Errorf("%w: at least one read-only VerificationSource is required", ErrVerificationRequest)
	}
	categories := make([]string, 0, len(req.Sources))
	for _, source := range req.Sources {
		if source == nil {
			return VerificationBatch{}, fmt.Errorf("%w: a nil VerificationSource was supplied", ErrVerificationRequest)
		}
		category := source.Category()
		if !category.Known() {
			return VerificationBatch{}, fmt.Errorf("%w: source reports category %q outside the closed V1-V9 set", ErrUnknownVerificationCategory, category)
		}
		categories = append(categories, string(category))
	}
	slices.Sort(categories)
	categories = slices.Compact(categories)

	// Idempotent read-back: a recorded accepted batch for this operation_id is
	// returned without any write; a different request under the same
	// operation_id is a conflict with zero writes.
	recorded, found, err := v.recordedBatch(ctx, instanceID, operationID)
	if err != nil {
		return VerificationBatch{}, err
	}
	if found {
		matches, err := recorded.matchesRequest(actor, requestScope, categories)
		if err != nil {
			return VerificationBatch{}, err
		}
		if !matches {
			return VerificationBatch{}, fmt.Errorf(
				"%w: operation_id %q was already used by a different verification request",
				controlstore.ErrOperationConflict, operationID)
		}
		return v.loadBatch(ctx, instanceID, recorded)
	}

	kind, err := v.instanceKind(ctx, instanceID)
	if err != nil {
		return VerificationBatch{}, err
	}
	if kind != "recovery" {
		v.auditVerification(ctx, instanceID, actor, operationID, controlstore.AuditRefused,
			fmt.Sprintf("instance kind=%s is not a recovery instance; checklist/verification items apply to recovery instances", kind), nil)
		return VerificationBatch{}, fmt.Errorf("%w: instance %s has kind=%s", ErrVerificationRequest, instanceID, kind)
	}
	token, err := CaptureEvidenceToken(ctx, v.store.Pool(), instanceID)
	if err != nil {
		return VerificationBatch{}, err
	}
	if token.State != "open" {
		v.auditVerification(ctx, instanceID, actor, operationID, controlstore.AuditRefused,
			fmt.Sprintf("instance state=%s is not open; verification requires an open instance", token.State), nil)
		return VerificationBatch{}, fmt.Errorf("%w: %s", controlstore.ErrInstanceNotOpen, token.InstanceID)
	}

	evaluatedAt := v.now()
	prepared, err := v.collect(ctx, instanceID, req, requestScope, evaluatedAt)
	if err != nil {
		// Read failures and refusals leave no result rows: one refusal audit
		// row and an error; the caller narrows the scope and steps again.
		v.auditVerification(ctx, instanceID, actor, operationID, controlstore.AuditRefused, err.Error(), nil)
		return VerificationBatch{}, err
	}

	digest := verificationBatchDigest(prepared)
	batchID := uuid.NewString()
	openedGaps := make([]Gap, 0, len(prepared))
	outcome, err := CommitEvidenceWrite(ctx, v.store, EvidenceWriteRequest{
		InstanceID:   instanceID,
		Token:        token,
		Kind:         MutationVerificationBatch,
		Actor:        actor,
		Reason:       fmt.Sprintf("V1-V9 verification batch: %d item(s) over %d read-only source(s)", len(prepared), len(req.Sources)),
		OperationID:  operationID,
		ResultDigest: digest,
		Apply: func(ctx context.Context, tx pgx.Tx, accepted EvidenceToken) error {
			batchScope, err := json.Marshal(map[string]any{
				"operation_id":      operationID,
				"actor":             actor,
				"request_scope":     json.RawMessage(requestScope),
				"source_categories": categories,
				"item_count":        len(prepared),
			})
			if err != nil {
				return fmt.Errorf("encode verification batch scope: %w", err)
			}
			if err := insertEvidence(ctx, tx, batchID, accepted.InstanceID, accepted.Generation,
				"verification_batch", batchScope, string(digest), "control:recovery_evidence/"+batchID, actor); err != nil {
				return err
			}
			for i := range prepared {
				item := &prepared[i].item
				item.InstanceID = accepted.InstanceID
				item.Generation = accepted.Generation
				if err := insertVerificationItem(ctx, tx, item); err != nil {
					return err
				}
			}
			for i := range prepared {
				if prepared[i].gap == nil {
					continue
				}
				gapRecord, err := insertVerificationGap(ctx, tx, accepted.InstanceID, *prepared[i].gap)
				if err != nil {
					return err
				}
				openedGaps = append(openedGaps, gapRecord)
			}
			generation := accepted.Generation
			target, err := json.Marshal(map[string]any{
				"batch_id":   batchID,
				"item_count": len(prepared),
				"categories": categories,
			})
			if err != nil {
				return fmt.Errorf("encode verification audit target: %w", err)
			}
			detail, err := json.Marshal(map[string]any{
				"operation_id": operationID,
				"gaps_opened":  len(openedGaps),
			})
			if err != nil {
				return fmt.Errorf("encode verification audit detail: %w", err)
			}
			return controlstore.WriteAudit(ctx, tx, controlstore.AuditRecord{
				InstanceID:         accepted.InstanceID,
				Actor:              actor,
				Action:             ActionVerification,
				Target:             target,
				Detail:             detail,
				Result:             controlstore.AuditOK,
				EvidenceGeneration: &generation,
				OperationID:        operationID,
			})
		},
	})
	if err != nil {
		return VerificationBatch{}, err
	}
	if outcome.Discarded {
		return VerificationBatch{
			InstanceID: instanceID,
			Generation: outcome.Token.Generation,
			Discarded:  true,
		}, nil
	}
	items := make([]VerificationItem, 0, len(prepared))
	for i := range prepared {
		items = append(items, prepared[i].item)
	}
	return VerificationBatch{
		InstanceID: instanceID,
		BatchID:    batchID,
		Generation: outcome.Token.Generation,
		Items:      items,
		Gaps:       openedGaps,
	}, nil
}

// collect reads every source and derives the pending items and gaps. It is
// strictly read-only: it never touches the control store or the data DB
// directly, and it never calls an effectful method (the source interface has
// none). instanceID is the canonical recovery instance id recorded on every
// pending item, so the prepared items are insertable without a later patch-up.
func (v *Verification) collect(ctx context.Context, instanceID string, req VerificationRequest, requestScope []byte, evaluatedAt time.Time) ([]verificationPendingItem, error) {
	type objectKey struct {
		category  VerificationCategory
		objectKey string
	}
	var order []objectKey
	groups := make(map[objectKey]*verificationObject)
	total := 0
	for _, source := range req.Sources {
		category := source.Category()
		observations, err := source.Observe(ctx)
		if err != nil {
			return nil, fmt.Errorf("%w: source %s observation failed: %s",
				ErrVerificationObservation, category, logx.Redact(err.Error()))
		}
		for _, observation := range observations {
			total++
			if total > v.batchLimit {
				// Stop reading further sources as soon as the configured bound
				// is exceeded: nothing is persisted for this step.
				return nil, fmt.Errorf("%w: more than the configured bound %d (%s) of observations were collected",
					ErrVerificationBound, v.batchLimit, VerificationBatchLimitConfigKey)
			}
			object := strings.TrimSpace(observation.ObjectKey)
			if object == "" {
				return nil, fmt.Errorf("%w: a %s observation carries no object_key; the stable object identity is required",
					ErrVerificationObservation, category)
			}
			if len(observation.Scope) > 0 && !json.Valid(observation.Scope) {
				return nil, fmt.Errorf("%w: observation %s/%s scope is not valid JSON", ErrVerificationObservation, category, object)
			}
			if len(observation.Sources) > 0 && !json.Valid(observation.Sources) {
				return nil, fmt.Errorf("%w: observation %s/%s sources payload is not valid JSON", ErrVerificationObservation, category, object)
			}
			if observation.GapEvidence != nil {
				if err := validateGapEvidence(object, *observation.GapEvidence); err != nil {
					return nil, err
				}
			}
			key := objectKey{category: category, objectKey: object}
			group := groups[key]
			if group == nil {
				scope := requestScope
				if len(observation.Scope) > 0 {
					scope = observation.Scope
				}
				group = &verificationObject{category: category, objectKey: object, scope: scope}
				groups[key] = group
				order = append(order, key)
			}
			group.observations = append(group.observations, observation)
			if observation.GapEvidence != nil && group.gap == nil {
				gap := *observation.GapEvidence
				group.gap = &gap
			}
		}
	}
	if len(order) == 0 {
		// A step that observed nothing proves nothing: committing it would
		// advance the evidence generation (invalidating approvals) without a
		// single conclusion. Refuse instead.
		return nil, fmt.Errorf("%w: the sources observed no objects; an empty batch is not a verification", ErrVerificationObservation)
	}

	prepared := make([]verificationPendingItem, 0, len(order))
	for _, key := range order {
		group := groups[key]
		input := ConclusionInput{
			Category: key.category,
			Sources:  group.observations,
			// The adapter's observation is its own complete/closed verdict:
			// an adapter that cannot prove completeness must return unknown.
			// The evaluator re-derives the verdict and can only downgrade it.
			EvidenceComplete: true,
			CoverageClosed:   true,
			Tolerance:        v.toleranceFor(key.category),
			Now:              evaluatedAt,
		}
		pending := verificationPendingItem{}
		if group.gap != nil {
			input.Missing = true
			pending.gap = &verificationPendingGap{
				objectKey: group.objectKey,
				scope:     group.scope,
				evidence:  *group.gap,
			}
		}
		conclusion, reason := EvaluateConclusion(input)
		sourcesPayload, err := marshalVerificationObservations(group.observations)
		if err != nil {
			return nil, fmt.Errorf("%w: encode %s/%s sources payload: %v", ErrVerificationObservation, key.category, key.objectKey, err)
		}
		item := VerificationItem{
			ItemID:       uuid.NewString(),
			InstanceID:   instanceID,
			Category:     key.category,
			ObjectKey:    key.objectKey,
			Scope:        group.scope,
			Sources:      sourcesPayload,
			Conclusion:   conclusion,
			Reason:       boundedEvidenceDetail(reason),
			EvidenceRefs: verificationEvidenceRefs(group.observations),
			ObservedAt:   verificationObservedAt(group.observations, evaluatedAt),
		}
		pending.item = item
		prepared = append(prepared, pending)
	}
	return prepared, nil
}

// verificationObject is one stable object identity observed by one or more
// source observations of the same category.
type verificationObject struct {
	category     VerificationCategory
	objectKey    string
	scope        []byte
	observations []SourceObservation
	gap          *GapEvidence
}

// verificationObservationPayload is the persisted shape of one observation
// inside recovery_verification_item.sources.
type verificationObservationPayload struct {
	ObjectKey    string          `json:"object_key"`
	Conclusion   string          `json:"conclusion"`
	Reason       string          `json:"reason,omitempty"`
	ObservedAt   string          `json:"observed_at,omitempty"`
	EvidenceRefs []string        `json:"evidence_refs,omitempty"`
	Sources      json.RawMessage `json:"sources,omitempty"`
}

// marshalVerificationObservations renders the item's sources JSONB payload:
// the declared per-source read values/locations/times and verdicts.
func marshalVerificationObservations(observations []SourceObservation) ([]byte, error) {
	payload := make([]verificationObservationPayload, 0, len(observations))
	for _, observation := range observations {
		entry := verificationObservationPayload{
			ObjectKey:    strings.TrimSpace(observation.ObjectKey),
			Conclusion:   string(observation.Conclusion),
			Reason:       observation.Reason,
			EvidenceRefs: verificationEvidenceRefs([]SourceObservation{observation}),
		}
		if !observation.ObservedAt.IsZero() {
			entry.ObservedAt = observation.ObservedAt.UTC().Format(time.RFC3339Nano)
		}
		if len(observation.Sources) > 0 {
			entry.Sources = json.RawMessage(observation.Sources)
		}
		payload = append(payload, entry)
	}
	return json.Marshal(payload)
}

// verificationObservedAt is the item's observation time: the newest declared
// observation timestamp, or the evaluation instant when no observation
// carrying one exists (the verdict is unknown in that case; the recorded
// instant is the batch's record time, never evidence of freshness).
func verificationObservedAt(observations []SourceObservation, evaluatedAt time.Time) time.Time {
	observedAt := time.Time{}
	for _, observation := range observations {
		if observation.ObservedAt.After(observedAt) {
			observedAt = observation.ObservedAt
		}
	}
	if observedAt.IsZero() {
		return evaluatedAt
	}
	return observedAt
}

// verificationEvidenceRefs aggregates the evidence references of a set of
// observations deterministically (trimmed, de-duplicated, sorted).
func verificationEvidenceRefs(observations []SourceObservation) []string {
	refs := make([]string, 0, len(observations))
	for _, observation := range observations {
		for _, ref := range observation.EvidenceRefs {
			if trimmed := strings.TrimSpace(ref); trimmed != "" {
				refs = append(refs, trimmed)
			}
		}
	}
	slices.Sort(refs)
	return slices.Compact(refs)
}

// validateGapEvidence enforces the FR-019 bundle contract: a verification
// observation may only ask for a gap when it names at least one known affected
// capability and carries the timeline, existing-evidence and required-evidence
// JSON payloads. Nothing is filled in from a default.
func validateGapEvidence(objectKey string, evidence GapEvidence) error {
	if len(evidence.AffectedCapabilities) == 0 {
		return fmt.Errorf("%w: gap %s names no affected capability", ErrVerificationObservation, objectKey)
	}
	for _, capability := range evidence.AffectedCapabilities {
		if !capability.Known() {
			return fmt.Errorf("%w: gap %s names capability %q outside the closed set",
				ErrVerificationObservation, objectKey, capability)
		}
	}
	for _, part := range []struct {
		name string
		raw  []byte
	}{
		{"timeline", evidence.Timeline},
		{"existing_evidence", evidence.ExistingEvidence},
		{"required_evidence", evidence.RequiredEvidence},
	} {
		if len(bytes.TrimSpace(part.raw)) == 0 || !json.Valid(part.raw) {
			return fmt.Errorf("%w: gap %s is missing the FR-019 %s payload", ErrVerificationObservation, objectKey, part.name)
		}
	}
	if len(evidence.Risk) > 0 && !json.Valid(evidence.Risk) {
		return fmt.Errorf("%w: gap %s risk payload is not valid JSON", ErrVerificationObservation, objectKey)
	}
	return nil
}

// conservativePauseSet expands a set of directly affected capabilities to the
// conservative pause set: every capability that transitively requires a
// paused capability is dragged in (the amplification direction of FR-019).
// "It is a different module" is never an independence proof; subtracting from
// the pause set requires the T041 dependency-proof path, which this helper
// cannot weaken.
func conservativePauseSet(direct []Capability) ([]Capability, error) {
	paused := make(map[Capability]bool, len(knownCapabilities))
	for _, capability := range direct {
		if !capability.Known() {
			return nil, fmt.Errorf("%w: %q", ErrUnknownCapability, capability)
		}
		paused[capability] = true
	}
	for changed := true; changed; {
		changed = false
		for _, capability := range knownCapabilities {
			if paused[capability] {
				continue
			}
			dependencies, err := DependencyClosure(capability)
			if err != nil {
				return nil, err
			}
			for _, dependency := range dependencies {
				if paused[dependency] {
					paused[capability] = true
					changed = true
					break
				}
			}
		}
	}
	out := make([]Capability, 0, len(paused))
	for _, capability := range knownCapabilities {
		if paused[capability] {
			out = append(out, capability)
		}
	}
	return out, nil
}

// verificationBatchDigest is the canonical digest of the accepted batch
// contents (items only; gaps are re-derivable from the item evidence and the
// observations). It enters the chained evidence hash through
// EvidenceWriteRequest.ResultDigest.
func verificationBatchDigest(prepared []verificationPendingItem) []byte {
	hash := sha256.New()
	hash.Write([]byte("txharbor-recovery-verification-batch-v1"))
	for i := range prepared {
		item := &prepared[i].item
		hash.Write([]byte{0})
		hash.Write([]byte(item.Category))
		hash.Write([]byte{0})
		hash.Write([]byte(item.ObjectKey))
		hash.Write([]byte{0})
		hash.Write([]byte(item.Conclusion))
		hash.Write([]byte{0})
		hash.Write([]byte(item.Reason))
		hash.Write([]byte{0})
		hash.Write(item.Scope)
		hash.Write([]byte{0})
		hash.Write(item.Sources)
		hash.Write([]byte{0})
		hash.Write([]byte(item.ObservedAt.UTC().Format(time.RFC3339Nano)))
	}
	sum := hash.Sum(nil)
	return []byte(hex.EncodeToString(sum[:]))
}

// insertVerificationItem appends one recovery_verification_item row inside the
// generation protocol's transaction and records the database-created_at.
func insertVerificationItem(ctx context.Context, tx pgx.Tx, item *VerificationItem) error {
	const sql = `
INSERT INTO recovery_verification_item
    (item_id, instance_id, generation, category, object_key, scope, sources,
     conclusion, reason, evidence_refs, observed_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
RETURNING created_at`
	reason := any(nil)
	if item.Reason != "" {
		reason = item.Reason
	}
	if err := tx.QueryRow(ctx, sql,
		item.ItemID, item.InstanceID, item.Generation, string(item.Category), item.ObjectKey,
		item.Scope, item.Sources, string(item.Conclusion), reason, item.EvidenceRefs, item.ObservedAt,
	).Scan(&item.CreatedAt); err != nil {
		return fmt.Errorf("insert verification item %s/%s: %w", item.Category, item.ObjectKey, err)
	}
	return nil
}

// insertVerificationGap establishes one open recovery_gap row for a
// verification observation that asked for a gap. An open gap for the same
// object identity is left untouched (closure only by new evidence, T041);
// its persisted row is returned instead. The pause set is conservatively
// expanded and dependency_proof stays NULL: verification never claims an
// independence it did not prove (FR-019).
func insertVerificationGap(ctx context.Context, tx pgx.Tx, instanceID string, pending verificationPendingGap) (Gap, error) {
	var existingID string
	err := tx.QueryRow(ctx,
		`SELECT gap_id::text FROM recovery_gap WHERE instance_id = $1 AND object_key = $2 AND state = 'open' LIMIT 1`,
		instanceID, pending.objectKey).Scan(&existingID)
	if err == nil {
		return scanGap(tx.QueryRow(ctx,
			`SELECT `+gapSelectColumns+` FROM recovery_gap WHERE gap_id = $1`, existingID))
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Gap{}, fmt.Errorf("check open gap for %s: %w", pending.objectKey, err)
	}

	affected, err := conservativePauseSet(pending.evidence.AffectedCapabilities)
	if err != nil {
		return Gap{}, fmt.Errorf("expand conservative pause set for %s: %w", pending.objectKey, err)
	}
	requiredEvidence, err := mergeGapRiskPayload(pending.evidence.RequiredEvidence, pending.evidence.Risk)
	if err != nil {
		return Gap{}, err
	}
	gapID := uuid.NewString()
	affectedNames := make([]string, 0, len(affected))
	for _, capability := range affected {
		affectedNames = append(affectedNames, string(capability))
	}
	record, err := scanGap(tx.QueryRow(ctx, `
INSERT INTO recovery_gap
    (gap_id, instance_id, object_key, scope, timeline, existing_evidence,
     required_evidence, affected_capabilities, dependency_proof, state, owner)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NULL, 'open', '')
RETURNING `+gapSelectColumns,
		gapID, instanceID, pending.objectKey, pending.scope, pending.evidence.Timeline,
		pending.evidence.ExistingEvidence, requiredEvidence, affectedNames))
	if err != nil {
		return Gap{}, fmt.Errorf("establish evidence gap for %s: %w", pending.objectKey, err)
	}
	return record, nil
}

// gapSelectColumns is the recovery_gap projection scanned by scanGap.
const gapSelectColumns = `
gap_id::text, instance_id::text, object_key, scope, timeline, existing_evidence,
required_evidence, affected_capabilities, COALESCE(dependency_proof::text, ''),
state, COALESCE(closed_by, ''), closed_at, COALESCE(closure_evidence::text, ''),
owner, COALESCE(escalation_ref, ''), created_at`

// scanGap scans one row of gapSelectColumns. The risk payload is unwrapped
// from required_evidence via splitGapRiskPayload.
func scanGap(row pgx.Row) (Gap, error) {
	var (
		record               Gap
		state                string
		dependencyProof      string
		closureEvidence      string
		affectedCapabilities []string
		closedAt             *time.Time
	)
	if err := row.Scan(
		&record.GapID, &record.InstanceID, &record.ObjectKey, &record.Scope,
		&record.Timeline, &record.ExistingEvidence, &record.RequiredEvidence,
		&affectedCapabilities, &dependencyProof, &state, &record.ClosedBy,
		&closedAt, &closureEvidence, &record.Owner, &record.EscalationRef, &record.CreatedAt,
	); err != nil {
		return Gap{}, err
	}
	record.RequiredEvidence, record.Risk = splitGapRiskPayload(record.RequiredEvidence)
	record.AffectedCapabilities = make([]Capability, 0, len(affectedCapabilities))
	for _, capability := range affectedCapabilities {
		record.AffectedCapabilities = append(record.AffectedCapabilities, Capability(capability))
	}
	if dependencyProof != "" {
		record.DependencyProof = []byte(dependencyProof)
	}
	if closureEvidence != "" {
		record.ClosureEvidence = []byte(closureEvidence)
	}
	record.State = GapState(state)
	if closedAt != nil {
		record.ClosedAt = *closedAt
	}
	return record, nil
}

// recordedVerificationBatch is the idempotency anchor of an accepted batch:
// the recovery_evidence row keyed by its operation_id.
type recordedVerificationBatch struct {
	EvidenceID string
	Generation int64
	Scope      []byte
}

// recordedBatch reads the accepted batch recorded under one operation_id
// (strictly read-only).
func (v *Verification) recordedBatch(ctx context.Context, instanceID, operationID string) (recordedVerificationBatch, bool, error) {
	const sql = `
SELECT evidence_id::text, generation, scope
FROM recovery_evidence
WHERE instance_id = $1 AND kind = 'verification_batch' AND scope->>'operation_id' = $2
ORDER BY created_at DESC, evidence_id DESC
LIMIT 1`
	var recorded recordedVerificationBatch
	err := v.store.Pool().QueryRow(ctx, sql, instanceID, operationID).Scan(
		&recorded.EvidenceID, &recorded.Generation, &recorded.Scope)
	if errors.Is(err, pgx.ErrNoRows) {
		return recordedVerificationBatch{}, false, nil
	}
	if err != nil {
		return recordedVerificationBatch{}, false, fmt.Errorf("read recorded verification batch %q: %w", operationID, err)
	}
	return recorded, true, nil
}

// matchesRequest reports whether a recorded batch was produced by the same
// request (actor, request scope and source categories). A different request
// under the same operation_id is an idempotency conflict with zero writes.
func (r recordedVerificationBatch) matchesRequest(actor string, requestScope []byte, categories []string) (bool, error) {
	var stored struct {
		Actor            string          `json:"actor"`
		RequestScope     json.RawMessage `json:"request_scope"`
		SourceCategories []string        `json:"source_categories"`
	}
	if err := json.Unmarshal(r.Scope, &stored); err != nil {
		return false, fmt.Errorf("decode recorded verification batch scope: %w", err)
	}
	if strings.TrimSpace(stored.Actor) != actor {
		return false, nil
	}
	if !slices.Equal(stored.SourceCategories, categories) {
		return false, nil
	}
	return verificationJSONEqual(stored.RequestScope, requestScope)
}

// loadBatch reconstructs a recorded batch for the idempotent read-back. Gaps
// are the currently open gaps of the batch's objects (gaps are not keyed by
// operation and re-verification never duplicates an open gap).
func (v *Verification) loadBatch(ctx context.Context, instanceID string, recorded recordedVerificationBatch) (VerificationBatch, error) {
	batch := VerificationBatch{
		InstanceID: instanceID,
		BatchID:    recorded.EvidenceID,
		Generation: recorded.Generation,
	}
	const itemSQL = `
SELECT item_id::text, instance_id::text, generation, category, object_key, scope, sources,
       conclusion, COALESCE(reason, ''), evidence_refs, observed_at, created_at
FROM recovery_verification_item
WHERE instance_id = $1 AND generation = $2
ORDER BY category, object_key, item_id`
	rows, err := v.store.Pool().Query(ctx, itemSQL, instanceID, recorded.Generation)
	if err != nil {
		return VerificationBatch{}, fmt.Errorf("read recorded verification items: %w", err)
	}
	defer rows.Close()
	objectKeys := make([]string, 0, 16)
	for rows.Next() {
		var (
			item       VerificationItem
			category   string
			conclusion string
		)
		if err := rows.Scan(&item.ItemID, &item.InstanceID, &item.Generation, &category,
			&item.ObjectKey, &item.Scope, &item.Sources, &conclusion, &item.Reason,
			&item.EvidenceRefs, &item.ObservedAt, &item.CreatedAt); err != nil {
			return VerificationBatch{}, fmt.Errorf("scan recorded verification item: %w", err)
		}
		item.Category = VerificationCategory(category)
		item.Conclusion = VerificationConclusion(conclusion)
		batch.Items = append(batch.Items, item)
		objectKeys = append(objectKeys, item.ObjectKey)
	}
	if err := rows.Err(); err != nil {
		return VerificationBatch{}, fmt.Errorf("read recorded verification items: %w", err)
	}
	if len(objectKeys) == 0 {
		return batch, nil
	}
	slices.Sort(objectKeys)
	objectKeys = slices.Compact(objectKeys)
	const gapSQL = `
SELECT ` + gapSelectColumns + `
FROM recovery_gap
WHERE instance_id = $1 AND state = 'open' AND object_key = ANY($2)
ORDER BY object_key, gap_id`
	gapRows, err := v.store.Pool().Query(ctx, gapSQL, instanceID, objectKeys)
	if err != nil {
		return VerificationBatch{}, fmt.Errorf("read recorded verification gaps: %w", err)
	}
	defer gapRows.Close()
	for gapRows.Next() {
		record, err := scanGap(gapRows)
		if err != nil {
			return VerificationBatch{}, fmt.Errorf("scan recorded verification gap: %w", err)
		}
		batch.Gaps = append(batch.Gaps, record)
	}
	if err := gapRows.Err(); err != nil {
		return VerificationBatch{}, fmt.Errorf("read recorded verification gaps: %w", err)
	}
	return batch, nil
}

// auditVerification appends one verification audit row (best effort: the
// refusal itself is already decided and the annotation is never a
// precondition).
func (v *Verification) auditVerification(ctx context.Context, instanceID, actor, operationID, result, reason string, generation *int64) {
	if v == nil || v.store == nil || instanceID == "" || actor == "" {
		return
	}
	detail, err := json.Marshal(map[string]any{"reason": boundedEvidenceDetail(reason)})
	if err != nil {
		return
	}
	_ = controlstore.WriteAudit(ctx, v.store.Pool(), controlstore.AuditRecord{
		InstanceID:         instanceID,
		Actor:              actor,
		Action:             ActionVerification,
		Detail:             detail,
		Result:             result,
		EvidenceGeneration: generation,
		OperationID:        operationID,
	})
}

// instanceKind reads the kind of one instance (the migration's kind sets are
// immutable per row).
func (v *Verification) instanceKind(ctx context.Context, instanceID string) (string, error) {
	var kind string
	err := v.store.Pool().QueryRow(ctx,
		`SELECT kind FROM recovery_instance WHERE instance_id = $1`, instanceID).Scan(&kind)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", fmt.Errorf("%w: %s", controlstore.ErrInstanceNotFound, instanceID)
	}
	if err != nil {
		return "", fmt.Errorf("read recovery instance %s: %w", instanceID, err)
	}
	return kind, nil
}

// verificationInstanceID canonicalizes and validates the instance id.
func verificationInstanceID(raw string) (string, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return "", fmt.Errorf("%w: instance_id is required", ErrVerificationRequest)
	}
	parsed, err := uuid.Parse(value)
	if err != nil {
		return "", fmt.Errorf("%w: instance_id %q is not a UUID", ErrVerificationRequest, raw)
	}
	return parsed.String(), nil
}

// normalizeVerificationScope validates the request scope: a verification
// without a scope (or with a non-object scope) is refused (the scope records
// chain/asset/business type/object range and is never defaulted).
func normalizeVerificationScope(raw []byte) ([]byte, error) {
	scope := bytes.TrimSpace(raw)
	if len(scope) == 0 {
		return nil, fmt.Errorf("%w: scope is required and is never defaulted", ErrVerificationRequest)
	}
	var object map[string]any
	if err := json.Unmarshal(scope, &object); err != nil {
		return nil, fmt.Errorf("%w: scope must be a JSON object", ErrVerificationRequest)
	}
	if object == nil {
		return nil, fmt.Errorf("%w: scope must be a JSON object", ErrVerificationRequest)
	}
	return scope, nil
}

// verificationJSONEqual compares two JSON payloads semantically (key order and
// spacing are not part of the value).
func verificationJSONEqual(a, b []byte) (bool, error) {
	var left, right any
	if err := decodeVerificationJSON(a, &left); err != nil {
		return false, err
	}
	if err := decodeVerificationJSON(b, &right); err != nil {
		return false, err
	}
	return reflect.DeepEqual(left, right), nil
}

// decodeVerificationJSON decodes one complete JSON value with numbers kept
// exact (json.Number), so two representations of the same number compare
// equal.
func decodeVerificationJSON(raw []byte, out *any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(out); err != nil {
		return err
	}
	if decoder.More() {
		return errors.New("trailing JSON after the first value")
	}
	return nil
}
