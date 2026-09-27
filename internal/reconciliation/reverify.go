// reverify.go implements T027: the bounded history-revalidation executor
// (FR-005/018, Q1/Q5-6; data-model.md §6; contracts/discrepancy-lifecycle.md
// History Revalidation Sweep; quickstart §11).
//
// The executor revalidates already-closed discrepancies inside one task scope
// under a bounded budget slice of the scan invocation:
//
//   - Finders: the scan loop's cross-check candidates (closed items falling
//     into the current budgeted interval) are consumed first; the directed
//     enumeration walks in-scope closed items oldest-evidence-first, tracked by
//     the `history_sweep_through` traversal cursor; items whose earlier
//     revalidation attempts failed are retried under bounded retry order
//     (failure-count-asc + evidence-age-desc, so no slice starvation).
//   - Evidence re-reads go through the ReverifyEvaluator seam; the built-in
//     EventStateReverifyEvaluator reuses the T015 read-only event-delivery
//     adapter (`eventstate.go`) and never writes anything.
//   - The executor writes exactly four things, all 014-owned: append-only
//     `reverify` verdict rows, `recon_gap` rows (reason `query_failed` /
//     `freshness_hold`), append-only `recon_audit` rows, and the task row's
//     `history_sweep_through` cursor. It NEVER touches payments, signatures,
//     broadcasts, recovery/replay paths, disposition rows or close_basis
//     (Q1/Q5-6). A divergent re-read only enters the approved reverify flow
//     through the T026 evaluator (never auto-disposal, never a close).
//   - Fail-closed verdicts: timeout / incomplete / unavailable / insufficient
//     evidence can never be recorded as `consistent`; a failed item keeps its
//     gap visible and can never support a close (its latest reverify row is no
//     longer consistent, so the T007 close guard refuses). The traversal
//     cursor is progress, never a verified-completeness claim:
//     ReverifySweepResult.VerifiedComplete() requires the waterline to be
//     reached AND zero open query_failed gaps AND no exhausted retries.
//
// There is no daemon and no auto-start here (ADR-001): one call performs one
// bounded slice; repeated invocations make progress.
package reconciliation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// reverifyEvidenceAtExpr is the SQL evidence-age position of one closed item:
// the recorded evidence instant, falling back to the row creation time so an
// item without a parsable evidence time still orders deterministically. A
// malformed (non-castable) value fails the query visibly instead of guessing.
const reverifyEvidenceAtExpr = `COALESCE(NULLIF(d.evidence_version_domain->>'evidence_at', '')::timestamptz, d.created_at)`

// HistorySweepCursor is the persisted `recon_task.history_sweep_through`
// value: the traversal position over in-scope closed items ordered
// oldest-evidence-first. It is a traversal cursor only; it is never a claim
// that everything before it was verified (data-model.md §6).
type HistorySweepCursor struct {
	EvidenceAt    time.Time `json:"evidence_at"`
	DiscrepancyID string    `json:"discrepancy_id"`
}

// Valid reports whether the cursor carries a usable position.
func (c HistorySweepCursor) Valid() bool {
	return !c.EvidenceAt.IsZero() && strings.TrimSpace(c.DiscrepancyID) != ""
}

// parseHistorySweepCursor decodes the persisted cursor. A malformed cursor is
// a contract error: it is never silently reset (a reset would re-walk the
// whole scope and hide the torn write).
func parseHistorySweepCursor(raw []byte) (*HistorySweepCursor, error) {
	text := strings.TrimSpace(string(raw))
	if text == "" || text == "null" {
		return nil, nil
	}
	var cursor HistorySweepCursor
	if err := json.Unmarshal([]byte(text), &cursor); err != nil {
		return nil, contractErrorf("history_sweep_through is not a cursor object: %v", err)
	}
	if !cursor.Valid() {
		return nil, contractErrorf("history_sweep_through cursor is incomplete")
	}
	cursor.EvidenceAt = cursor.EvidenceAt.UTC()
	return &cursor, nil
}

// HistorySweepThrough reads the recorded traversal cursor of one task. nil
// means the sweep has not started (or a cursor was never recorded) and the
// next pass begins at the oldest evidence.
func (s *Store) HistorySweepThrough(ctx context.Context, taskID string) (*HistorySweepCursor, error) {
	if s == nil || s.db == nil {
		return nil, contractErrorf("store has no database")
	}
	if strings.TrimSpace(taskID) == "" {
		return nil, contractErrorf("history sweep cursor requires a task_id")
	}
	var raw []byte
	err := s.db.QueryRow(ctx, readHistorySweepSQL, strings.TrimSpace(taskID)).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: task_id %s", ErrTaskNotFound, taskID)
	}
	if err != nil {
		return nil, fmt.Errorf("read history sweep cursor: %w", err)
	}
	return parseHistorySweepCursor(raw)
}

// CountQueryFailedGaps counts the task's open query_failed gap rows. The
// history-sweep result uses it for the honest failure-visibility metric: any
// visible revalidation query failure keeps the sweep from claiming
// completeness (a traversal cursor over a gap is not completeness). The
// sweep's completeness claim additionally reads every open gap through the
// existing OpenGapCount (data-model.md §1.3: "fully consistent" requires zero
// open gaps).
func (s *Store) CountQueryFailedGaps(ctx context.Context, taskID string) (int64, error) {
	if s == nil || s.db == nil {
		return 0, contractErrorf("store has no database")
	}
	var count int64
	if err := s.db.QueryRow(ctx, countQueryFailedGapsSQL, strings.TrimSpace(taskID)).Scan(&count); err != nil {
		return 0, fmt.Errorf("count query_failed gaps: %w", err)
	}
	return count, nil
}

// ReverifySlice bounds one invocation's history-revalidation work. Every field
// is a caller-supplied deployment/test parameter: the executor never invents
// production thresholds and unbounded is not representable.
type ReverifySlice struct {
	// MaxItems bounds how many closed items one slice revalidates.
	MaxItems int
	// MaxPGRequests bounds the slice's own PostgreSQL requests. When a parent
	// scan budget is supplied the same charges also consume the task's total
	// budget (data-model.md §6: the slice never exceeds its reservation).
	MaxPGRequests int
	// MaxItemDuration optionally bounds one item's evidence re-read; zero
	// means the parent context alone bounds it. A per-item timeout is recorded
	// as a failed attempt (never consistent), never as an abort.
	MaxItemDuration time.Duration
	// MaxItemAttempts is the bounded retry cap per item, counting the first
	// attempt. An item that exhausts it stays visibly gapped (no more slice
	// occupancy) and is escalated once per audit.
	MaxItemAttempts int
}

// Validate fails closed on missing or non-positive bounds.
func (s ReverifySlice) Validate() error {
	switch {
	case s.MaxItems <= 0:
		return contractErrorf("reverify slice max items must be positive")
	case s.MaxPGRequests <= 0:
		return contractErrorf("reverify slice max PG requests must be positive")
	case s.MaxItemDuration < 0:
		return contractErrorf("reverify slice item duration must not be negative")
	case s.MaxItemAttempts <= 0:
		return contractErrorf("reverify slice max item attempts must be positive")
	}
	return nil
}

// reverifySliceBudget is the T008-compatible accounting seam of one slice: it
// enforces the slice's own PG cap first and then charges the parent scan
// budget, so revalidation work is never unaccounted and never eats more than
// its reserved share (data-model.md §6).
type reverifySliceBudget struct {
	parent *Budget
	maxPG  int
	usedPG int
}

// newReverifySliceBudget wraps a slice cap; parent may be nil for a standalone
// bounded sweep (RPC charges then require an explicit parent).
func newReverifySliceBudget(parent *Budget, maxPG int) *reverifySliceBudget {
	return &reverifySliceBudget{parent: parent, maxPG: maxPG}
}

// ConsumePG charges the slice cap and the parent budget in order.
func (b *reverifySliceBudget) ConsumePG(ctx context.Context, requests int) error {
	if requests < 0 {
		return contractErrorf("reverify slice PG charge must not be negative")
	}
	if requests == 0 {
		return nil
	}
	if b.usedPG+requests > b.maxPG {
		return fmt.Errorf("%w: reverify slice PG quota reached (limit=%d used=%d)",
			ErrBudgetExhausted, b.maxPG, b.usedPG)
	}
	if b.parent != nil {
		if err := b.parent.ConsumePG(ctx, requests); err != nil {
			return err
		}
	}
	b.usedPG += requests
	return nil
}

// ConsumeRPC forwards RPC charges to the parent scan budget. Without a parent
// budget no RPC can be accounted, so the charge is refused as a wiring defect
// instead of being silently free.
func (b *reverifySliceBudget) ConsumeRPC(ctx context.Context, requests int) error {
	if requests < 0 {
		return contractErrorf("reverify slice RPC charge must not be negative")
	}
	if requests == 0 {
		return nil
	}
	if b.parent == nil {
		return contractErrorf("reverify slice has no parent budget to charge RPC requests")
	}
	return b.parent.ConsumeRPC(ctx, requests)
}

// ReverifyCandidate is one closed item handed over by the scan loop's
// cross-check (data-model.md §6 finder 1). EvidenceRef is an optional bounded
// observation reference recorded in the audit.
type ReverifyCandidate struct {
	DiscrepancyID string
	EvidenceRef   string
}

// ClosedDiscrepancy is one in-scope closed item materialized for
// revalidation: the recorded identity/conclusion (business key, content hash,
// version domain, close_basis) plus its evidence age and prior failed-attempt
// count.
type ClosedDiscrepancy struct {
	DiscrepancyID string
	Category      Category
	BusinessKey   BusinessKey
	ContentHash   []byte
	// Scope is the recorded detection scope (nil for legacy rows without a
	// scope marker); the directed enumeration only selects scoped rows.
	Scope      *IdentityScope
	Version    VersionDomain
	CloseBasis []byte
	// ReopenCount is the recorded reopen history count (never reset).
	ReopenCount int64
	// EvidenceAt is the recorded evidence instant the oldest-first ordering
	// (and the sweep waterline) is based on.
	EvidenceAt time.Time
	// FailCount is the bounded count of prior failed/inconclusive attempts.
	FailCount int64
}

// EvidenceAge reports how old the recorded evidence is at now.
func (d ClosedDiscrepancy) EvidenceAge(now time.Time) time.Duration {
	if d.EvidenceAt.IsZero() {
		return 0
	}
	return now.UTC().Sub(d.EvidenceAt.UTC())
}

// reverifyItem projects the closed item onto the shared persistence carrier.
func (d ClosedDiscrepancy) reverifyItem() reverifyItem {
	return reverifyItem{
		DiscrepancyID: d.DiscrepancyID,
		Version:       d.Version,
		EvidenceAt:    d.EvidenceAt,
		FailCount:     d.FailCount,
	}
}

// ReverifyFinding is one fail-closed evidence re-read outcome. Only a
// consistent verdict with an evidence reference and a non-zero freshness may
// support a close; everything else stays pending/invalidated.
type ReverifyFinding struct {
	Verdict     ReverifyVerdict
	EvidenceRef string
	FreshnessAt time.Time
	// Trigger optionally names the conclusion-affecting change a divergent
	// re-read observed. It is mapped onto the closed InvalidationTrigger
	// vocabulary before use; empty/unknown defaults to new_evidence.
	Trigger InvalidationTrigger
	// Detail is a bounded, secret-free audit annotation.
	Detail string
	// Scope optionally names the re-read range in bounded text form
	// ("height:2..5" / "time:..."); it is recorded in the audit trail so a
	// consistent verdict is traceable to the range it was derived from.
	Scope string
	// Version is the re-read evidence version domain (block identity and
	// business versions) recorded in the audit trail.
	Version *VersionDomain
}

// normalizeReverifyFinding enforces the fail-closed verdict rules: an unknown
// verdict is a wiring defect (error), and a consistent verdict without a
// bounded evidence reference and a non-zero freshness is downgraded to
// unknown (a close can never be supported by incomplete evidence, Q5-4).
func normalizeReverifyFinding(f ReverifyFinding) (ReverifyFinding, error) {
	if !f.Verdict.Valid() {
		return f, contractErrorf("reverify evaluator returned unknown verdict %q", f.Verdict)
	}
	if len(f.EvidenceRef) > scanEvidenceRefMax || strings.ContainsRune(f.EvidenceRef, 0) {
		return f, contractErrorf("reverify finding evidence_ref is malformed")
	}
	if len(f.Detail) > scanEvidenceRefMax || strings.ContainsRune(f.Detail, 0) {
		return f, contractErrorf("reverify finding detail is malformed")
	}
	if len(f.Scope) > scanEvidenceRefMax || strings.ContainsRune(f.Scope, 0) {
		return f, contractErrorf("reverify finding scope is malformed")
	}
	if f.Version != nil {
		if err := f.Version.Validate(); err != nil {
			return f, err
		}
		copied := *f.Version
		f.Version = &copied
	}
	f.EvidenceRef = strings.TrimSpace(f.EvidenceRef)
	f.Detail = strings.TrimSpace(f.Detail)
	f.Scope = strings.TrimSpace(f.Scope)
	if f.Verdict == ReverifyConsistent {
		if f.EvidenceRef == "" || f.FreshnessAt.IsZero() {
			f.Detail = joinInvalidationReason(f.Detail,
				"consistent finding without evidence reference/freshness downgraded to unknown")
			f.Verdict = ReverifyUnknown
			return f, nil
		}
		f.FreshnessAt = f.FreshnessAt.UTC()
	}
	return f, nil
}

// reverifyInvalidationTrigger maps a finding trigger onto the closed Q5
// vocabulary. Unknown/empty triggers default to new_evidence: a re-read that
// contradicts the recorded conclusion is new evidence, and the fallback can
// never widen the invalidation vocabulary.
func reverifyInvalidationTrigger(t InvalidationTrigger) InvalidationTrigger {
	if t.Valid() && t.AffectsConclusion() {
		return t
	}
	return InvalidationNewEvidence
}

// ReverifyEvaluator is the read-only per-item evidence re-read seam. An
// implementation MUST re-read evidence through the T013–T015 read-only
// adapters (never through a write path), charge its internal reads through the
// budget seam, and return a fail-closed finding. A returned error is a failed
// attempt (bounded retry; gap row), never a silent pass.
type ReverifyEvaluator interface {
	Reverify(ctx context.Context, item ClosedDiscrepancy, budget ScanQueryBudget) (ReverifyFinding, error)
}

// ReverifyEvaluatorFunc adapts a function to the seam (tests/assembly).
type ReverifyEvaluatorFunc func(ctx context.Context, item ClosedDiscrepancy, budget ScanQueryBudget) (ReverifyFinding, error)

// Reverify implements ReverifyEvaluator.
func (f ReverifyEvaluatorFunc) Reverify(ctx context.Context, item ClosedDiscrepancy, budget ScanQueryBudget) (ReverifyFinding, error) {
	if f == nil {
		return ReverifyFinding{}, contractErrorf("reverify evaluator is not wired")
	}
	return f(ctx, item, budget)
}

// ---------------------------------------------------------------------------
// T015-based evaluator
// ---------------------------------------------------------------------------

// EventStateReverifyEvaluator reuses the T015 read-only event-delivery adapter
// for the event-party dimension of one closed item:
//
//   - it re-reads the recorded scope's event evidence through Observe and maps
//     an incomplete/stale/unknown bundle onto unknown/stale (never consistent);
//   - a conclusively absent event, a changed block identity or a changed
//     recovery version of the recorded event is a divergent finding carrying
//     the matching invalidation trigger (reorg/new evidence);
//   - an unchanged, fully covered event party yields unknown: consistency
//     spans all three parties, so a single-party re-read can never prove it
//     (Q5-4). Callers that own the full T013–T015 compare path implement
//     ReverifyEvaluator directly to produce consistent findings.
type EventStateReverifyEvaluator struct {
	// Reader is the T015 read-only event surface (required).
	Reader EventStateReader
	// Consumers are the registered consumers whose delivery closure the
	// bundle must prove; an empty set keeps every bundle unclosed (unknown).
	Consumers []EventConsumerRegistration
	// Quarantine is the read-only quarantine surface; required when consumers
	// are registered (a poisoned event is never "not yet consumed").
	Quarantine EventQuarantineReader
	// UpstreamReceipts optionally carries the task's declared receipt sources;
	// a declaration never proves upstream success (FR-006).
	UpstreamReceipts []ChainUpstreamReceiptSource
	// ChainID is the numeric chain id used by the optional height window
	// resolver; 0 leaves the window unresolved (pending, never an absence).
	ChainID int64
	// HeightWindowResolver optionally resolves a height scope to its
	// chain-time window so blockless business-object rows are attributable
	// (T036 seam). A nil resolver keeps height scopes pending.
	HeightWindowResolver ScanHeightTimeWindowResolver
}

// Reverify implements ReverifyEvaluator without ever writing.
func (e *EventStateReverifyEvaluator) Reverify(ctx context.Context, item ClosedDiscrepancy, budget ScanQueryBudget) (ReverifyFinding, error) {
	if e == nil || e.Reader == nil {
		return ReverifyFinding{}, contractErrorf("event state reverify evaluator has no reader")
	}
	if budget == nil {
		return ReverifyFinding{}, contractErrorf("event state reverify evaluator requires a budget seam")
	}
	if item.Scope == nil {
		return ReverifyFinding{Verdict: ReverifyUnknown,
			Detail: "recorded evidence has no scope marker; event evidence cannot be re-read"}, nil
	}
	if item.BusinessKey.Kind != BusinessKeyEventID {
		return ReverifyFinding{Verdict: ReverifyUnknown,
			Detail: "identity is not an event-delivery identity; the event adapter cannot re-read it"}, nil
	}
	if _, err := uuid.Parse(strings.TrimSpace(item.BusinessKey.Value)); err != nil {
		return ReverifyFinding{Verdict: ReverifyUnknown,
			Detail: "event identity value is not a usable event id"}, nil
	}
	if len(e.Consumers) > 0 && e.Quarantine == nil {
		return ReverifyFinding{}, contractErrorf("event state reverify evaluator requires a quarantine reader when consumers are registered")
	}
	scope := *item.Scope
	interval, err := eventStateIntervalForScope(scope)
	if err != nil {
		return ReverifyFinding{Verdict: ReverifyUnknown, Detail: err.Error()}, nil
	}
	query := EventStateQuery{
		Scope:            scope,
		Interval:         interval,
		Consumers:        append([]EventConsumerRegistration(nil), e.Consumers...),
		Quarantine:       e.Quarantine,
		UpstreamReceipts: append([]ChainUpstreamReceiptSource(nil), e.UpstreamReceipts...),
	}
	// Height → chain-time window (T036): without it the event adapter refuses
	// to cover blockless rows and the bundle stays incomplete (pending).
	if scope.Kind == ScopeHeight && e.HeightWindowResolver != nil && e.ChainID > 0 {
		scanInterval := ScanInterval{From: HeightBound(scope.From), To: HeightBound(scope.To)}
		resolution, resolveErr := e.HeightWindowResolver.ResolveHeightTimeWindow(ctx, e.ChainID, scanInterval, budget)
		if resolveErr != nil {
			if ctxErr := scanContextError(resolveErr); ctxErr != nil {
				return ReverifyFinding{}, ctxErr
			}
			return ReverifyFinding{Verdict: ReverifyUnknown,
				Detail: "height time window resolution failed: " + boundedReverifyDetail(resolveErr.Error())}, nil
		}
		if err := resolution.Validate(); err != nil {
			return ReverifyFinding{}, err
		}
		if resolution.Status == WindowResolved {
			from, to := resolution.From.UTC(), resolution.To.UTC()
			query.OccurredFrom, query.OccurredTo = &from, &to
		}
	}

	if err := budget.ConsumePG(ctx, 1); err != nil {
		return ReverifyFinding{}, err
	}
	evidence, observeErr := e.Reader.Observe(ctx, query)
	if ctxErr := scanContextError(observeErr); ctxErr != nil {
		return ReverifyFinding{}, ctxErr
	}
	if observeErr != nil {
		return ReverifyFinding{
			Verdict:     ReverifyUnknown,
			EvidenceRef: boundedReverifyEvidenceRef(evidence.EvidenceRefs()),
			Detail:      "event evidence read failed: " + boundedReverifyDetail(observeErr.Error()),
		}, nil
	}
	if verdict, pending := evidence.PendingReverify(); pending {
		return ReverifyFinding{
			Verdict:     verdict,
			EvidenceRef: boundedReverifyEvidenceRef(evidence.EvidenceRefs()),
			Detail:      "event evidence is not conclusively covered: " + boundedReverifyDetail(strings.Join(evidence.Reasons, ",")),
		}, nil
	}

	observation, match := matchCandidateEventObservation(&evidence, item.BusinessKey)
	switch match {
	case eventMatchAmbiguous:
		return ReverifyFinding{
			Verdict:     ReverifyUnknown,
			EvidenceRef: boundedReverifyEvidenceRef(evidence.EvidenceRefs()),
			Detail:      "recorded event identity matches several observations; no version may be guessed",
		}, nil
	case eventMatchNone:
		// Complete coverage and the recorded event is gone: definite new
		// evidence against the recorded conclusion (never a silent pass).
		return ReverifyFinding{
			Verdict:     ReverifyDivergent,
			EvidenceRef: boundedReverifyEvidenceRef(evidence.EvidenceRefs()),
			FreshnessAt: evidence.CapturedAt,
			Trigger:     InvalidationNewEvidence,
			Detail:      "recorded event fact is absent under complete coverage",
		}, nil
	}

	observedBlock, observedHash := int64(0), ""
	if observation.BlockNumber != nil {
		observedBlock = *observation.BlockNumber
	}
	if observation.BlockHash != nil {
		observedHash = *observation.BlockHash
	}
	recordedBlock := int64(0)
	if item.Version.BlockNumber <= math.MaxInt64 {
		recordedBlock = int64(item.Version.BlockNumber)
	}
	recordedHash := item.Version.BlockHash
	blockChanged := observedBlock != recordedBlock ||
		(recordedHash != "" && !strings.EqualFold(observedHash, recordedHash))
	if blockChanged {
		trigger := InvalidationNewEvidence
		if recordedBlock > 0 && observedBlock > 0 {
			// Both identities exist and differ: a reorg replacement.
			trigger = InvalidationReorg
		}
		return ReverifyFinding{
			Verdict:     ReverifyDivergent,
			EvidenceRef: observation.EvidenceRef(),
			FreshnessAt: stableScanEvidenceInstant(nil, observation, nil, evidence.CapturedAt),
			Trigger:     trigger,
			Detail:      "recorded event block identity changed",
		}, nil
	}
	if observation.RecoveryVersion != nil && *observation.RecoveryVersion > 0 && item.Version.RecoveryVersion != "" {
		if fmt.Sprintf("%d", *observation.RecoveryVersion) != item.Version.RecoveryVersion {
			return ReverifyFinding{
				Verdict:     ReverifyDivergent,
				EvidenceRef: observation.EvidenceRef(),
				FreshnessAt: stableScanEvidenceInstant(nil, observation, nil, evidence.CapturedAt),
				Trigger:     InvalidationNewEvidence,
				Detail:      "recorded recovery version changed",
			}, nil
		}
	}
	// The event party is unchanged and fully covered, but consistency is a
	// three-party property: a single-party re-read cannot prove it (Q5-4).
	return ReverifyFinding{
		Verdict:     ReverifyUnknown,
		EvidenceRef: observation.EvidenceRef(),
		FreshnessAt: stableScanEvidenceInstant(nil, observation, nil, evidence.CapturedAt),
		Detail:      "event party unchanged; cross-party consistency is not provable from the event adapter alone",
	}, nil
}

// eventStateIntervalForScope builds the event-query interval of a recorded
// identity scope (height bounds or µs time bounds).
func eventStateIntervalForScope(scope IdentityScope) (EventStateInterval, error) {
	if err := scope.Validate(); err != nil {
		return EventStateInterval{}, err
	}
	switch scope.Kind {
	case ScopeHeight:
		return EventStateInterval{From: HeightBound(scope.From), To: HeightBound(scope.To)}, nil
	case ScopeTime:
		return EventStateInterval{
			From: TimeBound(time.UnixMicro(scope.From).UTC()),
			To:   TimeBound(time.UnixMicro(scope.To).UTC()),
		}, nil
	}
	return EventStateInterval{}, contractErrorf("recorded scope has unknown kind %q", scope.Kind)
}

// boundedReverifyEvidenceRef joins provenance refs into one bounded evidence
// reference (the reverify/audit bounds).
func boundedReverifyEvidenceRef(refs []EventEvidenceRef) string {
	parts := make([]string, 0, len(refs))
	for _, ref := range refs {
		parts = append(parts, ref.String())
	}
	joined := strings.Join(parts, " ")
	if joined == "" {
		return "events:v1 reverify"
	}
	return boundedReverifyDetail(joined)
}

// boundedReverifyDetail bounds a free-text detail/evidence annotation to the
// 512-byte audit/reverify bound, keeping the truncation visible.
func boundedReverifyDetail(text string) string {
	text = strings.TrimSpace(text)
	if len(text) <= scanEvidenceRefMax {
		return text
	}
	return text[:scanEvidenceRefMax-8] + " [bound]"
}

// ---------------------------------------------------------------------------
// Bounded sweep executor
// ---------------------------------------------------------------------------

// ReverifyStop names why one sweep slice ended.
type ReverifyStop string

const (
	// ReverifyStopWaterlineEnd: the directed enumeration reached the end of
	// the recorded evidence waterline (failed items may still be gapped).
	ReverifyStopWaterlineEnd ReverifyStop = "waterline_end"
	// ReverifyStopSliceExhausted: the slice quota stopped the pass; more
	// coverage remains for the next invocation.
	ReverifyStopSliceExhausted ReverifyStop = "slice_exhausted"
)

// ReverifySweepRequest is one bounded history-revalidation slice.
type ReverifySweepRequest struct {
	TaskID string
	// Actor is the audit actor; empty defaults to "system:reverify".
	Actor string
	// Slice bounds this invocation's work (required).
	Slice ReverifySlice
	// ParentBudget optionally links the slice to the scan invocation's total
	// budget: every slice charge then also consumes the task budget
	// (data-model.md §6). Nil runs a standalone bounded slice.
	ParentBudget *Budget
	// Evaluator is the read-only evidence re-read seam (required).
	Evaluator ReverifyEvaluator
	// CrossCheck carries the scan-loop cross-check candidates; they take
	// priority in the slice.
	CrossCheck []ReverifyCandidate
	// Now is the evidence-age instant; zero means time.Now().
	Now time.Time
	// Reason is an optional audit annotation.
	Reason string
}

// ReverifySweepResult is the observable outcome of one slice. None of the
// counters claims verified completeness; VerifiedComplete() is the only
// (conservative) completeness claim.
type ReverifySweepResult struct {
	TaskID             string
	CrossChecked       int
	CrossCheckSkipped  int
	Enumerated         int
	Retried            int
	Rechecked          int
	Consistent         int
	Divergent          int
	Pending            int
	Failed             int
	GapsWritten        int
	GapsResolved       int
	RetryExhausted     int
	InvalidationErrors int
	Cursor             *HistorySweepCursor
	CursorAdvanced     bool
	WaterlineEnd       bool
	OpenReverifyGaps   int64
	OpenGaps           int64
	Stop               ReverifyStop
	PGUsed             int
	Warnings           []string
	WarningsTruncated  int
}

// VerifiedComplete reports the only honest completeness claim: the directed
// waterline was reached, zero open uncovered-range rows remain for the task,
// no retry was exhausted, no revalidation failed, and the slice was not cut
// short. Any gap, any exhausted retry or any unswept remainder keeps it false
// (data-model.md §6: traversal cursor ≠ verified completeness; §1.3: a
// "fully consistent" conclusion requires zero open gaps).
func (r ReverifySweepResult) VerifiedComplete() bool {
	return r.WaterlineEnd && r.OpenGaps == 0 && r.OpenReverifyGaps == 0 &&
		r.RetryExhausted == 0 && r.Failed == 0 && r.Stop != ReverifyStopSliceExhausted
}

// addWarning records one bounded sweep warning.
func (r *ReverifySweepResult) addWarning(message string) {
	const maxWarnings = 16
	if len(r.Warnings) >= maxWarnings {
		r.WarningsTruncated++
		return
	}
	r.Warnings = append(r.Warnings, boundedReverifyDetail(message))
}

// reverifyPlanItem is one item selected for this slice plus its finder.
type reverifyPlanItem struct {
	item   ClosedDiscrepancy
	source string
}

// Finder tokens (stable audit/result vocabulary).
const (
	reverifySourceCrossCheck  = "cross_check"
	reverifySourceRetry       = "retry"
	reverifySourceEnumeration = "enumeration"
)

// reverifyPlan is the ordered slice selection plus its coverage signals.
type reverifyPlan struct {
	items             []reverifyPlanItem
	crossCheckSkipped int
	// waterlineEnd reports that the directed enumeration query returned fewer
	// rows than requested: no unswept item remains after the cursor.
	waterlineEnd bool
}

// RunReverifySweep runs one bounded history-revalidation slice (T027). It
// never closes a discrepancy, never disposes, and never calls recovery/replay/
// payment: consistent re-reads are recorded as reverify rows; divergent ones
// enter the approved reverify flow through the T026 evaluator.
func (s *Store) RunReverifySweep(ctx context.Context, req ReverifySweepRequest) (ReverifySweepResult, error) {
	result := ReverifySweepResult{TaskID: strings.TrimSpace(req.TaskID)}
	if s == nil || s.db == nil {
		return result, contractErrorf("store has no database")
	}
	if result.TaskID == "" {
		return result, contractErrorf("reverify sweep requires a task_id")
	}
	if err := req.Slice.Validate(); err != nil {
		return result, err
	}
	if req.Evaluator == nil {
		return result, contractErrorf("reverify sweep requires an evidence evaluator")
	}
	if len(req.Reason) > 1024 || strings.ContainsRune(req.Reason, 0) {
		return result, contractErrorf("reverify sweep reason is malformed")
	}
	actor := strings.TrimSpace(req.Actor)
	if actor == "" {
		actor = "system:reverify"
	}
	now := req.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}

	task, err := s.TaskByID(ctx, result.TaskID)
	if err != nil {
		return result, err
	}
	if task.State != TaskStateRunning {
		return result, fmt.Errorf("%w: state=%s", ErrTaskNotRunning, task.State)
	}
	scope, err := scanTaskIdentityScope(task)
	if err != nil {
		return result, err
	}
	cursor, err := s.HistorySweepThrough(ctx, result.TaskID)
	if err != nil {
		return result, err
	}
	result.Cursor = cursor
	budget := newReverifySliceBudget(req.ParentBudget, req.Slice.MaxPGRequests)

	finalize := func(stop ReverifyStop) (ReverifySweepResult, error) {
		result.Stop = stop
		result.PGUsed = budget.usedPG
		open, countErr := s.OpenGapCount(ctx, result.TaskID)
		if countErr != nil {
			return result, countErr
		}
		result.OpenGaps = open
		reverifyGaps, countErr := s.CountQueryFailedGaps(ctx, result.TaskID)
		if countErr != nil {
			return result, countErr
		}
		result.OpenReverifyGaps = reverifyGaps
		return result, nil
	}

	plan, stop, err := s.planReverifySweep(ctx, budget, scope, cursor, req)
	if err != nil {
		if errors.Is(err, ErrBudgetExhausted) {
			return finalize(ReverifyStopSliceExhausted)
		}
		return result, err
	}
	result.CrossCheckSkipped = plan.crossCheckSkipped
	result.WaterlineEnd = plan.waterlineEnd
	if stop != "" {
		return finalize(stop)
	}

	for _, planned := range plan.items {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		outcome, err := s.reverifyOne(ctx, req, budget, planned, actor, now)
		if err != nil {
			if errors.Is(err, ErrBudgetExhausted) {
				return finalize(ReverifyStopSliceExhausted)
			}
			return result, err
		}
		result.Rechecked++
		result.PGUsed = budget.usedPG
		switch outcome.verdict {
		case ReverifyConsistent:
			result.Consistent++
		case ReverifyDivergent:
			result.Divergent++
		default:
			result.Pending++
		}
		if outcome.failed {
			result.Failed++
		}
		if outcome.gapWritten {
			result.GapsWritten++
		}
		if outcome.gapResolved {
			result.GapsResolved++
		}
		if outcome.retryExhausted {
			result.RetryExhausted++
			result.addWarning(fmt.Sprintf(
				"discrepancy %s exhausted its %d bounded revalidation attempts; gap kept visible and escalated",
				planned.item.DiscrepancyID, req.Slice.MaxItemAttempts))
		}
		if outcome.invalidationError {
			result.InvalidationErrors++
			result.addWarning(fmt.Sprintf("discrepancy %s diverged but entering the reverify flow failed: %s",
				planned.item.DiscrepancyID, outcome.invalidationDetail))
		}
		if outcome.warning != "" {
			result.addWarning(outcome.warning)
		}
		switch planned.source {
		case reverifySourceCrossCheck:
			result.CrossChecked++
		case reverifySourceRetry:
			result.Retried++
		case reverifySourceEnumeration:
			result.Enumerated++
			if cursorAdvance(cursor, outcome.cursor) {
				cursor = outcome.cursor
				result.Cursor = cursor
				result.CursorAdvanced = true
			}
		}
	}

	// Persist the advanced traversal cursor. The cursor is progress only; the
	// result's completeness claim never derives from it alone.
	if result.CursorAdvanced {
		if err := budget.ConsumePG(ctx, 1); err != nil {
			if errors.Is(err, ErrBudgetExhausted) {
				return finalize(ReverifyStopSliceExhausted)
			}
			return result, err
		}
		if err := s.writeHistorySweepThrough(ctx, result.TaskID, cursor); err != nil {
			return result, err
		}
	}
	return finalize(ReverifyStopWaterlineEnd)
}

// planReverifySweep builds the ordered slice plan: scan-loop cross-check
// candidates first, then bounded retries (failure-count-asc + evidence-age-
// desc), then the directed enumeration from the traversal cursor (oldest
// evidence first). A non-empty stop means the slice quota was reached while
// planning.
func (s *Store) planReverifySweep(ctx context.Context, budget *reverifySliceBudget, scope IdentityScope,
	cursor *HistorySweepCursor, req ReverifySweepRequest) (reverifyPlan, ReverifyStop, error) {
	plan := reverifyPlan{items: make([]reverifyPlanItem, 0, req.Slice.MaxItems)}
	seen := make(map[string]struct{}, req.Slice.MaxItems)
	add := func(item ClosedDiscrepancy, source string) {
		if item.DiscrepancyID == "" {
			return
		}
		if _, ok := seen[item.DiscrepancyID]; ok {
			return
		}
		if len(plan.items) >= req.Slice.MaxItems {
			return
		}
		seen[item.DiscrepancyID] = struct{}{}
		plan.items = append(plan.items, reverifyPlanItem{item: item, source: source})
	}

	// Finder 1: the scan loop's cross-check candidates (priority).
	for _, candidate := range req.CrossCheck {
		if len(plan.items) >= req.Slice.MaxItems {
			return plan, ReverifyStopSliceExhausted, nil
		}
		id := strings.TrimSpace(candidate.DiscrepancyID)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		item, found, err := s.loadClosedDiscrepancy(ctx, budget, id, req.Slice.MaxItemAttempts)
		if err != nil {
			return reverifyPlan{}, "", err
		}
		if !found {
			// The item left closed (or never existed): nothing to revalidate.
			plan.crossCheckSkipped++
			continue
		}
		plan.items = append(plan.items, reverifyPlanItem{item: item, source: reverifySourceCrossCheck})
	}

	remaining := req.Slice.MaxItems - len(plan.items)
	if remaining <= 0 {
		return plan, ReverifyStopSliceExhausted, nil
	}

	// Finder 2: bounded retries of previously failed items. Ordering is
	// failure-count-asc + evidence-age-desc so a failing item never crowds out
	// the rest of the slice.
	retries, err := s.listReverifyItems(ctx, budget, scope,
		req.Slice.MaxItemAttempts, remaining, listReverifyRetryCandidatesSQL, nil)
	if err != nil {
		return reverifyPlan{}, "", err
	}
	for _, item := range retries {
		add(item, reverifySourceRetry)
	}
	remaining = req.Slice.MaxItems - len(plan.items)
	if remaining <= 0 {
		return plan, ReverifyStopSliceExhausted, nil
	}

	// Finder 3: directed enumeration from the traversal cursor, oldest
	// evidence first. Fewer rows than requested means the waterline end.
	fresh, err := s.listReverifyItems(ctx, budget, scope,
		req.Slice.MaxItemAttempts, remaining, listReverifyNewCandidatesSQL, cursor)
	if err != nil {
		return reverifyPlan{}, "", err
	}
	plan.waterlineEnd = len(fresh) < remaining
	for _, item := range fresh {
		add(item, reverifySourceEnumeration)
	}
	return plan, "", nil
}

// reverifyOutcome is the internal per-item result.
type reverifyOutcome struct {
	verdict            ReverifyVerdict
	failed             bool
	gapWritten         bool
	gapResolved        bool
	retryExhausted     bool
	invalidationError  bool
	invalidationDetail string
	cursor             *HistorySweepCursor
	warning            string
}

// reverifyOne re-reads one item's evidence and persists the outcome in one
// short transaction (reverify row + gap + audit). A divergent finding then
// enters the approved reverify flow through the T026 evaluator.
func (s *Store) reverifyOne(ctx context.Context, req ReverifySweepRequest, budget *reverifySliceBudget,
	planned reverifyPlanItem, actor string, now time.Time) (reverifyOutcome, error) {
	item := planned.item
	outcome := reverifyOutcome{}

	itemCtx := ctx
	if req.Slice.MaxItemDuration > 0 {
		var cancel context.CancelFunc
		itemCtx, cancel = context.WithTimeout(ctx, req.Slice.MaxItemDuration)
		defer cancel()
	}
	finding, evalErr := req.Evaluator.Reverify(itemCtx, item, budget)
	if evalErr != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return outcome, ctxErr
		}
		if errors.Is(evalErr, ErrBudgetExhausted) {
			return outcome, evalErr
		}
		finding = ReverifyFinding{Verdict: ReverifyUnknown,
			Detail: "evidence re-read failed: " + boundedReverifyDetail(evalErr.Error())}
		if errors.Is(evalErr, context.DeadlineExceeded) || itemCtx.Err() != nil {
			finding.Detail = "evidence re-read timed out"
		}
	}
	finding, err := normalizeReverifyFinding(finding)
	if err != nil {
		return outcome, err
	}
	outcome.verdict = finding.Verdict
	failureToken := ""
	if evalErr != nil {
		failureToken = reverifyFailureToken(evalErr, itemCtx)
	}

	// Persist the verdict and its coverage marker in one short transaction.
	if err := budget.ConsumePG(ctx, 1); err != nil {
		return outcome, err
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return outcome, fmt.Errorf("begin reverify persist: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	failed := finding.Verdict != ReverifyConsistent && finding.Verdict != ReverifyDivergent
	outcome.failed = failed
	gapWritten, gapResolved, err := persistReverifyOutcomeTx(ctx, tx, req.TaskID,
		actor, "history", item.reverifyItem(), item.Scope, finding, planned.source, failureToken, req.Reason)
	if err != nil {
		return outcome, err
	}
	outcome.gapWritten, outcome.gapResolved = gapWritten, gapResolved
	if err := tx.Commit(ctx); err != nil {
		return outcome, fmt.Errorf("commit reverify outcome: %w", err)
	}

	// Bounded retry bookkeeping: an item that exhausted its attempts is
	// escalated once, keeps its gap visible, and stops consuming slice slots.
	if failed && item.FailCount+1 >= int64(req.Slice.MaxItemAttempts) {
		exhaustedRecorded, err := s.reverifyRetryExhaustedRecorded(ctx, item.DiscrepancyID)
		if err != nil {
			return outcome, err
		}
		if !exhaustedRecorded {
			if err := budget.ConsumePG(ctx, 1); err != nil {
				return outcome, err
			}
			if err := s.recordReverifyRetryExhausted(ctx, item.reverifyItem(), actor, now,
				req.Slice.MaxItemAttempts, "history", planned.source); err != nil {
				return outcome, err
			}
		}
		outcome.retryExhausted = true
	}

	// A divergent re-read enters the approved reverify flow only: the T026
	// evaluator moves the closed item to pending_verify (reorg/new evidence ->
	// reverify only, never auto-disposal). An invalidation failure is recorded
	// and surfaced, never silently swallowed.
	if finding.Verdict == ReverifyDivergent {
		if err := budget.ConsumePG(ctx, 1); err != nil {
			return outcome, err
		}
		_, invErr := s.EvaluateInvalidationForDiscrepancy(ctx, DiscrepancyInvalidationRequest{
			DiscrepancyID: item.DiscrepancyID,
			Signal: &InvalidationSignal{
				Trigger:     reverifyInvalidationTrigger(finding.Trigger),
				EvidenceRef: finding.EvidenceRef,
			},
			Actor:  actor,
			Reason: "history revalidation divergent: " + finding.Detail,
			Now:    now,
		})
		if invErr != nil {
			outcome.invalidationError = true
			outcome.invalidationDetail = boundedReverifyDetail(invErr.Error())
		}
	}

	if planned.source == reverifySourceEnumeration {
		outcome.cursor = &HistorySweepCursor{EvidenceAt: item.EvidenceAt, DiscrepancyID: item.DiscrepancyID}
	}
	return outcome, nil
}

// reverifyFailureToken classifies an evaluator error into the bounded failure
// vocabulary (an item timeout is never an abort; the parent context is).
func reverifyFailureToken(err error, itemCtx context.Context) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, context.DeadlineExceeded),
		itemCtx != nil && errors.Is(itemCtx.Err(), context.DeadlineExceeded):
		return "timeout"
	default:
		return "query_failed"
	}
}

// cursorAdvance reports whether next is strictly after current in
// oldest-evidence-first order (evidence time, then discrepancy id). A nil
// current cursor is always behind a valid next.
func cursorAdvance(current *HistorySweepCursor, next *HistorySweepCursor) bool {
	if next == nil || !next.Valid() {
		return false
	}
	if current == nil || !current.Valid() {
		return true
	}
	nextAt, currentAt := next.EvidenceAt.UTC(), current.EvidenceAt.UTC()
	if nextAt.After(currentAt) {
		return true
	}
	if nextAt.Before(currentAt) {
		return false
	}
	return next.DiscrepancyID > current.DiscrepancyID
}

// SQL for the history-revalidation sweep. Writes touch only the 014-owned
// reverify/recon_gap/recon_audit tables and the task's traversal cursor.

const readHistorySweepSQL = `
SELECT COALESCE(history_sweep_through, 'null'::jsonb)
FROM recon_task
WHERE task_id = $1`

const writeHistorySweepSQL = `
UPDATE recon_task
SET history_sweep_through = $2::jsonb, updated_at = now()
WHERE task_id = $1`

const lockHistorySweepSQL = `
SELECT COALESCE(history_sweep_through, 'null'::jsonb)
FROM recon_task
WHERE task_id = $1
FOR UPDATE`

const countQueryFailedGapsSQL = `
SELECT count(*)::bigint FROM recon_gap
WHERE task_id = $1 AND reason = 'query_failed'`

// reverifyScopePredicate selects the closed items of one identity scope
// (chain, kind, containment inside the task scope). A row without the scope
// marker is not provably in scope and is conservatively excluded.
const reverifyScopePredicate = `
  d.state = 'closed'
  AND d.evidence_version_domain->'scope'->>'chain_id' = $1
  AND d.evidence_version_domain->'scope'->>'kind' = $2
  AND (d.evidence_version_domain->'scope'->>'from')::bigint >= $3
  AND (d.evidence_version_domain->'scope'->>'to')::bigint <= $4`

// readClosedDiscrepancySQL loads one closed item by id; $2 bounds the failure
// count (the bounded retry cap).
const readClosedDiscrepancySQL = `
SELECT d.discrepancy_id::text, d.category, d.business_key, d.content_hash,
       d.evidence_version_domain, COALESCE(d.close_basis, '{}'::jsonb),
       d.reopen_count::bigint,
       ` + reverifyEvidenceAtExpr + `,
       COALESCE((
           SELECT count(*) FROM (
               SELECT 1 FROM recon_audit a
               WHERE a.action = 'reverify'
                 AND a.result IN ('query_failed', 'timeout', 'unknown', 'stale')
                 AND a.target->>'discrepancy_id' = d.discrepancy_id::text
               LIMIT $2
           ) bounded), 0)::bigint
FROM discrepancy d
WHERE d.discrepancy_id = $1 AND d.state = 'closed'`

// listReverifyRetryCandidatesSQL lists items with prior failed/inconclusive
// attempts whose latest verdict is not consistent, ordered failure-count-asc +
// evidence-age-desc (oldest evidence first) so no item starves the slice. The
// cumulative attempt count is capped by $5, so a recovered item drops out of
// the pool while its history stays append-only. $6 is the remaining slice
// size.
const listReverifyRetryCandidatesSQL = `
SELECT q.id::text, q.category, q.business_key, q.content_hash, q.domain, q.close_basis,
       q.reopen_count, q.evidence_at, q.fail_count
FROM (
    SELECT d.discrepancy_id AS id, d.category, d.business_key, d.content_hash,
           d.evidence_version_domain AS domain, COALESCE(d.close_basis, '{}'::jsonb) AS close_basis,
           d.reopen_count::bigint AS reopen_count,
           ` + reverifyEvidenceAtExpr + ` AS evidence_at,
           COALESCE((
               SELECT count(*) FROM (
                   SELECT 1 FROM recon_audit a
                   WHERE a.action = 'reverify'
                     AND a.result IN ('query_failed', 'timeout', 'unknown', 'stale')
                     AND a.target->>'discrepancy_id' = d.discrepancy_id::text
                   LIMIT $5
               ) bounded), 0)::bigint AS fail_count,
           COALESCE((
               SELECT r.verdict FROM reverify r
               WHERE r.discrepancy_id = d.discrepancy_id
               ORDER BY r.created_at DESC, r.reverify_id DESC
               LIMIT 1
           ), '') AS latest_verdict
    FROM discrepancy d
    WHERE ` + reverifyScopePredicate + `
) q
WHERE q.fail_count > 0 AND q.fail_count < $5
  AND q.latest_verdict <> 'consistent'
ORDER BY q.fail_count ASC, q.evidence_at ASC, q.id ASC
LIMIT $6`

// listReverifyNewCandidatesSQL lists the not-yet-swept closed items after the
// traversal cursor, oldest evidence first. $5/$6 are the cursor (nullable),
// $7 the bounded retry cap, $8 the remaining slice size. The failure filter
// applies before LIMIT so an exhausted item never makes the waterline look
// finished early.
const listReverifyNewCandidatesSQL = `
SELECT q.id::text, q.category, q.business_key, q.content_hash, q.domain, q.close_basis,
       q.reopen_count, q.evidence_at, q.fail_count
FROM (
    SELECT f.id AS id, f.category, f.business_key, f.content_hash, f.domain, f.close_basis,
           f.reopen_count, f.evidence_at, f.fail_count
    FROM (
        SELECT d.discrepancy_id AS id, d.category, d.business_key, d.content_hash,
               d.evidence_version_domain AS domain, COALESCE(d.close_basis, '{}'::jsonb) AS close_basis,
               d.reopen_count::bigint AS reopen_count,
               ` + reverifyEvidenceAtExpr + ` AS evidence_at,
               COALESCE((
                   SELECT count(*) FROM (
                       SELECT 1 FROM recon_audit a
                       WHERE a.action = 'reverify'
                         AND a.result IN ('query_failed', 'timeout', 'unknown', 'stale')
                         AND a.target->>'discrepancy_id' = d.discrepancy_id::text
                       LIMIT $7
                   ) bounded), 0)::bigint AS fail_count
        FROM discrepancy d
        WHERE ` + reverifyScopePredicate + `
    ) f
    WHERE f.fail_count < $7
      AND ($5::timestamptz IS NULL
           OR (f.evidence_at, f.id) > ($5::timestamptz, $6::uuid))
    ORDER BY f.evidence_at ASC, f.id ASC
    LIMIT $8
) q
ORDER BY q.evidence_at ASC, q.id ASC`

// insertReverifySQL appends one read-only revalidation verdict.
const insertReverifySQL = `
INSERT INTO reverify (discrepancy_id, verdict, evidence_ref, freshness_at)
VALUES ($1::uuid, $2, $3, $4)`

// insertReverifyGapSQL records one visible coverage gap for a failed/pending
// revalidation without duplicating an identical open row.
const insertReverifyGapSQL = `
INSERT INTO recon_gap (task_id, range_start, range_end, range_start_at, range_end_at, reason)
SELECT $1::uuid, $2, $3, $4, $5, $6
WHERE NOT EXISTS (
    SELECT 1 FROM recon_gap
    WHERE task_id = $1::uuid AND reason = $6
      AND range_start IS NOT DISTINCT FROM $2
      AND range_end IS NOT DISTINCT FROM $3
      AND range_start_at IS NOT DISTINCT FROM $4
      AND range_end_at IS NOT DISTINCT FROM $5)`

// deleteReverifyGapSQL removes the coverage gaps of one position from the
// given reason list (a successful revalidation eliminates them; migration
// 000016: gap rows are removed only when the range is successfully covered).
const deleteReverifyGapSQL = `
DELETE FROM recon_gap
WHERE task_id = $1::uuid
  AND reason = ANY($2::text[])
  AND range_start IS NOT DISTINCT FROM $3
  AND range_end IS NOT DISTINCT FROM $4
  AND range_start_at IS NOT DISTINCT FROM $5
  AND range_end_at IS NOT DISTINCT FROM $6`

// reverifyRetryExhaustedExistsSQL reports whether an item's escalation row
// already exists (escalation is recorded once).
const reverifyRetryExhaustedExistsSQL = `
SELECT count(*)::bigint FROM recon_audit
WHERE action = 'reverify' AND result = 'retry_exhausted'
  AND target->>'discrepancy_id' = $1`

// reverifyItemRow is one raw closed-item row.
type reverifyItemRow struct {
	id          string
	category    string
	businessKey string
	contentHash []byte
	domain      []byte
	closeBasis  []byte
	reopenCount int64
	evidenceAt  time.Time
	failCount   int64
}

// scanReverifyItemRow scans the shared closed-item column list.
func scanReverifyItemRow(row pgx.Row) (reverifyItemRow, error) {
	var raw reverifyItemRow
	err := row.Scan(&raw.id, &raw.category, &raw.businessKey, &raw.contentHash,
		&raw.domain, &raw.closeBasis, &raw.reopenCount, &raw.evidenceAt, &raw.failCount)
	if err != nil {
		return raw, err
	}
	return raw, nil
}

// materializeClosedDiscrepancy validates one raw row into a revalidation item.
// A malformed row returns the item with only its id/evidence time populated,
// plus an error: the caller records a visible failure instead of guessing.
func materializeClosedDiscrepancy(raw reverifyItemRow) (ClosedDiscrepancy, error) {
	item := ClosedDiscrepancy{
		DiscrepancyID: raw.id,
		ContentHash:   raw.contentHash,
		CloseBasis:    raw.closeBasis,
		ReopenCount:   raw.reopenCount,
		EvidenceAt:    raw.evidenceAt.UTC(),
		FailCount:     raw.failCount,
	}
	kind, value, ok := strings.Cut(raw.businessKey, "=")
	if !ok || strings.TrimSpace(value) == "" {
		return item, contractErrorf("discrepancy %s has a malformed business key", raw.id)
	}
	item.BusinessKey = BusinessKey{Kind: BusinessKeyKind(kind), Value: value}
	if err := item.BusinessKey.Validate(); err != nil {
		return item, err
	}
	item.Category = Category(raw.category)
	if !item.Category.Known() {
		return item, contractErrorf("discrepancy %s has unknown category %q", raw.id, raw.category)
	}
	doc, err := ParsePersistedEvidenceDomain(raw.domain)
	if err != nil {
		return item, err
	}
	if !doc.EvidenceAt.IsZero() {
		item.EvidenceAt = doc.EvidenceAt.UTC()
	}
	if doc.Scope != nil {
		scope := *doc.Scope
		if err := scope.Validate(); err != nil {
			return item, err
		}
		item.Scope = &scope
	}
	item.Version = doc.VersionDomain
	return item, nil
}

// loadClosedDiscrepancy reads one closed item by id. found=false means the row
// does not exist or has left the closed state (nothing to revalidate). A
// malformed row is returned as a found item so it is recorded as a visible
// failed attempt, never silently skipped.
func (s *Store) loadClosedDiscrepancy(ctx context.Context, budget *reverifySliceBudget, id string,
	maxAttempts int) (ClosedDiscrepancy, bool, error) {
	if _, err := uuid.Parse(strings.TrimSpace(id)); err != nil {
		return ClosedDiscrepancy{}, false, contractErrorf("reverify item id %q is not a UUID", id)
	}
	if err := budget.ConsumePG(ctx, 1); err != nil {
		return ClosedDiscrepancy{}, false, err
	}
	raw, err := scanReverifyItemRow(s.db.QueryRow(ctx, readClosedDiscrepancySQL, strings.TrimSpace(id), maxAttempts))
	if errors.Is(err, pgx.ErrNoRows) {
		return ClosedDiscrepancy{}, false, nil
	}
	if err != nil {
		return ClosedDiscrepancy{}, false, fmt.Errorf("read closed discrepancy: %w", err)
	}
	item, _ := materializeClosedDiscrepancy(raw)
	return item, true, nil
}

// listReverifyItems runs one bounded candidate query and materializes its rows
// in SQL order (the caller owns slice truncation). Malformed rows are kept as
// visible failed items (id + evidence time populated).
func (s *Store) listReverifyItems(ctx context.Context, budget *reverifySliceBudget, scope IdentityScope,
	maxAttempts, limit int, query string, cursor *HistorySweepCursor) ([]ClosedDiscrepancy, error) {
	if limit <= 0 {
		return nil, nil
	}
	if err := budget.ConsumePG(ctx, 1); err != nil {
		return nil, err
	}
	var (
		rows pgx.Rows
		err  error
	)
	switch query {
	case listReverifyRetryCandidatesSQL:
		rows, err = s.reverifyRows(ctx, query, scope.ChainID, string(scope.Kind), scope.From, scope.To, maxAttempts, limit)
	case listReverifyNewCandidatesSQL:
		var (
			cursorAt any
			cursorID any
		)
		if cursor != nil && cursor.Valid() {
			cursorAt = cursor.EvidenceAt.UTC()
			cursorID = cursor.DiscrepancyID
		}
		rows, err = s.reverifyRows(ctx, query, scope.ChainID, string(scope.Kind), scope.From, scope.To,
			cursorAt, cursorID, maxAttempts, limit)
	default:
		return nil, contractErrorf("unknown reverify candidate query")
	}
	if err != nil {
		return nil, fmt.Errorf("list reverify candidates: %w", err)
	}
	defer rows.Close()
	var items []ClosedDiscrepancy
	for rows.Next() {
		raw, err := scanReverifyItemRow(rows)
		if err != nil {
			return nil, fmt.Errorf("scan reverify candidate: %w", err)
		}
		item, _ := materializeClosedDiscrepancy(raw)
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reverify candidate rows: %w", err)
	}
	return items, nil
}

// reverifyRows opens one read-only multi-row query in its own short
// transaction; the wrapper releases the transaction when the rows are drained.
func (s *Store) reverifyRows(ctx context.Context, query string, args ...any) (pgx.Rows, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin reverify read: %w", err)
	}
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return nil, err
	}
	return &reverifyReadRows{Rows: rows, tx: tx}, nil
}

// reverifyReadRows closes its read transaction once the rows are drained.
type reverifyReadRows struct {
	pgx.Rows
	tx pgx.Tx
}

// Close releases the rows and rolls back the read-only transaction.
func (r *reverifyReadRows) Close() {
	r.Rows.Close()
	_ = r.tx.Rollback(context.Background())
}

// writeHistorySweepThrough persists the advanced traversal cursor under the
// task row lock, and only when the new position is strictly after the
// recorded one: the waterline never regresses even if two bounded sweeps
// interleave (a lost race just leaves the position for the next invocation).
func (s *Store) writeHistorySweepThrough(ctx context.Context, taskID string, cursor *HistorySweepCursor) error {
	if cursor == nil || !cursor.Valid() {
		return contractErrorf("history sweep cursor is incomplete")
	}
	raw, err := json.Marshal(cursor)
	if err != nil {
		return fmt.Errorf("marshal history sweep cursor: %w", err)
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin history sweep cursor write: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	var recorded []byte
	err = tx.QueryRow(ctx, lockHistorySweepSQL, taskID).Scan(&recorded)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: task_id %s", ErrTaskNotFound, taskID)
	}
	if err != nil {
		return fmt.Errorf("lock history sweep cursor: %w", err)
	}
	current, err := parseHistorySweepCursor(recorded)
	if err != nil {
		return err
	}
	if !cursorAdvance(current, cursor) {
		// Not an advance: keep the recorded waterline untouched.
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit history sweep cursor no-op: %w", err)
		}
		return nil
	}
	tag, err := tx.Exec(ctx, writeHistorySweepSQL, taskID, string(raw))
	if err != nil {
		return fmt.Errorf("write history sweep cursor: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: task_id %s", ErrTaskNotFound, taskID)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit history sweep cursor: %w", err)
	}
	return nil
}

// reverifyGapPosition is the coverage position of one item's gap row: the
// recorded evidence block/instant, or (last resort) the item's recorded scope
// range.
type reverifyGapPosition struct {
	start RangeBound
	end   RangeBound
}

// reverifyItem is the common carrier of one re-verification subject: the
// closed-history sweep and the production pending_verify entry both persist
// their outcome through the same writer. The fields are exactly what the
// verdict row, the coverage gap and the audit trail need.
type reverifyItem struct {
	DiscrepancyID string
	Version       VersionDomain
	EvidenceAt    time.Time
	FailCount     int64
}

// gapPosition derives the deterministic gap position of one item. ok=false
// means no position can be represented; the failure stays visible through the
// reverify/audit rows instead of fabricating coverage. The position is the
// recorded evidence block/instant (last resort: the recorded scope range), so
// a successful retry deletes exactly the gap it resolves; per-item retry
// bookkeeping lives in the append-only reverify/audit trail, not in the gap
// row, so two items sharing one evidence position still stay individually
// visible and retryable.
func (i reverifyItem) gapPosition(scope *IdentityScope) (reverifyGapPosition, bool) {
	if i.Version.BlockNumber > 0 && i.Version.BlockNumber <= math.MaxInt64 {
		n := int64(i.Version.BlockNumber)
		return reverifyGapPosition{start: HeightBound(n), end: HeightBound(n)}, true
	}
	if !i.EvidenceAt.IsZero() {
		at := TimeBound(i.EvidenceAt)
		return reverifyGapPosition{start: at, end: at}, true
	}
	if scope != nil {
		switch scope.Kind {
		case ScopeHeight:
			return reverifyGapPosition{
				start: HeightBound(scope.From),
				end:   HeightBound(scope.To),
			}, true
		case ScopeTime:
			return reverifyGapPosition{
				start: TimeBound(time.UnixMicro(scope.From).UTC()),
				end:   TimeBound(time.UnixMicro(scope.To).UTC()),
			}, true
		}
	}
	return reverifyGapPosition{}, false
}

// persistReverifyOutcomeTx writes one item's verdict, coverage marker and
// audit row inside the caller's short transaction. It writes only the
// 014-owned reverify/recon_gap/recon_audit tables. sweep names the audit
// source discriminator ("history" for the closed sweep, "ticket" for the
// pending_verify entry).
func persistReverifyOutcomeTx(ctx context.Context, tx pgx.Tx, taskID, actor string, sweep string,
	item reverifyItem, scope *IdentityScope, finding ReverifyFinding, source, failureToken, reason string) (gapWritten, gapResolved bool, err error) {
	if _, err := tx.Exec(ctx, insertReverifySQL, item.DiscrepancyID, string(finding.Verdict),
		finding.EvidenceRef, nullableReverifyFreshness(finding)); err != nil {
		return false, false, fmt.Errorf("insert reverify row: %w", err)
	}
	if position, ok := item.gapPosition(scope); ok {
		startHeight, startAt := rangePairArgs(position.start)
		endHeight, endAt := rangePairArgs(position.end)
		if finding.Verdict == ReverifyConsistent {
			tag, err := tx.Exec(ctx, deleteReverifyGapSQL, taskID,
				[]string{string(GapQueryFailed), string(GapFreshnessHold)},
				startHeight, endHeight, startAt, endAt)
			if err != nil {
				return false, false, fmt.Errorf("delete reverify gap: %w", err)
			}
			gapResolved = tag.RowsAffected() > 0
		} else {
			gapReason := string(GapQueryFailed)
			if finding.Verdict == ReverifyStale {
				gapReason = string(GapFreshnessHold)
			}
			tag, err := tx.Exec(ctx, insertReverifyGapSQL, taskID,
				startHeight, endHeight, startAt, endAt, gapReason)
			if err != nil {
				return false, false, fmt.Errorf("insert reverify gap: %w", err)
			}
			gapWritten = tag.RowsAffected() > 0
		}
	}

	result := string(finding.Verdict)
	if failureToken != "" {
		result = failureToken
	}
	target := map[string]any{
		"sweep":          sweep,
		"task_id":        taskID,
		"discrepancy_id": item.DiscrepancyID,
		"source":         source,
		"verdict":        string(finding.Verdict),
		"attempt":        item.FailCount + 1,
		"auto_disposal":  false,
	}
	if !item.EvidenceAt.IsZero() {
		target["evidence_at"] = item.EvidenceAt.UTC().Format(time.RFC3339Nano)
	}
	if finding.EvidenceRef != "" {
		target["evidence_ref"] = finding.EvidenceRef
	}
	if finding.Detail != "" {
		target["detail"] = finding.Detail
	}
	if finding.Scope != "" {
		target["scope"] = finding.Scope
	}
	if finding.Version != nil {
		target["version"] = map[string]any{
			"block_number":          finding.Version.BlockNumber,
			"block_hash":            finding.Version.BlockHash,
			"recovery_version":      finding.Version.RecoveryVersion,
			"authorization_version": finding.Version.AuthorizationVersion,
			"scope_version":         finding.Version.ScopeVersion,
			"state_version":         finding.Version.StateVersion,
			"evidence_at":           finding.Version.EvidenceAt.UTC().Format(time.RFC3339Nano),
		}
	}
	auditReason := strings.TrimSpace(reason)
	if auditReason == "" {
		auditReason = "history revalidation " + result
	}
	if err := insertAuditTx(ctx, tx, AuditRecord{
		Actor:    actor,
		Action:   AuditActionReverify,
		Target:   target,
		Reason:   auditReason,
		Evidence: finding.EvidenceRef,
		Result:   result,
	}); err != nil {
		return gapWritten, gapResolved, err
	}
	return gapWritten, gapResolved, nil
}

// nullableReverifyFreshness keeps the reverify freshness NULL for verdicts
// that do not claim consistency (only a consistent verdict carries a
// freshness; the migration CHECK enforces the evidence-reference side).
func nullableReverifyFreshness(finding ReverifyFinding) any {
	if finding.Verdict == ReverifyConsistent && !finding.FreshnessAt.IsZero() {
		return finding.FreshnessAt.UTC()
	}
	return nil
}

// reverifyRetryExhaustedRecorded reports whether the escalation row of one
// item already exists.
func (s *Store) reverifyRetryExhaustedRecorded(ctx context.Context, discrepancyID string) (bool, error) {
	var count int64
	if err := s.db.QueryRow(ctx, reverifyRetryExhaustedExistsSQL, discrepancyID).Scan(&count); err != nil {
		return false, fmt.Errorf("read retry exhaustion row: %w", err)
	}
	return count > 0, nil
}

// recordReverifyRetryExhausted appends the one-time escalation row for an item
// that exhausted its bounded retry cap: the gap stays visible and the row is
// the alert hook, but the item stops occupying slice slots.
func (s *Store) recordReverifyRetryExhausted(ctx context.Context, item reverifyItem, actor string,
	now time.Time, maxAttempts int, sweep, source string) error {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin retry exhaustion audit: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	target := map[string]any{
		"sweep":          sweep,
		"discrepancy_id": item.DiscrepancyID,
		"source":         source,
		"max_attempts":   maxAttempts,
		"auto_disposal":  false,
	}
	if !item.EvidenceAt.IsZero() {
		age := now.UTC().Sub(item.EvidenceAt.UTC())
		if age < 0 {
			age = 0
		}
		target["evidence_at"] = item.EvidenceAt.UTC().Format(time.RFC3339Nano)
		target["evidence_age"] = age.String()
	}
	if err := insertAuditTx(ctx, tx, AuditRecord{
		Actor:  actor,
		Action: AuditActionReverify,
		Target: target,
		Reason: "bounded revalidation attempts exhausted; gap kept visible and escalated",
		Result: "retry_exhausted",
	}); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit retry exhaustion audit: %w", err)
	}
	return nil
}
