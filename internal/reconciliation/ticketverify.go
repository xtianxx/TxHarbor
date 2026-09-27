// ticketverify.go implements the production pending_verify re-verification
// entry (the US3 gap this file closes): a bounded, authorized, read-only
// full three-way re-comparison of one `pending_verify` ticket through the same
// chain/PG/event adapters, the same attribution-free identity rules, the same
// T017 classifier and the same T040 R1/R2/R3 expectation discriminator the
// scan compare loop uses.
//
// Why a separate entry (not an extension of the closed-history sweep): the
// sweep is a bounded traversal over `closed` items with a persisted
// oldest-evidence-first cursor (data-model.md §6); it never re-verifies a
// `pending_verify` item, and its `--max-items`/cursor semantics do not apply
// to a single ticket. The ticket entry is random-access by discrepancy id,
// writes no cursor, and its verdict is the evidence a `close` consumes. Both
// share the reverify row/gap/audit writer and the same fail-closed rules.
//
// Hard rules (Q1/Q5, contracts/discrepancy-lifecycle.md, quickstart §11):
//
//   - only a real full comparison may persist `consistent`: all three parties
//     must have been read through the production adapters on complete, fresh,
//     coverage-closed evidence, and the recorded conclusion must be resolved
//     (no remaining divergence, no tx-aggregate partial recording, no
//     recorded-recovery rotation, no block-identity change);
//   - single-party unchanged, candidate-not-found, both-absent, query failure,
//     possible retention trim, unknown coverage, an unreadable recorded
//     identity or a legacy ticket without enough recorded evidence to re-read
//     the claim all persist unknown/stale/divergent plus a visible gap, and can
//     never support a close;
//   - the entry never disposes, never closes, never recovers/replays/pays and
//     never writes outside the 014-owned reverify/recon_gap/recon_audit tables
//     and the task's own budget accounting;
//   - the entry is bounded (required positive slice bounds, parent-budget
//     charges, bounded per-item duration/attempts) and only runs on a running
//     task: a paused/cancelled task stops 014 work exactly like the scan;
//   - the verdict is written under the discrepancy row lock with a
//     state-predicate CAS: if the ticket left `pending_verify` while the slow
//     evidence read was in flight (an operator close won the race), the
//     outcome is discarded and audited, never written as fresh evidence.
package reconciliation

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// The stable audit source tokens of the two re-verification entries.
const (
	// ReverifySourceTicket marks the production pending_verify ticket entry.
	ReverifySourceTicket = "ticket"
)

var (
	// ErrTicketNotPendingVerify marks a ticket re-verification refused because
	// the ticket is not in the pending_verify state (only the post-dispose
	// verification gate may re-verify a ticket).
	ErrTicketNotPendingVerify = errors.New("reconciliation: ticket is not pending_verify")

	// ErrTicketVerifyExhausted marks a ticket whose bounded re-validation
	// attempts are exhausted: the gap stays visible, but no further attempt is
	// taken by this entry (the escalation is audited once).
	ErrTicketVerifyExhausted = errors.New("reconciliation: pending_verify re-validation attempts exhausted")

	// ErrTicketOutsideTaskScope marks a ticket whose recorded scope is not
	// contained in the task scope the invocation authorizes: the ticket cannot
	// be verified under this task, and no guess is made about its scope.
	ErrTicketOutsideTaskScope = errors.New("reconciliation: ticket scope is outside the task scope")
)

// TicketVerifyBounds bounds one ticket re-verification. Every field is a
// caller-supplied deployment/test parameter: no default is invented and
// unbounded is not representable.
type TicketVerifyBounds struct {
	// MaxPGRequests bounds this invocation's own PostgreSQL requests. The
	// charges also consume the task's configured total budget when a parent
	// budget is supplied.
	MaxPGRequests int
	// MaxItemAttempts is the bounded cumulative attempt cap of the ticket,
	// counting the first attempt (same accounting as the sweep).
	MaxItemAttempts int
	// MaxItemDuration optionally bounds the evidence re-read; zero means the
	// parent context alone bounds it. A per-item timeout is recorded as a
	// failed attempt, never as an abort.
	MaxItemDuration time.Duration
}

// Validate fails closed on missing or non-positive bounds.
func (b TicketVerifyBounds) Validate() error {
	switch {
	case b.MaxPGRequests <= 0:
		return contractErrorf("ticket verify max PG requests must be positive")
	case b.MaxItemAttempts <= 0:
		return contractErrorf("ticket verify max item attempts must be positive")
	case b.MaxItemDuration < 0:
		return contractErrorf("ticket verify item duration must not be negative")
	}
	return nil
}

// TicketVerifyRequest is one bounded production re-verification of one
// pending_verify ticket under one running task.
type TicketVerifyRequest struct {
	// TaskID is the running task whose scope owns the authorization, the
	// budget and the gap rows.
	TaskID string
	// DiscrepancyID is the pending_verify ticket to re-verify.
	DiscrepancyID string
	// Actor is the authenticated caller recorded on every row.
	Actor string
	// Reason is an optional bounded audit annotation.
	Reason string
	// Now is the re-read instant; zero means time.Now().
	Now time.Time
	// Limits are the task-total budget bounds (required positive).
	Limits BudgetLimits
	// FreshnessTolerance is the classification freshness window (required
	// positive; zero keeps every conclusion pending).
	FreshnessTolerance time.Duration
	// Bounds are this invocation's slice bounds (required).
	Bounds TicketVerifyBounds
	// Sources are the three production read-only adapters. A nil adapter
	// refuses the invocation: a full comparison cannot be claimed without its
	// parties.
	Sources ScanSources
	// WindowResolver resolves time-scoped intervals to height windows (T036).
	WindowResolver ScanTimeWindowResolver
	// HeightWindowResolver resolves height-scoped intervals to the chain-time
	// window the event adapter needs to attribute blockless business-object
	// rows (T036).
	HeightWindowResolver ScanHeightTimeWindowResolver
	// EventConsumers/EventQuarantine are the registered consumers whose
	// delivery closure the event bundle must prove.
	EventConsumers  []EventConsumerRegistration
	EventQuarantine EventQuarantineReader
}

// TicketVerifyResult is the observable outcome of one ticket re-verification.
// Only Consistent never claims completeness by itself: it is the same
// conclusion vocabulary the scan uses, recorded as an append-only reverify
// row, and the close guard independently re-reads it from the database.
type TicketVerifyResult struct {
	TaskID        string
	DiscrepancyID string
	// ObservedState is the lifecycle state found before the re-read.
	ObservedState DiscrepancyState
	// State is the lifecycle state after the invocation (unchanged by a
	// written verdict).
	State       DiscrepancyState
	Verdict     ReverifyVerdict
	EvidenceRef string
	FreshnessAt time.Time
	Range       ScanInterval
	// Consistent is true only when the three-way comparison conclusively
	// agreed on complete, fresh evidence and every recorded-conclusion guard
	// passed.
	Consistent bool
	// Pending is true for an unknown/stale verdict: the ticket stays under
	// observation and cannot close.
	Pending bool
	// Failed is true for any non-conclusive re-read (pending or divergent
	// evidence re-read failure); bounded retry accounting uses it.
	Failed bool
	// GapWritten reports a new visible coverage gap row.
	GapWritten bool
	// Discarded reports that the ticket left pending_verify while the evidence
	// read was in flight: the finding was discarded and audited, nothing was
	// written.
	Discarded     bool
	DiscardReason string
	// RetryExhausted reports that the cumulative attempt cap was reached: no
	// new attempt was taken.
	RetryExhausted bool
	Detail         string
	// BudgetUsage is the task-total budget consumption; SlicePGUsed is this
	// invocation's own charged PostgreSQL requests.
	BudgetUsage BudgetUsage
	SlicePGUsed int
}

// pendingTicket is the materialized pending_verify item plus its recorded
// detection metadata.
type pendingTicket struct {
	DiscrepancyID string
	State         DiscrepancyState
	Category      Category
	BusinessKey   BusinessKey
	Scope         *IdentityScope
	Version       VersionDomain
	Detection     *DetectionInterval
	TxMembers     []TxAggregateMember
	BusinessType  BusinessType
	EvidenceAt    time.Time
	FailCount     int64
}

// reverifyItem projects the pending ticket onto the shared persistence carrier.
func (t pendingTicket) reverifyItem() reverifyItem {
	return reverifyItem{
		DiscrepancyID: t.DiscrepancyID,
		Version:       t.Version,
		EvidenceAt:    t.EvidenceAt,
		FailCount:     t.FailCount,
	}
}

// readPendingDiscrepancySQL loads one ticket plus its bounded prior failed
// re-validation attempt count ($2 bounds the counted history so the cap check
// is O(bound)).
const readPendingDiscrepancySQL = `
SELECT d.discrepancy_id::text, d.state, d.category, d.business_key, d.evidence_version_domain,
       COALESCE((
           SELECT count(*) FROM (
               SELECT 1 FROM recon_audit a
               WHERE a.action = 'reverify'
                 AND a.result IN ('query_failed', 'timeout', 'unknown', 'stale')
                 AND a.target->>'discrepancy_id' = d.discrepancy_id::text
               LIMIT $2
           ) bounded), 0)::bigint
FROM discrepancy d
WHERE d.discrepancy_id = $1`

// identityScopeContains reports whether inner is contained in outer (same
// chain and kind, inner range inside the outer range, inner business types a
// subset of the outer set). It is the ticket-in-task containment rule of the
// production re-verification.
func identityScopeContains(outer, inner IdentityScope) bool {
	if outer.ChainID != inner.ChainID || outer.Kind != inner.Kind {
		return false
	}
	if inner.From < outer.From || inner.To > outer.To {
		return false
	}
	outerTypes, err := canonicalBusinessTypes(outer.BusinessTypes)
	if err != nil {
		return false
	}
	innerTypes, err := canonicalBusinessTypes(inner.BusinessTypes)
	if err != nil {
		return false
	}
	allowed := make(map[BusinessType]struct{}, len(outerTypes))
	for _, businessType := range outerTypes {
		allowed[businessType] = struct{}{}
	}
	for _, businessType := range innerTypes {
		if _, ok := allowed[businessType]; !ok {
			return false
		}
	}
	return true
}

// scanIntervalContains reports whether inner is contained in outer (same kind,
// inclusive bounds).
func scanIntervalContains(outer, inner ScanInterval) bool {
	if outer.From.Kind != inner.From.Kind {
		return false
	}
	fromCmp, err := outer.From.Compare(inner.From)
	if err != nil {
		return false
	}
	toCmp, err := outer.To.Compare(inner.To)
	if err != nil {
		return false
	}
	return fromCmp <= 0 && toCmp >= 0
}

// ticketVerifyBusinessType derives the PG read dimension of a ticket. The
// persisted detection business type wins; a legacy row without it is derived
// only from an unambiguous shape (request/intent -> withdrawal; a single-type
// recorded scope; an event identity -> event-delivery). A bare tx_hash under a
// mixed-type scope stays unresolved and is never guessed.
func ticketVerifyBusinessType(domain PersistedEvidenceDomain, key BusinessKey) (BusinessType, bool) {
	if domain.BusinessType != "" {
		if !domain.BusinessType.Known() {
			return "", false
		}
		return domain.BusinessType, true
	}
	switch key.Kind {
	case BusinessKeyRequestID, BusinessKeyIntentID, BusinessKeyAttemptID:
		return BusinessWithdrawal, true
	case BusinessKeyEventID:
		return BusinessEventDelivery, true
	case BusinessKeyTxHash:
		if domain.Scope == nil {
			return "", false
		}
		types, err := canonicalBusinessTypes(domain.Scope.BusinessTypes)
		if err != nil || len(types) != 1 {
			return "", false
		}
		return types[0], true
	}
	return "", false
}

// ticketReverifyInterval resolves the evidence window to re-read: the recorded
// detection interval when present, otherwise the recorded block anchor as a
// one-block window (height scopes only). ok=false means the recorded evidence
// cannot pin a window without guessing; the caller records unknown/gap.
func ticketReverifyInterval(version VersionDomain, detection *DetectionInterval) (ScanInterval, string, bool) {
	if detection != nil {
		interval, err := detection.ScanInterval()
		if err == nil {
			return interval, "detection", true
		}
	}
	if version.BlockNumber > 0 && version.BlockNumber <= uint64(1)<<62 {
		block := int64(version.BlockNumber)
		return ScanInterval{From: HeightBound(block), To: HeightBound(block)}, "block", true
	}
	return ScanInterval{}, "", false
}

// Load the ticket row and materialize it. found=false means no such ticket.
func (s *Store) loadPendingDiscrepancy(ctx context.Context, id string, maxAttempts int) (pendingTicket, bool, error) {
	if _, err := uuid.Parse(strings.TrimSpace(id)); err != nil {
		return pendingTicket{}, false, contractErrorf("ticket verify id %q is not a UUID", id)
	}
	var (
		rawState, rawCategory, rawBusinessKey string
		domainBytes                           []byte
		item                                  pendingTicket
	)
	err := s.db.QueryRow(ctx, readPendingDiscrepancySQL, strings.TrimSpace(id), maxAttempts).Scan(
		&item.DiscrepancyID, &rawState, &rawCategory, &rawBusinessKey, &domainBytes, &item.FailCount)
	if errors.Is(err, pgx.ErrNoRows) {
		return pendingTicket{}, false, nil
	}
	if err != nil {
		return pendingTicket{}, false, fmt.Errorf("read pending discrepancy: %w", err)
	}
	item.State = DiscrepancyState(rawState)
	if !item.State.Valid() {
		return pendingTicket{}, false, contractErrorf("discrepancy %s has unknown state %q", item.DiscrepancyID, rawState)
	}
	item.Category = Category(rawCategory)
	if !item.Category.Known() {
		return pendingTicket{}, false, contractErrorf("discrepancy %s has unknown category %q", item.DiscrepancyID, rawCategory)
	}
	kind, value, ok := strings.Cut(rawBusinessKey, "=")
	if !ok || strings.TrimSpace(value) == "" {
		return pendingTicket{}, false, contractErrorf("discrepancy %s has a malformed business key", item.DiscrepancyID)
	}
	item.BusinessKey = BusinessKey{Kind: BusinessKeyKind(kind), Value: value}
	if err := item.BusinessKey.Validate(); err != nil {
		return pendingTicket{}, false, err
	}
	domain, err := ParsePersistedEvidenceDomain(domainBytes)
	if err != nil {
		return pendingTicket{}, false, err
	}
	item.Version = domain.VersionDomain
	item.Detection = domain.Detection
	item.TxMembers = domain.TxMembers
	if domain.Scope != nil {
		scope := *domain.Scope
		if err := scope.Validate(); err != nil {
			return pendingTicket{}, false, err
		}
		item.Scope = &scope
	}
	item.BusinessType, _ = ticketVerifyBusinessType(domain, item.BusinessKey)
	item.EvidenceAt = domain.EvidenceAt.UTC()
	return item, true, nil
}

// auditTicketVerifyRefusal appends one refusal/discard audit row in its own
// short transaction. A failed audit write never turns the refusal into a pass.
func (s *Store) auditTicketVerifyRefusal(ctx context.Context, actor, discrepancyID, taskID, result, reason string) error {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin ticket verify refusal audit: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	target := map[string]any{
		"discrepancy_id": discrepancyID,
		"task_id":        taskID,
		"action":         "reverify_ticket",
	}
	if err := insertAuditTx(ctx, tx, AuditRecord{
		Actor:  actor,
		Action: AuditActionRefuse,
		Target: target,
		Reason: reason,
		Result: result,
	}); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit ticket verify refusal audit: %w", err)
	}
	return nil
}

// VerifyPendingDiscrepancy runs one bounded production re-verification of one
// pending_verify ticket under one running task. See the file header for the
// hard rules; the function never writes a lifecycle transition.
func (s *Store) VerifyPendingDiscrepancy(ctx context.Context, req TicketVerifyRequest) (result TicketVerifyResult, err error) {
	result = TicketVerifyResult{
		TaskID:        strings.TrimSpace(req.TaskID),
		DiscrepancyID: strings.TrimSpace(req.DiscrepancyID),
	}
	if s == nil || s.db == nil {
		return result, contractErrorf("store has no database")
	}
	if result.TaskID == "" || result.DiscrepancyID == "" {
		return result, contractErrorf("ticket verify requires a task_id and a discrepancy_id")
	}
	if strings.TrimSpace(req.Actor) == "" {
		return result, contractErrorf("ticket verify requires an actor")
	}
	if len(req.Reason) > 1024 || strings.ContainsRune(req.Reason, 0) {
		return result, contractErrorf("ticket verify reason is malformed")
	}
	if err := req.Limits.Validate(); err != nil {
		return result, err
	}
	if err := req.Bounds.Validate(); err != nil {
		return result, err
	}
	if req.Sources.Chain == nil || req.Sources.PG == nil || req.Sources.Events == nil {
		return result, contractErrorf("ticket verify requires the chain, PG and event adapters (a full comparison has no optional party)")
	}
	if len(req.EventConsumers) > 0 && req.EventQuarantine == nil {
		return result, contractErrorf("ticket verify requires a quarantine reader when consumers are registered")
	}
	now := req.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	actor := strings.TrimSpace(req.Actor)

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

	item, found, err := s.loadPendingDiscrepancy(ctx, result.DiscrepancyID, req.Bounds.MaxItemAttempts)
	if err != nil {
		return result, err
	}
	if !found {
		return result, fmt.Errorf("%w: discrepancy_id %s", ErrDiscrepancyNotFound, result.DiscrepancyID)
	}
	result.ObservedState = item.State
	result.State = item.State
	if item.State != DiscrepancyStatePendingVerify {
		auditErr := s.auditTicketVerifyRefusal(ctx, actor, item.DiscrepancyID, result.TaskID, "refused",
			fmt.Sprintf("ticket verify requires the pending_verify state; observed %s", item.State))
		if auditErr != nil {
			return result, auditErr
		}
		return result, fmt.Errorf("%w: observed=%s", ErrTicketNotPendingVerify, item.State)
	}
	if item.Scope == nil {
		if err := s.auditTicketVerifyRefusal(ctx, actor, item.DiscrepancyID, result.TaskID, "refused",
			"ticket has no recorded scope marker; it cannot be verified under a task scope"); err != nil {
			return result, err
		}
		return result, fmt.Errorf("%w: discrepancy %s has no recorded scope marker", ErrTicketOutsideTaskScope, item.DiscrepancyID)
	}
	if !identityScopeContains(scope, *item.Scope) {
		if err := s.auditTicketVerifyRefusal(ctx, actor, item.DiscrepancyID, result.TaskID, "refused",
			"ticket scope is not contained in the task scope; refusing an out-of-scope verification"); err != nil {
			return result, err
		}
		return result, fmt.Errorf("%w: discrepancy %s", ErrTicketOutsideTaskScope, item.DiscrepancyID)
	}
	if item.FailCount >= int64(req.Bounds.MaxItemAttempts) {
		result.RetryExhausted = true
		recorded, err := s.reverifyRetryExhaustedRecorded(ctx, item.DiscrepancyID)
		if err != nil {
			return result, err
		}
		if !recorded {
			if err := s.recordReverifyRetryExhausted(ctx, item.reverifyItem(), actor, now,
				req.Bounds.MaxItemAttempts, ReverifySourceTicket, ReverifySourceTicket); err != nil {
				return result, err
			}
		}
		return result, fmt.Errorf("%w: discrepancy %s has %d bounded attempts (cap %d)",
			ErrTicketVerifyExhausted, item.DiscrepancyID, item.FailCount, req.Bounds.MaxItemAttempts)
	}

	parentBudget, err := NewBudget(req.Limits)
	if err != nil {
		return result, err
	}
	slice := newReverifySliceBudget(parentBudget, req.Bounds.MaxPGRequests)
	defer func() {
		result.BudgetUsage = parentBudget.Usage()
		result.SlicePGUsed = slice.usedPG
	}()

	finding, interval, failureToken, evaluated, err := s.comparePendingTicket(ctx, &req, task, scope, item, slice, now)
	if err != nil {
		return result, err
	}
	if evaluated {
		finding, err = normalizeReverifyFinding(finding)
		if err != nil {
			return result, err
		}
	}
	result.Range = interval
	result.Verdict = finding.Verdict
	result.EvidenceRef = finding.EvidenceRef
	result.FreshnessAt = finding.FreshnessAt
	result.Detail = finding.Detail

	// Persist the verdict under the discrepancy row lock with a state CAS: a
	// ticket that left pending_verify while the evidence read was in flight is
	// discarded and audited, never written as fresh evidence.
	if err := slice.ConsumePG(ctx, 1); err != nil {
		return result, err
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return result, fmt.Errorf("begin ticket verify persist: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	var (
		lockedState  DiscrepancyState
		lockedReopen int64
	)
	if err := tx.QueryRow(ctx, lockDiscrepancySQL, item.DiscrepancyID).Scan(&lockedState, &lockedReopen); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return result, fmt.Errorf("%w: discrepancy_id %s", ErrDiscrepancyNotFound, item.DiscrepancyID)
		}
		return result, fmt.Errorf("lock discrepancy for ticket verify: %w", err)
	}
	if lockedState != DiscrepancyStatePendingVerify {
		result.Discarded = true
		result.DiscardReason = fmt.Sprintf("state changed during the evidence read: %s -> %s", item.State, lockedState)
		result.State = lockedState
		if err := insertAuditTx(ctx, tx, AuditRecord{
			Actor:  actor,
			Action: AuditActionReverify,
			Target: map[string]any{
				"discrepancy_id": item.DiscrepancyID,
				"task_id":        result.TaskID,
				"action":         "reverify_ticket",
			},
			Reason: "ticket left pending_verify during the evidence read; outcome discarded",
			Result: "discarded",
		}); err != nil {
			return result, err
		}
		if err := tx.Commit(ctx); err != nil {
			return result, fmt.Errorf("commit ticket verify discard: %w", err)
		}
		return result, nil
	}

	gapWritten, _, err := persistReverifyOutcomeTx(ctx, tx, result.TaskID, actor, ReverifySourceTicket,
		item.reverifyItem(), item.Scope, finding, ReverifySourceTicket, failureToken, req.Reason)
	if err != nil {
		return result, err
	}
	result.GapWritten = gapWritten
	if err := tx.Commit(ctx); err != nil {
		return result, fmt.Errorf("commit ticket verify outcome: %w", err)
	}

	switch finding.Verdict {
	case ReverifyConsistent:
		result.Consistent = true
	case ReverifyDivergent:
	default:
		result.Pending = true
	}
	if finding.Verdict != ReverifyConsistent && finding.Verdict != ReverifyDivergent {
		result.Failed = true
	}
	if failureToken != "" {
		result.Failed = true
	}
	return result, nil
}

// comparePendingTicket runs the read-only full comparison of one ticket. A
// returned error is a context or wiring error; an unusable recorded identity
// (missing scope marker, unreadable business type, unresolvable interval) is
// returned as a fail-closed unknown finding with evaluated=false, so the
// caller persists it visibly instead of aborting the invocation.
func (s *Store) comparePendingTicket(ctx context.Context, req *TicketVerifyRequest, task *Task,
	scope IdentityScope, item pendingTicket, slice *reverifySliceBudget, now time.Time) (ReverifyFinding, ScanInterval, string, bool, error) {
	itemCtx := ctx
	if req.Bounds.MaxItemDuration > 0 {
		var cancel context.CancelFunc
		itemCtx, cancel = context.WithTimeout(ctx, req.Bounds.MaxItemDuration)
		defer cancel()
	}

	unknownDetail := func(detail string) (ReverifyFinding, ScanInterval, string, bool, error) {
		return ReverifyFinding{Verdict: ReverifyUnknown, Detail: detail}, ScanInterval{}, "", false, nil
	}
	if item.BusinessType == "" || !item.BusinessType.Known() {
		return unknownDetail("recorded evidence does not determine the ticket's business type; the PG dimension cannot be read without guessing")
	}
	if item.BusinessType == BusinessEventDelivery {
		// An event-delivery identity has no PG business dimension: the event
		// adapter alone cannot prove a three-way agreement, and the entry
		// never upgrades a single-party read (Q5-4).
		return unknownDetail("event-delivery identities carry no PG business dimension; a single-party re-read cannot prove three-way consistency")
	}
	interval, intervalSource, ok := ticketReverifyInterval(item.Version, item.Detection)
	if !ok {
		return unknownDetail("recorded evidence carries neither a detection interval nor a block anchor; the chain/event window cannot be re-read without guessing")
	}
	if !scanIntervalContains(ScanInterval{From: taskScopeBound(task, true), To: taskScopeBound(task, false)}, interval) {
		return unknownDetail("recorded evidence window is outside the task scope")
	}

	// The candidate is reconstructed from the recorded identity only: no
	// member join and no event join is invented (a bare tx_hash keeps the
	// zero event key; the R2 discriminator owns that dimension).
	candidate := ScanCandidate{
		BusinessType: item.BusinessType,
		BusinessKey:  item.BusinessKey,
		EventKey:     EventAggregateBusinessKey(item.BusinessKey),
		ChainFact: ChainFactRef{
			BlockNumber: recordedBlockNumber(item.Version),
			BlockHash:   item.Version.BlockHash,
		},
		EvidenceRef: ticketVerifyEvidenceRef(item, "candidate"),
	}
	if item.BusinessKey.Kind == BusinessKeyTxHash {
		candidate.ChainFact.TxHash = item.BusinessKey.Value
	}
	if err := scanValidateCandidate(&candidate, scope); err != nil {
		return ReverifyFinding{}, ScanInterval{}, "", false, err
	}

	// A scratch ScanOnceRequest carries the read-only wiring through the
	// exact T036 window-resolution build the scan path uses (the scratch is
	// never claimed and never delegated to the scan loop).
	scratch := &ScanOnceRequest{
		TaskID:               req.TaskID,
		Owner:                req.Actor,
		Limits:               req.Limits,
		FreshnessTolerance:   req.FreshnessTolerance,
		Sources:              req.Sources,
		WindowResolver:       req.WindowResolver,
		HeightWindowResolver: req.HeightWindowResolver,
	}

	chainID, chainIDOK := scanNumericChainID(task)
	window, err := scanChainWindow(itemCtx, scratch, task, interval, parentBudgetOf(slice))
	if err != nil {
		return ReverifyFinding{}, interval, reverifyFailureToken(err, itemCtx), false, err
	}
	eventWindow, err := scanEventTimeWindow(itemCtx, scratch, task, interval, parentBudgetOf(slice))
	if err != nil {
		return ReverifyFinding{}, interval, reverifyFailureToken(err, itemCtx), false, err
	}

	// Chain facts (one interval-level read, charged like the scan).
	var (
		chainBundle *ChainFactsBundle
		chainUsable bool
	)
	if chainIDOK && window.resolved {
		if err := slice.ConsumePG(itemCtx, 1); err != nil {
			return ReverifyFinding{}, interval, "", false, err
		}
		if err := slice.ConsumeRPC(itemCtx, 1); err != nil {
			return ReverifyFinding{}, interval, "", false, err
		}
		bundle, observeErr := req.Sources.Chain.Observe(itemCtx, ChainFactsQuery{
			ChainID:           chainID,
			From:              window.from,
			To:                window.to,
			ConfirmThresholdN: scanTaskConfirmThresholdN(task),
			NeedTransferLogs:  true,
			UpstreamReceipts:  append([]ChainUpstreamReceiptSource(nil), task.UpstreamReceipts...),
		})
		if ctxErr := scanContextError(observeErr); ctxErr != nil {
			return ReverifyFinding{}, interval, reverifyFailureToken(ctxErr, itemCtx), false, ctxErr
		}
		chainBundle = &bundle
		chainUsable = observeErr == nil && bundle.CoverageClosed()
	}

	// Event delivery evidence (one interval-level read).
	var (
		eventEvidence *EventStateEvidence
		eventUsable   bool
	)
	if err := slice.ConsumePG(itemCtx, 1); err != nil {
		return ReverifyFinding{}, interval, "", false, err
	}
	evidence, observeErr := req.Sources.Events.Observe(itemCtx, EventStateQuery{
		Scope:            scope,
		Interval:         EventStateInterval{From: interval.From, To: interval.To},
		OccurredFrom:     eventWindow.from,
		OccurredTo:       eventWindow.to,
		Consumers:        append([]EventConsumerRegistration(nil), req.EventConsumers...),
		Quarantine:       req.EventQuarantine,
		UpstreamReceipts: append([]ChainUpstreamReceiptSource(nil), task.UpstreamReceipts...),
	})
	if ctxErr := scanContextError(observeErr); ctxErr != nil {
		return ReverifyFinding{}, interval, reverifyFailureToken(ctxErr, itemCtx), false, ctxErr
	}
	eventEvidence = &evidence
	eventUsable = observeErr == nil && evidence.CoverageClosed()

	// T040 expectation carrier for the reconstructed candidate's aggregate.
	var obligationEvidence EventObligationEvidence
	if aggregate, bound := EventObligationAggregateOf(candidate.EventKey); bound && req.Sources.Obligations != nil {
		if err := slice.ConsumePG(itemCtx, 2); err != nil {
			return ReverifyFinding{}, interval, "", false, err
		}
		obligations, readErr := req.Sources.Obligations.ReadObligations(itemCtx,
			EventObligationQuery{Aggregates: []EventObligationAggregate{aggregate}})
		if ctxErr := scanContextError(readErr); ctxErr != nil {
			return ReverifyFinding{}, interval, reverifyFailureToken(ctxErr, itemCtx), false, ctxErr
		}
		if readErr != nil {
			// The carrier could not be read: an event-only absence stays R3
			// (unproven), never missing and never consistent. A dedicated
			// read failure is not a comparison outcome, so it stays an
			// unknown finding with the read failure recorded.
			obligationEvidence = EventObligationEvidence{}
		} else {
			obligationEvidence = obligations
		}
	}

	compare := &scanCandidateCompare{
		sources: req.Sources,
		upstreamFor: func(businessType BusinessType) UpstreamReceiptSource {
			return scanUpstreamReceiptSource(task, businessType)
		},
		freshness:     req.FreshnessTolerance,
		scope:         scope,
		chainID:       chainID,
		chainIDOK:     chainIDOK,
		now:           now,
		chainBundle:   chainBundle,
		chainUsable:   chainUsable,
		eventEvidence: eventEvidence,
		eventUsable:   eventUsable,
		obligation:    obligationEvidence,
	}
	classification, pgRecord, evalErr := compareScanCandidate(itemCtx, compare, candidate, parentBudgetOf(slice))
	if evalErr != nil {
		if ctxErr := scanContextError(evalErr); ctxErr != nil {
			return ReverifyFinding{}, interval, reverifyFailureToken(ctxErr, itemCtx), false, ctxErr
		}
		return ReverifyFinding{Verdict: ReverifyUnknown,
			EvidenceRef: ticketVerifyEvidenceRef(item, "read_failed"),
			Detail:      "evidence re-read failed: " + boundedReverifyDetail(evalErr.Error())}, interval, reverifyFailureToken(evalErr, itemCtx), true, nil
	}
	return ticketVerifyOutcome(item, classification, pgRecord, interval, intervalSource), interval, "", true, nil
}

// parentBudgetOf unwraps the parent budget of a slice (the slice was built
// with a parent in every production path).
func parentBudgetOf(slice *reverifySliceBudget) *Budget {
	if slice == nil {
		return nil
	}
	return slice.parent
}

// taskScopeBound returns one inclusive bound of the task scope.
func taskScopeBound(task *Task, from bool) RangeBound {
	if from {
		return task.ScopeStart
	}
	return task.ScopeEnd
}

// recordedBlockNumber safely narrows the recorded block number.
func recordedBlockNumber(version VersionDomain) int64 {
	if version.BlockNumber == 0 || version.BlockNumber > uint64(1)<<62 {
		return 0
	}
	return int64(version.BlockNumber)
}

// ticketVerifyEvidenceRef builds the bounded evidence reference of one ticket
// re-verification stage.
func ticketVerifyEvidenceRef(item pendingTicket, stage string) string {
	return boundedReverifyDetail(fmt.Sprintf(
		"ticketverify/v1 stage=%s discrepancy=%s business_key=%s/%s",
		stage, item.DiscrepancyID, item.BusinessKey.Kind, item.BusinessKey.Value))
}

// ticketVerifyOutcome maps one full-comparison classification onto the
// fail-closed reverify verdict vocabulary. Only a consistent classification
// whose recorded-conclusion guards also pass may become `consistent`:
//
//   - a remaining divergence stays divergent (new evidence);
//   - a pending/unknown coverage or a stale/freshness-blocked observation
//     stays stale/unknown;
//   - a tx-aggregate ticket additionally proves member completeness against
//     the recorded member set and the stored per-member business records: a
//     partially recorded aggregate is a remaining divergence, never a fix;
//   - a recorded recovery-version rotation or a changed block identity is a
//     conclusion-affecting change and stays divergent/unknown.
func ticketVerifyOutcome(item pendingTicket, classification Classification, pg *PGStateRecord,
	interval ScanInterval, intervalSource string) ReverifyFinding {
	scopeText := ticketVerifyScopeText(interval)
	finding := ReverifyFinding{
		Verdict:     ReverifyUnknown,
		EvidenceRef: ticketVerifyEvidenceRef(item, "compare"),
		Detail:      "classification is not conclusive",
		Scope:       scopeText,
	}
	if classification.Version.Validate() == nil {
		version := classification.Version
		finding.Version = &version
	}
	finding.FreshnessAt = classification.EvidenceAt

	switch {
	case classification.Ticket:
		finding.Verdict = ReverifyDivergent
		finding.Trigger = InvalidationNewEvidence
		finding.Detail = "three-way comparison still diverges: " + string(classification.Reason)
		return finding
	case classification.Conclusion != ConclusionConsistent:
		switch classification.Reason {
		case ReasonFreshnessExpired, ReasonFreshnessUnproven, ReasonEvidenceTrimmed:
			finding.Verdict = ReverifyStale
		default:
			finding.Verdict = ReverifyUnknown
		}
		finding.Detail = "three-way comparison is not conclusive: " + string(classification.Reason)
		return finding
	}

	// The comparison agrees; apply the recorded-conclusion guards.
	if aggregate := ticketAggregateGate(item, classification, pg); !aggregate.ok {
		finding.Verdict = aggregate.verdict
		finding.Trigger = aggregate.trigger
		finding.Detail = aggregate.detail
		return finding
	}
	if item.Version.BlockNumber > 0 &&
		(classification.Version.BlockNumber != item.Version.BlockNumber ||
			!strings.EqualFold(classification.Version.BlockHash, item.Version.BlockHash)) {
		finding.Verdict = ReverifyDivergent
		finding.Trigger = InvalidationReorg
		finding.Detail = "recorded chain block identity changed"
		return finding
	}
	if recorded := strings.TrimSpace(item.Version.RecoveryVersion); recorded != "" {
		observed := strings.TrimSpace(classification.Version.RecoveryVersion)
		if observed == "" {
			finding.Verdict = ReverifyUnknown
			finding.Detail = "recorded recovery version is no longer observable; the recorded conclusion cannot be re-proven"
			return finding
		}
		if observed != recorded {
			finding.Verdict = ReverifyDivergent
			finding.Trigger = InvalidationNewEvidence
			finding.Detail = "recorded recovery version rotated"
			return finding
		}
	}

	finding.Verdict = ReverifyConsistent
	finding.Detail = "three-way comparison agrees on complete, fresh evidence"
	finding.EvidenceRef = boundedReverifyDetail(fmt.Sprintf(
		"ticketverify/v1 discrepancy=%s verdict=consistent reason=%s interval=%s source=%s event=%s",
		item.DiscrepancyID, classification.Reason, scopeText, intervalSource, eventPartyLabel(classification)))
	if finding.FreshnessAt.IsZero() {
		finding.FreshnessAt = time.Now().UTC()
	}
	return finding
}

// eventPartyLabel renders the event dimension of a consistent comparison (the
// only two shapes a consistent classification can carry).
func eventPartyLabel(classification Classification) string {
	if classification.Reason == ReasonEventNotApplicable {
		return string(PartyNotApplicable)
	}
	return string(PartyPresent)
}

// ticketAggregateGate proves member completeness of a tx-aggregate ticket.
// For a deposit aggregate every recorded member log must have its stored
// 004 credit row (a partially recorded aggregate is a remaining divergence);
// for every aggregate the re-read chain evidence must still carry every
// recorded member log (a missing member is a chain change, not a fix). A
// legacy tx_hash ticket without a recorded member set cannot prove deposit
// completeness and stays unknown; a withdrawal aggregate is tx-level and does
// not need the member set.
type ticketAggregateGateResult struct {
	ok      bool
	verdict ReverifyVerdict
	trigger InvalidationTrigger
	detail  string
}

func ticketAggregateGate(item pendingTicket, classification Classification, pg *PGStateRecord) ticketAggregateGateResult {
	if item.BusinessKey.Kind != BusinessKeyTxHash || item.Category != CategoryMissing {
		return ticketAggregateGateResult{ok: true}
	}
	if len(item.TxMembers) == 0 {
		if item.BusinessType == BusinessDeposit {
			return ticketAggregateGateResult{
				verdict: ReverifyUnknown,
				detail:  "recorded aggregate member set is missing; deposit member completeness cannot be proven",
			}
		}
		return ticketAggregateGateResult{ok: true}
	}

	// The re-read chain evidence must still carry every recorded member log.
	observed := make(map[string]struct{}, len(classification.Members))
	for _, member := range classification.Members {
		observed[txMemberKey(member)] = struct{}{}
	}
	for _, member := range item.TxMembers {
		if _, ok := observed[txMemberKey(member)]; !ok {
			return ticketAggregateGateResult{
				verdict: ReverifyDivergent,
				trigger: InvalidationReorg,
				detail: fmt.Sprintf("recorded member log tx=%s l=%d is no longer present in the re-read chain evidence",
					member.TxHash, member.LogIndex),
			}
		}
	}
	if item.BusinessType != BusinessDeposit {
		return ticketAggregateGateResult{ok: true}
	}
	if pg == nil || pg.Status != PGStateComplete {
		return ticketAggregateGateResult{
			verdict: ReverifyUnknown,
			detail:  "deposit business records could not be read completely; member completeness is unproven",
		}
	}
	stored := make(map[string]struct{}, len(pg.Deposits))
	for _, deposit := range pg.Deposits {
		stored[fmt.Sprintf("%s/%d", strings.ToLower(strings.TrimSpace(deposit.TxHash)), deposit.LogIndex)] = struct{}{}
	}
	missing := 0
	for _, member := range item.TxMembers {
		if _, ok := stored[fmt.Sprintf("%s/%d", strings.ToLower(strings.TrimSpace(member.TxHash)), member.LogIndex)]; !ok {
			missing++
		}
	}
	if missing > 0 {
		return ticketAggregateGateResult{
			verdict: ReverifyDivergent,
			trigger: InvalidationNewEvidence,
			detail: fmt.Sprintf("tx aggregate is partially recorded: %d of %d recorded member logs have no stored deposit row",
				missing, len(item.TxMembers)),
		}
	}
	return ticketAggregateGateResult{ok: true}
}

// txMemberKey is the on-chain member identity used by the completeness gate.
func txMemberKey(member TxAggregateMember) string {
	return fmt.Sprintf("%s/%s/%d", strings.ToLower(strings.TrimSpace(member.BlockHash)),
		strings.ToLower(strings.TrimSpace(member.TxHash)), member.LogIndex)
}

// ticketVerifyScopeText renders the re-read window for the audit trail.
func ticketVerifyScopeText(interval ScanInterval) string {
	switch interval.From.Kind {
	case ScopeHeight:
		return fmt.Sprintf("height:%d..%d", interval.From.Height, interval.To.Height)
	case ScopeTime:
		return fmt.Sprintf("time:%s..%s", interval.From.Time.UTC().Format(time.RFC3339Nano),
			interval.To.Time.UTC().Format(time.RFC3339Nano))
	}
	return "unknown"
}
