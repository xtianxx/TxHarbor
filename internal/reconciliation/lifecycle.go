// lifecycle.go implements the T007 discrepancy lifecycle skeleton
// (contracts/discrepancy-lifecycle.md, data-model.md §1.4–1.8/§3, Q5):
//
//   - the five states plus reopen: open_claimable -> claimed -> disposing ->
//     pending_verify -> closed, with closed -> pending_verify (invalidation)
//     and -> reopened (confirmed recurrence) / reopened -> pending_verify;
//   - disposed ≠ reverified ≠ closed: dispose -> pending_verify needs a
//     recorded effective disposition, and close needs a fresh consistent
//     reverify row plus a close_basis snapshot;
//   - Q5 invalidation/reopen rules as transition guards: only changes that
//     affect the conclusion invalidate a closed item (unrelated writes do
//     not), invalidation only triggers the approved reverify flow (never auto
//     disposal), and stale/incomplete/unknown evidence can never close.
//
// T022 adds the production claim/dispose paths on top of these guards (see the
// T022 section at the bottom of this file); the invalidation evaluator (T026)
// builds on the same guards. The file owns the state machine and its
// claim/dispose wiring, and writes only 014-owned rows.
package reconciliation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// DiscrepancyState mirrors discrepancy.state (data-model.md §1.4;
// contracts/discrepancy-lifecycle.md States).
type DiscrepancyState string

const (
	DiscrepancyStateOpenClaimable DiscrepancyState = "open_claimable"
	DiscrepancyStateClaimed       DiscrepancyState = "claimed"
	DiscrepancyStateDisposing     DiscrepancyState = "disposing"
	DiscrepancyStatePendingVerify DiscrepancyState = "pending_verify"
	DiscrepancyStateClosed        DiscrepancyState = "closed"
	DiscrepancyStateReopened      DiscrepancyState = "reopened"
)

// Valid reports whether s is one of the closed discrepancy states.
func (s DiscrepancyState) Valid() bool {
	switch s {
	case DiscrepancyStateOpenClaimable, DiscrepancyStateClaimed, DiscrepancyStateDisposing,
		DiscrepancyStatePendingVerify, DiscrepancyStateClosed, DiscrepancyStateReopened:
		return true
	}
	return false
}

// CanTransitionDiscrepancy reports whether the lifecycle allows from -> to.
// Structural edges only; state-specific evidence guards are applied by
// ValidateDiscrepancyTransition.
func CanTransitionDiscrepancy(from, to DiscrepancyState) bool {
	switch from {
	case DiscrepancyStateOpenClaimable:
		return to == DiscrepancyStateClaimed
	case DiscrepancyStateClaimed:
		return to == DiscrepancyStateDisposing
	case DiscrepancyStateDisposing:
		return to == DiscrepancyStatePendingVerify
	case DiscrepancyStatePendingVerify:
		return to == DiscrepancyStateClosed || to == DiscrepancyStateReopened
	case DiscrepancyStateClosed:
		// Invalidation (reverify only) or confirmed recurrence (reopen).
		return to == DiscrepancyStatePendingVerify || to == DiscrepancyStateReopened
	case DiscrepancyStateReopened:
		// Re-enter the approved reverify flow; closure always requires a new
		// fresh consistent verdict.
		return to == DiscrepancyStatePendingVerify
	}
	return false
}

// DispositionKind mirrors disposition.kind (data-model.md §1.6; FR-013).
type DispositionKind string

const (
	DispositionAckOnly       DispositionKind = "ack_only"
	DispositionReuseRecovery DispositionKind = "reuse_recovery"
	DispositionNewFixRule    DispositionKind = "new_fix_rule"
)

// Valid reports whether k is one of the three disposition kinds.
func (k DispositionKind) Valid() bool {
	switch k {
	case DispositionAckOnly, DispositionReuseRecovery, DispositionNewFixRule:
		return true
	}
	return false
}

// DispositionResult mirrors disposition.result (data-model.md §1.6).
type DispositionResult string

const (
	DispositionDone    DispositionResult = "done"
	DispositionRefused DispositionResult = "refused"
	DispositionFailed  DispositionResult = "failed"
	DispositionDryRun  DispositionResult = "dry_run"
)

// Valid reports whether r is one of the closed disposition results.
func (r DispositionResult) Valid() bool {
	switch r {
	case DispositionDone, DispositionRefused, DispositionFailed, DispositionDryRun:
		return true
	}
	return false
}

// DispositionSnapshot is the recorded disposition evidence a transition guard
// reads back. ActionRef references an existing entry point only (014 never
// auto-executes it); new_fix_rule stays dry_run-only until separately ruled.
type DispositionSnapshot struct {
	Kind      DispositionKind
	Result    DispositionResult
	ActionRef string
}

// ValidateDisposition enforces the disposition vocabulary and the phase rule:
// new_fix_rule is only allowed as dry_run (FR-023, data-model.md §1.6).
func ValidateDisposition(s DispositionSnapshot) error {
	if !s.Kind.Valid() {
		return contractErrorf("unknown disposition kind %q", s.Kind)
	}
	if !s.Result.Valid() {
		return contractErrorf("unknown disposition result %q", s.Result)
	}
	if s.Kind == DispositionNewFixRule && s.Result != DispositionDryRun {
		return contractErrorf("new_fix_rule is dry_run-only in this phase")
	}
	if s.Kind == DispositionReuseRecovery && strings.TrimSpace(s.ActionRef) == "" {
		return contractErrorf("reuse_recovery requires an action_ref to an existing entry point")
	}
	return nil
}

// effective reports whether the disposition completed in a way that may hand
// the item to verification (disposed ≠ reverified; refused/failed stay with
// the operator).
func (s DispositionSnapshot) effective() bool {
	return s.Result == DispositionDone || s.Result == DispositionDryRun
}

// ReverifyVerdict mirrors reverify.verdict (data-model.md §1.7). Only a fresh
// consistent verdict can close; timeout/incomplete/unavailable/insufficient
// evidence must never be recorded as consistent (Q5-4).
type ReverifyVerdict string

const (
	ReverifyConsistent ReverifyVerdict = "consistent"
	ReverifyDivergent  ReverifyVerdict = "divergent"
	ReverifyUnknown    ReverifyVerdict = "unknown"
	ReverifyStale      ReverifyVerdict = "stale"
)

// Valid reports whether v is one of the closed reverify verdicts.
func (v ReverifyVerdict) Valid() bool {
	switch v {
	case ReverifyConsistent, ReverifyDivergent, ReverifyUnknown, ReverifyStale:
		return true
	}
	return false
}

// ReverifyEvidence is the latest reverify row plus its freshness. It is system
// evidence only; it never triggers disposal or payment (Q1/Q5-6).
type ReverifyEvidence struct {
	Verdict     ReverifyVerdict
	EvidenceRef string
	FreshnessAt time.Time
}

// CanClose reports whether this evidence may support a close at now: the
// verdict must be consistent, carry an evidence reference, and be no older
// than tolerance. Missing/stale/future evidence returns false (fail-closed:
// expired results never close).
func (e *ReverifyEvidence) CanClose(now time.Time, tolerance time.Duration) bool {
	if e == nil || e.Verdict != ReverifyConsistent || strings.TrimSpace(e.EvidenceRef) == "" {
		return false
	}
	if tolerance <= 0 || e.FreshnessAt.IsZero() {
		return false
	}
	now = now.UTC()
	if e.FreshnessAt.After(now) {
		return false
	}
	return now.Sub(e.FreshnessAt) <= tolerance
}

// InvalidationTrigger is the closed vocabulary of change signals observed by
// the scan loop (Q5; data-model.md §3). Unrelated writes never invalidate.
type InvalidationTrigger string

const (
	// InvalidationUnrelatedWrite: a write that does not affect the recorded
	// conclusion; explicitly ignored.
	InvalidationUnrelatedWrite InvalidationTrigger = "unrelated_write"
	// InvalidationConcurrentWrite: a concurrent business-state write in the
	// recorded scope/version domain.
	InvalidationConcurrentWrite InvalidationTrigger = "concurrent_write"
	// InvalidationReorg: chain reorganization touching the recorded block
	// identity.
	InvalidationReorg InvalidationTrigger = "reorg"
	// InvalidationNewEvidence: new evidence discovered for the identity.
	InvalidationNewEvidence InvalidationTrigger = "new_evidence"
	// InvalidationSourceRotation: upstream receipt source rotation.
	InvalidationSourceRotation InvalidationTrigger = "source_rotation"
	// InvalidationVersionRotation: business-version rotation.
	InvalidationVersionRotation InvalidationTrigger = "version_rotation"
)

// Valid reports whether t is one of the closed invalidation triggers.
func (t InvalidationTrigger) Valid() bool {
	switch t {
	case InvalidationUnrelatedWrite, InvalidationConcurrentWrite, InvalidationReorg,
		InvalidationNewEvidence, InvalidationSourceRotation, InvalidationVersionRotation:
		return true
	}
	return false
}

// AffectsConclusion reports whether the trigger invalidates a recorded
// conclusion (Q5: only conclusion-affecting changes; unrelated writes do not
// reopen).
func (t InvalidationTrigger) AffectsConclusion() bool {
	return t != InvalidationUnrelatedWrite
}

// InvalidationSignal is one observed change.
type InvalidationSignal struct {
	Trigger     InvalidationTrigger
	EvidenceRef string
}

// TransitionGuard carries the evidence a guarded discrepancy transition needs.
// Zero values fail closed: close without fresh consistent evidence is
// refused, reopen without confirmed recurrence is refused.
type TransitionGuard struct {
	// ClaimOwner is required for -> claimed (the CAS claim owner; migration
	// CHECK discrepancy_claim_states_check requires an owner on claimed and
	// disposing rows).
	ClaimOwner string
	// Disposition is required for disposing -> pending_verify.
	Disposition *DispositionSnapshot
	// LatestReverify is the latest system reverify row, if any.
	LatestReverify *ReverifyEvidence
	// Invalidation is the observed change, required for closed ->
	// pending_verify.
	Invalidation *InvalidationSignal
	// ConfirmedRecurrence is required for any -> reopened edge.
	ConfirmedRecurrence bool
	// CloseBasis is the range/block/version/timestamp snapshot recorded on
	// close; required and must be valid JSON for -> closed.
	CloseBasis []byte
	// Now is the guard evaluation time; zero means time.Now().
	Now time.Time
	// ReverifyTolerance is the maximum evidence age a close may accept. Must
	// be positive for a close.
	ReverifyTolerance time.Duration
}

// ValidateDiscrepancyTransition applies the structural edge rule plus the
// state-specific guards (contracts/discrepancy-lifecycle.md Transitions):
// disposed ≠ reverified ≠ closed, Q5 invalidation/reopen rules, and the
// fail-closed close evidence rule.
func ValidateDiscrepancyTransition(from, to DiscrepancyState, g TransitionGuard) error {
	if !from.Valid() || !to.Valid() {
		return contractErrorf("unknown discrepancy state %q -> %q", from, to)
	}
	if !CanTransitionDiscrepancy(from, to) {
		return fmt.Errorf("%w: %s -> %s", ErrIllegalDiscrepancyTransition, from, to)
	}
	switch to {
	case DiscrepancyStateClaimed:
		if strings.TrimSpace(g.ClaimOwner) == "" {
			return contractErrorf("claim requires an owner")
		}
	case DiscrepancyStatePendingVerify:
		switch from {
		case DiscrepancyStateDisposing:
			if g.Disposition == nil {
				return ErrDispositionRequired
			}
			if err := ValidateDisposition(*g.Disposition); err != nil {
				return err
			}
			if !g.Disposition.effective() {
				return fmt.Errorf("%w: disposition result=%s", ErrDispositionRequired, g.Disposition.Result)
			}
		case DiscrepancyStateClosed:
			if g.Invalidation == nil {
				return fmt.Errorf("%w: missing invalidation signal", ErrInvalidationIgnored)
			}
			if !g.Invalidation.Trigger.Valid() {
				return contractErrorf("unknown invalidation trigger %q", g.Invalidation.Trigger)
			}
			if !g.Invalidation.Trigger.AffectsConclusion() {
				return fmt.Errorf("%w: trigger %s", ErrInvalidationIgnored, g.Invalidation.Trigger)
			}
		}
	case DiscrepancyStateClosed:
		now := g.Now
		if now.IsZero() {
			now = time.Now()
		}
		if !g.LatestReverify.CanClose(now, g.ReverifyTolerance) {
			return ErrReverifyRequired
		}
		if err := validateCloseBasis(g.CloseBasis); err != nil {
			return err
		}
	case DiscrepancyStateReopened:
		if !g.ConfirmedRecurrence {
			return ErrRecurrenceUnconfirmed
		}
	}
	return nil
}

// validateCloseBasis requires a JSON object snapshot: the migration CHECK
// discrepancy_close_basis_shape requires jsonb_typeof(close_basis)='object',
// and a close without a recorded range/block/version/timestamp basis is never
// valid (data-model.md §1.4/§3).
func validateCloseBasis(basis []byte) error {
	if len(basis) == 0 {
		return contractErrorf("close requires a close_basis snapshot")
	}
	var obj map[string]any
	if err := json.Unmarshal(basis, &obj); err != nil || obj == nil {
		return contractErrorf("close_basis must be a JSON object")
	}
	return nil
}

// EvaluateInvalidation applies the Q5 invalidation rule: a conclusion-affecting
// change moves a closed item to pending_verify (triggering only the approved
// reverify flow); unrelated writes and non-closed states are unchanged. An
// unknown trigger is a contract error, never silently ignored.
func EvaluateInvalidation(from DiscrepancyState, signal *InvalidationSignal) (DiscrepancyState, bool, error) {
	if signal == nil {
		return from, false, nil
	}
	if !signal.Trigger.Valid() {
		return from, false, contractErrorf("unknown invalidation trigger %q", signal.Trigger)
	}
	if !signal.Trigger.AffectsConclusion() {
		return from, false, nil
	}
	if from == DiscrepancyStateClosed {
		return DiscrepancyStatePendingVerify, true, nil
	}
	return from, false, nil
}

// EvaluateRecurrence applies the Q5 reopen rule: confirmed recurrence of the
// same identity reopens the original item (reopen_count+1, history retained).
// Unconfirmed reports are not recurrence.
func EvaluateRecurrence(from DiscrepancyState, confirmed bool) (DiscrepancyState, bool) {
	if !confirmed {
		return from, false
	}
	switch from {
	case DiscrepancyStateClosed, DiscrepancyStatePendingVerify:
		return DiscrepancyStateReopened, true
	}
	return from, false
}

// DiscrepancyTransitionRequest is one explicit, guarded discrepancy
// transition. Reason is required for audit quality on operator-driven edges.
type DiscrepancyTransitionRequest struct {
	DiscrepancyID string
	To            DiscrepancyState
	Actor         string
	Reason        string
	Guard         TransitionGuard
}

// DiscrepancyTransitionResult reports the applied transition.
type DiscrepancyTransitionResult struct {
	DiscrepancyID string
	From          DiscrepancyState
	To            DiscrepancyState
	ReopenCount   int64
}

// TransitionDiscrepancy applies one guarded transition in a short
// transaction: lock the discrepancy row, validate the edge and its guard,
// update state (reopen_count+1 on reopen; close_basis on close), audit,
// COMMIT. Illegal transitions and failed guards are refused and audited, never
// partially applied. The claim/dispose CAS paths (T022) call this helper.
func (s *Store) TransitionDiscrepancy(ctx context.Context, req DiscrepancyTransitionRequest) (DiscrepancyTransitionResult, error) {
	if s == nil || s.db == nil {
		return DiscrepancyTransitionResult{}, contractErrorf("store has no database")
	}
	if strings.TrimSpace(req.DiscrepancyID) == "" {
		return DiscrepancyTransitionResult{}, contractErrorf("transition requires a discrepancy_id")
	}
	if !req.To.Valid() {
		return DiscrepancyTransitionResult{}, contractErrorf("unknown target discrepancy state %q", req.To)
	}
	if strings.TrimSpace(req.Actor) == "" {
		return DiscrepancyTransitionResult{}, contractErrorf("transition requires an actor")
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return DiscrepancyTransitionResult{}, fmt.Errorf("begin discrepancy transition: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	var (
		from        DiscrepancyState
		reopenCount int64
	)
	err = tx.QueryRow(ctx, lockDiscrepancySQL, req.DiscrepancyID).Scan(&from, &reopenCount)
	if errors.Is(err, pgx.ErrNoRows) {
		return DiscrepancyTransitionResult{}, fmt.Errorf("%w: discrepancy_id %s", ErrDiscrepancyNotFound, req.DiscrepancyID)
	}
	if err != nil {
		return DiscrepancyTransitionResult{}, fmt.Errorf("lock discrepancy: %w", err)
	}
	if !from.Valid() {
		return DiscrepancyTransitionResult{}, contractErrorf("discrepancy %s has unknown state %q", req.DiscrepancyID, from)
	}
	result := DiscrepancyTransitionResult{DiscrepancyID: req.DiscrepancyID, From: from, To: req.To, ReopenCount: reopenCount}

	guardErr := ValidateDiscrepancyTransition(from, req.To, req.Guard)
	if guardErr != nil {
		if err := insertAuditTx(ctx, tx, AuditRecord{
			Actor:  req.Actor,
			Action: AuditActionRefuse,
			Target: discrepancyAuditTarget(req.DiscrepancyID, from, req.To, reopenCount),
			Reason: guardErr.Error(),
			Result: "refused",
		}); err != nil {
			return result, err
		}
		if err := tx.Commit(ctx); err != nil {
			return result, fmt.Errorf("commit refusal audit: %w", err)
		}
		return result, guardErr
	}

	reopenCount, err = persistDiscrepancyTransitionTx(ctx, tx, req.DiscrepancyID, from, req.To, req.Guard, req.Actor, req.Reason)
	if err != nil {
		return result, err
	}
	result.ReopenCount = reopenCount

	if err := tx.Commit(ctx); err != nil {
		return result, fmt.Errorf("commit discrepancy transition: %w", err)
	}
	return result, nil
}

// persistDiscrepancyTransitionTx writes one guard-approved discrepancy
// transition inside the caller's transaction: the state-predicate CAS update
// (the row lock is the caller's) plus the transition audit row. It never
// commits and never validates the edge: the caller MUST have validated it with
// ValidateDiscrepancyTransition and MUST hold the row lock of the same
// transaction. T022's dispose path applies a multi-step dispose through this
// same writer, so every lifecycle transition has one implementation.
func persistDiscrepancyTransitionTx(ctx context.Context, tx pgx.Tx, id string, from, to DiscrepancyState,
	guard TransitionGuard, actor, reason string) (int64, error) {
	var closeBasis any
	if to == DiscrepancyStateClosed {
		closeBasis = string(guard.CloseBasis)
	}
	var reopenCount int64
	err := tx.QueryRow(ctx, updateDiscrepancySQL, id, to, closeBasis, from, guard.ClaimOwner).Scan(&reopenCount)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("%w: concurrent state change", ErrIllegalDiscrepancyTransition)
	}
	if err != nil {
		return 0, fmt.Errorf("update discrepancy: %w", err)
	}
	action, auditResult := auditActionForDiscrepancyTransition(from, to)
	if err := insertAuditTx(ctx, tx, AuditRecord{
		Actor:  actor,
		Action: action,
		Target: discrepancyAuditTarget(id, from, to, reopenCount),
		Reason: reason,
		Result: auditResult,
	}); err != nil {
		return 0, err
	}
	return reopenCount, nil
}

// auditActionForDiscrepancyTransition maps a legal transition onto the closed
// recon_audit.action vocabulary (data-model.md §1.8). Invalidation audits as
// a reverify event because it only triggers the approved reverify flow.
func auditActionForDiscrepancyTransition(from, to DiscrepancyState) (AuditAction, string) {
	switch to {
	case DiscrepancyStateClaimed:
		return AuditActionClaim, "claimed"
	case DiscrepancyStateDisposing:
		return AuditActionDispose, "started"
	case DiscrepancyStatePendingVerify:
		if from == DiscrepancyStateClosed {
			return AuditActionReverify, "invalidated"
		}
		return AuditActionReverify, "pending"
	case DiscrepancyStateClosed:
		return AuditActionClose, "closed"
	case DiscrepancyStateReopened:
		return AuditActionReopen, "reopened"
	}
	return AuditActionRefuse, "refused"
}

// discrepancyAuditTarget renders the audit target of one transition.
func discrepancyAuditTarget(id string, from, to DiscrepancyState, reopenCount int64) map[string]any {
	return map[string]any{
		"discrepancy_id": id,
		"from":           string(from),
		"to":             string(to),
		"reopen_count":   reopenCount,
	}
}

// SQL statements for the discrepancy lifecycle. Column names follow
// data-model.md §1.4 (migration 000016, T004).

const lockDiscrepancySQL = `
SELECT state, reopen_count
FROM discrepancy
WHERE discrepancy_id = $1
FOR UPDATE`

const updateDiscrepancySQL = `
UPDATE discrepancy
SET state = $2,
    updated_at = now(),
    claim_owner = CASE WHEN $2 IN ('claimed', 'disposing')
        THEN COALESCE(NULLIF($5, ''), claim_owner) ELSE claim_owner END,
    claimed_at = CASE WHEN $2 = 'claimed'
        THEN COALESCE(claimed_at, now()) ELSE claimed_at END,
    close_basis = CASE WHEN $2 = 'closed' THEN $3::jsonb ELSE close_basis END,
    reopen_count = CASE WHEN $2 = 'reopened' THEN reopen_count + 1 ELSE reopen_count END
WHERE discrepancy_id = $1 AND state = $4
RETURNING reopen_count::bigint`

// ---------------------------------------------------------------------------
// T022: claim/dispose production paths (FR-010/012/013/016, Q1/Q2)
//
// These paths sit on the T007 guards above (ValidateDiscrepancyTransition,
// updateDiscrepancySQL, auditActionForDiscrepancyTransition) and on the T009
// evaluator, and they write only 014-owned rows (discrepancy, disposition,
// recon_audit):
//
//   - claim is a single-owner CAS (SELECT ... FOR UPDATE + state predicate).
//     It records responsibility only: it carries no execution right and never
//     hands the ticket to disposition. A competing claimer is refused with the
//     observed ownership hint and an audit row (US2-2).
//   - dispose records exactly one disposition row per idempotency_key (UNIQUE)
//     and, only for an effective result, applies disposing -> pending_verify
//     through the T007 guard. A repeated key reads the recorded row back in
//     place (persistent idempotency: no state change, no audit row, no
//     occurrence, no external action). A key reused with different input is an
//     operation_conflict with zero writes (the 011/013 shape).
//   - ack_only / reuse_recovery / new_fix_rule are recorded distinctly.
//     reuse_recovery only references an existing entry point (e.g.
//     "txlifecycle.UnknownRecovery", "events-admin replay"): 014 never executes
//     it and never substitutes its gates (FR-020); the caller MUST satisfy the
//     referenced capability's own authorization and gates separately, outside
//     the 014 transaction. new_fix_rule is dry_run-only in this phase (FR-023):
//     the kind can never record a real result, and whether even a dry-run is
//     invocable is decided by the T009 action vocabulary, not by this path.
//   - every action is authorized first: principal × action × scope against
//     recon_permission through the T009 evaluator (default deny). operator,
//     reason, evidence and idempotency_key are audit carriage; they are never
//     authorization inputs and never become a claim owner.
//
// State separation (FR-010/Q5): disposed ≠ reverified ≠ closed. These paths
// stop at pending_verify (or keep the item with the operator on a refused/
// failed result). They never write a reverify verdict, never write close_basis,
// never close, never accept risk, and never trigger recovery/replay/payment.
// Invalidation, evidence expiry, gap-limited evidence and version rotation are
// the T026/US3 paths and stay out of this write set.
// ---------------------------------------------------------------------------

var (
	// ErrDiscrepancyUnauthorized marks a claim/dispose refused by the T009
	// authorization evaluator (default deny). The refusal itself is audited by
	// the evaluator; operator/reason/evidence never authorize anything.
	ErrDiscrepancyUnauthorized = errors.New("reconciliation: discrepancy action is not authorized")

	// ErrDiscrepancyClaimTaken marks a claim that lost the single-owner CAS:
	// the ticket is already owned by another principal. The refusal carries the
	// observed owner (the US2-2 ownership hint) and is audited.
	ErrDiscrepancyClaimTaken = errors.New("reconciliation: discrepancy is already claimed by another owner")

	// ErrIdempotencyKeyReused marks a repeated dispose whose idempotency_key
	// already names a different disposition (different discrepancy, kind,
	// action_ref or result): operation_conflict with zero writes (the 011/013
	// shape), never a silent replay of the wrong row.
	ErrIdempotencyKeyReused = errors.New("reconciliation: idempotency key reused with different input")
)

// DiscrepancyAuthorizer is the T009 authorization seam every lifecycle action
// MUST pass before its write: principal × action × scope against
// recon_permission (default deny). *Evaluator implements it. A nil seam fails
// closed. The seam deliberately has no operator/reason/evidence/idempotency
// parameters: those fields are audit carriage and can never authorize (Q2).
type DiscrepancyAuthorizer interface {
	Authorize(ctx context.Context, principal Principal, action Action, scope AuthScope) (Decision, error)
}

// DiscrepancyAuthzError is one refused lifecycle action. Decision carries the
// machine reason; Err is set when the evaluator itself failed (registry read or
// refusal-audit write), which is still a denial, never an allow.
type DiscrepancyAuthzError struct {
	Action   Action
	Decision Decision
	Err      error
}

// Error renders the refusal without secrets.
func (e *DiscrepancyAuthzError) Error() string {
	if e == nil {
		return ErrDiscrepancyUnauthorized.Error()
	}
	if e.Err != nil {
		return fmt.Sprintf("%s: %s is not authorized: %v", ErrDiscrepancyUnauthorized, e.Action, e.Err)
	}
	return fmt.Sprintf("%s: %s refused: %s (%s)",
		ErrDiscrepancyUnauthorized, e.Action, e.Decision.Reason, e.Decision.Detail)
}

// Unwrap keeps errors.Is(err, ErrDiscrepancyUnauthorized) true and, when the
// evaluator failed, exposes that infrastructure error too.
func (e *DiscrepancyAuthzError) Unwrap() []error {
	if e == nil {
		return []error{ErrDiscrepancyUnauthorized}
	}
	if e.Err != nil {
		return []error{ErrDiscrepancyUnauthorized, e.Err}
	}
	return []error{ErrDiscrepancyUnauthorized}
}

// authorizeDiscrepancyAction runs one T009 evaluation. Any non-nil error or
// denial is a refusal (fail closed): the returned Decision is still the refusal
// record and the evaluator has already audited it.
func authorizeDiscrepancyAction(ctx context.Context, authorizer DiscrepancyAuthorizer, principal Principal,
	action Action, scope AuthScope) (Decision, error) {
	if authorizer == nil {
		return Decision{}, fmt.Errorf("%w: %w: authorization evaluator is not wired",
			ErrDiscrepancyUnauthorized, ErrAuthzUnavailable)
	}
	decision, err := authorizer.Authorize(ctx, principal, action, scope)
	if err != nil {
		return decision, &DiscrepancyAuthzError{Action: action, Decision: decision, Err: err}
	}
	if !decision.Allowed {
		return decision, &DiscrepancyAuthzError{Action: action, Decision: decision}
	}
	return decision, nil
}

// discrepancyAuthScope derives the authorization scope of one ticket from the
// scope marker stored with its evidence version domain (data-model.md §2). A
// ticket without a scope marker cannot be authorized: the caller must deny
// (invalid scope), never guess.
//
// Height tickets carry their concrete range. Time tickets mirror the T018/T023
// task-scope convention: the AuthScope range carrier is height-typed, so a time
// ticket is range-unbounded and can only be covered by a range-unbounded grant
// (matching stays exact-or-containment, never wildcarded).
func discrepancyAuthScope(id string, raw []byte) (AuthScope, error) {
	doc, err := ParsePersistedEvidenceDomain(raw)
	if err != nil {
		return AuthScope{}, fmt.Errorf("%w: discrepancy %s: %v", ErrInvalidAuthScope, id, err)
	}
	if doc.Scope == nil {
		return AuthScope{}, fmt.Errorf("%w: discrepancy %s has no recorded scope marker", ErrInvalidAuthScope, id)
	}
	if err := doc.Scope.Validate(); err != nil {
		return AuthScope{}, fmt.Errorf("%w: discrepancy %s scope marker: %v", ErrInvalidAuthScope, id, err)
	}
	scope := AuthScope{
		ChainID:       doc.Scope.ChainID,
		Kind:          doc.Scope.Kind,
		BusinessTypes: copyBusinessTypes(doc.Scope.BusinessTypes),
	}
	if doc.Scope.Kind == ScopeHeight {
		from, to := doc.Scope.From, doc.Scope.To
		scope.RangeStart, scope.RangeEnd = &from, &to
	}
	if err := scope.Validate(); err != nil {
		return AuthScope{}, err
	}
	return scope, nil
}

// readDiscrepancyLifecycleSQL reads the pre-authorization view of one ticket:
// its lifecycle state and the scope marker used to derive the authorization
// scope. It takes no lock: authorization must not run while a row lock is held.
const readDiscrepancyLifecycleSQL = `
SELECT state, COALESCE(claim_owner, ''), reopen_count::bigint, evidence_version_domain
FROM discrepancy
WHERE discrepancy_id = $1`

// discrepancyLifecycleRow is the read-only lifecycle view used before the
// authorization evaluation.
type discrepancyLifecycleRow struct {
	State          DiscrepancyState
	ClaimOwner     string
	ReopenCount    int64
	EvidenceDomain []byte
}

// readDiscrepancyLifecycle loads the pre-authorization view of one ticket.
func readDiscrepancyLifecycle(ctx context.Context, db StoreDB, id string) (discrepancyLifecycleRow, error) {
	var row discrepancyLifecycleRow
	err := db.QueryRow(ctx, readDiscrepancyLifecycleSQL, id).
		Scan(&row.State, &row.ClaimOwner, &row.ReopenCount, &row.EvidenceDomain)
	if errors.Is(err, pgx.ErrNoRows) {
		return row, fmt.Errorf("%w: discrepancy_id %s", ErrDiscrepancyNotFound, id)
	}
	if err != nil {
		return row, fmt.Errorf("read discrepancy lifecycle: %w", err)
	}
	if !row.State.Valid() {
		return row, contractErrorf("discrepancy %s has unknown state %q", id, row.State)
	}
	return row, nil
}

// lockDiscrepancyLifecycleSQL locks one ticket FOR UPDATE and reads the fields
// the claim/dispose CAS needs. The lock only lives inside the caller's short
// transaction (never across authorization or any slow call).
const lockDiscrepancyLifecycleSQL = `
SELECT state, COALESCE(claim_owner, ''), reopen_count::bigint
FROM discrepancy
WHERE discrepancy_id = $1
FOR UPDATE`

// DiscrepancyClaimRequest is one claim(owner) operation (contracts/
// discrepancy-lifecycle.md Transitions; data-model.md §3).
type DiscrepancyClaimRequest struct {
	// DiscrepancyID is the ticket to claim (UUID).
	DiscrepancyID string
	// Principal is the authenticated caller identity bound from its carrier
	// (API-key middleware pattern). It becomes the claim owner; operator free
	// text never reaches this field.
	Principal Principal
	// Authorizer is the T009 evaluation seam (nil fails closed).
	Authorizer DiscrepancyAuthorizer
	// Operator and Reason are audit annotations only; they never authorize and
	// never set the claim owner.
	Operator string
	Reason   string
}

// Validate checks the claim request shape before any I/O.
func (r DiscrepancyClaimRequest) Validate() error {
	if _, err := uuid.Parse(strings.TrimSpace(r.DiscrepancyID)); err != nil {
		return contractErrorf("claim requires a UUID discrepancy_id: %v", err)
	}
	if !r.Principal.Valid() {
		return fmt.Errorf("%w: claim requires an authenticated principal", ErrInvalidPrincipal)
	}
	if len(r.Operator) > 128 || strings.ContainsRune(r.Operator, 0) {
		return contractErrorf("claim operator annotation is malformed")
	}
	if len(r.Reason) > 1024 || strings.ContainsRune(r.Reason, 0) {
		return contractErrorf("claim reason is malformed")
	}
	return nil
}

// DiscrepancyClaimResult reports the claim outcome. On a refused competing
// claim Owner is the observed holder (the ownership hint) and Claimed is false.
type DiscrepancyClaimResult struct {
	DiscrepancyID string
	From          DiscrepancyState
	To            DiscrepancyState
	// Owner is the recorded claim owner after a successful claim; on a refusal
	// it is the observed current holder (US2-2 ownership hint).
	Owner       string
	ReopenCount int64
	Claimed     bool
}

// ClaimDiscrepancy claims one ticket for the authenticated principal
// (US2-1/US2-2, FR-011/FR-012):
//
//   - authorization first: the T009 evaluator checks principal × claim × the
//     ticket's recorded scope; a denial is audited by the evaluator and nothing
//     is written;
//   - the CAS locks the row (SELECT ... FOR UPDATE) and applies the state
//     predicate (open_claimable -> claimed); a competing claimer is refused
//     with the observed owner and an audit row naming that ownership;
//   - the claim records responsibility only: it does not authorize disposal,
//     closure or any downstream capability (FR-020).
func (s *Store) ClaimDiscrepancy(ctx context.Context, req DiscrepancyClaimRequest) (DiscrepancyClaimResult, error) {
	result := DiscrepancyClaimResult{DiscrepancyID: strings.TrimSpace(req.DiscrepancyID)}
	if s == nil || s.db == nil {
		return result, contractErrorf("store has no database")
	}
	if err := req.Validate(); err != nil {
		return result, err
	}
	if req.Authorizer == nil {
		return result, fmt.Errorf("%w: %w: authorization evaluator is not wired",
			ErrDiscrepancyUnauthorized, ErrAuthzUnavailable)
	}
	id := result.DiscrepancyID
	owner := req.Principal.String()

	row, err := readDiscrepancyLifecycle(ctx, s.db, id)
	if err != nil {
		return result, err
	}
	scope, scopeErr := discrepancyAuthScope(id, row.EvidenceDomain)
	if scopeErr != nil {
		// No derivable scope: fail closed through the evaluator so the refusal
		// is audited as an invalid-scope denial, never guessed around.
		if _, err := authorizeDiscrepancyAction(ctx, req.Authorizer, req.Principal, ActionClaim, AuthScope{}); err != nil {
			return result, err
		}
		return result, fmt.Errorf("%w: %v", ErrDiscrepancyUnauthorized, scopeErr)
	}
	if _, err := authorizeDiscrepancyAction(ctx, req.Authorizer, req.Principal, ActionClaim, scope); err != nil {
		return result, err
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return result, fmt.Errorf("begin discrepancy claim: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	var (
		from    DiscrepancyState
		current string
		reopen  int64
	)
	err = tx.QueryRow(ctx, lockDiscrepancyLifecycleSQL, id).Scan(&from, &current, &reopen)
	if errors.Is(err, pgx.ErrNoRows) {
		return result, fmt.Errorf("%w: discrepancy_id %s", ErrDiscrepancyNotFound, id)
	}
	if err != nil {
		return result, fmt.Errorf("lock discrepancy: %w", err)
	}
	if !from.Valid() {
		return result, contractErrorf("discrepancy %s has unknown state %q", id, from)
	}
	result.From, result.Owner, result.ReopenCount = from, current, reopen

	if from != DiscrepancyStateOpenClaimable {
		// The single-owner CAS lost: return and audit the observed ownership
		// (the US2-2 ownership hint), never overwrite the holder.
		refusal := fmt.Errorf("%w: %w (discrepancy %s is %s, owner %s)",
			ErrIllegalDiscrepancyTransition, ErrDiscrepancyClaimTaken, id, from, claimOwnerHint(current))
		target := discrepancyAuditTarget(id, from, DiscrepancyStateClaimed, reopen)
		target["action"] = string(ActionClaim)
		target["owner"] = current
		target["requested_owner"] = owner
		if err := insertAuditTx(ctx, tx, AuditRecord{
			Actor:  discrepancyAuditActor(req.Principal),
			Action: AuditActionRefuse,
			Target: target,
			Reason: refusal.Error(),
			Result: "refused",
		}); err != nil {
			return result, err
		}
		if err := tx.Commit(ctx); err != nil {
			return result, fmt.Errorf("commit claim refusal audit: %w", err)
		}
		return result, refusal
	}

	if err := ValidateDiscrepancyTransition(from, DiscrepancyStateClaimed, TransitionGuard{ClaimOwner: owner}); err != nil {
		return result, err
	}
	if err := tx.QueryRow(ctx, updateDiscrepancySQL, id, DiscrepancyStateClaimed, nil, from, owner).
		Scan(&reopen); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return result, fmt.Errorf("%w: concurrent state change", ErrIllegalDiscrepancyTransition)
		}
		return result, fmt.Errorf("update discrepancy claim: %w", err)
	}
	result.To, result.Owner, result.ReopenCount, result.Claimed = DiscrepancyStateClaimed, owner, reopen, true

	target := discrepancyAuditTarget(id, from, DiscrepancyStateClaimed, reopen)
	target["action"] = string(ActionClaim)
	target["owner"] = owner
	if err := insertAuditTx(ctx, tx, AuditRecord{
		Actor:    discrepancyAuditActor(req.Principal),
		Action:   AuditActionClaim,
		Target:   target,
		Reason:   claimAuditReason(req.Reason),
		Evidence: operatorAnnotation(req.Operator),
		Result:   "claimed",
	}); err != nil {
		return result, err
	}
	if err := tx.Commit(ctx); err != nil {
		return result, fmt.Errorf("commit discrepancy claim: %w", err)
	}
	return result, nil
}

// DispositionAuthAction maps one disposition kind onto its T009 action. The
// caller authorizes that action before any write; the kind's permission is
// resolved by the evaluator (dispose_ack / dispose_reuse; new_fix_rule has no
// grantable permission in this phase).
func DispositionAuthAction(kind DispositionKind) (Action, error) {
	switch kind {
	case DispositionAckOnly:
		return ActionDisposeAck, nil
	case DispositionReuseRecovery:
		return ActionDisposeReuse, nil
	case DispositionNewFixRule:
		return ActionDisposeNewFixRule, nil
	default:
		return "", contractErrorf("unknown disposition kind %q", kind)
	}
}

// DiscrepancyDisposeRequest is one dispose(kind, action_ref, idempotency_key)
// operation (contracts/discrepancy-lifecycle.md Transitions; data-model.md
// §1.6/§3).
type DiscrepancyDisposeRequest struct {
	DiscrepancyID string
	Kind          DispositionKind
	// ActionRef references an existing entry point (e.g.
	// "txlifecycle.UnknownRecovery", "events-admin replay"). Required for
	// reuse_recovery and new_fix_rule; ack_only may omit it. 014 only records
	// the reference and never executes it.
	ActionRef string
	// Result optionally records a non-effective outcome (refused|failed) for
	// ack_only/reuse_recovery. Empty derives done (ack_only, reuse_recovery) or
	// dry_run (new_fix_rule). new_fix_rule is dry_run-only in this phase, and a
	// refused/failed result never hands the item to verification.
	Result DispositionResult
	// Operator and Reason are required audit annotations (never authorization
	// inputs; the 011/012/013 precedent).
	Operator string
	Reason   string
	// EvidenceRef is an optional bounded evidence annotation.
	EvidenceRef string
	// IdempotencyKey is the persistent idempotency carriage: UNIQUE in
	// disposition; a repeat reads the recorded row back (zero side effects).
	IdempotencyKey string
	// Principal is the authenticated caller identity.
	Principal Principal
	// Authorizer is the T009 evaluation seam (nil fails closed).
	Authorizer DiscrepancyAuthorizer
}

// Validate checks the dispose request shape and derives the effective result
// before any I/O. new_fix_rule can only ever record dry_run (FR-023); a
// refused/failed result is allowed only for the other two kinds and never
// transitions the ticket.
func (r DiscrepancyDisposeRequest) Validate() (DispositionResult, error) {
	if _, err := uuid.Parse(strings.TrimSpace(r.DiscrepancyID)); err != nil {
		return "", contractErrorf("dispose requires a UUID discrepancy_id: %v", err)
	}
	if !r.Kind.Valid() {
		return "", contractErrorf("unknown disposition kind %q", r.Kind)
	}
	actionRef := strings.TrimSpace(r.ActionRef)
	if r.Kind != DispositionAckOnly && actionRef == "" {
		return "", contractErrorf("disposition kind %q requires an action_ref to an existing entry point", r.Kind)
	}
	if len(actionRef) > 512 || strings.ContainsRune(actionRef, 0) {
		return "", contractErrorf("disposition action_ref is malformed")
	}
	result := r.Result
	if result == "" {
		if r.Kind == DispositionNewFixRule {
			result = DispositionDryRun
		} else {
			result = DispositionDone
		}
	}
	switch r.Kind {
	case DispositionNewFixRule:
		if result != DispositionDryRun {
			return "", contractErrorf("new_fix_rule is dry_run-only in this phase (FR-023)")
		}
	default:
		switch result {
		case DispositionDone, DispositionRefused, DispositionFailed:
		default:
			return "", contractErrorf("disposition kind %q cannot record result %q", r.Kind, result)
		}
	}
	// The shared T007 vocabulary/phase guard is the final shape check.
	if err := ValidateDisposition(DispositionSnapshot{Kind: r.Kind, Result: result, ActionRef: actionRef}); err != nil {
		return "", err
	}
	operator := strings.TrimSpace(r.Operator)
	if operator == "" || len(operator) > 128 || strings.ContainsRune(operator, 0) {
		return "", contractErrorf("disposition requires an operator annotation of 1..128 characters")
	}
	reason := strings.TrimSpace(r.Reason)
	if reason == "" || len(reason) > 1024 || strings.ContainsRune(reason, 0) {
		return "", contractErrorf("disposition requires a reason of 1..1024 characters")
	}
	evidence := strings.TrimSpace(r.EvidenceRef)
	if len(evidence) > 512 || strings.ContainsRune(evidence, 0) {
		return "", contractErrorf("disposition evidence_ref is malformed")
	}
	key := strings.TrimSpace(r.IdempotencyKey)
	if key == "" || len(key) > 256 || strings.ContainsRune(key, 0) {
		return "", contractErrorf("disposition requires an idempotency_key of 1..256 characters")
	}
	if !r.Principal.Valid() {
		return "", fmt.Errorf("%w: dispose requires an authenticated principal", ErrInvalidPrincipal)
	}
	return result, nil
}

// DiscrepancyDisposeResult reports the recorded disposition and its lifecycle
// effect. StateAfter == pending_verify means the item was handed to
// verification: that is NOT verification and NOT closure (FR-010/Q5).
type DiscrepancyDisposeResult struct {
	DispositionID string
	DiscrepancyID string
	Kind          DispositionKind
	Result        DispositionResult
	ActionRef     string
	// StateBefore and StateAfter bound the lifecycle effect of this call.
	StateBefore DiscrepancyState
	StateAfter  DiscrepancyState
	// Transitioned reports that this call applied the dispose hand-over
	// (disposing -> pending_verify).
	Transitioned bool
	// IdempotentReplay reports that a repeated idempotency_key converged on the
	// recorded row with zero side effects (no state change, no audit row).
	IdempotentReplay bool
	// TargetGatesRequired reports that the referenced capability's own
	// authorization/gates still apply and were neither executed nor substituted
	// by 014 (reuse_recovery, FR-020).
	TargetGatesRequired bool
	ReopenCount         int64
	RecordedAt          time.Time
}

// DisposeDiscrepancy records one disposition and, only for an effective result,
// applies the dispose hand-over through the T007 guard (FR-010/013/016):
//
//   - authorization first: the T009 evaluator checks principal × the kind's
//     action × the ticket's recorded scope; a denial is audited and nothing is
//     written. Claim ownership never grants disposal (a claim is responsibility
//     only); the specific dispose permission is what gates this path;
//   - the disposition row is inserted with its UNIQUE idempotency_key inside
//     the same transaction as the transition: a repeated key reads the recorded
//     row back with zero side effects, a key reused with different input is an
//     operation_conflict with zero writes;
//   - from claimed the path first applies claimed -> disposing, then
//     disposing -> pending_verify for an effective result. A refused/failed
//     result is recorded but stays with the operator (disposed ≠ reverified);
//   - reuse_recovery only records the reference to an existing entry point:
//     014 never executes it and its own gates MUST be satisfied separately,
//     outside this transaction (FR-020).
func (s *Store) DisposeDiscrepancy(ctx context.Context, req DiscrepancyDisposeRequest) (DiscrepancyDisposeResult, error) {
	result := DiscrepancyDisposeResult{
		DiscrepancyID: strings.TrimSpace(req.DiscrepancyID),
		Kind:          req.Kind,
		ActionRef:     strings.TrimSpace(req.ActionRef),
	}
	if s == nil || s.db == nil {
		return result, contractErrorf("store has no database")
	}
	derived, err := req.Validate()
	if err != nil {
		return result, err
	}
	result.Result = derived
	if req.Authorizer == nil {
		return result, fmt.Errorf("%w: %w: authorization evaluator is not wired",
			ErrDiscrepancyUnauthorized, ErrAuthzUnavailable)
	}
	id := result.DiscrepancyID
	key := strings.TrimSpace(req.IdempotencyKey)

	action, err := DispositionAuthAction(req.Kind)
	if err != nil {
		return result, err
	}
	row, err := readDiscrepancyLifecycle(ctx, s.db, id)
	if err != nil {
		return result, err
	}
	scope, scopeErr := discrepancyAuthScope(id, row.EvidenceDomain)
	if scopeErr != nil {
		if _, err := authorizeDiscrepancyAction(ctx, req.Authorizer, req.Principal, action, AuthScope{}); err != nil {
			return result, err
		}
		return result, fmt.Errorf("%w: %v", ErrDiscrepancyUnauthorized, scopeErr)
	}
	decision, err := authorizeDiscrepancyAction(ctx, req.Authorizer, req.Principal, action, scope)
	if err != nil {
		return result, err
	}
	result.TargetGatesRequired = decision.RequiresTargetGates
	snapshot := DispositionSnapshot{Kind: req.Kind, Result: derived, ActionRef: result.ActionRef}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return result, fmt.Errorf("begin discrepancy dispose: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	var (
		state  DiscrepancyState
		owner  string
		reopen int64
	)
	err = tx.QueryRow(ctx, lockDiscrepancyLifecycleSQL, id).Scan(&state, &owner, &reopen)
	if errors.Is(err, pgx.ErrNoRows) {
		return result, fmt.Errorf("%w: discrepancy_id %s", ErrDiscrepancyNotFound, id)
	}
	if err != nil {
		return result, fmt.Errorf("lock discrepancy: %w", err)
	}
	if !state.Valid() {
		return result, contractErrorf("discrepancy %s has unknown state %q", id, state)
	}
	result.StateBefore, result.StateAfter, result.ReopenCount = state, state, reopen

	// Record the disposition first (after the row lock, so the FK's KEY SHARE
	// lock cannot deadlock with the FOR UPDATE upgrade): a repeated
	// idempotency_key must converge on the recorded row even when the ticket
	// has already left `disposing`.
	dispositionID := uuid.NewString()
	var createdAt time.Time
	err = tx.QueryRow(ctx, insertDispositionSQL, dispositionID, id, string(req.Kind), result.ActionRef,
		strings.TrimSpace(req.Operator), strings.TrimSpace(req.Reason), strings.TrimSpace(req.EvidenceRef),
		string(derived), key).Scan(&createdAt)
	if err != nil {
		if uniqueViolationConstraint(err) != dispositionIdempotencyKeyConstraint {
			return result, fmt.Errorf("insert disposition: %w", err)
		}
		// Persistent idempotency: the UNIQUE key already names a recorded
		// disposition. Roll this attempt back (releasing the row lock) and
		// converge on the recorded row with zero side effects — no state
		// change, no audit row, no occurrence, no external action.
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return s.replayDisposition(ctx, req, derived, result)
	}
	result.DispositionID = dispositionID
	result.RecordedAt = createdAt

	if state != DiscrepancyStateClaimed && state != DiscrepancyStateDisposing {
		refusal := fmt.Errorf("%w: discrepancy %s is %s; dispose requires claimed or disposing",
			ErrIllegalDiscrepancyTransition, id, state)
		return s.refuseDispose(ctx, tx, result, req, state, owner, refusal)
	}

	actor := discrepancyAuditActor(req.Principal)
	reason := strings.TrimSpace(req.Reason)

	if state == DiscrepancyStateClaimed {
		// The dispose action starts the disposition step; ownership stays as
		// recorded (the claim is responsibility, not an execution right).
		guard := TransitionGuard{ClaimOwner: owner}
		if err := ValidateDiscrepancyTransition(state, DiscrepancyStateDisposing, guard); err != nil {
			return s.refuseDispose(ctx, tx, result, req, state, owner, err)
		}
		nextReopen, err := persistDiscrepancyTransitionTx(ctx, tx, id, state, DiscrepancyStateDisposing, guard, actor, reason)
		if err != nil {
			return s.refuseDispose(ctx, tx, result, req, state, owner, err)
		}
		state, result.StateAfter, result.ReopenCount = DiscrepancyStateDisposing, DiscrepancyStateDisposing, nextReopen
	}

	if snapshot.effective() {
		guard := TransitionGuard{Disposition: &snapshot}
		if err := ValidateDiscrepancyTransition(state, DiscrepancyStatePendingVerify, guard); err != nil {
			return s.refuseDispose(ctx, tx, result, req, state, owner, err)
		}
		nextReopen, err := persistDiscrepancyTransitionTx(ctx, tx, id, state, DiscrepancyStatePendingVerify, guard, actor, reason)
		if err != nil {
			return s.refuseDispose(ctx, tx, result, req, state, owner, err)
		}
		result.StateAfter, result.Transitioned, result.ReopenCount = DiscrepancyStatePendingVerify, true, nextReopen
	}

	target := map[string]any{
		"discrepancy_id":        id,
		"disposition_id":        result.DispositionID,
		"kind":                  string(req.Kind),
		"action_ref":            result.ActionRef,
		"result":                string(derived),
		"state":                 string(result.StateAfter),
		"idempotency_key":       key,
		"executed_by_014":       false,
		"target_gates_required": result.TargetGatesRequired,
	}
	if err := insertAuditTx(ctx, tx, AuditRecord{
		Actor:    actor,
		Action:   AuditActionDispose,
		Target:   target,
		Reason:   reason,
		Evidence: dispositionEvidence(req),
		Result:   string(derived),
	}); err != nil {
		return result, err
	}
	if err := tx.Commit(ctx); err != nil {
		return result, fmt.Errorf("commit discrepancy dispose: %w", err)
	}
	return result, nil
}

// refuseDispose rolls back the in-flight dispose transaction (the uncommitted
// disposition row and any partial transition are discarded) and then appends
// the refusal audit in its own short transaction. The idempotency key is not
// consumed by an attempt that changed nothing.
func (s *Store) refuseDispose(ctx context.Context, tx pgx.Tx, result DiscrepancyDisposeResult,
	req DiscrepancyDisposeRequest, state DiscrepancyState, owner string, refusal error) (DiscrepancyDisposeResult, error) {
	_ = tx.Rollback(context.WithoutCancel(ctx))
	result.StateAfter = state
	// The rolled-back disposition never existed: report no recorded row rather
	// than a phantom id/result.
	result.DispositionID = ""
	result.Result = ""
	action, actionErr := DispositionAuthAction(req.Kind)
	if actionErr != nil {
		return result, actionErr
	}
	auditTx, err := s.db.Begin(ctx)
	if err != nil {
		return result, fmt.Errorf("begin dispose refusal audit: %w", err)
	}
	defer func() { _ = auditTx.Rollback(context.WithoutCancel(ctx)) }()
	if err := insertAuditTx(ctx, auditTx, AuditRecord{
		Actor:  discrepancyAuditActor(req.Principal),
		Action: AuditActionRefuse,
		Target: map[string]any{
			"discrepancy_id":  result.DiscrepancyID,
			"action":          string(action),
			"kind":            string(req.Kind),
			"action_ref":      result.ActionRef,
			"idempotency_key": strings.TrimSpace(req.IdempotencyKey),
			"state":           string(state),
			"owner":           owner,
			"to":              string(DiscrepancyStatePendingVerify),
		},
		Reason: refusal.Error(),
		Result: "refused",
	}); err != nil {
		return result, err
	}
	if err := auditTx.Commit(ctx); err != nil {
		return result, fmt.Errorf("commit dispose refusal audit: %w", err)
	}
	return result, refusal
}

// replayDisposition converges a repeated idempotency_key on the recorded row.
// The read-back is the persistent dedup: no state change, no audit row, no
// occurrence, no external action. A key that names a different input is an
// operation_conflict with zero writes, never a silent replay of the wrong row.
func (s *Store) replayDisposition(ctx context.Context, req DiscrepancyDisposeRequest, derived DispositionResult,
	result DiscrepancyDisposeResult) (DiscrepancyDisposeResult, error) {
	var (
		recordedID, recordedDiscrepancyID string
		recordedKind, recordedActionRef   string
		recordedResult, recordedKey       string
		createdAt                         time.Time
		state                             DiscrepancyState
		reopen                            int64
	)
	err := s.db.QueryRow(ctx, readDispositionByKeySQL, strings.TrimSpace(req.IdempotencyKey)).Scan(
		&recordedID, &recordedDiscrepancyID, &recordedKind, &recordedActionRef,
		&recordedResult, &recordedKey, &createdAt, &state, &reopen)
	if errors.Is(err, pgx.ErrNoRows) {
		return result, fmt.Errorf("%w: idempotency_key %q conflicted but no recorded disposition exists",
			ErrContract, strings.TrimSpace(req.IdempotencyKey))
	}
	if err != nil {
		return result, fmt.Errorf("read recorded disposition: %w", err)
	}
	if recordedDiscrepancyID != result.DiscrepancyID || DispositionKind(recordedKind) != req.Kind ||
		recordedActionRef != result.ActionRef || DispositionResult(recordedResult) != derived {
		return result, fmt.Errorf(
			"%w: idempotency_key %q already recorded discrepancy=%s kind=%s action_ref=%q result=%s",
			ErrIdempotencyKeyReused, recordedKey, recordedDiscrepancyID, recordedKind, recordedActionRef, recordedResult)
	}
	result.DispositionID = recordedID
	result.Result = DispositionResult(recordedResult)
	result.StateBefore, result.StateAfter = state, state
	result.ReopenCount = reopen
	result.RecordedAt = createdAt
	result.IdempotentReplay = true
	result.Transitioned = false
	result.TargetGatesRequired = req.Kind == DispositionReuseRecovery
	return result, nil
}

// claimOwnerHint renders the observed claim holder for a refusal message: an
// empty owner is explicit, never silently blank.
func claimOwnerHint(owner string) string {
	if owner = strings.TrimSpace(owner); owner != "" {
		return owner
	}
	return "<none>"
}

// claimAuditReason keeps the claim audit reason informative when the caller
// supplied none.
func claimAuditReason(reason string) string {
	if reason = strings.TrimSpace(reason); reason != "" {
		return reason
	}
	return "claim"
}

// operatorAnnotation renders the optional operator audit annotation. It is
// free-text carriage only and never an authorization input.
func operatorAnnotation(operator string) string {
	if operator = strings.TrimSpace(operator); operator != "" {
		return "operator=" + operator
	}
	return ""
}

// dispositionEvidence renders the bounded audit annotation of one disposition.
func dispositionEvidence(req DiscrepancyDisposeRequest) string {
	parts := []string{fmt.Sprintf("operator=%s", strings.TrimSpace(req.Operator))}
	if evidence := strings.TrimSpace(req.EvidenceRef); evidence != "" {
		parts = append(parts, "evidence_ref="+evidence)
	}
	parts = append(parts, "executed_by_014=false")
	if req.Kind == DispositionReuseRecovery {
		parts = append(parts, "target_gates_required=true")
	}
	return strings.Join(parts, " ")
}

// discrepancyAuditActor renders the authenticated principal for the
// recon_audit.actor column (1..128 bytes). A deployment-configured principal
// longer than the column is truncated for the audit annotation only; the
// permission evaluation always uses the full identity.
func discrepancyAuditActor(p Principal) string {
	actor := p.String()
	if len(actor) > 128 {
		actor = actor[:128]
	}
	return actor
}

// dispositionIdempotencyKeyConstraint is the UNIQUE constraint a repeated
// dispose classifies as its read-back trigger (migration 000016).
const dispositionIdempotencyKeyConstraint = "disposition_idempotency_key_uniq"

// insertDispositionSQL records one disposition row. The UNIQUE idempotency_key
// is the persistent idempotency carriage: a repeated dispose must classify the
// 23505 on disposition_idempotency_key_uniq and read the recorded row back.
const insertDispositionSQL = `
INSERT INTO disposition (
    disposition_id, discrepancy_id, kind, action_ref, operator, reason, evidence_ref, result, idempotency_key)
VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8, $9)
RETURNING created_at`

// readDispositionByKeySQL reads the recorded disposition row plus the current
// ticket state (read-only) for the idempotent replay path. Only the business
// input (discrepancy, kind, action_ref, result) is compared on a repeat; the
// operator/reason/evidence annotations stay audit metadata.
const readDispositionByKeySQL = `
SELECT d.disposition_id::text, d.discrepancy_id::text, d.kind, d.action_ref,
       d.result, d.idempotency_key, d.created_at,
       disc.state, disc.reopen_count::bigint
FROM disposition d
JOIN discrepancy disc ON disc.discrepancy_id = d.discrepancy_id
WHERE d.idempotency_key = $1`
