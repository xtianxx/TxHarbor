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
// Claim/dispose wiring (T022) and the invalidation evaluator (T026) build on
// these guards; this file owns only the state machine.
package reconciliation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

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

	var closeBasis any
	if req.To == DiscrepancyStateClosed {
		closeBasis = string(req.Guard.CloseBasis)
	}
	err = tx.QueryRow(ctx, updateDiscrepancySQL,
		req.DiscrepancyID, req.To, closeBasis, from, req.Guard.ClaimOwner).Scan(&reopenCount)
	if errors.Is(err, pgx.ErrNoRows) {
		return result, fmt.Errorf("%w: concurrent state change", ErrIllegalDiscrepancyTransition)
	}
	if err != nil {
		return result, fmt.Errorf("update discrepancy: %w", err)
	}
	result.ReopenCount = reopenCount

	action, auditResult := auditActionForDiscrepancyTransition(from, req.To)
	if err := insertAuditTx(ctx, tx, AuditRecord{
		Actor:  req.Actor,
		Action: action,
		Target: discrepancyAuditTarget(req.DiscrepancyID, from, req.To, reopenCount),
		Reason: req.Reason,
		Result: auditResult,
	}); err != nil {
		return result, err
	}

	if err := tx.Commit(ctx); err != nil {
		return result, fmt.Errorf("commit discrepancy transition: %w", err)
	}
	return result, nil
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
