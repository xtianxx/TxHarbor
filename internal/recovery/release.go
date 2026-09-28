// release.go implements T049 [US4]: the append-only release/revoke decision
// path of FR-021/FR-022/FR-024 — the only way a release decision enters the
// recovery_release stream (data-model.md §1.9/§3/§4.1/§4.2;
// contracts/approval-matrix.md §1/§3–§5; tasks.md T049).
//
// What a release is here (derived, never stored):
//
//   - A release row is a decision record, not a permission: no writable
//     "released"/"valid" boolean exists anywhere (INV-2). The current release
//     of (instance, capability, scope_hash) is the latest row by commit order
//     when it is a release and no later revoke of the same stream covers it;
//     every gate evaluation re-reads that stream under the instance row lock
//     and re-derives every §3 condition, so an old allow can never be reused
//     (F5) and an explicit revoke refuses the very next evaluation.
//   - Release records the approval basis of data-model §3.1: the current
//     approve decision of every approver-role principal of the instance is
//     judged by the single derived judgment (Gate.approvalsValid) — this file
//     re-implements none of the approval rules — and only an accepted basis is
//     recorded, deterministically sorted (dedupeApprovalRefs): a dual
//     capability records exactly two approvals by two distinct people, the
//     rest one non-executor approval. All other §3 conditions (capability
//     dependencies, isolation dependency set, gaps, existing fund gates) stay
//     evaluation-time conditions: releasing overrides no hard gate and starts
//     no signing/broadcast/payment action by itself.
//   - The row binds the authoritative (evidence_generation, evidence_hash)
//     token read under the lock: any later accepted evidence write invalidates
//     it immediately (approval_stale / release_invalidated_generation).
//   - revoke is explicit and authorized: RevokeRelease appends a revoke row of
//     the same stream; it covers only that stream and needs no approval basis
//     (withdrawing a release is fail-safe, never fail-open). The stream then
//     refuses the next evaluation with release_revoked.
//   - release and revoke require the authenticated caller to hold the
//     instance's recovery_execute binding (opened_by or an executor-role
//     participant); an unregistered, non-executor or free-form principal
//     refuses, is audited, and writes zero decision rows.
//
// Append-only and idempotent: every accepted call appends exactly one
// recovery_release row plus its paired ok audit row through the version-
// guarded control store (controlstore.InsertReleaseDecision). operation_id is
// the idempotency key: the same input reads the recorded row back with zero
// writes (Recorded=true); a different input under the same operation_id
// refuses with controlstore.ErrOperationConflict and zero writes. The
// close/supersede guards live in instance.go and are unchanged: close requires
// all seven capabilities currently release-valid and refuses over an open or
// escalated gap; supersede stays explicit and two-person approved.
//
// scope_hash is passed through exactly as requested (T050 owns the canonical
// vocabulary): this file never normalizes it, only exact matches are accepted,
// and an approval recorded for a different scope refuses (scope_mismatch from
// the shared judgment).
//
// The control store is reached only through a *controlstore.Store built by
// controlstore.NewStore, so the T069 schema-version guard (unknown or
// incompatible version => control_store_unavailable) is inherited and there
// is no unguarded release write path.
package recovery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

// The release decisions of recovery_release.decision (closed set).
const (
	releaseDecisionRelease = "release"
	releaseDecisionRevoke  = "revoke"

	// releaseFallbackActor is the audit actor of a refusal whose subject is not
	// in the authenticated <kind>:<id> form. It is an audit label only and
	// never authorizes anything.
	releaseFallbackActor = "system:recovery-release"
)

var (
	// ErrReleaseRequest marks a malformed release request (unknown capability,
	// non-UUID instance, missing scope or operation_id, missing control-store
	// handle or gate). It is a caller contract error, never a refusal.
	ErrReleaseRequest = errors.New("recovery release request is invalid")

	// ErrReleaseRefused marks a refused release/revoke decision: the caller
	// holds no recovery_execute binding on the instance, or the current
	// approval basis does not satisfy the conservative required class. The
	// refusal carries a closed-set class, is audited, and writes zero decision
	// rows.
	ErrReleaseRefused = errors.New("recovery release refused")
)

// ReleaseRequest is one append-only release/revoke request for one
// (instance, capability, scope_hash) stream. ScopeHash is passed through
// unchanged (T050 owns its canonical form); Reason is an audit annotation
// only; OperationID is the persistent idempotency key. The caller can never
// name the approvals to record: they are derived from the current approve
// decisions of the instance's approvers.
type ReleaseRequest struct {
	InstanceID  string
	Capability  Capability
	ScopeHash   string
	Principal   string
	Reason      string
	OperationID string
}

// ReleaseOutcome is the result of one release/revoke call. On success Decision
// is release|revoke, ReleaseID names the appended row and ApprovalRefs carries
// the deterministic ascending approval basis of a release (empty for a
// revoke).
//
// On a refusal, Decision repeats the request, RefusalClass is one member of
// the closed gate refusal set and Reason explains it; the refusal was audited
// and no decision row was written. Recorded is true when the call replayed an
// already-recorded decision for the same operation_id and input (zero writes);
// ReleaseID then names the recorded row.
type ReleaseOutcome struct {
	ReleaseID    string
	Decision     string
	ApprovalRefs []string
	Recorded     bool
	RefusalClass RefusalClass
	Reason       string
}

// Release appends one explicit release decision for (instance, capability,
// scope_hash) by the authenticated executor. The recorded approval basis is
// derived from the instance's current approve decisions and judged by the
// single derived judgment; an unsatisfied basis refuses, is audited and
// writes no release row. A release decision is not a resumption: the gate
// re-derives every condition on each evaluation and never lets an approval or
// release cover a hard gate.
func Release(ctx context.Context, store *controlstore.Store, gate *Gate, req ReleaseRequest) (ReleaseOutcome, error) {
	return decideRelease(ctx, store, gate, req, releaseDecisionRelease)
}

// RevokeRelease appends one explicit revoke decision of the same stream. It
// is authorized and idempotent exactly like Release; it covers only the
// (instance, capability, scope_hash) stream it names, needs no approval basis
// (withdrawing a release is always allowed, fail-safe), and the next gate
// evaluation refuses with release_revoked until a new release is recorded.
func RevokeRelease(ctx context.Context, store *controlstore.Store, gate *Gate, req ReleaseRequest) (ReleaseOutcome, error) {
	return decideRelease(ctx, store, gate, req, releaseDecisionRevoke)
}

// decideRelease runs one release/revoke decision: request-shape validation,
// then the executor authorization, the approval-basis derivation (release
// only) and the append inside the instance row lock the gate and every
// decision writer share.
func decideRelease(ctx context.Context, store *controlstore.Store, gate *Gate, req ReleaseRequest, decision string) (ReleaseOutcome, error) {
	if store == nil {
		return ReleaseOutcome{Decision: decision},
			fmt.Errorf("%w: a controlstore.Store built by controlstore.NewStore is required", ErrReleaseRequest)
	}
	if gate == nil {
		return ReleaseOutcome{Decision: decision},
			fmt.Errorf("%w: the derived release evaluation gate is required; no release verdict exists without it", ErrReleaseRequest)
	}
	instanceID, err := lifecycleInstanceID(req.InstanceID)
	if err != nil {
		return ReleaseOutcome{Decision: decision}, fmt.Errorf("%w: %v", ErrReleaseRequest, err)
	}
	if !req.Capability.Known() {
		return ReleaseOutcome{Decision: decision}, fmt.Errorf(
			"%w: capability %q is outside the closed 7-capability set", ErrReleaseRequest, req.Capability)
	}
	scope := strings.TrimSpace(req.ScopeHash)
	if scope == "" {
		return ReleaseOutcome{Decision: decision}, fmt.Errorf(
			"%w: scope_hash is required; a release never substitutes a default scope", ErrReleaseRequest)
	}
	operationID, err := approvalOperationID(req.OperationID)
	if err != nil {
		return ReleaseOutcome{Decision: decision}, fmt.Errorf("%w: %v", ErrReleaseRequest, err)
	}
	reason := boundedApprovalReason(req.Reason)
	principalText := strings.TrimSpace(req.Principal)
	outcome := ReleaseOutcome{Decision: decision}
	refusal := releaseRefusal{
		requestedInstance: instanceID,
		principal:         principalText,
		capability:        req.Capability,
		scope:             scope,
		decision:          decision,
		operationID:       operationID,
	}

	// The subject must be an authenticated <kind>:<id> principal: free-form
	// text can never stand in for authentication (approval-matrix §2).
	principal, err := controlstore.NormalizePrincipal(principalText)
	if err != nil {
		refusal.class = RefusalApprovalMissing
		refusal.reason = err.Error()
		return refusedRelease(ctx, store, outcome, refusal)
	}
	refusal.principal = principal

	tx, err := store.Pool().Begin(ctx)
	if err != nil {
		return outcome, fmt.Errorf("begin release %s decision: %w", decision, err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	token, err := controlstore.LockInstance(ctx, tx, instanceID)
	if err != nil {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		if errors.Is(err, controlstore.ErrInstanceNotFound) {
			refusal.class = RefusalInstanceMismatch
			refusal.reason = fmt.Sprintf(
				"recovery instance %s does not exist; a release binds to an open recovery instance", instanceID)
			return refusedRelease(ctx, store, outcome, refusal)
		}
		return outcome, fmt.Errorf("lock recovery instance for release: %w", err)
	}
	refusal.instanceID = token.InstanceID
	if token.State != "open" || token.Kind != "recovery" {
		refusal.class = RefusalInstanceMismatch
		refusal.reason = fmt.Sprintf(
			"instance %s is state=%s kind=%s; release decisions bind to an open recovery instance",
			token.InstanceID, token.State, token.Kind)
		return refusedReleaseLocked(ctx, tx, outcome, refusal)
	}

	// release/revoke is the recovery_execute act of this instance: the
	// authenticated principal must be the executor recorded at open time or a
	// participant bound with the executor role (approval-matrix §1).
	ids := newGateIdentities(ctx, tx, token.InstanceID, token.OpenedBy)
	if err := ids.ensureParticipants(); err != nil {
		return outcome, fmt.Errorf("read the participants of instance %s: %w", token.InstanceID, err)
	}
	if !ids.executorPrincipals[principal] {
		refusal.class = RefusalApprovalMissing
		refusal.reason = fmt.Sprintf(
			"principal %s holds no recovery_execute binding on instance %s (opened_by or executor role)", principal, token.InstanceID)
		return refusedReleaseLocked(ctx, tx, outcome, refusal)
	}

	row := controlstore.ReleaseDecisionRequest{
		InstanceID:         token.InstanceID,
		Capability:         string(req.Capability),
		ScopeHash:          scope,
		Decision:           decision,
		EvidenceGeneration: token.EvidenceGeneration,
		EvidenceHash:       token.EvidenceHash,
		Actor:              principal,
		Reason:             reason,
		OperationID:        operationID,
	}
	if decision == releaseDecisionRelease {
		refs, basisRefusal := releaseApprovalBasisLocked(ctx, store, gate, tx, token, scope, req.Capability, ids)
		if basisRefusal != nil {
			if basisRefusal.class == RefusalControlStoreUnavailable && basisRefusal.cause != nil {
				return outcome, fmt.Errorf("derive the approval basis of capability %s: %w", req.Capability, basisRefusal.cause)
			}
			refusal.class = basisRefusal.class
			refusal.reason = basisRefusal.reason
			return refusedReleaseLocked(ctx, tx, outcome, refusal)
		}
		row.ApprovalRefs = refs
	}

	result, err := controlstore.InsertReleaseDecision(ctx, tx, row)
	if errors.Is(err, controlstore.ErrOperationIDTaken) {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return replayReleaseDecision(ctx, store, row)
	}
	if err != nil {
		return outcome, fmt.Errorf("append release %s decision: %w", decision, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return outcome, fmt.Errorf("commit release %s decision: %w", decision, err)
	}
	return ReleaseOutcome{
		ReleaseID:    result.DecisionID,
		Decision:     result.Decision,
		ApprovalRefs: result.ApprovalRefs,
	}, nil
}

// releaseApprovalBasisLocked derives the approval references a release of
// (instance, capability, scope_hash) may record.
//
// The candidate set is mechanical: for every principal registered with the
// approver role on this instance, the principal's current decision of this
// stream when it is an approve (a later revoke covers an earlier approve); the
// stream read is the control store's commit-order derivation. This function
// re-implements none of the validity rules — the candidates are judged by the
// single derived judgment (Gate.approvalsValid):
//
//   - the whole current basis is judged first; when it is not acceptable its
//     refusal class is returned unchanged, so Release refuses with exactly the
//     class the gate would (approval_stale on an evidence change,
//     approval_identity_unverified on a mapping change, approval_missing on a
//     missing/short basis, approval_executor_excluded on self-approval);
//   - when the whole basis is acceptable, the minimal recorded basis is
//     selected deterministically (ascending principal order): the first
//     approval for a single capability, the first two approvals that resolve
//     to distinct people for a dual capability. The selected set is judged
//     again before it is returned, so only a basis the single judgment accepts
//     can be recorded;
//   - when the whole basis is not acceptable, a strictly smaller valid basis
//     may still exist (the §3.1 existence rule): singles and principal pairs
//     are tried in the same deterministic order; the whole-basis refusal stays
//     the reported class when no subset passes.
func releaseApprovalBasisLocked(ctx context.Context, store *controlstore.Store, gate *Gate, tx pgx.Tx, token controlstore.InstanceToken, scope string, c Capability, ids *gateIdentities) ([]string, *gateRefusal) {
	required, err := RequiredApprovalClass(c)
	if err != nil {
		return nil, &gateRefusal{class: RefusalApprovalMissing, reason: err.Error(), cause: err}
	}
	if err := ids.ensureParticipants(); err != nil {
		return nil, &gateRefusal{class: RefusalControlStoreUnavailable, reason: err.Error(), cause: err}
	}
	principals := make([]string, 0, len(ids.approvers))
	for principal := range ids.approvers {
		principals = append(principals, principal)
	}
	slices.Sort(principals)

	key := controlstore.DecisionKey{
		InstanceID: token.InstanceID,
		Capability: string(c),
		ScopeHash:  scope,
	}
	var candidates []releaseApprovalCandidate
	for _, principal := range principals {
		current, err := store.CurrentApprovalDecision(ctx, tx, key, principal)
		if err != nil {
			return nil, &gateRefusal{class: RefusalControlStoreUnavailable, reason: err.Error(), cause: err}
		}
		if current.Found && current.Decision == approvalDecisionApprove {
			candidates = append(candidates, releaseApprovalCandidate{principal: principal, ref: current.DecisionID})
		}
	}
	if len(candidates) == 0 {
		return nil, &gateRefusal{class: RefusalApprovalMissing, reason: fmt.Sprintf(
			"no approver of this instance holds a current approve decision for capability %s at this scope", c)}
	}
	if len(candidates) > maxGateApprovalRefs {
		return nil, &gateRefusal{class: RefusalApprovalMissing, reason: fmt.Sprintf(
			"this stream has %d current approve decisions, above the bound of %d; refusing instead of guessing a basis",
			len(candidates), maxGateApprovalRefs)}
	}

	ev := &gateEvaluation{ids: ids}
	all := make([]string, 0, len(candidates))
	for _, cand := range candidates {
		all = append(all, cand.ref)
	}
	whole := gate.approvalsValid(ctx, tx, token, scope, c, controlstore.DecisionState{ApprovalRefs: all}, ev)
	if whole == nil {
		selected, refusal := selectReleaseApprovalRefs(ctx, gate, tx, token, scope, c, ids, candidates, required, ev)
		if refusal != nil {
			return nil, refusal
		}
		return dedupeApprovalRefs(selected), nil
	}

	if required == ApprovalClassDualNonExecutor {
		for i := 0; i < len(candidates); i++ {
			for j := i + 1; j < len(candidates); j++ {
				pair := []string{candidates[i].ref, candidates[j].ref}
				if gate.approvalsValid(ctx, tx, token, scope, c, controlstore.DecisionState{ApprovalRefs: pair}, ev) == nil {
					return dedupeApprovalRefs(pair), nil
				}
			}
		}
	} else {
		for _, cand := range candidates {
			if gate.approvalsValid(ctx, tx, token, scope, c, controlstore.DecisionState{ApprovalRefs: []string{cand.ref}}, ev) == nil {
				return []string{cand.ref}, nil
			}
		}
	}
	return nil, whole
}

// releaseApprovalCandidate is one principal's current approve decision of the
// released stream: the mechanical input of the basis selection.
type releaseApprovalCandidate struct {
	principal string
	ref       string
}

// selectReleaseApprovalRefs picks the minimal deterministic basis out of a
// candidate set the whole-basis judgment already accepted: the first approval
// for a single capability, the first two approvals resolving to distinct
// people for a dual capability (ascending principal order, using the same
// identity resolver the judgment read). The selected set is judged again
// before it is returned, so only a basis the single judgment accepts is
// recorded.
func selectReleaseApprovalRefs(ctx context.Context, gate *Gate, tx pgx.Tx, token controlstore.InstanceToken, scope string, c Capability, ids *gateIdentities, candidates []releaseApprovalCandidate, required ApprovalClass, ev *gateEvaluation) ([]string, *gateRefusal) {
	if required != ApprovalClassDualNonExecutor {
		refs := []string{candidates[0].ref}
		if refusal := gate.approvalsValid(ctx, tx, token, scope, c, controlstore.DecisionState{ApprovalRefs: refs}, ev); refusal != nil {
			return nil, refusal
		}
		return refs, nil
	}

	selected := make([]string, 0, 2)
	firstPerson := ""
	for _, cand := range candidates {
		person, active, err := ids.mappingFor(cand.principal)
		if err != nil {
			return nil, &gateRefusal{class: RefusalControlStoreUnavailable, reason: err.Error(), cause: err}
		}
		if !active {
			// Unreachable after the whole-basis judgment accepted every
			// candidate; skip rather than trust an inconsistent read.
			continue
		}
		if len(selected) == 0 {
			firstPerson = person
			selected = append(selected, cand.ref)
			continue
		}
		if person != firstPerson {
			selected = append(selected, cand.ref)
			break
		}
	}
	if len(selected) != 2 {
		return nil, &gateRefusal{class: RefusalApprovalIdentityUnverified, reason: fmt.Sprintf(
			"capability %s requires dual approval by two different people; the current approve decisions resolve to fewer than two distinct people", c)}
	}
	if refusal := gate.approvalsValid(ctx, tx, token, scope, c, controlstore.DecisionState{ApprovalRefs: selected}, ev); refusal != nil {
		return nil, refusal
	}
	return selected, nil
}

// replayReleaseDecision resolves the operation_id idempotency read-back: the
// recorded row is returned when it matches the canonical input of this call
// (Recorded=true, zero writes); a reused operation_id with a different input
// refuses with controlstore.ErrOperationConflict and zero writes. The
// read-back compares the full carriage, including the deterministic approval
// refs and the evidence token, so a replay can never smuggle in a decision
// recorded for a different capability, scope or generation.
func replayReleaseDecision(ctx context.Context, store *controlstore.Store, req controlstore.ReleaseDecisionRequest) (ReleaseOutcome, error) {
	outcome := ReleaseOutcome{Decision: req.Decision}
	recorded, found, err := store.ReleaseDecisionByOperationID(ctx, nil, req.OperationID)
	if err != nil {
		return outcome, fmt.Errorf("read the recorded release decision for operation_id %q: %w", req.OperationID, err)
	}
	if !found {
		return outcome, fmt.Errorf(
			"operation_id %q is already recorded but its release decision row cannot be read back; refusing", req.OperationID)
	}
	if !releaseDecisionMatches(recorded, req) {
		return outcome, fmt.Errorf("%w: operation_id %q", controlstore.ErrOperationConflict, req.OperationID)
	}
	return ReleaseOutcome{
		ReleaseID:    recorded.DecisionID,
		Decision:     recorded.Decision,
		ApprovalRefs: recorded.ApprovalRefs,
		Recorded:     true,
	}, nil
}

// releaseDecisionMatches reports whether a recorded decision row is the exact
// input of a replay: every canonical field of the request, including the
// deterministic approval refs, the evidence token and the audit annotation.
func releaseDecisionMatches(recorded controlstore.DecisionResult, req controlstore.ReleaseDecisionRequest) bool {
	return recorded.InstanceID == req.InstanceID &&
		recorded.Capability == req.Capability &&
		recorded.ScopeHash == req.ScopeHash &&
		recorded.Decision == req.Decision &&
		recorded.EvidenceGeneration == req.EvidenceGeneration &&
		recorded.EvidenceHash == req.EvidenceHash &&
		slices.Equal(recorded.ApprovalRefs, req.ApprovalRefs) &&
		recorded.SupersedesID == req.SupersedesReleaseID &&
		recorded.OperationID == req.OperationID &&
		recorded.Reason == req.Reason
}

// releaseRefusal is one internal refusal of the release path: the closed
// class, the human reason and the audit carriage of the refused request.
// instanceID is empty when the audit cannot reference an instance row (the
// requested instance does not exist).
type releaseRefusal struct {
	instanceID        string
	requestedInstance string
	principal         string
	capability        Capability
	scope             string
	decision          string
	operationID       string
	class             RefusalClass
	reason            string
}

// actor renders the audit actor of a refusal: the authenticated principal
// when its form is valid, otherwise the non-authorizing fallback label.
func (r releaseRefusal) actor() string {
	if principal, err := controlstore.NormalizePrincipal(r.principal); err == nil {
		return principal
	}
	return releaseFallbackActor
}

// refusedRelease records the refusal of a request refused before the instance
// lock was taken (unknown instance or a free-form principal): the audit row is
// written standalone (the requested instance stays visible in the detail) and
// the refusal carries ErrReleaseRefused.
func refusedRelease(ctx context.Context, store *controlstore.Store, out ReleaseOutcome, r releaseRefusal) (ReleaseOutcome, error) {
	out.RefusalClass = r.class
	out.Reason = r.reason
	if err := writeReleaseRefusalAudit(ctx, store.Pool(), r); err != nil {
		out.Reason = r.reason + fmt.Sprintf(" (refusal audit failed: %v)", err)
	}
	return out, fmt.Errorf("%w: %s", ErrReleaseRefused, out.Reason)
}

// refusedReleaseLocked records the refusal of a request refused under the
// instance row lock: the audit row and the otherwise empty transaction are
// committed together, so the refusal is atomic with the in-lock derivation
// and the lock is released before returning.
func refusedReleaseLocked(ctx context.Context, tx pgx.Tx, out ReleaseOutcome, r releaseRefusal) (ReleaseOutcome, error) {
	out.RefusalClass = r.class
	out.Reason = r.reason
	if err := writeReleaseRefusalAudit(ctx, tx, r); err != nil {
		out.Reason = r.reason + fmt.Sprintf(" (refusal audit failed: %v)", err)
		return out, fmt.Errorf("%w: %s", ErrReleaseRefused, out.Reason)
	}
	if err := tx.Commit(ctx); err != nil {
		out.Reason = r.reason + fmt.Sprintf(" (refusal audit commit failed: %v)", err)
		return out, fmt.Errorf("%w: %s", ErrReleaseRefused, out.Reason)
	}
	return out, fmt.Errorf("%w: %s", ErrReleaseRefused, r.reason)
}

// writeReleaseRefusalAudit appends the refusal row of one release/revoke
// decision (result=refused, action=release_decision) under the request's
// operation_id. It carries no authorization input: the operation_id is the
// idempotency annotation and the reason is an audit annotation only.
func writeReleaseRefusalAudit(ctx context.Context, q controlstore.Queryer, r releaseRefusal) error {
	target := map[string]any{
		"capability": string(r.capability),
		"scope_hash": r.scope,
		"principal":  r.principal,
		"decision":   r.decision,
	}
	if r.instanceID != "" {
		target["instance_id"] = r.instanceID
	}
	detail := map[string]any{
		"reason":                r.reason,
		"operation_id":          r.operationID,
		"requested_instance_id": r.requestedInstance,
	}
	encodedTarget, err := json.Marshal(target)
	if err != nil {
		return fmt.Errorf("encode release refusal target: %w", err)
	}
	encodedDetail, err := json.Marshal(detail)
	if err != nil {
		return fmt.Errorf("encode release refusal detail: %w", err)
	}
	return controlstore.WriteAudit(ctx, q, controlstore.AuditRecord{
		InstanceID:   r.instanceID,
		Actor:        r.actor(),
		Action:       controlstore.ActionReleaseDecision,
		Target:       encodedTarget,
		Detail:       encodedDetail,
		Result:       controlstore.AuditRefused,
		RefusalClass: string(r.class),
		OperationID:  r.operationID,
	})
}
