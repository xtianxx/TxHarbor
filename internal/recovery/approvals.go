// approvals.go implements T048 [US4]: the approval decision write path of
// FR-023/FR-024 — the only way an approve/revoke decision enters the
// append-only recovery_approval stream (data-model.md §1.8/§3.1/§4.2/§4.3;
// contracts/approval-matrix.md §2–§5; tasks.md T048).
//
// What validity means here (all derived, never stored):
//
//   - An approval row is a decision record, not a permission: no writable
//     validity boolean exists on it (INV-2). Whether a set of approvals
//     satisfies the conservative class of a capability is re-derived on every
//     evaluation by the single judgment already in this package (gate.go
//     approvalsValid, data-model §3.1) — this file never re-implements that
//     derivation and never returns an "approved" verdict that could be read
//     as a release (F16: approved is not a state).
//   - The write path records the facts the derivation consumes, all resolved
//     inside the instance row lock (the same lock every decision writer
//     takes): the principal's person_id from the current active
//     recovery_identity mapping (a caller can never supply a person), the
//     conservative class snapshot of RequiredApprovalClassForScope with no
//     deployment ruling (a caller can never choose or force a class; the
//     scope-ruled narrowing is applied only at the derived evaluation), the
//     canonical capability scope of ParseCapabilityScope (a legacy opaque
//     scope refuses as scope_mismatch), and the authoritative
//     (evidence_generation, evidence_hash) token read under that lock (any
//     later accepted evidence write immediately invalidates the row).
//   - Execution, verification and approval are independent permissions, but
//     the instance executor (opened_by plus every executor-role participant,
//     directly or through the current mapping — the same derivation the gate
//     uses) can never record an approval decision on its own instance. A
//     principal without an active mapping cannot prove a person and refuses;
//     a principal not registered with the approver role on this instance
//     refuses; the same person under two principals is still one person and
//     counts as one approver, never two.
//   - Hard gates (missing evidence, unverified isolation, unknown payment
//     result, open/escalated gap, an active existing fund gate) are never
//     covered by an approval: this path records decisions only and the gate
//     re-derives every hard condition on each evaluation, so an approval can
//     never turn a hard refusal into an allow (approval-matrix §3 item 6).
//
// Refusals (missing/revoked mapping, principal not registered, wrong role,
// executor self-approval, missing/closed/unknown instance, free-form
// principal) carry a closed-set refusal class (data-model §3.3) and are
// audited; an infrastructure failure returns an error and writes no decision
// row. There is no emergency/force/override entry and no bypass over the
// existing fund gates.
//
// Append-only and idempotent: every accepted call appends exactly one
// recovery_approval row plus its paired ok audit row through the version-
// guarded control store (controlstore.InsertApprovalDecision). operation_id
// is the idempotency key: the same input reads the recorded row back with
// zero writes (Recorded=true); a different input under the same operation_id
// refuses with controlstore.ErrOperationConflict and zero writes. A revoke
// covers only its own principal's earlier approve rows of the same
// (instance, capability, scope_hash) stream (data-model §1.8).
//
// The control store is reached only through a *controlstore.Store built by
// controlstore.NewStore, so the T069 schema-version guard (unknown or
// incompatible version => control_store_unavailable) is inherited and there
// is no unguarded approval write path.
package recovery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

// The approval decisions of recovery_approval.decision (closed set).
const (
	approvalDecisionApprove = "approve"
	approvalDecisionRevoke  = "revoke"

	// approvalFallbackActor is the audit actor of a refusal whose subject is
	// not in the authenticated <kind>:<id> form (for example an empty
	// principal from an unconfigured deployment). It is an audit label only
	// and never authorizes anything.
	approvalFallbackActor = "system:recovery-approval"

	// maxApprovalReasonBytes bounds the free-text audit annotation. The
	// reason is never an authorization input.
	maxApprovalReasonBytes = 512
)

var (
	// ErrApprovalRequest marks a malformed approval request (unknown
	// capability, non-UUID instance, missing scope or operation_id) or a
	// missing control-store handle. It is a caller contract error, never an
	// authorization refusal.
	ErrApprovalRequest = errors.New("recovery approval request is invalid")

	// ErrApprovalRefused marks a refused approve/revoke decision: the caller
	// could not be authorized as a non-executor approver with a proven person
	// on this instance, or the instance is not an open recovery instance. The
	// refusal carries a closed-set class, is audited, and writes zero decision
	// rows.
	ErrApprovalRefused = errors.New("recovery approval refused")
)

// ApprovalRequest is one append-only approve/revoke decision request for one
// (instance, capability, scope_hash) stream. The carriage deliberately
// carries no person_id and no approval class: a caller can never supply a
// person or force a class. Person identity is resolved from the current
// active mapping and the class snapshot from the conservative scope
// classification (RequiredApprovalClassForScope, T050) inside the write
// transaction. Reason is an audit annotation only; OperationID is the
// persistent idempotency key.
//
// ScopeHash must be a canonical capability scope of Capability
// (ParseCapabilityScope): an equivalent representation is canonicalized, while
// a legacy opaque string, a non-canonical expression or a scope naming another
// capability refuses as scope_mismatch with zero writes.
//
// The recorded class snapshot is always the compiled conservative class (the
// deployment effect-class ruling is not a request input): a ruling may narrow
// the required class of a scope only where release validity is derived (gate
// and release basis), and a conservative dual snapshot satisfies a narrowed
// single requirement — never the other way around.
type ApprovalRequest struct {
	InstanceID  string
	Capability  Capability
	ScopeHash   string
	Principal   string
	Reason      string
	OperationID string
}

// ApprovalOutcome is the result of one approve/revoke decision call. On
// success Decision is approve|revoke, ApprovalID names the appended row and
// PersonID is the person resolved from the active mapping.
//
// On a refusal, Decision repeats the request, RefusalClass is one member of
// the closed gate refusal set and Reason explains it; the refusal was audited
// and no decision row was written. Recorded is true when the call replayed an
// already-recorded decision for the same operation_id and input (zero
// writes); ApprovalID/PersonID then identify the recorded row.
type ApprovalOutcome struct {
	ApprovalID    string
	Decision      string
	ApprovalClass ApprovalClass
	Principal     string
	PersonID      string
	Recorded      bool
	RefusalClass  RefusalClass
	Reason        string
}

// Approve records one explicit approve decision for (instance, capability,
// scope_hash) by the authenticated principal. The decision is appended only
// when the caller proves a non-executor approver identity on the open
// recovery instance; the recorded row carries the resolved person, the
// conservative class snapshot and the current evidence token. Approving is
// not releasing: release validity is re-derived by the gate on every
// evaluation, and a hard-gate condition is never covered by an approval.
//
// A refusal wraps ErrApprovalRefused (its class is in ApprovalOutcome), is
// audited, and writes no decision row. A malformed request wraps
// ErrApprovalRequest instead.
func Approve(ctx context.Context, store *controlstore.Store, req ApprovalRequest) (ApprovalOutcome, error) {
	return decideApproval(ctx, store, req, approvalDecisionApprove)
}

// RevokeApproval records one explicit revoke decision. It is authorized,
// audited and idempotent exactly like Approve (same role/mapping/executor
// checks), and it covers only the caller principal's own earlier approve rows
// of the same (instance, capability, scope_hash) stream — another approver's
// decision is never affected (data-model §1.8; approval-matrix §4).
func RevokeApproval(ctx context.Context, store *controlstore.Store, req ApprovalRequest) (ApprovalOutcome, error) {
	return decideApproval(ctx, store, req, approvalDecisionRevoke)
}

// decideApproval runs one approve/revoke decision: request-shape validation,
// then the authorization derivation and the append inside the instance row
// lock. The whole derivation runs under the lock the gate and every decision
// writer share, so a concurrent mapping change, participant change or
// evidence write is either seen here or invalidates the row afterwards —
// there is no window in which an unauthorized row can be recorded.
func decideApproval(ctx context.Context, store *controlstore.Store, req ApprovalRequest, decision string) (ApprovalOutcome, error) {
	if err := controlstore.ValidateCredentialText("reason", req.Reason); err != nil {
		return ApprovalOutcome{Decision: decision}, fmt.Errorf("%w: %v", ErrApprovalRequest, err)
	}
	if store == nil {
		return ApprovalOutcome{Decision: decision},
			fmt.Errorf("%w: a controlstore.Store built by controlstore.NewStore is required", ErrApprovalRequest)
	}
	instanceID, err := lifecycleInstanceID(req.InstanceID)
	if err != nil {
		return ApprovalOutcome{Decision: decision}, fmt.Errorf("%w: %v", ErrApprovalRequest, err)
	}
	if !req.Capability.Known() {
		return ApprovalOutcome{Decision: decision}, fmt.Errorf(
			"%w: capability %q is outside the closed 7-capability set", ErrApprovalRequest, req.Capability)
	}
	scope := strings.TrimSpace(req.ScopeHash)
	if scope == "" {
		return ApprovalOutcome{Decision: decision}, fmt.Errorf(
			"%w: scope_hash is required; an approval never substitutes a default scope", ErrApprovalRequest)
	}
	operationID, err := approvalOperationID(req.OperationID)
	if err != nil {
		return ApprovalOutcome{Decision: decision}, fmt.Errorf("%w: %v", ErrApprovalRequest, err)
	}

	reason := boundedApprovalReason(req.Reason)
	principalText := strings.TrimSpace(req.Principal)
	outcome := ApprovalOutcome{Decision: decision, Principal: principalText}
	refusal := approvalRefusal{
		requestedInstance: instanceID,
		principal:         principalText,
		capability:        req.Capability,
		scope:             scope,
		decision:          decision,
		operationID:       operationID,
	}

	// T050: the approval binds to a canonical capability scope of the requested
	// capability. An equivalent representation canonicalizes; a legacy opaque
	// string, a non-canonical expression or a scope naming another capability
	// refuses as scope_mismatch and writes zero decision rows.
	requestedScope, err := ParseCapabilityScope(scope, req.Capability)
	if err != nil {
		refusal.class = RefusalScopeMismatch
		refusal.reason = err.Error()
		return refusedApproval(ctx, store, outcome, refusal)
	}
	canonicalScope, err := requestedScope.Canonical()
	if err != nil {
		refusal.class = RefusalScopeMismatch
		refusal.reason = err.Error()
		return refusedApproval(ctx, store, outcome, refusal)
	}
	scope = canonicalScope
	refusal.scope = canonicalScope

	// The subject must be an authenticated <kind>:<id> principal: free-form
	// text can never stand in for authentication (approval-matrix §2).
	principal, err := controlstore.NormalizePrincipal(principalText)
	if err != nil {
		refusal.class = RefusalApprovalIdentityUnverified
		refusal.reason = err.Error()
		return refusedApproval(ctx, store, outcome, refusal)
	}
	outcome.Principal = principal
	refusal.principal = principal

	tx, err := store.Pool().Begin(ctx)
	if err != nil {
		return outcome, fmt.Errorf("begin approval %s decision: %w", decision, err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	token, err := controlstore.LockInstance(ctx, tx, instanceID)
	if err != nil {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		if errors.Is(err, controlstore.ErrInstanceNotFound) {
			refusal.class = RefusalInstanceMismatch
			refusal.reason = fmt.Sprintf(
				"recovery instance %s does not exist; an approval binds to an open recovery instance", instanceID)
			return refusedApproval(ctx, store, outcome, refusal)
		}
		return outcome, fmt.Errorf("lock recovery instance for approval: %w", err)
	}
	refusal.instanceID = token.InstanceID
	if token.State != "open" || token.Kind != "recovery" {
		refusal.class = RefusalInstanceMismatch
		refusal.reason = fmt.Sprintf(
			"instance %s is state=%s kind=%s; approval decisions bind to an open recovery instance",
			token.InstanceID, token.State, token.Kind)
		return refusedApprovalLocked(ctx, tx, outcome, refusal)
	}

	// Identity facts are read inside the same transaction and lock as the
	// append: the mapping, the approver binding and the executor exclusion are
	// the ones that hold at commit time (F19).
	ids := newGateIdentities(ctx, tx, token.InstanceID, token.OpenedBy)
	person, active, err := ids.mappingFor(principal)
	if err != nil {
		return outcome, fmt.Errorf("resolve the identity mapping of %s: %w", principal, err)
	}
	if !active {
		refusal.class = RefusalApprovalIdentityUnverified
		refusal.reason = fmt.Sprintf(
			"principal %s has no active identity mapping; a principal without a mapping cannot prove a person", principal)
		return refusedApprovalLocked(ctx, tx, outcome, refusal)
	}
	if err := ids.ensureParticipants(); err != nil {
		return outcome, fmt.Errorf("read the participants of instance %s: %w", token.InstanceID, err)
	}
	if !ids.approvers[principal] {
		refusal.class = RefusalApprovalMissing
		refusal.reason = fmt.Sprintf(
			"principal %s is not registered with the approver role on this instance", principal)
		return refusedApprovalLocked(ctx, tx, outcome, refusal)
	}
	executor, err := ids.isExecutorIdentity(principal, person)
	if err != nil {
		return outcome, fmt.Errorf("resolve the executor identity of %s: %w", principal, err)
	}
	if executor {
		refusal.class = RefusalApprovalExecutorExcluded
		refusal.reason = fmt.Sprintf(
			"principal %s resolves to this instance's executor person; the executor can never approve its own instance", principal)
		return refusedApprovalLocked(ctx, tx, outcome, refusal)
	}

	// The snapshot is the compiled conservative class: no deployment ruling is
	// a request input, and a conservative dual snapshot can satisfy a
	// scope-ruled single requirement at the derived evaluation but never the
	// reverse (gate.approvalsValid re-derives the required class from the
	// scope and the trusted ruling).
	class, err := RequiredApprovalClassForScope(requestedScope, ScopeEffectClass(requestedScope), nil)
	if err != nil {
		// Unreachable: the capability and scope were validated above.
		return outcome, fmt.Errorf("required approval class of %s: %w", req.Capability, err)
	}
	row := controlstore.ApprovalDecisionRequest{
		InstanceID:            token.InstanceID,
		Capability:            string(req.Capability),
		ScopeHash:             scope,
		Decision:              decision,
		ApprovalClassSnapshot: string(class),
		Principal:             principal,
		PersonID:              person,
		EvidenceGeneration:    token.EvidenceGeneration,
		EvidenceHash:          token.EvidenceHash,
		Reason:                reason,
		OperationID:           operationID,
	}

	result, err := controlstore.InsertApprovalDecision(ctx, tx, row)
	if errors.Is(err, controlstore.ErrOperationIDTaken) {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return replayApprovalDecision(ctx, store, row)
	}
	if err != nil {
		return outcome, fmt.Errorf("append approval %s decision: %w", decision, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return outcome, fmt.Errorf("commit approval %s decision: %w", decision, err)
	}
	return ApprovalOutcome{
		ApprovalID:    result.DecisionID,
		Decision:      result.Decision,
		ApprovalClass: ApprovalClass(result.ApprovalClass),
		Principal:     result.Principal,
		PersonID:      result.PersonID,
	}, nil
}

// replayApprovalDecision resolves the operation_id idempotency read-back: the
// recorded row is returned when it matches the canonical input of this call
// (Recorded=true, zero writes); a reused operation_id with a different input
// refuses with controlstore.ErrOperationConflict and zero writes. The
// read-back compares the full carriage, including the resolved person, the
// class snapshot and the evidence token, so a replay can never smuggle in a
// decision recorded for a different principal, person or generation.
func replayApprovalDecision(ctx context.Context, store *controlstore.Store, req controlstore.ApprovalDecisionRequest) (ApprovalOutcome, error) {
	outcome := ApprovalOutcome{Decision: req.Decision, Principal: req.Principal}
	recorded, found, err := store.ApprovalDecisionByOperationID(ctx, nil, req.OperationID)
	if err != nil {
		return outcome, fmt.Errorf("read the recorded approval decision for operation_id %q: %w", req.OperationID, err)
	}
	if !found {
		return outcome, fmt.Errorf(
			"operation_id %q is already recorded but its approval decision row cannot be read back; refusing", req.OperationID)
	}
	if !approvalDecisionMatches(recorded, req) {
		return outcome, fmt.Errorf("%w: operation_id %q", controlstore.ErrOperationConflict, req.OperationID)
	}
	return ApprovalOutcome{
		ApprovalID:    recorded.DecisionID,
		Decision:      recorded.Decision,
		ApprovalClass: ApprovalClass(recorded.ApprovalClass),
		Principal:     recorded.Principal,
		PersonID:      recorded.PersonID,
		Recorded:      true,
	}, nil
}

// approvalDecisionMatches reports whether a recorded decision row is the
// exact input of a replay: every canonical field of the request, including
// the resolved person, the class snapshot and the evidence token.
func approvalDecisionMatches(recorded controlstore.DecisionResult, req controlstore.ApprovalDecisionRequest) bool {
	return recorded.InstanceID == req.InstanceID &&
		recorded.Capability == req.Capability &&
		recorded.ScopeHash == req.ScopeHash &&
		recorded.Decision == req.Decision &&
		recorded.ApprovalClass == req.ApprovalClassSnapshot &&
		recorded.Principal == req.Principal &&
		recorded.PersonID == req.PersonID &&
		recorded.EvidenceGeneration == req.EvidenceGeneration &&
		recorded.EvidenceHash == req.EvidenceHash &&
		recorded.SupersedesID == req.SupersedesApprovalID &&
		recorded.OperationID == req.OperationID &&
		recorded.Reason == req.Reason
}

// approvalRefusal is one internal authorization refusal of the approval path:
// the closed class, the human reason and the audit carriage of the refused
// request. instanceID is empty when the audit cannot reference an instance
// row (the requested instance does not exist).
type approvalRefusal struct {
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
func (r approvalRefusal) actor() string {
	if principal, err := controlstore.NormalizePrincipal(r.principal); err == nil {
		return principal
	}
	return approvalFallbackActor
}

// refusedApproval records the refusal of a request that was refused before
// the instance lock was taken (unknown instance or a free-form principal): the
// audit row is written standalone (the requested instance stays visible in the
// detail) and the refusal carries ErrApprovalRefused.
func refusedApproval(ctx context.Context, store *controlstore.Store, out ApprovalOutcome, r approvalRefusal) (ApprovalOutcome, error) {
	out.RefusalClass = r.class
	out.Reason = r.reason
	if err := writeApprovalRefusalAudit(ctx, store.Pool(), r); err != nil {
		out.Reason = r.reason + fmt.Sprintf(" (refusal audit failed: %v)", err)
	}
	return out, fmt.Errorf("%w: %s", ErrApprovalRefused, out.Reason)
}

// refusedApprovalLocked records the refusal of a request refused under the
// instance row lock: the audit row and the otherwise empty transaction are
// committed together, so the refusal is atomic with the in-lock derivation
// and the lock is released before returning.
func refusedApprovalLocked(ctx context.Context, tx pgx.Tx, out ApprovalOutcome, r approvalRefusal) (ApprovalOutcome, error) {
	out.RefusalClass = r.class
	out.Reason = r.reason
	if err := writeApprovalRefusalAudit(ctx, tx, r); err != nil {
		out.Reason = r.reason + fmt.Sprintf(" (refusal audit failed: %v)", err)
		return out, fmt.Errorf("%w: %s", ErrApprovalRefused, out.Reason)
	}
	if err := tx.Commit(ctx); err != nil {
		out.Reason = r.reason + fmt.Sprintf(" (refusal audit commit failed: %v)", err)
		return out, fmt.Errorf("%w: %s", ErrApprovalRefused, out.Reason)
	}
	return out, fmt.Errorf("%w: %s", ErrApprovalRefused, r.reason)
}

// writeApprovalRefusalAudit appends the refusal row of one approval decision
// (result=refused, action=approval_decision) under the request's
// operation_id. It carries no authorization input: the operation_id is the
// idempotency annotation and the reason is an audit annotation only.
func writeApprovalRefusalAudit(ctx context.Context, q controlstore.Queryer, r approvalRefusal) error {
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
		return fmt.Errorf("encode approval refusal target: %w", err)
	}
	encodedDetail, err := json.Marshal(detail)
	if err != nil {
		return fmt.Errorf("encode approval refusal detail: %w", err)
	}
	return controlstore.WriteAudit(ctx, q, controlstore.AuditRecord{
		InstanceID:   r.instanceID,
		Actor:        r.actor(),
		Action:       controlstore.ActionApprovalDecision,
		Target:       encodedTarget,
		Detail:       encodedDetail,
		Result:       controlstore.AuditRefused,
		RefusalClass: string(r.class),
		OperationID:  r.operationID,
	})
}

// approvalOperationID canonicalizes the operation_id carriage: a bounded,
// control-free identity used for persistent idempotency and audit. It is
// never an authorization input.
func approvalOperationID(raw string) (string, error) {
	operation := strings.TrimSpace(raw)
	if operation == "" {
		return "", errors.New("operation_id is required")
	}
	if len(operation) > 128 || strings.ContainsAny(operation, "\x00\n\r\t") {
		return "", errors.New("operation_id must be 1..128 characters without control characters")
	}
	return operation, nil
}

// boundedApprovalReason bounds a free-text audit annotation to the 512-byte
// bound, keeping the truncation visible. The reason is never an authorization
// input.
func boundedApprovalReason(raw string) string {
	reason := strings.TrimSpace(raw)
	if len(reason) <= maxApprovalReasonBytes {
		return reason
	}
	return reason[:maxApprovalReasonBytes-8] + " [bound]"
}
