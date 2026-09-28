// instance.go implements T027: the recovery-instance lifecycle of
// data-model.md §4.1 — `instance-open`, `instance-close` and the explicit
// `supersede` path — on top of the version-guarded control store (T007) and
// the single derived release evaluation (T012).
//
// Rules of this file (all fail-closed):
//
//   - instance-open records the executor (opened_by; the gate resolves the
//     executor from opened_by plus the executor-role participants), relies on
//     the control store's partial unique index for the globally unique open
//     instance (INV-1), and never modifies or reuses an instance id: the
//     control store generates a fresh UUID per open and its CLI path is
//     idempotent through operation_id.
//   - `baseline` is the documentation-only deployment state. The production
//     gate arms `recovery` instances only (gate.go admitBound), so a baseline
//     instance changes no admission; instance-close still records the closure.
//   - instance-close is allowed only after every one of the seven capabilities
//     is currently release-valid. The check re-runs the single derived
//     evaluation (gate.evaluateLocked) inside the instance row lock — the same
//     lock every decision writer takes — over every scope hash that ever
//     carried a release decision for the capability, so a release that was
//     revoked or invalidated by a generation change refuses instead of being
//     read as "released". No release-validity logic is re-implemented here.
//   - If any release row exists for a funds/delivery capability
//     (existing_withdrawal_recovery, new_withdrawal_creation, event_publishing,
//     event_consuming — the conservative dual class of RequiredApprovalClass),
//     close additionally requires the current release of every such capability
//     to resolve to two distinct non-executor people; the explicit check keeps
//     the T027 dual condition reviewable independent of the conservative
//     approval-class table (T050/T049 may tighten; they must not silently
//     relax it).
//   - An open evidence gap keeps the instance open: close is refused with
//     refusal_class gap_open, the result marks the escalation and the audit
//     records risk_acceptance_delivered=false. The instance is never closed
//     over an unclosed capability and no risk acceptance is delivered
//     (data-model §4.1; spec clarification 3).
//   - supersede is explicit only: it requires the superseded instance id, two
//     current two-person approvals on that instance, closes the old instance
//     and opens a new instance (new UUID, supersedes_instance_id set) in one
//     transaction, and writes both audit rows. A self-written supersede row
//     is not this path and remains meaningless (T025/DG-4).
//   - Every refusal above is audited (result=refused) and every accepted
//     lifecycle write is audited (result=ok); no path writes a "released"
//     boolean anywhere.
package recovery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

// The recovery_audit action names of the lifecycle writes (the CLI command
// surface records the same names under its own operation_id).
const (
	// ActionInstanceClose is the audit action of an accepted/refused
	// instance-close attempt.
	ActionInstanceClose = "instance_close"
	// ActionInstanceSupersede is the audit action of an accepted/refused
	// explicit supersede. The superseded instance carries it; the new instance
	// also receives the regular open audit under controlstore.ActionInstanceOpen.
	ActionInstanceSupersede = "instance_supersede"
)

var (
	// ErrInstanceOpenInput marks a malformed instance-open request. It is a
	// caller contract error, never a gate refusal.
	ErrInstanceOpenInput = errors.New("recovery instance open request is invalid")
	// ErrInstanceCloseBlocked marks a close refused because at least one
	// capability is not currently release-valid (or an open gap blocks one).
	// The instance stays open.
	ErrInstanceCloseBlocked = errors.New("recovery instance close is blocked")
	// ErrInstanceSupersedeInput marks a malformed supersede request.
	ErrInstanceSupersedeInput = errors.New("recovery instance supersede request is invalid")
	// ErrInstanceApprovalRequired marks a lifecycle action refused because the
	// required two-person approval basis is missing or invalid.
	ErrInstanceApprovalRequired = errors.New("recovery instance action requires two-person approval")
)

// OpenInstanceRequest opens one recovery/baseline instance.
type OpenInstanceRequest struct {
	// Kind is recovery|baseline. `recovery` arms the production gate;
	// `baseline` is documentation-only deployment state.
	Kind string
	// OpenedBy is the authenticated executor principal (canonical
	// <kind>:<id> form). It becomes the instance's opened_by and therefore the
	// executor identity the gate excludes from approvals/verification.
	OpenedBy string
	// Reason is an audit annotation, never an authorization input.
	Reason string
	// RestorePoint / DataTarget are optional credential-free JSON payloads
	// (the control store validates data_target as fingerprints only).
	RestorePoint []byte
	DataTarget   []byte
}

// OpenInstanceResult carries the new instance identity.
type OpenInstanceResult struct {
	InstanceID string
	Kind       string
	OpenedBy   string
}

// OpenInstance creates one open instance through the control store. At most
// one instance may be open globally (INV-1, refused as
// controlstore.ErrInstanceAlreadyOpen); the instance id is generated by the
// control store (gen_random_uuid) and is never modified or reused by any 015
// path. The executor is recorded as opened_by; person provability (an active
// identity mapping) is enforced by the CLI binding path, not by this library
// wrapper, so library-level tests and future assembly points keep control.
func OpenInstance(ctx context.Context, store *controlstore.Store, req OpenInstanceRequest) (OpenInstanceResult, error) {
	if store == nil {
		return OpenInstanceResult{}, fmt.Errorf("%w: a control-store handle is required", ErrInstanceOpenInput)
	}
	kind := strings.TrimSpace(req.Kind)
	switch kind {
	case "recovery", "baseline":
	default:
		return OpenInstanceResult{}, fmt.Errorf("%w: kind must be recovery or baseline, got %q", ErrInstanceOpenInput, req.Kind)
	}
	openedBy, err := controlstore.NormalizePrincipal(req.OpenedBy)
	if err != nil {
		return OpenInstanceResult{}, fmt.Errorf("%w: opened_by: %v", ErrInstanceOpenInput, err)
	}
	result, err := store.OpenInstance(ctx, controlstore.OpenInstanceRequest{
		Kind:         kind,
		OpenedBy:     openedBy,
		Reason:       req.Reason,
		RestorePoint: req.RestorePoint,
		DataTarget:   req.DataTarget,
	})
	if err != nil {
		return OpenInstanceResult{}, err
	}
	return OpenInstanceResult{InstanceID: result.InstanceID, Kind: kind, OpenedBy: openedBy}, nil
}

// CloseInstanceRequest closes one open instance.
type CloseInstanceRequest struct {
	// InstanceID is the open instance to close.
	InstanceID string
	// Actor is the authenticated principal performing the closure (audit and
	// closed_by). The CLI additionally requires a participant binding.
	Actor string
	// Reason is an audit annotation, never an authorization input.
	Reason string
	// OperationID is the command idempotency key carried into the audit rows.
	OperationID string
}

// CloseInstanceResult reports one close attempt. On a blocked attempt Closed
// is false, Blocked names every capability that is not release-valid, and
// EscalationRequired marks the open-gap escalation path. RiskAcceptance is
// always false: T027 never delivers a risk acceptance over an unclosed
// capability.
type CloseInstanceResult struct {
	InstanceID              string
	Closed                  bool
	Baseline                bool
	Blocked                 []string
	EscalationRequired      bool
	RiskAcceptanceDelivered bool
}

// CloseInstance closes the instance only when all seven capabilities are
// currently release-valid. The whole guard runs inside the instance row lock
// through the real derived evaluation (gate.evaluateLocked), so no release
// decision is trusted merely because a row exists: a later revoke, a stale
// generation/hash, unverified isolation, an open gap or invalid approvals all
// refuse. The gate is required: without the single derived evaluation there is
// no close path.
func CloseInstance(ctx context.Context, store *controlstore.Store, gate *Gate, req CloseInstanceRequest) (CloseInstanceResult, error) {
	if store == nil {
		return CloseInstanceResult{}, errors.New("close recovery instance requires a control-store handle")
	}
	if gate == nil {
		return CloseInstanceResult{}, errors.New("close recovery instance requires the derived release evaluation gate")
	}
	instanceID, err := lifecycleInstanceID(req.InstanceID)
	if err != nil {
		return CloseInstanceResult{}, err
	}
	actor, err := controlstore.NormalizePrincipal(req.Actor)
	if err != nil {
		return CloseInstanceResult{}, fmt.Errorf("close recovery instance actor: %w", err)
	}

	tx, err := store.Pool().Begin(ctx)
	if err != nil {
		return CloseInstanceResult{}, fmt.Errorf("begin instance close: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	locked, err := controlstore.LockInstance(ctx, tx, instanceID)
	if err != nil {
		return CloseInstanceResult{}, err
	}
	result := CloseInstanceResult{InstanceID: locked.InstanceID}
	if locked.State != "open" {
		return result, fmt.Errorf("%w: %s", controlstore.ErrInstanceNotOpen, locked.InstanceID)
	}

	// baseline is documentation-only deployment state: it never armed the
	// production gate, so its closure needs no release guard. The closure is
	// still recorded and audited.
	if locked.Kind != "recovery" {
		if err := closeInstanceRow(ctx, tx, locked, actor); err != nil {
			return result, err
		}
		if err := writeInstanceAudit(ctx, tx, locked.InstanceID, actor, ActionInstanceClose, controlstore.AuditOK, "",
			req.OperationID, map[string]any{
				"reason":                    req.Reason,
				"kind":                      locked.Kind,
				"release_guard":             "not applicable (baseline is documentation-only state; the gate arms recovery instances)",
				"risk_acceptance_delivered": false,
			}); err != nil {
			return result, err
		}
		if err := tx.Commit(ctx); err != nil {
			return result, fmt.Errorf("commit instance close: %w", err)
		}
		result.Closed = true
		result.Baseline = true
		return result, nil
	}

	// An open evidence gap blocks at least its directly related capability
	// (schema CHECK); closing over it would carry an unclosed capability back
	// into daily operations. Refuse, keep the instance open, escalate to
	// manual handling and deliver no risk acceptance.
	var gapOpen bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM recovery_gap WHERE instance_id = $1 AND state = 'open')`,
		locked.InstanceID).Scan(&gapOpen); err != nil {
		return result, fmt.Errorf("read open evidence gaps: %w", err)
	}
	if gapOpen {
		result.EscalationRequired = true
		result.Blocked = []string{
			"an open evidence gap blocks at least one capability; the instance stays open and is escalated for manual handling (risk acceptance is not delivered)",
		}
		if err := writeInstanceAudit(ctx, tx, locked.InstanceID, actor, ActionInstanceClose, controlstore.AuditRefused,
			string(RefusalGapOpen), req.OperationID, map[string]any{
				"reason":                    req.Reason,
				"blocked":                   result.Blocked,
				"escalation_required":       true,
				"risk_acceptance_delivered": false,
			}); err != nil {
			return result, err
		}
		if err := tx.Commit(ctx); err != nil {
			return result, fmt.Errorf("commit instance close refusal: %w", err)
		}
		return result, fmt.Errorf("%w: %s", ErrInstanceCloseBlocked, result.Blocked[0])
	}

	// The full derived evaluation, inside the instance row lock, for every one
	// of the seven capabilities. Each capability may have release decisions at
	// more than one scope hash; the first scope whose release is currently
	// valid releases the capability, and only the single derived evaluation
	// decides.
	gate.invalidateCache(locked)
	type capabilityOutcome struct {
		allowed      bool
		scope        string
		releaseID    string
		refusalClass string
		reason       string
	}
	outcomes := make(map[Capability]capabilityOutcome, len(KnownCapabilities()))
	var blockers []string
	for _, capability := range KnownCapabilities() {
		scopes, err := instanceReleaseScopes(ctx, tx, locked.InstanceID, string(capability))
		if err != nil {
			return result, err
		}
		outcome := capabilityOutcome{
			refusalClass: string(RefusalNoRelease),
			reason:       "no release decision exists for this capability at any scope",
		}
		for _, scope := range scopes {
			ev := gate.evaluateLocked(ctx, tx, locked, GateRequest{
				InstanceID:  locked.InstanceID,
				Capability:  capability,
				ScopeHash:   scope,
				Actor:       actor,
				OperationID: req.OperationID,
			})
			if ev.allowed {
				outcome = capabilityOutcome{allowed: true, scope: scope, releaseID: ev.releaseID}
				break
			}
			if ev.refusal != nil {
				outcome = capabilityOutcome{
					refusalClass: string(ev.refusal.class),
					reason:       ev.refusal.reason,
				}
			}
		}
		outcomes[capability] = outcome
		if !outcome.allowed {
			blockers = append(blockers, fmt.Sprintf("capability %s is not release-valid (%s): %s",
				capability, outcome.refusalClass, outcome.reason))
		}
	}

	// T027 dual condition: when this instance ever released a funds/delivery
	// capability, close additionally requires the current release of every such
	// capability to resolve to two distinct non-executor people. The gate's
	// conservative approval classes already enforce dual for these
	// capabilities; this explicit re-derivation keeps the close condition
	// reviewable on its own and refuses if a future class table is relaxed.
	dualChecked := false
	if len(blockers) == 0 {
		needsDual := false
		for _, capability := range KnownCapabilities() {
			class, err := RequiredApprovalClass(capability)
			if err != nil {
				return result, err
			}
			if class != ApprovalClassDualNonExecutor {
				continue
			}
			var hasRelease bool
			if err := tx.QueryRow(ctx,
				`SELECT EXISTS (SELECT 1 FROM recovery_release
				 WHERE instance_id = $1 AND capability = $2 AND decision = 'release')`,
				locked.InstanceID, string(capability)).Scan(&hasRelease); err != nil {
				return result, fmt.Errorf("read release history of capability %s: %w", capability, err)
			}
			if hasRelease {
				needsDual = true
				break
			}
		}
		if needsDual {
			dualChecked = true
			ids := newGateIdentities(ctx, tx, locked.InstanceID, locked.OpenedBy)
			if err := ids.ensureParticipants(); err != nil {
				return result, err
			}
			for _, capability := range KnownCapabilities() {
				class, err := RequiredApprovalClass(capability)
				if err != nil {
					return result, err
				}
				if class != ApprovalClassDualNonExecutor {
					continue
				}
				outcome := outcomes[capability]
				if !outcome.allowed {
					continue
				}
				decision, err := store.CurrentReleaseDecision(ctx, tx, controlstore.DecisionKey{
					InstanceID: locked.InstanceID,
					Capability: string(capability),
					ScopeHash:  outcome.scope,
				})
				if err != nil {
					return result, fmt.Errorf("read current release of capability %s: %w", capability, err)
				}
				persons, err := instanceApprovalPersons(ctx, store, ids, locked, decision.ApprovalRefs)
				if err != nil {
					return result, fmt.Errorf("re-derive the approval basis of capability %s: %w", capability, err)
				}
				if len(persons) < 2 {
					reason := fmt.Sprintf(
						"capability %s was released during this instance; close requires dual two-person approval of its current release and only %d distinct non-executor person(s) are valid",
						capability, len(persons))
					outcomes[capability] = capabilityOutcome{
						refusalClass: string(RefusalApprovalMissing),
						reason:       reason,
					}
					blockers = append(blockers, reason)
				}
			}
		}
	}

	capabilityDetail := make(map[string]any, len(outcomes))
	for capability, outcome := range outcomes {
		detail := map[string]any{"allowed": outcome.allowed}
		if outcome.allowed {
			detail["scope_hash"] = outcome.scope
			detail["release_id"] = outcome.releaseID
		} else {
			detail["refusal_class"] = outcome.refusalClass
			detail["reason"] = outcome.reason
		}
		capabilityDetail[string(capability)] = detail
	}

	if len(blockers) > 0 {
		result.Blocked = blockers
		if err := writeInstanceAudit(ctx, tx, locked.InstanceID, actor, ActionInstanceClose, controlstore.AuditRefused, "",
			req.OperationID, map[string]any{
				"reason":                    req.Reason,
				"blocked":                   blockers,
				"capabilities":              capabilityDetail,
				"dual_approvals_checked":    dualChecked,
				"escalation_required":       false,
				"risk_acceptance_delivered": false,
			}); err != nil {
			return result, err
		}
		if err := tx.Commit(ctx); err != nil {
			return result, fmt.Errorf("commit instance close refusal: %w", err)
		}
		return result, fmt.Errorf("%w: %s", ErrInstanceCloseBlocked, strings.Join(blockers, "; "))
	}

	if err := closeInstanceRow(ctx, tx, locked, actor); err != nil {
		return result, err
	}
	if err := writeInstanceAudit(ctx, tx, locked.InstanceID, actor, ActionInstanceClose, controlstore.AuditOK, "",
		req.OperationID, map[string]any{
			"reason":                    req.Reason,
			"capabilities":              capabilityDetail,
			"dual_approvals_checked":    dualChecked,
			"escalation_required":       false,
			"risk_acceptance_delivered": false,
		}); err != nil {
		return result, err
	}
	if err := tx.Commit(ctx); err != nil {
		return result, fmt.Errorf("commit instance close: %w", err)
	}
	result.Closed = true
	return result, nil
}

// closeInstanceRow writes the closure facts under the held instance lock. The
// state predicate keeps a concurrent close from succeeding twice.
func closeInstanceRow(ctx context.Context, tx pgx.Tx, locked controlstore.InstanceToken, actor string) error {
	tag, err := tx.Exec(ctx, `
UPDATE recovery_instance
SET state = 'closed', closed_by = $2, closed_at = now()
WHERE instance_id = $1 AND state = 'open'`, locked.InstanceID, actor)
	if err != nil {
		return fmt.Errorf("close recovery instance %s: %w", locked.InstanceID, err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: %s changed during the close", controlstore.ErrInstanceNotOpen, locked.InstanceID)
	}
	return nil
}

// SupersedeInstanceRequest replaces the open instance explicitly. The
// request must name the instance being superseded (there is no implicit
// supersede) and carry two current approvals by two different non-executor
// people on that instance.
type SupersedeInstanceRequest struct {
	// InstanceID is the open instance being superseded; it is closed by this
	// call and never reused.
	InstanceID string
	// OpenedBy is the executor recorded on the new instance.
	OpenedBy string
	// Reason is an audit annotation for both the closure and the new open.
	Reason string
	// ApprovalRefs are the approval ids of the two-person authorization. Each
	// must be a current approve decision of this instance at the current
	// evidence generation with an active identity mapping, and at least two
	// distinct non-executor people must be covered.
	ApprovalRefs []string
	// OperationID is the command idempotency key carried into the audit rows.
	OperationID string
}

// SupersedeInstanceResult carries both instance identities.
type SupersedeInstanceResult struct {
	SupersededInstanceID string
	NewInstanceID        string
}

// SupersedeInstance closes the named open recovery instance and opens a new
// one in a single transaction: the old instance gets state=closed with the
// closure facts, the new instance gets a new control-store-generated UUID and
// supersedes_instance_id pointing at the old id. Both the closure and the
// open are audited. A row written directly into the control store is not this
// path and grants nothing (T025 negative); only this audited path is the
// supported supersede.
func SupersedeInstance(ctx context.Context, store *controlstore.Store, req SupersedeInstanceRequest) (SupersedeInstanceResult, error) {
	if store == nil {
		return SupersedeInstanceResult{}, fmt.Errorf("%w: a control-store handle is required", ErrInstanceSupersedeInput)
	}
	instanceID, err := lifecycleInstanceID(req.InstanceID)
	if err != nil {
		return SupersedeInstanceResult{}, err
	}
	openedBy, err := controlstore.NormalizePrincipal(req.OpenedBy)
	if err != nil {
		return SupersedeInstanceResult{}, fmt.Errorf("%w: opened_by: %v", ErrInstanceSupersedeInput, err)
	}
	refs := dedupeApprovalRefs(req.ApprovalRefs)
	if len(refs) < 2 {
		return SupersedeInstanceResult{}, fmt.Errorf("%w: explicit supersede requires two approval references by two distinct non-executor people", ErrInstanceApprovalRequired)
	}
	if len(refs) > maxGateApprovalRefs {
		return SupersedeInstanceResult{}, fmt.Errorf("%w: %d approval references exceed the bound of %d", ErrInstanceSupersedeInput, len(refs), maxGateApprovalRefs)
	}

	tx, err := store.Pool().Begin(ctx)
	if err != nil {
		return SupersedeInstanceResult{}, fmt.Errorf("begin instance supersede: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	locked, err := controlstore.LockInstance(ctx, tx, instanceID)
	if err != nil {
		return SupersedeInstanceResult{}, err
	}
	if locked.State != "open" {
		return SupersedeInstanceResult{}, fmt.Errorf("%w: %s", controlstore.ErrInstanceNotOpen, locked.InstanceID)
	}
	if locked.Kind != "recovery" {
		return SupersedeInstanceResult{}, fmt.Errorf("%w: supersede replaces a recovery instance, %s has kind=%s", ErrInstanceSupersedeInput, locked.InstanceID, locked.Kind)
	}

	ids := newGateIdentities(ctx, tx, locked.InstanceID, locked.OpenedBy)
	persons, err := instanceApprovalPersons(ctx, store, ids, locked, refs)
	if err != nil || len(persons) < 2 {
		reason := "two valid current approvals by two distinct non-executor people are required"
		if err != nil {
			reason = err.Error()
		} else {
			reason = fmt.Sprintf("%s; only %d distinct person(s) are valid", reason, len(persons))
		}
		if auditErr := writeInstanceAudit(ctx, tx, locked.InstanceID, openedBy, ActionInstanceSupersede, controlstore.AuditRefused,
			string(RefusalApprovalMissing), req.OperationID, map[string]any{
				"reason":                    reason,
				"approval_refs":             refs,
				"risk_acceptance_delivered": false,
			}); auditErr != nil {
			return SupersedeInstanceResult{}, auditErr
		}
		if err := tx.Commit(ctx); err != nil {
			return SupersedeInstanceResult{}, fmt.Errorf("commit instance supersede refusal: %w", err)
		}
		if err != nil {
			return SupersedeInstanceResult{}, fmt.Errorf("%w: %v", ErrInstanceApprovalRequired, err)
		}
		return SupersedeInstanceResult{}, fmt.Errorf("%w: %s", ErrInstanceApprovalRequired, reason)
	}

	if err := closeInstanceRow(ctx, tx, locked, openedBy); err != nil {
		return SupersedeInstanceResult{}, err
	}
	var newInstanceID string
	if err := tx.QueryRow(ctx, `
INSERT INTO recovery_instance
    (instance_id, kind, state, supersedes_instance_id, evidence_generation, evidence_hash, opened_by, reason)
VALUES (gen_random_uuid(), 'recovery', 'open', $1, 0, $2, $3, $4)
RETURNING instance_id::text`,
		locked.InstanceID, controlstore.EmptyEvidenceHash, openedBy, req.Reason).Scan(&newInstanceID); err != nil {
		return SupersedeInstanceResult{}, fmt.Errorf("open the superseding recovery instance: %w", err)
	}

	principalList := make([]string, 0, len(persons))
	for person, principal := range persons {
		principalList = append(principalList, principal+" ("+person+")")
	}
	if err := writeInstanceAudit(ctx, tx, locked.InstanceID, openedBy, ActionInstanceSupersede, controlstore.AuditOK, "",
		req.OperationID, map[string]any{
			"reason":          req.Reason,
			"new_instance_id": newInstanceID,
			"approvals":       principalList,
			"approval_refs":   refs,
		}); err != nil {
		return SupersedeInstanceResult{}, err
	}
	if err := writeInstanceAudit(ctx, tx, newInstanceID, openedBy, controlstore.ActionInstanceOpen, controlstore.AuditOK, "",
		req.OperationID, map[string]any{
			"reason":                 req.Reason,
			"supersedes_instance_id": locked.InstanceID,
			"kind":                   "recovery",
		}); err != nil {
		return SupersedeInstanceResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return SupersedeInstanceResult{}, fmt.Errorf("commit instance supersede: %w", err)
	}
	return SupersedeInstanceResult{SupersededInstanceID: locked.InstanceID, NewInstanceID: newInstanceID}, nil
}

// newGateIdentities builds the identity resolver used by the lifecycle and
// checklist paths (the same derivation the gate uses inside an admission).
// The maps ensureParticipants fills lazily are initialized here; mappings must
// be non-nil before the first lookup.
func newGateIdentities(ctx context.Context, tx pgx.Tx, instanceID, openedBy string) *gateIdentities {
	return &gateIdentities{
		ctx:        ctx,
		tx:         tx,
		instanceID: instanceID,
		openedBy:   openedBy,
		mappings:   make(map[string]gateMapping),
	}
}

// instanceReleaseScopes lists the distinct scope hashes that ever carried a
// release decision for (instance, capability), newest first. They are the
// candidate scopes for the close guard; validity is still decided solely by
// the derived evaluation.
func instanceReleaseScopes(ctx context.Context, tx pgx.Tx, instanceID, capability string) ([]string, error) {
	rows, err := tx.Query(ctx, `
SELECT scope_hash
FROM recovery_release
WHERE instance_id = $1 AND capability = $2
GROUP BY scope_hash
ORDER BY max(created_at) DESC, scope_hash`, instanceID, capability)
	if err != nil {
		return nil, fmt.Errorf("read release scopes of capability %s: %w", capability, err)
	}
	defer rows.Close()
	var scopes []string
	for rows.Next() {
		var scope string
		if err := rows.Scan(&scope); err != nil {
			return nil, fmt.Errorf("scan release scope: %w", err)
		}
		scopes = append(scopes, scope)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read release scopes of capability %s: %w", capability, err)
	}
	return scopes, nil
}

// instanceApprovalPersons validates a set of approval references against the
// locked instance and returns the distinct person ids they prove. Every
// reference must exist, be an approve decision of this instance, be the
// principal's current decision (a later revoke covers it), be bound to the
// locked (evidence_generation, evidence_hash), be recorded by a principal
// registered with the approver role, match the principal's active identity
// mapping, and resolve to a person other than the instance executor. Invalid
// references are reported; the caller decides between a refusal audit and an
// abort.
func instanceApprovalPersons(ctx context.Context, store *controlstore.Store, ids *gateIdentities, token controlstore.InstanceToken, refs []string) (map[string]string, error) {
	if err := ids.ensureParticipants(); err != nil {
		return nil, err
	}
	persons := make(map[string]string, len(refs))
	for _, ref := range refs {
		var (
			rowInstance, capability, scopeHash, decision string
			principal, personID                          string
			generation                                   int64
			hash                                         string
		)
		err := ids.tx.QueryRow(ctx, `
SELECT instance_id::text, capability, scope_hash, decision, principal, person_id,
       evidence_generation, evidence_hash
FROM recovery_approval
WHERE approval_id::text = $1`, ref).
			Scan(&rowInstance, &capability, &scopeHash, &decision, &principal, &personID, &generation, &hash)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("approval %s does not exist", ref)
		}
		if err != nil {
			return nil, fmt.Errorf("read approval %s: %w", ref, err)
		}
		if rowInstance != token.InstanceID {
			return nil, fmt.Errorf("approval %s belongs to instance %s, not %s", ref, rowInstance, token.InstanceID)
		}
		if decision != "approve" {
			return nil, fmt.Errorf("approval %s is a %q, not an approve", ref, decision)
		}
		if !ids.approvers[principal] {
			return nil, fmt.Errorf("approval %s was recorded by %s, which is not registered with the approver role on this instance", ref, principal)
		}
		if generation != token.EvidenceGeneration || hash != token.EvidenceHash {
			return nil, fmt.Errorf("approval %s is bound to generation=%d hash=%s but the instance is at generation=%d hash=%s",
				ref, generation, hash, token.EvidenceGeneration, token.EvidenceHash)
		}
		current, err := store.CurrentApprovalDecision(ctx, ids.tx, controlstore.DecisionKey{
			InstanceID: token.InstanceID,
			Capability: capability,
			ScopeHash:  scopeHash,
		}, principal)
		if err != nil {
			return nil, fmt.Errorf("read the current decision of %s: %w", principal, err)
		}
		if !current.Found || current.DecisionID != ref || current.Decision != "approve" {
			return nil, fmt.Errorf("approval %s is not the current approve decision of %s (a later decision covers it)", ref, principal)
		}
		mapped, active, err := ids.mappingFor(principal)
		if err != nil {
			return nil, err
		}
		if !active || mapped != personID {
			return nil, fmt.Errorf("approval %s recorded person_id %s for %s, which does not match the current active mapping", ref, personID, principal)
		}
		executor, err := ids.isExecutorIdentity(principal, personID)
		if err != nil {
			return nil, err
		}
		if executor {
			return nil, fmt.Errorf("approval %s by %s resolves to this instance's executor person", ref, principal)
		}
		persons[personID] = principal
	}
	return persons, nil
}

// lifecycleInstanceID canonicalizes and validates the instance id of a
// lifecycle request.
func lifecycleInstanceID(raw string) (string, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return "", errors.New("recovery instance lifecycle requires an instance_id")
	}
	parsed, err := uuid.Parse(value)
	if err != nil {
		return "", fmt.Errorf("instance_id %q is not a UUID: %w", raw, err)
	}
	return parsed.String(), nil
}

// writeInstanceAudit appends one lifecycle audit row. It validates the closed
// result/refusal-class sets through controlstore.WriteAudit.
func writeInstanceAudit(ctx context.Context, q controlstore.Queryer, instanceID, actor, action, result, refusalClass, operationID string, detail map[string]any) error {
	encodedDetail, err := json.Marshal(detail)
	if err != nil {
		return fmt.Errorf("encode %s audit detail: %w", action, err)
	}
	return controlstore.WriteAudit(ctx, q, controlstore.AuditRecord{
		InstanceID:   instanceID,
		Actor:        actor,
		Action:       action,
		Detail:       encodedDetail,
		Result:       result,
		RefusalClass: refusalClass,
		OperationID:  operationID,
	})
}
