// checklist.go implements T028: the isolation checklist of FR-010/011/012 —
// the recovery-environment version of the 000018 checklist pattern
// (contracts/resumption-gate.md §3, data-model.md §1.7/§4.3).
//
// The state machine, per item of the closed six-key set (capabilities.go):
//
//	(absent = pending) --Set(evidenced)--> evidenced --Verify--> verified
//	evidenced/verified --Verify(reject)--> rejected --Set(new evidence)--> evidenced
//
// Rules (all fail-closed):
//
//   - Collection (Set) is performed by this instance's executor and must carry
//     an external evidence reference. A checkpoint summary or state record
//     alone is never proof: an empty/blank evidence_ref refuses and writes
//     nothing. Collection does not advance the instance evidence generation.
//   - Confirmation (Verify) is performed by a registered verifier of this
//     instance whose person is not the executor — including the same person
//     under another principal (the person mapping decides, FR-023). A
//     self-verification/role/mapping refusal is audited and leaves the item
//     unchanged.
//   - verified/rejected transitions run through the T013 generation protocol
//     (CommitEvidenceWrite, MutationIsolationVerified/MutationIsolationRejected):
//     exactly one evidence-generation advance per accepted verdict, which
//     invalidates every release/approval bound to the previous generation.
//     A captured token that no longer matches is discarded with the protocol's
//     audit row and returns an error; it never lands as a stale verdict.
//   - A rejected item cannot be verified directly: it must be re-collected
//     with a new external reference first. A verified item is not silently
//     re-collected either; only a non-executor rejection verdict moves it back
//     to re-collection. Re-running the confirmation on a verified item is an
//     idempotent re-affirmation (it stays verified, is re-audited and still
//     advances the generation), so bounded retries converge (FR-024).
//   - `no_pre_release_effects` is verified against the gate audit, never by a
//     state field: if the control store already recorded an admitted
//     externally visible action (a gate admission of an effectful capability)
//     for this instance, collection and verification of that item refuse.
//     Boundary: this check covers admissions that committed before it runs; an
//     action admitted before OpenInstance and still in flight cannot be
//     discovered from the audit trail, so the isolation procedure must drain
//     or wait for in-flight actions before confirming this item (runbook
//     T064). "The next admission will refuse" is not by itself proof that the
//     interleaving was covered.
//   - The per-capability release prerequisite (its isolation_dependency_set
//     all verified) is enforced by the gate (T012); this file only owns the
//     item state machine and its persistence. `checkpoint_summary` is stored
//     for operators and never read as a verification basis.
//   - Every accepted and refused transition is audited (recovery_audit action
//     isolation_check). Refused writes leave the row untouched.
package recovery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

// ActionIsolationCheck is the recovery_audit action of every checklist
// transition (accepted and refused).
const ActionIsolationCheck = "isolation_check"

var (
	// ErrChecklistTransition marks an invalid state transition or a missing
	// evidence reference: the write is refused, nothing is persisted and the
	// refusal is audited.
	ErrChecklistTransition = errors.New("isolation checklist transition is not allowed")
	// ErrChecklistSelfVerification marks a verifier that resolves to this
	// instance's executor (including the same person under another principal).
	ErrChecklistSelfVerification = errors.New("isolation checklist verifier resolves to this instance's executor")
	// ErrChecklistActor marks an actor that is not authorized for the
	// operation: collection by a non-executor, or confirmation by a principal
	// without the verifier binding / active identity mapping.
	ErrChecklistActor = errors.New("isolation checklist actor is not authorized for this operation")
)

// ChecklistState is one member of the closed checklist state set of
// recovery_isolation_check.state. Pending is the implicit state of an absent
// row; it is never stored.
type ChecklistState string

// The four checklist states (data-model §1.7/§4.3).
const (
	ChecklistStatePending   ChecklistState = "pending"
	ChecklistStateEvidenced ChecklistState = "evidenced"
	ChecklistStateVerified  ChecklistState = "verified"
	ChecklistStateRejected  ChecklistState = "rejected"
)

// Known reports whether s is one of the four checklist states.
func (s ChecklistState) Known() bool {
	switch s {
	case ChecklistStatePending, ChecklistStateEvidenced, ChecklistStateVerified, ChecklistStateRejected:
		return true
	}
	return false
}

// ChecklistItem is one persisted checklist row. CheckedAt is zero when the
// row has no collection facts and VerifiedAt is zero while no verdict exists.
type ChecklistItem struct {
	InstanceID        string
	ItemKey           IsolationItemKey
	State             ChecklistState
	EvidenceRef       string
	CheckpointSummary []byte
	CheckedBy         string
	CheckedAt         time.Time
	VerifiedBy        string
	VerifiedAt        time.Time
}

// ChecklistEvidenceRequest is one evidence-collection request (Set). State
// accepts only ChecklistStateEvidenced: a journal-style summary can never be
// recorded as a verdict.
type ChecklistEvidenceRequest struct {
	InstanceID string
	ItemKey    IsolationItemKey
	State      ChecklistState
	// EvidenceRef is the external evidence reference and is required; a
	// blank value refuses (a state/checkpoint summary alone is never proof).
	EvidenceRef string
	// CheckpointSummary is status/context notes stored for operators. It is
	// never read as a verification basis and may be empty.
	CheckpointSummary []byte
	// Actor is this instance's executor (principal or person resolving to it).
	Actor string
	// OperationID is the command idempotency key carried into the audit rows.
	OperationID string
}

// ChecklistVerifyRequest is one non-executor verdict (Verify).
type ChecklistVerifyRequest struct {
	InstanceID string
	ItemKey    IsolationItemKey
	// Actor is a participant of this instance whose person is not the
	// executor's (same person under another principal included).
	Actor string
	// OperationID is the command idempotency key carried into the audit rows.
	OperationID string
	// Reject moves the item to rejected (insufficient or superseded evidence;
	// re-collection required). Rejecting an already verified item is allowed:
	// a later evidence change must be able to downgrade it.
	Reject bool
	// Reason is required for a rejection verdict and recorded in the audit.
	Reason string
}

// Checklist is the isolation checklist service over a version-guarded control
// store. It holds no mutable state.
type Checklist struct {
	store *controlstore.Store
}

// NewChecklist builds the checklist service. A nil store refuses: there is no
// unguarded persistence path (the T069 version guard is inherited through the
// store).
func NewChecklist(store *controlstore.Store) (*Checklist, error) {
	if store == nil {
		return nil, errors.New("isolation checklist requires a controlstore.Store built by controlstore.NewStore")
	}
	return &Checklist{store: store}, nil
}

const checklistSelectColumns = `
instance_id::text, item_key, state,
COALESCE(evidence_ref, ''), COALESCE(checkpoint_summary::text, ''),
COALESCE(checked_by, ''), checked_at, COALESCE(verified_by, ''), verified_at`

// Item reads one item. found=false means the item is in the implicit pending
// state (no row). An unknown item key refuses with ErrUnknownIsolationItem.
func (c *Checklist) Item(ctx context.Context, instanceID string, item IsolationItemKey) (ChecklistItem, bool, error) {
	if c == nil || c.store == nil {
		return ChecklistItem{}, false, errors.New("isolation checklist requires a control store")
	}
	if !item.Known() {
		return ChecklistItem{}, false, fmt.Errorf("%w: %q", ErrUnknownIsolationItem, item)
	}
	id, err := checklistInstanceID(instanceID)
	if err != nil {
		return ChecklistItem{}, false, err
	}
	row := c.store.Pool().QueryRow(ctx,
		`SELECT `+checklistSelectColumns+`
		 FROM recovery_isolation_check WHERE instance_id = $1 AND item_key = $2`, id, string(item))
	record, err := scanChecklistItem(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return ChecklistItem{}, false, nil
	}
	if err != nil {
		return ChecklistItem{}, false, err
	}
	return record, true, nil
}

// Set records evidence collection (pending/evidenced/rejected -> evidenced)
// for this instance's executor. It does not advance the evidence generation:
// only verified/rejected verdicts do (data-model §5). The transition is taken
// under the instance row lock, so it serializes with every verdict and
// decision write.
func (c *Checklist) Set(ctx context.Context, req ChecklistEvidenceRequest) (ChecklistItem, error) {
	if c == nil || c.store == nil {
		return ChecklistItem{}, errors.New("isolation checklist requires a control store")
	}
	item := req.ItemKey
	if !item.Known() {
		return ChecklistItem{}, fmt.Errorf("%w: %q", ErrUnknownIsolationItem, item)
	}
	if req.State != ChecklistStateEvidenced {
		return ChecklistItem{}, fmt.Errorf(
			"%w: collection records state=%q only; a verdict is recorded by a non-executor confirmation",
			ErrChecklistTransition, req.State)
	}
	id, err := checklistInstanceID(req.InstanceID)
	if err != nil {
		return ChecklistItem{}, err
	}
	actor := strings.TrimSpace(req.Actor)
	if actor == "" {
		return ChecklistItem{}, fmt.Errorf("%w: actor is required", ErrChecklistActor)
	}
	ref := strings.TrimSpace(req.EvidenceRef)
	if ref == "" {
		reason := "evidence_ref is required: a checkpoint summary or state record alone is never proof"
		checklistWriteRefusal(ctx, c.store.Pool(), id, actor, item, reason, "", req.OperationID)
		return ChecklistItem{}, fmt.Errorf("%w: %s", ErrChecklistTransition, reason)
	}
	summary, err := checklistSummary(req.CheckpointSummary)
	if err != nil {
		return ChecklistItem{}, err
	}

	tx, err := c.store.Pool().Begin(ctx)
	if err != nil {
		return ChecklistItem{}, fmt.Errorf("begin checklist collection: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	locked, err := controlstore.LockInstance(ctx, tx, id)
	if err != nil {
		return ChecklistItem{}, err
	}
	if locked.State != "open" {
		return ChecklistItem{}, fmt.Errorf("%w: %s", controlstore.ErrInstanceNotOpen, locked.InstanceID)
	}
	if locked.Kind != "recovery" {
		return ChecklistItem{}, fmt.Errorf("%w: %s has kind=%s; checklist items apply to recovery instances",
			ErrChecklistTransition, locked.InstanceID, locked.Kind)
	}

	refuse := func(reason string, wrapped error) (ChecklistItem, error) {
		_ = checklistWriteRefusal(ctx, tx, locked.InstanceID, actor, item, reason, "", req.OperationID)
		if err := tx.Commit(ctx); err != nil {
			return ChecklistItem{}, fmt.Errorf("commit checklist refusal audit: %w", err)
		}
		return ChecklistItem{}, fmt.Errorf("%w: %s", wrapped, reason)
	}

	ids := newGateIdentities(ctx, tx, locked.InstanceID, locked.OpenedBy)
	executor, err := ids.isExecutorIdentity(actor, "")
	if err != nil {
		return ChecklistItem{}, err
	}
	if !executor {
		return refuse(fmt.Sprintf("%s does not resolve to this instance's executor; evidence is collected by the executor", actor), ErrChecklistActor)
	}

	var state string
	err = tx.QueryRow(ctx,
		`SELECT state FROM recovery_isolation_check WHERE instance_id = $1 AND item_key = $2 FOR UPDATE`,
		locked.InstanceID, string(item)).Scan(&state)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return ChecklistItem{}, fmt.Errorf("read checklist item %s: %w", item, err)
	}
	switch ChecklistState(state) {
	case "", ChecklistStateEvidenced, ChecklistStateRejected:
	default:
		return refuse(fmt.Sprintf("%s is state=%s; a verified item is not re-collected silently (a non-executor rejection verdict must move it back first)", item, state), ErrChecklistTransition)
	}

	if item == IsolationItemNoPreReleaseEffects {
		blocked, err := checklistEffectAdmissionsExist(ctx, tx, locked.InstanceID)
		if err != nil {
			return ChecklistItem{}, err
		}
		if blocked {
			return refuse("the gate audit already records an admitted externally visible action for this instance; no_pre_release_effects cannot be collected", ErrChecklistTransition)
		}
	}

	record, err := scanChecklistItem(tx.QueryRow(ctx, `
INSERT INTO recovery_isolation_check
    (check_id, instance_id, item_key, state, evidence_ref, checkpoint_summary,
     checked_by, checked_at, verified_by, verified_at)
VALUES (gen_random_uuid(), $1, $2, 'evidenced', $3, $4, $5, now(), NULL, NULL)
ON CONFLICT (instance_id, item_key) DO UPDATE
   SET state = 'evidenced', evidence_ref = EXCLUDED.evidence_ref,
       checkpoint_summary = EXCLUDED.checkpoint_summary,
       checked_by = EXCLUDED.checked_by, checked_at = now(),
       verified_by = NULL, verified_at = NULL
RETURNING `+checklistSelectColumns, locked.InstanceID, string(item), ref, checklistJSONOrNil(summary), actor))
	if err != nil {
		return ChecklistItem{}, fmt.Errorf("record checklist evidence for %s: %w", item, err)
	}

	generation := locked.EvidenceGeneration
	detail, err := json.Marshal(map[string]any{
		"transition":      string(ChecklistStateEvidenced),
		"item_key":        string(item),
		"evidence_ref":    ref,
		"summary_present": len(summary) > 0,
	})
	if err != nil {
		return ChecklistItem{}, fmt.Errorf("encode checklist audit detail: %w", err)
	}
	if err := controlstore.WriteAudit(ctx, tx, controlstore.AuditRecord{
		InstanceID:         locked.InstanceID,
		Actor:              actor,
		Action:             ActionIsolationCheck,
		Detail:             detail,
		Result:             controlstore.AuditOK,
		EvidenceGeneration: &generation,
		OperationID:        req.OperationID,
	}); err != nil {
		return ChecklistItem{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ChecklistItem{}, fmt.Errorf("commit checklist collection: %w", err)
	}
	return record, nil
}

// Verify records one non-executor verdict. An approve verdict is allowed from
// evidenced and moves the item to verified; it also re-affirms an already
// verified item (idempotent reentrancy, FR-024: repeating an accepted verdict
// converges instead of refusing; the item stays verified and the verdict is
// re-audited). A rejection verdict is allowed from evidenced or verified and
// moves the item to rejected. Every accepted verdict goes through
// CommitEvidenceWrite, so it advances the evidence generation by exactly 1
// (invalidating older releases/approvals) and a stale captured token is
// discarded with the protocol's audit row.
func (c *Checklist) Verify(ctx context.Context, req ChecklistVerifyRequest) (ChecklistItem, error) {
	if c == nil || c.store == nil {
		return ChecklistItem{}, errors.New("isolation checklist requires a control store")
	}
	item := req.ItemKey
	if !item.Known() {
		return ChecklistItem{}, fmt.Errorf("%w: %q", ErrUnknownIsolationItem, item)
	}
	id, err := checklistInstanceID(req.InstanceID)
	if err != nil {
		return ChecklistItem{}, err
	}
	actor := strings.TrimSpace(req.Actor)
	if actor == "" {
		return ChecklistItem{}, fmt.Errorf("%w: actor is required", ErrChecklistActor)
	}
	reason := strings.TrimSpace(req.Reason)
	if req.Reject && reason == "" {
		return ChecklistItem{}, fmt.Errorf("%w: a rejection verdict requires a reason", ErrChecklistTransition)
	}

	token, err := CaptureEvidenceToken(ctx, c.store.Pool(), id)
	if err != nil {
		return ChecklistItem{}, err
	}
	if token.State != "open" {
		return ChecklistItem{}, fmt.Errorf("%w: %s", controlstore.ErrInstanceNotOpen, token.InstanceID)
	}

	kind := MutationIsolationVerified
	if req.Reject {
		kind = MutationIsolationRejected
	}
	var record ChecklistItem
	outcome, err := CommitEvidenceWrite(ctx, c.store, EvidenceWriteRequest{
		InstanceID:  id,
		Token:       token,
		Kind:        kind,
		Actor:       actor,
		Reason:      reason,
		OperationID: req.OperationID,
		Apply: func(ctx context.Context, tx pgx.Tx, accepted EvidenceToken) error {
			var instanceKind, openedBy string
			if err := tx.QueryRow(ctx,
				`SELECT kind, opened_by FROM recovery_instance WHERE instance_id = $1`,
				accepted.InstanceID).Scan(&instanceKind, &openedBy); err != nil {
				return fmt.Errorf("read instance for checklist verification: %w", err)
			}
			if instanceKind != "recovery" {
				return fmt.Errorf("%w: instance kind=%s; checklist items apply to recovery instances", ErrChecklistTransition, instanceKind)
			}
			ids := newGateIdentities(ctx, tx, accepted.InstanceID, openedBy)
			self, err := ids.isExecutorIdentity(actor, "")
			if err != nil {
				return err
			}
			if self {
				return fmt.Errorf("%w: %s resolves to this instance's executor (opened_by or executor binding/person); a non-executor participant must confirm", ErrChecklistSelfVerification, actor)
			}
			if err := checklistRequireVerifier(ctx, ids, actor); err != nil {
				return err
			}

			var state string
			err = tx.QueryRow(ctx,
				`SELECT state FROM recovery_isolation_check WHERE instance_id = $1 AND item_key = $2 FOR UPDATE`,
				accepted.InstanceID, string(item)).Scan(&state)
			if errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("%w: %s has no collected evidence; collect evidence before verification", ErrChecklistTransition, item)
			}
			if err != nil {
				return fmt.Errorf("read checklist item %s: %w", item, err)
			}
			valid := false
			switch {
			case req.Reject && state == string(ChecklistStateEvidenced):
				valid = true
			case req.Reject && state == string(ChecklistStateVerified):
				valid = true
			case !req.Reject && state == string(ChecklistStateEvidenced):
				valid = true
			case !req.Reject && state == string(ChecklistStateVerified):
				// Idempotent re-affirmation of a verified item: a repeated
				// accepted verdict converges (FR-024) instead of refusing, and
				// the item never regresses. The actor/verifier/mapping checks
				// above still apply, and the accepted verdict advances the
				// generation, so no older release/approval silently survives.
				valid = true
			}
			if !valid {
				return fmt.Errorf("%w: %s is state=%s; want evidenced or verified (a rejection verdict may also supersede verified)", ErrChecklistTransition, item, state)
			}
			if item == IsolationItemNoPreReleaseEffects {
				blocked, err := checklistEffectAdmissionsExist(ctx, tx, accepted.InstanceID)
				if err != nil {
					return err
				}
				if blocked {
					return fmt.Errorf("%w: no_pre_release_effects cannot be verified: the gate audit records an admitted externally visible action for this instance", ErrChecklistTransition)
				}
			}

			next := ChecklistStateVerified
			if req.Reject {
				next = ChecklistStateRejected
			}
			record, err = scanChecklistItem(tx.QueryRow(ctx, `
UPDATE recovery_isolation_check
   SET state = $3, verified_by = $4, verified_at = now()
 WHERE instance_id = $1 AND item_key = $2
RETURNING `+checklistSelectColumns, accepted.InstanceID, string(item), string(next), actor))
			if err != nil {
				return fmt.Errorf("record checklist verdict for %s: %w", item, err)
			}

			detail, err := json.Marshal(map[string]any{
				"transition":   string(next),
				"item_key":     string(item),
				"evidence_ref": record.EvidenceRef,
				"reason":       reason,
			})
			if err != nil {
				return fmt.Errorf("encode checklist audit detail: %w", err)
			}
			generation := accepted.Generation
			if err := controlstore.WriteAudit(ctx, tx, controlstore.AuditRecord{
				InstanceID:         accepted.InstanceID,
				Actor:              actor,
				Action:             ActionIsolationCheck,
				Detail:             detail,
				Result:             controlstore.AuditOK,
				EvidenceGeneration: &generation,
				OperationID:        req.OperationID,
			}); err != nil {
				return err
			}
			return nil
		},
	})
	if err != nil {
		if errors.Is(err, ErrChecklistTransition) || errors.Is(err, ErrChecklistSelfVerification) || errors.Is(err, ErrChecklistActor) {
			refusalClass := ""
			if errors.Is(err, ErrChecklistSelfVerification) {
				refusalClass = string(RefusalIsolationUnproven)
			}
			checklistWriteRefusal(ctx, c.store.Pool(), id, actor, item, err.Error(), refusalClass, req.OperationID)
		}
		return ChecklistItem{}, err
	}
	if outcome.Discarded {
		return ChecklistItem{}, fmt.Errorf(
			"isolation checklist verdict for %s was discarded because the evidence token changed (captured generation=%d, observed generation=%d); re-read the item and retry",
			item, token.Generation, outcome.Token.Generation)
	}
	return record, nil
}

// checklistRequireVerifier enforces the confirmation side of FR-023: the
// actor must have an active identity mapping, must hold the verifier binding
// of this instance, and the binding's recorded person must still match the
// current mapping (a mapping change invalidates the old basis, F19).
func checklistRequireVerifier(ctx context.Context, ids *gateIdentities, actor string) error {
	person, active, err := ids.mappingFor(actor)
	if err != nil {
		return err
	}
	if !active {
		return fmt.Errorf("%w: %s has no active identity mapping; a verifier person cannot be proven", ErrChecklistActor, actor)
	}
	var recordedPerson string
	err = ids.tx.QueryRow(ctx,
		`SELECT person_id FROM recovery_participant WHERE instance_id = $1 AND principal = $2 AND role = 'verifier'`,
		ids.instanceID, actor).Scan(&recordedPerson)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: %s is not registered as a verifier on instance %s", ErrChecklistActor, actor, ids.instanceID)
	}
	if err != nil {
		return fmt.Errorf("read verifier binding of %s: %w", actor, err)
	}
	if recordedPerson != person {
		return fmt.Errorf("%w: %s is registered as verifier with person_id %s but the active mapping now resolves to %s (a mapping change invalidates the old basis)",
			ErrChecklistActor, actor, recordedPerson, person)
	}
	return nil
}

// checklistEffectAdmissionsExist reports whether the gate audit already holds
// an admitted externally visible action for this instance: a gate admission
// that allowed one of the effectful capabilities. Reads are never effectful,
// so only writes/sends/deliveries count. Timeout/unreachability never counts
// as an admitted action (nothing is inferred from it).
func checklistEffectAdmissionsExist(ctx context.Context, q controlstore.Queryer, instanceID string) (bool, error) {
	effectful := []string{
		string(CapabilityNewWithdrawalCreation),
		string(CapabilityExistingWithdrawalRecovery),
		string(CapabilityEventPublishing),
		string(CapabilityEventConsuming),
	}
	var exists bool
	err := q.QueryRow(ctx, `
SELECT EXISTS (
    SELECT 1 FROM recovery_audit
    WHERE instance_id = $1 AND action = $2 AND result = $3
      AND target->>'capability' = ANY($4))`,
		instanceID, GateAuditAction, controlstore.AuditOK, effectful).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("read admitted external actions for instance %s: %w", instanceID, err)
	}
	return exists, nil
}

// checklistWriteRefusal appends one refusal audit row (best effort: the
// refusal itself is already decided and the row is never a precondition).
func checklistWriteRefusal(ctx context.Context, q controlstore.Queryer, instanceID, actor string, item IsolationItemKey, reason, refusalClass, operationID string) error {
	if q == nil || instanceID == "" || actor == "" {
		return nil
	}
	detail, err := json.Marshal(map[string]any{
		"transition": "refused",
		"item_key":   string(item),
		"reason":     reason,
	})
	if err != nil {
		return err
	}
	return controlstore.WriteAudit(ctx, q, controlstore.AuditRecord{
		InstanceID:   instanceID,
		Actor:        actor,
		Action:       ActionIsolationCheck,
		Detail:       detail,
		Result:       controlstore.AuditRefused,
		RefusalClass: refusalClass,
		OperationID:  operationID,
	})
}

// checklistSummary validates the optional checkpoint summary. It is stored
// for operators and is never a verification basis.
func checklistSummary(raw []byte) ([]byte, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	if !json.Valid(raw) {
		return nil, fmt.Errorf("%w: checkpoint_summary must be valid JSON", ErrChecklistTransition)
	}
	return raw, nil
}

func checklistJSONOrNil(raw []byte) any {
	if len(raw) == 0 {
		return nil
	}
	return string(raw)
}

// checklistInstanceID canonicalizes and validates the instance id.
func checklistInstanceID(raw string) (string, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return "", errors.New("isolation checklist requires an instance_id")
	}
	parsed, err := uuid.Parse(value)
	if err != nil {
		return "", fmt.Errorf("instance_id %q is not a UUID: %w", raw, err)
	}
	return parsed.String(), nil
}

// scanChecklistItem scans one row of checklistSelectColumns.
func scanChecklistItem(row pgx.Row) (ChecklistItem, error) {
	var (
		record     ChecklistItem
		itemKey    string
		state      string
		summary    string
		checkedAt  *time.Time
		verifiedAt *time.Time
	)
	if err := row.Scan(
		&record.InstanceID, &itemKey, &state,
		&record.EvidenceRef, &summary,
		&record.CheckedBy, &checkedAt, &record.VerifiedBy, &verifiedAt,
	); err != nil {
		return ChecklistItem{}, err
	}
	record.ItemKey = IsolationItemKey(itemKey)
	record.State = ChecklistState(state)
	if summary != "" {
		record.CheckpointSummary = []byte(summary)
	}
	if checkedAt != nil {
		record.CheckedAt = *checkedAt
	}
	if verifiedAt != nil {
		record.VerifiedAt = *verifiedAt
	}
	return record, nil
}
