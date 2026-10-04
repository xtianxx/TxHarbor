// gaps.go implements T041 [US3]: the evidence-gap service of FR-019 — the
// recovery_gap state machine, the human-handling evidence package, the
// conservative pause set with dependency proofs, audit-only notes, escalation
// and the bounded read-only review (FR-019; C3; data-model.md §1.6/§4.3/§6;
// contracts/verification-items.md §2; tasks.md T041).
//
// State machine (recovery_gap.state, schema CHECK):
//
//	absent --Open--> open --Close(new evidence)--> closed
//	                 open --Escalate(ref)--> escalated --Close(new evidence)--> closed
//
// Rules (all fail-closed):
//
//   - Open persists one complete FR-019 evidence package for a single object:
//     the object key and scope, the timeline, the existing evidence, the
//     required external evidence, the risk payload, the affected capabilities
//     and the responsibility owner (the escalation record follows on
//     Escalate). Every component is required: a missing/blank one refuses with
//     ErrGapInput, writes nothing and is audited. Nothing is filled in from a
//     default.
//   - The pause set is conservative. It always contains the directly related
//     capabilities, every capability that transitively requires them (the
//     amplification direction of the frozen requires_capabilities matrix,
//     capabilities.go) and every known capability named by the risk payload's
//     may_amplify hints. Narrowing the set requires a dependency proof that
//     carries a path from a directly affected capability to the proven one and
//     a non-empty evidence reference for every edge of that path; "it is a
//     different module" proves nothing, and an unreadable/incomplete proof
//     narrows nothing. The proof is persisted verbatim either way.
//   - Establishment and closure advance the evidence generation through the
//     T013 protocol (CommitEvidenceWrite, MutationGapOpened/MutationGapClosed),
//     invalidating every release/approval bound to the previous generation; a
//     stale captured token is discarded by the protocol and never lands.
//     Escalation, audit notes and reviews change no generation.
//   - Closure is possible only with new evidence (ErrGapClosureEvidence);
//     timeout / attempts_exhausted / acknowledged are audit notes, never a
//     state change, never a closure and never a release permission. An
//     escalated gap keeps blocking every capability it names (the gate treats
//     open and escalated alike) until new evidence closes it. A closed gap
//     stops blocking; a later gap on the same object is appended as a new row
//     and no historical row is ever updated or deleted.
//   - Review is a bounded read-only pass over rows of its own instance
//     (evidence, verification items, gaps, audit). The configured budget is a
//     hard bound on the number of passes; a missing/zero or exhausted budget
//     refuses with ErrReviewBudgetExhausted and one refusal audit row, and
//     changes no gap/instance/approval/release state. A timeout, an exhausted
//     budget and human knowledge are never closure or a release permission.
//   - operation_id is the idempotency key of every command: an accepted
//     operation replays from its recorded audit row without a second write,
//     and a different request under the same operation_id refuses with
//     controlstore.ErrOperationConflict.
//
// Boundaries: this service reads and writes only the control store and exposes
// no payment, signing, broadcast, replay, delivery or intent-creating entry
// point. It never releases a capability by itself (the derived gate evaluation
// of T012 and the FR-023 approval path own that) and it delivers no risk
// acceptance, write-off, compensation or forced resumption.
package recovery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

// Action constants of recovery_audit for the gap commands. Accepted
// transitions are recorded under their own action; refusals use the same
// action with result='refused'.
const (
	// ActionGapOpen: a gap was established (or an open one returned).
	ActionGapOpen = "gap_open"
	// ActionGapClose: a gap was closed by new evidence.
	ActionGapClose = "gap_close"
	// ActionGapEscalate: a gap was escalated with a reference.
	ActionGapEscalate = "gap_escalate"
	// ActionGapNote: an audit-only note (timeout/attempts_exhausted/
	// acknowledged) was recorded; no state changed.
	ActionGapNote = "gap_note"
	// ActionGapReview: a bounded read-only review pass (accepted or refused).
	ActionGapReview = "gap_review"
)

var (
	// ErrGapInput marks a malformed or incomplete gap request. The request is
	// never turned into a default: it is refused, nothing is written and —
	// when an instance/actor is identifiable — the refusal is audited.
	ErrGapInput = errors.New("evidence gap request is invalid")
	// ErrGapTransition marks a transition refused in the current state (for
	// example a close of an already closed gap, or an escalation of a
	// non-open gap).
	ErrGapTransition = errors.New("evidence gap transition is not allowed")
	// ErrGapClosureEvidence marks a closure without new evidence: timeouts,
	// exhausted attempts and acknowledgements are audit notes, never closure.
	ErrGapClosureEvidence = errors.New("evidence gap closure requires new evidence")
	// ErrGapEscalationRef marks an escalation without a recorded reference.
	ErrGapEscalationRef = errors.New("evidence gap escalation requires a reference")
	// ErrGapNotFound marks an operation on a gap that does not exist for the
	// instance.
	ErrGapNotFound = errors.New("evidence gap not found")
	// ErrReviewBudgetExhausted marks a review refused because the configured
	// read-only review budget is missing/zero or exhausted. The refusal is
	// audited and changes no state.
	ErrReviewBudgetExhausted = errors.New("evidence gap review budget is exhausted")
)

// GapNoteKind is one member of the closed audit-note set. The kinds are audit
// reasons recorded with the note; they are never gap states and never change
// one (FR-019, schema comment).
type GapNoteKind string

// The three audit-only note kinds.
const (
	GapNoteTimeout           GapNoteKind = "timeout"
	GapNoteAttemptsExhausted GapNoteKind = "attempts_exhausted"
	GapNoteAcknowledged      GapNoteKind = "acknowledged"
)

// knownGapNoteKinds is the canonical order; display/validation only.
var knownGapNoteKinds = []GapNoteKind{GapNoteTimeout, GapNoteAttemptsExhausted, GapNoteAcknowledged}

// Known reports whether k is one of the three audit-only note kinds.
func (k GapNoteKind) Known() bool {
	return slices.Contains(knownGapNoteKinds, k)
}

// KnownGapNoteKinds returns the closed note-kind set in canonical order. The
// caller receives a fresh slice and may mutate it freely.
func KnownGapNoteKinds() []GapNoteKind {
	return slices.Clone(knownGapNoteKinds)
}

// OpenGapRequest establishes one evidence gap. Every payload is required and
// persisted verbatim (nothing is defaulted); DependencyProof is optional and
// only narrows the conservative pause set when it proves one capability
// independent with a path and per-edge evidence.
type OpenGapRequest struct {
	InstanceID string
	// ObjectKey is the stable object identity the missing evidence belongs to
	// (for example payment_intent:<id>, block:<height>, consumer:<group>).
	ObjectKey string
	// Scope is the JSONB range of the gap (chain/asset/business type/object
	// range). Required.
	Scope []byte
	// Timeline is the JSONB timeline of the observed interval. Required.
	Timeline []byte
	// ExistingEvidence is the JSONB record of what is already observable for
	// the object. Required.
	ExistingEvidence []byte
	// RequiredEvidence is the JSONB list of the external evidence/manual
	// confirmation needed to close the gap. Required.
	RequiredEvidence []byte
	// Risk is the JSONB risk payload (for example unproven_external_effect,
	// may_amplify). Required: an evidence gap without a recorded risk is not a
	// human-handling package. It is stored inside required_evidence (no risk
	// column exists in the 0001 schema) and round-trips through the record.
	Risk []byte
	// AffectedCapabilities is the at-least-one directly related capability
	// set. The persisted set is conservatively expanded with every
	// transitive dependent.
	AffectedCapabilities []Capability
	// DependencyProof optionally proves specific dependent capabilities
	// independent (a path ascending the frozen dependency matrix plus one
	// non-empty evidence reference per edge). An incomplete proof narrows
	// nothing.
	DependencyProof []byte
	// Owner is the responsibility holder recorded on the gap (person
	// reference). Required.
	Owner string
	// Actor is the authenticated principal recorded on every audit row.
	Actor string
	// OperationID is the command idempotency key.
	OperationID string
}

// CloseGapRequest closes one gap by new evidence.
type CloseGapRequest struct {
	InstanceID string
	GapID      string
	// ClosureEvidence is the new evidence; required and never defaulted.
	// Empty/blank refuses with ErrGapClosureEvidence.
	ClosureEvidence []byte
	Actor           string
	OperationID     string
	Reason          string
}

// EscalateGapRequest escalates one open gap: escalation is not closure and no
// risk acceptance exists. EscalationRef is required; the owner recorded at
// establishment is preserved as the responsibility holder.
type EscalateGapRequest struct {
	InstanceID    string
	GapID         string
	EscalationRef string
	Actor         string
	OperationID   string
	Reason        string
}

// GapNoteRequest records one audit-only note. Notes never change the gap
// state, never close it and never permit a release.
type GapNoteRequest struct {
	InstanceID  string
	GapID       string
	Kind        GapNoteKind
	Reason      string
	Actor       string
	OperationID string
}

// ReviewBudget is the configured bound of the bounded read-only review: the
// maximum number of review passes allowed on one gap. It is deployment
// configuration resolved by the caller (local test values are inputs only); a
// missing/zero value is not configured and refuses, never an unbounded review.
type ReviewBudget struct {
	MaxReads int
}

// GapReviewRequest performs one bounded read-only review pass. It reads only
// rows of its own instance (evidence, verification items, gaps, audit) and
// changes no gap/instance/approval/release state.
type GapReviewRequest struct {
	InstanceID  string
	GapID       string
	Budget      ReviewBudget
	Actor       string
	OperationID string
}

// Gaps is the evidence-gap service over a version-guarded control store. It
// holds no mutable state and touches only the control store.
type Gaps struct {
	store *controlstore.Store
}

// NewGaps builds the gap service over a *controlstore.Store
// (controlstore.NewStore enforces the T069 version guard, so there is no
// unguarded path). A nil store refuses.
func NewGaps(store *controlstore.Store) (*Gaps, error) {
	if store == nil {
		return nil, errors.New("evidence gaps require a controlstore.Store built by controlstore.NewStore")
	}
	return &Gaps{store: store}, nil
}

// ---------------------------------------------------------------------------
// Reads (the evidence package export surface)
// ---------------------------------------------------------------------------

// Get reads one persisted gap. found=false means the gap does not exist for
// the instance. The returned record is the complete FR-019 evidence package:
// object and scope, timeline, existing evidence, required external evidence,
// risk, affected capabilities, dependency proof, owner, escalation record and
// closure facts.
func (g *Gaps) Get(ctx context.Context, instanceID, gapID string) (Gap, bool, error) {
	if g == nil || g.store == nil {
		return Gap{}, false, errors.New("evidence gaps require a controlstore.Store built by controlstore.NewStore")
	}
	instance, err := gapInstanceID(instanceID)
	if err != nil {
		return Gap{}, false, err
	}
	id, err := gapGapID(gapID)
	if err != nil {
		return Gap{}, false, err
	}
	record, err := scanGap(g.store.Pool().QueryRow(ctx,
		`SELECT `+gapSelectColumns+` FROM recovery_gap WHERE instance_id = $1 AND gap_id = $2`,
		instance, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Gap{}, false, nil
	}
	if err != nil {
		return Gap{}, false, fmt.Errorf("read evidence gap %s: %w", id, err)
	}
	return record, true, nil
}

// List reads every gap of one instance in creation order. The result is the
// per-instance evidence package export used for manual handling.
func (g *Gaps) List(ctx context.Context, instanceID string) ([]Gap, error) {
	if g == nil || g.store == nil {
		return nil, errors.New("evidence gaps require a controlstore.Store built by controlstore.NewStore")
	}
	instance, err := gapInstanceID(instanceID)
	if err != nil {
		return nil, err
	}
	rows, err := g.store.Pool().Query(ctx,
		`SELECT `+gapSelectColumns+` FROM recovery_gap WHERE instance_id = $1 ORDER BY created_at, gap_id`,
		instance)
	if err != nil {
		return nil, fmt.Errorf("read evidence gaps of instance %s: %w", instance, err)
	}
	defer rows.Close()
	var out []Gap
	for rows.Next() {
		record, err := scanGap(rows)
		if err != nil {
			return nil, fmt.Errorf("scan evidence gap of instance %s: %w", instance, err)
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read evidence gaps of instance %s: %w", instance, err)
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Open
// ---------------------------------------------------------------------------

// gapPendingOpen is one validated, prepared gap establishment.
type gapPendingOpen struct {
	objectKey        string
	scope            []byte
	timeline         []byte
	existingEvidence []byte
	requiredEvidence []byte // merged with the risk payload
	affected         []Capability
	affectedNames    []string
	dependencyProof  []byte
	owner            string
	digest           string
}

// Open establishes one open evidence gap through the T013 generation protocol
// (MutationGapOpened): the accepted transaction appends the gap row and the
// ActionGapOpen audit row, then advances the evidence generation by exactly
// one. An open gap for the same object identity is left untouched and returned
// (closure only by new evidence); a closed/escalated one is history and a
// later gap is appended as a new row.
func (g *Gaps) Open(ctx context.Context, req OpenGapRequest) (Gap, error) {
	if g == nil || g.store == nil {
		return Gap{}, errors.New("evidence gaps require a controlstore.Store built by controlstore.NewStore")
	}
	actor := strings.TrimSpace(req.Actor)
	operationID := strings.TrimSpace(req.OperationID)
	instanceID, err := gapInstanceID(req.InstanceID)
	if err != nil {
		g.writeGapRefusal(ctx, req.InstanceID, actor, ActionGapOpen, "", err.Error(), operationID)
		return Gap{}, err
	}
	pending, err := gapPrepareOpen(req)
	if err != nil {
		g.writeGapRefusal(ctx, instanceID, actor, ActionGapOpen, "", err.Error(), operationID)
		return Gap{}, err
	}

	// Idempotent read-back: an accepted open under this operation_id returns
	// the recorded gap without a second row; a different request refuses with
	// a conflict and zero writes.
	if recorded, found, err := g.recordedGapOperation(ctx, instanceID, ActionGapOpen, operationID); err != nil {
		return Gap{}, err
	} else if found {
		if recorded.Digest != pending.digest {
			return Gap{}, fmt.Errorf("%w: operation_id %q was already used by a different gap open request",
				controlstore.ErrOperationConflict, operationID)
		}
		record, found, err := g.Get(ctx, instanceID, recorded.GapID)
		if err != nil {
			return Gap{}, err
		}
		if !found {
			return Gap{}, fmt.Errorf("evidence gap %s recorded under operation_id %q is missing", recorded.GapID, operationID)
		}
		return record, nil
	}

	token, err := CaptureEvidenceToken(ctx, g.store.Pool(), instanceID)
	if err != nil {
		return Gap{}, err
	}
	if token.State != "open" {
		g.writeGapRefusal(ctx, instanceID, actor, ActionGapOpen, "",
			"the instance is not open; evidence gaps are established on an open instance", operationID)
		return Gap{}, fmt.Errorf("%w: %s", controlstore.ErrInstanceNotOpen, token.InstanceID)
	}
	var kind string
	if err := g.store.Pool().QueryRow(ctx,
		`SELECT kind FROM recovery_instance WHERE instance_id = $1`, instanceID).Scan(&kind); err != nil {
		return Gap{}, fmt.Errorf("read recovery instance %s: %w", instanceID, err)
	}
	if kind != "recovery" {
		g.writeGapRefusal(ctx, instanceID, actor, ActionGapOpen, "",
			fmt.Sprintf("instance kind=%s; evidence gaps belong to recovery instances", kind), operationID)
		return Gap{}, fmt.Errorf("%w: instance %s has kind=%s; evidence gaps belong to recovery instances",
			ErrGapInput, instanceID, kind)
	}

	if existing, found, err := g.openGapForObject(ctx, instanceID, pending.objectKey); err != nil {
		return Gap{}, err
	} else if found {
		// An open gap for this object identity is left untouched (closure only
		// by new evidence); its persisted package is returned. This mirrors the
		// verification path's insertVerificationGap so both writers agree.
		return existing, nil
	}

	var established Gap
	outcome, err := CommitEvidenceWrite(ctx, g.store, EvidenceWriteRequest{
		InstanceID:   instanceID,
		Token:        token,
		Kind:         MutationGapOpened,
		Actor:        actor,
		Reason:       "evidence gap established for " + pending.objectKey,
		OperationID:  operationID,
		ResultDigest: []byte(pending.digest),
		Apply: func(ctx context.Context, tx pgx.Tx, accepted EvidenceToken) error {
			record, err := gapInsert(ctx, tx, accepted.InstanceID, pending)
			if err != nil {
				return err
			}
			established = record
			target, err := json.Marshal(map[string]any{
				"gap_id":     record.GapID,
				"object_key": record.ObjectKey,
			})
			if err != nil {
				return fmt.Errorf("encode gap open audit target: %w", err)
			}
			detail, err := json.Marshal(map[string]any{
				"operation_id":          operationID,
				"request_digest":        pending.digest,
				"owner":                 record.Owner,
				"affected_capabilities": gapCapabilityNames(record.AffectedCapabilities),
				"dependency_proof":      len(record.DependencyProof) > 0,
				"reason":                "evidence gap established; every affected capability stays paused until new evidence closes the gap or an independence proof is approved",
			})
			if err != nil {
				return fmt.Errorf("encode gap open audit detail: %w", err)
			}
			generation := accepted.Generation
			return controlstore.WriteAudit(ctx, tx, controlstore.AuditRecord{
				InstanceID:         accepted.InstanceID,
				Actor:              actor,
				Action:             ActionGapOpen,
				Target:             target,
				Detail:             detail,
				Result:             controlstore.AuditOK,
				EvidenceGeneration: &generation,
				OperationID:        operationID,
			})
		},
	})
	if err != nil {
		return Gap{}, err
	}
	if outcome.Discarded {
		return Gap{}, fmt.Errorf(
			"evidence gap establishment was discarded because the evidence token changed (captured generation=%d, observed generation=%d); re-read the gaps and retry",
			token.Generation, outcome.Token.Generation)
	}
	return established, nil
}

// gapPrepareOpen validates the request and derives the persisted pause set,
// the merged required-evidence payload and the canonical request digest.
func gapPrepareOpen(req OpenGapRequest) (gapPendingOpen, error) {
	if err := gapValidateOpen(req); err != nil {
		return gapPendingOpen{}, err
	}
	requiredEvidence, err := mergeGapRiskPayload(req.RequiredEvidence, req.Risk)
	if err != nil {
		return gapPendingOpen{}, fmt.Errorf("%w: %s", ErrGapInput, err)
	}
	affected, err := gapPauseSet(req.AffectedCapabilities, req.Risk, req.DependencyProof)
	if err != nil {
		return gapPendingOpen{}, err
	}
	pending := gapPendingOpen{
		objectKey:        strings.TrimSpace(req.ObjectKey),
		scope:            req.Scope,
		timeline:         req.Timeline,
		existingEvidence: req.ExistingEvidence,
		requiredEvidence: requiredEvidence,
		affected:         affected,
		dependencyProof:  req.DependencyProof,
		owner:            strings.TrimSpace(req.Owner),
	}
	if len(pending.dependencyProof) == 0 {
		pending.dependencyProof = nil
	}
	pending.affectedNames = gapCapabilityNames(affected)
	pending.digest = gapOpenDigest(req, affected)
	return pending, nil
}

// gapValidateOpen enforces the FR-019 bundle contract: every component is
// required, payloads must be JSON, every named capability must be known and
// the actor/operation_id must be present. Nothing is filled in from a default.
func gapValidateOpen(req OpenGapRequest) error {
	if strings.TrimSpace(req.ObjectKey) == "" {
		return fmt.Errorf("%w: object_key is required; the gap must name the object whose evidence is missing", ErrGapInput)
	}
	for _, part := range []struct {
		name string
		raw  []byte
	}{
		{"scope", req.Scope},
		{"timeline", req.Timeline},
		{"existing_evidence", req.ExistingEvidence},
		{"required_evidence", req.RequiredEvidence},
		{"risk", req.Risk},
	} {
		if len(bytes.TrimSpace(part.raw)) == 0 || !json.Valid(part.raw) {
			return fmt.Errorf("%w: the FR-019 %s payload is required and must be valid JSON", ErrGapInput, part.name)
		}
	}
	if len(req.DependencyProof) > 0 && !json.Valid(req.DependencyProof) {
		return fmt.Errorf("%w: dependency_proof must be valid JSON (a malformed proof proves nothing)", ErrGapInput)
	}
	if len(req.AffectedCapabilities) == 0 {
		return fmt.Errorf("%w: affected_capabilities is required and must contain at least the directly related capability", ErrGapInput)
	}
	for _, capability := range req.AffectedCapabilities {
		if !capability.Known() {
			return fmt.Errorf("%w: affected capability %q is outside the closed seven-capability set", ErrGapInput, capability)
		}
	}
	if strings.TrimSpace(req.Owner) == "" {
		return fmt.Errorf("%w: owner is required; an evidence gap without recorded responsibility is not a human-handling package", ErrGapInput)
	}
	if strings.TrimSpace(req.Actor) == "" {
		return fmt.Errorf("%w: actor is required; the authenticated principal is recorded on every audit row", ErrGapInput)
	}
	if strings.TrimSpace(req.OperationID) == "" {
		return fmt.Errorf("%w: operation_id is required (idempotent commands)", ErrGapInput)
	}
	return nil
}

// gapInsert appends one open recovery_gap row inside the generation protocol's
// transaction.
func gapInsert(ctx context.Context, tx pgx.Tx, instanceID string, pending gapPendingOpen) (Gap, error) {
	var proof any
	if len(pending.dependencyProof) > 0 {
		proof = pending.dependencyProof
	}
	record, err := scanGap(tx.QueryRow(ctx, `
INSERT INTO recovery_gap
    (gap_id, instance_id, object_key, scope, timeline, existing_evidence,
     required_evidence, affected_capabilities, dependency_proof, state, owner)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'open', $10)
RETURNING `+gapSelectColumns,
		uuid.NewString(), instanceID, pending.objectKey, pending.scope, pending.timeline,
		pending.existingEvidence, pending.requiredEvidence, pending.affectedNames, proof, pending.owner))
	if err != nil {
		return Gap{}, fmt.Errorf("establish evidence gap for %s: %w", pending.objectKey, err)
	}
	return record, nil
}

// openGapForObject reads the open gap of one object identity, when one exists.
func (g *Gaps) openGapForObject(ctx context.Context, instanceID, objectKey string) (Gap, bool, error) {
	record, err := scanGap(g.store.Pool().QueryRow(ctx,
		`SELECT `+gapSelectColumns+`
		 FROM recovery_gap
		 WHERE instance_id = $1 AND object_key = $2 AND state = 'open'
		 ORDER BY created_at, gap_id
		 LIMIT 1`, instanceID, objectKey))
	if errors.Is(err, pgx.ErrNoRows) {
		return Gap{}, false, nil
	}
	if err != nil {
		return Gap{}, false, fmt.Errorf("read open evidence gap for %s: %w", objectKey, err)
	}
	return record, true, nil
}

// ---------------------------------------------------------------------------
// Conservative pause set and dependency proofs
// ---------------------------------------------------------------------------

// gapPauseSet derives the persisted affected set: the conservative
// amplification closure of the directly related capabilities plus every known
// capability named by the risk payload's may_amplify hints, minus the
// capabilities a valid dependency proof shows independent. Directly related
// capabilities and risk-flagged ones are never subtracted (a proof never
// overrides the operator's own statement).
func gapPauseSet(direct []Capability, risk, proof []byte) ([]Capability, error) {
	if len(direct) == 0 {
		return nil, fmt.Errorf("%w: affected_capabilities is required", ErrGapInput)
	}
	directSet := make(map[Capability]bool, len(direct))
	seeds := make([]Capability, 0, len(direct)+2)
	for _, capability := range direct {
		if !capability.Known() {
			return nil, fmt.Errorf("%w: affected capability %q is outside the closed seven-capability set", ErrGapInput, capability)
		}
		if !directSet[capability] {
			directSet[capability] = true
			seeds = append(seeds, capability)
		}
	}
	amplified := gapRiskAmplifications(risk)
	amplifiedSet := make(map[Capability]bool, len(amplified))
	for _, capability := range amplified {
		if !amplifiedSet[capability] {
			amplifiedSet[capability] = true
			seeds = append(seeds, capability)
		}
	}
	paused, err := conservativePauseSet(seeds)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrGapInput, err)
	}
	independent := gapProvenIndependent(direct, proof)
	out := make([]Capability, 0, len(paused))
	for _, capability := range paused {
		if independent[capability] && !directSet[capability] && !amplifiedSet[capability] {
			continue
		}
		out = append(out, capability)
	}
	return out, nil
}

// gapRiskAmplifications extracts the known capability names of the risk
// payload's may_amplify hints. Unknown names prove nothing and are ignored (a
// free-text hint can never introduce a capability outside the closed set);
// they only ever widen the pause set.
func gapRiskAmplifications(risk []byte) []Capability {
	if len(bytes.TrimSpace(risk)) == 0 {
		return nil
	}
	var payload struct {
		MayAmplify []string `json:"may_amplify"`
	}
	if err := json.Unmarshal(risk, &payload); err != nil {
		return nil
	}
	var out []Capability
	for _, raw := range payload.MayAmplify {
		capability := Capability(strings.TrimSpace(raw))
		if capability.Known() && !slices.Contains(out, capability) {
			out = append(out, capability)
		}
	}
	return out
}

// gapIndependenceClaim is one dependency-proof entry: a capability claimed
// independent of the gap, the path ascending the frozen requires_capabilities
// matrix from a directly affected capability to it, and the evidence
// reference of every edge on that path.
type gapIndependenceClaim struct {
	Capability string                `json:"capability"`
	Path       []string              `json:"path"`
	Edges      []gapIndependenceEdge `json:"edges"`
}

// gapIndependenceEdge is one per-edge evidence reference of a claim.
type gapIndependenceEdge struct {
	From        string `json:"from"`
	To          string `json:"to"`
	EvidenceRef string `json:"evidence_ref"`
}

// gapProvenIndependent parses a dependency proof and returns the capabilities
// it proves independent. Anything malformed, empty or incomplete proves
// nothing (an unreadable proof must never narrow the pause set).
func gapProvenIndependent(direct []Capability, proof []byte) map[Capability]bool {
	independent := make(map[Capability]bool)
	if len(bytes.TrimSpace(proof)) == 0 {
		return independent
	}
	var payload struct {
		Independent []gapIndependenceClaim `json:"independent"`
	}
	if err := json.Unmarshal(proof, &payload); err != nil {
		return independent
	}
	roots := make(map[Capability]bool, len(direct))
	for _, capability := range direct {
		roots[capability] = true
	}
	for _, claim := range payload.Independent {
		capability := Capability(strings.TrimSpace(claim.Capability))
		if !capability.Known() || roots[capability] {
			continue
		}
		if !gapProofPathProves(capability, claim, roots) {
			continue
		}
		independent[capability] = true
	}
	return independent
}

// gapProofPathProves reports whether one claim carries a complete path:
// starting at a directly affected capability, every step depends on the
// previous one along a direct requires_capabilities edge (the amplification
// direction, never reversed), and every edge carries a non-empty evidence
// reference.
func gapProofPathProves(capability Capability, claim gapIndependenceClaim, roots map[Capability]bool) bool {
	if len(claim.Path) < 2 || len(claim.Path) > len(knownCapabilities) {
		return false
	}
	path := make([]Capability, 0, len(claim.Path))
	for _, raw := range claim.Path {
		step := Capability(strings.TrimSpace(raw))
		if !step.Known() {
			return false
		}
		path = append(path, step)
	}
	if !roots[path[0]] || path[len(path)-1] != capability {
		return false
	}
	for i := 1; i < len(path); i++ {
		// path[i] must directly require path[i-1]: the omitted evidence of the
		// gap can only propagate downward through this frozen edge.
		if !slices.Contains(requiresCapabilities[path[i]], path[i-1]) {
			return false
		}
		if !gapProofEdgeBacked(claim.Edges, path[i-1], path[i]) {
			return false
		}
	}
	return true
}

// gapProofEdgeBacked reports whether the edge (from -> to) carries a
// non-empty evidence reference in the claim.
func gapProofEdgeBacked(edges []gapIndependenceEdge, from, to Capability) bool {
	for _, edge := range edges {
		if strings.TrimSpace(edge.From) != string(from) || strings.TrimSpace(edge.To) != string(to) {
			continue
		}
		if strings.TrimSpace(edge.EvidenceRef) == "" {
			continue
		}
		return true
	}
	return false
}

// ---------------------------------------------------------------------------
// Close
// ---------------------------------------------------------------------------

// Close closes one open or escalated gap by new evidence. It runs through the
// T013 generation protocol (MutationGapClosed): the accepted transaction
// updates the row with the closer/time/evidence and appends the ActionGapClose
// audit row, then advances the evidence generation by exactly one. Without new
// evidence nothing is written but the refusal audit row.
func (g *Gaps) Close(ctx context.Context, req CloseGapRequest) (Gap, error) {
	if g == nil || g.store == nil {
		return Gap{}, errors.New("evidence gaps require a controlstore.Store built by controlstore.NewStore")
	}
	if err := validateGapCredentialInputs(req.InstanceID, req.GapID, req.Actor, req.OperationID, req.Reason); err != nil {
		return Gap{}, err
	}
	// Blank evidence is "no new evidence" (ErrGapClosureEvidence), not a
	// credential-shape refusal: the sentinel semantics come first, exactly as
	// the closure discipline requires. Only non-blank evidence is checked for
	// credential-shaped material before it can become a durable digest.
	if len(bytes.TrimSpace(req.ClosureEvidence)) > 0 {
		if err := controlstore.ValidateCredentialJSON("closure_evidence", req.ClosureEvidence); err != nil {
			return Gap{}, err
		}
	}
	instanceID, err := gapInstanceID(req.InstanceID)
	if err != nil {
		g.writeGapRefusal(ctx, req.InstanceID, req.Actor, ActionGapClose, "", err.Error(), req.OperationID)
		return Gap{}, err
	}
	gapID, err := gapGapID(req.GapID)
	if err != nil {
		g.writeGapRefusal(ctx, instanceID, req.Actor, ActionGapClose, "", err.Error(), req.OperationID)
		return Gap{}, err
	}
	actor := strings.TrimSpace(req.Actor)
	operationID := strings.TrimSpace(req.OperationID)
	if actor == "" {
		return Gap{}, fmt.Errorf("%w: actor is required; the authenticated principal is recorded on the closure", ErrGapInput)
	}
	if operationID == "" {
		return Gap{}, fmt.Errorf("%w: operation_id is required (idempotent commands)", ErrGapInput)
	}
	evidence := bytes.TrimSpace(req.ClosureEvidence)
	if len(evidence) == 0 {
		reason := "closure requires new evidence: a timeout, exhausted attempts and a human acknowledgement are audit notes, never closure (FR-019)"
		g.writeGapRefusal(ctx, instanceID, actor, ActionGapClose, gapID, reason, operationID)
		return Gap{}, fmt.Errorf("%w: %s", ErrGapClosureEvidence, reason)
	}
	if !json.Valid(evidence) {
		g.writeGapRefusal(ctx, instanceID, actor, ActionGapClose, gapID, "closure_evidence is not valid JSON", operationID)
		return Gap{}, fmt.Errorf("%w: closure_evidence must be a JSON payload", ErrGapInput)
	}
	reason := strings.TrimSpace(req.Reason)
	digest := gapDigest(
		[]byte(ActionGapClose), []byte(instanceID), []byte(gapID), evidence,
		[]byte(actor), []byte(reason),
	)

	if recorded, found, err := g.recordedGapOperation(ctx, instanceID, ActionGapClose, operationID); err != nil {
		return Gap{}, err
	} else if found {
		if recorded.Digest != digest {
			return Gap{}, fmt.Errorf("%w: operation_id %q was already used by a different gap close request",
				controlstore.ErrOperationConflict, operationID)
		}
		record, found, err := g.Get(ctx, instanceID, recorded.GapID)
		if err != nil {
			return Gap{}, err
		}
		if !found {
			return Gap{}, fmt.Errorf("evidence gap %s recorded under operation_id %q is missing", recorded.GapID, operationID)
		}
		return record, nil
	}

	if _, found, err := g.Get(ctx, instanceID, gapID); err != nil {
		return Gap{}, err
	} else if !found {
		g.writeGapRefusal(ctx, instanceID, actor, ActionGapClose, gapID, "the gap does not exist", operationID)
		return Gap{}, fmt.Errorf("%w: %s", ErrGapNotFound, gapID)
	}

	token, err := CaptureEvidenceToken(ctx, g.store.Pool(), instanceID)
	if err != nil {
		return Gap{}, err
	}
	if token.State != "open" {
		g.writeGapRefusal(ctx, instanceID, actor, ActionGapClose, gapID, "the instance is not open", operationID)
		return Gap{}, fmt.Errorf("%w: %s", controlstore.ErrInstanceNotOpen, token.InstanceID)
	}

	var closed Gap
	outcome, err := CommitEvidenceWrite(ctx, g.store, EvidenceWriteRequest{
		InstanceID:   instanceID,
		Token:        token,
		Kind:         MutationGapClosed,
		Actor:        actor,
		Reason:       reason,
		OperationID:  operationID,
		ResultDigest: []byte(digest),
		Apply: func(ctx context.Context, tx pgx.Tx, accepted EvidenceToken) error {
			var state string
			err := tx.QueryRow(ctx,
				`SELECT state FROM recovery_gap WHERE instance_id = $1 AND gap_id = $2 FOR UPDATE`,
				accepted.InstanceID, gapID).Scan(&state)
			if errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("%w: %s", ErrGapNotFound, gapID)
			}
			if err != nil {
				return fmt.Errorf("read evidence gap %s for closure: %w", gapID, err)
			}
			if GapState(state) == GapStateClosed {
				return fmt.Errorf("%w: evidence gap %s is already closed; a closed gap is never rewritten (a later gap is appended as a new row)",
					ErrGapTransition, gapID)
			}
			record, err := scanGap(tx.QueryRow(ctx, `
UPDATE recovery_gap
   SET state = 'closed', closed_by = $3, closed_at = now(), closure_evidence = $4
 WHERE instance_id = $1 AND gap_id = $2
RETURNING `+gapSelectColumns,
				accepted.InstanceID, gapID, actor, evidence))
			if err != nil {
				return fmt.Errorf("close evidence gap %s: %w", gapID, err)
			}
			closed = record
			target, err := json.Marshal(map[string]any{"gap_id": gapID, "object_key": record.ObjectKey})
			if err != nil {
				return fmt.Errorf("encode gap close audit target: %w", err)
			}
			detail, err := json.Marshal(map[string]any{
				"operation_id":   operationID,
				"request_digest": digest,
				"reason":         reason,
			})
			if err != nil {
				return fmt.Errorf("encode gap close audit detail: %w", err)
			}
			generation := accepted.Generation
			return controlstore.WriteAudit(ctx, tx, controlstore.AuditRecord{
				InstanceID:         accepted.InstanceID,
				Actor:              actor,
				Action:             ActionGapClose,
				Target:             target,
				Detail:             detail,
				Result:             controlstore.AuditOK,
				EvidenceGeneration: &generation,
				OperationID:        operationID,
			})
		},
	})
	if err != nil {
		if errors.Is(err, ErrGapTransition) || errors.Is(err, ErrGapNotFound) {
			g.writeGapRefusal(ctx, instanceID, actor, ActionGapClose, gapID, err.Error(), operationID)
		}
		return Gap{}, err
	}
	if outcome.Discarded {
		return Gap{}, fmt.Errorf(
			"evidence gap closure was discarded because the evidence token changed (captured generation=%d, observed generation=%d); re-read the gap and retry",
			token.Generation, outcome.Token.Generation)
	}
	return closed, nil
}

// ---------------------------------------------------------------------------
// Escalate
// ---------------------------------------------------------------------------

// Escalate moves one open gap to escalated with a recorded reference.
// Escalation is not closure: the affected capabilities stay paused, the
// closure path still requires new evidence and no risk acceptance is
// delivered. It changes no evidence generation and records one ActionGapEscalate
// audit row (accepted or refused). The owner recorded at establishment is
// preserved as the responsibility holder.
func (g *Gaps) Escalate(ctx context.Context, req EscalateGapRequest) (Gap, error) {
	if g == nil || g.store == nil {
		return Gap{}, errors.New("evidence gaps require a controlstore.Store built by controlstore.NewStore")
	}
	if err := validateGapCredentialInputs(req.InstanceID, req.GapID, req.Actor, req.OperationID, req.EscalationRef, req.Reason); err != nil {
		return Gap{}, err
	}
	instanceID, err := gapInstanceID(req.InstanceID)
	if err != nil {
		g.writeGapRefusal(ctx, req.InstanceID, req.Actor, ActionGapEscalate, "", err.Error(), req.OperationID)
		return Gap{}, err
	}
	gapID, err := gapGapID(req.GapID)
	if err != nil {
		g.writeGapRefusal(ctx, instanceID, req.Actor, ActionGapEscalate, "", err.Error(), req.OperationID)
		return Gap{}, err
	}
	actor := strings.TrimSpace(req.Actor)
	operationID := strings.TrimSpace(req.OperationID)
	if actor == "" {
		return Gap{}, fmt.Errorf("%w: actor is required; the authenticated principal is recorded on the escalation", ErrGapInput)
	}
	if operationID == "" {
		return Gap{}, fmt.Errorf("%w: operation_id is required (idempotent commands)", ErrGapInput)
	}
	ref := strings.TrimSpace(req.EscalationRef)
	if ref == "" {
		reason := "escalation requires a recorded escalation reference; an unnamed escalation cannot be followed up"
		g.writeGapRefusal(ctx, instanceID, actor, ActionGapEscalate, gapID, reason, operationID)
		return Gap{}, fmt.Errorf("%w: %s", ErrGapEscalationRef, reason)
	}
	reason := strings.TrimSpace(req.Reason)
	digest := gapDigest(
		[]byte(ActionGapEscalate), []byte(instanceID), []byte(gapID), []byte(ref),
		[]byte(actor), []byte(reason),
	)

	if recorded, found, err := g.recordedGapOperation(ctx, instanceID, ActionGapEscalate, operationID); err != nil {
		return Gap{}, err
	} else if found {
		if recorded.Digest != digest {
			return Gap{}, fmt.Errorf("%w: operation_id %q was already used by a different gap escalation request",
				controlstore.ErrOperationConflict, operationID)
		}
		record, found, err := g.Get(ctx, instanceID, recorded.GapID)
		if err != nil {
			return Gap{}, err
		}
		if !found {
			return Gap{}, fmt.Errorf("evidence gap %s recorded under operation_id %q is missing", recorded.GapID, operationID)
		}
		return record, nil
	}

	tx, err := g.store.Pool().Begin(ctx)
	if err != nil {
		return Gap{}, fmt.Errorf("begin gap escalation: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Refusals decided inside the transaction are recorded in the same
	// transaction (the caller-managed helper never writes outside while the
	// instance row lock is held).
	refuse := func(wrapped error, why string) (Gap, error) {
		detail, err := json.Marshal(map[string]any{
			"gap_id": gapID,
			"reason": boundedEvidenceDetail(why),
		})
		if err != nil {
			return Gap{}, fmt.Errorf("encode gap escalation refusal detail: %w", err)
		}
		if err := controlstore.WriteAudit(ctx, tx, controlstore.AuditRecord{
			InstanceID:  instanceID,
			Actor:       actor,
			Action:      ActionGapEscalate,
			Detail:      detail,
			Result:      controlstore.AuditRefused,
			OperationID: operationID,
		}); err != nil {
			return Gap{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return Gap{}, fmt.Errorf("commit gap escalation refusal audit: %w", err)
		}
		return Gap{}, wrapped
	}

	locked, err := controlstore.LockInstance(ctx, tx, instanceID)
	if err != nil {
		return Gap{}, err
	}
	if locked.State != "open" {
		return refuse(fmt.Errorf("%w: %s", controlstore.ErrInstanceNotOpen, locked.InstanceID),
			"the instance is not open")
	}
	record, err := scanGap(tx.QueryRow(ctx,
		`SELECT `+gapSelectColumns+` FROM recovery_gap WHERE instance_id = $1 AND gap_id = $2 FOR UPDATE`,
		instanceID, gapID))
	if errors.Is(err, pgx.ErrNoRows) {
		return refuse(fmt.Errorf("%w: %s", ErrGapNotFound, gapID), "the gap does not exist")
	}
	if err != nil {
		return Gap{}, fmt.Errorf("read evidence gap %s for escalation: %w", gapID, err)
	}
	if record.State != GapStateOpen {
		return refuse(fmt.Errorf("%w: evidence gap %s is state=%s; only an open gap escalates and escalation is not closure either",
			ErrGapTransition, gapID, record.State),
			fmt.Sprintf("state=%s; only an open gap escalates", record.State))
	}
	updated, err := scanGap(tx.QueryRow(ctx, `
UPDATE recovery_gap
   SET state = 'escalated', escalation_ref = $3
 WHERE instance_id = $1 AND gap_id = $2
RETURNING `+gapSelectColumns,
		instanceID, gapID, ref))
	if err != nil {
		return Gap{}, fmt.Errorf("escalate evidence gap %s: %w", gapID, err)
	}
	target, err := json.Marshal(map[string]any{
		"gap_id":         gapID,
		"escalation_ref": ref,
		"owner":          updated.Owner,
	})
	if err != nil {
		return Gap{}, fmt.Errorf("encode gap escalation audit target: %w", err)
	}
	detail, err := json.Marshal(map[string]any{
		"operation_id":              operationID,
		"request_digest":            digest,
		"reason":                    reason,
		"closure":                   false,
		"risk_acceptance_delivered": false,
	})
	if err != nil {
		return Gap{}, fmt.Errorf("encode gap escalation audit detail: %w", err)
	}
	generation := locked.EvidenceGeneration
	if err := controlstore.WriteAudit(ctx, tx, controlstore.AuditRecord{
		InstanceID:         instanceID,
		Actor:              actor,
		Action:             ActionGapEscalate,
		Target:             target,
		Detail:             detail,
		Result:             controlstore.AuditOK,
		EvidenceGeneration: &generation,
		OperationID:        operationID,
	}); err != nil {
		return Gap{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Gap{}, fmt.Errorf("commit gap escalation: %w", err)
	}
	return updated, nil
}

// ---------------------------------------------------------------------------
// Note
// ---------------------------------------------------------------------------

// Note records one audit-only note (timeout, exhausted attempts or human
// acknowledgement). It changes no gap state, advances no generation and
// permits no release: a note is knowledge, not evidence.
func (g *Gaps) Note(ctx context.Context, req GapNoteRequest) (Gap, error) {
	if g == nil || g.store == nil {
		return Gap{}, errors.New("evidence gaps require a controlstore.Store built by controlstore.NewStore")
	}
	if err := validateGapCredentialInputs(req.InstanceID, req.GapID, req.Actor, req.OperationID, string(req.Kind), req.Reason); err != nil {
		return Gap{}, err
	}
	instanceID, err := gapInstanceID(req.InstanceID)
	if err != nil {
		return Gap{}, err
	}
	gapID, err := gapGapID(req.GapID)
	if err != nil {
		return Gap{}, err
	}
	actor := strings.TrimSpace(req.Actor)
	operationID := strings.TrimSpace(req.OperationID)
	if !req.Kind.Known() {
		return Gap{}, fmt.Errorf("%w: note kind %q is outside the closed timeout|attempts_exhausted|acknowledged set",
			ErrGapInput, req.Kind)
	}
	if actor == "" {
		return Gap{}, fmt.Errorf("%w: actor is required; the authenticated principal is recorded on the note", ErrGapInput)
	}
	if operationID == "" {
		return Gap{}, fmt.Errorf("%w: operation_id is required (idempotent commands)", ErrGapInput)
	}
	reason := strings.TrimSpace(req.Reason)
	if reason == "" {
		return Gap{}, fmt.Errorf("%w: a note requires its audit reason", ErrGapInput)
	}
	digest := gapDigest(
		[]byte(ActionGapNote), []byte(instanceID), []byte(gapID),
		[]byte(req.Kind), []byte(reason), []byte(actor),
	)

	if recorded, found, err := g.recordedGapOperation(ctx, instanceID, ActionGapNote, operationID); err != nil {
		return Gap{}, err
	} else if found {
		if recorded.Digest != digest {
			return Gap{}, fmt.Errorf("%w: operation_id %q was already used by a different gap note",
				controlstore.ErrOperationConflict, operationID)
		}
		record, found, err := g.Get(ctx, instanceID, recorded.GapID)
		if err != nil {
			return Gap{}, err
		}
		if !found {
			return Gap{}, fmt.Errorf("evidence gap %s recorded under operation_id %q is missing", recorded.GapID, operationID)
		}
		return record, nil
	}

	record, found, err := g.Get(ctx, instanceID, gapID)
	if err != nil {
		return Gap{}, err
	}
	if !found {
		g.writeGapRefusal(ctx, instanceID, actor, ActionGapNote, gapID, "the gap does not exist", operationID)
		return Gap{}, fmt.Errorf("%w: %s", ErrGapNotFound, gapID)
	}
	target, err := json.Marshal(map[string]any{"gap_id": gapID, "kind": string(req.Kind)})
	if err != nil {
		return Gap{}, fmt.Errorf("encode gap note audit target: %w", err)
	}
	detail, err := json.Marshal(map[string]any{
		"operation_id":    operationID,
		"request_digest":  digest,
		"reason":          reason,
		"state":           string(record.State),
		"state_changed":   false,
		"closure":         false,
		"release_granted": false,
	})
	if err != nil {
		return Gap{}, fmt.Errorf("encode gap note audit detail: %w", err)
	}
	if err := controlstore.WriteAudit(ctx, g.store.Pool(), controlstore.AuditRecord{
		InstanceID:  instanceID,
		Actor:       actor,
		Action:      ActionGapNote,
		Target:      target,
		Detail:      detail,
		Result:      controlstore.AuditOK,
		OperationID: operationID,
	}); err != nil {
		return Gap{}, err
	}
	return record, nil
}

// validateGapCredentialInputs rejects recognizable credential material in
// every caller-controlled value that may become a durable identifier, state
// value, or audit field. It is intentionally applied before canonicalization.
func validateGapCredentialInputs(values ...string) error {
	for _, value := range values {
		if err := controlstore.ValidateCredentialText("gap input", value); err != nil {
			return err
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Bounded read-only review
// ---------------------------------------------------------------------------

// gapReviewSummary is the bounded per-instance row census one review pass
// reads. Each field is an aggregate over rows of the reviewed instance only;
// no unbounded scan is performed.
type gapReviewSummary struct {
	EvidenceRecords   int64
	VerificationItems int64
	Gaps              int64
	AuditRows         int64
}

// gapReviewSummarySQL is the single bounded read of one review pass: aggregates
// over the reviewed instance's evidence/verification/gap/audit rows (the F13
// readable range). The instance-id predicate on every subquery is what bounds
// it; no full-table scan is issued.
const gapReviewSummarySQL = `
SELECT
    (SELECT count(*) FROM recovery_evidence WHERE instance_id = $1),
    (SELECT count(*) FROM recovery_verification_item WHERE instance_id = $1),
    (SELECT count(*) FROM recovery_gap WHERE instance_id = $1),
    (SELECT count(*) FROM recovery_audit WHERE instance_id = $1)`

// Review performs one bounded read-only review pass over the gap's instance.
// The pass reads aggregates of the instance's evidence, verification items,
// gaps and audit rows and appends one ActionGapReview audit row; it changes no
// gap/instance/approval/release state. The configured budget bounds the number
// of passes per gap: a missing/zero or exhausted budget refuses with
// ErrReviewBudgetExhausted, is audited and changes nothing. Timeout and
// exhaustion are never closure or a release permission.
func (g *Gaps) Review(ctx context.Context, req GapReviewRequest) (Gap, error) {
	if g == nil || g.store == nil {
		return Gap{}, errors.New("evidence gaps require a controlstore.Store built by controlstore.NewStore")
	}
	instanceID, err := gapInstanceID(req.InstanceID)
	if err != nil {
		return Gap{}, err
	}
	gapID, err := gapGapID(req.GapID)
	if err != nil {
		return Gap{}, err
	}
	actor := strings.TrimSpace(req.Actor)
	operationID := strings.TrimSpace(req.OperationID)
	if actor == "" {
		return Gap{}, fmt.Errorf("%w: actor is required; the authenticated principal is recorded on the review", ErrGapInput)
	}
	if operationID == "" {
		return Gap{}, fmt.Errorf("%w: operation_id is required (idempotent commands)", ErrGapInput)
	}

	record, found, err := g.Get(ctx, instanceID, gapID)
	if err != nil {
		return Gap{}, err
	}
	if !found {
		g.writeGapRefusal(ctx, instanceID, actor, ActionGapReview, gapID, "the gap does not exist", operationID)
		return Gap{}, fmt.Errorf("%w: %s", ErrGapNotFound, gapID)
	}
	digest := gapDigest(
		[]byte(ActionGapReview), []byte(instanceID), []byte(gapID),
		[]byte(strconv.Itoa(req.Budget.MaxReads)), []byte(actor),
	)

	if recorded, found, err := g.recordedGapOperation(ctx, instanceID, ActionGapReview, operationID); err != nil {
		return Gap{}, err
	} else if found {
		if recorded.Digest != digest {
			return Gap{}, fmt.Errorf("%w: operation_id %q was already used by a different gap review request",
				controlstore.ErrOperationConflict, operationID)
		}
		return record, nil
	}

	consumed, err := g.gapReviewConsumed(ctx, instanceID, gapID)
	if err != nil {
		return Gap{}, err
	}
	if req.Budget.MaxReads <= 0 || consumed+1 > req.Budget.MaxReads {
		reason := fmt.Sprintf(
			"the bounded read-only review budget is not configured or is exhausted (accepted passes=%d, configured=%d); a timeout, an exhausted budget and human knowledge are never closure or a release permission",
			consumed, req.Budget.MaxReads)
		g.writeGapRefusal(ctx, instanceID, actor, ActionGapReview, gapID, reason, operationID)
		return Gap{}, fmt.Errorf("%w: accepted passes=%d, configured=%d", ErrReviewBudgetExhausted, consumed, req.Budget.MaxReads)
	}

	summary, err := g.gapReviewSummary(ctx, instanceID)
	if err != nil {
		return Gap{}, err
	}
	target, err := json.Marshal(map[string]any{"gap_id": gapID})
	if err != nil {
		return Gap{}, fmt.Errorf("encode gap review audit target: %w", err)
	}
	detail, err := json.Marshal(map[string]any{
		"operation_id":       operationID,
		"request_digest":     digest,
		"budget":             req.Budget.MaxReads,
		"reads":              1,
		"consumed_before":    consumed,
		"evidence_records":   summary.EvidenceRecords,
		"verification_items": summary.VerificationItems,
		"gaps":               summary.Gaps,
		"audit_rows":         summary.AuditRows,
		"state_changed":      false,
		"closure":            false,
		"release_granted":    false,
	})
	if err != nil {
		return Gap{}, fmt.Errorf("encode gap review audit detail: %w", err)
	}
	if err := controlstore.WriteAudit(ctx, g.store.Pool(), controlstore.AuditRecord{
		InstanceID:  instanceID,
		Actor:       actor,
		Action:      ActionGapReview,
		Target:      target,
		Detail:      detail,
		Result:      controlstore.AuditOK,
		OperationID: operationID,
	}); err != nil {
		return Gap{}, err
	}
	return record, nil
}

// gapReviewConsumed counts the accepted review passes already recorded for one
// gap (the budget accounting anchor; refused reviews consume nothing).
func (g *Gaps) gapReviewConsumed(ctx context.Context, instanceID, gapID string) (int, error) {
	var count int
	if err := g.store.Pool().QueryRow(ctx, `
SELECT count(*)
FROM recovery_audit
WHERE instance_id = $1 AND action = $2 AND result = 'ok' AND target->>'gap_id' = $3`,
		instanceID, ActionGapReview, gapID).Scan(&count); err != nil {
		return 0, fmt.Errorf("read accepted reviews of gap %s: %w", gapID, err)
	}
	return count, nil
}

// gapReviewSummary runs the single bounded aggregate read of one review pass.
func (g *Gaps) gapReviewSummary(ctx context.Context, instanceID string) (gapReviewSummary, error) {
	var summary gapReviewSummary
	if err := g.store.Pool().QueryRow(ctx, gapReviewSummarySQL, instanceID).Scan(
		&summary.EvidenceRecords, &summary.VerificationItems, &summary.Gaps, &summary.AuditRows); err != nil {
		return gapReviewSummary{}, fmt.Errorf("read review summary of instance %s: %w", instanceID, err)
	}
	return summary, nil
}

// ---------------------------------------------------------------------------
// Idempotency, audit and helpers
// ---------------------------------------------------------------------------

// gapRecordedOperation is one accepted gap command read back from its audit
// row: the affected gap and the canonical request digest.
type gapRecordedOperation struct {
	GapID  string
	Digest string
}

// recordedGapOperation reads the accepted audit row of one command under its
// operation_id (strictly read-only). Refused/discarded attempts are not
// idempotency anchors: they are re-evaluated on a retry.
func (g *Gaps) recordedGapOperation(ctx context.Context, instanceID, action, operationID string) (gapRecordedOperation, bool, error) {
	if strings.TrimSpace(operationID) == "" {
		return gapRecordedOperation{}, false, nil
	}
	var recorded gapRecordedOperation
	err := g.store.Pool().QueryRow(ctx, `
SELECT COALESCE(target->>'gap_id', ''), COALESCE(detail->>'request_digest', '')
FROM recovery_audit
WHERE instance_id = $1 AND action = $2 AND result = 'ok' AND operation_id = $3
ORDER BY audit_id DESC
LIMIT 1`, instanceID, action, operationID).Scan(&recorded.GapID, &recorded.Digest)
	if errors.Is(err, pgx.ErrNoRows) {
		return gapRecordedOperation{}, false, nil
	}
	if err != nil {
		return gapRecordedOperation{}, false, fmt.Errorf("read recorded %s operation %q: %w", action, operationID, err)
	}
	return recorded, true, nil
}

// writeGapRefusal appends one refusal audit row (best effort: the refusal is
// already decided and the annotation is never a precondition). It is only
// called while no instance row lock is held by this goroutine.
func (g *Gaps) writeGapRefusal(ctx context.Context, instanceID, actor, action, gapID, reason, operationID string) {
	if g == nil || g.store == nil {
		return
	}
	instance, err := uuid.Parse(strings.TrimSpace(instanceID))
	if err != nil {
		return
	}
	actor = strings.TrimSpace(actor)
	if actor == "" {
		return
	}
	detail := map[string]any{"reason": boundedEvidenceDetail(reason)}
	if id := strings.TrimSpace(gapID); id != "" {
		detail["gap_id"] = id
	}
	encoded, err := json.Marshal(detail)
	if err != nil {
		return
	}
	_ = controlstore.WriteAudit(ctx, g.store.Pool(), controlstore.AuditRecord{
		InstanceID:  instance.String(),
		Actor:       actor,
		Action:      action,
		Detail:      encoded,
		Result:      controlstore.AuditRefused,
		OperationID: strings.TrimSpace(operationID),
	})
}

// gapCapabilityNames renders a capability set as its frozen vocabulary names.
func gapCapabilityNames(capabilities []Capability) []string {
	names := make([]string, 0, len(capabilities))
	for _, capability := range capabilities {
		names = append(names, string(capability))
	}
	return names
}

// gapOpenDigest is the canonical digest of one open request (including the
// derived pause set), used as the idempotency comparison and as the
// generation-chain result digest.
func gapOpenDigest(req OpenGapRequest, affected []Capability) string {
	directNames := gapCapabilityNames(req.AffectedCapabilities)
	slices.Sort(directNames)
	affectedNames := gapCapabilityNames(affected)
	slices.Sort(affectedNames)
	return gapDigest(
		[]byte(ActionGapOpen),
		[]byte(strings.TrimSpace(req.ObjectKey)),
		req.Scope,
		req.Timeline,
		req.ExistingEvidence,
		req.RequiredEvidence,
		req.Risk,
		[]byte(strings.Join(directNames, "\x00")),
		[]byte(strings.Join(affectedNames, "\x00")),
		req.DependencyProof,
		[]byte(strings.TrimSpace(req.Owner)),
		[]byte(strings.TrimSpace(req.Actor)),
	)
}

// gapDigest is the canonical, length-prefixed SHA-256 over the given parts.
// Length prefixing keeps distinct part sequences distinct.
func gapDigest(parts ...[]byte) string {
	h := sha256.New()
	h.Write([]byte("txharbor-recovery-gap-request-v1"))
	for _, part := range parts {
		h.Write([]byte{0})
		h.Write([]byte(strconv.Itoa(len(part))))
		h.Write([]byte{0})
		h.Write(part)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// gapInstanceID canonicalizes and validates the instance id.
func gapInstanceID(raw string) (string, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return "", fmt.Errorf("%w: instance_id is required", ErrGapInput)
	}
	parsed, err := uuid.Parse(value)
	if err != nil {
		return "", fmt.Errorf("%w: instance_id %q is not a UUID", ErrGapInput, raw)
	}
	return parsed.String(), nil
}

// gapGapID canonicalizes and validates the gap id.
func gapGapID(raw string) (string, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return "", fmt.Errorf("%w: gap_id is required", ErrGapInput)
	}
	parsed, err := uuid.Parse(value)
	if err != nil {
		return "", fmt.Errorf("%w: gap_id %q is not a UUID", ErrGapInput, raw)
	}
	return parsed.String(), nil
}
