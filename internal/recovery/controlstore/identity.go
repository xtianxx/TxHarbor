// identity.go implements T009: the only 015 write path for
// recovery_participant (per-instance executor/verifier/approver bindings,
// data-model §1.2) and recovery_identity (person<->principal mappings,
// data-model §1.3), plus the read helpers the gate (T012/T048) and the
// management CLI (T010) use.
//
// Trust boundary (data-model §1.3, approval-matrix §2/F19):
//
//   - Every identity and authorization fact read here comes from the
//     independent control store this store is bound to. Nothing in this
//     package ever reads, joins or derives identity/authorization from the
//     data DB — a rolled-back data DB can never re-grant or re-map a person.
//   - principal is the authenticated caller identity in the <kind>:<id> form;
//     free-form text is refused. person_id is never accepted from the caller
//     of a registration: it is resolved from the active recovery_identity
//     mapping inside the same transaction. A principal without an active
//     mapping cannot prove a person and registration refuses (zero
//     recovery_participant rows, one best-effort refused audit row).
//   - recovery_identity is deployment-controlled maintenance: only this
//     path writes it, every write records source/recorded_by/recorded_at and
//     one ok audit row, and revocation keeps the row (active=FALSE) so a
//     revoked mapping can never be silently reused as proof.
//   - F19 mapping-change discipline: a mapping change (person change,
//     revocation or reactivation) immediately invalidates the existing
//     approvals that relied on that mapping. Approvals are append-only with
//     no validity boolean, so the invalidation is recorded as refusal-path
//     audit rows (result='refused', refusal_class='approval_identity_unverified')
//     naming each open-instance approve row whose recorded person_id no
//     longer matches the current active mapping; the gate re-derives
//     person_id consistency on every evaluation (T048) and re-approval is
//     required. A single-privilege mapping maintenance cannot prove two-person
//     truth by itself, so the conservative rule is: missing/inconsistent
//     mapping -> no valid approval.
//   - operation_id is the persistent idempotency key of every identity write:
//     the same input replays the recorded outcome with zero writes, a
//     different input on the same operation_id refuses with
//     ErrOperationConflict and zero writes (the 011/014 shape).
package controlstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// Participant roles and binding sources: the closed sets of
// recovery_participant.role / binding_source (schema CHECK, data-model §1.2).
const (
	ParticipantRoleExecutor = "executor"
	ParticipantRoleVerifier = "verifier"
	ParticipantRoleApprover = "approver"

	BindingSourceDeployConfig  = "deploy_config"
	BindingSourceAuthPrincipal = "auth_principal"
)

// Identity-mapping sources: the closed set of recovery_identity.source. The
// mapping is maintained by the deployment-controlled management path only
// (T010); a local acceptance run uses the same deployment-control label.
const (
	MappingSourceDeployConfig   = "deploy_config"
	MappingSourceIdentitySource = "identity_source"
)

// Audit actions of the identity write path. Each write is paired with its
// audit row in the same transaction; the F19 invalidation rows additionally
// carry refusal_class=approval_identity_unverified (the closed-class refusal
// path a gate evaluation takes for a mapping-inconsistent approval).
const (
	ActionParticipantRegister         = "participant_register"
	ActionIdentityMapSet              = "identity_map_set"
	ActionIdentityMapRevoke           = "identity_map_revoke"
	ActionApprovalIdentityInvalidated = "approval_identity_invalidated"
)

// RefusalApprovalIdentityUnverified is the closed-set refusal class of an
// approval whose recorded person_id does not match the current active identity
// mapping (data-model §3.3; approval-matrix §2/F19). A mapping change records
// this refusal path for every invalidated approval and the gate re-derives the
// same conclusion on every evaluation.
const RefusalApprovalIdentityUnverified = "approval_identity_unverified"

var (
	participantRoleSet = map[string]bool{
		ParticipantRoleExecutor: true, ParticipantRoleVerifier: true, ParticipantRoleApprover: true,
	}
	bindingSourceSet = map[string]bool{
		BindingSourceDeployConfig: true, BindingSourceAuthPrincipal: true,
	}
	mappingSourceSet = map[string]bool{
		MappingSourceDeployConfig: true, MappingSourceIdentitySource: true,
	}
)

// NormalizePrincipal validates an authenticated-caller identity binding: the
// canonical <kind>:<id> form (data-model §1.2, "禁用自由填写"). It performs no
// I/O. CLI flags never carry the actor identity itself; this only canonicalizes
// a target/bound principal.
func NormalizePrincipal(raw string) (string, error) {
	return normalizePrincipal(raw)
}

func normalizePrincipal(raw string) (string, error) {
	principal := strings.TrimSpace(raw)
	if !principalPattern.MatchString(principal) {
		return "", fmt.Errorf("principal %q is not in the authenticated <kind>:<id> form (free-form identities are never accepted)", raw)
	}
	if len(principal) > 256 {
		return "", fmt.Errorf("principal exceeds the 256-character bound")
	}
	return principal, nil
}

// normalizeOperationID validates the operation_id carriage: a bounded,
// control-free identity used for persistent idempotency and audit. It is never
// an authorization input.
func normalizeOperationID(raw string) (string, error) {
	operation := strings.TrimSpace(raw)
	if operation == "" {
		return "", errors.New("operation_id is required")
	}
	if len(operation) > 128 || strings.ContainsAny(operation, "\x00\n\r\t") {
		return "", errors.New("operation_id must be 1..128 characters without control characters")
	}
	return operation, nil
}

func normalizeIdentityText(raw, field string, maxLen int) (string, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return "", fmt.Errorf("%s is required", field)
	}
	if len(value) > maxLen || strings.ContainsAny(value, "\x00\n\r\t") {
		return "", fmt.Errorf("%s must be 1..%d characters without control characters", field, maxLen)
	}
	return value, nil
}

// ---------------------------------------------------------------------------
// Read model
// ---------------------------------------------------------------------------

// IdentityMapping is one recovery_identity row (revoked rows are kept and
// reported with Active=false; they are never deleted or overwritten as proof).
type IdentityMapping struct {
	Principal  string
	PersonID   string
	Source     string
	RecordedBy string
	RecordedAt time.Time
	Active     bool
}

// ParticipantBinding is one recovery_participant row.
type ParticipantBinding struct {
	InstanceID    string
	Principal     string
	PersonID      string
	Role          string
	BoundAt       time.Time
	BindingSource string
	ProofRef      string
}

// InvalidatedApproval is one append-only approve row that a mapping change
// immediately invalidated (F19): its recorded person_id no longer matches the
// current active mapping (or no active mapping exists any more). The gate
// reaches the same conclusion by re-deriving person_id consistency on every
// evaluation; this helper is for the management path and tests.
type InvalidatedApproval struct {
	ApprovalID       string
	InstanceID       string
	Capability       string
	ScopeHash        string
	RecordedPersonID string
	CurrentPersonID  string
	// MappingState is one of missing|revoked|changed|reactivated.
	MappingState string
}

const selectIdentityMappingSQL = `
SELECT principal, person_id, source, recorded_by, recorded_at, active
FROM recovery_identity
WHERE principal = $1`

func scanIdentityMapping(row pgx.Row) (IdentityMapping, error) {
	var mapping IdentityMapping
	err := row.Scan(&mapping.Principal, &mapping.PersonID, &mapping.Source,
		&mapping.RecordedBy, &mapping.RecordedAt, &mapping.Active)
	return mapping, err
}

func readIdentityMapping(ctx context.Context, q Queryer, principal string) (IdentityMapping, bool, error) {
	mapping, err := scanIdentityMapping(q.QueryRow(ctx, selectIdentityMappingSQL, principal))
	if errors.Is(err, pgx.ErrNoRows) {
		return IdentityMapping{}, false, nil
	}
	if err != nil {
		return IdentityMapping{}, false, fmt.Errorf("read identity mapping %s: %w", principal, err)
	}
	return mapping, true, nil
}

// IdentityMapping reads one principal's mapping row (active or revoked).
func (s *Store) IdentityMapping(ctx context.Context, principal string) (IdentityMapping, bool, error) {
	normalized, err := normalizePrincipal(principal)
	if err != nil {
		return IdentityMapping{}, false, err
	}
	return readIdentityMapping(ctx, s.pool, normalized)
}

// ActivePersonID resolves one principal to the person_id of its active
// mapping. found=false means no row exists or the mapping was revoked: the
// principal cannot prove a person (approval-matrix §2).
func (s *Store) ActivePersonID(ctx context.Context, principal string) (string, bool, error) {
	normalized, err := normalizePrincipal(principal)
	if err != nil {
		return "", false, err
	}
	mapping, found, err := readIdentityMapping(ctx, s.pool, normalized)
	if err != nil {
		return "", false, err
	}
	if !found || !mapping.Active {
		return "", false, nil
	}
	return mapping.PersonID, true, nil
}

// IdentityMappings lists mapping rows, optionally filtered by principal and/or
// person_id (empty string means no filter). Revoked rows are included.
func (s *Store) IdentityMappings(ctx context.Context, principalFilter, personIDFilter string) ([]IdentityMapping, error) {
	principal := strings.TrimSpace(principalFilter)
	if principal != "" {
		normalized, err := normalizePrincipal(principal)
		if err != nil {
			return nil, err
		}
		principal = normalized
	}
	personID := strings.TrimSpace(personIDFilter)
	if len(personID) > 256 || strings.ContainsAny(personID, "\x00\n\r\t") {
		return nil, errors.New("person_id filter must be at most 256 characters without control characters")
	}
	const sql = `
SELECT principal, person_id, source, recorded_by, recorded_at, active
FROM recovery_identity
WHERE ($1 = '' OR principal = $1) AND ($2 = '' OR person_id = $2)
ORDER BY principal`
	rows, err := s.pool.Query(ctx, sql, principal, personID)
	if err != nil {
		return nil, fmt.Errorf("list identity mappings: %w", err)
	}
	defer rows.Close()
	var mappings []IdentityMapping
	for rows.Next() {
		mapping, err := scanIdentityMapping(rows)
		if err != nil {
			return nil, fmt.Errorf("scan identity mapping: %w", err)
		}
		mappings = append(mappings, mapping)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list identity mappings: %w", err)
	}
	return mappings, nil
}

// ParticipantBindings lists one instance's participant bindings.
func (s *Store) ParticipantBindings(ctx context.Context, instanceID string) ([]ParticipantBinding, error) {
	id, err := normalizeUUID(instanceID, "instance_id")
	if err != nil {
		return nil, err
	}
	const sql = `
SELECT instance_id::text, principal, person_id, role, bound_at, binding_source, proof_ref
FROM recovery_participant
WHERE instance_id = $1
ORDER BY principal, role`
	rows, err := s.pool.Query(ctx, sql, id)
	if err != nil {
		return nil, fmt.Errorf("list participant bindings: %w", err)
	}
	defer rows.Close()
	var bindings []ParticipantBinding
	for rows.Next() {
		binding, err := scanParticipantBinding(rows)
		if err != nil {
			return nil, fmt.Errorf("scan participant binding: %w", err)
		}
		bindings = append(bindings, binding)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list participant bindings: %w", err)
	}
	return bindings, nil
}

func scanParticipantBinding(row pgx.Row) (ParticipantBinding, error) {
	var binding ParticipantBinding
	err := row.Scan(&binding.InstanceID, &binding.Principal, &binding.PersonID, &binding.Role,
		&binding.BoundAt, &binding.BindingSource, &binding.ProofRef)
	return binding, err
}

// InvalidatedApprovals lists the open-instance approve rows of one principal
// whose recorded person_id is inconsistent with the current active mapping
// (missing or revoked mapping, or a changed person_id). It is a read-only
// derivation and performs no write.
func (s *Store) InvalidatedApprovals(ctx context.Context, principal string) ([]InvalidatedApproval, error) {
	normalized, err := normalizePrincipal(principal)
	if err != nil {
		return nil, err
	}
	mapping, found, err := readIdentityMapping(ctx, s.pool, normalized)
	if err != nil {
		return nil, err
	}
	includeAll := !found || !mapping.Active
	return listInvalidatedApprovals(ctx, s.pool, normalized, includeAll)
}

// ---------------------------------------------------------------------------
// Participant registration
// ---------------------------------------------------------------------------

// RegisterParticipantRequest binds one authenticated principal to an open
// recovery instance under one role. person_id is deliberately absent: it is
// resolved from the active recovery_identity mapping inside the write
// transaction (a caller can never supply a person).
type RegisterParticipantRequest struct {
	InstanceID    string
	Principal     string
	Role          string
	BindingSource string
	ProofRef      string
	// Actor is the authenticated management subject recorded as the audit
	// actor; Operator/Reason are audit annotations only.
	Actor       string
	Operator    string
	Reason      string
	OperationID string
}

// ParticipantRegistration is the outcome of RegisterParticipant.
type ParticipantRegistration struct {
	Binding ParticipantBinding
	// Recorded is true when the call read back the binding already recorded
	// under the same operation_id (zero writes).
	Recorded bool
}

func (r RegisterParticipantRequest) canonical() (RegisterParticipantRequest, error) {
	var err error
	if r.InstanceID, err = normalizeUUID(r.InstanceID, "instance_id"); err != nil {
		return RegisterParticipantRequest{}, err
	}
	if r.Principal, err = normalizePrincipal(r.Principal); err != nil {
		return RegisterParticipantRequest{}, err
	}
	if !participantRoleSet[r.Role] {
		return RegisterParticipantRequest{}, fmt.Errorf("role %q is not in the closed set executor|verifier|approver", r.Role)
	}
	if r.BindingSource == "" {
		r.BindingSource = BindingSourceDeployConfig
	}
	if !bindingSourceSet[r.BindingSource] {
		return RegisterParticipantRequest{}, fmt.Errorf("binding_source %q is not in the closed set deploy_config|auth_principal", r.BindingSource)
	}
	r.ProofRef = strings.TrimSpace(r.ProofRef)
	if len(r.ProofRef) > 256 || strings.ContainsAny(r.ProofRef, "\x00\n\r\t") {
		return RegisterParticipantRequest{}, errors.New("proof_ref must be at most 256 characters without control characters")
	}
	if err := validateCredentialText("proof_ref", r.ProofRef); err != nil {
		return RegisterParticipantRequest{}, err
	}
	if r.Actor, err = normalizeIdentityText(r.Actor, "audit actor", 256); err != nil {
		return RegisterParticipantRequest{}, err
	}
	if err := validateCredentialText("audit actor", r.Actor); err != nil {
		return RegisterParticipantRequest{}, err
	}
	r.Operator = strings.TrimSpace(r.Operator)
	r.Reason = strings.TrimSpace(r.Reason)
	if err := validateCredentialText("operator", r.Operator); err != nil {
		return RegisterParticipantRequest{}, err
	}
	if err := validateCredentialText("reason", r.Reason); err != nil {
		return RegisterParticipantRequest{}, err
	}
	if r.OperationID, err = normalizeOperationID(r.OperationID); err != nil {
		return RegisterParticipantRequest{}, err
	}
	return r, nil
}

const insertParticipantSQL = `
INSERT INTO recovery_participant
    (instance_id, principal, person_id, role, binding_source, proof_ref)
SELECT $1, $2, i.person_id, $3, $4, $5
FROM recovery_identity i
WHERE i.principal = $2 AND i.active
RETURNING instance_id::text, principal, person_id, role, bound_at, binding_source, proof_ref`

// RegisterParticipant binds one principal to an open recovery instance. The
// person_id is resolved from the principal's active recovery_identity mapping
// inside the same transaction:
//
//   - no active mapping (missing or revoked) -> the registration refuses with
//     ErrIdentityMappingMissing / ErrIdentityMappingRevoked, writes zero
//     recovery_participant rows and one best-effort refused audit row;
//   - an existing binding for (instance, principal, role) -> refuses with
//     ErrParticipantAlreadyRegistered (the PK is the anti-duplication guard);
//   - the same operation_id with the same input -> reads the recorded binding
//     back (Recorded=true, zero writes); a different input on the same
//     operation_id -> ErrOperationConflict, zero writes of any kind.
//
// The instance row lock serializes registration writes with the decision
// writers of the same instance.
func (s *Store) RegisterParticipant(ctx context.Context, req RegisterParticipantRequest) (ParticipantRegistration, error) {
	canonical, err := req.canonical()
	if err != nil {
		return ParticipantRegistration{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ParticipantRegistration{}, fmt.Errorf("begin participant registration: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	if err := lockIdentityOperation(ctx, tx, canonical.OperationID); err != nil {
		return ParticipantRegistration{}, err
	}
	token, err := LockInstance(ctx, tx, canonical.InstanceID)
	if err != nil {
		return ParticipantRegistration{}, err
	}
	if token.State != "open" {
		return ParticipantRegistration{}, fmt.Errorf("%w: %s", ErrInstanceNotOpen, token.InstanceID)
	}

	recorded, found, err := readAuditOperation(ctx, tx, ActionParticipantRegister, canonical.OperationID)
	if err != nil {
		return ParticipantRegistration{}, err
	}
	if found {
		if !canonical.matchesRecorded(recorded) {
			return ParticipantRegistration{}, fmt.Errorf("%w: operation_id %q", ErrOperationConflict, canonical.OperationID)
		}
		binding, exists, err := readParticipantBinding(ctx, tx, canonical.InstanceID, canonical.Principal, canonical.Role)
		if err != nil {
			return ParticipantRegistration{}, err
		}
		if !exists {
			return ParticipantRegistration{}, fmt.Errorf(
				"operation_id %q is recorded but its participant binding is missing; refusing to reconstruct", canonical.OperationID)
		}
		return ParticipantRegistration{Binding: binding, Recorded: true}, nil
	}

	binding, err := scanParticipantBinding(tx.QueryRow(ctx, insertParticipantSQL,
		canonical.InstanceID, canonical.Principal, canonical.Role, canonical.BindingSource, canonical.ProofRef))
	if errors.Is(err, pgx.ErrNoRows) {
		cause, mapErr := missingMappingCause(ctx, tx, canonical.Principal)
		if mapErr != nil {
			return ParticipantRegistration{}, mapErr
		}
		return ParticipantRegistration{}, commitIdentityRefusal(ctx, tx, AuditRecord{
			InstanceID: canonical.InstanceID,
			Actor:      canonical.Actor,
			Action:     ActionParticipantRegister,
			Target:     mustJSON(map[string]any{"instance_id": canonical.InstanceID, "principal": canonical.Principal, "role": canonical.Role}),
			Detail:     mustJSON(map[string]any{"reason": cause.Error(), "operator": canonical.Operator, "note": canonical.Reason, "operation_id": canonical.OperationID}),
			Result:     AuditRefused,
		}, cause)
	}
	if err != nil {
		if isUniqueViolation(err, "recovery_participant_pkey") {
			return ParticipantRegistration{}, commitIdentityRefusal(ctx, tx, AuditRecord{
				InstanceID: canonical.InstanceID,
				Actor:      canonical.Actor,
				Action:     ActionParticipantRegister,
				Target:     mustJSON(map[string]any{"instance_id": canonical.InstanceID, "principal": canonical.Principal, "role": canonical.Role}),
				Detail:     mustJSON(map[string]any{"reason": "binding already exists", "operator": canonical.Operator, "note": canonical.Reason, "operation_id": canonical.OperationID}),
				Result:     AuditRefused,
			}, fmt.Errorf("%w: instance=%s principal=%s role=%s", ErrParticipantAlreadyRegistered, canonical.InstanceID, canonical.Principal, canonical.Role))
		}
		return ParticipantRegistration{}, fmt.Errorf("register participant: %w", err)
	}

	if err := WriteAudit(ctx, tx, AuditRecord{
		InstanceID:  canonical.InstanceID,
		Actor:       canonical.Actor,
		Action:      ActionParticipantRegister,
		Target:      mustJSON(map[string]any{"instance_id": canonical.InstanceID, "principal": canonical.Principal, "role": canonical.Role}),
		Detail:      mustJSON(map[string]any{"person_id": binding.PersonID, "binding_source": binding.BindingSource, "proof_ref": binding.ProofRef, "operator": canonical.Operator, "note": canonical.Reason, "operation_id": canonical.OperationID}),
		Result:      AuditOK,
		OperationID: canonical.OperationID,
	}); err != nil {
		return ParticipantRegistration{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ParticipantRegistration{}, fmt.Errorf("commit participant registration: %w", err)
	}
	return ParticipantRegistration{Binding: binding}, nil
}

// missingMappingCause distinguishes the two fail-closed registration refusals:
// no mapping row at all vs a revoked mapping. The write itself is already
// refused by the INSERT ... SELECT guard regardless of this read.
func missingMappingCause(ctx context.Context, q Queryer, principal string) (error, error) {
	mapping, found, err := readIdentityMapping(ctx, q, principal)
	if err != nil {
		return nil, err
	}
	switch {
	case !found:
		return ErrIdentityMappingMissing, nil
	case !mapping.Active:
		return ErrIdentityMappingRevoked, nil
	default:
		return errors.New("the principal's active mapping disappeared during registration; refusing"), nil
	}
}

func readParticipantBinding(ctx context.Context, q Queryer, instanceID, principal, role string) (ParticipantBinding, bool, error) {
	const sql = `
SELECT instance_id::text, principal, person_id, role, bound_at, binding_source, proof_ref
FROM recovery_participant
WHERE instance_id = $1 AND principal = $2 AND role = $3`
	binding, err := scanParticipantBinding(q.QueryRow(ctx, sql, instanceID, principal, role))
	if errors.Is(err, pgx.ErrNoRows) {
		return ParticipantBinding{}, false, nil
	}
	if err != nil {
		return ParticipantBinding{}, false, fmt.Errorf("read participant binding: %w", err)
	}
	return binding, true, nil
}

// matchesRecorded compares a registration request with its recorded audit row
// (same operation_id): instance/principal/role and the binding carriage must
// match; operator/reason annotations are not part of the operation identity.
func (r RegisterParticipantRequest) matchesRecorded(recorded recordedAuditOperation) bool {
	var fields struct {
		InstanceID    string `json:"instance_id"`
		Principal     string `json:"principal"`
		Role          string `json:"role"`
		BindingSource string `json:"binding_source"`
		ProofRef      string `json:"proof_ref"`
	}
	if err := json.Unmarshal(recorded.Target, &fields); err != nil {
		return false
	}
	if err := json.Unmarshal(recorded.Detail, &fields); err != nil {
		return false
	}
	return recorded.Actor == r.Actor &&
		fields.InstanceID == r.InstanceID &&
		fields.Principal == r.Principal &&
		fields.Role == r.Role &&
		fields.BindingSource == r.BindingSource &&
		fields.ProofRef == r.ProofRef
}

// ---------------------------------------------------------------------------
// Identity-mapping maintenance
// ---------------------------------------------------------------------------

// SetIdentityMappingRequest sets (or refreshes) the active person<->principal
// mapping. RecordedBy is the authenticated management subject recorded in the
// row and as the audit actor; Operator/Reason are audit annotations only.
type SetIdentityMappingRequest struct {
	Principal   string
	PersonID    string
	Source      string
	RecordedBy  string
	Operator    string
	Reason      string
	OperationID string
}

// RevokeIdentityMappingRequest revokes one principal's active mapping. The row
// is kept (active=FALSE) so the revoked mapping can never be silently reused.
type RevokeIdentityMappingRequest struct {
	Principal   string
	RecordedBy  string
	Operator    string
	Reason      string
	OperationID string
}

// IdentityMappingChange is the outcome of a mapping write.
type IdentityMappingChange struct {
	// Mapping is the resulting row (active=FALSE for a revocation).
	Mapping IdentityMapping
	// Previous is the row before the write (nil when none existed).
	Previous *IdentityMapping
	// Created is true when no row existed before this write.
	Created bool
	// Changed is true when the write modified an existing row (person change,
	// revocation, reactivation or metadata refresh).
	Changed bool
	// Revoked/AlreadyRevoked are the revocation outcomes.
	Revoked        bool
	AlreadyRevoked bool
	// InvalidatedApprovals lists the existing approve rows this change
	// immediately invalidated (F19), each with a recorded refusal-path audit
	// row (approval_identity_unverified).
	InvalidatedApprovals []InvalidatedApproval
	// Recorded is true when the call read the operation back (zero writes).
	Recorded bool
}

func (r SetIdentityMappingRequest) canonical() (SetIdentityMappingRequest, error) {
	var err error
	if r.Principal, err = normalizePrincipal(r.Principal); err != nil {
		return SetIdentityMappingRequest{}, err
	}
	if r.PersonID, err = normalizeIdentityText(r.PersonID, "person_id", 256); err != nil {
		return SetIdentityMappingRequest{}, err
	}
	if r.Source == "" {
		r.Source = MappingSourceDeployConfig
	}
	if !mappingSourceSet[r.Source] {
		return SetIdentityMappingRequest{}, fmt.Errorf("source %q is not in the closed set deploy_config|identity_source", r.Source)
	}
	if r.RecordedBy, err = normalizeIdentityText(r.RecordedBy, "recorded_by", 256); err != nil {
		return SetIdentityMappingRequest{}, err
	}
	if err := validateCredentialText("recorded_by", r.RecordedBy); err != nil {
		return SetIdentityMappingRequest{}, err
	}
	r.Operator = strings.TrimSpace(r.Operator)
	r.Reason = strings.TrimSpace(r.Reason)
	if err := validateCredentialText("operator", r.Operator); err != nil {
		return SetIdentityMappingRequest{}, err
	}
	if err := validateCredentialText("reason", r.Reason); err != nil {
		return SetIdentityMappingRequest{}, err
	}
	if r.OperationID, err = normalizeOperationID(r.OperationID); err != nil {
		return SetIdentityMappingRequest{}, err
	}
	return r, nil
}

func (r RevokeIdentityMappingRequest) canonical() (RevokeIdentityMappingRequest, error) {
	var err error
	if r.Principal, err = normalizePrincipal(r.Principal); err != nil {
		return RevokeIdentityMappingRequest{}, err
	}
	if r.RecordedBy, err = normalizeIdentityText(r.RecordedBy, "recorded_by", 256); err != nil {
		return RevokeIdentityMappingRequest{}, err
	}
	if err := validateCredentialText("recorded_by", r.RecordedBy); err != nil {
		return RevokeIdentityMappingRequest{}, err
	}
	r.Operator = strings.TrimSpace(r.Operator)
	r.Reason = strings.TrimSpace(r.Reason)
	if err := validateCredentialText("operator", r.Operator); err != nil {
		return RevokeIdentityMappingRequest{}, err
	}
	if err := validateCredentialText("reason", r.Reason); err != nil {
		return RevokeIdentityMappingRequest{}, err
	}
	if r.OperationID, err = normalizeOperationID(r.OperationID); err != nil {
		return RevokeIdentityMappingRequest{}, err
	}
	return r, nil
}

const upsertIdentityMappingSQL = `
INSERT INTO recovery_identity (principal, person_id, source, recorded_by, active)
VALUES ($1, $2, $3, $4, TRUE)
ON CONFLICT (principal) DO UPDATE
SET person_id   = EXCLUDED.person_id,
    source      = EXCLUDED.source,
    recorded_by = EXCLUDED.recorded_by,
    recorded_at = now(),
    active      = TRUE
RETURNING principal, person_id, source, recorded_by, recorded_at, active`

const revokeIdentityMappingSQL = `
UPDATE recovery_identity
SET active = FALSE, recorded_by = $2, recorded_at = now()
WHERE principal = $1 AND active
RETURNING principal, person_id, source, recorded_by, recorded_at, active`

// SetIdentityMapping writes the active mapping for one principal and, in the
// same transaction, records the F19 consequence: every existing open-instance
// approve row of the principal whose recorded person_id no longer matches the
// active mapping (or whose mapping is no longer active) is invalidated and
// audited with refusal_class=approval_identity_unverified. A reactivation
// (active=FALSE -> active=TRUE) conservatively invalidates every existing
// approve row of the principal, so a revoked epoch can never silently revive.
//
// operation_id idempotency: same input replays the recorded operation with
// zero writes, a different input refuses with ErrOperationConflict.
func (s *Store) SetIdentityMapping(ctx context.Context, req SetIdentityMappingRequest) (IdentityMappingChange, error) {
	canonical, err := req.canonical()
	if err != nil {
		return IdentityMappingChange{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return IdentityMappingChange{}, fmt.Errorf("begin identity mapping set: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	if err := lockIdentityOperation(ctx, tx, canonical.OperationID); err != nil {
		return IdentityMappingChange{}, err
	}
	recorded, found, err := readAuditOperation(ctx, tx, ActionIdentityMapSet, canonical.OperationID)
	if err != nil {
		return IdentityMappingChange{}, err
	}
	if found {
		if !canonical.matchesRecorded(recorded) {
			return IdentityMappingChange{}, fmt.Errorf("%w: operation_id %q", ErrOperationConflict, canonical.OperationID)
		}
		mapping, exists, err := readIdentityMapping(ctx, tx, canonical.Principal)
		if err != nil {
			return IdentityMappingChange{}, err
		}
		if !exists {
			return IdentityMappingChange{}, fmt.Errorf(
				"operation_id %q is recorded but its identity mapping is missing; refusing to reconstruct", canonical.OperationID)
		}
		return IdentityMappingChange{Mapping: mapping, Recorded: true}, nil
	}

	previous, prevFound, err := lockIdentityMapping(ctx, tx, canonical.Principal)
	if err != nil {
		return IdentityMappingChange{}, err
	}
	mapping, err := scanIdentityMapping(tx.QueryRow(ctx, upsertIdentityMappingSQL,
		canonical.Principal, canonical.PersonID, canonical.Source, canonical.RecordedBy))
	if err != nil {
		return IdentityMappingChange{}, fmt.Errorf("set identity mapping: %w", err)
	}

	reactivated := prevFound && !previous.Active && mapping.Active
	includeAll := !mapping.Active || reactivated
	invalidated, err := listInvalidatedApprovals(ctx, tx, canonical.Principal, includeAll)
	if err != nil {
		return IdentityMappingChange{}, err
	}
	if err := writeInvalidationAudits(ctx, tx, canonical.Actor(), canonical.OperationID, mapping, invalidated, canonical.Operator, canonical.Reason); err != nil {
		return IdentityMappingChange{}, err
	}

	previousPerson := ""
	previousActive := false
	if prevFound {
		previousPerson, previousActive = previous.PersonID, previous.Active
	}
	if err := WriteAudit(ctx, tx, AuditRecord{
		Actor:       canonical.Actor(),
		Action:      ActionIdentityMapSet,
		Target:      mustJSON(map[string]any{"principal": canonical.Principal}),
		Detail:      mustJSON(map[string]any{"person_id": mapping.PersonID, "previous_person_id": previousPerson, "previous_active": previousActive, "source": mapping.Source, "recorded_by": mapping.RecordedBy, "active": mapping.Active, "operator": canonical.Operator, "note": canonical.Reason, "invalidated_approvals": len(invalidated), "invalidated_approval_ids": invalidatedApprovalIDs(invalidated), "operation_id": canonical.OperationID}),
		Result:      AuditOK,
		OperationID: canonical.OperationID,
	}); err != nil {
		return IdentityMappingChange{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return IdentityMappingChange{}, fmt.Errorf("commit identity mapping set: %w", err)
	}

	change := IdentityMappingChange{
		Mapping:              mapping,
		Created:              !prevFound,
		Changed:              prevFound && (previous.PersonID != mapping.PersonID || !previous.Active || previous.Source != mapping.Source),
		InvalidatedApprovals: invalidated,
	}
	if prevFound {
		previousCopy := previous
		change.Previous = &previousCopy
	}
	return change, nil
}

// RevokeIdentityMapping revokes one principal's active mapping, keeping the
// row (active=FALSE). All existing open-instance approve rows of the principal
// become invalid immediately and are audited with
// refusal_class=approval_identity_unverified. Revoking an already-revoked
// mapping is a no-op (AlreadyRevoked=true, zero row writes) and revoking an
// unknown principal refuses with ErrIdentityMappingMissing.
func (s *Store) RevokeIdentityMapping(ctx context.Context, req RevokeIdentityMappingRequest) (IdentityMappingChange, error) {
	canonical, err := req.canonical()
	if err != nil {
		return IdentityMappingChange{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return IdentityMappingChange{}, fmt.Errorf("begin identity mapping revoke: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	if err := lockIdentityOperation(ctx, tx, canonical.OperationID); err != nil {
		return IdentityMappingChange{}, err
	}
	recorded, found, err := readAuditOperation(ctx, tx, ActionIdentityMapRevoke, canonical.OperationID)
	if err != nil {
		return IdentityMappingChange{}, err
	}
	if found {
		if !canonical.matchesRecorded(recorded) {
			return IdentityMappingChange{}, fmt.Errorf("%w: operation_id %q", ErrOperationConflict, canonical.OperationID)
		}
		mapping, exists, err := readIdentityMapping(ctx, tx, canonical.Principal)
		if err != nil {
			return IdentityMappingChange{}, err
		}
		if !exists {
			return IdentityMappingChange{}, fmt.Errorf(
				"operation_id %q is recorded but its identity mapping is missing; refusing to reconstruct", canonical.OperationID)
		}
		return IdentityMappingChange{Mapping: mapping, Recorded: true}, nil
	}

	previous, prevFound, err := lockIdentityMapping(ctx, tx, canonical.Principal)
	if err != nil {
		return IdentityMappingChange{}, err
	}
	if !prevFound {
		return IdentityMappingChange{}, commitIdentityRefusal(ctx, tx, AuditRecord{
			Actor:  canonical.RecordedBy,
			Action: ActionIdentityMapRevoke,
			Target: mustJSON(map[string]any{"principal": canonical.Principal}),
			Detail: mustJSON(map[string]any{"reason": "no identity mapping exists for this principal", "operator": canonical.Operator, "note": canonical.Reason, "operation_id": canonical.OperationID}),
			Result: AuditRefused,
		}, fmt.Errorf("%w: %s", ErrIdentityMappingMissing, canonical.Principal))
	}
	if !previous.Active {
		if err := WriteAudit(ctx, tx, AuditRecord{
			Actor:       canonical.RecordedBy,
			Action:      ActionIdentityMapRevoke,
			Target:      mustJSON(map[string]any{"principal": canonical.Principal}),
			Detail:      mustJSON(map[string]any{"person_id": previous.PersonID, "outcome": "already_revoked", "operator": canonical.Operator, "note": canonical.Reason, "operation_id": canonical.OperationID}),
			Result:      AuditOK,
			OperationID: canonical.OperationID,
		}); err != nil {
			return IdentityMappingChange{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return IdentityMappingChange{}, fmt.Errorf("commit identity mapping revoke no-op: %w", err)
		}
		previousCopy := previous
		return IdentityMappingChange{Mapping: previous, Previous: &previousCopy, AlreadyRevoked: true}, nil
	}

	mapping, err := scanIdentityMapping(tx.QueryRow(ctx, revokeIdentityMappingSQL, canonical.Principal, canonical.RecordedBy))
	if errors.Is(err, pgx.ErrNoRows) {
		return IdentityMappingChange{}, errors.New("identity mapping revoked concurrently; retry")
	}
	if err != nil {
		return IdentityMappingChange{}, fmt.Errorf("revoke identity mapping: %w", err)
	}

	invalidated, err := listInvalidatedApprovals(ctx, tx, canonical.Principal, true)
	if err != nil {
		return IdentityMappingChange{}, err
	}
	if err := writeInvalidationAudits(ctx, tx, canonical.RecordedBy, canonical.OperationID, mapping, invalidated, canonical.Operator, canonical.Reason); err != nil {
		return IdentityMappingChange{}, err
	}
	if err := WriteAudit(ctx, tx, AuditRecord{
		Actor:       canonical.RecordedBy,
		Action:      ActionIdentityMapRevoke,
		Target:      mustJSON(map[string]any{"principal": canonical.Principal}),
		Detail:      mustJSON(map[string]any{"person_id": mapping.PersonID, "previous_person_id": previous.PersonID, "active": false, "recorded_by": mapping.RecordedBy, "operator": canonical.Operator, "note": canonical.Reason, "invalidated_approvals": len(invalidated), "invalidated_approval_ids": invalidatedApprovalIDs(invalidated), "operation_id": canonical.OperationID}),
		Result:      AuditOK,
		OperationID: canonical.OperationID,
	}); err != nil {
		return IdentityMappingChange{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return IdentityMappingChange{}, fmt.Errorf("commit identity mapping revoke: %w", err)
	}
	previousCopy := previous
	return IdentityMappingChange{
		Mapping: mapping, Previous: &previousCopy, Changed: true, Revoked: true,
		InvalidatedApprovals: invalidated,
	}, nil
}

// lockIdentityMapping locks one principal's mapping row FOR UPDATE inside tx
// (nil-safe when the row does not exist yet).
func lockIdentityMapping(ctx context.Context, tx pgx.Tx, principal string) (IdentityMapping, bool, error) {
	const sql = `
SELECT principal, person_id, source, recorded_by, recorded_at, active
FROM recovery_identity
WHERE principal = $1
FOR UPDATE`
	mapping, err := scanIdentityMapping(tx.QueryRow(ctx, sql, principal))
	if errors.Is(err, pgx.ErrNoRows) {
		return IdentityMapping{}, false, nil
	}
	if err != nil {
		return IdentityMapping{}, false, fmt.Errorf("lock identity mapping %s: %w", principal, err)
	}
	return mapping, true, nil
}

// lockIdentityOperation serializes equal operation ids across processes
// (transaction-scoped advisory lock; distinct ids only contend on a hash
// collision and stay correct).
func lockIdentityOperation(ctx context.Context, tx pgx.Tx, operationID string) error {
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, "recovery_identity_op:"+operationID); err != nil {
		return fmt.Errorf("lock identity operation: %w", err)
	}
	return nil
}

// Actor renders the audit actor of a mapping write.
func (r SetIdentityMappingRequest) Actor() string { return r.RecordedBy }

// matchesRecorded compares a mapping-set request with its recorded audit row.
func (r SetIdentityMappingRequest) matchesRecorded(recorded recordedAuditOperation) bool {
	var fields struct {
		Principal string `json:"principal"`
		PersonID  string `json:"person_id"`
		Source    string `json:"source"`
	}
	if err := json.Unmarshal(recorded.Target, &fields); err != nil {
		return false
	}
	if err := json.Unmarshal(recorded.Detail, &fields); err != nil {
		return false
	}
	return recorded.Actor == r.RecordedBy &&
		fields.Principal == r.Principal &&
		fields.PersonID == r.PersonID &&
		fields.Source == r.Source
}

// matchesRecorded compares a mapping-revoke request with its recorded audit row.
func (r RevokeIdentityMappingRequest) matchesRecorded(recorded recordedAuditOperation) bool {
	var fields struct {
		Principal string `json:"principal"`
	}
	if err := json.Unmarshal(recorded.Target, &fields); err != nil {
		return false
	}
	return recorded.Actor == r.RecordedBy && fields.Principal == r.Principal
}

// listInvalidatedApprovals lists the open-instance approve rows of one
// principal that are inconsistent with the current mapping state. includeAll
// forces every approve row of the principal (used for revocations and
// reactivations, where person_id equality alone cannot prove a fresh epoch).
func listInvalidatedApprovals(ctx context.Context, q Queryer, principal string, includeAll bool) ([]InvalidatedApproval, error) {
	const sql = `
SELECT a.approval_id::text, a.instance_id::text, a.capability, a.scope_hash, a.person_id,
       COALESCE(i.person_id, ''), COALESCE(i.active, FALSE)
FROM recovery_approval a
JOIN recovery_instance ri ON ri.instance_id = a.instance_id AND ri.state = 'open'
LEFT JOIN recovery_identity i ON i.principal = a.principal
WHERE a.principal = $1 AND a.decision = 'approve'
  AND ($2 OR i.principal IS NULL OR NOT i.active OR i.person_id <> a.person_id)
ORDER BY a.instance_id, a.capability, a.scope_hash, a.approval_id`
	rows, err := q.Query(ctx, sql, principal, includeAll)
	if err != nil {
		return nil, fmt.Errorf("list invalidated approvals: %w", err)
	}
	defer rows.Close()
	var invalidated []InvalidatedApproval
	for rows.Next() {
		var (
			item          InvalidatedApproval
			currentPerson string
			mappingActive bool
		)
		if err := rows.Scan(&item.ApprovalID, &item.InstanceID, &item.Capability, &item.ScopeHash,
			&item.RecordedPersonID, &currentPerson, &mappingActive); err != nil {
			return nil, fmt.Errorf("scan invalidated approval: %w", err)
		}
		item.CurrentPersonID = currentPerson
		switch {
		case !mappingActive && currentPerson == "":
			item.MappingState = "missing"
		case !mappingActive:
			item.MappingState = "revoked"
		case currentPerson != item.RecordedPersonID:
			item.MappingState = "changed"
		default:
			item.MappingState = "reactivated"
		}
		invalidated = append(invalidated, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list invalidated approvals: %w", err)
	}
	return invalidated, nil
}

// writeInvalidationAudits records one refusal-path audit row per invalidated
// approval: result='refused', refusal_class='approval_identity_unverified'
// (data-model §3.3). The rows carry the dedicated action token and the mapping
// operation id in their detail; the operation_id column is claimed only by the
// applied mapping write itself (a refusal applies no effect), so a replay read
// of the mapping operation stays unambiguous.
func writeInvalidationAudits(ctx context.Context, tx pgx.Tx, actor, operationID string, mapping IdentityMapping,
	invalidated []InvalidatedApproval, operator, note string) error {
	for _, item := range invalidated {
		if err := WriteAudit(ctx, tx, AuditRecord{
			InstanceID:   item.InstanceID,
			Actor:        actor,
			Action:       ActionApprovalIdentityInvalidated,
			Target:       mustJSON(map[string]any{"approval_id": item.ApprovalID, "instance_id": item.InstanceID, "capability": item.Capability, "scope_hash": item.ScopeHash}),
			Detail:       mustJSON(map[string]any{"principal": mapping.Principal, "recorded_person_id": item.RecordedPersonID, "current_person_id": item.CurrentPersonID, "mapping_state": item.MappingState, "operator": operator, "note": note, "operation_id": operationID}),
			Result:       AuditRefused,
			RefusalClass: RefusalApprovalIdentityUnverified,
		}); err != nil {
			return err
		}
	}
	return nil
}

// invalidatedApprovalIDs renders the deterministic id list recorded in the
// mapping-write audit detail.
func invalidatedApprovalIDs(invalidated []InvalidatedApproval) []string {
	ids := make([]string, 0, len(invalidated))
	for _, item := range invalidated {
		ids = append(ids, item.ApprovalID)
	}
	return ids
}

// ---------------------------------------------------------------------------
// Recorded-operation read-back
// ---------------------------------------------------------------------------

// recordedAuditOperation is the audit row read back for operation_id
// idempotency.
type recordedAuditOperation struct {
	OperationID string
	Actor       string
	Target      []byte
	Detail      []byte
}

func readAuditOperation(ctx context.Context, q Queryer, action, operationID string) (recordedAuditOperation, bool, error) {
	const sql = `
SELECT COALESCE(target, '{}'::jsonb), COALESCE(detail, '{}'::jsonb), actor
FROM recovery_audit
WHERE action = $1 AND operation_id = $2
ORDER BY audit_id DESC
LIMIT 1`
	recorded := recordedAuditOperation{OperationID: operationID}
	err := q.QueryRow(ctx, sql, action, operationID).Scan(&recorded.Target, &recorded.Detail, &recorded.Actor)
	if errors.Is(err, pgx.ErrNoRows) {
		return recordedAuditOperation{}, false, nil
	}
	if err != nil {
		return recordedAuditOperation{}, false, fmt.Errorf("read recorded %s operation: %w", action, err)
	}
	return recorded, true, nil
}

// commitIdentityRefusal commits the refused audit row of an identity refusal
// (best-effort: the refusal itself never depends on the annotation write) and
// returns the cause unchanged. A refusal applies no effect, so it does not
// claim the operation_id column (the id stays free for a corrected retry and
// is still recorded in the audit detail for correlation).
func commitIdentityRefusal(ctx context.Context, tx pgx.Tx, rec AuditRecord, cause error) error {
	if err := WriteAudit(ctx, tx, rec); err != nil {
		_ = tx.Rollback(ctx)
		return cause
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit identity refusal audit: %w", err)
	}
	return cause
}

// ---------------------------------------------------------------------------
// Typed refusals of the identity path
// ---------------------------------------------------------------------------

var (
	// ErrIdentityMappingMissing: the principal has no recovery_identity row at
	// all; a person cannot be proven, so registration refuses.
	ErrIdentityMappingMissing = errors.New("principal has no identity mapping; register the mapping before binding a participant")
	// ErrIdentityMappingRevoked: the principal's mapping was revoked
	// (active=FALSE); the row is kept as history and cannot be reused as proof.
	ErrIdentityMappingRevoked = errors.New("principal's identity mapping is revoked; re-activate the mapping before binding a participant")
	// ErrParticipantAlreadyRegistered: the (instance, principal, role) binding
	// already exists (PK); re-registration refuses rather than overwriting.
	ErrParticipantAlreadyRegistered = errors.New("participant binding already exists for (instance, principal, role)")
)
