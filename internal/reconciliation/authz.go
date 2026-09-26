// authz.go implements T009: the action × permission × scope evaluation helper
// for 014 reconciliation (contracts/auth-matrix.md Evaluation Source +
// Management, Q2).
//
// Rules enforced structurally here:
//   - The permission source is the dedicated recon_permission table
//     (data-model.md §1.10), matched per (principal, action, scope) with
//     default deny. 011 execution_caller_permission, 009 signer authorization,
//     and 012 operator fields are NEVER consulted or expanded.
//   - The principal is bound from an authenticated caller identity
//     (API-key middleware pattern, as in 011/012); free-text operator/reason/
//     evidence fields are audit-only and are not accepted by any evaluation
//     input. operation_id is an idempotency carriage, never an authorization
//     input, and has no place in these request types.
//   - claim is ownership only and never carries an execution right; a
//     reuse_recovery disposition only references an existing entrypoint whose
//     own authorization and gates remain mandatory (FR-020).
//   - Unconfigured rows, unknown actions, out-of-scope requests, and management
//     attempts without a valid deployment trust root are denied and audited.
//   - Management (grant/revoke/query of 014 permissions) is orthogonal to
//     recon_permission: only the controlled deployment-config trust root can
//     authorize it, so ordinary holders can never self-grant.
package reconciliation

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

var (
	// ErrInvalidPrincipal marks a principal that was not bound from an
	// authenticated caller identity.
	ErrInvalidPrincipal = errors.New("invalid reconciliation principal")

	// ErrInvalidAuthScope marks a malformed authorization scope.
	ErrInvalidAuthScope = errors.New("invalid reconciliation authorization scope")

	// ErrAuthzUnavailable marks an evaluator that is not fully wired; callers
	// MUST treat it as a denial, never as an allow.
	ErrAuthzUnavailable = errors.New("reconciliation authorization unavailable")
)

// Permission is the closed 014 permission vocabulary (data-model.md §1.10;
// migrations/000016 recon_permission.action). These values are frozen.
type Permission string

// The grantable 014 permissions. There is deliberately no management
// permission here: management authority comes only from the deployment trust
// root, never from recon_permission.
const (
	PermissionScanManage      Permission = "scan_manage"
	PermissionExceptionHandle Permission = "exception_handle"
	PermissionDisposeAck      Permission = "dispose_ack"
	PermissionDisposeReuse    Permission = "dispose_reuse"
	PermissionClose           Permission = "close"
)

// Valid reports whether p belongs to the closed permission vocabulary.
func (p Permission) Valid() bool {
	switch p {
	case PermissionScanManage, PermissionExceptionHandle, PermissionDisposeAck, PermissionDisposeReuse, PermissionClose:
		return true
	default:
		return false
	}
}

// KnownPermissions returns the closed permission vocabulary in stable order.
func KnownPermissions() []Permission {
	return []Permission{
		PermissionScanManage,
		PermissionExceptionHandle,
		PermissionDisposeAck,
		PermissionDisposeReuse,
		PermissionClose,
	}
}

// Action is the closed 014 action vocabulary of contracts/auth-matrix.md. Query
// and reverify are included because the matrix addresses them; reverify is
// system-only and never operator-grantable.
type Action string

// The recognized actions. These values are frozen vocabulary.
const (
	ActionQuery             Action = "query"
	ActionScanStart         Action = "scan_start"
	ActionScanPause         Action = "scan_pause"
	ActionScanResume        Action = "scan_resume"
	ActionClaim             Action = "claim"
	ActionDisposeAck        Action = "dispose_ack"
	ActionDisposeReuse      Action = "dispose_reuse"
	ActionDisposeNewFixRule Action = "dispose_new_fix_rule"
	ActionReverify          Action = "reverify"
	ActionVerifyClose       Action = "verify_close"
	ActionPermissionGrant   Action = "permission_grant"
	ActionPermissionRevoke  Action = "permission_revoke"
	ActionPermissionQuery   Action = "permission_query"
)

// Known reports whether a belongs to the closed action vocabulary.
func (a Action) Known() bool {
	switch a {
	case ActionQuery, ActionScanStart, ActionScanPause, ActionScanResume, ActionClaim,
		ActionDisposeAck, ActionDisposeReuse, ActionDisposeNewFixRule, ActionReverify,
		ActionVerifyClose, ActionPermissionGrant, ActionPermissionRevoke, ActionPermissionQuery:
		return true
	default:
		return false
	}
}

// actionPermission maps grantable operator actions to their recon_permission
// row. query has no single permission (any in-scope grant authorizes read);
// reverify/new_fix_rule and management actions are intentionally absent.
var actionPermission = map[Action]Permission{
	ActionScanStart:    PermissionScanManage,
	ActionScanPause:    PermissionScanManage,
	ActionScanResume:   PermissionScanManage,
	ActionClaim:        PermissionExceptionHandle,
	ActionDisposeAck:   PermissionDisposeAck,
	ActionDisposeReuse: PermissionDisposeReuse,
	ActionVerifyClose:  PermissionClose,
}

// GrantablePermission returns the concrete recon_permission value required by
// an action. ok=false means no recon_permission row can authorize the action:
// either it is unknown, query (any in-scope grant), or closed by policy
// (reverify is system-only; new_fix_rule is not approved; management actions
// evaluate against the deployment trust root). Callers MUST NOT read ok=false
// as an allow.
func GrantablePermission(action Action) (Permission, bool) {
	permission, ok := actionPermission[action]
	return permission, ok
}

// DenyReason is the stable machine reason of a refusal. It is audit vocabulary
// and MUST NOT be reworded once published.
type DenyReason string

// The refusal reasons.
const (
	// DenyUnauthenticated: no authenticated principal identity.
	DenyUnauthenticated DenyReason = "unauthenticated"
	// DenyUnknownAction: action or permission is outside the closed vocabulary.
	DenyUnknownAction DenyReason = "unknown_action"
	// DenyUnapprovedAction: the action exists but is closed by policy
	// (system-only reverify; unapproved new_fix_rule; management via
	// recon_permission).
	DenyUnapprovedAction DenyReason = "unapproved_action"
	// DenyInvalidScope: the requested scope is malformed.
	DenyInvalidScope DenyReason = "invalid_scope"
	// DenyNoGrant: no recon_permission row for the principal/action (the
	// default-deny case).
	DenyNoGrant DenyReason = "no_grant"
	// DenyOutOfScope: a grant exists but does not cover the requested scope.
	DenyOutOfScope DenyReason = "out_of_scope"
	// DenyMalformedGrant: the only matching rows are malformed; conservative
	// denial, never a partial allow.
	DenyMalformedGrant DenyReason = "malformed_grant"
	// DenyReadFailed: the permission registry could not be read; fail closed.
	DenyReadFailed DenyReason = "permission_read_failed"
	// DenyManagementUnconfigured: no valid deployment trust root.
	DenyManagementUnconfigured DenyReason = "management_unconfigured"
	// DenyNotManager: the authenticated caller is not the configured manager.
	DenyNotManager DenyReason = "not_manager"
	// DenyUnavailable: the evaluator is not wired; fail closed.
	DenyUnavailable DenyReason = "evaluator_unavailable"
)

// Decision is the complete evaluation outcome. A denied decision always carries
// a DenyReason; an allowed decision has an empty reason. Detail is a
// human-readable, secret-free explanation for the audit trail.
type Decision struct {
	Allowed bool
	Reason  DenyReason

	// Principal is the canonical authenticated identity evaluated.
	Principal string
	// TargetPrincipal is set for management requests.
	TargetPrincipal string
	// Action is the attempted action.
	Action Action
	// Permission is the concrete permission required (empty for query).
	Permission Permission
	// Scope is the requested scope.
	Scope AuthScope
	// RequiresTargetGates reports that the action, even when allowed, only
	// references an existing capability and its own authorization/gates MUST
	// still be satisfied (dispose_reuse: FR-020). It is never an execution
	// right by itself.
	RequiresTargetGates bool
	// Detail is a secret-free explanation for audit.
	Detail string
}

// ExecutionRight reports whether this decision itself carries any right to
// execute a downstream recovery/payment capability. It is always false: 014
// permissions authorize 014 actions only; a claim is ownership, not an
// execution right, and dispose_reuse only references an entrypoint whose own
// gates remain mandatory (FR-020, Q2).
func (d Decision) ExecutionRight() bool { return false }

// AuthScope is the resource scope of an authorization request or grant:
// chain, scope kind, business types, and an optional inclusive range
// (data-model.md §1.10). A request MUST state a concrete scope; a grant may be
// range-unbounded (both bounds nil) or one-sided (prefix/suffix), while an
// inverted grant range is malformed and never covers anything.
type AuthScope struct {
	ChainID       string         `json:"chain_id"`
	Kind          ScopeKind      `json:"kind"`
	BusinessTypes []BusinessType `json:"business_types"`
	RangeStart    *int64         `json:"range_start"`
	RangeEnd      *int64         `json:"range_end"`
}

// authScopeJSON is the frozen canonical JSON shape of AuthScope. It is a fixed
// field set with sorted business types, so the same logical scope always yields
// identical bytes and the same normalized hash (recon_permission PK).
type authScopeJSON struct {
	ChainID       string         `json:"chain_id"`
	Kind          ScopeKind      `json:"kind"`
	BusinessTypes []BusinessType `json:"business_types"`
	RangeStart    *int64         `json:"range_start"`
	RangeEnd      *int64         `json:"range_end"`
}

// Validate checks the request scope conservatively: chain, kind, and at least
// one known business type are required; bounds must be both present or both
// absent and ascending.
func (s AuthScope) Validate() error {
	if strings.TrimSpace(s.ChainID) == "" {
		return fmt.Errorf("%w: chain_id is required", ErrInvalidAuthScope)
	}
	if strings.ContainsAny(s.ChainID, "\x00\n\r\t") {
		return fmt.Errorf("%w: chain_id contains control characters", ErrInvalidAuthScope)
	}
	if !s.Kind.Known() {
		return fmt.Errorf("%w: kind %q must be %q or %q", ErrInvalidAuthScope, s.Kind, ScopeHeight, ScopeTime)
	}
	if len(s.BusinessTypes) == 0 {
		return fmt.Errorf("%w: at least one business type is required", ErrInvalidAuthScope)
	}
	for _, t := range s.BusinessTypes {
		if !t.Known() {
			return fmt.Errorf("%w: unknown business type %q", ErrInvalidAuthScope, t)
		}
	}
	if (s.RangeStart == nil) != (s.RangeEnd == nil) {
		return fmt.Errorf("%w: range bounds must be both present or both absent", ErrInvalidAuthScope)
	}
	if s.RangeStart != nil && *s.RangeStart > *s.RangeEnd {
		return fmt.Errorf("%w: inverted range %d..%d", ErrInvalidAuthScope, *s.RangeStart, *s.RangeEnd)
	}
	return nil
}

// CanonicalJSON returns the deterministic JSON bytes of the scope: fixed field
// order and sorted, deduplicated business types. It does not validate
// semantics, so callers MUST call Validate before persisting; it is safe to
// canonicalize a malformed stored row for conservative comparison.
func (s AuthScope) CanonicalJSON() ([]byte, error) {
	return json.Marshal(authScopeJSON{
		ChainID:       s.ChainID,
		Kind:          s.Kind,
		BusinessTypes: sortedAuthBusinessTypes(s.BusinessTypes),
		RangeStart:    s.RangeStart,
		RangeEnd:      s.RangeEnd,
	})
}

// Digest returns the lowercase hex SHA-256 of CanonicalJSON, the normalized
// scope hash used by the recon_permission primary key.
func (s AuthScope) Digest() (string, error) {
	canonical, err := s.CanonicalJSON()
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}

// MarshalJSON renders the canonical scope shape.
func (s AuthScope) MarshalJSON() ([]byte, error) { return s.CanonicalJSON() }

// UnmarshalJSON decodes a scope and normalizes business type order/dedup so
// decoded grants compare equal to canonical scopes.
func (s *AuthScope) UnmarshalJSON(data []byte) error {
	var raw authScopeJSON
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	*s = AuthScope{
		ChainID:       raw.ChainID,
		Kind:          raw.Kind,
		BusinessTypes: sortedAuthBusinessTypes(raw.BusinessTypes),
		RangeStart:    raw.RangeStart,
		RangeEnd:      raw.RangeEnd,
	}
	return nil
}

// Covers reports whether grant scope s authorizes every dimension of requested
// scope req. Matching is exact-or-containment, never wildcarded: chain and kind
// must be equal, every requested business type must be granted, and a bounded
// grant must fully contain the requested range. A malformed or inverted grant
// never covers anything.
func (s AuthScope) Covers(req AuthScope) bool {
	if s.ChainID == "" || req.ChainID == "" || s.ChainID != req.ChainID {
		return false
	}
	if s.Kind != req.Kind || !s.Kind.Known() {
		return false
	}
	if len(req.BusinessTypes) == 0 {
		return false
	}
	granted := make(map[BusinessType]struct{}, len(s.BusinessTypes))
	for _, t := range s.BusinessTypes {
		if !t.Known() {
			return false
		}
		granted[t] = struct{}{}
	}
	for _, want := range req.BusinessTypes {
		if !want.Known() {
			return false
		}
		if _, ok := granted[want]; !ok {
			return false
		}
	}
	return authzRangeCovered(s.RangeStart, s.RangeEnd, req.RangeStart, req.RangeEnd)
}

// Grant is one recon_permission row as consumed by evaluation. Principal is
// the canonical Principal.String() value stored in the table.
type Grant struct {
	Principal  string
	Permission Permission
	Scope      AuthScope

	// malformed marks a row whose stored scope could not be decoded. It is
	// unexported on purpose: rows with a durable shape defect are produced only
	// by the registry loader and always deny conservatively.
	malformed bool
}

// PrincipalKind names the authentication carrier a principal was bound from.
type PrincipalKind string

// The recognized carriers. apikey reuses the 011 API-key middleware pattern;
// deploy is the controlled deployment-config identity binding used to establish
// the management trust root.
const (
	PrincipalAPIKey PrincipalKind = "apikey"
	PrincipalDeploy PrincipalKind = "deploy"
)

// Principal is an authenticated caller identity. Its zero value is invalid and
// authorizes nothing. It is bound from an authentication carrier, never from
// free-text CLI input: APIKeyPrincipal binds a verified 011 caller id, and
// ConfigPrincipal binds an identity from controlled deployment configuration.
type Principal struct {
	kind PrincipalKind
	id   string
}

// APIKeyPrincipal binds the caller id of a successfully authenticated API key
// (the 011 middleware pattern) into a 014 principal. callerID MUST come from a
// verified credential result; operator free text never reaches this function.
func APIKeyPrincipal(callerID int64) (Principal, error) {
	if callerID <= 0 {
		return Principal{}, fmt.Errorf("%w: caller id must be positive, got %d", ErrInvalidPrincipal, callerID)
	}
	return Principal{kind: PrincipalAPIKey, id: strconv.FormatInt(callerID, 10)}, nil
}

// ConfigPrincipal parses a canonical "<kind>:<id>" principal from controlled
// deployment configuration (the management trust root identity binding). It
// MUST NOT be fed CLI free text: the deployment config is the trust root, and
// an invalid configuration denies management by default.
func ConfigPrincipal(raw string) (Principal, error) {
	kind, id, found := strings.Cut(strings.TrimSpace(raw), ":")
	if !found {
		return Principal{}, fmt.Errorf("%w: %q is not <kind>:<id>", ErrInvalidPrincipal, raw)
	}
	switch PrincipalKind(kind) {
	case PrincipalAPIKey, PrincipalDeploy:
	default:
		return Principal{}, fmt.Errorf("%w: unknown principal kind %q", ErrInvalidPrincipal, kind)
	}
	if err := validatePrincipalID(id); err != nil {
		return Principal{}, err
	}
	return Principal{kind: PrincipalKind(kind), id: id}, nil
}

// Kind returns the authentication carrier kind.
func (p Principal) Kind() PrincipalKind { return p.kind }

// ID returns the carrier-scoped identity.
func (p Principal) ID() string { return p.id }

// Valid reports whether the principal was bound from a carrier.
func (p Principal) Valid() bool { return p.kind != "" && p.id != "" }

// String returns the canonical stored form "<kind>:<id>" ("" when invalid).
func (p Principal) String() string {
	if !p.Valid() {
		return ""
	}
	return string(p.kind) + ":" + p.id
}

// Equal reports principal identity equality.
func (p Principal) Equal(other Principal) bool { return p == other }

// validatePrincipalID rejects empty, overlong, whitespace, or control-bearing
// identity values.
func validatePrincipalID(id string) error {
	if id == "" || len(id) > 200 {
		return fmt.Errorf("%w: principal id must be 1..200 characters", ErrInvalidPrincipal)
	}
	for _, r := range id {
		if r <= ' ' || r == 0x7f {
			return fmt.Errorf("%w: principal id contains whitespace or control characters", ErrInvalidPrincipal)
		}
	}
	return nil
}

// Evaluate is the pure decision function: authenticated principal × action ×
// scope against the caller's grant rows. It is deterministic and free of I/O,
// which is what the contract tests exercise. Default deny: an empty grant list
// (no recon_permission rows) always denies. operation_id, operator, reason, and
// evidence are not parameters by construction — fields can never authorize.
func Evaluate(principal Principal, action Action, scope AuthScope, grants []Grant) Decision {
	decision := Decision{
		Principal: principal.String(),
		Action:    action,
		Scope:     scope,
	}
	decision.RequiresTargetGates = action == ActionDisposeReuse

	if !principal.Valid() {
		decision.Reason = DenyUnauthenticated
		decision.Detail = "principal is not an authenticated caller identity"
		return decision
	}
	if !action.Known() {
		decision.Reason = DenyUnknownAction
		decision.Detail = fmt.Sprintf("unknown action %q", action)
		return decision
	}
	if err := scope.Validate(); err != nil {
		decision.Reason = DenyInvalidScope
		decision.Detail = err.Error()
		return decision
	}

	switch action {
	case ActionReverify:
		decision.Reason = DenyUnapprovedAction
		decision.Detail = "reverify is a system-only bounded read-only step; no operator grant can invoke it"
		return decision
	case ActionDisposeNewFixRule:
		decision.Reason = DenyUnapprovedAction
		decision.Detail = "new_fix_rule is not approved in this phase (FR-023)"
		return decision
	case ActionPermissionGrant, ActionPermissionRevoke, ActionPermissionQuery:
		decision.Reason = DenyUnapprovedAction
		decision.Detail = "management actions evaluate against the deployment trust root, never recon_permission"
		return decision
	}

	required, grantable := GrantablePermission(action)
	if !grantable && action != ActionQuery {
		decision.Reason = DenyUnknownAction
		decision.Detail = fmt.Sprintf("action %q has no permission mapping", action)
		return decision
	}
	if grantable {
		decision.Permission = required
	}

	principalText := principal.String()
	var sawMalformed, sawOutOfScope bool
	for _, g := range grants {
		if g.Principal != principalText {
			continue
		}
		if grantable && g.Permission != required {
			continue
		}
		if g.malformed || !g.Permission.Valid() {
			sawMalformed = true
			continue
		}
		if !g.Scope.Covers(scope) {
			sawOutOfScope = true
			continue
		}
		decision.Allowed = true
		return decision
	}

	switch {
	case sawMalformed:
		decision.Reason = DenyMalformedGrant
		decision.Detail = "matching grant rows are malformed"
	case sawOutOfScope:
		decision.Reason = DenyOutOfScope
		decision.Detail = fmt.Sprintf("no grant covers scope %s", authzScopeSummary(scope))
	case action == ActionQuery:
		decision.Reason = DenyNoGrant
		decision.Detail = "no in-scope grant authorizes read"
	default:
		decision.Reason = DenyNoGrant
		decision.Detail = fmt.Sprintf("no %s grant for principal", required)
	}
	return decision
}

// ManagementKind is the closed management-operation vocabulary.
type ManagementKind string

// The recognized management operations.
const (
	ManagementGrant  ManagementKind = "grant"
	ManagementRevoke ManagementKind = "revoke"
	ManagementQuery  ManagementKind = "query"
)

// ManagementTrust is the deployment-time management trust root: the single
// authenticated principal allowed to grant/revoke/query 014 permissions and the
// scope it may manage. It is established from controlled deployment
// configuration (identity binding + manageable scope); without a valid trust
// root every management operation is denied by default.
type ManagementTrust struct {
	Principal Principal
	Scope     AuthScope
}

// NewManagementTrust validates a trust root.
func NewManagementTrust(principal Principal, scope AuthScope) (ManagementTrust, error) {
	if !principal.Valid() {
		return ManagementTrust{}, fmt.Errorf("%w: management trust principal is required", ErrInvalidPrincipal)
	}
	if err := scope.Validate(); err != nil {
		return ManagementTrust{}, fmt.Errorf("%w: management trust scope: %w", ErrInvalidAuthScope, err)
	}
	return ManagementTrust{Principal: principal, Scope: scope}, nil
}

// Valid reports whether the trust root is configured and valid.
func (t ManagementTrust) Valid() bool {
	return t.Principal.Valid() && t.Scope.Validate() == nil
}

// ManagementConfig is the controlled deployment-config JSON shape that
// establishes the trust root, e.g.
//
//	{"principal":"deploy:ops-1","scope":{"chain_id":"1","kind":"height",
//	 "business_types":["withdrawal"],"range_start":0,"range_end":1000000}}
//
// The identity binding is deploy config, never a CLI argument.
type ManagementConfig struct {
	Principal string    `json:"principal"`
	Scope     AuthScope `json:"scope"`
}

// Trust validates and converts the configuration into a trust root.
func (c ManagementConfig) Trust() (ManagementTrust, error) {
	principal, err := ConfigPrincipal(c.Principal)
	if err != nil {
		return ManagementTrust{}, err
	}
	return NewManagementTrust(principal, c.Scope)
}

// ManagementRequest describes an intended management operation. It deliberately
// carries no operation_id, operator, reason, or evidence fields: those are
// audit/idempotency carriage only and can never constitute management
// authorization (Q2).
type ManagementRequest struct {
	Kind            ManagementKind
	TargetPrincipal Principal
	Permission      Permission
	Scope           AuthScope
}

// EvaluateManagement is the pure management decision function: it never consults
// recon_permission (orthogonality prevents self-grant), requires a valid
// deployment trust root, requires the authenticated actor to be exactly the
// configured manager, and requires the target scope to be inside the manager's
// manageable scope. Management authority is never granted through
// recon_permission rows.
func EvaluateManagement(trust *ManagementTrust, actor Principal, req ManagementRequest) Decision {
	decision := Decision{
		Principal:       actor.String(),
		TargetPrincipal: req.TargetPrincipal.String(),
		Scope:           req.Scope,
	}
	if trust == nil || !trust.Valid() {
		decision.Reason = DenyManagementUnconfigured
		decision.Detail = "no valid deployment management trust root is configured"
		return decision
	}
	if !actor.Valid() {
		decision.Reason = DenyUnauthenticated
		decision.Detail = "principal is not an authenticated caller identity"
		return decision
	}
	if !actor.Equal(trust.Principal) {
		decision.Reason = DenyNotManager
		decision.Detail = "authenticated principal is not the configured manager"
		return decision
	}

	action, ok := managementAction(req.Kind)
	if !ok {
		decision.Reason = DenyUnknownAction
		decision.Detail = fmt.Sprintf("unknown management operation %q", req.Kind)
		return decision
	}
	decision.Action = action

	if err := req.Scope.Validate(); err != nil {
		decision.Reason = DenyInvalidScope
		decision.Detail = err.Error()
		return decision
	}
	if !trust.Scope.Covers(req.Scope) {
		decision.Reason = DenyOutOfScope
		decision.Detail = "requested scope exceeds the deployment trust root scope"
		return decision
	}

	switch req.Kind {
	case ManagementGrant, ManagementRevoke:
		if !req.Permission.Valid() {
			decision.Reason = DenyUnknownAction
			decision.Detail = fmt.Sprintf("unknown permission %q", req.Permission)
			return decision
		}
		if !req.TargetPrincipal.Valid() {
			decision.Reason = DenyUnauthenticated
			decision.Detail = "target principal is required for grant/revoke"
			return decision
		}
		decision.Permission = req.Permission
	case ManagementQuery:
		if req.Permission != "" && !req.Permission.Valid() {
			decision.Reason = DenyUnknownAction
			decision.Detail = fmt.Sprintf("unknown permission filter %q", req.Permission)
			return decision
		}
		decision.Permission = req.Permission
		if req.TargetPrincipal != (Principal{}) && !req.TargetPrincipal.Valid() {
			decision.Reason = DenyUnauthenticated
			decision.Detail = "target principal filter is malformed"
			return decision
		}
	default:
		decision.Reason = DenyUnknownAction
		return decision
	}

	decision.Allowed = true
	return decision
}

// managementAction maps a management kind to its action vocabulary value.
func managementAction(kind ManagementKind) (Action, bool) {
	switch kind {
	case ManagementGrant:
		return ActionPermissionGrant, true
	case ManagementRevoke:
		return ActionPermissionRevoke, true
	case ManagementQuery:
		return ActionPermissionQuery, true
	default:
		return "", false
	}
}

// ReconPermissionTable is the 014 permission registry table (data-model.md
// §1.10, migrations/000016). It is the sole evaluation source for 014
// permissions.
const ReconPermissionTable = "recon_permission"

// reconPermissionSelectSQL reads the caller's grant rows. The (principal,
// action, scope) match itself happens in Go over the canonical scope shape so
// equality is deterministic and never operator-class dependent.
const reconPermissionSelectSQL = "SELECT principal, action, scope FROM " + ReconPermissionTable + " WHERE principal = $1"

// PermissionQueryer is the read subset shared by *pgxpool.Pool and pgx.Tx.
type PermissionQueryer interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// AuthzAuditRecord is one refusal record destined for the append-only 014 audit
// trail (data-model.md §1.8). It carries no secret material.
type AuthzAuditRecord struct {
	At              time.Time
	Principal       string
	TargetPrincipal string
	Action          Action
	Permission      Permission
	Scope           AuthScope
	Reason          DenyReason
	Detail          string
}

// AuthzAuditWriter persists refusal records. Refusals MUST be audited even when
// the caller's domain operation is itself refused; an audit failure never turns
// a denial into an allow.
type AuthzAuditWriter interface {
	RecordAuthzRefusal(ctx context.Context, record AuthzAuditRecord) error
}

// Evaluator is the DB-backed authorization helper. Create it with NewEvaluator;
// a nil or partially wired Evaluator denies and errors (fail closed).
type Evaluator struct {
	query PermissionQueryer
	audit AuthzAuditWriter
	now   func() time.Time
}

// NewEvaluator wires the permission reader and the refusal audit writer. Both
// are mandatory: authorization without an audit path would violate FR-012.
func NewEvaluator(query PermissionQueryer, audit AuthzAuditWriter) (*Evaluator, error) {
	if query == nil {
		return nil, fmt.Errorf("%w: permission query reader is required", ErrAuthzUnavailable)
	}
	if audit == nil {
		return nil, fmt.Errorf("%w: refusal audit writer is required", ErrAuthzUnavailable)
	}
	return &Evaluator{query: query, audit: audit}, nil
}

// Authorize evaluates one operator action and audits every refusal. It returns
// the decision plus an error only for infrastructure failures (registry read or
// refusal-audit write); any non-nil error MUST be treated as a denial, and the
// returned decision is still the refusal record.
func (e *Evaluator) Authorize(ctx context.Context, principal Principal, action Action, scope AuthScope) (Decision, error) {
	if e == nil || e.query == nil || e.audit == nil {
		decision := Decision{
			Principal: principal.String(),
			Action:    action,
			Scope:     scope,
			Reason:    DenyUnavailable,
			Detail:    "authorization evaluator is not wired",
		}
		return decision, fmt.Errorf("%w: evaluator is not wired", ErrAuthzUnavailable)
	}

	decision := Evaluate(principal, action, scope, nil)
	if decision.Allowed {
		return decision, nil
	}
	if decision.Reason != DenyNoGrant {
		// Structural refusal: no registry round trip, but it is still audited.
		if err := e.record(ctx, decision); err != nil {
			return decision, fmt.Errorf("record authorization refusal audit: %w", err)
		}
		return decision, nil
	}

	grants, err := loadGrants(ctx, e.query, principal.String())
	if err != nil {
		decision.Reason = DenyReadFailed
		decision.Detail = "permission registry read failed"
		auditErr := e.record(ctx, decision)
		return decision, errors.Join(err, auditErr)
	}
	decision = Evaluate(principal, action, scope, grants)
	if !decision.Allowed {
		if err := e.record(ctx, decision); err != nil {
			return decision, fmt.Errorf("record authorization refusal audit: %w", err)
		}
	}
	return decision, nil
}

// AuthorizeManagement evaluates one management operation and audits refusals.
// Allowed management operations are audited by the operation itself (T023) with
// before/after state, operator, reason, and result; this helper never writes
// recon_permission rows.
func (e *Evaluator) AuthorizeManagement(ctx context.Context, trust *ManagementTrust, actor Principal, req ManagementRequest) (Decision, error) {
	if e == nil || e.audit == nil {
		decision := Decision{
			Principal:       actor.String(),
			TargetPrincipal: req.TargetPrincipal.String(),
			Reason:          DenyUnavailable,
			Detail:          "authorization evaluator is not wired",
		}
		return decision, fmt.Errorf("%w: evaluator is not wired", ErrAuthzUnavailable)
	}
	decision := EvaluateManagement(trust, actor, req)
	if !decision.Allowed {
		if err := e.record(ctx, decision); err != nil {
			return decision, fmt.Errorf("record management refusal audit: %w", err)
		}
	}
	return decision, nil
}

// record persists one refusal as an audit row.
func (e *Evaluator) record(ctx context.Context, decision Decision) error {
	at := time.Now()
	if e.now != nil {
		at = e.now()
	}
	return e.audit.RecordAuthzRefusal(ctx, AuthzAuditRecord{
		At:              at,
		Principal:       decision.Principal,
		TargetPrincipal: decision.TargetPrincipal,
		Action:          decision.Action,
		Permission:      decision.Permission,
		Scope:           decision.Scope,
		Reason:          decision.Reason,
		Detail:          decision.Detail,
	})
}

// loadGrants reads the caller's recon_permission rows. A row whose stored scope
// cannot be decoded is marked malformed (conservative denial when it is the
// only candidate), never silently dropped and never partially trusted.
func loadGrants(ctx context.Context, query PermissionQueryer, principal string) ([]Grant, error) {
	rows, err := query.Query(ctx, reconPermissionSelectSQL, principal)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", ReconPermissionTable, err)
	}
	defer rows.Close()

	var grants []Grant
	for rows.Next() {
		var principalText, actionText string
		var scopeJSON []byte
		if err := rows.Scan(&principalText, &actionText, &scopeJSON); err != nil {
			return nil, fmt.Errorf("scan %s: %w", ReconPermissionTable, err)
		}
		grant := Grant{Principal: principalText, Permission: Permission(actionText)}
		trimmed := strings.TrimSpace(string(scopeJSON))
		if trimmed == "" || trimmed == "null" {
			grant.malformed = true
			grants = append(grants, grant)
			continue
		}
		if err := json.Unmarshal([]byte(trimmed), &grant.Scope); err != nil {
			grant.malformed = true
			grant.Scope = AuthScope{}
			grants = append(grants, grant)
			continue
		}
		grants = append(grants, grant)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate %s: %w", ReconPermissionTable, err)
	}
	return grants, nil
}

// authzRangeCovered reports whether grant bounds [gStart, gEnd] contain request
// bounds [rStart, rEnd]. Absent grant bounds mean range-unbounded and cover a
// request with or without a stated range (a time-scoped task carries no height
// range by design, so its request range is absent); a bounded grant only ever
// covers a bounded request fully inside it, and inverted bounds never match.
func authzRangeCovered(gStart, gEnd, rStart, rEnd *int64) bool {
	if gStart == nil && gEnd == nil {
		if rStart == nil && rEnd == nil {
			return true
		}
		return rStart != nil && rEnd != nil && *rStart <= *rEnd
	}
	if rStart == nil || rEnd == nil || *rStart > *rEnd {
		return false
	}
	if gStart != nil && gEnd != nil && *gStart > *gEnd {
		return false
	}
	if gStart != nil && *rStart < *gStart {
		return false
	}
	if gEnd != nil && *rEnd > *gEnd {
		return false
	}
	return true
}

// sortedAuthBusinessTypes returns sorted, deduplicated business types for a
// deterministic canonical scope.
func sortedAuthBusinessTypes(in []BusinessType) []BusinessType {
	seen := make(map[BusinessType]struct{}, len(in))
	for _, t := range in {
		seen[t] = struct{}{}
	}
	out := make([]BusinessType, 0, len(seen))
	for t := range seen {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// authzScopeSummary renders a compact secret-free scope description for audit
// details.
func authzScopeSummary(scope AuthScope) string {
	types := make([]string, len(scope.BusinessTypes))
	for i, t := range scope.BusinessTypes {
		types[i] = string(t)
	}
	rangeText := "unbounded"
	if scope.RangeStart != nil && scope.RangeEnd != nil {
		rangeText = fmt.Sprintf("%d..%d", *scope.RangeStart, *scope.RangeEnd)
	}
	return fmt.Sprintf("chain=%s kind=%s range=%s business_types=%s",
		scope.ChainID, scope.Kind, rangeText, strings.Join(types, ","))
}
