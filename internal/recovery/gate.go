// gate.go implements T012: the single derived release evaluation of the 015
// resumption gate — release_valid(I, C, S) of data-model.md §3 and
// contracts/resumption-gate.md §2.
//
// Rules of this file (all fail-closed):
//
//   - Normal state: no open `recovery` instance normally means pass-through,
//     except that any known recovery-instance target binding must have a
//     present, clean target guard. This prevents close/supersede from turning
//     unresolved target state into normal-mode pass-through. The control store
//     cannot prove cleanliness for unrelated unbound external targets without
//     a trusted inventory of those targets.
//   - Armed state: while a `recovery` instance is open, every admission denies
//     by default. A capability is released only by the derived evaluation:
//     instance open + capability in the closed set + every requires_capabilities
//     dependency release-valid + every isolation_dependency_set item verified +
//     no open/escalated gap naming the capability + the latest release decision is
//     `release` at the current (evidence_generation, evidence_hash) and is not
//     covered by a later revoke + approvals_valid + the injected phase-two
//     fundamental gate check (when wired).
//   - Single-action admission (R3): a hit and a miss both read the
//     authoritative (state, evidence_generation, evidence_hash) inside the
//     shared lock (the instance row lock used by every decision writer) before
//     the action; the judgment point is inside that lock, the lock is released
//     before the caller performs its one explicit action, and one admission
//     never covers more than that action. Revocations that commit before the
//     lock are seen and refuse; revocations after the lock take the in-flight +
//     unknown path (this gate never claims cross-system atomicity and never
//     rewinds already-committed work).
//   - Generation-aware cache (F5): the bounded TTL cache only reuses derived
//     results that are still valid at the in-lock token — the cache key
//     carrying generation/hash is not a freshness proof. Any
//     generation/hash change immediately invalidates the entry (the evaluator
//     is the discoverer, no waiting for the TTL), and approval/release
//     validity is re-derived on every admission so an old allow is never
//     reused to skip the current authorization check.
//   - Two-phase authority (F20): this gate is phase one (recovery allow). The
//     existing fund gates stay independently enforced at the real action site
//     and are never replaced, merged or short-circuited. The optional
//     FundGateChecker is only the call point for that reference; a failing
//     check refuses with hard_gate_active.
//   - No "disable the gate" switch exists, rejection classes come from the
//     closed set (data-model §3.3), and refusals are audited.
//   - The control store is reached only through a *controlstore.Store built by
//     controlstore.NewStore, so the T069 schema-version guard (unknown or
//     incompatible versions refuse as control_store_unavailable) is inherited
//     and there is no unguarded read path.
//   - Scope carriage (T050): ScopeHash is a canonical capability scope
//     (capability=<c>;chain=<id> plus optional asset/kind). The gate
//     canonicalizes it, refuses everything else (legacy opaque strings,
//     non-canonical expressions, a scope naming another capability) as
//     scope_mismatch, and evaluates requires_capabilities dependencies at the
//     explicit DependencyScope mapping of the request scope — never by
//     reusing the request scope string. The approval class of a stream is the
//     scope-level conservative classification (RequiredApprovalClassForScope)
//     resolved against the trusted deployment effect-class ruling carried in
//     GateOptions; the caller can never declare a lower class.
package recovery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/xtianxx/txharbor/internal/logx"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

const (
	// GateTTLConfigKey is the deployment configuration key that bounds the
	// gate-evaluation cache. The gate refuses to be constructed without a
	// positive TTL: there is no default and no "cache forever" mode. The
	// constant mirrors config.EnvRecoveryGateTTL (a unit test pins the two
	// together) so this file carries no dependency on the config package.
	GateTTLConfigKey = "TXHARBOR_RECOVERY_GATE_TTL"

	// GateAuditAction is the recovery_audit.action value of every gate
	// admission. Refusals and admissions are both recorded under it.
	GateAuditAction = "gate_admission"

	// defaultGateActor is recorded when the caller supplies no authenticated
	// subject. It is an audit label only; it never authorizes anything.
	defaultGateActor = "system:recovery-gate"

	// maxGateApprovalRefs bounds the approval references a release decision may
	// present. Releases reference a handful of approvals; anything larger is a
	// corrupt or abusive basis and refuses.
	maxGateApprovalRefs = 64

	// maxGateCacheEntries bounds the in-memory evaluation cache. Reaching the
	// bound only stops new entries from being cached — never admits anything.
	maxGateCacheEntries = 1024
)

// RefusalClass is one member of the closed gate refusal set of data-model.md
// §3.3 (mirrored by the recovery_audit.refusal_class CHECK).
type RefusalClass string

// The closed refusal set. no_instance is deliberately included: it is the
// normal-mode marker ("not a refusal, only marks normal"), never a refusal.
const (
	RefusalNoInstance                   RefusalClass = "no_instance"
	RefusalInstanceMismatch             RefusalClass = "instance_mismatch"
	RefusalNoRelease                    RefusalClass = "no_release"
	RefusalReleaseInvalidatedGeneration RefusalClass = "release_invalidated_generation"
	RefusalReleaseRevoked               RefusalClass = "release_revoked"
	RefusalCapabilityDependencyClosed   RefusalClass = "capability_dependency_closed"
	RefusalIsolationUnproven            RefusalClass = "isolation_unproven"
	RefusalGapOpen                      RefusalClass = "gap_open"
	RefusalApprovalMissing              RefusalClass = "approval_missing"
	RefusalApprovalIdentityUnverified   RefusalClass = controlstore.RefusalApprovalIdentityUnverified
	RefusalApprovalExecutorExcluded     RefusalClass = "approval_executor_excluded"
	RefusalApprovalStale                RefusalClass = "approval_stale"
	RefusalHardGateActive               RefusalClass = "hard_gate_active"
	RefusalControlStoreUnavailable      RefusalClass = controlstore.RefusalControlStoreUnavailable
	RefusalScopeMismatch                RefusalClass = "scope_mismatch"
)

// knownRefusalClasses is the canonical closed set in schema order.
var knownRefusalClasses = []RefusalClass{
	RefusalNoInstance,
	RefusalInstanceMismatch,
	RefusalNoRelease,
	RefusalReleaseInvalidatedGeneration,
	RefusalReleaseRevoked,
	RefusalCapabilityDependencyClosed,
	RefusalIsolationUnproven,
	RefusalGapOpen,
	RefusalApprovalMissing,
	RefusalApprovalIdentityUnverified,
	RefusalApprovalExecutorExcluded,
	RefusalApprovalStale,
	RefusalHardGateActive,
	RefusalControlStoreUnavailable,
	RefusalScopeMismatch,
}

// Known reports whether r is one of the closed gate refusal classes.
func (r RefusalClass) Known() bool {
	return slices.Contains(knownRefusalClasses, r)
}

// KnownRefusalClasses returns the closed set in schema order. The caller
// receives a fresh slice.
func KnownRefusalClasses() []RefusalClass {
	return slices.Clone(knownRefusalClasses)
}

// ApprovalClass is the closed approval-class vocabulary of
// recovery_approval.approval_class_snapshot (approval-matrix.md §1/§3).
type ApprovalClass string

const (
	ApprovalClassSingleNonExecutor ApprovalClass = "single_non_executor"
	ApprovalClassDualNonExecutor   ApprovalClass = "dual_non_executor"
)

// requiredApprovalClass is the conservative default classification of T012:
// new_withdrawal_creation, existing_withdrawal_recovery, event_publishing and
// event_consuming are high impact (dual: a real downstream delivery or a real
// downstream business effect cannot be excluded without the pending
// deployment ruling), the rest are single. T050 (scope.go) narrows the event
// capabilities by configured effect class; until that ruling exists the
// conservative dual default stands — never a single default.
var requiredApprovalClasses = map[Capability]ApprovalClass{
	CapabilityQuery:                      ApprovalClassSingleNonExecutor,
	CapabilityChainScan:                  ApprovalClassSingleNonExecutor,
	CapabilityDepositConfirmation:        ApprovalClassSingleNonExecutor,
	CapabilityExistingWithdrawalRecovery: ApprovalClassDualNonExecutor,
	CapabilityNewWithdrawalCreation:      ApprovalClassDualNonExecutor,
	CapabilityEventPublishing:            ApprovalClassDualNonExecutor,
	CapabilityEventConsuming:             ApprovalClassDualNonExecutor,
}

// RequiredApprovalClass returns the conservative approval class required for c
// at this gate layer. An unknown capability is refused with
// ErrUnknownCapability.
func RequiredApprovalClass(c Capability) (ApprovalClass, error) {
	if !c.Known() {
		return "", fmt.Errorf("%w: %q", ErrUnknownCapability, c)
	}
	return requiredApprovalClasses[c], nil
}

// FundGateChecker is the phase-two call point: the existing fund gates that
// must still pass at the real action site (FR-025/F20). This gate invokes it
// only for a candidate that phase one would otherwise allow; a non-nil error
// refuses with hard_gate_active. The checker never replaces the action site's
// own gate evaluation: nil means "not wired here", and the action site still
// enforces its original gates independently.
type FundGateChecker func(ctx context.Context, req GateRequest) error

// GateRequest is one single-action admission request. InstanceID is the
// caller's bound recovery instance (TXHARBOR_RECOVERY_INSTANCE); empty means
// "not bound" and resolves to normal pass-through unless a recovery instance
// is open (then it refuses as instance_mismatch). ScopeHash is the canonical
// capability scope of T050: the gate canonicalizes it (equivalent
// representations converge) and refuses a non-canonical, legacy opaque or
// mismatching scope as scope_mismatch; it is never treated as an
// interchangeable opaque key.
type GateRequest struct {
	InstanceID  string
	Capability  Capability
	ScopeHash   string
	Actor       string
	OperationID string
	Action      string
}

// GateDecision is the outcome of one admission. Allowed is the phase-one
// (recovery) verdict; callers MUST still run their original gates (phase two).
// Normal is true only for normal-mode pass-through (no open recovery
// instance); in that case RefusalClass carries no_instance as the normal
// marker, not a refusal.
type GateDecision struct {
	Allowed            bool
	Normal             bool
	RefusalClass       RefusalClass
	Reason             string
	InstanceID         string
	Capability         Capability
	ScopeHash          string
	EvidenceGeneration int64
	EvidenceHash       string
	// DecisionRef is the release decision row the admission was derived from
	// (empty unless allowed in recovery mode).
	DecisionRef string
	// CacheHit reports that the generation-bound isolation/gap derivation was
	// reused for this admission. It is diagnostic only: the in-lock token, the
	// release stream and approval validity were re-read regardless.
	CacheHit bool
	// PhaseTwoEvaluated reports that the injected FundGateChecker ran and
	// passed. False means phase two remains the action site's obligation.
	PhaseTwoEvaluated bool
}

// GateOptions constructs a Gate. TTL is required (GateTTLConfigKey); Now is a
// test seam for the TTL bookkeeping (nil means time.Now); FundGates is the
// optional phase-two call point; EffectClassRuling is the trusted deployment
// ruling (EffectClassRulingConfigKey) that resolves the effect-class dimension
// of a scope (T050). A nil ruling means "not configured": the event
// capabilities stay conservatively dual. A malformed ruling refuses
// construction (ErrEffectClassRuling) — a deployment configuration error is
// never guessed around. TrustedTarget must be independently derived from the
// deployment's configured data DSN for bound recovery admissions; a zero value
// is permitted at construction but refuses those admissions.
type GateOptions struct {
	TTL               time.Duration
	Now               func() time.Time
	FundGates         FundGateChecker
	EffectClassRuling EffectClassRuling
	TrustedTarget     GateTargetBinding
}

// GateTargetBinding is the deployment-trusted identity of the data target
// protected by this gate. It contains fingerprints only, never a DSN.
type GateTargetBinding struct {
	TargetGuardKey        string
	TargetRoleFingerprint string
}

// GateTargetBindingFromDSN derives the credential-free target identity used
// to bind a gate to deployment configuration. Parse errors are deliberately
// replaced because DSN parser errors may include credential material.
func GateTargetBindingFromDSN(dsn string) (GateTargetBinding, error) {
	target, err := controlstore.ParseDSNTarget(dsn)
	if err != nil {
		return GateTargetBinding{}, errors.New("trusted target DSN is invalid or not safely identifiable")
	}
	key, err := controlstore.TargetGuardKey(target)
	if err != nil {
		return GateTargetBinding{}, errors.New("trusted target identity is invalid")
	}
	role := target.DataTargetFingerprint().RoleFingerprint
	if !gateFingerprint(key) || !gateFingerprint(role) {
		return GateTargetBinding{}, errors.New("trusted target identity is incomplete")
	}
	return GateTargetBinding{TargetGuardKey: key, TargetRoleFingerprint: role}, nil
}

// Gate is the single derived release evaluator. It is safe for concurrent use.
type Gate struct {
	store     *controlstore.Store
	ttl       time.Duration
	now       func() time.Time
	fundGates FundGateChecker
	ruling    EffectClassRuling
	target    GateTargetBinding

	mu    sync.Mutex
	cache map[gateCacheKey]gateCapabilityFacts
}

// ErrGateRequest marks a request that violates the gate contract (for example
// an unknown capability). The caller must deny; the request is never turned
// into a default.
var ErrGateRequest = errors.New("recovery gate request is invalid")

// ErrGateControlStoreUnavailable marks an admission that could not be
// evaluated because the control store was unreachable or unusable. The
// decision already carries control_store_unavailable; the error carries the
// cause for logging.
var ErrGateControlStoreUnavailable = errors.New("recovery gate control store unavailable")

// NewGate builds the gate over a version-guarded control store. A nil store or
// a missing/non-positive TTL refuses: there is no default TTL and no unguarded
// evaluation path.
func NewGate(store *controlstore.Store, opts GateOptions) (*Gate, error) {
	if store == nil {
		return nil, fmt.Errorf("%w: a controlstore.Store built by controlstore.NewStore is required", ErrGateControlStoreUnavailable)
	}
	if opts.TTL <= 0 {
		return nil, fmt.Errorf("%s is required (not configured) and must be a positive duration; the gate has no default TTL", GateTTLConfigKey)
	}
	if err := opts.EffectClassRuling.Validate(); err != nil {
		return nil, err
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	return &Gate{
		store:     store,
		ttl:       opts.TTL,
		now:       now,
		fundGates: opts.FundGates,
		ruling:    opts.EffectClassRuling,
		target:    opts.TrustedTarget,
		cache:     make(map[gateCacheKey]gateCapabilityFacts),
	}, nil
}

// requiredApprovalClass is the conservative scope classification of one
// canonical scope: RequiredApprovalClassForScope with the scope's effect-class
// dimension resolved against the trusted deployment ruling (T050). It never
// weakens the compiled-in class of a capability except through the explicit
// positive ruling of the two event capabilities.
func (g *Gate) requiredApprovalClass(scope Scope) (ApprovalClass, error) {
	return RequiredApprovalClassForScope(scope, ScopeEffectClass(scope), g.ruling)
}

// Admit evaluates release_valid for one single action of req.Capability at
// req.ScopeHash. The returned decision is authoritative for phase one; a
// non-nil error means the evaluation could not complete (unknown capability,
// control store unavailable) and the caller must deny.
//
// Outside recovery mode (no open recovery instance and no unresolved guard for
// a target bound to a recovery instance) the admission passes through unchanged
// and writes no audit row. In recovery mode every outcome is audited; refusals
// always carry a closed-set refusal class.
func (g *Gate) Admit(ctx context.Context, req GateRequest) (GateDecision, error) {
	if !req.Capability.Known() {
		return GateDecision{Allowed: false, Capability: req.Capability},
			fmt.Errorf("%w: capability %q is outside the closed set; wire capabilities from this package's constants", ErrGateRequest, req.Capability)
	}
	rawScope := strings.TrimSpace(req.ScopeHash)
	if rawScope == "" {
		d := GateDecision{
			Allowed:      false,
			RefusalClass: RefusalScopeMismatch,
			Reason:       "scope_hash is required; the gate never substitutes a default scope",
			Capability:   req.Capability,
		}
		g.audit(ctx, req, d, nil)
		return d, nil
	}
	// The scope must be a canonical capability scope of the requested
	// capability (T050). A legacy opaque string, a non-canonical expression and
	// a scope naming another capability all refuse as scope_mismatch and are
	// never translated or defaulted; the recorded stream keys stay canonical,
	// so equivalent representations converge on one stream and an old opaque
	// release requires a fresh approval/release at the canonical scope.
	scope, err := ParseCapabilityScope(rawScope, req.Capability)
	if err != nil {
		d := GateDecision{
			Allowed:      false,
			RefusalClass: RefusalScopeMismatch,
			Reason:       err.Error(),
			Capability:   req.Capability,
		}
		g.audit(ctx, req, d, nil)
		return d, nil
	}
	canonical, err := scope.Canonical()
	if err != nil {
		// Unreachable: ParseCapabilityScope canonicalizes the same expression.
		d := GateDecision{
			Allowed:      false,
			RefusalClass: RefusalScopeMismatch,
			Reason:       err.Error(),
			Capability:   req.Capability,
		}
		g.audit(ctx, req, d, nil)
		return d, nil
	}
	req.ScopeHash = canonical
	if strings.TrimSpace(req.InstanceID) == "" {
		return g.admitUnbound(ctx, req)
	}
	return g.admitBound(ctx, req)
}

// admitUnbound evaluates a caller that is not bound to a recovery instance.
// No open instance (or an open documentation-only `baseline` instance) passes
// through only when every known recovery-instance target guard is clean; an
// open `recovery` instance refuses — a process that is not bound to the open
// instance must never keep acting inside recovery mode.
//
// Residual boundary: instance-open committing concurrently with an in-flight
// normal-mode admission is not ordered here (no cross-system atomicity is
// claimed); the next admission re-reads the control store and refuses.
func (g *Gate) admitUnbound(ctx context.Context, req GateRequest) (GateDecision, error) {
	openID, kind, err := g.openInstance(ctx)
	if err != nil {
		d := g.unavailableDecision(req, err)
		g.audit(ctx, req, d, nil)
		return d, fmt.Errorf("%w: %v", ErrGateControlStoreUnavailable, err)
	}
	if openID == "" || kind != "recovery" {
		unresolvedID, unresolved, guardErr := g.unresolvedRecoveryTargetGuard(ctx)
		if guardErr != nil {
			d := g.unavailableDecision(req, guardErr)
			g.audit(ctx, req, d, nil)
			return d, fmt.Errorf("%w: %v", ErrGateControlStoreUnavailable, guardErr)
		}
		if unresolved {
			d := GateDecision{
				Allowed: false, RefusalClass: RefusalIsolationUnproven,
				Reason:     "a target bound to a recovery instance has a missing or unresolved target guard; normal-mode pass-through is denied",
				InstanceID: unresolvedID, Capability: req.Capability, ScopeHash: req.ScopeHash,
			}
			g.audit(ctx, req, d, nil)
			return d, nil
		}
		return GateDecision{
			Allowed:      true,
			Normal:       true,
			RefusalClass: RefusalNoInstance,
			Reason:       "no open recovery instance; normal-mode pass-through (authoritative control-store read)",
			Capability:   req.Capability,
			ScopeHash:    req.ScopeHash,
		}, nil
	}
	d := GateDecision{
		Allowed:      false,
		RefusalClass: RefusalInstanceMismatch,
		Reason:       "a recovery instance is open and this admission is not bound to it; recovery mode denies by default",
		InstanceID:   openID,
		Capability:   req.Capability,
		ScopeHash:    req.ScopeHash,
	}
	g.audit(ctx, req, d, nil)
	return d, nil
}

// unresolvedRecoveryTargetGuard checks the trusted persisted inventory rather
// than request identity. A deployment-trusted target key scopes known bindings
// to the target this gate protects; legacy or malformed persisted keys remain
// global blockers because their target cannot be identified. If the gate has
// no valid trusted key, retain the conservative all-target check.
func (g *Gate) unresolvedRecoveryTargetGuard(ctx context.Context) (string, bool, error) {
	var id string
	trustedKey := g.target.TargetGuardKey
	if !gateFingerprint(trustedKey) {
		trustedKey = ""
	}
	err := g.store.Pool().QueryRow(ctx, `
SELECT i.instance_id::text
FROM recovery_instance i
LEFT JOIN recovery_target_guard tg ON tg.target_guard_key = i.target_guard_key
WHERE i.kind = 'recovery'
  AND ($1 = '' OR i.target_guard_key IS NULL OR i.target_guard_key !~ '^sha256:[0-9a-f]{64}$'
       OR i.target_guard_key = $1)
  AND (i.target_guard_key IS NULL OR i.target_guard_key = ''
       OR i.target_role_fingerprint IS NULL OR i.target_role_fingerprint = ''
       OR tg.target_guard_key IS NULL OR tg.disposition <> 'clean' OR tg.active_writer)
ORDER BY i.opened_at DESC, i.instance_id DESC
LIMIT 1`, trustedKey).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("read recovery target guard inventory: %w", err)
	}
	return id, true, nil
}

// admitBound evaluates an admission bound to one instance. The instance row is
// locked with the same lock every decision write uses, the authoritative
// (state, evidence_generation, evidence_hash) token is read inside that lock,
// the full derivation runs inside the lock (the judgment point), and the lock
// is released before the caller performs its action.
func (g *Gate) admitBound(ctx context.Context, req GateRequest) (GateDecision, error) {
	bound, parseErr := uuid.Parse(strings.TrimSpace(req.InstanceID))
	if parseErr != nil {
		d := GateDecision{
			Allowed:      false,
			RefusalClass: RefusalInstanceMismatch,
			Reason:       "bound instance id is not a UUID",
			Capability:   req.Capability,
			ScopeHash:    req.ScopeHash,
		}
		g.audit(ctx, req, d, nil)
		return d, nil
	}
	instanceID := bound.String()

	tx, err := g.store.Pool().Begin(ctx)
	if err != nil {
		d := g.unavailableDecision(req, err)
		g.audit(ctx, req, d, nil)
		return d, fmt.Errorf("%w: %v", ErrGateControlStoreUnavailable, err)
	}
	abort := func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }

	token, err := controlstore.LockInstance(ctx, tx, instanceID)
	if err != nil {
		abort()
		if errors.Is(err, controlstore.ErrInstanceNotFound) {
			d := GateDecision{
				Allowed:      false,
				RefusalClass: RefusalInstanceMismatch,
				Reason:       "bound instance does not exist; refusing",
				Capability:   req.Capability,
				ScopeHash:    req.ScopeHash,
			}
			// The audit row of a stale/unknown binding is attributed to the
			// currently open recovery instance when one exists, so the active
			// incident's trail records every refused caller (the caller's own
			// instance id has no row and cannot carry an FK-bound audit). The
			// returned decision keeps no instance: the caller's binding was
			// not the open instance.
			audited := d
			if openID, kind, openErr := g.openInstance(ctx); openErr == nil && openID != "" && kind == "recovery" {
				audited.InstanceID = openID
			}
			g.audit(ctx, req, audited, nil)
			return d, nil
		}
		d := g.unavailableDecision(req, err)
		g.audit(ctx, req, d, nil)
		return d, fmt.Errorf("%w: %v", ErrGateControlStoreUnavailable, err)
	}
	if token.State != "open" || token.Kind != "recovery" {
		abort()
		d := GateDecision{
			Allowed:      false,
			RefusalClass: RefusalInstanceMismatch,
			Reason:       fmt.Sprintf("bound instance is state=%s kind=%s; only an open recovery instance is gateable", token.State, token.Kind),
			InstanceID:   token.InstanceID,
			Capability:   req.Capability,
			ScopeHash:    req.ScopeHash,
		}
		g.audit(ctx, req, d, nil)
		return d, nil
	}
	// The deployment's independently configured target is required for a
	// bound admission. The locked instance binding is compared against it; no
	// request identity or instance-derived value can establish trust.
	if !gateFingerprint(g.target.TargetGuardKey) || !gateFingerprint(g.target.TargetRoleFingerprint) {
		abort()
		d := GateDecision{Allowed: false, RefusalClass: RefusalIsolationUnproven,
			Reason:     "trusted deployment target binding is missing or incomplete; refusing bound recovery admission",
			InstanceID: token.InstanceID, Capability: req.Capability, ScopeHash: req.ScopeHash}
		g.audit(ctx, req, d, nil)
		return d, nil
	}
	if token.TargetGuardKey != g.target.TargetGuardKey || token.TargetRoleFingerprint != g.target.TargetRoleFingerprint {
		abort()
		d := GateDecision{Allowed: false, RefusalClass: RefusalInstanceMismatch,
			Reason:     "bound recovery instance target does not match the trusted deployment target",
			InstanceID: token.InstanceID, Capability: req.Capability, ScopeHash: req.ScopeHash}
		g.audit(ctx, req, d, nil)
		return d, nil
	}
	// The locked persisted binding has matched the immutable deployment
	// binding above. Continue using the locked row for guard access; never allow
	// request-supplied identity to select or override the guarded target.
	if !gateFingerprint(token.TargetGuardKey) || !gateFingerprint(token.TargetRoleFingerprint) {
		abort()
		d := GateDecision{Allowed: false, RefusalClass: RefusalIsolationUnproven,
			Reason:     "bound recovery instance has no persisted target binding; refusing legacy or incomplete instance",
			InstanceID: token.InstanceID, Capability: req.Capability, ScopeHash: req.ScopeHash}
		g.audit(ctx, req, d, nil)
		return d, nil
	}
	guard, found, guardErr := controlstore.ReadTargetGuard(ctx, tx, token.TargetGuardKey)
	if guardErr != nil {
		abort()
		d := g.unavailableDecision(req, guardErr)
		d.InstanceID = token.InstanceID
		g.audit(ctx, req, d, nil)
		return d, fmt.Errorf("%w: %v", ErrGateControlStoreUnavailable, guardErr)
	}
	if !found || guard.Key != token.TargetGuardKey || guard.State != controlstore.TargetGuardClean || guard.ActiveWriter {
		abort()
		d := GateDecision{Allowed: false, RefusalClass: RefusalIsolationUnproven,
			Reason:     fmt.Sprintf("persisted target guard is missing or unresolved (state=%q active_writer=%t); recovery release is denied", guard.State, guard.ActiveWriter),
			InstanceID: token.InstanceID, Capability: req.Capability, ScopeHash: req.ScopeHash}
		g.audit(ctx, req, d, nil)
		return d, nil
	}

	g.invalidateCache(token)
	ev := g.evaluateLocked(ctx, tx, token, req)
	abort() // release the lock before the caller's single action

	d := GateDecision{
		InstanceID:         token.InstanceID,
		Capability:         req.Capability,
		ScopeHash:          req.ScopeHash,
		EvidenceGeneration: token.EvidenceGeneration,
		EvidenceHash:       token.EvidenceHash,
		CacheHit:           ev.cacheHit,
	}
	generation := token.EvidenceGeneration
	if ev.allowed {
		d.Allowed = true
		d.DecisionRef = ev.releaseID
		d.PhaseTwoEvaluated = ev.phaseTwoEvaluated
		d.Reason = fmt.Sprintf("release decision %s is current at evidence generation %d", ev.releaseID, token.EvidenceGeneration)
		g.audit(ctx, req, d, &generation)
		return d, nil
	}
	d.RefusalClass = ev.refusal.class
	d.Reason = ev.refusal.reason
	g.audit(ctx, req, d, &generation)
	if ev.refusal.cause != nil {
		return d, fmt.Errorf("%w: %v", ErrGateControlStoreUnavailable, ev.refusal.cause)
	}
	return d, nil
}

func gateFingerprint(value string) bool {
	if len(value) != len("sha256:")+64 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	for _, r := range value[len("sha256:"):] {
		if !(r >= '0' && r <= '9' || r >= 'a' && r <= 'f') {
			return false
		}
	}
	return true
}

// openInstance reads the current open instance (at most one globally, INV-1).
func (g *Gate) openInstance(ctx context.Context) (string, string, error) {
	var id, kind string
	err := g.store.Pool().QueryRow(ctx,
		`SELECT instance_id::text, kind FROM recovery_instance WHERE state = 'open'
		 ORDER BY opened_at DESC, instance_id DESC LIMIT 1`).Scan(&id, &kind)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", nil
	}
	if err != nil {
		return "", "", err
	}
	return id, kind, nil
}

func (g *Gate) unavailableDecision(req GateRequest, cause error) GateDecision {
	reason := "control store unavailable"
	if cause != nil {
		reason = "control store unavailable: " + logx.Redact(cause.Error())
	}
	return GateDecision{
		Allowed:      false,
		RefusalClass: RefusalControlStoreUnavailable,
		Reason:       reason,
		Capability:   req.Capability,
		ScopeHash:    req.ScopeHash,
	}
}

// ---------------------------------------------------------------------------
// Derived evaluation (runs entirely inside the instance row lock)
// ---------------------------------------------------------------------------

// gateRefusal is one internal refusal with its closed class, human reason and
// optional infrastructure cause.
type gateRefusal struct {
	class  RefusalClass
	reason string
	cause  error
}

// gateEvaluation is the evaluation outcome inside the lock.
type gateEvaluation struct {
	ids               *gateIdentities
	allowed           bool
	refusal           *gateRefusal
	cacheHit          bool
	phaseTwoEvaluated bool
	releaseID         string
}

// evaluateLocked runs release_valid for the requested capability: every
// requires_capabilities dependency first (evaluation order of
// DependencyClosure, each evaluated once), then the capability itself, then
// the phase-two call point. Formula order of data-model §3 is preserved:
// dependencies, isolation set, gaps, release stream, approvals, fund gates.
//
// Dependency coverage is the explicit T050 mapping: each dependency is
// evaluated at DependencyScope(requested, dep) — the same chain/asset/business
// type with the capability dimension replaced — never by reusing the
// dependent capability's scope string. The request scope is re-validated here
// (canonical capability scope) because lifecycle guards (instance close) feed
// recorded scopes straight into this evaluator; a non-canonical or
// mismatching recorded scope refuses as scope_mismatch instead of being read
// as a release.
func (g *Gate) evaluateLocked(ctx context.Context, tx pgx.Tx, token controlstore.InstanceToken, req GateRequest) gateEvaluation {
	ev := gateEvaluation{
		ids: &gateIdentities{
			ctx:        ctx,
			tx:         tx,
			instanceID: token.InstanceID,
			openedBy:   token.OpenedBy,
			mappings:   make(map[string]gateMapping),
		},
	}

	requested, err := ParseCapabilityScope(req.ScopeHash, req.Capability)
	if err != nil {
		// A scope violation is a refusal, never an infrastructure cause: the
		// caller must surface scope_mismatch and not control_store_unavailable.
		return gateEvaluation{refusal: &gateRefusal{class: RefusalScopeMismatch, reason: err.Error()}}
	}
	closure, err := DependencyClosure(req.Capability)
	if err != nil {
		// Impossible for the compiled-in matrix (ValidateCapabilityMatrix);
		// refuse rather than evaluate a partial closure.
		return gateEvaluation{refusal: &gateRefusal{class: RefusalCapabilityDependencyClosed, reason: err.Error(), cause: err}}
	}
	for _, dep := range closure {
		depScope, err := DependencyScope(requested, dep).Canonical()
		if err != nil {
			// Impossible for a validated request scope; refuse instead of
			// evaluating the dependency at a guessed scope.
			return gateEvaluation{refusal: &gateRefusal{class: RefusalCapabilityDependencyClosed, reason: err.Error(), cause: err}}
		}
		r, _ := g.evaluateSelfLocked(ctx, tx, token, depScope, dep, &ev)
		if r == nil {
			continue
		}
		if r.class == RefusalControlStoreUnavailable {
			return gateEvaluation{refusal: r, cacheHit: ev.cacheHit}
		}
		return gateEvaluation{refusal: &gateRefusal{
			class:  RefusalCapabilityDependencyClosed,
			reason: fmt.Sprintf("required capability %s is not release-valid at its mapped dependency scope %s: %s (%s)", dep, depScope, r.class, r.reason),
		}, cacheHit: ev.cacheHit}
	}
	r, releaseID := g.evaluateSelfLocked(ctx, tx, token, req.ScopeHash, req.Capability, &ev)
	if r != nil {
		return gateEvaluation{refusal: r, cacheHit: ev.cacheHit}
	}
	if g.fundGates != nil {
		if err := g.fundGates(ctx, req); err != nil {
			return gateEvaluation{refusal: &gateRefusal{
				class:  RefusalHardGateActive,
				reason: "an existing fund gate is active at the action site: " + logx.Redact(err.Error()),
			}, cacheHit: ev.cacheHit}
		}
	}
	return gateEvaluation{allowed: true, cacheHit: ev.cacheHit, phaseTwoEvaluated: g.fundGates != nil, releaseID: releaseID}
}

// evaluateSelfLocked evaluates one capability (not its dependencies):
// isolation dependency set verified, no open gap, current release decision,
// valid approvals. On success it also returns the release decision row it
// validated.
func (g *Gate) evaluateSelfLocked(ctx context.Context, tx pgx.Tx, token controlstore.InstanceToken, scope string, c Capability, ev *gateEvaluation) (*gateRefusal, string) {
	facts, err := g.capabilityFacts(ctx, token, tx, c, ev)
	if err != nil {
		return &gateRefusal{class: RefusalControlStoreUnavailable, reason: err.Error(), cause: err}, ""
	}
	if r := g.validateCapabilityFacts(facts, c, ev); r != nil {
		return r, ""
	}

	latest, err := g.latestRelease(ctx, tx, token, c, scope)
	if err != nil {
		return &gateRefusal{class: RefusalControlStoreUnavailable, reason: err.Error(), cause: err}, ""
	}
	if !latest.Found {
		return &gateRefusal{class: RefusalNoRelease, reason: fmt.Sprintf("no release decision exists for capability %s at this scope", c)}, ""
	}
	if latest.Decision != "release" {
		return &gateRefusal{class: RefusalReleaseRevoked, reason: fmt.Sprintf("the latest decision for capability %s is a revoke (decision %s)", c, latest.DecisionID)}, ""
	}
	if latest.EvidenceGeneration != token.EvidenceGeneration || latest.EvidenceHash != token.EvidenceHash {
		return &gateRefusal{class: RefusalReleaseInvalidatedGeneration, reason: fmt.Sprintf(
			"release decision %s is bound to generation=%d hash=%s but the instance is at generation=%d hash=%s",
			latest.DecisionID, latest.EvidenceGeneration, latest.EvidenceHash, token.EvidenceGeneration, token.EvidenceHash)}, ""
	}
	if r := g.approvalsValid(ctx, tx, token, scope, c, latest, ev); r != nil {
		return r, ""
	}
	return nil, latest.DecisionID
}

// latestRelease reads the current release/revoke decision of (I, C, S) inside
// the lock (commit order, never created_at).
func (g *Gate) latestRelease(ctx context.Context, tx pgx.Tx, token controlstore.InstanceToken, c Capability, scope string) (controlstore.DecisionState, error) {
	return g.store.CurrentReleaseDecision(ctx, tx, controlstore.DecisionKey{
		InstanceID: token.InstanceID,
		Capability: string(c),
		ScopeHash:  scope,
	})
}

// validateCapabilityFacts applies the isolation-set and gap conditions. It
// re-checks the verifier identity of every verified isolation item against the
// current participants/mappings on every admission (the schema cannot express
// "verified_by is not this instance's executor" cross-row; T029 refuses at
// write time and the gate re-derives here).
func (g *Gate) validateCapabilityFacts(facts gateCapabilityFacts, c Capability, ev *gateEvaluation) *gateRefusal {
	for _, row := range facts.isolation {
		if row.state != "verified" {
			return &gateRefusal{class: RefusalIsolationUnproven, reason: fmt.Sprintf(
				"isolation item %s of capability %s is %q (must be verified)", row.item, c, row.state)}
		}
		executor, err := ev.ids.isExecutorIdentity(row.verifiedBy, "")
		if err != nil {
			return &gateRefusal{class: RefusalControlStoreUnavailable, reason: err.Error(), cause: err}
		}
		if executor {
			return &gateRefusal{class: RefusalIsolationUnproven, reason: fmt.Sprintf(
				"isolation item %s was verified by %s, which resolves to this instance's executor (self-verification)", row.item, row.verifiedBy)}
		}
	}
	if facts.gapOpen {
		return &gateRefusal{class: RefusalGapOpen, reason: fmt.Sprintf(
			"an open or escalated gap lists capability %s in affected_capabilities", c)}
	}
	return nil
}

// ---------------------------------------------------------------------------
// approvals_valid
// ---------------------------------------------------------------------------

// approvalsValid re-derives the approval basis of a release decision
// (data-model §3.1, approval-matrix §3):
//
//   - every referenced approval exists, is an approve of this (I, C, S);
//   - it is its principal's current decision (a later revoke or re-approve
//     covers it);
//   - its generation/hash equal the current instance token;
//   - its principal is registered with the approver role on this instance;
//   - its recorded person_id matches the principal's current active mapping;
//   - its person is not the instance executor (opened_by or any executor-role
//     participant, directly or through the current mapping — the same person
//     under another principal is still excluded);
//   - the count/distinctness satisfies the conservative required class: dual
//     needs two approvals from distinct principals with distinct person_ids;
//     single needs one.
//
// requirements are never satisfied by a missing mapping, an unregistered
// principal, a wrong role, a self-approval, a stale generation or a revoke.
func (g *Gate) approvalsValid(ctx context.Context, tx pgx.Tx, token controlstore.InstanceToken, scope string, c Capability, release controlstore.DecisionState, ev *gateEvaluation) *gateRefusal {
	if len(release.ApprovalRefs) > maxGateApprovalRefs {
		return &gateRefusal{class: RefusalApprovalMissing, reason: fmt.Sprintf(
			"the release decision references %d approvals, above the bound of %d", len(release.ApprovalRefs), maxGateApprovalRefs)}
	}
	refs := dedupeApprovalRefs(release.ApprovalRefs)
	if len(refs) == 0 {
		return &gateRefusal{class: RefusalApprovalMissing, reason: "the release decision references no approvals"}
	}
	rows, err := g.readApprovalsByID(ctx, tx, refs)
	if err != nil {
		return &gateRefusal{class: RefusalControlStoreUnavailable, reason: err.Error(), cause: err}
	}
	// The required class is the scope-level conservative classification (T050):
	// capability plus the scope's effect-class dimension resolved against the
	// trusted deployment ruling. It is never the caller's declaration and never
	// weaker than the compiled-in class except through an explicit positive
	// ruling of the two event capabilities.
	requested, err := ParseCapabilityScope(scope, c)
	if err != nil {
		return &gateRefusal{class: RefusalScopeMismatch, reason: err.Error()}
	}
	required, err := g.requiredApprovalClass(requested)
	if err != nil {
		return &gateRefusal{class: RefusalApprovalMissing, reason: err.Error(), cause: err}
	}
	if err := ev.ids.ensureParticipants(); err != nil {
		return &gateRefusal{class: RefusalControlStoreUnavailable, reason: err.Error(), cause: err}
	}

	qualified := make(map[string]string, len(refs)) // principal -> person
	for _, ref := range refs {
		row, ok := rows[ref]
		if !ok {
			return &gateRefusal{class: RefusalApprovalMissing, reason: fmt.Sprintf("release references approval %s which does not exist", ref)}
		}
		if row.instanceID != token.InstanceID || row.capability != string(c) {
			return &gateRefusal{class: RefusalScopeMismatch, reason: fmt.Sprintf(
				"release references approval %s recorded for a different instance/capability", ref)}
		}
		// Scope equality is canonical equality (T050): the recorded scope and
		// the release scope must be the same canonical capability scope. A
		// legacy opaque or non-canonical recorded scope matches nothing and
		// refuses as scope_mismatch, so old approvals can never be reused.
		if err := CheckScopeMatch(scope, row.scopeHash); err != nil {
			return &gateRefusal{class: RefusalScopeMismatch, reason: fmt.Sprintf(
				"release references approval %s recorded for a different scope: %v", ref, err)}
		}
		if row.decision != "approve" {
			return &gateRefusal{class: RefusalApprovalMissing, reason: fmt.Sprintf("release references approval %s whose decision is %q", ref, row.decision)}
		}
		if !ev.ids.approvers[row.principal] {
			return &gateRefusal{class: RefusalApprovalMissing, reason: fmt.Sprintf(
				"approval %s was recorded by %s, which is not registered with the approver role on this instance", ref, row.principal)}
		}
		latest, err := g.store.CurrentApprovalDecision(ctx, tx, controlstore.DecisionKey{
			InstanceID: token.InstanceID,
			Capability: string(c),
			ScopeHash:  scope,
		}, row.principal)
		if err != nil {
			return &gateRefusal{class: RefusalControlStoreUnavailable, reason: err.Error(), cause: err}
		}
		if !latest.Found || latest.DecisionID != ref || latest.Decision != "approve" {
			return &gateRefusal{class: RefusalApprovalMissing, reason: fmt.Sprintf(
				"approval %s is not the current approve decision of %s (a later decision covers it)", ref, row.principal)}
		}
		if latest.EvidenceGeneration != token.EvidenceGeneration || latest.EvidenceHash != token.EvidenceHash {
			return &gateRefusal{class: RefusalApprovalStale, reason: fmt.Sprintf(
				"approval %s is bound to generation=%d hash=%s but the instance is at generation=%d hash=%s",
				ref, latest.EvidenceGeneration, latest.EvidenceHash, token.EvidenceGeneration, token.EvidenceHash)}
		}
		if required == ApprovalClassDualNonExecutor && row.approvalClass != string(ApprovalClassDualNonExecutor) {
			return &gateRefusal{class: RefusalApprovalMissing, reason: fmt.Sprintf(
				"capability %s requires %s but approval %s was recorded as %s", c, required, ref, row.approvalClass)}
		}
		person, active, err := ev.ids.mappingFor(row.principal)
		if err != nil {
			return &gateRefusal{class: RefusalControlStoreUnavailable, reason: err.Error(), cause: err}
		}
		if !active || person != row.personID {
			return &gateRefusal{class: RefusalApprovalIdentityUnverified, reason: fmt.Sprintf(
				"approval %s recorded person_id %s for %s, which does not match the current active mapping (F19: a mapping change invalidates the approval)", ref, row.personID, row.principal)}
		}
		executor, err := ev.ids.isExecutorIdentity(row.principal, row.personID)
		if err != nil {
			return &gateRefusal{class: RefusalControlStoreUnavailable, reason: err.Error(), cause: err}
		}
		if executor {
			return &gateRefusal{class: RefusalApprovalExecutorExcluded, reason: fmt.Sprintf(
				"approval %s by %s is excluded: the approver resolves to this instance's executor person", ref, row.principal)}
		}
		qualified[row.principal] = person
	}

	switch required {
	case ApprovalClassDualNonExecutor:
		if len(qualified) < 2 {
			return &gateRefusal{class: RefusalApprovalMissing, reason: fmt.Sprintf(
				"capability %s requires dual approval but only %d valid non-executor approval(s) are referenced", c, len(qualified))}
		}
		persons := make(map[string]bool, len(qualified))
		for _, person := range qualified {
			persons[person] = true
		}
		if len(persons) < 2 {
			return &gateRefusal{class: RefusalApprovalIdentityUnverified, reason: fmt.Sprintf(
				"capability %s requires dual approval by two different people; the referenced approvals resolve to %d distinct person(s) (the same person under two principals is not two people)", c, len(persons))}
		}
	default:
		if len(qualified) < 1 {
			return &gateRefusal{class: RefusalApprovalMissing, reason: fmt.Sprintf(
				"capability %s requires a single non-executor approval but none of the referenced approvals is valid", c)}
		}
	}
	return nil
}

// gateApproval is one approval row read by release reference.
type gateApproval struct {
	instanceID    string
	capability    string
	scopeHash     string
	decision      string
	approvalClass string
	principal     string
	personID      string
	generation    int64
	hash          string
}

// readApprovalsByID reads the referenced approval rows. The lookup is by
// approval_id (text comparison over the UUID column); the commit-order
// validity of each stream is re-derived through CurrentApprovalDecision.
func (g *Gate) readApprovalsByID(ctx context.Context, tx pgx.Tx, refs []string) (map[string]gateApproval, error) {
	const sql = `
SELECT approval_id::text, instance_id::text, capability, scope_hash, decision,
       approval_class_snapshot, principal, person_id, evidence_generation, evidence_hash
FROM recovery_approval
WHERE approval_id::text = ANY($1)`
	rows, err := tx.Query(ctx, sql, refs)
	if err != nil {
		return nil, fmt.Errorf("read release approval references: %w", err)
	}
	defer rows.Close()
	out := make(map[string]gateApproval, len(refs))
	for rows.Next() {
		var (
			id  string
			row gateApproval
		)
		if err := rows.Scan(&id, &row.instanceID, &row.capability, &row.scopeHash, &row.decision,
			&row.approvalClass, &row.principal, &row.personID, &row.generation, &row.hash); err != nil {
			return nil, fmt.Errorf("scan approval reference: %w", err)
		}
		out[id] = row
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read release approval references: %w", err)
	}
	return out, nil
}

func dedupeApprovalRefs(raw []string) []string {
	out := make([]string, 0, len(raw))
	seen := make(map[string]bool, len(raw))
	for _, ref := range raw {
		ref = strings.TrimSpace(ref)
		if ref == "" || seen[ref] {
			continue
		}
		seen[ref] = true
		out = append(out, ref)
	}
	slices.Sort(out)
	return out
}

// ---------------------------------------------------------------------------
// Participants / identity mappings (read inside the admission transaction)
// ---------------------------------------------------------------------------

type gateMapping struct {
	personID string
	found    bool
	active   bool
}

// gateIdentities loads the identity facts an admission needs, lazily and
// inside the admission transaction. Identity rows are never read from the data
// DB (data-model §1.3): a rolled-back data DB can never re-grant or re-map a
// person.
type gateIdentities struct {
	ctx        context.Context
	tx         pgx.Tx
	instanceID string
	openedBy   string

	loadedParticipants bool
	approvers          map[string]bool
	executorPrincipals map[string]bool
	executorPersons    map[string]bool

	mappings map[string]gateMapping
}

// ensureParticipants loads the instance's participants and derives the
// executor sets: opened_by plus every executor-role binding, and their
// recorded and currently mapped person_ids (a mapping change can only widen
// the exclusion, never narrow it).
func (ids *gateIdentities) ensureParticipants() error {
	if ids.loadedParticipants {
		return nil
	}
	ids.approvers = make(map[string]bool)
	ids.executorPrincipals = make(map[string]bool)
	ids.executorPersons = make(map[string]bool)
	if ids.openedBy != "" {
		ids.executorPrincipals[ids.openedBy] = true
	}
	rows, err := ids.tx.Query(ids.ctx,
		`SELECT principal, person_id, role FROM recovery_participant WHERE instance_id = $1`, ids.instanceID)
	if err != nil {
		return fmt.Errorf("read instance participants: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var principal, personID, role string
		if err := rows.Scan(&principal, &personID, &role); err != nil {
			return fmt.Errorf("scan instance participant: %w", err)
		}
		switch role {
		case "executor":
			ids.executorPrincipals[principal] = true
			ids.executorPersons[personID] = true
		case "approver":
			ids.approvers[principal] = true
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read instance participants: %w", err)
	}
	ids.loadedParticipants = true
	for principal := range ids.executorPrincipals {
		person, active, err := ids.mappingFor(principal)
		if err != nil {
			return err
		}
		if active {
			ids.executorPersons[person] = true
		}
	}
	return nil
}

// mappingFor resolves one principal's current mapping (active or revoked).
// mapping is false when no row exists. Reads are cached per admission.
func (ids *gateIdentities) mappingFor(principal string) (string, bool, error) {
	if principal == "" {
		return "", false, nil
	}
	if m, ok := ids.mappings[principal]; ok {
		return m.personID, m.active, nil
	}
	var m gateMapping
	err := ids.tx.QueryRow(ids.ctx,
		`SELECT person_id, active FROM recovery_identity WHERE principal = $1`, principal).
		Scan(&m.personID, &m.active)
	if errors.Is(err, pgx.ErrNoRows) {
		ids.mappings[principal] = gateMapping{}
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("read identity mapping %s: %w", principal, err)
	}
	m.found = true
	ids.mappings[principal] = m
	return m.personID, m.active, nil
}

// isExecutorIdentity reports whether a principal (and/or a recorded person_id)
// resolves to this instance's executor: the principal itself, the recorded
// person, or the principal's current mapped person. identityField may carry
// either form (isolation_check.verified_by has no format CHECK).
func (ids *gateIdentities) isExecutorIdentity(principal, recordedPerson string) (bool, error) {
	if err := ids.ensureParticipants(); err != nil {
		return false, err
	}
	if principal != "" {
		if ids.executorPrincipals[principal] || ids.executorPersons[principal] {
			return true, nil
		}
		person, active, err := ids.mappingFor(principal)
		if err != nil {
			return false, err
		}
		if active && ids.executorPersons[person] {
			return true, nil
		}
	}
	if recordedPerson != "" && ids.executorPersons[recordedPerson] {
		return true, nil
	}
	return false, nil
}

// ---------------------------------------------------------------------------
// Generation-bound capability facts and the bounded TTL cache
// ---------------------------------------------------------------------------

type gateCacheKey struct {
	instanceID string
	capability Capability
}

type gateIsolationRow struct {
	item       IsolationItemKey
	state      string
	verifiedBy string
}

// gateCapabilityFacts are the generation-bound derivations of one capability:
// the isolation item states and whether an open or escalated gap names the
// capability (escalation is not closure: FR-019 keeps an escalated gap
// blocking until new evidence closes it). Isolation transitions and gap
// open/close advance the evidence generation (data-model §5), so these facts
// are only reusable while the in-lock token matches the generation and hash
// they were read at.
type gateCapabilityFacts struct {
	generation int64
	hash       string
	expiresAt  time.Time
	isolation  []gateIsolationRow
	gapOpen    bool
}

// capabilityFacts returns the capability facts, reusing a still-valid cache
// entry when the in-lock token matches it, otherwise reading the control store
// inside the admission transaction. A cache hit never skips the token read,
// the release stream or approval validity; it only reuses the generation-bound
// isolation/gap derivation.
func (g *Gate) capabilityFacts(ctx context.Context, token controlstore.InstanceToken, tx pgx.Tx, c Capability, ev *gateEvaluation) (gateCapabilityFacts, error) {
	key := gateCacheKey{instanceID: token.InstanceID, capability: c}
	if entry, ok := g.cacheGet(key, token); ok {
		ev.cacheHit = true
		return entry, nil
	}
	items, err := IsolationDependencySet(c)
	if err != nil {
		return gateCapabilityFacts{}, err
	}
	facts := gateCapabilityFacts{
		generation: token.EvidenceGeneration,
		hash:       token.EvidenceHash,
		expiresAt:  g.now().Add(g.ttl),
		isolation:  make([]gateIsolationRow, 0, len(items)),
	}
	for _, item := range items {
		row := gateIsolationRow{item: item}
		err := tx.QueryRow(ctx,
			`SELECT state, COALESCE(verified_by, '') FROM recovery_isolation_check
			 WHERE instance_id = $1 AND item_key = $2`, token.InstanceID, string(item)).
			Scan(&row.state, &row.verifiedBy)
		if errors.Is(err, pgx.ErrNoRows) {
			row.state = "pending" // absent means "not verified"
		} else if err != nil {
			return gateCapabilityFacts{}, fmt.Errorf("read isolation item %s: %w", item, err)
		}
		facts.isolation = append(facts.isolation, row)
	}
	// An escalated gap blocks exactly like an open one: escalation requires
	// manual handling but is not closure and delivers no risk acceptance
	// (FR-019, C3). Only new evidence closes a gap and lifts the block.
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS (
		    SELECT 1 FROM recovery_gap
		    WHERE instance_id = $1 AND state IN ('open', 'escalated')
		      AND $2 = ANY(affected_capabilities))`,
		token.InstanceID, string(c)).Scan(&facts.gapOpen); err != nil {
		return gateCapabilityFacts{}, fmt.Errorf("read open gaps for capability %s: %w", c, err)
	}
	g.cachePut(key, facts)
	return facts, nil
}

// invalidateCache enforces F5: any generation/hash change immediately drops
// the entries of that instance, and expired entries are dropped everywhere.
// The evaluator is the discoverer; nothing waits for the TTL.
func (g *Gate) invalidateCache(token controlstore.InstanceToken) {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := g.now()
	for key, entry := range g.cache {
		if key.instanceID == token.InstanceID {
			if entry.generation != token.EvidenceGeneration || entry.hash != token.EvidenceHash || !now.Before(entry.expiresAt) {
				delete(g.cache, key)
			}
			continue
		}
		if !now.Before(entry.expiresAt) {
			delete(g.cache, key)
		}
	}
}

// cacheGet returns a reusable entry only when the in-lock token still matches
// the entry's generation and hash and the TTL has not expired.
func (g *Gate) cacheGet(key gateCacheKey, token controlstore.InstanceToken) (gateCapabilityFacts, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	entry, ok := g.cache[key]
	if !ok {
		return gateCapabilityFacts{}, false
	}
	if entry.generation != token.EvidenceGeneration || entry.hash != token.EvidenceHash || !g.now().Before(entry.expiresAt) {
		delete(g.cache, key)
		return gateCapabilityFacts{}, false
	}
	return entry, true
}

// cachePut stores one entry. Reaching the entry bound only stops new caching;
// it never affects an admission outcome.
func (g *Gate) cachePut(key gateCacheKey, entry gateCapabilityFacts) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.cache) >= maxGateCacheEntries {
		now := g.now()
		for k, e := range g.cache {
			if !now.Before(e.expiresAt) {
				delete(g.cache, k)
			}
		}
		if len(g.cache) >= maxGateCacheEntries {
			return
		}
	}
	g.cache[key] = entry
}

// cacheEntries reports the current number of cached facts (diagnostics/tests;
// it never influences an admission).
func (g *Gate) cacheEntries() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.cache)
}

// ---------------------------------------------------------------------------
// Audit (refusals and admissions; best effort — the refusal stands either way)
// ---------------------------------------------------------------------------

// audit writes one gate admission audit row. Normal-mode pass-through writes
// nothing (the daily runtime is unchanged). Every recovery-mode refusal and
// admission is recorded; a refusal always carries its closed refusal class.
func (g *Gate) audit(ctx context.Context, req GateRequest, d GateDecision, generation *int64) {
	if g.store == nil {
		return
	}
	result := controlstore.AuditRefused
	if d.Allowed {
		result = controlstore.AuditOK
	}
	actor := strings.TrimSpace(req.Actor)
	if actor == "" {
		actor = defaultGateActor
	}
	target := map[string]any{
		"capability":     string(req.Capability),
		"scope_hash":     req.ScopeHash,
		"request_action": req.Action,
	}
	if d.InstanceID != "" {
		target["instance_id"] = d.InstanceID
	}
	if d.DecisionRef != "" {
		target["release_id"] = d.DecisionRef
	}
	detail := map[string]any{
		"reason":              d.Reason,
		"normal_mode":         d.Normal,
		"phase_two_evaluated": d.PhaseTwoEvaluated,
	}
	if d.EvidenceHash != "" {
		detail["evidence_hash"] = d.EvidenceHash
	}
	rec := controlstore.AuditRecord{
		InstanceID:  d.InstanceID,
		Actor:       actor,
		Action:      GateAuditAction,
		Result:      result,
		OperationID: req.OperationID,
	}
	if result == controlstore.AuditRefused {
		rec.RefusalClass = string(d.RefusalClass)
	}
	if generation != nil {
		rec.EvidenceGeneration = generation
	}
	if encoded, err := json.Marshal(target); err == nil {
		rec.Target = encoded
	}
	if encoded, err := json.Marshal(detail); err == nil {
		rec.Detail = encoded
	}
	// Best effort by design: when the control store is the cause of the
	// refusal the annotation cannot be written and the refusal must still
	// stand (fail-closed direction).
	_ = controlstore.WriteAudit(ctx, g.store.Pool(), rec)
}
