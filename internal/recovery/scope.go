// scope.go implements T050 [US4]: the canonical capability scope (scope_hash)
// and the conservative effect-class classification of the 015 resumption gate
// (FR-021/FR-023; data-model.md §1.8/§1.9/§3.1/§4.2;
// contracts/approval-matrix.md §1–§3; contracts/resumption-gate.md §1;
// tasks.md T050).
//
// # Canonical scope
//
// FR-023 binds every approval and release to the scope it was granted for:
// chain / asset / business type / capability. This file owns the one canonical
// form of that scope. The gate and the decision writers compare scope_hash
// values by exact equality, so two callers that canonicalize the same logical
// scope differently would silently create two separate streams. ParseScope
// accepts exactly one expression grammar and Canonical emits exactly one
// string:
//
//	segment    = key "=" value                (keys sorted alphabetically)
//	keys       = asset | capability | chain | kind
//	chain      = required; uint64 > 0, decimal or 0x-prefixed hex,
//	             normalized to decimal (031337 and 0x7A69 both become 31337)
//	asset      = optional; 0x + 40 hex chars (normalized to lowercase) or a
//	             symbolic token [a-z0-9][a-z0-9._-]* (normalized to lowercase)
//	kind       = optional; business-type token [a-z0-9][a-z0-9._-]* (lowercase).
//	             withdrawal / deposit / event-delivery mirror the frozen 014
//	             vocabulary; any other bounded token is conservatively kept as
//	             its own scope dimension — never defaulted, never merged with a
//	             known one (the recovery root package must not import
//	             internal/reconciliation, doc.go hard import boundary)
//	capability = required; one of the closed seven, case-normalized
//
// Rules (all fail-closed):
//
//   - chain and capability are required. A missing/empty key, an unknown key,
//     a duplicate key, an empty value or an out-of-vocabulary value refuses
//     with the closed refusal class scope_mismatch and is never mapped onto a
//     default. Unknown keys are rejected instead of being ignored, so a caller
//     cannot smuggle a dimension past the canonical form.
//   - Whitespace around keys/values and key/value case are normalized
//     ("Chain = 031337" and "chain=0x7a69" are the same scope; "QUERY" and
//     "query" are the same capability).
//   - The canonical string is the scope_hash value: deterministic, injective
//     (values cannot contain ";" or "=", keys are sorted) and safe to record
//     verbatim and compare by equality. ScopeDigest is a derived fixed-size
//     form of the same string; callers that record a digest must use that form
//     consistently, because matching is equality of the recorded form.
//   - Only expressed dimensions appear in the canonical form. An omitted
//     optional dimension is not a wildcard and never collides with an expressed
//     one ("chain=1;capability=query" differs from
//     "asset=usdc;chain=1;capability=query").
//   - Scope matching (CheckScopeMatch / ScopeMatches) refuses every mismatch,
//     empty scope, non-canonical input and unknown vocabulary as
//     scope_mismatch (ScopeRefusal). An unknown scope never folds onto a known
//     one; two identical non-canonical legacy strings do not match either.
//   - The only database write in this file is the refused audit row of
//     AuditScopeMismatch. It goes through a *controlstore.Store built by
//     controlstore.NewStore, so the T069 schema-version guard (unknown or
//     incompatible version => control_store_unavailable) is inherited and no
//     unguarded scope audit path exists. The refusal class is always
//     scope_mismatch and the result is always refused: callers cannot forge an
//     admission row through this helper.
//
// # Entry scopes and dependency coverage (T050 wiring)
//
// CapabilityScope is the entry-scope constructor of the business entry points
// (T030–T034) and ParseCapabilityScope is the matching rule every decision
// writer, the gate and the CLI apply. Entry identity (serve surface, worker
// process, command name) is not a scope dimension: two entries that act on
// the same capability at the same chain converge on one canonical scope and
// one release covers both, while different chains, assets, business types,
// instances and contracts stay distinct — an omitted dimension is not a
// wildcard and never merges into an expressed one.
//
// Cross-capability dependency coverage is an explicit mapping, not a string
// comparison of entry scopes: DependencyScope carries the chain/asset/kind
// dimensions of a validated request scope over to the required dependency
// capability, and the gate evaluates every requires_capabilities entry at
// that mapped scope (data-model §3.2). A release of chain_scan for chain X
// therefore covers the chain_scan dependency of every dependent capability of
// chain X — and nothing else.
//
// A recorded scope outside the canonical capability form (the pre-T050 opaque
// strings such as "chain=1;surface=serve" or a canonical string with a
// different casing/order) is never translated, upgraded or matched by
// guessing: ParseCapabilityScope refuses it with scope_mismatch and the
// operator must re-approve and re-release at the canonical scope.
//
// # Conservative effect class
//
// RequiredApprovalClassForScope decides the approval class of a scope's
// capability together with the deployment effect-class ruling
// (EffectClassRuling):
//
//   - new_withdrawal_creation and existing_withdrawal_recovery are inherently
//     high impact (a real withdrawal creation / a real downstream payment
//     effect) and are always dual; the ruling can never narrow them.
//   - event_publishing (delivery to a real downstream) and event_consuming
//     (consumption that can produce a real downstream business effect) are dual
//     while the scope's effect class is empty, unknown, unconfigured, or
//     explicitly ruled real downstream. They may be classified single only when
//     the configured ruling positively labels that effect class as having no
//     real downstream effect. A ruling that merely lists real-downstream
//     classes leaves everything else dual: absence of evidence is never a
//     licence to single.
//   - The remaining capabilities (query, chain_scan, deposit_confirmation)
//     keep at least their compiled-in class (RequiredApprovalClass, gate.go);
//     an explicitly real-downstream effect class can only tighten them, never
//     relax the gate's requirement.
//   - An empty/nil ruling means "not configured": the classification stays
//     dual for the high-impact capabilities and never becomes a single default.
//   - A malformed ruling value refuses as ErrEffectClassRuling: a deployment
//     configuration error is never guessed around.
//
// The real list of downstream effect classes is a pre-deployment ruling
// (plan.md / tasks.md "部署前裁决"). Until it is configured this layer returns
// exactly the compiled-in conservative classes; it narrows only the two event
// capabilities through an explicit positive ruling, and it never relaxes
// anything else (T050 tightens, it does not widen).
//
// # Dependency direction
//
// capabilities.go stays the single source of the frozen dependency matrix.
// ValidateScopeDependencyDirection re-asserts the required direction —
// new_withdrawal_creation -> existing_withdrawal_recovery -> chain_scan and
// event_publishing -> chain_scan — and RequiredApprovalClassForScope refuses to
// classify when the compiled matrix no longer matches it. This file declares no
// dependency table of its own and never widens a requires_capabilities edge.
//
// # Boundaries
//
// This file performs no store read, no approval/release write, holds no
// writable validity boolean (INV-2) and exposes no bypass. The gate (T012)
// remains the single derived release evaluation; a classification from this
// file is a conservative input, never a release verdict.
package recovery

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

const (
	// The closed canonical scope key set. Any other key is rejected; the keys
	// are also the sort order of the canonical string (asset, capability,
	// chain, kind).
	scopeKeyAsset      = "asset"
	scopeKeyCapability = "capability"
	scopeKeyChain      = "chain"
	scopeKeyKind       = "kind"

	// maxScopeTextBytes bounds one scope expression. The bound is far above any
	// real chain/asset/kind/capability combination; it exists so a malformed or
	// abusive carriage cannot be retained or audited unbounded.
	maxScopeTextBytes = 512

	// maxScopeAssetBytes / maxScopeKindBytes bound the symbolic (non-address)
	// forms of the two optional dimensions. Unknown values are conservatively
	// preserved as distinct scope dimensions, so they are bounded here.
	maxScopeAssetBytes = 128
	maxScopeKindBytes  = 64

	// scopeDigestDomain separates the scope digest from every other hash of the
	// project. It is version-suffixed so a future canonical-form change does
	// not silently reuse old digests.
	scopeDigestDomain = "txharbor/recovery/scope/v1\n"

	// ScopeAuditAction is the recovery_audit.action value of one scope-layer
	// refusal. Refusals are append-only audit rows (result=refused) written by
	// AuditScopeMismatch.
	ScopeAuditAction = "scope_check"

	// scopeFallbackActor is recorded when a caller supplies no authenticated
	// subject for a scope refusal audit. It is an audit label only and never
	// authorizes anything.
	scopeFallbackActor = "system:recovery-scope"

	// EffectClassRulingConfigKey is the deployment configuration key of the
	// trusted effect-class ruling (a JSON object mapping effect-class tokens to
	// the closed impact set). The constant mirrors
	// config.EnvRecoveryEffectClassRuling (a unit test pins the two together)
	// so this file carries no dependency on the config package. An unconfigured
	// key means "not configured": every effect class is unknown and the event
	// capabilities stay conservatively dual.
	EffectClassRulingConfigKey = "TXHARBOR_RECOVERY_EFFECT_CLASS_RULING"

	// maxEffectClassRulingEntries bounds one deployment ruling. The bound is
	// far above any real downstream-class list; it exists so an abusive
	// carriage cannot be retained or looked up unbounded.
	maxEffectClassRulingEntries = 64
)

var (
	// ErrScopeMismatch is the sentinel of every scope-layer refusal: an empty,
	// malformed, out-of-vocabulary or non-matching scope. Its closed gate
	// refusal class is RefusalScopeMismatch. Callers deny; a scope error is
	// never turned into a default scope or a pass.
	ErrScopeMismatch = errors.New("recovery scope mismatch")

	// ErrEffectClassRuling marks a malformed effect-class ruling value. It is a
	// deployment contract error, not a downstream-impact verdict: the caller
	// denies instead of guessing a class.
	ErrEffectClassRuling = errors.New("recovery effect-class ruling is invalid")

	// ErrScopeContract marks a compiled-in dependency matrix that no longer
	// matches the frozen direction (capabilities.go is the authoritative
	// source). It is an internal contract violation, not a scope mismatch: the
	// caller denies instead of classifying on an inverted graph.
	ErrScopeContract = errors.New("recovery scope contract violated")
)

// ScopeRefusal is the scope-layer refusal error. It always maps onto the closed
// gate refusal class scope_mismatch (data-model §3.3) so a caller can surface
// and audit the class without re-deriving it. The Reason is an audit
// annotation; it is never an authorization input.
type ScopeRefusal struct {
	// Reason explains the refusal (which expression failed and why).
	Reason string
}

// Error implements error. The refusal class is part of the surfaced text so an
// entry point that only logs an error still names the closed class.
func (r *ScopeRefusal) Error() string {
	return fmt.Sprintf("recovery scope refusal [%s]: %s", RefusalScopeMismatch, r.Reason)
}

// Is reports ErrScopeMismatch as the sentinel of every scope refusal, so
// errors.Is(err, ErrScopeMismatch) holds through wrapping.
func (r *ScopeRefusal) Is(target error) bool {
	return target == ErrScopeMismatch
}

// RefusalClass returns the closed refusal class of every scope refusal:
// scope_mismatch. It exists so a caller can read the class without importing
// the string literal.
func (r *ScopeRefusal) RefusalClass() RefusalClass { return RefusalScopeMismatch }

// ScopeRefusalClass reports the closed gate refusal class carried by a
// scope-layer error. ok is false for an error that is not a scope refusal (for
// example a malformed ruling); the caller still denies in that case.
func ScopeRefusalClass(err error) (RefusalClass, bool) {
	var refusal *ScopeRefusal
	if errors.As(err, &refusal) {
		return RefusalScopeMismatch, true
	}
	return "", false
}

// scopeRefusef builds one scope refusal with a formatted reason.
func scopeRefusef(format string, args ...any) error {
	return &ScopeRefusal{Reason: fmt.Sprintf(format, args...)}
}

// ScopeKind is the business-type dimension of a canonical capability scope. The
// values mirror the frozen 014 business vocabulary
// (withdrawal / deposit / event-delivery) without importing
// internal/reconciliation (doc.go hard import boundary); a deployment may use
// any other bounded token and it is conservatively preserved as its own scope
// dimension by ParseScope.
type ScopeKind string

// The recognized 014 business-type values, for callers that construct scopes.
const (
	ScopeKindWithdrawal    ScopeKind = "withdrawal"
	ScopeKindDeposit       ScopeKind = "deposit"
	ScopeKindEventDelivery ScopeKind = "event-delivery"
)

// Known reports whether k is one of the recognized 014 business-type values.
// Unknown bounded tokens remain valid scope dimensions; Known is a display and
// construction aid, never a gate that folds an unknown kind onto a default.
func (k ScopeKind) Known() bool {
	return slices.Contains(KnownScopeKinds(), k)
}

// KnownScopeKinds returns the recognized 014 business-type values in stable
// order. The caller receives a fresh slice.
func KnownScopeKinds() []ScopeKind {
	return []ScopeKind{ScopeKindWithdrawal, ScopeKindDeposit, ScopeKindEventDelivery}
}

// Scope is one canonical capability scope: chain, optional asset, optional
// business type and capability. Constructing the struct directly is not enough
// for a usable scope_hash — Canonical normalizes and validates every dimension,
// so two callers with differently cased or spaced input converge on one string.
type Scope struct {
	// ChainID is the scope chain (required, > 0).
	ChainID uint64
	// Asset is the optional asset dimension (address or symbolic token); empty
	// means the dimension is not expressed, which is not a wildcard.
	Asset string
	// Kind is the optional business-type dimension; empty means the dimension
	// is not expressed, which is not a wildcard.
	Kind ScopeKind
	// Capability is the gate capability this scope releases (required).
	Capability Capability
}

// ParseScope parses one scope expression into its dimensions and canonical
// vocabulary. The accepted grammar is documented on this file. Every failure —
// empty input, a segment without "=", an unknown or duplicate key, an empty
// value, a chain that is not a positive uint64, a malformed asset address, an
// out-of-vocabulary capability or an oversized carriage — returns a
// *ScopeRefusal (errors.Is(err, ErrScopeMismatch)) whose class is
// scope_mismatch. No value is ever defaulted and an unknown scope never
// parses.
func ParseScope(raw string) (Scope, error) {
	text := strings.TrimSpace(raw)
	if text == "" {
		return Scope{}, scopeRefusef("scope is empty; a canonical scope needs at least %s and %s", scopeKeyChain, scopeKeyCapability)
	}
	if len(text) > maxScopeTextBytes {
		return Scope{}, scopeRefusef("scope is %d bytes, above the %d-byte bound", len(text), maxScopeTextBytes)
	}
	if strings.IndexFunc(text, func(r rune) bool { return r < 0x20 || r == 0x7f }) >= 0 {
		return Scope{}, scopeRefusef("scope contains a control character")
	}

	var (
		scope Scope
		seen  = make(map[string]bool, 4)
	)
	for _, segment := range strings.Split(text, ";") {
		key, value, ok := strings.Cut(segment, "=")
		if !ok {
			return Scope{}, scopeRefusef("scope segment %q is not a key=value pair", segment)
		}
		key = strings.ToLower(strings.TrimSpace(key))
		value = strings.TrimSpace(value)
		if value == "" {
			return Scope{}, scopeRefusef("scope key %q has an empty value", key)
		}
		if seen[key] {
			return Scope{}, scopeRefusef("scope key %q appears more than once", key)
		}
		seen[key] = true

		switch key {
		case scopeKeyChain:
			chainID, err := parseScopeChainID(value)
			if err != nil {
				return Scope{}, err
			}
			scope.ChainID = chainID
		case scopeKeyAsset:
			asset, err := canonicalScopeAsset(value)
			if err != nil {
				return Scope{}, err
			}
			scope.Asset = asset
		case scopeKeyKind:
			kind, err := canonicalScopeKind(value)
			if err != nil {
				return Scope{}, err
			}
			scope.Kind = kind
		case scopeKeyCapability:
			capability, err := canonicalScopeCapability(value)
			if err != nil {
				return Scope{}, err
			}
			scope.Capability = capability
		default:
			return Scope{}, scopeRefusef("scope key %q is outside the closed canonical key set (%s, %s, %s, %s)",
				key, scopeKeyAsset, scopeKeyCapability, scopeKeyChain, scopeKeyKind)
		}
	}
	if !seen[scopeKeyChain] {
		return Scope{}, scopeRefusef("scope is missing the required %s dimension", scopeKeyChain)
	}
	if !seen[scopeKeyCapability] {
		return Scope{}, scopeRefusef("scope is missing the required %s dimension", scopeKeyCapability)
	}
	return scope, nil
}

// Canonical returns the canonical scope_hash string of s: every dimension is
// normalized again here (case, whitespace, numeric form), the pairs are sorted
// by key and joined with ";". It refuses (scope_mismatch) a zero chain, an
// empty/unknown capability, a malformed address or an oversized symbolic
// value, so a Scope constructed by hand cannot mint a non-canonical key.
func (s Scope) Canonical() (string, error) {
	if s.ChainID == 0 {
		return "", scopeRefusef("%s 0 is not a usable scope chain; a canonical scope names a real chain", scopeKeyChain)
	}
	capability, err := canonicalScopeCapability(string(s.Capability))
	if err != nil {
		return "", err
	}
	pairs := make([]string, 0, 4)
	if s.Asset != "" {
		asset, err := canonicalScopeAsset(s.Asset)
		if err != nil {
			return "", err
		}
		pairs = append(pairs, scopeKeyAsset+"="+asset)
	}
	pairs = append(pairs, scopeKeyCapability+"="+string(capability))
	pairs = append(pairs, scopeKeyChain+"="+strconv.FormatUint(s.ChainID, 10))
	if s.Kind != "" {
		kind, err := canonicalScopeKind(string(s.Kind))
		if err != nil {
			return "", err
		}
		pairs = append(pairs, scopeKeyKind+"="+string(kind))
	}
	slices.Sort(pairs)
	return strings.Join(pairs, ";"), nil
}

// Digest returns the derived fixed-size form of the canonical scope:
// "sha256:" + hex(sha256(domain || canonical)). It is an alternative stable
// identifier for callers that need one; the recorded scope_hash form must be
// applied consistently, because the gate compares the recorded string by exact
// equality. Two equivalent scopes always digest identically; a digest never
// matches a canonical string (fail-closed).
func (s Scope) Digest() (string, error) {
	canonical, err := s.Canonical()
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256([]byte(scopeDigestDomain + canonical))
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

// CanonicalScopeHash parses raw and returns its canonical scope_hash string —
// the deterministic value the control store records and the gate compares.
// Any failure carries a *ScopeRefusal and the closed class scope_mismatch.
func CanonicalScopeHash(raw string) (string, error) {
	scope, err := ParseScope(raw)
	if err != nil {
		return "", err
	}
	return scope.Canonical()
}

// ScopeDigest returns the derived digest of the scope expression raw (see
// Scope.Digest). It is never the recorded scope_hash form unless a caller
// records it consistently.
func ScopeDigest(raw string) (string, error) {
	scope, err := ParseScope(raw)
	if err != nil {
		return "", err
	}
	return scope.Digest()
}

// CapabilityScope returns the canonical scope_hash of one capability stream
// with no further dimensions expressed: "capability=<c>;chain=<id>" in
// canonical key order. It is the entry-scope constructor of the business entry
// points (serve read/scan loops, withdrawal creation, execution/signer
// recovery, event publisher/consumer): entry identity is not a scope
// dimension, so every entry that acts on the same capability at the same chain
// derives the same scope and one release covers them all. A zero chain or an
// unknown capability refuses as scope_mismatch (fail-closed; no default
// scope).
func CapabilityScope(chainID uint64, capability Capability) (string, error) {
	return Scope{ChainID: chainID, Capability: capability}.Canonical()
}

// DependencyScope maps one validated request scope onto a required dependency
// capability: the chain, asset and business-type (kind) dimensions are carried
// over unchanged and only the capability dimension is replaced. This is the
// explicit dependency coverage mapping of data-model §3.2 — the gate never
// reuses the dependent capability's scope string for its dependency and never
// compares entry-scope strings for equality. A dependency release covers the
// dependent capability only when it was granted for the same chain, asset and
// business type.
func DependencyScope(requested Scope, dependency Capability) Scope {
	return Scope{
		ChainID:    requested.ChainID,
		Asset:      requested.Asset,
		Kind:       requested.Kind,
		Capability: dependency,
	}
}

// ParseCapabilityScope parses raw and requires that it is a canonical
// capability scope of capability c: the expression must be a valid canonical
// scope (all ParseScope rules) and its capability dimension must equal c. Any
// failure — a malformed or non-canonical expression, an unknown dimension, a
// missing capability, a legacy opaque string, or a scope naming a different
// capability — returns a *ScopeRefusal with the closed class scope_mismatch
// and no value is ever translated or defaulted.
//
// The returned Scope canonicalizes the same expression: callers that record a
// scope_hash call Canonical (or CanonicalScopeHash) and record that exact
// canonical form, because the control store keys streams by the recorded
// string and equivalent representations must converge on one key.
func ParseCapabilityScope(raw string, c Capability) (Scope, error) {
	if !c.Known() {
		return Scope{}, scopeRefusef("capability %q is outside the closed 7-capability set %v", c, KnownCapabilities())
	}
	scope, err := ParseScope(raw)
	if err != nil {
		return Scope{}, err
	}
	if scope.Capability != c {
		return Scope{}, scopeRefusef("scope names capability %s but the request is for capability %s; a scope never covers a different capability", scope.Capability, c)
	}
	if _, err := scope.Canonical(); err != nil {
		return Scope{}, err
	}
	return scope, nil
}

// ScopeEffectClass returns the effect-class key of a canonical scope: the
// scope's business-type (kind) dimension. The deployment ruling
// (EffectClassRuling) resolves that key; an omitted kind is an empty class and
// stays unknown, which classifies conservatively (dual for the two event
// capabilities). The class is never supplied by an operator flag: it is read
// from the scope expression the release/approval is bound to.
func ScopeEffectClass(scope Scope) EffectClass { return EffectClass(scope.Kind) }

// CheckScopeMatch verifies that a requested scope and the scope recorded on a
// release/approval are the same canonical scope. Any mismatch, empty scope,
// malformed or non-canonical expression, and any unknown vocabulary refuses
// with a *ScopeRefusal carrying the closed class scope_mismatch: an unknown
// scope never folds onto a known one, and a legacy opaque string that is not a
// canonical scope matches nothing. The caller denies; the returned class is
// surfaced and audited (AuditScopeMismatch).
func CheckScopeMatch(request, recorded string) error {
	requested, err := ParseScope(request)
	if err != nil {
		return scopeRefusef("requested scope is invalid: %v", err)
	}
	expected, err := ParseScope(recorded)
	if err != nil {
		return scopeRefusef("recorded scope is invalid: %v", err)
	}
	requestedCanonical, err := requested.Canonical()
	if err != nil {
		return scopeRefusef("requested scope is invalid: %v", err)
	}
	expectedCanonical, err := expected.Canonical()
	if err != nil {
		return scopeRefusef("recorded scope is invalid: %v", err)
	}
	if requestedCanonical != expectedCanonical {
		return scopeRefusef("requested scope %q does not match recorded scope %q", requestedCanonical, expectedCanonical)
	}
	return nil
}

// ScopeMatches reports whether two scope expressions are the same canonical
// scope. It is CheckScopeMatch without the error: anything it cannot prove
// equal is false (fail-closed), including two identical non-canonical strings.
func ScopeMatches(request, recorded string) bool {
	return CheckScopeMatch(request, recorded) == nil
}

// EffectClass names one deployment-defined downstream effect class (for
// example a broker topic or a downstream target identity). It is deliberately
// opaque: the list of real downstream effect classes is a pre-deployment ruling
// and is never hard-coded here. An empty or unrecognized class is unknown and
// classifies conservatively (dual for the event capabilities).
type EffectClass string

// EffectImpact is the closed impact vocabulary of one effect class in the
// deployment ruling.
type EffectImpact string

const (
	// EffectImpactRealDownstream marks an effect class that delivers to, or can
	// produce a business effect in, a real downstream system: high impact, dual.
	EffectImpactRealDownstream EffectImpact = "real_downstream"

	// EffectImpactNoRealDownstream marks an effect class positively ruled to
	// have no real downstream delivery or business effect. Only this explicit
	// label may narrow the two event capabilities to single.
	EffectImpactNoRealDownstream EffectImpact = "no_real_downstream_effect"
)

// KnownEffectImpacts returns the closed effect-impact vocabulary. The caller
// receives a fresh slice.
func KnownEffectImpacts() []EffectImpact {
	return []EffectImpact{EffectImpactRealDownstream, EffectImpactNoRealDownstream}
}

// EffectClassRuling is the deployment ruling of the real downstream effect
// classes. A nil or empty map means "not configured": every effect class is
// unknown and the classification is conservatively dual for the event
// capabilities. A class absent from a configured map is still unknown and
// stays dual — the map must positively label a class as
// no_real_downstream_effect before anything may be classified single.
type EffectClassRuling map[EffectClass]EffectImpact

// impactOf resolves one effect class against the ruling. found is false for an
// empty class, an unconfigured (nil/empty) ruling and a class the ruling does
// not name; all of those are conservative dual at the call site. A malformed
// impact value anywhere in the ruling refuses the whole lookup
// (ErrEffectClassRuling): a deployment error is never partially trusted.
func (r EffectClassRuling) impactOf(effectClass EffectClass) (EffectImpact, bool, error) {
	for key, impact := range r {
		switch impact {
		case EffectImpactRealDownstream, EffectImpactNoRealDownstream:
		default:
			return "", false, fmt.Errorf("%w: effect class %q carries impact %q outside the closed set %v",
				ErrEffectClassRuling, key, impact, KnownEffectImpacts())
		}
	}
	impact, ok := r[EffectClass(strings.TrimSpace(string(effectClass)))]
	if !ok {
		return "", false, nil
	}
	return impact, true, nil
}

// Validate refuses a ruling outside the closed impact vocabulary and a ruling
// above the entry bound. It is the construction-time check of the deployments
// that assemble a gate or record a decision: a malformed ruling is a
// deployment configuration error and the caller denies instead of guessing a
// partial class. A nil/empty ruling is valid ("not configured").
func (r EffectClassRuling) Validate() error {
	if len(r) > maxEffectClassRulingEntries {
		return fmt.Errorf("%w: ruling carries %d entries, above the bound of %d",
			ErrEffectClassRuling, len(r), maxEffectClassRulingEntries)
	}
	for key, impact := range r {
		if strings.TrimSpace(string(key)) == "" {
			return fmt.Errorf("%w: an effect-class key is empty", ErrEffectClassRuling)
		}
		switch impact {
		case EffectImpactRealDownstream, EffectImpactNoRealDownstream:
		default:
			return fmt.Errorf("%w: effect class %q carries impact %q outside the closed set %v",
				ErrEffectClassRuling, key, impact, KnownEffectImpacts())
		}
	}
	return nil
}

// ParseEffectClassRuling parses the deployment ruling carriage: a JSON object
// mapping effect-class tokens to the closed impact set
// ("real_downstream" | "no_real_downstream_effect"). Empty/blank input means
// "not configured" and returns a nil ruling (every class unknown, event
// capabilities conservatively dual). A malformed payload, an empty key, an
// unknown impact or an over-bound ruling refuses with ErrEffectClassRuling:
// the deployment configuration is never partially trusted.
//
// The ruling is trusted deployment configuration (EffectClassRulingConfigKey);
// it is never an operator or caller-declared downgrade, and it can only narrow
// the two event capabilities for scopes whose business-type dimension it
// positively labels.
func ParseEffectClassRuling(raw string) (EffectClassRuling, error) {
	text := strings.TrimSpace(raw)
	if text == "" {
		return nil, nil
	}
	var decoded map[string]string
	if err := json.Unmarshal([]byte(text), &decoded); err != nil {
		return nil, fmt.Errorf("%w: the ruling must be a JSON object mapping effect-class tokens to impacts: %v",
			ErrEffectClassRuling, err)
	}
	if len(decoded) > maxEffectClassRulingEntries {
		return nil, fmt.Errorf("%w: ruling carries %d entries, above the bound of %d",
			ErrEffectClassRuling, len(decoded), maxEffectClassRulingEntries)
	}
	if len(decoded) == 0 {
		return nil, nil
	}
	ruling := make(EffectClassRuling, len(decoded))
	for key, impact := range decoded {
		class := EffectClass(strings.TrimSpace(key))
		if class == "" {
			return nil, fmt.Errorf("%w: an effect-class key is empty", ErrEffectClassRuling)
		}
		ruling[class] = EffectImpact(strings.TrimSpace(impact))
	}
	if err := ruling.Validate(); err != nil {
		return nil, err
	}
	return ruling, nil
}

// RequiredApprovalClassForScope returns the conservative approval class of the
// scope's capability under the deployment effect-class ruling (nil/empty means
// not configured):
//
//   - new_withdrawal_creation and existing_withdrawal_recovery are always dual;
//   - event_publishing and event_consuming are dual unless the ruling
//     positively labels the scope's effect class as having no real downstream
//     effect, in which case they may be single;
//   - the remaining capabilities keep at least their compiled-in class
//     (RequiredApprovalClass, gate.go) and are only ever tightened by an
//     explicitly real-downstream ruling.
//
// It refuses a non-canonical scope (scope_mismatch), an unknown capability
// (ErrUnknownCapability) or a malformed ruling (ErrEffectClassRuling); the
// caller denies on every error. The result is never weaker than the compiled-in
// matrix for any capability other than the two explicitly ruled event
// capabilities, and it is never a release: the gate still derives release_valid
// on every admission.
func RequiredApprovalClassForScope(scope Scope, effectClass EffectClass, ruling EffectClassRuling) (ApprovalClass, error) {
	if err := ValidateScopeDependencyDirection(); err != nil {
		return "", err
	}
	if _, err := scope.Canonical(); err != nil {
		return "", err
	}
	base, err := RequiredApprovalClass(scope.Capability)
	if err != nil {
		return "", err
	}
	impact, ruled, err := ruling.impactOf(effectClass)
	if err != nil {
		return "", err
	}

	switch scope.Capability {
	case CapabilityNewWithdrawalCreation, CapabilityExistingWithdrawalRecovery:
		// Inherently high impact: a real withdrawal creation / a real
		// downstream payment effect. The ruling can never narrow them.
		return ApprovalClassDualNonExecutor, nil
	case CapabilityEventPublishing, CapabilityEventConsuming:
		// Publishing to a real downstream / consuming with a real downstream
		// business effect. Single requires the ruling to have positively
		// labeled this effect class as having no real downstream effect.
		if !ruled || impact != EffectImpactNoRealDownstream {
			return ApprovalClassDualNonExecutor, nil
		}
		return ApprovalClassSingleNonExecutor, nil
	default:
		// query / chain_scan / deposit_confirmation: the compiled-in class is
		// the floor; an explicitly real-downstream effect class only tightens.
		if ruled && impact == EffectImpactRealDownstream {
			return ApprovalClassDualNonExecutor, nil
		}
		return base, nil
	}
}

// ValidateScopeDependencyDirection re-asserts the frozen requires_capabilities
// direction against capabilities.go, the single source of the matrix:
//
//	new_withdrawal_creation -> existing_withdrawal_recovery -> chain_scan
//	event_publishing        -> chain_scan
//
// It fails when an edge is missing, when a reverse edge would invert the
// direction, or when chain_scan stops being the dependency root. The scope
// layer never declares or widens a dependency edge; this guard makes an
// inverted or widened matrix refuse the conservative classification instead of
// being silently accepted.
func ValidateScopeDependencyDirection() error {
	deps, err := RequiresCapabilities(CapabilityChainScan)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrScopeContract, err)
	}
	if len(deps) != 0 {
		return fmt.Errorf("%w: %s must stay the dependency root, got requires_capabilities=%v",
			ErrScopeContract, CapabilityChainScan, deps)
	}

	edges := []struct {
		capability Capability
		required   Capability
	}{
		{CapabilityNewWithdrawalCreation, CapabilityExistingWithdrawalRecovery},
		{CapabilityExistingWithdrawalRecovery, CapabilityChainScan},
		{CapabilityEventPublishing, CapabilityChainScan},
	}
	for _, edge := range edges {
		direct, err := RequiresCapabilities(edge.capability)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrScopeContract, err)
		}
		if !slices.Contains(direct, edge.required) {
			return fmt.Errorf("%w: dependency direction lost: %s must require %s, got %v (capabilities.go is authoritative)",
				ErrScopeContract, edge.capability, edge.required, direct)
		}
		reverse, err := RequiresCapabilities(edge.required)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrScopeContract, err)
		}
		if slices.Contains(reverse, edge.capability) {
			return fmt.Errorf("%w: dependency direction inverted: %s must not require %s, got %v",
				ErrScopeContract, edge.required, edge.capability, reverse)
		}
	}

	closure, err := DependencyClosure(CapabilityNewWithdrawalCreation)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrScopeContract, err)
	}
	chainIndex := slices.Index(closure, CapabilityChainScan)
	existingIndex := slices.Index(closure, CapabilityExistingWithdrawalRecovery)
	if chainIndex < 0 || existingIndex < 0 || chainIndex > existingIndex {
		return fmt.Errorf("%w: dependency order for %s must evaluate %s before %s, got %v",
			ErrScopeContract, CapabilityNewWithdrawalCreation, CapabilityChainScan, CapabilityExistingWithdrawalRecovery, closure)
	}
	if closure, err := DependencyClosure(CapabilityEventPublishing); err != nil {
		return fmt.Errorf("%w: %v", ErrScopeContract, err)
	} else if !slices.Contains(closure, CapabilityChainScan) {
		return fmt.Errorf("%w: %s must depend on %s, got closure %v",
			ErrScopeContract, CapabilityEventPublishing, CapabilityChainScan, closure)
	}
	return nil
}

// ScopeMismatchAudit is the carriage of one scope-mismatch refusal audit row.
// The instance id may be empty when the refusal precedes instance resolution;
// the actor falls back to a non-authorizing system label; the two scope
// expressions are audit annotations, not authorization inputs.
type ScopeMismatchAudit struct {
	InstanceID     string
	Actor          string
	RequestedScope string
	RecordedScope  string
	RequestAction  string
	OperationID    string
}

// AuditScopeMismatch appends one refused audit row (result=refused,
// action=scope_check, refusal_class=scope_mismatch) for a real scope mismatch.
// It re-runs CheckScopeMatch itself and refuses to write anything when the two
// scopes actually match, so this helper can never produce a false refusal row.
// store must be a *controlstore.Store built by controlstore.NewStore: the T069
// schema-version guard is inherited at construction and there is no unguarded
// scope audit path.
func AuditScopeMismatch(ctx context.Context, store *controlstore.Store, rec ScopeMismatchAudit) error {
	if store == nil {
		return fmt.Errorf("%w: a controlstore.Store built by controlstore.NewStore is required", ErrScopeMismatch)
	}
	mismatch := CheckScopeMatch(rec.RequestedScope, rec.RecordedScope)
	if mismatch == nil {
		return fmt.Errorf("%w: scopes %q and %q are the same canonical scope; refusing to write a false refusal row",
			ErrScopeMismatch, rec.RequestedScope, rec.RecordedScope)
	}

	actor := strings.TrimSpace(rec.Actor)
	if actor == "" {
		actor = scopeFallbackActor
	}
	operationID := strings.TrimSpace(rec.OperationID)
	target, err := json.Marshal(map[string]any{
		"requested_scope": rec.RequestedScope,
		"recorded_scope":  rec.RecordedScope,
		"request_action":  rec.RequestAction,
	})
	if err != nil {
		return fmt.Errorf("encode scope refusal target: %w", err)
	}
	detail, err := json.Marshal(map[string]any{
		"reason":       mismatch.Error(),
		"operation_id": operationID,
	})
	if err != nil {
		return fmt.Errorf("encode scope refusal detail: %w", err)
	}
	rec.InstanceID = strings.TrimSpace(rec.InstanceID)
	return controlstore.WriteAudit(ctx, store.Pool(), controlstore.AuditRecord{
		InstanceID:   rec.InstanceID,
		Actor:        actor,
		Action:       ScopeAuditAction,
		Target:       target,
		Detail:       detail,
		Result:       controlstore.AuditRefused,
		RefusalClass: string(RefusalScopeMismatch),
		OperationID:  operationID,
	})
}

// ---------------------------------------------------------------------------
// Dimension canonicalization (shared by ParseScope and Scope.Canonical)
// ---------------------------------------------------------------------------

// parseScopeChainID normalizes one chain value: a positive uint64 in decimal
// (leading zeros are normalized away) or 0x-prefixed hex. Chain 0 is refused —
// it is not a usable scope chain — and every other parse failure is a
// scope_mismatch refusal, never a default.
func parseScopeChainID(value string) (uint64, error) {
	digits, base := value, 10
	if strings.HasPrefix(value, "0x") || strings.HasPrefix(value, "0X") {
		digits, base = value[2:], 16
	}
	chainID, err := strconv.ParseUint(digits, base, 64)
	if err != nil {
		return 0, scopeRefusef("%s value %q is not a uint64 (decimal, or 0x-prefixed hex)", scopeKeyChain, value)
	}
	if chainID == 0 {
		return 0, scopeRefusef("%s value 0 is not a usable scope chain", scopeKeyChain)
	}
	return chainID, nil
}

// canonicalScopeAsset normalizes the optional asset dimension: a 0x-prefixed
// 40-hex-character address (lowercased) or a bounded symbolic token
// [a-z0-9][a-z0-9._-]* (lowercased). A malformed address is refused instead of
// being reinterpreted as a symbol, so the two forms can never collide.
func canonicalScopeAsset(value string) (string, error) {
	asset := strings.ToLower(value)
	if strings.HasPrefix(asset, "0x") {
		if len(asset) != 42 || !isHexString(asset[2:]) {
			return "", scopeRefusef("%s value %q must be 0x followed by 40 hex characters, or a symbolic asset token",
				scopeKeyAsset, value)
		}
		return asset, nil
	}
	if len(asset) > maxScopeAssetBytes || !validScopeToken(asset) {
		return "", scopeRefusef("%s value %q is not a bounded asset token ([a-z0-9][a-z0-9._-]*, at most %d bytes)",
			scopeKeyAsset, value, maxScopeAssetBytes)
	}
	return asset, nil
}

// canonicalScopeKind normalizes the optional business-type dimension to a
// bounded lowercase token. Values outside the 014 vocabulary are conservatively
// preserved (documented on this file), not rejected and never folded onto a
// known kind; the canonical string keeps them distinct.
func canonicalScopeKind(value string) (ScopeKind, error) {
	kind := strings.ToLower(value)
	if len(kind) > maxScopeKindBytes || !validScopeToken(kind) {
		return "", scopeRefusef("%s value %q is not a bounded business-type token ([a-z0-9][a-z0-9._-]*, at most %d bytes)",
			scopeKeyKind, value, maxScopeKindBytes)
	}
	return ScopeKind(kind), nil
}

// canonicalScopeCapability normalizes one capability value by case and requires
// it to belong to the closed seven-capability set. The gate's own
// ParseCapability stays exact; the scope layer is where the documented
// case/whitespace normalization of FR-023's scope carriage happens.
func canonicalScopeCapability(value string) (Capability, error) {
	capability := Capability(strings.ToLower(strings.TrimSpace(value)))
	if !capability.Known() {
		return "", scopeRefusef("capability %q is outside the closed 7-capability set %v", value, KnownCapabilities())
	}
	return capability, nil
}

// isHexString reports whether s is a non-empty lowercase hex string. Callers
// lowercase before the check.
func isHexString(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case c >= '0' && c <= '9', c >= 'a' && c <= 'f':
		default:
			return false
		}
	}
	return true
}

// validScopeToken reports whether s is a bounded symbolic token: an ASCII
// alphanumeric first byte followed by ASCII alphanumerics or ".", "_", "-".
// The character set cannot contain ";" or "=", so a value can never break out
// of its key=value segment and the canonical string stays injective.
func validScopeToken(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9':
		case i > 0 && (c == '.' || c == '_' || c == '-'):
		default:
			return false
		}
	}
	return true
}
