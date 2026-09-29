// recovery_scope.go owns the canonical capability scopes and the trusted
// effect-class ruling of the 015 business entry assemblies (T050).
//
// Scope discipline (data-model §3; contracts/approval-matrix.md §3; tasks.md
// T050):
//
//   - The business authorization scope is chain / asset / business type /
//     capability. Entry identity (serve surface, worker process, command name)
//     is not a scope dimension, so every entry that acts on the same
//     capability at the same chain derives the same canonical scope from
//     recovery.CapabilityScope and one release covers them all. Different
//     chains, assets, business types and instances stay distinct; an omitted
//     dimension is not a wildcard.
//   - No entry invents a scope string of its own and none carries an opaque
//     "surface=" dimension: a release recorded before this normalization (for
//     example "chain=1;surface=serve") matches no canonical stream and
//     requires a fresh approval and release at the canonical scope.
//   - The trusted effect-class ruling is deployment configuration
//     (recovery.EffectClassRulingConfigKey). It is parsed and validated at
//     assembly (recovery.ParseEffectClassRuling); a malformed value refuses
//     the entry by key name, and an absent value means "not configured" —
//     every effect class unknown, the two event capabilities conservatively
//     dual. Operators and callers can never declare a lower class.
package app

import (
	"fmt"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/recovery"
)

// recoveryScopeFor returns the canonical business scope of one capability on
// one chain (recovery.CapabilityScope). A zero chain or a capability outside
// the closed seven refuses: there is no default scope.
func recoveryScopeFor(chainID uint64, capability recovery.Capability) (string, error) {
	return recovery.CapabilityScope(chainID, capability)
}

// recoveryCapabilityScopes derives the canonical scope of every capability in
// the closed set for one deployment chain. Assemblies use the complete map so
// a missing/broken scope is refused at startup instead of at the first
// admission.
func recoveryCapabilityScopes(chainID uint64) (map[recovery.Capability]string, error) {
	scopes := make(map[recovery.Capability]string, len(recovery.KnownCapabilities()))
	for _, capability := range recovery.KnownCapabilities() {
		scope, err := recoveryScopeFor(chainID, capability)
		if err != nil {
			return nil, err
		}
		scopes[capability] = scope
	}
	return scopes, nil
}

// recoveryEffectClassRuling resolves the trusted deployment effect-class
// ruling from the parsed configuration. A malformed ruling refuses with the
// exact key name: a deployment configuration error is never guessed around.
func recoveryEffectClassRuling(cfg *config.Config) (recovery.EffectClassRuling, error) {
	if cfg == nil {
		return nil, nil
	}
	ruling, err := recovery.ParseEffectClassRuling(cfg.Recovery.EffectClassRulingJSON)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", recovery.EffectClassRulingConfigKey, err)
	}
	return ruling, nil
}
