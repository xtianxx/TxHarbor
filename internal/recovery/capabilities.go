// capabilities.go implements T011: the closed seven-capability set of the 015
// resumption gate (FR-021) and the two frozen dependency matrices of
// data-model.md §3.2 — requires_capabilities and isolation_dependency_set.
//
// Both matrices are code, not configuration. A dependency-graph change is a
// specification change (data-model §3.2): no configuration key widens or
// narrows either matrix, and unknown capabilities or unknown checklist item
// keys are refused instead of being mapped to a default. This file performs no
// I/O and holds no mutable state beyond the compiled-in tables.
//
// T012 (gate.go, the single derived release evaluation) consumes:
//
//   - RequiresCapabilities(C): the direct requires_capabilities entries that
//     must each be release-valid before C can be released.
//   - DependencyClosure(C): the same requirement expanded transitively, in
//     evaluation order (every dependency appears before the capabilities that
//     depend on it), so each dependency is evaluated once, before C.
//   - IsolationDependencySet(C): the checklist item keys that must all be
//     state='verified' in the control store. The set is direct, not
//     transitive: a dependency capability's isolation set is enforced when
//     that dependency is evaluated.
//
// The checklist item keys (contracts/resumption-gate.md §3) live here so the
// dependency matrix and the checklist state machine (T028, checklist.go) share
// one closed vocabulary.
package recovery

import (
	"errors"
	"fmt"
	"slices"
)

var (
	// ErrUnknownCapability marks a string outside the closed seven-capability
	// set. It is a contract error, not a gate refusal: callers wire
	// capabilities from this package's constants (T012), so an unknown value
	// means the caller bypassed the closed set. Refusing is the only safe
	// handling — never map an unknown capability onto a default.
	ErrUnknownCapability = errors.New("unknown recovery capability")

	// ErrUnknownIsolationItem marks a string outside the closed checklist
	// item_key set (contracts/resumption-gate.md §3). Same refusal discipline
	// as ErrUnknownCapability.
	ErrUnknownIsolationItem = errors.New("unknown isolation checklist item key")
)

// Capability is one member of the closed seven-capability set of the 015
// resumption gate (FR-021). Values are frozen vocabulary: they appear verbatim
// in the control store (recovery_approval/recovery_release/recovery_gap rows)
// and in every audit row, so renaming one is a specification change with a
// data migration, not a refactor.
type Capability string

// The seven capabilities (data-model.md §3, contracts/resumption-gate.md §1).
const (
	CapabilityQuery                      Capability = "query"
	CapabilityChainScan                  Capability = "chain_scan"
	CapabilityDepositConfirmation        Capability = "deposit_confirmation"
	CapabilityExistingWithdrawalRecovery Capability = "existing_withdrawal_recovery"
	CapabilityNewWithdrawalCreation      Capability = "new_withdrawal_creation"
	CapabilityEventPublishing            Capability = "event_publishing"
	CapabilityEventConsuming             Capability = "event_consuming"
)

// knownCapabilities is the canonical evaluation/display order: the order of
// data-model.md §3 and contracts/resumption-gate.md §1 (1 through 7). It is
// also the dependency-safe order in the sense that every capability appears
// after all of its dependencies, so iterating it once evaluates each
// capability after all entries in its dependency closure.
var knownCapabilities = []Capability{
	CapabilityQuery,
	CapabilityChainScan,
	CapabilityDepositConfirmation,
	CapabilityExistingWithdrawalRecovery,
	CapabilityNewWithdrawalCreation,
	CapabilityEventPublishing,
	CapabilityEventConsuming,
}

// Known reports whether c is one of the seven capabilities.
func (c Capability) Known() bool {
	return slices.Contains(knownCapabilities, c)
}

// KnownCapabilities returns the closed capability set in canonical order. The
// caller receives a fresh slice and may mutate it freely.
func KnownCapabilities() []Capability {
	return slices.Clone(knownCapabilities)
}

// ParseCapability maps s onto the closed capability set. Matching is exact:
// unknown, empty, differently-cased or whitespace-padded input is refused with
// ErrUnknownCapability. There is no normalization of a security-relevant
// vocabulary (an unparsed capability must never become a default).
func ParseCapability(s string) (Capability, error) {
	c := Capability(s)
	if !c.Known() {
		return "", fmt.Errorf("%w: %q (known: %v)", ErrUnknownCapability, s, knownCapabilities)
	}
	return c, nil
}

// IsolationItemKey is one member of the closed checklist item_key set
// (contracts/resumption-gate.md §3, the 000018 checklist pattern): the
// evidence items that must each reach state='verified' before a capability
// may be released.
type IsolationItemKey string

// The six isolation checklist item keys (contracts/resumption-gate.md §3).
const (
	IsolationItemOldWritersStopped     IsolationItemKey = "old_writers_stopped"
	IsolationItemWriterFencingObserved IsolationItemKey = "writer_fencing_observed"
	IsolationItemNetworkIsolation      IsolationItemKey = "network_isolation"
	IsolationItemVersionCompatible     IsolationItemKey = "version_compatible"
	IsolationItemNoPreReleaseEffects   IsolationItemKey = "no_pre_release_effects"
	IsolationItemAuthorizationRecheck  IsolationItemKey = "authorization_recheck"
)

// knownIsolationItems is the canonical item_key order; display-only, matched
// on demand by Known.
var knownIsolationItems = []IsolationItemKey{
	IsolationItemOldWritersStopped,
	IsolationItemWriterFencingObserved,
	IsolationItemNetworkIsolation,
	IsolationItemVersionCompatible,
	IsolationItemNoPreReleaseEffects,
	IsolationItemAuthorizationRecheck,
}

// Known reports whether k is one of the six isolation checklist item keys.
func (k IsolationItemKey) Known() bool {
	return slices.Contains(knownIsolationItems, k)
}

// KnownIsolationItemKeys returns the closed item_key set in canonical order.
// The caller receives a fresh slice and may mutate it freely.
func KnownIsolationItemKeys() []IsolationItemKey {
	return slices.Clone(knownIsolationItems)
}

// ParseIsolationItemKey maps s onto the closed checklist item_key set.
// Matching is exact; unknown input is refused with ErrUnknownIsolationItem.
func ParseIsolationItemKey(s string) (IsolationItemKey, error) {
	k := IsolationItemKey(s)
	if !k.Known() {
		return "", fmt.Errorf("%w: %q (known: %v)", ErrUnknownIsolationItem, s, knownIsolationItems)
	}
	return k, nil
}

// requiresCapabilities is the frozen requires_capabilities matrix of
// data-model.md §3.2. A capability is released only when every capability it
// requires is itself release-valid (T012 evaluates recursively/transitively).
//
//	deposit_confirmation         -> chain_scan
//	existing_withdrawal_recovery -> chain_scan
//	new_withdrawal_creation      -> existing_withdrawal_recovery
//	event_publishing             -> chain_scan
//
// The empty (nil) entries are deliberate: query, chain_scan and event_consuming
// have no capability dependencies. new_withdrawal_creation ->
// existing_withdrawal_recovery is a conservative inherent dependency
// ("do not open the entry without the recovery path prepared"); widening or
// narrowing this graph is a specification change, not a configuration change.
var requiresCapabilities = map[Capability][]Capability{
	CapabilityQuery:                      nil,
	CapabilityChainScan:                  nil,
	CapabilityDepositConfirmation:        {CapabilityChainScan},
	CapabilityExistingWithdrawalRecovery: {CapabilityChainScan},
	CapabilityNewWithdrawalCreation:      {CapabilityExistingWithdrawalRecovery},
	CapabilityEventPublishing:            {CapabilityChainScan},
	CapabilityEventConsuming:             nil,
}

// isolationDependencySets is the frozen isolation_dependency_set matrix of
// data-model.md §3.2: the checklist item keys that must all be state='verified'
// before the capability may be released. It is written down here, not read
// from configuration.
//
// Derivation discipline: §3.2 is prose, so each set is at least the closest
// item_key mapping of every component its row names; where a row names an
// execution/signing/broadcast path or a broker target, both writer fencing
// and network isolation are required. Extra items only keep a capability
// closed longer (fail-closed); no row's named component is ever dropped. A
// dependency capability's isolation set is enforced by that dependency's own
// release evaluation, so the sets below stay direct.
var isolationDependencySets = map[Capability][]IsolationItemKey{
	// Old instance stopped / network isolation / version compatibility.
	CapabilityQuery: {
		IsolationItemOldWritersStopped,
		IsolationItemNetworkIsolation,
		IsolationItemVersionCompatible,
	},
	// Old instance stopped + writer-fencing observation + version
	// compatibility (the new environment owns the lease/fencing token).
	CapabilityChainScan: {
		IsolationItemOldWritersStopped,
		IsolationItemWriterFencingObserved,
		IsolationItemVersionCompatible,
	},
	// Same as chain_scan: deposit confirmation depends on indexed chain facts.
	CapabilityDepositConfirmation: {
		IsolationItemOldWritersStopped,
		IsolationItemWriterFencingObserved,
		IsolationItemVersionCompatible,
	},
	// Old executor stopped + execution/signing/broadcast path isolation
	// (execution-claim fencing observed and the old environment unreachable
	// from PG/RPC) + authorization-surface recheck (V8).
	CapabilityExistingWithdrawalRecovery: {
		IsolationItemOldWritersStopped,
		IsolationItemWriterFencingObserved,
		IsolationItemNetworkIsolation,
		IsolationItemAuthorizationRecheck,
	},
	// Same as existing_withdrawal_recovery: opening the entry without the
	// recovery path prepared is forbidden (data-model §3.2).
	CapabilityNewWithdrawalCreation: {
		IsolationItemOldWritersStopped,
		IsolationItemWriterFencingObserved,
		IsolationItemNetworkIsolation,
		IsolationItemAuthorizationRecheck,
	},
	// Old publisher stopped + publisher-owner fencing observed + broker target
	// isolation + no pre-release outbox/obligation progress.
	CapabilityEventPublishing: {
		IsolationItemOldWritersStopped,
		IsolationItemWriterFencingObserved,
		IsolationItemNetworkIsolation,
		IsolationItemNoPreReleaseEffects,
	},
	// Old consumer stopped + no pre-release idempotency/progress advance.
	// The effect-class scope decision is enforced at release time by the scope
	// evaluation (T050), not by an isolation item.
	CapabilityEventConsuming: {
		IsolationItemOldWritersStopped,
		IsolationItemNoPreReleaseEffects,
	},
}

// RequiresCapabilities returns the direct requires_capabilities entries of c
// (data-model.md §3.2). The caller receives a fresh slice and may mutate it
// freely. An unknown capability is refused with ErrUnknownCapability.
func RequiresCapabilities(c Capability) ([]Capability, error) {
	if !c.Known() {
		return nil, fmt.Errorf("%w: %q", ErrUnknownCapability, c)
	}
	return slices.Clone(requiresCapabilities[c]), nil
}

// IsolationDependencySet returns the direct isolation checklist item keys of c
// that must all be state='verified' before c may be released (data-model.md
// §3.2). The caller receives a fresh slice and may mutate it freely. An
// unknown capability is refused with ErrUnknownCapability.
func IsolationDependencySet(c Capability) ([]IsolationItemKey, error) {
	if !c.Known() {
		return nil, fmt.Errorf("%w: %q", ErrUnknownCapability, c)
	}
	return slices.Clone(isolationDependencySets[c]), nil
}

// DependencyClosure returns every transitive requires_capabilities dependency
// of c in evaluation order: each dependency appears before every capability
// that depends on it, so a caller (T012) can evaluate each entry exactly once,
// in order, before evaluating c. c itself is not included. A capability with
// no dependencies yields a nil slice.
//
// An unknown capability is refused with ErrUnknownCapability. A dependency
// cycle (impossible in the compiled-in matrix; ValidateCapabilityMatrix
// rejects it) is refused rather than partially returned.
func DependencyClosure(c Capability) ([]Capability, error) {
	if !c.Known() {
		return nil, fmt.Errorf("%w: %q", ErrUnknownCapability, c)
	}

	const (
		unvisited = iota
		visiting
		done
	)
	state := make(map[Capability]int, len(knownCapabilities))
	var ordered []Capability

	var visit func(dep Capability) error
	visit = func(dep Capability) error {
		switch state[dep] {
		case visiting:
			return fmt.Errorf("recovery capability dependency cycle through %q", dep)
		case done:
			return nil
		}
		state[dep] = visiting
		for _, next := range requiresCapabilities[dep] {
			if !next.Known() {
				return fmt.Errorf("%w: %q (required by %q)", ErrUnknownCapability, next, dep)
			}
			if err := visit(next); err != nil {
				return err
			}
		}
		state[dep] = done
		ordered = append(ordered, dep)
		return nil
	}

	for _, dep := range requiresCapabilities[c] {
		if !dep.Known() {
			return nil, fmt.Errorf("%w: %q (required by %q)", ErrUnknownCapability, dep, c)
		}
		if err := visit(dep); err != nil {
			return nil, err
		}
	}
	return ordered, nil
}

// DependencyClosureFor returns the union of DependencyClosure over caps, in
// evaluation order and without duplicates (a capability required by several
// roots appears once, after all of its own dependencies). An unknown
// capability anywhere in caps is refused with ErrUnknownCapability and no
// partial result.
func DependencyClosureFor(caps ...Capability) ([]Capability, error) {
	seen := make(map[Capability]bool, len(knownCapabilities))
	var ordered []Capability
	for _, c := range caps {
		closure, err := DependencyClosure(c)
		if err != nil {
			return nil, err
		}
		for _, dep := range closure {
			if !seen[dep] {
				seen[dep] = true
				ordered = append(ordered, dep)
			}
		}
	}
	return ordered, nil
}

// ValidateCapabilityMatrix checks the compiled-in matrices for internal
// consistency: every known capability has a requires_capabilities entry and a
// non-empty isolation_dependency_set entry; every referenced capability and
// item key is known; there are no self-dependencies, duplicates or cycles; and
// no unlisted capability leaked into either matrix. It returns nil for the
// shipped matrix and is intended for tests and future spec-change reviews.
func ValidateCapabilityMatrix() error {
	for c := range requiresCapabilities {
		if !c.Known() {
			return fmt.Errorf("requires_capabilities has an unlisted capability %q", c)
		}
	}
	for c := range isolationDependencySets {
		if !c.Known() {
			return fmt.Errorf("isolation_dependency_set has an unlisted capability %q", c)
		}
	}

	for _, c := range knownCapabilities {
		deps, ok := requiresCapabilities[c]
		if !ok {
			return fmt.Errorf("requires_capabilities is missing capability %q", c)
		}
		seenDep := make(map[Capability]bool, len(deps))
		for _, dep := range deps {
			if !dep.Known() {
				return fmt.Errorf("requires_capabilities[%q] names unknown capability %q", c, dep)
			}
			if dep == c {
				return fmt.Errorf("requires_capabilities[%q] contains a self-dependency", c)
			}
			if seenDep[dep] {
				return fmt.Errorf("requires_capabilities[%q] contains duplicate dependency %q", c, dep)
			}
			seenDep[dep] = true
		}

		items, ok := isolationDependencySets[c]
		if !ok {
			return fmt.Errorf("isolation_dependency_set is missing capability %q", c)
		}
		if len(items) == 0 {
			return fmt.Errorf("isolation_dependency_set[%q] is empty", c)
		}
		seenItem := make(map[IsolationItemKey]bool, len(items))
		for _, item := range items {
			if !item.Known() {
				return fmt.Errorf("isolation_dependency_set[%q] names unknown isolation item %q", c, item)
			}
			if seenItem[item] {
				return fmt.Errorf("isolation_dependency_set[%q] contains duplicate item %q", c, item)
			}
			seenItem[item] = true
		}
	}

	const (
		unvisited = iota
		visiting
		done
	)
	state := make(map[Capability]int, len(knownCapabilities))
	var visit func(c Capability) error
	visit = func(c Capability) error {
		switch state[c] {
		case visiting:
			return fmt.Errorf("capability dependency cycle through %q", c)
		case done:
			return nil
		}
		state[c] = visiting
		for _, dep := range requiresCapabilities[c] {
			if err := visit(dep); err != nil {
				return err
			}
		}
		state[c] = done
		return nil
	}
	for _, c := range knownCapabilities {
		if err := visit(c); err != nil {
			return err
		}
	}
	return nil
}
