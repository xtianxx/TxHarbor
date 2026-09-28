// capabilities_test.go covers T011: the closed seven-capability set, the
// frozen requires_capabilities / isolation_dependency_set matrices of
// data-model.md §3.2, the dependency-closure helper T012 evaluates with, and
// the matrix self-check. Pure logic, no database.
package recovery

import (
	"errors"
	"slices"
	"strings"
	"testing"
)

// mutateMatrices snapshots both package matrices, applies fn, and returns a
// restore function. It lets matrix-integrity failure paths be exercised
// without leaking mutations into other tests.
func mutateMatrices(fn func()) func() {
	origRequires := make(map[Capability][]Capability, len(requiresCapabilities))
	for k, v := range requiresCapabilities {
		origRequires[k] = slices.Clone(v)
	}
	origIsolation := make(map[Capability][]IsolationItemKey, len(isolationDependencySets))
	for k, v := range isolationDependencySets {
		origIsolation[k] = slices.Clone(v)
	}
	fn()
	return func() {
		requiresCapabilities = origRequires
		isolationDependencySets = origIsolation
	}
}

func TestKnownCapabilitiesClosedSet(t *testing.T) {
	want := []Capability{
		CapabilityQuery,
		CapabilityChainScan,
		CapabilityDepositConfirmation,
		CapabilityExistingWithdrawalRecovery,
		CapabilityNewWithdrawalCreation,
		CapabilityEventPublishing,
		CapabilityEventConsuming,
	}
	got := KnownCapabilities()
	if !slices.Equal(got, want) {
		t.Fatalf("closed capability set = %v, want %v", got, want)
	}

	// Frozen string values: they are persisted in control-store rows and audit
	// entries, so they must match data-model §3 exactly.
	wantStrings := []string{
		"query",
		"chain_scan",
		"deposit_confirmation",
		"existing_withdrawal_recovery",
		"new_withdrawal_creation",
		"event_publishing",
		"event_consuming",
	}
	for i, c := range got {
		if string(c) != wantStrings[i] {
			t.Fatalf("capability[%d] = %q, want %q", i, c, wantStrings[i])
		}
	}

	// The caller receives a fresh slice.
	got[0] = "mutated"
	if KnownCapabilities()[0] != CapabilityQuery {
		t.Fatal("KnownCapabilities must return a fresh slice")
	}
}

func TestCapabilityKnownAndParse(t *testing.T) {
	for _, c := range KnownCapabilities() {
		if !c.Known() {
			t.Fatalf("%q must be known", c)
		}
		parsed, err := ParseCapability(string(c))
		if err != nil {
			t.Fatalf("ParseCapability(%q): %v", c, err)
		}
		if parsed != c {
			t.Fatalf("ParseCapability(%q) = %q, want %q", c, parsed, c)
		}
	}

	for _, s := range []string{
		"",
		"Query",
		"QUERY",
		"query ",
		" chain_scan",
		"chain-scan",
		"deposit",
		"all",
		"unknown",
	} {
		if Capability(s).Known() {
			t.Fatalf("Capability(%q) must not be known", s)
		}
		if _, err := ParseCapability(s); !errors.Is(err, ErrUnknownCapability) {
			t.Fatalf("ParseCapability(%q) error = %v, want ErrUnknownCapability", s, err)
		}
	}
}

func TestKnownIsolationItemKeysAndParse(t *testing.T) {
	want := []IsolationItemKey{
		IsolationItemOldWritersStopped,
		IsolationItemWriterFencingObserved,
		IsolationItemNetworkIsolation,
		IsolationItemVersionCompatible,
		IsolationItemNoPreReleaseEffects,
		IsolationItemAuthorizationRecheck,
	}
	got := KnownIsolationItemKeys()
	if !slices.Equal(got, want) {
		t.Fatalf("closed isolation item set = %v, want %v", got, want)
	}
	wantStrings := []string{
		"old_writers_stopped",
		"writer_fencing_observed",
		"network_isolation",
		"version_compatible",
		"no_pre_release_effects",
		"authorization_recheck",
	}
	for i, k := range got {
		if string(k) != wantStrings[i] {
			t.Fatalf("item[%d] = %q, want %q", i, k, wantStrings[i])
		}
		parsed, err := ParseIsolationItemKey(string(k))
		if err != nil || parsed != k {
			t.Fatalf("ParseIsolationItemKey(%q) = %q, %v; want %q, nil", k, parsed, err, k)
		}
	}

	got[0] = "mutated"
	if KnownIsolationItemKeys()[0] != IsolationItemOldWritersStopped {
		t.Fatal("KnownIsolationItemKeys must return a fresh slice")
	}

	for _, s := range []string{"", "old-writers-stopped", "writer_fencing", "unknown"} {
		if IsolationItemKey(s).Known() {
			t.Fatalf("IsolationItemKey(%q) must not be known", s)
		}
		if _, err := ParseIsolationItemKey(s); !errors.Is(err, ErrUnknownIsolationItem) {
			t.Fatalf("ParseIsolationItemKey(%q) error = %v, want ErrUnknownIsolationItem", s, err)
		}
	}
}

func TestRequiresCapabilitiesMatrix(t *testing.T) {
	cases := []struct {
		c    Capability
		want []Capability
	}{
		{CapabilityQuery, nil},
		{CapabilityChainScan, nil},
		{CapabilityDepositConfirmation, []Capability{CapabilityChainScan}},
		{CapabilityExistingWithdrawalRecovery, []Capability{CapabilityChainScan}},
		{CapabilityNewWithdrawalCreation, []Capability{CapabilityExistingWithdrawalRecovery}},
		{CapabilityEventPublishing, []Capability{CapabilityChainScan}},
		{CapabilityEventConsuming, nil},
	}
	for _, tc := range cases {
		got, err := RequiresCapabilities(tc.c)
		if err != nil {
			t.Fatalf("RequiresCapabilities(%q): %v", tc.c, err)
		}
		if !slices.Equal(got, tc.want) {
			t.Fatalf("RequiresCapabilities(%q) = %v, want %v", tc.c, got, tc.want)
		}
	}

	if _, err := RequiresCapabilities("bogus"); !errors.Is(err, ErrUnknownCapability) {
		t.Fatalf("RequiresCapabilities(unknown) error = %v, want ErrUnknownCapability", err)
	}

	// The caller receives a fresh slice; mutating it must not corrupt the
	// frozen matrix.
	deps, err := RequiresCapabilities(CapabilityDepositConfirmation)
	if err != nil {
		t.Fatal(err)
	}
	deps[0] = CapabilityQuery
	again, err := RequiresCapabilities(CapabilityDepositConfirmation)
	if err != nil {
		t.Fatal(err)
	}
	if again[0] != CapabilityChainScan {
		t.Fatal("RequiresCapabilities must return a fresh slice")
	}
}

func TestIsolationDependencySetMatrix(t *testing.T) {
	chainScanSet := []IsolationItemKey{
		IsolationItemOldWritersStopped,
		IsolationItemWriterFencingObserved,
		IsolationItemVersionCompatible,
	}
	withdrawalSet := []IsolationItemKey{
		IsolationItemOldWritersStopped,
		IsolationItemWriterFencingObserved,
		IsolationItemNetworkIsolation,
		IsolationItemAuthorizationRecheck,
	}
	cases := []struct {
		c    Capability
		want []IsolationItemKey
	}{
		{CapabilityQuery, []IsolationItemKey{
			IsolationItemOldWritersStopped,
			IsolationItemNetworkIsolation,
			IsolationItemVersionCompatible,
		}},
		{CapabilityChainScan, chainScanSet},
		// deposit_confirmation: same as chain_scan.
		{CapabilityDepositConfirmation, chainScanSet},
		{CapabilityExistingWithdrawalRecovery, withdrawalSet},
		// new_withdrawal_creation: same as existing_withdrawal_recovery.
		{CapabilityNewWithdrawalCreation, withdrawalSet},
		{CapabilityEventPublishing, []IsolationItemKey{
			IsolationItemOldWritersStopped,
			IsolationItemWriterFencingObserved,
			IsolationItemNetworkIsolation,
			IsolationItemNoPreReleaseEffects,
		}},
		{CapabilityEventConsuming, []IsolationItemKey{
			IsolationItemOldWritersStopped,
			IsolationItemNoPreReleaseEffects,
		}},
	}

	covered := make(map[IsolationItemKey]bool)
	for _, tc := range cases {
		got, err := IsolationDependencySet(tc.c)
		if err != nil {
			t.Fatalf("IsolationDependencySet(%q): %v", tc.c, err)
		}
		if !slices.Equal(got, tc.want) {
			t.Fatalf("IsolationDependencySet(%q) = %v, want %v", tc.c, got, tc.want)
		}
		if len(got) == 0 {
			t.Fatalf("IsolationDependencySet(%q) must not be empty", tc.c)
		}
		for _, item := range got {
			if !item.Known() {
				t.Fatalf("IsolationDependencySet(%q) names unknown item %q", tc.c, item)
			}
			covered[item] = true
		}
	}

	// Every closed item key is required by at least one capability; a key that
	// nothing depends on would be dead vocabulary.
	for _, item := range KnownIsolationItemKeys() {
		if !covered[item] {
			t.Fatalf("isolation item %q is not required by any capability", item)
		}
	}

	if _, err := IsolationDependencySet("bogus"); !errors.Is(err, ErrUnknownCapability) {
		t.Fatalf("IsolationDependencySet(unknown) error = %v, want ErrUnknownCapability", err)
	}

	items, err := IsolationDependencySet(CapabilityEventConsuming)
	if err != nil {
		t.Fatal(err)
	}
	items[0] = IsolationItemNetworkIsolation
	again, err := IsolationDependencySet(CapabilityEventConsuming)
	if err != nil {
		t.Fatal(err)
	}
	if again[0] != IsolationItemOldWritersStopped {
		t.Fatal("IsolationDependencySet must return a fresh slice")
	}
}

func TestDependencyClosure(t *testing.T) {
	cases := []struct {
		c    Capability
		want []Capability
	}{
		{CapabilityQuery, nil},
		{CapabilityChainScan, nil},
		{CapabilityDepositConfirmation, []Capability{CapabilityChainScan}},
		{CapabilityExistingWithdrawalRecovery, []Capability{CapabilityChainScan}},
		// Dependency-first order: chain_scan is evaluated before
		// existing_withdrawal_recovery, which is evaluated before
		// new_withdrawal_creation.
		{CapabilityNewWithdrawalCreation, []Capability{
			CapabilityChainScan,
			CapabilityExistingWithdrawalRecovery,
		}},
		{CapabilityEventPublishing, []Capability{CapabilityChainScan}},
		{CapabilityEventConsuming, nil},
	}
	for _, tc := range cases {
		got, err := DependencyClosure(tc.c)
		if err != nil {
			t.Fatalf("DependencyClosure(%q): %v", tc.c, err)
		}
		if !slices.Equal(got, tc.want) {
			t.Fatalf("DependencyClosure(%q) = %v, want %v", tc.c, got, tc.want)
		}
	}

	// Every returned entry is known, is not the capability itself, and appears
	// exactly once; every direct requirement is included.
	for _, c := range KnownCapabilities() {
		closure, err := DependencyClosure(c)
		if err != nil {
			t.Fatalf("DependencyClosure(%q): %v", c, err)
		}
		seen := make(map[Capability]bool)
		for _, dep := range closure {
			if !dep.Known() {
				t.Fatalf("DependencyClosure(%q) contains unknown %q", c, dep)
			}
			if dep == c {
				t.Fatalf("DependencyClosure(%q) must exclude itself", c)
			}
			if seen[dep] {
				t.Fatalf("DependencyClosure(%q) contains %q twice", c, dep)
			}
			seen[dep] = true
		}
		direct, err := RequiresCapabilities(c)
		if err != nil {
			t.Fatal(err)
		}
		for _, dep := range direct {
			if !seen[dep] {
				t.Fatalf("DependencyClosure(%q) must include direct dependency %q", c, dep)
			}
		}

		// Cross-check against an independent breadth-first transitive set.
		want := make(map[Capability]bool)
		queue := slices.Clone(direct)
		for len(queue) > 0 {
			dep := queue[0]
			queue = queue[1:]
			if want[dep] {
				continue
			}
			want[dep] = true
			next, err := RequiresCapabilities(dep)
			if err != nil {
				t.Fatal(err)
			}
			queue = append(queue, next...)
		}
		if len(want) != len(seen) {
			t.Fatalf("DependencyClosure(%q) = %v, transitive set = %v", c, closure, want)
		}
		for dep := range want {
			if !seen[dep] {
				t.Fatalf("DependencyClosure(%q) misses transitive dependency %q", c, dep)
			}
		}

		// Dependency-first order: for every capability in the closure, all of
		// its own dependencies in the closure must already have appeared.
		position := make(map[Capability]int, len(closure))
		for i, dep := range closure {
			position[dep] = i
		}
		for i, dep := range closure {
			for _, next := range mustRequires(t, dep) {
				if pos, ok := position[next]; ok && pos >= i {
					t.Fatalf("DependencyClosure(%q): dependency %q at %d after dependent %q at %d",
						c, next, pos, dep, i)
				}
			}
		}
	}

	if _, err := DependencyClosure("bogus"); !errors.Is(err, ErrUnknownCapability) {
		t.Fatalf("DependencyClosure(unknown) error = %v, want ErrUnknownCapability", err)
	}
}

func TestDependencyClosureFor(t *testing.T) {
	got, err := DependencyClosureFor(CapabilityNewWithdrawalCreation, CapabilityEventPublishing)
	if err != nil {
		t.Fatalf("DependencyClosureFor: %v", err)
	}
	want := []Capability{CapabilityChainScan, CapabilityExistingWithdrawalRecovery}
	if !slices.Equal(got, want) {
		t.Fatalf("DependencyClosureFor(new_withdrawal_creation, event_publishing) = %v, want %v", got, want)
	}

	// The union deduplicates chain_scan even when it is both a root and a
	// dependency of another root.
	got, err = DependencyClosureFor(CapabilityChainScan, CapabilityNewWithdrawalCreation)
	if err != nil {
		t.Fatalf("DependencyClosureFor: %v", err)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("DependencyClosureFor(chain_scan, new_withdrawal_creation) = %v, want %v", got, want)
	}

	got, err = DependencyClosureFor()
	if err != nil || got != nil {
		t.Fatalf("DependencyClosureFor() = %v, %v; want nil, nil", got, err)
	}

	if _, err := DependencyClosureFor(CapabilityQuery, "bogus"); !errors.Is(err, ErrUnknownCapability) {
		t.Fatalf("DependencyClosureFor(unknown) error = %v, want ErrUnknownCapability", err)
	}
}

func mustRequires(t *testing.T, c Capability) []Capability {
	t.Helper()
	deps, err := RequiresCapabilities(c)
	if err != nil {
		t.Fatal(err)
	}
	return deps
}

func TestValidateCapabilityMatrix(t *testing.T) {
	if err := ValidateCapabilityMatrix(); err != nil {
		t.Fatalf("shipped capability matrix must validate: %v", err)
	}

	fail := func(name string, mutate func()) {
		t.Run(name, func(t *testing.T) {
			restore := mutateMatrices(mutate)
			defer restore()
			err := ValidateCapabilityMatrix()
			if err == nil {
				t.Fatal("expected validation failure")
			}
		})
	}

	fail("missing requires entry", func() {
		delete(requiresCapabilities, CapabilityQuery)
	})
	fail("missing isolation entry", func() {
		delete(isolationDependencySets, CapabilityEventConsuming)
	})
	fail("empty isolation set", func() {
		isolationDependencySets[CapabilityEventConsuming] = nil
	})
	fail("unknown dependency", func() {
		requiresCapabilities[CapabilityChainScan] = []Capability{"bogus"}
	})
	fail("unlisted capability in requires", func() {
		requiresCapabilities["shadow"] = nil
	})
	fail("unlisted capability in isolation", func() {
		isolationDependencySets["shadow"] = []IsolationItemKey{IsolationItemOldWritersStopped}
	})
	fail("self dependency", func() {
		requiresCapabilities[CapabilityQuery] = []Capability{CapabilityQuery}
	})
	fail("duplicate dependency", func() {
		requiresCapabilities[CapabilityQuery] = []Capability{CapabilityChainScan, CapabilityChainScan}
	})
	fail("duplicate isolation item", func() {
		isolationDependencySets[CapabilityQuery] = []IsolationItemKey{
			IsolationItemOldWritersStopped,
			IsolationItemOldWritersStopped,
		}
	})
	fail("unknown isolation item", func() {
		isolationDependencySets[CapabilityQuery] = []IsolationItemKey{"bogus"}
	})
	fail("dependency cycle", func() {
		requiresCapabilities[CapabilityChainScan] = []Capability{CapabilityDepositConfirmation}
	})
}

// TestCapabilityErrorMessagesNameTheInput keeps refusals diagnosable: a caller
// that bypasses the closed set must see which value was rejected.
func TestCapabilityErrorMessagesNameTheInput(t *testing.T) {
	if _, err := ParseCapability("bogus"); err == nil || !strings.Contains(err.Error(), `"bogus"`) {
		t.Fatalf("ParseCapability error must name the rejected input: %v", err)
	}
	if _, err := ParseIsolationItemKey("bogus"); err == nil || !strings.Contains(err.Error(), `"bogus"`) {
		t.Fatalf("ParseIsolationItemKey error must name the rejected input: %v", err)
	}
}
