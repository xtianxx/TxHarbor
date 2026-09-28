// gate_unit_test.go covers T012's pure logic (no database): the closed refusal
// set, the conservative approval-class map, the TTL configuration guard, and
// the generation-aware cache bookkeeping. Every admission against a real
// control store lives in gate_integration_test.go.
package recovery

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

// TestGateTTLConfigKeyMatchesConfig pins GateTTLConfigKey to the config
// package's key so the "refuse by name" path can never drift from the
// deployment configuration.
func TestGateTTLConfigKeyMatchesConfig(t *testing.T) {
	if GateTTLConfigKey != config.EnvRecoveryGateTTL {
		t.Fatalf("GateTTLConfigKey = %q, want %q", GateTTLConfigKey, config.EnvRecoveryGateTTL)
	}
}

// TestRefusalClassClosedSet pins the 15-class closed set of data-model §3.3 /
// recovery_audit.refusal_class (schema order) and the Known predicate.
func TestRefusalClassClosedSet(t *testing.T) {
	want := []RefusalClass{
		"no_instance",
		"instance_mismatch",
		"no_release",
		"release_invalidated_generation",
		"release_revoked",
		"capability_dependency_closed",
		"isolation_unproven",
		"gap_open",
		"approval_missing",
		"approval_identity_unverified",
		"approval_executor_excluded",
		"approval_stale",
		"hard_gate_active",
		"control_store_unavailable",
		"scope_mismatch",
	}
	got := KnownRefusalClasses()
	if !slices.Equal(got, want) {
		t.Fatalf("closed refusal set = %v, want %v", got, want)
	}
	// The caller receives a fresh slice.
	got[0] = "mutated"
	if KnownRefusalClasses()[0] != RefusalNoInstance {
		t.Fatal("KnownRefusalClasses must return a fresh slice")
	}
	for _, class := range knownRefusalClasses {
		if !class.Known() {
			t.Fatalf("%q must be known", class)
		}
	}
	for _, raw := range []string{"", "no_instance ", "No_Instance", "unknown", "released"} {
		if RefusalClass(raw).Known() {
			t.Fatalf("RefusalClass(%q) must not be known", raw)
		}
	}
	// Cross-checks against the store constants this file aliases.
	if RefusalApprovalIdentityUnverified != controlstore.RefusalApprovalIdentityUnverified {
		t.Fatal("approval_identity_unverified must alias the controlstore constant")
	}
	if RefusalControlStoreUnavailable != controlstore.RefusalControlStoreUnavailable {
		t.Fatal("control_store_unavailable must alias the controlstore constant")
	}
}

// TestRequiredApprovalClassConservativeDefault pins the conservative T012
// classification: high-impact capabilities default to dual until the T050
// effect-class ruling narrows the event capabilities by scope.
func TestRequiredApprovalClassConservativeDefault(t *testing.T) {
	dual := []Capability{
		CapabilityExistingWithdrawalRecovery,
		CapabilityNewWithdrawalCreation,
		CapabilityEventPublishing,
		CapabilityEventConsuming,
	}
	single := []Capability{
		CapabilityQuery,
		CapabilityChainScan,
		CapabilityDepositConfirmation,
	}
	for _, c := range KnownCapabilities() {
		class, err := RequiredApprovalClass(c)
		if err != nil {
			t.Fatalf("RequiredApprovalClass(%q): %v", c, err)
		}
		want := ApprovalClassSingleNonExecutor
		if slices.Contains(dual, c) {
			want = ApprovalClassDualNonExecutor
		}
		if !slices.Contains(single, c) && !slices.Contains(dual, c) {
			t.Fatalf("capability %q is not classified by the test", c)
		}
		if class != want {
			t.Fatalf("RequiredApprovalClass(%q) = %q, want %q", c, class, want)
		}
	}
	if _, err := RequiredApprovalClass("order_book"); !errors.Is(err, ErrUnknownCapability) {
		t.Fatalf("unknown capability error = %v, want ErrUnknownCapability", err)
	}
}

// TestNewGateRefusesMissingStoreAndTTL covers the "no default TTL / no
// unguarded store" construction contract, including the exact key name in the
// failure.
func TestNewGateRefusesMissingStoreAndTTL(t *testing.T) {
	if _, err := NewGate(nil, GateOptions{TTL: time.Minute}); !errors.Is(err, ErrGateControlStoreUnavailable) {
		t.Fatalf("NewGate(nil) error = %v, want ErrGateControlStoreUnavailable", err)
	}
	store := &controlstore.Store{} // never dereferenced by NewGate
	for _, ttl := range []time.Duration{0, -time.Second} {
		_, err := NewGate(store, GateOptions{TTL: ttl})
		if err == nil || !strings.Contains(err.Error(), GateTTLConfigKey) {
			t.Fatalf("NewGate(ttl=%s) error = %v, want a refusal naming %s", ttl, err, GateTTLConfigKey)
		}
	}
	if _, err := NewGate(store, GateOptions{TTL: time.Minute}); err != nil {
		t.Fatalf("NewGate with a positive TTL: %v", err)
	}
}

// TestGateRequestContractRefusals covers the request validation that runs
// before any database access: an unknown capability is a caller-contract
// error, a missing scope is a scope_mismatch refusal (never a default).
func TestGateRequestContractRefusals(t *testing.T) {
	gate := newStorelessGate(t)
	if _, err := gate.Admit(context.Background(), GateRequest{Capability: "order_book", ScopeHash: "s"}); !errors.Is(err, ErrGateRequest) {
		t.Fatalf("unknown capability error = %v, want ErrGateRequest", err)
	}
	d, err := gate.Admit(context.Background(), GateRequest{Capability: CapabilityQuery, ScopeHash: "  "})
	if err != nil {
		t.Fatalf("empty scope: %v", err)
	}
	if d.Allowed || d.RefusalClass != RefusalScopeMismatch {
		t.Fatalf("empty scope decision = %+v, want refusal %s", d, RefusalScopeMismatch)
	}
}

// newStorelessGate builds a Gate literal without a control store. Only the
// pre-database request validation and the cache bookkeeping may be exercised
// with it; Admit paths that need the store must never be reached.
func newStorelessGate(t *testing.T) *Gate {
	t.Helper()
	return &Gate{
		ttl:   time.Minute,
		now:   time.Now,
		cache: make(map[gateCacheKey]gateCapabilityFacts),
	}
}

// TestGateCacheGenerationAwareReuse covers the cache contract: an entry is
// reusable only while the in-lock token matches its generation and hash and
// the TTL is unexpired; generation/hash changes and expiry drop it
// immediately (F5), and the persistent map stays bounded.
func TestGateCacheGenerationAwareReuse(t *testing.T) {
	now := time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)
	gate := &Gate{
		ttl:   time.Minute,
		now:   func() time.Time { return now },
		cache: make(map[gateCacheKey]gateCapabilityFacts),
	}
	key := gateCacheKey{instanceID: "11111111-1111-1111-1111-111111111111", capability: CapabilityQuery}
	token := controlstore.InstanceToken{
		InstanceID:         key.instanceID,
		EvidenceGeneration: 3,
		EvidenceHash:       "sha256:aa",
	}
	facts := gateCapabilityFacts{
		generation: 3,
		hash:       "sha256:aa",
		expiresAt:  now.Add(time.Minute),
		isolation: []gateIsolationRow{
			{item: IsolationItemOldWritersStopped, state: "verified", verifiedBy: "auth:verifier"},
		},
	}
	gate.cachePut(key, facts)
	if _, ok := gate.cacheGet(key, token); !ok {
		t.Fatal("entry at the matching token and unexpired TTL must be reusable")
	}

	// A generation change is an immediate miss (and the entry is dropped).
	other := token
	other.EvidenceGeneration = 4
	if _, ok := gate.cacheGet(key, other); ok {
		t.Fatal("entry must not be reusable across generations")
	}
	if gate.cacheEntries() != 0 {
		t.Fatalf("generation change must drop the entry immediately, %d left", gate.cacheEntries())
	}

	// A hash change with the same generation is also an immediate miss.
	gate.cachePut(key, facts)
	other = token
	other.EvidenceHash = "sha256:bb"
	if _, ok := gate.cacheGet(key, other); ok {
		t.Fatal("entry must not be reusable across evidence hashes")
	}

	// Expiry: same token, clock advanced beyond the TTL.
	gate.cachePut(key, facts)
	now = now.Add(2 * time.Minute)
	if _, ok := gate.cacheGet(key, token); ok {
		t.Fatal("expired entry must not be reusable")
	}

	// invalidateCache drops the instance's stale entries and expired entries
	// of other instances.
	gate.cachePut(key, gateCapabilityFacts{generation: 3, hash: "sha256:aa", expiresAt: now.Add(time.Minute)})
	otherKey := gateCacheKey{instanceID: "22222222-2222-2222-2222-222222222222", capability: CapabilityQuery}
	gate.cachePut(otherKey, gateCapabilityFacts{generation: 1, hash: "sha256:cc", expiresAt: now.Add(-time.Minute)})
	gate.invalidateCache(token)
	if got := gate.cacheEntries(); got != 1 {
		t.Fatalf("after invalidation %d entries remain, want 1 (the fresh other-instance entry)", got)
	}

	// The cache stays bounded: overfilling stops new caching but never fails.
	for i := 0; i < maxGateCacheEntries+16; i++ {
		instance := "33333333-3333-3333-3333-" + pad12(i)
		gate.cachePut(gateCacheKey{instanceID: instance, capability: CapabilityQuery},
			gateCapabilityFacts{generation: int64(i), hash: "sha256:dd", expiresAt: now.Add(time.Hour)})
	}
	if got := gate.cacheEntries(); got > maxGateCacheEntries {
		t.Fatalf("cache grew to %d entries, bound is %d", got, maxGateCacheEntries)
	}
}

func pad12(n int) string {
	const digits = "0123456789"
	out := make([]byte, 12)
	for i := 11; i >= 0; i-- {
		out[i] = digits[n%10]
		n /= 10
	}
	return string(out)
}
