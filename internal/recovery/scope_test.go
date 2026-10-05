// scope_test.go pins the T050 scope wiring as a unit: the canonical capability
// scope constructor, the capability matching rule of the decision writers and
// the gate, the explicit dependency mapping, the legacy-opaque refusal and the
// conservative effect-class classification. No database is needed; the store
// bound paths are exercised by the integration layers.
package recovery

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

// TestEffectClassRulingConfigKeyMatchesConfig pins the ruling key constant to
// the config package (the same pin GateTTLConfigKey carries): the deployment
// ruling is read from configuration and never from a caller flag.
func TestEffectClassRulingConfigKeyMatchesConfig(t *testing.T) {
	if EffectClassRulingConfigKey != config.EnvRecoveryEffectClassRuling {
		t.Fatalf("EffectClassRulingConfigKey = %q, want %q", EffectClassRulingConfigKey, config.EnvRecoveryEffectClassRuling)
	}
}

// TestGateAdmitRequiresCanonicalCapabilityScope pins the matching rule at the
// gate entry without a control store: a legacy opaque scope, a scope naming a
// different capability and a missing capability scope all refuse with the
// closed class scope_mismatch (fail-closed, never translated).
func TestGateAdmitRequiresCanonicalCapabilityScope(t *testing.T) {
	gate := &Gate{ttl: time.Minute, now: time.Now, cache: make(map[gateCacheKey]gateCapabilityFacts)}
	for _, tc := range []struct {
		name       string
		capability Capability
		scope      string
	}{
		{"legacy opaque", CapabilityQuery, "chain=31337;surface=serve"},
		{"cross capability", CapabilityQuery, "chain=31337;capability=chain_scan"},
		{"missing capability", CapabilityQuery, "chain=31337;asset=usdc;kind=deposit"},
		{"empty", CapabilityQuery, ""},
	} {
		decision, err := gate.Admit(context.Background(), GateRequest{Capability: tc.capability, ScopeHash: tc.scope})
		if err != nil {
			t.Fatalf("%s: Admit error = %v, want a decision", tc.name, err)
		}
		if decision.Allowed || decision.Normal || decision.RefusalClass != RefusalScopeMismatch {
			t.Fatalf("%s: decision = %+v, want a scope_mismatch refusal", tc.name, decision)
		}
	}
	// A malformed effect-class ruling refuses gate construction outright (the
	// zero-value store handle is never dereferenced on this path).
	if _, err := NewGate(&controlstore.Store{}, GateOptions{TTL: time.Minute,
		EffectClassRuling: EffectClassRuling{"topic": "not-an-impact"}}); !errors.Is(err, ErrEffectClassRuling) {
		t.Fatalf("malformed ruling gate construction error = %v, want ErrEffectClassRuling", err)
	}
}

func TestCapabilityScopeCanonicalAndFailClosed(t *testing.T) {
	scope, err := CapabilityScope(31337, CapabilityQuery)
	if err != nil {
		t.Fatalf("CapabilityScope: %v", err)
	}
	if want := "capability=query;chain=31337"; scope != want {
		t.Fatalf("CapabilityScope(31337, query) = %q, want %q (canonical key order)", scope, want)
	}
	// Two equivalent constructions converge on the same canonical scope.
	direct, err := Scope{ChainID: 0x7a69, Capability: "QUERY"}.Canonical()
	if err != nil {
		t.Fatalf("Scope.Canonical: %v", err)
	}
	if direct != scope {
		t.Fatalf("equivocal constructions diverged: %q vs %q", direct, scope)
	}
	if _, err := CapabilityScope(0, CapabilityQuery); !errors.Is(err, ErrScopeMismatch) {
		t.Fatalf("chain 0 error = %v, want scope_mismatch", err)
	}
	if _, err := CapabilityScope(31337, Capability("order_book")); !errors.Is(err, ErrScopeMismatch) {
		t.Fatalf("unknown capability error = %v, want scope_mismatch", err)
	}
}

func TestParseCapabilityScopeEquivalenceAndRefusals(t *testing.T) {
	// Equivalent representations of the same logical scope canonicalize onto
	// one scope, so the stream key is stable.
	equivalent := []string{
		"chain=31337;capability=query",
		"Chain=031337;CAPABILITY=QUERY",
		"  capability = query ; chain = 0x7a69 ",
	}
	var canonical string
	for _, raw := range equivalent {
		scope, err := ParseCapabilityScope(raw, CapabilityQuery)
		if err != nil {
			t.Fatalf("ParseCapabilityScope(%q): %v", raw, err)
		}
		got, err := scope.Canonical()
		if err != nil {
			t.Fatalf("Canonical(%q): %v", raw, err)
		}
		if canonical == "" {
			canonical = got
		} else if got != canonical {
			t.Fatalf("equivalent scope %q canonicalized to %q, want %q", raw, got, canonical)
		}
	}

	// Everything else refuses as scope_mismatch and is never mapped onto a
	// default: a different capability, a legacy opaque surface scope, an
	// unknown dimension, a missing capability and an out-of-vocabulary value.
	refusals := []struct {
		raw        string
		capability Capability
	}{
		{"chain=31337;capability=chain_scan", CapabilityQuery},
		{"chain=31337;surface=serve", CapabilityQuery},
		{"chain=31337", CapabilityQuery},
		{"chain=31337;capability=query;surface=serve", CapabilityQuery},
		{"chain=0;capability=query", CapabilityQuery},
		{"chain=31337;capability=", CapabilityQuery},
		{"", CapabilityQuery},
	}
	for _, refusal := range refusals {
		if _, err := ParseCapabilityScope(refusal.raw, refusal.capability); !errors.Is(err, ErrScopeMismatch) {
			t.Fatalf("ParseCapabilityScope(%q, %s) error = %v, want scope_mismatch", refusal.raw, refusal.capability, err)
		}
	}
	for _, raw := range []string{"chain=31337;capability=query", "chain=31337;surface=serve"} {
		if _, err := ParseCapabilityScope(raw, Capability("order_book")); !errors.Is(err, ErrScopeMismatch) {
			t.Fatalf("unknown request capability error = %v, want scope_mismatch", err)
		}
	}
}

func TestDependencyScopeMapsExplicitly(t *testing.T) {
	requested, err := ParseScope("chain=31337;asset=usdc;kind=withdrawal;capability=new_withdrawal_creation")
	if err != nil {
		t.Fatalf("ParseScope: %v", err)
	}
	mapped, err := DependencyScope(requested, CapabilityExistingWithdrawalRecovery).Canonical()
	if err != nil {
		t.Fatalf("DependencyScope.Canonical: %v", err)
	}
	want := "asset=usdc;capability=existing_withdrawal_recovery;chain=31337;kind=withdrawal"
	if mapped != want {
		t.Fatalf("mapped dependency scope = %q, want %q", mapped, want)
	}
	// The dependent capability's own scope string is never the dependency's.
	own, err := requested.Canonical()
	if err != nil {
		t.Fatalf("Canonical: %v", err)
	}
	if own == mapped {
		t.Fatalf("dependency scope %q must not reuse the dependent scope", mapped)
	}
	// The optional dimensions are carried over, never merged away.
	noKind, err := ParseScope("chain=1;capability=chain_scan")
	if err != nil {
		t.Fatalf("ParseScope: %v", err)
	}
	mapped, err = DependencyScope(noKind, CapabilityChainScan).Canonical()
	if err != nil {
		t.Fatalf("DependencyScope.Canonical: %v", err)
	}
	if mapped != "capability=chain_scan;chain=1" {
		t.Fatalf("mapped dependency scope = %q", mapped)
	}
}

func TestCheckScopeMatchCanonicalEquality(t *testing.T) {
	if err := CheckScopeMatch("chain=31337;capability=query", "capability=query;chain=0x7a69"); err != nil {
		t.Fatalf("equivalent scopes must match: %v", err)
	}
	if err := CheckScopeMatch("chain=31337;capability=query", "chain=1;capability=query"); !errors.Is(err, ErrScopeMismatch) {
		t.Fatalf("different chains must not match: %v", err)
	}
	if err := CheckScopeMatch("chain=31337;capability=query", "chain=31337;surface=serve"); !errors.Is(err, ErrScopeMismatch) {
		t.Fatalf("legacy opaque scopes must not match: %v", err)
	}
}

func TestRequiredApprovalClassForScopeConservativeDefaults(t *testing.T) {
	query, err := ParseCapabilityScope("chain=1;capability=query", CapabilityQuery)
	if err != nil {
		t.Fatalf("ParseCapabilityScope: %v", err)
	}
	if class, err := RequiredApprovalClassForScope(query, ScopeEffectClass(query), nil); err != nil || class != ApprovalClassSingleNonExecutor {
		t.Fatalf("query class = %q (%v), want single", class, err)
	}

	// Inherently high-impact capabilities stay dual without a ruling.
	for _, capability := range []Capability{CapabilityNewWithdrawalCreation, CapabilityExistingWithdrawalRecovery} {
		scope, err := ParseCapabilityScope("chain=1;capability="+string(capability), capability)
		if err != nil {
			t.Fatalf("ParseCapabilityScope(%s): %v", capability, err)
		}
		if class, err := RequiredApprovalClassForScope(scope, ScopeEffectClass(scope), nil); err != nil || class != ApprovalClassDualNonExecutor {
			t.Fatalf("%s class = %q (%v), want dual", capability, class, err)
		}
	}

	// The event capabilities are dual while unconfigured and stay dual for an
	// unknown or unlabelled effect class; only a positive deployment ruling on
	// the scope's business type may narrow them.
	eventScope, err := ParseCapabilityScope("chain=1;capability=event_consuming;kind=broker-topic-a", CapabilityEventConsuming)
	if err != nil {
		t.Fatalf("ParseCapabilityScope: %v", err)
	}
	if class, err := RequiredApprovalClassForScope(eventScope, ScopeEffectClass(eventScope), nil); err != nil || class != ApprovalClassDualNonExecutor {
		t.Fatalf("unconfigured event class = %q (%v), want dual", class, err)
	}
	ruling := EffectClassRuling{
		"broker-topic-a": EffectImpactNoRealDownstream,
		"broker-topic-b": EffectImpactRealDownstream,
	}
	if class, err := RequiredApprovalClassForScope(eventScope, ScopeEffectClass(eventScope), ruling); err != nil || class != ApprovalClassSingleNonExecutor {
		t.Fatalf("positively ruled event class = %q (%v), want single", class, err)
	}
	other, err := ParseCapabilityScope("chain=1;capability=event_consuming;kind=broker-topic-c", CapabilityEventConsuming)
	if err != nil {
		t.Fatalf("ParseCapabilityScope: %v", err)
	}
	if class, err := RequiredApprovalClassForScope(other, ScopeEffectClass(other), ruling); err != nil || class != ApprovalClassDualNonExecutor {
		t.Fatalf("unlabelled event class = %q (%v), want dual", class, err)
	}
	withoutKind, err := ParseCapabilityScope("chain=1;capability=event_consuming", CapabilityEventConsuming)
	if err != nil {
		t.Fatalf("ParseCapabilityScope: %v", err)
	}
	if class, err := RequiredApprovalClassForScope(withoutKind, ScopeEffectClass(withoutKind), ruling); err != nil || class != ApprovalClassDualNonExecutor {
		t.Fatalf("kind-less event class = %q (%v), want dual (an omitted dimension is not a wildcard)", class, err)
	}
	// The ruling never narrows the inherent classes and never relaxes a
	// non-event capability below its compiled-in class.
	if class, err := RequiredApprovalClassForScope(query, ScopeEffectClass(query), EffectClassRuling{"broker-topic-a": EffectImpactNoRealDownstream}); err != nil || class != ApprovalClassSingleNonExecutor {
		t.Fatalf("query class under ruling = %q (%v), want single", class, err)
	}
}

func TestParseEffectClassRulingCarriage(t *testing.T) {
	if ruling, err := ParseEffectClassRuling(""); err != nil || ruling != nil {
		t.Fatalf("empty ruling = %v (%v), want nil/not-configured", ruling, err)
	}
	ruling, err := ParseEffectClassRuling(`{"broker-topic-a":"no_real_downstream_effect","broker-topic-b":"real_downstream"}`)
	if err != nil {
		t.Fatalf("ParseEffectClassRuling: %v", err)
	}
	if len(ruling) != 2 || ruling["broker-topic-a"] != EffectImpactNoRealDownstream {
		t.Fatalf("parsed ruling = %+v", ruling)
	}
	for _, raw := range []string{
		`{"broker-topic-a":"single"}`,
		`{"":"real_downstream"}`,
		`["broker-topic-a"]`,
		`{"broker-topic-a":1}`,
		`not-json`,
	} {
		if _, err := ParseEffectClassRuling(raw); !errors.Is(err, ErrEffectClassRuling) {
			t.Fatalf("ParseEffectClassRuling(%q) error = %v, want ErrEffectClassRuling", raw, err)
		}
	}
	if !strings.Contains(ErrEffectClassRuling.Error(), "effect-class") {
		t.Fatalf("ErrEffectClassRuling text = %q", ErrEffectClassRuling.Error())
	}
}
