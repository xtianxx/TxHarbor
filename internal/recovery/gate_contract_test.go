//go:build contract

// gate_contract_test.go is T023 [US2]: the resumption-gate contract layer
// (tags: contract; no Docker; FR-009/FR-013/FR-021–FR-026; data-model.md §3;
// contracts/resumption-gate.md §1–§2; tasks.md T023).
//
// What this layer can and cannot execute without a database. The gate's
// authoritative admission (Admit) reads the open instance from the control
// store, so normal-mode pass-through against a live store, the bound-instance
// open-instance default deny and the fund-gate call point at the real action
// site are executed against a real PostgreSQL control store in
// gate_integration_test.go (T012) and in the US2 app-level layer (T026). This
// contract file pins the surface those layers depend on and executes the
// derivations that are pure:
//
//   - normal mode has exactly one marker (no_instance) and no other class is a
//     pass-through; the gate refuses to be constructed without an
//     authoritative (version-guarded) store, so a storeless normal-mode
//     allow cannot exist;
//   - an open recovery instance denies every one of the 7 capabilities while
//     its isolation dependency set is not verified: evaluateLocked and
//     validateCapabilityFacts run with generation-bound facts through the
//     real bounded cache, refusing isolation_unproven for the capabilities
//     without dependencies and capability_dependency_closed for every
//     capability whose dependency closure is not release-valid — including
//     new_withdrawal_creation, whose closure must pass through
//     existing_withdrawal_recovery first;
//   - only the exact state "verified" satisfies an isolation item;
//     pending/evidenced/rejected/absent or casing variants refuse (the
//     checklist write path that produces these states is T028/T029);
//   - the closed refusal set of data-model §3.3 (15 classes);
//   - the gate cannot be constructed without a positive TTL, refuses by the
//     exact key name TXHARBOR_RECOVERY_GATE_TTL, and neither the deployment
//     configuration nor the gate API carries a default TTL or an
//     enable/disable knob (no test-only "close the gate" switch, quickstart
//     §4);
//   - the fund gates remain phase two: the injected FundGateChecker is a call
//     point that can only run after phase one passed, a phase-one refusal
//     never reaches it, and no decision/option field can mark the fund gates
//     waived (FR-025/F20).
//
// Every assertion here is pure Go: no Docker, no database, no filesystem
// beyond reading the config package source for the bypass-key scan.
package recovery

import (
	"context"
	"errors"
	"os"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

const contractGateScope = "chain=31337;asset=usdc;kind=withdrawal"

// contractBypassName matches every field/method/key name that would look like
// a way to close, skip or force the gate. The 015 gate has no such switch:
// "no disable switch / environment bypass" is part of the T023 acceptance
// text and the quickstart §4 anti-cheat discipline.
var contractBypassName = regexp.MustCompile(`(?i)(disable|bypass|skip|waive|override|force|ignore|unsafe|emergency|emergency_allow)`)

// TestGateContractNormalModeMarker pins the normal-mode contract: no_instance
// is the one and only pass-through marker, every other class in the closed set
// is a refusal, and a gate cannot be built without an authoritative control
// store (there is no storeless normal-mode allow path).
func TestGateContractNormalModeMarker(t *testing.T) {
	classes := KnownRefusalClasses()
	if len(classes) != 15 {
		t.Fatalf("closed refusal set has %d classes, want 15 (data-model §3.3)", len(classes))
	}
	if classes[0] != RefusalNoInstance {
		t.Fatalf("normal-mode marker %q must open the canonical class order, got %q", RefusalNoInstance, classes[0])
	}
	for _, class := range classes {
		if !class.Known() {
			t.Fatalf("%q must be a known refusal class", class)
		}
	}
	// Every class except the normal marker denies; there is no second
	// pass-through value.
	if !RefusalNoInstance.Known() {
		t.Fatal("no_instance must be in the closed set (normal-mode marker)")
	}
	for _, raw := range []string{"", "normal", "pass", "allow", "enabled"} {
		if RefusalClass(raw).Known() {
			t.Fatalf("RefusalClass(%q) must not be known: pass-through is marked by no_instance only", raw)
		}
	}

	// The authoritative normal verdict needs the control-store read; building
	// the gate without a store refuses instead of defaulting to normal.
	if _, err := NewGate(nil, GateOptions{TTL: time.Minute}); !errors.Is(err, ErrGateControlStoreUnavailable) {
		t.Fatalf("NewGate(nil store) error = %v, want ErrGateControlStoreUnavailable", err)
	}

	// The pre-database request refusals never produce an allow either.
	gate := &Gate{ttl: time.Minute, now: time.Now, cache: make(map[gateCacheKey]gateCapabilityFacts)}
	if _, err := gate.Admit(context.Background(), GateRequest{Capability: "order_book", ScopeHash: contractGateScope}); !errors.Is(err, ErrGateRequest) {
		t.Fatalf("unknown capability error = %v, want ErrGateRequest", err)
	}
	if d, err := gate.Admit(context.Background(), GateRequest{Capability: CapabilityQuery, ScopeHash: " "}); err != nil {
		t.Fatalf("empty scope admission: %v", err)
	} else if d.Allowed || d.Normal || d.RefusalClass != RefusalScopeMismatch {
		t.Fatalf("empty scope decision = %+v, want a scope_mismatch refusal", d)
	}
}

// TestGateContractOpenInstanceDeniesAllCapabilities executes the default-deny
// derivation for an open recovery instance whose isolation dependency set is
// not verified. The facts are generation-bound entries in the real gate cache,
// so evaluateLocked takes its real path (dependencies first, then the
// capability, both refusing before any store read).
func TestGateContractOpenInstanceDeniesAllCapabilities(t *testing.T) {
	token := contractInstanceToken()
	gate := contractGateWithIsolationState(t, token, "pending")

	// Which refusal the derivation must produce: capabilities without a
	// dependency refuse on their own isolation set; capabilities with a
	// closure fail on the first dependency in evaluation order.
	wantClass := map[Capability]RefusalClass{
		CapabilityQuery:                      RefusalIsolationUnproven,
		CapabilityChainScan:                  RefusalIsolationUnproven,
		CapabilityDepositConfirmation:        RefusalCapabilityDependencyClosed,
		CapabilityExistingWithdrawalRecovery: RefusalCapabilityDependencyClosed,
		CapabilityNewWithdrawalCreation:      RefusalCapabilityDependencyClosed,
		CapabilityEventPublishing:            RefusalCapabilityDependencyClosed,
		CapabilityEventConsuming:             RefusalIsolationUnproven,
	}
	for _, capability := range KnownCapabilities() {
		req := GateRequest{InstanceID: token.InstanceID, Capability: capability, ScopeHash: contractGateScope}
		ev := contractEvaluate(t, gate, token, req)
		if ev.allowed {
			t.Fatalf("capability %s must be denied while its isolation dependency set is unverified", capability)
		}
		if ev.refusal == nil {
			t.Fatalf("capability %s produced no refusal", capability)
		}
		if !ev.refusal.class.Known() {
			t.Fatalf("capability %s refusal class %q is outside the closed set", capability, ev.refusal.class)
		}
		if ev.refusal.class != wantClass[capability] {
			t.Fatalf("capability %s refusal = %s, want %s (reason: %s)",
				capability, ev.refusal.class, wantClass[capability], ev.refusal.reason)
		}
		// The refusal names the missing evidence: the first unverified item
		// for a direct isolation refusal, the blocking dependency for a
		// closure refusal (the gate reports the first blocking item in
		// dependency-set order; the full list is a checklist status concern,
		// T028/T029).
		items, err := IsolationDependencySet(capability)
		if err != nil {
			t.Fatalf("IsolationDependencySet(%s): %v", capability, err)
		}
		switch ev.refusal.class {
		case RefusalIsolationUnproven:
			if len(items) == 0 || !strings.Contains(ev.refusal.reason, string(items[0])) {
				t.Fatalf("refusal for %s must name the missing item %s: %s", capability, items, ev.refusal.reason)
			}
		case RefusalCapabilityDependencyClosed:
			closure, err := DependencyClosure(capability)
			if err != nil {
				t.Fatalf("DependencyClosure(%s): %v", capability, err)
			}
			if len(closure) == 0 || !strings.Contains(ev.refusal.reason, string(closure[0])) {
				t.Fatalf("refusal for %s must name the blocking dependency %v: %s", capability, closure, ev.refusal.reason)
			}
		}
	}
}

// TestGateContractIsolationItemMustBeExactlyVerified pins the only state that
// can satisfy an isolation item: the exact string "verified". Everything else
// (missing row, pending, evidenced, rejected, casing or padding variants)
// refuses — "a state record is not proof" (contracts/resumption-gate.md §3).
func TestGateContractIsolationItemMustBeExactlyVerified(t *testing.T) {
	for _, state := range []string{"pending", "evidenced", "rejected", "", "VERIFIED", "Verified", "verified ", " verified", "unknown"} {
		token := contractInstanceToken()
		gate := contractGateWithIsolationState(t, token, state)
		ev := contractEvaluate(t, gate, token, GateRequest{
			InstanceID: token.InstanceID, Capability: CapabilityQuery, ScopeHash: contractGateScope,
		})
		if ev.allowed || ev.refusal == nil || ev.refusal.class != RefusalIsolationUnproven {
			t.Fatalf("isolation state %q must refuse with isolation_unproven, got allowed=%v refusal=%+v",
				state, ev.allowed, ev.refusal)
		}
	}
}

// TestGateContractCapabilityDependencyClosure pins the requires_capabilities
// matrix and the closure evaluation order, and executes the closure refusal:
// new_withdrawal_creation can never pass while a capability in its closure is
// not release-valid, and its closure must contain
// existing_withdrawal_recovery ("do not open the entry without the recovery
// path prepared", data-model §3.2).
func TestGateContractCapabilityDependencyClosure(t *testing.T) {
	wantRequires := map[Capability][]Capability{
		CapabilityQuery:                      nil,
		CapabilityChainScan:                  nil,
		CapabilityDepositConfirmation:        {CapabilityChainScan},
		CapabilityExistingWithdrawalRecovery: {CapabilityChainScan},
		CapabilityNewWithdrawalCreation:      {CapabilityExistingWithdrawalRecovery},
		CapabilityEventPublishing:            {CapabilityChainScan},
		CapabilityEventConsuming:             nil,
	}
	for _, capability := range KnownCapabilities() {
		got, err := RequiresCapabilities(capability)
		if err != nil {
			t.Fatalf("RequiresCapabilities(%s): %v", capability, err)
		}
		if !reflect.DeepEqual(got, wantRequires[capability]) {
			t.Fatalf("requires_capabilities[%s] = %v, want %v", capability, got, wantRequires[capability])
		}
	}
	if _, err := RequiresCapabilities("order_book"); !errors.Is(err, ErrUnknownCapability) {
		t.Fatalf("unknown capability error = %v, want ErrUnknownCapability", err)
	}
	if err := ValidateCapabilityMatrix(); err != nil {
		t.Fatalf("capability matrix is inconsistent: %v", err)
	}

	// Evaluation order: dependencies before the capability that requires them.
	closure, err := DependencyClosure(CapabilityNewWithdrawalCreation)
	if err != nil {
		t.Fatalf("DependencyClosure(new_withdrawal_creation): %v", err)
	}
	wantClosure := []Capability{CapabilityChainScan, CapabilityExistingWithdrawalRecovery}
	if !reflect.DeepEqual(closure, wantClosure) {
		t.Fatalf("new_withdrawal_creation closure = %v, want %v", closure, wantClosure)
	}
	if !slices.Contains(closure, CapabilityExistingWithdrawalRecovery) {
		t.Fatal("new_withdrawal_creation closure must contain existing_withdrawal_recovery")
	}

	// Executed: with every isolation set unverified, new_withdrawal_creation is
	// refused with capability_dependency_closed before any release evaluation,
	// and the reason names the first blocking dependency in evaluation order.
	token := contractInstanceToken()
	gate := contractGateWithIsolationState(t, token, "pending")
	ev := contractEvaluate(t, gate, token, GateRequest{
		InstanceID: token.InstanceID, Capability: CapabilityNewWithdrawalCreation, ScopeHash: contractGateScope,
	})
	if ev.allowed || ev.refusal == nil || ev.refusal.class != RefusalCapabilityDependencyClosed {
		t.Fatalf("new_withdrawal_creation with an unverified closure must be capability_dependency_closed, got allowed=%v refusal=%+v",
			ev.allowed, ev.refusal)
	}
	if !strings.Contains(ev.refusal.reason, string(CapabilityChainScan)) {
		t.Fatalf("closure refusal must name the first blocking dependency chain_scan: %s", ev.refusal.reason)
	}
}

// TestGateContractRefusalClosedSet re-pins the 15-class closed set of
// data-model §3.3 / recovery_audit.refusal_class in schema order. A refusal
// outside this set is a contract violation, and the two classes the control
// store also owns must keep their shared constants.
func TestGateContractRefusalClosedSet(t *testing.T) {
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
	if got := KnownRefusalClasses(); !reflect.DeepEqual(got, want) {
		t.Fatalf("closed refusal set = %v, want %v", got, want)
	}
	if RefusalApprovalIdentityUnverified != controlstore.RefusalApprovalIdentityUnverified {
		t.Fatal("approval_identity_unverified must alias the controlstore constant")
	}
	if RefusalControlStoreUnavailable != controlstore.RefusalControlStoreUnavailable {
		t.Fatal("control_store_unavailable must alias the controlstore constant")
	}
	tables := controlstore.ControlTableNames()
	if len(tables) == 0 {
		t.Fatal("the control-store table name list must not be empty")
	}
	for _, table := range tables {
		if !strings.HasPrefix(table, "recovery_") {
			t.Fatalf("control table %q is outside the recovery_ namespace", table)
		}
	}
}

// TestGateContractTTLRequiredByExactName pins the TTL contract: the gate has
// no default, no zero/negative TTL, and a missing or invalid value is refused
// by the exact key name TXHARBOR_RECOVERY_GATE_TTL.
func TestGateContractTTLRequiredByExactName(t *testing.T) {
	if GateTTLConfigKey != config.EnvRecoveryGateTTL {
		t.Fatalf("GateTTLConfigKey = %q, want %q", GateTTLConfigKey, config.EnvRecoveryGateTTL)
	}
	store := &controlstore.Store{} // never dereferenced by NewGate
	for _, ttl := range []time.Duration{0, -time.Second} {
		gate, err := NewGate(store, GateOptions{TTL: ttl})
		if gate != nil || err == nil {
			t.Fatalf("NewGate(ttl=%s) = (%v, %v), want a refusal", ttl, gate, err)
		}
		if !strings.Contains(err.Error(), config.EnvRecoveryGateTTL) {
			t.Fatalf("TTL refusal must name %s, got %v", config.EnvRecoveryGateTTL, err)
		}
	}
	// A positive TTL is accepted at construction; no database is touched.
	if gate, err := NewGate(store, GateOptions{TTL: time.Minute}); err != nil || gate == nil {
		t.Fatalf("NewGate with a positive TTL = (%v, %v), want a gate", gate, err)
	}
}

// TestGateContractRecoveryConfigHasNoDefaultsAndNoBypassKnob pins the
// configuration side of T023: the recovery knobs have no invented defaults
// (missing means "not configured", refused by name at the command path), a
// malformed TTL is refused by name, and the TXHARBOR_RECOVERY_* namespace
// carries no disable/bypass switch.
func TestGateContractRecoveryConfigHasNoDefaultsAndNoBypassKnob(t *testing.T) {
	cfg, err := config.Load(contractEnv(nil))
	if err != nil {
		t.Fatalf("Load with a valid base environment: %v", err)
	}
	if cfg.Recovery.ControlDSN != "" || cfg.Recovery.Principal != "" || cfg.Recovery.ArtifactDir != "" ||
		cfg.Recovery.GateTTL != 0 || cfg.Recovery.RPOTarget != 0 || cfg.Recovery.RTOTarget != 0 ||
		cfg.Recovery.BackupFrequency != 0 || cfg.Recovery.Retention != 0 {
		t.Fatalf("missing recovery configuration must stay unset (no defaults), got %+v", cfg.Recovery)
	}
	// The unset (zero) TTL is refused by the gate by name.
	if _, err := NewGate(&controlstore.Store{}, GateOptions{TTL: cfg.Recovery.GateTTL}); err == nil ||
		!strings.Contains(err.Error(), config.EnvRecoveryGateTTL) {
		t.Fatalf("unset TTL must refuse by name %s, got %v", config.EnvRecoveryGateTTL, err)
	}
	// Malformed and non-positive TTL values are refused by name.
	for _, raw := range []string{"abc", "0s", "-5s", "1"} {
		if _, err := config.Load(contractEnv(map[string]string{config.EnvRecoveryGateTTL: raw})); err == nil ||
			!strings.Contains(err.Error(), config.EnvRecoveryGateTTL) {
			t.Fatalf("TXHARBOR_RECOVERY_GATE_TTL=%q must refuse by name, got %v", raw, err)
		}
	}
	// Format-valid knobs parse (no bypass semantics anywhere).
	good, err := config.Load(contractEnv(map[string]string{
		config.EnvRecoveryGateTTL: "45s",
	}))
	if err != nil {
		t.Fatalf("valid TTL must parse: %v", err)
	}
	if good.Recovery.GateTTL != 45*time.Second {
		t.Fatalf("GateTTL = %s, want 45s", good.Recovery.GateTTL)
	}

	// Deployment-config bypass scan: every TXHARBOR_RECOVERY_* key in the
	// config source is inspected so a future "close the gate" knob fails this
	// contract instead of silently shipping.
	src, err := os.ReadFile("../config/config.go")
	if err != nil {
		t.Fatalf("read config source: %v", err)
	}
	keyRE := regexp.MustCompile(`"(TXHARBOR_RECOVERY_[A-Z0-9_]*)"`)
	matches := keyRE.FindAllStringSubmatch(string(src), -1)
	if len(matches) == 0 {
		t.Fatal("config source scan found no TXHARBOR_RECOVERY_* keys; the anti-bypass contract cannot be verified")
	}
	seen := make(map[string]bool, len(matches))
	for _, m := range matches {
		key := m[1]
		seen[key] = true
		if contractBypassName.MatchString(key) {
			t.Fatalf("config key %s looks like a gate bypass; 015 has no close/disable switch (quickstart §4)", key)
		}
	}
	for _, required := range []string{
		config.EnvRecoveryControlDSN, config.EnvRecoveryPrincipal, config.EnvRecoveryInstance,
		config.EnvRecoveryGateTTL, config.EnvRecoveryArtifactDir,
	} {
		if !seen[required] {
			t.Fatalf("config source must declare %s", required)
		}
	}
}

// TestGateContractFundGatesRemainPhaseTwo pins FR-025/F20: the existing fund
// gates are an independent second phase at the action site, exposed to the
// gate only as an injected checker; no option or decision field can waive
// them, and a phase-one refusal never reaches the checker.
func TestGateContractFundGatesRemainPhaseTwo(t *testing.T) {
	checkerType := reflect.TypeOf(FundGateChecker(nil))
	if checkerType.Kind() != reflect.Func || checkerType.NumIn() != 2 || checkerType.NumOut() != 1 {
		t.Fatalf("FundGateChecker must be a 2-in/1-out func, got %s", checkerType)
	}
	if checkerType.In(0) != reflect.TypeOf((*context.Context)(nil)).Elem() || checkerType.In(1) != reflect.TypeOf(GateRequest{}) {
		t.Fatalf("FundGateChecker signature = %s, want func(context.Context, GateRequest) error", checkerType)
	}
	if !checkerType.Out(0).Implements(reflect.TypeOf((*error)(nil)).Elem()) {
		t.Fatalf("FundGateChecker must return error, got %s", checkerType.Out(0))
	}
	optsField, ok := reflect.TypeOf(GateOptions{}).FieldByName("FundGates")
	if !ok || optsField.Type != checkerType {
		t.Fatalf("GateOptions must carry the phase-two call point as FundGates, got %+v (found=%v)", optsField, ok)
	}
	if _, ok := reflect.TypeOf(GateDecision{}).FieldByName("PhaseTwoEvaluated"); !ok {
		t.Fatal("GateDecision must report PhaseTwoEvaluated so a caller cannot mistake phase one for the full authority")
	}
	if !RefusalHardGateActive.Known() {
		t.Fatal("hard_gate_active must be a refusal class (the action-site fund gate wins a phase-one allow)")
	}

	// No decision or option field may waive, disable or pre-approve the fund
	// gates; the only phase-two surface is the orthogonal checker above.
	for _, typ := range []reflect.Type{reflect.TypeOf(GateDecision{}), reflect.TypeOf(GateOptions{}), reflect.TypeOf(GateRequest{})} {
		for i := 0; i < typ.NumField(); i++ {
			if contractBypassName.MatchString(typ.Field(i).Name) {
				t.Fatalf("%s.%s looks like a fund-gate/fund-gate-check bypass", typ.Name(), typ.Field(i).Name)
			}
		}
	}
	for _, typ := range []reflect.Type{reflect.TypeOf(&Gate{})} {
		for i := 0; i < typ.NumMethod(); i++ {
			if contractBypassName.MatchString(typ.Method(i).Name) {
				t.Fatalf("%s.%s looks like a gate bypass method", typ, typ.Method(i).Name)
			}
		}
	}

	// Executed: a phase-one refusal never reaches the injected checker.
	token := contractInstanceToken()
	gate := contractGateWithIsolationState(t, token, "pending")
	calls := 0
	gate.fundGates = func(context.Context, GateRequest) error { calls++; return nil }
	ev := contractEvaluate(t, gate, token, GateRequest{
		InstanceID: token.InstanceID, Capability: CapabilityQuery, ScopeHash: contractGateScope,
	})
	if ev.allowed || ev.refusal == nil {
		t.Fatalf("phase one must refuse with unverified isolation, got allowed=%v refusal=%+v", ev.allowed, ev.refusal)
	}
	if calls != 0 {
		t.Fatalf("the phase-two fund-gate checker ran %d time(s) before phase one passed", calls)
	}
}

// ---------------------------------------------------------------------------
// Storeless contract harness: generation-bound facts in the real cache
// ---------------------------------------------------------------------------

// contractInstanceToken is one open recovery instance token. No database row
// backs it here: the harness below seeds the generation-bound facts the cache
// would hold after a successful read at this token, and every derivation that
// would need the store is expected to refuse before touching it.
func contractInstanceToken() controlstore.InstanceToken {
	return controlstore.InstanceToken{
		InstanceID:         "11111111-2222-4333-8444-555555555555",
		State:              "open",
		Kind:               "recovery",
		EvidenceGeneration: 7,
		EvidenceHash:       "sha256:contract-gate-facts",
	}
}

// contractGateWithIsolationState builds a Gate whose bounded cache holds
// generation-bound capability facts for every capability: each item of the
// real isolation dependency set is set to state. The entries match the token
// generation/hash and are unexpired, so capabilityFacts takes its real cache
// path and evaluateLocked runs the real derivation without any store access.
func contractGateWithIsolationState(t *testing.T, token controlstore.InstanceToken, state string) *Gate {
	t.Helper()
	now := time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)
	gate := &Gate{
		ttl:   time.Minute,
		now:   func() time.Time { return now },
		cache: make(map[gateCacheKey]gateCapabilityFacts),
	}
	for _, capability := range KnownCapabilities() {
		items, err := IsolationDependencySet(capability)
		if err != nil {
			t.Fatalf("IsolationDependencySet(%s): %v", capability, err)
		}
		if len(items) == 0 {
			t.Fatalf("capability %s has an empty isolation dependency set; default deny is not guaranteed", capability)
		}
		rows := make([]gateIsolationRow, 0, len(items))
		for _, item := range items {
			if !item.Known() {
				t.Fatalf("capability %s names unknown isolation item %q", capability, item)
			}
			rows = append(rows, gateIsolationRow{item: item, state: state})
		}
		gate.cachePut(gateCacheKey{instanceID: token.InstanceID, capability: capability}, gateCapabilityFacts{
			generation: token.EvidenceGeneration,
			hash:       token.EvidenceHash,
			expiresAt:  now.Add(time.Minute),
			isolation:  rows,
		})
	}
	return gate
}

// contractEvaluate runs the real in-lock derivation with a nil transaction: a
// correct refusal happens before the first store read (isolation facts screen
// the release stream); reaching the store means the derivation changed shape,
// which is a contract failure, not a panic to tolerate.
func contractEvaluate(t *testing.T, gate *Gate, token controlstore.InstanceToken, req GateRequest) gateEvaluation {
	t.Helper()
	var ev gateEvaluation
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("contract evaluation of %s reached the control store (nil transaction): %v; the derivation must refuse before any store read", req.Capability, r)
			}
		}()
		ev = gate.evaluateLocked(context.Background(), nil, token, req)
	}()
	return ev
}

// contractEnv is the minimal valid environment for config.Load (the config
// package's own base fixture, duplicated here so this contract file depends
// only on exported API).
func contractEnv(overrides map[string]string) config.Getenv {
	env := map[string]string{
		config.EnvPGDSN:                 "postgres://txharbor:txharbor@127.0.0.1:5432/txharbor?sslmode=disable",
		config.EnvRPCURL:                "http://127.0.0.1:8545",
		config.EnvChainID:               "31337",
		config.EnvStartHeight:           "0",
		config.EnvLogStartHeight:        "0",
		config.EnvLogContracts:          "0x1111111111111111111111111111111111111111",
		config.EnvDepositStartHeight:    "0",
		config.EnvDepositContracts:      "0x1111111111111111111111111111111111111111",
		config.EnvDepositWatchAddresses: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		config.EnvConfirmationDepth:     "10",
	}
	for key, value := range overrides {
		env[key] = value
	}
	return func(key string) (string, bool) {
		value, ok := env[key]
		return value, ok
	}
}
