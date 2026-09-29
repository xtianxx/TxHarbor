package recovery

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

// drillTestInputs is a valid local-input annotation (purpose + the explicit
// unconfigured constraint list for constraints_configured=false).
func drillTestInputs() []byte {
	return []byte(`{"purpose":"local drill inputs only; not production thresholds","unconfigured_constraints":["rto_target","retention"]}`)
}

func drillValidInput() DrillRunInput {
	return DrillRunInput{
		InstanceID:        "11111111-1111-4111-8111-111111111111",
		Scenario:          DrillScenarioFullRecovery,
		RecoveryPoint:     []byte(`{"wall_clock":"2026-09-01T00:00:00Z","wal_lsn":"0/16B6C50"}`),
		BackupLag:         []byte(`{"state":"unknown","reason":"no lag measurement source in this drill invocation"}`),
		UncoveredInterval: []byte(`{"state":"unknown","reason":"not measured in this drill invocation"}`),
		TestInputs:        drillTestInputs(),
		GapCounts:         []byte(`{"open":0,"closed":0,"escalated":0}`),
		Result:            DrillResultOK,
		LogRef:            "docs/evidence/015/drill/drill-example.json",
		Actor:             "deploy:executor",
	}
}

// TestDrillScenarioSet covers the closed scenario vocabulary: the positive
// flow plus the seven failure-injection identifiers of FR-032.
func TestDrillScenarioSet(t *testing.T) {
	scenarios := KnownDrillScenarios()
	if len(scenarios) != 8 {
		t.Fatalf("KnownDrillScenarios() = %d, want 8 (full_recovery + F1..F7)", len(scenarios))
	}
	injections := KnownFailureInjections()
	if len(injections) != 7 {
		t.Fatalf("KnownFailureInjections() = %v, want 7 identifiers", injections)
	}
	seen := map[string]bool{}
	for _, scenario := range scenarios {
		if !scenario.Known() {
			t.Errorf("scenario %q is not Known()", scenario)
		}
		id, isInjection := scenario.FailureInjection()
		if scenario == DrillScenarioFullRecovery {
			if isInjection {
				t.Errorf("full_recovery must not carry a failure injection (%s)", id)
			}
			continue
		}
		if !isInjection || id == "" {
			t.Errorf("scenario %q must carry a failure injection identifier", scenario)
			continue
		}
		seen[id] = true
	}
	for _, id := range injections {
		if !seen[id] {
			t.Errorf("failure injection %s is not carried by any scenario", id)
		}
	}
}

// TestParseDrillScenarioAndResult pins the exact-match parsing: unknown input
// is refused, never mapped onto a default.
func TestParseDrillScenarioAndResult(t *testing.T) {
	for _, scenario := range KnownDrillScenarios() {
		parsed, err := ParseDrillScenario(" " + string(scenario) + " ")
		if err != nil || parsed != scenario {
			t.Fatalf("ParseDrillScenario(%q) = (%q, %v), want the same scenario", scenario, parsed, err)
		}
	}
	if _, err := ParseDrillScenario("F1"); !errors.Is(err, ErrUnknownDrillScenario) {
		t.Fatalf("bare F1 must be refused as an unknown scenario, got %v", err)
	}
	if _, err := ParseDrillScenario("FULL_RECOVERY"); !errors.Is(err, ErrUnknownDrillScenario) {
		t.Fatalf("case-folded scenario must be refused, got %v", err)
	}

	for _, result := range KnownDrillResults() {
		if parsed, err := ParseDrillResult(string(result)); err != nil || parsed != result {
			t.Fatalf("ParseDrillResult(%q) = (%q, %v)", result, parsed, err)
		}
	}
	if _, err := ParseDrillResult("passed"); !errors.Is(err, ErrUnknownDrillResult) {
		t.Fatalf("unknown result must be refused, got %v", err)
	}
}

// TestPrepareDrillRunValidation covers the T056 input contract: every missing
// or malformed component is refused with zero writes, and the
// constraints_configured=false annotation is mandatory.
func TestPrepareDrillRunValidation(t *testing.T) {
	valid := drillValidInput()
	cases := []struct {
		name    string
		mutate  func(*DrillRunInput)
		wantErr error
	}{
		{"missing instance", func(in *DrillRunInput) { in.InstanceID = "" }, ErrDrillInput},
		{"non-uuid instance", func(in *DrillRunInput) { in.InstanceID = "not-a-uuid" }, ErrDrillInput},
		{"unknown scenario", func(in *DrillRunInput) { in.Scenario = "f8" }, ErrUnknownDrillScenario},
		{"unknown result", func(in *DrillRunInput) { in.Result = "passed" }, ErrUnknownDrillResult},
		{"missing actor", func(in *DrillRunInput) { in.Actor = " " }, ErrDrillInput},
		{"missing recovery point", func(in *DrillRunInput) { in.RecoveryPoint = nil }, ErrDrillInput},
		{"scalar recovery point", func(in *DrillRunInput) { in.RecoveryPoint = []byte(`"0/1"`) }, ErrDrillInput},
		{"missing backup lag", func(in *DrillRunInput) { in.BackupLag = nil }, ErrDrillInput},
		{"missing uncovered interval", func(in *DrillRunInput) { in.UncoveredInterval = nil }, ErrDrillInput},
		{"missing test inputs", func(in *DrillRunInput) { in.TestInputs = nil }, ErrDrillInput},
		{"test inputs without purpose", func(in *DrillRunInput) {
			in.TestInputs = []byte(`{"unconfigured_constraints":["rto_target"]}`)
		}, ErrDrillInput},
		{"unconfigured constraints unnamed", func(in *DrillRunInput) {
			in.TestInputs = []byte(`{"purpose":"local drill inputs only"}`)
			in.ConstraintsConfigured = false
		}, ErrDrillInput},
		{"negative db restore seconds", func(in *DrillRunInput) {
			negative := -1.0
			in.DBRestoreSeconds = &negative
		}, ErrDrillInput},
		{"nan verification seconds", func(in *DrillRunInput) {
			nan := 0.0
			nan = nan / nan
			in.VerificationSeconds = &nan
		}, ErrDrillInput},
		{"unknown capability release key", func(in *DrillRunInput) {
			in.CapabilityReleaseSeconds = map[Capability]float64{"not_a_capability": 1}
		}, ErrDrillInput},
		{"negative capability release", func(in *DrillRunInput) {
			in.CapabilityReleaseSeconds = map[Capability]float64{CapabilityQuery: -1}
		}, ErrDrillInput},
		{"scalar gap counts", func(in *DrillRunInput) { in.GapCounts = []byte(`7`) }, ErrDrillInput},
		{"control character log ref", func(in *DrillRunInput) { in.LogRef = "docs/evidence/015\n.json" }, ErrDrillInput},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := valid
			tc.mutate(&input)
			if _, err := prepareDrillRun(input); !errors.Is(err, tc.wantErr) {
				t.Fatalf("prepareDrillRun() error = %v, want %v", err, tc.wantErr)
			}
		})
	}

	// A configured constraint set needs no unconfigured list; the purpose
	// annotation stays mandatory.
	configured := valid
	configured.ConstraintsConfigured = true
	configured.TestInputs = []byte(`{"purpose":"local drill inputs only; not production thresholds"}`)
	prepared, err := prepareDrillRun(configured)
	if err != nil {
		t.Fatalf("prepareDrillRun(configured) = %v", err)
	}
	if !prepared.constraintsConfigured {
		t.Fatalf("constraints_configured must be carried verbatim")
	}
}

// TestPrepareDrillRunSeparateTimingShape pins the storage shape: the two time
// scopes are independent, the per-capability payload carries every known
// capability (explicit null when unmeasured) and the digest is stable.
func TestPrepareDrillRunSeparateTimingShape(t *testing.T) {
	input := drillValidInput()
	restore := 12.5
	verification := 3.25
	input.DBRestoreSeconds = &restore
	input.VerificationSeconds = &verification
	input.CapabilityReleaseSeconds = map[Capability]float64{
		CapabilityQuery:     1.5,
		CapabilityChainScan: 4.5,
	}
	prepared, err := prepareDrillRun(input)
	if err != nil {
		t.Fatalf("prepareDrillRun() = %v", err)
	}
	if prepared.dbRestoreSeconds == nil || *prepared.dbRestoreSeconds != 12.5 {
		t.Fatalf("db_restore_seconds = %v, want 12.5 (separate scope)", prepared.dbRestoreSeconds)
	}
	if prepared.verificationSeconds == nil || *prepared.verificationSeconds != 3.25 {
		t.Fatalf("verification_seconds = %v, want 3.25 (separate scope)", prepared.verificationSeconds)
	}

	var payload map[string]*float64
	if err := json.Unmarshal(prepared.capabilityReleaseSeconds, &payload); err != nil {
		t.Fatalf("decode capability payload: %v", err)
	}
	if len(payload) != len(knownCapabilities) {
		t.Fatalf("capability payload has %d keys, want all %d known capabilities", len(payload), len(knownCapabilities))
	}
	if payload["query"] == nil || *payload["query"] != 1.5 {
		t.Fatalf("payload[query] = %v, want 1.5", payload["query"])
	}
	if payload["event_consuming"] != nil {
		t.Fatalf("payload[event_consuming] = %v, want explicit null (never a fabricated zero)", payload["event_consuming"])
	}

	digest2, err := drillInputDigestOf(prepared)
	if err != nil {
		t.Fatalf("drillInputDigestOf() = %v", err)
	}
	if digest2 != prepared.inputDigest {
		t.Fatalf("input digest is not stable: %s != %s", digest2, prepared.inputDigest)
	}
	changed := prepared
	changed.result = DrillResultRefusedSafe
	digest3, err := drillInputDigestOf(changed)
	if err != nil {
		t.Fatalf("drillInputDigestOf(changed) = %v", err)
	}
	if digest3 == prepared.inputDigest {
		t.Fatalf("an input change must change the digest")
	}
}

// TestSafeResumptionSecondsRequiresFullScope is the T056 anti-claim guard:
// db_restore_seconds (or any partial set) can never produce an RTO-grade
// end-to-end value.
func TestSafeResumptionSecondsRequiresFullScope(t *testing.T) {
	dbRestore := 42.0
	run := DrillRun{DBRestoreSeconds: &dbRestore}
	if seconds, ok := run.SafeResumptionSeconds(); ok {
		t.Fatalf("db_restore_seconds alone must not yield a safe-resumption duration, got %v", seconds)
	}

	partial := DrillRun{CapabilityReleaseSeconds: map[Capability]float64{
		CapabilityQuery: 1, CapabilityChainScan: 2, CapabilityDepositConfirmation: 3,
	}}
	if seconds, ok := partial.SafeResumptionSeconds(); ok {
		t.Fatalf("a partial capability set must not yield a safe-resumption duration, got %v", seconds)
	}

	full := DrillRun{CapabilityReleaseSeconds: map[Capability]float64{}}
	for i, capability := range knownCapabilities {
		full.CapabilityReleaseSeconds[capability] = float64(i + 1)
	}
	seconds, ok := full.SafeResumptionSeconds()
	if !ok || seconds != float64(len(knownCapabilities)) {
		t.Fatalf("SafeResumptionSeconds() = (%v, %t), want (%d, true)", seconds, ok, len(knownCapabilities))
	}
}

// TestDecodeCapabilityReleasesRefusesUnknownKeys pins the read-back
// validation: a row carrying an unknown capability never silently decodes.
func TestDecodeCapabilityReleasesRefusesUnknownKeys(t *testing.T) {
	if _, err := decodeCapabilityReleases([]byte(`{"query":1.5,"mystery":2}`)); err == nil ||
		!strings.Contains(err.Error(), "unknown capability") {
		t.Fatalf("decodeCapabilityReleases() error = %v, want an unknown-capability refusal", err)
	}
	decoded, err := decodeCapabilityReleases([]byte(`{"query":1.5,"event_consuming":null}`))
	if err != nil {
		t.Fatalf("decodeCapabilityReleases() = %v", err)
	}
	if len(decoded) != 1 || decoded[CapabilityQuery] != 1.5 {
		t.Fatalf("decoded = %v, want only query=1.5 (null is unmeasured)", decoded)
	}
}
