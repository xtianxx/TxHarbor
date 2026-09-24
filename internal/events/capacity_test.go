// capacity_test.go is the T069 unit layer: the fail-closed configuration
// formula, the soft/hard boundary classification, the admission rule
// (including the fail-closed unknown level) and the static invariants that
// keep the guard honest:
//
//   - I-CAP: the Append path (append.go) never consults the capacity guard, so
//     an accepted unit can never be refused because of capacity;
//   - the guard never mutates event rows (no SQL write shape in capacity.go,
//     so the pending|blocked red line cannot be violated from here);
//   - hard_limit is documented as an admission gate + pause trigger, not a
//     physical capacity limit (statement discipline).
package events

import (
	"os"
	"strings"
	"testing"
)

func TestCapacityLimitsValidateFailClosed(t *testing.T) {
	cases := []struct {
		name string
		l    CapacityLimits
		ok   bool
	}{
		{"valid", CapacityLimits{Reserve: 5, SoftLimit: 10, HardLimit: 20}, true},
		{"zero_reserve", CapacityLimits{Reserve: 0, SoftLimit: 10, HardLimit: 20}, false},
		{"zero_soft", CapacityLimits{Reserve: 5, SoftLimit: 0, HardLimit: 20}, false},
		{"zero_hard", CapacityLimits{Reserve: 5, SoftLimit: 10, HardLimit: 0}, false},
		{"reserve_equals_soft", CapacityLimits{Reserve: 10, SoftLimit: 10, HardLimit: 20}, false},
		{"soft_equals_hard", CapacityLimits{Reserve: 5, SoftLimit: 20, HardLimit: 20}, false},
		{"soft_above_hard", CapacityLimits{Reserve: 5, SoftLimit: 21, HardLimit: 20}, false},
		{"negative", CapacityLimits{Reserve: -1, SoftLimit: 10, HardLimit: 20}, false},
		{"empty", CapacityLimits{}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.l.Validate()
			if tc.ok && err != nil {
				t.Fatalf("Validate(%+v) = %v, want nil", tc.l, err)
			}
			if !tc.ok && err == nil {
				t.Fatalf("Validate(%+v) = nil, want a fail-closed refusal", tc.l)
			}
			// Configured is the all-positive pre-check; Validate is the
			// ordering formula. A fully positive but unordered set is
			// Configured and still must be refused.
			allPositive := tc.l.Reserve > 0 && tc.l.SoftLimit > 0 && tc.l.HardLimit > 0
			if tc.l.Configured() != allPositive {
				t.Fatalf("Configured(%+v) = %v, want %v", tc.l, tc.l.Configured(), allPositive)
			}
		})
	}
}

func TestClassifyCapacityLevelBoundaries(t *testing.T) {
	limits := CapacityLimits{Reserve: 5, SoftLimit: 10, HardLimit: 20}
	cases := []struct {
		pending int64
		want    CapacityLevel
	}{
		{-1, CapacityNormal}, // guarded: never negative
		{0, CapacityNormal},
		{9, CapacityNormal},
		{10, CapacitySoft}, // soft boundary is inclusive
		{19, CapacitySoft},
		{20, CapacityHard}, // hard boundary is inclusive
		{21, CapacityHard},
		{1 << 40, CapacityHard},
	}
	for _, tc := range cases {
		if got := ClassifyCapacityLevel(tc.pending, limits); got != tc.want {
			t.Fatalf("ClassifyCapacityLevel(%d) = %s, want %s", tc.pending, got, tc.want)
		}
	}
}

func TestCapacityLevelRefusesFailClosed(t *testing.T) {
	if CapacityNormal.RefusesNewControllable() {
		t.Fatal("normal level refuses new controllable work")
	}
	if !CapacitySoft.RefusesNewControllable() {
		t.Fatal("soft level admits new controllable work")
	}
	if !CapacityHard.RefusesNewControllable() {
		t.Fatal("hard level admits new controllable work")
	}
	if !CapacityLevel("").RefusesNewControllable() {
		t.Fatal("unknown/zero level must fail closed")
	}
	if !CapacityLevel("unclassified").RefusesNewControllable() {
		t.Fatal("unknown level must fail closed")
	}
}

func TestDecideAdmission(t *testing.T) {
	limits := CapacityLimits{Reserve: 5, SoftLimit: 10, HardLimit: 20}
	cases := []struct {
		pending   int64
		refused   bool
		levelWant CapacityLevel
	}{
		{9, false, CapacityNormal},
		{10, true, CapacitySoft},
		{19, true, CapacitySoft},
		{20, true, CapacityHard},
		{200, true, CapacityHard},
	}
	for _, tc := range cases {
		admission := DecideAdmission(CapacitySnapshot{
			PendingTotal:     tc.pending,
			OldestAgeSeconds: 3.5,
			Level:            ClassifyCapacityLevel(tc.pending, limits),
		})
		if admission.Refused != tc.refused || admission.Level != tc.levelWant {
			t.Fatalf("pending %d: admission = %+v, want refused=%v level=%s",
				tc.pending, admission, tc.refused, tc.levelWant)
		}
	}
}

func TestNewCapacityGuardFailClosed(t *testing.T) {
	if _, err := NewCapacityGuard(nil, CapacityLimits{Reserve: 1, SoftLimit: 2, HardLimit: 3}, nil); err == nil {
		t.Fatal("nil pool accepted")
	}
	invalid := []CapacityLimits{
		{},
		{Reserve: 0, SoftLimit: 10, HardLimit: 20},
		{Reserve: 10, SoftLimit: 10, HardLimit: 20},
		{Reserve: 5, SoftLimit: 20, HardLimit: 20},
	}
	for _, limits := range invalid {
		if _, err := NewCapacityGuard(nil, limits, nil); err == nil {
			t.Fatalf("invalid limits %+v accepted", limits)
		}
	}
}

// TestCapacityGuardNeverMutatesEvents is the static half of the pending|blocked
// red line: capacity.go carries no SQL write shape at all, so the guard cannot
// silently drop, overwrite, re-publish or delete an event row.
func TestCapacityGuardNeverMutatesEvents(t *testing.T) {
	body := readPackageSource(t, "capacity.go")
	for _, token := range []string{"INSERT INTO", "UPDATE", "DELETE", "TRUNCATE", "DROP "} {
		if strings.Contains(body, token) {
			t.Errorf("capacity.go carries SQL write token %q; the guard must stay read-only", token)
		}
	}
}

// TestCapacityICAPAppendPathUngated pins I-CAP structurally: the transaction
// bound Append path never references the capacity guard, so a unit that was
// accepted can never be refused because of capacity (its event row is written
// in the same transaction as its business state).
func TestCapacityICAPAppendPathUngated(t *testing.T) {
	body := readPackageSource(t, "append.go")
	for _, token := range []string{
		"AdmitNewControllable", "CapacityGuard", "DecideAdmission",
		"ClassifyCapacityLevel", "CapacityAdmission", "CapacityLevel", "CapacityLimits",
	} {
		if strings.Contains(body, token) {
			t.Errorf("append.go references the capacity guard (%q); the accepted-write path must never be gated", token)
		}
	}
}

// capacityRequiredStatements are the non-negotiable hard_limit phrases
// (contracts/capacity.md §3): admission gate + pause trigger, and explicitly
// not a physical capacity limit.
var capacityRequiredStatements = []string{
	"admission gate + pause trigger",
	"not a physical capacity limit",
}

// capacityForbiddenClaims are assembled from pieces so this scanner never
// trips itself; each one would misstate hard_limit as a physical guarantee.
var capacityForbiddenClaims = []string{
	"is a physical capacity limit",
	"guarantees that physical storage",
	"storage exhaustion is impossible",
	"never run out of disk",
	"prevents physical storage exhaustion",
}

// TestCapacityStatementDiscipline checks the hard_limit wording both
// directions: the required semantic phrases are present, and no source in the
// package asserts a physical-capacity guarantee.
func TestCapacityStatementDiscipline(t *testing.T) {
	capacity := readPackageSource(t, "capacity.go")
	for _, want := range capacityRequiredStatements {
		if !strings.Contains(capacity, want) {
			t.Errorf("capacity.go is missing the required hard_limit statement %q", want)
		}
	}
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		body := readPackageSource(t, name)
		for _, claim := range capacityForbiddenClaims {
			if strings.Contains(body, claim) {
				t.Errorf("%s carries a forbidden capacity claim (%q)", name, claim)
			}
		}
	}
}

// readPackageSource reads one source file relative to the package directory.
func readPackageSource(t *testing.T, name string) string {
	t.Helper()
	body, err := os.ReadFile(name)
	if err != nil {
		t.Fatalf("ReadFile %s: %v", name, err)
	}
	return string(body)
}
