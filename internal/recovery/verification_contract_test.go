//go:build contract

// verification_contract_test.go is T035 [US3]: the verification-conclusion
// contract layer (tags: contract; no Docker; FR-015/016/018; data-model.md
// §1.5/§4.2; contracts/verification-items.md §1; tasks.md T035).
//
// TDD-first. B10 lands the tests before the B11 implementation, so this file
// references the planned T038/T039/T040 API (internal/recovery/verification.go)
// that does not exist yet and the contract candidate fails to build until B11
// lands. Expected API surface (documented at every use site):
//
//	// internal/recovery/verification.go (T038)
//	type VerificationCategory string
//	const (
//	    VerificationV1 VerificationCategory = "V1"
//	    ... VerificationV9
//	)
//	func KnownVerificationCategories() []VerificationCategory
//	func ParseVerificationCategory(raw string) (VerificationCategory, error)
//	var ErrUnknownVerificationCategory error
//
//	type VerificationConclusion string
//	const (
//	    ConclusionConsistent VerificationConclusion = "consistent"
//	    ConclusionDivergent  VerificationConclusion = "divergent"
//	    ConclusionUnknown    VerificationConclusion = "unknown"
//	    ConclusionStale      VerificationConclusion = "stale"
//	)
//	func KnownVerificationConclusions() []VerificationConclusion
//	func ParseVerificationConclusion(raw string) (VerificationConclusion, error)
//	func (c VerificationConclusion) Known() bool
//	func (c VerificationConclusion) Passes() bool // true only for consistent
//	var ErrUnknownVerificationConclusion error
//
//	type SourceObservation struct {
//	    ObjectKey    string
//	    Scope        []byte
//	    Sources      []byte
//	    Conclusion   VerificationConclusion
//	    Reason       string
//	    EvidenceRefs []string
//	    ObservedAt   time.Time
//	}
//	type ConclusionInput struct {
//	    Category         VerificationCategory
//	    Sources          []SourceObservation
//	    EvidenceComplete bool
//	    CoverageClosed   bool
//	    Missing          bool
//	    Tolerance        time.Duration
//	    Now              time.Time
//	}
//	func EvaluateConclusion(in ConclusionInput) (VerificationConclusion, string)
//
// Conclusion rules pinned here (contracts/verification-items.md §1):
//
//   - consistent only when the evidence is complete, fresh within the
//     configured tolerance, coverage is closed and every source agrees;
//   - any source unknown caps the item at unknown (it can never become
//     consistent);
//   - unknown/stale never pass (Passes is false for everything but consistent);
//   - a missing record is not "never happened / never paid / safe to
//     re-execute": Missing yields unknown with a reason, never consistent;
//   - nothing is filled in from a default, a prior conclusion or a clock: an
//     unconfigured tolerance (<= 0), a zero evaluation instant, a zero
//     observation time, an incomplete coverage/evidence flag and an empty
//     source list all refuse a consistent verdict;
//   - divergent is reported explicitly when a source contradicts the local
//     state (FR-015: the difference must be listed in the conclusion).
//
// The evaluator is a pure function: no store, no clock, no network. The store
// and adaptation layers that persist these conclusions are exercised in
// verification_integration_test.go (T036) and gaps_integration_test.go (T037).
package recovery

import (
	"errors"
	"slices"
	"testing"
	"time"
)

// t035Now is the fixed evaluation instant every table below uses; EvaluateConclusion
// must consume it as an input, never read a clock.
var t035Now = time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)

const t035Tolerance = time.Hour

func t035Observation(objectKey string, conclusion VerificationConclusion, observedAt time.Time) SourceObservation {
	return SourceObservation{
		ObjectKey:    objectKey,
		Scope:        []byte(`{"chain_id":31337,"asset":"usdc"}`),
		Sources:      []byte(`{"pg":{"state":"observed"},"chain":{"height":12}}`),
		Conclusion:   conclusion,
		Reason:       "source " + string(conclusion),
		EvidenceRefs: []string{"evidence://015/t035/" + objectKey},
		ObservedAt:   observedAt,
	}
}

// t035Evaluate runs EvaluateConclusion and enforces the invariants every
// verdict must satisfy regardless of the case: a known conclusion, and a
// non-empty reason whenever the verdict is not consistent.
func t035Evaluate(t *testing.T, in ConclusionInput) VerificationConclusion {
	t.Helper()
	conclusion, reason := EvaluateConclusion(in)
	if !conclusion.Known() {
		t.Fatalf("EvaluateConclusion returned %q, outside the closed set", conclusion)
	}
	if conclusion != ConclusionConsistent && reason == "" {
		t.Fatalf("non-consistent conclusion %q must carry a reason", conclusion)
	}
	return conclusion
}

// TestT035ClosedConclusionAndCategorySets pins the closed V1–V9 category set
// and the closed consistent/divergent/unknown/stale conclusion set, the exact
// string vocabulary, the parse/known rejects, and the pass rule: only
// consistent passes (FR-018; data-model.md §1.5).
func TestT035ClosedConclusionAndCategorySets(t *testing.T) {
	wantCategories := []VerificationCategory{
		VerificationV1, VerificationV2, VerificationV3, VerificationV4, VerificationV5,
		VerificationV6, VerificationV7, VerificationV8, VerificationV9,
	}
	if got := KnownVerificationCategories(); !slices.Equal(got, wantCategories) {
		t.Fatalf("KnownVerificationCategories() = %v, want %v", got, wantCategories)
	}
	for _, raw := range []string{"", "v1", "V0", "V10", "V1 ", " V1", "unknown", "all"} {
		if _, err := ParseVerificationCategory(raw); !errors.Is(err, ErrUnknownVerificationCategory) {
			t.Fatalf("ParseVerificationCategory(%q) error = %v, want ErrUnknownVerificationCategory", raw, err)
		}
	}
	if c, err := ParseVerificationCategory("V4"); err != nil || c != VerificationV4 {
		t.Fatalf("ParseVerificationCategory(V4) = %q %v, want V4", c, err)
	}

	wantConclusions := []VerificationConclusion{
		ConclusionConsistent, ConclusionDivergent, ConclusionUnknown, ConclusionStale,
	}
	if got := KnownVerificationConclusions(); !slices.Equal(got, wantConclusions) {
		t.Fatalf("KnownVerificationConclusions() = %v, want %v", got, wantConclusions)
	}
	for _, raw := range []string{"", "CONSISTENT", "passed", "ok", "divergence", "unknown "} {
		if _, err := ParseVerificationConclusion(raw); !errors.Is(err, ErrUnknownVerificationConclusion) {
			t.Fatalf("ParseVerificationConclusion(%q) error = %v, want ErrUnknownVerificationConclusion", raw, err)
		}
	}
	if c, err := ParseVerificationConclusion("stale"); err != nil || c != ConclusionStale {
		t.Fatalf("ParseVerificationConclusion(stale) = %q %v", c, err)
	}

	for _, c := range wantConclusions {
		if !c.Known() {
			t.Fatalf("conclusion %q must be known", c)
		}
		if want := c == ConclusionConsistent; c.Passes() != want {
			t.Fatalf("conclusion %q Passes() = %v, want %v (unknown/stale must never pass)", c, c.Passes(), want)
		}
	}
}

// TestT035ConsistentRequiresCompleteFreshClosedUnanimous executes the
// consistent rule: only complete evidence, inside the configured freshness
// tolerance, with closed coverage and every source agreeing may pass. A single
// stale, unknown or divergent source blocks the item.
func TestT035ConsistentRequiresCompleteFreshClosedUnanimous(t *testing.T) {
	fresh := t035Now.Add(-30 * time.Minute)
	edgeFresh := t035Now.Add(-t035Tolerance) // exactly at the tolerance is still fresh
	tooOld := t035Now.Add(-t035Tolerance - time.Nanosecond)

	base := ConclusionInput{
		Category:         VerificationV1,
		EvidenceComplete: true,
		CoverageClosed:   true,
		Tolerance:        t035Tolerance,
		Now:              t035Now,
	}

	cases := []struct {
		name string
		in   ConclusionInput
		want VerificationConclusion
	}{
		{
			name: "single fresh consistent source",
			in:   withSources(base, t035Observation("block:12", ConclusionConsistent, fresh)),
			want: ConclusionConsistent,
		},
		{
			name: "two agreeing fresh sources",
			in: withSources(base,
				t035Observation("block:12", ConclusionConsistent, fresh),
				t035Observation("block:12", ConclusionConsistent, t035Now.Add(-time.Minute))),
			want: ConclusionConsistent,
		},
		{
			name: "observation exactly at the tolerance boundary is fresh",
			in:   withSources(base, t035Observation("block:12", ConclusionConsistent, edgeFresh)),
			want: ConclusionConsistent,
		},
		{
			name: "one nanosecond past the tolerance is stale",
			in:   withSources(base, t035Observation("block:12", ConclusionConsistent, tooOld)),
			want: ConclusionStale,
		},
		{
			name: "a stale source blocks an otherwise consistent item",
			in: withSources(base,
				t035Observation("block:12", ConclusionConsistent, fresh),
				t035Observation("log:7", ConclusionConsistent, tooOld)),
			want: ConclusionStale,
		},
		{
			name: "one unknown source caps the item at unknown",
			in: withSources(base,
				t035Observation("block:12", ConclusionConsistent, fresh),
				t035Observation("log:7", ConclusionUnknown, fresh)),
			want: ConclusionUnknown,
		},
		{
			name: "unknown outranks stale",
			in: withSources(base,
				t035Observation("block:12", ConclusionStale, tooOld),
				t035Observation("log:7", ConclusionUnknown, fresh)),
			want: ConclusionUnknown,
		},
		{
			name: "an explicit difference is reported as divergent",
			in: withSources(base,
				t035Observation("block:12", ConclusionConsistent, fresh),
				t035Observation("log:7", ConclusionDivergent, fresh)),
			want: ConclusionDivergent,
		},
		{
			name: "divergent plus unknown can never be consistent",
			in: withSources(base,
				t035Observation("block:12", ConclusionDivergent, fresh),
				t035Observation("log:7", ConclusionUnknown, fresh)),
			want: ConclusionDivergent, // precedence: explicit difference, then unknown, then stale
		},
		{
			name: "incomplete evidence blocks",
			in:   withSources(withoutEvidenceComplete(base), t035Observation("block:12", ConclusionConsistent, fresh)),
			want: ConclusionUnknown,
		},
		{
			name: "open coverage blocks",
			in:   withSources(withoutCoverageClosed(base), t035Observation("block:12", ConclusionConsistent, fresh)),
			want: ConclusionUnknown,
		},
		{
			name: "no sources is never consistent",
			in:   withSources(base),
			want: ConclusionUnknown,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := t035Evaluate(t, tc.in); got != tc.want {
				t.Fatalf("EvaluateConclusion = %s, want %s", got, tc.want)
			}
			if tc.want != ConclusionConsistent {
				if _, reason := EvaluateConclusion(tc.in); reason == "" {
					t.Fatal("a blocking verdict must explain itself")
				}
			}
		})
	}
}

// TestT035UnknownAndStaleNeverPass pins the FR-018 rule twice: as verdicts
// (Passes) and as evaluator outputs — no input combination that contains an
// unknown or stale condition may produce consistent.
func TestT035UnknownAndStaleNeverPass(t *testing.T) {
	if ConclusionUnknown.Passes() || ConclusionStale.Passes() {
		t.Fatal("unknown/stale must never pass (FR-018)")
	}
	fresh := t035Now.Add(-time.Minute)
	base := ConclusionInput{
		Category:         VerificationV2,
		EvidenceComplete: true,
		CoverageClosed:   true,
		Tolerance:        t035Tolerance,
		Now:              t035Now,
	}
	inputs := []ConclusionInput{
		withSources(base, t035Observation("intent:1", ConclusionUnknown, fresh)),
		withSources(base, t035Observation("intent:1", ConclusionStale, fresh)),
		withSources(base,
			t035Observation("intent:1", ConclusionConsistent, fresh),
			t035Observation("intent:1", ConclusionUnknown, fresh)),
		withSources(base,
			t035Observation("intent:1", ConclusionConsistent, fresh),
			t035Observation("intent:1", ConclusionStale, fresh)),
	}
	for i, in := range inputs {
		got := t035Evaluate(t, in)
		if got == ConclusionConsistent || got.Passes() {
			t.Fatalf("input %d produced %s which must not pass", i, got)
		}
	}
}

// TestT035MissingIsNotNeverHappened pins FR-016/FR-017: a record missing at the
// restore point is an evidence gap (unknown), never "never happened", "never
// paid" or "safe to re-execute"; and an entirely default (zero) input is
// unknown, not an implicit pass.
func TestT035MissingIsNotNeverHappened(t *testing.T) {
	fresh := t035Now.Add(-time.Minute)
	base := ConclusionInput{
		Category:         VerificationV2,
		EvidenceComplete: true,
		CoverageClosed:   true,
		Tolerance:        t035Tolerance,
		Now:              t035Now,
	}

	missing := withSources(base, t035Observation("intent:int-1", ConclusionConsistent, fresh))
	missing.Missing = true
	if got := t035Evaluate(t, missing); got != ConclusionUnknown {
		t.Fatalf("Missing=true evaluated to %s, want unknown (缺失 ≠ 从未发生)", got)
	}
	if _, reason := EvaluateConclusion(missing); reason == "" {
		t.Fatal("a missing-record verdict must carry the gap reason")
	}

	// The zero value must not be interpretable as a pass: no category, no
	// sources, no tolerance, no evaluation instant.
	if got := t035Evaluate(t, ConclusionInput{}); got != ConclusionUnknown {
		t.Fatalf("zero ConclusionInput evaluated to %s, want unknown (no default fill)", got)
	}

	// A missing record cannot be overridden by otherwise perfect sources.
	missingCoverage := withSources(base, t035Observation("intent:int-1", ConclusionConsistent, fresh))
	missingCoverage.CoverageClosed = false
	if got := t035Evaluate(t, missingCoverage); got != ConclusionUnknown {
		t.Fatalf("coverage-open item evaluated to %s, want unknown", got)
	}
}

// TestT035NoDefaultsOrTimeInference pins FR-018's "no defaults, no prior state,
// no time inference": an unconfigured tolerance refuses, a zero evaluation
// instant refuses, an observation without a timestamp refuses, a future
// timestamp cannot be assumed fresh, and the verdict changes only with the
// declared inputs (never with a hidden clock).
func TestT035NoDefaultsOrTimeInference(t *testing.T) {
	fresh := t035Now.Add(-time.Minute)
	base := ConclusionInput{
		Category:         VerificationV4,
		EvidenceComplete: true,
		CoverageClosed:   true,
		Tolerance:        t035Tolerance,
		Now:              t035Now,
	}

	// Tolerance is deployment configuration: missing (0) or negative refuses a
	// pass; it is never replaced by an invented default.
	for _, tolerance := range []time.Duration{0, -time.Second} {
		in := withSources(base, t035Observation("attempt:a-1", ConclusionConsistent, fresh))
		in.Tolerance = tolerance
		if got := t035Evaluate(t, in); got == ConclusionConsistent {
			t.Fatalf("tolerance %s tolerated a consistent verdict; missing freshness config must stay conservative", tolerance)
		}
	}

	// No evaluation instant: freshness is unprovable.
	noNow := withSources(base, t035Observation("attempt:a-1", ConclusionConsistent, fresh))
	noNow.Now = time.Time{}
	if got := t035Evaluate(t, noNow); got == ConclusionConsistent {
		t.Fatal("a zero evaluation instant must not produce consistent")
	}

	// No observation timestamp: cannot be assumed to be "now".
	noObservedAt := withSources(base, t035Observation("attempt:a-1", ConclusionConsistent, time.Time{}))
	if got := t035Evaluate(t, noObservedAt); got == ConclusionConsistent {
		t.Fatal("an observation without a timestamp must not produce consistent")
	}

	// A future timestamp cannot be trusted as fresh.
	future := withSources(base, t035Observation("attempt:a-1", ConclusionConsistent, t035Now.Add(time.Hour)))
	if got := t035Evaluate(t, future); got == ConclusionConsistent {
		t.Fatal("a future observation timestamp must not produce consistent")
	}

	// The same input is fresh or stale depending only on the declared Now: no
	// hidden wall clock, no cached prior verdict.
	watched := withSources(base, t035Observation("attempt:a-1", ConclusionConsistent, t035Now.Add(-time.Minute)))
	if got := t035Evaluate(t, watched); got != ConclusionConsistent {
		t.Fatalf("fresh input evaluated to %s, want consistent", got)
	}
	later := watched
	later.Now = t035Now.Add(2 * time.Hour)
	if got := t035Evaluate(t, later); got != ConclusionStale {
		t.Fatalf("the same observation evaluated two hours later = %s, want stale", got)
	}
	// ... and re-evaluating the original input still yields consistent: the
	// result is a pure function of the declared inputs.
	if got := t035Evaluate(t, watched); got != ConclusionConsistent {
		t.Fatalf("re-evaluating the original input = %s, want consistent (no cached/prior state)", got)
	}
}

// withSources attaches observations to a base input.
func withSources(base ConclusionInput, sources ...SourceObservation) ConclusionInput {
	base.Sources = sources
	return base
}

func withoutEvidenceComplete(base ConclusionInput) ConclusionInput {
	base.EvidenceComplete = false
	return base
}

func withoutCoverageClosed(base ConclusionInput) ConclusionInput {
	base.CoverageClosed = false
	return base
}
