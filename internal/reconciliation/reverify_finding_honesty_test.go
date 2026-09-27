// reverify_finding_honesty_test.go is the T031 evidence pin for the reverify
// caller-claim rule (quickstart §11; tasks T031 special verification; Q1/Q5-4;
// contracts/discrepancy-lifecycle.md History Revalidation Sweep):
//
//   - the bounded reverify executor never takes a caller-declared "consistent"
//     on faith: normalizeReverifyFinding downgrades a consistent finding that
//     lacks its bounded evidence reference or its freshness instant to unknown,
//     so a caller claim alone can never support a close;
//   - an unknown verdict token is a wiring defect (error), never silently
//     mapped onto a closable verdict;
//   - divergent and unknown findings keep their fail-closed meaning.
//
// The production evidence re-read itself (real chain/PG/event sources, not a
// caller assertion) is proven one layer down by the `reverify` CLI case in
// internal/app/reconcileadmin/us3admin_integration_test.go, which drives the
// real EventStateReverifyEvaluator and observes unknown rather than a
// single-party consistent claim. This file adds the unit-level guard that no
// seam — including a test or future caller implementation — can smuggle a
// consistent verdict past the write path without evidence.
//
// No database and no Docker: every decision here is a pure function.
package reconciliation

import (
	"strings"
	"testing"
	"time"
)

// TestNormalizeReverifyFindingNeverTakesCallerConsistentOnFaith pins that a
// consistent finding without a bounded evidence reference and a non-zero
// freshness is downgraded to unknown and can never close.
func TestNormalizeReverifyFindingNeverTakesCallerConsistentOnFaith(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Microsecond)
	const tolerance = time.Hour

	cases := []struct {
		name        string
		in          ReverifyFinding
		wantErr     bool
		wantVerdict ReverifyVerdict
		downgraded  bool
	}{
		{
			name:        "consistent with evidence reference and freshness is accepted",
			in:          ReverifyFinding{Verdict: ReverifyConsistent, EvidenceRef: "chain:100:0xabc|pg:row|event:ev-1", FreshnessAt: now},
			wantVerdict: ReverifyConsistent,
		},
		{
			name:        "consistent without an evidence reference is downgraded",
			in:          ReverifyFinding{Verdict: ReverifyConsistent, FreshnessAt: now, Detail: "trust me"},
			wantVerdict: ReverifyUnknown,
			downgraded:  true,
		},
		{
			name:        "consistent with a blank evidence reference is downgraded",
			in:          ReverifyFinding{Verdict: ReverifyConsistent, EvidenceRef: "   ", FreshnessAt: now},
			wantVerdict: ReverifyUnknown,
			downgraded:  true,
		},
		{
			name:        "consistent without a freshness instant is downgraded",
			in:          ReverifyFinding{Verdict: ReverifyConsistent, EvidenceRef: "evidence-ref"},
			wantVerdict: ReverifyUnknown,
			downgraded:  true,
		},
		{
			name:        "divergent without evidence stays divergent (invalidation only, never a close)",
			in:          ReverifyFinding{Verdict: ReverifyDivergent, Trigger: InvalidationNewEvidence},
			wantVerdict: ReverifyDivergent,
		},
		{
			name:        "unknown stays unknown",
			in:          ReverifyFinding{Verdict: ReverifyUnknown, EvidenceRef: "evidence-ref"},
			wantVerdict: ReverifyUnknown,
		},
		{
			name:    "unrecognized verdict token is a wiring defect",
			in:      ReverifyFinding{Verdict: ReverifyVerdict("probably_fine"), EvidenceRef: "evidence-ref", FreshnessAt: now},
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := normalizeReverifyFinding(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("normalizeReverifyFinding(%+v) err = nil, want a wiring defect", tc.in)
				}
				return
			}
			if err != nil {
				t.Fatalf("normalizeReverifyFinding(%+v) err = %v", tc.in, err)
			}
			if got.Verdict != tc.wantVerdict {
				t.Fatalf("verdict = %q, want %q", got.Verdict, tc.wantVerdict)
			}
			if tc.downgraded {
				if !strings.Contains(got.Detail, "downgraded to unknown") {
					t.Fatalf("downgrade detail %q does not record the reason", got.Detail)
				}
				evidence := ReverifyEvidence{
					Verdict:     got.Verdict,
					EvidenceRef: got.EvidenceRef,
					FreshnessAt: got.FreshnessAt,
				}
				if evidence.CanClose(now.Add(time.Minute), tolerance) {
					t.Fatal("a downgraded consistent claim closed")
				}
			}
			if tc.wantVerdict == ReverifyConsistent {
				evidence := ReverifyEvidence{
					Verdict:     got.Verdict,
					EvidenceRef: got.EvidenceRef,
					FreshnessAt: got.FreshnessAt,
				}
				if !evidence.CanClose(now.Add(time.Minute), tolerance) {
					t.Fatal("a fully referenced fresh consistent finding did not close")
				}
			}
		})
	}
}

// TestReverifyInvalidationTriggerNeverWidensVocabulary pins that a divergent
// re-read maps onto the closed Q5 invalidation vocabulary: an unknown or
// unrelated trigger falls back to new_evidence, never to a wider action.
func TestReverifyInvalidationTriggerNeverWidensVocabulary(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   InvalidationTrigger
		want InvalidationTrigger
	}{
		{"reorg is preserved", InvalidationReorg, InvalidationReorg},
		{"source rotation is preserved", InvalidationSourceRotation, InvalidationSourceRotation},
		{"new evidence is preserved", InvalidationNewEvidence, InvalidationNewEvidence},
		{"unrelated write falls back to new evidence", InvalidationUnrelatedWrite, InvalidationNewEvidence},
		{"empty falls back to new evidence", "", InvalidationNewEvidence},
		{"unknown token falls back to new evidence", InvalidationTrigger("made_up"), InvalidationNewEvidence},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := reverifyInvalidationTrigger(tc.in); got != tc.want {
				t.Fatalf("reverifyInvalidationTrigger(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
