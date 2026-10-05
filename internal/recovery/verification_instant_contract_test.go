//go:build contract

// verification_instant_contract_test.go pins the T038 evaluation-instant
// ordering: the read-only sources are consulted first and the evaluation
// instant is captured after their reads. This is the deterministic regression
// behind the occasional verify_approve_status V9 "future timestamp" failure:
// a live adapter stamps its observation with its own read instant, and the
// old capture-before-reads order could leave that instant ahead of the
// evaluation instant whenever the read crossed a clock second, making the
// evaluator's strict no-future rule (verification.go EvaluateConclusion)
// refuse an observation that plainly happened. No Docker, no database.
package recovery

import (
	"context"
	"testing"
	"time"
)

// t038InstantSource is a controllable read-only source that behaves like a
// live adapter: every Observe advances the shared wall clock by the declared
// read duration and stamps its observation with its own read instant (UTC).
type t038InstantSource struct {
	category VerificationCategory
	clock    *time.Time
	advance  time.Duration
	// future offsets the observation's declared timestamp beyond its own
	// read instant (a source whose clock runs ahead of the evaluation).
	future   time.Duration
	observed time.Time
}

// Category implements VerificationSource.
func (s *t038InstantSource) Category() VerificationCategory { return s.category }

// Observe implements VerificationSource.
func (s *t038InstantSource) Observe(context.Context) ([]SourceObservation, error) {
	*s.clock = (*s.clock).Add(s.advance)
	s.observed = *s.clock
	return []SourceObservation{{
		ObjectKey:  "v9/dependency?name=contract",
		Conclusion: ConclusionConsistent,
		ObservedAt: s.observed.Add(s.future),
	}}, nil
}

// TestT038EvaluationInstantFollowsSourceReads is the deterministic regression
// for the live-read race. The fake clock starts at .600s and the source read
// advances it across the second boundary to 1.000s, exactly like a read that
// straddles a second. The observation carries its own read instant. Capturing
// the evaluation instant before the reads judged it future (unknown: the
// verify_approve_status V9 flake); capturing it after the reads keeps the
// observation inside its own evaluation instant (consistent).
func TestT038EvaluationInstantFollowsSourceReads(t *testing.T) {
	clock := time.Date(2026, 9, 29, 12, 0, 0, 600_000_000, time.UTC)
	src := &t038InstantSource{
		category: VerificationV9,
		clock:    &clock,
		advance:  400 * time.Millisecond,
	}
	v := &Verification{
		batchLimit: 10,
		tolerance:  time.Minute,
		now:        func() time.Time { return clock },
	}

	prepared, err := v.collect(context.Background(), "inst-contract", VerificationRequest{
		Sources: []VerificationSource{src},
	}, nil)
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	if len(prepared) != 1 {
		t.Fatalf("collect returned %d item(s), want 1", len(prepared))
	}
	item := prepared[0].item
	if item.Conclusion != ConclusionConsistent {
		t.Fatalf("conclusion = %s (%s), want consistent: the evaluation instant must follow the source read",
			item.Conclusion, item.Reason)
	}
	if !item.ObservedAt.Equal(src.observed) {
		t.Fatalf("item observed_at = %s, want the source read instant %s", item.ObservedAt, src.observed)
	}
}

// TestT038EvaluationInstantStillRefusesFutureObservation is the negative
// control: the ordering fix does not relax the strict no-future rule. An
// observation whose declared timestamp runs ahead of the post-read evaluation
// instant still concludes unknown (never fresh), so a source with a fast
// clock or a forged timestamp is still refused (FR-018).
func TestT038EvaluationInstantStillRefusesFutureObservation(t *testing.T) {
	clock := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	src := &t038InstantSource{
		category: VerificationV4,
		clock:    &clock,
		future:   2 * time.Second,
	}
	v := &Verification{
		batchLimit: 10,
		tolerance:  time.Minute,
		now:        func() time.Time { return clock },
	}

	prepared, err := v.collect(context.Background(), "inst-contract", VerificationRequest{
		Sources: []VerificationSource{src},
	}, nil)
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	if len(prepared) != 1 {
		t.Fatalf("collect returned %d item(s), want 1", len(prepared))
	}
	item := prepared[0].item
	if item.Conclusion == ConclusionConsistent || item.Conclusion == ConclusionStale {
		t.Fatalf("conclusion = %s, want a non-pass verdict for a future observation", item.Conclusion)
	}
}
