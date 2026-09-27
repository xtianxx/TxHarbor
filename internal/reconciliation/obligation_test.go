// obligation_test.go is the T040 unit layer for the expected-event
// discriminator: the pure R1/R2/R3 matrix, the candidate→catalog-aggregate
// mapping, the retention-trim reasoning and the bounded adapter's failure
// behavior. No database and no Docker: the adapter unit test only drives the
// configuration/validation and parsing helpers.
package reconciliation

import (
	"testing"
	"time"
)

func obligationAggregateForTest() EventObligationAggregate {
	return EventObligationAggregate{AggregateType: "withdrawal_request", AggregateID: "req-1"}
}

func obligationEvidence(expectations ...ObligatedExpectation) EventObligationEvidence {
	aggregate := obligationAggregateForTest()
	return EventObligationEvidence{
		CapturedAt:      time.Now().UTC(),
		ReadOK:          true,
		RetentionReadOK: true,
		Expectations: map[EventObligationAggregate][]ObligatedExpectation{
			aggregate: expectations,
		},
	}
}

// TestDiscriminateEventObligationMatrix pins the three-way verdict table.
func TestDiscriminateEventObligationMatrix(t *testing.T) {
	now := time.Now().UTC()
	aggregate := obligationAggregateForTest()

	t.Run("no catalog aggregate is R2 even with unreadable evidence", func(t *testing.T) {
		verdict := DiscriminateEventObligation(nil, EventObligationEvidence{})
		if verdict.State != EventObligationNotApplicable || verdict.Reason != ObligationReasonNoCatalogAggregate {
			t.Fatalf("verdict = %+v, want not_applicable/%s", verdict, ObligationReasonNoCatalogAggregate)
		}
	})

	t.Run("unreadable carrier is R3", func(t *testing.T) {
		verdict := DiscriminateEventObligation(&aggregate, EventObligationEvidence{ReadOK: false})
		if verdict.State != EventObligationUnproven || verdict.Reason != ObligationReasonEvidenceUnavailable {
			t.Fatalf("verdict = %+v, want unproven/%s", verdict, ObligationReasonEvidenceUnavailable)
		}
	})

	t.Run("truncated carrier is R3 and never missing", func(t *testing.T) {
		evidence := obligationEvidence(ObligatedExpectation{EventType: "withdrawal.request.received", ObligatedAt: now})
		evidence.Truncated = true
		verdict := DiscriminateEventObligation(&aggregate, evidence)
		if verdict.State != EventObligationUnproven || verdict.ProvesMissing() {
			t.Fatalf("verdict = %+v, want unproven (truncation never proves a loss)", verdict)
		}
	})

	t.Run("no marker is R3 and never N/A", func(t *testing.T) {
		verdict := DiscriminateEventObligation(&aggregate, EventObligationEvidence{
			ReadOK: true, RetentionReadOK: true,
			Expectations: map[EventObligationAggregate][]ObligatedExpectation{},
		})
		if verdict.State != EventObligationUnproven || verdict.Reason != ObligationReasonNoMarker {
			t.Fatalf("verdict = %+v, want unproven/%s (标记缺席 ≠ N/A)", verdict, ObligationReasonNoMarker)
		}
	})

	t.Run("proven marker inside every prune cutoff is R1", func(t *testing.T) {
		prunedAt := now.Add(-time.Hour)
		evidence := obligationEvidence(
			ObligatedExpectation{EventType: "withdrawal.request.received", ObligatedAt: now.Add(-10 * time.Minute)})
		evidence.RetentionAudits = []RetentionPruneAudit{{PrunedAt: prunedAt, Window: time.Hour}}
		verdict := DiscriminateEventObligation(&aggregate, evidence)
		if verdict.State != EventObligationProven || verdict.Reason != ObligationReasonMarker {
			t.Fatalf("verdict = %+v, want proven/%s", verdict, ObligationReasonMarker)
		}
	})

	t.Run("marker older than a prune cutoff is R3 (possibly trimmed)", func(t *testing.T) {
		prunedAt := now.Add(-time.Hour)
		old := prunedAt.Add(-2 * time.Hour)
		evidence := obligationEvidence(ObligatedExpectation{EventType: "withdrawal.request.received", ObligatedAt: old})
		evidence.RetentionAudits = []RetentionPruneAudit{{PrunedAt: prunedAt, Window: time.Hour}}
		verdict := DiscriminateEventObligation(&aggregate, evidence)
		if verdict.State != EventObligationUnproven || verdict.Reason != ObligationReasonPossiblyTrimmed {
			t.Fatalf("verdict = %+v, want unproven/%s", verdict, ObligationReasonPossiblyTrimmed)
		}
	})

	t.Run("one expectation outside every prune cutoff keeps R1", func(t *testing.T) {
		prunedAt := now.Add(-time.Hour)
		old := prunedAt.Add(-2 * time.Hour)
		recent := prunedAt.Add(time.Minute)
		evidence := obligationEvidence(
			ObligatedExpectation{EventType: "withdrawal.execution.state_changed", ObligatedAt: old},
			ObligatedExpectation{EventType: "withdrawal.execution.revised", ObligatedAt: recent})
		evidence.RetentionAudits = []RetentionPruneAudit{{PrunedAt: prunedAt, Window: time.Hour}}
		verdict := DiscriminateEventObligation(&aggregate, evidence)
		if verdict.State != EventObligationProven {
			t.Fatalf("verdict = %+v, want proven: a recent expectation cannot be explained by the audited trim", verdict)
		}
	})

	t.Run("unprovable retention history is R3", func(t *testing.T) {
		evidence := obligationEvidence(ObligatedExpectation{EventType: "withdrawal.request.received", ObligatedAt: now})
		evidence.RetentionReadOK = false
		verdict := DiscriminateEventObligation(&aggregate, evidence)
		if verdict.State != EventObligationUnproven || verdict.Reason != ObligationReasonRetentionUnknown {
			t.Fatalf("verdict = %+v, want unproven/%s", verdict, ObligationReasonRetentionUnknown)
		}
	})

	t.Run("unreadable obligation instant never proves a loss", func(t *testing.T) {
		evidence := obligationEvidence(ObligatedExpectation{EventType: "withdrawal.request.received"})
		evidence.RetentionAudits = []RetentionPruneAudit{{PrunedAt: now.Add(-time.Hour), Window: time.Hour}}
		verdict := DiscriminateEventObligation(&aggregate, evidence)
		if verdict.ProvesMissing() {
			t.Fatalf("verdict = %+v, want conservative non-missing", verdict)
		}
	})
}

// TestDiscriminateEventObligationOldEntityNewTransitionIsIndependentOfAge
// pins the 旧实体新转换 boundary rule at the verdict layer: an obligation is
// decided by the durable marker for the (aggregate, event type) pair only —
// never by the entity's age or its earlier history. A marked new transition of
// an old entity is R1; the same transition unmarked is R3 (history never
// proves the new obligation, and never disproves it either).
func TestDiscriminateEventObligationOldEntityNewTransitionIsIndependentOfAge(t *testing.T) {
	now := time.Now().UTC()
	aggregate := obligationAggregateForTest()

	marked := obligationEvidence(ObligatedExpectation{
		EventType:        "withdrawal.execution.state_changed",
		AggregateVersion: 9, // a new transition on an entity with eight older versions
		ObligatedAt:      now,
	})
	if verdict := DiscriminateEventObligation(&aggregate, marked); verdict.State != EventObligationProven {
		t.Fatalf("marked new transition = %+v, want proven (age-independent)", verdict)
	}

	unmarked := EventObligationEvidence{
		ReadOK: true, RetentionReadOK: true,
		Expectations: map[EventObligationAggregate][]ObligatedExpectation{},
	}
	if verdict := DiscriminateEventObligation(&aggregate, unmarked); verdict.State != EventObligationUnproven {
		t.Fatalf("unmarked new transition = %+v, want unproven", verdict)
	}
}

// TestEventObligationAggregateOf pins the candidate→aggregate mapping: only
// the frozen aggregate convention maps; every other shape is R2 evidence.
func TestEventObligationAggregateOf(t *testing.T) {
	for _, tc := range []struct {
		name string
		key  BusinessKey
		want bool
	}{
		{"deposit observation", BusinessKey{Kind: EventBusinessKeyAggregate, Value: "deposit_observation/5/0xbh/0xtx/0"}, true},
		{"withdrawal request", BusinessKey{Kind: EventBusinessKeyAggregate, Value: "withdrawal_request/req-1"}, true},
		{"withdrawal intent", BusinessKey{Kind: EventBusinessKeyAggregate, Value: "withdrawal_intent/int-1"}, true},
		{"tx_hash business key", BusinessKey{Kind: BusinessKeyTxHash, Value: "0xtx"}, false},
		{"request id business key", BusinessKey{Kind: BusinessKeyRequestID, Value: "req-1"}, false},
		{"unknown aggregate type", BusinessKey{Kind: EventBusinessKeyAggregate, Value: "payment_intent/int-1"}, false},
		{"missing id split", BusinessKey{Kind: EventBusinessKeyAggregate, Value: "withdrawal_request"}, false},
		{"empty id", BusinessKey{Kind: EventBusinessKeyAggregate, Value: "withdrawal_request/"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			aggregate, ok := EventObligationAggregateOf(tc.key)
			if ok != tc.want {
				t.Fatalf("ok = %v, want %v (aggregate %+v)", ok, tc.want, aggregate)
			}
			if ok && !aggregate.Valid() {
				t.Fatalf("mapped aggregate is not valid: %+v", aggregate)
			}
		})
	}
}

// TestEventObligationConfigValidate pins the fail-closed bounds.
func TestEventObligationConfigValidate(t *testing.T) {
	if err := (EventObligationConfig{MaxExpectations: 1, MaxRetentionAudits: 1}).Validate(); err != nil {
		t.Fatalf("valid config refused: %v", err)
	}
	for _, cfg := range []EventObligationConfig{
		{MaxRetentionAudits: 1},
		{MaxExpectations: 1},
		{MaxExpectations: -1, MaxRetentionAudits: 1},
		{MaxExpectations: 1, MaxRetentionAudits: -1},
	} {
		if err := cfg.Validate(); err == nil {
			t.Fatalf("config %+v accepted, want a refusal", cfg)
		}
	}
	if _, err := NewEventObligationAdapter(nil, EventObligationConfig{MaxExpectations: 1, MaxRetentionAudits: 1}); err == nil {
		t.Fatal("nil pool accepted, want a refusal")
	}
}

// TestParseRetentionScope pins the audit-scope decoding: only the recorded
// positive duration is usable; anything else keeps the history unprovable.
func TestParseRetentionScope(t *testing.T) {
	if window, ok := parseRetentionScope([]byte(`{"retention":"168h0m0s"}`)); !ok || window != 168*time.Hour {
		t.Fatalf("parse = %v/%v, want 168h/true", window, ok)
	}
	for _, raw := range []string{
		``, `not-json`, `{}`, `{"retention":168}`, `{"retention":"0s"}`,
		`{"retention":"-1h"}`, `{"retention":"bogus"}`,
	} {
		if _, ok := parseRetentionScope([]byte(raw)); ok {
			t.Fatalf("scope %q accepted, want an unprovable result", raw)
		}
	}
}

// TestEventNotApplicableSnapshotIsStable pins the additive R2 marker: it is
// non-empty and content-stable (it is never mixed into the absence or
// unavailable markers, so no existing identity content hash changes).
func TestEventNotApplicableSnapshotIsStable(t *testing.T) {
	key := BusinessKey{Kind: EventBusinessKeyAggregate, Value: "withdrawal_request/req-1"}
	first := EventNotApplicableSnapshot(key)
	second := EventNotApplicableSnapshot(key)
	if len(first) == 0 || string(first) != string(second) {
		t.Fatalf("N/A snapshot is empty or unstable: %q vs %q", first, second)
	}
	absent := EventAbsenceSnapshot(key)
	if string(first) == string(absent) {
		t.Fatal("N/A marker collides with the absence marker")
	}
	if string(first) == string(eventUnavailableSnapshot(key)) {
		t.Fatal("N/A marker collides with the unavailable marker")
	}
}
