// eventstate_test.go pins the pure, DB-free rules of the T015 event-delivery
// adapter: the Q4 absorbed-duplicate boundary, the hard-contradiction
// divergence, the unknown-evidence conservatism, canonical snapshot stability,
// absence-marker shaping, and the bounded query/predicate builders.
package reconciliation

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/xtianxx/txharbor/internal/events"
)

// eventStateFixture builds one published business-object event observation and
// its durable version watermark.
func eventStateFixture(now time.Time) (EventDeliveryObservation, EventVersionWatermark) {
	observation := EventDeliveryObservation{
		EventID:          uuid.New(),
		IdentityKind:     events.IdentityKindBusinessObject,
		EventType:        events.EventTypeWithdrawalRequestReceived,
		AggregateType:    "withdrawal_intent",
		AggregateID:      "intent-1",
		AggregateVersion: 3,
		PayloadHash:      "hash-3",
		OccurredAt:       now.Add(-2 * time.Minute),
		EmittedAt:        now.Add(-2 * time.Minute),
		SourceKind:       "withdrawal_intent",
		SourceID:         "intent-1",
		SourceVersion:    int64Pointer(3),
		PublishState:     events.PublishStatePublished,
		Duplicate:        EventDuplicateNone,
		PendingReason:    "consumer_result_pending",
	}
	watermark := EventVersionWatermark{
		ConsumerName:  "txharbor.consumer.v1",
		AggregateType: "withdrawal_intent",
		AggregateID:   "intent-1",
		MaxVersion:    5,
		UpdatedAt:     now.Add(-time.Minute),
	}
	return observation, watermark
}

//go:fix inline
func int64Pointer(v int64) *int64 { return new(v) }

// TestEventStateAbsorbedDuplicateIsMetricsOnly pins Q4 end to end across T015
// and T017: the version guard ignoring a legal old version is absorbed
// evidence, produces no ticket, and keeps the classification metrics-only.
func TestEventStateAbsorbedDuplicateIsMetricsOnly(t *testing.T) {
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	observation, watermark := eventStateFixture(now)
	// Two deliveries share one source watermark (the redelivery shape).
	observation.SharedSourceDeliveries = 2

	observations := []EventDeliveryObservation{observation}
	evaluateEventDeliveries(observations, []string{watermark.ConsumerName},
		map[uuid.UUID][]EventApplication{},
		[]EventVersionWatermark{watermark}, nil,
		map[string]bool{watermark.ConsumerName: true},
		eventReadFacts{applicationsReadable: true, watermarksReadable: true}, now, time.Minute)

	got := observations[0]
	if got.Duplicate != EventDuplicateAbsorbed {
		t.Fatalf("duplicate verdict = %q, want %q (legal version-guard absorption)", got.Duplicate, EventDuplicateAbsorbed)
	}
	if !got.Terminal {
		t.Fatalf("absorbed event terminal = false, want true")
	}
	if got.PendingReason != "" {
		t.Errorf("pending reason = %q, want empty for a terminal delivery", got.PendingReason)
	}

	duplicateEvidence := got.DuplicateEvidence()
	if !duplicateEvidence.VersionGuardIgnoredLegalOld || !duplicateEvidence.ContentChecked {
		t.Fatalf("duplicate evidence = %+v, want VersionGuardIgnoredLegalOld and ContentChecked", duplicateEvidence)
	}
	if duplicateEvidence.Divergent() {
		t.Fatalf("absorbed duplicate reported as divergent")
	}
	if duplicateEvidence.Deliveries != 2 {
		t.Errorf("Deliveries = %d, want 2 (shared source watermark)", duplicateEvidence.Deliveries)
	}

	scope, err := HeightIdentityScope("1", 100, 110, BusinessWithdrawal)
	if err != nil {
		t.Fatalf("HeightIdentityScope: %v", err)
	}
	classification := Classify(Observation{
		Scope:        scope,
		BusinessKey:  got.EventBusinessKey(),
		BusinessType: BusinessWithdrawal,
		Chain:        PartyObservation{Status: PartyPresent, Content: []byte(`{"chain":"present"}`)},
		PG:           PartyObservation{Status: PartyPresent, Content: []byte(`{"pg":"present"}`)},
		Event:        got.PartyObservation(),
		Duplicates:   duplicateEvidence,
		Coverage: Coverage{
			ScanComplete:       true,
			EvidenceAt:         now,
			Now:                now,
			FreshnessTolerance: time.Hour,
		},
		Upstream: UpstreamReceiptSource{
			Source: "upstream.ledger", Connected: true, Available: true,
			Receipt: &UpstreamReceipt{Ref: "receipt-1", Success: true},
		},
		Version:     VersionDomain{BlockNumber: 100, BlockHash: "0xabc", EvidenceAt: now},
		EvidenceRef: got.EvidenceRef(),
	})
	if classification.Ticket || classification.HasDiscrepancy() {
		t.Fatalf("classification = %+v, want no ticket for an absorbed duplicate (Q4)", classification)
	}
	if !classification.MetricsOnly() {
		t.Fatalf("classification.MetricsOnly() = false, want true for absorbed duplicate")
	}
	if classification.Conclusion != ConclusionConsistent {
		t.Fatalf("conclusion = %q, want %q", classification.Conclusion, ConclusionConsistent)
	}
}

// TestEventStateContentContradictionIsDivergent pins the hard contradiction:
// an inbox record that disagrees with the emitted event is definite divergence,
// not a count-based suspicion.
func TestEventStateContentContradictionIsDivergent(t *testing.T) {
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	observation, watermark := eventStateFixture(now)
	observation.SharedSourceDeliveries = 2
	application := EventApplication{
		ConsumerName:     watermark.ConsumerName,
		EventID:          observation.EventID,
		AggregateType:    "withdrawal_intent",
		AggregateID:      "intent-1",
		AggregateVersion: observation.AggregateVersion + 1, // contradicts the emission
		Topic:            "txharbor.events.v1",
		Partition:        0,
		Offset:           41,
		AppliedAt:        now.Add(-time.Minute),
	}

	observations := []EventDeliveryObservation{observation}
	evaluateEventDeliveries(observations, []string{watermark.ConsumerName},
		map[uuid.UUID][]EventApplication{observation.EventID: {application}},
		[]EventVersionWatermark{watermark}, nil,
		map[string]bool{watermark.ConsumerName: true},
		eventReadFacts{applicationsReadable: true, watermarksReadable: true}, now, time.Minute)

	got := observations[0]
	if got.Duplicate != EventDuplicateDivergent {
		t.Fatalf("duplicate verdict = %q, want %q", got.Duplicate, EventDuplicateDivergent)
	}
	if !got.Terminal {
		t.Fatalf("contradiction terminal = false, want true")
	}
	duplicateEvidence := got.DuplicateEvidence()
	if !duplicateEvidence.ContentDivergent || !duplicateEvidence.Divergent() {
		t.Fatalf("duplicate evidence = %+v, want ContentDivergent", duplicateEvidence)
	}
}

// TestEventStateUnreadableEvidenceStaysUnknown pins FR-004: an unreadable
// consumer-result source yields unknown relations, an unproven duplicate and a
// non-terminal delivery — never absorption and never a divergence claim.
func TestEventStateUnreadableEvidenceStaysUnknown(t *testing.T) {
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	observation, watermark := eventStateFixture(now)
	observation.SharedSourceDeliveries = 2

	observations := []EventDeliveryObservation{observation}
	evaluateEventDeliveries(observations, []string{watermark.ConsumerName}, nil, nil, nil,
		map[string]bool{watermark.ConsumerName: true},
		eventReadFacts{applicationsReadable: false, watermarksReadable: false}, now, time.Minute)

	got := observations[0]
	if got.Duplicate != EventDuplicateUnverifiable {
		t.Fatalf("duplicate verdict = %q, want %q", got.Duplicate, EventDuplicateUnverifiable)
	}
	if got.Terminal {
		t.Fatalf("unreadable evidence terminal = true, want false (pending)")
	}
	if evidence := got.DuplicateEvidence(); evidence.VersionGuardIgnoredLegalOld || evidence.Divergent() {
		t.Fatalf("unreadable evidence produced absorption/divergence: %+v", evidence)
	}
}

// TestEventStatePublishedWithoutConsumersStaysUnclosed pins the conservative
// default: a published event with no registered consumer is pending, never
// consistent.
func TestEventStatePublishedWithoutConsumersStaysUnclosed(t *testing.T) {
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	observation, _ := eventStateFixture(now)

	observations := []EventDeliveryObservation{observation}
	evaluateEventDeliveries(observations, nil, nil, nil, nil, nil,
		eventReadFacts{applicationsReadable: true, watermarksReadable: true}, now, time.Minute)

	got := observations[0]
	if got.Terminal {
		t.Fatalf("event with no registered consumer terminal = true, want false")
	}
	if got.PendingReason != "no_consumer_registered" {
		t.Fatalf("pending reason = %q, want no_consumer_registered", got.PendingReason)
	}
	if len(got.Consumers) != 0 {
		t.Fatalf("relations = %d, want 0 with no registered consumers", len(got.Consumers))
	}
}

// TestEventStateVersionGapStaysPending pins FR-004/018 conservatism for a
// version jump: the gap is unconfirmed and observed, never classified.
func TestEventStateVersionGapStaysPending(t *testing.T) {
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	observation, watermark := eventStateFixture(now)
	observation.AggregateVersion = watermark.MaxVersion + 2
	watermark.MaxVersion = 3

	observations := []EventDeliveryObservation{observation}
	evaluateEventDeliveries(observations, []string{watermark.ConsumerName},
		map[uuid.UUID][]EventApplication{},
		[]EventVersionWatermark{watermark}, nil,
		map[string]bool{watermark.ConsumerName: true},
		eventReadFacts{applicationsReadable: true, watermarksReadable: true}, now, time.Minute)

	got := observations[0]
	if got.Terminal {
		t.Fatalf("version gap terminal = true, want false")
	}
	if got.PendingReason != "version_gap_unconfirmed" {
		t.Fatalf("pending reason = %q, want version_gap_unconfirmed", got.PendingReason)
	}
	if got.Consumers[0].Relation != EventVersionGap {
		t.Fatalf("relation = %q, want %q", got.Consumers[0].Relation, EventVersionGap)
	}
}

// TestEventStateBlockedPublishIsTerminal pins that a permanently blocked
// publish is a definite recorded outcome (alert-worthy), not an in-flight
// delivery.
func TestEventStateBlockedPublishIsTerminal(t *testing.T) {
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	observation, watermark := eventStateFixture(now)
	observation.PublishState = events.PublishStateBlocked
	observation.LastErrorClass = "permanent"

	observations := []EventDeliveryObservation{observation}
	evaluateEventDeliveries(observations, []string{watermark.ConsumerName},
		map[uuid.UUID][]EventApplication{},
		[]EventVersionWatermark{watermark}, nil,
		map[string]bool{watermark.ConsumerName: true},
		eventReadFacts{applicationsReadable: true, watermarksReadable: true}, now, time.Minute)

	got := observations[0]
	if !got.Terminal {
		t.Fatalf("blocked publish terminal = false, want true")
	}
	if got.Duplicate != EventDuplicateNone {
		t.Fatalf("blocked publish duplicate verdict = %q, want none", got.Duplicate)
	}
}

// TestEventStateCanonicalBytesAreOrderStable pins the evidence-stability rule:
// re-observing the same facts in a different row order yields identical
// canonical bytes (so the identity hash is stable).
func TestEventStateCanonicalBytesAreOrderStable(t *testing.T) {
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	first, watermark := eventStateFixture(now)
	second := first
	second.EventID = uuid.New()
	second.AggregateID = "intent-2"
	second.PayloadHash = "hash-4"
	second.AggregateVersion = 4

	scope, err := HeightIdentityScope("1", 100, 110, BusinessWithdrawal)
	if err != nil {
		t.Fatalf("HeightIdentityScope: %v", err)
	}
	interval := EventStateInterval{From: HeightBound(100), To: HeightBound(110)}
	left := EventStateEvidence{Scope: scope, Interval: interval,
		Observations: []EventDeliveryObservation{first, second}}
	right := EventStateEvidence{Scope: scope, Interval: interval,
		Observations: []EventDeliveryObservation{second, first}}

	if !bytes.Equal(left.CanonicalBytes(), right.CanonicalBytes()) {
		t.Fatalf("canonical bytes differ across observation order")
	}
	if bytes.Equal(first.CanonicalBytes(), second.CanonicalBytes()) {
		t.Fatalf("distinct events produced identical per-event canonical bytes")
	}
	_ = watermark
}

// TestEventAbsenceSnapshot pins the definitive-absence marker: non-empty (so a
// snapshot hash is possible) and distinct per business key.
func TestEventAbsenceSnapshot(t *testing.T) {
	first := EventAbsenceSnapshot(BusinessKey{Kind: BusinessKeyEventID, Value: "event-a"})
	second := EventAbsenceSnapshot(BusinessKey{Kind: BusinessKeyEventID, Value: "event-b"})
	if len(first) == 0 || len(second) == 0 {
		t.Fatalf("absence snapshot must be a non-empty marker")
	}
	if bytes.Equal(first, second) {
		t.Fatalf("absence snapshots of distinct keys must differ")
	}
	if !bytes.Equal(first, EventAbsenceSnapshot(BusinessKey{Kind: BusinessKeyEventID, Value: "event-a"})) {
		t.Fatalf("absence snapshot is not deterministic")
	}
}

// TestEventStateQueryValidation pins the fail-closed query shape checks.
func TestEventStateQueryValidation(t *testing.T) {
	scope, err := HeightIdentityScope("1", 1, 5, BusinessWithdrawal)
	if err != nil {
		t.Fatalf("HeightIdentityScope: %v", err)
	}
	progress := fakeProgressReader{}

	t.Run("interval kind must match scope kind", func(t *testing.T) {
		from := TimeBound(time.Date(2026, 9, 26, 0, 0, 0, 0, time.UTC))
		to := TimeBound(time.Date(2026, 9, 26, 1, 0, 0, 0, time.UTC))
		q := EventStateQuery{Scope: scope, Interval: EventStateInterval{From: from, To: to}}
		if err := q.Validate(); err == nil {
			t.Fatalf("kind mismatch accepted")
		}
	})

	t.Run("consumers require a quarantine reader", func(t *testing.T) {
		q := EventStateQuery{
			Scope:     scope,
			Interval:  EventStateInterval{From: HeightBound(1), To: HeightBound(5)},
			Consumers: []EventConsumerRegistration{{Name: "c1", Progress: progress}},
		}
		if err := q.Validate(); err == nil {
			t.Fatalf("missing quarantine reader accepted")
		}
	})

	t.Run("duplicate receipt declarations are refused", func(t *testing.T) {
		q := EventStateQuery{
			Scope:    scope,
			Interval: EventStateInterval{From: HeightBound(1), To: HeightBound(5)},
			UpstreamReceipts: []ChainUpstreamReceiptSource{
				{BusinessType: BusinessWithdrawal, Source: "a", Connected: true},
				{BusinessType: BusinessWithdrawal, Source: "b", Connected: true},
			},
		}
		if err := q.Validate(); err == nil {
			t.Fatalf("duplicate receipt declaration accepted")
		}
	})
}

// TestEventStateBusinessPredicate pins the closed business-type → event-type
// namespace mapping.
func TestEventStateBusinessPredicate(t *testing.T) {
	args := &eventStateArgs{}
	predicate, ok := eventStateBusinessPredicate([]BusinessType{BusinessWithdrawal, BusinessDeposit}, args)
	if !ok {
		t.Fatalf("withdrawal/deposit scope produced no predicate")
	}
	want := "(event_type LIKE $1 OR event_type LIKE $2)"
	if predicate != want {
		t.Fatalf("predicate = %q, want %q", predicate, want)
	}
	if len(args.args) != 2 {
		t.Fatalf("args = %d, want 2", len(args.args))
	}
	if _, ok := eventStateBusinessPredicate([]BusinessType{BusinessEventDelivery}, &eventStateArgs{}); ok {
		t.Fatalf("event-delivery scope must cover every event type (no predicate)")
	}
}

// TestEventStateWatermarkPairs pins the bounded watermark chunk SQL shape.
func TestEventStateWatermarkPairs(t *testing.T) {
	if got := eventStateWatermarkPairs(2); got != "($1,$2),($3,$4)" {
		t.Fatalf("pairs = %q", got)
	}
}

// TestEventStateEvidencePendingReverify pins the conservative status mapping.
func TestEventStateEvidencePendingReverify(t *testing.T) {
	cases := []struct {
		status   EventDeliveryStatus
		verdict  ReverifyVerdict
		expected bool
	}{
		{EventDeliveryComplete, "", false},
		{EventDeliveryStale, ReverifyStale, true},
		{EventDeliveryIncomplete, ReverifyUnknown, true},
		{EventDeliveryUnknown, ReverifyUnknown, true},
	}
	for _, tc := range cases {
		evidence := &EventStateEvidence{Status: tc.status}
		verdict, ok := evidence.PendingReverify()
		if ok != tc.expected || verdict != tc.verdict {
			t.Errorf("status %q: PendingReverify = (%q,%v), want (%q,%v)", tc.status, verdict, ok, tc.verdict, tc.expected)
		}
	}
	if (&EventStateEvidence{Status: EventDeliveryComplete}).CanSupportConsistent() != true {
		t.Errorf("complete evidence must support consistency")
	}
	if (&EventStateEvidence{Status: EventDeliveryIncomplete}).CanSupportConsistent() != false {
		t.Errorf("incomplete evidence must not support consistency")
	}
}

// fakeProgressReader is a DB-free EventProgressReader.
type fakeProgressReader struct{}

func (fakeProgressReader) ReadProgress(context.Context) ([]events.Progress, error) { return nil, nil }

func (fakeProgressReader) ResumeOffset(context.Context, string, int) (int64, bool, error) {
	return 0, false, nil
}
