// scan_evidence_honesty_test.go is the T030/T040 unit layer for the
// evidence-honesty pass of the scan compare loop (quickstart §3; tasks T030,
// T040). It pins:
//
//   - the missing PG business record (chain fact without its authoritative
//     row, US1-1) still mints its missing ticket, even when a durable
//     expectation marker exists: the event discriminator never masks a
//     chain/PG divergence (R2/祖父规则);
//   - incomplete coverage (chain or event bundle not closed) never renders a
//     fully consistent classification, even when the visible parties agree;
//   - the T040 expected-event discriminator on the decisive event-only
//     absence (chain fact and PG business record both present, event absent):
//     a durable expectation marker keeps the fail-closed missing ticket (R1);
//     a candidate with no catalog event aggregate is N/A and can never mint an
//     event-missing ticket (R2); a marker-less / unreadable / possibly
//     trimmed expectation stays pending/gap and alert-only (R3). 标记缺席 ≠
//     N/A, and missing evidence is never a missing claim.
//
// The tests are pure: they drive compareScanInterval with bounded fakes, no
// database and no Docker.
package reconciliation

import (
	"context"
	"errors"
	"testing"
	"time"
)

// honestyChainFacts is a complete height-scoped chain bundle whose blocks can
// satisfy one candidate reference.
type honestyChainFacts struct {
	indexedAt time.Time
	status    ChainFactsStatus
	reasons   []string
}

// Observe returns one canonical block per requested height.
func (f honestyChainFacts) Observe(_ context.Context, q ChainFactsQuery) (ChainFactsBundle, error) {
	bundle := ChainFactsBundle{
		ChainID: q.ChainID, From: q.From, To: q.To,
		Source: ChainSourceLocalIndex, CapturedAt: f.indexedAt,
		Status: f.status, Reasons: append([]string(nil), f.reasons...),
	}
	if bundle.Status == "" {
		bundle.Status = ChainFactsComplete
	}
	for height := q.From; height <= q.To; height++ {
		bundle.Blocks = append(bundle.Blocks, ChainFactBlock{
			Number: height, Hash: honestyBlockHash, Canonical: true, IndexedAt: f.indexedAt,
		})
	}
	return bundle, nil
}

// honestyPGState answers every read with the configured status.
type honestyPGState struct {
	status PGStateStatus
	at     time.Time
}

// Read returns the configured PG-state record.
func (f honestyPGState) Read(_ context.Context, req PGReadRequest) (PGStateRecord, error) {
	return PGStateRecord{
		BusinessType: req.BusinessType, BusinessKey: req.Key, ChainID: req.ChainID,
		Status: f.status, ReadAt: f.at,
	}, nil
}

// honestyEvents returns an event bundle with the configured status and
// observations; an empty observation list with a complete status means
// "covered and empty" (which a pre-cutover request legitimately looks like,
// and which a dropped row looks like too — the two are not distinguishable
// with today's contract, see the blocker note above).
type honestyEvents struct {
	capturedAt   time.Time
	status       EventDeliveryStatus
	reasons      []string
	observations []EventDeliveryObservation
}

// Observe returns the configured bundle.
func (f honestyEvents) Observe(_ context.Context, q EventStateQuery) (EventStateEvidence, error) {
	status := f.status
	if status == "" {
		status = EventDeliveryComplete
	}
	return EventStateEvidence{
		Scope: q.Scope, Interval: q.Interval, CapturedAt: f.capturedAt,
		Status: status, Reasons: append([]string(nil), f.reasons...),
		Observations: f.observations,
	}, nil
}

// honestyCandidates returns one withdrawal candidate whose event identity
// either declares the object's aggregate (the PG-anchored shape) or carries no
// event identity at all (the chain-first tx_hash shape).
type honestyCandidates struct {
	mismatch bool
	noEvent  bool
}

// ScanCandidates implements the legacy enumeration seam.
func (f honestyCandidates) ScanCandidates(_ context.Context, interval ScanInterval) ([]ScanCandidate, error) {
	candidate := ScanCandidate{
		BusinessType: BusinessWithdrawal,
		BusinessKey:  BusinessKey{Kind: BusinessKeyRequestID, Value: "eventless-request"},
		ChainFact: ChainFactRef{
			BlockNumber: interval.From.Height,
			BlockHash:   honestyBlockHash,
		},
		Mismatch: f.mismatch,
	}
	if !f.noEvent {
		candidate.EventKey = BusinessKey{
			Kind:  EventBusinessKeyAggregate,
			Value: "withdrawal_request/eventless-request",
		}
	}
	return []ScanCandidate{candidate}, nil
}

// honestyBlockHash is the canonical block hash the fakes and candidates share.
const honestyBlockHash = "0xhonestyblock"

// honestyTask builds one running, withdrawal-scoped height task with a
// connected upstream declaration (so pending evidence maps onto query_failed).
func honestyTask() *Task {
	return &Task{
		TaskID: "11111111-1111-1111-1111-111111111111", ScopeChainID: "7", ScopeKind: ScopeHeight,
		ScopeStart: HeightBound(100), ScopeEnd: HeightBound(103),
		State: TaskStateRunning, BusinessTypes: []BusinessType{BusinessWithdrawal},
		UpstreamReceipts: []ChainUpstreamReceiptSource{{
			BusinessType: BusinessWithdrawal, Source: "test-ledger", Connected: true,
		}},
	}
}

// honestyCompare runs one compareScanInterval over the honesty fixture.
func honestyCompare(t *testing.T, sources ScanSources, candidates ScanCandidateEnumerator) *scanIntervalOutcome {
	t.Helper()
	task := honestyTask()
	scope, err := scanTaskIdentityScope(task)
	if err != nil {
		t.Fatalf("scanTaskIdentityScope: %v", err)
	}
	req := &ScanOnceRequest{
		TaskID: task.TaskID, Owner: "test:honesty", LeaseTTL: time.Minute,
		Limits: BudgetLimits{
			MaxConcurrency: 1, MaxSpanPerClaim: 4, MaxDuration: time.Minute,
			MaxPGRequests: 50, MaxRPCRequests: 4,
		},
		FreshnessTolerance: time.Hour,
		Sources:            sources,
		Candidates:         candidates,
	}
	budget, err := NewBudget(req.Limits)
	if err != nil {
		t.Fatalf("NewBudget: %v", err)
	}
	outcome, err := compareScanInterval(context.Background(), req, task, scope,
		ScanInterval{From: HeightBound(100), To: HeightBound(103)}, budget)
	if err != nil {
		t.Fatalf("compareScanInterval: %v", err)
	}
	return outcome
}

// honestyObligations is a bounded expectation-carrier fake: it returns the
// configured evidence (or error) regardless of the requested aggregates.
type honestyObligations struct {
	evidence EventObligationEvidence
	err      error
}

// ReadObligations implements EventObligationReader.
func (f honestyObligations) ReadObligations(_ context.Context, q EventObligationQuery) (EventObligationEvidence, error) {
	if f.err != nil {
		return EventObligationEvidence{CapturedAt: time.Now().UTC()}, f.err
	}
	return f.evidence, nil
}

// honestyAggregate is the catalog event aggregate of the honesty candidate.
func honestyAggregate() EventObligationAggregate {
	return EventObligationAggregate{AggregateType: "withdrawal_request", AggregateID: "eventless-request"}
}

// honestyMarkerEvidence is a readable carrier holding one durable expectation
// marker for the honesty candidate.
func honestyMarkerEvidence(obligatedAt time.Time) EventObligationEvidence {
	aggregate := honestyAggregate()
	return EventObligationEvidence{
		CapturedAt:      time.Now().UTC(),
		ReadOK:          true,
		RetentionReadOK: true,
		Expectations: map[EventObligationAggregate][]ObligatedExpectation{
			aggregate: {{
				EventType:        "withdrawal.request.received",
				AggregateVersion: 1,
				ObligatedAt:      obligatedAt,
				SourceKind:       "withdrawal_intake",
				SourceID:         "eventless-request",
			}},
		},
	}
}

// TestCompareScanIntervalEventObligationDiscriminator pins the T040 three-way
// matrix on the decisive event-only absence.
func TestCompareScanIntervalEventObligationDiscriminator(t *testing.T) {
	fixedAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	baseSources := func() ScanSources {
		return ScanSources{
			Chain:  honestyChainFacts{indexedAt: fixedAt},
			PG:     honestyPGState{status: PGStateComplete, at: fixedAt},
			Events: honestyEvents{capturedAt: fixedAt},
		}
	}

	t.Run("R1 durable marker keeps the fail-closed missing ticket", func(t *testing.T) {
		sources := baseSources()
		sources.Obligations = honestyObligations{evidence: honestyMarkerEvidence(fixedAt)}
		outcome := honestyCompare(t, sources, honestyCandidates{})

		if len(outcome.classifications) != 1 {
			t.Fatalf("classifications = %d, want 1", len(outcome.classifications))
		}
		classification := outcome.classifications[0]
		if !classification.Ticket || classification.Category != CategoryMissing ||
			classification.Reason != ReasonMissing {
			t.Fatalf("proven obligation = %+v, want the fail-closed missing ticket", classification)
		}
		if outcome.pending != 0 || len(outcome.gapReasons) != 0 {
			t.Fatalf("pending/gaps = %d/%v, want 0/none for a proven missing", outcome.pending, outcome.gapReasons)
		}
	})

	t.Run("R3 no marker stays pending and observable", func(t *testing.T) {
		sources := baseSources()
		sources.Obligations = honestyObligations{evidence: EventObligationEvidence{
			CapturedAt: time.Now().UTC(), ReadOK: true, RetentionReadOK: true,
			Expectations: map[EventObligationAggregate][]ObligatedExpectation{},
		}}
		outcome := honestyCompare(t, sources, honestyCandidates{})

		classification := outcome.classifications[0]
		if classification.Ticket || classification.Conclusion != ConclusionPending ||
			classification.Reason != ReasonEventObligationUnproven {
			t.Fatalf("marker-less absence = %+v, want pending/%s", classification, ReasonEventObligationUnproven)
		}
		if outcome.pending != 1 || len(outcome.gapReasons) == 0 {
			t.Fatalf("pending/gaps = %d/%v, want 1 and a visible gap", outcome.pending, outcome.gapReasons)
		}
	})

	t.Run("R3 unwired obligation reader stays pending", func(t *testing.T) {
		outcome := honestyCompare(t, baseSources(), honestyCandidates{})
		classification := outcome.classifications[0]
		if classification.Ticket || classification.Conclusion != ConclusionPending ||
			classification.Reason != ReasonEventObligationUnproven {
			t.Fatalf("unwired reader = %+v, want pending/%s", classification, ReasonEventObligationUnproven)
		}
		if outcome.pending != 1 || len(outcome.gapReasons) == 0 {
			t.Fatalf("pending/gaps = %d/%v, want 1 and a visible gap", outcome.pending, outcome.gapReasons)
		}
	})

	t.Run("R3 failed carrier read stays pending", func(t *testing.T) {
		sources := baseSources()
		sources.Obligations = honestyObligations{err: errObligationRead}
		outcome := honestyCompare(t, sources, honestyCandidates{})
		classification := outcome.classifications[0]
		if classification.Ticket || classification.Conclusion != ConclusionPending ||
			classification.Reason != ReasonEventObligationUnproven {
			t.Fatalf("failed carrier read = %+v, want pending/%s", classification, ReasonEventObligationUnproven)
		}
		if outcome.pending != 1 || len(outcome.gapReasons) == 0 {
			t.Fatalf("pending/gaps = %d/%v, want 1 and a visible gap", outcome.pending, outcome.gapReasons)
		}
	})

	t.Run("R3 audited legal trim keeps the absence unprovable", func(t *testing.T) {
		// The marker predates the audited prune cutoff: a published row of
		// this obligation could legally have been deleted by retention, so
		// the absence is never a missing claim.
		prunedAt := time.Now().UTC()
		sources := baseSources()
		evidence := honestyMarkerEvidence(prunedAt.Add(-48 * time.Hour))
		evidence.RetentionAudits = []RetentionPruneAudit{{PrunedAt: prunedAt, Window: 24 * time.Hour}}
		sources.Obligations = honestyObligations{evidence: evidence}
		outcome := honestyCompare(t, sources, honestyCandidates{})

		classification := outcome.classifications[0]
		if classification.Ticket || classification.Conclusion != ConclusionPending ||
			classification.Reason != ReasonEventObligationUnproven {
			t.Fatalf("possibly trimmed = %+v, want pending/%s", classification, ReasonEventObligationUnproven)
		}
	})

	t.Run("R2 no catalog event aggregate is N/A and mints nothing", func(t *testing.T) {
		outcome := honestyCompare(t, baseSources(), honestyCandidates{noEvent: true})
		classification := outcome.classifications[0]
		if classification.Ticket || classification.Conclusion != ConclusionConsistent ||
			classification.Reason != ReasonEventNotApplicable {
			t.Fatalf("no catalog aggregate = %+v, want consistent/%s with no ticket", classification, ReasonEventNotApplicable)
		}
		if classification.FullyConsistent() {
			t.Fatal("R2 rendered fully consistent; chain/PG agreement is not a three-party claim")
		}
		if outcome.pending != 0 || len(outcome.gapReasons) != 0 {
			t.Fatalf("pending/gaps = %d/%v, want 0/none for a proven N/A", outcome.pending, outcome.gapReasons)
		}
	})
}

// errObligationRead simulates an expectation-carrier read failure.
var errObligationRead = errors.New("obligation carrier read failed")

// TestCompareScanIntervalMissingPGBusinessRecordStillTickets keeps the US1-1
// boundary: the event dimension never masks a confirmed chain fact without its
// authoritative PG business record — neither with a proven obligation marker
// nor with a missing one.
func TestCompareScanIntervalMissingPGBusinessRecordStillTickets(t *testing.T) {
	fixedAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	for _, tc := range []struct {
		name    string
		sources func() ScanSources
	}{
		{"no obligation evidence", func() ScanSources {
			return ScanSources{
				Chain:  honestyChainFacts{indexedAt: fixedAt},
				PG:     honestyPGState{status: PGStateAbsent, at: fixedAt},
				Events: honestyEvents{capturedAt: fixedAt},
			}
		}},
		{"proven obligation marker", func() ScanSources {
			return ScanSources{
				Chain:       honestyChainFacts{indexedAt: fixedAt},
				PG:          honestyPGState{status: PGStateAbsent, at: fixedAt},
				Events:      honestyEvents{capturedAt: fixedAt},
				Obligations: honestyObligations{evidence: honestyMarkerEvidence(fixedAt)},
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			outcome := honestyCompare(t, tc.sources(), honestyCandidates{})
			if len(outcome.classifications) != 1 {
				t.Fatalf("classifications = %d, want 1", len(outcome.classifications))
			}
			classification := outcome.classifications[0]
			if !classification.Ticket || classification.Category != CategoryMissing ||
				classification.Reason != ReasonMissing {
				t.Fatalf("missing PG record = %+v, want a missing ticket", classification)
			}
			if outcome.pending != 0 || len(outcome.gapReasons) != 0 {
				t.Fatalf("pending/gaps = %d/%v, want 0/none for a confirmed missing ticket",
					outcome.pending, outcome.gapReasons)
			}
		})
	}
}

// TestCompareScanIntervalIncompleteCoverageNeverConsistent pins quickstart §3:
// a bundle whose coverage is not closed (even when the visible parties agree)
// can never render a fully consistent classification.
func TestCompareScanIntervalIncompleteCoverageNeverConsistent(t *testing.T) {
	fixedAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	cases := []struct {
		name    string
		sources ScanSources
	}{
		{"incomplete event coverage", ScanSources{
			Chain: honestyChainFacts{indexedAt: fixedAt},
			PG:    honestyPGState{status: PGStateComplete, at: fixedAt},
			Events: honestyEvents{capturedAt: fixedAt, status: EventDeliveryIncomplete,
				reasons: []string{EventReasonScopeTruncated}},
		}},
		{"incomplete chain coverage", ScanSources{
			Chain: honestyChainFacts{indexedAt: fixedAt, status: ChainFactsIncomplete,
				reasons: []string{"range_not_indexed"}},
			PG:     honestyPGState{status: PGStateComplete, at: fixedAt},
			Events: honestyEvents{capturedAt: fixedAt},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			outcome := honestyCompare(t, tc.sources, honestyCandidates{})
			if len(outcome.classifications) != 1 {
				t.Fatalf("classifications = %d, want 1", len(outcome.classifications))
			}
			classification := outcome.classifications[0]
			if classification.Ticket || classification.Conclusion != ConclusionPending {
				t.Fatalf("incomplete coverage = %+v, want pending without a ticket", classification)
			}
			if classification.FullyConsistent() {
				t.Fatal("incomplete coverage rendered fully consistent")
			}
			if classification.ExternalCredit == ExternalCreditVerified {
				t.Fatal("incomplete coverage claimed verified upstream credit")
			}
			if outcome.pending != 1 || len(outcome.gapReasons) == 0 {
				t.Fatalf("pending/gaps = %d/%v, want 1 and a visible gap",
					outcome.pending, outcome.gapReasons)
			}
		})
	}
}
