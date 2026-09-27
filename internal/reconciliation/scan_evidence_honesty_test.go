// scan_evidence_honesty_test.go is the T030 unit layer for the evidence-honesty
// pass of the scan compare loop (quickstart §3; tasks T030). It pins the parts
// of T030 the current contracts can decide:
//
//   - the missing PG business record (chain fact without its authoritative
//     row, US1-1) still mints its missing ticket;
//   - incomplete coverage (chain or event bundle not closed) never renders a
//     fully consistent classification, even when the visible parties agree;
//   - event-only absence (chain fact and PG business record both present, no
//     event row) still follows the existing fail-closed missing-ticket path.
//
// T030 BLOCKER (recorded, not resolved): T030's third item asks that
// "pre-cutover requests without event rows stay pending/gap and never mint
// event-missing tickets". The current contract cannot distinguish a
// pre-cutover request from a post-cutover request whose event row was really
// dropped: there is no cutover-instant carrier (event_system_state.cutover_at
// is not plumbed into the scan), no expected-event registry, and
// recon_task.policy_refs is a version snapshot only (data-model.md §1.1), not a
// cutover instant. Treating every event-only absence as insufficient evidence
// would be a policy the contract does not authorize and it breaks the existing
// acceptance case `receipted_withdrawal_without_event_row_mints_missing`
// (internal/app/reconcileadmin, fresh post-cutover request -> missing ticket).
// Pre-cutover eventless requests therefore stay on the old fail-closed path
// (tasks.md:83 item ③) until an independent design item provides the
// discriminator; T030's third item is NOT completed in this round.
//
// The tests are pure: they drive compareScanInterval with bounded fakes, no
// database and no Docker.
package reconciliation

import (
	"context"
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

// TestCompareScanIntervalEventOnlyAbsenceStillTickets pins the reverted T030
// behavior that the existing acceptance contract requires: an absent event row
// with the chain fact and the PG business record present is still a fail-closed
// `missing` classification (a ticket). T030's third item wants the pre-cutover
// variant of this shape to stay pending/gap, but the discriminator does not
// exist in the current contract (see the blocker note in the file header), so
// the old fail-closed path stays until an independent design item lands.
func TestCompareScanIntervalEventOnlyAbsenceStillTickets(t *testing.T) {
	fixedAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	sources := ScanSources{
		Chain:  honestyChainFacts{indexedAt: fixedAt},
		PG:     honestyPGState{status: PGStateComplete, at: fixedAt},
		Events: honestyEvents{capturedAt: fixedAt},
	}

	for _, tc := range []struct {
		name      string
		candidate honestyCandidates
	}{
		{"declared event aggregate identity", honestyCandidates{}},
		{"no event identity (chain-first tx_hash shape)", honestyCandidates{noEvent: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			outcome := honestyCompare(t, sources, tc.candidate)
			if len(outcome.classifications) != 1 {
				t.Fatalf("classifications = %d, want 1", len(outcome.classifications))
			}
			classification := outcome.classifications[0]
			if !classification.Ticket || classification.Category != CategoryMissing ||
				classification.Reason != ReasonMissing {
				t.Fatalf("event-only absence = %+v, want the fail-closed missing ticket", classification)
			}
			if outcome.pending != 0 || len(outcome.gapReasons) != 0 {
				t.Fatalf("pending/gaps = %d/%v, want 0/none for the fail-closed ticket",
					outcome.pending, outcome.gapReasons)
			}
		})
	}
}

// TestCompareScanIntervalMissingPGBusinessRecordStillTickets keeps the US1-1
// boundary: the event dimension never masks a confirmed chain fact without its
// authoritative PG business record.
func TestCompareScanIntervalMissingPGBusinessRecordStillTickets(t *testing.T) {
	fixedAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	sources := ScanSources{
		Chain:  honestyChainFacts{indexedAt: fixedAt},
		PG:     honestyPGState{status: PGStateAbsent, at: fixedAt},
		Events: honestyEvents{capturedAt: fixedAt},
	}
	outcome := honestyCompare(t, sources, honestyCandidates{})

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
