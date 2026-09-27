// classify_test.go covers the T017 machine classifier decision table:
// business-divergence-only tickets (Q4), absorbed-duplicate metrics,
// insufficient-evidence conservatism, and the upstream receipt boundary
// (FR-006/009). Pure unit tests: no Docker, no build tags.
package reconciliation

import (
	"testing"
	"time"
)

var classifyNow = time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)

func classifyScope(t *testing.T) IdentityScope {
	t.Helper()
	scope, err := HeightIdentityScope("eth-mainnet", 100, 200, BusinessWithdrawal)
	if err != nil {
		t.Fatalf("build scope: %v", err)
	}
	return scope
}

func classifyVersion() VersionDomain {
	return VersionDomain{
		BlockNumber:  150,
		BlockHash:    "0xblockhash",
		StateVersion: 3,
		EvidenceAt:   classifyNow.Add(-time.Minute),
	}
}

func classifyCoverage() Coverage {
	return Coverage{
		ScanComplete:       true,
		EvidenceAt:         classifyNow.Add(-time.Minute),
		Now:                classifyNow,
		FreshnessTolerance: time.Hour,
	}
}

func classifyUpstream() UpstreamReceiptSource {
	return UpstreamReceiptSource{
		Source:    "upstream-ledger",
		Connected: true,
		Available: true,
		Receipt:   &UpstreamReceipt{Ref: "receipt-1", Success: true},
	}
}

func presentParty(content string) PartyObservation {
	return PartyObservation{Status: PartyPresent, Content: []byte(content)}
}

func absentParty() PartyObservation {
	return PartyObservation{Status: PartyAbsent, Content: []byte(`{"status":"absent"}`)}
}

func baseObservation(t *testing.T) Observation {
	t.Helper()
	return Observation{
		Scope:        classifyScope(t),
		BusinessKey:  BusinessKey{Kind: BusinessKeyRequestID, Value: "req-1"},
		BusinessType: BusinessWithdrawal,
		Chain:        presentParty(`{"chain":"confirmed"}`),
		PG:           presentParty(`{"pg":"confirmed"}`),
		Event:        presentParty(`{"event":"delivered"}`),
		Coverage:     classifyCoverage(),
		Upstream:     classifyUpstream(),
		Version:      classifyVersion(),
		EvidenceRef:  "evidence://scan/1",
	}
}

func TestClassifyThreeWayMatch(t *testing.T) {
	cls := Classify(baseObservation(t))

	if cls.Category != "" || cls.Ticket {
		t.Fatalf("clean match must not classify a discrepancy: category=%q ticket=%v", cls.Category, cls.Ticket)
	}
	if cls.Conclusion != ConclusionConsistent {
		t.Fatalf("conclusion = %q, want %q", cls.Conclusion, ConclusionConsistent)
	}
	if cls.ExternalCredit != ExternalCreditVerified {
		t.Fatalf("external credit = %q, want %q", cls.ExternalCredit, ExternalCreditVerified)
	}
	if !cls.FullyConsistent() {
		t.Fatal("connected+receipt+clean match must be fully consistent")
	}
	if cls.Reason != ReasonThreeWayMatch {
		t.Fatalf("reason = %q, want %q", cls.Reason, ReasonThreeWayMatch)
	}
	if cls.HasDiscrepancy() || cls.MetricsOnly() {
		t.Fatal("clean match must be neither a discrepancy nor a metrics-only duplicate")
	}
	if cls.EvidenceRef != "evidence://scan/1" {
		t.Fatalf("evidence ref not preserved: %q", cls.EvidenceRef)
	}
}

func TestClassifyMissingTicketsStableIdentity(t *testing.T) {
	obs := baseObservation(t)
	obs.PG = absentParty()

	cls := Classify(obs)
	if cls.Category != CategoryMissing || !cls.Ticket {
		t.Fatalf("category=%q ticket=%v, want missing ticket", cls.Category, cls.Ticket)
	}
	if cls.Conclusion != ConclusionDivergent {
		t.Fatalf("conclusion = %q, want %q", cls.Conclusion, ConclusionDivergent)
	}
	if !cls.Identity.Valid() || cls.Identity.Category() != CategoryMissing {
		t.Fatalf("missing ticket must carry a valid identity: %v", cls.Identity)
	}
	if cls.Reason != ReasonMissing {
		t.Fatalf("reason = %q, want %q", cls.Reason, ReasonMissing)
	}

	// Same fact twice -> same stable identity (dedup, no second ticket).
	again := Classify(obs)
	if !again.Identity.SameIdentity(cls.Identity) {
		t.Fatal("repeated detection must dedup onto the same identity")
	}

	// A changed evidence version domain is a different identity (reverify
	// boundary, Q5), not a duplicate of the old one.
	changed := obs
	changed.Version.StateVersion = 4
	other := Classify(changed)
	if !other.Ticket || other.Identity.SameIdentity(cls.Identity) {
		t.Fatal("changed version domain must mint a distinct identity")
	}
}

func TestClassifyMissingWithoutAbsenceMarkerIsIncomplete(t *testing.T) {
	obs := baseObservation(t)
	obs.PG = PartyObservation{Status: PartyAbsent} // no canonical absence bytes

	cls := Classify(obs)
	if cls.Ticket || cls.Category != CategoryIncomplete {
		t.Fatalf("weak evidence must downgrade to incomplete without a ticket: category=%q ticket=%v", cls.Category, cls.Ticket)
	}
	if cls.Conclusion != ConclusionPending || cls.Reason != ReasonEvidenceMissing {
		t.Fatalf("conclusion=%q reason=%q, want pending/evidence_missing", cls.Conclusion, cls.Reason)
	}
}

func TestClassifyStateMismatch(t *testing.T) {
	obs := baseObservation(t)
	obs.Mismatch = true
	cls := Classify(obs)
	if cls.Category != CategoryStateMismatch || !cls.Ticket || cls.Reason != ReasonStateMismatch {
		t.Fatalf("mismatch classification = %+v", cls)
	}

	// A present business fact without its chain fact is a mismatch too.
	obs = baseObservation(t)
	obs.Chain = absentParty()
	cls = Classify(obs)
	if cls.Category != CategoryStateMismatch || !cls.Ticket {
		t.Fatalf("fact-without-chain classification = %+v", cls)
	}
}

func TestClassifyAbsorbedDuplicateIsMetricsOnly(t *testing.T) {
	obs := baseObservation(t)
	obs.Duplicates = DuplicateEvidence{
		Deliveries:            2,
		ContentChecked:        true,
		IdempotencyRecorded:   true,
		EffectEvidencePresent: true,
		EffectCount:           1,
	}

	cls := Classify(obs)
	if cls.Ticket || cls.HasDiscrepancy() {
		t.Fatalf("absorbed duplicate must not create a ticket: %+v", cls)
	}
	if cls.Duplicate != DuplicateAbsorbed || !cls.MetricsOnly() {
		t.Fatalf("duplicate outcome = %q metricsOnly=%v, want absorbed", cls.Duplicate, cls.MetricsOnly())
	}
	if cls.Conclusion != ConclusionConsistent || cls.Reason != ReasonDuplicateAbsorbed {
		t.Fatalf("conclusion=%q reason=%q, want consistent/duplicate_absorbed", cls.Conclusion, cls.Reason)
	}
}

func TestClassifyAbsorbedLegalOldVersion(t *testing.T) {
	obs := baseObservation(t)
	obs.Duplicates = DuplicateEvidence{
		Deliveries:                  3,
		ContentChecked:              true,
		VersionGuardIgnoredLegalOld: true,
	}

	cls := Classify(obs)
	if cls.Ticket || cls.Duplicate != DuplicateAbsorbed || !cls.MetricsOnly() {
		t.Fatalf("legal old version must be absorbed: %+v", cls)
	}
}

func TestClassifyDivergentDuplicatesTicket(t *testing.T) {
	cases := []struct {
		name string
		dup  DuplicateEvidence
	}{
		{"same identity different content", DuplicateEvidence{Deliveries: 2, ContentChecked: true, ContentDivergent: true}},
		{"repeated business effect", DuplicateEvidence{Deliveries: 2, ContentChecked: true, RepeatedBusinessEffect: true}},
		{"repeated withdrawal intent", DuplicateEvidence{Deliveries: 2, ContentChecked: true, RepeatedWithdrawalIntent: true}},
		{"version rule violation", DuplicateEvidence{Deliveries: 2, ContentChecked: true, VersionRuleViolated: true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			obs := baseObservation(t)
			obs.Duplicates = tc.dup
			cls := Classify(obs)
			if cls.Category != CategoryDuplicateDivergent || !cls.Ticket {
				t.Fatalf("category=%q ticket=%v, want duplicate_divergent ticket", cls.Category, cls.Ticket)
			}
			if cls.Identity.Category() != CategoryDuplicateDivergent {
				t.Fatalf("identity category = %q", cls.Identity.Category())
			}
			if cls.Conclusion != ConclusionDivergent || cls.MetricsOnly() {
				t.Fatalf("conclusion=%q metricsOnly=%v", cls.Conclusion, cls.MetricsOnly())
			}
		})
	}
}

func TestClassifyDuplicateCountAloneIsNotFundAnomaly(t *testing.T) {
	obs := baseObservation(t)
	obs.Duplicates = DuplicateEvidence{Deliveries: 5}

	cls := Classify(obs)
	if cls.Ticket {
		t.Fatal("a delivery count alone must never create a fund ticket")
	}
	if cls.Duplicate != DuplicateUnproven || cls.Category != CategoryIncomplete {
		t.Fatalf("duplicate=%q category=%q, want unproven/incomplete", cls.Duplicate, cls.Category)
	}
	if cls.Conclusion != ConclusionPending || cls.Reason != ReasonDuplicateUnproven {
		t.Fatalf("conclusion=%q reason=%q, want pending/duplicate_unproven", cls.Conclusion, cls.Reason)
	}
}

func TestClassifyUnknownResult(t *testing.T) {
	obs := baseObservation(t)
	obs.Chain = PartyObservation{Status: PartyUnknown}

	cls := Classify(obs)
	if cls.Category != CategoryUnknown || cls.Ticket {
		t.Fatalf("unknown result must stay alert-only: %+v", cls)
	}
	if cls.Conclusion != ConclusionPending || cls.Reason != ReasonUnknownResult {
		t.Fatalf("conclusion=%q reason=%q", cls.Conclusion, cls.Reason)
	}
}

func TestClassifyEvidenceGates(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Observation)
		reason ClassificationReason
	}{
		{"scan not complete", func(o *Observation) { o.Coverage.ScanComplete = false }, ReasonScanIncomplete},
		{"open gap", func(o *Observation) { o.Coverage.OpenGaps = 1 }, ReasonScanIncomplete},
		{"retention trimmed", func(o *Observation) { o.Coverage.RetentionTrimmed = true }, ReasonEvidenceTrimmed},
		{"stale evidence", func(o *Observation) { o.Coverage.EvidenceAt = classifyNow.Add(-2 * time.Hour) }, ReasonFreshnessExpired},
		{"freshness unconfigured", func(o *Observation) { o.Coverage.FreshnessTolerance = 0 }, ReasonFreshnessUnproven},
		{"orphaned chain evidence", func(o *Observation) { o.Chain.Orphaned = true }, ReasonEvidenceOrphaned},
		{"pg unreadable", func(o *Observation) { o.PG = PartyObservation{Status: PartyUnknown} }, ReasonEvidenceMissing},
		{"event unreadable", func(o *Observation) { o.Event = PartyObservation{Status: PartyUnknown} }, ReasonEvidenceMissing},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			obs := baseObservation(t)
			tc.mutate(&obs)
			cls := Classify(obs)
			if cls.Ticket || cls.Category != CategoryIncomplete {
				t.Fatalf("gate must classify incomplete without a ticket: %+v", cls)
			}
			if cls.Conclusion != ConclusionPending || cls.Reason != tc.reason {
				t.Fatalf("conclusion=%q reason=%q, want pending/%q", cls.Conclusion, cls.Reason, tc.reason)
			}
		})
	}
}

func TestClassifyUnconnectedNeverConsistent(t *testing.T) {
	obs := baseObservation(t)
	obs.Upstream = UpstreamReceiptSource{Source: "upstream-ledger", Connected: false}

	cls := Classify(obs)
	if cls.Ticket || cls.Category != CategoryIncomplete {
		t.Fatalf("unconnected scope must stay incomplete/alert-only: %+v", cls)
	}
	if cls.Conclusion == ConclusionConsistent || cls.FullyConsistent() {
		t.Fatal("unconnected scope must never render consistent")
	}
	if cls.ExternalCredit != ExternalCreditUnverified || cls.Reason != ReasonUpstreamUnconnected {
		t.Fatalf("external=%q reason=%q", cls.ExternalCredit, cls.Reason)
	}
}

func TestClassifyUnconnectedKeepsInProjectDivergence(t *testing.T) {
	obs := baseObservation(t)
	obs.PG = absentParty()
	obs.Upstream = UpstreamReceiptSource{Source: "upstream-ledger", Connected: false}

	cls := Classify(obs)
	if cls.Category != CategoryMissing || !cls.Ticket {
		t.Fatalf("in-project divergence must still ticket: %+v", cls)
	}
	if cls.ExternalCredit != ExternalCreditUnverified {
		t.Fatalf("external credit must stay unverified: %q", cls.ExternalCredit)
	}
}

func TestClassifyConnectedFlagAloneDoesNotProveUpstream(t *testing.T) {
	obs := baseObservation(t)
	obs.Upstream = UpstreamReceiptSource{Source: "upstream-ledger", Connected: true, Available: true}

	cls := Classify(obs)
	if cls.Ticket || cls.Category != "" {
		t.Fatalf("connected-without-receipt is not a discrepancy: %+v", cls)
	}
	if cls.ExternalCredit != ExternalCreditUnverified {
		t.Fatalf("external credit = %q, want unverified without a receipt", cls.ExternalCredit)
	}
	if cls.FullyConsistent() {
		t.Fatal("a connected flag alone must never prove upstream success")
	}
}

func TestClassifyConfiguredButUnavailable(t *testing.T) {
	obs := baseObservation(t)
	obs.Upstream = UpstreamReceiptSource{Source: "upstream-ledger", Connected: true, Available: false}

	cls := Classify(obs)
	if cls.Category != CategoryIncomplete || cls.Conclusion != ConclusionPending {
		t.Fatalf("configured-but-unavailable must stay incomplete/pending: %+v", cls)
	}
	if cls.ExternalCredit != ExternalCreditUnavailable || cls.Reason != ReasonUpstreamUnavailable {
		t.Fatalf("external=%q reason=%q", cls.ExternalCredit, cls.Reason)
	}
}

func TestClassifyUpstreamReceiptFailure(t *testing.T) {
	obs := baseObservation(t)
	obs.Upstream.Receipt = &UpstreamReceipt{Ref: "receipt-2", Success: false}

	cls := Classify(obs)
	if cls.Category != CategoryStateMismatch || !cls.Ticket {
		t.Fatalf("evidenced upstream failure is a state mismatch: %+v", cls)
	}
	if cls.ExternalCredit != ExternalCreditFailed || cls.Reason != ReasonUpstreamReceiptFailed {
		t.Fatalf("external=%q reason=%q", cls.ExternalCredit, cls.Reason)
	}
}

func TestClassifyAbsorbedDuplicateUnconnectedKeepsMetrics(t *testing.T) {
	obs := baseObservation(t)
	obs.Duplicates = DuplicateEvidence{
		Deliveries:            2,
		ContentChecked:        true,
		IdempotencyRecorded:   true,
		EffectEvidencePresent: true,
		EffectCount:           1,
	}
	obs.Upstream = UpstreamReceiptSource{Source: "upstream-ledger", Connected: false}

	cls := Classify(obs)
	if cls.Ticket {
		t.Fatal("unconnected absorbed duplicate must not ticket")
	}
	if !cls.MetricsOnly() || cls.Duplicate != DuplicateAbsorbed {
		t.Fatalf("in-project duplicate metrics must be preserved: %+v", cls)
	}
	if cls.Conclusion == ConclusionConsistent {
		t.Fatal("unconnected scope must never render consistent")
	}
}

func TestClassifyNotApplicableEventIsNotMissing(t *testing.T) {
	obs := baseObservation(t)
	obs.Event = PartyObservation{Status: PartyNotApplicable}

	cls := Classify(obs)
	if cls.Ticket || cls.Category != "" || cls.Conclusion != ConclusionConsistent {
		t.Fatalf("not-applicable party must be excluded, not treated as absent: %+v", cls)
	}
}

func TestClassifyUnknownShapeIsConservative(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Observation)
	}{
		{"unknown party status", func(o *Observation) { o.Chain.Status = "sideways" }},
		{"unknown business type", func(o *Observation) { o.BusinessType = "mystery" }},
		{"empty business key", func(o *Observation) { o.BusinessKey.Value = "" }},
		{"negative duplicate count", func(o *Observation) { o.Duplicates.Deliveries = -1 }},
		{"duplicate without event party", func(o *Observation) {
			o.Event = absentParty()
			o.Duplicates = DuplicateEvidence{Deliveries: 2}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			obs := baseObservation(t)
			tc.mutate(&obs)
			cls := Classify(obs)
			if cls.Ticket || cls.Category != CategoryIncomplete {
				t.Fatalf("unknown shape must fall into the conservative default: %+v", cls)
			}
			if cls.Conclusion != ConclusionPending || cls.Reason != ReasonUnknownShape {
				t.Fatalf("conclusion=%q reason=%q", cls.Conclusion, cls.Reason)
			}
		})
	}
}

func TestClassifyZeroObservationIsConservative(t *testing.T) {
	cls := Classify(Observation{})
	if cls.Ticket || cls.Category != CategoryIncomplete || cls.Reason != ReasonUnknownShape {
		t.Fatalf("zero observation must be conservatively incomplete: %+v", cls)
	}
}
