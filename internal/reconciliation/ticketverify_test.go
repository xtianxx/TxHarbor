// ticketverify_test.go holds the pure-function tests of the production
// pending_verify re-verification: detection-metadata persistence, business-type
// and interval derivation, scope containment, and the fail-closed outcome
// mapping (including the tx-aggregate completeness gate). The database-backed
// entry is exercised by the integration/binary acceptance layer.
package reconciliation

import (
	"strings"
	"testing"
	"time"
)

func TestPersistedEvidenceDomainJSONForRoundTrip(t *testing.T) {
	scope, err := HeightIdentityScope("31337", 2, 5, BusinessDeposit)
	if err != nil {
		t.Fatalf("HeightIdentityScope: %v", err)
	}
	version := VersionDomain{BlockNumber: 3, BlockHash: "0xabc", EvidenceAt: time.Now().UTC()}
	members := []TxAggregateMember{
		{BlockNumber: 3, BlockHash: "0xabc", TxHash: "0xtx", LogIndex: 1, Contract: "0xc", Topic0: "0xt"},
	}
	raw, err := PersistedEvidenceDomainJSONFor(version, &scope, BusinessDeposit,
		&DetectionInterval{Kind: ScopeHeight, From: 2, To: 5}, members)
	if err != nil {
		t.Fatalf("PersistedEvidenceDomainJSONFor: %v", err)
	}
	doc, err := ParsePersistedEvidenceDomain(raw)
	if err != nil {
		t.Fatalf("ParsePersistedEvidenceDomain: %v", err)
	}
	if doc.BusinessType != BusinessDeposit {
		t.Fatalf("business type = %q, want deposit", doc.BusinessType)
	}
	if doc.Detection == nil || doc.Detection.From != 2 || doc.Detection.To != 5 || doc.Detection.Kind != ScopeHeight {
		t.Fatalf("detection = %+v, want height 2..5", doc.Detection)
	}
	if len(doc.TxMembers) != 1 || doc.TxMembers[0].LogIndex != 1 {
		t.Fatalf("tx members = %+v, want the recorded member", doc.TxMembers)
	}

	// The legacy entry point still produces a parseable document without the
	// new metadata.
	legacy, err := PersistedEvidenceDomainJSON(version, &scope)
	if err != nil {
		t.Fatalf("PersistedEvidenceDomainJSON: %v", err)
	}
	legacyDoc, err := ParsePersistedEvidenceDomain(legacy)
	if err != nil {
		t.Fatalf("parse legacy domain: %v", err)
	}
	if legacyDoc.BusinessType != "" || legacyDoc.Detection != nil || len(legacyDoc.TxMembers) != 0 {
		t.Fatalf("legacy domain carried new metadata: %+v", legacyDoc)
	}

	// Unknown business types and malformed detection shapes are refused.
	if _, err := PersistedEvidenceDomainJSONFor(version, &scope, BusinessType("bogus"), nil, nil); err == nil {
		t.Fatalf("unknown business type was persisted")
	}
	if _, err := PersistedEvidenceDomainJSONFor(version, &scope, "", &DetectionInterval{Kind: ScopeHeight, From: 9, To: 2}, nil); err == nil {
		t.Fatalf("malformed detection interval was persisted")
	}
}

func TestTicketVerifyBusinessTypeDerivation(t *testing.T) {
	withScope := func(kind ...BusinessType) PersistedEvidenceDomain {
		scope := &IdentityScope{ChainID: "1", Kind: ScopeHeight, From: 1, To: 2, BusinessTypes: kind}
		return PersistedEvidenceDomain{Scope: scope}
	}
	cases := []struct {
		name   string
		domain PersistedEvidenceDomain
		key    BusinessKey
		want   BusinessType
		ok     bool
	}{
		{name: "persisted wins", domain: PersistedEvidenceDomain{BusinessType: BusinessDeposit},
			key: BusinessKey{Kind: BusinessKeyTxHash, Value: "0x1"}, want: BusinessDeposit, ok: true},
		{name: "persisted unknown refused", domain: PersistedEvidenceDomain{BusinessType: "bogus"},
			key: BusinessKey{Kind: BusinessKeyRequestID, Value: "r"}, ok: false},
		{name: "request is withdrawal", domain: PersistedEvidenceDomain{},
			key: BusinessKey{Kind: BusinessKeyRequestID, Value: "r"}, want: BusinessWithdrawal, ok: true},
		{name: "intent is withdrawal", domain: PersistedEvidenceDomain{},
			key: BusinessKey{Kind: BusinessKeyIntentID, Value: "i"}, want: BusinessWithdrawal, ok: true},
		{name: "tx hash single-type scope", domain: withScope(BusinessDeposit),
			key: BusinessKey{Kind: BusinessKeyTxHash, Value: "0x1"}, want: BusinessDeposit, ok: true},
		{name: "tx hash mixed scope unresolved", domain: withScope(BusinessDeposit, BusinessWithdrawal),
			key: BusinessKey{Kind: BusinessKeyTxHash, Value: "0x1"}, ok: false},
		{name: "tx hash no scope unresolved", domain: PersistedEvidenceDomain{},
			key: BusinessKey{Kind: BusinessKeyTxHash, Value: "0x1"}, ok: false},
		{name: "event id is event delivery", domain: PersistedEvidenceDomain{},
			key: BusinessKey{Kind: BusinessKeyEventID, Value: "e"}, want: BusinessEventDelivery, ok: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := ticketVerifyBusinessType(tc.domain, tc.key)
			if ok != tc.ok || got != tc.want {
				t.Fatalf("business type = %q ok=%t, want %q ok=%t", got, ok, tc.want, tc.ok)
			}
		})
	}
}

func TestTicketReverifyInterval(t *testing.T) {
	version := VersionDomain{BlockNumber: 7, BlockHash: "0x7", EvidenceAt: time.Now().UTC()}
	detection := &DetectionInterval{Kind: ScopeHeight, From: 2, To: 5}

	interval, source, ok := ticketReverifyInterval(version, detection)
	if !ok || source != "detection" || interval.From.Height != 2 || interval.To.Height != 5 {
		t.Fatalf("detection interval = %+v source=%s ok=%t", interval, source, ok)
	}
	interval, source, ok = ticketReverifyInterval(version, nil)
	if !ok || source != "block" || interval.From.Height != 7 || interval.To.Height != 7 {
		t.Fatalf("block interval = %+v source=%s ok=%t", interval, source, ok)
	}
	if _, _, ok := ticketReverifyInterval(VersionDomain{EvidenceAt: time.Now().UTC()}, nil); ok {
		t.Fatalf("a ticket without interval or block anchor resolved an interval")
	}
}

func TestScopeAndIntervalContainment(t *testing.T) {
	outer, err := HeightIdentityScope("1", 1, 10, BusinessDeposit, BusinessWithdrawal)
	if err != nil {
		t.Fatalf("outer scope: %v", err)
	}
	inner, err := HeightIdentityScope("1", 2, 5, BusinessDeposit)
	if err != nil {
		t.Fatalf("inner scope: %v", err)
	}
	if !identityScopeContains(outer, inner) {
		t.Fatalf("inner scope not contained")
	}
	otherChain, _ := HeightIdentityScope("2", 2, 5, BusinessDeposit)
	if identityScopeContains(outer, otherChain) {
		t.Fatalf("different chain considered contained")
	}
	outside, _ := HeightIdentityScope("1", 0, 5, BusinessDeposit)
	if identityScopeContains(outer, outside) {
		t.Fatalf("out-of-range scope considered contained")
	}
	extraType, _ := HeightIdentityScope("1", 2, 5, BusinessEventDelivery)
	if identityScopeContains(outer, extraType) {
		t.Fatalf("scope with a business type outside the task set considered contained")
	}

	if !scanIntervalContains(
		ScanInterval{From: HeightBound(1), To: HeightBound(10)},
		ScanInterval{From: HeightBound(2), To: HeightBound(5)}) {
		t.Fatalf("inner interval not contained")
	}
	if scanIntervalContains(
		ScanInterval{From: HeightBound(1), To: HeightBound(10)},
		ScanInterval{From: HeightBound(0), To: HeightBound(5)}) {
		t.Fatalf("out-of-range interval considered contained")
	}
}

func TestTicketVerifyOutcomeFailClosedMatrix(t *testing.T) {
	now := time.Now().UTC()
	version := VersionDomain{BlockNumber: 4, BlockHash: "0x4", EvidenceAt: now}
	item := pendingTicket{
		DiscrepancyID: "00000000-0000-0000-0000-000000000001",
		Category:      CategoryMissing,
		BusinessKey:   BusinessKey{Kind: BusinessKeyTxHash, Value: "0xtx"},
		BusinessType:  BusinessWithdrawal,
		Version:       version,
	}
	interval := ScanInterval{From: HeightBound(2), To: HeightBound(5)}

	classification := Classification{
		Category:     CategoryMissing,
		Ticket:       true,
		Conclusion:   ConclusionDivergent,
		Reason:       ReasonMissing,
		BusinessType: BusinessWithdrawal,
		EvidenceAt:   now,
		Version:      version,
	}
	if finding := ticketVerifyOutcome(item, classification, nil, interval, "detection"); finding.Verdict != ReverifyDivergent {
		t.Fatalf("remaining divergence verdict = %s, want divergent", finding.Verdict)
	}

	classification = Classification{
		Conclusion:   ConclusionPending,
		Reason:       ReasonFreshnessExpired,
		BusinessType: BusinessWithdrawal,
		EvidenceAt:   now,
		Version:      version,
	}
	if finding := ticketVerifyOutcome(item, classification, nil, interval, "detection"); finding.Verdict != ReverifyStale {
		t.Fatalf("expired classification verdict = %s, want stale", finding.Verdict)
	}

	classification = Classification{
		Conclusion:   ConclusionPending,
		Reason:       ReasonEventObligationUnproven,
		BusinessType: BusinessWithdrawal,
		EvidenceAt:   now,
		Version:      version,
	}
	if finding := ticketVerifyOutcome(item, classification, nil, interval, "detection"); finding.Verdict != ReverifyUnknown {
		t.Fatalf("pending classification verdict = %s, want unknown", finding.Verdict)
	}

	consistent := Classification{
		Conclusion:   ConclusionConsistent,
		Reason:       ReasonEventNotApplicable,
		BusinessType: BusinessWithdrawal,
		EvidenceAt:   now,
		Version:      version,
	}
	finding := ticketVerifyOutcome(item, consistent, &PGStateRecord{Status: PGStateComplete}, interval, "detection")
	if finding.Verdict != ReverifyConsistent {
		t.Fatalf("clean consistent verdict = %s (%s), want consistent", finding.Verdict, finding.Detail)
	}
	if finding.EvidenceRef == "" || finding.FreshnessAt.IsZero() || finding.Scope == "" || finding.Version == nil {
		t.Fatalf("consistent finding lacks evidence_ref/freshness/scope/version: %+v", finding)
	}

	// Recorded recovery rotation.
	rotationItem := item
	rotationItem.Version = version
	rotationItem.Version.RecoveryVersion = "rec-1/2"
	rotated := version
	rotated.RecoveryVersion = "rec-1/3"
	rotatedClassification := consistent
	rotatedClassification.Version = rotated
	if finding := ticketVerifyOutcome(rotationItem, rotatedClassification, nil, interval, "detection"); finding.Verdict != ReverifyDivergent {
		t.Fatalf("recovery rotation verdict = %s, want divergent", finding.Verdict)
	}
	unobservable := version
	unobservable.RecoveryVersion = ""
	unobservableClassification := consistent
	unobservableClassification.Version = unobservable
	if finding := ticketVerifyOutcome(rotationItem, unobservableClassification, nil, interval, "detection"); finding.Verdict != ReverifyUnknown {
		t.Fatalf("unobservable recorded recovery verdict = %s, want unknown", finding.Verdict)
	}

	// Block identity change.
	blockChanged := version
	blockChanged.BlockNumber = 9
	blockChanged.BlockHash = "0x9"
	blockChangedClassification := consistent
	blockChangedClassification.Version = blockChanged
	if finding := ticketVerifyOutcome(item, blockChangedClassification, nil, interval, "detection"); finding.Verdict != ReverifyDivergent {
		t.Fatalf("block identity change verdict = %s, want divergent", finding.Verdict)
	}

	// A consistent classification whose version domain is unusable must still
	// persist consistent but without an audit version pointer.
	noBlockItem := item
	noBlockItem.Version = VersionDomain{EvidenceAt: now}
	noSignal := Classification{
		Conclusion:   ConclusionConsistent,
		Reason:       ReasonEventNotApplicable,
		BusinessType: BusinessWithdrawal,
		EvidenceAt:   now,
	}
	if finding := ticketVerifyOutcome(noBlockItem, noSignal, nil, interval, "detection"); finding.Verdict != ReverifyConsistent || finding.Version != nil {
		t.Fatalf("no-signal version finding = %s version=%+v", finding.Verdict, finding.Version)
	}
}

func TestTicketAggregateGateDepositPartialRecording(t *testing.T) {
	now := time.Now().UTC()
	version := VersionDomain{BlockNumber: 4, BlockHash: "0x4", EvidenceAt: now}
	members := []TxAggregateMember{
		{BlockNumber: 4, BlockHash: "0x4", TxHash: "0xtx", LogIndex: 0, Contract: "0xc", Topic0: "0xt"},
		{BlockNumber: 4, BlockHash: "0x4", TxHash: "0xtx", LogIndex: 1, Contract: "0xc", Topic0: "0xt"},
	}
	item := pendingTicket{
		DiscrepancyID: "00000000-0000-0000-0000-000000000002",
		Category:      CategoryMissing,
		BusinessKey:   BusinessKey{Kind: BusinessKeyTxHash, Value: "0xtx"},
		BusinessType:  BusinessDeposit,
		Version:       version,
		TxMembers:     members,
	}
	classification := Classification{
		Conclusion:   ConclusionConsistent,
		Reason:       ReasonEventNotApplicable,
		BusinessType: BusinessDeposit,
		EvidenceAt:   now,
		Version:      version,
		Members:      members,
	}
	interval := ScanInterval{From: HeightBound(2), To: HeightBound(5)}

	// Only log 0 recorded: the aggregate is partially recorded -> divergent.
	partial := &PGStateRecord{Status: PGStateComplete, Deposits: []PGDepositObservation{
		{TxHash: "0xtx", LogIndex: 0},
	}}
	finding := ticketVerifyOutcome(item, classification, partial, interval, "detection")
	if finding.Verdict != ReverifyDivergent || !strings.Contains(finding.Detail, "partially recorded") {
		t.Fatalf("partial aggregate finding = %s (%s), want divergent partial", finding.Verdict, finding.Detail)
	}

	// Both logs recorded -> consistent.
	complete := &PGStateRecord{Status: PGStateComplete, Deposits: []PGDepositObservation{
		{TxHash: "0xtx", LogIndex: 0},
		{TxHash: "0xtx", LogIndex: 1},
	}}
	if finding := ticketVerifyOutcome(item, classification, complete, interval, "detection"); finding.Verdict != ReverifyConsistent {
		t.Fatalf("complete aggregate finding = %s (%s), want consistent", finding.Verdict, finding.Detail)
	}

	// Legacy ticket without a recorded member set: deposit completeness cannot
	// be proven -> unknown (never consistent, never divergent-by-guess).
	legacy := item
	legacy.TxMembers = nil
	if finding := ticketVerifyOutcome(legacy, classification, complete, interval, "detection"); finding.Verdict != ReverifyUnknown {
		t.Fatalf("legacy aggregate finding = %s, want unknown", finding.Verdict)
	}

	// A recorded member that vanished from the re-read chain evidence is a
	// chain change -> divergent.
	reduced := classification
	reduced.Members = members[:1]
	finding = ticketVerifyOutcome(item, reduced, complete, interval, "detection")
	if finding.Verdict != ReverifyDivergent || !strings.Contains(finding.Detail, "no longer present") {
		t.Fatalf("missing member finding = %s (%s), want divergent chain change", finding.Verdict, finding.Detail)
	}
}
