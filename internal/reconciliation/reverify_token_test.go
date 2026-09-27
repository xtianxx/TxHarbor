// reverify_token_test.go pins the pure part of the T026/T027 write-write
// ordering protocol: the captured validity token comparison and the discard
// reason vocabulary (data-model.md §3 "复核有效性令牌";
// contracts/discrepancy-lifecycle.md "Revalidation Token Protocol").
package reconciliation

import (
	"strings"
	"testing"
	"time"
)

func TestReverifyTokenMatchesAllComponents(t *testing.T) {
	domain := []byte(`{"scope":{"chain_id":"1","kind":"height","from":2,"to":5},"evidence_at":"2026-01-01T00:00:00Z"}`)
	token := reverifyTokenFor(DiscrepancyStatePendingVerify, 7, domain)

	if !token.matches(DiscrepancyStatePendingVerify, 7, domain) {
		t.Fatal("identical target did not match its own token")
	}
	if token.matches(DiscrepancyStateClosed, 7, domain) {
		t.Fatal("state change must invalidate the token")
	}
	if token.matches(DiscrepancyStatePendingVerify, 8, domain) {
		t.Fatal("generation advance must invalidate the token")
	}
	if token.matches(DiscrepancyStatePendingVerify, 7, []byte(`{"scope":{"chain_id":"1","kind":"height","from":2,"to":6}}`)) {
		t.Fatal("evidence version change must invalidate the token")
	}
	if !token.evidenceMatches(domain) {
		t.Fatal("evidenceMatches must accept the captured domain")
	}
	if token.evidenceMatches(nil) {
		t.Fatal("evidenceMatches must reject an empty domain")
	}
}

// TestReverifyTokenIgnoresTimestampsAndVerdicts pins the explicit rule: the
// protection is the captured token, never a created_at/process clock or a
// verdict type. Two tokens captured with different wall-clock instants but
// the same persisted row are identical; a consistent-vs-consistent pair is
// governed by the same generation rule as any other verdict pair.
func TestReverifyTokenIgnoresTimestampsAndVerdicts(t *testing.T) {
	domain := []byte(`{"scope":{"chain_id":"1","kind":"height","from":2,"to":5}}`)
	first := reverifyTokenFor(DiscrepancyStatePendingVerify, 3, domain)
	second := reverifyTokenFor(DiscrepancyStatePendingVerify, 3, domain)
	_ = time.Now() // the token carries no time component at all
	if first != second {
		t.Fatal("tokens of the same persisted row differ")
	}
	if !first.matches(DiscrepancyStatePendingVerify, 3, domain) {
		t.Fatal("a same-row re-capture must match")
	}
}

func TestReverifyDiscardReasonNamesEveryMismatch(t *testing.T) {
	domain := []byte(`{"scope":{"chain_id":"1","kind":"height","from":2,"to":5}}`)
	item := reverifyItem{
		DiscrepancyID: "00000000-0000-0000-0000-000000000001",
		EvidenceAt:    time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		Token:         reverifyTokenFor(DiscrepancyStateClosed, 4, domain),
	}

	stateAndGeneration := reverifyDiscardReason(item, "history", lockedReverifyTarget{
		State: DiscrepancyStatePendingVerify, Generation: 6, Domain: domain,
	})
	for _, want := range []string{"state changed", "closed -> pending_verify", "generation advanced", "4 -> 6"} {
		if !strings.Contains(stateAndGeneration, want) {
			t.Fatalf("reason %q does not name %q", stateAndGeneration, want)
		}
	}

	evidenceOnly := reverifyDiscardReason(item, "history", lockedReverifyTarget{
		State: DiscrepancyStateClosed, Generation: 4, Domain: []byte(`{"scope":{"chain_id":"1","kind":"height","from":2,"to":6}}`),
	})
	if !strings.Contains(evidenceOnly, "evidence version changed") {
		t.Fatalf("reason %q does not name the evidence-version change", evidenceOnly)
	}

	ticket := reverifyDiscardReason(item, ReverifySourceTicket, lockedReverifyTarget{
		State: DiscrepancyStateClosed, Generation: 5, Domain: domain,
	})
	if !strings.Contains(ticket, "ticket re-verification") {
		t.Fatalf("ticket discard reason %q does not name the entry", ticket)
	}
	if len(ticket) > scanEvidenceRefMax {
		t.Fatalf("discard reason exceeds the audit bound: %d bytes", len(ticket))
	}
}

// TestReverifySweepVerifiedCompleteRejectsDiscards pins that a discarded
// outcome can never support the sweep's honest completeness claim.
func TestReverifySweepVerifiedCompleteRejectsDiscards(t *testing.T) {
	base := ReverifySweepResult{WaterlineEnd: true, Stop: ReverifyStopWaterlineEnd}
	if !base.VerifiedComplete() {
		t.Fatal("a clean waterline-end slice must be able to claim completeness")
	}
	base.Discarded = 1
	if base.VerifiedComplete() {
		t.Fatal("a discarded outcome must block the completeness claim")
	}
}
