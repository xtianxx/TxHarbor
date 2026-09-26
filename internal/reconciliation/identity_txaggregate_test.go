// identity_txaggregate_test.go is the T035 unit layer for the tx-aggregate
// identity and member-evidence rules (spec FR-007, Q2/Q5; data-model.md §2):
//
//   - one transaction with several member logs is ONE ticket group key and its
//     member facts are preserved completely (never split, never dropped);
//   - the member evidence reference enumerates every member and stays bounded
//     (oversized member sets are digested, never silently truncated);
//   - the persisted evidence domain carries the detection scope so aggregate
//     roots only merge within the same scope (different scope = different
//     identity, never a merge);
//   - a changed content hash or a reorg replacement block is the invalidation
//     decision input (same ticket, no new ticket);
//   - different transactions never share a key.
//
// No database and no Docker: every decision here is a pure function.
package reconciliation

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// txAggMember builds one member log fact of a transaction.
func txAggMember(txHash string, logIndex int64, contract string) TxAggregateMember {
	return TxAggregateMember{
		BlockNumber: 700,
		BlockHash:   "0x" + strings.Repeat("aa", 32),
		TxHash:      txHash,
		LogIndex:    logIndex,
		Contract:    contract,
		Topic0:      "0x" + strings.Repeat("dd", 32),
	}
}

// txAggClassification mints one ticketable tx-aggregate classification.
func txAggClassification(t *testing.T, scope IdentityScope, txHash string, members ...TxAggregateMember) Classification {
	t.Helper()
	identity, err := BuildIdentity(IdentityInput{
		Scope:    scope,
		Category: CategoryMissing,
		BusinessKey: BusinessKey{
			Kind:  BusinessKeyTxHash,
			Value: txHash,
		},
		Snapshot: Snapshot{
			Chain: []byte(`{"chain":"fact"}`),
			PG:    []byte(`{"pg":"absent"}`),
			Event: []byte(`{"event":"absent"}`),
		},
		VersionDomain: VersionDomain{
			BlockNumber: 700,
			BlockHash:   "0x" + strings.Repeat("aa", 32),
			EvidenceAt:  time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC),
		},
	})
	if err != nil {
		t.Fatalf("BuildIdentity: %v", err)
	}
	return Classification{
		Category: CategoryMissing,
		Ticket:   true,
		Identity: identity,
		Members:  members,
	}
}

func TestCanonicalizeTxAggregateMembersDeterministic(t *testing.T) {
	tx := "0x" + strings.Repeat("11", 32)
	a := txAggMember(strings.ToUpper(tx), 1, "0x"+strings.Repeat("22", 20))
	b := txAggMember(tx, 0, "0x"+strings.Repeat("33", 20))
	// A duplicate of a with different case must collapse onto a.
	dup := txAggMember(tx, 1, "0x"+strings.Repeat("22", 20))
	dup.BlockHash = strings.ToUpper(dup.BlockHash)

	first, err := CanonicalizeTxAggregateMembers([]TxAggregateMember{a, b, dup})
	if err != nil {
		t.Fatalf("CanonicalizeTxAggregateMembers: %v", err)
	}
	if len(first) != 2 {
		t.Fatalf("canonical members = %d, want 2 (duplicate collapsed)", len(first))
	}
	if first[0].LogIndex != 0 || first[1].LogIndex != 1 {
		t.Fatalf("member order = %d,%d, want 0,1 (stable by log index)", first[0].LogIndex, first[1].LogIndex)
	}
	for _, member := range first {
		if member.TxHash != tx || member.BlockHash != strings.ToLower(member.BlockHash) {
			t.Fatalf("member %+v is not canonical (lowercase tx/block hash)", member)
		}
	}

	second, err := CanonicalizeTxAggregateMembers([]TxAggregateMember{b, dup, a})
	if err != nil {
		t.Fatalf("CanonicalizeTxAggregateMembers (shuffled): %v", err)
	}
	if len(first) != len(second) {
		t.Fatalf("shuffled canonical members = %d, want %d", len(second), len(first))
	}
	for i := range first {
		if first[i] != second[i] {
			t.Fatalf("canonicalization is order-dependent: %+v vs %+v", first[i], second[i])
		}
	}

	for _, invalid := range []TxAggregateMember{
		{BlockNumber: -1, TxHash: tx},
		{LogIndex: -1, TxHash: tx},
		{TxHash: "  "},
	} {
		if _, err := CanonicalizeTxAggregateMembers([]TxAggregateMember{invalid}); err == nil {
			t.Fatalf("invalid member %+v accepted", invalid)
		}
	}
}

func TestTxAggregateEvidenceRefEnumeratesAndBounds(t *testing.T) {
	tx := "0x" + strings.Repeat("44", 32)
	members := []TxAggregateMember{
		txAggMember(tx, 0, "0x"+strings.Repeat("22", 20)),
		txAggMember(tx, 1, "0x"+strings.Repeat("33", 20)),
	}
	full, err := TxAggregateEvidenceRef(members, 512)
	if err != nil {
		t.Fatalf("TxAggregateEvidenceRef: %v", err)
	}
	for _, member := range members {
		if !strings.Contains(full, "l="+strconv.FormatInt(member.LogIndex, 10)) ||
			!strings.Contains(full, "c="+member.Contract) ||
			!strings.Contains(full, "t0="+member.Topic0) {
			t.Fatalf("evidence ref %q does not enumerate member %+v", full, member)
		}
	}
	if !strings.Contains(full, "members=2") {
		t.Fatalf("evidence ref %q lost the member count", full)
	}

	shuffled, err := TxAggregateEvidenceRef([]TxAggregateMember{members[1], members[0]}, 512)
	if err != nil || shuffled != full {
		t.Fatalf("member order changed the evidence ref: %q vs %q (err %v)", shuffled, full, err)
	}

	bounded, err := TxAggregateEvidenceRef(members, 180)
	if err != nil {
		t.Fatalf("bounded TxAggregateEvidenceRef: %v", err)
	}
	if len(bounded) > 180 {
		t.Fatalf("bounded evidence ref len = %d, want <= 180", len(bounded))
	}
	if !strings.Contains(bounded, "digest=sha256:") || !strings.Contains(bounded, "members=2") {
		t.Fatalf("bounded evidence ref %q is not the digested form", bounded)
	}
	if strings.Contains(bounded, members[0].Contract) {
		t.Fatalf("bounded evidence ref %q leaked an unbounded member subset", bounded)
	}

	if _, err := TxAggregateEvidenceRef(members, 5); err == nil {
		t.Fatalf("an impossibly small bound was accepted instead of refused")
	}
	if _, err := TxAggregateEvidenceRef(nil, 512); err == nil {
		t.Fatalf("member-less evidence was accepted")
	}
}

func TestPersistedEvidenceDomainRoundTrip(t *testing.T) {
	scope, err := HeightIdentityScope("7", 700, 703, BusinessWithdrawal)
	if err != nil {
		t.Fatalf("HeightIdentityScope: %v", err)
	}
	version := VersionDomain{
		BlockNumber: 700,
		BlockHash:   "0x" + strings.Repeat("aa", 32),
		EvidenceAt:  time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC),
	}
	raw, err := PersistedEvidenceDomainJSON(version, &scope)
	if err != nil {
		t.Fatalf("PersistedEvidenceDomainJSON: %v", err)
	}
	decoded, err := ParsePersistedEvidenceDomain(raw)
	if err != nil {
		t.Fatalf("ParsePersistedEvidenceDomain: %v", err)
	}
	if decoded.VersionDomain != version {
		t.Fatalf("decoded version domain = %+v, want %+v", decoded.VersionDomain, version)
	}
	if decoded.Scope == nil || !SameIdentityScope(*decoded.Scope, scope) {
		t.Fatalf("decoded scope = %+v, want the observed scope", decoded.Scope)
	}

	other, err := HeightIdentityScope("7", 700, 703, BusinessDeposit)
	if err != nil {
		t.Fatalf("HeightIdentityScope (other): %v", err)
	}
	if SameIdentityScope(scope, other) {
		t.Fatalf("different business types compared equal")
	}
	otherRange, err := HeightIdentityScope("7", 710, 713, BusinessWithdrawal)
	if err != nil {
		t.Fatalf("HeightIdentityScope (range): %v", err)
	}
	if SameIdentityScope(scope, otherRange) {
		t.Fatalf("different ranges compared equal")
	}
	otherChain, err := HeightIdentityScope("8", 700, 703, BusinessWithdrawal)
	if err != nil {
		t.Fatalf("HeightIdentityScope (chain): %v", err)
	}
	if SameIdentityScope(scope, otherChain) {
		t.Fatalf("different chains compared equal")
	}
	// Canonical business-type order is not part of the identity.
	reordered, err := HeightIdentityScope("7", 700, 703, BusinessDeposit, BusinessWithdrawal)
	if err != nil {
		t.Fatalf("HeightIdentityScope (reordered): %v", err)
	}
	twoTypes, err := HeightIdentityScope("7", 700, 703, BusinessWithdrawal, BusinessDeposit)
	if err != nil {
		t.Fatalf("HeightIdentityScope (two types): %v", err)
	}
	if !SameIdentityScope(reordered, twoTypes) {
		t.Fatalf("canonical business-type sets did not compare equal")
	}
	if SameIdentityScope(scope, twoTypes) {
		t.Fatalf("a subset business-type set compared equal to a superset")
	}

	if _, err := PersistedEvidenceDomainJSON(version, nil); err != nil {
		t.Fatalf("nil scope document: %v", err)
	}
	malformed := IdentityScope{ChainID: "7", Kind: ScopeHeight, From: 1, To: 2,
		BusinessTypes: []BusinessType{"unknown"}}
	if _, err := PersistedEvidenceDomainJSON(version, &malformed); err == nil {
		t.Fatalf("malformed scope was persisted instead of refused")
	}
	for _, raw := range [][]byte{nil, {}, []byte("  "), []byte("not-json"), []byte(`[]`)} {
		if _, err := ParsePersistedEvidenceDomain(raw); err == nil {
			t.Fatalf("malformed persisted domain %q was accepted", raw)
		}
	}
	// A document without a scope marker decodes with a nil scope (the legacy
	// compatibility shape): callers must handle it explicitly, never guess.
	legacy, err := ParsePersistedEvidenceDomain([]byte(`{"block_number":700,"block_hash":"0xaa"}`))
	if err != nil {
		t.Fatalf("legacy document: %v", err)
	}
	if legacy.Scope != nil {
		t.Fatalf("legacy document invented a scope: %+v", legacy.Scope)
	}
}

func TestTxAggregateIdentityStabilityAndIsolation(t *testing.T) {
	scope, err := HeightIdentityScope("7", 700, 703, BusinessWithdrawal)
	if err != nil {
		t.Fatalf("HeightIdentityScope: %v", err)
	}
	base := IdentityInput{
		Scope:    scope,
		Category: CategoryMissing,
		BusinessKey: BusinessKey{
			Kind:  BusinessKeyTxHash,
			Value: "0x" + strings.Repeat("11", 32),
		},
		Snapshot: Snapshot{
			Chain: []byte(`{"chain":"fact","block":"a"}`),
			PG:    []byte(`{"pg":"absent"}`),
			Event: []byte(`{"event":"absent"}`),
		},
		VersionDomain: VersionDomain{
			BlockNumber: 700,
			BlockHash:   "0x" + strings.Repeat("aa", 32),
			EvidenceAt:  time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC),
		},
	}
	first, err := BuildIdentity(base)
	if err != nil {
		t.Fatalf("BuildIdentity: %v", err)
	}
	repeat, err := BuildIdentity(base)
	if err != nil {
		t.Fatalf("BuildIdentity (repeat): %v", err)
	}
	if !first.SameIdentity(repeat) {
		t.Fatalf("identical detections minted different identities")
	}

	// A reorg replacement (same tx, new block/content) is a changed identity:
	// the persistence layer maps it back onto the same ticket and invalidates,
	// it never mints a second ticket.
	replaced := base
	replaced.Snapshot.Chain = []byte(`{"chain":"fact","block":"b"}`)
	replaced.VersionDomain.BlockHash = "0x" + strings.Repeat("bb", 32)
	replacedID, err := BuildIdentity(replaced)
	if err != nil {
		t.Fatalf("BuildIdentity (replaced): %v", err)
	}
	if first.SameIdentity(replacedID) {
		t.Fatalf("a reorg replacement minted the same identity")
	}
	if replacedID.BusinessKey() != first.BusinessKey() {
		t.Fatalf("replacement changed the business key: %+v vs %+v", replacedID.BusinessKey(), first.BusinessKey())
	}

	// A different transaction is a different identity.
	otherTx := base
	otherTx.BusinessKey.Value = "0x" + strings.Repeat("99", 32)
	otherID, err := BuildIdentity(otherTx)
	if err != nil {
		t.Fatalf("BuildIdentity (other tx): %v", err)
	}
	if first.SameIdentity(otherID) {
		t.Fatalf("different transactions shared one identity")
	}

	// The same tx in a different scope is a different identity (isolation).
	otherScope, err := HeightIdentityScope("7", 710, 713, BusinessWithdrawal)
	if err != nil {
		t.Fatalf("HeightIdentityScope (other scope): %v", err)
	}
	scoped := base
	scoped.Scope = otherScope
	scopedID, err := BuildIdentity(scoped)
	if err != nil {
		t.Fatalf("BuildIdentity (other scope): %v", err)
	}
	if first.SameIdentity(scopedID) {
		t.Fatalf("the same tx under a different scope shared one identity")
	}
}

func TestScanTicketGroupKeyAggregatesOneTx(t *testing.T) {
	scope, err := HeightIdentityScope("7", 700, 703, BusinessWithdrawal)
	if err != nil {
		t.Fatalf("HeightIdentityScope: %v", err)
	}
	first := txAggClassification(t, scope, "0x"+strings.Repeat("11", 32), txAggMember("0x"+strings.Repeat("11", 32), 0, "0xc1"))
	secondMember := txAggClassification(t, scope, "0x"+strings.Repeat("11", 32), txAggMember("0x"+strings.Repeat("11", 32), 1, "0xc1"))

	firstKey, firstAggregate := scanTicketGroupKey(first)
	secondKey, secondAggregate := scanTicketGroupKey(secondMember)
	if !firstAggregate || !secondAggregate || firstKey != secondKey {
		t.Fatalf("same-tx detections grouped differently: (%q,%v) vs (%q,%v)",
			firstKey, firstAggregate, secondKey, secondAggregate)
	}

	otherTx := txAggClassification(t, scope, "0x"+strings.Repeat("22", 32), txAggMember("0x"+strings.Repeat("22", 32), 0, "0xc2"))
	otherKey, otherAggregate := scanTicketGroupKey(otherTx)
	if !otherAggregate || otherKey == firstKey {
		t.Fatalf("different transactions shared the aggregate key %q", otherKey)
	}

	byRequest := first
	byRequest.Identity = func() Identity {
		id, err := BuildIdentity(IdentityInput{
			Scope: scope, Category: CategoryMissing,
			BusinessKey:   BusinessKey{Kind: BusinessKeyRequestID, Value: "req-1"},
			Snapshot:      Snapshot{Chain: []byte("c"), PG: []byte("p"), Event: []byte("e")},
			VersionDomain: VersionDomain{BlockNumber: 700, BlockHash: "0xaa", EvidenceAt: time.Now().UTC()},
		})
		if err != nil {
			t.Fatalf("BuildIdentity: %v", err)
		}
		return id
	}()
	if _, aggregate := scanTicketGroupKey(byRequest); aggregate {
		t.Fatalf("a non-tx business key was grouped as a tx aggregate")
	}

	notMissing := first
	notMissing.Category = CategoryStateMismatch
	if _, aggregate := scanTicketGroupKey(notMissing); aggregate {
		t.Fatalf("a non-missing category was grouped as a tx aggregate")
	}
}

func TestScanTicketGroupMembersPreserveAllLogs(t *testing.T) {
	scope, err := HeightIdentityScope("7", 700, 703, BusinessWithdrawal)
	if err != nil {
		t.Fatalf("HeightIdentityScope: %v", err)
	}
	tx := "0x" + strings.Repeat("11", 32)
	log0 := txAggMember(tx, 0, "0x"+strings.Repeat("22", 20))
	log1 := txAggMember(tx, 1, "0x"+strings.Repeat("33", 20))
	first := txAggClassification(t, scope, tx, log0)
	second := txAggClassification(t, scope, tx, log1)

	group := &scanTicketGroup{
		aggregate:       true,
		businessKey:     "tx_hash=" + tx,
		primary:         first,
		classifications: []Classification{first, second},
	}
	members := group.members()
	if len(members) != 2 {
		t.Fatalf("grouped members = %d, want both member logs preserved", len(members))
	}
	if members[0].LogIndex != 0 || members[1].LogIndex != 1 {
		t.Fatalf("grouped members = %+v, want canonical log order", members)
	}
	if members[0].Contract != log0.Contract || members[1].Contract != log1.Contract {
		t.Fatalf("grouped member payloads were lost: %+v", members)
	}

	// Repeats inside one batch stay a single ticket group with an unchanged
	// member set (no member inflation, no member loss).
	group.classifications = append(group.classifications, first)
	if members := group.members(); len(members) != 2 {
		t.Fatalf("repeated member facts changed the grouped member count: %d, want 2", len(members))
	}
}

func TestTxAggregateEvidenceChangedDecision(t *testing.T) {
	scope, err := HeightIdentityScope("7", 700, 703, BusinessWithdrawal)
	if err != nil {
		t.Fatalf("HeightIdentityScope: %v", err)
	}
	tx := "0x" + strings.Repeat("11", 32)
	primary := txAggClassification(t, scope, tx, txAggMember(tx, 0, "0xc1"))
	recorded := &scanAggregateRoot{
		ID:             uuid.New(),
		State:          DiscrepancyStateOpenClaimable,
		ContentHashHex: primary.Identity.ContentHash().Hex(),
		Domain: PersistedEvidenceDomain{
			VersionDomain: primary.Identity.VersionDomain(),
		},
	}
	changed, err := txAggregateEvidenceChanged(recorded, primary)
	if err != nil {
		t.Fatalf("txAggregateEvidenceChanged: %v", err)
	}
	if changed {
		t.Fatalf("an identical re-observation was reported as changed")
	}

	newContent := recorded
	newContent.ContentHashHex = strings.Repeat("00", 32)
	if changed, err := txAggregateEvidenceChanged(newContent, primary); err != nil || !changed {
		t.Fatalf("content change = (%v, %v), want changed", changed, err)
	}

	newBlock := recorded
	newBlock.Domain.BlockHash = "0x" + strings.Repeat("cc", 32)
	if changed, err := txAggregateEvidenceChanged(newBlock, primary); err != nil || !changed {
		t.Fatalf("reorg replacement block = (%v, %v), want changed", changed, err)
	}

	if _, err := txAggregateEvidenceChanged(nil, primary); err == nil {
		t.Fatalf("nil recorded ticket was accepted")
	}
	if _, err := txAggregateEvidenceChanged(recorded, Classification{}); err == nil {
		t.Fatalf("identity-less detection was accepted")
	}
}

func TestScanCandidateMembersPreferExplicitMembers(t *testing.T) {
	tx := "0x" + strings.Repeat("11", 32)
	explicit := []TxAggregateMember{txAggMember(tx, 0, "0xc1")}
	matched := []ChainFactLog{
		{BlockNumber: 700, BlockHash: "0xaa", TxHash: tx, LogIndex: 0, Contract: "0xc1", Topic0: "0xd"},
		{BlockNumber: 700, BlockHash: "0xaa", TxHash: tx, LogIndex: 1, Contract: "0xc1", Topic0: "0xd"},
	}
	candidate := ScanCandidate{
		BusinessType: BusinessWithdrawal,
		BusinessKey:  BusinessKey{Kind: BusinessKeyTxHash, Value: tx},
		Members:      explicit,
	}
	got := scanCandidateMembers(candidate, matched)
	if len(got) != 1 || got[0].Contract != "0xc1" {
		t.Fatalf("explicit members not preferred: %+v", got)
	}

	candidate.Members = nil
	got = scanCandidateMembers(candidate, matched)
	if len(got) != 2 {
		t.Fatalf("fallback members = %d, want both matched logs", len(got))
	}

	candidate.Members = []TxAggregateMember{{TxHash: "  "}}
	if got := scanCandidateMembers(candidate, matched); got != nil {
		// An invalid explicit member set must not silently fall back to the
		// matched logs (that would misrepresent the discovery evidence).
		t.Fatalf("invalid explicit members silently replaced: %+v", got)
	}
}
