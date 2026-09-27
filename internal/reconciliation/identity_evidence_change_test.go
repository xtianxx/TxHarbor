// identity_evidence_change_test.go is the unit layer for the T035/Q5 evidence
// change rules and the oversized-member evidence bound (spec FR-007, Q2/Q5,
// data-model.md §2):
//
//   - a changed member set and a changed evidence version (reorg replacement)
//     keep the original tx-aggregate ticket group and are reported as changed
//     evidence (the persistence layer maps them onto the SAME ticket, moving a
//     conclusion-bearing state to pending_verify — see
//     TestIntegrationTxAggregateIdentityAcceptance in
//     txaggregate_integration_test.go for the durable proof); an identical
//     re-observation is not a change;
//   - an oversized member set is summarized by a member digest, and that digest
//     never replaces the per-member occurrence references: every original
//     member log stays individually enumerable and queryable.
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

// memberSetClassification mints one tx-aggregate missing detection whose chain
// snapshot is content-bound to the member set (mimicking the compare loop's
// chainCandidateSnapshot, which encodes every matched member log) and whose
// version domain carries the observed block identity.
func memberSetClassification(t *testing.T, scope IdentityScope, txHash, blockHash string,
	blockNumber int64, members []TxAggregateMember) Classification {
	t.Helper()
	identity, err := BuildIdentity(IdentityInput{
		Scope:    scope,
		Category: CategoryMissing,
		BusinessKey: BusinessKey{
			Kind:  BusinessKeyTxHash,
			Value: txHash,
		},
		Snapshot: Snapshot{
			Chain: txAggregateMemberCanonicalBytes(members),
			PG:    []byte(`{"pg":"absent"}`),
			Event: []byte(`{"event":"absent"}`),
		},
		VersionDomain: VersionDomain{
			BlockNumber: uint64(blockNumber),
			BlockHash:   blockHash,
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

// changingMember builds one member log of the shared block identity.
func changingMember(txHash, blockHash string, blockNumber, logIndex int64) TxAggregateMember {
	return TxAggregateMember{
		BlockNumber: blockNumber,
		BlockHash:   blockHash,
		TxHash:      txHash,
		LogIndex:    logIndex,
		Contract:    "0x" + strings.Repeat("c1", 20),
		Topic0:      "0x" + strings.Repeat("d1", 32),
	}
}

func TestTxAggregateMemberSetAndVersionChangeKeepOriginalTicket(t *testing.T) {
	scope, err := HeightIdentityScope("7", 700, 703, BusinessWithdrawal)
	if err != nil {
		t.Fatalf("HeightIdentityScope: %v", err)
	}
	txHash := "0x" + strings.Repeat("11", 32)
	blockA := "0x" + strings.Repeat("aa", 32)
	blockB := "0x" + strings.Repeat("bb", 32)

	base := memberSetClassification(t, scope, txHash, blockA, 700,
		[]TxAggregateMember{changingMember(txHash, blockA, 700, 0), changingMember(txHash, blockA, 700, 1)})
	grown := memberSetClassification(t, scope, txHash, blockA, 700,
		[]TxAggregateMember{changingMember(txHash, blockA, 700, 0), changingMember(txHash, blockA, 700, 1), changingMember(txHash, blockA, 700, 2)})
	replaced := memberSetClassification(t, scope, txHash, blockB, 701,
		[]TxAggregateMember{changingMember(txHash, blockB, 701, 0), changingMember(txHash, blockB, 701, 1)})
	repeated := memberSetClassification(t, scope, txHash, blockA, 700,
		[]TxAggregateMember{changingMember(txHash, blockA, 700, 0), changingMember(txHash, blockA, 700, 1)})

	// One ticket group per tx: every variant groups under the same key, so the
	// persistence layer reuses the original ticket instead of minting a second
	// one (T035; the durable mapping is proven by the integration test).
	baseKey, baseAggregate := scanTicketGroupKey(base)
	if !baseAggregate {
		t.Fatalf("base detection was not grouped as a tx aggregate")
	}
	for name, variant := range map[string]Classification{
		"grown member set":       grown,
		"reorg replacement":      replaced,
		"identical re-detection": repeated,
	} {
		key, aggregate := scanTicketGroupKey(variant)
		if !aggregate || key != baseKey {
			t.Fatalf("%s grouped as (%q,%v), want the original key %q", name, key, aggregate, baseKey)
		}
		if variant.Identity.BusinessKey() != base.Identity.BusinessKey() {
			t.Fatalf("%s changed the business key: %+v vs %+v", name, variant.Identity.BusinessKey(), base.Identity.BusinessKey())
		}
	}

	// The recorded root is the first observation: an identical re-observation
	// is not a change; a grown member set and a reorg replacement both are.
	recorded := &scanAggregateRoot{
		ID:             uuid.New(),
		State:          DiscrepancyStateOpenClaimable,
		ContentHashHex: base.Identity.ContentHash().Hex(),
		Domain: PersistedEvidenceDomain{
			VersionDomain: base.Identity.VersionDomain(),
		},
	}
	if changed, err := txAggregateEvidenceChanged(recorded, repeated); err != nil || changed {
		t.Fatalf("identical re-observation = (%v, %v), want unchanged", changed, err)
	}
	if changed, err := txAggregateEvidenceChanged(recorded, grown); err != nil || !changed {
		t.Fatalf("grown member set = (%v, %v), want changed", changed, err)
	}
	if changed, err := txAggregateEvidenceChanged(recorded, replaced); err != nil || !changed {
		t.Fatalf("reorg replacement = (%v, %v), want changed", changed, err)
	}
	if recorded.ContentHashHex == grown.Identity.ContentHash().Hex() {
		t.Fatalf("the grown member set did not move the recorded content hash")
	}
}

func TestTxAggregateEvidenceRefDigestKeepsMembersIndividuallyTraceable(t *testing.T) {
	const memberCount = 60
	txHash := "0x" + strings.Repeat("22", 32)
	blockHash := "0x" + strings.Repeat("aa", 32)
	members := make([]TxAggregateMember, 0, memberCount)
	for index := int64(0); index < memberCount; index++ {
		members = append(members, changingMember(txHash, blockHash, 700, index))
	}

	// The single aggregate reference exceeds the occurrence evidence bound and
	// is summarized as a member digest (never a silently dropped subset).
	aggregate, err := TxAggregateEvidenceRef(members, 512)
	if err != nil {
		t.Fatalf("TxAggregateEvidenceRef: %v", err)
	}
	if !strings.Contains(aggregate, "members=60") || !strings.Contains(aggregate, "digest=sha256:") {
		t.Fatalf("aggregate ref %q is not the oversized digest form", aggregate)
	}
	if len(aggregate) > 512 {
		t.Fatalf("aggregate ref len = %d, want <= 512", len(aggregate))
	}

	// The digest is a summary only: the persistence path renders one bounded
	// occurrence reference per original member log (scanMemberEvidenceRef), and
	// each of them enumerates that member's block/log/contract/topic verbatim.
	traceable := make(map[int64]bool, memberCount)
	for _, member := range members {
		ref := scanMemberEvidenceRef(member, "attempt-under-test")
		if len(ref) > 512 {
			t.Fatalf("member occurrence ref len = %d, want <= 512", len(ref))
		}
		for _, token := range []string{
			"tx=" + txHash,
			"b=700",
			"h=" + blockHash,
			"l=" + strconv.FormatInt(member.LogIndex, 10),
			"c=" + member.Contract,
			"t0=" + member.Topic0,
		} {
			if !strings.Contains(ref, token) {
				t.Fatalf("member %d occurrence ref %q lost %s", member.LogIndex, ref, token)
			}
		}
		single, err := TxAggregateEvidenceRef([]TxAggregateMember{member}, 512)
		if err != nil {
			t.Fatalf("single member ref %d: %v", member.LogIndex, err)
		}
		if single != ref[:len(single)] {
			t.Fatalf("member %d single ref is not the prefix of its occurrence ref", member.LogIndex)
		}
		traceable[member.LogIndex] = true
	}
	if len(traceable) != memberCount {
		t.Fatalf("traceable members = %d, want %d", len(traceable), memberCount)
	}

	// The digest is content-bound and order-independent: a reordered set keeps
	// the same digest, a changed member moves it.
	reordered := make([]TxAggregateMember, 0, memberCount)
	for index := memberCount - 1; index >= 0; index-- {
		reordered = append(reordered, members[index])
	}
	shuffledRef, err := TxAggregateEvidenceRef(reordered, 512)
	if err != nil {
		t.Fatalf("TxAggregateEvidenceRef (reordered): %v", err)
	}
	if shuffledRef != aggregate {
		t.Fatalf("member order changed the aggregate digest: %q vs %q", shuffledRef, aggregate)
	}
	changedMembers := append([]TxAggregateMember(nil), members...)
	changedMembers[0].LogIndex = 999
	changedRef, err := TxAggregateEvidenceRef(changedMembers, 512)
	if err != nil {
		t.Fatalf("TxAggregateEvidenceRef (changed): %v", err)
	}
	if changedRef == aggregate {
		t.Fatalf("a changed member set kept the original digest")
	}
}
