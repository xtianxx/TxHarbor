//go:build contract

// lifecycle_contract_test.go is the T019 contract layer for the US2
// discrepancy lifecycle (contracts/discrepancy-lifecycle.md; data-model.md
// §1.4–1.8/§2–§3; FR-010/012/013/016/017/018; Q4/Q5). With no database and no
// Docker it pins:
//
//   - the closed state vocabulary and the frozen transition relation
//     `open_claimable -> claimed -> disposing -> pending_verify -> closed`,
//     plus `closed -> reopened -> pending_verify` and the invalidation edge;
//   - claim as a single-owner CAS edge: an owner is mandatory, B can never
//     re-claim or release the row A holds, and unknown states fail closed;
//   - disposed ≠ reverified ≠ closed: dispose only hands over with a recorded
//     effective disposition; close only with a fresh consistent reverify plus
//     a close_basis JSON object; no edge shortcuts either gate;
//   - Q5 invalidation/reopen guards: only conclusion-affecting changes
//     invalidate a closed item (unrelated writes are ignored), invalidation
//     triggers only the approved reverify flow (never auto-disposal), and
//     reopen requires confirmed recurrence of the same identity;
//   - the idempotency-key read-back classification a repeated dispose relies
//     on (PostgreSQL unique violation → read back the recorded row).
//
// `make test-contract` runs this layer with no database and no Docker. The
// persisted refusal/audit half lives in lifecycle_integration_test.go (T020).
package reconciliation

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// contractDiscrepancyStates returns the closed discrepancy state vocabulary
// (contracts/discrepancy-lifecycle.md States; data-model.md §1.4).
func contractDiscrepancyStates() []DiscrepancyState {
	return []DiscrepancyState{
		DiscrepancyStateOpenClaimable,
		DiscrepancyStateClaimed,
		DiscrepancyStateDisposing,
		DiscrepancyStatePendingVerify,
		DiscrepancyStateClosed,
		DiscrepancyStateReopened,
	}
}

// contractDiscrepancyLegalEdges is the frozen lifecycle edge set. Adding or
// removing an edge is a discrepancy-lifecycle contract change
// (contracts/discrepancy-lifecycle.md Transitions) and must be deliberate.
func contractDiscrepancyLegalEdges() [][2]DiscrepancyState {
	return [][2]DiscrepancyState{
		{DiscrepancyStateOpenClaimable, DiscrepancyStateClaimed},
		{DiscrepancyStateClaimed, DiscrepancyStateDisposing},
		{DiscrepancyStateDisposing, DiscrepancyStatePendingVerify},
		{DiscrepancyStatePendingVerify, DiscrepancyStateClosed},
		{DiscrepancyStatePendingVerify, DiscrepancyStateReopened},
		{DiscrepancyStateClosed, DiscrepancyStatePendingVerify},
		{DiscrepancyStateClosed, DiscrepancyStateReopened},
		{DiscrepancyStateReopened, DiscrepancyStatePendingVerify},
	}
}

func TestContractDiscrepancyStateVocabularyIsClosed(t *testing.T) {
	for _, s := range contractDiscrepancyStates() {
		if !s.Valid() {
			t.Errorf("DiscrepancyState(%q).Valid() = false, want true", s)
		}
	}
	for _, raw := range []string{
		"", "Open_Claimable", "OPEN_CLAIMABLE", "open_claimable ",
		"disposed", "verified", "resolved", "pending",
	} {
		if DiscrepancyState(raw).Valid() {
			t.Errorf("DiscrepancyState(%q).Valid() = true, want false (closed vocabulary)", raw)
		}
	}
}

func TestContractDiscrepancyLifecycleMatrixIsFrozen(t *testing.T) {
	states := contractDiscrepancyStates()
	legal := make(map[[2]DiscrepancyState]bool)
	for _, edge := range contractDiscrepancyLegalEdges() {
		if legal[edge] {
			t.Fatalf("duplicate legal edge %s -> %s", edge[0], edge[1])
		}
		legal[edge] = true
	}
	if len(legal) != 8 {
		t.Fatalf("discrepancy lifecycle has %d legal edges, want the frozen 8", len(legal))
	}

	for _, from := range states {
		for _, to := range states {
			want := legal[[2]DiscrepancyState{from, to}]
			if got := CanTransitionDiscrepancy(from, to); got != want {
				t.Errorf("CanTransitionDiscrepancy(%s, %s) = %v, want %v", from, to, got, want)
			}
		}
	}

	outbound := func(from DiscrepancyState) []DiscrepancyState {
		var edges []DiscrepancyState
		for _, to := range states {
			if CanTransitionDiscrepancy(from, to) {
				edges = append(edges, to)
			}
		}
		return edges
	}
	assertOnly := func(from DiscrepancyState, want ...DiscrepancyState) {
		t.Helper()
		got := outbound(from)
		if len(got) != len(want) {
			t.Fatalf("outbound(%s) = %v, want %v", from, got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("outbound(%s) = %v, want %v", from, got, want)
			}
		}
	}
	assertOnly(DiscrepancyStateOpenClaimable, DiscrepancyStateClaimed)
	assertOnly(DiscrepancyStateClaimed, DiscrepancyStateDisposing)
	assertOnly(DiscrepancyStateDisposing, DiscrepancyStatePendingVerify)
	assertOnly(DiscrepancyStatePendingVerify, DiscrepancyStateClosed, DiscrepancyStateReopened)
	assertOnly(DiscrepancyStateClosed, DiscrepancyStatePendingVerify, DiscrepancyStateReopened)
	assertOnly(DiscrepancyStateReopened, DiscrepancyStatePendingVerify)

	// disposed ≠ closed: no edge skips verification, so a disposed item can
	// never be recorded as closed directly.
	if CanTransitionDiscrepancy(DiscrepancyStateDisposing, DiscrepancyStateClosed) {
		t.Errorf("disposing -> closed is legal, want refused (disposed ≠ closed)")
	}
	// No skipping either: an open item cannot jump into disposition,
	// verification or closure.
	for _, to := range []DiscrepancyState{
		DiscrepancyStateDisposing, DiscrepancyStatePendingVerify,
		DiscrepancyStateClosed, DiscrepancyStateReopened,
	} {
		if CanTransitionDiscrepancy(DiscrepancyStateOpenClaimable, to) {
			t.Errorf("open_claimable -> %s is legal, want refused", to)
		}
	}
}

func TestContractClaimIsSingleOwnerCAS(t *testing.T) {
	if err := ValidateDiscrepancyTransition(DiscrepancyStateOpenClaimable, DiscrepancyStateClaimed, TransitionGuard{}); !errors.Is(err, ErrContract) {
		t.Fatalf("claim without owner err = %v, want ErrContract", err)
	}
	if err := ValidateDiscrepancyTransition(DiscrepancyStateOpenClaimable, DiscrepancyStateClaimed, TransitionGuard{ClaimOwner: "   "}); !errors.Is(err, ErrContract) {
		t.Fatalf("claim with blank owner err = %v, want ErrContract", err)
	}
	if err := ValidateDiscrepancyTransition(DiscrepancyStateOpenClaimable, DiscrepancyStateClaimed, TransitionGuard{ClaimOwner: "apikey:1001"}); err != nil {
		t.Fatalf("valid claim refused: %v", err)
	}

	// B may not re-claim, silently take over, or release the row A holds.
	for _, edge := range [][2]DiscrepancyState{
		{DiscrepancyStateClaimed, DiscrepancyStateClaimed},
		{DiscrepancyStateClaimed, DiscrepancyStateOpenClaimable},
		{DiscrepancyStateDisposing, DiscrepancyStateClaimed},
	} {
		err := ValidateDiscrepancyTransition(edge[0], edge[1], TransitionGuard{ClaimOwner: "apikey:1002"})
		if !errors.Is(err, ErrIllegalDiscrepancyTransition) {
			t.Errorf("transition %s -> %s err = %v, want ErrIllegalDiscrepancyTransition", edge[0], edge[1], err)
		}
	}

	// Unknown states fail closed with a contract error, never a silent pass.
	if err := ValidateDiscrepancyTransition(DiscrepancyState("forged"), DiscrepancyStateClaimed, TransitionGuard{ClaimOwner: "apikey:1001"}); !errors.Is(err, ErrContract) {
		t.Errorf("unknown source state err = %v, want ErrContract", err)
	}
	if err := ValidateDiscrepancyTransition(DiscrepancyStateOpenClaimable, DiscrepancyState("forged"), TransitionGuard{ClaimOwner: "apikey:1001"}); !errors.Is(err, ErrContract) {
		t.Errorf("unknown target state err = %v, want ErrContract", err)
	}

	// Without a database the store refuses to pretend: nil store ⇒ ErrContract.
	var store *Store
	if _, err := store.TransitionDiscrepancy(context.Background(), DiscrepancyTransitionRequest{
		DiscrepancyID: "d-1", To: DiscrepancyStateClaimed, Actor: "apikey:1001",
		Guard: TransitionGuard{ClaimOwner: "apikey:1001"},
	}); !errors.Is(err, ErrContract) {
		t.Errorf("nil store TransitionDiscrepancy err = %v, want ErrContract", err)
	}
	if _, err := NewStore(nil); !errors.Is(err, ErrContract) {
		t.Errorf("NewStore(nil) err = %v, want ErrContract", err)
	}
}

// contractCloseGuard builds a close guard evaluated at now with a one-hour
// freshness tolerance.
func contractCloseGuard(now time.Time, evidence *ReverifyEvidence, basis []byte) TransitionGuard {
	return TransitionGuard{
		LatestReverify:    evidence,
		CloseBasis:        basis,
		Now:               now,
		ReverifyTolerance: time.Hour,
	}
}

func TestContractDisposedIsNotReverifiedIsNotClosed(t *testing.T) {
	base := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	fresh := &ReverifyEvidence{Verdict: ReverifyConsistent, EvidenceRef: "evidence-1", FreshnessAt: base}
	validBasis := []byte(`{"range":"100..103","block":103,"version":"v1","observed_at":"2026-09-26T12:00:00Z"}`)

	// dispose -> pending_verify requires a recorded effective disposition:
	// refused/failed dispositions stay with the operator.
	if err := ValidateDiscrepancyTransition(DiscrepancyStateDisposing, DiscrepancyStatePendingVerify, TransitionGuard{}); !errors.Is(err, ErrDispositionRequired) {
		t.Fatalf("dispose without disposition err = %v, want ErrDispositionRequired", err)
	}
	for _, result := range []DispositionResult{DispositionRefused, DispositionFailed} {
		guard := TransitionGuard{Disposition: &DispositionSnapshot{Kind: DispositionAckOnly, Result: result}}
		if err := ValidateDiscrepancyTransition(DiscrepancyStateDisposing, DiscrepancyStatePendingVerify, guard); !errors.Is(err, ErrDispositionRequired) {
			t.Errorf("dispose result %q err = %v, want ErrDispositionRequired", result, err)
		}
	}
	for _, recorded := range []DispositionSnapshot{
		{Kind: DispositionAckOnly, Result: DispositionDone},
		{Kind: DispositionReuseRecovery, Result: DispositionDone, ActionRef: "txlifecycle.UnknownRecovery"},
		{Kind: DispositionNewFixRule, Result: DispositionDryRun},
	} {
		guard := TransitionGuard{Disposition: &recorded}
		if err := ValidateDiscrepancyTransition(DiscrepancyStateDisposing, DiscrepancyStatePendingVerify, guard); err != nil {
			t.Errorf("effective disposition %+v refused: %v", recorded, err)
		}
	}

	// pending_verify -> closed requires fresh consistent reverify evidence:
	// missing, divergent, unknown, stale, evidence-less or expired rows can
	// never close (Q5-4).
	if err := ValidateDiscrepancyTransition(DiscrepancyStatePendingVerify, DiscrepancyStateClosed, contractCloseGuard(base, nil, validBasis)); !errors.Is(err, ErrReverifyRequired) {
		t.Fatalf("close without reverify err = %v, want ErrReverifyRequired", err)
	}
	for _, evidence := range []*ReverifyEvidence{
		{Verdict: ReverifyStale, EvidenceRef: "evidence-1", FreshnessAt: base},
		{Verdict: ReverifyDivergent, EvidenceRef: "evidence-1", FreshnessAt: base},
		{Verdict: ReverifyUnknown, EvidenceRef: "evidence-1", FreshnessAt: base},
		{Verdict: ReverifyConsistent, EvidenceRef: "   ", FreshnessAt: base},
		{Verdict: ReverifyConsistent, EvidenceRef: "evidence-1", FreshnessAt: base.Add(-2 * time.Hour)},
	} {
		err := ValidateDiscrepancyTransition(DiscrepancyStatePendingVerify, DiscrepancyStateClosed, contractCloseGuard(base, evidence, validBasis))
		if !errors.Is(err, ErrReverifyRequired) {
			t.Errorf("close with evidence %+v err = %v, want ErrReverifyRequired", evidence, err)
		}
	}
	// A zero tolerance can never be satisfied: an unstated freshness bound
	// fails closed instead of accepting any age.
	if err := ValidateDiscrepancyTransition(DiscrepancyStatePendingVerify, DiscrepancyStateClosed, TransitionGuard{
		LatestReverify: fresh, CloseBasis: validBasis, Now: base,
	}); !errors.Is(err, ErrReverifyRequired) {
		t.Errorf("close with zero tolerance err = %v, want ErrReverifyRequired", err)
	}

	// The close basis is mandatory and must be a JSON object snapshot
	// (range/block/version/timestamp), never empty, null or a non-object.
	for _, basis := range [][]byte{nil, []byte(""), []byte("[]"), []byte("null"), []byte("not-json")} {
		err := ValidateDiscrepancyTransition(DiscrepancyStatePendingVerify, DiscrepancyStateClosed, contractCloseGuard(base, fresh, basis))
		if !errors.Is(err, ErrContract) {
			t.Errorf("close basis %q err = %v, want ErrContract", basis, err)
		}
	}
	if err := ValidateDiscrepancyTransition(DiscrepancyStatePendingVerify, DiscrepancyStateClosed, contractCloseGuard(base, fresh, validBasis)); err != nil {
		t.Fatalf("valid close refused: %v", err)
	}

	// Neither a disposed row nor a reopened one may jump straight to closed:
	// both must re-enter verification first.
	for _, from := range []DiscrepancyState{DiscrepancyStateDisposing, DiscrepancyStateReopened} {
		err := ValidateDiscrepancyTransition(from, DiscrepancyStateClosed, contractCloseGuard(base, fresh, validBasis))
		if !errors.Is(err, ErrIllegalDiscrepancyTransition) {
			t.Errorf("%s -> closed err = %v, want ErrIllegalDiscrepancyTransition", from, err)
		}
	}
}

func TestContractDispositionVocabularyAndPhaseRule(t *testing.T) {
	for _, kind := range []DispositionKind{DispositionAckOnly, DispositionReuseRecovery, DispositionNewFixRule} {
		if !kind.Valid() {
			t.Errorf("DispositionKind(%q).Valid() = false, want true", kind)
		}
	}
	for _, raw := range []string{"", "Ack_Only", "ACK_ONLY", "ack_only ", "manual_fix", "payment"} {
		if DispositionKind(raw).Valid() {
			t.Errorf("DispositionKind(%q).Valid() = true, want false (closed vocabulary)", raw)
		}
	}
	for _, result := range []DispositionResult{DispositionDone, DispositionRefused, DispositionFailed, DispositionDryRun} {
		if !result.Valid() {
			t.Errorf("DispositionResult(%q).Valid() = false, want true", result)
		}
	}
	for _, raw := range []string{"", "Done", "success", "cancelled"} {
		if DispositionResult(raw).Valid() {
			t.Errorf("DispositionResult(%q).Valid() = true, want false (closed vocabulary)", raw)
		}
	}

	// new_fix_rule is dry_run-only in this phase (FR-023): done/refused/failed
	// are contract violations, and the DB CHECK only re-enforces it.
	for _, result := range []DispositionResult{DispositionDone, DispositionRefused, DispositionFailed} {
		snapshot := DispositionSnapshot{Kind: DispositionNewFixRule, Result: result, ActionRef: "patch-rule-1"}
		if err := ValidateDisposition(snapshot); !errors.Is(err, ErrContract) {
			t.Errorf("new_fix_rule result %q err = %v, want ErrContract", result, err)
		}
	}
	if err := ValidateDisposition(DispositionSnapshot{Kind: DispositionNewFixRule, Result: DispositionDryRun}); err != nil {
		t.Errorf("new_fix_rule dry_run refused: %v", err)
	}

	// reuse_recovery references an existing entry point only; 014 never
	// auto-executes it, so an empty action_ref is a contract violation.
	if err := ValidateDisposition(DispositionSnapshot{Kind: DispositionReuseRecovery, Result: DispositionDone}); !errors.Is(err, ErrContract) {
		t.Errorf("reuse_recovery without action_ref err = %v, want ErrContract", err)
	}
	if err := ValidateDisposition(DispositionSnapshot{
		Kind: DispositionReuseRecovery, Result: DispositionDone, ActionRef: "events-admin replay",
	}); err != nil {
		t.Errorf("reuse_recovery with action_ref refused: %v", err)
	}

	// ack_only is the default and carries no action reference requirement.
	if err := ValidateDisposition(DispositionSnapshot{Kind: DispositionAckOnly, Result: DispositionDone}); err != nil {
		t.Errorf("ack_only refused: %v", err)
	}

	// Unknown kind/result are refused before any state change.
	if err := ValidateDisposition(DispositionSnapshot{Kind: DispositionKind("manual"), Result: DispositionDone}); !errors.Is(err, ErrContract) {
		t.Errorf("unknown disposition kind err = %v, want ErrContract", err)
	}
	if err := ValidateDisposition(DispositionSnapshot{Kind: DispositionAckOnly, Result: DispositionResult("ok")}); !errors.Is(err, ErrContract) {
		t.Errorf("unknown disposition result err = %v, want ErrContract", err)
	}
}

func TestContractReverifyVerdictsAndCloseFreshness(t *testing.T) {
	for _, verdict := range []ReverifyVerdict{ReverifyConsistent, ReverifyDivergent, ReverifyUnknown, ReverifyStale} {
		if !verdict.Valid() {
			t.Errorf("ReverifyVerdict(%q).Valid() = false, want true", verdict)
		}
	}
	for _, raw := range []string{"", "Consistent", "CONSISTENT", "consistent ", "verified", "failed"} {
		if ReverifyVerdict(raw).Valid() {
			t.Errorf("ReverifyVerdict(%q).Valid() = true, want false (closed vocabulary)", raw)
		}
	}

	base := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	var absent *ReverifyEvidence
	if absent.CanClose(base, time.Hour) {
		t.Errorf("missing evidence CanClose = true, want false")
	}
	// Only a consistent verdict may close; timeout/incomplete/unavailable/
	// insufficient evidence is recorded as divergent/unknown/stale.
	for _, verdict := range []ReverifyVerdict{ReverifyDivergent, ReverifyUnknown, ReverifyStale} {
		evidence := &ReverifyEvidence{Verdict: verdict, EvidenceRef: "evidence-1", FreshnessAt: base}
		if evidence.CanClose(base, time.Hour) {
			t.Errorf("verdict %q CanClose = true, want false", verdict)
		}
	}
	// Consistent evidence must carry its reference and a positive tolerance.
	if (&ReverifyEvidence{Verdict: ReverifyConsistent, FreshnessAt: base}).CanClose(base, time.Hour) {
		t.Errorf("consistent evidence without evidence_ref CanClose = true, want false")
	}
	fresh := &ReverifyEvidence{Verdict: ReverifyConsistent, EvidenceRef: "evidence-1", FreshnessAt: base}
	if fresh.CanClose(base, 0) {
		t.Errorf("zero tolerance CanClose = true, want false")
	}
	if fresh.CanClose(base, -time.Second) {
		t.Errorf("negative tolerance CanClose = true, want false")
	}
	if (&ReverifyEvidence{Verdict: ReverifyConsistent, EvidenceRef: "evidence-1", FreshnessAt: time.Time{}}).CanClose(base, time.Hour) {
		t.Errorf("zero freshness CanClose = true, want false")
	}
	// Future-dated evidence is not fresh evidence.
	if (&ReverifyEvidence{
		Verdict: ReverifyConsistent, EvidenceRef: "evidence-1", FreshnessAt: base.Add(time.Minute),
	}).CanClose(base, time.Hour) {
		t.Errorf("future evidence CanClose = true, want false")
	}
	// Exactly at the tolerance is still fresh; one nanosecond past is stale.
	if !fresh.CanClose(base.Add(time.Hour), time.Hour) {
		t.Errorf("evidence exactly at the tolerance CanClose = false, want true")
	}
	if fresh.CanClose(base.Add(time.Hour+time.Nanosecond), time.Hour) {
		t.Errorf("stale evidence CanClose = true, want false")
	}
}

func TestContractQ5InvalidationGuards(t *testing.T) {
	for _, trigger := range []InvalidationTrigger{
		InvalidationUnrelatedWrite, InvalidationConcurrentWrite, InvalidationReorg,
		InvalidationNewEvidence, InvalidationSourceRotation, InvalidationVersionRotation,
	} {
		if !trigger.Valid() {
			t.Errorf("InvalidationTrigger(%q).Valid() = false, want true", trigger)
		}
	}
	for _, raw := range []string{"", "Reorg", "REORG", "reorg ", "manual_reopen", "unknown_change"} {
		if InvalidationTrigger(raw).Valid() {
			t.Errorf("InvalidationTrigger(%q).Valid() = true, want false (closed vocabulary)", raw)
		}
	}

	// Unrelated writes never invalidate; every other trigger does.
	if InvalidationUnrelatedWrite.AffectsConclusion() {
		t.Errorf("unrelated_write AffectsConclusion = true, want false")
	}
	for _, trigger := range []InvalidationTrigger{
		InvalidationConcurrentWrite, InvalidationReorg, InvalidationNewEvidence,
		InvalidationSourceRotation, InvalidationVersionRotation,
	} {
		if !trigger.AffectsConclusion() {
			t.Errorf("%s AffectsConclusion = false, want true", trigger)
		}
	}

	// closed -> pending_verify needs a recorded conclusion-affecting change.
	if err := ValidateDiscrepancyTransition(DiscrepancyStateClosed, DiscrepancyStatePendingVerify, TransitionGuard{}); !errors.Is(err, ErrInvalidationIgnored) {
		t.Fatalf("invalidation without signal err = %v, want ErrInvalidationIgnored", err)
	}
	unrelated := &InvalidationSignal{Trigger: InvalidationUnrelatedWrite, EvidenceRef: "write-1"}
	if err := ValidateDiscrepancyTransition(DiscrepancyStateClosed, DiscrepancyStatePendingVerify, TransitionGuard{Invalidation: unrelated}); !errors.Is(err, ErrInvalidationIgnored) {
		t.Errorf("unrelated write err = %v, want ErrInvalidationIgnored", err)
	}
	unknown := &InvalidationSignal{Trigger: InvalidationTrigger("manual_reopen"), EvidenceRef: "write-1"}
	if err := ValidateDiscrepancyTransition(DiscrepancyStateClosed, DiscrepancyStatePendingVerify, TransitionGuard{Invalidation: unknown}); !errors.Is(err, ErrContract) {
		t.Errorf("unknown invalidation trigger err = %v, want ErrContract", err)
	}
	reorg := &InvalidationSignal{Trigger: InvalidationReorg, EvidenceRef: "block-103"}
	if err := ValidateDiscrepancyTransition(DiscrepancyStateClosed, DiscrepancyStatePendingVerify, TransitionGuard{Invalidation: reorg}); err != nil {
		t.Errorf("reorg invalidation refused: %v", err)
	}

	// EvaluateInvalidation: conclusion-affecting signals move only a closed
	// item, and only into the approved revertify flow — never into disposal or
	// closure.
	for _, trigger := range []InvalidationTrigger{
		InvalidationConcurrentWrite, InvalidationReorg, InvalidationNewEvidence,
		InvalidationSourceRotation, InvalidationVersionRotation,
	} {
		next, changed, err := EvaluateInvalidation(DiscrepancyStateClosed, &InvalidationSignal{Trigger: trigger})
		if err != nil || !changed || next != DiscrepancyStatePendingVerify {
			t.Errorf("EvaluateInvalidation(closed, %s) = (%s, %v, %v), want (pending_verify, true, nil)",
				trigger, next, changed, err)
		}
	}
	for _, from := range []DiscrepancyState{
		DiscrepancyStateOpenClaimable, DiscrepancyStateClaimed, DiscrepancyStateDisposing,
		DiscrepancyStatePendingVerify, DiscrepancyStateReopened,
	} {
		next, changed, err := EvaluateInvalidation(from, reorg)
		if err != nil || changed || next != from {
			t.Errorf("EvaluateInvalidation(%s, reorg) = (%s, %v, %v), want unchanged", from, next, changed, err)
		}
	}
	if next, changed, err := EvaluateInvalidation(DiscrepancyStateClosed, unrelated); err != nil || changed || next != DiscrepancyStateClosed {
		t.Errorf("EvaluateInvalidation(closed, unrelated) = (%s, %v, %v), want unchanged", next, changed, err)
	}
	if _, _, err := EvaluateInvalidation(DiscrepancyStateClosed, unknown); !errors.Is(err, ErrContract) {
		t.Errorf("EvaluateInvalidation(unknown trigger) err = %v, want ErrContract", err)
	}
	if next, changed, err := EvaluateInvalidation(DiscrepancyStateClosed, nil); err != nil || changed || next != DiscrepancyStateClosed {
		t.Errorf("EvaluateInvalidation(nil) = (%s, %v, %v), want unchanged", next, changed, err)
	}
	// Invalidation is never a disposal: no change signal reaches disposing.
	if CanTransitionDiscrepancy(DiscrepancyStateClosed, DiscrepancyStateDisposing) {
		t.Errorf("closed -> disposing is legal, want refused (invalidation ≠ auto-disposal)")
	}
}

func TestContractQ5ReopenGuards(t *testing.T) {
	// Unconfirmed reports are not recurrence, from any state.
	for _, from := range contractDiscrepancyStates() {
		next, changed := EvaluateRecurrence(from, false)
		if changed || next != from {
			t.Errorf("EvaluateRecurrence(%s, unconfirmed) = (%s, %v), want unchanged", from, next, changed)
		}
	}
	// Confirmed recurrence reopens the original item from closed or
	// pending_verify only; every other state is unchanged (no new ticket and
	// no forced reopen).
	for _, from := range []DiscrepancyState{DiscrepancyStateClosed, DiscrepancyStatePendingVerify} {
		next, changed := EvaluateRecurrence(from, true)
		if !changed || next != DiscrepancyStateReopened {
			t.Errorf("EvaluateRecurrence(%s, confirmed) = (%s, %v), want (reopened, true)", from, next, changed)
		}
	}
	for _, from := range []DiscrepancyState{
		DiscrepancyStateOpenClaimable, DiscrepancyStateClaimed,
		DiscrepancyStateDisposing, DiscrepancyStateReopened,
	} {
		next, changed := EvaluateRecurrence(from, true)
		if changed || next != from {
			t.Errorf("EvaluateRecurrence(%s, confirmed) = (%s, %v), want unchanged", from, next, changed)
		}
	}

	// The reopen edge needs confirmed recurrence.
	if err := ValidateDiscrepancyTransition(DiscrepancyStateClosed, DiscrepancyStateReopened, TransitionGuard{}); !errors.Is(err, ErrRecurrenceUnconfirmed) {
		t.Fatalf("reopen without recurrence err = %v, want ErrRecurrenceUnconfirmed", err)
	}
	confirmed := TransitionGuard{ConfirmedRecurrence: true}
	if err := ValidateDiscrepancyTransition(DiscrepancyStateClosed, DiscrepancyStateReopened, confirmed); err != nil {
		t.Errorf("confirmed reopen from closed refused: %v", err)
	}
	if err := ValidateDiscrepancyTransition(DiscrepancyStatePendingVerify, DiscrepancyStateReopened, confirmed); err != nil {
		t.Errorf("confirmed reopen from pending_verify refused: %v", err)
	}
	for _, from := range []DiscrepancyState{DiscrepancyStateOpenClaimable, DiscrepancyStateClaimed, DiscrepancyStateDisposing} {
		if err := ValidateDiscrepancyTransition(from, DiscrepancyStateReopened, confirmed); !errors.Is(err, ErrIllegalDiscrepancyTransition) {
			t.Errorf("%s -> reopened err = %v, want ErrIllegalDiscrepancyTransition", from, err)
		}
	}

	// A reopened item re-enters the approved reverify flow: it can never skip
	// straight to closed, and reopened -> pending_verify needs no extra guard.
	if err := ValidateDiscrepancyTransition(DiscrepancyStateReopened, DiscrepancyStateClosed, TransitionGuard{
		LatestReverify: &ReverifyEvidence{
			Verdict: ReverifyConsistent, EvidenceRef: "evidence-1", FreshnessAt: time.Now().UTC(),
		},
		CloseBasis:        []byte(`{"range":"100..103"}`),
		ReverifyTolerance: time.Hour,
	}); !errors.Is(err, ErrIllegalDiscrepancyTransition) {
		t.Errorf("reopened -> closed err = %v, want ErrIllegalDiscrepancyTransition", err)
	}
	if err := ValidateDiscrepancyTransition(DiscrepancyStateReopened, DiscrepancyStatePendingVerify, TransitionGuard{}); err != nil {
		t.Errorf("reopened -> pending_verify refused: %v", err)
	}
}

func TestContractIdempotencyKeyReadBackClassification(t *testing.T) {
	if sqlStateUniqueViolation != "23505" {
		t.Fatalf("sqlStateUniqueViolation = %q, want PostgreSQL's unique_violation 23505", sqlStateUniqueViolation)
	}

	// A repeated dispose with the same idempotency_key is a unique violation
	// on disposition_idempotency_key_uniq; the classification helper is what
	// the dispose path uses to switch to read-back instead of inserting a
	// second row or failing the operation.
	const keyConstraint = "disposition_idempotency_key_uniq"
	unique := &pgconn.PgError{Code: sqlStateUniqueViolation, ConstraintName: keyConstraint}
	if got := uniqueViolationConstraint(unique); got != keyConstraint {
		t.Errorf("uniqueViolationConstraint(unique) = %q, want %q", got, keyConstraint)
	}
	// pgx often wraps the driver error; classification must survive wrapping.
	wrapped := fmt.Errorf("insert disposition: %w", unique)
	if got := uniqueViolationConstraint(wrapped); got != keyConstraint {
		t.Errorf("uniqueViolationConstraint(wrapped unique) = %q, want %q", got, keyConstraint)
	}
	// Non-unique failures never classify as a read-back trigger.
	if got := uniqueViolationConstraint(&pgconn.PgError{
		Code: "23503", ConstraintName: "disposition_discrepancy_fkey",
	}); got != "" {
		t.Errorf("uniqueViolationConstraint(foreign key violation) = %q, want empty", got)
	}
	if got := uniqueViolationConstraint(errors.New("connection reset")); got != "" {
		t.Errorf("uniqueViolationConstraint(plain error) = %q, want empty", got)
	}
	if got := uniqueViolationConstraint(nil); got != "" {
		t.Errorf("uniqueViolationConstraint(nil) = %q, want empty", got)
	}

	// The read-back converges on the recorded result, so the result vocabulary
	// is a closed set stored verbatim (disposition.result CHECK).
	for result, stored := range map[DispositionResult]string{
		DispositionDone:    "done",
		DispositionRefused: "refused",
		DispositionFailed:  "failed",
		DispositionDryRun:  "dry_run",
	} {
		if string(result) != stored {
			t.Errorf("DispositionResult %q stores as %q, want %q", result, string(result), stored)
		}
	}
}
