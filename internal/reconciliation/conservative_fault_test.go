//go:build fault

// conservative_fault_test.go is the T024 fault layer for US3's conservative
// semantics under reorg, unknown receipts, kill/restart interrupts and
// concurrent updates (quickstart §4/§5/§7/§10; contracts/discrepancy-lifecycle.md
// Transitions/History; data-model.md §1.4/§1.7/§3; FR-004/005/010/016–019, Q5,
// SC-003/006).
//
// This file is an independent fault channel: `make test-fault` runs it with
// the `fault` build tag on the scheduled/release-gate lane and it is never an
// ordinary-PR gate (quickstart Layering). It performs no database, Docker,
// network or timing I/O — no sleeps, no clocks other than fixed guarded
// instants — and pins only the pure evaluator/guard layer of lifecycle.go
// (EvaluateInvalidation, EvaluateRecurrence, ValidateDiscrepancyTransition,
// ReverifyEvidence.CanClose and the transition relation). The T027 bounded
// reverify executor (reverify.go) is owned by a concurrent lane and is
// deliberately not imported here.
//
// The four fault-injection groups of the Phase 5 Independent Test are:
//
//  1. reorg: a chain reorganization touching a closed item's recorded block
//     identity invalidates it to pending_verify; unrelated writes do nothing;
//  2. unknown receipt / insufficient evidence: an unknown verdict, a missing
//     evidence reference or zero/expired freshness can never be recorded as
//     consistent; a close over it is refused with ErrReverifyRequired;
//  3. kill/restart interrupt: an interrupted close leaves no partial
//     conclusion — leaving closed still requires a valid conclusion-affecting
//     signal, after restart the close still requires fresh consistent
//     evidence, signal re-drives are idempotent, and only the frozen
//     conservative edges are reachable;
//  4. concurrent updates: conclusion-affecting concurrent writes, new
//     evidence and source/version rotation move a closed item to
//     pending_verify (never to disposal or closure), confirmed recurrence
//     reopens the original item, unconfirmed reports do nothing.
//
// The stale-result rule (a consistent verdict older than the tolerance never
// closes) is pinned separately, and the convergence matrix at the end asserts
// across groups that no fault path can ever leave the defined conservative
// state set (zero wrong payments: no fault outcome reaches a disposal or
// closure state without the full evidence chain) and that every refusal
// leaves an approved way forward (zero permanent false conclusions: no state
// is a dead end).
//
// Assertions mirror lifecycle_contract_test.go: plain t.Errorf/t.Fatalf on
// the pure functions, sentinel errors matched through errors.Is.
package reconciliation

import (
	"errors"
	"testing"
	"time"
)

// faultBase is the fixed guard evaluation instant all group cases share. The
// layer is time-pure: freshness assertions are computed against this instant,
// never against time.Now(), so the fault layer is deterministic.
var faultBase = time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)

// faultTolerance is the freshness tolerance every group close guard uses.
const faultTolerance = time.Hour

// faultBasis is a valid close_basis JSON object (range/block/version/timestamp).
func faultBasis() []byte {
	return []byte(`{"range":"100..103","block":103,"block_hash":"0xfeed","version":"v1","observed_at":"2026-09-27T12:00:00Z"}`)
}

// faultAllStates returns the closed discrepancy state vocabulary.
func faultAllStates() []DiscrepancyState {
	return []DiscrepancyState{
		DiscrepancyStateOpenClaimable,
		DiscrepancyStateClaimed,
		DiscrepancyStateDisposing,
		DiscrepancyStatePendingVerify,
		DiscrepancyStateClosed,
		DiscrepancyStateReopened,
	}
}

// faultNonClosedStates returns every state except closed.
func faultNonClosedStates() []DiscrepancyState {
	return []DiscrepancyState{
		DiscrepancyStateOpenClaimable,
		DiscrepancyStateClaimed,
		DiscrepancyStateDisposing,
		DiscrepancyStatePendingVerify,
		DiscrepancyStateReopened,
	}
}

// faultConclusionAffecting returns the triggers that affect a recorded
// conclusion (Q5): everything except unrelated_write.
func faultConclusionAffecting() []InvalidationTrigger {
	return []InvalidationTrigger{
		InvalidationConcurrentWrite,
		InvalidationReorg,
		InvalidationNewEvidence,
		InvalidationSourceRotation,
		InvalidationVersionRotation,
	}
}

// faultConsistent builds a consistent reverify row observed at freshness.
func faultConsistent(freshness time.Time) *ReverifyEvidence {
	return &ReverifyEvidence{Verdict: ReverifyConsistent, EvidenceRef: "evidence-1", FreshnessAt: freshness}
}

// faultSignal builds one observed change signal with a traceable evidence ref.
func faultSignal(trigger InvalidationTrigger) *InvalidationSignal {
	return &InvalidationSignal{Trigger: trigger, EvidenceRef: "change-" + string(trigger)}
}

// faultCloseGuard builds a close guard evaluated at now with faultTolerance.
func faultCloseGuard(now time.Time, evidence *ReverifyEvidence) TransitionGuard {
	return TransitionGuard{
		LatestReverify:    evidence,
		CloseBasis:        faultBasis(),
		Now:               now,
		ReverifyTolerance: faultTolerance,
	}
}

// TestFaultT024ReorgInvalidationIsConservative is fault group 1 (reorg,
// quickstart §4; FR-017/SC-003): a chain reorganization touching the recorded
// block identity invalidates the closed item into the approved reverify flow,
// unrelated writes are ignored, and no signal can shortcut back to a
// conclusion.
func TestFaultT024ReorgInvalidationIsConservative(t *testing.T) {
	now := faultBase
	reorg := faultSignal(InvalidationReorg)

	// A reorg on a closed item moves it to pending_verify, exactly once.
	next, changed, err := EvaluateInvalidation(DiscrepancyStateClosed, reorg)
	if err != nil || !changed || next != DiscrepancyStatePendingVerify {
		t.Fatalf("EvaluateInvalidation(closed, reorg) = (%s, %v, %v), want (pending_verify, true, nil)", next, changed, err)
	}
	if err := ValidateDiscrepancyTransition(DiscrepancyStateClosed, DiscrepancyStatePendingVerify, TransitionGuard{Invalidation: reorg}); err != nil {
		t.Fatalf("guarded closed -> pending_verify with reorg refused: %v", err)
	}
	if !InvalidationReorg.AffectsConclusion() {
		t.Fatalf("reorg AffectsConclusion = false, want true")
	}

	// Unrelated writes never invalidate: the evaluator ignores them and the
	// guard refuses the edge instead of silently applying it.
	unrelated := faultSignal(InvalidationUnrelatedWrite)
	if next, changed, err := EvaluateInvalidation(DiscrepancyStateClosed, unrelated); err != nil || changed || next != DiscrepancyStateClosed {
		t.Errorf("EvaluateInvalidation(closed, unrelated_write) = (%s, %v, %v), want unchanged", next, changed, err)
	}
	if err := ValidateDiscrepancyTransition(DiscrepancyStateClosed, DiscrepancyStatePendingVerify, TransitionGuard{Invalidation: unrelated}); !errors.Is(err, ErrInvalidationIgnored) {
		t.Errorf("unrelated write guard err = %v, want ErrInvalidationIgnored", err)
	}

	// A missing signal is not a change: no invalidation without a recorded
	// conclusion-affecting trigger.
	if next, changed, err := EvaluateInvalidation(DiscrepancyStateClosed, nil); err != nil || changed || next != DiscrepancyStateClosed {
		t.Errorf("EvaluateInvalidation(closed, nil) = (%s, %v, %v), want unchanged", next, changed, err)
	}
	if err := ValidateDiscrepancyTransition(DiscrepancyStateClosed, DiscrepancyStatePendingVerify, TransitionGuard{}); !errors.Is(err, ErrInvalidationIgnored) {
		t.Errorf("missing signal guard err = %v, want ErrInvalidationIgnored", err)
	}

	// Unknown triggers are a contract error, never ignored and never applied.
	unknown := &InvalidationSignal{Trigger: InvalidationTrigger("orphan_block_guess"), EvidenceRef: "guess-1"}
	if _, _, err := EvaluateInvalidation(DiscrepancyStateClosed, unknown); !errors.Is(err, ErrContract) {
		t.Errorf("EvaluateInvalidation(unknown trigger) err = %v, want ErrContract", err)
	}
	if err := ValidateDiscrepancyTransition(DiscrepancyStateClosed, DiscrepancyStatePendingVerify, TransitionGuard{Invalidation: unknown}); !errors.Is(err, ErrContract) {
		t.Errorf("unknown trigger guard err = %v, want ErrContract", err)
	}

	// Only closed items invalidate: a reorg leaves every other state alone.
	for _, from := range faultNonClosedStates() {
		if next, changed, err := EvaluateInvalidation(from, reorg); err != nil || changed || next != from {
			t.Errorf("EvaluateInvalidation(%s, reorg) = (%s, %v, %v), want unchanged", from, next, changed, err)
		}
	}

	// After invalidation the pre-reorg result is out of tolerance: closing
	// again requires a NEW fresh consistent reverify, so no conclusion
	// survives the reorg.
	preReorg := faultConsistent(now.Add(-2 * faultTolerance))
	if err := ValidateDiscrepancyTransition(DiscrepancyStatePendingVerify, DiscrepancyStateClosed, faultCloseGuard(now, preReorg)); !errors.Is(err, ErrReverifyRequired) {
		t.Errorf("close with pre-reorg evidence err = %v, want ErrReverifyRequired", err)
	}

	// Re-driving the same reorg signal from the invalidated state is a no-op:
	// the fault path converges instead of oscillating, and it never reaches a
	// disposal state (invalidation ≠ auto-disposal).
	if next, changed, err := EvaluateInvalidation(DiscrepancyStatePendingVerify, reorg); err != nil || changed || next != DiscrepancyStatePendingVerify {
		t.Errorf("EvaluateInvalidation(pending_verify, reorg) = (%s, %v, %v), want unchanged", next, changed, err)
	}
	if CanTransitionDiscrepancy(DiscrepancyStateClosed, DiscrepancyStateDisposing) {
		t.Errorf("closed -> disposing is legal, want refused (reorg invalidation never auto-disposes)")
	}
}

// TestFaultT024UnknownReceiptNeverCloses is fault group 2 (unknown receipt /
// insufficient evidence, quickstart §3/§5; data-model.md §1.7; Q5-4): unknown,
// stale, divergent, evidence-less, zero-freshness and expired verdicts can
// never be recorded as consistent; a close over them is refused with
// ErrReverifyRequired and never falls through to another edge.
func TestFaultT024UnknownReceiptNeverCloses(t *testing.T) {
	now := faultBase
	refused := []struct {
		name string
		ev   *ReverifyEvidence
	}{
		{"nil evidence", nil},
		{"unknown verdict", &ReverifyEvidence{Verdict: ReverifyUnknown, EvidenceRef: "evidence-1", FreshnessAt: now}},
		{"stale verdict", &ReverifyEvidence{Verdict: ReverifyStale, EvidenceRef: "evidence-1", FreshnessAt: now}},
		{"divergent verdict", &ReverifyEvidence{Verdict: ReverifyDivergent, EvidenceRef: "evidence-1", FreshnessAt: now}},
		{"empty verdict", &ReverifyEvidence{Verdict: ReverifyVerdict(""), EvidenceRef: "evidence-1", FreshnessAt: now}},
		{"unrecognized verdict", &ReverifyEvidence{Verdict: ReverifyVerdict("insufficient_evidence"), EvidenceRef: "evidence-1", FreshnessAt: now}},
		{"consistent without evidence_ref", &ReverifyEvidence{Verdict: ReverifyConsistent, EvidenceRef: "", FreshnessAt: now}},
		{"consistent with blank evidence_ref", &ReverifyEvidence{Verdict: ReverifyConsistent, EvidenceRef: "   ", FreshnessAt: now}},
		{"consistent with zero freshness", &ReverifyEvidence{Verdict: ReverifyConsistent, EvidenceRef: "evidence-1"}},
		{"consistent beyond tolerance", &ReverifyEvidence{Verdict: ReverifyConsistent, EvidenceRef: "evidence-1", FreshnessAt: now.Add(-2 * faultTolerance)}},
		{"consistent from the future", &ReverifyEvidence{Verdict: ReverifyConsistent, EvidenceRef: "evidence-1", FreshnessAt: now.Add(time.Minute)}},
	}
	for _, tc := range refused {
		if tc.ev.CanClose(now, faultTolerance) {
			t.Errorf("%s: CanClose = true, want false (fail-closed)", tc.name)
		}
		if err := ValidateDiscrepancyTransition(DiscrepancyStatePendingVerify, DiscrepancyStateClosed, faultCloseGuard(now, tc.ev)); !errors.Is(err, ErrReverifyRequired) {
			t.Errorf("%s: close err = %v, want ErrReverifyRequired", tc.name, err)
		}
	}

	// An unrecognized verdict is not a consistency promotion.
	for _, raw := range []ReverifyVerdict{"", "unknown ", "UNKNOWN", "insufficient_evidence", "timeout", "consistentish"} {
		if ReverifyVerdict(raw).Valid() {
			t.Errorf("ReverifyVerdict(%q).Valid() = true, want false (closed vocabulary)", raw)
		}
	}

	// The refusal never falls through to a structural shortcut: disposed and
	// reopened rows cannot close even with fresh consistent evidence.
	fresh := faultConsistent(now)
	for _, from := range []DiscrepancyState{DiscrepancyStateDisposing, DiscrepancyStateReopened} {
		if err := ValidateDiscrepancyTransition(from, DiscrepancyStateClosed, faultCloseGuard(now, fresh)); !errors.Is(err, ErrIllegalDiscrepancyTransition) {
			t.Errorf("%s -> closed err = %v, want ErrIllegalDiscrepancyTransition", from, err)
		}
	}

	// A refused item stays conservatively pending_verify; unknown evidence is
	// never promoted by a later read of the same row.
	if next, changed, err := EvaluateInvalidation(DiscrepancyStatePendingVerify, nil); err != nil || changed || next != DiscrepancyStatePendingVerify {
		t.Errorf("EvaluateInvalidation(pending_verify, nil) = (%s, %v, %v), want unchanged", next, changed, err)
	}
	// Freshness is monotone: evidence out of tolerance now is never closable
	// at any later instant for the same row.
	outOfTolerance := faultConsistent(now.Add(-2 * faultTolerance))
	if outOfTolerance.CanClose(now.Add(time.Hour), faultTolerance) {
		t.Errorf("expired evidence CanClose = true at a later instant, want false")
	}
}

// TestFaultT024InterruptRestartConvergesConservatively is fault group 3
// (kill/restart interrupt, quickstart §7; SC-003/006): at guard level an
// interrupt cannot fabricate an invalidation or a partial close — leaving
// closed still requires a valid conclusion-affecting signal, a restarted
// close still requires fresh consistent evidence, signal re-drives are
// idempotent, and only the frozen conservative edges are reachable.
func TestFaultT024InterruptRestartConvergesConservatively(t *testing.T) {
	now := faultBase
	reorg := faultSignal(InvalidationReorg)

	// A kill between decision and commit cannot fabricate an invalidation:
	// signal-less, unrelated and unknown recoveries are all refused.
	if err := ValidateDiscrepancyTransition(DiscrepancyStateClosed, DiscrepancyStatePendingVerify, TransitionGuard{}); !errors.Is(err, ErrInvalidationIgnored) {
		t.Errorf("missing signal guard err = %v, want ErrInvalidationIgnored", err)
	}
	if err := ValidateDiscrepancyTransition(DiscrepancyStateClosed, DiscrepancyStatePendingVerify, TransitionGuard{Invalidation: faultSignal(InvalidationUnrelatedWrite)}); !errors.Is(err, ErrInvalidationIgnored) {
		t.Errorf("unrelated recovery signal err = %v, want ErrInvalidationIgnored", err)
	}
	if err := ValidateDiscrepancyTransition(DiscrepancyStateClosed, DiscrepancyStatePendingVerify, TransitionGuard{Invalidation: &InvalidationSignal{Trigger: InvalidationTrigger("replay_guess")}}); !errors.Is(err, ErrContract) {
		t.Errorf("unknown recovery signal err = %v, want ErrContract", err)
	}

	// A close interrupted before commit leaves the persisted pending_verify;
	// after restart the pre-interrupt result is out of tolerance and the
	// close is refused until a fresh consistent reverify exists. No partial
	// conclusion survives the restart.
	preInterrupt := faultConsistent(now.Add(-3 * faultTolerance))
	if err := ValidateDiscrepancyTransition(DiscrepancyStatePendingVerify, DiscrepancyStateClosed, faultCloseGuard(now, preInterrupt)); !errors.Is(err, ErrReverifyRequired) {
		t.Errorf("restart close with pre-interrupt evidence err = %v, want ErrReverifyRequired", err)
	}
	if err := ValidateDiscrepancyTransition(DiscrepancyStatePendingVerify, DiscrepancyStateClosed, faultCloseGuard(now, faultConsistent(now))); err != nil {
		t.Fatalf("restart close with fresh consistent evidence refused: %v", err)
	}

	// Re-driving the same fault after restart is safe: the evaluator moves
	// closed -> pending_verify once, then observes the already-invalidated
	// state and changes nothing (idempotent convergence, no double reopen).
	first, changed, err := EvaluateInvalidation(DiscrepancyStateClosed, reorg)
	if err != nil || !changed || first != DiscrepancyStatePendingVerify {
		t.Fatalf("EvaluateInvalidation(closed, reorg) = (%s, %v, %v), want (pending_verify, true, nil)", first, changed, err)
	}
	if second, changed, err := EvaluateInvalidation(first, reorg); err != nil || changed || second != first {
		t.Errorf("EvaluateInvalidation(pending_verify, reorg) = (%s, %v, %v), want unchanged", second, changed, err)
	}
	// The retried guard edge is still accepted, so a crash between the
	// decision and the commit can be replayed.
	if err := ValidateDiscrepancyTransition(DiscrepancyStateClosed, DiscrepancyStatePendingVerify, TransitionGuard{Invalidation: reorg}); err != nil {
		t.Errorf("replayed invalidation guard refused: %v", err)
	}

	// After a restart only the frozen conservative edges are reachable:
	// pending_verify is the only predecessor of closed, and no conclusion
	// state can reach a claim or disposal state (no auto-disposal after a
	// fault).
	for _, from := range faultAllStates() {
		for _, to := range faultAllStates() {
			if !CanTransitionDiscrepancy(from, to) {
				continue
			}
			if to == DiscrepancyStateClosed && from != DiscrepancyStatePendingVerify {
				t.Errorf("legal edge %s -> closed, want only pending_verify -> closed", from)
			}
		}
	}
	for _, from := range []DiscrepancyState{DiscrepancyStateClosed, DiscrepancyStatePendingVerify, DiscrepancyStateReopened} {
		if CanTransitionDiscrepancy(from, DiscrepancyStateDisposing) {
			t.Errorf("%s -> disposing is legal, want refused (no auto-disposal)", from)
		}
		if CanTransitionDiscrepancy(from, DiscrepancyStateClaimed) {
			t.Errorf("%s -> claimed is legal, want refused", from)
		}
	}

	// Unknown persisted states (a torn or partial write) fail closed with a
	// contract error, never a silent pass.
	if err := ValidateDiscrepancyTransition(DiscrepancyState(""), DiscrepancyStateClosed, faultCloseGuard(now, faultConsistent(now))); !errors.Is(err, ErrContract) {
		t.Errorf("empty source state err = %v, want ErrContract", err)
	}
	if err := ValidateDiscrepancyTransition(DiscrepancyState("interrupted"), DiscrepancyStatePendingVerify, TransitionGuard{Invalidation: reorg}); !errors.Is(err, ErrContract) {
		t.Errorf("torn source state err = %v, want ErrContract", err)
	}
}

// TestFaultT024ConcurrentUpdatesInvalidateClosedOnly is fault group 4
// (concurrent updates, quickstart §5; FR-010/017/018; data-model.md §3):
// conclusion-affecting concurrent writes, new evidence and source/version
// rotation move a closed item into pending_verify and never into disposal or
// closure; confirmed recurrence reopens the original item from closed or
// pending_verify; unconfirmed reports do nothing.
func TestFaultT024ConcurrentUpdatesInvalidateClosedOnly(t *testing.T) {
	now := faultBase

	for _, trigger := range faultConclusionAffecting() {
		signal := faultSignal(trigger)

		// Closed -> pending_verify, through both the evaluator and the guard.
		next, changed, err := EvaluateInvalidation(DiscrepancyStateClosed, signal)
		if err != nil || !changed || next != DiscrepancyStatePendingVerify {
			t.Errorf("EvaluateInvalidation(closed, %s) = (%s, %v, %v), want (pending_verify, true, nil)", trigger, next, changed, err)
		}
		if err := ValidateDiscrepancyTransition(DiscrepancyStateClosed, DiscrepancyStatePendingVerify, TransitionGuard{Invalidation: signal}); err != nil {
			t.Errorf("%s closed -> pending_verify refused: %v", trigger, err)
		}
		// Rotation is reverify-only: it never maps to a disposal or close edge.
		if next == DiscrepancyStateDisposing || next == DiscrepancyStateClosed {
			t.Errorf("%s landed on %s, want pending_verify only", trigger, next)
		}

		// The same concurrent change does not disturb any non-closed state.
		for _, from := range faultNonClosedStates() {
			if next, changed, err := EvaluateInvalidation(from, signal); err != nil || changed || next != from {
				t.Errorf("EvaluateInvalidation(%s, %s) = (%s, %v, %v), want unchanged", from, trigger, next, changed, err)
			}
		}
	}

	// A concurrent change never closes directly: the evidence that was fresh
	// when the write was observed is out of tolerance by close time, and the
	// close edge requires a fresh consistent reverify.
	atWrite := faultConsistent(now.Add(-2 * faultTolerance))
	if err := ValidateDiscrepancyTransition(DiscrepancyStatePendingVerify, DiscrepancyStateClosed, faultCloseGuard(now, atWrite)); !errors.Is(err, ErrReverifyRequired) {
		t.Errorf("close after concurrent change err = %v, want ErrReverifyRequired", err)
	}
	// Global invariant: pending_verify is the only predecessor of closed.
	for _, from := range faultAllStates() {
		if from != DiscrepancyStatePendingVerify && CanTransitionDiscrepancy(from, DiscrepancyStateClosed) {
			t.Errorf("legal edge %s -> closed, want only pending_verify -> closed", from)
		}
	}

	// Confirmed recurrence reopens the original item from closed or
	// pending_verify (reopen_count+1 upstream, history retained); re-driving a
	// confirmed recurrence on the reopened item changes nothing.
	for _, from := range []DiscrepancyState{DiscrepancyStateClosed, DiscrepancyStatePendingVerify} {
		next, changed := EvaluateRecurrence(from, true)
		if !changed || next != DiscrepancyStateReopened {
			t.Errorf("EvaluateRecurrence(%s, confirmed) = (%s, %v), want (reopened, true)", from, next, changed)
		}
		if again, changedAgain := EvaluateRecurrence(next, true); changedAgain || again != next {
			t.Errorf("EvaluateRecurrence(reopened, confirmed) = (%s, %v), want unchanged", again, changedAgain)
		}
	}
	// Unconfirmed reports are not recurrence, from any state.
	for _, from := range faultAllStates() {
		if next, changed := EvaluateRecurrence(from, false); changed || next != from {
			t.Errorf("EvaluateRecurrence(%s, unconfirmed) = (%s, %v), want unchanged", from, next, changed)
		}
	}
	// The two conservative lanes stay distinct: invalidation never reopens,
	// recurrence never closes, and reopen still requires confirmation.
	if err := ValidateDiscrepancyTransition(DiscrepancyStateClosed, DiscrepancyStateReopened, TransitionGuard{}); !errors.Is(err, ErrRecurrenceUnconfirmed) {
		t.Errorf("unconfirmed reopen err = %v, want ErrRecurrenceUnconfirmed", err)
	}
	if err := ValidateDiscrepancyTransition(DiscrepancyStateClosed, DiscrepancyStateReopened, TransitionGuard{ConfirmedRecurrence: true}); err != nil {
		t.Errorf("confirmed reopen from closed refused: %v", err)
	}
	if next, _, err := EvaluateInvalidation(DiscrepancyStateClosed, faultSignal(InvalidationConcurrentWrite)); err != nil || next == DiscrepancyStateReopened {
		t.Errorf("invalidation landed on %s, want pending_verify only (never reopened)", next)
	}

	// A reopened item re-enters the approved reverify flow and can never jump
	// straight back to closed.
	if err := ValidateDiscrepancyTransition(DiscrepancyStateReopened, DiscrepancyStatePendingVerify, TransitionGuard{}); err != nil {
		t.Errorf("reopened -> pending_verify refused: %v", err)
	}
	if err := ValidateDiscrepancyTransition(DiscrepancyStateReopened, DiscrepancyStateClosed, faultCloseGuard(now, faultConsistent(now))); !errors.Is(err, ErrIllegalDiscrepancyTransition) {
		t.Errorf("reopened -> closed err = %v, want ErrIllegalDiscrepancyTransition", err)
	}
}

// TestFaultT024StaleResultCloseNeverCloses pins the stale-result rule
// (quickstart §5; data-model.md §1.7/§3): a consistent verdict older than the
// tolerance can never close, no matter how valid the close_basis is. The
// boundary is pinned so a later refactor cannot loosen it.
func TestFaultT024StaleResultCloseNeverCloses(t *testing.T) {
	now := faultBase

	// Exactly at the tolerance is the oldest closable result; one nanosecond
	// older is already stale.
	atBoundary := faultConsistent(now.Add(-faultTolerance))
	if !atBoundary.CanClose(now, faultTolerance) {
		t.Fatalf("evidence exactly at tolerance CanClose = false, want true")
	}
	if err := ValidateDiscrepancyTransition(DiscrepancyStatePendingVerify, DiscrepancyStateClosed, faultCloseGuard(now, atBoundary)); err != nil {
		t.Fatalf("close at the freshness boundary refused: %v", err)
	}
	oneNanoStale := faultConsistent(now.Add(-faultTolerance - time.Nanosecond))
	if oneNanoStale.CanClose(now, faultTolerance) {
		t.Errorf("evidence one nanosecond past tolerance CanClose = true, want false")
	}
	if err := ValidateDiscrepancyTransition(DiscrepancyStatePendingVerify, DiscrepancyStateClosed, faultCloseGuard(now, oneNanoStale)); !errors.Is(err, ErrReverifyRequired) {
		t.Errorf("one-nanosecond-stale close err = %v, want ErrReverifyRequired", err)
	}

	// The evidence gate dominates the close basis: a fully valid snapshot
	// cannot carry a stale result into closed.
	staleWithBasis := TransitionGuard{
		LatestReverify:    oneNanoStale,
		CloseBasis:        faultBasis(),
		Now:               now,
		ReverifyTolerance: faultTolerance,
	}
	if err := ValidateDiscrepancyTransition(DiscrepancyStatePendingVerify, DiscrepancyStateClosed, staleWithBasis); !errors.Is(err, ErrReverifyRequired) {
		t.Errorf("stale close with valid basis err = %v, want ErrReverifyRequired", err)
	}

	// An unstated (zero or negative) freshness bound can never be satisfied.
	for _, tolerance := range []time.Duration{0, -time.Second} {
		guard := TransitionGuard{
			LatestReverify:    faultConsistent(now),
			CloseBasis:        faultBasis(),
			Now:               now,
			ReverifyTolerance: tolerance,
		}
		if err := ValidateDiscrepancyTransition(DiscrepancyStatePendingVerify, DiscrepancyStateClosed, guard); !errors.Is(err, ErrReverifyRequired) {
			t.Errorf("close with tolerance %v err = %v, want ErrReverifyRequired", tolerance, err)
		}
	}

	// Staleness is permanent for a recorded row: evaluating the same evidence
	// later only ages it further, so it can never turn closable.
	if oneNanoStale.CanClose(now.Add(faultTolerance), faultTolerance) {
		t.Errorf("stale evidence CanClose = true at a later instant, want false")
	}

	// Guard evaluation at a fixed guarded instant is deterministic: repeated
	// validation of the same stale row refuses identically (no flapping into
	// a conclusion).
	for i := 0; i < 2; i++ {
		if err := ValidateDiscrepancyTransition(DiscrepancyStatePendingVerify, DiscrepancyStateClosed, faultCloseGuard(now, oneNanoStale)); !errors.Is(err, ErrReverifyRequired) {
			t.Errorf("repeated stale close %d err = %v, want ErrReverifyRequired", i, err)
		}
	}
}

// TestFaultT024ConservativeConvergenceMatrix is the cross-group capstone: no
// fault signal or recurrence path can ever change a state outside the defined
// conservative set (zero wrong payments: the fault layer never reaches a claim
// or disposal state, and closure stays fully evidence-gated), every refusal
// leaves an approved way forward (zero permanent false conclusions), and
// unknown inputs fail closed.
func TestFaultT024ConservativeConvergenceMatrix(t *testing.T) {
	now := faultBase

	// Invalidation over the whole state × trigger matrix: the only change a
	// signal may ever produce is closed -> pending_verify; unknown triggers
	// are contract errors from every state.
	for _, from := range faultAllStates() {
		for _, trigger := range append(faultConclusionAffecting(), InvalidationUnrelatedWrite) {
			next, changed, err := EvaluateInvalidation(from, faultSignal(trigger))
			if err != nil {
				t.Errorf("EvaluateInvalidation(%s, %s) err = %v, want nil", from, trigger, err)
				continue
			}
			if changed && next != DiscrepancyStatePendingVerify {
				t.Errorf("EvaluateInvalidation(%s, %s) changed to %s, want pending_verify only", from, trigger, next)
			}
			if !changed && next != from {
				t.Errorf("EvaluateInvalidation(%s, %s) = %s, want unchanged", from, trigger, next)
			}
		}
		if next, changed, err := EvaluateInvalidation(from, nil); err != nil || changed || next != from {
			t.Errorf("EvaluateInvalidation(%s, nil) = (%s, %v, %v), want unchanged", from, next, changed, err)
		}
		if _, _, err := EvaluateInvalidation(from, &InvalidationSignal{Trigger: InvalidationTrigger("forged")}); !errors.Is(err, ErrContract) {
			t.Errorf("EvaluateInvalidation(%s, forged trigger) err = %v, want ErrContract", from, err)
		}
	}

	// A forged source state is never treated as closed and guarded transitions
	// from it fail closed.
	forged := DiscrepancyState("forged")
	if _, changed, err := EvaluateInvalidation(forged, faultSignal(InvalidationReorg)); err != nil || changed {
		t.Errorf("EvaluateInvalidation(forged, reorg) = (%s, %v), want unchanged, no error", forged, changed)
	}
	for _, to := range faultAllStates() {
		if err := ValidateDiscrepancyTransition(forged, to, TransitionGuard{ClaimOwner: "apikey:1001"}); !errors.Is(err, ErrContract) {
			t.Errorf("forged source -> %s err = %v, want ErrContract", to, err)
		}
	}

	// Recurrence over the whole matrix: only closed/pending_verify may change,
	// only to reopened, and only when confirmed.
	for _, from := range faultAllStates() {
		if next, changed := EvaluateRecurrence(from, true); changed && next != DiscrepancyStateReopened {
			t.Errorf("EvaluateRecurrence(%s, confirmed) = %s, want reopened only", from, next)
		}
		if next, changed := EvaluateRecurrence(from, false); changed || next != from {
			t.Errorf("EvaluateRecurrence(%s, unconfirmed) = (%s, %v), want unchanged", from, next, changed)
		}
	}

	// Convergence, not dead ends: every conservative state has an approved
	// way forward and an approved way back.
	if err := ValidateDiscrepancyTransition(DiscrepancyStateClosed, DiscrepancyStatePendingVerify, TransitionGuard{Invalidation: faultSignal(InvalidationReorg)}); err != nil {
		t.Errorf("closed -> pending_verify refused: %v", err)
	}
	if err := ValidateDiscrepancyTransition(DiscrepancyStatePendingVerify, DiscrepancyStateClosed, faultCloseGuard(now, faultConsistent(now))); err != nil {
		t.Errorf("pending_verify -> closed with fresh consistent evidence refused: %v", err)
	}
	if err := ValidateDiscrepancyTransition(DiscrepancyStatePendingVerify, DiscrepancyStateReopened, TransitionGuard{ConfirmedRecurrence: true}); err != nil {
		t.Errorf("pending_verify -> reopened with confirmed recurrence refused: %v", err)
	}
	if err := ValidateDiscrepancyTransition(DiscrepancyStateReopened, DiscrepancyStatePendingVerify, TransitionGuard{}); err != nil {
		t.Errorf("reopened -> pending_verify refused: %v", err)
	}
}
