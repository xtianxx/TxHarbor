//go:build linux && drill

// borrowed-replacement-owner-postcommit-loss_linux_test.go is the bounded
// acknowledged-COMMIT -> late P1 loss -> terminal refusal lane. It composes the
// existing machinery without duplicating any transaction or observer path: the
// genuine autonomous baseline/capture and guard-row preamble, the recycled
// actual-refusal helper, and the existing observed journal transaction with the
// exact guard-row fence, the actual refused insertion, supplied-tx readback,
// pending invisibility and the structured contender 55P03. The unchanged
// owner-tail arbitration grants the provisional eligibility and the owner
// transaction returns with an acknowledged COMMIT (ownerErr == nil); the lane
// then injects an ACTUAL loss through the EXISTING post-return adapter hook:
// it verifies the exact registered P1 incarnation (PID, backend_start, role and
// database) through a separately owned bounded connection, terminates ONLY that
// incarnation, awaits the REAL observer/pump loss acknowledgement, and returns
// nil so no fabricated adapter error can establish the refusal. The existing
// final decision then refuses the terminal publication because of the actual
// latched loss, and every assertion is made AFTER the actual completion:
// acknowledged COMMIT preserved, actual termination and real loss
// acknowledged, no composite, replay/copy/reconstruction refused, exactly ONE
// durable refused non-instance-bound journal row, unchanged and independently
// lockable guard, acceptance zero, owner health through an independent bounded
// context, and Use/observer/pump completion proven before dependency cleanup.
// There is no continuous exclusion, dirty-guard admission, restore, immutable
// instance binding, manifest, atomic acceptance, T057/T058, downstream or Gate1
// authority; journal rows are never deleted or compensated; secrets, verifiers
// and DSNs are never logged.
package recovery_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// borrowedReplacementOwnerPostcommitLossProveCompletion requires the complete
// orchestration completion trace BEFORE any replay or cleanup: no flow
// termination uncertainty, no unexpected orchestration/join error, the ACTUAL
// Use-completion channel closed and a successful bounded pump completion.
func borrowedReplacementOwnerPostcommitLossProveCompletion(t *testing.T, outcome *borrowedReplacementOwnerTailOutcome) {
	t.Helper()
	if outcome == nil || outcome.flow == nil {
		t.Fatal("postcommit-loss completion proof requires the concrete flow outcome")
	}
	if outcome.flow.terminationUnknown {
		t.Fatal("postcommit-loss flow termination is UNKNOWN")
	}
	if outcome.runErr != nil {
		t.Fatalf("postcommit-loss flow returned an unexpected orchestration/join error: %v", outcome.runErr)
	}
	select {
	case <-outcome.flow.useCompletion:
	default:
		t.Fatal("postcommit-loss Use-completion channel was not closed")
	}
	if outcome.pumpJoinErr != nil {
		t.Fatalf("postcommit-loss pump join was UNKNOWN: %v", outcome.pumpJoinErr)
	}
}

// TestBorrowedReplacementOwnerPostcommitLoss is the bounded acknowledged-COMMIT
// late-loss lane described in the file header.
func TestBorrowedReplacementOwnerPostcommitLoss(t *testing.T) {
	ctx := t.Context()
	var probeCalls, acceptanceCalls int32

	// P: acknowledged COMMIT survives, a late ACTUAL P1 loss blocks the
	// terminal publication despite the prior provisional eligibility.
	func() {
		b, fresh := borrowedReplacementGuardRowFixtures(t, ctx)
		beforeDisposition, beforeOperation := borrowedReplacementGuardRowRead(t, ctx, b.fixture.controlPool, b.fixture.guardKey)
		attemptID := borrowedReplacementOwnerJournalActualRefusal(t, ctx, b, fresh, "postcommit-loss", &probeCalls, &acceptanceCalls)
		facts := &borrowedReplacementOwnerJournalFacts{}
		var lossInjected atomic.Bool
		outcome := borrowedReplacementOwnerJournalRun(ctx, t, b, fresh, &borrowedReplacementOwnerJournalStage{
			key: b.fixture.guardKey, expectedOperation: b.fixture.operation,
			insert: true, attemptID: attemptID, contenderProof: true,
		}, facts, &borrowedReplacementOwnerTailHooks{
			adapter: func(_ error, pump *borrowedReplacementAutonomousPump, _ func()) error {
				// REQUIRE an initially CLEAR loss state at injection: a
				// pre-existing unrelated failure must never take credit.
				if reason := pump.lossReasonNow(); reason != "" {
					t.Fatalf("postcommit-loss injection requires an initially clear loss state, got %q", reason)
				}
				if rawErr, completed := pump.checkpointResultNow(); completed && rawErr != nil {
					t.Fatalf("postcommit-loss injection requires no prior failing checkpoint, got %v", rawErr)
				}
				reg := pump.observer.reg
				// The termination statement ITSELF is predicated on the COMPLETE
				// incarnation identity (PID + backend_start + role + database)
				// through a separately owned bounded connection; no matching row
				// refuses as a no-row error.
				var terminated bool
				termCtx, cancelTerm := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
				termErr := b.fixture.controlPool.QueryRow(termCtx, `
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE pid = $1 AND backend_start = $2 AND usename = $3 AND datname = $4`,
					reg.state.backendPID, reg.state.backendStart, b.fixture.writerRole, b.fixture.targetDB).Scan(&terminated)
				cancelTerm()
				if termErr != nil {
					t.Fatalf("postcommit-loss complete-identity termination refused (no exactly-one matching row): %v", termErr)
				}
				if !terminated {
					t.Fatal("postcommit-loss complete-identity termination did not signal success")
				}
				select {
				case <-pump.lossAcked:
				case <-time.After(30 * time.Second):
					t.Fatal("postcommit-loss real observer/pump loss was not acknowledged")
				}
				// Credit ONLY a genuinely completed FAILED native observation
				// caused by the termination: the observer's census association
				// against the terminated registered incarnation. Reject a
				// generic acknowledgement, scheduler stall, protocol violation
				// or cancellation-only classification.
				lossReason := pump.lossReasonNow()
				if lossReason == "" {
					t.Fatal("postcommit-loss loss was not latched after the termination")
				}
				if !strings.Contains(lossReason, "strict census association refused") {
					t.Fatalf("postcommit-loss was not a genuinely completed failed native observation: %q", lossReason)
				}
				for _, nonObservation := range []string{
					"terminated before the interval seal",
					"stall hook",
					"progress stalled",
					"protocol violation",
					"nil success after cancellation",
				} {
					if strings.Contains(lossReason, nonObservation) {
						t.Fatalf("postcommit-loss credited a non-observation failure %q: %q", nonObservation, lossReason)
					}
				}
				lossInjected.Store(true)
				// RETURN NIL: no fabricated adapter error may establish the
				// refusal; only the REAL latched loss may.
				return nil
			},
		})
		if !lossInjected.Load() || !facts.inserted || facts.auditID <= 0 {
			t.Fatalf("postcommit-loss control: injected=%t inserted=%t auditID=%d", lossInjected.Load(), facts.inserted, facts.auditID)
		}
		// Credit requires the RETURNED ownerErr == nil: the COMMIT was
		// acknowledged, not merely independently visible.
		if outcome.flow.ownerErr != nil {
			t.Fatalf("postcommit-loss COMMIT was not acknowledged: %v", outcome.flow.ownerErr)
		}
		if outcome.pump == nil || outcome.pump.lossReasonNow() == "" {
			t.Fatal("postcommit-loss real observation loss was not latched")
		}
		if outcome.flow.state.compositePublished() {
			t.Fatal("postcommit-loss latched loss still published the composite")
		}
		if outcome.flow.decisionErr == nil {
			t.Fatal("postcommit-loss terminal decision did not refuse")
		}
		if !outcome.flow.state.lostNow() {
			t.Fatal("postcommit-loss terminal state lost flag is not set")
		}
		// The ACTUAL P1 termination is proven by the strict incarnation
		// disappearance check.
		borrowedAuthStagingAwaitDisappearance(ctx, t, b.fixture.fx.containerID, outcome.flow.reg.state.backendPID, outcome.flow.reg.state.osStart, borrowedAuthStagingDisappearanceBudget, true)
		// Exactly ONE durable refused non-instance-bound journal row.
		if count := borrowedReplacementOwnerJournalCount(t, ctx, b.fixture.controlPool, attemptID); count != 1 {
			t.Fatalf("postcommit-loss durable journal rows=%d, want exactly 1", count)
		}
		row, rowErr := borrowedReplacementOwnerJournalRead(ctx, b.fixture.controlPool, facts.auditID)
		if rowErr != nil {
			t.Fatalf("postcommit-loss durable row read refused: %v", rowErr)
		}
		if row.result != "refused" || row.instanceID != "" || row.operationID != attemptID {
			t.Fatalf("postcommit-loss durable row mismatch: result=%q instance=%q operation=%q", row.result, row.instanceID, row.operationID)
		}
		// Unchanged, unresolved and independently lockable guard.
		postDisposition, postOperation := borrowedReplacementGuardRowRead(t, ctx, b.fixture.controlPool, b.fixture.guardKey)
		if postDisposition != beforeDisposition || postOperation != beforeOperation {
			t.Fatal("postcommit-loss changed the guard contents")
		}
		if _, _, contenderErr := borrowedReplacementGuardRowContenderLock(ctx, b.fixture.controlPool, b.fixture.guardKey); contenderErr != nil {
			t.Fatalf("postcommit-loss released guard row was not lockable: %v", contenderErr)
		}
		borrowedOwnerDDLGuard(t, ctx, b)
		// Complete orchestration completion proven BEFORE replay/cleanup.
		borrowedReplacementOwnerPostcommitLossProveCompletion(t, outcome)
		// Provisional eligibility may have been granted but is never reusable
		// authority.
		if !outcome.state.consumeEligibility() {
			t.Fatal("postcommit-loss provisional eligibility was not consumable once")
		}
		borrowedReplacementOwnerTailReplayRefuse(t, ctx, b, fresh, outcome, "postcommit-loss")
		// Owner health through an independent bounded context.
		healthCtx, cancelHealth := context.WithTimeout(context.Background(), borrowedOwnerRotationHealthBudget)
		healthErr := b.fixture.lock.Health(healthCtx)
		cancelHealth()
		if healthErr != nil {
			t.Fatalf("postcommit-loss original owner health refused: %v", healthErr)
		}
		if _, recorded := outcome.state.useErrNow(); !recorded {
			t.Fatal("postcommit-loss use completion was not channel-confirmed")
		}
		if got := outcome.state.tailInvocationsNow(); got != 1 {
			t.Fatalf("postcommit-loss tail invocations=%d, want exactly 1", got)
		}
		borrowedSuccessorCloseConn(t, outcome.flow.conn)
		t.Logf("postcommit-loss positive: the owner transaction returned with an acknowledged COMMIT (ownerErr==nil), the ACTUAL registered P1 incarnation was terminated by a complete-identity-predicated statement (PID+backend_start+role+database) requiring exactly one matching successful result, and a genuinely completed FAILED native checkpoint observation was recorded and asserted (not a generic loss acknowledgement), the terminal publication refused despite the prior provisional eligibility (no composite), exactly ONE durable refused non-instance-bound journal row remained, the guard was unchanged/unresolved/independently lockable, replay/copy/reconstruction refused, acceptance was zero, the owner stayed healthy under an independent bounded context and completion was proven before cleanup")
	}()

	// N1: no-loss control: acknowledged COMMIT, one durable refused row, the
	// diagnostic composite publication, acceptance zero.
	func() {
		b, fresh := borrowedReplacementGuardRowFixtures(t, ctx)
		beforeDisposition, beforeOperation := borrowedReplacementGuardRowRead(t, ctx, b.fixture.controlPool, b.fixture.guardKey)
		attemptID := borrowedReplacementOwnerJournalActualRefusal(t, ctx, b, fresh, "postcommit-noloss", &probeCalls, &acceptanceCalls)
		facts := &borrowedReplacementOwnerJournalFacts{}
		outcome := borrowedReplacementOwnerJournalRun(ctx, t, b, fresh, &borrowedReplacementOwnerJournalStage{
			key: b.fixture.guardKey, expectedOperation: b.fixture.operation,
			insert: true, attemptID: attemptID, contenderProof: true,
		}, facts, &borrowedReplacementOwnerTailHooks{})
		if outcome.runErr != nil || outcome.flow.ownerErr != nil {
			t.Fatalf("postcommit-noloss flow refused: runErr=%v ownerErr=%v", outcome.runErr, outcome.flow.ownerErr)
		}
		if outcome.pump == nil || outcome.pump.lossReasonNow() != "" {
			t.Fatal("postcommit-noloss control latched an unexpected loss")
		}
		if !outcome.flow.state.compositePublished() || outcome.flow.state.lostNow() {
			t.Fatalf("postcommit-noloss terminal state: composite=%t lost=%t", outcome.flow.state.compositePublished(), outcome.flow.state.lostNow())
		}
		if !facts.inserted || facts.auditID <= 0 {
			t.Fatal("postcommit-noloss control did not insert the refused row")
		}
		if count := borrowedReplacementOwnerJournalCount(t, ctx, b.fixture.controlPool, attemptID); count != 1 {
			t.Fatalf("postcommit-noloss durable journal rows=%d, want exactly 1", count)
		}
		postDisposition, postOperation := borrowedReplacementGuardRowRead(t, ctx, b.fixture.controlPool, b.fixture.guardKey)
		if postDisposition != beforeDisposition || postOperation != beforeOperation {
			t.Fatal("postcommit-noloss control changed the guard contents")
		}
		if !outcome.state.consumeEligibility() {
			t.Fatal("postcommit-noloss provisional eligibility was not consumable once")
		}
		borrowedReplacementOwnerTailReplayRefuse(t, ctx, b, fresh, outcome, "postcommit-noloss")
		if err := borrowedReplacementOwnerTailRetireGuarded(t, ctx, b, outcome); err != nil {
			t.Fatalf("postcommit-noloss guarded retirement refused: %v", err)
		}
		borrowedOwnerDDLGuard(t, ctx, b)
		healthCtx, cancelHealth := context.WithTimeout(context.Background(), borrowedOwnerRotationHealthBudget)
		healthErr := b.fixture.lock.Health(healthCtx)
		cancelHealth()
		if healthErr != nil {
			t.Fatalf("postcommit-noloss owner health refused: %v", healthErr)
		}
		t.Logf("postcommit-noloss control: the acknowledged COMMIT left exactly ONE durable refused row and the diagnostic composite publication (never acceptance authority), the guard stayed unchanged and unresolved, replay/copy refused, acceptance was zero and guarded retirement ran; the composite is diagnostic only")
	}()

	// N2: late caller cancellation from the same post-return adapter WITHOUT
	// terminating the P1: the publication refuses, the committed journal
	// survives and the owner stays independently healthy.
	func() {
		b, fresh := borrowedReplacementGuardRowFixtures(t, ctx)
		beforeDisposition, beforeOperation := borrowedReplacementGuardRowRead(t, ctx, b.fixture.controlPool, b.fixture.guardKey)
		attemptID := borrowedReplacementOwnerJournalActualRefusal(t, ctx, b, fresh, "postcommit-cancel", &probeCalls, &acceptanceCalls)
		facts := &borrowedReplacementOwnerJournalFacts{}
		var canceled atomic.Bool
		outcome := borrowedReplacementOwnerJournalRun(ctx, t, b, fresh, &borrowedReplacementOwnerJournalStage{
			key: b.fixture.guardKey, expectedOperation: b.fixture.operation,
			insert: true, attemptID: attemptID, contenderProof: true,
		}, facts, &borrowedReplacementOwnerTailHooks{
			adapter: func(_ error, _ *borrowedReplacementAutonomousPump, cancelLane func()) error {
				if cancelLane == nil {
					t.Fatal("postcommit-cancel adapter has no lane cancel")
				}
				canceled.Store(true)
				cancelLane()
				return nil
			},
		})
		if !canceled.Load() || !facts.inserted {
			t.Fatalf("postcommit-cancel control canceled=%t inserted=%t", canceled.Load(), facts.inserted)
		}
		if outcome.flow.ownerErr != nil {
			t.Fatalf("postcommit-cancel COMMIT was not acknowledged: %v", outcome.flow.ownerErr)
		}
		// The cancellation itself classifies the pump's own pre-seal stop
		// through the shared loss latch (expected); what must NOT happen is the
		// P1 termination, which is asserted by the presence check below.
		if outcome.flow.state.compositePublished() {
			t.Fatal("postcommit-cancel published the composite")
		}
		if outcome.flow.decisionErr == nil {
			t.Fatal("postcommit-cancel terminal decision did not refuse")
		}
		if count := borrowedReplacementOwnerJournalCount(t, ctx, b.fixture.controlPool, attemptID); count != 1 {
			t.Fatalf("postcommit-cancel durable journal rows=%d, want exactly 1", count)
		}
		// The P1 was NOT terminated: the exact registered incarnation is still
		// present.
		var present int
		presenceCtx, cancelPresence := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
		presenceErr := b.fixture.controlPool.QueryRow(presenceCtx, `
SELECT count(*)::int FROM pg_stat_activity
WHERE pid = $1 AND backend_start = $2 AND usename = $3 AND datname = $4`,
			outcome.flow.reg.state.backendPID, outcome.flow.reg.state.backendStart, b.fixture.writerRole, b.fixture.targetDB).Scan(&present)
		cancelPresence()
		if presenceErr != nil || present != 1 {
			t.Fatalf("postcommit-cancel control changed the registered P1 incarnation presence: present=%d err=%v", present, presenceErr)
		}
		postDisposition, postOperation := borrowedReplacementGuardRowRead(t, ctx, b.fixture.controlPool, b.fixture.guardKey)
		if postDisposition != beforeDisposition || postOperation != beforeOperation {
			t.Fatal("postcommit-cancel control changed the guard contents")
		}
		// Complete orchestration completion proven BEFORE replay/cleanup.
		borrowedReplacementOwnerPostcommitLossProveCompletion(t, outcome)
		if !outcome.state.consumeEligibility() {
			t.Fatal("postcommit-cancel provisional eligibility was not consumable once")
		}
		borrowedReplacementOwnerTailReplayRefuse(t, ctx, b, fresh, outcome, "postcommit-cancel")
		healthCtx, cancelHealth := context.WithTimeout(context.Background(), borrowedOwnerRotationHealthBudget)
		healthErr := b.fixture.lock.Health(healthCtx)
		cancelHealth()
		if healthErr != nil {
			t.Fatalf("postcommit-cancel owner health refused: %v", healthErr)
		}
		borrowedSuccessorCloseConn(t, outcome.flow.conn)
		t.Logf("postcommit-cancel control: the acknowledged COMMIT survived, the caller cancellation from the same post-return adapter (WITHOUT terminating the P1) refused the terminal publication, the one committed refused journal row survived, the complete-identity presence predicate still matched exactly one registered P1 incarnation, the owner stayed independently healthy and no reusable provisional authority remained")
	}()

	if got := atomic.LoadInt32(&probeCalls); got != 0 {
		t.Fatalf("postcommit-loss lane ran the ordinary probe %d times, want 0", got)
	}
	if got := atomic.LoadInt32(&acceptanceCalls); got != 0 {
		t.Fatalf("postcommit-loss lane ran ordinary acceptance %d times, want 0", got)
	}
}
