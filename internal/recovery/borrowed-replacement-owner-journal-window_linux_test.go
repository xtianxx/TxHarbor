//go:build linux && drill

// borrowed-replacement-owner-journal-window_linux_test.go is the bounded
// same-owner non-authorizing journal commit/rollback lane. It composes the
// existing machinery without duplicating any transaction or observer path: the
// genuine autonomous baseline/capture and the guard-row preamble (original
// canonical key, operation provenance, deliberately unresolved guard), the
// reused owner-tail registration/tail steps, the unchanged observed owner
// window and the unchanged post-COMMIT checks/joins. This lane adds ONE
// new-file-local journal stage: inside the owner-tail atTail injection, after
// the exact guard-row fence (SELECT ... FOR UPDATE NOWAIT through ONLY the
// supplied transaction), it appends ONE uniquely identifiable refused audit row
// to the existing migrated recovery_audit table through the production fixed
// helper (RETURNING audit_id), with a NULL/empty instance_id (non-instance-
// bound), no secrets and no purported instance-bound authority. The inserted
// row is read back through the same supplied transaction and MUST stay
// invisible to an independent bounded control-store read until COMMIT; the
// same-row contender keeps the structured 55P03 proof. The unchanged final
// facts verification and shared-loss arbitration then decide; cancellation and
// actual observer loss are injected at atArbitration AFTER the insertion and
// the successful facts check. After an acknowledged COMMIT exactly one matching
// journal row is verified independently; a confirmed pre-COMMIT callback
// failure with successful rollback must leave ZERO matching rows; the guard
// contents stay unchanged, the guard stays unresolved and acceptance is zero.
// The terminal composite never treats journal existence as acceptance, the
// audit row is never deleted, compensated or used as authority, and a refusal
// never yields a reusable provisional result. There is no admission, restore,
// restore probe, generation advance, clean transition, capability, atomic
// acceptance, manifest, downstream or Gate1 authority; no new table and no
// arbitrary-SQL authority. Secrets, verifiers and DSNs are never logged.
package recovery_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

// Owner-journal refusal sentinels: controls capture and assert the ACTUAL
// refusal stage instead of any non-nil error.
var (
	errOwnerJournalStageRefused     = errors.New("owner-journal window: the fixed audit stage refused")
	errOwnerJournalExclusionRefused = errors.New("owner-journal window: the held-row exclusion proof refused")
)

// borrowedReplacementOwnerJournalAction is the fixed file-local audit action of
// the non-authorizing refused-attempt journal row.
const borrowedReplacementOwnerJournalAction = "borrowed_replacement_owner_journal_refused"

// borrowedReplacementOwnerJournalReadSQL is the fixed read of one audit row.
const borrowedReplacementOwnerJournalReadSQL = `SELECT audit_id, result, COALESCE(instance_id::text, ''), COALESCE(operation_id, ''), COALESCE(detail::text, '') FROM recovery_audit WHERE audit_id=$1`

// borrowedReplacementOwnerJournalRow is the fixed shape of one read journal row.
type borrowedReplacementOwnerJournalRow struct {
	auditID     int64
	result      string
	instanceID  string
	operationID string
	detail      string
}

// borrowedReplacementOwnerJournalStage is the file-local atTail plan: exact
// guard-row fence, optional fixed audit insert, pending isolation and the
// same-row contender proof.
type borrowedReplacementOwnerJournalStage struct {
	key               string
	expectedOperation string
	insert            bool
	attemptID         string
	journalFail       bool
	contenderProof    bool
}

// borrowedReplacementOwnerJournalFacts preserves the actually observed stage
// results for the lane assertions.
type borrowedReplacementOwnerJournalFacts struct {
	fenced         bool
	inserted       bool
	auditID        int64
	pendingViaTx   bool
	pendingViaPool int
	contenderCode  string
}

// borrowedReplacementOwnerJournalRecord is the fixed non-authorizing refused
// audit record: result=refused, reference to the actual refused attempt, empty
// instance identity (non-instance-bound) and no authority payload.
func borrowedReplacementOwnerJournalRecord(stage *borrowedReplacementOwnerJournalStage, b *borrowedSuccessorBaseline) controlstore.AuditRecord {
	return controlstore.AuditRecord{
		Actor:       "deploy:operator",
		Action:      borrowedReplacementOwnerJournalAction,
		Target:      []byte(fmt.Sprintf(`{"target_guard_key":%q}`, b.fixture.guardKey)),
		Detail:      []byte(fmt.Sprintf(`{"refused_attempt":%q,"refusal":"dirty_guard"}`, stage.attemptID)),
		Result:      controlstore.AuditRefused,
		OperationID: stage.attemptID,
	}
}

// borrowedReplacementOwnerJournalRead reads one audit row through the supplied
// querier (the owner transaction before COMMIT or the control pool after).
func borrowedReplacementOwnerJournalRead(ctx context.Context, q controlstore.Queryer, auditID int64) (borrowedReplacementOwnerJournalRow, error) {
	// P2-4: the read derives its own short bounded query context and cancels it
	// immediately, so a stalled pool acquisition or query can never block
	// retirement/cleanup until the package timeout.
	readCtx, cancelRead := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	defer cancelRead()
	var row borrowedReplacementOwnerJournalRow
	err := q.QueryRow(readCtx, borrowedReplacementOwnerJournalReadSQL, auditID).Scan(&row.auditID, &row.result, &row.instanceID, &row.operationID, &row.detail)
	if err != nil {
		return borrowedReplacementOwnerJournalRow{}, err
	}
	return row, nil
}

// borrowedReplacementOwnerJournalCount counts the durable matching refused
// journal rows for one unique attempt identity.
func borrowedReplacementOwnerJournalCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, operationID string) int {
	t.Helper()
	countCtx, cancelCount := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	var count int
	err := pool.QueryRow(countCtx, `SELECT count(*)::int FROM recovery_audit WHERE action=$1 AND operation_id=$2 AND result='refused'`,
		borrowedReplacementOwnerJournalAction, operationID).Scan(&count)
	cancelCount()
	if err != nil {
		t.Fatalf("owner-journal durable count refused (sqlstate=%s)", borrowedAuthSQLState(err))
	}
	return count
}

// borrowedReplacementOwnerJournalActualRefusal prepares, registers and actually
// dispatches one fresh native-ready replacement attempt that is refused by the
// deliberately dirty guard BEFORE any child, receipt, probe or acceptance
// callback, and returns that refused attempt's unique identity so every
// journaled payload references a REAL refusal (never a fabricated one).
func borrowedReplacementOwnerJournalActualRefusal(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding, label string, probeCalls, acceptanceCalls *int32) string {
	t.Helper()
	attemptID := fmt.Sprintf("borrowed-replacement-owner-journal-%s-%d", label, time.Now().UnixNano())
	attempt, err := newBorrowedReplacementPrelaunchAttempt(t, ctx, b, fresh, borrowedReplacementPrelaunchArchive(t, b.fixture), attemptID, probeCalls, acceptanceCalls)
	if err != nil {
		t.Fatalf("owner-journal actual refusal (%s) preparation refused: %v", label, err)
	}
	if err := attempt.registerOrigin(ctx); err != nil {
		t.Fatalf("owner-journal actual refusal (%s) origin registration refused: %v", label, err)
	}
	borrowedReplacementPrelaunchRunRefusal(t, ctx, b, fresh, attempt, probeCalls, acceptanceCalls)
	if got := atomic.LoadInt32(probeCalls); got != 0 {
		t.Fatalf("owner-journal actual refusal (%s) ran the probe %d times, want 0", label, got)
	}
	if got := atomic.LoadInt32(acceptanceCalls); got != 0 {
		t.Fatalf("owner-journal actual refusal (%s) ran acceptance %d times, want 0", label, got)
	}
	return attemptID
}

// borrowedReplacementOwnerJournalStageRun is the single file-local atTail
// stage: the exact supplied-tx guard-row fence, then (optionally) the fixed
// audit insert with RETURNING audit_id, the same-tx pending read, the
// independent bounded read that must not see the uncommitted row, and the
// same-row contender structured 55P03 proof. A fence refusal returns BEFORE any
// journal write; a journal failure is an explicit stage-specific refusal.
func borrowedReplacementOwnerJournalStageRun(tailCtx context.Context, t *testing.T, b *borrowedSuccessorBaseline, tx pgx.Tx, stage *borrowedReplacementOwnerJournalStage, facts *borrowedReplacementOwnerJournalFacts) error {
	t.Helper()
	if _, _, err := borrowedReplacementGuardRowFence(tailCtx, tx, stage.key, stage.expectedOperation); err != nil {
		return err
	}
	facts.fenced = true
	if !stage.insert {
		return nil
	}
	if stage.journalFail {
		// The ACTUAL fixed production insert query is exercised with an expired
		// bounded context: a stage-specific journal failure, never arbitrary SQL.
		failedCtx, cancelFailed := context.WithDeadline(tailCtx, time.Now().Add(-time.Second))
		_, err := controlstore.WriteAuditReturningID(failedCtx, tx, borrowedReplacementOwnerJournalRecord(stage, b))
		cancelFailed()
		if err == nil {
			return errors.New("owner-journal journal-failure injection unexpectedly succeeded")
		}
		return fmt.Errorf("%w: the fixed audit insert query was refused: %v", errOwnerJournalStageRefused, err)
	}
	auditID, err := controlstore.WriteAuditReturningID(tailCtx, tx, borrowedReplacementOwnerJournalRecord(stage, b))
	if err != nil {
		return fmt.Errorf("%w: the fixed audit insert refused: %v", errOwnerJournalStageRefused, err)
	}
	if auditID <= 0 {
		return fmt.Errorf("%w: the fixed audit insert returned no audit_id", errOwnerJournalStageRefused)
	}
	facts.inserted = true
	facts.auditID = auditID
	// Pending isolation: the SAME supplied transaction sees the exact fixed
	// non-instance-bound refused row that was just inserted.
	pendingRow, err := borrowedReplacementOwnerJournalRead(tailCtx, tx, auditID)
	if err != nil {
		return fmt.Errorf("%w: the pending read through the supplied tx refused: %v", errOwnerJournalStageRefused, err)
	}
	if pendingRow.auditID != auditID || pendingRow.result != controlstore.AuditRefused || pendingRow.instanceID != "" || pendingRow.operationID != stage.attemptID {
		return fmt.Errorf("%w: the pending row does not match the fixed non-instance-bound refused record", errOwnerJournalStageRefused)
	}
	facts.pendingViaTx = true
	// An independent bounded control-store read MUST NOT see the uncommitted row.
	pendingCtx, cancelPending := context.WithTimeout(tailCtx, borrowedOwnerRotationQueryBudget)
	var visible int
	err = b.fixture.controlPool.QueryRow(pendingCtx, `SELECT count(*)::int FROM recovery_audit WHERE audit_id=$1`, auditID).Scan(&visible)
	cancelPending()
	if err != nil {
		return fmt.Errorf("%w: the independent pending read refused: %v", errOwnerJournalStageRefused, err)
	}
	if visible != 0 {
		return fmt.Errorf("%w: the uncommitted journal row was visible to an independent connection", errOwnerJournalStageRefused)
	}
	facts.pendingViaPool = visible
	// Retention of the same-row contender structured 55P03 proof while the
	// supplied transaction still holds the exact guard row.
	if stage.contenderProof {
		if _, _, excErr := borrowedReplacementGuardRowContenderLock(tailCtx, b.fixture.controlPool, b.fixture.guardKey); excErr == nil {
			return fmt.Errorf("%w: the contender locked the held guard row", errOwnerJournalExclusionRefused)
		} else if code := borrowedReplacementGuardRowCode(excErr); code != "55P03" {
			return fmt.Errorf("%w: contender refusal was %q, not the structured 55P03", errOwnerJournalExclusionRefused, code)
		} else {
			facts.contenderCode = code
		}
	}
	return nil
}

// borrowedReplacementOwnerJournalRun is the shared positive-shape orchestration:
// the reused owner-tail run with the file-local journal stage in atTail.
func borrowedReplacementOwnerJournalRun(ctx context.Context, t *testing.T, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding, stage *borrowedReplacementOwnerJournalStage, facts *borrowedReplacementOwnerJournalFacts, extra *borrowedReplacementOwnerTailHooks) *borrowedReplacementOwnerTailOutcome {
	t.Helper()
	hooks := &borrowedReplacementOwnerTailHooks{
		ownerPID:         b.owner.BackendPID,
		ownerStart:       b.owner.BackendStart,
		expectedVerifier: b.verifierP1,
		expectedState:    b.preState,
	}
	if extra != nil {
		hooks.preRelease = extra.preRelease
		hooks.postProbe = extra.postProbe
		hooks.useWorkerStall = extra.useWorkerStall
		hooks.useJoinBound = extra.useJoinBound
		hooks.atTail = extra.atTail
		hooks.atArbitration = extra.atArbitration
		hooks.adapter = extra.adapter
	}
	previousTail := hooks.atTail
	hooks.atTail = func(tailCtx context.Context, pump *borrowedReplacementAutonomousPump, cancelLane func(), tx pgx.Tx) error {
		if previousTail != nil {
			if err := previousTail(tailCtx, pump, cancelLane, tx); err != nil {
				return err
			}
		}
		return borrowedReplacementOwnerJournalStageRun(tailCtx, t, b, tx, stage, facts)
	}
	return borrowedReplacementOwnerTailRun(ctx, t, b, fresh, hooks)
}

// TestBorrowedReplacementOwnerJournalWindow is the bounded same-owner
// non-authorizing journal commit/rollback lane described in the file header.
func TestBorrowedReplacementOwnerJournalWindow(t *testing.T) {
	ctx := t.Context()
	var probeCalls, acceptanceCalls int32

	// P: positive journal commit. A genuine fresh native-ready attempt is
	// actually refused first; the refused attempt identity is then journaled
	// through the same owner transaction and verified durably after COMMIT.
	func() {
		b, fresh := borrowedReplacementGuardRowFixtures(t, ctx)
		beforeDisposition, beforeOperation := borrowedReplacementGuardRowRead(t, ctx, b.fixture.controlPool, b.fixture.guardKey)
		attemptID := borrowedReplacementOwnerJournalActualRefusal(t, ctx, b, fresh, "refused", &probeCalls, &acceptanceCalls)
		facts := &borrowedReplacementOwnerJournalFacts{}
		outcome := borrowedReplacementOwnerJournalRun(ctx, t, b, fresh, &borrowedReplacementOwnerJournalStage{
			key: b.fixture.guardKey, expectedOperation: b.fixture.operation,
			insert: true, attemptID: attemptID, contenderProof: true,
		}, facts, nil)
		if outcome.runErr != nil {
			t.Fatalf("owner-journal positive flow refused: %v", outcome.runErr)
		}
		if outcome.flow.ownerErr != nil {
			t.Fatalf("owner-journal positive owner window refused: %v", outcome.flow.ownerErr)
		}
		if !facts.fenced || !facts.inserted || facts.auditID <= 0 || !facts.pendingViaTx || facts.pendingViaPool != 0 || facts.contenderCode != "55P03" {
			t.Fatalf("owner-journal positive stage: fenced=%t inserted=%t auditID=%d pendingTx=%t pendingPool=%d contender=%q",
				facts.fenced, facts.inserted, facts.auditID, facts.pendingViaTx, facts.pendingViaPool, facts.contenderCode)
		}
		if got := outcome.state.tailInvocationsNow(); got != 1 {
			t.Fatalf("owner-journal positive tail invocations=%d, want exactly 1", got)
		}
		if !outcome.state.eligibleNow() {
			t.Fatal("owner-journal positive did not grant the provisional eligibility through the real arbitration")
		}
		if !outcome.flow.state.compositePublished() || !outcome.flow.state.sealedNow() || outcome.flow.state.lostNow() {
			t.Fatalf("owner-journal positive terminal state: composite=%t sealed=%t lost=%t",
				outcome.flow.state.compositePublished(), outcome.flow.state.sealedNow(), outcome.flow.state.lostNow())
		}
		if outcome.pumpJoinErr != nil {
			t.Fatalf("owner-journal positive pump join was UNKNOWN: %v", outcome.pumpJoinErr)
		}
		if !outcome.state.consumeEligibility() {
			t.Fatal("owner-journal positive provisional eligibility was not consumable once")
		}
		if outcome.state.consumeEligibility() {
			t.Fatal("owner-journal positive provisional eligibility was consumable twice")
		}
		// After the ACKNOWLEDGED COMMIT exactly ONE matching durable journal row
		// is verified independently, non-instance-bound and refused.
		if count := borrowedReplacementOwnerJournalCount(t, ctx, b.fixture.controlPool, attemptID); count != 1 {
			t.Fatalf("owner-journal post-COMMIT durable rows=%d, want exactly 1", count)
		}
		row, err := borrowedReplacementOwnerJournalRead(ctx, b.fixture.controlPool, facts.auditID)
		if err != nil {
			t.Fatalf("owner-journal post-COMMIT read refused: %v", err)
		}
		if row.auditID != facts.auditID || row.result != controlstore.AuditRefused || row.instanceID != "" || row.operationID != attemptID {
			t.Fatalf("owner-journal post-COMMIT row mismatch: id=%d result=%q instance=%q operation=%q", row.auditID, row.result, row.instanceID, row.operationID)
		}
		if !strings.Contains(row.detail, attemptID) {
			t.Fatal("owner-journal post-COMMIT row does not reference the actual refused attempt")
		}
		postDisposition, postOperation := borrowedReplacementGuardRowRead(t, ctx, b.fixture.controlPool, b.fixture.guardKey)
		if postDisposition != beforeDisposition || postOperation != beforeOperation {
			t.Fatal("owner-journal positive changed the guard contents")
		}
		if err := borrowedReplacementOwnerTailRetireGuarded(t, ctx, b, outcome); err != nil {
			t.Fatalf("owner-journal positive guarded retirement refused: %v", err)
		}
		borrowedOwnerDDLGuard(t, ctx, b)
		t.Logf("owner-journal positive: the actual refused attempt was journaled as ONE non-instance-bound refused audit row through the supplied transaction (RETURNING audit_id=%d, invisible to an independent read until COMMIT, contender 55P03 held), the unchanged arbitration granted the one-shot provisional eligibility, the acknowledged COMMIT left exactly one durable matching row with the guard unchanged, guarded retirement ran and the guard stayed unresolved with acceptance zero", facts.auditID)
	}()

	// N1a: contender-first: the fence refuses BEFORE any journal write.
	func() {
		b, fresh := borrowedReplacementGuardRowFixtures(t, ctx)
		planned := fmt.Sprintf("borrowed-replacement-owner-journal-contender-%d", time.Now().UnixNano())
		hold, err := borrowedReplacementGuardRowHoldRow(ctx, b.fixture.controlPool, b.fixture.guardKey)
		if err != nil {
			t.Fatalf("owner-journal contender-first hold refused (sqlstate=%s): %v", borrowedReplacementGuardRowCode(err), err)
		}
		t.Cleanup(hold.release)
		facts := &borrowedReplacementOwnerJournalFacts{}
		outcome := borrowedReplacementOwnerJournalRun(ctx, t, b, fresh, &borrowedReplacementOwnerJournalStage{
			key: b.fixture.guardKey, expectedOperation: b.fixture.operation,
			insert: true, attemptID: planned,
		}, facts, nil)
		hold.release()
		if facts.inserted || facts.fenced {
			t.Fatalf("owner-journal contender-first fenced=%t inserted=%t, want a pre-insertion refusal", facts.fenced, facts.inserted)
		}
		if !errors.Is(outcome.flow.ownerErr, errGuardRowWindowRowRefused) {
			t.Fatalf("owner-journal contender-first refusal was not the guard-row fence stage: %v", outcome.flow.ownerErr)
		}
		if code := borrowedReplacementGuardRowCode(outcome.flow.ownerErr); code != "55P03" {
			t.Fatalf("owner-journal contender-first owner refusal=%q, want the structured 55P03 (ownerErr=%v)", code, outcome.flow.ownerErr)
		}
		if outcome.state.eligibleNow() || outcome.flow.state.compositePublished() {
			t.Fatal("owner-journal contender-first still produced eligibility/composite")
		}
		if count := borrowedReplacementOwnerJournalCount(t, ctx, b.fixture.controlPool, planned); count != 0 {
			t.Fatalf("owner-journal contender-first wrote %d journal rows before insertion", count)
		}
		t.Logf("owner-journal contender-first negative: the held guard row refused the fence with the structured 55P03 BEFORE any journal write (zero matching rows), with no eligibility and no composite")
	}()

	// N1b: absent guard row: the fence refuses BEFORE any journal write and no
	// fallback inventory is created.
	func() {
		b, fresh := borrowedReplacementGuardRowFixtures(t, ctx)
		planned := fmt.Sprintf("borrowed-replacement-owner-journal-absent-%d", time.Now().UnixNano())
		absentKey := "sha256:" + strings.Repeat("0", 64)
		facts := &borrowedReplacementOwnerJournalFacts{}
		outcome := borrowedReplacementOwnerJournalRun(ctx, t, b, fresh, &borrowedReplacementOwnerJournalStage{
			key: absentKey, expectedOperation: b.fixture.operation,
			insert: true, attemptID: planned,
		}, facts, nil)
		if facts.inserted || facts.fenced {
			t.Fatalf("owner-journal absent-guard fenced=%t inserted=%t, want a pre-insertion refusal", facts.fenced, facts.inserted)
		}
		if !errors.Is(outcome.flow.ownerErr, errGuardRowWindowRowRefused) {
			t.Fatalf("owner-journal absent-guard refusal was not the guard-row fence stage: %v", outcome.flow.ownerErr)
		}
		if outcome.state.eligibleNow() || outcome.flow.state.compositePublished() {
			t.Fatal("owner-journal absent-guard control still produced eligibility/composite")
		}
		if count := borrowedReplacementOwnerJournalCount(t, ctx, b.fixture.controlPool, planned); count != 0 {
			t.Fatalf("owner-journal absent-guard wrote %d journal rows before insertion", count)
		}
		if count := borrowedReplacementGuardRowCount(t, ctx, b.fixture.controlPool, absentKey); count != 0 {
			t.Fatalf("owner-journal absent-guard created %d fallback inventory rows", count)
		}
		t.Logf("owner-journal absent-guard negative: the absent row refused the fence explicitly BEFORE any journal write with no fallback inventory and no eligibility/composite")
	}()

	// N1c: wrong operation provenance: the fence refuses BEFORE any journal
	// write.
	func() {
		b, fresh := borrowedReplacementGuardRowFixtures(t, ctx)
		planned := fmt.Sprintf("borrowed-replacement-owner-journal-wrongop-%d", time.Now().UnixNano())
		facts := &borrowedReplacementOwnerJournalFacts{}
		outcome := borrowedReplacementOwnerJournalRun(ctx, t, b, fresh, &borrowedReplacementOwnerJournalStage{
			key: b.fixture.guardKey, expectedOperation: b.fixture.operation + "-not-the-original",
			insert: true, attemptID: planned,
		}, facts, nil)
		if facts.inserted || facts.fenced {
			t.Fatalf("owner-journal wrong-operation fenced=%t inserted=%t, want a pre-insertion refusal", facts.fenced, facts.inserted)
		}
		if !errors.Is(outcome.flow.ownerErr, errGuardRowWindowRowRefused) {
			t.Fatalf("owner-journal wrong-operation refusal was not the guard-row fence stage: %v", outcome.flow.ownerErr)
		}
		if outcome.state.eligibleNow() || outcome.flow.state.compositePublished() {
			t.Fatal("owner-journal wrong-operation control still produced eligibility/composite")
		}
		if count := borrowedReplacementOwnerJournalCount(t, ctx, b.fixture.controlPool, planned); count != 0 {
			t.Fatalf("owner-journal wrong-operation wrote %d journal rows before insertion", count)
		}
		if _, operationID := borrowedReplacementGuardRowRead(t, ctx, b.fixture.controlPool, b.fixture.guardKey); operationID != b.fixture.operation {
			t.Fatal("owner-journal wrong-operation control changed the guard provenance")
		}
		t.Logf("owner-journal wrong-operation negative: the mismatched provenance refused the fence BEFORE any journal write with zero matching rows and no eligibility/composite")
	}()

	// N2a: caller cancel AFTER the insertion and the successful facts check:
	// confirmed pre-COMMIT refusal, zero durable journal rows.
	func() {
		b, fresh := borrowedReplacementGuardRowFixtures(t, ctx)
		planned := borrowedReplacementOwnerJournalActualRefusal(t, ctx, b, fresh, "cancel", &probeCalls, &acceptanceCalls)
		facts := &borrowedReplacementOwnerJournalFacts{}
		var canceled atomic.Bool
		outcome := borrowedReplacementOwnerJournalRun(ctx, t, b, fresh, &borrowedReplacementOwnerJournalStage{
			key: b.fixture.guardKey, expectedOperation: b.fixture.operation,
			insert: true, attemptID: planned, contenderProof: true,
		}, facts, &borrowedReplacementOwnerTailHooks{
			atArbitration: func(_ context.Context, _ *borrowedReplacementAutonomousPump, cancelLane func(), _ pgx.Tx) error {
				if cancelLane == nil {
					t.Fatal("owner-journal cancel control has no lane cancel")
				}
				canceled.Store(true)
				cancelLane()
				return nil
			},
		})
		if !facts.inserted || !canceled.Load() {
			t.Fatalf("owner-journal cancel control inserted=%t canceled=%t, want insertion then cancel", facts.inserted, canceled.Load())
		}
		if !errors.Is(outcome.flow.ownerErr, errOwnerTailArbitration) {
			t.Fatalf("owner-journal cancel refusal was not the ACTUAL arbitration stage: %v", outcome.flow.ownerErr)
		}
		if outcome.flow.ownerErr == nil || strings.Contains(outcome.flow.ownerErr.Error(), "commit target-lock acceptance transaction") {
			t.Fatalf("owner-journal cancel control was not a confirmed pre-COMMIT refusal: %v", outcome.flow.ownerErr)
		}
		if strings.Contains(outcome.flow.ownerErr.Error(), "rollback target-lock acceptance transaction") {
			t.Fatalf("owner-journal cancel rollback was not confirmed clean: %v", outcome.flow.ownerErr)
		}
		if outcome.state.eligibleNow() || outcome.flow.state.compositePublished() {
			t.Fatal("owner-journal cancel control still produced eligibility/composite")
		}
		if count := borrowedReplacementOwnerJournalCount(t, ctx, b.fixture.controlPool, planned); count != 0 {
			t.Fatalf("owner-journal cancel refusal left %d durable journal rows", count)
		}
		t.Logf("owner-journal cancel negative: the cancel was injected after the insertion and the successful facts check; the REAL arbitration refused pre-COMMIT and the rollback left zero durable journal rows")
	}()

	// N2b: actual observer loss AFTER the insertion and the successful facts
	// check: confirmed pre-COMMIT refusal, zero durable journal rows.
	func() {
		b, fresh := borrowedReplacementGuardRowFixtures(t, ctx)
		planned := borrowedReplacementOwnerJournalActualRefusal(t, ctx, b, fresh, "loss", &probeCalls, &acceptanceCalls)
		facts := &borrowedReplacementOwnerJournalFacts{}
		outcome := borrowedReplacementOwnerJournalRun(ctx, t, b, fresh, &borrowedReplacementOwnerJournalStage{
			key: b.fixture.guardKey, expectedOperation: b.fixture.operation,
			insert: true, attemptID: planned, contenderProof: true,
		}, facts, &borrowedReplacementOwnerTailHooks{
			atArbitration: func(tailCtx context.Context, pump *borrowedReplacementAutonomousPump, _ func(), _ pgx.Tx) error {
				var terminated bool
				termCtx, cancelTerm := context.WithTimeout(tailCtx, borrowedOwnerRotationQueryBudget)
				termErr := b.fixture.controlPool.QueryRow(termCtx, `SELECT pg_terminate_backend($1)`, pump.observer.reg.state.backendPID).Scan(&terminated)
				cancelTerm()
				if termErr != nil || !terminated {
					t.Fatalf("owner-journal observer-loss termination refused: terminated=%t err=%v", terminated, termErr)
				}
				select {
				case <-pump.lossAcked:
				case <-tailCtx.Done():
					t.Fatalf("owner-journal observer loss was not acknowledged before the hook context ended")
				}
				return nil
			},
		})
		if !facts.inserted {
			t.Fatal("owner-journal observer-loss control did not insert before the loss")
		}
		if outcome.pump == nil || outcome.pump.lossReasonNow() == "" {
			t.Fatal("owner-journal observer loss was not permanently latched")
		}
		if !errors.Is(outcome.flow.ownerErr, errOwnerTailArbitration) {
			t.Fatalf("owner-journal observer-loss refusal was not the ACTUAL arbitration stage: %v", outcome.flow.ownerErr)
		}
		// P2-2: a confirmed callback failure must NOT carry a rollback failure:
		// zero visible rows alone is not proof of a clean unwind.
		if outcome.flow.ownerErr == nil || strings.Contains(outcome.flow.ownerErr.Error(), "commit target-lock acceptance transaction") {
			t.Fatalf("owner-journal observer-loss control was not a confirmed pre-COMMIT refusal: %v", outcome.flow.ownerErr)
		}
		if strings.Contains(outcome.flow.ownerErr.Error(), "rollback target-lock acceptance transaction") {
			t.Fatalf("owner-journal observer-loss rollback was not confirmed clean: %v", outcome.flow.ownerErr)
		}
		if outcome.state.eligibleNow() || outcome.flow.state.compositePublished() {
			t.Fatal("owner-journal observer-loss control still produced eligibility/composite")
		}
		if count := borrowedReplacementOwnerJournalCount(t, ctx, b.fixture.controlPool, planned); count != 0 {
			t.Fatalf("owner-journal observer-loss refusal left %d durable journal rows", count)
		}
		t.Logf("owner-journal observer-loss negative: the loss was injected after the insertion and the successful facts check, the REAL arbitration refused pre-COMMIT and the rollback left zero durable journal rows")
	}()

	// N3: journal-query failure: the fixed audit insert query is refused with a
	// stage-specific error, no eligibility and no composite.
	func() {
		b, fresh := borrowedReplacementGuardRowFixtures(t, ctx)
		planned := borrowedReplacementOwnerJournalActualRefusal(t, ctx, b, fresh, "query", &probeCalls, &acceptanceCalls)
		facts := &borrowedReplacementOwnerJournalFacts{}
		outcome := borrowedReplacementOwnerJournalRun(ctx, t, b, fresh, &borrowedReplacementOwnerJournalStage{
			key: b.fixture.guardKey, expectedOperation: b.fixture.operation,
			insert: true, attemptID: planned, journalFail: true,
		}, facts, nil)
		if !facts.fenced || facts.inserted {
			t.Fatalf("owner-journal journal-failure fenced=%t inserted=%t, want a fenced stage-specific journal refusal", facts.fenced, facts.inserted)
		}
		if !errors.Is(outcome.flow.ownerErr, errOwnerJournalStageRefused) {
			t.Fatalf("owner-journal journal-failure refusal was not the fixed audit stage: %v", outcome.flow.ownerErr)
		}
		if outcome.state.eligibleNow() || outcome.flow.state.compositePublished() {
			t.Fatal("owner-journal journal-failure control still produced eligibility/composite")
		}
		if count := borrowedReplacementOwnerJournalCount(t, ctx, b.fixture.controlPool, planned); count != 0 {
			t.Fatalf("owner-journal journal-failure refusal left %d durable journal rows", count)
		}
		t.Logf("owner-journal journal-failure negative: the fixed audit insert query failed stage-specifically after the fence with no eligibility, no composite and zero durable journal rows")
	}()

	// N4: UNKNOWN worker join: no lane verdict, retirement/reuse refused, zero
	// journal rows, release followed by the ACTUAL completion-channel proof.
	func() {
		b, fresh := borrowedReplacementGuardRowFixtures(t, ctx)
		planned := fmt.Sprintf("borrowed-replacement-owner-journal-worker-%d", time.Now().UnixNano())
		workerRelease := make(chan struct{})
		var workerReleaseOnce sync.Once
		releaseWorker := func() { workerReleaseOnce.Do(func() { close(workerRelease) }) }
		t.Cleanup(releaseWorker)
		workerEntered := make(chan struct{})
		var workerEnteredOnce sync.Once
		laneCtx, cancelLane := context.WithCancel(ctx)
		defer cancelLane()
		outcome := borrowedReplacementOwnerTailRun(laneCtx, t, b, fresh, &borrowedReplacementOwnerTailHooks{
			ownerPID: b.owner.BackendPID, ownerStart: b.owner.BackendStart,
			expectedVerifier: b.verifierP1, expectedState: b.preState,
			useJoinBound: 2 * time.Second,
			useWorkerStall: func(_ context.Context) {
				workerEnteredOnce.Do(func() { close(workerEntered) })
				<-workerRelease
			},
			postProbe: func(_ context.Context, _ *borrowedReplacementAutonomousPump, _ func()) error {
				return errors.New("owner-journal worker-stall control: window stopped before its join")
			},
			atTail: func(context.Context, *borrowedReplacementAutonomousPump, func(), pgx.Tx) error {
				return errors.New("owner-journal worker-stall control: the journal stage must not run while completion is unproven")
			},
		})
		completionProven := false
		t.Cleanup(func() {
			cancelLane()
			releaseWorker()
			if completionProven {
				return
			}
			select {
			case <-outcome.flow.useCompletion:
				completionProven = true
			case <-time.After(30 * time.Second):
				t.Errorf("owner-journal worker completion is unknown after the unwind release")
			}
		})
		select {
		case <-workerEntered:
		case <-time.After(60 * time.Second):
			t.Fatalf("owner-journal worker stall never acknowledged its entry")
		}
		if got := outcome.state.tailInvocationsNow(); got != 0 {
			t.Fatalf("owner-journal worker-stall tail invocations=%d, want ZERO while completion is unproven", got)
		}
		cancelLane()
		releaseWorker()
		select {
		case <-outcome.flow.useCompletion:
			completionProven = true
		case <-time.After(30 * time.Second):
			t.Fatal("owner-journal worker completion was not proven after the release")
		}
		if !outcome.flow.terminationUnknown {
			t.Fatal("owner-journal worker-stall did not exercise the REAL join-failure path")
		}
		if outcome.flow.state.compositePublished() || outcome.flow.decisionErr == nil {
			t.Fatal("owner-journal worker-stall produced a lane verdict")
		}
		if err := borrowedReplacementOwnerTailRetireGuarded(t, ctx, b, outcome); err == nil {
			t.Fatal("owner-journal retirement was accepted while the worker join was UNKNOWN")
		}
		reuseCtx, cancelReuse := context.WithTimeout(ctx, 30*time.Second)
		reuseErr := outcome.flow.reg.Use(reuseCtx)
		copied := *outcome.flow.reg
		copyErr := copied.Use(reuseCtx)
		cancelReuse()
		if reuseErr == nil || copyErr == nil {
			t.Fatal("owner-journal reuse was accepted after the UNKNOWN worker join")
		}
		if count := borrowedReplacementOwnerJournalCount(t, ctx, b.fixture.controlPool, planned); count != 0 {
			t.Fatalf("owner-journal worker-stall wrote %d journal rows", count)
		}
		borrowedSuccessorCloseConn(t, outcome.flow.conn)
		t.Logf("owner-journal worker-unknown negative: the REAL bounded worker stall prevented the journal stage (zero rows), no lane verdict was produced, retirement/reuse were refused and completion was proven through the ACTUAL completion channel after cancel-before-release")
	}()

	// N5: UNKNOWN pump join: no lane verdict, retirement/reuse refused, zero
	// journal rows, post-release completion proof.
	func() {
		b, fresh := borrowedReplacementGuardRowFixtures(t, ctx)
		planned := fmt.Sprintf("borrowed-replacement-owner-journal-pump-%d", time.Now().UnixNano())
		stallRelease := make(chan struct{})
		var stallReleaseOnce sync.Once
		releaseStall := func() { stallReleaseOnce.Do(func() { close(stallRelease) }) }
		t.Cleanup(releaseStall)
		stallEntered := make(chan struct{})
		var stallEnteredOnce sync.Once
		outcome := borrowedReplacementOwnerTailRun(ctx, t, b, fresh, &borrowedReplacementOwnerTailHooks{
			ownerPID: b.owner.BackendPID, ownerStart: b.owner.BackendStart,
			expectedVerifier: b.verifierP1, expectedState: b.preState,
			postProbe: func(postProbeCtx context.Context, pump *borrowedReplacementAutonomousPump, cancelLane func()) error {
				t.Cleanup(func() {
					releaseStall()
					if err := pump.stopBounded(30 * time.Second); err != nil {
						t.Errorf("owner-journal stalled pump completion is unknown after the deferred release: %v", err)
					}
				})
				pump.setStallFn(func(stallCtx context.Context) error {
					stallEnteredOnce.Do(func() { close(stallEntered) })
					select {
					case <-stallRelease:
						return errors.New("owner-journal stalled scheduler released")
					case <-stallCtx.Done():
						<-stallRelease
						return errors.New("owner-journal stalled scheduler released")
					}
				})
				select {
				case <-stallEntered:
				case <-postProbeCtx.Done():
					t.Fatalf("owner-journal scheduler stall never acknowledged its entry")
				}
				return borrowedReplacementAutonomousAwaitSamples(postProbeCtx, pump, cancelLane, 1, 3*time.Second)
			},
			atTail: func(context.Context, *borrowedReplacementAutonomousPump, func(), pgx.Tx) error {
				return errors.New("owner-journal pump-stall control: the journal stage must not run")
			},
		})
		if outcome.pumpJoinErr == nil {
			t.Fatal("owner-journal stalled pump join was expected to be UNKNOWN")
		}
		if outcome.flow.state.compositePublished() || outcome.flow.decisionErr == nil {
			t.Fatal("owner-journal pump-stall produced a lane verdict")
		}
		if err := borrowedReplacementOwnerTailRetireGuarded(t, ctx, b, outcome); err == nil {
			t.Fatal("owner-journal retirement was accepted while the pump join was UNKNOWN")
		}
		reuseCtx, cancelReuse := context.WithTimeout(ctx, 30*time.Second)
		reuseErr := outcome.flow.reg.Use(reuseCtx)
		copied := *outcome.flow.reg
		copyErr := copied.Use(reuseCtx)
		cancelReuse()
		if reuseErr == nil || copyErr == nil {
			t.Fatal("owner-journal reuse was accepted after the UNKNOWN pump join")
		}
		releaseStall()
		if err := outcome.pump.stopBounded(30 * time.Second); err != nil {
			t.Fatalf("owner-journal stalled pump completion is unknown after the release: %v", err)
		}
		if count := borrowedReplacementOwnerJournalCount(t, ctx, b.fixture.controlPool, planned); count != 0 {
			t.Fatalf("owner-journal pump-stall wrote %d journal rows", count)
		}
		borrowedSuccessorCloseConn(t, outcome.flow.conn)
		t.Logf("owner-journal pump-unknown negative: the UNKNOWN pump join prevented the journal stage (zero rows), produced no lane verdict, blocked retirement/reuse and the pump completion was proven after the release")
	}()

	// N6: loss AFTER the acknowledged COMMIT: the journal row may legitimately
	// remain durable while the terminal composite must refuse.
	func() {
		b, fresh := borrowedReplacementGuardRowFixtures(t, ctx)
		planned := borrowedReplacementOwnerJournalActualRefusal(t, ctx, b, fresh, "postcommit", &probeCalls, &acceptanceCalls)
		facts := &borrowedReplacementOwnerJournalFacts{}
		outcome := borrowedReplacementOwnerJournalRun(ctx, t, b, fresh, &borrowedReplacementOwnerJournalStage{
			key: b.fixture.guardKey, expectedOperation: b.fixture.operation,
			insert: true, attemptID: planned, contenderProof: true,
		}, facts, &borrowedReplacementOwnerTailHooks{
			adapter: func(_ error, pump *borrowedReplacementAutonomousPump, _ func()) error {
				// This adapter runs AFTER the owner window returned: the COMMIT
				// was acknowledged (ownerErr will be nil), so a loss here is a
				// post-COMMIT loss.
				var terminated bool
				termCtx, cancelTerm := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
				termErr := b.fixture.controlPool.QueryRow(termCtx, `SELECT pg_terminate_backend($1)`, pump.observer.reg.state.backendPID).Scan(&terminated)
				cancelTerm()
				if termErr != nil || !terminated {
					t.Fatalf("owner-journal post-COMMIT termination refused: terminated=%t err=%v", terminated, termErr)
				}
				select {
				case <-pump.lossAcked:
				case <-time.After(30 * time.Second):
					t.Fatalf("owner-journal post-COMMIT loss was not acknowledged")
				}
				return errors.New("owner-journal loss injected after the acknowledged COMMIT")
			},
		})
		if outcome.flow.ownerErr != nil {
			t.Fatalf("owner-journal post-COMMIT case did not acknowledge the COMMIT: %v", outcome.flow.ownerErr)
		}
		if !facts.inserted {
			t.Fatal("owner-journal post-COMMIT case did not insert the journal row")
		}
		if outcome.pump == nil || outcome.pump.lossReasonNow() == "" {
			t.Fatal("owner-journal post-COMMIT loss was not permanently latched")
		}
		if outcome.flow.state.compositePublished() {
			t.Fatal("owner-journal post-COMMIT loss still published the composite")
		}
		if outcome.flow.decisionErr == nil {
			t.Fatal("owner-journal post-COMMIT terminal decision did not refuse")
		}
		// The row is a legitimate durable refused journal row: never deleted,
		// never compensated, never treated as acceptance.
		if count := borrowedReplacementOwnerJournalCount(t, ctx, b.fixture.controlPool, planned); count != 1 {
			t.Fatalf("owner-journal post-COMMIT durable rows=%d, want exactly 1", count)
		}
		row, err := borrowedReplacementOwnerJournalRead(ctx, b.fixture.controlPool, facts.auditID)
		if err != nil {
			t.Fatalf("owner-journal post-COMMIT read refused: %v", err)
		}
		if row.result != controlstore.AuditRefused || row.instanceID != "" || row.operationID != planned {
			t.Fatalf("owner-journal post-COMMIT row mismatch: result=%q instance=%q operation=%q", row.result, row.instanceID, row.operationID)
		}
		if _, operationID := borrowedReplacementGuardRowRead(t, ctx, b.fixture.controlPool, b.fixture.guardKey); operationID != b.fixture.operation {
			t.Fatal("owner-journal post-COMMIT case changed the guard contents")
		}
		borrowedSuccessorCloseConn(t, outcome.flow.conn)
		t.Logf("owner-journal post-COMMIT loss negative: the COMMIT was acknowledged and the ONE refused non-instance-bound journal row referencing the actual refused attempt legitimately remained, while the terminal composite refused; the row was never deleted, compensated or treated as acceptance")
	}()

	if got := atomic.LoadInt32(&probeCalls); got != 0 {
		t.Fatalf("owner-journal window lane ran the ordinary probe %d times, want 0", got)
	}
	if got := atomic.LoadInt32(&acceptanceCalls); got != 0 {
		t.Fatalf("owner-journal window lane ran ordinary acceptance %d times, want 0", got)
	}
}
