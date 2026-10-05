//go:build linux && drill

// borrowed-replacement-guard-row-window_linux_test.go is the bounded same-owner
// durable-guard row-fencing lane. It composes the existing machinery without
// duplicating any owner transaction: the genuine autonomous baseline/capture
// (retained owner, replacement binding, registered P1, completed readiness,
// pump first ack and two in-park samples), the observation flow and the reused
// owner-tail registration/tail steps through their error-returning hooks. This
// lane adds ONE new-file-local fence: inside the owner-tail injection, using
// ONLY the supplied owner transaction, it locks the ACTUAL
// recovery_target_guard inventory row with SELECT ... FOR UPDATE NOWAIT,
// requires exactly the fixture original canonical key, the expected operation
// provenance and a deliberately unresolved (never clean) disposition, and
// proves exclusion while held with a separately owned bounded control-store
// transaction that must receive the structured 55P03 NOWAIT refusal. The row is
// never updated or reset and absence is an explicit refusal, never a fallback
// inventory creation (FOR UPDATE does not protect an absent row). The unchanged
// facts check and shared-loss arbitration then grant only the provisional
// eligibility; loss/cancel injections are placed immediately before the
// arbitration, never before the facts query. After the acknowledged COMMIT a
// fresh contender can lock the released row and rolls back boundedly with the
// guard contents unchanged. Refusals never yield a reusable provisional result;
// UNKNOWN worker/pump joins refuse retirement/reuse with the release followed
// by the actual completion-channel proof. A fresh native-ready replacement
// attempt still refuses the dirty guard with no child, receipt, probe or
// acceptance callback, and the session is retired only through the
// completion-proven guarded cleanup (session retire pattern). Guard unresolved,
// acceptance zero. There is no continuous exclusion, no controlled admission,
// no restore, no probe beyond the session's single fixed SELECT 1, no receipt
// authority, no clean transition, no atomic acceptance, no manifest, no
// downstream and no Gate1 authority. Secrets, verifiers and DSNs are never
// logged.
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
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Guard-row refusal sentinels: controls capture and assert the ACTUAL refusal
// stage instead of any non-nil error.
var (
	errGuardRowWindowRowRefused       = errors.New("guard-row window: the supplied-tx guard row fence refused")
	errGuardRowWindowExclusionRefused = errors.New("guard-row window: the held-row exclusion proof refused")
)

// borrowedReplacementGuardRowLockSQL is the single supplied-tx fence statement:
// the actual inventory row is locked FOR UPDATE NOWAIT; absence is a no-row and
// is never interpreted as clean or created.
const borrowedReplacementGuardRowLockSQL = `SELECT disposition, operation_id FROM recovery_target_guard WHERE target_guard_key=$1 FOR UPDATE NOWAIT`

// borrowedReplacementGuardRowFence locks the actual inventory row through ONLY
// the supplied owner transaction and validates exact canonical-key presence,
// the expected operation provenance and the deliberately unresolved (never
// clean) disposition. Missing rows and every query/permission/visibility
// refusal are explicit errors: no fallback inventory is created and no clean
// state is assumed.
func borrowedReplacementGuardRowFence(ctx context.Context, tx pgx.Tx, key, expectedOperation string) (string, string, error) {
	if tx == nil {
		return "", "", fmt.Errorf("%w: no supplied owner transaction", errGuardRowWindowRowRefused)
	}
	var disposition, operationID string
	err := tx.QueryRow(ctx, borrowedReplacementGuardRowLockSQL, key).Scan(&disposition, &operationID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", fmt.Errorf("%w: no guard row exists for the supplied key (FOR UPDATE does not protect absence)", errGuardRowWindowRowRefused)
	}
	if err != nil {
		// Preserve the original error in the chain so the structured SQLSTATE
		// remains inspectable while the refusal stays attributable to this stage.
		return "", "", fmt.Errorf("%w: row lock query refused (sqlstate=%s): %w", errGuardRowWindowRowRefused, borrowedAuthSQLState(err), err)
	}
	if disposition == "clean" {
		return "", "", fmt.Errorf("%w: disposition is clean instead of deliberately unresolved", errGuardRowWindowRowRefused)
	}
	if disposition == "" {
		return "", "", fmt.Errorf("%w: disposition is empty instead of deliberately unresolved", errGuardRowWindowRowRefused)
	}
	if operationID != expectedOperation {
		return "", "", fmt.Errorf("%w: operation provenance %q does not match the expected fixture original", errGuardRowWindowRowRefused, operationID)
	}
	return disposition, operationID, nil
}

// borrowedReplacementGuardRowRead reads the inventory row without locking it.
func borrowedReplacementGuardRowRead(t *testing.T, ctx context.Context, pool *pgxpool.Pool, key string) (string, string) {
	t.Helper()
	readCtx, cancelRead := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	var disposition, operationID string
	err := pool.QueryRow(readCtx, `SELECT disposition, operation_id FROM recovery_target_guard WHERE target_guard_key=$1`, key).Scan(&disposition, &operationID)
	cancelRead()
	if err != nil {
		t.Fatalf("guard-row window inventory read refused (sqlstate=%s)", borrowedAuthSQLState(err))
	}
	return disposition, operationID
}

// borrowedReplacementGuardRowCount counts the rows for one key: it proves the
// absence refusal never created a fallback inventory row.
func borrowedReplacementGuardRowCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, key string) int {
	t.Helper()
	countCtx, cancelCount := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	var count int
	err := pool.QueryRow(countCtx, `SELECT count(*)::int FROM recovery_target_guard WHERE target_guard_key=$1`, key).Scan(&count)
	cancelCount()
	if err != nil {
		t.Fatalf("guard-row window inventory count refused (sqlstate=%s)", borrowedAuthSQLState(err))
	}
	return count
}

// borrowedReplacementGuardRowCode extracts the structured PostgreSQL SQLSTATE
// without guessing one.
func borrowedReplacementGuardRowCode(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return "no-sqlstate"
}

// borrowedReplacementGuardRowContenderLock attempts the SAME row FOR UPDATE
// NOWAIT through a separately owned bounded control-store transaction. It never
// updates or resets the row and always rolls back through an independent
// bounded cleanup.
func borrowedReplacementGuardRowContenderLock(ctx context.Context, pool *pgxpool.Pool, key string) (string, string, error) {
	acqCtx, cancelAcq := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	conn, err := pool.Acquire(acqCtx)
	cancelAcq()
	if err != nil {
		return "", "", err
	}
	defer conn.Release()
	beginCtx, cancelBegin := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	tx, err := conn.Begin(beginCtx)
	cancelBegin()
	if err != nil {
		return "", "", err
	}
	defer func() {
		rollbackCtx, cancelRollback := context.WithTimeout(context.Background(), borrowedOwnerRotationCleanupBudget)
		_ = tx.Rollback(rollbackCtx)
		cancelRollback()
	}()
	lockCtx, cancelLock := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	var disposition, operationID string
	err = tx.QueryRow(lockCtx, borrowedReplacementGuardRowLockSQL, key).Scan(&disposition, &operationID)
	cancelLock()
	if err != nil {
		return "", "", err
	}
	return disposition, operationID, nil
}

// borrowedReplacementGuardRowHold holds the row lock through a separately owned
// bounded control-store transaction until release is called. The release is
// idempotent and independently bounded so a fatal can never strand the row.
type borrowedReplacementGuardRowHold struct {
	conn        *pgxpool.Conn
	tx          pgx.Tx
	releaseOnce sync.Once
}

func (h *borrowedReplacementGuardRowHold) release() {
	if h == nil {
		return
	}
	h.releaseOnce.Do(func() {
		rollbackCtx, cancelRollback := context.WithTimeout(context.Background(), borrowedOwnerRotationCleanupBudget)
		_ = h.tx.Rollback(rollbackCtx)
		cancelRollback()
		h.conn.Release()
	})
}

// borrowedReplacementGuardRowHoldRow acquires and holds the actual row through
// a separately owned bounded transaction for the contender-first negative.
func borrowedReplacementGuardRowHoldRow(ctx context.Context, pool *pgxpool.Pool, key string) (*borrowedReplacementGuardRowHold, error) {
	acqCtx, cancelAcq := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	conn, err := pool.Acquire(acqCtx)
	cancelAcq()
	if err != nil {
		return nil, err
	}
	beginCtx, cancelBegin := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	tx, err := conn.Begin(beginCtx)
	cancelBegin()
	if err != nil {
		conn.Release()
		return nil, err
	}
	lockCtx, cancelLock := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	var disposition, operationID string
	err = tx.QueryRow(lockCtx, borrowedReplacementGuardRowLockSQL, key).Scan(&disposition, &operationID)
	cancelLock()
	if err != nil {
		rollbackCtx, cancelRollback := context.WithTimeout(context.Background(), borrowedOwnerRotationCleanupBudget)
		_ = tx.Rollback(rollbackCtx)
		cancelRollback()
		conn.Release()
		return nil, err
	}
	return &borrowedReplacementGuardRowHold{conn: conn, tx: tx}, nil
}

// borrowedReplacementGuardRowFixtures is the per-case genuine setup with the
// retained original canonical key, the expected operation provenance and the
// deliberately unresolved (never clean) guard asserted directly.
func borrowedReplacementGuardRowFixtures(t *testing.T, ctx context.Context) (*borrowedSuccessorBaseline, *borrowedReplacementBinding) {
	t.Helper()
	b, fresh := borrowedReplacementAutonomousBaseline(t, ctx)
	if b.fixture.guardKey == "" || b.fixture.operation == "" {
		t.Fatal("guard-row window fixture key/operation provenance is missing")
	}
	if ownerKey := b.anchor.Diagnostics().OriginalTargetKey; ownerKey.String() != b.fixture.guardKey {
		t.Fatal("guard-row window inventory key is not the fixture original canonical key")
	}
	disposition, operationID := borrowedReplacementGuardRowRead(t, ctx, b.fixture.controlPool, b.fixture.guardKey)
	if disposition == "clean" {
		t.Fatal("guard-row window requires a deliberately unresolved (never clean) guard")
	}
	if operationID != b.fixture.operation {
		t.Fatal("guard-row window guard operation provenance is not the fixture original")
	}
	if count := borrowedReplacementGuardRowCount(t, ctx, b.fixture.controlPool, b.fixture.guardKey); count != 1 {
		t.Fatalf("guard-row window fixture inventory rows=%d, want exactly 1", count)
	}
	t.Logf("guard-row window fixture: fixture original canonical key retained, disposition %q (deliberately unresolved), operation provenance matches the fixture original", disposition)
	return b, fresh
}

// borrowedReplacementGuardRowOrdinaryRefuse dispatches one fresh native-ready
// replacement attempt that must still be refused by the deliberately dirty
// guard with no child, receipt, probe or acceptance callback.
func borrowedReplacementGuardRowOrdinaryRefuse(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding, label string, probeCalls, acceptanceCalls *int32) {
	t.Helper()
	attempt, err := newBorrowedReplacementPrelaunchAttempt(t, ctx, b, fresh, borrowedReplacementPrelaunchArchive(t, b.fixture),
		fmt.Sprintf("borrowed-replacement-guard-row-%s-%d", label, time.Now().UnixNano()), probeCalls, acceptanceCalls)
	if err != nil {
		t.Fatalf("guard-row ordinary refusal (%s) attempt preparation refused: %v", label, err)
	}
	if err := attempt.registerOrigin(ctx); err != nil {
		t.Fatalf("guard-row ordinary refusal (%s) origin registration refused: %v", label, err)
	}
	borrowedReplacementPrelaunchRunRefusal(t, ctx, b, fresh, attempt, probeCalls, acceptanceCalls)
}

// TestBorrowedReplacementGuardRowWindow is the bounded same-owner durable-guard
// row-fencing lane described in the file header.
func TestBorrowedReplacementGuardRowWindow(t *testing.T) {
	ctx := t.Context()
	var probeCalls, acceptanceCalls int32

	// P: positive row fencing through ONLY the supplied owner transaction, the
	// real exclusion proof (structured 55P03 while held), the unchanged facts
	// check and shared-loss arbitration, the acknowledged COMMIT, the released
	// row provable by a fresh contender, and guarded retirement with the guard
	// unresolved.
	func() {
		b, fresh := borrowedReplacementGuardRowFixtures(t, ctx)
		beforeDisposition, beforeOperation := borrowedReplacementGuardRowRead(t, ctx, b.fixture.controlPool, b.fixture.guardKey)
		var (
			fenceLocked   atomic.Bool
			fenceDisp     string
			fenceOp       string
			contenderCode string
		)
		outcome := borrowedReplacementOwnerTailRun(ctx, t, b, fresh, &borrowedReplacementOwnerTailHooks{
			ownerPID: b.owner.BackendPID, ownerStart: b.owner.BackendStart,
			expectedVerifier: b.verifierP1, expectedState: b.preState,
			atTail: func(tailCtx context.Context, _ *borrowedReplacementAutonomousPump, _ func(), tx pgx.Tx) error {
				disposition, operationID, err := borrowedReplacementGuardRowFence(tailCtx, tx, b.fixture.guardKey, b.fixture.operation)
				if err != nil {
					return err
				}
				if disposition != beforeDisposition || operationID != beforeOperation {
					return fmt.Errorf("%w: the supplied-tx lock changed the guard contents", errGuardRowWindowRowRefused)
				}
				fenceDisp, fenceOp = disposition, operationID
				fenceLocked.Store(true)
				// Exclusion while HELD: a separately owned bounded
				// control-store transaction must receive the structured 55P03
				// and never updates or resets the row.
				if _, _, excErr := borrowedReplacementGuardRowContenderLock(tailCtx, b.fixture.controlPool, b.fixture.guardKey); excErr == nil {
					return fmt.Errorf("%w: the contender locked the held row", errGuardRowWindowExclusionRefused)
				} else if code := borrowedReplacementGuardRowCode(excErr); code != "55P03" {
					return fmt.Errorf("%w: contender refusal was %q, not the structured 55P03", errGuardRowWindowExclusionRefused, code)
				} else {
					contenderCode = code
				}
				return nil
			},
		})
		if outcome.runErr != nil {
			t.Fatalf("guard-row positive flow refused: %v", outcome.runErr)
		}
		if outcome.flow.ownerErr != nil {
			t.Fatalf("guard-row positive owner window refused: %v", outcome.flow.ownerErr)
		}
		if !fenceLocked.Load() || fenceDisp != beforeDisposition || fenceOp != beforeOperation {
			t.Fatal("guard-row positive did not lock the actual inventory row through only the supplied transaction")
		}
		if contenderCode != "55P03" {
			t.Fatalf("guard-row positive exclusion proof saw %q, want the structured 55P03", contenderCode)
		}
		if got := outcome.state.tailInvocationsNow(); got != 1 {
			t.Fatalf("guard-row positive tail invocations=%d, want exactly 1", got)
		}
		if !outcome.state.eligibleNow() {
			t.Fatal("guard-row positive did not grant the provisional eligibility through the real arbitration")
		}
		if _, recorded := outcome.state.useErrNow(); !recorded {
			t.Fatal("guard-row positive use completion was not channel-confirmed")
		}
		if !outcome.flow.state.compositePublished() || !outcome.flow.state.sealedNow() || outcome.flow.state.lostNow() {
			t.Fatalf("guard-row positive terminal state: composite=%t sealed=%t lost=%t",
				outcome.flow.state.compositePublished(), outcome.flow.state.sealedNow(), outcome.flow.state.lostNow())
		}
		if !outcome.state.consumeEligibility() {
			t.Fatal("guard-row positive provisional eligibility was not consumable once")
		}
		if outcome.state.consumeEligibility() {
			t.Fatal("guard-row positive provisional eligibility was consumable twice")
		}
		// After the ACKNOWLEDGED COMMIT the row lock is released: a fresh
		// contender CAN lock the row, then rolls back boundedly with the guard
		// contents unchanged.
		postDisposition, postOperation, postErr := borrowedReplacementGuardRowContenderLock(ctx, b.fixture.controlPool, b.fixture.guardKey)
		if postErr != nil {
			t.Fatalf("guard-row post-COMMIT contender lock refused (sqlstate=%s): %v", borrowedReplacementGuardRowCode(postErr), postErr)
		}
		if postDisposition != beforeDisposition || postOperation != beforeOperation {
			t.Fatal("guard-row post-COMMIT contender lock observed changed guard contents")
		}
		if count := borrowedReplacementGuardRowCount(t, ctx, b.fixture.controlPool, b.fixture.guardKey); count != 1 {
			t.Fatalf("guard-row post-COMMIT inventory rows=%d, want exactly 1", count)
		}
		if err := borrowedReplacementOwnerTailRetireGuarded(t, ctx, b, outcome); err != nil {
			t.Fatalf("guard-row positive guarded retirement refused: %v", err)
		}
		borrowedOwnerDDLGuard(t, ctx, b)
		borrowedReplacementGuardRowOrdinaryRefuse(t, ctx, b, fresh, "after-success", &probeCalls, &acceptanceCalls)
		t.Logf("guard-row positive: the supplied-tx NOWAIT fence held the actual inventory row (disposition %q, fixture original operation provenance), the separately owned contender received the structured 55P03, the unchanged flow committed, a fresh contender locked the released row and rolled back, guarded retirement ran and the guard stayed unresolved with acceptance zero", beforeDisposition)
	}()

	// C1: a contender holds the row FIRST: the owner's real NOWAIT refusal, no
	// eligibility and no composite.
	func() {
		b, fresh := borrowedReplacementGuardRowFixtures(t, ctx)
		beforeDisposition, beforeOperation := borrowedReplacementGuardRowRead(t, ctx, b.fixture.controlPool, b.fixture.guardKey)
		hold, err := borrowedReplacementGuardRowHoldRow(ctx, b.fixture.controlPool, b.fixture.guardKey)
		if err != nil {
			t.Fatalf("guard-row contender-first hold refused (sqlstate=%s): %v", borrowedReplacementGuardRowCode(err), err)
		}
		t.Cleanup(hold.release)
		var ownerCode string
		outcome := borrowedReplacementOwnerTailRun(ctx, t, b, fresh, &borrowedReplacementOwnerTailHooks{
			ownerPID: b.owner.BackendPID, ownerStart: b.owner.BackendStart,
			expectedVerifier: b.verifierP1, expectedState: b.preState,
			atTail: func(tailCtx context.Context, _ *borrowedReplacementAutonomousPump, _ func(), tx pgx.Tx) error {
				_, _, fenceErr := borrowedReplacementGuardRowFence(tailCtx, tx, b.fixture.guardKey, b.fixture.operation)
				if fenceErr == nil {
					return errors.New("guard-row contender-first: the owner NOWAIT fence unexpectedly acquired the held row")
				}
				ownerCode = borrowedReplacementGuardRowCode(fenceErr)
				return fenceErr
			},
		})
		hold.release()
		if got := outcome.state.tailInvocationsNow(); got != 1 {
			t.Fatalf("guard-row contender-first tail invocations=%d, want exactly 1", got)
		}
		if outcome.state.eligibleNow() || outcome.flow.state.compositePublished() {
			t.Fatal("guard-row contender-first still produced eligibility/composite")
		}
		if ownerCode != "55P03" {
			t.Fatalf("guard-row contender-first owner refusal=%q, want the structured 55P03 (ownerErr=%v)", ownerCode, outcome.flow.ownerErr)
		}
		if !errors.Is(outcome.flow.ownerErr, errGuardRowWindowRowRefused) {
			t.Fatalf("guard-row contender-first owner refusal was not the row-fence stage: %v", outcome.flow.ownerErr)
		}
		if outcome.flow.decisionErr == nil {
			t.Fatal("guard-row contender-first final decision did not refuse")
		}
		disposition, operationID := borrowedReplacementGuardRowRead(t, ctx, b.fixture.controlPool, b.fixture.guardKey)
		if disposition != beforeDisposition || operationID != beforeOperation {
			t.Fatal("guard-row contender-first changed the guard contents")
		}
		if count := borrowedReplacementGuardRowCount(t, ctx, b.fixture.controlPool, b.fixture.guardKey); count != 1 {
			t.Fatalf("guard-row contender-first inventory rows=%d, want exactly 1", count)
		}
		t.Logf("guard-row contender-first negative: the held row produced the owner's real structured 55P03 refusal with no eligibility, no composite and unchanged guard contents")
	}()

	// C2: no matching row: absence is an explicit refusal and never a fallback
	// inventory creation; the ordinary invocation still refuses afterwards.
	func() {
		b, fresh := borrowedReplacementGuardRowFixtures(t, ctx)
		absentKey := "sha256:" + strings.Repeat("0", 64)
		outcome := borrowedReplacementOwnerTailRun(ctx, t, b, fresh, &borrowedReplacementOwnerTailHooks{
			ownerPID: b.owner.BackendPID, ownerStart: b.owner.BackendStart,
			expectedVerifier: b.verifierP1, expectedState: b.preState,
			atTail: func(tailCtx context.Context, _ *borrowedReplacementAutonomousPump, _ func(), tx pgx.Tx) error {
				_, _, fenceErr := borrowedReplacementGuardRowFence(tailCtx, tx, absentKey, b.fixture.operation)
				if fenceErr == nil {
					return errors.New("guard-row absent-key control unexpectedly found a row")
				}
				return fenceErr
			},
		})
		if outcome.state.eligibleNow() || outcome.flow.state.compositePublished() {
			t.Fatal("guard-row absent-key control still produced eligibility/composite")
		}
		if !errors.Is(outcome.flow.ownerErr, errGuardRowWindowRowRefused) {
			t.Fatalf("guard-row absent-key refusal was not the row-fence stage: %v", outcome.flow.ownerErr)
		}
		if outcome.flow.decisionErr == nil {
			t.Fatal("guard-row absent-key final decision did not refuse")
		}
		if count := borrowedReplacementGuardRowCount(t, ctx, b.fixture.controlPool, absentKey); count != 0 {
			t.Fatalf("guard-row absent-key refusal created %d fallback inventory rows", count)
		}
		if count := borrowedReplacementGuardRowCount(t, ctx, b.fixture.controlPool, b.fixture.guardKey); count != 1 {
			t.Fatalf("guard-row absent-key control changed the real inventory rows: %d", count)
		}
		disposition, operationID := borrowedReplacementGuardRowRead(t, ctx, b.fixture.controlPool, b.fixture.guardKey)
		if disposition == "clean" || operationID != b.fixture.operation {
			t.Fatal("guard-row absent-key control changed the real guard contents")
		}
		borrowedReplacementGuardRowOrdinaryRefuse(t, ctx, b, fresh, "after-failed-fencing", &probeCalls, &acceptanceCalls)
		t.Logf("guard-row absent-key negative: FOR UPDATE did not protect the absent row, the fence refused explicitly, no fallback inventory row was created and the ordinary invocation still refused")
	}()

	// C3: wrong operation provenance on the real row: an explicit refusal with
	// the row never modified.
	func() {
		b, fresh := borrowedReplacementGuardRowFixtures(t, ctx)
		beforeDisposition, beforeOperation := borrowedReplacementGuardRowRead(t, ctx, b.fixture.controlPool, b.fixture.guardKey)
		outcome := borrowedReplacementOwnerTailRun(ctx, t, b, fresh, &borrowedReplacementOwnerTailHooks{
			ownerPID: b.owner.BackendPID, ownerStart: b.owner.BackendStart,
			expectedVerifier: b.verifierP1, expectedState: b.preState,
			atTail: func(tailCtx context.Context, _ *borrowedReplacementAutonomousPump, _ func(), tx pgx.Tx) error {
				_, _, fenceErr := borrowedReplacementGuardRowFence(tailCtx, tx, b.fixture.guardKey, b.fixture.operation+"-not-the-original")
				if fenceErr == nil {
					return errors.New("guard-row wrong-operation control unexpectedly accepted the provenance")
				}
				return fenceErr
			},
		})
		if outcome.state.eligibleNow() || outcome.flow.state.compositePublished() {
			t.Fatal("guard-row wrong-operation control still produced eligibility/composite")
		}
		if !errors.Is(outcome.flow.ownerErr, errGuardRowWindowRowRefused) {
			t.Fatalf("guard-row wrong-operation refusal was not the row-fence stage: %v", outcome.flow.ownerErr)
		}
		if outcome.flow.decisionErr == nil {
			t.Fatal("guard-row wrong-operation final decision did not refuse")
		}
		disposition, operationID := borrowedReplacementGuardRowRead(t, ctx, b.fixture.controlPool, b.fixture.guardKey)
		if disposition != beforeDisposition || operationID != beforeOperation || operationID != b.fixture.operation {
			t.Fatal("guard-row wrong-operation control changed the real row")
		}
		if count := borrowedReplacementGuardRowCount(t, ctx, b.fixture.controlPool, b.fixture.guardKey); count != 1 {
			t.Fatalf("guard-row wrong-operation inventory rows=%d, want exactly 1", count)
		}
		t.Logf("guard-row wrong-operation negative: the mismatched provenance refused explicitly after the supplied-tx lock and the row stayed unchanged")
	}()

	// C4: caller cancel injected immediately before the arbitration (after the
	// successful row fence and the final facts check): the REAL arbitration
	// refuses with no reusable provisional result.
	func() {
		b, fresh := borrowedReplacementGuardRowFixtures(t, ctx)
		beforeDisposition, beforeOperation := borrowedReplacementGuardRowRead(t, ctx, b.fixture.controlPool, b.fixture.guardKey)
		var (
			fenceLocked atomic.Bool
			canceled    atomic.Bool
		)
		outcome := borrowedReplacementOwnerTailRun(ctx, t, b, fresh, &borrowedReplacementOwnerTailHooks{
			ownerPID: b.owner.BackendPID, ownerStart: b.owner.BackendStart,
			expectedVerifier: b.verifierP1, expectedState: b.preState,
			atTail: func(tailCtx context.Context, _ *borrowedReplacementAutonomousPump, _ func(), tx pgx.Tx) error {
				if _, _, err := borrowedReplacementGuardRowFence(tailCtx, tx, b.fixture.guardKey, b.fixture.operation); err != nil {
					return err
				}
				fenceLocked.Store(true)
				return nil
			},
			atArbitration: func(_ context.Context, _ *borrowedReplacementAutonomousPump, cancelLane func(), _ pgx.Tx) error {
				if cancelLane == nil {
					t.Fatal("guard-row caller-cancel control has no lane cancel")
				}
				canceled.Store(true)
				cancelLane()
				return nil
			},
		})
		if !fenceLocked.Load() || !canceled.Load() {
			t.Fatal("guard-row caller-cancel control did not run the fence and the cancel")
		}
		if outcome.state.eligibleNow() || outcome.flow.state.compositePublished() {
			t.Fatal("guard-row caller-cancel control still produced eligibility/composite")
		}
		if !errors.Is(outcome.flow.ownerErr, errOwnerTailArbitration) {
			t.Fatalf("guard-row caller-cancel refusal was not the ACTUAL arbitration stage: %v", outcome.flow.ownerErr)
		}
		if outcome.state.consumeEligibility() {
			t.Fatal("guard-row caller-cancel control left a reusable provisional result")
		}
		disposition, operationID := borrowedReplacementGuardRowRead(t, ctx, b.fixture.controlPool, b.fixture.guardKey)
		if disposition != beforeDisposition || operationID != beforeOperation {
			t.Fatal("guard-row caller-cancel control changed the guard contents")
		}
		t.Logf("guard-row caller-cancel negative: the cancel was injected after the successful fence and facts check and the REAL arbitration refused with no reusable provisional result")
	}()

	// C5: row-query timeout: the lock query is refused by an expired bounded
	// context (never a clean pass), with no eligibility/composite and the row
	// still lockable and unchanged afterwards.
	func() {
		b, fresh := borrowedReplacementGuardRowFixtures(t, ctx)
		beforeDisposition, beforeOperation := borrowedReplacementGuardRowRead(t, ctx, b.fixture.controlPool, b.fixture.guardKey)
		var timeoutCode string
		outcome := borrowedReplacementOwnerTailRun(ctx, t, b, fresh, &borrowedReplacementOwnerTailHooks{
			ownerPID: b.owner.BackendPID, ownerStart: b.owner.BackendStart,
			expectedVerifier: b.verifierP1, expectedState: b.preState,
			atTail: func(tailCtx context.Context, _ *borrowedReplacementAutonomousPump, _ func(), tx pgx.Tx) error {
				expiredCtx, cancelExpired := context.WithDeadline(tailCtx, time.Now().Add(-time.Second))
				_, _, fenceErr := borrowedReplacementGuardRowFence(expiredCtx, tx, b.fixture.guardKey, b.fixture.operation)
				cancelExpired()
				if fenceErr == nil {
					return errors.New("guard-row row-query timeout control unexpectedly succeeded")
				}
				timeoutCode = borrowedReplacementGuardRowCode(fenceErr)
				return fenceErr
			},
		})
		if outcome.state.eligibleNow() || outcome.flow.state.compositePublished() {
			t.Fatal("guard-row row-query timeout control still produced eligibility/composite")
		}
		if !errors.Is(outcome.flow.ownerErr, errGuardRowWindowRowRefused) {
			t.Fatalf("guard-row row-query timeout refusal was not the row-fence stage: %v", outcome.flow.ownerErr)
		}
		if timeoutCode == "55P03" {
			t.Fatal("guard-row row-query timeout control exercised the NOWAIT contention path instead of the timeout refusal")
		}
		if outcome.flow.decisionErr == nil {
			t.Fatal("guard-row row-query timeout final decision did not refuse")
		}
		postDisposition, postOperation, postErr := borrowedReplacementGuardRowContenderLock(ctx, b.fixture.controlPool, b.fixture.guardKey)
		if postErr != nil {
			t.Fatalf("guard-row row-query timeout left the row unacquirable: %v", postErr)
		}
		if postDisposition != beforeDisposition || postOperation != beforeOperation {
			t.Fatal("guard-row row-query timeout observed changed guard contents")
		}
		t.Logf("guard-row row-query timeout negative: the expired bounded context refused the lock query (sqlstate %s) with no eligibility/composite and the row stayed lockable and unchanged", timeoutCode)
	}()

	// C6: observer loss injected immediately before the arbitration (after the
	// successful row fence and the final facts check): the REAL arbitration
	// refuses with the permanent loss latched.
	func() {
		b, fresh := borrowedReplacementGuardRowFixtures(t, ctx)
		var fenceLocked atomic.Bool
		outcome := borrowedReplacementOwnerTailRun(ctx, t, b, fresh, &borrowedReplacementOwnerTailHooks{
			ownerPID: b.owner.BackendPID, ownerStart: b.owner.BackendStart,
			expectedVerifier: b.verifierP1, expectedState: b.preState,
			atTail: func(tailCtx context.Context, _ *borrowedReplacementAutonomousPump, _ func(), tx pgx.Tx) error {
				if _, _, err := borrowedReplacementGuardRowFence(tailCtx, tx, b.fixture.guardKey, b.fixture.operation); err != nil {
					return err
				}
				fenceLocked.Store(true)
				return nil
			},
			atArbitration: func(tailCtx context.Context, pump *borrowedReplacementAutonomousPump, _ func(), _ pgx.Tx) error {
				var terminated bool
				termCtx, cancelTerm := context.WithTimeout(tailCtx, borrowedOwnerRotationQueryBudget)
				termErr := b.fixture.controlPool.QueryRow(termCtx, `SELECT pg_terminate_backend($1)`, pump.observer.reg.state.backendPID).Scan(&terminated)
				cancelTerm()
				if termErr != nil || !terminated {
					t.Fatalf("guard-row observer-loss termination refused: terminated=%t err=%v", terminated, termErr)
				}
				select {
				case <-pump.lossAcked:
				case <-tailCtx.Done():
					t.Fatalf("guard-row observer loss was not acknowledged before the hook context ended")
				}
				return nil
			},
		})
		if !fenceLocked.Load() {
			t.Fatal("guard-row observer-loss control did not run the row fence")
		}
		if outcome.pump == nil || outcome.pump.lossReasonNow() == "" {
			t.Fatal("guard-row observer loss was not permanently latched")
		}
		if outcome.state.eligibleNow() || outcome.flow.state.compositePublished() {
			t.Fatal("guard-row observer-loss control still produced eligibility/composite")
		}
		if !errors.Is(outcome.flow.ownerErr, errOwnerTailArbitration) {
			t.Fatalf("guard-row observer-loss refusal was not the ACTUAL arbitration stage: %v", outcome.flow.ownerErr)
		}
		if outcome.state.consumeEligibility() {
			t.Fatal("guard-row observer-loss control left a reusable provisional result")
		}
		t.Logf("guard-row observer-loss negative: the loss was injected after the successful fence and facts check, the REAL arbitration refused and no reusable provisional result remained")
	}()

	// C7: UNKNOWN worker join: the REAL bounded worker stall before completion
	// closes, with zero row-fence invocations while completion is unproven,
	// retirement/reuse refused and the release followed by the ACTUAL
	// completion-channel proof.
	func() {
		b, fresh := borrowedReplacementGuardRowFixtures(t, ctx)
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
				return errors.New("guard-row worker-stall control: window stopped before its join")
			},
			atTail: func(context.Context, *borrowedReplacementAutonomousPump, func(), pgx.Tx) error {
				return errors.New("guard-row worker-stall control: the row fence must not run while completion is unproven")
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
				t.Errorf("guard-row worker completion is unknown after the unwind release")
			}
		})
		select {
		case <-workerEntered:
		case <-time.After(60 * time.Second):
			t.Fatalf("guard-row worker stall never acknowledged its entry")
		}
		if got := outcome.state.tailInvocationsNow(); got != 0 {
			t.Fatalf("guard-row worker-stall tail invocations=%d, want ZERO while completion is unproven", got)
		}
		cancelLane()
		releaseWorker()
		select {
		case <-outcome.flow.useCompletion:
			completionProven = true
		case <-time.After(30 * time.Second):
			t.Fatal("guard-row worker completion was not proven after the release")
		}
		if !outcome.flow.terminationUnknown {
			t.Fatal("guard-row worker-stall did not exercise the REAL join-failure path")
		}
		if outcome.flow.state.compositePublished() {
			t.Fatal("guard-row worker-stall published the composite")
		}
		if err := borrowedReplacementOwnerTailRetireGuarded(t, ctx, b, outcome); err == nil {
			t.Fatal("guard-row retirement was accepted while the worker join was UNKNOWN")
		}
		reuseCtx, cancelReuse := context.WithTimeout(ctx, 30*time.Second)
		reuseErr := outcome.flow.reg.Use(reuseCtx)
		copied := *outcome.flow.reg
		copyErr := copied.Use(reuseCtx)
		cancelReuse()
		if reuseErr == nil || copyErr == nil {
			t.Fatal("guard-row reuse was accepted after the UNKNOWN worker join")
		}
		disposition, operationID := borrowedReplacementGuardRowRead(t, ctx, b.fixture.controlPool, b.fixture.guardKey)
		if disposition == "clean" || operationID != b.fixture.operation {
			t.Fatal("guard-row worker-stall control changed the guard contents")
		}
		borrowedSuccessorCloseConn(t, outcome.flow.conn)
		t.Logf("guard-row worker-unknown negative: the REAL bounded worker stall blocked the row fence (zero invocations while completion was unproven), retirement/reuse were refused, and the completion was proven through the ACTUAL completion channel after cancel-before-release")
	}()

	// C8: UNKNOWN pump join: the stalled scheduler blocks retirement/reuse with
	// the unconditionally released stall and post-release completion proof.
	func() {
		b, fresh := borrowedReplacementGuardRowFixtures(t, ctx)
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
						t.Errorf("guard-row stalled pump completion is unknown after the deferred release: %v", err)
					}
				})
				pump.setStallFn(func(stallCtx context.Context) error {
					stallEnteredOnce.Do(func() { close(stallEntered) })
					select {
					case <-stallRelease:
						return errors.New("guard-row stalled scheduler released")
					case <-stallCtx.Done():
						<-stallRelease
						return errors.New("guard-row stalled scheduler released")
					}
				})
				select {
				case <-stallEntered:
				case <-postProbeCtx.Done():
					t.Fatalf("guard-row scheduler stall never acknowledged its entry")
				}
				return borrowedReplacementAutonomousAwaitSamples(postProbeCtx, pump, cancelLane, 1, 3*time.Second)
			},
		})
		if outcome.pumpJoinErr == nil {
			t.Fatal("guard-row stalled pump join was expected to be UNKNOWN")
		}
		if err := borrowedReplacementOwnerTailRetireGuarded(t, ctx, b, outcome); err == nil {
			t.Fatal("guard-row retirement was accepted while the pump join was UNKNOWN")
		}
		reuseCtx, cancelReuse := context.WithTimeout(ctx, 30*time.Second)
		reuseErr := outcome.flow.reg.Use(reuseCtx)
		copied := *outcome.flow.reg
		copyErr := copied.Use(reuseCtx)
		cancelReuse()
		if reuseErr == nil || copyErr == nil {
			t.Fatal("guard-row reuse was accepted after the UNKNOWN pump join")
		}
		releaseStall()
		if err := outcome.pump.stopBounded(30 * time.Second); err != nil {
			t.Fatalf("guard-row stalled pump completion is unknown after the release: %v", err)
		}
		disposition, operationID := borrowedReplacementGuardRowRead(t, ctx, b.fixture.controlPool, b.fixture.guardKey)
		if disposition == "clean" || operationID != b.fixture.operation {
			t.Fatal("guard-row stalled-pump control changed the guard contents")
		}
		borrowedSuccessorCloseConn(t, outcome.flow.conn)
		t.Logf("guard-row pump-unknown negative: the UNKNOWN pump join blocked retirement/reuse and the pump completion was proven after the release")
	}()

	if got := atomic.LoadInt32(&probeCalls); got != 0 {
		t.Fatalf("guard-row window lane ran the ordinary probe %d times, want 0", got)
	}
	if got := atomic.LoadInt32(&acceptanceCalls); got != 0 {
		t.Fatalf("guard-row window lane ran ordinary acceptance %d times, want 0", got)
	}
}
