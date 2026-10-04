//go:build linux && drill

// borrowed-replacement-catalog-window_linux_test.go is the bounded owner-held
// catalog fence across replacement-session use lane. It starts from a GENUINE
// replacement-bound capture (the actual bounded binding orchestration with
// stage counters), registers ONE replacement session, parks the actual Use at
// its pre-probe barrier (the session's own prefix/anchor/catalog/identity
// checks run BEFORE the owner transaction), and then enters the original owner
// transaction through the real caller-owned lock session: supplied-tx
// owner/incarnation/advisory checks, SHARE NOWAIT strictly before the reads,
// and the complete committed P1 facts/verifier validation BEFORE the Use is
// released. The protected window holds the SHARE fence across the Use's single
// SELECT 1 and its post-checks; an actual external mutation is refused with the
// exact 55P03 before the pre-probe release and again at the post-probe
// barrier. The Use is released, bounded-joined inside the window, the P1 facts
// are rechecked through the supplied transaction, and only then does the owner
// transaction commit. The composite publication requires BOTH the Use and the
// owner transaction to succeed; a commit ambiguity is UNKNOWN and never
// success. After the return the fresh prefix, the original anchor and the
// owner health are rechecked and the session is retired with the guard
// unresolved and acceptance unchanged. The owner callback never calls the full
// Use, Health, anchor recheck or prefix inspection (no recursion or deadlock);
// the resumed Use tail performs target/OS checks and shared-state decisions
// only. This is NOT atomic acceptance: the SHARE ends at COMMIT. There is no
// continuous exclusion, no guard admission, no restore, no probe beyond the
// session's single fixed SELECT 1, no receipt authority, no clean transition,
// no acceptance, no manifest, no downstream and no Gate1 authority. Secrets,
// verifiers and DSNs are never logged.
package recovery_test

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// borrowedReplacementCatalogWindowBarrier is the new-file two-stage
// notification/blocking barrier installed at the session's pre-probe and
// post-probe publication stages. Both stages select the barrier cancellation
// channel, so a caller cancel-before-release cleanup can never strand the Use.
type borrowedReplacementCatalogWindowBarrier struct {
	preEntered  chan struct{}
	postEntered chan struct{}
	preRelease  chan struct{}
	postRelease chan struct{}
	cancelled   chan struct{}

	preEnteredOnce  sync.Once
	postEnteredOnce sync.Once
	preReleaseOnce  sync.Once
	postReleaseOnce sync.Once
	cancelOnce      sync.Once
}

func installBorrowedReplacementCatalogWindowBarrier(t *testing.T, reg *borrowedReplacementSessionRegistration) *borrowedReplacementCatalogWindowBarrier {
	t.Helper()
	if reg == nil || reg.state == nil {
		t.Fatal("catalog window barrier requires the concrete session registration")
	}
	barrier := &borrowedReplacementCatalogWindowBarrier{
		preEntered:  make(chan struct{}),
		postEntered: make(chan struct{}),
		preRelease:  make(chan struct{}),
		postRelease: make(chan struct{}),
		cancelled:   make(chan struct{}),
	}
	reg.state.stage = func(stage borrowedReplacementSessionStage) {
		switch stage {
		case borrowedReplacementSessionStageUsePublish:
			barrier.preEnteredOnce.Do(func() { close(barrier.preEntered) })
			select {
			case <-barrier.preRelease:
			case <-barrier.cancelled:
			}
		case borrowedReplacementSessionStageUsePublication:
			barrier.postEnteredOnce.Do(func() { close(barrier.postEntered) })
			select {
			case <-barrier.postRelease:
			case <-barrier.cancelled:
			}
		}
	}
	t.Cleanup(barrier.releaseAll)
	return barrier
}

func (b *borrowedReplacementCatalogWindowBarrier) releasePre() {
	b.preReleaseOnce.Do(func() { close(b.preRelease) })
}

func (b *borrowedReplacementCatalogWindowBarrier) releasePost() {
	b.postReleaseOnce.Do(func() { close(b.postRelease) })
}

func (b *borrowedReplacementCatalogWindowBarrier) cancel() {
	b.cancelOnce.Do(func() { close(b.cancelled) })
}

func (b *borrowedReplacementCatalogWindowBarrier) releaseAll() {
	b.cancel()
	b.releasePre()
	b.releasePost()
}

// borrowedReplacementCatalogWindowValidateFacts validates the complete
// committed P1 facts/verifier through the SUPPLIED owner transaction only: a
// role-fact change returns a distinct error, a verifier mismatch with
// unchanged facts returns the shared verifier-drift sentinel.
func borrowedReplacementCatalogWindowValidateFacts(ctx context.Context, tx pgx.Tx, f *borrowedAuthHandoffFixture, expectedVerifier string, expectedState borrowedOwnerRotationRoleState) error {
	storedCtx, cancelStored := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	var storedVerifier string
	storedErr := tx.QueryRow(storedCtx, `SELECT coalesce(rolpassword,'') FROM pg_authid WHERE rolname=$1`, f.writerRole).Scan(&storedVerifier)
	cancelStored()
	if storedErr != nil {
		return errors.New("catalog window stored verifier read through supplied tx refused")
	}
	inTxCtx, cancelInTx := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	inTx, inTxErr := borrowedOwnerRotationReadRoleState(inTxCtx, tx, f.writerRole)
	cancelInTx()
	if inTxErr != nil {
		return errors.New("catalog window in-transaction W role state read refused")
	}
	if !borrowedFenceGapRoleStateEqual(inTx, expectedState) {
		return errors.New("catalog window W role facts changed under the same-owner fence")
	}
	if subtle.ConstantTimeCompare([]byte(storedVerifier), []byte(expectedVerifier)) != 1 {
		return fmt.Errorf("%w (observed_length=%d expected_length=%d)", errBorrowedFenceGapVerifierDrift, len(storedVerifier), len(expectedVerifier))
	}
	return nil
}

// borrowedReplacementCatalogWindowShareRefusal is the STRUCTURED SHARE
// acquisition refusal: it carries the actual stage, the actual SQLSTATE and
// the cause, so callers classify by the exact stage/code and never by a
// formatted-string substring search.
type borrowedReplacementCatalogWindowShareRefusal struct {
	stage string
	code  string
	cause error
}

func (e *borrowedReplacementCatalogWindowShareRefusal) Error() string {
	return "catalog window SHARE NOWAIT refused at stage " + e.stage + " (sqlstate=" + e.code + ")"
}

func (e *borrowedReplacementCatalogWindowShareRefusal) Unwrap() error { return e.cause }

// borrowedReplacementCatalogWindowTailHook is the test-only nil-default tail
// hook invoked after the proven Use completion and the successful supplied-tx
// facts recheck, before the owner callback returns. It is error-returning: a
// non-nil error refuses the owner transaction. It is read atomically and is nil
// in normal runs.
var borrowedReplacementCatalogWindowTailHook atomic.Pointer[func(context.Context, pgx.Tx) error]

func borrowedReplacementCatalogWindowPauseAtTail(ctx context.Context, tx pgx.Tx) error {
	if hook := borrowedReplacementCatalogWindowTailHook.Load(); hook != nil {
		return (*hook)(ctx, tx)
	}
	return nil
}

// borrowedReplacementCatalogWindowOwnerTx enters the original owner
// transaction through the real caller-owned lock session and orchestrates the
// protected window: supplied-tx owner/incarnation/advisory checks, SHARE
// NOWAIT before the reads, complete P1 facts/verifier validation before the
// Use is released, the first external mutation refusal, the pre-probe release,
// the post-probe notification, the optional post-probe loss injection, the
// second mutation refusal, the post-probe release, the bounded in-window join
// of the Use, and the supplied-tx P1 recheck before return. The callback never
// calls the full Use, Health, anchor recheck or prefix inspection.
func borrowedReplacementCatalogWindowOwnerTx(ctx context.Context, t *testing.T, b *borrowedSuccessorBaseline, barrier *borrowedReplacementCatalogWindowBarrier, useClosed <-chan struct{}, expectedVerifier string, expectedState borrowedOwnerRotationRoleState, postProbeLoss func(context.Context) error) error {
	f := b.fixture
	owner := b.owner
	var callbackReached int32
	txErr := f.lock.WithTransaction(ctx, func(callbackCtx context.Context, tx pgx.Tx) error {
		atomic.AddInt32(&callbackReached, 1)
		var (
			observedPID     int
			ownerStart      time.Time
			controlDBOID    uint32
			postmasterStart time.Time
			systemID        string
		)
		identityCtx, cancelIdentity := context.WithTimeout(callbackCtx, borrowedOwnerRotationQueryBudget)
		identityErr := tx.QueryRow(identityCtx, `
SELECT pid::int, backend_start, (SELECT oid FROM pg_database WHERE datname = current_database()), pg_postmaster_start_time()
FROM pg_stat_activity WHERE pid = pg_backend_pid()`).
			Scan(&observedPID, &ownerStart, &controlDBOID, &postmasterStart)
		cancelIdentity()
		if identityErr != nil {
			return errors.New("catalog window owner identity read through supplied tx refused")
		}
		systemCtx, cancelSystem := context.WithTimeout(callbackCtx, borrowedOwnerRotationQueryBudget)
		systemErr := tx.QueryRow(systemCtx, `SELECT system_identifier::text FROM pg_control_system()`).Scan(&systemID)
		cancelSystem()
		if systemErr != nil {
			return errors.New("catalog window cluster identity read through supplied tx refused")
		}
		if observedPID != owner.BackendPID || !ownerStart.Equal(owner.BackendStart) || controlDBOID != owner.ControlDatabaseOID ||
			!postmasterStart.Equal(owner.PostmasterStart) || systemID != owner.SystemIdentifier {
			return errors.New("catalog window original control owner identity changed")
		}
		var namespaceHeld bool
		namespaceCtx, cancelNamespace := context.WithTimeout(callbackCtx, borrowedOwnerRotationQueryBudget)
		namespaceErr := tx.QueryRow(namespaceCtx, `
SELECT EXISTS (
  SELECT 1 FROM pg_locks
  WHERE locktype = 'advisory' AND granted AND pid = pg_backend_pid()
    AND objsubid = 2 AND classid = $1::oid AND objid = $2::oid AND database = $3::oid
)`, owner.NamespaceClassID, owner.NamespaceObjectID, owner.NamespaceDatabaseID).Scan(&namespaceHeld)
		cancelNamespace()
		if namespaceErr != nil {
			return errors.New("catalog window advisory namespace read through supplied tx refused")
		}
		if !namespaceHeld {
			return errors.New("catalog window original granted advisory namespace is not held")
		}
		// SHARE NOWAIT strictly BEFORE reading the verifier or the role facts.
		fenceCtx, cancelFence := context.WithTimeout(callbackCtx, borrowedOwnerRotationQueryBudget)
		_, fenceErr := tx.Exec(fenceCtx, `LOCK TABLE pg_catalog.pg_authid, pg_catalog.pg_auth_members IN SHARE MODE NOWAIT`)
		cancelFence()
		if fenceErr != nil {
			return &borrowedReplacementCatalogWindowShareRefusal{stage: "share-nowait", code: borrowedAuthSQLState(fenceErr), cause: fenceErr}
		}
		// Complete committed P1 facts/verifier validation BEFORE releasing Use.
		if err := borrowedReplacementCatalogWindowValidateFacts(callbackCtx, tx, f, expectedVerifier, expectedState); err != nil {
			return err
		}
		// First protected-window external mutation refusal (exact 55P03).
		if err := borrowedFenceGapCrossDBMutatorRefusal(callbackCtx, t, f, expectedState.verifier); err != nil {
			return err
		}
		// Release the Use pre-probe; the Use runs its SELECT 1 and post-checks.
		barrier.releasePre()
		waitCtx, cancelWait := context.WithTimeout(callbackCtx, 60*time.Second)
		select {
		case <-barrier.postEntered:
		case <-useClosed:
			cancelWait()
			return errors.New("catalog window use completed before the post-probe notification")
		case <-waitCtx.Done():
			cancelWait()
			return errors.New("catalog window post-probe notification did not arrive")
		}
		cancelWait()
		// Optional ACTUAL post-probe loss injection: the Use has already
		// executed exactly one probe at this point (the post-probe notification
		// fired after executeProbe), and the loss is injected before the second
		// mutation refusal.
		if postProbeLoss != nil {
			if err := postProbeLoss(callbackCtx); err != nil {
				return fmt.Errorf("catalog window post-probe loss injection refused: %w", err)
			}
		}
		// Second external mutation refusal while the SHARE fence is held.
		if err := borrowedFenceGapCrossDBMutatorRefusal(callbackCtx, t, f, expectedState.verifier); err != nil {
			return err
		}
		// Release the Use publication and bounded-join it inside the window.
		barrier.releasePost()
		joinCtx, cancelJoin := context.WithTimeout(callbackCtx, 60*time.Second)
		select {
		case <-useClosed:
		case <-joinCtx.Done():
			cancelJoin()
			return errors.New("catalog window use did not complete inside the fence")
		}
		cancelJoin()
		// Recheck the P1 facts through the supplied tx before return.
		if err := borrowedReplacementCatalogWindowValidateFacts(callbackCtx, tx, f, expectedVerifier, expectedState); err != nil {
			return err
		}
		// Test-only nil-default tail hook: invoked AFTER the proven Use
		// completion and the successful supplied-tx facts recheck, BEFORE the
		// owner callback returns. It is read atomically and is nil in normal
		// runs, so the existing behavior is unchanged when unset.
		return borrowedReplacementCatalogWindowPauseAtTail(callbackCtx, tx)
	})
	if txErr == nil && atomic.LoadInt32(&callbackReached) != 1 {
		return fmt.Errorf("catalog window owner callback reached %d times, want exactly 1", atomic.LoadInt32(&callbackReached))
	}
	return txErr
}

// borrowedReplacementCatalogWindowOutcome is the common composite
// orchestration outcome: the retained connection/registration, the owner
// transaction error, the Use error and the actual probe executions.
type borrowedReplacementCatalogWindowOutcome struct {
	conn            *pgx.Conn
	reg             *borrowedReplacementSessionRegistration
	ownerErr        error
	useErr          error
	probeExecutions int32
}

// borrowedReplacementCatalogWindowComposite is the common error-returning
// composite orchestration: the Use is parked at the pre-probe barrier, the
// owner-fenced window runs (with the optional post-probe loss injection), and
// the outcome preserves BOTH errors so success requires BOTH the Use and the
// owner transaction to succeed; a commit ambiguity is never success.
func borrowedReplacementCatalogWindowComposite(ctx context.Context, t *testing.T, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding, postProbeLoss func(context.Context) error) (*borrowedReplacementCatalogWindowOutcome, error) {
	t.Helper()
	conn, reg := borrowedReplacementSessionRegisterP1(t, ctx, b, fresh)
	barrier := installBorrowedReplacementCatalogWindowBarrier(t, reg)
	useCtx, cancelUse := context.WithCancel(ctx)
	var useErr error
	useClosed := make(chan struct{})
	go func() {
		useErr = reg.Use(useCtx)
		close(useClosed)
	}()
	joined := false
	t.Cleanup(func() {
		cancelUse()
		barrier.releaseAll()
		if joined {
			return
		}
		select {
		case <-useClosed:
		case <-time.After(30 * time.Second):
			t.Errorf("catalog window composite use join is unknown after the bounded cleanup join")
		}
	})
	select {
	case <-barrier.preEntered:
	case <-time.After(120 * time.Second):
		return nil, errors.New("catalog window use never reached the pre-probe barrier")
	}
	ownerCtx, cancelOwner := context.WithTimeout(ctx, 180*time.Second)
	ownerErr := borrowedReplacementCatalogWindowOwnerTx(ownerCtx, t, b, barrier, useClosed, b.verifierP1, b.preState, postProbeLoss)
	cancelOwner()
	cancelUse()
	barrier.releaseAll()
	select {
	case <-useClosed:
		joined = true
	case <-time.After(30 * time.Second):
		return nil, errors.New("catalog window use did not complete within the bounded join")
	}
	return &borrowedReplacementCatalogWindowOutcome{
		conn: conn, reg: reg, ownerErr: ownerErr, useErr: useErr,
		probeExecutions: atomic.LoadInt32(&reg.state.probeExecutions),
	}, nil
}

// borrowedReplacementCatalogWindowCompositePositive is the positive composite:
// BOTH the Use and the owner transaction must succeed with exactly one probe
// execution, followed by the after-return fresh prefix/anchor/owner-health
// rechecks. It returns the retained connection and registration for the replay
// checks.
func borrowedReplacementCatalogWindowCompositePositive(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding) (*pgx.Conn, *borrowedReplacementSessionRegistration) {
	t.Helper()
	outcome, runErr := borrowedReplacementCatalogWindowComposite(ctx, t, b, fresh, nil)
	if runErr != nil {
		t.Fatalf("catalog window composite orchestration refused: %v", runErr)
	}
	if outcome.ownerErr != nil {
		t.Fatalf("catalog window owner transaction refused: %v", outcome.ownerErr)
	}
	if outcome.useErr != nil {
		t.Fatalf("catalog window use refused: %v", outcome.useErr)
	}
	if outcome.probeExecutions != 1 {
		t.Fatalf("catalog window composite probe executions=%d, want exactly 1", outcome.probeExecutions)
	}
	if outcome.reg.state.invalidReasonNow() != "" {
		t.Fatalf("catalog window composite left the session invalidated: %s", outcome.reg.state.invalidReasonNow())
	}
	// After return: fresh prefix, original anchor and owner health rechecked.
	prefixCtx, cancelPrefix := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	prefixErr := fresh.prefix.Recheck(t, prefixCtx)
	cancelPrefix()
	if prefixErr != nil {
		t.Fatalf("catalog window fresh prefix recheck after the composite refused: %v", prefixErr)
	}
	anchorCtx, cancelAnchor := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	anchorErr := fresh.anchor.Recheck(anchorCtx)
	cancelAnchor()
	if anchorErr != nil {
		t.Fatalf("catalog window original anchor recheck after the composite refused: %v", anchorErr)
	}
	healthCtx, cancelHealth := context.WithTimeout(ctx, borrowedOwnerRotationHealthBudget)
	healthErr := b.fixture.lock.Health(healthCtx)
	cancelHealth()
	if healthErr != nil {
		t.Fatalf("catalog window owner health after the composite refused: %v", healthErr)
	}
	t.Logf("catalog window composite published: use probe executions=%d, owner SHARE committed, fresh prefix/anchor/owner health retained", outcome.probeExecutions)
	return outcome.conn, outcome.reg
}

// borrowedReplacementCatalogWindowReplayRefuse proves the composite session is
// one-use: copies and reconstructions refuse after the successful composite.
func borrowedReplacementCatalogWindowReplayRefuse(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding, conn *pgx.Conn, reg *borrowedReplacementSessionRegistration) {
	t.Helper()
	replayCtx, cancelReplay := context.WithTimeout(ctx, 30*time.Second)
	replayErr := reg.Use(replayCtx)
	copied := *reg
	copyErr := copied.Use(replayCtx)
	cancelReplay()
	if replayErr == nil || copyErr == nil {
		t.Fatal("catalog window composite replay/copy use was accepted")
	}
	if got := atomic.LoadInt32(&reg.state.probeExecutions); got != 1 {
		t.Fatalf("catalog window replay executed the probe: executions=%d", got)
	}
	if _, reconErr := newBorrowedReplacementSessionRegistration(t, ctx, b, fresh, conn); reconErr == nil {
		t.Fatal("catalog window composite reconstruction minted a fresh session registration")
	}
	t.Logf("catalog window composite replay/copy/reconstruction refused")
}

// borrowedReplacementCatalogWindowRowExclusiveConflict proves an external
// ROW EXCLUSIVE holder makes the owner SHARE NOWAIT refuse with the exact
// 55P03 while the Use stays parked pre-probe (executions=0, never composite
// success).
func borrowedReplacementCatalogWindowRowExclusiveConflict(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding) {
	t.Helper()
	f := b.fixture
	conn, reg := borrowedReplacementSessionRegisterP1(t, ctx, b, fresh)
	barrier := installBorrowedReplacementCatalogWindowBarrier(t, reg)
	useCtx, cancelUse := context.WithCancel(ctx)
	useClosed := make(chan struct{})
	var useErr error
	go func() {
		useErr = reg.Use(useCtx)
		close(useClosed)
	}()
	joined := false
	t.Cleanup(func() {
		cancelUse()
		barrier.releaseAll()
		if joined {
			return
		}
		select {
		case <-useClosed:
		case <-time.After(30 * time.Second):
			t.Errorf("row-exclusive conflict use join is unknown after the bounded cleanup join")
		}
	})
	select {
	case <-barrier.preEntered:
	case <-time.After(120 * time.Second):
		t.Errorf("row-exclusive conflict use never reached the pre-probe barrier; completion unknown")
		return
	}
	holder := borrowedOwnerDDLConnect(t, ctx, f.controlDSN)
	holderCtx, cancelHolder := context.WithTimeout(ctx, borrowedOwnerRotationAuthBudget)
	holderTx, holderErr := holder.Begin(holderCtx)
	cancelHolder()
	if holderErr != nil {
		t.Fatalf("row-exclusive holder begin refused: %v", holderErr)
	}
	// Independent bounded rollback/close cleanup registered IMMEDIATELY after
	// the holder Begin succeeded: every exit path uses it, all cleanup errors
	// are preserved and an unexpected cleanup failure fails the control.
	holderCleaned := false
	holderCleanup := func() error {
		if holderCleaned {
			return nil
		}
		holderCleaned = true
		cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), borrowedOwnerRotationCleanupBudget)
		defer cancelCleanup()
		rollbackErr := holderTx.Rollback(cleanupCtx)
		closeErr := holder.Close(cleanupCtx)
		return errors.Join(rollbackErr, closeErr)
	}
	t.Cleanup(func() {
		if err := holderCleanup(); err != nil {
			t.Errorf("row-exclusive holder cleanup refused: %v", err)
		}
	})
	lockCtx, cancelLock := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	_, lockErr := holderTx.Exec(lockCtx, `LOCK TABLE pg_catalog.pg_authid IN ROW EXCLUSIVE MODE`)
	cancelLock()
	if lockErr != nil {
		cleanupErr := holderCleanup()
		if cleanupErr != nil {
			t.Fatalf("row-exclusive holder lock refused: %v; cleanup refused: %v", lockErr, cleanupErr)
		}
		t.Fatalf("row-exclusive holder lock refused: %v", lockErr)
	}
	ownerCtx, cancelOwner := context.WithTimeout(ctx, 60*time.Second)
	ownerErr := borrowedReplacementCatalogWindowOwnerTx(ownerCtx, t, b, barrier, useClosed, b.verifierP1, b.preState, nil)
	cancelOwner()
	if cleanupErr := holderCleanup(); cleanupErr != nil {
		t.Fatalf("row-exclusive holder cleanup refused: %v", cleanupErr)
	}
	// STRUCTURED SHARE refusal: the exact stage and code 55P03, never a
	// formatted-string substring search.
	var shareRefusal *borrowedReplacementCatalogWindowShareRefusal
	if ownerErr == nil || !errors.As(ownerErr, &shareRefusal) {
		t.Fatalf("row-exclusive conflict was not refused with a structured SHARE refusal: %v", ownerErr)
	}
	if shareRefusal.stage != "share-nowait" || shareRefusal.code != "55P03" {
		t.Fatalf("row-exclusive SHARE refusal stage/code: stage=%q code=%q, want share-nowait/55P03", shareRefusal.stage, shareRefusal.code)
	}
	cancelUse()
	barrier.releaseAll()
	select {
	case <-useClosed:
		joined = true
	case <-time.After(30 * time.Second):
		t.Errorf("row-exclusive conflict use did not complete within the bounded join; outcome unknown")
		return
	}
	if useErr == nil {
		t.Fatal("row-exclusive conflict use was accepted")
	}
	if got := atomic.LoadInt32(&reg.state.probeExecutions); got != 0 {
		t.Fatalf("row-exclusive conflict executed the probe %d times, want 0", got)
	}
	borrowedSuccessorCloseConn(t, conn)
	t.Logf("row-exclusive conflict: owner SHARE NOWAIT refused with 55P03 and the parked use refused pre-execution (executions=0)")
}

// borrowedReplacementCatalogWindowCancelRefuse proves a caller cancellation at
// the pre-probe park refuses with zero probe executions and no composite
// publication (cancel-before-release bounded join).
func borrowedReplacementCatalogWindowCancelRefuse(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding) {
	t.Helper()
	conn, reg := borrowedReplacementSessionRegisterP1(t, ctx, b, fresh)
	barrier := installBorrowedReplacementCatalogWindowBarrier(t, reg)
	useCtx, cancelUse := context.WithCancel(ctx)
	useClosed := make(chan struct{})
	var useErr error
	go func() {
		useErr = reg.Use(useCtx)
		close(useClosed)
	}()
	joined := false
	t.Cleanup(func() {
		cancelUse()
		barrier.releaseAll()
		if joined {
			return
		}
		select {
		case <-useClosed:
		case <-time.After(30 * time.Second):
			t.Errorf("cancellation use join is unknown after the bounded cleanup join")
		}
	})
	select {
	case <-barrier.preEntered:
	case <-time.After(120 * time.Second):
		t.Errorf("cancellation use never reached the pre-probe barrier; completion unknown")
		return
	}
	cancelUse()
	barrier.releaseAll()
	select {
	case <-useClosed:
		joined = true
	case <-time.After(30 * time.Second):
		t.Errorf("cancellation use did not complete within the bounded join; outcome unknown")
		return
	}
	if useErr == nil {
		t.Fatal("cancellation published a successful use")
	}
	if got := atomic.LoadInt32(&reg.state.probeExecutions); got != 0 {
		t.Fatalf("cancellation executed the probe %d times, want 0", got)
	}
	borrowedSuccessorCloseConn(t, conn)
	t.Logf("cancellation at the pre-probe park refused with zero probe executions and no composite publication")
}

// borrowedReplacementCatalogWindowIncompleteRefuse proves injected incomplete
// strict evidence refuses before the pre-probe park (zero probe executions,
// never composite success) and that the failed copy/reconstruction refuse.
func borrowedReplacementCatalogWindowIncompleteRefuse(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding) {
	t.Helper()
	conn, reg := borrowedReplacementSessionRegisterP1(t, ctx, b, fresh)
	reg.state.statFn = func(context.Context, *borrowedReplacementSessionState) (string, uint64, error) {
		return "", 0, errors.New("injected incomplete strict stat failure")
	}
	useCtx, cancelUse := context.WithTimeout(ctx, 60*time.Second)
	useErr := reg.Use(useCtx)
	cancelUse()
	if useErr == nil {
		t.Fatal("incomplete strict evidence was accepted as a catalog window use")
	}
	if got := atomic.LoadInt32(&reg.state.probeExecutions); got != 0 {
		t.Fatalf("incomplete strict evidence executed the probe %d times, want 0", got)
	}
	copied := *reg
	if copied.Use(ctx) == nil {
		t.Fatal("incomplete-evidence copy rehabilitated the session registration")
	}
	if _, reconErr := newBorrowedReplacementSessionRegistration(t, ctx, b, fresh, conn); reconErr == nil {
		t.Fatal("incomplete-evidence reconstruction minted a fresh session registration")
	}
	borrowedSuccessorCloseConn(t, conn)
	t.Logf("incomplete checks refused before the pre-probe park with zero probe executions; copy/reconstruction refused")
}

// borrowedReplacementCatalogWindowPrefixLossRefuse proves an actual
// copied-prefix loss observed at the pre-probe park refuses with zero probe
// executions and never composite success (LAST on the fixture: it permanently
// invalidates the shared replacement prefix).
func borrowedReplacementCatalogWindowPrefixLossRefuse(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding) {
	t.Helper()
	conn, reg := borrowedReplacementSessionRegisterP1(t, ctx, b, fresh)
	barrier := installBorrowedReplacementCatalogWindowBarrier(t, reg)
	useCtx, cancelUse := context.WithCancel(ctx)
	useClosed := make(chan struct{})
	var useErr error
	go func() {
		useErr = reg.Use(useCtx)
		close(useClosed)
	}()
	joined := false
	t.Cleanup(func() {
		cancelUse()
		barrier.releaseAll()
		if joined {
			return
		}
		select {
		case <-useClosed:
		case <-time.After(30 * time.Second):
			t.Errorf("prefix-loss use join is unknown after the bounded cleanup join")
		}
	})
	select {
	case <-barrier.preEntered:
	case <-time.After(120 * time.Second):
		t.Errorf("prefix-loss use never reached the pre-probe barrier; completion unknown")
		return
	}
	borrowedReplacementSessionCopiedPrefixLoss(t, fresh)
	barrier.releaseAll()
	select {
	case <-useClosed:
		joined = true
	case <-time.After(30 * time.Second):
		t.Errorf("prefix-loss use did not complete within the bounded join; outcome unknown")
		return
	}
	if useErr == nil {
		t.Fatal("shared-prefix loss published a successful use")
	}
	if got := atomic.LoadInt32(&reg.state.probeExecutions); got != 0 {
		t.Fatalf("shared-prefix loss executed the probe %d times, want 0", got)
	}
	if _, reconErr := newBorrowedReplacementSessionRegistration(t, ctx, b, fresh, conn); reconErr == nil {
		t.Fatal("shared-prefix loss reconstruction minted a fresh session registration")
	}
	borrowedSuccessorCloseConn(t, conn)
	t.Logf("shared-prefix loss at the pre-probe park refused with zero probe executions and no composite publication")
}

// borrowedReplacementCatalogWindowDriftRefuse proves a committed verifier
// drift while the Use is parked pre-probe refuses the owner transaction with
// the shared drift sentinel before any release; the Use is canceled with zero
// probe executions and there is never composite success (fresh destructive
// fixture).
func borrowedReplacementCatalogWindowDriftRefuse(t *testing.T, ctx context.Context) {
	t.Helper()
	b := newBorrowedSuccessorBaseline(t, ctx)
	orchestrateCtx, cancelOrchestrate := borrowedReplacementOrchestrationContext(ctx)
	var stages int32
	fresh, err := borrowedReplacementBindingOrchestrate(orchestrateCtx, t, b, &stages)
	cancelOrchestrate()
	if err != nil || fresh == nil {
		t.Fatalf("catalog window drift pipeline refused: capture=%v err=%v", fresh, err)
	}
	conn, reg := borrowedReplacementSessionRegisterP1(t, ctx, b, fresh)
	barrier := installBorrowedReplacementCatalogWindowBarrier(t, reg)
	useCtx, cancelUse := context.WithCancel(ctx)
	useClosed := make(chan struct{})
	var useErr error
	go func() {
		useErr = reg.Use(useCtx)
		close(useClosed)
	}()
	joined := false
	t.Cleanup(func() {
		cancelUse()
		barrier.releaseAll()
		if joined {
			return
		}
		select {
		case <-useClosed:
		case <-time.After(30 * time.Second):
			t.Errorf("drift use join is unknown after the bounded cleanup join")
		}
	})
	select {
	case <-barrier.preEntered:
	case <-time.After(120 * time.Second):
		t.Errorf("drift use never reached the pre-probe barrier; completion unknown")
		return
	}
	// Committed drift while parked (no fence is held yet).
	driftCtx, cancelDrift := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	_, driftErr := b.fixture.fx.admin.Exec(driftCtx, `ALTER ROLE `+pgx.Identifier{b.fixture.writerRole}.Sanitize()+` PASSWORD `+sqlLiteral(b.preState.verifier))
	cancelDrift()
	if driftErr != nil {
		t.Fatalf("catalog window drift mutation refused: %v", driftErr)
	}
	ownerCtx, cancelOwner := context.WithTimeout(ctx, 60*time.Second)
	ownerErr := borrowedReplacementCatalogWindowOwnerTx(ownerCtx, t, b, barrier, useClosed, b.verifierP1, b.preState, nil)
	cancelOwner()
	if !errors.Is(ownerErr, errBorrowedFenceGapVerifierDrift) {
		t.Fatalf("committed drift while parked was not refused with the shared drift sentinel: %v", ownerErr)
	}
	cancelUse()
	barrier.releaseAll()
	select {
	case <-useClosed:
		joined = true
	case <-time.After(30 * time.Second):
		t.Errorf("drift use did not complete within the bounded join; outcome unknown")
		return
	}
	if useErr == nil {
		t.Fatal("committed drift while parked published a successful use")
	}
	if got := atomic.LoadInt32(&reg.state.probeExecutions); got != 0 {
		t.Fatalf("committed drift executed the probe %d times, want 0", got)
	}
	borrowedSuccessorCloseConn(t, conn)
	borrowedOwnerDDLGuard(t, ctx, b)
	t.Logf("committed drift while parked: owner transaction refused with the drift sentinel before any release; the parked use was canceled with zero probe executions and no composite success")
}

// borrowedReplacementCatalogWindowUnknownShareRefusalRefuse proves an
// unrelated/unknown error that merely mentions 55P03 (a formatted string or a
// mutator outcome with an unknown/different result) is NOT accepted as a
// structured SHARE-acquisition refusal; only the exact structured stage/code
// is accepted.
func borrowedReplacementCatalogWindowUnknownShareRefusalRefuse(t *testing.T) {
	t.Helper()
	unknown := errors.New("unrelated mutator failure mentioning 55P03 with an unknown outcome")
	var shareRefusal *borrowedReplacementCatalogWindowShareRefusal
	if errors.As(unknown, &shareRefusal) {
		t.Fatal("unrelated error mentioning 55P03 was accepted as a structured SHARE refusal")
	}
	formatted := fmt.Errorf("catalog window SHARE NOWAIT refused: sqlstate=55P03 (formatted string only): %w", unknown)
	if errors.As(formatted, &shareRefusal) {
		t.Fatal("formatted SHARE-like string was accepted as a structured SHARE refusal")
	}
	mutatorOutcome := fmt.Errorf("cross-db mutator was not refused with 55P03 (sqlstate=%s)", "08006")
	if errors.As(mutatorOutcome, &shareRefusal) {
		t.Fatal("mutator outcome mentioning 55P03 was accepted as a structured SHARE refusal")
	}
	structured := &borrowedReplacementCatalogWindowShareRefusal{stage: "share-nowait", code: "55P03", cause: unknown}
	if !errors.As(structured, &shareRefusal) || shareRefusal.stage != "share-nowait" || shareRefusal.code != "55P03" {
		t.Fatal("structured SHARE refusal was not recognized with its exact stage and code")
	}
	t.Logf("unknown/unrelated SHARE-refusal rejection control: string-matched 55P03 errors are not accepted; only the structured refusal with the exact stage and code is")
}

// borrowedReplacementCatalogWindowOwnerLossAtPostProbeRefuse proves an ACTUAL
// owner loss injected at the post-probe barrier (after exactly one probe
// execution) fails the owner window with bounded termination and NO composite
// publication. It does NOT establish COMMIT acknowledgement-loss handling: the
// loss is injected before the owner transaction commit, so a lost commit
// acknowledgement remains outside this lane's evidence (fresh destructive
// fixture).
func borrowedReplacementCatalogWindowOwnerLossAtPostProbeRefuse(t *testing.T, ctx context.Context) {
	t.Helper()
	b := newBorrowedSuccessorBaseline(t, ctx)
	orchestrateCtx, cancelOrchestrate := borrowedReplacementOrchestrationContext(ctx)
	var stages int32
	fresh, err := borrowedReplacementBindingOrchestrate(orchestrateCtx, t, b, &stages)
	cancelOrchestrate()
	if err != nil || fresh == nil {
		t.Fatalf("catalog window post-probe owner-loss pipeline refused: capture=%v err=%v", fresh, err)
	}
	controlPID := fresh.binding.ControlBackendPID()
	if controlPID <= 0 || controlPID != b.owner.BackendPID {
		t.Fatalf("catalog window post-probe owner-loss control PID %d is missing or not the anchored owner PID %d", controlPID, b.owner.BackendPID)
	}
	outcome, runErr := borrowedReplacementCatalogWindowComposite(ctx, t, b, fresh, func(lossCtx context.Context) error {
		var terminated bool
		termCtx, cancelTerm := context.WithTimeout(lossCtx, borrowedOwnerRotationQueryBudget)
		termErr := b.fixture.controlPool.QueryRow(termCtx, `SELECT pg_terminate_backend($1)`, controlPID).Scan(&terminated)
		cancelTerm()
		if termErr != nil || !terminated {
			return fmt.Errorf("catalog window post-probe owner-loss terminate refused: terminated=%t err=%v", terminated, termErr)
		}
		return nil
	})
	if runErr != nil {
		t.Fatalf("catalog window post-probe owner-loss orchestration refused: %v", runErr)
	}
	if outcome.probeExecutions != 1 {
		t.Fatalf("catalog window post-probe owner loss probe executions=%d, want exactly 1", outcome.probeExecutions)
	}
	if outcome.ownerErr == nil {
		t.Fatal("owner window succeeded after the actual post-probe owner loss")
	}
	if outcome.useErr == nil {
		t.Fatal("parked use succeeded after the actual post-probe owner loss")
	}
	if outcome.reg.state.invalidReasonNow() == "" {
		t.Fatal("post-probe owner loss did not leave the session permanently refused")
	}
	healthCtx, cancelHealth := context.WithTimeout(ctx, borrowedOwnerRotationHealthBudget)
	healthErr := b.fixture.lock.Health(healthCtx)
	cancelHealth()
	if healthErr == nil {
		t.Fatal("owner health remained successful after the actual post-probe owner loss")
	}
	anchorCtx, cancelAnchor := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	anchorErr := fresh.anchor.Recheck(anchorCtx)
	cancelAnchor()
	if anchorErr == nil {
		t.Fatal("original anchor rechecked after the actual post-probe owner loss")
	}
	borrowedSuccessorCloseConn(t, outcome.conn)
	t.Logf("post-probe owner loss: actual owner termination after exactly one probe execution failed the owner window and the parked use with bounded termination and no composite publication (COMMIT acknowledgement-loss handling is NOT established)")
}

// borrowedReplacementCatalogWindowOwnerLossRefuse proves actual control owner
// loss refuses the owner transaction and the parked use with zero probe
// executions and no composite success (fresh destructive fixture).
func borrowedReplacementCatalogWindowOwnerLossRefuse(t *testing.T, ctx context.Context) {
	t.Helper()
	b := newBorrowedSuccessorBaseline(t, ctx)
	orchestrateCtx, cancelOrchestrate := borrowedReplacementOrchestrationContext(ctx)
	var stages int32
	fresh, err := borrowedReplacementBindingOrchestrate(orchestrateCtx, t, b, &stages)
	cancelOrchestrate()
	if err != nil || fresh == nil {
		t.Fatalf("catalog window owner-loss pipeline refused: capture=%v err=%v", fresh, err)
	}
	conn, reg := borrowedReplacementSessionRegisterP1(t, ctx, b, fresh)
	barrier := installBorrowedReplacementCatalogWindowBarrier(t, reg)
	useCtx, cancelUse := context.WithCancel(ctx)
	useClosed := make(chan struct{})
	var useErr error
	go func() {
		useErr = reg.Use(useCtx)
		close(useClosed)
	}()
	joined := false
	t.Cleanup(func() {
		cancelUse()
		barrier.releaseAll()
		if joined {
			return
		}
		select {
		case <-useClosed:
		case <-time.After(30 * time.Second):
			t.Errorf("owner-loss use join is unknown after the bounded cleanup join")
		}
	})
	select {
	case <-barrier.preEntered:
	case <-time.After(120 * time.Second):
		t.Errorf("owner-loss use never reached the pre-probe barrier; completion unknown")
		return
	}
	controlPID := fresh.binding.ControlBackendPID()
	if controlPID <= 0 || controlPID != b.owner.BackendPID {
		t.Fatalf("catalog window owner-loss control PID %d is missing or not the anchored owner PID %d", controlPID, b.owner.BackendPID)
	}
	var terminated bool
	lossCtx, cancelLoss := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	lossErr := b.fixture.controlPool.QueryRow(lossCtx, `SELECT pg_terminate_backend($1)`, controlPID).Scan(&terminated)
	cancelLoss()
	if lossErr != nil || !terminated {
		t.Fatalf("catalog window owner-loss terminate refused: terminated=%t err=%v", terminated, lossErr)
	}
	ownerCtx, cancelOwner := context.WithTimeout(ctx, 60*time.Second)
	ownerErr := borrowedReplacementCatalogWindowOwnerTx(ownerCtx, t, b, barrier, useClosed, b.verifierP1, b.preState, nil)
	cancelOwner()
	if ownerErr == nil {
		t.Fatal("owner loss was accepted by the owner transaction")
	}
	cancelUse()
	barrier.releaseAll()
	select {
	case <-useClosed:
		joined = true
	case <-time.After(30 * time.Second):
		t.Errorf("owner-loss use did not complete within the bounded join; outcome unknown")
		return
	}
	if useErr == nil {
		t.Fatal("owner loss published a successful use")
	}
	if got := atomic.LoadInt32(&reg.state.probeExecutions); got != 0 {
		t.Fatalf("owner loss executed the probe %d times, want 0", got)
	}
	healthCtx, cancelHealth := context.WithTimeout(ctx, borrowedOwnerRotationHealthBudget)
	healthErr := b.fixture.lock.Health(healthCtx)
	cancelHealth()
	if healthErr == nil {
		t.Fatal("owner health remained successful after real owner loss")
	}
	borrowedSuccessorCloseConn(t, conn)
	t.Logf("owner loss: owner transaction and the parked use both refused with zero probe executions and no composite success")
}

// TestBorrowedReplacementCatalogWindow is the bounded owner-held catalog fence
// across replacement-session use lane described in the file header.
func TestBorrowedReplacementCatalogWindow(t *testing.T) {
	ctx := t.Context()

	// Genuine replacement capture: the actual bounded shared orchestration with
	// stage counters; never a projection.
	b := newBorrowedSuccessorBaseline(t, ctx)
	originalTargetDSN := b.fixture.writerTargetDSN
	originalObserverDSN := b.fixture.observerTargetDSN
	var stages int32
	orchestrateCtx, cancelOrchestrate := borrowedReplacementOrchestrationContext(ctx)
	fresh, err := borrowedReplacementBindingOrchestrate(orchestrateCtx, t, b, &stages)
	cancelOrchestrate()
	if err != nil {
		t.Fatalf("catalog window genuine capture refused: %v", err)
	}
	if fresh == nil {
		t.Fatal("catalog window genuine capture produced no capture")
	}
	if b.fixture.writerTargetDSN != originalTargetDSN || b.fixture.observerTargetDSN != originalObserverDSN {
		t.Fatal("catalog window lane rewrote an original fixture DSN")
	}
	if got := atomic.LoadInt32(&stages); got != borrowedReplacementStagePrefixCaptured {
		t.Fatalf("catalog window pipeline stages=%d, want %d", got, borrowedReplacementStagePrefixCaptured)
	}
	if fresh.binding.TargetDatabaseOID() != fresh.replacementOID || fresh.replacementOID == fresh.oldOID {
		t.Fatalf("catalog window capture is not replacement-bound: replacement=%d old=%d binding=%d", fresh.replacementOID, fresh.oldOID, fresh.binding.TargetDatabaseOID())
	}
	t.Logf("catalog window genuine capture: old OID %d -> replacement OID %d", fresh.oldOID, fresh.replacementOID)

	// Positive composite: parked Use + owner-fenced window -> composite success.
	conn, reg := borrowedReplacementCatalogWindowCompositePositive(t, ctx, b, fresh)
	borrowedReplacementCatalogWindowReplayRefuse(t, ctx, b, fresh, conn, reg)

	// Retire via the session lane: close, incarnation gone, owner health,
	// guard non-clean, acceptance zero.
	borrowedReplacementSessionRetire(t, ctx, b, conn, reg.state)

	// Non-destructive negatives on the same fixture.
	borrowedReplacementCatalogWindowRowExclusiveConflict(t, ctx, b, fresh)
	borrowedReplacementCatalogWindowCancelRefuse(t, ctx, b, fresh)
	borrowedReplacementCatalogWindowIncompleteRefuse(t, ctx, b, fresh)
	borrowedReplacementCatalogWindowUnknownShareRefusalRefuse(t)

	// Shared-prefix loss LAST on this fixture.
	borrowedReplacementCatalogWindowPrefixLossRefuse(t, ctx, b, fresh)

	// Destructive negatives on fresh fixtures.
	borrowedReplacementCatalogWindowDriftRefuse(t, ctx)
	borrowedReplacementCatalogWindowOwnerLossRefuse(t, ctx)
	borrowedReplacementCatalogWindowOwnerLossAtPostProbeRefuse(t, ctx)

	// Guard unresolved and acceptance unchanged across the whole lane.
	borrowedOwnerDDLGuard(t, ctx, b)
}
