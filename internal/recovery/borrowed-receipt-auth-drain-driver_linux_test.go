//go:build linux && drill

// borrowed-receipt-auth-drain-driver_linux_test.go is the FIRST bounded native
// feasibility lane for the original-owner catalog rotation. It proves ONE
// claim with actual SQL and actual pgx PG18 clients: the retained ORIGINAL
// control owner (the same *recovery.TargetLock dedicated session and advisory
// namespace) can, inside a SINGLE transaction executed through
// TargetLock.WithTransaction, acquire a pg_catalog SHARE fence and rotate the
// restricted writer W to a fresh private P1 whose exact client-generated
// SCRAM verifier is verified in-transaction, then COMMIT. Real negative
// controls are included: a concurrent other-connection catalog write is
// refused with 55P03 while the fence is held, real P0 SCRAM authentication
// still succeeds BEFORE COMMIT (auth reads are not SHARE-blocked), a real
// ROW EXCLUSIVE NOWAIT probe proves the SHARE lock is released after COMMIT,
// and a separately owned ROW EXCLUSIVE holder makes the owner's SHARE
// acquisition fail with 55P03 before any mutation.
//
// This lane is feasibility ONLY and grants no authority: no full auth drain,
// no loaded P0, no STOP/CONT, no DDL, no guard clean, no acceptance, and no
// continuous-fence claim. A COMMIT error is UNKNOWN, never an accepted claim.
// The original fixture DSNs, keys and fingerprints are never rebound to P1;
// the P1 verification client is a private local DSN clone. No credential,
// verifier, full SQL or raw DSN is ever logged.
//
// Every catalog/auth/health step is bounded by a short deadline derived from
// the caller test context (or from the enclosing bounded barrier context for
// in-transaction work); session cleanup uses independent bounded contexts,
// and each spawned mutator registers a cancel-and-bounded-join cleanup whose
// single terminal result is published only after a bounded rollback/close.
//
// TestBorrowedAuthDrainFreezeExit adds the bounded SAME-FIXTURE freeze/drain
// feasibility lane: the genuine restricted-W receipt chain and one-time entry,
// two real staging holds (loaded-P0 SCRAM and prestartup) on the same W/target,
// a same-incarnation STOP of the exact postmaster, the loaded backend's frozen
// exit to a same-start zombie with an emptied FD table and no socket, the
// proven original-owner same-tx catalog SHARE + ALTER ROLE W P1 COMMIT under
// the freeze, CONT, authoritative reap, cooperative prestartup QUIT, bounded
// native 28P01/P1 auth checks, entry/anchor/prefix/lock rechecks, a refreeze
// strict census and a final owner-loss refusal. It is still feasibility ONLY:
// no DDL, no rebuild, no acceptance, no Gate1, no protected successor
// admission, no continuous fence. COMMIT errors remain UNKNOWN and are never
// accepted claims.
package recovery_test

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xtianxx/txharbor/internal/recovery"
)

// Bounded driver budgets. Step deadlines are derived from the caller test
// context; cleanup deadlines are independent of it so that a canceled test
// context can never prevent a bounded rollback/close.
const (
	borrowedOwnerRotationQueryBudget   = 30 * time.Second
	borrowedOwnerRotationAuthBudget    = 30 * time.Second
	borrowedOwnerRotationHealthBudget  = 15 * time.Second
	borrowedOwnerRotationCleanupBudget = 10 * time.Second
	borrowedOwnerRotationJoinBudget    = 20 * time.Second
)

// borrowedOwnerRotationMutatorResult is the single terminal result of one
// separately owned catalog mutator. refusalCode is the SQLSTATE of the real
// ALTER attempt and failure retains any pre-refusal cause plus every bounded
// rollback/close error; the result is published only after that bounded
// cleanup completed, so an unclean or unknown terminal state can never look
// like the expected 55P03 exclusion.
type borrowedOwnerRotationMutatorResult struct {
	refusalCode string
	failure     error
}

// borrowedOwnerRotationSQLStateError is a sanitized SQL failure: it displays
// only the stage and the SQLSTATE (never raw SQL, DSN or credential text)
// while still unwrapping to the original error so the SQLSTATE remains
// extractable through any wrapping.
type borrowedOwnerRotationSQLStateError struct {
	stage string
	code  string
	cause error
}

func (e *borrowedOwnerRotationSQLStateError) Error() string {
	return e.stage + " (sqlstate=" + e.code + ")"
}

func (e *borrowedOwnerRotationSQLStateError) Unwrap() error { return e.cause }

// borrowedOwnerRotationQuerier is the common read surface of a dedicated
// *pgx.Conn and the supplied pgx.Tx: the same state reader is used for the
// pre-snapshot, the in-transaction proof and the post-COMMIT verification.
type borrowedOwnerRotationQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// borrowedOwnerRotationRoleState is the read-only catalog/verifier snapshot of
// the restricted writer: OID, exact stored verifier and the complete
// membership closure in both directions. It is never logged with the verifier
// value.
type borrowedOwnerRotationRoleState struct {
	oid         uint32
	verifier    string
	login       bool
	super       bool
	createdb    bool
	createrole  bool
	inherit     bool
	replication bool
	bypass      bool
	memberships []uint32
}

func borrowedOwnerRotationReadRoleState(ctx context.Context, q borrowedOwnerRotationQuerier, role string) (borrowedOwnerRotationRoleState, error) {
	var state borrowedOwnerRotationRoleState
	var oid int64
	if err := q.QueryRow(ctx, `
SELECT oid::int8, coalesce(rolpassword,''), rolcanlogin, rolsuper, rolcreatedb, rolcreaterole, rolinherit, rolreplication, rolbypassrls
FROM pg_authid WHERE rolname=$1`, role).
		Scan(&oid, &state.verifier, &state.login, &state.super, &state.createdb, &state.createrole, &state.inherit, &state.replication, &state.bypass); err != nil {
		return state, err
	}
	if oid <= 0 {
		return state, errors.New("role OID is not positive")
	}
	state.oid = uint32(oid)
	rows, err := q.Query(ctx, `
SELECT m.roleid::int8 FROM pg_auth_members m JOIN pg_roles r ON r.oid=m.member WHERE r.rolname=$1
UNION ALL
SELECT m.member::int8 FROM pg_auth_members m JOIN pg_roles r ON r.oid=m.roleid WHERE r.rolname=$1
ORDER BY 1`, role)
	if err != nil {
		return state, err
	}
	defer rows.Close()
	for rows.Next() {
		var member int64
		if err := rows.Scan(&member); err != nil {
			return state, err
		}
		if member <= 0 {
			return state, errors.New("membership OID is not positive")
		}
		state.memberships = append(state.memberships, uint32(member))
	}
	if err := rows.Err(); err != nil {
		return state, err
	}
	return state, nil
}

// borrowedOwnerRotationAssertRoleStateUnchanged compares the immutable catalog
// facts (never the verifier value) and the exact membership closure.
func borrowedOwnerRotationAssertRoleStateUnchanged(t *testing.T, stage string, before, after borrowedOwnerRotationRoleState) {
	t.Helper()
	if before.oid != after.oid {
		t.Fatalf("%s: W role OID changed: was=%d now=%d", stage, before.oid, after.oid)
	}
	if before.login != after.login || before.super != after.super || before.createdb != after.createdb ||
		before.createrole != after.createrole || before.inherit != after.inherit ||
		before.replication != after.replication || before.bypass != after.bypass {
		t.Fatalf("%s: W role flags changed: login=%t super=%t createdb=%t createrole=%t inherit=%t replication=%t bypass=%t",
			stage, after.login, after.super, after.createdb, after.createrole, after.inherit, after.replication, after.bypass)
	}
	if len(before.memberships) != len(after.memberships) {
		t.Fatalf("%s: W membership closure size changed: was=%d now=%d", stage, len(before.memberships), len(after.memberships))
	}
	for i := range before.memberships {
		if before.memberships[i] != after.memberships[i] {
			t.Fatalf("%s: W membership closure changed at %d", stage, i)
		}
	}
}

// TestBorrowedOwnerRotationCommitFeasibility is the positive same-owner
// feasibility proof described in the file header.
func TestBorrowedOwnerRotationCommitFeasibility(t *testing.T) {
	f, session, outcome := runBorrowedReceiptAuthEntryPositiveChain(t)
	// Caller-derived root: every driver step below derives its own short
	// deadline from this test context and cleanup uses independent bounded
	// contexts.
	ctx := t.Context()

	// Deliberate probe rejection (probe=1/acceptance=0), non-clean guard,
	// genuine opaque token and completed clean owner receipt.
	if atomic.LoadInt32(&f.probeCalls) != 1 || atomic.LoadInt32(&f.acceptanceCalls) != 0 {
		t.Fatalf("deliberate probe rejection counts: probe=%d acceptance=%d", atomic.LoadInt32(&f.probeCalls), atomic.LoadInt32(&f.acceptanceCalls))
	}
	var disposition string
	guardBeforeCtx, cancelGuardBefore := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	err := f.controlPool.QueryRow(guardBeforeCtx, `SELECT disposition FROM recovery_target_guard WHERE target_guard_key=$1`, f.guardKey).Scan(&disposition)
	cancelGuardBefore()
	if err != nil {
		t.Fatalf("guard disposition before rotation: %v", err)
	}
	if disposition == "clean" {
		t.Fatal("deliberate probe rejection left a clean guard")
	}
	receipt := waitBorrowedOwnerReceipt(f.gate, session, 10*time.Second)
	if !receipt.Completed || receipt.Pending || !receipt.Clean {
		t.Fatalf("gate owner receipt is not a completed clean sole-wait success: %+v", receipt)
	}
	tokenFacts := outcome.handoff.Diagnostics()
	if !tokenFacts.Present || tokenFacts.Invalidated {
		t.Fatalf("genuine opaque handoff token absent or invalidated: %+v", tokenFacts)
	}

	// One-time auth entry consumes the authentic token exactly once.
	entry, err := newBorrowedReceiptAuthEntry(f, session, outcome.handoff)
	if err != nil {
		t.Fatalf("receipt auth entry construction: %v", err)
	}
	enterCtx, cancelEnter := context.WithTimeout(ctx, 30*time.Second)
	defer cancelEnter()
	if err := entry.Enter(enterCtx); err != nil {
		t.Fatalf("one-time auth entry: %v", err)
	}
	if !outcome.handoff.Diagnostics().Consumed {
		t.Fatal("the authentic one-use token was not consumed by the entry")
	}

	// Pre-snapshot the ORIGINAL control owner through the actual anchor:
	// backend PID/start, control database OID, cluster incarnation and the
	// exact granted advisory namespace.
	captureCtx, cancelCapture := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	anchor, err := f.run.CaptureControlAnchor(captureCtx)
	cancelCapture()
	if err != nil {
		t.Fatalf("capture original control anchor: %v", err)
	}
	owner := anchor.Diagnostics()
	if !owner.Present || owner.Invalidated {
		t.Fatalf("original control anchor absent or invalidated: %+v", owner)
	}
	binding := f.run.Binding()
	if owner.OriginalTargetKey != binding.OriginalTargetKey() || owner.ControlTargetKey != binding.ControlTargetKey() {
		t.Fatal("control anchor does not retain the original/control keys")
	}

	// Pre-snapshot W catalog/verifier state (the original immutable DSNs and
	// keys stay untouched; no field is rebound to P1).
	preStateCtx, cancelPreState := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	preState, err := borrowedOwnerRotationReadRoleState(preStateCtx, f.fx.admin, f.writerRole)
	cancelPreState()
	if err != nil {
		t.Fatalf("pre-rotation W role state: %v", err)
	}
	if !strings.HasPrefix(preState.verifier, "SCRAM-SHA-256$") {
		t.Fatal("pre-rotation W rolpassword is not a SCRAM verifier")
	}

	// Fresh private P1 and its exact client-generated SCRAM verifier. Neither
	// value is ever logged or stored in the fixture.
	passwordP1 := borrowedAuthCredential(t, "owner rotation P1")
	verifierP1, err := generateSCRAMVerifier(passwordP1, 4096)
	if err != nil {
		t.Fatalf("generate P1 SCRAM verifier: %v", err)
	}

	// Established pre-rotation W session: a real P0 SCRAM login held open
	// across the rotation, proving a committed rotation does not revoke loaded
	// or established authentication.
	establishedConnectCtx, cancelEstablishedConnect := context.WithTimeout(ctx, borrowedOwnerRotationAuthBudget)
	establishedConn, err := pgx.Connect(establishedConnectCtx, f.writerTargetDSN)
	cancelEstablishedConnect()
	if err != nil {
		t.Fatalf("established pre-rotation P0 session refused (sqlstate=%s)", borrowedAuthSQLState(err))
	}
	establishedClosed := false
	t.Cleanup(func() {
		if !establishedClosed {
			cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), borrowedOwnerRotationCleanupBudget)
			_ = establishedConn.Close(cleanupCtx)
			cancelCleanup()
		}
	})
	var establishedOne int
	establishedQueryCtx, cancelEstablishedQuery := context.WithTimeout(ctx, borrowedOwnerRotationAuthBudget)
	establishedErr := establishedConn.QueryRow(establishedQueryCtx, `SELECT 1`).Scan(&establishedOne)
	cancelEstablishedQuery()
	if establishedErr != nil || establishedOne != 1 {
		t.Fatalf("established pre-rotation P0 session query: one=%d err=%v", establishedOne, establishedErr)
	}

	// A second separately owned mutator connects to a DIFFERENT database (the
	// W-owned target) so cross-database lock coordination is proven by the
	// actual experiment, not inferred from documentation.
	routeCtx, cancelRoute := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	targetAdminDSN := borrowedAuthRoleDSN(t, borrowedIdentityRoute(t, routeCtx, f.fx, f.targetDB), f.adminRole, f.adminPassword)
	cancelRoute()

	// Deterministic barrier: the separately owned bounded catalog writers run
	// only while the owner actually holds the SHARE fence. Each spawn registers
	// its own cancel-and-bounded-join cleanup immediately, and the single
	// terminal result is published only after a bounded rollback/close retained
	// every cleanup error: an unknown or missing completion FAILS the test
	// instead of relying on fixture disposal.
	fenceHeld := make(chan struct{})
	var callbackReached int32
	var rotationOwnerPID int
	var controlMutatorCode, targetMutatorCode string
	mutatorCtx, cancelMutator := context.WithTimeout(ctx, 60*time.Second)
	runMutator := func(name, dsn string) <-chan borrowedOwnerRotationMutatorResult {
		done := make(chan borrowedOwnerRotationMutatorResult, 1)
		joined := make(chan struct{})
		// finish publishes the single terminal result only after the bounded
		// rollback/close completed, retaining every cleanup error.
		finish := func(tx pgx.Tx, conn *pgx.Conn, alterErr, stageErr error) {
			cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), borrowedOwnerRotationCleanupBudget)
			rollbackErr := tx.Rollback(cleanupCtx)
			closeErr := conn.Close(cleanupCtx)
			cancelCleanup()
			done <- borrowedOwnerRotationMutatorResult{
				refusalCode: borrowedAuthSQLState(alterErr),
				failure:     errors.Join(stageErr, rollbackErr, closeErr),
			}
		}
		go func() {
			defer close(joined)
			select {
			case <-fenceHeld:
			case <-mutatorCtx.Done():
				done <- borrowedOwnerRotationMutatorResult{refusalCode: "no-sqlstate", failure: mutatorCtx.Err()}
				return
			}
			mutatorConnectCtx, cancelMutatorConnect := context.WithTimeout(mutatorCtx, borrowedOwnerRotationAuthBudget)
			conn, err := pgx.Connect(mutatorConnectCtx, dsn)
			cancelMutatorConnect()
			if err != nil {
				done <- borrowedOwnerRotationMutatorResult{refusalCode: "no-sqlstate", failure: errors.New("mutator connect refused")}
				return
			}
			mutatorBeginCtx, cancelMutatorBegin := context.WithTimeout(mutatorCtx, borrowedOwnerRotationAuthBudget)
			tx, err := conn.Begin(mutatorBeginCtx)
			cancelMutatorBegin()
			if err != nil {
				cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), borrowedOwnerRotationCleanupBudget)
				closeErr := conn.Close(cleanupCtx)
				cancelCleanup()
				done <- borrowedOwnerRotationMutatorResult{refusalCode: "no-sqlstate", failure: errors.Join(errors.New("mutator begin refused"), closeErr)}
				return
			}
			mutatorLockTimeoutCtx, cancelMutatorLockTimeout := context.WithTimeout(mutatorCtx, borrowedOwnerRotationQueryBudget)
			_, mutatorLockTimeoutErr := tx.Exec(mutatorLockTimeoutCtx, `SET LOCAL lock_timeout = '2000ms'`)
			cancelMutatorLockTimeout()
			if mutatorLockTimeoutErr != nil {
				finish(tx, conn, nil, errors.New("mutator lock_timeout refused"))
				return
			}
			// Same W role and the SAME current verifier: this other-connection
			// catalog write must be excluded by the SHARE fence with 55P03.
			mutatorAlterCtx, cancelMutatorAlter := context.WithTimeout(mutatorCtx, borrowedOwnerRotationQueryBudget)
			_, alterErr := tx.Exec(mutatorAlterCtx, `ALTER ROLE `+pgx.Identifier{f.writerRole}.Sanitize()+` PASSWORD `+sqlLiteral(preState.verifier))
			cancelMutatorAlter()
			finish(tx, conn, alterErr, nil)
		}()
		t.Cleanup(func() {
			cancelMutator()
			select {
			case <-joined:
			case <-time.After(borrowedOwnerRotationJoinBudget):
				t.Errorf("%s catalog mutator completion is unknown after the bounded join budget", name)
			}
		})
		return done
	}
	controlMutator := runMutator("control-db", f.adminDSN)
	targetMutator := runMutator("target-db", targetAdminDSN)

	rotateCtx, cancelRotate := context.WithTimeout(ctx, 120*time.Second)
	defer cancelRotate()
	rotationErr := f.lock.WithTransaction(rotateCtx, func(callbackCtx context.Context, tx pgx.Tx) error {
		atomic.AddInt32(&callbackReached, 1)
		statementTimeoutCtx, cancelStatementTimeout := context.WithTimeout(callbackCtx, borrowedOwnerRotationQueryBudget)
		_, statementTimeoutErr := tx.Exec(statementTimeoutCtx, `SET LOCAL statement_timeout = '20000ms'`)
		cancelStatementTimeout()
		if statementTimeoutErr != nil {
			return errors.New("set statement timeout refused")
		}
		lockTimeoutCtx, cancelLockTimeout := context.WithTimeout(callbackCtx, borrowedOwnerRotationQueryBudget)
		_, lockTimeoutErr := tx.Exec(lockTimeoutCtx, `SET LOCAL lock_timeout = '5000ms'`)
		cancelLockTimeout()
		if lockTimeoutErr != nil {
			return errors.New("set lock timeout refused")
		}
		// Before mutation through the SUPPLIED tx: the exact original owner
		// identity, control database OID, cluster incarnation and granted
		// advisory namespace must be unchanged.
		var (
			ownerPID        int
			ownerStart      time.Time
			controlDBOID    uint32
			postmasterStart time.Time
			systemID        string
		)
		ownerIdentityCtx, cancelOwnerIdentity := context.WithTimeout(callbackCtx, borrowedOwnerRotationQueryBudget)
		ownerIdentityErr := tx.QueryRow(ownerIdentityCtx, `
SELECT pid::int, backend_start, (SELECT oid FROM pg_database WHERE datname = current_database()), pg_postmaster_start_time()
FROM pg_stat_activity WHERE pid = pg_backend_pid()`).
			Scan(&ownerPID, &ownerStart, &controlDBOID, &postmasterStart)
		cancelOwnerIdentity()
		if ownerIdentityErr != nil {
			return errors.New("owner identity read through supplied tx refused")
		}
		systemIdentityCtx, cancelSystemIdentity := context.WithTimeout(callbackCtx, borrowedOwnerRotationQueryBudget)
		systemIdentityErr := tx.QueryRow(systemIdentityCtx, `SELECT system_identifier::text FROM pg_control_system()`).Scan(&systemID)
		cancelSystemIdentity()
		if systemIdentityErr != nil {
			return errors.New("cluster identity read through supplied tx refused")
		}
		if ownerPID != owner.BackendPID || !ownerStart.Equal(owner.BackendStart) || controlDBOID != owner.ControlDatabaseOID ||
			!postmasterStart.Equal(owner.PostmasterStart) || systemID != owner.SystemIdentifier {
			return errors.New("original control owner identity changed before mutation")
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
			return errors.New("advisory namespace read through supplied tx refused")
		}
		if !namespaceHeld {
			return errors.New("original granted advisory namespace is not held by the owner")
		}
		rotationOwnerPID = ownerPID
		// Same-transaction catalog SHARE fence over the auth catalogs.
		fenceCtx, cancelFence := context.WithTimeout(callbackCtx, borrowedOwnerRotationQueryBudget)
		_, fenceErr := tx.Exec(fenceCtx, `LOCK TABLE pg_catalog.pg_authid, pg_catalog.pg_auth_members IN SHARE MODE NOWAIT`)
		cancelFence()
		if fenceErr != nil {
			return &borrowedOwnerRotationSQLStateError{stage: "catalog SHARE fence refused", code: borrowedAuthSQLState(fenceErr), cause: fenceErr}
		}
		close(fenceHeld)
		for index, mutator := range []<-chan borrowedOwnerRotationMutatorResult{controlMutator, targetMutator} {
			select {
			case result := <-mutator:
				if result.failure != nil {
					return fmt.Errorf("concurrent other-connection catalog writer %d terminal result was not a clean bounded refusal (sqlstate=%s): %w", index, result.refusalCode, result.failure)
				}
				if result.refusalCode != "55P03" {
					return fmt.Errorf("concurrent other-connection catalog writer %d was not refused with 55P03 (sqlstate=%s)", index, result.refusalCode)
				}
				if index == 0 {
					controlMutatorCode = result.refusalCode
				} else {
					targetMutatorCode = result.refusalCode
				}
			case <-callbackCtx.Done():
				return fmt.Errorf("concurrent catalog writer %d was not resolved before the rotation context ended: %w", index, callbackCtx.Err())
			case <-time.After(30 * time.Second):
				return fmt.Errorf("concurrent catalog writer %d did not return while the SHARE fence was held", index)
			}
		}
		// Real P0 SCRAM authentication is a catalog READ: the SHARE fence must
		// not block it BEFORE COMMIT.
		preCommitCtx, cancelPreCommit := context.WithTimeout(callbackCtx, 10*time.Second)
		p0Conn, err := pgx.Connect(preCommitCtx, f.writerTargetDSN)
		cancelPreCommit()
		if err != nil {
			return &borrowedOwnerRotationSQLStateError{stage: "pre-commit P0 authentication was not accepted", code: borrowedAuthSQLState(err), cause: err}
		}
		p0CloseCtx, cancelP0Close := context.WithTimeout(context.Background(), borrowedOwnerRotationCleanupBudget)
		p0CloseErr := p0Conn.Close(p0CloseCtx)
		cancelP0Close()
		if p0CloseErr != nil {
			return errors.New("close pre-commit P0 client refused")
		}
		// The SAME supplied tx rotates W to the exact client-generated P1
		// verifier; self-held SHARE and the ALTER's ROW EXCLUSIVE never
		// conflict within one backend.
		p1AlterCtx, cancelP1Alter := context.WithTimeout(callbackCtx, borrowedOwnerRotationQueryBudget)
		_, p1AlterErr := tx.Exec(p1AlterCtx, `ALTER ROLE `+pgx.Identifier{f.writerRole}.Sanitize()+` PASSWORD `+sqlLiteral(verifierP1))
		cancelP1Alter()
		if p1AlterErr != nil {
			return &borrowedOwnerRotationSQLStateError{stage: "same-tx ALTER ROLE P1 refused", code: borrowedAuthSQLState(p1AlterErr), cause: p1AlterErr}
		}
		var storedVerifier string
		storedVerifierCtx, cancelStoredVerifier := context.WithTimeout(callbackCtx, borrowedOwnerRotationQueryBudget)
		storedVerifierErr := tx.QueryRow(storedVerifierCtx, `SELECT coalesce(rolpassword,'') FROM pg_authid WHERE rolname=$1`, f.writerRole).Scan(&storedVerifier)
		cancelStoredVerifier()
		if storedVerifierErr != nil {
			return errors.New("stored verifier read through supplied tx refused")
		}
		if subtle.ConstantTimeCompare([]byte(storedVerifier), []byte(verifierP1)) != 1 {
			return errors.New("stored verifier is not the exact generated P1 verifier")
		}
		inTxCtx, cancelInTx := context.WithTimeout(callbackCtx, borrowedOwnerRotationQueryBudget)
		inTx, err := borrowedOwnerRotationReadRoleState(inTxCtx, tx, f.writerRole)
		cancelInTx()
		if err != nil {
			return errors.New("in-transaction W role state read refused")
		}
		if inTx.oid != preState.oid {
			return errors.New("W role OID changed inside the rotation transaction")
		}
		if inTx.login != preState.login || inTx.super != preState.super || inTx.createdb != preState.createdb ||
			inTx.createrole != preState.createrole || inTx.inherit != preState.inherit ||
			inTx.replication != preState.replication || inTx.bypass != preState.bypass {
			return errors.New("W role flags changed inside the rotation transaction")
		}
		if len(inTx.memberships) != len(preState.memberships) {
			return errors.New("W membership closure changed inside the rotation transaction")
		}
		for i := range inTx.memberships {
			if inTx.memberships[i] != preState.memberships[i] {
				return errors.New("W membership closure changed inside the rotation transaction")
			}
		}
		return nil
	})
	if rotationErr != nil {
		t.Fatalf("same-owner rotation transaction (commit outcome is UNKNOWN, never an accepted claim): %v", rotationErr)
	}
	if atomic.LoadInt32(&callbackReached) != 1 {
		t.Fatalf("rotation callback reached %d times, want exactly 1", atomic.LoadInt32(&callbackReached))
	}
	t.Logf("rotation tx owner backend pid=%d (matches the captured original anchor)", rotationOwnerPID)
	t.Logf("catalog SHARE exclusion sqlstates: control-db mutator=%s; target-db mutator=%s", controlMutatorCode, targetMutatorCode)

	// After the actual COMMIT: exact stored P1 verifier, unchanged catalog
	// facts, real P0 refusal 28P01 and real P1 success.
	postStateCtx, cancelPostState := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	postState, err := borrowedOwnerRotationReadRoleState(postStateCtx, f.fx.admin, f.writerRole)
	cancelPostState()
	if err != nil {
		t.Fatalf("post-rotation W role state: %v", err)
	}
	if subtle.ConstantTimeCompare([]byte(postState.verifier), []byte(verifierP1)) != 1 {
		t.Fatalf("committed verifier is not the exact generated P1 verifier (observed_length=%d generated_length=%d)", len(postState.verifier), len(verifierP1))
	}
	if postState.verifier == preState.verifier {
		t.Fatal("committed verifier equals the original P0 verifier")
	}
	borrowedOwnerRotationAssertRoleStateUnchanged(t, "after committed rotation", preState, postState)

	p0AfterCtx, cancelP0After := context.WithTimeout(ctx, borrowedOwnerRotationAuthBudget)
	p0AfterConn, err := pgx.Connect(p0AfterCtx, f.writerTargetDSN)
	cancelP0After()
	if err == nil {
		p0AfterCloseCtx, cancelP0AfterClose := context.WithTimeout(context.Background(), borrowedOwnerRotationCleanupBudget)
		_ = p0AfterConn.Close(p0AfterCloseCtx)
		cancelP0AfterClose()
		t.Fatal("pre-rotation W credential authenticated after the committed rotation")
	}
	p0AfterCode := borrowedAuthSQLState(err)
	if p0AfterCode != "28P01" {
		t.Fatalf("post-commit P0 rejection SQLSTATE=%s, want 28P01", p0AfterCode)
	}
	t.Logf("post-commit auth: P0 sqlstate=%s", p0AfterCode)

	p1DSN := borrowedAuthRoleDSN(t, f.writerTargetDSN, f.writerRole, passwordP1)
	p1Ctx, cancelP1 := context.WithTimeout(ctx, borrowedOwnerRotationAuthBudget)
	p1Conn, err := pgx.Connect(p1Ctx, p1DSN)
	cancelP1()
	if err != nil {
		t.Fatalf("post-commit P1 authentication was refused (sqlstate=%s)", borrowedAuthSQLState(err))
	}
	var one int
	p1QueryCtx, cancelP1Query := context.WithTimeout(ctx, borrowedOwnerRotationAuthBudget)
	p1Err := p1Conn.QueryRow(p1QueryCtx, `SELECT 1`).Scan(&one)
	cancelP1Query()
	if p1Err != nil || one != 1 {
		p1CloseCtx, cancelP1Close := context.WithTimeout(context.Background(), borrowedOwnerRotationCleanupBudget)
		_ = p1Conn.Close(p1CloseCtx)
		cancelP1Close()
		t.Fatalf("post-commit P1 client query: one=%d err=%v", one, p1Err)
	}
	p1CloseCtx, cancelP1Close := context.WithTimeout(context.Background(), borrowedOwnerRotationCleanupBudget)
	p1CloseErr := p1Conn.Close(p1CloseCtx)
	cancelP1Close()
	if p1CloseErr != nil {
		t.Fatalf("close post-commit P1 client: %v", p1CloseErr)
	}
	t.Logf("post-commit auth: P1 accepted")

	// The established pre-rotation P0 session survives the committed rotation:
	// a rotation does not revoke loaded or established authentication.
	establishedAfterCtx, cancelEstablishedAfter := context.WithTimeout(ctx, borrowedOwnerRotationAuthBudget)
	establishedAfterErr := establishedConn.QueryRow(establishedAfterCtx, `SELECT 1`).Scan(&establishedOne)
	cancelEstablishedAfter()
	if establishedAfterErr != nil || establishedOne != 1 {
		t.Fatalf("established pre-rotation P0 session was revoked by the rotation: one=%d err=%v", establishedOne, establishedAfterErr)
	}
	establishedCloseCtx, cancelEstablishedClose := context.WithTimeout(context.Background(), borrowedOwnerRotationCleanupBudget)
	establishedCloseErr := establishedConn.Close(establishedCloseCtx)
	cancelEstablishedClose()
	if establishedCloseErr != nil {
		t.Fatalf("close established pre-rotation P0 session: %v", establishedCloseErr)
	}
	establishedClosed = true
	t.Logf("established pre-rotation P0 session survived the committed rotation")

	// Explicit release proof: a bounded OTHER connection acquires ROW
	// EXCLUSIVE NOWAIT on the auth catalog after COMMIT and rolls back without
	// changing W's privilege state.
	releaseConnCtx, cancelReleaseConn := context.WithTimeout(ctx, borrowedOwnerRotationAuthBudget)
	releaseConn, err := pgx.Connect(releaseConnCtx, f.adminDSN)
	cancelReleaseConn()
	if err != nil {
		t.Fatalf("release-probe connect: %v", err)
	}
	releaseBeginCtx, cancelReleaseBegin := context.WithTimeout(ctx, borrowedOwnerRotationAuthBudget)
	releaseTx, err := releaseConn.Begin(releaseBeginCtx)
	cancelReleaseBegin()
	if err != nil {
		releaseCloseCtx, cancelReleaseClose := context.WithTimeout(context.Background(), borrowedOwnerRotationCleanupBudget)
		_ = releaseConn.Close(releaseCloseCtx)
		cancelReleaseClose()
		t.Fatalf("release-probe begin: %v", err)
	}
	releaseLockCtx, cancelReleaseLock := context.WithTimeout(ctx, borrowedOwnerRotationAuthBudget)
	_, releaseLockErr := releaseTx.Exec(releaseLockCtx, `LOCK TABLE pg_catalog.pg_authid IN ROW EXCLUSIVE MODE NOWAIT`)
	cancelReleaseLock()
	if releaseLockErr != nil {
		releaseRollbackCtx, cancelReleaseRollback := context.WithTimeout(context.Background(), borrowedOwnerRotationCleanupBudget)
		_ = releaseTx.Rollback(releaseRollbackCtx)
		cancelReleaseRollback()
		releaseCloseCtx, cancelReleaseClose := context.WithTimeout(context.Background(), borrowedOwnerRotationCleanupBudget)
		_ = releaseConn.Close(releaseCloseCtx)
		cancelReleaseClose()
		t.Fatalf("catalog SHARE was not released after COMMIT (sqlstate=%s)", borrowedAuthSQLState(releaseLockErr))
	}
	releaseRollbackCtx, cancelReleaseRollback := context.WithTimeout(context.Background(), borrowedOwnerRotationCleanupBudget)
	releaseRollbackErr := releaseTx.Rollback(releaseRollbackCtx)
	cancelReleaseRollback()
	if releaseRollbackErr != nil {
		releaseCloseCtx, cancelReleaseClose := context.WithTimeout(context.Background(), borrowedOwnerRotationCleanupBudget)
		_ = releaseConn.Close(releaseCloseCtx)
		cancelReleaseClose()
		t.Fatalf("release-probe rollback: %v", releaseRollbackErr)
	}
	releaseCloseCtx, cancelReleaseClose := context.WithTimeout(context.Background(), borrowedOwnerRotationCleanupBudget)
	releaseCloseErr := releaseConn.Close(releaseCloseCtx)
	cancelReleaseClose()
	if releaseCloseErr != nil {
		t.Fatalf("release-probe close: %v", releaseCloseErr)
	}

	// Original owner and the consumed checkpoint remain usable after the
	// rotation returns.
	recheckCtx, cancelRecheck := context.WithTimeout(ctx, 60*time.Second)
	defer cancelRecheck()
	if err := anchor.Recheck(recheckCtx); err != nil {
		t.Fatalf("original control anchor recheck after rotation: %v", err)
	}
	if err := entry.Recheck(recheckCtx); err != nil {
		t.Fatalf("post-rotation entry recheck: %v", err)
	}
	healthCtx, cancelHealth := context.WithTimeout(ctx, borrowedOwnerRotationHealthBudget)
	healthErr := f.lock.Health(healthCtx)
	cancelHealth()
	if healthErr != nil {
		t.Fatalf("original control owner health after rotation: %v", healthErr)
	}
	finalGuardCtx, cancelFinalGuard := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	guardErr := f.controlPool.QueryRow(finalGuardCtx, `SELECT disposition FROM recovery_target_guard WHERE target_guard_key=$1`, f.guardKey).Scan(&disposition)
	cancelFinalGuard()
	if guardErr != nil {
		t.Fatalf("guard disposition after rotation: %v", guardErr)
	}
	if disposition == "clean" {
		t.Fatal("rotation changed the deliberate non-clean guard")
	}
	finalStateCtx, cancelFinalState := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	finalState, err := borrowedOwnerRotationReadRoleState(finalStateCtx, f.fx.admin, f.writerRole)
	cancelFinalState()
	if err != nil {
		t.Fatalf("final W role state: %v", err)
	}
	borrowedOwnerRotationAssertRoleStateUnchanged(t, "final", preState, finalState)
	if subtle.ConstantTimeCompare([]byte(finalState.verifier), []byte(verifierP1)) != 1 {
		t.Fatal("final committed verifier is not the exact generated P1 verifier")
	}
}

// TestBorrowedOwnerRotationShareRefusal proves the meaningful actual refusal:
// a separately owned bounded connection holding ROW EXCLUSIVE on
// pg_catalog.pg_authid makes the owner's same-transaction catalog SHARE
// acquisition (NOWAIT) fail with 55P03 BEFORE any P1 mutation. The callback
// failure is distinguishable from an ambiguous COMMIT, and the original W
// catalog facts and owner health are retained.
func TestBorrowedOwnerRotationShareRefusal(t *testing.T) {
	f := newBorrowedAuthHandoffFixture(t)
	// Caller-derived root with the same bounded-step and independent bounded
	// cleanup discipline as the positive test.
	ctx := t.Context()

	preStateCtx, cancelPreState := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	preState, err := borrowedOwnerRotationReadRoleState(preStateCtx, f.fx.admin, f.writerRole)
	cancelPreState()
	if err != nil {
		t.Fatalf("pre-refusal W role state: %v", err)
	}
	if !strings.HasPrefix(preState.verifier, "SCRAM-SHA-256$") {
		t.Fatal("pre-refusal W rolpassword is not a SCRAM verifier")
	}

	// Separately owned bounded catalog ROW EXCLUSIVE holder: never the lock's
	// session and never the transaction owner.
	holderConnCtx, cancelHolderConn := context.WithTimeout(ctx, borrowedOwnerRotationAuthBudget)
	holderConn, err := pgx.Connect(holderConnCtx, f.adminDSN)
	cancelHolderConn()
	if err != nil {
		t.Fatalf("holder connect: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), borrowedOwnerRotationCleanupBudget)
		_ = holderConn.Close(cleanupCtx)
		cancelCleanup()
	})
	holderBeginCtx, cancelHolderBegin := context.WithTimeout(ctx, borrowedOwnerRotationAuthBudget)
	holderTx, err := holderConn.Begin(holderBeginCtx)
	cancelHolderBegin()
	if err != nil {
		t.Fatalf("holder begin: %v", err)
	}
	holderReleased := false
	t.Cleanup(func() {
		if !holderReleased {
			cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), borrowedOwnerRotationCleanupBudget)
			_ = holderTx.Rollback(cleanupCtx)
			cancelCleanup()
		}
	})
	holderLockCtx, cancelHolderLock := context.WithTimeout(ctx, borrowedOwnerRotationAuthBudget)
	_, holderLockErr := holderTx.Exec(holderLockCtx, `LOCK TABLE pg_catalog.pg_authid IN ROW EXCLUSIVE MODE NOWAIT`)
	cancelHolderLock()
	if holderLockErr != nil {
		t.Fatalf("holder ROW EXCLUSIVE acquisition refused (sqlstate=%s)", borrowedAuthSQLState(holderLockErr))
	}

	var callbackReached int32
	refusalCtx, cancelRefusal := context.WithTimeout(ctx, 90*time.Second)
	defer cancelRefusal()
	refusalErr := f.lock.WithTransaction(refusalCtx, func(callbackCtx context.Context, tx pgx.Tx) error {
		atomic.AddInt32(&callbackReached, 1)
		refusalLockTimeoutCtx, cancelRefusalLockTimeout := context.WithTimeout(callbackCtx, borrowedOwnerRotationQueryBudget)
		_, refusalLockTimeoutErr := tx.Exec(refusalLockTimeoutCtx, `SET LOCAL lock_timeout = '2000ms'`)
		cancelRefusalLockTimeout()
		if refusalLockTimeoutErr != nil {
			return errors.New("set lock timeout refused")
		}
		refusalFenceCtx, cancelRefusalFence := context.WithTimeout(callbackCtx, borrowedOwnerRotationQueryBudget)
		_, refusalFenceErr := tx.Exec(refusalFenceCtx, `LOCK TABLE pg_catalog.pg_authid, pg_catalog.pg_auth_members IN SHARE MODE NOWAIT`)
		cancelRefusalFence()
		if refusalFenceErr != nil {
			return &borrowedOwnerRotationSQLStateError{stage: "catalog SHARE acquisition refused", code: borrowedAuthSQLState(refusalFenceErr), cause: refusalFenceErr}
		}
		return nil
	})
	if refusalErr == nil {
		t.Fatal("owner catalog SHARE acquisition succeeded while another connection held ROW EXCLUSIVE")
	}
	if atomic.LoadInt32(&callbackReached) != 1 {
		t.Fatalf("refusal callback reached %d times, want exactly 1", atomic.LoadInt32(&callbackReached))
	}
	refusalCode := borrowedAuthSQLState(refusalErr)
	if refusalCode != "55P03" {
		t.Fatalf("catalog SHARE refusal SQLSTATE=%s, want 55P03", refusalCode)
	}
	t.Logf("owner catalog SHARE refusal sqlstate=%s (callback failure, no COMMIT attempted)", refusalCode)
	if !strings.Contains(refusalErr.Error(), "target-lock acceptance callback") {
		t.Fatal("refusal was not reported as the callback failure")
	}
	if strings.Contains(refusalErr.Error(), "commit target-lock acceptance transaction") {
		t.Fatal("refusal reported an attempted COMMIT instead of a callback failure")
	}

	// No mutation happened: the exact verifier and all role facts are
	// unchanged.
	postStateCtx, cancelPostState := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	postState, err := borrowedOwnerRotationReadRoleState(postStateCtx, f.fx.admin, f.writerRole)
	cancelPostState()
	if err != nil {
		t.Fatalf("post-refusal W role state: %v", err)
	}
	if postState.verifier != preState.verifier {
		t.Fatal("refused SHARE acquisition still changed the W verifier")
	}
	borrowedOwnerRotationAssertRoleStateUnchanged(t, "after refused SHARE acquisition", preState, postState)

	holderRollbackCtx, cancelHolderRollback := context.WithTimeout(context.Background(), borrowedOwnerRotationCleanupBudget)
	holderRollbackErr := holderTx.Rollback(holderRollbackCtx)
	cancelHolderRollback()
	if holderRollbackErr != nil {
		t.Fatalf("holder rollback: %v", holderRollbackErr)
	}
	holderReleased = true
	healthCtx, cancelHealth := context.WithTimeout(ctx, borrowedOwnerRotationHealthBudget)
	healthErr := f.lock.Health(healthCtx)
	cancelHealth()
	if healthErr != nil {
		t.Fatalf("original control owner health after refusal: %v", healthErr)
	}
	holderCloseCtx, cancelHolderClose := context.WithTimeout(context.Background(), borrowedOwnerRotationCleanupBudget)
	holderCloseErr := holderConn.Close(holderCloseCtx)
	cancelHolderClose()
	if holderCloseErr != nil {
		t.Fatalf("holder close: %v", holderCloseErr)
	}
}

// ---------------------------------------------------------------------------
// Bounded same-fixture freeze/drain feasibility lane
// ---------------------------------------------------------------------------

// borrowedAuthDrainPositiveChain establishes the genuine positive fixture for
// the freeze/drain lane: the real held run with the deliberate Probe rejection
// and no acceptance, the held first-frame target-absence proof, the authentic
// opaque handoff token and the actual clean gate retirement. It grants no
// authority; every step is bounded and the fixture/gate cleanups stay owned by
// the fixture.
func borrowedAuthDrainPositiveChain(t *testing.T) (*borrowedAuthHandoffFixture, *originGateSession, borrowedReceiptHandoffOutcome) {
	t.Helper()
	f := newBorrowedAuthHandoffFixture(t)
	installBorrowedReceiptAuthEntryProbe(t, f)
	ctx := t.Context()

	f.gate.HoldRegistration()
	t.Cleanup(f.gate.ReleaseRegistration)
	resultCh := make(chan borrowedReceiptHandoffOutcome, 1)
	runCtx, cancelRun := context.WithCancel(ctx)
	t.Cleanup(cancelRun)
	go func() {
		result, handoff, err := f.run.RunForReceiptHandoff(runCtx)
		resultCh <- borrowedReceiptHandoffOutcome{result, handoff, err}
	}()
	admitCtx, cancelAdmit := context.WithTimeout(ctx, 90*time.Second)
	t.Cleanup(cancelAdmit)
	session, err := f.gate.AdmitBorrowed(admitCtx, f.run, f.prefix)
	if err != nil {
		cancelRun()
		t.Fatalf("actual borrowed admission: %v", err)
	}
	heldDeadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(heldDeadline) && !(session.BackendPID() > 0 && session.BufferedFrames() > 0) {
		time.Sleep(20 * time.Millisecond)
	}
	if !(session.BackendPID() > 0 && session.BufferedFrames() > 0) {
		cancelRun()
		t.Fatal("actual native child never reached the held first executable frame")
	}
	// Held first frame: probe/acceptance must not have run and the W target
	// relation must still be absent (the deliberate rejection happens after the
	// restore, so this is the only target-absence window).
	if atomic.LoadInt32(&f.probeCalls) != 0 || atomic.LoadInt32(&f.acceptanceCalls) != 0 {
		cancelRun()
		t.Fatalf("held first frame already ran probe/acceptance: probe=%d acceptance=%d", atomic.LoadInt32(&f.probeCalls), atomic.LoadInt32(&f.acceptanceCalls))
	}
	targetCtx, cancelTarget := context.WithTimeout(ctx, borrowedOwnerRotationAuthBudget)
	targetConn, err := pgx.Connect(targetCtx, f.writerTargetDSN)
	cancelTarget()
	if err != nil {
		cancelRun()
		t.Fatalf("connect W target during hold (sqlstate=%s, W role OID %d)", borrowedAuthSQLState(err), f.writerRoleOID)
	}
	targetQueryCtx, cancelTargetQuery := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	var absent bool
	targetErr := targetConn.QueryRow(targetQueryCtx, `SELECT to_regclass($1) IS NULL`, "public."+f.table).Scan(&absent)
	cancelTargetQuery()
	targetCloseCtx, cancelTargetClose := context.WithTimeout(context.Background(), borrowedOwnerRotationCleanupBudget)
	_ = targetConn.Close(targetCloseCtx)
	cancelTargetClose()
	if targetErr != nil || !absent {
		cancelRun()
		t.Fatalf("target relation present during hold: absent=%t err=%v", absent, targetErr)
	}
	f.gate.ReleaseRegistration()
	releaseDeadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(releaseDeadline) && !session.WatcherReady() {
		time.Sleep(20 * time.Millisecond)
	}
	if !session.WatcherReady() {
		cancelRun()
		t.Fatal("actual watcher/admission checks never became ready")
	}
	var outcome borrowedReceiptHandoffOutcome
	select {
	case outcome = <-resultCh:
	case <-time.After(150 * time.Second):
		cancelRun()
		t.Fatal("actual receipt-handoff run did not return")
	}
	if outcome.err == nil || !outcome.handoff.Diagnostics().Present {
		cancelRun()
		t.Fatalf("positive chain produced no authentic token: err=%v facts=%+v", outcome.err, outcome.handoff.Diagnostics())
	}
	if atomic.LoadInt32(&f.probeCalls) != 1 || atomic.LoadInt32(&f.acceptanceCalls) != 0 {
		cancelRun()
		t.Fatalf("positive chain probe/acceptance counts: probe=%d acceptance=%d", atomic.LoadInt32(&f.probeCalls), atomic.LoadInt32(&f.acceptanceCalls))
	}
	clean, report := waitBorrowedGateRetirement(f, 30*time.Second)
	if !clean {
		cancelRun()
		t.Fatalf("actual gate did not publish clean retirement: %s", report)
	}
	return f, session, outcome
}

// borrowedAuthDrainHeldCancelNegative is the actual held-cancel no-handoff
// negative. It needs its own fixture because a canceled held run permanently
// latches its gate; it is bounded and grants no authority.
func borrowedAuthDrainHeldCancelNegative(t *testing.T, ctx context.Context) {
	t.Helper()
	f := newBorrowedAuthHandoffFixture(t)

	f.gate.HoldRegistration()
	defer f.gate.ReleaseRegistration()
	resultCh := make(chan borrowedReceiptHandoffOutcome, 1)
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	go func() {
		result, handoff, err := f.run.RunForReceiptHandoff(runCtx)
		resultCh <- borrowedReceiptHandoffOutcome{result, handoff, err}
	}()
	admitCtx, cancelAdmit := context.WithTimeout(ctx, 90*time.Second)
	defer cancelAdmit()
	session, err := f.gate.AdmitBorrowed(admitCtx, f.run, f.prefix)
	if err != nil {
		cancelRun()
		t.Fatalf("held-cancel negative admission: %v", err)
	}
	heldDeadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(heldDeadline) && !(session.BackendPID() > 0 && session.BufferedFrames() > 0) {
		time.Sleep(20 * time.Millisecond)
	}
	if !(session.BackendPID() > 0 && session.BufferedFrames() > 0) {
		cancelRun()
		t.Fatal("held-cancel negative: native child never reached the held first frame")
	}
	cancelRun()
	var outcome borrowedReceiptHandoffOutcome
	select {
	case outcome = <-resultCh:
	case <-time.After(150 * time.Second):
		t.Fatal("held-cancel negative: canceled run did not return")
	}
	if outcome.err == nil {
		t.Fatal("held-cancel negative: cancellation was converted to a nil error")
	}
	if outcome.handoff.Diagnostics().Present {
		t.Fatal("held-cancel negative: cancellation produced a handoff token")
	}
	if outcome.result.Command.Outcome != recovery.PGCommandCanceled || !outcome.result.Command.ProcessGroupDrained {
		t.Fatalf("held-cancel negative: not a genuine drained cancel: %+v", outcome.result.Command)
	}
	entry, entryErr := newBorrowedReceiptAuthEntry(f, session, outcome.handoff)
	if entryErr == nil || entry != nil {
		t.Fatal("held-cancel negative: absent token constructed an auth entry")
	}
	receipt := waitBorrowedOwnerReceipt(f.gate, session, 30*time.Second)
	if !receipt.Completed {
		t.Fatalf("held-cancel negative: gate never observed the canceled child sole wait: %+v", receipt)
	}
	if receipt.Clean {
		t.Fatal("held-cancel negative: canceled held child was reported as a clean success")
	}
	f.gate.ReleaseRegistration()
	latchDeadline := time.Now().Add(20 * time.Second)
	latched := false
	for time.Now().Before(latchDeadline) {
		if clean, _ := f.gate.CleanRetired(); clean {
			t.Fatal("held-cancel negative: canceled held run published clean gate retirement")
		}
		if l, _ := f.gate.Latched(); l {
			latched = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !latched {
		t.Fatal("held-cancel negative: canceled held run did not latch after the held barrier was released")
	}
	t.Logf("held-cancel negative: token absent, owner receipt completed=%t clean=%t, gate latched=%t, entry refused absent token", receipt.Completed, receipt.Clean, latched)
}

// borrowedAuthDrainInstallHelpers installs the staging client and its
// errno-preserving stat probe into the owned fixture container. The helper
// copy runs on an explicit short caller-derived Query child; the auth-boundary
// census helper was already installed by newOriginGateFixture.
func borrowedAuthDrainInstallHelpers(t *testing.T, ctx context.Context, f *borrowedAuthHandoffFixture) {
	t.Helper()
	staging := buildBorrowedAuthStagingHelper(t)
	copyCtx, cancelCopy := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	copyErr := f.fx.container.CopyFileToContainer(copyCtx, staging, borrowedAuthStagingHelperPath, 0o700)
	cancelCopy()
	if copyErr != nil {
		t.Fatalf("copy staging helper into the drain fixture: %v", copyErr)
	}
	installBorrowedAuthStagingStatProbe(t, f.fx)
}

// borrowedAuthDrainCensusSnapshot runs one bounded strict auth-boundary census
// with bounded retries for racy FD scans.
func borrowedAuthDrainCensusSnapshot(t *testing.T, ctx context.Context, f *borrowedAuthHandoffFixture) (map[int]authBoundaryProcess, []authBoundarySocket) {
	t.Helper()
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		attemptCtx, cancel := context.WithTimeout(ctx, borrowedAuthStagingAssociationBudget)
		processes, sockets, err := snapshotPostmasterChildren(attemptCtx, f.fx.containerID, f.fx.postmasterPID, f.fx.postmasterStr)
		cancel()
		if err == nil {
			return processes, sockets
		}
		lastErr = err
		select {
		case <-ctx.Done():
			t.Fatalf("drain census snapshot context ended: %v", lastErr)
		case <-time.After(100 * time.Millisecond):
		}
	}
	t.Fatalf("drain strict census snapshot never completed: %v", lastErr)
	return nil, nil
}

// errBorrowedAuthDrainTokenMismatch is the distinct refusal of the complete
// strict-census comparison branch: the retained full PID/start/inode/tuple
// token does not match exactly once. Only this sentinel is mismatch evidence;
// census exec/parse, container-stat and postmaster failures are different
// errors and can never be counted as a mismatch.
var errBorrowedAuthDrainTokenMismatch = errors.New("exact backend token changed before termination")

// borrowedAuthDrainRevalidateToken is the error-returning form of the exact
// pre-termination backend revalidation predicate: the same strict census child
// PID/start/inode/full-tuple token with no additional match, the same live
// strict container process identity and the old captured postmaster
// incarnation. Every operation is caller-bounded; every failure is a refusal,
// never a token.
func borrowedAuthDrainRevalidateToken(ctx context.Context, f *borrowedAuthHandoffFixture, token borrowedAuthStagingBackendToken) error {
	if token.ChildPID <= 1 || token.ChildStart == 0 || token.Inode == "" || token.Local == "" || token.Remote == "" {
		return errors.New("backend token identity is incomplete")
	}
	expectedStart, err := strconv.ParseUint(f.fx.postmasterStr, 10, 64)
	if err != nil || expectedStart == 0 {
		return errors.New("captured postmaster start identity is invalid")
	}
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		attemptCtx, cancelAttempt := context.WithTimeout(ctx, borrowedAuthStagingAssociationBudget)
		out, execErr := dockerExec(attemptCtx, f.fx.containerID, borrowedAuthStagingHelperPath, "census", strconv.Itoa(f.fx.postmasterPID), f.fx.postmasterStr)
		cancelAttempt()
		if execErr != nil {
			lastErr = errors.New("strict census exec refused")
		} else if report, parseErr := parseBorrowedAuthStagingCensus(out, f.fx.postmasterPID, expectedStart); parseErr != nil {
			lastErr = errors.New("strict census report refused")
		} else {
			matches := borrowedAuthStagingCensusMatches(report, token.Local, token.Remote)
			if len(matches) != 1 || matches[0] != token {
				return errBorrowedAuthDrainTokenMismatch
			}
			statCtx, cancelStat := context.WithTimeout(ctx, borrowedAuthStagingAssociationBudget)
			state, observedStart, statErr := borrowedAuthStagingContainerStat(statCtx, f.fx.containerID, token.ChildPID)
			cancelStat()
			if statErr != nil || observedStart != token.ChildStart || !borrowedAuthStagingLiveState(state) {
				return errors.New("backend PID/start is not the retained live server process")
			}
			pmCtx, cancelPm := context.WithTimeout(ctx, borrowedAuthStagingIncarnationBudget)
			pmPID, pmStart, pmErr := inspectPostmaster(pmCtx, f.fx.containerID)
			cancelPm()
			if pmErr != nil || pmPID != f.fx.postmasterPID || pmStart != f.fx.postmasterStr {
				return errors.New("captured postmaster incarnation changed before termination")
			}
			return nil
		}
		select {
		case <-ctx.Done():
			return lastErr
		case <-time.After(100 * time.Millisecond):
		}
	}
	return lastErr
}

// borrowedAuthDrainTokenMismatchNegative routes tampered tokens through the
// SAME error-returning revalidation predicate used before termination and
// requires an identity-mismatch refusal for PID, start and inode tampering
// individually (never struct inequality against a synthetic projection).
func borrowedAuthDrainTokenMismatchNegative(t *testing.T, ctx context.Context, f *borrowedAuthHandoffFixture, token borrowedAuthStagingBackendToken) {
	t.Helper()
	if err := borrowedAuthDrainRevalidateToken(ctx, f, token); err != nil {
		t.Fatalf("retained token refused by its own revalidation predicate: %v", err)
	}
	tamperedPID := token
	tamperedPID.ChildPID++
	tamperedStart := token
	tamperedStart.ChildStart++
	tamperedInode := token
	tamperedInode.Inode = "0"
	for _, testCase := range []struct {
		name string
		tok  borrowedAuthStagingBackendToken
	}{
		{"PID", tamperedPID},
		{"start", tamperedStart},
		{"inode", tamperedInode},
	} {
		err := borrowedAuthDrainRevalidateToken(ctx, f, testCase.tok)
		if err == nil {
			t.Fatalf("tampered %s token was accepted by the revalidation predicate", testCase.name)
		}
		if !errors.Is(err, errBorrowedAuthDrainTokenMismatch) {
			t.Fatalf("tampered %s token was refused by an unknown census/stat/postmaster failure instead of the specific mismatch sentinel: %v", testCase.name, err)
		}
	}
	t.Logf("token mismatch negative: retained token revalidates; PID, start and inode tampering are each refused with the specific mismatch sentinel by the same predicate")
}

// borrowedAuthDrainFDNonemptyNegative proves the loaded backend's live FD table
// is actually non-empty, so the later empty-FD exit gate is not vacuous.
func borrowedAuthDrainFDNonemptyNegative(t *testing.T, ctx context.Context, f *borrowedAuthHandoffFixture, pid int) {
	t.Helper()
	processes, sockets := borrowedAuthDrainCensusSnapshot(t, ctx, f)
	process, present := processes[pid]
	if !present || !borrowedAuthStagingLiveState(process.State) {
		t.Fatalf("loaded backend %d is not a live postmaster child before the freeze", pid)
	}
	entries, readable, ok := processFDTable(sockets, pid)
	if !ok || entries == 0 || readable == 0 {
		t.Fatalf("live loaded backend %d has an empty/unknown FD table (entries=%d readable=%d census=%t); the empty-FD exit gate would be vacuous", pid, entries, readable, ok)
	}
	if live := countLiveProcessSockets(sockets, pid); live == 0 {
		t.Fatalf("live loaded backend %d has no live socket record", pid)
	}
	t.Logf("FD nonempty negative: live loaded backend %d fd_entries=%d fd_readable=%d live_sockets=%d", pid, entries, readable, countLiveProcessSockets(sockets, pid))
}

// borrowedAuthDrainAwaitZombieDrained waits, bounded, for the exact retained
// backend PID/start to be present as a same-start zombie (state Z) with a fully
// enumerated empty FD table and no live socket record while the postmaster is
// frozen. A same-start zombie is explicitly NOT a reaped/disappearance proof;
// this only proves the frozen exit state before CONT.
func borrowedAuthDrainAwaitZombieDrained(ctx context.Context, t *testing.T, containerID string, pmPID int, pmStr string, pid int, start uint64) (int, int) {
	t.Helper()
	deadline := time.NewTimer(20 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	last := "no census yet"
	for {
		censusCtx, cancelCensus := context.WithTimeout(ctx, borrowedAuthStagingAssociationBudget)
		processes, sockets, err := snapshotPostmasterChildren(censusCtx, containerID, pmPID, pmStr)
		cancelCensus()
		if err == nil {
			process, present := processes[pid]
			switch {
			case !present:
				last = "pid absent before CONT"
			case process.Start != strconv.FormatUint(start, 10):
				last = "start identity changed before CONT"
			case process.State != "Z":
				last = fmt.Sprintf("state=%s", process.State)
			default:
				entries, readable, ok := processFDTable(sockets, pid)
				live := countLiveProcessSockets(sockets, pid)
				if ok && entries == 0 && readable == 0 && live == 0 {
					return entries, readable
				}
				last = fmt.Sprintf("fd_entries=%d fd_readable=%d fd_census=%t live_sockets=%d", entries, readable, ok, live)
			}
		} else {
			last = "census error"
		}
		select {
		case <-ctx.Done():
			t.Fatalf("zombie drain gate context ended: %s", last)
		case <-deadline.C:
			t.Fatalf("loaded backend %d/%d did not reach same-start Z with an empty FD table and no socket while frozen: %s", pid, start, last)
		case <-ticker.C:
		}
	}
}

// borrowedAuthDrainStrictCensusBounded runs the staging strict census with
// bounded retries for racy FD scans while the postmaster is frozen (T is a
// recognized non-dead state for the strict producer/consumer).
func borrowedAuthDrainStrictCensusBounded(t *testing.T, ctx context.Context, fx *originGateFixture) borrowedAuthStagingCensus {
	t.Helper()
	expectedStart := mustParseStart(t, fx.postmasterStr)
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		attemptCtx, cancel := context.WithTimeout(ctx, borrowedAuthStagingAssociationBudget)
		out, err := dockerExec(attemptCtx, fx.containerID, borrowedAuthStagingHelperPath, "census", strconv.Itoa(fx.postmasterPID), fx.postmasterStr)
		cancel()
		if err == nil {
			report, parseErr := parseBorrowedAuthStagingCensus(out, fx.postmasterPID, expectedStart)
			if parseErr == nil {
				if len(report.Children) == 0 {
					lastErr = errors.New("strict census has no direct postmaster children")
				} else {
					return report
				}
			} else {
				lastErr = parseErr
			}
		} else {
			lastErr = errors.New("strict census producer exec refused")
		}
		select {
		case <-ctx.Done():
			t.Fatalf("refrozen strict census context ended: %v", lastErr)
		case <-time.After(100 * time.Millisecond):
		}
	}
	t.Fatalf("refrozen strict census never produced a complete report: %v", lastErr)
	return borrowedAuthStagingCensus{}
}

// borrowedAuthDrainShareRefusalNegative proves the actual frozen SHARE refusal:
// a separately owned transaction established BEFORE the freeze holds ROW
// EXCLUSIVE on pg_catalog.pg_authid, so the original owner's same-tx catalog
// SHARE NOWAIT acquisition fails with 55P03 as a callback failure before any
// mutation or COMMIT.
func borrowedAuthDrainShareRefusalNegative(t *testing.T, ctx context.Context, f *borrowedAuthHandoffFixture) {
	t.Helper()
	var callbackReached int32
	refusalCtx, cancelRefusal := context.WithTimeout(ctx, 90*time.Second)
	defer cancelRefusal()
	refusalErr := f.lock.WithTransaction(refusalCtx, func(callbackCtx context.Context, tx pgx.Tx) error {
		atomic.AddInt32(&callbackReached, 1)
		lockTimeoutCtx, cancelLockTimeout := context.WithTimeout(callbackCtx, borrowedOwnerRotationQueryBudget)
		_, lockTimeoutErr := tx.Exec(lockTimeoutCtx, `SET LOCAL lock_timeout = '2000ms'`)
		cancelLockTimeout()
		if lockTimeoutErr != nil {
			return errors.New("set lock timeout refused")
		}
		fenceCtx, cancelFence := context.WithTimeout(callbackCtx, borrowedOwnerRotationQueryBudget)
		_, fenceErr := tx.Exec(fenceCtx, `LOCK TABLE pg_catalog.pg_authid, pg_catalog.pg_auth_members IN SHARE MODE NOWAIT`)
		cancelFence()
		if fenceErr != nil {
			return &borrowedOwnerRotationSQLStateError{stage: "catalog SHARE acquisition refused", code: borrowedAuthSQLState(fenceErr), cause: fenceErr}
		}
		return nil
	})
	if refusalErr == nil {
		t.Fatal("owner catalog SHARE acquisition succeeded while a separate connection held ROW EXCLUSIVE")
	}
	if atomic.LoadInt32(&callbackReached) != 1 {
		t.Fatalf("frozen SHARE refusal callback reached %d times, want exactly 1", atomic.LoadInt32(&callbackReached))
	}
	code := borrowedAuthSQLState(refusalErr)
	if code != "55P03" {
		t.Fatalf("frozen catalog SHARE refusal SQLSTATE=%s, want 55P03", code)
	}
	if !strings.Contains(refusalErr.Error(), "target-lock acceptance callback") {
		t.Fatal("frozen SHARE refusal was not reported as the callback failure")
	}
	if strings.Contains(refusalErr.Error(), "commit target-lock acceptance transaction") {
		t.Fatal("frozen SHARE refusal reported an attempted COMMIT instead of a callback failure")
	}
	t.Logf("frozen SHARE refusal sqlstate=%s (callback failure, no COMMIT attempted)", code)
}

// borrowedAuthDrainOwnerRotation runs the proven original-owner same-tx catalog
// SHARE NOWAIT + ALTER ROLE W P1 + COMMIT pattern while the postmaster is
// frozen. Every operation uses a short child context of the callback context;
// the callback must be reached exactly once and a COMMIT error is UNKNOWN.
func borrowedAuthDrainOwnerRotation(ctx context.Context, f *borrowedAuthHandoffFixture, owner recovery.DrillControlAnchorFacts, verifierP1 string, preState borrowedOwnerRotationRoleState) (int, int32, error) {
	var callbackReached int32
	ownerPID := 0
	rotateCtx, cancelRotate := context.WithTimeout(ctx, 120*time.Second)
	defer cancelRotate()
	rotationErr := f.lock.WithTransaction(rotateCtx, func(callbackCtx context.Context, tx pgx.Tx) error {
		atomic.AddInt32(&callbackReached, 1)
		statementTimeoutCtx, cancelStatementTimeout := context.WithTimeout(callbackCtx, borrowedOwnerRotationQueryBudget)
		_, statementTimeoutErr := tx.Exec(statementTimeoutCtx, `SET LOCAL statement_timeout = '20000ms'`)
		cancelStatementTimeout()
		if statementTimeoutErr != nil {
			return errors.New("set statement timeout refused")
		}
		lockTimeoutCtx, cancelLockTimeout := context.WithTimeout(callbackCtx, borrowedOwnerRotationQueryBudget)
		_, lockTimeoutErr := tx.Exec(lockTimeoutCtx, `SET LOCAL lock_timeout = '5000ms'`)
		cancelLockTimeout()
		if lockTimeoutErr != nil {
			return errors.New("set lock timeout refused")
		}
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
			return errors.New("owner identity read through supplied tx refused")
		}
		systemCtx, cancelSystem := context.WithTimeout(callbackCtx, borrowedOwnerRotationQueryBudget)
		systemErr := tx.QueryRow(systemCtx, `SELECT system_identifier::text FROM pg_control_system()`).Scan(&systemID)
		cancelSystem()
		if systemErr != nil {
			return errors.New("cluster identity read through supplied tx refused")
		}
		if observedPID != owner.BackendPID || !ownerStart.Equal(owner.BackendStart) || controlDBOID != owner.ControlDatabaseOID ||
			!postmasterStart.Equal(owner.PostmasterStart) || systemID != owner.SystemIdentifier {
			return errors.New("original control owner identity changed before mutation")
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
			return errors.New("advisory namespace read through supplied tx refused")
		}
		if !namespaceHeld {
			return errors.New("original granted advisory namespace is not held by the owner")
		}
		ownerPID = observedPID
		fenceCtx, cancelFence := context.WithTimeout(callbackCtx, borrowedOwnerRotationQueryBudget)
		_, fenceErr := tx.Exec(fenceCtx, `LOCK TABLE pg_catalog.pg_authid, pg_catalog.pg_auth_members IN SHARE MODE NOWAIT`)
		cancelFence()
		if fenceErr != nil {
			return &borrowedOwnerRotationSQLStateError{stage: "catalog SHARE fence refused", code: borrowedAuthSQLState(fenceErr), cause: fenceErr}
		}
		alterCtx, cancelAlter := context.WithTimeout(callbackCtx, borrowedOwnerRotationQueryBudget)
		_, alterErr := tx.Exec(alterCtx, `ALTER ROLE `+pgx.Identifier{f.writerRole}.Sanitize()+` PASSWORD `+sqlLiteral(verifierP1))
		cancelAlter()
		if alterErr != nil {
			return &borrowedOwnerRotationSQLStateError{stage: "same-tx ALTER ROLE P1 refused", code: borrowedAuthSQLState(alterErr), cause: alterErr}
		}
		var storedVerifier string
		storedCtx, cancelStored := context.WithTimeout(callbackCtx, borrowedOwnerRotationQueryBudget)
		storedErr := tx.QueryRow(storedCtx, `SELECT coalesce(rolpassword,'') FROM pg_authid WHERE rolname=$1`, f.writerRole).Scan(&storedVerifier)
		cancelStored()
		if storedErr != nil {
			return errors.New("stored verifier read through supplied tx refused")
		}
		if subtle.ConstantTimeCompare([]byte(storedVerifier), []byte(verifierP1)) != 1 {
			return errors.New("stored verifier is not the exact generated P1 verifier")
		}
		inTxCtx, cancelInTx := context.WithTimeout(callbackCtx, borrowedOwnerRotationQueryBudget)
		inTx, inTxErr := borrowedOwnerRotationReadRoleState(inTxCtx, tx, f.writerRole)
		cancelInTx()
		if inTxErr != nil {
			return errors.New("in-transaction W role state read refused")
		}
		if inTx.oid != preState.oid {
			return errors.New("W role OID changed inside the drain rotation transaction")
		}
		if inTx.login != preState.login || inTx.super != preState.super || inTx.createdb != preState.createdb ||
			inTx.createrole != preState.createrole || inTx.inherit != preState.inherit ||
			inTx.replication != preState.replication || inTx.bypass != preState.bypass {
			return errors.New("W role flags changed inside the drain rotation transaction")
		}
		if len(inTx.memberships) != len(preState.memberships) {
			return errors.New("W membership closure changed inside the drain rotation transaction")
		}
		for i := range inTx.memberships {
			if inTx.memberships[i] != preState.memberships[i] {
				return errors.New("W membership closure changed inside the drain rotation transaction")
			}
		}
		return nil
	})
	return ownerPID, atomic.LoadInt32(&callbackReached), rotationErr
}

// borrowedAuthDrainTimeoutEvidence reports whether a failure carries
// recognized deadline/timeout/transport evidence (context deadline, OS
// deadline or a net.Error timeout). A bare immediate refusal or configuration
// failure has none and can never pass as a timeout.
func borrowedAuthDrainTimeoutEvidence(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

// borrowedAuthDrainAuthTimeoutNegative proves an auth attempt while the exact
// postmaster is frozen actually reaches its bounded deadline and carries
// recognized timeout/transport evidence, and is NEVER misclassified as a
// genuine 28P01 rejection. An immediate refusal or configuration failure fails
// the check instead of being logged as a timeout pass.
func borrowedAuthDrainAuthTimeoutNegative(t *testing.T, ctx context.Context, f *borrowedAuthHandoffFixture) {
	t.Helper()
	timeoutCtx, cancelTimeout := context.WithTimeout(ctx, 3*time.Second)
	conn, err := pgx.Connect(timeoutCtx, f.writerTargetDSN)
	deadlineErr := timeoutCtx.Err()
	cancelTimeout()
	if err == nil {
		closeCtx, cancelClose := context.WithTimeout(context.Background(), borrowedOwnerRotationCleanupBudget)
		_ = conn.Close(closeCtx)
		cancelClose()
		t.Fatal("a fresh P0 connection was accepted while the postmaster was frozen")
	}
	if code := borrowedAuthSQLState(err); code == "28P01" {
		t.Fatal("a frozen-postmaster auth timeout was misclassified as a genuine 28P01 rejection")
	}
	if !errors.Is(deadlineErr, context.DeadlineExceeded) {
		t.Fatalf("frozen auth attempt did not reach its bounded deadline (ctx_err=%v); an immediate refusal/config failure is never a timeout PASS", deadlineErr)
	}
	if !borrowedAuthDrainTimeoutEvidence(err) {
		t.Fatalf("frozen auth failure carries no recognized timeout/transport evidence (type %T)", err)
	}
	t.Logf("frozen auth negative: bounded deadline expired and the failure carries recognized timeout/transport evidence (sqlstate=%s)", borrowedAuthSQLState(err))
}

// borrowedAuthDrainStopFailureCleanupNegative is the actual bounded post-STOP
// failure/cleanup negative: a real same-incarnation STOP is performed and the
// state-confirmation path is forced to fail (a bounded wait for R/S while the
// postmaster is genuinely T). The independently armed cleanup must then CONT
// the exact captured PID/start with a bounded 10s context and restore the same
// live incarnation. The forced failure is never success, and this path is not
// covered by the wrong-start CONT refusal.
func borrowedAuthDrainStopFailureCleanupNegative(t *testing.T, ctx context.Context, f *borrowedAuthHandoffFixture) {
	t.Helper()
	stopped := false
	resume := func() error {
		if !stopped {
			return nil
		}
		resumeCtx, cancelResume := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancelResume()
		if err := signalPostmaster(resumeCtx, f.fx.containerID, f.fx.postmasterPID, f.fx.postmasterStr, "CONT"); err != nil {
			return err
		}
		stopped = false
		return nil
	}
	t.Cleanup(func() {
		if err := resume(); err != nil {
			t.Errorf("post-STOP failure negative guaranteed CONT cleanup failed: %v", err)
		}
	})
	stopCtx, cancelStop := context.WithTimeout(ctx, 10*time.Second)
	defer cancelStop()
	if err := signalPostmaster(stopCtx, f.fx.containerID, f.fx.postmasterPID, f.fx.postmasterStr, "STOP"); err != nil {
		t.Fatalf("post-STOP failure negative SIGSTOP exact postmaster: %v", err)
	}
	stopped = true // armed on STOP-signal success before the state confirmation
	// Forced state-check failure path: the postmaster is genuinely T, so the
	// bounded wait for R/S must return an error and never success.
	if state, err := awaitProcessState(stopCtx, f.fx.containerID, f.fx.postmasterPID, f.fx.postmasterStr, "R", "S"); err == nil {
		t.Fatalf("forced post-STOP state-check failure path unexpectedly succeeded (state=%q)", state)
	}
	// The bounded same-incarnation cleanup must restore the exact incarnation.
	if err := resume(); err != nil {
		t.Fatalf("post-STOP failure cleanup CONT refused: %v", err)
	}
	liveCtx, cancelLive := context.WithTimeout(ctx, 10*time.Second)
	liveErr := awaitNativePGReady(liveCtx, f.adminDSN)
	cancelLive()
	if liveErr != nil {
		t.Fatalf("post-STOP failure cleanup did not restore the same incarnation live: %v", liveErr)
	}
	stateCtx, cancelState := context.WithTimeout(ctx, 10*time.Second)
	state, stateErr := processState(stateCtx, f.fx.containerID, f.fx.postmasterPID, f.fx.postmasterStr)
	cancelState()
	if stateErr != nil || state == "Z" {
		t.Fatalf("postmaster is not the same live incarnation after the failure cleanup: state=%q err=%v", state, stateErr)
	}
	t.Logf("post-STOP failure cleanup negative: forced state-check failure returned an error; bounded same-incarnation CONT restored postmaster PID %d start %s live (state=%s); the failure was never success", f.fx.postmasterPID, f.fx.postmasterStr, state)
}

// TestBorrowedAuthDrainFreezeExit is the bounded same-fixture freeze/drain
// feasibility driver described in the file header. It proves only this bounded
// lane; it grants no DDL/rebuild/acceptance/Gate1 authority and a COMMIT error
// is UNKNOWN, never an accepted claim.
func TestBorrowedAuthDrainFreezeExit(t *testing.T) {
	ctx := t.Context()

	// Actual held-cancel no-handoff negative (own fixture: a canceled held run
	// permanently latches its gate).
	borrowedAuthDrainHeldCancelNegative(t, ctx)

	// Step 1: genuine positive chain (held target-absent, probe=1/acceptance=0,
	// clean retirement) and the one-time entry over the authentic opaque token.
	f, session, outcome := borrowedAuthDrainPositiveChain(t)
	entry, err := newBorrowedReceiptAuthEntry(f, session, outcome.handoff)
	if err != nil {
		t.Fatalf("receipt auth entry construction: %v", err)
	}
	enterCtx, cancelEnter := context.WithTimeout(ctx, borrowedOwnerRotationAuthBudget)
	enterErr := entry.Enter(enterCtx)
	cancelEnter()
	if enterErr != nil {
		t.Fatalf("one-time auth entry: %v", enterErr)
	}
	if !outcome.handoff.Diagnostics().Consumed {
		t.Fatal("the authentic one-use token was not consumed by the entry")
	}
	borrowedAuthDrainInstallHelpers(t, ctx, f)

	preStateCtx, cancelPreState := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	preState, err := borrowedOwnerRotationReadRoleState(preStateCtx, f.fx.admin, f.writerRole)
	cancelPreState()
	if err != nil {
		t.Fatalf("pre-drain W role state: %v", err)
	}
	passwordP1 := borrowedAuthCredential(t, "drain P1")
	verifierP1, err := generateSCRAMVerifier(passwordP1, 4096)
	if err != nil {
		t.Fatalf("generate drain P1 SCRAM verifier: %v", err)
	}

	// Pre-freeze rechecks: entry, prefix, anchor, lock and the retained keys.
	anchorCtx, cancelAnchor := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	anchor, err := f.run.CaptureControlAnchor(anchorCtx)
	cancelAnchor()
	if err != nil {
		t.Fatalf("capture original control anchor: %v", err)
	}
	owner := anchor.Diagnostics()
	if !owner.Present || owner.Invalidated {
		t.Fatalf("original control anchor absent or invalidated: %+v", owner)
	}
	binding := f.run.Binding()
	if owner.OriginalTargetKey != binding.OriginalTargetKey() || owner.ControlTargetKey != binding.ControlTargetKey() {
		t.Fatal("control anchor does not retain the original/control keys")
	}
	preRecheckCtx, cancelPreRecheck := context.WithTimeout(ctx, 60*time.Second)
	if err := entry.Recheck(preRecheckCtx); err != nil {
		cancelPreRecheck()
		t.Fatalf("pre-freeze entry recheck: %v", err)
	}
	if err := f.prefix.Recheck(t, preRecheckCtx); err != nil {
		cancelPreRecheck()
		t.Fatalf("pre-freeze prefix recheck: %v", err)
	}
	if err := anchor.Recheck(preRecheckCtx); err != nil {
		cancelPreRecheck()
		t.Fatalf("pre-freeze anchor recheck: %v", err)
	}
	preHealthCtx, cancelPreHealth := context.WithTimeout(ctx, borrowedOwnerRotationHealthBudget)
	preHealthErr := f.lock.Health(preHealthCtx)
	cancelPreHealth()
	cancelPreRecheck()
	if preHealthErr != nil {
		t.Fatalf("pre-freeze original control owner health: %v", preHealthErr)
	}

	// Step 2: two real staging holds on the same W/target. The prestartup hold
	// is never assumed backend-free.
	loaded := startBorrowedAuthStagingProcess(t, ctx, f.fx.containerID, borrowedAuthStagingHelperPath,
		[]string{"scram-hold", f.writerRole, f.targetDB, "127.0.0.1", "5432"}, f.writerPassword)
	loadedMode, loadedLocal, _ := loaded.awaitREADY(30 * time.Second)
	if loadedMode != "scram-hold" {
		t.Fatalf("loaded readiness mode is not the requested hold mode")
	}
	loaded.awaitSASLContinue(30 * time.Second)
	loadedToken := borrowedAuthStagingAwaitBackend(t, ctx, f.fx, loadedLocal)
	prestartup := startBorrowedAuthStagingProcess(t, ctx, f.fx.containerID, borrowedAuthStagingHelperPath,
		[]string{"prestartup-hold", f.writerRole, f.targetDB, "127.0.0.1", "5432"}, "unused-secret")
	preMode, _, _ := prestartup.awaitREADY(30 * time.Second)
	if preMode != "prestartup-hold" {
		t.Fatalf("prestartup readiness mode is not the requested hold mode")
	}
	t.Logf("staging holds: loaded backend PID %d start %d inode %s; prestartup helper PID %d start %d (never assumed backend-free)",
		loadedToken.ChildPID, loadedToken.ChildStart, loadedToken.Inode, prestartup.selfPID, prestartup.selfStart)

	// Actual negatives on the real strict data: token/PID-start mismatch and a
	// non-vacuous live FD table.
	borrowedAuthDrainTokenMismatchNegative(t, ctx, f, loadedToken)
	borrowedAuthDrainFDNonemptyNegative(t, ctx, f, loadedToken.ChildPID)

	// Step 3: revalidate both holds and register the SHARE-refusal holder
	// BEFORE the freeze (no new backend may be created while frozen).
	if err := borrowedAuthDrainRevalidateToken(ctx, f, loadedToken); err != nil {
		t.Fatalf("retained backend token revalidation refused: %v", err)
	}
	preStatCtx, cancelPreStat := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	preStatState, preStatStart, preStatErr := borrowedAuthStagingContainerStat(preStatCtx, f.fx.containerID, prestartup.selfPID)
	cancelPreStat()
	if preStatErr != nil || preStatStart != prestartup.selfStart || !borrowedAuthStagingLiveState(preStatState) {
		t.Fatalf("prestartup helper identity is not the retained live container process")
	}
	holderConnCtx, cancelHolderConn := context.WithTimeout(ctx, borrowedOwnerRotationAuthBudget)
	holderConn, err := pgx.Connect(holderConnCtx, f.adminDSN)
	cancelHolderConn()
	if err != nil {
		t.Fatalf("frozen SHARE holder connect: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), borrowedOwnerRotationCleanupBudget)
		_ = holderConn.Close(cleanupCtx)
		cancelCleanup()
	})
	holderBeginCtx, cancelHolderBegin := context.WithTimeout(ctx, borrowedOwnerRotationAuthBudget)
	holderTx, err := holderConn.Begin(holderBeginCtx)
	cancelHolderBegin()
	if err != nil {
		t.Fatalf("frozen SHARE holder begin: %v", err)
	}
	holderReleased := false
	t.Cleanup(func() {
		if !holderReleased {
			cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), borrowedOwnerRotationCleanupBudget)
			_ = holderTx.Rollback(cleanupCtx)
			cancelCleanup()
		}
	})
	holderLockCtx, cancelHolderLock := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	_, holderLockErr := holderTx.Exec(holderLockCtx, `LOCK TABLE pg_catalog.pg_authid IN ROW EXCLUSIVE MODE`)
	cancelHolderLock()
	if holderLockErr != nil {
		t.Fatalf("frozen SHARE holder ROW EXCLUSIVE refused (sqlstate=%s)", borrowedAuthSQLState(holderLockErr))
	}

	// Emergency same-incarnation CONT cleanup armed BEFORE the STOP attempt:
	// the STOP signal targets the captured PID/start pair and the resume flag is
	// armed on STOP-signal success, before any state confirmation, so a STOP
	// whose state check times out or errors is still resumed by cleanup. The
	// flag clears only after a successful CONT of the same PID/start; cleanup
	// can never signal a replacement incarnation and never turns a failure into
	// success.
	stopped := false
	freeze := func() error {
		if stopped {
			return nil
		}
		stopCtx, cancelStop := context.WithTimeout(ctx, 10*time.Second)
		defer cancelStop()
		if err := signalPostmaster(stopCtx, f.fx.containerID, f.fx.postmasterPID, f.fx.postmasterStr, "STOP"); err != nil {
			return err
		}
		// STOP-signal success arms the exact same-incarnation resume before the
		// state confirmation; a later awaitProcessState timeout/error must not
		// leave the postmaster stopped.
		stopped = true
		if _, err := awaitProcessState(stopCtx, f.fx.containerID, f.fx.postmasterPID, f.fx.postmasterStr, "T", "t"); err != nil {
			return err
		}
		return nil
	}
	resume := func() error {
		if !stopped {
			return nil
		}
		resumeCtx, cancelResume := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancelResume()
		if err := signalPostmaster(resumeCtx, f.fx.containerID, f.fx.postmasterPID, f.fx.postmasterStr, "CONT"); err != nil {
			return err
		}
		stopped = false
		return nil
	}
	t.Cleanup(func() {
		if err := resume(); err != nil {
			t.Errorf("guaranteed same-incarnation CONT cleanup failed: %v", err)
		}
	})

	// Negative: a wrong-start CONT must be refused, proving the bounded cleanup
	// can never signal a replacement incarnation.
	wrongStart, err := strconv.ParseUint(f.fx.postmasterStr, 10, 64)
	if err != nil || wrongStart == 0 {
		t.Fatalf("postmaster start identity is invalid")
	}
	wrongCtx, cancelWrong := context.WithTimeout(ctx, 10*time.Second)
	wrongErr := signalPostmaster(wrongCtx, f.fx.containerID, f.fx.postmasterPID, strconv.FormatUint(wrongStart+1, 10), "CONT")
	cancelWrong()
	if wrongErr == nil {
		t.Fatal("a wrong-start CONT was accepted")
	}

	// Actual bounded post-STOP failure/cleanup negative (the wrong-start CONT
	// check above does not cover this path): a real STOP whose state check is
	// forced to fail must still be resumed by the independently armed
	// same-incarnation cleanup, restoring the exact live postmaster; the
	// failure is never success.
	borrowedAuthDrainStopFailureCleanupNegative(t, ctx, f)

	// STOP the same postmaster only and confirm T plus the unchanged owner.
	if err := freeze(); err != nil {
		t.Fatalf("SIGSTOP exact postmaster: %v", err)
	}
	stopStateCtx, cancelStopState := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	stopState, stopStateErr := processState(stopStateCtx, f.fx.containerID, f.fx.postmasterPID, f.fx.postmasterStr)
	cancelStopState()
	if stopStateErr != nil || (stopState != "T" && stopState != "t") {
		t.Fatalf("postmaster is not frozen at the retained identity: state=%q err=%v", stopState, stopStateErr)
	}
	stopHealthCtx, cancelStopHealth := context.WithTimeout(ctx, borrowedOwnerRotationHealthBudget)
	stopHealthErr := f.lock.Health(stopHealthCtx)
	cancelStopHealth()
	if stopHealthErr != nil {
		t.Fatalf("original control owner changed during STOP: %v", stopHealthErr)
	}
	borrowedAuthDrainAuthTimeoutNegative(t, ctx, f)

	// Step 4: loaded-P0 exit while frozen. The backend becomes a same-start
	// zombie with an emptied FD table and no socket and is explicitly NOT
	// reaped yet; the client observes the genuine server EOF and the helper is
	// strictly reaped.
	if err := borrowedAuthDrainRevalidateToken(ctx, f, loadedToken); err != nil {
		t.Fatalf("retained backend token revalidation refused: %v", err)
	}
	var terminated bool
	terminateCtx, cancelTerminate := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	terminateErr := f.fx.admin.QueryRow(terminateCtx, `SELECT pg_terminate_backend($1)`, loadedToken.ChildPID).Scan(&terminated)
	cancelTerminate()
	if terminateErr != nil || !terminated {
		t.Fatalf("terminate the identified loaded backend refused: terminated=%t err=%v", terminated, terminateErr)
	}
	_ = loaded.awaitLine("CLIENT_SERVER_EOF", 30*time.Second)
	if err := loaded.awaitWait(30 * time.Second); err != nil {
		t.Fatalf("loaded helper sole Wait failed")
	}
	if marker := loaded.terminal(); marker != borrowedAuthStagingTerminalServerEOF {
		t.Fatalf("loaded terminal marker is not the genuine server EOF: %q", marker)
	}
	borrowedAuthStagingAwaitDisappearance(ctx, t, f.fx.containerID, loaded.selfPID, loaded.selfStart, borrowedAuthStagingDisappearanceBudget, true)
	if message := borrowedAuthStagingAwaitDisappearanceResult(ctx, f.fx.containerID, loadedToken.ChildPID, loadedToken.ChildStart, 2*time.Second, nil); message == "" {
		t.Fatal("a same-start zombie was accepted as a disappearance while frozen")
	}
	entries, readable := borrowedAuthDrainAwaitZombieDrained(ctx, t, f.fx.containerID, f.fx.postmasterPID, f.fx.postmasterStr, loadedToken.ChildPID, loadedToken.ChildStart)
	t.Logf("frozen exit gate: loaded backend PID %d same-start Z with empty FD table (entries=%d readable=%d) and no socket; not reaped yet", loadedToken.ChildPID, entries, readable)

	// Step 5: actual SHARE refusal, then the proven original-owner rotation.
	borrowedAuthDrainShareRefusalNegative(t, ctx, f)
	holderRollbackCtx, cancelHolderRollback := context.WithTimeout(context.Background(), borrowedOwnerRotationCleanupBudget)
	holderRollbackErr := holderTx.Rollback(holderRollbackCtx)
	cancelHolderRollback()
	if holderRollbackErr != nil {
		t.Fatalf("frozen SHARE holder rollback: %v", holderRollbackErr)
	}
	holderReleased = true
	ownerPID, callbackReached, rotationErr := borrowedAuthDrainOwnerRotation(ctx, f, owner, verifierP1, preState)
	if rotationErr != nil {
		t.Fatalf("frozen same-owner rotation transaction (commit outcome is UNKNOWN, never an accepted claim): %v", rotationErr)
	}
	if callbackReached != 1 {
		t.Fatalf("frozen rotation callback reached %d times, want exactly 1", callbackReached)
	}
	if ownerPID != owner.BackendPID {
		t.Fatalf("frozen rotation owner PID %d does not match the captured anchor PID %d", ownerPID, owner.BackendPID)
	}
	postRotationCtx, cancelPostRotation := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	postRotation, postRotationErr := borrowedOwnerRotationReadRoleState(postRotationCtx, f.fx.admin, f.writerRole)
	cancelPostRotation()
	if postRotationErr != nil {
		t.Fatalf("frozen post-rotation W role state: %v", postRotationErr)
	}
	if subtle.ConstantTimeCompare([]byte(postRotation.verifier), []byte(verifierP1)) != 1 {
		t.Fatal("committed verifier is not the exact generated P1 verifier")
	}
	borrowedOwnerRotationAssertRoleStateUnchanged(t, "frozen committed drain rotation", preState, postRotation)
	t.Logf("frozen same-owner rotation committed: owner pid=%d, W rotated to the exact client-generated P1 verifier under the same-tx SHARE fence", ownerPID)

	// Step 6: CONT, authoritative reap, cooperative prestartup quit, bounded
	// native auth checks, rechecks and the refreeze census.
	if err := resume(); err != nil {
		t.Fatalf("CONT exact postmaster: %v", err)
	}
	readyCtx, cancelReady := context.WithTimeout(ctx, 30*time.Second)
	readyErr := awaitNativePGReady(readyCtx, f.adminDSN)
	cancelReady()
	if readyErr != nil {
		t.Fatalf("native server did not recover after CONT: %v", readyErr)
	}
	borrowedAuthStagingAwaitDisappearance(ctx, t, f.fx.containerID, loadedToken.ChildPID, loadedToken.ChildStart, borrowedAuthStagingDisappearanceBudget, true)
	prestartup.quit()
	_ = prestartup.awaitLine("CLIENT_QUIT", 30*time.Second)
	if err := prestartup.awaitWait(30 * time.Second); err != nil {
		t.Fatalf("prestartup helper sole Wait failed")
	}
	if marker := prestartup.terminal(); marker != borrowedAuthStagingTerminalQuit {
		t.Fatalf("prestartup terminal marker is not the managed quit: %q", marker)
	}
	for _, line := range prestartup.out.snapshot() {
		if strings.TrimSpace(line) == borrowedAuthStagingTerminalServerEOF {
			t.Fatal("prestartup managed quit emitted server-EOF evidence")
		}
	}
	borrowedAuthStagingAwaitDisappearance(ctx, t, f.fx.containerID, prestartup.selfPID, prestartup.selfStart, borrowedAuthStagingDisappearanceBudget, true)
	t.Logf("prestartup cooperative quit: sole CLIENT_QUIT, no EOF evidence, strict helper disappearance")

	p0Ctx, cancelP0 := context.WithTimeout(ctx, borrowedOwnerRotationAuthBudget)
	p0Conn, p0Err := pgx.Connect(p0Ctx, f.writerTargetDSN)
	cancelP0()
	if p0Err == nil {
		p0CloseCtx, cancelP0Close := context.WithTimeout(context.Background(), borrowedOwnerRotationCleanupBudget)
		_ = p0Conn.Close(p0CloseCtx)
		cancelP0Close()
		t.Fatal("pre-rotation W credential authenticated after the committed drain rotation")
	}
	if code := borrowedAuthSQLState(p0Err); code != "28P01" {
		t.Fatalf("post-drain P0 rejection SQLSTATE=%s, want 28P01", code)
	}
	serverIPCtx, cancelServerIP := context.WithTimeout(ctx, borrowedOwnerRotationAuthBudget)
	serverIP, err := f.fx.container.ContainerIP(serverIPCtx)
	cancelServerIP()
	if err != nil {
		t.Fatalf("drain fixture container IP unavailable: %v", err)
	}
	for _, transport := range []string{"unix", "loopback", "serverip"} {
		checkCtx, cancelCheck := context.WithTimeout(ctx, 20*time.Second)
		line := runHelperAuthCheck(t, checkCtx, f.fx.containerID, transport, serverIP, f.writerRole, f.writerPassword)
		cancelCheck()
		if !strings.Contains(line, "state=REJECT") || !strings.Contains(line, "sqlstate=28P01") {
			t.Fatalf("native authcheck transport %s did not return a bounded genuine 28P01: %s", transport, line)
		}
	}
	p1DSN := borrowedAuthRoleDSN(t, f.writerTargetDSN, f.writerRole, passwordP1)
	p1Ctx, cancelP1 := context.WithTimeout(ctx, borrowedOwnerRotationAuthBudget)
	p1Conn, p1Err := pgx.Connect(p1Ctx, p1DSN)
	cancelP1()
	if p1Err != nil {
		t.Fatalf("post-drain P1 authentication was refused (sqlstate=%s)", borrowedAuthSQLState(p1Err))
	}
	var one int
	p1QueryCtx, cancelP1Query := context.WithTimeout(ctx, borrowedOwnerRotationAuthBudget)
	p1QueryErr := p1Conn.QueryRow(p1QueryCtx, `SELECT 1`).Scan(&one)
	cancelP1Query()
	p1CloseCtx, cancelP1Close := context.WithTimeout(context.Background(), borrowedOwnerRotationCleanupBudget)
	_ = p1Conn.Close(p1CloseCtx)
	cancelP1Close()
	if p1QueryErr != nil || one != 1 {
		t.Fatalf("post-drain P1 SELECT 1: one=%d err=%v", one, p1QueryErr)
	}
	t.Logf("post-drain auth: P0 28P01 (pgx + native authcheck on unix/loopback/serverip); P1 accepted")

	postRecheckCtx, cancelPostRecheck := context.WithTimeout(ctx, 60*time.Second)
	if err := entry.Recheck(postRecheckCtx); err != nil {
		cancelPostRecheck()
		t.Fatalf("post-drain entry recheck: %v", err)
	}
	if err := f.prefix.Recheck(t, postRecheckCtx); err != nil {
		cancelPostRecheck()
		t.Fatalf("post-drain prefix recheck: %v", err)
	}
	if err := anchor.Recheck(postRecheckCtx); err != nil {
		cancelPostRecheck()
		t.Fatalf("post-drain anchor recheck: %v", err)
	}
	postHealthCtx, cancelPostHealth := context.WithTimeout(ctx, borrowedOwnerRotationHealthBudget)
	postHealthErr := f.lock.Health(postHealthCtx)
	cancelPostHealth()
	cancelPostRecheck()
	if postHealthErr != nil {
		t.Fatalf("post-drain original control owner health: %v", postHealthErr)
	}

	// Refreeze + bounded strict census + committed catalog verification.
	if err := freeze(); err != nil {
		t.Fatalf("refreeze exact postmaster: %v", err)
	}
	report := borrowedAuthDrainStrictCensusBounded(t, ctx, f.fx)
	t.Logf("refrozen strict census: children=%d sockets=%d child_enoent=%d", len(report.Children), len(report.Sockets), report.ChildENOENT)
	if err := resume(); err != nil {
		t.Fatalf("CONT after refreeze: %v", err)
	}
	readyAgainCtx, cancelReadyAgain := context.WithTimeout(ctx, 30*time.Second)
	readyAgainErr := awaitNativePGReady(readyAgainCtx, f.adminDSN)
	cancelReadyAgain()
	if readyAgainErr != nil {
		t.Fatalf("native server did not recover after refreeze CONT: %v", readyAgainErr)
	}
	finalCtx, cancelFinal := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	finalState, finalErr := borrowedOwnerRotationReadRoleState(finalCtx, f.fx.admin, f.writerRole)
	cancelFinal()
	if finalErr != nil {
		t.Fatalf("final committed W role state: %v", finalErr)
	}
	if subtle.ConstantTimeCompare([]byte(finalState.verifier), []byte(verifierP1)) != 1 {
		t.Fatal("final committed verifier is not the exact generated P1 verifier")
	}
	borrowedOwnerRotationAssertRoleStateUnchanged(t, "final committed drain", preState, finalState)
	finalPmCtx, cancelFinalPm := context.WithTimeout(ctx, 10*time.Second)
	finalStateStr, finalPmErr := processState(finalPmCtx, f.fx.containerID, f.fx.postmasterPID, f.fx.postmasterStr)
	cancelFinalPm()
	if finalPmErr != nil || finalStateStr == "Z" {
		t.Fatalf("postmaster incarnation changed/restarted during the drain lane: state=%q err=%v", finalStateStr, finalPmErr)
	}
	t.Logf("drain lane complete: postmaster PID %d start %s retained across STOP/CONT/refreeze; no restart/reacquire", f.fx.postmasterPID, f.fx.postmasterStr)

	// Final bounded owner-loss negative: terminate the captured control owner
	// and require both the retained checkpoint and the lock to refuse.
	controlPID := binding.ControlBackendPID()
	if controlPID <= 0 || controlPID != owner.BackendPID {
		t.Fatalf("captured control owner PID %d is missing or not the anchored owner PID %d", controlPID, owner.BackendPID)
	}
	var ownerTerminated bool
	ownerLossCtx, cancelOwnerLoss := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	ownerLossErr := f.controlPool.QueryRow(ownerLossCtx, `SELECT pg_terminate_backend($1)`, controlPID).Scan(&ownerTerminated)
	cancelOwnerLoss()
	if ownerLossErr != nil || !ownerTerminated {
		t.Fatalf("terminate captured control owner: terminated=%t err=%v", ownerTerminated, ownerLossErr)
	}
	ownerGoneDeadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(ownerGoneDeadline) {
		var present int
		rowCtx, cancelRow := context.WithTimeout(ctx, 5*time.Second)
		rowErr := f.controlPool.QueryRow(rowCtx, `SELECT count(*) FROM pg_stat_activity WHERE pid=$1`, controlPID).Scan(&present)
		cancelRow()
		if rowErr != nil || present == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	lossCtx, cancelLoss := context.WithTimeout(ctx, borrowedOwnerRotationAuthBudget)
	lossHealthErr := f.lock.Health(lossCtx)
	lossRecheckErr := entry.Recheck(lossCtx)
	cancelLoss()
	if lossHealthErr == nil {
		t.Fatal("original control owner health remained successful after real owner loss")
	}
	if lossRecheckErr == nil {
		t.Fatal("auth entry recheck remained successful after real owner loss")
	}
	_, reason := entry.stageNow()
	if reason == "" {
		t.Fatalf("owner-loss refusal did not permanently invalidate the checkpoint: reason=%q", reason)
	}
	t.Logf("owner-loss negative: control owner PID %d terminated; lock health and entry recheck refused, checkpoint invalidated (%s)", controlPID, reason)
}

// ---------------------------------------------------------------------------
// Committed-fence gap: same-owner revalidation lane
// ---------------------------------------------------------------------------

// errBorrowedFenceGapVerifierDrift is the distinct refusal of the complete
// successful fenced comparison: the SHARE fence was acquired, the supplied-tx
// owner identity/namespace checks passed and the committed verifier was read,
// but it no longer equals the expected preserved verifier. It is never
// returned for any earlier or unknown refusal, it never repairs the drift and
// it grants no reusable token.
var errBorrowedFenceGapVerifierDrift = errors.New("committed catalog verifier drifted under the same-owner fence")

// borrowedFenceGapRoleStateEqual compares the immutable W catalog facts (never
// the verifier value) and the exact membership closure.
func borrowedFenceGapRoleStateEqual(a, b borrowedOwnerRotationRoleState) bool {
	if a.oid != b.oid || a.login != b.login || a.super != b.super || a.createdb != b.createdb ||
		a.createrole != b.createrole || a.inherit != b.inherit || a.replication != b.replication || a.bypass != b.bypass {
		return false
	}
	if len(a.memberships) != len(b.memberships) {
		return false
	}
	for i := range a.memberships {
		if a.memberships[i] != b.memberships[i] {
			return false
		}
	}
	return true
}

// borrowedFenceGapFencedCheck is the fresh same-owner fenced check: the exact
// f.lock session and its SUPPLIED tx only (never the admin/pool as owner),
// short caller-derived children for every step, the supplied-tx owner identity
// and advisory namespace checks, SHARE NOWAIT strictly BEFORE any verifier or
// role-fact read, then the complete comparison. The optional whileHeld hook
// runs strictly after SHARE acquisition and before the reads. A complete
// successful comparison that finds a different committed verifier returns the
// distinct drift sentinel; a role-fact change returns a different error. The
// helper never repairs anything and returns only an error.
func borrowedFenceGapFencedCheck(ctx context.Context, f *borrowedAuthHandoffFixture, owner recovery.DrillControlAnchorFacts, expectedVerifier string, expectedState borrowedOwnerRotationRoleState, whileHeld func(context.Context) error) error {
	if expectedVerifier == "" {
		return errors.New("fenced check requires the expected committed verifier")
	}
	checkCtx, cancelCheck := context.WithTimeout(ctx, 120*time.Second)
	defer cancelCheck()
	var callbackReached int32
	checkErr := f.lock.WithTransaction(checkCtx, func(callbackCtx context.Context, tx pgx.Tx) error {
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
			return errors.New("fenced check owner identity read through supplied tx refused")
		}
		systemCtx, cancelSystem := context.WithTimeout(callbackCtx, borrowedOwnerRotationQueryBudget)
		systemErr := tx.QueryRow(systemCtx, `SELECT system_identifier::text FROM pg_control_system()`).Scan(&systemID)
		cancelSystem()
		if systemErr != nil {
			return errors.New("fenced check cluster identity read through supplied tx refused")
		}
		if observedPID != owner.BackendPID || !ownerStart.Equal(owner.BackendStart) || controlDBOID != owner.ControlDatabaseOID ||
			!postmasterStart.Equal(owner.PostmasterStart) || systemID != owner.SystemIdentifier {
			return errors.New("fenced check original control owner identity changed")
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
			return errors.New("fenced check advisory namespace read through supplied tx refused")
		}
		if !namespaceHeld {
			return errors.New("fenced check original granted advisory namespace is not held")
		}
		// SHARE NOWAIT strictly BEFORE reading the verifier or the role facts.
		fenceCtx, cancelFence := context.WithTimeout(callbackCtx, borrowedOwnerRotationQueryBudget)
		_, fenceErr := tx.Exec(fenceCtx, `LOCK TABLE pg_catalog.pg_authid, pg_catalog.pg_auth_members IN SHARE MODE NOWAIT`)
		cancelFence()
		if fenceErr != nil {
			return &borrowedOwnerRotationSQLStateError{stage: "fenced check SHARE refused", code: borrowedAuthSQLState(fenceErr), cause: fenceErr}
		}
		if whileHeld != nil {
			if err := whileHeld(callbackCtx); err != nil {
				return fmt.Errorf("fenced check while-held proof refused: %w", err)
			}
		}
		var storedVerifier string
		storedCtx, cancelStored := context.WithTimeout(callbackCtx, borrowedOwnerRotationQueryBudget)
		storedErr := tx.QueryRow(storedCtx, `SELECT coalesce(rolpassword,'') FROM pg_authid WHERE rolname=$1`, f.writerRole).Scan(&storedVerifier)
		cancelStored()
		if storedErr != nil {
			return errors.New("fenced check stored verifier read through supplied tx refused")
		}
		// The bounded in-transaction role-state read and the fact comparison
		// complete FIRST: the drift sentinel is returned only for a verifier
		// mismatch with unchanged OID/flags/membership facts. An unknown
		// verifier/role read and any OID/flags/membership change return
		// non-sentinel errors.
		inTxCtx, cancelInTx := context.WithTimeout(callbackCtx, borrowedOwnerRotationQueryBudget)
		inTx, inTxErr := borrowedOwnerRotationReadRoleState(inTxCtx, tx, f.writerRole)
		cancelInTx()
		if inTxErr != nil {
			return errors.New("fenced check in-transaction W role state read refused")
		}
		if !borrowedFenceGapRoleStateEqual(inTx, expectedState) {
			return errors.New("fenced check W role facts changed under the same-owner fence")
		}
		if subtle.ConstantTimeCompare([]byte(storedVerifier), []byte(expectedVerifier)) != 1 {
			return fmt.Errorf("%w (observed_length=%d expected_length=%d)", errBorrowedFenceGapVerifierDrift, len(storedVerifier), len(expectedVerifier))
		}
		return nil
	})
	if checkErr == nil && atomic.LoadInt32(&callbackReached) != 1 {
		return fmt.Errorf("fenced check callback reached %d times, want exactly 1", atomic.LoadInt32(&callbackReached))
	}
	return checkErr
}

// borrowedFenceGapCrossDBMutatorRefusal attempts one real ALTER ROLE through a
// separately owned bounded connection to the W-owned TARGET database (a
// different database from the owner's control database) while the owner's
// SHARE fence is held. It succeeds only when the mutation is refused with the
// exact SQLSTATE 55P03 and the bounded rollback/close completed cleanly; any
// other outcome or cleanup failure is returned as an error.
func borrowedFenceGapCrossDBMutatorRefusal(ctx context.Context, t *testing.T, f *borrowedAuthHandoffFixture, verifier string) error {
	t.Helper()
	routeCtx, cancelRoute := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	baseDSN := borrowedIdentityRoute(t, routeCtx, f.fx, f.targetDB)
	cancelRoute()
	dsn := borrowedAuthRoleDSN(t, baseDSN, f.adminRole, f.adminPassword)
	connectCtx, cancelConnect := context.WithTimeout(ctx, borrowedOwnerRotationAuthBudget)
	conn, err := pgx.Connect(connectCtx, dsn)
	cancelConnect()
	if err != nil {
		return errors.New("cross-db mutator connect refused")
	}
	beginCtx, cancelBegin := context.WithTimeout(ctx, borrowedOwnerRotationAuthBudget)
	tx, beginErr := conn.Begin(beginCtx)
	cancelBegin()
	if beginErr != nil {
		closeCtx, cancelClose := context.WithTimeout(context.Background(), borrowedOwnerRotationCleanupBudget)
		closeErr := conn.Close(closeCtx)
		cancelClose()
		return errors.Join(errors.New("cross-db mutator begin refused"), closeErr)
	}
	cleanup := func() error {
		cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), borrowedOwnerRotationCleanupBudget)
		defer cancelCleanup()
		rollbackErr := tx.Rollback(cleanupCtx)
		closeErr := conn.Close(cleanupCtx)
		return errors.Join(rollbackErr, closeErr)
	}
	lockCtx, cancelLock := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	_, lockErr := tx.Exec(lockCtx, `SET LOCAL lock_timeout = '2000ms'`)
	cancelLock()
	if lockErr != nil {
		return errors.Join(errors.New("cross-db mutator lock_timeout refused"), cleanup())
	}
	alterCtx, cancelAlter := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	_, alterErr := tx.Exec(alterCtx, `ALTER ROLE `+pgx.Identifier{f.writerRole}.Sanitize()+` PASSWORD `+sqlLiteral(verifier))
	cancelAlter()
	cleanupErr := cleanup()
	if code := borrowedAuthSQLState(alterErr); code != "55P03" {
		return fmt.Errorf("cross-db mutator was not refused with 55P03 (sqlstate=%s)", code)
	}
	if cleanupErr != nil {
		return fmt.Errorf("cross-db mutator bounded rollback/close failed: %w", cleanupErr)
	}
	return nil
}

// borrowedFenceGapBaseline captures the actual anchor of a factory-created
// borrowed run and commits the exact client-generated P1 verifier through the
// same-owner f.lock.WithTransaction mechanics, verifying the committed
// verifier and the unchanged role facts. It returns the anchor, its facts, the
// captured pre-rotation state and the generated P1 verifier (never the
// password).
func borrowedFenceGapBaseline(t *testing.T, ctx context.Context, f *borrowedAuthHandoffFixture) (recovery.DrillControlAnchor, recovery.DrillControlAnchorFacts, borrowedOwnerRotationRoleState, string) {
	t.Helper()
	anchorCtx, cancelAnchor := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	anchor, err := f.run.CaptureControlAnchor(anchorCtx)
	cancelAnchor()
	if err != nil {
		t.Fatalf("fence-gap baseline anchor capture: %v", err)
	}
	owner := anchor.Diagnostics()
	if !owner.Present || owner.Invalidated {
		t.Fatal("fence-gap baseline anchor is absent or invalidated")
	}
	preCtx, cancelPre := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	preState, err := borrowedOwnerRotationReadRoleState(preCtx, f.fx.admin, f.writerRole)
	cancelPre()
	if err != nil {
		t.Fatalf("fence-gap baseline W role state: %v", err)
	}
	passwordP1 := borrowedAuthCredential(t, "fence gap P1")
	verifierP1, err := generateSCRAMVerifier(passwordP1, 4096)
	if err != nil {
		t.Fatalf("fence-gap baseline P1 verifier: %v", err)
	}
	ownerPID, callbackReached, rotationErr := borrowedAuthDrainOwnerRotation(ctx, f, owner, verifierP1, preState)
	if rotationErr != nil {
		t.Fatalf("fence-gap baseline committed P1 rotation: %v", rotationErr)
	}
	if callbackReached != 1 || ownerPID != owner.BackendPID {
		t.Fatalf("fence-gap baseline rotation callback/owner mismatch: callback=%d ownerPID=%d anchorPID=%d", callbackReached, ownerPID, owner.BackendPID)
	}
	postCtx, cancelPost := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	postState, postErr := borrowedOwnerRotationReadRoleState(postCtx, f.fx.admin, f.writerRole)
	cancelPost()
	if postErr != nil {
		t.Fatalf("fence-gap baseline committed state read: %v", postErr)
	}
	if subtle.ConstantTimeCompare([]byte(postState.verifier), []byte(verifierP1)) != 1 {
		t.Fatal("fence-gap baseline committed verifier is not the exact generated P1 verifier")
	}
	borrowedOwnerRotationAssertRoleStateUnchanged(t, "fence-gap baseline", preState, postState)
	return anchor, owner, preState, verifierP1
}

// borrowedFenceGapStableNegative proves the stable-P1 fenced success and the
// real cross-DB mutator exclusion while the SHARE fence is held.
func borrowedFenceGapStableNegative(t *testing.T, ctx context.Context) {
	t.Helper()
	f := newBorrowedAuthHandoffFixture(t)
	_, owner, preState, verifierP1 := borrowedFenceGapBaseline(t, ctx, f)
	checkErr := borrowedFenceGapFencedCheck(ctx, f, owner, verifierP1, preState, func(callbackCtx context.Context) error {
		return borrowedFenceGapCrossDBMutatorRefusal(callbackCtx, t, f, preState.verifier)
	})
	if checkErr != nil {
		t.Fatalf("stable-P1 fenced check refused: %v", checkErr)
	}
	finalCtx, cancelFinal := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	finalState, finalErr := borrowedOwnerRotationReadRoleState(finalCtx, f.fx.admin, f.writerRole)
	cancelFinal()
	if finalErr != nil {
		t.Fatalf("stable-P1 post-check W role state: %v", finalErr)
	}
	if subtle.ConstantTimeCompare([]byte(finalState.verifier), []byte(verifierP1)) != 1 {
		t.Fatal("stable-P1 fenced check changed the committed verifier")
	}
	t.Logf("stable-P1 negative: the cross-DB mutator was refused with 55P03 while the SHARE fence was held and the fenced check completed with the committed P1 verifier unchanged")
}

// borrowedFenceGapDriftNegative proves the committed-fence gap detection: a
// separately owned connection commits the captured P0 restoration and the
// fresh fenced check refuses it with the specific verifier-drift sentinel,
// without repairing the drift.
func borrowedFenceGapDriftNegative(t *testing.T, ctx context.Context) {
	t.Helper()
	f := newBorrowedAuthHandoffFixture(t)
	_, owner, preState, verifierP1 := borrowedFenceGapBaseline(t, ctx, f)
	gapCtx, cancelGap := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	_, gapErr := f.fx.admin.Exec(gapCtx, `ALTER ROLE `+pgx.Identifier{f.writerRole}.Sanitize()+` PASSWORD `+sqlLiteral(preState.verifier))
	cancelGap()
	if gapErr != nil {
		t.Fatalf("drift negative separately owned committed P0 restoration refused: %v", gapErr)
	}
	afterGapCtx, cancelAfterGap := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	afterGap, afterGapErr := borrowedOwnerRotationReadRoleState(afterGapCtx, f.fx.admin, f.writerRole)
	cancelAfterGap()
	if afterGapErr != nil {
		t.Fatalf("drift negative exact catalog read: %v", afterGapErr)
	}
	if subtle.ConstantTimeCompare([]byte(afterGap.verifier), []byte(preState.verifier)) != 1 {
		t.Fatal("drift negative committed P0 restoration was not the exact captured verifier")
	}
	if subtle.ConstantTimeCompare([]byte(afterGap.verifier), []byte(verifierP1)) == 1 {
		t.Fatal("drift negative committed value still equals the preserved P1 verifier")
	}
	checkErr := borrowedFenceGapFencedCheck(ctx, f, owner, verifierP1, preState, nil)
	if !errors.Is(checkErr, errBorrowedFenceGapVerifierDrift) {
		t.Fatalf("drift negative was not refused with the specific verifier-drift sentinel: %v", checkErr)
	}
	postCheckCtx, cancelPostCheck := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	postCheck, postCheckErr := borrowedOwnerRotationReadRoleState(postCheckCtx, f.fx.admin, f.writerRole)
	cancelPostCheck()
	if postCheckErr != nil {
		t.Fatalf("drift negative post-check catalog read: %v", postCheckErr)
	}
	if subtle.ConstantTimeCompare([]byte(postCheck.verifier), []byte(preState.verifier)) != 1 {
		t.Fatal("drift negative fenced check repaired the committed verifier")
	}
	t.Logf("drift negative: committed P0 restoration detected by the fresh fenced check with the specific verifier-drift sentinel; no repair and no reusable token")
}

// borrowedFenceGapDriftWithFactsNegative proves the ordering discipline: a
// verifier drift committed TOGETHER with a real role-fact drift (CREATEDB)
// must be refused with the non-sentinel role-facts error, never with the
// verifier-drift sentinel.
func borrowedFenceGapDriftWithFactsNegative(t *testing.T, ctx context.Context) {
	t.Helper()
	f := newBorrowedAuthHandoffFixture(t)
	_, owner, preState, verifierP1 := borrowedFenceGapBaseline(t, ctx, f)
	gapCtx, cancelGap := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	_, gapErr := f.fx.admin.Exec(gapCtx, `ALTER ROLE `+pgx.Identifier{f.writerRole}.Sanitize()+` PASSWORD `+sqlLiteral(preState.verifier)+` CREATEDB`)
	cancelGap()
	if gapErr != nil {
		t.Fatalf("combined drift negative mutation refused: %v", gapErr)
	}
	afterCtx, cancelAfter := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	after, afterErr := borrowedOwnerRotationReadRoleState(afterCtx, f.fx.admin, f.writerRole)
	cancelAfter()
	if afterErr != nil {
		t.Fatalf("combined drift negative exact catalog read: %v", afterErr)
	}
	if subtle.ConstantTimeCompare([]byte(after.verifier), []byte(preState.verifier)) != 1 {
		t.Fatal("combined drift negative verifier is not the exact captured P0 verifier")
	}
	if after.createdb == preState.createdb {
		t.Fatal("combined drift negative role flag did not actually change")
	}
	checkErr := borrowedFenceGapFencedCheck(ctx, f, owner, verifierP1, preState, nil)
	if checkErr == nil {
		t.Fatal("combined verifier+fact drift was accepted by the fenced check")
	}
	if errors.Is(checkErr, errBorrowedFenceGapVerifierDrift) {
		t.Fatal("combined verifier+fact drift was misreported as the verifier-drift sentinel")
	}
	if !strings.Contains(checkErr.Error(), "W role facts changed") {
		t.Fatalf("combined drift refusal did not report the role-fact change: %v", checkErr)
	}
	t.Logf("combined drift negative: verifier drift plus a real CREATEDB role-fact drift refused with the non-sentinel role-facts error")
}

// borrowedFenceGapShareHolderNegative proves the fenced SHARE refusal: a
// separately owned ROW EXCLUSIVE holder makes the SHARE NOWAIT acquisition
// fail with 55P03 before any verifier or role-fact read, and the refusal is
// never reported as verifier drift.
func borrowedFenceGapShareHolderNegative(t *testing.T, ctx context.Context) {
	t.Helper()
	f := newBorrowedAuthHandoffFixture(t)
	_, owner, preState, _ := borrowedFenceGapBaseline(t, ctx, f)
	holderConnCtx, cancelHolderConn := context.WithTimeout(ctx, borrowedOwnerRotationAuthBudget)
	holderConn, err := pgx.Connect(holderConnCtx, f.adminDSN)
	cancelHolderConn()
	if err != nil {
		t.Fatalf("fence-gap holder connect: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), borrowedOwnerRotationCleanupBudget)
		_ = holderConn.Close(cleanupCtx)
		cancelCleanup()
	})
	holderBeginCtx, cancelHolderBegin := context.WithTimeout(ctx, borrowedOwnerRotationAuthBudget)
	holderTx, err := holderConn.Begin(holderBeginCtx)
	cancelHolderBegin()
	if err != nil {
		t.Fatalf("fence-gap holder begin: %v", err)
	}
	holderReleased := false
	t.Cleanup(func() {
		if !holderReleased {
			cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), borrowedOwnerRotationCleanupBudget)
			_ = holderTx.Rollback(cleanupCtx)
			cancelCleanup()
		}
	})
	holderLockCtx, cancelHolderLock := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	_, holderLockErr := holderTx.Exec(holderLockCtx, `LOCK TABLE pg_catalog.pg_authid IN ROW EXCLUSIVE MODE`)
	cancelHolderLock()
	if holderLockErr != nil {
		t.Fatalf("fence-gap holder ROW EXCLUSIVE refused (sqlstate=%s)", borrowedAuthSQLState(holderLockErr))
	}
	checkErr := borrowedFenceGapFencedCheck(ctx, f, owner, preState.verifier, preState, nil)
	if checkErr == nil {
		t.Fatal("fenced check SHARE succeeded while a separate holder held ROW EXCLUSIVE")
	}
	if errors.Is(checkErr, errBorrowedFenceGapVerifierDrift) {
		t.Fatal("fenced SHARE refusal was misreported as verifier drift")
	}
	if code := borrowedAuthSQLState(checkErr); code != "55P03" {
		t.Fatalf("fenced SHARE refusal SQLSTATE=%s, want 55P03", code)
	}
	holderRollbackCtx, cancelHolderRollback := context.WithTimeout(context.Background(), borrowedOwnerRotationCleanupBudget)
	holderRollbackErr := holderTx.Rollback(holderRollbackCtx)
	cancelHolderRollback()
	if holderRollbackErr != nil {
		t.Fatalf("fence-gap holder rollback: %v", holderRollbackErr)
	}
	holderReleased = true
	holderCloseCtx, cancelHolderClose := context.WithTimeout(context.Background(), borrowedOwnerRotationCleanupBudget)
	holderCloseErr := holderConn.Close(holderCloseCtx)
	cancelHolderClose()
	if holderCloseErr != nil {
		t.Fatalf("fence-gap holder close: %v", holderCloseErr)
	}
	t.Logf("holder negative: a separately owned ROW EXCLUSIVE holder made the fenced SHARE fail with 55P03 before any verifier/role-fact read (no continuation)")
}

// borrowedFenceGapCancelNegative proves the caller-cancel refusal at the
// reached stage: an ended caller context refuses the fenced check before any
// fence or catalog read, and the failed begin is destructive to the owner
// session, leaving no reusable replacement owner and no retry.
func borrowedFenceGapCancelNegative(t *testing.T, ctx context.Context) {
	t.Helper()
	f := newBorrowedAuthHandoffFixture(t)
	_, owner, preState, _ := borrowedFenceGapBaseline(t, ctx, f)
	cancelCtx, cancelCheck := context.WithCancel(ctx)
	cancelCheck()
	cancelErr := borrowedFenceGapFencedCheck(cancelCtx, f, owner, preState.verifier, preState, nil)
	if cancelErr == nil {
		t.Fatal("fenced check with an ended caller context succeeded")
	}
	if errors.Is(cancelErr, errBorrowedFenceGapVerifierDrift) {
		t.Fatal("caller cancel was misreported as verifier drift")
	}
	healthCtx, cancelHealth := context.WithTimeout(ctx, borrowedOwnerRotationHealthBudget)
	healthErr := f.lock.Health(healthCtx)
	cancelHealth()
	if healthErr == nil {
		t.Fatal("a canceled fenced check left a reusable replacement owner")
	}
	t.Logf("cancel negative: an ended caller context refused the fenced check at the reached stage and left no replacement owner or retry")
}

// borrowedFenceGapOwnerLossNegative proves the real owner-loss refusal at the
// reached stage with no replacement owner and no retry.
func borrowedFenceGapOwnerLossNegative(t *testing.T, ctx context.Context) {
	t.Helper()
	f := newBorrowedAuthHandoffFixture(t)
	anchor, owner, preState, _ := borrowedFenceGapBaseline(t, ctx, f)
	controlPID := f.run.Binding().ControlBackendPID()
	if controlPID <= 0 || controlPID != owner.BackendPID {
		t.Fatalf("fence-gap captured control owner PID %d is missing or not the anchored owner PID %d", controlPID, owner.BackendPID)
	}
	var terminated bool
	lossCtx, cancelLoss := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	lossErr := f.controlPool.QueryRow(lossCtx, `SELECT pg_terminate_backend($1)`, controlPID).Scan(&terminated)
	cancelLoss()
	if lossErr != nil || !terminated {
		t.Fatalf("terminate captured fence-gap control owner: terminated=%t err=%v", terminated, lossErr)
	}
	ownerGoneDeadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(ownerGoneDeadline) {
		var present int
		rowCtx, cancelRow := context.WithTimeout(ctx, 5*time.Second)
		rowErr := f.controlPool.QueryRow(rowCtx, `SELECT count(*) FROM pg_stat_activity WHERE pid=$1`, controlPID).Scan(&present)
		cancelRow()
		if rowErr != nil || present == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	lossCheckErr := borrowedFenceGapFencedCheck(ctx, f, owner, preState.verifier, preState, nil)
	if lossCheckErr == nil {
		t.Fatal("fenced check succeeded after real control owner loss")
	}
	if errors.Is(lossCheckErr, errBorrowedFenceGapVerifierDrift) {
		t.Fatal("real owner loss was misreported as verifier drift")
	}
	healthCtx, cancelHealth := context.WithTimeout(ctx, borrowedOwnerRotationHealthBudget)
	healthErr := f.lock.Health(healthCtx)
	cancelHealth()
	if healthErr == nil {
		t.Fatal("original control owner health remained successful after real owner loss")
	}
	anchorRecheckCtx, cancelAnchorRecheck := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	anchorRecheckErr := anchor.Recheck(anchorRecheckCtx)
	cancelAnchorRecheck()
	if anchorRecheckErr == nil {
		t.Fatal("retained anchor rechecked after real owner loss")
	}
	t.Logf("owner-loss negative: real control owner loss refused the fenced check at the reached stage with no replacement owner and no retry")
}

// TestBorrowedCatalogFenceGap is the bounded committed-fence gap lane: the
// genuine provenance chain and one-time entry, a committed P1 baseline through
// the same-owner supplied-tx mechanics, the actual gap (a separately owned
// connection commits the captured P0 restoration while retained health and
// anchor identity still succeed), the fresh fenced check that detects the
// drift with a distinct sentinel without repairing it, and the fresh-fixture
// negatives (stable-P1 success with a real cross-DB mutator refused 55P03
// while held, drift sentinel, ROW EXCLUSIVE SHARE refusal, caller cancel and
// real owner loss). It grants no continuous fence, no DDL, no rebuild, no
// acceptance and no Gate1 authority; COMMIT errors remain UNKNOWN.
func TestBorrowedCatalogFenceGap(t *testing.T) {
	ctx := t.Context()

	// Step 1: real provenance. Reuse the genuine positive chain, consume the
	// authentic one-use token through the real entry, capture the actual anchor
	// and preserve the keys/OID/fingerprint with an empty instance identity.
	f, session, outcome := borrowedAuthDrainPositiveChain(t)
	entry, err := newBorrowedReceiptAuthEntry(f, session, outcome.handoff)
	if err != nil {
		t.Fatalf("fence-gap receipt auth entry construction: %v", err)
	}
	enterCtx, cancelEnter := context.WithTimeout(ctx, borrowedOwnerRotationAuthBudget)
	enterErr := entry.Enter(enterCtx)
	cancelEnter()
	if enterErr != nil {
		t.Fatalf("fence-gap one-time auth entry: %v", enterErr)
	}
	if !outcome.handoff.Diagnostics().Consumed {
		t.Fatal("fence-gap entry did not consume the authentic one-use token")
	}
	if atomic.LoadInt32(&f.probeCalls) != 1 || atomic.LoadInt32(&f.acceptanceCalls) != 0 {
		t.Fatalf("fence-gap provenance probe/acceptance counts: probe=%d acceptance=%d", atomic.LoadInt32(&f.probeCalls), atomic.LoadInt32(&f.acceptanceCalls))
	}
	anchorCtx, cancelAnchor := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	anchor, err := f.run.CaptureControlAnchor(anchorCtx)
	cancelAnchor()
	if err != nil {
		t.Fatalf("fence-gap actual anchor capture: %v", err)
	}
	owner := anchor.Diagnostics()
	if !owner.Present || owner.Invalidated {
		t.Fatalf("fence-gap anchor absent or invalidated: %+v", owner)
	}
	binding := f.run.Binding()
	if owner.OriginalTargetKey != binding.OriginalTargetKey() || owner.ControlTargetKey != binding.ControlTargetKey() {
		t.Fatal("fence-gap anchor does not retain the original/control keys")
	}
	if owner.OriginalRoleFingerprint == "" || owner.OriginalRoleFingerprint != binding.OriginalRoleFingerprint() {
		t.Fatal("fence-gap anchor does not retain the original role fingerprint")
	}
	if owner.OriginalInstanceID != "" || binding.OriginalInstanceID() != "" {
		t.Fatal("fence-gap provenance disclosed a non-empty instance identity")
	}
	if f.writerRoleOID == 0 || f.writerRoleOID != binding.WriterRoleOID() {
		t.Fatal("fence-gap provenance lost the immutable writer role OID")
	}

	// Step 2: committed P1 baseline through the same-owner supplied-tx
	// mechanics (never the admin/pool as owner).
	preStateCtx, cancelPreState := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	preState, err := borrowedOwnerRotationReadRoleState(preStateCtx, f.fx.admin, f.writerRole)
	cancelPreState()
	if err != nil {
		t.Fatalf("fence-gap pre-baseline W role state: %v", err)
	}
	passwordP1 := borrowedAuthCredential(t, "fence gap P1")
	verifierP1, err := generateSCRAMVerifier(passwordP1, 4096)
	if err != nil {
		t.Fatalf("fence-gap generate P1 SCRAM verifier: %v", err)
	}
	ownerPID, callbackReached, rotationErr := borrowedAuthDrainOwnerRotation(ctx, f, owner, verifierP1, preState)
	if rotationErr != nil {
		t.Fatalf("fence-gap committed P1 baseline rotation (commit outcome is UNKNOWN, never an accepted claim): %v", rotationErr)
	}
	if callbackReached != 1 || ownerPID != owner.BackendPID {
		t.Fatalf("fence-gap baseline callback/owner mismatch: callback=%d ownerPID=%d anchorPID=%d", callbackReached, ownerPID, owner.BackendPID)
	}
	postBaselineCtx, cancelPostBaseline := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	postBaseline, postBaselineErr := borrowedOwnerRotationReadRoleState(postBaselineCtx, f.fx.admin, f.writerRole)
	cancelPostBaseline()
	if postBaselineErr != nil {
		t.Fatalf("fence-gap committed baseline state read: %v", postBaselineErr)
	}
	if subtle.ConstantTimeCompare([]byte(postBaseline.verifier), []byte(verifierP1)) != 1 {
		t.Fatal("fence-gap committed baseline verifier is not the exact generated P1 verifier")
	}
	borrowedOwnerRotationAssertRoleStateUnchanged(t, "fence-gap committed baseline", preState, postBaseline)

	// Step 3: expose the committed-fence gap. A separately owned connection
	// commits the actual ALTER ROLE restoring the captured P0 verifier; the
	// mutation must succeed and read back exactly, while retained owner health
	// and anchor identity still succeed even though the preserved P1 is gone.
	gapCtx, cancelGap := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	_, gapErr := f.fx.admin.Exec(gapCtx, `ALTER ROLE `+pgx.Identifier{f.writerRole}.Sanitize()+` PASSWORD `+sqlLiteral(preState.verifier))
	cancelGap()
	if gapErr != nil {
		t.Fatalf("fence-gap separately owned committed P0 restoration refused: %v", gapErr)
	}
	afterGapCtx, cancelAfterGap := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	afterGap, afterGapErr := borrowedOwnerRotationReadRoleState(afterGapCtx, f.fx.admin, f.writerRole)
	cancelAfterGap()
	if afterGapErr != nil {
		t.Fatalf("fence-gap post-gap exact catalog read: %v", afterGapErr)
	}
	if subtle.ConstantTimeCompare([]byte(afterGap.verifier), []byte(preState.verifier)) != 1 {
		t.Fatal("fence-gap committed P0 restoration is not the exact captured verifier")
	}
	if subtle.ConstantTimeCompare([]byte(afterGap.verifier), []byte(verifierP1)) == 1 {
		t.Fatal("fence-gap committed value still equals the preserved P1 verifier")
	}
	borrowedOwnerRotationAssertRoleStateUnchanged(t, "fence-gap after committed P0 restoration", preState, afterGap)
	gapHealthCtx, cancelGapHealth := context.WithTimeout(ctx, borrowedOwnerRotationHealthBudget)
	gapHealthErr := f.lock.Health(gapHealthCtx)
	cancelGapHealth()
	if gapHealthErr != nil {
		t.Fatalf("fence-gap retained owner health after the committed P0 restoration: %v", gapHealthErr)
	}
	gapAnchorCtx, cancelGapAnchor := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	gapAnchorErr := anchor.Recheck(gapAnchorCtx)
	cancelGapAnchor()
	if gapAnchorErr != nil {
		t.Fatalf("fence-gap retained anchor identity after the committed P0 restoration: %v", gapAnchorErr)
	}
	t.Logf("committed-fence gap exposed: a separately owned connection committed the exact captured P0 verifier; retained owner health and anchor identity still succeed while the preserved P1 is gone")

	// Step 4: fresh fenced check with genuine entry/anchor rechecks outside the
	// callback; SHARE NOWAIT before any verifier/role read; distinct drift
	// sentinel; no repair and no reusable token.
	entryRecheckCtx, cancelEntryRecheck := context.WithTimeout(ctx, 60*time.Second)
	entryRecheckErr := entry.Recheck(entryRecheckCtx)
	cancelEntryRecheck()
	if entryRecheckErr != nil {
		t.Fatalf("fence-gap genuine entry recheck outside the callback: %v", entryRecheckErr)
	}
	anchorRecheckCtx, cancelAnchorRecheck := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	anchorRecheckErr := anchor.Recheck(anchorRecheckCtx)
	cancelAnchorRecheck()
	if anchorRecheckErr != nil {
		t.Fatalf("fence-gap genuine anchor recheck outside the callback: %v", anchorRecheckErr)
	}
	checkErr := borrowedFenceGapFencedCheck(ctx, f, owner, verifierP1, preState, nil)
	if !errors.Is(checkErr, errBorrowedFenceGapVerifierDrift) {
		t.Fatalf("fence-gap drift was not refused with the specific verifier-drift sentinel: %v", checkErr)
	}
	afterCheckCtx, cancelAfterCheck := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	afterCheck, afterCheckErr := borrowedOwnerRotationReadRoleState(afterCheckCtx, f.fx.admin, f.writerRole)
	cancelAfterCheck()
	if afterCheckErr != nil {
		t.Fatalf("fence-gap post-check catalog read: %v", afterCheckErr)
	}
	if subtle.ConstantTimeCompare([]byte(afterCheck.verifier), []byte(preState.verifier)) != 1 {
		t.Fatal("fence-gap fenced check repaired the committed verifier")
	}
	t.Logf("fenced check refused the drift with the specific verifier-drift sentinel; the committed value is unchanged (no repair, no reusable token)")

	// Negatives on fresh fixtures where invalidation is irreversible.
	borrowedFenceGapStableNegative(t, ctx)
	borrowedFenceGapDriftNegative(t, ctx)
	borrowedFenceGapDriftWithFactsNegative(t, ctx)
	borrowedFenceGapShareHolderNegative(t, ctx)
	borrowedFenceGapCancelNegative(t, ctx)
	borrowedFenceGapOwnerLossNegative(t, ctx)
}
