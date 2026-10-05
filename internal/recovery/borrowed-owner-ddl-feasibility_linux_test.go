//go:build linux && drill

// borrowed-owner-ddl-feasibility_linux_test.go is the bounded FIRST SLICE of
// the same-owner autocommit DROP/CREATE lane. It reuses the genuine successor
// baseline (real chain, consumed receipt entry, captured control anchor,
// client-generated P1 baseline, empty instance identity, deliberate non-clean
// guard) and proves through ONE error-returning preflight -> fenced-check ->
// seam orchestration: pre-DDL entry/anchor rechecks, a complete empty
// target-session inventory with a known successor retired, a fresh fenced
// check that ends before the DDL, the fixed validated same-owner autocommit
// DROP DATABASE (never FORCE) then CREATE DATABASE ... OWNER <original writer>
// through the ORIGINAL dedicated control connection outside any transaction,
// an independently verified whole-catalog pristine W-owned replacement with a
// different OID and the same owner/namespace held, and the refusal of the
// retained old prefix/entry with their never-overwritten old database OID.
//
// Every refusal case goes through the same orchestration and asserts the
// specific gate refusal with zero seam invocations; removing a gate lets the
// seam run and fails those assertions. A confirmed server rejection is a known
// refusal; an unconfirmed DROP completion (transport/deadline/cancel) is
// recorded UNKNOWN and never auto-repaired.
//
// This lane is feasibility ONLY: no restore, no probe, no continuous fence, no
// guard clean, no acceptance, no manifest, no downstream and no Gate1. Short
// caller-derived children, independent bounded cleanup, cancel-and-bounded-join,
// no recursion and unlogged secrets are preserved; every refusal returns only
// an error.
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

	"github.com/xtianxx/txharbor/internal/recovery"
)

// Distinct orchestration gate refusals: every refusal case asserts the exact
// gate that refused and that the seam was never invoked.
var (
	errBorrowedOwnerDDLPreflightRefused = errors.New("owner DDL preflight refused")
	errBorrowedOwnerDDLEntryRefused     = errors.New("owner DDL entry/anchor recheck refused")
	errBorrowedOwnerDDLInventoryRefused = errors.New("owner DDL inventory refused")
	errBorrowedOwnerDDLIdentityRefused  = errors.New("owner DDL identity refused")
	errBorrowedOwnerDDLFenceRefused     = errors.New("owner DDL fenced check refused")
)

// borrowedOwnerDDLTargetOID reads the actual target database OID by name
// through the fixture administrator.
func borrowedOwnerDDLTargetOID(ctx context.Context, b *borrowedSuccessorBaseline) (uint32, error) {
	queryCtx, cancelQuery := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	defer cancelQuery()
	var oid uint32
	if err := b.fixture.fx.admin.QueryRow(queryCtx, `SELECT oid::oid FROM pg_database WHERE datname = $1`, b.fixture.targetDB).Scan(&oid); err != nil {
		return 0, errors.New("owner DDL target database OID read refused")
	}
	if oid == 0 {
		return 0, errors.New("owner DDL target database OID is not positive")
	}
	return oid, nil
}

// borrowedOwnerDDLTargetExists reports whether the target database exists.
func borrowedOwnerDDLTargetExists(ctx context.Context, b *borrowedSuccessorBaseline) (bool, error) {
	queryCtx, cancelQuery := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	defer cancelQuery()
	var exists bool
	if err := b.fixture.fx.admin.QueryRow(queryCtx, `SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)`, b.fixture.targetDB).Scan(&exists); err != nil {
		return false, errors.New("owner DDL target existence read refused")
	}
	return exists, nil
}

// borrowedOwnerDDLTargetSessionCount counts the actual sessions on the target
// database.
func borrowedOwnerDDLTargetSessionCount(ctx context.Context, b *borrowedSuccessorBaseline) (int, error) {
	queryCtx, cancelQuery := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	defer cancelQuery()
	var count int
	if err := b.fixture.fx.admin.QueryRow(queryCtx, `SELECT count(*) FROM pg_stat_activity WHERE datname = $1`, b.fixture.targetDB).Scan(&count); err != nil {
		return 0, errors.New("owner DDL target session inventory is UNKNOWN")
	}
	return count, nil
}

// borrowedOwnerDDLInventory requires a complete target-session inventory with
// zero retained/unknown sessions: any session blocks the DDL.
func borrowedOwnerDDLInventory(ctx context.Context, b *borrowedSuccessorBaseline) error {
	count, err := borrowedOwnerDDLTargetSessionCount(ctx, b)
	if err != nil {
		return err
	}
	if count != 0 {
		return fmt.Errorf("owner DDL target session inventory is not empty (%d retained/unknown sessions)", count)
	}
	return nil
}

// borrowedOwnerDDLWaitTargetSessions waits, bounded, until the target session
// inventory reaches the wanted count.
func borrowedOwnerDDLWaitTargetSessions(ctx context.Context, b *borrowedSuccessorBaseline, want int) {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		count, err := borrowedOwnerDDLTargetSessionCount(ctx, b)
		if err != nil || count == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// borrowedOwnerDDLPreflight is the pre-DDL gate: genuine entry/anchor rechecks,
// a complete empty target-session inventory, and the expected control/target
// database OID comparison against the actual values. Any mismatch refuses with
// its specific gate sentinel before any DDL.
func borrowedOwnerDDLPreflight(ctx context.Context, b *borrowedSuccessorBaseline, expectedControlOID, expectedTargetOID uint32) error {
	recheckCtx, cancelRecheck := context.WithTimeout(ctx, 60*time.Second)
	defer cancelRecheck()
	if err := b.entry.Recheck(recheckCtx); err != nil {
		return fmt.Errorf("%w: entry recheck", errBorrowedOwnerDDLEntryRefused)
	}
	if err := b.anchor.Recheck(recheckCtx); err != nil {
		return fmt.Errorf("%w: anchor recheck", errBorrowedOwnerDDLEntryRefused)
	}
	if err := borrowedOwnerDDLInventory(ctx, b); err != nil {
		return fmt.Errorf("%w: %w", errBorrowedOwnerDDLInventoryRefused, err)
	}
	facts := b.anchor.Diagnostics()
	if !facts.Present || facts.Invalidated || facts.ControlDatabaseOID == 0 {
		return fmt.Errorf("%w: control anchor facts unavailable", errBorrowedOwnerDDLIdentityRefused)
	}
	if facts.ControlDatabaseOID != expectedControlOID {
		return fmt.Errorf("%w: control database OID mismatch", errBorrowedOwnerDDLIdentityRefused)
	}
	targetOID, err := borrowedOwnerDDLTargetOID(ctx, b)
	if err != nil {
		return fmt.Errorf("%w: target database OID unavailable", errBorrowedOwnerDDLIdentityRefused)
	}
	if targetOID != expectedTargetOID {
		return fmt.Errorf("%w: target database OID mismatch", errBorrowedOwnerDDLIdentityRefused)
	}
	return nil
}

// borrowedOwnerDDLOrchestrate is the single lane path used by the positive and
// every refusal case: preflight -> fresh fenced check -> fixed seam.
// invocations counts the seam calls (0 or 1), so removing a gate lets the seam
// run and fails the refusal assertions.
func borrowedOwnerDDLOrchestrate(ctx context.Context, b *borrowedSuccessorBaseline, expectedControlOID, expectedTargetOID uint32, invocations *int32) (recovery.DrillOwnerDDLOutcome, error) {
	if err := borrowedOwnerDDLPreflight(ctx, b, expectedControlOID, expectedTargetOID); err != nil {
		return recovery.DrillOwnerDDLOutcome{}, fmt.Errorf("%w: %w", errBorrowedOwnerDDLPreflightRefused, err)
	}
	if err := borrowedFenceGapFencedCheck(ctx, b.fixture, b.owner, b.verifierP1, b.preState, nil); err != nil {
		return recovery.DrillOwnerDDLOutcome{}, fmt.Errorf("%w: %w", errBorrowedOwnerDDLFenceRefused, err)
	}
	atomic.AddInt32(invocations, 1)
	return b.fixture.run.DrillOwnerTargetDropCreate(ctx)
}

// borrowedOwnerDDLGuard asserts the deliberate non-clean guard and zero
// acceptance after a DDL attempt.
func borrowedOwnerDDLGuard(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline) {
	t.Helper()
	var disposition string
	guardCtx, cancelGuard := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	guardErr := b.fixture.controlPool.QueryRow(guardCtx, `SELECT disposition FROM recovery_target_guard WHERE target_guard_key=$1`, b.fixture.guardKey).Scan(&disposition)
	cancelGuard()
	if guardErr != nil {
		t.Fatalf("owner DDL guard disposition: %v", guardErr)
	}
	if disposition == "clean" {
		t.Fatal("owner DDL lane changed the deliberate non-clean guard")
	}
	if acceptance := atomic.LoadInt32(&b.fixture.acceptanceCalls); acceptance != 0 {
		t.Fatalf("owner DDL lane ran acceptance %d times", acceptance)
	}
}

// borrowedOwnerDDLConnect retains one connection to the supplied DSN with an
// immediate independent bounded cleanup.
func borrowedOwnerDDLConnect(t *testing.T, ctx context.Context, dsn string) *pgx.Conn {
	t.Helper()
	connectCtx, cancelConnect := context.WithTimeout(ctx, borrowedOwnerRotationAuthBudget)
	conn, err := pgx.Connect(connectCtx, dsn)
	cancelConnect()
	if err != nil {
		t.Fatalf("owner DDL connect refused (sqlstate=%s)", borrowedAuthSQLState(err))
	}
	t.Cleanup(func() {
		cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), borrowedOwnerRotationCleanupBudget)
		_ = conn.Close(cleanupCtx)
		cancelCleanup()
	})
	return conn
}

// borrowedOwnerDDLExternalReplace replaces the target database through the
// fixture administrator, changing its OID (used by the stale-target negative).
func borrowedOwnerDDLExternalReplace(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline) {
	t.Helper()
	exec := func(statement string) {
		execCtx, cancelExec := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
		_, err := b.fixture.fx.admin.Exec(execCtx, statement)
		cancelExec()
		if err != nil {
			t.Fatalf("external target replacement refused: %v", err)
		}
	}
	exec(`DROP DATABASE ` + pgx.Identifier{b.fixture.targetDB}.Sanitize())
	exec(`CREATE DATABASE ` + pgx.Identifier{b.fixture.targetDB}.Sanitize() + ` OWNER ` + pgx.Identifier{b.fixture.writerRole}.Sanitize())
}

// borrowedOwnerDDLExecAdmin runs one bounded administrator statement.
func borrowedOwnerDDLExecAdmin(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, statement string) {
	t.Helper()
	execCtx, cancelExec := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	_, err := b.fixture.fx.admin.Exec(execCtx, statement)
	cancelExec()
	if err != nil {
		t.Fatalf("owner DDL administrator statement refused: %v", err)
	}
}

// borrowedOwnerDDLContaminateTemplate contaminates template1 (the source
// template of the fixed CREATE) with the given statements and then waits,
// bounded, until no template1 session remains so the later CREATE is not
// blocked by the contamination connection itself.
func borrowedOwnerDDLContaminateTemplate(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, statements []string) {
	t.Helper()
	routeCtx, cancelRoute := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	templateBase := borrowedIdentityRoute(t, routeCtx, b.fixture.fx, "template1")
	cancelRoute()
	conn := borrowedOwnerDDLConnect(t, ctx, borrowedAuthRoleDSN(t, templateBase, b.fixture.adminRole, b.fixture.adminPassword))
	for _, statement := range statements {
		execCtx, cancelExec := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
		_, err := conn.Exec(execCtx, statement)
		cancelExec()
		if err != nil {
			t.Fatalf("template1 contamination refused: %v", err)
		}
	}
	borrowedSuccessorCloseConn(t, conn)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		queryCtx, cancelQuery := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
		var count int
		queryErr := b.fixture.fx.admin.QueryRow(queryCtx, `SELECT count(*) FROM pg_stat_activity WHERE datname = 'template1'`).Scan(&count)
		cancelQuery()
		if queryErr != nil || count == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// borrowedOwnerDDLPristine is the whole-catalog pristine verification of the
// replacement: the existing whole-catalog predicate (any user relation of any
// relkind in any non-builtin namespace and any non-default schema refuses)
// plus a lane-side non-builtin function/procedure scan, because the base
// predicate alone does not cover functions. A pristine catalog returns its
// digest; anything else refuses.
func borrowedOwnerDDLPristine(ctx context.Context, conn *pgx.Conn, expectedDatabase, expectedOwner string) (string, error) {
	digest, err := verifyBorrowedGatePristineCatalog(ctx, conn, expectedDatabase, expectedOwner)
	if err != nil {
		return "", err
	}
	var functions int
	if err := conn.QueryRow(ctx, `
SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
  AND n.nspname NOT LIKE 'pg_toast%' AND n.nspname NOT LIKE 'pg_temp%'`).Scan(&functions); err != nil {
		return "", errors.New("owner DDL replacement function scan unreadable")
	}
	if functions != 0 {
		return "", errors.New("owner DDL replacement carries non-builtin functions")
	}
	return digest, nil
}

// borrowedOwnerDDLTargetHeldContaminationNegative proves the retained-session
// refusal through the orchestration (zero seam invocations), the seam-level
// no-FORCE DROP refusal with 55006 and no CREATE, and the relations/schema
// contamination of template1 making the replacement non-pristine (fresh
// destructive fixture).
func borrowedOwnerDDLTargetHeldContaminationNegative(t *testing.T, ctx context.Context) {
	t.Helper()
	b := newBorrowedSuccessorBaseline(t, ctx)
	f := b.fixture
	binding := f.run.Binding()
	oldOID := binding.TargetDatabaseOID()
	held := borrowedOwnerDDLConnect(t, ctx, borrowedAuthRoleDSN(t, f.writerTargetDSN, f.writerRole, b.passwordP1))
	// 1. The lane gate refuses the retained session with zero seam invocations.
	var invocations int32
	if _, err := borrowedOwnerDDLOrchestrate(ctx, b, binding.ControlDatabaseOID(), oldOID, &invocations); err == nil ||
		!errors.Is(err, errBorrowedOwnerDDLPreflightRefused) || !errors.Is(err, errBorrowedOwnerDDLInventoryRefused) {
		t.Fatalf("retained target session was not refused by the orchestration inventory: %v", err)
	}
	if got := atomic.LoadInt32(&invocations); got != 0 {
		t.Fatalf("inventory refusal invoked the seam %d times, want 0", got)
	}
	// 2. Seam-level no-FORCE: the direct fixed DROP refuses with 55006 and the
	// CREATE step is never attempted.
	outcome, ddlErr := f.run.DrillOwnerTargetDropCreate(ctx)
	if ddlErr == nil || outcome.Dropped || outcome.Created || outcome.Unknown {
		t.Fatalf("held-target DROP refusal advanced the fixed operation: %+v err=%v", outcome, ddlErr)
	}
	if code := borrowedAuthSQLState(ddlErr); code != "55006" {
		t.Fatalf("held-target DROP refusal SQLSTATE=%s, want 55006", code)
	}
	afterOID, err := borrowedOwnerDDLTargetOID(ctx, b)
	if err != nil || afterOID != oldOID {
		t.Fatalf("held-target negative changed the target identity: oid=%d err=%v", afterOID, err)
	}
	borrowedSuccessorCloseConn(t, held)
	borrowedOwnerDDLWaitTargetSessions(ctx, b, 0)
	// 3. Relations/schema contamination of template1 makes the replacement
	// non-pristine even though the DDL itself commits.
	borrowedOwnerDDLContaminateTemplate(t, ctx, b, []string{
		`CREATE SCHEMA owner_ddl_extra`,
		`CREATE VIEW owner_ddl_extra.owner_ddl_view AS SELECT 1 AS one`,
		`CREATE SEQUENCE owner_ddl_extra.owner_ddl_seq`,
	})
	invocations = 0
	outcome, ddlErr = borrowedOwnerDDLOrchestrate(ctx, b, binding.ControlDatabaseOID(), oldOID, &invocations)
	if ddlErr != nil {
		t.Fatalf("contaminated-template DDL refused unexpectedly: %v", ddlErr)
	}
	if !outcome.Dropped || !outcome.Created {
		t.Fatalf("contaminated-template DDL outcome is not a complete DDL: %+v", outcome)
	}
	if got := atomic.LoadInt32(&invocations); got != 1 {
		t.Fatalf("contaminated-template DDL seam invocations=%d, want 1", got)
	}
	p1Conn := borrowedOwnerDDLConnect(t, ctx, borrowedAuthRoleDSN(t, f.writerTargetDSN, f.writerRole, b.passwordP1))
	pristineCtx, cancelPristine := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	digest, pristineErr := borrowedOwnerDDLPristine(pristineCtx, p1Conn, f.targetDB, f.writerRole)
	cancelPristine()
	if pristineErr == nil || digest != "" {
		t.Fatalf("relations-contaminated replacement was reported pristine: digest=%q err=%v", digest, pristineErr)
	}
	borrowedSuccessorCloseConn(t, p1Conn)
	borrowedOwnerDDLGuard(t, ctx, b)
	t.Logf("held/contamination negative: the retained session was refused pre-DDL with zero seam invocations; the seam's no-FORCE DROP refused with 55006 and no CREATE; the relations/schema-contaminated replacement was non-pristine")
}

// borrowedOwnerDDLRenameWrongIdentityNegative proves a renamed target identity
// (OID/owner preserved) is refused by the seam pre-DDL with no DROP, and that
// a wrong control OID and a real stale target OID are refused by the
// orchestration preflight with zero seam invocations (fresh destructive
// fixture).
func borrowedOwnerDDLRenameWrongIdentityNegative(t *testing.T, ctx context.Context) {
	t.Helper()
	b := newBorrowedSuccessorBaseline(t, ctx)
	f := b.fixture
	binding := f.run.Binding()
	oldOID := binding.TargetDatabaseOID()
	renamed := f.targetDB + "_renamed"
	borrowedOwnerDDLExecAdmin(t, ctx, b, `ALTER DATABASE `+pgx.Identifier{f.targetDB}.Sanitize()+` RENAME TO `+pgx.Identifier{renamed}.Sanitize())
	outcome, renameErr := f.run.DrillOwnerTargetDropCreate(ctx)
	if renameErr == nil || outcome.Dropped || outcome.Created || outcome.Unknown {
		t.Fatalf("renamed target was not refused pre-DDL: outcome=%+v err=%v", outcome, renameErr)
	}
	if !strings.Contains(renameErr.Error(), "renamed-identity") {
		t.Fatalf("rename refusal did not report the renamed identity: %v", renameErr)
	}
	var renamedOID uint32
	queryCtx, cancelQuery := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	queryErr := f.fx.admin.QueryRow(queryCtx, `SELECT oid::oid FROM pg_database WHERE datname = $1`, renamed).Scan(&renamedOID)
	cancelQuery()
	if queryErr != nil || renamedOID != oldOID {
		t.Fatalf("renamed target OID/identity changed: oid=%d err=%v", renamedOID, queryErr)
	}
	borrowedOwnerDDLExecAdmin(t, ctx, b, `ALTER DATABASE `+pgx.Identifier{renamed}.Sanitize()+` RENAME TO `+pgx.Identifier{f.targetDB}.Sanitize())
	// Wrong control-database OID through the orchestration: preflight identity
	// refusal with zero seam invocations.
	var invocations int32
	if _, err := borrowedOwnerDDLOrchestrate(ctx, b, oldOID, oldOID, &invocations); err == nil ||
		!errors.Is(err, errBorrowedOwnerDDLPreflightRefused) || !errors.Is(err, errBorrowedOwnerDDLIdentityRefused) {
		t.Fatalf("wrong control database OID was not refused by the orchestration preflight: %v", err)
	}
	if got := atomic.LoadInt32(&invocations); got != 0 {
		t.Fatalf("wrong-control refusal invoked the seam %d times, want 0", got)
	}
	// Real stale target OID: an external replacement changes the OID; the
	// orchestration refuses pre-DDL with zero seam invocations and no DDL.
	borrowedOwnerDDLExternalReplace(t, ctx, b)
	replacedOID, err := borrowedOwnerDDLTargetOID(ctx, b)
	if err != nil || replacedOID == oldOID {
		t.Fatalf("external target replacement did not change the OID: oid=%d err=%v", replacedOID, err)
	}
	invocations = 0
	if _, err := borrowedOwnerDDLOrchestrate(ctx, b, binding.ControlDatabaseOID(), oldOID, &invocations); err == nil ||
		!errors.Is(err, errBorrowedOwnerDDLPreflightRefused) {
		t.Fatalf("stale target database OID was not refused by the orchestration preflight: %v", err)
	}
	if got := atomic.LoadInt32(&invocations); got != 0 {
		t.Fatalf("stale-target refusal invoked the seam %d times, want 0", got)
	}
	stillOID, err := borrowedOwnerDDLTargetOID(ctx, b)
	if err != nil || stillOID != replacedOID {
		t.Fatalf("wrong-identity negative performed DDL: oid=%d err=%v", stillOID, err)
	}
	borrowedOwnerDDLGuard(t, ctx, b)
	t.Logf("rename/wrong-identity negative: the renamed target was refused by the seam pre-DDL with the OID preserved and no DROP; the wrong control OID and the real stale target OID were refused by the orchestration preflight with zero seam invocations")
}

// borrowedOwnerDDLCancelOwnerLossNegative proves an ended caller context and a
// real control owner loss are both refused by the orchestration preflight with
// zero seam invocations, no DDL, no replacement owner and no retry (fresh
// destructive fixture).
func borrowedOwnerDDLCancelOwnerLossNegative(t *testing.T, ctx context.Context) {
	t.Helper()
	b := newBorrowedSuccessorBaseline(t, ctx)
	f := b.fixture
	binding := f.run.Binding()
	oldOID := binding.TargetDatabaseOID()
	cancelCtx, cancelCall := context.WithCancel(ctx)
	cancelCall()
	var invocations int32
	if _, err := borrowedOwnerDDLOrchestrate(cancelCtx, b, binding.ControlDatabaseOID(), oldOID, &invocations); err == nil ||
		!errors.Is(err, errBorrowedOwnerDDLPreflightRefused) || !errors.Is(err, errBorrowedOwnerDDLEntryRefused) {
		t.Fatalf("caller cancel was not refused by the orchestration preflight: %v", err)
	}
	if got := atomic.LoadInt32(&invocations); got != 0 {
		t.Fatalf("caller cancel invoked the seam %d times, want 0", got)
	}
	controlPID := binding.ControlBackendPID()
	var terminated bool
	lossCtx, cancelLoss := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	lossErr := f.controlPool.QueryRow(lossCtx, `SELECT pg_terminate_backend($1)`, controlPID).Scan(&terminated)
	cancelLoss()
	if lossErr != nil || !terminated {
		t.Fatalf("terminate owner DDL control owner: terminated=%t err=%v", terminated, lossErr)
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
	invocations = 0
	if _, err := borrowedOwnerDDLOrchestrate(ctx, b, binding.ControlDatabaseOID(), oldOID, &invocations); err == nil ||
		!errors.Is(err, errBorrowedOwnerDDLPreflightRefused) || !errors.Is(err, errBorrowedOwnerDDLEntryRefused) {
		t.Fatalf("real owner loss was not refused by the orchestration preflight: %v", err)
	}
	if got := atomic.LoadInt32(&invocations); got != 0 {
		t.Fatalf("owner loss invoked the seam %d times, want 0", got)
	}
	healthCtx, cancelHealth := context.WithTimeout(ctx, borrowedOwnerRotationHealthBudget)
	healthErr := f.lock.Health(healthCtx)
	cancelHealth()
	if healthErr == nil {
		t.Fatal("owner DDL control owner health remained successful after real owner loss")
	}
	afterOID, err := borrowedOwnerDDLTargetOID(ctx, b)
	if err != nil || afterOID != oldOID {
		t.Fatalf("cancel/owner-loss negative performed DDL: oid=%d err=%v", afterOID, err)
	}
	t.Logf("cancel/owner-loss negative: an ended caller context and real owner loss were each refused by the orchestration preflight with zero seam invocations, no DDL, no replacement owner and no retry")
}

// borrowedOwnerDDLDriftInventoryNegative proves committed verifier drift is
// refused by the orchestration fenced check (specific drift classification)
// and a retained foreign target session is refused by the orchestration
// inventory, both with zero seam invocations and no DDL (fresh destructive
// fixture).
func borrowedOwnerDDLDriftInventoryNegative(t *testing.T, ctx context.Context) {
	t.Helper()
	b := newBorrowedSuccessorBaseline(t, ctx)
	f := b.fixture
	binding := f.run.Binding()
	oldOID := binding.TargetDatabaseOID()
	driftCtx, cancelDrift := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	_, driftErr := f.fx.admin.Exec(driftCtx, `ALTER ROLE `+pgx.Identifier{f.writerRole}.Sanitize()+` PASSWORD `+sqlLiteral(b.preState.verifier))
	cancelDrift()
	if driftErr != nil {
		t.Fatalf("owner DDL drift mutation refused: %v", driftErr)
	}
	var invocations int32
	if _, err := borrowedOwnerDDLOrchestrate(ctx, b, binding.ControlDatabaseOID(), oldOID, &invocations); err == nil ||
		!errors.Is(err, errBorrowedOwnerDDLFenceRefused) || !errors.Is(err, errBorrowedFenceGapVerifierDrift) {
		t.Fatalf("committed verifier drift was not refused by the orchestration fenced check: %v", err)
	}
	if got := atomic.LoadInt32(&invocations); got != 0 {
		t.Fatalf("drift refusal invoked the seam %d times, want 0", got)
	}
	routeCtx, cancelRoute := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	foreignBase := borrowedIdentityRoute(t, routeCtx, f.fx, f.targetDB)
	cancelRoute()
	foreign := borrowedOwnerDDLConnect(t, ctx, borrowedAuthRoleDSN(t, foreignBase, f.adminRole, f.adminPassword))
	invocations = 0
	if _, err := borrowedOwnerDDLOrchestrate(ctx, b, binding.ControlDatabaseOID(), oldOID, &invocations); err == nil ||
		!errors.Is(err, errBorrowedOwnerDDLPreflightRefused) || !errors.Is(err, errBorrowedOwnerDDLInventoryRefused) {
		t.Fatalf("retained foreign target session was not refused by the orchestration inventory: %v", err)
	}
	if got := atomic.LoadInt32(&invocations); got != 0 {
		t.Fatalf("foreign-inventory refusal invoked the seam %d times, want 0", got)
	}
	afterOID, err := borrowedOwnerDDLTargetOID(ctx, b)
	if err != nil || afterOID != oldOID {
		t.Fatalf("drift/inventory negative performed DDL: oid=%d err=%v", afterOID, err)
	}
	borrowedSuccessorCloseConn(t, foreign)
	t.Logf("drift/inventory negative: committed verifier drift was refused by the fenced check with the drift classification and a retained foreign target session by the inventory, both with zero seam invocations and no DDL")
}

// borrowedOwnerDDLPostDropFailureNegative proves a failure after the committed
// DROP leaves the target absent with a partial/UNKNOWN outcome, a non-clean
// guard and no auto-repair through the orchestration (fresh destructive
// fixture).
func borrowedOwnerDDLPostDropFailureNegative(t *testing.T, ctx context.Context) {
	t.Helper()
	b := newBorrowedSuccessorBaseline(t, ctx)
	f := b.fixture
	binding := f.run.Binding()
	oldOID := binding.TargetDatabaseOID()
	templateCtx, cancelTemplate := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	templateBase := borrowedIdentityRoute(t, templateCtx, f.fx, "template1")
	cancelTemplate()
	templateConn := borrowedOwnerDDLConnect(t, ctx, borrowedAuthRoleDSN(t, templateBase, f.controlRole, f.controlPass))
	var invocations int32
	outcome, ddlErr := borrowedOwnerDDLOrchestrate(ctx, b, binding.ControlDatabaseOID(), oldOID, &invocations)
	if ddlErr == nil {
		t.Fatal("owner DDL succeeded while template1 was held")
	}
	if !outcome.Dropped || outcome.Created || !outcome.Unknown {
		t.Fatalf("post-DROP failure outcome is not partial/UNKNOWN: %+v", outcome)
	}
	if got := atomic.LoadInt32(&invocations); got != 1 {
		t.Fatalf("post-DROP failure seam invocations=%d, want 1", got)
	}
	exists, err := borrowedOwnerDDLTargetExists(ctx, b)
	if err != nil || exists {
		t.Fatalf("target database is not absent after the post-DROP failure: exists=%t err=%v", exists, err)
	}
	borrowedOwnerDDLGuard(t, ctx, b)
	borrowedSuccessorCloseConn(t, templateConn)
	existsAgain, err := borrowedOwnerDDLTargetExists(ctx, b)
	if err != nil || existsAgain {
		t.Fatalf("owner DDL auto-repaired after the post-DROP failure: exists=%t err=%v", existsAgain, err)
	}
	t.Logf("post-DROP failure negative: CREATE failed after the committed DROP, target absent, partial/UNKNOWN outcome recorded, guard non-clean, no auto-repair")
}

// borrowedOwnerDDLUnconfirmedDropNegative proves a cancellation at the
// pre-DROP pause yields an unconfirmed DROP completion (Unknown=true,
// Dropped=false, no SQLSTATE) with no CREATE replacement (fresh destructive
// fixture).
func borrowedOwnerDDLUnconfirmedDropNegative(t *testing.T, ctx context.Context) {
	t.Helper()
	b := newBorrowedSuccessorBaseline(t, ctx)
	f := b.fixture
	binding := f.run.Binding()
	oldOID := binding.TargetDatabaseOID()
	entered := make(chan struct{})
	release := make(chan struct{})
	var enteredOnce, releaseOnce sync.Once
	reset := recovery.DrillOwnerDDLTestHook(func(stage string) {
		if stage != "before-drop" {
			return
		}
		enteredOnce.Do(func() { close(entered) })
		<-release
	})
	defer reset()
	var invocations int32
	var outcome recovery.DrillOwnerDDLOutcome
	useCtx, cancelUse := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		var err error
		outcome, err = borrowedOwnerDDLOrchestrate(useCtx, b, binding.ControlDatabaseOID(), oldOID, &invocations)
		done <- err
	}()
	joined := false
	t.Cleanup(func() {
		cancelUse()
		releaseOnce.Do(func() { close(release) })
		if joined {
			return
		}
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Errorf("unconfirmed-drop orchestration completion is unknown after the bounded join")
		}
	})
	select {
	case <-entered:
	case <-time.After(60 * time.Second):
		t.Errorf("owner DDL never reached the before-drop pause; completion is unknown")
		return
	}
	cancelUse()
	releaseOnce.Do(func() { close(release) })
	var ddlErr error
	select {
	case ddlErr = <-done:
		joined = true
	case <-time.After(30 * time.Second):
		t.Errorf("unconfirmed-drop orchestration did not complete within the bounded join; outcome unknown")
		return
	}
	if ddlErr == nil {
		t.Fatal("unconfirmed DROP was accepted")
	}
	if !outcome.Unknown || outcome.Dropped || outcome.Created {
		t.Fatalf("cancellation was not an unconfirmed DROP: %+v", outcome)
	}
	if code := borrowedAuthSQLState(ddlErr); code != "no-sqlstate" {
		t.Fatalf("unconfirmed DROP carried a server SQLSTATE=%s", code)
	}
	if got := atomic.LoadInt32(&invocations); got != 1 {
		t.Fatalf("unconfirmed-drop seam invocations=%d, want 1", got)
	}
	exists, err := borrowedOwnerDDLTargetExists(ctx, b)
	if err != nil {
		t.Fatalf("unconfirmed-drop target existence: %v", err)
	}
	if exists {
		oid, err := borrowedOwnerDDLTargetOID(ctx, b)
		if err != nil || oid != oldOID {
			t.Fatalf("present target is not the original OID: oid=%d err=%v", oid, err)
		}
	}
	borrowedOwnerDDLGuard(t, ctx, b)
	t.Logf("unconfirmed-DROP negative: cancellation at the pre-DROP pause produced an unconfirmed DROP (Unknown=true, Dropped=false, no SQLSTATE) and no CREATE replacement exists")
}

// borrowedOwnerDDLFunctionContaminationNegative proves a function-only
// contamination of template1 makes the replacement non-pristine through the
// lane-side function scan, while the base whole-catalog predicate alone does
// not cover functions (fresh destructive fixture).
func borrowedOwnerDDLFunctionContaminationNegative(t *testing.T, ctx context.Context) {
	t.Helper()
	b := newBorrowedSuccessorBaseline(t, ctx)
	f := b.fixture
	binding := f.run.Binding()
	oldOID := binding.TargetDatabaseOID()
	borrowedOwnerDDLContaminateTemplate(t, ctx, b, []string{
		`CREATE FUNCTION public.owner_ddl_probe() RETURNS integer LANGUAGE sql AS 'SELECT 1'`,
	})
	var invocations int32
	outcome, ddlErr := borrowedOwnerDDLOrchestrate(ctx, b, binding.ControlDatabaseOID(), oldOID, &invocations)
	if ddlErr != nil {
		t.Fatalf("function-contaminated template DDL refused unexpectedly: %v", ddlErr)
	}
	if !outcome.Dropped || !outcome.Created {
		t.Fatalf("function-contaminated template DDL outcome is not a complete DDL: %+v", outcome)
	}
	if got := atomic.LoadInt32(&invocations); got != 1 {
		t.Fatalf("function-contamination seam invocations=%d, want 1", got)
	}
	p1Conn := borrowedOwnerDDLConnect(t, ctx, borrowedAuthRoleDSN(t, f.writerTargetDSN, f.writerRole, b.passwordP1))
	baseCtx, cancelBase := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	baseDigest, baseErr := verifyBorrowedGatePristineCatalog(baseCtx, p1Conn, f.targetDB, f.writerRole)
	cancelBase()
	if baseErr != nil || baseDigest == "" {
		t.Fatalf("base whole-catalog predicate unexpectedly refused a function-only contamination: digest=%q err=%v", baseDigest, baseErr)
	}
	laneCtx, cancelLane := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	laneDigest, laneErr := borrowedOwnerDDLPristine(laneCtx, p1Conn, f.targetDB, f.writerRole)
	cancelLane()
	if laneErr == nil || laneDigest != "" {
		t.Fatalf("function-contaminated replacement was reported pristine: digest=%q err=%v", laneDigest, laneErr)
	}
	borrowedSuccessorCloseConn(t, p1Conn)
	borrowedOwnerDDLGuard(t, ctx, b)
	t.Logf("function-contamination negative: the base whole-catalog predicate alone accepted a function-only contamination; the lane-side function scan refused the replacement as non-pristine")
}

// TestBorrowedOwnerDDLFeasibility is the bounded same-owner autocommit
// DROP/CREATE first slice described in the file header. It grants no
// continuous fence, restore, probe, guard-clean, acceptance, manifest,
// downstream or Gate1 authority; a partial/UNKNOWN outcome is never
// auto-repaired.
func TestBorrowedOwnerDDLFeasibility(t *testing.T) {
	ctx := t.Context()
	b := newBorrowedSuccessorBaseline(t, ctx)
	f := b.fixture
	binding := f.run.Binding()
	oldOID := binding.TargetDatabaseOID()

	// Pre-DDL: retire one known successor, then run the single orchestration
	// (preflight -> fenced check -> seam).
	successorConn, successorReg := borrowedSuccessorConnectP1(t, ctx, b)
	borrowedSuccessorRetire(t, ctx, b, successorConn, successorReg.state)
	var invocations int32
	outcome, ddlErr := borrowedOwnerDDLOrchestrate(ctx, b, binding.ControlDatabaseOID(), oldOID, &invocations)
	if ddlErr != nil {
		t.Fatalf("owner DDL orchestration refused: %v", ddlErr)
	}
	if got := atomic.LoadInt32(&invocations); got != 1 {
		t.Fatalf("owner DDL seam invocations=%d, want 1", got)
	}
	if !outcome.Dropped || !outcome.Created || outcome.Unknown {
		t.Fatalf("owner DDL outcome is not a complete success: %+v", outcome)
	}
	if outcome.OldTargetOID != oldOID || outcome.NewTargetOID == 0 || outcome.NewTargetOID == oldOID {
		t.Fatalf("owner DDL OIDs are not old=%d -> different new: %+v", oldOID, outcome)
	}

	// Independent replacement verification: exists, W-owned, different OID,
	// whole-catalog pristine (including the lane-side function scan) and the
	// same owner/namespace held.
	replacedOID, err := borrowedOwnerDDLTargetOID(ctx, b)
	if err != nil || replacedOID != outcome.NewTargetOID {
		t.Fatalf("replacement target OID mismatch: oid=%d want=%d err=%v", replacedOID, outcome.NewTargetOID, err)
	}
	var ownerOID uint32
	ownerCtx, cancelOwner := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	ownerErr := f.fx.admin.QueryRow(ownerCtx, `SELECT datdba::oid FROM pg_database WHERE oid = $1`, replacedOID).Scan(&ownerOID)
	cancelOwner()
	if ownerErr != nil || ownerOID != binding.WriterRoleOID() {
		t.Fatalf("replacement database is not W-owned: owner=%d want=%d err=%v", ownerOID, binding.WriterRoleOID(), ownerErr)
	}
	p1Conn := borrowedOwnerDDLConnect(t, ctx, borrowedAuthRoleDSN(t, f.writerTargetDSN, f.writerRole, b.passwordP1))
	pristineCtx, cancelPristine := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	digest, pristineErr := borrowedOwnerDDLPristine(pristineCtx, p1Conn, f.targetDB, f.writerRole)
	cancelPristine()
	if pristineErr != nil || digest == "" {
		t.Fatalf("replacement target is not whole-catalog pristine: digest=%q err=%v", digest, pristineErr)
	}
	borrowedSuccessorCloseConn(t, p1Conn)
	anchorCtx, cancelAnchor := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	anchorErr := b.anchor.Recheck(anchorCtx)
	cancelAnchor()
	if anchorErr != nil {
		t.Fatalf("owner DDL post replacement anchor recheck: %v", anchorErr)
	}
	healthCtx, cancelHealth := context.WithTimeout(ctx, borrowedOwnerRotationHealthBudget)
	healthErr := f.lock.Health(healthCtx)
	cancelHealth()
	if healthErr != nil {
		t.Fatalf("owner DDL post replacement owner health: %v", healthErr)
	}
	borrowedOwnerDDLGuard(t, ctx, b)

	// Retained old prefix/entry refuse with their never-overwritten old
	// database OID: no OID overwrite and no checkpoint reconstruction.
	if f.prefix.capturedTargetDBOID != oldOID {
		t.Fatal("owner DDL overwrote the retained prefix target database OID")
	}
	prefixCtx, cancelPrefix := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	prefixErr := f.prefix.Recheck(t, prefixCtx)
	cancelPrefix()
	if prefixErr == nil {
		t.Fatal("retained old prefix recheck succeeded after the target replacement")
	}
	entryCtx, cancelEntry := context.WithTimeout(ctx, 60*time.Second)
	entryErr := b.entry.Recheck(entryCtx)
	cancelEntry()
	if entryErr == nil {
		t.Fatal("retained old entry recheck succeeded after the target replacement")
	}
	_, reason := b.entry.stageNow()
	if reason == "" {
		t.Fatal("retained old entry was not permanently invalidated after the target replacement")
	}
	t.Logf("owner DDL feasibility: DROP/CREATE committed old OID %d -> new OID %d with a whole-catalog pristine W-owned replacement; old prefix/entry refused with the retained old OID", oldOID, replacedOID)

	// Negatives on fresh fixtures where destructive.
	borrowedOwnerDDLTargetHeldContaminationNegative(t, ctx)
	borrowedOwnerDDLRenameWrongIdentityNegative(t, ctx)
	borrowedOwnerDDLCancelOwnerLossNegative(t, ctx)
	borrowedOwnerDDLDriftInventoryNegative(t, ctx)
	borrowedOwnerDDLPostDropFailureNegative(t, ctx)
	borrowedOwnerDDLUnconfirmedDropNegative(t, ctx)
	borrowedOwnerDDLFunctionContaminationNegative(t, ctx)
}
