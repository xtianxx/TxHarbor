//go:build linux && drill

// borrowed-replacement-binding-feasibility_linux_test.go is the bounded
// replacement-bound identity capture lane. After the actual same-owner
// autocommit DROP/CREATE replacement, it captures a FRESH borrowed transport
// binding on the SAME retained lock (fresh endpoint, sealed tools, unchanged
// logical target/control identities, local P1 credentials) and a FRESH OS
// prefix through the actual identity capture path, and proves that the fresh
// binding is bound to the independently verified replacement (different OID,
// W-owned, whole-catalog pristine, same retained owner) while the old
// prefix/entry and the old successor construction are permanently refused.
//
// The single error-returning orchestration owns the actual DDL call and the
// fresh captures; it never accepts a DDL outcome, OID, JSON projection or
// boolean as a substitute, never calls the original Run (the original run
// stays reserved) and never launches a child or executes a probe. Copies of
// the private capture share the underlying run/prefix state, so a permanent
// invalidation is shared. This lane grants no restore, probe, receipt,
// observation, guard-clean, acceptance, manifest, downstream or Gate1
// authority; short caller-derived children, independent 10s cleanup,
// cancel-and-bounded-join, no recursion and unlogged secrets are preserved.
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
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

// borrowedReplacementFactoryBudget is the explicit short caller-derived budget
// of the fresh transport factory call; a sooner caller deadline still clips it.
const borrowedReplacementFactoryBudget = 30 * time.Second

// borrowedReplacementOrchestrationBudget bounds the caller context of every
// replacement-binding orchestration call; a sooner caller deadline still clips
// it and the timeout path is a plain refusal, never a stranded wait.
const borrowedReplacementOrchestrationBudget = 180 * time.Second

// borrowedReplacementTriggerWindow bounds the late-cancel trigger wait for the
// publication pause; expiry exits through the same cancel-and-release path as
// the pause itself.
const borrowedReplacementTriggerWindow = 120 * time.Second

// borrowedReplacementJoinBudget bounds every join of the trigger/control
// goroutines; an unbounded receive is never used.
const borrowedReplacementJoinBudget = 30 * time.Second

// borrowedReplacementPublishHook is the test-only pause seam consulted at the
// final publication decision of the replacement-binding orchestration, after
// the fresh prefix recheck and before the caller-context check. The hook always
// selects the caller context, so a canceled pipeline never strands it. It is
// read atomically and is nil in normal runs.
var borrowedReplacementPublishHook atomic.Pointer[func(context.Context, *borrowedIdentityPrefix)]

func borrowedReplacementPauseAtPublish(ctx context.Context, prefix *borrowedIdentityPrefix) {
	if hook := borrowedReplacementPublishHook.Load(); hook != nil {
		(*hook)(ctx, prefix)
	}
}

// borrowedReplacementStageHook is the test-only pause seam consulted before the
// first orchestration stage; the deterministic timeout-before-pause control
// holds the pipeline here while its trigger window expires. The hook selects
// the pipeline context. It is read atomically and is nil in normal runs.
var borrowedReplacementStageHook atomic.Pointer[func(context.Context)]

func borrowedReplacementPauseAtStage(ctx context.Context) {
	if hook := borrowedReplacementStageHook.Load(); hook != nil {
		(*hook)(ctx)
	}
}

// borrowedReplacementOrchestrationContext bounds every orchestration caller
// context with the shared budget.
func borrowedReplacementOrchestrationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, borrowedReplacementOrchestrationBudget)
}

// borrowedReplacementP1TargetDSN derives the private P1 DSN from the local
// baseline password using the shared role-DSN pattern. The original fixture
// DSNs are never rewritten.
func borrowedReplacementP1TargetDSN(t *testing.T, b *borrowedSuccessorBaseline) string {
	t.Helper()
	return borrowedAuthRoleDSN(t, b.fixture.writerTargetDSN, b.fixture.writerRole, b.passwordP1)
}

// borrowedReplacementVerifyP1Auth proves the derived P1 DSN authenticates
// without calling Run: a bounded connect, one fixed SELECT 1 and a bounded
// close.
func borrowedReplacementVerifyP1Auth(ctx context.Context, dsn string) error {
	authCtx, cancelAuth := context.WithTimeout(ctx, borrowedOwnerRotationAuthBudget)
	conn, err := pgx.Connect(authCtx, dsn)
	cancelAuth()
	if err != nil {
		return errors.New("replacement binding local P1 credential authentication refused")
	}
	queryCtx, cancelQuery := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	var one int
	queryErr := conn.QueryRow(queryCtx, `SELECT 1`).Scan(&one)
	cancelQuery()
	closeCtx, cancelClose := context.WithTimeout(context.Background(), borrowedOwnerRotationCleanupBudget)
	closeErr := conn.Close(closeCtx)
	cancelClose()
	if queryErr != nil || one != 1 || closeErr != nil {
		return errors.New("replacement binding local P1 credential probe refused")
	}
	return nil
}

// Pipeline stages of the single replacement-binding orchestration. Deleting a
// prerequisite stops the pipeline and fails the stage-count assertions of the
// refusal negatives.
const (
	borrowedReplacementStageSuccessorRetired int32 = iota + 1
	borrowedReplacementStageDDLCommitted
	borrowedReplacementStageReplacementVerified
	borrowedReplacementStageTransportCaptured
	borrowedReplacementStagePrefixCaptured
)

// borrowedReplacementBinding is the private replacement-bound identity
// capture: the fresh run, the fresh OS prefix and the independently verified
// replacement identity. Copies share the underlying run/prefix/state pointers,
// so a permanent prefix invalidation is shared with every copy.
type borrowedReplacementBinding struct {
	fixture        *borrowedAuthHandoffFixture
	entry          *borrowedReceiptAuthEntry
	anchor         recovery.DrillControlAnchor
	run            *recovery.DrillBorrowedWriterRun
	prefix         *borrowedIdentityPrefix
	oldOID         uint32
	replacementOID uint32
	binding        recovery.DrillBorrowedControlBinding
}

// borrowedReplacementFreshRunWith captures a fresh borrowed transport binding
// through the actual factory with the SAME retained lock, the supplied
// observer DSN, the exact local P1 target DSN and a fresh actual loopback
// endpoint. The factory call runs on an explicit short caller-derived budget.
// It never launches a child and never calls Run.
func borrowedReplacementFreshRunWith(ctx context.Context, t *testing.T, f *borrowedAuthHandoffFixture, observerDSN, targetDSN string) (*recovery.DrillBorrowedWriterRun, error) {
	t.Helper()
	if f == nil || observerDSN == "" || targetDSN == "" {
		return nil, errors.New("replacement binding fresh transport requires the concrete fixture, observer identity and local P1 target DSN")
	}
	targetTarget, err := controlstore.ParseDSNTarget(targetDSN)
	if err != nil {
		return nil, errors.New("replacement binding trusted target identity is invalid")
	}
	endpoint, err := recovery.DrillOpenOriginEndpoint()
	if err != nil {
		return nil, errors.New("replacement binding fresh endpoint refused")
	}
	t.Cleanup(func() { _ = endpoint.Listener().Close() })
	factoryCtx, cancelFactory := context.WithTimeout(ctx, borrowedReplacementFactoryBudget)
	defer cancelFactory()
	writerOptions := recovery.TargetWriterOptions{
		OperationKind: recovery.TargetWriterOperationRestore,
		ControlDSN:    f.controlDSN,
		TargetDSN:     targetDSN,
		ObserverDSN:   observerDSN,
		TrustedTarget: targetTarget,
		OperationID:   f.operation,
	}
	// The REAL bound fixture instance id is carried through the fresh factory
	// binding only when the fixture owns an authentic first-open instance; an
	// unbound fixture keeps the exact empty identity and no caller-supplied id
	// is ever accepted.
	if f.bound && f.instanceID != "" {
		writerOptions.InstanceID = f.instanceID
	}
	run, err := recovery.DrillNewBorrowedWriterRun(factoryCtx, writerOptions, f.lock, endpoint, f.tools)
	if err != nil {
		return nil, fmt.Errorf("replacement binding fresh transport refused: %w", err)
	}
	return run, nil
}

// borrowedReplacementCompareBindings requires the fresh binding to retain the
// original owner/control/cluster identity exactly and to be bound to the
// independently verified replacement target OID.
func borrowedReplacementCompareBindings(original, fresh recovery.DrillBorrowedControlBinding, replacementOID uint32) error {
	if !fresh.SQLFactsComplete() {
		return errors.New("replacement binding fresh SQL facts are incomplete")
	}
	if fresh.ControlBackendPID() != original.ControlBackendPID() || !fresh.ControlBackendStart().Equal(original.ControlBackendStart()) {
		return errors.New("replacement binding control owner identity changed")
	}
	if fresh.ControlDatabaseOID() != original.ControlDatabaseOID() ||
		fresh.ControlPostmasterStart() != original.ControlPostmasterStart() ||
		fresh.ControlTargetKey() != original.ControlTargetKey() {
		return errors.New("replacement binding control session identity changed")
	}
	if fresh.OriginalTargetKey() != original.OriginalTargetKey() ||
		fresh.OriginalRoleFingerprint() != original.OriginalRoleFingerprint() ||
		fresh.OriginalOperationID() != original.OriginalOperationID() ||
		fresh.OriginalInstanceID() != original.OriginalInstanceID() {
		return errors.New("replacement binding logical target identity changed")
	}
	if fresh.ClusterSystemIdentifier() != original.ClusterSystemIdentifier() ||
		fresh.PostmasterStartTime() != original.PostmasterStartTime() ||
		fresh.WriterRoleOID() != original.WriterRoleOID() {
		return errors.New("replacement binding cluster/role identity changed")
	}
	if replacementOID == 0 || fresh.TargetDatabaseOID() != replacementOID {
		return errors.New("replacement binding target database OID is not the verified replacement")
	}
	return nil
}

// borrowedReplacementBindingOrchestrate is the single lane path: retire the
// known successor, run the actual shared DDL orchestration, independently
// verify the replacement, then capture the fresh transport binding and the
// fresh OS prefix. It owns the actual DDL call and the fresh captures and
// never accepts an outcome/OID/JSON/boolean substitute. stages counts the
// completed stages.
func borrowedReplacementBindingOrchestrate(ctx context.Context, t *testing.T, b *borrowedSuccessorBaseline, stages *int32) (*borrowedReplacementBinding, error) {
	t.Helper()
	f := b.fixture
	oldOID := f.run.Binding().TargetDatabaseOID()
	// Test-only pre-stage pause seam (nil in normal runs): it selects the
	// pipeline context, so a canceled caller never strands the pipeline and the
	// deterministic timeout-before-pause control can hold the pipeline while
	// its trigger window expires.
	borrowedReplacementPauseAtStage(ctx)
	if err := ctx.Err(); err != nil {
		return nil, errors.New("replacement binding orchestration context ended before the successor retire")
	}
	// Stage 1: retire the known successor exactly as the DDL positive does.
	successorConn, successorReg := borrowedSuccessorConnectP1(t, ctx, b)
	borrowedSuccessorRetire(t, ctx, b, successorConn, successorReg.state)
	atomic.AddInt32(stages, 1)
	// Stage 2: the actual DDL through the shared orchestration.
	var invocations int32
	outcome, ddlErr := borrowedOwnerDDLOrchestrate(ctx, b, f.run.Binding().ControlDatabaseOID(), oldOID, &invocations)
	if ddlErr != nil {
		return nil, fmt.Errorf("replacement binding DDL refused: %w", ddlErr)
	}
	if !outcome.Dropped || !outcome.Created || outcome.Unknown {
		return nil, fmt.Errorf("replacement binding DDL outcome is not a complete success: %+v", outcome)
	}
	if got := atomic.LoadInt32(&invocations); got != 1 {
		return nil, fmt.Errorf("replacement binding DDL seam invocations=%d, want 1", got)
	}
	atomic.AddInt32(stages, 1)
	// Stage 3: independent replacement verification: different OID, W-owned,
	// whole-catalog pristine and the retained owner/namespace held.
	replacedOID, err := borrowedOwnerDDLTargetOID(ctx, b)
	if err != nil {
		return nil, err
	}
	if replacedOID == oldOID || replacedOID != outcome.NewTargetOID {
		return nil, errors.New("replacement binding target OID is not the verified different replacement")
	}
	var ownerOID uint32
	ownerCtx, cancelOwner := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	ownerErr := f.fx.admin.QueryRow(ownerCtx, `SELECT datdba::oid FROM pg_database WHERE oid = $1`, replacedOID).Scan(&ownerOID)
	cancelOwner()
	if ownerErr != nil || ownerOID != f.run.Binding().WriterRoleOID() {
		return nil, errors.New("replacement binding database is not W-owned")
	}
	p1Conn := borrowedOwnerDDLConnect(t, ctx, borrowedAuthRoleDSN(t, f.writerTargetDSN, f.writerRole, b.passwordP1))
	pristineCtx, cancelPristine := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	digest, pristineErr := borrowedOwnerDDLPristine(pristineCtx, p1Conn, f.targetDB, f.writerRole)
	cancelPristine()
	borrowedSuccessorCloseConn(t, p1Conn)
	if pristineErr != nil || digest == "" {
		return nil, errors.New("replacement binding catalog is not whole-catalog pristine")
	}
	anchorCtx, cancelAnchor := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	anchorErr := b.anchor.Recheck(anchorCtx)
	cancelAnchor()
	if anchorErr != nil {
		return nil, errors.New("replacement binding retained owner anchor refused")
	}
	healthCtx, cancelHealth := context.WithTimeout(ctx, borrowedOwnerRotationHealthBudget)
	healthErr := f.lock.Health(healthCtx)
	cancelHealth()
	if healthErr != nil {
		return nil, errors.New("replacement binding retained owner health refused")
	}
	atomic.AddInt32(stages, 1)
	// Stage 4: the exact local P1 target DSN is derived from the baseline
	// password and its authentication is proven without calling Run; the fresh
	// transport binding uses the SAME retained lock, a fresh endpoint, sealed
	// tools, unchanged logical identities and that exact P1 DSN.
	p1TargetDSN := borrowedReplacementP1TargetDSN(t, b)
	if err := borrowedReplacementVerifyP1Auth(ctx, p1TargetDSN); err != nil {
		return nil, err
	}
	freshRun, err := borrowedReplacementFreshRunWith(ctx, t, f, f.observerTargetDSN, p1TargetDSN)
	if err != nil {
		return nil, err
	}
	if err := borrowedReplacementCompareBindings(f.run.Binding(), freshRun.Binding(), replacedOID); err != nil {
		return nil, err
	}
	atomic.AddInt32(stages, 1)
	// Stage 5: fresh OS prefix through the actual candidate capture path with
	// the same exact P1 DSN, then the recheck.
	setup := newBorrowedIdentityCandidateSetup(t, f.fx, p1TargetDSN)
	freshPrefix, err := setup.capture(ctx, freshRun)
	if err != nil {
		return nil, errors.New("replacement binding fresh OS prefix capture refused")
	}
	if err := freshPrefix.Recheck(t, ctx); err != nil {
		return nil, errors.New("replacement binding fresh OS prefix recheck refused")
	}
	// Final publication decision: a caller context that ended after the
	// recheck refuses, permanently invalidates the fresh state (shared with
	// every copy) and returns no capture. Every publication seam cancels the
	// pipeline context BEFORE it releases the waiter, so a release wake also
	// observes the cancellation here: no capture with a nil context error is
	// ever published after a release wake.
	borrowedReplacementPauseAtPublish(ctx, freshPrefix)
	if err := ctx.Err(); err != nil {
		freshPrefix.state.invalidate("replacement binding publication context ended")
		return nil, errors.New("replacement binding publication refused: caller context ended before publication")
	}
	atomic.AddInt32(stages, 1)
	return &borrowedReplacementBinding{
		fixture: f, entry: b.entry, anchor: b.anchor, run: freshRun, prefix: freshPrefix,
		oldOID: oldOID, replacementOID: replacedOID, binding: freshRun.Binding(),
	}, nil
}

// borrowedReplacementWrongIdentityNegative proves foreign/wrong identity
// captures refuse: a wrong declared database and a wrong declared role refuse
// the fresh prefix capture, and a reachable, authenticated second owned fixture
// observer refuses the fresh transport factory (non-destructive, shares the
// positive fixture).
func borrowedReplacementWrongIdentityNegative(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding) {
	t.Helper()
	f := b.fixture
	p1TargetDSN := borrowedReplacementP1TargetDSN(t, b)
	wrongDBSetup := newBorrowedIdentityCandidateSetup(t, f.fx,
		borrowedAuthRoleDSN(t, f.writerSourceDSN, f.writerRole, b.passwordP1))
	if _, err := wrongDBSetup.capture(ctx, fresh.run); err == nil {
		t.Fatal("wrong-database declared target produced a usable prefix capture")
	}
	wrongRoleSetup := newBorrowedIdentityCandidateSetup(t, f.fx,
		borrowedAuthRoleDSN(t, f.writerTargetDSN, f.observerRole, f.observerPass))
	if _, err := wrongRoleSetup.capture(ctx, fresh.run); err == nil {
		t.Fatal("wrong-role declared target produced a usable prefix capture")
	}
	// A reachable, authenticated SECOND owned fixture observer must be refused
	// with the intended identity refusal, not any connection error.
	secondFx := newOriginGateFixture(t)
	secondObserverDSN := borrowedAuthRoleDSN(t, secondFx.dsn, secondFx.role, secondFx.password)
	authCtx, cancelAuth := context.WithTimeout(ctx, borrowedOwnerRotationAuthBudget)
	secondConn, authErr := pgx.Connect(authCtx, secondObserverDSN)
	cancelAuth()
	if authErr != nil {
		t.Fatalf("second owned fixture observer did not authenticate (sqlstate=%s)", borrowedAuthSQLState(authErr))
	}
	var secondOne int
	secondQueryCtx, cancelSecondQuery := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	secondQueryErr := secondConn.QueryRow(secondQueryCtx, `SELECT 1`).Scan(&secondOne)
	cancelSecondQuery()
	secondCloseCtx, cancelSecondClose := context.WithTimeout(context.Background(), borrowedOwnerRotationCleanupBudget)
	secondCloseErr := secondConn.Close(secondCloseCtx)
	cancelSecondClose()
	if secondQueryErr != nil || secondOne != 1 || secondCloseErr != nil {
		t.Fatal("second owned fixture observer did not answer a bounded SELECT 1")
	}
	if _, err := borrowedReplacementFreshRunWith(ctx, t, f, secondObserverDSN, p1TargetDSN); err == nil ||
		!strings.Contains(err.Error(), "observer identity does not match") {
		t.Fatalf("second owned fixture observer was not refused with the intended identity refusal: %v", err)
	}
	// Positive control on the real fixture: its own observer identity still
	// produces a fresh transport binding.
	if _, err := borrowedReplacementFreshRunWith(ctx, t, f, f.observerTargetDSN, p1TargetDSN); err != nil {
		t.Fatalf("positive control fresh transport on the real fixture refused: %v", err)
	}
	t.Logf("wrong-identity negative: the wrong declared database and role were refused, and a reachable authenticated second owned fixture observer was refused with the intended identity refusal while the real fixture positive control succeeded")
}

// borrowedReplacementSecondReplacementNegative proves a second external
// replacement after the capture permanently invalidates the fresh prefix and
// is shared with every copy, while the fresh binding's recorded replacement
// OID is never overwritten (destructive, shares the positive fixture).
func borrowedReplacementSecondReplacementNegative(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding) {
	t.Helper()
	borrowedOwnerDDLExternalReplace(t, ctx, b)
	recheckCtx, cancelRecheck := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	recheckErr := fresh.prefix.Recheck(t, recheckCtx)
	cancelRecheck()
	if recheckErr == nil {
		t.Fatal("fresh prefix recheck succeeded after a second replacement")
	}
	invalid, reason := fresh.prefix.Invalid()
	if !invalid || reason == "" {
		t.Fatal("fresh prefix loss after the second replacement is not shared")
	}
	copied := *fresh
	copiedInvalid, _ := copied.prefix.Invalid()
	if !copiedInvalid {
		t.Fatal("copy did not share the fresh prefix invalidation")
	}
	if fresh.binding.TargetDatabaseOID() != fresh.replacementOID {
		t.Fatal("fresh binding replacement OID was overwritten")
	}
	t.Logf("second-replacement negative: the fresh prefix recheck permanently refused and the shared loss propagated to the copy; the recorded replacement OID is unchanged")
}

// borrowedReplacementNoCaptureNegative proves caller cancel, an incomplete
// strict inspection and real owner loss each produce no usable capture and
// that a previously captured binding refuses with the loss shared by copies
// (fresh destructive fixture).
func borrowedReplacementNoCaptureNegative(t *testing.T, ctx context.Context) {
	t.Helper()
	b := newBorrowedSuccessorBaseline(t, ctx)
	f := b.fixture
	p1TargetDSN := borrowedReplacementP1TargetDSN(t, b)
	var stages int32
	orchestrateCtx, cancelOrchestrate := borrowedReplacementOrchestrationContext(ctx)
	fresh, err := borrowedReplacementBindingOrchestrate(orchestrateCtx, t, b, &stages)
	cancelOrchestrate()
	if err != nil || fresh == nil {
		t.Fatalf("no-capture negative pipeline refused: capture=%v err=%v", fresh, err)
	}
	if got := atomic.LoadInt32(&stages); got != borrowedReplacementStagePrefixCaptured {
		t.Fatalf("no-capture negative pipeline stages=%d, want %d", got, borrowedReplacementStagePrefixCaptured)
	}
	// Caller cancel: no fresh transport and no fresh prefix capture.
	cancelCtx, cancelCall := context.WithCancel(ctx)
	cancelCall()
	if _, err := borrowedReplacementFreshRunWith(cancelCtx, t, f, f.observerTargetDSN, p1TargetDSN); err == nil {
		t.Fatal("canceled caller produced a fresh transport binding")
	}
	cancelSetup := newBorrowedIdentityCandidateSetup(t, f.fx, p1TargetDSN)
	if _, err := cancelSetup.capture(cancelCtx, fresh.run); err == nil {
		t.Fatal("canceled caller produced a usable prefix capture")
	}
	// Incomplete strict inspection: the census is corrupted and the capture
	// refuses.
	hook := func(census *borrowedIdentityStrictCensus, phase borrowedIdentityStrictPhase) {
		census.Complete = false
		census.Errors = append(census.Errors, "injected-incomplete")
	}
	borrowedIdentityStrictReportHook.Store(&hook)
	incompleteSetup := newBorrowedIdentityCandidateSetup(t, f.fx, p1TargetDSN)
	incompleteCapture, incompleteErr := incompleteSetup.capture(ctx, fresh.run)
	borrowedIdentityStrictReportHook.Store(nil)
	if incompleteErr == nil || incompleteCapture != nil {
		t.Fatal("incomplete strict inspection produced a usable capture")
	}
	// Real owner loss: the previously captured binding refuses and the loss is
	// shared with copies.
	controlPID := f.run.Binding().ControlBackendPID()
	var terminated bool
	lossCtx, cancelLoss := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	lossErr := f.controlPool.QueryRow(lossCtx, `SELECT pg_terminate_backend($1)`, controlPID).Scan(&terminated)
	cancelLoss()
	if lossErr != nil || !terminated {
		t.Fatalf("terminate replacement-binding control owner: terminated=%t err=%v", terminated, lossErr)
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
	lossRecheckCtx, cancelLossRecheck := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	lossRecheckErr := fresh.prefix.Recheck(t, lossRecheckCtx)
	cancelLossRecheck()
	if lossRecheckErr == nil {
		t.Fatal("captured binding prefix recheck succeeded after real owner loss")
	}
	lossCopy := *fresh
	if invalid, reason := lossCopy.prefix.Invalid(); !invalid || reason == "" {
		t.Fatal("owner-loss invalidation is not shared with the copy")
	}
	if _, err := borrowedReplacementFreshRunWith(ctx, t, f, f.observerTargetDSN, p1TargetDSN); err == nil {
		t.Fatal("owner loss produced a fresh transport binding")
	}
	borrowedOwnerDDLGuard(t, ctx, b)
	t.Logf("no-capture negative: caller cancel, an incomplete strict inspection and real owner loss each produced no usable capture, and the captured binding's shared loss refused its copy")
}

// borrowedReplacementPartialDDLNegative proves a partial/UNKNOWN DDL stops the
// pipeline before any capture (fresh destructive fixture).
func borrowedReplacementPartialDDLNegative(t *testing.T, ctx context.Context) {
	t.Helper()
	b := newBorrowedSuccessorBaseline(t, ctx)
	f := b.fixture
	templateCtx, cancelTemplate := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	templateBase := borrowedIdentityRoute(t, templateCtx, f.fx, "template1")
	cancelTemplate()
	templateConn := borrowedOwnerDDLConnect(t, ctx, borrowedAuthRoleDSN(t, templateBase, f.controlRole, f.controlPass))
	var stages int32
	orchestrateCtx, cancelOrchestrate := borrowedReplacementOrchestrationContext(ctx)
	captured, err := borrowedReplacementBindingOrchestrate(orchestrateCtx, t, b, &stages)
	cancelOrchestrate()
	if err == nil || captured != nil {
		t.Fatalf("partial/UNKNOWN DDL produced a capture: capture=%v err=%v", captured, err)
	}
	if got := atomic.LoadInt32(&stages); got != borrowedReplacementStageSuccessorRetired {
		t.Fatalf("partial/UNKNOWN DDL stages=%d, want stop at %d", got, borrowedReplacementStageSuccessorRetired)
	}
	borrowedSuccessorCloseConn(t, templateConn)
	borrowedOwnerDDLGuard(t, ctx, b)
	t.Logf("partial-DDL negative: the partial/UNKNOWN DDL stopped the pipeline before the replacement verification and the fresh captures")
}

// borrowedReplacementContaminatedNegative proves a contaminated replacement
// stops the pipeline after the DDL but before any capture (fresh destructive
// fixture).
func borrowedReplacementContaminatedNegative(t *testing.T, ctx context.Context) {
	t.Helper()
	b := newBorrowedSuccessorBaseline(t, ctx)
	borrowedOwnerDDLContaminateTemplate(t, ctx, b, []string{
		`CREATE VIEW public.owner_ddl_replacement_probe AS SELECT 1 AS one`,
	})
	var stages int32
	orchestrateCtx, cancelOrchestrate := borrowedReplacementOrchestrationContext(ctx)
	captured, err := borrowedReplacementBindingOrchestrate(orchestrateCtx, t, b, &stages)
	cancelOrchestrate()
	if err == nil || captured != nil {
		t.Fatalf("contaminated replacement produced a capture: capture=%v err=%v", captured, err)
	}
	if got := atomic.LoadInt32(&stages); got != borrowedReplacementStageDDLCommitted {
		t.Fatalf("contaminated replacement stages=%d, want stop at %d", got, borrowedReplacementStageDDLCommitted)
	}
	borrowedOwnerDDLGuard(t, ctx, b)
	t.Logf("contaminated negative: the non-pristine replacement stopped the pipeline after the DDL and before the fresh captures")
}

// borrowedReplacementLateCancelNegative proves a caller cancellation after the
// fresh prefix recheck and before the final publication decision returns no
// capture and permanently invalidates the fresh state, with the loss shared
// with every copy. Every trigger exit path cancels the pipeline context and
// releases the publication waiter exactly once, every join is bounded and the
// publication hook also selects the caller context (fresh destructive fixture).
func borrowedReplacementLateCancelNegative(t *testing.T, ctx context.Context) {
	t.Helper()
	b := newBorrowedSuccessorBaseline(t, ctx)
	entered := make(chan struct{})
	release := make(chan struct{})
	var enteredOnce, releaseOnce sync.Once
	var observed *borrowedIdentityPrefix
	var wokeByRelease atomic.Bool
	hook := func(hookCtx context.Context, prefix *borrowedIdentityPrefix) {
		observed = prefix
		enteredOnce.Do(func() { close(entered) })
		select {
		case <-release:
			// The trigger cancels the pipeline context BEFORE it closes the
			// release channel, so a release wake must observe the cancellation.
			wokeByRelease.Store(true)
			if hookCtx.Err() == nil {
				t.Errorf("publication hook woke by release before the pipeline context was canceled")
			}
		case <-hookCtx.Done():
		}
	}
	borrowedReplacementPublishHook.Store(&hook)
	defer borrowedReplacementPublishHook.Store(nil)
	useCtx, cancelUse := borrowedReplacementOrchestrationContext(ctx)
	triggerDone := make(chan struct{})
	go func() {
		// ONE deferred function: cancel the pipeline context BEFORE releasing
		// the publication waiter, and close triggerDone after both actions
		// (three separate defers would run release-first under LIFO order).
		defer func() {
			cancelUse()
			releaseOnce.Do(func() { close(release) })
			close(triggerDone)
		}()
		select {
		case <-entered:
		case <-time.After(borrowedReplacementTriggerWindow):
		}
	}()
	joinedTrigger := false
	t.Cleanup(func() {
		cancelUse()
		releaseOnce.Do(func() { close(release) })
		if joinedTrigger {
			return
		}
		select {
		case <-triggerDone:
		case <-time.After(borrowedReplacementJoinBudget):
			t.Errorf("late-cancel trigger completion is unknown after the bounded cleanup join")
		}
	})
	var stages int32
	captured, pipelineErr := borrowedReplacementBindingOrchestrate(useCtx, t, b, &stages)
	select {
	case <-triggerDone:
		joinedTrigger = true
	case <-time.After(borrowedReplacementJoinBudget):
		cancelUse()
		releaseOnce.Do(func() { close(release) })
		select {
		case <-triggerDone:
			joinedTrigger = true
		case <-time.After(borrowedReplacementJoinBudget):
			t.Errorf("late-cancel trigger completion is unknown after the bounded fallback join")
		}
	}
	if !joinedTrigger {
		t.Fatal("late-cancel trigger did not complete within the bounded joins")
	}
	if useCtx.Err() == nil {
		t.Fatal("late-cancel trigger did not cancel the pipeline context")
	}
	if wokeByRelease.Load() && (pipelineErr == nil || captured != nil) {
		t.Fatalf("a capture published with a nil context error after a release wake was accepted: capture=%v err=%v", captured, pipelineErr)
	}
	if pipelineErr == nil || captured != nil {
		t.Fatalf("late cancellation produced a capture: capture=%v err=%v", captured, pipelineErr)
	}
	if got := atomic.LoadInt32(&stages); got != borrowedReplacementStageTransportCaptured {
		t.Fatalf("late-cancel stages=%d, want stop at %d", got, borrowedReplacementStageTransportCaptured)
	}
	if observed == nil {
		t.Fatal("late-cancel publication pause did not observe the fresh prefix")
	}
	if invalid, reason := observed.Invalid(); !invalid || reason == "" {
		t.Fatal("late cancellation did not permanently invalidate the fresh state")
	}
	copied := *observed
	if invalid, _ := copied.Invalid(); !invalid {
		t.Fatal("late-cancel invalidation is not shared with the prefix copy")
	}
	borrowedOwnerDDLGuard(t, ctx, b)
	t.Logf("late-cancel negative: cancellation at the publication pause returned no capture, permanently invalidated the fresh state, shared the loss with the copy and every trigger path canceled and released within the bounded joins")
}

// borrowedReplacementTimeoutBeforePauseControl proves that when the trigger
// window expires before the publication pause (the pipeline is held at its
// pre-publication seam), the trigger still cancels the pipeline context and
// releases the publication waiter exactly once, the pipeline still terminates
// with no capture, and a stranded/unknown outcome is recorded as a failure
// rather than silently accepted (fresh destructive fixture).
func borrowedReplacementTimeoutBeforePauseControl(t *testing.T, ctx context.Context) {
	t.Helper()
	b := newBorrowedSuccessorBaseline(t, ctx)
	stageEntered := make(chan struct{})
	stageRelease := make(chan struct{})
	var stageEnteredOnce, stageReleaseOnce sync.Once
	stageHook := func(hookCtx context.Context) {
		stageEnteredOnce.Do(func() { close(stageEntered) })
		select {
		case <-stageRelease:
		case <-hookCtx.Done():
		}
	}
	borrowedReplacementStageHook.Store(&stageHook)
	defer borrowedReplacementStageHook.Store(nil)
	entered := make(chan struct{})
	release := make(chan struct{})
	var enteredOnce, releaseOnce sync.Once
	publishHook := func(hookCtx context.Context, prefix *borrowedIdentityPrefix) {
		enteredOnce.Do(func() { close(entered) })
		select {
		case <-release:
		case <-hookCtx.Done():
		}
	}
	borrowedReplacementPublishHook.Store(&publishHook)
	defer borrowedReplacementPublishHook.Store(nil)
	useCtx, cancelUse := borrowedReplacementOrchestrationContext(ctx)
	// The trigger window is deliberately far shorter than the held
	// pre-publication seam, so the trigger exits through its timeout path.
	triggerWindow := 200 * time.Millisecond
	triggerDone := make(chan struct{})
	go func() {
		// ONE deferred function: cancel the pipeline context BEFORE releasing
		// the publication waiter, and close triggerDone after both actions.
		defer func() {
			cancelUse()
			releaseOnce.Do(func() { close(release) })
			close(triggerDone)
		}()
		select {
		case <-entered:
		case <-time.After(triggerWindow):
		}
	}()
	joinedTrigger := false
	t.Cleanup(func() {
		cancelUse()
		releaseOnce.Do(func() { close(release) })
		stageReleaseOnce.Do(func() { close(stageRelease) })
		if joinedTrigger {
			return
		}
		select {
		case <-triggerDone:
		case <-time.After(borrowedReplacementJoinBudget):
			t.Errorf("timeout-before-pause trigger completion is unknown after the bounded cleanup join")
		}
	})
	var stages int32
	captured, pipelineErr := borrowedReplacementBindingOrchestrate(useCtx, t, b, &stages)
	select {
	case <-triggerDone:
		joinedTrigger = true
	case <-time.After(borrowedReplacementJoinBudget):
		cancelUse()
		releaseOnce.Do(func() { close(release) })
		stageReleaseOnce.Do(func() { close(stageRelease) })
		select {
		case <-triggerDone:
			joinedTrigger = true
		case <-time.After(borrowedReplacementJoinBudget):
			t.Errorf("timeout-before-pause trigger completion is unknown after the bounded fallback join")
		}
	}
	if !joinedTrigger {
		t.Fatal("timeout-before-pause trigger did not complete within the bounded joins")
	}
	if useCtx.Err() == nil {
		t.Fatal("timeout-before-pause trigger did not cancel the pipeline context")
	}
	if captured != nil {
		t.Fatal("timeout-before-pause control published a capture")
	}
	if pipelineErr == nil {
		t.Fatal("timeout-before-pause control did not fail the pipeline")
	}
	if got := atomic.LoadInt32(&stages); got != 0 {
		t.Fatalf("timeout-before-pause stages=%d, want stop at 0 (pre-publication hold)", got)
	}
	select {
	case <-entered:
		t.Fatal("timeout-before-pause control unexpectedly reached the publication pause")
	default:
	}
	select {
	case <-stageEntered:
	default:
		t.Fatal("timeout-before-pause control never held the pre-publication seam")
	}
	select {
	case <-release:
	default:
		t.Fatal("timeout-before-pause trigger did not release the publication waiter")
	}
	borrowedOwnerDDLGuard(t, ctx, b)
	t.Logf("timeout-before-pause control: the trigger window expired while the pipeline was held at its pre-publication seam; the trigger canceled and released exactly once, the pipeline terminated with no capture and no unknown outcome was silently accepted")
}

// TestBorrowedReplacementBindingFeasibility is the bounded replacement-bound
// identity capture lane described in the file header.
func TestBorrowedReplacementBindingFeasibility(t *testing.T) {
	ctx := t.Context()
	b := newBorrowedSuccessorBaseline(t, ctx)
	f := b.fixture
	originalTargetDSN := f.writerTargetDSN
	originalObserverDSN := f.observerTargetDSN
	var stages int32
	orchestrateCtx, cancelOrchestrate := borrowedReplacementOrchestrationContext(ctx)
	fresh, err := borrowedReplacementBindingOrchestrate(orchestrateCtx, t, b, &stages)
	cancelOrchestrate()
	if err != nil {
		t.Fatalf("replacement binding orchestration refused: %v", err)
	}
	if fresh == nil {
		t.Fatal("replacement binding orchestration produced no capture")
	}
	if f.writerTargetDSN != originalTargetDSN || f.observerTargetDSN != originalObserverDSN {
		t.Fatal("replacement binding lane rewrote an original fixture DSN")
	}
	if got := atomic.LoadInt32(&stages); got != borrowedReplacementStagePrefixCaptured {
		t.Fatalf("replacement binding pipeline stages=%d, want %d", got, borrowedReplacementStagePrefixCaptured)
	}
	if fresh.binding.TargetDatabaseOID() != fresh.replacementOID || fresh.replacementOID == fresh.oldOID {
		t.Fatalf("fresh binding is not replacement-bound: replacement=%d old=%d binding=%d", fresh.replacementOID, fresh.oldOID, fresh.binding.TargetDatabaseOID())
	}
	t.Logf("replacement binding captured: old OID %d -> replacement OID %d; fresh transport and OS prefix bound to the replacement", fresh.oldOID, fresh.replacementOID)

	// Old invalid: the retained old prefix/entry refuse with their
	// never-overwritten old database OID, and old successor construction over
	// the replacement is inappropriate.
	if f.prefix.capturedTargetDBOID != fresh.oldOID {
		t.Fatal("replacement binding lane overwrote the retained prefix target database OID")
	}
	prefixCtx, cancelPrefix := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	prefixErr := f.prefix.Recheck(t, prefixCtx)
	cancelPrefix()
	if prefixErr == nil {
		t.Fatal("retained old prefix recheck succeeded after the replacement")
	}
	entryCtx, cancelEntry := context.WithTimeout(ctx, 60*time.Second)
	entryErr := b.entry.Recheck(entryCtx)
	cancelEntry()
	if entryErr == nil {
		t.Fatal("retained old entry recheck succeeded after the replacement")
	}
	if _, reason := b.entry.stageNow(); reason == "" {
		t.Fatal("retained old entry was not permanently invalidated")
	}
	oldConn := borrowedOwnerDDLConnect(t, ctx, borrowedAuthRoleDSN(t, f.writerTargetDSN, f.writerRole, b.passwordP1))
	if _, err := newBorrowedSuccessorRegistration(t, ctx, f, b.entry, b.anchor, oldConn, b.verifierP1, b.preState); err == nil {
		t.Fatal("old successor construction over the replacement was accepted")
	}
	borrowedSuccessorCloseConn(t, oldConn)
	borrowedOwnerDDLGuard(t, ctx, b)

	// Negatives.
	borrowedReplacementWrongIdentityNegative(t, ctx, b, fresh)
	borrowedReplacementSecondReplacementNegative(t, ctx, b, fresh)
	borrowedReplacementNoCaptureNegative(t, ctx)
	borrowedReplacementPartialDDLNegative(t, ctx)
	borrowedReplacementContaminatedNegative(t, ctx)
	borrowedReplacementLateCancelNegative(t, ctx)
	borrowedReplacementTimeoutBeforePauseControl(t, ctx)
}
