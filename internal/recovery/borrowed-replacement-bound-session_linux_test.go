//go:build linux && drill

// borrowed-replacement-bound-session_linux_test.go is the bounded single-chain
// instance-bound retained-session registration/use/retirement lane. It starts
// from ONE authentic bound baseline (approved bound fixture -> single
// receipt/entry consumption -> same-owner P1) and the EXISTING bounded
// replacement-binding orchestration with its complete stage count and genuine
// replacement capture, snapshots the bound instance (complete guard-row hash,
// state/key/fingerprint, generation/hash, NULL inventory) after the capture and
// before registration, retains ONE P1 connection with immediate independent
// bounded disposal, registers that exact connection against the shared
// fresh-prefix state and the original anchor with strict SQL/OS/census
// evidence, uses it exactly once (one fixed SELECT 1 on the same connection,
// identity-discovery queries counted separately), refuses replay, retires it
// through the existing retirement path with strict incarnation disappearance,
// and re-compares the snapshot: the original owner/anchor health and the
// unresolved guard are unchanged, no successful restore-probe/acceptance
// evidence is added and no EntryChainBound or continuous-exclusion credit is
// claimed. Every negative control starts through the bound constructor and the
// actual production-independent primitives; there is no copied orchestration,
// no replacement child, no admission waiter, no fabricated token/evidence and
// no second-instance bypass of the single-open invariant. This lane grants no
// restore, rebuild, acceptance, manifest, downstream or Gate1 authority; the
// fresh Run is never invoked and secrets, verifiers and DSNs are never logged.
package recovery_test

import (
	"context"
	"crypto/subtle"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// newBorrowedBoundSessionCapture creates ONE authentic bound baseline through
// the approved bound constructor and the EXISTING bounded replacement-binding
// orchestration, asserting the complete stage count (all five stages) and the
// genuine replacement-bound capture.
func newBorrowedBoundSessionCapture(t *testing.T, ctx context.Context) (*borrowedSuccessorBaseline, *borrowedReplacementBinding) {
	t.Helper()
	b := newBorrowedSuccessorBaselineBound(t, ctx)
	if b == nil || b.fixture == nil || !b.fixture.bound || b.fixture.instanceID == "" {
		t.Fatalf("bound session baseline provenance: baseline=%v", b)
	}
	var stages int32
	orchestrateCtx, cancelOrchestrate := borrowedReplacementOrchestrationContext(ctx)
	fresh, err := borrowedReplacementBindingOrchestrate(orchestrateCtx, t, b, &stages)
	cancelOrchestrate()
	if err != nil || fresh == nil {
		t.Fatalf("bound session replacement capture refused: capture=%v err=%v", fresh, err)
	}
	if got := atomic.LoadInt32(&stages); got != borrowedReplacementStagePrefixCaptured {
		t.Fatalf("bound session replacement capture stages=%d, want the complete %d", got, borrowedReplacementStagePrefixCaptured)
	}
	if fresh.replacementOID == 0 || fresh.replacementOID == fresh.oldOID || fresh.binding.TargetDatabaseOID() != fresh.replacementOID {
		t.Fatalf("bound session capture is not replacement-bound: replacement=%d old=%d binding=%d", fresh.replacementOID, fresh.oldOID, fresh.binding.TargetDatabaseOID())
	}
	return b, fresh
}

// borrowedBoundSessionSnapshot is the complete comparable bound-instance
// snapshot: the guard-row hash, the persistent instance identity and the
// evidence generation/hash, with the instance inventory asserted NULL.
type borrowedBoundSessionSnapshot struct {
	guardHash     string
	state         string
	key           string
	fingerprint   string
	generation    int64
	evidenceHash  string
	inventoryNull bool
	versionNull   bool
}

func borrowedBoundSessionSnapshotNow(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline) borrowedBoundSessionSnapshot {
	t.Helper()
	f := b.fixture
	snap := borrowedBoundSessionSnapshot{guardHash: borrowedInstanceBindingGuardSnapshotHash(t, ctx, f.controlPool, f.guardKey)}
	readCtx, cancelRead := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	readErr := f.controlPool.QueryRow(readCtx, `
SELECT state, target_guard_key, target_role_fingerprint, evidence_generation, evidence_hash,
       entry_chain_inventory IS NULL, entry_chain_inventory_version IS NULL
FROM recovery_instance WHERE instance_id=$1`, f.instanceID).
		Scan(&snap.state, &snap.key, &snap.fingerprint, &snap.generation, &snap.evidenceHash, &snap.inventoryNull, &snap.versionNull)
	cancelRead()
	if readErr != nil {
		t.Fatalf("bound session instance snapshot refused: %v", readErr)
	}
	if snap.state != "open" || !snap.inventoryNull || !snap.versionNull {
		t.Fatalf("bound session instance snapshot is not an open NULL-inventory instance: state=%q inventory=%t version=%t", snap.state, snap.inventoryNull, snap.versionNull)
	}
	return snap
}

func borrowedBoundSessionAssertSnapshotEqual(t *testing.T, label string, before, after borrowedBoundSessionSnapshot) {
	t.Helper()
	if before != after {
		t.Fatalf("%s changed the complete bound instance snapshot: before=%+v after=%+v", label, before, after)
	}
}

// borrowedBoundSessionAssertProvenance requires the matching immutable
// InstanceID across the fixture, the original factory binding, the fresh
// replacement capture and the original anchor, plus the persisted open
// instance key/fingerprint with NULL inventory (never an EntryChainBound
// credit).
func borrowedBoundSessionAssertProvenance(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding, label string) {
	t.Helper()
	f := b.fixture
	original := f.run.Binding()
	if f.instanceID == "" || original.OriginalInstanceID() != f.instanceID ||
		fresh.binding.OriginalInstanceID() != f.instanceID || b.owner.OriginalInstanceID != f.instanceID {
		t.Fatalf("%s: the immutable InstanceID is not consistent across fixture/original binding/fresh capture/anchor: fixture=%q original=%q fresh=%q anchor=%q",
			label, f.instanceID, original.OriginalInstanceID(), fresh.binding.OriginalInstanceID(), b.owner.OriginalInstanceID)
	}
	var state, key, fingerprint string
	var inventoryNull, versionNull bool
	readCtx, cancelRead := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	readErr := f.controlPool.QueryRow(readCtx, `
SELECT state, target_guard_key, target_role_fingerprint, entry_chain_inventory IS NULL, entry_chain_inventory_version IS NULL
FROM recovery_instance WHERE instance_id=$1`, f.instanceID).Scan(&state, &key, &fingerprint, &inventoryNull, &versionNull)
	cancelRead()
	if readErr != nil || state != "open" || key != original.OriginalTargetKey().String() ||
		fingerprint != original.OriginalRoleFingerprint() || !inventoryNull || !versionNull {
		t.Fatalf("%s: persisted bound instance is not open/consistent/NULL: state=%q key=%q fingerprint=%q inventory=%t version=%t err=%v",
			label, state, key, fingerprint, inventoryNull, versionNull, readErr)
	}
}

// borrowedBoundSessionAssertRegistrationRetained requires the registration to
// retain the genuine fixture, the SHARED fresh-prefix state (never a wrapper
// address), the original anchor, the exact connection, the verified
// replacement OID and the committed P1 baseline, and correlates the SQL
// PID/start/role/database OIDs with the strict OS/start/socket/tuple evidence.
func borrowedBoundSessionAssertRegistrationRetained(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding, conn *pgx.Conn, reg *borrowedReplacementSessionRegistration, label string) {
	t.Helper()
	if reg == nil || reg.state == nil {
		t.Fatalf("%s: bound session registration is absent", label)
	}
	state := reg.state
	if state.fixture != b.fixture || state.prefix != fresh.prefix || state.prefix.state != fresh.prefix.state ||
		state.anchor != fresh.anchor || state.conn != conn || state.replacementOID != fresh.replacementOID ||
		state.expectedVerifier != b.verifierP1 || !borrowedFenceGapRoleStateEqual(state.expectedState, b.preState) {
		t.Fatalf("%s: registration did not retain the genuine fixture/shared prefix state/anchor/connection/replacement/P1 baseline", label)
	}
	identity, identityErr := borrowedSuccessorReadConnIdentity(ctx, conn)
	if identityErr != nil {
		t.Fatalf("%s: retained connection identity refused: %v", label, identityErr)
	}
	if identity.backendPID != state.backendPID || !identity.backendStart.Equal(state.backendStart) ||
		identity.roleOID != state.roleOID || identity.targetDBOID != state.targetDBOID ||
		identity.roleName != b.fixture.writerRole || identity.databaseName != b.fixture.targetDB {
		t.Fatalf("%s: retained SQL PID/start/role/database does not match the registration: %+v", label, identity)
	}
	statCtx, cancelStat := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	osState, osStart, statErr := borrowedAuthStagingContainerStat(statCtx, b.fixture.fx.containerID, state.backendPID)
	cancelStat()
	if statErr != nil || osStart != state.osStart || !borrowedAuthStagingLiveState(osState) {
		t.Fatalf("%s: strict registered OS identity is not the live incarnation: state=%q start=%d err=%v", label, osState, osStart, statErr)
	}
	token, tokenErr := borrowedSuccessorAssociateToken(ctx, b.fixture, state.serverLocal, state.clientLocal)
	if tokenErr != nil || token.ChildPID != state.backendPID || token.ChildStart != state.osStart ||
		token.Inode != state.socketInode || token.Local != state.serverLocal || token.Remote != state.clientLocal {
		t.Fatalf("%s: strict census tuple evidence does not match the registration: %+v err=%v", label, token, tokenErr)
	}
}

// borrowedBoundSessionCancelControl proves a real cancellation at the pre-probe
// barrier (zero probe executions) or the post-probe publication barrier (one
// probe execution, no successful publication) on an independent bound fixture,
// with the pre-fault retained copy and reconstructions permanently refused
// under fresh contexts.
func borrowedBoundSessionCancelControl(t *testing.T, ctx context.Context, postProbe bool) {
	t.Helper()
	label := "pre-probe"
	stage := borrowedReplacementSessionStageUsePublish
	wantExec := int32(0)
	barrierBudget := 60 * time.Second
	if postProbe {
		label = "post-probe publication"
		stage = borrowedReplacementSessionStageUsePublication
		wantExec = 1
		barrierBudget = 120 * time.Second
	}
	b, fresh := newBorrowedBoundSessionCapture(t, ctx)
	conn, reg := borrowedReplacementSessionRegisterP1(t, ctx, b, fresh)
	retained := *reg // retained BEFORE the fault
	seam := installBorrowedReplacementSessionSeam(t, reg, stage)
	useCtx, cancelUse := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- reg.Use(useCtx) }()
	joined := false
	unwind := func() {
		cancelUse()
		seam.releaseSeam()
		if joined {
			return
		}
		select {
		case <-done:
			joined = true
		case <-time.After(30 * time.Second):
			t.Errorf("bound session %s cancel use completion is unknown after the bounded unwind", label)
		}
	}
	defer unwind()
	t.Cleanup(unwind)
	select {
	case <-seam.entered:
	case <-time.After(barrierBudget):
		t.Errorf("bound session %s cancel use never reached the real barrier; completion is unknown", label)
		return
	}
	cancelUse()
	seam.releaseSeam()
	select {
	case useErr := <-done:
		joined = true
		if useErr == nil {
			t.Fatalf("canceled %s bound session use published success", label)
		}
		if errors.Is(useErr, errBorrowedReplacementSessionVerifierDrift) || errors.Is(useErr, errBorrowedReplacementSessionRoleFactsDrift) {
			t.Fatalf("bound session %s cancel was misclassified as drift: %v", label, useErr)
		}
	case <-time.After(30 * time.Second):
		t.Errorf("canceled bound session %s use did not complete within the bounded join; outcome unknown", label)
		return
	}
	if got := atomic.LoadInt32(&reg.state.probeExecutions); got != wantExec {
		t.Fatalf("bound session %s cancel probe executions=%d, want %d", label, got, wantExec)
	}
	freshCtx, cancelFresh := context.WithTimeout(ctx, 30*time.Second)
	if retained.Use(freshCtx) == nil {
		t.Fatalf("pre-fault %s cancel copy rehabilitated the bound session registration", label)
	}
	if reconReg, reconErr := newBorrowedReplacementSessionRegistration(t, freshCtx, b, fresh, conn); reconReg != nil || reconErr == nil {
		t.Fatalf("post-%s cancel reconstruction minted a fresh bound session registration (reg=%v err=%v)", label, reconReg, reconErr)
	}
	cancelFresh()
	borrowedSuccessorCloseConn(t, conn)
	borrowedOwnerDDLGuard(t, ctx, b)
	t.Logf("bound session %s cancel control: the caller context ended at the real barrier with %d probe execution(s) and no successful publication; the pre-fault copy and reconstruction were permanently refused", label, wantExec)
}

// borrowedBoundSessionPrefixLossControl proves the UNCHANGED copied-prefix
// source loss at the pre-probe barrier (zero probe executions) or the
// post-probe publication barrier (exactly one probe execution, no successful
// publication) on an independent bound fixture, with pre-fault retained copies,
// prefix-wrapper reconstructions and reconstructions permanently refused.
func borrowedBoundSessionPrefixLossControl(t *testing.T, ctx context.Context, postProbe bool) {
	t.Helper()
	label := "pre-probe"
	stage := borrowedReplacementSessionStageUsePublish
	wantExec := int32(0)
	barrierBudget := 60 * time.Second
	if postProbe {
		label = "post-probe publication"
		stage = borrowedReplacementSessionStageUsePublication
		wantExec = 1
		barrierBudget = 120 * time.Second
	}
	b, fresh := newBorrowedBoundSessionCapture(t, ctx)
	conn, reg := borrowedReplacementSessionRegisterP1(t, ctx, b, fresh)
	retained := *reg // retained BEFORE the loss
	seam := installBorrowedReplacementSessionSeam(t, reg, stage)
	useCtx, cancelUse := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- reg.Use(useCtx) }()
	joined := false
	unwind := func() {
		cancelUse()
		seam.releaseSeam()
		if joined {
			return
		}
		select {
		case <-done:
			joined = true
		case <-time.After(30 * time.Second):
			t.Errorf("bound session %s prefix-loss use completion is unknown after the bounded unwind", label)
		}
	}
	defer unwind()
	t.Cleanup(unwind)
	select {
	case <-seam.entered:
	case <-time.After(barrierBudget):
		t.Errorf("bound session %s prefix-loss use never reached the real barrier; completion is unknown", label)
		return
	}
	borrowedReplacementSessionCopiedPrefixLoss(t, fresh)
	seam.releaseSeam()
	select {
	case useErr := <-done:
		joined = true
		if useErr == nil {
			t.Fatalf("bound session %s shared prefix loss published a successful use", label)
		}
	case <-time.After(30 * time.Second):
		t.Errorf("bound session %s prefix-loss use did not complete within the bounded join; outcome unknown", label)
		return
	}
	if got := atomic.LoadInt32(&reg.state.probeExecutions); got != wantExec {
		t.Fatalf("bound session %s prefix loss probe executions=%d, want %d", label, got, wantExec)
	}
	if reg.state.invalidReasonNow() == "" {
		t.Fatalf("bound session %s prefix loss did not permanently invalidate the session", label)
	}
	if invalid, reason := fresh.prefix.Invalid(); !invalid || reason == "" {
		t.Fatalf("bound session %s prefix loss did not permanently invalidate the shared prefix", label)
	}
	freshCtx, cancelFresh := context.WithTimeout(ctx, 30*time.Second)
	if retained.Use(freshCtx) == nil {
		t.Fatalf("pre-fault %s prefix-loss copy rehabilitated the bound session registration", label)
	}
	if reconReg, reconErr := newBorrowedReplacementSessionRegistration(t, freshCtx, b, fresh, conn); reconReg != nil || reconErr == nil {
		t.Fatalf("post-%s prefix-loss reconstruction minted a fresh bound session registration (reg=%v err=%v)", label, reconReg, reconErr)
	}
	cancelFresh()
	prefixCopy := *fresh.prefix
	freshCopy := *fresh
	freshCopy.prefix = &prefixCopy
	if wrapperReg, wrapperErr := newBorrowedReplacementSessionRegistration(t, ctx, b, &freshCopy, conn); wrapperReg != nil || wrapperErr == nil {
		t.Fatalf("post-%s prefix-loss wrapper reconstruction minted a fresh bound session registration (reg=%v err=%v)", label, wrapperReg, wrapperErr)
	}
	borrowedSuccessorCloseConn(t, conn)
	borrowedOwnerDDLGuard(t, ctx, b)
	t.Logf("bound session %s shared-prefix-loss control: the actual copied-prefix loss at the real barrier refused with %d probe execution(s) and no successful publication; pre-fault copy, wrapper and reconstruction were permanently refused", label, wantExec)
}

// borrowedBoundSessionForeignCaptureRefuse proves a genuine foreign bound
// capture is refused by the registration at its early fixture/provenance
// precondition, and that this early mismatch is NEVER credited as an isolated
// InstanceID comparison.
func borrowedBoundSessionForeignCaptureRefuse(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding) {
	t.Helper()
	foreignBaseline, foreignFresh := newBorrowedBoundSessionCapture(t, ctx)
	original := b.fixture.run.Binding()
	other := foreignBaseline.fixture.run.Binding()
	if original.OriginalInstanceID() == "" || other.OriginalInstanceID() == "" ||
		original.OriginalInstanceID() == other.OriginalInstanceID() {
		t.Fatalf("foreign-capture control requires two distinct genuine bound instance ids: %q / %q", original.OriginalInstanceID(), other.OriginalInstanceID())
	}
	if foreignFresh.fixture == b.fixture {
		t.Fatal("foreign-capture control foreign capture shares the primary fixture")
	}
	conn := borrowedReplacementSessionConnectP1(t, ctx, b)
	foreignReg, foreignErr := newBorrowedReplacementSessionRegistration(t, ctx, b, foreignFresh, conn)
	if foreignReg != nil || foreignErr == nil {
		t.Fatalf("foreign bound capture minted a registration (reg=%v err=%v)", foreignReg, foreignErr)
	}
	if !strings.Contains(foreignErr.Error(), "requires the concrete baseline, fresh replacement capture and retained connection") {
		t.Fatalf("foreign bound capture was not refused at the early fixture/provenance precondition: %v", foreignErr)
	}
	t.Logf("foreign-capture control: a genuine foreign bound capture (distinct real instance ids %s / %s) was refused at the early fixture/provenance precondition; this early mismatch is not credited as an isolated InstanceID comparison", original.OriginalInstanceID(), other.OriginalInstanceID())
	borrowedSuccessorCloseConn(t, conn)
}

// borrowedBoundSessionUnknownPermanence is the in-lane UNKNOWN permanence
// control on the SAME genuine bound capture: injected strict stat/census
// unknown faults installed BEFORE work refuse with zero probe executions while
// the connection stays demonstrably live (UNKNOWN is not disappearance and is
// never classified as drift or success); the pre-fault retained copy and the
// constructor reconstruction are permanently refused with a nil registration.
func borrowedBoundSessionUnknownPermanence(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding) {
	t.Helper()
	for _, kind := range []string{"stat", "census"} {
		conn, reg := borrowedReplacementSessionRegisterP1(t, ctx, b, fresh)
		retained := *reg // retained BEFORE the injected unknown fault
		if kind == "stat" {
			reg.state.statFn = func(context.Context, *borrowedReplacementSessionState) (string, uint64, error) {
				return "", 0, errors.New("injected unknown strict stat failure")
			}
		} else {
			reg.state.associateFn = func(context.Context, *borrowedReplacementSessionState) (borrowedAuthStagingBackendToken, error) {
				return borrowedAuthStagingBackendToken{}, errors.New("injected unknown strict census failure")
			}
		}
		useCtx, cancelUse := context.WithTimeout(ctx, 60*time.Second)
		useErr := reg.Use(useCtx)
		cancelUse()
		if useErr == nil {
			t.Fatalf("in-lane unknown %s failure was accepted as a use", kind)
		}
		if errors.Is(useErr, errBorrowedReplacementSessionVerifierDrift) || errors.Is(useErr, errBorrowedReplacementSessionRoleFactsDrift) {
			t.Fatalf("in-lane unknown %s failure was misclassified as drift: %v", kind, useErr)
		}
		if got := atomic.LoadInt32(&reg.state.probeExecutions); got != 0 {
			t.Fatalf("in-lane unknown %s failure executed the probe %d times, want 0", kind, got)
		}
		if _, liveErr := borrowedSuccessorReadConnIdentity(ctx, conn); liveErr != nil {
			t.Fatalf("in-lane unknown %s failure is not distinguishable from disappearance: %v", kind, liveErr)
		}
		freshCtx, cancelFresh := context.WithTimeout(ctx, 30*time.Second)
		if retained.Use(freshCtx) == nil {
			t.Fatalf("pre-fault in-lane unknown %s copy rehabilitated the registration", kind)
		}
		reconReg, reconErr := newBorrowedReplacementSessionRegistration(t, freshCtx, b, fresh, conn)
		cancelFresh()
		if reconReg != nil || reconErr == nil {
			t.Fatalf("in-lane unknown %s reconstruction minted a registration (reg=%v err=%v)", kind, reconReg, reconErr)
		}
		prefixCopy := *fresh.prefix
		freshCopy := *fresh
		freshCopy.prefix = &prefixCopy
		wrapperReg, wrapperErr := newBorrowedReplacementSessionRegistration(t, ctx, b, &freshCopy, conn)
		if wrapperReg != nil || wrapperErr == nil {
			t.Fatalf("in-lane unknown %s wrapper reconstruction minted a registration (reg=%v err=%v)", kind, wrapperReg, wrapperErr)
		}
		borrowedSuccessorCloseConn(t, conn)
	}
	t.Logf("in-lane UNKNOWN permanence control: injected strict stat and census unknown faults refused with zero probe executions while the connection stayed live; the pre-fault copies and constructor reconstructions were permanently refused with nil registrations")
}

// borrowedBoundSessionWrongIdentityNilRefuse adds the direct in-lane nil
// registration assertions for the real wrong-role, wrong-database and foreign
// owned fixture connections: the constructor must return registration == nil
// together with the expected role/database refusal stage, so a forbidden
// usable publication beside the error can never pass.
func borrowedBoundSessionWrongIdentityNilRefuse(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding) {
	t.Helper()
	observerConn := borrowedOwnerDDLConnect(t, ctx, b.fixture.observerTargetDSN)
	observerReg, observerErr := newBorrowedReplacementSessionRegistration(t, ctx, b, fresh, observerConn)
	if observerReg != nil || observerErr == nil || !strings.Contains(observerErr.Error(), "role/database is not the W replacement target") {
		t.Fatalf("wrong-role connection minted a registration or missed its refusal stage (reg=%v err=%v)", observerReg, observerErr)
	}
	sourceConn := borrowedOwnerDDLConnect(t, ctx, borrowedAuthRoleDSN(t, b.fixture.writerSourceDSN, b.fixture.writerRole, b.passwordP1))
	sourceReg, sourceErr := newBorrowedReplacementSessionRegistration(t, ctx, b, fresh, sourceConn)
	if sourceReg != nil || sourceErr == nil || !strings.Contains(sourceErr.Error(), "role/database is not the W replacement target") {
		t.Fatalf("wrong-database connection minted a registration or missed its refusal stage (reg=%v err=%v)", sourceReg, sourceErr)
	}
	secondFx := newOriginGateFixture(t)
	foreignDSN := borrowedAuthRoleDSN(t, secondFx.dsn, secondFx.role, secondFx.password)
	foreignConn := borrowedOwnerDDLConnect(t, ctx, foreignDSN)
	foreignConnReg, foreignConnErr := newBorrowedReplacementSessionRegistration(t, ctx, b, fresh, foreignConn)
	if foreignConnReg != nil || foreignConnErr == nil || !strings.Contains(foreignConnErr.Error(), "role/database is not the W replacement target") {
		t.Fatalf("foreign owned fixture connection minted a registration or missed its refusal stage (reg=%v err=%v)", foreignConnReg, foreignConnErr)
	}
	borrowedSuccessorCloseConn(t, observerConn)
	borrowedSuccessorCloseConn(t, sourceConn)
	borrowedSuccessorCloseConn(t, foreignConn)
	t.Logf("in-lane registration identity nil control: real wrong-role, wrong-database and foreign owned fixture connections returned nil registrations with the expected role/database refusal stage")
}

// borrowedBoundSessionTerminationSubstitutionRefuse proves the exact
// registered incarnation termination refuses use before SELECT with zero probe
// executions while a live real same-role/database lookalike connection is
// present, and that the lost registration cannot substitute or recover.
func borrowedBoundSessionTerminationSubstitutionRefuse(t *testing.T, ctx context.Context) {
	t.Helper()
	b, fresh := newBorrowedBoundSessionCapture(t, ctx)
	connA, regA := borrowedReplacementSessionRegisterP1(t, ctx, b, fresh)
	connB, regB := borrowedReplacementSessionRegisterP1(t, ctx, b, fresh)
	if regA.state == regB.state {
		t.Fatal("distinct bound session connections shared one registration state")
	}
	identityB, identityErr := borrowedSuccessorReadConnIdentity(ctx, connB)
	if identityErr != nil {
		t.Fatalf("bound session lookalike identity read: %v", identityErr)
	}
	if identityB.backendPID == regA.state.backendPID {
		t.Fatal("bound session lookalike shares the registered session PID")
	}
	if identityB.roleOID != regA.state.roleOID || identityB.targetDBOID != regA.state.targetDBOID {
		t.Fatal("bound session lookalike is not the same-role/database connection")
	}
	identityA, identityAErr := borrowedSuccessorReadConnIdentity(ctx, connA)
	if identityAErr != nil {
		t.Fatalf("bound session registered identity read refused: %v", identityAErr)
	}
	if identityA.backendPID != regA.state.backendPID || !identityA.backendStart.Equal(regA.state.backendStart) ||
		identityA.roleName != b.fixture.writerRole || identityA.databaseName != b.fixture.targetDB {
		t.Fatalf("bound session registered identity is not the exact W incarnation: %+v", identityA)
	}
	retainedA := *regA // retained BEFORE the fault
	var matched int
	var terminated bool
	termCtx, cancelTerm := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	termErr := b.fixture.fx.admin.QueryRow(termCtx, `
WITH target AS (
  SELECT pid, pg_terminate_backend(pid) AS terminated
  FROM pg_stat_activity
  WHERE pid=$1 AND backend_start=$2 AND usename=$3 AND datname=$4
)
SELECT count(*), coalesce(bool_or(terminated), false) FROM target`,
		identityA.backendPID, identityA.backendStart, identityA.roleName, identityA.databaseName).Scan(&matched, &terminated)
	cancelTerm()
	if termErr != nil || matched != 1 || !terminated {
		t.Fatalf("terminate exact registered bound session incarnation (pid=%d start=%s role=%s database=%s): matched=%d terminated=%t err=%v",
			identityA.backendPID, identityA.backendStart, identityA.roleName, identityA.databaseName, matched, terminated, termErr)
	}
	borrowedAuthStagingAwaitDisappearance(ctx, t, b.fixture.fx.containerID, regA.state.backendPID, regA.state.osStart, borrowedAuthStagingDisappearanceBudget, true)
	useCtx, cancelUse := context.WithTimeout(ctx, 60*time.Second)
	useErr := regA.Use(useCtx)
	cancelUse()
	if useErr == nil {
		t.Fatal("terminated bound session was used successfully")
	}
	if errors.Is(useErr, errBorrowedReplacementSessionVerifierDrift) || errors.Is(useErr, errBorrowedReplacementSessionRoleFactsDrift) {
		t.Fatalf("bound session termination was misclassified as drift: %v", useErr)
	}
	if got := atomic.LoadInt32(&regA.state.probeExecutions); got != 0 {
		t.Fatalf("terminated bound session executed the probe %d times, want 0", got)
	}
	lookCtx, cancelLook := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	lookState, lookStart, lookErr := borrowedAuthStagingContainerStat(lookCtx, b.fixture.fx.containerID, identityB.backendPID)
	cancelLook()
	if lookErr != nil || lookStart != regB.state.osStart || !borrowedAuthStagingLiveState(lookState) {
		t.Fatalf("lookalike bound session is not the live real connection: state=%q err=%v", lookState, lookErr)
	}
	freshCtx, cancelFresh := context.WithTimeout(ctx, 30*time.Second)
	if retainedA.Use(freshCtx) == nil {
		t.Fatal("pre-fault terminated bound session copy rehabilitated the registration")
	}
	if reconReg, reconErr := newBorrowedReplacementSessionRegistration(t, freshCtx, b, fresh, connA); reconReg != nil || reconErr == nil {
		t.Fatalf("terminated bound session constructor-reconstruction minted a registration (reg=%v err=%v)", reconReg, reconErr)
	}
	cancelFresh()
	borrowedSuccessorCloseConn(t, connA)
	borrowedSuccessorCloseConn(t, connB)
	t.Logf("bound session termination/substitution control: the exact registered incarnation (pid=%d start=%d) was terminated under exact PID/backend_start/role/database predicates with exactly one match and strictly proven gone, use refused before SELECT with zero probe executions, the pre-fault copy and constructor-reconstruction permanently refused, the live same-role/database lookalike could not substitute and the lost registration could not recover", regA.state.backendPID, regA.state.osStart)
}

// borrowedBoundSessionOwnerLossRefuse proves real original control-owner loss
// (exact owner incarnation terminated) refuses use before SELECT with zero
// probe executions, refuses Health and the original anchor, and accepts no
// reacquisition or replacement registration.
func borrowedBoundSessionOwnerLossRefuse(t *testing.T, ctx context.Context) {
	t.Helper()
	b, fresh := newBorrowedBoundSessionCapture(t, ctx)
	conn, reg := borrowedReplacementSessionRegisterP1(t, ctx, b, fresh)
	controlPID := fresh.binding.ControlBackendPID()
	if controlPID <= 0 || controlPID != b.owner.BackendPID {
		t.Fatalf("owner-loss captured control owner PID %d is missing or not the anchored owner PID %d", controlPID, b.owner.BackendPID)
	}
	var ownerMatched int
	var ownerTerminated bool
	lossCtx, cancelLoss := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	lossErr := b.fixture.controlPool.QueryRow(lossCtx, `
WITH target AS (
  SELECT pid, pg_terminate_backend(pid) AS terminated
  FROM pg_stat_activity
  WHERE pid=$1 AND backend_start=$2 AND usename=$3 AND datname=$4
)
SELECT count(*), coalesce(bool_or(terminated), false) FROM target`,
		controlPID, b.owner.BackendStart, b.fixture.controlRole, b.fixture.controlDB).Scan(&ownerMatched, &ownerTerminated)
	cancelLoss()
	if lossErr != nil || ownerMatched != 1 || !ownerTerminated {
		t.Fatalf("terminate exact bound session control owner incarnation (pid=%d start=%s role=%s database=%s): matched=%d terminated=%t err=%v",
			controlPID, b.owner.BackendStart, b.fixture.controlRole, b.fixture.controlDB, ownerMatched, ownerTerminated, lossErr)
	}
	ownerGone := false
	ownerGoneDeadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(ownerGoneDeadline) {
		var present int
		rowCtx, cancelRow := context.WithTimeout(ctx, 5*time.Second)
		rowErr := b.fixture.controlPool.QueryRow(rowCtx, `
SELECT count(*) FROM pg_stat_activity WHERE pid=$1 AND backend_start=$2 AND usename=$3 AND datname=$4`,
			controlPID, b.owner.BackendStart, b.fixture.controlRole, b.fixture.controlDB).Scan(&present)
		cancelRow()
		if rowErr != nil {
			t.Fatalf("bound session owner disappearance read refused: %v", rowErr)
		}
		if present == 0 {
			ownerGone = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !ownerGone {
		t.Fatal("bound session owner incarnation did not positively disappear within the bounded window")
	}
	useCtx, cancelUse := context.WithTimeout(ctx, 60*time.Second)
	useErr := reg.Use(useCtx)
	cancelUse()
	if useErr == nil {
		t.Fatal("bound session use succeeded after real control owner loss")
	}
	if errors.Is(useErr, errBorrowedReplacementSessionVerifierDrift) || errors.Is(useErr, errBorrowedReplacementSessionRoleFactsDrift) {
		t.Fatalf("bound session owner loss was misclassified as drift: %v", useErr)
	}
	if got := atomic.LoadInt32(&reg.state.probeExecutions); got != 0 {
		t.Fatalf("bound session owner loss executed the probe %d times, want 0", got)
	}
	healthCtx, cancelHealth := context.WithTimeout(ctx, borrowedOwnerRotationHealthBudget)
	healthErr := b.fixture.lock.Health(healthCtx)
	cancelHealth()
	if healthErr == nil {
		t.Fatal("bound session original control owner health remained successful after real owner loss")
	}
	anchorCtx, cancelAnchor := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	anchorErr := b.anchor.Recheck(anchorCtx)
	cancelAnchor()
	if anchorErr == nil {
		t.Fatal("bound session retained original anchor rechecked after real owner loss")
	}
	// No reacquisition/replacement: a NEW genuine connection cannot mint a
	// registration after the owner loss.
	newConn := borrowedReplacementSessionConnectP1(t, ctx, b)
	if reacqReg, reacqErr := newBorrowedReplacementSessionRegistration(t, ctx, b, fresh, newConn); reacqReg != nil || reacqErr == nil {
		t.Fatalf("owner loss accepted a replacement bound session registration (reg=%v err=%v)", reacqReg, reacqErr)
	}
	copied := *reg
	freshCtx, cancelFresh := context.WithTimeout(ctx, 30*time.Second)
	if copied.Use(freshCtx) == nil {
		t.Fatal("owner-loss bound session copy rehabilitated the registration")
	}
	cancelFresh()
	borrowedSuccessorCloseConn(t, newConn)
	borrowedSuccessorCloseConn(t, conn)
	t.Logf("bound session owner-loss control: the exact owner incarnation (pid=%d start=%s role=%s database=%s) was terminated under exact predicates with exactly one match and positively proven absent, use refused before SELECT with zero probe executions, Health and the original anchor were refused, and no reacquisition or replacement registration was accepted", controlPID, b.owner.BackendStart, b.fixture.controlRole, b.fixture.controlDB)
}

// borrowedBoundSessionVerifierDriftRefuse proves a real committed verifier
// drift with unchanged role facts is classified with the SPECIFIC
// verifier-drift error, with zero probe executions and permanent refusal.
func borrowedBoundSessionVerifierDriftRefuse(t *testing.T, ctx context.Context) {
	t.Helper()
	b, fresh := newBorrowedBoundSessionCapture(t, ctx)
	conn, reg := borrowedReplacementSessionRegisterP1(t, ctx, b, fresh)
	driftPassword := borrowedAuthCredential(t, "bound session verifier drift")
	driftVerifier, genErr := generateSCRAMVerifier(driftPassword, 4096)
	if genErr != nil {
		t.Fatalf("bound session verifier drift generation refused: %v", genErr)
	}
	driftCtx, cancelDrift := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	_, driftErr := b.fixture.fx.admin.Exec(driftCtx, `ALTER ROLE `+pgx.Identifier{b.fixture.writerRole}.Sanitize()+` PASSWORD `+sqlLiteral(driftVerifier))
	cancelDrift()
	if driftErr != nil {
		t.Fatalf("bound session verifier drift mutation refused: %v", driftErr)
	}
	current, readErr := borrowedSuccessorReadCatalogFacts(ctx, b.fixture)
	if readErr != nil {
		t.Fatalf("bound session verifier drift exact catalog read: %v", readErr)
	}
	if !borrowedFenceGapRoleStateEqual(current, b.preState) {
		t.Fatal("bound session verifier drift control changed the immutable role facts")
	}
	if subtle.ConstantTimeCompare([]byte(current.verifier), []byte(b.verifierP1)) == 1 {
		t.Fatal("bound session verifier drift control did not change the committed verifier")
	}
	useCtx, cancelUse := context.WithTimeout(ctx, 60*time.Second)
	useErr := reg.Use(useCtx)
	cancelUse()
	if !errors.Is(useErr, errBorrowedReplacementSessionVerifierDrift) {
		t.Fatalf("bound session verifier drift was not refused with the specific verifier-drift error: %v", useErr)
	}
	if errors.Is(useErr, errBorrowedReplacementSessionRoleFactsDrift) {
		t.Fatal("bound session verifier drift was misclassified as role-facts drift")
	}
	if got := atomic.LoadInt32(&reg.state.probeExecutions); got != 0 {
		t.Fatalf("bound session verifier drift executed the probe %d times, want 0", got)
	}
	copied := *reg
	if copied.Use(ctx) == nil {
		t.Fatal("bound session verifier-drift copy rehabilitated the registration")
	}
	borrowedSuccessorCloseConn(t, conn)
	borrowedOwnerDDLGuard(t, ctx, b)
	t.Logf("bound session verifier-drift control: the committed verifier drifted with unchanged role facts and was refused with the specific verifier-drift classification and zero probe executions")
}

// borrowedBoundSessionRoleDriftRefuse proves a real committed role-fact drift
// with the expected P1 verifier is classified with the SPECIFIC role-facts
// drift error first, never the verifier-drift error, with zero probe
// executions and permanent refusal.
func borrowedBoundSessionRoleDriftRefuse(t *testing.T, ctx context.Context) {
	t.Helper()
	b, fresh := newBorrowedBoundSessionCapture(t, ctx)
	conn, reg := borrowedReplacementSessionRegisterP1(t, ctx, b, fresh)
	driftCtx, cancelDrift := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	_, driftErr := b.fixture.fx.admin.Exec(driftCtx, `ALTER ROLE `+pgx.Identifier{b.fixture.writerRole}.Sanitize()+` CREATEDB`)
	cancelDrift()
	if driftErr != nil {
		t.Fatalf("bound session role-facts drift mutation refused: %v", driftErr)
	}
	current, readErr := borrowedSuccessorReadCatalogFacts(ctx, b.fixture)
	if readErr != nil {
		t.Fatalf("bound session role-facts drift exact catalog read: %v", readErr)
	}
	if current.createdb == b.preState.createdb {
		t.Fatal("bound session role-facts drift mutation did not change the flag")
	}
	if subtle.ConstantTimeCompare([]byte(current.verifier), []byte(b.verifierP1)) != 1 {
		t.Fatal("bound session role-facts drift control changed the committed P1 verifier")
	}
	useCtx, cancelUse := context.WithTimeout(ctx, 60*time.Second)
	useErr := reg.Use(useCtx)
	cancelUse()
	if !errors.Is(useErr, errBorrowedReplacementSessionRoleFactsDrift) {
		t.Fatalf("bound session role-facts drift was not refused with the specific role-facts drift error: %v", useErr)
	}
	if errors.Is(useErr, errBorrowedReplacementSessionVerifierDrift) {
		t.Fatal("bound session role-facts drift was misclassified as verifier drift")
	}
	if got := atomic.LoadInt32(&reg.state.probeExecutions); got != 0 {
		t.Fatalf("bound session role-facts drift executed the probe %d times, want 0", got)
	}
	copied := *reg
	if copied.Use(ctx) == nil {
		t.Fatal("bound session role-facts-drift copy rehabilitated the registration")
	}
	borrowedSuccessorCloseConn(t, conn)
	borrowedOwnerDDLGuard(t, ctx, b)
	t.Logf("bound session role-facts-drift control: the committed CREATEDB drift with the expected P1 verifier was refused with the specific role-facts classification and zero probe executions")
}

// borrowedBoundSessionSecondReplacementRefuse proves a separately controlled
// second-target replacement invalidates the old capture/session permanently
// (zero probe executions, shared prefix loss propagated to copies) and that a
// NEW genuine connection cannot be accepted as a replacement incarnation.
func borrowedBoundSessionSecondReplacementRefuse(t *testing.T, ctx context.Context) {
	t.Helper()
	b, fresh := newBorrowedBoundSessionCapture(t, ctx)
	conn, reg := borrowedReplacementSessionRegisterP1(t, ctx, b, fresh)
	borrowedSuccessorCloseConn(t, conn)
	borrowedAuthStagingAwaitDisappearance(ctx, t, b.fixture.fx.containerID, reg.state.backendPID, reg.state.osStart, borrowedAuthStagingDisappearanceBudget, true)
	borrowedOwnerDDLExternalReplace(t, ctx, b)
	replacedOID, replaceErr := borrowedOwnerDDLTargetOID(ctx, b)
	if replaceErr != nil || replacedOID == fresh.replacementOID {
		t.Fatalf("bound session second replacement did not change the target database OID: oid=%d err=%v", replacedOID, replaceErr)
	}
	useCtx, cancelUse := context.WithTimeout(ctx, 60*time.Second)
	useErr := reg.Use(useCtx)
	cancelUse()
	if useErr == nil {
		t.Fatal("bound session was used after a second replacement")
	}
	if got := atomic.LoadInt32(&reg.state.probeExecutions); got != 0 {
		t.Fatalf("bound session second replacement executed the probe %d times, want 0", got)
	}
	if invalid, reason := fresh.prefix.Invalid(); !invalid || reason == "" {
		t.Fatal("bound session second replacement did not permanently invalidate the shared prefix")
	}
	prefixCopy := *fresh.prefix
	if invalid, _ := prefixCopy.Invalid(); !invalid {
		t.Fatal("bound session second-replacement prefix loss is not shared with the prefix copy")
	}
	copied := *reg
	if copied.Use(ctx) == nil {
		t.Fatal("bound session second-replacement copy rehabilitated the registration")
	}
	// No accepted new incarnation: a NEW genuine connection cannot mint a
	// replacement registration over the invalidated old capture.
	newConn := borrowedReplacementSessionConnectP1(t, ctx, b)
	if newReg, newErr := newBorrowedReplacementSessionRegistration(t, ctx, b, fresh, newConn); newReg != nil || newErr == nil {
		t.Fatalf("second replacement accepted a new bound session incarnation (reg=%v err=%v)", newReg, newErr)
	}
	borrowedSuccessorCloseConn(t, newConn)
	borrowedOwnerDDLGuard(t, ctx, b)
	t.Logf("bound session second-replacement control: the old capture/session permanently refused with zero probe executions and the shared prefix loss propagated to copies; no new incarnation was accepted")
}

// TestBorrowedReplacementBoundSessionRegistration is the bounded single-chain
// instance-bound retained-session registration/use/retirement lane described in
// the file header.
func TestBorrowedReplacementBoundSessionRegistration(t *testing.T) {
	ctx := t.Context()

	// Positive: the single chain over ONE authentic bound baseline, the
	// EXISTING replacement orchestration, one real registration/use/retirement
	// and the snapshot comparison.
	func() {
		b, fresh := newBorrowedBoundSessionCapture(t, ctx)
		f := b.fixture
		borrowedBoundSessionAssertProvenance(t, ctx, b, fresh, "positive")

		// Counter distinctions: the baseline intentional restore-probe refusal
		// ran once, acceptance never ran, and the replacement coordinator was
		// never invoked (no child, no restore-probe callback).
		if got := atomic.LoadInt32(&f.probeCalls); got != 1 {
			t.Fatalf("bound session baseline intentional probe refusals=%d, want 1", got)
		}
		if got := atomic.LoadInt32(&f.acceptanceCalls); got != 0 {
			t.Fatalf("bound session baseline ran acceptance %d times", got)
		}
		replacementIdentity := fresh.run.Observation().StartedIdentity()
		if replacementIdentity.Started || replacementIdentity.PID != 0 {
			t.Fatalf("bound session replacement coordinator observed a child: %+v", replacementIdentity)
		}

		// Snapshot after the capture, before registration: complete guard-row
		// hash, persistent instance identity, generation/hash, NULL inventory.
		snap0 := borrowedBoundSessionSnapshotNow(t, ctx, b)

		// Retain ONE P1 connection (with its immediate independent bounded
		// disposal) and register that exact connection.
		conn := borrowedReplacementSessionConnectP1(t, ctx, b)
		reg, regErr := newBorrowedReplacementSessionRegistration(t, ctx, b, fresh, conn)
		if regErr != nil {
			t.Fatalf("bound session registration refused: %v", regErr)
		}
		state := reg.state
		borrowedBoundSessionAssertRegistrationRetained(t, ctx, b, fresh, conn, reg, "positive")

		// Equivalent prefix wrapper over the SHARED state reuses the exact slot
		// state (never the wrapper address).
		prefixCopy := *fresh.prefix
		freshCopy := *fresh
		freshCopy.prefix = &prefixCopy
		equivalent, equivalentErr := newBorrowedReplacementSessionRegistration(t, ctx, b, &freshCopy, conn)
		if equivalentErr != nil {
			t.Fatalf("equivalent bound prefix wrapper was not reused: %v", equivalentErr)
		}
		if equivalent.state != state {
			t.Fatal("equivalent bound prefix wrapper did not reuse the shared slot state")
		}

		// Retain a copy BEFORE the successful use.
		retainedBeforeUse := *reg

		useCtx, cancelUse := context.WithTimeout(ctx, 60*time.Second)
		useErr := reg.Use(useCtx)
		cancelUse()
		if useErr != nil {
			t.Fatalf("bound session use refused: %v", useErr)
		}
		if got := atomic.LoadInt32(&state.probeExecutions); got != 1 {
			t.Fatalf("bound session probe executions=%d, want exactly 1", got)
		}
		if got := atomic.LoadInt32(&state.identityQueries); got == 0 {
			t.Fatal("bound session identity-discovery queries were not counted separately")
		}
		t.Logf("bound session registered+used: backend PID %d probe executions=%d identity-discovery queries=%d", state.backendPID, atomic.LoadInt32(&state.probeExecutions), atomic.LoadInt32(&state.identityQueries))

		// Replay refusal: same handle, the pre-fault copy, reconstruction and
		// prefix-wrapper reconstruction; the probe count stays exactly one.
		replayCtx, cancelReplay := context.WithTimeout(ctx, 30*time.Second)
		replayErr := reg.Use(replayCtx)
		copyUseErr := retainedBeforeUse.Use(replayCtx)
		cancelReplay()
		if replayErr == nil || copyUseErr == nil {
			t.Fatal("bound session replay/copy use was accepted")
		}
		if reconReg, reconErr := newBorrowedReplacementSessionRegistration(t, ctx, b, fresh, conn); reconReg != nil || reconErr == nil {
			t.Fatalf("constructor replay after a successful bound use minted a fresh registration (reg=%v err=%v)", reconReg, reconErr)
		}
		if wrapperReg, wrapperReplayErr := newBorrowedReplacementSessionRegistration(t, ctx, b, &freshCopy, conn); wrapperReg != nil || wrapperReplayErr == nil {
			t.Fatalf("prefix-wrapper reconstruction after a successful bound use minted a fresh registration (reg=%v err=%v)", wrapperReg, wrapperReplayErr)
		}
		if got := atomic.LoadInt32(&state.probeExecutions); got != 1 {
			t.Fatalf("bound session replay executed the probe: executions=%d", got)
		}

		// The single successful use adds no guard/instance/evidence change.
		borrowedBoundSessionAssertSnapshotEqual(t, "bound session use", snap0, borrowedBoundSessionSnapshotNow(t, ctx, b))

		// Concurrent second Use while the first is paused at the real
		// pre-probe barrier: the real in-progress reservation refuses it. The
		// call-domain unwind is installed immediately so every early return
		// cancels, releases and joins the actual completion before continuing.
		func() {
			conn2, reg2 := borrowedReplacementSessionRegisterP1(t, ctx, b, fresh)
			concurrentRetained := *reg2
			seam2 := installBorrowedReplacementSessionSeam(t, reg2, borrowedReplacementSessionStageUsePublish)
			concurrentCtx, cancelConcurrent := context.WithCancel(ctx)
			concurrentDone := make(chan error, 1)
			go func() { concurrentDone <- reg2.Use(concurrentCtx) }()
			concurrentJoined := false
			concurrentUnwind := func() {
				cancelConcurrent()
				seam2.releaseSeam()
				if concurrentJoined {
					return
				}
				select {
				case <-concurrentDone:
					concurrentJoined = true
				case <-time.After(30 * time.Second):
					t.Errorf("bound concurrent use completion is unknown after the bounded unwind")
				}
			}
			defer concurrentUnwind()
			t.Cleanup(concurrentUnwind)
			select {
			case <-seam2.entered:
			case <-time.After(60 * time.Second):
				t.Errorf("bound concurrent use never reached the real pre-probe barrier; completion is unknown")
				return
			}
			secondUseCtx, cancelSecondUse := context.WithTimeout(ctx, 30*time.Second)
			secondUseErr := reg2.Use(secondUseCtx)
			cancelSecondUse()
			if secondUseErr == nil || !strings.Contains(secondUseErr.Error(), "already being used") {
				t.Fatalf("bound concurrent second use was not refused as already being used: %v", secondUseErr)
			}
			seam2.releaseSeam()
			select {
			case firstConcurrentErr := <-concurrentDone:
				concurrentJoined = true
				if firstConcurrentErr != nil {
					t.Fatalf("first concurrent bound use refused: %v", firstConcurrentErr)
				}
			case <-time.After(30 * time.Second):
				t.Errorf("first concurrent bound use did not complete within the bounded join; outcome unknown")
				return
			}
			if got := atomic.LoadInt32(&reg2.state.probeExecutions); got != 1 {
				t.Fatalf("bound concurrent use probe executions=%d, want exactly 1", got)
			}
			concurrentReplyCtx, cancelConcurrentReply := context.WithTimeout(ctx, 30*time.Second)
			if concurrentRetained.Use(concurrentReplyCtx) == nil {
				t.Fatal("pre-fault concurrent copy rehabilitated the bound registration")
			}
			cancelConcurrentReply()
			t.Logf("bound concurrent replay control: the real in-progress reservation refused the second use and the first published exactly one probe")
			borrowedReplacementSessionRetire(t, ctx, b, conn2, reg2.state)
		}()

		// Retire through the existing retirement path: strict incarnation
		// disappearance, owner health, guard non-clean, acceptance zero.
		borrowedReplacementSessionRetire(t, ctx, b, conn, state)
		if _, closedIdentityErr := borrowedSuccessorReadConnIdentity(ctx, conn); closedIdentityErr == nil {
			t.Fatal("retired bound session connection still reads an identity")
		}
		healthCtx, cancelHealth := context.WithTimeout(ctx, borrowedOwnerRotationHealthBudget)
		healthErr := b.fixture.lock.Health(healthCtx)
		cancelHealth()
		if healthErr != nil {
			t.Fatalf("bound session retire original control owner health: %v", healthErr)
		}
		anchorCtx, cancelAnchor := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
		anchorErr := b.anchor.Recheck(anchorCtx)
		cancelAnchor()
		if anchorErr != nil {
			t.Fatalf("bound session retire original anchor recheck: %v", anchorErr)
		}
		borrowedBoundSessionAssertProvenance(t, ctx, b, fresh, "post-retirement")
		borrowedBoundSessionAssertSnapshotEqual(t, "bound session retirement", snap0, borrowedBoundSessionSnapshotNow(t, ctx, b))

		// Non-destructive registration identity and UNKNOWN controls on the
		// same genuine bound capture.
		borrowedReplacementSessionWrongIdentityRefuse(t, ctx, b, fresh)
		borrowedBoundSessionWrongIdentityNilRefuse(t, ctx, b, fresh)
		borrowedBoundSessionForeignCaptureRefuse(t, ctx, b, fresh)
		borrowedReplacementSessionUnknownRefuse(t, ctx, b, fresh)
		borrowedBoundSessionUnknownPermanence(t, ctx, b, fresh)
		borrowedBoundSessionAssertSnapshotEqual(t, "bound session non-destructive controls", snap0, borrowedBoundSessionSnapshotNow(t, ctx, b))
	}()

	// Destructive controls on independent bound fixtures.
	borrowedBoundSessionCancelControl(t, ctx, false)
	borrowedBoundSessionCancelControl(t, ctx, true)
	borrowedBoundSessionPrefixLossControl(t, ctx, false)
	borrowedBoundSessionPrefixLossControl(t, ctx, true)
	borrowedBoundSessionTerminationSubstitutionRefuse(t, ctx)
	borrowedBoundSessionOwnerLossRefuse(t, ctx)
	borrowedBoundSessionVerifierDriftRefuse(t, ctx)
	borrowedBoundSessionRoleDriftRefuse(t, ctx)
	borrowedBoundSessionSecondReplacementRefuse(t, ctx)
}
