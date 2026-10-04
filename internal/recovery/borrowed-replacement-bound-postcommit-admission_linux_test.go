//go:build linux && drill

// borrowed-replacement-bound-postcommit-admission_linux_test.go is the bounded
// bound postcommit-admission lane (NON-AUTHORIZING). It composes ONLY existing,
// unchanged primitives: the genuine bound capture with the single-consumed
// receipt/entry, the exact bound dirty-guard coordinator refusal whose
// preparation transaction PID is captured through the existing marker seam, the
// unchanged readiness machinery consumed ONCE as NON-AUTHORIZING lineage facts,
// the strict registered-P1 retirement, the unchanged writer-isolation flow and
// its live fence interval (through the existing duringProof hook), the
// unchanged baseline-commit stage (which performs the controlled-baseline
// preparation and its acknowledged COMMIT through the retained lock's
// WithTransaction), and the unchanged native coordinator dispatch (preflight
// child-argument hook + counted transactional restore_started marker injection
// + launch-intent commit + launch-stage hook).
//
// On ONE authentic bound fixture with the original retained owner, original
// instance and actual replacement, the lane proves the single chain: genuine
// bound capture -> exact dirty-guard refusal (coordinator preparation PID
// captured) -> readiness consumed once as lineage facts -> strict P1
// retirement -> prepare ONE fresh bound native attempt (descriptor-verified
// archive + authentic instance + genuine counted marker + unique operation +
// rejecting probe/acceptance callbacks) BEFORE the writer fence -> enter the
// unchanged writer-isolation flow -> inside its synchronous duringProof hook
// invoke the unchanged baseline-commit stage run once -> require the explicitly
// ACKNOWLEDGED commit and independently verify the committed preparation
// audit/guard delta -> WITHOUT returning from duringProof freshly revalidate
// the effective fence, every real route refusal, the retained-writer census,
// observer/control usability, owner/anchor, replacement catalog, shared prefix
// and authentic instance binding -> arm the lane-local postcommit-admission
// handoff gate from the ACKNOWLEDGED classification (never from visible rows)
// -> dispatch the one prepared native coordinator attempt: its counted marker
// injection repeats live/provenance checks on the genuine supplied preparation
// transaction (PID distinct from the retained owner, instance re-locked and
// revalidated), delegates to the genuine transactional restore_started marker,
// and ONLY THEN closes the attempt's existing endpoint listener, so the
// unchanged launch-stage endpoint revalidation refuses BEFORE any child starts.
// The lane requires the ACTUAL launch-stage endpoint-capability refusal and
// independently proves the marker/prelaunch admission transaction committed,
// the guard conservatively UNKNOWN with durable launch intent (exact native
// operation/application, NO launched timestamp, clean/rebuild fields
// legitimately replaced by the native preparation), the baseline preparation
// audit byte-identical and unrelabelled, zero child start, absent process
// receipt, zero probe/acceptance callbacks, no accepted output and the target
// catalog/data unchanged; the still-live fence is rechecked before leaving
// duringProof, the unchanged cleanup restores the HBA, and the lane verifies
// the restoration, the original-owner health and NO continuing isolation.
//
// The positive outcome is exactly "bounded native preparation admission
// witnessed; launch deliberately refused". It is never a successful Run: no
// ordinary native launch, no successful restore probe, no atomic acceptance,
// no completed-restoration evidence, no manifest, no downstream and no Gate1
// authority. Native preparation legitimately replaces the clean-guard
// preparation fields, so the lane never demands the old clean-guard delta back
// and never repairs committed history; the historical preparation audit is
// preserved and the new durable intent is classified on its own terms. No
// existing source is modified, no coordinator/transaction machinery is copied,
// no callback-side commit exists, no evidence/token is fabricated, no direct
// guard-update SQL exists, no artificial clean seed exists, no owner is
// reacquired, no second instance is opened, no replacement fixture is created,
// no new capability API or seam exists, and rebuildTargetWithWitness stays
// unmodified and intentionally refusing. The lane-local handoff gate is NOT new
// production fence enforcement and the lane never claims that arbitrary
// concurrent HBA mutation is atomically coupled to the native launch. Secrets,
// verifiers, DSNs and archive contents are never logged.
package recovery_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"reflect"

	"github.com/jackc/pgx/v5"

	"github.com/xtianxx/txharbor/internal/recovery"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

// Bound postcommit-admission refusal stages: every control captures and asserts
// the ACTUAL real stage instead of any non-nil error.
var (
	errBoundPostcommitAdmissionBaseline   = errors.New("bound postcommit admission: the handoff prerequisite was not an explicitly acknowledged baseline commit")
	errBoundPostcommitAdmissionUnarmed    = errors.New("bound postcommit admission: no acknowledged baseline armed this handoff")
	errBoundPostcommitAdmissionTerminal   = errors.New("bound postcommit admission: the live isolation interval is over")
	errBoundPostcommitAdmissionReplay     = errors.New("bound postcommit admission: the single native dispatch was already consumed")
	errBoundPostcommitAdmissionRevalidate = errors.New("bound postcommit admission: the post-commit fence/lineage revalidation refused")
	errBoundPostcommitAdmissionLaunch     = errors.New("bound postcommit admission: the native coordinator did not refuse at the launch-stage endpoint revalidation")
	errBoundPostcommitAdmissionSource     = errors.New("bound postcommit admission: the prepared native attempt did not refuse at its real source stage")
	errBoundPostcommitAdmissionCancelled  = errors.New("bound postcommit admission: the lane context ended at the recorded stage")
	errBoundPostcommitAdmissionEndpoint   = errors.New("bound postcommit admission: the endpoint control did not refuse at its real preflight stage")
	errBoundPostcommitAdmissionDurable    = errors.New("bound postcommit admission: the durable native launch-intent classification refused")
)

// borrowedBoundPostcommitAdmissionDispatchBudget bounds the one actual native
// coordinator dispatch (marker + launch-intent commit + launch refusal).
const borrowedBoundPostcommitAdmissionDispatchBudget = 60 * time.Second

// borrowedBoundPostcommitAdmissionPristineBudget bounds the target data check.
const borrowedBoundPostcommitAdmissionPristineBudget = 30 * time.Second

// borrowedBoundPostcommitAdmissionState is the copy-shared one-shot state of
// the lane-local postcommit-admission handoff gate. The gate is armed ONLY from
// an explicitly acknowledged baseline commit inside a still-live isolation
// flow, dispatches at most ONE native coordinator attempt, and refuses once the
// live interval is over. Copies share the same state pointer.
type borrowedBoundPostcommitAdmissionState struct {
	mu         sync.Mutex
	armed      bool
	dispatched bool
	terminal   bool
	operation  string
	flow       *borrowedBoundWriterIsolationFlowState
}

type borrowedBoundPostcommitAdmission struct {
	state *borrowedBoundPostcommitAdmissionState
}

func newBorrowedBoundPostcommitAdmission() *borrowedBoundPostcommitAdmission {
	return &borrowedBoundPostcommitAdmission{state: &borrowedBoundPostcommitAdmissionState{}}
}

// armAfterAcknowledgedBaseline arms the handoff ONLY from the acknowledged
// baseline classification and a flow that is still inside its live proof
// interval. Visible durable rows alone can never arm it.
func (a *borrowedBoundPostcommitAdmission) armAfterAcknowledgedBaseline(plan *borrowedBoundBaselineCommitPlan, flow *borrowedBoundWriterIsolationFlowState) error {
	if a == nil || a.state == nil || plan == nil {
		return fmt.Errorf("%w: the concrete plan and gate are required", errBoundPostcommitAdmissionUnarmed)
	}
	if !plan.commitAcknowledged || plan.commitAckLost || plan.rollbackAcknowledged || plan.successInferred ||
		!plan.durableAuditPreparation || plan.durableAuditCount != 1 ||
		plan.durableGuardState != controlstore.TargetGuardClean || plan.rehearsalOperationID == "" {
		return fmt.Errorf("%w: acknowledged=%t ackLost=%t rollback=%t inferred=%t guard=%q audit=%d op=%q",
			errBoundPostcommitAdmissionBaseline, plan.commitAcknowledged, plan.commitAckLost, plan.rollbackAcknowledged,
			plan.successInferred, plan.durableGuardState, plan.durableAuditCount, plan.rehearsalOperationID)
	}
	if flow == nil {
		return fmt.Errorf("%w: a live flow is required", errBoundPostcommitAdmissionUnarmed)
	}
	if flow.flowErr != nil || flow.cleanupRestored || !flow.fenceApplied || !flow.coverageOK {
		return fmt.Errorf("%w: flowErr=%v restored=%t fence=%t coverage=%t",
			errBoundPostcommitAdmissionTerminal, flow.flowErr, flow.cleanupRestored, flow.fenceApplied, flow.coverageOK)
	}
	a.state.mu.Lock()
	defer a.state.mu.Unlock()
	if a.state.armed || a.state.dispatched || a.state.terminal {
		return fmt.Errorf("%w: the handoff was already armed/dispatched/terminal", errBoundPostcommitAdmissionReplay)
	}
	a.state.armed = true
	a.state.operation = plan.rehearsalOperationID
	a.state.flow = flow
	return nil
}

// dispatchOnce consumes the single native dispatch. It refuses when the gate is
// unarmed, already dispatched, terminal, or when the armed flow has since left
// its live interval (completed/failed), so a completed fact set or an old flow
// state can never hand off.
func (a *borrowedBoundPostcommitAdmission) dispatchOnce(ctx context.Context, attempt *borrowedReplacementPrelaunchAttempt) (recovery.TargetWriterResult, recovery.DrillTargetProcessReceipt, error) {
	var zeroResult recovery.TargetWriterResult
	var zeroReceipt recovery.DrillTargetProcessReceipt
	if a == nil || a.state == nil || attempt == nil || attempt.run == nil {
		return zeroResult, zeroReceipt, errBoundPostcommitAdmissionUnarmed
	}
	a.state.mu.Lock()
	if a.state.terminal || !a.state.armed {
		reason := errBoundPostcommitAdmissionTerminal
		if !a.state.armed {
			reason = errBoundPostcommitAdmissionUnarmed
		}
		a.state.mu.Unlock()
		return zeroResult, zeroReceipt, reason
	}
	if a.state.dispatched {
		a.state.mu.Unlock()
		return zeroResult, zeroReceipt, errBoundPostcommitAdmissionReplay
	}
	flow := a.state.flow
	if flow == nil || flow.flowErr != nil || flow.cleanupRestored {
		a.state.mu.Unlock()
		return zeroResult, zeroReceipt, errBoundPostcommitAdmissionTerminal
	}
	a.state.dispatched = true
	a.state.mu.Unlock()
	return attempt.dispatchRun(ctx)
}

func (a *borrowedBoundPostcommitAdmission) sealTerminal() {
	if a == nil || a.state == nil {
		return
	}
	a.state.mu.Lock()
	a.state.terminal = true
	a.state.mu.Unlock()
}

func (a *borrowedBoundPostcommitAdmission) armedNow() bool {
	if a == nil || a.state == nil {
		return false
	}
	a.state.mu.Lock()
	defer a.state.mu.Unlock()
	return a.state.armed
}

func (a *borrowedBoundPostcommitAdmission) dispatchedNow() bool {
	if a == nil || a.state == nil {
		return false
	}
	a.state.mu.Lock()
	defer a.state.mu.Unlock()
	return a.state.dispatched
}

// borrowedBoundPostcommitAdmissionMarker is the counted wrapper around the
// genuine fixture transactional restore_started marker. Inside the genuine
// supplied preparation transaction it repeats the live/provenance checks (the
// backend PID must differ from the retained owner; the authentic instance row
// must still be open and re-lockable with the exact immutable binding), then
// delegates to the genuine marker, and ONLY after that success closes the
// attempt's existing endpoint listener so the unchanged launch-stage endpoint
// revalidation refuses before Start. A preDelegate error is an injected
// cancellation inside the marker before success.
type borrowedBoundPostcommitAdmissionMarker struct {
	calls               int32
	lastToken           recovery.EvidenceToken
	ownerPID            int
	pid                 int
	pidDistinct         bool
	instanceRevalidated bool
	endpointClosed      bool
	closeEndpoint       func() error
	preDelegate         func(context.Context) error
	preDelegateErr      error // the error the intended injected cancellation returned before the genuine delegate
	delegate            func(context.Context, pgx.Tx, controlstore.InstanceToken) (recovery.EvidenceToken, error)
}

func newBorrowedBoundPostcommitAdmissionMarker(operation, actorRole string, ownerPID int) *borrowedBoundPostcommitAdmissionMarker {
	return &borrowedBoundPostcommitAdmissionMarker{
		ownerPID: ownerPID,
		delegate: borrowedAuthFixtureRestoreStartedMarker(operation, actorRole),
	}
}

func (m *borrowedBoundPostcommitAdmissionMarker) hook(hookCtx context.Context, tx pgx.Tx, locked controlstore.InstanceToken) (recovery.EvidenceToken, error) {
	var zero recovery.EvidenceToken
	if m == nil || tx == nil {
		return zero, errors.New("bound postcommit admission marker requires the genuine transaction")
	}
	atomic.AddInt32(&m.calls, 1)
	var pid int
	if err := tx.QueryRow(hookCtx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil || pid <= 0 {
		return zero, fmt.Errorf("bound postcommit admission marker live identity refused: %v", err)
	}
	m.pid = pid
	if pid == m.ownerPID {
		return zero, errors.New("bound postcommit admission marker reused the retained owner session")
	}
	m.pidDistinct = true
	again, err := controlstore.LockInstance(hookCtx, tx, locked.InstanceID)
	if err != nil || again.InstanceID != locked.InstanceID || again.State != "open" ||
		again.TargetGuardKey != locked.TargetGuardKey || again.TargetRoleFingerprint != locked.TargetRoleFingerprint {
		return zero, fmt.Errorf("bound postcommit admission marker instance provenance refused: err=%v state=%q", err, again.State)
	}
	m.instanceRevalidated = true
	if m.preDelegate != nil {
		if err := m.preDelegate(hookCtx); err != nil {
			m.preDelegateErr = err
			return zero, err
		}
	}
	token, err := m.delegate(hookCtx, tx, locked)
	if err != nil {
		return token, err
	}
	m.lastToken = token
	if m.closeEndpoint != nil {
		if err := m.closeEndpoint(); err != nil {
			return token, fmt.Errorf("bound postcommit admission endpoint closure after preflight refused: %w", err)
		}
		m.endpointClosed = true
	}
	return token, nil
}

func (m *borrowedBoundPostcommitAdmissionMarker) callsNow() int32 {
	return atomic.LoadInt32(&m.calls)
}

func (m *borrowedBoundPostcommitAdmissionMarker) token() recovery.EvidenceToken {
	if m == nil {
		return recovery.EvidenceToken{}
	}
	return m.lastToken
}

// borrowedBoundPostcommitAdmissionObservation is the lane-local observed fact
// container of the positive handoff.
type borrowedBoundPostcommitAdmissionObservation struct {
	baselineOperation         string
	baselineAuditCount        int
	baselineAuditDigest       string
	preDispatchGuardState     string
	preDispatchGuardOperation string
	nativeOperation           string
	nativeResult              recovery.TargetWriterResult
	nativeReceipt             recovery.DrillTargetProcessReceipt
	nativeErr                 error
	nativeChildPID            int
	nativeChildStartID        uint64
	revalidated               bool
	fenceRechecked            bool
	attemptID                 string
}

// newBorrowedBoundPostcommitAdmissionAttempt prepares the ONE fresh bound
// native attempt through the EXISTING common attempt constructor with the real
// bound options, the authentic instance id, the descriptor-verified archive,
// the unique operation identity and the lane-local counted marker, and retains
// the actual endpoint listener for the post-preflight closure.
func newBorrowedBoundPostcommitAdmissionAttempt(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding, attemptID string, marker *borrowedBoundPostcommitAdmissionMarker, probeCalls, acceptanceCalls *int32) *borrowedReplacementPrelaunchAttempt {
	t.Helper()
	f := b.fixture
	attempt, err := newBorrowedReplacementPrelaunchAttemptWith(t, ctx, b, fresh, borrowedReplacementPrelaunchArchive(t, f), attemptID,
		probeCalls, acceptanceCalls, &borrowedReplacementPrelaunchBoundOptions{InstanceID: f.instanceID, Prelaunch: marker.hook})
	if err != nil || attempt == nil {
		t.Fatalf("bound postcommit admission attempt preparation refused: attempt=%v err=%v", attempt, err)
	}
	if attempt.source != fresh || attempt.gate == nil || attempt.prefix == nil || attempt.prefix.run != attempt.run {
		t.Fatal("bound postcommit admission attempt did not retain the genuine source, gate and fresh prefix")
	}
	runBinding := attempt.run.Binding()
	if runBinding.OriginalInstanceID() != f.instanceID ||
		runBinding.OriginalTargetKey() != fresh.binding.OriginalTargetKey() ||
		runBinding.OriginalRoleFingerprint() != fresh.binding.OriginalRoleFingerprint() ||
		runBinding.OriginalOperationID() != attemptID {
		t.Fatalf("bound postcommit admission attempt lost the preserved lineage or its unique operation: %+v", runBinding)
	}
	marker.closeEndpoint = func() error { return attempt.gate.endpoint.Listener().Close() }
	return attempt
}

// borrowedBoundPostcommitAdmissionInstanceFacts independently reads the
// authentic instance binding and evidence generation.
func borrowedBoundPostcommitAdmissionInstanceFacts(ctx context.Context, t *testing.T, b *borrowedSuccessorBaseline) (string, string, string, int64) {
	t.Helper()
	readCtx, cancelRead := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	defer cancelRead()
	var state, key, fingerprint string
	var generation int64
	if err := b.fixture.controlPool.QueryRow(readCtx, `
SELECT state, target_guard_key, target_role_fingerprint, evidence_generation
FROM recovery_instance WHERE instance_id=$1`, b.fixture.instanceID).Scan(&state, &key, &fingerprint, &generation); err != nil {
		t.Fatalf("bound postcommit admission instance facts refused: %v", err)
	}
	return state, key, fingerprint, generation
}

// borrowedBoundPostcommitAdmissionAuditDigest is the independent count + byte
// digest of one audit identity, so a committed preparation audit can be proven
// unchanged and unrelabelled.
func borrowedBoundPostcommitAdmissionAuditDigest(ctx context.Context, t *testing.T, b *borrowedSuccessorBaseline, action, operationID string) (int, string) {
	t.Helper()
	readCtx, cancelRead := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	defer cancelRead()
	var count int
	var digest string
	if err := b.fixture.controlPool.QueryRow(readCtx, `
SELECT count(*)::int, COALESCE(md5(string_agg(row_to_json(a)::text, '|' ORDER BY a.audit_id)), '')
FROM recovery_audit a WHERE a.action=$1 AND a.operation_id=$2`, action, operationID).Scan(&count, &digest); err != nil {
		t.Fatalf("bound postcommit admission audit digest %s/%s refused: %v", action, operationID, err)
	}
	return count, digest
}

// borrowedBoundPostcommitAdmissionRevalidate is the fresh revalidation pass
// between the acknowledged baseline commit and the native admission, all while
// the flow transaction proof interval is still held. It trusts no cached flow
// fact.
func borrowedBoundPostcommitAdmissionRevalidate(ctx context.Context, t *testing.T, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding, plan *borrowedBoundBaselineCommitPlan, flow *borrowedBoundWriterIsolationFlowState) error {
	t.Helper()
	f := b.fixture
	admin := f.fx.admin
	rules, err := borrowedBoundWriterIsolationReadRules(ctx, admin)
	if err != nil {
		return fmt.Errorf("%w: fresh coverage read: %v", errBoundPostcommitAdmissionRevalidate, err)
	}
	scopes := borrowedBoundWriterIsolationScopes(rules)
	if len(scopes) == 0 {
		return fmt.Errorf("%w: no supported writer scopes were re-enumerated", errBoundPostcommitAdmissionRevalidate)
	}
	if err := borrowedBoundWriterIsolationFenceStateCheck(rules, scopes, f.writerRole); err != nil {
		return fmt.Errorf("%w: fresh effective fence: %v", errBoundPostcommitAdmissionRevalidate, err)
	}
	serverIP, err := f.fx.container.ContainerIP(ctx)
	if err != nil || serverIP == "" {
		return fmt.Errorf("%w: fixture container endpoint unknown: %v", errBoundPostcommitAdmissionRevalidate, err)
	}
	for _, transport := range []string{"unix", "loopback", "serverip"} {
		line := runHelperAuthCheck(t, ctx, f.fx.containerID, transport, serverIP, f.writerRole, b.passwordP1)
		if !borrowedBoundWriterIsolationContains(line, "REJECT", "28000") {
			return fmt.Errorf("%w: transport %s admission line %q", errBoundPostcommitAdmissionRevalidate, transport, line)
		}
	}
	if flow == nil || flow.censusConn == nil {
		return fmt.Errorf("%w: the live flow census connection is absent", errBoundPostcommitAdmissionRevalidate)
	}
	census, err := borrowedBoundWriterIsolationCensus(ctx, flow.censusConn, f.writerRole)
	if err != nil || census != 0 {
		return fmt.Errorf("%w: retained-writer census=%d err=%v", errBoundPostcommitAdmissionRevalidate, census, err)
	}
	if err := borrowedBoundWriterIsolationObserverOK(ctx, f); err != nil {
		return fmt.Errorf("%w: %v", errBoundPostcommitAdmissionRevalidate, err)
	}
	if err := borrowedBoundWriterIsolationControlOK(ctx, f); err != nil {
		return fmt.Errorf("%w: %v", errBoundPostcommitAdmissionRevalidate, err)
	}
	healthCtx, cancelHealth := context.WithTimeout(ctx, borrowedOwnerRotationHealthBudget)
	healthErr := f.lock.Health(healthCtx)
	cancelHealth()
	anchorCtx, cancelAnchor := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	anchorErr := b.anchor.Recheck(anchorCtx)
	cancelAnchor()
	if healthErr != nil || anchorErr != nil {
		return fmt.Errorf("%w: owner health=%v anchor=%v", errBoundPostcommitAdmissionRevalidate, healthErr, anchorErr)
	}
	oid, oidErr := borrowedOwnerDDLTargetOID(ctx, b)
	if oidErr != nil || oid != fresh.replacementOID {
		return fmt.Errorf("%w: actual catalog oid=%d err=%v", errBoundPostcommitAdmissionRevalidate, oid, oidErr)
	}
	if invalid, reason := fresh.prefix.Invalid(); invalid {
		return fmt.Errorf("%w: shared replacement prefix invalid: %s", errBoundPostcommitAdmissionRevalidate, reason)
	}
	state, key, fingerprint, _ := borrowedBoundPostcommitAdmissionInstanceFacts(ctx, t, b)
	if state != "open" || key != f.guardKey || fingerprint != fresh.binding.OriginalRoleFingerprint() {
		return fmt.Errorf("%w: authentic instance binding state=%q key=%q fingerprint=%q", errBoundPostcommitAdmissionRevalidate, state, key, fingerprint)
	}
	if plan == nil || plan.rehearsalOperationID == "" {
		return fmt.Errorf("%w: no committed preparation operation was recorded", errBoundPostcommitAdmissionRevalidate)
	}
	guard, found, guardErr := controlstore.ReadTargetGuard(ctx, f.controlPool, f.guardKey)
	if guardErr != nil || !found || guard.State != controlstore.TargetGuardClean || guard.OperationID != plan.rehearsalOperationID {
		return fmt.Errorf("%w: committed preparation guard read err=%v found=%t state=%q op=%q want=%q",
			errBoundPostcommitAdmissionRevalidate, guardErr, found, guard.State, guard.OperationID, plan.rehearsalOperationID)
	}
	return nil
}

// borrowedBoundPostcommitAdmissionFenceRecheck re-reads the effective rules and
// re-exercises the intended admission-stage refusal immediately before leaving
// the proof interval.
func borrowedBoundPostcommitAdmissionFenceRecheck(ctx context.Context, t *testing.T, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding) error {
	t.Helper()
	f := b.fixture
	rules, err := borrowedBoundWriterIsolationReadRules(ctx, f.fx.admin)
	if err != nil {
		return fmt.Errorf("%w: fence recheck read: %v", errBoundPostcommitAdmissionRevalidate, err)
	}
	scopes := borrowedBoundWriterIsolationScopes(rules)
	if len(scopes) == 0 || borrowedBoundWriterIsolationFenceStateCheck(rules, scopes, f.writerRole) != nil {
		return fmt.Errorf("%w: the effective writer-reject fence did not stay live", errBoundPostcommitAdmissionRevalidate)
	}
	serverIP, err := f.fx.container.ContainerIP(ctx)
	if err != nil {
		return fmt.Errorf("%w: fence recheck endpoint: %v", errBoundPostcommitAdmissionRevalidate, err)
	}
	line := runHelperAuthCheck(t, ctx, f.fx.containerID, "serverip", serverIP, f.writerRole, b.passwordP1)
	if !borrowedBoundWriterIsolationContains(line, "REJECT", "28000") {
		return fmt.Errorf("%w: fence recheck admission line %q", errBoundPostcommitAdmissionRevalidate, line)
	}
	return nil
}

// borrowedBoundPostcommitAdmissionAssertNoAcceptedOutput requires a refusal to
// publish no command, no probe output and no process receipt.
func borrowedBoundPostcommitAdmissionAssertNoAcceptedOutput(t *testing.T, label string, result recovery.TargetWriterResult, receipt recovery.DrillTargetProcessReceipt) {
	t.Helper()
	if result.Command != (recovery.PGCommandResult{}) {
		t.Fatalf("%s: refusal published a command result: %+v", label, result.Command)
	}
	borrowedBoundNativeReadyRequireNoProbeOutput(t, label, result)
	borrowedBoundNativeReadyRequireAbsentReceipt(t, label, receipt)
}

// borrowedBoundPostcommitAdmissionAssertNativeIntent independently proves the
// committed native admission: marker evidence committed with the genuine token
// generation, the guard conservatively UNKNOWN with durable launch intent (exact
// operation/application, NO launched timestamp, clean/rebuild fields
// legitimately replaced), the historical baseline preparation audit
// byte-identical and unrelabelled, zero child start, absent process receipt,
// zero callbacks, no accepted output and the catalog unchanged.
func borrowedBoundPostcommitAdmissionAssertNativeIntent(ctx context.Context, t *testing.T, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding, attempt *borrowedReplacementPrelaunchAttempt, marker *borrowedBoundPostcommitAdmissionMarker, result recovery.TargetWriterResult, receipt recovery.DrillTargetProcessReceipt, probeCalls, acceptanceCalls *int32, obs *borrowedBoundPostcommitAdmissionObservation) {
	t.Helper()
	f := b.fixture
	label := "bound postcommit admission native intent"
	token := marker.token()
	if token == (recovery.EvidenceToken{}) || token.InstanceID != f.instanceID || token.State != "open" || token.Generation <= 0 {
		t.Fatalf("%s: the genuine transactional marker token is absent/invalid: %+v", label, token)
	}
	if result.MarkerToken != token {
		t.Fatalf("%s: the result marker token is not the genuine committed token", label)
	}
	state, key, fingerprint, generation := borrowedBoundPostcommitAdmissionInstanceFacts(ctx, t, b)
	if state != "open" || key != f.guardKey || fingerprint != fresh.binding.OriginalRoleFingerprint() {
		t.Fatalf("%s: committed instance provenance state=%q key=%q fingerprint=%q", label, state, key, fingerprint)
	}
	if generation != token.Generation {
		t.Fatalf("%s: committed instance generation=%d, marker token generation=%d", label, generation, token.Generation)
	}
	readCtx, cancelRead := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	var markerAudits int
	if err := f.controlPool.QueryRow(readCtx, `
SELECT count(*)::int FROM recovery_audit
WHERE instance_id=$1 AND action=$2 AND operation_id=$3 AND result='ok'
  AND target->>'kind'=$4 AND target->>'accepted_generation'=$5`,
		f.instanceID, recovery.ActionEvidenceWrite, obs.nativeOperation, string(recovery.MutationRestoreStarted), strconv.FormatInt(token.Generation, 10)).Scan(&markerAudits); err != nil {
		cancelRead()
		t.Fatalf("%s: committed marker audit read refused: %v", label, err)
	}
	cancelRead()
	if markerAudits != 1 {
		t.Fatalf("%s: committed marker audits=%d, want exactly 1", label, markerAudits)
	}
	guard, found, guardErr := controlstore.ReadTargetGuard(ctx, f.controlPool, f.guardKey)
	if guardErr != nil || !found {
		t.Fatalf("%s: native intent guard read err=%v found=%t", label, guardErr, found)
	}
	if guard.State != controlstore.TargetGuardUnknown || !guard.ActiveWriter || !guard.LaunchIntent ||
		guard.AttemptAppName != result.Application || guard.OperationID != obs.nativeOperation ||
		guard.LaunchIntentAt == nil || guard.LaunchedAt != nil || guard.CleanAt != nil ||
		guard.RebuildRequiredAt != nil || len(guard.RebuildEvidence) != 0 {
		t.Fatalf("%s: guard is not the conservative durable launch intent with replaced clean fields: %+v", label, guard)
	}
	if err := recovery.ValidateAttemptApplicationName(result.Application); err != nil {
		t.Fatalf("%s: native application identity invalid: %v", label, err)
	}
	count, digest := borrowedBoundPostcommitAdmissionAuditDigest(ctx, t, b, controlstore.ActionTargetGuardRebuild, obs.baselineOperation)
	if count != obs.baselineAuditCount || digest != obs.baselineAuditDigest {
		t.Fatalf("%s: baseline preparation audit is not byte-identical/unrelabelled: count=%d->%d digest=%q->%q", label, obs.baselineAuditCount, count, obs.baselineAuditDigest, digest)
	}
	nativeAudits := borrowedBoundBaselineCommitPreparationRowCount(t, ctx, b, obs.nativeOperation)
	if nativeAudits != 0 {
		t.Fatalf("%s: the native admission wrote %d target_guard_rebuild audit rows", label, nativeAudits)
	}
	identity := attempt.run.Observation().StartedIdentity()
	if identity.Started || identity.PID != 0 || identity.StartID != 0 {
		t.Fatalf("%s: the refused native attempt started a child: %+v", label, identity)
	}
	borrowedBoundPostcommitAdmissionAssertNoAcceptedOutput(t, label, result, receipt)
	if got := atomic.LoadInt32(probeCalls); got != 0 {
		t.Fatalf("%s: probe callbacks=%d, want 0", label, got)
	}
	if got := atomic.LoadInt32(acceptanceCalls); got != 0 {
		t.Fatalf("%s: acceptance callbacks=%d, want 0", label, got)
	}
	if oid, oidErr := borrowedOwnerDDLTargetOID(ctx, b); oidErr != nil || oid != fresh.replacementOID {
		t.Fatalf("%s: replacement catalog changed: oid=%d err=%v", label, oid, oidErr)
	}
}

// borrowedBoundPostcommitAdmissionAssertNoNativeOutcome requires a control to
// have produced no native outcome at all: the expected marker-call count, no
// started child, zero probe/acceptance callbacks and no gate dispatch.
func borrowedBoundPostcommitAdmissionAssertNoNativeOutcome(t *testing.T, label string, attempt *borrowedReplacementPrelaunchAttempt, marker *borrowedBoundPostcommitAdmissionMarker, probeCalls, acceptanceCalls *int32, wantMarkerCalls int32) {
	t.Helper()
	if got := marker.callsNow(); got != wantMarkerCalls {
		t.Fatalf("%s: marker callbacks=%d, want %d", label, got, wantMarkerCalls)
	}
	identity := attempt.run.Observation().StartedIdentity()
	if identity.Started || identity.PID != 0 || identity.StartID != 0 {
		t.Fatalf("%s: a child was started: %+v", label, identity)
	}
	if got := atomic.LoadInt32(probeCalls); got != 0 {
		t.Fatalf("%s: probe callbacks=%d, want 0", label, got)
	}
	if got := atomic.LoadInt32(acceptanceCalls); got != 0 {
		t.Fatalf("%s: acceptance callbacks=%d, want 0", label, got)
	}
}

// borrowedBoundPostcommitAdmissionMarkerRefusalClassified requires the ACTUAL
// marker-path refusal class instead of any non-nil error: either the intended
// injected cancellation sentinel survives the native error chain, or the error
// is EXACTLY the production prelaunch marker-hook transaction classification
// and the counted marker recorded that the intended injected cancellation ran
// before the genuine transactional delegate. The production classifier
// deliberately replaces the hook error with its own classification, so the
// sentinel is proven at its injection site on the counted marker.
func borrowedBoundPostcommitAdmissionMarkerRefusalClassified(runErr error, marker *borrowedBoundPostcommitAdmissionMarker) bool {
	if runErr == nil {
		return false
	}
	if errors.Is(runErr, errBoundPostcommitAdmissionCancelled) {
		return true
	}
	if runErr.Error() != "transactional restore-start marker hook failed" {
		return false
	}
	return marker != nil && marker.preDelegateErr != nil &&
		errors.Is(marker.preDelegateErr, errBoundPostcommitAdmissionCancelled)
}

// borrowedBoundPostcommitAdmissionAssertNoHandoffDurableState requires a
// no-handoff control to leave the deliberately unresolved original guard row
// byte-identical (the historical dirty row may legitimately carry a launch
// intent/launched_at from its earlier failed attempt; what must NOT exist is a
// NEW native intent, marker row or preparation row), with no launch-intent
// change and no preparation rows for either operation.
func borrowedBoundPostcommitAdmissionAssertNoHandoffDurableState(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, before controlstore.TargetGuard, baselineOp, nativeOp string) {
	t.Helper()
	f := b.fixture
	after, found, err := controlstore.ReadTargetGuard(ctx, f.controlPool, f.guardKey)
	if err != nil || !found {
		t.Fatalf("no-handoff control guard read err=%v found=%t", err, found)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("no-handoff control changed the original unresolved guard row: before=%+v after=%+v", before, after)
	}
	if before.State == "clean" || before.OperationID != f.operation {
		t.Fatalf("no-handoff control baseline is not the deliberately unresolved original guard: %+v", before)
	}
	if baselineOp != "" {
		if count := borrowedBoundBaselineCommitPreparationRowCount(t, ctx, b, baselineOp); count != 0 {
			t.Fatalf("no-handoff control wrote %d preparation rows for %q", count, baselineOp)
		}
	}
	if nativeOp != "" {
		if count := borrowedBoundBaselineCommitPreparationRowCount(t, ctx, b, nativeOp); count != 0 {
			t.Fatalf("no-handoff control wrote %d preparation rows for %q", count, nativeOp)
		}
	}
}

// borrowedBoundPostcommitAdmissionAssertPreservedDurableDelta independently
// re-reads the exact committed controlled-preparation guard/audit delta of a
// control whose baseline commit survived.
func borrowedBoundPostcommitAdmissionAssertPreservedDurableDelta(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, operationID string) {
	t.Helper()
	delta, err := borrowedBoundBaselineCommitReadDelta(ctx, b.fixture.controlPool, b.fixture.guardKey, operationID)
	if err != nil {
		t.Fatalf("preserved durable delta read refused: %v", err)
	}
	borrowedBoundBaselineCommitAssertDelta(t, "preserved durable delta", delta, b.fixture.instanceID, operationID)
}

// borrowedBoundPostcommitAdmissionAssertPreservedCommittedBaseline requires the
// committed baseline preparation classification to stay EXACTLY as committed
// with NO native marker/intent: the clean guard with the preparation operation,
// the byte-identical preparation audit and zero native marker/intent rows.
func borrowedBoundPostcommitAdmissionAssertPreservedCommittedBaseline(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, plan *borrowedBoundBaselineCommitPlan, obs *borrowedBoundPostcommitAdmissionObservation) {
	t.Helper()
	f := b.fixture
	guard, found, err := controlstore.ReadTargetGuard(ctx, f.controlPool, f.guardKey)
	if err != nil || !found || guard.State != controlstore.TargetGuardClean || guard.OperationID != plan.rehearsalOperationID ||
		guard.ActiveWriter || guard.CleanAt == nil || len(guard.RebuildEvidence) == 0 {
		t.Fatalf("preserved committed baseline guard changed: err=%v found=%t guard=%+v", err, found, guard)
	}
	count, digest := borrowedBoundPostcommitAdmissionAuditDigest(ctx, t, b, controlstore.ActionTargetGuardRebuild, obs.baselineOperation)
	if count != obs.baselineAuditCount || digest != obs.baselineAuditDigest {
		t.Fatalf("preserved committed baseline audit changed: count=%d->%d digest=%q->%q", obs.baselineAuditCount, count, obs.baselineAuditDigest, digest)
	}
	if nativeRows := borrowedBoundBaselineCommitPreparationRowCount(t, ctx, b, obs.nativeOperation); nativeRows != 0 {
		t.Fatalf("preserved committed baseline has %d native target_guard_rebuild rows", nativeRows)
	}
	if obs.attemptID != "" {
		readCtx, cancelRead := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
		var nativeMarkerRows int
		if err := f.controlPool.QueryRow(readCtx, `
SELECT count(*)::int FROM recovery_audit WHERE action=$1 AND operation_id=$2 AND target->>'kind'=$3`,
			recovery.ActionEvidenceWrite, obs.attemptID, string(recovery.MutationRestoreStarted)).Scan(&nativeMarkerRows); err != nil {
			cancelRead()
			t.Fatalf("preserved committed baseline native marker audit read refused: %v", err)
		}
		cancelRead()
		if nativeMarkerRows != 0 {
			t.Fatalf("preserved committed baseline committed %d native marker audits", nativeMarkerRows)
		}
	}
}

// borrowedBoundPostcommitAdmissionPristine is the independent whole-catalog
// data/catalog check through the genuine P1 target credential.
func borrowedBoundPostcommitAdmissionPristine(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline) string {
	t.Helper()
	f := b.fixture
	conn := borrowedOwnerDDLConnect(t, ctx, borrowedAuthRoleDSN(t, f.writerTargetDSN, f.writerRole, b.passwordP1))
	defer borrowedSuccessorCloseConn(t, conn)
	checkCtx, cancelCheck := context.WithTimeout(ctx, borrowedBoundPostcommitAdmissionPristineBudget)
	digest, err := borrowedOwnerDDLPristine(checkCtx, conn, f.targetDB, f.writerRole)
	cancelCheck()
	if err != nil {
		t.Fatalf("bound postcommit admission target pristine check refused: %v", err)
	}
	return digest
}

// borrowedBoundPostcommitAdmissionSuspendControl suspends the real control
// role login and returns the restoring cleanup, so the protected control route
// refusal is a genuine connection-level failure.
func borrowedBoundPostcommitAdmissionSuspendControl(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline) func() {
	t.Helper()
	target, err := controlstore.ParseDSNTarget(b.fixture.controlDSN)
	if err != nil || target.Role == "" {
		t.Fatalf("bound postcommit admission control identity refused: err=%v", err)
	}
	admin := b.fixture.fx.admin
	execCtx, cancelExec := context.WithTimeout(ctx, borrowedOwnerRotationAuthBudget)
	_, err = admin.Exec(execCtx, `ALTER ROLE `+pgx.Identifier{target.Role}.Sanitize()+` NOLOGIN`)
	cancelExec()
	if err != nil {
		t.Fatalf("bound postcommit admission control suspension refused: %v", err)
	}
	return func() {
		restoreCtx, cancelRestore := context.WithTimeout(context.Background(), borrowedOwnerRotationAuthBudget)
		_, _ = admin.Exec(restoreCtx, `ALTER ROLE `+pgx.Identifier{target.Role}.Sanitize()+` LOGIN`)
		cancelRestore()
	}
}

// borrowedBoundPostcommitAdmissionHookPlan is the lane-local duringProof
// control plan.
type borrowedBoundPostcommitAdmissionHookPlan struct {
	attemptID           string
	cancelLane          func()
	cancelBeforeHandoff bool
	cancelAfterIntent   bool
	latePrefixLoss      bool
	expect              string // "launch" | "source" | "marker"
}

// borrowedBoundPostcommitAdmissionDuringProof is the synchronous duringProof
// stage of the positive and of the flow-based controls. It NEVER returns before
// the acknowledged baseline commit and the native handoff classification are
// complete, and it rechecks the live fence before leaving.
func borrowedBoundPostcommitAdmissionDuringProof(hookCtx context.Context, t *testing.T, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding, attempt *borrowedReplacementPrelaunchAttempt, marker *borrowedBoundPostcommitAdmissionMarker, admission *borrowedBoundPostcommitAdmission, plan *borrowedBoundBaselineCommitPlan, preparation *borrowedBoundBaselineCommitPreparation, prepPID int, probeCalls, acceptanceCalls *int32, obs *borrowedBoundPostcommitAdmissionObservation, flow *borrowedBoundWriterIsolationFlowState, hook *borrowedBoundPostcommitAdmissionHookPlan) error {
	t.Helper()
	f := b.fixture
	obs.attemptID = hook.attemptID
	if err := borrowedBoundBaselineCommitStageRun(hookCtx, t, b, fresh, prepPID, plan, preparation); err != nil {
		return err
	}
	if !plan.commitAcknowledged || plan.commitAckLost || plan.rollbackAcknowledged || plan.successInferred {
		return fmt.Errorf("%w: acknowledged=%t ackLost=%t rollback=%t inferred=%t",
			errBoundPostcommitAdmissionBaseline, plan.commitAcknowledged, plan.commitAckLost, plan.rollbackAcknowledged, plan.successInferred)
	}
	obs.baselineOperation = plan.rehearsalOperationID
	obs.preDispatchGuardState, obs.preDispatchGuardOperation = borrowedReplacementGuardRowRead(t, hookCtx, f.controlPool, f.guardKey)
	obs.baselineAuditCount, obs.baselineAuditDigest = borrowedBoundPostcommitAdmissionAuditDigest(hookCtx, t, b, controlstore.ActionTargetGuardRebuild, plan.rehearsalOperationID)
	if obs.preDispatchGuardState != "clean" || obs.preDispatchGuardOperation != plan.rehearsalOperationID || obs.baselineAuditCount != 1 {
		return fmt.Errorf("%w: committed preparation state=%q op=%q audit=%d", errBoundPostcommitAdmissionRevalidate, obs.preDispatchGuardState, obs.preDispatchGuardOperation, obs.baselineAuditCount)
	}
	if err := admission.armAfterAcknowledgedBaseline(plan, flow); err != nil {
		return err
	}
	if err := borrowedBoundPostcommitAdmissionRevalidate(hookCtx, t, b, fresh, plan, flow); err != nil {
		return err
	}
	obs.revalidated = true
	if hook.latePrefixLoss {
		borrowedReplacementSessionCopiedPrefixLoss(t, fresh)
	}
	if hook.cancelBeforeHandoff && hook.cancelLane != nil {
		hook.cancelLane()
	}
	dispatchCtx, cancelDispatch := context.WithTimeout(hookCtx, borrowedBoundPostcommitAdmissionDispatchBudget)
	result, receipt, runErr := admission.dispatchOnce(dispatchCtx, attempt)
	cancelDispatch()
	obs.nativeOperation = hook.attemptID
	obs.nativeResult, obs.nativeReceipt, obs.nativeErr = result, receipt, runErr
	switch hook.expect {
	case "launch":
		if runErr == nil {
			return fmt.Errorf("%w: the native coordinator returned success", errBoundPostcommitAdmissionLaunch)
		}
		if !strings.Contains(runErr.Error(), "refused at launch") || !strings.Contains(runErr.Error(), "origin endpoint capability") || strings.Contains(runErr.Error(), "refused at preflight") {
			return fmt.Errorf("%w: %v", errBoundPostcommitAdmissionLaunch, runErr)
		}
		borrowedBoundPostcommitAdmissionAssertNativeIntent(hookCtx, t, b, fresh, attempt, marker, result, receipt, probeCalls, acceptanceCalls, obs)
	case "source":
		if runErr == nil || !strings.Contains(runErr.Error(), "prelaunch attempt") || strings.Contains(runErr.Error(), "refused at launch") {
			return fmt.Errorf("%w: %v", errBoundPostcommitAdmissionSource, runErr)
		}
		borrowedBoundPostcommitAdmissionAssertNoNativeOutcome(t, "bound postcommit admission source refusal", attempt, marker, probeCalls, acceptanceCalls, 0)
		borrowedBoundPostcommitAdmissionAssertNoAcceptedOutput(t, "bound postcommit admission source refusal", result, receipt)
	case "marker":
		if runErr == nil || strings.Contains(runErr.Error(), "refused at launch") {
			return fmt.Errorf("%w: %v", errBoundPostcommitAdmissionSource, runErr)
		}
		if !borrowedBoundPostcommitAdmissionMarkerRefusalClassified(runErr, marker) {
			return fmt.Errorf("%w: the marker refusal is not the classified prelaunch marker-hook transaction failure of the intended injected cancellation: %v", errBoundPostcommitAdmissionSource, runErr)
		}
		borrowedBoundPostcommitAdmissionAssertNoNativeOutcome(t, "bound postcommit admission marker refusal", attempt, marker, probeCalls, acceptanceCalls, 1)
		borrowedBoundPostcommitAdmissionAssertNoAcceptedOutput(t, "bound postcommit admission marker refusal", result, receipt)
	default:
		return fmt.Errorf("%w: unknown expected refusal %q", errBoundPostcommitAdmissionSource, hook.expect)
	}
	if hookCtx.Err() != nil {
		// A cancelled caller context ends the interval; the fence disposal is
		// still proven by the unchanged flow cleanup.
		return nil
	}
	if err := borrowedBoundPostcommitAdmissionFenceRecheck(hookCtx, t, b, fresh); err != nil {
		return err
	}
	obs.fenceRechecked = true
	if hook.cancelAfterIntent && hook.cancelLane != nil {
		hook.cancelLane()
	}
	return nil
}

// TestBorrowedReplacementBoundPostcommitAdmission is the bounded bound
// postcommit-admission lane described in the file header.
func TestBorrowedReplacementBoundPostcommitAdmission(t *testing.T) {
	ctx := t.Context()

	// P: the single chain over ONE authentic bound fixture.
	t.Run("P", func(t *testing.T) {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		f := b.fixture
		borrowedBoundSessionAssertProvenance(t, ctx, b, fresh, "bound postcommit admission")
		_ = borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)

		// Existing bound dirty-guard refusal; capture the coordinator
		// preparation transaction PID through the existing marker seam.
		var prepProbe, prepAcceptance int32
		prepAttemptID := fmt.Sprintf("bound-postcommit-admission-refusal-%d", time.Now().UnixNano())
		refusalMarker := newBorrowedBoundNativeReadyMarker(prepAttemptID, f.adminRole)
		var prepTxPID int
		inner := refusalMarker.inner
		refusalMarker.inner = func(hookCtx context.Context, tx pgx.Tx, locked controlstore.InstanceToken) (recovery.EvidenceToken, error) {
			var pid int
			if err := tx.QueryRow(hookCtx, `SELECT pg_backend_pid()`).Scan(&pid); err == nil && pid > 0 {
				prepTxPID = pid
			}
			return inner(hookCtx, tx, locked)
		}
		refusalAttempt := prepareBorrowedBoundNativeReadyAttempt(t, ctx, b, fresh, prepAttemptID, refusalMarker, &prepProbe, &prepAcceptance)
		if err := refusalAttempt.registerOrigin(ctx); err != nil {
			t.Fatalf("bound postcommit admission refusal origin registration refused: %v", err)
		}
		refusalResult, refusalReceipt, refusalErr := dispatchBorrowedBoundNativeReady(t, ctx, refusalAttempt)
		if refusalErr == nil || refusalErr.Error() != "bound target guard is not clean" {
			t.Fatalf("bound postcommit admission baseline prerequisite is not the exact dirty-guard refusal: %v", refusalErr)
		}
		if got := refusalMarker.callsNow(); got != 1 {
			t.Fatalf("bound postcommit admission refusal marker calls=%d, want 1", got)
		}
		if refusalToken := refusalMarker.token(); refusalResult.MarkerToken != refusalToken || refusalToken.InstanceID != f.instanceID || refusalToken.State != "open" {
			t.Fatalf("bound postcommit admission refusal marker token is not the genuine transaction-local token: %+v", refusalToken)
		}
		borrowedBoundNativeReadyRequireNoProbeOutput(t, "bound postcommit admission refusal", refusalResult)
		borrowedBoundNativeReadyRequireAbsentReceipt(t, "bound postcommit admission refusal", refusalReceipt)
		borrowedBoundReentryAssertOutcomeBoundaries(t, "bound postcommit admission refusal", refusalAttempt, refusalResult, refusalReceipt, &prepProbe, &prepAcceptance)
		if prepTxPID <= 0 || prepTxPID == b.owner.BackendPID {
			t.Fatalf("bound postcommit admission coordinator prep tx pid=%d owner=%d", prepTxPID, b.owner.BackendPID)
		}

		// Readiness consumed once as NON-AUTHORIZING lineage facts, then strict
		// registered-P1 retirement.
		stage := &borrowedBoundReentryReadinessStage{
			key: f.guardKey, expectedOperation: f.operation,
			journalAttemptID: prepAttemptID, journalFacts: &borrowedReplacementOwnerJournalFacts{},
		}
		outcome := borrowedBoundReentryReadinessRun(ctx, t, b, fresh, stage, nil)
		readiness := newBorrowedBoundReentryReadiness()
		if err := borrowedBoundReentryReadinessTryPublish(t, "bound postcommit admission readiness", b, fresh, stage, outcome, readiness); err != nil {
			t.Fatalf("bound postcommit admission readiness refused: %v", err)
		}
		readinessFacts, ok := readiness.consume()
		if !ok {
			t.Fatal("bound postcommit admission readiness witness was not consumable once")
		}
		if readinessFacts.InstanceID != f.instanceID || readinessFacts.TargetKey != f.guardKey ||
			readinessFacts.RoleFingerprint != fresh.binding.OriginalRoleFingerprint() ||
			readinessFacts.ReplacementOID != fresh.replacementOID || readinessFacts.OwnerPID != b.owner.BackendPID {
			t.Fatalf("bound postcommit admission readiness facts are not the observed provenance: %+v", readinessFacts)
		}
		if _, again := readiness.consume(); again {
			t.Fatal("bound postcommit admission readiness witness was consumable twice")
		}
		readinessCopy := *readiness
		if _, copyOK := readinessCopy.consume(); copyOK {
			t.Fatal("a shared-state readiness copy was consumable")
		}
		if err := borrowedReplacementOwnerTailRetireGuarded(t, ctx, b, outcome); err != nil {
			t.Fatalf("bound postcommit admission registered-P1 retirement refused: %v", err)
		}
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)

		// Prepare the ONE fresh full bound native attempt BEFORE the fence.
		var nativeProbe, nativeAcceptance int32
		nativeAttemptID := fmt.Sprintf("bound-postcommit-admission-native-%d", time.Now().UnixNano())
		nativeMarker := newBorrowedBoundPostcommitAdmissionMarker(nativeAttemptID, f.adminRole, b.owner.BackendPID)
		nativeAttempt := newBorrowedBoundPostcommitAdmissionAttempt(t, ctx, b, fresh, nativeAttemptID, nativeMarker, &nativeProbe, &nativeAcceptance)
		pristineBefore := borrowedBoundPostcommitAdmissionPristine(t, ctx, b)

		// The unchanged writer-isolation flow with the baseline-commit stage and
		// the bounded native admission inside its existing duringProof hook.
		plan := newBorrowedBoundBaselineCommitPlan()
		preparation := newBorrowedBoundBaselineCommitPreparation()
		admission := newBorrowedBoundPostcommitAdmission()
		obs := &borrowedBoundPostcommitAdmissionObservation{}
		flowState, flowErr := borrowedBoundWriterIsolationRun(ctx, t, b, fresh, &borrowedBoundWriterIsolationLineage{
			consumed: true, readinessBacked: false,
			instanceID: f.instanceID, targetKey: f.guardKey,
			fingerprint:    fresh.binding.OriginalRoleFingerprint(),
			replacementOID: fresh.replacementOID, ownerPID: b.owner.BackendPID,
			issuance: &borrowedBoundWriterIsolationIssuance{},
		}, &borrowedBoundWriterIsolationHooks{
			duringProof: func(hookCtx context.Context, flow *borrowedBoundWriterIsolationFlowState) error {
				return borrowedBoundPostcommitAdmissionDuringProof(hookCtx, t, b, fresh, nativeAttempt, nativeMarker, admission, plan, preparation, prepTxPID, &nativeProbe, &nativeAcceptance, obs, flow, &borrowedBoundPostcommitAdmissionHookPlan{
					attemptID: nativeAttemptID, expect: "launch",
				})
			},
		})
		if flowErr != nil {
			t.Fatalf("bound postcommit admission positive refused: %v", flowErr)
		}
		if !plan.commitAcknowledged || plan.commitAckLost || plan.rollbackAcknowledged {
			t.Fatalf("bound postcommit admission baseline classification: acknowledged=%t ackLost=%t rollback=%t", plan.commitAcknowledged, plan.commitAckLost, plan.rollbackAcknowledged)
		}
		if !obs.revalidated || !obs.fenceRechecked || !admission.armedNow() || !admission.dispatchedNow() {
			t.Fatalf("bound postcommit admission handoff facts: revalidated=%t fence=%t armed=%t dispatched=%t", obs.revalidated, obs.fenceRechecked, admission.armedNow(), admission.dispatchedNow())
		}
		if obs.nativeErr == nil || !strings.Contains(obs.nativeErr.Error(), "refused at launch") || !strings.Contains(obs.nativeErr.Error(), "origin endpoint capability") {
			t.Fatalf("bound postcommit admission did not require the actual launch-stage endpoint refusal: %v", obs.nativeErr)
		}
		if nativeMarker.callsNow() != 1 || !nativeMarker.pidDistinct || nativeMarker.pid == b.owner.BackendPID || !nativeMarker.instanceRevalidated || !nativeMarker.endpointClosed {
			t.Fatalf("bound postcommit admission marker facts: calls=%d pidDistinct=%t pid=%d owner=%d instance=%t endpointClosed=%t",
				nativeMarker.callsNow(), nativeMarker.pidDistinct, nativeMarker.pid, b.owner.BackendPID, nativeMarker.instanceRevalidated, nativeMarker.endpointClosed)
		}
		if !flowState.cleanupRestored || flowState.cleanupErr != nil {
			t.Fatalf("bound postcommit admission fence disposal was not proven clean: restored=%t err=%v", flowState.cleanupRestored, flowState.cleanupErr)
		}

		// Exactly the intended durable classification; the committed baseline
		// audit is preserved and the native clean-guard fields are legitimately
		// replaced by the launch intent.
		borrowedBoundPostcommitAdmissionAssertNativeIntent(ctx, t, b, fresh, nativeAttempt, nativeMarker, obs.nativeResult, obs.nativeReceipt, &nativeProbe, &nativeAcceptance, obs)
		postSnap := borrowedBoundSessionSnapshotNow(t, ctx, b)
		if postSnap.state != base.snapshot.state || postSnap.key != base.snapshot.key ||
			postSnap.fingerprint != base.snapshot.fingerprint || postSnap.inventoryNull != base.snapshot.inventoryNull ||
			postSnap.versionNull != base.snapshot.versionNull || postSnap.evidenceHash == base.snapshot.evidenceHash ||
			postSnap.guardHash == base.snapshot.guardHash || postSnap.generation != base.snapshot.generation+1 {
			t.Fatalf("bound postcommit admission changed non-intended instance state: before=%+v after=%+v", base.snapshot, postSnap)
		}
		postRows := borrowedBoundNativeReadyRowsNow(t, ctx, b)
		if postRows.evidenceCount != base.rows.evidenceCount || postRows.auditCount != base.rows.auditCount+2 {
			t.Fatalf("bound postcommit admission durable rows: evidence %d->%d audit %d->%d", base.rows.evidenceCount, postRows.evidenceCount, base.rows.auditCount, postRows.auditCount)
		}
		if afterOID, oidErr := borrowedOwnerDDLTargetOID(ctx, b); oidErr != nil || afterOID != base.catalogOID {
			t.Fatalf("bound postcommit admission changed the replacement catalog: oid=%d err=%v", afterOID, oidErr)
		}

		// Restoration, original-owner health and NO continuing isolation.
		serverIP, ipErr := f.fx.container.ContainerIP(ctx)
		if ipErr != nil {
			t.Fatalf("bound postcommit admission endpoint refused: %v", ipErr)
		}
		if err := borrowedBoundWriterIsolationAwaitState(t, ctx, f, serverIP, f.writerRole, b.passwordP1, "AUTH_OK", "", 10*time.Second); err != nil {
			t.Fatalf("bound postcommit admission fence disposal did not restore the HBA: %v", err)
		}
		restoredP0 := runHelperAuthCheck(t, ctx, f.fx.containerID, "serverip", serverIP, f.writerRole, f.writerPassword)
		if !borrowedBoundWriterIsolationContains(restoredP0, "REJECT", "28P01") {
			t.Fatalf("bound postcommit admission fence disposal rehabilitated P0: %s", restoredP0)
		}
		restoredRules, rulesErr := borrowedBoundWriterIsolationReadRules(ctx, f.fx.admin)
		if rulesErr != nil {
			t.Fatalf("bound postcommit admission restored rules read refused: %v", rulesErr)
		}
		if checkErr := borrowedBoundWriterIsolationFenceStateCheck(restoredRules, borrowedBoundWriterIsolationScopes(restoredRules), f.writerRole); checkErr == nil {
			t.Fatal("bound postcommit admission left a continuing writer-reject isolation")
		}
		healthCtx, cancelHealth := context.WithTimeout(ctx, borrowedOwnerRotationHealthBudget)
		healthErr := f.lock.Health(healthCtx)
		cancelHealth()
		anchorCtx, cancelAnchor := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
		anchorErr := b.anchor.Recheck(anchorCtx)
		cancelAnchor()
		if healthErr != nil || anchorErr != nil {
			t.Fatalf("bound postcommit admission owner health=%v anchor=%v after disposal", healthErr, anchorErr)
		}
		pristineAfter := borrowedBoundPostcommitAdmissionPristine(t, ctx, b)
		if pristineAfter != pristineBefore {
			t.Fatalf("bound postcommit admission changed target data: pristine %q -> %q", pristineBefore, pristineAfter)
		}

		// The terminal object and its copies cannot trigger another dispatch.
		if _, _, replayErr := admission.dispatchOnce(ctx, nativeAttempt); !errors.Is(replayErr, errBoundPostcommitAdmissionReplay) {
			t.Fatalf("bound postcommit admission terminal object redispatch was not refused: %v", replayErr)
		}
		admissionCopy := *admission
		if _, _, copyErr := admissionCopy.dispatchOnce(ctx, nativeAttempt); !errors.Is(copyErr, errBoundPostcommitAdmissionReplay) {
			t.Fatalf("bound postcommit admission terminal copy redispatch was not refused: %v", copyErr)
		}
		runCopy := *nativeAttempt.run
		replayCtx, cancelReplay := context.WithTimeout(ctx, 30*time.Second)
		_, _, runCopyErr := runCopy.Run(replayCtx)
		cancelReplay()
		if runCopyErr == nil || !strings.Contains(runCopyErr.Error(), "already reserved") {
			t.Fatalf("bound postcommit admission run copy replay was not refused as already reserved: %v", runCopyErr)
		}
		if nativeMarker.callsNow() != 1 {
			t.Fatalf("bound postcommit admission terminal replay added a marker callback: %d", nativeMarker.callsNow())
		}
		t.Logf("bound postcommit admission positive: owner tx pid %d distinct from prep tx pid %d; acknowledged baseline commit (guard clean op=%s, one instance-bound preparation audit) handed off through the live fence; the one native attempt admission committed its transactional restore_started marker (generation %d) and durable launch intent (op=%s app=%q, launched_at NULL) while the baseline preparation audit stayed byte-identical and unrelabelled; endpoint loss AFTER preflight produced the actual launch-stage endpoint-capability refusal with zero child start, absent receipt, zero probe/acceptance callbacks, no accepted output, unchanged catalog/data and a clean teardown with no continuing isolation; the terminal object/copies cannot redispatch",
			plan.ownerTxPID, prepTxPID, plan.rehearsalOperationID, nativeMarker.token().Generation, nativeAttemptID, obs.nativeResult.Application)
	})

	// N1: dirty/unacknowledged baseline: the handoff gate refuses without an
	// acknowledged commit and the raw coordinator dispatch is still the exact
	// dirty-guard refusal; no durable native outcome exists.
	t.Run("N1-dirty-baseline", func(t *testing.T) {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		f := b.fixture
		_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		var probeCalls, acceptanceCalls int32
		attemptID := fmt.Sprintf("bound-postcommit-admission-dirty-%d", time.Now().UnixNano())
		marker := newBorrowedBoundPostcommitAdmissionMarker(attemptID, f.adminRole, b.owner.BackendPID)
		attempt := newBorrowedBoundPostcommitAdmissionAttempt(t, ctx, b, fresh, attemptID, marker, &probeCalls, &acceptanceCalls)
		admission := newBorrowedBoundPostcommitAdmission()
		uncommitted := newBorrowedBoundBaselineCommitPlan()
		uncommitted.rehearsalOperationID = "bound-postcommit-admission-never-committed"
		if err := admission.armAfterAcknowledgedBaseline(uncommitted, nil); !errors.Is(err, errBoundPostcommitAdmissionBaseline) || admission.armedNow() {
			t.Fatalf("dirty-baseline handoff was armed without an acknowledged commit: %v", err)
		}
		beforeGuard, beforeFound, beforeErr := controlstore.ReadTargetGuard(ctx, f.controlPool, f.guardKey)
		if beforeErr != nil || !beforeFound {
			t.Fatalf("dirty-baseline original guard read refused: err=%v found=%t", beforeErr, beforeFound)
		}
		runCtx, cancelRun := context.WithTimeout(ctx, 60*time.Second)
		result, receipt, runErr := attempt.dispatchRun(runCtx)
		cancelRun()
		if runErr == nil || runErr.Error() != "bound target guard is not clean" {
			t.Fatalf("dirty-baseline raw dispatch is not the exact dirty-guard refusal: %v", runErr)
		}
		if marker.callsNow() != 1 || !marker.pidDistinct || marker.pid == b.owner.BackendPID || !marker.instanceRevalidated || !marker.endpointClosed {
			t.Fatalf("dirty-baseline marker facts: calls=%d pidDistinct=%t instance=%t endpointClosed=%t", marker.callsNow(), marker.pidDistinct, marker.instanceRevalidated, marker.endpointClosed)
		}
		borrowedBoundPostcommitAdmissionAssertNoNativeOutcome(t, "dirty baseline", attempt, marker, &probeCalls, &acceptanceCalls, 1)
		borrowedBoundPostcommitAdmissionAssertNoAcceptedOutput(t, "dirty baseline", result, receipt)
		borrowedBoundPostcommitAdmissionAssertNoHandoffDurableState(t, ctx, b, beforeGuard, "bound-postcommit-admission-never-committed", attemptID)
		borrowedBoundReentryReadinessAssertBaseline(t, "bound postcommit admission dirty baseline", ctx, b, base)
		t.Logf("dirty/unacknowledged baseline negative: the gate refused without an acknowledged commit and the raw dispatch was refused by the exact dirty guard with exactly one rolled-back marker callback, no child, absent receipt, no probe/acceptance, no native intent and complete baseline equality")
	})

	// N2: missing preparation: with no effective fence the fresh coverage
	// verification refuses before any transaction; the gate stays unarmed.
	t.Run("N2-missing-preparation", func(t *testing.T) {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		f := b.fixture
		_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		var probeCalls, acceptanceCalls int32
		attemptID := fmt.Sprintf("bound-postcommit-admission-nofence-%d", time.Now().UnixNano())
		marker := newBorrowedBoundPostcommitAdmissionMarker(attemptID, f.adminRole, b.owner.BackendPID)
		attempt := newBorrowedBoundPostcommitAdmissionAttempt(t, ctx, b, fresh, attemptID, marker, &probeCalls, &acceptanceCalls)
		plan := newBorrowedBoundBaselineCommitPlan()
		preparation := newBorrowedBoundBaselineCommitPreparation()
		stageErr := borrowedBoundBaselineCommitStageRun(ctx, t, b, fresh, 1, plan, preparation)
		if !errors.Is(stageErr, errBoundBaselineCommitCoverage) {
			t.Fatalf("missing-preparation refusal is not the real coverage stage: %v", stageErr)
		}
		admission := newBorrowedBoundPostcommitAdmission()
		if err := admission.armAfterAcknowledgedBaseline(plan, nil); !errors.Is(err, errBoundPostcommitAdmissionBaseline) || admission.armedNow() {
			t.Fatalf("missing-preparation handoff was armed: %v", err)
		}
		borrowedBoundPostcommitAdmissionAssertNoNativeOutcome(t, "missing preparation", attempt, marker, &probeCalls, &acceptanceCalls, 0)
		borrowedBoundReentryReadinessAssertBaseline(t, "bound postcommit admission missing preparation", ctx, b, base)
		t.Logf("missing-preparation negative: the fresh coverage verification refused before any transaction, the gate stayed unarmed and no native outcome exists; durable state classification: complete baseline equality, no transaction, no authorizing output")
	})

	// N3: rollback uncertainty: owner loss before the acknowledged commit
	// produces the authentic rollback-acknowledgement failure; the handoff gate
	// refuses and no native outcome exists.
	t.Run("N3-rollback-uncertainty", func(t *testing.T) {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		f := b.fixture
		_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		var probeCalls, acceptanceCalls int32
		attemptID := fmt.Sprintf("bound-postcommit-admission-rollback-%d", time.Now().UnixNano())
		marker := newBorrowedBoundPostcommitAdmissionMarker(attemptID, f.adminRole, b.owner.BackendPID)
		attempt := newBorrowedBoundPostcommitAdmissionAttempt(t, ctx, b, fresh, attemptID, marker, &probeCalls, &acceptanceCalls)
		plan := newBorrowedBoundBaselineCommitPlan()
		plan.ownerLossBeforeCommit = true
		preparation := newBorrowedBoundBaselineCommitPreparation()
		admission := newBorrowedBoundPostcommitAdmission()
		flowState, flowErr := borrowedBoundWriterIsolationRun(ctx, t, b, fresh, &borrowedBoundWriterIsolationLineage{
			consumed: true, readinessBacked: false,
			instanceID: f.instanceID, targetKey: f.guardKey,
			fingerprint:    fresh.binding.OriginalRoleFingerprint(),
			replacementOID: fresh.replacementOID, ownerPID: b.owner.BackendPID,
			issuance: &borrowedBoundWriterIsolationIssuance{},
		}, &borrowedBoundWriterIsolationHooks{
			duringProof: func(hookCtx context.Context, flow *borrowedBoundWriterIsolationFlowState) error {
				if err := borrowedBoundBaselineCommitStageRun(hookCtx, t, b, fresh, 1, plan, preparation); err != nil {
					return err
				}
				return errors.New("bound postcommit admission rollback-uncertainty control unexpectedly acknowledged a commit")
			},
		})
		if !errors.Is(flowErr, errBoundBaselineCommitUncertain) || flowErr == nil || !strings.Contains(flowErr.Error(), "rollback target-lock acceptance transaction") {
			t.Fatalf("rollback-uncertainty control is not the authentic rollback failure: %v", flowErr)
		}
		if plan.commitAcknowledged || plan.rollbackAcknowledged {
			t.Fatal("rollback-uncertainty control reported a commit or acknowledged rollback")
		}
		if err := admission.armAfterAcknowledgedBaseline(plan, flowState); !errors.Is(err, errBoundPostcommitAdmissionBaseline) || admission.armedNow() {
			t.Fatalf("rollback-uncertainty handoff was armed: %v", err)
		}
		borrowedBoundPostcommitAdmissionAssertNoNativeOutcome(t, "rollback uncertainty", attempt, marker, &probeCalls, &acceptanceCalls, 0)
		borrowedBoundReentryReadinessAssertBaseline(t, "bound postcommit admission rollback uncertainty", ctx, b, base)
		healthCtx, cancelHealth := context.WithTimeout(ctx, borrowedOwnerRotationHealthBudget)
		healthErr := f.lock.Health(healthCtx)
		cancelHealth()
		if healthErr == nil {
			t.Fatal("rollback-uncertainty control left the owner reusable")
		}
		if !flowState.cleanupRestored || flowState.cleanupErr != nil {
			t.Fatalf("rollback-uncertainty fence disposal was not clean: restored=%t err=%v", flowState.cleanupRestored, flowState.cleanupErr)
		}
		t.Logf("rollback-uncertainty negative: the owner loss before the commit produced the authentic rollback-acknowledgement failure with NO commit, the gate refused, no native marker/intent/child exists, the complete baseline stayed unchanged and the owner was retired with no reuse")
	})

	// N4: COMMIT-acknowledgement uncertainty: the actual COMMIT is dispatched
	// and its completion withheld, the committed delta is independently
	// observed, and the handoff is NEVER armed from the visible rows alone.
	t.Run("N4-commit-ack-uncertainty", func(t *testing.T) {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		f := b.fixture
		_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		controlKey := borrowedBoundReentryDifferentKey(t, b)
		fault, reset, err := recovery.ArmTargetLockCommitAmbiguityFault()
		if err != nil {
			t.Fatalf("bound postcommit admission fault reservation refused: %v", err)
		}
		defer reset()
		acqCtx, cancelAcq := context.WithTimeout(ctx, borrowedOwnerRotationAuthBudget)
		wrappedLock, err := recovery.AcquireTargetLock(acqCtx, f.controlDSN, controlKey, 3*time.Second, 10*time.Millisecond)
		cancelAcq()
		if err != nil {
			t.Fatalf("bound postcommit admission wrapped owner acquisition refused: %v", err)
		}
		defer func() {
			releaseCtx, cancelRelease := context.WithTimeout(context.Background(), borrowedOwnerRotationCleanupBudget)
			_ = wrappedLock.Release(releaseCtx)
			cancelRelease()
		}()
		var wrappedPID int
		var wrappedStart time.Time
		probeCtx, cancelProbe := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
		probeErr := wrappedLock.WithTransaction(probeCtx, func(txCtx context.Context, tx pgx.Tx) error {
			return tx.QueryRow(txCtx, `SELECT pid::int, backend_start FROM pg_stat_activity WHERE pid = pg_backend_pid()`).Scan(&wrappedPID, &wrappedStart)
		})
		cancelProbe()
		if probeErr != nil || wrappedPID <= 0 || wrappedPID == b.owner.BackendPID {
			t.Fatalf("bound postcommit admission wrapped owner identity probe refused: pid=%d owner=%d err=%v", wrappedPID, b.owner.BackendPID, probeErr)
		}
		var probeCalls, acceptanceCalls int32
		attemptID := fmt.Sprintf("bound-postcommit-admission-ackloss-%d", time.Now().UnixNano())
		marker := newBorrowedBoundPostcommitAdmissionMarker(attemptID, f.adminRole, b.owner.BackendPID)
		attempt := newBorrowedBoundPostcommitAdmissionAttempt(t, ctx, b, fresh, attemptID, marker, &probeCalls, &acceptanceCalls)
		k1, k2 := controlKey.AdvisoryLockKey()
		plan := newBorrowedBoundBaselineCommitPlan()
		plan.commitAckLoss = true
		plan.lockOverride = wrappedLock
		plan.ownerPIDOverride = wrappedPID
		plan.ownerStartOverride = wrappedStart
		plan.namespaceK1Override = k1
		plan.namespaceK2Override = k2
		plan.operationIDOverride = fmt.Sprintf("bound-postcommit-admission-ackloss-op-%d", time.Now().UnixNano())
		preparation := newBorrowedBoundBaselineCommitPreparation()
		if err := fault.ArmGate(); err != nil {
			t.Fatalf("bound postcommit admission fault gate arm refused: %v", err)
		}
		witness, watchCleanup := startBorrowedBoundBaselineCommitFaultWatcher(ctx, t, b, fault, plan.operationIDOverride)
		defer func() {
			if cleanupErr := watchCleanup(); cleanupErr != nil {
				t.Errorf("bound postcommit admission fault worker cleanup refused: %v", cleanupErr)
			}
		}()
		admission := newBorrowedBoundPostcommitAdmission()
		flowState, flowErr := borrowedBoundWriterIsolationRun(ctx, t, b, fresh, &borrowedBoundWriterIsolationLineage{
			consumed: true, readinessBacked: false,
			instanceID: f.instanceID, targetKey: f.guardKey,
			fingerprint:    fresh.binding.OriginalRoleFingerprint(),
			replacementOID: fresh.replacementOID, ownerPID: b.owner.BackendPID,
			issuance: &borrowedBoundWriterIsolationIssuance{},
		}, &borrowedBoundWriterIsolationHooks{
			duringProof: func(hookCtx context.Context, flow *borrowedBoundWriterIsolationFlowState) error {
				return borrowedBoundBaselineCommitStageRun(hookCtx, t, b, fresh, 1, plan, preparation)
			},
		})
		res := witness.join(t)
		if flowErr == nil || !errors.Is(flowErr, errBoundBaselineCommitAckLoss) || !strings.Contains(flowErr.Error(), "commit target-lock acceptance transaction") {
			t.Fatalf("acknowledgement-loss control is not the real ambiguous COMMIT path: flowErr=%v worker=%+v", flowErr, res)
		}
		if res.err != nil || !res.dispatch || !res.withheld || !res.broken {
			t.Fatalf("acknowledgement-loss fault witnesses: err=%v dispatch=%t withheld=%t broken=%t", res.err, res.dispatch, res.withheld, res.broken)
		}
		if plan.commitAcknowledged || plan.successInferred || !plan.commitAckLost {
			t.Fatalf("acknowledgement loss was misclassified: acknowledged=%t inferred=%t ackLost=%t", plan.commitAcknowledged, plan.successInferred, plan.commitAckLost)
		}
		if err := admission.armAfterAcknowledgedBaseline(plan, flowState); !errors.Is(err, errBoundPostcommitAdmissionBaseline) || admission.armedNow() {
			t.Fatalf("acknowledgement-loss handoff was armed from visible rows: %v", err)
		}
		borrowedBoundPostcommitAdmissionAssertNoNativeOutcome(t, "commit acknowledgement loss", attempt, marker, &probeCalls, &acceptanceCalls, 0)
		borrowedBoundPostcommitAdmissionAssertPreservedDurableDelta(t, ctx, b, plan.operationIDOverride)
		if !flowState.cleanupRestored || flowState.cleanupErr != nil {
			t.Fatalf("acknowledgement-loss fence disposal was not clean: restored=%t err=%v", flowState.cleanupRestored, flowState.cleanupErr)
		}
		t.Logf("acknowledgement-loss negative: the ACTUAL COMMIT was dispatched and its validated completion withheld, the committed delta was independently observed while withheld, the handoff was NEVER armed from the visible rows, no native marker/intent/child exists and the fence was disposed cleanly")
	})

	// N5: disposed/stale isolation: a flow that ends with the handoff armed but
	// never dispatched can neither hand off afterwards nor be replayed from its
	// old flow state.
	t.Run("N5-disposed-isolation", func(t *testing.T) {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		f := b.fixture
		_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		var probeCalls, acceptanceCalls int32
		attemptID := fmt.Sprintf("bound-postcommit-admission-disposed-%d", time.Now().UnixNano())
		marker := newBorrowedBoundPostcommitAdmissionMarker(attemptID, f.adminRole, b.owner.BackendPID)
		attempt := newBorrowedBoundPostcommitAdmissionAttempt(t, ctx, b, fresh, attemptID, marker, &probeCalls, &acceptanceCalls)
		plan := newBorrowedBoundBaselineCommitPlan()
		preparation := newBorrowedBoundBaselineCommitPreparation()
		admission := newBorrowedBoundPostcommitAdmission()
		obs := &borrowedBoundPostcommitAdmissionObservation{}
		flowState, flowErr := borrowedBoundWriterIsolationRun(ctx, t, b, fresh, &borrowedBoundWriterIsolationLineage{
			consumed: true, readinessBacked: false,
			instanceID: f.instanceID, targetKey: f.guardKey,
			fingerprint:    fresh.binding.OriginalRoleFingerprint(),
			replacementOID: fresh.replacementOID, ownerPID: b.owner.BackendPID,
			issuance: &borrowedBoundWriterIsolationIssuance{},
		}, &borrowedBoundWriterIsolationHooks{
			duringProof: func(hookCtx context.Context, flow *borrowedBoundWriterIsolationFlowState) error {
				if err := borrowedBoundBaselineCommitStageRun(hookCtx, t, b, fresh, 1, plan, preparation); err != nil {
					return err
				}
				if !plan.commitAcknowledged {
					return errBoundPostcommitAdmissionBaseline
				}
				obs.attemptID = attemptID
				obs.baselineOperation = plan.rehearsalOperationID
				obs.baselineAuditCount, obs.baselineAuditDigest = borrowedBoundPostcommitAdmissionAuditDigest(hookCtx, t, b, controlstore.ActionTargetGuardRebuild, plan.rehearsalOperationID)
				if err := admission.armAfterAcknowledgedBaseline(plan, flow); err != nil {
					return err
				}
				return nil
			},
		})
		if flowErr != nil || flowState.cleanupErr != nil || !flowState.cleanupRestored || !admission.armedNow() || admission.dispatchedNow() {
			t.Fatalf("disposed-isolation control setup refused: flowErr=%v restored=%t cleanupErr=%v armed=%t dispatched=%t", flowErr, flowState.cleanupRestored, flowState.cleanupErr, admission.armedNow(), admission.dispatchedNow())
		}
		disposedResult, disposedReceipt, terminalErr := admission.dispatchOnce(ctx, attempt)
		if !errors.Is(terminalErr, errBoundPostcommitAdmissionTerminal) {
			t.Fatalf("a completed flow state handed off: %v", terminalErr)
		}
		borrowedBoundPostcommitAdmissionAssertNoAcceptedOutput(t, "disposed isolation", disposedResult, disposedReceipt)
		admissionCopy := *admission
		copyResult, copyReceipt, copyErr := admissionCopy.dispatchOnce(ctx, attempt)
		if !errors.Is(copyErr, errBoundPostcommitAdmissionTerminal) {
			t.Fatalf("a copied completed flow state handed off: %v", copyErr)
		}
		borrowedBoundPostcommitAdmissionAssertNoAcceptedOutput(t, "disposed isolation copy", copyResult, copyReceipt)
		borrowedBoundPostcommitAdmissionAssertNoNativeOutcome(t, "disposed isolation", attempt, marker, &probeCalls, &acceptanceCalls, 0)
		serverIP, ipErr := f.fx.container.ContainerIP(ctx)
		if ipErr != nil {
			t.Fatalf("disposed-isolation endpoint refused: %v", ipErr)
		}
		if err := borrowedBoundWriterIsolationAwaitState(t, ctx, f, serverIP, f.writerRole, b.passwordP1, "AUTH_OK", "", 10*time.Second); err != nil {
			t.Fatalf("disposed isolation did not restore the HBA: %v", err)
		}
		borrowedBoundPostcommitAdmissionAssertPreservedCommittedBaseline(t, ctx, b, plan, obs)
		_ = base
		t.Logf("disposed/stale-isolation negative: the completed flow state (HBA restored) could not hand off through the gate or any copy, no native marker/intent/child exists and the captured committed baseline audit digest/classification is preserved")
	})

	// N6: incomplete isolation: a partial fence leaves an uncovered route that
	// still authenticates a genuine P1, and the fresh coverage refuses.
	t.Run("N6-incomplete-isolation", func(t *testing.T) {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		f := b.fixture
		admin := f.fx.admin
		_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		var probeCalls, acceptanceCalls int32
		attemptID := fmt.Sprintf("bound-postcommit-admission-partial-%d", time.Now().UnixNano())
		marker := newBorrowedBoundPostcommitAdmissionMarker(attemptID, f.adminRole, b.owner.BackendPID)
		attempt := newBorrowedBoundPostcommitAdmissionAttempt(t, ctx, b, fresh, attemptID, marker, &probeCalls, &acceptanceCalls)
		hbaPath, err := borrowedBoundWriterIsolationHBAPath(ctx, admin)
		if err != nil {
			t.Fatalf("incomplete-isolation HBA path refused: %v", err)
		}
		originalContent, err := borrowedBoundWriterIsolationReadHBA(ctx, f, hbaPath)
		if err != nil {
			t.Fatalf("incomplete-isolation HBA read refused: %v", err)
		}
		rules, err := borrowedBoundWriterIsolationReadRules(ctx, admin)
		if err != nil {
			t.Fatalf("incomplete-isolation rules read refused: %v", err)
		}
		scopes := borrowedBoundWriterIsolationScopes(rules)
		if len(scopes) < 2 {
			t.Fatalf("incomplete-isolation control requires at least two scopes, got %d", len(scopes))
		}
		partial := borrowedBoundWriterIsolationFencedContent(originalContent, scopes[:1], f.writerRole)
		if err := borrowedBoundWriterIsolationWriteHBA(ctx, t, f, hbaPath, partial); err != nil {
			t.Fatalf("incomplete-isolation fence write refused: %v", err)
		}
		defer func() {
			cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancelCleanup()
			_ = borrowedBoundWriterIsolationWriteHBA(cleanupCtx, t, f, hbaPath, originalContent)
			_ = borrowedBoundWriterIsolationReload(cleanupCtx, admin)
		}()
		if err := borrowedBoundWriterIsolationReload(ctx, admin); err != nil {
			t.Fatalf("incomplete-isolation reload refused: %v", err)
		}
		serverIP, ipErr := f.fx.container.ContainerIP(ctx)
		if ipErr != nil {
			t.Fatalf("incomplete-isolation endpoint refused: %v", ipErr)
		}
		unfenced := runHelperAuthCheck(t, ctx, f.fx.containerID, "serverip", serverIP, f.writerRole, b.passwordP1)
		if !strings.Contains(unfenced, "state=AUTH_OK") {
			t.Fatalf("incomplete-isolation control did not prove the uncovered route admits P1: %s", unfenced)
		}
		plan := newBorrowedBoundBaselineCommitPlan()
		preparation := newBorrowedBoundBaselineCommitPreparation()
		stageErr := borrowedBoundBaselineCommitStageRun(ctx, t, b, fresh, 1, plan, preparation)
		if !errors.Is(stageErr, errBoundBaselineCommitCoverage) {
			t.Fatalf("incomplete-isolation refusal is not the real coverage stage: %v", stageErr)
		}
		admission := newBorrowedBoundPostcommitAdmission()
		if err := admission.armAfterAcknowledgedBaseline(plan, nil); !errors.Is(err, errBoundPostcommitAdmissionBaseline) || admission.armedNow() {
			t.Fatalf("incomplete-isolation handoff was armed: %v", err)
		}
		borrowedBoundPostcommitAdmissionAssertNoNativeOutcome(t, "incomplete isolation", attempt, marker, &probeCalls, &acceptanceCalls, 0)
		borrowedBoundReentryReadinessAssertBaseline(t, "bound postcommit admission incomplete isolation", ctx, b, base)
		t.Logf("incomplete-isolation negative: the partial fence left the container-address route admitting a genuine P1 and the fresh coverage verification refused; no native outcome, durable state classification: complete baseline equality, no transaction, no authorizing output")
	})

	// N7: admitting route / retained genuine P1: the fresh route and census
	// gates refuse and the handoff stays unarmed.
	t.Run("N7-admitting-route", func(t *testing.T) {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		f := b.fixture
		_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		restore := borrowedBoundBaselineCommitApplyFence(ctx, t, b, fresh)
		defer restore()
		var probeCalls, acceptanceCalls int32
		attemptID := fmt.Sprintf("bound-postcommit-admission-route-%d", time.Now().UnixNano())
		marker := newBorrowedBoundPostcommitAdmissionMarker(attemptID, f.adminRole, b.owner.BackendPID)
		attempt := newBorrowedBoundPostcommitAdmissionAttempt(t, ctx, b, fresh, attemptID, marker, &probeCalls, &acceptanceCalls)
		plan := newBorrowedBoundBaselineCommitPlan()
		plan.routeOverride = func(transport string) (string, bool) {
			if transport == "loopback" {
				return "AUTHCHECK state=AUTH_OK", true
			}
			return "", false
		}
		preparation := newBorrowedBoundBaselineCommitPreparation()
		stageErr := borrowedBoundBaselineCommitStageRun(ctx, t, b, fresh, 1, plan, preparation)
		if !errors.Is(stageErr, errBoundBaselineCommitRoute) {
			t.Fatalf("admitting-route refusal is not the real route stage: %v", stageErr)
		}
		admission := newBorrowedBoundPostcommitAdmission()
		if err := admission.armAfterAcknowledgedBaseline(plan, nil); !errors.Is(err, errBoundPostcommitAdmissionBaseline) || admission.armedNow() {
			t.Fatalf("admitting-route handoff was armed: %v", err)
		}
		borrowedBoundPostcommitAdmissionAssertNoNativeOutcome(t, "admitting route", attempt, marker, &probeCalls, &acceptanceCalls, 0)
		borrowedBoundReentryReadinessAssertBaseline(t, "bound postcommit admission admitting route", ctx, b, base)
		t.Logf("admitting-route negative: complete effective coverage with one transport admitting P1 refused at the real route stage before any transaction; no native outcome")
	})
	t.Run("N7-retained-p1", func(t *testing.T) {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		f := b.fixture
		_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		var probeCalls, acceptanceCalls int32
		attemptID := fmt.Sprintf("bound-postcommit-admission-retained-%d", time.Now().UnixNano())
		marker := newBorrowedBoundPostcommitAdmissionMarker(attemptID, f.adminRole, b.owner.BackendPID)
		attempt := newBorrowedBoundPostcommitAdmissionAttempt(t, ctx, b, fresh, attemptID, marker, &probeCalls, &acceptanceCalls)
		retainCtx, cancelRetain := context.WithTimeout(ctx, borrowedOwnerRotationAuthBudget)
		retained, retainErr := pgx.Connect(retainCtx, borrowedReplacementP1TargetDSN(t, b))
		cancelRetain()
		if retainErr != nil {
			t.Fatalf("retained-P1 control connect refused: %v", retainErr)
		}
		defer borrowedSuccessorCloseConn(t, retained)
		restore := borrowedBoundBaselineCommitApplyFence(ctx, t, b, fresh)
		defer restore()
		plan := newBorrowedBoundBaselineCommitPlan()
		preparation := newBorrowedBoundBaselineCommitPreparation()
		stageErr := borrowedBoundBaselineCommitStageRun(ctx, t, b, fresh, 1, plan, preparation)
		if !errors.Is(stageErr, errBoundBaselineCommitCensus) {
			t.Fatalf("retained-P1 refusal is not the real census stage: %v", stageErr)
		}
		admission := newBorrowedBoundPostcommitAdmission()
		if err := admission.armAfterAcknowledgedBaseline(plan, nil); !errors.Is(err, errBoundPostcommitAdmissionBaseline) || admission.armedNow() {
			t.Fatalf("retained-P1 handoff was armed: %v", err)
		}
		borrowedBoundPostcommitAdmissionAssertNoNativeOutcome(t, "retained writer", attempt, marker, &probeCalls, &acceptanceCalls, 0)
		borrowedBoundReentryReadinessAssertBaseline(t, "bound postcommit admission retained writer", ctx, b, base)
		t.Logf("retained-genuine-P1 negative: the pre-existing genuine P1 session made the fresh census refuse before any transaction; no native outcome")
	})

	// N8: observer failure at the stage, control failure at the handoff
	// revalidation after the acknowledged commit.
	t.Run("N8-observer-failure", func(t *testing.T) {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		f := b.fixture
		_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		restore := borrowedBoundBaselineCommitApplyFence(ctx, t, b, fresh)
		defer restore()
		var probeCalls, acceptanceCalls int32
		attemptID := fmt.Sprintf("bound-postcommit-admission-observer-%d", time.Now().UnixNano())
		marker := newBorrowedBoundPostcommitAdmissionMarker(attemptID, f.adminRole, b.owner.BackendPID)
		attempt := newBorrowedBoundPostcommitAdmissionAttempt(t, ctx, b, fresh, attemptID, marker, &probeCalls, &acceptanceCalls)
		plan := newBorrowedBoundBaselineCommitPlan()
		plan.observerNologin = true
		preparation := newBorrowedBoundBaselineCommitPreparation()
		stageErr := borrowedBoundBaselineCommitStageRun(ctx, t, b, fresh, 1, plan, preparation)
		if !errors.Is(stageErr, errBoundBaselineCommitObserver) {
			t.Fatalf("observer-failure refusal is not the real observer stage: %v", stageErr)
		}
		admission := newBorrowedBoundPostcommitAdmission()
		if err := admission.armAfterAcknowledgedBaseline(plan, nil); !errors.Is(err, errBoundPostcommitAdmissionBaseline) || admission.armedNow() {
			t.Fatalf("observer-failure handoff was armed: %v", err)
		}
		borrowedBoundPostcommitAdmissionAssertNoNativeOutcome(t, "observer failure", attempt, marker, &probeCalls, &acceptanceCalls, 0)
		borrowedBoundReentryReadinessAssertBaseline(t, "bound postcommit admission observer failure", ctx, b, base)
		t.Logf("observer-failure negative: the suspended protected observer role refused the real observer stage before any transaction; no native outcome")
	})
	t.Run("N8-control-failure", func(t *testing.T) {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		f := b.fixture
		_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		var probeCalls, acceptanceCalls int32
		attemptID := fmt.Sprintf("bound-postcommit-admission-control-%d", time.Now().UnixNano())
		marker := newBorrowedBoundPostcommitAdmissionMarker(attemptID, f.adminRole, b.owner.BackendPID)
		attempt := newBorrowedBoundPostcommitAdmissionAttempt(t, ctx, b, fresh, attemptID, marker, &probeCalls, &acceptanceCalls)
		plan := newBorrowedBoundBaselineCommitPlan()
		preparation := newBorrowedBoundBaselineCommitPreparation()
		admission := newBorrowedBoundPostcommitAdmission()
		flowState, flowErr := borrowedBoundWriterIsolationRun(ctx, t, b, fresh, &borrowedBoundWriterIsolationLineage{
			consumed: true, readinessBacked: false,
			instanceID: f.instanceID, targetKey: f.guardKey,
			fingerprint:    fresh.binding.OriginalRoleFingerprint(),
			replacementOID: fresh.replacementOID, ownerPID: b.owner.BackendPID,
			issuance: &borrowedBoundWriterIsolationIssuance{},
		}, &borrowedBoundWriterIsolationHooks{
			duringProof: func(hookCtx context.Context, flow *borrowedBoundWriterIsolationFlowState) error {
				if err := borrowedBoundBaselineCommitStageRun(hookCtx, t, b, fresh, 1, plan, preparation); err != nil {
					return err
				}
				if !plan.commitAcknowledged {
					return errBoundPostcommitAdmissionBaseline
				}
				if err := admission.armAfterAcknowledgedBaseline(plan, flow); err != nil {
					return err
				}
				restoreControl := borrowedBoundPostcommitAdmissionSuspendControl(t, hookCtx, b)
				defer restoreControl()
				return borrowedBoundPostcommitAdmissionRevalidate(hookCtx, t, b, fresh, plan, flow)
			},
		})
		if !errors.Is(flowErr, errBoundPostcommitAdmissionRevalidate) {
			t.Fatalf("control-failure refusal is not the real handoff revalidation stage: %v", flowErr)
		}
		if !plan.commitAcknowledged || plan.commitAckLost || plan.rollbackAcknowledged {
			t.Fatalf("control-failure baseline classification: acknowledged=%t ackLost=%t rollback=%t", plan.commitAcknowledged, plan.commitAckLost, plan.rollbackAcknowledged)
		}
		if !admission.armedNow() || admission.dispatchedNow() {
			t.Fatalf("control-failure handoff facts: armed=%t dispatched=%t", admission.armedNow(), admission.dispatchedNow())
		}
		borrowedBoundPostcommitAdmissionAssertNoNativeOutcome(t, "control failure", attempt, marker, &probeCalls, &acceptanceCalls, 0)
		borrowedBoundPostcommitAdmissionAssertPreservedDurableDelta(t, ctx, b, plan.rehearsalOperationID)
		if !flowState.cleanupRestored || flowState.cleanupErr != nil {
			t.Fatalf("control-failure fence disposal was not clean: restored=%t err=%v", flowState.cleanupRestored, flowState.cleanupErr)
		}
		_ = base
		t.Logf("control-failure negative: the suspended protected control route refused the handoff revalidation after the acknowledged baseline commit; the committed preparation delta is preserved, the gate never dispatched and no native marker/intent/child exists")
	})

	// N9: owner/anchor loss: anchor loss refuses before the fence; owner loss
	// after the committed native intent retains the committed marker/intent and
	// never continues positively.
	t.Run("N9-anchor-loss", func(t *testing.T) {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		f := b.fixture
		_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		var probeCalls, acceptanceCalls int32
		attemptID := fmt.Sprintf("bound-postcommit-admission-anchor-%d", time.Now().UnixNano())
		marker := newBorrowedBoundPostcommitAdmissionMarker(attemptID, f.adminRole, b.owner.BackendPID)
		attempt := newBorrowedBoundPostcommitAdmissionAttempt(t, ctx, b, fresh, attemptID, marker, &probeCalls, &acceptanceCalls)
		facts := b.anchor.Diagnostics()
		if !facts.Present || facts.BackendPID <= 0 {
			t.Fatal("anchor-loss control requires the genuine anchor facts")
		}
		termCtx, cancelTerm := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
		var terminated bool
		termErr := f.controlPool.QueryRow(termCtx, `SELECT pg_terminate_backend($1)`, facts.BackendPID).Scan(&terminated)
		cancelTerm()
		if termErr != nil || !terminated {
			t.Fatalf("anchor-loss termination refused: %v", termErr)
		}
		admission := newBorrowedBoundPostcommitAdmission()
		flowState, flowErr := borrowedBoundWriterIsolationRun(ctx, t, b, fresh, &borrowedBoundWriterIsolationLineage{
			consumed: true, readinessBacked: false,
			instanceID: f.instanceID, targetKey: f.guardKey,
			fingerprint:    fresh.binding.OriginalRoleFingerprint(),
			replacementOID: fresh.replacementOID, ownerPID: b.owner.BackendPID,
			issuance: &borrowedBoundWriterIsolationIssuance{},
		}, &borrowedBoundWriterIsolationHooks{
			duringProof: func(hookCtx context.Context, flow *borrowedBoundWriterIsolationFlowState) error {
				return errors.New("anchor-loss control unexpectedly reached the duringProof interval")
			},
		})
		if !errors.Is(flowErr, errWriterIsolationOwner) {
			t.Fatalf("anchor-loss refusal is not the real owner/anchor stage: %v", flowErr)
		}
		if admission.armedNow() {
			t.Fatal("anchor-loss control armed the handoff")
		}
		borrowedBoundPostcommitAdmissionAssertNoNativeOutcome(t, "anchor loss", attempt, marker, &probeCalls, &acceptanceCalls, 0)
		borrowedBoundReentryReadinessAssertBaseline(t, "bound postcommit admission anchor loss", ctx, b, base)
		if flowState.fenceWritten {
			t.Fatal("anchor-loss control applied a fence after the anchor was lost")
		}
		t.Logf("anchor-loss negative: the terminated anchor refused the real owner/anchor stage before the fence; the handoff stayed unarmed and no native outcome exists")
	})
	t.Run("N9-owner-loss-after-intent", func(t *testing.T) {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		f := b.fixture
		_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		var probeCalls, acceptanceCalls int32
		attemptID := fmt.Sprintf("bound-postcommit-admission-ownerloss-%d", time.Now().UnixNano())
		marker := newBorrowedBoundPostcommitAdmissionMarker(attemptID, f.adminRole, b.owner.BackendPID)
		attempt := newBorrowedBoundPostcommitAdmissionAttempt(t, ctx, b, fresh, attemptID, marker, &probeCalls, &acceptanceCalls)
		plan := newBorrowedBoundBaselineCommitPlan()
		preparation := newBorrowedBoundBaselineCommitPreparation()
		admission := newBorrowedBoundPostcommitAdmission()
		obs := &borrowedBoundPostcommitAdmissionObservation{}
		var intentCommitted bool
		flowState, flowErr := borrowedBoundWriterIsolationRun(ctx, t, b, fresh, &borrowedBoundWriterIsolationLineage{
			consumed: true, readinessBacked: false,
			instanceID: f.instanceID, targetKey: f.guardKey,
			fingerprint:    fresh.binding.OriginalRoleFingerprint(),
			replacementOID: fresh.replacementOID, ownerPID: b.owner.BackendPID,
			issuance: &borrowedBoundWriterIsolationIssuance{},
		}, &borrowedBoundWriterIsolationHooks{
			duringProof: func(hookCtx context.Context, flow *borrowedBoundWriterIsolationFlowState) error {
				if err := borrowedBoundPostcommitAdmissionDuringProof(hookCtx, t, b, fresh, attempt, marker, admission, plan, preparation, 1, &probeCalls, &acceptanceCalls, obs, flow, &borrowedBoundPostcommitAdmissionHookPlan{
					attemptID: attemptID, expect: "launch",
				}); err != nil {
					return err
				}
				intentCommitted = true
				lossCtx, cancelLoss := context.WithTimeout(context.Background(), borrowedOwnerRotationQueryBudget)
				var terminated bool
				lossErr := f.controlPool.QueryRow(lossCtx, `SELECT pg_terminate_backend($1)`, b.owner.BackendPID).Scan(&terminated)
				cancelLoss()
				if lossErr != nil || !terminated {
					return fmt.Errorf("owner-loss-after-intent termination refused: %v", lossErr)
				}
				return nil
			},
		})
		if flowErr == nil || !errors.Is(flowErr, errWriterIsolationOwner) {
			t.Fatalf("owner-loss-after-intent refusal is not the real post-fence owner stage: %v", flowErr)
		}
		if !intentCommitted || !plan.commitAcknowledged {
			t.Fatalf("owner-loss-after-intent classification: intent=%t acknowledged=%t", intentCommitted, plan.commitAcknowledged)
		}
		borrowedBoundPostcommitAdmissionAssertNativeIntent(ctx, t, b, fresh, attempt, marker, obs.nativeResult, obs.nativeReceipt, &probeCalls, &acceptanceCalls, obs)
		healthCtx, cancelHealth := context.WithTimeout(ctx, borrowedOwnerRotationHealthBudget)
		healthErr := f.lock.Health(healthCtx)
		cancelHealth()
		if healthErr == nil {
			t.Fatal("owner-loss-after-intent control left the owner reusable")
		}
		if !flowState.cleanupRestored || flowState.cleanupErr != nil {
			t.Fatalf("owner-loss-after-intent fence disposal was not clean: restored=%t err=%v", flowState.cleanupRestored, flowState.cleanupErr)
		}
		if _, _, replayErr := admission.dispatchOnce(ctx, attempt); !errors.Is(replayErr, errBoundPostcommitAdmissionReplay) {
			t.Fatalf("owner-loss-after-intent object redispatched: %v", replayErr)
		}
		t.Logf("owner-loss-after-committed-intent negative: the committed marker/intent and historical preparation audit were retained, the owner fell unusable, no positive continuation exists and the fence was disposed cleanly")
	})

	// N10: replacement catalog mismatch: the fresh catalog stage refuses and
	// the handoff stays unarmed.
	t.Run("N10-catalog-mismatch", func(t *testing.T) {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		f := b.fixture
		_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		restore := borrowedBoundBaselineCommitApplyFence(ctx, t, b, fresh)
		defer restore()
		var probeCalls, acceptanceCalls int32
		attemptID := fmt.Sprintf("bound-postcommit-admission-catalog-%d", time.Now().UnixNano())
		marker := newBorrowedBoundPostcommitAdmissionMarker(attemptID, f.adminRole, b.owner.BackendPID)
		attempt := newBorrowedBoundPostcommitAdmissionAttempt(t, ctx, b, fresh, attemptID, marker, &probeCalls, &acceptanceCalls)
		plan := newBorrowedBoundBaselineCommitPlan()
		plan.renameTargetBeforeStage = true
		preparation := newBorrowedBoundBaselineCommitPreparation()
		stageErr := borrowedBoundBaselineCommitStageRun(ctx, t, b, fresh, 1, plan, preparation)
		if !errors.Is(stageErr, errBoundBaselineCommitCatalog) {
			t.Fatalf("replacement-change refusal is not the real catalog stage: %v", stageErr)
		}
		admission := newBorrowedBoundPostcommitAdmission()
		if err := admission.armAfterAcknowledgedBaseline(plan, nil); !errors.Is(err, errBoundPostcommitAdmissionBaseline) || admission.armedNow() {
			t.Fatalf("replacement-change handoff was armed: %v", err)
		}
		borrowedBoundPostcommitAdmissionAssertNoNativeOutcome(t, "replacement change", attempt, marker, &probeCalls, &acceptanceCalls, 0)
		borrowedBoundReentryReadinessAssertBaseline(t, "bound postcommit admission replacement change", ctx, b, base)
		t.Logf("replacement-change negative: the actual replacement catalog identity mismatch refused at the real catalog stage before any transaction; no native outcome")
	})

	// N11: wrong instance/key/role/preparation-operation provenance: each
	// refuses at its real stage with the acknowledged rollback and no handoff.
	t.Run("N11-wrong-provenance", func(t *testing.T) {
		for _, wrong := range []string{"key", "operation", "role", "instance"} {
			wrong := wrong
			func() {
				b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
				f := b.fixture
				_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
				base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
				restore := borrowedBoundBaselineCommitApplyFence(ctx, t, b, fresh)
				defer restore()
				var probeCalls, acceptanceCalls int32
				attemptID := fmt.Sprintf("bound-postcommit-admission-wrong-%s-%d", wrong, time.Now().UnixNano())
				marker := newBorrowedBoundPostcommitAdmissionMarker(attemptID, f.adminRole, b.owner.BackendPID)
				attempt := newBorrowedBoundPostcommitAdmissionAttempt(t, ctx, b, fresh, attemptID, marker, &probeCalls, &acceptanceCalls)
				plan := newBorrowedBoundBaselineCommitPlan()
				wantContains := ""
				switch wrong {
				case "key":
					diffKey := borrowedBoundReentryDifferentKey(t, b)
					if count := borrowedReplacementGuardRowCount(t, ctx, f.controlPool, diffKey.String()); count != 0 {
						t.Fatalf("wrong-key control key already has %d guard rows", count)
					}
					plan.fenceKeyOverride = diffKey.String()
					wantContains = "no guard row exists"
				case "operation":
					plan.fenceOperationOverride = f.operation + "-not-the-original"
					wantContains = "operation provenance"
				case "role":
					observerTarget, err := controlstore.ParseDSNTarget(f.observerTargetDSN)
					if err != nil {
						t.Fatalf("wrong-role observer target parse refused: %v", err)
					}
					wrongFingerprint := observerTarget.DataTargetFingerprint().RoleFingerprint
					if wrongFingerprint == "" || wrongFingerprint == fresh.binding.OriginalRoleFingerprint() {
						t.Fatal("wrong-role control did not obtain a distinct real fingerprint")
					}
					plan.expectedFingerprint = wrongFingerprint
				case "instance":
					plan.instanceIDOverride = borrowedBoundBaselineCommitMutantInstanceID(t, b)
				}
				preparation := newBorrowedBoundBaselineCommitPreparation()
				stageErr := borrowedBoundBaselineCommitStageRun(ctx, t, b, fresh, 1, plan, preparation)
				if wrong == "role" || wrong == "instance" {
					if !errors.Is(stageErr, errBoundBaselineCommitInstance) {
						t.Fatalf("wrong-%s refusal is not the real instance-binding stage: %v", wrong, stageErr)
					}
					if wrong == "instance" && !errors.Is(stageErr, controlstore.ErrInstanceNotFound) {
						t.Fatalf("wrong-instance refusal is not the no-row LockInstance stage: %v", stageErr)
					}
				} else {
					if !errors.Is(stageErr, errGuardRowWindowRowRefused) || (wantContains != "" && (stageErr == nil || !strings.Contains(stageErr.Error(), wantContains))) {
						t.Fatalf("wrong-%s refusal is not the real fence stage: %v", wrong, stageErr)
					}
				}
				if !plan.rollbackAcknowledged {
					t.Fatalf("wrong-%s refusal rollback was not acknowledged", wrong)
				}
				if plan.commitAcknowledged || plan.commitAckLost {
					t.Fatalf("wrong-%s refusal was reported as a commit outcome", wrong)
				}
				admission := newBorrowedBoundPostcommitAdmission()
				if err := admission.armAfterAcknowledgedBaseline(plan, nil); !errors.Is(err, errBoundPostcommitAdmissionBaseline) || admission.armedNow() {
					t.Fatalf("wrong-%s handoff was armed: %v", wrong, err)
				}
				borrowedBoundPostcommitAdmissionAssertNoNativeOutcome(t, "wrong "+wrong, attempt, marker, &probeCalls, &acceptanceCalls, 0)
				borrowedBoundReentryReadinessAssertBaseline(t, "bound postcommit admission wrong "+wrong, ctx, b, base)
				t.Logf("wrong-%s negative: the real mismatched provenance refused at its exact stage with the acknowledged rollback, the handoff stayed unarmed and no native outcome exists", wrong)
			}()
		}
	})

	// N12: early shared-prefix loss at the stage; late shared-prefix loss AFTER
	// the acknowledged baseline but BEFORE native admission: no native
	// marker/intent survives.
	t.Run("N12-early-prefix-loss", func(t *testing.T) {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		f := b.fixture
		_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		restore := borrowedBoundBaselineCommitApplyFence(ctx, t, b, fresh)
		defer restore()
		var probeCalls, acceptanceCalls int32
		attemptID := fmt.Sprintf("bound-postcommit-admission-prefix-%d", time.Now().UnixNano())
		marker := newBorrowedBoundPostcommitAdmissionMarker(attemptID, f.adminRole, b.owner.BackendPID)
		attempt := newBorrowedBoundPostcommitAdmissionAttempt(t, ctx, b, fresh, attemptID, marker, &probeCalls, &acceptanceCalls)
		plan := newBorrowedBoundBaselineCommitPlan()
		plan.prefixLoss = true
		preparation := newBorrowedBoundBaselineCommitPreparation()
		stageErr := borrowedBoundBaselineCommitStageRun(ctx, t, b, fresh, 1, plan, preparation)
		if !errors.Is(stageErr, errBoundBaselineCommitPrefix) {
			t.Fatalf("early prefix-loss refusal is not the real prefix stage: %v", stageErr)
		}
		admission := newBorrowedBoundPostcommitAdmission()
		if err := admission.armAfterAcknowledgedBaseline(plan, nil); !errors.Is(err, errBoundPostcommitAdmissionBaseline) || admission.armedNow() {
			t.Fatalf("early prefix-loss handoff was armed: %v", err)
		}
		borrowedBoundPostcommitAdmissionAssertNoNativeOutcome(t, "early prefix loss", attempt, marker, &probeCalls, &acceptanceCalls, 0)
		borrowedBoundReentryReadinessAssertBaseline(t, "bound postcommit admission early prefix loss", ctx, b, base)
		t.Logf("early-prefix-loss negative: the real copied-prefix loss refused at the real prefix stage before any transaction; no native outcome")
	})
	t.Run("N12-late-prefix-loss", func(t *testing.T) {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		f := b.fixture
		_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		var probeCalls, acceptanceCalls int32
		attemptID := fmt.Sprintf("bound-postcommit-admission-lateprefix-%d", time.Now().UnixNano())
		marker := newBorrowedBoundPostcommitAdmissionMarker(attemptID, f.adminRole, b.owner.BackendPID)
		attempt := newBorrowedBoundPostcommitAdmissionAttempt(t, ctx, b, fresh, attemptID, marker, &probeCalls, &acceptanceCalls)
		plan := newBorrowedBoundBaselineCommitPlan()
		preparation := newBorrowedBoundBaselineCommitPreparation()
		admission := newBorrowedBoundPostcommitAdmission()
		obs := &borrowedBoundPostcommitAdmissionObservation{}
		flowState, flowErr := borrowedBoundWriterIsolationRun(ctx, t, b, fresh, &borrowedBoundWriterIsolationLineage{
			consumed: true, readinessBacked: false,
			instanceID: f.instanceID, targetKey: f.guardKey,
			fingerprint:    fresh.binding.OriginalRoleFingerprint(),
			replacementOID: fresh.replacementOID, ownerPID: b.owner.BackendPID,
			issuance: &borrowedBoundWriterIsolationIssuance{},
		}, &borrowedBoundWriterIsolationHooks{
			duringProof: func(hookCtx context.Context, flow *borrowedBoundWriterIsolationFlowState) error {
				return borrowedBoundPostcommitAdmissionDuringProof(hookCtx, t, b, fresh, attempt, marker, admission, plan, preparation, 1, &probeCalls, &acceptanceCalls, obs, flow, &borrowedBoundPostcommitAdmissionHookPlan{
					attemptID: attemptID, expect: "source", latePrefixLoss: true,
				})
			},
		})
		if flowErr != nil {
			t.Fatalf("late-prefix-loss control flow refused: %v", flowErr)
		}
		if !plan.commitAcknowledged || !admission.dispatchedNow() || obs.nativeErr == nil || !strings.Contains(obs.nativeErr.Error(), "genuine source capture") {
			t.Fatalf("late-prefix-loss classification: acknowledged=%t dispatched=%t err=%v", plan.commitAcknowledged, admission.dispatchedNow(), obs.nativeErr)
		}
		borrowedBoundPostcommitAdmissionAssertNoNativeOutcome(t, "late prefix loss", attempt, marker, &probeCalls, &acceptanceCalls, 0)
		borrowedBoundPostcommitAdmissionAssertNoAcceptedOutput(t, "late prefix loss", obs.nativeResult, obs.nativeReceipt)
		guardState, guardOp := borrowedReplacementGuardRowRead(t, ctx, f.controlPool, f.guardKey)
		if guardState != "clean" || guardOp != plan.rehearsalOperationID {
			t.Fatalf("late-prefix-loss baseline classification changed: %q/%q want clean/%q", guardState, guardOp, plan.rehearsalOperationID)
		}
		count, digest := borrowedBoundPostcommitAdmissionAuditDigest(ctx, t, b, controlstore.ActionTargetGuardRebuild, plan.rehearsalOperationID)
		if count != 1 || digest != obs.baselineAuditDigest {
			t.Fatalf("late-prefix-loss baseline audit changed: count=%d digestMatch=%t", count, digest == obs.baselineAuditDigest)
		}
		postRows := borrowedBoundNativeReadyRowsNow(t, ctx, b)
		if postRows.evidenceCount != base.rows.evidenceCount || postRows.auditCount != base.rows.auditCount+1 {
			t.Fatalf("late-prefix-loss changed durable rows: evidence %d->%d audit %d->%d", base.rows.evidenceCount, postRows.evidenceCount, base.rows.auditCount, postRows.auditCount)
		}
		if !flowState.cleanupRestored || flowState.cleanupErr != nil {
			t.Fatalf("late-prefix-loss fence disposal was not clean: restored=%t err=%v", flowState.cleanupRestored, flowState.cleanupErr)
		}
		t.Logf("late-shared-prefix-loss negative: the prefix was invalidated after the acknowledged baseline and before native admission; the prepared attempt refused at its real source stage, ZERO native marker/intent/child survives, the committed baseline preparation delta stayed byte-identical and the fence was disposed cleanly")
	})

	// N13: cancellation before the handoff, inside the native marker before
	// success, and AFTER the committed native intent: separate cancellation
	// outcomes with no durable intent for the first two and the committed
	// marker/intent retained for the third.
	t.Run("N13-cancel-before-handoff", func(t *testing.T) {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		f := b.fixture
		_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		var probeCalls, acceptanceCalls int32
		attemptID := fmt.Sprintf("bound-postcommit-admission-cancel-%d", time.Now().UnixNano())
		marker := newBorrowedBoundPostcommitAdmissionMarker(attemptID, f.adminRole, b.owner.BackendPID)
		attempt := newBorrowedBoundPostcommitAdmissionAttempt(t, ctx, b, fresh, attemptID, marker, &probeCalls, &acceptanceCalls)
		plan := newBorrowedBoundBaselineCommitPlan()
		preparation := newBorrowedBoundBaselineCommitPreparation()
		admission := newBorrowedBoundPostcommitAdmission()
		obs := &borrowedBoundPostcommitAdmissionObservation{}
		laneCtx, cancelLane := context.WithCancel(ctx)
		defer cancelLane()
		flowState, flowErr := borrowedBoundWriterIsolationRun(laneCtx, t, b, fresh, &borrowedBoundWriterIsolationLineage{
			consumed: true, readinessBacked: false,
			instanceID: f.instanceID, targetKey: f.guardKey,
			fingerprint:    fresh.binding.OriginalRoleFingerprint(),
			replacementOID: fresh.replacementOID, ownerPID: b.owner.BackendPID,
			issuance: &borrowedBoundWriterIsolationIssuance{},
		}, &borrowedBoundWriterIsolationHooks{
			duringProof: func(hookCtx context.Context, flow *borrowedBoundWriterIsolationFlowState) error {
				return borrowedBoundPostcommitAdmissionDuringProof(hookCtx, t, b, fresh, attempt, marker, admission, plan, preparation, 1, &probeCalls, &acceptanceCalls, obs, flow, &borrowedBoundPostcommitAdmissionHookPlan{
					attemptID: attemptID, expect: "source", cancelBeforeHandoff: true, cancelLane: cancelLane,
				})
			},
		})
		if !errors.Is(flowErr, errWriterIsolationCancelled) || laneCtx.Err() == nil {
			t.Fatalf("cancel-before-handoff did not refuse at the flow cancellation stage: %v", flowErr)
		}
		if !plan.commitAcknowledged || plan.rollbackAcknowledged || obs.nativeErr == nil || !strings.Contains(obs.nativeErr.Error(), "caller context ended") {
			t.Fatalf("cancel-before-handoff classification: acknowledged=%t rollback=%t err=%v", plan.commitAcknowledged, plan.rollbackAcknowledged, obs.nativeErr)
		}
		borrowedBoundPostcommitAdmissionAssertNoNativeOutcome(t, "cancel before handoff", attempt, marker, &probeCalls, &acceptanceCalls, 0)
		borrowedBoundPostcommitAdmissionAssertNoAcceptedOutput(t, "cancel before handoff", obs.nativeResult, obs.nativeReceipt)
		borrowedBoundPostcommitAdmissionAssertPreservedCommittedBaseline(t, ctx, b, plan, obs)
		if !flowState.cleanupRestored || flowState.cleanupErr != nil {
			t.Fatalf("cancel-before-handoff fence disposal was not clean: restored=%t err=%v", flowState.cleanupRestored, flowState.cleanupErr)
		}
		_ = base
		t.Logf("cancel-before-handoff negative: the committed baseline classification was preserved, the prepared attempt refused at its real caller-context stage before any coordinator entry and ZERO native marker/intent/child exists")
	})
	t.Run("N13-cancel-inside-marker", func(t *testing.T) {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		f := b.fixture
		_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		var probeCalls, acceptanceCalls int32
		attemptID := fmt.Sprintf("bound-postcommit-admission-marker-cancel-%d", time.Now().UnixNano())
		marker := newBorrowedBoundPostcommitAdmissionMarker(attemptID, f.adminRole, b.owner.BackendPID)
		attempt := newBorrowedBoundPostcommitAdmissionAttempt(t, ctx, b, fresh, attemptID, marker, &probeCalls, &acceptanceCalls)
		plan := newBorrowedBoundBaselineCommitPlan()
		preparation := newBorrowedBoundBaselineCommitPreparation()
		admission := newBorrowedBoundPostcommitAdmission()
		obs := &borrowedBoundPostcommitAdmissionObservation{}
		laneCtx, cancelLane := context.WithCancel(ctx)
		defer cancelLane()
		marker.preDelegate = func(context.Context) error {
			cancelLane()
			return errBoundPostcommitAdmissionCancelled
		}
		flowState, flowErr := borrowedBoundWriterIsolationRun(laneCtx, t, b, fresh, &borrowedBoundWriterIsolationLineage{
			consumed: true, readinessBacked: false,
			instanceID: f.instanceID, targetKey: f.guardKey,
			fingerprint:    fresh.binding.OriginalRoleFingerprint(),
			replacementOID: fresh.replacementOID, ownerPID: b.owner.BackendPID,
			issuance: &borrowedBoundWriterIsolationIssuance{},
		}, &borrowedBoundWriterIsolationHooks{
			duringProof: func(hookCtx context.Context, flow *borrowedBoundWriterIsolationFlowState) error {
				return borrowedBoundPostcommitAdmissionDuringProof(hookCtx, t, b, fresh, attempt, marker, admission, plan, preparation, 1, &probeCalls, &acceptanceCalls, obs, flow, &borrowedBoundPostcommitAdmissionHookPlan{
					attemptID: attemptID, expect: "marker", cancelLane: cancelLane,
				})
			},
		})
		if !errors.Is(flowErr, errWriterIsolationCancelled) || laneCtx.Err() == nil {
			t.Fatalf("inside-marker cancellation did not refuse at the flow cancellation stage: %v", flowErr)
		}
		if !plan.commitAcknowledged || !borrowedBoundPostcommitAdmissionMarkerRefusalClassified(obs.nativeErr, marker) {
			t.Fatalf("inside-marker cancellation classification: acknowledged=%t err=%v", plan.commitAcknowledged, obs.nativeErr)
		}
		if marker.callsNow() != 1 || !marker.pidDistinct || !marker.instanceRevalidated || marker.endpointClosed {
			t.Fatalf("inside-marker cancellation marker facts: calls=%d pid=%t instance=%t endpointClosed=%t", marker.callsNow(), marker.pidDistinct, marker.instanceRevalidated, marker.endpointClosed)
		}
		if marker.token() != (recovery.EvidenceToken{}) {
			t.Fatal("inside-marker cancellation produced a genuine marker token although the transaction never committed")
		}
		borrowedBoundPostcommitAdmissionAssertNoNativeOutcome(t, "cancel inside marker", attempt, marker, &probeCalls, &acceptanceCalls, 1)
		borrowedBoundPostcommitAdmissionAssertNoAcceptedOutput(t, "cancel inside marker", obs.nativeResult, obs.nativeReceipt)
		borrowedBoundPostcommitAdmissionAssertPreservedCommittedBaseline(t, ctx, b, plan, obs)
		postRows := borrowedBoundNativeReadyRowsNow(t, ctx, b)
		if postRows.evidenceCount != base.rows.evidenceCount || postRows.auditCount != base.rows.auditCount+1 {
			t.Fatalf("inside-marker cancellation changed rolled-back rows: evidence %d->%d audit %d->%d", base.rows.evidenceCount, postRows.evidenceCount, base.rows.auditCount, postRows.auditCount)
		}
		if !flowState.cleanupRestored || flowState.cleanupErr != nil {
			t.Fatalf("inside-marker cancellation fence disposal was not clean: restored=%t err=%v", flowState.cleanupRestored, flowState.cleanupErr)
		}
		t.Logf("inside-native-marker-cancellation negative: the marker ran its live/provenance checks on the genuine preparation transaction, the injected cancellation refused BEFORE success, the prelaunch transaction rolled back with NO committed marker/intent, the committed baseline stayed byte-identical and the fence was disposed cleanly")
	})
	t.Run("N13-cancel-after-intent", func(t *testing.T) {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		f := b.fixture
		_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		var probeCalls, acceptanceCalls int32
		attemptID := fmt.Sprintf("bound-postcommit-admission-afterintent-%d", time.Now().UnixNano())
		marker := newBorrowedBoundPostcommitAdmissionMarker(attemptID, f.adminRole, b.owner.BackendPID)
		attempt := newBorrowedBoundPostcommitAdmissionAttempt(t, ctx, b, fresh, attemptID, marker, &probeCalls, &acceptanceCalls)
		plan := newBorrowedBoundBaselineCommitPlan()
		preparation := newBorrowedBoundBaselineCommitPreparation()
		admission := newBorrowedBoundPostcommitAdmission()
		obs := &borrowedBoundPostcommitAdmissionObservation{}
		laneCtx, cancelLane := context.WithCancel(ctx)
		defer cancelLane()
		flowState, flowErr := borrowedBoundWriterIsolationRun(laneCtx, t, b, fresh, &borrowedBoundWriterIsolationLineage{
			consumed: true, readinessBacked: false,
			instanceID: f.instanceID, targetKey: f.guardKey,
			fingerprint:    fresh.binding.OriginalRoleFingerprint(),
			replacementOID: fresh.replacementOID, ownerPID: b.owner.BackendPID,
			issuance: &borrowedBoundWriterIsolationIssuance{},
		}, &borrowedBoundWriterIsolationHooks{
			duringProof: func(hookCtx context.Context, flow *borrowedBoundWriterIsolationFlowState) error {
				return borrowedBoundPostcommitAdmissionDuringProof(hookCtx, t, b, fresh, attempt, marker, admission, plan, preparation, 1, &probeCalls, &acceptanceCalls, obs, flow, &borrowedBoundPostcommitAdmissionHookPlan{
					attemptID: attemptID, expect: "launch", cancelAfterIntent: true, cancelLane: cancelLane,
				})
			},
		})
		if !errors.Is(flowErr, errWriterIsolationCancelled) || laneCtx.Err() == nil {
			t.Fatalf("cancel-after-intent did not refuse at the flow cancellation stage: %v", flowErr)
		}
		if !plan.commitAcknowledged || plan.commitAckLost || plan.rollbackAcknowledged || obs.nativeErr == nil || !strings.Contains(obs.nativeErr.Error(), "refused at launch") {
			t.Fatalf("cancel-after-intent classification: acknowledged=%t ackLost=%t rollback=%t err=%v", plan.commitAcknowledged, plan.commitAckLost, plan.rollbackAcknowledged, obs.nativeErr)
		}
		if !obs.fenceRechecked || marker.callsNow() != 1 || !marker.pidDistinct || !marker.instanceRevalidated || !marker.endpointClosed {
			t.Fatalf("cancel-after-intent handoff facts: fence=%t calls=%d pid=%t instance=%t endpointClosed=%t", obs.fenceRechecked, marker.callsNow(), marker.pidDistinct, marker.instanceRevalidated, marker.endpointClosed)
		}
		// The cancellation happened AFTER the native intent commit: the committed
		// marker/genuine generation, the conservative UNKNOWN launch intent and
		// the unchanged baseline audit are re-read independently, with zero
		// child, absent receipt, zero probe/acceptance callbacks and no accepted
		// output.
		borrowedBoundPostcommitAdmissionAssertNativeIntent(ctx, t, b, fresh, attempt, marker, obs.nativeResult, obs.nativeReceipt, &probeCalls, &acceptanceCalls, obs)
		if !flowState.cleanupRestored || flowState.cleanupErr != nil {
			t.Fatalf("cancel-after-intent fence disposal was not clean: restored=%t err=%v", flowState.cleanupRestored, flowState.cleanupErr)
		}
		if _, _, replayErr := admission.dispatchOnce(ctx, attempt); !errors.Is(replayErr, errBoundPostcommitAdmissionReplay) {
			t.Fatalf("cancel-after-intent object redispatched: %v", replayErr)
		}
		admissionCopy := *admission
		if _, _, copyErr := admissionCopy.dispatchOnce(ctx, attempt); !errors.Is(copyErr, errBoundPostcommitAdmissionReplay) {
			t.Fatalf("cancel-after-intent copy redispatched: %v", copyErr)
		}
		runCopy := *attempt.run
		replayCtx, cancelReplay := context.WithTimeout(ctx, 30*time.Second)
		_, _, runCopyErr := runCopy.Run(replayCtx)
		cancelReplay()
		if runCopyErr == nil || !strings.Contains(runCopyErr.Error(), "already reserved") {
			t.Fatalf("cancel-after-intent run copy was not refused as already reserved: %v", runCopyErr)
		}
		if marker.callsNow() != 1 {
			t.Fatalf("cancel-after-intent replay added a marker callback: %d", marker.callsNow())
		}
		_ = base
		t.Logf("after-committed-intent cancellation negative: the caller context ended only after the committed native marker/intent and the live fence recheck; the flow refused at its real cancellation stage, the committed marker/genuine generation, conservative UNKNOWN launch intent and unchanged baseline audit were independently re-read, ZERO child/receipt/callbacks/accepted output exists, the fence was disposed cleanly and the terminal object/copies could not redispatch or publish")
	})

	// N14: endpoint loss BEFORE preflight: no preparation transaction at all.
	t.Run("N14-endpoint-before-preflight", func(t *testing.T) {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		f := b.fixture
		_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		var probeCalls, acceptanceCalls int32
		attemptID := fmt.Sprintf("bound-postcommit-admission-preflight-%d", time.Now().UnixNano())
		marker := newBorrowedBoundPostcommitAdmissionMarker(attemptID, f.adminRole, b.owner.BackendPID)
		attempt := newBorrowedBoundPostcommitAdmissionAttempt(t, ctx, b, fresh, attemptID, marker, &probeCalls, &acceptanceCalls)
		if err := attempt.gate.endpoint.Listener().Close(); err != nil {
			t.Fatalf("preflight-loss endpoint closure refused: %v", err)
		}
		runCtx, cancelRun := context.WithTimeout(ctx, 60*time.Second)
		result, receipt, runErr := attempt.dispatchRun(runCtx)
		cancelRun()
		if runErr == nil || !strings.Contains(runErr.Error(), "refused at preflight") || !strings.Contains(runErr.Error(), "origin endpoint capability") || strings.Contains(runErr.Error(), "refused at launch") {
			t.Fatalf("preflight endpoint loss is not the actual preflight refusal: %v", runErr)
		}
		borrowedBoundPostcommitAdmissionAssertNoNativeOutcome(t, "endpoint before preflight", attempt, marker, &probeCalls, &acceptanceCalls, 0)
		borrowedBoundPostcommitAdmissionAssertNoAcceptedOutput(t, "endpoint before preflight", result, receipt)
		borrowedBoundReentryReadinessAssertBaseline(t, "bound postcommit admission endpoint before preflight", ctx, b, base)
		t.Logf("endpoint-loss-before-preflight negative: the actual preflight hook refused (`%v`) before any preparation transaction, marker callback, launch intent or child; durable state classification: complete baseline equality, no transaction, no authorizing output", runErr)
	})

	// N15: row-holder-first: an independent holder owns the guard row and the
	// supplied-tx fence receives the structured 55P03; no handoff.
	t.Run("N15-row-holder-first", func(t *testing.T) {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		f := b.fixture
		_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		hold, holdErr := borrowedReplacementGuardRowHoldRow(ctx, f.controlPool, f.guardKey)
		if holdErr != nil {
			t.Fatalf("row-holder-first control hold refused: %v", holdErr)
		}
		defer hold.release()
		restore := borrowedBoundBaselineCommitApplyFence(ctx, t, b, fresh)
		defer restore()
		var probeCalls, acceptanceCalls int32
		attemptID := fmt.Sprintf("bound-postcommit-admission-rowholder-%d", time.Now().UnixNano())
		marker := newBorrowedBoundPostcommitAdmissionMarker(attemptID, f.adminRole, b.owner.BackendPID)
		attempt := newBorrowedBoundPostcommitAdmissionAttempt(t, ctx, b, fresh, attemptID, marker, &probeCalls, &acceptanceCalls)
		plan := newBorrowedBoundBaselineCommitPlan()
		preparation := newBorrowedBoundBaselineCommitPreparation()
		stageErr := borrowedBoundBaselineCommitStageRun(ctx, t, b, fresh, 1, plan, preparation)
		hold.release()
		if !errors.Is(stageErr, errGuardRowWindowRowRefused) || borrowedReplacementGuardRowCode(stageErr) != "55P03" {
			t.Fatalf("row-holder-first refusal is not the structured NOWAIT 55P03 stage: code=%s err=%v", borrowedReplacementGuardRowCode(stageErr), stageErr)
		}
		if !plan.rollbackAcknowledged {
			t.Fatal("row-holder-first refusal rollback was not acknowledged")
		}
		admission := newBorrowedBoundPostcommitAdmission()
		if err := admission.armAfterAcknowledgedBaseline(plan, nil); !errors.Is(err, errBoundPostcommitAdmissionBaseline) || admission.armedNow() {
			t.Fatalf("row-holder-first handoff was armed: %v", err)
		}
		borrowedBoundPostcommitAdmissionAssertNoNativeOutcome(t, "row holder first", attempt, marker, &probeCalls, &acceptanceCalls, 0)
		borrowedBoundReentryReadinessAssertBaseline(t, "bound postcommit admission row holder", ctx, b, base)
		t.Logf("row-holder-first negative: the independent holder produced the structured 55P03 at the supplied-tx fence with the acknowledged rollback; no native outcome")
	})

	// N16: concurrent reservation: two concurrent dispatches of the one-use
	// native attempt resolve to exactly ONE actual coordinator run and one gate
	// replay refusal, with only ONE marker/intent transaction committed.
	t.Run("N16-concurrent-reservation", func(t *testing.T) {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		f := b.fixture
		_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		var probeCalls, acceptanceCalls int32
		attemptID := fmt.Sprintf("bound-postcommit-admission-concurrent-%d", time.Now().UnixNano())
		marker := newBorrowedBoundPostcommitAdmissionMarker(attemptID, f.adminRole, b.owner.BackendPID)
		attempt := newBorrowedBoundPostcommitAdmissionAttempt(t, ctx, b, fresh, attemptID, marker, &probeCalls, &acceptanceCalls)
		plan := newBorrowedBoundBaselineCommitPlan()
		preparation := newBorrowedBoundBaselineCommitPreparation()
		admission := newBorrowedBoundPostcommitAdmission()
		obs := &borrowedBoundPostcommitAdmissionObservation{}
		flowState, flowErr := borrowedBoundWriterIsolationRun(ctx, t, b, fresh, &borrowedBoundWriterIsolationLineage{
			consumed: true, readinessBacked: false,
			instanceID: f.instanceID, targetKey: f.guardKey,
			fingerprint:    fresh.binding.OriginalRoleFingerprint(),
			replacementOID: fresh.replacementOID, ownerPID: b.owner.BackendPID,
			issuance: &borrowedBoundWriterIsolationIssuance{},
		}, &borrowedBoundWriterIsolationHooks{
			duringProof: func(hookCtx context.Context, flow *borrowedBoundWriterIsolationFlowState) error {
				if err := borrowedBoundBaselineCommitStageRun(hookCtx, t, b, fresh, 1, plan, preparation); err != nil {
					return err
				}
				if !plan.commitAcknowledged {
					return errBoundPostcommitAdmissionBaseline
				}
				obs.baselineOperation = plan.rehearsalOperationID
				obs.attemptID = attemptID
				obs.baselineAuditCount, obs.baselineAuditDigest = borrowedBoundPostcommitAdmissionAuditDigest(hookCtx, t, b, controlstore.ActionTargetGuardRebuild, plan.rehearsalOperationID)
				if err := admission.armAfterAcknowledgedBaseline(plan, flow); err != nil {
					return err
				}
				if err := borrowedBoundPostcommitAdmissionRevalidate(hookCtx, t, b, fresh, plan, flow); err != nil {
					return err
				}
				type concurrentResult struct {
					result  recovery.TargetWriterResult
					receipt recovery.DrillTargetProcessReceipt
					err     error
				}
				outcomes := make([]concurrentResult, 2)
				var wg sync.WaitGroup
				for i := 0; i < 2; i++ {
					i := i
					wg.Add(1)
					go func() {
						defer wg.Done()
						dispatchCtx, cancelDispatch := context.WithTimeout(hookCtx, borrowedBoundPostcommitAdmissionDispatchBudget)
						defer cancelDispatch()
						outcomes[i].result, outcomes[i].receipt, outcomes[i].err = admission.dispatchOnce(dispatchCtx, attempt)
					}()
				}
				wg.Wait()
				var launchErr, replayErr, unexpected error
				for _, outcome := range outcomes {
					switch {
					case outcome.err == nil:
						unexpected = errors.New("a concurrent dispatch returned success")
					case errors.Is(outcome.err, errBoundPostcommitAdmissionReplay):
						replayErr = outcome.err
					case strings.Contains(outcome.err.Error(), "refused at launch"):
						launchErr = outcome.err
						obs.nativeResult, obs.nativeReceipt, obs.nativeErr = outcome.result, outcome.receipt, outcome.err
					default:
						unexpected = outcome.err
					}
				}
				if unexpected != nil || launchErr == nil || replayErr == nil {
					return fmt.Errorf("%w: concurrent outcomes launch=%v replay=%v unexpected=%v", errBoundPostcommitAdmissionLaunch, launchErr, replayErr, unexpected)
				}
				if runErr := launchErr; !strings.Contains(runErr.Error(), "origin endpoint capability") {
					return fmt.Errorf("%w: %v", errBoundPostcommitAdmissionLaunch, runErr)
				}
				obs.nativeOperation = attemptID
				borrowedBoundPostcommitAdmissionAssertNativeIntent(hookCtx, t, b, fresh, attempt, marker, obs.nativeResult, obs.nativeReceipt, &probeCalls, &acceptanceCalls, obs)
				if marker.callsNow() != 1 {
					t.Fatalf("concurrent reservation marker callbacks=%d, want exactly 1", marker.callsNow())
				}
				if err := borrowedBoundPostcommitAdmissionFenceRecheck(hookCtx, t, b, fresh); err != nil {
					return err
				}
				obs.fenceRechecked = true
				return nil
			},
		})
		if flowErr != nil {
			t.Fatalf("concurrent reservation control refused: %v", flowErr)
		}
		obs.nativeOperation = attemptID
		if !admission.dispatchedNow() || !obs.fenceRechecked {
			t.Fatalf("concurrent reservation facts: dispatched=%t fence=%t", admission.dispatchedNow(), obs.fenceRechecked)
		}
		borrowedBoundPostcommitAdmissionAssertNativeIntent(ctx, t, b, fresh, attempt, marker, obs.nativeResult, obs.nativeReceipt, &probeCalls, &acceptanceCalls, obs)
		postRows := borrowedBoundNativeReadyRowsNow(t, ctx, b)
		if postRows.evidenceCount != base.rows.evidenceCount || postRows.auditCount != base.rows.auditCount+2 {
			t.Fatalf("concurrent reservation durable rows: evidence %d->%d audit %d->%d", base.rows.evidenceCount, postRows.evidenceCount, base.rows.auditCount, postRows.auditCount)
		}
		if !flowState.cleanupRestored || flowState.cleanupErr != nil {
			t.Fatalf("concurrent reservation fence disposal was not clean: restored=%t err=%v", flowState.cleanupRestored, flowState.cleanupErr)
		}
		t.Logf("concurrent-reservation negative: exactly ONE of two concurrent dispatches reserved and ran the actual coordinator (launch-stage endpoint refusal), the other was refused by the one-use gate, ONE marker/intent transaction committed and no second marker/audit/intent transaction exists")
	})

	// N17: teardown failure AFTER the committed native intent: the committed
	// marker/intent and historical preparation audit are retained, the fence
	// disposal cannot complete and no positive continuation/publication exists.
	t.Run("N17-teardown-after-intent", func(t *testing.T) {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		f := b.fixture
		_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		var probeCalls, acceptanceCalls int32
		attemptID := fmt.Sprintf("bound-postcommit-admission-teardown-%d", time.Now().UnixNano())
		marker := newBorrowedBoundPostcommitAdmissionMarker(attemptID, f.adminRole, b.owner.BackendPID)
		attempt := newBorrowedBoundPostcommitAdmissionAttempt(t, ctx, b, fresh, attemptID, marker, &probeCalls, &acceptanceCalls)
		plan := newBorrowedBoundBaselineCommitPlan()
		preparation := newBorrowedBoundBaselineCommitPreparation()
		admission := newBorrowedBoundPostcommitAdmission()
		obs := &borrowedBoundPostcommitAdmissionObservation{}
		flowState, flowErr := borrowedBoundWriterIsolationRun(ctx, t, b, fresh, &borrowedBoundWriterIsolationLineage{
			consumed: true, readinessBacked: false,
			instanceID: f.instanceID, targetKey: f.guardKey,
			fingerprint:    fresh.binding.OriginalRoleFingerprint(),
			replacementOID: fresh.replacementOID, ownerPID: b.owner.BackendPID,
			issuance: &borrowedBoundWriterIsolationIssuance{},
		}, &borrowedBoundWriterIsolationHooks{
			duringProof: func(hookCtx context.Context, flow *borrowedBoundWriterIsolationFlowState) error {
				if err := borrowedBoundPostcommitAdmissionDuringProof(hookCtx, t, b, fresh, attempt, marker, admission, plan, preparation, 1, &probeCalls, &acceptanceCalls, obs, flow, &borrowedBoundPostcommitAdmissionHookPlan{
					attemptID: attemptID, expect: "launch",
				}); err != nil {
					return err
				}
				var adminPID int
				adminCtx, cancelAdmin := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
				adminPIDErr := f.fx.admin.QueryRow(adminCtx, `SELECT pg_backend_pid()`).Scan(&adminPID)
				cancelAdmin()
				if adminPIDErr != nil || adminPID <= 0 {
					return fmt.Errorf("teardown-failure dependency identity refused: %v", adminPIDErr)
				}
				termCtx, cancelTerm := context.WithTimeout(context.Background(), borrowedOwnerRotationQueryBudget)
				var terminated bool
				termErr := f.controlPool.QueryRow(termCtx, `SELECT pg_terminate_backend($1)`, adminPID).Scan(&terminated)
				cancelTerm()
				if termErr != nil || !terminated {
					return fmt.Errorf("teardown-failure dependency termination refused: %v", termErr)
				}
				return nil
			},
		})
		if flowErr == nil {
			t.Fatal("teardown-failure-after-intent control unexpectedly completed the flow")
		}
		if !plan.commitAcknowledged || plan.commitAckLost || plan.rollbackAcknowledged {
			t.Fatalf("teardown-failure classification: acknowledged=%t ackLost=%t rollback=%t", plan.commitAcknowledged, plan.commitAckLost, plan.rollbackAcknowledged)
		}
		if !admission.dispatchedNow() || marker.callsNow() != 1 || obs.nativeErr == nil {
			t.Fatalf("teardown-failure native facts: dispatched=%t marker=%d err=%v", admission.dispatchedNow(), marker.callsNow(), obs.nativeErr)
		}
		postGuard, postFound, postGuardErr := controlstore.ReadTargetGuard(ctx, f.controlPool, f.guardKey)
		if postGuardErr != nil || !postFound || postGuard.State != controlstore.TargetGuardUnknown || !postGuard.LaunchIntent || !postGuard.ActiveWriter ||
			postGuard.OperationID != attemptID || postGuard.AttemptAppName != obs.nativeResult.Application || postGuard.LaunchedAt != nil {
			t.Fatalf("teardown failure did not retain the committed native intent: err=%v found=%t guard=%+v", postGuardErr, postFound, postGuard)
		}
		if nativeAudits := borrowedBoundBaselineCommitPreparationRowCount(t, ctx, b, plan.rehearsalOperationID); nativeAudits != 1 {
			t.Fatalf("teardown failure changed the retained preparation audit rows: %d", nativeAudits)
		}
		if flowState.cleanupRestored || flowState.cleanupErr == nil {
			t.Fatalf("teardown-failure control did not prove the failed disposal: restored=%t err=%v", flowState.cleanupRestored, flowState.cleanupErr)
		}
		isolation := newBorrowedBoundWriterIsolation()
		lineage := &borrowedBoundWriterIsolationLineage{
			consumed: true, readinessBacked: false,
			instanceID: f.instanceID, targetKey: f.guardKey,
			fingerprint:    fresh.binding.OriginalRoleFingerprint(),
			replacementOID: fresh.replacementOID, ownerPID: b.owner.BackendPID,
			issuance: &borrowedBoundWriterIsolationIssuance{},
		}
		if publishErr := borrowedBoundWriterIsolationTryPublish(t, "teardown failure after intent", b, fresh, lineage, flowState, isolation); publishErr == nil {
			t.Fatal("teardown failure after committed intent published a positive isolation result")
		}
		if isolation.publishedNow() {
			t.Fatal("teardown failure after committed intent published the isolation fact set")
		}
		healthCtx, cancelHealth := context.WithTimeout(ctx, borrowedOwnerRotationHealthBudget)
		healthErr := f.lock.Health(healthCtx)
		cancelHealth()
		anchorCtx, cancelAnchor := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
		anchorErr := b.anchor.Recheck(anchorCtx)
		cancelAnchor()
		if healthErr != nil || anchorErr != nil {
			t.Fatalf("teardown failure after committed intent affected the original owner: health=%v anchor=%v", healthErr, anchorErr)
		}
		t.Logf("teardown-failure-after-committed-intent negative: the committed marker/intent and historical preparation audit were retained, the fence disposal could not complete (restored=%t err=%v) and no positive continuation/publication exists; committed history was never repaired", flowState.cleanupRestored, flowState.cleanupErr)
	})
}
