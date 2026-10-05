//go:build linux && drill

// borrowed-replacement-bound-baseline-commit_linux_test.go is the bounded
// bound live-fence controlled-baseline ACKNOWLEDGED-COMMIT lane. It composes
// ONLY existing, unchanged primitives: the genuine bound capture with the
// consumed receipt/entry, the existing bound dirty-guard refusal (whose
// coordinator preparation transaction PID is captured through the existing
// marker injection seam), the unchanged readiness machinery consumed ONCE as
// NON-AUTHORIZING lineage facts, the strict registered-P1 retirement, and the
// unchanged writer-isolation flow whose live fence is used through its existing
// duringProof hook. Inside that hook the lane performs a fresh verification pass
// (effective writer-reject coverage, real route refusals, zero retained-writer
// census, protected observer usability, actual pristine replacement catalog) and
// then invokes the retained lock's UNCHANGED WithTransaction: the supplied
// transaction must be the ORIGINAL anchored owner session (never the coordinator
// preparation session), the authentic instance is locked/revalidated, the
// original guard row is fenced with its operation provenance, the immutable
// binding, shared prefix and actual replacement identity are rechecked
// immediately before the production writes, truthful fixture-scoped
// controlled-baseline PREPARATION evidence is written through the production
// audit hook, and the UNCHANGED RecordTargetGuardRebuild +
// ResolveTargetGuardClean transitions run in that same transaction with a
// transaction-local clean verification. While the transaction still holds the
// row, independent connections prove the COMMITTED guard remains unresolved,
// the pending preparation audit is invisible and a same-row NOWAIT contender
// receives the structured 55P03. Cancellation, shared-prefix loss and the
// relevant live prerequisites are rechecked INSIDE the callback before it
// returns nil, and ONLY a nil callback return lets the UNCHANGED WithTransaction
// perform the acknowledged COMMIT (there is NO callback-side tx.Commit). After
// the acknowledged commit the lane independently proves exactly the intended
// durable guard/audit delta (guard clean with the preparation operation id and
// the truthful preparation evidence; exactly ONE instance-bound
// target_guard_rebuild audit row whose detail equals the guard evidence and
// describes preparation, never completed restoration), the released row's
// lockability, the original-owner health and the clean fence teardown; every
// other instance/evidence/catalog/journal fact stays unchanged. The resolved
// guard grants NO admission and NO restore acceptance: there is no admission
// handle, no reusable preparation capability, no post-commit restore dispatch
// and teardown establishes no continuing isolation. Controls exercise missing/
// incomplete/stale live prerequisites, an admitting route, a retained writer,
// observer/catalog failure, wrong instance/key/operation/role provenance, the
// row-holder-first 55P03, early and late shared-prefix loss (late loss proves
// NEITHER production write was reached), the between-writes failure (audit
// executed, resolution refused, acknowledged rollback, NEITHER survives),
// cancellation before the transaction / between the writes / after the pending
// resolution, owner loss before the commit (unresolved guard, complete baseline
// equality, zero surviving preparation rows, rollback uncertainty, unusable
// owner), COMMIT acknowledgement loss through the UNCHANGED native companion
// (no success inferred from the error or from independently visible rows; the
// one-shot attempt is consumed and the uncertain owner session is never reused),
// an acknowledged commit followed by owner loss or teardown failure (the
// committed guard/audit are preserved and no positive continuation is emitted;
// committed history is never repaired by deleting the audit or reversing the
// committed transition) and the shared one-shot replay/copy refusal (no second
// preparation transaction or audit). Only acknowledged pre-commit rollbacks
// justify complete baseline equality; commit uncertainty is independently
// observed without relabeling success. There is no ordinary native replacement
// launch, no successful restore probe, no restored evidence, no restore atomic
// acceptance, no manifest change, no capability/downstream effect, no direct
// guard-update SQL, no artificial clean seed, no second instance, no owner
// reacquisition, no copied transaction machinery, no wrapper seam, no
// replacement fixture and no rebuildTargetWithWitness modification. Secrets,
// verifiers, DSNs and archive contents are never logged.
package recovery_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xtianxx/txharbor/internal/recovery"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

// Bound baseline-commit stage sentinels: every control captures and asserts the
// ACTUAL real stage instead of any non-nil error.
var (
	errBoundBaselineCommitCoverage     = errors.New("bound baseline commit: effective fence coverage refused")
	errBoundBaselineCommitRoute        = errors.New("bound baseline commit: a live route was not rejected at the intended admission stage")
	errBoundBaselineCommitCensus       = errors.New("bound baseline commit: the retained-writer census refused")
	errBoundBaselineCommitObserver     = errors.New("bound baseline commit: the protected observer route refused")
	errBoundBaselineCommitCatalog      = errors.New("bound baseline commit: the actual replacement catalog refused")
	errBoundBaselineCommitPrefix       = errors.New("bound baseline commit: the shared replacement prefix refused")
	errBoundBaselineCommitOwnerTx      = errors.New("bound baseline commit: the supplied transaction is not the original anchored owner")
	errBoundBaselineCommitInstance     = errors.New("bound baseline commit: the authentic instance binding refused")
	errBoundBaselineCommitClean        = errors.New("bound baseline commit: the controlled baseline preparation writes refused")
	errBoundBaselineCommitGuardDelta   = errors.New("bound baseline commit: the durable guard delta is not the intended controlled resolution")
	errBoundBaselineCommitAuditDelta   = errors.New("bound baseline commit: the durable audit is not the truthful fixture-scoped preparation")
	errBoundBaselineCommitContender    = errors.New("bound baseline commit: the held-row contender proof refused")
	errBoundBaselineCommitCancelled    = errors.New("bound baseline commit: the lane context ended")
	errBoundBaselineCommitPreCallback  = errors.New("bound baseline commit: the target-lock transaction refused before the callback")
	errBoundBaselineCommitUncertain    = errors.New("bound baseline commit: the rollback outcome is uncertain")
	errBoundBaselineCommitAckLoss      = errors.New("bound baseline commit: the COMMIT acknowledgement was lost")
	errBoundBaselineCommitContinuation = errors.New("bound baseline commit: no positive continuation is permitted after the acknowledged commit")
	errBoundBaselineCommitTeardown     = errors.New("bound baseline commit: the fixture teardown could not be completed after the acknowledged commit")
	errBoundBaselineCommitReplay       = errors.New("bound baseline commit: the preparation was already consumed")
)

// borrowedBoundBaselineCommitPreparation is the copy-shared one-shot
// preparation latch: one controlled-baseline attempt per lineage, never a
// reusable positive result. Copies share the same state pointer.
type borrowedBoundBaselineCommitPreparation struct {
	state *borrowedBoundBaselineCommitPreparationState
}

type borrowedBoundBaselineCommitPreparationState struct {
	mu       sync.Mutex
	claimed  bool
	finished bool
	nonce    int64
}

func newBorrowedBoundBaselineCommitPreparation() *borrowedBoundBaselineCommitPreparation {
	return &borrowedBoundBaselineCommitPreparation{state: &borrowedBoundBaselineCommitPreparationState{}}
}

func (p *borrowedBoundBaselineCommitPreparation) claim() bool {
	if p == nil || p.state == nil {
		return false
	}
	p.state.mu.Lock()
	defer p.state.mu.Unlock()
	if p.state.claimed {
		return false
	}
	p.state.claimed = true
	p.state.nonce++
	return true
}

func (p *borrowedBoundBaselineCommitPreparation) finish() {
	if p == nil || p.state == nil {
		return
	}
	p.state.mu.Lock()
	p.state.finished = true
	p.state.mu.Unlock()
}

func (p *borrowedBoundBaselineCommitPreparation) finishedNow() bool {
	if p == nil || p.state == nil {
		return false
	}
	p.state.mu.Lock()
	defer p.state.mu.Unlock()
	return p.state.finished
}

func (p *borrowedBoundBaselineCommitPreparation) claimedNow() bool {
	if p == nil || p.state == nil {
		return false
	}
	p.state.mu.Lock()
	defer p.state.mu.Unlock()
	return p.state.claimed
}

// borrowedBoundBaselineCommitPlan is the lane-local stage plan and observed
// fact container.
type borrowedBoundBaselineCommitPlan struct {
	// injection seams (nil/empty in the positive).
	fenceKeyOverride        string
	fenceOperationOverride  string
	expectedFingerprint     string
	instanceIDOverride      string
	prefixLoss              bool
	routeOverride           func(transport string) (string, bool)
	emptyResolveEvidence    bool
	cancelAt                string
	cancelLane              func()
	ownerLossBeforeCommit   bool
	latePrefixLoss          bool
	observerNologin         bool
	renameTargetBeforeStage bool
	commitAckLoss           bool
	postCommitOwnerLoss     bool
	postCommitTeardown      bool
	lockOverride            *recovery.TargetLock
	ownerPIDOverride        int
	ownerStartOverride      time.Time
	namespaceK1Override     int32
	namespaceK2Override     int32
	operationIDOverride     string

	// observed facts.
	freshCoverage            bool
	routeLines               map[string]string
	census                   int
	observerOK               bool
	catalogOID               uint32
	ownerTxPID               int
	instanceState            string
	instanceKey              string
	instanceFingerprint      string
	guardDisposition         string
	guardOperation           string
	auditExecuted            bool
	resolveAttempted         bool
	resolveRefused           bool
	cleanVerified            bool
	localAuditFound          bool
	pendingGuardState        string
	pendingAuditVisible      int
	contenderCode            string
	liveRecheckOK            bool
	durableGuardState        string
	durableGuardOperation    string
	durableAuditCount        int
	durableAuditPreparation  bool
	lockableAfterCommit      bool
	ownerHealthyAfterCommit  bool
	commitAcknowledged       bool
	commitAckLost            bool
	successInferred          bool
	rollbackAcknowledged     bool
	cancelObserved           bool
	reachedStage             string
	auditWrittenBeforeCancel bool
	rehearsalOperationID     string
}

func newBorrowedBoundBaselineCommitPlan() *borrowedBoundBaselineCommitPlan {
	return &borrowedBoundBaselineCommitPlan{routeLines: map[string]string{}}
}

// borrowedBoundBaselineCommitDurableDelta is the independent read of the exact
// intended post-commit durable delta.
type borrowedBoundBaselineCommitDurableDelta struct {
	guardState        string
	guardOperation    string
	guardActiveWriter bool
	auditCount        int
	auditResult       string
	auditInstanceID   string
	auditScope        string
	auditRestoration  string
	evidenceMatches   bool
}

// borrowedBoundBaselineCommitReadDelta independently reads the durable guard
// row and the preparation audit row through an independent Queryer.
func borrowedBoundBaselineCommitReadDelta(ctx context.Context, q controlstore.Queryer, key, operationID string) (borrowedBoundBaselineCommitDurableDelta, error) {
	var delta borrowedBoundBaselineCommitDurableDelta
	guard, found, err := controlstore.ReadTargetGuard(ctx, q, key)
	if err != nil {
		return delta, fmt.Errorf("durable guard read refused: %w", err)
	}
	if !found {
		return delta, errors.New("durable guard row is absent")
	}
	delta.guardState = guard.State
	delta.guardOperation = guard.OperationID
	delta.guardActiveWriter = guard.ActiveWriter
	readCtx, cancelRead := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	defer cancelRead()
	if err := q.QueryRow(readCtx, `SELECT count(*)::int FROM recovery_audit WHERE action=$1 AND operation_id=$2`,
		controlstore.ActionTargetGuardRebuild, operationID).Scan(&delta.auditCount); err != nil {
		return delta, fmt.Errorf("durable preparation audit count refused: %w", err)
	}
	if delta.auditCount == 1 {
		if err := q.QueryRow(readCtx, `
SELECT COALESCE(a.result,''), COALESCE(a.instance_id::text,''), COALESCE(a.detail->>'scope',''), COALESCE(a.detail->>'completed_restoration',''),
       (g.rebuild_evidence = a.detail)
FROM recovery_audit a
JOIN recovery_target_guard g ON g.target_guard_key = $1
WHERE a.action=$2 AND a.operation_id=$3`,
			key, controlstore.ActionTargetGuardRebuild, operationID).
			Scan(&delta.auditResult, &delta.auditInstanceID, &delta.auditScope, &delta.auditRestoration, &delta.evidenceMatches); err != nil {
			return delta, fmt.Errorf("durable preparation audit read refused: %w", err)
		}
	}
	return delta, nil
}

// borrowedBoundBaselineCommitAssertDelta requires exactly the intended durable
// guard/audit delta: the controlled clean resolution with the preparation
// operation id, ONE instance-bound target_guard_rebuild audit row whose detail
// equals the guard evidence and describes the fixture-scoped PREPARATION (never
// a completed restoration).
func borrowedBoundBaselineCommitAssertDelta(t *testing.T, label string, delta borrowedBoundBaselineCommitDurableDelta, instanceID, operationID string) {
	t.Helper()
	if delta.guardState != controlstore.TargetGuardClean || delta.guardActiveWriter || delta.guardOperation != operationID {
		t.Fatalf("%s: durable guard delta is not the intended controlled resolution: state=%q active=%t operation=%q want=%q",
			label, delta.guardState, delta.guardActiveWriter, delta.guardOperation, operationID)
	}
	if delta.auditCount != 1 || delta.auditResult != controlstore.AuditOK || delta.auditInstanceID != instanceID || !delta.evidenceMatches {
		t.Fatalf("%s: durable audit delta is not the truthful instance-bound preparation record: count=%d result=%q instance=%q evidence_matches=%t",
			label, delta.auditCount, delta.auditResult, delta.auditInstanceID, delta.evidenceMatches)
	}
	if delta.auditScope != "bound_fixture_controlled_baseline_preparation" || delta.auditRestoration != "false" {
		t.Fatalf("%s: durable audit does not describe the fixture-scoped preparation: scope=%q completed_restoration=%q",
			label, delta.auditScope, delta.auditRestoration)
	}
}

// borrowedBoundBaselineCommitAssertPreservedDelta independently re-reads and
// requires the exact committed guard/audit delta after a post-commit control.
func borrowedBoundBaselineCommitAssertPreservedDelta(t *testing.T, label string, ctx context.Context, b *borrowedSuccessorBaseline, operationID string) {
	t.Helper()
	delta, err := borrowedBoundBaselineCommitReadDelta(ctx, b.fixture.controlPool, b.fixture.guardKey, operationID)
	if err != nil {
		t.Fatalf("%s: %v", label, err)
	}
	borrowedBoundBaselineCommitAssertDelta(t, label, delta, b.fixture.instanceID, operationID)
}

// borrowedBoundBaselineCommitPreparationRowCount counts the durable
// target_guard_rebuild rows of one preparation identity through an independent
// connection.
func borrowedBoundBaselineCommitPreparationRowCount(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, operationID string) int {
	t.Helper()
	if operationID == "" {
		return 0
	}
	countCtx, cancelCount := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	defer cancelCount()
	var count int
	if err := b.fixture.controlPool.QueryRow(countCtx, `SELECT count(*)::int FROM recovery_audit WHERE action=$1 AND operation_id=$2`,
		controlstore.ActionTargetGuardRebuild, operationID).Scan(&count); err != nil {
		t.Fatalf("preparation audit count refused: %v", err)
	}
	return count
}

// borrowedBoundBaselineCommitMutantInstanceID derives a syntactically valid but
// non-existent instance identity from the authentic UUID (one hex digit
// changed), so the real LockInstance refuses with the authentic no-row stage
// without opening a second instance.
func borrowedBoundBaselineCommitMutantInstanceID(t *testing.T, b *borrowedSuccessorBaseline) string {
	t.Helper()
	id := b.fixture.instanceID
	if len(id) != 36 {
		t.Fatalf("bound baseline commit mutant instance id requires the authentic UUID, got %q", id)
	}
	mutated := []byte(id)
	switch mutated[len(mutated)-1] {
	case '0':
		mutated[len(mutated)-1] = '1'
	default:
		mutated[len(mutated)-1] = '0'
	}
	if string(mutated) == id {
		t.Fatal("bound baseline commit mutant instance id did not change the authentic identity")
	}
	return string(mutated)
}

// borrowedBoundBaselineCommitPostCommitProofs independently proves exactly the
// intended durable guard/audit delta, the released row's lockability and the
// original-owner health AFTER the acknowledged COMMIT. No success is inferred
// from the error path or from independently visible rows.
func borrowedBoundBaselineCommitPostCommitProofs(ctx context.Context, t *testing.T, b *borrowedSuccessorBaseline, plan *borrowedBoundBaselineCommitPlan, operationID string) error {
	t.Helper()
	f := b.fixture
	delta, err := borrowedBoundBaselineCommitReadDelta(ctx, f.controlPool, f.guardKey, operationID)
	if err != nil {
		return fmt.Errorf("%w: %v", errBoundBaselineCommitAuditDelta, err)
	}
	if delta.guardState != controlstore.TargetGuardClean || delta.guardActiveWriter || delta.guardOperation != operationID {
		return fmt.Errorf("%w: state=%q active=%t operation=%q want=%q", errBoundBaselineCommitGuardDelta, delta.guardState, delta.guardActiveWriter, delta.guardOperation, operationID)
	}
	if delta.auditCount != 1 || delta.auditResult != controlstore.AuditOK || delta.auditInstanceID != f.instanceID || !delta.evidenceMatches ||
		delta.auditScope != "bound_fixture_controlled_baseline_preparation" || delta.auditRestoration != "false" {
		return fmt.Errorf("%w: count=%d result=%q instance=%q scope=%q completed_restoration=%q evidence_matches=%t",
			errBoundBaselineCommitAuditDelta, delta.auditCount, delta.auditResult, delta.auditInstanceID, delta.auditScope, delta.auditRestoration, delta.evidenceMatches)
	}
	plan.durableGuardState = delta.guardState
	plan.durableGuardOperation = delta.guardOperation
	plan.durableAuditCount = delta.auditCount
	plan.durableAuditPreparation = true
	if disposition, operation, lockErr := borrowedReplacementGuardRowContenderLock(ctx, f.controlPool, f.guardKey); lockErr != nil || disposition != controlstore.TargetGuardClean || operation != operationID {
		return fmt.Errorf("%w: the released clean row was not lockable/unchanged: %q/%q err=%v", errBoundBaselineCommitGuardDelta, disposition, operation, lockErr)
	}
	plan.lockableAfterCommit = true
	healthCtx, cancelHealth := context.WithTimeout(ctx, borrowedOwnerRotationHealthBudget)
	healthErr := f.lock.Health(healthCtx)
	cancelHealth()
	if healthErr != nil {
		return fmt.Errorf("%w: original-owner health refused after the acknowledged commit: %v", errBoundBaselineCommitContinuation, healthErr)
	}
	plan.ownerHealthyAfterCommit = true
	plan.successInferred = false
	return nil
}

// borrowedBoundBaselineCommitStageRun is the duringProof stage. It NEVER trusts
// the flow's cached facts: it re-reads the effective fence, re-exercises every
// supported route, re-censuses and re-checks the observer/catalog/prefix, then
// runs the controlled baseline preparation through the retained lock's
// unchanged WithTransaction. In the positive the callback returns nil ONLY
// after the cancellation/prefix/live-prerequisite rechecks, and the unchanged
// WithTransaction performs the acknowledged COMMIT; no callback-side
// tx.Commit exists.
func borrowedBoundBaselineCommitStageRun(ctx context.Context, t *testing.T, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding, prepPID int, plan *borrowedBoundBaselineCommitPlan, preparation *borrowedBoundBaselineCommitPreparation) error {
	t.Helper()
	if plan == nil || preparation == nil {
		return errors.New("bound baseline commit requires the concrete plan and preparation")
	}
	if !preparation.claim() {
		return fmt.Errorf("%w: the preparation was already consumed", errBoundBaselineCommitReplay)
	}
	defer preparation.finish()
	f := b.fixture
	admin := f.fx.admin
	serverIP, err := f.fx.container.ContainerIP(ctx)
	if err != nil || serverIP == "" {
		return fmt.Errorf("%w: the fixture container endpoint is unknown: %v", errBoundBaselineCommitRoute, err)
	}

	// 1. Fresh effective writer-reject coverage (never the flow's cached scopes).
	freshRules, err := borrowedBoundWriterIsolationReadRules(ctx, admin)
	if err != nil {
		return fmt.Errorf("%w: %v", errBoundBaselineCommitCoverage, err)
	}
	freshScopes := borrowedBoundWriterIsolationScopes(freshRules)
	if len(freshScopes) == 0 {
		return fmt.Errorf("%w: no supported writer scopes were enumerated", errBoundBaselineCommitCoverage)
	}
	if err := borrowedBoundWriterIsolationFenceStateCheck(freshRules, freshScopes, f.writerRole); err != nil {
		return fmt.Errorf("%w: %v", errBoundBaselineCommitCoverage, err)
	}
	plan.freshCoverage = true

	// 2. Fresh real route refusals on every supported transport.
	for _, transport := range []string{"unix", "loopback", "serverip"} {
		line := ""
		if plan.routeOverride != nil {
			if override, ok := plan.routeOverride(transport); ok {
				line = override
			}
		}
		if line == "" {
			line = runHelperAuthCheck(t, ctx, f.fx.containerID, transport, serverIP, f.writerRole, b.passwordP1)
		}
		plan.routeLines[transport] = line
		if !borrowedBoundWriterIsolationContains(line, "REJECT", "28000") {
			return fmt.Errorf("%w: transport %s admission line %q", errBoundBaselineCommitRoute, transport, line)
		}
	}

	// 3. Fresh zero retained-writer census through an independent connection.
	censusConn, err := pgx.Connect(ctx, f.adminDSN)
	if err != nil {
		return fmt.Errorf("%w: independent census connection refused: %v", errBoundBaselineCommitCensus, err)
	}
	defer func() {
		closeCtx, cancelClose := context.WithTimeout(context.Background(), borrowedOwnerRotationCleanupBudget)
		_ = censusConn.Close(closeCtx)
		cancelClose()
	}()
	census, err := borrowedBoundWriterIsolationCensus(ctx, censusConn, f.writerRole)
	if err != nil {
		return fmt.Errorf("%w: %v", errBoundBaselineCommitCensus, err)
	}
	if census != 0 {
		return fmt.Errorf("%w: %d retained writer sessions", errBoundBaselineCommitCensus, census)
	}
	plan.census = census

	// 4. Fresh protected observer usability.
	if plan.observerNologin {
		nologinCtx, cancelNologin := context.WithTimeout(ctx, borrowedOwnerRotationAuthBudget)
		_, nologinErr := admin.Exec(nologinCtx, `ALTER ROLE `+pgx.Identifier{f.observerRole}.Sanitize()+` NOLOGIN`)
		cancelNologin()
		if nologinErr != nil {
			return fmt.Errorf("%w: observer suspension refused: %v", errBoundBaselineCommitObserver, nologinErr)
		}
		defer func() {
			loginCtx, cancelLogin := context.WithTimeout(context.Background(), borrowedOwnerRotationAuthBudget)
			_, _ = admin.Exec(loginCtx, `ALTER ROLE `+pgx.Identifier{f.observerRole}.Sanitize()+` LOGIN`)
			cancelLogin()
		}()
	}
	if err := borrowedBoundWriterIsolationObserverOK(ctx, f); err != nil {
		return fmt.Errorf("%w: %v", errBoundBaselineCommitObserver, err)
	}
	plan.observerOK = true

	// 5. Fresh actual pristine replacement catalog identity.
	if plan.renameTargetBeforeStage {
		changed := f.targetDB + "_commit_changed"
		termCtx, cancelTerm := context.WithTimeout(ctx, borrowedOwnerRotationAuthBudget)
		_, _ = admin.Exec(termCtx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=$1 AND pid <> pg_backend_pid()`, f.targetDB)
		renameCtx, cancelRename := context.WithTimeout(ctx, borrowedOwnerRotationAuthBudget)
		_, renameErr := admin.Exec(renameCtx, `ALTER DATABASE `+pgx.Identifier{f.targetDB}.Sanitize()+` RENAME TO `+pgx.Identifier{changed}.Sanitize())
		cancelRename()
		cancelTerm()
		if renameErr != nil {
			return fmt.Errorf("%w: replacement rename refused: %v", errBoundBaselineCommitCatalog, renameErr)
		}
		defer func() {
			backCtx, cancelBack := context.WithTimeout(context.Background(), borrowedOwnerRotationAuthBudget)
			_, _ = admin.Exec(backCtx, `ALTER DATABASE `+pgx.Identifier{changed}.Sanitize()+` RENAME TO `+pgx.Identifier{f.targetDB}.Sanitize())
			cancelBack()
		}()
	}
	catalogOID, err := borrowedOwnerDDLTargetOID(ctx, b)
	if err != nil || catalogOID != fresh.replacementOID {
		return fmt.Errorf("%w: actual catalog oid=%d err=%v", errBoundBaselineCommitCatalog, catalogOID, err)
	}
	plan.catalogOID = catalogOID

	// 6. Fresh shared-prefix validity.
	if plan.prefixLoss {
		borrowedReplacementSessionCopiedPrefixLoss(t, fresh)
	}
	if invalid, reason := fresh.prefix.Invalid(); invalid {
		return fmt.Errorf("%w: the shared replacement prefix is permanently invalidated: %s", errBoundBaselineCommitPrefix, reason)
	}

	if plan.cancelAt == "before-mutation" {
		plan.cancelObserved = true
		plan.reachedStage = "before-mutation"
		if plan.cancelLane != nil {
			plan.cancelLane()
		}
		return fmt.Errorf("%w: cancelled before the controlled transition", errBoundBaselineCommitCancelled)
	}

	// 7. The controlled baseline preparation through the retained lock's
	// UNCHANGED WithTransaction; only a nil callback return lets the unchanged
	// path commit.
	preparationOperationID := plan.operationIDOverride
	if preparationOperationID == "" {
		preparationOperationID = fmt.Sprintf("bound-baseline-commit-%d", time.Now().UnixNano())
	}
	plan.rehearsalOperationID = preparationOperationID
	expectedFingerprint := fresh.binding.OriginalRoleFingerprint()
	if plan.expectedFingerprint != "" {
		expectedFingerprint = plan.expectedFingerprint
	}
	guardKey := f.guardKey
	if plan.fenceKeyOverride != "" {
		guardKey = plan.fenceKeyOverride
	}
	guardOperation := f.operation
	if plan.fenceOperationOverride != "" {
		guardOperation = plan.fenceOperationOverride
	}
	instanceID := f.instanceID
	if plan.instanceIDOverride != "" {
		instanceID = plan.instanceIDOverride
	}
	evidenceBytes, err := json.Marshal(map[string]any{
		"scope":                 "bound_fixture_controlled_baseline_preparation",
		"completed_restoration": false,
		"instance_id":           f.instanceID,
		"target_guard_key":      f.guardKey,
		"role_fingerprint":      expectedFingerprint,
		"replacement_oid":       fresh.replacementOID,
		"owner_pid":             b.owner.BackendPID,
		"preparation_tx_pid":    prepPID,
		"routes":                []string{"unix", "loopback", "serverip"},
		"operation_id":          preparationOperationID,
	})
	if err != nil {
		return fmt.Errorf("%w: preparation evidence construction refused: %v", errBoundBaselineCommitClean, err)
	}

	if plan.cancelAt == "before-transaction" {
		plan.cancelObserved = true
		plan.reachedStage = "before-transaction"
		if plan.cancelLane != nil {
			plan.cancelLane()
		}
	}
	lock := f.lock
	if plan.lockOverride != nil {
		lock = plan.lockOverride
	}
	callbackRan := false
	var callbackErr error
	callbackBody := func(txCtx context.Context, tx pgx.Tx) error {
		// The supplied transaction must be the original anchored owner session,
		// never the coordinator preparation session (or, in the dedicated
		// COMMIT-acknowledgement-loss control, the companion-wrapped owner
		// session whose expected identity is recorded in the plan).
		var txPID int
		var txStart time.Time
		identCtx, cancelIdent := context.WithTimeout(txCtx, borrowedOwnerRotationQueryBudget)
		identErr := tx.QueryRow(identCtx, `SELECT pid::int, backend_start FROM pg_stat_activity WHERE pid = pg_backend_pid()`).Scan(&txPID, &txStart)
		cancelIdent()
		if identErr != nil {
			return fmt.Errorf("%w: supplied-tx identity read refused", errBoundBaselineCommitOwnerTx)
		}
		expectedPID := b.owner.BackendPID
		expectedStart := b.owner.BackendStart
		if plan.ownerPIDOverride > 0 {
			expectedPID = plan.ownerPIDOverride
			expectedStart = plan.ownerStartOverride
		}
		if txPID != expectedPID || !txStart.Equal(expectedStart) {
			return fmt.Errorf("%w: supplied-tx pid %d is not the expected anchored owner %d", errBoundBaselineCommitOwnerTx, txPID, expectedPID)
		}
		if prepPID > 0 && txPID == prepPID {
			return fmt.Errorf("%w: supplied tx reused the coordinator preparation session", errBoundBaselineCommitOwnerTx)
		}
		k1 := b.owner.NamespaceClassID
		k2 := b.owner.NamespaceObjectID
		if plan.ownerPIDOverride > 0 {
			k1 = uint32(plan.namespaceK1Override)
			k2 = uint32(plan.namespaceK2Override)
		}
		nsCtx, cancelNs := context.WithTimeout(txCtx, borrowedOwnerRotationQueryBudget)
		var namespace bool
		nsErr := tx.QueryRow(nsCtx, `
SELECT EXISTS (
  SELECT 1 FROM pg_locks
  WHERE locktype = 'advisory' AND granted AND pid = pg_backend_pid() AND objsubid = 2
    AND classid = $1::oid AND objid = $2::oid AND database = $3::oid
)`, k1, k2, b.owner.NamespaceDatabaseID).Scan(&namespace)
		cancelNs()
		if nsErr != nil || !namespace {
			return fmt.Errorf("%w: original advisory namespace not held", errBoundBaselineCommitOwnerTx)
		}
		plan.ownerTxPID = txPID

		instanceToken, err := controlstore.LockInstance(txCtx, tx, instanceID)
		if err != nil {
			return fmt.Errorf("%w: instance lock refused: %w", errBoundBaselineCommitInstance, err)
		}
		if instanceToken.State != "open" || instanceToken.TargetGuardKey != f.guardKey || instanceToken.TargetRoleFingerprint != expectedFingerprint {
			return fmt.Errorf("%w: instance state=%q key=%q fingerprint=%q", errBoundBaselineCommitInstance, instanceToken.State, instanceToken.TargetGuardKey, instanceToken.TargetRoleFingerprint)
		}
		plan.instanceState = instanceToken.State
		plan.instanceKey = instanceToken.TargetGuardKey
		plan.instanceFingerprint = instanceToken.TargetRoleFingerprint

		disposition, operation, fenceErr := borrowedReplacementGuardRowFence(txCtx, tx, guardKey, guardOperation)
		if fenceErr != nil {
			return fenceErr
		}
		plan.guardDisposition = disposition
		plan.guardOperation = operation

		var actualOID uint32
		oidCtx, cancelOID := context.WithTimeout(txCtx, borrowedOwnerRotationQueryBudget)
		oidErr := tx.QueryRow(oidCtx, `SELECT oid::oid FROM pg_database WHERE datname=$1`, f.targetDB).Scan(&actualOID)
		cancelOID()
		if oidErr != nil || actualOID != fresh.replacementOID {
			return fmt.Errorf("%w: in-transaction replacement oid=%d err=%v", errBoundBaselineCommitCatalog, actualOID, oidErr)
		}

		if plan.latePrefixLoss {
			borrowedReplacementSessionCopiedPrefixLoss(t, fresh)
		}
		if invalid, reason := fresh.prefix.Invalid(); invalid {
			return fmt.Errorf("%w: the shared replacement prefix was invalidated under the held transaction: %s", errBoundBaselineCommitPrefix, reason)
		}
		if err := controlstore.RecordTargetGuardRebuild(txCtx, tx, instanceID, guardKey, "deploy:bound-baseline-commit", preparationOperationID, evidenceBytes); err != nil {
			return fmt.Errorf("%w: rebuild audit refused: %v", errBoundBaselineCommitClean, err)
		}
		plan.auditExecuted = true
		if plan.cancelAt == "between-writes" {
			localCtx, cancelLocal := context.WithTimeout(txCtx, borrowedOwnerRotationQueryBudget)
			var written int
			writeErr := tx.QueryRow(localCtx, `SELECT count(*)::int FROM recovery_audit WHERE action=$1 AND operation_id=$2`,
				controlstore.ActionTargetGuardRebuild, preparationOperationID).Scan(&written)
			cancelLocal()
			plan.auditWrittenBeforeCancel = writeErr == nil && written == 1
			plan.cancelObserved = true
			plan.reachedStage = "between-writes"
			if plan.cancelLane != nil {
				plan.cancelLane()
			}
			return fmt.Errorf("%w: cancelled between the audit and the resolution", errBoundBaselineCommitCancelled)
		}
		resolveEvidence := evidenceBytes
		if plan.emptyResolveEvidence {
			resolveEvidence = []byte{}
		}
		plan.resolveAttempted = true
		if err := controlstore.ResolveTargetGuardClean(txCtx, tx, guardKey, preparationOperationID, resolveEvidence); err != nil {
			plan.resolveRefused = true
			return fmt.Errorf("%w: controlled resolution refused: %v", errBoundBaselineCommitClean, err)
		}
		if err := controlstore.RequireCleanTargetGuard(txCtx, tx, instanceID, guardKey, expectedFingerprint); err != nil {
			return fmt.Errorf("%w: transaction-local clean verification refused: %v", errBoundBaselineCommitClean, err)
		}
		resolved, found, readErr := controlstore.ReadTargetGuard(txCtx, tx, guardKey)
		if readErr != nil || !found || resolved.State != controlstore.TargetGuardClean {
			return fmt.Errorf("%w: transaction-local resolved guard read refused (err=%v found=%t)", errBoundBaselineCommitClean, readErr, found)
		}
		plan.cleanVerified = true
		localAuditCtx, cancelLocalAudit := context.WithTimeout(txCtx, borrowedOwnerRotationQueryBudget)
		var localAudit int
		localAuditErr := tx.QueryRow(localAuditCtx, `SELECT count(*)::int FROM recovery_audit WHERE action=$1 AND operation_id=$2`,
			controlstore.ActionTargetGuardRebuild, preparationOperationID).Scan(&localAudit)
		cancelLocalAudit()
		if localAuditErr != nil || localAudit != 1 {
			return fmt.Errorf("%w: transaction-local preparation audit count=%d err=%v", errBoundBaselineCommitClean, localAudit, localAuditErr)
		}
		plan.localAuditFound = true

		// Independent proofs while the supplied transaction still holds the row.
		pendingDisposition, pendingOperation := borrowedReplacementGuardRowRead(t, txCtx, f.controlPool, guardKey)
		if pendingDisposition == "clean" {
			return fmt.Errorf("%w: the committed guard was already clean", errBoundBaselineCommitContender)
		}
		if pendingOperation != f.operation {
			return fmt.Errorf("%w: the committed guard operation provenance changed", errBoundBaselineCommitContender)
		}
		pendingAuditCtx, cancelPendingAudit := context.WithTimeout(txCtx, borrowedOwnerRotationQueryBudget)
		var pendingAudit int
		pendingAuditErr := f.controlPool.QueryRow(pendingAuditCtx, `SELECT count(*)::int FROM recovery_audit WHERE action=$1 AND operation_id=$2`,
			controlstore.ActionTargetGuardRebuild, preparationOperationID).Scan(&pendingAudit)
		cancelPendingAudit()
		if pendingAuditErr != nil || pendingAudit != 0 {
			return fmt.Errorf("%w: the pending preparation audit was visible before COMMIT (visible=%d err=%v)", errBoundBaselineCommitContender, pendingAudit, pendingAuditErr)
		}
		if _, _, contenderErr := borrowedReplacementGuardRowContenderLock(txCtx, f.controlPool, guardKey); contenderErr == nil {
			return fmt.Errorf("%w: the contender locked the held row", errBoundBaselineCommitContender)
		} else if code := borrowedReplacementGuardRowCode(contenderErr); code != "55P03" {
			return fmt.Errorf("%w: contender refusal was %q, not the structured 55P03", errBoundBaselineCommitContender, code)
		} else {
			plan.pendingGuardState = pendingDisposition
			plan.pendingAuditVisible = pendingAudit
			plan.contenderCode = code
		}
		if plan.cancelAt == "after-resolution" {
			plan.cancelObserved = true
			plan.reachedStage = "after-resolution"
			if plan.cancelLane != nil {
				plan.cancelLane()
			}
			return fmt.Errorf("%w: cancelled after the pending resolution", errBoundBaselineCommitCancelled)
		}

		// Recheck cancellation/prefix + the relevant live prerequisites BEFORE
		// callback success: nothing may be committed under a lost prerequisite.
		if err := txCtx.Err(); err != nil {
			plan.cancelObserved = true
			plan.reachedStage = "pre-commit-recheck"
			return fmt.Errorf("%w: the lane context ended before the commit: %v", errBoundBaselineCommitCancelled, err)
		}
		if invalid, reason := fresh.prefix.Invalid(); invalid {
			return fmt.Errorf("%w: the shared replacement prefix was invalidated before the commit: %s", errBoundBaselineCommitPrefix, reason)
		}
		recheckRules, err := borrowedBoundWriterIsolationReadRules(txCtx, admin)
		if err != nil {
			return fmt.Errorf("%w: pre-commit coverage recheck refused: %v", errBoundBaselineCommitCoverage, err)
		}
		recheckScopes := borrowedBoundWriterIsolationScopes(recheckRules)
		if len(recheckScopes) == 0 {
			return fmt.Errorf("%w: pre-commit recheck enumerated no supported scopes", errBoundBaselineCommitCoverage)
		}
		if err := borrowedBoundWriterIsolationFenceStateCheck(recheckRules, recheckScopes, f.writerRole); err != nil {
			return fmt.Errorf("%w: pre-commit coverage recheck refused: %v", errBoundBaselineCommitCoverage, err)
		}
		census2, err := borrowedBoundWriterIsolationCensus(txCtx, censusConn, f.writerRole)
		if err != nil {
			return fmt.Errorf("%w: pre-commit census recheck refused: %v", errBoundBaselineCommitCensus, err)
		}
		if census2 != 0 {
			return fmt.Errorf("%w: %d retained writer sessions at the pre-commit recheck", errBoundBaselineCommitCensus, census2)
		}
		if err := borrowedBoundWriterIsolationObserverOK(txCtx, f); err != nil {
			return fmt.Errorf("%w: pre-commit observer recheck refused: %v", errBoundBaselineCommitObserver, err)
		}
		var recheckOID uint32
		oidRecheckCtx, cancelOIDRecheck := context.WithTimeout(txCtx, borrowedOwnerRotationQueryBudget)
		recheckOIDErr := tx.QueryRow(oidRecheckCtx, `SELECT oid::oid FROM pg_database WHERE datname=$1`, f.targetDB).Scan(&recheckOID)
		cancelOIDRecheck()
		if recheckOIDErr != nil || recheckOID != fresh.replacementOID {
			return fmt.Errorf("%w: pre-commit catalog recheck oid=%d err=%v", errBoundBaselineCommitCatalog, recheckOID, recheckOIDErr)
		}
		var ownerAlive int
		ownerCtx, cancelOwner := context.WithTimeout(txCtx, borrowedOwnerRotationQueryBudget)
		ownerErr := f.controlPool.QueryRow(ownerCtx, `SELECT count(*)::int FROM pg_stat_activity WHERE pid=$1 AND backend_start=$2`, expectedPID, expectedStart).Scan(&ownerAlive)
		cancelOwner()
		if ownerErr != nil || ownerAlive != 1 {
			return fmt.Errorf("%w: the expected owner session is not alive at the pre-commit recheck", errBoundBaselineCommitOwnerTx)
		}
		plan.liveRecheckOK = true

		if plan.ownerLossBeforeCommit {
			// Test-only injection: terminate the owner connection AFTER every
			// prerequisite recheck and just before the callback returns nil, so
			// the unchanged commit/held-check path reports the authentic
			// rollback-uncertainty semantics. NOTHING is committed.
			lossCtx, cancelLoss := context.WithTimeout(context.Background(), borrowedOwnerRotationQueryBudget)
			var terminated bool
			lossErr := f.controlPool.QueryRow(lossCtx, `SELECT pg_terminate_backend($1)`, b.owner.BackendPID).Scan(&terminated)
			cancelLoss()
			if lossErr != nil || !terminated {
				return fmt.Errorf("%w: owner termination refused: %v", errBoundBaselineCommitUncertain, lossErr)
			}
		}
		return nil
	}
	txErr := lock.WithTransaction(ctx, func(txCtx context.Context, tx pgx.Tx) error {
		callbackRan = true
		callbackErr = callbackBody(txCtx, tx)
		return callbackErr
	})
	if txErr == nil {
		// Acknowledged COMMIT: independently prove exactly the intended durable
		// delta, the released row's lockability and the original-owner health.
		plan.commitAcknowledged = true
		if proofErr := borrowedBoundBaselineCommitPostCommitProofs(ctx, t, b, plan, preparationOperationID); proofErr != nil {
			return proofErr
		}
		if plan.postCommitOwnerLoss {
			lossCtx, cancelLoss := context.WithTimeout(context.Background(), borrowedOwnerRotationQueryBudget)
			var terminated bool
			lossErr := f.controlPool.QueryRow(lossCtx, `SELECT pg_terminate_backend($1)`, b.owner.BackendPID).Scan(&terminated)
			cancelLoss()
			if lossErr != nil || !terminated {
				return fmt.Errorf("%w: owner termination after the acknowledged commit refused: %v", errBoundBaselineCommitContinuation, lossErr)
			}
			plan.ownerHealthyAfterCommit = false
			borrowedBoundBaselineCommitAssertPreservedDelta(t, "post-commit owner loss", ctx, b, preparationOperationID)
			return fmt.Errorf("%w: the original owner session was lost after the acknowledged commit", errBoundBaselineCommitContinuation)
		}
		if plan.postCommitTeardown {
			var adminPID int
			adminCtx, cancelAdmin := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
			adminPIDErr := admin.QueryRow(adminCtx, `SELECT pg_backend_pid()`).Scan(&adminPID)
			cancelAdmin()
			if adminPIDErr != nil || adminPID <= 0 {
				return fmt.Errorf("%w: teardown dependency identity refused: %v", errBoundBaselineCommitTeardown, adminPIDErr)
			}
			termCtx, cancelTerm := context.WithTimeout(context.Background(), borrowedOwnerRotationQueryBudget)
			var terminated bool
			termErr := f.controlPool.QueryRow(termCtx, `SELECT pg_terminate_backend($1)`, adminPID).Scan(&terminated)
			cancelTerm()
			if termErr != nil || !terminated {
				return fmt.Errorf("%w: teardown dependency termination refused: %v", errBoundBaselineCommitTeardown, termErr)
			}
			borrowedBoundBaselineCommitAssertPreservedDelta(t, "post-commit teardown failure", ctx, b, preparationOperationID)
			return fmt.Errorf("%w: the fixture teardown dependency was lost after the acknowledged commit", errBoundBaselineCommitTeardown)
		}
		return nil
	}
	// COMMIT acknowledgement loss: the unchanged path reported an ambiguous
	// COMMIT error. Independently observe the durable delta, but NEVER relabel
	// success from the error or from the visible rows.
	if strings.Contains(txErr.Error(), "commit target-lock acceptance transaction") {
		plan.commitAckLost = true
		if proofErr := borrowedBoundBaselineCommitPostCommitProofs(ctx, t, b, plan, preparationOperationID); proofErr != nil {
			return errors.Join(fmt.Errorf("%w: %v", errBoundBaselineCommitAckLoss, txErr), proofErr)
		}
		return fmt.Errorf("%w: %v", errBoundBaselineCommitAckLoss, txErr)
	}
	if strings.Contains(txErr.Error(), "rollback target-lock acceptance transaction") {
		return fmt.Errorf("%w: %v", errBoundBaselineCommitUncertain, txErr)
	}
	if !callbackRan || callbackErr == nil {
		// A refusal before the callback (BEGIN or the held-lock check) rolled
		// back NO transaction: no acknowledgment may be recorded.
		return fmt.Errorf("%w: the refusal preceded the callback (ran=%t): %v", errBoundBaselineCommitPreCallback, callbackRan, txErr)
	}
	plan.rollbackAcknowledged = true
	return txErr
}

// borrowedBoundBaselineCommitApplyFence applies the full writer-reject fence
// for every enumerated scope on an independent fixture and returns an
// idempotent disposal that restores the original HBA and reloads.
func borrowedBoundBaselineCommitApplyFence(ctx context.Context, t *testing.T, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding) func() {
	t.Helper()
	f := b.fixture
	admin := f.fx.admin
	hbaPath, err := borrowedBoundWriterIsolationHBAPath(ctx, admin)
	if err != nil {
		t.Fatalf("baseline commit fence path refused: %v", err)
	}
	original, err := borrowedBoundWriterIsolationReadHBA(ctx, f, hbaPath)
	if err != nil {
		t.Fatalf("baseline commit fence read refused: %v", err)
	}
	rules, err := borrowedBoundWriterIsolationReadRules(ctx, admin)
	if err != nil {
		t.Fatalf("baseline commit fence rules refused: %v", err)
	}
	scopes := borrowedBoundWriterIsolationScopes(rules)
	if len(scopes) == 0 {
		t.Fatal("baseline commit fence requires enumerated writer scopes")
	}
	fenced := borrowedBoundWriterIsolationFencedContent(original, scopes, f.writerRole)
	if err := borrowedBoundWriterIsolationWriteHBA(ctx, t, f, hbaPath, fenced); err != nil {
		t.Fatalf("baseline commit fence write refused: %v", err)
	}
	if err := borrowedBoundWriterIsolationReload(ctx, admin); err != nil {
		t.Fatalf("baseline commit fence reload refused: %v", err)
	}
	serverIP, err := f.fx.container.ContainerIP(ctx)
	if err != nil {
		t.Fatalf("baseline commit fence endpoint refused: %v", err)
	}
	if err := borrowedBoundWriterIsolationAwaitState(t, ctx, f, serverIP, f.writerRole, b.passwordP1, "REJECT", "28000", 10*time.Second); err != nil {
		t.Fatalf("baseline commit fence did not become effective: %v", err)
	}
	restored := false
	restore := func() {
		if restored {
			return
		}
		restored = true
		cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancelCleanup()
		_ = borrowedBoundWriterIsolationWriteHBA(cleanupCtx, t, f, hbaPath, original)
		_ = borrowedBoundWriterIsolationReload(cleanupCtx, admin)
	}
	t.Cleanup(restore)
	return restore
}

// borrowedBoundBaselineCommitFaultWitness preserves the actually witnessed
// companion fault facts and the independent committed-delta observation.
type borrowedBoundBaselineCommitFaultWitness struct {
	dispatch          bool
	withheld          bool
	broken            bool
	durableAuditCount int
	guardState        string
	guardOperation    string
	lockable          bool
	err               error
}

// borrowedBoundBaselineCommitFaultWorker waits boundedly for the actual COMMIT
// dispatch and its strictly validated withholding, independently observes the
// committed guard/audit delta while the completion is still withheld, and then
// breaks the owner transport so the ACTUAL Commit call returns an error. It
// never infers success and never reuses the uncertain owner session.
type borrowedBoundBaselineCommitFaultWorker struct {
	ctx         context.Context
	cancel      context.CancelFunc
	b           *borrowedSuccessorBaseline
	fault       *recovery.TargetLockCommitAmbiguityFault
	operationID string
	doneCh      chan struct{}
	resultCh    chan *borrowedBoundBaselineCommitFaultWitness
}

func startBorrowedBoundBaselineCommitFaultWatcher(ctx context.Context, t *testing.T, b *borrowedSuccessorBaseline, fault *recovery.TargetLockCommitAmbiguityFault, operationID string) (*borrowedBoundBaselineCommitFaultWorker, func() error) {
	t.Helper()
	workerCtx, cancel := context.WithCancel(ctx)
	worker := &borrowedBoundBaselineCommitFaultWorker{
		ctx: workerCtx, cancel: cancel, b: b, fault: fault, operationID: operationID,
		doneCh:   make(chan struct{}),
		resultCh: make(chan *borrowedBoundBaselineCommitFaultWitness, 1),
	}
	var cleanupOnce sync.Once
	cleanup := func() error {
		var cleanupErr error
		cleanupOnce.Do(func() {
			cancel()
			fault.Break()
			select {
			case <-worker.doneCh:
			case <-time.After(10 * time.Second):
				cleanupErr = errors.New("baseline commit fault worker did not exit within the bounded unwind join")
			}
		})
		return cleanupErr
	}
	t.Cleanup(func() {
		if cleanupErr := cleanup(); cleanupErr != nil {
			t.Errorf("baseline commit fault worker cleanup fallback: %v", cleanupErr)
		}
	})
	go worker.run()
	return worker, cleanup
}

func (w *borrowedBoundBaselineCommitFaultWorker) join(t *testing.T) *borrowedBoundBaselineCommitFaultWitness {
	t.Helper()
	select {
	case <-w.doneCh:
	case <-time.After(120 * time.Second):
		t.Fatal("baseline commit fault worker did not finish within the bounded join")
	}
	select {
	case result := <-w.resultCh:
		return result
	default:
		t.Fatal("baseline commit fault worker completed without publishing a result")
		return nil
	}
}

func (w *borrowedBoundBaselineCommitFaultWorker) run() {
	defer close(w.doneCh)
	result := &borrowedBoundBaselineCommitFaultWitness{}
	defer func() {
		w.fault.Break()
		result.broken = true
		w.resultCh <- result
	}()
	dispatchCtx, cancelDispatch := context.WithTimeout(w.ctx, 90*time.Second)
	defer cancelDispatch()
	if !w.fault.AwaitDispatched(dispatchCtx) {
		result.err = errors.New("NOTESTABLISHED: the real COMMIT was never observed on the wire")
		return
	}
	result.dispatch = true
	withheldCtx, cancelWithheld := context.WithTimeout(w.ctx, 90*time.Second)
	defer cancelWithheld()
	if !w.fault.AwaitWithheld(withheldCtx) {
		result.err = errors.New("NOTESTABLISHED: the validated COMMIT completion was never withheld")
		return
	}
	result.withheld = true
	// While the validated completion is withheld (the server has committed),
	// the intended durable delta MUST be independently visible.
	delta, err := borrowedBoundBaselineCommitReadDelta(w.ctx, w.b.fixture.controlPool, w.b.fixture.guardKey, w.operationID)
	if err != nil {
		result.err = fmt.Errorf("independent committed-delta read refused: %w", err)
		return
	}
	result.durableAuditCount = delta.auditCount
	result.guardState = delta.guardState
	result.guardOperation = delta.guardOperation
	lockCtx, cancelLock := context.WithTimeout(w.ctx, borrowedOwnerRotationQueryBudget)
	_, _, contenderErr := borrowedReplacementGuardRowContenderLock(lockCtx, w.b.fixture.controlPool, w.b.fixture.guardKey)
	cancelLock()
	result.lockable = contenderErr == nil
}

// TestBorrowedReplacementBoundBaselineCommit is the bounded live-fence
// controlled-baseline acknowledged-commit lane described in the file header.
func TestBorrowedReplacementBoundBaselineCommit(t *testing.T) {
	ctx := t.Context()

	// P: the single chain over ONE authentic bound fixture.
	func() {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		f := b.fixture
		borrowedBoundSessionAssertProvenance(t, ctx, b, fresh, "bound baseline commit")
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)

		// Existing bound dirty-guard refusal; capture the coordinator
		// preparation transaction PID through the existing marker seam.
		var probeCalls, acceptanceCalls int32
		prepAttemptID := fmt.Sprintf("bound-baseline-commit-refusal-%d", time.Now().UnixNano())
		marker := newBorrowedBoundNativeReadyMarker(prepAttemptID, f.adminRole)
		var prepTxPID int
		inner := marker.inner
		marker.inner = func(hookCtx context.Context, tx pgx.Tx, locked controlstore.InstanceToken) (recovery.EvidenceToken, error) {
			var pid int
			if err := tx.QueryRow(hookCtx, `SELECT pg_backend_pid()`).Scan(&pid); err == nil && pid > 0 {
				prepTxPID = pid
			}
			return inner(hookCtx, tx, locked)
		}
		attempt := prepareBorrowedBoundNativeReadyAttempt(t, ctx, b, fresh, prepAttemptID, marker, &probeCalls, &acceptanceCalls)
		if err := attempt.registerOrigin(ctx); err != nil {
			t.Fatalf("bound baseline commit refusal origin registration refused: %v", err)
		}
		result, receipt, runErr := dispatchBorrowedBoundNativeReady(t, ctx, attempt)
		if runErr == nil || runErr.Error() != "bound target guard is not clean" {
			t.Fatalf("bound baseline commit refusal is not the exact production bound dirty-guard refusal: %v", runErr)
		}
		if got := marker.callsNow(); got != 1 {
			t.Fatalf("bound baseline commit refusal marker calls=%d, want exactly 1", got)
		}
		if refusalToken := marker.token(); result.MarkerToken != refusalToken || refusalToken.InstanceID != f.instanceID || refusalToken.State != "open" {
			t.Fatalf("bound baseline commit refusal did not carry the genuine transaction-local marker token: %+v", refusalToken)
		}
		borrowedBoundNativeReadyRequireNoProbeOutput(t, "bound baseline commit refusal", result)
		borrowedBoundNativeReadyRequireAbsentReceipt(t, "bound baseline commit refusal", receipt)
		borrowedBoundReentryAssertOutcomeBoundaries(t, "bound baseline commit refusal", attempt, result, receipt, &probeCalls, &acceptanceCalls)
		if prepTxPID <= 0 || prepTxPID == b.owner.BackendPID {
			t.Fatalf("bound baseline commit coordinator prep tx pid=%d owner=%d", prepTxPID, b.owner.BackendPID)
		}
		borrowedBoundReentryReadinessAssertBaseline(t, "bound baseline commit refusal", ctx, b, base)

		// Readiness consumed once as NON-AUTHORIZING lineage facts, then strict
		// retirement.
		stage := &borrowedBoundReentryReadinessStage{
			key: f.guardKey, expectedOperation: f.operation,
			journalAttemptID: prepAttemptID, journalFacts: &borrowedReplacementOwnerJournalFacts{},
		}
		outcome := borrowedBoundReentryReadinessRun(ctx, t, b, fresh, stage, nil)
		readiness := newBorrowedBoundReentryReadiness()
		if err := borrowedBoundReentryReadinessTryPublish(t, "bound baseline commit readiness", b, fresh, stage, outcome, readiness); err != nil {
			t.Fatalf("bound baseline commit readiness refused: %v", err)
		}
		readinessFacts, ok := readiness.consume()
		if !ok {
			t.Fatal("bound baseline commit readiness witness was not consumable once")
		}
		if readinessFacts.InstanceID != f.instanceID || readinessFacts.TargetKey != f.guardKey ||
			readinessFacts.RoleFingerprint != fresh.binding.OriginalRoleFingerprint() ||
			readinessFacts.ReplacementOID != fresh.replacementOID || readinessFacts.OwnerPID != b.owner.BackendPID {
			t.Fatalf("bound baseline commit readiness facts are not the observed provenance: %+v", readinessFacts)
		}
		if _, again := readiness.consume(); again {
			t.Fatal("bound baseline commit readiness witness was consumable twice")
		}
		readinessCopy := *readiness
		if _, copyOK := readinessCopy.consume(); copyOK {
			t.Fatal("a shared-state readiness copy was consumable")
		}
		if err := borrowedReplacementOwnerTailRetireGuarded(t, ctx, b, outcome); err != nil {
			t.Fatalf("bound baseline commit registered-P1 retirement refused: %v", err)
		}

		// Full durable baseline AFTER the prerequisite journal writes and before
		// the controlled baseline preparation.
		base = borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)

		// The unchanged writer-isolation flow with the controlled-baseline
		// preparation stage in its existing duringProof hook.
		plan := newBorrowedBoundBaselineCommitPlan()
		preparation := newBorrowedBoundBaselineCommitPreparation()
		flowState, flowErr := borrowedBoundWriterIsolationRun(ctx, t, b, fresh, &borrowedBoundWriterIsolationLineage{
			consumed: true, readinessBacked: false,
			instanceID: f.instanceID, targetKey: f.guardKey,
			fingerprint:    fresh.binding.OriginalRoleFingerprint(),
			replacementOID: fresh.replacementOID, ownerPID: b.owner.BackendPID,
			issuance: &borrowedBoundWriterIsolationIssuance{},
		}, &borrowedBoundWriterIsolationHooks{
			duringProof: func(hookCtx context.Context, _ *borrowedBoundWriterIsolationFlowState) error {
				return borrowedBoundBaselineCommitStageRun(hookCtx, t, b, fresh, prepTxPID, plan, preparation)
			},
		})
		if flowErr != nil {
			t.Fatalf("bound baseline commit positive refused: %v", flowErr)
		}
		if !plan.commitAcknowledged || plan.commitAckLost || plan.rollbackAcknowledged {
			t.Fatalf("bound baseline commit positive commit classification: acknowledged=%t ackLost=%t rollback=%t", plan.commitAcknowledged, plan.commitAckLost, plan.rollbackAcknowledged)
		}
		if plan.ownerTxPID != b.owner.BackendPID || plan.ownerTxPID == prepTxPID {
			t.Fatalf("bound baseline commit owner transaction not distinguished: ownerTx=%d owner=%d prep=%d", plan.ownerTxPID, b.owner.BackendPID, prepTxPID)
		}
		if !plan.freshCoverage || plan.census != 0 || plan.catalogOID != fresh.replacementOID ||
			plan.instanceState != "open" || plan.instanceKey != f.guardKey ||
			plan.instanceFingerprint != fresh.binding.OriginalRoleFingerprint() ||
			plan.guardDisposition == "clean" || plan.guardOperation != f.operation ||
			!plan.auditExecuted || !plan.resolveAttempted || plan.resolveRefused ||
			!plan.cleanVerified || !plan.localAuditFound || plan.pendingAuditVisible != 0 ||
			plan.contenderCode != "55P03" || !plan.liveRecheckOK {
			t.Fatalf("bound baseline commit in-transaction facts incomplete: %+v", plan)
		}
		if !plan.durableAuditPreparation || plan.durableAuditCount != 1 || plan.durableGuardState != controlstore.TargetGuardClean ||
			plan.durableGuardOperation != plan.rehearsalOperationID || !plan.lockableAfterCommit || !plan.ownerHealthyAfterCommit || plan.successInferred {
			t.Fatalf("bound baseline commit post-commit facts incomplete: %+v", plan)
		}
		if !preparation.claimedNow() || !preparation.finishedNow() {
			t.Fatal("bound baseline commit preparation was not claimed and finished by the stage")
		}
		if !flowState.cleanupRestored || flowState.cleanupErr != nil {
			t.Fatalf("bound baseline commit fence disposal was not proven clean: restored=%t err=%v", flowState.cleanupRestored, flowState.cleanupErr)
		}
		for _, transport := range []string{"unix", "loopback", "serverip"} {
			if !borrowedBoundWriterIsolationContains(plan.routeLines[transport], "REJECT", "28000") {
				t.Fatalf("bound baseline commit positive route %s admission %q", transport, plan.routeLines[transport])
			}
		}

		// Exactly the intended durable delta, independently re-read after the
		// teardown; all other instance/evidence/catalog/journal facts unchanged.
		borrowedBoundBaselineCommitAssertPreservedDelta(t, "bound baseline commit positive", ctx, b, plan.rehearsalOperationID)
		postSnap := borrowedBoundSessionSnapshotNow(t, ctx, b)
		if postSnap.state != base.snapshot.state || postSnap.key != base.snapshot.key ||
			postSnap.fingerprint != base.snapshot.fingerprint || postSnap.generation != base.snapshot.generation ||
			postSnap.evidenceHash != base.snapshot.evidenceHash || postSnap.inventoryNull != base.snapshot.inventoryNull ||
			postSnap.versionNull != base.snapshot.versionNull || postSnap.guardHash == base.snapshot.guardHash {
			t.Fatalf("bound baseline commit changed non-intended instance state: before=%+v after=%+v", base.snapshot, postSnap)
		}
		postRows := borrowedBoundNativeReadyRowsNow(t, ctx, b)
		if postRows.evidenceCount != base.rows.evidenceCount || postRows.evidenceHash != base.rows.evidenceHash || postRows.auditCount != base.rows.auditCount+1 {
			t.Fatalf("bound baseline commit changed non-intended evidence/audit rows: before=%+v after=%+v", base.rows, postRows)
		}
		if afterOID, oidErr := borrowedOwnerDDLTargetOID(ctx, b); oidErr != nil || afterOID != base.catalogOID {
			t.Fatalf("bound baseline commit changed the replacement catalog: oid=%d err=%v", afterOID, oidErr)
		}
		if count := borrowedReplacementOwnerJournalCount(t, ctx, f.controlPool, prepAttemptID); count != 1 {
			t.Fatalf("bound baseline commit changed the append-only refused journal row: count=%d", count)
		}
		if count := borrowedBoundBaselineCommitPreparationRowCount(t, ctx, b, prepAttemptID); count != 0 {
			t.Fatalf("bound baseline commit wrote %d preparation rows for the refused attempt identity", count)
		}
		if disposition, operation, contenderErr := borrowedReplacementGuardRowContenderLock(ctx, f.controlPool, f.guardKey); contenderErr != nil || disposition != controlstore.TargetGuardClean || operation != plan.rehearsalOperationID {
			t.Fatalf("bound baseline commit clean row was not independently lockable/unchanged: %q/%q err=%v", disposition, operation, contenderErr)
		}
		healthCtx, cancelHealth := context.WithTimeout(ctx, borrowedOwnerRotationHealthBudget)
		healthErr := f.lock.Health(healthCtx)
		cancelHealth()
		if healthErr != nil {
			t.Fatalf("bound baseline commit original-owner health refused: %v", healthErr)
		}
		serverIP, ipErr := f.fx.container.ContainerIP(ctx)
		if ipErr != nil {
			t.Fatalf("bound baseline commit endpoint refused: %v", ipErr)
		}
		if err := borrowedBoundWriterIsolationAwaitState(t, ctx, f, serverIP, f.writerRole, b.passwordP1, "AUTH_OK", "", 10*time.Second); err != nil {
			t.Fatalf("bound baseline commit fence disposal did not restore the HBA: %v", err)
		}
		restoredP0 := runHelperAuthCheck(t, ctx, f.fx.containerID, "serverip", serverIP, f.writerRole, f.writerPassword)
		if !borrowedBoundWriterIsolationContains(restoredP0, "REJECT", "28P01") {
			t.Fatalf("bound baseline commit fence disposal rehabilitated P0: %s", restoredP0)
		}
		t.Logf("bound baseline commit positive: owner tx pid %d distinct from prep tx pid %d; authentic instance and original guard provenance locked; truthful fixture-scoped preparation evidence written via the production audit hook and the unchanged controlled resolution committed; durable delta independently proven (clean guard op=%s, exactly one instance-bound preparation audit, row lockable, owner healthy, clean teardown, no continuing isolation); all other instance/evidence/catalog/journal state unchanged; no admission handle, reusable preparation capability or post-commit restore dispatch", plan.ownerTxPID, prepTxPID, plan.rehearsalOperationID)
	}()

	// N1: historical/disposed fence: with no writer-reject rules the fresh
	// coverage verification refuses before any transaction, the one-shot
	// preparation is consumed and its replay refused.
	func() {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		plan := newBorrowedBoundBaselineCommitPlan()
		preparation := newBorrowedBoundBaselineCommitPreparation()
		stageErr := borrowedBoundBaselineCommitStageRun(ctx, t, b, fresh, 1, plan, preparation)
		if !errors.Is(stageErr, errBoundBaselineCommitCoverage) {
			t.Fatalf("historical-fence refusal is not the real coverage stage: %v", stageErr)
		}
		if plan.rollbackAcknowledged || plan.commitAcknowledged {
			t.Fatal("historical-fence refusal recorded a transaction outcome without a transaction")
		}
		if !preparation.claimedNow() || !preparation.finishedNow() {
			t.Fatal("historical-fence control did not consume its one-shot preparation")
		}
		if replay := borrowedBoundBaselineCommitStageRun(ctx, t, b, fresh, 1, plan, preparation); !errors.Is(replay, errBoundBaselineCommitReplay) {
			t.Fatalf("historical-fence replay was not refused by the preparation latch: %v", replay)
		}
		borrowedBoundReentryReadinessAssertBaseline(t, "bound baseline commit historical fence", ctx, b, base)
		t.Logf("bound baseline commit historical/disposed-fence negative: the fresh effective-coverage verification refused before any transaction, the one-shot preparation was consumed and its replay refused; durable state classification: complete baseline equality, no transaction, no authorizing output")
	}()

	// N2: incomplete coverage: a partial fence leaves an uncovered route; the
	// fresh coverage verification refuses and the uncovered transport still
	// admits a genuine P1.
	func() {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		f := b.fixture
		admin := f.fx.admin
		hbaPath, err := borrowedBoundWriterIsolationHBAPath(ctx, admin)
		if err != nil {
			t.Fatalf("incomplete-coverage HBA path refused: %v", err)
		}
		originalContent, err := borrowedBoundWriterIsolationReadHBA(ctx, f, hbaPath)
		if err != nil {
			t.Fatalf("incomplete-coverage HBA read refused: %v", err)
		}
		rules, err := borrowedBoundWriterIsolationReadRules(ctx, admin)
		if err != nil {
			t.Fatalf("incomplete-coverage rules read refused: %v", err)
		}
		scopes := borrowedBoundWriterIsolationScopes(rules)
		if len(scopes) < 2 {
			t.Fatalf("incomplete-coverage control requires at least two scopes, got %d", len(scopes))
		}
		partial := borrowedBoundWriterIsolationFencedContent(originalContent, scopes[:1], f.writerRole)
		if err := borrowedBoundWriterIsolationWriteHBA(ctx, t, f, hbaPath, partial); err != nil {
			t.Fatalf("incomplete-coverage fence write refused: %v", err)
		}
		defer func() {
			cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancelCleanup()
			_ = borrowedBoundWriterIsolationWriteHBA(cleanupCtx, t, f, hbaPath, originalContent)
			_ = borrowedBoundWriterIsolationReload(cleanupCtx, admin)
		}()
		if err := borrowedBoundWriterIsolationReload(ctx, admin); err != nil {
			t.Fatalf("incomplete-coverage reload refused: %v", err)
		}
		serverIP, ipErr := f.fx.container.ContainerIP(ctx)
		if ipErr != nil {
			t.Fatalf("incomplete-coverage endpoint refused: %v", ipErr)
		}
		unfenced := runHelperAuthCheck(t, ctx, f.fx.containerID, "serverip", serverIP, f.writerRole, b.passwordP1)
		if !strings.Contains(unfenced, "state=AUTH_OK") {
			t.Fatalf("incomplete-coverage control did not prove the uncovered route admits P1: %s", unfenced)
		}
		plan := newBorrowedBoundBaselineCommitPlan()
		preparation := newBorrowedBoundBaselineCommitPreparation()
		stageErr := borrowedBoundBaselineCommitStageRun(ctx, t, b, fresh, 1, plan, preparation)
		if !errors.Is(stageErr, errBoundBaselineCommitCoverage) {
			t.Fatalf("incomplete-coverage refusal is not the real coverage stage: %v", stageErr)
		}
		borrowedBoundReentryReadinessAssertBaseline(t, "bound baseline commit incomplete coverage", ctx, b, base)
		t.Logf("bound baseline commit incomplete-coverage negative: the partial fence left the container-address route admitting a genuine P1 and the fresh coverage verification refused; durable state classification: complete baseline equality, no transaction, no authorizing output")
	}()

	// N3: failed route refusal via the route override seam: the effective
	// coverage is complete but one transport still admits; the route stage
	// refuses before any transaction.
	func() {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		restore := borrowedBoundBaselineCommitApplyFence(ctx, t, b, fresh)
		defer restore()
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
			t.Fatalf("failed-route refusal is not the real route stage: %v", stageErr)
		}
		borrowedBoundReentryReadinessAssertBaseline(t, "bound baseline commit failed route", ctx, b, base)
		t.Logf("bound baseline commit failed-route negative: complete effective coverage with one transport admitting P1 refused at the real route stage before any transaction; durable state classification: complete baseline equality, no transaction, no authorizing output")
	}()

	// N4: retained writer session: a genuine P1 session opened before the stage
	// makes the fresh census refuse.
	func() {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		retainCtx, cancelRetain := context.WithTimeout(ctx, borrowedOwnerRotationAuthBudget)
		retained, retainErr := pgx.Connect(retainCtx, borrowedReplacementP1TargetDSN(t, b))
		cancelRetain()
		if retainErr != nil {
			t.Fatalf("retained-writer control P1 connect refused: %v", retainErr)
		}
		defer borrowedSuccessorCloseConn(t, retained)
		restore := borrowedBoundBaselineCommitApplyFence(ctx, t, b, fresh)
		defer restore()
		plan := newBorrowedBoundBaselineCommitPlan()
		preparation := newBorrowedBoundBaselineCommitPreparation()
		stageErr := borrowedBoundBaselineCommitStageRun(ctx, t, b, fresh, 1, plan, preparation)
		if !errors.Is(stageErr, errBoundBaselineCommitCensus) {
			t.Fatalf("retained-writer refusal is not the real census stage: %v", stageErr)
		}
		borrowedBoundReentryReadinessAssertBaseline(t, "bound baseline commit retained writer", ctx, b, base)
		t.Logf("bound baseline commit retained-writer negative: the pre-existing genuine P1 session made the fresh census refuse before any transaction; durable state classification: complete baseline equality, no transaction, no authorizing output")
	}()

	// N5: wrong REAL key / missing guard row: a different existing canonical key
	// is independently proven absent and the supplied-tx fence refuses at the
	// real no-row stage.
	func() {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		diffKey := borrowedBoundReentryDifferentKey(t, b)
		if count := borrowedReplacementGuardRowCount(t, ctx, b.fixture.controlPool, diffKey.String()); count != 0 {
			t.Fatalf("wrong-key control key already has %d guard rows", count)
		}
		restore := borrowedBoundBaselineCommitApplyFence(ctx, t, b, fresh)
		defer restore()
		plan := newBorrowedBoundBaselineCommitPlan()
		plan.fenceKeyOverride = diffKey.String()
		preparation := newBorrowedBoundBaselineCommitPreparation()
		stageErr := borrowedBoundBaselineCommitStageRun(ctx, t, b, fresh, 1, plan, preparation)
		if !errors.Is(stageErr, errGuardRowWindowRowRefused) || !strings.Contains(stageErr.Error(), "no guard row exists") {
			t.Fatalf("wrong-key refusal is not the real no-row fence stage: %v", stageErr)
		}
		if !plan.rollbackAcknowledged {
			t.Fatal("wrong-key refusal rollback was not acknowledged")
		}
		borrowedBoundReentryReadinessAssertBaseline(t, "bound baseline commit wrong key", ctx, b, base)
		if count := borrowedReplacementGuardRowCount(t, ctx, b.fixture.controlPool, diffKey.String()); count != 0 {
			t.Fatalf("wrong-key refusal created %d fallback inventory rows", count)
		}
		t.Logf("bound baseline commit wrong-key/missing-row negative: the real different canonical key was proven absent and the supplied-tx fence refused at the real no-row stage with the acknowledged rollback and complete baseline equality; no authorizing output")
	}()

	// N6: wrong REAL operation provenance on the real row: the supplied-tx
	// fence refuses at the operation-provenance stage.
	func() {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		restore := borrowedBoundBaselineCommitApplyFence(ctx, t, b, fresh)
		defer restore()
		plan := newBorrowedBoundBaselineCommitPlan()
		plan.fenceOperationOverride = b.fixture.operation + "-not-the-original"
		preparation := newBorrowedBoundBaselineCommitPreparation()
		stageErr := borrowedBoundBaselineCommitStageRun(ctx, t, b, fresh, 1, plan, preparation)
		if !errors.Is(stageErr, errGuardRowWindowRowRefused) || !strings.Contains(stageErr.Error(), "operation provenance") {
			t.Fatalf("wrong-operation refusal is not the real operation-provenance stage: %v", stageErr)
		}
		if !plan.rollbackAcknowledged {
			t.Fatal("wrong-operation refusal rollback was not acknowledged")
		} // the rollback of the real transaction must be acknowledged
		borrowedBoundReentryReadinessAssertBaseline(t, "bound baseline commit wrong operation", ctx, b, base)
		t.Logf("bound baseline commit wrong-operation negative: the real mismatched operation provenance refused the supplied-tx fence at the real operation-provenance stage with the acknowledged rollback and complete baseline equality; no authorizing output")
	}()

	// N7: wrong REAL role provenance: the observer role's real fingerprint
	// refuses the authentic instance binding.
	func() {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		observerTarget, err := controlstore.ParseDSNTarget(b.fixture.observerTargetDSN)
		if err != nil {
			t.Fatalf("wrong-role observer target parse refused: %v", err)
		}
		wrongFingerprint := observerTarget.DataTargetFingerprint().RoleFingerprint
		if wrongFingerprint == "" || wrongFingerprint == fresh.binding.OriginalRoleFingerprint() {
			t.Fatalf("wrong-role control did not obtain a distinct real fingerprint")
		}
		restore := borrowedBoundBaselineCommitApplyFence(ctx, t, b, fresh)
		defer restore()
		plan := newBorrowedBoundBaselineCommitPlan()
		plan.expectedFingerprint = wrongFingerprint
		preparation := newBorrowedBoundBaselineCommitPreparation()
		stageErr := borrowedBoundBaselineCommitStageRun(ctx, t, b, fresh, 1, plan, preparation)
		if !errors.Is(stageErr, errBoundBaselineCommitInstance) {
			t.Fatalf("wrong-role refusal is not the real instance-binding stage: %v", stageErr)
		}
		if !plan.rollbackAcknowledged {
			t.Fatal("wrong-role refusal rollback was not acknowledged")
		}
		borrowedBoundReentryReadinessAssertBaseline(t, "bound baseline commit wrong role", ctx, b, base)
		t.Logf("bound baseline commit wrong-role negative: the observer role's real fingerprint refused the authentic instance binding at the real stage with the acknowledged rollback and complete baseline equality; no authorizing output")
	}()

	// N8: wrong instance identity: a valid-format non-existent UUID refuses the
	// real LockInstance no-row stage without opening a second instance.
	func() {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		restore := borrowedBoundBaselineCommitApplyFence(ctx, t, b, fresh)
		defer restore()
		plan := newBorrowedBoundBaselineCommitPlan()
		plan.instanceIDOverride = borrowedBoundBaselineCommitMutantInstanceID(t, b)
		preparation := newBorrowedBoundBaselineCommitPreparation()
		stageErr := borrowedBoundBaselineCommitStageRun(ctx, t, b, fresh, 1, plan, preparation)
		if !errors.Is(stageErr, errBoundBaselineCommitInstance) || !errors.Is(stageErr, controlstore.ErrInstanceNotFound) {
			t.Fatalf("wrong-instance refusal is not the real no-row instance lock stage: %v", stageErr)
		}
		if !plan.rollbackAcknowledged {
			t.Fatal("wrong-instance refusal rollback was not acknowledged")
		}
		borrowedBoundReentryReadinessAssertBaseline(t, "bound baseline commit wrong instance", ctx, b, base)
		t.Logf("bound baseline commit wrong-instance negative: the valid-format non-existent instance UUID refused the real LockInstance no-row stage (no second instance opened) with the acknowledged rollback and complete baseline equality; no authorizing output")
	}()

	// N9: row-holder-first: an independent holder owns the guard row; the
	// supplied-tx fence receives the structured 55P03 and nothing is written.
	func() {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		hold, holdErr := borrowedReplacementGuardRowHoldRow(ctx, b.fixture.controlPool, b.fixture.guardKey)
		if holdErr != nil {
			t.Fatalf("row-holder-first control hold refused: %v", holdErr)
		}
		defer hold.release()
		restore := borrowedBoundBaselineCommitApplyFence(ctx, t, b, fresh)
		defer restore()
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
		borrowedBoundReentryReadinessAssertBaseline(t, "bound baseline commit row holder", ctx, b, base)
		t.Logf("bound baseline commit row-holder-first negative: the independent holder produced the structured 55P03 at the supplied-tx fence with the acknowledged rollback and complete baseline equality; no authorizing output")
	}()

	// N10: replacement change: the actual target catalog identity no longer
	// matches the captured replacement; the fresh catalog check refuses.
	func() {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		restore := borrowedBoundBaselineCommitApplyFence(ctx, t, b, fresh)
		defer restore()
		plan := newBorrowedBoundBaselineCommitPlan()
		plan.renameTargetBeforeStage = true
		preparation := newBorrowedBoundBaselineCommitPreparation()
		stageErr := borrowedBoundBaselineCommitStageRun(ctx, t, b, fresh, 1, plan, preparation)
		if !errors.Is(stageErr, errBoundBaselineCommitCatalog) {
			t.Fatalf("replacement-change refusal is not the real catalog stage: %v", stageErr)
		}
		borrowedBoundReentryReadinessAssertBaseline(t, "bound baseline commit replacement change", ctx, b, base)
		t.Logf("bound baseline commit replacement-change negative: the actual replacement catalog identity mismatch refused at the real catalog stage before any transaction; durable state classification: complete baseline equality, no transaction, no authorizing output")
	}()

	// N11: observer failure: a real suspended observer role refuses the
	// protected observer stage before any transaction.
	func() {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		restore := borrowedBoundBaselineCommitApplyFence(ctx, t, b, fresh)
		defer restore()
		plan := newBorrowedBoundBaselineCommitPlan()
		plan.observerNologin = true
		preparation := newBorrowedBoundBaselineCommitPreparation()
		stageErr := borrowedBoundBaselineCommitStageRun(ctx, t, b, fresh, 1, plan, preparation)
		if !errors.Is(stageErr, errBoundBaselineCommitObserver) {
			t.Fatalf("observer-failure refusal is not the real observer stage: %v", stageErr)
		}
		borrowedBoundReentryReadinessAssertBaseline(t, "bound baseline commit observer failure", ctx, b, base)
		t.Logf("bound baseline commit observer-failure negative: the suspended protected observer role refused the real observer stage before any transaction; durable state classification: complete baseline equality, no transaction, no authorizing output")
	}()

	// N12: early shared-prefix loss: the real copied-prefix loss refuses the
	// prefix stage before any transaction.
	func() {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		restore := borrowedBoundBaselineCommitApplyFence(ctx, t, b, fresh)
		defer restore()
		plan := newBorrowedBoundBaselineCommitPlan()
		plan.prefixLoss = true
		preparation := newBorrowedBoundBaselineCommitPreparation()
		stageErr := borrowedBoundBaselineCommitStageRun(ctx, t, b, fresh, 1, plan, preparation)
		if !errors.Is(stageErr, errBoundBaselineCommitPrefix) {
			t.Fatalf("early shared-prefix-loss refusal is not the real prefix stage: %v", stageErr)
		}
		if plan.rollbackAcknowledged || plan.auditExecuted || plan.cleanVerified {
			t.Fatalf("early shared-prefix loss reached a transaction/write: rollback=%t audit=%t clean=%t", plan.rollbackAcknowledged, plan.auditExecuted, plan.cleanVerified)
		}
		borrowedBoundReentryReadinessAssertBaseline(t, "bound baseline commit early prefix loss", ctx, b, base)
		t.Logf("bound baseline commit early shared-prefix-loss negative: the real copied-prefix loss refused at the real prefix stage before any transaction; durable state classification: complete baseline equality, no transaction, no authorizing output")
	}()

	// N13: failure between the two production writes: the rebuild audit
	// succeeds and the controlled resolution refuses at its real validation
	// stage; neither write survives the acknowledged rollback.
	func() {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		restore := borrowedBoundBaselineCommitApplyFence(ctx, t, b, fresh)
		defer restore()
		plan := newBorrowedBoundBaselineCommitPlan()
		plan.emptyResolveEvidence = true
		preparation := newBorrowedBoundBaselineCommitPreparation()
		stageErr := borrowedBoundBaselineCommitStageRun(ctx, t, b, fresh, 1, plan, preparation)
		if !errors.Is(stageErr, errBoundBaselineCommitClean) || !strings.Contains(stageErr.Error(), "rebuild evidence is required") {
			t.Fatalf("between-writes refusal is not the real controlled-resolution validation stage: %v", stageErr)
		}
		if !plan.auditExecuted || !plan.resolveAttempted || !plan.resolveRefused {
			t.Fatalf("between-writes facts: audit=%t resolveAttempted=%t resolveRefused=%t", plan.auditExecuted, plan.resolveAttempted, plan.resolveRefused)
		}
		if !plan.rollbackAcknowledged {
			t.Fatal("between-writes failure rollback was not acknowledged")
		}
		if plan.cleanVerified || plan.localAuditFound {
			t.Fatal("between-writes failure reached the pending resolution")
		}
		borrowedBoundReentryReadinessAssertBaseline(t, "bound baseline commit between writes", ctx, b, base)
		if count := borrowedBoundBaselineCommitPreparationRowCount(t, ctx, b, plan.rehearsalOperationID); count != 0 {
			t.Fatalf("between-writes failure left %d durable preparation rows", count)
		}
		t.Logf("bound baseline commit between-writes negative: the rebuild audit executed, the controlled resolution refused at its real validation stage and the acknowledged rollback left NEITHER write; durable state classification: complete baseline equality, zero preparation rows, no authorizing output")
	}()

	// N14: cancellation before mutation / between the writes / after the
	// pending resolution: the actual canceled context and distinct stage facts
	// are observed; no rollback acknowledgment is recorded before callback
	// entry and no pending transition survives.
	for _, cancelAt := range []string{"before-mutation", "between-writes", "after-resolution"} {
		cancelAt := cancelAt
		func() {
			b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
			_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
			base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
			laneCtx, cancelLane := context.WithCancel(ctx)
			defer cancelLane()
			restore := borrowedBoundBaselineCommitApplyFence(ctx, t, b, fresh)
			defer restore()
			plan := newBorrowedBoundBaselineCommitPlan()
			plan.cancelAt = cancelAt
			plan.cancelLane = cancelLane
			preparation := newBorrowedBoundBaselineCommitPreparation()
			stageErr := borrowedBoundBaselineCommitStageRun(laneCtx, t, b, fresh, 1, plan, preparation)
			if stageErr == nil {
				t.Fatalf("cancellation %s unexpectedly returned success", cancelAt)
			}
			if !errors.Is(stageErr, errBoundBaselineCommitCancelled) {
				t.Fatalf("cancellation %s did not return the cancellation sentinel: %v", cancelAt, stageErr)
			}
			if laneCtx.Err() == nil {
				t.Fatalf("cancellation %s did not cancel the lane context", cancelAt)
			}
			if !plan.cancelObserved || plan.reachedStage != cancelAt {
				t.Fatalf("cancellation %s reached stage %q (observed=%t)", cancelAt, plan.reachedStage, plan.cancelObserved)
			}
			if errors.Is(stageErr, errBoundBaselineCommitUncertain) || strings.Contains(stageErr.Error(), "rollback target-lock acceptance transaction") {
				t.Fatalf("cancellation %s produced a rollback uncertainty: %v", cancelAt, stageErr)
			}
			if plan.commitAcknowledged || plan.commitAckLost {
				t.Fatalf("cancellation %s was reported as a commit outcome", cancelAt)
			}
			switch cancelAt {
			case "before-mutation":
				if plan.ownerTxPID != 0 {
					t.Fatalf("before-mutation cancellation started a transaction (ownerTx=%d)", plan.ownerTxPID)
				}
				if plan.rollbackAcknowledged {
					t.Fatalf("before-mutation cancellation claimed an acknowledged rollback without a transaction")
				}
			case "between-writes":
				if plan.ownerTxPID != b.owner.BackendPID {
					t.Fatalf("between-writes cancellation ownerTx=%d, want the anchored owner %d", plan.ownerTxPID, b.owner.BackendPID)
				}
				if !plan.auditWrittenBeforeCancel {
					t.Fatalf("between-writes cancellation did not execute the audit write before the cancellation")
				}
				if plan.localAuditFound || plan.cleanVerified {
					t.Fatalf("between-writes cancellation reached the pending resolution (audit=%t clean=%t)", plan.localAuditFound, plan.cleanVerified)
				}
				if !plan.rollbackAcknowledged {
					t.Fatalf("between-writes cancellation rollback was not acknowledged")
				}
			case "after-resolution":
				if plan.ownerTxPID != b.owner.BackendPID || !plan.localAuditFound || !plan.cleanVerified {
					t.Fatalf("after-resolution cancellation did not reach the pending resolution (ownerTx=%d audit=%t clean=%t)", plan.ownerTxPID, plan.localAuditFound, plan.cleanVerified)
				}
				if !plan.rollbackAcknowledged {
					t.Fatalf("after-resolution cancellation rollback was not acknowledged")
				}
			}
			borrowedBoundReentryReadinessAssertBaseline(t, "bound baseline commit cancellation "+cancelAt, ctx, b, base)
			if count := borrowedBoundBaselineCommitPreparationRowCount(t, ctx, b, plan.rehearsalOperationID); count != 0 {
				t.Fatalf("cancellation %s left %d durable preparation rows", cancelAt, count)
			}
			if !preparation.finishedNow() {
				t.Fatalf("cancellation %s left the preparation unfinished", cancelAt)
			}
			t.Logf("bound baseline commit %s cancellation negative: the real cancellation sentinel was observed at the distinct %s stage with the actual canceled context, no rollback acknowledgment was recorded before callback entry, no pending transition survived and the one-shot preparation is finished; durable state classification: complete baseline equality, zero preparation rows, no authorizing output", cancelAt, plan.reachedStage)
		}()
	}

	// N15: pre-callback/pre-BEGIN refusal: the lane context is cancelled between
	// the initial checks and transaction entry, so the unchanged WithTransaction
	// refuses before the callback runs; NO transaction was rolled back and no
	// acknowledgment may be recorded.
	func() {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		laneCtx, cancelLane := context.WithCancel(ctx)
		defer cancelLane()
		restore := borrowedBoundBaselineCommitApplyFence(ctx, t, b, fresh)
		defer restore()
		plan := newBorrowedBoundBaselineCommitPlan()
		plan.cancelAt = "before-transaction"
		plan.cancelLane = cancelLane
		preparation := newBorrowedBoundBaselineCommitPreparation()
		stageErr := borrowedBoundBaselineCommitStageRun(laneCtx, t, b, fresh, 1, plan, preparation)
		if stageErr == nil {
			t.Fatal("pre-callback refusal control unexpectedly returned success")
		}
		if !errors.Is(stageErr, errBoundBaselineCommitPreCallback) {
			t.Fatalf("pre-callback refusal is not the real pre-callback/BEGIN stage: %v", stageErr)
		}
		if laneCtx.Err() == nil {
			t.Fatal("pre-callback refusal control did not cancel the lane context")
		}
		if plan.rollbackAcknowledged || plan.commitAcknowledged {
			t.Fatal("pre-callback refusal recorded a transaction outcome although no transaction was rolled back")
		}
		if !plan.cancelObserved || plan.reachedStage != "before-transaction" {
			t.Fatalf("pre-callback refusal reached stage %q (observed=%t)", plan.reachedStage, plan.cancelObserved)
		}
		if plan.ownerTxPID != 0 {
			t.Fatalf("pre-callback refusal entered the callback (ownerTx=%d)", plan.ownerTxPID)
		}
		borrowedBoundReentryReadinessAssertBaseline(t, "bound baseline commit pre-callback refusal", ctx, b, base)
		if count := borrowedBoundBaselineCommitPreparationRowCount(t, ctx, b, plan.rehearsalOperationID); count != 0 {
			t.Fatalf("pre-callback refusal left %d durable preparation rows", count)
		}
		if !preparation.finishedNow() {
			t.Fatal("pre-callback refusal left the preparation unfinished")
		}
		t.Logf("bound baseline commit pre-callback negative: the cancelled lane context made the unchanged transaction entry refuse before the callback ran, so no transaction was rolled back, no acknowledgment was recorded and the complete baseline stayed unchanged; no authorizing output")
	}()

	// N16: owner loss before the commit: the owner connection is terminated
	// after the pending resolution and the rechecks, so the unchanged
	// commit/held-check path reports the authentic rollback uncertainty with NO
	// commit, the complete baseline unchanged and zero surviving preparation
	// rows.
	func() {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		f := b.fixture
		_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		restore := borrowedBoundBaselineCommitApplyFence(ctx, t, b, fresh)
		defer restore()
		plan := newBorrowedBoundBaselineCommitPlan()
		plan.ownerLossBeforeCommit = true
		preparation := newBorrowedBoundBaselineCommitPreparation()
		stageErr := borrowedBoundBaselineCommitStageRun(ctx, t, b, fresh, 1, plan, preparation)
		if !errors.Is(stageErr, errBoundBaselineCommitUncertain) ||
			!strings.Contains(stageErr.Error(), "rollback target-lock acceptance transaction") {
			t.Fatalf("owner-loss control did not report the authentic rollback-acknowledgement failure: %v", stageErr)
		}
		if plan.commitAcknowledged || plan.rollbackAcknowledged {
			t.Fatal("owner-loss control reported a commit or an acknowledged rollback")
		}
		if !plan.cleanVerified || !plan.localAuditFound || !plan.liveRecheckOK {
			t.Fatal("owner-loss control did not reach the pending resolution and the pre-commit rechecks before the owner loss")
		}
		if disposition, operation := borrowedReplacementGuardRowRead(t, ctx, f.controlPool, f.guardKey); disposition == "clean" || operation != f.operation {
			t.Fatalf("owner-loss control committed the baseline transition: disposition=%q operation=%q", disposition, operation)
		}
		borrowedBoundReentryReadinessAssertBaseline(t, "bound baseline commit owner loss", ctx, b, base)
		if count := borrowedBoundBaselineCommitPreparationRowCount(t, ctx, b, plan.rehearsalOperationID); count != 0 {
			t.Fatalf("owner-loss control left %d durable preparation rows", count)
		}
		healthCtx, cancelHealth := context.WithTimeout(ctx, borrowedOwnerRotationHealthBudget)
		healthErr := f.lock.Health(healthCtx)
		cancelHealth()
		if healthErr == nil {
			t.Fatal("owner-loss control left the owner reusable")
		}
		if !preparation.finishedNow() {
			t.Fatal("owner-loss control left the preparation unfinished")
		}
		t.Logf("bound baseline commit owner-loss negative: the owner connection loss before the commit produced the authentic rollback-acknowledgement failure with NO commit, the guard stayed unresolved, the complete baseline and zero preparation rows were verified and the owner was retired with no owner reuse; durable state classification: rollback uncertainty, no authorizing output")
	}()

	// N17: late shared-prefix loss under the held transaction, immediately
	// before the production writes: the in-transaction recheck refuses and
	// NEITHER production write is reached.
	func() {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		restore := borrowedBoundBaselineCommitApplyFence(ctx, t, b, fresh)
		defer restore()
		plan := newBorrowedBoundBaselineCommitPlan()
		plan.latePrefixLoss = true
		preparation := newBorrowedBoundBaselineCommitPreparation()
		stageErr := borrowedBoundBaselineCommitStageRun(ctx, t, b, fresh, 1, plan, preparation)
		if !errors.Is(stageErr, errBoundBaselineCommitPrefix) {
			t.Fatalf("late-prefix-loss refusal is not the real in-transaction prefix stage: %v", stageErr)
		}
		if !plan.rollbackAcknowledged {
			t.Fatal("late-prefix-loss rollback was not acknowledged")
		}
		if plan.auditExecuted || plan.cleanVerified || plan.resolveAttempted {
			t.Fatalf("late-prefix-loss control reached a production write (audit=%t resolve=%t clean=%t)", plan.auditExecuted, plan.resolveAttempted, plan.cleanVerified)
		}
		borrowedBoundReentryReadinessAssertBaseline(t, "bound baseline commit late prefix loss", ctx, b, base)
		if count := borrowedBoundBaselineCommitPreparationRowCount(t, ctx, b, plan.rehearsalOperationID); count != 0 {
			t.Fatalf("late-prefix-loss control left %d durable preparation rows", count)
		}
		t.Logf("bound baseline commit late-prefix-loss negative: the shared prefix was invalidated after the initial check and the in-transaction recheck immediately before the production writes refused with the acknowledged rollback; durable state classification: complete baseline equality, NEITHER production write reached, zero preparation rows, no authorizing output")
	}()

	// N18: COMMIT acknowledgement loss through the UNCHANGED native companion:
	// the ACTUAL COMMIT is dispatched, its validated completion is withheld,
	// the committed delta is independently observed while withheld, the
	// transport is broken so the ACTUAL Commit call returns an error, and the
	// UNKNOWN acknowledgement is NEVER relabeled success. The one-shot attempt
	// is consumed and the uncertain owner session is never reused.
	func() {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		controlKey := borrowedBoundReentryDifferentKey(t, b)
		// The unchanged companion can wrap only the NEXT dedicated owner-session
		// connection establishment, so it is reserved HERE, after the genuine
		// bound fixture (whose bootstrap/retained locks must NOT consume the
		// reservation) and immediately before the dedicated owner session that
		// carries the controlled transition.
		fault, reset, err := recovery.ArmTargetLockCommitAmbiguityFault()
		if err != nil {
			t.Fatalf("baseline commit fault reservation refused: %v", err)
		}
		defer reset()
		if got := fault.WrappedCount(); got != 0 {
			t.Fatalf("baseline commit companion had already wrapped %d sockets before the dedicated acquisition", got)
		}
		acqCtx, cancelAcq := context.WithTimeout(ctx, borrowedOwnerRotationAuthBudget)
		wrappedLock, err := recovery.AcquireTargetLock(acqCtx, b.fixture.controlDSN, controlKey, 3*time.Second, 10*time.Millisecond)
		cancelAcq()
		if err != nil {
			t.Fatalf("baseline commit wrapped owner session acquisition refused: %v", err)
		}
		defer func() {
			releaseCtx, cancelRelease := context.WithTimeout(context.Background(), borrowedOwnerRotationCleanupBudget)
			_ = wrappedLock.Release(releaseCtx)
			cancelRelease()
		}()
		if got := fault.WrappedCount(); got != 1 {
			t.Fatalf("baseline commit companion wrapped %d owner sockets, want exactly 1", got)
		}
		var wrappedPID int
		var wrappedStart time.Time
		probeCtx, cancelProbe := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
		probeErr := wrappedLock.WithTransaction(probeCtx, func(txCtx context.Context, tx pgx.Tx) error {
			return tx.QueryRow(txCtx, `SELECT pid::int, backend_start FROM pg_stat_activity WHERE pid = pg_backend_pid()`).Scan(&wrappedPID, &wrappedStart)
		})
		cancelProbe()
		if probeErr != nil || wrappedPID <= 0 || wrappedPID == b.owner.BackendPID {
			t.Fatalf("baseline commit wrapped owner identity probe refused: pid=%d owner=%d err=%v", wrappedPID, b.owner.BackendPID, probeErr)
		}
		k1, k2 := controlKey.AdvisoryLockKey()
		plan := newBorrowedBoundBaselineCommitPlan()
		plan.commitAckLoss = true
		plan.lockOverride = wrappedLock
		plan.ownerPIDOverride = wrappedPID
		plan.ownerStartOverride = wrappedStart
		plan.namespaceK1Override = k1
		plan.namespaceK2Override = k2
		plan.operationIDOverride = fmt.Sprintf("bound-baseline-commit-ackloss-%d", time.Now().UnixNano())
		preparation := newBorrowedBoundBaselineCommitPreparation()
		if err := fault.ArmGate(); err != nil {
			t.Fatalf("baseline commit fault gate arm refused: %v", err)
		}
		witness, watchCleanup := startBorrowedBoundBaselineCommitFaultWatcher(ctx, t, b, fault, plan.operationIDOverride)
		defer func() {
			if cleanupErr := watchCleanup(); cleanupErr != nil {
				t.Errorf("baseline commit fault worker cleanup refused: %v", cleanupErr)
			}
		}()
		flowState, flowErr := borrowedBoundWriterIsolationRun(ctx, t, b, fresh, &borrowedBoundWriterIsolationLineage{
			consumed: true, readinessBacked: false,
			instanceID: b.fixture.instanceID, targetKey: b.fixture.guardKey,
			fingerprint:    fresh.binding.OriginalRoleFingerprint(),
			replacementOID: fresh.replacementOID, ownerPID: b.owner.BackendPID,
			issuance: &borrowedBoundWriterIsolationIssuance{},
		}, &borrowedBoundWriterIsolationHooks{
			duringProof: func(hookCtx context.Context, _ *borrowedBoundWriterIsolationFlowState) error {
				return borrowedBoundBaselineCommitStageRun(hookCtx, t, b, fresh, 1, plan, preparation)
			},
		})
		res := witness.join(t)
		if flowErr == nil || !errors.Is(flowErr, errBoundBaselineCommitAckLoss) || !strings.Contains(flowErr.Error(), "commit target-lock acceptance transaction") {
			t.Fatalf("baseline commit acknowledgement-loss control is not the real ambiguous COMMIT path: flowErr=%v worker=%+v plan=%+v", flowErr, res, plan)
		}
		if res.err != nil {
			t.Fatalf("baseline commit fault worker refused (flowErr=%v): %v", flowErr, res.err)
		}
		if !res.dispatch || !res.withheld || !res.broken {
			t.Fatalf("baseline commit fault witnesses: dispatch=%t withheld=%t broken=%t", res.dispatch, res.withheld, res.broken)
		}
		if strings.Contains(flowErr.Error(), "rollback target-lock acceptance transaction") {
			t.Fatal("baseline commit acknowledgement-loss control was reported as a rollback")
		}
		if plan.commitAcknowledged || plan.successInferred {
			t.Fatal("baseline commit acknowledgement loss was relabeled success from the error or the visible rows")
		}
		if !plan.commitAckLost {
			t.Fatal("baseline commit acknowledgement loss was not classified as acknowledgement lost")
		}
		if res.durableAuditCount != 1 || res.guardState != controlstore.TargetGuardClean || res.guardOperation != plan.operationIDOverride || !res.lockable {
			t.Fatalf("baseline commit withheld-delta observation: audit=%d state=%q operation=%q lockable=%t", res.durableAuditCount, res.guardState, res.guardOperation, res.lockable)
		}
		borrowedBoundBaselineCommitAssertPreservedDelta(t, "baseline commit acknowledgement loss", ctx, b, plan.operationIDOverride)
		if !preparation.claimedNow() || !preparation.finishedNow() {
			t.Fatal("baseline commit acknowledgement loss did not consume the one-shot attempt")
		}
		if replay := borrowedBoundBaselineCommitStageRun(ctx, t, b, fresh, 1, plan, preparation); !errors.Is(replay, errBoundBaselineCommitReplay) {
			t.Fatalf("baseline commit acknowledgement-loss replay was not refused: %v", replay)
		}
		reuseCtx, cancelReuse := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
		reuseErr := wrappedLock.WithTransaction(reuseCtx, func(context.Context, pgx.Tx) error { return nil })
		cancelReuse()
		if reuseErr == nil {
			t.Fatal("baseline commit uncertain owner session was reusable")
		}
		reuseHealthCtx, cancelReuseHealth := context.WithTimeout(ctx, borrowedOwnerRotationHealthBudget)
		reuseHealthErr := wrappedLock.Health(reuseHealthCtx)
		cancelReuseHealth()
		if reuseHealthErr == nil {
			t.Fatal("baseline commit uncertain owner session reported healthy")
		}
		origHealthCtx, cancelOrigHealth := context.WithTimeout(ctx, borrowedOwnerRotationHealthBudget)
		origHealthErr := b.fixture.lock.Health(origHealthCtx)
		cancelOrigHealth()
		if origHealthErr != nil {
			t.Fatalf("baseline commit acknowledgement loss affected the original retained owner: %v", origHealthErr)
		}
		if !flowState.cleanupRestored || flowState.cleanupErr != nil {
			t.Fatalf("baseline commit acknowledgement-loss fence disposal was not clean: restored=%t err=%v", flowState.cleanupRestored, flowState.cleanupErr)
		}
		t.Logf("baseline commit acknowledgement-loss negative: the ACTUAL COMMIT was dispatched and its validated completion withheld before pgx saw it, the committed delta (audit=%d, guard=%q/%q) was independently observed while withheld, the transport break made the ACTUAL Commit call return the ambiguous error (never an injected acknowledgement), the UNKNOWN acknowledgement was never relabeled success, the one-shot attempt was consumed, the uncertain owner session was not reusable, the original retained owner stayed healthy and the fence was disposed cleanly; durable state classification: commit-uncertain with independently observed committed delta, no authorizing output", res.durableAuditCount, res.guardState, res.guardOperation)
	}()

	// N19: acknowledged commit followed by owner loss: the committed guard and
	// audit are preserved, the owner is unusable and no positive continuation
	// is emitted; committed history is never repaired.
	func() {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		f := b.fixture
		_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		plan := newBorrowedBoundBaselineCommitPlan()
		plan.postCommitOwnerLoss = true
		preparation := newBorrowedBoundBaselineCommitPreparation()
		flowState, flowErr := borrowedBoundWriterIsolationRun(ctx, t, b, fresh, &borrowedBoundWriterIsolationLineage{
			consumed: true, readinessBacked: false,
			instanceID: f.instanceID, targetKey: f.guardKey,
			fingerprint:    fresh.binding.OriginalRoleFingerprint(),
			replacementOID: fresh.replacementOID, ownerPID: b.owner.BackendPID,
			issuance: &borrowedBoundWriterIsolationIssuance{},
		}, &borrowedBoundWriterIsolationHooks{
			duringProof: func(hookCtx context.Context, _ *borrowedBoundWriterIsolationFlowState) error {
				return borrowedBoundBaselineCommitStageRun(hookCtx, t, b, fresh, 1, plan, preparation)
			},
		})
		if !errors.Is(flowErr, errBoundBaselineCommitContinuation) {
			t.Fatalf("post-commit owner-loss control did not refuse positive continuation: %v", flowErr)
		}
		if !plan.commitAcknowledged || plan.commitAckLost || plan.rollbackAcknowledged {
			t.Fatalf("post-commit owner-loss classification: acknowledged=%t ackLost=%t rollback=%t", plan.commitAcknowledged, plan.commitAckLost, plan.rollbackAcknowledged)
		}
		if !plan.durableAuditPreparation || plan.durableAuditCount != 1 || plan.durableGuardState != controlstore.TargetGuardClean {
			t.Fatalf("post-commit owner-loss durable proof incomplete: %+v", plan)
		}
		borrowedBoundBaselineCommitAssertPreservedDelta(t, "post-commit owner loss preserved", ctx, b, plan.rehearsalOperationID)
		if count := borrowedBoundBaselineCommitPreparationRowCount(t, ctx, b, plan.rehearsalOperationID); count != 1 {
			t.Fatalf("post-commit owner-loss control changed the committed preparation rows: %d", count)
		}
		healthCtx, cancelHealth := context.WithTimeout(ctx, borrowedOwnerRotationHealthBudget)
		healthErr := f.lock.Health(healthCtx)
		cancelHealth()
		if healthErr == nil {
			t.Fatal("post-commit owner-loss control left the owner reusable")
		}
		if !flowState.cleanupRestored || flowState.cleanupErr != nil {
			t.Fatalf("post-commit owner-loss fence disposal was not clean: restored=%t err=%v", flowState.cleanupRestored, flowState.cleanupErr)
		}
		if !preparation.finishedNow() {
			t.Fatal("post-commit owner-loss control left the preparation unfinished")
		}
		t.Logf("baseline commit post-commit owner-loss negative: the acknowledged commit left the committed guard clean and exactly ONE durable preparation audit row, the owner loss refused any positive continuation (no success, no admission), the committed guard/audit were preserved and never repaired by deleting the audit or reversing the committed transition; durable state classification: committed preparation delta retained, owner unusable, no authorizing output")
	}()

	// N20: acknowledged commit followed by teardown failure: the committed
	// guard/audit are preserved, the flow cannot complete its fence disposal,
	// and no positive continuation/publication is emitted.
	func() {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		f := b.fixture
		_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		plan := newBorrowedBoundBaselineCommitPlan()
		plan.postCommitTeardown = true
		preparation := newBorrowedBoundBaselineCommitPreparation()
		flowState, flowErr := borrowedBoundWriterIsolationRun(ctx, t, b, fresh, &borrowedBoundWriterIsolationLineage{
			consumed: true, readinessBacked: false,
			instanceID: f.instanceID, targetKey: f.guardKey,
			fingerprint:    fresh.binding.OriginalRoleFingerprint(),
			replacementOID: fresh.replacementOID, ownerPID: b.owner.BackendPID,
			issuance: &borrowedBoundWriterIsolationIssuance{},
		}, &borrowedBoundWriterIsolationHooks{
			duringProof: func(hookCtx context.Context, _ *borrowedBoundWriterIsolationFlowState) error {
				return borrowedBoundBaselineCommitStageRun(hookCtx, t, b, fresh, 1, plan, preparation)
			},
		})
		if !errors.Is(flowErr, errBoundBaselineCommitTeardown) {
			t.Fatalf("post-commit teardown-failure control did not refuse positive continuation: %v", flowErr)
		}
		if !plan.commitAcknowledged || plan.commitAckLost || plan.rollbackAcknowledged {
			t.Fatalf("post-commit teardown-failure classification: acknowledged=%t ackLost=%t rollback=%t", plan.commitAcknowledged, plan.commitAckLost, plan.rollbackAcknowledged)
		}
		borrowedBoundBaselineCommitAssertPreservedDelta(t, "post-commit teardown failure preserved", ctx, b, plan.rehearsalOperationID)
		if flowState.cleanupRestored || flowState.cleanupErr == nil {
			t.Fatalf("post-commit teardown-failure control did not prove the failed teardown: restored=%t err=%v", flowState.cleanupRestored, flowState.cleanupErr)
		}
		isolation := newBorrowedBoundWriterIsolation()
		if publishErr := borrowedBoundWriterIsolationTryPublish(t, "post-commit teardown failure", b, fresh, &borrowedBoundWriterIsolationLineage{
			consumed: true, readinessBacked: false,
			instanceID: f.instanceID, targetKey: f.guardKey,
			fingerprint:    fresh.binding.OriginalRoleFingerprint(),
			replacementOID: fresh.replacementOID, ownerPID: b.owner.BackendPID,
			issuance: &borrowedBoundWriterIsolationIssuance{},
		}, flowState, isolation); publishErr == nil {
			t.Fatal("post-commit teardown failure published a positive isolation result")
		}
		if isolation.publishedNow() {
			t.Fatal("post-commit teardown failure published the isolation fact set")
		}
		healthCtx, cancelHealth := context.WithTimeout(ctx, borrowedOwnerRotationHealthBudget)
		healthErr := f.lock.Health(healthCtx)
		cancelHealth()
		if healthErr != nil {
			t.Fatalf("post-commit teardown-failure control affected the original owner: %v", healthErr)
		}
		if !preparation.finishedNow() {
			t.Fatal("post-commit teardown-failure control left the preparation unfinished")
		}
		t.Logf("baseline commit post-commit teardown-failure negative: the acknowledged commit left the committed guard/audit delta intact while the fixture teardown could not complete (restored=%t err=%v), the flow refused any positive continuation and the single-use isolation publication refused; committed history was never repaired; durable state classification: committed preparation delta retained, teardown failed, no authorizing output", flowState.cleanupRestored, flowState.cleanupErr)
	}()

	// N21: replay/copies with no reusable preparation result: a second stage run
	// (including through a copy of the preparation object) refuses and no
	// second preparation transaction or audit exists.
	func() {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		plan := newBorrowedBoundBaselineCommitPlan()
		preparation := newBorrowedBoundBaselineCommitPreparation()
		first := borrowedBoundBaselineCommitStageRun(ctx, t, b, fresh, 1, plan, preparation)
		if !errors.Is(first, errBoundBaselineCommitCoverage) {
			t.Fatalf("replay control first stage is not the real coverage refusal: %v", first)
		}
		preparationCopy := *preparation
		second := borrowedBoundBaselineCommitStageRun(ctx, t, b, fresh, 1, plan, &preparationCopy)
		if !errors.Is(second, errBoundBaselineCommitReplay) {
			t.Fatalf("preparation-copy replay was not refused by the shared latch: %v", second)
		}
		if plan.rehearsalOperationID != "" {
			if count := borrowedBoundBaselineCommitPreparationRowCount(t, ctx, b, plan.rehearsalOperationID); count != 0 {
				t.Fatalf("replay control left %d durable preparation rows", count)
			}
		}
		borrowedBoundReentryReadinessAssertBaseline(t, "bound baseline commit replay", ctx, b, base)
		postRows := borrowedBoundNativeReadyRowsNow(t, ctx, b)
		if postRows != base.rows {
			t.Fatalf("replay control changed the instance-scoped durable rows: before=%+v after=%+v", base.rows, postRows)
		}
		t.Logf("bound baseline commit replay negative: the one-shot preparation refused a second run and a copy of the preparation shared the same refusal; no second preparation transaction or audit exists and the complete baseline is unchanged; no authorizing output")
	}()
}
