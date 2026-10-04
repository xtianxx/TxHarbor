//go:build linux && drill

// borrowed-replacement-bound-baseline-rollback_linux_test.go is the bounded
// bound live-fence controlled-baseline ROLLBACK REHEARSAL lane. It composes ONLY
// existing, unchanged primitives: the genuine bound capture with the consumed
// receipt/entry, the existing bound dirty-guard refusal (whose coordinator
// preparation transaction PID is captured through the existing marker injection
// seam), the unchanged readiness machinery consumed ONCE as lineage facts, the
// strict registered-P1 retirement, and the unchanged writer-isolation flow
// whose live fence is used through its existing duringProof hook. Inside that
// hook the lane performs a fresh verification pass (effective writer-reject
// coverage, real route refusals, zero retained-writer census, protected
// observer usability, actual pristine replacement catalog) and then invokes the
// retained lock's UNCHANGED WithTransaction: the supplied transaction must be
// the ORIGINAL anchored owner session (never the coordinator preparation
// session), the authentic instance is locked/revalidated, the original guard
// row is fenced with its operation provenance, the immutable binding, shared
// prefix and actual replacement identity are rechecked, truthful
// fixture-scoped rehearsal evidence is written through the production audit
// hook, and the UNCHANGED RecordTargetGuardRebuild + ResolveTargetGuardClean
// transitions run in that same transaction with a transaction-local clean
// verification. While the transaction still holds the row, independent
// connections prove the COMMITTED guard remains unresolved, the pending audit
// is invisible and a same-row NOWAIT contender receives the structured 55P03.
// The callback then UNCONDITIONALLY returns a dedicated rollback sentinel (no
// successful return, no callback-side commit) so the unchanged WithTransaction
// performs an acknowledged rollback; afterwards the complete
// guard/instance/evidence/audit/catalog equality, independent row lockability,
// owner health and successful fence disposal are verified. A transaction-local
// clean read grants NO admission: nothing is committed, no admission handle or
// reusable preparation result exists and the guard stays deliberately
// unresolved. There is no ordinary native replacement launch, no successful
// restore probe, no restore acceptance, no manifest, no capability, no
// downstream effect, no direct guard-update SQL, no second instance, no owner
// reacquisition, no copied transaction machinery and no rebuildTargetWithWitness
// change. Secrets, verifiers, DSNs and archive contents are never logged.
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

// Bound baseline-rollback stage sentinels: every control captures and asserts
// the ACTUAL real stage instead of any non-nil error.
var (
	errBoundBaselineRollbackRehearsal   = errors.New("bound baseline rollback rehearsal: the pending transition must never commit")
	errBoundBaselineRollbackCoverage    = errors.New("bound baseline rollback rehearsal: effective fence coverage refused")
	errBoundBaselineRollbackRoute       = errors.New("bound baseline rollback rehearsal: a live route was not rejected at the intended admission stage")
	errBoundBaselineRollbackCensus      = errors.New("bound baseline rollback rehearsal: the retained-writer census refused")
	errBoundBaselineRollbackObserver    = errors.New("bound baseline rollback rehearsal: the protected observer route refused")
	errBoundBaselineRollbackCatalog     = errors.New("bound baseline rollback rehearsal: the actual replacement catalog refused")
	errBoundBaselineRollbackPrefix      = errors.New("bound baseline rollback rehearsal: the shared replacement prefix refused")
	errBoundBaselineRollbackOwnerTx     = errors.New("bound baseline rollback rehearsal: the supplied transaction is not the original anchored owner")
	errBoundBaselineRollbackInstance    = errors.New("bound baseline rollback rehearsal: the authentic instance binding refused")
	errBoundBaselineRollbackGuardRow    = errors.New("bound baseline rollback rehearsal: the original guard-row provenance refused")
	errBoundBaselineRollbackClean       = errors.New("bound baseline rollback rehearsal: the transaction-local clean verification refused")
	errBoundBaselineRollbackContender   = errors.New("bound baseline rollback rehearsal: the held-row contender proof refused")
	errBoundBaselineRollbackRollback    = errors.New("bound baseline rollback rehearsal: the rollback was not acknowledged")
	errBoundBaselineRollbackUncertain   = errors.New("bound baseline rollback rehearsal: the rollback outcome is uncertain")
	errBoundBaselineRollbackCancelled   = errors.New("bound baseline rollback rehearsal: the lane context ended")
	errBoundBaselineRollbackPreCallback = errors.New("bound baseline rollback rehearsal: the target-lock transaction refused before the callback")
	errBoundBaselineRollbackReplay      = errors.New("bound baseline rollback rehearsal: the preparation was already consumed")
)

// borrowedBoundBaselineRollbackPreparation is the copy-shared one-shot
// preparation latch: one rehearsal attempt per lineage, never a reusable
// positive result. Copies share the same state pointer.
type borrowedBoundBaselineRollbackPreparation struct {
	state *borrowedBoundBaselineRollbackPreparationState
}

type borrowedBoundBaselineRollbackPreparationState struct {
	mu       sync.Mutex
	claimed  bool
	finished bool
	nonce    int64
}

func newBorrowedBoundBaselineRollbackPreparation() *borrowedBoundBaselineRollbackPreparation {
	return &borrowedBoundBaselineRollbackPreparation{state: &borrowedBoundBaselineRollbackPreparationState{}}
}

func (p *borrowedBoundBaselineRollbackPreparation) claim() bool {
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

func (p *borrowedBoundBaselineRollbackPreparation) finish() {
	if p == nil || p.state == nil {
		return
	}
	p.state.mu.Lock()
	p.state.finished = true
	p.state.mu.Unlock()
}

func (p *borrowedBoundBaselineRollbackPreparation) finishedNow() bool {
	if p == nil || p.state == nil {
		return false
	}
	p.state.mu.Lock()
	defer p.state.mu.Unlock()
	return p.state.finished
}

func (p *borrowedBoundBaselineRollbackPreparation) claimedNow() bool {
	if p == nil || p.state == nil {
		return false
	}
	p.state.mu.Lock()
	defer p.state.mu.Unlock()
	return p.state.claimed
}

// borrowedBoundBaselineRollbackPlan is the lane-local stage plan and observed
// fact container.
type borrowedBoundBaselineRollbackPlan struct {
	// injection seams (nil/empty in the positive).
	fenceKeyOverride        string
	fenceOperationOverride  string
	expectedFingerprint     string
	prefixLoss              bool
	routeOverride           func(transport string) (string, bool)
	emptyResolveEvidence    bool
	cancelAt                string
	cancelLane              func()
	ownerLossBeforeRollback bool
	latePrefixLoss          bool
	observerNologin         bool
	renameTargetBeforeStage bool
	skipFreshChecks         bool

	mu                       sync.Mutex
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
	cleanVerified            bool
	localAuditFound          bool
	pendingGuardState        string
	pendingAuditVisible      int
	contenderCode            string
	rollbackAcknowledged     bool
	cancelObserved           bool
	reachedStage             string
	auditWrittenBeforeCancel bool
	rehearsalOperationID     string
}

func newBorrowedBoundBaselineRollbackPlan() *borrowedBoundBaselineRollbackPlan {
	return &borrowedBoundBaselineRollbackPlan{routeLines: map[string]string{}}
}

func (p *borrowedBoundBaselineRollbackPlan) observedNow() (bool, int, uint32, int, string, string, string, bool, bool, int, string, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.freshCoverage, p.census, p.catalogOID, p.ownerTxPID, p.instanceKey, p.instanceFingerprint,
		p.guardOperation, p.cleanVerified, p.localAuditFound, p.pendingAuditVisible, p.contenderCode, p.rollbackAcknowledged
}

func (p *borrowedBoundBaselineRollbackPlan) recordRoute(transport, line string) {
	p.mu.Lock()
	p.routeLines[transport] = line
	p.mu.Unlock()
}

func (p *borrowedBoundBaselineRollbackPlan) cancelStateNow() (bool, string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.cancelObserved, p.reachedStage
}

// borrowedBoundBaselineRollbackStageRun is the duringProof stage. It NEVER
// trusts the flow's cached facts: it re-reads the effective fence, re-exercises
// every supported route, re-censuses and re-checks the observer/catalog/prefix,
// then runs the controlled baseline transition through the retained lock's
// unchanged WithTransaction and unconditionally rolls it back.
func borrowedBoundBaselineRollbackStageRun(ctx context.Context, t *testing.T, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding, prepPID int, plan *borrowedBoundBaselineRollbackPlan, preparation *borrowedBoundBaselineRollbackPreparation) error {
	t.Helper()
	if plan == nil || preparation == nil {
		return errors.New("bound baseline rollback rehearsal requires the concrete plan and preparation")
	}
	if !preparation.claim() {
		return fmt.Errorf("%w: the rehearsal preparation was already consumed", errBoundBaselineRollbackReplay)
	}
	defer preparation.finish()
	f := b.fixture
	admin := f.fx.admin
	serverIP, err := f.fx.container.ContainerIP(ctx)
	if err != nil || serverIP == "" {
		return fmt.Errorf("%w: the fixture container endpoint is unknown: %v", errBoundBaselineRollbackRoute, err)
	}

	// 1. Fresh effective writer-reject coverage (never the flow's cached scopes).
	freshRules, err := borrowedBoundWriterIsolationReadRules(ctx, admin)
	if err != nil {
		return fmt.Errorf("%w: %v", errBoundBaselineRollbackCoverage, err)
	}
	freshScopes := borrowedBoundWriterIsolationScopes(freshRules)
	if len(freshScopes) == 0 {
		return fmt.Errorf("%w: no supported writer scopes were enumerated", errBoundBaselineRollbackCoverage)
	}
	if err := borrowedBoundWriterIsolationFenceStateCheck(freshRules, freshScopes, f.writerRole); err != nil {
		return fmt.Errorf("%w: %v", errBoundBaselineRollbackCoverage, err)
	}
	plan.mu.Lock()
	plan.freshCoverage = true
	plan.mu.Unlock()

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
		plan.recordRoute(transport, line)
		if !borrowedBoundWriterIsolationContains(line, "REJECT", "28000") {
			return fmt.Errorf("%w: transport %s admission line %q", errBoundBaselineRollbackRoute, transport, line)
		}
	}

	// 3. Fresh zero retained-writer census through an independent connection.
	censusConn, err := pgx.Connect(ctx, f.adminDSN)
	if err != nil {
		return fmt.Errorf("%w: independent census connection refused: %v", errBoundBaselineRollbackCensus, err)
	}
	defer func() {
		closeCtx, cancelClose := context.WithTimeout(context.Background(), borrowedOwnerRotationCleanupBudget)
		_ = censusConn.Close(closeCtx)
		cancelClose()
	}()
	census, err := borrowedBoundWriterIsolationCensus(ctx, censusConn, f.writerRole)
	if err != nil {
		return fmt.Errorf("%w: %v", errBoundBaselineRollbackCensus, err)
	}
	if census != 0 {
		return fmt.Errorf("%w: %d retained writer sessions", errBoundBaselineRollbackCensus, census)
	}
	plan.mu.Lock()
	plan.census = census
	plan.mu.Unlock()

	// 4. Fresh protected observer usability.
	if plan.observerNologin {
		nologinCtx, cancelNologin := context.WithTimeout(ctx, borrowedOwnerRotationAuthBudget)
		_, nologinErr := admin.Exec(nologinCtx, `ALTER ROLE `+pgx.Identifier{f.observerRole}.Sanitize()+` NOLOGIN`)
		cancelNologin()
		if nologinErr != nil {
			return fmt.Errorf("%w: observer suspension refused: %v", errBoundBaselineRollbackObserver, nologinErr)
		}
		defer func() {
			loginCtx, cancelLogin := context.WithTimeout(context.Background(), borrowedOwnerRotationAuthBudget)
			_, _ = admin.Exec(loginCtx, `ALTER ROLE `+pgx.Identifier{f.observerRole}.Sanitize()+` LOGIN`)
			cancelLogin()
		}()
	}
	if err := borrowedBoundWriterIsolationObserverOK(ctx, f); err != nil {
		return fmt.Errorf("%w: %v", errBoundBaselineRollbackObserver, err)
	}
	plan.mu.Lock()
	plan.observerOK = true
	plan.mu.Unlock()

	// 5. Fresh actual pristine replacement catalog identity.
	if plan.renameTargetBeforeStage {
		changed := f.targetDB + "_rehearsal_changed"
		termCtx, cancelTerm := context.WithTimeout(ctx, borrowedOwnerRotationAuthBudget)
		_, _ = admin.Exec(termCtx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=$1 AND pid <> pg_backend_pid()`, f.targetDB)
		renameCtx, cancelRename := context.WithTimeout(ctx, borrowedOwnerRotationAuthBudget)
		_, renameErr := admin.Exec(renameCtx, `ALTER DATABASE `+pgx.Identifier{f.targetDB}.Sanitize()+` RENAME TO `+pgx.Identifier{changed}.Sanitize())
		cancelRename()
		cancelTerm()
		if renameErr != nil {
			return fmt.Errorf("%w: replacement rename refused: %v", errBoundBaselineRollbackCatalog, renameErr)
		}
		defer func() {
			backCtx, cancelBack := context.WithTimeout(context.Background(), borrowedOwnerRotationAuthBudget)
			_, _ = admin.Exec(backCtx, `ALTER DATABASE `+pgx.Identifier{changed}.Sanitize()+` RENAME TO `+pgx.Identifier{f.targetDB}.Sanitize())
			cancelBack()
		}()
	}
	catalogOID, err := borrowedOwnerDDLTargetOID(ctx, b)
	if err != nil || catalogOID != fresh.replacementOID {
		return fmt.Errorf("%w: actual catalog oid=%d err=%v", errBoundBaselineRollbackCatalog, catalogOID, err)
	}
	plan.mu.Lock()
	plan.catalogOID = catalogOID
	plan.mu.Unlock()

	// 6. Fresh shared-prefix validity.
	if plan.prefixLoss {
		borrowedReplacementSessionCopiedPrefixLoss(t, fresh)
	}
	if invalid, reason := fresh.prefix.Invalid(); invalid {
		return fmt.Errorf("%w: the shared replacement prefix is permanently invalidated: %s", errBoundBaselineRollbackPrefix, reason)
	}

	if plan.cancelAt == "before-mutation" {
		plan.mu.Lock()
		plan.cancelObserved = true
		plan.reachedStage = "before-mutation"
		plan.mu.Unlock()
		if plan.cancelLane != nil {
			plan.cancelLane()
		}
		return fmt.Errorf("%w: cancelled before the controlled transition", errBoundBaselineRollbackCancelled)
	}

	// 7. The controlled baseline transition through the retained lock's
	// UNCHANGED WithTransaction, unconditionally rolled back afterwards.
	rehearsalOperationID := fmt.Sprintf("bound-baseline-rollback-%d", time.Now().UnixNano())
	plan.mu.Lock()
	plan.rehearsalOperationID = rehearsalOperationID
	plan.mu.Unlock()
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
	evidenceBytes, err := json.Marshal(map[string]any{
		"scope":              "bound_fixture_rollback_rehearsal",
		"instance_id":        f.instanceID,
		"target_guard_key":   f.guardKey,
		"role_fingerprint":   expectedFingerprint,
		"replacement_oid":    fresh.replacementOID,
		"owner_pid":          b.owner.BackendPID,
		"preparation_tx_pid": prepPID,
		"routes":             []string{"unix", "loopback", "serverip"},
		"operation_id":       rehearsalOperationID,
	})
	if err != nil {
		return fmt.Errorf("%w: rehearsal evidence construction refused: %v", errBoundBaselineRollbackRehearsal, err)
	}
	if plan.cancelAt == "before-transaction" {
		plan.mu.Lock()
		plan.cancelObserved = true
		plan.reachedStage = "before-transaction"
		plan.mu.Unlock()
		if plan.cancelLane != nil {
			plan.cancelLane()
		}
	}
	callbackRan := false
	var callbackErr error
	callbackBody := func(txCtx context.Context, tx pgx.Tx) error {
		var (
			txPID     int
			txStart   time.Time
			namespace bool
		)
		identCtx, cancelIdent := context.WithTimeout(txCtx, borrowedOwnerRotationQueryBudget)
		identErr := tx.QueryRow(identCtx, `SELECT pid::int, backend_start FROM pg_stat_activity WHERE pid = pg_backend_pid()`).Scan(&txPID, &txStart)
		cancelIdent()
		if identErr != nil {
			return fmt.Errorf("%w: supplied-tx identity read refused", errBoundBaselineRollbackOwnerTx)
		}
		nsCtx, cancelNs := context.WithTimeout(txCtx, borrowedOwnerRotationQueryBudget)
		nsErr := tx.QueryRow(nsCtx, `
SELECT EXISTS (
  SELECT 1 FROM pg_locks
  WHERE locktype = 'advisory' AND granted AND pid = pg_backend_pid() AND objsubid = 2
    AND classid = $1::oid AND objid = $2::oid AND database = $3::oid
)`, b.owner.NamespaceClassID, b.owner.NamespaceObjectID, b.owner.NamespaceDatabaseID).Scan(&namespace)
		cancelNs()
		if nsErr != nil || !namespace {
			return fmt.Errorf("%w: original advisory namespace not held", errBoundBaselineRollbackOwnerTx)
		}
		if txPID != b.owner.BackendPID || !txStart.Equal(b.owner.BackendStart) {
			return fmt.Errorf("%w: supplied-tx pid %d is not the original anchored owner %d", errBoundBaselineRollbackOwnerTx, txPID, b.owner.BackendPID)
		}
		if prepPID > 0 && txPID == prepPID {
			return fmt.Errorf("%w: supplied tx reused the coordinator preparation session", errBoundBaselineRollbackOwnerTx)
		}
		plan.mu.Lock()
		plan.ownerTxPID = txPID
		plan.mu.Unlock()

		instanceToken, err := controlstore.LockInstance(txCtx, tx, f.instanceID)
		if err != nil {
			return fmt.Errorf("%w: instance lock refused: %v", errBoundBaselineRollbackInstance, err)
		}
		if instanceToken.State != "open" || instanceToken.TargetGuardKey != f.guardKey || instanceToken.TargetRoleFingerprint != expectedFingerprint {
			return fmt.Errorf("%w: instance state=%q key=%q fingerprint=%q", errBoundBaselineRollbackInstance, instanceToken.State, instanceToken.TargetGuardKey, instanceToken.TargetRoleFingerprint)
		}
		plan.mu.Lock()
		plan.instanceState = instanceToken.State
		plan.instanceKey = instanceToken.TargetGuardKey
		plan.instanceFingerprint = instanceToken.TargetRoleFingerprint
		plan.mu.Unlock()

		disposition, operation, fenceErr := borrowedReplacementGuardRowFence(txCtx, tx, guardKey, guardOperation)
		if fenceErr != nil {
			return fenceErr
		}
		plan.mu.Lock()
		plan.guardDisposition = disposition
		plan.guardOperation = operation
		plan.mu.Unlock()

		var actualOID uint32
		oidCtx, cancelOID := context.WithTimeout(txCtx, borrowedOwnerRotationQueryBudget)
		oidErr := tx.QueryRow(oidCtx, `SELECT oid::oid FROM pg_database WHERE datname=$1`, f.targetDB).Scan(&actualOID)
		cancelOID()
		if oidErr != nil || actualOID != fresh.replacementOID {
			return fmt.Errorf("%w: in-transaction replacement oid=%d err=%v", errBoundBaselineRollbackCatalog, actualOID, oidErr)
		}

		if plan.latePrefixLoss {
			borrowedReplacementSessionCopiedPrefixLoss(t, fresh)
		}
		if invalid, reason := fresh.prefix.Invalid(); invalid {
			return fmt.Errorf("%w: the shared replacement prefix was invalidated under the held transaction: %s", errBoundBaselineRollbackPrefix, reason)
		}
		if err := controlstore.RecordTargetGuardRebuild(txCtx, tx, f.instanceID, f.guardKey, "deploy:bound-baseline-rollback", rehearsalOperationID, evidenceBytes); err != nil {
			return fmt.Errorf("%w: rebuild audit refused: %v", errBoundBaselineRollbackClean, err)
		}
		if plan.cancelAt == "between-writes" {
			localCtx, cancelLocal := context.WithTimeout(txCtx, borrowedOwnerRotationQueryBudget)
			var written int
			writeErr := tx.QueryRow(localCtx, `SELECT count(*)::int FROM recovery_audit WHERE action=$1 AND operation_id=$2`,
				controlstore.ActionTargetGuardRebuild, rehearsalOperationID).Scan(&written)
			cancelLocal()
			plan.mu.Lock()
			plan.auditWrittenBeforeCancel = writeErr == nil && written == 1
			plan.cancelObserved = true
			plan.reachedStage = "between-writes"
			plan.mu.Unlock()
			if plan.cancelLane != nil {
				plan.cancelLane()
			}
			return fmt.Errorf("%w: cancelled between the audit and the resolution", errBoundBaselineRollbackCancelled)
		}
		resolveEvidence := evidenceBytes
		if plan.emptyResolveEvidence {
			resolveEvidence = []byte{}
		}
		if err := controlstore.ResolveTargetGuardClean(txCtx, tx, f.guardKey, rehearsalOperationID, resolveEvidence); err != nil {
			return fmt.Errorf("%w: controlled resolution refused: %v", errBoundBaselineRollbackClean, err)
		}
		if err := controlstore.RequireCleanTargetGuard(txCtx, tx, f.instanceID, f.guardKey, expectedFingerprint); err != nil {
			return fmt.Errorf("%w: transaction-local clean verification refused: %v", errBoundBaselineRollbackClean, err)
		}
		resolved, found, readErr := controlstore.ReadTargetGuard(txCtx, tx, f.guardKey)
		if readErr != nil || !found || resolved.State != controlstore.TargetGuardClean {
			return fmt.Errorf("%w: transaction-local resolved guard read refused (err=%v found=%t)", errBoundBaselineRollbackClean, readErr, found)
		}
		plan.mu.Lock()
		plan.cleanVerified = true
		plan.mu.Unlock()
		localAuditCtx, cancelLocalAudit := context.WithTimeout(txCtx, borrowedOwnerRotationQueryBudget)
		var localAudit int
		localAuditErr := tx.QueryRow(localAuditCtx, `SELECT count(*)::int FROM recovery_audit WHERE action=$1 AND operation_id=$2`,
			controlstore.ActionTargetGuardRebuild, rehearsalOperationID).Scan(&localAudit)
		cancelLocalAudit()
		if localAuditErr != nil || localAudit != 1 {
			return fmt.Errorf("%w: transaction-local rebuild audit count=%d err=%v", errBoundBaselineRollbackClean, localAudit, localAuditErr)
		}
		plan.mu.Lock()
		plan.localAuditFound = true
		plan.mu.Unlock()

		// Independent proofs while the supplied transaction still holds the row.
		pendingDisposition, pendingOperation := borrowedReplacementGuardRowRead(t, txCtx, f.controlPool, f.guardKey)
		if pendingDisposition == "clean" {
			return fmt.Errorf("%w: the committed guard was already clean", errBoundBaselineRollbackContender)
		}
		if pendingOperation != f.operation {
			return fmt.Errorf("%w: the committed guard operation provenance changed", errBoundBaselineRollbackContender)
		}
		pendingAuditCtx, cancelPendingAudit := context.WithTimeout(txCtx, borrowedOwnerRotationQueryBudget)
		var pendingAudit int
		pendingAuditErr := f.controlPool.QueryRow(pendingAuditCtx, `SELECT count(*)::int FROM recovery_audit WHERE action=$1 AND operation_id=$2`,
			controlstore.ActionTargetGuardRebuild, rehearsalOperationID).Scan(&pendingAudit)
		cancelPendingAudit()
		if pendingAuditErr != nil || pendingAudit != 0 {
			return fmt.Errorf("%w: the pending rebuild audit was visible before COMMIT (visible=%d err=%v)", errBoundBaselineRollbackContender, pendingAudit, pendingAuditErr)
		}
		if _, _, contenderErr := borrowedReplacementGuardRowContenderLock(txCtx, f.controlPool, f.guardKey); contenderErr == nil {
			return fmt.Errorf("%w: the contender locked the held row", errBoundBaselineRollbackContender)
		} else if code := borrowedReplacementGuardRowCode(contenderErr); code != "55P03" {
			return fmt.Errorf("%w: contender refusal was %q, not the structured 55P03", errBoundBaselineRollbackContender, code)
		} else {
			plan.mu.Lock()
			plan.pendingGuardState = pendingDisposition
			plan.pendingAuditVisible = pendingAudit
			plan.contenderCode = code
			plan.mu.Unlock()
		}
		if plan.cancelAt == "after-resolution" {
			// The cancellation is observed after the pending resolution; the
			// callback still must NOT commit.
			plan.mu.Lock()
			plan.cancelObserved = true
			plan.reachedStage = "after-resolution"
			plan.mu.Unlock()
			if plan.cancelLane != nil {
				plan.cancelLane()
			}
			return fmt.Errorf("%w: cancelled after the pending resolution", errBoundBaselineRollbackCancelled)
		}
		if plan.ownerLossBeforeRollback {
			// Test-only injection: terminate the owner connection so the
			// unchanged rollback path reports the authentic acknowledgement
			// failure and retirement semantics. NOTHING is committed.
			lossCtx, cancelLoss := context.WithTimeout(context.Background(), borrowedOwnerRotationQueryBudget)
			var terminated bool
			lossErr := f.controlPool.QueryRow(lossCtx, `SELECT pg_terminate_backend($1)`, b.owner.BackendPID).Scan(&terminated)
			cancelLoss()
			if lossErr != nil || !terminated {
				return fmt.Errorf("%w: owner termination refused: %v", errBoundBaselineRollbackUncertain, lossErr)
			}
		}
		// UNCONDITIONAL dedicated rollback sentinel: no successful return and no
		// callback-side transaction finalization.
		return errBoundBaselineRollbackRehearsal
	}
	txErr := f.lock.WithTransaction(ctx, func(txCtx context.Context, tx pgx.Tx) error {
		callbackRan = true
		callbackErr = callbackBody(txCtx, tx)
		return callbackErr
	})
	if txErr == nil {
		return fmt.Errorf("%w: the controlled transition unexpectedly returned success", errBoundBaselineRollbackRehearsal)
	}
	if strings.Contains(txErr.Error(), "rollback target-lock acceptance transaction") {
		return fmt.Errorf("%w: %v", errBoundBaselineRollbackUncertain, txErr)
	}
	if !callbackRan || callbackErr == nil {
		// A refusal before the callback (BEGIN or the held-lock check) rolled
		// back NO transaction: no acknowledgment may be recorded.
		return fmt.Errorf("%w: the refusal preceded the callback (ran=%t): %v", errBoundBaselineRollbackPreCallback, callbackRan, txErr)
	}
	plan.mu.Lock()
	plan.rollbackAcknowledged = true
	plan.mu.Unlock()
	return txErr
}

// borrowedBoundBaselineRollbackApplyFence applies the full writer-reject fence
// for every enumerated scope on an independent fixture and returns an
// idempotent disposal that restores the original HBA and reloads.
func borrowedBoundBaselineRollbackApplyFence(ctx context.Context, t *testing.T, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding) func() {
	t.Helper()
	f := b.fixture
	admin := f.fx.admin
	hbaPath, err := borrowedBoundWriterIsolationHBAPath(ctx, admin)
	if err != nil {
		t.Fatalf("baseline rollback fence path refused: %v", err)
	}
	original, err := borrowedBoundWriterIsolationReadHBA(ctx, f, hbaPath)
	if err != nil {
		t.Fatalf("baseline rollback fence read refused: %v", err)
	}
	rules, err := borrowedBoundWriterIsolationReadRules(ctx, admin)
	if err != nil {
		t.Fatalf("baseline rollback fence rules refused: %v", err)
	}
	scopes := borrowedBoundWriterIsolationScopes(rules)
	if len(scopes) == 0 {
		t.Fatal("baseline rollback fence requires enumerated writer scopes")
	}
	fenced := borrowedBoundWriterIsolationFencedContent(original, scopes, f.writerRole)
	if err := borrowedBoundWriterIsolationWriteHBA(ctx, t, f, hbaPath, fenced); err != nil {
		t.Fatalf("baseline rollback fence write refused: %v", err)
	}
	if err := borrowedBoundWriterIsolationReload(ctx, admin); err != nil {
		t.Fatalf("baseline rollback fence reload refused: %v", err)
	}
	serverIP, err := f.fx.container.ContainerIP(ctx)
	if err != nil {
		t.Fatalf("baseline rollback fence endpoint refused: %v", err)
	}
	if err := borrowedBoundWriterIsolationAwaitState(t, ctx, f, serverIP, f.writerRole, b.passwordP1, "REJECT", "28000", 10*time.Second); err != nil {
		t.Fatalf("baseline rollback fence did not become effective: %v", err)
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

// TestBorrowedReplacementBoundBaselineRollback is the bounded live-fence
// controlled-baseline rollback rehearsal lane described in the file header.
func TestBorrowedReplacementBoundBaselineRollback(t *testing.T) {
	ctx := t.Context()

	// P: the single chain over ONE authentic bound fixture.
	func() {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		f := b.fixture
		borrowedBoundSessionAssertProvenance(t, ctx, b, fresh, "bound baseline rollback")
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)

		// Existing bound dirty-guard refusal; capture the coordinator
		// preparation transaction PID through the existing marker seam.
		var probeCalls, acceptanceCalls int32
		prepAttemptID := fmt.Sprintf("bound-baseline-rollback-refusal-%d", time.Now().UnixNano())
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
			t.Fatalf("bound baseline rollback refusal origin registration refused: %v", err)
		}
		result, receipt, runErr := dispatchBorrowedBoundNativeReady(t, ctx, attempt)
		if runErr == nil || runErr.Error() != "bound target guard is not clean" {
			t.Fatalf("bound baseline rollback refusal is not the exact production bound dirty-guard refusal: %v", runErr)
		}
		borrowedBoundNativeReadyRequireNoProbeOutput(t, "bound baseline rollback refusal", result)
		borrowedBoundNativeReadyRequireAbsentReceipt(t, "bound baseline rollback refusal", receipt)
		borrowedBoundReentryAssertOutcomeBoundaries(t, "bound baseline rollback refusal", attempt, result, receipt, &probeCalls, &acceptanceCalls)
		if prepTxPID <= 0 || prepTxPID == b.owner.BackendPID {
			t.Fatalf("bound baseline rollback coordinator prep tx pid=%d owner=%d", prepTxPID, b.owner.BackendPID)
		}

		// Readiness consumed once as lineage facts, then strict retirement.
		stage := &borrowedBoundReentryReadinessStage{
			key: f.guardKey, expectedOperation: f.operation,
			journalAttemptID: prepAttemptID, journalFacts: &borrowedReplacementOwnerJournalFacts{},
		}
		outcome := borrowedBoundReentryReadinessRun(ctx, t, b, fresh, stage, nil)
		readiness := newBorrowedBoundReentryReadiness()
		if err := borrowedBoundReentryReadinessTryPublish(t, "bound baseline rollback readiness", b, fresh, stage, outcome, readiness); err != nil {
			t.Fatalf("bound baseline rollback readiness refused: %v", err)
		}
		if _, ok := readiness.consume(); !ok {
			t.Fatal("bound baseline rollback readiness witness was not consumable once")
		}
		if err := borrowedReplacementOwnerTailRetireGuarded(t, ctx, b, outcome); err != nil {
			t.Fatalf("bound baseline rollback registered-P1 retirement refused: %v", err)
		}

		// Full durable baseline AFTER the prerequisite journal writes and before
		// this stage.
		base = borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)

		// The unchanged writer-isolation flow with the rehearsal stage in its
		// existing duringProof hook.
		plan := newBorrowedBoundBaselineRollbackPlan()
		preparation := newBorrowedBoundBaselineRollbackPreparation()
		flowState, flowErr := borrowedBoundWriterIsolationRun(ctx, t, b, fresh, &borrowedBoundWriterIsolationLineage{
			consumed: true, readinessBacked: false,
			instanceID: f.instanceID, targetKey: f.guardKey,
			fingerprint:    fresh.binding.OriginalRoleFingerprint(),
			replacementOID: fresh.replacementOID, ownerPID: b.owner.BackendPID,
			issuance: &borrowedBoundWriterIsolationIssuance{},
		}, &borrowedBoundWriterIsolationHooks{
			duringProof: func(hookCtx context.Context, _ *borrowedBoundWriterIsolationFlowState) error {
				return borrowedBoundBaselineRollbackStageRun(hookCtx, t, b, fresh, prepTxPID, plan, preparation)
			},
		})
		if !errors.Is(flowErr, errBoundBaselineRollbackRehearsal) {
			t.Fatalf("bound baseline rollback positive did not return the dedicated rollback sentinel: %v", flowErr)
		}
		if plan.ownerTxPID != b.owner.BackendPID || plan.ownerTxPID == prepTxPID {
			t.Fatalf("bound baseline rollback owner transaction not distinguished: ownerTx=%d owner=%d prep=%d", plan.ownerTxPID, b.owner.BackendPID, prepTxPID)
		}
		freshCoverage, census, catalogOID, ownerTxPID, instanceKey, instanceFingerprint, guardOperation, cleanVerified, localAudit, pendingAudit, contenderCode, rollbackAck := plan.observedNow()
		if !freshCoverage || census != 0 || catalogOID != fresh.replacementOID || ownerTxPID != b.owner.BackendPID ||
			instanceKey != f.guardKey || instanceFingerprint != fresh.binding.OriginalRoleFingerprint() ||
			guardOperation != f.operation || !cleanVerified || !localAudit || pendingAudit != 0 || contenderCode != "55P03" || !rollbackAck {
			t.Fatalf("bound baseline rollback observed stage facts incomplete: coverage=%t census=%d catalog=%d ownerTx=%d key=%q fp=%q op=%q clean=%t localAudit=%t pendingAudit=%d contender=%q rollbackAck=%t",
				freshCoverage, census, catalogOID, ownerTxPID, instanceKey, instanceFingerprint, guardOperation, cleanVerified, localAudit, pendingAudit, contenderCode, rollbackAck)
		}
		if !preparation.claimedNow() || !preparation.finishedNow() {
			t.Fatal("bound baseline rollback preparation was not claimed and finished by the stage")
		}
		if !flowState.cleanupRestored || flowState.cleanupErr != nil {
			t.Fatalf("bound baseline rollback fence disposal was not proven clean: restored=%t err=%v", flowState.cleanupRestored, flowState.cleanupErr)
		}

		// Complete durable equality: no pending transition and no rehearsal row
		// survived; the guard is unresolved and independently lockable.
		borrowedBoundReentryReadinessAssertBaseline(t, "bound baseline rollback positive", ctx, b, base)
		if count := borrowedReplacementOwnerJournalCount(t, ctx, f.controlPool, plan.rehearsalOperationID); count != 0 {
			t.Fatalf("bound baseline rollback left %d durable rehearsal audit rows", count)
		}
		if released, releasedOperation, releasedErr := borrowedReplacementGuardRowContenderLock(ctx, f.controlPool, f.guardKey); releasedErr != nil || released != base.disposition || releasedOperation != base.operation {
			t.Fatalf("bound baseline rollback did not leave the row independently lockable/unchanged: %q/%q err=%v", released, releasedOperation, releasedErr)
		}
		healthCtx, cancelHealth := context.WithTimeout(ctx, borrowedOwnerRotationHealthBudget)
		healthErr := f.lock.Health(healthCtx)
		cancelHealth()
		if healthErr != nil {
			t.Fatalf("bound baseline rollback owner health refused: %v", healthErr)
		}
		serverIP, ipErr := f.fx.container.ContainerIP(ctx)
		if ipErr != nil {
			t.Fatalf("bound baseline rollback endpoint refused: %v", ipErr)
		}
		if err := borrowedBoundWriterIsolationAwaitState(t, ctx, f, serverIP, f.writerRole, b.passwordP1, "AUTH_OK", "", 10*time.Second); err != nil {
			t.Fatalf("bound baseline rollback fence disposal did not restore the HBA: %v", err)
		}
		restoredP0 := runHelperAuthCheck(t, ctx, f.fx.containerID, "serverip", serverIP, f.writerRole, f.writerPassword)
		if !borrowedBoundWriterIsolationContains(restoredP0, "REJECT", "28P01") {
			t.Fatalf("bound baseline rollback fence disposal rehabilitated P0: %s", restoredP0)
		}
		borrowedOwnerDDLGuard(t, ctx, b)
		t.Logf("bound baseline rollback positive: live fence freshly verified; owner tx pid %d distinct from prep tx pid %d; authentic instance and original guard provenance locked; production rebuild audit + controlled resolution ran in-transaction with a successful clean read and pending-audit invisibility + 55P03 contender proof; the unconditional rollback sentinel produced an acknowledged rollback with complete baseline equality, zero surviving rehearsal rows, independent lockability, owner health and clean fence disposal", plan.ownerTxPID, prepTxPID)
	}()

	// N1: historical/disposed fence: with no writer-reject rules the fresh
	// coverage verification refuses before any transaction.
	func() {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		plan := newBorrowedBoundBaselineRollbackPlan()
		preparation := newBorrowedBoundBaselineRollbackPreparation()
		stageErr := borrowedBoundBaselineRollbackStageRun(ctx, t, b, fresh, 1, plan, preparation)
		if !errors.Is(stageErr, errBoundBaselineRollbackCoverage) {
			t.Fatalf("historical-fence refusal is not the real coverage stage: %v", stageErr)
		}
		if !preparation.claimedNow() || !preparation.finishedNow() {
			t.Fatal("historical-fence control did not consume its one-shot preparation")
		}
		if replay := borrowedBoundBaselineRollbackStageRun(ctx, t, b, fresh, 1, plan, preparation); !errors.Is(replay, errBoundBaselineRollbackReplay) {
			t.Fatalf("historical-fence replay was not refused by the preparation latch: %v", replay)
		}
		borrowedBoundReentryReadinessAssertBaseline(t, "bound baseline rollback historical fence", ctx, b, base)
		t.Logf("bound baseline rollback historical/disposed-fence negative: the fresh effective-coverage verification refused before any transaction, the one-shot preparation was consumed and its replay refused")
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
		plan := newBorrowedBoundBaselineRollbackPlan()
		preparation := newBorrowedBoundBaselineRollbackPreparation()
		stageErr := borrowedBoundBaselineRollbackStageRun(ctx, t, b, fresh, 1, plan, preparation)
		if !errors.Is(stageErr, errBoundBaselineRollbackCoverage) {
			t.Fatalf("incomplete-coverage refusal is not the real coverage stage: %v", stageErr)
		}
		borrowedBoundReentryReadinessAssertBaseline(t, "bound baseline rollback incomplete coverage", ctx, b, base)
		t.Logf("bound baseline rollback incomplete-coverage negative: the partial fence left the container-address route admitting a genuine P1 and the fresh coverage verification refused")
	}()

	// N3: failed route refusal via the route override seam: the effective
	// coverage is complete but one transport still admits; the route stage
	// refuses before any transaction.
	func() {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		restore := borrowedBoundBaselineRollbackApplyFence(ctx, t, b, fresh)
		defer restore()
		plan := newBorrowedBoundBaselineRollbackPlan()
		plan.routeOverride = func(transport string) (string, bool) {
			if transport == "loopback" {
				return "AUTHCHECK state=AUTH_OK", true
			}
			return "", false
		}
		preparation := newBorrowedBoundBaselineRollbackPreparation()
		stageErr := borrowedBoundBaselineRollbackStageRun(ctx, t, b, fresh, 1, plan, preparation)
		if !errors.Is(stageErr, errBoundBaselineRollbackRoute) {
			t.Fatalf("failed-route refusal is not the real route stage: %v", stageErr)
		}
		borrowedBoundReentryReadinessAssertBaseline(t, "bound baseline rollback failed route", ctx, b, base)
		t.Logf("bound baseline rollback failed-route negative: complete effective coverage with one transport admitting P1 refused at the real route stage before any transaction")
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
			t.Fatalf("retained-writer rehearsal P1 connect refused: %v", retainErr)
		}
		defer borrowedSuccessorCloseConn(t, retained)
		restore := borrowedBoundBaselineRollbackApplyFence(ctx, t, b, fresh)
		defer restore()
		plan := newBorrowedBoundBaselineRollbackPlan()
		preparation := newBorrowedBoundBaselineRollbackPreparation()
		stageErr := borrowedBoundBaselineRollbackStageRun(ctx, t, b, fresh, 1, plan, preparation)
		if !errors.Is(stageErr, errBoundBaselineRollbackCensus) {
			t.Fatalf("retained-writer refusal is not the real census stage: %v", stageErr)
		}
		borrowedBoundReentryReadinessAssertBaseline(t, "bound baseline rollback retained writer", ctx, b, base)
		t.Logf("bound baseline rollback retained-writer negative: the pre-existing genuine P1 session made the fresh census refuse before any transaction")
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
			t.Fatalf("wrong-key rehearsal control key already has %d guard rows", count)
		}
		restore := borrowedBoundBaselineRollbackApplyFence(ctx, t, b, fresh)
		defer restore()
		plan := newBorrowedBoundBaselineRollbackPlan()
		plan.fenceKeyOverride = diffKey.String()
		preparation := newBorrowedBoundBaselineRollbackPreparation()
		stageErr := borrowedBoundBaselineRollbackStageRun(ctx, t, b, fresh, 1, plan, preparation)
		if !errors.Is(stageErr, errGuardRowWindowRowRefused) || !strings.Contains(stageErr.Error(), "no guard row exists") {
			t.Fatalf("wrong-key refusal is not the real no-row fence stage: %v", stageErr)
		}
		borrowedBoundReentryReadinessAssertBaseline(t, "bound baseline rollback wrong key", ctx, b, base)
		t.Logf("bound baseline rollback wrong-key/missing-row negative: the real different canonical key was proven absent and the supplied-tx fence refused at the real no-row stage")
	}()

	// N6: wrong REAL operation provenance on the real row: the supplied-tx
	// fence refuses at the operation-provenance stage.
	func() {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		restore := borrowedBoundBaselineRollbackApplyFence(ctx, t, b, fresh)
		defer restore()
		plan := newBorrowedBoundBaselineRollbackPlan()
		plan.fenceOperationOverride = b.fixture.operation + "-not-the-original"
		preparation := newBorrowedBoundBaselineRollbackPreparation()
		stageErr := borrowedBoundBaselineRollbackStageRun(ctx, t, b, fresh, 1, plan, preparation)
		if !errors.Is(stageErr, errGuardRowWindowRowRefused) || !strings.Contains(stageErr.Error(), "operation provenance") {
			t.Fatalf("wrong-operation refusal is not the real operation-provenance stage: %v", stageErr)
		}
		borrowedBoundReentryReadinessAssertBaseline(t, "bound baseline rollback wrong operation", ctx, b, base)
		t.Logf("bound baseline rollback wrong-operation negative: the real mismatched operation provenance refused the supplied-tx fence at the real operation-provenance stage")
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
		restore := borrowedBoundBaselineRollbackApplyFence(ctx, t, b, fresh)
		defer restore()
		plan := newBorrowedBoundBaselineRollbackPlan()
		plan.expectedFingerprint = wrongFingerprint
		preparation := newBorrowedBoundBaselineRollbackPreparation()
		stageErr := borrowedBoundBaselineRollbackStageRun(ctx, t, b, fresh, 1, plan, preparation)
		if !errors.Is(stageErr, errBoundBaselineRollbackInstance) {
			t.Fatalf("wrong-role refusal is not the real instance-binding stage: %v", stageErr)
		}
		borrowedBoundReentryReadinessAssertBaseline(t, "bound baseline rollback wrong role", ctx, b, base)
		t.Logf("bound baseline rollback wrong-role negative: the observer role's real fingerprint refused the authentic instance binding at the real stage")
	}()

	// N8: row-holder-first: an independent holder owns the guard row; the
	// supplied-tx fence receives the structured 55P03 and nothing is written.
	func() {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		hold, holdErr := borrowedReplacementGuardRowHoldRow(ctx, b.fixture.controlPool, b.fixture.guardKey)
		if holdErr != nil {
			t.Fatalf("row-holder-first rehearsal hold refused: %v", holdErr)
		}
		defer hold.release()
		restore := borrowedBoundBaselineRollbackApplyFence(ctx, t, b, fresh)
		defer restore()
		plan := newBorrowedBoundBaselineRollbackPlan()
		preparation := newBorrowedBoundBaselineRollbackPreparation()
		stageErr := borrowedBoundBaselineRollbackStageRun(ctx, t, b, fresh, 1, plan, preparation)
		hold.release()
		if !errors.Is(stageErr, errGuardRowWindowRowRefused) || borrowedReplacementGuardRowCode(stageErr) != "55P03" {
			t.Fatalf("row-holder-first refusal is not the structured NOWAIT 55P03 stage: code=%s err=%v", borrowedReplacementGuardRowCode(stageErr), stageErr)
		}
		borrowedBoundReentryReadinessAssertBaseline(t, "bound baseline rollback row holder", ctx, b, base)
		t.Logf("bound baseline rollback row-holder-first negative: the independent holder produced the structured 55P03 at the supplied-tx fence with the complete baseline unchanged")
	}()

	// N9: replacement change: the actual target catalog identity no longer
	// matches the captured replacement; the fresh catalog check refuses.
	func() {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		restore := borrowedBoundBaselineRollbackApplyFence(ctx, t, b, fresh)
		defer restore()
		plan := newBorrowedBoundBaselineRollbackPlan()
		plan.renameTargetBeforeStage = true
		preparation := newBorrowedBoundBaselineRollbackPreparation()
		stageErr := borrowedBoundBaselineRollbackStageRun(ctx, t, b, fresh, 1, plan, preparation)
		if !errors.Is(stageErr, errBoundBaselineRollbackCatalog) {
			t.Fatalf("replacement-change refusal is not the real catalog stage: %v", stageErr)
		}
		borrowedBoundReentryReadinessAssertBaseline(t, "bound baseline rollback replacement change", ctx, b, base)
		t.Logf("bound baseline rollback replacement-change negative: the actual replacement catalog identity mismatch refused at the real catalog stage")
	}()

	// N10: shared-prefix loss: the real copied-prefix loss refuses the prefix
	// stage.
	func() {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		restore := borrowedBoundBaselineRollbackApplyFence(ctx, t, b, fresh)
		defer restore()
		plan := newBorrowedBoundBaselineRollbackPlan()
		plan.prefixLoss = true
		preparation := newBorrowedBoundBaselineRollbackPreparation()
		stageErr := borrowedBoundBaselineRollbackStageRun(ctx, t, b, fresh, 1, plan, preparation)
		if !errors.Is(stageErr, errBoundBaselineRollbackPrefix) {
			t.Fatalf("shared-prefix-loss refusal is not the real prefix stage: %v", stageErr)
		}
		borrowedBoundReentryReadinessAssertBaseline(t, "bound baseline rollback shared prefix loss", ctx, b, base)
		t.Logf("bound baseline rollback shared-prefix-loss negative: the real copied-prefix loss refused at the real prefix stage")
	}()

	// N11: observer failure: a real suspended observer role refuses the
	// protected observer stage before any transaction.
	func() {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		restore := borrowedBoundBaselineRollbackApplyFence(ctx, t, b, fresh)
		defer restore()
		plan := newBorrowedBoundBaselineRollbackPlan()
		plan.observerNologin = true
		preparation := newBorrowedBoundBaselineRollbackPreparation()
		stageErr := borrowedBoundBaselineRollbackStageRun(ctx, t, b, fresh, 1, plan, preparation)
		if !errors.Is(stageErr, errBoundBaselineRollbackObserver) {
			t.Fatalf("observer-failure refusal is not the real observer stage: %v", stageErr)
		}
		borrowedBoundReentryReadinessAssertBaseline(t, "bound baseline rollback observer failure", ctx, b, base)
		t.Logf("bound baseline rollback observer-failure negative: the suspended protected observer role refused the real observer stage before any transaction")
	}()

	// N12: failure between the two production writes: the rebuild audit
	// succeeds and the controlled resolution refuses at its real validation
	// stage; neither write survives the rollback.
	func() {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		restore := borrowedBoundBaselineRollbackApplyFence(ctx, t, b, fresh)
		defer restore()
		plan := newBorrowedBoundBaselineRollbackPlan()
		plan.emptyResolveEvidence = true
		preparation := newBorrowedBoundBaselineRollbackPreparation()
		stageErr := borrowedBoundBaselineRollbackStageRun(ctx, t, b, fresh, 1, plan, preparation)
		if !errors.Is(stageErr, errBoundBaselineRollbackClean) || !strings.Contains(stageErr.Error(), "rebuild evidence is required") {
			t.Fatalf("between-writes refusal is not the real controlled-resolution validation stage: %v", stageErr)
		}
		if errors.Is(stageErr, errBoundBaselineRollbackRehearsal) {
			t.Fatal("between-writes failure was reported as the positive rollback sentinel")
		}
		if !plan.rollbackAcknowledged {
			t.Fatal("between-writes failure rollback was not acknowledged")
		}
		borrowedBoundReentryReadinessAssertBaseline(t, "bound baseline rollback between writes", ctx, b, base)
		if count := borrowedReplacementOwnerJournalCount(t, ctx, b.fixture.controlPool, plan.rehearsalOperationID); count != 0 {
			t.Fatalf("between-writes failure left %d durable rehearsal audit rows", count)
		}
		t.Logf("bound baseline rollback between-writes negative: the rebuild audit succeeded, the controlled resolution refused at its real validation stage and the acknowledged rollback left neither write")
	}()

	// N13: cancellation before mutation / between the writes / after the
	// pending resolution: no pending transition survives.
	for _, cancelAt := range []string{"before-mutation", "between-writes", "after-resolution"} {
		cancelAt := cancelAt
		func() {
			b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
			_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
			base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
			laneCtx, cancelLane := context.WithCancel(ctx)
			defer cancelLane()
			restore := borrowedBoundBaselineRollbackApplyFence(ctx, t, b, fresh)
			defer restore()
			plan := newBorrowedBoundBaselineRollbackPlan()
			plan.cancelAt = cancelAt
			plan.cancelLane = cancelLane
			preparation := newBorrowedBoundBaselineRollbackPreparation()
			stageErr := borrowedBoundBaselineRollbackStageRun(laneCtx, t, b, fresh, 1, plan, preparation)
			if stageErr == nil {
				t.Fatalf("cancellation %s unexpectedly returned success", cancelAt)
			}
			if !errors.Is(stageErr, errBoundBaselineRollbackCancelled) {
				t.Fatalf("cancellation %s did not return the cancellation sentinel: %v", cancelAt, stageErr)
			}
			if laneCtx.Err() == nil {
				t.Fatalf("cancellation %s did not cancel the lane context", cancelAt)
			}
			observed, reached := plan.cancelStateNow()
			if !observed || reached != cancelAt {
				t.Fatalf("cancellation %s reached stage %q (observed=%t)", cancelAt, reached, observed)
			}
			if errors.Is(stageErr, errBoundBaselineRollbackUncertain) || strings.Contains(stageErr.Error(), "rollback target-lock acceptance transaction") {
				t.Fatalf("cancellation %s produced a rollback uncertainty: %v", cancelAt, stageErr)
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
			borrowedBoundReentryReadinessAssertBaseline(t, "bound baseline rollback cancellation "+cancelAt, ctx, b, base)
			if count := borrowedReplacementOwnerJournalCount(t, ctx, b.fixture.controlPool, plan.rehearsalOperationID); count != 0 {
				t.Fatalf("cancellation %s left %d durable rehearsal rows", cancelAt, count)
			}
			if !preparation.finishedNow() {
				t.Fatalf("cancellation %s left the preparation unfinished", cancelAt)
			}
			t.Logf("bound baseline rollback %s cancellation negative: the real cancellation sentinel was observed at the distinct %s stage, no pending transition survived and the one-shot preparation is finished", cancelAt, reached)
		}()
	}

	// N14: replay/copies with no reusable preparation result: a second stage
	// run (including through a copy of the preparation object) refuses.
	func() {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		plan := newBorrowedBoundBaselineRollbackPlan()
		preparation := newBorrowedBoundBaselineRollbackPreparation()
		first := borrowedBoundBaselineRollbackStageRun(ctx, t, b, fresh, 1, plan, preparation)
		if !errors.Is(first, errBoundBaselineRollbackCoverage) {
			t.Fatalf("replay control first stage is not the real coverage refusal: %v", first)
		}
		preparationCopy := *preparation
		second := borrowedBoundBaselineRollbackStageRun(ctx, t, b, fresh, 1, plan, &preparationCopy)
		if !errors.Is(second, errBoundBaselineRollbackReplay) {
			t.Fatalf("preparation-copy replay was not refused by the shared latch: %v", second)
		}
		borrowedBoundReentryReadinessAssertBaseline(t, "bound baseline rollback replay", ctx, b, base)
		t.Logf("bound baseline rollback replay negative: the one-shot preparation refused a second run and a copy of the preparation shared the same refusal; no reusable result exists")
	}()

	// N15: rollback-acknowledgement failure via owner-connection loss WITHOUT
	// any COMMIT: the writes abort with the owner connection, the guard stays
	// unresolved, the complete baseline is unchanged, zero rehearsal rows
	// survive and the owner is retired with no reusable result.
	func() {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		f := b.fixture
		_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		restore := borrowedBoundBaselineRollbackApplyFence(ctx, t, b, fresh)
		defer restore()
		plan := newBorrowedBoundBaselineRollbackPlan()
		plan.ownerLossBeforeRollback = true
		preparation := newBorrowedBoundBaselineRollbackPreparation()
		stageErr := borrowedBoundBaselineRollbackStageRun(ctx, t, b, fresh, 1, plan, preparation)
		if !errors.Is(stageErr, errBoundBaselineRollbackUncertain) ||
			!strings.Contains(stageErr.Error(), "rollback target-lock acceptance transaction") {
			t.Fatalf("owner-loss control did not report the authentic rollback-acknowledgement failure: %v", stageErr)
		}
		if plan.rollbackAcknowledged {
			t.Fatal("owner-loss control reported an acknowledged rollback")
		}
		if !plan.cleanVerified || !plan.localAuditFound {
			t.Fatal("owner-loss control did not reach the in-transaction pending resolution before the owner loss")
		}
		if disposition, operation := borrowedReplacementGuardRowRead(t, ctx, f.controlPool, f.guardKey); disposition == "clean" || operation != f.operation {
			t.Fatalf("owner-loss control committed the baseline transition: disposition=%q operation=%q", disposition, operation)
		}
		borrowedBoundReentryReadinessAssertBaseline(t, "bound baseline rollback owner loss", ctx, b, base)
		if count := borrowedReplacementOwnerJournalCount(t, ctx, f.controlPool, plan.rehearsalOperationID); count != 0 {
			t.Fatalf("owner-loss control left %d durable rehearsal rows", count)
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
		t.Logf("bound baseline rollback owner-loss negative: the owner connection loss produced the authentic rollback-acknowledgement failure with NO commit, the guard stayed unresolved, the complete baseline and zero rehearsal rows were verified and the owner was retired with no owner reuse")
	}()

	// N16: late shared-prefix loss under the held transaction, immediately
	// before the production writes: the in-transaction recheck refuses and
	// neither write is reached.
	func() {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		restore := borrowedBoundBaselineRollbackApplyFence(ctx, t, b, fresh)
		defer restore()
		plan := newBorrowedBoundBaselineRollbackPlan()
		plan.latePrefixLoss = true
		preparation := newBorrowedBoundBaselineRollbackPreparation()
		stageErr := borrowedBoundBaselineRollbackStageRun(ctx, t, b, fresh, 1, plan, preparation)
		if !errors.Is(stageErr, errBoundBaselineRollbackPrefix) {
			t.Fatalf("late-prefix-loss refusal is not the real in-transaction prefix stage: %v", stageErr)
		}
		if !plan.rollbackAcknowledged {
			t.Fatal("late-prefix-loss rollback was not acknowledged")
		}
		if plan.localAuditFound || plan.cleanVerified {
			t.Fatalf("late-prefix-loss control reached a production write (audit=%t clean=%t)", plan.localAuditFound, plan.cleanVerified)
		}
		borrowedBoundReentryReadinessAssertBaseline(t, "bound baseline rollback late prefix loss", ctx, b, base)
		if count := borrowedReplacementOwnerJournalCount(t, ctx, b.fixture.controlPool, plan.rehearsalOperationID); count != 0 {
			t.Fatalf("late-prefix-loss control left %d durable rehearsal rows", count)
		}
		t.Logf("bound baseline rollback late-prefix-loss negative: the shared prefix was invalidated after the initial check and the in-transaction recheck immediately before the production writes refused with the acknowledged rollback leaving neither write")
	}()

	// N17: pre-callback/pre-BEGIN refusal: the lane context is cancelled between
	// the initial checks and transaction entry, so the unchanged WithTransaction
	// refuses before the callback runs; NO transaction was rolled back and no
	// acknowledgment may be recorded.
	func() {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		laneCtx, cancelLane := context.WithCancel(ctx)
		defer cancelLane()
		restore := borrowedBoundBaselineRollbackApplyFence(ctx, t, b, fresh)
		defer restore()
		plan := newBorrowedBoundBaselineRollbackPlan()
		plan.cancelAt = "before-transaction"
		plan.cancelLane = cancelLane
		preparation := newBorrowedBoundBaselineRollbackPreparation()
		stageErr := borrowedBoundBaselineRollbackStageRun(laneCtx, t, b, fresh, 1, plan, preparation)
		if stageErr == nil {
			t.Fatal("pre-callback refusal control unexpectedly returned success")
		}
		if !errors.Is(stageErr, errBoundBaselineRollbackPreCallback) {
			t.Fatalf("pre-callback refusal is not the real pre-callback/BEGIN stage: %v", stageErr)
		}
		if laneCtx.Err() == nil {
			t.Fatal("pre-callback refusal control did not cancel the lane context")
		}
		if plan.rollbackAcknowledged {
			t.Fatal("pre-callback refusal recorded an acknowledged rollback although no transaction was rolled back")
		}
		observed, reached := plan.cancelStateNow()
		if !observed || reached != "before-transaction" {
			t.Fatalf("pre-callback refusal reached stage %q (observed=%t)", reached, observed)
		}
		if plan.ownerTxPID != 0 {
			t.Fatalf("pre-callback refusal entered the callback (ownerTx=%d)", plan.ownerTxPID)
		}
		if errors.Is(stageErr, errBoundBaselineRollbackUncertain) || strings.Contains(stageErr.Error(), "rollback target-lock acceptance transaction") {
			t.Fatalf("pre-callback refusal produced a rollback uncertainty: %v", stageErr)
		}
		borrowedBoundReentryReadinessAssertBaseline(t, "bound baseline rollback pre-callback refusal", ctx, b, base)
		if count := borrowedReplacementOwnerJournalCount(t, ctx, b.fixture.controlPool, plan.rehearsalOperationID); count != 0 {
			t.Fatalf("pre-callback refusal left %d durable rehearsal rows", count)
		}
		if !preparation.finishedNow() {
			t.Fatal("pre-callback refusal left the preparation unfinished")
		}
		t.Logf("bound baseline rollback pre-callback negative: the cancelled lane context made the unchanged transaction entry refuse before the callback ran, so no transaction was rolled back, no acknowledgment was recorded and the complete baseline stayed unchanged")
	}()
}
