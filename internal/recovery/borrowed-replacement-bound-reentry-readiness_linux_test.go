//go:build linux && drill

// borrowed-replacement-bound-reentry-readiness_linux_test.go is the bounded
// instance-bound controlled-reentry READINESS WITNESS lane (NON-AUTHORIZING).
// It composes ONLY existing, unchanged primitives: the genuine bound baseline
// with the single-consumed receipt/entry, the same-owner committed P1 rotation
// and the unchanged five-stage replacement orchestration/comparator (the bound
// native-ready capture), the existing bound dirty-guard refusal with its closed
// output contract, the UNCHANGED observed owner-window/owner-tail machinery
// (P1 registration, parked Use, independent observer readiness, owner-fenced
// SHARE window, final facts validation, shared-loss arbitration, acknowledged
// transaction completion, post-COMMIT rechecks, bounded joins), the existing
// supplied-transaction guard-row fence, and the existing fixed non-authorizing
// refusal journal helper. The lane adds ONE new-file-local atTail observation
// stage inside the supplied owner transaction: it proves the transaction is the
// original anchored owner (never the coordinator preparation transaction of the
// preceding re-entry lane), revalidates the bound instance provenance, the
// ACTUAL replacement catalog, the shared prefix, the real guard row with the
// original operation provenance and the independently held original advisory
// key. Only after every completion proof does the lane publish a single-use,
// lane-local, NON-AUTHORIZING readiness witness: plain observed facts with a
// one-shot consume, never a guard-resolution token, an executable admission
// handle, a capability or a clean transition. The guard stays deliberately
// unresolved, the inventory stays NULL, acceptance stays zero and the refused
// journal row stays append-only and non-authorizing. There is no rebuild
// readiness, no rebuildTargetWithWitness modification, no guard clean
// transition, no ResolveTargetGuardClean invocation, no admission, no restore,
// no restore probe beyond the session's single fixed SELECT 1, no acceptance,
// no manifest, no downstream and no Gate1 authority. Secrets, verifiers, DSNs
// and archive contents are never logged.
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

// Bound re-entry readiness refusal stages: every control captures and asserts
// the ACTUAL real stage instead of any non-nil error.
var (
	errBorrowedBoundReentryReadinessOwnerTx    = errors.New("bound reentry readiness: the supplied transaction is not the original anchored owner")
	errBorrowedBoundReentryReadinessInstance   = errors.New("bound reentry readiness: the bound instance provenance refused")
	errBorrowedBoundReentryReadinessCatalog    = errors.New("bound reentry readiness: the actual replacement catalog revalidation refused")
	errBorrowedBoundReentryReadinessPrefix     = errors.New("bound reentry readiness: the shared replacement prefix revalidation refused")
	errBorrowedBoundReentryReadinessAdvisory   = errors.New("bound reentry readiness: the independent advisory ownership confirmation refused")
	errBorrowedBoundReentryReadinessJournal    = errors.New("bound reentry readiness: the fixed refusal journal stage refused")
	errBorrowedBoundReentryReadinessPublished  = errors.New("bound reentry readiness: readiness publication refused")
	errBorrowedBoundReentryReadinessCompletion = errors.New("bound reentry readiness: the completion proofs are incomplete")
)

// borrowedBoundReentryReadinessObservation is the complete observed fact set of
// the supplied-tx atTail stage.
type borrowedBoundReentryReadinessObservation struct {
	ownerTxPID           int
	instanceState        string
	instanceKey          string
	instanceFingerprint  string
	instanceGeneration   int64
	instanceEvidenceHash string
	inventoryNull        bool
	versionNull          bool
	catalogOID           uint32
	advisoryHeld         bool
	prefixValid          bool
	disposition          string
	operation            string
	journalInserted      bool
	journalAuditID       int64
	journalPendingViaTx  bool
	journalPendingPool   int
}

func (o borrowedBoundReentryReadinessObservation) complete() bool {
	return o.ownerTxPID > 0 && o.instanceState == "open" && o.instanceKey != "" && o.instanceFingerprint != "" &&
		o.inventoryNull && o.versionNull && o.catalogOID != 0 && o.advisoryHeld && o.prefixValid &&
		o.disposition != "" && o.disposition != "clean" && o.operation != ""
}

// borrowedBoundReentryReadinessStage is the lane-local atTail plan and observed
// fact container. The optional journal attempt identity inserts ONE fixed
// non-authorizing refused audit row through the production fixed helper.
type borrowedBoundReentryReadinessStage struct {
	key               string
	expectedOperation string
	journalAttemptID  string
	journalFacts      *borrowedReplacementOwnerJournalFacts

	mu          sync.Mutex
	invocations int64
	observed    borrowedBoundReentryReadinessObservation
}

func (s *borrowedBoundReentryReadinessStage) invocationsNow() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.invocations
}

func (s *borrowedBoundReentryReadinessStage) observationNow() borrowedBoundReentryReadinessObservation {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.observed
}

// borrowedBoundReentryReadinessStageRun is the lane-local supplied-tx atTail
// stage. It runs strictly inside the unchanged owner-tail step (after the
// proven Use completion and before the unchanged final facts validation) and
// refuses with the exact stage sentinel on any missing/wrong fact. It NEVER
// bypasses a production check, NEVER resolves the guard and NEVER writes
// arbitrary SQL: the only durable write is the optional fixed refusal journal
// row through the production helper.
func borrowedBoundReentryReadinessStageRun(tailCtx context.Context, t *testing.T, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding, tx pgx.Tx, stage *borrowedBoundReentryReadinessStage) error {
	if tx == nil {
		return fmt.Errorf("%w: no supplied owner transaction", errBorrowedBoundReentryReadinessOwnerTx)
	}
	if stage == nil || b == nil || b.fixture == nil || fresh == nil {
		return errors.New("bound reentry readiness stage requires the concrete fixture, capture and plan")
	}
	f := b.fixture
	stage.mu.Lock()
	stage.invocations++
	stage.mu.Unlock()
	var observed borrowedBoundReentryReadinessObservation

	// 1. The supplied transaction must be the original anchored owner session,
	// explicitly distinct from the coordinator preparation transaction of the
	// preceding lane (asserted by the caller against the captured prep PID).
	var ownerTxPID int
	identityCtx, cancelIdentity := context.WithTimeout(tailCtx, borrowedOwnerRotationQueryBudget)
	identityErr := tx.QueryRow(identityCtx, `SELECT pg_backend_pid()`).Scan(&ownerTxPID)
	cancelIdentity()
	if identityErr != nil || ownerTxPID <= 0 {
		return fmt.Errorf("%w: the supplied-tx owner identity read refused", errBorrowedBoundReentryReadinessOwnerTx)
	}
	if ownerTxPID != b.owner.BackendPID {
		return fmt.Errorf("%w: supplied-tx pid %d is not the original anchored owner pid %d", errBorrowedBoundReentryReadinessOwnerTx, ownerTxPID, b.owner.BackendPID)
	}
	observed.ownerTxPID = ownerTxPID

	// 2. Bound instance provenance read FRESH through the supplied transaction.
	instanceCtx, cancelInstance := context.WithTimeout(tailCtx, borrowedOwnerRotationQueryBudget)
	instanceErr := tx.QueryRow(instanceCtx, `
SELECT state, target_guard_key, target_role_fingerprint, evidence_generation, evidence_hash,
       entry_chain_inventory IS NULL, entry_chain_inventory_version IS NULL
FROM recovery_instance WHERE instance_id=$1`, f.instanceID).
		Scan(&observed.instanceState, &observed.instanceKey, &observed.instanceFingerprint,
			&observed.instanceGeneration, &observed.instanceEvidenceHash,
			&observed.inventoryNull, &observed.versionNull)
	cancelInstance()
	if instanceErr != nil {
		return fmt.Errorf("%w: the bound instance provenance read refused: %v", errBorrowedBoundReentryReadinessInstance, instanceErr)
	}
	if observed.instanceState != "open" || observed.instanceKey != f.guardKey ||
		observed.instanceFingerprint != fresh.binding.OriginalRoleFingerprint() ||
		!observed.inventoryNull || !observed.versionNull {
		return fmt.Errorf("%w: the bound instance provenance is not the open NULL-inventory original", errBorrowedBoundReentryReadinessInstance)
	}

	// 3. ACTUAL replacement catalog revalidation through the same transaction.
	catalogCtx, cancelCatalog := context.WithTimeout(tailCtx, borrowedOwnerRotationQueryBudget)
	var catalogOID uint32
	catalogErr := tx.QueryRow(catalogCtx, `SELECT oid::oid FROM pg_database WHERE datname=$1`, f.targetDB).Scan(&catalogOID)
	cancelCatalog()
	if catalogErr != nil || catalogOID == 0 || catalogOID != fresh.replacementOID || catalogOID != fresh.binding.TargetDatabaseOID() {
		return fmt.Errorf("%w: the actual replacement catalog oid=%d is not the captured replacement oid=%d (err=%v)", errBorrowedBoundReentryReadinessCatalog, catalogOID, fresh.replacementOID, catalogErr)
	}
	observed.catalogOID = catalogOID

	// 4. Shared replacement prefix revalidation (shared in-memory state; the
	// unchanged flow re-performs the bounded I/O prefix recheck after return).
	if invalid, reason := fresh.prefix.Invalid(); invalid {
		return fmt.Errorf("%w: the shared replacement prefix is permanently invalidated: %s", errBorrowedBoundReentryReadinessPrefix, reason)
	}
	observed.prefixValid = true

	// 5. Real guard-row fence through ONLY the supplied transaction: the
	// deliberately unresolved original row with the expected operation
	// provenance, never a fallback inventory and never a clean transition.
	disposition, operation, fenceErr := borrowedReplacementGuardRowFence(tailCtx, tx, stage.key, stage.expectedOperation)
	if fenceErr != nil {
		return fenceErr
	}
	if disposition == "clean" || disposition == "" || operation != stage.expectedOperation {
		return fmt.Errorf("%w: the fenced guard row is not the unresolved expected original", errGuardRowWindowRowRefused)
	}
	observed.disposition = disposition
	observed.operation = operation

	// 6. Independent advisory-ownership confirmation through the same
	// transaction: the original canonical key advisory lock is really held by
	// the supplied owner session.
	k1, k2 := fresh.binding.OriginalTargetKey().AdvisoryLockKey()
	advisoryCtx, cancelAdvisory := context.WithTimeout(tailCtx, borrowedOwnerRotationQueryBudget)
	var advisoryHeld bool
	advisoryErr := tx.QueryRow(advisoryCtx, `
SELECT EXISTS (
  SELECT 1 FROM pg_locks
  WHERE locktype='advisory' AND granted AND pid=pg_backend_pid()
    AND classid::bigint = ($1::bigint & 4294967295)
    AND objid::bigint = ($2::bigint & 4294967295)
)`, k1, k2).Scan(&advisoryHeld)
	cancelAdvisory()
	if advisoryErr != nil || !advisoryHeld {
		return fmt.Errorf("%w: the original advisory key is not independently held by the supplied owner transaction (err=%v)", errBorrowedBoundReentryReadinessAdvisory, advisoryErr)
	}
	observed.advisoryHeld = true

	// 7. Optional fixed non-authorizing refusal journal row: ONE uniquely
	// identifiable refused audit row referencing the ACTUAL bound refusal,
	// inserted through the production fixed helper, pending-invisible to an
	// independent connection and never an authority payload.
	if stage.journalAttemptID != "" {
		auditID, journalErr := controlstore.WriteAuditReturningID(tailCtx, tx, borrowedReplacementOwnerJournalRecord(
			&borrowedReplacementOwnerJournalStage{attemptID: stage.journalAttemptID}, b))
		if journalErr != nil || auditID <= 0 {
			return fmt.Errorf("%w: the fixed audit insert refused: %v", errBorrowedBoundReentryReadinessJournal, journalErr)
		}
		row, rowErr := borrowedReplacementOwnerJournalRead(tailCtx, tx, auditID)
		if rowErr != nil || row.auditID != auditID || row.result != controlstore.AuditRefused || row.instanceID != "" || row.operationID != stage.journalAttemptID {
			return fmt.Errorf("%w: the pending fixed refused row does not match", errBorrowedBoundReentryReadinessJournal)
		}
		pendingCtx, cancelPending := context.WithTimeout(tailCtx, borrowedOwnerRotationQueryBudget)
		var visible int
		pendingErr := b.fixture.controlPool.QueryRow(pendingCtx, `SELECT count(*)::int FROM recovery_audit WHERE audit_id=$1`, auditID).Scan(&visible)
		cancelPending()
		if pendingErr != nil || visible != 0 {
			return fmt.Errorf("%w: the uncommitted journal row was visible to an independent connection (visible=%d err=%v)", errBorrowedBoundReentryReadinessJournal, visible, pendingErr)
		}
		observed.journalInserted = true
		observed.journalAuditID = auditID
		observed.journalPendingViaTx = true
		observed.journalPendingPool = visible
		if stage.journalFacts != nil {
			stage.journalFacts.inserted = true
			stage.journalFacts.auditID = auditID
			stage.journalFacts.pendingViaTx = true
			stage.journalFacts.pendingViaPool = visible
		}
	}

	stage.mu.Lock()
	stage.observed = observed
	stage.mu.Unlock()
	return nil
}

// borrowedBoundReentryReadinessRun composes the UNCHANGED owner-tail machinery
// with the lane-local supplied-tx observation stage in atTail.
func borrowedBoundReentryReadinessRun(ctx context.Context, t *testing.T, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding, stage *borrowedBoundReentryReadinessStage, extra *borrowedReplacementOwnerTailHooks) *borrowedReplacementOwnerTailOutcome {
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
		return borrowedBoundReentryReadinessStageRun(tailCtx, t, b, fresh, tx, stage)
	}
	return borrowedReplacementOwnerTailRun(ctx, t, b, fresh, hooks)
}

// borrowedBoundReentryReadinessFacts is the plain observed fact set of the
// published witness: no handle, no capability, no resolution authority.
type borrowedBoundReentryReadinessFacts struct {
	InstanceID         string
	TargetKey          string
	RoleFingerprint    string
	ReplacementOID     uint32
	OwnerPID           int
	OwnerTxPID         int
	EvidenceGeneration int64
	EvidenceHash       string
	Nonce              int64
}

// borrowedBoundReentryReadinessState is the copy-shared single-use state: the
// witness can be published at most once and consumed at most once.
type borrowedBoundReentryReadinessState struct {
	mu        sync.Mutex
	published bool
	consumed  bool
	nonce     int64
	facts     borrowedBoundReentryReadinessFacts
}

// borrowedBoundReentryReadiness is the lane-local NON-AUTHORIZING readiness
// witness. Copies share the same state pointer; consume is one-shot and returns
// only plain observed facts.
type borrowedBoundReentryReadiness struct {
	state *borrowedBoundReentryReadinessState
}

func newBorrowedBoundReentryReadiness() *borrowedBoundReentryReadiness {
	return &borrowedBoundReentryReadiness{state: &borrowedBoundReentryReadinessState{}}
}

func (r *borrowedBoundReentryReadiness) publish(facts borrowedBoundReentryReadinessFacts) error {
	if r == nil || r.state == nil {
		return errors.New("bound reentry readiness witness is absent")
	}
	if facts.InstanceID == "" || facts.TargetKey == "" || facts.RoleFingerprint == "" ||
		facts.ReplacementOID == 0 || facts.OwnerPID <= 0 || facts.OwnerTxPID <= 0 {
		return errors.New("bound reentry readiness publication requires the complete observed provenance facts")
	}
	r.state.mu.Lock()
	defer r.state.mu.Unlock()
	if r.state.published {
		return errors.New("bound reentry readiness witness was already published")
	}
	if r.state.consumed {
		return errors.New("bound reentry readiness witness was already consumed")
	}
	r.state.nonce++
	facts.Nonce = r.state.nonce
	r.state.facts = facts
	r.state.published = true
	return nil
}

func (r *borrowedBoundReentryReadiness) consume() (borrowedBoundReentryReadinessFacts, bool) {
	if r == nil || r.state == nil {
		return borrowedBoundReentryReadinessFacts{}, false
	}
	r.state.mu.Lock()
	defer r.state.mu.Unlock()
	if !r.state.published || r.state.consumed {
		return borrowedBoundReentryReadinessFacts{}, false
	}
	r.state.consumed = true
	return r.state.facts, true
}

func (r *borrowedBoundReentryReadiness) publishedNow() bool {
	if r == nil || r.state == nil {
		return false
	}
	r.state.mu.Lock()
	defer r.state.mu.Unlock()
	return r.state.published
}

// borrowedBoundReentryReadinessTryPublish evaluates EVERY completion proof of
// the unchanged machinery and only then publishes the single-use witness. Any
// incomplete/UNKNOWN/refused proof returns an error without publishing.
func borrowedBoundReentryReadinessTryPublish(t *testing.T, label string, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding, stage *borrowedBoundReentryReadinessStage, outcome *borrowedReplacementOwnerTailOutcome, readiness *borrowedBoundReentryReadiness) error {
	t.Helper()
	if readiness == nil || readiness.state == nil {
		return fmt.Errorf("%w: %s has no readiness witness", errBorrowedBoundReentryReadinessPublished, label)
	}
	if outcome == nil || outcome.flow == nil || outcome.state == nil {
		return fmt.Errorf("%w: %s has no completion outcome", errBorrowedBoundReentryReadinessCompletion, label)
	}
	if outcome.runErr != nil {
		return fmt.Errorf("%w: %s flow refused: %v", errBorrowedBoundReentryReadinessCompletion, label, outcome.runErr)
	}
	if outcome.flow.ownerErr != nil || outcome.flow.useErr != nil || outcome.flow.prefixErr != nil ||
		outcome.flow.anchorErr != nil || outcome.flow.healthErr != nil || outcome.flow.inspectionErr != nil ||
		outcome.flow.ackErr != nil || outcome.flow.decisionErr != nil {
		return fmt.Errorf("%w: %s completion proof is incomplete", errBorrowedBoundReentryReadinessCompletion, label)
	}
	if outcome.pumpJoinErr != nil || outcome.flow.terminationUnknown {
		return fmt.Errorf("%w: %s termination is UNKNOWN", errBorrowedBoundReentryReadinessCompletion, label)
	}
	if !outcome.flow.state.compositePublished() || !outcome.flow.state.sealedNow() || outcome.flow.state.lostNow() {
		return fmt.Errorf("%w: %s terminal state is not the sealed loss-free composite", errBorrowedBoundReentryReadinessCompletion, label)
	}
	if outcome.state.tailInvocationsNow() != 1 || outcome.state.adapterInvocationsNow() != 1 || !outcome.state.eligibleNow() {
		return fmt.Errorf("%w: %s owner-tail/arbitration proofs are incomplete", errBorrowedBoundReentryReadinessCompletion, label)
	}
	if outcome.flow.probeExecutions != 1 {
		return fmt.Errorf("%w: %s probe executions=%d, want exactly 1", errBorrowedBoundReentryReadinessCompletion, label, outcome.flow.probeExecutions)
	}
	if stage == nil {
		return fmt.Errorf("%w: %s has no supplied-tx stage", errBorrowedBoundReentryReadinessCompletion, label)
	}
	if stage.invocationsNow() != 1 {
		return fmt.Errorf("%w: %s supplied-tx stage invocations=%d, want exactly 1", errBorrowedBoundReentryReadinessCompletion, label, stage.invocationsNow())
	}
	observed := stage.observationNow()
	if !observed.complete() {
		return fmt.Errorf("%w: %s supplied-tx observation is incomplete", errBorrowedBoundReentryReadinessCompletion, label)
	}
	if observed.instanceFingerprint != fresh.binding.OriginalRoleFingerprint() || observed.catalogOID != fresh.replacementOID ||
		observed.operation != stage.expectedOperation || observed.ownerTxPID != b.owner.BackendPID {
		return fmt.Errorf("%w: %s supplied-tx observation does not match the captured provenance", errBorrowedBoundReentryReadinessCompletion, label)
	}
	return readiness.publish(borrowedBoundReentryReadinessFacts{
		InstanceID:         b.fixture.instanceID,
		TargetKey:          b.fixture.guardKey,
		RoleFingerprint:    observed.instanceFingerprint,
		ReplacementOID:     observed.catalogOID,
		OwnerPID:           b.owner.BackendPID,
		OwnerTxPID:         observed.ownerTxPID,
		EvidenceGeneration: observed.instanceGeneration,
		EvidenceHash:       observed.instanceEvidenceHash,
	})
}

// borrowedBoundReentryReadinessAssertUnpublished requires a refusal control to
// have published nothing and to have no consumable witness (including through a
// shared-state copy).
func borrowedBoundReentryReadinessAssertUnpublished(t *testing.T, label string, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding, stage *borrowedBoundReentryReadinessStage, outcome *borrowedReplacementOwnerTailOutcome, readiness *borrowedBoundReentryReadiness) {
	t.Helper()
	if err := borrowedBoundReentryReadinessTryPublish(t, label, b, fresh, stage, outcome, readiness); err == nil {
		t.Fatalf("%s: readiness was published without the complete completion proofs", label)
	}
	if readiness.publishedNow() {
		t.Fatalf("%s: readiness reports published after a refusal", label)
	}
	if _, ok := readiness.consume(); ok {
		t.Fatalf("%s: readiness was consumable after a refusal", label)
	}
	copied := *readiness
	if _, ok := copied.consume(); ok {
		t.Fatalf("%s: a shared-state readiness copy was consumable after a refusal", label)
	}
}

// borrowedBoundReentryReadinessBaseline is the complete pre/post durable
// baseline of one bound fixture: the instance snapshot (identity, generation,
// hash, NULL inventory + guard hash), the evidence/audit row digest, the actual
// replacement catalog OID and the real guard row.
type borrowedBoundReentryReadinessBaseline struct {
	snapshot    borrowedBoundSessionSnapshot
	rows        borrowedBoundNativeReadyRows
	catalogOID  uint32
	disposition string
	operation   string
}

func borrowedBoundReentryReadinessCaptureBaseline(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding) *borrowedBoundReentryReadinessBaseline {
	t.Helper()
	base := &borrowedBoundReentryReadinessBaseline{
		snapshot: borrowedBoundSessionSnapshotNow(t, ctx, b),
		rows:     borrowedBoundNativeReadyRowsNow(t, ctx, b),
	}
	oid, oidErr := borrowedOwnerDDLTargetOID(ctx, b)
	if oidErr != nil || oid != fresh.replacementOID {
		t.Fatalf("bound reentry readiness baseline replacement catalog identity: oid=%d err=%v", oid, oidErr)
	}
	if fresh.replacementOID == 0 || fresh.replacementOID == fresh.oldOID || fresh.binding.TargetDatabaseOID() != fresh.replacementOID {
		t.Fatalf("bound reentry readiness baseline is not replacement-bound: replacement=%d old=%d binding=%d", fresh.replacementOID, fresh.oldOID, fresh.binding.TargetDatabaseOID())
	}
	base.catalogOID = oid
	base.disposition, base.operation = borrowedReplacementGuardRowRead(t, ctx, b.fixture.controlPool, b.fixture.guardKey)
	if base.disposition == "clean" || base.operation != b.fixture.operation {
		t.Fatalf("bound reentry readiness baseline guard row is not the deliberately unresolved original: disposition=%q operation=%q", base.disposition, base.operation)
	}
	return base
}

func borrowedBoundReentryReadinessAssertBaseline(t *testing.T, label string, ctx context.Context, b *borrowedSuccessorBaseline, base *borrowedBoundReentryReadinessBaseline) {
	t.Helper()
	borrowedBoundSessionAssertSnapshotEqual(t, label, base.snapshot, borrowedBoundSessionSnapshotNow(t, ctx, b))
	if after := borrowedBoundNativeReadyRowsNow(t, ctx, b); after != base.rows {
		t.Fatalf("%s changed durable evidence/audit rows: before=%+v after=%+v", label, base.rows, after)
	}
	if afterOID, oidErr := borrowedOwnerDDLTargetOID(ctx, b); oidErr != nil || afterOID != base.catalogOID {
		t.Fatalf("%s changed the replacement catalog: oid=%d err=%v", label, afterOID, oidErr)
	}
	if disposition, operation := borrowedReplacementGuardRowRead(t, ctx, b.fixture.controlPool, b.fixture.guardKey); disposition != base.disposition || operation != base.operation {
		t.Fatalf("%s changed the guard row: %q/%q -> %q/%q", label, base.disposition, base.operation, disposition, operation)
	}
}

// TestBorrowedReplacementBoundReentryReadiness is the bounded instance-bound
// controlled-reentry readiness witness lane described in the file header.
func TestBorrowedReplacementBoundReentryReadiness(t *testing.T) {
	ctx := t.Context()

	// P: the single chain over ONE authentic bound fixture.
	func() {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		f := b.fixture
		ownerPID := fresh.binding.ControlBackendPID()
		if ownerPID <= 0 || ownerPID != b.owner.BackendPID {
			t.Fatalf("bound reentry readiness original owner identity: control=%d anchor=%d", ownerPID, b.owner.BackendPID)
		}
		// Preserved provenance: authentic instance identity, immutable
		// key/fingerprint, original owner/anchor, actual replacement OID and
		// NULL inventory (restated by the baseline helper).
		borrowedBoundSessionAssertProvenance(t, ctx, b, fresh, "bound reentry readiness")
		if f.instanceID == "" || b.owner.OriginalInstanceID != f.instanceID || fresh.binding.OriginalInstanceID() != f.instanceID {
			t.Fatalf("bound reentry readiness lost the authentic instance identity: fixture=%q anchor=%q capture=%q", f.instanceID, b.owner.OriginalInstanceID, fresh.binding.OriginalInstanceID())
		}
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)

		// Stage 1: the EXISTING bound dirty-guard refusal with the closed
		// output contract, capturing the coordinator preparation transaction
		// PID through the existing marker injection seam.
		var probeCalls, acceptanceCalls int32
		prepAttemptID := fmt.Sprintf("borrowed-replacement-bound-reentry-readiness-refusal-%d", time.Now().UnixNano())
		marker := newBorrowedBoundNativeReadyMarker(prepAttemptID, f.adminRole)
		var prepTxPID atomic.Int64
		inner := marker.inner
		marker.inner = func(hookCtx context.Context, tx pgx.Tx, locked controlstore.InstanceToken) (recovery.EvidenceToken, error) {
			var pid int
			if err := tx.QueryRow(hookCtx, `SELECT pg_backend_pid()`).Scan(&pid); err == nil && pid > 0 {
				prepTxPID.Store(int64(pid))
			}
			return inner(hookCtx, tx, locked)
		}
		attempt := prepareBorrowedBoundNativeReadyAttempt(t, ctx, b, fresh, prepAttemptID, marker, &probeCalls, &acceptanceCalls)
		if err := attempt.registerOrigin(ctx); err != nil {
			t.Fatalf("bound reentry readiness refusal origin registration refused: %v", err)
		}
		result, receipt, runErr := dispatchBorrowedBoundNativeReady(t, ctx, attempt)
		if runErr == nil || runErr.Error() != "bound target guard is not clean" {
			t.Fatalf("bound reentry readiness refusal is not the exact production bound dirty-guard refusal: %v", runErr)
		}
		if got := marker.callsNow(); got != 1 {
			t.Fatalf("bound reentry readiness refusal marker calls=%d, want exactly 1", got)
		}
		refusalToken := marker.token()
		if refusalToken.InstanceID != f.instanceID || refusalToken.State != "open" || refusalToken.Generation <= 0 || refusalToken.Hash == "" {
			t.Fatalf("bound reentry readiness refusal marker did not return the genuine token: %+v", refusalToken)
		}
		if result.MarkerToken != refusalToken {
			t.Fatalf("bound reentry readiness refusal did not carry the genuine transaction-local marker token")
		}
		borrowedBoundNativeReadyRequireNoProbeOutput(t, "bound reentry readiness refusal", result)
		borrowedBoundNativeReadyRequireAbsentReceipt(t, "bound reentry readiness refusal", receipt)
		borrowedBoundReentryAssertOutcomeBoundaries(t, "bound reentry readiness refusal", attempt, result, receipt, &probeCalls, &acceptanceCalls)
		prepPID := int(prepTxPID.Load())
		if prepPID <= 0 {
			t.Fatal("bound reentry readiness refusal did not capture the coordinator preparation transaction PID")
		}
		if prepPID == ownerPID {
			t.Fatalf("bound reentry readiness coordinator preparation transaction reused the original owner session: pid=%d", prepPID)
		}
		borrowedBoundReentryReadinessAssertBaseline(t, "bound reentry readiness refusal", ctx, b, base)
		t.Logf("bound reentry readiness refusal: exact production refusal %q, marker once with generation %d, coordinator preparation tx pid %d distinct from owner pid %d, closed output contract held and the complete baseline unchanged", runErr.Error(), refusalToken.Generation, prepPID, ownerPID)

		// Stage 2: the UNCHANGED observed owner-window/owner-tail machinery with
		// the lane-local supplied-tx observation stage and the fixed refused
		// journal row referencing the ACTUAL bound refusal.
		stage := &borrowedBoundReentryReadinessStage{
			key: f.guardKey, expectedOperation: f.operation,
			journalAttemptID: prepAttemptID, journalFacts: &borrowedReplacementOwnerJournalFacts{},
		}
		outcome := borrowedBoundReentryReadinessRun(ctx, t, b, fresh, stage, nil)
		readiness := newBorrowedBoundReentryReadiness()
		if err := borrowedBoundReentryReadinessTryPublish(t, "bound reentry readiness positive", b, fresh, stage, outcome, readiness); err != nil {
			t.Fatalf("bound reentry readiness positive did not satisfy the completion proofs: %v (ownerErr=%v useErr=%v prefixErr=%v terminationUnknown=%t)", err, outcome.flow.ownerErr, outcome.flow.useErr, outcome.flow.prefixErr, outcome.flow.terminationUnknown)
		}
		observed := stage.observationNow()
		if observed.ownerTxPID != ownerPID || observed.ownerTxPID == prepPID {
			t.Fatalf("bound reentry readiness owner transaction is not distinguished from the coordinator preparation transaction: ownerTx=%d owner=%d prep=%d", observed.ownerTxPID, ownerPID, prepPID)
		}
		if observed.instanceState != "open" || observed.instanceKey != f.guardKey ||
			observed.instanceFingerprint != fresh.binding.OriginalRoleFingerprint() || !observed.inventoryNull || !observed.versionNull {
			t.Fatalf("bound reentry readiness supplied-tx instance provenance: %+v", observed)
		}
		if observed.catalogOID != fresh.replacementOID || !observed.advisoryHeld || !observed.prefixValid {
			t.Fatalf("bound reentry readiness supplied-tx catalog/prefix/advisory facts: %+v", observed)
		}
		if observed.disposition != base.disposition || observed.operation != base.operation {
			t.Fatalf("bound reentry readiness supplied-tx fence changed the guard contents: %q/%q -> %q/%q", base.disposition, base.operation, observed.disposition, observed.operation)
		}
		if !observed.journalInserted || observed.journalAuditID <= 0 || !observed.journalPendingViaTx || observed.journalPendingPool != 0 {
			t.Fatalf("bound reentry readiness fixed journal stage: %+v", observed)
		}
		if got := outcome.state.tailInvocationsNow(); got != 1 {
			t.Fatalf("bound reentry readiness tail invocations=%d, want exactly 1", got)
		}
		facts, ok := readiness.consume()
		if !ok {
			t.Fatal("bound reentry readiness witness was not consumable once")
		}
		if facts.InstanceID != f.instanceID || facts.TargetKey != f.guardKey || facts.RoleFingerprint != fresh.binding.OriginalRoleFingerprint() ||
			facts.ReplacementOID != fresh.replacementOID || facts.OwnerPID != ownerPID || facts.OwnerTxPID != ownerPID || facts.Nonce != 1 {
			t.Fatalf("bound reentry readiness facts are not the observed provenance: %+v", facts)
		}
		if _, again := readiness.consume(); again {
			t.Fatal("bound reentry readiness witness was consumable twice")
		}
		copiedReadiness := *readiness
		if _, copiedOK := copiedReadiness.consume(); copiedOK {
			t.Fatal("a shared-state bound reentry readiness copy was consumable")
		}
		if !outcome.state.consumeEligibility() || outcome.state.consumeEligibility() != false {
			t.Fatal("bound reentry readiness owner-tail eligibility was not one-shot")
		}
		// The fixed refused journal row is durable, append-only and
		// non-authorizing: exactly one matching row, non-instance-bound, never
		// referenced by the readiness witness and never compensated away.
		if count := borrowedReplacementOwnerJournalCount(t, ctx, f.controlPool, prepAttemptID); count != 1 {
			t.Fatalf("bound reentry readiness durable refused journal rows=%d, want exactly 1", count)
		}
		journalRow, journalErr := borrowedReplacementOwnerJournalRead(ctx, f.controlPool, observed.journalAuditID)
		if journalErr != nil || journalRow.result != controlstore.AuditRefused || journalRow.instanceID != "" || journalRow.operationID != prepAttemptID {
			t.Fatalf("bound reentry readiness durable journal row mismatch: %+v err=%v", journalRow, journalErr)
		}
		// The fixed refused journal row is NON-instance-bound (NULL
		// instance_id), so the instance-scoped evidence/audit digest must stay
		// byte-identical; the row is independently verified above through the
		// fixed journal count/read helpers and is never authority.
		afterRows := borrowedBoundNativeReadyRowsNow(t, ctx, b)
		if afterRows != base.rows {
			t.Fatalf("bound reentry readiness changed the instance-scoped durable evidence/audit rows: before=%+v after=%+v", base.rows, afterRows)
		}
		borrowedBoundSessionAssertSnapshotEqual(t, "bound reentry readiness positive", base.snapshot, borrowedBoundSessionSnapshotNow(t, ctx, b))
		if afterOID, oidErr := borrowedOwnerDDLTargetOID(ctx, b); oidErr != nil || afterOID != base.catalogOID {
			t.Fatalf("bound reentry readiness positive changed the replacement catalog: oid=%d err=%v", afterOID, oidErr)
		}
		if disposition, operation := borrowedReplacementGuardRowRead(t, ctx, f.controlPool, f.guardKey); disposition != base.disposition || operation != base.operation {
			t.Fatalf("bound reentry readiness positive changed the guard row: %q/%q -> %q/%q", base.disposition, base.operation, disposition, operation)
		}
		borrowedReplacementOwnerTailReplayRefuse(t, ctx, b, fresh, outcome, "post-readiness")
		if err := borrowedReplacementOwnerTailRetireGuarded(t, ctx, b, outcome); err != nil {
			t.Fatalf("bound reentry readiness guarded retirement refused: %v", err)
		}
		borrowedOwnerDDLGuard(t, ctx, b)
		t.Logf("bound reentry readiness positive: bound provenance preserved; existing dirty-guard refusal %q with coordinator prep tx pid %d; supplied-tx observation proved owner tx pid %d (distinct from prep, equal to the anchored owner), open NULL-inventory instance %s, actual catalog oid %d, shared prefix valid, unresolved guard row %q/%q and the independently held advisory key; the unchanged composite committed, one fixed non-instance-bound refused journal row is durable/append-only, the single-use NON-AUTHORIZING readiness witness was consumed once, guarded retirement ran and the guard stayed unresolved with inventory NULL and acceptance zero", runErr.Error(), prepPID, observed.ownerTxPID, f.instanceID, observed.catalogOID, observed.disposition, observed.operation)
	}()

	// N1: wrong REAL canonical key + missing guard row: the different existing
	// database key is independently proven to have no guard row, the real
	// supplied-tx fence refuses at the no-row stage, nothing is published and
	// no fallback inventory row appears.
	func() {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		diffKey := borrowedBoundReentryDifferentKey(t, b)
		if diffKey.String() == b.fixture.guardKey {
			t.Fatal("bound reentry readiness wrong-key control key equals the original canonical key")
		}
		if count := borrowedReplacementGuardRowCount(t, ctx, b.fixture.controlPool, diffKey.String()); count != 0 {
			t.Fatalf("bound reentry readiness missing-row control key already has %d guard rows", count)
		}
		stage := &borrowedBoundReentryReadinessStage{key: diffKey.String(), expectedOperation: b.fixture.operation}
		outcome := borrowedBoundReentryReadinessRun(ctx, t, b, fresh, stage, nil)
		readiness := newBorrowedBoundReentryReadiness()
		borrowedBoundReentryReadinessAssertUnpublished(t, "bound reentry readiness wrong-key/missing-row", b, fresh, stage, outcome, readiness)
		if !errors.Is(outcome.flow.ownerErr, errGuardRowWindowRowRefused) || !strings.Contains(outcome.flow.ownerErr.Error(), "no guard row exists") {
			t.Fatalf("wrong-key/missing-row refusal is not the real no-row fence stage: %v", outcome.flow.ownerErr)
		}
		if outcome.state.eligibleNow() || outcome.flow.state.compositePublished() {
			t.Fatal("wrong-key/missing-row control produced eligibility/composite")
		}
		borrowedBoundReentryReadinessAssertBaseline(t, "bound reentry readiness wrong-key/missing-row", ctx, b, base)
		if count := borrowedReplacementGuardRowCount(t, ctx, b.fixture.controlPool, diffKey.String()); count != 0 {
			t.Fatalf("wrong-key/missing-row refusal created %d fallback inventory rows", count)
		}
		t.Logf("bound reentry readiness wrong-key/missing-row negative: the real different canonical key %s was independently proven to have no guard row, the supplied-tx fence refused at the exact no-row stage, no fallback inventory appeared and nothing was published", diffKey.String()[:18])
	}()

	// N2: wrong REAL operation provenance on the REAL row: the supplied-tx fence
	// refuses at the operation-provenance stage with the row unchanged and
	// nothing published.
	func() {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		stage := &borrowedBoundReentryReadinessStage{key: b.fixture.guardKey, expectedOperation: b.fixture.operation + "-not-the-original"}
		outcome := borrowedBoundReentryReadinessRun(ctx, t, b, fresh, stage, nil)
		readiness := newBorrowedBoundReentryReadiness()
		borrowedBoundReentryReadinessAssertUnpublished(t, "bound reentry readiness wrong-operation", b, fresh, stage, outcome, readiness)
		if !errors.Is(outcome.flow.ownerErr, errGuardRowWindowRowRefused) || !strings.Contains(outcome.flow.ownerErr.Error(), "operation provenance") {
			t.Fatalf("wrong-operation refusal is not the real operation-provenance stage: %v", outcome.flow.ownerErr)
		}
		if outcome.state.eligibleNow() || outcome.flow.state.compositePublished() {
			t.Fatal("wrong-operation control produced eligibility/composite")
		}
		borrowedBoundReentryReadinessAssertBaseline(t, "bound reentry readiness wrong-operation", ctx, b, base)
		t.Logf("bound reentry readiness wrong-operation negative: the real mismatched operation provenance refused the supplied-tx fence with the row and the complete baseline unchanged and nothing published")
	}()

	// N3: wrong REAL role provenance: a real observer-role connection is
	// refused by the unchanged registration at its exact role/database stage,
	// nothing is published and the complete baseline is unchanged.
	func() {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		observerConn := borrowedOwnerDDLConnect(t, ctx, b.fixture.observerTargetDSN)
		reg, regErr := newBorrowedReplacementSessionRegistration(t, ctx, b, fresh, observerConn)
		borrowedSuccessorCloseConn(t, observerConn)
		if reg != nil || regErr == nil || !strings.Contains(regErr.Error(), "role/database is not the W replacement target") {
			t.Fatalf("wrong-role registration was not refused at the real role/database stage: reg=%v err=%v", reg, regErr)
		}
		readiness := newBorrowedBoundReentryReadiness()
		stage := &borrowedBoundReentryReadinessStage{key: b.fixture.guardKey, expectedOperation: b.fixture.operation}
		if err := borrowedBoundReentryReadinessTryPublish(t, "bound reentry readiness wrong-role", b, fresh, stage, nil, readiness); err == nil {
			t.Fatal("wrong-role control published a readiness witness without any owner-tail completion")
		}
		if readiness.publishedNow() {
			t.Fatal("wrong-role control reports a published readiness witness")
		}
		if _, ok := readiness.consume(); ok {
			t.Fatal("wrong-role control left a consumable readiness witness")
		}
		borrowedBoundReentryReadinessAssertBaseline(t, "bound reentry readiness wrong-role", ctx, b, base)
		t.Logf("bound reentry readiness wrong-role negative: the real observer-role connection was refused by the unchanged registration at the role/database stage, no witness was published and the complete baseline stayed unchanged")
	}()

	// N4: row-holder-first: an independent holder owns the REAL guard row, the
	// supplied-tx fence receives the structured 55P03, the owner transaction
	// refuses, nothing is published and the released row is unchanged.
	func() {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		hold, holdErr := borrowedReplacementGuardRowHoldRow(ctx, b.fixture.controlPool, b.fixture.guardKey)
		if holdErr != nil {
			t.Fatalf("bound reentry readiness row-holder-first hold refused: %v", holdErr)
		}
		defer hold.release()
		stage := &borrowedBoundReentryReadinessStage{key: b.fixture.guardKey, expectedOperation: b.fixture.operation}
		outcome := borrowedBoundReentryReadinessRun(ctx, t, b, fresh, stage, nil)
		readiness := newBorrowedBoundReentryReadiness()
		borrowedBoundReentryReadinessAssertUnpublished(t, "bound reentry readiness row-holder-first", b, fresh, stage, outcome, readiness)
		if !errors.Is(outcome.flow.ownerErr, errGuardRowWindowRowRefused) || borrowedReplacementGuardRowCode(outcome.flow.ownerErr) != "55P03" {
			t.Fatalf("row-holder-first refusal is not the structured NOWAIT 55P03 stage: code=%s err=%v", borrowedReplacementGuardRowCode(outcome.flow.ownerErr), outcome.flow.ownerErr)
		}
		hold.release()
		borrowedBoundReentryReadinessAssertBaseline(t, "bound reentry readiness row-holder-first", ctx, b, base)
		if released, releasedOperation, releasedErr := borrowedReplacementGuardRowContenderLock(ctx, b.fixture.controlPool, b.fixture.guardKey); releasedErr != nil || released != base.disposition || releasedOperation != base.operation {
			t.Fatalf("row-holder-first released row is not lockable/unchanged: %q/%q err=%v", released, releasedOperation, releasedErr)
		}
		t.Logf("bound reentry readiness row-holder-first negative: the independent holder produced the structured 55P03 at the supplied-tx fence, nothing was published, the complete baseline stayed unchanged and the released row was lockable/unchanged")
	}()

	// N5: real replacement contamination: the unchanged capture refuses at the
	// real DDL stage and no readiness witness exists.
	func() {
		readiness := newBorrowedBoundReentryReadiness()
		borrowedBoundNativeReadyContaminatedRefuse(t, ctx)
		if readiness.publishedNow() {
			t.Fatal("contaminated replacement control published a readiness witness")
		}
		if _, ok := readiness.consume(); ok {
			t.Fatal("contaminated replacement control left a consumable readiness witness")
		}
		t.Logf("bound reentry readiness contaminated-replacement negative: the unchanged bound capture refused at the real DDL stage and no readiness witness was published")
	}()

	// N6: shared-prefix loss: the REAL copied-prefix loss makes the parked Use
	// refuse; the owner transaction refuses through the actual use-failure
	// stage, the prefix is permanently invalidated, session reconstruction
	// refuses, nothing is published and the complete baseline is unchanged.
	func() {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		stage := &borrowedBoundReentryReadinessStage{key: b.fixture.guardKey, expectedOperation: b.fixture.operation}
		outcome := borrowedBoundReentryReadinessRun(ctx, t, b, fresh, stage, &borrowedReplacementOwnerTailHooks{
			postProbe: func(_ context.Context, _ *borrowedReplacementAutonomousPump, _ func()) error {
				borrowedReplacementSessionCopiedPrefixLoss(t, fresh)
				return nil
			},
		})
		readiness := newBorrowedBoundReentryReadiness()
		borrowedBoundReentryReadinessAssertUnpublished(t, "bound reentry readiness shared-prefix loss", b, fresh, stage, outcome, readiness)
		if invalid, reason := fresh.prefix.Invalid(); !invalid || reason == "" {
			t.Fatal("shared-prefix loss control did not permanently invalidate the shared prefix")
		}
		if !errors.Is(outcome.flow.ownerErr, errOwnerTailUseFailed) && outcome.flow.useErr == nil {
			t.Fatalf("shared-prefix loss refusal is not the actual use-failure stage: ownerErr=%v useErr=%v", outcome.flow.ownerErr, outcome.flow.useErr)
		}
		if outcome.state.eligibleNow() || outcome.flow.state.compositePublished() {
			t.Fatal("shared-prefix loss control produced eligibility/composite")
		}
		borrowedReplacementOwnerTailReplayRefuse(t, ctx, b, fresh, outcome, "shared-prefix loss")
		borrowedBoundReentryReadinessAssertBaseline(t, "bound reentry readiness shared-prefix loss", ctx, b, base)
		t.Logf("bound reentry readiness shared-prefix loss negative: the real copied-prefix loss made the Use and the owner transaction refuse through the actual use-failure stage, reconstruction refused, nothing was published and the complete baseline stayed unchanged")
	}()

	// N7: authentic observer failure immediately before the arbitration: the
	// permanent loss is latched through the real evidence path, the REAL
	// arbitration refuses, nothing is published.
	func() {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		stage := &borrowedBoundReentryReadinessStage{key: b.fixture.guardKey, expectedOperation: b.fixture.operation}
		outcome := borrowedBoundReentryReadinessRun(ctx, t, b, fresh, stage, &borrowedReplacementOwnerTailHooks{
			atArbitration: func(tailCtx context.Context, pump *borrowedReplacementAutonomousPump, _ func(), _ pgx.Tx) error {
				var terminated bool
				termCtx, cancelTerm := context.WithTimeout(tailCtx, borrowedOwnerRotationQueryBudget)
				termErr := b.fixture.controlPool.QueryRow(termCtx, `SELECT pg_terminate_backend($1)`, pump.observer.reg.state.backendPID).Scan(&terminated)
				cancelTerm()
				if termErr != nil || !terminated {
					t.Fatalf("bound reentry readiness observer termination refused: terminated=%t err=%v", terminated, termErr)
				}
				select {
				case <-pump.lossAcked:
				case <-tailCtx.Done():
					t.Fatalf("bound reentry readiness observer loss was not acknowledged before the hook context ended")
				}
				return nil
			},
		})
		readiness := newBorrowedBoundReentryReadiness()
		borrowedBoundReentryReadinessAssertUnpublished(t, "bound reentry readiness observer failure", b, fresh, stage, outcome, readiness)
		if outcome.pump == nil || outcome.pump.lossReasonNow() == "" {
			t.Fatal("bound reentry readiness observer failure was not permanently latched")
		}
		if !errors.Is(outcome.flow.ownerErr, errOwnerTailArbitration) {
			t.Fatalf("observer-failure refusal is not the ACTUAL arbitration stage: %v", outcome.flow.ownerErr)
		}
		if outcome.state.eligibleNow() || outcome.flow.state.compositePublished() {
			t.Fatal("observer-failure control produced eligibility/composite")
		}
		borrowedBoundReentryReadinessAssertBaseline(t, "bound reentry readiness observer failure", ctx, b, base)
		t.Logf("bound reentry readiness observer-failure negative: the authentic observer loss was acknowledged and permanently latched, the REAL arbitration refused and nothing was published")
	}()

	// N8: cancellation immediately before the final arbitration: the REAL
	// arbitration refuses, the join stays bounded and nothing is published.
	func() {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		stage := &borrowedBoundReentryReadinessStage{key: b.fixture.guardKey, expectedOperation: b.fixture.operation}
		var canceled atomic.Bool
		laneCtx, cancelLane := context.WithCancel(ctx)
		defer cancelLane()
		var completionProven bool
		outcome := borrowedBoundReentryReadinessRun(laneCtx, t, b, fresh, stage, &borrowedReplacementOwnerTailHooks{
			atArbitration: func(_ context.Context, _ *borrowedReplacementAutonomousPump, cancel func(), _ pgx.Tx) error {
				if cancel == nil {
					t.Fatal("bound reentry readiness cancellation control has no lane cancel")
				}
				canceled.Store(true)
				cancel()
				return nil
			},
		})
		t.Cleanup(func() {
			cancelLane()
			if completionProven || outcome == nil || outcome.flow == nil {
				return
			}
			select {
			case <-outcome.flow.useCompletion:
				completionProven = true
			case <-time.After(30 * time.Second):
				t.Errorf("bound reentry readiness cancellation use completion is unknown after the bounded unwind")
			}
		})
		select {
		case <-outcome.flow.useCompletion:
			completionProven = true
		case <-time.After(30 * time.Second):
			t.Fatal("bound reentry readiness cancellation use completion was not proven")
		}
		readiness := newBorrowedBoundReentryReadiness()
		borrowedBoundReentryReadinessAssertUnpublished(t, "bound reentry readiness cancellation", b, fresh, stage, outcome, readiness)
		if !canceled.Load() {
			t.Fatal("bound reentry readiness cancellation was not exercised")
		}
		if !errors.Is(outcome.flow.ownerErr, errOwnerTailArbitration) {
			t.Fatalf("cancellation refusal is not the ACTUAL arbitration stage: %v", outcome.flow.ownerErr)
		}
		if outcome.pumpJoinErr != nil || outcome.flow.terminationUnknown {
			t.Fatalf("cancellation control did not join bounded: pumpJoin=%v terminationUnknown=%t", outcome.pumpJoinErr, outcome.flow.terminationUnknown)
		}
		if outcome.state.eligibleNow() || outcome.flow.state.compositePublished() {
			t.Fatal("cancellation control produced eligibility/composite")
		}
		borrowedBoundReentryReadinessAssertBaseline(t, "bound reentry readiness cancellation", ctx, b, base)
		t.Logf("bound reentry readiness cancellation negative: the cancel was observed immediately before the final arbitration, the REAL arbitration refused, cancel->release->bounded join completed and nothing was published")
	}()

	// N9: post-COMMIT observed loss: the owner transaction acknowledged the
	// COMMIT, then the authentic observer loss permanently latched and the
	// terminal composite refused; nothing is published.
	func() {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		stage := &borrowedBoundReentryReadinessStage{key: b.fixture.guardKey, expectedOperation: b.fixture.operation}
		outcome := borrowedBoundReentryReadinessRun(ctx, t, b, fresh, stage, &borrowedReplacementOwnerTailHooks{
			adapter: func(_ error, pump *borrowedReplacementAutonomousPump, _ func()) error {
				var terminated bool
				termCtx, cancelTerm := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
				termErr := b.fixture.controlPool.QueryRow(termCtx, `SELECT pg_terminate_backend($1)`, pump.observer.reg.state.backendPID).Scan(&terminated)
				cancelTerm()
				if termErr != nil || !terminated {
					t.Fatalf("bound reentry readiness post-COMMIT observer termination refused: terminated=%t err=%v", terminated, termErr)
				}
				select {
				case <-pump.lossAcked:
				case <-time.After(30 * time.Second):
					t.Fatalf("bound reentry readiness post-COMMIT loss was not acknowledged")
				}
				return errors.New("bound reentry readiness loss injected after the acknowledged COMMIT")
			},
		})
		readiness := newBorrowedBoundReentryReadiness()
		borrowedBoundReentryReadinessAssertUnpublished(t, "bound reentry readiness post-COMMIT loss", b, fresh, stage, outcome, readiness)
		if outcome.flow.ownerErr != nil {
			t.Fatalf("post-COMMIT loss case did not acknowledge the COMMIT: %v", outcome.flow.ownerErr)
		}
		if outcome.pump == nil || outcome.pump.lossReasonNow() == "" {
			t.Fatal("post-COMMIT loss was not permanently latched")
		}
		if outcome.flow.state.compositePublished() || outcome.flow.state.lostNow() != true {
			t.Fatalf("post-COMMIT terminal state: composite=%t lost=%t", outcome.flow.state.compositePublished(), outcome.flow.state.lostNow())
		}
		if outcome.flow.decisionErr == nil {
			t.Fatal("post-COMMIT loss did not refuse the terminal decision")
		}
		// The existing provisional owner-tail eligibility may legitimately have
		// been granted inside the acknowledged owner transaction; it is NOT the
		// lane readiness witness and must never be reusable after the refusal.
		if outcome.state.eligibleNow() {
			if !outcome.state.consumeEligibility() || outcome.state.consumeEligibility() {
				t.Fatal("post-COMMIT loss left a reusable provisional owner-tail eligibility")
			}
		}
		borrowedBoundReentryReadinessAssertBaseline(t, "bound reentry readiness post-COMMIT loss", ctx, b, base)
		t.Logf("bound reentry readiness post-COMMIT loss negative: the COMMIT was acknowledged, the authentic post-COMMIT observed loss was permanently latched, the terminal composite refused and nothing was published")
	}()

	// N10: unknown worker completion: the REAL bounded worker stall with no
	// completion-channel proof blocks the supplied-tx stage and any publication,
	// retirement/reuse refuse, and the post-release completion proof resolves
	// the UNKNOWN without ever publishing.
	func() {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		stage := &borrowedBoundReentryReadinessStage{key: b.fixture.guardKey, expectedOperation: b.fixture.operation}
		workerRelease := make(chan struct{})
		var workerReleaseOnce sync.Once
		releaseWorker := func() { workerReleaseOnce.Do(func() { close(workerRelease) }) }
		t.Cleanup(releaseWorker)
		workerEntered := make(chan struct{})
		var workerEnteredOnce sync.Once
		laneCtx, cancelLane := context.WithCancel(ctx)
		defer cancelLane()
		outcome := borrowedBoundReentryReadinessRun(laneCtx, t, b, fresh, stage, &borrowedReplacementOwnerTailHooks{
			useJoinBound: 2 * time.Second,
			useWorkerStall: func(_ context.Context) {
				workerEnteredOnce.Do(func() { close(workerEntered) })
				<-workerRelease
			},
			postProbe: func(_ context.Context, _ *borrowedReplacementAutonomousPump, _ func()) error {
				return errors.New("bound reentry readiness worker-stall control: window stopped before its join")
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
				t.Errorf("bound reentry readiness worker completion is unknown after the unwind release")
			}
		})
		select {
		case <-workerEntered:
		case <-time.After(60 * time.Second):
			t.Fatalf("bound reentry readiness worker stall never acknowledged its entry")
		}
		if got := stage.invocationsNow(); got != 0 {
			t.Fatalf("bound reentry readiness worker-stall supplied-tx stage invocations=%d, want ZERO while completion is unproven", got)
		}
		readiness := newBorrowedBoundReentryReadiness()
		borrowedBoundReentryReadinessAssertUnpublished(t, "bound reentry readiness worker-unknown", b, fresh, stage, outcome, readiness)
		cancelLane()
		releaseWorker()
		select {
		case <-outcome.flow.useCompletion:
			completionProven = true
		case <-time.After(30 * time.Second):
			t.Fatal("bound reentry readiness worker completion was not proven after the release")
		}
		if !outcome.flow.terminationUnknown {
			t.Fatal("worker-stall control did not exercise the REAL join-failure path")
		}
		if outcome.flow.state.compositePublished() {
			t.Fatal("worker-stall control published the composite")
		}
		if err := borrowedReplacementOwnerTailRetireGuarded(t, ctx, b, outcome); err == nil {
			t.Fatal("worker-stall control retired while completion was UNKNOWN")
		}
		reuseCtx, cancelReuse := context.WithTimeout(ctx, 30*time.Second)
		reuseErr := outcome.flow.reg.Use(reuseCtx)
		copied := *outcome.flow.reg
		copyErr := copied.Use(reuseCtx)
		cancelReuse()
		if reuseErr == nil || copyErr == nil {
			t.Fatal("worker-stall control allowed reuse after the UNKNOWN completion")
		}
		borrowedBoundReentryReadinessAssertBaseline(t, "bound reentry readiness worker-unknown", ctx, b, base)
		t.Logf("bound reentry readiness worker-unknown negative: the REAL bounded worker stall kept the supplied-tx stage at zero invocations, nothing was published, retirement/reuse refused, and completion was proven through the ACTUAL completion channel after cancel-before-release")
	}()
}
