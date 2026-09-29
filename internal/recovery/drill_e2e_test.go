//go:build drill

// drill_e2e_test.go is T058 [US6]: the independent end-to-end drill of the
// real flow
//
//	generate backup -> external progress (a real confirmed payment on Anvil, a
//	signed/broadcast record, downstream consumption) -> restore the old data ->
//	verification finds the gaps and unsafe resumption is refused -> with
//	evidence, approvals and the derived gate, one named capability is released.
//
// Every phase runs the real entry point (real pg_dump/pg_restore through the
// pinned container, real PostgreSQL control store, real Anvil). The drill
// records the S12 timing scopes separately (recovery point, db restore seconds,
// verification seconds, per-capability release seconds, backup lag, uncovered
// interval) and archives the run under the drill evidence directory.
//
// The channel never writes approval/release/gap/isolation state by SQL: every
// decision row below is produced by the exported entry points and re-derived by
// the single gate evaluation (quickstart §4 anti-cheat discipline). Zero
// duplicate payments and zero wrong event effects are asserted by full-content
// fingerprints of the effect tables of both the live database and the restored
// target, before and after the recovery flow.
//
// The second scenario is the conservative counter-case the spec requires: a
// gap that cannot be closed keeps the affected capability paused (no
// "release everything to pass the drill" shortcut).
package recovery_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/xtianxx/txharbor/internal/recovery"
)

// TestT058DrillE2EBackupAdvanceRestoreRefuseThenRelease is the positive drill
// flow: the restored environment starts refused (the external lead is a real
// gap), one named capability is released with evidence and approvals, and the
// whole flow leaves both databases without a single duplicated payment or
// wrong event effect.
func TestT058DrillE2EBackupAdvanceRestoreRefuseThenRelease(t *testing.T) {
	requireDrillLocalPGRestore(t)
	env := newDrillEnv(t, true)

	// --- phase 0: controlled live data (data preparation, not a source) ----
	env.seedLiveBusinessState()
	atBackup := drillFingerprint(t, env.data)

	// --- phase 1: real backup ---------------------------------------------
	backup := env.backup()
	if backup.Manifest.Verification.State != recovery.VerificationUnverified {
		t.Fatalf("a fresh backup must be unverified, got %s", backup.Manifest.Verification.State)
	}
	recoveryPointAt := drillManifestWallClock(t, backup)
	// The lag is measured on the source database's own clock (the manifest
	// recovery point is written there) so a host/container clock offset can
	// never fabricate a value.
	backupLagSeconds := env.dbNow(env.dataDSN).Sub(recoveryPointAt).Seconds()

	// --- phase 2: external progress after the recovery point --------------
	advance := env.advanceExternally()
	liveAfterAdvance := drillFingerprint(t, env.data)
	if atBackup["chain_blocks"] == liveAfterAdvance["chain_blocks"] {
		t.Fatal("the external advance did not change the live database; the fixture proves nothing")
	}

	// --- phase 3: isolated verify-backup (real pg_restore) ----------------
	verifyTargetDSN := env.createDatabase("verify")
	env.verifyBackup(backup.ManifestPath, verifyTargetDSN, env.operation("verify-backup"))
	if got := env.controlEvidenceCount("backup_manifest", backup.Manifest.BackupID); got != 1 {
		t.Fatalf("backup_manifest evidence rows = %d, want 1 (verified conclusion is control-store bound)", got)
	}

	// --- phase 4: real restore of the old data ----------------------------
	recoveredDSN := env.createDatabase("recovered")
	recovered := env.openPool(recoveredDSN)
	restoreStarted := time.Now()
	env.restore(backup.ManifestPath, recoveredDSN, env.operation("restore"))
	dbRestoreSeconds := time.Since(restoreStarted).Seconds()
	restoreStartMarker := env.restoreStartedAt()
	if got := env.controlEvidenceCount("restore_probe", backup.Manifest.BackupID); got != 1 {
		t.Fatalf("restore_probe evidence rows = %d, want 1 (a real restore happened)", got)
	}

	// --- phase 5: verification finds the external lead and the gaps -------
	verifyStarted := time.Now()
	batch := env.verify(recovered, env.operation("verify"))
	verificationSeconds := time.Since(verifyStarted).Seconds()
	verificationCompletedAt := time.Now().UTC()
	drillAssertAllCategoriesObserved(t, batch)
	drillAssertV1NotConsistent(t, batch)

	// The restored target is the recovery point: effect rows equal the
	// backup-time snapshot and the post-recovery-point facts are absent.
	recoveredAfterVerify := drillFingerprint(t, recovered)
	drillAssertFingerprintEqual(t, atBackup, recoveredAfterVerify, "restored target vs recovery point")
	drillAssertPostAdvanceAbsent(t, env, recovered, advance)

	gaps := env.openGaps()
	if len(gaps) == 0 {
		t.Fatal("verification over a restored recovery point with external progress must establish an open gap")
	}
	drillRequireGapFor(t, gaps, recovery.CapabilityExistingWithdrawalRecovery)

	// --- phase 6: refuse unsafe resumption (the gap blocks the capability) --
	// Every isolation verification is a generation-advancing write, so all of
	// them run before any approval/release binds the current generation.
	env.checklistVerified(recovery.CapabilityChainScan)
	env.checklistVerified(recovery.CapabilityExistingWithdrawalRecovery)
	env.checklistVerified(recovery.CapabilityNewWithdrawalCreation)
	env.checklistVerified(recovery.CapabilityQuery)
	gate := env.gate()

	// The verification gap blocks the dependency root (chain_scan) itself:
	// isolation proven, approval present, a release decision appended — and the
	// derived gate still refuses with gap_open. A release decision is never a
	// resumption.
	env.approve(recovery.CapabilityChainScan, "auth:approver")
	chainRelease := env.release(gate, recovery.CapabilityChainScan)
	if chainRelease.ReleaseID == "" {
		t.Fatal("the chain_scan release decision was not recorded")
	}
	chainBlocked := env.admit(gate, recovery.CapabilityChainScan)
	if chainBlocked.Allowed {
		t.Fatal("chain_scan was admitted although verification left a blocking gap")
	}
	if chainBlocked.RefusalClass != recovery.RefusalGapOpen {
		t.Fatalf("chain_scan refusal class = %s (%s), want gap_open", chainBlocked.RefusalClass, chainBlocked.Reason)
	}

	// The gap-affected withdrawal capability is blocked too; its refusal is
	// the dependency chain pointing at the gap (fail-closed through the
	// frozen dependency matrix, never a silent pass).
	env.approve(recovery.CapabilityExistingWithdrawalRecovery, "auth:approver")
	env.approve(recovery.CapabilityExistingWithdrawalRecovery, "auth:approver2")
	existingRelease := env.release(gate, recovery.CapabilityExistingWithdrawalRecovery)
	if existingRelease.ReleaseID == "" {
		t.Fatal("the existing_withdrawal_recovery release decision was not recorded")
	}
	existingBlocked := env.admit(gate, recovery.CapabilityExistingWithdrawalRecovery)
	if existingBlocked.Allowed {
		t.Fatal("a gap-affected capability was admitted; unsafe resumption was not refused")
	}
	if existingBlocked.RefusalClass != recovery.RefusalCapabilityDependencyClosed ||
		!strings.Contains(existingBlocked.Reason, string(recovery.RefusalGapOpen)) {
		t.Fatalf("existing_withdrawal_recovery refusal = %s (%s), want capability_dependency_closed tracing to gap_open",
			existingBlocked.RefusalClass, existingBlocked.Reason)
	}

	// The entry capability stays closed behind its dependency.
	newCreation := env.admit(gate, recovery.CapabilityNewWithdrawalCreation)
	if newCreation.Allowed {
		t.Fatal("new_withdrawal_creation was admitted although its recovery path is blocked")
	}
	if newCreation.RefusalClass != recovery.RefusalCapabilityDependencyClosed {
		t.Fatalf("new_withdrawal_creation refusal class = %s (%s), want capability_dependency_closed",
			newCreation.RefusalClass, newCreation.Reason)
	}

	// --- phase 7: evidence-backed release of the named safe capability ----
	queryApproval := env.approve(recovery.CapabilityQuery, "auth:approver")
	if queryApproval.ApprovalID == "" {
		t.Fatalf("the query approval was not recorded: %+v", queryApproval)
	}
	queryRelease := env.release(gate, recovery.CapabilityQuery)
	if queryRelease.ReleaseID == "" {
		t.Fatal("the query release decision was not recorded")
	}
	if d := env.admit(gate, recovery.CapabilityQuery); !d.Allowed {
		t.Fatalf("query admission refused (%s: %s) although its evidence, approval and release are in place",
			d.RefusalClass, d.Reason)
	}
	// Anything without its own evidence/approval/release stays refused.
	if d := env.admit(gate, recovery.CapabilityDepositConfirmation); d.Allowed {
		t.Fatal("deposit_confirmation was admitted without a release decision")
	}

	// --- phase 8: zero duplicate payments / zero wrong event effects ------
	drillAssertFingerprintEqual(t, liveAfterAdvance, drillFingerprint(t, env.data), "live database after the recovery flow")
	drillAssertFingerprintEqual(t, recoveredAfterVerify, drillFingerprint(t, recovered), "restored target after decisions")
	drillAssertPendingEventState(t, env.data, 6)
	drillAssertPendingEventState(t, recovered, 5)

	// --- phase 9: record the run with the separate timing scopes ----------
	// Only the capability whose admission the derived gate actually allowed is
	// a measured release; the refused release decisions above are recorded as
	// refusals, never as resumption measurements.
	firstReleaseAt := env.releaseCreatedAt(queryRelease.ReleaseID)
	capabilityReleases := map[recovery.Capability]float64{
		recovery.CapabilityQuery: env.releaseSecondsOf(queryRelease.ReleaseID, restoreStartMarker),
	}
	runID := uuid.NewString()
	evidencePath := drillWriteEvidence(t, "t058-e2e", map[string]any{
		"drill_id":               runID,
		"scenario":               string(recovery.DrillScenarioFullRecovery),
		"purpose":                "local drill inputs only; not production thresholds; T000-P stays OPEN",
		"backup_id":              backup.Manifest.BackupID,
		"recovery_point":         json.RawMessage(drillRecoveryPointJSON(t, backup)),
		"external_advance":       advance,
		"db_restore_seconds":     dbRestoreSeconds,
		"verification_seconds":   verificationSeconds,
		"verification_completed": verificationCompletedAt.Format(time.RFC3339Nano),
		"capability_release_seconds": map[string]any{
			string(recovery.CapabilityQuery): capabilityReleases[recovery.CapabilityQuery],
		},
		"release_refusals": map[string]string{
			string(recovery.CapabilityChainScan):                  string(chainBlocked.RefusalClass),
			string(recovery.CapabilityExistingWithdrawalRecovery): string(existingBlocked.RefusalClass),
			string(recovery.CapabilityNewWithdrawalCreation):      string(newCreation.RefusalClass),
		},
		"backup_lag":         json.RawMessage(drillMeasuredLag(backupLagSeconds, "source-database clock at backup completion minus the manifest recovery point wall clock")),
		"uncovered_interval": json.RawMessage(drillMeasuredLag(firstReleaseAt.Sub(recoveryPointAt).Seconds(), "manifest recovery point wall clock until the first evidence-backed capability release")),
		"gaps":               drillGapSummary(t, env),
		"non_claims": []string{
			"local drill inputs only; not production thresholds",
			"a reachable database, a completed restore or a single timing scope is never an RTO measurement",
			"no cross-system exactly-once claim; no external ledger consistency claim",
		},
	})
	run, err := recovery.RecordDrillRun(env.ctx, env.store, recovery.DrillRunInput{
		InstanceID:               env.instanceID,
		DrillID:                  runID,
		Scenario:                 recovery.DrillScenarioFullRecovery,
		RecoveryPoint:            drillRecoveryPointJSON(t, backup),
		DBRestoreSeconds:         &dbRestoreSeconds,
		VerificationSeconds:      &verificationSeconds,
		CapabilityReleaseSeconds: capabilityReleases,
		BackupLag:                drillMeasuredLag(backupLagSeconds, "source-database clock at backup completion minus the manifest recovery point wall clock"),
		UncoveredInterval:        drillMeasuredLag(firstReleaseAt.Sub(recoveryPointAt).Seconds(), "manifest recovery point wall clock until the first evidence-backed capability release"),
		ConstraintsConfigured:    false,
		TestInputs:               []byte(`{"purpose":"local drill inputs only; not production thresholds","unconfigured_constraints":["rpo_target","rto_target","backup_frequency","retention"]}`),
		GapCounts:                drillGapCountsJSON(t, env),
		Result:                   recovery.DrillResultRefusedSafe,
		LogRef:                   evidencePath,
		Actor:                    "deploy:executor",
		OperationID:              env.operation("record-drill"),
	})
	if err != nil {
		t.Fatalf("RecordDrillRun: %v", err)
	}
	if run.Recorded || run.DrillID != runID {
		t.Fatalf("drill run = %+v, want a freshly recorded run %s", run, runID)
	}
	if _, ok := run.SafeResumptionSeconds(); ok {
		t.Fatal("a partial capability set must never yield an end-to-end safe-resumption duration")
	}

	// The conservative result must stay observable: the run classifies the
	// open gaps, the refused capabilities stay refused, and the measured
	// timings are recorded as separate scopes (never a single RTO figure).
	if run.Result != recovery.DrillResultRefusedSafe {
		t.Fatalf("drill result = %s, want refused_safe", run.Result)
	}
	if run.DBRestoreSeconds == nil || run.VerificationSeconds == nil {
		t.Fatalf("separate timing scopes did not round-trip: %+v", run)
	}
}

// TestT058DrillGapCannotBeClosedStaysPaused is the conservative counter-case:
// when the required external evidence cannot be produced, closing, timeout
// notes, exhausted attempts and a human acknowledgement never close the gap —
// the affected capability stays paused and the drill records a safe refusal.
func TestT058DrillGapCannotBeClosedStaysPaused(t *testing.T) {
	requireDrillLocalPGRestore(t)
	env := newDrillEnv(t, true)
	env.seedLiveBusinessState()
	backup := env.backup()
	env.advanceExternally()

	verifyTargetDSN := env.createDatabase("verify2")
	env.verifyBackup(backup.ManifestPath, verifyTargetDSN, env.operation("verify-backup-2"))

	recoveredDSN := env.createDatabase("recovered2")
	recovered := env.openPool(recoveredDSN)
	env.restore(backup.ManifestPath, recoveredDSN, env.operation("restore-2"))
	batch := env.verify(recovered, env.operation("verify-2"))
	if len(batch.Gaps) == 0 {
		t.Fatal("verification established no gap; the pause scenario cannot be exercised")
	}

	gaps, err := recovery.NewGaps(env.store)
	if err != nil {
		t.Fatalf("NewGaps: %v", err)
	}
	gap := drillGapFor(t, env.openGaps(), recovery.CapabilityExistingWithdrawalRecovery)

	// An empty closure is refused: no evidence, no closure.
	if _, err := gaps.Close(env.ctx, recovery.CloseGapRequest{
		InstanceID: env.instanceID, GapID: gap.GapID, ClosureEvidence: nil,
		Actor: "deploy:executor", OperationID: env.operation("close-empty"),
	}); err == nil {
		t.Fatal("a gap was closed without new evidence")
	}
	if got := drillGapState(t, env, gap.GapID); got != recovery.GapStateOpen {
		t.Fatalf("gap state after the refused closure = %s, want open", got)
	}

	// Timeout, exhausted attempts and human acknowledgement are audit notes,
	// never closure (FR-019/FR-032).
	for _, kind := range []recovery.GapNoteKind{
		recovery.GapNoteTimeout, recovery.GapNoteAttemptsExhausted, recovery.GapNoteAcknowledged,
	} {
		if _, err := gaps.Note(env.ctx, recovery.GapNoteRequest{
			InstanceID: env.instanceID, GapID: gap.GapID, Kind: kind,
			Reason: "drill: the external evidence could not be produced (local test input)",
			Actor:  "deploy:executor", OperationID: env.operation("note"),
		}); err != nil {
			t.Fatalf("gap note %s: %v", kind, err)
		}
	}
	if got := drillGapState(t, env, gap.GapID); got != recovery.GapStateOpen {
		t.Fatalf("gap state after the audit notes = %s, want open (knowledge is not evidence)", got)
	}

	// Bounded read-only review: the configured budget is consumed by a real
	// pass and a second pass is refused without changing any state (F13).
	review := recovery.GapReviewRequest{
		InstanceID: env.instanceID, GapID: gap.GapID,
		Budget: recovery.ReviewBudget{MaxReads: 1},
		Actor:  "auth:verifier", OperationID: env.operation("review"),
	}
	if _, err := gaps.Review(env.ctx, review); err != nil {
		t.Fatalf("bounded review: %v", err)
	}
	review.OperationID = env.operation("review-again")
	if _, err := gaps.Review(env.ctx, review); err == nil {
		t.Fatal("a review beyond the configured budget was accepted")
	}
	if got := drillGapState(t, env, gap.GapID); got != recovery.GapStateOpen {
		t.Fatalf("gap state after the exhausted review = %s, want open", got)
	}

	// The verification gap is a complete FR-019 handling package: object and
	// range, timeline, existing evidence, required external evidence and risk.
	if gap.ObjectKey == "" || len(gap.Scope) == 0 || len(gap.Timeline) == 0 ||
		len(gap.ExistingEvidence) == 0 || len(gap.RequiredEvidence) == 0 || len(gap.Risk) == 0 {
		t.Fatalf("the verification gap is not a complete handling package: %+v", gap)
	}

	// A responsibility-carrying gap is opened for the same missing external
	// evidence through the real entry point (the owner is recorded, then the
	// escalation preserves it; the verification gap itself stays untouched).
	owned, err := gaps.Open(env.ctx, recovery.OpenGapRequest{
		InstanceID:       env.instanceID,
		ObjectKey:        "payment_intent:drill-req-1",
		Scope:            []byte(`{"chain_id":31337,"object":"payment_intent:drill-req-1"}`),
		Timeline:         []byte(`{"from":"recovery point","to":"drill decision","state":"unknown"}`),
		ExistingEvidence: []byte(`{"withdrawal_request":"drill-req-1","payment_intent":null,"note":"the restored recovery point has a request with no provable intent"}`),
		RequiredEvidence: []byte(`{"external":["the signed payment intent and its broadcast receipt produced after the recovery point"]}`),
		Risk:             []byte(`{"unproven_external_effect":true,"may_amplify":"new_withdrawal_creation"}`),
		AffectedCapabilities: []recovery.Capability{
			recovery.CapabilityExistingWithdrawalRecovery,
		},
		Owner:       "person-owner-1",
		Actor:       "deploy:executor",
		OperationID: env.operation("gap-open-owned"),
	})
	if err != nil {
		t.Fatalf("Gaps.Open: %v", err)
	}
	if owned.Owner != "person-owner-1" {
		t.Fatalf("the opened gap lost its responsibility holder: %+v", owned)
	}
	escalated, err := gaps.Escalate(env.ctx, recovery.EscalateGapRequest{
		InstanceID: env.instanceID, GapID: owned.GapID,
		EscalationRef: "drill://escalation/owner-person-1",
		Actor:         "deploy:executor", Reason: "evidence cannot be produced in this drill",
		OperationID: env.operation("escalate"),
	})
	if err != nil {
		t.Fatalf("Escalate: %v", err)
	}
	if escalated.State != recovery.GapStateEscalated || escalated.EscalationRef == "" {
		t.Fatalf("escalated gap = %+v, want state=escalated with the responsibility reference", escalated)
	}
	if escalated.Owner != "person-owner-1" {
		t.Fatalf("the escalated gap lost its responsibility holder: %+v", escalated)
	}
	// The original verification gap is untouched by the follow-up package.
	if got := drillGapState(t, env, gap.GapID); got != recovery.GapStateOpen {
		t.Fatalf("verification gap state = %s, want open", got)
	}

	// The affected capability stays paused — with isolation proven and a
	// release decision appended, exactly as in the positive flow.
	gate := env.gate()
	env.checklistVerified(recovery.CapabilityChainScan)
	env.checklistVerified(recovery.CapabilityExistingWithdrawalRecovery)
	env.approve(recovery.CapabilityChainScan, "auth:approver")
	env.release(gate, recovery.CapabilityChainScan)
	env.approve(recovery.CapabilityExistingWithdrawalRecovery, "auth:approver")
	env.approve(recovery.CapabilityExistingWithdrawalRecovery, "auth:approver2")
	env.release(gate, recovery.CapabilityExistingWithdrawalRecovery)
	decision := env.admit(gate, recovery.CapabilityExistingWithdrawalRecovery)
	if decision.Allowed {
		t.Fatal("an escalated, unclosable gap still admitted the affected capability")
	}
	if decision.RefusalClass != recovery.RefusalCapabilityDependencyClosed ||
		!strings.Contains(decision.Reason, string(recovery.RefusalGapOpen)) {
		t.Fatalf("refusal = %s (%s), want capability_dependency_closed tracing to gap_open",
			decision.RefusalClass, decision.Reason)
	}
	if d := env.admit(gate, recovery.CapabilityChainScan); d.Allowed {
		t.Fatal("the dependency root was admitted although the verification gap blocks it")
	}

	// A safe refusal is recorded; the drill never force-releases the rest.
	drillWriteEvidence(t, "t058-gap-paused", map[string]any{
		"scenario":           string(recovery.DrillScenarioFullRecovery),
		"verification_gap":   gap.GapID,
		"verification_state": string(recovery.GapStateOpen),
		"owned_gap":          owned.GapID,
		"gap_state":          string(escalated.State),
		"owner":              escalated.Owner,
		"escalation_ref":     escalated.EscalationRef,
		"refusal_class":      string(decision.RefusalClass),
		"non_claims": []string{
			"timeout, exhausted attempts and human acknowledgement are not closure",
			"local drill inputs only; not production thresholds",
		},
		"result":            string(recovery.DrillResultRefusedSafe),
		"capability_states": drillCapabilityStates(t, env),
	})
}

// ---------------------------------------------------------------------------
// Assertions and small helpers (drill-prefixed: no collision with the other
// tagged layers of this package).
// ---------------------------------------------------------------------------

func drillManifestWallClock(t *testing.T, backup recovery.BackupResult) time.Time {
	t.Helper()
	at, err := time.Parse(time.RFC3339, backup.Manifest.RecoveryPoint.WallClock)
	if err != nil {
		t.Fatalf("parse manifest recovery point wall clock %q: %v", backup.Manifest.RecoveryPoint.WallClock, err)
	}
	return at
}

func drillRecoveryPointJSON(t *testing.T, backup recovery.BackupResult) []byte {
	t.Helper()
	raw, err := json.Marshal(backup.Manifest.RecoveryPoint)
	if err != nil {
		t.Fatalf("encode recovery point: %v", err)
	}
	return raw
}

func drillMeasuredLag(seconds float64, method string) []byte {
	raw, err := json.Marshal(map[string]any{
		"state":   "measured",
		"seconds": seconds,
		"method":  method,
		"purpose": "local drill measurement only; not a production RPO claim",
	})
	if err != nil {
		panic(err)
	}
	return raw
}

func drillAssertAllCategoriesObserved(t *testing.T, batch recovery.VerificationBatch) {
	t.Helper()
	seen := make(map[recovery.VerificationCategory]int)
	for _, item := range batch.Items {
		if !item.Conclusion.Known() {
			t.Fatalf("persisted item %s has conclusion %q outside the closed set", item.ItemID, item.Conclusion)
		}
		if item.Conclusion != recovery.ConclusionConsistent && item.Reason == "" {
			t.Fatalf("item %s (%s) is %s without a reason", item.ItemID, item.Category, item.Conclusion)
		}
		seen[item.Category]++
	}
	for _, category := range recovery.KnownVerificationCategories() {
		if seen[category] == 0 {
			t.Fatalf("no %s verification item was produced from the real adapters (seen: %v)", category, seen)
		}
	}
}

func drillAssertV1NotConsistent(t *testing.T, batch recovery.VerificationBatch) {
	t.Helper()
	found := false
	for _, item := range batch.Items {
		if item.Category != recovery.VerificationV1 {
			continue
		}
		found = true
		if item.Conclusion == recovery.ConclusionConsistent {
			t.Fatalf("V1 reported consistent while the chain leads the restored frontier: %+v", item)
		}
	}
	if !found {
		t.Fatal("no V1 item was produced for the external-lead case")
	}
}

// drillAssertPostAdvanceAbsent proves the restore really rolled the recovered
// environment back behind the external progress.
func drillAssertPostAdvanceAbsent(t *testing.T, env *drillEnv, recovered *pgxpool.Pool, advance drillExternalAdvance) {
	t.Helper()
	counts := func(pool *pgxpool.Pool, query string) int {
		t.Helper()
		var n int
		if err := pool.QueryRow(env.ctx, query).Scan(&n); err != nil {
			t.Fatalf("count (%s): %v", query, err)
		}
		return n
	}
	if got := counts(recovered, `SELECT count(*) FROM signature_results`); got != 0 {
		t.Fatalf("the restored target carries %d signed/broadcast results of the live instance; the restore did not roll back", got)
	}
	if got := counts(recovered, `SELECT count(*) FROM consumer_inbox`); got != 0 {
		t.Fatalf("the restored target carries %d downstream consumption rows; the restore did not roll back", got)
	}
	if got := counts(recovered, `SELECT count(*) FROM erc20_transfer_logs`); got != 0 {
		t.Fatalf("the restored target carries %d transfer logs observed after the recovery point", got)
	}
	if got := counts(env.data, `SELECT count(*) FROM signature_results`); got != 1 {
		t.Fatalf("the live database lost its signed/broadcast record (got %d)", got)
	}
	if got := counts(env.data, `SELECT count(*) FROM erc20_transfer_logs`); got != 1 {
		t.Fatalf("the live database lost the observed payment transfer (got %d)", got)
	}
	var height int
	if err := recovered.QueryRow(env.ctx,
		`SELECT height FROM indexer_checkpoint WHERE chain_id = $1`, drillChainID).Scan(&height); err != nil {
		t.Fatalf("read restored checkpoint: %v", err)
	}
	if height != 1 {
		t.Fatalf("restored indexer checkpoint height = %d, want the recovery point (1)", height)
	}
	if advance.BlockNumber == 0 {
		t.Fatal("the external payment did not produce a real block")
	}
}

func drillRequireGapFor(t *testing.T, gaps []recovery.Gap, capability recovery.Capability) {
	t.Helper()
	if len(drillGapsFor(gaps, capability)) == 0 {
		t.Fatalf("no blocking gap names capability %s (gaps: %s)", capability, drillGapKeys(gaps))
	}
}

func drillGapFor(t *testing.T, gaps []recovery.Gap, capability recovery.Capability) recovery.Gap {
	t.Helper()
	matches := drillGapsFor(gaps, capability)
	if len(matches) == 0 {
		t.Fatalf("no blocking gap names capability %s (gaps: %s)", capability, drillGapKeys(gaps))
	}
	return matches[0]
}

func drillGapsFor(gaps []recovery.Gap, capability recovery.Capability) []recovery.Gap {
	var out []recovery.Gap
	for _, gap := range gaps {
		for _, affected := range gap.AffectedCapabilities {
			if affected == capability {
				out = append(out, gap)
				break
			}
		}
	}
	return out
}

func drillGapKeys(gaps []recovery.Gap) string {
	out := make([]string, 0, len(gaps))
	for _, gap := range gaps {
		out = append(out, fmt.Sprintf("%s(%s)", gap.GapID, gap.ObjectKey))
	}
	return fmt.Sprint(out)
}

func drillGapState(t *testing.T, env *drillEnv, gapID string) recovery.GapState {
	t.Helper()
	var state string
	if err := env.ctrl.QueryRow(env.ctx,
		`SELECT state FROM recovery_gap WHERE instance_id = $1 AND gap_id = $2`,
		env.instanceID, gapID).Scan(&state); err != nil {
		t.Fatalf("read gap %s state: %v", gapID, err)
	}
	return recovery.GapState(state)
}

// drillAssertPendingEventState pins the zero-wrong-event-effect invariant: the
// historical outbox event is still unpublished (pending), and the consumer
// progress of each environment stayed where the recovery flow found it.
func drillAssertPendingEventState(t *testing.T, pool *pgxpool.Pool, wantOffset int64) {
	t.Helper()
	var pending int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM outbox_events WHERE publish_state = 'pending'`).Scan(&pending); err != nil {
		t.Fatalf("count pending outbox: %v", err)
	}
	var published int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM outbox_events WHERE publish_state <> 'pending'`).Scan(&published); err != nil {
		t.Fatalf("count published outbox: %v", err)
	}
	if pending != 1 || published != 0 {
		t.Fatalf("outbox state = pending:%d published:%d, want the historical event untouched (pending:1 published:0)", pending, published)
	}
	var offset int64
	if err := pool.QueryRow(context.Background(),
		`SELECT next_offset FROM consumer_progress
		  WHERE consumer_name = 'drill-consumer' AND topic = 'txharbor.events.v1' AND partition = 0`).Scan(&offset); err != nil {
		t.Fatalf("read consumer progress: %v", err)
	}
	if offset != wantOffset {
		t.Fatalf("consumer progress = %d, want %d (no replay/consumption effect)", offset, wantOffset)
	}
}

func (e *drillEnv) releaseCreatedAt(releaseID string) time.Time {
	e.t.Helper()
	var at time.Time
	if err := e.ctrl.QueryRow(e.ctx,
		`SELECT created_at FROM recovery_release WHERE release_id = $1`, releaseID).Scan(&at); err != nil {
		e.t.Fatalf("read release %s created_at: %v", releaseID, err)
	}
	return at
}

// drillGapSummary reads the gap counts by state and the affected-capability
// distribution (read-only).
func drillGapSummary(t *testing.T, env *drillEnv) map[string]any {
	t.Helper()
	rows, err := env.ctrl.Query(env.ctx,
		`SELECT state, count(*) FROM recovery_gap WHERE instance_id = $1 GROUP BY state`, env.instanceID)
	if err != nil {
		t.Fatalf("read gap counts: %v", err)
	}
	defer rows.Close()
	byState := map[string]int64{"open": 0, "closed": 0, "escalated": 0}
	for rows.Next() {
		var state string
		var n int64
		if err := rows.Scan(&state, &n); err != nil {
			t.Fatalf("scan gap count: %v", err)
		}
		byState[state] = n
	}
	return map[string]any{"by_state": byState}
}

func drillGapCountsJSON(t *testing.T, env *drillEnv) []byte {
	t.Helper()
	raw, err := json.Marshal(drillGapSummary(t, env))
	if err != nil {
		t.Fatalf("encode gap counts: %v", err)
	}
	return raw
}

// drillCapabilityStates records the derived per-capability state for the
// conservative pause evidence (read-only gate observations).
func drillCapabilityStates(t *testing.T, env *drillEnv) map[string]string {
	t.Helper()
	gate := env.gate()
	out := make(map[string]string, len(recovery.KnownCapabilities()))
	for _, capability := range recovery.KnownCapabilities() {
		decision, err := gate.Admit(env.ctx, recovery.GateRequest{
			InstanceID: env.instanceID, Capability: capability, ScopeHash: env.scope(capability),
			Actor: "deploy:executor", OperationID: env.operation("observe"), Action: "drill_observe",
		})
		if err != nil {
			t.Fatalf("observe %s: %v", capability, err)
		}
		state := "refused:" + string(decision.RefusalClass)
		if decision.Allowed {
			state = "released"
		}
		out[string(capability)] = state
	}
	return out
}

// ---------------------------------------------------------------------------
// Optional event layer: a real Kafka broker (NOT RUN when it cannot be
// provisioned, never a silent pass).
// ---------------------------------------------------------------------------

// drillKafkaTopic is the canonical 013 topic (kept as a literal so this
// drill-tagged file does not depend on the unit-layer testutil package; a
// drift from testutil.KafkaTopic fails the V5/V6 assertions loudly).
const drillKafkaTopic = "txharbor.events.v1"

// drillKafkaBroker is the drill's own minimal real-broker handle: the pinned
// confluent-local KRaft image, started through the testcontainers Kafka module
// (the module's starter script targets that image).
type drillKafkaBroker struct {
	container *tckafka.KafkaContainer
	brokers   []string
}

// drillStartKafkaOrSkip provisions the real broker. When it cannot be
// provisioned the test reports NOT RUN with an archived record: a drill run
// without the event layer is not a covered run.
func drillStartKafkaOrSkip(t *testing.T) *drillKafkaBroker {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()
	ctr, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.9.10",
		// Explicit creation only: a typo in a topic name must surface instead
		// of silently materializing a topic (the module's default wait
		// strategy — broker transitioned to RUNNING — is kept).
		testcontainers.WithEnv(map[string]string{"KAFKA_AUTO_CREATE_TOPICS_ENABLE": "false"}),
	)
	if err != nil {
		drillWriteEvidence(t, "kafka-NOT-RUN", map[string]any{
			"state":  "NOT RUN",
			"layer":  "event (Kafka)",
			"reason": err.Error(),
			"note":   "the event-layer drill scenario was not executed; this is not a pass",
		})
		t.Skipf("NOT RUN: no Kafka broker could be provisioned (%v); the event-layer scenario is not covered", err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })
	brokers, err := ctr.Brokers(ctx)
	if err != nil {
		t.Fatalf("kafka brokers: %v", err)
	}
	return &drillKafkaBroker{container: ctr, brokers: brokers}
}

func (k *drillKafkaBroker) Brokers() []string { return k.brokers }

// drillKafkaEnsureTopic creates the topic with bounded retries for the
// transient responses a fresh KRaft broker can return before leadership
// settles.
func drillKafkaEnsureTopic(t *testing.T, k *drillKafkaBroker, topic string, partitions int32) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	cl, err := kgo.NewClient(kgo.SeedBrokers(k.brokers...))
	if err != nil {
		t.Fatalf("kafka admin client: %v", err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	deadline := time.Now().Add(60 * time.Second)
	for {
		responses, err := adm.CreateTopics(ctx, partitions, 1, nil, topic)
		if err == nil {
			detail, ok := responses[topic]
			if !ok {
				t.Fatalf("create topic %q: broker returned no response", topic)
			}
			if detail.Err == nil || errors.Is(detail.Err, kerr.TopicAlreadyExists) {
				return
			}
			err = detail.Err
		}
		if !(errors.Is(err, kerr.NotController) || errors.Is(err, kerr.LeaderNotAvailable) ||
			errors.Is(err, kerr.RequestTimedOut) || errors.Is(err, kerr.NetworkException)) {
			t.Fatalf("create topic %q: %v", topic, err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("create topic %q: %v", topic, err)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("create topic %q: %v", topic, ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// drillKafkaOffsets is the read-only broker surface for one consumer group:
// the committed offset (next offset to consume) of one topic/partition, read
// through kadm. It is bound to one group by construction, mirroring the
// T039/T040 BrokerOffsetReader seam.
type drillKafkaOffsets struct {
	brokers []string
	groupID string
}

// CommittedOffset implements sources.BrokerOffsetReader.
func (r drillKafkaOffsets) CommittedOffset(ctx context.Context, topic string, partition int) (int64, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(r.brokers...))
	if err != nil {
		return 0, fmt.Errorf("kafka offsets client: %w", err)
	}
	defer cl.Close()
	responses, err := kadm.NewClient(cl).FetchOffsets(ctx, r.groupID)
	if err != nil {
		return 0, fmt.Errorf("fetch committed offsets of group %s: %w", r.groupID, err)
	}
	response, ok := responses.Lookup(topic, int32(partition))
	if !ok {
		return 0, fmt.Errorf("no committed-offset response for %s/%d", topic, partition)
	}
	if response.Err != nil {
		return 0, fmt.Errorf("committed offset %s/%d: %w", topic, partition, response.Err)
	}
	if response.At < 0 {
		return 0, fmt.Errorf("no committed offset exists for %s/%d", topic, partition)
	}
	return response.At, nil
}

// drillKafkaSeedGroup creates the topic, produces count records and commits
// group progress at count: the real "downstream consumption happened after the
// recovery point" fact of the event layer.
func drillKafkaSeedGroup(t *testing.T, kafka *drillKafkaBroker, group string, count int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	drillKafkaEnsureTopic(t, kafka, drillKafkaTopic, 1)
	producer, err := kgo.NewClient(
		kgo.SeedBrokers(kafka.Brokers()...),
		kgo.DefaultProduceTopic(drillKafkaTopic),
	)
	if err != nil {
		t.Fatalf("kafka producer: %v", err)
	}
	for i := 0; i < count; i++ {
		record := &kgo.Record{Partition: 0, Key: []byte(fmt.Sprintf("drill-%d", i)), Value: []byte(`{"drill":true}`)}
		if err := producer.ProduceSync(ctx, record).FirstErr(); err != nil {
			producer.Close()
			t.Fatalf("produce record %d: %v", i, err)
		}
	}
	producer.Close()

	admin, err := kgo.NewClient(kgo.SeedBrokers(kafka.Brokers()...))
	if err != nil {
		t.Fatalf("kafka admin client: %v", err)
	}
	defer admin.Close()
	if _, err := kadm.NewClient(admin).CommitOffsets(ctx, group,
		kadm.Offsets{drillKafkaTopic: {0: kadm.Offset{At: int64(count), LeaderEpoch: -1}}}); err != nil {
		t.Fatalf("commit group offset: %v", err)
	}
}

func drillKafkaEndOffset(t *testing.T, kafka *drillKafkaBroker) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cl, err := kgo.NewClient(kgo.SeedBrokers(kafka.Brokers()...))
	if err != nil {
		t.Fatalf("kafka client: %v", err)
	}
	defer cl.Close()
	ends, err := kadm.NewClient(cl).ListEndOffsets(ctx, drillKafkaTopic)
	if err != nil {
		t.Fatalf("ListEndOffsets: %v", err)
	}
	detail, ok := ends.Lookup(drillKafkaTopic, 0)
	if !ok || detail.Err != nil {
		t.Fatalf("end offset of %s/0: ok=%t err=%v", drillKafkaTopic, ok, detail.Err)
	}
	return detail.Offset
}

// TestT058DrillEventLayerKafkaBrokerOffsetDivergence is the real-broker event
// scenario of the drill: the restored consumer progress trails the broker's
// committed offset, V6 reports the divergence (never a silent re-consumption),
// and the verification flow neither produces nor consumes on the broker.
func TestT058DrillEventLayerKafkaBrokerOffsetDivergence(t *testing.T) {
	requireDrillLocalPGRestore(t)
	kafka := drillStartKafkaOrSkip(t)
	const group = "drill-consumer-group"
	const records = 6
	drillKafkaSeedGroup(t, kafka, group, records)
	endBefore := drillKafkaEndOffset(t, kafka)

	env := newDrillEnv(t, true)
	env.seedLiveBusinessState()
	backup := env.backup()
	advance := env.advanceExternally()

	verifyTarget := env.createDatabase("kafka_verify")
	env.verifyBackup(backup.ManifestPath, verifyTarget, env.operation("kafka-verify-backup"))
	recoveredDSN := env.createDatabase("kafka_recovered")
	recovered := env.openPool(recoveredDSN)
	env.restore(backup.ManifestPath, recoveredDSN, env.operation("kafka-restore"))
	recoveredBefore := drillFingerprint(t, recovered)

	// The broker reader is bound to the seeded group: the restored next_offset
	// (5) trails the committed offset (6).
	env.broker = drillKafkaOffsets{brokers: kafka.Brokers(), groupID: group}
	batch := env.verify(recovered, env.operation("kafka-verify"))

	var v6 []recovery.VerificationItem
	for _, item := range batch.Items {
		if item.Category == recovery.VerificationV6 {
			v6 = append(v6, item)
		}
	}
	if len(v6) == 0 {
		t.Fatal("no V6 item was produced with a real broker reader")
	}
	divergent := false
	for _, item := range v6 {
		if item.Conclusion != recovery.ConclusionDivergent {
			continue
		}
		var observations []struct {
			Sources map[string]any `json:"sources"`
		}
		if err := json.Unmarshal(item.Sources, &observations); err != nil || len(observations) == 0 {
			t.Fatalf("decode V6 sources: %v (%s)", err, item.Sources)
		}
		committed, committedOK := observations[0].Sources["broker_committed_offset"].(float64)
		nextOffset, nextOK := observations[0].Sources["next_offset"].(float64)
		if committedOK && nextOK && int64(committed) == records && int64(nextOffset) == 5 {
			divergent = true
		}
	}
	if !divergent {
		t.Fatalf("V6 did not report the broker-leads-restored-progress divergence (broker=%d restored=5): %+v", records, v6)
	}

	// Read-only broker discipline: no produce/consume happened; the committed
	// offset and the end offset are unchanged.
	committed, err := (drillKafkaOffsets{brokers: kafka.Brokers(), groupID: group}).CommittedOffset(env.ctx, drillKafkaTopic, 0)
	if err != nil || committed != records {
		t.Fatalf("committed offset after verification = %d err=%v, want %d", committed, err, records)
	}
	if end := drillKafkaEndOffset(t, kafka); end != endBefore {
		t.Fatalf("the topic end offset changed during verification: %d -> %d", endBefore, end)
	}
	drillAssertFingerprintEqual(t, recoveredBefore, drillFingerprint(t, recovered), "restored target across the event-layer drill")

	drillWriteEvidence(t, "t058-event-kafka", map[string]any{
		"purpose":           "local drill inputs only; not production thresholds",
		"broker_topic":      drillKafkaTopic,
		"broker_group":      group,
		"broker_committed":  records,
		"restored_progress": 5,
		"v6_conclusion":     string(recovery.ConclusionDivergent),
		"external_advance":  advance,
		"result":            "broker leads restored progress; divergence recorded, no re-consumption",
	})
}
