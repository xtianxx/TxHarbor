//go:build integration

// drill_integration_test.go runs the T056 recovery_drill_run writer/reader
// against a real PostgreSQL 18.6 control store (testcontainers): the recorded
// row round-trips its separate timing scopes and per-capability payload, the
// paired drill_run audit row anchors the operation_id idempotency, a changed
// input under the same operation_id conflicts with zero writes, and the
// no-production-threshold annotation is enforced before any write.
//
// Docker provider missing: the package reports NOT RUN (see TestMain) — an
// unrun PG layer is never a pass.
package recovery

import (
	"errors"
	"testing"
	"time"
)

// TestDrillRunRecordReadRoundTrip covers the storage round-trip: separate
// db_restore/verification columns, per-capability release payload and the
// explicit unconfigured-constraint annotation.
func TestDrillRunRecordReadRoundTrip(t *testing.T) {
	ctx, _, pool, store := generationControlStore(t)
	instanceID := openGenerationInstance(t, ctx, store)

	restore := 21.5
	verification := 4.75
	run, err := RecordDrillRun(ctx, store, DrillRunInput{
		InstanceID:          instanceID,
		Scenario:            DrillScenarioFullRecovery,
		RecoveryPoint:       []byte(`{"wall_clock":"2026-09-01T00:00:00Z","wal_lsn":"0/16B6C50"}`),
		DBRestoreSeconds:    &restore,
		VerificationSeconds: &verification,
		CapabilityReleaseSeconds: map[Capability]float64{
			CapabilityQuery:     2.5,
			CapabilityChainScan: 9.5,
		},
		BackupLag:         []byte(`{"state":"unknown","reason":"no lag source in this drill"}`),
		UncoveredInterval: []byte(`{"state":"unknown","reason":"not measured"}`),
		TestInputs:        []byte(`{"purpose":"local drill inputs only; not production thresholds","unconfigured_constraints":["rto_target","retention"]}`),
		GapCounts:         []byte(`{"open":1,"closed":0,"escalated":1}`),
		Result:            DrillResultRefusedSafe,
		LogRef:            "docs/evidence/015/drill/drill-roundtrip.json",
		Actor:             "deploy:executor",
	})
	if err != nil {
		t.Fatalf("RecordDrillRun: %v", err)
	}
	if run.Recorded || run.DrillID == "" {
		t.Fatalf("RecordDrillRun must insert a new row, got %+v", run)
	}
	if run.DBRestoreSeconds == nil {
		t.Fatalf("db_restore_seconds must round-trip (got nil)")
	}
	if *run.DBRestoreSeconds != restore || run.VerificationSeconds == nil || *run.VerificationSeconds != verification {
		t.Fatalf("timing scopes did not round-trip: %v / %v", run.DBRestoreSeconds, run.VerificationSeconds)
	}
	if seconds, ok := run.SafeResumptionSeconds(); ok {
		t.Fatalf("a partial capability set must not claim a safe-resumption duration, got %v", seconds)
	}

	rows, err := DrillRuns(ctx, store, instanceID)
	if err != nil {
		t.Fatalf("DrillRuns: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("DrillRuns = %d rows, want 1", len(rows))
	}
	got := rows[0]
	if got.DrillID != run.DrillID || got.Scenario != DrillScenarioFullRecovery ||
		got.Result != DrillResultRefusedSafe || got.ConstraintsConfigured {
		t.Fatalf("read-back row differs: %+v", got)
	}
	if len(got.CapabilityReleaseSeconds) != 2 || got.CapabilityReleaseSeconds[CapabilityChainScan] != 9.5 {
		t.Fatalf("capability releases did not round-trip: %v", got.CapabilityReleaseSeconds)
	}

	// The paired audit row anchors the operation id for the idempotent path.
	var audits int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM recovery_audit WHERE action = $1 AND instance_id = $2`,
		ActionDrillRun, instanceID).Scan(&audits); err != nil {
		t.Fatalf("count drill audits: %v", err)
	}
	if audits != 1 {
		t.Fatalf("drill audit rows = %d, want 1 (paired with the drill row)", audits)
	}
}

// TestDrillRunOperationIDIdempotency covers replay/conflict semantics: the
// same id + input reads the recorded run back with zero writes; a changed
// input conflicts with zero writes.
func TestDrillRunOperationIDIdempotency(t *testing.T) {
	ctx, _, pool, store := generationControlStore(t)
	instanceID := openGenerationInstance(t, ctx, store)

	input := DrillRunInput{
		InstanceID:        instanceID,
		Scenario:          DrillScenarioF1BackupUnusable,
		RecoveryPoint:     []byte(`{"wall_clock":"2026-09-01T00:00:00Z"}`),
		BackupLag:         []byte(`{"state":"unknown"}`),
		UncoveredInterval: []byte(`{"state":"unknown"}`),
		TestInputs:        []byte(`{"purpose":"local drill inputs only","unconfigured_constraints":["rto_target"]}`),
		Result:            DrillResultFailedInjected,
		Actor:             "deploy:executor",
		OperationID:       "drill-op-1",
	}
	first, err := RecordDrillRun(ctx, store, input)
	if err != nil {
		t.Fatalf("RecordDrillRun(first): %v", err)
	}
	replay, err := RecordDrillRun(ctx, store, input)
	if err != nil {
		t.Fatalf("RecordDrillRun(replay): %v", err)
	}
	if !replay.Recorded || replay.DrillID != first.DrillID {
		t.Fatalf("replay = %+v, want the recorded row %s", replay, first.DrillID)
	}
	var rows int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM recovery_drill_run WHERE instance_id = $1`, instanceID).Scan(&rows); err != nil {
		t.Fatalf("count drill rows: %v", err)
	}
	if rows != 1 {
		t.Fatalf("drill rows after replay = %d, want 1 (zero duplicate writes)", rows)
	}

	conflict := input
	conflict.Result = DrillResultRefusedSafe
	if _, err := RecordDrillRun(ctx, store, conflict); err == nil {
		t.Fatalf("a changed input under the same operation_id must conflict")
	}
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM recovery_drill_run WHERE instance_id = $1`, instanceID).Scan(&rows); err != nil {
		t.Fatalf("count drill rows: %v", err)
	}
	if rows != 1 {
		t.Fatalf("drill rows after conflict = %d, want 1 (zero writes)", rows)
	}
}

// TestDrillRunRefusalsWriteNothing covers the fail-closed writer boundary: a
// non-existent/closed instance and the missing unconfigured-constraint
// annotation are refused before any row is written.
func TestDrillRunRefusalsWriteNothing(t *testing.T) {
	ctx, _, pool, store := generationControlStore(t)
	instanceID := openGenerationInstance(t, ctx, store)

	missing := DrillRunInput{
		InstanceID:        "99999999-9999-4999-8999-999999999999",
		Scenario:          DrillScenarioFullRecovery,
		RecoveryPoint:     []byte(`{"wall_clock":"2026-09-01T00:00:00Z"}`),
		BackupLag:         []byte(`{"state":"unknown"}`),
		UncoveredInterval: []byte(`{"state":"unknown"}`),
		TestInputs:        []byte(`{"purpose":"local drill inputs only","unconfigured_constraints":["rto_target"]}`),
		Result:            DrillResultOK,
		Actor:             "deploy:executor",
	}
	if _, err := RecordDrillRun(ctx, store, missing); err == nil {
		t.Fatalf("a non-existent instance must refuse")
	}

	silent := missing
	silent.InstanceID = instanceID
	silent.TestInputs = []byte(`{"purpose":"local drill inputs only"}`)
	if _, err := RecordDrillRun(ctx, store, silent); err == nil {
		t.Fatalf("constraints_configured=false without unconfigured_constraints must refuse")
	}

	var rows int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM recovery_drill_run WHERE instance_id = $1`, instanceID).Scan(&rows); err != nil {
		t.Fatalf("count drill rows: %v", err)
	}
	if rows != 0 {
		t.Fatalf("refused drill runs wrote %d rows, want 0", rows)
	}

	// A closed instance is refused too (drills record against the open
	// instance; closing is a separate, release-guarded command).
	if _, err := pool.Exec(ctx,
		`UPDATE recovery_instance SET state = 'closed', closed_by = 'deploy:executor', closed_at = now() WHERE instance_id = $1`, instanceID); err != nil {
		t.Fatalf("mark instance closed: %v", err)
	}
	if _, err := RecordDrillRun(ctx, store, DrillRunInput{
		InstanceID:        instanceID,
		Scenario:          DrillScenarioFullRecovery,
		RecoveryPoint:     []byte(`{"wall_clock":"2026-09-01T00:00:00Z"}`),
		BackupLag:         []byte(`{"state":"unknown"}`),
		UncoveredInterval: []byte(`{"state":"unknown"}`),
		TestInputs:        []byte(`{"purpose":"local drill inputs only","unconfigured_constraints":["rto_target"]}`),
		Result:            DrillResultOK,
		Actor:             "deploy:executor",
	}); err == nil {
		t.Fatalf("a closed instance must refuse a drill run")
	}
	// The control store stays compatible (the store handle is untouched).
	if _, err := store.Pool().Exec(ctx, `SELECT 1`); err != nil {
		t.Fatalf("store pool broken: %v", err)
	}
}

// TestDrillRunRecordOnlyGenerationIsAtomicWithInsert controls the race between
// accepting a restore probe and inserting a record-only drill. The competing
// transaction owns the instance-row lock; RecordDrillRun blocks, then sees the
// advanced generation after that transaction commits and refuses the insert.
func TestDrillRunRecordOnlyGenerationIsAtomicWithInsert(t *testing.T) {
	ctx, _, pool, store := generationControlStore(t)
	instanceID := openGenerationInstance(t, ctx, store)

	var generation int64
	if err := pool.QueryRow(ctx, `SELECT evidence_generation FROM recovery_instance WHERE instance_id = $1`, instanceID).Scan(&generation); err != nil {
		t.Fatalf("read initial evidence generation: %v", err)
	}
	lockTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin generation interleaving transaction: %v", err)
	}
	defer func() { _ = lockTx.Rollback(ctx) }()
	var lockedGeneration int64
	if err := lockTx.QueryRow(ctx, `SELECT evidence_generation FROM recovery_instance WHERE instance_id = $1 FOR UPDATE`, instanceID).Scan(&lockedGeneration); err != nil {
		t.Fatalf("lock recovery instance: %v", err)
	}
	if lockedGeneration != generation {
		t.Fatalf("locked generation=%d, initial=%d", lockedGeneration, generation)
	}

	result := make(chan error, 1)
	go func() {
		_, err := RecordDrillRun(ctx, store, DrillRunInput{
			InstanceID:                 instanceID,
			Scenario:                   DrillScenarioFullRecovery,
			RecoveryPoint:              []byte(`{"wall_clock":"2026-09-01T00:00:00Z"}`),
			BackupLag:                  []byte(`{"state":"unknown"}`),
			UncoveredInterval:          []byte(`{"state":"unknown"}`),
			TestInputs:                 []byte(`{"purpose":"local drill inputs only","unconfigured_constraints":["rto_target"]}`),
			Result:                     DrillResultRefusedSafe,
			Actor:                      "deploy:executor",
			ExpectedEvidenceGeneration: &generation,
		})
		result <- err
	}()

	// Wait until the writer is actually blocked on the row lock, avoiding a
	// timing-only sleep that could let it start after the generation update.
	deadline := time.Now().Add(5 * time.Second)
	for {
		var waiting bool
		if err := pool.QueryRow(ctx, `
SELECT EXISTS (
    SELECT 1 FROM pg_stat_activity
    WHERE wait_event_type = 'Lock'
      AND query LIKE '%SELECT kind, state, evidence_generation FROM recovery_instance%'
)`).Scan(&waiting); err != nil {
			t.Fatalf("observe blocked drill insert: %v", err)
		}
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("RecordDrillRun did not block on the locked instance row")
		}
		time.Sleep(10 * time.Millisecond)
	}

	if _, err := lockTx.Exec(ctx, `UPDATE recovery_instance SET evidence_generation = evidence_generation + 1 WHERE instance_id = $1`, instanceID); err != nil {
		t.Fatalf("advance evidence generation while holding lock: %v", err)
	}
	if err := lockTx.Commit(ctx); err != nil {
		t.Fatalf("commit generation advance: %v", err)
	}
	if err := <-result; !errors.Is(err, ErrDrillStaleGeneration) {
		t.Fatalf("stale record-only insert error=%v, want ErrDrillStaleGeneration", err)
	}

	var rows, audits int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM recovery_drill_run WHERE instance_id = $1`, instanceID).Scan(&rows); err != nil {
		t.Fatalf("count drill rows: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM recovery_audit WHERE action = $1 AND instance_id = $2`, ActionDrillRun, instanceID).Scan(&audits); err != nil {
		t.Fatalf("count drill audits: %v", err)
	}
	if rows != 0 || audits != 0 {
		t.Fatalf("stale generation wrote drill rows=%d audit rows=%d; want zero", rows, audits)
	}
}
