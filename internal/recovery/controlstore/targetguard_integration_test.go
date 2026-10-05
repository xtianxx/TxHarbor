//go:build integration

package controlstore

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore/schema"
)

func TestTargetGuardFailClosedLifecycle(t *testing.T) {
	ctx := context.Background()
	dsn := startControlPostgres(t)
	if err := db.MigrateUp(ctx, db.MigrateOptions{DSN: dsn, LockTimeout: 10 * time.Second, ConnectTimeout: 5 * time.Second, FS: schema.FS}, io.Discard); err != nil {
		t.Fatalf("migrate control store: %v", err)
	}
	pool := openControlTestPool(t, dsn)
	target := DSNTarget{Host: "DB.EXAMPLE", Port: 5432, Database: "app", Role: "restore"}
	key, err := TargetGuardKey(target)
	if err != nil {
		t.Fatal(err)
	}
	roleFP := "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := InitializeTargetGuard(ctx, tx, key, "init-1"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	const instanceID = "10000000-0000-4000-8000-000000000001"
	if _, err := pool.Exec(ctx, `INSERT INTO recovery_instance
(instance_id, kind, state, evidence_hash, opened_by, target_guard_key, target_role_fingerprint)
VALUES ($1, 'recovery', 'open', $2, 'test', $3, $4)`, instanceID, EmptyEvidenceHash, key, roleFP); err != nil {
		t.Fatalf("insert instance: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM recovery_instance WHERE instance_id=$1`, instanceID)
	})

	if err := RequireCleanTargetGuard(ctx, pool, instanceID, key, roleFP); err == nil {
		t.Fatal("unknown target guard allowed mutation")
	}
	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := ResolveTargetGuardClean(ctx, tx, key, "baseline-1", []byte(`{"baseline":true}`)); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := PrepareTargetGuard(ctx, tx, key, "launch-1"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := RequireCleanTargetGuard(ctx, pool, instanceID, key, roleFP); err == nil {
		t.Fatal("unresolved prepared guard allowed mutation")
	}
	rollbackKey, err := TargetGuardKey(DSNTarget{Host: "db", Port: 5432, Database: "rollback"})
	if err != nil {
		t.Fatal(err)
	}
	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := InitializeTargetGuard(ctx, tx, rollbackKey, "rollback-init"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := ResolveTargetGuardClean(ctx, tx, rollbackKey, "rollback-baseline", []byte(`{"baseline":true}`)); err != nil {
		t.Fatal(err)
	}
	if err := PrepareTargetGuard(ctx, tx, rollbackKey, "rollback-launch"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := MarkTargetGuardLaunchIntent(ctx, tx, rollbackKey, "txharbor_attempt_lost", "rollback-launch"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	lost, found, err := ReadTargetGuard(ctx, pool, rollbackKey)
	if err != nil || !found || lost.State != TargetGuardUnknown || lost.LaunchIntent {
		t.Fatalf("rolled-back launch intent persisted: found=%t guard=%+v err=%v", found, lost, err)
	}

	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := MarkTargetGuardLaunchIntent(ctx, tx, key, "txharbor_attempt_1", "launch-1"); err != nil {
		t.Fatal(err)
	}
	if err := MarkTargetGuardLaunched(ctx, tx, key); err != nil {
		t.Fatal(err)
	}
	if err := MarkTargetGuardWriterDrained(ctx, tx, key); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	// Normal successful attempt: intent/launch/drain and caller evidence are
	// committed in distinct coordinator transactions, ending clean.
	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := AcceptTargetGuardClean(ctx, tx, key, "success-1", []byte(`{"restore_evidence":"accepted"}`)); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := RequireCleanTargetGuard(ctx, pool, instanceID, key, roleFP); err != nil {
		t.Fatalf("successful evidence did not resolve guard: %v", err)
	}

	// A successor durably records may-have-launched before external start. A
	// crash at that boundary blocks another attempt, and a later drain does not
	// make the disposition clean.
	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := PrepareTargetGuard(ctx, tx, key, "launch-2"); err != nil {
		t.Fatal(err)
	}
	if err := MarkTargetGuardLaunchIntent(ctx, tx, key, "txharbor_attempt_2", "launch-2"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := RequireCleanTargetGuard(ctx, pool, instanceID, key, roleFP); err == nil {
		t.Fatal("crash after intent allowed successor mutation")
	}
	if tx, err = pool.Begin(ctx); err != nil {
		t.Fatal(err)
	} else {
		if err := PrepareTargetGuard(ctx, tx, key, "attempt-3"); err == nil {
			t.Fatal("dirty/unknown guard allowed successor attempt")
		}
		_ = tx.Rollback(ctx)
	}
	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := MarkTargetGuardWriterDrained(ctx, tx, key); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := MarkTargetGuardRebuildRequired(ctx, tx, key); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := RequireCleanTargetGuard(ctx, pool, instanceID, key, roleFP); err == nil {
		t.Fatal("dirty guard allowed mutation")
	}
	if err := RequireCleanTargetGuard(ctx, pool, instanceID, "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", roleFP); err == nil {
		t.Fatal("mismatched key allowed mutation")
	}

	// Audit and resolution share a transaction with the evidence write; rolling
	// back must leave the guard dirty, and only the committed transaction clears.
	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `CREATE TEMP TABLE target_rebuild_evidence (payload JSONB)`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO target_rebuild_evidence VALUES ('{"verified":true}')`); err != nil {
		t.Fatal(err)
	}
	evidence := []byte(`{"verified":true,"source":"rebuild-check"}`)
	if err := RecordTargetGuardRebuild(ctx, tx, instanceID, key, "operator:test", "rebuild-1", evidence); err != nil {
		t.Fatal(err)
	}
	if err := ResolveTargetGuardClean(ctx, tx, key, "rebuild-1", evidence); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := RequireCleanTargetGuard(ctx, pool, instanceID, key, roleFP); err != nil {
		t.Fatalf("clean guard refused: %v", err)
	}

	if _, err := pool.Exec(ctx, `UPDATE recovery_instance SET target_role_fingerprint=$2 WHERE instance_id=$1`, instanceID, roleFP); err != nil {
		t.Fatalf("same binding should remain allowed: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE recovery_instance SET target_guard_key=$2 WHERE instance_id=$1`, instanceID, "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"); err == nil {
		t.Fatal("immutable binding mutation succeeded")
	}
	if _, err := pool.Exec(ctx, `UPDATE recovery_instance SET target_guard_key=NULL, target_role_fingerprint=NULL WHERE instance_id=$1`, instanceID); err == nil {
		t.Fatal("binding removal succeeded")
	}

	guard, found, err := ReadTargetGuard(ctx, pool, key)
	if err != nil || !found || guard.State != TargetGuardClean {
		t.Fatalf("read clean guard: found=%t guard=%+v err=%v", found, guard, err)
	}
	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := ResolveTargetGuardClean(ctx, tx, key, "bad-tx", []byte(`{"fake":true}`)); err == nil {
		t.Fatal("clean state was cleared outside rebuild-required state")
	}
	_ = tx.Rollback(ctx)
}

func TestTargetGuardKeyCanonicalEndpointAndUnknownRefusal(t *testing.T) {
	target := DSNTarget{Host: "DB.EXAMPLE", Port: 5432, Database: "app", Role: "restore"}
	key, err := TargetGuardKey(target)
	if err != nil || !sha256FingerprintPattern.MatchString(key) {
		t.Fatalf("key=%q err=%v", key, err)
	}
	otherRole, err := TargetGuardKey(DSNTarget{Host: "db.example", Port: 5432, Database: "app", Role: "verify"})
	if err != nil || key != otherRole {
		t.Fatalf("role changed endpoint key: %q != %q (%v)", key, otherRole, err)
	}
	if _, err := TargetGuardKey(DSNTarget{}); err == nil {
		t.Fatal("unknown target accepted")
	}
}

func TestTargetGuardAttemptTransitionsFenceStaleAttempts(t *testing.T) {
	ctx := context.Background()
	dsn := startControlPostgres(t)
	if err := db.MigrateUp(ctx, db.MigrateOptions{DSN: dsn, LockTimeout: 10 * time.Second, ConnectTimeout: 5 * time.Second, FS: schema.FS}, io.Discard); err != nil {
		t.Fatalf("migrate control store: %v", err)
	}
	pool := openControlTestPool(t, dsn)
	key, err := TargetGuardKey(DSNTarget{Host: "db", Port: 5432, Database: "attempt-fence"})
	if err != nil {
		t.Fatal(err)
	}
	if err := func() error {
		tx, err := pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)
		if err := InitializeTargetGuard(ctx, tx, key, "init-fence"); err != nil {
			return err
		}
		if err := ResolveTargetGuardClean(ctx, tx, key, "baseline-fence", []byte(`{"baseline":true}`)); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}(); err != nil {
		t.Fatal(err)
	}

	const appA = "txharbor_attempt_A"
	const appB = "txharbor_attempt_B"
	startAttempt := func(operationID, appName, previousOperationID, previousAppName string) {
		t.Helper()
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := PrepareTargetGuardForAttempt(ctx, tx, key, previousOperationID, previousAppName, operationID); err != nil {
			_ = tx.Rollback(ctx)
			t.Fatalf("prepare %s: %v", operationID, err)
		}
		if operationID == "attempt-A" {
			if err := MarkTargetGuardLaunchIntent(ctx, tx, key, appB, "attempt-B"); err == nil {
				_ = tx.Rollback(ctx)
				t.Fatal("cross-operation launch intent took over attempt A's prepared row")
			}
			prepared, found, err := ReadTargetGuard(ctx, tx, key)
			if err != nil || !found || prepared.OperationID != operationID || prepared.LaunchIntent || prepared.ActiveWriter || prepared.AttemptAppName != "" {
				_ = tx.Rollback(ctx)
				t.Fatalf("rejected handoff changed prepared attempt: found=%t guard=%+v err=%v", found, prepared, err)
			}
		}
		if err := MarkTargetGuardLaunchIntent(ctx, tx, key, appName, operationID); err != nil {
			_ = tx.Rollback(ctx)
			t.Fatalf("launch intent %s: %v", operationID, err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("commit %s: %v", operationID, err)
		}
	}
	finishAttempt := func(operationID, appName string) {
		t.Helper()
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := MarkTargetGuardLaunchedForAttempt(ctx, tx, key, operationID, appName); err != nil {
			_ = tx.Rollback(ctx)
			t.Fatalf("mark launched %s: %v", operationID, err)
		}
		if err := MarkTargetGuardWriterDrainedForAttempt(ctx, tx, key, operationID, appName); err != nil {
			_ = tx.Rollback(ctx)
			t.Fatalf("mark drained %s: %v", operationID, err)
		}
		if err := AcceptTargetGuardCleanForAttempt(ctx, tx, key, operationID, appName, []byte(`{"success":true}`)); err != nil {
			_ = tx.Rollback(ctx)
			t.Fatalf("accept clean %s: %v", operationID, err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("commit clean %s: %v", operationID, err)
		}
	}
	startAttempt("attempt-A", appA, "baseline-fence", "")
	finishAttempt("attempt-A", appA)
	startAttempt("attempt-B", appB, "attempt-A", appA)

	staleCalls := []struct {
		name string
		call func(pgx.Tx) error
	}{
		{"launched", func(tx pgx.Tx) error { return MarkTargetGuardLaunchedForAttempt(ctx, tx, key, "attempt-A", appA) }},
		{"drained", func(tx pgx.Tx) error { return MarkTargetGuardWriterDrainedForAttempt(ctx, tx, key, "attempt-A", appA) }},
		{"rebuild", func(tx pgx.Tx) error {
			return MarkTargetGuardRebuildRequiredForAttempt(ctx, tx, key, "attempt-A", appA)
		}},
		{"clean acceptance", func(tx pgx.Tx) error {
			return AcceptTargetGuardCleanForAttempt(ctx, tx, key, "attempt-A", appA, []byte(`{"stale":true}`))
		}},
		{"clean invalidation", func(tx pgx.Tx) error {
			return PrepareTargetGuardForAttempt(ctx, tx, key, "attempt-A", appA, "stale-next")
		}},
		{"operation mismatch", func(tx pgx.Tx) error { return MarkTargetGuardWriterDrainedForAttempt(ctx, tx, key, "attempt-A", appB) }},
		{"app mismatch", func(tx pgx.Tx) error { return MarkTargetGuardWriterDrainedForAttempt(ctx, tx, key, "attempt-B", appA) }},
	}
	for _, stale := range staleCalls {
		t.Run(stale.name, func(t *testing.T) {
			tx, err := pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := stale.call(tx); err == nil {
				_ = tx.Rollback(ctx)
				t.Fatal("stale transition unexpectedly matched current guard")
			} else if !strings.Contains(err.Error(), "identity does not match") {
				_ = tx.Rollback(ctx)
				t.Fatalf("stale transition did not report a zero-row identity mismatch: %v", err)
			}
			_ = tx.Rollback(ctx)
			guard, found, err := ReadTargetGuard(ctx, pool, key)
			if err != nil || !found {
				t.Fatalf("read guard: found=%t err=%v", found, err)
			}
			if guard.State != TargetGuardUnknown || !guard.ActiveWriter || !guard.LaunchIntent || guard.OperationID != "attempt-B" || guard.AttemptAppName != appB {
				t.Fatalf("stale transition changed B guard: %+v", guard)
			}
		})
	}

	// A's clean state was committed before B began. B must still be accepted
	// using its own identity after all delayed A transitions were rejected.
	finishAttempt("attempt-B", appB)
	for _, stale := range staleCalls {
		t.Run("after B clean/"+stale.name, func(t *testing.T) {
			tx, err := pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := stale.call(tx); err == nil {
				_ = tx.Rollback(ctx)
				t.Fatal("stale transition unexpectedly matched B's clean guard")
			} else if !strings.Contains(err.Error(), "identity does not match") {
				_ = tx.Rollback(ctx)
				t.Fatalf("stale transition did not report a zero-row identity mismatch: %v", err)
			}
			_ = tx.Rollback(ctx)
			guard, found, err := ReadTargetGuard(ctx, pool, key)
			if err != nil || !found || guard.State != TargetGuardClean || guard.ActiveWriter || guard.OperationID != "attempt-B" || guard.AttemptAppName != appB {
				t.Fatalf("stale transition changed B's clean guard: found=%t guard=%+v err=%v", found, guard, err)
			}
		})
	}
	guard, found, err := ReadTargetGuard(ctx, pool, key)
	if err != nil || !found || guard.State != TargetGuardClean || guard.OperationID != "attempt-B" || guard.AttemptAppName != appB {
		t.Fatalf("B clean acceptance failed: found=%t guard=%+v err=%v", found, guard, err)
	}
}

func TestTargetGuardControlledRebuildCleanIsConsistentForInstanceUse(t *testing.T) {
	ctx, pool, store, _ := migratedControlStore(t)
	key := testTargetGuardKey
	evidence := []byte(`{"verified":true,"source":"controlled-rebuild"}`)

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := InitializeTargetGuard(ctx, tx, key, "rebuild-init"); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := MarkTargetGuardLaunchIntent(ctx, tx, key, "txharbor_attempt_rebuild", "rebuild-init"); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := MarkTargetGuardWriterDrained(ctx, tx, key); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := MarkTargetGuardRebuildRequired(ctx, tx, key); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := RecordTargetGuardRebuild(ctx, tx, "", key, "operator:test", "controlled-rebuild", evidence); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := ResolveTargetGuardClean(ctx, tx, key, "controlled-rebuild", evidence); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	guard, found, err := ReadTargetGuard(ctx, pool, key)
	var storedEvidence map[string]any
	if err == nil && found {
		err = json.Unmarshal(guard.RebuildEvidence, &storedEvidence)
	}
	if err != nil || !found || guard.State != TargetGuardClean || guard.RebuildRequiredAt != nil || storedEvidence["verified"] != true || storedEvidence["source"] != "controlled-rebuild" {
		t.Fatalf("controlled rebuild did not leave a consistent clean guard: found=%t guard=%+v err=%v", found, guard, err)
	}
	if countRows(t, ctx, pool, `SELECT count(*) FROM recovery_audit WHERE action=$1 AND operation_id=$2`, ActionTargetGuardRebuild, "controlled-rebuild") != 1 {
		t.Fatal("controlled rebuild audit record was not preserved")
	}
	instance, err := store.OpenInstance(ctx, OpenInstanceRequest{
		Kind: "recovery", OpenedBy: "deploy:executor", TargetGuardKey: key,
		TargetRoleFingerprint: testTargetRoleFingerprint,
	})
	if err != nil {
		t.Fatalf("open instance with controlled-clean guard: %v", err)
	}
	if err := RequireCleanTargetGuard(ctx, pool, instance.InstanceID, key, testTargetRoleFingerprint); err != nil {
		t.Fatalf("controlled-clean guard refused for instance use: %v", err)
	}
}
