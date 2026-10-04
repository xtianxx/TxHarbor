//go:build linux && integration

package recovery

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore/schema"
)

func TestTargetWriterSerializesSameTargetAndAllowsDifferentTargets(t *testing.T) {
	f := newTargetWriterFixture(t)
	dsn := f.createTarget(t)
	trusted, err := controlstore.ParseDSNTarget(dsn)
	if err != nil {
		t.Fatal(err)
	}
	key, err := CanonicalTargetKey(trusted)
	if err != nil {
		t.Fatal(err)
	}
	initCleanWriterGuard(t, f.ctrl, key)
	startMarker := filepath.Join(t.TempDir(), "started")
	writer := writerScript(t, 1.2, startMarker)
	base := writerOptions(f, dsn, trusted, "writer-same-1", writer)
	base.LockTimeout = 5 * time.Second
	base.QuiescenceTimeout = 3 * time.Second
	base.Runner = TargetProcessRunner{DrainTimeout: 2 * time.Second, PollInterval: 10 * time.Millisecond,
		HealthCheckInterval: 20 * time.Millisecond}
	var wg sync.WaitGroup
	firstDone := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := runTargetWriter(context.Background(), base)
		firstDone <- err
	}()
	waitForFile(t, startMarker, 5*time.Second)
	second := base
	second.OperationID = "writer-same-2"
	secondDone := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := runTargetWriter(context.Background(), second)
		secondDone <- err
	}()
	// The first target backend remains tagged while the second invocation waits
	// on the dedicated session lock; a late/successor writer cannot overlap it.
	time.Sleep(150 * time.Millisecond)
	var tagged int
	if err := f.admin.QueryRow(f.ctx, `SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE 'txh015_%'`).Scan(&tagged); err != nil {
		t.Fatal(err)
	}
	if tagged != 1 {
		t.Fatalf("same-target tagged writer sessions = %d, want exactly one", tagged)
	}
	if err := <-firstDone; err != nil {
		t.Fatalf("first writer: %v", err)
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("serialized successor: %v", err)
	}
	wg.Wait()

	// Distinct database keys do not contend on the target lock.
	dsnA := f.createTarget(t)
	dsnB := f.createTarget(t)
	targetA, _ := controlstore.ParseDSNTarget(dsnA)
	targetB, _ := controlstore.ParseDSNTarget(dsnB)
	keyA, _ := CanonicalTargetKey(targetA)
	keyB, _ := CanonicalTargetKey(targetB)
	initCleanWriterGuard(t, f.ctrl, keyA)
	initCleanWriterGuard(t, f.ctrl, keyB)
	lockA, err := AcquireTargetLock(f.ctx, f.ctrlDSN, keyA, time.Second, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer lockA.Release(context.Background())
	lockB, err := AcquireTargetLock(f.ctx, f.ctrlDSN, keyB, time.Second, time.Millisecond)
	if err != nil {
		t.Fatalf("different target was serialized behind first key: %v", err)
	}
	_ = lockB.Release(context.Background())

}

func TestTargetWriterLockLossAndPostIntentFailureBlockSuccessor(t *testing.T) {
	f := newTargetWriterFixture(t)
	dsn := f.createTarget(t)
	target, err := controlstore.ParseDSNTarget(dsn)
	if err != nil {
		t.Fatal(err)
	}
	key, err := CanonicalTargetKey(target)
	if err != nil {
		t.Fatal(err)
	}
	initCleanWriterGuard(t, f.ctrl, key)
	marker := filepath.Join(t.TempDir(), "started")
	writer := writerScript(t, 30, marker)
	opts := writerOptions(f, dsn, target, "writer-lock-loss", writer)
	opts.OperationKind = TargetWriterOperationRestore
	opts.LockTimeout = 3 * time.Second
	opts.QuiescenceTimeout = time.Second
	opts.Runner = TargetProcessRunner{DrainTimeout: 2 * time.Second, PollInterval: 10 * time.Millisecond,
		HealthCheckInterval: 20 * time.Millisecond}
	done := make(chan error, 1)
	go func() { _, err := runTargetWriter(context.Background(), opts); done <- err }()
	waitForFile(t, marker, 5*time.Second)
	k1, k2 := key.AdvisoryLockKey()
	var lockPID int
	if err := f.admin.QueryRow(f.ctx, `SELECT pid FROM pg_locks WHERE locktype='advisory' AND granted AND classid=$1::oid AND objid=$2::oid AND objsubid=2 LIMIT 1`, uint32(k1), uint32(k2)).Scan(&lockPID); err != nil {
		t.Fatalf("find target lock session: %v", err)
	}
	var terminated bool
	if err := f.admin.QueryRow(f.ctx, `SELECT pg_terminate_backend($1)`, lockPID).Scan(&terminated); err != nil || !terminated {
		t.Fatalf("terminate target lock session: terminated=%v err=%v", terminated, err)
	}
	if err := <-done; err == nil {
		t.Fatal("lock loss was accepted")
	}
	guard, found, err := controlstore.ReadTargetGuard(f.ctx, f.ctrl, key.String())
	if err != nil || !found || guard.State != controlstore.TargetGuardUnknown || !guard.ActiveWriter || !guard.LaunchIntent {
		t.Fatalf("lock-loss attempt did not remain unknown/active: guard=%+v found=%v err=%v", guard, found, err)
	}

	// A later ordinary attempt is refused until an explicitly controlled
	// rebuild resolves the target guard.
	opts.OperationID = "writer-successor"
	opts.executable = "/bin/false"
	if _, err := runTargetWriter(context.Background(), opts); err == nil {
		t.Fatal("ordinary successor accepted an unresolved post-intent target")
	}

}

func TestTargetWriterRejectsWrongRoleNoOpAcceptanceAndWrongProbeTag(t *testing.T) {
	f := newTargetWriterFixture(t)
	dsn := f.createTarget(t)
	trusted, err := controlstore.ParseDSNTarget(dsn)
	if err != nil {
		t.Fatal(err)
	}
	key, err := CanonicalTargetKey(trusted)
	if err != nil {
		t.Fatal(err)
	}
	initCleanWriterGuard(t, f.ctrl, key)
	marker := filepath.Join(t.TempDir(), "wrong-role-child")
	wrongRoleURL, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	wrongRoleURL.User = url.User("not_the_trusted_role")
	wrongRole := writerOptions(f, dsn, trusted, "writer-wrong-role", writerScript(t, 0, marker))
	wrongRole.TargetDSN = wrongRoleURL.String()
	if _, err := runTargetWriter(context.Background(), wrongRole); err == nil {
		t.Fatal("target DSN with a different role was accepted")
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("wrong-role child launched unexpectedly, stat err=%v", err)
	}

	// A clean guard is required for each new attempt; this one proves wrong
	// probe tags and nil-error/no-row acceptance receipts never restore clean.
	wrongTagDSN := f.createTarget(t)
	wrongTagTarget, err := controlstore.ParseDSNTarget(wrongTagDSN)
	if err != nil {
		t.Fatal(err)
	}
	wrongTagKey, _ := CanonicalTargetKey(wrongTagTarget)
	initCleanWriterGuard(t, f.ctrl, wrongTagKey)
	wrongTag := writerOptions(f, wrongTagDSN, wrongTagTarget, "writer-wrong-tag", "/bin/true")
	wrongTag.Probe = func(context.Context, TargetWriterProof) (TargetWriterProbeResult, error) {
		return TargetWriterProbeResult{Outcome: TargetWriterProbePassed, ApplicationName: "txh015_wrong", Evidence: []byte(`{"probe":true}`)}, nil
	}
	if _, err := runTargetWriter(context.Background(), wrongTag); err == nil {
		t.Fatal("probe result tagged for a different attempt was accepted")
	}
	wrongTagGuard, found, err := controlstore.ReadTargetGuard(f.ctx, f.ctrl, wrongTagKey.String())
	if err != nil || !found || wrongTagGuard.State == controlstore.TargetGuardClean {
		t.Fatalf("wrong-tag probe unexpectedly left a clean guard: %+v found=%v err=%v", wrongTagGuard, found, err)
	}

	noOpDSN := f.createTarget(t)
	noOpTarget, err := controlstore.ParseDSNTarget(noOpDSN)
	if err != nil {
		t.Fatal(err)
	}
	noOpKey, _ := CanonicalTargetKey(noOpTarget)
	initCleanWriterGuard(t, f.ctrl, noOpKey)
	noOp := writerOptions(f, noOpDSN, noOpTarget, "writer-no-op-accept", "/bin/true")
	noOp.Acceptance = func(context.Context, pgx.Tx, TargetWriterProof) (TargetWriterAcceptance, error) {
		return TargetWriterAcceptance{}, nil
	}
	if _, err := runTargetWriter(context.Background(), noOp); err == nil {
		t.Fatal("nil-error acceptance without accepted row references was accepted")
	}
	noOpGuard, found, err := controlstore.ReadTargetGuard(f.ctx, f.ctrl, noOpKey.String())
	if err != nil || !found || noOpGuard.State == controlstore.TargetGuardClean {
		t.Fatalf("no-op acceptance unexpectedly left a clean guard: %+v found=%v err=%v", noOpGuard, found, err)
	}
}

func TestTargetWriterLockLossDuringProbeRefusesAcceptance(t *testing.T) {
	f := newTargetWriterFixture(t)
	dsn := f.createTarget(t)
	target, err := controlstore.ParseDSNTarget(dsn)
	if err != nil {
		t.Fatal(err)
	}
	key, _ := CanonicalTargetKey(target)
	initCleanWriterGuard(t, f.ctrl, key)
	opts := writerOptions(f, dsn, target, "writer-probe-lock-loss", "/bin/true")
	opts.LockHealthInterval = 10 * time.Millisecond
	probeStarted := make(chan struct{})
	var acceptanceCalled atomic.Bool
	opts.Probe = func(ctx context.Context, proof TargetWriterProof) (TargetWriterProbeResult, error) {
		close(probeStarted)
		<-ctx.Done()
		return TargetWriterProbeResult{}, ctx.Err()
	}
	opts.Acceptance = func(ctx context.Context, tx pgx.Tx, proof TargetWriterProof) (TargetWriterAcceptance, error) {
		acceptanceCalled.Store(true)
		return successfulWriterAcceptance(ctx, tx, proof)
	}
	done := make(chan error, 1)
	go func() { _, err := runTargetWriter(context.Background(), opts); done <- err }()
	select {
	case <-probeStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("target probe did not start")
	}
	k1, k2 := key.AdvisoryLockKey()
	var lockPID int
	if err := f.admin.QueryRow(f.ctx, `SELECT pid FROM pg_locks WHERE locktype='advisory' AND granted AND classid=$1::oid AND objid=$2::oid AND objsubid=2 LIMIT 1`, uint32(k1), uint32(k2)).Scan(&lockPID); err != nil {
		t.Fatalf("find target lock session: %v", err)
	}
	var terminated bool
	if err := f.admin.QueryRow(f.ctx, `SELECT pg_terminate_backend($1)`, lockPID).Scan(&terminated); err != nil || !terminated {
		t.Fatalf("terminate target lock session: terminated=%v err=%v", terminated, err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("probe completed successfully after target lock loss")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("target writer did not stop after lock loss during probe")
	}
	if acceptanceCalled.Load() {
		t.Fatal("acceptance callback ran after lock loss")
	}
	guard, found, err := controlstore.ReadTargetGuard(f.ctx, f.ctrl, key.String())
	if err != nil || !found || guard.State != controlstore.TargetGuardUnknown || !guard.ActiveWriter || !guard.LaunchIntent {
		t.Fatalf("probe lock-loss attempt did not remain unknown/active: %+v found=%v err=%v", guard, found, err)
	}
}

func TestTargetWriterPrelaunchCommitErrorsPreservePriorCleanOrLeaveAttemptUnknown(t *testing.T) {
	for _, commitMode := range []string{"before-commit", "after-commit-before-ack"} {
		t.Run(commitMode, func(t *testing.T) {
			f := newTargetWriterFixture(t)
			dsn := f.createTarget(t)
			target, err := controlstore.ParseDSNTarget(dsn)
			if err != nil {
				t.Fatal(err)
			}
			key, _ := CanonicalTargetKey(target)
			initCleanWriterGuard(t, f.ctrl, key)
			before, found, err := controlstore.ReadTargetGuard(f.ctx, f.ctrl, key.String())
			if err != nil || !found || before.State != controlstore.TargetGuardClean {
				t.Fatalf("initial guard: %+v found=%v err=%v", before, found, err)
			}
			marker := filepath.Join(t.TempDir(), "must-not-launch")
			opts := writerOptions(f, dsn, target, "writer-prelaunch-"+commitMode, writerScript(t, 0, marker))
			opts.prelaunchCommit = func(ctx context.Context, tx pgx.Tx) error {
				if commitMode == "after-commit-before-ack" {
					if err := tx.Commit(ctx); err != nil {
						return err
					}
				}
				return errors.New("simulated prelaunch commit acknowledgement failure")
			}
			if _, err := runTargetWriter(f.ctx, opts); err == nil {
				t.Fatal("prelaunch commit failure was accepted")
			}
			assertWriterChildNotStarted(t, marker)
			after, found, err := controlstore.ReadTargetGuard(f.ctx, f.ctrl, key.String())
			if err != nil || !found {
				t.Fatalf("read guard after simulated commit failure: found=%v err=%v", found, err)
			}
			if commitMode == "before-commit" {
				if after.State != controlstore.TargetGuardClean || after.OperationID != before.OperationID || after.AttemptAppName != before.AttemptAppName {
					t.Fatalf("failed pre-commit attempt changed prior clean guard: before=%+v after=%+v", before, after)
				}
			} else if after.State != controlstore.TargetGuardUnknown || !after.LaunchIntent || after.OperationID != opts.OperationID {
				t.Fatalf("ambiguous committed intent was not left blocking: %+v", after)
			}
		})
	}
}

func TestTargetWriterStaleCleanupCannotReplaceLaterCleanAttempt(t *testing.T) {
	f := newTargetWriterFixture(t)
	dsn := f.createTarget(t)
	target, err := controlstore.ParseDSNTarget(dsn)
	if err != nil {
		t.Fatal(err)
	}
	key, _ := CanonicalTargetKey(target)
	initCleanWriterGuard(t, f.ctrl, key)
	opts := writerOptions(f, dsn, target, "writer-B-clean", "/bin/true")
	result, err := runTargetWriter(f.ctx, opts)
	if err != nil {
		t.Fatalf("attempt B: %v", err)
	}
	before, found, err := controlstore.ReadTargetGuard(f.ctx, f.ctrl, key.String())
	if err != nil || !found || before.State != controlstore.TargetGuardClean || before.OperationID != opts.OperationID || before.AttemptAppName != result.Application {
		t.Fatalf("attempt B was not clean: guard=%+v found=%v err=%v", before, found, err)
	}
	staleApp, err := NewAttemptApplicationName()
	if err != nil {
		t.Fatal(err)
	}
	lock, err := AcquireTargetLock(f.ctx, f.ctrlDSN, key, time.Second, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Release(context.Background())
	lockHealth := func(ctx context.Context) error { return lock.Health(ctx) }
	if err := finalizeFailedTargetAttempt(lock, lockHealth, dsn, key.String(), "writer-A-stale", staleApp, time.Second, 10*time.Millisecond); err == nil {
		t.Fatal("real stale-attempt finalizer unexpectedly accepted attempt A")
	}
	after, found, err := controlstore.ReadTargetGuard(f.ctx, f.ctrl, key.String())
	if err != nil || !found || !reflect.DeepEqual(after, before) {
		t.Fatalf("stale attempt A cleanup changed valid attempt B clean: before=%+v after=%+v found=%v err=%v", before, after, found, err)
	}
}

func TestTargetWriterRetainsValidCleanAfterAmbiguousAcceptanceAck(t *testing.T) {
	f := newTargetWriterFixture(t)
	dsn := f.createTarget(t)
	target, err := controlstore.ParseDSNTarget(dsn)
	if err != nil {
		t.Fatal(err)
	}
	key, _ := CanonicalTargetKey(target)
	initCleanWriterGuard(t, f.ctrl, key)
	opts := writerOptions(f, dsn, target, "writer-accept-ack-loss", "/bin/true")
	opts.afterAcceptanceCommit = func() error { return errors.New("simulated lost commit acknowledgement") }
	result, err := runTargetWriter(f.ctx, opts)
	if err == nil {
		t.Fatal("simulated post-commit acknowledgement error was not returned")
	}
	guard, found, guardErr := controlstore.ReadTargetGuard(f.ctx, f.ctrl, key.String())
	if guardErr != nil || !found || guard.State != controlstore.TargetGuardClean || guard.OperationID != opts.OperationID || guard.AttemptAppName != result.Application {
		t.Fatalf("ambiguous accepted attempt lost valid clean state: guard=%+v found=%v err=%v runErr=%v", guard, found, guardErr, err)
	}
	var persisted int
	if err := f.ctrl.QueryRow(f.ctx, `SELECT count(*) FROM target_writer_test_acceptance WHERE row_ref=$1`, "test:target_writer_acceptance/"+result.Application).Scan(&persisted); err != nil || persisted != 1 {
		t.Fatalf("accepted evidence missing after ambiguous acknowledgement: count=%d err=%v", persisted, err)
	}
}

func TestBoundTargetWriterRequiresTransactionalAdvancedMarkerToken(t *testing.T) {
	f := newTargetWriterFixture(t)
	dsn := f.createTarget(t)
	target, err := controlstore.ParseDSNTarget(dsn)
	if err != nil {
		t.Fatal(err)
	}
	key, _ := CanonicalTargetKey(target)
	initCleanWriterGuard(t, f.ctrl, key)
	opened, err := f.store.OpenInstance(f.ctx, controlstore.OpenInstanceRequest{
		Kind: "recovery", OpenedBy: "deploy:writer",
		TargetGuardKey: key.String(), TargetRoleFingerprint: target.DataTargetFingerprint().RoleFingerprint,
	})
	if err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(t.TempDir(), "bound-writer-started")
	opts := writerOptions(f, dsn, target, "writer-bound-no-marker", writerScript(t, 0, marker))
	opts.InstanceID = opened.InstanceID
	if _, err := runTargetWriter(context.Background(), opts); err == nil {
		t.Fatal("bound restore without a prelaunch marker hook was accepted")
	}
	hookCalled := false
	opts.OperationID = "writer-bound-unchanged-marker"
	opts.Prelaunch = func(_ context.Context, _ pgx.Tx, locked controlstore.InstanceToken) (EvidenceToken, error) {
		hookCalled = true
		return LockedEvidenceToken(locked), nil
	}
	if _, err := runTargetWriter(context.Background(), opts); err == nil {
		t.Fatal("bound restore accepted a prelaunch hook without an advanced persisted marker token")
	}
	if !hookCalled {
		t.Fatal("bound prelaunch hook was not invoked under the instance transaction")
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("bound restore child launched without a committed marker token, stat err=%v", err)
	}
	guard, found, err := controlstore.ReadTargetGuard(f.ctx, f.ctrl, key.String())
	if err != nil || !found || guard.State != controlstore.TargetGuardClean {
		t.Fatalf("failed prelaunch transaction changed the target guard: %+v found=%v err=%v", guard, found, err)
	}
}

func TestBoundTargetWriterChecksPersistedRoleBeforeHookOrChild(t *testing.T) {
	f := newTargetWriterFixture(t)
	dsn := f.createTarget(t)
	target, err := controlstore.ParseDSNTarget(dsn)
	if err != nil {
		t.Fatal(err)
	}
	key, _ := CanonicalTargetKey(target)
	initCleanWriterGuard(t, f.ctrl, key)
	wrongBinding := target
	wrongBinding.Role = "different_persisted_role"
	opened, err := f.store.OpenInstance(f.ctx, controlstore.OpenInstanceRequest{
		Kind: "recovery", OpenedBy: "deploy:writer",
		TargetGuardKey: key.String(), TargetRoleFingerprint: wrongBinding.DataTargetFingerprint().RoleFingerprint,
	})
	if err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(t.TempDir(), "persisted-role-child")
	opts := writerOptions(f, dsn, target, "writer-persisted-role", writerScript(t, 0, marker))
	opts.InstanceID = opened.InstanceID
	hookCalled := false
	opts.Prelaunch = func(context.Context, pgx.Tx, controlstore.InstanceToken) (EvidenceToken, error) {
		hookCalled = true
		return EvidenceToken{}, nil
	}
	if _, err := runTargetWriter(context.Background(), opts); err == nil {
		t.Fatal("persisted role mismatch was accepted")
	}
	if hookCalled {
		t.Fatal("prelaunch marker hook ran for a mismatched persisted target role")
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("child launched for a persisted role mismatch, stat err=%v", err)
	}
}

func TestBoundTargetWriterRunsMarkerAndAcceptanceEvidenceInCoordinatorTransactions(t *testing.T) {
	f := newTargetWriterFixture(t)
	dsn := f.createTarget(t)
	target, err := controlstore.ParseDSNTarget(dsn)
	if err != nil {
		t.Fatal(err)
	}
	key, _ := CanonicalTargetKey(target)
	initCleanWriterGuard(t, f.ctrl, key)
	opened, err := f.store.OpenInstance(f.ctx, controlstore.OpenInstanceRequest{
		Kind: "recovery", OpenedBy: "deploy:writer",
		TargetGuardKey: key.String(), TargetRoleFingerprint: target.DataTargetFingerprint().RoleFingerprint,
	})
	if err != nil {
		t.Fatal(err)
	}
	opts := writerOptions(f, dsn, target, "writer-bound-complete", "/bin/true")
	opts.InstanceID = opened.InstanceID
	probeEvidenceRef := "control:recovery_evidence/target-writer-probe-" + newUUIDString()
	opts.Prelaunch = func(ctx context.Context, tx pgx.Tx, locked controlstore.InstanceToken) (EvidenceToken, error) {
		outcome, err := CommitEvidenceWriteTx(ctx, tx, EvidenceWriteRequest{
			InstanceID: locked.InstanceID, Token: LockedEvidenceToken(locked),
			Kind: MutationRestoreStarted, Actor: "deploy:writer", OperationID: opts.OperationID,
			ResultDigest: []byte("marker"),
			Apply: func(ctx context.Context, tx pgx.Tx, accepted EvidenceToken) error {
				_, err := tx.Exec(ctx, `INSERT INTO target_writer_test_marker (instance_id, generation) VALUES ($1, $2)`, accepted.InstanceID, accepted.Generation)
				return err
			},
		})
		if err != nil {
			return EvidenceToken{}, err
		}
		if outcome.Discarded {
			return EvidenceToken{}, errors.New("marker evidence was discarded")
		}
		return outcome.Token, nil
	}
	opts.Acceptance = func(ctx context.Context, tx pgx.Tx, proof TargetWriterProof) (TargetWriterAcceptance, error) {
		outcome, err := CommitEvidenceWriteTx(ctx, tx, EvidenceWriteRequest{
			InstanceID: proof.MarkerToken.InstanceID, Token: proof.MarkerToken,
			Kind: MutationRestoreProbeAccepted, Actor: "deploy:writer", OperationID: opts.OperationID,
			ResultDigest: proof.Probe.Evidence,
			Apply: func(ctx context.Context, tx pgx.Tx, accepted EvidenceToken) error {
				var txID int64
				if err := tx.QueryRow(ctx, `SELECT txid_current()`).Scan(&txID); err != nil {
					return err
				}
				ref := "test:bound_acceptance/" + proof.Application
				_, err := tx.Exec(ctx, `INSERT INTO target_writer_test_acceptance (row_ref, transaction_id) VALUES ($1, $2)`, ref, txID)
				if err != nil {
					return err
				}
				return insertEvidence(ctx, tx, newUUIDString(), accepted.InstanceID, accepted.Generation,
					"restore_probe", []byte(`{"writer_test":true}`), "sha256:writer-test", probeEvidenceRef, "deploy:writer")
			},
		})
		if err != nil {
			return TargetWriterAcceptance{}, err
		}
		if outcome.Discarded {
			return TargetWriterAcceptance{}, errors.New("acceptance evidence was discarded")
		}
		var txID int64
		if err := tx.QueryRow(ctx, `SELECT txid_current()`).Scan(&txID); err != nil {
			return TargetWriterAcceptance{}, err
		}
		return TargetWriterAcceptance{
			AcceptedRowRefs: []string{"test:bound_acceptance/" + proof.Application, probeEvidenceRef},
			TransactionID:   txID, AcceptedEvidenceToken: outcome.Token,
		}, nil
	}
	if _, err := runTargetWriter(context.Background(), opts); err != nil {
		t.Fatalf("bound target writer: %v", err)
	}
	var markerCount, acceptedCount int
	if err := f.ctrl.QueryRow(f.ctx, `SELECT count(*) FROM target_writer_test_marker WHERE instance_id=$1`, opened.InstanceID).Scan(&markerCount); err != nil {
		t.Fatal(err)
	}
	if err := f.ctrl.QueryRow(f.ctx, `SELECT count(*) FROM target_writer_test_acceptance WHERE row_ref LIKE 'test:bound_acceptance/%'`).Scan(&acceptedCount); err != nil {
		t.Fatal(err)
	}
	if markerCount != 1 || acceptedCount != 1 {
		t.Fatalf("expected atomic marker and accepted rows: marker=%d accepted=%d", markerCount, acceptedCount)
	}

	// A discarded transaction-scoped evidence result reports the observed
	// marker token; even a callback attempting to attach receipt fields cannot
	// make that unchanged token eligible for clean disposition.
	opts.OperationID = "writer-bound-discarded-acceptance"
	opts.Acceptance = func(ctx context.Context, tx pgx.Tx, proof TargetWriterProof) (TargetWriterAcceptance, error) {
		stale := proof.MarkerToken
		stale.Hash = "sha256:" + strings.Repeat("0", 64)
		outcome, err := CommitEvidenceWriteTx(ctx, tx, EvidenceWriteRequest{
			InstanceID: proof.MarkerToken.InstanceID, Token: stale,
			Kind: MutationRestoreProbeAccepted, Actor: "deploy:writer", OperationID: opts.OperationID,
			Apply: func(context.Context, pgx.Tx, EvidenceToken) error { return nil },
		})
		if err != nil {
			return TargetWriterAcceptance{}, err
		}
		var txID int64
		if err := tx.QueryRow(ctx, `SELECT txid_current()`).Scan(&txID); err != nil {
			return TargetWriterAcceptance{}, err
		}
		return TargetWriterAcceptance{
			AcceptedRowRefs: []string{"test:discarded/" + proof.Application},
			TransactionID:   txID, AcceptedEvidenceToken: outcome.Token,
		}, nil
	}
	if _, err := runTargetWriter(context.Background(), opts); err == nil {
		t.Fatal("discarded evidence token was accepted as clean target evidence")
	}
	guard, found, err := controlstore.ReadTargetGuard(f.ctx, f.ctrl, key.String())
	if err != nil || !found || guard.State == controlstore.TargetGuardClean {
		t.Fatalf("discarded acceptance unexpectedly left a clean guard: %+v found=%v err=%v", guard, found, err)
	}
}

func TestBoundRestoreRejectsAdvancedAcceptanceWithoutRestoreProbeRow(t *testing.T) {
	f := newTargetWriterFixture(t)
	dsn := f.createTarget(t)
	target, err := controlstore.ParseDSNTarget(dsn)
	if err != nil {
		t.Fatal(err)
	}
	key, _ := CanonicalTargetKey(target)
	initCleanWriterGuard(t, f.ctrl, key)
	opened, err := f.store.OpenInstance(f.ctx, controlstore.OpenInstanceRequest{
		Kind: "recovery", OpenedBy: "deploy:writer",
		TargetGuardKey: key.String(), TargetRoleFingerprint: target.DataTargetFingerprint().RoleFingerprint,
	})
	if err != nil {
		t.Fatal(err)
	}
	opts := writerOptions(f, dsn, target, "writer-bound-noop-probe-evidence", "/bin/true")
	opts.InstanceID = opened.InstanceID
	opts.Prelaunch = func(ctx context.Context, tx pgx.Tx, locked controlstore.InstanceToken) (EvidenceToken, error) {
		write, err := CommitEvidenceWriteTx(ctx, tx, EvidenceWriteRequest{
			InstanceID: locked.InstanceID, Token: LockedEvidenceToken(locked), Kind: MutationRestoreStarted,
			Actor: "deploy:writer", OperationID: opts.OperationID,
			Apply: func(context.Context, pgx.Tx, EvidenceToken) error { return nil },
		})
		if err != nil || write.Discarded {
			return EvidenceToken{}, errors.New("restore marker was not accepted")
		}
		return write.Token, nil
	}
	opts.Acceptance = func(ctx context.Context, tx pgx.Tx, proof TargetWriterProof) (TargetWriterAcceptance, error) {
		write, err := CommitEvidenceWriteTx(ctx, tx, EvidenceWriteRequest{
			InstanceID: opened.InstanceID, Token: proof.MarkerToken, Kind: MutationRestoreProbeAccepted,
			Actor: "deploy:writer", OperationID: opts.OperationID,
			Apply: func(context.Context, pgx.Tx, EvidenceToken) error { return nil },
		})
		if err != nil || write.Discarded {
			return TargetWriterAcceptance{}, errors.New("restore acceptance was not advanced")
		}
		var txID int64
		if err := tx.QueryRow(ctx, `SELECT txid_current()`).Scan(&txID); err != nil {
			return TargetWriterAcceptance{}, err
		}
		return TargetWriterAcceptance{AcceptedRowRefs: []string{"control:recovery_evidence/forged"},
			TransactionID: txID, AcceptedEvidenceToken: write.Token}, nil
	}
	if _, err := runTargetWriter(context.Background(), opts); err == nil {
		t.Fatal("advanced restore acceptance without a same-transaction restore_probe row was accepted")
	}
	guard, found, err := controlstore.ReadTargetGuard(f.ctx, f.ctrl, key.String())
	if err != nil || !found || guard.State == controlstore.TargetGuardClean {
		t.Fatalf("no-op restore evidence unexpectedly left a clean guard: guard=%+v found=%v err=%v", guard, found, err)
	}
}

func TestUnboundVerifyRejectsCollidingInstanceAcrossPrepareCommit(t *testing.T) {
	t.Run("instance opens first", func(t *testing.T) {
		f := newTargetWriterFixture(t)
		dsn := f.createTarget(t)
		target, err := controlstore.ParseDSNTarget(dsn)
		if err != nil {
			t.Fatal(err)
		}
		key, _ := CanonicalTargetKey(target)
		initCleanWriterGuard(t, f.ctrl, key)
		opts := writerOptions(f, dsn, target, "verify-binding-open-first", writerScript(t, 0, filepath.Join(t.TempDir(), "must-not-start")))
		opts.OperationKind = TargetWriterOperationVerifyBackup
		if _, err := f.store.OpenInstance(f.ctx, controlstore.OpenInstanceRequest{Kind: "recovery", OpenedBy: "deploy:collision",
			TargetGuardKey: key.String(), TargetRoleFingerprint: target.DataTargetFingerprint().RoleFingerprint}); err != nil {
			t.Fatal(err)
		}
		if _, err := runTargetWriter(context.Background(), opts); err == nil {
			t.Fatal("stale opaque binding was used after a colliding instance opened")
		}
	})

	t.Run("verification prepare commits first", func(t *testing.T) {
		f := newTargetWriterFixture(t)
		dsn := f.createTarget(t)
		target, err := controlstore.ParseDSNTarget(dsn)
		if err != nil {
			t.Fatal(err)
		}
		key, _ := CanonicalTargetKey(target)
		initCleanWriterGuard(t, f.ctrl, key)
		marker := filepath.Join(t.TempDir(), "verification-started")
		opts := writerOptions(f, dsn, target, "verify-binding-writer-first", writerScript(t, 0.8, marker))
		opts.OperationKind = TargetWriterOperationVerifyBackup
		opts.Acceptance = func(ctx context.Context, tx pgx.Tx, _ TargetWriterProof) (TargetWriterAcceptance, error) {
			ref := "control:recovery_evidence/prepare-commit-test"
			if err := insertUnboundVerifyEvidenceTx(ctx, tx, newUUIDString(), []byte(`{"backup_id":"prepare-commit-test"}`), "sha256:prepare-commit", ref, "deploy:verify"); err != nil {
				return TargetWriterAcceptance{}, err
			}
			var txID int64
			if err := tx.QueryRow(ctx, `SELECT txid_current()`).Scan(&txID); err != nil {
				return TargetWriterAcceptance{}, err
			}
			return TargetWriterAcceptance{AcceptedRowRefs: []string{ref}, TransactionID: txID}, nil
		}
		writerDone := make(chan error, 1)
		go func() { _, err := runTargetWriter(f.ctx, opts); writerDone <- err }()
		waitForFile(t, marker, 5*time.Second) // preflight passed; child is held open
		if _, err := f.store.OpenInstance(f.ctx, controlstore.OpenInstanceRequest{Kind: "recovery", OpenedBy: "deploy:collision",
			TargetGuardKey: key.String(), TargetRoleFingerprint: target.DataTargetFingerprint().RoleFingerprint}); err == nil {
			t.Fatal("colliding instance opened while verification launch intent left the guard dirty")
		}
		if err := <-writerDone; err != nil {
			t.Fatalf("unbound verification: %v", err)
		}
		guard, found, err := controlstore.ReadTargetGuard(f.ctx, f.ctrl, key.String())
		if err != nil || !found || guard.State != controlstore.TargetGuardClean {
			t.Fatalf("accepted verification did not leave clean guard: guard=%+v found=%v err=%v", guard, found, err)
		}
	})
}

func TestVerifyInventoryRecheckWaitsForUncommittedOpenInstance(t *testing.T) {
	f := newTargetWriterFixture(t)
	dsn := f.createTarget(t)
	target, err := controlstore.ParseDSNTarget(dsn)
	if err != nil {
		t.Fatal(err)
	}
	key, err := CanonicalTargetKey(target)
	if err != nil {
		t.Fatal(err)
	}
	initCleanWriterGuard(t, f.ctrl, key)
	marker := filepath.Join(t.TempDir(), "must-not-launch")
	opts := writerOptions(f, dsn, target, "verify-open-commit-gap", writerScript(t, 0, marker))
	opts.OperationKind = TargetWriterOperationVerifyBackup

	// Block Store.OpenInstance in its final audit write. At this point it has
	// inserted the open instance and validated the clean target guard, but its
	// transaction (and inventory row) are still uncommitted.
	const barrierKey int64 = 912734109
	barrier, err := pgx.Connect(f.ctx, f.ctrlDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer barrier.Close(context.Background())
	if _, err := barrier.Exec(f.ctx, `SELECT pg_advisory_lock($1)`, barrierKey); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = barrier.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, barrierKey) }()
	if _, err := f.admin.Exec(f.ctx, `CREATE FUNCTION target_writer_open_barrier() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.action = 'instance_open' THEN
    PERFORM pg_advisory_xact_lock(912734109);
  END IF;
  RETURN NEW;

END $$`); err != nil {
		t.Fatal(err)
	}
	if _, err := f.admin.Exec(f.ctx, `CREATE TRIGGER target_writer_open_barrier BEFORE INSERT ON recovery_audit
FOR EACH ROW EXECUTE FUNCTION target_writer_open_barrier()`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = f.admin.Exec(context.Background(), `DROP TRIGGER IF EXISTS target_writer_open_barrier ON recovery_audit`)
		_, _ = f.admin.Exec(context.Background(), `DROP FUNCTION IF EXISTS target_writer_open_barrier()`)
	})

	openDone := make(chan error, 1)
	go func() {
		_, err := f.store.OpenInstance(f.ctx, controlstore.OpenInstanceRequest{Kind: "recovery", OpenedBy: "deploy:barrier",
			TargetGuardKey: key.String(), TargetRoleFingerprint: target.DataTargetFingerprint().RoleFingerprint})
		openDone <- err
	}()
	waitForAdvisoryWaiters(t, f.admin, 1, 5*time.Second)

	verifyDone := make(chan error, 1)
	go func() { _, err := runTargetWriter(f.ctx, opts); verifyDone <- err }()
	waitForAdvisoryWaiters(t, f.admin, 2, 5*time.Second)

	if _, err := barrier.Exec(f.ctx, `SELECT pg_advisory_unlock($1)`, barrierKey); err != nil {
		t.Fatal(err)
	}
	if err := <-openDone; err != nil {
		t.Fatalf("Store.OpenInstance did not commit: %v", err)
	}
	if err := <-verifyDone; err == nil {
		t.Fatal("verification accepted inventory that committed while it waited for the target guard")
	}
	assertWriterChildNotStarted(t, marker)
	guard, found, err := controlstore.ReadTargetGuard(f.ctx, f.ctrl, key.String())
	if err != nil || !found || guard.State != controlstore.TargetGuardClean {
		t.Fatalf("prelaunch refusal did not preserve the clean guard: guard=%+v found=%v err=%v", guard, found, err)
	}
}

func waitForAdvisoryWaiters(t *testing.T, pool *pgxpool.Pool, minimum int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var waiting int
		if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM pg_stat_activity
WHERE datname = current_database() AND wait_event_type = 'Lock' AND wait_event = 'advisory'`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting >= minimum {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	rows, err := pool.Query(context.Background(), `SELECT wait_event_type, wait_event, query FROM pg_stat_activity
WHERE datname = current_database() AND wait_event_type = 'Lock'`)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var eventType, event, query string
			if rows.Scan(&eventType, &event, &query) == nil {
				t.Logf("database lock waiter: type=%s event=%s query=%s", eventType, event, query)
			}
		}
	}
	t.Fatalf("timed out waiting for %d advisory-lock waiters", minimum)
}

func TestBoundVerifyBackupAcceptsManifestEvidenceWithoutRestoreStarted(t *testing.T) {
	f := newTargetWriterFixture(t)
	dsn := f.createTarget(t)
	target, err := controlstore.ParseDSNTarget(dsn)
	if err != nil {
		t.Fatal(err)
	}
	key, _ := CanonicalTargetKey(target)
	initCleanWriterGuard(t, f.ctrl, key)
	authoritativeDSN := f.createTarget(t)
	authoritative, err := controlstore.ParseDSNTarget(authoritativeDSN)
	if err != nil {
		t.Fatal(err)
	}
	authoritativeKey, _ := CanonicalTargetKey(authoritative)
	initCleanWriterGuard(t, f.ctrl, authoritativeKey)
	opened, err := f.store.OpenInstance(f.ctx, controlstore.OpenInstanceRequest{
		Kind: "recovery", OpenedBy: "deploy:verify",
		TargetGuardKey: authoritativeKey.String(), TargetRoleFingerprint: authoritative.DataTargetFingerprint().RoleFingerprint,
	})
	if err != nil {
		t.Fatal(err)
	}
	opts := writerOptions(f, dsn, target, "verify-bound-manifest", "/bin/true")
	opts.OperationKind = TargetWriterOperationVerifyBackup
	opts.InstanceID = opened.InstanceID
	setVerifyBinding(t, f, &opts, authoritativeDSN)
	opts.Acceptance = func(ctx context.Context, tx pgx.Tx, proof TargetWriterProof) (TargetWriterAcceptance, error) {
		ref := "control:recovery_evidence/" + newUUIDString()
		scope := []byte(`{"backup_id":"controlled-test-backup"}`)
		write, err := CommitEvidenceWriteTx(ctx, tx, EvidenceWriteRequest{
			InstanceID: opened.InstanceID, Token: proof.MarkerToken,
			Kind: MutationEvidenceSnapshotAccepted, Actor: "deploy:verify",
			OperationID: opts.OperationID, Reason: "backup verification accepted",
			Apply: func(ctx context.Context, tx pgx.Tx, next EvidenceToken) error {
				return insertEvidence(ctx, tx, strings.TrimPrefix(ref, "control:recovery_evidence/"), next.InstanceID,
					next.Generation, "backup_manifest", scope, "sha256:controlled", ref, "deploy:verify")
			},
		})
		if err != nil || write.Discarded {
			return TargetWriterAcceptance{}, errors.New("verification evidence write was discarded")
		}
		var txID int64
		if err := tx.QueryRow(ctx, `SELECT txid_current()`).Scan(&txID); err != nil {
			return TargetWriterAcceptance{}, err
		}
		return TargetWriterAcceptance{AcceptedRowRefs: []string{ref}, TransactionID: txID, AcceptedEvidenceToken: write.Token}, nil
	}
	if _, err := runTargetWriter(context.Background(), opts); err != nil {
		t.Fatalf("bound verify_backup: %v", err)
	}
	var manifests, markers, acceptedAudits int
	if err := f.ctrl.QueryRow(f.ctx, `SELECT count(*) FROM recovery_evidence WHERE instance_id=$1 AND kind='backup_manifest'`, opened.InstanceID).Scan(&manifests); err != nil {
		t.Fatal(err)
	}
	if err := f.ctrl.QueryRow(f.ctx, `SELECT count(*) FROM recovery_audit WHERE instance_id=$1 AND action=$2 AND target->>'kind'=$3`, opened.InstanceID, ActionEvidenceWrite, string(MutationRestoreStarted)).Scan(&markers); err != nil {
		t.Fatal(err)
	}
	var acceptedGeneration int64
	if err := f.ctrl.QueryRow(f.ctx, `SELECT evidence_generation FROM recovery_instance WHERE instance_id=$1`, opened.InstanceID).Scan(&acceptedGeneration); err != nil {
		t.Fatal(err)
	}
	if err := f.ctrl.QueryRow(f.ctx, `SELECT count(*) FROM recovery_audit WHERE instance_id=$1 AND action=$2 AND target->>'kind'=$3 AND target->>'accepted_generation'=$4`,
		opened.InstanceID, ActionEvidenceWrite, string(MutationEvidenceSnapshotAccepted), strconv.FormatInt(acceptedGeneration, 10)).Scan(&acceptedAudits); err != nil {
		t.Fatal(err)
	}
	guard, found, err := controlstore.ReadTargetGuard(f.ctx, f.ctrl, key.String())
	if err != nil || !found || guard.State != controlstore.TargetGuardClean || manifests != 1 || markers != 0 || acceptedAudits != 1 {
		t.Fatalf("verify result: guard=%+v found=%v manifests=%d restore_started=%d accepted_audits=%d err=%v", guard, found, manifests, markers, acceptedAudits, err)
	}
}

func TestBoundVerifyOwnerBackendKilledBeforeAcceptanceCommitRollsBack(t *testing.T) {
	f := newTargetWriterFixture(t)
	dsn := f.createTarget(t)
	target, err := controlstore.ParseDSNTarget(dsn)
	if err != nil {
		t.Fatal(err)
	}
	key, _ := CanonicalTargetKey(target)
	initCleanWriterGuard(t, f.ctrl, key)
	authoritativeDSN := f.authoritativeDSN
	authoritative, err := controlstore.ParseDSNTarget(authoritativeDSN)
	if err != nil {
		t.Fatal(err)
	}
	authoritativeKey, _ := CanonicalTargetKey(authoritative)
	initCleanWriterGuard(t, f.ctrl, authoritativeKey)
	opened, err := f.store.OpenInstance(f.ctx, controlstore.OpenInstanceRequest{
		Kind: "recovery", OpenedBy: "deploy:verify",
		TargetGuardKey: authoritativeKey.String(), TargetRoleFingerprint: authoritative.DataTargetFingerprint().RoleFingerprint,
	})
	if err != nil {
		t.Fatal(err)
	}
	opts := writerOptions(f, dsn, target, "verify-owner-killed", "/bin/true")
	opts.OperationKind, opts.InstanceID = TargetWriterOperationVerifyBackup, opened.InstanceID
	setVerifyBinding(t, f, &opts, authoritativeDSN)
	artifactRef := "control:recovery_evidence/" + newUUIDString()
	entered := make(chan int32, 1)
	resume := make(chan struct{})
	opts.Acceptance = func(ctx context.Context, tx pgx.Tx, proof TargetWriterProof) (TargetWriterAcceptance, error) {
		write, err := CommitEvidenceWriteTx(ctx, tx, EvidenceWriteRequest{
			InstanceID: opened.InstanceID, Token: proof.MarkerToken,
			Kind: MutationEvidenceSnapshotAccepted, Actor: "deploy:verify", OperationID: opts.OperationID,
			Reason: "verification evidence staged before owner backend termination",
			Apply: func(ctx context.Context, tx pgx.Tx, next EvidenceToken) error {
				return insertEvidence(ctx, tx, strings.TrimPrefix(artifactRef, "control:recovery_evidence/"), next.InstanceID,
					next.Generation, "backup_manifest", []byte(`{"backup_id":"owner-kill"}`), "sha256:owner-kill", artifactRef, "deploy:verify")
			},
		})
		if err != nil {
			return TargetWriterAcceptance{}, fmt.Errorf("verification evidence write failed: %w", err)
		}
		if write.Discarded {
			return TargetWriterAcceptance{}, errors.New("verification evidence write was discarded")
		}
		var backendPID int32
		if err := tx.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&backendPID); err != nil {
			return TargetWriterAcceptance{}, err
		}
		entered <- backendPID
		select {
		case <-resume:
		case <-ctx.Done():
			return TargetWriterAcceptance{}, ctx.Err()
		}
		var txID int64
		if err := tx.QueryRow(ctx, `SELECT txid_current()`).Scan(&txID); err != nil {
			return TargetWriterAcceptance{}, err
		}
		return TargetWriterAcceptance{AcceptedRowRefs: []string{artifactRef}, TransactionID: txID, AcceptedEvidenceToken: write.Token}, nil
	}
	writerDone := make(chan error, 1)
	go func() { _, err := runTargetWriter(f.ctx, opts); writerDone <- err }()
	var ownerPID int32
	select {
	case ownerPID = <-entered:
	case err := <-writerDone:
		t.Fatalf("target writer failed before acceptance callback: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("acceptance callback did not start")
	}
	var terminated bool
	if err := f.admin.QueryRow(f.ctx, `SELECT pg_terminate_backend($1)`, ownerPID).Scan(&terminated); err != nil || !terminated {
		t.Fatalf("could not terminate target-lock owner backend %d: terminated=%v err=%v", ownerPID, terminated, err)
	}
	close(resume)
	if err := <-writerDone; err == nil {
		t.Fatal("verification succeeded after its target-lock owner backend was terminated before commit")
	}
	var persisted int
	if err := f.ctrl.QueryRow(f.ctx, `SELECT count(*) FROM recovery_evidence WHERE instance_id=$1 AND artifact_ref=$2`, opened.InstanceID, artifactRef).Scan(&persisted); err != nil {
		t.Fatal(err)
	}
	guard, found, err := controlstore.ReadTargetGuard(f.ctx, f.ctrl, key.String())
	if err != nil || !found || guard.State == controlstore.TargetGuardClean || persisted != 0 {
		t.Fatalf("terminated owner transaction leaked acceptance: guard=%+v found=%v evidence=%d err=%v", guard, found, persisted, err)
	}
}

func TestBoundVerifyBackupRejectsStaleTokenAndNoOpAcceptance(t *testing.T) {
	for _, mode := range []string{"stale", "noop"} {
		t.Run(mode, func(t *testing.T) {
			f := newTargetWriterFixture(t)
			dsn := f.createTarget(t)
			target, err := controlstore.ParseDSNTarget(dsn)
			if err != nil {
				t.Fatal(err)
			}
			key, _ := CanonicalTargetKey(target)
			initCleanWriterGuard(t, f.ctrl, key)
			authoritativeDSN := f.createTarget(t)
			authoritative, err := controlstore.ParseDSNTarget(authoritativeDSN)
			if err != nil {
				t.Fatal(err)
			}
			authoritativeKey, _ := CanonicalTargetKey(authoritative)
			initCleanWriterGuard(t, f.ctrl, authoritativeKey)
			opened, err := f.store.OpenInstance(f.ctx, controlstore.OpenInstanceRequest{
				Kind: "recovery", OpenedBy: "deploy:verify",
				TargetGuardKey: authoritativeKey.String(), TargetRoleFingerprint: authoritative.DataTargetFingerprint().RoleFingerprint,
			})
			if err != nil {
				t.Fatal(err)
			}
			opts := writerOptions(f, dsn, target, "verify-bound-"+mode, "/bin/true")
			opts.OperationKind, opts.InstanceID = TargetWriterOperationVerifyBackup, opened.InstanceID
			setVerifyBinding(t, f, &opts, authoritativeDSN)
			opts.Acceptance = func(ctx context.Context, tx pgx.Tx, proof TargetWriterProof) (TargetWriterAcceptance, error) {
				var txID int64
				if err := tx.QueryRow(ctx, `SELECT txid_current()`).Scan(&txID); err != nil {
					return TargetWriterAcceptance{}, err
				}
				if mode == "stale" {
					stale := proof.MarkerToken
					stale.Hash = "sha256:" + strings.Repeat("0", 64)
					write, err := CommitEvidenceWriteTx(ctx, tx, EvidenceWriteRequest{
						InstanceID: opened.InstanceID, Token: stale, Kind: MutationEvidenceSnapshotAccepted,
						Actor: "deploy:verify", OperationID: opts.OperationID,
						Apply: func(context.Context, pgx.Tx, EvidenceToken) error { return nil },
					})
					if err != nil {
						return TargetWriterAcceptance{}, err
					}
					return TargetWriterAcceptance{AcceptedRowRefs: []string{"control:recovery_evidence/stale"},
						TransactionID: txID, AcceptedEvidenceToken: write.Token}, nil
				}
				return TargetWriterAcceptance{AcceptedRowRefs: []string{"control:recovery_evidence/noop"}, TransactionID: txID}, nil
			}
			if _, err := runTargetWriter(context.Background(), opts); err == nil {
				t.Fatalf("%s verification acceptance was accepted", mode)
			}
			guard, found, err := controlstore.ReadTargetGuard(f.ctx, f.ctrl, key.String())
			if err != nil || !found || guard.State == controlstore.TargetGuardClean {
				t.Fatalf("%s verification left target clean: guard=%+v found=%v err=%v", mode, guard, found, err)
			}
		})
	}
}

func TestUnboundVerifyBackupRequiresManifestEvidenceAndAudit(t *testing.T) {
	f := newTargetWriterFixture(t)
	dsn := f.createTarget(t)
	target, err := controlstore.ParseDSNTarget(dsn)
	if err != nil {
		t.Fatal(err)
	}
	key, _ := CanonicalTargetKey(target)
	initCleanWriterGuard(t, f.ctrl, key)
	opts := writerOptions(f, dsn, target, "verify-unbound-manifest", "/bin/true")
	opts.OperationKind = TargetWriterOperationVerifyBackup
	opts.Acceptance = func(ctx context.Context, tx pgx.Tx, proof TargetWriterProof) (TargetWriterAcceptance, error) {
		evidenceID := newUUIDString()
		ref := "control:recovery_evidence/" + evidenceID
		scope := []byte(`{"backup_id":"controlled-unbound-test"}`)
		if err := insertUnboundVerifyEvidenceTx(ctx, tx, evidenceID, scope, "sha256:controlled", ref, "deploy:verify"); err != nil {
			return TargetWriterAcceptance{}, err
		}
		var txID int64
		if err := tx.QueryRow(ctx, `SELECT txid_current()`).Scan(&txID); err != nil {
			return TargetWriterAcceptance{}, err
		}
		return TargetWriterAcceptance{AcceptedRowRefs: []string{ref}, TransactionID: txID}, nil
	}
	if _, err := runTargetWriter(context.Background(), opts); err != nil {
		t.Fatalf("unbound verify_backup with manifest evidence and audit: %v", err)
	}
	var evidenceCount, auditCount int
	if err := f.ctrl.QueryRow(f.ctx, `SELECT count(*) FROM recovery_evidence WHERE instance_id IS NULL AND generation=0 AND kind='backup_manifest'`).Scan(&evidenceCount); err != nil {
		t.Fatal(err)
	}
	if err := f.ctrl.QueryRow(f.ctx, `SELECT count(*) FROM recovery_audit WHERE action='verify_backup' AND result='ok'`).Scan(&auditCount); err != nil {
		t.Fatal(err)
	}
	guard, found, err := controlstore.ReadTargetGuard(f.ctx, f.ctrl, key.String())
	if err != nil || !found || guard.State != controlstore.TargetGuardClean || evidenceCount != 1 || auditCount != 1 {
		t.Fatalf("unbound verify result: guard=%+v found=%v evidence=%d audit=%d err=%v", guard, found, evidenceCount, auditCount, err)
	}
}

func TestVerifyBackupPrelaunchRefusalsAndAcceptanceFailures(t *testing.T) {
	t.Run("missing instance binding", func(t *testing.T) {
		f := newTargetWriterFixture(t)
		dsn := f.createTarget(t)
		target, err := controlstore.ParseDSNTarget(dsn)
		if err != nil {
			t.Fatal(err)
		}
		key, _ := CanonicalTargetKey(target)
		initCleanWriterGuard(t, f.ctrl, key)
		marker := filepath.Join(t.TempDir(), "verify-missing-binding-child")
		opts := writerOptions(f, dsn, target, "verify-missing-binding", writerScript(t, 0, marker))
		opts.OperationKind = TargetWriterOperationVerifyBackup
		opts.InstanceID = "00000000-0000-0000-0000-000000000000"
		if _, err := runTargetWriter(f.ctx, opts); err == nil {
			t.Fatal("verification without an open bound instance was accepted")
		}
		assertWriterChildNotStarted(t, marker)
	})

	t.Run("observer mismatch", func(t *testing.T) {
		f := newTargetWriterFixture(t)
		dsn := f.createTarget(t)
		otherDSN := f.createTarget(t)
		target, err := controlstore.ParseDSNTarget(dsn)
		if err != nil {
			t.Fatal(err)
		}
		key, _ := CanonicalTargetKey(target)
		initCleanWriterGuard(t, f.ctrl, key)
		marker := filepath.Join(t.TempDir(), "verify-observer-mismatch-child")
		opts := writerOptions(f, dsn, target, "verify-observer-mismatch", writerScript(t, 0, marker))
		opts.OperationKind, opts.ObserverDSN = TargetWriterOperationVerifyBackup, otherDSN
		if _, err := runTargetWriter(f.ctx, opts); err == nil {
			t.Fatal("verification with a mismatched observer was accepted")
		}
		assertWriterChildNotStarted(t, marker)
	})

	t.Run("unknown guard", func(t *testing.T) {
		f := newTargetWriterFixture(t)
		dsn := f.createTarget(t)
		target, err := controlstore.ParseDSNTarget(dsn)
		if err != nil {
			t.Fatal(err)
		}
		marker := filepath.Join(t.TempDir(), "verify-unknown-guard-child")
		opts := writerOptions(f, dsn, target, "verify-unknown-guard", writerScript(t, 0, marker))
		opts.OperationKind = TargetWriterOperationVerifyBackup
		if _, err := runTargetWriter(f.ctx, opts); err == nil {
			t.Fatal("verification with a missing/unknown target guard was accepted")
		}
		assertWriterChildNotStarted(t, marker)
	})

	for _, failure := range []string{"publication failure", "evidence failure"} {
		t.Run(failure, func(t *testing.T) {
			f := newTargetWriterFixture(t)
			dsn := f.createTarget(t)
			target, err := controlstore.ParseDSNTarget(dsn)
			if err != nil {
				t.Fatal(err)
			}
			key, _ := CanonicalTargetKey(target)
			initCleanWriterGuard(t, f.ctrl, key)
			authoritativeDSN := f.createTarget(t)
			authoritative, err := controlstore.ParseDSNTarget(authoritativeDSN)
			if err != nil {
				t.Fatal(err)
			}
			authoritativeKey, _ := CanonicalTargetKey(authoritative)
			initCleanWriterGuard(t, f.ctrl, authoritativeKey)
			opened, err := f.store.OpenInstance(f.ctx, controlstore.OpenInstanceRequest{
				Kind: "recovery", OpenedBy: "deploy:verify",
				TargetGuardKey: authoritativeKey.String(), TargetRoleFingerprint: authoritative.DataTargetFingerprint().RoleFingerprint,
			})
			if err != nil {
				t.Fatal(err)
			}
			marker := filepath.Join(t.TempDir(), "verify-acceptance-failure-child")
			opts := writerOptions(f, dsn, target, "verify-acceptance-failure", writerScript(t, 0, marker))
			opts.OperationKind, opts.InstanceID = TargetWriterOperationVerifyBackup, opened.InstanceID
			setVerifyBinding(t, f, &opts, authoritativeDSN)
			opts.Acceptance = func(context.Context, pgx.Tx, TargetWriterProof) (TargetWriterAcceptance, error) {
				return TargetWriterAcceptance{}, errors.New(failure)
			}
			if _, err := runTargetWriter(f.ctx, opts); err == nil {
				t.Fatalf("verification accepted after %s", failure)
			}
			waitForFile(t, marker, time.Second)
			guard, found, err := controlstore.ReadTargetGuard(f.ctx, f.ctrl, key.String())
			if err != nil || !found || guard.State == controlstore.TargetGuardClean {
				t.Fatalf("failed %s left guard reusable: guard=%+v found=%v err=%v", failure, guard, found, err)
			}
			var restoreStarted int
			if err := f.ctrl.QueryRow(f.ctx, `SELECT count(*) FROM recovery_audit WHERE instance_id=$1 AND action=$2 AND target->>'kind'=$3`,
				opened.InstanceID, ActionEvidenceWrite, string(MutationRestoreStarted)).Scan(&restoreStarted); err != nil {
				t.Fatal(err)
			}
			if restoreStarted != 0 {
				t.Fatalf("verification wrote %d restore_started audit rows", restoreStarted)
			}
		})
	}
}

func assertWriterChildNotStarted(t *testing.T, marker string) {
	t.Helper()
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("verification child launched before refusal, stat err=%v", err)
	}
}

func writerOptions(f *targetWriterFixture, dsn string, target controlstore.DSNTarget, operation, executable string) TargetWriterOptions {
	// Every fixture target is distinct from both control and a separately
	// provisioned authoritative database at the same endpoint. Verify tests may
	// override this with the authority whose instance is actually open.
	authoritativeDSN := f.authoritativeDSN
	binding, err := BindIsolatedTarget(authoritativeDSN, f.ctrlDSN, dsn, nil)
	if err != nil {
		panic(err)
	}
	return TargetWriterOptions{
		OperationKind: TargetWriterOperationRestore,
		Store:         f.store, ControlDSN: f.ctrlDSN, TargetDSN: dsn, ObserverDSN: dsn,
		TrustedTarget: target, IsolatedBinding: binding, AuthoritativeDSN: authoritativeDSN,
		OperationID: operation, Archive: strings.NewReader("archive"),
		executable: executable, LockTimeout: 3 * time.Second, QuiescenceTimeout: time.Second,
		Runner: TargetProcessRunner{DrainTimeout: 2 * time.Second, PollInterval: 10 * time.Millisecond,
			HealthCheckInterval: 20 * time.Millisecond},
		Probe: successfulWriterProbe, Acceptance: successfulWriterAcceptance,
		Convergence: func(ctx context.Context, req ConvergenceRequest) error {
			// Fixture deployment lane: the fixture's admin identity converges
			// ownership to the target role and records the audited prerequisite.
			step := DeploymentConvergence{
				AdminDSN: dsn, TargetDSN: dsn, OriginalRole: target.Role,
				Actor: "deploy:writer", Store: f.store,
			}
			return step.Converge(ctx, req)
		},
	}
}

func setVerifyBinding(t *testing.T, f *targetWriterFixture, opts *TargetWriterOptions, authoritativeDSN string) {
	t.Helper()
	bindings, err := readOpenIsolatedBindings(f.ctx, f.store)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := BindIsolatedTarget(authoritativeDSN, f.ctrlDSN, opts.TargetDSN, bindings)
	if err != nil {
		t.Fatal(err)
	}
	opts.AuthoritativeDSN = authoritativeDSN
	opts.IsolatedBinding = binding
}

func successfulWriterProbe(_ context.Context, proof TargetWriterProof) (TargetWriterProbeResult, error) {
	return TargetWriterProbeResult{
		Outcome: TargetWriterProbePassed, ApplicationName: proof.Application,
		Evidence: []byte(`{"controlled_test_probe":true}`),
	}, nil
}

func successfulWriterAcceptance(ctx context.Context, tx pgx.Tx, proof TargetWriterProof) (TargetWriterAcceptance, error) {
	var txID int64
	if err := tx.QueryRow(ctx, `SELECT txid_current()`).Scan(&txID); err != nil {
		return TargetWriterAcceptance{}, err
	}
	ref := "test:target_writer_acceptance/" + proof.Application
	if _, err := tx.Exec(ctx, `INSERT INTO target_writer_test_acceptance (row_ref, transaction_id) VALUES ($1, $2)`, ref, txID); err != nil {
		return TargetWriterAcceptance{}, err
	}
	return TargetWriterAcceptance{AcceptedRowRefs: []string{ref}, TransactionID: txID}, nil
}

func initCleanWriterGuard(t *testing.T, pool *pgxpool.Pool, key TargetKey) {
	t.Helper()
	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	suffix := key.String()[len(key.String())-8:]
	if err := controlstore.InitializeTargetGuard(context.Background(), tx, key.String(), "writer-init-"+suffix); err != nil {
		t.Fatal(err)
	}
	if err := controlstore.ResolveTargetGuardClean(context.Background(), tx, key.String(), "writer-baseline-"+suffix, []byte(`{"controlled_test_baseline":true}`)); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
}

type targetWriterFixture struct {
	ctx              context.Context
	ctr              *postgres.PostgresContainer
	ctrlDSN          string
	adminDSN         string
	ctrl             *pgxpool.Pool
	admin            *pgxpool.Pool
	store            *controlstore.Store
	authoritativeDSN string
	seq              int
}

func newTargetWriterFixture(t *testing.T) *targetWriterFixture {
	t.Helper()
	ctx := context.Background()
	ctr, err := postgres.Run(ctx, bkpPGImage,
		postgres.WithDatabase(bkpBaseDB), postgres.WithUsername(bkpBaseUser), postgres.WithPassword(bkpBasePass),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2)))
	if err != nil {
		t.Fatalf("start target-writer PostgreSQL fixture: %v", err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })
	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	admin := bkpOpenPool(t, dsn)
	if err := db.MigrateUp(ctx, db.MigrateOptions{DSN: dsn, LockTimeout: 10 * time.Second, ConnectTimeout: 5 * time.Second, FS: schema.FS}, io.Discard); err != nil {
		t.Fatalf("migrate target-writer control database: %v", err)
	}
	store, err := controlstore.NewStore(ctx, admin)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, `CREATE TABLE target_writer_test_acceptance (row_ref text PRIMARY KEY, transaction_id bigint NOT NULL)`); err != nil {
		t.Fatalf("create target-writer test acceptance table: %v", err)
	}
	if _, err := admin.Exec(ctx, `CREATE TABLE target_writer_test_marker (instance_id uuid NOT NULL, generation bigint NOT NULL)`); err != nil {
		t.Fatalf("create target-writer test marker table: %v", err)
	}
	f := &targetWriterFixture{ctx: ctx, ctr: ctr, ctrlDSN: dsn, adminDSN: dsn, ctrl: admin, admin: admin, store: store}
	f.authoritativeDSN = f.createTarget(t)
	return f
}

func (f *targetWriterFixture) createTarget(t *testing.T) string {
	t.Helper()
	f.seq++
	name := fmt.Sprintf("target_writer_%d", f.seq)
	if _, err := f.admin.Exec(f.ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	return bkpDSNFor(t, f.adminDSN, name)
}

// This child-process entry point is selected only by writerScript. Running it
// in the current test binary keeps the PostgreSQL connection on the same
// Linux process tree as the supervised child while still using the test DB.
func TestTargetWriterChildProcess(t *testing.T) {
	if os.Getenv("TXH_TARGET_WRITER_CHILD") != "1" {
		return
	}
	conn, err := pgx.Connect(context.Background(), os.Getenv("TXH_TARGET_WRITER_DSN"))
	if err != nil {
		t.Fatalf("controlled target child connection failed")
	}
	defer conn.Close(context.Background())
	seconds, err := strconv.ParseFloat(os.Getenv("TXH_TARGET_WRITER_SLEEP"), 64)
	if err != nil {
		t.Fatal("invalid child sleep")
	}
	if _, err := conn.Exec(context.Background(), `SELECT pg_sleep($1)`, seconds); err != nil {
		t.Fatalf("controlled target child query failed")
	}
}

func writerScript(t *testing.T, sleep float64, marker string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "controlled-writer")
	script := fmt.Sprintf(`#!/bin/sh
for arg do case "$arg" in --dbname=*) dsn=${arg#--dbname=};; esac; done
touch %q
export TXH_TARGET_WRITER_CHILD=1 TXH_TARGET_WRITER_DSN="$dsn" TXH_TARGET_WRITER_SLEEP=%q
%q -test.run='^TestTargetWriterChildProcess$' &
exit 0
`, marker, strconv.FormatFloat(sleep, 'f', -1, 64), os.Args[0])
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}

func waitForFile(t *testing.T, path string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for controlled child marker %s", filepath.Base(path))
}
