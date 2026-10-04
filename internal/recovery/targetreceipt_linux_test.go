//go:build linux && integration

package recovery

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore/schema"
)

func TestTargetWriterDefaultDriverMatchesPublicRestore(t *testing.T) {
	if got, want := reflect.ValueOf(defaultTargetWriterDriver()).Pointer(), reflect.ValueOf(runTargetWriter).Pointer(); got != want {
		t.Fatalf("public restore default driver pointer=%x, runTargetWriter=%x", got, want)
	}
}

func TestTargetWriterBorrowedLockRejectsWrongKeyAndUnhealthyBeforeIntent(t *testing.T) {
	f := newTargetWriterFixture(t)
	dsn := f.createTarget(t)
	otherDSN := f.createTarget(t)
	target, err := controlstore.ParseDSNTarget(dsn)
	if err != nil {
		t.Fatal(err)
	}
	other, err := controlstore.ParseDSNTarget(otherDSN)
	if err != nil {
		t.Fatal(err)
	}
	key, _ := CanonicalTargetKey(target)
	otherKey, _ := CanonicalTargetKey(other)
	initCleanWriterGuard(t, f.ctrl, key)
	marker := filepath.Join(t.TempDir(), "launched")
	executable := filepath.Join(t.TempDir(), "must-not-launch")
	if err := os.WriteFile(executable, []byte("#!/bin/sh\ntouch "+marker+"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	opts := writerOptions(f, dsn, target, "borrowed-refusal", executable)
	opts.LockTimeout = 100 * time.Millisecond

	wrong, err := AcquireTargetLock(f.ctx, f.ctrlDSN, otherKey, time.Second, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	opts.borrowedLock = wrong
	if _, err := runTargetWriter(f.ctx, opts); err == nil {
		t.Fatal("wrong-key borrowed target lock was accepted")
	}
	_ = wrong.Release(context.Background())

	owned, err := AcquireTargetLock(f.ctx, f.ctrlDSN, key, time.Second, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if err := owned.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	opts.borrowedLock = owned
	if _, err := runTargetWriter(f.ctx, opts); err == nil {
		t.Fatal("closed/unhealthy borrowed target lock was accepted")
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("child launched despite borrowed-lock refusal, stat err=%v", err)
	}
	guard, found, err := controlstore.ReadTargetGuard(f.ctx, f.ctrl, key.String())
	if err != nil || !found || guard.State != controlstore.TargetGuardClean || guard.LaunchIntent {
		t.Fatalf("guard changed before borrowed-lock refusal: %+v found=%t err=%v", guard, found, err)
	}
}

func TestTargetWriterBorrowedLockRejectsWrongControlStoreBeforeIntent(t *testing.T) {
	f := newTargetWriterFixture(t)
	targetDSN := f.createTarget(t)
	target, err := controlstore.ParseDSNTarget(targetDSN)
	if err != nil {
		t.Fatal(err)
	}
	key, err := CanonicalTargetKey(target)
	if err != nil {
		t.Fatal(err)
	}
	initCleanWriterGuard(t, f.ctrl, key)

	wrongControlDSN := f.createTarget(t)
	if err := db.MigrateUp(f.ctx, db.MigrateOptions{DSN: wrongControlDSN, LockTimeout: 10 * time.Second, ConnectTimeout: 5 * time.Second, FS: schema.FS}, io.Discard); err != nil {
		t.Fatal("migrate independent wrong control database")
	}
	wrongPool, err := pgxpool.New(f.ctx, wrongControlDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer wrongPool.Close()
	_, err = controlstore.NewStore(f.ctx, wrongPool)
	if err != nil {
		t.Fatal(err)
	}
	wrongLock, err := AcquireTargetLock(f.ctx, wrongControlDSN, key, time.Second, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = wrongLock.Release(context.Background()) }()
	if err := wrongLock.Health(f.ctx); err != nil {
		t.Fatalf("wrong-database lock should be healthy and held: %v", err)
	}

	marker := filepath.Join(t.TempDir(), "launched")
	executable := filepath.Join(t.TempDir(), "must-not-launch")
	if err := os.WriteFile(executable, []byte("#!/bin/sh\ntouch "+marker+"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	opts := writerOptions(f, targetDSN, target, "wrong-control-store", executable)
	opts.borrowedLock = wrongLock
	var prelaunch, probes, acceptances int
	opts.Prelaunch = func(context.Context, pgx.Tx, controlstore.InstanceToken) (EvidenceToken, error) {
		prelaunch++
		return EvidenceToken{}, nil
	}
	opts.Probe = func(context.Context, TargetWriterProof) (TargetWriterProbeResult, error) {
		probes++
		return TargetWriterProbeResult{}, nil
	}
	opts.Acceptance = func(context.Context, pgx.Tx, TargetWriterProof) (TargetWriterAcceptance, error) {
		acceptances++
		return TargetWriterAcceptance{}, nil
	}
	if _, err := runTargetWriter(f.ctx, opts); err == nil || !strings.Contains(err.Error(), "borrowed target lock control-store identity") {
		t.Fatalf("wrong-control lock refusal should identify control-store identity mismatch, got %v", err)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("child launched despite wrong-control lock refusal, stat err=%v", err)
	}
	if prelaunch != 0 || probes != 0 || acceptances != 0 {
		t.Fatalf("wrong-control lock reached marker/probe/acceptance: %d/%d/%d", prelaunch, probes, acceptances)
	}
	guard, found, err := controlstore.ReadTargetGuard(f.ctx, f.ctrl, key.String())
	if err != nil || !found || guard.State != controlstore.TargetGuardClean || guard.LaunchIntent || guard.ActiveWriter {
		t.Fatalf("guard changed before wrong-control refusal: %+v found=%t err=%v", guard, found, err)
	}
	var acceptedRows int
	if err := f.ctrl.QueryRow(f.ctx, `SELECT count(*) FROM target_writer_test_acceptance`).Scan(&acceptedRows); err != nil || acceptedRows != 0 {
		t.Fatalf("accepted evidence rows=%d err=%v", acceptedRows, err)
	}
	if err := wrongLock.Health(f.ctx); err != nil {
		t.Fatalf("writer released or damaged caller-owned wrong-control lock: %v", err)
	}
}

func TestTargetWriterBorrowedLockRetainsActualCanceledRestoreReceipt(t *testing.T) {
	testNativeCanceledRestoreReceipt(t, true)
}

func TestTargetWriterDefaultLockCancellationReturnsAndReleases(t *testing.T) {
	testNativeCanceledRestoreReceipt(t, false)
}

func testNativeCanceledRestoreReceipt(t *testing.T, borrowed bool) {
	t.Helper()
	pgRestore, err := exec.LookPath("pg_restore")
	if err != nil {
		t.Skipf("NOT RUN: native receipt integration requires local direct pg_restore ELF: %v", err)
	}
	versionOutput, err := exec.Command(pgRestore, "--version").Output()
	if err != nil || !strings.Contains(string(versionOutput), "18.") {
		t.Skipf("NOT RUN: native receipt integration requires PostgreSQL 18 pg_restore, found %q (err=%v)", versionOutput, err)
	}
	pgDump, err := exec.LookPath("pg_dump")
	if err != nil {
		t.Skipf("NOT RUN: native receipt integration requires local pg_dump: %v", err)
	}
	f := newTargetWriterFixture(t)
	ctx, cancel := context.WithCancel(f.ctx)
	defer cancel()
	targetDSN := f.createTarget(t)
	target, err := controlstore.ParseDSNTarget(targetDSN)
	if err != nil {
		t.Fatal(err)
	}
	key, err := CanonicalTargetKey(target)
	if err != nil {
		t.Fatal(err)
	}
	initCleanWriterGuard(t, f.ctrl, key)
	sourceDSN := f.createTarget(t)
	source, err := pgx.Connect(f.ctx, sourceDSN)
	if err != nil {
		t.Fatal(err)
	}
	_, err = source.Exec(f.ctx, `CREATE TABLE public.early_rows(id integer PRIMARY KEY);
CREATE FUNCTION public.drill_pause_check(id integer) RETURNS boolean LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN RETURN true; END $$;
CREATE TABLE public.slow_rows(id integer CONSTRAINT drill_pause CHECK (public.drill_pause_check(id)));
INSERT INTO public.early_rows VALUES (1);
INSERT INTO public.slow_rows VALUES (1);`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.Exec(f.ctx, `CREATE OR REPLACE FUNCTION public.drill_pause_check(id integer) RETURNS boolean LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN PERFORM pg_sleep(3); RETURN true; END $$`); err != nil {
		t.Fatal(err)
	}
	_ = source.Close(f.ctx)
	archivePath := filepath.Join(t.TempDir(), "receipt.dump")
	args := []string{"--format=custom", "--file=" + archivePath, "--dbname=" + sourceDSN}
	cmd := exec.Command(pgDump, args...)
	cleanup, err := protectPGChildArgsWithEnvironment(cmd, "pg_dump", args, os.Environ())
	if err != nil {
		t.Fatal("protect pg_dump test fixture invocation")
	}
	if output, err := cmd.CombinedOutput(); err != nil {
		cleanup()
		t.Fatalf("create real custom archive: %v (%s)", err, output)
	}
	cleanup()
	archive, err := os.Open(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	defer archive.Close()
	var lock *TargetLock
	if borrowed {
		lock, err = AcquireTargetLock(f.ctx, f.ctrlDSN, key, time.Second, time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = lock.Release(context.Background()) }()
	}
	observation := &targetProcessObservation{}
	operation := "default-canceled-restore"
	if borrowed {
		operation = "borrowed-canceled-restore"
	}
	opts := writerOptions(f, targetDSN, target, operation, "")
	opts.Archive = archive
	opts.borrowedLock = lock
	opts.Runner.observation = observation
	opts.Runner.DrainTimeout = 8 * time.Second
	opts.QuiescenceTimeout = 8 * time.Second
	writeDone := make(chan struct {
		result TargetWriterResult
		err    error
	}, 1)
	go func() {
		result, err := runTargetWriter(ctx, opts)
		writeDone <- struct {
			result TargetWriterResult
			err    error
		}{result, err}
	}()

	observer, err := pgx.Connect(f.ctx, targetDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer observer.Close(context.Background())
	deadline := time.Now().Add(60 * time.Second)
	partialAndBlocked := false
	for time.Now().Before(deadline) {
		var earlyRows int
		var blockedBackend bool
		queryErr := observer.QueryRow(f.ctx, `SELECT count(*) FROM public.early_rows`).Scan(&earlyRows)
		if queryErr == nil {
			queryErr = observer.QueryRow(f.ctx, `SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND state='active' AND wait_event_type='Timeout')`).Scan(&blockedBackend)
		}
		if queryErr == nil && earlyRows == 1 && blockedBackend {
			partialAndBlocked = true
			break
		}
		select {
		case completed := <-writeDone:
			t.Fatalf("restore terminated before observing the later blocked archive operation: command=%+v err=%v", completed.result.Command, completed.err)
		case <-time.After(30 * time.Millisecond):
		}
	}
	if !partialAndBlocked {
		cancel()
		t.Fatal("did not observe an earlier committed restore write while the later archive operation was blocked")
	}
	lockWait := make(chan struct {
		lock *TargetLock
		err  error
	}, 1)
	if !borrowed {
		go func() {
			waiter, err := AcquireTargetLock(f.ctx, f.ctrlDSN, key, 12*time.Second, 10*time.Millisecond)
			lockWait <- struct {
				lock *TargetLock
				err  error
			}{waiter, err}
		}()
	}
	cancel()
	var outcome struct {
		result TargetWriterResult
		err    error
	}
	select {
	case outcome = <-writeDone:
	case <-time.After(20 * time.Second):
		t.Fatal("supervised canceled pg_restore did not return after reaping/draining")
	}
	if outcome.err == nil || outcome.result.Command.Outcome != PGCommandCanceled || !outcome.result.Command.Started || !outcome.result.Command.ProcessGroupDrained {
		t.Fatalf("canceled restore result=%+v err=%v", outcome.result.Command, outcome.err)
	}
	receipt := outcome.result.processReceipt
	if receipt == nil || receipt.snapshot.cmd == nil || receipt.snapshot.process == nil || !receipt.snapshot.terminal || receipt.snapshot.startErr != nil || receipt.snapshot.startID == 0 {
		t.Fatalf("actual supervised process receipt incomplete: %+v", receipt)
	}
	if receipt.snapshot.result != outcome.result.Command || receipt.snapshot.cmd.Process != receipt.snapshot.process || receipt.snapshot.pid != receipt.snapshot.process.Pid ||
		receipt.targetKey != key || receipt.roleFingerprint != target.DataTargetFingerprint().RoleFingerprint || receipt.operationID != opts.OperationID {
		t.Fatal("receipt does not identify the actual terminal child and original trusted target/role/operation")
	}
	probeErr := syscall.Kill(receipt.snapshot.pid, 0)
	if receipt.snapshot.cmd.ProcessState == nil || probeErr == nil || !errors.Is(probeErr, syscall.ESRCH) {
		t.Fatalf("retained pg_restore PID %d is not proven gone (cmd=%s/%d)", receipt.snapshot.pid, filepath.Base(pgRestore), receipt.snapshot.startID)
	}
	if borrowed {
		if err := lock.Health(f.ctx); err != nil {
			t.Fatalf("borrowed lock was released or lost by writer coordinator: %v", err)
		}
	} else {
		select {
		case acquired := <-lockWait:
			if acquired.err != nil {
				t.Fatalf("default writer did not release its lock after canceled child drain: %v", acquired.err)
			}
			if err := acquired.lock.Health(f.ctx); err != nil {
				t.Fatalf("successor could not health-check released default lock: %v", err)
			}
			_ = acquired.lock.Release(context.Background())
		case <-time.After(13 * time.Second):
			t.Fatal("default writer retained its target lock after canceled child drain")
		}
	}
	if outcome.result.Probe.Outcome != "" {
		t.Fatal("canceled attempt reached probe/acceptance")
	}
	guard, found, err := controlstore.ReadTargetGuard(f.ctx, f.ctrl, key.String())
	if err != nil || !found || guard.State == controlstore.TargetGuardClean || guard.ActiveWriter {
		t.Fatalf("interrupted attempt guard=%+v found=%t err=%v; it must remain non-clean and inactive", guard, found, err)
	}
	var earlyRows int
	if err := observer.QueryRow(f.ctx, `SELECT count(*) FROM public.early_rows`).Scan(&earlyRows); err != nil || earlyRows != 1 {
		t.Fatalf("earlier committed write after interruption = %d err=%v", earlyRows, err)
	}
}

func TestTargetWriterBorrowedOwnerTerminationDuringRestoreRemainsUnknown(t *testing.T) {
	pgRestore, err := exec.LookPath("pg_restore")
	if err != nil {
		t.Skipf("NOT RUN: native owner-loss integration requires local direct pg_restore ELF: %v", err)
	}
	version, err := exec.Command(pgRestore, "--version").Output()
	if err != nil || !strings.Contains(string(version), "18.") {
		t.Skipf("NOT RUN: native owner-loss integration requires PostgreSQL 18 pg_restore, found %q (err=%v)", version, err)
	}
	pgDump, err := exec.LookPath("pg_dump")
	if err != nil {
		t.Skipf("NOT RUN: native owner-loss integration requires local pg_dump: %v", err)
	}
	f := newTargetWriterFixture(t)
	targetDSN := f.createTarget(t)
	target, err := controlstore.ParseDSNTarget(targetDSN)
	if err != nil {
		t.Fatal(err)
	}
	key, err := CanonicalTargetKey(target)
	if err != nil {
		t.Fatal(err)
	}
	initCleanWriterGuard(t, f.ctrl, key)
	sourceDSN := f.createTarget(t)
	source, err := pgx.Connect(f.ctx, sourceDSN)
	if err != nil {
		t.Fatal(err)
	}
	_, err = source.Exec(f.ctx, `CREATE TABLE public.early_rows(id integer PRIMARY KEY);
CREATE FUNCTION public.drill_pause_check(id integer) RETURNS boolean LANGUAGE plpgsql IMMUTABLE AS $$ BEGIN RETURN true; END $$;
CREATE TABLE public.slow_rows(id integer CONSTRAINT drill_pause CHECK (public.drill_pause_check(id)));
INSERT INTO public.early_rows VALUES (1);
INSERT INTO public.slow_rows VALUES (1);`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.Exec(f.ctx, `CREATE OR REPLACE FUNCTION public.drill_pause_check(id integer) RETURNS boolean LANGUAGE plpgsql IMMUTABLE AS $$ BEGIN PERFORM pg_sleep(90); RETURN true; END $$`); err != nil {
		t.Fatal(err)
	}
	_ = source.Close(f.ctx)
	archivePath := filepath.Join(t.TempDir(), "owner-loss.dump")
	args := []string{"--format=custom", "--file=" + archivePath, "--dbname=" + sourceDSN}
	cmd := exec.Command(pgDump, args...)
	cleanup, err := protectPGChildArgsWithEnvironment(cmd, "pg_dump", args, os.Environ())
	if err != nil {
		t.Fatal("protect pg_dump owner-loss fixture invocation")
	}
	if output, err := cmd.CombinedOutput(); err != nil {
		cleanup()
		t.Fatalf("create real owner-loss custom archive: %v (%s)", err, output)
	}
	cleanup()
	archive, err := os.Open(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	defer archive.Close()
	lock, err := AcquireTargetLock(f.ctx, f.ctrlDSN, key, time.Second, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lock.Release(context.Background()) }()
	k1, k2 := key.AdvisoryLockKey()
	var ownerPID int
	if err := f.admin.QueryRow(f.ctx, `SELECT pid FROM pg_locks WHERE locktype='advisory' AND granted AND objsubid=2 AND classid=$1::oid AND objid=$2::oid LIMIT 1`, uint32(k1), uint32(k2)).Scan(&ownerPID); err != nil {
		t.Fatalf("find exact borrowed lock owner backend: %v", err)
	}
	observer, err := pgx.Connect(f.ctx, targetDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer observer.Close(context.Background())
	observation := &targetProcessObservation{}
	opts := writerOptions(f, targetDSN, target, "borrowed-owner-loss", "")
	opts.Archive = archive
	opts.borrowedLock = lock
	opts.Runner.observation = observation
	opts.Runner.DrainTimeout = 8 * time.Second
	var probes, acceptances int
	opts.Probe = func(ctx context.Context, proof TargetWriterProof) (TargetWriterProbeResult, error) {
		probes++
		return successfulWriterProbe(ctx, proof)
	}
	opts.Acceptance = func(ctx context.Context, tx pgx.Tx, proof TargetWriterProof) (TargetWriterAcceptance, error) {
		acceptances++
		return successfulWriterAcceptance(ctx, tx, proof)
	}
	done := make(chan struct {
		result TargetWriterResult
		err    error
	}, 1)
	go func() {
		result, err := runTargetWriter(f.ctx, opts)
		done <- struct {
			result TargetWriterResult
			err    error
		}{result, err}
	}()
	deadline := time.Now().Add(45 * time.Second)
	blocked := false
	for time.Now().Before(deadline) {
		var sleeping bool
		if err := observer.QueryRow(f.ctx, `SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND state='active' AND wait_event_type='Timeout')`).Scan(&sleeping); err == nil && sleeping {
			blocked = true
			break
		}
		select {
		case outcome := <-done:
			t.Fatalf("writer terminated before real archive SQL blocked: command=%+v err=%v", outcome.result.Command, outcome.err)
		case <-time.After(30 * time.Millisecond):
		}
	}
	if !blocked {
		t.Fatal("did not observe native archive SQL blocked in PostgreSQL 18")
	}
	var terminated bool
	if err := f.admin.QueryRow(f.ctx, `SELECT pg_terminate_backend($1)`, ownerPID).Scan(&terminated); err != nil || !terminated {
		t.Fatalf("terminate exact target-lock owner pid=%d: terminated=%t err=%v", ownerPID, terminated, err)
	}
	var outcome struct {
		result TargetWriterResult
		err    error
	}
	select {
	case outcome = <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("writer did not terminate and reap after target-lock owner loss")
	}
	if outcome.err == nil || outcome.result.Command.Outcome != PGCommandLockLost || !outcome.result.Command.Started || !outcome.result.Command.ProcessGroupDrained {
		t.Fatalf("lock-owner loss result=%+v err=%v; expected drained lock-lost refusal", outcome.result.Command, outcome.err)
	}
	if probes != 0 || acceptances != 0 || outcome.result.Probe.Outcome != "" {
		t.Fatalf("owner loss reached probe/acceptance: probes=%d acceptances=%d result=%+v", probes, acceptances, outcome.result.Probe)
	}
	guard, found, err := controlstore.ReadTargetGuard(f.ctx, f.ctrl, key.String())
	if err != nil || !found || guard.State == controlstore.TargetGuardClean || !guard.ActiveWriter || !guard.LaunchIntent {
		t.Fatalf("owner-loss guard=%+v found=%t err=%v; expected non-clean unresolved active intent", guard, found, err)
	}
}
