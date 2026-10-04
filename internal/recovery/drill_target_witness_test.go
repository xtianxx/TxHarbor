//go:build drill

package recovery_test

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xtianxx/txharbor/internal/recovery"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

// TestDrillWitnessOldWriterHelper is a real fixture-owned client process. The
// parent retains its Cmd handle, kills it, and waits/reaps it; the server PID
// is never used as process identity or termination authority.
func TestDrillWitnessOldWriterHelper(t *testing.T) {
	dsn := os.Getenv("TXHARBOR_DRILL_WITNESS_DSN")
	if dsn == "" {
		return
	}
	conn, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	if _, err := conn.Exec(context.Background(), `INSERT INTO public.drill_witness_writes(note) VALUES ('old-writer')`); err != nil {
		t.Fatal(err)
	}
	fmt.Fprintln(os.Stdout, "READY")
	for {
		if _, err := conn.Exec(context.Background(), `INSERT INTO public.drill_witness_writes(note) VALUES ('old-writer')`); err != nil {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

type drillWitnessState uint8

const (
	witnessPrepared drillWitnessState = iota
	witnessObserving
	witnessAccepted
	witnessLost
)

// targetRebuildWitness is deliberately test-local (there is no production
// callback or generic proving API). It is single use and irrevocably latches
// any loss of observation. The immutable binding includes the real target key,
// role fingerprint, prior writer attempt and successor operation.
type targetRebuildWitness struct {
	mu                                                   sync.Mutex
	state                                                drillWitnessState
	targetKey, roleFingerprint, oldAttempt, newOperation string
	lostReason                                           string
}

func newTargetRebuildWitness(key, role, oldAttempt, newOperation string) *targetRebuildWitness {
	return &targetRebuildWitness{state: witnessPrepared, targetKey: key, roleFingerprint: role, oldAttempt: oldAttempt, newOperation: newOperation}
}

func envTargetRoleFingerprint(t *testing.T, dsn string) string {
	t.Helper()
	target, err := controlstore.ParseDSNTarget(dsn)
	if err != nil {
		t.Fatalf("parse target role fingerprint: %v", err)
	}
	return target.DataTargetFingerprint().RoleFingerprint
}

func (w *targetRebuildWitness) start() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.state != witnessPrepared {
		return errors.New("witness is not prepared")
	}
	w.state = witnessObserving
	return nil
}

func (w *targetRebuildWitness) lose(reason string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.state != witnessLost {
		w.state, w.lostReason = witnessLost, reason
	}
}

func (w *targetRebuildWitness) accepts(key, role, oldAttempt, newOperation string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.state != witnessObserving {
		return fmt.Errorf("witness is not observing (state=%d reason=%s)", w.state, w.lostReason)
	}
	if key != w.targetKey || role != w.roleFingerprint || oldAttempt != w.oldAttempt || newOperation != w.newOperation {
		w.state, w.lostReason = witnessLost, "identity mismatch"
		return errors.New("witness identity mismatch; witness permanently lost")
	}
	return nil
}

func TestDrillTargetWitnessOldReconnectRejected(t *testing.T) {
	env := newDrillEnv(t, false)
	target := env.recoveryTarget()
	database := env.dbNameOf(target)
	role := "drill_old_writer"
	roleSQL := pgx.Identifier{role}.Sanitize()
	if _, err := env.admin.Exec(env.ctx, `CREATE ROLE `+roleSQL+` LOGIN PASSWORD 'old-writer-test-secret'`); err != nil {
		t.Fatal(err)
	}
	writerURL, err := url.Parse(target)
	if err != nil {
		t.Fatal(err)
	}
	writerURL.User = url.UserPassword(role, "old-writer-test-secret")
	writerDSN := writerURL.String()
	conn, err := pgx.Connect(env.ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(env.ctx, `CREATE TABLE public.drill_witness_writes(id bigserial PRIMARY KEY, note text NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(env.ctx, `GRANT INSERT ON public.drill_witness_writes TO `+roleSQL); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(env.ctx, `GRANT USAGE, SELECT ON SEQUENCE public.drill_witness_writes_id_seq TO `+roleSQL); err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(env.ctx); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestDrillWitnessOldWriterHelper$")
	cmd.Env = append(os.Environ(), "TXHARBOR_DRILL_WITNESS_DSN="+writerDSN)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	ready := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			if strings.TrimSpace(scanner.Text()) == "READY" {
				ready <- nil
				return
			}
		}
		if err := scanner.Err(); err != nil {
			ready <- err
		} else {
			ready <- errors.New("writer helper exited before READY")
		}
	}()
	select {
	case err := <-ready:
		if err != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatal("old writer helper did not connect and commit its first write")
	}
	var initialWrites int
	writerObserver, err := pgx.Connect(env.ctx, target)
	if err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatalf("connect to observe the old writer's committed row: %v", err)
	}
	if err := writerObserver.QueryRow(env.ctx, `SELECT count(*) FROM public.drill_witness_writes WHERE note='old-writer'`).Scan(&initialWrites); err != nil {
		_ = writerObserver.Close(context.Background())
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatalf("observe the owned old writer's first committed write: %v", err)
	}
	if err := writerObserver.Close(env.ctx); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatalf("close old-writer row observer: %v", err)
	}
	if initialWrites == 0 {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatal("old writer announced READY without a committed write")
	}
	// The retained Cmd is the process identity and termination authority.
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err == nil {
		t.Fatal("killed old writer exited successfully; expected termination")
	}
	if _, err := env.admin.Exec(env.ctx, `ALTER ROLE `+roleSQL+` NOLOGIN`); err != nil {
		t.Fatal(err)
	}
	var canLogin, isSuper, canCreateDB, canCreateRole bool
	if err := env.admin.QueryRow(env.ctx, `SELECT rolcanlogin, rolsuper, rolcreatedb, rolcreaterole FROM pg_roles WHERE rolname=$1`, role).Scan(&canLogin, &isSuper, &canCreateDB, &canCreateRole); err != nil {
		t.Fatal(err)
	}
	if canLogin || isSuper || canCreateDB || canCreateRole {
		t.Fatalf("owned old writer role is not a restricted NOLOGIN role: login=%t super=%t createdb=%t createrole=%t", canLogin, isSuper, canCreateDB, canCreateRole)
	}
	if _, err := pgx.Connect(env.ctx, writerDSN); err == nil {
		t.Fatal("old role reconnected after NOLOGIN")
	}
	var sessions int
	if err := env.admin.QueryRow(env.ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname=$1 AND usename=$2`, database, role).Scan(&sessions); err != nil {
		t.Fatal(err)
	}
	if sessions != 0 {
		t.Fatalf("old role still has %d server sessions after kill, Wait, and NOLOGIN", sessions)
	}
}

// drillWaitNoTargetSessions performs one authoritative inventory read after
// the owned child has returned and been Waited; it does not poll to manufacture
// a drained-process claim.
func drillWaitNoTargetSessions(ctx context.Context, admin *pgxpool.Pool, database string) error {
	var n int
	if err := admin.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname=$1 AND pid<>pg_backend_pid()`, database).Scan(&n); err != nil {
		return err
	}
	if n != 0 {
		return fmt.Errorf("target %s still has %d sessions after owned process Wait", database, n)
	}
	return nil
}

// rebuildTargetWithWitness intentionally refuses this recovery path. The
// interrupted restore's original target credentials/process handle are not
// available here as immutable witness evidence; manufacturing a new NOLOGIN
// role after the attempt would prove an unrelated identity. Keep this explicit
// failure (rather than SKIP or a false clean transition) until the original
// attempt can supply authentic local proof.
func (e *drillEnv) rebuildTargetWithWitness(_ string, newOperation string) {
	e.t.Helper()
	e.t.Fatalf("local proof NOT ESTABLISHED: original interrupted attempt %q has no retained authenticated writer identity/process handle bound to its credentials; refusing DROP/CREATE and clean guard acceptance", newOperation)
}

func TestDrillTargetWitnessLossAndObserverInterruptionFailClosed(t *testing.T) {
	w := newTargetRebuildWitness("sha256:target", "sha256:role", "old-attempt", "new-op")
	if err := w.start(); err != nil {
		t.Fatal(err)
	}
	w.lose("observer query timed out")
	if err := w.accepts("sha256:target", "sha256:role", "old-attempt", "new-op"); err == nil {
		t.Fatal("lost witness accepted after observation interruption")
	}
	w2 := newTargetRebuildWitness("sha256:target", "sha256:role", "old-attempt", "new-op")
	if err := w2.start(); err != nil {
		t.Fatal(err)
	}
	if err := w2.accepts("sha256:wrong", "sha256:role", "old-attempt", "new-op"); err == nil {
		t.Fatal("wrong-target witness accepted")
	}
	if err := w2.accepts("sha256:target", "sha256:role", "old-attempt", "new-op"); err == nil {
		t.Fatal("mismatched witness was reusable")
	}
}

func TestDrillTargetWitnessRecoveryRequiresExactFreshIdentity(t *testing.T) {
	w := newTargetRebuildWitness("sha256:target", "sha256:role", "old-attempt", "new-op")
	if err := w.accepts("sha256:target", "sha256:role", "old-attempt", "new-op"); err == nil {
		t.Fatal("unprepared witness accepted")
	}
	if err := w.start(); err != nil {
		t.Fatal(err)
	}
	if err := w.accepts("sha256:target", "sha256:role", "wrong-attempt", "new-op"); err == nil {
		t.Fatal("stale attempt accepted")
	}
	if err := w.accepts("sha256:target", "sha256:role", "old-attempt", "new-op"); err == nil {
		t.Fatal("witness reused after mismatch")
	}
}

// These tests exercise only cancellation at the real restore_evidence
// acceptance barrier. They do not establish an original-writer witness or
// justify target rebuild acceptance.
func TestDrillTargetWitnessOldRoleLoginDuringRestoreAcceptanceFailsClosed(t *testing.T) {
	drillRestoreAcceptanceProofLoss(t, false)
}

func TestDrillTargetWitnessObserverTerminationDuringRestoreAcceptanceFailsClosed(t *testing.T) {
	drillRestoreAcceptanceProofLoss(t, true)
}

// This is a real PostgreSQL acceptance barrier, not a witness-object test:
// EXCLUSIVE permits probe reads but blocks INSERT into recovery_evidence. The
// observer loss is latched while the real restore is waiting at that INSERT;
// canceling the restore must leave no restore_probe and no clean guard.
func drillRestoreAcceptanceProofLoss(t *testing.T, terminateObserver bool) {
	t.Helper()
	requireDrillLocalPGRestore(t)
	env := newDrillEnv(t, false)
	env.seedLiveBusinessState()
	backup := env.backup()
	verifyTarget := env.createDatabase("witness_loss_verify")
	env.verifyBackup(backup.ManifestPath, verifyTarget, env.operation("witness-loss-verify"))
	env.seedAuthoritativeTargetCleanBaseline()
	target := env.recoveryTarget()

	role := fmt.Sprintf("drill_old_role_%d", env.seq+1)
	roleSQL := pgx.Identifier{role}.Sanitize()
	if _, err := env.admin.Exec(env.ctx, "CREATE ROLE "+roleSQL+" LOGIN PASSWORD 'witness-old-role-secret'"); err != nil {
		t.Fatal(err)
	}
	if _, err := env.admin.Exec(env.ctx, "ALTER ROLE "+roleSQL+" NOLOGIN"); err != nil {
		t.Fatal(err)
	}
	targetInfo, err := controlstore.ParseDSNTarget(target)
	if err != nil {
		t.Fatal(err)
	}
	targetGuardKey, err := controlstore.TargetGuardKey(targetInfo)
	if err != nil {
		t.Fatal(err)
	}
	priorGuard, found, err := controlstore.ReadTargetGuard(env.ctx, env.ctrl, targetGuardKey)
	if err != nil || !found || priorGuard.State != controlstore.TargetGuardClean {
		t.Fatalf("read actual clean pre-restore guard: %+v found=%t err=%v", priorGuard, found, err)
	}

	blocker, err := pgx.Connect(env.ctx, env.ctrlDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close(context.Background())
	if _, err := blocker.Exec(env.ctx, `BEGIN`); err != nil {
		t.Fatal(err)
	}
	if _, err := blocker.Exec(env.ctx, `LOCK TABLE recovery_evidence IN EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}

	observerURL, err := url.Parse(env.adminDSN)
	if err != nil {
		t.Fatal(err)
	}
	observerURL.Query().Set("application_name", "drill-witness-observer")
	query := observerURL.Query()
	query.Set("application_name", "drill-witness-observer")
	observerURL.RawQuery = query.Encode()
	observer, err := pgx.Connect(env.ctx, observerURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer observer.Close(context.Background())
	var observerPID int32
	var visible bool
	if err := observer.QueryRow(env.ctx, `SELECT pg_backend_pid(), rolsuper OR pg_has_role(current_user, 'pg_read_all_stats', 'USAGE') FROM pg_roles WHERE rolname=current_user`).Scan(&observerPID, &visible); err != nil {
		t.Fatal(err)
	}
	if !visible {
		t.Fatal("dedicated witness observer lacks cluster-wide session visibility")
	}
	operation := env.operation("witness-loss-restore")
	witness := newTargetRebuildWitness(targetGuardKey, targetInfo.DataTargetFingerprint().RoleFingerprint, priorGuard.OperationID, operation)
	if err := witness.start(); err != nil {
		t.Fatal(err)
	}
	restoreCtx, cancelRestore := context.WithCancel(context.Background())
	defer cancelRestore()
	monitorDone := make(chan error, 1)
	monitorCtx, stopMonitor := context.WithCancel(context.Background())
	defer stopMonitor()
	go func() {
		err := drillMonitorAcceptanceBoundary(monitorCtx, observer, role, witness)
		if err != nil {
			witness.lose(err.Error())
			cancelRestore()
		}
		monitorDone <- err
	}()
	type restoreOutcome struct {
		result recovery.RestoreResult
		err    error
	}
	restoreDone := make(chan restoreOutcome, 1)
	go func() {
		result, err := recovery.ExecuteRestore(restoreCtx, recovery.RestoreOptions{
			ManifestPath: backup.ManifestPath, InstanceID: env.instanceID,
			ControlStore: env.store, ControlDSN: env.ctrlDSN,
			ObserverDSN: env.observerDSNFor(target), TargetDSN: target,
			TargetDeclaration: recovery.TargetIsolated, TargetReason: "drill: isolated recovery environment",
			Actor: "deploy:executor", ProgramVersion: drillProgramVersion,
			OperationID: operation, PG: env.pg,
		})
		restoreDone <- restoreOutcome{result: result, err: err}
	}()
	if err := drillWaitRecoveryEvidenceInsert(env.ctx, env.admin, env.dbNameOf(env.ctrlDSN), 45*time.Second); err != nil {
		var active string
		_ = env.admin.QueryRow(env.ctx, `SELECT COALESCE(string_agg(pid||':'||usename||'/'||application_name||'/'||wait_event_type||'/'||wait_event||'/'||left(query,160), E'\n'), '') FROM pg_stat_activity WHERE datname=$1`, env.dbNameOf(env.ctrlDSN)).Scan(&active)
		select {
		case out := <-restoreDone:
			err = fmt.Errorf("%w; restore result=%+v restore_err=%v; control activity=%s", err, out.result, out.err, active)
		default:
			err = fmt.Errorf("%w; restore still running; control activity=%s", err, active)
		}
		_, _ = blocker.Exec(context.Background(), `ROLLBACK`)
		t.Fatal(err)
	}
	var started int
	if err := env.ctrl.QueryRow(env.ctx, `SELECT count(*) FROM recovery_audit WHERE instance_id=$1 AND action=$2 AND result='ok'`, env.instanceID, recovery.ActionRestoreStarted).Scan(&started); err != nil || started == 0 {
		_, _ = blocker.Exec(context.Background(), `ROLLBACK`)
		t.Fatalf("restore start marker at evidence INSERT barrier = %d err=%v", started, err)
	}
	activeGuard, guardFound, guardErr := controlstore.ReadTargetGuard(env.ctx, env.ctrl, targetGuardKey)
	if guardErr != nil || !guardFound || !activeGuard.LaunchIntent || activeGuard.AttemptAppName == "" {
		_, _ = blocker.Exec(context.Background(), `ROLLBACK`)
		t.Fatalf("target writer marker at blocked restore acceptance: %+v found=%t err=%v", activeGuard, guardFound, guardErr)
	}
	if terminateObserver {
		var terminated bool
		if err := env.admin.QueryRow(env.ctx, `SELECT pg_terminate_backend($1)`, observerPID).Scan(&terminated); err != nil || !terminated {
			_, _ = blocker.Exec(context.Background(), `ROLLBACK`)
			t.Fatalf("terminate dedicated observer pid %d: terminated=%t err=%v", observerPID, terminated, err)
		}
	} else if _, err := env.admin.Exec(env.ctx, "ALTER ROLE "+roleSQL+" LOGIN"); err != nil {
		_, _ = blocker.Exec(context.Background(), `ROLLBACK`)
		t.Fatal(err)
	}
	select {
	case err := <-monitorDone:
		if err == nil {
			_, _ = blocker.Exec(context.Background(), `ROLLBACK`)
			t.Fatal("observer reported success after proof invalidation")
		}
		witness.lose(err.Error())
	case <-time.After(5 * time.Second):
		_, _ = blocker.Exec(context.Background(), `ROLLBACK`)
		t.Fatal("observer did not latch role/connection loss at the acceptance barrier")
	}
	if _, err := blocker.Exec(context.Background(), `ROLLBACK`); err != nil {
		t.Fatalf("release evidence insertion barrier: %v", err)
	}
	select {
	case out := <-restoreDone:
		if out.err == nil || out.result.Restored {
			t.Fatalf("restore after witness loss = %+v err=%v, want refused", out.result, out.err)
		}
	case <-time.After(2 * time.Minute):
		t.Fatal("restore did not return after observer loss cancellation")
	}
	if got := env.controlEvidenceCount("restore_probe", backup.Manifest.BackupID); got != 0 {
		t.Fatalf("restore_probe accepted after observer loss = %d, want 0", got)
	}
	info, err := controlstore.ParseDSNTarget(target)
	if err != nil {
		t.Fatal(err)
	}
	key, err := controlstore.TargetGuardKey(info)
	if err != nil {
		t.Fatal(err)
	}
	guard, found, err := controlstore.ReadTargetGuard(env.ctx, env.ctrl, key)
	if err != nil || !found || guard.State == controlstore.TargetGuardClean {
		t.Fatalf("target guard after proof loss = %+v found=%t err=%v; must not infer clean", guard, found, err)
	}
}

func drillMonitorAcceptanceBoundary(ctx context.Context, observer *pgx.Conn, oldRole string, witness *targetRebuildWitness) error {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			var visible, oldCanLogin bool
			if err := observer.QueryRow(ctx, `SELECT rolsuper OR pg_has_role(current_user, 'pg_read_all_stats', 'USAGE') FROM pg_roles WHERE rolname=current_user`).Scan(&visible); err != nil {
				witness.lose("dedicated observer backend lost: " + err.Error())
				return err
			}
			if !visible {
				err := errors.New("dedicated observer lost pg_stat_activity visibility")
				witness.lose(err.Error())
				return err
			}
			if err := observer.QueryRow(ctx, `SELECT rolcanlogin FROM pg_roles WHERE rolname=$1`, oldRole).Scan(&oldCanLogin); err != nil {
				witness.lose("old-role boundary query failed: " + err.Error())
				return err
			}
			if oldCanLogin {
				err := errors.New("old writer role LOGIN was restored during restore acceptance")
				witness.lose(err.Error())
				return err
			}
		}
	}
}

func drillWaitRecoveryEvidenceInsert(ctx context.Context, admin *pgxpool.Pool, controlDB string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var waiting int
		err := admin.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname=$1 AND wait_event_type='Lock' AND query ILIKE '%INSERT INTO recovery_evidence%'`, controlDB).Scan(&waiting)
		if err != nil {
			return fmt.Errorf("observe acceptance INSERT wait: %w", err)
		}
		if waiting > 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
	return errors.New("real restore did not reach the blocked recovery_evidence INSERT within the barrier deadline")
}

// TestDrillTargetWitnessSameRoleCredentialRotationPreservesBinding is the
// first native-PG prototype for the ora11 decision. It deliberately uses one
// LOGIN role and rotates only its SCRAM password: neither the immutable target
// nor role fingerprint may change. The test is evidence about this pinned
// fixture only, not production credential-management authority.
func TestDrillTargetWitnessSameRoleCredentialRotationPreservesBinding(t *testing.T) {
	env := newDrillEnv(t, false)
	dsn := env.recoveryTarget()
	role := fmt.Sprintf("drill_rotate_%d", env.seq+1)
	roleSQL := pgx.Identifier{role}.Sanitize()
	if _, err := env.admin.Exec(env.ctx, "CREATE ROLE "+roleSQL+" LOGIN PASSWORD 'drill-p0-secret'"); err != nil {
		t.Fatal(err)
	}
	urlBefore, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	urlBefore.User = url.UserPassword(role, "drill-p0-secret")
	p0 := urlBefore.String()
	before, err := controlstore.ParseDSNTarget(p0)
	if err != nil {
		t.Fatal(err)
	}
	keyBefore, err := recovery.CanonicalTargetKey(before)
	if err != nil {
		t.Fatal(err)
	}
	if conn, err := pgx.Connect(env.ctx, p0); err != nil {
		t.Fatalf("P0 SCRAM credential did not authenticate: %v", err)
	} else if err := conn.Close(env.ctx); err != nil {
		t.Fatal(err)
	}
	wrongURL, _ := url.Parse(dsn)
	wrongURL.User = url.UserPassword(role, "wrong-drill-password")
	if conn, err := pgx.Connect(env.ctx, wrongURL.String()); err == nil {
		_ = conn.Close(env.ctx)
		t.Fatal("wrong SCRAM password authenticated")
	}
	var scram bool
	if err := env.admin.QueryRow(env.ctx, `SELECT rolpassword LIKE 'SCRAM-SHA-256$%' FROM pg_authid WHERE rolname=$1`, role).Scan(&scram); err != nil {
		t.Fatalf("read protected SCRAM verifier metadata: %v", err)
	}
	if !scram {
		t.Fatal("fixture role did not store a SCRAM verifier")
	}
	if _, err := env.admin.Exec(env.ctx, "ALTER ROLE "+roleSQL+" PASSWORD 'drill-p1-secret'"); err != nil {
		t.Fatal(err)
	}
	if conn, err := pgx.Connect(env.ctx, p0); err == nil {
		_ = conn.Close(env.ctx)
		t.Fatal("P0 authenticated after same-role credential rotation")
	}
	urlAfter, _ := url.Parse(dsn)
	urlAfter.User = url.UserPassword(role, "drill-p1-secret")
	conn, err := pgx.Connect(env.ctx, urlAfter.String())
	if err != nil {
		t.Fatalf("P1 SCRAM credential did not authenticate: %v", err)
	}
	_ = conn.Close(env.ctx)
	after, err := controlstore.ParseDSNTarget(urlAfter.String())
	if err != nil {
		t.Fatal(err)
	}
	keyAfter, err := recovery.CanonicalTargetKey(after)
	if err != nil || keyAfter != keyBefore || after.DataTargetFingerprint().RoleFingerprint != before.DataTargetFingerprint().RoleFingerprint {
		t.Fatalf("same-role password rotation changed immutable binding: before=%s after=%s err=%v", keyBefore, keyAfter, err)
	}
}

// TestDrillTargetWitnessCredentialFenceBlocksPasswordAndMembershipChanges
// is the PG18 catalog-lock prototype. SHARE locks on both auth catalogs must
// hold a real ALTER ROLE password/self-password and membership edit until the
// protected observer transaction ends.
func TestDrillTargetWitnessCredentialFenceBlocksPasswordAndMembershipChanges(t *testing.T) {
	env := newDrillEnv(t, false)
	role := fmt.Sprintf("drill_fence_%d", env.seq+1)
	member := role + "_member"
	roleSQL, memberSQL := pgx.Identifier{role}.Sanitize(), pgx.Identifier{member}.Sanitize()
	if _, err := env.admin.Exec(env.ctx, "CREATE ROLE "+roleSQL+" LOGIN PASSWORD 'fence-p0-secret'"); err != nil {
		t.Fatal(err)
	}
	if _, err := env.admin.Exec(env.ctx, "CREATE ROLE "+memberSQL); err != nil {
		t.Fatal(err)
	}
	observer, err := pgx.Connect(env.ctx, env.adminDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer observer.Close(context.Background())
	if _, err := observer.Exec(env.ctx, `BEGIN`); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = observer.Exec(context.Background(), `ROLLBACK`) }()
	if _, err := observer.Exec(env.ctx, `LOCK TABLE pg_catalog.pg_authid, pg_catalog.pg_auth_members IN SHARE MODE`); err != nil {
		t.Fatalf("PG18 credential catalog fence unavailable: %v", err)
	}
	blockedDDL := func(dsn, statement, label string) {
		t.Helper()
		mutator, err := pgx.Connect(env.ctx, dsn)
		if err != nil {
			t.Fatal(err)
		}
		blockedCtx, cancel := context.WithTimeout(env.ctx, 300*time.Millisecond)
		defer cancel()
		_, execErr := mutator.Exec(blockedCtx, statement)
		_ = mutator.Close(context.Background())
		if execErr == nil || !errors.Is(execErr, context.DeadlineExceeded) {
			t.Fatalf("%s was not blocked until deadline: %v", label, execErr)
		}
	}
	selfURL, err := url.Parse(env.adminDSN)
	if err != nil {
		t.Fatal(err)
	}
	selfURL.User = url.UserPassword(role, "fence-p0-secret")
	blockedDDL(selfURL.String(), "ALTER ROLE "+roleSQL+" PASSWORD 'fence-p1-secret'", "self-password ALTER ROLE")
	blockedDDL(env.adminDSN, "GRANT "+roleSQL+" TO "+memberSQL, "membership change")
	if _, err := observer.Exec(env.ctx, `COMMIT`); err != nil {
		t.Fatal(err)
	}
	mutator, err := pgx.Connect(env.ctx, selfURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer mutator.Close(context.Background())
	if _, err := mutator.Exec(env.ctx, "ALTER ROLE "+roleSQL+" PASSWORD 'fence-p1-secret'"); err != nil {
		t.Fatalf("password rotation remained blocked after fence release: %v", err)
	}
}

// The target advisory lock is owned by the exact session whose transaction is
// supplied here; the guard snapshot, rebuild audit and clean transition share
// that same lock-owner transaction.
