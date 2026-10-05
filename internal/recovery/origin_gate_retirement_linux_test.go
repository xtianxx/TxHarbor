//go:build linux && drill

// origin_gate_retirement_linux_test.go contains the P1/P2 retirement,
// cancellation and watch-census causal tests: sole-wait retention when the
// output copier blocks Wait after the child exited, unverified clean
// retirement when observer/control are lost, native cancellation before the
// readiness barrier release, deterministic inspection-seam refusals, and a
// genuine post-registration inherited-descriptor transfer refusal.
package recovery_test

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xtianxx/txharbor/internal/recovery"
)

// blockingWriter blocks every Write until released; it stalls the os/exec
// output copier so the sole cmd.Wait stays pending after the child exits.
type blockingWriter struct {
	release chan struct{}
}

func (w *blockingWriter) Write(data []byte) (int, error) {
	<-w.release
	return len(data), nil
}

func retirementFixtureSession(t *testing.T, ctx context.Context, gate *originGate, launcher *originGateLauncher, ref string) (*originGateClient, *originGateSession, *pgx.Conn) {
	t.Helper()
	client, cap, err := launcher.launch(t, ctx, gate,
		[]string{"write", gate.Endpoint(), gate.fx.role, gate.fx.table, ref, "none", originGateAppName}, gate.fx.password)
	if err != nil {
		t.Fatalf("spawn retirement fixture client: %v", err)
	}
	session, err := gate.Admit(ctx, cap)
	if err != nil {
		t.Fatalf("admit retirement fixture origin: %v", err)
	}
	if _, err := client.waitEvent(ctx, "first write completion", func(line string) bool {
		return strings.Contains(line, "state=COMMAND")
	}); err != nil {
		t.Fatalf("%v", err)
	}
	observation, err := pgx.Connect(ctx, gate.fx.dsn)
	if err != nil {
		t.Fatalf("connect retirement observation session: %v", err)
	}
	return client, session, observation
}

// TestDrillOriginGateRetirementWaitsForSoleWait retains the session while the
// launcher's sole wait is still blocked by the output copier even after the
// child exited and the backend is gone; successors stay refused, and the clean
// retirement happens only once the wait receipt completes.
func TestDrillOriginGateRetirementWaitsForSoleWait(t *testing.T) {
	detail := "blocking the output copier after child exit: active retained, no clean retirement and no successor until the launcher sole wait receipt completes"
	recordOriginGateResult(t, "TestDrillOriginGateRetirementWaitsForSoleWait", &detail)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	fx := newOriginGateFixture(t)
	ref := fmt.Sprintf("origin-gate-wait-retained-%d", time.Now().UnixNano())
	gate := newOriginGate(t, ctx, fx)
	launcher := newOriginGateLauncher(t)
	writer := &blockingWriter{release: make(chan struct{})}
	client, cap, err := launcher.launchWithStdout(t, ctx, gate,
		[]string{"write", gate.Endpoint(), fx.role, fx.table, ref, "none", originGateAppName}, fx.password, writer)
	if err != nil {
		t.Fatalf("spawn blocked-wait client: %v", err)
	}
	session, err := gate.Admit(ctx, cap)
	if err != nil {
		t.Fatalf("admit blocked-wait origin: %v", err)
	}
	registration, err := originGateWaitRegistration(ctx, session)
	if err != nil {
		t.Fatalf("%v", err)
	}
	observation, err := pgx.Connect(ctx, fx.dsn)
	if err != nil {
		t.Fatalf("connect blocked-wait observation session: %v", err)
	}
	defer observation.Close(context.Background())
	if err := originGateWaitFor(ctx, "first write visible", func() bool {
		var rows int
		if err := observation.QueryRow(ctx, `SELECT count(*) FROM `+pgx.Identifier{fx.table}.Sanitize()+` WHERE ref=$1`, ref).Scan(&rows); err != nil {
			return false
		}
		return rows == 1
	}); err != nil {
		t.Fatalf("%v", err)
	}
	if err := client.sendLine("quit"); err != nil {
		t.Fatalf("request client terminate: %v", err)
	}
	// The backend must drain naturally, but the launcher's Wait stays blocked
	// by the copier: retention without any clean-retirement verdict.
	if err := originGateWaitFor(ctx, "backend row gone while sole wait is pending", func() bool {
		var rows int
		if err := fx.admin.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE pid=$1`, registration.PID).Scan(&rows); err != nil {
			return false
		}
		return rows == 0
	}); err != nil {
		t.Fatalf("%v", err)
	}
	time.Sleep(700 * time.Millisecond)
	if gate.CurrentSession() != session {
		t.Fatalf("session was retired although the sole wait receipt is still pending")
	}
	if clean, report := gate.CleanRetired(); clean {
		t.Fatalf("clean retirement was granted while the sole wait is pending: %s", report)
	}
	if latched, reason := gate.Latched(); latched {
		t.Fatalf("blocked sole wait latched instead of retaining: %s", reason)
	}
	successor, successorCap, err := launcher.launch(t, ctx, gate,
		[]string{"write", gate.Endpoint(), fx.role, fx.table, ref + "-successor", "none", originGateAppName}, fx.password)
	if err != nil {
		t.Fatalf("spawn successor client: %v", err)
	}
	if _, err := gate.Admit(ctx, successorCap); err == nil {
		t.Fatalf("successor admission succeeded while the sole wait receipt is pending")
	}
	_ = successor.killAndWait(ctx)
	close(writer.release)
	if err := client.waitExit(ctx); err != nil {
		t.Fatalf("released sole wait did not complete cleanly: %v", err)
	}
	if err := originGateWaitFor(ctx, "clean retirement after wait release", func() bool {
		clean, _ := gate.CleanRetired()
		return clean
	}); err != nil {
		t.Fatalf("%v", err)
	}
	if err := originGateWaitSessionEnded(ctx, gate, session); err != nil {
		t.Fatalf("%v", err)
	}
	if latched, reason := gate.Latched(); latched {
		t.Fatalf("gate latched during the released clean retirement: %s", reason)
	}
	detail = fmt.Sprintf("child exited and backend PID %d drained while the copier kept the sole wait pending; session retained (%s), no clean verdict, successor refused; after release the receipt completed cleanly and only then the canonical clean retirement ran", registration.PID, func() string { _, report := gate.CleanRetired(); return report }())
}

// TestDrillOriginGateRetirementUnverifiedCleanOnLoss proves frontend EOF plus
// observer failure plus an unavailable drain control never yields a verified
// clean retirement.
func TestDrillOriginGateRetirementUnverifiedCleanOnLoss(t *testing.T) {
	detail := "frontend EOF with observer and independent control lost must not produce a verified clean retirement"
	recordOriginGateResult(t, "TestDrillOriginGateRetirementUnverifiedCleanOnLoss", &detail)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	fx := newOriginGateFixture(t)
	ref := fmt.Sprintf("origin-gate-unverified-clean-%d", time.Now().UnixNano())
	gate := newOriginGate(t, ctx, fx)
	gate.drainTimeout = 4 * time.Second
	launcher := newOriginGateLauncher(t)
	client, session, observation := retirementFixtureSession(t, ctx, gate, launcher, ref)
	defer observation.Close(context.Background())
	var terminated bool
	for _, pid := range []int{gate.observerPID, gate.controlPID} {
		if err := fx.admin.QueryRow(ctx, `SELECT pg_terminate_backend($1)`, pid).Scan(&terminated); err != nil || !terminated {
			t.Fatalf("terminate protected session PID %d: terminated=%t err=%v", pid, terminated, err)
		}
	}
	if err := client.sendLine("quit"); err != nil {
		t.Fatalf("request client terminate: %v", err)
	}
	if err := client.waitExit(ctx); err != nil {
		t.Fatalf("client did not exit: %v", err)
	}
	if err := originGateWaitFor(ctx, "clean-retirement verification failure latch", func() bool {
		latched, _ := gate.Latched()
		return latched
	}); err != nil {
		t.Fatalf("%v", err)
	}
	if clean, report := gate.CleanRetired(); clean {
		t.Fatalf("clean retirement was granted without a verified drain: %s", report)
	}
	if state := gate.DrainState(); state.Verified {
		t.Fatalf("drain was verified although observer and control were lost: %+v", state)
	}
	if gate.CurrentSession() != session {
		t.Fatalf("session was retired without a verified drain")
	}
	successor, successorCap, err := launcher.launch(t, ctx, gate,
		[]string{"write", gate.Endpoint(), fx.role, fx.table, ref + "-successor", "none", originGateAppName}, fx.password)
	if err != nil {
		t.Fatalf("spawn successor client: %v", err)
	}
	if _, err := gate.Admit(ctx, successorCap); err == nil {
		t.Fatalf("successor admission succeeded after an unverified clean path")
	}
	_ = successor.killAndWait(ctx)
	var rows int
	if err := observation.QueryRow(ctx, `SELECT count(*) FROM `+pgx.Identifier{fx.table}.Sanitize()+` WHERE ref=$1`, ref).Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("completed write changed during the unverified path: rows=%d err=%v", rows, err)
	}
	detail = fmt.Sprintf("frontend EOF with observer and control lost: latch %s, VerifiedClean=false, session retained, successor refused", reasonOf(gate))
}

// TestDrillOriginGateRetirementNativeCancelBeforeBarrierRelease cancels the
// admitted session context while the authenticated native first executable
// frame is still held by the unreleased registration barrier: bounded latch
// and drain, no SQL, no successor.
func TestDrillOriginGateRetirementNativeCancelBeforeBarrierRelease(t *testing.T) {
	detail := "canceling the armed native session before the readiness barrier release must latch and drain without any SQL"
	recordOriginGateResult(t, "TestDrillOriginGateRetirementNativeCancelBeforeBarrierRelease", &detail)
	testCtx, cancelTest := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancelTest()
	fx := newOriginGateFixture(t)
	tools, err := recovery.DrillProvisionNativePGTools(t)
	if err != nil {
		t.Fatalf("provision sealed native tools: %v", err)
	}
	nano := time.Now().UnixNano()
	sourceDB := fmt.Sprintf("origin_retire_cancel_src_%d", nano)
	targetDB := fmt.Sprintf("origin_retire_cancel_tgt_%d", nano)
	table := "origin_retire_cancel_rows"
	if err := fx.createOwnedDatabase(testCtx, sourceDB); err != nil {
		t.Fatalf("create owned source database: %v", err)
	}
	if err := fx.createOwnedDatabase(testCtx, targetDB); err != nil {
		t.Fatalf("create owned target database: %v", err)
	}
	archivePath := supervisorArchiveFromTools(t, testCtx, fx, tools, sourceDB, table, 3, false)
	gateCtx, cancelGate := context.WithCancel(testCtx)
	gate := newOriginGate(t, testCtx, fx)
	gate.HoldRegistration()
	handle := recovery.DrillNewTargetProcessObservation()
	if err := gate.ArmObserved(handle, tools, recovery.DrillPGRestoreArmOptions{
		TargetDSN:   armedGateDSN(gate, targetDB, fx.role, fx.password),
		OperationID: fmt.Sprintf("origin-retire-cancel-%d", nano),
	}); err != nil {
		t.Fatalf("arm cancel-before-release restore: %v", err)
	}
	runDone := make(chan struct{})
	var result recovery.PGCommandResult
	var runErr error
	go func() {
		defer close(runDone)
		result, runErr = recovery.DrillRunObservedPGRestore(gateCtx, recovery.TargetProcessRunner{DrainTimeout: 15 * time.Second}, handle, supervisorOpenArchive(t, archivePath), nil, nil)
	}()
	if err := handle.AwaitBound(testCtx); err != nil {
		t.Fatalf("cancel-before-release operation was never bound: %v", err)
	}
	session, err := gate.AdmitObserved(gateCtx, handle)
	if err != nil {
		t.Fatalf("admit cancel-before-release origin: %v", err)
	}
	if err := originGateWaitFor(testCtx, "held first executable frame", func() bool {
		return session.BufferedFrames() >= 1
	}); err != nil {
		t.Fatalf("%v", err)
	}
	if !session.ServerAuthOK() {
		t.Fatalf("gate never observed the genuine server AuthenticationOk before cancellation")
	}
	if _, registered := session.Registration(); registered {
		t.Fatalf("registration completed although the readiness barrier was never released")
	}
	cancelGate()
	if err := originGateWaitFor(testCtx, "cancellation latch before barrier release", func() bool {
		latched, _ := gate.Latched()
		return latched
	}); err != nil {
		t.Fatalf("%v", err)
	}
	if _, reason := gate.Latched(); !strings.Contains(reason, "context canceled") {
		t.Fatalf("cancellation did not latch as such: %s", reason)
	}
	if err := originGateWaitFor(testCtx, "cancel-before-release drain", func() bool {
		return gate.DrainState().Verified
	}); err != nil {
		t.Fatalf("%v (state %+v)", err, gate.DrainState())
	}
	select {
	case <-runDone:
	case <-testCtx.Done():
		t.Fatalf("cancelled native restore did not return: %v", testCtx.Err())
	}
	if result.Outcome == recovery.PGCommandSucceeded {
		t.Fatalf("cancelled native restore reported success: %+v err=%v", result, runErr)
	}
	if clean, _ := gate.CleanRetired(); clean {
		t.Fatalf("cancel before barrier release produced a clean retirement")
	}
	targetConn, err := pgx.Connect(testCtx, fx.dsnAs(targetDB, "txharbor", "txharbor"))
	if err != nil {
		t.Fatalf("connect cancel-before-release target: %v", err)
	}
	defer targetConn.Close(context.Background())
	if exists, _ := supervisorTableState(t, testCtx, targetConn, table); exists {
		t.Fatalf("cancel before barrier release executed SQL in the target")
	}
	detail = fmt.Sprintf("authenticated native first frame was held (buffered=%d, no registration) when the session context was cancelled; bounded latch (%s) and verified drain occurred with no SQL and no successor", session.BufferedFrames(), reasonOf(gate))
}

// TestDrillOriginGateRetirementInspectionSeamRefuses uses the deterministic
// test-only inspection seam to prove a changed captured start identity and an
// incomplete ownership census both refuse (latch), never pass.
func TestDrillOriginGateRetirementInspectionSeamRefuses(t *testing.T) {
	detail := "deterministic inspection seam: changed captured origin start and incomplete FD-owner census must refuse"
	recordOriginGateResult(t, "TestDrillOriginGateRetirementInspectionSeamRefuses", &detail)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	fx := newOriginGateFixture(t)
	launcher := newOriginGateLauncher(t)

	changedStartGate := newOriginGate(t, ctx, fx)
	var changedStartArmed atomic.Bool
	changedStartGate.seam = &originGateInspectionSeam{
		processStart: func(pid int) (uint64, error) {
			if changedStartArmed.Load() {
				return 123456789, nil
			}
			return hostProcessStartID(pid)
		},
	}
	changedRef := fmt.Sprintf("origin-gate-seam-start-%d", time.Now().UnixNano())
	changedClient, changedSession, changedObservation := retirementFixtureSession(t, ctx, changedStartGate, launcher, changedRef)
	defer changedObservation.Close(context.Background())
	changedStartArmed.Store(true)
	if err := originGateWaitFor(ctx, "changed captured start latch", func() bool {
		latched, _ := changedStartGate.Latched()
		return latched
	}); err != nil {
		t.Fatalf("%v", err)
	}
	if _, reason := changedStartGate.Latched(); !strings.Contains(reason, "start identity mismatch") {
		t.Fatalf("changed captured start was not refused as an identity mismatch: %s", reason)
	}
	if clean, _ := changedStartGate.CleanRetired(); clean {
		t.Fatalf("changed captured start produced a clean retirement")
	}
	_ = changedClient

	censusGate := newOriginGate(t, ctx, fx)
	var censusArmed atomic.Bool
	censusGate.seam = &originGateInspectionSeam{
		socketOwners: func(inode string) ([]int, error) {
			if censusArmed.Load() {
				return nil, fmt.Errorf("census incomplete: deterministic seam")
			}
			return hostSocketInodeOwners(inode)
		},
	}
	censusRef := fmt.Sprintf("origin-gate-seam-census-%d", time.Now().UnixNano())
	censusClient, censusSession, censusObservation := retirementFixtureSession(t, ctx, censusGate, launcher, censusRef)
	defer censusObservation.Close(context.Background())
	censusArmed.Store(true)
	if err := originGateWaitFor(ctx, "incomplete census latch", func() bool {
		latched, _ := censusGate.Latched()
		return latched
	}); err != nil {
		t.Fatalf("%v", err)
	}
	if _, reason := censusGate.Latched(); !strings.Contains(reason, "owner census failed") {
		t.Fatalf("incomplete ownership census was not refused: %s", reason)
	}
	if clean, _ := censusGate.CleanRetired(); clean {
		t.Fatalf("incomplete ownership census produced a clean retirement")
	}
	_ = censusClient
	_ = changedSession
	_ = censusSession
	detail = fmt.Sprintf("seam-changed captured start latched (%s) and seam-incomplete FD-owner census latched (%s); both sessions refused and never cleanly retired", reasonOf(changedStartGate), reasonOf(censusGate))
}

// TestDrillOriginGateRetirementPostRegistrationInheritanceRefused proves a
// genuine descriptor inheritance AFTER registration is refused by the owner
// census and drained, not merely checked at admission.
func TestDrillOriginGateRetirementPostRegistrationInheritanceRefused(t *testing.T) {
	detail := "genuine post-registration socket inheritance transfer refused by the FD-owner census with latch and drain"
	recordOriginGateResult(t, "TestDrillOriginGateRetirementPostRegistrationInheritanceRefused", &detail)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	fx := newOriginGateFixture(t)
	table := fmt.Sprintf("origin_gate_retire_seq_%d", time.Now().UnixNano())
	if _, err := fx.admin.Exec(ctx, `CREATE TABLE `+pgx.Identifier{table}.Sanitize()+` (seq bigserial PRIMARY KEY, ref text NOT NULL UNIQUE)`); err != nil {
		t.Fatalf("create retirement sequence table: %v", err)
	}
	if _, err := fx.admin.Exec(ctx, `GRANT INSERT, SELECT ON `+pgx.Identifier{table}.Sanitize()+` TO `+pgx.Identifier{fx.role}.Sanitize()); err != nil {
		t.Fatalf("grant retirement table access: %v", err)
	}
	if _, err := fx.admin.Exec(ctx, `GRANT USAGE, SELECT ON SEQUENCE `+pgx.Identifier{table + "_seq_seq"}.Sanitize()+` TO `+pgx.Identifier{fx.role}.Sanitize()); err != nil {
		t.Fatalf("grant retirement sequence usage: %v", err)
	}
	gate := newOriginGate(t, ctx, fx)
	launcher := lifecycleLauncher(t)
	ref := fmt.Sprintf("retire-inherit-%d", time.Now().UnixNano())
	client, cap, err := launcher.launch(t, ctx, gate,
		[]string{"sequence", gate.Endpoint(), fx.role, table, originGateAppName}, fx.password)
	if err != nil {
		t.Fatalf("spawn lifecycle inheritance client: %v", err)
	}
	session, err := gate.Admit(ctx, cap)
	if err != nil {
		t.Fatalf("admit lifecycle inheritance origin: %v", err)
	}
	if _, err := client.waitEvent(ctx, "lifecycle authentication", func(line string) bool {
		return strings.Contains(line, "ORIGIN_LIFECYCLE_AUTH")
	}); err != nil {
		t.Fatalf("%v", err)
	}
	if _, err := originGateWaitRegistration(ctx, session); err != nil {
		t.Fatalf("%v", err)
	}
	if err := client.sendLine(ref); err != nil {
		t.Fatalf("send pre-transfer ref: %v", err)
	}
	if _, err := client.waitEvent(ctx, "pre-transfer command completion", func(line string) bool {
		return strings.Contains(line, "ORIGIN_LIFECYCLE_COMMAND")
	}); err != nil {
		t.Fatalf("%v", err)
	}
	if err := client.sendLine("forkfd"); err != nil {
		t.Fatalf("request post-registration descriptor transfer: %v", err)
	}
	forkLine, err := client.waitEvent(ctx, "post-registration fork record", func(line string) bool {
		return strings.Contains(line, "ORIGIN_LIFECYCLE_FORK") && strings.Contains(line, "post-registration")
	})
	if err != nil {
		t.Fatalf("%v", err)
	}
	childPID, _ := strconv.Atoi(originGateEventFields(forkLine)["child"])
	if childPID > 1 {
		t.Cleanup(func() {
			if process, err := os.FindProcess(childPID); err == nil {
				_ = process.Kill()
			}
		})
	}
	if err := originGateWaitFor(ctx, "post-registration inheritance latch", func() bool {
		latched, _ := gate.Latched()
		return latched
	}); err != nil {
		t.Fatalf("%v", err)
	}
	if _, reason := gate.Latched(); !strings.Contains(reason, "no longer owned solely") {
		t.Fatalf("post-registration inheritance was not refused by the owner census: %s", reason)
	}
	if err := originGateWaitFor(ctx, "post-registration inheritance drain", func() bool {
		return gate.DrainState().Verified
	}); err != nil {
		t.Fatalf("%v (state %+v)", err, gate.DrainState())
	}
	if clean, _ := gate.CleanRetired(); clean {
		t.Fatalf("post-registration inheritance produced a clean retirement")
	}
	if latched, _ := gate.Latched(); !latched {
		t.Fatalf("gate did not remain latched after the transfer refusal")
	}
	observation, err := pgx.Connect(ctx, fx.dsn)
	if err != nil {
		t.Fatalf("connect inheritance observation session: %v", err)
	}
	defer observation.Close(context.Background())
	var rows int
	if err := observation.QueryRow(ctx, `SELECT count(*) FROM `+pgx.Identifier{table}.Sanitize()+` WHERE ref=$1`, ref).Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("pre-transfer completed write changed during the refusal: rows=%d err=%v", rows, err)
	}
	detail = fmt.Sprintf("child PID %d inherited the live socket after registration; owner census latched (%s) and the exact backend was drained (%s); completed write retained, no clean retirement", childPID, reasonOf(gate), gate.DrainState().Report)
}
