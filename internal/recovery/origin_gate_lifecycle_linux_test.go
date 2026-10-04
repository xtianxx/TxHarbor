//go:build linux && drill

// origin_gate_lifecycle_linux_test.go contains the OG02/OG03/OG04 hardening
// tests for the continuous-origin gate: authoritative exact-identity backend
// termination and drain, in-flight write cancellation without later commit,
// drain-unknown retention, observer stall/loss refusal, context-cancel drain,
// inherited-descriptor refusal, ordered exactly-once release of held frames,
// and post-registration supervised owner loss.
//
// Scope: test-only. No full borrowed writer, drop/create, or acceptance path.
package recovery_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xtianxx/txharbor/internal/recovery"
)

func buildLifecycleClientHelper(t *testing.T) string {
	t.Helper()
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate lifecycle test source for the standalone client helper")
	}
	source := filepath.Join(filepath.Dir(testFile), "origin_gate_lifecycle_client_linux_testhelper.go")
	output := filepath.Join(t.TempDir(), "origin-gate-lifecycle-client")
	cmd := exec.Command("go", "build", "-trimpath", "-o", output, source)
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	if result, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build lifecycle client helper: %v: %s", err, strings.TrimSpace(string(result)))
	}
	return output
}

func lifecycleLauncher(t *testing.T) *originGateLauncher {
	t.Helper()
	return &originGateLauncher{helperPath: buildLifecycleClientHelper(t)}
}

func lifecycleCommandCount(t *testing.T, events func() string, refs ...string) {
	t.Helper()
	for _, ref := range refs {
		if !strings.Contains(events(), "ORIGIN_LIFECYCLE_SENT ref="+ref) {
			t.Fatalf("lifecycle client never sent %q: %s", ref, events())
		}
	}
}

func lifecycleInsertedRefs(t *testing.T, ctx context.Context, conn *pgx.Conn, table string) string {
	t.Helper()
	var rows string
	if err := conn.QueryRow(ctx,
		`SELECT coalesce(string_agg(ref, ',' ORDER BY seq), '') FROM public.`+pgx.Identifier{table}.Sanitize()).Scan(&rows); err != nil {
		t.Fatalf("read lifecycle table order: %v", err)
	}
	return rows
}

// TestDrillOriginGateLifecycleReleaseInterleaveExactlyOnce proves OG04: frames
// that arrive inside the registration-release transition are forwarded exactly
// once and in order by the single ordered frontend writer.
func TestDrillOriginGateLifecycleReleaseInterleaveExactlyOnce(t *testing.T) {
	detail := "driving several executable frames across the registration release transition and requiring exact-once ordered forwarding"
	recordOriginGateResult(t, "TestDrillOriginGateLifecycleReleaseInterleaveExactlyOnce", &detail)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	fx := newOriginGateFixture(t)
	table := fmt.Sprintf("origin_gate_lifecycle_seq_%d", time.Now().UnixNano())
	if _, err := fx.admin.Exec(ctx, `CREATE TABLE `+pgx.Identifier{table}.Sanitize()+` (seq bigserial PRIMARY KEY, ref text NOT NULL UNIQUE)`); err != nil {
		t.Fatalf("create lifecycle sequence table: %v", err)
	}
	if _, err := fx.admin.Exec(ctx, `GRANT INSERT, SELECT ON `+pgx.Identifier{table}.Sanitize()+` TO `+pgx.Identifier{fx.role}.Sanitize()); err != nil {
		t.Fatalf("grant lifecycle table access: %v", err)
	}
	if _, err := fx.admin.Exec(ctx, `GRANT USAGE, SELECT ON SEQUENCE `+pgx.Identifier{table + "_seq_seq"}.Sanitize()+` TO `+pgx.Identifier{fx.role}.Sanitize()); err != nil {
		t.Fatalf("grant lifecycle sequence usage: %v", err)
	}
	gate := newOriginGate(t, ctx, fx)
	gate.HoldRegistration()
	launcher := lifecycleLauncher(t)
	client, cap, err := launcher.launch(t, ctx, gate,
		[]string{"sequence", gate.Endpoint(), fx.role, table, originGateAppName}, fx.password)
	if err != nil {
		t.Fatalf("spawn lifecycle sequence client: %v", err)
	}
	session, err := gate.Admit(ctx, cap)
	if err != nil {
		t.Fatalf("admit lifecycle sequence origin: %v", err)
	}
	if _, err := client.waitEvent(ctx, "lifecycle client ready", func(line string) bool {
		return strings.HasPrefix(line, "ORIGIN_LIFECYCLE_READY")
	}); err != nil {
		t.Fatalf("%v", err)
	}
	if _, err := client.waitEvent(ctx, "lifecycle SCRAM completion", func(line string) bool {
		return strings.Contains(line, "ORIGIN_LIFECYCLE_AUTH")
	}); err != nil {
		t.Fatalf("%v", err)
	}
	if err := client.sendLine("r1"); err != nil {
		t.Fatalf("send first ref: %v", err)
	}
	if _, err := client.waitEvent(ctx, "first frame held", func(line string) bool {
		return strings.Contains(line, "ORIGIN_LIFECYCLE_SENT ref=r1")
	}); err != nil {
		t.Fatalf("%v", err)
	}
	if err := originGateWaitFor(ctx, "gate to hold the first executable frame", func() bool {
		return session.BufferedFrames() >= 1
	}); err != nil {
		t.Fatalf("%v", err)
	}
	// Release and immediately push two more frames into the transition window.
	gate.ReleaseRegistration()
	if err := client.sendLine("r2"); err != nil {
		t.Fatalf("send second ref: %v", err)
	}
	if err := client.sendLine("r3"); err != nil {
		t.Fatalf("send third ref: %v", err)
	}
	for index := 0; index < 3; index++ {
		if _, err := client.waitEvent(ctx, fmt.Sprintf("command completion %d", index+1), func(line string) bool {
			return strings.Contains(line, "ORIGIN_LIFECYCLE_COMMAND")
		}); err != nil {
			t.Fatalf("%v", err)
		}
	}
	if !session.WatcherReady() {
		t.Fatalf("executable frames were forwarded before the watcher was established")
	}
	if err := client.sendLine("r4"); err != nil {
		t.Fatalf("send fourth ref: %v", err)
	}
	if _, err := client.waitEvent(ctx, "fourth command completion", func(line string) bool {
		return strings.Contains(line, "ORIGIN_LIFECYCLE_COMMAND")
	}); err != nil {
		t.Fatalf("%v", err)
	}
	checkConn, err := pgx.Connect(ctx, fx.dsn)
	if err != nil {
		t.Fatalf("connect lifecycle check session: %v", err)
	}
	defer checkConn.Close(context.Background())
	if err := originGateWaitFor(ctx, "ordered exactly-once rows", func() bool {
		return lifecycleInsertedRefs(t, ctx, checkConn, table) == "r1,r2,r3,r4"
	}); err != nil {
		t.Fatalf("lifecycle frame order/exactly-once violated: %v (observed %q)", err, lifecycleInsertedRefs(t, ctx, checkConn, table))
	}
	var duplicates int
	if err := checkConn.QueryRow(ctx, `SELECT count(*) FROM (SELECT ref FROM public.`+pgx.Identifier{table}.Sanitize()+` GROUP BY ref HAVING count(*) > 1) dup`).Scan(&duplicates); err != nil || duplicates != 0 {
		t.Fatalf("duplicate lifecycle refs forwarded: duplicates=%d err=%v", duplicates, err)
	}
	if err := client.sendLine("quit"); err != nil {
		t.Fatalf("quit lifecycle client: %v", err)
	}
	if err := client.waitExit(ctx); err != nil {
		t.Fatalf("lifecycle client did not exit: %v (events: %s)", err, client.observed())
	}
	if err := originGateWaitFor(ctx, "drain-verified retirement", func() bool {
		state := gate.DrainState()
		return state.Verified && !state.Unknown
	}); err != nil {
		t.Fatalf("%v (drain state %+v)", err, gate.DrainState())
	}
	if latched, reason := gate.Latched(); latched {
		t.Fatalf("gate latched during an ordered clean session: %s", reason)
	}
	detail = fmt.Sprintf("frames r1..r4 driven across the release transition were forwarded exactly once and in order (rows %q, zero duplicates); watcher was established before the first forwarded frame; session retired on authoritatively verified drain (%s)", lifecycleInsertedRefs(t, ctx, checkConn, table), gate.DrainState().Report)
}

// TestDrillOriginGateLifecycleInflightWriteTerminatedDrained proves OG02: a
// real blocked write forwarded before the loss cannot commit after the lock is
// released, because the gate terminates the exact registered backend and
// verifies an authoritative drain before any successor is possible.
func TestDrillOriginGateLifecycleInflightWriteTerminatedDrained(t *testing.T) {
	detail := "forwarding a real lock-blocked INSERT, then losing the observer and requiring exact-identity termination plus authoritative drain with no later commit"
	recordOriginGateResult(t, "TestDrillOriginGateLifecycleInflightWriteTerminatedDrained", &detail)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	fx := newOriginGateFixture(t)
	blockedRef := fmt.Sprintf("origin-gate-inflight-%d", time.Now().UnixNano())
	if _, err := fx.admin.Exec(ctx, `CREATE UNIQUE INDEX `+pgx.Identifier{"origin_gate_inflight_idx_" + strconv.FormatInt(time.Now().UnixNano(), 10)}.Sanitize()+` ON `+pgx.Identifier{fx.table}.Sanitize()+` (ref)`); err != nil {
		t.Fatalf("create unique constraint for the blocked write: %v", err)
	}
	gate := newOriginGate(t, ctx, fx)
	blocker, err := fx.admin.Begin(ctx)
	if err != nil {
		t.Fatalf("begin blocker transaction: %v", err)
	}
	defer func() { _ = blocker.Rollback(context.Background()) }()
	if _, err := blocker.Exec(ctx, `INSERT INTO `+pgx.Identifier{fx.table}.Sanitize()+` (ref) VALUES ($1)`, blockedRef); err != nil {
		t.Fatalf("hold blocker row: %v", err)
	}
	launcher := newOriginGateLauncher(t)
	client, cap, err := launcher.launch(t, ctx, gate,
		[]string{"write", gate.Endpoint(), fx.role, fx.table, blockedRef, "none", originGateAppName}, fx.password)
	if err != nil {
		t.Fatalf("spawn blocked write client: %v", err)
	}
	session, err := gate.Admit(ctx, cap)
	if err != nil {
		t.Fatalf("admit blocked write origin: %v", err)
	}
	registration, err := originGateWaitRegistration(ctx, session)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if _, err := client.waitEvent(ctx, "blocked write forwarded", func(line string) bool {
		return strings.Contains(line, "state=QUERY_SENT")
	}); err != nil {
		t.Fatalf("%v", err)
	}
	if err := originGateWaitFor(ctx, "registered backend waiting on the real row/index lock", func() bool {
		var waitType *string
		if err := fx.admin.QueryRow(ctx, `SELECT wait_event_type FROM pg_stat_activity WHERE pid=$1`, registration.PID).Scan(&waitType); err != nil {
			return false
		}
		return waitType != nil && *waitType == "Lock"
	}); err != nil {
		t.Fatalf("%v", err)
	}
	observer, err := pgx.Connect(ctx, fx.dsn)
	if err != nil {
		t.Fatalf("open independent observation session: %v", err)
	}
	defer observer.Close(context.Background())
	var visible int
	if err := observer.QueryRow(ctx, `SELECT count(*) FROM `+pgx.Identifier{fx.table}.Sanitize()+` WHERE ref=$1`, blockedRef).Scan(&visible); err != nil || visible != 0 {
		t.Fatalf("blocked write was visible before commit: rows=%d err=%v", visible, err)
	}
	var terminated bool
	if err := fx.admin.QueryRow(ctx, `SELECT pg_terminate_backend($1)`, gate.observerPID).Scan(&terminated); err != nil || !terminated {
		t.Fatalf("terminate observer to trigger loss: terminated=%t err=%v", terminated, err)
	}
	if _, err := client.waitEvent(ctx, "blocked channel closed", func(line string) bool {
		return strings.HasPrefix(line, "ORIGIN_CLIENT_EXIT")
	}); err != nil {
		t.Fatalf("%v", err)
	}
	if err := client.waitExit(ctx); err != nil {
		t.Fatalf("blocked write client did not exit: %v", err)
	}
	if err := originGateWaitFor(ctx, "authoritative drain verdict", func() bool {
		state := gate.DrainState()
		return state.Verified || state.Unknown
	}); err != nil {
		t.Fatalf("%v", err)
	}
	if state := gate.DrainState(); !state.Verified {
		t.Fatalf("in-flight termination did not verify an authoritative drain: %+v", state)
	}
	if err := blocker.Rollback(ctx); err != nil {
		t.Fatalf("release blocker row lock: %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	if err := observer.QueryRow(ctx, `SELECT count(*) FROM `+pgx.Identifier{fx.table}.Sanitize()+` WHERE ref=$1`, blockedRef).Scan(&visible); err != nil || visible != 0 {
		t.Fatalf("terminated in-flight write committed after lock release: rows=%d err=%v", visible, err)
	}
	successor, successorCap, err := launcher.launch(t, ctx, gate,
		[]string{"write", gate.Endpoint(), fx.role, fx.table, blockedRef + "-successor", "none", originGateAppName}, fx.password)
	if err != nil {
		t.Fatalf("spawn successor client: %v", err)
	}
	if _, err := gate.Admit(ctx, successorCap); err == nil || !strings.Contains(err.Error(), "latched") {
		t.Fatalf("successor admission was not refused after the drain latch: %v", err)
	}
	_ = successor.killAndWait(ctx)
	detail = fmt.Sprintf("real blocked INSERT forwarded to backend PID %d (wait_event=Lock); observer loss latched admission; exact-identity pg_terminate_backend and authoritative drain verified (%s); blocker rollback produced zero committed rows; successor admission refused", registration.PID, gate.DrainState().Report)
}

// TestDrillOriginGateLifecycleDrainUnknownRetainsAndRefuses proves that a
// control-session fault leaves the exact backend drain unproven: the session
// stays retained, the gate refuses every successor and no replacement owner is
// ever touched.
func TestDrillOriginGateLifecycleDrainUnknownRetainsAndRefuses(t *testing.T) {
	detail := "killing the independent control session before the loss so the exact backend drain cannot be proven; the session must stay retained and successors refused"
	recordOriginGateResult(t, "TestDrillOriginGateLifecycleDrainUnknownRetainsAndRefuses", &detail)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	fx := newOriginGateFixture(t)
	ref := fmt.Sprintf("origin-gate-drain-unknown-%d", time.Now().UnixNano())
	gate := newOriginGate(t, ctx, fx)
	gate.drainTimeout = 4 * time.Second
	launcher := newOriginGateLauncher(t)
	client, cap, err := launcher.launch(t, ctx, gate,
		[]string{"write", gate.Endpoint(), fx.role, fx.table, ref, "none", originGateAppName}, fx.password)
	if err != nil {
		t.Fatalf("spawn drain-unknown client: %v", err)
	}
	session, err := gate.Admit(ctx, cap)
	if err != nil {
		t.Fatalf("admit drain-unknown origin: %v", err)
	}
	if _, err := client.waitEvent(ctx, "first write completion", func(line string) bool {
		return strings.Contains(line, "state=COMMAND")
	}); err != nil {
		t.Fatalf("%v", err)
	}
	var terminated bool
	if err := fx.admin.QueryRow(ctx, `SELECT pg_terminate_backend($1)`, gate.controlPID).Scan(&terminated); err != nil || !terminated {
		t.Fatalf("terminate independent control session: terminated=%t err=%v", terminated, err)
	}
	if err := fx.admin.QueryRow(ctx, `SELECT pg_terminate_backend($1)`, gate.observerPID).Scan(&terminated); err != nil || !terminated {
		t.Fatalf("terminate observer to trigger loss: terminated=%t err=%v", terminated, err)
	}
	if err := originGateWaitFor(ctx, "unknown drain verdict", func() bool {
		return gate.DrainState().Unknown
	}); err != nil {
		t.Fatalf("%v (state %+v)", err, gate.DrainState())
	}
	if _, err := client.waitEvent(ctx, "channel closed", func(line string) bool {
		return strings.HasPrefix(line, "ORIGIN_CLIENT_EXIT")
	}); err != nil {
		t.Fatalf("%v", err)
	}
	if err := client.waitExit(ctx); err != nil {
		t.Fatalf("drain-unknown client did not exit: %v", err)
	}
	if gate.CurrentSession() != session {
		t.Fatalf("session was retired although the backend drain is unproven")
	}
	successor, successorCap, err := launcher.launch(t, ctx, gate,
		[]string{"write", gate.Endpoint(), fx.role, fx.table, ref + "-successor", "none", originGateAppName}, fx.password)
	if err != nil {
		t.Fatalf("spawn successor client: %v", err)
	}
	if _, err := gate.Admit(ctx, successorCap); err == nil || !strings.Contains(err.Error(), "latched") {
		t.Fatalf("successor admission was not refused while the drain is unproven: %v", err)
	}
	_ = successor.killAndWait(ctx)
	detail = fmt.Sprintf("control session termination made the exact backend drain unprovable (%s); admitted session remained retained, successors refused, no replacement owner touched", gate.DrainState().Report)
}

// TestDrillOriginGateLifecycleObserverStallRefuses proves a genuine observer
// stall (the real server backend is SIGSTOPped) is a watched gap that latches
// and authoritatively drains the registered backend.
func TestDrillOriginGateLifecycleObserverStallRefuses(t *testing.T) {
	detail := "stalling the real observer process with SIGSTOP and requiring the watcher gap to latch and drain the registered backend"
	recordOriginGateResult(t, "TestDrillOriginGateLifecycleObserverStallRefuses", &detail)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	fx := newOriginGateFixture(t)
	ref := fmt.Sprintf("origin-gate-observer-stall-%d", time.Now().UnixNano())
	gate := newOriginGate(t, ctx, fx)
	launcher := newOriginGateLauncher(t)
	client, cap, err := launcher.launch(t, ctx, gate,
		[]string{"write", gate.Endpoint(), fx.role, fx.table, ref, "none", originGateAppName}, fx.password)
	if err != nil {
		t.Fatalf("spawn observer-stall client: %v", err)
	}
	session, err := gate.Admit(ctx, cap)
	if err != nil {
		t.Fatalf("admit observer-stall origin: %v", err)
	}
	registration, err := originGateWaitRegistration(ctx, session)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if _, err := client.waitEvent(ctx, "first write completion", func(line string) bool {
		return strings.Contains(line, "state=COMMAND")
	}); err != nil {
		t.Fatalf("%v", err)
	}
	if _, err := dockerExec(ctx, fx.containerID, "sh", "-ec", fmt.Sprintf("kill -STOP %d", gate.observerPID)); err != nil {
		t.Fatalf("SIGSTOP the real observer backend: %v", err)
	}
	defer func() {
		resumeCtx, resumeCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer resumeCancel()
		_, _ = dockerExec(resumeCtx, fx.containerID, "sh", "-ec", fmt.Sprintf("kill -CONT %d 2>/dev/null || true", gate.observerPID))
	}()
	if err := originGateWaitFor(ctx, "observer-stall latch", func() bool {
		latched, _ := gate.Latched()
		return latched
	}); err != nil {
		t.Fatalf("%v", err)
	}
	if _, reason := gate.Latched(); !strings.Contains(reason, "observer") {
		t.Fatalf("observer stall did not latch with an observer liveness reason: %s", reason)
	}
	if err := originGateWaitFor(ctx, "drain after observer stall", func() bool {
		return gate.DrainState().Verified
	}); err != nil {
		t.Fatalf("%v (state %+v)", err, gate.DrainState())
	}
	if _, err := client.waitEvent(ctx, "stalled channel closed", func(line string) bool {
		return strings.HasPrefix(line, "ORIGIN_CLIENT_EXIT")
	}); err != nil {
		t.Fatalf("%v", err)
	}
	if err := client.waitExit(ctx); err != nil {
		t.Fatalf("observer-stall client did not exit: %v", err)
	}
	observation, err := pgx.Connect(ctx, fx.dsn)
	if err != nil {
		t.Fatalf("open post-drain observation session: %v", err)
	}
	defer observation.Close(context.Background())
	var rows int
	if err := observation.QueryRow(ctx, `SELECT count(*) FROM `+pgx.Identifier{fx.table}.Sanitize()+` WHERE ref=$1`, ref).Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("completed pre-stall write lost during drain: rows=%d err=%v", rows, err)
	}
	successor, successorCap, err := launcher.launch(t, ctx, gate,
		[]string{"write", gate.Endpoint(), fx.role, fx.table, ref + "-successor", "none", originGateAppName}, fx.password)
	if err != nil {
		t.Fatalf("spawn successor client: %v", err)
	}
	if _, err := gate.Admit(ctx, successorCap); err == nil {
		t.Fatalf("successor admission was not refused after the observer-stall latch")
	}
	_ = successor.killAndWait(ctx)
	detail = fmt.Sprintf("real observer backend PID %d SIGSTOPped; watcher gap latched (%s) and backend PID %d was authoritatively drained (%s); completed write retained, successor refused", gate.observerPID, reasonOf(gate), registration.PID, gate.DrainState().Report)
}

func reasonOf(gate *originGate) string {
	_, reason := gate.Latched()
	return reason
}

// TestDrillOriginGateLifecycleContextCancelDrains proves context cancellation
// is a latch plus drain, never a silent watcher exit with active forward loops.
func TestDrillOriginGateLifecycleContextCancelDrains(t *testing.T) {
	detail := "canceling the admitted-session context and requiring latch plus authoritative drain"
	recordOriginGateResult(t, "TestDrillOriginGateLifecycleContextCancelDrains", &detail)
	testCtx, cancelTest := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancelTest()
	fx := newOriginGateFixture(t)
	ref := fmt.Sprintf("origin-gate-ctx-cancel-%d", time.Now().UnixNano())
	gateCtx, cancelGate := context.WithCancel(testCtx)
	gate := newOriginGate(t, gateCtx, fx)
	launcher := newOriginGateLauncher(t)
	client, cap, err := launcher.launch(t, testCtx, gate,
		[]string{"write", gate.Endpoint(), fx.role, fx.table, ref, "none", originGateAppName}, fx.password)
	if err != nil {
		t.Fatalf("spawn ctx-cancel client: %v", err)
	}
	session, err := gate.Admit(gateCtx, cap)
	if err != nil {
		t.Fatalf("admit ctx-cancel origin: %v", err)
	}
	if _, err := client.waitEvent(testCtx, "first write completion", func(line string) bool {
		return strings.Contains(line, "state=COMMAND")
	}); err != nil {
		t.Fatalf("%v", err)
	}
	cancelGate()
	if err := originGateWaitFor(testCtx, "context-cancel latch", func() bool {
		latched, _ := gate.Latched()
		return latched
	}); err != nil {
		t.Fatalf("%v", err)
	}
	if _, reason := gate.Latched(); !strings.Contains(reason, "context canceled") {
		t.Fatalf("context cancellation did not latch as such: %s", reason)
	}
	if err := originGateWaitFor(testCtx, "drain after context cancel", func() bool {
		return gate.DrainState().Verified
	}); err != nil {
		t.Fatalf("%v (state %+v)", err, gate.DrainState())
	}
	if _, err := client.waitEvent(testCtx, "canceled channel closed", func(line string) bool {
		return strings.HasPrefix(line, "ORIGIN_CLIENT_EXIT")
	}); err != nil {
		t.Fatalf("%v", err)
	}
	if err := client.waitExit(testCtx); err != nil {
		t.Fatalf("ctx-cancel client did not exit: %v", err)
	}
	_ = session
	detail = fmt.Sprintf("context cancellation latched (%s) and the registered backend was authoritatively drained (%s)", reasonOf(gate), gate.DrainState().Report)
}

// TestDrillOriginGateLifecycleInheritedSocketRefused proves the authoritative
// FD-owner census refuses a copied process that inherits the exact accepted
// socket descriptor.
func TestDrillOriginGateLifecycleInheritedSocketRefused(t *testing.T) {
	detail := "forking a child that inherits the exact accepted socket descriptor and requiring the FD-owner census to refuse it"
	recordOriginGateResult(t, "TestDrillOriginGateLifecycleInheritedSocketRefused", &detail)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	fx := newOriginGateFixture(t)
	gate := newOriginGate(t, ctx, fx)
	launcher := lifecycleLauncher(t)
	client, cap, err := launcher.launch(t, ctx, gate, []string{"fork", gate.Endpoint()}, "")
	if err != nil {
		t.Fatalf("spawn inherited-descriptor client: %v", err)
	}
	forkLine, err := client.waitEvent(ctx, "inherited-descriptor fork record", func(line string) bool {
		return strings.HasPrefix(line, "ORIGIN_LIFECYCLE_FORK")
	})
	if err != nil {
		t.Fatalf("%v", err)
	}
	fields := originGateEventFields(forkLine)
	childPID, _ := strconv.Atoi(fields["child"])
	if childPID > 1 {
		t.Cleanup(func() {
			if process, err := os.FindProcess(childPID); err == nil {
				_ = process.Kill()
			}
		})
	}
	if _, err := gate.Admit(ctx, cap); err == nil {
		t.Fatalf("gate admitted a process whose socket descriptor was inherited by another process")
	}
	if latched, reason := gate.Latched(); !latched || (!strings.Contains(reason, "solely") && !strings.Contains(reason, "inherited")) {
		t.Fatalf("inherited-descriptor origin was not refused by the FD-owner census: latched=%t reason=%s", latched, reason)
	}
	if gate.SupervisorOriginCount() != 0 {
		t.Fatalf("inherited-descriptor origin was published in the registry")
	}
	if err := client.sendLine("quit"); err != nil {
		t.Fatalf("quit fork client: %v", err)
	}
	if err := client.waitExit(ctx); err != nil {
		t.Fatalf("fork client did not exit: %v", err)
	}
	detail = fmt.Sprintf("child PID %s inherited the accepted socket descriptor; admission refused (%s); no origin published, no SQL", fields["child"], reasonOf(gate))
}

// TestDrillOriginGateLifecycleSupervisedOwnerLossDrained is the parked
// post-registration supervised owner-loss proof, migrated to the sealed
// arm/run API: canceling the genuine native child while a blocked restore
// statement is in flight must terminate the exact backend, drain it, and leave
// no committed effect after the row lock is released.
func TestDrillOriginGateLifecycleSupervisedOwnerLossDrained(t *testing.T) {
	detail := "post-registration cancel of the sealed native child with a blocked in-flight restore statement: exact termination, drain, no later commit"
	recordOriginGateResult(t, "TestDrillOriginGateLifecycleSupervisedOwnerLossDrained", &detail)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	fx := newOriginGateFixture(t)
	tools, err := recovery.DrillProvisionNativePGTools(t)
	if err != nil {
		t.Fatalf("provision sealed native tools: %v", err)
	}
	nano := time.Now().UnixNano()
	sourceDB := fmt.Sprintf("origin_sup_ownerloss_src_%d", nano)
	targetDB := fmt.Sprintf("origin_sup_ownerloss_tgt_%d", nano)
	table := "origin_supervisor_ownerloss_rows"
	if err := fx.createOwnedDatabase(ctx, sourceDB); err != nil {
		t.Fatalf("create owned source database: %v", err)
	}
	if err := fx.createOwnedDatabase(ctx, targetDB); err != nil {
		t.Fatalf("create owned target database: %v", err)
	}
	archivePath := supervisorArchiveFromTools(t, ctx, fx, tools, sourceDB, table, 3, true)
	targetAdmin, err := pgx.Connect(ctx, fx.dsnAs(targetDB, "txharbor", "txharbor"))
	if err != nil {
		t.Fatalf("connect supervised target database: %v", err)
	}
	defer targetAdmin.Close(context.Background())
	if _, err := targetAdmin.Exec(ctx, `CREATE TABLE `+pgx.Identifier{table}.Sanitize()+` (id int PRIMARY KEY, note text NOT NULL)`); err != nil {
		t.Fatalf("pre-create target table: %v", err)
	}
	lockTx, err := targetAdmin.Begin(ctx)
	if err != nil {
		t.Fatalf("begin target lock transaction: %v", err)
	}
	defer func() { _ = lockTx.Rollback(context.Background()) }()
	if _, err := lockTx.Exec(ctx, `LOCK TABLE `+pgx.Identifier{table}.Sanitize()+` IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatalf("hold target table lock: %v", err)
	}
	gate := newOriginGate(t, ctx, fx)
	handle := recovery.DrillNewTargetProcessObservation()
	if err := gate.ArmObserved(handle, tools, recovery.DrillPGRestoreArmOptions{
		TargetDSN:   armedGateDSN(gate, targetDB, fx.role, fx.password),
		OperationID: fmt.Sprintf("origin-supervisor-ownerloss-%d", nano),
	}); err != nil {
		t.Fatalf("arm supervised owner-loss restore: %v", err)
	}
	var stderr bytes.Buffer
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	runDone := make(chan struct{})
	var result recovery.PGCommandResult
	var runErr error
	go func() {
		defer close(runDone)
		result, runErr = recovery.DrillRunObservedPGRestore(runCtx, recovery.TargetProcessRunner{DrainTimeout: 15 * time.Second}, handle, supervisorOpenArchive(t, archivePath), nil, &stderr)
	}()
	if err := handle.AwaitBound(ctx); err != nil {
		t.Fatalf("supervised owner-loss operation was never bound: %v", err)
	}
	session, err := gate.AdmitObserved(ctx, handle)
	if err != nil {
		t.Fatalf("admit supervised owner-loss origin: %v", err)
	}
	registration, err := originGateWaitRegistration(ctx, session)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if err := supervisorRegisteredBackendWaiting(t, ctx, fx, registration.PID); err != nil {
		t.Fatalf("%v (stderr bytes=%d)", err, stderr.Len())
	}
	cancelRun()
	select {
	case <-runDone:
	case <-ctx.Done():
		t.Fatalf("canceled supervised restore did not return: %v", ctx.Err())
	}
	if result.Outcome == recovery.PGCommandSucceeded {
		t.Fatalf("canceled supervised restore reported success: %+v err=%v", result, runErr)
	}
	if err := originGateWaitFor(ctx, "supervised owner-loss latch", func() bool {
		latched, _ := gate.Latched()
		return latched
	}); err != nil {
		t.Fatalf("%v", err)
	}
	if err := originGateWaitFor(ctx, "supervised owner-loss drain", func() bool {
		return gate.DrainState().Verified
	}); err != nil {
		t.Fatalf("%v (state %+v)", err, gate.DrainState())
	}
	terminal := handle.WaitTerminal()
	if !terminal.Terminal || terminal.WaitExitCode == 0 {
		t.Fatalf("supervised child terminal fact is not an owner loss: %+v", terminal)
	}
	if err := lockTx.Rollback(ctx); err != nil {
		t.Fatalf("release target lock: %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	if exists, count := supervisorTableState(t, ctx, targetAdmin, table); !exists || count != 0 {
		t.Fatalf("canceled supervised restore committed after lock release: exists=%t rows=%d", exists, count)
	}
	if _, err := handle.ClaimForEndpoint(gate.endpoint); err == nil || !strings.Contains(err.Error(), "claimed") {
		t.Fatalf("used supervised origin handle was not denied on replay: %v", err)
	}
	successor := recovery.DrillNewTargetProcessObservation()
	if err := gate.ArmObserved(successor, tools, recovery.DrillPGRestoreArmOptions{
		TargetDSN: armedGateDSN(gate, targetDB, fx.role, fx.password), OperationID: "ownerloss-successor",
	}); err == nil {
		t.Fatalf("successor arm was accepted after the owner-loss latch")
	}
	detail = fmt.Sprintf("sealed native child canceled after registration with a blocked in-flight statement on backend PID %d; latch %s; drain verified (%s); zero committed rows after lock release; replay and successor refused", registration.PID, reasonOf(gate), gate.DrainState().Report)
}
