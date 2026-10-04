//go:build linux && drill

// borrowed-gate-safety_linux_test.go covers the BG01/BG03/BG04 safety fixes:
// the final full watcher pass after the borrowed pre-release check (observer
// loss during the check must refuse with zero forwarded frames), the
// native/control identity classification (PID + captured start of the exact
// instance, never a globally compared tick), and the immutable gate mode
// lifecycle (ordinary/borrowed freeze before admission, one borrowed install).
package recovery_test

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xtianxx/txharbor/internal/recovery"
)

// TestBorrowedGateNativeControlIdentityClassification is the BG03 regression:
// two different PIDs with equal Linux start ticks are legitimately distinct
// identities; the same PID/start pair is a same-instance conflation; an empty
// native start or a missing/zero control PID is refused. This is identity
// classification only, never a restore-success proof.
func TestBorrowedGateNativeControlIdentityClassification(t *testing.T) {
	if err := borrowedNativeControlDistinct(101, "5000", 202, "5000"); err != nil {
		t.Fatalf("equal start ticks on different PIDs were refused as the same process: %v", err)
	}
	if err := borrowedNativeControlDistinct(101, "1234", 202, "5678"); err != nil {
		t.Fatalf("distinct native/control identities were refused: %v", err)
	}
	if err := borrowedNativeControlDistinct(101, "5000", 101, "5000"); err == nil {
		t.Fatal("same PID/start pair was accepted as distinct native/control identities")
	}
	if err := borrowedNativeControlDistinct(101, "", 202, "5000"); err == nil {
		t.Fatal("empty native OS start was accepted")
	}
	if err := borrowedNativeControlDistinct(101, "5000", 0, "5000"); err == nil {
		t.Fatal("missing control PID was accepted")
	}
}

func borrowedGateArmOptions(g *originGate, operation string) recovery.DrillPGRestoreArmOptions {
	return recovery.DrillPGRestoreArmOptions{
		TargetDSN:   "postgres://txharbor:txharbor@" + g.Endpoint() + "/txharbor?sslmode=disable",
		OperationID: operation,
	}
}

// TestBorrowedGateModeLifecycle is the BG04 regression: the gate mode is
// frozen under g.mu before admission/arm. A second borrowed install, an
// ordinary arm on a borrowed gate, and a borrowed install after any ordinary
// arm are all refused; concurrent borrowed installs admit exactly one winner.
func TestBorrowedGateModeLifecycle(t *testing.T) {
	f := newBorrowedGateFixture(t)
	ctx := context.Background()

	// 1. Repeated borrowed install on the already-borrowed gate is refused and
	// leaves the frozen mode/hook untouched.
	if err := f.gate.UseBorrowedOrigin(f.run, f.prefix); err == nil {
		t.Fatal("second borrowed install was accepted")
	}
	f.gate.mu.Lock()
	hook := f.gate.borrowedCheck
	mode := f.gate.mode
	f.gate.mu.Unlock()
	if hook == nil || mode != gateModeBorrowed {
		t.Fatal("second borrowed install changed the frozen mode/hook")
	}

	// 2. An ordinary arm on the borrowed gate is refused by the frozen mode.
	armErr := f.gate.ArmObserved(recovery.DrillNewTargetProcessObservation(), f.tools, borrowedGateArmOptions(f.gate, "mode-borrowed-ordinary"))
	if armErr == nil || !strings.Contains(armErr.Error(), "borrowed") {
		t.Fatalf("ordinary arm on a borrowed gate was not refused by the mode: %v", armErr)
	}

	// 3. Ordinary-first gate refuses a borrowed install and keeps the normal
	// hook nil.
	ordinaryGate := newOriginGate(t, ctx, f.fx)
	if err := ordinaryGate.ArmObserved(recovery.DrillNewTargetProcessObservation(), f.tools, borrowedGateArmOptions(ordinaryGate, "mode-ordinary-first")); err != nil {
		t.Fatalf("ordinary arm on a free gate: %v", err)
	}
	if err := ordinaryGate.UseBorrowedOrigin(f.run, f.prefix); err == nil {
		t.Fatal("borrowed install was accepted after an ordinary arm")
	}
	ordinaryGate.mu.Lock()
	ordinaryHook := ordinaryGate.borrowedCheck
	ordinaryMode := ordinaryGate.mode
	ordinaryGate.mu.Unlock()
	if ordinaryHook != nil || ordinaryMode != gateModeOrdinary {
		t.Fatal("refused borrowed install changed the ordinary mode/nil hook")
	}

	// 4. Concurrent borrowed installs on a free gate: exactly one mode winner.
	freeGate := newOriginGate(t, ctx, f.fx)
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for index := 0; index < 2; index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- freeGate.UseBorrowedOrigin(f.run, f.prefix)
		}()
	}
	wg.Wait()
	close(results)
	wins := 0
	for err := range results {
		if err == nil {
			wins++
		}
	}
	if wins != 1 {
		t.Fatalf("concurrent borrowed installs admitted %d winners, want exactly 1", wins)
	}
	if err := freeGate.UseBorrowedOrigin(f.run, f.prefix); err == nil {
		t.Fatal("late borrowed install after the race was accepted")
	}
}

// confirmBorrowedGateObserverGone proves, through an independent protected
// admin session, that the exact observer backend (PID + backend_start) no
// longer appears in pg_stat_activity. The pg_terminate_backend return value is
// checked by the caller; this is the actual-node confirmation, never a boolean
// shortcut.
func confirmBorrowedGateObserverGone(t *testing.T, ctx context.Context, admin *pgx.Conn, pid int, backendStart time.Time) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var rows int
		if err := admin.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE pid=$1 AND backend_start=$2`, pid, backendStart).Scan(&rows); err != nil {
			t.Fatalf("independent observer-absence probe failed: %v", err)
		}
		if rows == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("observer backend PID %d (start %s) did not disappear from pg_stat_activity", pid, backendStart)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestBorrowedGateObserverLossDuringCheckZeroFrames is the BG01 causal test.
// The post-catalog stage BLOCKS the borrowed check before the independent
// prefix recheck and the genuine Health call; the exact observer backend is
// then terminated and its absence is confirmed on an independent admin session
// before the barrier is released. The real check must still succeed and ONLY
// the final full watcher may refuse: the latch reason is the exact final
// watcher cause (not the pre-release check or the initial watcher), with zero
// forwarded executable frames, no target effects, a genuine healthy control
// owner and prefix chain, child sole-wait reap, verified backend drain and no
// replay.
func TestBorrowedGateObserverLossDuringCheckZeroFrames(t *testing.T) {
	f := newBorrowedGateFixture(t)
	ctx := context.Background()
	stageEntered := make(chan struct{})
	terminationConfirmed := make(chan struct{})
	hookCtx, cancelHook := context.WithTimeout(context.Background(), 45*time.Second)
	var confirmOnce sync.Once
	confirmTermination := func() { confirmOnce.Do(func() { close(terminationConfirmed) }) }
	t.Cleanup(func() {
		cancelHook()
		confirmTermination()
	})
	var enteredOnce sync.Once
	f.gate.borrowedOrigin.stage = func(stage string) {
		if stage != "after-catalog" {
			return
		}
		enteredOnce.Do(func() { close(stageEntered) })
		select {
		case <-terminationConfirmed:
		case <-hookCtx.Done():
		}
	}

	outcome := make(chan borrowedGateRunOutcome, 1)
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	go func() {
		result, receipt, err := f.run.Run(runCtx)
		outcome <- borrowedGateRunOutcome{result: result, receipt: receipt, err: err}
	}()
	admitCtx, cancelAdmit := context.WithTimeout(ctx, 90*time.Second)
	defer cancelAdmit()
	session, err := f.gate.AdmitBorrowed(admitCtx, f.run, f.prefix)
	if err != nil {
		cancelRun()
		t.Fatalf("borrowed admission: %v", err)
	}
	f.waitFor(t, 60*time.Second, "the held first executable frame", func() bool {
		return session.BackendPID() > 0 && session.BufferedFrames() > 0
	})
	select {
	case <-stageEntered:
	case <-time.After(60 * time.Second):
		cancelRun()
		t.Fatal("borrowed pre-release check never entered the post-catalog stage")
	}
	// The hook is now BLOCKED before the independent prefix recheck and the
	// genuine Health call. The observer backend identity is the real captured
	// PostgreSQL PID/backend_start, never a fake boolean.
	observerPID := f.gate.observerPID
	observerBackendStart := f.gate.observerBackendAt
	if observerPID <= 0 || observerBackendStart.IsZero() {
		cancelRun()
		t.Fatalf("observer backend identity is missing: pid=%d start=%s", observerPID, observerBackendStart)
	}
	// Terminate exactly the observer backend through the protected fixture
	// administrator (not the control owner and not the fixture admin itself).
	var terminated bool
	if err := f.fx.admin.QueryRow(ctx, `SELECT pg_terminate_backend($1)`, observerPID).Scan(&terminated); err != nil || !terminated {
		cancelRun()
		t.Fatalf("terminate the protected observer backend: terminated=%t err=%v", terminated, err)
	}
	// Confirm the actual observer row is ABSENT on an independent admin session
	// before releasing the barrier: the observer's PG connection is really gone.
	confirmBorrowedGateObserverGone(t, ctx, f.fx.admin, observerPID, observerBackendStart)
	confirmTermination()

	f.waitFor(t, 60*time.Second, "the gate latch after the final watcher refusal", func() bool {
		latched, _ := f.gate.Latched()
		return latched
	})
	latched, latchReason := f.gate.Latched()
	if !latched {
		t.Fatal("gate did not latch after the observer loss")
	}
	if !strings.Contains(latchReason, "borrowed origin final watcher could not be re-established") {
		t.Fatalf("latch reason is not the final watcher refusal: %q", latchReason)
	}
	if strings.Contains(latchReason, "pre-release check failed") || strings.Contains(latchReason, "protected origin watcher could not be established") {
		t.Fatalf("latch came from an earlier stage, not the final full watcher: %q", latchReason)
	}
	if session.WatcherReady() {
		t.Fatal("executable frames were released despite the observer loss")
	}
	f.waitFor(t, 30*time.Second, "the backend-specific authoritative drain", func() bool {
		return f.gate.DrainState().Verified
	})
	drain := f.gate.DrainState()
	if !drain.Verified || drain.Unknown {
		t.Fatalf("backend drain after observer loss is not authoritatively verified: %+v", drain)
	}
	select {
	case got := <-outcome:
		if got.err == nil {
			t.Fatal("observer loss during the borrowed check was reported as success")
		}
		facts, err := got.receipt.ConsumeFacts()
		if err != nil {
			t.Fatalf("frozen receipt after observer loss: %v", err)
		}
		if !facts.Started || !facts.Terminal || !facts.Command.ProcessGroupDrained {
			t.Fatalf("native child has no stopped/reaped sole-wait facts: %+v", facts)
		}
		if _, err := got.receipt.ConsumeFacts(); err == nil {
			t.Fatal("process receipt replay was accepted after observer loss")
		}
	case <-time.After(60 * time.Second):
		t.Fatal("coordinator run did not return after the observer loss")
	}
	if atomic.LoadInt32(&f.probeCalls) != 0 || atomic.LoadInt32(&f.acceptanceCalls) != 0 {
		t.Fatalf("observer-loss attempt ran probe/acceptance: probe=%d acceptance=%d", f.probeCalls, f.acceptanceCalls)
	}
	if !f.relationAbsent(t) {
		t.Fatal("observer-loss attempt executed target frames")
	}
	if session.BufferedFrames() == 0 {
		t.Fatal("buffered executable frame was forwarded despite the refusal")
	}
	// Only the observer/native freshness was lost: the independent identity
	// prefix chain and the genuine control owner remain healthy, which proves
	// the earlier real borrowed check actually succeeded.
	if err := f.prefix.inspect(ctx); err != nil {
		t.Fatalf("identity prefix recheck after observer termination is not healthy: %v", err)
	}
	if err := f.run.Health(ctx); err != nil {
		t.Fatalf("control owner health after observer termination is not genuine: %v", err)
	}
	if state := f.guardState(t); state == "clean" {
		t.Fatal("observer-loss attempt left a clean target guard")
	}
	if _, _, err := f.run.Run(ctx); err == nil {
		t.Fatal("one-use borrowed run replay was accepted after observer loss")
	}
}
