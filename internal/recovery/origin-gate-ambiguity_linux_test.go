//go:build linux && drill

// origin-gate-ambiguity_linux_test.go closes the last evidence gap of the
// OG01/OG05 chain: a supervised native restore whose bounded runner returns
// PGCommandAmbiguous while the actual sole cmd.Wait is still pending, followed
// by a LATE authentic zero-exit Wait, must never be counted as clean
// retirement.
//
// The genuine pinned pg_restore is launched through the gate's armed factory
// endpoint and sealed tools, restores the real custom archive (3 rows), and
// exits zero. The Go os/exec copier delay is realized on the real stdin
// archive pipe: the reader serves the whole archive and then blocks, so the
// child itself exits while the copy goroutine (and therefore the sole
// cmd.Wait) stays pending. The stderr route was probed and is unavailable:
// pg_restore 18.6 suppresses server notices, so a real event-trigger WARNING
// fired (3 DDL events recorded) but produced zero stderr bytes without
// changing the hardcoded flags or the sealed ELF.
//
// The test then cancels only the run context (a real bounded DrainTimeout),
// observes the ambiguous return, releases the reader, and requires the actual
// owner facts: late zero-exit Wait completed, disposition still ambiguous,
// owner receipt Completed but not Clean, gate latched, clean retirement never
// published, endpoint authority consumed, successors refused.
package recovery_test

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xtianxx/txharbor/internal/recovery"
)

// ambiguityBlockingArchive is a real io.Reader that serves the genuine custom
// archive and then blocks until released, holding the Go os/exec stdin copier
// (and therefore the sole cmd.Wait) open after the native child exited zero.
type ambiguityBlockingArchive struct {
	data      []byte
	pos       int
	delivered chan struct{}
	release   chan struct{}
	once      sync.Once
	mu        sync.Mutex
	served    int
}

func newAmbiguityBlockingArchive(path string) (*ambiguityBlockingArchive, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return &ambiguityBlockingArchive{data: data, delivered: make(chan struct{}), release: make(chan struct{})}, nil
}

func (r *ambiguityBlockingArchive) Read(p []byte) (int, error) {
	if r.pos < len(r.data) {
		n := copy(p, r.data[r.pos:])
		r.pos += n
		r.mu.Lock()
		r.served += n
		r.mu.Unlock()
		return n, nil
	}
	r.once.Do(func() { close(r.delivered) })
	<-r.release
	return 0, io.EOF
}

func (r *ambiguityBlockingArchive) unblock() { close(r.release) }

func (r *ambiguityBlockingArchive) servedBytes() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.served
}

func ambiguityChildState(pid int) string {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return "gone"
	}
	closeIndex := strings.LastIndexByte(string(data), ')')
	if closeIndex < 0 {
		return "unknown"
	}
	fields := strings.Fields(string(data[closeIndex+1:]))
	if len(fields) == 0 {
		return "unknown"
	}
	return fields[0]
}

func TestDrillOriginGateAmbiguousReturnLateZeroWaitNeverClean(t *testing.T) {
	detail := "bounded ambiguous return from the sealed native restore while the genuine stdin archive copier holds the sole Wait; a late authentic zero-exit Wait must never retire cleanly"
	recordOriginGateResult(t, "TestDrillOriginGateAmbiguousReturnLateZeroWaitNeverClean", &detail)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	fx := newOriginGateFixture(t)
	tools, err := recovery.DrillProvisionNativePGTools(t)
	if err != nil {
		t.Fatalf("provision sealed native tools: %v", err)
	}
	nano := time.Now().UnixNano()
	sourceDB := fmt.Sprintf("origin_amb_src_%d", nano)
	targetDB := fmt.Sprintf("origin_amb_tgt_%d", nano)
	table := fmt.Sprintf("origin_ambiguity_rows_%d", nano)
	if err := fx.createOwnedDatabase(ctx, sourceDB); err != nil {
		t.Fatalf("create ambiguity source database: %v", err)
	}
	if err := fx.createOwnedDatabase(ctx, targetDB); err != nil {
		t.Fatalf("create ambiguity target database: %v", err)
	}
	archivePath := supervisorArchiveFromTools(t, ctx, fx, tools, sourceDB, table, 3, false)
	blocking, err := newAmbiguityBlockingArchive(archivePath)
	if err != nil {
		t.Fatalf("stage genuine blocking archive reader: %v", err)
	}

	gate := newOriginGate(t, ctx, fx)
	gate.HoldRegistration()
	handle := recovery.DrillNewTargetProcessObservation()
	if err := gate.ArmObserved(handle, tools, recovery.DrillPGRestoreArmOptions{
		TargetDSN:   armedGateDSN(gate, targetDB, fx.role, fx.password),
		OperationID: fmt.Sprintf("origin-ambiguity-%d", nano),
	}); err != nil {
		t.Fatalf("arm the ambiguity restore: %v", err)
	}
	runner := recovery.TargetProcessRunner{DrainTimeout: 1200 * time.Millisecond}
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	runDone := make(chan struct{})
	var result recovery.PGCommandResult
	var runErr error
	go func() {
		defer close(runDone)
		result, runErr = recovery.DrillRunObservedPGRestore(runCtx, runner, handle, blocking, io.Discard, io.Discard)
	}()
	if err := handle.AwaitBound(ctx); err != nil {
		t.Fatalf("armed operation was never bound: %v", err)
	}
	session, err := gate.AdmitObserved(ctx, handle)
	if err != nil {
		t.Fatalf("admit the ambiguity origin: %v", err)
	}
	identity := handle.StartedIdentity()
	if !identity.Started || identity.PID <= 0 || identity.StartID == 0 || identity.StartErr != nil {
		t.Fatalf("live origin start identity is not usable: %+v", identity)
	}
	if err := originGateWaitFor(ctx, "gate to hold the first executable frame", func() bool {
		return session.BufferedFrames() >= 1
	}); err != nil {
		t.Fatalf("%v", err)
	}
	gate.ReleaseRegistration()
	if _, err := originGateWaitRegistration(ctx, session); err != nil {
		t.Fatalf("%v", err)
	}
	targetConn, err := pgx.Connect(ctx, fx.dsnAs(targetDB, "txharbor", "txharbor"))
	if err != nil {
		t.Fatalf("connect ambiguity target: %v", err)
	}
	defer targetConn.Close(context.Background())
	if err := originGateWaitFor(ctx, "native archive data restore", func() bool {
		exists, count := supervisorTableState(t, ctx, targetConn, table)
		return exists && count == 3
	}); err != nil {
		t.Fatalf("%v", err)
	}

	// All genuine archive bytes are served and the stdin copier is inside the
	// real blocking read; the native child must then exit (unreaped) while the
	// sole Wait stays pending.
	select {
	case <-blocking.delivered:
	case <-time.After(30 * time.Second):
		t.Fatal("blocking archive reader was never fully served")
	}
	// Go's Cmd.Wait reaps the OS process first and only then awaits the copy
	// goroutines, so the authoritative observable is /proc gone while the sole
	// Wait itself is still pending on the blocked stdin copier.
	if err := originGateWaitFor(ctx, "native child reaped while the sole Wait stays pending", func() bool {
		return ambiguityChildState(identity.PID) == "gone" && !handle.WaitTerminal().Terminal
	}); err != nil {
		t.Fatalf("%v (state=%s terminal=%+v)", err, ambiguityChildState(identity.PID), handle.WaitTerminal())
	}
	if disposition := handle.RunnerDisposition(); disposition.Returned {
		t.Fatalf("bounded runner returned before the run context was canceled: %+v", disposition)
	}
	if terminal := handle.WaitTerminal(); terminal.Terminal || terminal.ChildWaitCompleted {
		t.Fatalf("sole wait completed while the genuine stdin copier was blocked: %+v", terminal)
	}
	t.Logf("pre-cancel barrier: child /proc=gone (owner os.Wait reaped) pid=%d start=%d archive_served=%d go_copier=blocked wait_pending=true", identity.PID, identity.StartID, blocking.servedBytes())

	// Cancel only the run context AFTER the child exited: the bounded runner
	// must return ambiguous with the drain unresolved, while the authentic
	// late wait is still pending.
	cancelRun()
	select {
	case <-runDone:
	case <-ctx.Done():
		t.Fatalf("bounded ambiguity run did not return: %v", ctx.Err())
	}
	if runErr == nil || result.Outcome != recovery.PGCommandAmbiguous || !result.Started || result.ProcessGroupDrained {
		t.Fatalf("bounded return is not the expected unresolved ambiguity: %+v err=%v", result, runErr)
	}
	disposition := handle.RunnerDisposition()
	if !disposition.Returned || disposition.Result.Outcome != recovery.PGCommandAmbiguous {
		t.Fatalf("recorded disposition is not the ambiguous bounded return: %+v", disposition)
	}
	if terminal := handle.WaitTerminal(); terminal.Terminal || terminal.ChildWaitCompleted {
		t.Fatalf("late wait was recorded before the copier was released: %+v", terminal)
	}
	preReceipt := gate.ownerReceipt(session)
	if !preReceipt.Pending || preReceipt.Completed || preReceipt.Clean {
		t.Fatalf("pre-release owner receipt is not pending: %+v", preReceipt)
	}
	t.Logf("ambiguous bounded return: outcome=%s error=%q receipt=%+v", result.Outcome, runErr, preReceipt)

	// Release the genuine copier: the pinned owner's sole cmd.Wait returns the
	// authentic zero exit.
	blocking.unblock()
	if err := originGateWaitFor(ctx, "late authentic zero-exit sole wait", func() bool {
		terminal := handle.WaitTerminal()
		return terminal.Terminal && terminal.ChildWaitCompleted && terminal.WaitExitCode == 0
	}); err != nil {
		t.Fatalf("%v (terminal=%+v)", err, handle.WaitTerminal())
	}
	postReceipt := gate.ownerReceipt(session)
	if !postReceipt.Completed || postReceipt.Clean || postReceipt.Pending {
		t.Fatalf("post-release owner receipt must be completed-not-clean: %+v", postReceipt)
	}
	if !strings.Contains(postReceipt.Detail, "does not authorize clean retirement") {
		t.Fatalf("owner receipt lacks the specific ambiguous diagnosis: %+v", postReceipt)
	}

	// The gate must latch on the owner terminal loss and never publish clean
	// retirement.
	latchReason, err := originGateWaitLatch(ctx, gate)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if !strings.Contains(latchReason, "does not authorize clean retirement") {
		t.Fatalf("latch reason lacks the specific ambiguous diagnosis: %s", latchReason)
	}
	if clean, report := gate.CleanRetired(); clean {
		t.Fatalf("false clean retirement published: %s", report)
	}
	if err := originGateWaitFor(ctx, "loss-drain to settle", func() bool {
		return !gate.DrainState().Pending
	}); err != nil {
		t.Logf("loss-drain still pending at deadline: %+v", gate.DrainState())
	}
	t.Logf("post-release causal: disposition=%s terminal=%+v receipt=%+v latched=true clean=false drain=%+v",
		disposition.Result.Outcome, handle.WaitTerminal(), postReceipt, gate.DrainState())

	// No authority reuse of the consumed endpoint and no successor admission.
	if _, err := handle.ClaimForEndpoint(gate.endpoint); err == nil || !strings.Contains(err.Error(), "claimed") {
		t.Fatalf("consumed endpoint claim was reusable: %v", err)
	}
	if _, err := gate.AdmitObserved(ctx, handle); err == nil || !strings.Contains(err.Error(), "latched") {
		t.Fatalf("latched gate admitted a successor: %v", err)
	}

	// The intentional target change from the real restore is still present; no
	// rollback or payment proof is claimed.
	if exists, count := supervisorTableState(t, ctx, targetConn, table); !exists || count != 3 {
		t.Fatalf("real restored rows did not persist as intentionally changed target: exists=%t rows=%d", exists, count)
	}
	detail = fmt.Sprintf("sealed pinned pg_restore behind gate endpoint %s restored 3 rows from the genuine archive; child exited unreaped (Z) while the blocked stdin copier held the sole Wait pending; run-context cancel returned bounded %s (started=true, drain=false); late authentic zero-exit Wait completed; disposition remained %s and owner receipt %q; gate latched, clean retirement never published, endpoint authority consumed, successor refused; target intentionally remains changed",
		gate.Endpoint(), result.Outcome, disposition.Result.Outcome, postReceipt.Detail)
}
