//go:build linux && drill

// receipt-handoff-ambiguity_linux_test.go is the causal frozen-ambiguity test
// for the STEP1 receipt handoff: the ACTUAL authentic factory
// RunForReceiptHandoff path is driven with a genuine blocking archive reader,
// so the real native pg_restore delivers/restores the complete custom archive
// and exits zero while the Go os/exec stdin copier (and therefore the sole
// cmd.Wait) stays pending. Cancelling the run context yields the real bounded
// PGCommandAmbiguous return with no handoff token; releasing the copier
// afterwards produces the late authentic zero-exit Wait, which must leave the
// frozen ambiguous disposition and the absent token unchanged.
package recovery

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// receiptHandoffBlockingArchive serves the complete genuine custom archive and
// then blocks until released, holding the stdin copy goroutine (and the sole
// Wait) open after the native child already exited zero.
type receiptHandoffBlockingArchive struct {
	mu        sync.Mutex
	data      []byte
	pos       int
	release   chan struct{}
	once      sync.Once
	delivered chan struct{}
	deliver   sync.Once
}

func newReceiptHandoffBlockingArchive(path string) (*receiptHandoffBlockingArchive, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return &receiptHandoffBlockingArchive{data: data, release: make(chan struct{}), delivered: make(chan struct{})}, nil
}

func (r *receiptHandoffBlockingArchive) Read(p []byte) (int, error) {
	r.mu.Lock()
	if r.pos < len(r.data) {
		n := copy(p, r.data[r.pos:])
		r.pos += n
		r.mu.Unlock()
		if r.pos >= len(r.data) {
			r.deliver.Do(func() { close(r.delivered) })
		}
		return n, nil
	}
	r.mu.Unlock()
	r.deliver.Do(func() { close(r.delivered) })
	<-r.release
	return 0, io.EOF
}

func (r *receiptHandoffBlockingArchive) unblock() {
	r.once.Do(func() { close(r.release) })
}

// TestReceiptHandoffFrozenAmbiguityLateZeroWaitNoToken drives the real
// coordinator through RunForReceiptHandoff with a blocking archive: the native
// restore completes (three rows are an intentional unverified side effect)
// while the sole Wait stays pending, the run context cancellation returns the
// frozen PGCommandAmbiguous result with NO handoff token, and the late
// authentic zero-exit Wait afterwards does not change the frozen disposition,
// create a token, resurrect the one-use run, or disturb the healthy control
// owner.
func TestReceiptHandoffFrozenAmbiguityLateZeroWaitNoToken(t *testing.T) {
	unsetBorrowedTransportPGEnvironment(t)
	e := newBorrowedTransportPG(t)
	tools, err := DrillProvisionNativePGTools(t)
	if err != nil {
		t.Fatalf("provision sealed tools: %v", err)
	}
	endpoint, err := DrillOpenOriginEndpoint()
	if err != nil {
		t.Fatalf("open factory endpoint: %v", err)
	}
	proxyAddr := net.JoinHostPort(e.target.Host, strconv.Itoa(int(e.target.Port)))
	stopProxy := receiptHandoffProxy(t, endpoint, proxyAddr)
	defer stopProxy()
	targetKey, err := CanonicalTargetKey(e.target)
	if err != nil {
		t.Fatalf("target key: %v", err)
	}
	lock, err := AcquireTargetLock(e.ctx, e.ctrlDSN, targetKey, 3*time.Second, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("borrowed lock: %v", err)
	}
	t.Cleanup(func() { _ = lock.Release(context.Background()) })

	// Genuine custom archive with three rows, then drop the table so the real
	// restore is the only creator (an intentional unverified side effect).
	table := fmt.Sprintf("receipt_handoff_amb_rows_%d", time.Now().UnixNano())
	targetPool := openBorrowedTransportPool(t, e.ctx, e.targetDSN)
	if _, err := targetPool.Exec(e.ctx, `CREATE TABLE public.`+pgx.Identifier{table}.Sanitize()+` (ref text NOT NULL)`); err != nil {
		t.Fatalf("create ambiguity source table: %v", err)
	}
	for row := 1; row <= 3; row++ {
		if _, err := targetPool.Exec(e.ctx, `INSERT INTO public.`+pgx.Identifier{table}.Sanitize()+` (ref) VALUES ($1)`, fmt.Sprintf("row-%d", row)); err != nil {
			t.Fatalf("seed ambiguity source row: %v", err)
		}
	}
	dumpPath, _ := tools.DumpPath()
	archivePath := filepath.Join(t.TempDir(), "receipt-handoff-ambiguity.dump")
	dump := exec.CommandContext(e.ctx, dumpPath, "--format=custom", "--no-owner", "--no-privileges", "--file="+archivePath, e.targetDSN)
	if err := dump.Run(); err != nil {
		t.Fatalf("real ambiguity source dump: %v", err)
	}
	if _, err := targetPool.Exec(e.ctx, `DROP TABLE public.`+pgx.Identifier{table}.Sanitize()); err != nil {
		t.Fatalf("drop source table for pristine restore: %v", err)
	}
	blocking, err := newReceiptHandoffBlockingArchive(archivePath)
	if err != nil {
		t.Fatalf("open blocking archive: %v", err)
	}
	t.Cleanup(blocking.unblock)

	operation := fmt.Sprintf("receipt-handoff-ambiguity-%d", time.Now().UnixNano())
	borrowedTransportSeedCleanGuard(t, e, targetKey, operation)
	var probeCalls, acceptanceCalls int32
	run, err := DrillNewBorrowedWriterRun(e.ctx, TargetWriterOptions{
		OperationKind: TargetWriterOperationRestore,
		Store:         e.store, ControlDSN: e.ctrlDSN, TargetDSN: e.targetDSN,
		ObserverDSN: borrowedTransportObserverDSN(t, e.adminDSN, e.targetDSN), TrustedTarget: e.target,
		OperationID: operation, Archive: blocking,
		Runner:            TargetProcessRunner{DrainTimeout: 500 * time.Millisecond},
		QuiescenceTimeout: 5 * time.Second, QuiescenceInterval: 50 * time.Millisecond,
		Probe: func(context.Context, TargetWriterProof) (TargetWriterProbeResult, error) {
			atomic.AddInt32(&probeCalls, 1)
			return TargetWriterProbeResult{}, fmt.Errorf("ambiguity probe must never run")
		},
		Acceptance: func(context.Context, pgx.Tx, TargetWriterProof) (TargetWriterAcceptance, error) {
			atomic.AddInt32(&acceptanceCalls, 1)
			return TargetWriterAcceptance{}, fmt.Errorf("ambiguity acceptance must never run")
		},
	}, lock, endpoint, tools)
	if err != nil {
		t.Fatalf("ambiguity handoff factory: %v", err)
	}
	observation := run.Observation()
	runCtx, cancelRun := context.WithCancel(e.ctx)
	defer cancelRun()
	type ambiguityOutcome struct {
		result  TargetWriterResult
		handoff OpaqueDrillReceiptHandoff
		err     error
	}
	outcome := make(chan ambiguityOutcome, 1)
	go func() {
		result, handoff, err := run.RunForReceiptHandoff(runCtx)
		outcome <- ambiguityOutcome{result: result, handoff: handoff, err: err}
	}()

	// Barrier 1: the complete archive has been served to the native child.
	select {
	case <-blocking.delivered:
	case <-time.After(60 * time.Second):
		cancelRun()
		t.Fatal("native child never consumed the complete archive")
	}
	// Barrier 2: the real native child exited (zombie: Wait still pending),
	// the restore side effect landed, and the sole Wait has NOT completed.
	deadline := time.Now().Add(60 * time.Second)
	for {
		identity := observation.StartedIdentity()
		if identity.PID > 0 && identity.StartID != 0 {
			stage, stageErr := receiptHandoffProcessIdentityStage(identity.PID, identity.StartID, nil)
			if stageErr != nil {
				cancelRun()
				t.Fatalf("strict child identity inspection error (refusal): %v", stageErr)
			}
			if stage == receiptHandoffStageZombie || stage == receiptHandoffStageGone {
				break
			}
		}
		if time.Now().After(deadline) {
			cancelRun()
			t.Fatalf("native child did not exit with the sole Wait still pending: %+v", observation.StartedIdentity())
		}
		time.Sleep(20 * time.Millisecond)
	}
	var restored int
	if err := targetPool.QueryRow(e.ctx, `SELECT count(*) FROM public.`+pgx.Identifier{table}.Sanitize()).Scan(&restored); err != nil || restored != 3 {
		cancelRun()
		t.Fatalf("native restore side effect did not land before cancellation: count=%d err=%v", restored, err)
	}
	if terminal := observation.WaitTerminal(); terminal.Terminal || terminal.ChildWaitCompleted {
		cancelRun()
		t.Fatalf("sole Wait completed before cancellation; no genuine ambiguity: %+v", terminal)
	}

	// Real bounded cancellation while the copier blocks: PGCommandAmbiguous.
	cancelRun()
	var got ambiguityOutcome
	select {
	case got = <-outcome:
	case <-time.After(60 * time.Second):
		t.Fatal("ambiguous handoff run did not return after cancellation")
	}
	if got.err == nil {
		t.Fatal("ambiguous run was reported as success")
	}
	if got.result.Command.Outcome != PGCommandAmbiguous || !got.result.Command.Started || got.result.Command.ProcessGroupDrained {
		t.Fatalf("run did not return the real bounded ambiguous disposition: %+v", got.result.Command)
	}
	if got.handoff.Diagnostics().Present {
		t.Fatal("ambiguous run produced a handoff token")
	}
	if err := got.handoff.ConsumeForAuth(e.ctx); err == nil {
		t.Fatal("absent handoff token accepted auth consumption")
	}
	if got.result.processReceipt == nil {
		t.Fatal("ambiguous run produced no frozen private receipt")
	}
	if got.result.processReceipt.snapshot.terminal || got.result.processReceipt.snapshot.result.Outcome != PGCommandAmbiguous {
		t.Fatalf("frozen receipt is not the ambiguous disposition: %+v", got.result.processReceipt.snapshot)
	}

	// Late authentic zero-exit Wait after the reader is released: the copier
	// finishes and the pinned owner records the real terminal success.
	blocking.unblock()
	deadline = time.Now().Add(30 * time.Second)
	for {
		terminal := observation.WaitTerminal()
		if terminal.Terminal && terminal.ChildWaitCompleted {
			if terminal.WaitErr != nil || terminal.WaitExitCode != 0 {
				t.Fatalf("late Wait was not a genuine zero exit: %+v", terminal)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("late authentic zero-exit Wait never completed: %+v", terminal)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got.result.Command.Outcome != PGCommandAmbiguous || got.result.Command.ProcessGroupDrained {
		t.Fatalf("late zero Wait changed the frozen ambiguous disposition: %+v", got.result.Command)
	}
	if !got.result.processReceipt.snapshot.runnerReturned || got.result.processReceipt.snapshot.terminal {
		t.Fatalf("frozen receipt changed after the late Wait: %+v", got.result.processReceipt.snapshot)
	}

	// One-use run replay and unchanged identity/health.
	if _, replayHandoff, replayErr := run.RunForReceiptHandoff(e.ctx); replayErr == nil {
		t.Fatal("one-use ambiguity run replay was accepted")
	} else if replayHandoff.Diagnostics().Present {
		t.Fatal("replayed ambiguity run produced a token")
	}
	if atomic.LoadInt32(&probeCalls) != 0 || atomic.LoadInt32(&acceptanceCalls) != 0 {
		t.Fatalf("ambiguous attempt ran probe/acceptance: probe=%d acceptance=%d", probeCalls, acceptanceCalls)
	}
	if run.Binding().OriginalTargetKey() != targetKey ||
		run.Binding().OriginalRoleFingerprint() != e.target.DataTargetFingerprint().RoleFingerprint ||
		run.Binding().OriginalOperationID() != operation {
		t.Fatal("ambiguous run changed the original target/role/operation binding")
	}
	if run.TransportTargetKey() == targetKey {
		t.Fatal("transport key collapsed onto the original target key")
	}
	if err := run.Health(e.ctx); err != nil {
		t.Fatalf("control owner is not healthy after the ambiguous cancellation: %v", err)
	}
	var disposition string
	if err := e.ctrl.QueryRow(e.ctx, `SELECT disposition FROM recovery_target_guard WHERE target_guard_key=$1`, targetKey.String()).Scan(&disposition); err != nil {
		t.Fatalf("ambiguous guard state read: %v", err)
	}
	if disposition == "clean" {
		t.Fatal("ambiguous attempt left a clean target guard")
	}
}
