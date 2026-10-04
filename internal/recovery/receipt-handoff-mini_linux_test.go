//go:build linux && drill

// receipt-handoff-mini_linux_test.go is the focused real-PG validation of the
// STEP1 opaque receipt -> retained-controller handoff. It reuses the canonical
// borrowed-phase1 fixture read-only and adds a local test-only TCP forwarder so
// the real native pg_restore child reaches the real PostgreSQL server behind
// the factory's armed endpoint. No production/helper file is modified.
package recovery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// receiptHandoffProxy forwards endpoint connections to the real canonical
// target server for this test only.
func receiptHandoffProxy(t *testing.T, endpoint DrillOriginEndpoint, serverAddr string) func() {
	t.Helper()
	listener := endpoint.Listener()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			go func(client net.Conn) {
				defer client.Close()
				server, err := net.Dial("tcp", serverAddr)
				if err != nil {
					return
				}
				defer server.Close()
				copyDone := make(chan struct{})
				go func() {
					_, _ = io.Copy(server, client)
					close(copyDone)
				}()
				_, _ = io.Copy(client, server)
				<-copyDone
			}(client)
		}
	}()
	return func() {
		_ = listener.Close()
		<-done
	}
}

// receiptHandoffClosingProxy accepts and immediately closes, so a real native
// child starts and fails nonzero.
func receiptHandoffClosingProxy(t *testing.T, endpoint DrillOriginEndpoint) func() {
	t.Helper()
	listener := endpoint.Listener()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			_ = client.Close()
		}
	}()
	return func() {
		_ = listener.Close()
		<-done
	}
}

func receiptHandoffNativeRun(t *testing.T, e *borrowedTransportPG, tools DrillNativePGTools, endpoint DrillOriginEndpoint, lock *TargetLock, targetKey TargetKey, archivePath, operation string, probeCalls, acceptanceCalls *int32) *DrillBorrowedWriterRun {
	t.Helper()
	borrowedTransportSeedCleanGuard(t, e, targetKey, operation)
	archive, err := os.Open(archivePath)
	if err != nil {
		t.Fatalf("open receipt-handoff archive: %v", err)
	}
	t.Cleanup(func() { _ = archive.Close() })
	run, err := DrillNewBorrowedWriterRun(e.ctx, TargetWriterOptions{
		OperationKind: TargetWriterOperationRestore,
		Store:         e.store, ControlDSN: e.ctrlDSN, TargetDSN: e.targetDSN,
		ObserverDSN: borrowedTransportObserverDSN(t, e.adminDSN, e.targetDSN), TrustedTarget: e.target,
		OperationID: operation, Archive: archive,
		QuiescenceTimeout: 5 * time.Second, QuiescenceInterval: 50 * time.Millisecond,
		Probe: func(context.Context, TargetWriterProof) (TargetWriterProbeResult, error) {
			atomic.AddInt32(probeCalls, 1)
			return TargetWriterProbeResult{}, fmt.Errorf("receipt-handoff probe intentionally rejects")
		},
		Acceptance: func(context.Context, pgx.Tx, TargetWriterProof) (TargetWriterAcceptance, error) {
			atomic.AddInt32(acceptanceCalls, 1)
			return TargetWriterAcceptance{}, fmt.Errorf("receipt-handoff acceptance must never run")
		},
	}, lock, endpoint, tools)
	if err != nil {
		t.Fatalf("receipt-handoff factory: %v", err)
	}
	return run
}

// TestReceiptHandoffNativeSuccessProbeReject proves the whole STEP1 causal
// chain: real native success with a deliberate Probe rejection still yields
// the opaque token with the ORIGINAL coordinator error, the private receipt is
// consumed exactly once internally, the run is one-use, and only the retained
// controller can recheck the token.
func TestReceiptHandoffNativeSuccessProbeReject(t *testing.T) {
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

	// Real custom archive from an owned source table in the target DB, then
	// drop the table so the real restore is the only creator.
	table := fmt.Sprintf("receipt_handoff_rows_%d", time.Now().UnixNano())
	targetPool := openBorrowedTransportPool(t, e.ctx, e.targetDSN)
	if _, err := targetPool.Exec(e.ctx, `CREATE TABLE public.`+pgx.Identifier{table}.Sanitize()+` (ref text NOT NULL)`); err != nil {
		t.Fatalf("create source table: %v", err)
	}
	for row := 1; row <= 3; row++ {
		if _, err := targetPool.Exec(e.ctx, `INSERT INTO public.`+pgx.Identifier{table}.Sanitize()+` (ref) VALUES ($1)`, fmt.Sprintf("row-%d", row)); err != nil {
			t.Fatalf("seed source row: %v", err)
		}
	}
	dumpPath, _ := tools.DumpPath()
	archivePath := filepath.Join(t.TempDir(), "receipt-handoff.dump")
	dump := exec.CommandContext(e.ctx, dumpPath, "--format=custom", "--no-owner", "--no-privileges", "--file="+archivePath, e.targetDSN)
	if err := dump.Run(); err != nil {
		t.Fatalf("real source dump: %v", err)
	}
	if _, err := targetPool.Exec(e.ctx, `DROP TABLE public.`+pgx.Identifier{table}.Sanitize()); err != nil {
		t.Fatalf("drop source table for pristine restore: %v", err)
	}

	var probeCalls, acceptanceCalls int32
	operation := fmt.Sprintf("receipt-handoff-%d", time.Now().UnixNano())
	run := receiptHandoffNativeRun(t, e, tools, endpoint, lock, targetKey, archivePath, operation, &probeCalls, &acceptanceCalls)
	result, handoff, runErr := run.RunForReceiptHandoff(e.ctx)
	if runErr == nil {
		t.Fatal("deliberate Probe rejection was converted to a nil error")
	}
	facts := handoff.Diagnostics()
	if !facts.Present || facts.Invalidated {
		t.Fatalf("native success with rejected probe produced no token: facts=%+v err=%v", facts, runErr)
	}
	if facts.OriginalKey != targetKey || facts.OperationID != operation || facts.ChildPID <= 0 || facts.ChildStartID == 0 || facts.WriterRoleOID == 0 {
		t.Fatalf("token does not retain the original binding identity: %+v", facts)
	}
	if result.Command.Outcome != PGCommandSucceeded || result.Command.ExitCode != 0 || !result.Command.ProcessGroupDrained {
		t.Fatalf("token was not built on a frozen success/zero/drained command: %+v", result.Command)
	}
	if atomic.LoadInt32(&probeCalls) != 1 || atomic.LoadInt32(&acceptanceCalls) != 0 {
		t.Fatalf("probe/acceptance counts are not the deliberate rejection: probe=%d acceptance=%d", probeCalls, acceptanceCalls)
	}
	if err := handoff.Recheck(e.ctx); err != nil {
		t.Fatalf("bounded token recheck: %v", err)
	}
	if err := handoff.ConsumeForAuth(e.ctx); err != nil {
		t.Fatalf("first auth consumption: %v", err)
	}
	if err := handoff.ConsumeForAuth(e.ctx); err == nil {
		t.Fatal("second auth consumption was accepted")
	}
	// RH02: a nil recheck context permanently invalidates the shared state,
	// visible to every copy, and later operations refuse.
	copied := handoff
	if err := copied.Recheck(nil); err == nil {
		t.Fatal("nil-context recheck was accepted")
	}
	if valid, reason := handoff.Valid(); valid || reason == "" {
		t.Fatalf("nil-context recheck did not permanently invalidate the shared state: valid=%t reason=%q", valid, reason)
	}
	if err := handoff.Recheck(e.ctx); err == nil {
		t.Fatal("permanently invalidated token rechecked")
	}
	if err := handoff.ConsumeForAuth(e.ctx); err == nil {
		t.Fatal("permanently invalidated token accepted auth consumption")
	}
	if result.processReceipt == nil || !result.processReceipt.consumed {
		t.Fatal("private receipt was not consumed exactly once internally")
	}
	if _, err := (DrillTargetProcessReceipt{receipt: result.processReceipt}).ConsumeFacts(); err == nil {
		t.Fatal("external receipt projection bypassed the internal consumption")
	}
	if stage := handoff.GateRetirementStage(); stage != DrillReceiptHandoffGateRetirementStage {
		t.Fatalf("gate retirement stage is not the fixed external requirement: %q", stage)
	}
	if _, _, replayErr := run.RunForReceiptHandoff(e.ctx); replayErr == nil {
		t.Fatal("one-use handoff run replay was accepted")
	}
	var restored int
	if err := targetPool.QueryRow(e.ctx, `SELECT count(*) FROM public.`+pgx.Identifier{table}.Sanitize()).Scan(&restored); err != nil || restored != 3 {
		t.Fatalf("real native restore did not produce three rows: count=%d err=%v", restored, err)
	}

	// Direct in-package bypass check: a receipt consumed through the external
	// projection before the handoff must refuse.
	secondOperation := fmt.Sprintf("receipt-handoff-consumed-%d", time.Now().UnixNano())
	second := receiptHandoffNativeRun(t, e, tools, endpoint, lock, targetKey, archivePath, secondOperation, &probeCalls, &acceptanceCalls)
	anchor, err := second.CaptureControlAnchor(e.ctx)
	if err != nil {
		t.Fatalf("second anchor capture: %v", err)
	}
	secondResult, secondReceipt, secondErr := second.Run(e.ctx)
	if secondErr == nil {
		t.Fatal("second deliberate probe rejection was nil")
	}
	if _, err := secondReceipt.ConsumeFacts(); err != nil {
		t.Fatalf("external consume of the second receipt: %v", err)
	}
	if _, qerr := second.buildReceiptHandoff(e.ctx, anchor, secondResult); qerr == nil {
		t.Fatal("already-consumed private receipt produced a handoff token")
	}
}

// TestReceiptHandoffRefusalsAndSharedInvalidation proves canceled and nonzero
// native runs refuse a token, the two-sided frozen receipt owner is
// permanently invalidated by real control-owner loss (shared with copies),
// and inert values are refused.
func TestReceiptHandoffRefusalsAndSharedInvalidation(t *testing.T) {
	unsetBorrowedTransportPGEnvironment(t)
	e := newBorrowedTransportPG(t)
	tools, err := DrillProvisionNativePGTools(t)
	if err != nil {
		t.Fatalf("provision sealed tools: %v", err)
	}
	targetKey, err := CanonicalTargetKey(e.target)
	if err != nil {
		t.Fatalf("target key: %v", err)
	}
	lock, err := AcquireTargetLock(e.ctx, e.ctrlDSN, targetKey, 3*time.Second, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("borrowed lock: %v", err)
	}
	t.Cleanup(func() { _ = lock.Release(context.Background()) })
	archivePath := filepath.Join(t.TempDir(), "receipt-handoff-refusal.dump")
	if err := os.WriteFile(archivePath, []byte("archive"), 0o600); err != nil {
		t.Fatalf("write placeholder archive: %v", err)
	}
	var probeCalls, acceptanceCalls int32

	// Canceled before the run: no token and the original error is preserved.
	firstEndpoint, err := DrillOpenOriginEndpoint()
	if err != nil {
		t.Fatalf("open canceled-run endpoint: %v", err)
	}
	t.Cleanup(func() { _ = firstEndpoint.Listener().Close() })
	canceledRun := receiptHandoffNativeRun(t, e, tools, firstEndpoint, lock, targetKey, archivePath, fmt.Sprintf("receipt-handoff-canceled-%d", time.Now().UnixNano()), &probeCalls, &acceptanceCalls)
	canceledCtx, cancel := context.WithCancel(e.ctx)
	cancel()
	if _, handoff, err := canceledRun.RunForReceiptHandoff(canceledCtx); err == nil {
		t.Fatal("canceled handoff run was accepted")
	} else if handoff.Diagnostics().Present {
		t.Fatal("canceled handoff run produced a token")
	}

	// Real native child that fails nonzero (endpoint closes immediately): no
	// token even though the child really started.
	secondEndpoint, err := DrillOpenOriginEndpoint()
	if err != nil {
		t.Fatalf("open nonzero-run endpoint: %v", err)
	}
	stopClosing := receiptHandoffClosingProxy(t, secondEndpoint)
	defer stopClosing()
	failingRun := receiptHandoffNativeRun(t, e, tools, secondEndpoint, lock, targetKey, archivePath, fmt.Sprintf("receipt-handoff-failing-%d", time.Now().UnixNano()), &probeCalls, &acceptanceCalls)
	if _, handoff, err := failingRun.RunForReceiptHandoff(e.ctx); err == nil {
		t.Fatal("nonzero native handoff run was accepted")
	} else if handoff.Diagnostics().Present {
		t.Fatal("nonzero native handoff run produced a token")
	}

	// Inert and forged values cannot act as tokens.
	var zero OpaqueDrillReceiptHandoff
	if err := zero.Recheck(e.ctx); err == nil {
		t.Fatal("zero handoff rechecked")
	}
	if err := zero.ConsumeForAuth(e.ctx); err == nil {
		t.Fatal("zero handoff consumed")
	}
	if zero.Diagnostics().Present {
		t.Fatal("zero handoff reported diagnostics")
	}
	var nilPointer *OpaqueDrillReceiptHandoff
	if err := nilPointer.Recheck(e.ctx); err == nil {
		t.Fatal("nil handoff rechecked")
	}
	encoded, err := json.Marshal(zero)
	if err != nil || string(encoded) != "{}" {
		t.Fatalf("handoff serialized authority material: err=%v doc=%q", err, encoded)
	}
	var decoded OpaqueDrillReceiptHandoff
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal handoff: %v", err)
	}
	if err := decoded.Recheck(e.ctx); err == nil {
		t.Fatal("JSON round-trip handoff rechecked")
	}
}

// TestReceiptHandoffProcessReapedClassification is the pure Linux identity
// classification regression: a live child with the same start is not reaped, a
// same-PID different-start (PID reuse) is gone, an exited/reaped child is gone,
// and a zombie is not reaped.
func TestReceiptHandoffProcessReapedClassification(t *testing.T) {
	live := exec.Command("sleep", "30")
	if err := live.Start(); err != nil {
		t.Fatalf("start live child: %v", err)
	}
	t.Cleanup(func() {
		_ = live.Process.Kill()
		_ = live.Wait()
	})
	liveStart, err := linuxProcessStartIdentity(live.Process.Pid)
	if err != nil || liveStart == 0 {
		t.Fatalf("live child start identity: err=%v start=%d", err, liveStart)
	}
	if err := receiptHandoffProcessReaped(live.Process.Pid, liveStart); err == nil {
		t.Fatal("live child with the same start was classified as reaped")
	}
	if err := receiptHandoffProcessReaped(live.Process.Pid, liveStart+1); err != nil {
		t.Fatalf("same-PID different-start (PID reuse) was not classified as gone: %v", err)
	}

	reaped := exec.Command("true")
	if err := reaped.Start(); err != nil {
		t.Fatalf("start reaped child: %v", err)
	}
	reapedStart, err := linuxProcessStartIdentity(reaped.Process.Pid)
	if err != nil {
		t.Fatalf("reaped child start identity: %v", err)
	}
	if err := reaped.Wait(); err != nil {
		t.Fatalf("wait reaped child: %v", err)
	}
	if err := receiptHandoffProcessReaped(reaped.Process.Pid, reapedStart); err != nil {
		t.Fatalf("reaped child was not classified as gone: %v", err)
	}

	zombie := exec.Command("true")
	if err := zombie.Start(); err != nil {
		t.Fatalf("start zombie child: %v", err)
	}
	zombieStart, err := linuxProcessStartIdentity(zombie.Process.Pid)
	if err != nil {
		t.Fatalf("zombie child start identity: %v", err)
	}
	t.Cleanup(func() { _ = zombie.Wait() })
	deadline := time.Now().Add(5 * time.Second)
	for {
		raw, readErr := os.ReadFile(fmt.Sprintf("/proc/%d/stat", zombie.Process.Pid))
		if readErr == nil {
			if end := strings.LastIndexByte(string(raw), ')'); end >= 0 {
				if fields := strings.Fields(string(raw[end+1:])); len(fields) > 0 && fields[0] == "Z" {
					break
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("child never became a zombie for the classification check")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := receiptHandoffProcessReaped(zombie.Process.Pid, zombieStart); err == nil {
		t.Fatal("zombie child was classified as reaped")
	}
}

// TestReceiptHandoffOwnerLossInvalidatesToken covers the previously missing
// RH03 owner-loss case: a genuinely valid token is invalidated by real
// pg_terminate_backend of the captured control backend, shared with copies,
// and every later recheck/consumption refuses.
func TestReceiptHandoffOwnerLossInvalidatesToken(t *testing.T) {
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

	table := fmt.Sprintf("receipt_handoff_ownerloss_rows_%d", time.Now().UnixNano())
	targetPool := openBorrowedTransportPool(t, e.ctx, e.targetDSN)
	if _, err := targetPool.Exec(e.ctx, `CREATE TABLE public.`+pgx.Identifier{table}.Sanitize()+` (ref text NOT NULL)`); err != nil {
		t.Fatalf("create owner-loss source table: %v", err)
	}
	for row := 1; row <= 3; row++ {
		if _, err := targetPool.Exec(e.ctx, `INSERT INTO public.`+pgx.Identifier{table}.Sanitize()+` (ref) VALUES ($1)`, fmt.Sprintf("row-%d", row)); err != nil {
			t.Fatalf("seed owner-loss source row: %v", err)
		}
	}
	dumpPath, _ := tools.DumpPath()
	archivePath := filepath.Join(t.TempDir(), "receipt-handoff-ownerloss.dump")
	dump := exec.CommandContext(e.ctx, dumpPath, "--format=custom", "--no-owner", "--no-privileges", "--file="+archivePath, e.targetDSN)
	if err := dump.Run(); err != nil {
		t.Fatalf("real owner-loss source dump: %v", err)
	}
	if _, err := targetPool.Exec(e.ctx, `DROP TABLE public.`+pgx.Identifier{table}.Sanitize()); err != nil {
		t.Fatalf("drop owner-loss source table: %v", err)
	}

	var probeCalls, acceptanceCalls int32
	run := receiptHandoffNativeRun(t, e, tools, endpoint, lock, targetKey, archivePath, fmt.Sprintf("receipt-handoff-ownerloss-%d", time.Now().UnixNano()), &probeCalls, &acceptanceCalls)
	_, handoff, runErr := run.RunForReceiptHandoff(e.ctx)
	if runErr == nil || !handoff.Diagnostics().Present {
		t.Fatalf("owner-loss positive token missing: err=%v facts=%+v", runErr, handoff.Diagnostics())
	}
	ownerPID := handoff.state.anchor.Diagnostics().BackendPID
	if ownerPID <= 0 {
		t.Fatalf("captured control backend PID is missing: %d", ownerPID)
	}
	if _, err := e.ctrl.Exec(e.ctx, `SELECT pg_terminate_backend($1)`, ownerPID); err != nil {
		t.Fatalf("terminate captured control owner: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		if err := handoff.Recheck(e.ctx); err != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("real control-owner loss did not invalidate the token")
		}
		time.Sleep(20 * time.Millisecond)
	}
	copied := handoff
	if valid, reason := copied.Valid(); valid || reason == "" {
		t.Fatalf("owner-loss invalidation is not shared with copies: valid=%t reason=%q", valid, reason)
	}
	if err := copied.ConsumeForAuth(e.ctx); err == nil {
		t.Fatal("owner-loss invalidated token accepted auth consumption")
	}
	if err := handoff.Recheck(e.ctx); err == nil {
		t.Fatal("owner-loss invalidated token rechecked")
	}
	if atomic.LoadInt32(&probeCalls) != 1 || atomic.LoadInt32(&acceptanceCalls) != 0 {
		t.Fatalf("owner-loss attempt probe/acceptance counts wrong: probe=%d acceptance=%d", probeCalls, acceptanceCalls)
	}
}

// TestReceiptHandoffProcessStatParserRefusals is the RH01 negative-only parser
// regression: wrong leading PID, unrecognized state, garbage/zero/overflow
// start, truncated or malformed stat documents and injected read errors must
// all refuse as UNKNOWN; only a structurally valid different numeric start on
// the expected PID may classify the original process as gone. None of this is
// a sole-wait token by itself.
func TestReceiptHandoffProcessStatParserRefusals(t *testing.T) {
	fields := append([]string{"S"}, make([]string, 18)...)
	for index := 1; index < len(fields); index++ {
		fields[index] = "0"
	}
	fields = append(fields, "99")
	valid := "4242 (pg restore (x)) " + strings.Join(fields, " ")
	state, start, err := parseReceiptHandoffProcessStat(4242, []byte(valid))
	if err != nil || state != "S" || start != 99 {
		t.Fatalf("valid stat parse failed: state=%q start=%d err=%v", state, start, err)
	}
	if _, _, err := parseReceiptHandoffProcessStat(1, []byte(valid)); err == nil {
		t.Fatal("wrong leading PID was accepted")
	}
	if _, _, err := parseReceiptHandoffProcessStat(4242, []byte(strings.Replace(valid, " S ", " ? ", 1))); err == nil {
		t.Fatal("unrecognized state was accepted")
	}
	for _, startText := range []string{"abc", "0", "999999999999999999999999"} {
		if _, _, err := parseReceiptHandoffProcessStat(4242, []byte(strings.Replace(valid, " 99", " "+startText, 1))); err == nil {
			t.Fatalf("garbage start %q was accepted", startText)
		}
	}
	if _, _, err := parseReceiptHandoffProcessStat(4242, []byte("4242 (pg) S 1 2 3")); err == nil {
		t.Fatal("truncated stat was accepted")
	}
	if _, _, err := parseReceiptHandoffProcessStat(4242, []byte("4242 pg_restore S 1 2 3")); err == nil {
		t.Fatal("malformed command delimiter was accepted")
	}
	for name, readErr := range map[string]error{"eacces": fs.ErrPermission, "eio": errors.New("io error")} {
		stage, stageErr := receiptHandoffProcessIdentityStage(4242, 99, func(int) ([]byte, error) { return nil, readErr })
		if stageErr == nil || stage != receiptHandoffStageUnknown {
			t.Fatalf("%s read error was not an UNKNOWN refusal: stage=%v err=%v", name, stage, stageErr)
		}
	}
	if stage, stageErr := receiptHandoffProcessIdentityStage(4242, 99, func(int) ([]byte, error) { return []byte("garbage"), nil }); stageErr == nil || stage != receiptHandoffStageUnknown {
		t.Fatalf("malformed injected stat was not an UNKNOWN refusal: stage=%v err=%v", stage, stageErr)
	}
	live := exec.Command("sleep", "30")
	if err := live.Start(); err != nil {
		t.Fatalf("start live process for stage classification: %v", err)
	}
	t.Cleanup(func() {
		_ = live.Process.Kill()
		_ = live.Wait()
	})
	liveStart, err := linuxProcessStartIdentity(live.Process.Pid)
	if err != nil || liveStart == 0 {
		t.Fatalf("live process start identity: err=%v start=%d", err, liveStart)
	}
	if stage, err := receiptHandoffProcessIdentityStage(live.Process.Pid, liveStart, nil); err != nil || stage != receiptHandoffStageLive {
		t.Fatalf("live same-start process was not LIVE: stage=%v err=%v", stage, err)
	}
	if stage, err := receiptHandoffProcessIdentityStage(live.Process.Pid, liveStart+1, nil); err != nil || stage != receiptHandoffStageGone {
		t.Fatalf("valid different numeric start was not GONE: stage=%v err=%v", stage, err)
	}
	// A valid PID with a different numeric start is Gone even when the state
	// is Z (PID reuse); only the same PID/start Z is the exited-not-reaped
	// case, and the same start in another recognized state stays Live.
	zombieRaw := []byte(strings.Replace(valid, " S ", " Z ", 1))
	if stage, err := receiptHandoffProcessIdentityStage(4242, 99, func(int) ([]byte, error) { return zombieRaw, nil }); err != nil || stage != receiptHandoffStageZombie {
		t.Fatalf("same PID/start Z was not ZOMBIE: stage=%v err=%v", stage, err)
	}
	reusedRaw := []byte(strings.Replace(string(zombieRaw), " 99", " 100", 1))
	if stage, err := receiptHandoffProcessIdentityStage(4242, 99, func(int) ([]byte, error) { return reusedRaw, nil }); err != nil || stage != receiptHandoffStageGone {
		t.Fatalf("valid different start with Z was not GONE (PID reuse): stage=%v err=%v", stage, err)
	}
}

// TestReceiptHandoffAfterRunErrorBranch is the pure bounded regression for the
// cancel-after-run decision: the coordinator error stays authoritative when
// present, otherwise the ended context error is returned (never a silent nil).
// The real nil-runErr path requires a full acceptance contract, which is out
// of this scope and is not claimed here.
func TestReceiptHandoffAfterRunErrorBranch(t *testing.T) {
	coordinatorErr := errors.New("coordinator error")
	if got := receiptHandoffAfterRunError(context.Canceled, coordinatorErr); got != coordinatorErr {
		t.Fatalf("coordinator error was not preserved: %v", got)
	}
	if got := receiptHandoffAfterRunError(context.Canceled, nil); got != context.Canceled {
		t.Fatalf("ended context did not become the returned error: %v", got)
	}
}

// TestReceiptHandoffPublicationSeams covers the RH03 notification seams with
// genuine tokens from real native-success/Probe-reject runs: Recheck and
// ConsumeForAuth pause after a successful anchor recheck and before the shared
// publication mutex; own-parent cancellation and copy invalidation must both
// refuse and permanently invalidate the shared state. A legitimate
// consumption keeps the token genuinely rechecked, and a second copy cannot
// consume again.
func TestReceiptHandoffPublicationSeams(t *testing.T) {
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

	table := fmt.Sprintf("receipt_handoff_seam_rows_%d", time.Now().UnixNano())
	targetPool := openBorrowedTransportPool(t, e.ctx, e.targetDSN)
	if _, err := targetPool.Exec(e.ctx, `CREATE TABLE public.`+pgx.Identifier{table}.Sanitize()+` (ref text NOT NULL)`); err != nil {
		t.Fatalf("create seam source table: %v", err)
	}
	for row := 1; row <= 3; row++ {
		if _, err := targetPool.Exec(e.ctx, `INSERT INTO public.`+pgx.Identifier{table}.Sanitize()+` (ref) VALUES ($1)`, fmt.Sprintf("row-%d", row)); err != nil {
			t.Fatalf("seed seam source row: %v", err)
		}
	}
	dumpPath, _ := tools.DumpPath()
	archivePath := filepath.Join(t.TempDir(), "receipt-handoff-seam.dump")
	dump := exec.CommandContext(e.ctx, dumpPath, "--format=custom", "--no-owner", "--no-privileges", "--file="+archivePath, e.targetDSN)
	if err := dump.Run(); err != nil {
		t.Fatalf("real seam source dump: %v", err)
	}
	if _, err := targetPool.Exec(e.ctx, `DROP TABLE public.`+pgx.Identifier{table}.Sanitize()); err != nil {
		t.Fatalf("drop seam source table: %v", err)
	}
	mint := func(subtestT *testing.T, subtest string) OpaqueDrillReceiptHandoff {
		subtestT.Helper()
		var probeCalls, acceptanceCalls int32
		run := receiptHandoffNativeRun(t, e, tools, endpoint, lock, targetKey, archivePath, fmt.Sprintf("receipt-handoff-seam-%s-%d", subtest, time.Now().UnixNano()), &probeCalls, &acceptanceCalls)
		_, token, runErr := run.RunForReceiptHandoff(e.ctx)
		if runErr == nil || !token.Diagnostics().Present {
			subtestT.Fatalf("genuine token mint failed: err=%v facts=%+v", runErr, token.Diagnostics())
		}
		return token
	}
	pause := func(subtest *testing.T, token OpaqueDrillReceiptHandoff, want string) (chan struct{}, func()) {
		subtest.Helper()
		entered := make(chan struct{})
		release := make(chan struct{})
		var releaseOnce sync.Once
		releaseFunc := func() { releaseOnce.Do(func() { close(release) }) }
		var first atomic.Bool
		token.state.stage = func(stage string) {
			if stage != want || !first.CompareAndSwap(false, true) {
				return
			}
			close(entered)
			<-release
		}
		// The cleanup unblocks the paused publisher on every fatal path,
		// before any later assertion can leak the goroutine.
		subtest.Cleanup(releaseFunc)
		return entered, releaseFunc
	}
	waitEntered := func(subtest *testing.T, entered chan struct{}) {
		subtest.Helper()
		select {
		case <-entered:
		case <-time.After(30 * time.Second):
			subtest.Fatal("publication seam was never entered")
		}
	}

	t.Run("recheck own-parent cancel", func(t *testing.T) {
		token := mint(t, "recheck-owncancel")
		entered, release := pause(t, token, "recheck-publish")
		parent, cancelParent := context.WithCancel(e.ctx)
		result := make(chan error, 1)
		go func() { result <- token.Recheck(parent) }()
		waitEntered(t, entered)
		cancelParent()
		release()
		select {
		case err := <-result:
			if err == nil {
				t.Fatal("paused recheck published success after own-parent cancellation")
			}
		case <-time.After(30 * time.Second):
			t.Fatal("paused recheck never completed")
		}
		copied := token
		if valid, reason := copied.Valid(); valid || reason == "" {
			t.Fatalf("own-cancel did not permanently invalidate copies: valid=%t reason=%q", valid, reason)
		}
		if err := token.Recheck(e.ctx); err == nil {
			t.Fatal("own-cancel invalidated token rechecked")
		}
	})

	t.Run("recheck copy invalidation", func(t *testing.T) {
		token := mint(t, "recheck-copyinvalid")
		entered, release := pause(t, token, "recheck-publish")
		result := make(chan error, 1)
		go func() { result <- token.Recheck(e.ctx) }()
		waitEntered(t, entered)
		copied := token
		canceled, cancel := context.WithCancel(e.ctx)
		cancel()
		if err := copied.Recheck(canceled); err == nil {
			t.Fatal("copy invalidation recheck unexpectedly succeeded")
		}
		release()
		select {
		case err := <-result:
			if err == nil {
				t.Fatal("paused recheck published success after copy invalidation")
			}
		case <-time.After(30 * time.Second):
			t.Fatal("paused recheck never completed after copy invalidation")
		}
		if valid, reason := token.Valid(); valid || reason == "" {
			t.Fatalf("copy invalidation is not shared: valid=%t reason=%q", valid, reason)
		}
	})

	t.Run("consume own-parent cancel", func(t *testing.T) {
		token := mint(t, "consume-owncancel")
		entered, release := pause(t, token, "consume-publish")
		parent, cancelParent := context.WithCancel(e.ctx)
		result := make(chan error, 1)
		go func() { result <- token.ConsumeForAuth(parent) }()
		waitEntered(t, entered)
		cancelParent()
		release()
		select {
		case err := <-result:
			if err == nil {
				t.Fatal("paused consumption published success after own-parent cancellation")
			}
		case <-time.After(30 * time.Second):
			t.Fatal("paused consumption never completed")
		}
		if diagnostics := token.Diagnostics(); diagnostics.Consumed {
			t.Fatal("failed own-cancel consumption marked the token consumed")
		}
		copied := token
		if valid, _ := copied.Valid(); valid {
			t.Fatal("own-cancel consumption did not invalidate the shared state")
		}
	})

	t.Run("consume copy invalidation", func(t *testing.T) {
		token := mint(t, "consume-copyinvalid")
		entered, release := pause(t, token, "consume-publish")
		result := make(chan error, 1)
		go func() { result <- token.ConsumeForAuth(e.ctx) }()
		waitEntered(t, entered)
		copied := token
		canceled, cancel := context.WithCancel(e.ctx)
		cancel()
		if err := copied.Recheck(canceled); err == nil {
			t.Fatal("copy invalidation recheck unexpectedly succeeded")
		}
		release()
		select {
		case err := <-result:
			if err == nil {
				t.Fatal("paused consumption published success after copy invalidation")
			}
		case <-time.After(30 * time.Second):
			t.Fatal("paused consumption never completed after copy invalidation")
		}
		if diagnostics := token.Diagnostics(); diagnostics.Consumed {
			t.Fatal("invalidated consumption marked the token consumed")
		}
	})

	t.Run("legit consume keeps genuine recheck and is one-time", func(t *testing.T) {
		token := mint(t, "consume-legit")
		if err := token.ConsumeForAuth(e.ctx); err != nil {
			t.Fatalf("legitimate consumption: %v", err)
		}
		if err := token.Recheck(e.ctx); err != nil {
			t.Fatalf("consumed token lost its genuine recheck: %v", err)
		}
		copied := token
		if err := copied.ConsumeForAuth(e.ctx); err == nil {
			t.Fatal("second copy consumption was accepted")
		}
	})
}
