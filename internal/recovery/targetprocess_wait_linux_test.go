//go:build linux

package recovery

// This file proves the bounded supervised-runner contract: the value the
// runner returns to its caller is not evidence that the sole cmd.Wait
// completed, and lifecycle-thread ownership follows the actual wait, not the
// bounded return. The delay is produced by real OS behavior: a real child
// writes to a stdout sink whose first Write blocks, so Go's exec.Cmd.Wait
// stays pending on its copier goroutine until this test releases it. No fake
// command, no fault-injection hook, and no second Wait is involved. The nil
// lock-health callback means the run is process supervision only; it is never
// a target-lock or target-cleanliness claim.

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"
)

// delayedWaitWriter is a real stdout/stderr sink whose first Write reports
// that the exec.Cmd copier reached it and then blocks until the test releases
// it. The blocked copier keeps the sole cmd.Wait from returning.
type delayedWaitWriter struct {
	entered     chan struct{}
	release     chan struct{}
	enterOnce   sync.Once
	releaseOnce sync.Once
	mu          sync.Mutex
	writes      int
}

func newDelayedWaitWriter() *delayedWaitWriter {
	return &delayedWaitWriter{entered: make(chan struct{}), release: make(chan struct{})}
}

func (w *delayedWaitWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	w.writes++
	w.mu.Unlock()
	w.enterOnce.Do(func() { close(w.entered) })
	<-w.release
	return len(p), nil
}

func (w *delayedWaitWriter) observedWrite() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writes > 0
}

func (w *delayedWaitWriter) unlock() {
	w.releaseOnce.Do(func() { close(w.release) })
}

type delayedWaitRun struct {
	result PGCommandResult
	err    error
}

func awaitDelayedWaitSnapshot(t *testing.T, observation *targetProcessObservation, ready func(targetProcessSnapshot) bool, timeout time.Duration) targetProcessSnapshot {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if snapshot := observation.snapshot(); ready(snapshot) {
			return snapshot
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("target process observation did not reach the required fact before deadline")
	return targetProcessSnapshot{}
}

func awaitDelayedWaitChildGone(pid int, startID uint64, timeout time.Duration) bool {
	return waitForLinuxProcessGone(pid, startID, timeout)
}

func TestDelayedWaitChildGoneRefusesUnreadableIdentity(t *testing.T) {
	previous := linuxProcessStatRead
	t.Cleanup(func() { linuxProcessStatRead = previous })
	for _, input := range []struct {
		data []byte
		err  error
	}{{nil, os.ErrPermission}, {[]byte("malformed stat"), nil}} {
		linuxProcessStatRead = func(string) ([]byte, error) { return input.data, input.err }
		if awaitDelayedWaitChildGone(123456, 42, 0) {
			t.Fatal("inspection refusal must not prove delayed-wait child disappearance")
		}
	}
}

// killExactDelayedWaitChild terminates only a PID that still carries the
// recorded start identity, so test cleanup can never signal a reused PID.
func killExactDelayedWaitChild(pid int, startID uint64) {
	if actual, err := linuxProcessStartIdentity(pid); err == nil && actual == startID {
		_ = syscall.Kill(pid, syscall.SIGKILL)
	}
}

func awaitDelayedWaitGoroutineDrain(t *testing.T, baseline int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= baseline {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("sole pinned owner goroutine did not exit after actual wait completion (goroutines=%d baseline=%d)",
		runtime.NumGoroutine(), baseline)
}

func TestSupervisedRunnerBoundedReturnDoesNotClaimPendingSoleWait(t *testing.T) {
	cases := []struct {
		name string
		// script writes real output, so the exec.Cmd stdout copier blocks in the
		// controlled sink, and the sole Wait cannot return until release.
		script string
		// exitedSuccessfully distinguishes a child that finished with status 0
		// from one still running when the bound cancels it.
		exitedSuccessfully bool
	}{
		{
			name:               "successful-child-held-open-by-copier",
			script:             "printf ready\nexec /bin/sleep 0.4\n",
			exitedSuccessfully: true,
		},
		{
			name:               "running-child-killed-at-the-bound",
			script:             "printf ready\nexec /bin/sleep 30\n",
			exitedSuccessfully: false,
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			writer := newDelayedWaitWriter()
			observation := &targetProcessObservation{}
			runner := TargetProcessRunner{
				DrainTimeout: 250 * time.Millisecond,
				PollInterval: 10 * time.Millisecond,
				observation:  observation,
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			baseline := runtime.NumGoroutine()
			done := make(chan delayedWaitRun, 1)
			go func() {
				result, err := runner.RunPGCommand(ctx, "/bin/sh", []string{"-c", testCase.script}, nil, writer, writer, nil)
				done <- delayedWaitRun{result: result, err: err}
			}()
			var launched bool
			var pid int
			var startID uint64
			t.Cleanup(func() {
				writer.unlock()
				if launched {
					killExactDelayedWaitChild(pid, startID)
				}
			})

			select {
			case <-writer.entered:
			case outcome := <-done:
				t.Fatalf("runner returned before its stdout copier blocked: result=%+v err=%v", outcome.result, outcome.err)
			case <-time.After(5 * time.Second):
				t.Fatal("child stdout copier never reached the controlled sink")
			}
			startedSnapshot := awaitDelayedWaitSnapshot(t, observation, func(s targetProcessSnapshot) bool {
				return s.started
			}, 5*time.Second)
			if startedSnapshot.cmd == nil || startedSnapshot.process == nil ||
				startedSnapshot.cmd.Process != startedSnapshot.process ||
				startedSnapshot.pid != startedSnapshot.process.Pid ||
				startedSnapshot.startID == 0 || startedSnapshot.startErr != nil {
				t.Fatalf("start observation does not identify the exact child: %+v", startedSnapshot)
			}
			if startedSnapshot.runnerReturned || startedSnapshot.childWaitCompleted || startedSnapshot.terminal {
				t.Fatalf("start observation fabricated return/wait facts: %+v", startedSnapshot)
			}
			launched, pid, startID = true, startedSnapshot.pid, startedSnapshot.startID

			if testCase.exitedSuccessfully {
				// The real child exited 0 and its PID identity is already gone,
				// yet the sole Wait is still pending on the blocked stdout copier.
				if !awaitDelayedWaitChildGone(pid, startID, 3*time.Second) {
					t.Fatal("real child did not exit successfully before the bound")
				}
			}

			cancel()
			var bounded delayedWaitRun
			select {
			case bounded = <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("bounded runner did not return while the sole Wait was pending")
			}
			if bounded.err == nil || bounded.result.Outcome != PGCommandAmbiguous ||
				!bounded.result.Started || bounded.result.ProcessGroupDrained {
				t.Fatalf("bounded return must be ambiguous, started and undrained: result=%+v err=%v", bounded.result, bounded.err)
			}
			if !writer.observedWrite() {
				t.Fatal("runner returned without the controlled sink ever being reached")
			}
			select {
			case <-writer.release:
				t.Fatal("controlled sink was released before the bounded-return assertion")
			default:
			}
			if targetAttemptFailureCleanupEligible(bounded.result) {
				t.Fatal("bounded ambiguous pending-wait result became eligible for finalization/reuse")
			}
			pending := observation.snapshot()
			if !pending.runnerReturned || pending.result != bounded.result {
				t.Fatalf("bounded disposition was not recorded exactly as returned: %+v", pending)
			}
			if pending.childWaitCompleted || pending.terminal || pending.waitErr != nil {
				t.Fatalf("bounded return fabricated terminal wait facts while the sole Wait was pending: %+v", pending)
			}
			if pending.cmd != startedSnapshot.cmd || pending.process != startedSnapshot.process ||
				pending.pid != startedSnapshot.pid || pending.startID != startedSnapshot.startID {
				t.Fatalf("bounded return no longer identifies the exact private child: %+v", pending)
			}

			writer.unlock()
			completed := awaitDelayedWaitSnapshot(t, observation, func(s targetProcessSnapshot) bool {
				return s.childWaitCompleted
			}, 5*time.Second)
			if !completed.terminal || !completed.runnerReturned {
				t.Fatalf("actual wait completion was not recorded as terminal: %+v", completed)
			}
			if completed.result != bounded.result || completed.result.Outcome != PGCommandAmbiguous {
				t.Fatalf("late actual Wait completion rewrote the bounded ambiguous disposition: %+v", completed.result)
			}
			if targetAttemptFailureCleanupEligible(completed.result) {
				t.Fatalf("late actual Wait completion turned the ambiguous attempt reusable: %+v", completed.result)
			}
			if completed.cmd != startedSnapshot.cmd || completed.process != startedSnapshot.process ||
				completed.pid != startedSnapshot.pid || completed.startID != startedSnapshot.startID {
				t.Fatalf("late completion no longer identifies the exact same private child: %+v", completed)
			}
			if completed.waitExitCode != processExitCode(completed.waitErr) {
				t.Fatalf("recorded exit code %d does not match the actual Wait error %v", completed.waitExitCode, completed.waitErr)
			}
			if testCase.exitedSuccessfully {
				if completed.waitErr != nil || completed.waitExitCode != 0 {
					t.Fatalf("successful child wait facts were not retained: err=%v exit=%d", completed.waitErr, completed.waitExitCode)
				}
			} else {
				var exitErr *exec.ExitError
				if !errors.As(completed.waitErr, &exitErr) {
					t.Fatalf("killed child wait error is not an exit error: %v", completed.waitErr)
				}
				waitStatus, ok := exitErr.ProcessState.Sys().(syscall.WaitStatus)
				if !ok || waitStatus.Signal() != syscall.SIGKILL || completed.waitExitCode != -1 {
					t.Fatalf("killed child wait facts are not the actual SIGKILL reaping: status=%v exit=%d", waitStatus, completed.waitExitCode)
				}
			}
			if !awaitDelayedWaitChildGone(pid, startID, 2*time.Second) {
				killExactDelayedWaitChild(pid, startID)
				t.Fatal("actual wait completion did not leave the exact child reaped")
			}

			// The completion fact must be recorded exactly once and never be
			// rewritten afterwards.
			time.Sleep(50 * time.Millisecond)
			stable := observation.snapshot()
			if !stable.childWaitCompleted || !stable.terminal || stable.result != completed.result ||
				stable.waitExitCode != completed.waitExitCode || stable.cmd != completed.cmd || stable.process != completed.process {
				t.Fatalf("actual wait completion was not recorded exactly once: %+v", stable)
			}

			// The sole pinned owner (including its OS-thread release path)
			// finished exactly once; no goroutine or ownership leak remains.
			awaitDelayedWaitGoroutineDrain(t, baseline, 3*time.Second)
		})
	}
}

func TestTargetProcessObservationSeparatesBoundedReturnFromActualWait(t *testing.T) {
	observation := &targetProcessObservation{}
	bounded := PGCommandResult{Outcome: PGCommandAmbiguous, Started: true, ExitCode: -1}
	observation.recordRunnerReturned(bounded)
	pending := observation.snapshot()
	if !pending.runnerReturned || pending.result != bounded {
		t.Fatalf("bounded runner disposition was not recorded: %+v", pending)
	}
	if pending.childWaitCompleted || pending.terminal {
		t.Fatal("bounded runner return claimed a terminal wait that had not completed")
	}
	observation.recordChildWaitCompleted(nil)
	completed := observation.snapshot()
	if !completed.childWaitCompleted || !completed.terminal || !completed.runnerReturned {
		t.Fatalf("actual wait completion was not recorded as the terminal fact: %+v", completed)
	}
	if completed.result != bounded || completed.result.Outcome != PGCommandAmbiguous {
		t.Fatalf("late authentic wait completion rewrote the bounded disposition: %+v", completed.result)
	}
	if completed.waitErr != nil || completed.waitExitCode != 0 {
		t.Fatalf("authentic wait facts were not retained: err=%v exit=%d", completed.waitErr, completed.waitExitCode)
	}
}

func TestTargetProcessObservationRecordsStartIdentityFailureFailClosed(t *testing.T) {
	observation := &targetProcessObservation{}
	// 1<<30 exceeds the Linux PID ceiling, so the /proc start-identity read
	// fails and must be retained as an error rather than defaulted identity.
	cmd := &exec.Cmd{Process: &os.Process{Pid: 1 << 30}}
	observation.recordStart(cmd)
	snapshot := observation.snapshot()
	if !snapshot.started || snapshot.cmd != cmd || snapshot.process != cmd.Process || snapshot.pid != cmd.Process.Pid {
		t.Fatalf("start observation does not retain the exact command/process pointer: %+v", snapshot)
	}
	if snapshot.startID != 0 || snapshot.startErr == nil {
		t.Fatalf("unreadable Linux start identity must fail closed, never default: id=%d err=%v", snapshot.startID, snapshot.startErr)
	}
	if snapshot.runnerReturned || snapshot.childWaitCompleted || snapshot.terminal {
		t.Fatalf("start observation fabricated return/wait facts: %+v", snapshot)
	}
}
