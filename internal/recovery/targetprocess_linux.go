//go:build linux

package recovery

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// TargetProcessRunner supervises a direct local child in its own process
// group. It proves group disappearance (within DrainTimeout) in addition to
// reaping the direct child. It does not prove that the target database is
// quiescent; callers must separately run CheckTargetQuiescent.
type TargetProcessRunner struct {
	DrainTimeout        time.Duration
	PollInterval        time.Duration
	HealthCheckInterval time.Duration
	HealthCheckTimeout  time.Duration
	observation         *targetProcessObservation
}

// targetProcessObservation is an internal, non-callback observation channel.
// It retains the exact Cmd/Process owned by this runner and records, under a
// short mutex, three separate facts: the observed start identity, the bounded
// disposition the runner returned to its caller, and the actual completion of
// the sole cmd.Wait. It can neither Wait nor influence child cancellation. It
// is intentionally not a public proving interface.
type targetProcessObservation struct {
	mu                 sync.Mutex
	started            bool
	cmd                *exec.Cmd
	process            *os.Process
	pid                int
	startID            uint64
	startErr           error
	runnerReturned     bool
	result             PGCommandResult
	childWaitCompleted bool
	waitErr            error
	waitExitCode       int
	terminal           bool
}

type targetProcessSnapshot struct {
	started            bool
	cmd                *exec.Cmd
	process            *os.Process
	pid                int
	startID            uint64
	startErr           error
	runnerReturned     bool
	result             PGCommandResult
	childWaitCompleted bool
	waitErr            error
	waitExitCode       int
	terminal           bool
}

func (o *targetProcessObservation) recordStart(cmd *exec.Cmd) {
	if o == nil || cmd == nil || cmd.Process == nil {
		return
	}
	startID, err := linuxProcessStartIdentity(cmd.Process.Pid)
	o.mu.Lock()
	o.started, o.cmd, o.process, o.pid, o.startID, o.startErr = true, cmd, cmd.Process, cmd.Process.Pid, startID, err
	o.mu.Unlock()
}

// recordRunnerReturned records the bounded disposition this runner returned to
// its caller. A disposition is not proof that the child was reaped: on a
// deadline bound the runner may return PGCommandAmbiguous while the sole wait
// is still pending.
func (o *targetProcessObservation) recordRunnerReturned(result PGCommandResult) {
	if o == nil {
		return
	}
	o.mu.Lock()
	o.runnerReturned, o.result = true, result
	o.mu.Unlock()
}

// recordChildWaitCompleted records the only authentic terminal fact: the sole
// owner observed cmd.Wait return for the exact child. It is written by that
// owner after the wait returns and never by a bounded runner return.
func (o *targetProcessObservation) recordChildWaitCompleted(err error) {
	if o == nil {
		return
	}
	o.mu.Lock()
	o.childWaitCompleted, o.waitErr, o.waitExitCode, o.terminal = true, err, processExitCode(err), true
	o.mu.Unlock()
}

func (o *targetProcessObservation) snapshot() targetProcessSnapshot {
	if o == nil {
		return targetProcessSnapshot{}
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	return targetProcessSnapshot{
		started:            o.started,
		cmd:                o.cmd,
		process:            o.process,
		pid:                o.pid,
		startID:            o.startID,
		startErr:           o.startErr,
		runnerReturned:     o.runnerReturned,
		result:             o.result,
		childWaitCompleted: o.childWaitCompleted,
		waitErr:            o.waitErr,
		waitExitCode:       o.waitExitCode,
		terminal:           o.terminal,
	}
}

func linuxProcessStartIdentity(pid int) (uint64, error) {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, err
	}
	// comm is parenthesized and may itself contain spaces or ')'; fields after
	// its final ')' begin with field 3 (state), making starttime field 22 index 19.
	end := strings.LastIndexByte(string(b), ')')
	if end < 0 {
		return 0, errors.New("malformed proc stat")
	}
	fields := strings.Fields(string(b[end+1:]))
	if len(fields) <= 19 {
		return 0, errors.New("truncated proc stat")
	}
	return strconv.ParseUint(fields[19], 10, 64)
}

type targetProcessReceipt struct {
	mu              sync.Mutex
	consumed        bool
	snapshot        targetProcessSnapshot
	targetKey       TargetKey
	roleFingerprint string
	operationID     string
}

func bindTargetProcessReceipt(snapshot targetProcessSnapshot, key TargetKey, roleFingerprint, operationID string) *targetProcessReceipt {
	return &targetProcessReceipt{snapshot: snapshot, targetKey: key, roleFingerprint: roleFingerprint, operationID: operationID}
}

// PGCommandOutcome is deliberately about process supervision, not target
// cleanliness. Only a successful process result and a drained process group
// are represented by PGCommandSucceeded; callers still need target-side
// quiescence, probes, and durable guard/evidence transitions.
type PGCommandOutcome string

const (
	PGCommandNotStarted PGCommandOutcome = "not_started"
	PGCommandSucceeded  PGCommandOutcome = "process_succeeded_group_drained"
	PGCommandFailed     PGCommandOutcome = "process_failed_group_drained"
	PGCommandCanceled   PGCommandOutcome = "canceled_group_drained"
	PGCommandLockLost   PGCommandOutcome = "lock_lost_group_drained"
	PGCommandAmbiguous  PGCommandOutcome = "ambiguous"
)

// PGCommandResult records only local child/process-group facts. It never
// states that the PostgreSQL target is clean or safe for re-entry.
type PGCommandResult struct {
	Outcome             PGCommandOutcome
	Started             bool
	ProcessGroupDrained bool
	ExitCode            int
}

// Run starts executable directly (no shell) in a new process group. On
// cancellation it SIGKILLs the entire group, waits for the direct child, and
// requires the group to disappear. It intentionally does not use
// exec.CommandContext, whose cancellation only targets the direct process.
func (r TargetProcessRunner) Run(ctx context.Context, executable string, args ...string) error {
	_, err := r.runSupervised(ctx, executable, args, nil, nil, nil, nil, nil, false)
	return err
}

// RunPGCommand supervises a directly invoked local command (normally
// pg_restore) in a dedicated process group while streaming stdin/stdout/stderr.
// A lock-health callback is mandatory and is checked before launch and
// periodically while the child runs. Cancellation or any failed/unknown lock
// probe kills the whole process group and waits for the child/group drain.
// Error text is intentionally generic and never includes executable, argv,
// conninfo, or callback error details.
func (r TargetProcessRunner) RunPGCommand(
	ctx context.Context,
	name string,
	args []string,
	stdin io.Reader,
	stdout, stderr io.Writer,
	lockHealth func(context.Context) error,
) (PGCommandResult, error) {
	protectPG := filepath.Base(name) == "pg_restore"
	var environ []string
	if protectPG {
		environ = os.Environ()
	}
	return r.runSupervised(ctx, name, args, stdin, stdout, stderr, environ, lockHealth, protectPG)
}

// RunPGCommandWithEnv supervises pg_restore using the supplied parent
// environment as input to the mandatory PostgreSQL argument/environment
// protection helper. The helper validates and filters ambient PG* settings,
// rewrites --dbname to a credential-safe form, and adds a private passfile if
// needed before the process starts.
func (r TargetProcessRunner) RunPGCommandWithEnv(
	ctx context.Context,
	name string,
	args []string,
	stdin io.Reader,
	stdout, stderr io.Writer,
	env []string,
	lockHealth func(context.Context) error,
) (PGCommandResult, error) {
	if lockHealth == nil {
		return PGCommandResult{Outcome: PGCommandNotStarted, ExitCode: -1}, errors.New("target lock health callback is required")
	}
	return r.runSupervised(ctx, name, args, stdin, stdout, stderr, env, lockHealth, true)
}

func (r TargetProcessRunner) runSupervised(ctx context.Context, executable string, args []string,
	stdin io.Reader, stdout, stderr io.Writer, env []string, lockHealth func(context.Context) error, protectPG bool) (PGCommandResult, error) {
	result := PGCommandResult{Outcome: PGCommandNotStarted, ExitCode: -1}
	if ctx == nil || executable == "" {
		return result, errors.New("context and executable are required")
	}
	limit := r.DrainTimeout
	if limit <= 0 {
		limit = 5 * time.Second
	}
	poll := r.PollInterval
	if poll <= 0 {
		poll = 20 * time.Millisecond
	}
	healthInterval := r.HealthCheckInterval
	if healthInterval <= 0 {
		healthInterval = 250 * time.Millisecond
	}
	healthTimeout := r.HealthCheckTimeout
	if healthTimeout <= 0 {
		healthTimeout = time.Second
	}
	if lockHealth != nil {
		if err := runBoundedLockHealth(ctx, lockHealth, healthTimeout); err != nil {
			if ctx.Err() != nil {
				result.Outcome = PGCommandCanceled
				return result, ctx.Err()
			}
			return result, errors.New("target lock health could not be proven before child launch")
		}
	}
	if err := ctx.Err(); err != nil {
		result.Outcome = PGCommandCanceled
		return result, err
	}
	cmd := exec.Command(executable, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cleanup := func() {}
	if protectPG {
		var err error
		cleanup, err = protectPGChildArgsWithEnvironment(cmd, "pg_restore", args, env)
		if err != nil {
			return result, errors.New("prepare protected PostgreSQL child arguments failed")
		}
	} else if env != nil {
		cmd.Env = append([]string(nil), env...)
	}
	defer cleanup()
	cmd.Stdin, cmd.Stdout, cmd.Stderr = stdin, stdout, stderr

	// Launch and the sole cmd.Wait belong to one owner goroutine pinned to its
	// OS thread. Linux delivers Pdeathsig when the thread that forked the child
	// exits, so lifecycle ownership must outlive the actual Wait: the bounded
	// runner below may return deadline ambiguity while that wait is still
	// pending, and the same pinned owner records the authentic reaped facts
	// when Wait returns. The caller never issues a second Wait.
	startCh := make(chan error, 1)
	waitCh := make(chan error, 1)
	go func() {
		releaseLifecycleThread := lockPGChildParentDeath(cmd)
		if err := cmd.Start(); err != nil {
			startCh <- err
			releaseLifecycleThread()
			return
		}
		// The start identity is captured here, on the same pinned owner and
		// before the sole Wait can reap the child.
		r.observation.recordStart(cmd)
		startCh <- nil
		waitErr := cmd.Wait()
		r.observation.recordChildWaitCompleted(waitErr)
		waitCh <- waitErr
		releaseLifecycleThread()
	}()
	if err := <-startCh; err != nil {
		return result, errors.New("start supervised target child failed")
	}
	result.Started = true
	pid := cmd.Process.Pid
	ticker := time.NewTicker(healthInterval)
	defer ticker.Stop()
	for {
		select {
		case childErr := <-waitCh:
			// The pinned owner already recorded the actual wait completion for
			// this event; only process-group drain facts remain to be proven.
			result.ExitCode = processExitCode(childErr)
			if err := waitProcessGroupGone(pid, limit, poll); err != nil {
				_ = syscall.Kill(-pid, syscall.SIGKILL)
				if drainErr := waitProcessGroupGone(pid, limit, poll); drainErr != nil {
					result.Outcome = PGCommandAmbiguous
					return r.recordRunnerReturned(result, errors.New("target process group drain could not be proven"))
				}
				result.ProcessGroupDrained = true
				result.Outcome = PGCommandAmbiguous
				return r.recordRunnerReturned(result, errors.New("target child exited with remaining process-group members"))
			}
			result.ProcessGroupDrained = true
			if childErr != nil {
				result.Outcome = PGCommandFailed
				return r.recordRunnerReturned(result, errors.New("target child failed"))
			}
			result.Outcome = PGCommandSucceeded
			return r.recordRunnerReturned(result, nil)
		case <-ctx.Done():
			stopped, stopErr := r.stopProcessGroup(ctx, pid, waitCh, result, PGCommandCanceled, limit, poll, ctx.Err())
			return r.recordRunnerReturned(stopped, stopErr)
		case <-ticker.C:
			if lockHealth == nil {
				continue
			}
			if err := runBoundedLockHealth(ctx, lockHealth, healthTimeout); err != nil {
				if ctx.Err() != nil {
					stopped, stopErr := r.stopProcessGroup(ctx, pid, waitCh, result, PGCommandCanceled, limit, poll, ctx.Err())
					return r.recordRunnerReturned(stopped, stopErr)
				}
				stopped, stopErr := r.stopProcessGroup(ctx, pid, waitCh, result, PGCommandLockLost, limit, poll,
					errors.New("target lock health could not be proven"))
				return r.recordRunnerReturned(stopped, stopErr)
			}
		}
	}
}

// recordRunnerReturned records the bounded disposition handed to the caller.
// It deliberately does not claim that the child was waited: that fact is
// recorded only by the pinned owner once the sole cmd.Wait returns, so a
// deadline-bound ambiguity can never fabricate a terminal receipt.
func (r TargetProcessRunner) recordRunnerReturned(result PGCommandResult, err error) (PGCommandResult, error) {
	r.observation.recordRunnerReturned(result)
	return result, err
}

func (r TargetProcessRunner) stopProcessGroup(ctx context.Context, pgid int, waitCh <-chan error,
	result PGCommandResult, outcome PGCommandOutcome, timeout, poll time.Duration, cause error) (PGCommandResult, error) {
	_ = syscall.Kill(-pgid, syscall.SIGKILL)
	select {
	case childErr := <-waitCh:
		result.ExitCode = processExitCode(childErr)
	case <-time.After(timeout):
		result.Outcome = PGCommandAmbiguous
		return result, errors.New("terminated target child did not reap before deadline")
	}
	if err := waitProcessGroupGone(pgid, timeout, poll); err != nil {
		result.Outcome = PGCommandAmbiguous
		return result, errors.New("terminated target process group drain could not be proven")
	}
	result.ProcessGroupDrained = true
	result.Outcome = outcome
	if outcome == PGCommandCanceled {
		if ctx != nil && ctx.Err() != nil {
			return result, ctx.Err()
		}
		return result, context.Canceled
	}
	return result, cause
}

func runBoundedLockHealth(parent context.Context, check func(context.Context) error, timeout time.Duration) error {
	if parent == nil || check == nil {
		return errors.New("target lock health context and check are required")
	}
	if timeout <= 0 {
		timeout = time.Second
	}
	// Child cancellation must not cancel the control-session query: pgx treats a
	// canceled query context as a connection failure and closes the borrowed
	// advisory-lock owner. The independent deadline keeps the query bounded.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), timeout)
	defer cancel()
	// Run synchronously so no health goroutine can retain the lock mutex or
	// continue querying after this result is used to stop/reap the writer.
	return check(ctx)
}

func processExitCode(err error) int {
	if err == nil {
		return 0
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode()
	}
	return -1
}

func waitProcessGroupGone(pgid int, timeout, interval time.Duration) error {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		err := syscall.Kill(-pgid, 0)
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}
		if err != nil && !errors.Is(err, syscall.EPERM) {
			return fmt.Errorf("probe process group %d: %w", pgid, err)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("process group %d remains", pgid)
		}
		<-ticker.C
	}
}
