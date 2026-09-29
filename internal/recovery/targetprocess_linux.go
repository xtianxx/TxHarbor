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
	if err := cmd.Start(); err != nil {
		return result, errors.New("start supervised target child failed")
	}
	result.Started = true
	pid := cmd.Process.Pid
	waitCh := make(chan error, 1)
	go func() { waitCh <- cmd.Wait() }()
	ticker := time.NewTicker(healthInterval)
	defer ticker.Stop()
	for {
		select {
		case childErr := <-waitCh:
			result.ExitCode = processExitCode(childErr)
			if err := waitProcessGroupGone(pid, limit, poll); err != nil {
				_ = syscall.Kill(-pid, syscall.SIGKILL)
				if drainErr := waitProcessGroupGone(pid, limit, poll); drainErr != nil {
					result.Outcome = PGCommandAmbiguous
					return result, errors.New("target process group drain could not be proven")
				}
				result.ProcessGroupDrained = true
				result.Outcome = PGCommandAmbiguous
				return result, errors.New("target child exited with remaining process-group members")
			}
			result.ProcessGroupDrained = true
			if childErr != nil {
				result.Outcome = PGCommandFailed
				return result, errors.New("target child failed")
			}
			result.Outcome = PGCommandSucceeded
			return result, nil
		case <-ctx.Done():
			return r.stopProcessGroup(ctx, pid, waitCh, result, PGCommandCanceled, limit, poll, ctx.Err())
		case <-ticker.C:
			if lockHealth == nil {
				continue
			}
			if err := runBoundedLockHealth(ctx, lockHealth, healthTimeout); err != nil {
				if ctx.Err() != nil {
					return r.stopProcessGroup(ctx, pid, waitCh, result, PGCommandCanceled, limit, poll, ctx.Err())
				}
				return r.stopProcessGroup(ctx, pid, waitCh, result, PGCommandLockLost, limit, poll,
					errors.New("target lock health could not be proven"))
			}
		}
	}
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
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- check(ctx) }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
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
