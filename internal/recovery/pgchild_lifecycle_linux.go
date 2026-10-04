//go:build linux

package recovery

import (
	"os/exec"
	"runtime"
	"syscall"
)

// lockPGChildParentDeath binds the child launch to the lifetime of this
// goroutine's OS thread. Linux delivers Pdeathsig when the thread that forked
// the child exits, not merely when the containing process exits. Callers must
// hold the returned release function at least until the sole cmd.Wait for the
// direct child has returned; the pinned launch thread must not be released
// while that wait is still pending. Pdeathsig covers only the direct child, so
// an owned process group is drained by explicit group probing rather than by
// retaining the launch thread past the wait.
// Pdeathsig applies only to this direct child; it does not promise cleanup of
// arbitrary descendants once the supervising process is itself killed.
func lockPGChildParentDeath(cmd *exec.Cmd) func() {
	runtime.LockOSThread()
	attr := new(syscall.SysProcAttr)
	if cmd.SysProcAttr != nil {
		*attr = *cmd.SysProcAttr
	}
	attr.Pdeathsig = syscall.SIGKILL
	cmd.SysProcAttr = attr
	return runtime.UnlockOSThread
}
