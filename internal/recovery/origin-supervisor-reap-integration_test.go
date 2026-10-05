//go:build linux && drill

// origin-supervisor-reap-integration_test.go is the OG05 regression guard for
// the INTEGRATED reaping verdict of the armed native positive
// (supervisorChildReapError). It exercises the actual helper, not just the
// classifier, through the existing injected originSupervisorReadStat seam: a
// read error can never collapse into "reaped" again.
package recovery_test

import (
	"errors"
	"fmt"
	"io/fs"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestOriginSupervisorIntegratedReapVerdictStrict(t *testing.T) {
	const (
		pid     = 5150
		startID = 271828183
	)
	const shortWait = 60 * time.Millisecond

	cases := []struct {
		name       string
		build      func() ([]byte, error)
		wantNil    bool
		wantReason string
	}{
		{"enoent is authoritative reaping", func() ([]byte, error) {
			return nil, &fs.PathError{Op: "open", Path: fmt.Sprintf("/proc/%d/stat", pid), Err: fs.ErrNotExist}
		}, true, ""},
		{"different start identity is authoritative reaping", func() ([]byte, error) {
			return originSupervisorStatLine(pid, "pg_restore", "S", 1, startID+1), nil
		}, true, ""},
		{"eacces must not prove reaping", func() ([]byte, error) {
			return nil, &fs.PathError{Op: "open", Path: fmt.Sprintf("/proc/%d/stat", pid), Err: fs.ErrPermission}
		}, false, "inspection refused"},
		{"io error must not prove reaping", func() ([]byte, error) {
			return nil, syscall.EIO
		}, false, "inspection refused"},
		{"malformed stat must not prove reaping", func() ([]byte, error) {
			return []byte("garbage"), nil
		}, false, "inspection refused"},
		{"partial stat must not prove reaping", func() ([]byte, error) {
			return []byte(fmt.Sprintf("%d (pg_restore) S 1", pid)), nil
		}, false, "inspection refused"},
		{"zombie same identity stays not gone", func() ([]byte, error) {
			return originSupervisorStatLine(pid, "pg_restore", "Z", 1, startID), nil
		}, false, "still observable"},
		{"live same identity stays not gone", func() ([]byte, error) {
			return originSupervisorStatLine(pid, "pg_restore", "S", 1, startID), nil
		}, false, "still observable"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stubOriginSupervisorReadStat(t, func(string) ([]byte, error) { return tc.build() })
			err := supervisorChildReapError(pid, startID, shortWait)
			if tc.wantNil {
				if err != nil {
					t.Fatalf("integrated verdict = %v, want authoritative reaping", err)
				}
				return
			}
			if err == nil {
				t.Fatal("non-authoritative state passed the integrated reaping verdict")
			}
			if !strings.Contains(err.Error(), tc.wantReason) {
				t.Fatalf("integrated verdict %q does not report %q", err, tc.wantReason)
			}
		})
	}
}

// TestOriginSupervisorIntegratedReapRealWaitPendingNotGone proves the verdict
// against a real process whose sole Wait is still pending: the same start
// identity stays not-gone, and only after an identity-verified kill and real
// reaping does the verdict pass. An unverifiable identity is never killed.
func TestOriginSupervisorIntegratedReapRealWaitPendingNotGone(t *testing.T) {
	cmd := exec.Command("/bin/sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start real wait-pending child: %v", err)
	}
	pid := cmd.Process.Pid
	waited := false
	defer func() {
		if waited {
			return
		}
		// Cleanup kills only after re-observing the exact start identity as
		// alive; an unknown identity is never killed.
		if start, err := supervisorRealStartIdentity(pid); err == nil {
			if presence, err := originSupervisorPresenceOf(pid, start); err == nil && presence.String() == "alive" {
				_ = syscall.Kill(pid, syscall.SIGKILL)
			}
		}
		_ = cmd.Wait()
	}()

	start, err := supervisorRealStartIdentity(pid)
	if err != nil {
		t.Fatalf("read real child start identity: %v", err)
	}
	if err := supervisorChildReapError(pid, start, 120*time.Millisecond); err == nil {
		t.Fatal("integrated verdict claimed reaping while the real sole Wait is still pending")
	} else {
		t.Logf("causal: real_wait_pending_verdict=%q", err)
	}
	if presence, err := originSupervisorPresenceOf(pid, start); err != nil || presence.String() != "alive" {
		t.Fatalf("real child identity is not verifiably alive before cleanup: presence=%v err=%v", presence, err)
	}
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
		t.Fatalf("identity-verified kill of the real child: %v", err)
	}
	waitErr := cmd.Wait()
	waited = true
	var exitErr *exec.ExitError
	if !errors.As(waitErr, &exitErr) {
		t.Fatalf("real child wait did not report the expected SIGKILL exit: %v", waitErr)
	}
	if status, ok := exitErr.ProcessState.Sys().(syscall.WaitStatus); !ok || status.Signal() != syscall.SIGKILL {
		t.Fatalf("real child wait status was not SIGKILL: %v", waitErr)
	}
	if presence, err := awaitOriginSupervisorGone(pid, start, 3*time.Second); err != nil || presence.String() != "gone" {
		t.Fatalf("real child did not reach authoritative gone: presence=%v err=%v", presence, err)
	}
	if err := supervisorChildReapError(pid, start, 120*time.Millisecond); err != nil {
		t.Fatalf("integrated verdict refused a genuinely reaped child: %v", err)
	}
}

// supervisorRealStartIdentity reads the real /proc start identity through the
// same injected seam used by the classifier.
func supervisorRealStartIdentity(pid int) (uint64, error) {
	data, err := originSupervisorReadStat(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, err
	}
	stat, err := parseOriginSupervisorProcStat(data)
	if err != nil {
		return 0, err
	}
	return stat.start, nil
}
