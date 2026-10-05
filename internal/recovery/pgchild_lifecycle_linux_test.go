//go:build linux

package recovery

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

const pgChildLifecycleHelperEnv = "TXHARBOR_TEST_PGCHILD_PARENT_DEATH"

// This test entrypoint is run in a short-lived subprocess by
// TestPGChildParentDeathKillsAndReapsOwnedDirectChild.
func TestPGChildLifecycleParentDeathHelper(t *testing.T) {
	if os.Getenv(pgChildLifecycleHelperEnv) != "1" {
		return
	}
	cmd := exec.Command("/bin/sleep", "30")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	release := lockPGChildParentDeath(cmd)
	defer release()
	if !cmd.SysProcAttr.Setpgid || cmd.SysProcAttr.Pdeathsig != syscall.SIGKILL {
		t.Fatal("parent-death setup did not preserve the existing process-group attribute")
	}
	if err := cmd.Start(); err != nil {
		fmt.Fprintln(os.Stdout, "start-failed")
		os.Exit(2)
	}
	fmt.Fprintln(os.Stdout, cmd.Process.Pid)
	_ = cmd.Wait()
}

func TestPGChildParentDeathKillsAndReapsOwnedDirectChild(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal("locate test executable")
	}
	parent := exec.Command(executable, "-test.run=^TestPGChildLifecycleParentDeathHelper$")
	parent.Env = append(os.Environ(), pgChildLifecycleHelperEnv+"=1")
	stdout, err := parent.StdoutPipe()
	if err != nil {
		t.Fatal("open helper stdout")
	}
	if err := parent.Start(); err != nil {
		t.Fatalf("start lifecycle helper: %v", err)
	}
	line, readErr := bufio.NewReader(stdout).ReadString('\n')
	_ = stdout.Close()
	if readErr != nil {
		_ = parent.Process.Kill()
		_ = parent.Wait()
		t.Fatal("lifecycle helper did not report its direct child")
	}
	childPID, err := strconv.Atoi(strings.TrimSpace(line))
	if err != nil || childPID <= 0 {
		_ = parent.Process.Kill()
		_ = parent.Wait()
		t.Fatal("lifecycle helper returned an invalid child identity")
	}
	childStart, err := linuxProcessStartIdentity(childPID)
	if err != nil {
		_ = parent.Process.Kill()
		_ = parent.Wait()
		t.Fatal("cannot establish direct-child start identity")
	}
	if err := parent.Process.Kill(); err != nil {
		// Cleanup kill only after re-observing the exact start identity; an
		// unreadable or reused PID is never killed.
		if verified, _ := linuxProcessPresenceOf(childPID, childStart); verified == linuxProcessPresenceAlive {
			_ = syscall.Kill(childPID, syscall.SIGKILL)
		}
		_ = parent.Wait()
		t.Fatalf("kill lifecycle helper: %v", err)
	}
	waitErr := parent.Wait()
	assertPGLifecycleParentWasKilled(t, waitErr)
	presence, inspectErr := awaitLinuxProcessGone(childPID, childStart, 3*time.Second)
	if presence == linuxProcessPresenceGone {
		return
	}
	// Preserve the failed observation, then clean up so the test never leaks a
	// sleeping process into the runner. A kill is issued only after the exact
	// start identity was re-observed as alive; an unreadable /proc is never
	// death evidence and never authorizes killing a possibly reused PID.
	ppid, state := linuxChildObservation(childPID, childStart)
	cleaned := false
	if verified, _ := linuxProcessPresenceOf(childPID, childStart); verified == linuxProcessPresenceAlive {
		_ = syscall.Kill(childPID, syscall.SIGKILL)
		cleanupPresence, _ := awaitLinuxProcessGone(childPID, childStart, 3*time.Second)
		cleaned = cleanupPresence == linuxProcessPresenceGone
	}
	if presence == linuxProcessPresenceUnknown {
		t.Fatalf("parent death outcome is unproven: process inspection refused (pid=%d start=%d): %v (ppid=%d state=%s cleanup_drained=%t)", childPID, childStart, inspectErr, ppid, state, cleaned)
	}
	t.Fatalf("parent death did not terminate direct child (pid=%d start=%d ppid=%d state=%s cleanup_drained=%t)", childPID, childStart, ppid, state, cleaned)
}

func assertPGLifecycleParentWasKilled(t *testing.T, err error) {
	t.Helper()
	exitErr, ok := err.(*exec.ExitError)
	if !ok {
		t.Fatalf("parent Wait did not report the expected SIGKILL exit: %v", err)
	}
	waitStatus, ok := exitErr.ProcessState.Sys().(syscall.WaitStatus)
	if !ok || waitStatus.Signal() != syscall.SIGKILL {
		t.Fatal("parent Wait status was not SIGKILL")
	}
}

func TestLocalPGCommandContextCancellationWaitsForChild(t *testing.T) {
	pidFile := filepath.Join(t.TempDir(), "child.pid")
	child := writePGChildScript(t, "#!/bin/sh\nprintf '%s' \"$$\" > "+pidFile+"\nexec /bin/sleep 30\n")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- (LocalPGCommand{}).Run(ctx, child,
			[]string{"--dbname=host=localhost port=5432 dbname=postgres user=postgres password=cancel-test"},
			nil, &bytes.Buffer{}, &bytes.Buffer{})
	}()
	deadline := time.Now().Add(3 * time.Second)
	var pid int
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(pidFile)
		if err == nil {
			pid, _ = strconv.Atoi(strings.TrimSpace(string(data)))
			if pid > 0 {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if pid == 0 {
		t.Fatal("LocalPGCommand did not start its child")
	}
	start, err := linuxProcessStartIdentity(pid)
	if err != nil {
		t.Fatal("cannot observe LocalPGCommand child identity")
	}
	cancel()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("canceled LocalPGCommand unexpectedly succeeded")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("LocalPGCommand did not return after context cancellation")
	}
	presence, inspectErr := awaitLinuxProcessGone(pid, start, 3*time.Second)
	switch presence {
	case linuxProcessPresenceGone:
		return
	case linuxProcessPresenceAlive:
		// The identity was observed alive immediately before this cleanup
		// kill; a timeout alone is never drain evidence.
		_ = syscall.Kill(pid, syscall.SIGKILL)
		t.Fatal("LocalPGCommand returned before its canceled direct child was reaped")
	default:
		t.Fatalf("canceled direct child outcome is unproven: process inspection refused (pid=%d start=%d): %v", pid, start, inspectErr)
	}
}

// ---------------------------------------------------------------------------
// Process identity inspection (narrow test helpers).
//
// Disappearance evidence is asymmetric on purpose: only an authoritative
// ENOENT or a successfully read, different start identity proves that the
// observed process is gone. EACCES, IO errors, transient failures, malformed
// stat data and every other unreadable state are inspection_unknown and must
// be refused as evidence. A zombie keeps the same start identity observable
// until it is reaped, so it classifies as alive here, never gone.
// ---------------------------------------------------------------------------

// linuxProcessStatRead is the narrow injection seam used by the deterministic
// unit tests below. Its default is the real /proc reader; the native canary
// test above still observes real kernel state.
var linuxProcessStatRead = os.ReadFile

type linuxProcessPresence int

const (
	// linuxProcessPresenceUnknown: the identity could not be inspected. The
	// caller must refuse this as evidence, never read it as gone.
	linuxProcessPresenceUnknown linuxProcessPresence = iota
	// linuxProcessPresenceAlive: the same start identity is observable,
	// including zombie state Z (an unreaped process).
	linuxProcessPresenceAlive
	// linuxProcessPresenceGone: authoritative disappearance: ENOENT or a
	// different start identity (this PID no longer names that process).
	linuxProcessPresenceGone
)

func (p linuxProcessPresence) String() string {
	switch p {
	case linuxProcessPresenceAlive:
		return "alive"
	case linuxProcessPresenceGone:
		return "gone"
	default:
		return "inspection_unknown"
	}
}

type linuxProcessStat struct {
	state string
	ppid  int
	start uint64
}

// parseLinuxProcessStat parses the fields needed from /proc/<pid>/stat. comm
// is parenthesized and may itself contain spaces or ')'; fields after its
// final ')' begin at field 3, so starttime (field 22) is index 19.
func parseLinuxProcessStat(data []byte) (linuxProcessStat, error) {
	closeIndex := strings.LastIndexByte(string(data), ')')
	if closeIndex < 0 {
		return linuxProcessStat{}, errors.New("malformed process stat: no command terminator")
	}
	fields := strings.Fields(string(data[closeIndex+1:]))
	if len(fields) <= 19 {
		return linuxProcessStat{}, errors.New("malformed process stat: fewer than 20 fields")
	}
	ppid, err := strconv.Atoi(fields[1])
	if err != nil {
		return linuxProcessStat{}, fmt.Errorf("malformed process stat ppid: %w", err)
	}
	start, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil {
		return linuxProcessStat{}, fmt.Errorf("malformed process stat starttime: %w", err)
	}
	return linuxProcessStat{state: fields[0], ppid: ppid, start: start}, nil
}

// linuxProcessPresenceOf classifies one /proc observation of pid against the
// expected start identity. Only ENOENT or a different successfully-read start
// identity classify as gone; anything unreadable is inspection_unknown.
func linuxProcessPresenceOf(pid int, start uint64) (linuxProcessPresence, error) {
	data, err := linuxProcessStatRead(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return linuxProcessPresenceGone, nil
		}
		return linuxProcessPresenceUnknown, err
	}
	stat, err := parseLinuxProcessStat(data)
	if err != nil {
		return linuxProcessPresenceUnknown, err
	}
	if stat.start != start {
		return linuxProcessPresenceGone, nil
	}
	return linuxProcessPresenceAlive, nil
}

// awaitLinuxProcessGone polls up to timeout and reports only authoritative
// states: gone on ENOENT/different start identity, alive while the same start
// identity stays observable, inspection_unknown (with the cause) when /proc
// cannot be read. Timeout expiry is never by itself drain evidence.
func awaitLinuxProcessGone(pid int, start uint64, timeout time.Duration) (linuxProcessPresence, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		presence, err := linuxProcessPresenceOf(pid, start)
		if presence == linuxProcessPresenceGone {
			return presence, nil
		}
		if err != nil {
			lastErr = err
		}
		if !time.Now().Before(deadline) {
			if presence == linuxProcessPresenceUnknown {
				return presence, lastErr
			}
			return presence, nil
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// waitForLinuxProcessGone is the boolean compatibility form used by the
// integration helper. An inconclusive inspection is false (not drained),
// never a success.
func waitForLinuxProcessGone(pid int, start uint64, timeout time.Duration) bool {
	presence, _ := awaitLinuxProcessGone(pid, start, timeout)
	return presence == linuxProcessPresenceGone
}

func linuxChildObservation(pid int, start uint64) (int, string) {
	data, err := linuxProcessStatRead(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return 0, "gone"
		}
		return 0, "inspection_unknown"
	}
	stat, err := parseLinuxProcessStat(data)
	if err != nil {
		return 0, "malformed_stat"
	}
	if stat.start != start {
		return 0, "reused"
	}
	return stat.ppid, stat.state
}

// ---------------------------------------------------------------------------
// Deterministic inspection-class unit tests (no real process involved).
// ---------------------------------------------------------------------------

func linuxUnitStatLine(pid int, comm, state string, ppid int, start uint64) []byte {
	fields := make([]string, 20)
	for i := range fields {
		fields[i] = "0"
	}
	fields[0] = state
	fields[1] = strconv.Itoa(ppid)
	fields[19] = strconv.FormatUint(start, 10)
	return []byte(fmt.Sprintf("%d (%s) %s\n", pid, comm, strings.Join(fields, " ")))
}

func stubLinuxProcessStatRead(t *testing.T, read func(string) ([]byte, error)) {
	t.Helper()
	previous := linuxProcessStatRead
	linuxProcessStatRead = read
	t.Cleanup(func() { linuxProcessStatRead = previous })
}

func TestLinuxProcessPresenceClassificationIsStrictAboutUnreadableState(t *testing.T) {
	const (
		pid   = 4242
		start = 987654321
	)
	cases := []struct {
		name    string
		build   func() ([]byte, error)
		want    linuxProcessPresence
		wantErr bool
	}{
		{"enoent is authoritative disappearance", func() ([]byte, error) {
			return nil, &fs.PathError{Op: "open", Path: fmt.Sprintf("/proc/%d/stat", pid), Err: fs.ErrNotExist}
		}, linuxProcessPresenceGone, false},
		{"eacces refuses evidence", func() ([]byte, error) {
			return nil, &fs.PathError{Op: "open", Path: fmt.Sprintf("/proc/%d/stat", pid), Err: fs.ErrPermission}
		}, linuxProcessPresenceUnknown, true},
		{"io error refuses evidence", func() ([]byte, error) {
			return nil, syscall.EIO
		}, linuxProcessPresenceUnknown, true},
		{"same start identity is alive", func() ([]byte, error) {
			return linuxUnitStatLine(pid, "sleep", "S", 1, start), nil
		}, linuxProcessPresenceAlive, false},
		{"zombie is not reaped and stays alive", func() ([]byte, error) {
			return linuxUnitStatLine(pid, "sleep", "Z", 1, start), nil
		}, linuxProcessPresenceAlive, false},
		{"different start identity is gone (pid reuse)", func() ([]byte, error) {
			return linuxUnitStatLine(pid, "sleep", "S", 1, start+1), nil
		}, linuxProcessPresenceGone, false},
		{"malformed stat refuses evidence", func() ([]byte, error) {
			return []byte(fmt.Sprintf("%d (sleep) S 1 2 3", pid)), nil
		}, linuxProcessPresenceUnknown, true},
		{"non-numeric starttime refuses evidence", func() ([]byte, error) {
			line := linuxUnitStatLine(pid, "sleep", "S", 1, start)
			return bytes.Replace(line, []byte(strconv.FormatUint(start, 10)), []byte("not-a-number"), 1), nil
		}, linuxProcessPresenceUnknown, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stubLinuxProcessStatRead(t, func(string) ([]byte, error) { return tc.build() })
			got, err := linuxProcessPresenceOf(pid, start)
			if got != tc.want {
				t.Fatalf("presence = %s, want %s", got, tc.want)
			}
			if tc.wantErr && err == nil {
				t.Fatal("unreadable state was classified as evidence without an error")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("authoritative state reported an unexpected error: %v", err)
			}
		})
	}
}

func TestAwaitLinuxProcessGoneDistinguishesUnknownFromGone(t *testing.T) {
	const (
		pid   = 5150
		start = 123456789
	)
	const shortWait = 80 * time.Millisecond

	t.Run("enoent is gone", func(t *testing.T) {
		stubLinuxProcessStatRead(t, func(string) ([]byte, error) { return nil, fs.ErrNotExist })
		presence, err := awaitLinuxProcessGone(pid, start, shortWait)
		if presence != linuxProcessPresenceGone || err != nil {
			t.Fatalf("presence=%s err=%v, want gone", presence, err)
		}
	})
	t.Run("unreadable state is refused, never inferred as drained", func(t *testing.T) {
		stubLinuxProcessStatRead(t, func(string) ([]byte, error) { return nil, fs.ErrPermission })
		presence, err := awaitLinuxProcessGone(pid, start, shortWait)
		if presence != linuxProcessPresenceUnknown || err == nil {
			t.Fatalf("presence=%s err=%v, want inspection_unknown with its cause", presence, err)
		}
	})
	t.Run("transient refusal recovers to a later authoritative read", func(t *testing.T) {
		calls := 0
		stubLinuxProcessStatRead(t, func(string) ([]byte, error) {
			calls++
			if calls < 3 {
				return nil, fs.ErrPermission
			}
			return nil, fs.ErrNotExist
		})
		presence, err := awaitLinuxProcessGone(pid, start, time.Second)
		if presence != linuxProcessPresenceGone || err != nil {
			t.Fatalf("presence=%s err=%v, want gone after a transient refusal", presence, err)
		}
	})
	t.Run("zombie same-start identity stays alive at timeout", func(t *testing.T) {
		stubLinuxProcessStatRead(t, func(string) ([]byte, error) {
			return linuxUnitStatLine(pid, "sleep", "Z", 1, start), nil
		})
		presence, err := awaitLinuxProcessGone(pid, start, shortWait)
		if presence != linuxProcessPresenceAlive || err != nil {
			t.Fatalf("presence=%s err=%v, want alive: timeout expiry is not drain evidence", presence, err)
		}
	})
	t.Run("same-start live identity is alive at timeout", func(t *testing.T) {
		stubLinuxProcessStatRead(t, func(string) ([]byte, error) {
			return linuxUnitStatLine(pid, "sleep", "S", 1, start), nil
		})
		presence, err := awaitLinuxProcessGone(pid, start, shortWait)
		if presence != linuxProcessPresenceAlive || err != nil {
			t.Fatalf("presence=%s err=%v, want alive", presence, err)
		}
	})
	t.Run("different start identity is gone on first read", func(t *testing.T) {
		stubLinuxProcessStatRead(t, func(string) ([]byte, error) {
			return linuxUnitStatLine(pid, "sleep", "S", 1, start+7), nil
		})
		presence, err := awaitLinuxProcessGone(pid, start, shortWait)
		if presence != linuxProcessPresenceGone || err != nil {
			t.Fatalf("presence=%s err=%v, want gone", presence, err)
		}
	})
	t.Run("malformed stat is refused", func(t *testing.T) {
		stubLinuxProcessStatRead(t, func(string) ([]byte, error) { return []byte("garbage"), nil })
		presence, err := awaitLinuxProcessGone(pid, start, shortWait)
		if presence != linuxProcessPresenceUnknown || err == nil {
			t.Fatalf("presence=%s err=%v, want inspection_unknown", presence, err)
		}
	})
}

// TestLinuxProcessPresenceDefaultReaderObservesRealProc is a mini canary for
// the default seam: the deterministic unit tests must not have replaced the
// real /proc reader, and the test process itself must classify as alive.
func TestLinuxProcessPresenceDefaultReaderObservesRealProc(t *testing.T) {
	self := os.Getpid()
	data, err := linuxProcessStatRead(fmt.Sprintf("/proc/%d/stat", self))
	if err != nil || !strings.HasPrefix(string(data), fmt.Sprintf("%d (", self)) {
		t.Fatalf("default process reader is not the real /proc reader (err=%v)", err)
	}
	start, err := linuxProcessStartIdentity(self)
	if err != nil {
		t.Fatalf("read own start identity: %v", err)
	}
	presence, err := linuxProcessPresenceOf(self, start)
	if presence != linuxProcessPresenceAlive || err != nil {
		t.Fatalf("own process presence=%s err=%v, want alive", presence, err)
	}
}
