//go:build linux && drill

// borrowed-receipt-auth-entry-probe-fd_linux_test.go is the STANDALONE
// generated-program-unit wrapper for the entry-private strict probe's
// protected-scope fd scan (RE04 P2). It compiles the ignore-tagged probe
// helper source exactly as the entry test does, then drives deterministic
// regressions against the ACTUAL census scan function:
//
//   - fd renumbering race: the census enumerates a live child's fd directory
//     while the victim socket is held at the old fd, the child duplicates the
//     socket to a new fd and closes the old one before the readlink, and the
//     retained child still owns the socket. The scan must refuse UNKNOWN on
//     the unreconciled fd ENOENT and must never report OK from an incomplete
//     ownership accounting.
//   - fd directory vanish: the child identity was read alive and then the fd
//     directory disappears before the scan. The scan must refuse UNKNOWN; a
//     vanished fd directory is never classified as proven process
//     disappearance or proven absence.
//   - injected negative errno branches (readdir EACCES, readlink EIO) must
//     still refuse with the exact errno named.
//
// Determinism: the census reads /proc/<postmaster>/task/<postmaster>/children,
// which is thread-specific, and the Go runtime forks from arbitrary threads.
// The wrapper therefore locks its test goroutine to one OS thread, starts the
// retained child from that locked thread, and passes that thread's TID and
// kernel start identity as the census postmaster; the census probe itself is
// started from a separate worker goroutine on another thread, so the scanned
// scope is exactly the retained child.
//
// The wrapper is UNIT-ONLY and non-authorizing: it is not part of the real-PG
// retirement proof, it uses only the Go standard library and no new
// permission, and it grants no auth/clean/DDL/acceptance authority. The
// injected-errno counterexamples are honest unit faults, not real root/kernel
// EACCES authority. It does not modify the entry implementation, the shared
// fixture, or any other writer's tests.
package recovery_test

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

const (
	// borrowedReceiptAuthEntryProbeFDChildEnv marks a re-execution of this
	// test binary as the retained fd child helper process.
	borrowedReceiptAuthEntryProbeFDChildEnv = "TXHARBOR_PROBE_FD_CHILD"
	// borrowedReceiptAuthEntryProbeFDHookDirEnv / HookPIDEnv activate the
	// probe's test-only census notification hook for one child PID.
	borrowedReceiptAuthEntryProbeFDHookDirEnv = "TXHARBOR_PROBE_FD_HOOK_DIR"
	borrowedReceiptAuthEntryProbeFDHookPIDEnv = "TXHARBOR_PROBE_FD_HOOK_PID"
	// borrowedReceiptAuthEntryProbeFDFaultEnv injects one negative errno into
	// the probe's unexported fd I/O dependency (readdir|readlink:ERRNO).
	borrowedReceiptAuthEntryProbeFDFaultEnv = "TXHARBOR_PROBE_FD_FAULT"
	// borrowedReceiptAuthEntryProbeFDHeldFD is the fd number the child helper
	// holds the victim socket at before the renumbering race.
	borrowedReceiptAuthEntryProbeFDHeldFD = 7
	// borrowedReceiptAuthEntryProbeFDRenumberedFD is the fd number the child
	// helper duplicates the victim socket to before closing the old fd.
	borrowedReceiptAuthEntryProbeFDRenumberedFD = 100
	// borrowedReceiptAuthEntryProbeFDChildTestRun selects only the child
	// helper test when the test binary is re-executed as the child process.
	borrowedReceiptAuthEntryProbeFDChildTestRun = "^TestBorrowedAuthEntryProbeFDChildProcess$"
)

// buildBorrowedReceiptAuthEntryProbeFD compiles the entry-private strict probe
// from the ignore-tagged helper source as a static Go binary. It is a
// standalone copy of the entry build so this wrapper depends on no other
// writer's test file.
func buildBorrowedReceiptAuthEntryProbeFD(t *testing.T) string {
	t.Helper()
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate fd wrapper source for the strict probe")
	}
	source := filepath.Join(filepath.Dir(testFile), "borrowed-receipt-auth-entry-probe_linux_testhelper.go")
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatalf("chmod fd probe build directory: %v", err)
	}
	output := filepath.Join(dir, "borrowed-receipt-auth-entry-probe")
	cmd := exec.Command("go", "build", "-trimpath", "-o", output, source)
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	if result, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build fd strict probe: %v: %s", err, strings.TrimSpace(string(result)))
	}
	return output
}

// borrowedReceiptAuthEntryProbeFDThreadStart reads the kernel start identity
// of the given thread from /proc/<tid>/stat, exactly as the census reads it.
func borrowedReceiptAuthEntryProbeFDThreadStart(tid int) (string, error) {
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", tid))
	if err != nil {
		return "", err
	}
	text := string(raw)
	last := strings.LastIndexByte(text, ')')
	if last <= 0 || last+1 >= len(text) {
		return "", errors.New("thread stat command delimiter is malformed")
	}
	fields := strings.Fields(text[last+1:])
	if len(fields) <= 19 {
		return "", errors.New("thread stat fields are truncated")
	}
	start, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil || start == 0 {
		return "", fmt.Errorf("thread stat start identity is invalid: %q", fields[19])
	}
	return strconv.FormatUint(start, 10), nil
}

// borrowedReceiptAuthEntryProbeFDBuffer is a mutex-guarded buffer so the test
// goroutine may read child/probe diagnostics while the exec copy goroutines
// are still writing.
type borrowedReceiptAuthEntryProbeFDBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *borrowedReceiptAuthEntryProbeFDBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *borrowedReceiptAuthEntryProbeFDBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// borrowedReceiptAuthEntryProbeFDChild is a real owned child process started
// from the locked postmaster thread. It holds one real socket inode at a known
// fd and can duplicate it to a new fd and close the old one on command,
// producing the deterministic fd renumbering race against the actual census
// scan.
type borrowedReceiptAuthEntryProbeFDChild struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout *bufio.Reader
	stderr *borrowedReceiptAuthEntryProbeFDBuffer
	pid    int
	fd     int
	inode  string
}

// startBorrowedReceiptAuthEntryProbeFDChild starts the retained child helper on
// the CALLING thread (the wrapper calls it from its locked postmaster thread)
// and validates its READY report. The caller must arrange cleanup via stop.
func startBorrowedReceiptAuthEntryProbeFDChild() (*borrowedReceiptAuthEntryProbeFDChild, error) {
	exe, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("locate fd wrapper test binary: %w", err)
	}
	cmd := exec.Command(exe, "-test.run="+borrowedReceiptAuthEntryProbeFDChildTestRun)
	cmd.Env = append(os.Environ(), borrowedReceiptAuthEntryProbeFDChildEnv+"=1")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("fd child stdin pipe: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("fd child stdout pipe: %w", err)
	}
	stderr := &borrowedReceiptAuthEntryProbeFDBuffer{}
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start fd child helper process: %w", err)
	}
	child := &borrowedReceiptAuthEntryProbeFDChild{
		cmd:    cmd,
		stdin:  stdin,
		stdout: bufio.NewReader(stdout),
		stderr: stderr,
		pid:    cmd.Process.Pid,
	}
	line, err := child.readLine("READY ", 30*time.Second)
	if err != nil {
		child.stop()
		return nil, fmt.Errorf("fd child READY: %w (child stderr: %s)", err, stderr.String())
	}
	fields := strings.Fields(line)
	if len(fields) != 3 {
		child.stop()
		return nil, fmt.Errorf("fd child READY line %q is malformed", line)
	}
	fdText, ok := strings.CutPrefix(fields[1], "fd=")
	if !ok {
		child.stop()
		return nil, fmt.Errorf("fd child READY line %q has no held fd", line)
	}
	fd, err := strconv.Atoi(fdText)
	if err != nil || fd != borrowedReceiptAuthEntryProbeFDHeldFD {
		child.stop()
		return nil, fmt.Errorf("fd child held fd %q, want %d", fdText, borrowedReceiptAuthEntryProbeFDHeldFD)
	}
	inode, ok := strings.CutPrefix(fields[2], "inode=")
	if !ok || inode == "" {
		child.stop()
		return nil, fmt.Errorf("fd child READY line %q has no socket inode", line)
	}
	child.fd = fd
	child.inode = inode
	return child, nil
}

func (c *borrowedReceiptAuthEntryProbeFDChild) readLine(prefix string, timeout time.Duration) (string, error) {
	type readResult struct {
		line string
		err  error
	}
	results := make(chan readResult, 1)
	go func() {
		for {
			line, err := c.stdout.ReadString('\n')
			if err != nil {
				results <- readResult{err: err}
				return
			}
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, prefix) {
				results <- readResult{line: line}
				return
			}
		}
	}()
	select {
	case result := <-results:
		if result.err != nil {
			return "", fmt.Errorf("waiting for %q: %w (child stderr: %s)", prefix, result.err, c.stderr.String())
		}
		return result.line, nil
	case <-time.After(timeout):
		return "", fmt.Errorf("timed out waiting for %q (child stderr: %s)", prefix, c.stderr.String())
	}
}

func (c *borrowedReceiptAuthEntryProbeFDChild) send(command string) error {
	if _, err := io.WriteString(c.stdin, command+"\n"); err != nil {
		return fmt.Errorf("send %q: %w", command, err)
	}
	return nil
}

func (c *borrowedReceiptAuthEntryProbeFDChild) killAndReap() error {
	if err := c.cmd.Process.Kill(); err != nil {
		return fmt.Errorf("kill fd child: %w", err)
	}
	err := c.cmd.Wait()
	var exitErr *exec.ExitError
	if err == nil || !errors.As(err, &exitErr) {
		return fmt.Errorf("reap fd child: %w", err)
	}
	return nil
}

func (c *borrowedReceiptAuthEntryProbeFDChild) stop() {
	if c == nil || c.cmd == nil || c.cmd.Process == nil {
		return
	}
	_ = c.stdin.Close()
	_ = c.cmd.Process.Kill()
	_ = c.cmd.Wait()
}

// borrowedReceiptAuthEntryProbeFDCensusRun is one asynchronous census probe
// execution so the wrapper can acknowledge notification hooks while the probe
// is blocked inside the actual scan function.
type borrowedReceiptAuthEntryProbeFDCensusRun struct {
	cmd    *exec.Cmd
	stdout *borrowedReceiptAuthEntryProbeFDBuffer
	stderr *borrowedReceiptAuthEntryProbeFDBuffer
	done   chan struct{}
	err    error
}

func startBorrowedReceiptAuthEntryProbeFDCensus(probe string, env []string, args ...string) (*borrowedReceiptAuthEntryProbeFDCensusRun, error) {
	cmd := exec.Command(probe, args...)
	cmd.Env = append(os.Environ(), env...)
	stdout := &borrowedReceiptAuthEntryProbeFDBuffer{}
	stderr := &borrowedReceiptAuthEntryProbeFDBuffer{}
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start fd census probe: %w", err)
	}
	run := &borrowedReceiptAuthEntryProbeFDCensusRun{
		cmd:    cmd,
		stdout: stdout,
		stderr: stderr,
		done:   make(chan struct{}),
	}
	go func() {
		run.err = cmd.Wait()
		close(run.done)
	}()
	return run, nil
}

func (r *borrowedReceiptAuthEntryProbeFDCensusRun) wait(timeout time.Duration) (string, error) {
	select {
	case <-r.done:
	case <-time.After(timeout):
		r.ensureDone()
		return "", fmt.Errorf("fd census probe timed out (stderr: %s)", r.stderr.String())
	}
	return strings.TrimSpace(r.stdout.String()), r.err
}

func (r *borrowedReceiptAuthEntryProbeFDCensusRun) ensureDone() {
	select {
	case <-r.done:
	default:
		_ = r.cmd.Process.Kill()
		<-r.done
	}
}

// waitBorrowedReceiptAuthEntryProbeFDHookOrExit waits for the ready marker of
// one activated phase and returns the fresh nonce the probe wrote. It reads
// only regular files, so a preexisting FIFO or symlink is never opened or
// interpreted; if the probe exits first it returns the actual probe output so
// negative handshake cases are asserted from the real terminal, never
// inferred.
func waitBorrowedReceiptAuthEntryProbeFDHookOrExit(run *borrowedReceiptAuthEntryProbeFDCensusRun, dir string, childPID int, phase string, timeout time.Duration) (bool, string, string, error) {
	path := filepath.Join(dir, fmt.Sprintf("%d.%s.ready", childPID, phase))
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if info, err := os.Lstat(path); err == nil && info.Mode().IsRegular() {
			if raw, err := os.ReadFile(path); err == nil && len(raw) > 0 {
				return true, string(raw), "", nil
			}
		}
		select {
		case <-run.done:
			return false, "", strings.TrimSpace(run.stdout.String()), run.err
		default:
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false, "", "", fmt.Errorf("fd census probe hook %s did not fire within %s (stderr: %s)", path, timeout, run.stderr.String())
}

func ackBorrowedReceiptAuthEntryProbeFDHook(dir string, childPID int, phase, nonce string) error {
	path := filepath.Join(dir, fmt.Sprintf("%d.%s.ack", childPID, phase))
	if err := os.WriteFile(path, []byte(nonce), 0o600); err != nil {
		return fmt.Errorf("fd census hook ack %s: %w", path, err)
	}
	return nil
}

// runBorrowedReceiptAuthEntryProbeFDCensusDirect runs the compiled probe once
// and returns its stdout/exit without any retained-child harness; it is used
// for configurations that must refuse before any scan.
func runBorrowedReceiptAuthEntryProbeFDCensusDirect(t *testing.T, probe string, env []string, args ...string) (string, error) {
	t.Helper()
	run, err := startBorrowedReceiptAuthEntryProbeFDCensus(probe, env, args...)
	if err != nil {
		t.Fatalf("start fd census probe: %v", err)
	}
	t.Cleanup(run.ensureDone)
	return run.wait(60 * time.Second)
}

// borrowedReceiptAuthEntryProbeFDScenario describes one deterministic census
// scenario: the retained child is started from the locked postmaster thread,
// and the worker optionally mutates it at one exact notification phase.
type borrowedReceiptAuthEntryProbeFDScenario struct {
	probe string
	env   []string
	// hookActive forces the notification hook on even when no mutation
	// callback is set (negative handshake cases).
	hookActive bool
	// hookDir overrides the private hook directory (for example a missing
	// directory for the marker creation failure).
	hookDir string
	// prepareHook runs after the child started and before the probe starts, so
	// a test can place a preexisting object at the exact ready-marker path.
	prepareHook func(child *borrowedReceiptAuthEntryProbeFDChild, hookDir string) error
	// skipAck leaves the ready marker unanswered, exercising the bounded ack
	// deadline.
	skipAck bool
	// replaceReady runs after the ready marker fired and before the ack; it
	// may replace the probe-created marker to prove the helper never unlinks a
	// replacement during the handshake.
	replaceReady func(child *borrowedReceiptAuthEntryProbeFDChild, hookDir, nonce string) error
	// ackFIFO places a FIFO at the ack path and streams the exact nonce
	// through it instead of writing a regular ack file, proving the descriptor
	// proof refuses a non-regular descriptor even when the nonce is supplied.
	ackFIFO bool
	// afterStat runs after the census has read the child identity alive and
	// before the fd directory is read.
	afterStat func(child *borrowedReceiptAuthEntryProbeFDChild) error
	// afterReaddir runs after the census enumerated the child fd directory and
	// before the readlinks; it is used for the fd renumbering race.
	afterReaddir func(child *borrowedReceiptAuthEntryProbeFDChild) error
}

type borrowedReceiptAuthEntryProbeFDResult struct {
	out string
	err error
}

// runBorrowedReceiptAuthEntryProbeFDScenario locks the calling test goroutine
// to one OS thread, starts the retained child from that thread, and runs the
// census probe from a separate worker goroutine so the probe is never part of
// the scanned scope. It returns the probe stdout, the retained child (still
// alive until the test cleanup) and an error after the probe has exited.
func runBorrowedReceiptAuthEntryProbeFDScenario(t *testing.T, s *borrowedReceiptAuthEntryProbeFDScenario) (string, *borrowedReceiptAuthEntryProbeFDChild, error) {
	t.Helper()
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	postmasterTID := syscall.Gettid()
	postmasterStart, err := borrowedReceiptAuthEntryProbeFDThreadStart(postmasterTID)
	if err != nil {
		t.Fatalf("read postmaster thread stat: %v", err)
	}
	child, err := startBorrowedReceiptAuthEntryProbeFDChild()
	if err != nil {
		t.Fatalf("start retained fd child: %v", err)
	}
	t.Cleanup(child.stop)
	hookDir := s.hookDir
	if hookDir == "" {
		hookDir = t.TempDir()
	}
	if s.prepareHook != nil {
		if err := s.prepareHook(child, hookDir); err != nil {
			t.Fatalf("prepare fd hook: %v", err)
		}
	}
	hooks := s.hookActive || s.skipAck || s.afterStat != nil || s.afterReaddir != nil
	env := append([]string{}, s.env...)
	if hooks {
		env = append(env,
			borrowedReceiptAuthEntryProbeFDHookDirEnv+"="+hookDir,
			borrowedReceiptAuthEntryProbeFDHookPIDEnv+"="+strconv.Itoa(child.pid))
	}
	resultCh := make(chan borrowedReceiptAuthEntryProbeFDResult, 1)
	go func() {
		resultCh <- borrowedReceiptAuthEntryProbeFDProbeWorker(s, env, hookDir, hooks, postmasterTID, postmasterStart, child)
	}()
	result := <-resultCh
	return result.out, child, result.err
}

func borrowedReceiptAuthEntryProbeFDProbeWorker(s *borrowedReceiptAuthEntryProbeFDScenario, env []string, hookDir string, hooks bool, postmasterTID int, postmasterStart string, child *borrowedReceiptAuthEntryProbeFDChild) borrowedReceiptAuthEntryProbeFDResult {
	run, err := startBorrowedReceiptAuthEntryProbeFDCensus(s.probe, env,
		"census", strconv.Itoa(postmasterTID), postmasterStart, "424242", "1", child.inode)
	if err != nil {
		return borrowedReceiptAuthEntryProbeFDResult{err: err}
	}
	defer run.ensureDone()
	if hooks {
		fired, nonce, out, err := waitBorrowedReceiptAuthEntryProbeFDHookOrExit(run, hookDir, child.pid, "after-stat", 30*time.Second)
		if !fired {
			return borrowedReceiptAuthEntryProbeFDResult{out: out, err: err}
		}
		if s.afterStat != nil {
			if err := s.afterStat(child); err != nil {
				return borrowedReceiptAuthEntryProbeFDResult{err: err}
			}
		}
		if !s.skipAck {
			if s.replaceReady != nil {
				if err := s.replaceReady(child, hookDir, nonce); err != nil {
					return borrowedReceiptAuthEntryProbeFDResult{err: err}
				}
			}
			if s.ackFIFO {
				// A FIFO carrying the exact nonce must never be accepted: the
				// descriptor-type proof refuses before any byte is read.
				ackPath := filepath.Join(hookDir, fmt.Sprintf("%d.after-stat.ack", child.pid))
				if err := syscall.Mkfifo(ackPath, 0o600); err != nil {
					return borrowedReceiptAuthEntryProbeFDResult{err: err}
				}
				stream, err := os.OpenFile(ackPath, os.O_RDWR, 0)
				if err != nil {
					return borrowedReceiptAuthEntryProbeFDResult{err: err}
				}
				if _, err := io.WriteString(stream, nonce); err != nil {
					_ = stream.Close()
					return borrowedReceiptAuthEntryProbeFDResult{err: err}
				}
				if err := stream.Close(); err != nil {
					return borrowedReceiptAuthEntryProbeFDResult{err: err}
				}
			} else {
				if err := ackBorrowedReceiptAuthEntryProbeFDHook(hookDir, child.pid, "after-stat", nonce); err != nil {
					return borrowedReceiptAuthEntryProbeFDResult{err: err}
				}
				if s.afterReaddir != nil {
					fired, nonce, out, err := waitBorrowedReceiptAuthEntryProbeFDHookOrExit(run, hookDir, child.pid, "after-readdir", 30*time.Second)
					if !fired {
						return borrowedReceiptAuthEntryProbeFDResult{out: out, err: err}
					}
					if err := s.afterReaddir(child); err != nil {
						return borrowedReceiptAuthEntryProbeFDResult{err: err}
					}
					if err := ackBorrowedReceiptAuthEntryProbeFDHook(hookDir, child.pid, "after-readdir", nonce); err != nil {
						return borrowedReceiptAuthEntryProbeFDResult{err: err}
					}
				}
			}
		}
	}
	out, err := run.wait(60 * time.Second)
	return borrowedReceiptAuthEntryProbeFDResult{out: out, err: err}
}

// TestBorrowedReceiptAuthEntryProbeFDFdRenumberRace reproduces the RE04 P2
// false-absence: the census enumerates fd 7 holding victim socket X, the
// retained child duplicates X to fd 100 and closes fd 7 before the readlink,
// and fd 100 was never enumerated. The actual scan function must refuse
// UNKNOWN on the unreconciled ENOENT and never report OK.
func TestBorrowedReceiptAuthEntryProbeFDFdRenumberRace(t *testing.T) {
	probe := buildBorrowedReceiptAuthEntryProbeFD(t)
	out, child, err := runBorrowedReceiptAuthEntryProbeFDScenario(t, &borrowedReceiptAuthEntryProbeFDScenario{
		probe: probe,
		afterReaddir: func(child *borrowedReceiptAuthEntryProbeFDChild) error {
			t.Logf("fd renumber trace: census enumerated held fd %d (socket inode %s) for retained live child pid %d", child.fd, child.inode, child.pid)
			if err := child.send("dup-close"); err != nil {
				return err
			}
			line, err := child.readLine("DONE dup-close", 30*time.Second)
			if err != nil {
				return err
			}
			t.Logf("fd renumber trace: child %q; old fd closed, socket still owned at fd %d", line, borrowedReceiptAuthEntryProbeFDRenumberedFD)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("fd renumber race: %v", err)
	}
	t.Logf("fd renumber trace: probe verdict %q", out)
	if out != "CENSUS UNKNOWN errno=ENOENT reason=child-fd-readlink-error" {
		t.Fatalf("fd renumber race: got %q, want the explicit UNKNOWN ENOENT refusal", out)
	}
	if strings.Contains(out, "CENSUS OK") || strings.Contains(out, "CENSUS PRESENT") || strings.Contains(out, "complete=true") {
		t.Fatalf("fd renumber race: incomplete fd accounting produced a success/partial verdict %q", out)
	}

	// Positive control: the retained process still owns the victim socket at
	// the renumbered fd, so an OK verdict would have been a false absence.
	target, err := os.Readlink(fmt.Sprintf("/proc/%d/fd/%d", child.pid, borrowedReceiptAuthEntryProbeFDRenumberedFD))
	if err != nil {
		t.Fatalf("retained fd child does not own the renumbered fd: %v", err)
	}
	if target != "socket:["+child.inode+"]" {
		t.Fatalf("renumbered fd target %q, want socket:[%s]", target, child.inode)
	}
}

// TestBorrowedReceiptAuthEntryProbeFDFdDirVanishes reproduces the RE04 P2
// vanished-directory branch: the child identity was read alive, then the fd
// directory disappears before the scan. The actual scan function must refuse
// UNKNOWN; it never classifies a vanished fd directory as proven process
// disappearance or proven absence.
func TestBorrowedReceiptAuthEntryProbeFDFdDirVanishes(t *testing.T) {
	probe := buildBorrowedReceiptAuthEntryProbeFD(t)
	out, _, err := runBorrowedReceiptAuthEntryProbeFDScenario(t, &borrowedReceiptAuthEntryProbeFDScenario{
		probe: probe,
		afterStat: func(child *borrowedReceiptAuthEntryProbeFDChild) error {
			t.Logf("fd vanish trace: child pid %d identity was read live; killing and reaping before the fd scan", child.pid)
			if err := child.killAndReap(); err != nil {
				return err
			}
			if _, err := os.Stat(fmt.Sprintf("/proc/%d/fd", child.pid)); err == nil {
				return fmt.Errorf("reaped fd child still exposes /proc/%d/fd", child.pid)
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("fd directory vanish: %v", err)
	}
	t.Logf("fd vanish trace: probe verdict %q", out)
	if out != "CENSUS UNKNOWN errno=ENOENT reason=child-fd-table-read-error" {
		t.Fatalf("fd directory vanish: got %q, want the explicit UNKNOWN ENOENT refusal", out)
	}
	if strings.Contains(out, "CENSUS OK") || strings.Contains(out, "CENSUS PRESENT") || strings.Contains(out, "complete=true") {
		t.Fatalf("fd directory vanish: incomplete fd accounting produced a success/partial verdict %q", out)
	}
}

// TestBorrowedReceiptAuthEntryProbeFDFaultErrno asserts that the unexported
// negative errno injection still refuses with the exact errno named for the fd
// directory (EACCES) and readlink (EIO) branches. These are honest unit faults
// against the actual scan function, not real kernel/root EACCES authority.
func TestBorrowedReceiptAuthEntryProbeFDFaultErrno(t *testing.T) {
	probe := buildBorrowedReceiptAuthEntryProbeFD(t)
	cases := []struct {
		name  string
		fault string
		want  string
	}{
		{"readdir-eacces", "readdir:EACCES", "CENSUS UNKNOWN errno=EACCES reason=child-fd-table-read-error"},
		{"readlink-eio", "readlink:EIO", "CENSUS UNKNOWN errno=EIO reason=child-fd-readlink-error"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, _, err := runBorrowedReceiptAuthEntryProbeFDScenario(t, &borrowedReceiptAuthEntryProbeFDScenario{
				probe: probe,
				env:   []string{borrowedReceiptAuthEntryProbeFDFaultEnv + "=" + tc.fault},
			})
			if err != nil {
				t.Fatalf("injected %s: %v", tc.fault, err)
			}
			if out != tc.want {
				t.Fatalf("injected %s: got %q, want %q", tc.fault, out, tc.want)
			}
			if strings.Contains(out, "CENSUS OK") || strings.Contains(out, "CENSUS PRESENT") {
				t.Fatalf("injected negative errno produced a success verdict %q", out)
			}
		})
	}
}

// TestBorrowedReceiptAuthEntryProbeFDFaultSpecRejects asserts the P2-1 strict
// pre-scan validation of the injected fault specification: a missing
// delimiter, empty/unknown operation, empty/unsupported errno or trailing
// garbage is an explicit UNKNOWN before ANY scan, never a silent fallback to
// normal reads. The retained child stays untouched, proving no scan ran.
func TestBorrowedReceiptAuthEntryProbeFDFaultSpecRejects(t *testing.T) {
	probe := buildBorrowedReceiptAuthEntryProbeFD(t)
	cases := []struct {
		name string
		spec string
	}{
		{"missing-delimiter", "readdir"},
		{"empty-operation", ":EACCES"},
		{"unknown-operation", "readfoo:EACCES"},
		{"empty-errno", "readdir:"},
		{"unsupported-errno", "readdir:ENOENT"},
		{"trailing-garbage", "readdir:EACCES:x"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, child, err := runBorrowedReceiptAuthEntryProbeFDScenario(t, &borrowedReceiptAuthEntryProbeFDScenario{
				probe: probe,
				env:   []string{borrowedReceiptAuthEntryProbeFDFaultEnv + "=" + tc.spec},
			})
			if err != nil {
				t.Fatalf("fault spec %q: %v", tc.spec, err)
			}
			if out != "CENSUS UNKNOWN reason=probe-fd-fault-invalid" {
				t.Fatalf("fault spec %q: got %q, want the pre-scan UNKNOWN refusal", tc.spec, out)
			}
			if strings.Contains(out, "CENSUS OK") || strings.Contains(out, "CENSUS PRESENT") {
				t.Fatalf("fault spec %q produced a success verdict %q", tc.spec, out)
			}
			target, err := os.Readlink(fmt.Sprintf("/proc/%d/fd/%d", child.pid, borrowedReceiptAuthEntryProbeFDHeldFD))
			if err != nil || target != "socket:["+child.inode+"]" {
				t.Fatalf("invalid fault spec did not leave the retained child untouched: %v %q", err, target)
			}
		})
	}
}

// TestBorrowedReceiptAuthEntryProbeFDHookConfigRejects asserts the P2-2
// pre-scan validation of a nonempty hook configuration: a missing,
// non-numeric or non-positive target PID refuses before any scan.
func TestBorrowedReceiptAuthEntryProbeFDHookConfigRejects(t *testing.T) {
	probe := buildBorrowedReceiptAuthEntryProbeFD(t)
	cases := []struct {
		name string
		pid  string
	}{
		{"missing-pid", ""},
		{"nonnumeric-pid", "abc"},
		{"zero-pid", "0"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			out, err := runBorrowedReceiptAuthEntryProbeFDCensusDirect(t, probe,
				[]string{
					borrowedReceiptAuthEntryProbeFDHookDirEnv + "=" + dir,
					borrowedReceiptAuthEntryProbeFDHookPIDEnv + "=" + tc.pid,
				},
				"census", "2", "1", "424242", "1", "1")
			if err != nil {
				t.Fatalf("hook config %q: %v", tc.pid, err)
			}
			if out != "CENSUS UNKNOWN reason=probe-fd-hook-invalid" {
				t.Fatalf("hook config %q: got %q, want the pre-scan UNKNOWN refusal", tc.pid, out)
			}
		})
	}
}

// TestBorrowedReceiptAuthEntryProbeFDHookMarkerFailure asserts the P2-2 marker
// lifecycle: a marker creation failure and every preexisting ready-marker
// object (regular file, FIFO, symlink) are bounded UNKNOWN refusals, the scan
// never resumes, and the preexisting object is never opened, unlinked or
// replaced.
func TestBorrowedReceiptAuthEntryProbeFDHookMarkerFailure(t *testing.T) {
	probe := buildBorrowedReceiptAuthEntryProbeFD(t)
	t.Run("missing-directory", func(t *testing.T) {
		missing := filepath.Join(t.TempDir(), "missing")
		start := time.Now()
		out, _, err := runBorrowedReceiptAuthEntryProbeFDScenario(t, &borrowedReceiptAuthEntryProbeFDScenario{
			probe:      probe,
			hookActive: true,
			hookDir:    missing,
		})
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("marker creation failure: %v", err)
		}
		if out != "CENSUS UNKNOWN errno=ENOENT reason=child-fd-hook-error phase=after-stat" {
			t.Fatalf("marker creation failure: got %q", out)
		}
		if strings.Contains(out, "CENSUS OK") || strings.Contains(out, "CENSUS PRESENT") {
			t.Fatalf("marker creation failure resumed the scan: %q", out)
		}
		if elapsed > 30*time.Second {
			t.Fatalf("marker creation failure was not bounded: %s", elapsed)
		}
	})
	for _, kind := range []string{"regular", "fifo", "symlink"} {
		t.Run("preexisting-"+kind, func(t *testing.T) {
			var readyPath string
			start := time.Now()
			out, _, err := runBorrowedReceiptAuthEntryProbeFDScenario(t, &borrowedReceiptAuthEntryProbeFDScenario{
				probe:      probe,
				hookActive: true,
				prepareHook: func(child *borrowedReceiptAuthEntryProbeFDChild, hookDir string) error {
					readyPath = filepath.Join(hookDir, fmt.Sprintf("%d.after-stat.ready", child.pid))
					switch kind {
					case "regular":
						return os.WriteFile(readyPath, []byte("preexisting"), 0o600)
					case "fifo":
						return syscall.Mkfifo(readyPath, 0o600)
					default:
						return os.Symlink(filepath.Join(hookDir, "no-such-target"), readyPath)
					}
				},
			})
			elapsed := time.Since(start)
			if err != nil {
				t.Fatalf("preexisting %s: %v", kind, err)
			}
			if out != "CENSUS UNKNOWN reason=child-fd-hook-marker-preexisting phase=after-stat" {
				t.Fatalf("preexisting %s: got %q", kind, out)
			}
			if strings.Contains(out, "CENSUS OK") || strings.Contains(out, "CENSUS PRESENT") {
				t.Fatalf("preexisting %s resumed the scan: %q", kind, out)
			}
			if elapsed > 30*time.Second {
				t.Fatalf("preexisting %s was not bounded: %s", kind, elapsed)
			}
			info, err := os.Lstat(readyPath)
			if err != nil {
				t.Fatalf("preexisting %s object was removed: %v", kind, err)
			}
			switch kind {
			case "regular":
				if !info.Mode().IsRegular() {
					t.Fatalf("preexisting regular object changed type: %v", info.Mode())
				}
			case "fifo":
				if info.Mode()&os.ModeNamedPipe == 0 {
					t.Fatalf("preexisting FIFO object changed type: %v", info.Mode())
				}
			case "symlink":
				if info.Mode()&os.ModeSymlink == 0 {
					t.Fatalf("preexisting symlink object changed type: %v", info.Mode())
				}
			}
		})
	}
}

// TestBorrowedReceiptAuthEntryProbeFDHookAckTimeout asserts the P2-2 bounded
// ack deadline: a missing acknowledgement is an explicit UNKNOWN after a
// finite, measured wait, and the scan never resumes.
func TestBorrowedReceiptAuthEntryProbeFDHookAckTimeout(t *testing.T) {
	probe := buildBorrowedReceiptAuthEntryProbeFD(t)
	start := time.Now()
	out, _, err := runBorrowedReceiptAuthEntryProbeFDScenario(t, &borrowedReceiptAuthEntryProbeFDScenario{
		probe:      probe,
		hookActive: true,
		skipAck:    true,
	})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("missing ack: %v", err)
	}
	if out != "CENSUS UNKNOWN reason=child-fd-hook-ack-timeout phase=after-stat" {
		t.Fatalf("missing ack: got %q", out)
	}
	if strings.Contains(out, "CENSUS OK") || strings.Contains(out, "CENSUS PRESENT") {
		t.Fatalf("missing ack resumed the scan: %q", out)
	}
	if elapsed < 5*time.Second || elapsed > 45*time.Second {
		t.Fatalf("missing ack was not a finite bounded wait: %s", elapsed)
	}
}

// TestBorrowedReceiptAuthEntryProbeFDSeamDisabledDefault proves the disabled
// seam default behavior is unchanged: with no fault and no hook environment
// the census performs the real scan and reports the retained child's socket
// ownership exactly as before.
func TestBorrowedReceiptAuthEntryProbeFDSeamDisabledDefault(t *testing.T) {
	probe := buildBorrowedReceiptAuthEntryProbeFD(t)
	out, _, err := runBorrowedReceiptAuthEntryProbeFDScenario(t, &borrowedReceiptAuthEntryProbeFDScenario{probe: probe})
	if err != nil {
		t.Fatalf("disabled seam: %v", err)
	}
	t.Logf("disabled seam default verdict: %q", out)
	if !strings.HasPrefix(out, "CENSUS PRESENT pid_absent=1 inode_absent=0 ") {
		t.Fatalf("disabled seam changed the default scan verdict: %q", out)
	}
}

// TestBorrowedReceiptAuthEntryProbeFDHookAckDescriptorProof asserts the P2-2
// descriptor-type proof: a FIFO substituted at the ack path — even while
// carrying the exact ready nonce — is refused by the fstat of the ACTUAL
// opened descriptor before any byte is read, so the handshake never succeeds
// and the bounded deadline refuses. The FIFO is never unlinked or consumed.
func TestBorrowedReceiptAuthEntryProbeFDHookAckDescriptorProof(t *testing.T) {
	probe := buildBorrowedReceiptAuthEntryProbeFD(t)
	hookDir := t.TempDir()
	start := time.Now()
	out, child, err := runBorrowedReceiptAuthEntryProbeFDScenario(t, &borrowedReceiptAuthEntryProbeFDScenario{
		probe:      probe,
		hookActive: true,
		hookDir:    hookDir,
		ackFIFO:    true,
	})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("ack descriptor proof: %v", err)
	}
	if out != "CENSUS UNKNOWN reason=child-fd-hook-ack-timeout phase=after-stat" {
		t.Fatalf("ack descriptor proof: got %q, want the bounded UNKNOWN refusal", out)
	}
	if strings.Contains(out, "CENSUS OK") || strings.Contains(out, "CENSUS PRESENT") {
		t.Fatalf("non-regular ack descriptor was accepted: %q", out)
	}
	if elapsed < 5*time.Second || elapsed > 45*time.Second {
		t.Fatalf("ack descriptor refusal was not a finite bounded wait: %s", elapsed)
	}
	ackPath := filepath.Join(hookDir, fmt.Sprintf("%d.after-stat.ack", child.pid))
	info, err := os.Lstat(ackPath)
	if err != nil {
		t.Fatalf("FIFO ack replacement was removed: %v", err)
	}
	if info.Mode()&os.ModeNamedPipe == 0 {
		t.Fatalf("FIFO ack replacement changed type: %v", info.Mode())
	}
}

// TestBorrowedReceiptAuthEntryProbeFDHookMarkerReplacementPersists asserts the
// P2-2 ownership fix: when the probe-created ready marker is replaced during
// the handshake, the helper never unlinks, truncates or unblocks the
// replacement; a valid nonce ack still finishes the real scan, and the
// replacement persists until the wrapper-owned temp directory lifecycle ends.
func TestBorrowedReceiptAuthEntryProbeFDHookMarkerReplacementPersists(t *testing.T) {
	probe := buildBorrowedReceiptAuthEntryProbeFD(t)
	for _, kind := range []string{"fifo", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			hookDir := t.TempDir()
			var readyPath string
			out, _, err := runBorrowedReceiptAuthEntryProbeFDScenario(t, &borrowedReceiptAuthEntryProbeFDScenario{
				probe:      probe,
				hookActive: true,
				hookDir:    hookDir,
				replaceReady: func(child *borrowedReceiptAuthEntryProbeFDChild, dir, nonce string) error {
					readyPath = filepath.Join(dir, fmt.Sprintf("%d.after-stat.ready", child.pid))
					if err := os.Remove(readyPath); err != nil {
						return err
					}
					if kind == "fifo" {
						return syscall.Mkfifo(readyPath, 0o600)
					}
					return os.Symlink(filepath.Join(dir, "replacement-target"), readyPath)
				},
				afterReaddir: func(*borrowedReceiptAuthEntryProbeFDChild) error { return nil },
			})
			if err != nil {
				t.Fatalf("marker replacement %s: %v", kind, err)
			}
			if !strings.HasPrefix(out, "CENSUS PRESENT pid_absent=1 inode_absent=0 ") {
				t.Fatalf("marker replacement %s: handshake did not finish the real scan: %q", kind, out)
			}
			info, err := os.Lstat(readyPath)
			if err != nil {
				t.Fatalf("replacement %s did not persist: %v", kind, err)
			}
			if kind == "fifo" && info.Mode()&os.ModeNamedPipe == 0 {
				t.Fatalf("replacement FIFO changed type: %v", info.Mode())
			}
			if kind == "symlink" && info.Mode()&os.ModeSymlink == 0 {
				t.Fatalf("replacement symlink changed type: %v", info.Mode())
			}
		})
	}
}

// TestBorrowedAuthEntryProbeFDChildProcess is the retained fd child helper. It
// is inert unless the wrapper re-executes this test binary with the private
// child environment variable, so a focused run of the wrapper tests never
// starts it. It holds one real socket inode at fd 7 and, on command,
// duplicates it to fd 100 and closes fd 7.
func TestBorrowedAuthEntryProbeFDChildProcess(t *testing.T) {
	if os.Getenv(borrowedReceiptAuthEntryProbeFDChildEnv) != "1" {
		t.Skip("fd probe child helper is only active when re-executed by the fd wrapper")
	}
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatalf("fd child socketpair: %v", err)
	}
	held, peer := fds[0], fds[1]
	if peer == borrowedReceiptAuthEntryProbeFDHeldFD {
		// The peer end landed exactly on the target fd; drop it before moving
		// the held end onto the target, otherwise the later close would close
		// the duplicated socket.
		if err := syscall.Close(peer); err != nil {
			t.Fatalf("fd child close peer fd: %v", err)
		}
		peer = -1
	}
	if held != borrowedReceiptAuthEntryProbeFDHeldFD {
		if err := syscall.Dup2(held, borrowedReceiptAuthEntryProbeFDHeldFD); err != nil {
			t.Fatalf("fd child dup2 to held fd: %v", err)
		}
		if err := syscall.Close(held); err != nil {
			t.Fatalf("fd child close original held fd: %v", err)
		}
	}
	if peer >= 0 {
		if err := syscall.Close(peer); err != nil {
			t.Fatalf("fd child close peer fd: %v", err)
		}
	}
	target, err := os.Readlink(fmt.Sprintf("/proc/self/fd/%d", borrowedReceiptAuthEntryProbeFDHeldFD))
	if err != nil {
		t.Fatalf("fd child read held fd target: %v", err)
	}
	const socketPrefix = "socket:["
	if !strings.HasPrefix(target, socketPrefix) || !strings.HasSuffix(target, "]") {
		t.Fatalf("fd child held fd is not a socket: %q", target)
	}
	inode := strings.TrimSuffix(strings.TrimPrefix(target, socketPrefix), "]")
	if inode == "" {
		t.Fatalf("fd child held socket inode is empty: %q", target)
	}
	fmt.Printf("READY fd=%d inode=%s\n", borrowedReceiptAuthEntryProbeFDHeldFD, inode)

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		switch strings.TrimSpace(scanner.Text()) {
		case "dup-close":
			if err := syscall.Dup2(borrowedReceiptAuthEntryProbeFDHeldFD, borrowedReceiptAuthEntryProbeFDRenumberedFD); err != nil {
				fmt.Printf("ERROR dup2: %v\n", err)
				continue
			}
			if err := syscall.Close(borrowedReceiptAuthEntryProbeFDHeldFD); err != nil {
				fmt.Printf("ERROR close: %v\n", err)
				continue
			}
			fmt.Printf("DONE dup-close\n")
		case "exit":
			_ = syscall.Close(borrowedReceiptAuthEntryProbeFDRenumberedFD)
			return
		}
	}
}
