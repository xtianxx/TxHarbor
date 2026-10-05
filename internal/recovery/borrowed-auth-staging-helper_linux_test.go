//go:build linux && drill

// borrowed-auth-staging-helper_linux_test.go is the private build/owned-process
// harness for the borrowed-auth staging prerequisite. It compiles the static
// staging client, runs it through `docker exec -i` inside the owned fixture
// (sole Wait owned once, output completion coordinated before Wait), awaits the
// strict HELPER_STARTED self identity before writing the private secret, then
// the actual READY tuple and SASL hold, parses only public phase markers and
// the strict census through the pure consumer, and proves real server-EOF
// versus managed QUIT, strict helper reap in the owned container namespace and
// strict producer invariants.
//
// The container-side process probe is a separate stdlib-only read helper that
// preserves errno: only its own exact ENOENT sentinel plus exit 44 proves
// absence, while EACCES/EIO/timeouts/malformed documents are UNKNOWN and never
// gone. Retirement is cooperative and bounded (private quit, bounded stdin
// close, then cancel/reap of only the host docker exec CLI); there is no
// forced signal to a bare container PID, so a PID observed before a possible
// reuse can never be signalled. A successful host Wait is never a retirement
// proof: the captured identity must strictly disappear or the result is
// UNKNOWN. The pre-auth SQL row is recorded as a non-capability observation
// only (a visible assigned role or a NULL database is accepted); association
// identity comes only from the strict census socket token.
//
// No writer/clean/accepted capability exists here and no actual borrowed run
// is started.
package recovery_test

import (
	"bytes"
	"context"
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
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/xtianxx/txharbor/internal/recovery"
)

const (
	borrowedAuthStagingHelperPath    = "/tmp/borrowed-auth-staging-client"
	borrowedAuthStagingStatProbePath = "/tmp/borrowed-auth-staging-statprobe"

	// The stat probe's own absence discriminator: the exact stdout sentinel
	// combined with the probe's own exit code. No generic exit code, docker
	// failure, shell boolean or failed cat can ever prove absence.
	borrowedAuthStagingStatProbeAbsentSentinel = "STATPROBE_ABSENT"
	borrowedAuthStagingStatProbeAbsentExit     = 44

	// Bounded budgets. Every operation context derives from its parent, so a
	// remaining parent deadline always clips a child operation.
	borrowedAuthStagingProbeBudget         = 5 * time.Second
	borrowedAuthStagingWriteBudget         = 10 * time.Second
	borrowedAuthStagingAssociationBudget   = 30 * time.Second
	borrowedAuthStagingDisappearanceBudget = 15 * time.Second
	borrowedAuthStagingCleanupBudget       = 30 * time.Second
	borrowedAuthStagingQuitWaitBudget      = 5 * time.Second
	borrowedAuthStagingReapWaitBudget      = 10 * time.Second
)

// borrowedAuthStagingAssociationRetryInterval is the bounded pause between
// strict census association attempts; the pause selects on the caller context,
// so a cancellation refuses promptly instead of sleeping out the wall
// association window.
const borrowedAuthStagingAssociationRetryInterval = 100 * time.Millisecond

// borrowedAuthStagingIncarnationBudget clips the final postmaster incarnation
// check (shared PID/start inspection plus strict container stat) as one bounded
// operation; the caller context always clips it further.
const borrowedAuthStagingIncarnationBudget = 10 * time.Second

// borrowedAuthStagingField extracts one key=value field from a public phase
// marker line. It never prints raw credential material.
func borrowedAuthStagingField(t *testing.T, line, key string) string {
	t.Helper()
	for _, field := range strings.Fields(line) {
		if strings.HasPrefix(field, key+"=") {
			return strings.TrimPrefix(field, key+"=")
		}
	}
	t.Fatalf("phase marker is missing the %s field", key)
	return ""
}

func buildBorrowedAuthStagingHelper(t *testing.T) string {
	t.Helper()
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate borrowed-auth staging test source")
	}
	source := filepath.Join(filepath.Dir(testFile), "borrowed-auth-staging-client_linux_testhelper.go")
	output := filepath.Join(t.TempDir(), "borrowed-auth-staging-client")
	cmd := exec.Command("go", "build", "-trimpath", "-o", output, source)
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	if _, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build borrowed-auth staging helper refused")
	}
	return output
}

// buildBorrowedAuthStagingStatProbe compiles the ignore-tagged stdlib-only
// stat probe as a static binary. It is copied only into the owned fixture
// container at its own private path; it is never copied to the shared fixture
// census path or any entry-private probe path.
func buildBorrowedAuthStagingStatProbe(t *testing.T) string {
	t.Helper()
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate borrowed-auth staging stat probe source")
	}
	source := filepath.Join(filepath.Dir(testFile), "borrowed-auth-staging-statprobe_linux_testhelper.go")
	output := filepath.Join(t.TempDir(), "borrowed-auth-staging-statprobe")
	cmd := exec.Command("go", "build", "-trimpath", "-o", output, source)
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	if _, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build borrowed-auth staging stat probe refused")
	}
	return output
}

// installBorrowedAuthStagingStatProbe installs the errno-preserving stat probe
// into the owned disposable fixture container.
func installBorrowedAuthStagingStatProbe(t *testing.T, fx *originGateFixture) {
	t.Helper()
	probe := buildBorrowedAuthStagingStatProbe(t)
	if err := fx.container.CopyFileToContainer(context.Background(), probe, borrowedAuthStagingStatProbePath, 0o700); err != nil {
		t.Fatalf("copy staging stat probe into the owned fixture: %v", err)
	}
}

// borrowedAuthStagingOutput is the os/exec-owned stdout copier: os/exec drains
// the pipe into this writer and Wait waits for that copy to complete, so output
// completion is explicitly coordinated before the sole Wait returns. It never
// blocks the copier (append + non-blocking notify) and exposes immutable line
// snapshots plus bounded prefix waits.
type borrowedAuthStagingOutput struct {
	mu      sync.Mutex
	pending string
	lines   []string
	notify  chan struct{}
}

func newBorrowedAuthStagingOutput() *borrowedAuthStagingOutput {
	return &borrowedAuthStagingOutput{notify: make(chan struct{}, 1)}
}

func (o *borrowedAuthStagingOutput) Write(p []byte) (int, error) {
	o.mu.Lock()
	o.pending += string(p)
	for {
		index := strings.IndexByte(o.pending, '\n')
		if index < 0 {
			break
		}
		line := strings.TrimRight(o.pending[:index], "\r")
		o.pending = o.pending[index+1:]
		o.lines = append(o.lines, line)
	}
	o.mu.Unlock()
	select {
	case o.notify <- struct{}{}:
	default:
	}
	return len(p), nil
}

func (o *borrowedAuthStagingOutput) snapshot() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.lines...)
}

func (o *borrowedAuthStagingOutput) await(prefix string, timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	for {
		o.mu.Lock()
		for _, line := range o.lines {
			if strings.HasPrefix(line, prefix) {
				o.mu.Unlock()
				return line, nil
			}
		}
		o.mu.Unlock()
		if time.Now().After(deadline) {
			return "", fmt.Errorf("output prefix %q was not observed within %s", prefix, timeout)
		}
		select {
		case <-o.notify:
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// borrowedAuthStagingStderrCounter is a bounded stderr sink: content is never
// retained or printed, only a byte count for safe diagnostics.
type borrowedAuthStagingStderrCounter struct {
	mu    sync.Mutex
	bytes int
}

func (c *borrowedAuthStagingStderrCounter) Write(p []byte) (int, error) {
	c.mu.Lock()
	c.bytes += len(p)
	c.mu.Unlock()
	return len(p), nil
}

// borrowedAuthStagingProcess owns the real docker exec helper process, its
// private stdin (secret + managed commands), the os/exec-owned output copier
// and exactly one sole Wait. The strict container-side self identity is
// captured from HELPER_STARTED before any secret is written.
//
// The retirement hooks are nil in the real harness (the bounded real
// implementation is used) and are injected by the bounded-retirement unit
// tests so every blocking probe/write/wait path can be proven bounded without
// docker. There is deliberately no forced-signal hook: retirement never
// signals a bare container PID.
type borrowedAuthStagingProcess struct {
	t         *testing.T
	cmd       *exec.Cmd
	cancel    context.CancelFunc
	stdin     io.WriteCloser
	out       *borrowedAuthStagingOutput
	stderr    *borrowedAuthStagingStderrCounter
	wait      chan struct{}
	waitMu    sync.Mutex
	waitErr   error
	stdinOnce sync.Once

	selfPID   int
	selfStart uint64
	selfKnown bool

	probeFn     func(ctx context.Context, containerID string, pid int) (string, uint64, error)
	writeFn     func(ctx context.Context, data string) error
	waitQuietFn func(ctx context.Context, timeout time.Duration) bool
}

// startBorrowedAuthStagingProcess starts the helper, registers cleanup
// IMMEDIATELY after cmd.Start (before any secret write or fatal), awaits the
// strict HELPER_STARTED self identity in the owned container namespace, and
// only then writes the private secret through the bounded stdin writer. SELF
// is an identity prerequisite, never AUTH-ready authority.
func startBorrowedAuthStagingProcess(t *testing.T, ctx context.Context, containerID, helperPath string, args []string, secret string) *borrowedAuthStagingProcess {
	t.Helper()
	runCtx, cancel := context.WithCancel(ctx)
	cmd := exec.CommandContext(runCtx, "docker", append([]string{"exec", "-i", containerID, helperPath}, args...)...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		cancel()
		t.Fatalf("helper private stdin: %v", err)
	}
	out := newBorrowedAuthStagingOutput()
	stderr := &borrowedAuthStagingStderrCounter{}
	cmd.Stdout = out
	cmd.Stderr = stderr
	process := &borrowedAuthStagingProcess{t: t, cmd: cmd, cancel: cancel, stdin: stdin, out: out, stderr: stderr, wait: make(chan struct{})}
	if err := cmd.Start(); err != nil {
		cancel()
		t.Fatalf("start helper: %v", err)
	}
	// ST05: cleanup is registered before any secret write or fatal path.
	t.Cleanup(func() { process.cleanup(containerID) })
	go func() {
		err := cmd.Wait() // sole Wait, owned here; output copy completed first
		process.waitMu.Lock()
		process.waitErr = err
		process.waitMu.Unlock()
		close(process.wait)
	}()
	selfLine, err := out.await("HELPER_STARTED ", 30*time.Second)
	if err != nil {
		t.Fatalf("helper never reported HELPER_STARTED before the private secret: %v", err)
	}
	pid, err := strconv.Atoi(borrowedAuthStagingField(t, selfLine, "pid"))
	if err != nil || pid <= 1 {
		t.Fatalf("HELPER_STARTED PID is not a positive process identity")
	}
	start, err := strconv.ParseUint(borrowedAuthStagingField(t, selfLine, "start"), 10, 64)
	if err != nil || start == 0 {
		t.Fatalf("HELPER_STARTED start is not a positive process identity")
	}
	state, observedStart, err := borrowedAuthStagingContainerStat(ctx, containerID, pid)
	if err != nil || observedStart != start || !borrowedAuthStagingLiveState(state) {
		t.Fatalf("HELPER_STARTED identity does not match the live owned container process")
	}
	process.selfPID, process.selfStart, process.selfKnown = pid, start, true
	secretCtx, cancelSecret := context.WithTimeout(ctx, borrowedAuthStagingWriteBudget)
	defer cancelSecret()
	if err := process.writeStdinBounded(secretCtx, secret+"\n"); err != nil {
		t.Fatalf("write private secret line: %v", err)
	}
	return process
}

func (p *borrowedAuthStagingProcess) awaitLine(prefix string, timeout time.Duration) string {
	p.t.Helper()
	line, err := p.out.await(prefix, timeout)
	if err != nil {
		p.t.Fatalf("helper output did not reach %q: %v", prefix, err)
	}
	return line
}

// awaitREADY awaits the actual READY tuple and requires the READY PID/start to
// be exactly the strict HELPER_STARTED self identity.
func (p *borrowedAuthStagingProcess) awaitREADY(timeout time.Duration) (mode, local, remote string) {
	p.t.Helper()
	line := p.awaitLine("READY ", timeout)
	mode = borrowedAuthStagingField(p.t, line, "mode")
	pid, err := strconv.Atoi(borrowedAuthStagingField(p.t, line, "pid"))
	if err != nil || pid != p.selfPID {
		p.t.Fatalf("READY PID does not match the strict HELPER_STARTED identity")
	}
	start, err := strconv.ParseUint(borrowedAuthStagingField(p.t, line, "start"), 10, 64)
	if err != nil || start != p.selfStart {
		p.t.Fatalf("READY start does not match the strict HELPER_STARTED identity")
	}
	local = borrowedAuthStagingField(p.t, line, "local")
	remote = borrowedAuthStagingField(p.t, line, "remote")
	if err := borrowedAuthStagingCanonicalEndpoint(local); err != nil {
		p.t.Fatalf("READY local tuple is not canonical")
	}
	if err := borrowedAuthStagingCanonicalEndpoint(remote); err != nil {
		p.t.Fatalf("READY remote tuple is not canonical")
	}
	return mode, local, remote
}

func (p *borrowedAuthStagingProcess) awaitSASLContinue(timeout time.Duration) {
	p.t.Helper()
	line := p.awaitLine("SASL_CONTINUE ", timeout)
	iterations := borrowedAuthStagingField(p.t, line, "iterations")
	if iterations == "" || iterations == "0" {
		p.t.Fatalf("server SASLContinue has no loaded verifier iterations")
	}
}

func (p *borrowedAuthStagingProcess) quit() {
	p.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), borrowedAuthStagingWriteBudget)
	defer cancel()
	if err := p.writeStdinBounded(ctx, "quit\n"); err != nil {
		p.t.Fatalf("managed quit write refused")
	}
}

func (p *borrowedAuthStagingProcess) exited() bool {
	select {
	case <-p.wait:
		return true
	default:
		return false
	}
}

func (p *borrowedAuthStagingProcess) awaitWaitQuiet(timeout time.Duration) bool {
	select {
	case <-p.wait:
		return true
	case <-time.After(timeout):
		return false
	}
}

func (p *borrowedAuthStagingProcess) awaitWait(timeout time.Duration) error {
	p.t.Helper()
	if !p.awaitWaitQuiet(timeout) {
		p.t.Fatalf("helper sole Wait did not complete")
	}
	p.waitMu.Lock()
	defer p.waitMu.Unlock()
	return p.waitErr
}

// terminal requires exactly one terminal marker after the sole Wait completed
// (output completion is coordinated by the os/exec-owned copier). A managed
// QUIT can never count as server EOF.
func (p *borrowedAuthStagingProcess) terminal() string {
	p.t.Helper()
	marker, err := borrowedAuthStagingSoleTerminal(p.out.snapshot())
	if err != nil {
		p.t.Fatalf("helper terminal markers are not exactly one: %v", err)
	}
	return marker
}

// --- bounded cooperative retirement (no forced bare-PID signal) ---

// writeStdinBounded writes one private line with a bounded wait: the write runs
// in a goroutine and a context end returns the bounded refusal. A blocked
// writer is released by the host-CLI cancel / stdin close that follow, never by
// a forced container signal.
func (p *borrowedAuthStagingProcess) writeStdinBounded(ctx context.Context, data string) error {
	if p.writeFn != nil {
		return p.writeFn(ctx, data)
	}
	if p.stdin == nil {
		return errors.New("helper private stdin is unavailable")
	}
	done := make(chan error, 1)
	go func() {
		_, err := io.WriteString(p.stdin, data)
		done <- err
	}()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// closeStdinBounded closes the private stdin with a bounded wait. Close is
// normally immediate; the bound exists so a blocked close can never strand
// cleanup.
func (p *borrowedAuthStagingProcess) closeStdinBounded(ctx context.Context) {
	if p.stdin == nil {
		return
	}
	done := make(chan struct{})
	go func() {
		p.stdinOnce.Do(func() { _ = p.stdin.Close() })
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
	}
}

// awaitWaitQuietBounded waits for the sole host Wait with both a duration and
// a context bound.
func (p *borrowedAuthStagingProcess) awaitWaitQuietBounded(ctx context.Context, timeout time.Duration) bool {
	if p.waitQuietFn != nil {
		return p.waitQuietFn(ctx, timeout)
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-p.wait:
		return true
	case <-ctx.Done():
		return false
	case <-timer.C:
		return false
	}
}

func (p *borrowedAuthStagingProcess) cancelHost() {
	if p.cancel != nil {
		p.cancel()
	}
}

// retire is the bounded cooperative retirement: managed private quit, then a
// bounded stdin close, then cancel/reap of ONLY the host docker exec CLI on
// timeout. It never sends a forced signal to the container helper by bare PID
// (a PID observed before a possible reuse cannot be signalled safely), and a
// successful host Wait is never a retirement proof: the captured container
// identity must strictly disappear (ENOENT or a validated different positive
// start) or the result is UNKNOWN. The owned disposable container is disposed
// of by the existing fixture cleanup, which is cleanup and not a retirement
// proof. Every failure is returned as an issue string, never silently
// ignored.
func (p *borrowedAuthStagingProcess) retire(containerID string, budget time.Duration) []string {
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	var issues []string
	if !p.exited() {
		if err := p.writeStdinBounded(ctx, "quit\n"); err == nil && p.awaitWaitQuietBounded(ctx, borrowedAuthStagingQuitWaitBudget) {
			p.closeStdinBounded(ctx)
			p.cancelHost()
			if message := p.verifyDisappearanceBounded(ctx, containerID); message != "" {
				issues = append(issues, message)
			}
			return issues
		}
	}
	p.closeStdinBounded(ctx)
	if p.awaitWaitQuietBounded(ctx, borrowedAuthStagingReapWaitBudget) {
		p.cancelHost()
		if message := p.verifyDisappearanceBounded(ctx, containerID); message != "" {
			issues = append(issues, message)
		}
		return issues
	}
	p.cancelHost()
	if !p.awaitWaitQuietBounded(ctx, borrowedAuthStagingReapWaitBudget) {
		issues = append(issues, "staging helper host docker exec CLI did not reap within the bounded cleanup budget")
	}
	if !p.selfKnown {
		issues = append(issues, "staging helper strict container identity was never captured; retirement is UNKNOWN (not claimed)")
		return issues
	}
	if message := p.verifyDisappearanceBounded(ctx, containerID); message != "" {
		issues = append(issues, message)
	}
	return issues
}

func (p *borrowedAuthStagingProcess) verifyDisappearanceBounded(ctx context.Context, containerID string) string {
	if !p.selfKnown {
		return "staging helper strict identity was never captured; retirement is UNKNOWN (not claimed)"
	}
	return borrowedAuthStagingAwaitDisappearanceResult(ctx, containerID, p.selfPID, p.selfStart, borrowedAuthStagingDisappearanceBudget, p.probeFn)
}

// cleanup runs the bounded retirement and records every bounded failure as a
// test error, never ignored.
func (p *borrowedAuthStagingProcess) cleanup(containerID string) {
	p.t.Helper()
	for _, issue := range p.retire(containerID, borrowedAuthStagingCleanupBudget) {
		p.t.Errorf("%s", issue)
	}
}

// --- strict container-side process identity (errno-preserving read probe) ---

var (
	errBorrowedAuthStagingProcessAbsent    = errors.New("staging helper process is absent")
	errBorrowedAuthStagingStatProbeUnknown = errors.New("strict container stat probe is UNKNOWN")
)

// borrowedAuthStagingBoundedProbeExec runs one bounded read-only probe inside
// the owned container with exec.CommandContext. The operation is clipped to
// the remaining parent deadline (context.WithTimeout derives the earlier
// deadline); stderr is never retained or printed (byte count only) and the
// returned error never carries stdout/stderr text. A timeout is an explicit
// UNKNOWN error, never a disappearance.
func borrowedAuthStagingBoundedProbeExec(ctx context.Context, containerID string, args ...string) ([]byte, error) {
	if ctx == nil {
		return nil, errors.New("bounded staging probe requires a context")
	}
	child, cancel := context.WithTimeout(ctx, borrowedAuthStagingProbeBudget)
	defer cancel()
	cmd := exec.CommandContext(child, "docker", append([]string{"exec", containerID}, args...)...)
	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &borrowedAuthStagingStderrCounter{}
	if err := cmd.Run(); err != nil {
		if child.Err() != nil {
			return stdout.Bytes(), child.Err()
		}
		return stdout.Bytes(), err
	}
	return stdout.Bytes(), nil
}

// borrowedAuthStagingClassifyStatProbe classifies one bounded stat probe
// execution. Only the probe's own exact STATPROBE_ABSENT sentinel combined
// with the probe's own exit 44 proves absence; a generic exit code, a
// docker-level failure, EACCES/EIO, a timeout, a contradictory sentinel or a
// malformed/truncated document are UNKNOWN and never gone. The strict
// PID/state/start parse stays on the Go side.
func borrowedAuthStagingClassifyStatProbe(pid int, out []byte, runErr error) (string, uint64, error) {
	if pid < 2 {
		return "", 0, errors.New("strict container stat expected PID is not positive")
	}
	if runErr != nil {
		var exitErr *exec.ExitError
		if errors.As(runErr, &exitErr) &&
			exitErr.ExitCode() == borrowedAuthStagingStatProbeAbsentExit &&
			strings.TrimSpace(string(out)) == borrowedAuthStagingStatProbeAbsentSentinel {
			return "", 0, errBorrowedAuthStagingProcessAbsent
		}
		return "", 0, errBorrowedAuthStagingStatProbeUnknown
	}
	if bytes.Contains(out, []byte("STATPROBE_")) {
		// A sentinel with a zero exit is contradictory: never absence.
		return "", 0, errBorrowedAuthStagingStatProbeUnknown
	}
	state, start, _, err := borrowedAuthStagingParseProcStat(pid, out)
	if err != nil {
		return "", 0, errBorrowedAuthStagingStatProbeUnknown
	}
	return state, start, nil
}

// borrowedAuthStagingContainerStat reads /proc/<pid>/stat inside the owned
// container namespace through the errno-preserving read probe and parses it
// strictly in Go: only the probe's own ENOENT discriminator or a validated
// different positive start can prove disappearance; EACCES/EIO/timeouts and
// malformed output are UNKNOWN and never gone. The older shell `[ -e ]`/cat
// weak parse (where any shell false aliases EACCES into exit 44) is not
// reused.
func borrowedAuthStagingContainerStat(ctx context.Context, containerID string, pid int) (string, uint64, error) {
	if pid < 2 {
		return "", 0, errors.New("strict container stat expected PID is not positive")
	}
	out, err := borrowedAuthStagingBoundedProbeExec(ctx, containerID, borrowedAuthStagingStatProbePath, strconv.Itoa(pid))
	return borrowedAuthStagingClassifyStatProbe(pid, out, err)
}

// borrowedAuthStagingDisappearanceVerdict classifies one strict stat probe
// into a disappearance decision. The expected identity must be positive
// BEFORE any probe interpretation: an invalid expected PID/start is always
// UNKNOWN, never gone, even for an injected Absent or different-start result.
// Only the probe's own ENOENT discriminator or a validated different POSITIVE
// start proves the original instance gone; a same-start dead state is not
// reaped proof; EACCES/EIO/timeout/malformed are UNKNOWN and never gone.
func borrowedAuthStagingDisappearanceVerdict(pid int, expectedStart uint64, state string, observedStart uint64, probeErr error) (gone bool, unknown bool) {
	if pid < 2 || expectedStart == 0 {
		return false, true
	}
	if errors.Is(probeErr, errBorrowedAuthStagingProcessAbsent) {
		return true, false
	}
	if probeErr != nil {
		return false, true
	}
	if observedStart != expectedStart {
		if observedStart > 0 {
			return true, false
		}
		return false, true
	}
	// Same positive start: a recognized live state means "not gone yet"; a
	// dead state is unreaped, not a disappearance proof.
	return false, false
}

type borrowedAuthStagingStatProbeFn func(ctx context.Context, containerID string, pid int) (string, uint64, error)

// borrowedAuthStagingAwaitDisappearanceResult polls the bounded strict probe
// until the captured identity strictly disappears, and returns "" only for a
// proven disappearance. It returns a bounded refusal message otherwise
// (timeout, UNKNOWN probe, same-start dead state or still-live identity);
// nothing here can ever report gone on an unknown probe. The poll context
// derives from the parent, so a remaining parent deadline always clips it.
func borrowedAuthStagingAwaitDisappearanceResult(ctx context.Context, containerID string, pid int, start uint64, timeout time.Duration, probe borrowedAuthStagingStatProbeFn) string {
	if pid < 2 || start == 0 {
		return fmt.Sprintf("staging helper disappearance expected identity is not positive (pid=%d start=%d); refusal is UNKNOWN and never gone", pid, start)
	}
	if probe == nil {
		probe = borrowedAuthStagingContainerStat
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	last := "not probed"
	for {
		state, observed, err := probe(ctx, containerID, pid)
		gone, unknown := borrowedAuthStagingDisappearanceVerdict(pid, start, state, observed, err)
		switch {
		case gone:
			return ""
		case unknown:
			last = "identity probe is UNKNOWN"
		case observed == start && !borrowedAuthStagingLiveState(state):
			last = "same start in a dead state is not a disappearance proof"
		default:
			last = "still live with the retained start"
		}
		select {
		case <-ctx.Done():
			return fmt.Sprintf("staging helper %d/%d did not strictly disappear within %s: %s", pid, start, timeout, last)
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func borrowedAuthStagingAwaitDisappearance(ctx context.Context, t *testing.T, containerID string, pid int, start uint64, timeout time.Duration, fatal bool) {
	t.Helper()
	message := borrowedAuthStagingAwaitDisappearanceResult(ctx, containerID, pid, start, timeout, nil)
	if message == "" {
		return
	}
	if fatal {
		t.Fatalf("%s", message)
	}
	t.Errorf("%s", message)
}

// --- strict backend association (census-only, anonymous pre-auth row) ---

type borrowedAuthStagingBackendToken struct {
	ChildPID   int
	ChildStart uint64
	Inode      string
	Local      string
	Remote     string
}

func borrowedAuthStagingCensusMatches(report borrowedAuthStagingCensus, local, remote string) []borrowedAuthStagingBackendToken {
	var matches []borrowedAuthStagingBackendToken
	for _, child := range report.Children {
		if child.Gone || child.Uncertain {
			continue
		}
		for _, socket := range child.Sockets {
			if socket.Kind == "tcp" && socket.TCP && socket.Local == local && socket.Remote == remote {
				matches = append(matches, borrowedAuthStagingBackendToken{
					ChildPID: child.PID, ChildStart: child.Start, Inode: socket.Inode, Local: socket.Local, Remote: socket.Remote,
				})
			}
		}
	}
	return matches
}

// borrowedAuthStagingStrictCensus runs one strict census through the pure
// consumer with a context derived from the caller and clipped to the
// association budget; the raw exec result is never printed.
func borrowedAuthStagingStrictCensus(t *testing.T, ctx context.Context, fx *originGateFixture) borrowedAuthStagingCensus {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, borrowedAuthStagingAssociationBudget)
	defer cancel()
	out, err := dockerExec(ctx, fx.containerID, borrowedAuthStagingHelperPath, "census",
		strconv.Itoa(fx.postmasterPID), fx.postmasterStr)
	if err != nil {
		t.Fatalf("strict census producer exec refused")
	}
	report, err := parseBorrowedAuthStagingCensus(out, fx.postmasterPID, mustParseStart(t, fx.postmasterStr))
	if err != nil {
		t.Fatalf("strict census consumer refused: %v", err)
	}
	return report
}

// borrowedAuthStagingCensusAttemptFn is the test-only injectable census
// attempt seam for the association deadline negatives. The real association
// always builds the bounded docker exec closure; an injected synthetic attempt
// can only produce refusals and can never mint an association capability.
type borrowedAuthStagingCensusAttemptFn func(ctx context.Context) (borrowedAuthStagingCensus, error)

// borrowedAuthStagingRetryPause is the bounded retry pause: it selects on the
// caller context instead of sleeping, so an immediate cancellation refuses
// promptly and never waits out the wall association window.
func borrowedAuthStagingRetryPause(ctx context.Context) error {
	timer := time.NewTimer(borrowedAuthStagingAssociationRetryInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// borrowedAuthStagingAwaitBackend associates the paused SASL backend through
// the strict census socket tuples only: server local 127.0.0.1:5432 and the
// helper's actual local tuple. Identity is chosen by the census/socket token
// alone: the SQL auth row is a non-capability observation and is deliberately
// never used for association (no W role/appname/idle whitelist), so a missing
// or changed census can never be rescued by a SQL assigned role. Exactly one
// exact match is required; zero matches retry bounded, multiple matches refuse
// as an unknown owner.
//
// The FULL association is caller-context-bounded: the operation context is
// derived from the caller and clipped to the 30s association budget, the
// caller context is checked before every attempt and the retry pause selects
// on it, so a cancellation returns an error before any further attempt (the
// caller wrapper may fail the test) and no canceled exec attempt is ever made.
func borrowedAuthStagingAwaitBackend(t *testing.T, ctx context.Context, fx *originGateFixture, helperLocal string) borrowedAuthStagingBackendToken {
	t.Helper()
	const serverLocal = "127.0.0.1:5432"
	attempt := func(ctx context.Context) (borrowedAuthStagingCensus, error) {
		out, err := dockerExec(ctx, fx.containerID, borrowedAuthStagingHelperPath, "census",
			strconv.Itoa(fx.postmasterPID), fx.postmasterStr)
		if err != nil {
			return borrowedAuthStagingCensus{}, errors.New("strict census exec refused")
		}
		return parseBorrowedAuthStagingCensus(out, fx.postmasterPID, mustParseStart(t, fx.postmasterStr))
	}
	token, err := borrowedAuthStagingAwaitBackendWith(ctx, serverLocal, helperLocal, attempt)
	if err != nil {
		t.Fatalf("%v", err)
	}
	return token
}

// borrowedAuthStagingAwaitBackendWith is the context-bounded association core:
// the operation context derives from the caller and is clipped to the
// association budget (a nil parent is refused, never treated as unbounded), the
// caller context is checked before every attempt and the retry pause selects on
// it, so no canceled exec attempt is ever made and a cancellation returns a
// refusal error promptly. A census error (exec refusal, malformed document or
// unknown report) is retried and can never be treated as valid ownership; only
// an exact single strict socket-tuple match returns a token.
func borrowedAuthStagingAwaitBackendWith(ctx context.Context, serverLocal, helperLocal string, attempt borrowedAuthStagingCensusAttemptFn) (borrowedAuthStagingBackendToken, error) {
	if ctx == nil {
		return borrowedAuthStagingBackendToken{}, errors.New("strict census association refused: no parent context; ownership is UNKNOWN")
	}
	if attempt == nil {
		return borrowedAuthStagingBackendToken{}, errors.New("strict census association refused: no bounded census attempt")
	}
	ctx, cancel := context.WithTimeout(ctx, borrowedAuthStagingAssociationBudget)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return borrowedAuthStagingBackendToken{}, fmt.Errorf("strict census association refused: caller context already done: %w", err)
	}
	last := "no strict census attempt"
	for {
		if err := ctx.Err(); err != nil {
			return borrowedAuthStagingBackendToken{}, fmt.Errorf("strict census never associated the helper auth backend: %s: %w", last, err)
		}
		report, err := attempt(ctx)
		if ctxErr := ctx.Err(); ctxErr != nil {
			// A successful or failed attempt that raced a caller cancellation
			// must never publish a token: refuse with the caller cause before
			// any report/error interpretation. This is a local pre-publication
			// linearization guard only, never a global admission or fence.
			return borrowedAuthStagingBackendToken{}, fmt.Errorf("strict census association refused: caller context done after the census attempt: %w", ctxErr)
		}
		if err != nil {
			last = err.Error()
		} else {
			matches := borrowedAuthStagingCensusMatches(report, serverLocal, helperLocal)
			if len(matches) > 1 {
				return borrowedAuthStagingBackendToken{}, fmt.Errorf("ambiguous auth backend association: %d exact census matches for one socket tuple", len(matches))
			}
			if len(matches) == 1 {
				return matches[0], nil
			}
			last = "no exact census association yet"
		}
		if waitErr := borrowedAuthStagingRetryPause(ctx); waitErr != nil {
			return borrowedAuthStagingBackendToken{}, fmt.Errorf("strict census never associated the helper auth backend: %s: %w", last, waitErr)
		}
	}
}

// borrowedAuthStagingRevalidateBackend re-proves the EXACT retained backend
// token before termination: the same strict census child PID/start/inode/tuples
// with no additional match, the same live container process identity and the
// old captured postmaster incarnation. All operations derive from a bounded
// child context.
func borrowedAuthStagingRevalidateBackend(t *testing.T, ctx context.Context, fx *originGateFixture, token borrowedAuthStagingBackendToken) {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, borrowedAuthStagingAssociationBudget)
	defer cancel()
	report := borrowedAuthStagingStrictCensus(t, ctx, fx)
	matches := borrowedAuthStagingCensusMatches(report, token.Local, token.Remote)
	if len(matches) != 1 || matches[0] != token {
		t.Fatalf("exact backend token changed before termination")
	}
	state, start, err := borrowedAuthStagingContainerStat(ctx, fx.containerID, token.ChildPID)
	if err != nil || start != token.ChildStart || !borrowedAuthStagingLiveState(state) {
		t.Fatalf("backend PID/start is not the retained live server process")
	}
	pid, pmStart, err := inspectPostmaster(ctx, fx.containerID)
	if err != nil || pid != fx.postmasterPID || pmStart != fx.postmasterStr {
		t.Fatalf("captured postmaster incarnation changed before termination")
	}
}

// borrowedAuthStagingAuthRowObservation is the non-capability observation of
// the paused backend's SQL row. It never authorizes and never participates in
// association.
type borrowedAuthStagingAuthRowObservation struct {
	Visible          bool
	RoleAssigned     bool
	DatabaseAssigned bool
	AppNamePresent   bool
}

// borrowedAuthStagingInterpretAuthRow interprets the nullable row fields. It
// has no refusal path: an invisible row is non-authorizing, and a visible row
// with an assigned role and/or a NULL database is accepted as a non-capability
// observation. Anonymity is never required and no zero role is imposed.
func borrowedAuthStagingInterpretAuthRow(visible, roleAssigned, databaseAssigned, appNamePresent bool) borrowedAuthStagingAuthRowObservation {
	if !visible {
		return borrowedAuthStagingAuthRowObservation{}
	}
	return borrowedAuthStagingAuthRowObservation{
		Visible:          true,
		RoleAssigned:     roleAssigned,
		DatabaseAssigned: databaseAssigned,
		AppNamePresent:   appNamePresent,
	}
}

// status is the safe diagnostic form: booleans only, never role/database/
// application_name values.
func (o borrowedAuthStagingAuthRowObservation) status() string {
	if !o.Visible {
		return "row-not-visible"
	}
	return fmt.Sprintf("visible role_assigned=%t database_assigned=%t application_name_present=%t",
		o.RoleAssigned, o.DatabaseAssigned, o.AppNamePresent)
}

// borrowedAuthStagingObservePreAuthRow records the pre-auth SQL observation
// without imposing anonymity: the row may be invisible (non-authorizing) and a
// visible row may already carry an assigned role and a NULL database. The
// actual PostgreSQL path records this observation and never refuses on an
// assigned role. Association identity is chosen by the strict census socket
// token alone; a missing or changed census can never be rescued by a SQL
// assigned role.
func borrowedAuthStagingObservePreAuthRow(t *testing.T, ctx context.Context, fx *originGateFixture, pid int) {
	t.Helper()
	var role, database, appName pgtype.Text
	err := fx.admin.QueryRow(ctx, `SELECT usename::text, datname::text, application_name::text FROM pg_stat_activity WHERE pid=$1`, pid).
		Scan(&role, &database, &appName)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		t.Logf("pre-auth SQL observation (non-capability, never used for association): %s",
			borrowedAuthStagingInterpretAuthRow(false, false, false, false).status())
	case err != nil:
		t.Logf("pre-auth SQL observation unavailable (non-capability, never used for association): query refused")
	default:
		t.Logf("pre-auth SQL observation (non-capability, never used for association): %s",
			borrowedAuthStagingInterpretAuthRow(true, role.Valid, database.Valid, appName.Valid && appName.String != "").status())
	}
}

// TestBorrowedAuthStagingHelperHoldAndServerEOF proves the real staging
// protocol: strict HELPER_STARTED self identity before the private secret,
// prestartup hold with no startup/backend and managed QUIT as CLIENT_QUIT only,
// scram-hold reaching the genuine AuthenticationSASLContinue (loaded W P0
// verifier), strict census association and revalidation of the exact backend
// token (non-capability SQL row observation; old postmaster), then terminating
// only that known backend with the postmaster still running to yield the real
// CLIENT_SERVER_EOF marker (never a managed QUIT or CLIENT_UNKNOWN), exactly
// one sole Wait and a strict container-namespace helper reap. The final
// postmaster check is a strict stat (expected PID/start in a recognized
// non-dead state), not only the shared PID/start inspection. No borrowed run,
// probe or acceptance runs.
func TestBorrowedAuthStagingHelperHoldAndServerEOF(t *testing.T) {
	ctx := context.Background()
	fx := newOriginGateFixture(t)
	installBorrowedAuthStagingStatProbe(t, fx)
	helper := buildBorrowedAuthStagingHelper(t)
	if err := fx.container.CopyFileToContainer(ctx, helper, borrowedAuthStagingHelperPath, 0o700); err != nil {
		t.Fatalf("copy staging helper into the owned fixture: %v", err)
	}
	nano := time.Now().UnixNano()
	targetDB := fmt.Sprintf("borrowed_auth_stage_%d", nano)
	if err := fx.createOwnedDatabase(ctx, targetDB); err != nil {
		t.Fatalf("create staging target database: %v", err)
	}

	// Prestartup hold: no StartupMessage, no server backend. A managed quit is
	// explicitly CLIENT_QUIT, never server-EOF evidence.
	pre := startBorrowedAuthStagingProcess(t, ctx, fx.containerID, borrowedAuthStagingHelperPath,
		[]string{"prestartup-hold", fx.role, targetDB, "127.0.0.1", "5432"}, "unused-secret")
	mode, _, _ := pre.awaitREADY(30 * time.Second)
	if mode != "prestartup-hold" {
		t.Fatalf("prestartup readiness mode is not the requested hold mode")
	}
	pre.quit()
	_ = pre.awaitLine("CLIENT_QUIT", 30*time.Second)
	if err := pre.awaitWait(30 * time.Second); err != nil {
		t.Fatalf("prestartup helper sole Wait failed")
	}
	if marker := pre.terminal(); marker != borrowedAuthStagingTerminalQuit {
		t.Fatalf("prestartup terminal marker is not the managed quit: %q", marker)
	}
	borrowedAuthStagingAwaitDisappearance(ctx, t, fx.containerID, pre.selfPID, pre.selfStart, borrowedAuthStagingDisappearanceBudget, true)

	// SCRAM hold: real startup to the genuine SASLContinue, strict association
	// of the exact backend token, then terminate only that identified backend
	// while the postmaster keeps running.
	scram := startBorrowedAuthStagingProcess(t, ctx, fx.containerID, borrowedAuthStagingHelperPath,
		[]string{"scram-hold", fx.role, targetDB, "127.0.0.1", "5432"}, fx.password)
	mode, helperLocal, _ := scram.awaitREADY(30 * time.Second)
	if mode != "scram-hold" {
		t.Fatalf("scram readiness mode is not the requested hold mode")
	}
	scram.awaitSASLContinue(30 * time.Second)
	token := borrowedAuthStagingAwaitBackend(t, ctx, fx, helperLocal)
	borrowedAuthStagingRevalidateBackend(t, ctx, fx, token)
	borrowedAuthStagingObservePreAuthRow(t, ctx, fx, token.ChildPID)
	var terminated bool
	if err := fx.admin.QueryRow(ctx, `SELECT pg_terminate_backend($1)`, token.ChildPID).Scan(&terminated); err != nil || !terminated {
		t.Fatalf("terminate the identified auth backend refused")
	}
	_ = scram.awaitLine("CLIENT_SERVER_EOF", 30*time.Second)
	if err := scram.awaitWait(30 * time.Second); err != nil {
		t.Fatalf("scram helper sole Wait failed")
	}
	if marker := scram.terminal(); marker != borrowedAuthStagingTerminalServerEOF {
		t.Fatalf("scram terminal marker is not the genuine server EOF: %q", marker)
	}
	borrowedAuthStagingAwaitDisappearance(ctx, t, fx.containerID, scram.selfPID, scram.selfStart, borrowedAuthStagingDisappearanceBudget, true)

	// The postmaster was never stopped and the protected control owner is
	// untouched: no P1 rotation/probe/acceptance/run started here. The final
	// check is strict: the expected actual postmaster PID/start must still be a
	// recognized non-dead state (Z/X/x refuse; T is allowed and R/S may change
	// transiently).
	borrowedAuthStagingAssertPostmasterIncarnation(t, ctx, fx)
	if identity := recovery.DrillNewTargetProcessObservation().StartedIdentity(); identity.Started {
		t.Fatal("staging helper lane started a borrowed writer observation")
	}
}

// borrowedAuthStagingIncarnationInspectFn and
// borrowedAuthStagingIncarnationStatFn are the test-only injectable bounded
// inspection seams for the final incarnation check. The real check always uses
// the shared inspectPostmaster and the strict container stat; an injected
// synthetic inspection can only produce refusals and can never mint a
// live-incarnation capability.
type borrowedAuthStagingIncarnationInspectFn func(ctx context.Context, containerID string) (int, string, error)
type borrowedAuthStagingIncarnationStatFn func(ctx context.Context, containerID string, pid int) (string, uint64, error)

// borrowedAuthStagingAssertPostmasterIncarnation proves the final actual
// postmaster identity: the shared postmaster.pid PID/start inspection plus a
// STRICT container-namespace stat of that same PID with the expected retained
// start in a recognized non-dead state. The shared inspectPostmaster helper is
// deliberately not modified.
func borrowedAuthStagingAssertPostmasterIncarnation(t *testing.T, ctx context.Context, fx *originGateFixture) {
	t.Helper()
	if err := borrowedAuthStagingCheckPostmasterIncarnation(ctx, fx.containerID, fx.postmasterPID, fx.postmasterStr, nil, nil); err != nil {
		t.Fatal(err)
	}
}

// borrowedAuthStagingCheckPostmasterIncarnation is the bounded core of the
// final incarnation check: a caller-clipped operation context is derived
// INSIDE this check before the first call and is used for BOTH the shared
// inspectPostmaster call and the strict container stat, so neither call can run
// unbounded Docker. An expired operation deadline is a refusal (UNKNOWN), never
// a healthy result: the strict stat is not called once the operation deadline
// has expired, even if the preliminary inspection returned the expected
// identity after expiry. A nil parent is refused, never treated as unbounded.
func borrowedAuthStagingCheckPostmasterIncarnation(ctx context.Context, containerID string, expectedPID int, expectedStartRaw string, inspect borrowedAuthStagingIncarnationInspectFn, stat borrowedAuthStagingIncarnationStatFn) error {
	if ctx == nil {
		return errors.New("postmaster incarnation check refused: no parent context; the result is UNKNOWN")
	}
	if inspect == nil {
		inspect = inspectPostmaster
	}
	if stat == nil {
		stat = borrowedAuthStagingContainerStat
	}
	opCtx, cancel := context.WithTimeout(ctx, borrowedAuthStagingIncarnationBudget)
	defer cancel()
	if err := opCtx.Err(); err != nil {
		return fmt.Errorf("postmaster incarnation check is UNKNOWN: caller deadline already expired: %w", err)
	}
	pid, start, err := inspect(opCtx, containerID)
	if err != nil || pid != expectedPID || start != expectedStartRaw {
		return errors.New("captured postmaster incarnation changed during the staging hold")
	}
	if err := opCtx.Err(); err != nil {
		return fmt.Errorf("postmaster incarnation check is UNKNOWN: operation deadline expired after the shared inspection: %w", err)
	}
	expectedStart, err := strconv.ParseUint(expectedStartRaw, 10, 64)
	if err != nil || expectedStart == 0 {
		return errors.New("postmaster start identity is invalid")
	}
	state, observedStart, err := stat(opCtx, containerID, expectedPID)
	if ctxErr := opCtx.Err(); ctxErr != nil {
		// A strict stat that raced an operation-deadline expiration or a caller
		// cancellation must never return a healthy incarnation, even with the
		// expected state/start.
		return fmt.Errorf("postmaster incarnation check is UNKNOWN: operation context done after the strict stat: %w", ctxErr)
	}
	if err != nil || observedStart != expectedStart || !borrowedAuthStagingLiveState(state) {
		return errors.New("strict postmaster state after helper EOF/reap is not the expected live incarnation")
	}
	return nil
}

// TestBorrowedAuthStagingCensusProducerLive runs the strict census producer
// inside the owned fixture with the real postmaster identity, brackets the
// census with a strict container-namespace postmaster stat (recognized live
// state, retained start before/after) and validates the immutable report
// through the strict consumer. Racy background FD changes may make a single
// scan conservative-incomplete; bounded retries are used and a persistent
// incomplete report is a genuine failure, never a weakened scope.
func TestBorrowedAuthStagingCensusProducerLive(t *testing.T) {
	ctx := context.Background()
	fx := newOriginGateFixture(t)
	installBorrowedAuthStagingStatProbe(t, fx)
	helper := buildBorrowedAuthStagingHelper(t)
	if err := fx.container.CopyFileToContainer(ctx, helper, borrowedAuthStagingHelperPath, 0o700); err != nil {
		t.Fatalf("copy census helper into the owned fixture: %v", err)
	}
	expectedStart := mustParseStart(t, fx.postmasterStr)
	stateBefore, startBefore, err := borrowedAuthStagingContainerStat(ctx, fx.containerID, fx.postmasterPID)
	if err != nil || startBefore != expectedStart || !borrowedAuthStagingLiveState(stateBefore) {
		t.Fatalf("postmaster is not the expected live process before the census")
	}
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		attemptCtx, cancelAttempt := context.WithTimeout(ctx, borrowedAuthStagingAssociationBudget)
		out, err := dockerExec(attemptCtx, fx.containerID, borrowedAuthStagingHelperPath, "census",
			strconv.Itoa(fx.postmasterPID), fx.postmasterStr)
		cancelAttempt()
		if err != nil {
			lastErr = errors.New("census producer exec refused")
			time.Sleep(100 * time.Millisecond)
			continue
		}
		report, err := parseBorrowedAuthStagingCensus(out, fx.postmasterPID, expectedStart)
		if err != nil {
			lastErr = err
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if len(report.Children) == 0 {
			lastErr = errors.New("census report has no direct postmaster children")
			continue
		}
		stateAfter, startAfter, err := borrowedAuthStagingContainerStat(ctx, fx.containerID, fx.postmasterPID)
		if err != nil || startAfter != expectedStart || !borrowedAuthStagingLiveState(stateAfter) {
			t.Fatalf("postmaster is not the expected live process after the census")
		}
		t.Logf("live census: children=%d sockets=%d enoent=%d", len(report.Children), len(report.Sockets), report.ChildENOENT)
		return
	}
	t.Fatalf("live census never produced a complete strict report: %v", lastErr)
}

func mustParseStart(t *testing.T, raw string) uint64 {
	t.Helper()
	start, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || start == 0 {
		t.Fatalf("postmaster start identity is invalid")
	}
	return start
}
