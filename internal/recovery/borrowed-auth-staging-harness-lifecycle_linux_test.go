//go:build linux && drill

// borrowed-auth-staging-harness-lifecycle_linux_test.go holds the pure
// harness-side proofs for the ST04/ST05 residuals. Nothing here touches a
// fixture, a PostgreSQL process or the producer: the injected negatives are
// labeled as such and can never mint an authorization capability.
//
//	ST05A errno-preserving stat probe: only the probe's own exact
//	      STATPROBE_ABSENT sentinel + exit 44 is positive absence; EACCES/EIO/
//	      timeout/malformed documents are UNKNOWN and never gone. A generic
//	      exit code, a failed cat or a shell boolean can never prove absence.
//	ST05B disappearance verdict: expected PID>=2 and start>0 are validated
//	      BEFORE any probe interpretation (an injected Absent/different start
//	      cannot bypass it); a validated different positive start may prove
//	      the original gone; a same-start Z is not reaped; an unknown state is
//	      never gone.
//	ST05C bounded retirement: blocking probe/write/wait hooks observe refusal
//	      within the cleanup budget, record UNKNOWN test-error issues and are
//	      released (no stranded goroutines). No forced signal exists.
//	ST05E execution deadlines: the final incarnation check derives a
//	      caller-clipped operation context before its first call (an expired
//	      deadline refuses and never runs the strict stat) and the FULL
//	      backend association is caller-context-bounded (no wall sleep, no
//	      attempt after cancellation, a blocking census attempt is clipped by
//	      the child context). Injected attempts are negative-only.
//	ST04 pre-auth SQL row: a visible assigned role and a NULL database are an
//	      accepted non-capability observation; association identity is chosen
//	      by the strict census socket token alone.
package recovery_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// generatedStatProbeUnitTests exercises the ignore-tagged stdlib-only stat
// probe source as a package-main unit: errno classification, errno
// preservation, the raw read pass-through and the malformed-invocation
// refusal. ENOENT is the only absence discriminator.
const generatedStatProbeUnitTests = `package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestStatProbeUnitClassification(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want int
	}{
		{"nil-is-ok", nil, statProbeExitOK},
		{"enoent-is-absent", fs.ErrNotExist, statProbeExitAbsent},
		{"wrapped-enoent-is-absent", fmt.Errorf("read stat: %w", fs.ErrNotExist), statProbeExitAbsent},
		{"eacces-is-unknown", syscall.EACCES, statProbeExitUnknown},
		{"eio-is-unknown", syscall.EIO, statProbeExitUnknown},
		{"wrapped-eacces-is-unknown", fmt.Errorf("read stat: %w", syscall.EACCES), statProbeExitUnknown},
		{"deadline-is-unknown", context.DeadlineExceeded, statProbeExitUnknown},
	}
	for _, testCase := range cases {
		if got := classifyStatError(testCase.err); got != testCase.want {
			t.Fatalf("%s: classify=%d want=%d", testCase.name, got, testCase.want)
		}
	}
}

func TestStatProbeUnitErrnoPreserved(t *testing.T) {
	if got := statProbeErrno(fmt.Errorf("read stat: %w", syscall.EACCES)); got != uintptr(syscall.EACCES) {
		t.Fatalf("EACCES errno was not preserved: %d", got)
	}
	if got := statProbeErrno(fmt.Errorf("read stat: %w", syscall.EIO)); got != uintptr(syscall.EIO) {
		t.Fatalf("EIO errno was not preserved: %d", got)
	}
	if got := statProbeErrno(fs.ErrNotExist); got != 0 {
		t.Fatalf("non-errno error reported errno %d", got)
	}
	if got := statProbeErrno(errors.New("plain")); got != 0 {
		t.Fatalf("plain error reported errno %d", got)
	}
}

func TestStatProbeUnitRun(t *testing.T) {
	root := t.TempDir()
	var absent bytes.Buffer
	if code := run([]string{"4242", root}, &absent); code != statProbeExitAbsent || absent.String() != statProbeAbsentSentinel+"\n" {
		t.Fatalf("missing proc entry: code=%d out=%q", code, absent.String())
	}
	if err := os.MkdirAll(filepath.Join(root, "4243"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "4243", "stat"), []byte("4243 (x) S 1 2 3"), 0o600); err != nil {
		t.Fatal(err)
	}
	var raw bytes.Buffer
	if code := run([]string{"4243", root}, &raw); code != statProbeExitOK || raw.String() != "4243 (x) S 1 2 3" {
		t.Fatalf("raw stat passthrough: code=%d out=%q", code, raw.String())
	}
	for _, args := range [][]string{{}, {"0"}, {"-1"}, {"not-a-pid"}, {"4242", "root", "extra"}} {
		if code := run(args, io.Discard); code != statProbeExitUnknown {
			t.Fatalf("malformed invocation %v: code=%d want=%d", args, code, statProbeExitUnknown)
		}
	}
}
`

// TestBorrowedAuthStagingStatProbeUnit compiles the ignore-tagged stat probe
// source with generated package-main unit tests in a temporary directory
// (explicit-file go test, ignore-tag safe) and proves the errno-preserving
// classification: only a real fs.ErrNotExist is absence, everything else is
// UNKNOWN.
func TestBorrowedAuthStagingStatProbeUnit(t *testing.T) {
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate staging lifecycle test source")
	}
	source := filepath.Join(filepath.Dir(testFile), "borrowed-auth-staging-statprobe_linux_testhelper.go")
	helperSource, err := os.ReadFile(source)
	if err != nil {
		t.Fatalf("read staging stat probe source: %v", err)
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "statprobe.go"), helperSource, 0o600); err != nil {
		t.Fatalf("write stat probe copy: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "statprobe_unit_test.go"), []byte(generatedStatProbeUnitTests), 0o600); err != nil {
		t.Fatalf("write generated stat probe unit tests: %v", err)
	}
	cmd := exec.Command("go", "test", "-count=1", "-run", "TestStatProbeUnit", "statprobe.go", "statprobe_unit_test.go")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("generated stat probe unit tests failed: %v\n%s", err, strings.TrimSpace(string(out)))
	}
}

// TestBorrowedAuthStagingStatProbeRealBinary proves the built probe's actual
// process contract: a real ENOENT read reports the exact sentinel with exit
// 44 (positive absence), and a malformed invocation is UNKNOWN with exit 45,
// never absence. Real-UID EACCES is optional and not required here.
func TestBorrowedAuthStagingStatProbeRealBinary(t *testing.T) {
	probe := buildBorrowedAuthStagingStatProbe(t)
	// A PID far above pid_max cannot exist, so the read is a real ENOENT.
	cmd := exec.Command(probe, "2147483646")
	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	var exitErr *exec.ExitError
	if err := cmd.Run(); !errors.As(err, &exitErr) || exitErr.ExitCode() != borrowedAuthStagingStatProbeAbsentExit ||
		strings.TrimSpace(stdout.String()) != borrowedAuthStagingStatProbeAbsentSentinel {
		t.Fatalf("real ENOENT probe did not report the positive absence discriminator: exit=%v out=%q", err, stdout.String())
	}
	stdout.Reset()
	cmd = exec.Command(probe, "not-a-pid")
	cmd.Stdout = &stdout
	exitErr = nil
	if err := cmd.Run(); !errors.As(err, &exitErr) || exitErr.ExitCode() != 45 ||
		!strings.HasPrefix(strings.TrimSpace(stdout.String()), "STATPROBE_UNKNOWN") {
		t.Fatalf("malformed probe invocation did not report UNKNOWN: exit=%v out=%q", err, stdout.String())
	}
}

// borrowedAuthStagingExitError returns a real *exec.ExitError with the wanted
// exit code for the injected harness classification negatives.
func borrowedAuthStagingExitError(t *testing.T, code int) error {
	t.Helper()
	err := exec.Command("sh", "-c", fmt.Sprintf("exit %d", code)).Run()
	if err == nil {
		t.Fatalf("expected a nonzero exit error for code %d", code)
	}
	return err
}

// TestBorrowedAuthStagingStatProbeHarnessClassification proves the harness
// classifier: only the probe's own exact sentinel + exit 44 is positive
// absence; a generic exit code, a contradictory sentinel, EACCES/EIO, a
// timeout and a malformed document are UNKNOWN and never gone.
func TestBorrowedAuthStagingStatProbeHarnessClassification(t *testing.T) {
	validStat := []byte("4242 (pg helper (x)) S 7 4242 4242 0 -1 4194304 0 0 0 0 0 0 0 0 20 0 1 0 12345 0 0")
	exit44 := borrowedAuthStagingExitError(t, 44)
	exit45 := borrowedAuthStagingExitError(t, 45)
	exit1 := borrowedAuthStagingExitError(t, 1)
	absent := func(out []byte, runErr error) bool {
		_, _, probeErr := borrowedAuthStagingClassifyStatProbe(4242, out, runErr)
		return errors.Is(probeErr, errBorrowedAuthStagingProcessAbsent)
	}
	unknown := func(out []byte, runErr error) bool {
		_, _, probeErr := borrowedAuthStagingClassifyStatProbe(4242, out, runErr)
		return probeErr != nil && !errors.Is(probeErr, errBorrowedAuthStagingProcessAbsent)
	}
	if !absent([]byte("STATPROBE_ABSENT\n"), exit44) {
		t.Fatal("the probe's own ENOENT sentinel + exit 44 was not the positive absence discriminator")
	}
	if !unknown([]byte("STATPROBE_UNKNOWN errno=13\n"), exit44) {
		t.Fatal("a generic exit 44 without the sentinel was treated as absence")
	}
	if !unknown(nil, exit44) {
		t.Fatal("an empty exit 44 result was treated as absence")
	}
	if !unknown([]byte("STATPROBE_UNKNOWN errno=5\n"), exit45) {
		t.Fatal("an exit 45 unknown result was treated as absence")
	}
	if !unknown(nil, exit1) {
		t.Fatal("a generic exit 1 failure was treated as absence")
	}
	if !unknown(nil, context.DeadlineExceeded) {
		t.Fatal("a probe timeout was treated as absence")
	}
	if !unknown(nil, fmt.Errorf("probe exec: %w", syscall.EACCES)) {
		t.Fatal("an EACCES probe failure was treated as absence")
	}
	if !unknown(nil, fmt.Errorf("probe exec: %w", syscall.EIO)) {
		t.Fatal("an EIO probe failure was treated as absence")
	}
	if !unknown([]byte("STATPROBE_ABSENT\n"), nil) {
		t.Fatal("a contradictory sentinel with a zero exit was treated as absence")
	}
	if !unknown([]byte("not a stat document"), nil) {
		t.Fatal("a malformed stat document was treated as absence")
	}
	state, start, err := borrowedAuthStagingClassifyStatProbe(4242, validStat, nil)
	if err != nil || state != "S" || start != 12345 {
		t.Fatalf("valid stat document was not parsed strictly: state=%q start=%d err=%v", state, start, err)
	}
	if _, _, err := borrowedAuthStagingClassifyStatProbe(1, validStat, nil); err == nil {
		t.Fatal("a nonpositive expected PID was accepted")
	}
}

// TestBorrowedAuthStagingDisappearanceVerdict proves ST05B: the expected
// identity is validated before any probe interpretation, a validated
// different positive start may prove the original gone, a same-start Z is not
// reaped and an unknown probe is never gone.
func TestBorrowedAuthStagingDisappearanceVerdict(t *testing.T) {
	cases := []struct {
		name          string
		pid           int
		expectedStart uint64
		state         string
		observedStart uint64
		probeErr      error
		gone          bool
		unknown       bool
	}{
		{"absent-valid-identity", 4242, 99, "", 0, errBorrowedAuthStagingProcessAbsent, true, false},
		{"replacement-positive-start", 4242, 99, "S", 100, nil, true, false},
		{"same-start-live", 4242, 99, "S", 99, nil, false, false},
		{"same-start-stopped", 4242, 99, "T", 99, nil, false, false},
		{"same-start-zombie-not-reaped", 4242, 99, "Z", 99, nil, false, false},
		{"same-start-dead-x", 4242, 99, "X", 99, nil, false, false},
		{"same-start-unrecognized", 4242, 99, "?", 99, nil, false, false},
		{"probe-eacces-unknown", 4242, 99, "", 0, fmt.Errorf("probe: %w", syscall.EACCES), false, true},
		{"probe-eio-unknown", 4242, 99, "", 0, fmt.Errorf("probe: %w", syscall.EIO), false, true},
		{"probe-timeout-unknown", 4242, 99, "", 0, context.DeadlineExceeded, false, true},
		{"probe-malformed-unknown", 4242, 99, "", 0, errors.New("strict container stat parse is UNKNOWN"), false, true},
		{"different-zero-start-unknown", 4242, 99, "S", 0, nil, false, true},
		{"absent-pid-one-refused", 1, 99, "", 0, errBorrowedAuthStagingProcessAbsent, false, true},
		{"absent-pid-zero-refused", 0, 99, "", 0, errBorrowedAuthStagingProcessAbsent, false, true},
		{"absent-negative-pid-refused", -1, 99, "", 0, errBorrowedAuthStagingProcessAbsent, false, true},
		{"absent-zero-start-refused", 4242, 0, "", 0, errBorrowedAuthStagingProcessAbsent, false, true},
		{"replacement-zero-start-refused", 4242, 0, "S", 100, nil, false, true},
		{"replacement-negative-pid-refused", -1, 99, "S", 100, nil, false, true},
	}
	for _, testCase := range cases {
		gone, unknown := borrowedAuthStagingDisappearanceVerdict(
			testCase.pid, testCase.expectedStart, testCase.state, testCase.observedStart, testCase.probeErr)
		if gone != testCase.gone || unknown != testCase.unknown {
			t.Fatalf("%s: gone=%t unknown=%t want gone=%t unknown=%t", testCase.name, gone, unknown, testCase.gone, testCase.unknown)
		}
	}
	// Injected Absent and validated replacement results prove retirement
	// without docker; a blocking probe is bounded, UNKNOWN and released.
	if message := borrowedAuthStagingAwaitDisappearanceResult(context.Background(), "synthetic", 4242, 99, time.Second,
		func(context.Context, string, int) (string, uint64, error) {
			return "", 0, errBorrowedAuthStagingProcessAbsent
		}); message != "" {
		t.Fatalf("injected absent did not prove disappearance: %s", message)
	}
	if message := borrowedAuthStagingAwaitDisappearanceResult(context.Background(), "synthetic", 4242, 99, time.Second,
		func(context.Context, string, int) (string, uint64, error) { return "S", 100, nil }); message != "" {
		t.Fatalf("validated replacement start did not prove disappearance: %s", message)
	}
	var released atomic.Bool
	start := time.Now()
	message := borrowedAuthStagingAwaitDisappearanceResult(context.Background(), "synthetic", 4242, 99, 300*time.Millisecond,
		func(ctx context.Context, _ string, _ int) (string, uint64, error) {
			<-ctx.Done()
			released.Store(true)
			return "", 0, ctx.Err()
		})
	if message == "" || !strings.Contains(message, "UNKNOWN") {
		t.Fatalf("blocking probe was not reported as bounded UNKNOWN: %q", message)
	}
	if !released.Load() {
		t.Fatal("blocking probe was not released by the bounded disappearance poll")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("blocking probe poll was not bounded: %s", elapsed)
	}
	if message := borrowedAuthStagingAwaitDisappearanceResult(context.Background(), "synthetic", 0, 99, 50*time.Millisecond,
		func(context.Context, string, int) (string, uint64, error) {
			return "", 0, errBorrowedAuthStagingProcessAbsent
		}); !strings.Contains(message, "UNKNOWN") {
		t.Fatalf("invalid expected identity with an injected absent probe was not refused: %q", message)
	}
}

// borrowedAuthStagingNopWriteCloser is a synthetic private stdin for the
// bounded-retirement hooks; it never blocks.
type borrowedAuthStagingNopWriteCloser struct{}

func (borrowedAuthStagingNopWriteCloser) Write(p []byte) (int, error) { return len(p), nil }
func (borrowedAuthStagingNopWriteCloser) Close() error                { return nil }

// TestBorrowedAuthStagingBoundedRetirement proves ST05C/ST05D: retirement is
// cooperative and bounded, a blocking probe/write/wait observes refusal within
// the budget and records UNKNOWN test-error issues, a validated replacement is
// never signalled, host Wait success alone can never report retirement, and an
// unknown identity before SELF is never positive. No forced signal exists in
// the harness retirement path at all.
func TestBorrowedAuthStagingBoundedRetirement(t *testing.T) {
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate staging lifecycle test source")
	}
	helperSource, err := os.ReadFile(filepath.Join(filepath.Dir(testFile), "borrowed-auth-staging-helper_linux_test.go"))
	if err != nil {
		t.Fatalf("read staging helper harness source: %v", err)
	}
	for _, forbidden := range []string{"kill -9", "pkill", "syscall.Kill", "SIGKILL"} {
		if bytes.Contains(helperSource, []byte(forbidden)) {
			t.Fatalf("staging retirement harness still contains a forced-signal path %q", forbidden)
		}
	}
	newSynthetic := func() *borrowedAuthStagingProcess {
		return &borrowedAuthStagingProcess{
			t: t, wait: make(chan struct{}), stdin: borrowedAuthStagingNopWriteCloser{}, cancel: func() {},
			selfPID: 4242, selfStart: 99, selfKnown: true,
		}
	}
	t.Run("blocking-probe-write-wait-is-bounded-and-unknown", func(t *testing.T) {
		p := newSynthetic()
		var writeReleased, probeReleased atomic.Bool
		p.writeFn = func(ctx context.Context, data string) error {
			<-ctx.Done()
			writeReleased.Store(true)
			return ctx.Err()
		}
		p.waitQuietFn = func(ctx context.Context, timeout time.Duration) bool {
			<-ctx.Done()
			return false
		}
		p.probeFn = func(ctx context.Context, containerID string, pid int) (string, uint64, error) {
			<-ctx.Done()
			probeReleased.Store(true)
			return "", 0, ctx.Err()
		}
		start := time.Now()
		issues := p.retire("synthetic", 400*time.Millisecond)
		if elapsed := time.Since(start); elapsed > 5*time.Second {
			t.Fatalf("bounded retirement took %s", elapsed)
		}
		joined := strings.Join(issues, "\n")
		if len(issues) == 0 || !strings.Contains(joined, "UNKNOWN") || !strings.Contains(joined, "did not reap") {
			t.Fatalf("bounded retirement did not record the expected UNKNOWN/test-error issues: %v", issues)
		}
		if !writeReleased.Load() || !probeReleased.Load() {
			t.Fatal("a blocked retirement hook was not released within the bounded budget")
		}
	})
	t.Run("validated-replacement-is-never-signalled", func(t *testing.T) {
		p := newSynthetic()
		p.writeFn = func(ctx context.Context, data string) error { return errors.New("private stdin write refused") }
		p.waitQuietFn = func(ctx context.Context, timeout time.Duration) bool { return true }
		p.probeFn = func(ctx context.Context, containerID string, pid int) (string, uint64, error) { return "S", 100, nil }
		issues := p.retire("synthetic", 2*time.Second)
		if len(issues) != 0 {
			t.Fatalf("validated replacement retirement recorded issues: %v", issues)
		}
	})
	t.Run("host-wait-success-with-live-helper-cannot-report-retirement", func(t *testing.T) {
		p := newSynthetic()
		p.writeFn = func(ctx context.Context, data string) error { return errors.New("private stdin write refused") }
		p.waitQuietFn = func(ctx context.Context, timeout time.Duration) bool { return true }
		p.probeFn = func(ctx context.Context, containerID string, pid int) (string, uint64, error) { return "S", 99, nil }
		start := time.Now()
		issues := p.retire("synthetic", 600*time.Millisecond)
		if elapsed := time.Since(start); elapsed > 5*time.Second {
			t.Fatalf("live-helper retirement was not bounded: %s", elapsed)
		}
		if joined := strings.Join(issues, "\n"); !strings.Contains(joined, "did not strictly disappear") {
			t.Fatalf("host Wait success with a still-live helper was not refused as UNKNOWN: %v", issues)
		}
	})
	t.Run("unknown-before-self-is-never-positive", func(t *testing.T) {
		p := newSynthetic()
		p.selfKnown = false
		p.writeFn = func(ctx context.Context, data string) error { return errors.New("private stdin write refused") }
		p.waitQuietFn = func(ctx context.Context, timeout time.Duration) bool { return false }
		p.probeFn = func(ctx context.Context, containerID string, pid int) (string, uint64, error) {
			return "", 0, errBorrowedAuthStagingProcessAbsent
		}
		issues := p.retire("synthetic", 600*time.Millisecond)
		if joined := strings.Join(issues, "\n"); !strings.Contains(joined, "UNKNOWN (not claimed)") {
			t.Fatalf("pre-SELF retirement was not UNKNOWN: %v", issues)
		}
	})
}

// TestBorrowedAuthStagingAuthRowObservationNonCapability proves ST04: the
// pre-auth SQL row observation has no anonymity requirement. A visible row
// with an assigned role and a NULL database is accepted as a non-capability
// observation, an invisible row is non-authorizing and a NULL role/database
// row is equally accepted; nothing here can refuse on an assigned role and
// nothing here participates in association.
func TestBorrowedAuthStagingAuthRowObservationNonCapability(t *testing.T) {
	observed := borrowedAuthStagingInterpretAuthRow(true, true, false, false)
	if !observed.Visible || !observed.RoleAssigned || observed.DatabaseAssigned || observed.AppNamePresent {
		t.Fatalf("visible assigned-role observation was not preserved: %+v", observed)
	}
	if got := observed.status(); got != "visible role_assigned=true database_assigned=false application_name_present=false" {
		t.Fatalf("assigned-role observation status refuses or hides the row: %q", got)
	}
	invisible := borrowedAuthStagingInterpretAuthRow(false, true, true, true)
	if invisible.Visible || invisible.RoleAssigned || invisible.DatabaseAssigned || invisible.AppNamePresent {
		t.Fatalf("invisible row observation is not empty: %+v", invisible)
	}
	if got := invisible.status(); got != "row-not-visible" {
		t.Fatalf("invisible row status: %q", got)
	}
	nullRow := borrowedAuthStagingInterpretAuthRow(true, false, false, false)
	if !nullRow.Visible || nullRow.RoleAssigned || nullRow.DatabaseAssigned || nullRow.AppNamePresent {
		t.Fatalf("NULL role/database observation was not accepted: %+v", nullRow)
	}
}

// TestBorrowedAuthStagingPostmasterIncarnationDeadline proves the final
// incarnation check derives a caller-clipped operation context INSIDE the check
// before its first call and uses it for BOTH the shared inspection and the
// strict stat: an expired operation deadline is a refusal (UNKNOWN), never a
// healthy result, and the strict stat is not called once the operation deadline
// has expired even if the preliminary inspection returned the expected identity
// after expiry. The injected inspection seams are negative-only and mint no
// capability.
func TestBorrowedAuthStagingPostmasterIncarnationDeadline(t *testing.T) {
	t.Run("expired-preliminary-refuses-without-strict-stat", func(t *testing.T) {
		parent, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		var released atomic.Bool
		var strictCalls atomic.Int32
		inspect := func(ctx context.Context, containerID string) (int, string, error) {
			<-ctx.Done()
			released.Store(true)
			// A syntactically expected identity returned after the operation
			// deadline must never produce a healthy result.
			return 4242, "99", nil
		}
		stat := func(ctx context.Context, containerID string, pid int) (string, uint64, error) {
			strictCalls.Add(1)
			return "S", 99, nil
		}
		start := time.Now()
		err := borrowedAuthStagingCheckPostmasterIncarnation(parent, "synthetic", 4242, "99", inspect, stat)
		elapsed := time.Since(start)
		if err == nil {
			t.Fatal("an expired preliminary inspection produced a healthy incarnation result")
		}
		if !strings.Contains(err.Error(), "UNKNOWN") {
			t.Fatalf("expired preliminary refusal is not UNKNOWN: %v", err)
		}
		if strictCalls.Load() != 0 {
			t.Fatalf("strict stat ran after the operation deadline expired: calls=%d", strictCalls.Load())
		}
		if !released.Load() {
			t.Fatal("blocked preliminary inspection was not released by the caller-clipped operation context")
		}
		if elapsed > 5*time.Second {
			t.Fatalf("caller-clipped incarnation check was not bounded: %s", elapsed)
		}
	})
	t.Run("already-expired-parent-refuses-before-inspection", func(t *testing.T) {
		parent, cancel := context.WithCancel(context.Background())
		cancel()
		var inspectCalls, strictCalls atomic.Int32
		inspect := func(context.Context, string) (int, string, error) {
			inspectCalls.Add(1)
			return 4242, "99", nil
		}
		stat := func(context.Context, string, int) (string, uint64, error) {
			strictCalls.Add(1)
			return "S", 99, nil
		}
		if err := borrowedAuthStagingCheckPostmasterIncarnation(parent, "synthetic", 4242, "99", inspect, stat); err == nil {
			t.Fatal("an already-expired caller context produced a healthy incarnation result")
		}
		if inspectCalls.Load() != 0 || strictCalls.Load() != 0 {
			t.Fatalf("expired caller context still ran inspections: inspect=%d strict=%d", inspectCalls.Load(), strictCalls.Load())
		}
	})
	t.Run("nil-parent-refused", func(t *testing.T) {
		if err := borrowedAuthStagingCheckPostmasterIncarnation(nil, "synthetic", 4242, "99", nil, nil); err == nil {
			t.Fatal("nil parent context was accepted for the incarnation check")
		}
	})
}

// TestBorrowedAuthStagingAwaitBackendDeadline proves the FULL association is
// caller-context-bounded: a short caller deadline clips the 30s association
// window so repeated negative census attempts never wait out the wall window,
// and a census error (exec refusal, malformed document or unknown report) is
// retried but never treated as valid ownership.
func TestBorrowedAuthStagingAwaitBackendDeadline(t *testing.T) {
	t.Run("short-parent-clips-the-association-window", func(t *testing.T) {
		parent, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		var attempts atomic.Int32
		attempt := func(context.Context) (borrowedAuthStagingCensus, error) {
			attempts.Add(1)
			return borrowedAuthStagingCensus{}, nil // zero strict socket-tuple matches
		}
		start := time.Now()
		token, err := borrowedAuthStagingAwaitBackendWith(parent, "127.0.0.1:5432", "127.0.0.1:40123", attempt)
		elapsed := time.Since(start)
		if err == nil {
			t.Fatal("an expired caller context still associated a synthetic backend")
		}
		if token != (borrowedAuthStagingBackendToken{}) {
			t.Fatalf("expired association returned a non-zero token: %+v", token)
		}
		if attempts.Load() < 1 {
			t.Fatal("no bounded census attempt was made before the caller deadline")
		}
		if elapsed > 5*time.Second {
			t.Fatalf("short caller deadline did not clip the association window: %s", elapsed)
		}
	})
	t.Run("census-errors-are-retried-never-owned", func(t *testing.T) {
		parent, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		var attempts atomic.Int32
		attempt := func(context.Context) (borrowedAuthStagingCensus, error) {
			attempts.Add(1)
			return borrowedAuthStagingCensus{}, errors.New("census report is malformed")
		}
		token, err := borrowedAuthStagingAwaitBackendWith(parent, "127.0.0.1:5432", "127.0.0.1:40123", attempt)
		if err == nil || token != (borrowedAuthStagingBackendToken{}) {
			t.Fatalf("a malformed/unknown census attempt was treated as valid ownership: token=%+v err=%v", token, err)
		}
		if attempts.Load() < 1 {
			t.Fatal("no bounded census attempt was made for the malformed census")
		}
	})
}

// TestBorrowedAuthStagingAwaitBackendCancellation proves an immediate
// cancellation refuses promptly: the caller context is checked before every
// attempt, the retry pause selects on it instead of sleeping out the wall
// window, and no further attempt runs after cancellation.
func TestBorrowedAuthStagingAwaitBackendCancellation(t *testing.T) {
	t.Run("cancel-while-paused-retry-refuses-promptly", func(t *testing.T) {
		parent, cancel := context.WithCancel(context.Background())
		defer cancel()
		first := make(chan struct{})
		var attempts atomic.Int32
		attempt := func(context.Context) (borrowedAuthStagingCensus, error) {
			if attempts.Add(1) == 1 {
				close(first)
			}
			return borrowedAuthStagingCensus{}, nil // zero matches -> bounded retry pause
		}
		done := make(chan error, 1)
		start := time.Now()
		go func() {
			_, err := borrowedAuthStagingAwaitBackendWith(parent, "127.0.0.1:5432", "127.0.0.1:40123", attempt)
			done <- err
		}()
		<-first
		cancel()
		select {
		case err := <-done:
			if err == nil {
				t.Fatal("cancellation while paused in retry did not refuse")
			}
			if attempts.Load() != 1 {
				t.Fatalf("association attempts continued after cancellation: %d", attempts.Load())
			}
		case <-time.After(5 * time.Second):
			t.Fatal("cancellation while paused in retry did not return promptly")
		}
		if elapsed := time.Since(start); elapsed > 5*time.Second {
			t.Fatalf("cancellation refusal was not prompt: %s", elapsed)
		}
	})
	t.Run("already-canceled-parent-no-attempt", func(t *testing.T) {
		parent, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := borrowedAuthStagingAwaitBackendWith(parent, "127.0.0.1:5432", "127.0.0.1:40123", func(context.Context) (borrowedAuthStagingCensus, error) {
			t.Fatal("a census attempt ran with an already-canceled caller context")
			return borrowedAuthStagingCensus{}, nil
		}); err == nil {
			t.Fatal("an already-canceled caller context was accepted for the association")
		}
	})
	t.Run("nil-parent-refused", func(t *testing.T) {
		if _, err := borrowedAuthStagingAwaitBackendWith(nil, "127.0.0.1:5432", "127.0.0.1:40123", func(context.Context) (borrowedAuthStagingCensus, error) {
			t.Fatal("a census attempt ran with a nil parent context")
			return borrowedAuthStagingCensus{}, nil
		}); err == nil {
			t.Fatal("nil parent context was accepted for the association")
		}
	})
}

// TestBorrowedAuthStagingAwaitBackendBlockedProbe proves a blocking census
// attempt observes its child context clipped by the caller (the child carries
// the caller deadline), is released when that deadline expires and can never
// become ownership: the refusal returns promptly and no token is minted.
func TestBorrowedAuthStagingAwaitBackendBlockedProbe(t *testing.T) {
	parent, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	var released, deadlineObserved atomic.Bool
	attempt := func(ctx context.Context) (borrowedAuthStagingCensus, error) {
		if _, ok := ctx.Deadline(); ok {
			deadlineObserved.Store(true)
		}
		<-ctx.Done()
		released.Store(true)
		return borrowedAuthStagingCensus{}, ctx.Err()
	}
	start := time.Now()
	token, err := borrowedAuthStagingAwaitBackendWith(parent, "127.0.0.1:5432", "127.0.0.1:40123", attempt)
	if err == nil || token != (borrowedAuthStagingBackendToken{}) {
		t.Fatalf("a blocked census attempt was treated as ownership: token=%+v err=%v", token, err)
	}
	if !deadlineObserved.Load() {
		t.Fatal("the census attempt child context carried no caller-clipped deadline")
	}
	if !released.Load() {
		t.Fatal("the blocking census attempt was not released by the clipped child context")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("blocking census attempt was not bounded: %s", elapsed)
	}
}

// TestBorrowedAuthStagingAwaitBackendLateCancel proves the post-I/O
// linearization guard of the association: when the injected census attempt
// cancels the caller and returns an otherwise-valid exactly-one-match report,
// the association refuses with the caller cause, publishes no token and
// performs no retry. The synthetic report is a cancellation vehicle only
// (negative-only injection); it is never an authorization capability, and the
// one-exact-match candidate check exists solely to show that cancellation wins
// over an otherwise-publishable token.
func TestBorrowedAuthStagingAwaitBackendLateCancel(t *testing.T) {
	const serverLocal, helperLocal = "127.0.0.1:5432", "127.0.0.1:40123"
	candidate := borrowedAuthStagingCensus{
		Children: []borrowedAuthStagingCensusChild{{
			PID: 4242, Start: 99, PPID: 1, StateBefore: "S", StateAfter: "S",
			FDTotal: 3, FDReadable: 3,
			Sockets: []borrowedAuthStagingCensusSocket{{
				Inode: "777", Kind: "tcp", TCP: true, Local: serverLocal, Remote: helperLocal, State: "ESTABLISHED",
			}},
		}},
	}
	if matches := borrowedAuthStagingCensusMatches(candidate, serverLocal, helperLocal); len(matches) != 1 {
		t.Fatalf("synthetic late-cancel census is not a one-exact-match candidate: %d", len(matches))
	}
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	var attempts atomic.Int32
	attempt := func(context.Context) (borrowedAuthStagingCensus, error) {
		attempts.Add(1)
		cancel() // cancels the caller AFTER a successful one-match census
		return candidate, nil
	}
	token, err := borrowedAuthStagingAwaitBackendWith(parent, serverLocal, helperLocal, attempt)
	if err == nil {
		t.Fatal("a late caller cancellation after a one-match census published a token")
	}
	if token != (borrowedAuthStagingBackendToken{}) {
		t.Fatalf("late-cancel association returned a non-zero token: %+v", token)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("late-cancel association refusal does not carry the caller cause: %v", err)
	}
	if attempts.Load() != 1 {
		t.Fatalf("late-cancel association retried after cancellation: attempts=%d", attempts.Load())
	}
}

// TestBorrowedAuthStagingPostmasterIncarnationLateCancel proves the post-I/O
// guard of the final incarnation check: a strict stat that cancels the caller
// and returns the expected live state/start must still refuse as UNKNOWN with
// the caller cause, never a healthy result. The injected stat is negative-only
// and mints no capability.
func TestBorrowedAuthStagingPostmasterIncarnationLateCancel(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	inspect := func(context.Context, string) (int, string, error) { return 4242, "99", nil }
	var strictCalls atomic.Int32
	stat := func(context.Context, string, int) (string, uint64, error) {
		strictCalls.Add(1)
		cancel() // cancels the caller AFTER a successful strict stat
		return "S", 99, nil
	}
	err := borrowedAuthStagingCheckPostmasterIncarnation(parent, "synthetic", 4242, "99", inspect, stat)
	if err == nil {
		t.Fatal("a late caller cancellation after the strict stat produced a healthy incarnation result")
	}
	if !strings.Contains(err.Error(), "UNKNOWN") {
		t.Fatalf("late-cancel incarnation refusal is not UNKNOWN: %v", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("late-cancel incarnation refusal does not carry the caller cause: %v", err)
	}
	if strictCalls.Load() != 1 {
		t.Fatalf("late-cancel incarnation check ran the strict stat %d times", strictCalls.Load())
	}
}
