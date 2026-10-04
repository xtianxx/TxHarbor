//go:build integration && linux

// verificationepoch_iononroot_linux_test.go runs the real backup verification
// against a mode-000 artifact as a genuine non-root process.
//
// The parent test (TestVerifyBackupIntegrityIOUncertaintyRemainsUnverified)
// has already created the real fixture, verified the backup, and chmod 000 the
// artifact. When that parent runs as root, mode 000 is still readable, so the
// IO-uncertainty counterexample cannot be produced in-process. In that case
// the parent re-executes the real test binary under uid/gid 65534: the child
// opens the artifact and must observe an actual EACCES, then runs the real
// ExecuteVerifyBackup against the fixture control store and its genuine
// isolated target binding.
//
// The child receives the fixture DSNs (disposable test credentials) as JSON on
// stdin and writes its report on a private pipe descriptor; no credential
// value ever reaches argv, the environment, or stderr. The parent stages only
// this test's own temporary paths for the helper identity: traversal on the
// test-owned parent directories, writable ownership of the artifact directory
// and the manifest file (the child publishes the unverified manifest
// write-back with a sibling temp file plus rename). The artifact itself stays
// root-owned mode 000, so the denial is real. No repository path is chmodded.

package recovery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/logx"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

const (
	epochIONonRootHelperEnv = "TXHARBOR_EPOCH_IO_NONROOT_HELPER"
	epochIONonRootReportFD  = 3
)

func init() {
	epochIONonRootVerify = runEpochIONonRootVerify
}

// TestEpochIONonRootHelperProcess is the dedicated child entry point. The
// parent starts it with -test.run anchored to this name and the helper
// environment flag set; a normal package run only skips it.
func TestEpochIONonRootHelperProcess(t *testing.T) {
	if os.Getenv(epochIONonRootHelperEnv) != "1" {
		t.Skip("non-root epoch IO helper process; started only by TestVerifyBackupIntegrityIOUncertaintyRemainsUnverified when that test runs as root")
	}
	os.Exit(runEpochIONonRootHelperProcess())
}

// epochIONonRootConfig is the stdin payload of one helper run. It carries the
// disposable fixture DSNs and public paths only.
type epochIONonRootConfig struct {
	ManifestPath     string `json:"manifest_path"`
	ArtifactPath     string `json:"artifact_path"`
	ControlDSN       string `json:"control_dsn"`
	AuthoritativeDSN string `json:"authoritative_dsn"`
	TargetDSN        string `json:"target_dsn"`
	ObserverDSN      string `json:"observer_dsn"`
	InstanceID       string `json:"instance_id"`
	OperationID      string `json:"operation_id"`
	ProgramVersion   string `json:"program_version"`
	Verifier         string `json:"verifier"`
}

// runEpochIONonRootHelperProcess is the whole child body. It never uses the
// test harness for assertions: it writes one JSON report to the private pipe
// descriptor and returns the process exit code.
func runEpochIONonRootHelperProcess() int {
	reportFile := os.NewFile(epochIONonRootReportFD, "epoch-io-nonroot-report")
	if reportFile == nil {
		fmt.Fprintln(os.Stderr, "epoch IO non-root helper: report descriptor unavailable")
		return 2
	}
	report := epochIONonRootOutcome{}
	emit := func(code int) int {
		encoded, err := json.Marshal(report)
		if err != nil {
			fmt.Fprintf(os.Stderr, "epoch IO non-root helper: encode report: %v\n", err)
			return 2
		}
		if _, err := reportFile.Write(encoded); err != nil {
			fmt.Fprintf(os.Stderr, "epoch IO non-root helper: write report: %v\n", err)
			return 2
		}
		return code
	}
	fail := func(reason string) int {
		report.Failure = logx.Redact(reason)
		return emit(1)
	}

	var cfg epochIONonRootConfig
	if err := json.NewDecoder(os.Stdin).Decode(&cfg); err != nil {
		return fail("decode helper configuration: " + err.Error())
	}

	// Prove the identity premise as observed by the child itself: the parent
	// asked for uid/gid 65534 and the report must carry exactly that.
	report.UID, report.GID = os.Geteuid(), os.Getegid()
	if report.UID != epochIONonRootUID || report.GID != epochIONonRootGID {
		return fail(fmt.Sprintf("helper runs as uid/gid %d:%d, want %d:%d", report.UID, report.GID, epochIONonRootUID, epochIONonRootGID))
	}

	// First prove the counterexample premise with a real open: mode 000 root
	// ownership must deny uid 65534, not merely be assumed to deny it.
	artifact, err := os.Open(cfg.ArtifactPath)
	switch {
	case err == nil:
		_ = artifact.Close()
		return fail("artifact remained readable to the non-root helper")
	case !errors.Is(err, fs.ErrPermission):
		return fail("artifact open did not fail with permission denied: " + err.Error())
	}
	report.ArtifactDenied = true

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool, err := db.OpenPool(ctx, cfg.ControlDSN, 10*time.Second)
	if err != nil {
		return fail("open fixture control store: " + err.Error())
	}
	defer pool.Close()
	store, err := controlstore.NewStore(ctx, pool)
	if err != nil {
		return fail("validate fixture control store: " + err.Error())
	}
	open, err := readOpenIsolatedBindings(ctx, store)
	if err != nil {
		return fail("read open target bindings: " + err.Error())
	}
	binding, err := BindIsolatedTarget(cfg.AuthoritativeDSN, cfg.ControlDSN, cfg.TargetDSN, open)
	if err != nil {
		return fail("bind fixture isolated target: " + err.Error())
	}

	result, verifyErr := ExecuteVerifyBackup(ctx, VerifyBackupOptions{
		ManifestPath: cfg.ManifestPath, Binding: binding, TargetDSN: cfg.TargetDSN,
		Verifier: cfg.Verifier, InstanceID: cfg.InstanceID, ControlStore: store,
		ControlDSN: cfg.ControlDSN, AuthoritativeDSN: cfg.AuthoritativeDSN,
		ObserverDSN: cfg.ObserverDSN, ProgramVersion: cfg.ProgramVersion,
		OperationID: cfg.OperationID, PG: epochIONonRootDirectPG{},
	})
	report.OK = true
	report.State = result.State
	report.Reason = result.Reason
	if verifyErr != nil {
		report.Err = verifyErr.Error()
	}
	return emit(0)
}

// runEpochIONonRootVerify stages the parent fixture for uid/gid 65534 and runs
// the helper child to completion.
func runEpochIONonRootVerify(t *testing.T, opts VerifyBackupOptions, artifactPath string) epochIONonRootOutcome {
	t.Helper()
	artifactDir := filepath.Dir(artifactPath)
	makeEpochIONonRootPathTraversable(t, artifactDir)
	if err := os.Chown(artifactDir, epochIONonRootUID, epochIONonRootGID); err != nil {
		t.Fatalf("hand artifact directory to uid %d: %v", epochIONonRootUID, err)
	}
	if err := os.Chmod(artifactDir, 0o700); err != nil {
		t.Fatalf("restrict artifact directory after handover: %v", err)
	}
	if err := os.Chown(opts.ManifestPath, epochIONonRootUID, epochIONonRootGID); err != nil {
		t.Fatalf("hand manifest write-back to uid %d: %v", epochIONonRootUID, err)
	}
	if err := os.Chmod(opts.ManifestPath, 0o600); err != nil {
		t.Fatalf("restrict manifest after handover: %v", err)
	}
	helperPath := stageEpochIONonRootHelperBinary(t, artifactDir)

	payload, err := json.Marshal(epochIONonRootConfig{
		ManifestPath: opts.ManifestPath, ArtifactPath: artifactPath,
		ControlDSN: opts.ControlDSN, AuthoritativeDSN: opts.AuthoritativeDSN,
		TargetDSN: opts.TargetDSN, ObserverDSN: opts.ObserverDSN,
		InstanceID: opts.InstanceID, OperationID: opts.OperationID,
		ProgramVersion: opts.ProgramVersion, Verifier: opts.Verifier,
	})
	if err != nil {
		t.Fatalf("encode non-root helper configuration: %v", err)
	}

	reportRead, reportWrite, err := os.Pipe()
	if err != nil {
		t.Fatalf("create non-root helper report pipe: %v", err)
	}
	defer func() { _ = reportRead.Close() }()
	var stdout, stderr bytes.Buffer
	cmd := exec.Command(helperPath, "-test.run=^TestEpochIONonRootHelperProcess$")
	cmd.Env = append(os.Environ(), epochIONonRootHelperEnv+"=1")
	cmd.Stdin = bytes.NewReader(payload)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	cmd.ExtraFiles = []*os.File{reportWrite}
	cmd.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{
		Uid: epochIONonRootUID, Gid: epochIONonRootGID,
		Groups: epochIONonRootSupplementaryGroups(t),
	}}
	runErr := cmd.Run()
	_ = reportWrite.Close()
	raw, readErr := io.ReadAll(reportRead)
	if readErr != nil || len(bytes.TrimSpace(raw)) == 0 {
		t.Fatalf("non-root helper produced no report: run_err=%v read_err=%v stdout=%q stderr=%q",
			runErr, readErr, epochIOBoundedDiagnostic(stdout.String()), epochIOBoundedDiagnostic(stderr.String()))
	}
	var report epochIONonRootOutcome
	if err := json.Unmarshal(raw, &report); err != nil {
		t.Fatalf("decode non-root helper report: %v raw=%q stderr=%q",
			err, epochIOBoundedDiagnostic(string(raw)), epochIOBoundedDiagnostic(stderr.String()))
	}
	if runErr != nil && report.OK {
		t.Fatalf("non-root helper reported success but exited with %v", runErr)
	}
	return report
}

// makeEpochIONonRootPathTraversable grants traversal-only (0711) on the
// test-owned directories between the temp root and the artifact directory so
// uid 65534 can reach the staged paths. The temp root itself is never modified:
// it is not this test's property, so an untraversable temp root is refused.
func makeEpochIONonRootPathTraversable(t *testing.T, dir string) {
	t.Helper()
	stop := filepath.Clean(os.TempDir())
	if info, err := os.Stat(stop); err == nil && info.Mode().Perm()&0o001 == 0 {
		t.Fatalf("temp root %s is not traversable by the non-root helper; refusing to weaken the temp root", stop)
	}
	for current := filepath.Dir(filepath.Clean(dir)); ; current = filepath.Dir(current) {
		if current == stop || current == string(filepath.Separator) || current == filepath.Dir(current) {
			return
		}
		if err := os.Chmod(current, 0o711); err != nil {
			t.Fatalf("grant non-root traversal on %s: %v", current, err)
		}
	}
}

// stageEpochIONonRootHelperBinary copies the running test binary into this
// test's own artifact directory and makes it executable. Only that exact path
// is touched; the repository and the module/build caches are not chmodded.
func stageEpochIONonRootHelperBinary(t *testing.T, dir string) string {
	t.Helper()
	source, err := os.Executable()
	if err != nil {
		t.Fatalf("locate the running test binary: %v", err)
	}
	data, err := os.ReadFile(source)
	if err != nil {
		t.Fatalf("read the running test binary: %v", err)
	}
	path := filepath.Join(dir, "epoch-io-nonroot-helper.test")
	if err := os.WriteFile(path, data, 0o755); err != nil {
		t.Fatalf("stage non-root helper binary: %v", err)
	}
	if err := os.Chmod(path, 0o755); err != nil {
		t.Fatalf("make non-root helper binary executable: %v", err)
	}
	return path
}

// epochIONonRootSupplementaryGroups carries exactly the docker socket's group
// into the child. The package TestMain requires a healthy docker provider
// before any test runs, and the non-root helper must pass that same gate; the
// socket group is the only supplementary group it receives.
func epochIONonRootSupplementaryGroups(t *testing.T) []uint32 {
	t.Helper()
	socketPath := "/var/run/docker.sock"
	if host := os.Getenv("DOCKER_HOST"); strings.HasPrefix(host, "unix://") {
		socketPath = strings.TrimPrefix(host, "unix://")
	}
	var stat syscall.Stat_t
	if err := syscall.Stat(socketPath, &stat); err != nil {
		t.Fatalf("non-root helper needs the docker socket to satisfy the package docker gate: stat %s: %v", socketPath, err)
	}
	return []uint32{stat.Gid}
}

// epochIOBoundedDiagnostic bounds and redacts child output before it can reach
// a test failure message; credential-shaped text never belongs in either
// stream.
func epochIOBoundedDiagnostic(text string) string {
	text = strings.TrimSpace(logx.Redact(text))
	const limit = 4000
	if len(text) > limit {
		return text[:limit] + "...[truncated]"
	}
	return text
}

// epochIONonRootDirectPG executes the resolved client binaries directly. It is
// used only by the non-root helper process, which runs under the fixed runner
// image where the real PostgreSQL 18.6 ELF tools are on PATH. ExecuteVerifyBackup
// requires a real PGCommand, never nil; the denied-artifact path fails its
// integrity check before any probe, so this seam runs no target write here.
type epochIONonRootDirectPG struct{}

func (epochIONonRootDirectPG) Run(ctx context.Context, name string, args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdin = stdin
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("run %s: %w", name, err)
	}
	return nil
}
