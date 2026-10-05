//go:build linux

package recovery

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// The fake process captures its actual argv and execs this test binary as a
// child to fstat/read the inherited anonymous descriptor. This check does not
// depend on a locally installed pg_restore binary. Descriptor cleanup and
// parent-death ownership are covered by TestPGCredentialFileIsUnlinkedAndPrivate,
// TestLocalPGCommandCleansPassfileWhenChildFailsAndDoesNotLeakError and
// TestNativePG18AnonymousCredentialFDAuthenticatesDumpAndRestore.
func TestSupervisedPGRestoreProtectsArgvAndEnvironment(t *testing.T) {
	const helperEnv = "TXHARBOR_SUPERVISED_RESTORE_HELPER"
	const secret = "password-must-not-reach-argv"
	if os.Getenv(helperEnv) == "1" {
		passfile := os.Getenv("PGPASSFILE")
		const fdPrefix = "/proc/self/fd/"
		if !strings.HasPrefix(passfile, fdPrefix) {
			t.Fatalf("child PGPASSFILE is not an anonymous descriptor reference: %q", passfile)
		}
		fd, err := strconv.Atoi(strings.TrimPrefix(passfile, fdPrefix))
		if err != nil || fd < 3 {
			t.Fatalf("child PGPASSFILE descriptor is invalid: %q err=%v", passfile, err)
		}
		if _, ok := os.LookupEnv("PGPASSWORD"); ok {
			t.Fatal("child environment contains PGPASSWORD")
		}
		for _, entry := range os.Environ() {
			if strings.Contains(entry, secret) {
				t.Fatalf("child environment contains the DSN password: %q", entry)
			}
		}

		var st syscall.Stat_t
		if err := syscall.Fstat(fd, &st); err != nil {
			t.Fatalf("fstat inherited credential descriptor: %v", err)
		}
		if st.Mode&syscall.S_IFMT != syscall.S_IFREG || st.Mode&0o777 != 0o600 || st.Nlink != 0 {
			t.Fatalf("inherited credential fd must be an anonymous regular 0600 file: mode=%#o nlink=%d", st.Mode, st.Nlink)
		}
		contents := make([]byte, 4096)
		n, err := syscall.Pread(fd, contents, 0)
		if err != nil {
			t.Fatalf("read inherited credential descriptor: %v", err)
		}
		if !strings.Contains(string(contents[:n]), "*:*:*:*:"+secret) {
			t.Fatalf("inherited passfile does not contain the expected credential entry")
		}
		for _, entry := range os.Args {
			if strings.Contains(entry, secret) {
				t.Fatalf("helper argv contains the DSN password: %q", entry)
			}
		}
		captureDir := os.Getenv("CAPTURE_DIR")
		entries, err := os.ReadDir(captureDir)
		if err != nil {
			t.Fatalf("inspect child capture directory: %v", err)
		}
		for _, entry := range entries {
			if strings.Contains(strings.ToLower(entry.Name()), "pgpass") {
				t.Fatalf("named passfile appeared while child was running: %q", entry.Name())
			}
		}
		report := fmt.Sprintf("PGPASSFILE=%s\nPGPASSWORD_PRESENT=false\nPASSFILE_REGULAR=true\nPASSFILE_MODE=%04o\nPASSFILE_NLINK=%d\nPASSFILE_CONTENT_MATCH=true\nNO_NAMED_PASSFILE=true\n",
			passfile, st.Mode&0o777, st.Nlink)
		if err := os.WriteFile(os.Getenv("CAPTURE_REPORT"), []byte(report), 0o600); err != nil {
			t.Fatalf("write non-secret child report: %v", err)
		}
		return
	}

	dir := t.TempDir()
	fake := filepath.Join(dir, "fake-pg-restore")
	argvPath := filepath.Join(dir, "argv")
	reportPath := filepath.Join(dir, "child-report")
	helperOutputPath := filepath.Join(dir, "child-output")
	helperBinary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	quote := func(value string) string { return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'" }
	// The fixture supplies its own controls after launch; production filtering
	// must not forward arbitrary application environment variables for this test.
	script := "#!/bin/sh\nexport " + helperEnv + "=1 CAPTURE_DIR=" + quote(dir) + " CAPTURE_REPORT=" + quote(reportPath) + "\n" +
		"printf '%s\\n' \"$@\" > " + quote(argvPath) + "\nexec " + quote(helperBinary) +
		" -test.run='^TestSupervisedPGRestoreProtectsArgvAndEnvironment$' > " + quote(helperOutputPath) + " 2>&1\n"
	if err := os.WriteFile(fake, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	const appName = "txh015_test_attempt"
	dsn := "postgres://operator:" + secret + "@127.0.0.1:55432/app?application_name=old"
	tagged, err := ConninfoWithAttemptApplicationName(dsn, appName)
	if err != nil {
		t.Fatal(err)
	}
	assertNoNamedPassfile := func(phase string) {
		t.Helper()
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("read %s capture directory: %v", phase, err)
		}
		for _, entry := range entries {
			if strings.Contains(strings.ToLower(entry.Name()), "pgpass") {
				t.Fatalf("named passfile exists %s child: %q", phase, entry.Name())
			}
		}
	}
	assertNoNamedPassfile("before")
	result, err := (TargetProcessRunner{DrainTimeout: time.Second}).RunPGCommandWithEnv(
		context.Background(), fake, []string{"--clean", "--if-exists", "--dbname=" + tagged}, nil, nil, nil,
		[]string{"PATH=" + os.Getenv("PATH")},
		func(context.Context) error { return nil })
	if err != nil || result.Outcome != PGCommandSucceeded || !result.ProcessGroupDrained {
		childOutput, _ := os.ReadFile(helperOutputPath)
		t.Fatalf("fake supervised restore failed: result=%+v err=%v child output=%s", result, err, childOutput)
	}
	argv, err := os.ReadFile(argvPath)
	if err != nil {
		t.Fatal(err)
	}
	gotArgv := string(argv)
	if strings.Contains(gotArgv, secret) || strings.Contains(gotArgv, dsn) || strings.Contains(gotArgv, "password=") {
		t.Fatalf("supervised child argv contains original DSN credentials: %q", gotArgv)
	}
	if !strings.Contains(gotArgv, "application_name="+appName) {
		t.Fatalf("supervised child argv lost exact attempt tag: %q", gotArgv)
	}
	childReport, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatal(err)
	}
	report := string(childReport)
	for _, expected := range []string{
		"PGPASSWORD_PRESENT=false\n", "PASSFILE_REGULAR=true\n", "PASSFILE_MODE=0600\n",
		"PASSFILE_NLINK=0\n", "PASSFILE_CONTENT_MATCH=true\n", "NO_NAMED_PASSFILE=true\n",
	} {
		if !strings.Contains(report, expected) {
			t.Fatalf("child did not prove %q: report=%q", strings.TrimSpace(expected), report)
		}
	}
	if !strings.Contains(report, "PGPASSFILE=/proc/self/fd/") || strings.Contains(report, dir) || strings.Contains(report, secret) {
		t.Fatalf("child report should contain only an anonymous descriptor reference: %q", report)
	}
	assertNoNamedPassfile("after")
}

func TestAttemptTagReplacesExistingKeywordApplicationName(t *testing.T) {
	const appName = "txh015_exact_attempt"
	conninfo, err := ConninfoWithAttemptApplicationName(
		"host=127.0.0.1 port=55432 dbname=app user=operator application_name=stale",
		appName)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(conninfo, "application_name=") != 1 {
		t.Fatalf("keyword conninfo duplicated application_name: %q", conninfo)
	}
	cfg, err := pgx.ParseConfig(conninfo)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RuntimeParams["application_name"] != appName {
		t.Fatalf("application_name = %q, want %q", cfg.RuntimeParams["application_name"], appName)
	}
}

func TestAmbiguousDrainedProcessIsNotEligibleForFailureFinalization(t *testing.T) {
	for _, outcome := range []PGCommandOutcome{PGCommandAmbiguous, PGCommandLockLost} {
		command := PGCommandResult{Outcome: outcome, Started: true, ProcessGroupDrained: true}
		if targetAttemptFailureCleanupEligible(command) {
			t.Fatalf("%s process result incorrectly qualified for failure finalization", outcome)
		}
	}
	for _, outcome := range []PGCommandOutcome{PGCommandSucceeded, PGCommandFailed, PGCommandCanceled} {
		command := PGCommandResult{Outcome: outcome, Started: true, ProcessGroupDrained: true}
		if !targetAttemptFailureCleanupEligible(command) {
			t.Fatalf("%s fully drained process should qualify for failure finalization", outcome)
		}
	}
	if targetAttemptFailureCleanupEligible(PGCommandResult{Outcome: PGCommandFailed, Started: true}) {
		t.Fatal("process result without proven process-group drain qualified for failure finalization")
	}
}
