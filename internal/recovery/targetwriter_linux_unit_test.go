//go:build linux

package recovery

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// The fake process captures its actual argv/environment, so this check does
// not depend on a locally installed pg_restore binary.
func TestSupervisedPGRestoreProtectsArgvAndEnvironment(t *testing.T) {
	dir := t.TempDir()
	fake := filepath.Join(dir, "fake-pg-restore")
	argvPath := filepath.Join(dir, "argv")
	envPath := filepath.Join(dir, "env")
	script := "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$CAPTURE_ARGV\"\nprintf '%s\\n' \"$PGPASSWORD\" \"$PGPASSFILE\" > \"$CAPTURE_ENV\"\n"
	if err := os.WriteFile(fake, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	const secret = "password-must-not-reach-argv"
	const appName = "txh015_test_attempt"
	dsn := "postgres://operator:" + secret + "@127.0.0.1:55432/app?application_name=old"
	tagged, err := ConninfoWithAttemptApplicationName(dsn, appName)
	if err != nil {
		t.Fatal(err)
	}
	result, err := (TargetProcessRunner{DrainTimeout: time.Second}).RunPGCommandWithEnv(
		context.Background(), fake, []string{"--clean", "--if-exists", "--dbname=" + tagged}, nil, nil, nil,
		[]string{"PATH=" + os.Getenv("PATH"), "CAPTURE_ARGV=" + argvPath, "CAPTURE_ENV=" + envPath},
		func(context.Context) error { return nil })
	if err != nil || result.Outcome != PGCommandSucceeded || !result.ProcessGroupDrained {
		t.Fatalf("fake supervised restore failed: result=%+v err=%v", result, err)
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
	childEnv, err := os.ReadFile(envPath)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(string(childEnv), "\n"), "\n")
	if len(lines) != 2 || lines[0] != "" || lines[1] == "" {
		t.Fatalf("expected no PGPASSWORD and a private PGPASSFILE, got %q", childEnv)
	}
	if _, err := os.Stat(lines[1]); !os.IsNotExist(err) {
		t.Fatalf("private passfile should be removed after child completion, stat err=%v", err)
	}
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
