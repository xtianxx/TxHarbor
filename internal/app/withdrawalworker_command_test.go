package app

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

// TestWithdrawalWorkerCommandRefusesUnlinkedJointWiring pins the loud-refusal
// contract: a process without the 010/008 assembly must not start a
// claim-scan-only worker. The refusal happens before any pool is opened, so
// the test needs no database.
func TestWithdrawalWorkerCommandRefusesUnlinkedJointWiring(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := WithdrawalWorkerCommand(context.Background(), nil, Deps{
		Getenv: fakeEnv(fullServeEnv("127.0.0.1:0")),
		Stdout: &stdout,
		Stderr: &stderr,
	})
	if code != 1 {
		t.Fatalf("exit = %d, want 1; stderr=%s", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "joint wiring is not linked") {
		t.Fatalf("stderr = %q, want the unlinked-wiring refusal", stderr.String())
	}
}
