// restore_unit_test.go pins the operator-facing boundaries of
// `recovery-admin restore` help output (G1/G4 closure): the production_main
// preconditions, the audit-only reason, the operation-id replay/real-rerun
// distinction, the pre-write evidence-invalidation timing and the T028/T064
// program-boundary statement. Help text states limits; it makes no runtime
// enforcement claim.
package recoveryadmin

import (
	"strings"
	"testing"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/recovery"
)

func TestRestoreHelpStatesPreconditionsAndEvidenceTiming(t *testing.T) {
	env := controlTestEnv("postgres://u:c@127.0.0.1:1/control?sslmode=disable",
		"postgres://u:d@127.0.0.1:1/data?sslmode=disable", "deploy:executor")
	wants := []string{
		"restore --manifest",
		"--declaration",
		"production_main preconditions",                 // verified backup + evidence chain + target + probes
		"does NOT prove the old writers stopped",        // no isolation claim
		"audit annotation only, never an authorization", // --reason
		"omitted = a real rerun (non-replay)",           // operation-id semantics
		"before the first target write",                 // evidence invalidation timing
		"T028/T064 are not delivered",                   // program boundary
		"no runtime enforcement is claimed",
	}
	for _, invocation := range [][]string{
		{"restore", "--help"},
		{"restore", "-h"},
		{"help", "restore"},
	} {
		code, stdout, stderr := runRecoveryAdmin(t, invocation, env)
		if code != 0 {
			t.Fatalf("%v: exit=%d want 0 (stderr=%q)", invocation, code, stderr)
		}
		for _, want := range wants {
			if !strings.Contains(stdout, want) {
				t.Fatalf("%v: help must state %q, got:\n%s", invocation, want, stdout)
			}
		}
	}
}

func TestRestoreRequiresExplicitObserverConfiguration(t *testing.T) {
	env := controlTestEnv("postgres://u:c@127.0.0.1:1/control?sslmode=disable",
		"postgres://u:d@127.0.0.1:1/data?sslmode=disable", "deploy:executor")
	delete(env, config.EnvRecoveryObserverDSN)
	code, _, stderr := runRecoveryAdmin(t, []string{"restore", "--manifest", "manifest", "--target-dsn", "postgres://u:d@127.0.0.1:1/data?sslmode=disable", "--instance", "instance"}, env)
	if code != 1 || !strings.Contains(stderr, config.EnvRecoveryObserverDSN+" is required") {
		t.Fatalf("missing observer configuration: exit=%d stderr=%q", code, stderr)
	}
}

func TestRestoreRejectsCredentialShapedReasonBeforePersistence(t *testing.T) {
	const canary = "restore-reason-unit-canary"
	env := controlTestEnv("postgres://u:c@127.0.0.1:1/control?sslmode=disable",
		"postgres://u:d@127.0.0.1:1/data?sslmode=disable", "deploy:executor")
	args := []string{"restore", "--manifest", "manifest", "--target-dsn", "postgres://u:d@127.0.0.1:1/data?sslmode=disable",
		"--instance", "instance", "--declaration", "production_main", "--reason", "password=" + canary}
	code, stdout, stderr := runRecoveryAdmin(t, args, env)
	if code != 1 || !strings.Contains(stderr, "target reason contains credential-shaped material") {
		t.Fatalf("credential-shaped reason was not rejected early: exit=%d stderr=%q", code, stderr)
	}
	if strings.Contains(stdout+stderr, canary) {
		t.Fatalf("reason canary escaped refusal output: stdout=%q stderr=%q", stdout, stderr)
	}
}

func TestRestoreTargetAuthorityPreflight(t *testing.T) {
	const authoritative = "postgres://operator:secret@db.example.test:5432/data?sslmode=disable"
	const control = "postgres://operator:secret@db.example.test:5432/control?sslmode=disable"
	t.Run("production target requires explicit declaration", func(t *testing.T) {
		if err := recoveryRestoreTargetPreflight(authoritative, authoritative, control, recovery.TargetIsolated); err == nil || !strings.Contains(err.Error(), "isolated declaration refused") {
			t.Fatalf("isolated declaration for authority: got %v", err)
		}
	})
	t.Run("matching production configuration accepted", func(t *testing.T) {
		if err := recoveryRestoreTargetPreflight(authoritative, authoritative, control, recovery.TargetProductionMain); err != nil {
			t.Fatalf("matching production target config rejected: %v", err)
		}
	})
	t.Run("same endpoint wrong role refused", func(t *testing.T) {
		wrongRole := "postgres://other:secret@db.example.test:5432/data?sslmode=disable"
		if err := recoveryRestoreTargetPreflight(wrongRole, authoritative, control, recovery.TargetProductionMain); err == nil || !strings.Contains(err.Error(), "does not match") {
			t.Fatalf("same-key wrong-role target: got %v", err)
		}
	})
	t.Run("control database refused", func(t *testing.T) {
		if err := recoveryRestoreTargetPreflight(control, authoritative, control, recovery.TargetProductionMain); err == nil || !strings.Contains(err.Error(), "control database") {
			t.Fatalf("control database target: got %v", err)
		}
	})
}
