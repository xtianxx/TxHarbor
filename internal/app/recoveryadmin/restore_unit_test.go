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
