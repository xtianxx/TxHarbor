// drill_unit_test.go covers the pre-connection trust boundary of the T057
// `recovery-admin drill` wiring without Docker: help, usage errors (missing
// flags, unknown scenario, invalid chain id/local inputs), the refused
// pre-connection paths and the required-configuration refusal by exact key
// name. The real restore/record flow runs in drill_integration_test.go.
package recoveryadmin

import (
	"strings"
	"testing"

	"github.com/xtianxx/txharbor/internal/config"
)

func drillTestArgs() []string {
	return []string{"drill",
		"--manifest", "manifest.json",
		"--target-dsn", "postgres://u:d@127.0.0.1:1/drill_env?sslmode=disable",
		"--instance", "11111111-1111-4111-8111-111111111111",
		"--chain-id", "1",
	}
}

func TestDrillHelpAndUsageErrors(t *testing.T) {
	env := controlTestEnv("postgres://u:c@127.0.0.1:1/control?sslmode=disable",
		"postgres://u:d@127.0.0.1:1/drill_env?sslmode=disable", "deploy:executor")

	code, stdout, _ := runRecoveryAdmin(t, []string{"drill", "-h"}, env)
	if code != 0 || !strings.Contains(stdout, "--record-only") || !strings.Contains(stdout, "--chain-id") {
		t.Fatalf("drill -h must print the full surface: exit=%d stdout=%q", code, stdout)
	}

	cases := []struct {
		name string
		args []string
	}{
		{"no flags", []string{"drill"}},
		{"missing chain id", []string{"drill", "--manifest", "m.json", "--target-dsn", "postgres://u:d@127.0.0.1:1/x?sslmode=disable", "--instance", "11111111-1111-4111-8111-111111111111"}},
		{"unknown scenario", append(drillTestArgs(), "--scenario", "f8")},
		{"case-folded scenario", append(drillTestArgs(), "--scenario", "FULL_RECOVERY")},
		{"bare failure injection id", append(drillTestArgs(), "--scenario", "F1")},
		{"invalid chain id", []string{"drill", "--manifest", "m.json", "--target-dsn", "postgres://u:d@127.0.0.1:1/x?sslmode=disable", "--instance", "11111111-1111-4111-8111-111111111111", "--chain-id", "zero"}},
		{"negative lag input", append(drillTestArgs(), "--backup-lag-seconds", "-5")},
		{"unexpected positional", append(drillTestArgs(), "extra")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, _, stderr := runRecoveryAdmin(t, tc.args, env)
			if code != 2 {
				t.Fatalf("exit=%d want 2 (stderr=%q)", code, stderr)
			}
			if strings.Contains(stderr, "NOT IMPLEMENTED") {
				t.Fatalf("drill is wired; it must never answer NOT IMPLEMENTED: %q", stderr)
			}
		})
	}
}

func TestDrillRefusesBeforeConnecting(t *testing.T) {
	// Missing control DSN: refused by exact key name before anything opens.
	env := controlTestEnv("", "postgres://u:d@127.0.0.1:1/drill_env?sslmode=disable", "deploy:executor")
	code, _, stderr := runRecoveryAdmin(t, drillTestArgs(), env)
	if code != 1 || !strings.Contains(stderr, config.EnvRecoveryControlDSN) {
		t.Fatalf("missing control DSN must refuse by key name: exit=%d stderr=%q", code, stderr)
	}

	// A --instance that disagrees with the deployment binding never reaches
	// the database.
	env = controlTestEnv("postgres://u:c@127.0.0.1:1/control?sslmode=disable",
		"postgres://u:d@127.0.0.1:1/drill_env?sslmode=disable", "deploy:executor")
	env[config.EnvRecoveryInstance] = "22222222-2222-4222-8222-222222222222"
	code, _, stderr = runRecoveryAdmin(t, drillTestArgs(), env)
	if code != 1 || !strings.Contains(stderr, "instance bound execution") {
		t.Fatalf("a mismatched instance binding must refuse: exit=%d stderr=%q", code, stderr)
	}

	// Without the executor participant configuration the command refuses
	// fail-closed at the control store (unreachable here), never with success.
	env = controlTestEnv("postgres://u:c@127.0.0.1:1/control?sslmode=disable",
		"postgres://u:d@127.0.0.1:1/drill_env?sslmode=disable", "deploy:executor")
	code, stdout, stderr := runRecoveryAdmin(t, append(drillTestArgs(), "--record-only"), env)
	if code == 0 {
		t.Fatalf("an unreachable control store must refuse: exit=%d stdout=%q", code, stdout)
	}
	if strings.Contains(stdout, "drill_id=") || strings.Contains(stdout, "result=ok") {
		t.Fatalf("a refused drill must never print a recorded run: %q", stdout)
	}
}
