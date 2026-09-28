// control_unit_test.go covers the pre-connection trust boundary of the T010
// management surface (`recovery-admin control ...`) without Docker: required
// configuration (control DSN, data DSN for the independence proof, the
// authenticated principal), the same-target refusal before any connection,
// malformed/unreachable control DSNs with redaction, usage errors, the
// remaining stub actions and the B6 refusal-by-name boundary. The real
// identity paths run in control_integration_test.go.
package recoveryadmin

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/xtianxx/txharbor/internal/config"
)

func runRecoveryAdmin(t *testing.T, args []string, env map[string]string) (int, string, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := Run(context.Background(), args, Deps{
		Getenv: migrateTestEnv(env),
		Stdout: &stdout,
		Stderr: &stderr,
	})
	return code, stdout.String(), stderr.String()
}

func runControl(t *testing.T, args []string, env map[string]string) (int, string, string) {
	t.Helper()
	return runRecoveryAdmin(t, append([]string{"control"}, args...), env)
}

func controlTestEnv(controlDSN, dataDSN, principal string) map[string]string {
	env := map[string]string{
		config.EnvPGDSN: dataDSN,
	}
	if controlDSN != "" {
		env[config.EnvRecoveryControlDSN] = controlDSN
	}
	if principal != "" {
		env[config.EnvRecoveryPrincipal] = principal
	}
	return env
}

func TestControlUsageErrors(t *testing.T) {
	env := controlTestEnv("postgres://u:c@127.0.0.1:1/control?sslmode=disable",
		"postgres://u:d@127.0.0.1:1/data?sslmode=disable", "deploy:manager")
	cases := [][]string{
		{}, // no subcommand
		{"bogus"},
		{"participant-register"}, // missing required flags
		{"participant-register", "--instance", "not-a-uuid", "--principal", "deploy:alice", "--role", "approver"},
		{"identity-map-set"}, // missing principal/person/operation/reason/operator
		{"identity-map-set", "--principal", "deploy:alice", "--revoke", "--person-id", "person-a", "--operator", "ops", "--reason", "r", "--operation-id", "op-1"},
		{"identity-map-set", "--principal", "deploy:alice", "--operator", "ops", "--reason", "r", "--operation-id", "op-1"}, // person-id missing without --revoke
		{"identity-map-show", "unexpected"},
	}
	for _, args := range cases {
		code, _, stderr := runControl(t, args, env)
		if code != 2 {
			t.Fatalf("control %v: exit=%d want 2 (stderr=%q)", args, code, stderr)
		}
	}
	// The dispatch itself answers help on stdout with exit 0.
	code, stdout, _ := runControl(t, []string{"-h"}, env)
	if code != 0 || !strings.Contains(stdout, "participant-register") {
		t.Fatalf("control -h must print the management surface: exit=%d stdout=%q", code, stdout)
	}
}

func TestControlRequiresConfiguration(t *testing.T) {
	t.Run("missing control dsn", func(t *testing.T) {
		code, _, stderr := runControl(t, []string{"identity-map-show"},
			controlTestEnv("", "postgres://u:d@127.0.0.1:1/data?sslmode=disable", "deploy:manager"))
		if code != 1 || !strings.Contains(stderr, config.EnvRecoveryControlDSN) {
			t.Fatalf("missing control DSN must refuse by key name: exit=%d stderr=%q", code, stderr)
		}
	})
	t.Run("missing data dsn for independence", func(t *testing.T) {
		code, _, stderr := runControl(t, []string{"identity-map-show"},
			controlTestEnv("postgres://u:controlsecret@127.0.0.1:1/control?sslmode=disable", "", "deploy:manager"))
		if code != 1 || !strings.Contains(stderr, config.EnvPGDSN) {
			t.Fatalf("missing data DSN must refuse by key name: exit=%d stderr=%q", code, stderr)
		}
		if strings.Contains(stderr, "controlsecret") {
			t.Fatalf("refusal must not leak the control DSN password: %q", stderr)
		}
	})
	t.Run("missing authenticated principal", func(t *testing.T) {
		code, _, stderr := runControl(t, []string{"identity-map-show"},
			controlTestEnv("postgres://u:c@127.0.0.1:1/control?sslmode=disable",
				"postgres://u:d@127.0.0.1:1/data?sslmode=disable", ""))
		if code != 1 || !strings.Contains(stderr, config.EnvRecoveryPrincipal) {
			t.Fatalf("missing principal must refuse by key name: exit=%d stderr=%q", code, stderr)
		}
	})
	t.Run("free-form principal refused", func(t *testing.T) {
		code, _, stderr := runControl(t, []string{"identity-map-show"},
			controlTestEnv("postgres://u:c@127.0.0.1:1/control?sslmode=disable",
				"postgres://u:d@127.0.0.1:1/data?sslmode=disable", "Alice"))
		if code != 1 || !strings.Contains(stderr, config.EnvRecoveryPrincipal) {
			t.Fatalf("a free-form principal must refuse: exit=%d stderr=%q", code, stderr)
		}
	})
	t.Run("padded principal refused", func(t *testing.T) {
		code, _, stderr := runControl(t, []string{"identity-map-show"},
			controlTestEnv("postgres://u:c@127.0.0.1:1/control?sslmode=disable",
				"postgres://u:d@127.0.0.1:1/data?sslmode=disable", " deploy:manager "))
		if code != 1 || !strings.Contains(stderr, "surrounding whitespace") {
			t.Fatalf("a padded principal must refuse: exit=%d stderr=%q", code, stderr)
		}
	})
}

func TestControlRefusesSameDatabaseTargetBeforeConnecting(t *testing.T) {
	dsn := "postgres://txharbor:samesecret@127.0.0.1:1/one_db?sslmode=disable"
	code, stdout, stderr := runControl(t, []string{"identity-map-set", "--principal", "deploy:alice",
		"--person-id", "person-a", "--operator", "ops", "--reason", "r", "--operation-id", "op-1"},
		controlTestEnv(dsn, dsn, "deploy:manager"))
	if code != 1 {
		t.Fatalf("equal control/data DSNs must refuse with exit 1, got %d (stderr=%q)", code, stderr)
	}
	if !strings.Contains(stderr, "same database target") {
		t.Fatalf("refusal must state the same-target reason: %q", stderr)
	}
	if strings.Contains(stderr, "control store unavailable") {
		t.Fatalf("the same-target refusal must precede any connection attempt: %q", stderr)
	}
	for _, out := range []string{stdout, stderr} {
		if strings.Contains(out, "samesecret") || strings.Contains(out, "postgres://") {
			t.Fatalf("refusal must not leak the DSN: %q", out)
		}
	}
}

func TestControlRefusesMalformedControlDSN(t *testing.T) {
	code, stdout, stderr := runControl(t, []string{"identity-map-show"},
		controlTestEnv("postgres://user:sekret@localhost:700000/db",
			"postgres://user:pw@localhost:5432/data?sslmode=disable", "deploy:manager"))
	if code != 1 || !strings.Contains(stderr, config.EnvRecoveryControlDSN) {
		t.Fatalf("malformed control DSN must refuse by key name: exit=%d stderr=%q", code, stderr)
	}
	if strings.Contains(stderr, "sekret") || strings.Contains(stdout, "sekret") {
		t.Fatalf("refusal must redact the malformed DSN: stdout=%q stderr=%q", stdout, stderr)
	}
}

func TestControlRefusesUnreachableControlStore(t *testing.T) {
	code, stdout, stderr := runControl(t, []string{"identity-map-show"},
		controlTestEnv("postgres://txharbor:controlsecret@127.0.0.1:1/control?sslmode=disable",
			"postgres://txharbor:datasecret@127.0.0.1:1/data?sslmode=disable", "deploy:manager"))
	if code != 1 || !strings.Contains(stderr, "control store unavailable") {
		t.Fatalf("unreachable control store must refuse: exit=%d stderr=%q", code, stderr)
	}
	for _, secret := range []string{"controlsecret", "datasecret", "postgres://"} {
		if strings.Contains(stdout, secret) || strings.Contains(stderr, secret) {
			t.Fatalf("output must not leak %q: stdout=%q stderr=%q", secret, stdout, stderr)
		}
	}
}

func TestControlBadOperationIDIsUsageError(t *testing.T) {
	env := controlTestEnv("postgres://u:c@127.0.0.1:1/control?sslmode=disable",
		"postgres://u:d@127.0.0.1:1/data?sslmode=disable", "deploy:manager")
	code, _, stderr := runControl(t, []string{"identity-map-set", "--principal", "deploy:alice",
		"--person-id", "person-a", "--operator", "ops", "--reason", "r", "--operation-id", "bad\nop"}, env)
	if code != 2 {
		t.Fatalf("a control-character operation_id is a usage error: exit=%d stderr=%q", code, stderr)
	}
}

// TestRecoveryAdminUnwiredActionsRemainStub pins the wiring boundary after
// B6/T021-T022: migrate/control/backup/verify-backup/restore act (or refuse
// fail-closed), the remaining actions are still the B0 stub, and a wired
// command that lacks required configuration refuses by exact key name instead
// of claiming success.
func TestRecoveryAdminUnwiredActionsRemainStub(t *testing.T) {
	env := controlTestEnv("postgres://u:c@127.0.0.1:1/control?sslmode=disable",
		"postgres://u:d@127.0.0.1:1/data?sslmode=disable", "deploy:manager")
	code, _, stderr := runRecoveryAdmin(t, []string{"instance-open", "--kind", "recovery"}, env)
	if code != 1 || !strings.Contains(stderr, "NOT IMPLEMENTED") {
		t.Fatalf("instance-open must remain the B0 stub: exit=%d stderr=%q", code, stderr)
	}
	code, _, stderr = runRecoveryAdmin(t, []string{"drill"}, env)
	if code != 1 || !strings.Contains(stderr, "NOT IMPLEMENTED") {
		t.Fatalf("drill must remain the B0 stub: exit=%d stderr=%q", code, stderr)
	}

	// backup is wired (T021): without the required artifact directory it
	// refuses by key name - it never reports NOT IMPLEMENTED and never claims
	// a backup.
	code, stdout, stderr := runRecoveryAdmin(t, []string{"backup", "--chain-id", "1"}, env)
	if code != 1 {
		t.Fatalf("backup without the required artifact dir must refuse: exit=%d stderr=%q", code, stderr)
	}
	if strings.Contains(stderr, "NOT IMPLEMENTED") {
		t.Fatalf("backup is a wired action; it must not answer NOT IMPLEMENTED: %q", stderr)
	}
	if !strings.Contains(stderr, config.EnvRecoveryArtifactDir) {
		t.Fatalf("the missing artifact dir must be refused by key name: %q", stderr)
	}
	if strings.Contains(stdout, "verification=") || strings.Contains(stdout, "backup_id=") {
		t.Fatalf("refused backup must not claim success: stdout=%q", stdout)
	}
}
