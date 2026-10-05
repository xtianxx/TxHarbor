// migrate_unit_test.go covers the pre-connection trust boundary of
// `recovery-admin migrate` without Docker: required configuration, target
// identity (control DSN == data DSN refused before any connection), malformed
// and unreachable control DSNs, redaction, and the untouched stub surface.
// The real migration/version-guard paths run in migrate_integration_test.go.
package recoveryadmin

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/xtianxx/txharbor/internal/config"
)

func migrateTestEnv(values map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}

func runMigrate(t *testing.T, args []string, env map[string]string) (int, string, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := Run(context.Background(), append([]string{"migrate"}, args...), Deps{
		Getenv: migrateTestEnv(env),
		Stdout: &stdout,
		Stderr: &stderr,
	})
	return code, stdout.String(), stderr.String()
}

func TestMigrateUsageErrors(t *testing.T) {
	for _, args := range [][]string{{}, {"bogus"}, {"up", "extra"}} {
		code, _, stderr := runMigrate(t, args, nil)
		if code != 2 {
			t.Fatalf("args %v: exit=%d want 2 (stderr=%q)", args, code, stderr)
		}
	}
}

func TestMigrateRequiresControlDSN(t *testing.T) {
	code, _, stderr := runMigrate(t, []string{"up"}, map[string]string{
		config.EnvPGDSN: "postgres://u:datasecret@127.0.0.1:5432/data?sslmode=disable",
	})
	if code != 1 {
		t.Fatalf("missing control DSN must refuse with exit 1, got %d", code)
	}
	if !strings.Contains(stderr, config.EnvRecoveryControlDSN) {
		t.Fatalf("refusal must name the missing key: %q", stderr)
	}
	if strings.Contains(stderr, "datasecret") {
		t.Fatalf("refusal must not leak the data DSN password: %q", stderr)
	}
}

func TestMigrateRequiresDataDSNForIndependence(t *testing.T) {
	code, _, stderr := runMigrate(t, []string{"status"}, map[string]string{
		config.EnvRecoveryControlDSN: "postgres://u:controlsecret@127.0.0.1:5432/control?sslmode=disable",
	})
	if code != 1 {
		t.Fatalf("missing data DSN must refuse with exit 1, got %d", code)
	}
	if !strings.Contains(stderr, config.EnvPGDSN) {
		t.Fatalf("refusal must name %s: %q", config.EnvPGDSN, stderr)
	}
	if strings.Contains(stderr, "controlsecret") {
		t.Fatalf("refusal must not leak the control DSN password: %q", stderr)
	}
}

func TestMigrateRefusesSameDatabaseTargetBeforeConnecting(t *testing.T) {
	dsn := "postgres://txharbor:samesecret@127.0.0.1:1/one_db?sslmode=disable"
	code, _, stderr := runMigrate(t, []string{"up"}, map[string]string{
		config.EnvRecoveryControlDSN: dsn,
		config.EnvPGDSN:              dsn,
	})
	if code != 1 {
		t.Fatalf("equal control/data DSNs must refuse with exit 1, got %d", code)
	}
	if !strings.Contains(stderr, "same database target") {
		t.Fatalf("refusal must state the same-target reason: %q", stderr)
	}
	if strings.Contains(stderr, "control store unavailable") {
		t.Fatalf("the same-target refusal must precede any connection attempt: %q", stderr)
	}
	if strings.Contains(stderr, "samesecret") {
		t.Fatalf("refusal must not leak the DSN password: %q", stderr)
	}
}

func TestMigrateRefusesSameTargetDifferentCredentials(t *testing.T) {
	code, _, stderr := runMigrate(t, []string{"status"}, map[string]string{
		config.EnvRecoveryControlDSN: "postgres://alice:controlsecret@127.0.0.1:1/one_db?sslmode=disable",
		config.EnvPGDSN:              "postgres://bob:datasecret@127.0.0.1:1/one_db?sslmode=disable",
	})
	if code != 1 || !strings.Contains(stderr, "same database target") {
		t.Fatalf("same host/port/database with different credentials must refuse: exit=%d stderr=%q", code, stderr)
	}
	if strings.Contains(stderr, "controlsecret") || strings.Contains(stderr, "datasecret") {
		t.Fatalf("refusal must not leak credentials: %q", stderr)
	}
}

func TestMigrateRefusesMalformedControlDSN(t *testing.T) {
	code, _, stderr := runMigrate(t, []string{"up"}, map[string]string{
		config.EnvRecoveryControlDSN: "postgres://user:sekret@localhost:700000/db",
		config.EnvPGDSN:              "postgres://user:pw@localhost:5432/data?sslmode=disable",
	})
	if code != 1 {
		t.Fatalf("malformed control DSN must refuse with exit 1, got %d", code)
	}
	if !strings.Contains(stderr, config.EnvRecoveryControlDSN) {
		t.Fatalf("refusal must name the offending key: %q", stderr)
	}
	if strings.Contains(stderr, "sekret") {
		t.Fatalf("refusal must redact the malformed DSN: %q", stderr)
	}
}

func TestMigrateRefusesUnreachableControlStore(t *testing.T) {
	code, stdout, stderr := runMigrate(t, []string{"up"}, map[string]string{
		config.EnvRecoveryControlDSN: "postgres://txharbor:controlsecret@127.0.0.1:1/control?sslmode=disable",
		config.EnvPGDSN:              "postgres://txharbor:datasecret@127.0.0.1:1/data?sslmode=disable",
	})
	if code != 1 {
		t.Fatalf("unreachable control store must refuse with exit 1, got %d", code)
	}
	if !strings.Contains(stderr, "control store unavailable") {
		t.Fatalf("refusal must report the unavailable control store: %q", stderr)
	}
	for _, secret := range []string{"controlsecret", "datasecret", "postgres://"} {
		if strings.Contains(stderr, secret) || strings.Contains(stdout, secret) {
			t.Fatalf("output must not leak %q: stdout=%q stderr=%q", secret, stdout, stderr)
		}
	}
}
