//go:build integration && linux

package recoveryadmin

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// Run the shipped command entrypoint with realistic secret-bearing ambient
// application variables. A conflicting PG override must be rejected before
// any action, and the refusal must remain generic and credential-free.
func TestRecoveryAdminCLIRejectsAmbientPGEnvironmentWithoutLeakingCanaries(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate integration test source")
	}
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "../../../"))
	bin := filepath.Join(t.TempDir(), "txharbor")
	build := exec.Command("go", "build", "-o", bin, "./cmd/txharbor")
	build.Dir = repoRoot
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build production CLI: %v: %s", err, out)
	}
	canaries := []string{
		"postgres://operator:cli-target-password@db.invalid/target",
		"postgres://operator:cli-control-password@db.invalid/control",
		"postgres://operator:cli-data-password@db.invalid/data",
		"https://rpc.invalid/cli-rpc-api-key", "cli-signer-secret", "cli-vault-api-key",
		"postgres://operator:cli-broker-password@db.invalid/broker", "cli-custom-secret",
	}
	keys := []string{"TXHARBOR_RECOVERY_TARGET_DSN", "TXHARBOR_RECOVERY_CONTROL_DSN", "TXHARBOR_PG_DSN", "TXHARBOR_RPC_URL", "TXHARBOR_SIGNER_TOKEN", "VAULT_TOKEN", "BROKER_DSN", "TESTSECRET"}
	previous := make(map[string]string, len(keys))
	wasSet := make(map[string]bool, len(keys))
	for i, key := range keys {
		previous[key], wasSet[key] = os.LookupEnv(key)
		_ = os.Setenv(key, canaries[i%len(canaries)])
	}
	defer func() {
		for _, key := range keys {
			if wasSet[key] {
				_ = os.Setenv(key, previous[key])
			} else {
				_ = os.Unsetenv(key)
			}
		}
	}()
	previousPGHost, hadPGHost := os.LookupEnv("PGHOST")
	_ = os.Setenv("PGHOST", "ambient-override-canary")
	defer func() {
		if hadPGHost {
			_ = os.Setenv("PGHOST", previousPGHost)
		} else {
			_ = os.Unsetenv("PGHOST")
		}
	}()
	cmd := exec.CommandContext(context.Background(), bin, "recovery-admin", "backup", "--chain-id", "1", "--out", t.TempDir())
	cmd.Env = os.Environ()
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err == nil {
		t.Fatal("CLI accepted ambient PGHOST override")
	}
	combined := stdout.String() + stderr.String()
	if strings.Contains(combined, "ambient-override-canary") || strings.Contains(combined, "secret") {
		t.Fatalf("CLI refusal leaked credential-shaped input: %q", combined)
	}
	for _, canary := range canaries {
		if strings.Contains(combined, canary) {
			t.Fatalf("CLI output leaked environment canary: %q", combined)
		}
	}
}
