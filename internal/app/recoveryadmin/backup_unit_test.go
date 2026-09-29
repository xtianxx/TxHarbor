package recoveryadmin

import (
	"strings"
	"testing"

	"github.com/xtianxx/txharbor/internal/config"
)

func TestVerifyBackupRequiresObserverBeforeOpeningControlStore(t *testing.T) {
	env := controlTestEnv("postgres://u:control-secret@127.0.0.1:1/control?sslmode=disable",
		"postgres://u:data-secret@127.0.0.1:1/data?sslmode=disable", "auth:verifier")
	code, stdout, stderr := runRecoveryAdmin(t, []string{"verify-backup", "--manifest", "manifest.json"}, env)
	if code != 1 || !strings.Contains(stderr, config.EnvRecoveryObserverDSN) {
		t.Fatalf("missing observer must refuse by key name: exit=%d stdout=%q stderr=%q", code, stdout, stderr)
	}
	if strings.Contains(stderr, "control-secret") || strings.Contains(stderr, "data-secret") {
		t.Fatalf("missing observer refusal leaked DSN material: %q", stderr)
	}
}
