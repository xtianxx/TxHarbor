//go:build integration

package recoveryadmin

import (
	"strings"
	"testing"

	"github.com/xtianxx/txharbor/internal/config"
)

func TestRecoveryAdminInstanceCloseRejectsMismatchedProcessBinding(t *testing.T) {
	f := newCLIFixture(t)
	env := f.env("deploy:executor")
	env[config.EnvRecoveryInstance] = f.instanceID
	otherID := "00000000-0000-4000-8000-000000000001"
	code, stdout, stderr := runRecoveryAdmin(t, []string{
		"instance-close", "--instance", otherID, "--operation-id", "instance-binding-mismatch",
	}, env)
	if code != 1 || !strings.Contains(stderr, "does not match "+config.EnvRecoveryInstance) {
		t.Fatalf("close with a different process-bound instance = exit %d, stdout=%q stderr=%q; want binding refusal", code, stdout, stderr)
	}
	var state string
	if err := f.control.QueryRow(f.ctx,
		`SELECT state FROM recovery_instance WHERE instance_id = $1`, f.instanceID).Scan(&state); err != nil {
		t.Fatalf("read bound instance state: %v", err)
	}
	if state != "open" {
		t.Fatalf("bound instance state after mismatch = %q, want open", state)
	}
	var auditRows int
	if err := f.control.QueryRow(f.ctx,
		`SELECT count(*) FROM recovery_audit WHERE action = 'instance_close' AND operation_id = $1`,
		"instance-binding-mismatch").Scan(&auditRows); err != nil {
		t.Fatalf("count close audit rows: %v", err)
	}
	if auditRows != 0 {
		t.Fatalf("pre-dispatch instance binding mismatch wrote %d instance-close audit row(s), want none", auditRows)
	}
}
