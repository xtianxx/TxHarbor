//go:build integration

// execution_recovery_gate_integration_test.go is the T032 acceptance of the
// execution HTTP write path (F2): POST /withdrawals/{request_id}/execution has
// its own 015 existing_withdrawal_recovery admission — a different funnel from
// the CLI `withdrawal-exec` (execOperatorOp) — installed before
// execution.Admit. While a recovery instance is open and the process is
// unbound, the route refuses with the closed refusal_class exposed and
// audited, and no payment intent, claim or withdrawal row is created.
//
// It reuses the T026 scene helpers (recovery_isolation_integration_test.go):
// one PostgreSQL container with the migrated data database and an independent
// control-store database. The handler's lazy assembly seam is bound to the
// same environment the serve process receives, so the real control store and
// the real T012 gate run.
package app

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/xtianxx/txharbor/internal/recovery"
)

// TestRecoveryIsolationExecutionWritePathDefaultDeny is the execution HTTP
// negative case of T032: an open recovery instance, an unbound serve process,
// and a POST to the execution route without any credential — the 015 refusal
// arrives ahead of the 011 authentication/permission/admission order, and the
// data database shows no effect.
func TestRecoveryIsolationExecutionWritePathDefaultDeny(t *testing.T) {
	scene := recovNewScene(t, true)
	scene.openRecoveryInstance(t)

	addr := freeAddr(t)
	env := scene.serveEnv(addr)

	// The handler assembles its admission lazily from the process environment;
	// bind the seam to the serve process environment so the production
	// assembly path (config keys → version-guarded store → T012 gate) runs
	// exactly as it would in the process.
	previous := withdrawalExecutionRecoveryLoad
	withdrawalExecutionRecoveryLoad = func(ctx context.Context, chainID uint64, _ func(string) (string, bool)) (*existingWithdrawalRecoveryWiring, error) {
		cfg, err := existingWithdrawalRecoveryConfigFromEnv(envGetter(env), chainID)
		if err != nil {
			return nil, err
		}
		return assembleExistingWithdrawalRecovery(ctx, cfg, envGetter(env))
	}
	defer func() { withdrawalExecutionRecoveryLoad = previous }()

	h := recovStartServe(t, env, addr)
	defer h.stop()
	h.waitReady(90 * time.Second)

	auditBefore := recovRefusedAuditCount(t, scene)

	// No body, no credential: the recovery admission is the first decision, so
	// the refusal is neither a 400 nor a 401.
	status, body := recovHTTP(t, http.MethodPost, h.base()+"/withdrawals/recov-exec-1/execution", nil, "")
	recovRefusalCheck(t, status, body, recovery.RefusalInstanceMismatch, "POST /withdrawals/{request_id}/execution")

	if n := recovCount(t, scene.ctx, scene.dataPool,
		`SELECT count(*) FROM payment_intents WHERE request_id = 'recov-exec-1'`); n != 0 {
		t.Errorf("payment_intents for the refused request = %d, want 0 (no admission may run)", n)
	}
	if n := recovCount(t, scene.ctx, scene.dataPool, `SELECT count(*) FROM execution_claims`); n != 0 {
		t.Errorf("execution_claims = %d, want 0 (a refused admission must not claim)", n)
	}
	if n := recovCount(t, scene.ctx, scene.dataPool, `SELECT count(*) FROM withdrawal_requests`); n != 0 {
		t.Errorf("withdrawal_requests = %d, want 0", n)
	}
	if after := recovRefusedAuditCount(t, scene); after <= auditBefore {
		t.Errorf("execution write-path refusal was not audited (before=%d after=%d); the closed refusal_class must be audited",
			auditBefore, after)
	}
}
