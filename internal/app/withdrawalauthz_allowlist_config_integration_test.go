//go:build integration

// withdrawalauthz_allowlist_config_integration_test.go owns the V1 CLI contract
// (012-007-authorization-carrier): an illegal TXHARBOR_AUTHZ_ISSUER_CALLERS
// value is a startup configuration error the supply carrier reports with exit
// code 2 (withdrawal.LoadIssuerAllowlist -> withdrawalAuthzAllowlist), and the
// allowlist is loaded BEFORE any pool is opened (withdrawalAuthzAllowlist runs
// before withdrawalAuthzConnect in withdrawalauthz.go), so no grant/scope/audit
// row can be written. The legacy scopeless supply path still loads the mapping,
// so the illegal value is refused regardless of --api-key.
package app

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/withdrawal"
)

// deadPGDSN is a syntactically valid DSN whose host refuses connections: any
// pool dial is a guaranteed exit-1 failure. An illegal allowlist on this DSN
// must still return exit 2, which proves the config error precedes the pool.
const deadPGDSN = "postgres://txharbor:sup3rs3cret@127.0.0.1:1/txharbor?sslmode=disable"

// TestWithdrawalAuthzIllegalAllowlistConfigErrorExit2 is V1. It pins exit code 2
// and zero rows for an illegal issuance allowlist, and pins the load-before-pool
// order with an unreachable database.
func TestWithdrawalAuthzIllegalAllowlistConfigErrorExit2(t *testing.T) {
	const (
		authID    = "authz-allowlist-illegal"
		asset     = "0x1111111111111111111111111111111111111111"
		recipient = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	)
	supplyArgs := func(opID string) []string {
		return []string{
			"supply",
			"--operation-id", opID,
			"--authorization-id", authID,
			"--caller-id", "8301",
			"--chain-id", "31337",
			"--asset", asset,
			"--recipient", recipient,
			"--amount", "100",
			"--operator", "op-allowlist",
			"--reason", "illegal allowlist",
		}
	}
	wantConfigError := func(t *testing.T, stderr string) {
		t.Helper()
		for _, want := range []string{"configuration error", withdrawal.EnvIssuerCallers, "abc"} {
			if !strings.Contains(stderr, want) {
				t.Fatalf("stderr %q lacks %q", stderr, want)
			}
		}
	}

	// The config error must win over an unreachable database: without the
	// illegal allowlist this same invocation would fail the dial with exit 1,
	// so exit 2 here proves the allowlist loads before withdrawalAuthzConnect.
	t.Run("config error precedes pool use", func(t *testing.T) {
		ctx := context.Background()
		env := withdrawalAuthzEnv(deadPGDSN)
		env[withdrawal.EnvIssuerCallers] = "abc"
		opID := withdrawalAuthzMintID(t, ctx, env)
		code, stdout, stderr := withdrawalAuthzRun(ctx, env, supplyArgs(opID)...)
		if code != 2 {
			t.Fatalf("exit code = %d, want 2 (a dial failure would be 1); stdout=%s stderr=%s", code, stdout, stderr)
		}
		wantConfigError(t, stderr)
	})

	// Against a real migrated scratch database the illegal config still exits 2
	// and writes zero grant/scope/audit rows.
	t.Run("illegal allowlist writes zero rows", func(t *testing.T) {
		dsn := startConfirmAuthPostgres(t)
		ctx := context.Background()

		pool, err := db.OpenPool(ctx, dsn, 5*time.Second)
		if err != nil {
			t.Fatalf("open pool: %v", err)
		}
		defer pool.Close()

		env := withdrawalAuthzEnv(dsn)
		env[withdrawal.EnvIssuerCallers] = "abc"
		opID := withdrawalAuthzMintID(t, ctx, env)
		code, stdout, stderr := withdrawalAuthzRun(ctx, env, supplyArgs(opID)...)
		if code != 2 {
			t.Fatalf("exit code = %d, want 2; stdout=%s stderr=%s", code, stdout, stderr)
		}
		wantConfigError(t, stderr)
		if n := withdrawalAuthzGrantCount(t, ctx, pool, authID); n != 0 {
			t.Fatalf("grant rows = %d, want 0", n)
		}
		if n := withdrawalAuthzScopeCount(t, ctx, pool, authID); n != 0 {
			t.Fatalf("scope rows = %d, want 0", n)
		}
		if got := withdrawalAuthzAuditActions(t, ctx, pool, authID); len(got) != 0 {
			t.Fatalf("audit actions = %v, want none", got)
		}
		if n := withdrawalAuthzAuditCountByOp(t, ctx, pool, opID); n != 0 {
			t.Fatalf("audit rows for operation %q = %d, want 0", opID, n)
		}
	})
}
