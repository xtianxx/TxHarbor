//go:build integration

// whitelist_integration_test.go owns the T020 end-to-end proof on real
// PostgreSQL: the FR-05 allowlist is resolved from the live 004 policy ledger
// (deposit_config_history), and a policy change is honored by the receipt core
// without any asset-list redefinition. It reuses the T006 container/migration
// helper (grantSetup) and the T010 receipt helpers (intakeKey, intakeSupplyGrant,
// intakeReq, SubmitWithdrawal).
package withdrawal

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// whitelistWatch is a valid (unrelated) watch address for the policy rows; the
// whitelist reader ignores watches.
const whitelistWatch = "0x3333333333333333333333333333333333333333"

// whitelistInsertPolicy appends one 004 policy version whose asset set is
// assets (lowercase `<address>:<effective>` lines). policyHash is any 64-hex
// config identity; the reader never consumes it.
func whitelistInsertPolicy(t *testing.T, ctx context.Context, pool *pgxpool.Pool, version int64, assets string) {
	t.Helper()
	_, err := pool.Exec(ctx, `
INSERT INTO deposit_config_history
    (chain_id, version_seq, config_hash, start_block, assets, watches, replay_from, request_id)
VALUES ($1, $2, $3, 0, $4, $5, 0, $6)`,
		intakeChainID, version, strings.Repeat("a", 64), assets, whitelistWatch+":0", "req-"+strings.Repeat("b", int(version)))
	if err != nil {
		t.Fatalf("insert deposit_config_history v%d: %v", version, err)
	}
}

// TestWithdrawalWhitelistPolicySourceT020 proves T020 on a real server: an asset
// present in the newest 004 policy version is accepted by the receipt core, and
// removing it from the policy source turns the next create into a 422.
func TestWithdrawalWhitelistPolicySourceT020(t *testing.T) {
	ctx, pool := grantSetup(t)

	key := intakeKey(t, ctx, pool, 7201)
	intakeSupplyGrant(t, ctx, pool, 7201, "auth-whitelist-1", intakeAmount)

	// Given the live policy source carries intakeAsset
	whitelistInsertPolicy(t, ctx, pool, 1, intakeAsset+":0")

	// When the allowlist is resolved from the policy source
	allowlist, err := ResolveAssetAllowlist(ctx, pool, intakeChainID)
	if err != nil {
		t.Fatalf("ResolveAssetAllowlist: %v", err)
	}
	if len(allowlist) != 1 || allowlist[0] != intakeAsset {
		t.Fatalf("resolved allowlist = %v, want [%s]", allowlist, intakeAsset)
	}

	// Then a create carrying that asset is accepted
	req := intakeReq(key, "idem-whitelist-1", "auth-whitelist-1")
	req.Allowlist = allowlist
	first, err := SubmitWithdrawal(ctx, pool, req)
	if err != nil {
		t.Fatalf("first SubmitWithdrawal: %v", err)
	}
	if first.Status != 201 {
		t.Fatalf("first status = %d (%+v), want 201", first.Status, first)
	}

	// Given the policy advances to a version without intakeAsset
	whitelistInsertPolicy(t, ctx, pool, 2, whitelistOther+":0")

	// When the allowlist is re-resolved
	allowlist2, err := ResolveAssetAllowlist(ctx, pool, intakeChainID)
	if err != nil {
		t.Fatalf("ResolveAssetAllowlist after update: %v", err)
	}
	if len(allowlist2) != 1 || allowlist2[0] != whitelistOther {
		t.Fatalf("re-resolved allowlist = %v, want [%s]", allowlist2, whitelistOther)
	}

	// Then the removed asset is rejected 422 and nothing is persisted
	req2 := intakeReq(key, "idem-whitelist-2", "auth-whitelist-1")
	req2.Allowlist = allowlist2
	second, err := SubmitWithdrawal(ctx, pool, req2)
	if err != nil {
		t.Fatalf("second SubmitWithdrawal: %v", err)
	}
	if second.Status != 422 || second.Code != CodeValidationFailed {
		t.Fatalf("second = %+v, want 422 %s", second, CodeValidationFailed)
	}
	if n := intakeRequestCount(t, ctx, pool); n != 1 {
		t.Fatalf("request rows = %d, want 1 (removed asset not persisted)", n)
	}
}

// TestResolveAssetAllowlistMissingPolicy proves a chain with no policy version
// is an explicit error, never an empty allowlist that silently accepts nothing
// or a full-chain fallback.
func TestResolveAssetAllowlistMissingPolicy(t *testing.T) {
	ctx, pool := grantSetup(t)

	_, err := ResolveAssetAllowlist(ctx, pool, 999999)
	if err == nil {
		t.Fatal("ResolveAssetAllowlist(missing chain) = nil, want error")
	}
}
