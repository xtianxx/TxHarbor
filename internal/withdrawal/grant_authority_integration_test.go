//go:build integration

// grant_authority_integration_test.go owns the T015 integration tests: the
// in-tx authority re-verification inside T-supply+scope (R-PB10). It reuses the
// T008 grant helpers (grantSetup, grantSeedCaller, grantTestOp) and adds only
// authority-prefixed helpers so no name collides with the T010/T023 files.
//
// The failure outcome is durable: it records `supply_refused` naming the failed
// check while writing ZERO grant/scope rows. That is the difference between a
// refused attempt and a partial write.
package withdrawal

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// grantAuthorityAllowlist loads an allowlist permitting exactly the given
// caller ids (deny-all when empty), through the production loader.
func grantAuthorityAllowlist(t *testing.T, callers ...int64) *IssuerAllowlist {
	t.Helper()
	parts := make([]string, 0, len(callers))
	for _, id := range callers {
		parts = append(parts, strconv.FormatInt(id, 10))
	}
	allow, err := LoadIssuerAllowlist(allowlistEnv(map[string]string{
		EnvIssuerCallers: strings.Join(parts, ","),
	}))
	if err != nil {
		t.Fatalf("LoadIssuerAllowlist: %v", err)
	}
	return allow
}

// grantAuthorityScopeCount counts scope rows for one grant.
func grantAuthorityScopeCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, authorizationID string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM withdrawal_authorization_scopes WHERE authorization_id = $1`, authorizationID).Scan(&n); err != nil {
		t.Fatalf("count scope rows for %q: %v", authorizationID, err)
	}
	return n
}

// grantAuthorityAuditReasonByOp reads the recorded refusal reason for one
// attempt (persistent state, never inferred from the returned error).
func grantAuthorityAuditReasonByOp(t *testing.T, ctx context.Context, pool *pgxpool.Pool, operationID string) string {
	t.Helper()
	var reason string
	if err := pool.QueryRow(ctx,
		`SELECT reason FROM withdrawal_grant_audit WHERE operation_id = $1`, operationID).Scan(&reason); err != nil {
		t.Fatalf("read audit reason for operation %q: %v", operationID, err)
	}
	return reason
}

// TestWithdrawalGrantAuthorityInTxCheck covers the two T015 obligations with a
// real PostgreSQL: a valid mapped credential passes the in-tx re-check and
// supplies, while a credential revoked before the attempt is observed by the
// in-tx `FOR SHARE` re-read and refused with zero grant/scope writes.
func TestWithdrawalGrantAuthorityInTxCheck(t *testing.T) {
	ctx, pool := grantSetup(t)
	grantSeedCaller(t, ctx, pool, 7401)
	allow := grantAuthorityAllowlist(t, 7401)
	key, ref, err := IssueKey(ctx, pool, 7401, "authority-test")
	if err != nil {
		t.Fatalf("issue key: %v", err)
	}

	t.Run("valid mapped credential supplies", func(t *testing.T) {
		const authID = "auth-authority-ok"
		op := grantTestOp("a0000000000000000000000000000001", authID, 7401, "100")
		auth := SupplyAuthority{PresentedKey: key, Issuers: allow}
		out, err := SupplyGrantAuthorized(ctx, pool, op, auth, "op", "authorized supply")
		if err != nil {
			t.Fatalf("authorized supply: %v", err)
		}
		if out.Action != grantOutcomeSupplied {
			t.Fatalf("outcome = %+v, want %s", out, grantOutcomeSupplied)
		}
		if n := grantCount(t, ctx, pool, authID); n != 1 {
			t.Fatalf("grant rows = %d, want 1", n)
		}
	})

	t.Run("revoked credential refused with zero writes", func(t *testing.T) {
		if err := RevokeKey(ctx, pool, ref.KeyID); err != nil {
			t.Fatalf("revoke key: %v", err)
		}
		const authID = "auth-authority-revoked"
		op := grantTestOp("a0000000000000000000000000000002", authID, 7401, "100")
		auth := SupplyAuthority{PresentedKey: key, Issuers: allow}
		out, err := SupplyGrantAuthorized(ctx, pool, op, auth, "op", "revoked supply")
		if out != nil {
			t.Fatalf("refused outcome = %+v, want nil", out)
		}
		var e *Error
		if !errors.As(err, &e) || e.Code != CodeUnauthorized {
			t.Fatalf("error = %v (%T), want *Error code %q", err, err, CodeUnauthorized)
		}
		if n := grantCount(t, ctx, pool, authID); n != 0 {
			t.Fatalf("grant rows after revoked supply = %d, want 0", n)
		}
		if n := grantAuthorityScopeCount(t, ctx, pool, authID); n != 0 {
			t.Fatalf("scope rows after revoked supply = %d, want 0", n)
		}
		acted := grantAuditActions(t, ctx, pool, authID)
		if len(acted) != 1 || acted[0] != grantOutcomeSupplyRefused {
			t.Fatalf("audit actions = %v, want exactly [%s]", acted, grantOutcomeSupplyRefused)
		}
		if reason := grantAuthorityAuditReasonByOp(t, ctx, pool, op.OperationID); !strings.Contains(reason, authorityCheckAPIKey) {
			t.Fatalf("audit reason = %q, want it to name the failed %s check", reason, authorityCheckAPIKey)
		}
	})
}

// TestWithdrawalGrantAuthorityUnmappedRefused covers the allowlist re-check
// inside the tx: a valid credential whose caller is absent from the loaded
// mapping is refused with zero grant/scope writes.
func TestWithdrawalGrantAuthorityUnmappedRefused(t *testing.T) {
	ctx, pool := grantSetup(t)
	grantSeedCaller(t, ctx, pool, 7402)
	allow := grantAuthorityAllowlist(t) // deny-all
	key, _, err := IssueKey(ctx, pool, 7402, "authority-unmapped")
	if err != nil {
		t.Fatalf("issue key: %v", err)
	}

	const authID = "auth-authority-unmapped"
	op := grantTestOp("a0000000000000000000000000000003", authID, 7402, "100")
	auth := SupplyAuthority{PresentedKey: key, Issuers: allow}
	if out, err := SupplyGrantAuthorized(ctx, pool, op, auth, "op", "unmapped"); out != nil || err == nil {
		t.Fatalf("unmapped supply = (%+v, %v), want (nil, refusal)", out, err)
	}
	if n := grantCount(t, ctx, pool, authID); n != 0 {
		t.Fatalf("grant rows = %d, want 0", n)
	}
	if n := grantAuthorityScopeCount(t, ctx, pool, authID); n != 0 {
		t.Fatalf("scope rows = %d, want 0", n)
	}
	if reason := grantAuthorityAuditReasonByOp(t, ctx, pool, op.OperationID); !strings.Contains(reason, authorityCheckIssuers) {
		t.Fatalf("audit reason = %q, want it to name the failed %s check", reason, authorityCheckIssuers)
	}
}

// TestWithdrawalGrantAuthorityRevokeRace releases a key revocation and a supply
// on the same credential together. The api_key `FOR SHARE` re-read and the
// revoke's single-statement UPDATE are the same lock, so exactly one order is
// observed: either the revoke commits first and the supply is refused with zero
// grant/scope rows, or the supply's SHARE wins and it commits before the revoke
// takes effect. Both outcomes are legal; a partial grant/scope write is not.
func TestWithdrawalGrantAuthorityRevokeRace(t *testing.T) {
	ctx, pool := grantSetup(t)
	grantSeedCaller(t, ctx, pool, 7403)
	allow := grantAuthorityAllowlist(t, 7403)
	key, ref, err := IssueKey(ctx, pool, 7403, "authority-race")
	if err != nil {
		t.Fatalf("issue key: %v", err)
	}

	const authID = "auth-authority-race"
	op := grantTestOp("a0000000000000000000000000000004", authID, 7403, "100")

	idempotencyProvePoolCapacity(t, ctx, pool, 2)
	start := make(chan struct{})
	var (
		wg     sync.WaitGroup
		supply error
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		_, supply = SupplyGrantAuthorized(ctx, pool, op,
			SupplyAuthority{PresentedKey: key, Issuers: allow}, "op", "race supply")
	}()
	go func() {
		defer wg.Done()
		<-start
		_ = RevokeKey(ctx, pool, ref.KeyID)
	}()
	close(start)
	wg.Wait()

	grantRows := grantCount(t, ctx, pool, authID)
	scopeRows := grantAuthorityScopeCount(t, ctx, pool, authID)
	if supply == nil {
		// The supply won the share and committed: it must be complete.
		if grantRows != 1 || scopeRows != 0 {
			t.Fatalf("successful race supply left grant=%d scope=%d, want 1/0", grantRows, scopeRows)
		}
		if acted := grantAuditActions(t, ctx, pool, authID); len(acted) != 1 || acted[0] != grantOutcomeSupplied {
			t.Fatalf("audit actions = %v, want [%s]", acted, grantOutcomeSupplied)
		}
	} else {
		// The revoke committed first and the loser observed it: zero writes.
		if grantRows != 0 || scopeRows != 0 {
			t.Fatalf("refused race supply left grant=%d scope=%d, want 0/0", grantRows, scopeRows)
		}
		if acted := grantAuditActions(t, ctx, pool, authID); len(acted) != 1 || acted[0] != grantOutcomeSupplyRefused {
			t.Fatalf("audit actions = %v, want [%s]", acted, grantOutcomeSupplyRefused)
		}
	}
}
