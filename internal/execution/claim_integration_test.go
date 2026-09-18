//go:build integration

// claim_integration_test.go executes quickstart V2 (T020) against real
// PostgreSQL with real connections: N-way claim exclusivity, renewal on the
// same version, DB-clock expiry disqualification, takeover by a new version,
// and exact-version fencing of the old holder's writes.
package execution

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func seedExecIntentRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, intentID string) {
	t.Helper()
	seedExecCaller(t, ctx, pool, 1)
	authz := "authz-" + intentID
	seedExecAuthorization(t, ctx, pool, authz, 1, "active", nil)
	seedExecRequest(t, ctx, pool, "req-"+intentID, authz, 1)
	if _, err := pool.Exec(ctx, `INSERT INTO payment_intents
		(intent_id, request_id, chain_id, sender, authorization_id, authorization_version,
		 state, admitted_recovery_version)
		VALUES ($1, $2, $3, $4, $5, 1, 'admitted', 0)`,
		intentID, "req-"+intentID, execChainID, execSender, authz); err != nil {
		t.Fatalf("seed intent %s: %v", intentID, err)
	}
}

func execClaimStore(t *testing.T, pool *pgxpool.Pool) *ClaimStore {
	t.Helper()
	store, err := NewClaimStore(pool, 30*time.Second, 5*time.Minute)
	if err != nil {
		t.Fatalf("NewClaimStore: %v", err)
	}
	return store
}

func expireExecClaim(t *testing.T, ctx context.Context, pool *pgxpool.Pool, intentID string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `UPDATE execution_claims
		SET acquired_at = now() - interval '1 hour', expires_at = now() - interval '1 second'
		WHERE intent_id = $1`, intentID); err != nil {
		t.Fatalf("expire claim %s: %v", intentID, err)
	}
}

func claimVersion(t *testing.T, ctx context.Context, pool *pgxpool.Pool, intentID string) (int64, bool) {
	t.Helper()
	c, found, err := ReadClaim(ctx, pool, intentID)
	if err != nil {
		t.Fatalf("ReadClaim: %v", err)
	}
	return c.LeaseVersion, found
}

func TestClaimExclusivityRenewalExpiry(t *testing.T) {
	ctx, pool := executionPool(t)
	seedExecIntentRow(t, ctx, pool, "intent-claim-1")
	store := execClaimStore(t, pool)

	const n = 8
	var wg sync.WaitGroup
	versions := make([]int64, n)
	wins := make([]bool, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			versions[i], wins[i], errs[i] = store.Acquire(ctx, "intent-claim-1", "owner-"+string(rune('a'+i)))
		}(i)
	}
	wg.Wait()
	winner := -1
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("Acquire[%d]: %v", i, errs[i])
		}
		if wins[i] {
			if winner >= 0 {
				t.Fatalf("more than one winner: %d and %d", winner, i)
			}
			winner = i
		}
	}
	if winner < 0 {
		t.Fatal("no winner among concurrent Acquire calls")
	}
	if versions[winner] != 1 {
		t.Fatalf("winner version = %d, want 1", versions[winner])
	}
	if v, found := claimVersion(t, ctx, pool, "intent-claim-1"); !found || v != 1 {
		t.Fatalf("stored claim = v%d found=%v, want v1", v, found)
	}

	owner := "owner-" + string(rune('a'+winner))
	var before time.Time
	if err := pool.QueryRow(ctx, `SELECT expires_at FROM execution_claims WHERE intent_id = 'intent-claim-1'`).Scan(&before); err != nil {
		t.Fatalf("read expires_at: %v", err)
	}
	time.Sleep(30 * time.Millisecond)
	if err := store.Renew(ctx, "intent-claim-1", owner, 1); err != nil {
		t.Fatalf("Renew: %v", err)
	}
	var after time.Time
	if err := pool.QueryRow(ctx, `SELECT expires_at FROM execution_claims WHERE intent_id = 'intent-claim-1'`).Scan(&after); err != nil {
		t.Fatalf("read expires_at after renew: %v", err)
	}
	if !after.After(before) {
		t.Fatalf("expires_at did not advance: %s -> %s", before, after)
	}
	if v, _ := claimVersion(t, ctx, pool, "intent-claim-1"); v != 1 {
		t.Fatalf("renewal changed the version to %d", v)
	}

	// Expire the winner on the DB clock; its fenced write must affect 0 rows.
	expireExecClaim(t, ctx, pool, "intent-claim-1")
	if err := store.AdvanceProgress(ctx, "intent-claim-1", owner, 1); !errors.Is(err, ErrClaimLost) {
		t.Fatalf("expired fenced write = %v, want ErrClaimLost", err)
	}
	var expired bool
	if err := pool.QueryRow(ctx, `SELECT expires_at <= now() FROM execution_claims WHERE intent_id = 'intent-claim-1'`).Scan(&expired); err != nil || !expired {
		t.Fatalf("claim expired flag = %v (err %v), want true", expired, err)
	}

	v2, ok, err := store.Acquire(ctx, "intent-claim-1", "owner-b")
	if err != nil || !ok {
		t.Fatalf("takeover Acquire = %v/%v, want won", ok, err)
	}
	if v2 != 2 {
		t.Fatalf("takeover version = %d, want 2", v2)
	}
	// The old version is fenced; the new holder can advance progress.
	if err := store.AdvanceProgress(ctx, "intent-claim-1", owner, 1); !errors.Is(err, ErrClaimLost) {
		t.Fatalf("old-version write = %v, want ErrClaimLost", err)
	}
	if err := store.AdvanceProgress(ctx, "intent-claim-1", "owner-b", 2); err != nil {
		t.Fatalf("new-holder write: %v", err)
	}
}
