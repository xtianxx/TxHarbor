//go:build integration

// stall_integration_test.go executes quickstart V5 (T026, M3/FR-15): the stall
// predicate is re-evaluated under the claim row lock, progress prevents
// takeover while a stale watermark allows it with full evidence, renewal alone
// does not prevent takeover, the sweep marks without taking over and changes no
// business state, and claim-revoke requires evidence and the exact version.
package execution

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func setStaleProgress(t *testing.T, ctx context.Context, pool *pgxpool.Pool, intentID string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `UPDATE execution_claims
		SET last_progress_at = now() - interval '10 minutes' WHERE intent_id = $1`, intentID); err != nil {
		t.Fatalf("set stale progress: %v", err)
	}
}

func TestStallNoFalseTakeover(t *testing.T) {
	ctx, pool := executionPool(t)
	store := execClaimStore(t, pool)
	seedExecClaimedIntent(t, ctx, pool, "req-stall-1", "authz-stall-1", "intent-stall-1", "owner-a", store)

	res, err := store.Takeover(ctx, "intent-stall-1", "owner-b")
	if err != nil {
		t.Fatalf("Takeover: %v", err)
	}
	if res.Acquired {
		t.Fatalf("recent-progress assumption was taken over: %+v", res)
	}
	if got, err := store.Claim(ctx, "intent-stall-1", "owner-b"); err != nil || got.Acquired {
		t.Fatalf("Claim on a fresh claim = %+v (err %v), want not acquired", got, err)
	}
	if v, _ := claimVersion(t, ctx, pool, "intent-stall-1"); v != 1 {
		t.Fatalf("version changed to %d without a valid takeover", v)
	}
}

func TestStallTakeoverRenewalAndSweep(t *testing.T) {
	ctx, pool := executionPool(t)
	store := execClaimStore(t, pool)
	v1 := seedExecClaimedIntent(t, ctx, pool, "req-stall-2", "authz-stall-2", "intent-stall-2", "owner-a", store)
	before := intentState(t, ctx, pool, "intent-stall-2")

	// Renewal extends the lease but is not progress.
	if err := store.Renew(ctx, "intent-stall-2", "owner-a", v1); err != nil {
		t.Fatalf("Renew: %v", err)
	}
	setStaleProgress(t, ctx, pool, "intent-stall-2")

	// The sweep marks idempotently without taking over and without touching
	// business state.
	marked, err := store.SweepStalls(ctx)
	if err != nil || marked != 1 {
		t.Fatalf("first sweep = %d (err %v), want 1", marked, err)
	}
	marked, err = store.SweepStalls(ctx)
	if err != nil || marked != 0 {
		t.Fatalf("second sweep = %d (err %v), want 0 (idempotent)", marked, err)
	}
	var stallEvents int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM execution_events WHERE intent_id = 'intent-stall-2' AND kind = 'stall_flagged'`).Scan(&stallEvents); err != nil || stallEvents != 1 {
		t.Fatalf("stall_flagged events = %d (err %v), want 1", stallEvents, err)
	}
	if got := intentState(t, ctx, pool, "intent-stall-2"); got != before {
		t.Fatalf("sweep changed business state %s -> %s", before, got)
	}
	if v, _ := claimVersion(t, ctx, pool, "intent-stall-2"); v != v1 {
		t.Fatalf("sweep changed the version to %d", v)
	}

	// Takeover: +1, new owner, taken_over event with prior owner/version.
	res, err := store.Takeover(ctx, "intent-stall-2", "owner-b")
	if err != nil || !res.Acquired || !res.TakenOver {
		t.Fatalf("Takeover = %+v (err %v), want acquired takeover", res, err)
	}
	if res.Version != v1+1 || res.PriorOwner != "owner-a" || res.PriorVersion != v1 {
		t.Fatalf("takeover evidence = %+v, want prior owner-a v%d and new v%d", res, v1, v1+1)
	}
	var detail string
	if err := pool.QueryRow(ctx, `SELECT detail FROM execution_events
		WHERE intent_id = 'intent-stall-2' AND kind = 'taken_over' ORDER BY event_id DESC LIMIT 1`).Scan(&detail); err != nil {
		t.Fatalf("read taken_over event: %v", err)
	}
	if detail == "" {
		t.Fatal("taken_over event carries no prior owner/version/watermark evidence")
	}
	// The old holder's next write is fenced.
	if err := store.AdvanceProgress(ctx, "intent-stall-2", "owner-a", v1); !errors.Is(err, ErrClaimLost) {
		t.Fatalf("old-holder write after takeover = %v, want ErrClaimLost", err)
	}
	// Repeated re-claiming is not progress: the new claim is fresh, so a third
	// holder cannot take over immediately.
	if got, err := store.Takeover(ctx, "intent-stall-2", "owner-c"); err != nil || got.Acquired {
		t.Fatalf("immediate re-takeover = %+v (err %v), want not acquired", got, err)
	}
}

func TestRevokeClaimEvidenceAndVersion(t *testing.T) {
	ctx, pool := executionPool(t)
	store := execClaimStore(t, pool)
	v1 := seedExecClaimedIntent(t, ctx, pool, "req-revoke", "authz-revoke", "intent-revoke", "owner-a", store)

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := RevokeClaimTx(ctx, tx, "intent-revoke", v1, "op", ""); err == nil {
		_ = tx.Rollback(ctx)
		t.Fatal("RevokeClaimTx accepted empty evidence")
	}
	_ = tx.Rollback(ctx)

	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	applied, err := RevokeClaimTx(ctx, tx, "intent-revoke", v1+1, "op", "evidence")
	if err != nil || applied {
		_ = tx.Rollback(ctx)
		t.Fatalf("version-mismatch revoke = %v (err %v), want refused with zero writes", applied, err)
	}
	_ = tx.Rollback(ctx)
	if v, found := claimVersion(t, ctx, pool, "intent-revoke"); !found || v != v1 {
		t.Fatalf("claim after refused revoke = v%d found=%v, want v%d untouched", v, found, v1)
	}

	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	applied, err = RevokeClaimTx(ctx, tx, "intent-revoke", v1, "op", "evidence")
	if err != nil || !applied {
		_ = tx.Rollback(ctx)
		t.Fatalf("exact-version revoke = %v (err %v), want applied", applied, err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit revoke: %v", err)
	}
	var state string
	if err := pool.QueryRow(ctx, `SELECT state FROM execution_claims WHERE intent_id = 'intent-revoke'`).Scan(&state); err != nil || state != "revoked" {
		t.Fatalf("claim state = %s (err %v), want revoked", state, err)
	}
	var revokedEvents int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM execution_events WHERE intent_id = 'intent-revoke' AND kind = 'revoked'`).Scan(&revokedEvents); err != nil || revokedEvents != 1 {
		t.Fatalf("revoked events = %d (err %v), want 1", revokedEvents, err)
	}
}
