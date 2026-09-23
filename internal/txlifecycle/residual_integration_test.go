//go:build integration

package txlifecycle

import (
	"context"
	"testing"
)

// TestT041ResidualEvidence is T041: G-010-2(b) region abort evidence,
// G-010-2(c) freeze + controlled release, and G-010-1 last-moment expiry.
func TestT041ResidualEvidence(t *testing.T) {
	t.Run("region_aborted_no_dispatch", func(t *testing.T) {
		// Destructive lane (DROP TABLE): dedicated container per whitelist.
		e := newDedicatedEnv(t)
		ctx := context.Background()
		f := e.seed()
		f.sign()
		// Force a hard pre-dispatch storage failure: the hold gate read can no
		// longer run, which is a reliably-preventable abort (G-010-2(b)).
		e.exec(`DROP TABLE nonce_scope_holds`)
		before := e.rpc.dispatchCount()
		_, err := e.send(f, SendInitial, nil)
		if err == nil {
			t.Fatal("expected a region abort error")
		}
		if got := e.rpc.dispatchCount(); got != before {
			t.Fatalf("region abort dispatched: %d -> %d", before, got)
		}
		var aborts, sends int
		if err := e.pool.QueryRow(ctx,
			`SELECT count(*) FROM tx_attempt_events WHERE attempt_id = $1 AND event = 'region_aborted_no_dispatch'`, f.attemptID).Scan(&aborts); err != nil {
			t.Fatal(err)
		}
		if aborts != 1 {
			t.Fatalf("region_aborted_no_dispatch events = %d, want 1", aborts)
		}
		if err := e.pool.QueryRow(ctx, `SELECT count(*) FROM tx_send_attempts WHERE attempt_id = $1`, f.attemptID).Scan(&sends); err != nil {
			t.Fatal(err)
		}
		if sends != 0 {
			t.Fatalf("region abort wrote send rows: %d", sends)
		}
		attempt, err := e.store.AttemptByID(ctx, f.attemptID)
		if err != nil {
			t.Fatal(err)
		}
		if attempt.State != "signed" {
			t.Fatalf("attempt state after abort = %s, want signed (unchanged)", attempt.State)
		}
	})

	t.Run("lock_loss_freeze_and_release", func(t *testing.T) {
		e := newEnv(t)
		ctx := context.Background()
		f := e.seed()
		f.sign()
		res, err := e.send(f, SendInitial, nil)
		if err != nil || res.Outcome != "accepted" {
			t.Fatalf("initial send = %+v %v", res, err)
		}
		// Prove a stale basis: a pause committed before the dispatch that
		// recorded pause=none (G-010-2 class (c)).
		e.exec(`INSERT INTO indexer_pause (chain_id, height, expected_hash, actual_hash, kind, created_at)
			VALUES ($1,100,$2,$3,'hash_mismatch', now() - interval '1 hour')`,
			e.chainID, blockHashHex(100), blockHashHex(101))
		if _, err := e.store.Reconcile(ctx, f.attemptID, ""); err != nil {
			t.Fatal(err)
		}
		var freezes int
		if err := e.pool.QueryRow(ctx, `SELECT count(*) FROM tx_intent_freezes WHERE intent_id = $1 AND released_at IS NULL`, f.intentID).Scan(&freezes); err != nil {
			t.Fatal(err)
		}
		if freezes != 1 {
			t.Fatalf("freeze rows = %d, want 1", freezes)
		}
		// Frozen: every further send refuses, zero dispatch; reconcile stays allowed.
		before := e.rpc.dispatchCount()
		blockedRes, err := e.send(f, SendReplay, nil)
		assertBlocked(t, e, f.attemptID, blockedRes, err, ClassIntentFrozen, before)
		if _, err := e.store.Reconcile(ctx, f.attemptID, ""); err != nil {
			t.Fatalf("reconcile while frozen: %v", err)
		}

		// Controlled release: wrong permission refuses; correct permission lifts
		// only the freeze cause.
		e.store.WithReleaseToken("release-secret")
		if _, err := e.store.Release(ctx, &ReleaseRequest{IntentID: f.intentID, Permission: "wrong"}); refusalClass(err) != ClassReleaseNotPermitted {
			t.Fatalf("wrong permission = %v", err)
		}
		if ok, err := e.store.Release(ctx, &ReleaseRequest{
			IntentID: f.intentID, Permission: "release-secret", Operator: "ops-1", Reason: "reviewed", Basis: "evidence-1",
		}); err != nil || !ok {
			t.Fatalf("release = %v %v", ok, err)
		}
		// The pause is still present: release never overrides another gate.
		before = e.rpc.dispatchCount()
		blockedRes, err = e.send(f, SendReplay, nil)
		assertBlocked(t, e, f.attemptID, blockedRes, err, ClassPausePresent, before)
		var released int
		if err := e.pool.QueryRow(ctx,
			`SELECT count(*) FROM tx_attempt_events WHERE attempt_id = $1 AND event = 'released'`, f.attemptID).Scan(&released); err != nil {
			t.Fatal(err)
		}
		if released == 0 {
			t.Fatal("release audit event missing")
		}
	})

	t.Run("indeterminate_ordering_does_not_freeze", func(t *testing.T) {
		e := newEnv(t)
		ctx := context.Background()
		f := e.seed()
		f.sign()
		if res, err := e.send(f, SendInitial, nil); err != nil || res.Outcome != "accepted" {
			t.Fatalf("initial send = %+v %v", res, err)
		}
		// A pause committed after the dispatch is not violation evidence.
		e.exec(`INSERT INTO indexer_pause (chain_id, height, expected_hash, actual_hash, kind, created_at)
			VALUES ($1,100,$2,$3,'hash_mismatch', now() + interval '1 hour')`,
			e.chainID, blockHashHex(100), blockHashHex(101))
		if _, err := e.store.Reconcile(ctx, f.attemptID, ""); err != nil {
			t.Fatal(err)
		}
		var freezes int
		if err := e.pool.QueryRow(ctx, `SELECT count(*) FROM tx_intent_freezes WHERE intent_id = $1`, f.intentID).Scan(&freezes); err != nil {
			t.Fatal(err)
		}
		if freezes != 0 {
			t.Fatalf("indeterminate ordering froze the intent: %d", freezes)
		}
	})

	t.Run("last_moment_expiry_refuses_clean", func(t *testing.T) {
		e := newEnv(t)
		f := e.seed()
		f.sign()
		e.exec(`UPDATE withdrawal_authorizations SET expires_at = now() - interval '1 second' WHERE authorization_id = $1`, f.authID)
		before := e.rpc.dispatchCount()
		res, err := e.send(f, SendInitial, nil)
		assertBlocked(t, e, f.attemptID, res, err, ClassAuthorizationExpired, before)
	})
}
