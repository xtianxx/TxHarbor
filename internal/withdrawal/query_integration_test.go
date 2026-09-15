//go:build integration

// query_integration_test.go owns the T011 real-PostgreSQL verification: the
// ownership-enforced self-read round-trip, the byte-identical 404 for a
// foreign vs a nonexistent id, and the 006 recovery-state projection
// (none/recovering/paused_reconcile/released) against a real PostgreSQL 18
// container (testcontainers).
//
// Requests are seeded with raw SQL on purpose: T010 owns the intake
// round-trip (T012), so T011 must not depend on SubmitWithdrawal landing in
// parallel. The unknown-state branch (either recovery read failing) is
// asserted at unit level only — failure injection is T028's surface.
//
// It reuses the T006 container/migration helpers (withdrawalStartPostgres,
// withdrawalMigrateOptions, withdrawalHash, withdrawalAddr,
// withdrawalMaxUint256) and adds query-prefixed helpers so no name collides.
package withdrawal

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/db"
)

// querySetup boots one migrated scratch PostgreSQL database and returns a pool
// over it (the T006 container harness).
func querySetup(t *testing.T) (context.Context, *pgxpool.Pool) {
	t.Helper()
	dsn := withdrawalStartPostgres(t)
	ctx := context.Background()
	if err := db.MigrateUp(ctx, withdrawalMigrateOptions(dsn), io.Discard); err != nil {
		t.Fatalf("migrate up: %v", err)
	}
	pool, err := db.OpenPool(ctx, dsn, 5*time.Second)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return ctx, pool
}

// querySeedCaller inserts the stable caller identity a request belongs to.
func querySeedCaller(t *testing.T, ctx context.Context, pool *pgxpool.Pool, callerID int64) {
	t.Helper()
	if _, err := pool.Exec(ctx, `INSERT INTO caller (caller_id, label) VALUES ($1, 'query-test')`, callerID); err != nil {
		t.Fatalf("seed caller %d: %v", callerID, err)
	}
}

// querySeedRequest inserts one Accepted request via raw SQL, with a distinct
// idempotency key and authorization id derived from the request id.
func querySeedRequest(t *testing.T, ctx context.Context, pool *pgxpool.Pool, requestID string, callerID, chainID int64, amount string) {
	t.Helper()
	_, err := pool.Exec(ctx, `INSERT INTO withdrawal_requests
		(request_id, caller_id, idempotency_key, authorization_id, chain_id, asset, recipient, amount)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8::numeric)`,
		requestID, callerID, "key-"+requestID, "auth-"+requestID, chainID,
		withdrawalAddr("11"), withdrawalAddr("22"), amount)
	if err != nil {
		t.Fatalf("seed request %s: %v", requestID, err)
	}
}

// querySeedPolicy opens the reorg policy version the recovery row's FK needs.
func querySeedPolicy(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64) {
	t.Helper()
	if _, err := pool.Exec(ctx, `INSERT INTO reorg_policy_history
		(chain_id, policy_seq, max_depth, operator) VALUES ($1, 1, 64, 'bootstrap')`, chainID); err != nil {
		t.Fatalf("seed reorg policy %d: %v", chainID, err)
	}
}

// querySeedRecoveryRow inserts one active reorg_recovery row at the given phase.
func querySeedRecoveryRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, recoveryID, phase string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `INSERT INTO reorg_recovery
		(chain_id, recovery_id, phase, policy_seq, max_depth, bound_old_number, bound_old_hash, recovery_seq)
		VALUES ($1, $2, $3, 1, 64, 100, $4, 1)`, chainID, recoveryID, phase, withdrawalHash("aa")); err != nil {
		t.Fatalf("seed recovery row for chain %d: %v", chainID, err)
	}
}

// querySeedTerminalEvent inserts one terminal recovery event with no active
// row (the released shape).
func querySeedTerminalEvent(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, recoveryID string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `INSERT INTO reorg_recovery_events
		(chain_id, recovery_id, recovery_seq, event_seq, event) VALUES ($1, $2, 1, 1, 'released')`, chainID, recoveryID); err != nil {
		t.Fatalf("seed terminal event for chain %d: %v", chainID, err)
	}
}

// TestWithdrawalQuerySelfReadRoundTrip proves a caller reads back its own
// request with every field intact: exact uint256 amount-string round-trip,
// canonical addresses, accepted status, a non-zero created_at, and the
// standing recovery signal (no 006 rows → none / not_started).
func TestWithdrawalQuerySelfReadRoundTrip(t *testing.T) {
	ctx, pool := querySetup(t)
	const (
		callerID  = int64(7101)
		chainID   = int64(1)
		requestID = "wr-query-self"
	)
	querySeedCaller(t, ctx, pool, callerID)
	querySeedRequest(t, ctx, pool, requestID, callerID, chainID, withdrawalMaxUint256)

	view, err := GetWithdrawal(ctx, pool, callerID, requestID)
	if err != nil {
		t.Fatalf("GetWithdrawal() error = %v", err)
	}
	if view.RequestID != requestID || view.CallerID != callerID || view.ChainID != chainID {
		t.Errorf("identity = %s/%d/%d, want %s/%d/%d",
			view.RequestID, view.CallerID, view.ChainID, requestID, callerID, chainID)
	}
	if view.Asset != withdrawalAddr("11") || view.Recipient != withdrawalAddr("22") {
		t.Errorf("asset/recipient = %s/%s, want %s/%s",
			view.Asset, view.Recipient, withdrawalAddr("11"), withdrawalAddr("22"))
	}
	if view.Amount != withdrawalMaxUint256 {
		t.Errorf("amount = %q, want exact uint256 round-trip %q", view.Amount, withdrawalMaxUint256)
	}
	if view.Status != "accepted" {
		t.Errorf("status = %q, want accepted", view.Status)
	}
	if view.CreatedAt.IsZero() {
		t.Error("created_at is zero, want a real timestamp")
	}
	if view.Recovery.State != "none" || view.Recovery.Execution != "not_started" {
		t.Errorf("recovery = %+v, want {none not_started}", view.Recovery)
	}
}

// TestWithdrawalQueryForeignAndRandomIdenticalNotFound proves ownership
// enforcement: another caller's existing request and a nonexistent id return
// the byte-identical *Error{Code: CodeNotFound}. A final owner read proves the
// foreign probe hit a real row, not an absent one.
func TestWithdrawalQueryForeignAndRandomIdenticalNotFound(t *testing.T) {
	ctx, pool := querySetup(t)
	const (
		ownerID   = int64(7201)
		foreignID = int64(7202)
		requestID = "wr-query-owned"
	)
	querySeedCaller(t, ctx, pool, ownerID)
	querySeedRequest(t, ctx, pool, requestID, ownerID, 1, "1000")

	_, foreignErr := GetWithdrawal(ctx, pool, foreignID, requestID)
	_, randomErr := GetWithdrawal(ctx, pool, foreignID, "wr-query-absent")
	if foreignErr == nil || randomErr == nil {
		t.Fatalf("expected not-found errors, got foreign=%v random=%v", foreignErr, randomErr)
	}
	var foreign, random *Error
	if !errors.As(foreignErr, &foreign) || !errors.As(randomErr, &random) {
		t.Fatalf("errors = %T/%T, want *Error", foreignErr, randomErr)
	}
	if foreign.Code != CodeNotFound || random.Code != CodeNotFound {
		t.Fatalf("codes = %q/%q, want %q", foreign.Code, random.Code, CodeNotFound)
	}
	if foreignErr.Error() != randomErr.Error() {
		t.Fatalf("foreign error %q differs from random error %q (existence leakage)",
			foreignErr.Error(), randomErr.Error())
	}

	// The owner can still read the row the foreign probe was denied — so the
	// identical 404 came from ownership, not from a missing row.
	if _, err := GetWithdrawal(ctx, pool, ownerID, requestID); err != nil {
		t.Fatalf("owner read after 404 probes: %v (row must exist)", err)
	}
}

// TestWithdrawalQueryRecoveryStates projects the 006 recovery state onto 007
// requests across four chains in one database: no rows → none; an active row
// in a replaying phase → recovering; reconcile_required → paused_reconcile;
// a terminal event with no active row → released. Execution stays not_started
// throughout.
func TestWithdrawalQueryRecoveryStates(t *testing.T) {
	ctx, pool := querySetup(t)
	const callerID = int64(7301)
	querySeedCaller(t, ctx, pool, callerID)

	requests := map[string]int64{
		"wr-query-none":       7311,
		"wr-query-recovering": 7312,
		"wr-query-paused":     7313,
		"wr-query-released":   7314,
	}
	for requestID, chainID := range requests {
		querySeedRequest(t, ctx, pool, requestID, callerID, chainID, "1")
	}

	querySeedPolicy(t, ctx, pool, 7312)
	querySeedRecoveryRow(t, ctx, pool, 7312, "rec-query-recovering", "detected")
	querySeedPolicy(t, ctx, pool, 7313)
	querySeedRecoveryRow(t, ctx, pool, 7313, "rec-query-paused", "reconcile_required")
	querySeedTerminalEvent(t, ctx, pool, 7314, "rec-query-released")

	want := map[string]string{
		"wr-query-none":       "none",
		"wr-query-recovering": "recovering",
		"wr-query-paused":     "paused_reconcile",
		"wr-query-released":   "released",
	}
	for requestID, wantState := range want {
		view, err := GetWithdrawal(ctx, pool, callerID, requestID)
		if err != nil {
			t.Fatalf("GetWithdrawal(%s) error = %v", requestID, err)
		}
		if view.Recovery.State != wantState {
			t.Errorf("%s recovery.state = %q, want %q", requestID, view.Recovery.State, wantState)
		}
		if view.Recovery.Execution != "not_started" {
			t.Errorf("%s recovery.execution = %q, want not_started", requestID, view.Recovery.Execution)
		}
	}
}
