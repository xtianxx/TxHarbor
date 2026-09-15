//go:build integration

// retention_integration_test.go owns review gap #7(b) for 007-withdrawal-
// creation: a real-PostgreSQL probe that the three FR-11 permanence carriers —
// the Accepted `withdrawal_requests` row, its `withdrawal_request_audit`
// receipt log, and the `withdrawal_authorizations` grant binding — survive
// further activity and a process restart (a fresh pool over the same DSN), and
// that the original key still replays to the SAME request_id.
//
// HONEST BOUNDARY — finite tests cannot prove infinite time. Every assertion
// below observes one bounded window: a row exists, is byte-identical before and
// after THIS test's activity, and is still addressable through a new pool. No
// test can run forever, so this file does not and cannot prove the rows persist
// "for all time". The permanence claim therefore rests on two independently
// checkable legs:
//
//	(i)  No runtime path deletes these rows. A repository code search over
//	     *.go and *.sql for
//	       delete\s+from\s+(withdrawal_requests|withdrawal_request_audit|
//	         withdrawal_authorizations)
//	       truncate\s+(table\s+)?(withdrawal_requests|withdrawal_request_audit|
//	         withdrawal_authorizations)
//	     and for TTL-ish vocabulary (ttl|retention|purge|prune|cleanup)
//	     co-occurring with those tables returns ZERO matches. The only
//	     destructive statement that names them is 000007's own +goose Down
//	     `DROP TABLE IF EXISTS ...`, which is schema-rollback authoring hygiene
//	     and is never executed by the application. With no delete path,
//	     elapsing time alone cannot remove a row.
//	(ii) Replay-after-activity (asserted below): the original key replays to
//	     the SAME request_id after new requests have been accepted and after
//	     the process reconnects through a fresh pool — the row is not merely
//	     present but still functionally addressable.
package withdrawal

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/db"
)

// retentionSetup boots one migrated scratch PostgreSQL (the T006 container
// harness) and returns the DSN alongside the pool, so the restart leg can open
// a SECOND pool over the same database.
func retentionSetup(t *testing.T) (context.Context, string, *pgxpool.Pool) {
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
	return ctx, dsn, pool
}

// retentionAuditRowsByRequest reads the receipt-log rows for requestID in
// audit_id order — the direct read, never inferred from a response. A missing
// audit row shows up as length 0.
func retentionAuditRowsByRequest(t *testing.T, ctx context.Context, pool *pgxpool.Pool, requestID string) []string {
	t.Helper()
	rows, err := pool.Query(ctx,
		`SELECT action FROM withdrawal_request_audit WHERE request_id = $1 ORDER BY audit_id`, requestID)
	if err != nil {
		t.Fatalf("query audit rows for %q: %v", requestID, err)
	}
	defer rows.Close()
	var actions []string
	for rows.Next() {
		var a string
		if err := rows.Scan(&a); err != nil {
			t.Fatalf("scan audit row for %q: %v", requestID, err)
		}
		actions = append(actions, a)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate audit rows for %q: %v", requestID, err)
	}
	return actions
}

// retentionBindingOf reads the grant row for authorizationID joined to the
// request it is bound to (the authorization_id UNIQUE carrier): state, amount,
// and bound request_id in one read, with COALESCE(”) for an unbound grant.
func retentionBindingOf(t *testing.T, ctx context.Context, pool *pgxpool.Pool, authorizationID string) (boundRequestID, state, amount string) {
	t.Helper()
	if err := pool.QueryRow(ctx, `
SELECT a.state, a.amount::text, COALESCE(r.request_id, '')
FROM withdrawal_authorizations a
LEFT JOIN withdrawal_requests r ON r.authorization_id = a.authorization_id
WHERE a.authorization_id = $1`, authorizationID).Scan(&state, &amount, &boundRequestID); err != nil {
		t.Fatalf("read grant binding for %q: %v", authorizationID, err)
	}
	return boundRequestID, state, amount
}

// retentionGrantCountAll counts every grant row.
func retentionGrantCountAll(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM withdrawal_authorizations`).Scan(&n); err != nil {
		t.Fatalf("count withdrawal_authorizations: %v", err)
	}
	return n
}

// TestWithdrawalRetentionPermanenceAcrossActivity proves the three FR-11
// carriers survive further activity and a restart, the original key still
// replays to the same id, and the row counts grow ONLY by the new activity.
func TestWithdrawalRetentionPermanenceAcrossActivity(t *testing.T) {
	ctx, dsn, pool := retentionSetup(t)

	const (
		callerA = int64(7501)
		callerB = int64(7502)
		keyA1   = "retention-key-a1"
		keyA2   = "retention-key-a2"
		keyB1   = "retention-key-b1"
		authA1  = "auth-retention-a1"
		authA2  = "auth-retention-a2"
		authB1  = "auth-retention-b1"
	)

	// Given one accepted request for caller A with its credential and grant.
	presentedA := idempotencySeed(t, ctx, pool, callerA, authA1)
	first, err := SubmitWithdrawal(ctx, pool, intakeReq(presentedA, keyA1, authA1))
	if err != nil {
		t.Fatalf("first SubmitWithdrawal: %v", err)
	}
	if first.Status != 201 {
		t.Fatalf("first status = %d (%+v), want 201", first.Status, first)
	}
	intakeWantNoAudit(t, first)
	firstID := first.RequestID

	// Capture the three carriers at rest: byte-identical row, bound grant, and
	// the receipt audit — plus the baseline counts.
	requestBefore := idempotencyRowSnapshotOf(t, ctx, pool, firstID)
	requestsBefore := intakeRequestCount(t, ctx, pool)
	auditsBefore := intakeAuditCount(t, ctx, pool)
	grantsBefore := retentionGrantCountAll(t, ctx, pool)
	if requestsBefore != 1 || auditsBefore != 1 || grantsBefore != 1 {
		t.Fatalf("baseline counts = requests %d / audits %d / grants %d, want 1/1/1",
			requestsBefore, auditsBefore, grantsBefore)
	}
	if bound, state, amount := retentionBindingOf(t, ctx, pool, authA1); bound != firstID || state != "active" || amount != intakeAmount {
		t.Fatalf("baseline binding = (%q, %q, %q), want (%q, active, %q)", bound, state, amount, firstID, intakeAmount)
	}
	if got := retentionAuditRowsByRequest(t, ctx, pool, firstID); len(got) != 1 || got[0] != auditActionCreated {
		t.Fatalf("baseline audit rows for %q = %v, want [created]", firstID, got)
	}

	// When further activity lands: a NEW key for caller A against a new grant,
	// then a new caller B with its own key and grant — both accepted.
	intakeSupplyGrant(t, ctx, pool, callerA, authA2, intakeAmount)
	second, err := SubmitWithdrawal(ctx, pool, intakeReq(presentedA, keyA2, authA2))
	if err != nil || second.Status != 201 {
		t.Fatalf("second (new key, same caller) = (%+v, %v), want 201", second, err)
	}
	presentedB := idempotencySeed(t, ctx, pool, callerB, authB1)
	third, err := SubmitWithdrawal(ctx, pool, intakeReq(presentedB, keyB1, authB1))
	if err != nil || third.Status != 201 {
		t.Fatalf("third (new caller, new key) = (%+v, %v), want 201", third, err)
	}

	// Then the accepted request is byte-identical, still queryable, its receipt
	// audit survives, and its grant binding still resolves to it.
	if got := idempotencyRowSnapshotOf(t, ctx, pool, firstID); got != requestBefore {
		t.Fatalf("accepted request changed across activity:\n before %+v\n after  %+v", requestBefore, got)
	}
	view, err := GetWithdrawal(ctx, pool, callerA, firstID)
	if err != nil {
		t.Fatalf("GetWithdrawal after activity: %v", err)
	}
	if view.RequestID != firstID || view.Status != "accepted" || view.Amount != intakeAmount {
		t.Fatalf("self-read after activity = %+v, want %q/accepted/%q", view, firstID, intakeAmount)
	}
	if got := retentionAuditRowsByRequest(t, ctx, pool, firstID); len(got) != 1 || got[0] != auditActionCreated {
		t.Fatalf("audit rows for %q after activity = %v, want [created]", firstID, got)
	}
	if bound, state, amount := retentionBindingOf(t, ctx, pool, authA1); bound != firstID || state != "active" || amount != intakeAmount {
		t.Fatalf("binding after activity = (%q, %q, %q), want (%q, active, %q)", bound, state, amount, firstID, intakeAmount)
	}

	// And the counts grew ONLY by the new activity: one request, one receipt
	// audit, and one grant per accepted new key — never a sweep.
	if n := intakeRequestCount(t, ctx, pool); n != requestsBefore+2 {
		t.Fatalf("requests after activity = %d, want %d (only the 2 new)", n, requestsBefore+2)
	}
	if n := intakeAuditCount(t, ctx, pool); n != auditsBefore+2 {
		t.Fatalf("audits after activity = %d, want %d (only the 2 new)", n, auditsBefore+2)
	}
	if n := retentionGrantCountAll(t, ctx, pool); n != grantsBefore+2 {
		t.Fatalf("grants after activity = %d, want %d (only the 2 new)", n, grantsBefore+2)
	}

	// When the process reconnects through a FRESH pool over the same DSN (a
	// restart) and the ORIGINAL key is replayed ...
	restartPool, err := db.OpenPool(ctx, dsn, 5*time.Second)
	if err != nil {
		t.Fatalf("open restart pool: %v", err)
	}
	t.Cleanup(restartPool.Close)
	replay, err := SubmitWithdrawal(ctx, restartPool, intakeReq(presentedA, keyA1, authA1))
	if err != nil {
		t.Fatalf("replay on restart pool: %v", err)
	}

	// Then it is the 200 replay of the SAME id, the row is still readable and
	// unchanged through the restart pool, and the binding still resolves.
	if replay.Status != 200 || replay.RequestID != firstID {
		t.Fatalf("restart replay = %+v, want 200 with request_id %q", replay, firstID)
	}
	intakeWantIntent(t, replay, auditActionReplayed, callerA, firstID)
	if _, err := GetWithdrawal(ctx, restartPool, callerA, firstID); err != nil {
		t.Fatalf("restart self-read: %v", err)
	}
	if bound, state, amount := retentionBindingOf(t, ctx, restartPool, authA1); bound != firstID || state != "active" || amount != intakeAmount {
		t.Fatalf("restart binding = (%q, %q, %q), want (%q, active, %q)", bound, state, amount, firstID, intakeAmount)
	}
	if got := idempotencyRowSnapshotOf(t, ctx, restartPool, firstID); got != requestBefore {
		t.Fatalf("request changed across restart replay:\n before %+v\n after  %+v", requestBefore, got)
	}

	// And the replay wrote NO core row: counts are exactly those left by the
	// new activity (a replay reads the existing row).
	if n := intakeRequestCount(t, ctx, restartPool); n != requestsBefore+2 {
		t.Fatalf("requests after restart replay = %d, want %d", n, requestsBefore+2)
	}
	if n := intakeAuditCount(t, ctx, restartPool); n != auditsBefore+2 {
		t.Fatalf("audits after restart replay = %d, want %d (replay writes no row)", n, auditsBefore+2)
	}
}
