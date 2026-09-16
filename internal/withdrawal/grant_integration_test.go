//go:build integration

// grant_integration_test.go owns the T008 grant-supply integration tests: the
// Table 6 attempt semantics and the supply pseudocode 23505/commit-unknown
// branches against a real PostgreSQL 18 container (testcontainers).
//
// It reuses the T006 container/migration helpers (withdrawalStartPostgres,
// withdrawalMigrateOptions) and adds grant-prefixed helpers so no name
// collides with migration_integration_test.go. Each test boots its own
// isolated scratch database.
package withdrawal

import (
	"context"
	"errors"
	"io"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/db"
)

// grantSetup boots a migrated scratch PostgreSQL and returns a pool for it.
func grantSetup(t *testing.T) (context.Context, *pgxpool.Pool) {
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

func grantSeedCaller(t *testing.T, ctx context.Context, pool *pgxpool.Pool, callerID int64) {
	t.Helper()
	if _, err := pool.Exec(ctx, `INSERT INTO caller (caller_id, label) VALUES ($1, 'grant-test')`, callerID); err != nil {
		t.Fatalf("seed caller %d: %v", callerID, err)
	}
}

func grantWantCode(t *testing.T, err error, code Code) *Error {
	t.Helper()
	var e *Error
	if !errors.As(err, &e) {
		t.Fatalf("error = %v (%T), want *Error code %q", err, err, code)
	}
	if e.Code != code {
		t.Fatalf("error code = %q, want %q (%v)", e.Code, code, e)
	}
	return e
}

func grantAuditActions(t *testing.T, ctx context.Context, pool *pgxpool.Pool, authorizationID string) []string {
	t.Helper()
	rows, err := pool.Query(ctx,
		`SELECT action FROM withdrawal_grant_audit WHERE authorization_id = $1 ORDER BY audit_id`, authorizationID)
	if err != nil {
		t.Fatalf("query grant audit: %v", err)
	}
	defer rows.Close()
	var actions []string
	for rows.Next() {
		var a string
		if err := rows.Scan(&a); err != nil {
			t.Fatalf("scan grant audit action: %v", err)
		}
		actions = append(actions, a)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate grant audit: %v", err)
	}
	return actions
}

func grantAuditCountByOp(t *testing.T, ctx context.Context, pool *pgxpool.Pool, operationID string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM withdrawal_grant_audit WHERE operation_id = $1`, operationID).Scan(&n); err != nil {
		t.Fatalf("count audit rows for operation %q: %v", operationID, err)
	}
	return n
}

func grantCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, authorizationID string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM withdrawal_authorizations WHERE authorization_id = $1`, authorizationID).Scan(&n); err != nil {
		t.Fatalf("count grant rows for %q: %v", authorizationID, err)
	}
	return n
}

func grantStateAndAmount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, authorizationID string) (string, string) {
	t.Helper()
	var state, amount string
	if err := pool.QueryRow(ctx,
		`SELECT state, amount::text FROM withdrawal_authorizations WHERE authorization_id = $1`, authorizationID).
		Scan(&state, &amount); err != nil {
		t.Fatalf("read grant row for %q: %v", authorizationID, err)
	}
	return state, amount
}

// grantTestOp builds a supply op-input with the given attempt key.
func grantTestOp(operationID, authorizationID string, callerID int64, amount string) OpInput {
	return OpInput{
		OperationID:     operationID,
		Action:          grantSupplyAction,
		AuthorizationID: authorizationID,
		CallerID:        callerID,
		ChainID:         31337,
		Asset:           "0x1111111111111111111111111111111111111111",
		Recipient:       "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Amount:          amount,
	}
}

// TestWithdrawalGrantSupplyFirstAndEqualResupply covers the first-supply row
// shape and an equal re-supply with a NEW operation id: 'resupplied' with the
// grant row left untouched.
func TestWithdrawalGrantSupplyFirstAndEqualResupply(t *testing.T) {
	ctx, pool := grantSetup(t)
	grantSeedCaller(t, ctx, pool, 7001)

	const (
		authID = "auth-first"
		opID1  = "11111111111111111111111111111111"
		opID2  = "22222222222222222222222222222222"
	)
	op := grantTestOp(opID1, authID, 7001, "100")

	out, err := SupplyGrant(ctx, pool, op, "operator-a", "initial supply")
	if err != nil {
		t.Fatalf("first SupplyGrant: %v", err)
	}
	if out.Action != grantOutcomeSupplied || out.AuthorizationID != authID {
		t.Fatalf("first outcome = %+v, want {%s %s}", out, grantOutcomeSupplied, authID)
	}
	if n := grantCount(t, ctx, pool, authID); n != 1 {
		t.Fatalf("grant rows after first supply = %d, want 1", n)
	}
	acted := grantAuditActions(t, ctx, pool, authID)
	if len(acted) != 1 || acted[0] != grantOutcomeSupplied {
		t.Fatalf("audit actions after first supply = %v, want [supplied]", acted)
	}
	if got := grantAuditCountByOp(t, ctx, pool, opID1); got != 1 {
		t.Fatalf("audit rows for op1 = %d, want 1", got)
	}

	// Equal re-supply with a new operation id: recorded as an attempt, grant untouched.
	op2 := op
	op2.OperationID = opID2
	out2, err := SupplyGrant(ctx, pool, op2, "operator-b", "repeat supply")
	if err != nil {
		t.Fatalf("equal re-supply: %v", err)
	}
	if out2.Action != grantOutcomeResupplied {
		t.Fatalf("re-supply outcome = %+v, want %s", out2, grantOutcomeResupplied)
	}
	if n := grantCount(t, ctx, pool, authID); n != 1 {
		t.Fatalf("grant rows after re-supply = %d, want 1", n)
	}
	acted = grantAuditActions(t, ctx, pool, authID)
	if len(acted) != 2 || acted[0] != grantOutcomeSupplied || acted[1] != grantOutcomeResupplied {
		t.Fatalf("audit actions after re-supply = %v, want [supplied resupplied]", acted)
	}
}

// TestWithdrawalGrantSupplyRefusedDistinctAttempts covers A-then-B/C异参:
// each differing attempt records its own 'supply_refused' row with distinct
// detail while the grant keeps the winner's params.
func TestWithdrawalGrantSupplyRefusedDistinctAttempts(t *testing.T) {
	ctx, pool := grantSetup(t)
	grantSeedCaller(t, ctx, pool, 7002)

	const authID = "auth-refused"
	winner := grantTestOp("33333333333333333333333333333333", authID, 7002, "100")
	if out, err := SupplyGrant(ctx, pool, winner, "op", "winner"); err != nil || out.Action != grantOutcomeSupplied {
		t.Fatalf("winner supply = (%+v, %v), want %s", out, err, grantOutcomeSupplied)
	}

	for i, tc := range []struct{ opID, amount string }{
		{"44444444444444444444444444444444", "101"},
		{"44444444444444444444444444444445", "102"},
	} {
		op := grantTestOp(tc.opID, authID, 7002, tc.amount)
		out, err := SupplyGrant(ctx, pool, op, "op", "refused")
		if err != nil {
			t.Fatalf("refused supply %d: %v", i, err)
		}
		if out.Action != grantOutcomeSupplyRefused {
			t.Fatalf("refused supply %d outcome = %+v, want %s", i, out, grantOutcomeSupplyRefused)
		}
	}

	acted := grantAuditActions(t, ctx, pool, authID)
	want := []string{grantOutcomeSupplied, grantOutcomeSupplyRefused, grantOutcomeSupplyRefused}
	if len(acted) != len(want) {
		t.Fatalf("audit actions = %v, want %v", acted, want)
	}
	for i := range want {
		if acted[i] != want[i] {
			t.Fatalf("audit actions = %v, want %v", acted, want)
		}
	}
	if state, amount := grantStateAndAmount(t, ctx, pool, authID); amount != "100" || state != "active" {
		t.Fatalf("grant after refusals = (%s, %s), want (active, 100)", state, amount)
	}
}

// TestWithdrawalGrantSameOperationConcurrent covers same-O + same-OPIN twins:
// exactly one grant row, exactly one audit row, and every caller reports the
// recorded outcome.
func TestWithdrawalGrantSameOperationConcurrent(t *testing.T) {
	ctx, pool := grantSetup(t)
	grantSeedCaller(t, ctx, pool, 7003)

	const authID = "auth-same-op"
	op := grantTestOp("55555555555555555555555555555555", authID, 7003, "100")

	start := make(chan struct{})
	results := make(chan *GrantOutcome, 2)
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			out, err := SupplyGrant(ctx, pool, op, "op", "twin")
			results <- out
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent same-O supply error: %v", err)
		}
	}
	for out := range results {
		if out.Action != grantOutcomeSupplied {
			t.Fatalf("concurrent same-O outcome = %+v, want %s", out, grantOutcomeSupplied)
		}
	}
	if n := grantCount(t, ctx, pool, authID); n != 1 {
		t.Fatalf("grant rows = %d, want 1", n)
	}
	acted := grantAuditActions(t, ctx, pool, authID)
	if len(acted) != 1 || acted[0] != grantOutcomeSupplied {
		t.Fatalf("audit actions = %v, want exactly [supplied]", acted)
	}
}

// TestWithdrawalGrantSameOperationDifferentInputConflict covers same-O +
// different-OPIN: operation_conflict, zero writes, and the original outcome
// (including a refusal) left intact.
func TestWithdrawalGrantSameOperationDifferentInputConflict(t *testing.T) {
	ctx, pool := grantSetup(t)
	grantSeedCaller(t, ctx, pool, 7004)

	const authID = "auth-conflict"
	op := grantTestOp("66666666666666666666666666666666", authID, 7004, "100")
	if _, err := SupplyGrant(ctx, pool, op, "op", "original"); err != nil {
		t.Fatalf("original supply: %v", err)
	}

	// Same O, different amount → conflict with zero new writes.
	differ := op
	differ.Amount = "999"
	out, err := SupplyGrant(ctx, pool, differ, "op", "differ")
	if out != nil {
		t.Fatalf("conflicting outcome = %+v, want nil", out)
	}
	grantWantCode(t, err, CodeOperationConflict)
	if got := grantAuditCountByOp(t, ctx, pool, op.OperationID); got != 1 {
		t.Fatalf("audit rows for original op = %d, want 1 (zero writes)", got)
	}
	if _, amount := grantStateAndAmount(t, ctx, pool, authID); amount != "100" {
		t.Fatalf("grant amount after conflict = %s, want 100 (original intact)", amount)
	}

	// Refusal stays refusal: record a refusal, then retry its O with different
	// input → conflict, and the stored outcome is still 'supply_refused'.
	refusal := grantTestOp("77777777777777777777777777777777", authID, 7004, "200")
	if out, err := SupplyGrant(ctx, pool, refusal, "op", "refuse"); err != nil || out.Action != grantOutcomeSupplyRefused {
		t.Fatalf("refusal attempt = (%+v, %v), want %s", out, err, grantOutcomeSupplyRefused)
	}
	refusalDiffer := refusal
	refusalDiffer.Amount = "201"
	if _, err := SupplyGrant(ctx, pool, refusalDiffer, "op", "differ"); err == nil {
		t.Fatal("expected operation_conflict on refused-op retry with different input")
	} else {
		grantWantCode(t, err, CodeOperationConflict)
	}
	readBack, found, err := ReadAttempt(ctx, pool, refusal.OperationID)
	if err != nil || !found {
		t.Fatalf("ReadAttempt(refusal) = (%v, %v, %v), want found", readBack, found, err)
	}
	if readBack.Action != grantOutcomeSupplyRefused {
		t.Fatalf("stored refusal outcome = %q, want %s (never upgraded)", readBack.Action, grantOutcomeSupplyRefused)
	}
	if got := grantAuditCountByOp(t, ctx, pool, refusal.OperationID); got != 1 {
		t.Fatalf("audit rows for refusal op = %d, want 1", got)
	}
}

// TestWithdrawalGrantRevokeThenRepeat covers revoke → 'revoked' and a repeat
// revoke → a distinct 'revoke_nop' row.
func TestWithdrawalGrantRevokeThenRepeat(t *testing.T) {
	ctx, pool := grantSetup(t)
	grantSeedCaller(t, ctx, pool, 7005)

	const authID = "auth-revoke"
	if _, err := SupplyGrant(ctx, pool, grantTestOp("88888888888888888888888888888888", authID, 7005, "100"), "op", "supply"); err != nil {
		t.Fatalf("supply before revoke: %v", err)
	}

	out, err := RevokeGrant(ctx, pool, "99999999999999999999999999999999", authID, "op", "revoke")
	if err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if out.Action != grantOutcomeRevoked {
		t.Fatalf("revoke outcome = %+v, want %s", out, grantOutcomeRevoked)
	}
	if state, _ := grantStateAndAmount(t, ctx, pool, authID); state != "revoked" {
		t.Fatalf("grant state = %q, want revoked", state)
	}

	out2, err := RevokeGrant(ctx, pool, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", authID, "op", "repeat revoke")
	if err != nil {
		t.Fatalf("repeat revoke: %v", err)
	}
	if out2.Action != grantOutcomeRevokeNop {
		t.Fatalf("repeat revoke outcome = %+v, want %s", out2, grantOutcomeRevokeNop)
	}

	acted := grantAuditActions(t, ctx, pool, authID)
	want := []string{grantOutcomeSupplied, grantOutcomeRevoked, grantOutcomeRevokeNop}
	if len(acted) != len(want) {
		t.Fatalf("audit actions = %v, want %v", acted, want)
	}
	for i := range want {
		if acted[i] != want[i] {
			t.Fatalf("audit actions = %v, want %v", acted, want)
		}
	}
}

// TestWithdrawalGrantConcurrentFirstSupplyTwins covers the two first-supply
// race shapes: same-O same-OPIN → one grant + one 'supplied'; different-O
// different-OPIN → one grant (winner) + 'supplied' + 'supply_refused'.
func TestWithdrawalGrantConcurrentFirstSupplyTwins(t *testing.T) {
	t.Run("same-O same-OPIN", func(t *testing.T) {
		ctx, pool := grantSetup(t)
		grantSeedCaller(t, ctx, pool, 7006)
		const authID = "auth-twins-same"
		op := grantTestOp("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", authID, 7006, "100")

		runTwin(t, ctx, pool, op, op)
		if n := grantCount(t, ctx, pool, authID); n != 1 {
			t.Fatalf("grant rows = %d, want 1", n)
		}
		acted := grantAuditActions(t, ctx, pool, authID)
		if len(acted) != 1 || acted[0] != grantOutcomeSupplied {
			t.Fatalf("audit actions = %v, want exactly [supplied]", acted)
		}
	})

	t.Run("different-O different-OPIN", func(t *testing.T) {
		ctx, pool := grantSetup(t)
		grantSeedCaller(t, ctx, pool, 7007)
		const authID = "auth-twins-diff"
		winA := grantTestOp("cccccccccccccccccccccccccccccccc", authID, 7007, "100")
		winB := grantTestOp("dddddddddddddddddddddddddddddddd", authID, 7007, "200")

		runTwin(t, ctx, pool, winA, winB)
		if n := grantCount(t, ctx, pool, authID); n != 1 {
			t.Fatalf("grant rows = %d, want 1", n)
		}
		acted := grantAuditActions(t, ctx, pool, authID)
		if len(acted) != 2 || !hasAction(acted, grantOutcomeSupplied) || !hasAction(acted, grantOutcomeSupplyRefused) {
			t.Fatalf("audit actions = %v, want [supplied supply_refused] in some order", acted)
		}
		if _, amount := grantStateAndAmount(t, ctx, pool, authID); amount != "100" && amount != "200" {
			t.Fatalf("grant amount = %s, want the winner's 100 or 200", amount)
		}
	})
}

// runTwin fires two concurrent supply attempts and fails the test on any error.
func runTwin(t *testing.T, ctx context.Context, pool *pgxpool.Pool, a, b OpInput) {
	t.Helper()
	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, op := range []OpInput{a, b} {
		wg.Add(1)
		go func(op OpInput) {
			defer wg.Done()
			<-start
			_, err := SupplyGrant(ctx, pool, op, "op", "twin")
			errs <- err
		}(op)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent supply error: %v", err)
		}
	}
}

// grantAuditCallerByOp reads the single audit row recorded for operationID and
// returns its caller_id, failing unless exactly one row exists. It proves the
// stored caller attribution directly, never inferring it from a return code.
func grantAuditCallerByOp(t *testing.T, ctx context.Context, pool *pgxpool.Pool, operationID string) int64 {
	t.Helper()
	rows, err := pool.Query(ctx,
		`SELECT caller_id FROM withdrawal_grant_audit WHERE operation_id = $1`, operationID)
	if err != nil {
		t.Fatalf("query audit caller for operation %q: %v", operationID, err)
	}
	defer rows.Close()
	var (
		callerID int64
		n        int
	)
	for rows.Next() {
		if err := rows.Scan(&callerID); err != nil {
			t.Fatalf("scan audit caller for operation %q: %v", operationID, err)
		}
		n++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate audit caller for operation %q: %v", operationID, err)
	}
	if n != 1 {
		t.Fatalf("audit rows for operation %q = %d, want exactly 1", operationID, n)
	}
	return callerID
}

// TestWithdrawalGrantRevokeMissingGrantRefusalRecordedCallerZero covers revoke
// item #3: a revoke carries no caller input by design, so a revoke on a missing
// grant records `supply_refused` with caller_id 0 — an honest unattributable
// marker (never a fabricated caller, never operator-as-caller). That O is then
// a recorded refusal forever: same-O retries add no row and never upgrade to
// success, even after the grant is later supplied.
func TestWithdrawalGrantRevokeMissingGrantRefusalRecordedCallerZero(t *testing.T) {
	ctx, pool := grantSetup(t)
	grantSeedCaller(t, ctx, pool, 7201)

	const (
		authID = "auth-revoke-miss"
		opID   = "f0000000000000000000000000000001"
	)

	// Phase 1: revoke a grant that was never supplied → recorded refusal with
	// caller_id 0, and zero grant rows.
	out, err := RevokeGrant(ctx, pool, opID, authID, "operator-revoke", "no grant yet")
	if err != nil {
		t.Fatalf("revoke on missing grant: %v", err)
	}
	if out.Action != grantOutcomeSupplyRefused {
		t.Fatalf("outcome = %+v, want %s", out, grantOutcomeSupplyRefused)
	}
	if n := grantCount(t, ctx, pool, authID); n != 0 {
		t.Fatalf("grant rows after revoke-miss = %d, want 0 (nothing to revoke)", n)
	}
	if callerID := grantAuditCallerByOp(t, ctx, pool, opID); callerID != 0 {
		t.Fatalf("audit caller_id = %d, want 0 (honest unattributable marker)", callerID)
	}

	// Phase 2: same-O retry with different operator/reason → the recorded
	// refusal, still no second row (operator/reason are retry metadata).
	retry, err := RevokeGrant(ctx, pool, opID, authID, "operator-retry", "retry reason")
	if err != nil {
		t.Fatalf("same-O revoke retry: %v", err)
	}
	if retry.Action != grantOutcomeSupplyRefused {
		t.Fatalf("retry outcome = %+v, want recorded %s", retry, grantOutcomeSupplyRefused)
	}
	if n := grantAuditCountByOp(t, ctx, pool, opID); n != 1 {
		t.Fatalf("audit rows for O after retry = %d, want 1 (no new row)", n)
	}

	// Phase 3: the grant is later supplied, then the SAME O retries yet again —
	// the recorded refusal is never upgraded, the revoke UPDATE rolls back with
	// the conflicting audit INSERT, and the newly supplied grant stays active.
	if _, err := SupplyGrant(ctx, pool, grantTestOp("f0000000000000000000000000000002", authID, 7201, "100"), "op", "late supply"); err != nil {
		t.Fatalf("late supply: %v", err)
	}
	late, err := RevokeGrant(ctx, pool, opID, authID, "operator-late", "retry after supply")
	if err != nil {
		t.Fatalf("same-O revoke retry after supply: %v", err)
	}
	if late.Action != grantOutcomeSupplyRefused {
		t.Fatalf("retry-after-supply outcome = %+v, want recorded %s (never upgraded)", late, grantOutcomeSupplyRefused)
	}
	if state, _ := grantStateAndAmount(t, ctx, pool, authID); state != "active" {
		t.Fatalf("grant state after refused-O retry = %q, want active (revoke rolled back)", state)
	}
	if callerID := grantAuditCallerByOp(t, ctx, pool, opID); callerID != 0 {
		t.Fatalf("audit caller_id after retries = %d, want 0", callerID)
	}
	if n := grantAuditCountByOp(t, ctx, pool, opID); n != 1 {
		t.Fatalf("audit rows for O = %d, want exactly 1", n)
	}
	if acted := grantAuditActions(t, ctx, pool, authID); !hasAction(acted, grantOutcomeSupplyRefused) || !hasAction(acted, grantOutcomeSupplied) {
		t.Fatalf("audit actions = %v, want one supply_refused (the refused O) + one supplied (late supply)", acted)
	}
}

func hasAction(actions []string, want string) bool {
	for _, a := range actions {
		if a == want {
			return true
		}
	}
	return false
}

// TestWithdrawalGrantUncertainCommitSameOperationRetry simulates an uncertain
// COMMIT: the attempt row already exists (as if this tx committed but the
// response was lost). Retrying with the SAME operation id and op-input reports
// the recorded outcome without writing a second row.
func TestWithdrawalGrantUncertainCommitSameOperationRetry(t *testing.T) {
	ctx, pool := grantSetup(t)
	grantSeedCaller(t, ctx, pool, 7008)

	const authID = "auth-uncertain"
	op := grantTestOp("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", authID, 7008, "100")

	// Pre-seed the committed state: the grant row plus its 'supplied' attempt.
	if _, err := pool.Exec(ctx, `
INSERT INTO withdrawal_authorizations
    (authorization_id, caller_id, chain_id, asset, recipient, amount, state, supplied_by)
VALUES ($1, $2, $3, $4, $5, $6::numeric, 'active', 'seed')`,
		op.AuthorizationID, op.CallerID, op.ChainID, op.Asset, op.Recipient, op.Amount); err != nil {
		t.Fatalf("pre-seed grant: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO withdrawal_grant_audit
    (operation_id, authorization_id, caller_id, action, operator, reason, detail)
VALUES ($1, $2, $3, $4, 'seed', 'seed', $5)`,
		op.OperationID, op.AuthorizationID, op.CallerID, grantOutcomeSupplied, opInputDetail(op)); err != nil {
		t.Fatalf("pre-seed attempt: %v", err)
	}

	out, err := SupplyGrant(ctx, pool, op, "retry-operator", "retry-reason")
	if err != nil {
		t.Fatalf("same-O retry after uncertain commit: %v", err)
	}
	if out.Action != grantOutcomeSupplied {
		t.Fatalf("retry outcome = %+v, want recorded %s", out, grantOutcomeSupplied)
	}
	if n := grantAuditCountByOp(t, ctx, pool, op.OperationID); n != 1 {
		t.Fatalf("audit rows for op = %d, want 1 (no second row)", n)
	}
	if n := grantCount(t, ctx, pool, authID); n != 1 {
		t.Fatalf("grant rows = %d, want 1", n)
	}

	// ReadAttempt reports the recorded outcome for the same O.
	readBack, found, err := ReadAttempt(ctx, pool, op.OperationID)
	if err != nil || !found || readBack.Action != grantOutcomeSupplied {
		t.Fatalf("ReadAttempt = (%+v, %v, %v), want supplied/found", readBack, found, err)
	}
}

// ---------------------------------------------------------------------------
// T023 [US4] same-O op-conflict + grant-PK first-supply three-outcome matrix.
//
// These tests reuse the T008 grant helpers above unchanged and add only
// T023-prefixed helpers (grantRaceTwin, grantAuditRowByOp, grantRaceOutcome) so
// no name collides with migration_integration_test.go, idempotency_integration_test.go,
// or the T008 tests. Persistent state is read back from BOTH tables (COUNT /
// per-O rows / stored detail), never inferred from a return code alone.
// ---------------------------------------------------------------------------

// grantRaceTwin releases two supply attempts from one start barrier after
// proving the pool can hold both connections, then returns their outcomes and
// errors in caller-index order (outcomes[i]/errs[i] belong to ops[i]).
//
// The barrier is the only synchronization (the T021 discipline): every
// goroutine signals ready, parks at <-start, and close(start) launches both
// together — no sleeps, no chance-based pacing. idempotencyProvePoolCapacity
// is the deliberately generic T021 helper; db.OpenPool pins MaxConns to 8, so
// both SupplyGrant transactions are provably in flight. The three first-supply
// outcomes asserted by the callers are order-independent, so which caller wins
// the grant PK insert cannot make a test pass or fail for the wrong reason.
func grantRaceTwin(t *testing.T, ctx context.Context, pool *pgxpool.Pool, a, b OpInput) ([]*GrantOutcome, []error) {
	t.Helper()
	idempotencyProvePoolCapacity(t, ctx, pool, 2)
	ops := []OpInput{a, b}
	outs := make([]*GrantOutcome, len(ops))
	errs := make([]error, len(ops))
	var ready, done sync.WaitGroup
	start := make(chan struct{})
	ready.Add(len(ops))
	done.Add(len(ops))
	for i, op := range ops {
		go func(i int, op OpInput) {
			defer done.Done()
			ready.Done() // parked at the barrier
			<-start
			outs[i], errs[i] = SupplyGrant(ctx, pool, op, "op", "pk-race")
		}(i, op)
	}
	ready.Wait() // both callers parked
	close(start) // release them together
	done.Wait()
	return outs, errs
}

// grantRaceOutcome returns one twin's outcome, failing on any error. A
// grant-PK race is never surfaced to the caller as operation_conflict or a 503
// retryable: the loser restarts and reports a recorded outcome.
func grantRaceOutcome(t *testing.T, out *GrantOutcome, err error) *GrantOutcome {
	t.Helper()
	if err != nil {
		t.Fatalf("concurrent first-supply error: %v (never operation_conflict/503 for a PK race)", err)
	}
	if out == nil {
		t.Fatal("concurrent first-supply returned a nil outcome with no error")
	}
	return out
}

// grantAuditRowByOp reads the single audit row recorded for operationID — a
// persistent-state read (not a response) of that attempt's action and its
// redacted params snapshot. It fails unless exactly one row exists, so a
// duplicated attempt can never be silently hidden.
func grantAuditRowByOp(t *testing.T, ctx context.Context, pool *pgxpool.Pool, operationID string) (string, string) {
	t.Helper()
	rows, err := pool.Query(ctx,
		`SELECT action, detail FROM withdrawal_grant_audit WHERE operation_id = $1 ORDER BY audit_id`, operationID)
	if err != nil {
		t.Fatalf("query audit row for operation %q: %v", operationID, err)
	}
	defer rows.Close()
	var (
		action, detail string
		n              int
	)
	for rows.Next() {
		if err := rows.Scan(&action, &detail); err != nil {
			t.Fatalf("scan audit row for operation %q: %v", operationID, err)
		}
		n++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate audit row for operation %q: %v", operationID, err)
	}
	if n != 1 {
		t.Fatalf("audit rows for operation %q = %d, want exactly 1", operationID, n)
	}
	return action, detail
}

// TestWithdrawalGrantConflictSameOperationMatrix covers T023 same-O op-conflict
// deterministically (no race, no luck): a second attempt that reuses the
// original operation id with a DIFFERENT op-input MUST report
// operation_conflict with ZERO new business/audit writes, and the original row
// plus its recorded outcome stay intact — including a recorded refusal, which
// is never upgraded to success.
func TestWithdrawalGrantConflictSameOperationMatrix(t *testing.T) {
	// Given one recorded first supply.
	ctx, pool := grantSetup(t)
	grantSeedCaller(t, ctx, pool, 7101)
	const authID = "auth-conflict-matrix"
	original := grantTestOp("10000000000000000000000000000001", authID, 7101, "100")
	if out, err := SupplyGrant(ctx, pool, original, "op", "original"); err != nil || out.Action != grantOutcomeSupplied {
		t.Fatalf("original supply = (%+v, %v), want %s", out, err, grantOutcomeSupplied)
	}

	// When the same O is retried with a different amount, then with a different
	// authorization_id (both are same-O different-op-input).
	differAmount := original
	differAmount.Amount = "999"
	differAuth := original
	differAuth.AuthorizationID = "auth-conflict-other"
	for i, differ := range []OpInput{differAmount, differAuth} {
		out, err := SupplyGrant(ctx, pool, differ, "op", "differ")
		// Then the retry reports the conflict with no outcome and no new writes.
		if out != nil {
			t.Fatalf("conflicting retry %d outcome = %+v, want nil", i, out)
		}
		grantWantCode(t, err, CodeOperationConflict)
		if got := grantAuditCountByOp(t, ctx, pool, original.OperationID); got != 1 {
			t.Fatalf("retry %d: audit rows for original O = %d, want 1 (zero writes)", i, got)
		}
	}
	if n := grantCount(t, ctx, pool, "auth-conflict-other"); n != 0 {
		t.Fatalf("grant rows for the conflicting authorization_id = %d, want 0 (rolled back)", n)
	}

	// And the original row and its recorded outcome are untouched.
	if n := grantCount(t, ctx, pool, authID); n != 1 {
		t.Fatalf("grant rows for %q = %d, want 1", authID, n)
	}
	if state, amount := grantStateAndAmount(t, ctx, pool, authID); state != "active" || amount != "100" {
		t.Fatalf("original grant = (%s, %s), want (active, 100)", state, amount)
	}
	if readBack, found, err := ReadAttempt(ctx, pool, original.OperationID); err != nil || !found || readBack.Action != grantOutcomeSupplied {
		t.Fatalf("original outcome = (%+v, %v, %v), want supplied/found", readBack, found, err)
	}

	// When a refused attempt's O is retried with different input, the retry is a
	// conflict and the stored outcome stays 'supply_refused' (never upgraded).
	refusal := grantTestOp("10000000000000000000000000000002", authID, 7101, "200")
	if out, err := SupplyGrant(ctx, pool, refusal, "op", "refuse"); err != nil || out.Action != grantOutcomeSupplyRefused {
		t.Fatalf("refusal attempt = (%+v, %v), want %s", out, err, grantOutcomeSupplyRefused)
	}
	refusalDiffer := refusal
	refusalDiffer.Amount = "201"
	conflictOut, conflictErr := SupplyGrant(ctx, pool, refusalDiffer, "op", "differ")
	if conflictOut != nil {
		t.Fatalf("refused-O retry outcome = %+v, want nil", conflictOut)
	}
	grantWantCode(t, conflictErr, CodeOperationConflict)
	readBack, found, err := ReadAttempt(ctx, pool, refusal.OperationID)
	if err != nil || !found || readBack.Action != grantOutcomeSupplyRefused {
		t.Fatalf("stored refusal outcome = (%+v, %v, %v), want supply_refused/found", readBack, found, err)
	}
	if got := grantAuditCountByOp(t, ctx, pool, refusal.OperationID); got != 1 {
		t.Fatalf("audit rows for refusal O = %d, want 1", got)
	}
}

// TestWithdrawalGrantPKRaceSameOperationTwins covers T023 first-supply
// concurrency (i): same-O same-OPIN twins released together ⇒ ONE grant row and
// ONE `supplied` audit row; the loser restarts into resupply with the SAME O
// and every caller reports the recorded outcome — never operation_conflict/503.
func TestWithdrawalGrantPKRaceSameOperationTwins(t *testing.T) {
	ctx, pool := grantSetup(t)
	grantSeedCaller(t, ctx, pool, 7102)
	const authID = "auth-pk-same-o"
	op := grantTestOp("20000000000000000000000000000001", authID, 7102, "100")

	// When two identical same-O same-OPIN first-supplies are released together.
	outs, errs := grantRaceTwin(t, ctx, pool, op, op)

	// Then both report the one recorded outcome, with one grant + one audit row.
	for i := range outs {
		if o := grantRaceOutcome(t, outs[i], errs[i]); o.Action != grantOutcomeSupplied {
			t.Fatalf("twin %d outcome = %+v, want %s", i, o, grantOutcomeSupplied)
		}
	}
	if n := grantCount(t, ctx, pool, authID); n != 1 {
		t.Fatalf("grant rows = %d, want 1", n)
	}
	acted := grantAuditActions(t, ctx, pool, authID)
	if len(acted) != 1 || acted[0] != grantOutcomeSupplied {
		t.Fatalf("audit actions = %v, want exactly [supplied]", acted)
	}
	if got := grantAuditCountByOp(t, ctx, pool, op.OperationID); got != 1 {
		t.Fatalf("audit rows for shared O = %d, want 1", got)
	}
	if action, detail := grantAuditRowByOp(t, ctx, pool, op.OperationID); action != grantOutcomeSupplied || detail != opInputDetail(op) {
		t.Fatalf("stored attempt = (%q, %q), want (%q, %q)", action, detail, grantOutcomeSupplied, opInputDetail(op))
	}
}

// TestWithdrawalGrantPKRaceDifferentOperationSameInput covers T023 first-supply
// concurrency (ii): different-O same-OPIN twins ⇒ ONE grant row (the first
// supply) plus ONE audit row PER O — `supplied` for the winner and `resupplied`
// for the loser — never operation_conflict/503.
func TestWithdrawalGrantPKRaceDifferentOperationSameInput(t *testing.T) {
	ctx, pool := grantSetup(t)
	grantSeedCaller(t, ctx, pool, 7103)
	const authID = "auth-pk-two-o"
	opA := grantTestOp("30000000000000000000000000000001", authID, 7103, "100")
	opB := grantTestOp("30000000000000000000000000000002", authID, 7103, "100")

	outs, errs := grantRaceTwin(t, ctx, pool, opA, opB)

	for i := range outs {
		o := grantRaceOutcome(t, outs[i], errs[i])
		if o.Action != grantOutcomeSupplied && o.Action != grantOutcomeResupplied {
			t.Fatalf("twin %d outcome = %+v, want supplied or resupplied (never conflict/503)", i, o)
		}
	}
	if n := grantCount(t, ctx, pool, authID); n != 1 {
		t.Fatalf("grant rows = %d, want 1", n)
	}
	actionA, _ := grantAuditRowByOp(t, ctx, pool, opA.OperationID)
	actionB, _ := grantAuditRowByOp(t, ctx, pool, opB.OperationID)
	acted := grantAuditActions(t, ctx, pool, authID)
	if len(acted) != 2 || !hasAction(acted, grantOutcomeSupplied) || !hasAction(acted, grantOutcomeResupplied) {
		t.Fatalf("audit actions = %v, want exactly one supplied + one resupplied", acted)
	}
	if (actionA != grantOutcomeSupplied || actionB != grantOutcomeResupplied) &&
		(actionA != grantOutcomeResupplied || actionB != grantOutcomeSupplied) {
		t.Fatalf("per-O actions = (%q, %q), want {supplied, resupplied}", actionA, actionB)
	}
	if state, amount := grantStateAndAmount(t, ctx, pool, authID); state != "active" || amount != "100" {
		t.Fatalf("grant = (%s, %s), want (active, 100) (same OPIN)", state, amount)
	}
}

// TestWithdrawalGrantPKRaceDifferentOperationDifferentInput covers T023
// first-supply concurrency (iii): different-O different-OPIN twins ⇒ ONE grant
// row carrying the winner's params + winner `supplied` + loser `supply_refused`
// whose `detail` records the loser's own params; the grant row is untouched by
// the loser — never operation_conflict/503 for the grant-PK race.
func TestWithdrawalGrantPKRaceDifferentOperationDifferentInput(t *testing.T) {
	ctx, pool := grantSetup(t)
	grantSeedCaller(t, ctx, pool, 7104)
	const authID = "auth-pk-differ"
	opA := grantTestOp("40000000000000000000000000000001", authID, 7104, "100")
	opB := grantTestOp("40000000000000000000000000000002", authID, 7104, "200")

	outs, errs := grantRaceTwin(t, ctx, pool, opA, opB)

	for i := range outs {
		o := grantRaceOutcome(t, outs[i], errs[i])
		if o.Action != grantOutcomeSupplied && o.Action != grantOutcomeSupplyRefused {
			t.Fatalf("twin %d outcome = %+v, want supplied or supply_refused (never conflict/503)", i, o)
		}
	}
	if n := grantCount(t, ctx, pool, authID); n != 1 {
		t.Fatalf("grant rows = %d, want 1", n)
	}
	acted := grantAuditActions(t, ctx, pool, authID)
	if len(acted) != 2 || !hasAction(acted, grantOutcomeSupplied) || !hasAction(acted, grantOutcomeSupplyRefused) {
		t.Fatalf("audit actions = %v, want exactly one supplied + one supply_refused", acted)
	}

	actionA, detailA := grantAuditRowByOp(t, ctx, pool, opA.OperationID)
	actionB, detailB := grantAuditRowByOp(t, ctx, pool, opB.OperationID)
	winner, loser := opA, opB
	winnerAction, loserAction, loserDetail := actionA, actionB, detailB
	if actionB == grantOutcomeSupplied {
		winner, loser = opB, opA
		winnerAction, loserAction, loserDetail = actionB, actionA, detailA
	}
	if winnerAction != grantOutcomeSupplied || loserAction != grantOutcomeSupplyRefused {
		t.Fatalf("per-O actions = (%q, %q), want winner supplied + loser supply_refused", actionA, actionB)
	}
	// The winner's params are the grant row; the loser's own params live only in
	// its refusal detail, and the grant carries nothing of the loser.
	if state, amount := grantStateAndAmount(t, ctx, pool, authID); state != "active" || amount != winner.Amount {
		t.Fatalf("grant = (%s, %s), want (active, %s) — the winner's params", state, amount, winner.Amount)
	}
	if loserDetail != opInputDetail(loser) {
		t.Fatalf("loser refusal detail = %q, want %q (loser's attempted params)", loserDetail, opInputDetail(loser))
	}
}

// ---------------------------------------------------------------------------
// T010 [US1] legal scoped-supply round-trip (fail-first).
//
// A legal supply writes grant + scope + audit in the SAME transaction: the
// scope row is 1:1 with the grant with authorization_version 1, every scope
// column equals the op-input payload (row equality, sender lowercased), and
// the recorded audit attempt agrees with grant↔scope↔principal (same
// authorization_id, same principal in the snapshot). This test is RED until
// T012 writes the scope row inside runSupplyTx; T010 does not implement it.
// Deterministic single round-trip, no sleeps-as-proof.
// ---------------------------------------------------------------------------

// grantScopedTestOp is a fully-populated legal scoped supply op-input: the
// eight Table 6 fields plus every PB scope field and the server-resolved
// attested_by, matching the contract's extended supply entry.
func grantScopedTestOp(operationID, authorizationID string, callerID int64) OpInput {
	op := grantTestOp(operationID, authorizationID, callerID, "100")
	op.IntentID = "intent-" + authorizationID
	op.RequestID = "request-" + authorizationID
	op.Sender = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	op.FeeMaxTotal = 21000
	op.FeeMaxPerGas = 2
	op.FeeMaxPriority = 1
	op.AllowsFeeReplacement = true
	op.AttestedBy = "principal-issuer"
	return op
}

// grantScopeRow is one withdrawal_authorization_scopes row read back verbatim.
type grantScopeRow struct {
	authorizationID      string
	intentID             string
	requestID            string
	sender               string
	feeMaxTotal          int64
	feeMaxPerGas         int64
	feeMaxPriority       int64
	allowsFeeReplacement bool
	authorizationVersion int64
	attestedBy           string
}

// grantReadScopeRow reads the single scope row for a grant, failing when it is
// absent — which is exactly the T012 gap this fail-first test pins down. It
// reads persistent state, never an inferred return code.
func grantReadScopeRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, authorizationID string) grantScopeRow {
	t.Helper()
	var r grantScopeRow
	err := pool.QueryRow(ctx, `
SELECT authorization_id, intent_id, request_id, sender, fee_max_total,
       fee_max_per_gas, fee_max_priority, allows_fee_replacement,
       authorization_version, attested_by
FROM withdrawal_authorization_scopes
WHERE authorization_id = $1`, authorizationID).
		Scan(&r.authorizationID, &r.intentID, &r.requestID, &r.sender, &r.feeMaxTotal,
			&r.feeMaxPerGas, &r.feeMaxPriority, &r.allowsFeeReplacement,
			&r.authorizationVersion, &r.attestedBy)
	if err != nil {
		t.Fatalf("scope row for grant %q missing/unreadable (T012 must write it in the supply tx): %v",
			authorizationID, err)
	}
	return r
}

// grantCallerIDByID reads the stored caller_id of a grant row.
func grantCallerIDByID(t *testing.T, ctx context.Context, pool *pgxpool.Pool, authorizationID string) int64 {
	t.Helper()
	var callerID int64
	if err := pool.QueryRow(ctx,
		`SELECT caller_id FROM withdrawal_authorizations WHERE authorization_id = $1`, authorizationID).
		Scan(&callerID); err != nil {
		t.Fatalf("read grant caller for %q: %v", authorizationID, err)
	}
	return callerID
}

// TestWithdrawalGrantScopedSupplyWritesScopeRow is T010: a legal supply must
// write grant + scope + audit atomically with matching payloads.
func TestWithdrawalGrantScopedSupplyWritesScopeRow(t *testing.T) {
	ctx, pool := grantSetup(t)
	grantSeedCaller(t, ctx, pool, 7301)

	const (
		authID = "auth-scoped"
		opID   = "50000000000000000000000000000001"
	)
	op := grantScopedTestOp(opID, authID, 7301)

	out, err := SupplyGrant(ctx, pool, op, "operator-scoped", "legal scoped supply")
	if err != nil {
		t.Fatalf("scoped SupplyGrant: %v", err)
	}
	if out.Action != grantOutcomeSupplied || out.AuthorizationID != authID {
		t.Fatalf("outcome = %+v, want {%s %s}", out, grantOutcomeSupplied, authID)
	}
	if n := grantCount(t, ctx, pool, authID); n != 1 {
		t.Fatalf("grant rows = %d, want 1", n)
	}

	// Scope row: 1:1 with the grant, every column equal to the op-input scope
	// payload (sender normalized to lowercase), version 1 on first supply.
	scope := grantReadScopeRow(t, ctx, pool, authID)
	if scope.authorizationID != authID || scope.authorizationID != op.AuthorizationID {
		t.Fatalf("scope authorization_id = %q, want the grant/op id %q", scope.authorizationID, authID)
	}
	if scope.intentID != op.IntentID || scope.requestID != op.RequestID {
		t.Fatalf("scope identity = (%q, %q), want (%q, %q)",
			scope.intentID, scope.requestID, op.IntentID, op.RequestID)
	}
	if wantSender := strings.ToLower(op.Sender); scope.sender != wantSender {
		t.Fatalf("scope sender = %q, want lowercased %q", scope.sender, wantSender)
	}
	if scope.feeMaxTotal != op.FeeMaxTotal || scope.feeMaxPerGas != op.FeeMaxPerGas ||
		scope.feeMaxPriority != op.FeeMaxPriority || scope.allowsFeeReplacement != op.AllowsFeeReplacement {
		t.Fatalf("scope fee/purpose = (%d, %d, %d, %t), want (%d, %d, %d, %t)",
			scope.feeMaxTotal, scope.feeMaxPerGas, scope.feeMaxPriority, scope.allowsFeeReplacement,
			op.FeeMaxTotal, op.FeeMaxPerGas, op.FeeMaxPriority, op.AllowsFeeReplacement)
	}
	if scope.attestedBy != op.AttestedBy {
		t.Fatalf("scope attested_by = %q, want %q (server-resolved principal)", scope.attestedBy, op.AttestedBy)
	}
	if scope.authorizationVersion != 1 {
		t.Fatalf("authorization_version = %d, want 1 on first scoped supply", scope.authorizationVersion)
	}

	// Audit agreement grant↔scope↔principal: the recorded attempt names the
	// same grant/scope row, binds the same scope snapshot (so the stored scope
	// payload is what the attempt recorded), attributes the same caller, and
	// carries the scope's principal in attested_by.
	action, detail := grantAuditRowByOp(t, ctx, pool, opID)
	if action != grantOutcomeSupplied {
		t.Fatalf("audit action = %q, want %s", action, grantOutcomeSupplied)
	}
	if detail != opInputDetail(op) {
		t.Fatalf("audit detail = %q, want the scoped op-input snapshot %q", detail, opInputDetail(op))
	}
	if callerID := grantAuditCallerByOp(t, ctx, pool, opID); callerID != op.CallerID {
		t.Fatalf("audit caller_id = %d, want %d", callerID, op.CallerID)
	}
	if callerID := grantCallerIDByID(t, ctx, pool, authID); callerID != op.CallerID {
		t.Fatalf("grant caller_id = %d, want %d", callerID, op.CallerID)
	}
	if wantPrincipal := "attested_by=" + strconv.Quote(scope.attestedBy); !strings.Contains(detail, wantPrincipal) {
		t.Fatalf("audit detail %q must agree with scope principal %q", detail, scope.attestedBy)
	}
}

// TestWithdrawalGrantScopedSupplyFeeTripleBoundaries is T016 against a real
// PostgreSQL: every PB-C2 fee boundary is exercised on both sides. A fee triple
// exactly at the priority<=per-gas cap, and the legacy gas_price path
// (priority 0), write the scope row with the supplied values; a triple one unit
// over the cap or missing an applicable cap is refused with zero grant, scope
// and audit rows. Deterministic, one row census per case.
func TestWithdrawalGrantScopedSupplyFeeTripleBoundaries(t *testing.T) {
	ctx, pool := grantSetup(t)
	grantSeedCaller(t, ctx, pool, 7302)

	type greenCase struct {
		name                    string
		total, perGas, priority int64
	}
	greens := []greenCase{
		{"priority at per-gas cap", 10, 10, 10},
		{"legacy gas_price path (priority zero)", 21000, 2, 0},
	}
	for i, tc := range greens {
		t.Run(tc.name, func(t *testing.T) {
			authID := "auth-fee-green-" + strconv.Itoa(i)
			opID := "6000000000000000000000000000000" + strconv.Itoa(i)
			op := grantScopedTestOp(opID, authID, 7302)
			op.FeeMaxTotal, op.FeeMaxPerGas, op.FeeMaxPriority = tc.total, tc.perGas, tc.priority

			out, err := SupplyGrant(ctx, pool, op, "op-fee", "fee boundary green")
			if err != nil {
				t.Fatalf("SupplyGrant() error = %v, want supplied", err)
			}
			if out.Action != grantOutcomeSupplied {
				t.Fatalf("outcome = %+v, want %s", out, grantOutcomeSupplied)
			}
			if n := grantCount(t, ctx, pool, authID); n != 1 {
				t.Fatalf("grant rows = %d, want 1", n)
			}
			scope := grantReadScopeRow(t, ctx, pool, authID)
			if scope.feeMaxTotal != tc.total || scope.feeMaxPerGas != tc.perGas || scope.feeMaxPriority != tc.priority {
				t.Fatalf("scope fee triple = (%d, %d, %d), want (%d, %d, %d)",
					scope.feeMaxTotal, scope.feeMaxPerGas, scope.feeMaxPriority,
					tc.total, tc.perGas, tc.priority)
			}
		})
	}

	reds := []struct {
		name                    string
		total, perGas, priority int64
		wantField               string
	}{
		{"priority one above per-gas cap", 10, 10, 11, "fee_max_priority"},
		{"missing total cap", 0, 10, 0, "fee_max_total"},
		{"missing per-gas cap", 100, 0, 0, "fee_max_per_gas"},
	}
	for i, tc := range reds {
		t.Run(tc.name+" refused with zero rows", func(t *testing.T) {
			authID := "auth-fee-red-" + strconv.Itoa(i)
			opID := "6100000000000000000000000000000" + strconv.Itoa(i)
			op := grantScopedTestOp(opID, authID, 7302)
			op.FeeMaxTotal, op.FeeMaxPerGas, op.FeeMaxPriority = tc.total, tc.perGas, tc.priority

			out, err := SupplyGrant(ctx, pool, op, "op-fee", "fee boundary refused")
			if out != nil {
				t.Fatalf("outcome = %+v, want nil", out)
			}
			e := grantWantCode(t, err, CodeValidationFailed)
			if e.Field != tc.wantField {
				t.Fatalf("error field = %q, want %q", e.Field, tc.wantField)
			}
			if n := grantCount(t, ctx, pool, authID); n != 0 {
				t.Fatalf("grant rows after refusal = %d, want 0", n)
			}
			if n := grantAuthorityScopeCount(t, ctx, pool, authID); n != 0 {
				t.Fatalf("scope rows after refusal = %d, want 0", n)
			}
			if n := grantAuditCountByOp(t, ctx, pool, opID); n != 0 {
				t.Fatalf("audit rows after refusal = %d, want 0", n)
			}
		})
	}
}
