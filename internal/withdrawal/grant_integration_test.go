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
