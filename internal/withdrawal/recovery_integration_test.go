//go:build integration

// recovery_integration_test.go owns spec task T024 (US5, FR-12/FR-13, SC-05,
// V6): crash / restart / retry recovery on a real PostgreSQL 18 container
// (testcontainers). Every guarantee here is carried by durable rows, so a
// restart replays against the SAME constraints and the SAME operation id (O) —
// never against in-memory state and never by minting a fresh O.
//
// Three evidence classes are distinguished by name (data-model.md Table 6 and
// §四 scenario 5):
//   - pre-commit failure: ZERO durable rows; the attempt never landed, so
//     nothing needs same-O recovery and ReadAttempt proves the absence.
//   - committed-but-response-lost: exactly ONE recorded row; a same-O retry
//     converges to that recorded outcome with no second row.
//   - still-unknown: O-miss + grant-miss is retryable WITH the SAME O, never
//     proof of rollback and never a new mint.
//
// Scene mapping: (a) crash after O-capture — never landed
// (TestWithdrawalRecoveryUnknownRetryableSameOperation) and committed
// (TestWithdrawalRecoveryCommittedResponseLostConverges); (b) crash before
// O-capture — TestWithdrawalRecoveryCrashBeforeCaptureNewAttempt; (c) committed
// response lost — TestWithdrawalRecoveryCommittedResponseLostNewPoolReRead.
// Table 6's grant-pre-exists + this-attempt-refused rule is
// TestWithdrawalRecoveryGrantPreExistsAttemptRefused.
//
// Fault mechanism HONESTLY: every "crash" in THIS file is a cancelled caller
// context plus a closed actor pool, then a NEW pool over the same DSN. That is
// a LOST CALLER, not a dead PROCESS: no signal is sent and the OS process stays
// alive. It proves durable-row recovery from a dropped connection, and it is
// NOT proof of kill-9 / process-death equivalence — no phrase here is backed by
// a signal. The genuine SIGKILL evidence (a real child process running the real
// WithdrawalHandler, killed mid-request and restarted) lives in
// recovery_kill_integration_test.go
// (TestWithdrawalRecoverySigkillChildPreCommit /
// TestWithdrawalRecoverySigkillChildUnknownCommit).
//
// It reuses the T006 container/migration helpers (withdrawalStartPostgres,
// withdrawalMigrateOptions) and the T008 grant fixtures (grantSeedCaller,
// grantTestOp, grantCount, grantAuditCountByOp, grantStateAndAmount,
// grantWantCode) and adds only recovery-prefixed helpers, so no name collides
// with a sibling test file. Production code is untouched.
package withdrawal

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/db"
)

// recoverySetup boots a migrated scratch PostgreSQL and returns its DSN plus a
// first ("actor") pool. The DSN is returned so restart scenes can open a NEW
// pool instance against the same durable database.
func recoverySetup(t *testing.T) (context.Context, string, *pgxpool.Pool) {
	t.Helper()
	dsn := withdrawalStartPostgres(t)
	ctx := context.Background()
	if err := db.MigrateUp(ctx, withdrawalMigrateOptions(dsn), io.Discard); err != nil {
		t.Fatalf("migrate up: %v", err)
	}
	return ctx, dsn, recoveryRestart(t, dsn)
}

// recoveryRestart opens a brand-new pool (fresh connection instances) over the
// same DSN, re-reading durable state from PostgreSQL. Callers MUST stop using
// the previous in-memory pool before calling this.
func recoveryRestart(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()
	pool, err := db.OpenPool(context.Background(), dsn, 5*time.Second)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// recoveryMint mints one opaque attempt key, failing the test on error.
func recoveryMint(t *testing.T) string {
	t.Helper()
	id, err := MintOperationID()
	if err != nil {
		t.Fatalf("mint operation id: %v", err)
	}
	return id
}

// recoveryAuditRowCount returns the total row count of withdrawal_grant_audit,
// so a scene can prove no extra attempt row appeared across a restart.
func recoveryAuditRowCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM withdrawal_grant_audit`).Scan(&n); err != nil {
		t.Fatalf("count all grant audit rows: %v", err)
	}
	return n
}

// recoveryAssertZeroDurableRows proves a pre-commit failure left no durable
// evidence: zero audit rows, zero grant rows, and no recorded outcome for O.
func recoveryAssertZeroDurableRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, op OpInput) {
	t.Helper()
	if n := recoveryAuditRowCount(t, ctx, pool); n != 0 {
		t.Fatalf("grant audit rows = %d, want 0 (pre-commit failure must not persist)", n)
	}
	if n := grantCount(t, ctx, pool, op.AuthorizationID); n != 0 {
		t.Fatalf("grant rows for %q = %d, want 0", op.AuthorizationID, n)
	}
	out, found, err := ReadAttempt(ctx, pool, op.OperationID)
	if err != nil {
		t.Fatalf("ReadAttempt(%q): %v", op.OperationID, err)
	}
	if found || out != nil {
		t.Fatalf("ReadAttempt(%q) = (%+v, %v), want not found", op.OperationID, out, found)
	}
}

// TestWithdrawalRecoveryPreCommitZeroRows covers evidence class 1: a
// pre-commit failure leaves zero durable rows and never fabricates an outcome.
func TestWithdrawalRecoveryPreCommitZeroRows(t *testing.T) {
	t.Run("validation_failure_before_any_statement", func(t *testing.T) {
		// Given a migrated database and a captured O with an invalid op-input
		ctx, _, pool := recoverySetup(t)
		grantSeedCaller(t, ctx, pool, 7211)
		op := grantTestOp(recoveryMint(t), "auth-precommit-invalid", 7211, "0")

		// When the supply attempt is validated
		_, err := SupplyGrant(ctx, pool, op, "op", "invalid amount")

		// Then it is a definite pre-commit failure and nothing is persisted
		grantWantCode(t, err, CodeValidationFailed)
		recoveryAssertZeroDurableRows(t, ctx, pool, op)
	})

	t.Run("caller_context_cancel_before_commit", func(t *testing.T) {
		// Given a captured O and a caller context cancelled before any
		// statement lands (a lost caller; the process stays alive)
		ctx, _, pool := recoverySetup(t)
		grantSeedCaller(t, ctx, pool, 7212)
		op := grantTestOp(recoveryMint(t), "auth-precommit-dead", 7212, "100")
		crashCtx, cancel := context.WithCancel(ctx)
		cancel() // caller context cancelled before any statement lands

		// When the attempt runs on the dead context
		_, err := SupplyGrant(crashCtx, pool, op, "op", "pre-commit crash")

		// Then it fails (never a false success) and leaves zero rows
		if err == nil {
			t.Fatal("expected a pre-commit failure, got success")
		}
		recoveryAssertZeroDurableRows(t, ctx, pool, op)
	})
}

// TestWithdrawalRecoveryUnknownRetryableSameOperation covers evidence class 3:
// O-miss + grant-miss is "still unknown", never proof of rollback. The caller
// retries with the SAME O and SAME op-input — a fresh O would be a NEW attempt.
func TestWithdrawalRecoveryUnknownRetryableSameOperation(t *testing.T) {
	// Given a candidate grant and a supply attempt whose caller context was
	// cancelled before any statement landed (mint O, then cancel)
	ctx, dsn, pool := recoverySetup(t)
	grantSeedCaller(t, ctx, pool, 7221)
	op := grantTestOp(recoveryMint(t), "auth-unknown-retry", 7221, "100")
	crashCtx, cancel := context.WithCancel(ctx)
	cancel()

	// When the aborted attempt is classified
	_, err := SupplyGrant(crashCtx, pool, op, "op", "unknown outcome")

	// Then it is retryable (unknown storage/outcome), never a success, and
	// neither the O nor the grant row exists yet
	grantWantCode(t, err, CodeTemporarilyUnavailable)
	recoveryAssertZeroDurableRows(t, ctx, pool, op)

	// When the operator restarts (a NEW pool) and retries with the SAME O
	pool.Close()
	restarted := recoveryRestart(t, dsn)
	out, err := SupplyGrant(ctx, restarted, op, "op", "same-O retry")

	// Then the still-unknown attempt proceeds as a first supply under that SAME O
	if err != nil {
		t.Fatalf("same-O retry after unknown outcome: %v", err)
	}
	if out.Action != grantOutcomeSupplied {
		t.Fatalf("same-O retry outcome = %+v, want %s", out, grantOutcomeSupplied)
	}
	if n := grantAuditCountByOp(t, ctx, restarted, op.OperationID); n != 1 {
		t.Fatalf("audit rows for the retried O = %d, want 1", n)
	}
	if n := recoveryAuditRowCount(t, ctx, restarted); n != 1 {
		t.Fatalf("total audit rows = %d, want 1", n)
	}
	readBack, found, err := ReadAttempt(ctx, restarted, op.OperationID)
	if err != nil || !found || readBack.Action != grantOutcomeSupplied {
		t.Fatalf("ReadAttempt(same O) = (%+v, %v, %v), want supplied/found", readBack, found, err)
	}
}

// TestWithdrawalRecoveryCommittedResponseLostConverges covers evidence class 2:
// the attempt reached COMMIT but its caller never read the result. A same-O
// retry after restart converges to the recorded outcome, writing no second row.
func TestWithdrawalRecoveryCommittedResponseLostConverges(t *testing.T) {
	// Given the first attempt committed
	ctx, dsn, actor := recoverySetup(t)
	grantSeedCaller(t, ctx, actor, 7231)
	const authID = "auth-response-lost"
	op := grantTestOp(recoveryMint(t), authID, 7231, "100")
	first, err := SupplyGrant(ctx, actor, op, "op", "first attempt (response lost)")
	if err != nil {
		t.Fatalf("first supply: %v", err)
	}
	if first.Action != grantOutcomeSupplied {
		t.Fatalf("first outcome = %+v, want %s", first, grantOutcomeSupplied)
	}

	// When the process restarts (all in-memory state dropped; NEW pool)
	actor.Close()
	restarted := recoveryRestart(t, dsn)

	// Then a same-O + same-op-input retry converges to the recorded outcome
	retry, err := SupplyGrant(ctx, restarted, op, "retry-op", "retry-reason")
	if err != nil {
		t.Fatalf("same-O retry: %v", err)
	}
	if retry.Action != first.Action {
		t.Fatalf("retry outcome = %+v, want recorded %s", retry, first.Action)
	}
	// ... without a second audit row or a second grant row
	if n := recoveryAuditRowCount(t, ctx, restarted); n != 1 {
		t.Fatalf("total audit rows = %d, want 1 (no duplicate attempt)", n)
	}
	if n := grantAuditCountByOp(t, ctx, restarted, op.OperationID); n != 1 {
		t.Fatalf("audit rows for O = %d, want 1", n)
	}
	if n := grantCount(t, ctx, restarted, authID); n != 1 {
		t.Fatalf("grant rows = %d, want 1", n)
	}
	readBack, found, err := ReadAttempt(ctx, restarted, op.OperationID)
	if err != nil || !found || readBack.Action != grantOutcomeSupplied {
		t.Fatalf("ReadAttempt = (%+v, %v, %v), want supplied/found", readBack, found, err)
	}
}

// TestWithdrawalRecoveryCrashBeforeCaptureNewAttempt covers scene (b): a crash
// before O-capture means no attempt ever existed, so a fresh mint is a NEW
// attempt by definition — two distinct O values, two distinct audit rows, and
// no false convergence of the new attempt onto the old one.
func TestWithdrawalRecoveryCrashBeforeCaptureNewAttempt(t *testing.T) {
	// Given the first operator captured O1 and supplied the grant
	ctx, dsn, actor := recoverySetup(t)
	grantSeedCaller(t, ctx, actor, 7241)
	const authID = "auth-before-capture"
	o1 := recoveryMint(t)
	op1 := grantTestOp(o1, authID, 7241, "100")
	out1, err := SupplyGrant(ctx, actor, op1, "op", "first operator")
	if err != nil || out1.Action != grantOutcomeSupplied {
		t.Fatalf("first supply = (%+v, %v), want %s", out1, err, grantOutcomeSupplied)
	}

	// When the process restarts and a second operator mints a fresh O2 (their
	// crash happened before any capture) with the SAME op-input
	actor.Close()
	restarted := recoveryRestart(t, dsn)
	o2 := recoveryMint(t)
	if o2 == o1 {
		t.Fatalf("fresh mint reused the prior operation id %q", o1)
	}
	op2 := grantTestOp(o2, authID, 7241, "100")
	out2, err := SupplyGrant(ctx, restarted, op2, "op", "second attempt (new O)")

	// Then O2 records its OWN outcome — no false convergence onto O1
	if err != nil || out2.Action != grantOutcomeResupplied {
		t.Fatalf("second attempt = (%+v, %v), want %s", out2, err, grantOutcomeResupplied)
	}
	if n := recoveryAuditRowCount(t, ctx, restarted); n != 2 {
		t.Fatalf("total audit rows = %d, want 2 (one row per operation id)", n)
	}
	if n := grantCount(t, ctx, restarted, authID); n != 1 {
		t.Fatalf("grant rows = %d, want 1", n)
	}
	for _, tc := range []struct{ opID, want string }{
		{o1, grantOutcomeSupplied},
		{o2, grantOutcomeResupplied},
	} {
		got, found, err := ReadAttempt(ctx, restarted, tc.opID)
		if err != nil || !found || got.Action != tc.want {
			t.Fatalf("ReadAttempt(%s) = (%+v, %v, %v), want %s/found", tc.opID, got, found, err, tc.want)
		}
	}
}

// TestWithdrawalRecoveryCommittedResponseLostNewPoolReRead covers scene (c) as
// THIS file can actually produce it: a committed attempt whose caller context
// is cancelled and whose actor pool is closed, then a NEW pool over the same
// DSN. It proves a committed outcome survives a dropped caller connection; it
// is NOT kill-9 (no process is signalled) — the genuine SIGKILL evidence lives
// in recovery_kill_integration_test.go.
func TestWithdrawalRecoveryCommittedResponseLostNewPoolReRead(t *testing.T) {
	// Given an in-flight attempt whose caller never reads the result
	ctx, dsn, actor := recoverySetup(t)
	grantSeedCaller(t, ctx, actor, 7251)
	op := grantTestOp(recoveryMint(t), "auth-committed-lost", 7251, "100")
	actCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = SupplyGrant(actCtx, actor, op, "op", "committed response lost")
	}()
	<-done
	cancel()      // the caller connection is abandoned (no process death)
	actor.Close() // the actor pool is dropped; the process stays alive

	// When a NEW pool re-reads the SAME O over the same durable database
	restarted := recoveryRestart(t, dsn)
	readBack, found, err := ReadAttempt(ctx, restarted, op.OperationID)

	// Then the recorded outcome is visible and no duplicate row exists
	if err != nil || !found {
		t.Fatalf("ReadAttempt after committed response loss = (%+v, %v, %v), want found", readBack, found, err)
	}
	if readBack.Action != grantOutcomeSupplied {
		t.Fatalf("recorded outcome = %q, want %s", readBack.Action, grantOutcomeSupplied)
	}
	if n := recoveryAuditRowCount(t, ctx, restarted); n != 1 {
		t.Fatalf("total audit rows = %d, want 1", n)
	}
	if n := grantAuditCountByOp(t, ctx, restarted, op.OperationID); n != 1 {
		t.Fatalf("audit rows for O = %d, want 1", n)
	}
	if n := grantCount(t, ctx, restarted, op.AuthorizationID); n != 1 {
		t.Fatalf("grant rows = %d, want 1", n)
	}

	// And a same-O retry still converges without a duplicate
	retry, err := SupplyGrant(ctx, restarted, op, "op", "same-O recovery")
	if err != nil || retry.Action != grantOutcomeSupplied {
		t.Fatalf("same-O retry = (%+v, %v), want %s", retry, err, grantOutcomeSupplied)
	}
	if n := recoveryAuditRowCount(t, ctx, restarted); n != 1 {
		t.Fatalf("total audit rows after retry = %d, want 1", n)
	}
}

// TestWithdrawalRecoveryGrantPreExistsAttemptRefused covers Table 6's
// grant-pre-exists + this-attempt-refused rule: a restart + same-O retry stays
// a refusal — never business success — and the winner's grant row is untouched.
func TestWithdrawalRecoveryGrantPreExistsAttemptRefused(t *testing.T) {
	// Given a grant supplied with the winner's params
	ctx, dsn, actor := recoverySetup(t)
	grantSeedCaller(t, ctx, actor, 7261)
	const authID = "auth-refused-recovery"
	winner := grantTestOp(recoveryMint(t), authID, 7261, "100")
	if out, err := SupplyGrant(ctx, actor, winner, "op", "winner"); err != nil || out.Action != grantOutcomeSupplied {
		t.Fatalf("winner supply = (%+v, %v), want %s", out, err, grantOutcomeSupplied)
	}

	// When an attempt with different params is refused
	refused := grantTestOp(recoveryMint(t), authID, 7261, "999")
	out, err := SupplyGrant(ctx, actor, refused, "op", "refuse")
	if err != nil || out.Action != grantOutcomeSupplyRefused {
		t.Fatalf("refused attempt = (%+v, %v), want %s", out, err, grantOutcomeSupplyRefused)
	}

	// When the process restarts and the SAME O is retried
	actor.Close()
	restarted := recoveryRestart(t, dsn)

	// Then the recorded outcome stays a refusal (never upgraded), with no new row
	readBack, found, err := ReadAttempt(ctx, restarted, refused.OperationID)
	if err != nil || !found || readBack.Action != grantOutcomeSupplyRefused {
		t.Fatalf("ReadAttempt(refused) = (%+v, %v, %v), want supply_refused/found", readBack, found, err)
	}
	retry, err := SupplyGrant(ctx, restarted, refused, "op", "same-O retry")
	if err != nil || retry.Action != grantOutcomeSupplyRefused {
		t.Fatalf("same-O retry = (%+v, %v), want %s (never business success)", retry, err, grantOutcomeSupplyRefused)
	}
	if n := recoveryAuditRowCount(t, ctx, restarted); n != 2 {
		t.Fatalf("total audit rows = %d, want 2 (winner + refusal)", n)
	}
	if state, amount := grantStateAndAmount(t, ctx, restarted, authID); state != "active" || amount != "100" {
		t.Fatalf("grant after refused recovery = (%s, %s), want (active, 100) intact", state, amount)
	}
}
