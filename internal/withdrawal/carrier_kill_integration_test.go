//go:build integration

// carrier_kill_integration_test.go owns the V-PB7 crash proof for the SCOPED
// authorization-carrier supply: a real child process executes the production
// scoped three-table write (grant + scope + audit in one tx) and is killed with
// a real SIGKILL at a deterministic, phase-point coordination (a file barrier
// or a lock-parked statement — never a sleep-as-proof).
//
// Recovery_kill_integration_test.go proves the 007 HTTP receipt path; it seeds
// SCOPELESS grants and drives app.WithdrawalHandler. THIS file is the scoped
// complement: the child calls the real production carrier entry
// withdrawal.SupplyGrantAuthorized, which routes through runSupplyTx and writes
// withdrawal_authorizations + withdrawal_authorization_scopes +
// withdrawal_grant_audit in the same transaction (T012). Two deterministic kill
// points:
//
//  1. pre-commit (TestCarrierKillScopedSupplyPreCommit): the parent holds
//     withdrawal_grant_audit ACCESS EXCLUSIVE, so the child's tx runs INSERT
//     grant + INSERT scope, then parks on the in-tx audit INSERT. Both written
//     rows are uncommitted and durable-invisible; SIGKILL proves the aborted tx
//     left ZERO partial rows across all three tables, and the same operation id
//     then converges to exactly one grant + scope + audit triple.
//  2. post-commit (TestCarrierKillScopedSupplyPostCommit): the child writes a
//     committed-marker file AFTER SupplyGrantAuthorized returns (COMMIT durable)
//     and the parent SIGKILLs it before any result is delivered — the result is
//     lost, the write is not. Convergence: still exactly one triple, same-op
//     retry adds nothing, no duplicate grant, no duplicate audit.
//
// Both cases then run a subsequent independent scoped supply on the same
// database to prove locks/connections recover.
//
// Test-only: the child is this test binary, fault injection is the parent's
// table lock and SIGKILL, and no production fault-injection flag, handler or
// endpoint exists. The file reuses the child re-exec harness helpers
// (killSetup, killWaitForFile, killChild) from recovery_kill_integration_test.go
// without editing it; every symbol here is pbkill-prefixed.
package withdrawal_test

import (
	"context"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/withdrawal"
)

const (
	pbKillChildEnv     = "TXHARBOR_CARRIER_KILL_CHILD"
	pbKillDSNEnv       = "TXHARBOR_CARRIER_KILL_DSN"
	pbKillOpEnv        = "TXHARBOR_CARRIER_KILL_OP"
	pbKillAuthEnv      = "TXHARBOR_CARRIER_KILL_AUTH"
	pbKillCallerEnv    = "TXHARBOR_CARRIER_KILL_CALLER"
	pbKillKeyEnv       = "TXHARBOR_CARRIER_KILL_KEY"
	pbKillCommittedEnv = "TXHARBOR_CARRIER_KILL_COMMITTED"

	// pbKillAppName tags the child's backend so the parent can wait for the
	// SIGKILLed connection to be reaped before it reuses the same database.
	pbKillAppName = "txharbor-carrier-kill-child"

	pbKillChainID    int64 = 31337
	pbKillAsset            = "0x1111111111111111111111111111111111111111"
	pbKillRecipient        = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	pbKillSender           = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	pbKillAttestedBy       = "principal-issuer"
	pbKillAmount           = "100"
	pbKillOperator         = "pb-kill-operator"
	pbKillReason           = "pb-kill scoped supply"
)

// pbKillScopedOp builds one legal scoped supply op-input: the eight Table 6
// fields plus the full PB scope payload, so runSupplyTx takes the three-table
// branch (grant + scope + audit). It is the exact op the child and the parent
// retry present, so convergence is a same-operation comparison.
func pbKillScopedOp(operationID, authorizationID string, callerID int64) withdrawal.OpInput {
	return withdrawal.OpInput{
		OperationID:          operationID,
		Action:               "supply",
		AuthorizationID:      authorizationID,
		CallerID:             callerID,
		ChainID:              pbKillChainID,
		Asset:                pbKillAsset,
		Recipient:            pbKillRecipient,
		Amount:               pbKillAmount,
		IntentID:             "intent-" + authorizationID,
		RequestID:            "request-" + authorizationID,
		Sender:               pbKillSender,
		FeeMaxTotal:          21000,
		FeeMaxPerGas:         2,
		FeeMaxPriority:       1,
		AllowsFeeReplacement: true,
		AttestedBy:           pbKillAttestedBy,
	}
}

// pbKillAuthority loads a real issuer allowlist permitting exactly callerID and
// pairs it with the caller's presented key: the production in-tx authority
// re-check (T015) the carrier runs before the three-table write.
func pbKillAuthority(t *testing.T, callerID int64, presentedKey string) withdrawal.SupplyAuthority {
	t.Helper()
	allow, err := withdrawal.LoadIssuerAllowlist(func(name string) (string, bool) {
		if name == withdrawal.EnvIssuerCallers {
			return strconv.FormatInt(callerID, 10), true
		}
		return "", false
	})
	if err != nil {
		t.Fatalf("LoadIssuerAllowlist: %v", err)
	}
	return withdrawal.SupplyAuthority{PresentedKey: presentedKey, Issuers: allow}
}

// pbKillSeedKey creates the stable caller plus one active credential and
// returns the plaintext key (created exactly once, displayed once).
func pbKillSeedKey(t *testing.T, ctx context.Context, pool *pgxpool.Pool, callerID int64) string {
	t.Helper()
	key, _, err := withdrawal.IssueKey(ctx, pool, callerID, "carrier-kill")
	if err != nil {
		t.Fatalf("IssueKey(%d): %v", callerID, err)
	}
	return key
}

// pbKillCount runs one count query with a single argument.
func pbKillCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sql, arg string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, sql, arg).Scan(&n); err != nil {
		t.Fatalf("count %q for %q: %v", sql, arg, err)
	}
	return n
}

// pbKillWantGrantScope asserts only the grant + scope census. It is the safe
// pre-commit check while the parent still holds withdrawal_grant_audit ACCESS
// EXCLUSIVE: counting the audit table there would block on the parent's own
// lock (the same trap recovery_kill_integration_test.go documents).
func pbKillWantGrantScope(t *testing.T, ctx context.Context, pool *pgxpool.Pool, authID string, want int) {
	t.Helper()
	if n := pbKillCount(t, ctx, pool,
		`SELECT count(*) FROM withdrawal_authorizations WHERE authorization_id = $1`, authID); n != want {
		t.Fatalf("grant rows for %q = %d, want %d", authID, n, want)
	}
	if n := pbKillCount(t, ctx, pool,
		`SELECT count(*) FROM withdrawal_authorization_scopes WHERE authorization_id = $1`, authID); n != want {
		t.Fatalf("scope rows for %q = %d, want %d", authID, n, want)
	}
}

// pbKillWantTriple asserts the scoped supply census: grant, scope and audit all
// carry exactly want rows for the operation. Every V-PB7 claim is carried by
// these exact counts — a partial write or a duplicate is a non-zero delta.
func pbKillWantTriple(t *testing.T, ctx context.Context, pool *pgxpool.Pool, authID, opID string, want int) {
	t.Helper()
	if n := pbKillCount(t, ctx, pool,
		`SELECT count(*) FROM withdrawal_authorizations WHERE authorization_id = $1`, authID); n != want {
		t.Fatalf("grant rows for %q = %d, want %d", authID, n, want)
	}
	if n := pbKillCount(t, ctx, pool,
		`SELECT count(*) FROM withdrawal_authorization_scopes WHERE authorization_id = $1`, authID); n != want {
		t.Fatalf("scope rows for %q = %d, want %d", authID, n, want)
	}
	if n := pbKillCount(t, ctx, pool,
		`SELECT count(*) FROM withdrawal_grant_audit WHERE operation_id = $1`, opID); n != want {
		t.Fatalf("audit rows for operation %q = %d, want %d", opID, n, want)
	}
}

// pbKillWaitForBlockedAudit waits until a backend is parked on the in-tx
// withdrawal_grant_audit INSERT, waiting on a lock. That is the deterministic
// pre-commit point: the grant and scope INSERTs already ran in the SAME
// uncommitted transaction, and no COMMIT has reached the server.
func pbKillWaitForBlockedAudit(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		var n int
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FROM pg_stat_activity
			WHERE wait_event_type = 'Lock'
			  AND query ILIKE '%INSERT INTO withdrawal_grant_audit%'`).Scan(&n); err != nil {
			t.Fatalf("poll pg_stat_activity: %v", err)
		}
		if n > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("child never parked in the in-tx audit INSERT (pre-commit kill precondition unmet)")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// pbKillWaitBackendGone waits until the SIGKILLed child's backend has left the
// server. Without it a retry could block on the dead backend's still-open
// transaction; with it recovery is deterministic, not timing-lucky.
func pbKillWaitBackendGone(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		var n int
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM pg_stat_activity WHERE application_name = $1`, pbKillAppName).Scan(&n); err != nil {
			t.Fatalf("poll child backend: %v", err)
		}
		if n == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("child backend %q still present after SIGKILL; recovery would be blocked", pbKillAppName)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// pbKillAssertSubsequentSupply proves locks/connections recovered: an
// independent scoped supply of a NEW grant on the SAME database succeeds and
// writes its own exactly-one triple.
func pbKillAssertSubsequentSupply(t *testing.T, ctx context.Context, pool *pgxpool.Pool, callerID int64, key, authID, opID string) {
	t.Helper()
	op := pbKillScopedOp(opID, authID, callerID)
	out, err := withdrawal.SupplyGrantAuthorized(ctx, pool, op, pbKillAuthority(t, callerID, key), "post-recovery", "subsequent supply")
	if err != nil {
		t.Fatalf("subsequent scoped supply after kill: %v", err)
	}
	if out.Action != "supplied" {
		t.Fatalf("subsequent scoped supply = %s, want supplied", out.Action)
	}
	pbKillWantTriple(t, ctx, pool, authID, opID, 1)
}

// pbKillOpenChildPool opens the child's pool with a recognizable
// application_name so the parent can deterministically observe its death.
func pbKillOpenChildPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	cfg.ConnConfig.RuntimeParams["application_name"] = pbKillAppName
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

// TestCarrierKillChildScopedSupply is the re-exec'd child. It is a no-op during
// a normal test run; the parent starts it with -test.run and pbKillChildEnv=1.
// It runs the REAL production carrier entry (SupplyGrantAuthorized → runSupplyTx
// scoped three-table write) over a fresh pool, then blocks until SIGKILL.
//
// For the post-commit kill it writes a committed-marker AFTER the carrier
// returns, which proves COMMIT was durable before the kill. For the pre-commit
// kill the marker env is unset and the call never returns: the tx parks on the
// audit INSERT until the parent kills it.
func TestCarrierKillChildScopedSupply(t *testing.T) {
	if os.Getenv(pbKillChildEnv) != "1" {
		return
	}
	ctx := context.Background()
	pool, err := pbKillOpenChildPool(ctx, os.Getenv(pbKillDSNEnv))
	if err != nil {
		t.Fatalf("child open pool: %v", err)
	}
	defer pool.Close()

	callerID, err := strconv.ParseInt(os.Getenv(pbKillCallerEnv), 10, 64)
	if err != nil {
		t.Fatalf("child caller id: %v", err)
	}
	op := pbKillScopedOp(os.Getenv(pbKillOpEnv), os.Getenv(pbKillAuthEnv), callerID)
	auth := pbKillAuthority(t, callerID, os.Getenv(pbKillKeyEnv))

	out, err := withdrawal.SupplyGrantAuthorized(ctx, pool, op, auth, pbKillOperator, pbKillReason)

	// Post-commit barrier. Reached ONLY after runSupplyTx's COMMIT returned.
	if path := os.Getenv(pbKillCommittedEnv); path != "" {
		verdict := "error"
		if err != nil {
			verdict = "error:" + err.Error()
		} else if out != nil {
			verdict = "committed:" + out.Action
		}
		if werr := os.WriteFile(path, []byte(verdict), 0o600); werr != nil {
			t.Fatalf("child committed marker: %v", werr)
		}
	}

	// Block until the parent SIGKILLs this process: a blocking accept syscall,
	// so the Go runtime never declares a spurious deadlock.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("child listen: %v", err)
	}
	_ = (&http.Server{ReadHeaderTimeout: 5 * time.Second}).Serve(ln)
}

// pbKillStartChild re-execs this test binary as the scoped-supply child with
// the shared op-input and (for the post-commit point) the committed-marker
// path. It mirrors killStartChild's SIGKILL-at-cleanup discipline.
func pbKillStartChild(t *testing.T, dsn, opID, authID string, callerID int64, key, committedPath string) *killChild {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	logPath := filepath.Join(t.TempDir(), "child.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("create child log: %v", err)
	}
	cmd := exec.Command(exe, "-test.run=^TestCarrierKillChildScopedSupply$")
	env := append(os.Environ(),
		pbKillChildEnv+"=1",
		pbKillDSNEnv+"="+dsn,
		pbKillOpEnv+"="+opID,
		pbKillAuthEnv+"="+authID,
		pbKillCallerEnv+"="+strconv.FormatInt(callerID, 10),
		pbKillKeyEnv+"="+key,
	)
	if committedPath != "" {
		env = append(env, pbKillCommittedEnv+"="+committedPath)
	}
	cmd.Env = env
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		t.Fatalf("start child: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Signal(syscall.SIGKILL)
		_ = cmd.Wait()
		_ = logFile.Close()
	})
	return &killChild{cmd: cmd, logPath: logPath}
}

// TestCarrierKillScopedSupplyPreCommit owns V-PB7 kill point 1. The child's
// scoped supply tx is parked on the in-tx audit INSERT with the grant and scope
// rows already written but uncommitted; SIGKILL proves the aborted tx left ZERO
// partial rows and that the same operation id then converges to exactly one
// grant + scope + audit triple.
func TestCarrierKillScopedSupplyPreCommit(t *testing.T) {
	ctx, dsn, pool := killSetup(t)
	const (
		callerID int64 = 8501
		authID         = "auth-carrier-kill-precommit"
		opID           = "71000000000000000000000000000001"
	)
	key := pbKillSeedKey(t, ctx, pool, callerID)

	// The parent holds withdrawal_grant_audit ACCESS EXCLUSIVE: the child's tx
	// runs INSERT grant + INSERT scope and then parks on the in-tx audit INSERT.
	lockConn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect lock holder: %v", err)
	}
	defer func() { _ = lockConn.Close(ctx) }()
	lockTx, err := lockConn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin lock tx: %v", err)
	}
	defer func() { _ = lockTx.Rollback(ctx) }()
	if _, err := lockTx.Exec(ctx, `LOCK TABLE withdrawal_grant_audit IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatalf("lock audit table: %v", err)
	}

	child := pbKillStartChild(t, dsn, opID, authID, callerID, key, "")
	pbKillWaitForBlockedAudit(t, ctx, pool)

	// Pre-commit proof: the parked tx's written grant and scope rows are
	// invisible to the parent (the audit count would block on the parent's own
	// table lock), so no COMMIT has landed and nothing is durable.
	pbKillWantGrantScope(t, ctx, pool, authID, 0)

	child.sigkill(t)

	// Release the audit lock (its job, parking the child pre-commit, is done)
	// and wait for the dead backend to be reaped so the retry cannot block.
	if err := lockTx.Rollback(ctx); err != nil {
		t.Fatalf("release audit lock: %v", err)
	}
	_ = lockConn.Close(ctx)
	pbKillWaitBackendGone(t, ctx, pool)

	// Zero durable rows after the kill: the aborted tx left no partial grant,
	// no partial scope, no orphan audit.
	pbKillWantTriple(t, ctx, pool, authID, opID, 0)

	// Retry with the SAME operation id converges to exactly one triple.
	out, err := withdrawal.SupplyGrantAuthorized(ctx, pool, pbKillScopedOp(opID, authID, callerID),
		pbKillAuthority(t, callerID, key), "retry-precommit", "same-op retry")
	if err != nil {
		t.Fatalf("same-op retry after pre-commit kill: %v", err)
	}
	if out.Action != "supplied" {
		t.Fatalf("same-op retry = %s, want supplied", out.Action)
	}
	pbKillWantTriple(t, ctx, pool, authID, opID, 1)

	pbKillAssertSubsequentSupply(t, ctx, pool, callerID, key,
		"auth-carrier-kill-precommit-after", "71000000000000000000000000000002")
}

// TestCarrierKillScopedSupplyPostCommit owns V-PB7 kill point 2. The child
// commits the scoped three-table write, signals that with a committed-marker
// file, and is SIGKILLed before it can deliver any result: the write survived
// and the result was lost. The retry with the SAME operation id must converge
// on exactly one triple — no duplicate grant, no duplicate audit.
func TestCarrierKillScopedSupplyPostCommit(t *testing.T) {
	ctx, dsn, pool := killSetup(t)
	const (
		callerID int64 = 8502
		authID         = "auth-carrier-kill-postcommit"
		opID           = "71000000000000000000000000000003"
	)
	key := pbKillSeedKey(t, ctx, pool, callerID)

	committedPath := filepath.Join(t.TempDir(), "committed")
	child := pbKillStartChild(t, dsn, opID, authID, callerID, key, committedPath)

	// Deterministic post-commit point: the marker is written only after the
	// carrier returned, so COMMIT is durable before the parent acts.
	if verdict := killWaitForFile(t, committedPath, child.logPath); verdict != "committed:supplied" {
		t.Fatalf("child pre-kill verdict = %q, want committed:supplied", verdict)
	}

	child.sigkill(t)
	pbKillWaitBackendGone(t, ctx, pool)

	// The committed write survived the kill even though the result was lost.
	pbKillWantTriple(t, ctx, pool, authID, opID, 1)

	// Same-op retry: the recorded attempt converges; nothing is duplicated.
	out, err := withdrawal.SupplyGrantAuthorized(ctx, pool, pbKillScopedOp(opID, authID, callerID),
		pbKillAuthority(t, callerID, key), "retry-postcommit", "same-op retry")
	if err != nil {
		t.Fatalf("same-op retry after post-commit kill: %v", err)
	}
	if out.Action != "supplied" {
		t.Fatalf("same-op retry = %s, want supplied (the recorded attempt)", out.Action)
	}
	pbKillWantTriple(t, ctx, pool, authID, opID, 1)

	pbKillAssertSubsequentSupply(t, ctx, pool, callerID, key,
		"auth-carrier-kill-postcommit-after", "71000000000000000000000000000004")
}
