//go:build integration

// recovery_kill_integration_test.go owns the GENUINE process-termination
// evidence for 007 T024's V6 claim ("kill -9 between COMMIT and response →
// retry same key → 200 original; restart → retry → 200 original").
//
// recovery_integration_test.go (package withdrawal) proves the grant/O recovery
// rules with a cancelled caller context and a closed pool — that models a lost
// CALLER, not a dead PROCESS, and it is labelled that way. THIS file supplies
// the missing process death: it re-executes the test binary (os.Executable +
// env flag) as a real child process running the REAL app.WithdrawalHandler over
// a real pgx pool to the test's PostgreSQL container, then delivers a real
// SIGKILL via syscall.SIGKILL. Two deterministic kill points:
//
//  1. pre-commit (TestWithdrawalRecoverySigkillChildPreCommit): the parent
//     holds withdrawal_request_audit ACCESS EXCLUSIVE, so the child's receipt
//     transaction parks on the in-tx audit INSERT with an uncommitted request
//     row. Commit state is KNOWN pre-commit — the row is invisible to the
//     parent and no COMMIT ever reached the server. SIGKILL, restart on the
//     same database, same-key replay → 201 from zero durable rows.
//  2. unknown commit (TestWithdrawalRecoverySigkillChildUnknownCommit): the
//     child writes a dispatch marker as it ENTERS ServeHTTP (before the core
//     runs) and the parent SIGKILLs immediately without reading a response.
//     Whether the receipt committed is GENUINELY UNKNOWN at that instant; the
//     invariant asserted is convergence: exactly one request row afterwards,
//     and a replay that reports 200 (same request_id, committed) or 201 (new
//     request_id, not committed) per whichever state survived.
//
// Test-only: the child is this test binary, the middleware is test-only, and no
// production fault-injection flag, endpoint or handler exists.
package withdrawal_test

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/xtianxx/txharbor/internal/app"
	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/withdrawal"
)

const (
	killChainID   int64 = 31337
	killAsset           = "0x1111111111111111111111111111111111111111"
	killRecipient       = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	killAmount          = "100"

	killChildEnv    = "TXHARBOR_RECOVERY_KILL_CHILD"
	killDSNEnv      = "TXHARBOR_RECOVERY_KILL_DSN"
	killReadyEnv    = "TXHARBOR_RECOVERY_KILL_READY"
	killDispatchEnv = "TXHARBOR_RECOVERY_KILL_DISPATCH"
)

// killSetup boots a migrated scratch PostgreSQL and returns its DSN plus a
// parent pool for seeding and durable-row assertions.
func killSetup(t *testing.T) (context.Context, string, *pgxpool.Pool) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()
	ctr, err := postgres.Run(ctx, "postgres:18.6-trixie",
		postgres.WithDatabase("txharbor"),
		postgres.WithUsername("txharbor"),
		postgres.WithPassword("txharbor"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2)),
	)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })
	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres connection string: %v", err)
	}
	if err := db.MigrateUp(ctx, db.MigrateOptions{DSN: dsn, LockTimeout: 5 * time.Second, ConnectTimeout: 5 * time.Second}, io.Discard); err != nil {
		t.Fatalf("migrate up: %v", err)
	}
	pool, err := db.OpenPool(ctx, dsn, 5*time.Second)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return ctx, dsn, pool
}

// killSeed creates the caller + one active grant and returns the caller's
// plaintext API key.
func killSeed(t *testing.T, ctx context.Context, pool *pgxpool.Pool, callerID int64, authID string) string {
	t.Helper()
	key, _, err := withdrawal.IssueKey(ctx, pool, callerID, "kill-test")
	if err != nil {
		t.Fatalf("IssueKey(%d): %v", callerID, err)
	}
	out, err := withdrawal.SupplyGrant(ctx, pool, withdrawal.OpInput{
		OperationID:     "seed-" + authID,
		Action:          "supply",
		AuthorizationID: authID,
		CallerID:        callerID,
		ChainID:         killChainID,
		Asset:           killAsset,
		Recipient:       killRecipient,
		Amount:          killAmount,
	}, "kill-test", "seed")
	if err != nil {
		t.Fatalf("SupplyGrant(%s): %v", authID, err)
	}
	if out.Action != "supplied" {
		t.Fatalf("seed outcome = %s, want supplied", out.Action)
	}
	return key
}

// killSeedPolicy appends the 004 policy version the handler resolves per
// request for FR-05 (newest deposit_config_history row); without it every POST
// is a 503 before the core runs.
func killSeedPolicy(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
INSERT INTO deposit_config_history
    (chain_id, version_seq, config_hash, start_block, assets, watches, replay_from, request_id)
VALUES ($1, 1, $2, 0, $3, $4, 0, $5)`,
		killChainID, strings.Repeat("a", 64), killAsset+":0", killRecipient+":0", "req-kill-1"); err != nil {
		t.Fatalf("seed deposit config history: %v", err)
	}
}

// killCreateBody renders the canonical POST /withdrawals body for a key/auth.
func killCreateBody(t *testing.T, key, authID string) string {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"idempotency_key":  key,
		"chain_id":         killChainID,
		"asset":            killAsset,
		"recipient":        killRecipient,
		"amount":           killAmount,
		"authorization_id": authID,
	})
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	return string(raw)
}

// killPost fires one authenticated POST and returns status + body.
func killPost(t *testing.T, addr, token, body string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, "http://"+addr+"/withdrawals", strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("POST /withdrawals: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, raw
}

// killPostNoResponse fires a POST from a caller that will never read a response
// (the child is killed mid-request). Its error is the point: a killed child
// cannot deliver a success.
func killPostNoResponse(addr, token, body string) error {
	req, err := http.NewRequest(http.MethodPost, "http://"+addr+"/withdrawals", strings.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		return err
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.Body.Close()
}

// TestRecoveryKillChildProcess is the re-exec'd child. It is a no-op during a
// normal test run; the parent starts it with -test.run and killChildEnv=1.
// start:
//
//	serve the REAL app.WithdrawalHandler over a fresh pgx pool on an
//	ephemeral port, publish the address to the ready file, and write the
//	dispatch file as the first POST enters ServeHTTP (before the core runs).
//
// It blocks in ServeHTTP until the parent SIGKILLs the process.
func TestRecoveryKillChildProcess(t *testing.T) {
	if os.Getenv(killChildEnv) != "1" {
		return
	}
	ctx := context.Background()
	pool, err := db.OpenPool(ctx, os.Getenv(killDSNEnv), 5*time.Second)
	if err != nil {
		t.Fatalf("child open pool: %v", err)
	}
	defer pool.Close()

	handler := &app.WithdrawalHandler{
		Pool:    pool,
		ChainID: killChainID,
	}
	mux := http.NewServeMux()
	mux.Handle("/withdrawals", killDispatchSignal(os.Getenv(killDispatchEnv), handler))
	mux.Handle("/withdrawals/", handler)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("child listen: %v", err)
	}
	if err := os.WriteFile(os.Getenv(killReadyEnv), []byte(ln.Addr().String()), 0o600); err != nil {
		t.Fatalf("child ready file: %v", err)
	}
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	_ = srv.Serve(ln) // the parent SIGKILLs this process
}

// killDispatchSignal writes signalPath once, as the first POST enters
// ServeHTTP and BEFORE the real handler runs. That is the parent's
// deterministic "server accepted the request" point for the unknown-commit
// kill; it reveals nothing about the commit, which is decided later inside the
// real handler.
func killDispatchSignal(signalPath string, next http.Handler) http.Handler {
	var signalled atomic.Bool
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && signalPath != "" && signalled.CompareAndSwap(false, true) {
			_ = os.WriteFile(signalPath, []byte("dispatched"), 0o600)
		}
		next.ServeHTTP(w, r)
	})
}

// killChild is one running child server process.
type killChild struct {
	cmd      *exec.Cmd
	addr     string
	dispatch string
	logPath  string
}

// killStartChild re-execs this test binary as the child server and returns it
// once it has published its listen address. The child is SIGKILLed at cleanup
// even when the test fails early.
func killStartChild(t *testing.T, dsn string) *killChild {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	dir := t.TempDir()
	ready := filepath.Join(dir, "ready")
	dispatch := filepath.Join(dir, "dispatch")
	logPath := filepath.Join(dir, "child.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("create child log: %v", err)
	}
	cmd := exec.Command(exe, "-test.run=^TestRecoveryKillChildProcess$")
	cmd.Env = append(os.Environ(),
		killChildEnv+"=1",
		killDSNEnv+"="+dsn,
		killReadyEnv+"="+ready,
		killDispatchEnv+"="+dispatch,
	)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		t.Fatalf("start child: %v", err)
	}
	c := &killChild{cmd: cmd, dispatch: dispatch, logPath: logPath}
	t.Cleanup(func() {
		_ = cmd.Process.Signal(syscall.SIGKILL)
		_ = cmd.Wait()
		_ = logFile.Close()
	})
	c.addr = killWaitForFile(t, ready, logPath)
	return c
}

// sigkill delivers a real SIGKILL and reaps the child.
func (c *killChild) sigkill(t *testing.T) {
	t.Helper()
	if err := c.cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatalf("SIGKILL child: %v", err)
	}
	if err := c.cmd.Wait(); err == nil {
		t.Fatal("killed child exited 0; expected termination by signal")
	}
}

// killWaitForFile polls until path exists and is non-empty, dumping the child
// log on timeout so startup failures are diagnosable.
func killWaitForFile(t *testing.T, path, logPath string) string {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		if raw, err := os.ReadFile(path); err == nil && strings.TrimSpace(string(raw)) != "" {
			return strings.TrimSpace(string(raw))
		}
		if time.Now().After(deadline) {
			log := ""
			if raw, err := os.ReadFile(logPath); err == nil {
				log = string(raw)
			}
			t.Fatalf("timed out waiting for %s; child log:\n%s", path, log)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// killWaitForBlockedReceipt waits until the child's receipt transaction is
// parked on the in-tx audit INSERT, waiting on a lock. That is the deterministic
// pre-commit kill point: the request INSERT already ran in the same
// uncommitted transaction, so the row is durable-invisible and no COMMIT has
// reached the server.
func killWaitForBlockedReceipt(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		var n int
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FROM pg_stat_activity
			WHERE wait_event_type = 'Lock'
			  AND query ILIKE '%INSERT INTO withdrawal_request_audit%'`).Scan(&n); err != nil {
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

// The durable-row assertions. Every recovery claim is carried by exact counts.

func killRequestRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, callerID int64, key string) (int, string) {
	t.Helper()
	var n int
	var id string
	if err := pool.QueryRow(ctx, `
		SELECT count(*), coalesce(max(request_id), '')
		FROM withdrawal_requests
		WHERE caller_id = $1 AND idempotency_key = $2`, callerID, key).Scan(&n, &id); err != nil {
		t.Fatalf("read withdrawal_requests for key %q: %v", key, err)
	}
	return n, id
}

func killCreatedAuditCountFor(t *testing.T, ctx context.Context, pool *pgxpool.Pool, requestID string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM withdrawal_request_audit
		WHERE action = 'created' AND request_id = $1`, requestID).Scan(&n); err != nil {
		t.Fatalf("count created audit rows for %q: %v", requestID, err)
	}
	return n
}

func killCreatedAuditCountByCaller(t *testing.T, ctx context.Context, pool *pgxpool.Pool, callerID int64) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM withdrawal_request_audit
		WHERE action = 'created' AND caller_id = $1`, callerID).Scan(&n); err != nil {
		t.Fatalf("count created audit rows for caller %d: %v", callerID, err)
	}
	return n
}

func killGrantCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, authID string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM withdrawal_authorizations WHERE authorization_id = $1`, authID).Scan(&n); err != nil {
		t.Fatalf("count grant rows for %q: %v", authID, err)
	}
	return n
}

func killGrantStateAmount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, authID string) (string, string) {
	t.Helper()
	var state, amount string
	if err := pool.QueryRow(ctx,
		`SELECT state, amount::text FROM withdrawal_authorizations WHERE authorization_id = $1`, authID).
		Scan(&state, &amount); err != nil {
		t.Fatalf("read grant row for %q: %v", authID, err)
	}
	return state, amount
}

// TestWithdrawalRecoverySigkillChildPreCommit owns T024 kill point 1. The child
// is SIGKILLed while its receipt transaction is blocked pre-commit; the restart
// proves the aborted transaction left ZERO durable rows and the same key then
// creates exactly one request.
func TestWithdrawalRecoverySigkillChildPreCommit(t *testing.T) {
	ctx, dsn, pool := killSetup(t)
	const (
		callerID int64 = 8301
		idemKey        = "idem-sigkill-precommit"
		authID         = "auth-sigkill-precommit"
	)
	token := killSeed(t, ctx, pool, callerID, authID)
	killSeedPolicy(t, ctx, pool)

	// The parent holds withdrawal_request_audit ACCESS EXCLUSIVE in an open tx:
	// the child's receipt tx will reach the in-tx audit INSERT and park there.
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
	if _, err := lockTx.Exec(ctx, `LOCK TABLE withdrawal_request_audit IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatalf("lock audit table: %v", err)
	}

	child := killStartChild(t, dsn)
	reqErr := make(chan error, 1)
	go func() { reqErr <- killPostNoResponse(child.addr, token, killCreateBody(t, idemKey, authID)) }()

	killWaitForBlockedReceipt(t, ctx, pool)

	// Pre-commit proof: the parked tx's request row is invisible to the parent,
	// so no COMMIT has landed.
	if n, _ := killRequestRow(t, ctx, pool, callerID, idemKey); n != 0 {
		t.Fatalf("request rows while the child is parked = %d, want 0 (uncommitted tx)", n)
	}

	child.sigkill(t)
	if err := <-reqErr; err == nil {
		t.Fatal("the SIGKILLed child still delivered a response; the kill did not land mid-request")
	}

	// Release the audit lock before reading the audit table: our own ACCESS
	// EXCLUSIVE lock would block the count. The lock's job (parking the child
	// pre-commit) is done once the child is dead.
	if err := lockTx.Rollback(ctx); err != nil {
		t.Fatalf("release audit lock: %v", err)
	}
	_ = lockConn.Close(ctx)

	// Zero durable rows between kill and replay: no partial request, no receipt
	// audit, and the seeded grant untouched.
	if n, _ := killRequestRow(t, ctx, pool, callerID, idemKey); n != 0 {
		t.Fatalf("request rows after kill = %d, want 0 (aborted tx left no partial row)", n)
	}
	if n := killCreatedAuditCountByCaller(t, ctx, pool, callerID); n != 0 {
		t.Fatalf("created audit rows after kill = %d, want 0", n)
	}
	if n := killGrantCount(t, ctx, pool, authID); n != 1 {
		t.Fatalf("grant rows after kill = %d, want 1 (seed only)", n)
	}
	if state, amount := killGrantStateAmount(t, ctx, pool, authID); state != "active" || amount != killAmount {
		t.Fatalf("grant after kill = (%s, %s), want (active, %s)", state, amount, killAmount)
	}

	// Restart on the SAME database and replay the SAME key.
	restarted := killStartChild(t, dsn)
	status, raw := killPost(t, restarted.addr, token, killCreateBody(t, idemKey, authID))
	if status != http.StatusCreated {
		t.Fatalf("same-key replay after restart = %d (%s), want 201 from zero durable rows", status, raw)
	}
	var created struct {
		RequestID string `json:"request_id"`
	}
	if err := json.Unmarshal(raw, &created); err != nil {
		t.Fatalf("decode create body %s: %v", raw, err)
	}

	rows, storedID := killRequestRow(t, ctx, pool, callerID, idemKey)
	if rows != 1 {
		t.Fatalf("request rows after replay = %d, want exactly 1", rows)
	}
	if storedID != created.RequestID {
		t.Fatalf("stored request_id %q != returned %q", storedID, created.RequestID)
	}
	if n := killCreatedAuditCountFor(t, ctx, pool, storedID); n != 1 {
		t.Fatalf("created audit rows for %q = %d, want exactly 1", storedID, n)
	}
}

// TestWithdrawalRecoverySigkillChildUnknownCommit owns T024 kill point 2. The
// child is SIGKILLed immediately after it accepts the request, with no response
// read: whether the receipt committed is genuinely unknown at kill time. The
// restart then proves the required convergence — never a duplicate row — and
// reports the outcome that actually survived.
func TestWithdrawalRecoverySigkillChildUnknownCommit(t *testing.T) {
	ctx, dsn, pool := killSetup(t)
	const (
		callerID int64 = 8302
		idemKey        = "idem-sigkill-unknown"
		authID         = "auth-sigkill-unknown"
	)
	token := killSeed(t, ctx, pool, callerID, authID)
	killSeedPolicy(t, ctx, pool)

	child := killStartChild(t, dsn)
	reqErr := make(chan error, 1)
	go func() { reqErr <- killPostNoResponse(child.addr, token, killCreateBody(t, idemKey, authID)) }()

	// Deterministic dispatch: the child wrote the dispatch marker as it entered
	// ServeHTTP, before the core ran. SIGKILL now — commit state is UNKNOWN.
	killWaitForFile(t, child.dispatch, child.logPath)
	child.sigkill(t)
	<-reqErr

	// Whatever survived the kill is durable state we did NOT observe as a
	// response. It can only be zero rows (rolled back) or one row (committed).
	beforeRows, _ := killRequestRow(t, ctx, pool, callerID, idemKey)
	if beforeRows > 1 {
		t.Fatalf("request rows before replay = %d, want <= 1", beforeRows)
	}

	restarted := killStartChild(t, dsn)
	status, raw := killPost(t, restarted.addr, token, killCreateBody(t, idemKey, authID))
	if status != http.StatusOK && status != http.StatusCreated {
		t.Fatalf("replay after unknown-commit kill = %d (%s), want 200 or 201", status, raw)
	}
	var resp struct {
		RequestID string `json:"request_id"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatalf("decode replay body %s: %v", raw, err)
	}

	rows, storedID := killRequestRow(t, ctx, pool, callerID, idemKey)
	if rows != 1 {
		t.Fatalf("request rows after replay = %d, want exactly 1 (never duplicated)", rows)
	}
	if storedID != resp.RequestID {
		t.Fatalf("stored request_id %q != returned %q", storedID, resp.RequestID)
	}
	// Convergence is bound to the commit state that actually survived: a
	// committed kill yields 200 with the SAME request_id; a pre-commit kill
	// yields 201 with the only row's id. This is the honest per-point state.
	if beforeRows == 1 && status != http.StatusOK {
		t.Fatalf("kill landed post-commit (row present) but replay = %d, want 200", status)
	}
	if beforeRows == 0 && status != http.StatusCreated {
		t.Fatalf("kill landed pre-commit (no row) but replay = %d, want 201", status)
	}
	if n := killCreatedAuditCountFor(t, ctx, pool, storedID); n != 1 {
		t.Fatalf("created audit rows for %q = %d, want exactly 1", storedID, n)
	}
	if n := killGrantCount(t, ctx, pool, authID); n != 1 {
		t.Fatalf("grant rows after replay = %d, want 1 (seed only)", n)
	}
	if state, amount := killGrantStateAmount(t, ctx, pool, authID); state != "active" || amount != killAmount {
		t.Fatalf("grant after replay = (%s, %s), want (active, %s)", state, amount, killAmount)
	}
}
