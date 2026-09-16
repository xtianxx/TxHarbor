//go:build integration

// restart_integration_test.go owns spec task T021 for 008-nonce-manager
// (V3, FR-06/FR-13, SC-03): crash/restart reuse over a real PostgreSQL proven
// with a REAL kill -9 of a REAL OS process.
//
// T019 already covered pooled-handle teardown (close the pool, reopen); that is
// NOT this test. Here the integration binary re-executes itself as a child
// process that performs exactly one admission against the scratch database and
// then blocks. The parent waits for the child's commit signal, then sends
// SIGKILL (kill -9). The child's memory is gone, so the only possible basis for
// the restart retry is the durable rows:
//
//   - scenario "committed": kill -9 after the admission commit, before any
//     downstream effect;
//   - scenario "effect_started": kill -9 after the child signalled that it
//     began the post-commit external step with an unknown result, before it
//     reported the outcome.
//
// In both scenarios, after restart:
//
//   - while the rebuild gate is still closed, the retry refuses
//     rebuild_incomplete before any RPC/DB work;
//   - after VerifyRebuild opens the gate, the retry replays the original
//     binding_id/nonce and writes zero new rows — never a second binding.
//
// The child is the same test binary invoked with -test.run, so it imports
// nothing outside the module. The container/migration harness is reused from
// migration_integration_test.go; the JSON-RPC double answers the unexported
// block-result shape through JSON exactly as the real geth client would (the
// working pattern from allocate_replay_integration_test.go).
package nonce_test

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/nonce"
)

const (
	restartChainID = int64(90001)

	restartAuthCommitted = "wa-restart-committed" // scenario 1 authorization
	restartAuthEffect    = "wa-restart-effect"    // scenario 2 authorization
)

var restartSender = nonceAddr("77")

// Child-process handshake: the parent re-executes os.Args[0] with these
// variables set. When restartHelperEnv is absent the helper test is a no-op.
const (
	restartHelperEnv     = "TXHARBOR_RESTART_HELPER"
	restartModeEnv       = "TXHARBOR_RESTART_MODE"
	restartDSNEnv        = "TXHARBOR_RESTART_DSN"
	restartIntentEnv     = "TXHARBOR_RESTART_INTENT"
	restartAuthEnv       = "TXHARBOR_RESTART_AUTH"
	restartSenderEnv     = "TXHARBOR_RESTART_SENDER"
	restartChainEnv      = "TXHARBOR_RESTART_CHAIN"
	restartModeCommitted = "committed"
	restartModeEffect    = "effect_started"
)

// Child stdout signals (one line each, prefix-matched by the parent).
const (
	restartSignalAdmitted = "RESTART_ADMITTED"
	restartSignalEffect   = "RESTART_EFFECT_STARTED"
)

// restartRPC is the scripted JSON-RPC double: a frozen healthy chain view
// (latest == pending == 0, head present) so the first admission for a fresh
// scope derives candidate nonce 0 with no Anvil node. Values are JSON because
// the observer's block-result type is package-private. calls counts RPC
// invocations so the closed-gate refusal can be proven to do zero external work.
type restartRPC struct{ calls int }

func (r *restartRPC) CallContext(_ context.Context, result any, method string, _ ...any) error {
	r.calls++
	switch method {
	case "eth_getTransactionCount":
		return json.Unmarshal([]byte(`"0x0"`), result)
	case "eth_getBlockByNumber":
		head := `{"number":"0x5","hash":"0x` + strings.Repeat("ab", 32) + `"}`
		return json.Unmarshal([]byte(head), result)
	default:
		return fmt.Errorf("restartRPC: unexpected method %q", method)
	}
}

// restartObserver builds the pre-tx observer over the scripted double.
func restartObserver(rpc *restartRPC) *nonce.Observer {
	return nonce.NewObserver(rpc, nonce.ObserverConfig{
		RPCTimeout:   2 * time.Second,
		RetryInitial: time.Millisecond,
		RetryMax:     5 * time.Millisecond,
	})
}

// restartOpenPool opens one fresh pooled handle; Close is idempotent, so the
// cleanup registration stays safe when a handle is closed early.
func restartOpenPool(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := db.OpenPool(ctx, dsn, 5*time.Second)
	if err != nil {
		t.Fatalf("open pgx pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// restartSeed inserts the 007 caller/authorization rows and the 008 wallet
// registry row every scenario needs to pass admission.
func restartSeed(t *testing.T, sqlDB *sql.DB) {
	t.Helper()
	nonceMustExec(t, sqlDB, `INSERT INTO caller (caller_id, label) VALUES (1, 'restart-it')`)
	nonceMustExec(t, sqlDB, `INSERT INTO nonce_wallet_registry
		(chain_id, sender, state, registry_seq) VALUES ($1, $2, 'active', 1)`,
		restartChainID, restartSender)
	for _, auth := range []string{restartAuthCommitted, restartAuthEffect} {
		nonceMustExec(t, sqlDB, `INSERT INTO withdrawal_authorizations
			(authorization_id, caller_id, chain_id, asset, recipient, amount, state)
			VALUES ($1, 1, $2, $3, $4, 1, 'active')`,
			auth, restartChainID, nonceAddr("aa"), nonceAddr("bb"))
	}
}

// restartCensus is the whole-DB row census used to prove "zero new rows".
type restartCensus struct {
	bindings     int
	events       int
	observations int
	holds        int
	scopes       int
}

func restartCounts(t *testing.T, sqlDB *sql.DB) restartCensus {
	t.Helper()
	var c restartCensus
	if err := sqlDB.QueryRow(`SELECT
		(SELECT count(*) FROM nonce_bindings),
		(SELECT count(*) FROM nonce_binding_events),
		(SELECT count(*) FROM nonce_observations),
		(SELECT count(*) FROM nonce_scope_holds),
		(SELECT count(*) FROM nonce_scope_state)`).
		Scan(&c.bindings, &c.events, &c.observations, &c.holds, &c.scopes); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	return c
}

func restartIntentBindingCount(t *testing.T, sqlDB *sql.DB, intentID string) int {
	t.Helper()
	var n int
	if err := sqlDB.QueryRow(`SELECT count(*) FROM nonce_bindings WHERE intent_id = $1`, intentID).
		Scan(&n); err != nil {
		t.Fatalf("count bindings for %q: %v", intentID, err)
	}
	return n
}

// restartDurableBinding reads the one binding the crashed child committed.
func restartDurableBinding(t *testing.T, sqlDB *sql.DB, intentID string) (bindingID, nonceValue string) {
	t.Helper()
	if err := sqlDB.QueryRow(`SELECT binding_id, nonce::text FROM nonce_bindings WHERE intent_id = $1`,
		intentID).Scan(&bindingID, &nonceValue); err != nil {
		t.Fatalf("read durable binding for %q: %v", intentID, err)
	}
	return bindingID, nonceValue
}

// restartCrashChild spawns, signals, and SIGKILLs one helper process. It
// returns the binding identity the child durably committed (parsed from the
// child's stdout signal) so the parent can assert the retry replays exactly it.
func restartCrashChild(t *testing.T, dsn, mode, intentID, authID string) (bindingID, nonceValue string) {
	t.Helper()

	cmd := exec.Command(os.Args[0], "-test.run=^TestNonceRestartCrashHelper$")
	cmd.Env = append(os.Environ(),
		restartHelperEnv+"=1",
		restartModeEnv+"="+mode,
		restartDSNEnv+"="+dsn,
		restartIntentEnv+"="+intentID,
		restartAuthEnv+"="+authID,
		restartSenderEnv+"="+restartSender,
		restartChainEnv+"="+strconv.FormatInt(restartChainID, 10),
	)
	// The child's own failures go straight to the test log; stdout is the
	// handshake pipe (read below).
	cmd.Stderr = os.Stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("helper stdout pipe: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start crash helper: %v", err)
	}

	// Drain stdout lines so the child's signal is found regardless of any
	// incidental framework output.
	lines := make(chan string, 8)
	go func() {
		defer close(lines)
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			lines <- sc.Text()
		}
	}()

	wantPrefix := restartSignalAdmitted
	if mode == restartModeEffect {
		wantPrefix = restartSignalEffect
	}

	var signal string
	select {
	case line, ok := <-lines:
		if !ok {
			t.Fatalf("crash helper exited before signalling %s", wantPrefix)
		}
		if !strings.HasPrefix(line, wantPrefix) {
			t.Fatalf("crash helper first line = %q, want prefix %q", line, wantPrefix)
		}
		signal = line
	case <-time.After(60 * time.Second):
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatalf("timed out waiting for crash helper %s", wantPrefix)
	}

	// The child has committed the admission (scenario 1) or committed it and
	// begun the post-commit effect (scenario 2); kill -9 it before it can do
	// anything else. Nothing is reported back to us beyond the signal, so the
	// post-kill state is exactly "durable rows only".
	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatalf("kill -9 crash helper: %v", err)
	}
	werr := cmd.Wait()
	ws, ok := cmd.ProcessState.Sys().(syscall.WaitStatus)
	if !ok || !ws.Signaled() || ws.Signal() != syscall.SIGKILL {
		t.Fatalf("crash helper was not killed by SIGKILL (wait err %v, state %v)", werr, cmd.ProcessState)
	}

	fields := strings.Fields(signal)
	if len(fields) != 3 {
		t.Fatalf("crash helper signal = %q, want %q <binding_id> <nonce>", signal, wantPrefix)
	}
	return fields[1], fields[2]
}

// restartScenario is the shared V3 flow: crash a real process, then exercise
// the restart path against the same database and intent.
func restartScenario(t *testing.T, dsn string, sqlDB *sql.DB, mode, intentID, authID string) {
	t.Helper()
	ctx := context.Background()

	originalID, originalNonce := restartCrashChild(t, dsn, mode, intentID, authID)

	// The killed child's memory is gone; the durable rows are the only basis.
	afterCrash := restartCounts(t, sqlDB)
	durableID, durableNonce := restartDurableBinding(t, sqlDB, intentID)
	if durableID != originalID || durableNonce != originalNonce {
		t.Fatalf("durable binding %s/%s != child-reported %s/%s",
			durableID, durableNonce, originalID, originalNonce)
	}
	if n := restartIntentBindingCount(t, sqlDB, intentID); n != 1 {
		t.Fatalf("intent %q has %d durable bindings after kill -9, want exactly 1", intentID, n)
	}

	// --- restart: closed gate refuses before verification -----------------
	pool := restartOpenPool(t, dsn)
	rpc := &restartRPC{}
	closedGate := nonce.NewRebuildGate()
	closed := nonce.NewAllocator(pool, restartObserver(rpc), closedGate)

	req := nonce.AllocationRequest{
		IntentID:        intentID,
		ChainID:         restartChainID,
		Sender:          restartSender,
		AuthorizationID: authID,
	}
	binding, outcome, err := closed.Allocate(ctx, req)
	if binding != nil {
		t.Fatalf("closed gate returned a binding: %+v", binding)
	}
	if outcome != nonce.OutcomeRebuildIncomplete || !nonce.IsOutcome(err, nonce.OutcomeRebuildIncomplete) {
		t.Fatalf("closed-gate retry = (%q, %v), want rebuild_incomplete", outcome, err)
	}
	if rpc.calls != 0 {
		t.Fatalf("closed-gate retry performed %d RPC calls, want 0", rpc.calls)
	}
	if got := restartCounts(t, sqlDB); got != afterCrash {
		t.Fatalf("closed-gate retry changed rows: %+v, want %+v", got, afterCrash)
	}

	// --- verification opens the gate --------------------------------------
	if _, err := nonce.VerifyRebuild(ctx, pool, restartChainID); err != nil {
		t.Fatalf("VerifyRebuild after crash: %v", err)
	}
	closedGate.Open()
	if !closedGate.IsOpen() {
		t.Fatal("gate stayed closed after a successful rebuild verification")
	}

	// --- retry converges on the durable binding, zero double allocation ----
	open := nonce.NewAllocator(pool, restartObserver(&restartRPC{}), closedGate)
	replay, outcome, err := open.Allocate(ctx, req)
	if err != nil || outcome != nonce.OutcomeReplayed {
		t.Fatalf("post-restart retry = (%+v, %q, %v), want replayed", replay, outcome, err)
	}
	if replay == nil || replay.BindingID != originalID || replay.Nonce.String() != originalNonce {
		t.Fatalf("post-restart replay = %+v, want the durable original %s/%s",
			replay, originalID, originalNonce)
	}
	if got := restartCounts(t, sqlDB); got != afterCrash {
		t.Fatalf("post-restart replay wrote rows: %+v, want %+v", got, afterCrash)
	}
	if n := restartIntentBindingCount(t, sqlDB, intentID); n != 1 {
		t.Fatalf("intent %q has %d bindings after restart, want exactly 1", intentID, n)
	}
}

// TestNonceRestartCrashKill9Integration is the T021 V3 acceptance: a real
// kill -9 of a real process, twice, over one migrated scratch database.
func TestNonceRestartCrashKill9Integration(t *testing.T) {
	dsn := nonceStartPostgres(t)
	ctx := context.Background()

	var out bytes.Buffer
	if err := db.MigrateUp(ctx, nonceMigrateOptions(dsn), &out); err != nil {
		t.Fatalf("MigrateUp() error = %v (output %q)", err, out.String())
	}
	sqlDB := nonceOpenSQL(t, dsn)
	restartSeed(t, sqlDB)

	t.Run("kill_after_admission_commit", func(t *testing.T) {
		restartScenario(t, dsn, sqlDB,
			restartModeCommitted, "restart-intent-committed", restartAuthCommitted)
	})
	t.Run("kill_after_effect_started", func(t *testing.T) {
		restartScenario(t, dsn, sqlDB,
			restartModeEffect, "restart-intent-effect", restartAuthEffect)
	})
}

// TestNonceRestartCrashHelper is the child-process body, not a test: the parent
// re-executes it with restartHelperEnv set. It opens its own pool over the
// scratch database, runs one admission with the gate open (the pre-crash
// process had already completed startup verification), prints the durable
// binding identity, and then blocks until the parent SIGKILLs it. With the env
// marker absent it is a no-op (so the parent's own -run sweep is unaffected).
func TestNonceRestartCrashHelper(t *testing.T) {
	if os.Getenv(restartHelperEnv) == "" {
		return
	}

	ctx := context.Background()
	pool, err := db.OpenPool(ctx, os.Getenv(restartDSNEnv), 5*time.Second)
	if err != nil {
		fmt.Fprintf(os.Stderr, "restart helper: open pool: %v\n", err)
		os.Exit(2)
	}
	chainID, err := strconv.ParseInt(os.Getenv(restartChainEnv), 10, 64)
	if err != nil {
		fmt.Fprintf(os.Stderr, "restart helper: parse chain: %v\n", err)
		os.Exit(2)
	}
	gate := nonce.NewRebuildGate()
	gate.Open()
	alloc := nonce.NewAllocator(pool, restartObserver(&restartRPC{}), gate)

	req := nonce.AllocationRequest{
		IntentID:        os.Getenv(restartIntentEnv),
		ChainID:         chainID,
		Sender:          os.Getenv(restartSenderEnv),
		AuthorizationID: os.Getenv(restartAuthEnv),
	}
	binding, outcome, err := alloc.Allocate(ctx, req)
	if err != nil || binding == nil || outcome != nonce.OutcomeAllocated {
		fmt.Fprintf(os.Stderr, "restart helper: allocate = (%+v, %q, %v), want allocated\n",
			binding, outcome, err)
		os.Exit(3)
	}

	label := restartSignalAdmitted
	if os.Getenv(restartModeEnv) == restartModeEffect {
		// The admission is durable; we now begin the post-commit external step
		// whose result will never be reported (the parent kills us here).
		label = restartSignalEffect
	}
	fmt.Printf("%s %s %s\n", label, binding.BindingID, binding.Nonce.String())

	// Block until the parent SIGKILLs us: the crash must lose all in-memory
	// state, so sleep in a timer-backed loop (never a bare select{}).
	for {
		time.Sleep(time.Hour)
	}
}
