//go:build integration

package app

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	gethrpc "github.com/ethereum/go-ethereum/rpc"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"

	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/nonce"
)

// sc09Decoy mints a test-only credential: recognizable by its sc09- prefix,
// random per run, never a real secret. It MUST travel a real authentication or
// logging path (never a declared-but-unused string) for the scan below to mean
// anything.
func sc09Decoy(t *testing.T, kind string) string {
	t.Helper()
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		t.Fatalf("sc09 %s decoy entropy: %v", kind, err)
	}
	return "sc09-" + kind + "-" + hex.EncodeToString(raw[:])
}

// sc09Scan counts exact hits of decoy in corpus. The positive control
// (TestSC09ScannerPositiveControl, same file) proves a present decoy is always
// reported, so a zero below is evidence, not a broken matcher.
func sc09Scan(corpus, decoy string) int {
	return strings.Count(corpus, decoy)
}

// TestSC09ScannerPositiveControl pins the scanner both directions on strings
// that are NOT the acceptance run: a present decoy must be reported, an absent
// one must not. It never touches the acceptance corpus.
func TestSC09ScannerPositiveControl(t *testing.T) {
	decoy := sc09Decoy(t, "control")
	if got := sc09Scan("prefix "+decoy+" suffix", decoy); got != 1 {
		t.Fatalf("scanner found %d hits for a present decoy, want 1", got)
	}
	if got := sc09Scan("clean output with no decoy", decoy); got != 0 {
		t.Fatalf("scanner found %d hits for an absent decoy, want 0", got)
	}
}

// TestServeSC09LiveSecretScan is the SC-09 acceptance run (option B): one real
// boot of production Serve over real PostgreSQL + real Anvil, then real
// allocation / read / admin calls with two test-only decoys on real paths —
// the read token through config-load + HTTP Bearer auth (correct, wrong, and
// missing), and an intent-shaped decoy through the allocation log fields —
// plus refusal (sender_not_registered), 404, 401, and admin-usage error paths.
// Every captured byte (serve stdout/stderr, slog stream, HTTP bodies, admin
// outputs, startup/shutdown diagnosis) is scanned for both decoys with an
// asserted hit count of 0. Conclusion is scoped to this covered run only.
func TestServeSC09LiveSecretScan(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()

	readToken := sc09Decoy(t, "readtoken")
	var intentRaw, secretRaw [16]byte
	if _, err := rand.Read(intentRaw[:]); err != nil {
		t.Fatalf("sc09 intent decoy entropy: %v", err)
	}
	if _, err := rand.Read(secretRaw[:]); err != nil {
		t.Fatalf("sc09 secret decoy entropy: %v", err)
	}
	// plainIntent drives the read-back path (business ID: contract §3.1 echoes
	// it verbatim, FR-21 requires it in logs — specified, never scanned).
	plainIntent := "sc09-intent-" + hex.EncodeToString(intentRaw[:])
	// secretIntent carries a kv credential shape the log redactor must mask
	// (logx kvSecret `token=`). It is allocated but never read back: response
	// echo of operator-chosen IDs is contract-pinned verbatim (T034), so the
	// redaction property under test lives in the log funnel only.
	secretValue := hex.EncodeToString(secretRaw[:])
	secretIntent := "sc09-kv-token=" + secretValue

	pgCtr := startPostgresContainer(t)
	anvilCtr := startAnvilContainer(t)
	dsn := postgresDSN(t, pgCtr)
	rpcURL := anvilURL(t, anvilCtr)

	if err := db.MigrateUp(ctx, db.MigrateOptions{DSN: dsn, LockTimeout: 5 * time.Second, ConnectTimeout: 5 * time.Second}, io.Discard); err != nil {
		t.Fatalf("migrate up: %v", err)
	}

	const (
		chainID = "31337"
		asset   = "0x1111111111111111111111111111111111111111"
		watch   = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		sender  = "0x5151305151305151305151305151305151305151"
		authID  = "sc09-auth-1"
	)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, `INSERT INTO nonce_wallet_registry
		(chain_id, sender, state, registry_seq) VALUES (31337, $1, 'active', 1)`, sender); err != nil {
		t.Fatalf("seed registry: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO caller (caller_id, label) VALUES (7, 'sc09')`); err != nil {
		t.Fatalf("seed caller: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO withdrawal_authorizations
		(authorization_id, caller_id, chain_id, asset, recipient, amount, state, expires_at)
		VALUES ($1, 7, 31337, $2, $3, 1, 'active', NULL)`,
		authID, asset, watch); err != nil {
		t.Fatalf("seed authorization: %v", err)
	}

	// Capture the production slog stream in-process. Serve and the nonce
	// package log through slog.Default, which otherwise escapes to the test
	// binary's own stderr and would make the scan vacuous. Serial-only by
	// construction (no t.Parallel anywhere in this package); restored below.
	origLog := slog.Default()
	var slogBuf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&slogBuf, nil)))
	defer slog.SetDefault(origLog)

	httpAddr := freeAddr(t)
	env := map[string]string{
		"TXHARBOR_PG_DSN":                  dsn,
		"TXHARBOR_RPC_URL":                 rpcURL,
		"TXHARBOR_CHAIN_ID":                chainID,
		"TXHARBOR_START_HEIGHT":            "0",
		"TXHARBOR_LOG_START_HEIGHT":        "0",
		"TXHARBOR_LOG_CONTRACTS":           asset,
		"TXHARBOR_DEPOSIT_START_HEIGHT":    "0",
		"TXHARBOR_DEPOSIT_CONTRACTS":       asset + ":0",
		"TXHARBOR_DEPOSIT_WATCH_ADDRESSES": watch + ":0",
		"TXHARBOR_CONFIRMATION_DEPTH":      "10",
		"TXHARBOR_REORG_MAX_DEPTH":         "100",
		"TXHARBOR_HTTP_ADDR":               httpAddr,
		"TXHARBOR_NONCE_READ_TOKEN":        readToken,
	}
	var serveOut, serveErr bytes.Buffer
	signals := make(chan os.Signal, 1)
	done := make(chan int, 1)
	go func() {
		done <- Serve(ctx, Deps{Getenv: envGetter(env), Stdout: &serveOut, Stderr: &serveErr, Signals: signals})
	}()

	base := "http://" + httpAddr
	// Bounded client: a serving-but-silent endpoint must fail the poll loudly,
	// never park the test forever on one request.
	httpClient := &http.Client{Timeout: 15 * time.Second}
	pollReady := func() {
		deadline := time.Now().Add(90 * time.Second)
		for time.Now().Before(deadline) {
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, base+"/nonce/bindings/sc09-absent", nil)
			req.Header.Set("Authorization", "Bearer "+readToken)
			resp, err := httpClient.Do(req)
			if err == nil {
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode == http.StatusNotFound {
					return
				}
			}
			time.Sleep(500 * time.Millisecond)
		}
		t.Fatal("serve read API never became ready")
	}
	pollReady()

	var httpBodies bytes.Buffer
	doRead := func(token, path string) (int, string) {
		t.Helper()
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, base+path, nil)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := httpClient.Do(req)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		fmt.Fprintf(&httpBodies, "### %s -> %d\n%s\n", path, resp.StatusCode, body)
		return resp.StatusCode, string(body)
	}

	rpcClient, err := gethrpc.DialContext(ctx, rpcURL)
	if err != nil {
		t.Fatalf("dial anvil: %v", err)
	}
	defer rpcClient.Close()
	observer := nonce.NewObserver(rpcClient, nonce.ObserverConfig{
		RPCTimeout:   5 * time.Second,
		RetryInitial: time.Millisecond,
		RetryMax:     5 * time.Millisecond,
	})
	gate := nonce.NewRebuildGate()
	gate.Open()
	alloc := nonce.NewAllocator(pool, observer, gate)

	// Allocation through the production funnel: a plain admission (read back
	// below) plus a secret-shaped admission exercising log redaction.
	binding, outcome, err := alloc.Allocate(ctx, nonce.AllocationRequest{
		IntentID:        plainIntent,
		ChainID:         31337,
		Sender:          sender,
		AuthorizationID: authID,
	})
	if err != nil || outcome != nonce.OutcomeAllocated || binding == nil {
		t.Fatalf("allocate = (%+v, %q, %v), want allocated", binding, outcome, err)
	}
	if _, outcome, err := alloc.Allocate(ctx, nonce.AllocationRequest{
		IntentID:        secretIntent,
		ChainID:         31337,
		Sender:          sender,
		AuthorizationID: authID,
	}); err != nil || outcome != nonce.OutcomeAllocated {
		t.Fatalf("secret-shaped allocate = (%q, %v), want allocated", outcome, err)
	}
	// Refusal path (sender_not_registered): must log, must not bind.
	if _, outcome, err := alloc.Allocate(ctx, nonce.AllocationRequest{
		IntentID:        "sc09-intent-refused",
		ChainID:         31337,
		Sender:          "0x6161616161616161616161616161616161616161",
		AuthorizationID: authID,
	}); err == nil || outcome != nonce.OutcomeSenderNotRegistered {
		t.Fatalf("refused allocate = (%q, %v), want sender_not_registered", outcome, err)
	}

	// Read paths: bound (200), wrong token (401), missing token (401),
	// unknown binding (404).
	if code, body := doRead(readToken, "/nonce/bindings/"+binding.BindingID); code != http.StatusOK {
		t.Fatalf("bound read status = %d, want 200", code)
	} else if !strings.Contains(body, binding.BindingID) {
		t.Fatalf("bound read body lacks the binding id: %s", body)
	}
	if code, _ := doRead("sc09-wrong-token", "/nonce/bindings/"+binding.BindingID); code != http.StatusUnauthorized {
		t.Fatalf("wrong-token read status = %d, want 401", code)
	}
	if code, _ := doRead("", "/nonce/bindings/"+binding.BindingID); code != http.StatusUnauthorized {
		t.Fatalf("missing-token read status = %d, want 401", code)
	}
	if code, _ := doRead(readToken, "/nonce/bindings/sc09-absent"); code != http.StatusNotFound {
		t.Fatalf("unknown read status = %d, want 404", code)
	}

	// Admin paths: status success to stdout, usage error to stderr.
	var adminOut, adminErr bytes.Buffer
	adminDeps := Deps{Getenv: envGetter(env), Stdout: &adminOut, Stderr: &adminErr}
	if code := NonceAdmin(ctx, []string{"status", "--chain-id", chainID, "--sender", sender}, adminDeps); code != 0 {
		t.Fatalf("admin status exit = %d, want 0 (stderr: %s)", code, adminErr.String())
	}
	if !strings.Contains(adminOut.String(), "status ok") {
		t.Fatalf("admin status output lacks the ok line: %q", adminOut.String())
	}
	var adminErr2 bytes.Buffer
	if code := NonceAdmin(ctx, []string{"bogus-action"}, Deps{Getenv: envGetter(env), Stdout: io.Discard, Stderr: &adminErr2}); code != 2 {
		t.Fatalf("admin bogus-action exit = %d, want 2", code)
	}

	signals <- os.Interrupt
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("Serve() exit code = %d, want 0 on clean shutdown", code)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("Serve did not shut down after the termination signal")
	}

	// Non-vacuity: every captured surface must actually contain its run.
	if slogBuf.Len() == 0 {
		t.Fatal("captured slog stream is empty: the scan would be vacuous")
	}
	if !strings.Contains(slogBuf.String(), "nonce allocation") {
		t.Fatal("slog stream lacks allocation events: the decoy path may not have executed")
	}
	if httpBodies.Len() == 0 || adminOut.Len() == 0 || adminErr2.Len() == 0 {
		t.Fatal("one captured surface is empty (http/admin): the scan would be vacuous")
	}

	corpus := serveOut.String() + serveErr.String() + slogBuf.String() +
		httpBodies.String() + adminOut.String() + adminErr.String() + adminErr2.String()
	// The credential must appear nowhere. The secret-shaped intent must appear
	// nowhere either (never read back; in logs only masked): absence of both
	// the full string and the bare secret value. The surviving `sc09-kv-`
	// prefix in the logs proves the secret intent was actually emitted (not
	// dropped) and only its credential shape masked.
	for name, decoy := range map[string]string{
		"read_token": readToken, "secret_intent": secretIntent, "secret_value": secretValue,
	} {
		if got := sc09Scan(corpus, decoy); got != 0 {
			t.Fatalf("SC-09 live scan: decoy %s hit %d time(s), want 0", name, got)
		}
	}
	if got := sc09Scan(slogBuf.String()+serveOut.String()+serveErr.String(), "sc09-kv-"); got == 0 {
		t.Fatal("secret intent never reached the logs: redaction had nothing to mask (vacuous)")
	}
}
