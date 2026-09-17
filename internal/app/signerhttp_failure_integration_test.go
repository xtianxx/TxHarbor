//go:build integration

// signerhttp_failure_integration_test.go owns the reopened T043
// (contract-deviation batch) case for 009-signer-service: DETECTABLE transport
// write failures on the signer-serve delivery sink must reach the current
// attempt's unknown handling — never success, and never a success that is
// inherited from a historic `delivered` marker.
//
// The delivery sink writes the T-deliver protected region's bytes to the HTTP
// response writer. net/http buffers the small payload, so the first real socket
// write happens on flush: a bare `http.Flusher.Flush()` (no return value)
// silently discarded a write error or a `WriteTimeout` expiry, and the region
// then committed `delivered` over bytes the peer never received. The sink now
// flushes through `http.NewResponseController(w).Flush()`, which returns the
// transport error, so the region resolves the attempt as `unknown`
// (`outcome_unknown`, `bytes_may_be_out=true`) and never marks a failed attempt
// successful from history.
//
// Cases:
//
//   - real HTTP + PG: a `WriteTimeout`-expired response (the request is held
//     past the server's write deadline while the delivery region waits on the
//     caller lock) maps to `unknown`, rolls the region back, commits no
//     `delivered` marker and records the secrets-free `delivery_unknown` audit;
//   - real HTTP + PG: the same failing transport on a same-identity REPLAY of an
//     already-`delivered` request maps to `unknown`, retains the historic marker,
//     adds no new admission and never re-signs (no success-from-history);
//   - direct sink: write error, short write, flush error, deadline flush error
//     and a non-flushable writer each surface a non-nil error (the flush-error
//     case is the exact pre-fix hole), while a clean recorder succeeds; the
//     failure diagnostic is secrets-free and `ResponseController` support is
//     verified against the real net/http server writer.
//
// A mid-region client disconnect is a different path: net/http cancels the
// request context before the sink runs, so the region reports a retryable gate
// read failure with zero bytes — it is not a detectable write failure and is
// deliberately not asserted here.
package app

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/signer"
	"github.com/xtianxx/txharbor/internal/withdrawal"
)

// fhProbeTimeout is the server WriteTimeout for the failure harness: long
// enough that a clean delivery completes well inside it, short enough that a
// held region reliably writes past it.
const fhProbeTimeout = 2 * time.Second

// ---------------------------------------------------------------------------
// Synchronized stderr capture (the server goroutine writes while the test
// reads).
// ---------------------------------------------------------------------------

type fhSyncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *fhSyncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *fhSyncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// ---------------------------------------------------------------------------
// Harness: the real SignerServe with a small WriteTimeout (copy of shStart /
// shNewHarness with the deadline knob and captured stderr; the shared harness
// is fixed at the production default).
// ---------------------------------------------------------------------------

// fhStart boots the real SignerServe in-process and returns the base URL plus
// the captured stderr buffer.
func fhStart(t *testing.T, ctx context.Context, dsn string, env map[string]string) (string, *fhSyncBuffer) {
	t.Helper()
	addr := freePort(t)
	env[config.EnvSignerHTTPAddr] = addr
	env[config.EnvPGDSN] = dsn
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan int, 1)
	stderr := &fhSyncBuffer{}
	go func() {
		done <- SignerServe(runCtx, nil, Deps{
			Getenv: fakeEnv(env),
			Stdout: io.Discard,
			Stderr: stderr,
		})
	}()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case code := <-done:
			t.Fatalf("SignerServe exited early with %d; stderr=%s", code, stderr.String())
		default:
		}
		probe, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			probe.Close()
			t.Cleanup(func() {
				cancel()
				select {
				case code := <-done:
					if code != 0 {
						t.Errorf("SignerServe exit code = %d, want 0; stderr=%s", code, stderr.String())
					}
				case <-time.After(15 * time.Second):
					t.Errorf("SignerServe did not shut down after ctx cancel")
				}
			})
			return "http://" + addr, stderr
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("signer listener %s never opened; stderr=%s", addr, stderr.String())
	return "", nil
}

// fhNewHarness builds the shared fixture (live 008 stack, PB grant carrier, one
// generated key, real signer process) with the failure harness's write
// deadline.
func fhNewHarness(t *testing.T, ctx context.Context, probeTimeout string) (*shHarness, *fhSyncBuffer) {
	t.Helper()
	dsn := startConfirmAuthPostgres(t)
	pool, err := db.OpenPool(ctx, dsn, 5*time.Second)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)

	h := &shHarness{
		ctx:      ctx,
		pool:     pool,
		client:   &http.Client{Timeout: 20 * time.Second},
		callerID: 19501,
		alloc:    shAllocator(pool, newSHChainView()),
	}
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	h.sender = strings.ToLower(crypto.PubkeyToAddress(key.PublicKey).Hex())
	keyFile := filepath.Join(t.TempDir(), "t3.key")
	if err := os.WriteFile(keyFile, []byte(common.Bytes2Hex(crypto.FromECDSA(key))), 0o600); err != nil {
		t.Fatalf("write test key: %v", err)
	}

	apiKey, _, err := withdrawal.IssueKey(ctx, pool, h.callerID, "t3-issuer")
	if err != nil {
		t.Fatalf("IssueKey: %v", err)
	}
	h.issuerKey = apiKey
	cred, err := signer.IssueCredential(ctx, pool, h.callerID, "t3-signer")
	if err != nil {
		t.Fatalf("IssueCredential: %v", err)
	}
	h.cred = cred

	h.foreignID = 19502
	foreign, err := signer.IssueCredential(ctx, pool, h.foreignID, "t3-foreign")
	if err != nil {
		t.Fatalf("IssueCredential(foreign): %v", err)
	}
	if err := signer.SetCanSign(ctx, pool, h.foreignID, false); err != nil {
		t.Fatalf("SetCanSign(foreign): %v", err)
	}
	h.foreign = foreign

	env := fullServeEnv("127.0.0.1:0")
	env[config.EnvPGDSN] = dsn
	env[config.EnvChainID] = strconv.FormatInt(shChainID, 10)
	env[withdrawal.EnvIssuerCallers] = strconv.FormatInt(h.callerID, 10)
	h.pbEnv = env

	serverEnv := fullServeEnv("127.0.0.1:0")
	serverEnv[config.EnvSignerMode] = "development"
	serverEnv[config.EnvSignerKeyFile] = keyFile
	serverEnv[config.EnvSignerChains] = strconv.FormatInt(shChainID, 10)
	serverEnv[config.EnvSignerSenders] = h.sender
	serverEnv[config.EnvSignerAssets] = shAsset
	serverEnv[config.EnvSignerRecipients] = shRecipient
	serverEnv[config.EnvSignerMaxAmount] = "2000000"
	serverEnv[config.EnvSignerMaxGasLimit] = "100000"
	serverEnv[config.EnvSignerMaxFeePerGas] = "2000000000"
	serverEnv[config.EnvSignerMaxPriorityFee] = "1500000000"
	serverEnv[config.EnvSignerMaxGasPrice] = "2000000000"
	serverEnv[config.EnvNonceReadToken] = shReadToken
	serverEnv[config.EnvProbeTimeout] = probeTimeout
	var stderr *fhSyncBuffer
	h.baseURL, stderr = fhStart(t, ctx, dsn, serverEnv)

	shRegistry(t, ctx, pool, h.sender)
	return h, stderr
}

// fhWaitRegionBlocked waits until a backend is waiting on the caller-row lock
// inside the delivery region's `can_sign ... FOR SHARE` read.
func fhWaitRegionBlocked(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for {
		var n int
		err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity
			WHERE datname = current_database()
			  AND wait_event_type = 'Lock'
			  AND query ILIKE '%signer_caller%'`).Scan(&n)
		if err == nil && n > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("delivery region never blocked on the caller lock (err=%v)", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// fhHoldDelivery holds the caller row so the delivery region waits, then
// releases it only after hold — by which point the request's server-side write
// deadline has expired, so the region's flush is a real, detectable transport
// failure. Returns the client-side outcome (an error or a non-200 response).
func fhHoldDelivery(t *testing.T, h *shHarness, id *shIdentity, body []byte, hold time.Duration) (int, []byte, error) {
	t.Helper()
	ctx := h.ctx
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin caller lock: %v", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	var locked int64
	if err := tx.QueryRow(ctx,
		`SELECT caller_id FROM signer_caller WHERE caller_id = $1 FOR NO KEY UPDATE`,
		h.callerID).Scan(&locked); err != nil {
		t.Fatalf("lock caller row: %v", err)
	}

	type wireResp struct {
		status int
		body   []byte
		err    error
	}
	ch := make(chan wireResp, 1)
	go func() {
		status, raw, err := h.doRaw(http.MethodPost, "/signer/v1/signing-requests", body, h.cred)
		ch <- wireResp{status: status, body: raw, err: err}
	}()

	fhWaitRegionBlocked(t, ctx, h.pool)
	time.Sleep(hold)
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit caller lock: %v", err)
	}
	res := <-ch
	return res.status, res.body, res.err
}

// fhAuditDetails returns the recorded `detail` values for one audit action.
func fhAuditDetails(t *testing.T, ctx context.Context, pool *pgxpool.Pool, requestID string, callerID int64, action string) []string {
	t.Helper()
	rows, err := pool.Query(ctx, `SELECT detail FROM signing_request_audit
		WHERE signing_request_id = $1 AND caller_id = $2 AND action = $3 ORDER BY audit_id`,
		requestID, callerID, action)
	if err != nil {
		t.Fatalf("query %s audit: %v", action, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var d string
		if err := rows.Scan(&d); err != nil {
			t.Fatalf("scan %s audit: %v", action, err)
		}
		out = append(out, d)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate %s audit: %v", action, err)
	}
	return out
}

// fhAssertNoSecret checks the captured signer log for signature, tx hash and
// credential material (the failure diagnostic must never carry them).
func fhAssertNoSecret(t *testing.T, stderr *fhSyncBuffer, sig, hash, cred string) {
	t.Helper()
	logged := stderr.String()
	for _, secret := range []string{sig, hash, cred} {
		if secret == "" {
			continue
		}
		if strings.Contains(logged, secret) {
			t.Fatalf("signer log leaks secret material %q", secret)
		}
	}
}

// ---------------------------------------------------------------------------
// Real HTTP + PG: detectable write failures resolve the current attempt unknown.
// ---------------------------------------------------------------------------

func TestSignerHTTPWriteFailureUnknown(t *testing.T) {
	ctx := context.Background()
	h, stderr := fhNewHarness(t, ctx, fhProbeTimeout.String())

	// A clean delivery proves the running server's ResponseWriter supports
	// error-returning Flush end to end (an unsupported writer would map every
	// delivery to unknown), and the direct probe confirms it on a real
	// net/http writer.
	t.Run("response controller support is verified", func(t *testing.T) {
		var probeErr error
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			probeErr = http.NewResponseController(w).Flush()
			_, _ = io.WriteString(w, "ok")
		}))
		defer srv.Close()
		resp, err := srv.Client().Get(srv.URL)
		if err != nil {
			t.Fatalf("probe server: %v", err)
		}
		_ = resp.Body.Close()
		if probeErr != nil {
			t.Fatalf("net/http server ResponseWriter has no error-returning Flush: %v", probeErr)
		}

		id := h.identity(t, "rc", false)
		status, raw := h.post(t, h.body(t, id), h.cred)
		if status != http.StatusOK {
			t.Fatalf("clean delivery = %d/%s, want 200", status, raw)
		}
		rowID := shRowID(t, ctx, h.pool, h.callerID, id.requestID)
		adm := shWaitAdmission(t, ctx, h.pool, rowID, "delivered", 1)
		if adm.deliveredAt == nil || !shDeliveredMarker(t, ctx, h.pool, rowID) {
			t.Fatalf("clean delivery admission = %+v, want committed delivered", adm)
		}
	})

	// A response whose write deadline already fired: the region's buffer flush
	// is the first real socket write, so a value-less Flush would hide the
	// failure and commit `delivered`. It must resolve unknown.
	t.Run("deadline write failure maps to unknown", func(t *testing.T) {
		id := h.identity(t, "deadline", false)
		body := h.body(t, id)
		status, raw, err := fhHoldDelivery(t, h, id, body, fhProbeTimeout+time.Second)
		if err == nil && status == http.StatusOK {
			t.Fatalf("deadline-failed delivery = %d/%s, want no 200 success", status, raw)
		}
		rowID := shRowID(t, ctx, h.pool, h.callerID, id.requestID)
		sig, hash := shResult(t, ctx, h.pool, rowID)
		shWantNoSignature(t, raw, sig, hash)
		if shDeliveredMarker(t, ctx, h.pool, rowID) {
			t.Fatal("failed delivery committed a delivered marker")
		}
		if n := shAdmissionCount(t, ctx, h.pool, rowID); n != 0 {
			t.Fatalf("delivery_admissions rows = %d, want 0 (the failed region rolled back)", n)
		}
		details := fhAuditDetails(t, ctx, h.pool, id.requestID, h.callerID, "delivery_unknown")
		if len(details) == 0 {
			t.Fatal("failed write recorded no delivery_unknown audit")
		}
		for _, d := range details {
			if !strings.Contains(d, "bytes_may_be_out=true") {
				t.Errorf("delivery_unknown detail %q lacks bytes_may_be_out=true", d)
			}
			if sig != "" && strings.Contains(d, sig) {
				t.Fatalf("delivery_unknown detail leaks the signature: %q", d)
			}
		}
		fhAssertNoSecret(t, stderr, sig, hash, h.cred)
		t.Logf("deadline: %s write failed after %s; region rolled back (0 admissions), audit=delivery_unknown, no delivered marker",
			id.requestID, fhProbeTimeout)
	})

	// A same-identity replay of an already-delivered request whose current
	// transport write fails must NOT succeed from the historic marker.
	t.Run("failed replay never succeeds from the historic marker", func(t *testing.T) {
		id := h.identity(t, "replay", false)
		body := h.body(t, id)
		if status, raw := h.post(t, body, h.cred); status != http.StatusOK {
			t.Fatalf("seed delivery = %d/%s, want 200", status, raw)
		}
		rowID := shRowID(t, ctx, h.pool, h.callerID, id.requestID)
		shWaitAdmission(t, ctx, h.pool, rowID, "delivered", 1)
		sig, hash := shResult(t, ctx, h.pool, rowID)

		status, raw, err := fhHoldDelivery(t, h, id, body, fhProbeTimeout+time.Second)
		if err == nil && status == http.StatusOK {
			t.Fatalf("failed replay = %d/%s, want no 200 success-from-history", status, raw)
		}
		shWantNoSignature(t, raw, sig, hash)
		if !shDeliveredMarker(t, ctx, h.pool, rowID) {
			t.Fatal("failed replay retracted the historic delivered marker")
		}
		if n := shAdmissionCount(t, ctx, h.pool, rowID); n != 1 {
			t.Fatalf("delivery_admissions rows = %d, want 1 (failed replay rolled back; history retained)", n)
		}
		if sig2, hash2 := shResult(t, ctx, h.pool, rowID); sig2 != sig || hash2 != hash {
			t.Fatalf("failed replay rewrote the persisted result: %s/%s", sig2, hash2)
		}
		if len(fhAuditDetails(t, ctx, h.pool, id.requestID, h.callerID, "delivery_unknown")) == 0 {
			t.Fatal("failed replay recorded no delivery_unknown audit")
		}
		fhAssertNoSecret(t, stderr, sig, hash, h.cred)
		t.Logf("replay: %s failed write maps to unknown; historic marker retained, exactly 1 admission, result never re-signed", id.requestID)
	})
}

// ---------------------------------------------------------------------------
// Direct sink: every observable transport failure is returned (deterministic).
// ---------------------------------------------------------------------------

// fhRW is a scripted ResponseWriter that reports FlushError (the API
// ResponseController prefers) and Flush (the legacy no-error API).
type fhRW struct {
	header   http.Header
	code     int
	writeErr error
	shortBy  int
	flushErr error
}

func (w *fhRW) Header() http.Header {
	if w.header == nil {
		w.header = http.Header{}
	}
	return w.header
}

func (w *fhRW) WriteHeader(code int) { w.code = code }

func (w *fhRW) Write(p []byte) (int, error) {
	if w.writeErr != nil {
		return 0, w.writeErr
	}
	if w.shortBy > 0 {
		return len(p) - w.shortBy, nil
	}
	return len(p), nil
}

func (w *fhRW) Flush()            {}
func (w *fhRW) FlushError() error { return w.flushErr }

// fhPlainRW supports neither Flush nor FlushError.
type fhPlainRW struct {
	header http.Header
	code   int
}

func (w *fhPlainRW) Header() http.Header {
	if w.header == nil {
		w.header = http.Header{}
	}
	return w.header
}

func (w *fhPlainRW) WriteHeader(code int)        { w.code = code }
func (w *fhPlainRW) Write(p []byte) (int, error) { return len(p), nil }

func TestSignerHTTPDeliverySinkSurfacesWriteErrors(t *testing.T) {
	payload := []byte(`{"signing_request_id":"sr-fh","state":"signed","signature":"0xdeadbeefcafe","tx_hash":"0xfeedface","delivery":"delivered"}`)
	errWrite := errors.New("write refused")
	errFlush := errors.New("flush refused")

	// The sink's failure diagnostic must be secrets-free; capture slog.
	var logBuf fhSyncBuffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logBuf, nil)))
	defer slog.SetDefault(prev)

	cases := []struct {
		name       string
		w          http.ResponseWriter
		wantAnyErr bool
		wantIs     error
	}{
		{name: "write error", w: &fhRW{writeErr: errWrite}, wantAnyErr: true, wantIs: errWrite},
		{name: "short write", w: &fhRW{shortBy: 3}, wantAnyErr: true, wantIs: io.ErrShortWrite},
		{name: "flush error", w: &fhRW{flushErr: errFlush}, wantAnyErr: true, wantIs: errFlush},
		{name: "deadline flush error", w: &fhRW{flushErr: os.ErrDeadlineExceeded}, wantAnyErr: true, wantIs: os.ErrDeadlineExceeded},
		{name: "flush unsupported", w: &fhPlainRW{}, wantAnyErr: true, wantIs: http.ErrNotSupported},
		{name: "clean recorder", w: httptest.NewRecorder(), wantAnyErr: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sink := &signerHTTPDeliverySink{w: tc.w, trace: "fh-trace"}
			err := sink.WriteDelivery(context.Background(), payload)
			if tc.wantAnyErr {
				if err == nil {
					t.Fatalf("WriteDelivery = nil, want a surfaced transport error")
				}
				if tc.wantIs != nil && !errors.Is(err, tc.wantIs) {
					t.Fatalf("WriteDelivery error = %v, want %v", err, tc.wantIs)
				}
				if !sink.started {
					t.Fatal("sink.started = false after WriteHeader, want true")
				}
			} else if err != nil {
				t.Fatalf("clean WriteDelivery = %v, want nil", err)
			}
		})
	}

	// The flush-error diagnostic exists, names the trace, and never carries the
	// payload (signature/tx hash).
	logged := logBuf.String()
	if !strings.Contains(logged, "fh-trace") {
		t.Fatalf("write-failure diagnostic missing the trace id: %q", logged)
	}
	for _, secret := range []string{"0xdeadbeefcafe", "0xfeedface", string(payload)} {
		if strings.Contains(logged, secret) {
			t.Fatalf("write-failure diagnostic leaks payload material %q: %q", secret, logged)
		}
	}
}
