//go:build integration

// signerhttp_integration_test.go owns spec task T043 for 009-signer-service:
// the HTTP transport acceptance. It boots the REAL `SignerServe` process path
// (config gate → pool → policy → key provider → live 008 wiring → listener)
// against a real, isolated PostgreSQL, a real PB-supplied scoped grant (the
// internal/app withdrawal-authz carrier), and the live 008 binding stack
// (operator registry_register → Allocator.Allocate → ReadProvider), then
// drives the wire:
//
//   - V1: a legal submit signs over HTTP and its success body is proven to be
//     the T-deliver protected region's render of the persisted result (bytes
//     compared against the facts read back from `signature_results` +
//     `delivery_admissions`, not against the library's return value), with the
//     `delivered` admission marker committed by the same region;
//   - status: desensitized (api.md §3) — never the signature, tx_hash only
//     while the latest admission cleared it, identical 404 for a foreign and a
//     nonexistent id;
//   - auth/refusal: the api.md §2 codes (401/403/400/422) with no credential
//     echo;
//   - retry: same-identity replay re-delivers byte-identically; a request whose
//     delivery was withheld re-gates on every attempt (no TTL grace) over
//     HTTP, and only a fresh admission produces the bytes;
//   - delivery-gating: `can_sign` off, 007 revoke, and a 006 pause each block
//     the response with zero bytes over HTTP; releasing the pause re-delivers
//     the persisted bytes.
//
// The `can_sign` block is made deterministic without racing: the test holds
// the `signer_caller` row FOR NO KEY UPDATE; submit's permission read is a
// plain SELECT (never blocked), while the delivery region's `can_sign` re-read
// is FOR SHARE (delivery.go deliveryCanSignSQL) and waits — the test then flips
// `can_sign` and commits. The revoked/paused cases replay an identity that
// already has a persisted result and no delivered marker, so submit's replay
// fast path (api.md §4 step 3, deterministic — no re-gate) is lockless and the
// delivery re-gate is the only gate read.
//
// The process under test carries the production default wiring
// (signerlive.go newSignerLiveBinding), so every case here also exercises the
// untagged adapter end to end; TestSignerHTTPLiveBindingParity pins that
// adapter's classification to the canonical integration-tagged
// signer.LiveBindingReader mapping.
//
// Test-only key material: one generated dev key written to t.TempDir() and
// loaded by the real provider; no credential ever reaches a response or log.
package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/nonce"
	"github.com/xtianxx/txharbor/internal/signer"
	"github.com/xtianxx/txharbor/internal/withdrawal"
)

const (
	shChainID     = int64(31337)
	shAsset       = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	shRecipient   = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	shAmount      = "1000000"
	shGasLimit    = "65000"
	shReadToken   = "t043-nonce-read-token"
	shMaxFee      = "1500000000"
	shMaxPriority = "1000000000"
)

// shHash is a syntactically valid 32-byte 0x hash for the 006 pause fixture.
func shHash(fill string) string { return "0x" + strings.Repeat(fill, 32) }

// ---------------------------------------------------------------------------
// Live 008 stack (same wiring shape as the T028/T040 acceptance, rebuilt here
// because the package-signer helpers are not importable).
// ---------------------------------------------------------------------------

// shChainView is the scripted 008 chain view: a healthy scope (latest ==
// pending == 0) for the generated sender. It substitutes no 009/008 behavior —
// the live binding read under test reaches no RPC.
type shChainView struct {
	mu      sync.Mutex
	latest  string
	pending string
}

func newSHChainView() *shChainView { return &shChainView{latest: "0x0", pending: "0x0"} }

func (v *shChainView) CallContext(_ context.Context, result any, method string, args ...any) error {
	switch method {
	case "eth_getTransactionCount":
		v.mu.Lock()
		q := v.latest
		if len(args) > 1 {
			if block, ok := args[1].(string); ok && block == "pending" {
				q = v.pending
			}
		}
		v.mu.Unlock()
		return json.Unmarshal([]byte(strconv.Quote(q)), result)
	case "eth_getBlockByNumber":
		payload := fmt.Sprintf(`{"number":"0x1","hash":%q}`, "0x"+strings.Repeat("ef", 32))
		return json.Unmarshal([]byte(payload), result)
	default:
		return fmt.Errorf("shChainView: unexpected method %q", method)
	}
}

// shAllocator wires 008's admission core over the scripted view and an
// explicitly opened rebuild gate (008's own test wiring).
func shAllocator(pool *pgxpool.Pool, view *shChainView) *nonce.Allocator {
	gate := nonce.NewRebuildGate()
	gate.Open()
	return nonce.NewAllocator(pool, nonce.NewObserver(view, nonce.ObserverConfig{
		RPCTimeout:   2 * time.Second,
		RetryInitial: time.Millisecond,
		RetryMax:     5 * time.Millisecond,
	}), gate)
}

// shRegistry registers the generated sender through 008's operator API.
func shRegistry(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sender string) {
	t.Helper()
	res, err := nonce.NewAdminRunner(pool, nil).Run(ctx, nonce.AdminRequest{
		Action: nonce.AdminActionRegistryRegister, OperationID: "t043-reg-1",
		ChainID: shChainID, Sender: sender,
	})
	if err != nil || res.Outcome != nonce.AdminApplied {
		t.Fatalf("008 registry_register = (%+v, %v), want applied", res, err)
	}
}

// ---------------------------------------------------------------------------
// Harness: real SignerServe on a free local port.
// ---------------------------------------------------------------------------

// shHarness is one running signer process plus its fixture identities.
type shHarness struct {
	ctx       context.Context
	pool      *pgxpool.Pool
	baseURL   string
	client    *http.Client
	sender    string
	callerID  int64
	cred      string
	foreignID int64
	foreign   string
	issuerKey string
	pbEnv     map[string]string
	alloc     *nonce.Allocator
}

// shStart boots the real SignerServe (the same function `cmd/txharbor` calls)
// in-process with a full signer environment, waits for the listener, and
// registers a clean shutdown.
func shStart(t *testing.T, ctx context.Context, dsn string, env map[string]string) string {
	t.Helper()
	addr := freePort(t)
	env[config.EnvSignerHTTPAddr] = addr
	env[config.EnvPGDSN] = dsn
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan int, 1)
	var stderr bytes.Buffer
	go func() {
		done <- SignerServe(runCtx, nil, Deps{
			Getenv: fakeEnv(env),
			Stdout: io.Discard,
			Stderr: &stderr,
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
			return "http://" + addr
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("signer listener %s never opened; stderr=%s", addr, stderr.String())
	return ""
}

// shNewHarness builds the shared fixture: one caller with a 009 credential and
// a 007 issuer key, one generated sender key, the live 008 registry/binding
// stack, and the running signer process.
func shNewHarness(t *testing.T, ctx context.Context) *shHarness {
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
		callerID: 19401,
		alloc:    shAllocator(pool, newSHChainView()),
	}
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	h.sender = strings.ToLower(crypto.PubkeyToAddress(key.PublicKey).Hex())
	keyFile := filepath.Join(t.TempDir(), "t043.key")
	if err := os.WriteFile(keyFile, []byte(common.Bytes2Hex(crypto.FromECDSA(key))), 0o600); err != nil {
		t.Fatalf("write test key: %v", err)
	}

	// Caller identity: 009 credential + the 007 issuer key the PB carrier
	// authenticates its issuer with.
	apiKey, _, err := withdrawal.IssueKey(ctx, pool, h.callerID, "t043-issuer")
	if err != nil {
		t.Fatalf("IssueKey: %v", err)
	}
	h.issuerKey = apiKey
	cred, err := signer.IssueCredential(ctx, pool, h.callerID, "t043-signer")
	if err != nil {
		t.Fatalf("IssueCredential: %v", err)
	}
	h.cred = cred

	// Foreign caller: authenticates (valid credential) but owns nothing and
	// may not sign — the 404-ownership and 403-permission subject.
	h.foreignID = 19402
	foreign, err := signer.IssueCredential(ctx, pool, h.foreignID, "t043-foreign")
	if err != nil {
		t.Fatalf("IssueCredential(foreign): %v", err)
	}
	if err := signer.SetCanSign(ctx, pool, h.foreignID, false); err != nil {
		t.Fatalf("SetCanSign(foreign): %v", err)
	}
	h.foreign = foreign

	// PB issuer allowlist for the real supply carrier.
	env := fullServeEnv("127.0.0.1:0")
	env[config.EnvPGDSN] = dsn
	env[config.EnvChainID] = strconv.FormatInt(shChainID, 10)
	env[withdrawal.EnvIssuerCallers] = strconv.FormatInt(h.callerID, 10)
	h.pbEnv = env

	// The signer process environment: full config + signer required-ness +
	// the test key and the nonce read token.
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
	h.baseURL = shStart(t, ctx, dsn, serverEnv)

	shRegistry(t, ctx, pool, h.sender)
	return h
}

// shIdentity is one live fixture identity: one PB-supplied scoped grant and
// one live 008 binding carrying its nonce.
type shIdentity struct {
	authzID   string
	intentID  string
	requestID string
	attemptID string
	nonce     string
	bindingID string
}

// identity supplies the grant through the real PB carrier and allocates the
// live 008 binding for a fresh intent.
func (h *shHarness) identity(t *testing.T, tag string, allowsFeeReplacement bool) *shIdentity {
	t.Helper()
	id := &shIdentity{
		authzID:   "wa-t043-" + tag,
		intentID:  "pi-t043-" + tag,
		requestID: "sr-t043-" + tag,
		attemptID: "at-t043-" + tag,
	}
	opID := withdrawalAuthzMintID(t, h.ctx, h.pbEnv)
	args := []string{
		"supply", "--operation-id", opID,
		"--authorization-id", id.authzID,
		"--api-key", h.issuerKey,
		"--caller-id", strconv.FormatInt(h.callerID, 10),
		"--chain-id", strconv.FormatInt(shChainID, 10),
		"--asset", shAsset, "--recipient", shRecipient, "--amount", shAmount,
		"--operator", "t043-operator", "--reason", "T043 HTTP transport acceptance",
		"--intent-id", id.intentID, "--request-id", id.requestID, "--sender", h.sender,
		"--fee-max-total", "100000000000000", "--fee-max-per-gas", "2000000000",
		"--fee-max-priority", "1500000000",
	}
	if allowsFeeReplacement {
		args = append(args, "--allows-fee-replacement")
	}
	code, stdout, stderr := withdrawalAuthzRun(h.ctx, h.pbEnv, args...)
	if code != 0 || !strings.Contains(stdout, "action=supplied") {
		t.Fatalf("PB supply exit=%d stdout=%q stderr=%q, want supplied", code, stdout, stderr)
	}
	binding, outcome, err := h.alloc.Allocate(h.ctx, nonce.AllocationRequest{
		IntentID: id.intentID, ChainID: shChainID, Sender: h.sender, AuthorizationID: id.authzID,
	})
	if err != nil || outcome != nonce.OutcomeAllocated || binding == nil {
		t.Fatalf("008 allocate = (%v, %q, %v), want allocated", binding, outcome, err)
	}
	id.bindingID, id.nonce = binding.BindingID, binding.Nonce.String()
	return id
}

// shReqJSON is the exact api.md §2 submit body (no unknown keys; type-2 fee
// shape).
type shReqJSON struct {
	SigningRequestID     string `json:"signing_request_id"`
	AttemptID            string `json:"attempt_id"`
	IntentID             string `json:"intent_id"`
	BindingRef           string `json:"binding_ref"`
	RecoveryVersion      uint64 `json:"recovery_version"`
	ChainID              uint64 `json:"chain_id"`
	Sender               string `json:"sender"`
	Nonce                string `json:"nonce"`
	TxType               uint8  `json:"tx_type"`
	To                   string `json:"to"`
	Value                string `json:"value"`
	Data                 string `json:"data"`
	GasLimit             string `json:"gas_limit"`
	MaxFeePerGas         string `json:"max_fee_per_gas,omitempty"`
	MaxPriorityFeePerGas string `json:"max_priority_fee_per_gas,omitempty"`
	Asset                string `json:"asset"`
	Recipient            string `json:"recipient"`
	Amount               string `json:"amount"`
	AuthorizationID      string `json:"authorization_id"`
}

// body renders one compliant type-2 transfer request for the identity.
func (h *shHarness) body(t *testing.T, id *shIdentity) []byte {
	t.Helper()
	raw, err := json.Marshal(shReqJSON{
		SigningRequestID: id.requestID, AttemptID: id.attemptID,
		IntentID: id.intentID, BindingRef: id.bindingID,
		RecoveryVersion: 0, ChainID: uint64(shChainID),
		Sender: h.sender, Nonce: id.nonce, TxType: 2,
		To: shAsset, Value: "0", Data: shTransferData(shRecipient),
		GasLimit: shGasLimit, MaxFeePerGas: shMaxFee, MaxPriorityFeePerGas: shMaxPriority,
		Asset: shAsset, Recipient: shRecipient, Amount: shAmount,
		AuthorizationID: id.authzID,
	})
	if err != nil {
		t.Fatalf("marshal submit body: %v", err)
	}
	return raw
}

// shTransferData is transfer(recipient, 1000000) calldata.
func shTransferData(recipient string) string {
	return "0xa9059cbb" + strings.Repeat("0", 24) + strings.TrimPrefix(recipient, "0x") +
		fmt.Sprintf("%064x", big.NewInt(1000000))
}

// ---------------------------------------------------------------------------
// Wire client + payload/state helpers.
// ---------------------------------------------------------------------------

// doRaw is the transport call without test-fatal handling: it is also used
// from the withholding helper's request goroutine, where t.Fatal is illegal.
func (h *shHarness) doRaw(method, path string, body []byte, cred string) (int, []byte, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(h.ctx, method, h.baseURL+path, reader)
	if err != nil {
		return 0, nil, err
	}
	if cred != "" {
		req.Header.Set("Authorization", "Bearer "+cred)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := h.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, nil, err
	}
	return resp.StatusCode, raw, nil
}

func (h *shHarness) do(t *testing.T, method, path string, body []byte, cred string) (int, []byte) {
	t.Helper()
	status, raw, err := h.doRaw(method, path, body, cred)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	return status, raw
}

func (h *shHarness) post(t *testing.T, body []byte, cred string) (int, []byte) {
	t.Helper()
	return h.do(t, http.MethodPost, "/signer/v1/signing-requests", body, cred)
}

func (h *shHarness) get(t *testing.T, id, cred string) (int, []byte) {
	t.Helper()
	return h.do(t, http.MethodGet, "/signer/v1/signing-requests/"+id, nil, cred)
}

// shPayload mirrors the region's wire payload (delivery.go deliveryPayload) so
// the test can byte-compare the HTTP response against the persisted facts.
type shPayload struct {
	SigningRequestID string `json:"signing_request_id"`
	State            string `json:"state"`
	Signature        string `json:"signature"`
	TxHash           string `json:"tx_hash"`
	Delivery         string `json:"delivery"`
}

// shWantDelivered asserts the response is exactly the region's render of the
// persisted facts (signature_results + the signed state) — the HTTP bytes
// traverse the T-deliver region, never a library-only rendering.
func shWantDelivered(t *testing.T, raw []byte, requestID, sig, hash string) {
	t.Helper()
	want, err := json.Marshal(shPayload{
		SigningRequestID: requestID, State: "signed",
		Signature: sig, TxHash: hash, Delivery: "delivered",
	})
	if err != nil {
		t.Fatalf("marshal expected payload: %v", err)
	}
	if !bytes.Equal(raw, want) {
		t.Fatalf("response bytes = %s, want the region's persisted render %s", raw, want)
	}
}

// shWantNoSignature asserts no signing material is in the body.
func shWantNoSignature(t *testing.T, raw []byte, sig, hash string) {
	t.Helper()
	if sig != "" && bytes.Contains(raw, []byte(sig)) {
		t.Fatalf("response leaks the signature bytes: %s", raw)
	}
	if hash != "" && bytes.Contains(raw, []byte(hash)) {
		t.Fatalf("response leaks the transaction hash: %s", raw)
	}
	if bytes.Contains(raw, []byte(`"signature"`)) {
		t.Fatalf("response carries a signature field: %s", raw)
	}
}

// shErrorBody is the api.md error shape as the test reads it.
type shErrorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Field   string `json:"field"`
	TraceID string `json:"trace_id"`
	Status  *struct {
		SigningRequestID string `json:"signing_request_id"`
		State            string `json:"state"`
		RefusalClass     string `json:"refusal_class"`
		TxHash           string `json:"tx_hash"`
		Delivery         *struct {
			Verdict            string `json:"verdict"`
			AttemptSeq         int    `json:"attempt_seq"`
			Reason             string `json:"reason"`
			PauseBasis         string `json:"pause_basis"`
			AuthorizationState string `json:"authorization_state"`
			CanSign            bool   `json:"can_sign"`
		} `json:"delivery"`
	} `json:"status"`
}

func shDecodeError(t *testing.T, raw []byte) shErrorBody {
	t.Helper()
	var body shErrorBody
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("error body is not JSON (%v): %s", err, raw)
	}
	return body
}

// shRowID resolves one persisted request row.
func shRowID(t *testing.T, ctx context.Context, pool *pgxpool.Pool, callerID int64, requestID string) int64 {
	t.Helper()
	var id int64
	if err := pool.QueryRow(ctx,
		`SELECT id FROM signing_requests WHERE caller_id = $1 AND signing_request_id = $2`,
		callerID, requestID).Scan(&id); err != nil {
		t.Fatalf("read signing_requests %s: %v", requestID, err)
	}
	return id
}

// shRequestRows counts a caller's persisted rows for one request id.
func shRequestRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, callerID int64, requestID string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM signing_requests WHERE caller_id = $1 AND signing_request_id = $2`,
		callerID, requestID).Scan(&n); err != nil {
		t.Fatalf("count signing_requests %s: %v", requestID, err)
	}
	return n
}

// shState reads the request row state.
func shState(t *testing.T, ctx context.Context, pool *pgxpool.Pool, rowID int64) string {
	t.Helper()
	var state string
	if err := pool.QueryRow(ctx, `SELECT state FROM signing_requests WHERE id = $1`, rowID).Scan(&state); err != nil {
		t.Fatalf("read state: %v", err)
	}
	return state
}

// shResult reads the single persisted signing result (1 row or fail).
func shResult(t *testing.T, ctx context.Context, pool *pgxpool.Pool, rowID int64) (string, string) {
	t.Helper()
	var n int
	var sig, hash string
	if err := pool.QueryRow(ctx, `SELECT count(*), COALESCE(MIN(signature), ''), COALESCE(MIN(tx_hash), '')
		FROM signature_results WHERE signing_request_row = $1`, rowID).Scan(&n, &sig, &hash); err != nil {
		t.Fatalf("read signature_results: %v", err)
	}
	if n != 1 {
		t.Fatalf("signature_results rows = %d, want exactly 1 (never a second signature)", n)
	}
	return sig, hash
}

// shAdmission is the latest delivery admission snapshot.
type shAdmission struct {
	verdict     string
	reason      string
	pauseBasis  string
	authzState  string
	canSign     bool
	attemptSeq  int
	deliveredAt *time.Time
}

func shLastAdmission(t *testing.T, ctx context.Context, pool *pgxpool.Pool, rowID int64) (shAdmission, bool) {
	t.Helper()
	var a shAdmission
	err := pool.QueryRow(ctx, `SELECT verdict, reason, pause_basis, authorization_state, can_sign, attempt_seq, delivered_at
		FROM delivery_admissions WHERE signing_request_row = $1 ORDER BY attempt_seq DESC LIMIT 1`, rowID).Scan(
		&a.verdict, &a.reason, &a.pauseBasis, &a.authzState, &a.canSign, &a.attemptSeq, &a.deliveredAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return a, false
	}
	if err != nil {
		t.Fatalf("read delivery_admissions: %v", err)
	}
	return a, true
}

// shWaitAdmission waits for the delivery transaction to commit the expected
// admission: the region writes the response bytes before the marker UPDATE and
// COMMIT (delivery.go), so a client that just read a 200 may observe the
// previous admission for a moment.
func shWaitAdmission(t *testing.T, ctx context.Context, pool *pgxpool.Pool, rowID int64, verdict string, attemptSeq int) shAdmission {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		adm, ok := shLastAdmission(t, ctx, pool, rowID)
		if ok && adm.verdict == verdict && adm.attemptSeq == attemptSeq {
			return adm
		}
		if time.Now().After(deadline) {
			t.Fatalf("admission for row %d = %+v found=%v, want %s attempt %d", rowID, adm, ok, verdict, attemptSeq)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func shAdmissionCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, rowID int64) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM delivery_admissions WHERE signing_request_row = $1`, rowID).Scan(&n); err != nil {
		t.Fatalf("count delivery_admissions: %v", err)
	}
	return n
}

func shDeliveredMarker(t *testing.T, ctx context.Context, pool *pgxpool.Pool, rowID int64) bool {
	t.Helper()
	var exists bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM delivery_admissions
		WHERE signing_request_row = $1 AND verdict = 'delivered')`, rowID).Scan(&exists); err != nil {
		t.Fatalf("read delivered marker: %v", err)
	}
	return exists
}

// shWithholdFirstDelivery creates one fresh identity's signature and blocks
// its delivery deterministically: the test transaction holds the caller row
// FOR NO KEY UPDATE — the strongest lock that still lets the submit INSERT
// pass its caller_id foreign-key check (FK checks take FOR KEY SHARE, which
// conflicts with FOR UPDATE but not with FOR NO KEY UPDATE). Submit's
// permission read is a plain SELECT, while the delivery region's can_sign
// re-read is FOR SHARE (a conflict with FOR NO KEY UPDATE) — the region waits
// there until the test flips can_sign and commits. The response must be the
// api.md withheld outcome with zero bytes.
func shWithholdFirstDelivery(t *testing.T, h *shHarness, id *shIdentity, body []byte) (int, []byte) {
	t.Helper()
	ctx := h.ctx
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin caller lock: %v", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	var locked int64
	if err := tx.QueryRow(ctx, `SELECT caller_id FROM signer_caller WHERE caller_id = $1 FOR NO KEY UPDATE`, h.callerID).Scan(&locked); err != nil {
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

	// The submit transaction commits the signature before delivery starts;
	// once it is visible, the only remaining work is the blocked region.
	deadline := time.Now().Add(4 * time.Second)
	for {
		var n int
		if err := h.pool.QueryRow(ctx, `SELECT count(*) FROM signature_results s
			JOIN signing_requests r ON r.id = s.signing_request_row
			WHERE r.caller_id = $1 AND r.signing_request_id = $2`, h.callerID, id.requestID).Scan(&n); err != nil {
			t.Fatalf("poll signature_results: %v", err)
		}
		if n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("signature for %s never committed while the delivery was held", id.requestID)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := tx.Exec(ctx, `UPDATE signer_caller SET can_sign = false WHERE caller_id = $1`, h.callerID); err != nil {
		t.Fatalf("flip can_sign: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit caller lock: %v", err)
	}
	res := <-ch
	if res.err != nil {
		t.Fatalf("withheld submit: %v", res.err)
	}
	if err := signer.SetCanSign(ctx, h.pool, h.callerID, true); err != nil {
		t.Fatalf("restore can_sign: %v", err)
	}
	return res.status, res.body
}

// ---------------------------------------------------------------------------
// T043 — the transport acceptance.
// ---------------------------------------------------------------------------

func TestSignerHTTPTransportAcceptance(t *testing.T) {
	ctx := context.Background()
	h := shNewHarness(t, ctx)

	// ---------------------------------------------------------------
	// V1: legal sign over HTTP, bytes proven to traverse the region.
	// ---------------------------------------------------------------
	t.Run("V1 legal submit delivers region-produced bytes", func(t *testing.T) {
		id := h.identity(t, "a", false)
		status, raw := h.post(t, h.body(t, id), h.cred)
		if status != http.StatusOK {
			t.Fatalf("submit status = %d body=%s, want 200", status, raw)
		}
		rowID := shRowID(t, ctx, h.pool, h.callerID, id.requestID)
		if state := shState(t, ctx, h.pool, rowID); state != "signed" {
			t.Fatalf("request state = %q, want signed", state)
		}
		sig, hash := shResult(t, ctx, h.pool, rowID)
		shWantDelivered(t, raw, id.requestID, sig, hash)

		adm := shWaitAdmission(t, ctx, h.pool, rowID, "delivered", 1)
		if adm.deliveredAt == nil {
			t.Fatalf("admission = %+v, want delivered attempt 1 with delivered_at", adm)
		}
		if !shDeliveredMarker(t, ctx, h.pool, rowID) {
			t.Fatal("delivered marker missing after the first response")
		}
		if bytes.Contains(raw, []byte(h.cred)) {
			t.Fatal("response echoes the credential")
		}
		t.Logf("V1: %s signed over HTTP; response bytes == region render of persisted %s; admission attempt=%d",
			id.requestID, sig, adm.attemptSeq)
	})

	// ---------------------------------------------------------------
	// Status: desensitized, ownership-scoped, identical 404.
	// ---------------------------------------------------------------
	t.Run("status is desensitized and ownership-scoped", func(t *testing.T) {
		id := h.identity(t, "status", false)
		if status, raw := h.post(t, h.body(t, id), h.cred); status != http.StatusOK {
			t.Fatalf("seed submit status = %d body=%s", status, raw)
		}
		rowID := shRowID(t, ctx, h.pool, h.callerID, id.requestID)
		shWaitAdmission(t, ctx, h.pool, rowID, "delivered", 1)
		sig, hash := shResult(t, ctx, h.pool, rowID)

		status, raw := h.get(t, id.requestID, h.cred)
		if status != http.StatusOK {
			t.Fatalf("status code = %d body=%s, want 200", status, raw)
		}
		var view struct {
			SigningRequestID string `json:"signing_request_id"`
			State            string `json:"state"`
			ContentHash      string `json:"content_hash"`
			AttemptID        string `json:"attempt_id"`
			IntentID         string `json:"intent_id"`
			PolicyVersion    string `json:"policy_version"`
			TxHash           string `json:"tx_hash"`
			Delivery         *struct {
				Verdict string `json:"verdict"`
			} `json:"delivery"`
		}
		if err := json.Unmarshal(raw, &view); err != nil {
			t.Fatalf("status body is not JSON: %v (%s)", err, raw)
		}
		if view.SigningRequestID != id.requestID || view.State != "signed" || view.ContentHash == "" ||
			view.AttemptID != id.attemptID || view.IntentID != id.intentID || view.PolicyVersion == "" {
			t.Fatalf("status view = %+v, want the recorded facts of %s", view, id.requestID)
		}
		if view.TxHash != hash || view.Delivery == nil || view.Delivery.Verdict != "delivered" {
			t.Fatalf("status tx_hash/delivery = %q/%+v, want the delivered admission facts", view.TxHash, view.Delivery)
		}
		shWantNoSignature(t, raw, sig, "")

		// Another caller's id and a nonexistent id are the identical 404.
		foreignStatus, foreignRaw := h.get(t, id.requestID, h.foreign)
		missingStatus, missingRaw := h.get(t, "sr-t043-does-not-exist", h.cred)
		if foreignStatus != http.StatusNotFound || missingStatus != http.StatusNotFound {
			t.Fatalf("foreign/nonexistent status = %d/%d, want identical 404", foreignStatus, missingStatus)
		}
		foreignBody, missingBody := shDecodeError(t, foreignRaw), shDecodeError(t, missingRaw)
		if foreignBody.Code != "not_found" || foreignBody.Code != missingBody.Code ||
			foreignBody.Message != missingBody.Message {
			t.Fatalf("404 bodies differ: %+v vs %+v", foreignBody, missingBody)
		}
		shWantNoSignature(t, foreignRaw, sig, hash)
		shWantNoSignature(t, missingRaw, sig, hash)
		if bytes.Contains(foreignRaw, []byte(id.requestID)) || bytes.Contains(missingRaw, []byte(id.requestID)) {
			t.Fatal("404 body leaks the request identity")
		}
		// Unauthenticated status is the generic 401, never a 404 difference.
		if unauthStatus, unauthRaw := h.get(t, id.requestID, ""); unauthStatus != http.StatusUnauthorized ||
			shDecodeError(t, unauthRaw).Code != string(signer.ClassUnauthenticated) {
			t.Fatalf("unauthenticated status = %d/%s, want 401 unauthenticated", unauthStatus, unauthRaw)
		}
		t.Logf("status: %s desensitized (no signature bytes; tx_hash only on the delivered admission); foreign/nonexistent are the identical 404",
			id.requestID)
	})

	// ---------------------------------------------------------------
	// Auth/permission refusals: 401 generic, 403 permission.
	// ---------------------------------------------------------------
	t.Run("auth and permission refuse with api.md codes", func(t *testing.T) {
		id := h.identity(t, "auth", false)
		body := h.body(t, id)

		status, raw := h.post(t, body, "")
		if status != http.StatusUnauthorized || shDecodeError(t, raw).Code != string(signer.ClassUnauthenticated) {
			t.Fatalf("missing credential = %d/%s, want 401 unauthenticated", status, raw)
		}
		badStatus, badRaw := h.post(t, body, "txs_not-a-real-credential")
		if badStatus != http.StatusUnauthorized || shDecodeError(t, badRaw).Code != string(signer.ClassUnauthenticated) {
			t.Fatalf("bad credential = %d/%s, want 401 unauthenticated", badStatus, badRaw)
		}
		if bytes.Contains(badRaw, []byte("txs_not-a-real-credential")) {
			t.Fatal("401 body echoes the presented credential")
		}
		permStatus, permRaw := h.post(t, body, h.foreign)
		perm := shDecodeError(t, permRaw)
		if permStatus != http.StatusForbidden || perm.Code != string(signer.ClassSigningNotPermitted) {
			t.Fatalf("can_sign=false submit = %d/%s, want 403 signing_not_permitted", permStatus, permRaw)
		}
		shWantNoSignature(t, permRaw, "", "")
		if n := shRequestRows(t, ctx, h.pool, h.callerID, id.requestID); n != 0 {
			t.Fatalf("permission refusal persisted %d request row(s), want 0", n)
		}
		t.Logf("auth: missing/bad credential = 401 unauthenticated (no echo); can_sign=false = 403 signing_not_permitted with zero signature")
	})

	// ---------------------------------------------------------------
	// Shape refusals: 400 malformed, 422 digest/validation.
	// ---------------------------------------------------------------
	t.Run("shape refusals map to 400 and 422", func(t *testing.T) {
		id := h.identity(t, "shape", false)

		digestBody := []byte(`{"signing_request_id":"sr-t043-digest","digest":"` + shHash("aa") + `"}`)
		digestStatus, digestRaw := h.post(t, digestBody, h.cred)
		if digestStatus != http.StatusUnprocessableEntity ||
			shDecodeError(t, digestRaw).Code != string(signer.ClassArbitraryDigestRejected) {
			t.Fatalf("digest-only = %d/%s, want 422 arbitrary_digest_rejected", digestStatus, digestRaw)
		}

		var body map[string]any
		if err := json.Unmarshal(h.body(t, id), &body); err != nil {
			t.Fatalf("unmarshal legal body: %v", err)
		}
		body["extra_field"] = 1
		unknown, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal unknown-field body: %v", err)
		}
		unknownStatus, unknownRaw := h.post(t, unknown, h.cred)
		if unknownStatus != http.StatusBadRequest ||
			shDecodeError(t, unknownRaw).Code != string(signer.ClassMalformedRequest) {
			t.Fatalf("unknown field = %d/%s, want 400 malformed_request", unknownStatus, unknownRaw)
		}

		var bad shReqJSON
		if err := json.Unmarshal(h.body(t, id), &bad); err != nil {
			t.Fatalf("unmarshal legal body: %v", err)
		}
		bad.Sender = "not-an-address"
		validation, err := json.Marshal(bad)
		if err != nil {
			t.Fatalf("marshal invalid-address body: %v", err)
		}
		validationStatus, validationRaw := h.post(t, validation, h.cred)
		if validationStatus != http.StatusUnprocessableEntity ||
			shDecodeError(t, validationRaw).Code != string(signer.ClassValidationFailed) {
			t.Fatalf("invalid address = %d/%s, want 422 validation_failed", validationStatus, validationRaw)
		}
		t.Logf("shape: digest-only=422 arbitrary_digest_rejected; unknown field=400 malformed_request; bad address=422 validation_failed")
	})

	// ---------------------------------------------------------------
	// Delivery gating 1: can_sign re-read blocks the FIRST response.
	// ---------------------------------------------------------------
	var withheld *shIdentity
	t.Run("first response withholds bytes when the delivery gate blocks", func(t *testing.T) {
		withheld = h.identity(t, "withheld", false)
		status, raw := shWithholdFirstDelivery(t, h, withheld, h.body(t, withheld))

		bodyErr := shDecodeError(t, raw)
		if status != http.StatusConflict || bodyErr.Code != string(signer.ClassSignatureWithheld) {
			t.Fatalf("withheld submit = %d/%s, want 409 signature_withheld", status, raw)
		}
		if bodyErr.Status == nil || bodyErr.Status.Delivery == nil ||
			bodyErr.Status.Delivery.Verdict != "blocked" || bodyErr.Status.Delivery.CanSign {
			t.Fatalf("withheld body status = %+v, want the blocked admission with can_sign=false", bodyErr.Status)
		}
		rowID := shRowID(t, ctx, h.pool, h.callerID, withheld.requestID)
		sig, hash := shResult(t, ctx, h.pool, rowID)
		shWantNoSignature(t, raw, sig, hash)
		adm, ok := shLastAdmission(t, ctx, h.pool, rowID)
		if !ok || adm.verdict != "blocked" || adm.reason != string(signer.ClassSignatureWithheld) ||
			adm.canSign || adm.deliveredAt != nil {
			t.Fatalf("admission = %+v found=%v, want blocked/signature_withheld/can_sign=false with no delivered_at", adm, ok)
		}
		if shDeliveredMarker(t, ctx, h.pool, rowID) {
			t.Fatal("withheld delivery committed a delivered marker")
		}
		t.Logf("withheld: %s persisted a signature (%s) but the region re-gated can_sign=false and wrote ZERO bytes", withheld.requestID, sig)
	})

	// ---------------------------------------------------------------
	// Retry: missing-marker re-gate then idempotent redelivery.
	// ---------------------------------------------------------------
	t.Run("retry re-gates the missing marker then redelivers identically", func(t *testing.T) {
		if withheld == nil {
			t.Skip("first-withheld fixture unavailable")
		}
		rowID := shRowID(t, ctx, h.pool, h.callerID, withheld.requestID)
		sig, hash := shResult(t, ctx, h.pool, rowID)

		status, raw := h.post(t, h.body(t, withheld), h.cred)
		if status != http.StatusOK {
			t.Fatalf("released retry = %d body=%s, want 200 delivered", status, raw)
		}
		shWantDelivered(t, raw, withheld.requestID, sig, hash)
		firstBytes := append([]byte(nil), raw...)
		shWaitAdmission(t, ctx, h.pool, rowID, "delivered", 2)

		// A second same-identity retry is the already-delivered marker path:
		// identical bytes, no new admission, still exactly one signature.
		status2, raw2 := h.post(t, h.body(t, withheld), h.cred)
		if status2 != http.StatusOK || !bytes.Equal(raw2, firstBytes) {
			t.Fatalf("idempotent retry = %d/%s, want the identical delivered bytes", status2, raw2)
		}
		adm, ok := shLastAdmission(t, ctx, h.pool, rowID)
		if !ok || adm.verdict != "delivered" || adm.attemptSeq != 2 || shAdmissionCount(t, ctx, h.pool, rowID) != 2 {
			t.Fatalf("retry admission = %+v found=%v (count=%d), want exactly one new delivered admission at attempt 2",
				adm, ok, shAdmissionCount(t, ctx, h.pool, rowID))
		}
		if sig2, hash2 := shResult(t, ctx, h.pool, rowID); sig2 != sig || hash2 != hash {
			t.Fatalf("retry rewrote the persisted result: %s/%s", sig2, hash2)
		}
		if !shDeliveredMarker(t, ctx, h.pool, rowID) {
			t.Fatal("delivered marker missing after the retry")
		}
		t.Logf("retry: re-gated on the missing marker, delivered attempt=%d, byte-identical redelivery, signature unchanged", adm.attemptSeq)
	})

	// ---------------------------------------------------------------
	// Delivery gating 2: 007 revoke blocks every redelivery (no TTL grace).
	// ---------------------------------------------------------------
	t.Run("revoked grant blocks every redelivery over HTTP", func(t *testing.T) {
		id := h.identity(t, "revoke", false)
		body := h.body(t, id)
		if status, raw := shWithholdFirstDelivery(t, h, id, body); status != http.StatusConflict {
			t.Fatalf("first withheld = %d/%s, want 409", status, raw)
		}
		rowID := shRowID(t, ctx, h.pool, h.callerID, id.requestID)
		sig, hash := shResult(t, ctx, h.pool, rowID)

		// The real PB carrier revokes the grant.
		opID := withdrawalAuthzMintID(t, ctx, h.pbEnv)
		code, stdout, stderr := withdrawalAuthzRun(ctx, h.pbEnv, "revoke",
			"--operation-id", opID, "--authorization-id", id.authzID,
			"--operator", "t043-operator", "--reason", "T043 delivery block: revoke")
		if code != 0 || !strings.Contains(stdout, "action=revoked") {
			t.Fatalf("PB revoke exit=%d stdout=%q stderr=%q, want revoked", code, stdout, stderr)
		}

		status, raw := h.post(t, body, h.cred)
		bodyErr := shDecodeError(t, raw)
		if status != http.StatusConflict || bodyErr.Code != string(signer.ClassSignatureWithheld) {
			t.Fatalf("revoked redelivery = %d/%s, want 409 signature_withheld", status, raw)
		}
		if bodyErr.Status == nil || bodyErr.Status.Delivery == nil ||
			bodyErr.Status.Delivery.Verdict != "blocked" || bodyErr.Status.Delivery.AuthorizationState != "revoked" {
			t.Fatalf("revoked body status = %+v, want the blocked admission with authorization_state=revoked", bodyErr.Status)
		}
		shWantNoSignature(t, raw, sig, hash)

		// No TTL grace: the next same-identity attempt re-gates and blocks again.
		status2, raw2 := h.post(t, body, h.cred)
		if status2 != http.StatusConflict || shDecodeError(t, raw2).Code != string(signer.ClassSignatureWithheld) {
			t.Fatalf("second revoked redelivery = %d/%s, want 409 signature_withheld", status2, raw2)
		}
		shWantNoSignature(t, raw2, sig, hash)
		if n := shAdmissionCount(t, ctx, h.pool, rowID); n != 3 {
			t.Fatalf("admission attempts = %d, want 3 (withheld + two re-gated refusals)", n)
		}
		adm, _ := shLastAdmission(t, ctx, h.pool, rowID)
		if adm.verdict != "blocked" || adm.authzState != "revoked" {
			t.Fatalf("latest admission = %+v, want blocked/revoked", adm)
		}
		if shDeliveredMarker(t, ctx, h.pool, rowID) {
			t.Fatal("revoked redelivery committed a delivered marker")
		}

		// Status is status-only: the latest admission is blocked, so tx_hash
		// disappears even though a persisted result exists (OC-6 inversion).
		stStatus, stRaw := h.get(t, id.requestID, h.cred)
		if stStatus != http.StatusOK {
			t.Fatalf("status after revoke = %d body=%s", stStatus, stRaw)
		}
		var view struct {
			State        string `json:"state"`
			RefusalClass string `json:"refusal_class"`
			TxHash       string `json:"tx_hash"`
			Delivery     *struct {
				Verdict string `json:"verdict"`
			} `json:"delivery"`
		}
		if err := json.Unmarshal(stRaw, &view); err != nil {
			t.Fatalf("status body: %v (%s)", err, stRaw)
		}
		if view.TxHash != "" || view.Delivery == nil || view.Delivery.Verdict != "blocked" ||
			view.RefusalClass != string(signer.ClassSignatureWithheld) {
			t.Fatalf("post-revoke status = %+v, want status-only blocked/signature_withheld with no tx_hash", view)
		}
		shWantNoSignature(t, stRaw, sig, hash)
		if sig2, _ := shResult(t, ctx, h.pool, rowID); sig2 != sig {
			t.Fatalf("revoke rewrote the persisted signature: %s", sig2)
		}
		t.Logf("revoke: %s blocked on every redelivery (attempts=%d), zero bytes, status is status-only (no tx_hash)", id.requestID, 3)
	})

	// ---------------------------------------------------------------
	// Delivery gating 3: 006 pause blocks; release re-delivers.
	// ---------------------------------------------------------------
	t.Run("006 pause blocks over HTTP and release re-delivers", func(t *testing.T) {
		id := h.identity(t, "pause", false)
		body := h.body(t, id)
		if status, raw := shWithholdFirstDelivery(t, h, id, body); status != http.StatusConflict {
			t.Fatalf("first withheld = %d/%s, want 409", status, raw)
		}
		rowID := shRowID(t, ctx, h.pool, h.callerID, id.requestID)
		sig, hash := shResult(t, ctx, h.pool, rowID)

		if _, err := h.pool.Exec(ctx, `INSERT INTO indexer_pause
			(chain_id, height, expected_hash, actual_hash, kind) VALUES ($1, 10, $2, $3, 'hash_mismatch')`,
			shChainID, shHash("aa"), shHash("bb")); err != nil {
			t.Fatalf("insert 006 pause: %v", err)
		}
		t.Cleanup(func() { _, _ = h.pool.Exec(context.Background(), `DELETE FROM indexer_pause`) })

		status, raw := h.post(t, body, h.cred)
		bodyErr := shDecodeError(t, raw)
		if status != http.StatusConflict || bodyErr.Code != string(signer.ClassSignatureWithheld) {
			t.Fatalf("paused redelivery = %d/%s, want 409 signature_withheld", status, raw)
		}
		if bodyErr.Status == nil || bodyErr.Status.Delivery == nil ||
			bodyErr.Status.Delivery.PauseBasis != "indexer_pause" {
			t.Fatalf("paused body status = %+v, want pause_basis=indexer_pause", bodyErr.Status)
		}
		shWantNoSignature(t, raw, sig, hash)

		// Release the pause: the same identity re-gates and delivers the
		// persisted bytes.
		if _, err := h.pool.Exec(ctx, `DELETE FROM indexer_pause`); err != nil {
			t.Fatalf("delete 006 pause: %v", err)
		}
		status2, raw2 := h.post(t, body, h.cred)
		if status2 != http.StatusOK {
			t.Fatalf("released redelivery = %d/%s, want 200 delivered", status2, raw2)
		}
		shWantDelivered(t, raw2, id.requestID, sig, hash)
		adm := shWaitAdmission(t, ctx, h.pool, rowID, "delivered", 3)
		if sig2, hash2 := shResult(t, ctx, h.pool, rowID); sig2 != sig || hash2 != hash {
			t.Fatalf("release rewrote the persisted result: %s/%s", sig2, hash2)
		}
		t.Logf("pause: blocked attempt=%d with pause_basis=indexer_pause; release re-gated and delivered attempt=%d without a re-sign",
			bodyErr.Status.Delivery.AttemptSeq, adm.attemptSeq)
	})
}

// TestSignerHTTPLiveBindingParity pins the untagged production adapter
// (signerlive.go signerBindingResult) to the canonical T028 mapping
// (signer.ClassifyBindingResponse, internal/signer/binding_live.go): the two
// copies must classify the full 008 outcome matrix identically, and the live
// read + scope-lock halves must agree against the real 008 state.
func TestSignerHTTPLiveBindingParity(t *testing.T) {
	ctx := context.Background()

	t.Run("classification matrix agrees with the canonical mapping", func(t *testing.T) {
		open := nonce.ReadAnnotations{
			Gate:          nonce.ReadGate{State: nonce.ReadGateOpen},
			Recovery:      nonce.ReadRecovery{State: nonce.RecoveryNone},
			RegistryState: nonce.RegistryActive,
		}
		cases := []struct {
			name    string
			resp    nonce.ReadResponse
			err     error
			want    signer.BindingResult
			wantErr bool
		}{
			{name: "bound clean", resp: nonce.ReadResponse{Outcome: nonce.ReadBound, Annotations: &open},
				want: signer.BindingMatches},
			{name: "bound gate held", resp: nonce.ReadResponse{Outcome: nonce.ReadBound, Annotations: &nonce.ReadAnnotations{
				Gate:          nonce.ReadGate{State: nonce.ReadGateHeld, Causes: []nonce.ReadGateCause{{HoldID: "hold-1", Cause: "reconcile", EstablishedAt: time.Now()}}},
				Recovery:      nonce.ReadRecovery{State: nonce.RecoveryNone},
				RegistryState: nonce.RegistryActive,
			}}, want: signer.BindingPaused},
			{name: "bound open gate with causes", resp: nonce.ReadResponse{Outcome: nonce.ReadBound, Annotations: &nonce.ReadAnnotations{
				Gate:          nonce.ReadGate{State: nonce.ReadGateOpen, Causes: []nonce.ReadGateCause{{HoldID: "hold-2", Cause: "hold", EstablishedAt: time.Now()}}},
				Recovery:      nonce.ReadRecovery{State: nonce.RecoveryNone},
				RegistryState: nonce.RegistryActive,
			}}, want: signer.BindingPaused},
			{name: "bound recovery recovering", resp: nonce.ReadResponse{Outcome: nonce.ReadBound, Annotations: &nonce.ReadAnnotations{
				Gate:          nonce.ReadGate{State: nonce.ReadGateOpen},
				Recovery:      nonce.ReadRecovery{State: nonce.RecoveryRecovering},
				RegistryState: nonce.RegistryActive,
			}}, want: signer.BindingPaused},
			{name: "bound recovery unknown", resp: nonce.ReadResponse{Outcome: nonce.ReadBound, Annotations: &nonce.ReadAnnotations{
				Gate:          nonce.ReadGate{State: nonce.ReadGateOpen},
				Recovery:      nonce.ReadRecovery{State: nonce.RecoveryUnknown},
				RegistryState: nonce.RegistryActive,
			}}, want: signer.BindingPaused},
			{name: "bound registry disabled", resp: nonce.ReadResponse{Outcome: nonce.ReadBound, Annotations: &nonce.ReadAnnotations{
				Gate:          nonce.ReadGate{State: nonce.ReadGateOpen},
				Recovery:      nonce.ReadRecovery{State: nonce.RecoveryNone},
				RegistryState: nonce.RegistryDisabled,
			}}, want: signer.BindingPaused},
			{name: "bound nil annotations", resp: nonce.ReadResponse{Outcome: nonce.ReadBound},
				want: signer.BindingReadFailed, wantErr: true},
			{name: "terminal", resp: nonce.ReadResponse{Outcome: nonce.ReadTerminal}, want: signer.BindingTerminal},
			{name: "not_bound", resp: nonce.ReadResponse{Outcome: nonce.ReadNotBound}, want: signer.BindingAbsent},
			{name: "mismatch", resp: nonce.ReadResponse{Outcome: nonce.ReadMismatch}, want: signer.BindingConflict},
			{name: "unavailable", resp: nonce.ReadResponse{Outcome: nonce.ReadUnavailable}, want: signer.BindingReadFailed},
			{name: "unauthenticated", resp: nonce.ReadResponse{Outcome: nonce.ReadUnauthenticated},
				want: signer.BindingReadFailed, wantErr: true},
			{name: "unknown outcome", resp: nonce.ReadResponse{Outcome: nonce.Outcome("future_outcome")},
				want: signer.BindingReadFailed, wantErr: true},
			{name: "provider read error", resp: nonce.ReadResponse{Outcome: nonce.ReadBound, Annotations: &open},
				err: errors.New("provider read failed"), want: signer.BindingReadFailed, wantErr: true},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				appClass, appErr := signerBindingResult(tc.resp, tc.err)
				canonClass, canonErr := signer.ClassifyBindingResponse(tc.resp, tc.err)
				if appClass != canonClass || (appErr == nil) != (canonErr == nil) {
					t.Fatalf("app mapping = %v/%v, canonical = %v/%v", appClass, appErr, canonClass, canonErr)
				}
				if appClass != tc.want || (appErr != nil) != tc.wantErr {
					t.Fatalf("mapping = %v/%v, want class %v err=%v", appClass, appErr, tc.want, tc.wantErr)
				}
			})
		}
	})

	t.Run("live reads and scope lock agree with the canonical adapter", func(t *testing.T) {
		h := shNewHarness(t, ctx)
		id := h.identity(t, "parity", false)

		appLive, err := newSignerLiveBinding(h.pool, shReadToken)
		if err != nil || appLive.Binding == nil || appLive.Scope == nil {
			t.Fatalf("newSignerLiveBinding = (%+v, %v), want binding + scope", appLive, err)
		}
		canonical := signer.NewLiveBindingReader(nonce.NewReadProvider(h.pool, shReadToken))

		for _, probe := range []struct {
			name     string
			intentID string
			want     signer.BindingResult
		}{
			{name: "bound", intentID: id.intentID, want: signer.BindingMatches},
			{name: "not_bound", intentID: "pi-t043-parity-missing", want: signer.BindingAbsent},
		} {
			appClass, appErr := appLive.Binding.ReadBinding(ctx, probe.intentID, id.attemptID)
			canonClass, canonErr := canonical.ReadBinding(ctx, probe.intentID, id.attemptID)
			if appClass != canonClass || (appErr == nil) != (canonErr == nil) {
				t.Fatalf("%s: app adapter = %v/%v, canonical = %v/%v", probe.name, appClass, appErr, canonClass, canonErr)
			}
			if appClass != probe.want {
				t.Fatalf("%s: classification = %v, want %v", probe.name, appClass, probe.want)
			}
		}

		// The lock halves must take a real scope-row FOR SHARE: with both
		// adapters holding it in a transaction, a concurrent writer is refused
		// by lock_timeout (55P03) instead of proceeding.
		lockTx, err := h.pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin lock tx: %v", err)
		}
		defer func() { _ = lockTx.Rollback(context.Background()) }()
		if err := appLive.Scope.LockScope(ctx, lockTx, shChainID, h.sender); err != nil {
			t.Fatalf("app LockScope: %v", err)
		}
		if err := canonical.LockScope(ctx, lockTx, shChainID, h.sender); err != nil {
			t.Fatalf("canonical LockScope: %v", err)
		}
		blocked, err := h.pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin concurrent writer: %v", err)
		}
		defer func() { _ = blocked.Rollback(context.Background()) }()
		if _, err := blocked.Exec(ctx, `SET LOCAL lock_timeout = '300ms'`); err != nil {
			t.Fatalf("set lock_timeout: %v", err)
		}
		_, err = blocked.Exec(ctx, `UPDATE nonce_scope_state SET reconciled_floor = reconciled_floor
			WHERE chain_id = $1 AND sender = $2`, shChainID, h.sender)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "55P03" {
			t.Fatalf("concurrent scope writer = %v, want a lock_timeout refusal (55P03) while the adapters hold FOR SHARE", err)
		}
		t.Logf("live parity: bound/not_bound classes agree; both lock halves hold the real scope-row FOR SHARE (writer refused 55P03)")
	})
}
