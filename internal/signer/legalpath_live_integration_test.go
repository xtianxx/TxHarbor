//go:build integration

// legalpath_live_integration_test.go owns spec task T040 for 009-signer-service:
// the H5 full legal-path acceptance, joint with the delivered PB carrier
// (handoff-009.md H5; tasks T036-T039 + T028; SC-06/SC-08; Q-A/Q-B).
//
// What it proves on the merged tree:
//
//   - the grant is minted by the REAL PB supply entry (internal/app
//     `withdrawal-authz`, driven in-process exactly as its own integration
//     tests do: mint → supply with the issuing credential resolved through the
//     deployment allowlist), so the carrier row is written by
//     withdrawal.SupplyGrantAuthorized with its audited principal — never a
//     hand-forged scope row;
//   - the binding is the LIVE 008 one (T028 stack: operator registry_register
//   - Allocator.Allocate + ReadProvider) read through the production
//     LiveBindingReader adapter — never a contract-shape double;
//   - the legal path 009 submit → delivery passes end to end with zero
//     authorization_unverifiable: the supplied scope's content, version,
//     sender, fee caps and the intent + signing-request linkage are all
//     verified, the signature is persisted before any response byte, and
//     delivery re-checks the same live grant/binding before handing out
//     byte-identical bytes (never a re-sign);
//   - the permitted fee replacement reuses the grant (OC-5 branch 1): the new
//     identity is persisted as a replacement of the seeded anchor under the
//     same grant, one 008 binding backs both attempts (same intent, same
//     nonce — no second intent/nonce), the purpose token + fee caps admit it,
//     and the replacement signs AND delivers;
//   - the forbidden reuse refuses per identity (supplied scope without the
//     purpose token): a rejected replacement row + authorization_refused audit
//     naming the anchor, zero signatures, the anchor row and the partial
//     one-anchor-per-grant index intact;
//   - a scopeless stock grant still refuses authorization_unverifiable per
//     request (PB-FR-04) — the joint V-PB3-009 behavior, alongside the
//     existing gates_integration_test.go case which stays green;
//   - revoke / expiry / 006 pause each block delivery AFTER a successful
//     sign: status-only, zero bytes, observed basis recorded in the admission
//     and audit, and never a re-sign;
//   - same-identity retry and unknown recovery converge on the persisted
//     result: a failed delivery write reports outcome_unknown (never "nothing
//     delivered"), the same-identity redelivery re-passes the live gates and
//     hands out byte-identical payloads, and signature_results is never
//     rewritten (no TTL/grace: every attempt re-gates).
//
// Why this file lives in signer_test (external test package): the real supply
// entry is internal/app, which imports internal/signer, so only an external
// test package can import both without the app -> signer test cycle — the same
// reason credential_lifecycle_integration_test.go lives here, whose isolated-PG
// harness (credStartPG/credEnv) this file reuses. The internal/nonce import
// stays in this test and in the integration-tagged production adapter
// (binding_live.go); internal/signer's default production graph stays RPC-free
// and T023's boundary assertion stays green.
package signer_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
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
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/app"
	"github.com/xtianxx/txharbor/internal/nonce"
	"github.com/xtianxx/txharbor/internal/signer"
	"github.com/xtianxx/txharbor/internal/withdrawal"
)

const (
	lpChainID   = int64(31337)
	lpReadToken = "lp-nonce-read-token"
	lpAsset     = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	lpRecipient = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	lpAmount    = "1000000"
	lpGasLimit  = "65000"
)

// lpHash is a syntactically valid 32-byte 0x hash for 006 fixture rows.
func lpHash(fill string) string { return "0x" + strings.Repeat(fill, 32) }

// ---------------------------------------------------------------------------
// Live 008 stack (the T028 wiring, rebuilt in the external package: the
// package-signer helpers are not importable here).
// ---------------------------------------------------------------------------

// lpChainView is the scripted 008 chain view: a healthy, consistent scope
// (latest == pending == 0, one head) so a fresh scope admits nonce 0. It is
// the only "substitute" on this path and it substitutes no 009/008 behavior —
// the binding read under test reaches no RPC.
type lpChainView struct {
	mu      sync.Mutex
	latest  string
	pending string
}

func newLPChainView() *lpChainView { return &lpChainView{latest: "0x0", pending: "0x0"} }

func (v *lpChainView) CallContext(_ context.Context, result any, method string, args ...any) error {
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
		return fmt.Errorf("lpChainView: unexpected method %q", method)
	}
}

func lpObserver(view *lpChainView) *nonce.Observer {
	return nonce.NewObserver(view, nonce.ObserverConfig{
		RPCTimeout:   2 * time.Second,
		RetryInitial: time.Millisecond,
		RetryMax:     5 * time.Millisecond,
	})
}

// lpAllocator wires 008's admission core over the scripted chain view and an
// explicitly opened rebuild gate (008's own test wiring).
func lpAllocator(pool *pgxpool.Pool, view *lpChainView) *nonce.Allocator {
	gate := nonce.NewRebuildGate()
	gate.Open()
	return nonce.NewAllocator(pool, lpObserver(view), gate)
}

// lpReadProvider returns the live 008 read provider the adapter consumes.
func lpReadProvider(pool *pgxpool.Pool) *nonce.ReadProvider {
	gate := nonce.NewRebuildGate()
	gate.Open()
	return nonce.NewReadProvider(pool, lpReadToken, gate)
}

// lpRegistry registers the fixture sender through 008's operator API.
func lpRegistry(t *testing.T, ctx context.Context, pool *pgxpool.Pool, n int, sender string) {
	t.Helper()
	res, err := nonce.NewAdminRunner(pool, nil).Run(ctx, nonce.AdminRequest{
		Action: nonce.AdminActionRegistryRegister, OperationID: "lp-reg-" + strconv.Itoa(n),
		ChainID: lpChainID, Sender: sender,
	})
	if err != nil {
		t.Fatalf("008 registry_register: %v", err)
	}
	if res.Outcome != nonce.AdminApplied {
		t.Fatalf("008 registry_register = %q, want applied", res.Outcome)
	}
}

// ---------------------------------------------------------------------------
// Real PB supply entry (internal/app withdrawal-authz) + env.
// ---------------------------------------------------------------------------

// lpRun drives the real PB supply/revoke carrier in-process (no shell-out),
// mirroring withdrawalAuthzRun in internal/app's own integration test.
func lpRun(ctx context.Context, env map[string]string, args ...string) (int, string, string) {
	var stdout, stderr bytes.Buffer
	code := app.WithdrawalAuthz(ctx, args, app.Deps{
		Getenv: func(k string) (string, bool) { v, ok := env[k]; return v, ok },
		Stdout: &stdout,
		Stderr: &stderr,
	})
	return code, stdout.String(), stderr.String()
}

// lpEnv is the T018/J6 serve env (credEnv) plus the issuance allowlist mapping
// this fixture's caller as an authorized issuer.
func lpEnv(base map[string]string, callerID int64) map[string]string {
	env := make(map[string]string, len(base)+1)
	for k, v := range base {
		env[k] = v
	}
	env[withdrawal.EnvIssuerCallers] = strconv.FormatInt(callerID, 10)
	return env
}

// lpMintID mints one opaque attempt id through the real carrier.
func lpMintID(t *testing.T, ctx context.Context, env map[string]string) string {
	t.Helper()
	code, stdout, stderr := lpRun(ctx, env, "mint")
	if code != 0 {
		t.Fatalf("mint exit code = %d, want 0; stderr=%s", code, stderr)
	}
	id := strings.TrimSpace(stdout)
	if len(id) != 32 {
		t.Fatalf("mint stdout = %q, want one 32-hex id", stdout)
	}
	return id
}

// lpSeedOpts is the supplied-grant shape: scopeless (legacy stock) or a scoped
// carrier with the given caps / purpose token / expiry.
type lpSeedOpts struct {
	scoped               bool
	allowsFeeReplacement bool
	feeMaxTotal          int64
	feeMaxPerGas         int64
	feeMaxPriority       int64
	expiresAt            *time.Time
}

// lpSupply mints and supplies the fixture's grant through the REAL PB entry.
func lpSupply(t *testing.T, ctx context.Context, f *lpFixture, opts lpSeedOpts) {
	t.Helper()
	opID := lpMintID(t, ctx, f.env)
	args := []string{
		"supply", "--operation-id", opID,
		"--authorization-id", f.authzID,
		"--caller-id", strconv.FormatInt(f.callerID, 10),
		"--chain-id", strconv.FormatInt(lpChainID, 10),
		"--asset", lpAsset,
		"--recipient", lpRecipient,
		"--amount", lpAmount,
		"--operator", "h5-operator",
		"--reason", "T040 H5 joint legal path",
	}
	if opts.scoped {
		args = append(args,
			"--api-key", f.apiKey,
			"--intent-id", f.intentID,
			"--request-id", f.requestID,
			"--sender", f.sender,
			"--fee-max-total", strconv.FormatInt(opts.feeMaxTotal, 10),
			"--fee-max-per-gas", strconv.FormatInt(opts.feeMaxPerGas, 10),
			"--fee-max-priority", strconv.FormatInt(opts.feeMaxPriority, 10),
		)
		if opts.allowsFeeReplacement {
			args = append(args, "--allows-fee-replacement")
		}
	}
	if opts.expiresAt != nil {
		args = append(args, "--expires-at", opts.expiresAt.UTC().Format(time.RFC3339))
	}
	code, stdout, stderr := lpRun(ctx, f.env, args...)
	if code != 0 || !strings.Contains(stdout, "action=supplied") {
		t.Fatalf("PB supply exit code = %d stdout=%q stderr=%q, want supplied", code, stdout, stderr)
	}
}

// ---------------------------------------------------------------------------
// 009 fixture: caller + credential + supplied grant + live binding + deps.
// ---------------------------------------------------------------------------

type lpFixture struct {
	callerID  int64
	authzID   string
	intentID  string
	requestID string
	attemptID string
	sender    string
	bindingID string
	nonce     string
	cred      string
	apiKey    string
	env       map[string]string
	submit    signer.SubmitDeps
	deliver   signer.DeliveryDeps
}

// lpSeed builds one isolated live fixture: 009 caller/credential first, the
// grant through the real PB entry, then the 008 registry + binding through
// 008's own operator/admission APIs, then the 009 submit/delivery deps over
// the production live adapter.
func lpSeed(t *testing.T, ctx context.Context, pool *pgxpool.Pool, alloc *nonce.Allocator,
	live *signer.LiveBindingReader, baseEnv map[string]string, n int, opts lpSeedOpts) *lpFixture {
	t.Helper()
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	hexKey := common.Bytes2Hex(crypto.FromECDSA(key))
	sender := strings.ToLower(crypto.PubkeyToAddress(key.PublicKey).Hex())
	f := &lpFixture{
		callerID:  int64(19000 + n),
		authzID:   "wa-h5-" + strconv.Itoa(n),
		intentID:  "pi-h5-" + strconv.Itoa(n),
		requestID: "sr-h5-" + strconv.Itoa(n),
		attemptID: "at-h5-" + strconv.Itoa(n),
		sender:    sender,
	}
	// IssueKey creates the shared caller row and the issuing credential read
	// by the supply carrier; IssueCredential is the 009-side auth material.
	apiKey, _, err := withdrawal.IssueKey(ctx, pool, f.callerID, "h5-issuer-"+strconv.Itoa(n))
	if err != nil {
		t.Fatalf("IssueKey: %v", err)
	}
	cred, err := signer.IssueCredential(ctx, pool, f.callerID, "h5-"+strconv.Itoa(n))
	if err != nil {
		t.Fatalf("IssueCredential: %v", err)
	}
	f.apiKey, f.cred = apiKey, cred
	f.env = lpEnv(baseEnv, f.callerID)

	lpSupply(t, ctx, f, opts)

	lpRegistry(t, ctx, pool, n, f.sender)
	binding, outcome, err := alloc.Allocate(ctx, nonce.AllocationRequest{
		IntentID: f.intentID, ChainID: lpChainID, Sender: f.sender, AuthorizationID: f.authzID,
	})
	if err != nil || outcome != nonce.OutcomeAllocated || binding == nil {
		t.Fatalf("008 allocate = (%v, %q, %v), want allocated", binding, outcome, err)
	}
	f.bindingID, f.nonce = binding.BindingID, binding.Nonce.String()

	policy, err := signer.NewPolicy(signer.PolicyConfig{
		ChainIDs:             []int64{lpChainID},
		Senders:              []string{f.sender},
		Assets:               []string{lpAsset},
		Recipients:           []string{lpRecipient},
		MaxAmount:            big.NewInt(2000000),
		MaxGasLimit:          100000,
		MaxFeePerGas:         big.NewInt(2000000000),
		MaxPriorityFeePerGas: big.NewInt(1500000000),
		MaxGasPrice:          big.NewInt(2000000000),
	})
	if err != nil {
		t.Fatalf("NewPolicy: %v", err)
	}
	keyFile := filepath.Join(t.TempDir(), "h5.key")
	if err := os.WriteFile(keyFile, []byte(hexKey), 0o600); err != nil {
		t.Fatalf("write key file: %v", err)
	}
	provider, err := signer.NewDevKeyProvider(signer.ModeDevelopment, keyFile, 0)
	if err != nil {
		t.Fatalf("NewDevKeyProvider: %v", err)
	}
	if provider.Address() != common.HexToAddress(f.sender) {
		t.Fatalf("provider bound to %s, want the fixture sender %s", provider.Address(), f.sender)
	}
	f.submit = signer.SubmitDeps{DB: pool, Policy: policy, Provider: provider, Binding: live, ScopeLock: live}
	f.deliver = signer.DeliveryDeps{DB: pool, Binding: live, ScopeLock: live}
	return f
}

// lpReqJSON is the exact api.md §2 submit body (no unknown keys, fee shape per
// tx_type 2).
type lpReqJSON struct {
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

// lpBody renders one compliant type-2 transfer request for the fixture.
func (f *lpFixture) lpBody(t *testing.T, requestID, attemptID, maxFee, maxPriority string) []byte {
	t.Helper()
	body, err := json.Marshal(lpReqJSON{
		SigningRequestID: requestID, AttemptID: attemptID,
		IntentID: f.intentID, BindingRef: f.bindingID,
		RecoveryVersion: 0, ChainID: uint64(lpChainID),
		Sender: f.sender, Nonce: f.nonce, TxType: 2,
		To: lpAsset, Value: "0", Data: lpTransferData(),
		GasLimit: lpGasLimit, MaxFeePerGas: maxFee, MaxPriorityFeePerGas: maxPriority,
		Asset: lpAsset, Recipient: lpRecipient, Amount: lpAmount,
		AuthorizationID: f.authzID,
	})
	if err != nil {
		t.Fatalf("marshal submit body: %v", err)
	}
	return body
}

// lpDefaultBody is the fixture's first-attempt body (base fee shape).
func (f *lpFixture) lpDefaultBody(t *testing.T) []byte {
	t.Helper()
	return f.lpBody(t, f.requestID, f.attemptID, "1500000000", "1000000000")
}

// lpTransferData is transfer(recipient, 1000000) calldata.
func lpTransferData() string {
	return "0xa9059cbb" + strings.Repeat("0", 24) + strings.TrimPrefix(lpRecipient, "0x") +
		fmt.Sprintf("%064x", big.NewInt(1000000))
}

// ---------------------------------------------------------------------------
// Submit / deliver drivers + row probes.
// ---------------------------------------------------------------------------

func lpSubmit(t *testing.T, ctx context.Context, f *lpFixture, body []byte) *signer.SubmitResponse {
	t.Helper()
	resp, err := signer.Submit(ctx, f.submit, f.cred, body)
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if resp == nil {
		t.Fatal("Submit returned no response")
	}
	return resp
}

func lpRefusal(t *testing.T, err error, want signer.RefusalClass) *signer.RefusalError {
	t.Helper()
	var re *signer.RefusalError
	if !errors.As(err, &re) {
		t.Fatalf("error %v is not a *signer.RefusalError", err)
	}
	if re.Class != want {
		t.Fatalf("refusal class = %s, want %s (%v)", re.Class, want, err)
	}
	return re
}

// lpSink is the abstract byte sink (the local transport): it records every
// payload and optionally fails the write (the unknown-outcome trigger).
type lpSink struct {
	mu       sync.Mutex
	payloads [][]byte
	fail     bool
}

func (s *lpSink) WriteDelivery(_ context.Context, payload []byte) error {
	s.mu.Lock()
	s.payloads = append(s.payloads, append([]byte(nil), payload...))
	s.mu.Unlock()
	if s.fail {
		return errors.New("lp: scripted transport failure")
	}
	return nil
}

func (s *lpSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.payloads)
}

func (s *lpSink) last() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.payloads) == 0 {
		return nil
	}
	return s.payloads[len(s.payloads)-1]
}

func lpDeliver(t *testing.T, ctx context.Context, f *lpFixture, sink *lpSink) (*signer.DeliveryResult, error) {
	t.Helper()
	return signer.Deliver(ctx, f.deliver, signer.Caller{ID: f.callerID, CanSign: true}, f.requestID, sink)
}

func lpExec(t *testing.T, ctx context.Context, pool *pgxpool.Pool, query string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(ctx, query, args...); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

// lpRowFacts is one persisted 009 request identity.
type lpRowFacts struct {
	rowID         int64
	state         string
	refusalClass  string
	authzID       string
	authzState    string
	authVersion   *int64
	replacementOf *int64
	nonce         string
	bindingRef    string
}

func lpRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, callerID int64, requestID string) lpRowFacts {
	t.Helper()
	var f lpRowFacts
	if err := pool.QueryRow(ctx, `SELECT id, state, refusal_class, authorization_id, authorization_state,
		authorization_version, replacement_of, nonce::text, binding_ref
		FROM signing_requests WHERE caller_id = $1 AND signing_request_id = $2`,
		callerID, requestID).Scan(&f.rowID, &f.state, &f.refusalClass, &f.authzID, &f.authzState,
		&f.authVersion, &f.replacementOf, &f.nonce, &f.bindingRef); err != nil {
		t.Fatalf("read signing_requests %s: %v", requestID, err)
	}
	return f
}

// lpScopeFacts is one PB carrier row (as supplied by the real entry).
type lpScopeFacts struct {
	intentID, requestID, sender               string
	feeMaxTotal, feeMaxPerGas, feeMaxPriority int64
	allowsFeeReplacement                      bool
	version                                   int64
	attestedBy                                string
}

func lpScope(t *testing.T, ctx context.Context, pool *pgxpool.Pool, authzID string) (lpScopeFacts, bool) {
	t.Helper()
	var s lpScopeFacts
	err := pool.QueryRow(ctx, `SELECT intent_id, request_id, sender, fee_max_total, fee_max_per_gas,
		fee_max_priority, allows_fee_replacement, authorization_version, attested_by
		FROM withdrawal_authorization_scopes WHERE authorization_id = $1`, authzID).Scan(
		&s.intentID, &s.requestID, &s.sender, &s.feeMaxTotal, &s.feeMaxPerGas,
		&s.feeMaxPriority, &s.allowsFeeReplacement, &s.version, &s.attestedBy)
	if errors.Is(err, pgx.ErrNoRows) {
		return s, false
	}
	if err != nil {
		t.Fatalf("read scope for %s: %v", authzID, err)
	}
	return s, true
}

// lpSignatures reports the persisted result count and values for one row.
func lpSignatures(t *testing.T, ctx context.Context, pool *pgxpool.Pool, rowID int64) (int, string, string) {
	t.Helper()
	var n int
	var sig, hash string
	if err := pool.QueryRow(ctx, `SELECT count(*), COALESCE(MIN(signature), ''), COALESCE(MIN(tx_hash), '')
		FROM signature_results WHERE signing_request_row = $1`, rowID).Scan(&n, &sig, &hash); err != nil {
		t.Fatalf("read signature_results: %v", err)
	}
	return n, sig, hash
}

// lpWantPersistedResult pins the never-re-sign claim: exactly one result row
// and it carries the expected facts.
func lpWantPersistedResult(t *testing.T, ctx context.Context, pool *pgxpool.Pool, rowID int64, sig, hash string) {
	t.Helper()
	n, gotSig, gotHash := lpSignatures(t, ctx, pool, rowID)
	if n != 1 || gotSig != sig || gotHash != hash {
		t.Fatalf("persisted result = %d rows sig=%q hash=%q, want 1 row %q/%q", n, gotSig, gotHash, sig, hash)
	}
}

// lpWantNoResult pins the zero-signature claim on refusal paths.
func lpWantNoResult(t *testing.T, ctx context.Context, pool *pgxpool.Pool, rowID int64) {
	t.Helper()
	if n, sig, hash := lpSignatures(t, ctx, pool, rowID); n != 0 || sig != "" || hash != "" {
		t.Fatalf("refusal path produced %d result row(s) (%q/%q), want 0", n, sig, hash)
	}
}

func lpLastAudit(t *testing.T, ctx context.Context, pool *pgxpool.Pool, requestID string) (action, reason, detail string) {
	t.Helper()
	if err := pool.QueryRow(ctx, `SELECT action, reason_class, detail FROM signing_request_audit
		WHERE signing_request_id = $1 ORDER BY audit_id DESC LIMIT 1`, requestID).Scan(&action, &reason, &detail); err != nil {
		t.Fatalf("read last audit for %s: %v", requestID, err)
	}
	return action, reason, detail
}

func lpAuditCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, requestID, reasonClass string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM signing_request_audit
		WHERE signing_request_id = $1 AND reason_class = $2`, requestID, reasonClass).Scan(&n); err != nil {
		t.Fatalf("count audits for %s: %v", requestID, err)
	}
	return n
}

// lpUnverifiable counts the caller's authorization_unverifiable audits: the
// legal-path acceptance requires zero.
func lpUnverifiable(t *testing.T, ctx context.Context, pool *pgxpool.Pool, callerID int64) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM signing_request_audit
		WHERE caller_id = $1 AND reason_class = 'authorization_unverifiable'`, callerID).Scan(&n); err != nil {
		t.Fatalf("count unverifiable audits: %v", err)
	}
	return n
}

func lpNonAnchorCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, authzID string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM signing_requests
		WHERE authorization_id = $1 AND replacement_of IS NULL`, authzID).Scan(&n); err != nil {
		t.Fatalf("count anchor requests for %s: %v", authzID, err)
	}
	return n
}

func lpBindingCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, intentID string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM nonce_bindings WHERE intent_id = $1`, intentID).Scan(&n); err != nil {
		t.Fatalf("count 008 bindings for %s: %v", intentID, err)
	}
	return n
}

// lpAdmissionFacts is the last delivery admission snapshot for one request.
type lpAdmissionFacts struct {
	verdict      string
	bindingClass string
	reason       string
	pauseBasis   string
	authzState   string
	deliveredAt  *time.Time
}

func lpLastAdmission(t *testing.T, ctx context.Context, pool *pgxpool.Pool, rowID int64) (lpAdmissionFacts, bool) {
	t.Helper()
	var a lpAdmissionFacts
	err := pool.QueryRow(ctx, `SELECT verdict, binding_class, reason, pause_basis, authorization_state, delivered_at
		FROM delivery_admissions WHERE signing_request_row = $1 ORDER BY attempt_seq DESC LIMIT 1`, rowID).Scan(
		&a.verdict, &a.bindingClass, &a.reason, &a.pauseBasis, &a.authzState, &a.deliveredAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return a, false
	}
	if err != nil {
		t.Fatalf("read delivery_admissions: %v", err)
	}
	return a, true
}

func lpAdmissionCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, rowID int64) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM delivery_admissions WHERE signing_request_row = $1`, rowID).Scan(&n); err != nil {
		t.Fatalf("count delivery_admissions: %v", err)
	}
	return n
}

func lpDeliveredMarker(t *testing.T, ctx context.Context, pool *pgxpool.Pool, rowID int64) bool {
	t.Helper()
	var exists bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM delivery_admissions
		WHERE signing_request_row = $1 AND verdict = 'delivered')`, rowID).Scan(&exists); err != nil {
		t.Fatalf("read delivered marker: %v", err)
	}
	return exists
}

func lpPayloadFacts(t *testing.T, payload []byte) map[string]string {
	t.Helper()
	var m map[string]string
	if err := json.Unmarshal(payload, &m); err != nil {
		t.Fatalf("delivery payload is not JSON: %v", err)
	}
	return m
}

// lpSeedAnchor writes one predecessor (anchor) request row directly: the
// replacement shares the anchor's caller + intent_id + binding_ref + grant
// while the grant's carrier binds the request identity that actually submits
// (the replacement). The PB carrier content is immutable, and this anchor
// needed no signature of its own — it is the predecessor attempt the
// replacement supersedes (research R7: "a replacement reusing the anchor's
// grant MUST declare the anchor's intent_id (and the same binding_ref)").
func lpSeedAnchor(t *testing.T, ctx context.Context, pool *pgxpool.Pool, f *lpFixture, requestID, attemptID string) int64 {
	t.Helper()
	var id int64
	err := pool.QueryRow(ctx, `INSERT INTO signing_requests
		(caller_id, signing_request_id, attempt_id, intent_id, binding_ref, recovery_version,
		 chain_id, sender, nonce, tx_type, to_addr, value, data, gas_limit,
		 max_fee_per_gas, max_priority_fee_per_gas, asset, recipient, amount,
		 canonical_envelope, content_hash, authorization_id, authorization_fingerprint,
		 authorization_state, policy_version, state)
		VALUES ($1,$2,$3,$4,$5,0,$6,$7,$8,2,$9,'0',$10,$11,
		 $12,$13,$14,$15,$16,'0x00',$17,$18,$19,'active','h5-anchor','received')
		RETURNING id`,
		f.callerID, requestID, attemptID, f.intentID, f.bindingID,
		lpChainID, f.sender, f.nonce, lpAsset, []byte{},
		lpGasLimit, "1500000000", "1000000000", lpAsset, lpRecipient, lpAmount,
		lpHash("ab"), f.authzID, strings.Repeat("ab", 32)).Scan(&id)
	if err != nil {
		t.Fatalf("seed anchor row: %v", err)
	}
	return id
}

// ---------------------------------------------------------------------------
// T040 — the joint acceptance.
// ---------------------------------------------------------------------------

func TestSignerLegalPathH5(t *testing.T) {
	pool, dsn := credStartPG(t)
	ctx := context.Background()
	baseEnv := credEnv(dsn)
	view := newLPChainView()
	alloc := lpAllocator(pool, view)
	readProvider := lpReadProvider(pool)
	live := signer.NewLiveBindingReader(readProvider)

	t.Run("legal first sign passes submit and delivery with zero authorization_unverifiable", func(t *testing.T) {
		f := lpSeed(t, ctx, pool, alloc, live, baseEnv, 1, lpSeedOpts{
			scoped: true, feeMaxTotal: 100000000000000, feeMaxPerGas: 2000000000, feeMaxPriority: 1500000000,
		})
		body := f.lpDefaultBody(t)
		resp := lpSubmit(t, ctx, f, body)

		row := lpRow(t, ctx, pool, f.callerID, f.requestID)
		if row.state != "signed" || row.refusalClass != "" || row.authzID != f.authzID || row.replacementOf != nil {
			t.Fatalf("persisted row = state %q class %q authz %q replacement_of %v, want signed/''/%s/nil",
				row.state, row.refusalClass, row.authzID, row.replacementOf, f.authzID)
		}
		if row.authVersion == nil || *row.authVersion != 1 {
			t.Fatalf("persisted authorization_version = %v, want 1", row.authVersion)
		}
		if row.nonce != f.nonce || row.bindingRef != f.bindingID {
			t.Fatalf("persisted nonce/binding_ref = %q/%q, want %q/%q", row.nonce, row.bindingRef, f.nonce, f.bindingID)
		}

		scope, found := lpScope(t, ctx, pool, f.authzID)
		if !found {
			t.Fatal("supplied grant has no carrier row; the real PB supply did not land")
		}
		if scope.intentID != f.intentID || scope.requestID != f.requestID || scope.sender != f.sender ||
			scope.version != 1 || scope.allowsFeeReplacement {
			t.Fatalf("scope = intent %q request %q sender %q version %d allows %v; want %q/%q/%q/1/false",
				scope.intentID, scope.requestID, scope.sender, scope.version, scope.allowsFeeReplacement,
				f.intentID, f.requestID, f.sender)
		}
		if scope.feeMaxTotal <= 0 || scope.feeMaxPerGas <= 0 || scope.feeMaxPriority <= 0 {
			t.Fatalf("scope fee caps = %d/%d/%d, want the supplied positive triple",
				scope.feeMaxTotal, scope.feeMaxPerGas, scope.feeMaxPriority)
		}
		if !strings.Contains(scope.attestedBy, "caller:"+strconv.FormatInt(f.callerID, 10)) {
			t.Fatalf("scope attested_by = %q, want the resolved issuing principal", scope.attestedBy)
		}

		lpWantPersistedResult(t, ctx, pool, row.rowID, resp.Signature, resp.TxHash)
		if n := lpUnverifiable(t, ctx, pool, f.callerID); n != 0 {
			t.Fatalf("legal path recorded %d authorization_unverifiable audit(s), want 0", n)
		}
		if n := lpAuditCount(t, ctx, pool, f.requestID, "authorization_unverifiable"); n != 0 {
			t.Fatalf("legal request recorded %d authorization_unverifiable audit(s), want 0", n)
		}

		sink := &lpSink{}
		res, err := lpDeliver(t, ctx, f, sink)
		if err != nil {
			t.Fatalf("Deliver: %v", err)
		}
		if res.Verdict != signer.VerdictDelivered || res.AttemptSeq != 1 || sink.count() != 1 {
			t.Fatalf("delivery = %+v sink=%d, want delivered attempt 1 with one payload", res, sink.count())
		}
		facts := lpPayloadFacts(t, sink.last())
		if facts["signature"] != resp.Signature || facts["tx_hash"] != resp.TxHash ||
			facts["signing_request_id"] != f.requestID || facts["delivery"] != "delivered" {
			t.Fatalf("payload facts = %v, want the persisted signature/hash facts", facts)
		}
		adm, ok := lpLastAdmission(t, ctx, pool, row.rowID)
		if !ok || adm.verdict != "delivered" || adm.bindingClass != "matches" || adm.deliveredAt == nil {
			t.Fatalf("admission = %+v found=%v, want delivered/matches with a delivered_at", adm, ok)
		}
		if !lpDeliveredMarker(t, ctx, pool, row.rowID) {
			t.Fatal("delivered marker missing after a successful delivery")
		}
		lpWantPersistedResult(t, ctx, pool, row.rowID, resp.Signature, resp.TxHash)
		t.Logf("legal first sign: scope version=%d sender=%s intent=%s request=%s; row state=%s authorization_version=%d; delivery verdict=%s attempt=%d binding_class=%s signature=%s",
			scope.version, scope.sender, scope.intentID, scope.requestID, row.state, *row.authVersion,
			adm.verdict, res.AttemptSeq, adm.bindingClass, resp.Signature)

		// 009 is read-only on 008 state: the live binding is untouched.
		after, aerr := readProvider.ReadByBindingID(ctx, f.bindingID)
		if aerr != nil || after.Outcome != nonce.ReadBound || after.Binding == nil ||
			after.Binding.State != nonce.StateAllocated || after.Binding.Nonce != f.nonce {
			t.Fatalf("post-delivery 008 read = (%q, %+v, %v), want bound/allocated nonce %s",
				after.Outcome, after.Binding, aerr, f.nonce)
		}

		// Same-identity retry converges on the persisted result: no second
		// signature and no new identity.
		resp2 := lpSubmit(t, ctx, f, body)
		if resp2.Signature != resp.Signature || resp2.TxHash != resp.TxHash {
			t.Fatalf("same-identity retry = %+v, want the persisted %s/%s", resp2, resp.Signature, resp.TxHash)
		}
		lpWantPersistedResult(t, ctx, pool, row.rowID, resp.Signature, resp.TxHash)
	})

	t.Run("permitted fee replacement reuses the supplied grant", func(t *testing.T) {
		f := lpSeed(t, ctx, pool, alloc, live, baseEnv, 2, lpSeedOpts{
			scoped: true, allowsFeeReplacement: true,
			feeMaxTotal: 200000000000000, feeMaxPerGas: 2000000000, feeMaxPriority: 1500000000,
		})
		anchorID := lpSeedAnchor(t, ctx, pool, f, "sr-h5-rep-anchor-2", "at-h5-rep-anchor-2")

		// The replacement is a new attempt (new signing_request_id +
		// attempt_id, OC-4) over the same intent + binding, fee raised within
		// the carrier's caps.
		body := f.lpBody(t, f.requestID, f.attemptID, "1800000000", "1200000000")
		resp := lpSubmit(t, ctx, f, body)

		row := lpRow(t, ctx, pool, f.callerID, f.requestID)
		if row.state != "signed" || row.authzID != f.authzID {
			t.Fatalf("replacement = state %q authz %q, want signed/%s", row.state, row.authzID, f.authzID)
		}
		if row.replacementOf == nil || *row.replacementOf != anchorID {
			t.Fatalf("replacement_of = %v, want the anchor row %d", row.replacementOf, anchorID)
		}
		if row.authVersion == nil || *row.authVersion != 1 {
			t.Fatalf("replacement authorization_version = %v, want 1", row.authVersion)
		}
		if n := lpAuditCount(t, ctx, pool, f.requestID, "authorization_invalid"); n != 0 {
			t.Fatalf("permitted reuse recorded %d authorization_invalid audit(s), want 0", n)
		}
		var reuseRefusals int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM signing_request_audit
			WHERE signing_request_id = $1 AND detail LIKE '%fresh_authorization_required%'`, f.requestID).Scan(&reuseRefusals); err != nil {
			t.Fatalf("count reuse-gate refusals: %v", err)
		}
		if reuseRefusals != 0 {
			t.Fatalf("permitted reuse recorded %d fresh-authorization refusal(s), want 0", reuseRefusals)
		}
		lpWantPersistedResult(t, ctx, pool, row.rowID, resp.Signature, resp.TxHash)
		if n := lpUnverifiable(t, ctx, pool, f.callerID); n != 0 {
			t.Fatalf("permitted reuse recorded %d authorization_unverifiable audit(s), want 0", n)
		}

		// No second intent/nonce: one 008 binding backs both attempts, both
		// rows carry the same live binding reference and nonce, and the anchor
		// is never rebound or replaced in place.
		anchor := lpRow(t, ctx, pool, f.callerID, "sr-h5-rep-anchor-2")
		if anchor.rowID != anchorID || anchor.replacementOf != nil || anchor.authzID != f.authzID || anchor.state != "received" {
			t.Fatalf("anchor row changed: %+v, want id %d/nil/%s", anchor, anchorID, f.authzID)
		}
		if anchor.nonce != row.nonce || anchor.bindingRef != row.bindingRef || row.bindingRef != f.bindingID {
			t.Fatalf("anchor/replacement nonce+binding = %q/%q vs %q/%q, want one binding %s nonce %s",
				anchor.nonce, anchor.bindingRef, row.nonce, row.bindingRef, f.bindingID, f.nonce)
		}
		if got := lpBindingCount(t, ctx, pool, f.intentID); got != 1 {
			t.Fatalf("008 bindings for the intent = %d, want exactly 1 (no second intent/nonce)", got)
		}
		if got := lpNonAnchorCount(t, ctx, pool, f.authzID); got != 1 {
			t.Fatalf("anchor requests on the grant = %d, want exactly 1", got)
		}

		// The replacement delivers under the same live binding.
		sink := &lpSink{}
		res, err := lpDeliver(t, ctx, f, sink)
		if err != nil || res.Verdict != signer.VerdictDelivered || sink.count() != 1 {
			t.Fatalf("replacement delivery = %+v / %v sink=%d, want delivered", res, err, sink.count())
		}
		facts := lpPayloadFacts(t, sink.last())
		if facts["signature"] != resp.Signature || facts["tx_hash"] != resp.TxHash {
			t.Fatalf("replacement payload = %v, want the persisted signature/hash", facts)
		}
		adm, ok := lpLastAdmission(t, ctx, pool, row.rowID)
		if !ok || adm.verdict != "delivered" || adm.bindingClass != "matches" {
			t.Fatalf("replacement admission = %+v found=%v, want delivered/matches", adm, ok)
		}
		lpWantPersistedResult(t, ctx, pool, row.rowID, resp.Signature, resp.TxHash)
		t.Logf("permitted replacement: anchor=%d replacement_of=%d grant=%s; one 008 binding for intent %s (nonce %s); delivery verdict=%s binding_class=%s",
			anchorID, *row.replacementOf, f.authzID, f.intentID, f.nonce, adm.verdict, adm.bindingClass)
	})

	t.Run("forbidden replacement reuse refuses per identity", func(t *testing.T) {
		f := lpSeed(t, ctx, pool, alloc, live, baseEnv, 3, lpSeedOpts{
			scoped: true, allowsFeeReplacement: false,
			feeMaxTotal: 200000000000000, feeMaxPerGas: 2000000000, feeMaxPriority: 1500000000,
		})
		anchorID := lpSeedAnchor(t, ctx, pool, f, "sr-h5-rep-anchor-3", "at-h5-rep-anchor-3")

		body := f.lpBody(t, f.requestID, f.attemptID, "1800000000", "1200000000")
		resp, err := signer.Submit(ctx, f.submit, f.cred, body)
		if resp != nil {
			t.Fatalf("purpose-denied reuse returned a signature response: %+v", resp)
		}
		lpRefusal(t, err, signer.ClassAuthorizationInvalid)

		row := lpRow(t, ctx, pool, f.callerID, f.requestID)
		if row.state != "rejected" || row.refusalClass != string(signer.ClassAuthorizationInvalid) ||
			row.replacementOf == nil || *row.replacementOf != anchorID || row.authzID != f.authzID {
			t.Fatalf("refused replacement = %+v, want rejected/authorization_invalid replacement of %d on %s",
				row, anchorID, f.authzID)
		}
		action, reason, detail := lpLastAudit(t, ctx, pool, f.requestID)
		if action != "authorization_refused" || reason != string(signer.ClassAuthorizationInvalid) ||
			!strings.Contains(detail, "fresh_authorization_required") ||
			!strings.Contains(detail, "anchor="+strconv.FormatInt(anchorID, 10)) {
			t.Fatalf("refusal audit = %q/%q/%q, want authorization_refused naming the anchor + fresh-authorization instruction",
				action, reason, detail)
		}
		lpWantNoResult(t, ctx, pool, row.rowID)

		anchor := lpRow(t, ctx, pool, f.callerID, "sr-h5-rep-anchor-3")
		if anchor.rowID != anchorID || anchor.replacementOf != nil || anchor.authzID != f.authzID {
			t.Fatalf("anchor rebound by the refused reuse: %+v", anchor)
		}
		if got := lpNonAnchorCount(t, ctx, pool, f.authzID); got != 1 {
			t.Fatalf("anchor requests on the grant = %d, want exactly 1", got)
		}
		t.Logf("forbidden reuse: %s refused %s, replacement_of=%d anchor=%d, detail=%q",
			f.requestID, row.refusalClass, *row.replacementOf, anchorID, detail)
	})

	t.Run("scopeless stock grants still refuse authorization_unverifiable per request", func(t *testing.T) {
		f := lpSeed(t, ctx, pool, alloc, live, baseEnv, 4, lpSeedOpts{})
		if _, found := lpScope(t, ctx, pool, f.authzID); found {
			t.Fatal("scopeless supply wrote a carrier row, want stock (no scope)")
		}

		body := f.lpDefaultBody(t)
		resp, err := signer.Submit(ctx, f.submit, f.cred, body)
		if resp != nil {
			t.Fatalf("scopeless stock returned a signature response: %+v", resp)
		}
		re := lpRefusal(t, err, signer.ClassAuthorizationUnverifiable)
		if signer.RetryabilityOf(re.Class) != signer.RetryNever {
			t.Fatalf("authorization_unverifiable retryability = %v, want RetryNever", signer.RetryabilityOf(re.Class))
		}

		row := lpRow(t, ctx, pool, f.callerID, f.requestID)
		if row.state != "rejected" || row.refusalClass != string(signer.ClassAuthorizationUnverifiable) || row.authzID != f.authzID {
			t.Fatalf("persisted refusal = %+v, want rejected/authorization_unverifiable on %s", row, f.authzID)
		}
		action, reason, detail := lpLastAudit(t, ctx, pool, f.requestID)
		if action != "authorization_refused" || reason != string(signer.ClassAuthorizationUnverifiable) ||
			!strings.Contains(detail, "scope_carrier=absent") {
			t.Fatalf("refusal audit = %q/%q/%q, want authorization_refused with scope_carrier=absent", action, reason, detail)
		}
		lpWantNoResult(t, ctx, pool, row.rowID)
		t.Logf("scopeless stock: %s refused %s (retryability %v), carrier absent, zero signatures",
			f.requestID, row.refusalClass, signer.RetryabilityOf(re.Class))
	})

	t.Run("revoked grant blocks delivery after a successful sign", func(t *testing.T) {
		f := lpSeed(t, ctx, pool, alloc, live, baseEnv, 5, lpSeedOpts{
			scoped: true, feeMaxTotal: 100000000000000, feeMaxPerGas: 2000000000, feeMaxPriority: 1500000000,
		})
		resp := lpSubmit(t, ctx, f, f.lpDefaultBody(t))
		row := lpRow(t, ctx, pool, f.callerID, f.requestID)

		opID := lpMintID(t, ctx, f.env)
		code, stdout, stderr := lpRun(ctx, f.env, "revoke",
			"--operation-id", opID, "--authorization-id", f.authzID,
			"--operator", "h5-operator", "--reason", "T040 delivery block: revoke")
		if code != 0 || !strings.Contains(stdout, "action=revoked") {
			t.Fatalf("PB revoke exit=%d stdout=%q stderr=%q, want revoked", code, stdout, stderr)
		}
		scope, found := lpScope(t, ctx, pool, f.authzID)
		if !found || scope.version != 2 {
			t.Fatalf("scope version after revoke = %d found=%v, want the bumped 2", scope.version, found)
		}
		if row.authVersion == nil || *row.authVersion != 1 {
			t.Fatalf("persisted version = %v, want the submit-time 1 (never silently adopted)", row.authVersion)
		}

		sink := &lpSink{}
		res, err := lpDeliver(t, ctx, f, sink)
		lpRefusal(t, err, signer.ClassSignatureWithheld)
		if res == nil || res.Verdict != signer.VerdictBlocked || sink.count() != 0 {
			t.Fatalf("revoked delivery = %+v / %v sink=%d, want status-only blocked with zero bytes", res, err, sink.count())
		}
		adm, ok := lpLastAdmission(t, ctx, pool, row.rowID)
		if !ok || adm.verdict != "blocked" || adm.reason != string(signer.ClassSignatureWithheld) ||
			adm.authzState != "revoked" || adm.deliveredAt != nil {
			t.Fatalf("blocked admission = %+v found=%v, want blocked/signature_withheld/revoked with no delivered_at", adm, ok)
		}
		if _, _, detail := lpLastAudit(t, ctx, pool, f.requestID); !strings.Contains(detail, "observed=authorization_revoked") {
			t.Fatalf("audit detail %q lacks observed=authorization_revoked", detail)
		}
		if lpDeliveredMarker(t, ctx, pool, row.rowID) {
			t.Fatal("blocked delivery committed a delivered marker")
		}
		lpWantPersistedResult(t, ctx, pool, row.rowID, resp.Signature, resp.TxHash)
		t.Logf("revoke block: scope version=%d grant=%s; delivery verdict=%s reason=%s authorization_state=%s",
			scope.version, f.authzID, adm.verdict, adm.reason, adm.authzState)
	})

	t.Run("expired grant blocks delivery after a successful sign", func(t *testing.T) {
		expiry := time.Now().Add(3 * time.Second)
		f := lpSeed(t, ctx, pool, alloc, live, baseEnv, 6, lpSeedOpts{
			scoped: true, feeMaxTotal: 100000000000000, feeMaxPerGas: 2000000000, feeMaxPriority: 1500000000,
			expiresAt: &expiry,
		})
		resp := lpSubmit(t, ctx, f, f.lpDefaultBody(t))
		row := lpRow(t, ctx, pool, f.callerID, f.requestID)
		if row.state != "signed" {
			t.Fatalf("pre-expiry submit = %q, want signed", row.state)
		}
		time.Sleep(time.Until(expiry.Add(400 * time.Millisecond)))

		sink := &lpSink{}
		res, err := lpDeliver(t, ctx, f, sink)
		lpRefusal(t, err, signer.ClassSignatureWithheld)
		if res == nil || res.Verdict != signer.VerdictBlocked || sink.count() != 0 {
			t.Fatalf("expired delivery = %+v / %v sink=%d, want status-only blocked", res, err, sink.count())
		}
		_, _, detail := lpLastAudit(t, ctx, pool, f.requestID)
		if !strings.Contains(detail, "observed=authorization_expired") {
			t.Fatalf("audit detail %q lacks observed=authorization_expired", detail)
		}
		lpWantPersistedResult(t, ctx, pool, row.rowID, resp.Signature, resp.TxHash)
		t.Logf("expiry block: grant expired at %s; delivery verdict=%s detail=%q",
			expiry.UTC().Format(time.RFC3339), res.Verdict, detail)
	})

	t.Run("006 pause blocks delivery after a successful sign", func(t *testing.T) {
		f := lpSeed(t, ctx, pool, alloc, live, baseEnv, 7, lpSeedOpts{
			scoped: true, feeMaxTotal: 100000000000000, feeMaxPerGas: 2000000000, feeMaxPriority: 1500000000,
		})
		resp := lpSubmit(t, ctx, f, f.lpDefaultBody(t))
		row := lpRow(t, ctx, pool, f.callerID, f.requestID)

		lpExec(t, ctx, pool, `INSERT INTO indexer_pause
			(chain_id, height, expected_hash, actual_hash, kind) VALUES ($1, 10, $2, $3, 'hash_mismatch')`,
			lpChainID, lpHash("aa"), lpHash("bb"))
		t.Cleanup(func() {
			_, _ = pool.Exec(context.Background(), `DELETE FROM indexer_pause`)
		})

		sink := &lpSink{}
		res, err := lpDeliver(t, ctx, f, sink)
		lpRefusal(t, err, signer.ClassSignatureWithheld)
		if res == nil || res.Verdict != signer.VerdictBlocked || sink.count() != 0 {
			t.Fatalf("paused delivery = %+v / %v sink=%d, want status-only blocked", res, err, sink.count())
		}
		adm, ok := lpLastAdmission(t, ctx, pool, row.rowID)
		if !ok || adm.verdict != "blocked" || adm.pauseBasis != "indexer_pause" {
			t.Fatalf("blocked admission = %+v found=%v, want blocked with pause_basis=indexer_pause", adm, ok)
		}
		_, _, detail := lpLastAudit(t, ctx, pool, f.requestID)
		if !strings.Contains(detail, "observed=recovery_paused") {
			t.Fatalf("audit detail %q lacks observed=recovery_paused", detail)
		}
		lpWantPersistedResult(t, ctx, pool, row.rowID, resp.Signature, resp.TxHash)
		t.Logf("pause block: admission verdict=%s pause_basis=%s; detail=%q", adm.verdict, adm.pauseBasis, detail)
	})

	t.Run("same-identity retry and unknown recovery converge without re-signing", func(t *testing.T) {
		f := lpSeed(t, ctx, pool, alloc, live, baseEnv, 8, lpSeedOpts{
			scoped: true, feeMaxTotal: 100000000000000, feeMaxPerGas: 2000000000, feeMaxPriority: 1500000000,
		})
		body := f.lpDefaultBody(t)
		resp := lpSubmit(t, ctx, f, body)
		row := lpRow(t, ctx, pool, f.callerID, f.requestID)

		// A failed write is unknown, never "nothing delivered": the region
		// rolled back (no admission, no marker) and the bytes may be out.
		failSink := &lpSink{fail: true}
		res, err := lpDeliver(t, ctx, f, failSink)
		lpRefusal(t, err, signer.ClassOutcomeUnknown)
		if res == nil || res.Verdict != signer.VerdictUnknownReconcile || res.Class != signer.ClassOutcomeUnknown {
			t.Fatalf("failed write = %+v, want unknown_reconcile/outcome_unknown", res)
		}
		if failSink.count() != 1 {
			t.Fatalf("failed-write sink attempts = %d, want 1 (bytes may be out)", failSink.count())
		}
		if got := lpAdmissionCount(t, ctx, pool, row.rowID); got != 0 {
			t.Fatalf("unknown outcome left %d admission row(s), want 0", got)
		}
		if lpDeliveredMarker(t, ctx, pool, row.rowID) {
			t.Fatal("unknown outcome committed a delivered marker")
		}
		action, reason, _ := lpLastAudit(t, ctx, pool, f.requestID)
		if action != "delivery_unknown" || reason != string(signer.ClassOutcomeUnknown) {
			t.Fatalf("unknown audit = %s/%s, want delivery_unknown/outcome_unknown", action, reason)
		}
		lpWantPersistedResult(t, ctx, pool, row.rowID, resp.Signature, resp.TxHash)

		// Same identity retry re-passes the live gates and hands out the
		// persisted bytes byte-identically; no re-sign.
		good := &lpSink{}
		res2, err := lpDeliver(t, ctx, f, good)
		if err != nil || res2.Verdict != signer.VerdictDelivered || res2.AttemptSeq != 1 || good.count() != 1 {
			t.Fatalf("recovery delivery = %+v / %v sink=%d, want delivered attempt 1", res2, err, good.count())
		}
		facts := lpPayloadFacts(t, good.last())
		if facts["signature"] != resp.Signature || facts["tx_hash"] != resp.TxHash {
			t.Fatalf("recovered payload = %v, want the persisted signature/hash", facts)
		}
		res3, err := lpDeliver(t, ctx, f, good)
		if err != nil || res3.Verdict != signer.VerdictDelivered || good.count() != 2 {
			t.Fatalf("already-delivered retry = %+v / %v sink=%d, want idempotent delivered", res3, err, good.count())
		}
		if !bytes.Equal(good.payloads[0], good.payloads[1]) {
			t.Fatal("re-delivery payload is not byte-identical to the admitted one")
		}
		lpWantPersistedResult(t, ctx, pool, row.rowID, resp.Signature, resp.TxHash)

		// Same-identity submit retry converges on the persisted result too.
		resp2 := lpSubmit(t, ctx, f, body)
		if resp2.Signature != resp.Signature || resp2.TxHash != resp.TxHash {
			t.Fatalf("submit retry = %+v, want the persisted %s/%s", resp2, resp.Signature, resp.TxHash)
		}
		lpWantPersistedResult(t, ctx, pool, row.rowID, resp.Signature, resp.TxHash)

		// The live binding is never consumed or mutated by 009.
		after, aerr := readProvider.ReadByBindingID(ctx, f.bindingID)
		if aerr != nil || after.Outcome != nonce.ReadBound || after.Binding == nil ||
			after.Binding.State != nonce.StateAllocated || after.Binding.Nonce != f.nonce {
			t.Fatalf("008 binding after recovery = (%q, %+v, %v), want bound/allocated", after.Outcome, after.Binding, aerr)
		}
		t.Logf("unknown recovery: failed write verdict=%s class=%s; retry delivered attempt=%d; byte-identical redelivery; signature unchanged (%s)",
			res.Verdict, res.Class, res2.AttemptSeq, resp.Signature)
	})
}
