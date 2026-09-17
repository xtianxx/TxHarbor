//go:build integration

// submit_scope_lock_integration_test.go owns the submit-side 008 scope-row
// FOR SHARE lock (contracts/persistence.md §1 fixed lock order,
// contracts/gates.md §3 bilateral rule, data-model.md T-submit-first): submit's
// first-receipt transaction takes the 008 scope row FOR SHARE FIRST, before the
// 006 gate-table SHARE lock and every gate read, and holds it to COMMIT. The
// delivery-side re-check does NOT substitute for this lock.
//
// What it proves on a real, isolated PostgreSQL with live 008 rows:
//
//   - lock order: the production LiveBindingReader's scope lock is taken before
//     its binding read inside the submit transaction (recorded by a probe that
//     delegates both halves to the live adapter — no lock or class is
//     substituted);
//   - the held submit transaction blocks a REAL 008-side writer: with the
//     submit transaction parked inside its binding read (scope row already
//     held), the 008 writer's UPDATE and SELECT … FOR UPDATE both wait past
//     their bound (SQLSTATE 55P03), and succeed only after the submit commits;
//   - a nil ScopeLocker fails closed before BEGIN: refuse, never a silent
//     unscoped sign.
//
// Helpers are shared with T027/T028 (blAllocator/blProvider/newBLChainView,
// blScopeUpdateAttempt/blScopeForUpdateAttempt/blLockNotAvailable, gateExec/
// gateReset006); the 008 binding and scope rows are created by 008's own
// operator/admission APIs, and the 009 grant/scope carrier are raw-SQL fixtures
// in the same shape the existing signer integration tests use.
package signer

import (
	"context"
	"math/big"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/nonce"
)

// sslFixture is one isolated signable 009 request over a real 008 binding: the
// 009 caller/grant/PB carrier plus the 008 registry + admitted binding for the
// same intent and sender.
type sslFixture struct {
	callerID  int64
	authzID   string
	intentID  string
	requestID string
	attemptID string
	sender    string
	bindingID string
	cred      string
	body      []byte
	deps      SubmitDeps
}

// sslPolicy allows exactly the fixture sender (a generated signing key) so the
// legal path reaches SignTx.
func sslPolicy(t *testing.T, sender string) *Policy {
	t.Helper()
	p, err := NewPolicy(PolicyConfig{
		ChainIDs:             []int64{gateChainID},
		Senders:              []string{sender},
		Assets:               []string{gateAsset},
		Recipients:           []string{gateRecipient},
		MaxAmount:            big.NewInt(2000000),
		MaxGasLimit:          100000,
		MaxFeePerGas:         big.NewInt(2000000000),
		MaxPriorityFeePerGas: big.NewInt(1500000000),
		MaxGasPrice:          big.NewInt(2000000000),
	})
	if err != nil {
		t.Fatalf("NewPolicy: %v", err)
	}
	return p
}

// sslBody retargets the shared compliant type-2 body (submitGrantBody: clean
// 006 version 0) at the fixture's identity, including the 008-admitted nonce.
func sslBody(t *testing.T, f *sslFixture, nonce string) []byte {
	t.Helper()
	b := submitGrantBody()
	for _, rep := range [][2]string{
		{`"authorization_id": "` + gateAuthID + `"`, `"authorization_id": "` + f.authzID + `"`},
		{`"signing_request_id": "sr-7f3a"`, `"signing_request_id": "` + f.requestID + `"`},
		{`"attempt_id": "at-9c02"`, `"attempt_id": "` + f.attemptID + `"`},
		{`"intent_id": "pi-1b44"`, `"intent_id": "` + f.intentID + `"`},
		{`"sender": "0x1111111111111111111111111111111111111111"`, `"sender": "` + f.sender + `"`},
		{`"nonce": "42"`, `"nonce": "` + nonce + `"`},
	} {
		b = strings.Replace(b, rep[0], rep[1], 1)
	}
	req := mustDecode(t, b)
	if req.IntentID != f.intentID || req.Sender != f.sender || req.AuthorizationID != f.authzID {
		t.Fatalf("fixture body retarget failed: intent=%q sender=%q authz=%q", req.IntentID, req.Sender, req.AuthorizationID)
	}
	return []byte(b)
}

// sslSeed builds one isolated signable fixture: 009 caller + active grant +
// present-and-verifiable PB carrier (all-zero fee caps = no constraint), then
// the 008 registry + binding through 008's own APIs.
func sslSeed(t *testing.T, pool *pgxpool.Pool, alloc *nonce.Allocator, live *LiveBindingReader, n int) *sslFixture {
	t.Helper()
	ctx := context.Background()
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	hexKey := common.Bytes2Hex(crypto.FromECDSA(key))
	sender := strings.ToLower(crypto.PubkeyToAddress(key.PublicKey).Hex())
	f := &sslFixture{
		callerID:  int64(9600 + n),
		authzID:   "wa-ssl-" + strconv.Itoa(n),
		intentID:  "pi-ssl-" + strconv.Itoa(n),
		requestID: "sr-ssl-" + strconv.Itoa(n),
		attemptID: "at-ssl-" + strconv.Itoa(n),
		sender:    sender,
	}

	gateExec(t, pool, `INSERT INTO caller (caller_id, can_create) VALUES ($1, TRUE)`, f.callerID)
	gateExec(t, pool, `INSERT INTO withdrawal_authorizations
		(authorization_id, caller_id, chain_id, asset, recipient, amount, state, expires_at, supplied_by)
		VALUES ($1, $2, $3, $4, $5, $6, 'active', now() + interval '1 hour', 'ssl-test')`,
		f.authzID, f.callerID, gateChainID, gateAsset, gateRecipient, gateAmount)
	gateExec(t, pool, `INSERT INTO withdrawal_authorization_scopes
		(authorization_id, intent_id, request_id, sender, fee_max_total, fee_max_per_gas,
		 fee_max_priority, allows_fee_replacement, authorization_version, attested_by)
		VALUES ($1, $2, $3, $4, 0, 0, 0, FALSE, 1, 'ssl-test')`,
		f.authzID, f.intentID, f.requestID, f.sender)

	cred, err := IssueCredential(ctx, pool, f.callerID, "ssl-"+strconv.Itoa(n))
	if err != nil {
		t.Fatalf("IssueCredential: %v", err)
	}
	f.cred = cred

	res, err := nonce.NewAdminRunner(pool, nil).Run(ctx, nonce.AdminRequest{
		Action: nonce.AdminActionRegistryRegister, OperationID: "ssl-reg-" + strconv.Itoa(n),
		ChainID: gateChainID, Sender: f.sender,
	})
	if err != nil || res.Outcome != nonce.AdminApplied {
		t.Fatalf("008 registry_register = (%q, %v), want applied", res.Outcome, err)
	}
	binding, outcome, err := alloc.Allocate(ctx, nonce.AllocationRequest{
		IntentID: f.intentID, ChainID: gateChainID, Sender: f.sender, AuthorizationID: f.authzID,
	})
	if err != nil || outcome != nonce.OutcomeAllocated || binding == nil {
		t.Fatalf("008 allocate = (%v, %q, %v), want allocated", binding, outcome, err)
	}
	f.bindingID = binding.BindingID
	f.body = sslBody(t, f, binding.Nonce.String())

	provider, err := NewDevKeyProvider(ModeDevelopment, writeTestKey(t, hexKey), 0)
	if err != nil {
		t.Fatalf("NewDevKeyProvider: %v", err)
	}
	if provider.Address() != common.HexToAddress(f.sender) {
		t.Fatalf("provider bound to %s, want the fixture sender %s", provider.Address(), f.sender)
	}
	f.deps = SubmitDeps{
		DB: pool, Policy: sslPolicy(t, f.sender), Provider: provider,
		Binding: live, ScopeLock: live,
	}
	return f
}

// sslSignatureCount counts persisted results for one request identity (the
// fail-closed case must leave zero).
func sslSignatureCount(t *testing.T, pool *pgxpool.Pool, callerID int64, requestID string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM signature_results s
		   JOIN signing_requests r ON r.id = s.signing_request_row
		  WHERE r.caller_id = $1 AND r.signing_request_id = $2`,
		callerID, requestID).Scan(&n); err != nil {
		t.Fatalf("count signature_results: %v", err)
	}
	return n
}

// sslOrderProbe records the submit transaction's scope-lock vs binding-read
// call order while delegating both halves to the production live adapter (no
// lock and no binding class is substituted).
type sslOrderProbe struct {
	live  *LiveBindingReader
	mu    sync.Mutex
	calls []string
}

func (p *sslOrderProbe) add(call string) {
	p.mu.Lock()
	p.calls = append(p.calls, call)
	p.mu.Unlock()
}

func (p *sslOrderProbe) snapshot() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.calls...)
}

func (p *sslOrderProbe) ReadBinding(ctx context.Context, intentID, attemptID string) (BindingResult, error) {
	p.add("binding")
	return p.live.ReadBinding(ctx, intentID, attemptID)
}

func (p *sslOrderProbe) LockScope(ctx context.Context, tx pgx.Tx, chainID int64, sender string) error {
	p.add("scope")
	return p.live.LockScope(ctx, tx, chainID, sender)
}

// sslHeldBinding parks the submit transaction inside its binding read (the
// scope row is already locked by then) until released — the window in which the
// submit transaction must hold the 008 scope row FOR SHARE.
type sslHeldBinding struct {
	live    *LiveBindingReader
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *sslHeldBinding) ReadBinding(ctx context.Context, intentID, attemptID string) (BindingResult, error) {
	b.once.Do(func() { close(b.entered) })
	<-b.release
	return b.live.ReadBinding(ctx, intentID, attemptID)
}

// TestSignerSubmitTakesScopeLockFirst pins the submit lock order: the live
// scope-row FOR SHARE is taken before the live binding read, and the legal
// submit commits a signed decision.
func TestSignerSubmitTakesScopeLockFirst(t *testing.T) {
	pool := signerAuthStartPG(t)
	ctx := context.Background()
	gateReset006(t, pool)
	live := NewLiveBindingReader(blProvider(pool))
	f := sslSeed(t, pool, blAllocator(t, pool, newBLChainView()), live, 1)

	probe := &sslOrderProbe{live: live}
	deps := f.deps
	deps.Binding = probe
	deps.ScopeLock = probe

	resp, err := Submit(ctx, deps, f.cred, f.body)
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if resp == nil || resp.Signature == "" || resp.TxHash == "" {
		t.Fatalf("Submit response = %+v, want a persisted signature + tx hash", resp)
	}
	order := probe.snapshot()
	if len(order) < 2 || order[0] != "scope" || order[1] != "binding" {
		t.Fatalf("submit call order = %v, want the live scope-row FOR SHARE before the live binding read", order)
	}
	var state string
	if err := pool.QueryRow(ctx,
		`SELECT state FROM signing_requests WHERE caller_id = $1 AND signing_request_id = $2`,
		f.callerID, f.requestID).Scan(&state); err != nil {
		t.Fatalf("read request row: %v", err)
	}
	if state != string(StateSigned) {
		t.Fatalf("persisted state = %q, want %q", state, StateSigned)
	}
	if got := sslSignatureCount(t, pool, f.callerID, f.requestID); got != 1 {
		t.Fatalf("signature_results rows = %d, want exactly 1", got)
	}
}

// TestSignerSubmitHoldsScopeLockAgainst008Writer pins the submit transaction's
// FOR SHARE against a REAL 008-side writer in both directions: the writer waits
// while the submit holds the lock, and proceeds once the submit commits.
func TestSignerSubmitHoldsScopeLockAgainst008Writer(t *testing.T) {
	pool := signerAuthStartPG(t)
	ctx := context.Background()
	gateReset006(t, pool)
	live := NewLiveBindingReader(blProvider(pool))
	f := sslSeed(t, pool, blAllocator(t, pool, newBLChainView()), live, 2)

	held := &sslHeldBinding{live: live, entered: make(chan struct{}), release: make(chan struct{})}
	deps := f.deps
	deps.Binding = held
	deps.ScopeLock = live

	type outcome struct {
		resp *SubmitResponse
		err  error
	}
	done := make(chan outcome, 1)
	go func() {
		resp, err := Submit(ctx, deps, f.cred, f.body)
		done <- outcome{resp, err}
	}()
	select {
	case <-held.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("submit never reached its binding read")
	}

	// The submit transaction is inside its binding read and holds the live 008
	// scope row FOR SHARE: both 008 writer shapes must wait past their bound.
	if err := blScopeUpdateAttempt(t, pool, &blFixture{sender: f.sender}); !blLockNotAvailable(err) {
		t.Fatalf("008 UPDATE against the held submit scope share = %v, want lock_not_available", err)
	}
	if err := blScopeForUpdateAttempt(t, pool, &blFixture{sender: f.sender}); !blLockNotAvailable(err) {
		t.Fatalf("008 SELECT FOR UPDATE against the held submit scope share = %v, want lock_not_available", err)
	}
	select {
	case out := <-done:
		t.Fatalf("submit completed while it held the scope row: %+v / %v", out.resp, out.err)
	default:
	}

	close(held.release)
	select {
	case out := <-done:
		if out.err != nil || out.resp == nil {
			t.Fatalf("submit after release = %+v / %v, want success", out.resp, out.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("submit did not complete after release")
	}
	// The share lock died with the committed transaction: the 008 writer now
	// proceeds.
	if err := blScopeUpdateAttempt(t, pool, &blFixture{sender: f.sender}); err != nil {
		t.Fatalf("008 writer after submit committed = %v, want success", err)
	}
}

// TestSignerSubmitNilScopeLockFailsClosed pins that a missing ScopeLocker is an
// explicit refusal before BEGIN — never a silent unscoped sign.
func TestSignerSubmitNilScopeLockFailsClosed(t *testing.T) {
	pool := signerAuthStartPG(t)
	ctx := context.Background()
	gateReset006(t, pool)
	live := NewLiveBindingReader(blProvider(pool))
	f := sslSeed(t, pool, blAllocator(t, pool, newBLChainView()), live, 3)

	deps := f.deps
	deps.ScopeLock = nil
	resp, err := Submit(ctx, deps, f.cred, f.body)
	if resp != nil {
		t.Fatalf("nil ScopeLock returned a response %+v, want a fail-closed refusal", resp)
	}
	signerAuthRefusal(t, err, ClassStorageUnavailable)
	if got := submitRowCount(t, pool, f.callerID, f.requestID); got != 0 {
		t.Fatalf("nil ScopeLock persisted %d request row(s), want 0", got)
	}
	if got := sslSignatureCount(t, pool, f.callerID, f.requestID); got != 0 {
		t.Fatalf("nil ScopeLock persisted %d signature result(s), want 0", got)
	}
}
