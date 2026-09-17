//go:build integration

// binding_live_integration_test.go owns spec task T028 for 009-signer-service:
// the 008 real-integration acceptance on a real, isolated PostgreSQL with the
// merged 008 schema and code (quickstart V6/V7; FR-18; contracts/gates.md §3;
// plan D3; research R6 lock order).
//
// What it proves against LIVE 008 rows:
//
//   - the fact half: LiveBindingReader.ReadBinding over *nonce.ReadProvider
//     maps every live (Outcome, Annotations) to the six gates.md:149-158
//     classes — bound+clean → matches, bound+hold / recovery != none /
//     registry disabled → paused, terminal → terminal, not_bound → absent,
//     mismatch → conflict, unavailable → read_failed — and each class lands on
//     the §3 admission refusal through BindingRefusal;
//   - the lock half, tested separately: the scope-row `FOR SHARE` is taken
//     INSIDE the T034 delivery transaction via ScopeLocker against the live
//     008 row — an 008 writer holding the row `FOR UPDATE` blocks the delivery
//     until it commits (the delivery session is observed waiting on the live
//     relation), and while the delivery region is open the same writer is
//     refused with lock_not_available until the delivery commits. A standalone
//     ReadProvider.Read fact call holds no scope lock: it alone does NOT
//     satisfy the lock half;
//   - the delivery path end to end over the live adapter: a live clean binding
//     delivers the persisted bytes with binding_class=matches, zero re-signing
//     and an untouched 008 binding, and a live 008 pause (registry disabled)
//     blocks with a status-only response recording binding_class=paused.
//
// Doubles: none on this path. The T026/T027 contract-shape doubles
// (gateBindingDouble/dlvBinding/dlvScope) are not used here — every binding
// read and scope lock below is the production adapter over live 008 rows.
// Only the abstract byte sink (dlvSink) and the dlv* row probes are shared
// with T027, and they substitute no gate behavior.
//
// Seeding: the 009 rows are 009-owned fixtures (the raw-SQL shape T027 uses);
// the 008 registry row is created through the operator API
// (`registry_register`), rows are flipped through
// `registry_disable`/`binding_release`, and the binding itself is admitted by
// 008's own Allocator.Allocate — never a hand-forged nonce_bindings row. The
// upstream chain view is scripted (as 008's own read-api lock-order test
// does): the binding/read path under test reaches no RPC.
package signer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/nonce"
)

// blReadToken is a test-only provider credential (never a deployment secret).
const blReadToken = "bl-nonce-read-token"

// blChainView is the scripted 008 chain view: a healthy, consistent scope
// (latest == pending == 0, one head) so a fresh scope admits nonce 0, plus a
// mutable pair so one scenario can present a divergent view and make 008
// establish a real hold through its own classification path.
type blChainView struct {
	mu      sync.Mutex
	latest  string
	pending string
}

func newBLChainView() *blChainView { return &blChainView{latest: "0x0", pending: "0x0"} }

func (v *blChainView) set(latest, pending string) {
	v.mu.Lock()
	v.latest, v.pending = latest, pending
	v.mu.Unlock()
}

func (v *blChainView) CallContext(_ context.Context, result any, method string, args ...any) error {
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
		return fmt.Errorf("blChainView: unexpected method %q", method)
	}
}

func blObserver(view *blChainView) *nonce.Observer {
	return nonce.NewObserver(view, nonce.ObserverConfig{
		RPCTimeout:   2 * time.Second,
		RetryInitial: time.Millisecond,
		RetryMax:     5 * time.Millisecond,
	})
}

// blAllocator wires 008's admission core over the scripted chain view and an
// explicitly opened rebuild gate (008's own test wiring).
func blAllocator(t *testing.T, pool *pgxpool.Pool, view *blChainView) *nonce.Allocator {
	t.Helper()
	gate := nonce.NewRebuildGate()
	gate.Open()
	return nonce.NewAllocator(pool, blObserver(view), gate)
}

// blProvider returns the live 008 read provider (open rebuild gate, test
// token) — the exact provider the adapter consumes.
func blProvider(pool *pgxpool.Pool) *nonce.ReadProvider {
	gate := nonce.NewRebuildGate()
	gate.Open()
	return nonce.NewReadProvider(pool, blReadToken, gate)
}

// blAdmin runs one 008 operator attempt (registry register/disable,
// binding_release). view may be nil for the registry actions (no RPC).
func blAdmin(t *testing.T, pool *pgxpool.Pool, view *blChainView, req nonce.AdminRequest) nonce.AdminResult {
	t.Helper()
	var observer *nonce.Observer
	if view != nil {
		observer = blObserver(view)
	}
	res, err := nonce.NewAdminRunner(pool, observer).Run(context.Background(), req)
	if err != nil {
		t.Fatalf("008 admin %s: %v", req.Action, err)
	}
	return res
}

// blFixture is one live 009 request over a real 008 binding: the 009-owned
// rows (caller/grant/request/result) plus the 008-admitted binding for the
// same intent. It embeds dlvFixture so the T027 row probes stay reusable.
type blFixture struct {
	dlvFixture
	intentID  string
	attemptID string
	sender    string
	bindingID string
	nonce     string
}

// blBody retargets the shared valid body at the fixture's identity. The
// declared nonce matches the fresh scope's first admission (nonce 0).
func blBody(t *testing.T, f *blFixture) string {
	t.Helper()
	b := submitGrantBody()
	for _, rep := range [][2]string{
		{`"authorization_id": "` + gateAuthID + `"`, `"authorization_id": "` + f.authzID + `"`},
		{`"signing_request_id": "sr-7f3a"`, `"signing_request_id": "` + f.requestID + `"`},
		{`"attempt_id": "at-9c02"`, `"attempt_id": "` + f.attemptID + `"`},
		{`"intent_id": "pi-1b44"`, `"intent_id": "` + f.intentID + `"`},
		{`"sender": "0x1111111111111111111111111111111111111111"`, `"sender": "` + f.sender + `"`},
		{`"nonce": "42"`, `"nonce": "0"`},
	} {
		b = strings.Replace(b, rep[0], rep[1], 1)
	}
	req := mustDecode(t, b)
	if req.IntentID != f.intentID || req.Sender != f.sender || req.AuthorizationID != f.authzID {
		t.Fatalf("fixture body retarget failed: intent=%q sender=%q authz=%q", req.IntentID, req.Sender, req.AuthorizationID)
	}
	return b
}

// blSeed builds one isolated live fixture: 009 rows first, then the 008
// registry + binding through 008's own operator/admission APIs.
func blSeed(t *testing.T, pool *pgxpool.Pool, alloc *nonce.Allocator, n int) *blFixture {
	t.Helper()
	ctx := context.Background()
	f := &blFixture{
		dlvFixture: dlvFixture{
			callerID:  int64(9100 + n),
			authzID:   "wa-bl-" + strconv.Itoa(n),
			requestID: "sr-bl-" + strconv.Itoa(n),
		},
		intentID:  "pi-bl-" + strconv.Itoa(n),
		attemptID: "at-bl-" + strconv.Itoa(n),
		sender:    fmt.Sprintf("0x%040x", 0x3000+n),
	}

	gateExec(t, pool, `INSERT INTO caller (caller_id, can_create) VALUES ($1, TRUE)`, f.callerID)
	gateExec(t, pool, `INSERT INTO withdrawal_authorizations
		(authorization_id, caller_id, chain_id, asset, recipient, amount, state, expires_at, supplied_by)
		VALUES ($1, $2, $3, $4, $5, $6, 'active', now() + interval '1 hour', 'bl-test')`,
		f.authzID, f.callerID, gateChainID, gateAsset, gateRecipient, gateAmount)
	if _, err := IssueCredential(ctx, pool, f.callerID, "bl-"+strconv.Itoa(n)); err != nil {
		t.Fatalf("IssueCredential: %v", err)
	}

	f.rowID = submitSeedRow(t, pool, f.callerID, blBody(t, f), string(StateSigned))
	f.signature = signerMigrationSignature("ab")
	f.txHash = signerMigrationHash(fmt.Sprintf("%02x", n%256))
	gateExec(t, pool, `INSERT INTO signature_results (signing_request_row, signature, tx_hash) VALUES ($1, $2, $3)`,
		f.rowID, f.signature, f.txHash)
	dlvSyncFingerprint(t, pool, f.rowID, f.authzID)

	reg := blAdmin(t, pool, nil, nonce.AdminRequest{
		Action: nonce.AdminActionRegistryRegister, OperationID: "bl-reg-" + strconv.Itoa(n),
		ChainID: gateChainID, Sender: f.sender,
	})
	if reg.Outcome != nonce.AdminApplied {
		t.Fatalf("008 registry_register = %q, want applied", reg.Outcome)
	}
	binding, outcome, err := alloc.Allocate(ctx, nonce.AllocationRequest{
		IntentID: f.intentID, ChainID: gateChainID, Sender: f.sender, AuthorizationID: f.authzID,
	})
	if err != nil || outcome != nonce.OutcomeAllocated || binding == nil {
		t.Fatalf("008 allocate = (%v, %q, %v), want allocated", binding, outcome, err)
	}
	f.bindingID, f.nonce = binding.BindingID, binding.Nonce.String()
	// The 009 row records the binding reference it read (gates.md §3).
	gateExec(t, pool, `UPDATE signing_requests SET binding_ref = $1 WHERE id = $2`, f.bindingID, f.rowID)
	return f
}

// blMapRead runs one live 008 provider read and returns the response plus its
// mapped class, logging the observed (Outcome, Annotations) basis — the
// per-case log T028 retains.
func blMapRead(t *testing.T, ctx context.Context, provider *nonce.ReadProvider, req nonce.ReadRequest) (nonce.ReadResponse, BindingResult, error) {
	t.Helper()
	resp, rerr := provider.Read(ctx, req)
	class, cerr := ClassifyBindingResponse(resp, rerr)
	gateState, holds, recovery, registry := "-", 0, "-", "-"
	if ann := resp.Annotations; ann != nil {
		gateState, holds, recovery, registry = ann.Gate.State, len(ann.Gate.Causes), ann.Recovery.State, ann.RegistryState
	}
	t.Logf("live 008 read: outcome=%s gate=%s holds=%d recovery=%s registry=%s -> class=%s (read_err=%v map_err=%v)",
		resp.Outcome, gateState, holds, recovery, registry, bindingClassName(class), rerr, cerr)
	return resp, class, cerr
}

// blWantRefusal pins the produced class on the §3 admission refusal mapping.
func blWantRefusal(t *testing.T, class BindingResult, mapErr error, want RefusalClass) {
	t.Helper()
	if got := BindingRefusal(class, mapErr); got != want {
		t.Fatalf("BindingRefusal(%s, %v) = %q, want %q", bindingClassName(class), mapErr, got, want)
	}
}

// blScopeForUpdateAttempt runs the 008 writer's scope-row `SELECT … FOR UPDATE`
// with a short lock_timeout and returns the error (nil = acquired).
func blScopeForUpdateAttempt(t *testing.T, pool *pgxpool.Pool, f *blFixture) error {
	t.Helper()
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin 008 writer tx: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, "SET LOCAL lock_timeout = '300ms'"); err != nil {
		t.Fatalf("set writer lock_timeout: %v", err)
	}
	var cid int64
	return tx.QueryRow(ctx,
		`SELECT chain_id FROM nonce_scope_state WHERE chain_id = $1 AND sender = $2 FOR UPDATE`,
		gateChainID, f.sender).Scan(&cid)
}

// blScopeUpdateAttempt runs an 008 writer's scope-row UPDATE with a short
// lock_timeout (the same row lock as lockScopeRowTx, as a write statement).
func blScopeUpdateAttempt(t *testing.T, pool *pgxpool.Pool, f *blFixture) error {
	t.Helper()
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin 008 writer tx: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, "SET LOCAL lock_timeout = '300ms'"); err != nil {
		t.Fatalf("set writer lock_timeout: %v", err)
	}
	_, err = tx.Exec(ctx,
		`UPDATE nonce_scope_state SET last_latest = last_latest WHERE chain_id = $1 AND sender = $2`,
		gateChainID, f.sender)
	return err
}

// blLockNotAvailable reports SQLSTATE 55P03: the writer waited past its bound.
func blLockNotAvailable(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "55P03"
}

// blHoldScopeRow opens one transaction holding the live 008 scope row in the
// named lock mode until Commit (the writer-first direction of the bilateral
// protocol; mode is a test literal).
func blHoldScopeRow(t *testing.T, pool *pgxpool.Pool, mode string, f *blFixture) pgx.Tx {
	t.Helper()
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin scope %s holder: %v", mode, err)
	}
	t.Cleanup(func() { _ = tx.Rollback(context.Background()) }) // no-op after Commit
	if _, err := tx.Exec(ctx, "SET LOCAL lock_timeout = '4s'"); err != nil {
		t.Fatalf("set holder lock_timeout: %v", err)
	}
	var cid int64
	if err := tx.QueryRow(ctx,
		`SELECT chain_id FROM nonce_scope_state WHERE chain_id = $1 AND sender = $2 FOR `+mode,
		gateChainID, f.sender).Scan(&cid); err != nil {
		t.Fatalf("hold scope row FOR %s: %v", mode, err)
	}
	return tx
}

// blWantScopeWait polls until a session is observed waiting on the live 008
// scope relation: the delivery transaction blocked on its scope-row FOR SHARE.
func blWantScopeWait(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var n int
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM pg_stat_activity
			  WHERE wait_event_type = 'Lock' AND state = 'active'
			    AND query ILIKE '%nonce_scope_state%'`).Scan(&n); err != nil {
			t.Fatalf("pg_stat_activity poll: %v", err)
		}
		if n > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("no session observed waiting on the live 008 scope row")
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// blOrderProbe is a call-order probe over the PRODUCTION adapter: it records
// the sequence of the delivery transaction's scope-lock and binding-read calls
// while delegating both to the live 008 adapter (no class or lock is
// substituted).
type blOrderProbe struct {
	live  *LiveBindingReader
	mu    sync.Mutex
	calls []string
}

func newBLOrderProbe(live *LiveBindingReader) *blOrderProbe { return &blOrderProbe{live: live} }

func (p *blOrderProbe) add(call string) {
	p.mu.Lock()
	p.calls = append(p.calls, call)
	p.mu.Unlock()
}

func (p *blOrderProbe) snapshot() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.calls...)
}

func (p *blOrderProbe) ReadBinding(ctx context.Context, intentID, attemptID string) (BindingResult, error) {
	p.add("binding")
	return p.live.ReadBinding(ctx, intentID, attemptID)
}

func (p *blOrderProbe) LockScope(ctx context.Context, tx pgx.Tx, chainID int64, sender string) error {
	p.add("scope")
	return p.live.LockScope(ctx, tx, chainID, sender)
}

// blHeldSink blocks the delivery region at its byte write until release, so
// the test can probe the locks the admission transaction holds meanwhile.
type blHeldSink struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blHeldSink) WriteDelivery(_ context.Context, _ []byte) error {
	s.once.Do(func() { close(s.entered) })
	<-s.release
	return nil
}

// TestSignerBindingLive008Mapping is the T028 fact half: every live 008
// (Outcome, Annotations) shape maps to exactly the gates.md:149-158 class, on
// rows created by 008's own APIs.
func TestSignerBindingLive008Mapping(t *testing.T) {
	pool := signerAuthStartPG(t)
	ctx := context.Background()
	gateReset006(t, pool)
	view := newBLChainView()
	alloc := blAllocator(t, pool, view)
	provider := blProvider(pool)
	adapter := NewLiveBindingReader(provider)
	n := 0
	next := func() *blFixture { n++; return blSeed(t, pool, alloc, n) }

	t.Run("bound clean maps to matches", func(t *testing.T) {
		f := next()
		resp, class, cerr := blMapRead(t, ctx, provider, nonce.ReadRequest{IntentID: f.intentID})
		if resp.Outcome != nonce.ReadBound {
			t.Fatalf("008 outcome = %q, want bound", resp.Outcome)
		}
		if class != BindingMatches || cerr != nil {
			t.Fatalf("class = %s err = %v, want matches", bindingClassName(class), cerr)
		}
		blWantRefusal(t, class, cerr, "")
		// The production adapter interface agrees with the mapping.
		if got, err := adapter.ReadBinding(ctx, f.intentID, f.attemptID); err != nil || got != BindingMatches {
			t.Fatalf("adapter.ReadBinding = (%s, %v), want matches", bindingClassName(got), err)
		}
	})

	t.Run("not_bound maps to absent", func(t *testing.T) {
		resp, class, cerr := blMapRead(t, ctx, provider, nonce.ReadRequest{IntentID: "pi-bl-absent"})
		if resp.Outcome != nonce.ReadNotBound || class != BindingAbsent || cerr != nil {
			t.Fatalf("(outcome, class, err) = (%q, %s, %v), want (not_bound, absent, nil)",
				resp.Outcome, bindingClassName(class), cerr)
		}
		blWantRefusal(t, class, cerr, ClassBindingAbsent)
	})

	t.Run("mismatch maps to conflict", func(t *testing.T) {
		f := next()
		otherChain := gateChainID + 1
		resp, class, cerr := blMapRead(t, ctx, provider,
			nonce.ReadRequest{IntentID: f.intentID, ExpectedChainID: &otherChain, ExpectedSender: f.sender})
		if resp.Outcome != nonce.ReadMismatch || class != BindingConflict || cerr != nil {
			t.Fatalf("wrong chain: (outcome, class, err) = (%q, %s, %v), want (mismatch, conflict, nil)",
				resp.Outcome, bindingClassName(class), cerr)
		}
		blWantRefusal(t, class, cerr, ClassBindingConflict)

		rightChain := gateChainID
		resp, class, cerr = blMapRead(t, ctx, provider,
			nonce.ReadRequest{IntentID: f.intentID, ExpectedChainID: &rightChain,
				ExpectedSender: "0x00000000000000000000000000000000000000ff"})
		if resp.Outcome != nonce.ReadMismatch || class != BindingConflict || cerr != nil {
			t.Fatalf("wrong sender: (outcome, class, err) = (%q, %s, %v), want (mismatch, conflict, nil)",
				resp.Outcome, bindingClassName(class), cerr)
		}
	})

	t.Run("bound with a disabled registry maps to paused", func(t *testing.T) {
		f := next()
		res := blAdmin(t, pool, nil, nonce.AdminRequest{
			Action: nonce.AdminActionRegistryDisable, OperationID: "bl-disable-" + strconv.Itoa(n),
			ChainID: gateChainID, Sender: f.sender,
		})
		if res.Outcome != nonce.AdminApplied {
			t.Fatalf("008 registry_disable = %q, want applied", res.Outcome)
		}
		resp, class, cerr := blMapRead(t, ctx, provider, nonce.ReadRequest{IntentID: f.intentID})
		if resp.Outcome != nonce.ReadBound || class != BindingPaused || cerr != nil {
			t.Fatalf("(outcome, class, err) = (%q, %s, %v), want (bound, paused, nil)",
				resp.Outcome, bindingClassName(class), cerr)
		}
		if resp.Annotations == nil || resp.Annotations.RegistryState != nonce.RegistryDisabled {
			t.Fatalf("annotations = %+v, want registry disabled", resp.Annotations)
		}
		blWantRefusal(t, class, cerr, ClassBindingPaused)
	})

	t.Run("bound under a live hold maps to paused", func(t *testing.T) {
		f := next()
		// A divergent view (latest == pending == 2 over a scope whose durable
		// frontier is nonce 0) makes 008's own classification establish a real
		// hold and refuse the next admission.
		view.set("0x2", "0x2")
		if _, outcome, err := alloc.Allocate(ctx, nonce.AllocationRequest{
			IntentID: f.intentID + "-hold", ChainID: gateChainID, Sender: f.sender, AuthorizationID: f.authzID,
		}); !nonce.IsOutcome(err, nonce.OutcomeScopeHeld) {
			t.Fatalf("divergent allocate = (%q, %v), want scope_held", outcome, err)
		}
		view.set("0x0", "0x0")

		resp, class, cerr := blMapRead(t, ctx, provider, nonce.ReadRequest{IntentID: f.intentID})
		if resp.Outcome != nonce.ReadBound || class != BindingPaused || cerr != nil {
			t.Fatalf("(outcome, class, err) = (%q, %s, %v), want (bound, paused, nil)",
				resp.Outcome, bindingClassName(class), cerr)
		}
		if resp.Annotations == nil || resp.Annotations.Gate.State != nonce.ReadGateHeld ||
			len(resp.Annotations.Gate.Causes) == 0 {
			t.Fatalf("annotations = %+v, want an active hold", resp.Annotations)
		}
		blWantRefusal(t, class, cerr, ClassBindingPaused)
	})

	t.Run("bound with recovery != none maps to paused", func(t *testing.T) {
		f := next()
		gateSeedRecovery(t, pool, "rec-bl-1", "replaying", 3)
		t.Cleanup(func() { gateReset006(t, pool) })

		resp, class, cerr := blMapRead(t, ctx, provider, nonce.ReadRequest{IntentID: f.intentID})
		if resp.Outcome != nonce.ReadBound || class != BindingPaused || cerr != nil {
			t.Fatalf("(outcome, class, err) = (%q, %s, %v), want (bound, paused, nil)",
				resp.Outcome, bindingClassName(class), cerr)
		}
		if resp.Annotations == nil || resp.Annotations.Recovery.State != nonce.RecoveryRecovering {
			t.Fatalf("annotations = %+v, want recovery recovering", resp.Annotations)
		}
		blWantRefusal(t, class, cerr, ClassBindingPaused)
		gateReset006(t, pool)
	})

	t.Run("terminal maps to terminal", func(t *testing.T) {
		f := next()
		res := blAdmin(t, pool, view, nonce.AdminRequest{
			Action: nonce.AdminActionBindingRelease, OperationID: "bl-rel-" + strconv.Itoa(n),
			ChainID: gateChainID, Sender: f.sender, BindingID: f.bindingID,
			Evidence: "bl-test: no-side-effect release with a fresh consistent observation",
		})
		if res.Outcome != nonce.AdminApplied {
			t.Fatalf("008 binding_release = %q (%s), want applied", res.Outcome, res.Detail)
		}
		resp, class, cerr := blMapRead(t, ctx, provider, nonce.ReadRequest{IntentID: f.intentID})
		if resp.Outcome != nonce.ReadTerminal || class != BindingTerminal || cerr != nil {
			t.Fatalf("(outcome, class, err) = (%q, %s, %v), want (terminal, terminal, nil)",
				resp.Outcome, bindingClassName(class), cerr)
		}
		blWantRefusal(t, class, cerr, ClassBindingTerminal)
	})

	t.Run("unavailable maps to read_failed", func(t *testing.T) {
		f := next()
		closed := nonce.NewRebuildGate() // zero value: closed
		blocked := nonce.NewReadProvider(pool, blReadToken, closed)
		resp, class, cerr := blMapRead(t, ctx, blocked, nonce.ReadRequest{IntentID: f.intentID})
		if resp.Outcome != nonce.ReadUnavailable || class != BindingReadFailed || cerr == nil {
			t.Fatalf("(outcome, class, err) = (%q, %s, %v), want (unavailable, read_failed, non-nil)",
				resp.Outcome, bindingClassName(class), cerr)
		}
		if got := BindingRefusal(class, cerr); got != ClassBindingReadFailed {
			t.Fatalf("BindingRefusal = %q, want binding_read_failed", got)
		}
		// The same identity is bound on the open provider: the class comes from
		// the read path, not from the fixture.
		if open, oerr := provider.ReadByBindingID(ctx, f.bindingID); oerr != nil || open.Outcome != nonce.ReadBound {
			t.Fatalf("open-gate read = (%q, %v), want bound", open.Outcome, oerr)
		}
	})

	t.Run("standalone fact read holds no scope lock", func(t *testing.T) {
		f := next()
		resp, err := provider.ReadByBindingID(ctx, f.bindingID)
		if err != nil || resp.Outcome != nonce.ReadBound {
			t.Fatalf("standalone fact read = (%q, %v), want bound", resp.Outcome, err)
		}
		if err := blScopeForUpdateAttempt(t, pool, f); err != nil {
			t.Fatalf("008 writer after the fact read returned = %v, want success (the fact call must hold no lock)", err)
		}
	})
}

// TestSignerBindingLive008Delivery is the T028 lock + delivery half: T034's
// protocol wired against the live adapter end to end, with the scope-lock
// semantics asserted against the live 008 row in both directions.
func TestSignerBindingLive008Delivery(t *testing.T) {
	pool := signerAuthStartPG(t)
	ctx := context.Background()
	gateReset006(t, pool)
	view := newBLChainView()
	alloc := blAllocator(t, pool, view)
	provider := blProvider(pool)
	live := NewLiveBindingReader(provider)
	deps := func(binding BindingReader, scope ScopeLocker) DeliveryDeps {
		return DeliveryDeps{DB: pool, Binding: binding, ScopeLock: scope}
	}
	n := 100
	next := func() *blFixture { n++; return blSeed(t, pool, alloc, n) }
	type blOutcome struct {
		res *DeliveryResult
		err error
	}

	t.Run("end to end delivery over the live adapter", func(t *testing.T) {
		f := next()
		sink := &dlvSink{}
		res, err := Deliver(ctx, deps(live, live), Caller{ID: f.callerID, CanSign: true}, f.requestID, sink)
		if err != nil {
			t.Fatalf("Deliver: %v", err)
		}
		if res.Verdict != VerdictDelivered || res.AttemptSeq != 1 || sink.count() != 1 {
			t.Fatalf("result = %+v sink=%d, want delivered attempt 1 with one payload", res, sink.count())
		}
		facts := dlvPayloadFacts(t, sink.last())
		if facts["signature"] != f.signature || facts["tx_hash"] != f.txHash ||
			facts["signing_request_id"] != f.requestID || facts["delivery"] != "delivered" {
			t.Fatalf("payload facts = %v, want the persisted signature/hash facts", facts)
		}
		var verdict, bindingClass string
		var deliveredAt *time.Time
		if err := pool.QueryRow(ctx,
			`SELECT verdict, binding_class, delivered_at FROM delivery_admissions
			   WHERE signing_request_row = $1`, f.rowID).Scan(&verdict, &bindingClass, &deliveredAt); err != nil {
			t.Fatalf("read admission: %v", err)
		}
		if verdict != string(VerdictDelivered) || bindingClass != "matches" || deliveredAt == nil {
			t.Fatalf("admission = %s/%s delivered_at=%v, want delivered/matches", verdict, bindingClass, deliveredAt)
		}
		if !dlvDeliveredMarker(t, pool, f.rowID) {
			t.Fatal("delivered marker missing")
		}
		dlvAssertNoResign(t, pool, &f.dlvFixture)

		// 009 is read-only on 008 state: the live binding is untouched.
		after, aerr := provider.ReadByBindingID(ctx, f.bindingID)
		if aerr != nil || after.Outcome != nonce.ReadBound || after.Binding == nil {
			t.Fatalf("post-delivery 008 read = (%q, %v), want bound", after.Outcome, aerr)
		}
		if after.Binding.BindingID != f.bindingID || after.Binding.IntentID != f.intentID ||
			after.Binding.State != nonce.StateAllocated || after.Binding.Nonce != f.nonce {
			t.Fatalf("008 binding mutated by 009: %+v", after.Binding)
		}
	})

	t.Run("delivery takes the live scope lock before the binding read", func(t *testing.T) {
		f := next()
		probe := newBLOrderProbe(live)
		sink := &dlvSink{}
		res, err := Deliver(ctx, deps(probe, probe), Caller{ID: f.callerID, CanSign: true}, f.requestID, sink)
		if err != nil || res.Verdict != VerdictDelivered {
			t.Fatalf("probe delivery = %+v / %v, want delivered", res, err)
		}
		order := probe.snapshot()
		if len(order) < 2 || order[0] != "scope" || order[1] != "binding" {
			t.Fatalf("delivery call order = %v, want the live scope-row FOR SHARE before the live binding read", order)
		}
	})

	t.Run("live 008 pause blocks delivery status only", func(t *testing.T) {
		f := next()
		res := blAdmin(t, pool, nil, nonce.AdminRequest{
			Action: nonce.AdminActionRegistryDisable, OperationID: "bl-dlv-disable-" + strconv.Itoa(n),
			ChainID: gateChainID, Sender: f.sender,
		})
		if res.Outcome != nonce.AdminApplied {
			t.Fatalf("008 registry_disable = %q, want applied", res.Outcome)
		}
		sink := &dlvSink{}
		dres, err := Deliver(ctx, deps(live, live), Caller{ID: f.callerID, CanSign: true}, f.requestID, sink)
		if dres == nil || dres.Verdict != VerdictBlocked {
			t.Fatalf("paused delivery = %+v / %v, want blocked", dres, err)
		}
		signerAuthRefusal(t, err, ClassSignatureWithheld)
		if sink.count() != 0 {
			t.Fatalf("blocked delivery wrote %d payload(s), want 0 signature bytes", sink.count())
		}
		var verdict, bindingClass, reason string
		if err := pool.QueryRow(ctx,
			`SELECT verdict, binding_class, reason FROM delivery_admissions WHERE signing_request_row = $1`,
			f.rowID).Scan(&verdict, &bindingClass, &reason); err != nil {
			t.Fatalf("read blocked admission: %v", err)
		}
		if verdict != string(VerdictBlocked) || bindingClass != "paused" || reason != string(ClassSignatureWithheld) {
			t.Fatalf("blocked admission = %s/%s/%s, want blocked/paused/signature_withheld", verdict, bindingClass, reason)
		}
		if _, _, detail := dlvLastAudit(t, pool, f.requestID); !strings.Contains(detail, "binding_class=paused") {
			t.Fatalf("audit detail %q missing the observed binding class", detail)
		}
		dlvAssertNoResign(t, pool, &f.dlvFixture)
	})

	t.Run("delivery waits for a live 008 scope writer", func(t *testing.T) {
		f := next()
		holder := blHoldScopeRow(t, pool, "UPDATE", f)
		sink := &dlvSink{}
		done := make(chan blOutcome, 1)
		go func() {
			res, err := Deliver(ctx, deps(live, live), Caller{ID: f.callerID, CanSign: true}, f.requestID, sink)
			done <- blOutcome{res, err}
		}()
		blWantScopeWait(t, pool)
		select {
		case out := <-done:
			t.Fatalf("delivery completed while an 008 writer held the scope row: %+v / %v", out.res, out.err)
		case <-time.After(200 * time.Millisecond):
		}
		// Blocked at the head of the fixed order: no region ran.
		if got := dlvAdmissionCount(t, pool, f.rowID); got != 0 {
			t.Fatalf("blocked delivery wrote %d admission row(s), want 0", got)
		}
		if sink.count() != 0 {
			t.Fatalf("blocked delivery wrote %d payload(s), want 0", sink.count())
		}
		if err := holder.Commit(ctx); err != nil {
			t.Fatalf("commit the 008 writer: %v", err)
		}
		select {
		case out := <-done:
			if out.err != nil || out.res.Verdict != VerdictDelivered {
				t.Fatalf("delivery after the writer committed = %+v / %v, want delivered", out.res, out.err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("delivery did not complete after the 008 writer committed")
		}
	})

	t.Run("the admission transaction holds the scope share against 008 writers", func(t *testing.T) {
		f := next()
		sink := &blHeldSink{entered: make(chan struct{}), release: make(chan struct{})}
		done := make(chan blOutcome, 1)
		go func() {
			res, err := Deliver(ctx, deps(live, live), Caller{ID: f.callerID, CanSign: true}, f.requestID, sink)
			done <- blOutcome{res, err}
		}()
		select {
		case <-sink.entered:
		case <-time.After(10 * time.Second):
			t.Fatal("delivery never reached its write region")
		}
		// The delivery transaction is inside its protected region and holds the
		// live 008 scope row FOR SHARE: both 008 writer shapes must wait.
		if err := blScopeUpdateAttempt(t, pool, f); !blLockNotAvailable(err) {
			t.Fatalf("008 writer against the held scope share = %v, want lock_not_available", err)
		}
		if err := blScopeForUpdateAttempt(t, pool, f); !blLockNotAvailable(err) {
			t.Fatalf("008 FOR UPDATE against the held scope share = %v, want lock_not_available", err)
		}
		close(sink.release)
		select {
		case out := <-done:
			if out.err != nil || out.res.Verdict != VerdictDelivered {
				t.Fatalf("delivery after the writer probes = %+v / %v, want delivered", out.res, out.err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("delivery did not complete after the region was released")
		}
		// The share lock died with the transaction: the writer now proceeds.
		if err := blScopeUpdateAttempt(t, pool, f); err != nil {
			t.Fatalf("008 writer after the delivery committed = %v, want success", err)
		}
	})
}
