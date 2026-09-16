// allocate_test.go is the T012 EARLY-VALIDATION suite for the admission core:
// scripted querier/RPC doubles only (no acceptance claim rests on them;
// real-PostgreSQL integration belongs to T018/T019/T022). It pins the
// T-allocate ordering, T-replay/T-conflict decision points, and T-converge's
// rollback-then-fixed-order-classify contract.
package nonce

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

const (
	allocTestChain  = int64(31337)
	allocTestSender = "0x2222222222222222222222222222222222222222"
	allocTestIntent = "intent-012"
	allocTestAuth   = "wa-012"
)

func allocRequest() AllocationRequest {
	return AllocationRequest{
		IntentID:        allocTestIntent,
		ChainID:         allocTestChain,
		Sender:          allocTestSender,
		AuthorizationID: allocTestAuth,
	}
}

func allocExistingBinding(authorizationID string) Binding {
	now := time.Now().UTC()
	return Binding{
		BindingID:               "nb-orig",
		IntentID:                allocTestIntent,
		ChainID:                 allocTestChain,
		Sender:                  allocTestSender,
		Nonce:                   big.NewInt(41),
		State:                   StateAllocated,
		AuthorizationID:         authorizationID,
		AuthorizationVersion:    strings.Repeat("a", 64),
		RegistrySeq:             7,
		AllocationObservationID: "no-prev",
		CreatedAt:               now,
		UpdatedAt:               now,
	}
}

// allocBindingValues renders a Binding in bindingColumnsSQL scan order.
func allocBindingValues(b Binding) []any {
	return []any{
		b.BindingID, b.IntentID, b.ChainID, b.Sender, FormatDecimal(b.Nonce), b.State,
		b.AuthorizationID, b.AuthorizationVersion, b.RegistrySeq, b.AllocationObservationID,
		b.CreatedAt, b.UpdatedAt, b.ConsumedAt, b.ReleasedAt, b.ReleaseOperationID,
	}
}

func allocRegistryValues(state string, seq int64) []any {
	now := time.Now().UTC()
	return []any{allocTestChain, allocTestSender, state, seq, now, now}
}

func allocHoldValues(cause string) []any {
	return []any{
		"nh-test", allocTestChain, allocTestSender, cause, HoldStatusActive, time.Now().UTC(),
		"no-prev", "test hold",
		nil, nil, nil, nil, nil,
	}
}

func allocAuthValues() []any {
	return []any{
		allocTestChain,
		"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		"100",
		RegistryActive,
		nil,
	}
}

// allocFakeRows implements pgx.Rows for the multi-row scope readers.
type allocFakeRows struct {
	data [][]any
	err  error
	i    int
}

func allocRowSet(data ...[]any) *allocFakeRows { return &allocFakeRows{data: data} }

func (r *allocFakeRows) Close()                                       {}
func (r *allocFakeRows) Err() error                                   { return r.err }
func (r *allocFakeRows) CommandTag() pgconn.CommandTag                { return pgconn.NewCommandTag("SELECT 0") }
func (r *allocFakeRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *allocFakeRows) Values() ([]any, error)                       { return nil, nil }
func (r *allocFakeRows) RawValues() [][]byte                          { return nil }
func (r *allocFakeRows) Conn() *pgx.Conn                              { return nil }
func (r *allocFakeRows) TypeMap() *pgtype.Map                         { return nil }

func (r *allocFakeRows) Next() bool {
	if r.i >= len(r.data) {
		return false
	}
	r.i++
	return true
}

func (r *allocFakeRows) Scan(dest ...any) error {
	if r.i < 1 || r.i > len(r.data) {
		return pgx.ErrNoRows
	}
	return fakeRow{vals: r.data[r.i-1]}.Scan(dest...)
}

// allocSQL collapses a statement for order assertions.
func allocSQL(sql string) string { return strings.Join(strings.Fields(sql), " ") }

// allocFakeTx is a txQuerier with a transaction surface and an ordered event
// log ("txN:exec:…", "txN:query:…", "txN:commit"/"txN:rollback").
type allocFakeTx struct {
	f  *allocFixture
	id int
	*fakeQuerier
	committed  bool
	rolledBack bool
	commitErr  error
	execArgs   [][]any
}

var _ allocTx = (*allocFakeTx)(nil)

func (t *allocFakeTx) log(ev string) { t.f.record(fmt.Sprintf("tx%d:%s", t.id, ev)) }

func (t *allocFakeTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	t.log("exec:" + allocSQL(sql))
	t.execArgs = append(t.execArgs, append([]any(nil), args...))
	return t.fakeQuerier.Exec(ctx, sql, args...)
}

func (t *allocFakeTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	t.log("query:" + allocSQL(sql))
	return t.fakeQuerier.QueryRow(ctx, sql, args...)
}

func (t *allocFakeTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	t.log("query:" + allocSQL(sql))
	return t.f.rowsHandler(sql, args)
}

func (t *allocFakeTx) Commit(context.Context) error {
	t.committed = true
	t.log("commit")
	return t.commitErr
}

func (t *allocFakeTx) Rollback(context.Context) error {
	t.rolledBack = true
	t.f.racing = true // the winner of the race is visible after the rollback
	t.log("rollback")
	return nil
}

// allocFixture scripts one allocator run over a single event log shared by
// the RPC double and every transaction the run opens.
type allocFixture struct {
	t   *testing.T
	rpc *obsFakeRPC

	mu  sync.Mutex
	log []string
	seq int

	// racing flips once any transaction rolls back; T-converge then reads the
	// durable winner instead of the pre-race miss.
	racing bool

	// Scripted durable facts (nil slice → no row for the QueryRow readers).
	gateHit    bool
	registry   []any
	holdRows   [][]any
	auth       []any
	existing   []any
	winner     []any
	otherNonce []any
	bindRows   [][]any
	scope      []any

	insertErr func(sql string) error

	txs []*allocFakeTx
}

func newAllocFixture(t *testing.T, rpc *obsFakeRPC) *allocFixture {
	t.Helper()
	if rpc == nil {
		rpc = &obsFakeRPC{latest: "0x0", pending: "0x0", block: obsTestHead()}
	}
	return &allocFixture{
		t:        t,
		rpc:      rpc,
		registry: allocRegistryValues(RegistryActive, 7),
		auth:     allocAuthValues(),
	}
}

func (f *allocFixture) record(ev string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.log = append(f.log, ev)
}

func (f *allocFixture) indexOf(substr string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, ev := range f.log {
		if strings.Contains(ev, substr) {
			return i
		}
	}
	return -1
}

func (f *allocFixture) logString() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return strings.Join(f.log, "\n")
}

func (f *allocFixture) requireOrder(before, after string) {
	f.t.Helper()
	i, j := f.indexOf(before), f.indexOf(after)
	if i < 0 || j < 0 {
		f.t.Fatalf("missing log entry %q (idx %d) or %q (idx %d):\n%s", before, i, after, j, f.logString())
	}
	if i >= j {
		f.t.Fatalf("order violation: %q (idx %d) must precede %q (idx %d):\n%s", before, i, after, j, f.logString())
	}
}

func (f *allocFixture) requireAbsent(substr string) {
	f.t.Helper()
	if i := f.indexOf(substr); i >= 0 {
		f.t.Fatalf("unexpected log entry %q at %d:\n%s", substr, i, f.logString())
	}
}

// rowHandler answers the QueryRow readers of the T-allocate transaction.
func (f *allocFixture) rowHandler(sql string, _ []any) pgx.Row {
	switch {
	case strings.Contains(sql, "indexer_lease"):
		return rows("nonce-008", int64(0), true)
	case strings.Contains(sql, "nonce_scope_state") && strings.Contains(sql, "FOR UPDATE"):
		return rows(allocTestChain)
	case strings.Contains(sql, "indexer_pause"),
		strings.Contains(sql, "log_pause"),
		strings.Contains(sql, "deposit_pause"):
		if f.gateHit {
			return rows(int64(1))
		}
		return noRows()
	case strings.Contains(sql, "reorg_recovery"):
		return noRows()
	case strings.Contains(sql, "nonce_wallet_registry"):
		if f.registry == nil {
			return noRows()
		}
		return rows(f.registry...)
	case strings.Contains(sql, "withdrawal_authorizations"):
		if f.auth == nil {
			return noRows()
		}
		return rows(f.auth...)
	case strings.Contains(sql, "WHERE intent_id = $1"):
		if f.racing {
			if f.winner == nil {
				return noRows()
			}
			return rows(f.winner...)
		}
		if f.existing == nil {
			return noRows()
		}
		return rows(f.existing...)
	case strings.Contains(sql, "AND nonce = $3::numeric"):
		if f.otherNonce == nil {
			return noRows()
		}
		return rows(f.otherNonce...)
	case strings.Contains(sql, "reconciled_floor"):
		if f.scope == nil {
			return noRows()
		}
		return rows(f.scope...)
	default:
		return noRows()
	}
}

// rowsHandler answers the multi-row readers (active holds, scope bindings).
func (f *allocFixture) rowsHandler(sql string, _ []any) (pgx.Rows, error) {
	switch {
	case strings.Contains(sql, "nonce_scope_holds"):
		return allocRowSet(f.holdRows...), nil
	case strings.Contains(sql, "FROM nonce_bindings"):
		return allocRowSet(f.bindRows...), nil
	}
	return allocRowSet(), nil
}

func (f *allocFixture) newTx() *allocFakeTx {
	f.seq++
	tx := &allocFakeTx{
		f:  f,
		id: f.seq,
		fakeQuerier: &fakeQuerier{
			handler: f.rowHandler,
			execErr: func(sql string, _ []any) error {
				if f.insertErr == nil {
					return nil
				}
				return f.insertErr(sql)
			},
		},
	}
	f.txs = append(f.txs, tx)
	return tx
}

func (f *allocFixture) begin(context.Context) (allocTx, error) {
	tx := f.newTx()
	tx.log("begin")
	return tx, nil
}

// allocRPC records RPC calls on the fixture's ordered log so tests can prove
// every sample ran before BEGIN.
type allocRPC struct {
	inner rpcCaller
	f     *allocFixture
}

func (r allocRPC) CallContext(ctx context.Context, result any, method string, args ...any) error {
	r.f.record("rpc:" + method)
	return r.inner.CallContext(ctx, result, method, args...)
}

func (f *allocFixture) allocator() *Allocator {
	return &Allocator{
		observer: obsTestObserver(allocRPC{inner: f.rpc, f: f}),
		begin:    f.begin,
	}
}

// execArgsFor returns the recorded args of the first exec whose statement
// contains substr.
func (f *allocFixture) execArgsFor(tx *allocFakeTx, substr string) []any {
	for i, sql := range tx.recordedExecs() {
		if strings.Contains(allocSQL(sql), substr) && i < len(tx.execArgs) {
			return tx.execArgs[i]
		}
	}
	return nil
}

func (f *allocFixture) execCount(tx *allocFakeTx, substr string) int {
	n := 0
	for _, sql := range tx.recordedExecs() {
		if strings.Contains(allocSQL(sql), substr) {
			n++
		}
	}
	return n
}

func TestAllocationRequestValidate(t *testing.T) {
	valid := allocRequest()
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid request refused: %v", err)
	}
	if err := (AllocationRequest{
		IntentID:        strings.Repeat("i", 128),
		ChainID:         7,
		Sender:          allocTestSender,
		AuthorizationID: strings.Repeat("a", 128),
	}).Validate(); err != nil {
		t.Fatalf("max-length identifiers refused: %v", err)
	}

	cases := []struct {
		name string
		mut  func(*AllocationRequest)
	}{
		{"empty-intent", func(r *AllocationRequest) { r.IntentID = "" }},
		{"intent-too-long", func(r *AllocationRequest) { r.IntentID = strings.Repeat("i", 129) }},
		{"intent-space", func(r *AllocationRequest) { r.IntentID = "a b" }},
		{"intent-control", func(r *AllocationRequest) { r.IntentID = "a\tb" }},
		{"empty-authorization", func(r *AllocationRequest) { r.AuthorizationID = "" }},
		{"authorization-too-long", func(r *AllocationRequest) { r.AuthorizationID = strings.Repeat("a", 129) }},
		{"authorization-control", func(r *AllocationRequest) { r.AuthorizationID = "a\nb" }},
		{"uppercase-sender", func(r *AllocationRequest) { r.Sender = "0x222222222222222222222222222222222222222A" }},
		{"short-sender", func(r *AllocationRequest) { r.Sender = "0x22" }},
		{"zero-chain", func(r *AllocationRequest) { r.ChainID = 0 }},
		{"negative-chain", func(r *AllocationRequest) { r.ChainID = -7 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := valid
			tc.mut(&req)
			if err := req.Validate(); err == nil {
				t.Fatal("malformed request accepted")
			}
		})
	}
}

// TestAllocateInvalidInputStopsBeforeRPCOrTx pins "pre-tx input validation":
// a malformed request never samples the chain and never opens a transaction.
func TestAllocateInvalidInputStopsBeforeRPCOrTx(t *testing.T) {
	f := newAllocFixture(t, nil)
	req := allocRequest()
	req.IntentID = ""

	_, outcome, err := f.allocator().Allocate(context.Background(), req)
	if err == nil || outcome != "" {
		t.Fatalf("invalid input = (%q, %v), want plain error with no outcome", outcome, err)
	}
	if IsOutcome(err, OutcomeTemporarilyUnavailable) {
		t.Fatalf("validation surfaced as a wire outcome: %v", err)
	}
	if got := len(f.rpc.recordedCalls()); got != 0 {
		t.Fatalf("invalid input made %d RPC calls, want 0", got)
	}
	if len(f.txs) != 0 {
		t.Fatalf("invalid input opened %d transactions, want 0", len(f.txs))
	}
}

// TestAllocateReplayReturnsOriginalUntouched pins T-replay ordering: the
// authorization is validated (FOR SHARE) before the intent re-read, the equal
// input returns the original binding, and zero rows are written.
func TestAllocateReplayReturnsOriginalUntouched(t *testing.T) {
	f := newAllocFixture(t, nil)
	f.existing = allocBindingValues(allocExistingBinding(allocTestAuth))

	binding, outcome, err := f.allocator().Allocate(context.Background(), allocRequest())
	if err != nil {
		t.Fatalf("replay returned error %v", err)
	}
	if outcome != OutcomeReplayed || binding == nil || binding.BindingID != "nb-orig" || binding.Nonce.Cmp(big.NewInt(41)) != 0 {
		t.Fatalf("replay = (%+v, %q), want the original nb-orig/41", binding, outcome)
	}
	if !f.tx().rolledBack || f.tx().committed {
		t.Fatalf("replay tx = (rolledBack=%v, committed=%v), want rollback only", f.tx().rolledBack, f.tx().committed)
	}
	f.requireAbsent("INSERT INTO nonce_observations")
	f.requireAbsent("INSERT INTO nonce_bindings")
	f.requireOrder("withdrawal_authorizations", "WHERE intent_id = $1")
	f.requireOrder("tx1:begin", "withdrawal_authorizations")
}

// TestAllocateConflictOnDifferingInput pins T-conflict: a differing input set
// fail-closes with allocation_conflict and never writes a second binding.
func TestAllocateConflictOnDifferingInput(t *testing.T) {
	f := newAllocFixture(t, nil)
	f.existing = allocBindingValues(allocExistingBinding("wa-other"))

	binding, outcome, err := f.allocator().Allocate(context.Background(), allocRequest())
	if !IsOutcome(err, OutcomeAllocationConflict) || outcome != OutcomeAllocationConflict {
		t.Fatalf("differing input = (%v, %q), want allocation_conflict", err, outcome)
	}
	if binding != nil {
		t.Fatalf("conflict returned binding %+v, want none", binding)
	}
	if !f.tx().rolledBack || f.tx().committed {
		t.Fatalf("conflict tx = (rolledBack=%v, committed=%v), want rollback only", f.tx().rolledBack, f.tx().committed)
	}
	f.requireAbsent("INSERT INTO nonce_observations")
	f.requireAbsent("INSERT INTO nonce_bindings")
}

// TestAllocateCommitOrderAndEvidence pins the T-allocate happy path: the RPC
// sample precedes BEGIN, the lock order is coord → gate reads → scope row →
// authorization row → own rows, and the commits land observation → binding →
// creation event.
func TestAllocateCommitOrderAndEvidence(t *testing.T) {
	f := newAllocFixture(t, nil)

	binding, outcome, err := f.allocator().Allocate(context.Background(), allocRequest())
	if err != nil {
		t.Fatalf("allocate error = %v", err)
	}
	if outcome != OutcomeAllocated || binding == nil {
		t.Fatalf("allocate = (%+v, %q), want a new binding", binding, outcome)
	}
	if binding.Nonce.Sign() != 0 || binding.State != StateAllocated || binding.RegistrySeq != 7 {
		t.Fatalf("binding = %+v, want candidate 0 / allocated / registry_seq 7", binding)
	}
	if binding.AuthorizationID != allocTestAuth || len(binding.AuthorizationVersion) != 64 ||
		strings.ToLower(binding.AuthorizationVersion) != binding.AuthorizationVersion {
		t.Fatalf("binding authorization = %q/%q, want id + 64 lowercase hex digest",
			binding.AuthorizationID, binding.AuthorizationVersion)
	}
	if !strings.HasPrefix(binding.AllocationObservationID, "no-") {
		t.Fatalf("allocation observation = %q, want a minted no- id", binding.AllocationObservationID)
	}
	if !f.tx().committed || f.tx().rolledBack {
		t.Fatalf("allocate tx = (committed=%v, rolledBack=%v), want commit only", f.tx().committed, f.tx().rolledBack)
	}

	// RPC strictly before BEGIN; zero RPC after the transaction opened.
	f.requireOrder("rpc:eth_getBlockByNumber", "tx1:begin")
	f.requireOrder("tx1:begin", "tx1:exec:SET LOCAL statement_timeout")
	f.requireOrder("tx1:exec:SET LOCAL statement_timeout", "tx1:query:SELECT owner_id, fencing_token")
	// Lock order: coordination row → pause reads → registry → scope row FOR
	// UPDATE → holds → authorization row FOR SHARE → own rows.
	f.requireOrder("tx1:query:SELECT owner_id, fencing_token", "tx1:query:SELECT 1 FROM indexer_pause")
	f.requireOrder("tx1:query:SELECT 1 FROM indexer_pause", "tx1:query:SELECT chain_id, sender, state, registry_seq")
	f.requireOrder("tx1:query:SELECT chain_id, sender, state, registry_seq", "tx1:query:SELECT chain_id FROM nonce_scope_state WHERE chain_id = $1 AND sender = $2 FOR UPDATE")
	f.requireOrder("FOR UPDATE", "tx1:query:SELECT hold_id, chain_id, sender, cause")
	f.requireOrder("tx1:query:SELECT hold_id, chain_id, sender, cause", "withdrawal_authorizations")
	f.requireOrder("withdrawal_authorizations", "WHERE intent_id = $1")
	f.requireOrder("WHERE intent_id = $1", "FROM nonce_bindings WHERE chain_id = $1 AND sender = $2 ORDER BY nonce")
	// Evidence order: observation always before the binding and its event.
	f.requireOrder("tx1:exec:INSERT INTO nonce_observations", "tx1:exec:INSERT INTO nonce_bindings")
	f.requireOrder("tx1:exec:INSERT INTO nonce_bindings", "tx1:exec:INSERT INTO nonce_binding_events")

	obsArgs := f.execArgsFor(f.tx(), "INSERT INTO nonce_observations")
	if obsArgs == nil || obsArgs[4] != ClassificationConsistent || obsArgs[3] != ObservationKindAllocation {
		t.Fatalf("observation args = %v, want allocation/consistent", obsArgs)
	}
	if obsArgs[0] != binding.AllocationObservationID {
		t.Fatalf("observation id arg = %v, binding anchors %q", obsArgs[0], binding.AllocationObservationID)
	}
	bindArgs := f.execArgsFor(f.tx(), "INSERT INTO nonce_bindings")
	if bindArgs == nil || bindArgs[9] != binding.AllocationObservationID {
		t.Fatalf("binding args = %v, want allocation_observation_id %q", bindArgs, binding.AllocationObservationID)
	}
	evArgs := f.execArgsFor(f.tx(), "INSERT INTO nonce_binding_events")
	if evArgs == nil || evArgs[1] != nil || evArgs[2] != StateAllocated {
		t.Fatalf("creation event args = %v, want NULL from_state → allocated", evArgs)
	}
}

// TestAllocateClassificationRefusalCommitsObservationAndHold pins the "else
// commit observation (+ hold when the matrix says so) with no binding" branch:
// a divergent view (L > P) establishes the divergence hold, commits both
// evidence rows, and refuses scope_held.
func TestAllocateClassificationRefusalCommitsObservationAndHold(t *testing.T) {
	f := newAllocFixture(t, &obsFakeRPC{latest: "0x5", pending: "0x4", block: obsTestHead()})

	binding, outcome, err := f.allocator().Allocate(context.Background(), allocRequest())
	if !IsOutcome(err, OutcomeScopeHeld) || outcome != OutcomeScopeHeld {
		t.Fatalf("divergence = (%v, %q), want scope_held", err, outcome)
	}
	if binding != nil {
		t.Fatalf("held scope returned binding %+v", binding)
	}
	if !f.tx().committed || f.tx().rolledBack {
		t.Fatalf("refusal tx = (committed=%v, rolledBack=%v), want observation commit", f.tx().committed, f.tx().rolledBack)
	}
	f.requireAbsent("INSERT INTO nonce_bindings")
	f.requireOrder("tx1:exec:INSERT INTO nonce_observations", "tx1:exec:INSERT INTO nonce_scope_holds")

	obsArgs := f.execArgsFor(f.tx(), "INSERT INTO nonce_observations")
	if obsArgs == nil || obsArgs[4] != ClassificationDivergence {
		t.Fatalf("observation args = %v, want divergence", obsArgs)
	}
	holdArgs := f.execArgsFor(f.tx(), "INSERT INTO nonce_scope_holds")
	if holdArgs == nil || holdArgs[3] != CauseChainViewDivergence || holdArgs[4] != obsArgs[0] {
		t.Fatalf("hold args = %v, want cause divergence + evidence %v", holdArgs, obsArgs[0])
	}
}

// TestAllocateUnavailableObservationPersisted pins the RPC-failure branch: a
// failed sample still reaches classification, persists unavailable + the
// error class, and refuses chain_view_unavailable with no hold and no binding.
func TestAllocateUnavailableObservationPersisted(t *testing.T) {
	f := newAllocFixture(t, &obsFakeRPC{latest: "0x1", pending: "0x1", block: obsTestHead(), failNext: 3})

	binding, outcome, err := f.allocator().Allocate(context.Background(), allocRequest())
	if !IsOutcome(err, OutcomeChainViewUnavailable) || outcome != OutcomeChainViewUnavailable {
		t.Fatalf("outage = (%v, %q), want chain_view_unavailable", err, outcome)
	}
	if binding != nil {
		t.Fatalf("outage returned binding %+v", binding)
	}
	if !f.tx().committed {
		t.Fatal("unavailable observation was not committed")
	}
	f.requireAbsent("INSERT INTO nonce_scope_holds")
	f.requireAbsent("INSERT INTO nonce_bindings")

	obsArgs := f.execArgsFor(f.tx(), "INSERT INTO nonce_observations")
	if obsArgs == nil || obsArgs[4] != ClassificationUnavailable || obsArgs[9] != "transport" {
		t.Fatalf("observation args = %v, want unavailable/transport", obsArgs)
	}
}

// TestAllocateGateHitRollsBackZeroWrites pins 006 precedence: a gate hit
// refuses recovery_active before any scope-row creation or evidence write.
func TestAllocateGateHitRollsBackZeroWrites(t *testing.T) {
	f := newAllocFixture(t, nil)
	f.gateHit = true

	_, outcome, err := f.allocator().Allocate(context.Background(), allocRequest())
	if !IsOutcome(err, OutcomeRecoveryActive) || outcome != OutcomeRecoveryActive {
		t.Fatalf("gate hit = (%v, %q), want recovery_active", err, outcome)
	}
	if !f.tx().rolledBack || f.tx().committed {
		t.Fatalf("gate refusal tx = (rolledBack=%v, committed=%v), want rollback", f.tx().rolledBack, f.tx().committed)
	}
	f.requireAbsent("INSERT INTO nonce_scope_state")
	f.requireAbsent("INSERT INTO nonce_observations")
	f.requireAbsent("INSERT INTO nonce_bindings")
	f.requireAbsent("withdrawal_authorizations")
}

// TestAllocateActiveHoldRefusal pins the active-hold recheck: a scope with an
// active hold refuses scope_held with the cause and writes nothing.
func TestAllocateActiveHoldRefusal(t *testing.T) {
	f := newAllocFixture(t, nil)
	f.holdRows = [][]any{allocHoldValues(CauseUnattributedConsumption)}

	_, outcome, err := f.allocator().Allocate(context.Background(), allocRequest())
	if !IsOutcome(err, OutcomeScopeHeld) || outcome != OutcomeScopeHeld {
		t.Fatalf("held scope = (%v, %q), want scope_held", err, outcome)
	}
	if !f.tx().rolledBack {
		t.Fatal("hold refusal did not roll back")
	}
	f.requireAbsent("INSERT INTO nonce_observations")
	f.requireAbsent("INSERT INTO nonce_bindings")
}

// TestAllocateConvergeIntentRaceReplays pins T-converge: the 23505 aborts the
// tx, ROLLBACK precedes the off-pool classify, and the fixed-order intent
// probe returns the winner as a replay.
func TestAllocateConvergeIntentRaceReplays(t *testing.T) {
	f := newAllocFixture(t, nil)
	f.insertErr = func(sql string) error {
		if strings.Contains(allocSQL(sql), "INSERT INTO nonce_bindings ") {
			return &pgconn.PgError{Code: "23505", ConstraintName: "nonce_bindings_intent_uniq"}
		}
		return nil
	}
	f.winner = allocBindingValues(allocExistingBinding(allocTestAuth))

	binding, outcome, err := f.allocator().Allocate(context.Background(), allocRequest())
	if err != nil {
		t.Fatalf("race replay returned error %v", err)
	}
	if outcome != OutcomeReplayed || binding == nil || binding.BindingID != "nb-orig" {
		t.Fatalf("race replay = (%+v, %q), want the winner binding", binding, outcome)
	}
	if len(f.txs) != 2 {
		t.Fatalf("transactions = %d, want aborted tx + off-pool classify tx", len(f.txs))
	}
	if !f.txs[0].rolledBack || f.txs[0].committed {
		t.Fatalf("raced tx = (rolledBack=%v, committed=%v), want rollback", f.txs[0].rolledBack, f.txs[0].committed)
	}
	f.requireOrder("tx1:exec:INSERT INTO nonce_bindings", "tx1:rollback")
	f.requireOrder("tx1:rollback", "tx2:query:SELECT binding_id, intent_id, chain_id, sender, nonce::text")
	if f.txs[1].committed {
		t.Fatal("off-pool classify committed; it must stay read-only")
	}
	if got := f.execCount(f.txs[0], "INSERT INTO nonce_bindings "); got != 1 {
		t.Fatalf("raced tx wrote %d binding rows, want exactly the racing insert", got)
	}
}

// TestAllocateConvergeScopeNonceHitRetryable pins T-converge step 2: when the
// intent probe misses and the scope+nonce carrier is taken by another intent,
// the outcome is retryable — the other intent's binding is never adopted.
func TestAllocateConvergeScopeNonceHitRetryable(t *testing.T) {
	f := newAllocFixture(t, nil)
	f.insertErr = func(sql string) error {
		if strings.Contains(allocSQL(sql), "INSERT INTO nonce_bindings ") {
			return &pgconn.PgError{Code: "23505", ConstraintName: "nonce_bindings_scope_nonce_uniq"}
		}
		return nil
	}
	other := allocExistingBinding(allocTestAuth)
	other.BindingID, other.IntentID = "nb-other", "intent-other"
	f.otherNonce = allocBindingValues(other)

	binding, outcome, err := f.allocator().Allocate(context.Background(), allocRequest())
	if !IsOutcome(err, OutcomeTemporarilyUnavailable) || outcome != OutcomeTemporarilyUnavailable {
		t.Fatalf("scope nonce race = (%v, %q), want temporarily_unavailable", err, outcome)
	}
	if binding != nil {
		t.Fatalf("race adopted another intent's binding %+v", binding)
	}
	f.requireOrder("tx1:rollback", "tx2:query:SELECT binding_id, intent_id, chain_id, sender, nonce::text")
	if f.indexOf("AND nonce = $3::numeric") < 0 {
		t.Fatalf("off-pool classify missed the scope+nonce probe:\n%s", f.logString())
	}
}

// TestAllocateConvergeUnknownConstraintRetryable pins "exact ConstraintName
// match only": a 23505 outside the named binding carriers never enters the
// replay/conflict classify and stays retryable with one aborted tx.
func TestAllocateConvergeUnknownConstraintRetryable(t *testing.T) {
	f := newAllocFixture(t, nil)
	f.insertErr = func(sql string) error {
		if strings.Contains(allocSQL(sql), "INSERT INTO nonce_bindings ") {
			return &pgconn.PgError{Code: "23505", ConstraintName: "unrelated_uniq"}
		}
		return nil
	}

	_, outcome, err := f.allocator().Allocate(context.Background(), allocRequest())
	if !IsOutcome(err, OutcomeTemporarilyUnavailable) || outcome != OutcomeTemporarilyUnavailable {
		t.Fatalf("unknown 23505 = (%v, %q), want temporarily_unavailable", err, outcome)
	}
	if len(f.txs) != 1 || !f.txs[0].rolledBack {
		t.Fatalf("unknown 23505 opened %d txs (rollback=%v), want one rolled-back tx", len(f.txs), f.txs[0].rolledBack)
	}
	f.requireAbsent("tx2:")
}

func (f *allocFixture) tx() *allocFakeTx {
	if len(f.txs) == 0 {
		f.t.Fatal("no transaction was opened")
	}
	return f.txs[len(f.txs)-1]
}
