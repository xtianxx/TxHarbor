//go:build linux && drill

// borrowed-replacement-owner-commit-ambiguity_linux_test.go is the bounded real
// owner COMMIT acknowledgement-loss lane. It composes the existing machinery
// without duplicating any transaction or observer path: the genuine autonomous
// baseline/capture and guard-row preamble (created through the UNARMED drill
// socket wrap so ordinary traffic is unchanged), the recycled actual-refusal
// helper (real unique refused attempt identity, no-Start/no-callback fact
// only), and the existing observed journal transaction with exact guard-row
// locking, same-transaction journal insertion and pending invisibility. The
// lane adds ONE file-local fault path: a one-shot wire gate wraps the REAL
// owner socket established by the drill-only connection-establishment seam,
// stays unarmed through the whole baseline, is armed ONLY in the owner-tail
// atArbitration hook after the insertion and the successful final facts
// verification, and then establishes that the ACTUAL COMMIT was sent and
// withholds its completion BEFORE pgx receives it. While withheld, an
// independent bounded control-store read must observe the exact committed
// refused journal row (and the released guard row); then the owner transport is
// broken, causing the ACTUAL Commit call to return an error - never an injected
// error after a received acknowledgement. The UNKNOWN COMMIT acknowledgement
// loss is never reported as success or rollback: the composite is refused, the
// durable refused non-instance-bound journal row remains, the provisional
// eligibility never authorizes retry/acceptance/recovery, the guard stays
// unresolved, and the fault worker/pump/Use are finished with bounded joins and
// independent cleanup without reacquiring or reviving the owner. There is no
// manifest, restore, admission, atomic acceptance, downstream or Gate1
// authority; no replacement owner, DSN/key rebinding, copied acquisition loop
// or arbitrary connector/SQL authority; authentication payloads, secrets,
// verifiers and DSNs are never logged.
package recovery_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xtianxx/txharbor/internal/recovery"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

// borrowedReplacementCommitAmbiguityWitness is the gate's established witness:
// NOTESTABLISHED (no COMMIT dispatch claimed), DISPATCHED (the real COMMIT was
// sent) or WITHHELD (the server completion was withheld before pgx saw it).
type borrowedReplacementCommitAmbiguityWitness int

const (
	borrowedReplacementCommitWitnessNotEstablished borrowedReplacementCommitAmbiguityWitness = iota
	borrowedReplacementCommitWitnessDispatched
	borrowedReplacementCommitWitnessWithheld
)

func (w borrowedReplacementCommitAmbiguityWitness) String() string {
	switch w {
	case borrowedReplacementCommitWitnessDispatched:
		return "dispatched"
	case borrowedReplacementCommitWitnessWithheld:
		return "withheld"
	default:
		return "not-established"
	}
}

// borrowedReplacementCommitAmbiguityGate wraps the REAL owner socket and
// passes bytes through until armed. Once armed it recognizes the actual COMMIT
// query frame, forwards it (so the server really commits), withholds the server
// completion before pgx reads it, and can then break the transport so the
// ACTUAL Commit call returns an error.
type borrowedReplacementCommitAmbiguityGate struct {
	real net.Conn

	mu           sync.Mutex
	armed        bool
	dispatched   bool
	withheld     bool
	broken       bool
	unrecognized int

	dispatchedCh chan struct{}
	withheldCh   chan struct{}
	brokenCh     chan struct{}
	armOnce      sync.Once
	withholdOnce sync.Once
	breakOnce    sync.Once
}

func newBorrowedReplacementCommitAmbiguityGate(real net.Conn) *borrowedReplacementCommitAmbiguityGate {
	return &borrowedReplacementCommitAmbiguityGate{
		real:         real,
		dispatchedCh: make(chan struct{}),
		withheldCh:   make(chan struct{}),
		brokenCh:     make(chan struct{}),
	}
}

func (g *borrowedReplacementCommitAmbiguityGate) arm() {
	g.mu.Lock()
	g.armed = true
	g.mu.Unlock()
}

func (g *borrowedReplacementCommitAmbiguityGate) witness() borrowedReplacementCommitAmbiguityWitness {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.withheld {
		return borrowedReplacementCommitWitnessWithheld
	}
	if g.dispatched {
		return borrowedReplacementCommitWitnessDispatched
	}
	return borrowedReplacementCommitWitnessNotEstablished
}

func (g *borrowedReplacementCommitAmbiguityGate) unrecognizedFramingNow() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.unrecognized
}

// cleanupUncertain reports a broken transport whose COMMIT completion was never
// withheld: the actual database outcome is UNKNOWN and must never be inferred
// as a rollback.
func (g *borrowedReplacementCommitAmbiguityGate) cleanupUncertain() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.broken && !g.withheld
}

func (g *borrowedReplacementCommitAmbiguityGate) awaitDispatched(ctx context.Context) bool {
	select {
	case <-g.dispatchedCh:
		return true
	case <-ctx.Done():
		return false
	}
}

func (g *borrowedReplacementCommitAmbiguityGate) awaitWithheld(ctx context.Context) bool {
	select {
	case <-g.withheldCh:
		return true
	case <-ctx.Done():
		return false
	}
}

func (g *borrowedReplacementCommitAmbiguityGate) Write(p []byte) (int, error) {
	n, err := g.real.Write(p)
	if err != nil {
		return n, err
	}
	g.maybeEstablishDispatch(p)
	return n, nil
}

func (g *borrowedReplacementCommitAmbiguityGate) maybeEstablishDispatch(p []byte) {
	g.mu.Lock()
	if !g.armed || g.dispatched {
		g.mu.Unlock()
		return
	}
	g.mu.Unlock()
	commit, recognized := classifyBorrowedReplacementCommitFrame(p)
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.armed || g.dispatched {
		return
	}
	if !recognized {
		g.unrecognized++
		return
	}
	if commit {
		g.dispatched = true
		close(g.dispatchedCh)
	}
}

func (g *borrowedReplacementCommitAmbiguityGate) Read(p []byte) (int, error) {
	g.mu.Lock()
	withhold := g.armed && g.dispatched && !g.withheld && !g.broken
	g.mu.Unlock()
	if withhold {
		g.withholdOnce.Do(func() {
			// Consume the server completion from the REAL socket and withhold
			// it from pgx; the server has already committed at this point.
			buf := make([]byte, 64*1024)
			_, _ = g.real.Read(buf)
			g.mu.Lock()
			g.withheld = true
			g.mu.Unlock()
			close(g.withheldCh)
		})
		<-g.brokenCh
		return 0, net.ErrClosed
	}
	return g.real.Read(p)
}

func (g *borrowedReplacementCommitAmbiguityGate) breakTransport() {
	g.breakOnce.Do(func() {
		g.mu.Lock()
		g.broken = true
		g.mu.Unlock()
		_ = g.real.Close()
		close(g.brokenCh)
	})
}

// Close satisfies net.Conn and breaks the transport: pgx may close the socket
// on its own error paths and that must never leave a withheld completion
// dangling.
func (g *borrowedReplacementCommitAmbiguityGate) Close() error {
	g.breakTransport()
	return nil
}

func (g *borrowedReplacementCommitAmbiguityGate) LocalAddr() net.Addr  { return g.real.LocalAddr() }
func (g *borrowedReplacementCommitAmbiguityGate) RemoteAddr() net.Addr { return g.real.RemoteAddr() }

func (g *borrowedReplacementCommitAmbiguityGate) SetDeadline(t time.Time) error {
	return g.real.SetDeadline(t)
}
func (g *borrowedReplacementCommitAmbiguityGate) SetReadDeadline(t time.Time) error {
	return g.real.SetReadDeadline(t)
}
func (g *borrowedReplacementCommitAmbiguityGate) SetWriteDeadline(t time.Time) error {
	return g.real.SetWriteDeadline(t)
}

// classifyBorrowedReplacementCommitFrame recognizes the actual simple-protocol
// COMMIT query frame pgx sends for tx.Commit and never claims an establishment
// for unrecognized, partial or non-commit framing.
func classifyBorrowedReplacementCommitFrame(p []byte) (commit, recognized bool) {
	for len(p) > 0 {
		if len(p) < 5 {
			return false, false
		}
		messageType := p[0]
		length := int(binary.BigEndian.Uint32(p[1:5]))
		if length < 4 || 1+length > len(p) {
			return false, false
		}
		body := p[5 : 1+length]
		if messageType == 'Q' {
			sql := strings.TrimSpace(string(bytes.TrimRight(body, "\x00")))
			if strings.EqualFold(strings.TrimSuffix(sql, ";"), "commit") {
				return true, true
			}
		}
		p = p[1+length:]
	}
	return false, true
}

// borrowedReplacementCommitAmbiguityCommitFrame builds the exact simple-protocol
// commit query frame for the deterministic gate controls.
func borrowedReplacementCommitAmbiguityCommitFrame() []byte {
	sql := "commit"
	frame := make([]byte, 5+len(sql)+1)
	frame[0] = 'Q'
	binary.BigEndian.PutUint32(frame[1:5], uint32(4+len(sql)+1))
	copy(frame[5:], sql)
	return frame
}

// borrowedReplacementCommitAmbiguityRegistry captures every owner session
// socket wrapped by the armed seam so the lane can require exactly one real
// owner socket.
type borrowedReplacementCommitAmbiguityRegistry struct {
	mu    sync.Mutex
	gates []*borrowedReplacementCommitAmbiguityGate
}

func (r *borrowedReplacementCommitAmbiguityRegistry) wrap(real net.Conn) net.Conn {
	gate := newBorrowedReplacementCommitAmbiguityGate(real)
	r.mu.Lock()
	r.gates = append(r.gates, gate)
	r.mu.Unlock()
	return gate
}

func (r *borrowedReplacementCommitAmbiguityRegistry) single(t *testing.T) *borrowedReplacementCommitAmbiguityGate {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.gates) != 1 {
		t.Fatalf("commit-ambiguity wire gate wrapped %d owner sockets, want exactly 1", len(r.gates))
	}
	return r.gates[0]
}

// borrowedReplacementCommitAmbiguityTuple is the immutable audit-ID/attempt
// tuple published to the fault worker at the arming boundary.
type borrowedReplacementCommitAmbiguityTuple struct {
	auditID   int64
	attemptID string
}

// borrowedReplacementCommitAmbiguityResult preserves the actually witnessed
// fault-worker facts.
type borrowedReplacementCommitAmbiguityResult struct {
	tuple            borrowedReplacementCommitAmbiguityTuple
	dispatch         bool
	withheld         bool
	broken           bool
	durableRows      int
	durableRow       borrowedReplacementOwnerJournalRow
	guardLocked      bool
	guardDisposition string
	guardOperation   string
	err              error
}

// borrowedReplacementCommitAmbiguityFaultWorker waits for the published tuple,
// witnesses the dispatch and the withholding, independently reads the exact
// committed refused row while the completion is withheld, and breaks the owner
// transport so the ACTUAL Commit call returns an error.
type borrowedReplacementCommitAmbiguityFaultWorker struct {
	ctx      context.Context
	b        *borrowedSuccessorBaseline
	gate     *borrowedReplacementCommitAmbiguityGate
	tupleCh  chan borrowedReplacementCommitAmbiguityTuple
	resultCh chan *borrowedReplacementCommitAmbiguityResult
}

func newBorrowedReplacementCommitAmbiguityFaultWorker(ctx context.Context, b *borrowedSuccessorBaseline, gate *borrowedReplacementCommitAmbiguityGate) *borrowedReplacementCommitAmbiguityFaultWorker {
	worker := &borrowedReplacementCommitAmbiguityFaultWorker{
		ctx: ctx, b: b, gate: gate,
		tupleCh:  make(chan borrowedReplacementCommitAmbiguityTuple, 1),
		resultCh: make(chan *borrowedReplacementCommitAmbiguityResult, 1),
	}
	go worker.run()
	return worker
}

// publish hands the immutable tuple to the worker through a synchronized
// buffered channel; no shared publication mutex is held and no I/O is
// performed while arming.
func (w *borrowedReplacementCommitAmbiguityFaultWorker) publish(tuple borrowedReplacementCommitAmbiguityTuple) {
	w.tupleCh <- tuple
}

func (w *borrowedReplacementCommitAmbiguityFaultWorker) join(t *testing.T) *borrowedReplacementCommitAmbiguityResult {
	t.Helper()
	select {
	case res := <-w.resultCh:
		return res
	case <-time.After(180 * time.Second):
		t.Fatal("commit-ambiguity fault worker did not finish within the bounded join")
		return nil
	}
}

func (w *borrowedReplacementCommitAmbiguityFaultWorker) run() {
	res := &borrowedReplacementCommitAmbiguityResult{}
	// ALWAYS break the owner transport: a fault-path refusal can never leave
	// the owner COMMIT withheld forever.
	defer func() {
		w.gate.breakTransport()
		res.broken = true
		w.resultCh <- res
	}()
	select {
	case tuple := <-w.tupleCh:
		res.tuple = tuple
	case <-time.After(90 * time.Second):
		res.err = errors.New("NOTESTABLISHED: the armed gate tuple was never published")
		return
	}
	dispatchCtx, cancelDispatch := context.WithTimeout(w.ctx, 90*time.Second)
	defer cancelDispatch()
	if !w.gate.awaitDispatched(dispatchCtx) {
		res.err = errors.New("NOTESTABLISHED: the real COMMIT was never observed on the wire")
		return
	}
	res.dispatch = true
	withheldCtx, cancelWithheld := context.WithTimeout(w.ctx, 90*time.Second)
	defer cancelWithheld()
	if !w.gate.awaitWithheld(withheldCtx) {
		res.err = errors.New("NOTESTABLISHED: the COMMIT completion was never withheld")
		return
	}
	res.withheld = true
	// While the completion is withheld (the server has committed), the exact
	// refused journal row MUST be independently visible.
	countCtx, cancelCount := context.WithTimeout(w.ctx, borrowedOwnerRotationQueryBudget)
	countErr := w.b.fixture.controlPool.QueryRow(countCtx,
		`SELECT count(*)::int FROM recovery_audit WHERE action=$1 AND operation_id=$2 AND result='refused'`,
		borrowedReplacementOwnerJournalAction, res.tuple.attemptID).Scan(&res.durableRows)
	cancelCount()
	if countErr != nil {
		res.err = fmt.Errorf("independent committed-journal read refused: %w", countErr)
		return
	}
	rowCtx, cancelRow := context.WithTimeout(w.ctx, borrowedOwnerRotationQueryBudget)
	row, rowErr := borrowedReplacementOwnerJournalRead(rowCtx, w.b.fixture.controlPool, res.tuple.auditID)
	cancelRow()
	if rowErr != nil {
		res.err = fmt.Errorf("independent committed-row read refused: %w", rowErr)
		return
	}
	res.durableRow = row
	// The server-side commit also released the owner guard row: a contender can
	// lock it while pgx still has not seen the completion.
	guardCtx, cancelGuard := context.WithTimeout(w.ctx, borrowedOwnerRotationQueryBudget)
	_, _, contenderErr := borrowedReplacementGuardRowContenderLock(guardCtx, w.b.fixture.controlPool, w.b.fixture.guardKey)
	cancelGuard()
	res.guardLocked = contenderErr == nil
	guardReadCtx, cancelGuardRead := context.WithTimeout(w.ctx, borrowedOwnerRotationQueryBudget)
	guardReadErr := w.b.fixture.controlPool.QueryRow(guardReadCtx,
		`SELECT disposition, operation_id FROM recovery_target_guard WHERE target_guard_key=$1`,
		w.b.fixture.guardKey).Scan(&res.guardDisposition, &res.guardOperation)
	cancelGuardRead()
	if guardReadErr != nil {
		res.err = fmt.Errorf("independent guard read refused: %w", guardReadErr)
		return
	}
}

// borrowedReplacementCommitAmbiguityFakeConn is the deterministic in-memory
// transport of the gate state-machine controls.
type borrowedReplacementCommitAmbiguityFakeConn struct {
	mu     sync.Mutex
	server bytes.Buffer
	closed bool
}

func (f *borrowedReplacementCommitAmbiguityFakeConn) pushServerResponse(data []byte) {
	f.mu.Lock()
	f.server.Write(data)
	f.mu.Unlock()
}

func (f *borrowedReplacementCommitAmbiguityFakeConn) Read(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, net.ErrClosed
	}
	if f.server.Len() == 0 {
		return 0, errors.New("fake transport has no server bytes")
	}
	return f.server.Read(p)
}

func (f *borrowedReplacementCommitAmbiguityFakeConn) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, net.ErrClosed
	}
	return len(p), nil
}

func (f *borrowedReplacementCommitAmbiguityFakeConn) Close() error {
	f.mu.Lock()
	f.closed = true
	f.mu.Unlock()
	return nil
}

type borrowedReplacementCommitAmbiguityFakeAddr string

func (a borrowedReplacementCommitAmbiguityFakeAddr) Network() string { return "fake" }
func (a borrowedReplacementCommitAmbiguityFakeAddr) String() string  { return string(a) }

func (f *borrowedReplacementCommitAmbiguityFakeConn) LocalAddr() net.Addr {
	return borrowedReplacementCommitAmbiguityFakeAddr("fake-local")
}
func (f *borrowedReplacementCommitAmbiguityFakeConn) RemoteAddr() net.Addr {
	return borrowedReplacementCommitAmbiguityFakeAddr("fake-remote")
}
func (f *borrowedReplacementCommitAmbiguityFakeConn) SetDeadline(time.Time) error      { return nil }
func (f *borrowedReplacementCommitAmbiguityFakeConn) SetReadDeadline(time.Time) error  { return nil }
func (f *borrowedReplacementCommitAmbiguityFakeConn) SetWriteDeadline(time.Time) error { return nil }

// TestBorrowedReplacementOwnerCommitAmbiguity is the bounded real owner COMMIT
// acknowledgement-loss lane described in the file header.
func TestBorrowedReplacementOwnerCommitAmbiguity(t *testing.T) {
	ctx := t.Context()
	var probeCalls, acceptanceCalls int32

	// P: the ACTUAL COMMIT is sent, its completion is withheld before pgx sees
	// it, the committed refused journal row is independently observed, the
	// transport is then broken so the ACTUAL Commit call returns an error, and
	// the UNKNOWN loss is never a success or a rollback.
	func() {
		registry := &borrowedReplacementCommitAmbiguityRegistry{}
		reset := recovery.ArmTargetLockSocketWrap(registry.wrap)
		defer reset()
		b, fresh := borrowedReplacementGuardRowFixtures(t, ctx)
		gate := registry.single(t)
		beforeDisposition, beforeOperation := borrowedReplacementGuardRowRead(t, ctx, b.fixture.controlPool, b.fixture.guardKey)
		attemptID := borrowedReplacementOwnerJournalActualRefusal(t, ctx, b, fresh, "commit-ambiguity", &probeCalls, &acceptanceCalls)
		facts := &borrowedReplacementOwnerJournalFacts{}
		worker := newBorrowedReplacementCommitAmbiguityFaultWorker(ctx, b, gate)
		outcome := borrowedReplacementOwnerJournalRun(ctx, t, b, fresh, &borrowedReplacementOwnerJournalStage{
			key: b.fixture.guardKey, expectedOperation: b.fixture.operation,
			insert: true, attemptID: attemptID, contenderProof: true,
		}, facts, &borrowedReplacementOwnerTailHooks{
			atArbitration: func(_ context.Context, _ *borrowedReplacementAutonomousPump, _ func(), _ pgx.Tx) error {
				if !facts.inserted || facts.auditID <= 0 {
					return errors.New("commit-ambiguity arm refused: the insertion was not confirmed")
				}
				gate.arm()
				worker.publish(borrowedReplacementCommitAmbiguityTuple{auditID: facts.auditID, attemptID: attemptID})
				return nil
			},
		})
		res := worker.join(t)
		if res.err != nil {
			t.Fatalf("commit-ambiguity fault worker refused: %v", res.err)
		}
		if !res.dispatch || !res.withheld || !res.broken {
			t.Fatalf("commit-ambiguity witnesses: dispatch=%t withheld=%t broken=%t", res.dispatch, res.withheld, res.broken)
		}
		if res.durableRows != 1 {
			t.Fatalf("commit-ambiguity committed journal rows while withheld=%d, want exactly 1", res.durableRows)
		}
		if res.durableRow.auditID != facts.auditID || res.durableRow.result != controlstore.AuditRefused || res.durableRow.instanceID != "" || res.durableRow.operationID != attemptID {
			t.Fatalf("commit-ambiguity committed row mismatch: id=%d result=%q instance=%q operation=%q", res.durableRow.auditID, res.durableRow.result, res.durableRow.instanceID, res.durableRow.operationID)
		}
		if !res.guardLocked {
			t.Fatal("commit-ambiguity committed transaction did not release the owner guard row")
		}
		if res.guardDisposition != beforeDisposition || res.guardOperation != beforeOperation {
			t.Fatal("commit-ambiguity committed transaction changed the guard contents")
		}
		if !facts.fenced || !facts.inserted || facts.auditID <= 0 || !facts.pendingViaTx || facts.pendingViaPool != 0 || facts.contenderCode != "55P03" {
			t.Fatalf("commit-ambiguity stage: fenced=%t inserted=%t auditID=%d pendingTx=%t pendingPool=%d contender=%q",
				facts.fenced, facts.inserted, facts.auditID, facts.pendingViaTx, facts.pendingViaPool, facts.contenderCode)
		}
		if got := outcome.state.tailInvocationsNow(); got != 1 {
			t.Fatalf("commit-ambiguity tail invocations=%d, want exactly 1", got)
		}
		if outcome.runErr != nil {
			t.Fatalf("commit-ambiguity flow returned an unexpected join error: %v", outcome.runErr)
		}
		if outcome.flow.ownerErr == nil {
			t.Fatal("commit-ambiguity broken owner transport was accepted as a success")
		}
		if !strings.Contains(outcome.flow.ownerErr.Error(), "commit target-lock acceptance transaction") {
			t.Fatalf("commit-ambiguity owner error was not the ambiguous COMMIT path: %v", outcome.flow.ownerErr)
		}
		if strings.Contains(outcome.flow.ownerErr.Error(), "rollback target-lock acceptance transaction") {
			t.Fatalf("commit-ambiguity owner error must never be reported as a rollback: %v", outcome.flow.ownerErr)
		}
		if outcome.flow.state.compositePublished() || outcome.flow.decisionErr == nil {
			t.Fatal("commit-ambiguity UNKNOWN COMMIT still produced a terminal verdict")
		}
		if count := borrowedReplacementOwnerJournalCount(t, ctx, b.fixture.controlPool, attemptID); count != 1 {
			t.Fatalf("commit-ambiguity durable journal rows=%d, want exactly 1", count)
		}
		// The provisional eligibility never authorizes retry, acceptance or
		// recovery after the UNKNOWN COMMIT.
		if !outcome.state.consumeEligibility() {
			t.Fatal("commit-ambiguity provisional eligibility was not consumable once")
		}
		if outcome.state.consumeEligibility() {
			t.Fatal("commit-ambiguity provisional eligibility was reusable")
		}
		reuseCtx, cancelReuse := context.WithTimeout(ctx, 30*time.Second)
		reuseErr := outcome.flow.reg.Use(reuseCtx)
		copied := *outcome.flow.reg
		copyErr := copied.Use(reuseCtx)
		cancelReuse()
		if reuseErr == nil || copyErr == nil {
			t.Fatal("commit-ambiguity retry/reuse was accepted after the UNKNOWN COMMIT")
		}
		borrowedOwnerDDLGuard(t, ctx, b)
		borrowedSuccessorCloseConn(t, outcome.flow.conn)
		t.Logf("commit-ambiguity positive: the drill socket wrap established that the ACTUAL COMMIT was sent and withheld its completion before pgx saw it (audit_id=%d), the independent bounded read observed exactly ONE committed refused non-instance-bound row and the released guard row while withheld, the transport break made the ACTUAL Commit call return an error (never an injected acknowledgement error), the composite refused, the durable row remained, the provisional eligibility was one-shot and non-reusable, and the guard stayed unresolved with acceptance zero", facts.auditID)
	}()

	// N1: UNARMED control: ordinary traffic is unchanged, the COMMIT is
	// acknowledged, exactly one genuine refused row is durable, the guard is
	// unchanged, guarded retirement runs and replay/copy plus the ordinary
	// dirty-guard refusal are preserved.
	func() {
		registry := &borrowedReplacementCommitAmbiguityRegistry{}
		reset := recovery.ArmTargetLockSocketWrap(registry.wrap)
		defer reset()
		b, fresh := borrowedReplacementGuardRowFixtures(t, ctx)
		gate := registry.single(t)
		beforeDisposition, beforeOperation := borrowedReplacementGuardRowRead(t, ctx, b.fixture.controlPool, b.fixture.guardKey)
		attemptID := borrowedReplacementOwnerJournalActualRefusal(t, ctx, b, fresh, "unarmed", &probeCalls, &acceptanceCalls)
		facts := &borrowedReplacementOwnerJournalFacts{}
		outcome := borrowedReplacementOwnerJournalRun(ctx, t, b, fresh, &borrowedReplacementOwnerJournalStage{
			key: b.fixture.guardKey, expectedOperation: b.fixture.operation,
			insert: true, attemptID: attemptID, contenderProof: true,
		}, facts, &borrowedReplacementOwnerTailHooks{})
		if outcome.runErr != nil || outcome.flow.ownerErr != nil {
			t.Fatalf("commit-ambiguity unarmed control refused: runErr=%v ownerErr=%v", outcome.runErr, outcome.flow.ownerErr)
		}
		if !outcome.flow.state.compositePublished() || outcome.flow.state.lostNow() {
			t.Fatalf("commit-ambiguity unarmed terminal state: composite=%t lost=%t", outcome.flow.state.compositePublished(), outcome.flow.state.lostNow())
		}
		if !facts.inserted || facts.auditID <= 0 {
			t.Fatal("commit-ambiguity unarmed control did not insert the refused row")
		}
		if count := borrowedReplacementOwnerJournalCount(t, ctx, b.fixture.controlPool, attemptID); count != 1 {
			t.Fatalf("commit-ambiguity unarmed durable rows=%d, want exactly 1", count)
		}
		if gate.witness() != borrowedReplacementCommitWitnessNotEstablished {
			t.Fatalf("commit-ambiguity UNARMED gate claimed witness %s", gate.witness())
		}
		witnessCtx, cancelWitness := context.WithTimeout(ctx, 2*time.Second)
		unarmedDispatch := gate.awaitDispatched(witnessCtx)
		cancelWitness()
		if unarmedDispatch {
			t.Fatal("commit-ambiguity UNARMED gate claimed a COMMIT dispatch")
		}
		postDisposition, postOperation := borrowedReplacementGuardRowRead(t, ctx, b.fixture.controlPool, b.fixture.guardKey)
		if postDisposition != beforeDisposition || postOperation != beforeOperation {
			t.Fatal("commit-ambiguity unarmed control changed the guard contents")
		}
		if err := borrowedReplacementOwnerTailRetireGuarded(t, ctx, b, outcome); err != nil {
			t.Fatalf("commit-ambiguity unarmed guarded retirement refused: %v", err)
		}
		borrowedOwnerDDLGuard(t, ctx, b)
		reuseCtx, cancelReuse := context.WithTimeout(ctx, 30*time.Second)
		reuseErr := outcome.flow.reg.Use(reuseCtx)
		copied := *outcome.flow.reg
		copyErr := copied.Use(reuseCtx)
		cancelReuse()
		if reuseErr == nil || copyErr == nil {
			t.Fatal("commit-ambiguity unarmed replay/copy use was accepted")
		}
		ordinaryID := borrowedReplacementOwnerJournalActualRefusal(t, ctx, b, fresh, "ordinary", &probeCalls, &acceptanceCalls)
		t.Logf("commit-ambiguity unarmed control: the UNARMED gate passed all ordinary traffic (no witness claimed), the COMMIT was acknowledged with exactly ONE genuine refused row (audit_id=%d, attempt %s), the guard stayed unchanged, guarded retirement ran, replay/copy refused and the ordinary native-ready attempt was still refused by the dirty guard", facts.auditID, ordinaryID)
	}()

	// N2: pre-COMMIT cancellation with the gate ARMED: the REAL arbitration
	// refuses precisely, no COMMIT is dispatched, the rollback is confirmed
	// successful, zero journal rows are durable and the witness-timeout stays
	// NOTESTABLISHED (never inferred as a rollback).
	func() {
		registry := &borrowedReplacementCommitAmbiguityRegistry{}
		reset := recovery.ArmTargetLockSocketWrap(registry.wrap)
		defer reset()
		b, fresh := borrowedReplacementGuardRowFixtures(t, ctx)
		gate := registry.single(t)
		attemptID := borrowedReplacementOwnerJournalActualRefusal(t, ctx, b, fresh, "precommit-cancel", &probeCalls, &acceptanceCalls)
		facts := &borrowedReplacementOwnerJournalFacts{}
		var canceled atomic.Bool
		outcome := borrowedReplacementOwnerJournalRun(ctx, t, b, fresh, &borrowedReplacementOwnerJournalStage{
			key: b.fixture.guardKey, expectedOperation: b.fixture.operation,
			insert: true, attemptID: attemptID, contenderProof: true,
		}, facts, &borrowedReplacementOwnerTailHooks{
			atArbitration: func(_ context.Context, _ *borrowedReplacementAutonomousPump, cancelLane func(), _ pgx.Tx) error {
				if cancelLane == nil {
					t.Fatal("commit-ambiguity pre-COMMIT cancel control has no lane cancel")
				}
				gate.arm()
				canceled.Store(true)
				cancelLane()
				return nil
			},
		})
		if !facts.inserted || !canceled.Load() {
			t.Fatalf("commit-ambiguity pre-COMMIT control inserted=%t canceled=%t", facts.inserted, canceled.Load())
		}
		if !errors.Is(outcome.flow.ownerErr, errOwnerTailArbitration) {
			t.Fatalf("commit-ambiguity pre-COMMIT refusal was not the ACTUAL arbitration stage: %v", outcome.flow.ownerErr)
		}
		if outcome.flow.ownerErr == nil || strings.Contains(outcome.flow.ownerErr.Error(), "commit target-lock acceptance transaction") {
			t.Fatalf("commit-ambiguity pre-COMMIT control attempted a COMMIT: %v", outcome.flow.ownerErr)
		}
		if strings.Contains(outcome.flow.ownerErr.Error(), "rollback target-lock acceptance transaction") {
			t.Fatalf("commit-ambiguity pre-COMMIT rollback was not confirmed clean: %v", outcome.flow.ownerErr)
		}
		if outcome.state.eligibleNow() || outcome.flow.state.compositePublished() {
			t.Fatal("commit-ambiguity pre-COMMIT control still produced eligibility/composite")
		}
		if count := borrowedReplacementOwnerJournalCount(t, ctx, b.fixture.controlPool, attemptID); count != 0 {
			t.Fatalf("commit-ambiguity pre-COMMIT refusal left %d durable journal rows", count)
		}
		witnessCtx, cancelWitness := context.WithTimeout(ctx, 2*time.Second)
		dispatched := gate.awaitDispatched(witnessCtx)
		cancelWitness()
		if dispatched {
			t.Fatal("commit-ambiguity pre-COMMIT refusal still dispatched a COMMIT")
		}
		if gate.witness() != borrowedReplacementCommitWitnessNotEstablished {
			t.Fatalf("commit-ambiguity pre-COMMIT witness=%s, want not-established", gate.witness())
		}
		if gate.cleanupUncertain() {
			t.Fatal("commit-ambiguity pre-COMMIT control left an uncertain cleanup")
		}
		healthCtx, cancelHealth := context.WithTimeout(ctx, borrowedOwnerRotationHealthBudget)
		healthErr := b.fixture.lock.Health(healthCtx)
		cancelHealth()
		if healthErr != nil {
			t.Fatalf("commit-ambiguity pre-COMMIT rollback did not preserve the owner session: %v", healthErr)
		}
		t.Logf("commit-ambiguity pre-COMMIT cancellation: the committed rollback was confirmed clean, NO COMMIT was dispatched (witness-timeout stayed not-established, never inferred as a rollback), zero journal rows were durable and the owner session survived")
	}()

	// N3: deterministic gate state-machine negatives with the in-memory
	// transport: unrecognized framing, missing barriers, witness timeout and
	// uncertain cleanup all stay NOTESTABLISHED/UNCERTAIN and are never inferred
	// as a rollback or a success.
	func() {
		// Unrecognized framing.
		unrecognized := newBorrowedReplacementCommitAmbiguityGate(&borrowedReplacementCommitAmbiguityFakeConn{})
		unrecognized.arm()
		if _, err := unrecognized.Write([]byte("garbage-frame")); err != nil {
			t.Fatalf("commit-ambiguity unrecognized framing write refused: %v", err)
		}
		if unrecognized.witness() != borrowedReplacementCommitWitnessNotEstablished || unrecognized.unrecognizedFramingNow() == 0 {
			t.Fatal("commit-ambiguity unrecognized framing claimed an establishment")
		}
		unrecognized.breakTransport()
		if !unrecognized.cleanupUncertain() {
			t.Fatal("commit-ambiguity unrecognized framing cleanup was not uncertain")
		}
		// Missing barriers: a break before any dispatch establishment.
		missingBarriers := newBorrowedReplacementCommitAmbiguityGate(&borrowedReplacementCommitAmbiguityFakeConn{})
		missingBarriers.arm()
		missingBarriers.breakTransport()
		if missingBarriers.witness() != borrowedReplacementCommitWitnessNotEstablished || !missingBarriers.cleanupUncertain() {
			t.Fatal("commit-ambiguity missing-barrier break claimed an establishment")
		}
		// Witness timeout: no COMMIT within the bound.
		timeoutGate := newBorrowedReplacementCommitAmbiguityGate(&borrowedReplacementCommitAmbiguityFakeConn{})
		timeoutGate.arm()
		timeoutCtx, cancelTimeout := context.WithTimeout(ctx, 100*time.Millisecond)
		timedOut := timeoutGate.awaitDispatched(timeoutCtx)
		cancelTimeout()
		if timedOut || timeoutGate.witness() != borrowedReplacementCommitWitnessNotEstablished {
			t.Fatal("commit-ambiguity witness timeout claimed an establishment")
		}
		timeoutGate.breakTransport()
		// Uncertain cleanup: dispatch established, completion never withheld,
		// transport broken -> UNKNOWN, never withheld/rollback.
		uncertain := newBorrowedReplacementCommitAmbiguityGate(&borrowedReplacementCommitAmbiguityFakeConn{})
		uncertain.arm()
		if _, err := uncertain.Write(borrowedReplacementCommitAmbiguityCommitFrame()); err != nil {
			t.Fatalf("commit-ambiguity uncertain-cleanup commit write refused: %v", err)
		}
		if uncertain.witness() != borrowedReplacementCommitWitnessDispatched {
			t.Fatalf("commit-ambiguity uncertain-cleanup dispatch witness=%s", uncertain.witness())
		}
		uncertain.breakTransport()
		if uncertain.witness() == borrowedReplacementCommitWitnessWithheld || !uncertain.cleanupUncertain() {
			t.Fatal("commit-ambiguity uncertain cleanup claimed the withheld completion")
		}
		// Recognized framing sanity: the exact commit frame dispatches and its
		// server response is withheld, never delivered.
		fake := &borrowedReplacementCommitAmbiguityFakeConn{}
		fake.pushServerResponse([]byte("COMMIT\x00"))
		recognized := newBorrowedReplacementCommitAmbiguityGate(fake)
		recognized.arm()
		if _, err := recognized.Write(borrowedReplacementCommitAmbiguityCommitFrame()); err != nil {
			t.Fatalf("commit-ambiguity recognized commit write refused: %v", err)
		}
		readDone := make(chan struct{})
		go func() {
			buf := make([]byte, 64)
			_, _ = recognized.Read(buf)
			close(readDone)
		}()
		withheldCtx, cancelWithheld := context.WithTimeout(ctx, 5*time.Second)
		withheld := recognized.awaitWithheld(withheldCtx)
		cancelWithheld()
		if !withheld || recognized.witness() != borrowedReplacementCommitWitnessWithheld {
			t.Fatal("commit-ambiguity recognized commit frame was not withheld")
		}
		if recognized.cleanupUncertain() {
			t.Fatal("commit-ambiguity withheld completion was reported uncertain")
		}
		recognized.breakTransport()
		select {
		case <-readDone:
		case <-time.After(5 * time.Second):
			t.Fatal("commit-ambiguity withheld read did not finish after the break")
		}
		t.Logf("commit-ambiguity gate negatives: unrecognized framing, missing barriers, witness timeout and uncertain cleanup all stayed NOTESTABLISHED/UNCERTAIN and were never inferred as a rollback; the exact commit frame was recognized, dispatched and withheld until the transport break")
	}()

	if got := atomic.LoadInt32(&probeCalls); got != 0 {
		t.Fatalf("commit-ambiguity lane ran the ordinary probe %d times, want 0", got)
	}
	if got := atomic.LoadInt32(&acceptanceCalls); got != 0 {
		t.Fatalf("commit-ambiguity lane ran ordinary acceptance %d times, want 0", got)
	}
}
