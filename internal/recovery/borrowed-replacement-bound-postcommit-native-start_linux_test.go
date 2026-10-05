//go:build linux && drill

// borrowed-replacement-bound-postcommit-native-start_linux_test.go is the
// bounded bound postcommit-native-start lane (NON-AUTHORIZING). It composes
// ONLY existing, unchanged primitives: the authentic bound capture with the
// single-consumed receipt/entry, the exact bound dirty-guard refusal whose
// coordinator preparation-transaction PID is captured through the existing
// marker seam, the readiness machinery consumed ONCE as NON-AUTHORIZING lineage
// facts, the strict registered-P1 retirement, the unchanged writer-isolation
// flow and its live synchronous duringProof interval, the unchanged
// baseline-commit stage, and the unchanged bound native coordinator dispatch
// through the common attempt constructor (factory endpoint + sealed tools +
// counted genuine transactional marker wrapper).
//
// The lane proves ONE bounded event: a genuine supervised native child is
// actually STARTED by the coordinator after the acknowledged baseline commit,
// the child's frontend connection to the armed origin endpoint is accepted by
// a lane-local READER-ONLY stalling peer which reads the initial client bytes
// and sends nothing (so no authentication happens and no executable frame is
// ever released), the accepted peer is attributed to the authentic observed
// child with the UNCHANGED origin-attribution checks, and then ONLY the child
// run context is canceled (the proof context stays live): the real
// cancellation/drain path produces Started + PGCommandCanceled +
// ProcessGroupDrained, the authentic sole-Wait terminal observation and the
// single-consumed process receipt bound to the exact child PID/start, target,
// role and operation; the durable marker/generation and the guard record the
// launch, retain the intent, mark the writer drained and the normal
// rebuild_required failure disposition while the historical preparation audit
// stays byte-identical. The fence is rechecked before the hook returns, every
// lane goroutine is joined, and the unchanged disposal restores the HBA with
// the original owner healthy and no continuing isolation.
//
// Positive outcome wording: "Bounded native child start witnessed;
// authentication withheld; owner cancellation and drain witnessed; restoration
// unaccepted."
//
// The lane NEVER weakens the live fence, never relays upstream (the peer opens
// no upstream connection, never calls AdmitBorrowed, never starts a gate
// session, never authenticates and never releases an executable frame), never
// fabricates identities/tokens, never runs direct guard SQL and never repairs
// committed history. Native backend admission, a successful restore probe,
// atomic acceptance, manifests, downstream effects, T057 and Gate1 remain
// untouched and OPEN.
package recovery_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xtianxx/txharbor/internal/recovery"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

var (
	errBoundPostcommitNativeStart = errors.New("bound postcommit native start: the held child did not reach the expected canceled/drained witness")
)

// borrowedNativeStartOutcome is the joined result of the one real coordinator
// dispatch performed by a lane goroutine.
type borrowedNativeStartOutcome struct {
	result  recovery.TargetWriterResult
	receipt recovery.DrillTargetProcessReceipt
	err     error
}

// borrowedNativeStartLineage is the unchanged writer-isolation lineage input of
// this lane's authentic bound fixture.
func borrowedNativeStartLineage(f *borrowedAuthHandoffFixture, fresh *borrowedReplacementBinding, b *borrowedSuccessorBaseline) *borrowedBoundWriterIsolationLineage {
	return &borrowedBoundWriterIsolationLineage{
		consumed: true, readinessBacked: false,
		instanceID: f.instanceID, targetKey: f.guardKey,
		fingerprint:    fresh.binding.OriginalRoleFingerprint(),
		replacementOID: fresh.replacementOID, ownerPID: b.owner.BackendPID,
		issuance: &borrowedBoundWriterIsolationIssuance{},
	}
}

// borrowedNativeStartFixedSetup is the common bounded prelude of every control:
// the authentic bound fixture, the strict registered-P1 retirement and the ONE
// fresh bound native attempt prepared through the closed bound attempt
// constructor. The caller decides whether the optional endpoint-closing
// callback stays set (endpoint-after-marker refusal) or is UNSET (the live
// child start).
func borrowedNativeStartFixedSetup(t *testing.T, ctx context.Context, markerAttach string) (*borrowedSuccessorBaseline, *borrowedReplacementBinding, *borrowedReplacementPrelaunchAttempt, *borrowedBoundPostcommitAdmissionMarker, *int32, *int32) {
	t.Helper()
	b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
	f := b.fixture
	_ = borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
	var probeCalls, acceptanceCalls int32
	attemptID := fmt.Sprintf("bound-postcommit-native-start-%s-%d", markerAttach, time.Now().UnixNano())
	marker := newBorrowedBoundPostcommitAdmissionMarker(attemptID, f.adminRole, b.owner.BackendPID)
	attempt := newBorrowedBoundPostcommitAdmissionAttempt(t, ctx, b, fresh, attemptID, marker, &probeCalls, &acceptanceCalls)
	// Return the ORIGINAL counter pointers the attempt callbacks retain, never a
	// copy: every control must inspect the exact counters the coordinator sees.
	return b, fresh, attempt, marker, &probeCalls, &acceptanceCalls
}

// borrowedNativeStartPeer is the lane-local bounded READER-ONLY stalling peer.
// It accepts on the actual factory endpoint listener, retains the accepted
// frontend connection and its four-tuple, and reads the initial client bytes.
// It NEVER writes a byte, never dials an upstream connection, never calls
// AdmitBorrowed, never starts a gate session, never authenticates and never
// releases an executable frame.
type borrowedNativeStartPeer struct {
	listener        net.Listener
	mu              sync.Mutex
	accepted        int
	read            int
	remote          *net.TCPAddr
	conn            net.Conn
	conns           map[net.Conn]bool
	first           *borrowedNativeStartTrackedConn
	firstReaderDone chan struct{}
	readerDoneOnce  sync.Once
	shutdown        bool
	closed          sync.Once
	wg              sync.WaitGroup
}

// borrowedNativeStartTrackedConn records whether Close was called, so the lane
// can assert that a connection which hit EOF before any shutdown was still
// closed by its reader instead of being silently untracked.
type borrowedNativeStartTrackedConn struct {
	net.Conn
	closed atomic.Bool
}

func (c *borrowedNativeStartTrackedConn) Close() error {
	c.closed.Store(true)
	return c.Conn.Close()
}

func newBorrowedNativeStartPeer(listener net.Listener) *borrowedNativeStartPeer {
	peer := &borrowedNativeStartPeer{listener: listener, conns: map[net.Conn]bool{}, firstReaderDone: make(chan struct{})}
	peer.wg.Add(1)
	go peer.acceptLoop()
	return peer
}

func (p *borrowedNativeStartPeer) acceptLoop() {
	defer p.wg.Done()
	for {
		conn, err := p.listener.Accept()
		if err != nil {
			return
		}
		addr, _ := conn.RemoteAddr().(*net.TCPAddr)
		tracked := &borrowedNativeStartTrackedConn{Conn: conn}
		p.mu.Lock()
		if p.shutdown {
			p.mu.Unlock()
			_ = tracked.Close()
			return
		}
		// EVERY accepted connection is tracked and closed on shutdown, and each
		// reader closes its own connection before untracking it; the first one
		// also supplies the observed four-tuple.
		p.conns[tracked] = true
		isFirst := false
		if p.first == nil && addr != nil {
			p.first = tracked
			p.conn = tracked
			p.remote = addr
			isFirst = true
		}
		p.accepted++
		p.mu.Unlock()
		p.wg.Add(1)
		go p.readLoop(tracked, isFirst)
	}
}

func (p *borrowedNativeStartPeer) readLoop(conn *borrowedNativeStartTrackedConn, first bool) {
	defer p.wg.Done()
	defer func() {
		// Close BEFORE untracking: an EOF/error exit must never leave the
		// accepted socket open after it is removed from the tracked set.
		_ = conn.Close()
		p.mu.Lock()
		delete(p.conns, conn)
		p.mu.Unlock()
		if first {
			p.readerDoneOnce.Do(func() { close(p.firstReaderDone) })
		}
	}()
	buf := make([]byte, 4096)
	for {
		n, err := conn.Read(buf)
		if n > 0 {
			p.mu.Lock()
			p.read += n
			p.mu.Unlock()
		}
		if err != nil {
			return
		}
	}
}

// awaitFirstReaderExit waits for the first accepted connection's reader to exit
// on its own (EOF/error) within the bound and reports whether that reader exit
// closed the connection before any peer shutdown; the assertion distinguishes
// an EOF-before-shutdown closure from a shutdown-driven one.
func (p *borrowedNativeStartPeer) awaitFirstReaderExit(bound time.Duration) (bool, error) {
	select {
	case <-p.firstReaderDone:
	case <-time.After(bound):
		return false, errors.New("the first accepted connection's reader did not exit within the bound")
	}
	p.mu.Lock()
	first := p.first
	p.mu.Unlock()
	if first == nil {
		return false, errors.New("no first accepted connection was recorded")
	}
	return first.closed.Load(), nil
}

func (p *borrowedNativeStartPeer) acceptedNow() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.accepted
}

func (p *borrowedNativeStartPeer) readNow() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.read
}

func (p *borrowedNativeStartPeer) remoteAddr() *net.TCPAddr {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.remote
}

func (p *borrowedNativeStartPeer) waitAccepted(t *testing.T, ctx context.Context) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if p.acceptedNow() > 0 {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("accepted frontend peer was not observed before the proof context ended: %v", ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	t.Fatal("the supervised native child never connected to the armed origin endpoint")
}

func (p *borrowedNativeStartPeer) waitRead(t *testing.T, ctx context.Context) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if p.readNow() > 0 {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("initial client bytes were not read before the proof context ended: %v", ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	t.Fatal("the accepted frontend peer read no initial client bytes")
}

func (p *borrowedNativeStartPeer) close() {
	p.closed.Do(func() {
		p.mu.Lock()
		p.shutdown = true
		for conn := range p.conns {
			_ = conn.Close()
		}
		p.mu.Unlock()
		_ = p.listener.Close()
	})
}

// closeAndJoin closes the listener and EVERY accepted connection and joins all
// peer goroutines (the accept loop and every read loop) within the bound. It is
// idempotent: a repeated call after completion returns immediately.
func (p *borrowedNativeStartPeer) closeAndJoin(bound time.Duration) error {
	p.close()
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-time.After(bound):
		return errors.New("the reader-only peer goroutines did not join within the bound")
	}
}

// borrowedNativeStartJoinState is the lane-local observable of the live hook's
// failure cleanup: attempts counts cleanup invocations, dispatchDrained and
// peerJoined record the individual completions, complete is set only when the
// WHOLE cleanup (dispatch join, peer shutdown/join and winner classification)
// finished without error, and errs retains every cleanup error so an attempted
// cleanup can be distinguished from a successful one.
type borrowedNativeStartJoinState struct {
	mu              sync.Mutex
	attempts        int
	dispatchDrained bool
	eofClosed       bool
	peerJoined      bool
	complete        bool
	errs            []string
}

func (s *borrowedNativeStartJoinState) noteAttempt() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.attempts++
	s.mu.Unlock()
}

func (s *borrowedNativeStartJoinState) noteDispatchDrained() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.dispatchDrained = true
	s.mu.Unlock()
}

func (s *borrowedNativeStartJoinState) notePeerJoined() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.peerJoined = true
	s.mu.Unlock()
}

func (s *borrowedNativeStartJoinState) noteEOFClosure() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.eofClosed = true
	s.mu.Unlock()
}

func (s *borrowedNativeStartJoinState) noteComplete() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.complete = true
	s.mu.Unlock()
}

func (s *borrowedNativeStartJoinState) noteErr(err error) {
	if s == nil || err == nil {
		return
	}
	s.mu.Lock()
	s.errs = append(s.errs, err.Error())
	s.mu.Unlock()
}

func (s *borrowedNativeStartJoinState) attemptedNow() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.attempts > 0
}

func (s *borrowedNativeStartJoinState) drainedNow() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dispatchDrained
}

func (s *borrowedNativeStartJoinState) peerJoinedNow() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.peerJoined
}

func (s *borrowedNativeStartJoinState) eofClosedNow() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.eofClosed
}

func (s *borrowedNativeStartJoinState) completeNow() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.complete
}

func (s *borrowedNativeStartJoinState) errsNow() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.errs...)
}

// borrowedNativeStartCountProcesses counts live processes whose resolved
// executable is the exact sealed native client file, so "exactly one
// supervised child while held" is an independent census, not a scalar.
func borrowedNativeStartCountProcesses(t *testing.T, executable string) int {
	t.Helper()
	sealed, err := os.Stat(executable)
	if err != nil {
		t.Fatalf("stat sealed native executable: %v", err)
	}
	entries, err := os.ReadDir("/proc")
	if err != nil {
		t.Fatalf("read proc: %v", err)
	}
	count := 0
	for _, entry := range entries {
		if _, err := strconv.Atoi(entry.Name()); err != nil {
			continue
		}
		info, err := os.Stat(filepath.Join("/proc", entry.Name(), "exe"))
		if err != nil {
			continue
		}
		if os.SameFile(info, sealed) {
			count++
		}
	}
	return count
}

// borrowedNativeStartAwaitGone requires the supervised child's process group to
// disappear after the sole-Wait terminal fact, bounded.
func borrowedNativeStartAwaitGone(t *testing.T, executable string, bound time.Duration) {
	t.Helper()
	deadline := time.Now().Add(bound)
	for borrowedNativeStartCountProcesses(t, executable) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("native child did not disappear after the pinned owner's sole Wait")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// borrowedNativeStartAssertChild is the unchanged /proc observation of the
// started child: the exact closed three-argument invocation on the
// factory-sealed restore ELF routed to the armed endpoint, with no password in
// argv and the private descriptor-backed passfile in the environment. It
// returns the sealed executable path for the census.
func borrowedNativeStartAssertChild(t *testing.T, identity recovery.DrillStartedIdentity, endpointAddr, expectedDB, sealedDir string) string {
	t.Helper()
	exeInfo, err := os.Stat(fmt.Sprintf("/proc/%d/exe", identity.PID))
	if err != nil {
		t.Fatalf("stat native child executable: %v", err)
	}
	rawCmdline, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", identity.PID))
	if err != nil {
		t.Fatalf("read native child argv: %v", err)
	}
	argv := strings.Split(strings.TrimRight(string(rawCmdline), "\x00"), "\x00")
	if len(argv) != 6 || argv[1] != "--clean" || argv[2] != "--if-exists" || argv[3] != "--no-owner" || argv[4] != "--no-privileges" || !strings.HasPrefix(argv[5], "--dbname=") {
		t.Fatalf("native child argv is not the exact closed transport invocation on the sealed executable: %q", argv)
	}
	if !filepath.IsAbs(argv[0]) || filepath.Dir(argv[0]) != sealedDir || filepath.Base(argv[0]) != "pg_restore" {
		t.Fatalf("native child executable %q is not the factory-sealed restore client under %q", argv[0], sealedDir)
	}
	linkInfo, err := os.Stat(argv[0])
	if err != nil || !os.SameFile(exeInfo, linkInfo) {
		t.Fatalf("running child is not the sealed private pg_restore ELF (argv0=%q err=%v)", argv[0], err)
	}
	transportDSN, err := url.Parse(strings.TrimPrefix(argv[5], "--dbname="))
	if err != nil || transportDSN.Host != endpointAddr || transportDSN.Path != "/"+expectedDB {
		t.Fatalf("native child was not routed to the armed endpoint with the original database: %v", err)
	}
	if _, hasPassword := transportDSN.User.Password(); hasPassword {
		t.Fatal("native child argv carries a password")
	}
	rawEnviron, err := os.ReadFile(fmt.Sprintf("/proc/%d/environ", identity.PID))
	if err != nil {
		t.Fatalf("read native child environment: %v", err)
	}
	if !strings.Contains(string(rawEnviron), "PGPASSFILE=/proc/self/fd/") {
		t.Fatal("native child did not receive the private descriptor-backed passfile")
	}
	return argv[0]
}

// borrowedNativeStartMarkerAuditCount independently counts the committed
// transactional restore_started marker audits of the exact attempt/generation.
func borrowedNativeStartMarkerAuditCount(ctx context.Context, t *testing.T, b *borrowedSuccessorBaseline, operationID string, generation int64) int {
	t.Helper()
	readCtx, cancelRead := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	defer cancelRead()
	var count int
	if err := b.fixture.controlPool.QueryRow(readCtx, `
SELECT count(*)::int FROM recovery_audit
WHERE instance_id=$1 AND action=$2 AND operation_id=$3 AND result='ok'
  AND target->>'kind'=$4 AND target->>'accepted_generation'=$5`,
		b.fixture.instanceID, recovery.ActionEvidenceWrite, operationID,
		string(recovery.MutationRestoreStarted), strconv.FormatInt(generation, 10)).Scan(&count); err != nil {
		t.Fatalf("bound native start committed marker audit read refused: %v", err)
	}
	return count
}

// borrowedNativeStartLiveHook is the ONE live synchronous duringProof interval
// of the positive and of the copy/concurrent-replay control: acknowledged
// baseline commit, fresh revalidation, the real coordinator dispatch with the
// child held before authentication by the reader-only peer, cancellation of
// ONLY the child run context, the authentic cancellation/drain/sole-Wait
// witness, the durable marker/generation/guard classification, the preserved
// historical preparation audit, and the fence recheck AFTER every lane
// goroutine (each dispatch, the accept loop and every read loop) was joined.
// The named error result lets a failure exit still join everything inside the
// hook before the flow disposes the fence.
func borrowedNativeStartLiveHook(hookCtx context.Context, t *testing.T, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding,
	attempt *borrowedReplacementPrelaunchAttempt, marker *borrowedBoundPostcommitAdmissionMarker, admission *borrowedBoundPostcommitAdmission,
	plan *borrowedBoundBaselineCommitPlan, preparation *borrowedBoundBaselineCommitPreparation, prepPID int,
	probeCalls, acceptanceCalls *int32, obs *borrowedBoundPostcommitAdmissionObservation,
	flow *borrowedBoundWriterIsolationFlowState, peer *borrowedNativeStartPeer, contend bool,
	joinState *borrowedNativeStartJoinState, injectFailure error) (retErr error) {
	f := b.fixture
	// Cleanup ownership is established BEFORE any fallible work involving the
	// already-running peer: every exit path, including early prerequisite
	// failures before dispatch and post-dispatch failures, cancels/drains a
	// started dispatch and closes/joins EVERY peer goroutine. A failed cleanup
	// attempt is retained and joined into the returned error.
	runCtx, cancelRun := context.WithCancel(hookCtx)
	dispatchStarted := false
	outstanding := 0
	var finished borrowedNativeStartOutcome
	winnerCount, loserCount := 0, 0
	singleOutcome := make(chan borrowedNativeStartOutcome, 1)
	contenderOutcomes := make(chan borrowedNativeStartOutcome, 2)
	joinAll := func() error {
		joinState.noteAttempt()
		cancelRun()
		var errs []error
		if dispatchStarted && outstanding > 0 {
			deadline := time.After(120 * time.Second)
		drain:
			for outstanding > 0 {
				var got borrowedNativeStartOutcome
				select {
				case got = <-contenderOutcomes:
					if !contend {
						errs = append(errs, errors.New("the single dispatch surfaced on the contender channel"))
						break drain
					}
				case got = <-singleOutcome:
					if contend {
						errs = append(errs, errors.New("the single dispatch channel received a contender outcome"))
						break drain
					}
				case <-deadline:
					errs = append(errs, fmt.Errorf("a native dispatch did not return within the bounded join window (outstanding=%d)", outstanding))
					break drain
				}
				outstanding--
				if errors.Is(got.err, errBoundPostcommitAdmissionReplay) {
					loserCount++
					continue
				}
				winnerCount++
				finished = got
			}
		}
		if winnerCount > 1 {
			errs = append(errs, errors.New("more than one native dispatch passed the shared one-use reservation"))
		}
		if !dispatchStarted || outstanding == 0 {
			joinState.noteDispatchDrained()
		}
		if dispatchStarted {
			// EOF-before-shutdown closure assertion: the canceled child closes
			// its socket, so the first accepted connection's reader must exit on
			// its own AND close that connection before any peer shutdown.
			eofClosed, eofErr := peer.awaitFirstReaderExit(30 * time.Second)
			if eofErr != nil {
				errs = append(errs, eofErr)
			} else if !eofClosed {
				errs = append(errs, errors.New("the first accepted connection hit EOF before shutdown but was not closed by its reader"))
			} else {
				joinState.noteEOFClosure()
			}
		}
		if err := peer.closeAndJoin(30 * time.Second); err != nil {
			errs = append(errs, err)
		} else {
			joinState.notePeerJoined()
		}
		err := errors.Join(errs...)
		if err != nil {
			joinState.noteErr(err)
			return err
		}
		joinState.noteComplete()
		return nil
	}
	defer func() {
		if err := joinAll(); err != nil {
			if retErr == nil {
				retErr = err
			} else {
				retErr = errors.Join(retErr, err)
			}
		}
	}()
	sealedDir := ""
	if dumpPath, ok := f.tools.DumpPath(); ok {
		sealedDir = filepath.Dir(dumpPath)
	}
	if sealedDir == "" {
		return errors.New("bound postcommit native start: the factory sealed tools directory is unknown")
	}
	if err := borrowedBoundBaselineCommitStageRun(hookCtx, t, b, fresh, prepPID, plan, preparation); err != nil {
		return err
	}
	if !plan.commitAcknowledged || plan.commitAckLost || plan.rollbackAcknowledged || plan.successInferred {
		return fmt.Errorf("%w: acknowledged=%t ackLost=%t rollback=%t inferred=%t",
			errBoundPostcommitAdmissionBaseline, plan.commitAcknowledged, plan.commitAckLost, plan.rollbackAcknowledged, plan.successInferred)
	}
	obs.baselineOperation = plan.rehearsalOperationID
	obs.attemptID = attempt.run.Binding().OriginalOperationID()
	obs.preDispatchGuardState, obs.preDispatchGuardOperation = borrowedReplacementGuardRowRead(t, hookCtx, f.controlPool, f.guardKey)
	obs.baselineAuditCount, obs.baselineAuditDigest = borrowedBoundPostcommitAdmissionAuditDigest(hookCtx, t, b, controlstore.ActionTargetGuardRebuild, plan.rehearsalOperationID)
	if obs.preDispatchGuardState != "clean" || obs.preDispatchGuardOperation != plan.rehearsalOperationID || obs.baselineAuditCount != 1 {
		return fmt.Errorf("%w: committed preparation state=%q op=%q audit=%d", errBoundPostcommitAdmissionRevalidate, obs.preDispatchGuardState, obs.preDispatchGuardOperation, obs.baselineAuditCount)
	}
	if err := admission.armAfterAcknowledgedBaseline(plan, flow); err != nil {
		return err
	}
	if err := borrowedBoundPostcommitAdmissionRevalidate(hookCtx, t, b, fresh, plan, flow); err != nil {
		return err
	}
	obs.revalidated = true

	endpoint := attempt.gate.endpoint
	handle := attempt.run.Observation()
	dispatch := func(gate *borrowedBoundPostcommitAdmission, channel chan borrowedNativeStartOutcome) {
		result, receipt, err := gate.dispatchOnce(runCtx, attempt)
		channel <- borrowedNativeStartOutcome{result: result, receipt: receipt, err: err}
	}
	if contend {
		copied := *admission
		outstanding = 2
		go dispatch(admission, contenderOutcomes)
		go dispatch(&copied, contenderOutcomes)
	} else {
		outstanding = 1
		go dispatch(admission, singleOutcome)
	}
	dispatchStarted = true
	startCtx, cancelStart := context.WithTimeout(hookCtx, 60*time.Second)
	defer cancelStart()
	if err := handle.AwaitStarted(startCtx); err != nil {
		return fmt.Errorf("the supervised native child did not start: %w", err)
	}
	identity := handle.StartedIdentity()
	if !identity.Started || identity.StartErr != nil || identity.PID <= 0 || identity.StartID == 0 {
		return fmt.Errorf("the supervised native child start identity is not usable: %+v", identity)
	}
	currentStartID, err := hostProcessStartID(identity.PID)
	if err != nil || currentStartID != identity.StartID {
		return fmt.Errorf("the supervised native child start identity changed: observed=%d current=%d err=%v", identity.StartID, currentStartID, err)
	}
	peer.waitAccepted(t, startCtx)
	peer.waitRead(t, startCtx)
	peerAddr := peer.remoteAddr()
	if peerAddr == nil || peerAddr.IP == nil || !peerAddr.IP.IsLoopback() || peerAddr.Port <= 0 {
		return fmt.Errorf("the accepted frontend peer has no loopback four-tuple: %+v", peerAddr)
	}
	gateHost, gatePortRaw, err := net.SplitHostPort(endpoint.Addr())
	if err != nil {
		return fmt.Errorf("the armed endpoint address is invalid: %w", err)
	}
	gatePort, err := strconv.Atoi(gatePortRaw)
	if err != nil || gatePort <= 0 {
		return fmt.Errorf("the armed endpoint port is invalid: %v", err)
	}
	if _, err := hostOwnedSocketInode(identity.PID, peerAddr.IP, peerAddr.Port, net.ParseIP(gateHost), gatePort); err != nil {
		return fmt.Errorf("the accepted endpoint peer is not owned by the observed child: %w", err)
	}
	claimed, err := handle.ClaimForEndpoint(endpoint)
	if err != nil {
		return fmt.Errorf("the started child could not be claimed with the armed endpoint capability: %w", err)
	}
	if !claimed.MatchesEndpoint(endpoint) {
		return errors.New("the claimed origin does not match the armed endpoint capability")
	}
	if claimedIdentity := claimed.StartedIdentity(); claimedIdentity.PID != identity.PID || claimedIdentity.StartID != identity.StartID {
		return errors.New("the claimed origin identity does not match the started child")
	}
	bound, err := claimed.BoundOperation()
	if err != nil {
		return fmt.Errorf("the claimed origin operation identity is absent: %w", err)
	}
	if bound.TargetKey != fresh.binding.OriginalTargetKey() || bound.RoleFingerprint != fresh.binding.OriginalRoleFingerprint() ||
		bound.OperationID != obs.attemptID || bound.Executable != "pg_restore" ||
		bound.Database != f.targetDB || bound.Role != f.writerRole {
		return fmt.Errorf("the claimed origin is not the factory-bound operation: %+v", bound)
	}
	sealedPath := borrowedNativeStartAssertChild(t, identity, endpoint.Addr(), f.targetDB, sealedDir)
	if count := borrowedNativeStartCountProcesses(t, sealedPath); count != 1 {
		return fmt.Errorf("expected exactly one supervised native child while held, found %d", count)
	}
	if terminal := handle.WaitTerminal(); terminal.Terminal || terminal.ChildWaitCompleted {
		return fmt.Errorf("terminal wait facts were fabricated while the child was still held: %+v", terminal)
	}
	if disposition := handle.RunnerDisposition(); disposition.Returned {
		return errors.New("the runner disposition was returned while the child was still held")
	}
	if atomic.LoadInt32(probeCalls) != 0 || atomic.LoadInt32(acceptanceCalls) != 0 {
		return errors.New("probe/acceptance callbacks ran before any accepted outcome")
	}
	if injectFailure != nil {
		// Deterministic post-dispatch failure exercise: the live hook returns
		// through its deferred cleanup with the child held and the peer in
		// flight, proving the failure path cancels/drains and joins every
		// goroutine before the flow disposes the fence.
		return injectFailure
	}
	if contend {
		// TWO competing contenders (the original gate and an actual copy) race
		// for the same attempt while the child is held: exactly one wins the
		// shared one-use reservation and runs the real coordinator, while the
		// loser must resolve to the gate replay refusal with zero output.
		var loser borrowedNativeStartOutcome
		select {
		case loser = <-contenderOutcomes:
		case <-time.After(30 * time.Second):
			return errors.New("the competing reservation did not resolve while the winner was held")
		}
		outstanding--
		if !errors.Is(loser.err, errBoundPostcommitAdmissionReplay) {
			return fmt.Errorf("the competing reservation was not refused by the one-use gate: %v", loser.err)
		}
		loserCount++
		borrowedBoundPostcommitAdmissionAssertNoAcceptedOutput(t, "competing reservation loser", loser.result, loser.receipt)
		// Every further rejected invocation (a copied run, the original run and
		// a further gate replay) must also publish zero result and no receipt.
		runCopy := *attempt.run
		runCopyResult, runCopyReceipt, runCopyErr := runCopy.Run(hookCtx)
		if runCopyErr == nil || !strings.Contains(runCopyErr.Error(), "already reserved") {
			return fmt.Errorf("a copied run was not refused as already reserved: %v", runCopyErr)
		}
		borrowedBoundPostcommitAdmissionAssertNoAcceptedOutput(t, "rejected copied run", runCopyResult, runCopyReceipt)
		origResult, origReceipt, origErr := attempt.run.Run(hookCtx)
		if origErr == nil || !strings.Contains(origErr.Error(), "already reserved") {
			return fmt.Errorf("the original run was not refused as already reserved: %v", origErr)
		}
		borrowedBoundPostcommitAdmissionAssertNoAcceptedOutput(t, "rejected original run", origResult, origReceipt)
		gateReplayResult, gateReplayReceipt, gateReplayErr := admission.dispatchOnce(hookCtx, attempt)
		if !errors.Is(gateReplayErr, errBoundPostcommitAdmissionReplay) {
			return fmt.Errorf("the consumed gate handed off again: %v", gateReplayErr)
		}
		borrowedBoundPostcommitAdmissionAssertNoAcceptedOutput(t, "rejected gate replay", gateReplayResult, gateReplayReceipt)
		if marker.callsNow() != 1 {
			return fmt.Errorf("replay attempts changed the marker callback count: %d", marker.callsNow())
		}
		if again := handle.StartedIdentity(); again.PID != identity.PID || again.StartID != identity.StartID {
			return errors.New("replay attempts overwrote the real start facts")
		}
		if terminal := handle.WaitTerminal(); terminal.Terminal {
			return errors.New("replay attempts fabricated terminal wait facts")
		}
	}
	// The child is held before authentication; joinAll cancels ONLY the child
	// run context, awaits the coordinator/drain completion and closes and joins
	// every peer goroutine before the fence is rechecked.
	if err := joinAll(); err != nil {
		return err
	}
	if hookCtx.Err() != nil {
		return errors.New("the proof context ended during the child cancellation")
	}
	if !contend && (winnerCount != 1 || loserCount != 0) {
		return fmt.Errorf("the single native dispatch did not resolve as the genuine winner: winners=%d losers=%d", winnerCount, loserCount)
	}
	if contend && (winnerCount != 1 || loserCount != 1) {
		return fmt.Errorf("expected exactly one winning dispatch and one competing gate replay refusal, got winners=%d losers=%d", winnerCount, loserCount)
	}
	if finished.err == nil {
		return errors.New("the canceled native run was reported as a clean success")
	}
	if !finished.result.Command.Started || finished.result.Command.Outcome != recovery.PGCommandCanceled || !finished.result.Command.ProcessGroupDrained {
		return fmt.Errorf("%w: command=%+v", errBoundPostcommitNativeStart, finished.result.Command)
	}
	if finished.result.Application == "" || recovery.ValidateAttemptApplicationName(finished.result.Application) != nil {
		return fmt.Errorf("%w: application identity %q", errBoundPostcommitNativeStart, finished.result.Application)
	}
	if finished.result.TargetKey != fresh.binding.OriginalTargetKey() {
		return fmt.Errorf("%w: result target key changed", errBoundPostcommitNativeStart)
	}
	if finished.result.Probe.Outcome == recovery.TargetWriterProbePassed || finished.result.Probe.ApplicationName != "" || len(finished.result.Probe.Evidence) != 0 {
		return fmt.Errorf("%w: a canceled attempt published probe output: %+v", errBoundPostcommitNativeStart, finished.result.Probe)
	}
	// BOTH original counters must still be zero after the coordinator completed
	// (cancel + drain + sole Wait) and before the hook returns.
	if atomic.LoadInt32(probeCalls) != 0 || atomic.LoadInt32(acceptanceCalls) != 0 {
		return fmt.Errorf("%w: probe/acceptance callbacks ran on the canceled native attempt (probe=%d acceptance=%d)",
			errBoundPostcommitNativeStart, atomic.LoadInt32(probeCalls), atomic.LoadInt32(acceptanceCalls))
	}
	facts, err := finished.receipt.ConsumeFacts()
	if err != nil {
		return fmt.Errorf("the single process receipt refused: %w", err)
	}
	if !facts.Started || !facts.Terminal || facts.PID != identity.PID || facts.StartIdentity != identity.StartID {
		return fmt.Errorf("%w: receipt facts do not retain the exact terminal child: %+v", errBoundPostcommitNativeStart, facts)
	}
	if facts.TargetKey != fresh.binding.OriginalTargetKey() || facts.RoleFingerprint != fresh.binding.OriginalRoleFingerprint() || facts.OperationID != obs.attemptID {
		return fmt.Errorf("%w: receipt lost the original target/role/operation binding: %+v", errBoundPostcommitNativeStart, facts)
	}
	if terminal := handle.WaitTerminal(); !terminal.ChildWaitCompleted || !terminal.Terminal {
		return fmt.Errorf("%w: the pinned owner did not record the authentic sole-Wait terminal fact: %+v", errBoundPostcommitNativeStart, terminal)
	}
	borrowedNativeStartAwaitGone(t, sealedPath, 15*time.Second)
	if contend {
		if _, err := finished.receipt.ConsumeFacts(); err == nil {
			return errors.New("the process receipt was consumable twice")
		}
	}
	token := marker.token()
	if token == (recovery.EvidenceToken{}) || token.InstanceID != f.instanceID || token.State != "open" || token.Generation <= 0 {
		return fmt.Errorf("%w: the genuine committed marker token is absent/invalid: %+v", errBoundPostcommitNativeStart, token)
	}
	state, key, fingerprint, generation := borrowedBoundPostcommitAdmissionInstanceFacts(hookCtx, t, b)
	if state != "open" || key != f.guardKey || fingerprint != fresh.binding.OriginalRoleFingerprint() || generation != token.Generation {
		return fmt.Errorf("%w: committed instance provenance state=%q key=%q fingerprint=%q generation=%d token=%d",
			errBoundPostcommitNativeStart, state, key, fingerprint, generation, token.Generation)
	}
	if audits := borrowedNativeStartMarkerAuditCount(hookCtx, t, b, obs.attemptID, token.Generation); audits != 1 {
		return fmt.Errorf("%w: committed marker audits=%d, want exactly 1", errBoundPostcommitNativeStart, audits)
	}
	guard, found, guardErr := controlstore.ReadTargetGuard(hookCtx, f.controlPool, f.guardKey)
	if guardErr != nil || !found {
		return fmt.Errorf("%w: guard read err=%v found=%t", errBoundPostcommitNativeStart, guardErr, found)
	}
	if guard.State != controlstore.TargetGuardRebuildRequired || guard.ActiveWriter || !guard.LaunchIntent ||
		guard.AttemptAppName != finished.result.Application || guard.OperationID != obs.attemptID ||
		guard.LaunchIntentAt == nil || guard.LaunchedAt == nil || guard.RebuildRequiredAt == nil ||
		guard.CleanAt != nil || len(guard.RebuildEvidence) != 0 {
		return fmt.Errorf("%w: guard is not the recorded launch / retained intent / drained writer / rebuild_required disposition: %+v", errBoundPostcommitNativeStart, guard)
	}
	count, digest := borrowedBoundPostcommitAdmissionAuditDigest(hookCtx, t, b, controlstore.ActionTargetGuardRebuild, plan.rehearsalOperationID)
	if count != obs.baselineAuditCount || digest != obs.baselineAuditDigest {
		return fmt.Errorf("%w: the historical preparation audit changed: count=%d->%d digest=%q->%q", errBoundPostcommitNativeStart, obs.baselineAuditCount, count, obs.baselineAuditDigest, digest)
	}
	if err := borrowedBoundPostcommitAdmissionFenceRecheck(hookCtx, t, b, fresh); err != nil {
		return err
	}
	obs.fenceRechecked = true
	obs.nativeOperation = obs.attemptID
	obs.nativeResult, obs.nativeReceipt, obs.nativeErr = finished.result, finished.receipt, finished.err
	obs.nativeChildPID, obs.nativeChildStartID = identity.PID, identity.StartID
	return nil
}

// borrowedNativeStartAssertPositiveDisposal is the shared post-flow positive
// / replay-control witness: fence disposal, original-owner health and the
// absence of any continuing isolation.
func borrowedNativeStartAssertPositiveDisposal(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding, flowState *borrowedBoundWriterIsolationFlowState, pristineBefore string) {
	t.Helper()
	f := b.fixture
	if !flowState.cleanupRestored || flowState.cleanupErr != nil {
		t.Fatalf("the writer-isolation disposal was not proven clean: restored=%t err=%v", flowState.cleanupRestored, flowState.cleanupErr)
	}
	serverIP, ipErr := f.fx.container.ContainerIP(ctx)
	if ipErr != nil {
		t.Fatalf("bound postcommit native start endpoint refused: %v", ipErr)
	}
	if err := borrowedBoundWriterIsolationAwaitState(t, ctx, f, serverIP, f.writerRole, b.passwordP1, "AUTH_OK", "", 10*time.Second); err != nil {
		t.Fatalf("the fence disposal did not restore the HBA: %v", err)
	}
	restoredP0 := runHelperAuthCheck(t, ctx, f.fx.containerID, "serverip", serverIP, f.writerRole, f.writerPassword)
	if !borrowedBoundWriterIsolationContains(restoredP0, "REJECT", "28P01") {
		t.Fatalf("the fence disposal rehabilitated P0: %s", restoredP0)
	}
	restoredRules, rulesErr := borrowedBoundWriterIsolationReadRules(ctx, f.fx.admin)
	if rulesErr != nil {
		t.Fatalf("restored rules read refused: %v", rulesErr)
	}
	if checkErr := borrowedBoundWriterIsolationFenceStateCheck(restoredRules, borrowedBoundWriterIsolationScopes(restoredRules), f.writerRole); checkErr == nil {
		t.Fatal("a continuing writer-reject isolation was left behind")
	}
	healthCtx, cancelHealth := context.WithTimeout(ctx, borrowedOwnerRotationHealthBudget)
	healthErr := f.lock.Health(healthCtx)
	cancelHealth()
	anchorCtx, cancelAnchor := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	anchorErr := b.anchor.Recheck(anchorCtx)
	cancelAnchor()
	if healthErr != nil || anchorErr != nil {
		t.Fatalf("owner health=%v anchor=%v after disposal", healthErr, anchorErr)
	}
	pristineAfter := borrowedBoundPostcommitAdmissionPristine(t, ctx, b)
	if pristineAfter != pristineBefore {
		t.Fatalf("target catalog/data changed: pristine %q -> %q", pristineBefore, pristineAfter)
	}
}

// TestBorrowedReplacementBoundPostcommitNativeStart is the bounded lane
// described in the file header.
func TestBorrowedReplacementBoundPostcommitNativeStart(t *testing.T) {
	ctx := t.Context()

	// P: the single chain over ONE authentic bound fixture: a genuine
	// supervised native child starts, is held before authentication by the
	// reader-only peer, and is canceled and drained by the owner context.
	t.Run("P", func(t *testing.T) {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		f := b.fixture
		borrowedBoundSessionAssertProvenance(t, ctx, b, fresh, "bound postcommit native start")
		_ = borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)

		// Exact dirty-guard refusal with the genuine preparation transaction
		// PID captured through the existing marker seam.
		var prepProbe, prepAcceptance int32
		prepAttemptID := fmt.Sprintf("bound-postcommit-native-start-refusal-%d", time.Now().UnixNano())
		refusalMarker := newBorrowedBoundNativeReadyMarker(prepAttemptID, f.adminRole)
		var prepTxPID int
		inner := refusalMarker.inner
		refusalMarker.inner = func(hookCtx context.Context, tx pgx.Tx, locked controlstore.InstanceToken) (recovery.EvidenceToken, error) {
			var pid int
			if err := tx.QueryRow(hookCtx, `SELECT pg_backend_pid()`).Scan(&pid); err == nil && pid > 0 {
				prepTxPID = pid
			}
			return inner(hookCtx, tx, locked)
		}
		refusalAttempt := prepareBorrowedBoundNativeReadyAttempt(t, ctx, b, fresh, prepAttemptID, refusalMarker, &prepProbe, &prepAcceptance)
		if err := refusalAttempt.registerOrigin(ctx); err != nil {
			t.Fatalf("bound postcommit native start refusal origin registration refused: %v", err)
		}
		refusalResult, refusalReceipt, refusalErr := dispatchBorrowedBoundNativeReady(t, ctx, refusalAttempt)
		if refusalErr == nil || refusalErr.Error() != "bound target guard is not clean" {
			t.Fatalf("the baseline prerequisite is not the exact dirty-guard refusal: %v", refusalErr)
		}
		if got := refusalMarker.callsNow(); got != 1 {
			t.Fatalf("refusal marker calls=%d, want 1", got)
		}
		if refusalToken := refusalMarker.token(); refusalResult.MarkerToken != refusalToken || refusalToken.InstanceID != f.instanceID || refusalToken.State != "open" {
			t.Fatalf("refusal marker token is not the genuine transaction-local token: %+v", refusalToken)
		}
		borrowedBoundReentryAssertOutcomeBoundaries(t, "bound postcommit native start refusal", refusalAttempt, refusalResult, refusalReceipt, &prepProbe, &prepAcceptance)
		if prepTxPID <= 0 || prepTxPID == b.owner.BackendPID {
			t.Fatalf("coordinator prep tx pid=%d owner=%d", prepTxPID, b.owner.BackendPID)
		}

		// Readiness consumed once as NON-AUTHORIZING lineage, strict P1 retirement.
		stage := &borrowedBoundReentryReadinessStage{
			key: f.guardKey, expectedOperation: f.operation,
			journalAttemptID: prepAttemptID, journalFacts: &borrowedReplacementOwnerJournalFacts{},
		}
		outcome := borrowedBoundReentryReadinessRun(ctx, t, b, fresh, stage, nil)
		readiness := newBorrowedBoundReentryReadiness()
		if err := borrowedBoundReentryReadinessTryPublish(t, "bound postcommit native start readiness", b, fresh, stage, outcome, readiness); err != nil {
			t.Fatalf("readiness refused: %v", err)
		}
		readinessFacts, ok := readiness.consume()
		if !ok {
			t.Fatal("readiness witness was not consumable once")
		}
		if readinessFacts.InstanceID != f.instanceID || readinessFacts.TargetKey != f.guardKey ||
			readinessFacts.RoleFingerprint != fresh.binding.OriginalRoleFingerprint() ||
			readinessFacts.ReplacementOID != fresh.replacementOID || readinessFacts.OwnerPID != b.owner.BackendPID {
			t.Fatalf("readiness facts are not the observed provenance: %+v", readinessFacts)
		}
		readinessCopy := *readiness
		if _, copyOK := readinessCopy.consume(); copyOK {
			t.Fatal("a shared-state readiness copy was consumable")
		}
		if err := borrowedReplacementOwnerTailRetireGuarded(t, ctx, b, outcome); err != nil {
			t.Fatalf("registered-P1 retirement refused: %v", err)
		}
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)

		// ONE fresh bound native attempt BEFORE the fence; the optional
		// endpoint-closing callback is UNSET so the child actually starts.
		var nativeProbe, nativeAcceptance int32
		nativeAttemptID := fmt.Sprintf("bound-postcommit-native-start-live-%d", time.Now().UnixNano())
		nativeMarker := newBorrowedBoundPostcommitAdmissionMarker(nativeAttemptID, f.adminRole, b.owner.BackendPID)
		nativeAttempt := newBorrowedBoundPostcommitAdmissionAttempt(t, ctx, b, fresh, nativeAttemptID, nativeMarker, &nativeProbe, &nativeAcceptance)
		nativeMarker.closeEndpoint = nil // the positive difference: the endpoint stays open so the child actually starts.
		pristineBefore := borrowedBoundPostcommitAdmissionPristine(t, ctx, b)

		plan := newBorrowedBoundBaselineCommitPlan()
		preparation := newBorrowedBoundBaselineCommitPreparation()
		admission := newBorrowedBoundPostcommitAdmission()
		obs := &borrowedBoundPostcommitAdmissionObservation{}
		joinState := &borrowedNativeStartJoinState{}
		peer := newBorrowedNativeStartPeer(nativeAttempt.gate.endpoint.Listener())
		defer func() {
			if err := peer.closeAndJoin(30 * time.Second); err != nil {
				t.Errorf("positive peer shutdown/join refused: %v", err)
			}
		}()

		flowState, flowErr := borrowedBoundWriterIsolationRun(ctx, t, b, fresh, borrowedNativeStartLineage(f, fresh, b), &borrowedBoundWriterIsolationHooks{
			duringProof: func(hookCtx context.Context, flow *borrowedBoundWriterIsolationFlowState) error {
				return borrowedNativeStartLiveHook(hookCtx, t, b, fresh, nativeAttempt, nativeMarker, admission, plan, preparation, prepTxPID, &nativeProbe, &nativeAcceptance, obs, flow, peer, false, joinState, nil)
			},
		})
		if flowErr != nil {
			t.Fatalf("bound postcommit native start positive refused: %v", flowErr)
		}
		if !joinState.completeNow() || !joinState.drainedNow() || !joinState.eofClosedNow() || !joinState.peerJoinedNow() {
			t.Fatalf("positive live-hook cleanup did not complete inside the hook: complete=%t drained=%t eofClosed=%t peerJoined=%t errs=%v",
				joinState.completeNow(), joinState.drainedNow(), joinState.eofClosedNow(), joinState.peerJoinedNow(), joinState.errsNow())
		}
		if !plan.commitAcknowledged || plan.commitAckLost || plan.rollbackAcknowledged {
			t.Fatalf("baseline classification: acknowledged=%t ackLost=%t rollback=%t", plan.commitAcknowledged, plan.commitAckLost, plan.rollbackAcknowledged)
		}
		if !obs.revalidated || !obs.fenceRechecked || !admission.armedNow() || !admission.dispatchedNow() {
			t.Fatalf("handoff facts: revalidated=%t fence=%t armed=%t dispatched=%t", obs.revalidated, obs.fenceRechecked, admission.armedNow(), admission.dispatchedNow())
		}
		if nativeMarker.callsNow() != 1 || !nativeMarker.pidDistinct || nativeMarker.pid == b.owner.BackendPID || !nativeMarker.instanceRevalidated || nativeMarker.endpointClosed {
			t.Fatalf("marker facts: calls=%d pidDistinct=%t pid=%d owner=%d instance=%t endpointClosed=%t",
				nativeMarker.callsNow(), nativeMarker.pidDistinct, nativeMarker.pid, b.owner.BackendPID, nativeMarker.instanceRevalidated, nativeMarker.endpointClosed)
		}
		postSnap := borrowedBoundSessionSnapshotNow(t, ctx, b)
		if postSnap.state != base.snapshot.state || postSnap.key != base.snapshot.key ||
			postSnap.fingerprint != base.snapshot.fingerprint || postSnap.inventoryNull != base.snapshot.inventoryNull ||
			postSnap.versionNull != base.snapshot.versionNull || postSnap.evidenceHash == base.snapshot.evidenceHash ||
			postSnap.guardHash == base.snapshot.guardHash || postSnap.generation != base.snapshot.generation+1 {
			t.Fatalf("the positive changed non-intended instance state: before=%+v after=%+v", base.snapshot, postSnap)
		}
		postRows := borrowedBoundNativeReadyRowsNow(t, ctx, b)
		// R5 ruling adds one audited durable fact to the interrupted attempt:
		// the coordinator's retained attempt_proof row (child start identity,
		// sole-Wait terminal, group drain and the role credential binding)
		// recorded by the authentic supervisor at failure time. The lane
		// therefore counts base+3 (marker + generation + retained proof) with
		// the same exact-count discipline; all other positive/negative class
		// assertions are unchanged.
		if postRows.evidenceCount != base.rows.evidenceCount || postRows.auditCount != base.rows.auditCount+3 {
			t.Fatalf("durable rows: evidence %d->%d audit %d->%d", base.rows.evidenceCount, postRows.evidenceCount, base.rows.auditCount, postRows.auditCount)
		}
		// The +3d row must be THIS attempt's retained proof: exactly one
		// attempt_proof row for THIS attempt's operation, action/result from
		// the coordinator's own write (not any observer or player), bound to
		// the original operation identity and the child's authentic facts.
		proofCount, proofHash := borrowedBoundNativeReadyAttemptProof(t, ctx, b, nativeAttemptID)
		if proofCount != 1 {
			t.Fatalf("attempt_proof rows for this attempt's operation = %d, want exactly 1 (the interrupted attempt's R5 row)", proofCount)
		}
		if proofHash == "" {
			t.Fatal("attempt_proof row digest absent")
		}
		var (
			proofActor, proofOperation, proofApplication, proofFactsJSON, proofTargetKeyOut string
			proofFacts                                                                      struct {
				TargetGuardKey  string `json:"target_guard_key"`
				Application     string `json:"application_name"`
				ChildPID        int    `json:"child_pid"`
				ChildStartID    uint64 `json:"child_start_id"`
				WaitTerminal    bool   `json:"wait_terminal"`
				ProcessDrained  bool   `json:"process_group_drained"`
				RoleFingerprint string `json:"role_fingerprint"`
				VerifierSHA256  string `json:"verifier_sha256"`
				Complete        bool   `json:"complete"`
			}
		)
		if err := b.fixture.controlPool.QueryRow(ctx, `
SELECT actor, operation_id, target->>'target_guard_key', detail->>'application_name', detail
FROM recovery_audit WHERE instance_id=$1 AND action='attempt_proof' AND result='ok'
  AND operation_id=$2`,
			b.fixture.instanceID, nativeAttemptID).Scan(&proofActor, &proofOperation, &proofTargetKeyOut, &proofApplication, &proofFactsJSON); err != nil {
			t.Fatalf("attempt_proof row read refused: %v", err)
		}
		if proofActor != "system:recovery-coordinator" {
			t.Fatalf("attempt_proof actor %q is not the coordinator's own write", proofActor)
		}
		if proofOperation != obs.attemptID || proofApplication != obs.nativeResult.Application || proofTargetKeyOut != f.guardKey {
			t.Fatalf("attempt_proof identity lost the original operation/app/target binding: op=%q app=%q target=%q want op=%q app=%q target=%q",
				proofOperation, proofApplication, proofTargetKeyOut, obs.attemptID, obs.nativeResult.Application, f.guardKey)
		}
		if err := json.Unmarshal([]byte(proofFactsJSON), &proofFacts); err != nil {
			t.Fatalf("attempt_proof detail json refused: %v", err)
		}
		if proofFacts.TargetGuardKey != f.guardKey || proofFacts.Application != obs.nativeResult.Application ||
			proofFacts.ChildPID != obs.nativeChildPID || proofFacts.ChildStartID != obs.nativeChildStartID ||
			!proofFacts.WaitTerminal || !proofFacts.ProcessDrained ||
			proofFacts.RoleFingerprint != fresh.binding.OriginalRoleFingerprint() ||
			proofFacts.VerifierSHA256 == "" || !proofFacts.Complete {
			t.Fatalf("attempt_proof facts do not bind the authentic interrupted child: %+v", proofFacts)
		}
		if afterOID, oidErr := borrowedOwnerDDLTargetOID(ctx, b); oidErr != nil || afterOID != base.catalogOID {
			t.Fatalf("replacement catalog changed: oid=%d err=%v", afterOID, oidErr)
		}
		borrowedNativeStartAssertPositiveDisposal(t, ctx, b, fresh, flowState, pristineBefore)
		replayResult, replayReceipt, replayErr := admission.dispatchOnce(ctx, nativeAttempt)
		if !errors.Is(replayErr, errBoundPostcommitAdmissionReplay) {
			t.Fatalf("the terminal gate handed off again: %v", replayErr)
		}
		borrowedBoundPostcommitAdmissionAssertNoAcceptedOutput(t, "terminal gate replay", replayResult, replayReceipt)
		admissionCopy := *admission
		copyResult, copyReceipt, copyErr := admissionCopy.dispatchOnce(ctx, nativeAttempt)
		if !errors.Is(copyErr, errBoundPostcommitAdmissionReplay) {
			t.Fatalf("a copied terminal gate handed off: %v", copyErr)
		}
		borrowedBoundPostcommitAdmissionAssertNoAcceptedOutput(t, "terminal copied gate replay", copyResult, copyReceipt)
		runCopy := *nativeAttempt.run
		replayCtx, cancelReplay := context.WithTimeout(ctx, 30*time.Second)
		runCopyResult, runCopyReceipt, runCopyErr := runCopy.Run(replayCtx)
		cancelReplay()
		if runCopyErr == nil || !strings.Contains(runCopyErr.Error(), "already reserved") {
			t.Fatalf("run copy replay was not refused as already reserved: %v", runCopyErr)
		}
		borrowedBoundPostcommitAdmissionAssertNoAcceptedOutput(t, "terminal run copy replay", runCopyResult, runCopyReceipt)
		if nativeMarker.callsNow() != 1 {
			t.Fatalf("terminal replay added a marker callback: %d", nativeMarker.callsNow())
		}
		t.Logf("Bounded native child start witnessed; authentication withheld; owner cancellation and drain witnessed; restoration unaccepted. The genuine supervised child started (PID distinct, sealed ELF, exact closed argv routed to the armed endpoint), the reader-only peer accepted its frontend connection and read the initial bytes without sending anything, the unchanged origin-attribution checks proved the accepted peer belonged to the observed child, cancellation of ONLY the child run context produced Started + PGCommandCanceled + ProcessGroupDrained, the authentic sole-Wait terminal observation and the single-consumed receipt bound to PID/start/target/role/operation, the committed marker/generation and the recorded launch / retained intent / drained writer / rebuild_required guard were re-read independently, the historical preparation audit stayed byte-identical and the fence was disposed cleanly with no continuing isolation")
	})

	// N1: rollback/commit-acknowledgement uncertainty cannot arm the handoff,
	// even with visible rows: the authentic rollback stage refuses, the gate
	// stays unarmed and no native outcome exists.
	t.Run("N1-no-acknowledged-commit", func(t *testing.T) {
		b, fresh, attempt, marker, probeCalls, acceptanceCalls := borrowedNativeStartFixedSetup(t, ctx, "noack")
		f := b.fixture
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		plan := newBorrowedBoundBaselineCommitPlan()
		plan.ownerLossBeforeCommit = true
		preparation := newBorrowedBoundBaselineCommitPreparation()
		admission := newBorrowedBoundPostcommitAdmission()
		flowState, flowErr := borrowedBoundWriterIsolationRun(ctx, t, b, fresh, borrowedNativeStartLineage(f, fresh, b), &borrowedBoundWriterIsolationHooks{
			duringProof: func(hookCtx context.Context, flow *borrowedBoundWriterIsolationFlowState) error {
				if err := borrowedBoundBaselineCommitStageRun(hookCtx, t, b, fresh, 1, plan, preparation); err != nil {
					return err
				}
				return errors.New("no-acknowledged-commit control unexpectedly acknowledged a commit")
			},
		})
		if !errors.Is(flowErr, errBoundBaselineCommitUncertain) || flowErr == nil || !strings.Contains(flowErr.Error(), "rollback target-lock acceptance transaction") {
			t.Fatalf("no-acknowledged-commit is not the authentic rollback-uncertainty stage: %v", flowErr)
		}
		if plan.commitAcknowledged || plan.rollbackAcknowledged {
			t.Fatal("no-acknowledged-commit reported a commit or acknowledged rollback")
		}
		if err := admission.armAfterAcknowledgedBaseline(plan, flowState); !errors.Is(err, errBoundPostcommitAdmissionBaseline) || admission.armedNow() {
			t.Fatalf("the handoff was armed without an acknowledged commit, even with visible rows: %v", err)
		}
		borrowedBoundPostcommitAdmissionAssertNoNativeOutcome(t, "no acknowledged commit", attempt, marker, probeCalls, acceptanceCalls, 0)
		borrowedBoundReentryReadinessAssertBaseline(t, "bound postcommit native start no acknowledged commit", ctx, b, base)
		healthCtx, cancelHealth := context.WithTimeout(ctx, borrowedOwnerRotationHealthBudget)
		healthErr := f.lock.Health(healthCtx)
		cancelHealth()
		if healthErr == nil {
			t.Fatal("the rollback-uncertainty control left the owner reusable")
		}
		if !flowState.cleanupRestored || flowState.cleanupErr != nil {
			t.Fatalf("no-acknowledged-commit fence disposal was not clean: restored=%t err=%v", flowState.cleanupRestored, flowState.cleanupErr)
		}
		t.Logf("no-acknowledged-commit negative: the authentic owner-loss-before-commit rollback uncertainty refused at its real stage, the gate could not arm from visible rows, no native marker/intent/child exists, the complete baseline stayed unchanged and the owner was retired with no reuse")
	})

	// N2: disposed/stale isolation: completed flow facts cannot dispatch, and
	// the captured committed baseline digest is preserved.
	t.Run("N2-disposed-isolation", func(t *testing.T) {
		b, fresh, attempt, marker, probeCalls, acceptanceCalls := borrowedNativeStartFixedSetup(t, ctx, "disposed")
		f := b.fixture
		plan := newBorrowedBoundBaselineCommitPlan()
		preparation := newBorrowedBoundBaselineCommitPreparation()
		admission := newBorrowedBoundPostcommitAdmission()
		obs := &borrowedBoundPostcommitAdmissionObservation{}
		flowState, flowErr := borrowedBoundWriterIsolationRun(ctx, t, b, fresh, borrowedNativeStartLineage(f, fresh, b), &borrowedBoundWriterIsolationHooks{
			duringProof: func(hookCtx context.Context, flow *borrowedBoundWriterIsolationFlowState) error {
				if err := borrowedBoundBaselineCommitStageRun(hookCtx, t, b, fresh, 1, plan, preparation); err != nil {
					return err
				}
				if !plan.commitAcknowledged {
					return errBoundPostcommitAdmissionBaseline
				}
				obs.attemptID = attempt.run.Binding().OriginalOperationID()
				obs.baselineOperation = plan.rehearsalOperationID
				obs.baselineAuditCount, obs.baselineAuditDigest = borrowedBoundPostcommitAdmissionAuditDigest(hookCtx, t, b, controlstore.ActionTargetGuardRebuild, plan.rehearsalOperationID)
				return admission.armAfterAcknowledgedBaseline(plan, flow)
			},
		})
		if flowErr != nil || flowState.cleanupErr != nil || !flowState.cleanupRestored || !admission.armedNow() || admission.dispatchedNow() {
			t.Fatalf("disposed-isolation control setup refused: flowErr=%v restored=%t cleanupErr=%v armed=%t dispatched=%t", flowErr, flowState.cleanupRestored, flowState.cleanupErr, admission.armedNow(), admission.dispatchedNow())
		}
		disposedResult, disposedReceipt, terminalErr := admission.dispatchOnce(ctx, attempt)
		if !errors.Is(terminalErr, errBoundPostcommitAdmissionTerminal) {
			t.Fatalf("a completed flow state handed off: %v", terminalErr)
		}
		borrowedBoundPostcommitAdmissionAssertNoAcceptedOutput(t, "disposed isolation", disposedResult, disposedReceipt)
		admissionCopy := *admission
		copyResult, copyReceipt, copyErr := admissionCopy.dispatchOnce(ctx, attempt)
		if !errors.Is(copyErr, errBoundPostcommitAdmissionTerminal) {
			t.Fatalf("a copied completed flow state handed off: %v", copyErr)
		}
		borrowedBoundPostcommitAdmissionAssertNoAcceptedOutput(t, "disposed isolation copy", copyResult, copyReceipt)
		borrowedBoundPostcommitAdmissionAssertNoNativeOutcome(t, "disposed isolation", attempt, marker, probeCalls, acceptanceCalls, 0)
		serverIP, ipErr := f.fx.container.ContainerIP(ctx)
		if ipErr != nil {
			t.Fatalf("disposed-isolation endpoint refused: %v", ipErr)
		}
		if err := borrowedBoundWriterIsolationAwaitState(t, ctx, f, serverIP, f.writerRole, b.passwordP1, "AUTH_OK", "", 10*time.Second); err != nil {
			t.Fatalf("disposed isolation did not restore the HBA: %v", err)
		}
		borrowedBoundPostcommitAdmissionAssertPreservedCommittedBaseline(t, ctx, b, plan, obs)
		t.Logf("disposed/stale-isolation negative: the completed flow state could not hand off through the gate or any copy, no native marker/intent/child exists and the captured committed baseline audit digest/classification is preserved")
	})

	// N3: live revalidation loss: an actual protected-control provenance loss
	// after the acknowledged commit refuses before any child.
	t.Run("N3-live-revalidation-loss", func(t *testing.T) {
		b, fresh, attempt, marker, probeCalls, acceptanceCalls := borrowedNativeStartFixedSetup(t, ctx, "revalidation")
		f := b.fixture
		plan := newBorrowedBoundBaselineCommitPlan()
		preparation := newBorrowedBoundBaselineCommitPreparation()
		admission := newBorrowedBoundPostcommitAdmission()
		flowState, flowErr := borrowedBoundWriterIsolationRun(ctx, t, b, fresh, borrowedNativeStartLineage(f, fresh, b), &borrowedBoundWriterIsolationHooks{
			duringProof: func(hookCtx context.Context, flow *borrowedBoundWriterIsolationFlowState) error {
				if err := borrowedBoundBaselineCommitStageRun(hookCtx, t, b, fresh, 1, plan, preparation); err != nil {
					return err
				}
				if !plan.commitAcknowledged {
					return errBoundPostcommitAdmissionBaseline
				}
				if err := admission.armAfterAcknowledgedBaseline(plan, flow); err != nil {
					return err
				}
				restoreControl := borrowedBoundPostcommitAdmissionSuspendControl(t, hookCtx, b)
				defer restoreControl()
				return borrowedBoundPostcommitAdmissionRevalidate(hookCtx, t, b, fresh, plan, flow)
			},
		})
		if !errors.Is(flowErr, errBoundPostcommitAdmissionRevalidate) {
			t.Fatalf("live-revalidation-loss is not the real handoff revalidation stage: %v", flowErr)
		}
		if !plan.commitAcknowledged || plan.commitAckLost || plan.rollbackAcknowledged {
			t.Fatalf("live-revalidation-loss baseline classification: acknowledged=%t ackLost=%t rollback=%t", plan.commitAcknowledged, plan.commitAckLost, plan.rollbackAcknowledged)
		}
		if !admission.armedNow() || admission.dispatchedNow() {
			t.Fatalf("live-revalidation-loss handoff facts: armed=%t dispatched=%t", admission.armedNow(), admission.dispatchedNow())
		}
		borrowedBoundPostcommitAdmissionAssertNoNativeOutcome(t, "live revalidation loss", attempt, marker, probeCalls, acceptanceCalls, 0)
		borrowedBoundPostcommitAdmissionAssertPreservedDurableDelta(t, ctx, b, plan.rehearsalOperationID)
		if !flowState.cleanupRestored || flowState.cleanupErr != nil {
			t.Fatalf("live-revalidation-loss fence disposal was not clean: restored=%t err=%v", flowState.cleanupRestored, flowState.cleanupErr)
		}
		t.Logf("live-revalidation-loss negative: the suspended protected control route refused the fresh revalidation after the acknowledged commit, so no native child was ever started; the committed preparation delta is preserved and the gate never dispatched")
	})

	// N4: endpoint loss BEFORE preflight: the exact preflight endpoint
	// capability refusal, no preparation transaction and no native outcome.
	t.Run("N4-endpoint-before-preflight", func(t *testing.T) {
		b, fresh, attempt, marker, probeCalls, acceptanceCalls := borrowedNativeStartFixedSetup(t, ctx, "preflight")
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		if err := attempt.gate.endpoint.Listener().Close(); err != nil {
			t.Fatalf("preflight endpoint closure refused: %v", err)
		}
		runCtx, cancelRun := context.WithTimeout(ctx, 60*time.Second)
		result, receipt, runErr := attempt.dispatchRun(runCtx)
		cancelRun()
		if runErr == nil || !strings.Contains(runErr.Error(), "refused at preflight") || !strings.Contains(runErr.Error(), "origin endpoint capability") || strings.Contains(runErr.Error(), "refused at launch") {
			t.Fatalf("endpoint loss before preflight is not the actual preflight refusal: %v", runErr)
		}
		borrowedBoundPostcommitAdmissionAssertNoNativeOutcome(t, "endpoint before preflight", attempt, marker, probeCalls, acceptanceCalls, 0)
		borrowedBoundPostcommitAdmissionAssertNoAcceptedOutput(t, "endpoint before preflight", result, receipt)
		borrowedBoundReentryReadinessAssertBaseline(t, "bound postcommit native start endpoint before preflight", ctx, b, base)
		t.Logf("endpoint-loss-before-preflight negative: the actual preflight hook refused (`%v`) before any preparation transaction, marker callback, launch intent or child; durable state classification: complete baseline equality, no transaction, no authorizing output", runErr)
	})

	// N5: endpoint loss AFTER the committed marker: the exact launch-stage
	// endpoint refusal with the committed intent and zero child start.
	t.Run("N5-endpoint-after-marker", func(t *testing.T) {
		b, fresh, attempt, marker, probeCalls, acceptanceCalls := borrowedNativeStartFixedSetup(t, ctx, "aftermarker")
		f := b.fixture
		plan := newBorrowedBoundBaselineCommitPlan()
		preparation := newBorrowedBoundBaselineCommitPreparation()
		admission := newBorrowedBoundPostcommitAdmission()
		obs := &borrowedBoundPostcommitAdmissionObservation{}
		operationID := attempt.run.Binding().OriginalOperationID()
		flowState, flowErr := borrowedBoundWriterIsolationRun(ctx, t, b, fresh, borrowedNativeStartLineage(f, fresh, b), &borrowedBoundWriterIsolationHooks{
			duringProof: func(hookCtx context.Context, flow *borrowedBoundWriterIsolationFlowState) error {
				return borrowedBoundPostcommitAdmissionDuringProof(hookCtx, t, b, fresh, attempt, marker, admission, plan, preparation, 1, probeCalls, acceptanceCalls, obs, flow, &borrowedBoundPostcommitAdmissionHookPlan{attemptID: operationID, expect: "launch"})
			},
		})
		if flowErr != nil {
			t.Fatalf("endpoint-after-marker control flow refused: %v", flowErr)
		}
		if !plan.commitAcknowledged || !admission.dispatchedNow() || obs.nativeErr == nil ||
			!strings.Contains(obs.nativeErr.Error(), "refused at launch") || !strings.Contains(obs.nativeErr.Error(), "origin endpoint capability") {
			t.Fatalf("endpoint-after-marker classification: acknowledged=%t dispatched=%t err=%v", plan.commitAcknowledged, admission.dispatchedNow(), obs.nativeErr)
		}
		if marker.callsNow() != 1 || marker.token() == (recovery.EvidenceToken{}) || !marker.endpointClosed {
			t.Fatalf("endpoint-after-marker marker facts: calls=%d token=%+v endpointClosed=%t", marker.callsNow(), marker.token(), marker.endpointClosed)
		}
		if identity := attempt.run.Observation().StartedIdentity(); identity.Started || identity.PID != 0 || identity.StartID != 0 {
			t.Fatalf("endpoint-after-marker control started a child: %+v", identity)
		}
		borrowedBoundPostcommitAdmissionAssertNativeIntent(ctx, t, b, fresh, attempt, marker, obs.nativeResult, obs.nativeReceipt, probeCalls, acceptanceCalls, obs)
		if !flowState.cleanupRestored || flowState.cleanupErr != nil {
			t.Fatalf("endpoint-after-marker fence disposal was not clean: restored=%t err=%v", flowState.cleanupRestored, flowState.cleanupErr)
		}
		t.Logf("endpoint-loss-after-marker negative: the committed transactional marker closed the endpoint, the unchanged launch-stage endpoint revalidation refused, the durable launch intent was retained with zero child start, absent receipt and no accepted output, and the fence was disposed cleanly")
	})

	// N6: cancellation before dispatch: no native outcome at all.
	t.Run("N6-cancel-before-dispatch", func(t *testing.T) {
		b, fresh, attempt, marker, probeCalls, acceptanceCalls := borrowedNativeStartFixedSetup(t, ctx, "cancel-before")
		f := b.fixture
		plan := newBorrowedBoundBaselineCommitPlan()
		preparation := newBorrowedBoundBaselineCommitPreparation()
		admission := newBorrowedBoundPostcommitAdmission()
		obs := &borrowedBoundPostcommitAdmissionObservation{}
		laneCtx, cancelLane := context.WithCancel(ctx)
		defer cancelLane()
		operationID := attempt.run.Binding().OriginalOperationID()
		flowState, flowErr := borrowedBoundWriterIsolationRun(laneCtx, t, b, fresh, borrowedNativeStartLineage(f, fresh, b), &borrowedBoundWriterIsolationHooks{
			duringProof: func(hookCtx context.Context, flow *borrowedBoundWriterIsolationFlowState) error {
				return borrowedBoundPostcommitAdmissionDuringProof(hookCtx, t, b, fresh, attempt, marker, admission, plan, preparation, 1, probeCalls, acceptanceCalls, obs, flow, &borrowedBoundPostcommitAdmissionHookPlan{
					attemptID: operationID, expect: "source", cancelBeforeHandoff: true, cancelLane: cancelLane,
				})
			},
		})
		if !errors.Is(flowErr, errWriterIsolationCancelled) || laneCtx.Err() == nil {
			t.Fatalf("cancel-before-dispatch did not refuse at the flow cancellation stage: %v", flowErr)
		}
		if !plan.commitAcknowledged || plan.rollbackAcknowledged || obs.nativeErr == nil || !strings.Contains(obs.nativeErr.Error(), "caller context ended") {
			t.Fatalf("cancel-before-dispatch classification: acknowledged=%t rollback=%t err=%v", plan.commitAcknowledged, plan.rollbackAcknowledged, obs.nativeErr)
		}
		borrowedBoundPostcommitAdmissionAssertNoNativeOutcome(t, "cancel before dispatch", attempt, marker, probeCalls, acceptanceCalls, 0)
		borrowedBoundPostcommitAdmissionAssertNoAcceptedOutput(t, "cancel before dispatch", obs.nativeResult, obs.nativeReceipt)
		borrowedBoundPostcommitAdmissionAssertPreservedCommittedBaseline(t, ctx, b, plan, obs)
		if !flowState.cleanupRestored || flowState.cleanupErr != nil {
			t.Fatalf("cancel-before-dispatch fence disposal was not clean: restored=%t err=%v", flowState.cleanupRestored, flowState.cleanupErr)
		}
		t.Logf("cancel-before-dispatch negative: the committed baseline classification was preserved, the prepared attempt refused at its real caller-context stage before any coordinator entry and no native marker/intent/child exists")
	})

	// N7: cancellation inside the marker before success: the recorded injection
	// and the exact classified marker failure, with a rolled-back marker.
	t.Run("N7-cancel-inside-marker", func(t *testing.T) {
		b, fresh, attempt, marker, probeCalls, acceptanceCalls := borrowedNativeStartFixedSetup(t, ctx, "cancel-inside")
		f := b.fixture
		plan := newBorrowedBoundBaselineCommitPlan()
		preparation := newBorrowedBoundBaselineCommitPreparation()
		admission := newBorrowedBoundPostcommitAdmission()
		obs := &borrowedBoundPostcommitAdmissionObservation{}
		laneCtx, cancelLane := context.WithCancel(ctx)
		defer cancelLane()
		operationID := attempt.run.Binding().OriginalOperationID()
		marker.preDelegate = func(context.Context) error {
			cancelLane()
			return errBoundPostcommitAdmissionCancelled
		}
		flowState, flowErr := borrowedBoundWriterIsolationRun(laneCtx, t, b, fresh, borrowedNativeStartLineage(f, fresh, b), &borrowedBoundWriterIsolationHooks{
			duringProof: func(hookCtx context.Context, flow *borrowedBoundWriterIsolationFlowState) error {
				return borrowedBoundPostcommitAdmissionDuringProof(hookCtx, t, b, fresh, attempt, marker, admission, plan, preparation, 1, probeCalls, acceptanceCalls, obs, flow, &borrowedBoundPostcommitAdmissionHookPlan{
					attemptID: operationID, expect: "marker", cancelLane: cancelLane,
				})
			},
		})
		if !errors.Is(flowErr, errWriterIsolationCancelled) || laneCtx.Err() == nil {
			t.Fatalf("cancel-inside-marker did not refuse at the flow cancellation stage: %v", flowErr)
		}
		if !plan.commitAcknowledged || !borrowedBoundPostcommitAdmissionMarkerRefusalClassified(obs.nativeErr, marker) {
			t.Fatalf("cancel-inside-marker classification: acknowledged=%t err=%v", plan.commitAcknowledged, obs.nativeErr)
		}
		if marker.callsNow() != 1 || !marker.pidDistinct || !marker.instanceRevalidated || marker.endpointClosed {
			t.Fatalf("cancel-inside-marker marker facts: calls=%d pid=%t instance=%t endpointClosed=%t", marker.callsNow(), marker.pidDistinct, marker.instanceRevalidated, marker.endpointClosed)
		}
		if marker.token() != (recovery.EvidenceToken{}) {
			t.Fatal("cancel-inside-marker produced a genuine marker token although the transaction never committed")
		}
		borrowedBoundPostcommitAdmissionAssertNoNativeOutcome(t, "cancel inside marker", attempt, marker, probeCalls, acceptanceCalls, 1)
		borrowedBoundPostcommitAdmissionAssertNoAcceptedOutput(t, "cancel inside marker", obs.nativeResult, obs.nativeReceipt)
		borrowedBoundPostcommitAdmissionAssertPreservedCommittedBaseline(t, ctx, b, plan, obs)
		if !flowState.cleanupRestored || flowState.cleanupErr != nil {
			t.Fatalf("cancel-inside-marker fence disposal was not clean: restored=%t err=%v", flowState.cleanupRestored, flowState.cleanupErr)
		}
		t.Logf("cancel-inside-marker negative: the marker ran its live/provenance checks on the genuine preparation transaction, the recorded injected cancellation refused before success, the prelaunch transaction rolled back with no committed marker/intent, and the committed baseline stayed byte-identical")
	})

	// N8: copy/concurrent replay: exactly ONE reservation and ONE real child
	// start; two competing contenders (original gate + copied gate) race for the
	// same attempt, and the original/copied runs and further replays can neither
	// overwrite the observation nor consume another receipt.
	t.Run("N8-copy-concurrent-replay", func(t *testing.T) {
		b, fresh, attempt, marker, probeCalls, acceptanceCalls := borrowedNativeStartFixedSetup(t, ctx, "replay")
		f := b.fixture
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		plan := newBorrowedBoundBaselineCommitPlan()
		preparation := newBorrowedBoundBaselineCommitPreparation()
		admission := newBorrowedBoundPostcommitAdmission()
		obs := &borrowedBoundPostcommitAdmissionObservation{}
		marker.closeEndpoint = nil
		pristineBefore := borrowedBoundPostcommitAdmissionPristine(t, ctx, b)
		joinState := &borrowedNativeStartJoinState{}
		peer := newBorrowedNativeStartPeer(attempt.gate.endpoint.Listener())
		defer func() {
			if err := peer.closeAndJoin(30 * time.Second); err != nil {
				t.Errorf("copy/concurrent-replay peer shutdown/join refused: %v", err)
			}
		}()
		flowState, flowErr := borrowedBoundWriterIsolationRun(ctx, t, b, fresh, borrowedNativeStartLineage(f, fresh, b), &borrowedBoundWriterIsolationHooks{
			duringProof: func(hookCtx context.Context, flow *borrowedBoundWriterIsolationFlowState) error {
				return borrowedNativeStartLiveHook(hookCtx, t, b, fresh, attempt, marker, admission, plan, preparation, 1, probeCalls, acceptanceCalls, obs, flow, peer, true, joinState, nil)
			},
		})
		if flowErr != nil {
			t.Fatalf("copy/concurrent-replay control refused: %v", flowErr)
		}
		if !joinState.completeNow() || !joinState.drainedNow() || !joinState.eofClosedNow() || !joinState.peerJoinedNow() {
			t.Fatalf("copy/concurrent-replay live-hook cleanup did not complete inside the hook: complete=%t drained=%t eofClosed=%t peerJoined=%t errs=%v",
				joinState.completeNow(), joinState.drainedNow(), joinState.eofClosedNow(), joinState.peerJoinedNow(), joinState.errsNow())
		}
		if !plan.commitAcknowledged || !obs.revalidated || !obs.fenceRechecked || !admission.dispatchedNow() {
			t.Fatalf("copy/concurrent-replay facts: acknowledged=%t revalidated=%t fence=%t dispatched=%t", plan.commitAcknowledged, obs.revalidated, obs.fenceRechecked, admission.dispatchedNow())
		}
		if marker.callsNow() != 1 || marker.token() == (recovery.EvidenceToken{}) || marker.endpointClosed {
			t.Fatalf("copy/concurrent-replay marker facts: calls=%d token=%+v endpointClosed=%t", marker.callsNow(), marker.token(), marker.endpointClosed)
		}
		borrowedNativeStartAssertPositiveDisposal(t, ctx, b, fresh, flowState, pristineBefore)
		_ = base
		t.Logf("copy/concurrent-replay negative: TWO channel-coordinated contenders (the original gate and an actual copy) raced for the one attempt while the child was held; exactly ONE won the shared one-use reservation and started the one supervised native child (launch-stage accepted by the reader-only peer), the competing contender resolved to the gate replay refusal with zero result and no receipt, and the rejected original run, copied run and further gate replay also published zero output without overwriting the real start facts or adding a marker callback")
	})

	// N9: live-hook early prerequisite failure: the stage refuses before any
	// dispatch and the hook's own deferred cleanup closes and joins EVERY peer
	// goroutine before the flow disposes the fence.
	t.Run("N9-live-hook-early-failure", func(t *testing.T) {
		b, fresh, attempt, marker, probeCalls, acceptanceCalls := borrowedNativeStartFixedSetup(t, ctx, "hook-early")
		f := b.fixture
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		plan := newBorrowedBoundBaselineCommitPlan()
		plan.routeOverride = func(transport string) (string, bool) {
			if transport == "loopback" {
				return "AUTHCHECK state=AUTH_OK", true
			}
			return "", false
		}
		preparation := newBorrowedBoundBaselineCommitPreparation()
		admission := newBorrowedBoundPostcommitAdmission()
		obs := &borrowedBoundPostcommitAdmissionObservation{}
		joinState := &borrowedNativeStartJoinState{}
		peer := newBorrowedNativeStartPeer(attempt.gate.endpoint.Listener())
		defer func() {
			if err := peer.closeAndJoin(30 * time.Second); err != nil {
				t.Errorf("early-failure peer shutdown/join refused: %v", err)
			}
		}()
		flowState, flowErr := borrowedBoundWriterIsolationRun(ctx, t, b, fresh, borrowedNativeStartLineage(f, fresh, b), &borrowedBoundWriterIsolationHooks{
			duringProof: func(hookCtx context.Context, flow *borrowedBoundWriterIsolationFlowState) error {
				return borrowedNativeStartLiveHook(hookCtx, t, b, fresh, attempt, marker, admission, plan, preparation, 1, probeCalls, acceptanceCalls, obs, flow, peer, false, joinState, nil)
			},
		})
		if flowErr == nil || !errors.Is(flowErr, errBoundBaselineCommitRoute) {
			t.Fatalf("live-hook early failure is not the real route stage: %v", flowErr)
		}
		if plan.commitAcknowledged || admission.armedNow() || admission.dispatchedNow() {
			t.Fatalf("live-hook early-failure control facts: ack=%t armed=%t dispatched=%t", plan.commitAcknowledged, admission.armedNow(), admission.dispatchedNow())
		}
		if !joinState.attemptedNow() || !joinState.drainedNow() || !joinState.peerJoinedNow() || !joinState.completeNow() {
			t.Fatalf("live-hook early-failure cleanup did not complete inside the hook: attempted=%t drained=%t peerJoined=%t complete=%t errs=%v",
				joinState.attemptedNow(), joinState.drainedNow(), joinState.peerJoinedNow(), joinState.completeNow(), joinState.errsNow())
		}
		borrowedBoundPostcommitAdmissionAssertNoNativeOutcome(t, "live-hook early failure", attempt, marker, probeCalls, acceptanceCalls, 0)
		borrowedBoundReentryReadinessAssertBaseline(t, "bound postcommit native start live-hook early failure", ctx, b, base)
		if !flowState.cleanupRestored || flowState.cleanupErr != nil {
			t.Fatalf("live-hook early-failure fence disposal was not clean: restored=%t err=%v", flowState.cleanupRestored, flowState.cleanupErr)
		}
		t.Logf("live-hook early-failure negative: a real pre-dispatch route-stage prerequisite failure returned through the hook's deferred cleanup, which closed and joined every reader-only peer goroutine and recorded completion before the flow disposed the fence; no native outcome exists and the complete baseline is preserved")
	})

	// N10: live-hook post-dispatch failure: with the genuine child held and the
	// reader-only peer in flight, the hook returns an injected failure through
	// its deferred cleanup, which cancels/drains the child and joins every
	// goroutine before the fence disposal; the authentic sole-Wait and the
	// rebuild_required durable disposition are retained.
	t.Run("N10-live-hook-post-dispatch-failure", func(t *testing.T) {
		b, fresh, attempt, marker, probeCalls, acceptanceCalls := borrowedNativeStartFixedSetup(t, ctx, "hook-postdispatch")
		f := b.fixture
		plan := newBorrowedBoundBaselineCommitPlan()
		preparation := newBorrowedBoundBaselineCommitPreparation()
		admission := newBorrowedBoundPostcommitAdmission()
		obs := &borrowedBoundPostcommitAdmissionObservation{}
		marker.closeEndpoint = nil
		joinState := &borrowedNativeStartJoinState{}
		peer := newBorrowedNativeStartPeer(attempt.gate.endpoint.Listener())
		defer func() {
			if err := peer.closeAndJoin(30 * time.Second); err != nil {
				t.Errorf("post-dispatch-failure peer shutdown/join refused: %v", err)
			}
		}()
		injected := errors.New("bound postcommit native start: injected post-dispatch failure after the held-child witness")
		flowState, flowErr := borrowedBoundWriterIsolationRun(ctx, t, b, fresh, borrowedNativeStartLineage(f, fresh, b), &borrowedBoundWriterIsolationHooks{
			duringProof: func(hookCtx context.Context, flow *borrowedBoundWriterIsolationFlowState) error {
				return borrowedNativeStartLiveHook(hookCtx, t, b, fresh, attempt, marker, admission, plan, preparation, 1, probeCalls, acceptanceCalls, obs, flow, peer, false, joinState, injected)
			},
		})
		if !errors.Is(flowErr, injected) {
			t.Fatalf("live-hook post-dispatch failure did not surface the injected failure: %v", flowErr)
		}
		if !joinState.attemptedNow() || !joinState.drainedNow() || !joinState.eofClosedNow() || !joinState.peerJoinedNow() || !joinState.completeNow() {
			t.Fatalf("live-hook post-dispatch-failure cleanup did not complete inside the hook: attempted=%t drained=%t eofClosed=%t peerJoined=%t complete=%t errs=%v",
				joinState.attemptedNow(), joinState.drainedNow(), joinState.eofClosedNow(), joinState.peerJoinedNow(), joinState.completeNow(), joinState.errsNow())
		}
		handle := attempt.run.Observation()
		identity := handle.StartedIdentity()
		if !identity.Started || identity.PID <= 0 || identity.StartID == 0 {
			t.Fatalf("live-hook post-dispatch failure did not witness the started child: %+v", identity)
		}
		if terminal := handle.WaitTerminal(); !terminal.ChildWaitCompleted || !terminal.Terminal {
			t.Fatalf("live-hook post-dispatch cleanup did not join the authentic sole-Wait terminal fact: %+v", terminal)
		}
		if marker.callsNow() != 1 || marker.token() == (recovery.EvidenceToken{}) {
			t.Fatalf("live-hook post-dispatch-failure marker facts: calls=%d token=%+v", marker.callsNow(), marker.token())
		}
		guard, found, guardErr := controlstore.ReadTargetGuard(ctx, f.controlPool, f.guardKey)
		if guardErr != nil || !found || guard.State != controlstore.TargetGuardRebuildRequired || guard.ActiveWriter || !guard.LaunchIntent ||
			guard.LaunchedAt == nil || guard.RebuildRequiredAt == nil || guard.CleanAt != nil {
			t.Fatalf("live-hook post-dispatch-failure durable disposition: err=%v found=%t guard=%+v", guardErr, found, guard)
		}
		if dumpPath, ok := f.tools.DumpPath(); ok {
			borrowedNativeStartAwaitGone(t, filepath.Join(filepath.Dir(dumpPath), "pg_restore"), 15*time.Second)
		}
		if atomic.LoadInt32(probeCalls) != 0 || atomic.LoadInt32(acceptanceCalls) != 0 {
			t.Fatal("live-hook post-dispatch-failure control ran probe/acceptance callbacks")
		}
		if !flowState.cleanupRestored || flowState.cleanupErr != nil {
			t.Fatalf("live-hook post-dispatch-failure fence disposal was not clean: restored=%t err=%v", flowState.cleanupRestored, flowState.cleanupErr)
		}
		t.Logf("live-hook post-dispatch-failure negative: with the genuine supervised child held and the reader-only peer in flight, the injected failure returned through the hook's deferred cleanup, which canceled+drained the dispatch, closed/joined every peer goroutine and recorded completion before the fence disposal; the authentic sole-Wait terminal observation and the rebuild_required durable disposition were retained")
	})
}
