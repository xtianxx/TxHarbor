//go:build linux && drill

// origin_gate_proxy_linux_test.go implements the bounded TEST-ONLY
// continuous-origin gate primitive for the fix101 lane. It is a real TCP
// PostgreSQL forwarding proxy, not a stub:
//
//   - the gate endpoint is declared (listening) before any test client is
//     spawned against it;
//   - the only way to admit a connection is a private one-use capability issued
//     by the fixture launcher that owns the real exec.Cmd of the frontend
//     client; the capability type has no exported fields, no JSON constructor
//     and no generic register/PID/clean flag;
//   - admission binds the accepted frontend peer four-tuple to the real client
//     OS process start identity and its own /proc fd socket inode;
//   - the gate opens a second real TCP connection to the protected native PG18
//     fixture and forwards the genuine SCRAM exchange; it never manufactures
//     AuthenticationOk, SSL/GSS negotiation bytes or a gate-only P0 verdict;
//   - executable frontend frames (Query/extended protocol/Copy) are held until
//     the server BackendKeyData PID is registered through a protected observer
//     (backend_start) plus the real server process identity and socket inode;
//   - any origin rejection, protocol violation, observer loss or protected
//     server liveness loss latches the gate forever: both directions are
//     closed, new connections are refused and there is no success fallback.
//
// The primitive is deliberately not the supervised writer receipt/rebuild or
// atomic acceptance path; those remain OPEN and are wired by later lanes.
package recovery_test

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"

	"github.com/xtianxx/txharbor/internal/recovery"
)

const (
	// originGateEvidence is the fresh bounded proof ledger for this lane. It
	// never rewrites the archived restart-auth-prototype evidence.
	originGateEvidence = "/tmp/opencode/015-e504462/origin-gate-proofs.jsonl"
	// originGateMessageMax bounds every relayed PostgreSQL frame.
	originGateMessageMax = 1 << 20
	// originGateAppName is deliberately shared by owned and unowned clients to
	// prove that application_name is never consulted for admission.
	originGateAppName = "origin-gate-copied-application-name"
)

type originGateEvidenceRecord struct {
	Time   string `json:"time"`
	Test   string `json:"test"`
	Result string `json:"result"`
	Detail string `json:"detail,omitempty"`
}

// recordOriginGateResult appends one bounded public record per drill test. The
// detail never contains credentials, SCRAM material or query text.
func recordOriginGateResult(t *testing.T, name string, detail *string) {
	t.Helper()
	t.Cleanup(func() {
		if err := os.MkdirAll(filepath.Dir(originGateEvidence), 0o700); err != nil {
			t.Errorf("preserve origin-gate evidence: %v", err)
			return
		}
		f, err := os.OpenFile(originGateEvidence, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
		if err != nil {
			t.Errorf("preserve origin-gate evidence: %v", err)
			return
		}
		result := "PASS"
		if t.Failed() {
			result = "FAIL"
		}
		text := ""
		if detail != nil {
			text = *detail
		}
		_ = json.NewEncoder(f).Encode(originGateEvidenceRecord{
			Time: time.Now().UTC().Format(time.RFC3339Nano), Test: name,
			Result: result, Detail: text,
		})
		if err := f.Close(); err != nil {
			t.Errorf("close origin-gate evidence: %v", err)
		}
	})
}

// originGateFixture is one protected native PG18 fixture shared by the funnel
// tests of a single drill case: a restricted W role forced through SCRAM, a
// target table whose row count is the SQL-side barrier, the pinned fixture
// census helper and the exact postmaster identity. The carrier image is the
// repo-pinned postgres@sha256:4ef4dbc939d61acea57712655ddb4b4ab27419c913f94cca0cd57cb3ea3c2280
// (18.6-trixie) started through startAuthBoundaryPGWithPostmasterChildContainer
// with the explicit protected SCRAM-only HBA; there is no stub server and no
// native PG restore replacement anywhere in this lane.
type originGateFixture struct {
	t             *testing.T
	container     testcontainers.Container
	containerID   string
	dsn           string
	serverAddr    string
	admin         *pgx.Conn
	role          string
	password      string
	table         string
	postmasterPID int
	postmasterStr string
}

func newOriginGateFixture(t *testing.T) *originGateFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	container, dsn := startAuthBoundaryPGWithPostmasterChildContainer(t)
	containerID := container.GetContainerID()
	requireNativePG18(t, dsn)
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect protected fixture administrator: %v", err)
	}
	t.Cleanup(func() { _ = admin.Close(context.Background()) })
	role := fmt.Sprintf("origin_gate_w_%d", time.Now().UnixNano())
	table := fmt.Sprintf("origin_gate_rows_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, `CREATE ROLE `+pgx.Identifier{role}.Sanitize()+
		` LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD `+sqlLiteral(passwordP0)); err != nil {
		t.Fatalf("create restricted origin-gate W role: %v", err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), `DROP ROLE IF EXISTS `+pgx.Identifier{role}.Sanitize())
	})
	if _, err := admin.Exec(ctx, `CREATE TABLE `+pgx.Identifier{table}.Sanitize()+` (ref text NOT NULL)`); err != nil {
		t.Fatalf("create origin-gate target table: %v", err)
	}
	if _, err := admin.Exec(ctx, `GRANT INSERT, SELECT ON `+pgx.Identifier{table}.Sanitize()+` TO `+pgx.Identifier{role}.Sanitize()); err != nil {
		t.Fatalf("grant restricted W role target table access: %v", err)
	}
	assertProtectedScramOnlyHBA(t, ctx, admin)
	censusHelper := buildAuthBoundaryClientHelper(t)
	if err := container.CopyFileToContainer(ctx, censusHelper, "/tmp/drill-auth-boundary-client", 0o700); err != nil {
		t.Fatalf("copy fixture census helper into protected server namespace: %v", err)
	}
	postmasterPID, postmasterStart, err := inspectPostmaster(ctx, containerID)
	if err != nil {
		t.Fatalf("bind fixture postmaster identity: %v", err)
	}
	if postmasterPID == 1 {
		t.Fatalf("postmaster is namespace PID 1; the origin-gate fixture requires a non-PID1 postmaster")
	}
	u, err := url.Parse(dsn)
	if err != nil || u.Host == "" {
		t.Fatalf("parse protected fixture endpoint: %v", err)
	}
	return &originGateFixture{
		t: t, container: container, containerID: containerID, dsn: dsn, serverAddr: u.Host,
		admin: admin, role: role, password: passwordP0, table: table,
		postmasterPID: postmasterPID, postmasterStr: postmasterStart,
	}
}

func (f *originGateFixture) rowCount(ctx context.Context, ref string) (int, error) {
	var count int
	err := f.admin.QueryRow(ctx, `SELECT count(*) FROM `+pgx.Identifier{f.table}.Sanitize()+` WHERE ref=$1`, ref).Scan(&count)
	return count, err
}

func (f *originGateFixture) mustRowCount(ctx context.Context, ref string) int {
	f.t.Helper()
	count, err := f.rowCount(ctx, ref)
	if err != nil {
		f.t.Fatalf("read target table row count for ref %q: %v", ref, err)
	}
	return count
}

func (f *originGateFixture) totalRows(ctx context.Context) (int, error) {
	var count int
	err := f.admin.QueryRow(ctx, `SELECT count(*) FROM `+pgx.Identifier{f.table}.Sanitize()).Scan(&count)
	return count, err
}

// originGateCap is the private one-use origin capability. Every field is
// unexported: it has no JSON constructor, no exported register/clean flags and
// can only be created by originGateLauncher.launch, which retains the real
// exec.Cmd of the spawned client. JSON marshal yields an empty object and a
// JSON-forged copy carries no usable token.
type originGateCap struct {
	token [32]byte
	pid   int
	start string
	// ownerClient retains the fixture launcher's own process handle so gate
	// retirement can consume the launcher's captured sole cmd.Wait receipt
	// instead of inferring reaping from /proc errors.
	ownerClient *originGateClient
	// expectedRole/expectedDatabase are test-only protocol-layer startup
	// expectations for the non-native mismatch negative. They carry no endpoint
	// or sealed-tool authority; the native path binds identity inside the arm.
	expectedRole     string
	expectedDatabase string
}

type originGateCapState struct {
	pid      int
	start    string
	consumed bool
}

// originGateRegistration is the protected-observer registration of one real
// server backend. It never contains SQL text, role names or application_name.
type originGateRegistration struct {
	PID          int
	BackendStart time.Time
	OSStart      string
	SocketInode  string
	SocketLocal  string
	SocketRemote string
	ClientAddr   string
	ClientPort   int
}

type originGateSessionReport struct {
	CapPID          int
	CapStart        string
	PeerIP          string
	PeerPort        int
	OriginInode     string
	BackendPID      int
	ServerAuthOK    bool
	Supervisor      bool
	ObservedPID     int
	ObservedStartID uint64
	BoundRole       string
	BoundDatabase   string
}

var (
	errOriginGateEnded             = errors.New("origin gate session ended normally")
	errOriginGateProtocolViolation = errors.New("origin gate protocol violation")
)

// originGate is the real forwarding gate. It has one listener, at most one
// active admitted session, and a latch that is permanent once set.
type originGate struct {
	fx         *originGateFixture
	endpoint   recovery.DrillOriginEndpoint
	ln         net.Listener
	gateIP     string
	gatePort   int
	serverAddr string

	// armedRestore is the gate-private arm record: the exact fresh handle armed
	// against this gate's factory endpoint and sealed tools before invocation.
	armedRestore *originGateArmedRestore

	observer          *pgx.Conn
	observerPID       int
	observerBackendAt time.Time
	observerOSStart   string
	observerMu        sync.Mutex

	// control is the independent protected control session used only to
	// terminate the exact registered backend and to verify its authoritative
	// drain. It is deliberately distinct from the observer, which may be the
	// failed component.
	control    *pgx.Conn
	controlPID int
	controlMu  sync.Mutex

	drainMu       sync.Mutex
	drainPending  bool
	drainVerified bool
	drainUnknown  bool
	drainReport   string
	drainTimeout  time.Duration

	mu                  sync.Mutex
	issued              map[[32]byte]*originGateCapState
	latched             bool
	latchReason         string
	latchCh             chan struct{}
	active              *originGateSession
	admitting           bool
	cleanRetired        bool
	cleanReport         string
	publishBarrier      chan struct{}
	censusBarrier       chan struct{}
	censusEntered       chan struct{}
	censusEnteredOnce   sync.Once
	publishDone         chan struct{}
	publishOnce         sync.Once
	seam                *originGateInspectionSeam
	supervisorOrigins   []recovery.DrillClaimedOrigin
	registrationBarrier chan struct{}
	barrierOnce         sync.Once
	// borrowedCheck is the optional borrowed-writer pre-release check. It is
	// nil in the normal mode (no behavior change for the 26 prototype tests):
	// when set, it runs after registration and the first successful watcher
	// pass, immediately before the held executable frames are released.
	borrowedCheck func(context.Context, *originGateRegistration) error
	// borrowedOrigin is the optional borrowed-writer origin registration
	// (concrete run + fresh identity prefix) used only by AdmitBorrowed.
	borrowedOrigin *originGateBorrowedOrigin
	// mode is the immutable gate mode. It is frozen under g.mu before any
	// admission or arm and can never switch between ordinary and borrowed.
	mode originGateMode
}

// originGateMode is the frozen gate usage mode. Precedence is structural: the
// mode is chosen under g.mu before admission/arm and is immutable afterwards.
type originGateMode int

const (
	gateModeFree originGateMode = iota
	gateModeOrdinary
	gateModeBorrowed
)

// freezeOrdinaryModeLocked records the ordinary mode under g.mu before any
// ordinary admission or arm. A gate already frozen to borrowed mode refuses
// every ordinary path.
func (g *originGate) freezeOrdinaryModeLocked() error {
	switch g.mode {
	case gateModeBorrowed:
		return errors.New("ordinary origin path refused: gate mode is borrowed")
	case gateModeFree:
		g.mode = gateModeOrdinary
	}
	return nil
}

// borrowedModeSnapshot returns the frozen mode and the immutable check hook
// without holding g.mu across any I/O.
func (g *originGate) borrowedModeSnapshot() (originGateMode, func(context.Context, *originGateRegistration) error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.mode, g.borrowedCheck
}

// originGateInspectionSeam is a deterministic test-only seam for the watch
// ownership checks. Nil fields use the real /proc facts.
type originGateInspectionSeam struct {
	processStart func(pid int) (uint64, error)
	socketOwners func(inode string) ([]int, error)
}

func (g *originGate) processStartID(pid int) (uint64, error) {
	if g.seam != nil && g.seam.processStart != nil {
		return g.seam.processStart(pid)
	}
	return hostProcessStartID(pid)
}

func (g *originGate) socketInodeOwners(inode string) ([]int, error) {
	if g.seam != nil && g.seam.socketOwners != nil {
		return g.seam.socketOwners(inode)
	}
	return hostSocketInodeOwners(inode)
}

func newOriginGate(t *testing.T, ctx context.Context, fx *originGateFixture) *originGate {
	t.Helper()
	endpoint, err := recovery.DrillOpenOriginEndpoint()
	if err != nil {
		t.Fatalf("adopt the factory origin endpoint capability: %v", err)
	}
	ln := endpoint.Listener()
	addr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("factory origin endpoint has unexpected address type")
	}
	observer, err := pgx.Connect(ctx, fx.dsn)
	if err != nil {
		t.Fatalf("open protected observer for origin gate: %v", err)
	}
	observerPID, observerBackendAt, err := backendIdentity(ctx, fx.admin, observer)
	if err != nil {
		t.Fatalf("register protected observer backend identity: %v", err)
	}
	observerOSStart, err := processStartIdentity(ctx, fx.containerID, observerPID)
	if err != nil {
		t.Fatalf("register protected observer OS process identity: %v", err)
	}
	control, err := pgx.Connect(ctx, fx.dsn)
	if err != nil {
		t.Fatalf("open independent protected control session for origin gate: %v", err)
	}
	controlPID, _, err := backendIdentity(ctx, fx.admin, control)
	if err != nil {
		t.Fatalf("register independent protected control session identity: %v", err)
	}
	g := &originGate{
		fx: fx, ln: ln, endpoint: endpoint, gateIP: addr.IP.String(), gatePort: addr.Port,
		serverAddr: fx.serverAddr,
		observer:   observer, observerPID: observerPID, observerBackendAt: observerBackendAt, observerOSStart: observerOSStart,
		control: control, controlPID: controlPID,
		drainPending: true, drainTimeout: 20 * time.Second, publishDone: make(chan struct{}),
		issued:  make(map[[32]byte]*originGateCapState),
		latchCh: make(chan struct{}),
	}
	t.Cleanup(func() {
		_ = ln.Close()
		if session := g.CurrentSession(); session != nil {
			session.closeBoth()
		}
		// Serialize with in-flight observer/control queries before closing the
		// pgx connections: a concurrent Close with an active query is a pgx
		// internal panic, and the latch drain is bounded so this wait is short.
		observerCtx, observerCancel := context.WithTimeout(context.Background(), 3*time.Second)
		g.observerMu.Lock()
		_ = observer.Close(observerCtx)
		g.observerMu.Unlock()
		observerCancel()
		controlCtx, controlCancel := context.WithTimeout(context.Background(), 3*time.Second)
		g.controlMu.Lock()
		_ = control.Close(controlCtx)
		g.controlMu.Unlock()
		controlCancel()
	})
	return g
}

func (g *originGate) Endpoint() string { return g.endpoint.Addr() }

// originGateArmedRestore is the gate-private arm record binding one fresh
// supervision handle to this gate's exact endpoint capability and sealed
// native tools before invocation.
type originGateArmedRestore struct {
	handle recovery.DrillTargetProcessObservation
	tools  recovery.DrillNativePGTools
	opts   recovery.DrillPGRestoreArmOptions
}

// ArmObserved arms exactly one fresh handle against this gate's factory origin
// endpoint and sealed native tool set. The bridge performs the authoritative
// validation (handle freshness, endpoint listener identity, sealed tool
// provenance, single-TCP-endpoint DSN equality with the armed listener); the
// gate stores the arm record so admission can require that exact handle and
// endpoint. No caller DSN/address/scalar is trusted as arm authority here.
func (g *originGate) ArmObserved(handle recovery.DrillTargetProcessObservation, tools recovery.DrillNativePGTools, opts recovery.DrillPGRestoreArmOptions) error {
	if !g.endpoint.Valid() {
		return errors.New("arm requires the gate's factory origin endpoint")
	}
	if !handle.Valid() || !tools.Valid() {
		return errors.New("arm requires a live supervision handle and sealed native tools")
	}
	if _, bound := handle.BoundOperation(); bound {
		return errors.New("arm requires a fresh handle (already bound)")
	}
	g.mu.Lock()
	if err := g.freezeOrdinaryModeLocked(); err != nil {
		g.mu.Unlock()
		return err
	}
	if g.armedRestore != nil {
		g.mu.Unlock()
		return errors.New("arm refused: this gate already has an armed restore")
	}
	g.mu.Unlock()
	if err := recovery.DrillArmObservedPGRestore(handle, g.endpoint, tools, opts); err != nil {
		return err
	}
	g.mu.Lock()
	g.armedRestore = &originGateArmedRestore{handle: handle, tools: tools, opts: opts}
	g.mu.Unlock()
	return nil
}

// issueCap is unexported and called only by the fixture launcher immediately
// after the real client Cmd is spawned and its OS start identity read.
func (g *originGate) issueCap(cap *originGateCap) {
	g.mu.Lock()
	g.issued[cap.token] = &originGateCapState{pid: cap.pid, start: cap.start}
	g.mu.Unlock()
}

func (g *originGate) HoldRegistration() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.registrationBarrier == nil {
		g.registrationBarrier = make(chan struct{})
	}
}

func (g *originGate) ReleaseRegistration() {
	g.mu.Lock()
	barrier := g.registrationBarrier
	g.mu.Unlock()
	if barrier != nil {
		g.barrierOnce.Do(func() { close(barrier) })
	}
}

func (g *originGate) Latched() (bool, string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.latched, g.latchReason
}

func (g *originGate) isLatched() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.latched
}

func (g *originGate) CurrentSession() *originGateSession {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.active
}

func (g *originGate) latch(reason string) {
	g.mu.Lock()
	if g.latched {
		g.mu.Unlock()
		return
	}
	// OG02: latch admission FIRST. No successor can be admitted from this
	// point, even while the protected backend is still draining.
	g.latched = true
	g.latchReason = reason
	close(g.latchCh)
	session := g.active
	g.mu.Unlock()
	_ = g.ln.Close()
	if session == nil {
		g.finishDrain(true, "latch with no admitted session to drain")
		return
	}
	session.closeBoth()
	go g.beginDrain(session, "latch: "+reason)
}

// originGateDrainState is the public-safe authoritative-drain verdict.
type originGateDrainState struct {
	Pending  bool
	Verified bool
	Unknown  bool
	Report   string
}

func (g *originGate) DrainState() originGateDrainState {
	g.drainMu.Lock()
	defer g.drainMu.Unlock()
	return originGateDrainState{Pending: g.drainPending, Verified: g.drainVerified, Unknown: g.drainUnknown, Report: g.drainReport}
}

type originGateRetirement struct {
	session  *originGateSession
	done     chan struct{}
	doneOnce sync.Once
}

func (g *originGate) retireSession(session *originGateSession) {
	g.mu.Lock()
	if g.active == session {
		g.active = nil
	}
	g.mu.Unlock()
}

// endSession retires only sessions that never captured a protected backend
// identity. A registered session is retired solely through the authoritative
// drain verdict, so frontend EOF can never clear the active slot early.
func (g *originGate) endSession(session *originGateSession) {
	if session.BackendPID() != 0 {
		return
	}
	g.retireSession(session)
}

// sessionClosed is the normal end-of-relay path. frontend EOF/Terminate is
// merely a retirement request: the single canonical path below requires the
// genuine owner sole-wait receipt AND an authoritative backend drain before
// anything is retired. No candidate/sentinel can retire a session.
func (g *originGate) sessionClosed(session *originGateSession) {
	if g.isLatched() {
		return
	}
	session.retireOnce.Do(func() { go g.retireOrDrain(session, "frontend closed") })
}

// originGateOwnerReceipt is the genuine owner-terminal verdict.
type originGateOwnerReceipt struct {
	Completed bool
	Clean     bool
	Pending   bool
	Detail    string
}

// ownerReceipt returns the genuine owner terminal facts:
//   - Completed: the sole cmd.Wait actually returned (supervised bridge
//     WaitTerminal or the fixture launcher's captured Cmd.Wait receipt);
//   - Clean: completed with zero exit AND, for supervised sessions, a bounded
//     runner disposition of success (an ambiguous bounded return is never
//     success even if a late wait completes);
//   - Pending: the wait or its disposition has not produced a verdict yet.
func (g *originGate) ownerReceipt(session *originGateSession) originGateOwnerReceipt {
	if session.supervisor != nil {
		terminal := session.supervisor.WaitTerminal()
		if !terminal.Terminal {
			return originGateOwnerReceipt{Pending: true, Detail: "supervised sole wait has not completed"}
		}
		if terminal.WaitExitCode != 0 {
			return originGateOwnerReceipt{Completed: true, Detail: fmt.Sprintf("supervised owner exited with code %d", terminal.WaitExitCode)}
		}
		disposition := session.supervisor.RunnerDisposition()
		if !disposition.Returned {
			return originGateOwnerReceipt{Completed: true, Pending: true, Detail: "bounded runner disposition has not returned"}
		}
		if disposition.Result.Outcome != recovery.PGCommandSucceeded {
			return originGateOwnerReceipt{Completed: true, Detail: fmt.Sprintf("bounded runner disposition %q does not authorize clean retirement", disposition.Result.Outcome)}
		}
		return originGateOwnerReceipt{Completed: true, Clean: true}
	}
	if session.ownerClient == nil {
		return originGateOwnerReceipt{Pending: true, Detail: "fixture-owned client sole-wait receipt is unavailable"}
	}
	completed, waitErr := session.ownerClient.waitReceipt()
	if !completed {
		return originGateOwnerReceipt{Pending: true, Detail: "fixture-owned client sole wait has not completed"}
	}
	if waitErr != nil {
		return originGateOwnerReceipt{Completed: true, Detail: fmt.Sprintf("fixture-owned client exited with error: %v", waitErr)}
	}
	return originGateOwnerReceipt{Completed: true, Clean: true}
}

// retireOrDrain is the single canonical clean-retirement path: genuine sole
// wait success (including a success runner disposition) plus authoritative
// backend drain. Anything else latches; a pending wait keeps the session
// active and refuses successors.
func (g *originGate) retireOrDrain(session *originGateSession, cause string) {
	deadline := time.Now().Add(15 * time.Second)
	last := "no verdict yet"
	for {
		receipt := g.ownerReceipt(session)
		if receipt.Completed && !receipt.Clean && !receipt.Pending {
			g.latch("owned origin terminal loss during " + cause + ": " + receipt.Detail)
			return
		}
		if receipt.Completed && receipt.Clean {
			registration, hasRegistration := session.Registration()
			pid := session.BackendPID()
			osStart, inode := "", ""
			var registeredStart time.Time
			if hasRegistration {
				osStart, inode, registeredStart = registration.OSStart, registration.SocketInode, registration.BackendStart
			}
			drained, err := g.authoritativeBackendGone(pid, osStart, inode, registeredStart)
			if err == nil && drained {
				// P2: record the verified drain, then allow the deterministic
				// publication barrier to run before the atomic clean verdict.
				g.finishDrain(true, fmt.Sprintf("verified drain before clean publication for %s", cause))
				if barrier := g.retirementPublishBarrier(); barrier != nil {
					<-barrier
				}
				clean := g.finishCleanRetirement(session, fmt.Sprintf("clean %s: sole wait success and backend PID %d, OS identity and socket authoritatively drained", cause, pid))
				g.signalPublishDone()
				if !clean {
					// A latch won during the barrier: no clean evidence was
					// published and the latched loss-drain path owns the verdict.
					return
				}
				return
			}
			if err != nil {
				last = err.Error()
			} else {
				last = fmt.Sprintf("backend PID %d still present after sole wait", pid)
			}
		} else {
			last = receipt.Detail
		}
		if time.Now().After(deadline) {
			g.latch(fmt.Sprintf("clean %s could not be authoritatively verified: %s", cause, last))
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// finishCleanRetirement is the only clean-retirement verdict, published
// atomically: the gate must still be unlatched and must still own exactly this
// session, otherwise no clean evidence is published and the caller routes to
// the latched loss-drain path. It is distinct from the latch-drain verdict: a
// latched gate never reopens successors.
func (g *originGate) finishCleanRetirement(session *originGateSession, report string) bool {
	g.mu.Lock()
	if g.latched || g.active != session {
		g.mu.Unlock()
		return false
	}
	g.cleanRetired = true
	g.cleanReport = report
	g.active = nil
	g.mu.Unlock()
	g.finishDrain(true, report)
	return true
}

func (g *originGate) CleanRetired() (bool, string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.cleanRetired, g.cleanReport
}

// retirementPublishBarrier is the deterministic test-only seam between the
// authoritative drain observation and clean-verdict publication.
func (g *originGate) retirementPublishBarrier() chan struct{} {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.publishBarrier
}

// holdCensusBarrier installs the deterministic test-only census pause under
// g.mu and returns the barrier plus an "entered" ack. The ack is closed by the
// actual watcher only when it reaches the real census wait point, so a test can
// prove the watcher is paused before triggering an orphan transfer. There is
// no fake census invocation.
func (g *originGate) holdCensusBarrier() (chan struct{}, chan struct{}) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.censusBarrier == nil {
		g.censusBarrier = make(chan struct{})
		g.censusEntered = make(chan struct{})
	}
	return g.censusBarrier, g.censusEntered
}

// waitCensusBarrier blocks the watcher at the census wait point, signalling the
// entered ack once. No mutex is held while waiting, and session cancellation or
// a latch always wins over the barrier.
func (g *originGate) waitCensusBarrier(s *originGateSession) {
	g.mu.Lock()
	barrier, entered := g.censusBarrier, g.censusEntered
	g.mu.Unlock()
	if barrier == nil {
		return
	}
	if entered != nil {
		g.censusEnteredOnce.Do(func() { close(entered) })
	}
	select {
	case <-barrier:
	case <-s.done:
	case <-s.g.latchCh:
	case <-s.ctx.Done():
	}
}

// signalPublishDone closes once after a clean-publication attempt completes.
func (g *originGate) signalPublishDone() {
	g.publishOnce.Do(func() { close(g.publishDone) })
}

// retirementPublishDone is the completion signal of the clean-publication
// attempt, so tests can assert only after finishCleanRetirement actually ran.
func (g *originGate) retirementPublishDone() chan struct{} {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.publishDone
}

// beginDrain terminates the exact registered backend through the independent
// protected control session (different from the observer, which may itself be
// the failed component) and then proves an authoritative drain: SQL session
// absent, OS PID/start gone, socket inode gone. Query errors, permission
// failures and timeouts are never counted as gone; the session then stays
// retained and every successor stays refused.
func (g *originGate) beginDrain(session *originGateSession, cause string) {
	registration, hasRegistration := session.Registration()
	pid := session.BackendPID()
	osStart, inode := "", ""
	var registeredStart time.Time
	if hasRegistration {
		osStart, inode, registeredStart = registration.OSStart, registration.SocketInode, registration.BackendStart
	}
	if pid <= 0 {
		g.finishDrain(true, "no protected server backend identity was ever captured")
		g.retireSession(session)
		return
	}
	terminateAttempted := false
	lastErr := error(nil)
	drainBound := g.drainTimeout
	if drainBound <= 0 {
		drainBound = 20 * time.Second
	}
	deadline := time.Now().Add(drainBound)
	for time.Now().Before(deadline) {
		drained, err := g.authoritativeBackendGone(pid, osStart, inode, registeredStart)
		if err != nil {
			lastErr = err
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if !drained {
			if !terminateAttempted {
				if err := g.terminateExactBackend(pid, registeredStart); err != nil {
					lastErr = err
					time.Sleep(100 * time.Millisecond)
					continue
				}
				terminateAttempted = true
			}
			time.Sleep(50 * time.Millisecond)
			continue
		}
		g.finishDrain(true, fmt.Sprintf("%s: backend PID %d SQL session, OS identity and socket authoritatively drained", cause, pid))
		g.retireSession(session)
		return
	}
	g.finishDrain(false, fmt.Sprintf("%s: backend PID %d drain unverified (final error: %v)", cause, pid, lastErr))
}

// terminateExactBackend re-checks the exact registered identity through the
// control session before issuing the real pg_terminate_backend. A changed
// backend_start refuses the terminate (no replacement owner is ever touched).
func (g *originGate) terminateExactBackend(pid int, registeredStart time.Time) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	g.controlMu.Lock()
	defer g.controlMu.Unlock()
	var backendStart time.Time
	var backendType string
	err := g.control.QueryRow(ctx, `SELECT backend_start, backend_type FROM pg_stat_activity WHERE pid=$1`, pid).Scan(&backendStart, &backendType)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("control identity check failed: %w", err)
	}
	if backendType != "client backend" {
		return fmt.Errorf("control identity check: PID %d is not a client backend", pid)
	}
	if !registeredStart.IsZero() && !backendStart.Equal(registeredStart) {
		return fmt.Errorf("control identity check: PID %d backend_start changed; refusing terminate", pid)
	}
	var terminated bool
	if err := g.control.QueryRow(ctx, `SELECT pg_terminate_backend($1)`, pid).Scan(&terminated); err != nil {
		return fmt.Errorf("pg_terminate_backend failed: %w", err)
	}
	if !terminated {
		return fmt.Errorf("pg_terminate_backend reported no session for PID %d", pid)
	}
	return nil
}

func (g *originGate) finishDrain(verified bool, report string) {
	g.drainMu.Lock()
	g.drainPending = false
	g.drainVerified = g.drainVerified || verified
	g.drainUnknown = g.drainUnknown || !verified
	g.drainReport = report
	g.drainMu.Unlock()
}

// authoritativeBackendGone requires all three facts before declaring the exact
// registered backend drained: the SQL session row is absent (or backend_start
// changed), the container PID/start identity is GONE or REUSED, and the
// registered socket inode no longer appears in the protected census. Any
// query/probe/census error is returned and never treated as gone.
func (g *originGate) authoritativeBackendGone(pid int, osStart, inode string, registeredStart time.Time) (bool, error) {
	if pid <= 0 {
		return true, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	backendStart, backendType, err := g.controlBackendRow(ctx, pid)
	rowGone := false
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		rowGone = true
	case err != nil:
		return false, fmt.Errorf("control drain query failed: %w", err)
	default:
		rowGone = backendType != "client backend" || (!registeredStart.IsZero() && !backendStart.Equal(registeredStart))
	}
	if !rowGone {
		return false, nil
	}
	probeCtx, probeCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer probeCancel()
	if osStart != "" {
		gone, _, err := containerProcessGone(probeCtx, g.fx.containerID, pid, osStart)
		if err != nil {
			return false, fmt.Errorf("OS PID/start probe failed: %w", err)
		}
		if !gone {
			return false, nil
		}
	} else {
		gone, err := containerPIDEverGone(probeCtx, g.fx.containerID, pid)
		if err != nil {
			return false, fmt.Errorf("OS PID probe failed: %w", err)
		}
		if !gone {
			return false, nil
		}
	}
	_, sockets, err := snapshotPostmasterChildren(probeCtx, g.fx.containerID, g.fx.postmasterPID, g.fx.postmasterStr)
	if err != nil {
		return false, fmt.Errorf("protected census failed: %w", err)
	}
	if inode != "" {
		for _, socket := range sockets {
			if socket.Inode == inode && socket.Local != "fd" && socket.Local != "fdcount" {
				return false, nil
			}
		}
	} else {
		for _, socket := range sockets {
			if socket.PID == pid && socket.Local != "fd" && socket.Local != "fdcount" {
				return false, nil
			}
		}
	}
	return true, nil
}

func (g *originGate) controlBackendRow(ctx context.Context, pid int) (time.Time, string, error) {
	g.controlMu.Lock()
	defer g.controlMu.Unlock()
	if g.control == nil {
		return time.Time{}, "", errors.New("independent protected control session is unavailable")
	}
	var backendStart time.Time
	var backendType string
	err := g.control.QueryRow(ctx, `SELECT backend_start, backend_type FROM pg_stat_activity WHERE pid=$1`, pid).Scan(&backendStart, &backendType)
	return backendStart, backendType, err
}

// containerPIDEverGone is the pre-registration drain probe: it only asserts
// that the container PID no longer exists. A probe error is never gone.
func containerPIDEverGone(ctx context.Context, containerID string, pid int) (bool, error) {
	cmd := fmt.Sprintf(`if [ -e "/proc/%d/stat" ]; then exit 1; fi`, pid)
	_, err := dockerExec(ctx, containerID, "sh", "-ec", cmd)
	if err == nil {
		return true, nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
		return false, nil
	}
	return false, err
}

// Admit validates the private capability before touching the network, accepts
// exactly one connection, binds it to the cap origin's real OS process
// identity and fd socket inode, and only then dials the protected server.
func (g *originGate) Admit(ctx context.Context, cap *originGateCap) (*originGateSession, error) {
	if cap == nil {
		return nil, errors.New("origin gate admission requires a fixture-launcher-issued origin capability")
	}
	g.mu.Lock()
	if err := g.freezeOrdinaryModeLocked(); err != nil {
		g.mu.Unlock()
		return nil, err
	}
	if g.latched {
		reason := g.latchReason
		g.mu.Unlock()
		return nil, fmt.Errorf("origin gate is latched forever: %s", reason)
	}
	if g.active != nil {
		g.mu.Unlock()
		return nil, errors.New("origin gate already has an active admitted session")
	}
	if g.admitting {
		g.mu.Unlock()
		return nil, errors.New("origin gate admission is already in progress")
	}
	state, issued := g.issued[cap.token]
	if !issued {
		g.mu.Unlock()
		return nil, errors.New("origin capability was not issued by the fixture launcher")
	}
	if state.consumed {
		g.mu.Unlock()
		return nil, errors.New("origin capability already consumed (one-use connection binding forbids replay)")
	}
	if state.pid != cap.pid || state.start != cap.start {
		g.mu.Unlock()
		return nil, errors.New("origin capability identity fields do not match the issued origin process")
	}
	state.consumed = true
	g.admitting = true
	g.mu.Unlock()
	defer func() {
		g.mu.Lock()
		g.admitting = false
		g.mu.Unlock()
	}()

	conn, peer, err := g.acceptAdmitted(ctx)
	if err != nil {
		return nil, err
	}
	inode, err := g.verifyAcceptedOrigin(cap, peer)
	if err != nil {
		_ = conn.Close()
		g.latch("accepted frontend origin rejected: " + err.Error())
		return nil, fmt.Errorf("accepted frontend origin rejected: %w", err)
	}
	return g.startSession(ctx, conn, peer, inode, cap.pid, cap.start, cap.ownerClient, nil, 0, 0, cap.expectedRole, cap.expectedDatabase)
}

// AdmitObserved admits one connection whose origin is the real supervised
// child recorded by the pinned owner. An inert/zero/unbound handle is an input
// refusal; a real handle is claimed exactly once and the accepted peer tuple
// plus startup database/role must match the immutable bound operation.
func (g *originGate) AdmitObserved(ctx context.Context, handle recovery.DrillTargetProcessObservation) (*originGateSession, error) {
	if !g.endpoint.Valid() {
		return nil, errors.New("observed origin admission requires the gate's factory endpoint capability")
	}
	if !handle.Valid() {
		return nil, errors.New("observed origin admission requires a live supervision handle")
	}
	g.mu.Lock()
	armed := g.armedRestore
	g.mu.Unlock()
	if armed == nil || armed.handle != handle {
		return nil, errors.New("observed origin admission requires the handle armed to this gate's exact endpoint and sealed tools")
	}
	if _, bound := handle.BoundOperation(); !bound {
		return nil, errors.New("observed origin admission requires an operation identity bound from validated options")
	}
	g.mu.Lock()
	if err := g.freezeOrdinaryModeLocked(); err != nil {
		g.mu.Unlock()
		return nil, err
	}
	if g.latched {
		reason := g.latchReason
		g.mu.Unlock()
		return nil, fmt.Errorf("origin gate is latched forever: %s", reason)
	}
	if g.active != nil {
		g.mu.Unlock()
		return nil, errors.New("origin gate already has an active admitted session")
	}
	if g.admitting {
		g.mu.Unlock()
		return nil, errors.New("origin gate admission is already in progress")
	}
	g.admitting = true
	g.mu.Unlock()
	defer func() {
		g.mu.Lock()
		g.admitting = false
		g.mu.Unlock()
	}()

	if err := handle.AwaitStarted(ctx); err != nil {
		g.latch("owned origin did not start: " + err.Error())
		return nil, fmt.Errorf("owned origin did not start: %w", err)
	}
	claimed, err := handle.ClaimForEndpoint(g.endpoint)
	if err != nil {
		return nil, fmt.Errorf("endpoint-bound origin claim refused: %w", err)
	}
	if !claimed.MatchesEndpoint(g.endpoint) {
		g.latch("endpoint claim swap: claimed origin does not match the gate's exact endpoint capability")
		return nil, errors.New("claimed origin does not match the gate's exact endpoint capability")
	}
	bound, err := claimed.BoundOperation()
	if err != nil {
		return nil, err
	}
	identity := claimed.StartedIdentity()
	currentStartID, err := hostProcessStartID(identity.PID)
	if err != nil {
		g.latch(fmt.Sprintf("owned origin process %d identity unavailable before admission: %v", identity.PID, err))
		return nil, fmt.Errorf("owned origin process %d identity unavailable before admission: %w", identity.PID, err)
	}
	if currentStartID != identity.StartID {
		g.latch(fmt.Sprintf("owned origin process %d start identity changed before admission: observed=%d current=%d", identity.PID, identity.StartID, currentStartID))
		return nil, errors.New("owned origin start identity changed before admission")
	}
	conn, peer, err := g.acceptAdmitted(ctx)
	if err != nil {
		return nil, err
	}
	inode, err := g.verifyObservedOrigin(identity, peer)
	if err != nil {
		_ = conn.Close()
		g.latch("accepted frontend origin rejected: " + err.Error())
		return nil, fmt.Errorf("accepted frontend origin rejected: %w", err)
	}
	return g.startSession(ctx, conn, peer, inode, 0, "", nil, &claimed, identity.PID, identity.StartID, bound.Role, bound.Database)
}

func (g *originGate) SupervisorOriginCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.supervisorOrigins)
}

func (g *originGate) acceptAdmitted(ctx context.Context) (net.Conn, *net.TCPAddr, error) {
	acceptDeadline := time.Now().Add(30 * time.Second)
	if deadline, ok := ctx.Deadline(); ok && deadline.Before(acceptDeadline) {
		acceptDeadline = deadline
	}
	if listener, ok := g.ln.(*net.TCPListener); ok {
		_ = listener.SetDeadline(acceptDeadline)
	}
	conn, err := g.ln.Accept()
	if err != nil {
		return nil, nil, fmt.Errorf("no owned origin connection accepted before deadline: %w", err)
	}
	peer, ok := conn.RemoteAddr().(*net.TCPAddr)
	if !ok {
		_ = conn.Close()
		g.latch("accepted frontend source has no TCP four-tuple")
		return nil, nil, errors.New("accepted frontend source has no TCP four-tuple")
	}
	return conn, peer, nil
}

func (g *originGate) startSession(ctx context.Context, conn net.Conn, peer *net.TCPAddr, inode string,
	capPID int, capStart string, ownerClient *originGateClient, claimed *recovery.DrillClaimedOrigin,
	observedPID int, observedStartID uint64, expectedRole, expectedDatabase string) (*originGateSession, error) {
	server, err := net.DialTimeout("tcp", g.serverAddr, 10*time.Second)
	if err != nil {
		_ = conn.Close()
		g.latch("protected server dial failed: " + err.Error())
		return nil, fmt.Errorf("protected server dial failed: %w", err)
	}
	session := &originGateSession{
		g: g, client: conn, server: server,
		clientBuf: bufio.NewReader(conn), serverBuf: bufio.NewReader(server),
		peer: peer, originInode: inode,
		capPID: capPID, capStart: capStart, ownerClient: ownerClient,
		supervisor: claimed, observedPID: observedPID, observedStartID: observedStartID,
		expectedRole: expectedRole, expectedDatabase: expectedDatabase,
		ctx:  ctx,
		done: make(chan struct{}), releaseCh: make(chan struct{}), watchReady: make(chan struct{}),
	}
	g.mu.Lock()
	g.active = session
	g.mu.Unlock()
	go session.watchCancellation()
	go session.run()
	return session, nil
}

// verifyAcceptedOrigin binds the accepted peer four-tuple to the cap process:
// the process must be alive with the exact registered start identity and must
// own the socket inode whose local endpoint is the accepted peer tuple.
func (g *originGate) verifyAcceptedOrigin(cap *originGateCap, peer *net.TCPAddr) (string, error) {
	start, err := hostProcessStart(cap.pid)
	if err != nil {
		return "", fmt.Errorf("origin process %d identity unavailable: %w", cap.pid, err)
	}
	if start != cap.start {
		return "", fmt.Errorf("origin process %d start identity mismatch: cap=%s actual=%s", cap.pid, cap.start, start)
	}
	if peer.IP == nil || !peer.IP.IsLoopback() {
		return "", fmt.Errorf("accepted peer %s is not a loopback source owned by the cap process", peer)
	}
	inode, err := hostOwnedSocketInode(cap.pid, peer.IP, peer.Port, net.ParseIP(g.gateIP), g.gatePort)
	if err != nil {
		return "", fmt.Errorf("origin process %d does not own accepted peer tuple %s: %w", cap.pid, peer, err)
	}
	return inode, nil
}

// verifyObservedOrigin binds the accepted peer four-tuple to the real
// supervised child recorded by the pinned owner: live PID with the exact Linux
// start identity and its own fd socket inode. Role, application_name, idle
// state and SQL shape are never consulted here.
func (g *originGate) verifyObservedOrigin(identity recovery.DrillStartedIdentity, peer *net.TCPAddr) (string, error) {
	if identity.PID <= 0 || identity.StartID == 0 || identity.StartErr != nil {
		return "", errors.New("observed origin start identity is not usable")
	}
	currentStartID, err := hostProcessStartID(identity.PID)
	if err != nil {
		return "", fmt.Errorf("origin process %d identity unavailable: %w", identity.PID, err)
	}
	if currentStartID != identity.StartID {
		return "", fmt.Errorf("origin process %d start identity mismatch: observed=%d current=%d", identity.PID, identity.StartID, currentStartID)
	}
	if peer.IP == nil || !peer.IP.IsLoopback() {
		return "", fmt.Errorf("accepted peer %s is not a loopback source owned by the observed process", peer)
	}
	inode, err := hostOwnedSocketInode(identity.PID, peer.IP, peer.Port, net.ParseIP(g.gateIP), g.gatePort)
	if err != nil {
		return "", fmt.Errorf("observed process %d does not own accepted peer tuple %s: %w", identity.PID, peer, err)
	}
	return inode, nil
}

// registerBackend registers the real server backend behind BackendKeyData with
// the protected observer: pg_stat_activity backend_start/backend_type, the
// server OS process start identity, the direct postmaster child relation and
// the server-side socket inode whose peer is the exact client tuple the server
// reports. The observer's own identity must be stable.
func (g *originGate) registerBackend(ctx context.Context, pid int) (*originGateRegistration, error) {
	if pid <= 0 {
		return nil, errors.New("no real server BackendKeyData PID was captured")
	}
	g.observerMu.Lock()
	observerCtx, observerCancel := context.WithTimeout(context.Background(), 3*time.Second)
	var observerStart time.Time
	err := g.observer.QueryRow(observerCtx, `SELECT backend_start FROM pg_stat_activity WHERE pid=$1`, g.observerPID).Scan(&observerStart)
	observerCancel()
	if err != nil {
		g.observerMu.Unlock()
		return nil, fmt.Errorf("protected observer identity query failed: %w", err)
	}
	if !observerStart.Equal(g.observerBackendAt) {
		g.observerMu.Unlock()
		return nil, errors.New("protected observer backend identity changed")
	}
	var backendStart time.Time
	var backendType string
	var clientAddr string
	var clientPort *int
	backendCtx, backendCancel := context.WithTimeout(context.Background(), 3*time.Second)
	err = g.observer.QueryRow(backendCtx,
		`SELECT backend_start, backend_type, coalesce(host(client_addr),''), client_port FROM pg_stat_activity WHERE pid=$1`,
		pid).Scan(&backendStart, &backendType, &clientAddr, &clientPort)
	backendCancel()
	g.observerMu.Unlock()
	if err != nil {
		return nil, fmt.Errorf("protected observer cannot see backend PID %d: %w", pid, err)
	}
	if backendType != "client backend" {
		return nil, fmt.Errorf("server PID %d has backend_type %q, not an authenticated client backend", pid, backendType)
	}
	if clientPort == nil {
		return nil, fmt.Errorf("server PID %d has no server-side client_port tuple", pid)
	}
	osStart, err := processStartIdentity(ctx, g.fx.containerID, pid)
	if err != nil {
		return nil, fmt.Errorf("read server PID %d OS process start identity: %w", pid, err)
	}
	processes, sockets, err := snapshotPostmasterChildren(ctx, g.fx.containerID, g.fx.postmasterPID, g.fx.postmasterStr)
	if err != nil {
		return nil, fmt.Errorf("enumerate protected postmaster children: %w", err)
	}
	process, ok := processes[pid]
	if !ok {
		return nil, fmt.Errorf("server PID %d is not a direct child of the protected postmaster", pid)
	}
	if process.State == "Z" {
		return nil, fmt.Errorf("server PID %d already exited during registration", pid)
	}
	if process.Start != osStart {
		return nil, fmt.Errorf("server PID %d process start identity changed: census=%s proc=%s", pid, process.Start, osStart)
	}
	ip := net.ParseIP(clientAddr)
	if ip == nil {
		return nil, fmt.Errorf("server PID %d reported unparsable client_addr %q", pid, clientAddr)
	}
	wantRemote, err := procNetEndpoint(ip, *clientPort)
	if err != nil {
		return nil, fmt.Errorf("format server-side client tuple: %w", err)
	}
	var socket *authBoundarySocket
	for index := range sockets {
		candidate := sockets[index]
		if candidate.PID != pid || candidate.Local == "fd" || candidate.Local == "fdcount" || candidate.Inode == "" {
			continue
		}
		if strings.EqualFold(candidate.Remote, wantRemote) {
			socket = &candidate
			break
		}
	}
	if socket == nil {
		return nil, fmt.Errorf("server PID %d has no live socket inode whose peer is the reported client tuple %s", pid, wantRemote)
	}
	return &originGateRegistration{
		PID: pid, BackendStart: backendStart, OSStart: osStart,
		SocketInode: socket.Inode, SocketLocal: socket.Local, SocketRemote: socket.Remote,
		ClientAddr: clientAddr, ClientPort: *clientPort,
	}, nil
}

// checkBackendLiveness re-verifies the registered backend through the same
// protected observer. An observer failure, a missing backend row or a changed
// backend_start is a liveness loss.
func (g *originGate) checkBackendLiveness(ctx context.Context, reg *originGateRegistration) error {
	g.observerMu.Lock()
	defer g.observerMu.Unlock()
	var backendStart time.Time
	var backendType string
	if err := g.observer.QueryRow(ctx, `SELECT backend_start, backend_type FROM pg_stat_activity WHERE pid=$1`, reg.PID).Scan(&backendStart, &backendType); err != nil {
		return fmt.Errorf("protected observer query failed: %w", err)
	}
	if backendType != "client backend" {
		return fmt.Errorf("registered backend PID %d backend_type changed to %q", reg.PID, backendType)
	}
	if !backendStart.Equal(reg.BackendStart) {
		return fmt.Errorf("registered backend PID %d backend_start changed", reg.PID)
	}
	return nil
}

func (g *originGate) releaseSession(session *originGateSession) bool {
	if g.isLatched() {
		return false
	}
	// OG04: the frontend goroutine is the single ordered writer of executable
	// frames. Release only flips the transition and wakes it; the held frames
	// are flushed by that same writer in exact order, so a frame that arrives
	// during this transition cannot be lost or reordered.
	session.mu.Lock()
	session.released = true
	session.mu.Unlock()
	session.releaseOnce.Do(func() { close(session.releaseCh) })
	return true
}

// flushHeldFrames drains the held queue in order. Only the frontend goroutine
// calls this, so ordering and exactly-once forwarding are structural.
func (s *originGateSession) flushHeldFrames() error {
	for {
		s.mu.Lock()
		if len(s.buffered) == 0 {
			s.mu.Unlock()
			return nil
		}
		frame := s.buffered[0]
		s.buffered = s.buffered[1:]
		s.mu.Unlock()
		if err := s.writeServer(frame); err != nil {
			return err
		}
	}
}

// originGateSession is one admitted frontend connection bound to one launcher
// capability and one real protected server backend.
type originGateSession struct {
	g         *originGate
	client    net.Conn
	server    net.Conn
	clientBuf *bufio.Reader
	serverBuf *bufio.Reader
	peer      *net.TCPAddr

	originInode string
	capPID      int
	capStart    string

	// ownerClient is the launcher's retained process handle; its captured sole
	// cmd.Wait receipt is the only cap-path owner-terminal authority.
	ownerClient *originGateClient

	// Supervisor-observed origin (fix101 bridge): the private claimed handle,
	// the scalar start identity from the real pinned owner and the immutable
	// operation identity the startup message must match.
	supervisor       *recovery.DrillClaimedOrigin
	observedPID      int
	observedStartID  uint64
	expectedDatabase string
	expectedRole     string

	clientWriteMu sync.Mutex
	serverWriteMu sync.Mutex

	mu           sync.Mutex
	buffered     [][]byte
	released     bool
	backendPID   int
	serverAuthOK bool
	registration *originGateRegistration
	ended        bool
	clientDone   bool

	ctx         context.Context
	done        chan struct{}
	closeOnce   sync.Once
	releaseCh   chan struct{}
	releaseOnce sync.Once
	retireOnce  sync.Once
	watchReady  chan struct{}
	watchOnce   sync.Once
}

func (s *originGateSession) BackendPID() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.backendPID
}

func (s *originGateSession) WatcherReady() bool {
	select {
	case <-s.watchReady:
		return true
	default:
		return false
	}
}

func (s *originGateSession) markWatcherReady() {
	s.watchOnce.Do(func() { close(s.watchReady) })
}

func (s *originGateSession) BufferedFrames() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.buffered)
}

func (s *originGateSession) ServerAuthOK() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.serverAuthOK
}

func (s *originGateSession) Registration() (*originGateRegistration, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.registration == nil {
		return nil, false
	}
	copyOf := *s.registration
	return &copyOf, true
}

func (s *originGateSession) Report() originGateSessionReport {
	s.mu.Lock()
	defer s.mu.Unlock()
	report := originGateSessionReport{
		CapPID: s.capPID, CapStart: s.capStart,
		PeerIP: s.peer.IP.String(), PeerPort: s.peer.Port,
		OriginInode: s.originInode, BackendPID: s.backendPID, ServerAuthOK: s.serverAuthOK,
		Supervisor:  s.supervisor != nil,
		ObservedPID: s.observedPID, ObservedStartID: s.observedStartID,
		BoundRole: s.expectedRole, BoundDatabase: s.expectedDatabase,
	}
	return report
}

func (s *originGateSession) run() {
	if err := s.negotiatePreStartup(); err != nil {
		if !errors.Is(err, errOriginGateEnded) {
			s.g.latch(fmt.Sprintf("pre-startup protocol failure: %v", err))
		}
		s.closeBoth()
		s.g.endSession(s)
		return
	}
	go s.backendRelay()
	s.frontendLoop()
	s.closeBoth()
	s.g.sessionClosed(s)
}

// negotiatePreStartup forwards the untyped pre-startup messages strictly:
// CancelRequest is relayed and ends the session; SSLRequest/GSSENCRequest are
// relayed and exactly one real server reply byte is passed back. A server that
// answers 'S'/'G' cannot be upgraded by this primitive, so the gate closes
// instead of falling back to plaintext. Nothing is generated by the gate.
func (s *originGateSession) negotiatePreStartup() error {
	for {
		var header [4]byte
		if _, err := io.ReadFull(s.clientBuf, header[:]); err != nil {
			return errOriginGateEnded
		}
		length := binary.BigEndian.Uint32(header[:])
		if length == 8 {
			var codeBytes [4]byte
			if _, err := io.ReadFull(s.clientBuf, codeBytes[:]); err != nil {
				return errOriginGateEnded
			}
			code := binary.BigEndian.Uint32(codeBytes[:])
			raw := append(append([]byte(nil), header[:]...), codeBytes[:]...)
			switch code {
			case 80877102: // CancelRequest
				_ = s.writeServer(raw)
				return errOriginGateEnded
			case 80877103, 80877104: // SSLRequest, GSSENCRequest
				if err := s.writeServer(raw); err != nil {
					return err
				}
				_ = s.server.SetReadDeadline(time.Now().Add(15 * time.Second))
				reply := make([]byte, 1)
				if _, err := io.ReadFull(s.serverBuf, reply); err != nil {
					return fmt.Errorf("read real negotiation reply: %w", err)
				}
				_ = s.server.SetReadDeadline(time.Time{})
				if err := s.writeClient(reply); err != nil {
					return err
				}
				switch reply[0] {
				case 'N':
					continue
				case 'S', 'G':
					// A real upgrade the primitive does not implement: fail
					// closed, never fall back to plaintext frames.
					return errOriginGateEnded
				default:
					return fmt.Errorf("%w: unexpected server negotiation byte %q", errOriginGateProtocolViolation, reply[0])
				}
			default:
				return fmt.Errorf("%w: unknown 8-byte pre-startup code %d", errOriginGateProtocolViolation, code)
			}
		}
		if length < 9 || length > originGateMessageMax {
			return fmt.Errorf("%w: invalid startup length %d", errOriginGateProtocolViolation, length)
		}
		body := make([]byte, int(length)-4)
		if _, err := io.ReadFull(s.clientBuf, body); err != nil {
			return errOriginGateEnded
		}
		if err := s.verifyStartupIdentity(body); err != nil {
			return err
		}
		raw := append(append([]byte(nil), header[:]...), body...)
		return s.writeServer(raw)
	}
}

// verifyStartupIdentity compares the real startup user/database against the
// immutable operation identity bound by the observed-run invocation. It is a
// startup-identity check, not a query whitelist: everything after a valid
// startup is classified only by protocol frame type.
func (s *originGateSession) verifyStartupIdentity(body []byte) error {
	if s.expectedRole == "" && s.expectedDatabase == "" {
		return nil
	}
	user, database, err := startupMessageIdentity(body)
	if err != nil {
		return fmt.Errorf("%w: startup identity unreadable: %v", errOriginGateProtocolViolation, err)
	}
	if user != s.expectedRole || database != s.expectedDatabase {
		return fmt.Errorf("%w: startup identity user=%q database=%q does not match the bound operation identity role=%q database=%q",
			errOriginGateProtocolViolation, user, database, s.expectedRole, s.expectedDatabase)
	}
	return nil
}

// startupMessageIdentity extracts user/database from a StartupMessage body.
func startupMessageIdentity(body []byte) (string, string, error) {
	if len(body) < 5 {
		return "", "", errors.New("startup message is too short")
	}
	fields := strings.Split(string(body[4:]), "\x00")
	user, database := "", ""
	for index := 0; index+1 < len(fields); index += 2 {
		key, value := fields[index], fields[index+1]
		if key == "" {
			break
		}
		switch key {
		case "user":
			user = value
		case "database":
			database = value
		}
	}
	if user == "" {
		return "", "", errors.New("startup message has no user parameter")
	}
	return user, database, nil
}

type originGateFrontendFrame struct {
	typ byte
	raw []byte
}

func (s *originGateSession) frontendLoop() {
	for {
		frame, err := readFrontendFrame(s.clientBuf)
		if err != nil {
			// A client that goes away without Terminate simply ends the
			// session; it is not a protected liveness loss.
			s.mu.Lock()
			s.clientDone = true
			s.mu.Unlock()
			return
		}
		switch frame.typ {
		case 'p':
			if err := s.writeServer(frame.raw); err != nil {
				s.g.latch(fmt.Sprintf("forwarding bootstrap authentication frame for backend PID %d failed: %v", s.backendPID, err))
				return
			}
		case 'X':
			s.mu.Lock()
			s.clientDone = true
			s.mu.Unlock()
			_ = s.writeServer(frame.raw)
			return
		default:
			s.mu.Lock()
			if s.released {
				s.mu.Unlock()
				if err := s.writeServer(frame.raw); err != nil {
					s.g.latch(fmt.Sprintf("forwarding admitted frontend frame for backend PID %d failed: %v", s.backendPID, err))
					return
				}
				continue
			}
			s.buffered = append(s.buffered, frame.raw)
			s.mu.Unlock()
			select {
			case <-s.releaseCh:
				if s.g.isLatched() {
					return
				}
				if err := s.flushHeldFrames(); err != nil {
					s.g.latch(fmt.Sprintf("flushing held frontend frames for backend PID %d failed: %v", s.backendPID, err))
					return
				}
			case <-s.done:
				return
			case <-s.g.latchCh:
				return
			}
		}
	}
}

func readBackendFrame(reader *bufio.Reader) (byte, []byte, []byte, error) {
	typ, err := reader.ReadByte()
	if err != nil {
		return 0, nil, nil, err
	}
	var header [4]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return 0, nil, nil, err
	}
	length := binary.BigEndian.Uint32(header[:])
	if length < 4 || length > originGateMessageMax {
		return 0, nil, nil, fmt.Errorf("invalid backend frame length %d", length)
	}
	body := make([]byte, int(length)-4)
	if _, err := io.ReadFull(reader, body); err != nil {
		return 0, nil, nil, err
	}
	raw := make([]byte, 1+len(header)+len(body))
	raw[0] = typ
	copy(raw[1:5], header[:])
	copy(raw[5:], body)
	return typ, body, raw, nil
}

func readFrontendFrame(reader *bufio.Reader) (originGateFrontendFrame, error) {
	typ, err := reader.ReadByte()
	if err != nil {
		return originGateFrontendFrame{}, err
	}
	var header [4]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return originGateFrontendFrame{}, err
	}
	length := binary.BigEndian.Uint32(header[:])
	if length < 4 || length > originGateMessageMax {
		return originGateFrontendFrame{}, fmt.Errorf("invalid frontend frame length %d", length)
	}
	body := make([]byte, int(length)-4)
	if _, err := io.ReadFull(reader, body); err != nil {
		return originGateFrontendFrame{}, err
	}
	raw := make([]byte, 1+len(header)+len(body))
	raw[0] = typ
	copy(raw[1:5], header[:])
	copy(raw[5:], body)
	return originGateFrontendFrame{typ: typ, raw: raw}, nil
}

func (s *originGateSession) backendRelay() {
	for {
		typ, body, raw, err := readBackendFrame(s.serverBuf)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			if s.clientFinished() {
				return
			}
			if !s.g.isLatched() {
				s.g.latch(fmt.Sprintf("protected server connection to backend PID %d lost: %v", s.backendPID, err))
			}
			return
		}
		fatalSeverity := ""
		if typ == 'E' {
			if severity := serverErrorFields(body)['S']; severity == "FATAL" || severity == "PANIC" {
				fatalSeverity = severity
			}
		}
		if typ == 'K' && len(body) >= 8 {
			s.noteBackendKeyData(int(binary.BigEndian.Uint32(body[:4])))
		}
		if typ == 'R' && len(body) >= 4 && binary.BigEndian.Uint32(body[:4]) == 0 {
			s.mu.Lock()
			s.serverAuthOK = true
			s.mu.Unlock()
		}
		if err := s.writeClient(raw); err != nil {
			if !errors.Is(err, net.ErrClosed) && !s.g.isLatched() {
				switch {
				case fatalSeverity != "" && s.isRegistered():
					s.g.latch(fmt.Sprintf("relaying terminating %s for registered backend PID %d failed: %v", fatalSeverity, s.backendPID, err))
				case fatalSeverity == "" && !s.clientFinished():
					s.g.latch(fmt.Sprintf("relaying protected server frame to frontend failed: %v", err))
				}
			}
			return
		}
		if fatalSeverity != "" {
			// A terminating server error is a protected liveness loss only for
			// an already registered backend; during bootstrap the server also
			// uses FATAL for ordinary credential rejection, which must not
			// latch an origin gate that never admitted a channel.
			if s.isRegistered() {
				s.g.latch(fmt.Sprintf("protected server sent a terminating %s error for backend PID %d", fatalSeverity, s.backendPID))
			}
			return
		}
	}
}

func (s *originGateSession) isRegistered() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.registration != nil
}

func (s *originGateSession) clientFinished() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.clientDone
}

// expectedEnd reports whether the frontend side already ended (X/EOF) or the
// supervised owner reached its genuine sole-wait terminal. Backend
// disappearance is only expected in that state; otherwise it is a loss.
func (s *originGateSession) expectedEnd() bool {
	if s.clientFinished() {
		return true
	}
	if s.supervisor != nil {
		return s.supervisor.WaitTerminal().Terminal
	}
	return false
}

func (s *originGateSession) noteBackendKeyData(pid int) {
	s.mu.Lock()
	if s.backendPID != 0 {
		s.mu.Unlock()
		return
	}
	s.backendPID = pid
	s.mu.Unlock()
	go s.register(pid)
}

func (s *originGateSession) register(pid int) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	barrier := s.registrationBarrier()
	if barrier != nil {
		select {
		case <-barrier:
		case <-s.ctx.Done():
			s.g.latch("admitted session context canceled before registration release")
			return
		case <-s.g.latchCh:
			return
		case <-s.done:
			return
		}
	}
	if s.ctx.Err() != nil {
		s.g.latch("admitted session context canceled before registration")
		return
	}
	registration, err := s.g.registerBackend(ctx, pid)
	if err != nil {
		if s.expectedEnd() || s.g.isLatched() {
			// The session already ended or lost its gate while registration was
			// in flight: the retirement/drain path owns the verdict.
			s.g.sessionClosed(s)
			return
		}
		s.g.latch(fmt.Sprintf("protected origin registration failed for backend PID %d: %v", pid, err))
		return
	}
	s.mu.Lock()
	s.registration = registration
	s.mu.Unlock()
	if s.supervisor != nil {
		s.g.mu.Lock()
		s.g.supervisorOrigins = append(s.g.supervisorOrigins, *s.supervisor)
		s.g.mu.Unlock()
	}
	// OG03: publish the registration and establish one full successful watcher
	// pass BEFORE any executable frame is released. Any watch gap refuses.
	// Freeze the mode/check snapshot under g.mu, then never hold g.mu across
	// the check or watcher I/O. Normal mode keeps exactly one initial watcher
	// pass; borrowed mode requires a FINAL full watcher pass after the
	// potentially long borrowed check, immediately before readiness
	// publication and executable-frame release.
	mode, borrowedCheck := s.g.borrowedModeSnapshot()
	if err := s.g.watchOnce(s, registration); err != nil {
		s.g.latch(fmt.Sprintf("protected origin watcher could not be established for backend PID %d: %v", pid, err))
		return
	}
	if mode == gateModeBorrowed && borrowedCheck != nil {
		if err := borrowedCheck(ctx, registration); err != nil {
			s.g.latch(fmt.Sprintf("borrowed origin pre-release check failed for backend PID %d: %v", pid, err))
			return
		}
		if err := s.g.watchOnce(s, registration); err != nil {
			s.g.latch(fmt.Sprintf("borrowed origin final watcher could not be re-established for backend PID %d: %v", pid, err))
			return
		}
	}
	s.markWatcherReady()
	if !s.g.releaseSession(s) {
		return
	}
	go s.watchSession(registration)
}

func (s *originGateSession) registrationBarrier() chan struct{} {
	s.g.mu.Lock()
	defer s.g.mu.Unlock()
	return s.g.registrationBarrier
}

// watchSession continuously re-verifies the origin, backend, socket census and
// owner terminal facts while the session is retained. Any observer/identity/
// census uncertainty latches and drains; retirement itself belongs exclusively
// to the canonical retireOrDrain path.
func (s *originGateSession) watchSession(registration *originGateRegistration) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-s.g.latchCh:
			return
		case <-s.ctx.Done():
			s.g.latch("admitted session context canceled")
			return
		case <-ticker.C:
		}
		if err := s.g.watchOnce(s, registration); err != nil {
			s.g.latch(fmt.Sprintf("protected origin liveness lost: %v", err))
			return
		}
	}
}

// watchCancellation supervises context cancellation from session creation,
// before authentication, registration or the first-frame hold. A canceled
// admitted session latches and drains even if the readiness barrier was never
// released.
func (s *originGateSession) watchCancellation() {
	select {
	case <-s.done:
	case <-s.g.latchCh:
	case <-s.ctx.Done():
		s.g.latch("admitted session context canceled")
	}
}

// watchOnce is one bounded watch pass with exact captured-identity checks
// before and after the FD/ownership inspection.
func (g *originGate) watchOnce(s *originGateSession, registration *originGateRegistration) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	g.observerMu.Lock()
	var backendStart time.Time
	var backendType string
	err := g.observer.QueryRow(ctx, `SELECT backend_start, backend_type FROM pg_stat_activity WHERE pid=$1`, registration.PID).Scan(&backendStart, &backendType)
	g.observerMu.Unlock()
	// Observer query failure (transport/context) is always uncertainty/loss.
	// A successful query reporting the registered backend gone is only
	// expected after the frontend ended; otherwise it is a live loss.
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			if s.expectedEnd() {
				g.sessionClosed(s)
				return nil
			}
			return fmt.Errorf("registered backend PID %d row disappeared while the session is live", registration.PID)
		}
		return fmt.Errorf("protected observer liveness query failed: %w", err)
	}
	if backendType != "client backend" || !backendStart.Equal(registration.BackendStart) {
		if s.expectedEnd() {
			g.sessionClosed(s)
			return nil
		}
		return fmt.Errorf("registered backend PID %d identity changed", registration.PID)
	}
	if receipt := g.ownerReceipt(s); receipt.Completed && !receipt.Clean && !receipt.Pending {
		return fmt.Errorf("owner terminal loss: %s", receipt.Detail)
	}
	originPID, capturedStart := s.observedPID, s.observedStartID
	if s.supervisor == nil {
		originPID = s.capPID
		if s.capStart != "" {
			if parsed, err := strconv.ParseUint(s.capStart, 10, 64); err == nil {
				capturedStart = parsed
			}
		}
	}
	// Exact captured identity BEFORE the ownership inspection.
	if originPID > 0 && capturedStart != 0 {
		before, err := g.processStartID(originPID)
		if err != nil {
			return g.originIdentityUnavailable(s, originPID, err)
		}
		if before != capturedStart {
			return fmt.Errorf("origin PID %d start identity mismatch before inspection: captured=%d current=%d", originPID, capturedStart, before)
		}
	}
	processes, sockets, err := snapshotPostmasterChildren(ctx, g.fx.containerID, g.fx.postmasterPID, g.fx.postmasterStr)
	if err != nil {
		return fmt.Errorf("protected process/socket census failed: %w", err)
	}
	// Exact captured identity AFTER the ownership inspection.
	if originPID > 0 && capturedStart != 0 {
		after, err := g.processStartID(originPID)
		if err != nil {
			return g.originIdentityUnavailable(s, originPID, err)
		}
		if after != capturedStart {
			return fmt.Errorf("origin PID %d start identity mismatch after inspection: captured=%d current=%d", originPID, capturedStart, after)
		}
	}
	process, present := processes[registration.PID]
	if !present || process.Start != registration.OSStart || process.State == "Z" {
		if s.expectedEnd() {
			g.sessionClosed(s)
			return nil
		}
		return fmt.Errorf("registered backend PID %d is not a live direct postmaster child", registration.PID)
	}
	socketPresent := false
	for _, socket := range sockets {
		if socket.PID == registration.PID && socket.Inode == registration.SocketInode {
			socketPresent = true
			break
		}
	}
	if !socketPresent {
		if s.expectedEnd() {
			g.sessionClosed(s)
			return nil
		}
		return fmt.Errorf("registered backend PID %d socket inode %s left the census", registration.PID, registration.SocketInode)
	}
	if s.originInode != "" && originPID > 0 {
		g.waitCensusBarrier(s)
		owners, err := g.socketInodeOwners(s.originInode)
		if err != nil {
			return fmt.Errorf("origin socket owner census failed: %w", err)
		}
		if len(owners) != 1 || owners[0] != originPID {
			return fmt.Errorf("origin socket inode %s is no longer owned solely by PID %d (owners=%v)", s.originInode, originPID, owners)
		}
	}
	// P2: the admitted captured start identity must be identical before the
	// FD/owner scan AND after it; a start change during the ownership scan is
	// a refusal even when the scan itself completed.
	if originPID > 0 && capturedStart != 0 {
		final, err := g.processStartID(originPID)
		if err != nil {
			return g.originIdentityUnavailable(s, originPID, err)
		}
		if final != capturedStart {
			return fmt.Errorf("origin PID %d start identity mismatch after ownership scan: captured=%d current=%d", originPID, capturedStart, final)
		}
	}
	return nil
}

// originIdentityUnavailable keeps a session retained while its sole wait is
// still pending after the frontend ended; otherwise it is a loss.
func (g *originGate) originIdentityUnavailable(s *originGateSession, pid int, cause error) error {
	receipt := g.ownerReceipt(s)
	if receipt.Pending {
		if s.expectedEnd() {
			g.sessionClosed(s)
			return nil
		}
		return fmt.Errorf("origin process %d identity unavailable while the sole wait is pending: %w", pid, cause)
	}
	if receipt.Completed && !receipt.Clean {
		return fmt.Errorf("owner terminal loss: %s", receipt.Detail)
	}
	g.sessionClosed(s)
	return nil
}

func (s *originGateSession) writeClient(raw []byte) error {
	s.clientWriteMu.Lock()
	defer s.clientWriteMu.Unlock()
	_, err := s.client.Write(raw)
	return err
}

func (s *originGateSession) writeServer(raw []byte) error {
	s.serverWriteMu.Lock()
	defer s.serverWriteMu.Unlock()
	_, err := s.server.Write(raw)
	return err
}

func (s *originGateSession) closeBoth() {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.ended = true
		s.mu.Unlock()
		_ = s.client.Close()
		_ = s.server.Close()
		close(s.done)
		s.releaseOnce.Do(func() { close(s.releaseCh) })
	})
}

// originGateLauncher is the only constructor of originGateCap. It retains the
// real exec.Cmd, its own start identity and the sole Wait for every spawned
// frontend client.
type originGateLauncher struct {
	helperPath string
}

func newOriginGateLauncher(t *testing.T) *originGateLauncher {
	t.Helper()
	return &originGateLauncher{helperPath: buildOriginGateClientHelper(t)}
}

func buildOriginGateClientHelper(t *testing.T) string {
	t.Helper()
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate origin-gate source for the standalone client helper")
	}
	source := filepath.Join(filepath.Dir(testFile), "origin_gate_client_linux_testhelper.go")
	if _, err := os.Stat(source); err != nil {
		t.Fatalf("origin-gate client helper source unavailable: %v", err)
	}
	output := filepath.Join(t.TempDir(), "origin-gate-client")
	cmd := exec.Command("go", "build", "-trimpath", "-o", output, source)
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	if result, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build standalone origin-gate client helper: %v: %s", err, strings.TrimSpace(string(result)))
	}
	return output
}

type originGateClient struct {
	cmd     *exec.Cmd
	stdin   io.WriteCloser
	events  chan string
	pid     int
	start   string
	mu      sync.Mutex
	log     []string
	exited  bool
	waitErr error
	waited  chan struct{}
}

// spawn starts a genuine owned client process and retains the Cmd. The
// credential (if any) is written to private stdin only.
func (l *originGateLauncher) spawn(t *testing.T, ctx context.Context, args []string, secret string) (*originGateClient, error) {
	return l.spawnWithStdout(t, ctx, args, secret, nil)
}

// spawnWithStdout is the retention-test seam: a non-nil writer lets the caller
// block the os/exec output copier so the sole cmd.Wait stays pending after the
// child exits. The launcher remains the only Wait owner.
func (l *originGateLauncher) spawnWithStdout(t *testing.T, ctx context.Context, args []string, secret string, stdoutWriter io.Writer) (*originGateClient, error) {
	t.Helper()
	cmd := exec.Command(l.helperPath, args...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("open owned client private stdin: %v", err)
	}
	client := &originGateClient{cmd: cmd, stdin: stdin, events: make(chan string, 64), pid: 0, waited: make(chan struct{})}
	if stdoutWriter != nil {
		cmd.Stdout = stdoutWriter
		close(client.events)
	} else {
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			t.Fatalf("open owned client public metadata stream: %v", err)
		}
		go func() {
			scanner := bufio.NewScanner(stdout)
			for scanner.Scan() {
				line := scanner.Text()
				client.mu.Lock()
				client.log = append(client.log, line)
				client.mu.Unlock()
				client.events <- line
			}
			close(client.events)
		}()
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start owned origin-gate client process: %v", err)
	}
	client.pid = cmd.Process.Pid
	if start, err := hostProcessStart(cmd.Process.Pid); err == nil {
		client.start = start
	}
	if secret != "" {
		if _, err := fmt.Fprintln(stdin, secret); err != nil {
			t.Fatalf("send private client credential on stdin: %v", err)
		}
	}
	go func() {
		err := cmd.Wait()
		client.mu.Lock()
		client.waitErr = err
		client.exited = true
		client.mu.Unlock()
		close(client.waited)
	}()
	t.Cleanup(func() { client.cleanup(t) })
	return client, nil
}

func (l *originGateLauncher) launch(t *testing.T, ctx context.Context, gate *originGate, args []string, secret string) (*originGateClient, *originGateCap, error) {
	t.Helper()
	return l.launchWithStdout(t, ctx, gate, args, secret, nil)
}

func (l *originGateLauncher) launchWithStdout(t *testing.T, ctx context.Context, gate *originGate, args []string, secret string, stdoutWriter io.Writer) (*originGateClient, *originGateCap, error) {
	t.Helper()
	client, err := l.spawnWithStdout(t, ctx, args, secret, stdoutWriter)
	if err != nil {
		return nil, nil, err
	}
	if client.start == "" {
		return nil, nil, fmt.Errorf("owned client PID %d has no OS start identity; cannot issue an origin capability", client.pid)
	}
	var token [32]byte
	if _, err := rand.Read(token[:]); err != nil {
		t.Fatalf("draw origin capability token: %v", err)
	}
	cap := &originGateCap{token: token, pid: client.pid, start: client.start, ownerClient: client}
	gate.issueCap(cap)
	return client, cap, nil
}

func (c *originGateClient) cleanup(t *testing.T) {
	t.Helper()
	c.mu.Lock()
	exited := c.exited
	c.mu.Unlock()
	if !exited {
		if c.cmd.Process != nil {
			_ = c.cmd.Process.Kill()
		}
		select {
		case <-c.waited:
		case <-time.After(5 * time.Second):
			t.Errorf("owned client PID %d did not exit after kill", c.pid)
		}
	}
}

func (c *originGateClient) waitEvent(ctx context.Context, describe string, match func(string) bool) (string, error) {
	for {
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("timed out waiting for %s; observed events: %s", describe, c.observed())
		case line, ok := <-c.events:
			if !ok {
				return "", fmt.Errorf("owned client output closed before %s; observed events: %s", describe, c.observed())
			}
			if match(line) {
				return line, nil
			}
		}
	}
}

func (c *originGateClient) observed() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return strings.Join(c.log, " | ")
}

func (c *originGateClient) sendLine(line string) error {
	_, err := fmt.Fprintln(c.stdin, line)
	return err
}

func (c *originGateClient) waitExit(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return fmt.Errorf("timed out waiting for owned client PID %d exit; observed: %s", c.pid, c.observed())
	case <-c.waited:
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.waitErr
	}
}

// waitReceipt is the launcher's captured sole cmd.Wait fact. It never blocks.
func (c *originGateClient) waitReceipt() (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.exited, c.waitErr
}

// waitRefusalExit accepts the helper's scripted transport-refusal exit code
// (3 = TRANSPORT_ERROR) as the expected outcome of a refused connection. A
// crash (exit 2) or a signal is still reported as an error.
func (c *originGateClient) waitRefusalExit(ctx context.Context) error {
	err := c.waitExit(ctx)
	if err == nil {
		return nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) && exitErr.ExitCode() == 3 {
		return nil
	}
	return err
}

func (c *originGateClient) exitedAlready() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.exited
}

func (c *originGateClient) killAndWait(ctx context.Context) error {
	if c.exitedAlready() {
		return nil
	}
	if c.cmd.Process != nil {
		_ = c.cmd.Process.Kill()
	}
	err := c.waitExit(ctx)
	if err == nil {
		return nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return nil // the kill was deliberate
	}
	return err
}

func originGateEventFields(line string) map[string]string {
	fields := make(map[string]string)
	for _, item := range strings.Fields(line) {
		key, value, ok := strings.Cut(item, "=")
		if ok {
			fields[key] = value
		}
	}
	return fields
}

// hostProcessStart reads the Linux start identity (field 22) of a host PID.
func hostProcessStart(pid int) (string, error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return "", err
	}
	line := string(data)
	end := strings.LastIndex(line, ") ")
	if end < 0 || end+2 >= len(line) {
		return "", fmt.Errorf("malformed /proc/%d/stat", pid)
	}
	fields := strings.Fields(line[end+2:])
	if len(fields) < 20 {
		return "", fmt.Errorf("short /proc/%d/stat", pid)
	}
	return fields[19], nil
}

// hostProcessStartID is the numeric form of the Linux start identity used by
// the real pinned owner (production linuxProcessStartIdentity).
func hostProcessStartID(pid int) (uint64, error) {
	raw, err := hostProcessStart(pid)
	if err != nil {
		return 0, err
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || value == 0 {
		return 0, fmt.Errorf("invalid Linux start identity %q for PID %d", raw, pid)
	}
	return value, nil
}

type hostTCPRow struct {
	local  string
	remote string
	state  string
	inode  string
}

func readHostTCPRows() ([]hostTCPRow, error) {
	var rows []hostTCPRow
	for _, path := range []string{"/proc/net/tcp", "/proc/net/tcp6"} {
		file, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			fields := strings.Fields(scanner.Text())
			if len(fields) < 10 {
				continue
			}
			rows = append(rows, hostTCPRow{local: fields[1], remote: fields[2], state: fields[3], inode: fields[9]})
		}
		err = scanner.Err()
		_ = file.Close()
		if err != nil {
			return nil, err
		}
	}
	return rows, nil
}

// procNetEndpoint formats one IP:port pair the way /proc/net/tcp{,6} bytes are
// rendered (IPv4 little-endian word; IPv6 four little-endian words).
func procNetEndpoint(ip net.IP, port int) (string, error) {
	if ip == nil {
		return "", errors.New("nil IP for /proc endpoint formatting")
	}
	if v4 := ip.To4(); v4 != nil {
		return fmt.Sprintf("%02X%02X%02X%02X:%04X", v4[3], v4[2], v4[1], v4[0], port), nil
	}
	v6 := ip.To16()
	if v6 == nil {
		return "", fmt.Errorf("unformattable IP %v", ip)
	}
	var builder strings.Builder
	for i := 0; i < 4; i++ {
		word := v6[i*4 : i*4+4]
		fmt.Fprintf(&builder, "%02X%02X%02X%02X", word[3], word[2], word[1], word[0])
	}
	return fmt.Sprintf("%s:%04X", builder.String(), port), nil
}

// hostSocketInodeOwners returns every live PID whose fd table references the
// socket inode. The census is positive only when EVERY live process is fully
// inspectable: any non-ENOENT fd-table/status/start/fd read error for any live
// process (permission, malformed, IO) yields an incomplete-census error that
// callers must treat as UNKNOWN and refuse. No ancestry, UID, name, appname or
// idle whitelist grants uniqueness, because a double-forked orphan holding an
// inherited descriptor can be reparented to PID 1 while its original owner is
// still alive, and a PID-lifetime registry cannot see forks between samples.
// Readable processes are always scanned. ENOENT is the only reconcile-by-absence
// outcome (the PID/fd vanished between samples). Controlled cross-process
// SCM_RIGHTS transfer from unrelated processes is explicitly outside this
// primitive and is not treated as ordinary orphan inheritance.
// Test-only proc read-order seams; defaults are the real calls.
var (
	hostCensusStartReader = hostProcessStartID
	hostCensusDirReader   = os.ReadDir
)

func hostSocketInodeOwners(inode string) ([]int, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, err
	}
	suffix := "socket:[" + inode + "]"
	type inspectedProcess struct {
		pid   int
		start uint64
	}
	var inspected []inspectedProcess
	var owners []int
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid <= 0 {
			continue
		}
		fdDir := fmt.Sprintf("/proc/%d/fd", pid)
		// Per-process read order: the start identity is captured BEFORE the fd
		// directory enumeration, so a PID reused between the two reads cannot
		// pair old fd names with a new incarnation; the after-scan recapture
		// below must equal this initial value.
		startID, startErr := hostCensusStartReader(pid)
		if startErr != nil {
			if errors.Is(startErr, os.ErrNotExist) {
				continue // vanished during the scan
			}
			return nil, fmt.Errorf("owner census incomplete: PID %d start identity unreadable: %w", pid, startErr)
		}
		fds, err := hostCensusDirReader(fdDir)
		switch {
		case err == nil:
			owned := false
			for _, fd := range fds {
				target, err := os.Readlink(filepath.Join(fdDir, fd.Name()))
				if err != nil {
					if errors.Is(err, os.ErrNotExist) {
						continue // fd vanished during the scan
					}
					return nil, fmt.Errorf("owner census incomplete: PID %d fd unreadable: %w", pid, err)
				}
				if strings.HasSuffix(target, suffix) {
					owned = true
					break
				}
			}
			inspected = append(inspected, inspectedProcess{pid: pid, start: startID})
			if owned {
				owners = append(owners, pid)
			}
		case errors.Is(err, os.ErrNotExist):
			continue // vanished during the scan
		default:
			return nil, fmt.Errorf("owner census incomplete: PID %d fd table unreadable: %w", pid, err)
		}
	}
	// PID start reuse around the scan: every inspected process must still carry
	// its captured start identity; otherwise the census is incomplete.
	for _, candidate := range inspected {
		current, err := hostCensusStartReader(candidate.pid)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue // vanished; it cannot own the inode any more
			}
			return nil, fmt.Errorf("owner census incomplete: PID %d start identity unreadable after scan: %w", candidate.pid, err)
		}
		if current != candidate.start {
			return nil, fmt.Errorf("owner census incomplete: PID %d start identity changed during the scan", candidate.pid)
		}
	}
	sort.Ints(owners)
	return owners, nil
}

// hostOwnedSocketInode verifies that the cap process owns the exact accepted
// peer four-tuple and returns the owned socket inode. Role, application_name,
// idle state and SQL shape are never consulted.
func hostOwnedSocketInode(pid int, peerIP net.IP, peerPort int, gateIP net.IP, gatePort int) (string, error) {
	fdDir := fmt.Sprintf("/proc/%d/fd", pid)
	entries, err := os.ReadDir(fdDir)
	if err != nil {
		return "", fmt.Errorf("read process fd table: %w", err)
	}
	owned := make(map[string]bool)
	for _, entry := range entries {
		target, err := os.Readlink(filepath.Join(fdDir, entry.Name()))
		if err != nil {
			continue
		}
		if !strings.Contains(target, "socket:[") {
			continue
		}
		inode := target[strings.Index(target, "socket:[")+len("socket:["):]
		inode = strings.TrimSuffix(inode, "]")
		if inode != "" {
			owned[inode] = true
		}
	}
	wantLocal, err := procNetEndpoint(peerIP, peerPort)
	if err != nil {
		return "", err
	}
	wantRemote, err := procNetEndpoint(gateIP, gatePort)
	if err != nil {
		return "", err
	}
	rows, err := readHostTCPRows()
	if err != nil {
		return "", fmt.Errorf("read host tcp tables: %w", err)
	}
	for _, row := range rows {
		if row.state != "01" || !owned[row.inode] {
			continue
		}
		if strings.EqualFold(row.local, wantLocal) && strings.EqualFold(row.remote, wantRemote) {
			owners, err := hostSocketInodeOwners(row.inode)
			if err != nil {
				return "", fmt.Errorf("origin socket owner census failed: %w", err)
			}
			if len(owners) != 1 || owners[0] != pid {
				return "", fmt.Errorf("origin socket inode %s is owned by %v, not solely by PID %d (inherited/transferred descriptor)", row.inode, owners, pid)
			}
			return row.inode, nil
		}
	}
	var diagnostics []string
	for _, row := range rows {
		if row.state == "01" && owned[row.inode] {
			diagnostics = append(diagnostics, fmt.Sprintf("inode=%s %s>%s", row.inode, row.local, row.remote))
		}
	}
	sort.Strings(diagnostics)
	return "", fmt.Errorf("no owned established socket matches %s>%s; owned sockets: [%s]", wantLocal, wantRemote, strings.Join(diagnostics, ", "))
}

// ---------------------------------------------------------------------------
// Minimal bridge binding contract for the later supervised-writer lane.
//
// NOT implemented here: the drill bridge, target writer and target process
// files are out of this lane's write scope and must not be modified until the
// parent authorizes the wiring. CORE05 terminal retains the real facts in
// internal/recovery/targetprocess_linux.go (targetProcessObservation): the
// pinned owner performs cmd.Start plus the sole cmd.Wait and records
//   started/cmd/process/pid/startID/startErr, then
//   runnerReturned/result, then
//   childWaitCompleted/waitErr/waitExitCode/terminal
// as separate facts, with launch and the sole Wait on one pinned owner.
//
// To bind this gate primitive to that real TargetRunner, the bridge needs
// exactly these read-only, non-forgeable projections, fed only by that pinned
// owner:
//
//  1. an opaque drill-only handle wrapping the live *targetProcessObservation
//     (created by the pinned owner at launch time; no exported fields, no JSON
//     constructor, no ability to Wait, cancel or mutate the child), plus
//  2. three projections that mirror the retained facts one-to-one instead of a
//     merged authority value:
//       StartedIdentity() -> (cmd *exec.Cmd, process *os.Process, pid int,
//                             startID uint64, startErr error, started bool)
//       RunnerDisposition() -> (runnerReturned bool, result PGCommandResult)
//       WaitTerminal() -> (childWaitCompleted bool, waitErr error,
//                          waitExitCode int, terminal bool)
//  3. an AwaitStarted(ctx) readiness signal so the accepted peer four-tuple can
//     be bound to the authentic PID/startID only after the pinned owner has
//     captured it, failing closed on startErr or context end;
//  4. an invocation variant that lets the drill hand a caller-created
//     observation handle into the real pinned owner (same shape as
//     DrillRunTargetWriterBorrowingLock, which creates its observation
//     internally today), so the origin capability is issued by the real
//     launcher process and never by the drill test from diagnostic facts or
//     ordinary JSON.
//
// Latch rule for that binding: only WaitTerminal().terminal (the sole cmd.Wait
// actually returned for the exact child) may invalidate/latch an owned origin;
// a bounded runner disposition (runnerReturned, e.g. PGCommandAmbiguous while
// the sole wait is still pending) must never be projected as terminal
// authority, and StartIdentity must never be defaulted on startErr.
// ---------------------------------------------------------------------------
