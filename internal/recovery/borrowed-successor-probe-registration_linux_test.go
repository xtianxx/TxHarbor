//go:build linux && drill

// borrowed-successor-probe-registration_linux_test.go is the bounded READ-ONLY
// P1 successor registration/use lane. It registers ONE authenticated successor
// P1 connection (never a staging SASL hold and never a new native client) as a
// copy-shared one-use capability bound to the fixture, the consumed receipt
// entry, the captured control anchor and the exact retained connection; the
// single use rechecks the entry/anchor outside any owner transaction, reads
// fresh catalog facts, revalidates the exact successor identity through the
// strict census/OS evidence and executes exactly one fixed SELECT 1 probe on
// the SAME connection with a post-probe identity recheck. There is no
// continuous fence, no DDL, no restore, no rebuild, no acceptance, no manifest,
// no downstream and no Gate1 authority; identity-discovery queries are counted
// separately from probe executions; every failure is permanent and shared with
// every copy of the registration. Secrets, verifiers and DSNs are never logged.
package recovery_test

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xtianxx/txharbor/internal/recovery"
)

// errBorrowedSuccessorVerifierDrift is the distinct refusal of the fresh
// catalog comparison when the committed verifier no longer equals the expected
// P1 verifier while the role facts are unchanged.
var errBorrowedSuccessorVerifierDrift = errors.New("successor catalog verifier drifted from the registered P1 baseline")

// errBorrowedSuccessorRoleFactsDrift is the distinct refusal of the fresh
// catalog comparison when the immutable W role facts (OID/flags/membership)
// changed.
var errBorrowedSuccessorRoleFactsDrift = errors.New("successor catalog W role facts drifted")

// borrowedSuccessorStage is the deterministic pre-probe publication barrier of
// the one-use successor probe. It is a test-only notification/barrier seam and
// grants nothing.
type borrowedSuccessorStage string

const borrowedSuccessorStageUsePublish borrowedSuccessorStage = "use-publish"

// borrowedSuccessorConnIdentity is the read-only identity of the retained
// successor connection as read through that same connection.
type borrowedSuccessorConnIdentity struct {
	backendPID   int
	backendStart time.Time
	roleName     string
	databaseName string
	roleOID      uint32
	targetDBOID  uint32
	serverLocal  string
	clientLocal  string
}

// borrowedSuccessorState is the copy-shared one-use/permanent-loss state of one
// registered successor. Copies of a registration share this pointer, so a
// genuine failure permanently invalidates every copy; there is no reset or
// reconstruction rehabilitation for copies.
type borrowedSuccessorState struct {
	mu            sync.Mutex
	invalidated   bool
	invalidReason string
	using         bool
	used          bool

	fixture *borrowedAuthHandoffFixture
	entry   *borrowedReceiptAuthEntry
	anchor  recovery.DrillControlAnchor
	conn    *pgx.Conn

	backendPID   int
	backendStart time.Time
	osStart      uint64
	socketInode  string
	roleOID      uint32
	targetDBOID  uint32
	serverLocal  string
	clientLocal  string

	expectedVerifier string
	expectedState    borrowedOwnerRotationRoleState

	probeExecutions int32
	identityQueries int32

	// stage is the optional test-only pre-probe publication barrier; statFn and
	// associateFn are test-only injectable seams for the unknown-evidence
	// negatives. They are nil in real runs.
	stage       func(borrowedSuccessorStage)
	statFn      func(context.Context, *borrowedSuccessorState) (string, uint64, error)
	associateFn func(context.Context, *borrowedSuccessorState) (borrowedAuthStagingBackendToken, error)
}

// borrowedSuccessorRegistration is the private one-use successor capability.
// It exposes no public authority and only ever returns errors.
type borrowedSuccessorRegistration struct {
	state *borrowedSuccessorState
}

// borrowedSuccessorBindingKey is the concrete registration binding: the exact
// fixture, shared entry state, anchor, retained connection and expected
// committed verifier. All fields are comparable, so the key can be used for
// the shared slot registry; the entry is keyed by its SHARED state identity
// (never the wrapper address), so equivalent wrappers over the same
// fixture/anchor/conn/baseline reuse or refuse the same slot state.
type borrowedSuccessorBindingKey struct {
	fixture    *borrowedAuthHandoffFixture
	entryState *borrowedReceiptAuthEntryState
	anchor     recovery.DrillControlAnchor
	conn       *pgx.Conn
	verifier   string
}

// borrowedSuccessorSlot is the shared registration slot of one concrete
// binding. The registry is test-process-local; a reconstruction over the same
// binding can only reuse the existing state (or refuse a consumed/invalidated
// one) and can never mint fresh state.
type borrowedSuccessorSlot struct {
	mu    sync.Mutex
	state *borrowedSuccessorState
}

var borrowedSuccessorSlots sync.Map

// borrowedSuccessorAcquireSlot acquires the binding slot with a caller-bounded
// wait: TryLock plus a bounded ticker that selects the caller context, so a
// held slot can never block a caller past its own deadline and no locker
// goroutine is abandoned.
func borrowedSuccessorAcquireSlot(ctx context.Context, slot *borrowedSuccessorSlot) error {
	if ctx == nil || slot == nil {
		return errors.New("successor slot acquisition requires a caller context and the binding slot")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("successor slot acquisition refused: caller context already done: %w", err)
	}
	if slot.mu.TryLock() {
		return nil
	}
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("successor slot acquisition refused: caller context ended while the binding slot was held: %w", ctx.Err())
		case <-ticker.C:
			if slot.mu.TryLock() {
				return nil
			}
		}
	}
}

// borrowedSuccessorEntryConsumed requires a completed entry consumption under
// the entry state mutex: an unentered, in-progress or invalidated entry
// refuses.
func borrowedSuccessorEntryConsumed(entry *borrowedReceiptAuthEntry) error {
	if entry == nil || entry.state == nil {
		return errors.New("successor registration requires the concrete receipt entry")
	}
	state := entry.state
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.invalidated {
		return errors.New("successor registration entry is permanently invalidated")
	}
	if state.entering {
		return errors.New("successor registration entry is still entering the auth stage")
	}
	if !state.entered {
		return errors.New("successor registration entry was never consumed")
	}
	return nil
}

// borrowedSuccessorBaseline is the reusable genuine provenance of one
// successor fixture: the real positive chain, the consumed receipt entry, the
// captured control anchor and the committed client-generated P1 baseline.
type borrowedSuccessorBaseline struct {
	fixture    *borrowedAuthHandoffFixture
	entry      *borrowedReceiptAuthEntry
	anchor     recovery.DrillControlAnchor
	owner      recovery.DrillControlAnchorFacts
	preState   borrowedOwnerRotationRoleState
	verifierP1 string
	passwordP1 string
}

// borrowedSuccessorDiagnoseAssociation is a bounded, best-effort failure
// diagnostic: it logs only public endpoint tuples and identities (never a
// secret) to make a strict association refusal attributable.
func borrowedSuccessorDiagnoseAssociation(t *testing.T, ctx context.Context, f *borrowedAuthHandoffFixture, identity borrowedSuccessorConnIdentity) {
	t.Helper()
	t.Logf("successor association diagnosis: expected pid=%d server=%s client=%s", identity.backendPID, identity.serverLocal, identity.clientLocal)
	diagCtx, cancelDiag := context.WithTimeout(ctx, borrowedAuthStagingAssociationBudget)
	defer cancelDiag()
	expectedStart, err := strconv.ParseUint(f.fx.postmasterStr, 10, 64)
	if err != nil || expectedStart == 0 {
		t.Logf("successor association diagnosis: postmaster start identity invalid")
		return
	}
	out, execErr := dockerExec(diagCtx, f.fx.containerID, borrowedAuthStagingHelperPath, "census", strconv.Itoa(f.fx.postmasterPID), f.fx.postmasterStr)
	if execErr != nil {
		t.Logf("successor association diagnosis: census exec refused")
		return
	}
	report, parseErr := parseBorrowedAuthStagingCensus(out, f.fx.postmasterPID, expectedStart)
	if parseErr != nil {
		t.Logf("successor association diagnosis: census report refused")
		return
	}
	for _, child := range report.Children {
		for _, socket := range child.Sockets {
			t.Logf("successor association diagnosis: child pid=%d start=%d state=%s socket kind=%s local=%s remote=%s",
				child.PID, child.Start, child.StateBefore, socket.Kind, socket.Local, socket.Remote)
		}
	}
}

// borrowedSuccessorReadConnIdentity reads the retained connection identity
// through that same connection only (PID/start, W role/name/OID, target
// database name/OID and both TCP endpoints). It never uses a diagnostic
// projection or a boolean claim.
func borrowedSuccessorReadConnIdentity(ctx context.Context, conn *pgx.Conn) (borrowedSuccessorConnIdentity, error) {
	var identity borrowedSuccessorConnIdentity
	var serverAddr, clientAddr string
	var serverPort, clientPort int
	readCtx, cancelRead := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	err := conn.QueryRow(readCtx, `
SELECT a.pid::int, a.backend_start, a.usename::text, a.datname::text, r.oid::oid, d.oid::oid,
       coalesce(host(inet_server_addr()),''), coalesce(inet_server_port(),0)::int,
       coalesce(host(inet_client_addr()),''), coalesce(inet_client_port(),0)::int
FROM pg_stat_activity a
JOIN pg_roles r ON r.rolname = a.usename
JOIN pg_database d ON d.datname = a.datname
WHERE a.pid = pg_backend_pid()`).
		Scan(&identity.backendPID, &identity.backendStart, &identity.roleName, &identity.databaseName,
			&identity.roleOID, &identity.targetDBOID, &serverAddr, &serverPort, &clientAddr, &clientPort)
	cancelRead()
	if err != nil {
		return identity, errors.New("successor identity read through the retained connection refused")
	}
	if identity.backendPID <= 1 || identity.backendStart.IsZero() {
		return identity, errors.New("successor backend PID/start is not a positive identity")
	}
	if serverAddr == "" || serverPort <= 0 || clientAddr == "" || clientPort <= 0 {
		return identity, errors.New("successor connection is not an established TCP tuple")
	}
	identity.serverLocal = net.JoinHostPort(serverAddr, strconv.Itoa(serverPort))
	identity.clientLocal = net.JoinHostPort(clientAddr, strconv.Itoa(clientPort))
	return identity, nil
}

// borrowedSuccessorReadCatalogFacts reads the fresh committed W catalog facts
// through the fixture administrator with a bounded child.
func borrowedSuccessorReadCatalogFacts(ctx context.Context, f *borrowedAuthHandoffFixture) (borrowedOwnerRotationRoleState, error) {
	readCtx, cancelRead := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	state, err := borrowedOwnerRotationReadRoleState(readCtx, f.fx.admin, f.writerRole)
	cancelRead()
	return state, err
}

// borrowedSuccessorAssociateToken associates the successor's actual server and
// client tuples through the complete strict census with bounded retries, using
// the shared strict matcher. It never uses a SQL role/appname whitelist.
func borrowedSuccessorAssociateToken(ctx context.Context, f *borrowedAuthHandoffFixture, serverLocal, clientLocal string) (borrowedAuthStagingBackendToken, error) {
	expectedStart, err := strconv.ParseUint(f.fx.postmasterStr, 10, 64)
	if err != nil || expectedStart == 0 {
		return borrowedAuthStagingBackendToken{}, errors.New("successor association postmaster start identity is invalid")
	}
	attempt := func(attemptCtx context.Context) (borrowedAuthStagingCensus, error) {
		out, execErr := dockerExec(attemptCtx, f.fx.containerID, borrowedAuthStagingHelperPath, "census", strconv.Itoa(f.fx.postmasterPID), f.fx.postmasterStr)
		if execErr != nil {
			return borrowedAuthStagingCensus{}, errors.New("successor strict census exec refused")
		}
		return parseBorrowedAuthStagingCensus(out, f.fx.postmasterPID, expectedStart)
	}
	return borrowedAuthStagingAwaitBackendWith(ctx, serverLocal, clientLocal, attempt)
}

// borrowedSuccessorStat is the bounded strict OS identity stat of the retained
// successor, with the test-only injection seam.
func borrowedSuccessorStat(ctx context.Context, s *borrowedSuccessorState) (string, uint64, error) {
	if s.statFn != nil {
		return s.statFn(ctx, s)
	}
	statCtx, cancelStat := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	defer cancelStat()
	return borrowedAuthStagingContainerStat(statCtx, s.fixture.fx.containerID, s.backendPID)
}

// borrowedSuccessorAssociate is the bounded exact tuple association of the
// retained successor, with the test-only injection seam.
func borrowedSuccessorAssociate(ctx context.Context, s *borrowedSuccessorState) (borrowedAuthStagingBackendToken, error) {
	if s.associateFn != nil {
		return s.associateFn(ctx, s)
	}
	return borrowedSuccessorAssociateToken(ctx, s.fixture, s.serverLocal, s.clientLocal)
}

// borrowedSuccessorRevalidate revalidates the exact retained successor identity
// in a fixed order: the strict OS stat of the registered PID/start first (a
// terminated backend refuses immediately and no live lookalike can substitute),
// then the complete strict census tuple association, then the strict backend
// inspection with the retained socket inode, then the same-connection identity
// read. Every read is caller-bounded and counted as an identity-discovery
// query, never as a probe execution.
func borrowedSuccessorRevalidate(ctx context.Context, s *borrowedSuccessorState) error {
	osState, osStart, statErr := borrowedSuccessorStat(ctx, s)
	atomic.AddInt32(&s.identityQueries, 1)
	if statErr != nil || osStart != s.osStart || !borrowedAuthStagingLiveState(osState) {
		return errors.New("successor retained OS identity is not the live registered backend")
	}
	token, err := borrowedSuccessorAssociate(ctx, s)
	atomic.AddInt32(&s.identityQueries, 1)
	if err != nil {
		return err
	}
	if token.ChildPID != s.backendPID || token.ChildStart != s.osStart || token.Inode != s.socketInode ||
		token.Local != s.serverLocal || token.Remote != s.clientLocal {
		return errors.New("successor strict census token changed")
	}
	inspectCtx, cancelInspect := context.WithTimeout(ctx, borrowedAuthStagingAssociationBudget)
	_, inspectErr := inspectBorrowedIdentityStrict(inspectCtx, s.fixture.fx.containerID, s.fixture.prefix.strictHelperPath,
		s.fixture.fx.postmasterPID, s.fixture.fx.postmasterStr, s.backendPID, s.socketInode, borrowedIdentityStrictPhaseInitial)
	cancelInspect()
	atomic.AddInt32(&s.identityQueries, 1)
	if inspectErr != nil {
		return errors.New("successor strict backend inspection refused")
	}
	identity, err := borrowedSuccessorReadConnIdentity(ctx, s.conn)
	atomic.AddInt32(&s.identityQueries, 1)
	if err != nil {
		return err
	}
	if identity.backendPID != s.backendPID || !identity.backendStart.Equal(s.backendStart) ||
		identity.roleOID != s.roleOID || identity.targetDBOID != s.targetDBOID {
		return errors.New("successor retained connection identity changed")
	}
	return nil
}

// newBorrowedSuccessorRegistration validates and binds the concrete fixture,
// the consumed receipt entry, the captured control anchor and the retained
// authenticated connection into the copy-shared one-use state. It reads the
// actual connection identity, requires the exact W/target role and database
// catalog OIDs, requires the registered P1 catalog baseline, and correlates the
// complete strict census and strict OS evidence with the exact full tuple
// token. It never accepts a PID/JSON/boolean claim and never uses a staging
// SASL hold or a new native client.
func newBorrowedSuccessorRegistration(t *testing.T, ctx context.Context, f *borrowedAuthHandoffFixture, entry *borrowedReceiptAuthEntry, anchor recovery.DrillControlAnchor, conn *pgx.Conn, expectedVerifier string, expectedState borrowedOwnerRotationRoleState) (*borrowedSuccessorRegistration, error) {
	if f == nil || f.fx == nil || f.prefix == nil || entry == nil || entry.state == nil || conn == nil {
		return nil, errors.New("successor registration requires the concrete fixture, entry and retained connection")
	}
	if entry.state.fixture != f {
		return nil, errors.New("successor registration entry does not belong to this fixture")
	}
	if err := borrowedSuccessorEntryConsumed(entry); err != nil {
		return nil, err
	}
	facts := anchor.Diagnostics()
	if !facts.Present || facts.Invalidated {
		return nil, errors.New("successor registration anchor is absent or invalidated")
	}
	binding := f.run.Binding()
	if facts.OriginalTargetKey != binding.OriginalTargetKey() || facts.ControlTargetKey != binding.ControlTargetKey() {
		return nil, errors.New("successor registration anchor does not retain the original/control keys")
	}
	if expectedVerifier == "" || expectedState.oid == 0 {
		return nil, errors.New("successor registration requires the expected committed P1 verifier and role facts")
	}
	// Shared binding slot: a reconstruction over the same concrete binding can
	// only reuse the existing state (and refuse a consumed/invalidated one); it
	// never mints fresh state. The slot is acquired with a caller-bounded wait,
	// so a held slot can never block a caller past its own deadline.
	key := borrowedSuccessorBindingKey{fixture: f, entryState: entry.state, anchor: anchor, conn: conn, verifier: expectedVerifier}
	slotAny, _ := borrowedSuccessorSlots.LoadOrStore(key, &borrowedSuccessorSlot{})
	slot := slotAny.(*borrowedSuccessorSlot)
	if err := borrowedSuccessorAcquireSlot(ctx, slot); err != nil {
		return nil, err
	}
	defer slot.mu.Unlock()
	if existing := slot.state; existing != nil {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("successor registration refused: caller context ended before reuse publication: %w", err)
		}
		if existing.invalidReasonNow() != "" {
			return nil, errors.New("successor registration binding is permanently invalidated")
		}
		if existing.usedNow() {
			return nil, errors.New("successor registration binding was already used")
		}
		return &borrowedSuccessorRegistration{state: existing}, nil
	}
	identity, err := borrowedSuccessorReadConnIdentity(ctx, conn)
	if err != nil {
		return nil, err
	}
	if identity.roleName != f.writerRole || identity.databaseName != f.targetDB {
		return nil, errors.New("successor registration connection role/database is not the W target")
	}
	if identity.roleOID != f.writerRoleOID || identity.targetDBOID != f.targetDBOID {
		return nil, errors.New("successor registration connection catalog OIDs are not the W target")
	}
	current, err := borrowedSuccessorReadCatalogFacts(ctx, f)
	if err != nil {
		return nil, errors.New("successor registration catalog facts read refused")
	}
	if subtle.ConstantTimeCompare([]byte(current.verifier), []byte(expectedVerifier)) != 1 {
		return nil, errors.New("successor registration catalog verifier is not the expected committed P1 verifier")
	}
	if !borrowedFenceGapRoleStateEqual(current, expectedState) {
		return nil, errors.New("successor registration catalog role facts changed")
	}
	token, err := borrowedSuccessorAssociateToken(ctx, f, identity.serverLocal, identity.clientLocal)
	if err != nil {
		borrowedSuccessorDiagnoseAssociation(t, ctx, f, identity)
		return nil, err
	}
	if token.ChildPID != identity.backendPID || token.Local != identity.serverLocal || token.Remote != identity.clientLocal {
		return nil, errors.New("successor registration strict census token does not match the retained connection identity")
	}
	inspectCtx, cancelInspect := context.WithTimeout(ctx, borrowedAuthStagingAssociationBudget)
	_, inspectErr := inspectBorrowedIdentityStrict(inspectCtx, f.fx.containerID, f.prefix.strictHelperPath,
		f.fx.postmasterPID, f.fx.postmasterStr, identity.backendPID, token.Inode, borrowedIdentityStrictPhaseInitial)
	cancelInspect()
	if inspectErr != nil {
		return nil, errors.New("successor registration strict backend inspection refused")
	}
	osStatCtx, cancelOsStat := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	osState, osStart, osErr := borrowedAuthStagingContainerStat(osStatCtx, f.fx.containerID, identity.backendPID)
	cancelOsStat()
	if osErr != nil || osStart != token.ChildStart || !borrowedAuthStagingLiveState(osState) {
		return nil, errors.New("successor registration strict OS identity refused")
	}
	state := &borrowedSuccessorState{
		fixture: f, entry: entry, anchor: anchor, conn: conn,
		backendPID: identity.backendPID, backendStart: identity.backendStart,
		osStart: token.ChildStart, socketInode: token.Inode,
		roleOID: identity.roleOID, targetDBOID: identity.targetDBOID,
		serverLocal: identity.serverLocal, clientLocal: identity.clientLocal,
		expectedVerifier: expectedVerifier, expectedState: expectedState,
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("successor registration refused: caller context ended before publication: %w", err)
	}
	slot.state = state
	return &borrowedSuccessorRegistration{state: state}, nil
}

// Use performs the single read-only successor probe. Classification and the
// in-progress reservation happen under the shared mutex BEFORE the caller
// context; the genuine entry/anchor rechecks run outside any owner transaction;
// fresh catalog facts are classified with the role-fact change first and the
// verifier drift second; the exact successor identity is revalidated; the
// deterministic pre-probe publication barrier runs; exactly one fixed SELECT 1
// probe executes on the SAME connection with a post-probe identity recheck; and
// the one-use publication linearizes with invalidation under the shared mutex.
// No SHARE is held, no owner transaction is opened and no probe is retried.
func (r *borrowedSuccessorRegistration) Use(ctx context.Context) error {
	if r == nil || r.state == nil {
		return errors.New("successor registration is absent")
	}
	state := r.state
	state.mu.Lock()
	if state.invalidated {
		state.mu.Unlock()
		return errors.New("successor registration is permanently invalidated")
	}
	if state.used {
		state.mu.Unlock()
		return errors.New("successor registration was already used")
	}
	if state.using {
		state.mu.Unlock()
		return errors.New("successor registration is already being used")
	}
	if ctx == nil {
		state.invalidated = true
		state.invalidReason = "successor use context is missing"
		state.mu.Unlock()
		return errors.New("successor use requires a bounded context")
	}
	state.using = true
	state.mu.Unlock()

	if err := state.verifyEntryAndAnchor(ctx); err != nil {
		state.invalidate("successor entry/anchor recheck refused")
		return err
	}
	if err := state.verifyCatalogFacts(ctx); err != nil {
		state.invalidate("successor fresh catalog facts refused")
		return err
	}
	if err := borrowedSuccessorRevalidate(ctx, state); err != nil {
		state.invalidate("successor identity revalidation refused")
		return err
	}
	// Deterministic pre-probe publication barrier: a cancel or loss here
	// refuses before any probe execution.
	state.pauseAt(borrowedSuccessorStageUsePublish)
	if err := ctx.Err(); err != nil {
		state.invalidate("context ended before successor probe execution")
		return errors.New("successor use context ended before the probe")
	}
	if err := state.executeProbe(ctx); err != nil {
		state.invalidate("successor probe refused")
		return err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.invalidated {
		return errors.New("successor registration was permanently invalidated concurrently")
	}
	if state.used {
		return errors.New("successor registration was already used")
	}
	if err := ctx.Err(); err != nil {
		state.invalidated = true
		state.invalidReason = "context ended before successor use publication"
		return errors.New("successor use context ended before publication")
	}
	state.using = false
	state.used = true
	return nil
}

// verifyEntryAndAnchor rechecks the genuine consumed entry and the captured
// control anchor OUTSIDE any owner transaction, with a caller-bounded child.
func (s *borrowedSuccessorState) verifyEntryAndAnchor(ctx context.Context) error {
	recheckCtx, cancelRecheck := context.WithTimeout(ctx, 60*time.Second)
	defer cancelRecheck()
	if err := s.entry.Recheck(recheckCtx); err != nil {
		return errors.New("successor use entry recheck refused")
	}
	if err := s.anchor.Recheck(recheckCtx); err != nil {
		return errors.New("successor use anchor recheck refused")
	}
	return nil
}

// verifyCatalogFacts reads fresh catalog facts and classifies them: an
// OID/flags/membership change returns the role-facts drift error first; a
// verifier mismatch with unchanged facts returns the verifier-drift error.
func (s *borrowedSuccessorState) verifyCatalogFacts(ctx context.Context) error {
	current, err := borrowedSuccessorReadCatalogFacts(ctx, s.fixture)
	atomic.AddInt32(&s.identityQueries, 1)
	if err != nil {
		return errors.New("successor fresh catalog facts read refused")
	}
	if !borrowedFenceGapRoleStateEqual(current, s.expectedState) {
		return fmt.Errorf("%w: W role facts changed", errBorrowedSuccessorRoleFactsDrift)
	}
	if subtle.ConstantTimeCompare([]byte(current.verifier), []byte(s.expectedVerifier)) != 1 {
		return fmt.Errorf("%w (observed_length=%d expected_length=%d)", errBorrowedSuccessorVerifierDrift, len(current.verifier), len(s.expectedVerifier))
	}
	return nil
}

// executeProbe runs the single fixed SELECT 1 probe on the SAME retained
// connection and then rechecks the connection/OS identity. The probe execution
// counter is incremented exactly once per execution attempt; the post-probe
// identity reads are counted as identity-discovery queries.
func (s *borrowedSuccessorState) executeProbe(ctx context.Context) error {
	probeCtx, cancelProbe := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	var one int
	probeErr := s.conn.QueryRow(probeCtx, `SELECT 1`).Scan(&one)
	cancelProbe()
	atomic.AddInt32(&s.probeExecutions, 1)
	if probeErr != nil || one != 1 {
		return errors.New("successor fixed SELECT 1 probe refused")
	}
	postCtx, cancelPost := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	var pid int
	pidErr := s.conn.QueryRow(postCtx, `SELECT pg_backend_pid()`).Scan(&pid)
	cancelPost()
	atomic.AddInt32(&s.identityQueries, 1)
	if pidErr != nil || pid != s.backendPID {
		return errors.New("successor probe ran on a changed connection identity")
	}
	statCtx, cancelStat := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	osState, osStart, statErr := borrowedAuthStagingContainerStat(statCtx, s.fixture.fx.containerID, s.backendPID)
	cancelStat()
	atomic.AddInt32(&s.identityQueries, 1)
	if statErr != nil || osStart != s.osStart || !borrowedAuthStagingLiveState(osState) {
		return errors.New("successor probe post-check identity changed")
	}
	return nil
}

func (s *borrowedSuccessorState) invalidate(reason string) {
	s.mu.Lock()
	if !s.invalidated {
		s.invalidated = true
		s.invalidReason = reason
	}
	s.mu.Unlock()
}

func (s *borrowedSuccessorState) invalidReasonNow() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.invalidated {
		return ""
	}
	return s.invalidReason
}

func (s *borrowedSuccessorState) usedNow() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.used
}

func (s *borrowedSuccessorState) pauseAt(stage borrowedSuccessorStage) {
	if s.stage != nil {
		s.stage(stage)
	}
}

// borrowedSuccessorSeam is the test-only blocking notification barrier at the
// pre-probe publication stage. It is released exactly once at cleanup so a
// fatal can never strand the goroutine.
type borrowedSuccessorSeam struct {
	stage       borrowedSuccessorStage
	entered     chan struct{}
	release     chan struct{}
	enteredOnce sync.Once
	releaseOnce sync.Once
}

func installBorrowedSuccessorSeam(t *testing.T, reg *borrowedSuccessorRegistration, stage borrowedSuccessorStage) *borrowedSuccessorSeam {
	t.Helper()
	seam := &borrowedSuccessorSeam{stage: stage, entered: make(chan struct{}), release: make(chan struct{})}
	reg.state.stage = func(current borrowedSuccessorStage) {
		if current != seam.stage {
			return
		}
		seam.enteredOnce.Do(func() { close(seam.entered) })
		<-seam.release
	}
	t.Cleanup(seam.releaseSeam)
	return seam
}

func (s *borrowedSuccessorSeam) releaseSeam() {
	s.releaseOnce.Do(func() { close(s.release) })
}

// newBorrowedSuccessorBaseline establishes the genuine provenance: the real
// positive chain (probe rejected, acceptance zero), the consumed receipt entry,
// the actual captured control anchor with preserved keys/OID/fingerprint and an
// empty instance identity, the committed client-generated P1 baseline through
// the same-owner supplied-tx mechanics, and the staging helper/stat-probe
// installation needed for the strict census/OS evidence.
func newBorrowedSuccessorBaseline(t *testing.T, ctx context.Context) *borrowedSuccessorBaseline {
	t.Helper()
	return newBorrowedSuccessorBaselineWith(t, ctx, true)
}

// newBorrowedSuccessorUnconsumedBaseline is the genuine baseline variant that
// constructs the entry but deliberately never consumes it, for the
// unconsumed-entry registration negative.
func newBorrowedSuccessorUnconsumedBaseline(t *testing.T, ctx context.Context) *borrowedSuccessorBaseline {
	t.Helper()
	return newBorrowedSuccessorBaselineWith(t, ctx, false)
}

func newBorrowedSuccessorBaselineWith(t *testing.T, ctx context.Context, consume bool) *borrowedSuccessorBaseline {
	t.Helper()
	f, session, outcome := borrowedAuthDrainPositiveChain(t)
	return newBorrowedSuccessorBaselineFromChain(t, ctx, f, session, outcome, consume, false)
}

// newBorrowedSuccessorBaselineBound establishes the same genuine baseline over
// the approved BOUND fixture chain: the real first-open instance with its
// original factory binding, the real receipt/entry single consumption and the
// same-owner committed P1 rotation, preserving the authentic non-empty instance
// identity end to end for the instance-bound replacement capture lane.
func newBorrowedSuccessorBaselineBound(t *testing.T, ctx context.Context) *borrowedSuccessorBaseline {
	t.Helper()
	f, session, outcome := runBorrowedReceiptAuthEntryPositiveChainWith(t, func(t *testing.T) *borrowedAuthHandoffFixture {
		return newBorrowedAuthHandoffFixtureBound(t, &borrowedAuthHandoffBoundOptions{
			Kind: "recovery", OpenedBy: "deploy:bound-capture", Reason: "bound capture baseline",
		})
	})
	return newBorrowedSuccessorBaselineFromChain(t, ctx, f, session, outcome, true, true)
}

// newBorrowedSuccessorBaselineFromChain is the mechanical shared core of the
// successor baseline: the consumed entry, the actual captured control anchor,
// the committed client-generated P1 rotation and the staging installation. The
// default entry keeps the exact unbound chain and the empty-instance-identity
// provenance assertion byte-behavior; the bound entry additionally requires the
// authentic non-empty instance identity captured by the approved bound fixture.
func newBorrowedSuccessorBaselineFromChain(t *testing.T, ctx context.Context, f *borrowedAuthHandoffFixture, session *originGateSession, outcome borrowedReceiptHandoffOutcome, consume, bound bool) *borrowedSuccessorBaseline {
	t.Helper()
	entry, err := newBorrowedReceiptAuthEntry(f, session, outcome.handoff)
	if err != nil {
		t.Fatalf("successor baseline entry construction: %v", err)
	}
	if consume {
		enterCtx, cancelEnter := context.WithTimeout(ctx, borrowedOwnerRotationAuthBudget)
		enterErr := entry.Enter(enterCtx)
		cancelEnter()
		if enterErr != nil {
			t.Fatalf("successor baseline one-time entry: %v", enterErr)
		}
		if !outcome.handoff.Diagnostics().Consumed {
			t.Fatal("successor baseline entry did not consume the authentic one-use token")
		}
		if atomic.LoadInt32(&f.probeCalls) != 1 || atomic.LoadInt32(&f.acceptanceCalls) != 0 {
			t.Fatalf("successor baseline probe/acceptance counts: probe=%d acceptance=%d", atomic.LoadInt32(&f.probeCalls), atomic.LoadInt32(&f.acceptanceCalls))
		}
	}
	anchorCtx, cancelAnchor := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	anchor, err := f.run.CaptureControlAnchor(anchorCtx)
	cancelAnchor()
	if err != nil {
		t.Fatalf("successor baseline actual anchor capture: %v", err)
	}
	owner := anchor.Diagnostics()
	if !owner.Present || owner.Invalidated {
		t.Fatalf("successor baseline anchor absent or invalidated: %+v", owner)
	}
	binding := f.run.Binding()
	if owner.OriginalTargetKey != binding.OriginalTargetKey() || owner.ControlTargetKey != binding.ControlTargetKey() {
		t.Fatal("successor baseline anchor does not retain the original/control keys")
	}
	if owner.OriginalRoleFingerprint == "" || owner.OriginalRoleFingerprint != binding.OriginalRoleFingerprint() {
		t.Fatal("successor baseline anchor does not retain the original role fingerprint")
	}
	if bound {
		if !f.bound || f.instanceID == "" || binding.OriginalInstanceID() != f.instanceID || owner.OriginalInstanceID != f.instanceID {
			t.Fatalf("bound successor baseline lost the authentic instance identity: bound=%t instance=%q binding=%q anchor=%q", f.bound, f.instanceID, binding.OriginalInstanceID(), owner.OriginalInstanceID)
		}
	} else if owner.OriginalInstanceID != "" || binding.OriginalInstanceID() != "" {
		t.Fatal("successor baseline provenance disclosed a non-empty instance identity")
	}
	if f.writerRoleOID == 0 || f.writerRoleOID != binding.WriterRoleOID() {
		t.Fatal("successor baseline provenance lost the immutable writer role OID")
	}
	preCtx, cancelPre := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	preState, err := borrowedOwnerRotationReadRoleState(preCtx, f.fx.admin, f.writerRole)
	cancelPre()
	if err != nil {
		t.Fatalf("successor baseline pre-rotation W role state: %v", err)
	}
	passwordP1 := borrowedAuthCredential(t, "successor P1")
	verifierP1, err := generateSCRAMVerifier(passwordP1, 4096)
	if err != nil {
		t.Fatalf("successor baseline generate P1 SCRAM verifier: %v", err)
	}
	ownerPID, callbackReached, rotationErr := borrowedAuthDrainOwnerRotation(ctx, f, owner, verifierP1, preState)
	if rotationErr != nil {
		t.Fatalf("successor baseline committed P1 rotation (commit outcome is UNKNOWN, never an accepted claim): %v", rotationErr)
	}
	if callbackReached != 1 || ownerPID != owner.BackendPID {
		t.Fatalf("successor baseline rotation callback/owner mismatch: callback=%d ownerPID=%d anchorPID=%d", callbackReached, ownerPID, owner.BackendPID)
	}
	postCtx, cancelPost := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	postState, postErr := borrowedOwnerRotationReadRoleState(postCtx, f.fx.admin, f.writerRole)
	cancelPost()
	if postErr != nil {
		t.Fatalf("successor baseline committed state read: %v", postErr)
	}
	if subtle.ConstantTimeCompare([]byte(postState.verifier), []byte(verifierP1)) != 1 {
		t.Fatal("successor baseline committed verifier is not the exact generated P1 verifier")
	}
	borrowedOwnerRotationAssertRoleStateUnchanged(t, "successor baseline", preState, postState)
	borrowedAuthDrainInstallHelpers(t, ctx, f)
	return &borrowedSuccessorBaseline{
		fixture: f, entry: entry, anchor: anchor, owner: owner,
		preState: preState, verifierP1: verifierP1, passwordP1: passwordP1,
	}
}

// borrowedSuccessorConnectP1Raw retains ONE authenticated P1 connection with
// an immediate independent bounded cleanup. It does not register it.
func borrowedSuccessorConnectP1Raw(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline) *pgx.Conn {
	t.Helper()
	p1DSN := borrowedAuthRoleDSN(t, b.fixture.writerTargetDSN, b.fixture.writerRole, b.passwordP1)
	connectCtx, cancelConnect := context.WithTimeout(ctx, borrowedOwnerRotationAuthBudget)
	conn, err := pgx.Connect(connectCtx, p1DSN)
	cancelConnect()
	if err != nil {
		t.Fatalf("successor P1 connect refused (sqlstate=%s)", borrowedAuthSQLState(err))
	}
	t.Cleanup(func() {
		cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), borrowedOwnerRotationCleanupBudget)
		_ = conn.Close(cleanupCtx)
		cancelCleanup()
	})
	return conn
}

// borrowedSuccessorConnectP1 retains ONE authenticated P1 connection with an
// immediate independent bounded cleanup and registers it as the successor.
func borrowedSuccessorConnectP1(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline) (*pgx.Conn, *borrowedSuccessorRegistration) {
	t.Helper()
	conn := borrowedSuccessorConnectP1Raw(t, ctx, b)
	reg, err := newBorrowedSuccessorRegistration(t, ctx, b.fixture, b.entry, b.anchor, conn, b.verifierP1, b.preState)
	if err != nil {
		t.Fatalf("successor registration refused: %v", err)
	}
	return conn, reg
}

// borrowedSuccessorCloseConn closes one retained connection with an independent
// bounded cleanup context.
func borrowedSuccessorCloseConn(t *testing.T, conn *pgx.Conn) {
	t.Helper()
	closeCtx, cancelClose := context.WithTimeout(context.Background(), borrowedOwnerRotationCleanupBudget)
	closeErr := conn.Close(closeCtx)
	cancelClose()
	if closeErr != nil {
		t.Fatalf("successor connection close refused: %v", closeErr)
	}
}

// borrowedSuccessorRetire closes the successor, proves its backend incarnation
// strictly gone, and re-proves the original owner health, the deliberate
// non-clean guard and zero acceptance.
func borrowedSuccessorRetire(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, conn *pgx.Conn, state *borrowedSuccessorState) {
	t.Helper()
	borrowedSuccessorCloseConn(t, conn)
	borrowedAuthStagingAwaitDisappearance(ctx, t, b.fixture.fx.containerID, state.backendPID, state.osStart, borrowedAuthStagingDisappearanceBudget, true)
	healthCtx, cancelHealth := context.WithTimeout(ctx, borrowedOwnerRotationHealthBudget)
	healthErr := b.fixture.lock.Health(healthCtx)
	cancelHealth()
	if healthErr != nil {
		t.Fatalf("successor retire original control owner health: %v", healthErr)
	}
	var disposition string
	guardCtx, cancelGuard := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	guardErr := b.fixture.controlPool.QueryRow(guardCtx, `SELECT disposition FROM recovery_target_guard WHERE target_guard_key=$1`, b.fixture.guardKey).Scan(&disposition)
	cancelGuard()
	if guardErr != nil {
		t.Fatalf("successor retire guard disposition: %v", guardErr)
	}
	if disposition == "clean" {
		t.Fatal("successor lane changed the deliberate non-clean guard")
	}
	if atomic.LoadInt32(&b.fixture.acceptanceCalls) != 0 {
		t.Fatalf("successor lane ran acceptance %d times", atomic.LoadInt32(&b.fixture.acceptanceCalls))
	}
	t.Logf("successor retired: backend PID %d incarnation strictly gone, owner health retained, guard non-clean, acceptance zero", state.backendPID)
}

// borrowedSuccessorWrongRoleDBRefuse proves a real connection with the wrong
// role and a real connection with the wrong database are both refused by the
// registration (non-destructive, shares the main fixture).
func borrowedSuccessorWrongRoleDBRefuse(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline) {
	t.Helper()
	connect := func(dsn string) *pgx.Conn {
		connectCtx, cancelConnect := context.WithTimeout(ctx, borrowedOwnerRotationAuthBudget)
		conn, err := pgx.Connect(connectCtx, dsn)
		cancelConnect()
		if err != nil {
			t.Fatalf("successor wrong-identity connect refused (sqlstate=%s)", borrowedAuthSQLState(err))
		}
		t.Cleanup(func() {
			cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), borrowedOwnerRotationCleanupBudget)
			_ = conn.Close(cleanupCtx)
			cancelCleanup()
		})
		return conn
	}
	observerConn := connect(b.fixture.observerTargetDSN)
	_, roleErr := newBorrowedSuccessorRegistration(t, ctx, b.fixture, b.entry, b.anchor, observerConn, b.verifierP1, b.preState)
	if roleErr == nil || !strings.Contains(roleErr.Error(), "role/database is not the W target") {
		t.Fatalf("wrong-role connection was not refused specifically: %v", roleErr)
	}
	sourceConn := connect(borrowedAuthRoleDSN(t, b.fixture.writerSourceDSN, b.fixture.writerRole, b.passwordP1))
	_, dbErr := newBorrowedSuccessorRegistration(t, ctx, b.fixture, b.entry, b.anchor, sourceConn, b.verifierP1, b.preState)
	if dbErr == nil || !strings.Contains(dbErr.Error(), "role/database is not the W target") {
		t.Fatalf("wrong-database connection was not refused specifically: %v", dbErr)
	}
	borrowedSuccessorCloseConn(t, observerConn)
	borrowedSuccessorCloseConn(t, sourceConn)
	t.Logf("wrong-role/wrong-DB negative: observer-on-target and W-on-source connections were refused by the registration")
}

// borrowedSuccessorUnknownRefuse proves injected unknown strict stat and census
// failures refuse with zero probe executions and that failed copies cannot
// rehabilitate (non-destructive, shares the main fixture).
func borrowedSuccessorUnknownRefuse(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline) {
	t.Helper()
	for _, kind := range []string{"stat", "census"} {
		conn, reg := borrowedSuccessorConnectP1(t, ctx, b)
		kind := kind
		if kind == "stat" {
			reg.state.statFn = func(context.Context, *borrowedSuccessorState) (string, uint64, error) {
				return "", 0, errors.New("injected unknown strict stat failure")
			}
		} else {
			reg.state.associateFn = func(context.Context, *borrowedSuccessorState) (borrowedAuthStagingBackendToken, error) {
				return borrowedAuthStagingBackendToken{}, errors.New("injected unknown strict census failure")
			}
		}
		useCtx, cancelUse := context.WithTimeout(ctx, 30*time.Second)
		useErr := reg.Use(useCtx)
		cancelUse()
		if useErr == nil {
			t.Fatalf("unknown %s failure was accepted as a successor use", kind)
		}
		if errors.Is(useErr, errBorrowedSuccessorVerifierDrift) || errors.Is(useErr, errBorrowedSuccessorRoleFactsDrift) {
			t.Fatalf("unknown %s failure was misclassified as drift: %v", kind, useErr)
		}
		if got := atomic.LoadInt32(&reg.state.probeExecutions); got != 0 {
			t.Fatalf("unknown %s failure executed the probe %d times, want 0", kind, got)
		}
		copied := *reg
		if copied.Use(ctx) == nil {
			t.Fatalf("failed %s copy rehabilitated the successor registration", kind)
		}
		// The connection is still live, but the shared binding slot is
		// permanently invalidated: a reconstruction must refuse and never mint
		// fresh state.
		if _, liveErr := borrowedSuccessorReadConnIdentity(ctx, conn); liveErr != nil {
			t.Fatalf("unknown %s negative connection is not live before reconstruction: %v", kind, liveErr)
		}
		if _, reconErr := newBorrowedSuccessorRegistration(t, ctx, b.fixture, b.entry, b.anchor, conn, b.verifierP1, b.preState); reconErr == nil {
			t.Fatalf("post-unknown %s reconstruction minted a fresh successor registration", kind)
		}
		entryCopy := *b.entry
		wrapperCopy := &entryCopy
		if _, wrapperErr := newBorrowedSuccessorRegistration(t, ctx, b.fixture, wrapperCopy, b.anchor, conn, b.verifierP1, b.preState); wrapperErr == nil {
			t.Fatalf("entry-wrapper-copy reconstruction after unknown %s loss minted a fresh successor registration", kind)
		}
		borrowedSuccessorCloseConn(t, conn)
	}
	t.Logf("unknown census/stat negative: injected unknown strict stat and census failures refused with zero probe executions; failed copies refused")
}

// borrowedSuccessorCanceledWaiterRefuse proves the caller-bounded slot
// acquisition: a short caller deadline that expires while the binding slot is
// held refuses promptly with a context-end error and publishes no late
// registration; a live reconstruction afterwards publishes exactly one state.
func borrowedSuccessorCanceledWaiterRefuse(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline) {
	t.Helper()
	conn := borrowedSuccessorConnectP1Raw(t, ctx, b)
	key := borrowedSuccessorBindingKey{fixture: b.fixture, entryState: b.entry.state, anchor: b.anchor, conn: conn, verifier: b.verifierP1}
	slotAny, _ := borrowedSuccessorSlots.LoadOrStore(key, &borrowedSuccessorSlot{})
	slot := slotAny.(*borrowedSuccessorSlot)
	slot.mu.Lock()
	released := false
	defer func() {
		if !released {
			slot.mu.Unlock()
		}
	}()
	waiterCtx, cancelWaiter := context.WithTimeout(ctx, 2*time.Second)
	done := make(chan error, 1)
	go func() {
		_, err := newBorrowedSuccessorRegistration(t, waiterCtx, b.fixture, b.entry, b.anchor, conn, b.verifierP1, b.preState)
		done <- err
	}()
	// Cancel-and-bounded-join cleanup registered immediately after the launch.
	joined := false
	t.Cleanup(func() {
		cancelWaiter()
		if joined {
			return
		}
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Errorf("canceled slot waiter completion is unknown after the bounded join")
		}
	})
	var waitErr error
	select {
	case waitErr = <-done:
		joined = true
	case <-time.After(30 * time.Second):
		t.Errorf("canceled slot waiter did not complete within the bounded join; outcome unknown")
		return
	}
	if waitErr == nil {
		t.Fatal("constructor with an expired deadline while the slot was held was accepted")
	}
	if !strings.Contains(waitErr.Error(), "caller context ended") {
		t.Fatalf("canceled slot waiter was not refused with a context-end error: %v", waitErr)
	}
	slot.mu.Unlock()
	released = true
	if slot.state != nil {
		t.Fatal("canceled slot waiter published a late registration")
	}
	liveReg, liveErr := newBorrowedSuccessorRegistration(t, ctx, b.fixture, b.entry, b.anchor, conn, b.verifierP1, b.preState)
	if liveErr != nil {
		t.Fatalf("live reconstruction after the canceled waiter refused: %v", liveErr)
	}
	if liveReg.state == nil {
		t.Fatal("live reconstruction after the canceled waiter published no state")
	}
	borrowedSuccessorCloseConn(t, conn)
	t.Logf("canceled-waiter negative: the short deadline expired while the binding slot was held; the waiter refused promptly with no late registration and a live reconstruction published exactly one state")
}

// borrowedSuccessorUnconsumedEntryRefuse proves a genuine but unconsumed entry
// refuses registration after a real P1 rotation and a real P1 connect.
func borrowedSuccessorUnconsumedEntryRefuse(t *testing.T, ctx context.Context) {
	t.Helper()
	b := newBorrowedSuccessorUnconsumedBaseline(t, ctx)
	conn := borrowedSuccessorConnectP1Raw(t, ctx, b)
	_, regErr := newBorrowedSuccessorRegistration(t, ctx, b.fixture, b.entry, b.anchor, conn, b.verifierP1, b.preState)
	if regErr == nil || !strings.Contains(regErr.Error(), "never consumed") {
		t.Fatalf("unconsumed entry was not refused by the registration: %v", regErr)
	}
	borrowedSuccessorCloseConn(t, conn)
	t.Logf("unconsumed-entry negative: genuine entry without Enter, real P1 rotation and connect; registration refused the unconsumed entry")
}

// borrowedSuccessorCancelRefuse proves a cancel at the deterministic pre-probe
// publication barrier refuses with zero probe executions and that the failed
// copy cannot rehabilitate (non-destructive, shares the main fixture).
func borrowedSuccessorCancelRefuse(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline) {
	t.Helper()
	conn, reg := borrowedSuccessorConnectP1(t, ctx, b)
	seam := installBorrowedSuccessorSeam(t, reg, borrowedSuccessorStageUsePublish)
	useCtx, cancelUse := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- reg.Use(useCtx) }()
	// Cancel-and-bounded-join cleanup registered IMMEDIATELY after the launch:
	// cancel, release the seam, bounded join, and only then (LIFO, because the
	// connection cleanup was registered earlier) the connection cleanup may
	// close the retained connection. A join timeout preserves the unknown
	// outcome as a failure instead of a Fatal without established completion.
	joined := false
	t.Cleanup(func() {
		cancelUse()
		seam.releaseSeam()
		if joined {
			return
		}
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Errorf("pre-probe cancel use completion is unknown after the bounded join")
		}
	})
	select {
	case <-seam.entered:
	case <-time.After(60 * time.Second):
		t.Errorf("successor use never reached the pre-probe publication barrier; completion is unknown")
		return
	}
	cancelUse()
	seam.releaseSeam()
	select {
	case useErr := <-done:
		joined = true
		if useErr == nil {
			t.Fatal("canceled pre-probe successor use published success")
		}
		if errors.Is(useErr, errBorrowedSuccessorVerifierDrift) || errors.Is(useErr, errBorrowedSuccessorRoleFactsDrift) {
			t.Fatalf("pre-probe cancel was misclassified as drift: %v", useErr)
		}
	case <-time.After(30 * time.Second):
		t.Errorf("canceled successor use did not complete within the bounded join; outcome unknown")
		return
	}
	if got := atomic.LoadInt32(&reg.state.probeExecutions); got != 0 {
		t.Fatalf("pre-probe cancel executed the probe %d times, want 0", got)
	}
	if _, reconErr := newBorrowedSuccessorRegistration(t, ctx, b.fixture, b.entry, b.anchor, conn, b.verifierP1, b.preState); reconErr == nil {
		t.Fatal("post-cancel reconstruction minted a fresh successor registration")
	}
	copied := *reg
	if copied.Use(ctx) == nil {
		t.Fatal("failed pre-probe cancel copy rehabilitated the successor registration")
	}
	borrowedSuccessorCloseConn(t, conn)
	t.Logf("pre-probe cancel negative: the canceled barrier use refused with zero probe executions; reconstruction and the failed copy refused")
}

// borrowedSuccessorTerminationSubstitutionRefuse proves the registered
// successor termination refuses use with zero probe executions, and that
// another real same-W/target connection cannot substitute for the terminated
// registered successor (fresh destructive fixture).
func borrowedSuccessorTerminationSubstitutionRefuse(t *testing.T, ctx context.Context) {
	t.Helper()
	b := newBorrowedSuccessorBaseline(t, ctx)
	connA, regA := borrowedSuccessorConnectP1(t, ctx, b)
	connB, regB := borrowedSuccessorConnectP1(t, ctx, b)
	identityB, err := borrowedSuccessorReadConnIdentity(ctx, connB)
	if err != nil {
		t.Fatalf("substitution negative lookalike identity read: %v", err)
	}
	if identityB.backendPID == regA.state.backendPID {
		t.Fatal("substitution negative lookalike shares the registered successor PID")
	}
	var terminated bool
	termCtx, cancelTerm := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	termErr := b.fixture.fx.admin.QueryRow(termCtx, `SELECT pg_terminate_backend($1)`, regA.state.backendPID).Scan(&terminated)
	cancelTerm()
	if termErr != nil || !terminated {
		t.Fatalf("terminate registered successor refused: terminated=%t err=%v", terminated, termErr)
	}
	goneDeadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(goneDeadline) {
		goneCtx, cancelGone := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
		goneState, goneStart, goneErr := borrowedAuthStagingContainerStat(goneCtx, b.fixture.fx.containerID, regA.state.backendPID)
		cancelGone()
		if goneErr != nil || goneStart != regA.state.osStart || !borrowedAuthStagingLiveState(goneState) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	useCtx, cancelUse := context.WithTimeout(ctx, 60*time.Second)
	useErr := regA.Use(useCtx)
	cancelUse()
	if useErr == nil {
		t.Fatal("terminated successor was used successfully")
	}
	if errors.Is(useErr, errBorrowedSuccessorVerifierDrift) || errors.Is(useErr, errBorrowedSuccessorRoleFactsDrift) {
		t.Fatalf("successor termination was misclassified as drift: %v", useErr)
	}
	if got := atomic.LoadInt32(&regA.state.probeExecutions); got != 0 {
		t.Fatalf("terminated successor executed the probe %d times, want 0", got)
	}
	lookalikeCtx, cancelLookalike := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	lookalikeState, lookalikeStart, lookalikeErr := borrowedAuthStagingContainerStat(lookalikeCtx, b.fixture.fx.containerID, identityB.backendPID)
	cancelLookalike()
	if lookalikeErr != nil || lookalikeStart != regB.state.osStart || !borrowedAuthStagingLiveState(lookalikeState) {
		t.Fatalf("lookalike successor is not the live real connection: state=%q err=%v", lookalikeState, lookalikeErr)
	}
	copied := *regA
	if copied.Use(ctx) == nil {
		t.Fatal("terminated successor copy rehabilitated the registration")
	}
	borrowedSuccessorCloseConn(t, connA)
	borrowedSuccessorCloseConn(t, connB)
	t.Logf("termination/substitution negative: registered successor termination refused use with zero probe executions and the live real same-W/target lookalike could not substitute")
}

// borrowedSuccessorOwnerLossRefuse proves real control owner loss refuses use
// with zero probe executions and no replacement owner (fresh destructive
// fixture).
func borrowedSuccessorOwnerLossRefuse(t *testing.T, ctx context.Context) {
	t.Helper()
	b := newBorrowedSuccessorBaseline(t, ctx)
	conn, reg := borrowedSuccessorConnectP1(t, ctx, b)
	controlPID := b.fixture.run.Binding().ControlBackendPID()
	if controlPID <= 0 || controlPID != b.owner.BackendPID {
		t.Fatalf("successor owner-loss captured control owner PID %d is missing or not the anchored owner PID %d", controlPID, b.owner.BackendPID)
	}
	var terminated bool
	lossCtx, cancelLoss := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	lossErr := b.fixture.controlPool.QueryRow(lossCtx, `SELECT pg_terminate_backend($1)`, controlPID).Scan(&terminated)
	cancelLoss()
	if lossErr != nil || !terminated {
		t.Fatalf("terminate successor-lane control owner: terminated=%t err=%v", terminated, lossErr)
	}
	ownerGoneDeadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(ownerGoneDeadline) {
		var present int
		rowCtx, cancelRow := context.WithTimeout(ctx, 5*time.Second)
		rowErr := b.fixture.controlPool.QueryRow(rowCtx, `SELECT count(*) FROM pg_stat_activity WHERE pid=$1`, controlPID).Scan(&present)
		cancelRow()
		if rowErr != nil || present == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	useCtx, cancelUse := context.WithTimeout(ctx, 60*time.Second)
	useErr := reg.Use(useCtx)
	cancelUse()
	if useErr == nil {
		t.Fatal("successor use succeeded after real control owner loss")
	}
	if errors.Is(useErr, errBorrowedSuccessorVerifierDrift) || errors.Is(useErr, errBorrowedSuccessorRoleFactsDrift) {
		t.Fatalf("owner loss was misclassified as drift: %v", useErr)
	}
	if got := atomic.LoadInt32(&reg.state.probeExecutions); got != 0 {
		t.Fatalf("owner loss executed the probe %d times, want 0", got)
	}
	healthCtx, cancelHealth := context.WithTimeout(ctx, borrowedOwnerRotationHealthBudget)
	healthErr := b.fixture.lock.Health(healthCtx)
	cancelHealth()
	if healthErr == nil {
		t.Fatal("original control owner health remained successful after real owner loss")
	}
	anchorCtx, cancelAnchor := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	anchorErr := b.anchor.Recheck(anchorCtx)
	cancelAnchor()
	if anchorErr == nil {
		t.Fatal("retained anchor rechecked after real owner loss")
	}
	copied := *reg
	if copied.Use(ctx) == nil {
		t.Fatal("owner-loss copy rehabilitated the successor registration")
	}
	borrowedSuccessorCloseConn(t, conn)
	t.Logf("owner-loss negative: real control owner loss refused successor use with zero probe executions, no replacement owner and no retry")
}

// borrowedSuccessorVerifierDriftRefuse proves a committed verifier drift with
// unchanged role facts is classified with the specific verifier-drift error and
// zero probe executions (fresh destructive fixture).
func borrowedSuccessorVerifierDriftRefuse(t *testing.T, ctx context.Context) {
	t.Helper()
	b := newBorrowedSuccessorBaseline(t, ctx)
	conn, reg := borrowedSuccessorConnectP1(t, ctx, b)
	gapCtx, cancelGap := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	_, gapErr := b.fixture.fx.admin.Exec(gapCtx, `ALTER ROLE `+pgx.Identifier{b.fixture.writerRole}.Sanitize()+` PASSWORD `+sqlLiteral(b.preState.verifier))
	cancelGap()
	if gapErr != nil {
		t.Fatalf("successor verifier drift mutation refused: %v", gapErr)
	}
	current, err := borrowedSuccessorReadCatalogFacts(ctx, b.fixture)
	if err != nil {
		t.Fatalf("successor verifier drift exact catalog read: %v", err)
	}
	if subtle.ConstantTimeCompare([]byte(current.verifier), []byte(b.preState.verifier)) != 1 {
		t.Fatal("successor verifier drift mutation is not the exact captured P0 verifier")
	}
	useCtx, cancelUse := context.WithTimeout(ctx, 60*time.Second)
	useErr := reg.Use(useCtx)
	cancelUse()
	if !errors.Is(useErr, errBorrowedSuccessorVerifierDrift) {
		t.Fatalf("verifier drift was not refused with the specific verifier-drift error: %v", useErr)
	}
	if got := atomic.LoadInt32(&reg.state.probeExecutions); got != 0 {
		t.Fatalf("verifier drift executed the probe %d times, want 0", got)
	}
	copied := *reg
	if copied.Use(ctx) == nil {
		t.Fatal("verifier-drift copy rehabilitated the successor registration")
	}
	borrowedSuccessorCloseConn(t, conn)
	t.Logf("verifier-drift negative: committed verifier drift with unchanged role facts refused with the specific verifier-drift error and zero probe executions")
}

// borrowedSuccessorRoleFactsDriftRefuse proves a committed role-fact drift
// (CREATEDB) with the verifier still at the expected P1 value is classified
// with the specific role-facts drift error, never the verifier-drift error, and
// with zero probe executions (fresh destructive fixture).
func borrowedSuccessorRoleFactsDriftRefuse(t *testing.T, ctx context.Context) {
	t.Helper()
	b := newBorrowedSuccessorBaseline(t, ctx)
	conn, reg := borrowedSuccessorConnectP1(t, ctx, b)
	driftCtx, cancelDrift := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	_, driftErr := b.fixture.fx.admin.Exec(driftCtx, `ALTER ROLE `+pgx.Identifier{b.fixture.writerRole}.Sanitize()+` CREATEDB`)
	cancelDrift()
	if driftErr != nil {
		t.Fatalf("successor role-facts drift mutation refused: %v", driftErr)
	}
	current, err := borrowedSuccessorReadCatalogFacts(ctx, b.fixture)
	if err != nil {
		t.Fatalf("successor role-facts drift exact catalog read: %v", err)
	}
	if current.createdb == b.preState.createdb {
		t.Fatal("successor role-facts drift mutation did not change the flag")
	}
	if subtle.ConstantTimeCompare([]byte(current.verifier), []byte(b.verifierP1)) != 1 {
		t.Fatal("successor role-facts drift negative changed the committed P1 verifier")
	}
	useCtx, cancelUse := context.WithTimeout(ctx, 60*time.Second)
	useErr := reg.Use(useCtx)
	cancelUse()
	if !errors.Is(useErr, errBorrowedSuccessorRoleFactsDrift) {
		t.Fatalf("role-facts drift was not refused with the specific role-facts drift error: %v", useErr)
	}
	if errors.Is(useErr, errBorrowedSuccessorVerifierDrift) {
		t.Fatal("role-facts drift was misclassified as verifier drift")
	}
	if got := atomic.LoadInt32(&reg.state.probeExecutions); got != 0 {
		t.Fatalf("role-facts drift executed the probe %d times, want 0", got)
	}
	copied := *reg
	if copied.Use(ctx) == nil {
		t.Fatal("role-facts-drift copy rehabilitated the successor registration")
	}
	borrowedSuccessorCloseConn(t, conn)
	t.Logf("role-facts-drift negative: committed CREATEDB drift with the expected P1 verifier refused with the specific role-facts drift error and zero probe executions")
}

// TestBorrowedSuccessorProbeRegistration is the bounded read-only P1 successor
// registration/use lane described in the file header. It grants no continuous
// fence, no DDL, no restore, no rebuild, no acceptance, no manifest, no
// downstream and no Gate1 authority; identity-discovery queries are counted
// separately from probe executions; every failure is permanent and shared.
func TestBorrowedSuccessorProbeRegistration(t *testing.T) {
	ctx := t.Context()
	b := newBorrowedSuccessorBaseline(t, ctx)

	// Build: retain ONE authenticated P1 connection with immediate independent
	// bounded cleanup and register it as the successor.
	conn, reg := borrowedSuccessorConnectP1(t, ctx, b)
	state := reg.state

	// Equivalent entry wrapper: the registry key uses the SHARED entry state,
	// so a wrapper copy over the same fixture/anchor/conn/baseline reuses the
	// exact same slot state.
	entryCopy := *b.entry
	wrapperCopy := &entryCopy
	equivalent, equivalentErr := newBorrowedSuccessorRegistration(t, ctx, b.fixture, wrapperCopy, b.anchor, conn, b.verifierP1, b.preState)
	if equivalentErr != nil {
		t.Fatalf("equivalent entry wrapper was not reused: %v", equivalentErr)
	}
	if equivalent.state != state {
		t.Fatal("equivalent entry wrapper did not reuse the shared slot state")
	}

	// Use: one read-only successor probe.
	useCtx, cancelUse := context.WithTimeout(ctx, 60*time.Second)
	useErr := reg.Use(useCtx)
	cancelUse()
	if useErr != nil {
		t.Fatalf("successor use refused: %v", useErr)
	}
	if got := atomic.LoadInt32(&state.probeExecutions); got != 1 {
		t.Fatalf("successor probe executions=%d, want exactly 1", got)
	}
	if got := atomic.LoadInt32(&state.identityQueries); got == 0 {
		t.Fatal("successor identity-discovery queries were not counted separately")
	}
	t.Logf("successor registered+used: backend PID %d probe executions=%d identity-discovery queries=%d", state.backendPID, atomic.LoadInt32(&state.probeExecutions), atomic.LoadInt32(&state.identityQueries))

	// Exactly-one probe and replay refusal (same and copy), no new probe.
	replayCtx, cancelReplay := context.WithTimeout(ctx, 30*time.Second)
	replayErr := reg.Use(replayCtx)
	copied := *reg
	copyErr := copied.Use(replayCtx)
	cancelReplay()
	if replayErr == nil || copyErr == nil {
		t.Fatal("successor replay/copy use was accepted")
	}
	if got := atomic.LoadInt32(&state.probeExecutions); got != 1 {
		t.Fatalf("successor replay executed the probe: executions=%d", got)
	}
	// Constructor replay after a successful Use: the shared binding slot reuses
	// the consumed state and refuses to mint fresh state, including through an
	// equivalent entry wrapper.
	if _, replayRegErr := newBorrowedSuccessorRegistration(t, ctx, b.fixture, b.entry, b.anchor, conn, b.verifierP1, b.preState); replayRegErr == nil {
		t.Fatal("constructor replay after a successful use minted a fresh successor registration")
	}
	if _, wrapperReplayErr := newBorrowedSuccessorRegistration(t, ctx, b.fixture, wrapperCopy, b.anchor, conn, b.verifierP1, b.preState); wrapperReplayErr == nil {
		t.Fatal("entry-wrapper-copy reconstruction after a successful use minted a fresh successor registration")
	}

	// Retire: close, prove backend incarnation gone, owner health, guard
	// non-clean and acceptance zero.
	borrowedSuccessorRetire(t, ctx, b, conn, state)

	// Non-destructive negatives on the same fixture.
	borrowedSuccessorWrongRoleDBRefuse(t, ctx, b)
	borrowedSuccessorUnknownRefuse(t, ctx, b)
	borrowedSuccessorCancelRefuse(t, ctx, b)
	borrowedSuccessorCanceledWaiterRefuse(t, ctx, b)

	// Destructive negatives on fresh fixtures.
	borrowedSuccessorUnconsumedEntryRefuse(t, ctx)
	borrowedSuccessorTerminationSubstitutionRefuse(t, ctx)
	borrowedSuccessorOwnerLossRefuse(t, ctx)
	borrowedSuccessorVerifierDriftRefuse(t, ctx)
	borrowedSuccessorRoleFactsDriftRefuse(t, ctx)
}
