//go:build linux && drill

// borrowed-replacement-session-registration_linux_test.go is the bounded
// replacement-bound retained-session registration/use lane. It starts from a
// GENUINE replacement-bound identity capture (the actual bounded
// borrowedReplacementBindingOrchestrate path, never an outcome/OID projection),
// retains ONE actual P1 connection with a short child connect and an immediate
// independent bounded cleanup, registers that exact connection against the
// fresh replacement prefix and the original control anchor with the strict
// census/OS identity evidence, uses it exactly once (one fixed SELECT 1 on the
// SAME connection, bracketed by identity rechecks, with identity-discovery
// queries counted separately), and retires it by closing the connection and
// proving the exact backend incarnation strictly gone with the original owner
// health, the deliberate non-clean guard and zero acceptance. The registry is
// keyed by the SHARED replacement-prefix state (never wrapper addresses), so
// copies and reconstructions share one-use and permanent-UNKNOWN loss. There is
// no continuous fence, no restore, no rebuild, no acceptance, no manifest, no
// downstream and no Gate1 authority; the fresh Run is never invoked and no
// child is ever launched. Secrets, verifiers and DSNs are never logged.
package recovery_test

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xtianxx/txharbor/internal/recovery"
)

// errBorrowedReplacementSessionVerifierDrift is the distinct refusal of the
// fresh catalog comparison when the committed verifier no longer equals the
// expected P1 verifier while the W role facts are unchanged.
var errBorrowedReplacementSessionVerifierDrift = errors.New("replacement session catalog verifier drifted from the registered P1 baseline")

// errBorrowedReplacementSessionRoleFactsDrift is the distinct refusal of the
// fresh catalog comparison when the immutable W role facts changed.
var errBorrowedReplacementSessionRoleFactsDrift = errors.New("replacement session catalog W role facts drifted")

// borrowedReplacementSessionStage is the deterministic publication barrier of
// the one-use replacement session: the pre-probe stage refuses before any probe
// execution, the publication stage refuses after the single probe and before
// the one-use publication. It is a test-only notification/barrier seam and
// grants nothing.
type borrowedReplacementSessionStage string

const (
	borrowedReplacementSessionStageUsePublish     borrowedReplacementSessionStage = "use-publish"
	borrowedReplacementSessionStageUsePublication borrowedReplacementSessionStage = "use-publication"
)

// borrowedReplacementSessionState is the copy-shared one-use/permanent-loss
// state of one registered replacement session. Copies of a registration share
// this pointer, so a genuine failure permanently invalidates every copy; there
// is no reset or reconstruction rehabilitation for copies.
type borrowedReplacementSessionState struct {
	mu            sync.Mutex
	invalidated   bool
	invalidReason string
	using         bool
	used          bool

	fixture *borrowedAuthHandoffFixture
	prefix  *borrowedIdentityPrefix
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

	replacementOID uint32

	expectedVerifier string
	expectedState    borrowedOwnerRotationRoleState

	probeExecutions int32
	identityQueries int32

	// stage is the optional test-only publication barrier; statFn and
	// associateFn are test-only injectable seams for the unknown-evidence
	// negatives. They are nil in real runs.
	stage       func(borrowedReplacementSessionStage)
	statFn      func(context.Context, *borrowedReplacementSessionState) (string, uint64, error)
	associateFn func(context.Context, *borrowedReplacementSessionState) (borrowedAuthStagingBackendToken, error)
}

// borrowedReplacementSessionRegistration is the private one-use replacement
// session capability. It exposes no public authority and only ever returns
// errors.
type borrowedReplacementSessionRegistration struct {
	state *borrowedReplacementSessionState
}

// borrowedReplacementSessionBindingKey is the concrete registration binding:
// the exact fixture, the SHARED replacement-prefix state pointer (never a
// wrapper address), the retained connection, the verified replacement OID and
// the expected committed P1 verifier. All fields are comparable, so the key can
// be used for the shared slot registry and an equivalent prefix wrapper over
// the same shared state reuses or refuses the same slot state.
type borrowedReplacementSessionBindingKey struct {
	fixture        *borrowedAuthHandoffFixture
	prefixState    *borrowedIdentityState
	conn           *pgx.Conn
	replacementOID uint32
	verifier       string
}

// borrowedReplacementSessionSlot is the shared registration slot of one
// concrete binding. The registry is test-process-local; a reconstruction over
// the same binding can only reuse the existing state (or refuse a
// consumed/invalidated one) and can never mint fresh state.
type borrowedReplacementSessionSlot struct {
	mu    sync.Mutex
	state *borrowedReplacementSessionState
}

var borrowedReplacementSessionSlots sync.Map

// borrowedReplacementSessionAcquireSlot acquires the binding slot with a
// caller-bounded wait: TryLock plus a bounded ticker that selects the caller
// context, so a held slot can never block a caller past its own deadline and no
// locker goroutine is abandoned.
func borrowedReplacementSessionAcquireSlot(ctx context.Context, slot *borrowedReplacementSessionSlot) error {
	if ctx == nil || slot == nil {
		return errors.New("replacement session slot acquisition requires a caller context and the binding slot")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("replacement session slot acquisition refused: caller context already done: %w", err)
	}
	if slot.mu.TryLock() {
		return nil
	}
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("replacement session slot acquisition refused: caller context ended while the binding slot was held: %w", ctx.Err())
		case <-ticker.C:
			if slot.mu.TryLock() {
				return nil
			}
		}
	}
}

// borrowedReplacementSessionConnectP1 retains ONE authenticated P1 connection
// through the private P1 DSN derived with the shared role-DSN pattern (the
// original fixture DSNs are never rewritten), with a short child connect and an
// immediate independent bounded cleanup.
func borrowedReplacementSessionConnectP1(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline) *pgx.Conn {
	t.Helper()
	p1DSN := borrowedReplacementP1TargetDSN(t, b)
	connectCtx, cancelConnect := context.WithTimeout(ctx, borrowedOwnerRotationAuthBudget)
	conn, err := pgx.Connect(connectCtx, p1DSN)
	cancelConnect()
	if err != nil {
		t.Fatalf("replacement session P1 connect refused (sqlstate=%s)", borrowedAuthSQLState(err))
	}
	t.Cleanup(func() {
		cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), borrowedOwnerRotationCleanupBudget)
		_ = conn.Close(cleanupCtx)
		cancelCleanup()
	})
	return conn
}

// borrowedReplacementSessionStat is the bounded strict OS identity stat of the
// retained session, with the test-only injection seam.
func borrowedReplacementSessionStat(ctx context.Context, s *borrowedReplacementSessionState) (string, uint64, error) {
	if s.statFn != nil {
		return s.statFn(ctx, s)
	}
	statCtx, cancelStat := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	defer cancelStat()
	return borrowedAuthStagingContainerStat(statCtx, s.fixture.fx.containerID, s.backendPID)
}

// borrowedReplacementSessionAssociate is the bounded exact tuple association of
// the retained session, with the test-only injection seam.
func borrowedReplacementSessionAssociate(ctx context.Context, s *borrowedReplacementSessionState) (borrowedAuthStagingBackendToken, error) {
	if s.associateFn != nil {
		return s.associateFn(ctx, s)
	}
	return borrowedSuccessorAssociateToken(ctx, s.fixture, s.serverLocal, s.clientLocal)
}

// borrowedReplacementSessionRevalidate revalidates the exact retained session
// identity in a fixed order: the strict OS stat of the registered PID/start
// first (a terminated backend refuses immediately and no live lookalike can
// substitute), then the complete strict census tuple association, then the
// strict backend inspection with the retained socket inode, then the
// same-connection identity read. Every read is caller-bounded and counted as an
// identity-discovery query, never as a probe execution.
func borrowedReplacementSessionRevalidate(ctx context.Context, s *borrowedReplacementSessionState) error {
	osState, osStart, statErr := borrowedReplacementSessionStat(ctx, s)
	atomic.AddInt32(&s.identityQueries, 1)
	if statErr != nil || osStart != s.osStart || !borrowedAuthStagingLiveState(osState) {
		return errors.New("replacement session retained OS identity is not the live registered backend")
	}
	token, err := borrowedReplacementSessionAssociate(ctx, s)
	atomic.AddInt32(&s.identityQueries, 1)
	if err != nil {
		return err
	}
	if token.ChildPID != s.backendPID || token.ChildStart != s.osStart || token.Inode != s.socketInode ||
		token.Local != s.serverLocal || token.Remote != s.clientLocal {
		return errors.New("replacement session strict census token changed")
	}
	inspectCtx, cancelInspect := context.WithTimeout(ctx, borrowedAuthStagingAssociationBudget)
	_, inspectErr := inspectBorrowedIdentityStrict(inspectCtx, s.fixture.fx.containerID, s.prefix.strictHelperPath,
		s.fixture.fx.postmasterPID, s.fixture.fx.postmasterStr, s.backendPID, s.socketInode, borrowedIdentityStrictPhaseInitial)
	cancelInspect()
	atomic.AddInt32(&s.identityQueries, 1)
	if inspectErr != nil {
		return errors.New("replacement session strict backend inspection refused")
	}
	identity, err := borrowedSuccessorReadConnIdentity(ctx, s.conn)
	atomic.AddInt32(&s.identityQueries, 1)
	if err != nil {
		return err
	}
	if identity.backendPID != s.backendPID || !identity.backendStart.Equal(s.backendStart) ||
		identity.roleOID != s.roleOID || identity.targetDBOID != s.targetDBOID {
		return errors.New("replacement session retained connection identity changed")
	}
	return nil
}

// newBorrowedReplacementSessionRegistration validates and binds the fresh
// replacement-bound capture and the retained authenticated connection into the
// copy-shared one-use state. It reads the actual connection identity, requires
// the replacement OID plus the original W role name/OID and target database
// name, requires the registered P1 catalog baseline, correlates the complete
// strict census and strict OS evidence with the exact full tuple token, and
// rechecks the FRESH replacement prefix and the original anchor. It never
// accepts a PID/JSON/boolean claim and never uses the old successor
// constructor or the old entry.
func newBorrowedReplacementSessionRegistration(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding, conn *pgx.Conn) (*borrowedReplacementSessionRegistration, error) {
	if b == nil || b.fixture == nil || fresh == nil || fresh.fixture != b.fixture || fresh.prefix == nil || fresh.prefix.state == nil || conn == nil {
		return nil, errors.New("replacement session registration requires the concrete baseline, fresh replacement capture and retained connection")
	}
	if reason := fresh.prefix.state.invalidReason(); reason != "" {
		return nil, errors.New("replacement session replacement prefix is permanently invalidated")
	}
	if fresh.replacementOID == 0 || fresh.replacementOID == fresh.oldOID || fresh.binding.TargetDatabaseOID() != fresh.replacementOID {
		return nil, errors.New("replacement session registration requires the verified replacement-bound capture")
	}
	if b.verifierP1 == "" || b.preState.oid == 0 {
		return nil, errors.New("replacement session registration requires the expected committed P1 verifier and role facts")
	}
	// Shared binding slot keyed by the SHARED replacement-prefix state (never a
	// wrapper address): an equivalent prefix wrapper over the same state can
	// only reuse the existing state (and refuse a consumed/invalidated one); it
	// never mints fresh state. The slot is acquired with a caller-bounded wait.
	key := borrowedReplacementSessionBindingKey{
		fixture: b.fixture, prefixState: fresh.prefix.state, conn: conn,
		replacementOID: fresh.replacementOID, verifier: b.verifierP1,
	}
	slotAny, _ := borrowedReplacementSessionSlots.LoadOrStore(key, &borrowedReplacementSessionSlot{})
	slot := slotAny.(*borrowedReplacementSessionSlot)
	if err := borrowedReplacementSessionAcquireSlot(ctx, slot); err != nil {
		return nil, err
	}
	defer slot.mu.Unlock()
	if existing := slot.state; existing != nil {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("replacement session registration refused: caller context ended before reuse publication: %w", err)
		}
		if existing.invalidReasonNow() != "" {
			return nil, errors.New("replacement session binding is permanently invalidated")
		}
		if existing.usedNow() {
			return nil, errors.New("replacement session binding was already used")
		}
		// The reuse publication rechecks the SHARED replacement-prefix validity
		// under the slot lock (no I/O), closing the stale-check window between
		// the initial prefix check and this publication: a known prefix loss
		// permanently invalidates the binding and refuses the reuse.
		if invalid, reason := existing.sharedPrefixInvalid(); invalid {
			existing.mu.Lock()
			if !existing.invalidated {
				existing.invalidated = true
				existing.invalidReason = "replacement session shared replacement prefix loss before reuse publication: " + reason
			}
			existing.mu.Unlock()
			return nil, errors.New("replacement session shared replacement prefix was permanently invalidated before reuse publication")
		}
		return &borrowedReplacementSessionRegistration{state: existing}, nil
	}
	f := b.fixture
	identity, err := borrowedSuccessorReadConnIdentity(ctx, conn)
	if err != nil {
		return nil, err
	}
	if identity.roleName != f.writerRole || identity.databaseName != f.targetDB {
		return nil, errors.New("replacement session connection role/database is not the W replacement target")
	}
	if identity.roleOID != f.writerRoleOID || identity.targetDBOID != fresh.replacementOID {
		return nil, errors.New("replacement session connection catalog OIDs are not the W replacement target")
	}
	current, err := borrowedSuccessorReadCatalogFacts(ctx, f)
	if err != nil {
		return nil, errors.New("replacement session catalog facts read refused")
	}
	if subtle.ConstantTimeCompare([]byte(current.verifier), []byte(b.verifierP1)) != 1 {
		return nil, errors.New("replacement session catalog verifier is not the expected committed P1 verifier")
	}
	if !borrowedFenceGapRoleStateEqual(current, b.preState) {
		return nil, errors.New("replacement session catalog role facts changed")
	}
	token, err := borrowedSuccessorAssociateToken(ctx, f, identity.serverLocal, identity.clientLocal)
	if err != nil {
		borrowedSuccessorDiagnoseAssociation(t, ctx, f, identity)
		return nil, err
	}
	if token.ChildPID != identity.backendPID || token.Local != identity.serverLocal || token.Remote != identity.clientLocal {
		return nil, errors.New("replacement session strict census token does not match the retained connection identity")
	}
	inspectCtx, cancelInspect := context.WithTimeout(ctx, borrowedAuthStagingAssociationBudget)
	_, inspectErr := inspectBorrowedIdentityStrict(inspectCtx, f.fx.containerID, fresh.prefix.strictHelperPath,
		f.fx.postmasterPID, f.fx.postmasterStr, identity.backendPID, token.Inode, borrowedIdentityStrictPhaseInitial)
	cancelInspect()
	if inspectErr != nil {
		return nil, errors.New("replacement session strict backend inspection refused")
	}
	osStatCtx, cancelOsStat := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	osState, osStart, osErr := borrowedAuthStagingContainerStat(osStatCtx, f.fx.containerID, identity.backendPID)
	cancelOsStat()
	if osErr != nil || osStart != token.ChildStart || !borrowedAuthStagingLiveState(osState) {
		return nil, errors.New("replacement session strict OS identity refused")
	}
	// FRESH replacement prefix + original anchor recheck before publication.
	recheckCtx, cancelRecheck := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	prefixErr := fresh.prefix.inspect(recheckCtx)
	anchorErr := fresh.anchor.Recheck(recheckCtx)
	cancelRecheck()
	if prefixErr != nil {
		return nil, errors.New("replacement session fresh replacement prefix recheck refused")
	}
	if anchorErr != nil {
		return nil, errors.New("replacement session original anchor recheck refused")
	}
	state := &borrowedReplacementSessionState{
		fixture: f, prefix: fresh.prefix, anchor: fresh.anchor, conn: conn,
		backendPID: identity.backendPID, backendStart: identity.backendStart,
		osStart: token.ChildStart, socketInode: token.Inode,
		roleOID: identity.roleOID, targetDBOID: identity.targetDBOID,
		serverLocal: identity.serverLocal, clientLocal: identity.clientLocal,
		replacementOID:   fresh.replacementOID,
		expectedVerifier: b.verifierP1, expectedState: b.preState,
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("replacement session registration refused: caller context ended before publication: %w", err)
	}
	// The fresh-state publication rechecks the SHARED replacement-prefix
	// validity under the slot lock (no I/O), closing the stale-check window
	// between the initial prefix check and this publication: a known prefix loss
	// refuses the publication and never mints fresh state.
	if invalid, reason := state.sharedPrefixInvalid(); invalid {
		return nil, fmt.Errorf("replacement session shared replacement prefix was permanently invalidated before fresh publication: %s", reason)
	}
	slot.state = state
	return &borrowedReplacementSessionRegistration{state: state}, nil
}

// Use performs the single read-only replacement session use. Classification and
// the in-progress reservation happen under the shared mutex BEFORE the caller
// context; the genuine fresh replacement-prefix and original-anchor rechecks
// run outside any owner transaction; fresh catalog facts are classified with
// the role-fact change first and the verifier drift second; the exact session
// identity is revalidated; the deterministic pre-probe publication barrier
// runs; exactly one fixed SELECT 1 probe executes on the SAME connection with a
// post-probe identity recheck; a post-probe publication barrier and the final
// caller-context check run; and the one-use publication linearizes with
// invalidation under the shared mutex with no I/O under it. No owner
// transaction is opened and no probe is retried.
func (r *borrowedReplacementSessionRegistration) Use(ctx context.Context) error {
	if r == nil || r.state == nil {
		return errors.New("replacement session registration is absent")
	}
	state := r.state
	state.mu.Lock()
	if state.invalidated {
		state.mu.Unlock()
		return errors.New("replacement session registration is permanently invalidated")
	}
	if state.used {
		state.mu.Unlock()
		return errors.New("replacement session registration was already used")
	}
	if state.using {
		state.mu.Unlock()
		return errors.New("replacement session registration is already being used")
	}
	if ctx == nil {
		state.invalidated = true
		state.invalidReason = "replacement session use context is missing"
		state.mu.Unlock()
		return errors.New("replacement session use requires a bounded context")
	}
	// Shared replacement-prefix validity is part of the pre-execution decision:
	// a known prefix loss permanently invalidates this session before any work.
	// Lock order is session.mu -> prefix.state.mu and this read performs no I/O.
	if invalid, reason := state.sharedPrefixInvalid(); invalid {
		state.invalidated = true
		state.invalidReason = "replacement session shared replacement prefix loss before use: " + reason
		state.mu.Unlock()
		return errors.New("replacement session shared replacement prefix was permanently invalidated before use")
	}
	state.using = true
	state.mu.Unlock()

	if err := state.verifyReplacementAndAnchor(ctx); err != nil {
		state.invalidate("replacement session replacement prefix/anchor recheck refused")
		return err
	}
	if err := state.verifyCatalogFacts(ctx); err != nil {
		state.invalidate("replacement session fresh catalog facts refused")
		return err
	}
	if err := borrowedReplacementSessionRevalidate(ctx, state); err != nil {
		state.invalidate("replacement session identity revalidation refused")
		return err
	}
	// Deterministic pre-probe publication barrier: a cancel or a shared prefix
	// loss observed here refuses before any probe execution.
	state.pauseAt(borrowedReplacementSessionStageUsePublish)
	if err := ctx.Err(); err != nil {
		state.invalidate("context ended before replacement session probe execution")
		return errors.New("replacement session use context ended before the probe")
	}
	if invalid, reason := state.sharedPrefixInvalid(); invalid {
		return state.refuseSharedPrefixLoss("before the probe: " + reason)
	}
	if err := state.executeProbe(ctx); err != nil {
		state.invalidate("replacement session probe refused")
		return err
	}
	// Deterministic post-probe publication barrier: a cancel or a shared prefix
	// loss observed here refuses after the single probe and before any
	// successful use publication.
	state.pauseAt(borrowedReplacementSessionStageUsePublication)
	if err := ctx.Err(); err != nil {
		state.invalidate("context ended before replacement session use publication")
		return errors.New("replacement session use context ended before publication")
	}
	if invalid, reason := state.sharedPrefixInvalid(); invalid {
		return state.refuseSharedPrefixLoss("before publication: " + reason)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.invalidated {
		return errors.New("replacement session registration was permanently invalidated concurrently")
	}
	if state.used {
		return errors.New("replacement session registration was already used")
	}
	// The final publication decision includes the SHARED replacement-prefix
	// validity (lock order session.mu -> prefix.state.mu, no I/O): a prefix loss
	// permanently invalidates the session and never publishes a successful use.
	if invalid, reason := state.sharedPrefixInvalid(); invalid {
		state.invalidated = true
		state.invalidReason = "replacement session shared replacement prefix loss before publication: " + reason
		return errors.New("replacement session shared replacement prefix was permanently invalidated before publication")
	}
	if err := ctx.Err(); err != nil {
		state.invalidated = true
		state.invalidReason = "context ended before replacement session use publication"
		return errors.New("replacement session use context ended before publication")
	}
	state.using = false
	state.used = true
	return nil
}

// verifyReplacementAndAnchor rechecks the FRESH replacement prefix (its full
// private identity window, which permanently invalidates the shared prefix
// state on any loss) and the original captured control anchor, OUTSIDE any
// owner transaction and with a caller-bounded child.
func (s *borrowedReplacementSessionState) verifyReplacementAndAnchor(ctx context.Context) error {
	recheckCtx, cancelRecheck := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	defer cancelRecheck()
	if err := s.prefix.inspect(recheckCtx); err != nil {
		return errors.New("replacement session fresh replacement prefix recheck refused")
	}
	if err := s.anchor.Recheck(recheckCtx); err != nil {
		return errors.New("replacement session original anchor recheck refused")
	}
	return nil
}

// verifyCatalogFacts reads fresh committed P1 catalog facts and classifies
// them: an OID/flags/membership change returns the role-facts drift error
// first; a verifier mismatch with unchanged facts returns the verifier-drift
// error.
func (s *borrowedReplacementSessionState) verifyCatalogFacts(ctx context.Context) error {
	current, err := borrowedSuccessorReadCatalogFacts(ctx, s.fixture)
	atomic.AddInt32(&s.identityQueries, 1)
	if err != nil {
		return errors.New("replacement session fresh catalog facts read refused")
	}
	if !borrowedFenceGapRoleStateEqual(current, s.expectedState) {
		return fmt.Errorf("%w: W role facts changed", errBorrowedReplacementSessionRoleFactsDrift)
	}
	if subtle.ConstantTimeCompare([]byte(current.verifier), []byte(s.expectedVerifier)) != 1 {
		return fmt.Errorf("%w (observed_length=%d expected_length=%d)", errBorrowedReplacementSessionVerifierDrift, len(current.verifier), len(s.expectedVerifier))
	}
	return nil
}

// executeProbe runs the single fixed SELECT 1 probe on the SAME retained
// connection and then rechecks the connection/OS identity. The probe execution
// counter is incremented exactly once per execution attempt; the post-probe
// identity reads are counted as identity-discovery queries.
func (s *borrowedReplacementSessionState) executeProbe(ctx context.Context) error {
	probeCtx, cancelProbe := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	var one int
	probeErr := s.conn.QueryRow(probeCtx, `SELECT 1`).Scan(&one)
	cancelProbe()
	atomic.AddInt32(&s.probeExecutions, 1)
	if probeErr != nil || one != 1 {
		return errors.New("replacement session fixed SELECT 1 probe refused")
	}
	postCtx, cancelPost := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	var pid int
	pidErr := s.conn.QueryRow(postCtx, `SELECT pg_backend_pid()`).Scan(&pid)
	cancelPost()
	atomic.AddInt32(&s.identityQueries, 1)
	if pidErr != nil || pid != s.backendPID {
		return errors.New("replacement session probe ran on a changed connection identity")
	}
	statCtx, cancelStat := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	osState, osStart, statErr := borrowedAuthStagingContainerStat(statCtx, s.fixture.fx.containerID, s.backendPID)
	cancelStat()
	atomic.AddInt32(&s.identityQueries, 1)
	if statErr != nil || osStart != s.osStart || !borrowedAuthStagingLiveState(osState) {
		return errors.New("replacement session probe post-check identity changed")
	}
	return nil
}

func (s *borrowedReplacementSessionState) invalidate(reason string) {
	s.mu.Lock()
	if !s.invalidated {
		s.invalidated = true
		s.invalidReason = reason
	}
	s.mu.Unlock()
}

func (s *borrowedReplacementSessionState) invalidReasonNow() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.invalidated {
		return ""
	}
	return s.invalidReason
}

func (s *borrowedReplacementSessionState) usedNow() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.used
}

func (s *borrowedReplacementSessionState) pauseAt(stage borrowedReplacementSessionStage) {
	if s.stage != nil {
		s.stage(stage)
	}
}

// sharedPrefixInvalid reads the SHARED replacement-prefix state validity with
// no I/O. It is the only prefix-state read used under a session/slot lock; the
// consistent lock order is session.mu (or slot.mu) -> prefix.state.mu, and no
// path ever takes prefix.state.mu before a session/slot lock.
func (s *borrowedReplacementSessionState) sharedPrefixInvalid() (bool, string) {
	if s == nil || s.prefix == nil || s.prefix.state == nil {
		return true, "replacement session replacement prefix state is absent"
	}
	reason := s.prefix.state.invalidReason()
	return reason != "", reason
}

// refuseSharedPrefixLoss permanently invalidates the session (if not already)
// and returns the shared-prefix-loss refusal. It must be called WITHOUT holding
// the session mutex; the lock order is session.mu -> prefix.state.mu and no I/O
// is performed under either lock.
func (s *borrowedReplacementSessionState) refuseSharedPrefixLoss(detail string) error {
	s.mu.Lock()
	if !s.invalidated {
		s.invalidated = true
		s.invalidReason = "replacement session shared replacement prefix loss " + detail
	}
	s.mu.Unlock()
	return errors.New("replacement session shared replacement prefix was permanently invalidated")
}

// borrowedReplacementSessionSeam is the test-only blocking notification barrier
// at a replacement-session publication stage. It is released exactly once at
// cleanup so a fatal can never strand the goroutine.
type borrowedReplacementSessionSeam struct {
	stage       borrowedReplacementSessionStage
	entered     chan struct{}
	release     chan struct{}
	enteredOnce sync.Once
	releaseOnce sync.Once
}

func installBorrowedReplacementSessionSeam(t *testing.T, reg *borrowedReplacementSessionRegistration, stage borrowedReplacementSessionStage) *borrowedReplacementSessionSeam {
	t.Helper()
	seam := &borrowedReplacementSessionSeam{stage: stage, entered: make(chan struct{}), release: make(chan struct{})}
	reg.state.stage = func(current borrowedReplacementSessionStage) {
		if current != seam.stage {
			return
		}
		seam.enteredOnce.Do(func() { close(seam.entered) })
		<-seam.release
	}
	t.Cleanup(seam.releaseSeam)
	return seam
}

func (s *borrowedReplacementSessionSeam) releaseSeam() {
	s.releaseOnce.Do(func() { close(s.release) })
}

// borrowedReplacementSessionRegisterP1 retains ONE authenticated P1 connection
// and registers it against the fresh replacement-bound capture.
func borrowedReplacementSessionRegisterP1(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding) (*pgx.Conn, *borrowedReplacementSessionRegistration) {
	t.Helper()
	conn := borrowedReplacementSessionConnectP1(t, ctx, b)
	reg, err := newBorrowedReplacementSessionRegistration(t, ctx, b, fresh, conn)
	if err != nil {
		t.Fatalf("replacement session registration refused: %v", err)
	}
	return conn, reg
}

// borrowedReplacementSessionRetire closes the session connection, proves the
// exact backend incarnation strictly gone, and re-proves the original owner
// health, the deliberate non-clean guard and zero acceptance. It never
// manufactures old state.
func borrowedReplacementSessionRetire(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, conn *pgx.Conn, state *borrowedReplacementSessionState) {
	t.Helper()
	borrowedSuccessorCloseConn(t, conn)
	borrowedAuthStagingAwaitDisappearance(ctx, t, b.fixture.fx.containerID, state.backendPID, state.osStart, borrowedAuthStagingDisappearanceBudget, true)
	healthCtx, cancelHealth := context.WithTimeout(ctx, borrowedOwnerRotationHealthBudget)
	healthErr := b.fixture.lock.Health(healthCtx)
	cancelHealth()
	if healthErr != nil {
		t.Fatalf("replacement session retire original control owner health: %v", healthErr)
	}
	borrowedOwnerDDLGuard(t, ctx, b)
	t.Logf("replacement session retired: backend PID %d incarnation strictly gone, owner health retained, guard non-clean, acceptance zero", state.backendPID)
}

// borrowedReplacementSessionWrongIdentityRefuse proves a real connection with
// the wrong role, a real connection with the wrong database and a real
// connection to a foreign owned fixture are all refused by the registration
// (non-destructive, shares the fresh replacement capture).
func borrowedReplacementSessionWrongIdentityRefuse(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding) {
	t.Helper()
	observerConn := borrowedOwnerDDLConnect(t, ctx, b.fixture.observerTargetDSN)
	_, roleErr := newBorrowedReplacementSessionRegistration(t, ctx, b, fresh, observerConn)
	if roleErr == nil || !strings.Contains(roleErr.Error(), "role/database is not the W replacement target") {
		t.Fatalf("wrong-role connection was not refused specifically: %v", roleErr)
	}
	sourceConn := borrowedOwnerDDLConnect(t, ctx, borrowedAuthRoleDSN(t, b.fixture.writerSourceDSN, b.fixture.writerRole, b.passwordP1))
	_, dbErr := newBorrowedReplacementSessionRegistration(t, ctx, b, fresh, sourceConn)
	if dbErr == nil || !strings.Contains(dbErr.Error(), "role/database is not the W replacement target") {
		t.Fatalf("wrong-database connection was not refused specifically: %v", dbErr)
	}
	secondFx := newOriginGateFixture(t)
	foreignDSN := borrowedAuthRoleDSN(t, secondFx.dsn, secondFx.role, secondFx.password)
	foreignConn := borrowedOwnerDDLConnect(t, ctx, foreignDSN)
	_, foreignErr := newBorrowedReplacementSessionRegistration(t, ctx, b, fresh, foreignConn)
	if foreignErr == nil || !strings.Contains(foreignErr.Error(), "role/database is not the W replacement target") {
		t.Fatalf("foreign owned fixture connection was not refused specifically: %v", foreignErr)
	}
	borrowedSuccessorCloseConn(t, observerConn)
	borrowedSuccessorCloseConn(t, sourceConn)
	borrowedSuccessorCloseConn(t, foreignConn)
	t.Logf("wrong-identity negative: observer-on-target, W-on-source and foreign owned fixture connections were refused by the registration")
}

// borrowedReplacementSessionUnknownRefuse proves injected unknown strict stat
// and census failures (incomplete inspection) refuse use with zero probe
// executions and that failed copies and reconstructions cannot rehabilitate
// (non-destructive, shares the fresh replacement capture).
func borrowedReplacementSessionUnknownRefuse(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding) {
	t.Helper()
	for _, kind := range []string{"stat", "census"} {
		conn, reg := borrowedReplacementSessionRegisterP1(t, ctx, b, fresh)
		kind := kind
		if kind == "stat" {
			reg.state.statFn = func(context.Context, *borrowedReplacementSessionState) (string, uint64, error) {
				return "", 0, errors.New("injected unknown strict stat failure")
			}
		} else {
			reg.state.associateFn = func(context.Context, *borrowedReplacementSessionState) (borrowedAuthStagingBackendToken, error) {
				return borrowedAuthStagingBackendToken{}, errors.New("injected unknown strict census failure")
			}
		}
		useCtx, cancelUse := context.WithTimeout(ctx, 60*time.Second)
		useErr := reg.Use(useCtx)
		cancelUse()
		if useErr == nil {
			t.Fatalf("unknown %s failure was accepted as a replacement session use", kind)
		}
		if errors.Is(useErr, errBorrowedReplacementSessionVerifierDrift) || errors.Is(useErr, errBorrowedReplacementSessionRoleFactsDrift) {
			t.Fatalf("unknown %s failure was misclassified as drift: %v", kind, useErr)
		}
		if got := atomic.LoadInt32(&reg.state.probeExecutions); got != 0 {
			t.Fatalf("unknown %s failure executed the probe %d times, want 0", kind, got)
		}
		copied := *reg
		if copied.Use(ctx) == nil {
			t.Fatalf("failed %s copy rehabilitated the replacement session registration", kind)
		}
		if _, liveErr := borrowedSuccessorReadConnIdentity(ctx, conn); liveErr != nil {
			t.Fatalf("unknown %s negative connection is not live before reconstruction: %v", kind, liveErr)
		}
		if _, reconErr := newBorrowedReplacementSessionRegistration(t, ctx, b, fresh, conn); reconErr == nil {
			t.Fatalf("post-unknown %s reconstruction minted a fresh replacement session registration", kind)
		}
		prefixCopy := *fresh.prefix
		freshCopy := *fresh
		freshCopy.prefix = &prefixCopy
		if _, wrapperErr := newBorrowedReplacementSessionRegistration(t, ctx, b, &freshCopy, conn); wrapperErr == nil {
			t.Fatalf("prefix-wrapper reconstruction after unknown %s loss minted a fresh replacement session registration", kind)
		}
		borrowedSuccessorCloseConn(t, conn)
	}
	t.Logf("incomplete-inspection negative: injected unknown strict stat and census failures refused with zero probe executions; failed copies and reconstructions refused")
}

// borrowedReplacementSessionCancelRefuse proves a cancel at the pre-probe
// publication barrier refuses with zero probe executions and a cancel at the
// post-probe publication barrier refuses after the single probe without any
// successful use, and that failed copies and reconstructions cannot
// rehabilitate (non-destructive, shares the fresh replacement capture).
func borrowedReplacementSessionCancelRefuse(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding) {
	t.Helper()
	// Pre-probe cancel: no probe execution.
	conn, reg := borrowedReplacementSessionRegisterP1(t, ctx, b, fresh)
	seam := installBorrowedReplacementSessionSeam(t, reg, borrowedReplacementSessionStageUsePublish)
	useCtx, cancelUse := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- reg.Use(useCtx) }()
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
		t.Errorf("replacement session use never reached the pre-probe publication barrier; completion is unknown")
		return
	}
	cancelUse()
	seam.releaseSeam()
	select {
	case useErr := <-done:
		joined = true
		if useErr == nil {
			t.Fatal("canceled pre-probe replacement session use published success")
		}
		if errors.Is(useErr, errBorrowedReplacementSessionVerifierDrift) || errors.Is(useErr, errBorrowedReplacementSessionRoleFactsDrift) {
			t.Fatalf("pre-probe cancel was misclassified as drift: %v", useErr)
		}
	case <-time.After(30 * time.Second):
		t.Errorf("canceled replacement session use did not complete within the bounded join; outcome unknown")
		return
	}
	if got := atomic.LoadInt32(&reg.state.probeExecutions); got != 0 {
		t.Fatalf("pre-probe cancel executed the probe %d times, want 0", got)
	}
	if _, reconErr := newBorrowedReplacementSessionRegistration(t, ctx, b, fresh, conn); reconErr == nil {
		t.Fatal("post-cancel reconstruction minted a fresh replacement session registration")
	}
	copied := *reg
	if copied.Use(ctx) == nil {
		t.Fatal("failed pre-probe cancel copy rehabilitated the replacement session registration")
	}
	borrowedSuccessorCloseConn(t, conn)
	t.Logf("pre-probe cancel negative: the canceled barrier use refused with zero probe executions; reconstruction and the failed copy refused")

	// Post-probe publication cancel: the single probe executes, no successful
	// use is published.
	conn2, reg2 := borrowedReplacementSessionRegisterP1(t, ctx, b, fresh)
	seam2 := installBorrowedReplacementSessionSeam(t, reg2, borrowedReplacementSessionStageUsePublication)
	use2Ctx, cancelUse2 := context.WithCancel(ctx)
	done2 := make(chan error, 1)
	go func() { done2 <- reg2.Use(use2Ctx) }()
	joined2 := false
	t.Cleanup(func() {
		cancelUse2()
		seam2.releaseSeam()
		if joined2 {
			return
		}
		select {
		case <-done2:
		case <-time.After(30 * time.Second):
			t.Errorf("publication cancel use completion is unknown after the bounded join")
		}
	})
	select {
	case <-seam2.entered:
	case <-time.After(120 * time.Second):
		t.Errorf("replacement session use never reached the post-probe publication barrier; completion is unknown")
		return
	}
	cancelUse2()
	seam2.releaseSeam()
	select {
	case use2Err := <-done2:
		joined2 = true
		if use2Err == nil {
			t.Fatal("canceled post-probe replacement session use published success")
		}
	case <-time.After(30 * time.Second):
		t.Errorf("publication-canceled replacement session use did not complete within the bounded join; outcome unknown")
		return
	}
	if got := atomic.LoadInt32(&reg2.state.probeExecutions); got != 1 {
		t.Fatalf("publication cancel probe executions=%d, want exactly 1", got)
	}
	if _, recon2Err := newBorrowedReplacementSessionRegistration(t, ctx, b, fresh, conn2); recon2Err == nil {
		t.Fatal("post-publication-cancel reconstruction minted a fresh replacement session registration")
	}
	borrowedSuccessorCloseConn(t, conn2)
	t.Logf("publication cancel negative: the canceled post-probe barrier refused after exactly one probe execution with no successful use; reconstruction refused")
}

// borrowedReplacementSessionCopiedPrefixLoss performs the ACTUAL copied-prefix
// loss: a COPY of the fresh replacement prefix is rechecked with a canceled
// context, which permanently invalidates the SHARED prefix state; the original
// prefix observes the same loss. This is observed permanent invalidation, not
// continuous exclusion.
func borrowedReplacementSessionCopiedPrefixLoss(t *testing.T, fresh *borrowedReplacementBinding) {
	t.Helper()
	prefixCopy := *fresh.prefix
	cancelCtx, cancelLoss := context.WithCancel(context.Background())
	cancelLoss()
	if err := prefixCopy.inspect(cancelCtx); err == nil {
		t.Fatal("copied replacement prefix canceled-ctx recheck succeeded")
	}
	if invalid, reason := prefixCopy.Invalid(); !invalid || reason == "" {
		t.Fatal("copied replacement prefix canceled-ctx recheck did not permanently invalidate the copy")
	}
	if invalid, reason := fresh.prefix.Invalid(); !invalid || reason == "" {
		t.Fatal("copied replacement prefix loss is not shared with the original prefix")
	}
}

// borrowedReplacementSessionPrefixLossPreProbeControl proves an actual
// copied-prefix loss injected at the pre-probe publication barrier refuses the
// use with zero probe executions and no publication, permanently invalidating
// the session and the shared prefix, and that reuse/reconstruction cannot
// rehabilitate (fresh destructive fixture).
func borrowedReplacementSessionPrefixLossPreProbeControl(t *testing.T, ctx context.Context) {
	t.Helper()
	b := newBorrowedSuccessorBaseline(t, ctx)
	orchestrateCtx, cancelOrchestrate := borrowedReplacementOrchestrationContext(ctx)
	var stages int32
	fresh, err := borrowedReplacementBindingOrchestrate(orchestrateCtx, t, b, &stages)
	cancelOrchestrate()
	if err != nil || fresh == nil {
		t.Fatalf("pre-probe shared-prefix-loss pipeline refused: capture=%v err=%v", fresh, err)
	}
	conn, reg := borrowedReplacementSessionRegisterP1(t, ctx, b, fresh)
	seam := installBorrowedReplacementSessionSeam(t, reg, borrowedReplacementSessionStageUsePublish)
	useCtx, cancelUse := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- reg.Use(useCtx) }()
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
			t.Errorf("pre-probe shared-prefix-loss use completion is unknown after the bounded join")
		}
	})
	select {
	case <-seam.entered:
	case <-time.After(60 * time.Second):
		t.Errorf("replacement session use never reached the pre-probe publication barrier; completion is unknown")
		return
	}
	borrowedReplacementSessionCopiedPrefixLoss(t, fresh)
	seam.releaseSeam()
	select {
	case useErr := <-done:
		joined = true
		if useErr == nil {
			t.Fatal("pre-probe shared prefix loss published a successful use")
		}
	case <-time.After(30 * time.Second):
		t.Errorf("pre-probe shared-prefix-loss use did not complete within the bounded join; outcome unknown")
		return
	}
	if got := atomic.LoadInt32(&reg.state.probeExecutions); got != 0 {
		t.Fatalf("pre-probe shared prefix loss executed the probe %d times, want 0", got)
	}
	if reg.state.invalidReasonNow() == "" {
		t.Fatal("pre-probe shared prefix loss did not permanently invalidate the session")
	}
	if invalid, reason := fresh.prefix.Invalid(); !invalid || reason == "" {
		t.Fatal("pre-probe shared prefix loss did not permanently invalidate the shared prefix")
	}
	if _, reconErr := newBorrowedReplacementSessionRegistration(t, ctx, b, fresh, conn); reconErr == nil {
		t.Fatal("post-pre-probe-loss reconstruction minted a fresh replacement session registration")
	}
	copied := *reg
	if copied.Use(ctx) == nil {
		t.Fatal("pre-probe-loss copy rehabilitated the replacement session registration")
	}
	borrowedSuccessorCloseConn(t, conn)
	borrowedOwnerDDLGuard(t, ctx, b)
	t.Logf("pre-probe shared-prefix-loss control: the actual copied-prefix canceled-ctx loss at the pre-probe barrier refused the use with zero probe executions and no publication; reuse and reconstruction refused")
}

// borrowedReplacementSessionPrefixLossPostProbeControl proves an actual
// copied-prefix loss injected at the post-probe publication barrier refuses the
// use after exactly one probe execution with no successful publication,
// permanently invalidating the session and the shared prefix, and that
// reuse/reconstruction cannot rehabilitate (fresh destructive fixture).
func borrowedReplacementSessionPrefixLossPostProbeControl(t *testing.T, ctx context.Context) {
	t.Helper()
	b := newBorrowedSuccessorBaseline(t, ctx)
	orchestrateCtx, cancelOrchestrate := borrowedReplacementOrchestrationContext(ctx)
	var stages int32
	fresh, err := borrowedReplacementBindingOrchestrate(orchestrateCtx, t, b, &stages)
	cancelOrchestrate()
	if err != nil || fresh == nil {
		t.Fatalf("post-probe shared-prefix-loss pipeline refused: capture=%v err=%v", fresh, err)
	}
	conn, reg := borrowedReplacementSessionRegisterP1(t, ctx, b, fresh)
	seam := installBorrowedReplacementSessionSeam(t, reg, borrowedReplacementSessionStageUsePublication)
	useCtx, cancelUse := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- reg.Use(useCtx) }()
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
			t.Errorf("post-probe shared-prefix-loss use completion is unknown after the bounded join")
		}
	})
	select {
	case <-seam.entered:
	case <-time.After(120 * time.Second):
		t.Errorf("replacement session use never reached the post-probe publication barrier; completion is unknown")
		return
	}
	borrowedReplacementSessionCopiedPrefixLoss(t, fresh)
	seam.releaseSeam()
	select {
	case useErr := <-done:
		joined = true
		if useErr == nil {
			t.Fatal("post-probe shared prefix loss published a successful use")
		}
	case <-time.After(30 * time.Second):
		t.Errorf("post-probe shared-prefix-loss use did not complete within the bounded join; outcome unknown")
		return
	}
	if got := atomic.LoadInt32(&reg.state.probeExecutions); got != 1 {
		t.Fatalf("post-probe shared prefix loss probe executions=%d, want exactly 1", got)
	}
	if reg.state.invalidReasonNow() == "" {
		t.Fatal("post-probe shared prefix loss did not permanently invalidate the session")
	}
	if invalid, reason := fresh.prefix.Invalid(); !invalid || reason == "" {
		t.Fatal("post-probe shared prefix loss did not permanently invalidate the shared prefix")
	}
	if _, reconErr := newBorrowedReplacementSessionRegistration(t, ctx, b, fresh, conn); reconErr == nil {
		t.Fatal("post-post-probe-loss reconstruction minted a fresh replacement session registration")
	}
	copied := *reg
	if copied.Use(ctx) == nil {
		t.Fatal("post-probe-loss copy rehabilitated the replacement session registration")
	}
	borrowedSuccessorCloseConn(t, conn)
	borrowedOwnerDDLGuard(t, ctx, b)
	t.Logf("post-probe shared-prefix-loss control: the actual copied-prefix canceled-ctx loss at the post-probe barrier refused the use after exactly one probe execution with no successful publication; reuse and reconstruction refused")
}

// borrowedReplacementSessionSecondReplacementRefuse proves a registered
// replacement session permanently refuses after a SECOND genuine replacement of
// the same target, and that the fresh prefix shared loss propagates to copies
// (fresh destructive fixture). The session connection is closed first (with the
// exact backend incarnation proven gone) so the genuine external replacement
// can drop the target database; the registration state stays registered and
// unused.
func borrowedReplacementSessionSecondReplacementRefuse(t *testing.T, ctx context.Context) {
	t.Helper()
	b := newBorrowedSuccessorBaseline(t, ctx)
	orchestrateCtx, cancelOrchestrate := borrowedReplacementOrchestrationContext(ctx)
	var stages int32
	fresh, err := borrowedReplacementBindingOrchestrate(orchestrateCtx, t, b, &stages)
	cancelOrchestrate()
	if err != nil || fresh == nil {
		t.Fatalf("second-replacement pipeline refused: capture=%v err=%v", fresh, err)
	}
	conn, reg := borrowedReplacementSessionRegisterP1(t, ctx, b, fresh)
	borrowedSuccessorCloseConn(t, conn)
	borrowedAuthStagingAwaitDisappearance(ctx, t, b.fixture.fx.containerID, reg.state.backendPID, reg.state.osStart, borrowedAuthStagingDisappearanceBudget, true)
	borrowedOwnerDDLExternalReplace(t, ctx, b)
	replacedOID, err := borrowedOwnerDDLTargetOID(ctx, b)
	if err != nil || replacedOID == fresh.replacementOID {
		t.Fatalf("second replacement did not change the target database OID: oid=%d err=%v", replacedOID, err)
	}
	useCtx, cancelUse := context.WithTimeout(ctx, 60*time.Second)
	useErr := reg.Use(useCtx)
	cancelUse()
	if useErr == nil {
		t.Fatal("registered replacement session was used after a second replacement")
	}
	if got := atomic.LoadInt32(&reg.state.probeExecutions); got != 0 {
		t.Fatalf("second replacement executed the probe %d times, want 0", got)
	}
	if invalid, reason := fresh.prefix.Invalid(); !invalid || reason == "" {
		t.Fatal("second replacement did not permanently invalidate the fresh replacement prefix")
	}
	prefixCopy := *fresh.prefix
	if invalid, _ := prefixCopy.Invalid(); !invalid {
		t.Fatal("second-replacement prefix loss is not shared with the prefix copy")
	}
	copied := *reg
	if copied.Use(ctx) == nil {
		t.Fatal("second-replacement copy rehabilitated the replacement session registration")
	}
	borrowedOwnerDDLGuard(t, ctx, b)
	t.Logf("second-replacement negative: the registered replacement session permanently refused after a second replacement with zero probe executions; the fresh prefix loss was shared with the copy")
}

// borrowedReplacementSessionTerminationSubstitutionRefuse proves the
// registered session termination refuses use with zero probe executions, and
// that another real same-W/target connection cannot substitute for the
// terminated registered session (fresh destructive fixture).
func borrowedReplacementSessionTerminationSubstitutionRefuse(t *testing.T, ctx context.Context) {
	t.Helper()
	b := newBorrowedSuccessorBaseline(t, ctx)
	orchestrateCtx, cancelOrchestrate := borrowedReplacementOrchestrationContext(ctx)
	var stages int32
	fresh, err := borrowedReplacementBindingOrchestrate(orchestrateCtx, t, b, &stages)
	cancelOrchestrate()
	if err != nil || fresh == nil {
		t.Fatalf("termination pipeline refused: capture=%v err=%v", fresh, err)
	}
	connA, regA := borrowedReplacementSessionRegisterP1(t, ctx, b, fresh)
	connB, regB := borrowedReplacementSessionRegisterP1(t, ctx, b, fresh)
	identityB, err := borrowedSuccessorReadConnIdentity(ctx, connB)
	if err != nil {
		t.Fatalf("substitution negative lookalike identity read: %v", err)
	}
	if identityB.backendPID == regA.state.backendPID {
		t.Fatal("substitution negative lookalike shares the registered session PID")
	}
	var terminated bool
	termCtx, cancelTerm := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	termErr := b.fixture.fx.admin.QueryRow(termCtx, `SELECT pg_terminate_backend($1)`, regA.state.backendPID).Scan(&terminated)
	cancelTerm()
	if termErr != nil || !terminated {
		t.Fatalf("terminate registered replacement session refused: terminated=%t err=%v", terminated, termErr)
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
		t.Fatal("terminated replacement session was used successfully")
	}
	if errors.Is(useErr, errBorrowedReplacementSessionVerifierDrift) || errors.Is(useErr, errBorrowedReplacementSessionRoleFactsDrift) {
		t.Fatalf("replacement session termination was misclassified as drift: %v", useErr)
	}
	if got := atomic.LoadInt32(&regA.state.probeExecutions); got != 0 {
		t.Fatalf("terminated replacement session executed the probe %d times, want 0", got)
	}
	lookalikeCtx, cancelLookalike := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	lookalikeState, lookalikeStart, lookalikeErr := borrowedAuthStagingContainerStat(lookalikeCtx, b.fixture.fx.containerID, identityB.backendPID)
	cancelLookalike()
	if lookalikeErr != nil || lookalikeStart != regB.state.osStart || !borrowedAuthStagingLiveState(lookalikeState) {
		t.Fatalf("lookalike replacement session is not the live real connection: state=%q err=%v", lookalikeState, lookalikeErr)
	}
	copied := *regA
	if copied.Use(ctx) == nil {
		t.Fatal("terminated replacement session copy rehabilitated the registration")
	}
	borrowedSuccessorCloseConn(t, connA)
	borrowedSuccessorCloseConn(t, connB)
	t.Logf("termination/substitution negative: registered replacement session termination refused use with zero probe executions and the live real same-W/target lookalike could not substitute")
}

// borrowedReplacementSessionOwnerLossRefuse proves real control owner loss
// refuses use with zero probe executions, no replacement owner and the
// original owner health/anchor permanently refused (fresh destructive
// fixture).
func borrowedReplacementSessionOwnerLossRefuse(t *testing.T, ctx context.Context) {
	t.Helper()
	b := newBorrowedSuccessorBaseline(t, ctx)
	orchestrateCtx, cancelOrchestrate := borrowedReplacementOrchestrationContext(ctx)
	var stages int32
	fresh, err := borrowedReplacementBindingOrchestrate(orchestrateCtx, t, b, &stages)
	cancelOrchestrate()
	if err != nil || fresh == nil {
		t.Fatalf("owner-loss pipeline refused: capture=%v err=%v", fresh, err)
	}
	conn, reg := borrowedReplacementSessionRegisterP1(t, ctx, b, fresh)
	controlPID := fresh.binding.ControlBackendPID()
	if controlPID <= 0 || controlPID != b.owner.BackendPID {
		t.Fatalf("owner-loss captured control owner PID %d is missing or not the anchored owner PID %d", controlPID, b.owner.BackendPID)
	}
	var terminated bool
	lossCtx, cancelLoss := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	lossErr := b.fixture.controlPool.QueryRow(lossCtx, `SELECT pg_terminate_backend($1)`, controlPID).Scan(&terminated)
	cancelLoss()
	if lossErr != nil || !terminated {
		t.Fatalf("terminate replacement-session control owner: terminated=%t err=%v", terminated, lossErr)
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
		t.Fatal("replacement session use succeeded after real control owner loss")
	}
	if errors.Is(useErr, errBorrowedReplacementSessionVerifierDrift) || errors.Is(useErr, errBorrowedReplacementSessionRoleFactsDrift) {
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
		t.Fatal("retained original anchor rechecked after real owner loss")
	}
	copied := *reg
	if copied.Use(ctx) == nil {
		t.Fatal("owner-loss copy rehabilitated the replacement session registration")
	}
	borrowedSuccessorCloseConn(t, conn)
	t.Logf("owner-loss negative: real control owner loss refused replacement session use with zero probe executions, no replacement owner and no retry")
}

// borrowedReplacementSessionRoleDriftRefuse proves a committed P1 role-fact
// drift (CREATEDB) with the verifier still at the expected P1 value is
// classified with the specific role-facts drift error, never the verifier-drift
// error, with zero probe executions and permanent shared refusal (fresh
// destructive fixture).
func borrowedReplacementSessionRoleDriftRefuse(t *testing.T, ctx context.Context) {
	t.Helper()
	b := newBorrowedSuccessorBaseline(t, ctx)
	orchestrateCtx, cancelOrchestrate := borrowedReplacementOrchestrationContext(ctx)
	var stages int32
	fresh, err := borrowedReplacementBindingOrchestrate(orchestrateCtx, t, b, &stages)
	cancelOrchestrate()
	if err != nil || fresh == nil {
		t.Fatalf("P1-role drift pipeline refused: capture=%v err=%v", fresh, err)
	}
	conn, reg := borrowedReplacementSessionRegisterP1(t, ctx, b, fresh)
	driftCtx, cancelDrift := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	_, driftErr := b.fixture.fx.admin.Exec(driftCtx, `ALTER ROLE `+pgx.Identifier{b.fixture.writerRole}.Sanitize()+` CREATEDB`)
	cancelDrift()
	if driftErr != nil {
		t.Fatalf("replacement session role-facts drift mutation refused: %v", driftErr)
	}
	current, err := borrowedSuccessorReadCatalogFacts(ctx, b.fixture)
	if err != nil {
		t.Fatalf("replacement session role-facts drift exact catalog read: %v", err)
	}
	if current.createdb == b.preState.createdb {
		t.Fatal("replacement session role-facts drift mutation did not change the flag")
	}
	if subtle.ConstantTimeCompare([]byte(current.verifier), []byte(b.verifierP1)) != 1 {
		t.Fatal("replacement session role-facts drift negative changed the committed P1 verifier")
	}
	useCtx, cancelUse := context.WithTimeout(ctx, 60*time.Second)
	useErr := reg.Use(useCtx)
	cancelUse()
	if !errors.Is(useErr, errBorrowedReplacementSessionRoleFactsDrift) {
		t.Fatalf("P1-role drift was not refused with the specific role-facts drift error: %v", useErr)
	}
	if errors.Is(useErr, errBorrowedReplacementSessionVerifierDrift) {
		t.Fatal("P1-role drift was misclassified as verifier drift")
	}
	if got := atomic.LoadInt32(&reg.state.probeExecutions); got != 0 {
		t.Fatalf("P1-role drift executed the probe %d times, want 0", got)
	}
	copied := *reg
	if copied.Use(ctx) == nil {
		t.Fatal("P1-role-drift copy rehabilitated the replacement session registration")
	}
	borrowedSuccessorCloseConn(t, conn)
	borrowedOwnerDDLGuard(t, ctx, b)
	t.Logf("P1-role-drift negative: committed CREATEDB drift with the expected P1 verifier refused with the specific role-facts drift error and zero probe executions")
}

// TestBorrowedReplacementSessionRegistration is the bounded replacement-bound
// retained-session registration/use lane described in the file header. It
// grants no continuous fence, no restore, no rebuild, no acceptance, no
// manifest, no downstream and no Gate1 authority; the fresh Run is never
// invoked, no child is launched, identity-discovery queries are counted
// separately from probe executions and every failure is permanent and shared.
func TestBorrowedReplacementSessionRegistration(t *testing.T) {
	ctx := t.Context()

	// Genuine capture: the actual bounded replacement-binding orchestration as
	// its positive does; never a projection.
	b := newBorrowedSuccessorBaseline(t, ctx)
	originalTargetDSN := b.fixture.writerTargetDSN
	originalObserverDSN := b.fixture.observerTargetDSN
	var stages int32
	orchestrateCtx, cancelOrchestrate := borrowedReplacementOrchestrationContext(ctx)
	fresh, err := borrowedReplacementBindingOrchestrate(orchestrateCtx, t, b, &stages)
	cancelOrchestrate()
	if err != nil {
		t.Fatalf("replacement session genuine capture refused: %v", err)
	}
	if fresh == nil {
		t.Fatal("replacement session genuine capture produced no capture")
	}
	if b.fixture.writerTargetDSN != originalTargetDSN || b.fixture.observerTargetDSN != originalObserverDSN {
		t.Fatal("replacement session lane rewrote an original fixture DSN")
	}
	if got := atomic.LoadInt32(&stages); got != borrowedReplacementStagePrefixCaptured {
		t.Fatalf("replacement session pipeline stages=%d, want %d", got, borrowedReplacementStagePrefixCaptured)
	}
	if fresh.binding.TargetDatabaseOID() != fresh.replacementOID || fresh.replacementOID == fresh.oldOID {
		t.Fatalf("replacement session capture is not replacement-bound: replacement=%d old=%d binding=%d", fresh.replacementOID, fresh.oldOID, fresh.binding.TargetDatabaseOID())
	}
	t.Logf("replacement session genuine capture: old OID %d -> replacement OID %d", fresh.oldOID, fresh.replacementOID)

	// Build: retain ONE authenticated P1 connection and register it against the
	// fresh replacement prefix and the original anchor.
	conn, reg := borrowedReplacementSessionRegisterP1(t, ctx, b, fresh)
	state := reg.state

	// Equivalent prefix wrapper: the registry key uses the SHARED
	// replacement-prefix state (never the wrapper address), so an equivalent
	// wrapper over the same state reuses the exact same slot state.
	prefixCopy := *fresh.prefix
	freshCopy := *fresh
	freshCopy.prefix = &prefixCopy
	equivalent, equivalentErr := newBorrowedReplacementSessionRegistration(t, ctx, b, &freshCopy, conn)
	if equivalentErr != nil {
		t.Fatalf("equivalent replacement-prefix wrapper was not reused: %v", equivalentErr)
	}
	if equivalent.state != state {
		t.Fatal("equivalent replacement-prefix wrapper did not reuse the shared slot state")
	}

	// Use: one read-only replacement session use on the SAME connection.
	useCtx, cancelUse := context.WithTimeout(ctx, 60*time.Second)
	useErr := reg.Use(useCtx)
	cancelUse()
	if useErr != nil {
		t.Fatalf("replacement session use refused: %v", useErr)
	}
	if got := atomic.LoadInt32(&state.probeExecutions); got != 1 {
		t.Fatalf("replacement session probe executions=%d, want exactly 1", got)
	}
	if got := atomic.LoadInt32(&state.identityQueries); got == 0 {
		t.Fatal("replacement session identity-discovery queries were not counted separately")
	}
	t.Logf("replacement session registered+used: backend PID %d probe executions=%d identity-discovery queries=%d", state.backendPID, atomic.LoadInt32(&state.probeExecutions), atomic.LoadInt32(&state.identityQueries))

	// Replay refusal after Use: same, copy, reconstruction and prefix-wrapper
	// reconstruction; no new probe.
	replayCtx, cancelReplay := context.WithTimeout(ctx, 30*time.Second)
	replayErr := reg.Use(replayCtx)
	copied := *reg
	copyErr := copied.Use(replayCtx)
	cancelReplay()
	if replayErr == nil || copyErr == nil {
		t.Fatal("replacement session replay/copy use was accepted")
	}
	if got := atomic.LoadInt32(&state.probeExecutions); got != 1 {
		t.Fatalf("replacement session replay executed the probe: executions=%d", got)
	}
	if _, replayRegErr := newBorrowedReplacementSessionRegistration(t, ctx, b, fresh, conn); replayRegErr == nil {
		t.Fatal("constructor replay after a successful use minted a fresh replacement session registration")
	}
	if _, wrapperReplayErr := newBorrowedReplacementSessionRegistration(t, ctx, b, &freshCopy, conn); wrapperReplayErr == nil {
		t.Fatal("prefix-wrapper reconstruction after a successful use minted a fresh replacement session registration")
	}

	// Concurrent replay: the in-progress reservation refuses a second Use
	// while the first is held at the pre-probe publication barrier, and the
	// first still publishes exactly one probe.
	conn2, reg2 := borrowedReplacementSessionRegisterP1(t, ctx, b, fresh)
	seam2 := installBorrowedReplacementSessionSeam(t, reg2, borrowedReplacementSessionStageUsePublish)
	concurrentCtx, cancelConcurrent := context.WithCancel(ctx)
	concurrentDone := make(chan error, 1)
	go func() { concurrentDone <- reg2.Use(concurrentCtx) }()
	concurrentJoined := false
	t.Cleanup(func() {
		cancelConcurrent()
		seam2.releaseSeam()
		if concurrentJoined {
			return
		}
		select {
		case <-concurrentDone:
		case <-time.After(30 * time.Second):
			t.Errorf("concurrent use completion is unknown after the bounded join")
		}
	})
	select {
	case <-seam2.entered:
	case <-time.After(60 * time.Second):
		t.Errorf("concurrent replacement session use never reached the pre-probe publication barrier; completion is unknown")
		return
	}
	secondUseCtx, cancelSecondUse := context.WithTimeout(ctx, 30*time.Second)
	secondUseErr := reg2.Use(secondUseCtx)
	cancelSecondUse()
	if secondUseErr == nil || !strings.Contains(secondUseErr.Error(), "already being used") {
		t.Fatalf("concurrent replacement session use was not refused as already being used: %v", secondUseErr)
	}
	seam2.releaseSeam()
	select {
	case use2Err := <-concurrentDone:
		concurrentJoined = true
		if use2Err != nil {
			t.Fatalf("first concurrent replacement session use refused: %v", use2Err)
		}
	case <-time.After(30 * time.Second):
		t.Errorf("concurrent replacement session use did not complete within the bounded join; outcome unknown")
		return
	}
	if got := atomic.LoadInt32(&reg2.state.probeExecutions); got != 1 {
		t.Fatalf("concurrent replay executed the probe: executions=%d", got)
	}
	t.Logf("concurrent replay negative: the in-progress reservation refused the second use and the first published exactly one probe")

	// Retire: close both sessions, prove both backend incarnations strictly
	// gone, owner health, guard non-clean and acceptance zero.
	borrowedReplacementSessionRetire(t, ctx, b, conn, state)
	borrowedReplacementSessionRetire(t, ctx, b, conn2, reg2.state)

	// Non-destructive negatives on the same genuine capture.
	borrowedReplacementSessionWrongIdentityRefuse(t, ctx, b, fresh)
	borrowedReplacementSessionUnknownRefuse(t, ctx, b, fresh)
	borrowedReplacementSessionCancelRefuse(t, ctx, b, fresh)

	// Destructive negatives on fresh fixtures.
	borrowedReplacementSessionPrefixLossPreProbeControl(t, ctx)
	borrowedReplacementSessionPrefixLossPostProbeControl(t, ctx)
	borrowedReplacementSessionSecondReplacementRefuse(t, ctx)
	borrowedReplacementSessionTerminationSubstitutionRefuse(t, ctx)
	borrowedReplacementSessionOwnerLossRefuse(t, ctx)
	borrowedReplacementSessionRoleDriftRefuse(t, ctx)
}
