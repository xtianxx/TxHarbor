//go:build linux && drill

// targetwriter_controlanchor_drillbridge_linux_test.go is the mini-phase-1
// ACTUAL CONTROL ANCHOR bridge (oracle14 contract). It is drill-only and adds
// no production API.
//
// A factory-created *DrillBorrowedWriterRun already retains the real borrowed
// *TargetLock and the immutable factory SQL binding. CaptureControlAnchor
// reads, from the SAME live dedicated PGX control connection and under the
// lock's actual mutex, the real SQL incarnation facts (backend pid/start,
// control database OID via current_database(), pg_control_system
// system_identifier, pg_postmaster_start_time), the exact granted original
// target namespace advisory lock (classid/objid = uint32 advisory pair of the
// logical original DataTargetKey, objsubid=2, database = actual control
// database OID), and the live TCP tuple (LocalAddr/RemoteAddr) of that same
// connection. Every fact must match the immutable factory binding; any
// missing, changed or unclassifiable fact refuses with an UNKNOWN error and
// the factory never returns valid facts.
//
// The returned DrillControlAnchor is an opaque concrete value whose private
// pointer is shared by copies. Recheck re-reads the actual facts through the
// same live lock; any owner loss/release/changed identity/tuple or context
// interruption permanently invalidates the anchor. There is no cached
// validity boolean, no constructor from caller tuples/PIDs/hashes, no real
// connection or *exec.Cmd projection, and no authority to launch, wait,
// mutate a child, probe/accept or reset a guard. The phase-2 OS/socket prefix
// remains OPEN.
package recovery

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	// drillControlAnchorFactBound bounds each capture/recheck read when the
	// caller supplied no nearer deadline.
	drillControlAnchorFactBound = 3 * time.Second
	// drillControlAnchorHealthBound bounds the genuine lock Health call made
	// outside the control mutex.
	drillControlAnchorHealthBound = time.Second
	// drillControlAnchorMutexPoll is the TryLock retry interval: the caller
	// context is respected without ever spawning an abandoned locker
	// goroutine.
	drillControlAnchorMutexPoll = 5 * time.Millisecond
)

// drillControlAnchorTestStage names the narrow test-only pause points used by
// the causal tests. They are not authorization callbacks and are never
// consulted by any production path (this whole file is drill-tagged).
type drillControlAnchorTestStage string

const (
	// drillControlAnchorStageCaptureHealth pauses CaptureControlAnchor after
	// fact inspection/binding match and before the genuine Health call.
	drillControlAnchorStageCaptureHealth drillControlAnchorTestStage = "capture-health"
	// drillControlAnchorStageRecheckHealth pauses Recheck after fact
	// inspection and before the genuine Health call.
	drillControlAnchorStageRecheckHealth drillControlAnchorTestStage = "recheck-health"
	// drillControlAnchorStageRecheckPublish pauses Recheck after genuine
	// Health succeeded and before the success is published under invalidMu.
	drillControlAnchorStageRecheckPublish drillControlAnchorTestStage = "recheck-publish"
	// drillControlAnchorStageCapturePublish pauses CaptureControlAnchor after
	// genuine Health succeeded and before the new capability is published.
	drillControlAnchorStageCapturePublish drillControlAnchorTestStage = "capture-publish"
)

// drillControlAnchorContextEndedReason is the fixed safe reason recorded when
// a caller context ends between a successful genuine Health call and the
// publication decision. It never includes raw context text or DSN material.
const drillControlAnchorContextEndedReason = "control context ended before success publication"

// drillControlAnchorStageHook is the per-process test pause seam. Tests
// install it for the duration of one test and clear it at cleanup; it is read
// atomically so no global race is possible.
var drillControlAnchorStageHook atomic.Pointer[func(drillControlAnchorTestStage)]

func drillControlAnchorPauseAt(stage drillControlAnchorTestStage) {
	if hook := drillControlAnchorStageHook.Load(); hook != nil {
		(*hook)(stage)
	}
}

// drillControlAnchorRefusal is the fixed, safe refusal of the anchor bridge.
// reason is a constant string; no DSN, address, pid or SQL text is surfaced.
type drillControlAnchorRefusal struct {
	reason string
}

func (e *drillControlAnchorRefusal) Error() string {
	return "control anchor is UNKNOWN (" + e.reason + ")"
}

func drillControlAnchorRefuse(reason string) error {
	return &drillControlAnchorRefusal{reason: reason}
}

// drillControlAnchorReadFacts is the private read-only fact set captured from
// the actual live control connection. The net.Conn pointer is retained only
// for same-connection identity; it is never projected.
type drillControlAnchorReadFacts struct {
	backendPID          int
	backendStart        time.Time
	controlDatabaseOID  uint32
	systemIdentifier    string
	postmasterStart     time.Time
	namespaceClassID    uint32
	namespaceObjectID   uint32
	namespaceDatabaseID uint32
	localAddr           *net.TCPAddr
	remoteAddr          *net.TCPAddr
	socketInode         uint64
	netConn             net.Conn
}

// drillControlAnchorState is the shared private state of one captured anchor.
// Copies of DrillControlAnchor share this pointer, so permanent invalidation
// is shared. No exported field, no JSON representation, no authority.
type drillControlAnchorState struct {
	invalidMu     sync.Mutex
	invalidated   bool
	invalidReason string

	backendPID          int
	backendStart        time.Time
	controlDatabaseOID  uint32
	systemIdentifier    string
	postmasterStart     time.Time
	namespaceClassID    uint32
	namespaceObjectID   uint32
	namespaceDatabaseID uint32
	localAddr           *net.TCPAddr
	remoteAddr          *net.TCPAddr
	socketInode         uint64
	capturedAt          time.Time
	netConn             net.Conn

	lock *TargetLock
	run  *DrillBorrowedWriterRun

	originalTargetKey       TargetKey
	controlTargetKey        TargetKey
	transportTargetKey      TargetKey
	originalRoleFingerprint string
	originalOperationID     string
	originalInstanceID      string
}

// DrillControlAnchor is the opaque actual-control-owner anchor. The zero value
// and any JSON round trip are inert: only the factory method on a real
// factory-created run can produce a recheckable anchor.
type DrillControlAnchor struct {
	state *drillControlAnchorState
}

// DrillControlAnchorFacts is the read-only diagnostic projection of a captured
// anchor. It is not authority and cannot be converted back into an anchor.
type DrillControlAnchorFacts struct {
	Present       bool
	Invalidated   bool
	InvalidReason string
	CapturedAt    time.Time

	BackendPID          int
	BackendStart        time.Time
	ControlDatabaseOID  uint32
	SystemIdentifier    string
	PostmasterStart     time.Time
	NamespaceClassID    uint32
	NamespaceObjectID   uint32
	NamespaceDatabaseID uint32
	SocketInode         uint64

	LocalAddress  string
	LocalPort     int
	RemoteAddress string
	RemotePort    int

	OriginalTargetKey       TargetKey
	ControlTargetKey        TargetKey
	TransportTargetKey      TargetKey
	OriginalRoleFingerprint string
	OriginalOperationID     string
	OriginalInstanceID      string
}

// CaptureControlAnchor captures the actual control-owner anchor of a
// factory-created borrowed run. It validates the factory lifetime and the
// complete immutable factory SQL binding, then reads the real facts from the
// same live dedicated control connection under the actual lock mutex (with a
// caller-bounded context and TryLock-based acquisition that never abandons a
// locker goroutine). Genuine run.Health is checked outside the mutex. Any
// absence, cancellation, mismatch or unclassifiable fact returns an UNKNOWN
// refusal; valid facts are never synthesized.
func (r *DrillBorrowedWriterRun) CaptureControlAnchor(ctx context.Context) (DrillControlAnchor, error) {
	if r == nil || r.life == nil || r.lock == nil {
		return DrillControlAnchor{}, drillControlAnchorRefuse("not a factory-created borrowed run")
	}
	if !r.binding.SQLFactsComplete() {
		return DrillControlAnchor{}, drillControlAnchorRefuse("factory SQL capture is incomplete")
	}
	bounded, cancel, err := drillControlAnchorBounded(ctx)
	if err != nil {
		return DrillControlAnchor{}, err
	}
	defer cancel()
	lock := r.lock
	if err := drillControlAnchorAcquireLockContext(bounded, &lock.mu); err != nil {
		return DrillControlAnchor{}, drillControlAnchorRefuse("control lock mutex was not acquired within the bounded context")
	}
	facts, readErr := drillControlAnchorReadLocked(bounded, lock)
	lock.mu.Unlock()
	if readErr != nil {
		return DrillControlAnchor{}, readErr
	}
	if err := drillControlAnchorMatchBinding(r, lock, facts); err != nil {
		return DrillControlAnchor{}, err
	}
	state := &drillControlAnchorState{
		backendPID:              facts.backendPID,
		backendStart:            facts.backendStart,
		controlDatabaseOID:      facts.controlDatabaseOID,
		systemIdentifier:        facts.systemIdentifier,
		postmasterStart:         facts.postmasterStart,
		namespaceClassID:        facts.namespaceClassID,
		namespaceObjectID:       facts.namespaceObjectID,
		namespaceDatabaseID:     facts.namespaceDatabaseID,
		localAddr:               drillControlAnchorCopyTCP(facts.localAddr),
		remoteAddr:              drillControlAnchorCopyTCP(facts.remoteAddr),
		socketInode:             facts.socketInode,
		capturedAt:              time.Now().UTC(),
		netConn:                 facts.netConn,
		lock:                    lock,
		run:                     r,
		originalTargetKey:       r.binding.originalTargetKey,
		controlTargetKey:        r.binding.controlTargetKey,
		transportTargetKey:      r.binding.transportTargetKey,
		originalRoleFingerprint: r.binding.originalRoleFingerprint,
		originalOperationID:     r.binding.originalOperationID,
		originalInstanceID:      r.binding.originalInstanceID,
	}
	anchor := DrillControlAnchor{state: state}
	drillControlAnchorPauseAt(drillControlAnchorStageCaptureHealth)
	healthCtx, healthCancel := context.WithTimeout(ctx, drillControlAnchorHealthBound)
	defer healthCancel()
	if err := r.Health(healthCtx); err != nil {
		return DrillControlAnchor{}, drillControlAnchorRefuse("control lock health is not genuine")
	}
	// Capture publishes a brand-new anchor: its private state is created in
	// this call and is not shared with any other handle until the return, so
	// there is no pre-existing invalidation state to linearize against. The
	// publication still fails closed when the caller's own context ended
	// between the successful Health call and this point: no usable capability
	// is ever returned for an ended attempt. A failed Health refuses instead
	// of publishing an invalid capability.
	drillControlAnchorPauseAt(drillControlAnchorStageCapturePublish)
	if err := healthCtx.Err(); err != nil {
		return DrillControlAnchor{}, drillControlAnchorRefuse(drillControlAnchorContextEndedReason)
	}
	return anchor, nil
}

// Recheck re-reads the actual control-owner facts through the same live lock
// and the same connection identity. Any loss, release, changed identity,
// tuple mismatch, missing visibility, permission failure or context
// interruption permanently invalidates the captured anchor (shared with every
// copy) and returns the UNKNOWN refusal. A successful Recheck never caches
// validity: the next Recheck reads the actual facts again.
func (a *DrillControlAnchor) Recheck(ctx context.Context) error {
	if a == nil || a.state == nil {
		return drillControlAnchorRefuse("anchor is absent")
	}
	state := a.state
	if state.invalidReasonNow() != "" {
		return drillControlAnchorRefuse("anchor is permanently invalidated")
	}
	bounded, cancel, err := drillControlAnchorBounded(ctx)
	if err != nil {
		state.invalidate("context interruption")
		return err
	}
	defer cancel()
	if err := drillControlAnchorRecheckSession(bounded, state); err != nil {
		state.invalidate("captured control identity changed or became unreadable")
		return err
	}
	drillControlAnchorPauseAt(drillControlAnchorStageRecheckHealth)
	healthCtx, healthCancel := context.WithTimeout(ctx, drillControlAnchorHealthBound)
	defer healthCancel()
	if err := state.run.Health(healthCtx); err != nil {
		state.invalidate("control lock health is not genuine")
		return drillControlAnchorRefuse("control lock health is not genuine")
	}
	// Success publication linearizes with invalidation under the same
	// invalidMu: if any copy invalidated the shared state while this healthy
	// observation was in progress, this call fails even though its own fact
	// and Health checks passed. If the caller's OWN parent context ended after
	// the successful Health call (cancellation or expiry) before this
	// decision, the end is recorded directly as a shared permanent
	// invalidation instead of returning success; the reason is a fixed safe
	// constant and the fields are set directly because calling invalidate
	// here would recurse on the already-held mutex. The mutex is held only
	// for this short decision, never across I/O.
	//
	// Linearization point: once this decision returns success, a later
	// cancellation cannot retroactively revoke that returned success; the
	// decision itself is the source-level linearization point.
	drillControlAnchorPauseAt(drillControlAnchorStageRecheckPublish)
	state.invalidMu.Lock()
	defer state.invalidMu.Unlock()
	if state.invalidated {
		return drillControlAnchorRefuse("anchor was permanently invalidated concurrently")
	}
	if err := healthCtx.Err(); err != nil {
		state.invalidated = true
		state.invalidReason = drillControlAnchorContextEndedReason
		return drillControlAnchorRefuse(drillControlAnchorContextEndedReason)
	}
	return nil
}

// Diagnostics returns the read-only diagnostic projection. It reports the
// last invalidation for diagnostics only; Recheck remains the only validity
// authority and always re-reads the actual facts.
func (a *DrillControlAnchor) Diagnostics() DrillControlAnchorFacts {
	if a == nil || a.state == nil {
		return DrillControlAnchorFacts{}
	}
	state := a.state
	state.invalidMu.Lock()
	invalidated, reason := state.invalidated, state.invalidReason
	state.invalidMu.Unlock()
	local, remote := "", ""
	if state.localAddr != nil {
		local = state.localAddr.String()
	}
	if state.remoteAddr != nil {
		remote = state.remoteAddr.String()
	}
	return DrillControlAnchorFacts{
		Present:       true,
		Invalidated:   invalidated,
		InvalidReason: reason,
		CapturedAt:    state.capturedAt,

		BackendPID:          state.backendPID,
		BackendStart:        state.backendStart,
		ControlDatabaseOID:  state.controlDatabaseOID,
		SystemIdentifier:    state.systemIdentifier,
		PostmasterStart:     state.postmasterStart,
		NamespaceClassID:    state.namespaceClassID,
		NamespaceObjectID:   state.namespaceObjectID,
		NamespaceDatabaseID: state.namespaceDatabaseID,
		SocketInode:         state.socketInode,

		LocalAddress:  local,
		LocalPort:     drillControlAnchorPort(state.localAddr),
		RemoteAddress: remote,
		RemotePort:    drillControlAnchorPort(state.remoteAddr),

		OriginalTargetKey:       state.originalTargetKey,
		ControlTargetKey:        state.controlTargetKey,
		TransportTargetKey:      state.transportTargetKey,
		OriginalRoleFingerprint: state.originalRoleFingerprint,
		OriginalOperationID:     state.originalOperationID,
		OriginalInstanceID:      state.originalInstanceID,
	}
}

func (s *drillControlAnchorState) invalidReasonNow() string {
	if s == nil {
		return "absent"
	}
	s.invalidMu.Lock()
	defer s.invalidMu.Unlock()
	if !s.invalidated {
		return ""
	}
	return s.invalidReason
}

func (s *drillControlAnchorState) invalidate(reason string) {
	if s == nil {
		return
	}
	s.invalidMu.Lock()
	if !s.invalidated {
		s.invalidated = true
		s.invalidReason = reason
	}
	s.invalidMu.Unlock()
}

// drillControlAnchorRecheckSession verifies the live session under the actual
// lock mutex, then verifies genuine Health outside the mutex.
func drillControlAnchorRecheckSession(ctx context.Context, state *drillControlAnchorState) error {
	lock := state.lock
	if lock == nil {
		return drillControlAnchorRefuse("control lock is absent")
	}
	if err := drillControlAnchorAcquireLockContext(ctx, &lock.mu); err != nil {
		return drillControlAnchorRefuse("control lock mutex was not acquired within the bounded context")
	}
	facts, readErr := drillControlAnchorReadLocked(ctx, lock)
	lock.mu.Unlock()
	if readErr != nil {
		return readErr
	}
	if facts.backendPID != state.backendPID ||
		!facts.backendStart.Equal(state.backendStart) ||
		facts.controlDatabaseOID != state.controlDatabaseOID ||
		facts.systemIdentifier != state.systemIdentifier ||
		!facts.postmasterStart.Equal(state.postmasterStart) ||
		facts.namespaceClassID != state.namespaceClassID ||
		facts.namespaceObjectID != state.namespaceObjectID ||
		facts.namespaceDatabaseID != state.namespaceDatabaseID {
		return drillControlAnchorRefuse("captured control SQL identity changed")
	}
	if facts.netConn != state.netConn {
		return drillControlAnchorRefuse("control connection identity changed")
	}
	if !drillControlAnchorSameTCP(facts.localAddr, state.localAddr) ||
		!drillControlAnchorSameTCP(facts.remoteAddr, state.remoteAddr) {
		return drillControlAnchorRefuse("control connection tuple changed")
	}
	return nil
}

// drillControlAnchorReadLocked reads the actual facts from the same live
// dedicated PGX connection. The caller must hold the actual lock mutex.
func drillControlAnchorReadLocked(ctx context.Context, lock *TargetLock) (drillControlAnchorReadFacts, error) {
	if lock == nil || lock.conn == nil || lock.closed {
		return drillControlAnchorReadFacts{}, drillControlAnchorRefuse("control session is closed")
	}
	pgxConn, ok := lock.conn.(*pgx.Conn)
	if !ok || pgxConn == nil {
		return drillControlAnchorReadFacts{}, drillControlAnchorRefuse("control session is not the dedicated PGX connection")
	}
	pgConn := pgxConn.PgConn()
	if pgConn == nil || pgConn.IsClosed() {
		return drillControlAnchorReadFacts{}, drillControlAnchorRefuse("control connection is closed")
	}
	netConn := pgConn.Conn()
	if netConn == nil {
		return drillControlAnchorReadFacts{}, drillControlAnchorRefuse("control connection has no live transport")
	}
	local, ok := netConn.LocalAddr().(*net.TCPAddr)
	if !ok || local == nil || local.Port <= 0 || local.IP.IsUnspecified() {
		return drillControlAnchorReadFacts{}, drillControlAnchorRefuse("control local address is not a live TCP tuple")
	}
	remote, ok := netConn.RemoteAddr().(*net.TCPAddr)
	if !ok || remote == nil || remote.Port <= 0 || remote.IP.IsUnspecified() {
		return drillControlAnchorReadFacts{}, drillControlAnchorRefuse("control remote address is not a live TCP tuple")
	}
	var facts drillControlAnchorReadFacts
	if err := lock.conn.QueryRow(ctx, `
SELECT pid::int, backend_start, pg_postmaster_start_time(),
       (SELECT oid FROM pg_database WHERE datname = current_database())
FROM pg_stat_activity WHERE pid = pg_backend_pid()`).
		Scan(&facts.backendPID, &facts.backendStart, &facts.postmasterStart, &facts.controlDatabaseOID); err != nil {
		return drillControlAnchorReadFacts{}, drillControlAnchorRefuse("control SQL identity is unavailable")
	}
	if facts.backendPID <= 0 || facts.backendStart.IsZero() || facts.postmasterStart.IsZero() || facts.controlDatabaseOID == 0 {
		return drillControlAnchorReadFacts{}, drillControlAnchorRefuse("control SQL identity is incomplete")
	}
	facts.namespaceClassID = uint32(lock.key1)
	facts.namespaceObjectID = uint32(lock.key2)
	facts.namespaceDatabaseID = facts.controlDatabaseOID
	var namespaceHeld bool
	if err := lock.conn.QueryRow(ctx, `
SELECT EXISTS (
  SELECT 1 FROM pg_locks
  WHERE locktype = 'advisory' AND granted AND pid = pg_backend_pid()
    AND objsubid = 2 AND classid = $1::oid AND objid = $2::oid AND database = $3::oid
)`, facts.namespaceClassID, facts.namespaceObjectID, facts.namespaceDatabaseID).Scan(&namespaceHeld); err != nil {
		return drillControlAnchorReadFacts{}, drillControlAnchorRefuse("control namespace advisory lock is unreadable")
	}
	if !namespaceHeld {
		return drillControlAnchorReadFacts{}, drillControlAnchorRefuse("control namespace advisory lock is not held in the actual control database")
	}
	if err := lock.conn.QueryRow(ctx, `SELECT system_identifier::text FROM pg_control_system()`).Scan(&facts.systemIdentifier); err != nil || facts.systemIdentifier == "" {
		return drillControlAnchorReadFacts{}, drillControlAnchorRefuse("control cluster identity is unavailable")
	}
	facts.localAddr = drillControlAnchorCopyTCP(local)
	facts.remoteAddr = drillControlAnchorCopyTCP(remote)
	facts.netConn = netConn
	if inode, ok := drillControlAnchorSocketInode(netConn); ok {
		facts.socketInode = inode
	}
	return facts, nil
}

// drillControlAnchorMatchBinding requires every captured fact to equal the
// immutable factory binding, including the logical original DataTargetKey
// namespace (never the transport key).
func drillControlAnchorMatchBinding(r *DrillBorrowedWriterRun, lock *TargetLock, facts drillControlAnchorReadFacts) error {
	binding := r.binding
	k1, k2 := binding.originalTargetKey.AdvisoryLockKey()
	if binding.originalTargetKey == (TargetKey{}) || binding.controlTargetKey == (TargetKey{}) ||
		binding.originalRoleFingerprint == "" || binding.originalOperationID == "" {
		return drillControlAnchorRefuse("factory binding identity is incomplete")
	}
	if lock.controlKey != binding.controlTargetKey || lock.controlKey == (TargetKey{}) {
		return drillControlAnchorRefuse("control lock key does not match the factory capture")
	}
	if lock.key1 != k1 || lock.key2 != k2 {
		return drillControlAnchorRefuse("control lock namespace does not match the logical original DataTargetKey")
	}
	if facts.namespaceClassID != uint32(k1) || facts.namespaceObjectID != uint32(k2) {
		return drillControlAnchorRefuse("granted advisory namespace does not match the logical original DataTargetKey")
	}
	if facts.backendPID != binding.controlBackendPID || !facts.backendStart.Equal(binding.controlBackendStart) {
		return drillControlAnchorRefuse("control backend identity does not match the factory capture")
	}
	if facts.controlDatabaseOID != binding.controlDatabaseOID {
		return drillControlAnchorRefuse("control database OID does not match the factory capture")
	}
	if facts.systemIdentifier != binding.controlClusterIdentifier || facts.systemIdentifier == "" {
		return drillControlAnchorRefuse("control cluster identity does not match the factory capture")
	}
	if !facts.postmasterStart.Equal(binding.controlPostmasterStart) || binding.controlPostmasterStart.IsZero() {
		return drillControlAnchorRefuse("control postmaster incarnation does not match the factory capture")
	}
	return nil
}

// drillControlAnchorBounded ALWAYS derives a subcontext bounded by the
// mini-phase-1 fact bound, so an arbitrarily long caller deadline can never
// extend the SQL/socket/namespace read window; a sooner caller deadline is
// respected automatically by WithTimeout. A nil context is refused; a
// canceled context yields an immediately-done bounded context whose reads
// fail closed. The genuine Health stage is separately capped at one second.
func drillControlAnchorBounded(ctx context.Context) (context.Context, context.CancelFunc, error) {
	if ctx == nil {
		return nil, nil, drillControlAnchorRefuse("bounded context is required")
	}
	bounded, cancel := context.WithTimeout(ctx, drillControlAnchorFactBound)
	return bounded, cancel, nil
}

// drillControlAnchorAcquireLockContext acquires the actual control mutex
// without ever spawning a locker goroutine: TryLock is retried on a ticker
// until the bounded context ends.
func drillControlAnchorAcquireLockContext(ctx context.Context, mu *sync.Mutex) error {
	if ctx == nil || mu == nil {
		return drillControlAnchorRefuse("bounded context and actual control mutex are required")
	}
	if mu.TryLock() {
		return nil
	}
	ticker := time.NewTicker(drillControlAnchorMutexPoll)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if mu.TryLock() {
				return nil
			}
		}
	}
}

func drillControlAnchorCopyTCP(addr *net.TCPAddr) *net.TCPAddr {
	if addr == nil {
		return nil
	}
	copied := *addr
	copied.IP = append(net.IP(nil), addr.IP...)
	return &copied
}

func drillControlAnchorSameTCP(a, b *net.TCPAddr) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Port == b.Port && a.IP.Equal(b.IP) && a.Zone == b.Zone
}

func drillControlAnchorPort(addr *net.TCPAddr) int {
	if addr == nil {
		return 0
	}
	return addr.Port
}

// drillControlAnchorSocketInode is the optional best-effort kernel socket
// identity of the same live connection. It is not mandatory for mini-phase-1
// and returns ok=false when no real syscall.Conn is safely available.
func drillControlAnchorSocketInode(conn net.Conn) (uint64, bool) {
	syscallConn, ok := conn.(syscall.Conn)
	if !ok {
		return 0, false
	}
	raw, err := syscallConn.SyscallConn()
	if err != nil {
		return 0, false
	}
	var inode uint64
	var statErr error
	if err := raw.Control(func(fd uintptr) {
		var st syscall.Stat_t
		if err := syscall.Fstat(int(fd), &st); err != nil {
			statErr = err
			return
		}
		inode = st.Ino
	}); err != nil || statErr != nil || inode == 0 {
		return 0, false
	}
	return inode, true
}

// ---------------------------------------------------------------------------
// Fixed same-owner autocommit DROP/CREATE seam (drill-only)
// ---------------------------------------------------------------------------

// drillOwnerDDLBound bounds the fixed owner DDL slice when the caller supplied
// no nearer deadline. It is intentionally larger than the fact bound because a
// DROP/CREATE of the small fixture database is a real catalog operation, while
// every caller deadline still clips it.
const drillOwnerDDLBound = 30 * time.Second

// DrillOwnerDDLOutcome is the fixed read-only projection of one same-owner
// DROP/CREATE attempt: which fixed steps completed, whether the terminal state
// is UNKNOWN/incomplete, and the observed catalog OIDs. It grants no authority
// and is diagnostic only.
type DrillOwnerDDLOutcome struct {
	Dropped      bool
	Created      bool
	Unknown      bool
	OldTargetOID uint32
	NewTargetOID uint32
}

// drillOwnerDDLRefusal is the fixed safe refusal of the owner DDL seam: only
// the fixed stage and a sanitized SQLSTATE appear; no SQL text, DSN, role or
// database name is surfaced.
type drillOwnerDDLRefusal struct {
	stage string
	code  string
	cause error
}

func (e *drillOwnerDDLRefusal) Error() string {
	return "owner DDL " + e.stage + " refused (sqlstate=" + e.code + ")"
}

func (e *drillOwnerDDLRefusal) Unwrap() error { return e.cause }

func drillOwnerDDLRefuse(stage string, cause error) error {
	return &drillOwnerDDLRefusal{stage: stage, code: drillOwnerDDLSQLState(cause), cause: cause}
}

// drillOwnerDDLSQLState extracts the SQLSTATE by unwrapping the error chain
// without importing any extra package; an unknown failure is "no-sqlstate",
// never a guessed code.
func drillOwnerDDLSQLState(err error) string {
	for err != nil {
		if stater, ok := err.(interface{ SQLState() string }); ok {
			return stater.SQLState()
		}
		unwrapper, ok := err.(interface{ Unwrap() error })
		if !ok {
			return "no-sqlstate"
		}
		err = unwrapper.Unwrap()
	}
	return "no-sqlstate"
}

// drillOwnerDDLStage names the narrow test-only pause point of the owner DDL
// seam. It is not an authorization callback and is never consulted by any
// production path (this whole file is drill-tagged).
type drillOwnerDDLStage string

const drillOwnerDDLStageBeforeDrop drillOwnerDDLStage = "before-drop"

// drillOwnerDDLStageHook is the per-process test pause seam, read atomically so
// no global race is possible.
var drillOwnerDDLStageHook atomic.Pointer[func(drillOwnerDDLStage)]

func drillOwnerDDLPauseAt(stage drillOwnerDDLStage) {
	if hook := drillOwnerDDLStageHook.Load(); hook != nil {
		(*hook)(stage)
	}
}

// DrillOwnerDDLTestHook installs the per-process owner DDL pause hook and
// returns a reset function. It is a drill-only test seam and grants no
// authority; a nil hook clears the seam.
func DrillOwnerDDLTestHook(hook func(stage string)) func() {
	if hook == nil {
		drillOwnerDDLStageHook.Store(nil)
		return func() { drillOwnerDDLStageHook.Store(nil) }
	}
	wrapped := func(stage drillOwnerDDLStage) { hook(string(stage)) }
	drillOwnerDDLStageHook.Store(&wrapped)
	return func() { drillOwnerDDLStageHook.Store(nil) }
}

// DrillOwnerTargetDropCreate performs the fixed validated same-owner autocommit
// DDL slice: DROP DATABASE <original target> (never FORCE) then CREATE DATABASE
// <original target> OWNER <original writer role>, both through the ORIGINAL
// dedicated control connection outside any transaction. It is controller-bound
// to the factory-created run, validates the immutable factory binding, acquires
// the actual control mutex with a bounded caller wait, checks the original
// owner identity and granted advisory namespace through that same connection,
// derives the fixed database/role names from the validated catalog OIDs, and
// records partial/unknown outcomes. It accepts no SQL text, no callback, no raw
// connection and no admin/pool substitution; a cancellation or owner loss never
// retries with a replacement owner.
func (r *DrillBorrowedWriterRun) DrillOwnerTargetDropCreate(ctx context.Context) (DrillOwnerDDLOutcome, error) {
	var outcome DrillOwnerDDLOutcome
	if r == nil || r.life == nil || r.lock == nil {
		return outcome, &drillOwnerDDLRefusal{stage: "binding", code: "no-sqlstate"}
	}
	binding := r.binding
	if !binding.SQLFactsComplete() {
		return outcome, &drillOwnerDDLRefusal{stage: "binding", code: "no-sqlstate"}
	}
	outcome.OldTargetOID = binding.targetDatabaseOID
	if ctx == nil {
		return outcome, &drillOwnerDDLRefusal{stage: "context", code: "no-sqlstate"}
	}
	bounded, cancel := context.WithTimeout(ctx, drillOwnerDDLBound)
	defer cancel()
	if err := drillControlAnchorAcquireLockContext(bounded, &r.lock.mu); err != nil {
		return outcome, &drillOwnerDDLRefusal{stage: "serialization", code: "no-sqlstate"}
	}
	defer r.lock.mu.Unlock()
	facts, readErr := drillControlAnchorReadLocked(bounded, r.lock)
	if readErr != nil {
		return outcome, &drillOwnerDDLRefusal{stage: "identity", code: "no-sqlstate"}
	}
	if err := drillControlAnchorMatchBinding(r, r.lock, facts); err != nil {
		return outcome, &drillOwnerDDLRefusal{stage: "identity", code: "no-sqlstate"}
	}
	var targetName, roleName string
	var targetOwner uint32
	if err := r.lock.conn.QueryRow(bounded, `SELECT datname FROM pg_database WHERE oid = $1`, binding.targetDatabaseOID).Scan(&targetName); err != nil || targetName == "" {
		return outcome, &drillOwnerDDLRefusal{stage: "target-name", code: "no-sqlstate"}
	}
	if err := r.lock.conn.QueryRow(bounded, `SELECT rolname FROM pg_roles WHERE oid = $1`, binding.writerRoleOID).Scan(&roleName); err != nil || roleName == "" {
		return outcome, &drillOwnerDDLRefusal{stage: "writer-name", code: "no-sqlstate"}
	}
	if err := r.lock.conn.QueryRow(bounded, `SELECT datdba::oid FROM pg_database WHERE oid = $1`, binding.targetDatabaseOID).Scan(&targetOwner); err != nil || targetOwner != binding.writerRoleOID {
		return outcome, &drillOwnerDDLRefusal{stage: "target-binding", code: "no-sqlstate"}
	}
	// Validate the OID-resolved names against the immutable factory logical
	// binding BEFORE any DDL: a renamed target or writer identity is refused
	// pre-DDL (no DROP). No concurrent-rename exclusion is claimed.
	if r.state == nil || r.state.bound == nil {
		return outcome, &drillOwnerDDLRefusal{stage: "logical-binding", code: "no-sqlstate"}
	}
	bound := r.state.bound
	if bound.Database == "" || bound.Role == "" || bound.TargetKey != binding.originalTargetKey || bound.RoleFingerprint != binding.originalRoleFingerprint {
		return outcome, &drillOwnerDDLRefusal{stage: "logical-binding", code: "no-sqlstate"}
	}
	if targetName != bound.Database || roleName != bound.Role {
		return outcome, &drillOwnerDDLRefusal{stage: "renamed-identity", code: "no-sqlstate"}
	}
	// Test-only pause point strictly before the fixed DROP.
	drillOwnerDDLPauseAt(drillOwnerDDLStageBeforeDrop)
	// Fixed validated DROP without FORCE: a confirmed server rejection (e.g. a
	// held target connection with 55006) means the DROP definitively did not
	// run and the CREATE step is never attempted; an unconfirmed completion
	// (transport/deadline/cancel without any SQLSTATE) is recorded UNKNOWN.
	if _, dropErr := r.lock.conn.Exec(bounded, `DROP DATABASE `+pgx.Identifier{targetName}.Sanitize()); dropErr != nil {
		if drillOwnerDDLSQLState(dropErr) == "no-sqlstate" {
			outcome.Unknown = true
			return outcome, drillOwnerDDLRefuse("drop-unconfirmed", dropErr)
		}
		return outcome, drillOwnerDDLRefuse("drop", dropErr)
	}
	outcome.Dropped = true
	// Fixed validated CREATE of the same name owned by the original writer
	// role. A failure here is a partial/UNKNOWN terminal state.
	if _, createErr := r.lock.conn.Exec(bounded, `CREATE DATABASE `+pgx.Identifier{targetName}.Sanitize()+` OWNER `+pgx.Identifier{roleName}.Sanitize()); createErr != nil {
		outcome.Unknown = true
		return outcome, drillOwnerDDLRefuse("create-after-drop", createErr)
	}
	outcome.Created = true
	var newOID uint32
	if err := r.lock.conn.QueryRow(bounded, `SELECT oid::oid FROM pg_database WHERE datname = $1`, targetName).Scan(&newOID); err != nil || newOID == 0 || newOID == binding.targetDatabaseOID {
		outcome.Unknown = true
		return outcome, &drillOwnerDDLRefusal{stage: "replacement-identity", code: "no-sqlstate"}
	}
	var newOwner uint32
	if err := r.lock.conn.QueryRow(bounded, `SELECT datdba::oid FROM pg_database WHERE oid = $1`, newOID).Scan(&newOwner); err != nil || newOwner != binding.writerRoleOID {
		outcome.Unknown = true
		return outcome, &drillOwnerDDLRefusal{stage: "replacement-owner", code: "no-sqlstate"}
	}
	outcome.NewTargetOID = newOID
	// Post-DDL identity/namespace re-check under the same serialization: the
	// same original owner must still hold the same granted namespace.
	postFacts, postErr := drillControlAnchorReadLocked(bounded, r.lock)
	if postErr != nil {
		outcome.Unknown = true
		return outcome, &drillOwnerDDLRefusal{stage: "post-identity", code: "no-sqlstate"}
	}
	if err := drillControlAnchorMatchBinding(r, r.lock, postFacts); err != nil {
		outcome.Unknown = true
		return outcome, &drillOwnerDDLRefusal{stage: "post-identity", code: "no-sqlstate"}
	}
	return outcome, nil
}
