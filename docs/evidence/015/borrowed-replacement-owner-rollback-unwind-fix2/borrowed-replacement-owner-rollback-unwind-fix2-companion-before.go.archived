//go:build linux && drill

// targetlock_commitambiguity_hook_linux_test.go is the drill-only fixed
// companion of the owner COMMIT acknowledgement-loss lane. It reserves the
// unexported production socket-wrap seam with ONE fixed wire gate and exposes
// ONLY an opaque control handle with fixed fault operations (arm/withhold/
// break/witness/query). Every handle has an IMMUTABLE mode assigned at
// construction: a reserved NATIVE-fault handle can NEVER use the
// deterministic byte-feed/read/stall controls (from reservation onward, not
// merely after wrapping), and a deterministic CONTROL handle is never
// installed into the production seam. The live-socket adapter is UNEXPORTED
// and is the only net.Conn: the exported handle has NO Read/Write/Close
// capability. WITHHELD is published only after the ACTUAL PostgreSQL COMMIT
// completion has been bounded-assembled and strictly validated
// (CommandComplete body exactly "COMMIT\0" followed by a length-5
// ReadyForQuery with the idle status). Assembly and both deadline setters use
// ONE synchronized deadline path computing the MINIMUM of the CURRENT caller
// deadline and the active assembly cap, applied as an absolute timestamp
// without holding a lock across a read. EOF, read errors, deadline expiry,
// partial and unrecognized/malformed framing stay NOTESTABLISHED/UNCERTAIN and
// are never inferred as a rollback or a success.
package recovery

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"time"
)

// Fixed witness strings of the fault control.
const (
	TargetLockCommitWitnessNotEstablished = "not-established"
	TargetLockCommitWitnessDispatched     = "dispatched"
	TargetLockCommitWitnessWithheld       = "withheld"
)

// targetLockCommitAmbiguityMode is the IMMUTABLE handle mode assigned at
// construction.
type targetLockCommitAmbiguityMode int

const (
	targetLockCommitAmbiguityNativeMode targetLockCommitAmbiguityMode = iota
	targetLockCommitAmbiguityControlMode
)

// Sealed wire-gate command kind: fixed at the FIRST arm and never relabeled.
const (
	targetLockCommandUnset = iota
	targetLockCommandCommit
	targetLockCommandRollback
)

// armCommand seals the command kind at the first arm and rejects a conflicting
// re-arm; the same kind is idempotent.
func (c *targetLockCommitAmbiguityCore) armCommand(kind int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.commandKind == targetLockCommandUnset {
		c.commandKind = kind
	} else if c.commandKind != kind {
		return errors.New("target-lock commit fault: conflicting command kind (the command kind is sealed at first arm)")
	}
	c.armed = true
	return nil
}

// targetLockCommitAmbiguityCore is the unexported shared state of one fault.
type targetLockCommitAmbiguityCore struct {
	real net.Conn

	mu               sync.Mutex
	live             bool
	armed            bool
	commandKind      int
	commitObserved   bool
	rollbackObserved bool
	dispatched       bool
	withheld         bool
	broken           bool
	readFailure      bool
	unrecognized     int
	wrapped          int
	withholdErr      error
	assembledBytes   int

	dispatchedCh chan struct{}
	withheldCh   chan struct{}
	brokenCh     chan struct{}
	withholdOnce sync.Once
	breakOnce    sync.Once

	// deterministic in-memory transport of the control instance
	controlMu           sync.Mutex
	controlServer       bytes.Buffer
	controlServerErr    error
	controlConsumed     int
	controlStallAfter   int
	controlStallOngoing bool
	controlStallCh      chan struct{}
	controlWakeCh       chan struct{}

	// ONE synchronized deadline state: the caller deadline (set by both
	// setters) and the active assembly cap; the effective read deadline is
	// their MINIMUM.
	deadlineMu      sync.Mutex
	callerDeadline  time.Time
	assemblyCap     time.Time
	deadlineChanged chan struct{}
}

func newTargetLockCommitAmbiguityCore() *targetLockCommitAmbiguityCore {
	return &targetLockCommitAmbiguityCore{
		dispatchedCh:      make(chan struct{}),
		withheldCh:        make(chan struct{}),
		brokenCh:          make(chan struct{}),
		deadlineChanged:   make(chan struct{}, 1),
		controlStallAfter: -1,
		controlStallCh:    make(chan struct{}, 1),
		controlWakeCh:     make(chan struct{}, 1),
	}
}

// targetLockCommitAmbiguitySocket is the UNEXPORTED live-socket adapter: the
// only net.Conn of this companion. It is returned to the production seam and
// never handed to callers.
type targetLockCommitAmbiguitySocket struct {
	core *targetLockCommitAmbiguityCore
}

func (s *targetLockCommitAmbiguitySocket) Read(p []byte) (int, error) {
	return s.core.read(p)
}

func (s *targetLockCommitAmbiguitySocket) Write(p []byte) (int, error) {
	return s.core.write(p)
}

func (s *targetLockCommitAmbiguitySocket) Close() error {
	s.core.breakTransport()
	return nil
}

func (s *targetLockCommitAmbiguitySocket) LocalAddr() net.Addr {
	return s.core.localAddr()
}

func (s *targetLockCommitAmbiguitySocket) RemoteAddr() net.Addr {
	return s.core.remoteAddr()
}

func (s *targetLockCommitAmbiguitySocket) SetDeadline(t time.Time) error {
	return s.core.setDeadline(t)
}

func (s *targetLockCommitAmbiguitySocket) SetReadDeadline(t time.Time) error {
	return s.core.setReadDeadline(t)
}

func (s *targetLockCommitAmbiguitySocket) SetWriteDeadline(t time.Time) error {
	return s.core.setWriteDeadline(t)
}

// TargetLockCommitAmbiguityFault is the fixed OPAQUE control handle. It exposes
// only fixed fault operations; it is not a net.Conn and has no Read/Write/Close
// capability. The mode is IMMUTABLE: a reserved native-fault handle rejects
// every deterministic control from reservation onward.
type TargetLockCommitAmbiguityFault struct {
	core *targetLockCommitAmbiguityCore
	mode targetLockCommitAmbiguityMode
}

// controlAllowed rejects every deterministic control on a native-fault handle
// (reserved or live) using the IMMUTABLE construction mode.
func (f *TargetLockCommitAmbiguityFault) controlAllowed() error {
	if f.mode != targetLockCommitAmbiguityControlMode {
		return errors.New("target-lock commit fault: deterministic controls are unavailable on a reserved native-fault handle")
	}
	if f.core.isLive() {
		return errors.New("target-lock commit fault: deterministic controls are unavailable on a live-socket fault")
	}
	return nil
}

// ArmTargetLockCommitAmbiguityFault reserves the production socket-wrap seam
// with this fixed fault and returns the opaque NATIVE handle, the
// identity-conditional reset and an error for an overlapping reservation. No
// socket callback is accepted.
func ArmTargetLockCommitAmbiguityFault() (*TargetLockCommitAmbiguityFault, func(), error) {
	core := newTargetLockCommitAmbiguityCore()
	reset, err := reserveTargetLockSocketWrap(core.wrap)
	if err != nil {
		return nil, nil, err
	}
	return &TargetLockCommitAmbiguityFault{core: core, mode: targetLockCommitAmbiguityNativeMode}, reset, nil
}

// NewTargetLockCommitAmbiguityControl builds the deterministic CONTROL
// instance (never installed into the production seam) whose fixed in-memory
// transport is driven through ControlWriteClientBytes / ControlFeedServerBytes
// / ControlFeedServerError / ControlRead / ControlStallAfterBytes.
func NewTargetLockCommitAmbiguityControl() *TargetLockCommitAmbiguityFault {
	return &TargetLockCommitAmbiguityFault{core: newTargetLockCommitAmbiguityCore(), mode: targetLockCommitAmbiguityControlMode}
}

// TargetLockCommitRequestFrame is the exact simple-protocol commit query frame
// pgx sends for tx.Commit.
func TargetLockCommitRequestFrame() []byte {
	sql := "commit"
	frame := make([]byte, 5+len(sql)+1)
	frame[0] = 'Q'
	binary.BigEndian.PutUint32(frame[1:5], uint32(4+len(sql)+1))
	copy(frame[5:], sql)
	return frame
}

// TargetLockCommitCompletionFrames is the REAL PostgreSQL COMMIT completion:
// CommandComplete with the exact body "COMMIT\0" followed by ReadyForQuery
// with the idle transaction status.
func TargetLockCommitCompletionFrames() []byte {
	tag := []byte("COMMIT\x00")
	frames := make([]byte, 0, 32)
	frames = append(frames, 'C')
	frames = binary.BigEndian.AppendUint32(frames, uint32(4+len(tag)))
	frames = append(frames, tag...)
	frames = append(frames, 'Z')
	frames = binary.BigEndian.AppendUint32(frames, 5)
	frames = append(frames, 'I')
	return frames
}

// ArmGate arms the wire gate: the ACTUAL commit request is then recognized,
// forwarded (the server really commits) and its strictly validated completion
// withheld before pgx receives it.
func (f *TargetLockCommitAmbiguityFault) ArmGate() error {
	return f.core.armCommand(targetLockCommandCommit)
}

// TargetLockRollbackRequestFrame is the exact simple-protocol rollback query
// frame pgx sends for tx.Rollback.
func TargetLockRollbackRequestFrame() []byte {
	sql := "rollback"
	frame := make([]byte, 5+len(sql)+1)
	frame[0] = 'Q'
	binary.BigEndian.PutUint32(frame[1:5], uint32(4+len(sql)+1))
	copy(frame[5:], sql)
	return frame
}

// TargetLockRollbackCompletionFrames is the REAL PostgreSQL ROLLBACK
// completion: CommandComplete with the exact body "ROLLBACK\0" followed by
// ReadyForQuery with the idle transaction status.
func TargetLockRollbackCompletionFrames() []byte {
	tag := []byte("ROLLBACK\x00")
	frames := make([]byte, 0, 32)
	frames = append(frames, 'C')
	frames = binary.BigEndian.AppendUint32(frames, uint32(4+len(tag)))
	frames = append(frames, tag...)
	frames = append(frames, 'Z')
	frames = binary.BigEndian.AppendUint32(frames, 5)
	frames = append(frames, 'I')
	return frames
}

// ArmRollbackGate arms the FIXED rollback-only gate: the ACTUAL rollback
// request is recognized, forwarded (the server really rolls back) and its
// strictly validated completion withheld before pgx receives it. No SQL,
// connector or socket callback authority is added.
func (f *TargetLockCommitAmbiguityFault) ArmRollbackGate() error {
	return f.core.armCommand(targetLockCommandRollback)
}

// Break breaks the owner transport so the ACTUAL Commit call returns an error.
func (f *TargetLockCommitAmbiguityFault) Break() {
	f.core.breakTransport()
}

// Witness returns ONLY the ordering actually established.
func (f *TargetLockCommitAmbiguityFault) Witness() string {
	f.core.mu.Lock()
	defer f.core.mu.Unlock()
	if f.core.withheld {
		return TargetLockCommitWitnessWithheld
	}
	if f.core.dispatched {
		return TargetLockCommitWitnessDispatched
	}
	return TargetLockCommitWitnessNotEstablished
}

// WrappedCount is the number of real sockets the fixed adapter has seen.
func (f *TargetLockCommitAmbiguityFault) WrappedCount() int {
	f.core.mu.Lock()
	defer f.core.mu.Unlock()
	return f.core.wrapped
}

// UnrecognizedFramingCount counts client writes the fixed classifier could not
// recognize while armed.
func (f *TargetLockCommitAmbiguityFault) UnrecognizedFramingCount() int {
	f.core.mu.Lock()
	defer f.core.mu.Unlock()
	return f.core.unrecognized
}

// ReadFailed reports an EOF/error/partial/unrecognized/malformed completion
// read.
func (f *TargetLockCommitAmbiguityFault) ReadFailed() bool {
	f.core.mu.Lock()
	defer f.core.mu.Unlock()
	return f.core.readFailure
}

// AssembledBytes is the number of server bytes the last completion assembly
// actually consumed (success or failure).
func (f *TargetLockCommitAmbiguityFault) AssembledBytes() int {
	f.core.mu.Lock()
	defer f.core.mu.Unlock()
	return f.core.assembledBytes
}

// CleanupUncertain reports a broken or failed-read transport whose COMMIT
// completion was never withheld: the actual outcome is UNKNOWN and must never
// be inferred as a rollback.
func (f *TargetLockCommitAmbiguityFault) CleanupUncertain() bool {
	f.core.mu.Lock()
	defer f.core.mu.Unlock()
	return (f.core.broken || f.core.readFailure) && !f.core.withheld
}

// CommandKind reports the SEALED command kind ("unset", "commit" or
// "rollback").
func (f *TargetLockCommitAmbiguityFault) CommandKind() string {
	f.core.mu.Lock()
	defer f.core.mu.Unlock()
	switch f.core.commandKind {
	case targetLockCommandCommit:
		return "commit"
	case targetLockCommandRollback:
		return "rollback"
	default:
		return "unset"
	}
}

// RollbackMode reports the sealed rollback gate mode.
func (f *TargetLockCommitAmbiguityFault) RollbackMode() bool {
	f.core.mu.Lock()
	defer f.core.mu.Unlock()
	return f.core.commandKind == targetLockCommandRollback
}

// CommitObserved reports an ACTUAL commit frame forwarded during the armed
// interval, independently of the selected command kind.
func (f *TargetLockCommitAmbiguityFault) CommitObserved() bool {
	f.core.mu.Lock()
	defer f.core.mu.Unlock()
	return f.core.commitObserved
}

// RollbackObserved reports an ACTUAL rollback frame forwarded during the armed
// interval, independently of the selected command kind.
func (f *TargetLockCommitAmbiguityFault) RollbackObserved() bool {
	f.core.mu.Lock()
	defer f.core.mu.Unlock()
	return f.core.rollbackObserved
}

// RollbackDispatched reports the ACTUAL rollback dispatch witness of the
// sealed rollback kind.
func (f *TargetLockCommitAmbiguityFault) RollbackDispatched() bool {
	f.core.mu.Lock()
	defer f.core.mu.Unlock()
	return f.core.dispatched && f.core.commandKind == targetLockCommandRollback
}

// RollbackWithheld reports the strictly validated withheld rollback completion.
func (f *TargetLockCommitAmbiguityFault) RollbackWithheld() bool {
	f.core.mu.Lock()
	defer f.core.mu.Unlock()
	return f.core.withheld && f.core.commandKind == targetLockCommandRollback
}

// CommitDispatched reports the ACTUAL commit dispatch witness of the sealed
// commit kind; a COMMIT observed while expecting ROLLBACK is reported through
// CommitObserved instead.
func (f *TargetLockCommitAmbiguityFault) CommitDispatched() bool {
	f.core.mu.Lock()
	defer f.core.mu.Unlock()
	return f.core.dispatched && f.core.commandKind == targetLockCommandCommit
}

// AwaitDispatched waits boundedly for the actual COMMIT dispatch witness.
func (f *TargetLockCommitAmbiguityFault) AwaitDispatched(ctx context.Context) bool {
	select {
	case <-f.core.dispatchedCh:
		return true
	case <-ctx.Done():
		return false
	}
}

// AwaitWithheld waits boundedly for the strictly validated withheld completion
// witness.
func (f *TargetLockCommitAmbiguityFault) AwaitWithheld(ctx context.Context) bool {
	select {
	case <-f.core.withheldCh:
		return true
	case <-ctx.Done():
		return false
	}
}

// ControlWriteClientBytes drives the fixed classifier with client bytes; it
// REJECTS native-fault handles from reservation onward.
func (f *TargetLockCommitAmbiguityFault) ControlWriteClientBytes(b []byte) error {
	if err := f.controlAllowed(); err != nil {
		return err
	}
	f.core.maybeEstablishDispatch(b)
	return nil
}

// ControlFeedServerBytes queues server bytes for the deterministic transport;
// it REJECTS native-fault handles from reservation onward.
func (f *TargetLockCommitAmbiguityFault) ControlFeedServerBytes(b []byte) error {
	if err := f.controlAllowed(); err != nil {
		return err
	}
	f.core.controlMu.Lock()
	f.core.controlServer.Write(b)
	f.core.controlMu.Unlock()
	select {
	case f.core.controlWakeCh <- struct{}{}:
	default:
	}
	return nil
}

// ControlFeedServerError queues one read failure for the deterministic
// transport; it REJECTS native-fault handles from reservation onward.
func (f *TargetLockCommitAmbiguityFault) ControlFeedServerError(err error) error {
	if err := f.controlAllowed(); err != nil {
		return err
	}
	f.core.controlMu.Lock()
	f.core.controlServerErr = err
	f.core.controlMu.Unlock()
	select {
	case f.core.controlWakeCh <- struct{}{}:
	default:
	}
	return nil
}

// ControlSetReadDeadline sets the tracked caller read deadline of the
// deterministic control instance; it REJECTS native-fault handles from
// reservation onward.
func (f *TargetLockCommitAmbiguityFault) ControlSetReadDeadline(t time.Time) error {
	if err := f.controlAllowed(); err != nil {
		return err
	}
	return f.core.setReadDeadline(t)
}

// ControlStallAfterBytes arms the one-shot deterministic stall after n consumed
// server bytes; it REJECTS native-fault handles from reservation onward.
func (f *TargetLockCommitAmbiguityFault) ControlStallAfterBytes(n int) error {
	if err := f.controlAllowed(); err != nil {
		return err
	}
	if n < 0 {
		return errors.New("target-lock commit fault: stall byte offset must be >= 0")
	}
	select {
	case <-f.core.controlWakeCh:
	default:
	}
	f.core.controlMu.Lock()
	f.core.controlStallAfter = n
	f.core.controlMu.Unlock()
	return nil
}

// ControlStallReached waits boundedly until the armed deterministic stall is
// entered; it REJECTS native-fault handles from reservation onward.
func (f *TargetLockCommitAmbiguityFault) ControlStallReached(ctx context.Context) bool {
	if err := f.controlAllowed(); err != nil {
		return false
	}
	select {
	case <-f.core.controlStallCh:
		return true
	case <-ctx.Done():
		return false
	}
}

// ControlEffectiveDeadline returns the currently computed effective read
// deadline (the MINIMUM of the current caller deadline and the active assembly
// cap); it REJECTS native-fault handles from reservation onward.
func (f *TargetLockCommitAmbiguityFault) ControlEffectiveDeadline() (time.Time, error) {
	if err := f.controlAllowed(); err != nil {
		return time.Time{}, err
	}
	effective, _ := f.core.effectiveDeadline()
	return effective, nil
}

// ControlRead drives the fixed Read path once; it REJECTS native-fault handles
// from reservation onward.
func (f *TargetLockCommitAmbiguityFault) ControlRead(p []byte) (int, error) {
	if err := f.controlAllowed(); err != nil {
		return 0, err
	}
	return f.core.read(p)
}

func (c *targetLockCommitAmbiguityCore) isLive() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.live
}

// wrap is the fixed wrapper reserved into the production seam: it records the
// REAL established socket exactly once, marks the instance live and returns the
// UNEXPORTED adapter.
func (c *targetLockCommitAmbiguityCore) wrap(real net.Conn) net.Conn {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.wrapped++
	if real == nil || c.real != nil {
		return real
	}
	c.real = real
	c.live = true
	return &targetLockCommitAmbiguitySocket{core: c}
}

func (c *targetLockCommitAmbiguityCore) realConn() net.Conn {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.real
}

func (c *targetLockCommitAmbiguityCore) write(p []byte) (int, error) {
	if real := c.realConn(); real != nil {
		n, err := real.Write(p)
		if n > 0 {
			c.maybeEstablishDispatch(p[:n])
		}
		return n, err
	}
	return 0, errors.New("target-lock commit fault: no established socket")
}

func (c *targetLockCommitAmbiguityCore) maybeEstablishDispatch(p []byte) {
	c.mu.Lock()
	if !c.armed || c.dispatched {
		c.mu.Unlock()
		return
	}
	c.mu.Unlock()
	c.mu.Lock()
	if !c.armed {
		c.mu.Unlock()
		return
	}
	kind := c.commandKind
	c.mu.Unlock()
	commitMatch, commitRecognized := classifyTargetLockCommandFrame(p, "commit")
	rollbackMatch, rollbackRecognized := classifyTargetLockCommandFrame(p, "rollback")
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.armed {
		return
	}
	if commitMatch {
		c.commitObserved = true
	}
	if rollbackMatch {
		c.rollbackObserved = true
	}
	if !commitRecognized || !rollbackRecognized {
		c.unrecognized++
		return
	}
	if !c.dispatched && ((kind == targetLockCommandCommit && commitMatch) || (kind == targetLockCommandRollback && rollbackMatch)) {
		c.dispatched = true
		close(c.dispatchedCh)
	}
}

func (c *targetLockCommitAmbiguityCore) read(p []byte) (int, error) {
	c.mu.Lock()
	withhold := c.armed && c.dispatched && !c.withheld && !c.broken
	c.mu.Unlock()
	if !withhold {
		return c.readRaw(p)
	}
	c.withholdOnce.Do(func() {
		err := c.withholdCommitCompletion()
		c.mu.Lock()
		c.withholdErr = err
		if err == nil {
			c.withheld = true
		}
		c.mu.Unlock()
		if err == nil {
			close(c.withheldCh)
		}
	})
	c.mu.Lock()
	withholdErr := c.withholdErr
	withheld := c.withheld
	c.mu.Unlock()
	if withholdErr != nil {
		return 0, withholdErr
	}
	if !withheld {
		return 0, net.ErrClosed
	}
	return 0, c.waitAfterWithhold()
}

// withholdCommitCompletion bounded-assembles and STRICTLY validates the actual
// PostgreSQL COMMIT completion; failure marks the read failed and NEVER
// publishes WITHHELD. It installs the active assembly cap and re-applies the
// CURRENT minimum (caller deadline vs cap) as an absolute timestamp before each
// read via the single synchronized deadline path.
func (c *targetLockCommitAmbiguityCore) withholdCommitCompletion() (err error) {
	const completionLimit = 1 << 16
	c.deadlineMu.Lock()
	c.assemblyCap = time.Now().Add(30 * time.Second)
	c.deadlineMu.Unlock()
	c.applyDeadlineToSocket()
	assembled := make([]byte, 0, 256)
	scratch := make([]byte, 512)
	defer func() {
		c.deadlineMu.Lock()
		c.assemblyCap = time.Time{}
		c.deadlineMu.Unlock()
		c.applyDeadlineToSocket()
		c.wakeDeadlineWaiters()
		c.mu.Lock()
		c.assembledBytes = len(assembled)
		c.mu.Unlock()
	}()
	for len(assembled) <= completionLimit {
		effective, set := c.effectiveDeadline()
		if set && !time.Now().Before(effective) {
			c.markReadFailure()
			return os.ErrDeadlineExceeded
		}
		// Re-apply the CURRENT minimum before each read: a stale snapshot can
		// never undo a concurrent cancellation, and a concurrent clear can never
		// remove the active assembly cap.
		c.applyDeadlineToSocket()
		n, readErr := c.readRaw(scratch)
		if n > 0 {
			assembled = append(assembled, scratch[:n]...)
		}
		if readErr != nil {
			c.markReadFailure()
			return readErr
		}
		c.mu.Lock()
		kind := c.commandKind
		c.mu.Unlock()
		completionTag := "COMMIT\x00"
		if kind == targetLockCommandRollback {
			completionTag = "ROLLBACK\x00"
		}
		if targetLockCommandCompletionAssembled(assembled, completionTag) {
			return nil
		}
	}
	c.markReadFailure()
	c.markUnrecognized()
	return errors.New("target-lock COMMIT completion framing was not recognized")
}

// effectiveDeadlineLocked computes the MINIMUM of the current caller deadline
// and the active assembly cap.
func (c *targetLockCommitAmbiguityCore) effectiveDeadlineLocked() (time.Time, bool) {
	effective := time.Time{}
	set := false
	if !c.callerDeadline.IsZero() {
		effective = c.callerDeadline
		set = true
	}
	if !c.assemblyCap.IsZero() && (!set || c.assemblyCap.Before(effective)) {
		effective = c.assemblyCap
		set = true
	}
	return effective, set
}

func (c *targetLockCommitAmbiguityCore) effectiveDeadline() (time.Time, bool) {
	c.deadlineMu.Lock()
	defer c.deadlineMu.Unlock()
	return c.effectiveDeadlineLocked()
}

// applyDeadlineToSocket serializes the minimum computation AND the absolute
// SetReadDeadline application under the SAME deadline mutex, so a concurrent
// cancellation can never be undone by a stale apply and a concurrent clear can
// never remove the active assembly cap. The lock is never held across a Read.
func (c *targetLockCommitAmbiguityCore) applyDeadlineToSocket() {
	real := c.realConn()
	c.deadlineMu.Lock()
	c.applyEffectiveDeadlineLocked(real)
	c.deadlineMu.Unlock()
}

// applyEffectiveDeadlineLocked computes the MINIMUM of the current caller
// deadline and the active assembly cap and applies it as an ABSOLUTE timestamp;
// the caller must hold the deadline mutex.
func (c *targetLockCommitAmbiguityCore) applyEffectiveDeadlineLocked(real net.Conn) {
	effective, set := c.effectiveDeadlineLocked()
	if real == nil {
		return
	}
	if set {
		_ = real.SetReadDeadline(effective)
	} else {
		_ = real.SetReadDeadline(time.Time{})
	}
}

func (c *targetLockCommitAmbiguityCore) wakeDeadlineWaiters() {
	select {
	case c.deadlineChanged <- struct{}{}:
	default:
	}
}

// waitAfterWithhold blocks until the transport is broken or the tracked caller
// deadline expires; deadline changes and Close are honored and a timeout fails
// closed without upgrading the witness.
func (c *targetLockCommitAmbiguityCore) waitAfterWithhold() error {
	for {
		c.deadlineMu.Lock()
		deadline := c.callerDeadline
		c.deadlineMu.Unlock()
		var timer *time.Timer
		var timerCh <-chan time.Time
		if !deadline.IsZero() {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				return os.ErrDeadlineExceeded
			}
			timer = time.NewTimer(remaining)
			timerCh = timer.C
		}
		select {
		case <-c.brokenCh:
			if timer != nil {
				timer.Stop()
			}
			return net.ErrClosed
		case <-timerCh:
			c.deadlineMu.Lock()
			current := c.callerDeadline
			c.deadlineMu.Unlock()
			if !current.IsZero() && !time.Now().Before(current) {
				return os.ErrDeadlineExceeded
			}
		case <-c.deadlineChanged:
			if timer != nil {
				timer.Stop()
			}
		}
	}
}

// stallWait waits until the armed deterministic stall is woken by an ACTUAL
// server feed or queued error (a stale wake without data keeps waiting), the
// effective deadline expires, or the transport is broken; a break is observed
// PROMPTLY and fails closed with a transport error.
func (c *targetLockCommitAmbiguityCore) stallWait() error {
	for {
		select {
		case <-c.brokenCh:
			return net.ErrClosed
		default:
		}
		effective, set := c.effectiveDeadline()
		if set {
			remaining := time.Until(effective)
			if remaining <= 0 {
				return os.ErrDeadlineExceeded
			}
			timer := time.NewTimer(remaining)
			select {
			case <-c.brokenCh:
				timer.Stop()
				return net.ErrClosed
			case <-c.controlWakeCh:
				timer.Stop()
				if c.controlHasData() {
					return nil
				}
				continue
			case <-c.deadlineChanged:
				timer.Stop()
				continue
			case <-timer.C:
				return os.ErrDeadlineExceeded
			}
		}
		select {
		case <-c.brokenCh:
			return net.ErrClosed
		case <-c.controlWakeCh:
			if c.controlHasData() {
				return nil
			}
			continue
		case <-c.deadlineChanged:
			continue
		}
	}
}

// controlHasData reports whether queued server bytes or a queued error are
// actually available for the deterministic transport.
func (c *targetLockCommitAmbiguityCore) controlHasData() bool {
	c.controlMu.Lock()
	defer c.controlMu.Unlock()
	return c.controlServer.Len() > 0 || c.controlServerErr != nil
}

// readRaw returns queued control bytes BEFORE any queued control error, so a
// partial frame is ACTUALLY consumed before the failure is surfaced. The
// deterministic stall is one-shot and honors the effective deadline.
func (c *targetLockCommitAmbiguityCore) readRaw(p []byte) (int, error) {
	if real := c.realConn(); real != nil {
		return real.Read(p)
	}
	c.controlMu.Lock()
	if c.controlStallAfter >= 0 && c.controlConsumed >= c.controlStallAfter {
		c.controlStallAfter = -1
		c.controlStallOngoing = true
		select {
		case c.controlStallCh <- struct{}{}:
		default:
		}
		c.controlMu.Unlock()
		stallErr := c.stallWait()
		c.controlMu.Lock()
		c.controlStallOngoing = false
		c.controlMu.Unlock()
		if stallErr != nil {
			return 0, stallErr
		}
		return c.readRaw(p)
	}
	if c.controlServer.Len() > 0 {
		n, err := c.controlServer.Read(p)
		c.controlConsumed += n
		c.controlMu.Unlock()
		return n, err
	}
	if c.controlServerErr != nil {
		err := c.controlServerErr
		c.controlServerErr = nil
		c.controlMu.Unlock()
		return 0, err
	}
	c.controlMu.Unlock()
	return 0, io.EOF
}

func (c *targetLockCommitAmbiguityCore) breakTransport() {
	c.breakOnce.Do(func() {
		c.mu.Lock()
		c.broken = true
		real := c.real
		c.mu.Unlock()
		if real != nil {
			_ = real.Close()
		}
		close(c.brokenCh)
	})
}

func (c *targetLockCommitAmbiguityCore) markReadFailure() {
	c.mu.Lock()
	c.readFailure = true
	c.mu.Unlock()
}

func (c *targetLockCommitAmbiguityCore) markUnrecognized() {
	c.mu.Lock()
	c.unrecognized++
	c.mu.Unlock()
}

func (c *targetLockCommitAmbiguityCore) setReadDeadline(t time.Time) error {
	real := c.realConn()
	c.deadlineMu.Lock()
	c.callerDeadline = t
	c.applyEffectiveDeadlineLocked(real)
	c.deadlineMu.Unlock()
	c.wakeDeadlineWaiters()
	return nil
}

func (c *targetLockCommitAmbiguityCore) setDeadline(t time.Time) error {
	if err := c.setReadDeadline(t); err != nil {
		return err
	}
	if real := c.realConn(); real != nil {
		return real.SetWriteDeadline(t)
	}
	return nil
}

func (c *targetLockCommitAmbiguityCore) setWriteDeadline(t time.Time) error {
	if real := c.realConn(); real != nil {
		return real.SetWriteDeadline(t)
	}
	return nil
}

// targetLockCommitAmbiguityAddr is the nil-safe control-mode address.
type targetLockCommitAmbiguityAddr string

func (a targetLockCommitAmbiguityAddr) Network() string { return "fault-control" }
func (a targetLockCommitAmbiguityAddr) String() string  { return string(a) }

func (c *targetLockCommitAmbiguityCore) localAddr() net.Addr {
	if real := c.realConn(); real != nil {
		return real.LocalAddr()
	}
	return targetLockCommitAmbiguityAddr("fault-control-local")
}

func (c *targetLockCommitAmbiguityCore) remoteAddr() net.Addr {
	if real := c.realConn(); real != nil {
		return real.RemoteAddr()
	}
	return targetLockCommitAmbiguityAddr("fault-control-remote")
}

// ---------------------------------------------------------------------------
// Real-socket serialized deadline probe
// ---------------------------------------------------------------------------

// TargetLockCommitAmbiguityDeadlineProbe is the fixed opaque probe of the
// serialized socket-deadline path over a REAL net.Pipe socket pair. It shares
// the exact adapter/core deadline mechanism (wrap + serialized minimum apply)
// and exposes only fixed probe operations; it is never installed into the
// production seam.
type TargetLockCommitAmbiguityDeadlineProbe struct {
	core    *targetLockCommitAmbiguityCore
	adapter net.Conn
	peer    net.Conn

	readMu   sync.Mutex
	readDone chan struct{}
	readErr  error
}

// NewTargetLockCommitAmbiguityDeadlineProbe builds the probe over a real
// net.Pipe socket pair.
func NewTargetLockCommitAmbiguityDeadlineProbe() *TargetLockCommitAmbiguityDeadlineProbe {
	client, server := net.Pipe()
	core := newTargetLockCommitAmbiguityCore()
	adapter := core.wrap(client)
	return &TargetLockCommitAmbiguityDeadlineProbe{core: core, adapter: adapter, peer: server}
}

// ArmGate arms the probe gate.
func (p *TargetLockCommitAmbiguityDeadlineProbe) ArmGate() {
	_ = p.core.armCommand(targetLockCommandCommit)
}

// Break closes the real socket pair and breaks the transport.
func (p *TargetLockCommitAmbiguityDeadlineProbe) Break() {
	p.core.breakTransport()
	_ = p.peer.Close()
}

// Witness returns only the ordering actually established.
func (p *TargetLockCommitAmbiguityDeadlineProbe) Witness() string {
	p.core.mu.Lock()
	defer p.core.mu.Unlock()
	if p.core.withheld {
		return TargetLockCommitWitnessWithheld
	}
	if p.core.dispatched {
		return TargetLockCommitWitnessDispatched
	}
	return TargetLockCommitWitnessNotEstablished
}

// WriteClient drives the real adapter write path.
func (p *TargetLockCommitAmbiguityDeadlineProbe) WriteClient(b []byte) error {
	_, err := p.adapter.Write(b)
	return err
}

// ProbeSetAssemblyCap installs the active assembly cap.
func (p *TargetLockCommitAmbiguityDeadlineProbe) ProbeSetAssemblyCap(t time.Time) {
	p.core.deadlineMu.Lock()
	p.core.assemblyCap = t
	p.core.deadlineMu.Unlock()
	p.core.applyDeadlineToSocket()
}

// ProbeSetCallerDeadline updates the caller deadline through the serialized
// setter.
func (p *TargetLockCommitAmbiguityDeadlineProbe) ProbeSetCallerDeadline(t time.Time) {
	_ = p.core.setReadDeadline(t)
}

// ProbeClearCallerDeadline clears the caller deadline through the serialized
// setter.
func (p *TargetLockCommitAmbiguityDeadlineProbe) ProbeClearCallerDeadline() {
	_ = p.core.setReadDeadline(time.Time{})
}

// ProbeApplyEffectiveDeadline runs one serialized minimum apply.
func (p *TargetLockCommitAmbiguityDeadlineProbe) ProbeApplyEffectiveDeadline() {
	p.core.applyDeadlineToSocket()
}

// ProbeStartBlockedRead starts one blocked adapter read over the real socket.
func (p *TargetLockCommitAmbiguityDeadlineProbe) ProbeStartBlockedRead() {
	p.readMu.Lock()
	p.readDone = make(chan struct{})
	p.readErr = nil
	done := p.readDone
	p.readMu.Unlock()
	go func() {
		_, err := p.adapter.Read(make([]byte, 64))
		p.readMu.Lock()
		p.readErr = err
		p.readMu.Unlock()
		close(done)
	}()
}

// ProbeAwaitRead waits boundedly for the started read and returns its error.
func (p *TargetLockCommitAmbiguityDeadlineProbe) ProbeAwaitRead(bound time.Duration) error {
	p.readMu.Lock()
	done := p.readDone
	p.readMu.Unlock()
	if done == nil {
		return errors.New("target-lock commit fault: no probe read was started")
	}
	select {
	case <-done:
		p.readMu.Lock()
		defer p.readMu.Unlock()
		return p.readErr
	case <-time.After(bound):
		return errors.New("target-lock commit fault: the probe read did not return within the bound")
	}
}

// classifyTargetLockCommandFrame recognizes the actual simple-protocol command
// query frame (commit or rollback) and never claims an establishment for
// unrecognized, partial or non-command framing.
func classifyTargetLockCommandFrame(p []byte, expected string) (match, recognized bool) {
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
			sql := string(bytes.TrimRight(body, "\x00"))
			sql = string(trimASCIIWhitespace(sql))
			if equalFoldASCII(sql, expected) {
				return true, true
			}
		}
		p = p[1+length:]
	}
	return false, true
}

// targetLockCommandCompletionAssembled STRICTLY validates the REAL command
// completion: a CommandComplete whose body is exactly the expected tag (for
// example "COMMIT\0" or "ROLLBACK\0") followed by a ReadyForQuery of exactly
// length 5 carrying the idle status. Anything else (missing NUL, wrong tag,
// missing/short/long/unknown status frame, unknown frame, partial framing) is
// not a completion.
func targetLockCommandCompletionAssembled(buf []byte, expectedTag string) bool {
	commitComplete := false
	for len(buf) >= 5 {
		messageType := buf[0]
		length := int(binary.BigEndian.Uint32(buf[1:5]))
		if length < 4 || 1+length > len(buf) {
			return false
		}
		body := buf[5 : 1+length]
		switch messageType {
		case 'C':
			if string(body) != expectedTag {
				return false
			}
			commitComplete = true
		case 'Z':
			if !commitComplete || length != 5 || len(body) != 1 || body[0] != 'I' {
				return false
			}
			return true
		case 'N', 'S', 'A':
			// asynchronous/notice traffic is skipped
		default:
			return false
		}
		buf = buf[1+length:]
	}
	return false
}

func trimASCIIWhitespace(s string) []byte {
	start := 0
	for start < len(s) && (s[start] == ' ' || s[start] == '\t' || s[start] == '\n' || s[start] == '\r') {
		start++
	}
	end := len(s)
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t' || s[end-1] == '\n' || s[end-1] == '\r') {
		end--
	}
	return []byte(s[start:end])
}

func equalFoldASCII(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		ca, cb := a[i], b[i]
		if 'A' <= ca && ca <= 'Z' {
			ca += 'a' - 'A'
		}
		if 'A' <= cb && cb <= 'Z' {
			cb += 'a' - 'A'
		}
		if ca != cb {
			return false
		}
	}
	return true
}
