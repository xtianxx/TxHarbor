//go:build linux && drill

// targetlock_commitambiguity_hook_linux_test.go is the drill-only fixed
// companion of the owner COMMIT acknowledgement-loss lane. It reserves the
// unexported production socket-wrap seam with ONE fixed wire gate and exposes
// ONLY an opaque control handle with fixed fault operations (arm/withhold/
// break/witness/query). The live-socket adapter is UNEXPORTED and is the only
// net.Conn: the exported handle has NO Read/Write/Close capability, and the
// deterministic byte-feed/read controls REJECT live-socket instances. It never
// exposes a raw connection, a caller-supplied socket callback, a connector or
// SQL authority, and it never changes the DSN, key, owner, canonical identity
// checks or acquisition machinery. WITHHELD is published only after the ACTUAL
// PostgreSQL COMMIT completion has been bounded-assembled and strictly
// validated (CommandComplete body exactly "COMMIT\0" followed by a correctly
// sized ReadyForQuery with the idle status). EOF, read errors, deadline expiry,
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

// targetLockCommitAmbiguityCore is the unexported shared state of one fault.
type targetLockCommitAmbiguityCore struct {
	real net.Conn

	mu             sync.Mutex
	live           bool
	armed          bool
	dispatched     bool
	withheld       bool
	broken         bool
	readFailure    bool
	unrecognized   int
	wrapped        int
	withholdErr    error
	assembledBytes int

	dispatchedCh chan struct{}
	withheldCh   chan struct{}
	brokenCh     chan struct{}
	withholdOnce sync.Once
	breakOnce    sync.Once

	// deterministic in-memory transport of the control instance
	controlMu        sync.Mutex
	controlServer    bytes.Buffer
	controlServerErr error

	// deadline/cancellation awareness of the assembly and withheld waits
	deadlineMu      sync.Mutex
	readDeadline    time.Time
	deadlineChanged chan struct{}
}

func newTargetLockCommitAmbiguityCore() *targetLockCommitAmbiguityCore {
	return &targetLockCommitAmbiguityCore{
		dispatchedCh:    make(chan struct{}),
		withheldCh:      make(chan struct{}),
		brokenCh:        make(chan struct{}),
		deadlineChanged: make(chan struct{}, 1),
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
// capability. Deterministic byte-feed/read controls reject live-socket
// instances.
type TargetLockCommitAmbiguityFault struct {
	core *targetLockCommitAmbiguityCore
}

// ArmTargetLockCommitAmbiguityFault reserves the production socket-wrap seam
// with this fixed fault and returns the opaque handle, the identity-conditional
// reset and an error for an overlapping reservation. No socket callback is
// accepted.
func ArmTargetLockCommitAmbiguityFault() (*TargetLockCommitAmbiguityFault, func(), error) {
	core := newTargetLockCommitAmbiguityCore()
	reset, err := reserveTargetLockSocketWrap(core.wrap)
	if err != nil {
		return nil, nil, err
	}
	return &TargetLockCommitAmbiguityFault{core: core}, reset, nil
}

// NewTargetLockCommitAmbiguityControl builds the deterministic control
// instance (never installed into the production seam) whose fixed in-memory
// transport is driven through ControlWriteClientBytes / ControlFeedServerBytes
// / ControlFeedServerError / ControlRead.
func NewTargetLockCommitAmbiguityControl() *TargetLockCommitAmbiguityFault {
	return &TargetLockCommitAmbiguityFault{core: newTargetLockCommitAmbiguityCore()}
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
func (f *TargetLockCommitAmbiguityFault) ArmGate() {
	f.core.mu.Lock()
	f.core.armed = true
	f.core.mu.Unlock()
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
// REJECTS live-socket instances.
func (f *TargetLockCommitAmbiguityFault) ControlWriteClientBytes(b []byte) error {
	if f.core.isLive() {
		return errors.New("target-lock commit fault: control byte feed is unavailable on a live-socket fault")
	}
	f.core.maybeEstablishDispatch(b)
	return nil
}

// ControlFeedServerBytes queues server bytes for the deterministic transport;
// it REJECTS live-socket instances.
func (f *TargetLockCommitAmbiguityFault) ControlFeedServerBytes(b []byte) error {
	if f.core.isLive() {
		return errors.New("target-lock commit fault: control server feed is unavailable on a live-socket fault")
	}
	f.core.controlMu.Lock()
	f.core.controlServer.Write(b)
	f.core.controlMu.Unlock()
	return nil
}

// ControlFeedServerError queues one read failure for the deterministic
// transport; it REJECTS live-socket instances.
func (f *TargetLockCommitAmbiguityFault) ControlFeedServerError(err error) error {
	if f.core.isLive() {
		return errors.New("target-lock commit fault: control server error is unavailable on a live-socket fault")
	}
	f.core.controlMu.Lock()
	f.core.controlServerErr = err
	f.core.controlMu.Unlock()
	return nil
}

// ControlSetReadDeadline sets the tracked read deadline of the deterministic
// control instance; it REJECTS live-socket instances.
func (f *TargetLockCommitAmbiguityFault) ControlSetReadDeadline(t time.Time) error {
	if f.core.isLive() {
		return errors.New("target-lock commit fault: control deadline is unavailable on a live-socket fault")
	}
	f.core.noteDeadline(t)
	return nil
}

// ControlRead drives the fixed Read path once; it REJECTS live-socket
// instances.
func (f *TargetLockCommitAmbiguityFault) ControlRead(p []byte) (int, error) {
	if f.core.isLive() {
		return 0, errors.New("target-lock commit fault: control read is unavailable on a live-socket fault")
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
	commit, recognized := classifyTargetLockCommitFrame(p)
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.armed || c.dispatched {
		return
	}
	if !recognized {
		c.unrecognized++
		return
	}
	if commit {
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
// publishes WITHHELD. The effective read deadline is the earlier of the
// assembly limit and the caller's current tracked deadline.
func (c *targetLockCommitAmbiguityCore) withholdCommitCompletion() (err error) {
	const completionLimit = 1 << 16
	assemblyDeadline := time.Now().Add(30 * time.Second)
	assembled := make([]byte, 0, 256)
	scratch := make([]byte, 512)
	defer func() {
		c.mu.Lock()
		c.assembledBytes = len(assembled)
		c.mu.Unlock()
	}()
	for len(assembled) <= completionLimit {
		effective := c.effectiveReadDeadline(assemblyDeadline)
		remaining := time.Until(effective)
		if remaining <= 0 {
			c.markReadFailure()
			return os.ErrDeadlineExceeded
		}
		if real := c.realConn(); real != nil {
			// Never set a later deadline than the caller's current one.
			_ = real.SetReadDeadline(time.Now().Add(remaining))
		}
		n, readErr := c.readRaw(scratch)
		if n > 0 {
			assembled = append(assembled, scratch[:n]...)
		}
		if readErr != nil {
			c.markReadFailure()
			return readErr
		}
		if targetLockCommitCompletionAssembled(assembled) {
			return nil
		}
	}
	c.markReadFailure()
	c.markUnrecognized()
	return errors.New("target-lock COMMIT completion framing was not recognized")
}

// effectiveReadDeadline is the earlier of the assembly limit and the caller's
// current tracked deadline.
func (c *targetLockCommitAmbiguityCore) effectiveReadDeadline(assemblyDeadline time.Time) time.Time {
	c.deadlineMu.Lock()
	callerDeadline := c.readDeadline
	c.deadlineMu.Unlock()
	if !callerDeadline.IsZero() && callerDeadline.Before(assemblyDeadline) {
		return callerDeadline
	}
	return assemblyDeadline
}

// waitAfterWithhold blocks until the transport is broken or the tracked read
// deadline expires; deadline changes and Close are honored and a timeout fails
// closed without upgrading the witness.
func (c *targetLockCommitAmbiguityCore) waitAfterWithhold() error {
	for {
		c.deadlineMu.Lock()
		deadline := c.readDeadline
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
			current := c.readDeadline
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

// readRaw returns queued control bytes BEFORE any queued control error, so a
// partial frame is ACTUALLY consumed before the failure is surfaced.
func (c *targetLockCommitAmbiguityCore) readRaw(p []byte) (int, error) {
	if real := c.realConn(); real != nil {
		return real.Read(p)
	}
	c.controlMu.Lock()
	defer c.controlMu.Unlock()
	if c.controlServer.Len() > 0 {
		return c.controlServer.Read(p)
	}
	if c.controlServerErr != nil {
		err := c.controlServerErr
		c.controlServerErr = nil
		return 0, err
	}
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

func (c *targetLockCommitAmbiguityCore) noteDeadline(t time.Time) {
	c.deadlineMu.Lock()
	c.readDeadline = t
	c.deadlineMu.Unlock()
	select {
	case c.deadlineChanged <- struct{}{}:
	default:
	}
}

func (c *targetLockCommitAmbiguityCore) setReadDeadline(t time.Time) error {
	c.noteDeadline(t)
	if real := c.realConn(); real != nil {
		return real.SetReadDeadline(t)
	}
	return nil
}

func (c *targetLockCommitAmbiguityCore) setDeadline(t time.Time) error {
	c.noteDeadline(t)
	if real := c.realConn(); real != nil {
		return real.SetDeadline(t)
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

// classifyTargetLockCommitFrame recognizes the actual simple-protocol COMMIT
// query frame and never claims an establishment for unrecognized, partial or
// non-commit framing.
func classifyTargetLockCommitFrame(p []byte) (commit, recognized bool) {
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
			if equalFoldASCII(sql, "commit") {
				return true, true
			}
		}
		p = p[1+length:]
	}
	return false, true
}

// targetLockCommitCompletionAssembled STRICTLY validates the REAL COMMIT
// completion: a CommandComplete whose body is exactly "COMMIT\0" followed by a
// ReadyForQuery of exactly length 5 carrying the idle status. Anything else
// (missing NUL, wrong tag, missing/short/long/unknown status frame, unknown
// frame, partial framing) is not a completion.
func targetLockCommitCompletionAssembled(buf []byte) bool {
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
			if string(body) != "COMMIT\x00" {
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
