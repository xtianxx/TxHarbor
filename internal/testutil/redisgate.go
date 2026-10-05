// redisgate.go is test support only (same charter as redis.go: production
// packages never import it, `make test` never starts it). It is a userspace
// TCP gate in front of a real (or absent) Redis backend with three scripted
// modes, so a fake outage can be driven per-fault-shape without killing a
// container:
//
//   - GatePass: bidirectional byte forwarding — the healthy path (shape ④).
//   - GateHold: accept, read the client bytes, then hold them without ever
//     replying — "connection established, handshake done, command has no
//     response" (shape ③). A mode flip CONVERSIONS established Pass pairs
//     into Hold pairs: their backend side is closed (so the real Redis can
//     no longer answer through them) and the gateway side goes into the
//     same read-swallow loop. A held EVAL is DISCARDED, never replayed.
//   - GateDown: stop listening — dial meets ECONNREFUSED (shape ①). All
//     tracked pairs are closed (dials carried an established connection
//     at read time now see a reset).
//
// A userspace listener CANNOT express shape ② (SYN blackhole): on Linux a
// listening socket completes the handshake into the accept queue even when
// nothing ever Accept()s, so the client's connect succeeds. Shape ② is
// therefore driven from the caller side by a blackhole Dialer (see the
// experiment test), not by this gate. This header note exists so nobody
// mistakes a Hold listener for a dial-blocker.
//
// The backend address is fixed for the gate's lifetime (a fixed loopback
// port just like testutil.StartRedis), so a client built against the gate
// address survives mode flips — the recovery comparison keeps one address
// across shapes ①③④.
package testutil

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// --- evidence ---------------------------------------------------------------

// evidenceDirEnv overrides the evidence directory. Evidence never defaults
// into the repository working tree (same discipline as faultdrill's
// EvidenceWriter, which lives behind the fault tag and cannot be imported
// across layers).
const evidenceDirEnv = "TXHARBOR_FAULT_EVIDENCE_DIR"

// EvidenceWriter persists experiment evidence: timeline.jsonl (one JSON
// object per recorded event), named JSON exports and free-form files. It is
// safe for concurrent use.
type EvidenceWriter struct {
	dir  string
	mu   sync.Mutex
	file *os.File
}

// NewEvidenceWriter opens the evidence directory (the environment override
// or a fresh temp directory).
func NewEvidenceWriter() (*EvidenceWriter, error) {
	dir := strings.TrimSpace(os.Getenv(evidenceDirEnv))
	if dir == "" {
		var err error
		dir, err = os.MkdirTemp("", "txharbor-test-evidence-*")
		if err != nil {
			return nil, fmt.Errorf("evidence temp dir: %w", err)
		}
	} else if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("evidence dir: %w", err)
	}
	file, err := os.OpenFile(filepath.Join(dir, "timeline.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open timeline: %w", err)
	}
	return &EvidenceWriter{dir: dir, file: file}, nil
}

// Dir returns the evidence directory.
func (w *EvidenceWriter) Dir() string { return w.dir }

// WriteJSON writes one named JSON export into the evidence directory.
func (w *EvidenceWriter) WriteJSON(name string, data any) error {
	if w == nil {
		return nil
	}
	body, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal %s: %w", name, err)
	}
	return os.WriteFile(filepath.Join(w.dir, name), append(body, '\n'), 0o644)
}

// Close flushes and closes the timeline.
func (w *EvidenceWriter) Close() error {
	if w == nil || w.file == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	err := w.file.Close()
	w.file = nil
	return err
}

// --- gate -------------------------------------------------------------------

// GateMode is one scripted gate posture.
type GateMode int

const (
	// GatePass forwards both directions (healthy).
	GatePass GateMode = iota
	// GateHold accepts and swallows client bytes without replying (shape ③).
	GateHold
	// GateDown closes the listener: dials meet ECONNREFUSED (shape ①).
	GateDown
)

// RedisGate is one scripted TCP gate over a Redis (or absent) backend.
type RedisGate struct {
	backend string

	mu sync.Mutex
	// listener is the live accept loop; nil while Down or mid-reopen.
	listener net.Listener
	// lastAddr remembers the address across a Down flip so HostPort stays
	// one address for the gate's lifetime.
	lastAddr string
	mode     GateMode
	pairs    map[*connPair]struct{}
	closed   bool
}

// connPair is one proxied connection: the client-facing socket plus (in
// Pass mode) the backend socket and the shutdown channel. A pair's copy
// goroutines stop on pair.abort; mode flips convert or close the pair.
type connPair struct {
	client net.Conn
	// mu guards backend/holding mutations against the teardown path (the
	// -race finding: toHold and teardown raced on backend).
	mu      sync.Mutex
	backend net.Conn
	holding bool
	// abort is closed when ANY flip-or-Close invalidates the pair's current
	// behavior entirely (Down, Close); PASS→HOLD conversion keeps the pair
	// alive and does NOT close abort — it flips `holding` and closes the
	// backend copy loops, then the pair's reader goes into the swallow loop.
	abort chan struct{}
	// once guards the idempotent teardown of one pair.
	once sync.Once
}

// closeBackend closes the backend side only (Hold conversion), under mu.
func (p *connPair) closeBackend() {
	p.mu.Lock()
	b := p.backend
	p.backend = nil
	p.holding = true
	p.mu.Unlock()
	if b != nil {
		_ = b.Close()
	}
}

// teardown closes both sockets and signals the goroutines once (idempotent,
// race-free under mu).
func (p *connPair) teardown() {
	p.once.Do(func() {
		p.mu.Lock()
		b, c := p.backend, p.client
		p.backend, p.client = nil, nil
		p.holding = true
		p.mu.Unlock()
		close(p.abort)
		if b != nil {
			_ = b.Close()
		}
		if c != nil {
			_ = c.Close()
		}
	})
}

// toHold converts a live Pass pair into a Hold pair: backend side closed
// (so no more real-Redis bytes can arrive and the copy loops exit), client
// side stays open; the pair's reader then swallows via holding=true.
func (p *connPair) toHold() {
	p.mu.Lock()
	if p.holding {
		p.mu.Unlock()
		return // already holding (or torn down)
	}
	p.mu.Unlock()
	p.closeBackend()
}

// NewRedisGate takes a fixed free loopback port for the gate and points the
// forwarding backend at backendHostPort ("host:port" of a real Redis, e.g.
// `testutil.Redis.HostPort()`).
func NewRedisGate(backendHostPort string) (*RedisGate, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("redisgate reserve loopback: %w", err)
	}
	g := &RedisGate{
		backend:  backendHostPort,
		listener: ln,
		lastAddr: ln.Addr().String(),
		mode:     GatePass,
		pairs:    map[*connPair]struct{}{},
	}
	go g.serve(ln)
	return g, nil
}

// HostPort returns the gate's "host:port" address. A Down gate returns the
// LAST live address so callers can keep one address across a flip; a never-
// opened gate is a fixture bug (the callers always open in Pass).
func (g *RedisGate) HostPort() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.lastAddr == "" {
		panic("redisgate: HostPort before any listener was open (fixture bug)")
	}
	return g.lastAddr
}

// Close terminates the gate for good: closes the listener and every tracked
// pair (idempotent).
func (g *RedisGate) Close() error {
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return nil
	}
	g.closed = true
	ln := g.listener
	g.listener = nil
	pairs := g.pairs
	g.pairs = map[*connPair]struct{}{}
	g.mu.Unlock()
	if ln != nil {
		_ = ln.Close()
	}
	for pair := range pairs {
		pair.teardown()
	}
	return nil
}

// Mode reports the current posture.
func (g *RedisGate) Mode() GateMode {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.mode
}

// SetMode flips the posture and CONVERTS tracked pairs:
//
//   - →Hold: Pass pairs lose their backend side (the real Redis can no
//     longer answer through them) and go into the read-swallow loop; held
//     bytes are discarded, never replayed. The listener is (re)opened on
//     the same address so new dials land in Hold.
//   - →Down: the listener closes and every tracked pair is closed in both
//     directions (established sockets see a reset; new dials see
//     ECONNREFUSED).
//   - →Pass: every tracked pair is closed (clients redial) and the
//     listener is (re)opened on the same address.
//
// The reopen retry loop absorbs the kernel's release window on the bound
// address after the previous listener's Close.
func (g *RedisGate) SetMode(mode GateMode) error {
	g.mu.Lock()
	addr := g.lastAddr
	g.mode = mode
	if mode == GateDown && g.listener != nil {
		ln := g.listener
		g.listener = nil
		g.mu.Unlock()
		_ = ln.Close()
		g.convertPairs(nil)
		return nil
	}
	g.mu.Unlock()

	if mode == GateDown {
		g.convertPairs(nil)
		return nil
	}

	// Pass/Hold: rebind the listener on the remembered address.
	g.mu.Lock()
	ln := g.listener
	g.mu.Unlock()
	if ln == nil {
		newLn, err := bindLoopbackRetry(addr, 50, 20*time.Millisecond)
		if err != nil {
			return fmt.Errorf("redisgate reopen %s: %w", addr, err)
		}
		g.mu.Lock()
		g.listener = newLn
		g.lastAddr = newLn.Addr().String()
		g.mu.Unlock()
		go g.serve(newLn)
	}

	// Convert the tracked pairs to the new posture.
	g.mu.Lock()
	target := g.mode
	g.mu.Unlock()
	switch target {
	case GateHold:
		g.convertHold()
	case GatePass:
		g.convertPairs(nil)
	}
	return nil
}

// bindLoopbackRetry binds the address with a bounded retry loop: the kernel
// can keep the old listen socket's address pinned for a short window after
// Close while its accepted sockets drain.
func bindLoopbackRetry(addr string, attempts int, backoff time.Duration) (net.Listener, error) {
	var lastErr error
	for range attempts {
		ln, err := net.Listen("tcp", addr)
		if err == nil {
			return ln, nil
		}
		lastErr = err
		time.Sleep(backoff)
	}
	return nil, lastErr
}

// convertPairs tears down every tracked pair (both directions closed).
// Run with pairs list snapshotted; runs the pair teardowns concurrently.
func (g *RedisGate) convertPairs(mustMode *GateMode) {
	g.mu.Lock()
	pairs := g.pairs
	g.pairs = map[*connPair]struct{}{}
	g.mu.Unlock()
	for pair := range pairs {
		pair.teardown()
	}
}

// convertHold turns every tracked Pass pair into a Hold pair: close the
// backend side (no more real-Redis bytes can arrive) and let the pair's
// client-side reader go into the swallow loop. Hold pairs stay held.
func (g *RedisGate) convertHold() {
	g.mu.Lock()
	pairs := make([]*connPair, 0, len(g.pairs))
	for pair := range g.pairs {
		pairs = append(pairs, pair)
	}
	g.mu.Unlock()
	for _, pair := range pairs {
		pair.toHold()
	}
}

// serve accepts on one listener until it is closed; each accepted conn
// becomes one pair.
func (g *RedisGate) serve(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return // the listener was closed/flipped under us
		}
		pair := &connPair{client: conn, abort: make(chan struct{})}
		g.mu.Lock()
		if g.closed {
			g.mu.Unlock()
			_ = conn.Close()
			return
		}
		// Serve the connection under the CURRENT mode snapshot.
		mode := g.mode
		g.pairs[pair] = struct{}{}
		g.mu.Unlock()
		go g.runPair(pair, mode)
	}
}

// runPair serves ONE connection according to the mode snapshot taken at
// accept time, and re-checks the pair's conversion flags as it goes.
func (g *RedisGate) runPair(pair *connPair, mode GateMode) {
	defer func() {
		g.mu.Lock()
		delete(g.pairs, pair)
		g.mu.Unlock()
		pair.teardown()
	}()

	switch mode {
	case GateHold:
		g.runHold(pair)
	case GatePass:
		g.runPass(pair)
	default:
		return
	}
}

// runHold reads and discards client bytes forever (shape ③). The client
// socket is snapshotted once at entry: teardown may nil it concurrently
// (the -race finding); the local copy either already-owns the socket or
// teardown closed it, and both orders exit safely.
func (g *RedisGate) runHold(pair *connPair) {
	pair.mu.Lock()
	client := pair.client
	pair.mu.Unlock()
	if client == nil {
		return
	}
	buf := make([]byte, 4096)
	for {
		select {
		case <-pair.abort:
			return
		default:
		}
		if _, err := client.Read(buf); err != nil {
			return
		}
		// Never answer: bytes are dropped.
	}
}

// runPass forwards both directions (shape ④). On a Hold conversion the
// backend socket is closed by toHold/closeBackend, which unblocks both copy
// loops AND the conversion signal below; when the copies drain, this
// goroutine (not a new one) takes over the CLIENT side into the swallow
// loop, so an established warm conn genuinely stops responding and hangs
// (the reviewer's P2: previously only the abort channel woke this goroutine,
// so the swallow handoff was unreachable).
func (g *RedisGate) runPass(pair *connPair) {
	backend, err := net.DialTimeout("tcp", g.backend, 3*time.Second)
	if err != nil {
		return // drop the client side through teardown
	}
	pair.mu.Lock()
	if pair.holding {
		pair.mu.Unlock()
		_ = backend.Close()
		g.runHold(pair) // conversion happened before the backend was up
		return
	}
	pair.backend = backend
	pair.mu.Unlock()

	done := make(chan struct{})
	var wg sync.WaitGroup
	pair.mu.Lock()
	cli := pair.client
	pair.mu.Unlock()
	if cli == nil {
		return
	}
	wg.Add(2)
	go func() { defer wg.Done(); _, _ = io.Copy(backend, cli) }()
	go func() { defer wg.Done(); _, _ = io.Copy(cli, backend) }()

	// Signal both local exits: teardown (abort) and conversion (backend
	// closed by closeBackend). The conversion path does not abort the pair —
	// the client socket must survive into the swallow loop.
	go func() {
		defer close(done)
		select {
		case <-pair.abort:
		case <-conversionWait(pair):
		}
	}()
	<-done
	wg.Wait()

	pair.mu.Lock()
	holding := pair.holding
	pair.mu.Unlock()
	if holding {
		select {
		case <-pair.abort:
			return
		default:
		}
		g.runHold(pair)
	}
}

// conversionWait returns a one-shot channel that closes when the pair's
// backend side is torn down by a Hold conversion (backend==nil while the
// pair is NOT aborted). Detects the backend==nil flip cheaply via a poll;
// the conversion path sets backend=nil under mu in closeBackend.
func conversionWait(pair *connPair) <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		defer close(ch)
		for {
			select {
			case <-pair.abort:
				return
			default:
			}
			pair.mu.Lock()
			holding := pair.holding
			pair.mu.Unlock()
			if holding {
				return
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()
	return ch
}
