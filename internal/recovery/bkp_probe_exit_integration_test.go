//go:build integration

// Probe-exit synchronization discrimination tests for the F2 interrupted
// restore rebuild helper (TestRestoreInterruptionNotRestoredAndRerunIdempotent,
// run 37257158182 failure follow-up).
//
// These tests prove the bkpAwaitProbeExit helper's discrimination, not wiring:
// they never touch the production recovery paths. The positive case proves the
// helper returns once the probe's own backend is gone and the authoritative
// census then observes zero sessions. The negative cases prove the bounded
// wait cannot absorb a foreign session (unknown writer or old-attempt
// application_name tag): the census still refuses while the helper keeps
// waiting only on its own probe identity, and the timeout diagnostic names the
// remaining session's identity (pid/backend_start/application_name/state).
//
// The boundedness negatives inject real blocked admin states through a
// self-built net.Listen stub backend (bkpFakePG): the client is a real pgx pool
// speaking the real v3 wire protocol, only the server never answers. They prove
// the wait returns at its deadline when the observation query hangs, when the
// connection acquisition itself stalls, returns promptly with the cancellation
// error when the caller cancels mid-query, and keeps a blocked diagnostic dump
// inside its own short budget while the rebuild-lock proxy is released as soon
// as the wait returns.
package recovery

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// bkpCensusDiscriminationFixture opens one fixture (its own throwaway
// postgres container through the package's standard bkpFixture TestMain
// discipline) and a dedicated discrimination database inside it.
func bkpCensusDiscriminationFixture(t *testing.T) (f *bkpFixture, targetDB string, targetDSN string) {
	t.Helper()
	f = newBkpFixture(t)
	targetDSN = f.createDatabase(t, "probe_disc")
	targetDB = bkpDBNameOf(t, f.adminDSN, targetDSN)
	return f, targetDB, targetDSN
}

// Positive: after the probe's own backend exits, the bounded wait returns and
// the authoritative census observes exactly zero sessions.
func TestBkpProbeExitPositiveZeroCensus(t *testing.T) {
	f, targetDB, targetDSN := bkpCensusDiscriminationFixture(t)

	probe, err := pgx.Connect(context.Background(), targetDSN)
	if err != nil {
		t.Fatalf("connect probe: %v", err)
	}
	var probePID int32
	var probeBackendStart time.Time
	if err := probe.QueryRow(context.Background(),
		`SELECT pg_backend_pid(), backend_start FROM pg_stat_activity WHERE pid = pg_backend_pid()`).Scan(&probePID, &probeBackendStart); err != nil {
		_ = probe.Close(context.Background())
		t.Fatalf("capture probe identity: %v", err)
	}
	if _, err := probe.Exec(context.Background(), `SELECT 1`); err != nil {
		t.Fatalf("probe roundtrip: %v", err)
	}
	if err := probe.Close(context.Background()); err != nil {
		t.Fatalf("close probe: %v", err)
	}

	start := time.Now()
	if err := bkpAwaitProbeExit(f.ctx, f.admin, targetDB, probePID, probeBackendStart,
		bkpProbeExitTimeout, bkpProbeExitInterval); err != nil {
		t.Fatalf("probe exit wait failed: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed > 5*time.Second {
		t.Fatalf("probe exit took %s, want within the known ~30ms window", elapsed)
	}

	var postSessions int
	if err := f.admin.QueryRow(f.ctx,
		`SELECT count(*) FROM pg_stat_activity WHERE datname=$1`, targetDB).Scan(&postSessions); err != nil || postSessions != 0 {
		t.Fatalf("post-exit census = %d err=%v, want zero sessions\n%s", postSessions, err,
			bkpTargetSessionDump(f.ctx, f.admin, targetDB))
	}
}

// Negative 1: an unknown foreign session (empty application_name, the
// old-writer shape) present while the helper waits means the census must
// refuse - the bounded probe wait never absorbs or excludes it.
func TestBkpProbeExitForeignSessionRefusedByCensus(t *testing.T) {
	f, targetDB, targetDSN := bkpCensusDiscriminationFixture(t)

	probe, err := pgx.Connect(context.Background(), targetDSN)
	if err != nil {
		t.Fatalf("connect probe: %v", err)
	}
	var probePID int32
	var probeBackendStart time.Time
	if err := probe.QueryRow(context.Background(),
		`SELECT pg_backend_pid(), backend_start FROM pg_stat_activity WHERE pid = pg_backend_pid()`).Scan(&probePID, &probeBackendStart); err != nil {
		_ = probe.Close(context.Background())
		t.Fatalf("capture probe identity: %v", err)
	}
	if err := probe.Close(context.Background()); err != nil {
		t.Fatalf("close probe: %v", err)
	}
	if err := bkpAwaitProbeExit(f.ctx, f.admin, targetDB, probePID, probeBackendStart,
		bkpProbeExitTimeout, bkpProbeExitInterval); err != nil {
		t.Fatalf("probe exit wait failed: %v", err)
	}

	// Inject the foreign session AFTER the probe is gone: an idle session the
	// test does not own, shaped like an old writer (empty application_name).
	foreign, err := pgx.Connect(context.Background(), targetDSN)
	if err != nil {
		t.Fatalf("connect foreign session: %v", err)
	}
	defer func() {
		// The test closes only the session it created itself; the helper under
		// test must never terminate unknown sessions (asserted below).
		_ = foreign.Close(context.Background())
	}()
	if _, err := foreign.Exec(f.ctx, `BEGIN`); err != nil {
		t.Fatalf("foreign begin: %v", err)
	}
	var foreignPID int32
	if err := foreign.QueryRow(f.ctx,
		`SELECT pg_backend_pid() FROM pg_stat_activity WHERE pid = pg_backend_pid()`).Scan(&foreignPID); err != nil {
		t.Fatalf("capture foreign identity: %v", err)
	}

	var postSessions int
	if err := f.admin.QueryRow(f.ctx,
		`SELECT count(*) FROM pg_stat_activity WHERE datname=$1`, targetDB).Scan(&postSessions); err == nil && postSessions == 0 {
		t.Fatal("census observed zero sessions despite a foreign session present: the fix would be absorbing unknown writers")
	}
	dump := bkpTargetSessionDump(f.ctx, f.admin, targetDB)
	if !strings.Contains(dump, "application_name=\"\"") {
		t.Fatalf("session dump must identify the foreign session by identity, got:\n%s", dump)
	}
	if !strings.Contains(dump, "state=\"idle in transaction\"") {
		t.Fatalf("session dump must show the foreign session state, got:\n%s", dump)
	}
	if foreignPID == 0 || !strings.Contains(dump, "pid=") {
		t.Fatalf("session dump missing pid, got:\n%s", dump)
	}
}

// Negative 2: a session tagged with the interrupted attempt's
// application_name tag shape must still be refused by the census: the fix
// must not whitelist or blacklist by application_name.
func TestBkpProbeExitAttemptTaggedSessionStillRefused(t *testing.T) {
	f, targetDB, targetDSN := bkpCensusDiscriminationFixture(t)

	probe, err := pgx.Connect(context.Background(), targetDSN)
	if err != nil {
		t.Fatalf("connect probe: %v", err)
	}
	var probePID int32
	var probeBackendStart time.Time
	if err := probe.QueryRow(context.Background(),
		`SELECT pg_backend_pid(), backend_start FROM pg_stat_activity WHERE pid = pg_backend_pid()`).Scan(&probePID, &probeBackendStart); err != nil {
		_ = probe.Close(context.Background())
		t.Fatalf("capture probe identity: %v", err)
	}
	if err := probe.Close(context.Background()); err != nil {
		t.Fatalf("close probe: %v", err)
	}
	if err := bkpAwaitProbeExit(f.ctx, f.admin, targetDB, probePID, probeBackendStart,
		bkpProbeExitTimeout, bkpProbeExitInterval); err != nil {
		t.Fatalf("probe exit wait failed: %v", err)
	}

	// Inject a session carrying an attempt-tag-shaped application_name
	// (txh015_<hex> is the production AttemptAppName shape, targetlock.go).
	tagged, err := pgx.ConnectConfig(context.Background(), mustBkpProbeTaggedConfig(t, targetDSN, "txh015_deadbeef01"))
	if err != nil {
		t.Fatalf("connect tagged session: %v", err)
	}
	defer func() { _ = tagged.Close(context.Background()) }()
	if _, err := tagged.Exec(f.ctx, `SELECT 1`); err != nil {
		t.Fatalf("tagged roundtrip: %v", err)
	}

	var postSessions int
	if err := f.admin.QueryRow(f.ctx,
		`SELECT count(*) FROM pg_stat_activity WHERE datname=$1`, targetDB).Scan(&postSessions); err == nil && postSessions == 0 {
		t.Fatal("census observed zero sessions despite an attempt-tagged session present: name-based exclusion leaked in")
	}
	dump := bkpTargetSessionDump(f.ctx, f.admin, targetDB)
	if !strings.Contains(dump, "txh015_deadbeef01") {
		t.Fatalf("session dump must identify the tagged session, got:\n%s", dump)
	}
}

// Negative 3 (boundedness/fault injection): the bounded wait must fail with
// identity diagnostics when the probe's (pid, backend_start) never leaves.
// A live session's own real identity is handed to the helper with a
// deliberately short deadline; the error must name the tuple and enumerate
// remaining sessions, and must not exceed the deadline by more than
// scheduling slack.
func TestBkpProbeExitWaitIsBoundedAndDiagnoses(t *testing.T) {
	f, _, targetDSN := bkpCensusDiscriminationFixture(t)

	// Keep a live session open; the helper is called with that session's own
	// real (pid, backend_start) identity, which cannot exit while the
	// connection lives. This is the fault-injection stand-in for "probe did
	// not leave".
	ghost, err := pgx.Connect(context.Background(), targetDSN)
	if err != nil {
		t.Fatalf("connect ghost: %v", err)
	}
	defer func() { _ = ghost.Close(context.Background()) }()
	var ghostPID int32
	var ghostBackendStart time.Time
	if err := ghost.QueryRow(f.ctx,
		`SELECT pg_backend_pid(), backend_start FROM pg_stat_activity WHERE pid = pg_backend_pid()`).Scan(&ghostPID, &ghostBackendStart); err != nil {
		t.Fatalf("capture ghost identity: %v", err)
	}

	start := time.Now()
	waitErr := bkpAwaitProbeExit(f.ctx, f.admin, f.bkpDBNameOfDisc(targetDSN), ghostPID, ghostBackendStart,
		400*time.Millisecond, 25*time.Millisecond)
	elapsed := time.Since(start)
	if waitErr == nil {
		t.Fatal("bounded wait returned nil for a never-exiting backend")
	}
	if elapsed > 2*time.Second {
		t.Fatalf("bounded wait exceeded its deadline by %s", elapsed)
	}
	if !strings.Contains(waitErr.Error(), "pid=") || !strings.Contains(waitErr.Error(), "backend_start=") {
		t.Fatalf("timeout error must name the probe tuple, got: %v", waitErr)
	}
	if !strings.Contains(waitErr.Error(), "remaining session pid=") {
		t.Fatalf("timeout error must dump remaining sessions, got: %v", waitErr)
	}
}

// ---------------------------------------------------------------------------
// Fault injection: minimal fake PostgreSQL frontend
// ---------------------------------------------------------------------------

const (
	bkpFakePGProtocolV3    = 196608
	bkpFakePGCancelRequest = 80877102
	bkpFakePGSSLRequest    = 80877103
	bkpFakePGGSSENCRequest = 80877104
)

// bkpFakePG is a self-built net.Listen PostgreSQL-shaped endpoint used only to
// inject the blocked admin states the bounded wait must survive: an observation
// query whose reply never arrives, a query cancelled in flight by the caller's
// context, and a stalled connection acquisition. The client is a real pgx pool
// speaking the real v3 wire protocol; only the backend is a stub: it completes
// the startup handshake (AuthenticationOk + ParameterStatus + BackendKeyData +
// ReadyForQuery) and then consumes every client message without ever answering,
// so the client genuinely blocks inside QueryRow/Scan the way the review found.
// In stalled mode it never completes the handshake at all, so the client blocks
// inside connect/Acquire. The injection is deterministic: firstPacket is closed
// when a client packet reaches the wire and queryHit when the helper's query
// does, and nothing here guesses timing. A Terminate packet closes the
// connection like a real server would, so pgx's bounded asyncClose drain does
// not idle out.
type bkpFakePG struct {
	ln          net.Listener
	dsn         string
	silent      bool
	firstPacket chan struct{}
	queryHit    chan struct{}

	packetOnce sync.Once
	hitOnce    sync.Once
	mu         sync.Mutex
	closed     bool
	conns      []net.Conn
	wg         sync.WaitGroup
}

func bkpStartFakePG(t *testing.T) *bkpFakePG {
	t.Helper()
	return bkpStartFakePGMode(t, false)
}

// bkpStartFakePGStalled accepts TCP but never completes the v3 startup, so the
// client blocks inside the connection acquisition (dial + handshake).
func bkpStartFakePGStalled(t *testing.T) *bkpFakePG {
	t.Helper()
	return bkpStartFakePGMode(t, true)
}

func bkpStartFakePGMode(t *testing.T, silent bool) *bkpFakePG {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen fake postgres endpoint: %v", err)
	}
	s := &bkpFakePG{
		ln:          ln,
		dsn:         fmt.Sprintf("postgres://txharbor:txharbor@%s/txharbor?sslmode=disable", ln.Addr().String()),
		silent:      silent,
		firstPacket: make(chan struct{}),
		queryHit:    make(chan struct{}),
	}
	t.Cleanup(s.close)
	s.wg.Add(1)
	go s.acceptLoop()
	return s
}

func (s *bkpFakePG) close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	_ = s.ln.Close()
	for _, conn := range s.conns {
		_ = conn.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
}

func (s *bkpFakePG) acceptLoop() {
	defer s.wg.Done()
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			_ = conn.Close()
			return
		}
		s.conns = append(s.conns, conn)
		s.mu.Unlock()
		s.wg.Add(1)
		go s.serve(conn)
	}
}

func (s *bkpFakePG) serve(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()
	for first := true; ; {
		code, ok := bkpFakePGReadStartup(conn)
		if !ok {
			return
		}
		if first {
			first = false
			s.packetOnce.Do(func() { close(s.firstPacket) })
		}
		switch code {
		case bkpFakePGSSLRequest, bkpFakePGGSSENCRequest:
			// The pool connects with sslmode=disable; refuse politely anyway.
			if _, err := conn.Write([]byte{'N'}); err != nil {
				return
			}
			continue
		}
		if s.silent {
			// Stall the connection attempt: the startup packet is consumed and
			// nothing is ever answered, so the client blocks inside connect.
			s.consume(conn)
			return
		}
		if code == bkpFakePGCancelRequest {
			return
		}
		if code != bkpFakePGProtocolV3 {
			return
		}
		if !s.writeHandshake(conn) {
			return
		}
		s.consume(conn)
		return
	}
}

// bkpFakePGReadStartup reads one length-prefixed startup packet and returns its
// protocol code (the SSL/GSS/cancel request codes included).
func bkpFakePGReadStartup(conn net.Conn) (uint32, bool) {
	var head [4]byte
	if _, err := io.ReadFull(conn, head[:]); err != nil {
		return 0, false
	}
	length := binary.BigEndian.Uint32(head[:])
	if length < 8 || length > 1<<16 {
		return 0, false
	}
	body := make([]byte, length-4)
	if _, err := io.ReadFull(conn, body); err != nil {
		return 0, false
	}
	return binary.BigEndian.Uint32(body[:4]), true
}

// writeHandshake completes the v3 startup: AuthenticationOk, the parameter
// statuses a pgx client reads, BackendKeyData and ReadyForQuery.
func (s *bkpFakePG) writeHandshake(conn net.Conn) bool {
	var buf bytes.Buffer
	bkpFakePGAppend(&buf, 'R', []byte{0, 0, 0, 0})
	for _, kv := range [][2]string{
		{"server_version", "15.0"},
		{"server_encoding", "UTF8"},
		{"client_encoding", "UTF8"},
		{"DateStyle", "ISO, MDY"},
		{"integer_datetimes", "on"},
		{"standard_conforming_strings", "on"},
		{"TimeZone", "UTC"},
	} {
		payload := append([]byte(kv[0]), 0)
		payload = append(payload, kv[1]...)
		payload = append(payload, 0)
		bkpFakePGAppend(&buf, 'S', payload)
	}
	key := make([]byte, 8)
	binary.BigEndian.PutUint32(key[0:4], 4242)
	binary.BigEndian.PutUint32(key[4:8], 7)
	bkpFakePGAppend(&buf, 'K', key)
	bkpFakePGAppend(&buf, 'Z', []byte{'I'})
	_, err := conn.Write(buf.Bytes())
	return err == nil
}

// consume reads and discards every post-handshake message, never answering. The
// first message is the helper's observation query reaching the wire. A Terminate
// closes the connection like a real server would (pgx's asyncClose drains until
// the server closes, bounded at 15s otherwise).
func (s *bkpFakePG) consume(conn net.Conn) {
	first := true
	for {
		var head [5]byte
		if _, err := io.ReadFull(conn, head[:]); err != nil {
			return
		}
		length := binary.BigEndian.Uint32(head[1:])
		if length < 4 || length > 1<<24 {
			return
		}
		body := make([]byte, length-4)
		if _, err := io.ReadFull(conn, body); err != nil {
			return
		}
		if head[0] == 'X' {
			return
		}
		if first {
			first = false
			s.hitOnce.Do(func() { close(s.queryHit) })
		}
	}
}

func bkpFakePGAppend(buf *bytes.Buffer, typ byte, payload []byte) {
	buf.WriteByte(typ)
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(4+len(payload)))
	buf.Write(length[:])
	buf.Write(payload)
}

// bkpFakePGPool opens a pool against the fake endpoint. It is deliberately not
// db.OpenPool: that opener pings, and the ping would never return here. MinConns
// stays 0 so no background goroutine dials the stalled endpoint.
func bkpFakePGPool(t *testing.T, s *bkpFakePG) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(s.dsn)
	if err != nil {
		t.Fatalf("parse fake postgres dsn: %v", err)
	}
	cfg.MaxConns = 1
	cfg.MinConns = 0
	cfg.HealthCheckPeriod = time.Hour
	cfg.MaxConnLifetime = time.Hour
	cfg.MaxConnIdleTime = time.Hour
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("create fake postgres pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// Negative 4 (boundedness under a hung admin query): the helper's observation
// query genuinely blocks on the wire - a real pgx pool against the stub backend
// that never answers - and the wait must still return at its bounded window with
// the probe tuple as the primary error instead of hanging forever.
func TestBkpProbeExitBoundedWhenAdminQueryHangs(t *testing.T) {
	srv := bkpStartFakePG(t)
	pool := bkpFakePGPool(t, srv)

	const timeout = 250 * time.Millisecond
	budget := timeout + bkpTargetSessionDumpTimeout + 2*time.Second // guard slack, not a timing guess

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	start := time.Now()
	done := make(chan error, 1)
	go func() {
		done <- bkpAwaitProbeExit(ctx, pool, "bkp_hung_query", 4242, time.Now().Add(-time.Minute), timeout, 25*time.Millisecond)
	}()

	select {
	case <-srv.queryHit:
	case <-time.After(5 * time.Second):
		t.Fatal("the helper's observation query never reached the injected endpoint: the hang was not injected inside QueryRow/Scan")
	}

	var waitErr error
	select {
	case waitErr = <-done:
	case <-time.After(budget):
		t.Fatal("bounded wait did not return while the injected admin query stayed hung")
	}
	elapsed := time.Since(start)
	if waitErr == nil {
		t.Fatal("bounded wait returned nil while the injected admin query stayed hung")
	}
	if elapsed < timeout {
		t.Fatalf("bounded wait returned after %s, before its %s window: %v", elapsed, timeout, waitErr)
	}
	if elapsed > budget {
		t.Fatalf("bounded wait took %s, above its %s budget (timeout + bounded diagnostic): %v", elapsed, budget, waitErr)
	}
	if !strings.Contains(waitErr.Error(), "still visible") || !strings.Contains(waitErr.Error(), "pid=4242") ||
		!strings.Contains(waitErr.Error(), "backend_start=") {
		t.Fatalf("hung-query expiry must keep the probe tuple as the primary error, got: %v", waitErr)
	}
	if !strings.Contains(waitErr.Error(), "session dump unavailable") && !strings.Contains(waitErr.Error(), "session dump incomplete") {
		t.Fatalf("hung-query expiry must note the bounded diagnostic failure as an additional line, got: %v", waitErr)
	}
}

// Negative 5 (boundedness of the connection acquisition): an endpoint that
// accepts TCP but never completes the v3 startup must not pin the wait either -
// pgxpool's Acquire (dial + handshake) runs on the same bounded context, so the
// helper still returns at its deadline with the timeout diagnostics.
func TestBkpProbeExitBoundedWhenConnectionAcquireStalls(t *testing.T) {
	srv := bkpStartFakePGStalled(t)
	pool := bkpFakePGPool(t, srv)

	const timeout = 250 * time.Millisecond
	budget := timeout + bkpTargetSessionDumpTimeout + 2*time.Second // guard slack, not a timing guess

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	start := time.Now()
	done := make(chan error, 1)
	go func() {
		done <- bkpAwaitProbeExit(ctx, pool, "bkp_stalled_acquire", 4242, time.Now().Add(-time.Minute), timeout, 25*time.Millisecond)
	}()

	select {
	case <-srv.firstPacket:
	case <-time.After(5 * time.Second):
		t.Fatal("the stalled endpoint never saw a connection attempt: the acquire was not exercised")
	}

	var waitErr error
	select {
	case waitErr = <-done:
	case <-time.After(budget):
		t.Fatal("bounded wait did not return while the connection acquisition stayed stalled")
	}
	elapsed := time.Since(start)
	if waitErr == nil {
		t.Fatal("bounded wait returned nil while the connection acquisition stayed stalled")
	}
	if elapsed < timeout {
		t.Fatalf("bounded wait returned after %s, before its %s window: %v", elapsed, timeout, waitErr)
	}
	if elapsed > budget {
		t.Fatalf("bounded wait took %s, above its %s budget (timeout + bounded diagnostic): %v", elapsed, budget, waitErr)
	}
	if !strings.Contains(waitErr.Error(), "still visible") || !strings.Contains(waitErr.Error(), "pid=4242") {
		t.Fatalf("stalled-acquire expiry must keep the probe tuple as the primary error, got: %v", waitErr)
	}
	if !strings.Contains(waitErr.Error(), "session dump unavailable") && !strings.Contains(waitErr.Error(), "session dump incomplete") {
		t.Fatalf("stalled-acquire expiry must note the bounded diagnostic failure as an additional line, got: %v", waitErr)
	}
}

// Negative 6 (caller cancellation): a query cancelled in flight must abort the
// wait promptly with the cancellation error - not with the timeout diagnostics,
// and not after the (much longer) bounded window.
func TestBkpProbeExitCallerCancelUnblocksHungQuery(t *testing.T) {
	srv := bkpStartFakePG(t)
	pool := bkpFakePGPool(t, srv)

	const timeout = 30 * time.Second
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	start := time.Now()
	done := make(chan error, 1)
	go func() {
		done <- bkpAwaitProbeExit(ctx, pool, "bkp_cancel_query", 4242, time.Now().Add(-time.Minute), timeout, 25*time.Millisecond)
	}()

	select {
	case <-srv.queryHit:
	case <-time.After(5 * time.Second):
		t.Fatal("the helper's observation query never reached the injected endpoint: the query was not in flight")
	}
	cancel()

	var waitErr error
	select {
	case waitErr = <-done:
	case <-time.After(3 * time.Second):
		t.Fatalf("wait did not return within 3s of caller cancellation (window was %s): the in-flight query was not cancelled", timeout)
	}
	if waitErr == nil {
		t.Fatal("wait returned nil after caller cancellation")
	}
	if !errors.Is(waitErr, context.Canceled) || !strings.Contains(waitErr.Error(), "canceled") {
		t.Fatalf("caller cancellation must surface as a cancellation error, got: %v", waitErr)
	}
	if strings.Contains(waitErr.Error(), "still visible") {
		t.Fatalf("caller cancellation must not be reported as the bounded timeout, got: %v", waitErr)
	}
	if elapsed := time.Since(start); elapsed >= timeout {
		t.Fatalf("wait took %s, at/above the %s window: cancellation did not unblock the in-flight query", elapsed, timeout)
	}
}

// Negative 7 (bounded diagnostic + rebuild-lock release): when the timeout
// branch fires, a blocked diagnostic dump must neither replace the primary
// timeout error nor extend the wait beyond its own budget, and the analogous
// rebuild lock must be free again as soon as the wait returns. The production
// wait runs inside the rebuild lock's acceptance transaction
// (rebuildInterruptedRestoreTarget -> lock.WithTransaction); the proxy below
// holds a transaction-scoped advisory lock on the real control store across the
// wait and proves an independent session can take it immediately afterwards.
func TestBkpProbeExitTimeoutBoundedDiagnosticReleasesLock(t *testing.T) {
	srv := bkpStartFakePG(t)
	pool := bkpFakePGPool(t, srv)
	f, _, _ := bkpCensusDiscriminationFixture(t)

	const timeout = 300 * time.Millisecond
	budget := timeout + bkpTargetSessionDumpTimeout + 2*time.Second // guard slack, not a timing guess

	const lockProxyKey = int64(0x015B0DE) // test-private key, no production key space
	holder, err := f.ctrl.Acquire(f.ctx)
	if err != nil {
		t.Fatalf("acquire control-store connection for the rebuild-lock proxy: %v", err)
	}
	defer holder.Release()
	if _, err := holder.Exec(f.ctx, `BEGIN`); err != nil {
		t.Fatalf("begin rebuild-lock proxy transaction: %v", err)
	}
	if _, err := holder.Exec(f.ctx, `SELECT pg_advisory_xact_lock($1)`, lockProxyKey); err != nil {
		t.Fatalf("hold rebuild-lock proxy: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type probeExitOutcome struct {
		err     error
		elapsed time.Duration
	}
	outcomeCh := make(chan probeExitOutcome, 1)
	go func() {
		start := time.Now()
		waitErr := bkpAwaitProbeExit(ctx, pool, "bkp_diag_hang", 4242, time.Now().Add(-time.Minute), timeout, 25*time.Millisecond)
		elapsed := time.Since(start)
		// The wait returned: end the transaction that models the rebuild-lock
		// hold, exactly as the production transaction end releases its lock.
		rollbackCtx, cancelRollback := context.WithTimeout(context.Background(), 5*time.Second)
		_, rollbackErr := holder.Exec(rollbackCtx, `ROLLBACK`)
		cancelRollback()
		outcomeCh <- probeExitOutcome{err: errors.Join(waitErr, rollbackErr), elapsed: elapsed}
	}()

	select {
	case <-srv.queryHit:
	case <-time.After(5 * time.Second):
		t.Fatal("the helper's observation query never reached the injected endpoint: the hang was not injected inside QueryRow/Scan")
	}

	var outcome probeExitOutcome
	select {
	case outcome = <-outcomeCh:
	case <-time.After(budget):
		t.Fatal("bounded wait never returned while the diagnostic endpoint stayed hung: the rebuild-lock proxy was never released")
	}
	if outcome.err == nil {
		t.Fatal("bounded wait returned nil while the injected admin query stayed hung")
	}
	if outcome.elapsed < timeout {
		t.Fatalf("bounded wait returned after %s, before its %s window: %v", outcome.elapsed, timeout, outcome.err)
	}
	if outcome.elapsed > budget {
		t.Fatalf("bounded wait took %s, above its %s budget (timeout + bounded diagnostic): %v", outcome.elapsed, budget, outcome.err)
	}
	if !strings.Contains(outcome.err.Error(), "still visible") || !strings.Contains(outcome.err.Error(), "pid=4242") {
		t.Fatalf("a blocked diagnostic must not mask the primary timeout error, got: %v", outcome.err)
	}
	if !strings.Contains(outcome.err.Error(), "session dump unavailable") && !strings.Contains(outcome.err.Error(), "session dump incomplete") {
		t.Fatalf("blocked diagnostic must be noted as an additional line, got: %v", outcome.err)
	}

	// The rebuild-lock proxy must be immediately available to an independent
	// session once the bounded wait returned (pg_try_advisory_lock never blocks,
	// so this is deterministic rather than a timing guess).
	other, err := f.ctrl.Acquire(f.ctx)
	if err != nil {
		t.Fatalf("acquire independent control-store session: %v", err)
	}
	defer other.Release()
	var acquired bool
	if err := other.QueryRow(f.ctx, `SELECT pg_try_advisory_lock($1)`, lockProxyKey).Scan(&acquired); err != nil {
		t.Fatalf("probe rebuild-lock proxy availability: %v", err)
	}
	if !acquired {
		t.Fatal("rebuild-lock proxy was still held after the bounded wait returned")
	}
	if _, err := other.Exec(f.ctx, `SELECT pg_advisory_unlock($1)`, lockProxyKey); err != nil {
		t.Fatalf("release probed rebuild-lock proxy: %v", err)
	}
}

// bkpDBNameOfDisc exposes the package helper's database-name derivation for
// this file without re-parsing DSNs by hand.
func (f *bkpFixture) bkpDBNameOfDisc(dsn string) string {
	return bkpDBNameOf(f.t, f.adminDSN, dsn)
}

func mustBkpProbeTaggedConfig(t *testing.T, dsn, appName string) (cfg *pgx.ConnConfig) {
	t.Helper()
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse tagged dsn: %v", err)
	}
	cfg.RuntimeParams["application_name"] = appName
	return cfg
}
