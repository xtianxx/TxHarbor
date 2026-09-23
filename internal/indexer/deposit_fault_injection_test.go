package indexer

// deposit_fault_injection_test.go owns the pure-logic half of the TCP-level
// deposit fault injector: the trigger markers, the verdicts, the depositFault
// classifier and the frontend startup-packet guard. The connection plumbing
// (depositFaultConn, depositOpenFaultPool) needs a real PostgreSQL session and
// stays in deposit_integration_test.go. Keeping the classifier here means the
// plain, Docker-free unit run covers the connect-handshake regression, where an
// integration skip could mask it.

import (
	"bytes"
	"encoding/binary"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Fault-injection triggers, matched against the SQL text pgx writes to the
// connection. The markers name the exact protocol points:
const (
	// depositFaultAfterReads is the first statement of the commit transaction:
	// once it is written every unit query has completed, so a connection death
	// here is the deterministic "process exits after the queries, before any
	// write" state.
	depositFaultAfterReads = "SET LOCAL statement_timeout"
	// depositFaultMidTransaction is the progress UPDATE of an advancing unit,
	// written after the observations: failing there must roll them all back
	// (no partial commit). The first unit's INSERT checkpoint path is covered
	// by depositFaultAfterReads.
	depositFaultMidTransaction = "UPDATE deposit_checkpoint"
	// depositFaultCommit is the COMMIT statement itself (the reply-lost and
	// never-reached cases).
	depositFaultCommit = "commit"
	// depositFaultPauseAudit is the pause audit INSERT shared by the manual
	// release (insertReleasePauseAuditSQL) and the authorization
	// (insertAuthPauseAuditSQL) paths: failing it must roll the whole pause
	// transaction back. One marker covers both statements because both insert
	// into deposit_pause_audit.
	depositFaultPauseAudit = "INSERT INTO deposit_pause_audit"
)

// Deposit-fault verdicts returned by depositFault.match.
const (
	depositFaultNone = iota
	depositFaultDropReply
	depositFaultFailWrite
)

// depositFault injects deterministic faults into every connection of a fault
// pool, through a real dial wrapper (never by simulating return values). A SQL
// trigger matches a client write containing marker: dropReply forwards the
// statement and closes the connection before the reply can be read (the
// statement lands, the worker observes a connection error: the process-exit /
// uncertain-commit state); failWrite closes before forwarding (the statement
// never lands: a mid-transaction connection failure). sticky makes every
// matching write fire so a worker that must stay dead cannot slip a commit
// through while the test stops it. failDials fails every dial while set
// (transient database unavailability). fired closes on the first trigger so
// tests synchronize on the fault itself, never on sleeps.
type depositFault struct {
	marker    string
	dropReply bool
	sticky    bool
	armed     atomic.Bool
	fires     atomic.Int32
	dialFails atomic.Bool
	dialTries atomic.Int32
	fired     chan struct{}
	once      sync.Once
}

func newDepositFault() *depositFault { return &depositFault{fired: make(chan struct{})} }

// armSQL arms a SQL-text trigger.
func (f *depositFault) armSQL(marker string, dropReply, sticky bool) {
	f.marker = marker
	f.dropReply = dropReply
	f.sticky = sticky
	f.armed.Store(true)
}

func (f *depositFault) disarm() { f.armed.Store(false) }

// match classifies one client write and records the trigger. Connection
// startup packets are never classified: they carry the connection parameters
// (user, database, password), not SQL (see isDepositFaultStartupPacket).
func (f *depositFault) match(b []byte) int {
	if !f.armed.Load() || f.marker == "" || isDepositFaultStartupPacket(b) {
		return depositFaultNone
	}
	if !bytes.Contains(b, []byte(f.marker)) {
		return depositFaultNone
	}
	if !f.sticky && !f.armed.CompareAndSwap(true, false) {
		return depositFaultNone
	}
	f.fires.Add(1)
	f.once.Do(func() { close(f.fired) })
	if f.dropReply {
		return depositFaultDropReply
	}
	return depositFaultFailWrite
}

// waitFired blocks until the trigger fires at least once.
func (f *depositFault) waitFired(t *testing.T, what string) {
	t.Helper()
	select {
	case <-f.fired:
	case <-time.After(15 * time.Second):
		t.Fatalf("timed out waiting for the injected fault: %s", what)
	}
}

// Frontend startup-family codes occupy the protocol-version slot (bytes 4..8)
// of the first write on a fresh connection: StartupMessage (3.0/3.2),
// CancelRequest, SSLRequest and GSSENCRequest. None of them is SQL.
const (
	depositFaultStartupV30    = 196608   // pgproto3.ProtocolVersion30
	depositFaultStartupV32    = 196610   // pgproto3.ProtocolVersion32 (196609 is not a protocol version)
	depositFaultCancelRequest = 80877102 // pgproto3 cancelRequestNumber
	depositFaultSSLRequest    = 80877103 // pgproto3 sslRequestNumber
	depositFaultGSSENCRequest = 80877104 // pgproto3 gssEncRequestNumber
)

// isDepositFaultStartupPacket reports whether b is a connection startup packet
// rather than a query write. Startup packets carry the connection parameters,
// and the shared-container harness derives the database name from the test
// name (idx_t_<test>_<seq>): a short marker such as depositFaultCommit
// ("commit", as in Test...UnknownCommit...) occurs verbatim inside those bytes.
// Matching them would fire the fault during the connect handshake — killing the
// connection before any SQL is sent ("failed to receive message: use of closed
// network connection") — instead of at the intended statement. Frontend query
// messages start with an ASCII type byte ('Q', 'P', ...), so a zero leading
// length byte plus a startup code identifies the packet unambiguously.
func isDepositFaultStartupPacket(b []byte) bool {
	if len(b) < 8 || b[0] != 0 {
		return false
	}
	switch binary.BigEndian.Uint32(b[4:8]) {
	case depositFaultStartupV30, depositFaultStartupV32,
		depositFaultCancelRequest, depositFaultSSLRequest, depositFaultGSSENCRequest:
		return true
	}
	return false
}

// buildStartup assembles a frontend startup-family packet with the wire shape
// pgproto3 writes: an int32 length, the 4-byte code in the protocol-version
// slot (bytes 4..8) and NUL-terminated parameters ("user", "txharbor",
// "database", "txharbor", final NUL). Cancel and encryption requests carry no
// parameters.
func buildStartup(code uint32, params ...string) []byte {
	body := binary.BigEndian.AppendUint32(nil, code)
	for _, p := range params {
		body = append(body, p...)
		body = append(body, 0)
	}
	body = append(body, 0)
	out := binary.BigEndian.AppendUint32(nil, uint32(4+len(body)))
	return append(out, body...)
}

// buildQuery assembles the wire shape of a frontend query message: the ASCII
// type byte 'Q', the int32 length (including itself), the SQL text and the
// terminating NUL. pgx writes the commit statement as "commit" (tx.go).
func buildQuery(sql string) []byte {
	out := []byte{'Q'}
	out = binary.BigEndian.AppendUint32(out, uint32(5+len(sql)))
	out = append(out, sql...)
	return append(out, 0)
}

// TestIsDepositFaultStartupPacket pins the startup-packet classification that
// keeps the SQL markers from firing during the connect handshake. The shared
// container names every database after its test (idx_t_<test>_<seq>), so a
// test name containing "commit" (Test...UnknownCommit...) put the marker
// verbatim into the handshake bytes; misclassifying those bytes as SQL killed
// the connection before any statement was sent.
func TestIsDepositFaultStartupPacket(t *testing.T) {
	// Regression pin: pgproto3.ProtocolVersion32 is 196610. The old 196609 is
	// not a protocol version, so V32 handshakes were not recognized as startup
	// packets and the marker matched inside them.
	if depositFaultStartupV32 != 196610 {
		t.Fatalf("depositFaultStartupV32 = %d, want 196610 (pgproto3.ProtocolVersion32)", depositFaultStartupV32)
	}

	tests := []struct {
		name string
		b    []byte
		want bool
	}{
		{"v30_startup", buildStartup(depositFaultStartupV30, "user", "txharbor", "database", "txharbor"), true},
		{"v32_startup", buildStartup(depositFaultStartupV32, "user", "txharbor", "database", "txharbor"), true},
		{"v32_unknowncommit_database", buildStartup(depositFaultStartupV32,
			"user", "txharbor", "database", "idx_t_unknowncommit_1"), true},
		{"ssl_request", buildStartup(depositFaultSSLRequest), true},
		{"cancel_request", buildStartup(depositFaultCancelRequest), true},
		{"gssenc_request", buildStartup(depositFaultGSSENCRequest), true},
		{"ssl_request_exact_8_bytes", []byte{0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x2f}, true},
		{"query", buildQuery("select 1"), false},
		{"query_uppercase_commit", buildQuery("COMMIT"), false},
		{"bare_uppercase_commit", []byte("COMMIT"), false},
		{"parse", []byte("P\x00\x00\x00\x04"), false},
		{"bind", []byte("B\x00\x00\x00\x04"), false},
		{"describe", []byte("D\x00\x00\x00\x04"), false},
		{"execute", []byte("E\x00\x00\x00\x04"), false},
		{"truncated_below_8_bytes", []byte{0, 0, 0, 7, 0, 3, 0}, false},
		{"length_prefix_only", []byte{0, 0, 0, 8}, false},
		{"empty", nil, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isDepositFaultStartupPacket(tc.b); got != tc.want {
				t.Fatalf("isDepositFaultStartupPacket(% x) = %v, want %v", tc.b, got, tc.want)
			}
		})
	}
}

// TestDepositFaultMatchStartupIgnoredThenCommitFires pins the connect-handshake
// regression at the match level: a startup packet whose database name contains
// the trigger marker must neither fire nor consume the one-shot arm, so the
// fault still fires exactly once on the intended COMMIT write and is then
// exhausted.
func TestDepositFaultMatchStartupIgnoredThenCommitFires(t *testing.T) {
	f := newDepositFault()
	f.armSQL(depositFaultCommit, true, false)

	startup := buildStartup(depositFaultStartupV32,
		"user", "txharbor", "database", "idx_t_unknowncommit_1")
	if got := f.match(startup); got != depositFaultNone {
		t.Fatalf("match(startup V32 with a commit-bearing database name) = %d, want depositFaultNone", got)
	}
	if !f.armed.Load() {
		t.Fatal("the startup packet consumed the one-shot arm, want it still armed")
	}
	if got := f.fires.Load(); got != 0 {
		t.Fatalf("fires = %d after the startup packet, want 0", got)
	}

	commit := buildQuery("commit")
	if got := f.match(commit); got != depositFaultDropReply {
		t.Fatalf("match(COMMIT write) = %d, want depositFaultDropReply", got)
	}
	f.waitFired(t, "COMMIT write") // the fired gate must already be closed
	if f.armed.Load() {
		t.Fatal("the one-shot arm must be consumed by the COMMIT write")
	}
	if got := f.fires.Load(); got != 1 {
		t.Fatalf("fires = %d after the COMMIT write, want 1", got)
	}

	// Exhausted: a replayed COMMIT write must not fire a second time.
	if got := f.match(commit); got != depositFaultNone {
		t.Fatalf("match(replayed COMMIT write) = %d, want depositFaultNone", got)
	}
	if got := f.fires.Load(); got != 1 {
		t.Fatalf("fires = %d after the replayed COMMIT write, want 1", got)
	}
}

// TestDepositFaultMatchStickyFailWriteAndDisarm pins the sticky failWrite
// semantics: every matching write fires (a worker that must stay dead cannot
// slip a commit through), non-matching writes stay silent, and disarm stops
// the injector without touching the fire count.
func TestDepositFaultMatchStickyFailWriteAndDisarm(t *testing.T) {
	f := newDepositFault()
	f.armSQL(depositFaultCommit, false, true)

	commit := buildQuery("commit")
	for i := 1; i <= 3; i++ {
		if got := f.match(commit); got != depositFaultFailWrite {
			t.Fatalf("match(COMMIT write #%d) = %d, want depositFaultFailWrite", i, got)
		}
		if !f.armed.Load() {
			t.Fatalf("the sticky arm disarmed itself after fire #%d", i)
		}
	}
	if got := f.fires.Load(); got != 3 {
		t.Fatalf("fires = %d after three matching writes, want 3", got)
	}

	if got := f.match(buildQuery("select 1")); got != depositFaultNone {
		t.Fatalf("match(non-matching write) = %d, want depositFaultNone", got)
	}
	if got := f.fires.Load(); got != 3 {
		t.Fatalf("fires = %d after a non-matching write, want 3", got)
	}

	f.disarm()
	if f.armed.Load() {
		t.Fatal("disarm left the injector armed")
	}
	if got := f.match(commit); got != depositFaultNone {
		t.Fatalf("match(COMMIT write) after disarm = %d, want depositFaultNone", got)
	}
	if got := f.fires.Load(); got != 3 {
		t.Fatalf("fires = %d after disarm, want 3 (disarm must not fire)", got)
	}
}
