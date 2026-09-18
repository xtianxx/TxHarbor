//go:build integration

// logscan_commitdrop_readside_integration_test.go pins the Lane-I read-side
// completion-drop injector at the protocol boundary, with no PostgreSQL and no
// containers: the wrapper runs over net.Pipe against a scripted in-memory
// backend, so every frame boundary (fragmented reads, merged frames, short
// reads, ErrorResponse, EOF, bounded deadline) is driven explicitly.
//
// The guarantees under test:
//  1. a successful COMMIT reply is never delivered to the client before the
//     full completion pair (CommandComplete tag COMMIT + ReadyForQuery idle)
//     has been observed on the wire — achievement is recognized only on that
//     complete evidence, never on the request write alone;
//  2. when server-side completion cannot be confirmed (ErrorResponse, EOF,
//     bounded deadline), the real bytes/error reach the client and the
//     injector reports itself NOT achieved — it never fakes the loss;
//  3. unarmed connections stay byte-transparent.
//
// net.Pipe is synchronous (a Write blocks until the peer reads), so the
// backend script runs on goroutines sequenced by the wrapper's own
// consumption; the 150ms probes assert the withholding, not the wire.
//
// This complements (does not replace) TestT028CrashDrillAndRefork, whose
// original assertions stay untouched and which proves the injector end to end
// against a real backend.
package indexer

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

// t28Frame encodes one PostgreSQL wire frame: type byte + 4-byte big-endian
// declared length (including itself) + payload.
func t28Frame(typ byte, payload string) []byte {
	out := make([]byte, 0, 1+4+len(payload))
	out = append(out, typ)
	var l [4]byte
	binary.BigEndian.PutUint32(l[:], uint32(4+len(payload)))
	out = append(out, l[:]...)
	return append(out, payload...)
}

// t28CompletionReply is the exact success reply to a COMMIT: CommandComplete
// with tag COMMIT, then ReadyForQuery(idle 'I').
var t28CompletionReply = func() []byte {
	out := t28Frame('C', "COMMIT\x00")
	return append(out, t28Frame('Z', "I")...)
}()

// t28ErrorReply is a backend rejection of the COMMIT: an ErrorResponse frame
// (minimal empty-fields shape) then ReadyForQuery(idle 'I' — the transaction
// is over after a failed COMMIT).
var t28ErrorReply = func() []byte {
	out := t28Frame('E', "\x00")
	return append(out, t28Frame('Z', "I")...)
}()

// t28Pair wires a client-side completion-drop conn with a scripted backend
// over net.Pipe. The backend end is returned raw for the test to script; the
// client end is the instrumented wrapper.
func t28Pair(t *testing.T, bound time.Duration, drop *logscanCompletionDrop) (net.Conn, net.Conn) {
	t.Helper()
	client, backend := net.Pipe()
	if bound > 0 {
		drop.bound = bound
	} else if drop.bound == 0 {
		drop.bound = 30 * time.Second
	}
	wrapped := &logscanCompletionDropConn{Conn: client, d: drop, id: int(drop.nextConn.Add(1)), bound: drop.bound}
	t.Cleanup(func() { _ = wrapped.Close() })
	t.Cleanup(func() { _ = backend.Close() })
	return wrapped, backend
}

// t28ForwardCommit writes the COMMIT request through the wrapper while a
// goroutine consumes it from the backend (net.Pipe needs concurrent
// directions), asserting the full byte-exact forward.
func t28ForwardCommit(t *testing.T, client, backend net.Conn, drop *logscanCompletionDrop) {
	t.Helper()
	req := t28Frame('Q', "commit\x00")
	type fwd struct {
		b   []byte
		err error
	}
	ch := make(chan fwd, 1)
	go func() {
		seen := make([]byte, 0, len(req))
		buf := make([]byte, len(req))
		for len(seen) < len(req) {
			nn, rerr := backend.Read(buf[:len(req)-len(seen)])
			if rerr != nil {
				ch <- fwd{seen, rerr}
				return
			}
			seen = append(seen, buf[:nn]...)
		}
		ch <- fwd{seen, nil}
	}()
	n, err := client.Write(req)
	if err != nil || n != len(req) {
		t.Fatalf("forwarding the COMMIT request = (%d, %v), want full forward", n, err)
	}
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("backend read after commit forward: %v", r.err)
		}
		if !bytes.Equal(r.b, req) {
			t.Fatalf("backend received %q, want the forwarded request verbatim", r.b)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("backend never consumed the forwarded COMMIT request")
	}
	if drop.dropped.Load() != 1 {
		t.Fatalf("dropped = %d, want 1 (write-side trigger counted on the commit forward)", drop.dropped.Load())
	}
}

// t28WriteAll writes a full slice to the backend, coping with net.Pipe's
// synchronous semantics via a loop (the wrapper's reader consumes it).
func t28WriteAll(t *testing.T, backend net.Conn, b []byte) {
	t.Helper()
	for len(b) > 0 {
		n, err := backend.Write(b)
		if err != nil {
			t.Fatalf("backend write: %v", err)
		}
		b = b[n:]
	}
}

// t28ReadResult is the client-side read outcome of one interception window.
type t28ReadResult struct {
	b   []byte
	err error
}

// t28ClientReader parks reading the client side and reports the first data
// delivery or error, exactly what pgx would have observed.
func t28ClientReader(client net.Conn, result chan<- t28ReadResult) {
	out := make([]byte, 0, 512)
	buf := make([]byte, 64) // small buffer: forces short reads against the wrapper
	for {
		n, rerr := client.Read(buf)
		out = append(out, buf[:n]...)
		if rerr != nil || len(out) > 0 {
			result <- t28ReadResult{out, rerr}
			return
		}
	}
}

// t28ExpectWithheld asserts the client reader is still parked (nothing
// delivered) while more backend bytes are pending, using a probe timeout.
func t28ExpectWithheld(t *testing.T, result chan t28ReadResult, where string) {
	t.Helper()
	select {
	case r := <-result:
		t.Fatalf("%s: client observed %q (err %v) while the window should still withhold", where, r.b, r.err)
	case <-time.After(150 * time.Millisecond):
	}
}

func TestLogScanCompletionDropProtocolBoundary(t *testing.T) {
	t.Run("success reply withheld until the full completion pair", func(t *testing.T) {
		drop := &logscanCompletionDrop{}
		client, backend := t28Pair(t, 0, drop)
		drop.arm.Store(true)
		t28ForwardCommit(t, client, backend, drop)

		result := make(chan t28ReadResult, 1)
		go t28ClientReader(client, result)

		// 1. A wrong-tag CommandComplete (any other statement's tag) must not
		// resolve the window: still withheld.
		t28WriteAll(t, backend, t28Frame('C', "BEGIN\x00"))
		t28ExpectWithheld(t, result, "wrong-tag CommandComplete")

		// 2. The real CommandComplete(COMMIT) frame arrives SPLIT mid-frame
		// (fragmented wire): still withheld until the frame completes...
		ccFrame := t28Frame('C', "COMMIT\x00")
		t28WriteAll(t, backend, ccFrame[:6])
		t28ExpectWithheld(t, result, "mid-frame fragment")
		t28WriteAll(t, backend, ccFrame[6:])
		// ...and even with the COMMIT tag complete, the window holds until
		// ReadyForQuery(idle) is also observed.
		t28ExpectWithheld(t, result, "CommandComplete(COMMIT) without ReadyForQuery")

		// 3. ReadyForQuery(idle) completes the pair: the client resolves with
		// a connection error and NEVER receives the withheld success bytes.
		t28WriteAll(t, backend, t28Frame('Z', "I"))
		select {
		case r := <-result:
			if len(r.b) != 0 {
				t.Fatalf("client received %d withheld byte(s) %q, want none (success never leaks)", len(r.b), r.b)
			}
			if r.err == nil {
				t.Fatal("client read after confirmed completion = nil, want the connection error")
			}
		case <-time.After(2 * time.Second):
			t.Fatal("client read never resolved after the completion pair was written")
		}
		if drop.achieved.Load() != 1 {
			t.Fatalf("achieved = %d, want 1 (recognized only on the complete pair)", drop.achieved.Load())
		}
		// The wrapper closed the client side: the backend's next write fails.
		if _, err := backend.Write([]byte("x")); !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("backend write after wrapper close = %v, want io.ErrClosedPipe", err)
		}
	})

	t.Run("ErrorResponse resolves as not-achieved and delivers the real bytes", func(t *testing.T) {
		drop := &logscanCompletionDrop{}
		client, backend := t28Pair(t, 0, drop)
		drop.arm.Store(true)
		t28ForwardCommit(t, client, backend, drop)

		result := make(chan t28ReadResult, 1)
		go t28ClientReader(client, result)
		// The backend rejects the COMMIT: real bytes must reach the client
		// and the injector must NOT claim achievement.
		t28WriteAll(t, backend, t28ErrorReply)
		var r t28ReadResult
		select {
		case r = <-result:
		case <-time.After(2 * time.Second):
			t.Fatal("client read never resolved after the ErrorResponse")
		}
		if r.err != nil {
			t.Fatalf("client read after ErrorResponse = %v, want the real bytes delivered", r.err)
		}
		if !bytes.Contains(r.b, t28Frame('Z', "I")) {
			t.Fatalf("client received %q, want the real ErrorResponse+ReadyForQuery bytes", r.b)
		}
		if drop.achieved.Load() != 0 {
			t.Fatalf("achieved = %d, want 0 (server-side completion NOT confirmed; must not fake the loss)", drop.achieved.Load())
		}
		// The window ended in passthrough: a following frame flows unmodified
		// (the conn is still usable after a failed COMMIT).
		done := make(chan error, 1)
		go func() {
			if _, err := backend.Write(t28Frame('Z', "I")); err != nil {
				done <- err
			}
			close(done)
		}()
		more := make(chan t28ReadResult, 1)
		go t28ClientReader(client, more)
		select {
		case m := <-more:
			if m.err != nil {
				t.Fatalf("post-window passthrough read = %v", m.err)
			}
			if !bytes.Equal(m.b, t28Frame('Z', "I")) {
				t.Fatalf("post-window read = %q, want verbatim passthrough", m.b)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("post-window passthrough never delivered")
		}
		if err := <-done; err != nil {
			t.Fatalf("backend write after window: %v", err)
		}
	})

	t.Run("EOF before the completion pair is not faked as loss", func(t *testing.T) {
		drop := &logscanCompletionDrop{}
		client, backend := t28Pair(t, 0, drop)
		drop.arm.Store(true)
		t28ForwardCommit(t, client, backend, drop)

		result := make(chan t28ReadResult, 1)
		go t28ClientReader(client, result)

		ccFrame := t28Frame('C', "COMMIT\x00")
		t28WriteAll(t, backend, ccFrame[:6]) // partial frame only
		_ = backend.Close()                  // backend dies before completing the pair

		select {
		case r := <-result:
			if r.err == nil {
				t.Fatalf("client read = (%q, nil), want the real EOF surfaced", r.b)
			}
			if !errors.Is(r.err, io.EOF) && !errors.Is(r.err, io.ErrClosedPipe) {
				t.Fatalf("client error = %v, want the real EOF family", r.err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("client read never resolved after the backend EOF")
		}
		if drop.achieved.Load() != 0 {
			t.Fatalf("achieved = %d, want 0 (completion unconfirmed)", drop.achieved.Load())
		}
	})

	t.Run("bounded wait without completion resolves as not-achieved", func(t *testing.T) {
		drop := &logscanCompletionDrop{}
		client, backend := t28Pair(t, 250*time.Millisecond, drop)
		drop.arm.Store(true)
		t28ForwardCommit(t, client, backend, drop)

		result := make(chan t28ReadResult, 1)
		go t28ClientReader(client, result)

		start := time.Now()
		var r t28ReadResult
		select {
		case r = <-result:
		case <-time.After(3 * time.Second):
			t.Fatal("bounded withholding never resolved; want the deadline error surfaced to the client")
		}
		if elapsed := time.Since(start); elapsed < 200*time.Millisecond {
			t.Fatalf("bounded wait resolved after %v, want the 250ms bound honored", elapsed)
		}
		if r.err == nil {
			t.Fatalf("client observed %q, want the deadline error (no completion evidence)", r.b)
		}
		if drop.achieved.Load() != 0 {
			t.Fatalf("achieved = %d, want 0 (deadline without completion evidence)", drop.achieved.Load())
		}
	})

	t.Run("unarmed connections stay byte-transparent", func(t *testing.T) {
		drop := &logscanCompletionDrop{}
		client, backend := t28Pair(t, 0, drop)
		// arm NOT stored: even a commit-shaped request must not intercept.

		req := t28Frame('Q', "commit\x00")
		reply := make(chan error, 1)
		// net.Pipe writes block until read: the backend consumes the request
		// and then writes the reply from its own goroutine.
		go func() {
			buf := make([]byte, len(req))
			for seen := 0; seen < len(req); {
				n, err := backend.Read(buf[seen:])
				if err != nil {
					reply <- err
					return
				}
				seen += n
			}
			_, err := backend.Write(t28CompletionReply)
			reply <- err
		}()
		if _, err := client.Write(req); err != nil {
			t.Fatalf("write: %v", err)
		}
		result := make(chan t28ReadResult, 1)
		go t28ClientReader(client, result)
		var r t28ReadResult
		select {
		case r = <-result:
		case <-time.After(2 * time.Second):
			t.Fatal("unarmed passthrough read never delivered")
		}
		if r.err != nil {
			t.Fatalf("unarmed passthrough read = %v", r.err)
		}
		if !bytes.Equal(r.b, t28CompletionReply) {
			t.Fatalf("client received %q, want the reply verbatim (byte-transparent)", r.b)
		}
		if err := <-reply; err != nil {
			t.Fatalf("backend write: %v", err)
		}
		if drop.dropped.Load() != 0 || drop.achieved.Load() != 0 {
			t.Fatalf("drop counters moved without an armed commit: dropped=%d achieved=%d", drop.dropped.Load(), drop.achieved.Load())
		}
	})
}
