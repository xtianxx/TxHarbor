//go:build integration

// Batch C4: the POST-COMMIT unknown-outcome window of the 007 receipt path
// (T-accept, intake.go:372-378 commit-error branch; contracts/api.md §2
// uncertain-submit row; data-model.md T-accept "commit-error → re-classify").
//
// Defect guarded: tx.Commit returning an error does NOT prove the receipt
// transaction did not commit. If the path treated a lost COMMIT
// acknowledgement as "not created", as a key collision, or as a reason to mint
// a new request id, a durably committed row would be orphaned or a caller
// retry would create a second withdrawal for one authorization.
//
// Invariant: when the server durably committed but the caller cannot observe
// the acknowledgement, the caller gets the retryable 503 shape (same key, never
// a new key, never "Accepted") and the same-key retry converges to 200 with the
// SAME request_id — one keyed request row, one in-tx `created` audit row, and
// the grant rows untouched.
//
// Gap: the existing 40P01 fault test (failure_integration_test.go) aborts the
// tx BEFORE commit (trigger on the INSERT), so its retry yields the FIRST 201
// on zero prior rows. The post-commit-unknown window — durable row, caller
// sees failure, 200 replay convergence on the same key — was uncovered. The
// receipt path owns pool.Begin/tx.Commit directly (no begin seam), so this
// window is injected at the transport layer.
//
// Level: integration (testcontainers PostgreSQL 18, real BEGIN..COMMIT).
//
// Criteria: first attempt = 503 temporarily_unavailable, empty request_id, the
// `unavailable` audit intent; the receipt row is already durable (status
// accepted, wr- id); same-key retry = 200 with the SAME request_id and the
// `replayed` intent; keyed request count stays 1; withdrawal_request_audit
// holds exactly [created]; withdrawal_authorizations and
// withdrawal_grant_audit rows are byte-identical.
//
// Fault injection (deterministic, channel handshakes only, never a sleep): the
// pool wraps every backend net.Conn via pgconn's AfterNetConnect hook — the
// mechanism carrier_kill_integration_test.go already proves; DialFunc would
// additionally have to re-implement connect plumbing for no gain — and
// withholds the server's COMMIT CommandComplete from pgx. CommandComplete
// ('COMMIT') is sent only after the WAL flush, so the barrier is the server
// acknowledgement itself. The test then cancels the REQUEST context before
// releasing the blocked read: tx.Commit fails and the post-commit classify runs
// on the same cancelled context, so the caller-visible path is unavailable
// while the row is already durable.
package withdrawal

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// errIntakeCommitAckHeld is the synthetic transport failure the wrapper returns
// once the server's COMMIT acknowledgement was withheld: the caller frame must
// see an error, the server side is already durable.
var errIntakeCommitAckHeld = errors.New("test injection: COMMIT acknowledgement withheld after durable server commit")

// intakeCommitHoldConn wraps one backend net.Conn and drops the first
// CommandComplete('COMMIT') message (plus everything after it) instead of
// delivering it to pgx. The ack channel is closed exactly when the server ack
// was seen; Read then blocks until release, so the test controls the instant at
// which the caller frame observes the failure.
type intakeCommitHoldConn struct {
	net.Conn

	ack     chan struct{}
	release chan struct{}
	once    sync.Once

	pending []byte // server bytes not yet classified (starts at a message boundary)
	out     []byte // whole messages cleared for delivery
	held    bool   // COMMIT ack seen; withhold everything from here on
}

func (c *intakeCommitHoldConn) Read(p []byte) (int, error) {
	for {
		if len(c.out) > 0 {
			n := copy(p, c.out)
			c.out = c.out[n:]
			return n, nil
		}
		if c.held {
			<-c.release // closed by the test once the request context is cancelled
			return 0, errIntakeCommitAckHeld
		}
		if off := intakeScanCommitAck(c.pending); off >= 0 {
			c.out = c.pending[:off]
			c.pending = c.pending[off:]
			c.held = true
			c.once.Do(func() { close(c.ack) })
			continue
		}
		if n := intakeCompleteMessages(c.pending); n > 0 {
			c.out = c.pending[:n]
			c.pending = c.pending[n:]
			continue
		}
		buf := make([]byte, 32*1024)
		n, err := c.Conn.Read(buf)
		if n > 0 {
			c.pending = append(c.pending, buf[:n]...)
			continue
		}
		if err != nil {
			if len(c.pending) > 0 {
				c.out = c.pending
				c.pending = nil
				continue
			}
			return 0, err
		}
	}
}

// intakeScanCommitAck returns the offset of the first whole CommandComplete
// message tagged "COMMIT" (always a message boundary), or -1 while it is not
// yet fully buffered. Shape mirrors carrier_kill_integration_test.go's
// pbKillScanCommitAck for the internal test package.
func intakeScanCommitAck(buf []byte) int {
	for i := 0; i+5 <= len(buf); {
		msgLen := int(binary.BigEndian.Uint32(buf[i+1 : i+5]))
		if msgLen < 4 {
			return -1
		}
		total := 1 + msgLen
		if i+total > len(buf) {
			return -1
		}
		if buf[i] == 'C' && string(bytes.TrimRight(buf[i+5:i+total], "\x00")) == "COMMIT" {
			return i
		}
		i += total
	}
	return -1
}

// intakeCompleteMessages returns the length of the longest whole-message prefix
// of buf (0 when the first message is still incomplete).
func intakeCompleteMessages(buf []byte) int {
	i := 0
	for i+5 <= len(buf) {
		msgLen := int(binary.BigEndian.Uint32(buf[i+1 : i+5]))
		if msgLen < 4 {
			return i
		}
		total := 1 + msgLen
		if i+total > len(buf) {
			break
		}
		i += total
	}
	return i
}

// intakeCommitHoldHarness is the wrapped pool plus its two handshake channels.
type intakeCommitHoldHarness struct {
	pool    *pgxpool.Pool
	ack     <-chan struct{}
	release chan struct{}
}

// intakeCommitHoldPool opens a second pool over the migrated scratch database
// whose every backend connection withholds the COMMIT acknowledgement.
func intakeCommitHoldPool(t *testing.T, ctx context.Context, dsn string) *intakeCommitHoldHarness {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse dsn for hold pool: %v", err)
	}
	ack := make(chan struct{})
	release := make(chan struct{})
	cfg.ConnConfig.AfterNetConnect = func(_ context.Context, _ *pgconn.Config, conn net.Conn) (net.Conn, error) {
		return &intakeCommitHoldConn{Conn: conn, ack: ack, release: release}, nil
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("open hold pool: %v", err)
	}
	t.Cleanup(pool.Close)
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		t.Fatalf("ping hold pool: %v", err)
	}
	return &intakeCommitHoldHarness{pool: pool, ack: ack, release: release}
}

// intakeCommitRowDigest snapshots one row wholesale (md5 over its full text)
// for byte-identical before/after comparisons.
func intakeCommitRowDigest(t *testing.T, ctx context.Context, pool *pgxpool.Pool, query string, args ...any) string {
	t.Helper()
	var digest string
	if err := pool.QueryRow(ctx, query, args...).Scan(&digest); err != nil {
		t.Fatalf("snapshot row (%s): %v", query, err)
	}
	return digest
}

// TestWithdrawalIntakeCommitUnknownSameKeyConverges proves the durable-commit /
// caller-failure window converges: the first submit returns the retryable 503
// while the receipt is already committed, and the same-key retry returns 200
// with the same request_id, one row, one audit row, unchanged grants.
func TestWithdrawalIntakeCommitUnknownSameKeyConverges(t *testing.T) {
	ctx, pool, dsn := failureSetup(t)
	const (
		callerID = int64(7210)
		authID   = "auth-commit-unknown"
		idemKey  = "idem-commit-unknown"
	)
	key := intakeKey(t, ctx, pool, callerID)
	intakeSupplyGrant(t, ctx, pool, callerID, authID, intakeAmount)

	grantBefore := intakeCommitRowDigest(t, ctx, pool,
		`SELECT md5(x::text) FROM withdrawal_authorizations x WHERE authorization_id = $1`, authID)
	grantAuditBefore := failureGrantAuditCount(t, ctx, pool, authID)

	harness := intakeCommitHoldPool(t, ctx, dsn)
	submitCtx, cancel := context.WithCancel(ctx)

	// When: the receipt transaction commits on the server while pgx is held
	// inside the withheld COMMIT acknowledgement.
	type outcome struct {
		res *SubmitResult
		err error
	}
	done := make(chan outcome, 1)
	go func() {
		res, err := SubmitWithdrawal(submitCtx, harness.pool, intakeReq(key, idemKey, authID))
		done <- outcome{res: res, err: err}
	}()

	select {
	case <-harness.ack:
		// The server acknowledged COMMIT: the row is durable.
	case <-time.After(30 * time.Second):
		t.Fatal("COMMIT acknowledgement barrier never fired; the injection is broken")
	}
	// Break the caller's view without touching durable state: cancel the
	// request context BEFORE releasing the blocked read, so tx.Commit errors
	// and the post-commit classify (same context) is unavailable too.
	cancel()
	close(harness.release)
	out := <-done

	// Then: the contract 503 shape, not a defect error and not an "Accepted".
	if out.err != nil {
		t.Fatalf("commit-unknown submit = defect error %v, want a 503 contract outcome", out.err)
	}
	if out.res.Status != 503 || out.res.Code != CodeTemporarilyUnavailable {
		t.Fatalf("commit-unknown result = %+v, want 503/%s", out.res, CodeTemporarilyUnavailable)
	}
	if out.res.RequestID != "" {
		t.Fatalf("commit-unknown 503 request_id = %q, want empty", out.res.RequestID)
	}
	if out.res.Message != intakeUnavailableMessage {
		t.Fatalf("commit-unknown message = %q, want %q", out.res.Message, intakeUnavailableMessage)
	}
	intakeWantIntent(t, out.res, auditActionUnavailable, callerID, "")

	// Then: the commit landed durably — exactly one accepted row, one wr- id.
	var durableID, durableStatus string
	if err := pool.QueryRow(ctx,
		`SELECT request_id, status FROM withdrawal_requests WHERE caller_id = $1 AND idempotency_key = $2`,
		callerID, idemKey).Scan(&durableID, &durableStatus); err != nil {
		t.Fatalf("read durable receipt after the unknown commit: %v", err)
	}
	if durableStatus != "accepted" || !intakeRequestIDShape.MatchString(durableID) {
		t.Fatalf("durable receipt = (id=%q status=%q), want an accepted wr- id", durableID, durableStatus)
	}
	if n := failureRequestCountForKey(t, ctx, pool, callerID, idemKey); n != 1 {
		t.Fatalf("keyed request rows after the unknown commit = %d, want 1", n)
	}

	// Then: the same-key retry converges on the durable row (200, SAME id,
	// replayed intent) through the same transport.
	replay, err := SubmitWithdrawal(ctx, harness.pool, intakeReq(key, idemKey, authID))
	if err != nil {
		t.Fatalf("same-key retry = defect error %v, want a 200 outcome", err)
	}
	if replay.Status != 200 || replay.RequestID != durableID {
		t.Fatalf("retry = %+v, want 200 with the same request_id %q", replay, durableID)
	}
	intakeWantIntent(t, replay, auditActionReplayed, callerID, durableID)

	// Then: no second row, exactly the in-tx `created` audit, grants untouched.
	if n := failureRequestCountForKey(t, ctx, pool, callerID, idemKey); n != 1 {
		t.Fatalf("keyed request rows after the retry = %d, want 1", n)
	}
	if n := intakeRequestCount(t, ctx, pool); n != 1 {
		t.Fatalf("total request rows = %d, want 1 (no extra identity)", n)
	}
	if n := intakeAuditCount(t, ctx, pool); n != 1 {
		t.Fatalf("audit rows = %d, want exactly 1", n)
	}
	intakeWantActions(t, intakeAuditActions(t, ctx, pool), []string{auditActionCreated})
	if got := intakeCommitRowDigest(t, ctx, pool,
		`SELECT md5(x::text) FROM withdrawal_authorizations x WHERE authorization_id = $1`, authID); got != grantBefore {
		t.Fatalf("grant row changed: before=%s after=%s", grantBefore, got)
	}
	if got := failureGrantAuditCount(t, ctx, pool, authID); got != grantAuditBefore {
		t.Fatalf("grant-audit rows for %s changed %d -> %d", authID, grantAuditBefore, got)
	}
}
