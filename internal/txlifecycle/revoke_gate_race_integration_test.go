//go:build integration

// revoke_gate_race_integration_test.go owns the V13-2b scene recorded as the
// unexecuted gap in specs/011-withdrawal-executor/integration-readiness.md:
// revoke/expiry racing a send at 010's gate in BOTH lock orders, over a real
// PostgreSQL and the real 010 send region, with the real 011 claim revoke
// (execution.RevokeClaimTx) — never a sequential call or a mock standing in
// for the interleave.
//
// What it proves (each leg pinned by a lock-observation barrier in the shape
// of the 008 precedent, internal/nonce/authz_revoke_race_integration_test.go):
//
//  1. The real revoke commits while the send waits for the claim share lock:
//     the send's `SELECT … FOR SHARE` re-evaluates under READ COMMITTED,
//     refuses claim_revoked with zero dispatch and committed gate_refused
//     evidence, and neither rewrites the attempt nor writes a send row.
//  2. The send region takes the claim FOR SHARE first: the real revoke's
//     UPDATE is observed waiting on that row (with an independent NOWAIT
//     probe proving the region holds the share lock), the region commits its
//     single dispatch first, and only then does the revoke commit. A second
//     send afterwards is fenced with claim_revoked, zero new dispatch, and no
//     second attempt row for the step.
//  3. Natural claim expiry (a disqualifier, never a state rewrite) commits
//     while the send waits: the refusal is the DB-clock comparison
//     (`clock_timestamp()`) in the claim read, zero dispatch.
//  4. Natural authorization expiry committed while the request is queued
//     inside the region before dispatch entry: the gate's own wall-clock read
//     refuses authorization_expired with zero dispatch and NO
//     post_final_check_expiry residual — a queued request is not in flight,
//     G-010-1 is not widened, and there is no grace/TTL.
//
// Determinism: every park is a real lock conflict (ACCESS EXCLUSIVE on a
// table the region reads after the claim share lock; an uncommitted writer
// holding the claim row), observed through pg_stat_activity and an
// independent lock probe, never sleeps alone. Every raw transaction carries a
// lock_timeout and every wait has a deadline, so a lock cycle surfaces as a
// bounded failure; a failed scene prints a sanitized timeline of fixed labels
// and refusal classes only — no DSN, no credentials, no raw bytes, no SQL
// parameter values.
package txlifecycle

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/execution"
)

const (
	// rgHoldWindow is how long a blocked participant must stay blocked for the
	// observation to count as "held" (the 008 V11 hold window).
	rgHoldWindow = 150 * time.Millisecond
	// rgWaitDeadline bounds every pg_stat_activity observation.
	rgWaitDeadline = 5 * time.Second
	// rgResultDeadline bounds every participant's completion; exceeding it is
	// a deadlock/permanent-wait failure, never a pass.
	rgResultDeadline = 15 * time.Second
	// rgRawLockTimeout bounds the raw writer/barrier transactions.
	rgRawLockTimeout = "8s"
)

// TestV13_2bRevokeGateInterleave is the V13-2b acceptance over one migrated
// scratch database. The chain double counts dispatches (zero-dispatch
// refusals are observable); the gate, the SQL locks, and the 011 revoke path
// are all real.
func TestV13_2bRevokeGateInterleave(t *testing.T) {
	e := newEnv(t)
	rp := rgOpenPool(t, e.dsn)
	ctx := context.Background()

	// --- revoke-first: the revoke commits, the send re-checks and refuses ---
	t.Run("claim_revoke_commits_first_refuses_zero_dispatch", func(t *testing.T) {
		tl := rgTimelineFor(t)
		base := e.rpc.dispatchCount()
		f := e.seed()
		f.sign()

		raw := rgBeginRaw(t, rp)
		applied, err := execution.RevokeClaimTx(ctx, raw, f.intentID, f.leaseVersion,
			"lane-a13-v13-2b", "interleave: revoke commits before the send observes the claim")
		if err != nil || !applied {
			t.Fatalf("RevokeClaimTx = %v (err %v), want applied", applied, err)
		}
		tl.mark("real 011 revoke applied (uncommitted; claim row locked)")

		done := rgSendAsync(e, f, SendInitial)
		rgWaitForWaiter(t, rp, "FROM execution_claims", tl)
		tl.mark("send observed waiting on the claim FOR SHARE")
		rgWantHeld(t, "send", done, tl)

		rgCommitRaw(t, raw)
		tl.mark("revoke committed while the send waited")
		out := rgWaitSend(t, done, tl)

		assertBlocked(t, e, f.attemptID, out.res, out.err, ClassClaimRevoked, base)
		if state := rgClaimState(t, rp, f.intentID); state != "revoked" {
			t.Fatalf("claim state = %q, want revoked", state)
		}
		if n := rgCount(t, rp,
			`SELECT count(*) FROM execution_events WHERE intent_id = $1 AND kind = $2`,
			f.intentID, execution.EventRevoked); n != 1 {
			t.Fatalf("execution_events revoked rows = %d, want 1 (real 011 revoke evidence)", n)
		}
		if got := rgAttemptState(t, rp, f.attemptID); got != "signed" {
			t.Fatalf("attempt state = %q, want signed (refusal must not dispatch or rewrite)", got)
		}
		if n := rgCount(t, rp, `SELECT count(*) FROM tx_send_attempts WHERE attempt_id = $1`, f.attemptID); n != 0 {
			t.Fatalf("tx_send_attempts rows = %d, want 0 (zero dispatch)", n)
		}
		tl.mark("refusal settled: claim_revoked, zero dispatch, no send row")
	})

	// --- send-region-first: the revoke waits behind the region's share lock --
	t.Run("send_region_lock_first_revoke_waits_then_fenced", func(t *testing.T) {
		tl := rgTimelineFor(t)
		base := e.rpc.dispatchCount()
		f := e.seed()
		f.sign()

		// indexer_lease is the region's step-2 coordination row: it is read
		// after the claim FOR SHARE and is not touched by the pre-region
		// reconcile path (which already reads the 006 pause tables).
		park := rgLockTableRaw(t, rp, "indexer_lease")
		tl.mark("park: ACCESS EXCLUSIVE on indexer_lease")
		done := rgSendAsync(e, f, SendInitial)
		rgWaitForRegionPark(t, rp, tl, done)
		tl.mark("send parked at the coordination row, after the claim FOR SHARE")
		rgWantHeld(t, "send", done, tl)
		rgWantClaimShareHeld(t, rp, f.intentID, tl)

		revoke := rgRevokeAsync(rp, f.intentID, f.leaseVersion)
		rgWaitForWaiter(t, rp, "UPDATE execution_claims", tl)
		tl.mark("real 011 revoke blocked on the claim row held by the send region")
		rgWantHeldRevoke(t, revoke, tl)

		rgCommitRaw(t, park)
		tl.mark("park released")
		out := rgWaitSend(t, done, tl)
		if out.err != nil || out.res.Outcome != "accepted" {
			t.Fatalf("region send = %+v (err %v), want accepted", out.res, out.err)
		}
		tl.mark("region committed accepted (dispatches=%d)", e.rpc.dispatchCount())
		rgWaitRevoke(t, revoke, tl)
		tl.mark("revoke committed after the region")

		if n := e.rpc.dispatchCount(); n != base+1 {
			t.Fatalf("dispatch count = %d, want exactly %d", n, base+1)
		}
		if state := rgClaimState(t, rp, f.intentID); state != "revoked" {
			t.Fatalf("claim state = %q, want revoked", state)
		}
		var version int64
		var expiry *time.Time
		if err := rp.QueryRow(ctx,
			`SELECT observed_claim_version, observed_claim_expiry FROM tx_send_attempts WHERE attempt_id = $1`,
			f.attemptID).Scan(&version, &expiry); err != nil {
			t.Fatalf("read accepted send row: %v", err)
		}
		if version != f.leaseVersion || expiry == nil {
			t.Fatalf("accepted send basis = version %d expiry %v, want the pre-revoke active claim", version, expiry)
		}

		before := e.rpc.dispatchCount()
		res2, err2 := e.send(f, SendReplay, nil)
		assertBlocked(t, e, f.attemptID, res2, err2, ClassClaimRevoked, before)
		if n := rgCount(t, rp, `SELECT count(*) FROM tx_send_attempts WHERE attempt_id = $1`, f.attemptID); n != 1 {
			t.Fatalf("tx_send_attempts rows = %d, want 1 (no second dispatch for the step)", n)
		}
		if n := rgCount(t, rp, `SELECT count(*) FROM tx_attempts WHERE intent_id = $1`, f.intentID); n != 1 {
			t.Fatalf("tx_attempts rows = %d, want 1 (no second attempt for the step)", n)
		}
		tl.mark("post-revoke replay fenced: claim_revoked, zero new dispatch, one attempt")
	})

	// --- natural expiry, commit-first: the DB clock refuses, zero dispatch ---
	t.Run("claim_natural_expiry_commits_first_refuses_zero_dispatch", func(t *testing.T) {
		tl := rgTimelineFor(t)
		base := e.rpc.dispatchCount()
		f := e.seed()
		f.sign()

		raw := rgBeginRaw(t, rp)
		tag, err := raw.Exec(ctx, `UPDATE execution_claims
			SET acquired_at = clock_timestamp() - interval '2 hours',
			    expires_at = clock_timestamp() - interval '1 hour',
			    updated_at = clock_timestamp()
			WHERE intent_id = $1`, f.intentID)
		if err != nil || tag.RowsAffected() != 1 {
			t.Fatalf("stage claim expiry = %d rows (err %v), want 1", tag.RowsAffected(), err)
		}
		tl.mark("claim natural expiry staged on the DB clock (uncommitted; state stays active)")

		done := rgSendAsync(e, f, SendInitial)
		rgWaitForWaiter(t, rp, "FROM execution_claims", tl)
		rgWantHeld(t, "send", done, tl)
		rgCommitRaw(t, raw)
		tl.mark("expiry committed while the send waited")
		out := rgWaitSend(t, done, tl)

		assertBlocked(t, e, f.attemptID, out.res, out.err, ClassClaimExpired, base)
		detail := rgGateRefusalDetail(t, rp, f.attemptID, string(ClassClaimExpired))
		if !strings.Contains(detail, "database clock") {
			t.Fatalf("claim_expired basis %q does not carry the DB-clock comparison", detail)
		}
		if state := rgClaimState(t, rp, f.intentID); state != "active" {
			t.Fatalf("claim state = %q, want active (natural expiry is not a state rewrite)", state)
		}
		if got := rgAttemptState(t, rp, f.attemptID); got != "signed" {
			t.Fatalf("attempt state = %q, want signed", got)
		}
		if n := rgCount(t, rp, `SELECT count(*) FROM tx_send_attempts WHERE attempt_id = $1`, f.attemptID); n != 0 {
			t.Fatalf("tx_send_attempts rows = %d, want 0 (zero dispatch)", n)
		}
		tl.mark("expiry refusal settled: claim_expired on the DB clock, zero dispatch")
	})

	// --- queued expiry: no residual, no in-flight treatment, no grace -------
	t.Run("authorization_expiry_while_queued_refuses_no_residual", func(t *testing.T) {
		tl := rgTimelineFor(t)
		base := e.rpc.dispatchCount()
		f := e.seed()
		f.sign()

		park := rgLockTableRaw(t, rp, "nonce_bindings")
		tl.mark("park: ACCESS EXCLUSIVE on nonce_bindings (gate step 5, after the claim FOR SHARE)")
		done := rgSendAsync(e, f, SendInitial)
		rgWaitForWaiter(t, rp, "FROM nonce_bindings", tl)
		tl.mark("send queued in-region before the authorization read")
		rgWantHeld(t, "send", done, tl)

		raw := rgBeginRaw(t, rp)
		tag, err := raw.Exec(ctx, `UPDATE withdrawal_authorizations
			SET expires_at = clock_timestamp() - interval '1 hour'
			WHERE authorization_id = $1`, f.authID)
		if err != nil || tag.RowsAffected() != 1 {
			t.Fatalf("stage authorization expiry = %d rows (err %v), want 1", tag.RowsAffected(), err)
		}
		rgCommitRaw(t, raw)
		tl.mark("authorization natural expiry committed while the request was queued")
		rgCommitRaw(t, park)

		out := rgWaitSend(t, done, tl)
		assertBlocked(t, e, f.attemptID, out.res, out.err, ClassAuthorizationExpired, base)
		detail := rgGateRefusalDetail(t, rp, f.attemptID, string(ClassAuthorizationExpired))
		if !strings.Contains(detail, "observed_now=") {
			t.Fatalf("authorization_expired basis %q does not carry the statement wall clock", detail)
		}
		// G-010-1 is not widened: a request queued before dispatch entry is not
		// in flight, so the zero-dispatch refusal must not record a residual.
		if n := rgCount(t, rp,
			`SELECT count(*) FROM tx_attempt_events WHERE attempt_id = $1 AND event = $2`,
			f.attemptID, EventPostFinalCheckExpiry); n != 0 {
			t.Fatalf("post_final_check_expiry rows = %d, want 0: queued is not in flight", n)
		}
		if got := rgAttemptState(t, rp, f.attemptID); got != "signed" {
			t.Fatalf("attempt state = %q, want signed", got)
		}
		if n := rgCount(t, rp, `SELECT count(*) FROM tx_send_attempts WHERE attempt_id = $1`, f.attemptID); n != 0 {
			t.Fatalf("tx_send_attempts rows = %d, want 0 (zero dispatch)", n)
		}
		tl.mark("queued expiry refusal settled: zero dispatch, no residual, no in-flight state")
	})
}

// --- raw-session plumbing (real PostgreSQL; bounded; rg-prefixed) ----------

// rgOpenPool opens a dedicated pool for raw writer/barrier sessions and
// pg_stat_activity observation.
func rgOpenPool(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	cfg.MaxConns = 6
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("open raw pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// rgBeginRaw opens one raw transaction with a bounded lock_timeout (never a
// statement_timeout: a writer must be able to block, not fail on the pause).
func rgBeginRaw(t *testing.T, pool *pgxpool.Pool) pgx.Tx {
	t.Helper()
	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin raw tx: %v", err)
	}
	t.Cleanup(func() { _ = tx.Rollback(context.Background()) }) // no-op after an explicit Commit
	if _, err := tx.Exec(context.Background(), `SET LOCAL lock_timeout = '`+rgRawLockTimeout+`'`); err != nil {
		t.Fatalf("set raw lock_timeout: %v", err)
	}
	return tx
}

// rgLockTableRaw takes ACCESS EXCLUSIVE on a table the send region reads after
// the claim share lock, parking the live region at that step (the 008
// rrLockTableRaw precedent).
func rgLockTableRaw(t *testing.T, pool *pgxpool.Pool, table string) pgx.Tx {
	t.Helper()
	tx := rgBeginRaw(t, pool)
	if _, err := tx.Exec(context.Background(),
		fmt.Sprintf(`LOCK TABLE %s IN ACCESS EXCLUSIVE MODE`, table)); err != nil {
		t.Fatalf("lock table %s: %v", table, err)
	}
	return tx
}

func rgCommitRaw(t *testing.T, tx pgx.Tx) {
	t.Helper()
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatalf("commit raw tx: %v", err)
	}
}

// rgWaitForWaiter blocks until some other session is waiting on a lock whose
// current statement matches needle.
func rgWaitForWaiter(t *testing.T, pool *pgxpool.Pool, needle string, tl *rgTimeline) {
	t.Helper()
	deadline := time.Now().Add(rgWaitDeadline)
	for {
		var n int
		if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM pg_stat_activity
			WHERE pid <> pg_backend_pid() AND wait_event_type = 'Lock'
			  AND position($1 in query) > 0`, needle).Scan(&n); err != nil {
			t.Fatalf("poll pg_stat_activity for %q: %v\ntimeline:\n%s", needle, err, tl.render())
		}
		if n > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("no session waited on a lock matching %q within %s\ntimeline:\n%s",
				needle, rgWaitDeadline, tl.render())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// rgWaitForRegionPark blocks until the send is observed waiting on the parked
// coordination row; a send that returns first is a failed scene (reported
// with its class, never silently accepted).
func rgWaitForRegionPark(t *testing.T, pool *pgxpool.Pool, tl *rgTimeline, done <-chan rgSendOutcome) {
	t.Helper()
	deadline := time.Now().Add(rgWaitDeadline)
	lastWait := "(none observed)"
	for {
		select {
		case out := <-done:
			t.Fatalf("send returned before parking at the 006 lock: outcome=%q class=%q err=%v\nlast lock wait: %s\ntimeline:\n%s",
				out.res.Outcome, out.res.RefusalClass, out.err, lastWait, tl.render())
		default:
		}
		var query string
		err := pool.QueryRow(context.Background(), `SELECT COALESCE(left(query, 70), '(null)') FROM pg_stat_activity
			WHERE pid <> pg_backend_pid() AND wait_event_type = 'Lock' ORDER BY pid LIMIT 1`).Scan(&query)
		switch {
		case err == nil:
			lastWait = query
			if strings.Contains(query, "FROM indexer_lease") {
				return
			}
		case errors.Is(err, pgx.ErrNoRows):
		default:
			t.Fatalf("poll pg_stat_activity for the region park: %v\ntimeline:\n%s", err, tl.render())
		}
		if time.Now().After(deadline) {
			t.Fatalf("send never parked at the coordination row within %s\nlast lock wait: %s\n%s\ntimeline:\n%s",
				rgWaitDeadline, lastWait, rgNonIdleCensus(t, pool), tl.render())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// rgNonIdleCensus is a sanitized backend census for failure output: state and
// wait event names only, never query text or parameters.
func rgNonIdleCensus(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	var blob string
	if err := pool.QueryRow(context.Background(),
		`SELECT COALESCE(string_agg(state || '/' || COALESCE(wait_event_type, '-') || '/' || COALESCE(wait_event, '-'), ',' ORDER BY pid), '(none)')
		   FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND state <> 'idle'`).Scan(&blob); err != nil {
		return "non-idle census: unavailable"
	}
	return "non-idle census (state/wait_type/wait_event): " + blob
}

// rgWantClaimShareHeld proves the send region holds the claim FOR SHARE lock:
// an independent FOR UPDATE NOWAIT probe must fail with SQLSTATE 55P03.
func rgWantClaimShareHeld(t *testing.T, pool *pgxpool.Pool, intentID string, tl *rgTimeline) {
	t.Helper()
	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin NOWAIT probe: %v", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	var got string
	err = tx.QueryRow(context.Background(),
		`SELECT intent_id FROM execution_claims WHERE intent_id = $1 FOR UPDATE NOWAIT`, intentID).Scan(&got)
	if err == nil {
		t.Fatalf("NOWAIT probe acquired the claim row: the region does not hold the share lock\ntimeline:\n%s", tl.render())
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "55P03" {
		t.Fatalf("NOWAIT probe error = %v, want SQLSTATE 55P03 (lock_not_available)\ntimeline:\n%s", err, tl.render())
	}
	tl.mark("NOWAIT probe refused (55P03): the region holds the claim share lock")
}

// --- participant channels --------------------------------------------------

type rgSendOutcome struct {
	res SendResult
	err error
}

func rgSendAsync(e *env, f *fixture, kind SendKind) <-chan rgSendOutcome {
	done := make(chan rgSendOutcome, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), rgResultDeadline)
		defer cancel()
		res, err := e.store.Send(ctx, &SendRequest{AttemptID: f.attemptID, Kind: kind, Claim: f.claim()})
		done <- rgSendOutcome{res: res, err: err}
	}()
	return done
}

type rgRevokeOutcome struct {
	applied bool
	err     error
}

// rgRevokeAsync runs the real 011 revoke in its own transaction and commits
// it as soon as it is no longer blocked. On error the transaction rolls back.
func rgRevokeAsync(pool *pgxpool.Pool, intentID string, version int64) <-chan rgRevokeOutcome {
	done := make(chan rgRevokeOutcome, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), rgResultDeadline)
		defer cancel()
		tx, err := pool.Begin(ctx)
		if err != nil {
			done <- rgRevokeOutcome{err: err}
			return
		}
		defer func() { _ = tx.Rollback(context.Background()) }()
		if _, err := tx.Exec(ctx, `SET LOCAL lock_timeout = '`+rgRawLockTimeout+`'`); err != nil {
			done <- rgRevokeOutcome{err: err}
			return
		}
		applied, err := execution.RevokeClaimTx(ctx, tx, intentID, version,
			"lane-a13-v13-2b", "interleave: send region holds the claim lock first")
		if err != nil || !applied {
			done <- rgRevokeOutcome{applied: applied, err: err}
			return
		}
		done <- rgRevokeOutcome{applied: true, err: tx.Commit(ctx)}
	}()
	return done
}

func rgWantHeld(t *testing.T, label string, done <-chan rgSendOutcome, tl *rgTimeline) {
	t.Helper()
	select {
	case out := <-done:
		t.Fatalf("%s returned while it should have been blocked: outcome=%q class=%q err=%v\ntimeline:\n%s",
			label, out.res.Outcome, out.res.RefusalClass, out.err, tl.render())
	case <-time.After(rgHoldWindow):
		tl.mark("%s still blocked after the hold window", label)
	}
}

func rgWaitSend(t *testing.T, done <-chan rgSendOutcome, tl *rgTimeline) rgSendOutcome {
	t.Helper()
	select {
	case out := <-done:
		tl.mark("send returned outcome=%q class=%q", out.res.Outcome, out.res.RefusalClass)
		return out
	case <-time.After(rgResultDeadline):
		t.Fatalf("send did not complete within %s (possible deadlock/permanent wait)\ntimeline:\n%s",
			rgResultDeadline, tl.render())
		return rgSendOutcome{}
	}
}

func rgWantHeldRevoke(t *testing.T, done <-chan rgRevokeOutcome, tl *rgTimeline) {
	t.Helper()
	select {
	case out := <-done:
		t.Fatalf("revoke returned while blocked behind the send region: applied=%v err=%v\ntimeline:\n%s",
			out.applied, out.err, tl.render())
	case <-time.After(rgHoldWindow):
		tl.mark("revoke still blocked after the hold window")
	}
}

func rgWaitRevoke(t *testing.T, done <-chan rgRevokeOutcome, tl *rgTimeline) {
	t.Helper()
	select {
	case out := <-done:
		if out.err != nil || !out.applied {
			t.Fatalf("revoke outcome = applied %v (err %v), want applied\ntimeline:\n%s",
				out.applied, out.err, tl.render())
		}
	case <-time.After(rgResultDeadline):
		t.Fatalf("revoke did not complete within %s (possible deadlock/permanent wait)\ntimeline:\n%s",
			rgResultDeadline, tl.render())
	}
}

// --- read-back evidence helpers --------------------------------------------

func rgCount(t *testing.T, pool *pgxpool.Pool, query string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("count query: %v", err)
	}
	return n
}

func rgClaimState(t *testing.T, pool *pgxpool.Pool, intentID string) string {
	t.Helper()
	var state string
	if err := pool.QueryRow(context.Background(),
		`SELECT state FROM execution_claims WHERE intent_id = $1`, intentID).Scan(&state); err != nil {
		t.Fatalf("read claim state: %v", err)
	}
	return state
}

func rgAttemptState(t *testing.T, pool *pgxpool.Pool, attemptID string) string {
	t.Helper()
	var state string
	if err := pool.QueryRow(context.Background(),
		`SELECT state FROM tx_attempts WHERE attempt_id = $1`, attemptID).Scan(&state); err != nil {
		t.Fatalf("read attempt state: %v", err)
	}
	return state
}

// rgGateRefusalDetail reads the latest committed gate_refused basis for a
// class, so the DB-clock evidence (not just the class) is asserted.
func rgGateRefusalDetail(t *testing.T, pool *pgxpool.Pool, attemptID, class string) string {
	t.Helper()
	var detail string
	if err := pool.QueryRow(context.Background(),
		`SELECT detail FROM tx_attempt_events
		  WHERE attempt_id = $1 AND event = 'gate_refused' AND reason_class = $2
		  ORDER BY event_seq DESC LIMIT 1`, attemptID, class).Scan(&detail); err != nil {
		t.Fatalf("read gate_refused detail: %v", err)
	}
	return detail
}

// --- sanitized timeline ----------------------------------------------------

// rgTimeline records fixed labels and elapsed milliseconds only; it never
// records DSNs, credentials, SQL parameter values, or raw bytes. A failed
// scene prints it in the test cleanup.
type rgTimeline struct {
	mu     sync.Mutex
	start  time.Time
	events []string
}

func rgTimelineFor(t *testing.T) *rgTimeline {
	t.Helper()
	tl := &rgTimeline{start: time.Now()}
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("V13-2b sanitized timeline:\n%s", tl.render())
		}
	})
	return tl
}

func (tl *rgTimeline) mark(format string, args ...any) {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	tl.events = append(tl.events, fmt.Sprintf("+%dms %s",
		time.Since(tl.start).Milliseconds(), fmt.Sprintf(format, args...)))
}

func (tl *rgTimeline) render() string {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	if len(tl.events) == 0 {
		return "(no events)"
	}
	return strings.Join(tl.events, "\n")
}
