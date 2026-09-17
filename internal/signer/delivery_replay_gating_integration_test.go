//go:build integration

// delivery_replay_gating_integration_test.go owns the reopened T027/T034
// contract-deviation case for 009-signer-service: a committed `delivered`
// marker is NOT a retransmit permit (contracts/gates.md §3). Every delivery —
// including a same-identity, same-content retransmit of an already-delivered
// request — re-passes the current gates inside the delivery lock window:
//
//   - one successful delivery commits the marker (one persisted signature row);
//   - revoke / expiry / 006 pause / can_sign-off each block the replay with
//     zero signature bytes (status-only), the committed marker retained and the
//     persisted result never re-signed;
//   - an unblocked replay hands out byte-identical bytes and records no new
//     signature_results row;
//   - a replay transport failure maps to `unknown_reconcile` (never
//     success-from-history) with the failed admission rolled back, the original
//     marker standing and the audit recording `bytes_may_be_out`.
package signer

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// rpSignatureRows counts the persisted signature_results rows for a request;
// no delivery path ever re-signs, so the count is invariant.
func rpSignatureRows(t *testing.T, pool *pgxpool.Pool, rowID int64) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM signature_results WHERE signing_request_row = $1`, rowID).Scan(&n); err != nil {
		t.Fatalf("count signature_results: %v", err)
	}
	return n
}

// rpDeliverFirst runs the one successful delivery that commits the delivered
// marker, returning the exact bytes handed out.
func rpDeliverFirst(t *testing.T, ctx context.Context, pool *pgxpool.Pool, f *dlvFixture) []byte {
	t.Helper()
	sink := &dlvSink{}
	res, err := Deliver(ctx, dlvDeps(pool, &dlvBinding{results: []BindingResult{BindingMatches}}, nil),
		Caller{ID: f.callerID, CanSign: true}, f.requestID, sink)
	if err != nil || res.Verdict != VerdictDelivered || sink.count() != 1 {
		t.Fatalf("first delivery = %+v / %v sink=%d, want delivered with one payload", res, err, sink.count())
	}
	if !dlvDeliveredMarker(t, pool, f.rowID) {
		t.Fatal("first delivery did not commit a delivered marker")
	}
	if got := rpSignatureRows(t, pool, f.rowID); got != 1 {
		t.Fatalf("first delivery left %d signature row(s), want 1", got)
	}
	return append([]byte(nil), sink.last()...)
}

// rpAssertBlockedReplay asserts a blocked replay delivered zero signature bytes
// (status-only), recorded the blocked admission superseding the marker for this
// attempt, retained the committed marker, and never re-signed.
func rpAssertBlockedReplay(t *testing.T, ctx context.Context, pool *pgxpool.Pool, f *dlvFixture, wantObserved string) {
	t.Helper()
	replay := &dlvSink{}
	res, err := Deliver(ctx, dlvDeps(pool, &dlvBinding{results: []BindingResult{BindingMatches}}, nil),
		Caller{ID: f.callerID, CanSign: true}, f.requestID, replay)
	if res == nil || res.Verdict != VerdictBlocked {
		t.Fatalf("blocked replay = %+v / %v, want blocked", res, err)
	}
	signerAuthRefusal(t, err, ClassSignatureWithheld)
	if replay.count() != 0 {
		t.Fatalf("blocked replay wrote %d payload(s), want zero signature bytes", replay.count())
	}
	if !dlvDeliveredMarker(t, pool, f.rowID) {
		t.Fatal("blocked replay retracted the committed delivered marker")
	}
	if got := dlvAdmissionCount(t, pool, f.rowID); got != 2 {
		t.Fatalf("blocked replay left %d admission row(s), want the first + one blocked", got)
	}
	var verdict, reason string
	if err := pool.QueryRow(ctx,
		`SELECT verdict, reason FROM delivery_admissions
		   WHERE signing_request_row = $1 ORDER BY attempt_seq DESC LIMIT 1`, f.rowID).Scan(&verdict, &reason); err != nil {
		t.Fatalf("read blocked replay admission: %v", err)
	}
	if verdict != string(VerdictBlocked) || reason != string(ClassSignatureWithheld) {
		t.Fatalf("blocked replay admission = %s/%s, want blocked/signature_withheld", verdict, reason)
	}
	if _, _, detail := dlvLastAudit(t, pool, f.requestID); !strings.Contains(detail, wantObserved) {
		t.Fatalf("blocked replay audit detail %q missing observed class %q", detail, wantObserved)
	}
	if got := rpSignatureRows(t, pool, f.rowID); got != 1 {
		t.Fatalf("blocked replay changed signature_results rows to %d, want 1 (never re-signs)", got)
	}
	dlvAssertNoResign(t, pool, f)
}

// TestSignerDeliveryReplayGating drives the reopened T027/T034 ruling: a
// historic delivered marker never exempts a retransmit from current gates.
func TestSignerDeliveryReplayGating(t *testing.T) {
	pool := signerAuthStartPG(t)
	ctx := context.Background()
	n := 0
	next := func() *dlvFixture { n++; return dlvSeed(t, pool, n) }

	t.Run("unblocked replay is byte-identical and never re-signs", func(t *testing.T) {
		gateReset006(t, pool)
		f := next()
		first := rpDeliverFirst(t, ctx, pool, f)

		replay := &dlvSink{}
		res, err := Deliver(ctx, dlvDeps(pool, &dlvBinding{results: []BindingResult{BindingMatches}}, nil),
			Caller{ID: f.callerID, CanSign: true}, f.requestID, replay)
		if err != nil || res.Verdict != VerdictDelivered {
			t.Fatalf("unblocked replay = %+v / %v, want delivered", res, err)
		}
		if replay.count() != 1 || !bytes.Equal(replay.last(), first) {
			t.Fatalf("unblocked replay sink=%d byte-identical=%v, want one byte-identical payload",
				replay.count(), replay.count() == 1 && bytes.Equal(replay.last(), first))
		}
		if !dlvDeliveredMarker(t, pool, f.rowID) {
			t.Fatal("unblocked replay lost the delivered marker")
		}
		if got := rpSignatureRows(t, pool, f.rowID); got != 1 {
			t.Fatalf("unblocked replay wrote %d signature row(s), want 1 (never re-signs)", got)
		}
		dlvAssertNoResign(t, pool, f)
	})

	t.Run("revoked replay is blocked with zero signature bytes", func(t *testing.T) {
		gateReset006(t, pool)
		f := next()
		rpDeliverFirst(t, ctx, pool, f)
		gateExec(t, pool, `UPDATE withdrawal_authorizations SET state = 'revoked' WHERE authorization_id = $1`, f.authzID)
		rpAssertBlockedReplay(t, ctx, pool, f, "authorization_revoked")
	})

	t.Run("expired replay is blocked with zero signature bytes", func(t *testing.T) {
		gateReset006(t, pool)
		f := next()
		rpDeliverFirst(t, ctx, pool, f)
		gateExec(t, pool, `UPDATE withdrawal_authorizations
			SET expires_at = now() - interval '1 minute' WHERE authorization_id = $1`, f.authzID)
		rpAssertBlockedReplay(t, ctx, pool, f, "authorization_expired")
	})

	t.Run("006 pause replay is blocked with zero signature bytes", func(t *testing.T) {
		gateReset006(t, pool)
		t.Cleanup(func() { gateReset006(t, pool) })
		f := next()
		rpDeliverFirst(t, ctx, pool, f)
		gateExec(t, pool, `INSERT INTO indexer_pause
			(chain_id, height, expected_hash, actual_hash, kind) VALUES ($1, 10, $2, $3, 'hash_mismatch')`,
			gateChainID, gateHash("aa"), gateHash("bb"))
		rpAssertBlockedReplay(t, ctx, pool, f, "recovery_paused")
	})

	t.Run("can_sign off replay is blocked with zero signature bytes", func(t *testing.T) {
		gateReset006(t, pool)
		f := next()
		rpDeliverFirst(t, ctx, pool, f)
		if err := SetCanSign(ctx, pool, f.callerID, false); err != nil {
			t.Fatalf("SetCanSign(false): %v", err)
		}
		t.Cleanup(func() { _ = SetCanSign(context.Background(), pool, f.callerID, true) })
		rpAssertBlockedReplay(t, ctx, pool, f, "signing_not_permitted")
	})

	t.Run("replay transport failure is unknown never success from history", func(t *testing.T) {
		gateReset006(t, pool)
		f := next()
		rpDeliverFirst(t, ctx, pool, f)

		failSink := &dlvSink{fn: func(context.Context, []byte) error { return errors.New("transport reset") }}
		res, err := Deliver(ctx, dlvDeps(pool, &dlvBinding{results: []BindingResult{BindingMatches}}, nil),
			Caller{ID: f.callerID, CanSign: true}, f.requestID, failSink)
		if res == nil || res.Verdict != VerdictUnknownReconcile || res.Class != ClassOutcomeUnknown {
			t.Fatalf("failed replay = %+v / %v, want unknown_reconcile/outcome_unknown", res, err)
		}
		signerAuthRefusal(t, err, ClassOutcomeUnknown)
		if failSink.count() != 1 {
			t.Fatalf("failed replay sink attempts = %d, want 1 (bytes may be out)", failSink.count())
		}
		// The failed region rolled back: no new admission, the original marker
		// stands (the past delivery is neither backfilled nor retracted).
		if got := dlvAdmissionCount(t, pool, f.rowID); got != 1 {
			t.Fatalf("failed replay left %d admission row(s), want the original 1", got)
		}
		if !dlvDeliveredMarker(t, pool, f.rowID) {
			t.Fatal("failed replay lost the original delivered marker")
		}
		action, reason, detail := dlvLastAudit(t, pool, f.requestID)
		if action != "delivery_unknown" || reason != string(ClassOutcomeUnknown) || !strings.Contains(detail, "bytes_may_be_out=true") {
			t.Fatalf("failed replay audit = %s/%s %q, want delivery_unknown/outcome_unknown with bytes_may_be_out", action, reason, detail)
		}
		if got := rpSignatureRows(t, pool, f.rowID); got != 1 {
			t.Fatalf("failed replay changed signature_results rows to %d, want 1", got)
		}
		dlvAssertNoResign(t, pool, f)
	})
}
