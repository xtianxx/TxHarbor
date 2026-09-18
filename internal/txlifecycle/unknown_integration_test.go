//go:build integration

package txlifecycle

import (
	"context"
	"testing"

	"github.com/xtianxx/txharbor/internal/eth"
)

func (e *env) setDispatchErr(err error) {
	e.rpc.mu.Lock()
	e.rpc.txErr = err
	e.rpc.mu.Unlock()
}

// TestV4DispatchClassification is quickstart V4's classification matrix
// (dispatch-level injection): fail-safe unknown/rejected, durable bytes, and
// immutable send rows. The 010<->009 HTTP fault proxy named by T026 is not part
// of this lane-local injection (see task status).
func TestV4DispatchClassification(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()

	cases := []struct {
		name    string
		err     error
		outcome string
		class   string
		state   string
	}{
		{"timeout", &eth.Error{Kind: eth.KindTimeout, Op: "stub"}, "unknown", "timeout", "unknown"},
		{"transport", &eth.Error{Kind: eth.KindTransport, Op: "stub"}, "unknown", "transport", "unknown"},
		{"rate_limited", &eth.Error{Kind: eth.KindRateLimited, Op: "stub"}, "unknown", "rate_limited", "unknown"},
		{"invalid_response", &eth.Error{Kind: eth.KindInvalidResponse, Op: "stub"}, "unknown", "invalid_response", "unknown"},
		{"hash_mismatch", &eth.Error{Kind: eth.KindHashMismatch, Op: "stub"}, "unknown", "hash_mismatch", "unknown"},
		{"nonce_too_low", &eth.Error{Kind: eth.KindNonceTooLow, Op: "stub"}, "rejected", "nonce_too_low", "unknown"},
		{"insufficient_funds", &eth.Error{Kind: eth.KindInsufficientFunds, Op: "stub"}, "rejected", "insufficient_funds", "unknown"},
		{"already_known", &eth.Error{Kind: eth.KindAlreadyKnown, Op: "stub"}, "accepted", "already_known", "sent"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := e.seed()
			f.sign()
			hashBefore, _, err := e.store.signingRow(ctx, f.attemptID)
			if err != nil {
				t.Fatal(err)
			}
			e.setDispatchErr(c.err)
			defer e.setDispatchErr(nil)
			res, err := e.send(f, SendInitial, nil)
			if err != nil {
				t.Fatal(err)
			}
			if res.Outcome != c.outcome || res.RPCClass != c.class {
				t.Fatalf("outcome = %s/%s, want %s/%s", res.Outcome, res.RPCClass, c.outcome, c.class)
			}
			attempt, err := e.store.AttemptByID(ctx, f.attemptID)
			if err != nil {
				t.Fatal(err)
			}
			if attempt.State != c.state {
				t.Fatalf("state = %s, want %s", attempt.State, c.state)
			}
			// Bytes/hash are intact and unchanged.
			hashAfter, _, err := e.store.signingRow(ctx, f.attemptID)
			if err != nil {
				t.Fatal(err)
			}
			if hashAfter != hashBefore {
				t.Fatal("signing fact changed")
			}
			// The send row is never a success/failure verdict.
			var outcome string
			if err := e.pool.QueryRow(ctx,
				`SELECT outcome FROM tx_send_attempts WHERE attempt_id = $1 ORDER BY send_id DESC LIMIT 1`, f.attemptID).Scan(&outcome); err != nil {
				t.Fatal(err)
			}
			if outcome != c.outcome {
				t.Fatalf("durable outcome = %s, want %s", outcome, c.outcome)
			}
		})
	}
}

// TestV4KnownRowsImmutableAndRetry covers V4's "known rows are never updated"
// and the post-COMMIT response-loss retry path.
func TestV4KnownRowsImmutableAndRetry(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	f := e.seed()
	f.sign()

	res, err := e.send(f, SendInitial, nil)
	if err != nil || res.Outcome != "accepted" {
		t.Fatalf("initial = %+v %v", res, err)
	}
	var sendID int64
	var outcome, class string
	if err := e.pool.QueryRow(ctx,
		`SELECT send_id, outcome, rpc_class FROM tx_send_attempts WHERE attempt_id = $1`, f.attemptID).
		Scan(&sendID, &outcome, &class); err != nil {
		t.Fatal(err)
	}

	// Caller retry after a lost response: already_accepted, no duplicate attempt.
	before := e.rpc.dispatchCount()
	res2, err2 := e.send(f, SendInitial, nil)
	assertBlocked(t, e, f.attemptID, res2, err2, ClassAlreadyAccepted, before)
	// Replay is allowed under gates.
	res3, err3 := e.send(f, SendReplay, nil)
	if err3 != nil || res3.Outcome != "accepted" {
		t.Fatalf("replay = %+v %v", res3, err3)
	}
	var attempts int
	if err := e.pool.QueryRow(ctx, `SELECT count(*) FROM tx_attempts WHERE intent_id = $1`, f.intentID).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 {
		t.Fatalf("duplicate attempt created: %d", attempts)
	}

	// A reconcile pass never edits the known send row.
	if _, err := e.store.Reconcile(ctx, f.attemptID, ""); err != nil {
		t.Fatal(err)
	}
	var outcomeAfter, classAfter string
	if err := e.pool.QueryRow(ctx, `SELECT outcome, rpc_class FROM tx_send_attempts WHERE send_id = $1`, sendID).Scan(&outcomeAfter, &classAfter); err != nil {
		t.Fatal(err)
	}
	if outcomeAfter != outcome || classAfter != class {
		t.Fatalf("known send row rewritten: %s/%s -> %s/%s", outcome, class, outcomeAfter, classAfter)
	}
}
