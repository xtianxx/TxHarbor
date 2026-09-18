//go:build integration

package txlifecycle

import (
	"bytes"
	"context"
	"testing"
)

// TestV7Replacement is quickstart V7: the reuse branch creates a new attempt
// with unchanged semantics and never reuses old bytes, the forbidden branches
// refuse with zero dispatch, and a fresh grant anchors a new attempt.
func TestV7Replacement(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	chain := e.chainID

	anchor := e.seed()
	anchor.sign()
	anchorRes, err := e.send(anchor, SendInitial, nil)
	if err != nil || anchorRes.Outcome != "accepted" {
		t.Fatalf("anchor send = %+v %v", anchorRes, err)
	}
	anchorRaw := e.rpc.dispatchedRaw()[0]

	replacement := func(suffix string, mutate func(r *PrepareRequest)) *PrepareRequest {
		r := *anchor.request
		r.AttemptID = anchor.attemptID + suffix
		r.SigningRequestID = anchor.signingRequestID + suffix
		r.ReplacementOf = anchor.attemptID
		mutate(&r)
		return &r
	}

	t.Run("no_fee_change_refused", func(t *testing.T) {
		req := replacement("-nofee", func(r *PrepareRequest) {})
		if _, err := e.store.PrepareAttempt(ctx, req); refusalClass(err) != ClassReplacementNoFeeChange {
			t.Fatalf("no-fee-change = %v", err)
		}
	})

	t.Run("mismatch_refused", func(t *testing.T) {
		req := replacement("-mismatch", func(r *PrepareRequest) { r.Amount = "2000" })
		if _, err := e.store.PrepareAttempt(ctx, req); refusalClass(err) != ClassReplacementMismatch {
			t.Fatalf("mismatch = %v", err)
		}
	})

	t.Run("reuse_branch", func(t *testing.T) {
		e.exec(`UPDATE withdrawal_authorization_scopes SET allows_fee_replacement = TRUE WHERE authorization_id = $1`, anchor.authID)
		req := replacement("-reuse", func(r *PrepareRequest) { r.MaxPriorityFeePerGas = "50000000" })
		if _, err := e.store.PrepareAttempt(ctx, req); err != nil {
			t.Fatal(err)
		}
		anchor.signRequest(req)
		res, err := e.store.Send(ctx, &SendRequest{AttemptID: req.AttemptID, Kind: SendInitial, Claim: anchor.claim()})
		if err != nil || res.Outcome != "accepted" {
			t.Fatalf("reuse send = %+v %v", res, err)
		}
		raw := e.rpc.dispatchedRaw()
		if bytes.Equal(raw[len(raw)-1], anchorRaw) {
			t.Fatal("replacement reused the anchor's bytes")
		}
		// New identity, same intent/binding, unchanged semantics.
		created, err := e.store.AttemptByID(ctx, req.AttemptID)
		if err != nil {
			t.Fatal(err)
		}
		if created.ReplacementOf != anchor.attemptID || created.IntentID != anchor.intentID ||
			created.BindingRef != anchor.bindingID || created.Sender != anchor.sender || created.Nonce != anchor.nonce {
			t.Fatalf("replacement identity/semantics changed: %+v", created)
		}
		// Anchor history untouched.
		var anchorSends int
		if err := e.pool.QueryRow(ctx, `SELECT count(*) FROM tx_send_attempts WHERE attempt_id = $1`, anchor.attemptID).Scan(&anchorSends); err != nil {
			t.Fatal(err)
		}
		if anchorSends != 1 {
			t.Fatalf("anchor history changed: %d sends", anchorSends)
		}
	})

	t.Run("reuse_forbidden", func(t *testing.T) {
		e.exec(`UPDATE withdrawal_authorization_scopes SET allows_fee_replacement = FALSE WHERE authorization_id = $1`, anchor.authID)
		req := replacement("-forbidden", func(r *PrepareRequest) { r.MaxPriorityFeePerGas = "40000000" })
		if _, err := e.store.PrepareAttempt(ctx, req); err != nil {
			t.Fatal(err)
		}
		anchor.signRequest(req)
		before := e.rpc.dispatchCount()
		res, err := e.store.Send(ctx, &SendRequest{AttemptID: req.AttemptID, Kind: SendInitial, Claim: anchor.claim()})
		assertBlocked(t, e, req.AttemptID, res, err, ClassScopeReuseForbidden, before)
	})

	t.Run("fee_scope_exceeded", func(t *testing.T) {
		e.exec(`UPDATE withdrawal_authorization_scopes SET allows_fee_replacement = TRUE WHERE authorization_id = $1`, anchor.authID)
		req := replacement("-overfee", func(r *PrepareRequest) { r.MaxFeePerGas = "2000000000" })
		if _, err := e.store.PrepareAttempt(ctx, req); err != nil {
			t.Fatal(err)
		}
		anchor.signRequest(req)
		before := e.rpc.dispatchCount()
		res, err := e.store.Send(ctx, &SendRequest{AttemptID: req.AttemptID, Kind: SendInitial, Claim: anchor.claim()})
		if got := refusalClass(err); got != ClassFeeScopeExceeded {
			t.Fatalf("overfee = %s", got)
		}
		if res.Outcome != "blocked" || e.rpc.dispatchCount() != before {
			t.Fatalf("overfee dispatched: %+v", res)
		}
	})

	t.Run("fresh_grant_branch", func(t *testing.T) {
		freshAuth := anchor.authID + "-fresh"
		e.exec(`INSERT INTO withdrawal_authorizations (authorization_id, caller_id, chain_id, asset, recipient, amount, state)
			VALUES ($1,1,$2,$3,$4,'1000','active')`, freshAuth, chain, fxAsset, fxRecipient)
		e.exec(`INSERT INTO withdrawal_authorization_scopes
			(authorization_id, intent_id, request_id, sender, fee_max_total, fee_max_per_gas, fee_max_priority, allows_fee_replacement, authorization_version, attested_by)
			VALUES ($1,$2,$3,$4,100000000000000,1000000000,100000000,FALSE,1,'test')`,
			freshAuth, anchor.intentID, "req-"+freshAuth, anchor.sender)
		req := replacement("-fresh", func(r *PrepareRequest) {
			r.AuthorizationID = freshAuth
			r.MaxPriorityFeePerGas = "60000000"
		})
		if _, err := e.store.PrepareAttempt(ctx, req); err != nil {
			t.Fatal(err)
		}
		anchor.signRequest(req)
		res, err := e.store.Send(ctx, &SendRequest{AttemptID: req.AttemptID, Kind: SendInitial, Claim: anchor.claim()})
		if err != nil || res.Outcome != "accepted" {
			t.Fatalf("fresh send = %+v %v", res, err)
		}
	})

	// Every attempt traces to one intent/binding.
	var attempts, distinctIntents int
	if err := e.pool.QueryRow(ctx,
		`SELECT count(*), count(DISTINCT intent_id) FROM tx_attempts WHERE binding_ref = $1`, anchor.bindingID).
		Scan(&attempts, &distinctIntents); err != nil {
		t.Fatal(err)
	}
	if distinctIntents != 1 {
		t.Fatalf("attempts span %d intents, want 1", distinctIntents)
	}
}
