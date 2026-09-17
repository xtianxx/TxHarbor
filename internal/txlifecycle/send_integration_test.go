//go:build integration

package txlifecycle

import (
	"context"
	"testing"
)

func (e *env) send(f *fixture, kind SendKind, expected *int64) (SendResult, error) {
	return e.store.Send(context.Background(), &SendRequest{
		AttemptID: f.attemptID, Kind: kind, Claim: f.claim(), ExpectedRevision: expected,
	})
}

func assertBlocked(t *testing.T, e *env, attemptID string, res SendResult, err error, want RefusalClass, dispatchBefore int) {
	t.Helper()
	if got := refusalClass(err); got != want {
		t.Fatalf("refusal = %s (%v), want %s", got, err, want)
	}
	if res.Outcome != "blocked" || res.RefusalClass != string(want) {
		t.Fatalf("result = %+v, want blocked %s", res, want)
	}
	if got := e.rpc.dispatchCount(); got != dispatchBefore {
		t.Fatalf("dispatch count moved: %d -> %d (must be zero-dispatch)", dispatchBefore, got)
	}
	var evidence int
	if err := e.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM tx_attempt_events WHERE attempt_id = $1 AND event = 'gate_refused' AND reason_class = $2`,
		attemptID, string(want)).Scan(&evidence); err != nil {
		t.Fatal(err)
	}
	if evidence == 0 {
		t.Fatalf("no gate_refused evidence for %s", want)
	}
}

// TestV6GateRefusals is quickstart V6's refusal matrix: every class refuses
// with zero dispatch and committed evidence.
func TestV6GateRefusals(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	chain := e.chainID

	cases := []struct {
		name    string
		mutate  func(f *fixture)
		cleanup func(f *fixture)
		want    RefusalClass
	}{
		{"claim_absent", func(f *fixture) {
			e.exec(`DELETE FROM execution_claims WHERE intent_id = $1`, f.intentID)
		}, nil, ClassClaimAbsent},
		{"claim_expired", func(f *fixture) {
			e.exec(`UPDATE execution_claims SET acquired_at = now() - interval '2 hours', expires_at = now() - interval '1 hour' WHERE intent_id = $1`, f.intentID)
		}, nil, ClassClaimExpired},
		{"claim_revoked", func(f *fixture) {
			e.exec(`UPDATE execution_claims SET state = 'revoked', ended_at = now(), end_kind = 'revoked' WHERE intent_id = $1`, f.intentID)
		}, nil, ClassClaimRevoked},
		{"claim_version_mismatch", func(f *fixture) {
			e.exec(`UPDATE execution_claims SET lease_version = 99 WHERE intent_id = $1`, f.intentID)
		}, nil, ClassClaimVersionMismatch},
		{"pause_present", func(f *fixture) {
			e.exec(`INSERT INTO indexer_pause (chain_id, height, expected_hash, actual_hash, kind) VALUES ($1,100,$2,$3,'hash_mismatch')`,
				chain, blockHashHex(100), blockHashHex(101))
		}, func(f *fixture) {
			e.exec(`DELETE FROM indexer_pause WHERE chain_id = $1`, chain)
		}, ClassPausePresent},
		{"grant_revoked", func(f *fixture) {
			e.exec(`UPDATE withdrawal_authorizations SET state = 'revoked' WHERE authorization_id = $1`, f.authID)
		}, nil, ClassAuthorizationRevoked},
		{"grant_expired", func(f *fixture) {
			e.exec(`UPDATE withdrawal_authorizations SET expires_at = now() - interval '1 second' WHERE authorization_id = $1`, f.authID)
		}, nil, ClassAuthorizationExpired},
		{"grant_inactive", func(f *fixture) {
			e.exec(`UPDATE withdrawal_authorizations SET state = 'expired' WHERE authorization_id = $1`, f.authID)
		}, nil, ClassAuthorizationInactive},
		{"grant_mismatch", func(f *fixture) {
			e.exec(`UPDATE withdrawal_authorizations SET amount = '9999' WHERE authorization_id = $1`, f.authID)
		}, nil, ClassAuthorizationMismatch},
		{"scopeless", func(f *fixture) {
			e.exec(`DELETE FROM withdrawal_authorization_scopes WHERE authorization_id = $1`, f.authID)
		}, nil, ClassAuthorizationUnverifiable},
		{"binding_terminal", func(f *fixture) {
			e.exec(`UPDATE nonce_bindings SET state = 'consumed', consumed_at = now() WHERE binding_id = $1`, f.bindingID)
		}, nil, ClassBindingTerminal},
		{"registry_disabled", func(f *fixture) {
			e.exec(`UPDATE nonce_wallet_registry SET state = 'disabled' WHERE chain_id = $1 AND sender = $2`, chain, f.sender)
		}, nil, ClassBindingPaused},
		{"active_hold", func(f *fixture) {
			e.exec(`INSERT INTO nonce_scope_holds (hold_id, chain_id, sender, cause, status, evidence_observation_id)
				VALUES ($1,$2,$3,'chain_view_divergence','active','obs-hold')`, "hold-"+f.intentID, chain, f.sender)
		}, nil, ClassBindingPaused},
		{"attempt_not_sendable", func(f *fixture) {
			e.exec(`UPDATE tx_attempts SET state = 'effective', effective_at = now(), revision_seq = revision_seq + 1 WHERE attempt_id = $1`, f.attemptID)
		}, nil, ClassAttemptNotSendable},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := e.seed()
			f.sign()
			c.mutate(f)
			before := e.rpc.dispatchCount()
			res, err := e.send(f, SendInitial, nil)
			assertBlocked(t, e, f.attemptID, res, err, c.want, before)
			if c.cleanup != nil {
				c.cleanup(f)
			}
		})
	}
	_ = ctx
}

// TestV6BindingAbsent covers the 008 binding-absent refusal. The 010 FK to
// nonce_bindings normally prevents this state, so the scratch DB drops the
// constraint to exercise the adapter's fail-closed path.
func TestV6BindingAbsent(t *testing.T) {
	e := newEnv(t)
	f := e.seed()
	f.sign()
	e.exec(`ALTER TABLE tx_attempts DROP CONSTRAINT tx_attempts_binding_fkey`)
	e.exec(`DELETE FROM nonce_bindings WHERE binding_id = $1`, f.bindingID)
	before := e.rpc.dispatchCount()
	res, err := e.send(f, SendInitial, nil)
	assertBlocked(t, e, f.attemptID, res, err, ClassBindingAbsent, before)
}

// TestV6RecoveryGateRefusals covers the 006 active-recovery and stale-version
// refusals in their own env (the recovery row is chain-scoped and persists).
func TestV6RecoveryGateRefusals(t *testing.T) {
	e := newEnv(t)
	chain := e.chainID

	t.Run("recovery_active", func(t *testing.T) {
		f := e.seed()
		f.sign()
		e.exec(`INSERT INTO reorg_policy_history (chain_id, policy_seq, max_depth) VALUES ($1,1,5)`, chain)
		e.exec(`INSERT INTO reorg_recovery (chain_id, recovery_id, phase, policy_seq, max_depth, bound_old_number, bound_old_hash, recovery_seq)
			VALUES ($1,'rec-active','detected',1,5,100,$2,1)`, chain, blockHashHex(100))
		before := e.rpc.dispatchCount()
		res, err := e.send(f, SendInitial, nil)
		assertBlocked(t, e, f.attemptID, res, err, ClassRecoveryActive, before)
	})

	t.Run("recovery_version_changed", func(t *testing.T) {
		e.exec(`DELETE FROM reorg_recovery WHERE chain_id = $1`, chain)
		f := e.seed()
		f.sign()
		e.exec(`INSERT INTO reorg_recovery_events (chain_id, recovery_id, recovery_seq, event_seq, event)
			VALUES ($1,'rec-stale',7,1,'established')`, chain)
		before := e.rpc.dispatchCount()
		res, err := e.send(f, SendInitial, nil)
		assertBlocked(t, e, f.attemptID, res, err, ClassRecoveryVersionChanged, before)
	})
}

// TestV6RaceOrdering covers the ordering/fencing cases: a stale
// ExpectedRevision refuses; a second initial after acceptance refuses
// already_accepted; concurrent send/replay serialize with unique send_seq.
func TestV6RaceOrdering(t *testing.T) {
	e := newEnv(t)

	t.Run("send_stale", func(t *testing.T) {
		f := e.seed()
		f.sign()
		stale := int64(999)
		before := e.rpc.dispatchCount()
		res, err := e.send(f, SendInitial, &stale)
		assertBlocked(t, e, f.attemptID, res, err, ClassSendStale, before)
	})

	t.Run("already_accepted", func(t *testing.T) {
		f := e.seed()
		f.sign()
		res, err := e.send(f, SendInitial, nil)
		if err != nil || res.Outcome != "accepted" {
			t.Fatalf("first send: %+v %v", res, err)
		}
		before := e.rpc.dispatchCount()
		res2, err2 := e.send(f, SendInitial, nil)
		assertBlocked(t, e, f.attemptID, res2, err2, ClassAlreadyAccepted, before)
	})

	t.Run("concurrent_send_replay", func(t *testing.T) {
		f := e.seed()
		f.sign()
		type outcome struct {
			res SendResult
			err error
		}
		results := make(chan outcome, 2)
		go func() { r, err := e.send(f, SendInitial, nil); results <- outcome{r, err} }()
		go func() { r, err := e.send(f, SendReplay, nil); results <- outcome{r, err} }()
		for i := 0; i < 2; i++ {
			<-results
		}
		var accepted, seqs int
		if err := e.pool.QueryRow(context.Background(),
			`SELECT count(*) FILTER (WHERE outcome = 'accepted'), count(DISTINCT send_seq) FROM tx_send_attempts WHERE attempt_id = $1`,
			f.attemptID).Scan(&accepted, &seqs); err != nil {
			t.Fatal(err)
		}
		if accepted == 0 {
			t.Fatal("no accepted dispatch recorded")
		}
		var total int
		if err := e.pool.QueryRow(context.Background(),
			`SELECT count(*) FROM tx_send_attempts WHERE attempt_id = $1`, f.attemptID).Scan(&total); err != nil {
			t.Fatal(err)
		}
		if seqs != total {
			t.Fatalf("send_seq not unique: %d rows, %d distinct seqs", total, seqs)
		}
	})
}
