//go:build integration

package txlifecycle

import (
	"bytes"
	"context"
	"testing"
)

// TestV3ReplayIdentity is quickstart V3: replay dispatches the persisted bytes
// byte-for-byte, creates no new attempt/signing identity, increments send_seq,
// and still obeys current gates.
func TestV3ReplayIdentity(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	f := e.seed()
	f.sign()

	first, err := e.send(f, SendInitial, nil)
	if err != nil || first.Outcome != "accepted" {
		t.Fatalf("initial send = %+v %v", first, err)
	}
	_, persisted, err := e.store.signingRow(ctx, f.attemptID)
	if err != nil {
		t.Fatal(err)
	}
	raw := e.rpc.dispatchedRaw()
	if len(raw) != 1 || !bytes.Equal(raw[0], persisted) {
		t.Fatal("initial dispatch did not use the persisted bytes")
	}

	replay, err := e.send(f, SendReplay, nil)
	if err != nil || replay.Outcome != "accepted" {
		t.Fatalf("replay = %+v %v", replay, err)
	}
	raw = e.rpc.dispatchedRaw()
	if len(raw) != 2 || !bytes.Equal(raw[0], raw[1]) {
		t.Fatal("replay bytes differ from the persisted bytes")
	}

	var attempts, signings int
	if err := e.pool.QueryRow(ctx, `SELECT count(*) FROM tx_attempts WHERE intent_id = $1`, f.intentID).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 {
		t.Fatalf("replay created a new attempt: %d", attempts)
	}
	if err := e.pool.QueryRow(ctx, `SELECT count(*) FROM tx_attempt_signings WHERE attempt_id = $1`, f.attemptID).Scan(&signings); err != nil {
		t.Fatal(err)
	}
	if signings != 1 {
		t.Fatalf("replay re-signed: %d", signings)
	}
	var seqs int
	if err := e.pool.QueryRow(ctx, `SELECT count(DISTINCT send_seq) FROM tx_send_attempts WHERE attempt_id = $1`, f.attemptID).Scan(&seqs); err != nil {
		t.Fatal(err)
	}
	if seqs != 2 {
		t.Fatalf("send_seq increments = %d, want 2", seqs)
	}

	// Replay with a revoked grant refuses with the observed basis, zero dispatch.
	e.exec(`UPDATE withdrawal_authorizations SET state = 'revoked' WHERE authorization_id = $1`, f.authID)
	before := e.rpc.dispatchCount()
	res, err := e.send(f, SendReplay, nil)
	assertBlocked(t, e, f.attemptID, res, err, ClassAuthorizationRevoked, before)
}
