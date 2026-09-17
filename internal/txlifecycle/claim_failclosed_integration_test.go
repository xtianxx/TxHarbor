//go:build integration

package txlifecycle

import (
	"context"
	"testing"
)

// TestV10ClaimAbsentFailClosed is T053/V10's pre-011 independent acceptance:
// with the contract-shaped claim fixture table or row absent, every send entry
// point refuses claim_absent with zero dispatch and committed evidence
// (FR-14; G-010-4; R-010-11). Executable before 011 merges.
func TestV10ClaimAbsentFailClosed(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	f := e.seed()
	f.sign()

	// Table absent: the contract-shaped fixture is dropped entirely.
	e.exec(`DROP TABLE execution_claims`)
	for _, kind := range []SendKind{SendInitial, SendReplay} {
		before := e.rpc.dispatchCount()
		res, err := e.send(f, kind, nil)
		assertBlocked(t, e, f.attemptID, res, err, ClassClaimAbsent, before)
	}

	// Row absent: the table exists but carries no claim for the intent.
	e.exec(`CREATE TABLE execution_claims (
		intent_id TEXT PRIMARY KEY, owner_id TEXT NOT NULL,
		lease_version BIGINT NOT NULL, state TEXT NOT NULL DEFAULT 'active',
		acquired_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		expires_at TIMESTAMPTZ NOT NULL, ended_at TIMESTAMPTZ, end_kind TEXT)`)
	for _, kind := range []SendKind{SendInitial, SendReplay} {
		before := e.rpc.dispatchCount()
		res, err := e.send(f, kind, nil)
		assertBlocked(t, e, f.attemptID, res, err, ClassClaimAbsent, before)
	}

	if got := e.rpc.dispatchCount(); got != 0 {
		t.Fatalf("dispatch count = %d, want 0 (claim_absent is fail-closed)", got)
	}
	a, err := e.store.AttemptByID(ctx, f.attemptID)
	if err != nil {
		t.Fatal(err)
	}
	if a.State != "signed" {
		t.Fatalf("attempt state = %s, want signed (zero-dispatch refusals never transition)", a.State)
	}
}
