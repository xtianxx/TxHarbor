//go:build integration

// grant_attestedby_integration_test.go is the T014 attested_by mismatch half
// against a real PostgreSQL: same operation id from a different principal ⇒
// operation_conflict (zero new writes), same principal ⇒ converge on the
// recorded outcome (contracts/supply-scope.md). It reuses the T008 grantSetup /
// grantSeedCaller harness rather than adding a second one.
package withdrawal

import "testing"

// TestWithdrawalGrantAttestedByMismatchConflict pins the contract line that
// attested_by joins op-input equality: reusing one operation id with a
// DIFFERENT authenticated principal is operation_conflict with zero new writes,
// while the SAME principal converges (retry metadata never reopens an attempt).
func TestWithdrawalGrantAttestedByMismatchConflict(t *testing.T) {
	ctx, pool := grantSetup(t)
	grantSeedCaller(t, ctx, pool, 7201)

	const authID = "auth-t014-attested-by"
	scoped := grantTestOp("01400000000000000000000000000002", authID, 7201, "100")
	scoped.IntentID = "intent-t014"
	scoped.RequestID = "request-t014"
	scoped.Sender = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	scoped.AttestedBy = "principal-a"

	// Given principal-a's first scoped supply.
	out, err := SupplyGrant(ctx, pool, scoped, "operator-a", "first supply")
	if err != nil {
		t.Fatalf("first scoped supply: %v", err)
	}
	if out.Action != grantOutcomeSupplied {
		t.Fatalf("first scoped supply action = %q, want %s", out.Action, grantOutcomeSupplied)
	}

	// When the SAME operation id is retried by the SAME principal with different
	// operator/reason (retry metadata), then it converges on the recorded
	// outcome and writes nothing new.
	out, err = SupplyGrant(ctx, pool, scoped, "operator-b", "retry metadata differs")
	if err != nil {
		t.Fatalf("same-principal retry: %v (want converge)", err)
	}
	if out.Action != grantOutcomeSupplied {
		t.Fatalf("same-principal retry action = %q, want recorded %s", out.Action, grantOutcomeSupplied)
	}
	if n := grantAuditCountByOp(t, ctx, pool, scoped.OperationID); n != 1 {
		t.Fatalf("audit rows after same-principal retry = %d, want 1 (converged)", n)
	}

	// When the SAME operation id is retried by a DIFFERENT principal, then
	// attested_by is the differing op-input and the attempt is
	// operation_conflict with zero new writes.
	other := scoped
	other.AttestedBy = "principal-b"
	conflictOut, conflictErr := SupplyGrant(ctx, pool, other, "operator-b", "different principal")
	if conflictOut != nil {
		t.Fatalf("different-principal outcome = %+v, want nil", conflictOut)
	}
	grantWantCode(t, conflictErr, CodeOperationConflict)
	if n := grantAuditCountByOp(t, ctx, pool, scoped.OperationID); n != 1 {
		t.Fatalf("audit rows after principal mismatch = %d, want 1 (zero new writes)", n)
	}
	if n := grantCount(t, ctx, pool, authID); n != 1 {
		t.Fatalf("grant rows after principal mismatch = %d, want 1", n)
	}
	if state, amount := grantStateAndAmount(t, ctx, pool, authID); state != "active" || amount != "100" {
		t.Fatalf("grant after principal mismatch = (%s, %s), want (active, 100)", state, amount)
	}
}
