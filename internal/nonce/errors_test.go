package nonce

import "testing"

// TestOutcome pins every machine string exactly (T002): admission outcomes
// from contracts/downstream.md §1, hold causes from
// contracts/observation.md §2, operator attempt outcomes from
// contracts/observation.md §3.4/§4, read outcomes from contracts/read-api.md
// §1–§3. The literals are wire vocabulary — a rename here is a contract
// change.
func TestOutcome(t *testing.T) {
	cases := []struct {
		name string
		got  Outcome
		want string
	}{
		{"allocated", OutcomeAllocated, "allocated"},
		{"replayed", OutcomeReplayed, "replayed"},
		{"allocation_conflict", OutcomeAllocationConflict, "allocation_conflict"},
		{"chain_view_unavailable", OutcomeChainViewUnavailable, "chain_view_unavailable"},
		{"scope_held", OutcomeScopeHeld, "scope_held"},
		{"sender_not_registered", OutcomeSenderNotRegistered, "sender_not_registered"},
		{"sender_disabled", OutcomeSenderDisabled, "sender_disabled"},
		{"authorization_invalid", OutcomeAuthorizationInvalid, "authorization_invalid"},
		{"rebuild_incomplete", OutcomeRebuildIncomplete, "rebuild_incomplete"},
		{"recovery_active", OutcomeRecoveryActive, "recovery_active"},
		{"temporarily_unavailable", OutcomeTemporarilyUnavailable, "temporarily_unavailable"},
		{"operation_conflict", OutcomeOperationConflict, "operation_conflict"},
		{"read bound", ReadBound, "bound"},
		{"read terminal", ReadTerminal, "terminal"},
		{"read not_bound", ReadNotBound, "not_bound"},
		{"read mismatch", ReadMismatch, "mismatch"},
		{"read unavailable", ReadUnavailable, "unavailable"},
		{"read unauthenticated", ReadUnauthenticated, "unauthenticated"},
		{"cause unattributed_consumption", CauseUnattributedConsumption, "unattributed_consumption"},
		{"cause unexplained_gap", CauseUnexplainedGap, "unexplained_gap"},
		{"cause chain_view_divergence", CauseChainViewDivergence, "chain_view_divergence"},
		{"admin applied", AdminApplied, "applied"},
		{"admin nop", AdminNop, "nop"},
		{"admin refused", AdminRefused, "refused"},
	}
	seen := map[string]string{}
	for _, tc := range cases {
		if string(tc.got) != tc.want {
			t.Errorf("%s = %q, want %q", tc.name, tc.got, tc.want)
		}
		if prev, dup := seen[tc.want]; dup {
			t.Errorf("duplicate machine string %q (%s and %s)", tc.want, prev, tc.name)
		}
		seen[tc.want] = tc.name
	}
}

// TestOutcomeError pins the classified error behavior: OutcomeOf/IsOutcome
// only match the classified error and never a bare error.
func TestOutcomeError(t *testing.T) {
	err := Refuse(OutcomeScopeHeld, "two active holds")
	if got := OutcomeOf(err); got != OutcomeScopeHeld {
		t.Fatalf("OutcomeOf = %q, want %q", got, OutcomeScopeHeld)
	}
	if !IsOutcome(err, OutcomeScopeHeld) {
		t.Fatal("IsOutcome(scope_held) = false, want true")
	}
	if IsOutcome(err, OutcomeAllocated) {
		t.Fatal("IsOutcome(allocated) = true for a scope_held error")
	}
	if OutcomeOf(nil) != "" {
		t.Fatal("OutcomeOf(nil) != empty")
	}
	if err.Error() != "scope_held: two active holds" {
		t.Fatalf("Error() = %q", err.Error())
	}
	base := Refuse(OutcomeTemporarilyUnavailable, "retry")
	wrapped := base.Wrap(err)
	if !IsOutcome(wrapped, OutcomeTemporarilyUnavailable) {
		t.Fatal("wrapped error lost its outcome")
	}
	if OutcomeOf(wrapped) != OutcomeTemporarilyUnavailable || wrapped.Reason != "retry" {
		t.Fatalf("wrapped = %+v", wrapped)
	}
}
