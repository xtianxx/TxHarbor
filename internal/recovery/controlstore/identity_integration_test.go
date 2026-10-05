//go:build integration

// identity_integration_test.go runs the T009 identity store against a real
// PostgreSQL 18.6 (testcontainers): mapping set/resolve/revoke-keeps-row, the
// same-person-many-accounts identification, participant registration resolved
// through the mapping (missing/revoked -> refusal with zero rows), operation_id
// idempotency and conflict zero-writes, the registration PK guard, and the F19
// mapping-change invalidation of existing approvals with the recorded
// approval_identity_unverified refusal path (open instances only).
//
// Docker provider missing: the package fails under CI=true or
// TXHARBOR_REQUIRE_DOCKER=1 and reports NOT RUN locally (exit 0).
package controlstore

import (
	"errors"
	"testing"
)

func TestIdentityMappingLifecycleAndSamePersonAccounts(t *testing.T) {
	ctx, pool, store, _ := migratedControlStore(t)

	change, err := store.SetIdentityMapping(ctx, SetIdentityMappingRequest{
		Principal: "deploy:alice", PersonID: "person-a", Source: MappingSourceDeployConfig,
		RecordedBy: "deploy:manager", Operator: "ops", Reason: "bootstrap", OperationID: "map-1",
	})
	if err != nil {
		t.Fatalf("set identity mapping: %v", err)
	}
	if !change.Created || change.Changed || change.Recorded || !change.Mapping.Active {
		t.Fatalf("first set must create an active mapping: %+v", change)
	}
	if change.Mapping.PersonID != "person-a" || change.Mapping.RecordedBy != "deploy:manager" || change.Mapping.Source != MappingSourceDeployConfig {
		t.Fatalf("unexpected mapping row: %+v", change.Mapping)
	}
	if n := countRows(t, ctx, pool,
		"SELECT count(*) FROM recovery_audit WHERE action = $1 AND result = 'ok'", ActionIdentityMapSet); n != 1 {
		t.Fatalf("exactly one ok identity_map_set audit row expected, got %d", n)
	}

	// Same person, second account: both principals resolve to the same
	// person_id (approval-matrix §2: the same person under another account is
	// not a second approver).
	if _, err := store.SetIdentityMapping(ctx, SetIdentityMappingRequest{
		Principal: "sso:alice", PersonID: "person-a", RecordedBy: "deploy:manager",
		Reason: "second account", OperationID: "map-2",
	}); err != nil {
		t.Fatalf("set second-account mapping: %v", err)
	}
	first, foundFirst, err := store.ActivePersonID(ctx, "deploy:alice")
	if err != nil || !foundFirst {
		t.Fatalf("deploy:alice must resolve: found=%t err=%v", foundFirst, err)
	}
	second, foundSecond, err := store.ActivePersonID(ctx, "sso:alice")
	if err != nil || !foundSecond {
		t.Fatalf("sso:alice must resolve: found=%t err=%v", foundSecond, err)
	}
	if first != "person-a" || second != "person-a" {
		t.Fatalf("both accounts must resolve to the same person, got %q and %q", first, second)
	}

	// operation_id replay: same input reads back with zero writes.
	replay, err := store.SetIdentityMapping(ctx, SetIdentityMappingRequest{
		Principal: "deploy:alice", PersonID: "person-a", Source: MappingSourceDeployConfig,
		RecordedBy: "deploy:manager", Operator: "ops", Reason: "bootstrap", OperationID: "map-1",
	})
	if err != nil || !replay.Recorded {
		t.Fatalf("same input must replay the recorded operation: %+v err=%v", replay, err)
	}
	if n := countRows(t, ctx, pool, "SELECT count(*) FROM recovery_identity"); n != 2 {
		t.Fatalf("replay must write no mapping rows, got %d", n)
	}
	if n := countRows(t, ctx, pool,
		"SELECT count(*) FROM recovery_audit WHERE action = $1", ActionIdentityMapSet); n != 2 {
		t.Fatalf("replay must write no audit rows, got %d identity_map_set rows", n)
	}

	// A different input on the same operation_id conflicts with zero writes.
	conflict, err := store.SetIdentityMapping(ctx, SetIdentityMappingRequest{
		Principal: "deploy:alice", PersonID: "person-b", RecordedBy: "deploy:manager",
		Reason: "bootstrap", OperationID: "map-1",
	})
	if !errors.Is(err, ErrOperationConflict) {
		t.Fatalf("different input on the same operation_id must conflict, got %v", err)
	}
	if conflict.Recorded {
		t.Fatal("a conflict must not report a recorded outcome")
	}
	if n := countRows(t, ctx, pool, "SELECT count(*) FROM recovery_identity"); n != 2 {
		t.Fatalf("conflict must write zero rows, got %d", n)
	}

	// Revocation keeps the row (active=FALSE) so the mapping can never be
	// silently reused as proof.
	revoked, err := store.RevokeIdentityMapping(ctx, RevokeIdentityMappingRequest{
		Principal: "deploy:alice", RecordedBy: "deploy:manager",
		Operator: "ops", Reason: "offboard", OperationID: "map-3",
	})
	if err != nil || !revoked.Revoked || !revoked.Changed || revoked.Mapping.Active {
		t.Fatalf("revoke must deactivate the mapping and keep its facts: %+v err=%v", revoked, err)
	}
	if n := countRows(t, ctx, pool,
		"SELECT count(*) FROM recovery_identity WHERE principal = 'deploy:alice'"); n != 1 {
		t.Fatalf("revoked mapping row must be kept, got %d rows", n)
	}
	if n := countRows(t, ctx, pool,
		"SELECT count(*) FROM recovery_identity WHERE principal = 'deploy:alice' AND active"); n != 0 {
		t.Fatalf("revoked mapping must not stay active, got %d active rows", n)
	}
	if _, found, err := store.ActivePersonID(ctx, "deploy:alice"); err != nil || found {
		t.Fatalf("a revoked mapping must not resolve a person: found=%t err=%v", found, err)
	}
	// Revoking an already-revoked mapping with a fresh operation is a no-op.
	again, err := store.RevokeIdentityMapping(ctx, RevokeIdentityMappingRequest{
		Principal: "deploy:alice", RecordedBy: "deploy:manager",
		Reason: "repeat", OperationID: "map-4",
	})
	if err != nil || !again.AlreadyRevoked || again.Revoked || again.Recorded {
		t.Fatalf("repeated revoke must be a no-op: %+v err=%v", again, err)
	}
	// Revoking an unknown principal refuses.
	if _, err := store.RevokeIdentityMapping(ctx, RevokeIdentityMappingRequest{
		Principal: "deploy:nobody", RecordedBy: "deploy:manager", Reason: "x", OperationID: "map-5",
	}); !errors.Is(err, ErrIdentityMappingMissing) {
		t.Fatalf("revoking an unknown principal must refuse with ErrIdentityMappingMissing, got %v", err)
	}

	// Reactivation with the same person is a change (conservative epoch rule).
	reactivated, err := store.SetIdentityMapping(ctx, SetIdentityMappingRequest{
		Principal: "deploy:alice", PersonID: "person-a", RecordedBy: "deploy:manager",
		Reason: "rehire", OperationID: "map-6",
	})
	if err != nil || !reactivated.Changed || reactivated.Created {
		t.Fatalf("reactivation must count as a change: %+v err=%v", reactivated, err)
	}

	// identity-map-show filters: person filter sees the same-person accounts.
	mappings, err := store.IdentityMappings(ctx, "", "person-a")
	if err != nil || len(mappings) != 2 {
		t.Fatalf("person filter must find both accounts of person-a: %d err=%v", len(mappings), err)
	}
}

func TestRegisterParticipantMappingContract(t *testing.T) {
	ctx, pool, store, _ := migratedControlStore(t)
	instanceID := openTestInstance(t, ctx, store)

	base := RegisterParticipantRequest{
		InstanceID: instanceID, Role: ParticipantRoleApprover,
		BindingSource: BindingSourceDeployConfig, ProofRef: "ticket-1",
		Actor: "deploy:manager", Operator: "ops", Reason: "onboard", OperationID: "reg-1",
	}

	// Missing mapping: a person cannot be proven, registration refuses with
	// zero participant rows and one refused audit row.
	missing := base
	missing.Principal = "deploy:carol"
	if _, err := store.RegisterParticipant(ctx, missing); !errors.Is(err, ErrIdentityMappingMissing) {
		t.Fatalf("registration without a mapping must refuse, got %v", err)
	}
	if n := countRows(t, ctx, pool, "SELECT count(*) FROM recovery_participant"); n != 0 {
		t.Fatalf("refused registration must write zero participant rows, got %d", n)
	}
	if n := countRows(t, ctx, pool,
		"SELECT count(*) FROM recovery_audit WHERE action = $1 AND result = 'refused'", ActionParticipantRegister); n != 1 {
		t.Fatalf("refused registration must be audited once, got %d", n)
	}

	// Revoked mapping is equally unprovable.
	if _, err := store.SetIdentityMapping(ctx, SetIdentityMappingRequest{
		Principal: "deploy:dave", PersonID: "person-d", RecordedBy: "deploy:manager",
		Reason: "bootstrap", OperationID: "map-d1",
	}); err != nil {
		t.Fatalf("set dave mapping: %v", err)
	}
	if _, err := store.RevokeIdentityMapping(ctx, RevokeIdentityMappingRequest{
		Principal: "deploy:dave", RecordedBy: "deploy:manager", Reason: "offboard", OperationID: "map-d2",
	}); err != nil {
		t.Fatalf("revoke dave mapping: %v", err)
	}
	revoked := base
	revoked.Principal, revoked.OperationID = "deploy:dave", "reg-2"
	if _, err := store.RegisterParticipant(ctx, revoked); !errors.Is(err, ErrIdentityMappingRevoked) {
		t.Fatalf("registration over a revoked mapping must refuse, got %v", err)
	}

	// A mapped principal registers with the mapping's person_id.
	if _, err := store.SetIdentityMapping(ctx, SetIdentityMappingRequest{
		Principal: "deploy:bob", PersonID: "person-b", RecordedBy: "deploy:manager",
		Reason: "bootstrap", OperationID: "map-b1",
	}); err != nil {
		t.Fatalf("set bob mapping: %v", err)
	}
	register := base
	register.Principal, register.OperationID = "deploy:bob", "reg-3"
	result, err := store.RegisterParticipant(ctx, register)
	if err != nil {
		t.Fatalf("register participant: %v", err)
	}
	if result.Recorded || result.Binding.PersonID != "person-b" ||
		result.Binding.InstanceID != instanceID || result.Binding.Role != ParticipantRoleApprover ||
		result.Binding.BindingSource != BindingSourceDeployConfig {
		t.Fatalf("registration must resolve the mapping person: %+v", result)
	}

	// Same operation_id and same input replays with zero writes.
	replay, err := store.RegisterParticipant(ctx, register)
	if err != nil || !replay.Recorded || replay.Binding.PersonID != "person-b" {
		t.Fatalf("registration replay must read back: %+v err=%v", replay, err)
	}
	if n := countRows(t, ctx, pool, "SELECT count(*) FROM recovery_participant"); n != 1 {
		t.Fatalf("replay must write no participant rows, got %d", n)
	}
	if n := countRows(t, ctx, pool,
		"SELECT count(*) FROM recovery_audit WHERE action = $1 AND result = 'ok'", ActionParticipantRegister); n != 1 {
		t.Fatalf("replay must write no audit rows, got %d", n)
	}

	// Same operation_id with a different input conflicts, zero writes.
	conflict := register
	conflict.Role = ParticipantRoleVerifier
	if _, err := store.RegisterParticipant(ctx, conflict); !errors.Is(err, ErrOperationConflict) {
		t.Fatalf("different input on the same operation_id must conflict, got %v", err)
	}
	// The PK guard refuses a re-registration under a fresh operation_id.
	duplicate := register
	duplicate.OperationID = "reg-4"
	if _, err := store.RegisterParticipant(ctx, duplicate); !errors.Is(err, ErrParticipantAlreadyRegistered) {
		t.Fatalf("duplicate binding must refuse, got %v", err)
	}
	if n := countRows(t, ctx, pool, "SELECT count(*) FROM recovery_participant"); n != 1 {
		t.Fatalf("refused duplicates must write zero rows, got %d", n)
	}

	// Bindings are visible per instance.
	bindings, err := store.ParticipantBindings(ctx, instanceID)
	if err != nil || len(bindings) != 1 || bindings[0].Principal != "deploy:bob" {
		t.Fatalf("participant bindings view: %v err=%v", bindings, err)
	}

	// A closed instance accepts no new participant bindings.
	if _, err := pool.Exec(ctx, `
UPDATE recovery_instance SET state = 'closed', closed_by = 'deploy:manager', closed_at = now()
WHERE instance_id = $1`, instanceID); err != nil {
		t.Fatalf("close instance: %v", err)
	}
	if _, err := store.SetIdentityMapping(ctx, SetIdentityMappingRequest{
		Principal: "deploy:erin", PersonID: "person-e", RecordedBy: "deploy:manager",
		Reason: "bootstrap", OperationID: "map-e1",
	}); err != nil {
		t.Fatalf("set erin mapping: %v", err)
	}
	closed := base
	closed.Principal, closed.OperationID = "deploy:erin", "reg-5"
	if _, err := store.RegisterParticipant(ctx, closed); !errors.Is(err, ErrInstanceNotOpen) {
		t.Fatalf("registration on a closed instance must refuse, got %v", err)
	}
}

func TestRefusedRegistrationKeepsOperationIDFree(t *testing.T) {
	ctx, pool, store, _ := migratedControlStore(t)
	instanceID := openTestInstance(t, ctx, store)

	req := RegisterParticipantRequest{
		InstanceID: instanceID, Principal: "deploy:carol", Role: ParticipantRoleVerifier,
		BindingSource: BindingSourceDeployConfig, Actor: "deploy:manager",
		Operator: "ops", Reason: "onboard", OperationID: "reg-retry",
	}
	if _, err := store.RegisterParticipant(ctx, req); !errors.Is(err, ErrIdentityMappingMissing) {
		t.Fatalf("registration without a mapping must refuse, got %v", err)
	}
	if n := countRows(t, ctx, pool,
		"SELECT count(*) FROM recovery_audit WHERE action = $1 AND result = 'refused'", ActionParticipantRegister); n != 1 {
		t.Fatalf("the refusal must be audited, got %d rows", n)
	}
	// A refusal applies no effect, so it does not claim the operation_id: a
	// corrected retry under the same id may still apply.
	if n := countRows(t, ctx, pool, "SELECT count(*) FROM recovery_audit WHERE operation_id = 'reg-retry'"); n != 0 {
		t.Fatalf("a refused attempt must not claim the operation_id column, got %d rows", n)
	}

	if _, err := store.SetIdentityMapping(ctx, SetIdentityMappingRequest{
		Principal: "deploy:carol", PersonID: "person-c", RecordedBy: "deploy:manager",
		Reason: "bootstrap", OperationID: "map-c1",
	}); err != nil {
		t.Fatalf("set carol mapping: %v", err)
	}
	result, err := store.RegisterParticipant(ctx, req)
	if err != nil || result.Recorded || result.Binding.PersonID != "person-c" {
		t.Fatalf("the corrected retry must apply: %+v err=%v", result, err)
	}
	replay, err := store.RegisterParticipant(ctx, req)
	if err != nil || !replay.Recorded {
		t.Fatalf("after the retry applied, the same input must replay: %+v err=%v", replay, err)
	}
}

func TestIdentityMappingChangeInvalidatesApprovals(t *testing.T) {
	ctx, pool, store, _ := migratedControlStore(t)
	instanceID := openTestInstance(t, ctx, store)

	if _, err := store.SetIdentityMapping(ctx, SetIdentityMappingRequest{
		Principal: "deploy:alice", PersonID: "person-a", RecordedBy: "deploy:manager",
		Reason: "bootstrap", OperationID: "map-a",
	}); err != nil {
		t.Fatalf("set alice mapping: %v", err)
	}

	first, err := store.AppendApprovalDecision(ctx, ApprovalDecisionRequest{
		InstanceID: instanceID, Capability: "existing_withdrawal_recovery", ScopeHash: "scope-x",
		Decision: "approve", ApprovalClassSnapshot: "dual_non_executor",
		Principal: "deploy:alice", PersonID: "person-a",
		EvidenceGeneration: 0, EvidenceHash: EmptyEvidenceHash, OperationID: "appr-1",
	})
	if err != nil {
		t.Fatalf("append approval: %v", err)
	}
	if invalidated, err := store.InvalidatedApprovals(ctx, "deploy:alice"); err != nil || len(invalidated) != 0 {
		t.Fatalf("a consistent approval must not be invalidated: %v err=%v", invalidated, err)
	}

	// Person change: the existing approval is invalid immediately and the
	// refusal path (approval_identity_unverified) is recorded in the same
	// atomic change.
	change, err := store.SetIdentityMapping(ctx, SetIdentityMappingRequest{
		Principal: "deploy:alice", PersonID: "person-b", RecordedBy: "deploy:manager",
		Reason: "identity corrected", OperationID: "map-b",
	})
	if err != nil {
		t.Fatalf("change mapping: %v", err)
	}
	if len(change.InvalidatedApprovals) != 1 {
		t.Fatalf("the mapping change must invalidate the existing approval: %+v", change.InvalidatedApprovals)
	}
	invalidated := change.InvalidatedApprovals[0]
	if invalidated.ApprovalID != first.DecisionID || invalidated.MappingState != "changed" ||
		invalidated.RecordedPersonID != "person-a" || invalidated.CurrentPersonID != "person-b" {
		t.Fatalf("unexpected invalidated approval: %+v", invalidated)
	}
	if n := countRows(t, ctx, pool, `
SELECT count(*) FROM recovery_audit
WHERE action = $1 AND result = 'refused' AND refusal_class = $2 AND target->>'approval_id' = $3`,
		ActionApprovalIdentityInvalidated, RefusalApprovalIdentityUnverified, first.DecisionID); n != 1 {
		t.Fatalf("exactly one approval_identity_unverified refusal audit row expected, got %d", n)
	}
	if again, err := store.InvalidatedApprovals(ctx, "deploy:alice"); err != nil || len(again) != 1 || again[0].ApprovalID != first.DecisionID {
		t.Fatalf("the invalidation must be visible through the read helper: %v err=%v", again, err)
	}

	// Re-approval against the current mapping adds a consistent row; the old
	// mismatched row stays invalid (append-only, no silent revival).
	second, err := store.AppendApprovalDecision(ctx, ApprovalDecisionRequest{
		InstanceID: instanceID, Capability: "existing_withdrawal_recovery", ScopeHash: "scope-x",
		Decision: "approve", ApprovalClassSnapshot: "dual_non_executor",
		Principal: "deploy:alice", PersonID: "person-b",
		EvidenceGeneration: 0, EvidenceHash: EmptyEvidenceHash, OperationID: "appr-2",
	})
	if err != nil {
		t.Fatalf("re-approval: %v", err)
	}
	afterReapproval, err := store.InvalidatedApprovals(ctx, "deploy:alice")
	if err != nil || len(afterReapproval) != 1 || afterReapproval[0].ApprovalID != first.DecisionID {
		t.Fatalf("only the stale approval may stay invalid: %v err=%v", afterReapproval, err)
	}
	if afterReapproval[0].ApprovalID == second.DecisionID {
		t.Fatal("the re-approval must not be invalidated by its own mapping")
	}

	// Revocation invalidates every approve row of the principal (all epochs).
	revoked, err := store.RevokeIdentityMapping(ctx, RevokeIdentityMappingRequest{
		Principal: "deploy:alice", RecordedBy: "deploy:manager",
		Reason: "offboard", OperationID: "map-c",
	})
	if err != nil {
		t.Fatalf("revoke mapping: %v", err)
	}
	if len(revoked.InvalidatedApprovals) != 2 {
		t.Fatalf("revocation must invalidate every approve row of the principal, got %+v", revoked.InvalidatedApprovals)
	}
	for _, item := range revoked.InvalidatedApprovals {
		if item.MappingState != "revoked" {
			t.Fatalf("revoked mapping must report mapping_state=revoked, got %+v", item)
		}
	}
	if n := countRows(t, ctx, pool,
		"SELECT count(*) FROM recovery_audit WHERE action = $1 AND result = 'refused'", ActionApprovalIdentityInvalidated); n != 3 {
		t.Fatalf("expected 3 invalidation refusal audit rows (1 change + 2 revoke), got %d", n)
	}
}

func TestIdentityMappingChangeOnlyInvalidatesOpenInstanceApprovals(t *testing.T) {
	ctx, pool, store, _ := migratedControlStore(t)
	firstInstance := openTestInstance(t, ctx, store)

	if _, err := store.SetIdentityMapping(ctx, SetIdentityMappingRequest{
		Principal: "deploy:alice", PersonID: "person-a", RecordedBy: "deploy:manager",
		Reason: "bootstrap", OperationID: "map-a",
	}); err != nil {
		t.Fatalf("set alice mapping: %v", err)
	}
	if _, err := store.AppendApprovalDecision(ctx, ApprovalDecisionRequest{
		InstanceID: firstInstance, Capability: "query", ScopeHash: "scope-1",
		Decision: "approve", ApprovalClassSnapshot: "single_non_executor",
		Principal: "deploy:alice", PersonID: "person-a",
		EvidenceGeneration: 0, EvidenceHash: EmptyEvidenceHash, OperationID: "appr-closed",
	}); err != nil {
		t.Fatalf("append approval on the first instance: %v", err)
	}
	if _, err := pool.Exec(ctx, `
UPDATE recovery_instance SET state = 'closed', closed_by = 'deploy:manager', closed_at = now()
WHERE instance_id = $1`, firstInstance); err != nil {
		t.Fatalf("close first instance: %v", err)
	}

	// No open instance: a mapping change invalidates nothing (there is no
	// active recovery under evaluation).
	change, err := store.SetIdentityMapping(ctx, SetIdentityMappingRequest{
		Principal: "deploy:alice", PersonID: "person-b", RecordedBy: "deploy:manager",
		Reason: "no open instance", OperationID: "map-b",
	})
	if err != nil || len(change.InvalidatedApprovals) != 0 {
		t.Fatalf("a closed instance's approvals must not be reported invalidated: %+v err=%v", change, err)
	}

	secondInstance := openTestInstance(t, ctx, store)
	second, err := store.AppendApprovalDecision(ctx, ApprovalDecisionRequest{
		InstanceID: secondInstance, Capability: "query", ScopeHash: "scope-1",
		Decision: "approve", ApprovalClassSnapshot: "single_non_executor",
		Principal: "deploy:alice", PersonID: "person-b",
		EvidenceGeneration: 0, EvidenceHash: EmptyEvidenceHash, OperationID: "appr-open",
	})
	if err != nil {
		t.Fatalf("append approval on the second instance: %v", err)
	}
	change, err = store.SetIdentityMapping(ctx, SetIdentityMappingRequest{
		Principal: "deploy:alice", PersonID: "person-c", RecordedBy: "deploy:manager",
		Reason: "identity corrected", OperationID: "map-c",
	})
	if err != nil || len(change.InvalidatedApprovals) != 1 || change.InvalidatedApprovals[0].ApprovalID != second.DecisionID {
		t.Fatalf("only the open instance's approval may be invalidated: %+v err=%v", change, err)
	}
}
