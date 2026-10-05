//go:build integration

package recovery

import (
	"errors"
	"testing"

	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

func TestSupportedSupersedeClosesAndCreatesFreshBoundInstance(t *testing.T) {
	s := t044NewBoundService(t, GateOptions{})
	gateMapIdentity(t, s.f.ctx, s.f.store, "auth:approver-2", "person-approver-2")
	gateRegister(t, s.f.ctx, s.f.store, s.f.instanceID, "auth:approver-2", "approver")
	scopeHash, err := CapabilityScope(1, CapabilityQuery)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Approve(s.f.ctx, s.f.store, ApprovalRequest{
		InstanceID: s.f.instanceID, Capability: CapabilityQuery, ScopeHash: scopeHash,
		Principal: "deploy:executor", Reason: "executor must not approve", OperationID: gateOperation("supersede-executor-approval"),
	}); !errors.Is(err, ErrApprovalRefused) {
		t.Fatalf("executor approval attempt error = %v, want ErrApprovalRefused", err)
	}
	first, err := Approve(s.f.ctx, s.f.store, ApprovalRequest{
		InstanceID: s.f.instanceID, Capability: CapabilityQuery, ScopeHash: scopeHash,
		Principal: "auth:approver", Reason: "explicit supersede basis", OperationID: gateOperation("supersede-approve-one"),
	})
	if err != nil {
		t.Fatalf("first supersede approval: %v", err)
	}
	second, err := Approve(s.f.ctx, s.f.store, ApprovalRequest{
		InstanceID: s.f.instanceID, Capability: CapabilityQuery, ScopeHash: scopeHash,
		Principal: "auth:approver-2", Reason: "explicit supersede basis", OperationID: gateOperation("supersede-approve-two"),
	})
	if err != nil {
		t.Fatalf("second supersede approval: %v", err)
	}
	guardKey, roleFingerprint := t044BoundTarget(t, s)
	result, err := SupersedeInstance(s.f.ctx, s.f.store, SupersedeInstanceRequest{
		InstanceID: s.f.instanceID, OpenedBy: "deploy:executor", Reason: "supported controlled replacement",
		ApprovalRefs: []string{first.ApprovalID, second.ApprovalID}, OperationID: gateOperation("supersede-positive"),
		TrustedEntryChains: []uint64{1}, TargetGuardKey: guardKey, TargetRoleFingerprint: roleFingerprint,
	})
	if err != nil {
		t.Fatalf("supported SupersedeInstance: %v", err)
	}
	if result.SupersededInstanceID != s.f.instanceID || result.NewInstanceID == "" || result.NewInstanceID == s.f.instanceID {
		t.Fatalf("supersede result = %+v, want old id closed and fresh new id", result)
	}
	var oldState, newState, newSupersedes, newGuard, newRole string
	var newInventory []byte
	if err := s.f.pool.QueryRow(s.f.ctx,
		`SELECT state FROM recovery_instance WHERE instance_id = $1`, s.f.instanceID).Scan(&oldState); err != nil {
		t.Fatalf("read superseded instance: %v", err)
	}
	if err := s.f.pool.QueryRow(s.f.ctx,
		`SELECT state, supersedes_instance_id::text, target_guard_key, target_role_fingerprint, entry_chain_inventory
		 FROM recovery_instance WHERE instance_id = $1`, result.NewInstanceID).
		Scan(&newState, &newSupersedes, &newGuard, &newRole, &newInventory); err != nil {
		t.Fatalf("read replacement instance: %v", err)
	}
	if oldState != "closed" || newState != "open" || newSupersedes != s.f.instanceID || newGuard != guardKey || newRole != roleFingerprint || string(newInventory) != "[1]" {
		t.Fatalf("supersede lifecycle/binding old=%q new=%q supersedes=%q guard=%q role=%q inventory=%s", oldState, newState, newSupersedes, newGuard, newRole, newInventory)
	}
	var oldAudit, newOpenAudit, newApprovals int
	if err := s.f.pool.QueryRow(s.f.ctx,
		`SELECT count(*) FROM recovery_audit WHERE instance_id = $1 AND action = $2 AND result = 'ok'`,
		s.f.instanceID, ActionInstanceSupersede).Scan(&oldAudit); err != nil {
		t.Fatalf("count supersede audit: %v", err)
	}
	if err := s.f.pool.QueryRow(s.f.ctx,
		`SELECT count(*) FROM recovery_audit WHERE instance_id = $1 AND action = $2 AND result = 'ok'`,
		result.NewInstanceID, controlstore.ActionInstanceOpen).Scan(&newOpenAudit); err != nil {
		t.Fatalf("count replacement open audit: %v", err)
	}
	if err := s.f.pool.QueryRow(s.f.ctx,
		`SELECT count(*) FROM recovery_approval WHERE instance_id = $1`, result.NewInstanceID).Scan(&newApprovals); err != nil {
		t.Fatalf("count replacement approvals: %v", err)
	}
	if oldAudit != 1 || newOpenAudit != 1 || newApprovals != 0 {
		t.Fatalf("supersede evidence old_action_audit=%d new_open_audit=%d replacement_approvals=%d, want 1/1/0", oldAudit, newOpenAudit, newApprovals)
	}
}
