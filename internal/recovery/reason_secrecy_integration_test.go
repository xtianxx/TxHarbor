//go:build integration

package recovery

import (
	"strings"
	"testing"
)

func TestApprovalAndReleaseRejectLongPEMReasonsBeforePersistence(t *testing.T) {
	s := t044NewService(t, GateOptions{})
	const canary = "app-reason-private-key-canary-581"
	reasons := []string{
		strings.Repeat("ordinary-reason-", 50) + "-----BEGIN PRIVATE KEY-----" + canary + strings.Repeat("private-material", 50) + "-----END PRIVATE KEY-----",
		strings.Repeat("ordinary-reason-", 50) + "-----BEGIN RSA PRIVATE KEY-----" + canary + strings.Repeat("private-material", 50),
	}
	beforeApprovals := s.count(t, "SELECT count(*) FROM recovery_approval")
	beforeReleases := s.count(t, "SELECT count(*) FROM recovery_release")
	beforeAudits := s.count(t, "SELECT count(*) FROM recovery_audit")
	for _, reason := range reasons {
		_, err := Approve(s.f.ctx, s.f.store, ApprovalRequest{
			InstanceID: s.f.instanceID, Capability: CapabilityQuery, ScopeHash: gateScopeFor(CapabilityQuery),
			Principal: "auth:approver", Reason: reason, OperationID: gateOperation("long-pem-approval"),
		})
		assertReasonCredentialError(t, "approval", err, canary)
		_, err = Release(s.f.ctx, s.f.store, s.gate, ReleaseRequest{
			InstanceID: s.f.instanceID, Capability: CapabilityQuery, ScopeHash: gateScopeFor(CapabilityQuery),
			Principal: "deploy:executor", Reason: reason, OperationID: gateOperation("long-pem-release"),
		})
		assertReasonCredentialError(t, "release", err, canary)
	}
	if got := s.count(t, "SELECT count(*) FROM recovery_approval"); got != beforeApprovals {
		t.Fatalf("rejected approval reasons wrote rows: %d -> %d", beforeApprovals, got)
	}
	if got := s.count(t, "SELECT count(*) FROM recovery_release"); got != beforeReleases {
		t.Fatalf("rejected release reasons wrote rows: %d -> %d", beforeReleases, got)
	}
	if got := s.count(t, "SELECT count(*) FROM recovery_audit"); got != beforeAudits {
		t.Fatalf("rejected reasons wrote audit rows: %d -> %d", beforeAudits, got)
	}
	if got := s.count(t, `
SELECT
 (SELECT count(*) FROM recovery_instance WHERE reason LIKE '%' || $1 || '%') +
 (SELECT count(*) FROM recovery_approval WHERE reason LIKE '%' || $1 || '%') +
 (SELECT count(*) FROM recovery_release WHERE reason LIKE '%' || $1 || '%') +
 (SELECT count(*) FROM recovery_audit WHERE coalesce(target::text, '') LIKE '%' || $1 || '%' OR coalesce(detail::text, '') LIKE '%' || $1 || '%')`, canary); got != 0 {
		t.Fatalf("rejected reason canary persisted in %d rows", got)
	}
}

func assertReasonCredentialError(t *testing.T, operation string, err error, canary string) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), "reason") || strings.Contains(err.Error(), canary) {
		t.Fatalf("%s should reject credential reason without echoing it, got %v", operation, err)
	}
}
