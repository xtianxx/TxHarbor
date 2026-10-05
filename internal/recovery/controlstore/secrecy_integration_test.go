//go:build integration

package controlstore

import (
	"strings"
	"testing"
)

func TestAuditAndIdentityCredentialRejectionsWriteNoRows(t *testing.T) {
	ctx, pool, store, _ := migratedControlStore(t)
	instanceID := openTestInstance(t, ctx, store)
	const canary = "controlstore-secret-canary-927"

	for _, detail := range [][]byte{
		[]byte(`{"nested":{"client_secret":"` + canary + `"}}`),
		[]byte(`{"metadata":[{"authorization":"Bearer ` + canary + `"}]}`),
		[]byte(`{"connection":"postgres://alice:` + canary + `@db/recovery"}`),
		[]byte(`{"nested":{"postgres://alice:` + canary + `@db/recovery":"ordinary"}}`),
		[]byte(`{"items":[{"password=` + canary + `":"ordinary"}]}`),
		[]byte(`{"items":[{" private_key ":"` + canary + `"}]}`),
		[]byte(`{"items":[{" authorization ":"` + canary + `"}]}`),
		[]byte(`{"items":[{" client_secret ":"` + canary + `"}]}`),
	} {
		err := WriteAudit(ctx, pool, AuditRecord{Actor: "deploy:operator", Action: "secrecy_probe", Detail: detail, Result: AuditOK})
		if err == nil || strings.Contains(err.Error(), canary) {
			t.Fatalf("expected generic credential rejection, got %v", err)
		}
	}
	if n := countRows(t, ctx, pool, `SELECT count(*) FROM recovery_audit WHERE action='secrecy_probe'`); n != 0 {
		t.Fatalf("rejected audit payloads wrote %d rows", n)
	}

	if _, err := store.SetIdentityMapping(ctx, SetIdentityMappingRequest{
		Principal: "deploy:secret-user", PersonID: "person-secret", RecordedBy: "deploy:manager",
		Operator: "ops", Reason: "password=" + canary, OperationID: "secret-map-reason",
	}); err == nil || strings.Contains(err.Error(), canary) {
		t.Fatalf("expected generic mapping reason rejection, got %v", err)
	}
	if n := countRows(t, ctx, pool, `SELECT count(*) FROM recovery_identity WHERE principal='deploy:secret-user'`); n != 0 {
		t.Fatalf("rejected mapping wrote %d mapping rows", n)
	}
	if n := countRows(t, ctx, pool, `SELECT count(*) FROM recovery_audit WHERE operation_id='secret-map-reason'`); n != 0 {
		t.Fatalf("rejected mapping wrote %d audit rows", n)
	}

	if _, err := store.SetIdentityMapping(ctx, SetIdentityMappingRequest{
		Principal: "deploy:proof-user", PersonID: "person-proof", RecordedBy: "deploy:manager", OperationID: "proof-map",
	}); err != nil {
		t.Fatalf("seed identity mapping: %v", err)
	}
	_, err := store.RegisterParticipant(ctx, RegisterParticipantRequest{
		InstanceID: instanceID, Principal: "deploy:proof-user", Role: ParticipantRoleExecutor,
		ProofRef: "https://example.invalid/?api_key=" + canary, Actor: "deploy:manager", OperationID: "secret-proof-ref",
	})
	if err == nil || strings.Contains(err.Error(), canary) {
		t.Fatalf("expected generic proof_ref rejection, got %v", err)
	}
	if n := countRows(t, ctx, pool, `SELECT count(*) FROM recovery_participant WHERE principal='deploy:proof-user'`); n != 0 {
		t.Fatalf("rejected proof_ref wrote %d participant rows", n)
	}
	if n := countRows(t, ctx, pool, `SELECT count(*) FROM recovery_audit WHERE operation_id='secret-proof-ref'`); n != 0 {
		t.Fatalf("rejected proof_ref wrote %d audit rows", n)
	}
}

func TestAuditAcceptsWhitespacePaddedStatusAndFingerprintKeys(t *testing.T) {
	ctx, pool, _, _ := migratedControlStore(t)
	err := WriteAudit(ctx, pool, AuditRecord{
		Actor: "deploy:operator", Action: "secrecy_padded_safe",
		Detail: []byte(`{" status ":"healthy"," key_fingerprint ":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`),
		Result: AuditOK,
	})
	if err != nil {
		t.Fatalf("ordinary padded status/fingerprint fields rejected: %v", err)
	}
	if n := countRows(t, ctx, pool, `SELECT count(*) FROM recovery_audit WHERE action='secrecy_padded_safe'`); n != 1 {
		t.Fatalf("expected accepted audit row, got %d", n)
	}
}

func TestDecisionReasonsRejectCredentialsBeforeAnyWrites(t *testing.T) {
	ctx, pool, store, _ := migratedControlStore(t)
	instanceID := openTestInstance(t, ctx, store)
	const canary = "decision-reason-secret-canary-731"
	credentialReasons := []string{
		"postgres://alice:" + canary + "@db/control",
		"password=" + canary,
		"-----BEGIN PRIVATE KEY-----" + canary + "-----END PRIVATE KEY-----",
		"-----BEGIN RSA PRIVATE KEY-----" + canary + "-----END RSA PRIVATE KEY-----",
		"Authorization: Bearer " + canary,
		strings.Repeat("ordinary-reason-", 50) + "-----BEGIN PRIVATE KEY-----" + canary + strings.Repeat("private-material", 50),
		"-----BEGIN PRIVATE KEY-----" + canary + strings.Repeat("private-material", 50),
	}
	baseRelease := ReleaseDecisionRequest{
		InstanceID: instanceID, Capability: "query", ScopeHash: "scope-secrecy", Decision: "release",
		EvidenceHash: "sha256:abc", Actor: "deploy:operator",
	}
	baseApproval := ApprovalDecisionRequest{
		InstanceID: instanceID, Capability: "query", ScopeHash: "scope-secrecy", Decision: "approve",
		ApprovalClassSnapshot: "single_non_executor", Principal: "deploy:operator", PersonID: "person-operator",
		EvidenceHash: "sha256:abc",
	}
	instanceCount := countRows(t, ctx, pool, "SELECT count(*) FROM recovery_instance")
	releaseCount := countRows(t, ctx, pool, "SELECT count(*) FROM recovery_release")
	approvalCount := countRows(t, ctx, pool, "SELECT count(*) FROM recovery_approval")
	auditCount := countRows(t, ctx, pool, "SELECT count(*) FROM recovery_audit")

	assertGeneric := func(label string, err error) {
		t.Helper()
		if err == nil || strings.Contains(err.Error(), canary) || !strings.Contains(err.Error(), "reason") {
			t.Fatalf("%s should fail with a field-only credential error, got %v", label, err)
		}
	}
	for i, reason := range credentialReasons {
		if _, err := store.OpenInstance(ctx, OpenInstanceRequest{
			Kind: "recovery", OpenedBy: "deploy:operator", Reason: reason,
			TargetGuardKey: testTargetGuardKey, TargetRoleFingerprint: testTargetRoleFingerprint,
		}); err == nil {
			assertGeneric("open instance", err)
		} else {
			assertGeneric("open instance", err)
		}

		release := baseRelease
		release.Reason = reason
		release.OperationID = "secrecy-release-" + string(rune('a'+i))
		if _, err := store.AppendReleaseDecision(ctx, release); err == nil {
			assertGeneric("append release", err)
		} else {
			assertGeneric("append release", err)
		}
		approval := baseApproval
		approval.Reason = reason
		approval.OperationID = "secrecy-approval-" + string(rune('a'+i))
		if _, err := store.AppendApprovalDecision(ctx, approval); err == nil {
			assertGeneric("append approval", err)
		} else {
			assertGeneric("append approval", err)
		}

		// Exercise the exported low-level transaction writers too: callers may
		// bypass the Store append wrappers, so validation belongs in canonical().
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin raw release transaction: %v", err)
		}
		directRelease := release
		directRelease.OperationID += "-direct"
		_, insertErr := InsertReleaseDecision(ctx, tx, directRelease)
		_ = tx.Rollback(ctx)
		assertGeneric("direct insert release", insertErr)

		tx, err = pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin raw approval transaction: %v", err)
		}
		directApproval := approval
		directApproval.OperationID += "-direct"
		_, insertErr = InsertApprovalDecision(ctx, tx, directApproval)
		_ = tx.Rollback(ctx)
		assertGeneric("direct insert approval", insertErr)
	}
	if n := countRows(t, ctx, pool, "SELECT count(*) FROM recovery_instance"); n != instanceCount {
		t.Fatalf("credential reasons wrote instances: before=%d after=%d", instanceCount, n)
	}
	if n := countRows(t, ctx, pool, "SELECT count(*) FROM recovery_release"); n != releaseCount {
		t.Fatalf("credential reasons wrote releases: before=%d after=%d", releaseCount, n)
	}
	if n := countRows(t, ctx, pool, "SELECT count(*) FROM recovery_approval"); n != approvalCount {
		t.Fatalf("credential reasons wrote approvals: before=%d after=%d", approvalCount, n)
	}
	if n := countRows(t, ctx, pool, "SELECT count(*) FROM recovery_audit"); n != auditCount {
		t.Fatalf("credential reasons wrote accepted audit rows: before=%d after=%d", auditCount, n)
	}
	if n := countRows(t, ctx, pool, `
SELECT
 (SELECT count(*) FROM recovery_instance WHERE reason LIKE '%' || $1 || '%') +
 (SELECT count(*) FROM recovery_release WHERE reason LIKE '%' || $1 || '%') +
 (SELECT count(*) FROM recovery_approval WHERE reason LIKE '%' || $1 || '%') +
 (SELECT count(*) FROM recovery_audit WHERE coalesce(target::text, '') LIKE '%' || $1 || '%' OR coalesce(detail::text, '') LIKE '%' || $1 || '%')`, canary); n != 0 {
		t.Fatalf("credential canary persisted in %d rows", n)
	}
}

func TestDecisionSafeReasonsRemainAccepted(t *testing.T) {
	ctx, _, store, _ := migratedControlStore(t)
	const safeReason = "authorization_recheck sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	opened, err := store.OpenInstance(ctx, OpenInstanceRequest{
		Kind: "recovery", OpenedBy: "deploy:operator", Reason: safeReason,
		TargetGuardKey: testTargetGuardKey, TargetRoleFingerprint: testTargetRoleFingerprint,
	})
	if err != nil {
		t.Fatalf("safe open-instance reason rejected: %v", err)
	}
	instanceID := opened.InstanceID
	release := ReleaseDecisionRequest{
		InstanceID: instanceID, Capability: "query", ScopeHash: "scope-safe", Decision: "release",
		EvidenceHash: "sha256:abc", Actor: "deploy:operator", Reason: safeReason, OperationID: "safe-reason-release",
	}
	if _, err := store.AppendReleaseDecision(ctx, release); err != nil {
		t.Fatalf("safe release reason rejected: %v", err)
	}
	approval := ApprovalDecisionRequest{
		InstanceID: instanceID, Capability: "query", ScopeHash: "scope-safe", Decision: "approve",
		ApprovalClassSnapshot: "single_non_executor", Principal: "deploy:operator", PersonID: "person-operator",
		EvidenceHash: "sha256:abc", Reason: safeReason, OperationID: "safe-reason-approval",
	}
	if _, err := store.AppendApprovalDecision(ctx, approval); err != nil {
		t.Fatalf("safe approval reason rejected: %v", err)
	}
}

func TestAuditCredentialFreeFingerprintAndStatusRemainAccepted(t *testing.T) {
	ctx, pool, _, _ := migratedControlStore(t)
	if err := WriteAudit(ctx, pool, AuditRecord{
		Actor: "deploy:operator", Action: "secrecy_positive",
		Target: []byte(`{"target_guard_key":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","key_fingerprint":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}`),
		Detail: []byte(`{"evidence_token":"sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","status":"authorization_recheck"}`),
		Result: AuditOK,
	}); err != nil {
		t.Fatalf("credential-free fingerprints/status rejected: %v", err)
	}
	if n := countRows(t, ctx, pool, `SELECT count(*) FROM recovery_audit WHERE action='secrecy_positive'`); n != 1 {
		t.Fatalf("expected one positive audit row, got %d", n)
	}
}
