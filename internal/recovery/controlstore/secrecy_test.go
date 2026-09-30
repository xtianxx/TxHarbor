package controlstore

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestCredentialSecrecyValidation(t *testing.T) {
	for _, raw := range []string{
		`{"nested":{"client-secret":"secret-canary"}}`,
		`{"items":[{"Authorization":"Bearer bearer-canary"}]}`,
		`{"note":"postgres://alice:uri-canary@db/recovery"}`,
		`{"note":"password=keyword-canary"}`,
		`{"note":"-----BEGIN PRIVATE KEY-----pem-canary-----END PRIVATE KEY-----"}`,
		`{"note":"-----BEGIN RSA PRIVATE KEY-----privatekey-canary"}`,
	} {
		t.Run(raw, func(t *testing.T) {
			err := validateCredentialJSON("audit detail", []byte(raw))
			if err == nil {
				t.Fatal("expected credential rejection")
			}
			if strings.Contains(err.Error(), "canary") || !strings.Contains(err.Error(), "audit detail") {
				t.Fatalf("error must name only the field and not echo input: %v", err)
			}
		})
	}

	for _, raw := range []string{
		`{"nested":{"evidence_token":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"target_guard_key":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","key_fingerprint":"sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","status":"authorization_recheck"}`,
		`{"authorization":"[REDACTED]","private_key":"[REDACTED]"}`,
	} {
		if err := validateCredentialJSON("audit detail", []byte(raw)); err != nil {
			t.Errorf("safe credential-free/redacted JSON rejected: %v", err)
		}
	}

	for _, value := range []string{"authorization_recheck passed", "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "role-fingerprint"} {
		if err := validateCredentialText("proof_ref", value); err != nil {
			t.Errorf("safe text %q rejected: %v", value, err)
		}
	}
	for _, value := range []string{
		"client_secret=client-canary",
		"postgres://alice:uri-canary@db/recovery",
		"password=keyword-canary",
		"-----BEGIN PRIVATE KEY-----pem-canary-----END PRIVATE KEY-----",
		"-----BEGIN RSA PRIVATE KEY-----privatekey-canary-----END RSA PRIVATE KEY-----",
		"Authorization: Bearer authorization-canary",
		"-----BEGIN PRIVATE KEY-----truncated-canary",
	} {
		if err := validateCredentialText("reason", value); err == nil || strings.Contains(err.Error(), "canary") || !strings.Contains(err.Error(), "reason") {
			t.Errorf("credential reason %q must be rejected without echoing it, got %v", value, err)
		}
	}
}

func TestDecisionReasonsAreCredentialCheckedBeforeCanonicalization(t *testing.T) {
	const instanceID = "11111111-1111-4111-8111-111111111111"
	release := ReleaseDecisionRequest{
		InstanceID: instanceID, Capability: "query", ScopeHash: "scope-1", Decision: "release",
		EvidenceHash: "sha256:abc", Actor: "deploy:ops", OperationID: "release-secret",
	}
	approval := ApprovalDecisionRequest{
		InstanceID: instanceID, Capability: "query", ScopeHash: "scope-1", Decision: "approve",
		ApprovalClassSnapshot: "single_non_executor", Principal: "deploy:alice", PersonID: "person-1",
		EvidenceHash: "sha256:abc", OperationID: "approval-secret",
	}
	for _, reason := range []string{"postgres://u:decision-canary@db/control", "password=decision-canary", "Authorization: Bearer decision-canary", "-----BEGIN PRIVATE KEY-----decision-canary-----END PRIVATE KEY-----"} {
		candidate := release
		candidate.Reason = reason
		if _, err := candidate.canonical(); err == nil || strings.Contains(err.Error(), "decision-canary") || !strings.Contains(err.Error(), "reason") {
			t.Errorf("release reason should fail generically before canonicalization, got %v", err)
		}
		approvalCandidate := approval
		approvalCandidate.Reason = reason
		if _, err := approvalCandidate.canonical(); err == nil || strings.Contains(err.Error(), "decision-canary") || !strings.Contains(err.Error(), "reason") {
			t.Errorf("approval reason should fail generically before canonicalization, got %v", err)
		}
	}
	for _, reason := range []string{"", "operator approved after authorization_recheck", "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"} {
		release.Reason = reason
		if _, err := release.canonical(); err != nil {
			t.Errorf("safe release reason %q rejected: %v", reason, err)
		}
		approval.Reason = reason
		if _, err := approval.canonical(); err != nil {
			t.Errorf("safe approval reason %q rejected: %v", reason, err)
		}
	}
}

func TestSensitiveJSONNamesAreExact(t *testing.T) {
	for _, key := range []string{"target_guard_key", "key_fingerprint", "evidence_token", "nonce_token"} {
		raw, _ := json.Marshal(map[string]any{key: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"})
		if err := validateCredentialJSON("audit detail", raw); err != nil {
			t.Errorf("non-credential field %q rejected: %v", key, err)
		}
	}
}

func TestCredentialJSONChecksKeysAsWellAsValues(t *testing.T) {
	for _, raw := range []string{
		`{"nested":{"postgres://alice:uri-canary@db":"ordinary"}}`,
		`{"items":[{"password=keyword-canary":"ordinary"}]}`,
		`{"nested":{"client_secret":"canary"}}`,
		`{"nested":{"client-secret":"canary"}}`,
		`{"items":[{" private_key ":"key-canary"}]}`,
		`{"items":[{" authorization ":"auth-canary"}]}`,
		`{"items":[{" client_secret ":"client-canary"}]}`,
	} {
		err := ValidateCredentialJSON("audit detail", []byte(raw))
		if err == nil || strings.Contains(err.Error(), "canary") || !strings.Contains(err.Error(), "audit detail") {
			t.Errorf("credential-shaped JSON key must be rejected generically: %v", err)
		}
	}
	for _, raw := range []string{
		`{"status":"authorization_recheck","key_fingerprint":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`,
		`{"evidence_token":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}`,
		`{" status ":"authorization_recheck"," key_fingerprint ":"sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}`,
	} {
		if err := ValidateCredentialJSON("audit detail", []byte(raw)); err != nil {
			t.Errorf("safe status/fingerprint payload rejected: %v", err)
		}
	}
}
