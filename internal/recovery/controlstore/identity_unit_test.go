// identity_unit_test.go covers the pure logic of the T009 identity path: the
// authenticated <kind>:<id> principal form, the closed role/binding-source/
// mapping-source sets, the refusal-class membership of the F19 invalidation
// path and request canonicalization (person_id is deliberately absent from the
// registration request; operation_id/actor validation). Database behavior
// (mapping resolution, revocation keeping the row, idempotency, invalidation)
// is covered by identity_integration_test.go against a real PostgreSQL.
package controlstore

import (
	"errors"
	"strings"
	"testing"
)

func TestNormalizePrincipalIdentityForm(t *testing.T) {
	valid := []string{"deploy:alice", "sso:alice@example.com", "auth:user-1", "deploy:primary", "idp:alice#2"}
	for _, raw := range valid {
		got, err := NormalizePrincipal("  " + raw + "  ")
		if err != nil {
			t.Fatalf("principal %q must be accepted: %v", raw, err)
		}
		if got != raw {
			t.Fatalf("principal %q must canonicalize to itself, got %q", raw, got)
		}
	}
	invalid := []string{"", "   ", "Alice", "deploy", "deploy:", ":alice", "deploy:al ice", "Deploy:alice", "dep loy:alice", "deploy:ali\tce"}
	for _, raw := range invalid {
		if _, err := NormalizePrincipal(raw); err == nil {
			t.Fatalf("free-form/invalid principal %q must be refused", raw)
		}
	}
	if _, err := NormalizePrincipal("deploy:" + strings.Repeat("x", 260)); err == nil {
		t.Fatal("an over-long principal must be refused")
	}
}

func TestIdentityClosedSets(t *testing.T) {
	for _, role := range []string{ParticipantRoleExecutor, ParticipantRoleVerifier, ParticipantRoleApprover} {
		if !participantRoleSet[role] {
			t.Fatalf("role %q must be in the closed set", role)
		}
	}
	for _, source := range []string{BindingSourceDeployConfig, BindingSourceAuthPrincipal} {
		if !bindingSourceSet[source] {
			t.Fatalf("binding source %q must be in the closed set", source)
		}
	}
	for _, source := range []string{MappingSourceDeployConfig, MappingSourceIdentitySource} {
		if !mappingSourceSet[source] {
			t.Fatalf("mapping source %q must be in the closed set", source)
		}
	}
	// The F19 invalidation refusal path must be expressible in the audited
	// closed refusal set (data-model §3.3).
	if !refusalClasses[RefusalApprovalIdentityUnverified] {
		t.Fatalf("refusal class %q is not in the audited closed set", RefusalApprovalIdentityUnverified)
	}
}

func TestRegisterParticipantRequestCanonical(t *testing.T) {
	instance := "11111111-1111-4111-8111-111111111111"
	valid := RegisterParticipantRequest{
		InstanceID: instance, Principal: "deploy:alice", Role: ParticipantRoleApprover,
		BindingSource: BindingSourceDeployConfig, ProofRef: "ticket-1",
		Actor: "deploy:manager", Operator: "ops", Reason: "bootstrap", OperationID: "op-1",
	}
	canonical, err := valid.canonical()
	if err != nil {
		t.Fatalf("valid registration must canonicalize: %v", err)
	}
	if canonical.Principal != "deploy:alice" || canonical.BindingSource != BindingSourceDeployConfig {
		t.Fatalf("unexpected canonical request: %+v", canonical)
	}

	defaulted := valid
	defaulted.BindingSource = ""
	canonicalDefault, err := defaulted.canonical()
	if err != nil || canonicalDefault.BindingSource != BindingSourceDeployConfig {
		t.Fatalf("an absent binding_source must default to deploy_config: %+v err=%v", canonicalDefault, err)
	}

	bad := []RegisterParticipantRequest{
		func() RegisterParticipantRequest { r := valid; r.InstanceID = "not-a-uuid"; return r }(),
		func() RegisterParticipantRequest { r := valid; r.InstanceID = ""; return r }(),
		func() RegisterParticipantRequest { r := valid; r.Principal = "Alice"; return r }(),
		func() RegisterParticipantRequest { r := valid; r.Role = "owner"; return r }(),
		func() RegisterParticipantRequest { r := valid; r.Role = ""; return r }(),
		func() RegisterParticipantRequest { r := valid; r.BindingSource = "free_form"; return r }(),
		func() RegisterParticipantRequest { r := valid; r.Actor = ""; return r }(),
		func() RegisterParticipantRequest { r := valid; r.OperationID = ""; return r }(),
		func() RegisterParticipantRequest { r := valid; r.OperationID = strings.Repeat("x", 129); return r }(),
		func() RegisterParticipantRequest { r := valid; r.OperationID = "op\n1"; return r }(),
		func() RegisterParticipantRequest { r := valid; r.ProofRef = "proof\nref"; return r }(),
	}
	for i, candidate := range bad {
		if _, err := candidate.canonical(); err == nil {
			t.Fatalf("bad registration request %d must be refused", i)
		}
	}
}

func TestSetIdentityMappingRequestCanonical(t *testing.T) {
	valid := SetIdentityMappingRequest{
		Principal: "deploy:alice", PersonID: "person-a", Source: MappingSourceDeployConfig,
		RecordedBy: "deploy:manager", Operator: "ops", Reason: "bootstrap", OperationID: "op-1",
	}
	if _, err := valid.canonical(); err != nil {
		t.Fatalf("valid mapping set must canonicalize: %v", err)
	}
	defaulted := valid
	defaulted.Source = ""
	if canonical, err := defaulted.canonical(); err != nil || canonical.Source != MappingSourceDeployConfig {
		t.Fatalf("an absent source must default to deploy_config: %+v err=%v", canonical, err)
	}

	bad := []SetIdentityMappingRequest{
		func() SetIdentityMappingRequest { r := valid; r.Principal = "alice"; return r }(),
		func() SetIdentityMappingRequest { r := valid; r.PersonID = ""; return r }(),
		func() SetIdentityMappingRequest { r := valid; r.PersonID = "person\n-a"; return r }(),
		func() SetIdentityMappingRequest { r := valid; r.Source = "spreadsheet"; return r }(),
		func() SetIdentityMappingRequest { r := valid; r.RecordedBy = ""; return r }(),
		func() SetIdentityMappingRequest { r := valid; r.OperationID = ""; return r }(),
	}
	for i, candidate := range bad {
		if _, err := candidate.canonical(); err == nil {
			t.Fatalf("bad mapping set request %d must be refused", i)
		}
	}
}

func TestRevokeIdentityMappingRequestCanonical(t *testing.T) {
	valid := RevokeIdentityMappingRequest{
		Principal: "deploy:alice", RecordedBy: "deploy:manager", Operator: "ops",
		Reason: "offboard", OperationID: "op-1",
	}
	if _, err := valid.canonical(); err != nil {
		t.Fatalf("valid mapping revoke must canonicalize: %v", err)
	}
	bad := []RevokeIdentityMappingRequest{
		func() RevokeIdentityMappingRequest { r := valid; r.Principal = "Alice"; return r }(),
		func() RevokeIdentityMappingRequest { r := valid; r.RecordedBy = ""; return r }(),
		func() RevokeIdentityMappingRequest { r := valid; r.OperationID = "op\ttab"; return r }(),
	}
	for i, candidate := range bad {
		if _, err := candidate.canonical(); err == nil {
			t.Fatalf("bad mapping revoke request %d must be refused", i)
		}
	}
}

func TestIdentityRefusalSentinels(t *testing.T) {
	for _, err := range []error{ErrIdentityMappingMissing, ErrIdentityMappingRevoked, ErrParticipantAlreadyRegistered} {
		if !errors.Is(err, err) || err.Error() == "" {
			t.Fatalf("identity refusal sentinel %v must be a non-empty error", err)
		}
	}
}
