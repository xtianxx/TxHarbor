//go:build integration

// identity_path_integration_test.go is T046 [US4]: the identity and
// authorization acceptance path for releases against a real PostgreSQL 18.6
// control store (tags: integration; FR-023; contracts/approval-matrix.md
// §1–§4; tasks.md T046; F19/F7).
//
// TDD-first. B12 lands the tests before the B13 implementation, so this file
// references the planned T048/T049 API (internal/recovery/approvals.go and
// release.go) that does not exist until B13: the integration candidate fails
// to build until then. Expected API surface:
//
//	// internal/recovery/approvals.go (T048)
//	type ApprovalRequest struct { InstanceID string; Capability Capability; ScopeHash, Principal, Reason, OperationID string }
//	type ApprovalOutcome struct { ApprovalID, Decision string; ApprovalClass ApprovalClass; Principal, PersonID string; Recorded bool; RefusalClass RefusalClass; Reason string }
//	func Approve(ctx context.Context, store *controlstore.Store, req ApprovalRequest) (ApprovalOutcome, error)
//	func RevokeApproval(ctx context.Context, store *controlstore.Store, req ApprovalRequest) (ApprovalOutcome, error)
//	var ErrApprovalRefused error
//
//	// internal/recovery/release.go (T049)
//	type ReleaseRequest struct { InstanceID string; Capability Capability; ScopeHash, Principal, Reason, OperationID string }
//	type ReleaseOutcome struct { ReleaseID, Decision string; ApprovalRefs []string; Recorded bool; RefusalClass RefusalClass; Reason string }
//	func Release(ctx context.Context, store *controlstore.Store, gate *Gate, req ReleaseRequest) (ReleaseOutcome, error)
//	var ErrReleaseRefused error
//
// Semantics pinned here (real control store; identity data is written only
// through the deployment-controlled T009/T010 paths — recovery_identity and
// recovery_participant — and approvals/releases only through the T048/T049
// API):
//
//   - two explicitly registered principals of two different people complete a
//     dual approval; the authenticated principal comes from the
//     deployment-controlled configuration (TXHARBOR_RECOVERY_PRINCIPAL via
//     config.Load), never from free text or a request field;
//   - the approval carriage has no person_id/class field: a caller can never
//     supply a person or force the single class — person identity is resolved
//     from the current active mapping, and the class is the conservative
//     derived class;
//   - an unregistered principal, a principal without an active mapping and a
//     principal with the wrong role are refused and audited (F7);
//   - execution, verification and approval are independent permissions: one
//     person may hold several roles, but the instance executor can never
//     approve its own case (including the same person under another
//     principal);
//   - F19 mapping change: changing a mapping immediately invalidates the
//     approvals that relied on it, records an auditable invalidation
//     (who/when/which mapping) and the gate refuses the old basis with
//     approval_identity_unverified until re-approval; a conservative dual
//     class never offsets a wrong mapping (zero releases).
//
// Docker missing: the package TestMain (generation_integration_test.go)
// reports NOT RUN locally (exit 0) and fails the package under CI=true /
// TXHARBOR_REQUIRE_DOCKER=1 — an unrun PG layer is never a pass.
package recovery

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

const (
	t046Alice = "auth:t046-alice"
	t046Bob   = "auth:t046-bob"
)

// t046Count runs one constant test query over the fixture pool.
func t046Count(t *testing.T, f *gateFixture, sql string, args ...any) int {
	t.Helper()
	return countGenerationRows(t, f.ctx, f.pool, sql, args...)
}

// t046ConfigPrincipal loads the deployment configuration with the given
// authenticated principal (TXHARBOR_RECOVERY_PRINCIPAL) and returns what the
// process would bind. The base environment mirrors the config package's own
// fixture so only the principal varies.
func t046ConfigPrincipal(t *testing.T, principal string) string {
	t.Helper()
	env := map[string]string{
		config.EnvPGDSN:                 "postgres://txharbor:txharbor@127.0.0.1:5432/txharbor?sslmode=disable",
		config.EnvRPCURL:                "http://127.0.0.1:8545",
		config.EnvChainID:               "31337",
		config.EnvStartHeight:           "0",
		config.EnvLogStartHeight:        "0",
		config.EnvLogContracts:          "0x1111111111111111111111111111111111111111",
		config.EnvDepositStartHeight:    "0",
		config.EnvDepositContracts:      "0x1111111111111111111111111111111111111111",
		config.EnvDepositWatchAddresses: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		config.EnvConfirmationDepth:     "10",
	}
	if principal != "" {
		env[config.EnvRecoveryPrincipal] = principal
	}
	cfg, err := config.Load(func(key string) (string, bool) {
		value, ok := env[key]
		return value, ok
	})
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	return cfg.Recovery.Principal
}

// t046Approve performs one approval through the T048 API.
func t046Approve(t *testing.T, f *gateFixture, principal string, capability Capability) (ApprovalOutcome, error) {
	t.Helper()
	return Approve(f.ctx, f.store, ApprovalRequest{
		InstanceID: f.instanceID, Capability: capability, ScopeHash: gateScopeFor(capability),
		Principal: principal, Reason: "t046 approval", OperationID: gateOperation("t046-approve"),
	})
}

func t046ApproveOK(t *testing.T, f *gateFixture, principal string, capability Capability) ApprovalOutcome {
	t.Helper()
	out, err := t046Approve(t, f, principal, capability)
	if err != nil {
		t.Fatalf("Approve(%s, %s): %v", principal, capability, err)
	}
	return out
}

func t046Release(t *testing.T, f *gateFixture, gate *Gate, capability Capability) (ReleaseOutcome, error) {
	t.Helper()
	return Release(f.ctx, f.store, gate, ReleaseRequest{
		InstanceID: f.instanceID, Capability: capability, ScopeHash: gateScopeFor(capability),
		Principal: "deploy:executor", Reason: "t046 release", OperationID: gateOperation("t046-release"),
	})
}

func t046ReleaseOK(t *testing.T, f *gateFixture, gate *Gate, capability Capability) ReleaseOutcome {
	t.Helper()
	out, err := t046Release(t, f, gate, capability)
	if err != nil {
		t.Fatalf("Release(%s): %v", capability, err)
	}
	return out
}

func t046Admit(t *testing.T, f *gateFixture, gate *Gate, capability Capability) GateDecision {
	t.Helper()
	decision, err := gate.Admit(f.ctx, GateRequest{
		InstanceID: f.instanceID, Capability: capability, ScopeHash: gateScopeFor(capability),
		Actor: "deploy:executor", OperationID: gateOperation("t046-admit"), Action: "test:t046",
	})
	if err != nil {
		t.Fatalf("Admit(%s): %v", capability, err)
	}
	return decision
}

// t046BaseFixture is the shared scene: executor, verifier and one approver
// (auth:approver -> person-approver) plus a second approver
// (t046Alice -> person-alice) and (t046Bob -> person-bob), all explicitly
// mapped before registration.
func t046BaseFixture(t *testing.T) (*gateFixture, *Checklist, *Gate) {
	t.Helper()
	f := gateBaseFixture(t)
	gateMapIdentity(t, f.ctx, f.store, t046Alice, "person-alice")
	gateMapIdentity(t, f.ctx, f.store, t046Bob, "person-bob")
	gateRegister(t, f.ctx, f.store, f.instanceID, t046Alice, "approver")
	gateRegister(t, f.ctx, f.store, f.instanceID, t046Bob, "approver")
	checklist, err := NewChecklist(f.store)
	if err != nil {
		t.Fatalf("NewChecklist: %v", err)
	}
	gate := gateNewGate(t, f.store, GateOptions{TTL: time.Minute})
	return f, checklist, gate
}

// t046AllowDual seeds and releases the dependency chain_scan (through the
// baseline approver, whose mapping never changes in these tests), then returns
// once `capability` (dual) has its isolation set verified. All isolation
// verdicts are seeded before any release: a verdict advances the evidence
// generation and would otherwise invalidate the earlier release.
func t046AllowDual(t *testing.T, f *gateFixture, checklist *Checklist, gate *Gate, capability Capability) {
	t.Helper()
	if capability == CapabilityExistingWithdrawalRecovery {
		isoVerifyAll(t, f, checklist, CapabilityChainScan)
		isoVerifyAll(t, f, checklist, capability)
		t046ApproveOK(t, f, "auth:approver", CapabilityChainScan)
		t046ReleaseOK(t, f, gate, CapabilityChainScan)
		return
	}
	isoVerifyAll(t, f, checklist, capability)
}

// ---------------------------------------------------------------------------
// The explicit two-person path and the deployment-controlled principal
// ---------------------------------------------------------------------------

func TestT046ExplicitTwoPrincipalDualApprovalPath(t *testing.T) {
	f, checklist, gate := t046BaseFixture(t)
	t046AllowDual(t, f, checklist, gate, CapabilityExistingWithdrawalRecovery)

	// The approval carriage accepts no person and no class: identity comes
	// from the current mapping and the class from the conservative matrix.
	for _, typ := range []reflect.Type{reflect.TypeOf(ApprovalRequest{})} {
		for i := 0; i < typ.NumField(); i++ {
			name := typ.Field(i).Name
			if strings.Contains(strings.ToLower(name), "person") {
				t.Fatalf("ApprovalRequest carries field %q; a caller must never supply a person (approval-matrix §2)", name)
			}
			if strings.Contains(strings.ToLower(name), "class") {
				t.Fatalf("ApprovalRequest carries field %q; the approval class is derived conservatively, never chosen", name)
			}
		}
	}

	// The authenticated principals come from the deployment-controlled
	// configuration (TXHARBOR_RECOVERY_PRINCIPAL).
	alice := t046ConfigPrincipal(t, t046Alice)
	bob := t046ConfigPrincipal(t, t046Bob)
	if alice == "" || bob == "" || alice == bob {
		t.Fatalf("deployment configuration principals = (%q, %q), want two distinct bound principals", alice, bob)
	}

	first := t046ApproveOK(t, f, alice, CapabilityExistingWithdrawalRecovery)
	second := t046ApproveOK(t, f, bob, CapabilityExistingWithdrawalRecovery)
	if first.PersonID != "person-alice" || second.PersonID != "person-bob" {
		t.Fatalf("resolved persons = (%q, %q), want the explicit mappings (person-alice, person-bob)", first.PersonID, second.PersonID)
	}
	if first.PersonID == second.PersonID {
		t.Fatal("dual approval needs two distinct people")
	}

	rel := t046ReleaseOK(t, f, gate, CapabilityExistingWithdrawalRecovery)
	if len(rel.ApprovalRefs) != 2 {
		t.Fatalf("dual release recorded %d approval refs, want 2", len(rel.ApprovalRefs))
	}
	// The recorded basis resolves to two distinct persons in the control
	// store (never proven by a caller-supplied person_id).
	if got := t046Count(t, f, `
SELECT count(DISTINCT person_id) FROM recovery_approval
WHERE instance_id = $1 AND capability = 'existing_withdrawal_recovery' AND approval_id::text = ANY($2)`,
		f.instanceID, rel.ApprovalRefs); got != 2 {
		t.Fatalf("recorded approval basis covers %d distinct persons, want 2", got)
	}
	if got := t046Admit(t, f, gate, CapabilityExistingWithdrawalRecovery); !got.Allowed {
		t.Fatalf("dual-released capability must be admitted, got %+v", got)
	}
}

func TestT046DeploymentPrincipalBindingIsRequired(t *testing.T) {
	f, checklist, _ := t046BaseFixture(t)
	isoVerifyAll(t, f, checklist, CapabilityQuery)

	// No TXHARBOR_RECOVERY_PRINCIPAL configured: the process binds no
	// principal and the approval path must refuse (never authorize by an
	// empty subject).
	if got := t046ConfigPrincipal(t, ""); got != "" {
		t.Fatalf("unset %s = %q, want empty (missing means not configured)", config.EnvRecoveryPrincipal, got)
	}
	before := t046Count(t, f, `SELECT count(*) FROM recovery_approval WHERE instance_id = $1`, f.instanceID)
	if _, err := t046Approve(t, f, "", CapabilityQuery); err == nil {
		t.Fatal("an empty principal must never be accepted as an authenticated approver")
	}
	if _, err := t046Approve(t, f, "not-a-principal", CapabilityQuery); err == nil {
		t.Fatal("a free-form principal must never be accepted (the <kind>:<id> form is binding)")
	}
	if got := t046Count(t, f, `SELECT count(*) FROM recovery_approval WHERE instance_id = $1`, f.instanceID); got != before {
		t.Fatalf("refused principals wrote %d approval row(s)", got-before)
	}

	// A valid bound principal approves.
	out := t046ApproveOK(t, f, t046ConfigPrincipal(t, t046Alice), CapabilityQuery)
	if out.Principal != t046Alice || out.PersonID != "person-alice" {
		t.Fatalf("bound approval = %+v, want principal %q with person-alice", out, t046Alice)
	}
}

// ---------------------------------------------------------------------------
// Missing mapping / unregistered principal / wrong role
// ---------------------------------------------------------------------------

func TestT046MissingMappingOrUnregisteredPrincipalRefused(t *testing.T) {
	f, checklist, _ := t046BaseFixture(t)
	isoVerifyAll(t, f, checklist, CapabilityQuery)

	// Principal with no mapping and no binding.
	before := t046Count(t, f, `SELECT count(*) FROM recovery_audit WHERE result = 'refused'`)
	out, err := t046Approve(t, f, "auth:t046-carol", CapabilityQuery)
	if err == nil || !errors.Is(err, ErrApprovalRefused) {
		t.Fatalf("unmapped principal approval = (%+v, %v), want ErrApprovalRefused", out, err)
	}
	if out.RefusalClass != RefusalApprovalIdentityUnverified {
		t.Fatalf("unmapped principal class = %q, want %q", out.RefusalClass, RefusalApprovalIdentityUnverified)
	}
	if got := t046Count(t, f, `SELECT count(*) FROM recovery_approval WHERE instance_id = $1`, f.instanceID); got != 0 {
		t.Fatalf("unmapped approval wrote %d row(s)", got)
	}
	if got := t046Count(t, f, `SELECT count(*) FROM recovery_audit WHERE result = 'refused'`); got <= before {
		t.Fatal("the unmapped-principal refusal was not audited")
	}

	// Mapped principal without the approver role on this instance.
	gateMapIdentity(t, f.ctx, f.store, "auth:t046-unregistered", "person-unregistered")
	before = t046Count(t, f, `SELECT count(*) FROM recovery_audit WHERE result = 'refused'`)
	out, err = t046Approve(t, f, "auth:t046-unregistered", CapabilityQuery)
	if err == nil || !errors.Is(err, ErrApprovalRefused) {
		t.Fatalf("unregistered principal approval = (%+v, %v), want ErrApprovalRefused", out, err)
	}
	if out.RefusalClass != RefusalApprovalMissing {
		t.Fatalf("unregistered principal class = %q, want %q", out.RefusalClass, RefusalApprovalMissing)
	}
	if got := t046Count(t, f, `SELECT count(*) FROM recovery_approval WHERE instance_id = $1`, f.instanceID); got != 0 {
		t.Fatalf("unregistered approval wrote %d row(s)", got)
	}
	if got := t046Count(t, f, `SELECT count(*) FROM recovery_audit WHERE result = 'refused'`); got <= before {
		t.Fatal("the unregistered-principal refusal was not audited")
	}

	// A revoked mapping cannot prove a person either.
	if _, err := f.store.RevokeIdentityMapping(f.ctx, controlstore.RevokeIdentityMappingRequest{
		Principal: t046Bob, RecordedBy: "deploy:admin", OperationID: gateOperation("t046-map-revoke"),
	}); err != nil {
		t.Fatalf("revoke mapping: %v", err)
	}
	out, err = t046Approve(t, f, t046Bob, CapabilityQuery)
	if err == nil || !errors.Is(err, ErrApprovalRefused) {
		t.Fatalf("revoked-mapping approval = (%+v, %v), want ErrApprovalRefused", out, err)
	}
	if out.RefusalClass != RefusalApprovalIdentityUnverified {
		t.Fatalf("revoked-mapping class = %q, want %q", out.RefusalClass, RefusalApprovalIdentityUnverified)
	}
	if got := t046Count(t, f, `SELECT count(*) FROM recovery_approval WHERE instance_id = $1`, f.instanceID); got != 0 {
		t.Fatalf("revoked-mapping approval wrote %d row(s)", got)
	}
}

// ---------------------------------------------------------------------------
// Execution / verification / approval independence
// ---------------------------------------------------------------------------

func TestT046ExecutionVerificationAndApprovalAreIndependent(t *testing.T) {
	f, checklist, _ := t046BaseFixture(t)

	// The executor collects checklist evidence (execution) but can never
	// approve its own instance — even when additionally bound as approver.
	if _, err := checklist.Set(f.ctx, ChecklistEvidenceRequest{
		InstanceID: f.instanceID, ItemKey: IsolationItemOldWritersStopped,
		State: ChecklistStateEvidenced, EvidenceRef: "evidence://015/t046/old-writers",
		Actor: "deploy:executor", OperationID: gateOperation("t046-set"),
	}); err != nil {
		t.Fatalf("executor checklist.Set: %v", err)
	}
	gateRegister(t, f.ctx, f.store, f.instanceID, "deploy:executor", "approver")
	out, err := t046Approve(t, f, "deploy:executor", CapabilityQuery)
	if err == nil || !errors.Is(err, ErrApprovalRefused) {
		t.Fatalf("executor approval = (%+v, %v), want ErrApprovalRefused", out, err)
	}
	if out.RefusalClass != RefusalApprovalExecutorExcluded {
		t.Fatalf("executor approval class = %q, want %q", out.RefusalClass, RefusalApprovalExecutorExcluded)
	}

	// The verifier confirms the item (verification) but is not an approver.
	if _, err := checklist.Verify(f.ctx, ChecklistVerifyRequest{
		InstanceID: f.instanceID, ItemKey: IsolationItemOldWritersStopped,
		Actor: "auth:verifier", OperationID: gateOperation("t046-verify"),
	}); err != nil {
		t.Fatalf("verifier checklist.Verify: %v", err)
	}
	out, err = t046Approve(t, f, "auth:verifier", CapabilityQuery)
	if err == nil || !errors.Is(err, ErrApprovalRefused) {
		t.Fatalf("verifier approval = (%+v, %v), want ErrApprovalRefused", out, err)
	}
	if out.RefusalClass != RefusalApprovalMissing {
		t.Fatalf("verifier approval class = %q, want %q", out.RefusalClass, RefusalApprovalMissing)
	}

	// The approver approves but cannot verify checklist items (no verifier
	// binding): approval and verification are separate permissions.
	if _, err := checklist.Verify(f.ctx, ChecklistVerifyRequest{
		InstanceID: f.instanceID, ItemKey: IsolationItemOldWritersStopped,
		Actor: "auth:approver", OperationID: gateOperation("t046-verify-wrong-role"),
	}); err == nil {
		t.Fatal("an approver without the verifier binding must not confirm a checklist item")
	}
	if _, err := t046Approve(t, f, "auth:approver", CapabilityQuery); err != nil {
		t.Fatalf("the approver must be able to approve: %v", err)
	}

	// One person may hold several roles: the verifier bound additionally as
	// approver can approve (a different person from the executor).
	gateRegister(t, f.ctx, f.store, f.instanceID, "auth:verifier", "approver")
	if _, err := t046Approve(t, f, "auth:verifier", CapabilityQuery); err != nil {
		t.Fatalf("a verifier additionally bound as approver must be able to approve: %v", err)
	}
}

// ---------------------------------------------------------------------------
// F19: mapping change invalidates approvals, is audited, and dual never
// offsets a wrong mapping
// ---------------------------------------------------------------------------

func TestT046MappingChangeInvalidatesApprovalsAndIsAudited(t *testing.T) {
	f, checklist, gate := t046BaseFixture(t)
	t046AllowDual(t, f, checklist, gate, CapabilityExistingWithdrawalRecovery)
	aliceApproval := t046ApproveOK(t, f, t046Alice, CapabilityExistingWithdrawalRecovery)
	t046ApproveOK(t, f, t046Bob, CapabilityExistingWithdrawalRecovery)
	t046ReleaseOK(t, f, gate, CapabilityExistingWithdrawalRecovery)
	if got := t046Admit(t, f, gate, CapabilityExistingWithdrawalRecovery); !got.Allowed {
		t.Fatalf("dual-released capability must be admitted, got %+v", got)
	}

	// The mapping changes: the old approval is immediately invalidated and the
	// change records who/when/which mapping.
	change, err := f.store.SetIdentityMapping(f.ctx, controlstore.SetIdentityMappingRequest{
		Principal: t046Alice, PersonID: "person-alice-other", RecordedBy: "deploy:admin",
		Reason: "t046 mapping change", OperationID: gateOperation("t046-map-change"),
	})
	if err != nil {
		t.Fatalf("SetIdentityMapping: %v", err)
	}
	if !change.Changed || change.Mapping.PersonID != "person-alice-other" {
		t.Fatalf("mapping change = %+v, want a changed mapping to person-alice-other", change)
	}
	found := false
	for _, invalidated := range change.InvalidatedApprovals {
		if invalidated.ApprovalID == aliceApproval.ApprovalID {
			found = true
		}
	}
	if !found {
		t.Fatalf("the mapping change did not report the invalidated approval %s", aliceApproval.ApprovalID)
	}

	// The invalidation is auditable: refusal-class row for the approval plus
	// the mapping-change row naming principal and recorded_by.
	if got := t046Count(t, f, `
SELECT count(*) FROM recovery_audit
WHERE action = 'approval_identity_invalidated' AND result = 'refused'
  AND refusal_class = $1 AND target->>'approval_id' = $2`,
		controlstore.RefusalApprovalIdentityUnverified, aliceApproval.ApprovalID); got < 1 {
		t.Fatalf("invalidation audit rows = %d, want >= 1 (F19 audit trail)", got)
	}
	if got := t046Count(t, f, `
SELECT count(*) FROM recovery_audit
WHERE action = 'identity_map_set' AND actor = 'deploy:admin'
  AND target->>'principal' = $1 AND detail->>'person_id' = 'person-alice-other'`,
		t046Alice); got < 1 {
		t.Fatalf("mapping-change audit rows = %d, want >= 1 naming the principal, person and actor", got)
	}

	// The gate refuses the old basis with approval_identity_unverified, and a
	// re-release is refused with zero new release rows.
	if got := t046Admit(t, f, gate, CapabilityExistingWithdrawalRecovery); got.Allowed || got.RefusalClass != RefusalApprovalIdentityUnverified {
		t.Fatalf("mapping-changed admission = %+v, want %s", got, RefusalApprovalIdentityUnverified)
	}
	before := t046Count(t, f,
		`SELECT count(*) FROM recovery_release WHERE instance_id = $1 AND capability = 'existing_withdrawal_recovery' AND decision = 'release'`,
		f.instanceID)
	rel, err := t046Release(t, f, gate, CapabilityExistingWithdrawalRecovery)
	if err == nil || !errors.Is(err, ErrReleaseRefused) {
		t.Fatalf("release over a mapping-changed approval = (%+v, %v), want ErrReleaseRefused", rel, err)
	}
	if rel.RefusalClass != RefusalApprovalIdentityUnverified {
		t.Fatalf("mapping-changed release class = %q, want %q", rel.RefusalClass, RefusalApprovalIdentityUnverified)
	}
	if got := t046Count(t, f,
		`SELECT count(*) FROM recovery_release WHERE instance_id = $1 AND capability = 'existing_withdrawal_recovery' AND decision = 'release'`,
		f.instanceID); got != before {
		t.Fatalf("mapping-changed release wrote %d new release row(s)", got-before)
	}

	// Re-map and re-approve: the path converges.
	gateMapIdentity(t, f.ctx, f.store, t046Alice, "person-alice")
	t046ApproveOK(t, f, t046Alice, CapabilityExistingWithdrawalRecovery)
	t046ApproveOK(t, f, t046Bob, CapabilityExistingWithdrawalRecovery)
	t046ReleaseOK(t, f, gate, CapabilityExistingWithdrawalRecovery)
	if got := t046Admit(t, f, gate, CapabilityExistingWithdrawalRecovery); !got.Allowed {
		t.Fatalf("re-mapped and re-approved capability must be admitted, got %+v", got)
	}
}

func TestT046ConservativeDualDoesNotOffsetWrongMapping(t *testing.T) {
	// Two principals of the SAME person cannot complete a dual approval, no
	// matter how many valid-looking approve rows exist.
	f, checklist, gate := t046BaseFixture(t)
	t046AllowDual(t, f, checklist, gate, CapabilityExistingWithdrawalRecovery)
	gateMapIdentity(t, f.ctx, f.store, "auth:t046-carol", "person-alice")
	gateRegister(t, f.ctx, f.store, f.instanceID, "auth:t046-carol", "approver")
	t046ApproveOK(t, f, t046Alice, CapabilityExistingWithdrawalRecovery)
	t046ApproveOK(t, f, "auth:t046-carol", CapabilityExistingWithdrawalRecovery)
	before := t046Count(t, f,
		`SELECT count(*) FROM recovery_release WHERE instance_id = $1 AND capability = 'existing_withdrawal_recovery' AND decision = 'release'`,
		f.instanceID)
	rel, err := t046Release(t, f, gate, CapabilityExistingWithdrawalRecovery)
	if err == nil {
		t.Fatalf("same-person dual approval released %+v; conservative dual must not offset a wrong mapping", rel)
	}
	if rel.RefusalClass != RefusalApprovalIdentityUnverified {
		t.Fatalf("same-person refusal class = %q, want %q", rel.RefusalClass, RefusalApprovalIdentityUnverified)
	}
	if got := t046Count(t, f,
		`SELECT count(*) FROM recovery_release WHERE instance_id = $1 AND capability = 'existing_withdrawal_recovery' AND decision = 'release'`,
		f.instanceID); got != before {
		t.Fatalf("same-person dual wrote %d release row(s)", got-before)
	}
	if got := t046Admit(t, f, gate, CapabilityExistingWithdrawalRecovery); got.Allowed {
		t.Fatalf("same-person dual must not admit, got %+v", got)
	}

	// A conflict where a second principal is re-mapped onto the first
	// person's id: the old approvals no longer prove two people.
	f2, checklist2, gate2 := t046BaseFixture(t)
	t046AllowDual(t, f2, checklist2, gate2, CapabilityExistingWithdrawalRecovery)
	t046ApproveOK(t, f2, t046Alice, CapabilityExistingWithdrawalRecovery)
	t046ApproveOK(t, f2, t046Bob, CapabilityExistingWithdrawalRecovery)
	if _, err := f2.store.SetIdentityMapping(f2.ctx, controlstore.SetIdentityMappingRequest{
		Principal: t046Bob, PersonID: "person-alice", RecordedBy: "deploy:admin",
		Reason: "t046 conflicting mapping", OperationID: gateOperation("t046-map-conflict"),
	}); err != nil {
		t.Fatalf("conflicting mapping: %v", err)
	}
	// No release row exists for (I, C, S) here, so the gate formula returns
	// no_release before it re-derives the approval basis (data-model §3 order:
	// release row first, then approvals_valid). F19 is still exercised on the
	// release path below, where a release decision exists and references the
	// approvals invalidated by this mapping conflict.
	got := t046Admit(t, f2, gate2, CapabilityExistingWithdrawalRecovery)
	if got.Allowed || got.RefusalClass != RefusalNoRelease {
		t.Fatalf("conflicting mapping admission = %+v, want %s", got, RefusalNoRelease)
	}
	rel2, err := t046Release(t, f2, gate2, CapabilityExistingWithdrawalRecovery)
	if err == nil || rel2.RefusalClass != RefusalApprovalIdentityUnverified {
		t.Fatalf("conflicting-mapping release = (%+v, %v), want %s", rel2, err, RefusalApprovalIdentityUnverified)
	}
	if got := t046Count(t, f2,
		`SELECT count(*) FROM recovery_release WHERE instance_id = $1 AND capability = 'existing_withdrawal_recovery' AND decision = 'release'`,
		f2.instanceID); got != 0 {
		t.Fatalf("conflicting-mapping release wrote %d release row(s), want 0", got)
	}
}
