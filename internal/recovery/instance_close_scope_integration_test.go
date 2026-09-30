//go:build integration

package recovery

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

// t044NewBoundService uses the normal DSN-derived target identity path. The
// generic gate fixture intentionally exercises legacy bindings, which are not
// appropriate for these lifecycle-closure cases.
func t044NewBoundService(t *testing.T, opts GateOptions) *t044Service {
	return t044NewBoundServiceKind(t, opts, "recovery", []uint64{1})
}

func t044NewBoundServiceForChains(t *testing.T, opts GateOptions, chains []uint64) *t044Service {
	return t044NewBoundServiceKind(t, opts, "recovery", chains)
}

func t044NewBoundServiceKind(t *testing.T, opts GateOptions, kind string, chains []uint64) *t044Service {
	t.Helper()
	ctx, dsn, pool, store := gateControlStore(t)
	target, err := controlstore.ParseDSNTarget(dsn)
	if err != nil {
		t.Fatalf("parse fixture target DSN: %v", err)
	}
	guardKey, err := controlstore.TargetGuardKey(target)
	if err != nil {
		t.Fatalf("derive fixture target guard key: %v", err)
	}
	dataTarget, err := json.Marshal(target.DataTargetFingerprint())
	if err != nil {
		t.Fatalf("encode fixture target fingerprint: %v", err)
	}
	opened, err := store.OpenInstance(ctx, controlstore.OpenInstanceRequest{
		Kind: kind, OpenedBy: "deploy:executor", EntryChainInventory: chains,
		DataTarget: dataTarget, TargetGuardKey: guardKey,
		TargetRoleFingerprint: target.DataTargetFingerprint().RoleFingerprint,
	})
	if err != nil {
		t.Fatalf("open DSN-bound recovery instance: %v", err)
	}
	// OpenInstance provisions an unknown guard. Establish a clearly controlled
	// test baseline through the supported audited resolution operations.
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin clean target baseline: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	op := gateOperation("bound-target-clean")
	evidence := []byte(`{"fixture":"synthetic controlled baseline; test-only"}`)
	if err := controlstore.RecordTargetGuardRebuild(ctx, tx, opened.InstanceID, guardKey, "deploy:executor", op, evidence); err != nil {
		t.Fatalf("record clean target baseline: %v", err)
	}
	if err := controlstore.ResolveTargetGuardClean(ctx, tx, guardKey, op, evidence); err != nil {
		t.Fatalf("resolve clean target baseline: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit clean target baseline: %v", err)
	}
	for _, identity := range []struct{ principal, person, role string }{
		{"deploy:admin", "person-admin", "admin"},
		{"deploy:executor", "person-executor", "executor"},
		{"auth:verifier", "person-verifier", "verifier"},
		{"auth:approver", "person-approver", "approver"},
	} {
		gateMapIdentity(t, ctx, store, identity.principal, identity.person)
		if identity.role != "admin" {
			gateRegister(t, ctx, store, opened.InstanceID, identity.principal, identity.role)
		}
	}
	f := &gateFixture{ctx: ctx, dsn: dsn, pool: pool, store: store, instanceID: opened.InstanceID}
	checklist, err := NewChecklist(store)
	if err != nil {
		t.Fatalf("NewChecklist over the real control store: %v", err)
	}
	return &t044Service{f: f, checklist: checklist, gate: gateNewGate(t, store, opts)}
}

func t044BoundTarget(t *testing.T, s *t044Service) (string, string) {
	t.Helper()
	target, err := controlstore.ParseDSNTarget(s.f.dsn)
	if err != nil {
		t.Fatalf("parse fixture target DSN: %v", err)
	}
	key, err := controlstore.TargetGuardKey(target)
	if err != nil {
		t.Fatalf("derive fixture target guard key: %v", err)
	}
	return key, target.DataTargetFingerprint().RoleFingerprint
}

func t044ReleaseAllEntryScopes(t *testing.T, s *t044Service) {
	t044ReleaseAllEntryScopesForChains(t, s, []uint64{1})
}

func t044ReleaseAllEntryScopesForChains(t *testing.T, s *t044Service, chains []uint64) {
	t.Helper()
	order := []Capability{CapabilityChainScan, CapabilityDepositConfirmation, CapabilityExistingWithdrawalRecovery, CapabilityNewWithdrawalCreation, CapabilityEventPublishing, CapabilityEventConsuming, CapabilityQuery}
	for _, capability := range order {
		s.seedIsolation(t, capability)
	}
	for _, capability := range order {
		for _, chain := range chains {
			scopeHash, err := CapabilityScope(chain, capability)
			if err != nil {
				t.Fatal(err)
			}
			scope, err := ParseCapabilityScope(scopeHash, capability)
			if err != nil {
				t.Fatal(err)
			}
			class, err := s.gate.requiredApprovalClass(scope)
			if err != nil {
				t.Fatal(err)
			}
			for i, principal := range []string{"auth:approver", "auth:approver-2"} {
				if i == 1 && class != ApprovalClassDualNonExecutor {
					break
				}
				if i == 1 {
					s.ensureSecondApprover(t)
				}
				if _, err := Approve(s.f.ctx, s.f.store, ApprovalRequest{InstanceID: s.f.instanceID, Capability: capability, ScopeHash: scopeHash, Principal: principal, Reason: "complete entry-scope approval", OperationID: gateOperation("entry-approve")}); err != nil {
					t.Fatalf("approve %s on chain %d at entry scope: %v", capability, chain, err)
				}
			}
			if _, err := Release(s.f.ctx, s.f.store, s.gate, ReleaseRequest{InstanceID: s.f.instanceID, Capability: capability, ScopeHash: scopeHash, Principal: "deploy:executor", Reason: "complete entry-scope release", OperationID: gateOperation("entry-release")}); err != nil {
				t.Fatalf("release %s on chain %d at entry scope: %v", capability, chain, err)
			}
		}
	}
}

// A complete-looking set of asset-specific decisions is still narrower than
// the chain-level scopes consumed by entry points and cannot close an instance.
func TestInstanceCloseDoesNotTreatNarrowAssetScopesAsEntryCoverage(t *testing.T) {
	s := t044NewBoundService(t, GateOptions{})
	for _, capability := range KnownCapabilities() {
		s.seedIsolation(t, capability)
		scope := Scope{ChainID: 1, Asset: "asset-7", Capability: capability}
		scopeHash, err := scope.Canonical()
		if err != nil {
			t.Fatal(err)
		}
		class, err := s.gate.requiredApprovalClass(scope)
		if err != nil {
			t.Fatal(err)
		}
		refs := []string{}
		approval, err := Approve(s.f.ctx, s.f.store, ApprovalRequest{InstanceID: s.f.instanceID, Capability: capability, ScopeHash: scopeHash, Principal: "auth:approver", Reason: "scoped approval", OperationID: gateOperation("asset-approval")})
		if err != nil {
			t.Fatalf("approve %s at narrow scope: %v", capability, err)
		}
		refs = append(refs, approval.ApprovalID)
		if class == ApprovalClassDualNonExecutor {
			s.ensureSecondApprover(t)
			second, err := Approve(s.f.ctx, s.f.store, ApprovalRequest{InstanceID: s.f.instanceID, Capability: capability, ScopeHash: scopeHash, Principal: "auth:approver-2", Reason: "scoped dual approval", OperationID: gateOperation("asset-approval-2")})
			if err != nil {
				t.Fatalf("second approve %s at narrow scope: %v", capability, err)
			}
			refs = append(refs, second.ApprovalID)
		}
		if _, err := Release(s.f.ctx, s.f.store, s.gate, ReleaseRequest{InstanceID: s.f.instanceID, Capability: capability, ScopeHash: scopeHash, Principal: "deploy:executor", Reason: "narrow release", OperationID: gateOperation("asset-release")}); err != nil {
			t.Fatalf("release %s at narrow scope: %v (approvals=%v)", capability, err, refs)
		}
	}
	guardKey, roleFingerprint := t044BoundTarget(t, s)
	result, err := CloseInstance(s.f.ctx, s.f.store, s.gate, CloseInstanceRequest{
		InstanceID: s.f.instanceID, Actor: "deploy:executor", OperationID: gateOperation("asset-close"),
		TrustedEntryChains: []uint64{1}, TargetGuardKey: guardKey, TargetRoleFingerprint: roleFingerprint,
	})
	if !errors.Is(err, ErrInstanceCloseBlocked) || result.Closed {
		t.Fatalf("close with seven asset-narrow releases = (%+v, %v), want blocked", result, err)
	}
}

func TestInstanceCloseRequiresEveryTrustedDeploymentChain(t *testing.T) {
	s := t044NewBoundService(t, GateOptions{})
	t044ReleaseAllEntryScopes(t, s)
	guardKey, roleFingerprint := t044BoundTarget(t, s)
	result, err := CloseInstance(s.f.ctx, s.f.store, s.gate, CloseInstanceRequest{
		InstanceID: s.f.instanceID, Actor: "deploy:executor", OperationID: gateOperation("missing-chain-close"),
		TrustedEntryChains: []uint64{1, 10}, TargetGuardKey: guardKey, TargetRoleFingerprint: roleFingerprint,
	})
	if !errors.Is(err, ErrInstanceCloseBlocked) || result.Closed {
		t.Fatalf("close with missing deployment chain scopes = (%+v, %v), want blocked", result, err)
	}
}

// A close must cover every scope in the immutable, deployment-supplied chain
// inventory, including dual approval on every effectful chain scope. The
// fixture inventory is test input; this proves binding/coverage, not that a
// production inventory is complete.
func TestInstanceCloseSucceedsAfterAllCapabilitiesReleasedOnEveryBoundChain(t *testing.T) {
	chains := []uint64{1, 10}
	s := t044NewBoundServiceForChains(t, GateOptions{}, chains)
	t044ReleaseAllEntryScopesForChains(t, s, chains)
	guardKey, roleFingerprint := t044BoundTarget(t, s)
	result, err := CloseInstance(s.f.ctx, s.f.store, s.gate, CloseInstanceRequest{
		InstanceID: s.f.instanceID, Actor: "deploy:executor", OperationID: gateOperation("all-two-chain-scopes-close"),
		TrustedEntryChains: chains, TargetGuardKey: guardKey, TargetRoleFingerprint: roleFingerprint,
	})
	if err != nil || !result.Closed || result.RiskAcceptanceDelivered {
		t.Fatalf("close after all 14 current capability scopes released = (%+v, %v), want closed without risk acceptance", result, err)
	}
	if got := countGenerationRows(t, s.f.ctx, s.f.pool,
		`SELECT count(*) FROM recovery_release WHERE instance_id = $1 AND decision = 'release'`, s.f.instanceID); got < len(KnownCapabilities())*len(chains) {
		t.Fatalf("recorded release scopes = %d, want at least %d (seven capabilities x two chains)", got, len(KnownCapabilities())*len(chains))
	}
}

// Prior dual releases do not make a revoked basis acceptable at close. Once
// the approval is renewed and the capability is released again, the current
// release carries the current two-person basis and close can proceed.
func TestInstanceCloseRequiresCurrentDualBasisAfterApprovalRevokeAndRenewal(t *testing.T) {
	s := t044NewBoundService(t, GateOptions{})
	t044ReleaseAllEntryScopes(t, s)
	capability := CapabilityExistingWithdrawalRecovery
	scopeHash, err := CapabilityScope(1, capability)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := RevokeApproval(s.f.ctx, s.f.store, ApprovalRequest{
		InstanceID: s.f.instanceID, Capability: capability, ScopeHash: scopeHash,
		Principal: "auth:approver", Reason: "exercise current dual basis", OperationID: gateOperation("close-history-approval-revoke"),
	}); err != nil {
		t.Fatalf("revoke first current approval: %v", err)
	}
	guardKey, roleFingerprint := t044BoundTarget(t, s)
	result, err := CloseInstance(s.f.ctx, s.f.store, s.gate, CloseInstanceRequest{
		InstanceID: s.f.instanceID, Actor: "deploy:executor", OperationID: gateOperation("close-history-blocked"),
		TrustedEntryChains: []uint64{1}, TargetGuardKey: guardKey, TargetRoleFingerprint: roleFingerprint,
	})
	if !errors.Is(err, ErrInstanceCloseBlocked) || result.Closed {
		t.Fatalf("close with revoked historical dual basis = (%+v, %v), want blocked", result, err)
	}
	if _, err := Approve(s.f.ctx, s.f.store, ApprovalRequest{
		InstanceID: s.f.instanceID, Capability: capability, ScopeHash: scopeHash,
		Principal: "auth:approver", Reason: "renew current dual basis", OperationID: gateOperation("close-history-approval-renew"),
	}); err != nil {
		t.Fatalf("renew first current approval: %v", err)
	}
	if _, err := Release(s.f.ctx, s.f.store, s.gate, ReleaseRequest{
		InstanceID: s.f.instanceID, Capability: capability, ScopeHash: scopeHash,
		Principal: "deploy:executor", Reason: "release with renewed dual basis", OperationID: gateOperation("close-history-release-renew"),
	}); err != nil {
		t.Fatalf("release with renewed dual basis: %v", err)
	}
	result, err = CloseInstance(s.f.ctx, s.f.store, s.gate, CloseInstanceRequest{
		InstanceID: s.f.instanceID, Actor: "deploy:executor", OperationID: gateOperation("close-history-renewed"),
		TrustedEntryChains: []uint64{1}, TargetGuardKey: guardKey, TargetRoleFingerprint: roleFingerprint,
	})
	if err != nil || !result.Closed {
		t.Fatalf("close after renewed current-generation dual release = (%+v, %v), want closed", result, err)
	}
}

func TestBaselineInstanceCloseIsDocumentationOnly(t *testing.T) {
	s := t044NewBoundServiceKind(t, GateOptions{}, "baseline", nil)
	guardKey, roleFingerprint := t044BoundTarget(t, s)
	result, err := CloseInstance(s.f.ctx, s.f.store, s.gate, CloseInstanceRequest{
		InstanceID: s.f.instanceID, Actor: "deploy:executor", OperationID: gateOperation("baseline-close"),
		TrustedEntryChains: []uint64{1}, TargetGuardKey: guardKey, TargetRoleFingerprint: roleFingerprint,
	})
	if err != nil || !result.Closed || !result.Baseline || result.RiskAcceptanceDelivered {
		t.Fatalf("baseline close without release decisions = (%+v, %v), want documentation-only close", result, err)
	}
}

func TestInstanceCloseCoreRejectsTargetBindingMismatch(t *testing.T) {
	s := t044NewBoundService(t, GateOptions{})
	guardKey, _ := t044BoundTarget(t, s)
	result, err := CloseInstance(s.f.ctx, s.f.store, s.gate, CloseInstanceRequest{
		InstanceID: s.f.instanceID, Actor: "deploy:executor", OperationID: gateOperation("target-mismatch-close"),
		TrustedEntryChains: []uint64{1}, TargetGuardKey: guardKey, TargetRoleFingerprint: "sha256:" + strings.Repeat("0", 64),
	})
	if err == nil || result.Closed {
		t.Fatalf("close with mismatched target role fingerprint = (%+v, %v), want refusal", result, err)
	}
	var state string
	if err := s.f.pool.QueryRow(s.f.ctx, `SELECT state FROM recovery_instance WHERE instance_id = $1`, s.f.instanceID).Scan(&state); err != nil {
		t.Fatalf("read instance state after target mismatch: %v", err)
	}
	if state != "open" {
		t.Fatalf("instance state after target mismatch = %q, want open", state)
	}
}

func TestInstanceCloseRejectsChangedInventory(t *testing.T) {
	t.Run("changed current deployment inventory", func(t *testing.T) {
		s := t044NewBoundService(t, GateOptions{})
		guardKey, roleFingerprint := t044BoundTarget(t, s)
		result, err := CloseInstance(s.f.ctx, s.f.store, s.gate, CloseInstanceRequest{
			InstanceID: s.f.instanceID, Actor: "deploy:executor", OperationID: gateOperation("changed-inventory-close"),
			TrustedEntryChains: []uint64{2}, TargetGuardKey: guardKey, TargetRoleFingerprint: roleFingerprint,
		})
		if !errors.Is(err, ErrInstanceCloseBlocked) || result.Closed {
			t.Fatalf("close after deployment inventory change = (%+v, %v), want blocked", result, err)
		}
	})
}

func TestCloseRejectsMalformedTrustedInventory(t *testing.T) {
	s := t044NewService(t, GateOptions{})
	for _, chains := range [][]uint64{nil, {}, {1, 1}, {2, 1}, {0}} {
		if _, err := CloseInstance(s.f.ctx, s.f.store, s.gate, CloseInstanceRequest{
			InstanceID: s.f.instanceID, Actor: "deploy:executor", TrustedEntryChains: chains,
		}); err == nil {
			t.Fatalf("malformed trusted inventory %v must be rejected before any close evaluation", chains)
		}
	}
}

func TestInstanceCloseCoreRequiresExecutorRole(t *testing.T) {
	s := t044NewService(t, GateOptions{})
	for _, actor := range []string{"auth:approver", "auth:verifier"} {
		result, err := CloseInstance(s.f.ctx, s.f.store, s.gate, CloseInstanceRequest{InstanceID: s.f.instanceID, Actor: actor, OperationID: gateOperation("non-executor-close"), TrustedEntryChains: []uint64{1}})
		if err == nil || result.Closed {
			t.Fatalf("direct core close by non-executor %s = (%+v, %v), want refusal", actor, result, err)
		}
	}
	if _, err := s.f.pool.Exec(s.f.ctx, `UPDATE recovery_identity SET active = false WHERE principal = 'deploy:executor'`); err != nil {
		t.Fatalf("revoke executor identity mapping for negative case: %v", err)
	}
	result, err := CloseInstance(s.f.ctx, s.f.store, s.gate, CloseInstanceRequest{InstanceID: s.f.instanceID, Actor: "deploy:executor", OperationID: gateOperation("revoked-executor-close"), TrustedEntryChains: []uint64{1}})
	if err == nil || result.Closed {
		t.Fatalf("direct core close by revoked executor identity = (%+v, %v), want refusal", result, err)
	}
}
