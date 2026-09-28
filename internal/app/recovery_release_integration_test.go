//go:build integration

// recovery_release_integration_test.go is T045 [US4]: the real-entry-point
// release acceptance of the 015 resumption gate (tags: integration;
// FR-021/022/023/025; quickstart S8/S9 and the positive hard assertion; T050
// owns the canonical scope vocabulary, this file uses the current stable
// scopes the production wiring reads).
//
// TDD-first. B12 lands the tests before the B13 implementation, so this file
// references the planned T048/T049 API (internal/recovery/approvals.go and
// release.go) that does not exist until B13: the integration candidate fails
// to build until then. Expected API surface:
//
//	// internal/recovery (T048/T049)
//	type ApprovalRequest struct { InstanceID string; Capability Capability; ScopeHash, Principal, Reason, OperationID string }
//	type ApprovalOutcome struct { ApprovalID, Decision string; ApprovalClass ApprovalClass; Principal, PersonID string; Recorded bool; RefusalClass RefusalClass; Reason string }
//	type ReleaseRequest struct { InstanceID string; Capability Capability; ScopeHash, Principal, Reason, OperationID string }
//	type ReleaseOutcome struct { ReleaseID, Decision string; ApprovalRefs []string; Recorded bool; RefusalClass RefusalClass; Reason string }
//	func Approve(ctx context.Context, store *controlstore.Store, req ApprovalRequest) (ApprovalOutcome, error)
//	func RevokeApproval(ctx context.Context, store *controlstore.Store, req ApprovalRequest) (ApprovalOutcome, error)
//	func Release(ctx context.Context, store *controlstore.Store, gate *Gate, req ReleaseRequest) (ReleaseOutcome, error)
//	func RevokeRelease(ctx context.Context, store *controlstore.Store, gate *Gate, req ReleaseRequest) (ReleaseOutcome, error)
//
// What is pinned against the real serve process and a real control store:
//
//   - before S8 every real entry point refuses: POST /withdrawals is refused
//     at HTTP admission with a release-absence class (no_release family) and
//     zero withdrawal rows are created;
//   - a single non-executor approval + release of `query` opens only the read
//     paths (GET /withdrawals/{id} reaches the original 007 auth and answers
//     401 instead of 503) while the write path still refuses;
//   - chain_scan -> deposit_confirmation -> existing_withdrawal_recovery are
//     released one by one in dependency order, each with its own approvals;
//     new_withdrawal_creation stays refused until it is itself approved and
//     released — and after its dependencies are released the refusal class is
//     exactly no_release;
//   - releasing a capability never unlocks or replaces the existing gates: a
//     released read path still answers the original 007 authentication
//     decision, a released execution write path still answers the original
//     011 authentication decision, and a released POST /withdrawals still
//     hits the original decode/auth order (never a recovery refusal);
//   - INV-2: there is no writable release boolean; the release is re-derived
//     on every admission, so an explicit revoke immediately refuses again.
//
// Isolation transitions are seeded through the real checklist write path and
// approvals/releases only through the T048/T049 API — never by direct writes.
// The serve process runs the real production assembly (config keys →
// version-guarded control store → T012 gate), bound to the open instance.
//
// Docker missing: the package TestMain (app_shared_pg_test.go) reports NOT RUN
// locally (exit 0) and fails the package under CI=true /
// TXHARBOR_REQUIRE_DOCKER=1 — an unrun PG layer is never a pass.
package app

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/recovery"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

const (
	// t045ChainID mirrors the serve environment chain id.
	t045ChainID = 31337

	t045Verifier  = "auth:t045-verifier"
	t045Approver1 = "auth:t045-approver-1"
	t045Approver2 = "auth:t045-approver-2"
)

var t045OpSeq atomic.Int64

func t045Op(prefix string) string {
	return fmt.Sprintf("t045-%s-%d", prefix, t045OpSeq.Add(1))
}

// t045ServeScope is the serve surface scope the production wiring reads for
// the /withdrawals and /nonce/bindings entry points (serveRecoveryScope).
func t045ServeScope() string { return fmt.Sprintf("chain=%d;surface=serve", t045ChainID) }

// t045ExistingScope is the existing_withdrawal_recovery entry family scope
// (existingWithdrawalRecoveryScope) used by the execution write path.
func t045ExistingScope() string {
	return fmt.Sprintf("chain=%d;capability=existing_withdrawal_recovery", t045ChainID)
}

// t045ScopeFor maps a capability onto the scope its production entry point
// reads.
func t045ScopeFor(capability recovery.Capability) string {
	if capability == recovery.CapabilityExistingWithdrawalRecovery {
		return t045ExistingScope()
	}
	return t045ServeScope()
}

// t045Scene is the T026 scene plus the checklist service and the single
// derived gate over the same control store.
type t045Scene struct {
	*recovScene
	checklist *recovery.Checklist
	gate      *recovery.Gate
}

func t045Map(t *testing.T, s *t045Scene, principal, person string) {
	t.Helper()
	if _, err := s.store.SetIdentityMapping(s.ctx, controlstore.SetIdentityMappingRequest{
		Principal: principal, PersonID: person, RecordedBy: "deploy:t045-admin", OperationID: t045Op("map"),
	}); err != nil {
		t.Fatalf("map %s -> %s: %v", principal, person, err)
	}
}

func t045Register(t *testing.T, s *t045Scene, principal, role string) {
	t.Helper()
	if _, err := s.store.RegisterParticipant(s.ctx, controlstore.RegisterParticipantRequest{
		InstanceID: s.instanceID, Principal: principal, Role: role,
		Actor: "deploy:t045-admin", OperationID: t045Op("register"),
	}); err != nil {
		t.Fatalf("register %s as %s: %v", principal, role, err)
	}
}

// t045NewScene boots the real scene, binds the explicit identity mappings and
// participants (executor/verifier/two approvers with distinct people), opens
// the recovery instance, and builds the checklist service and the derived
// gate.
func t045NewScene(t *testing.T) *t045Scene {
	t.Helper()
	scene := recovNewScene(t, true)
	s := &t045Scene{recovScene: scene}
	t045Map(t, s, recovPrincipal, "person-t045-executor")
	t045Map(t, s, t045Verifier, "person-t045-verifier")
	t045Map(t, s, t045Approver1, "person-t045-approver-1")
	t045Map(t, s, t045Approver2, "person-t045-approver-2")
	scene.openRecoveryInstance(t)

	t045Register(t, s, recovPrincipal, "executor")
	t045Register(t, s, t045Verifier, "verifier")
	t045Register(t, s, t045Approver1, "approver")
	t045Register(t, s, t045Approver2, "approver")

	checklist, err := recovery.NewChecklist(scene.store)
	if err != nil {
		t.Fatalf("NewChecklist: %v", err)
	}
	gate, err := recovery.NewGate(scene.store, recovery.GateOptions{TTL: 30 * time.Second})
	if err != nil {
		t.Fatalf("NewGate: %v", err)
	}
	s.checklist = checklist
	s.gate = gate
	return s
}

// seedIsolation records evidence (executor) and the non-executor verdict
// (verifier) for every isolation dependency item of capability, through the
// real checklist path. Every verdict advances the evidence generation, so all
// seeding must complete before any approval/release.
func (s *t045Scene) seedIsolation(t *testing.T, capability recovery.Capability) {
	t.Helper()
	items, err := recovery.IsolationDependencySet(capability)
	if err != nil {
		t.Fatalf("IsolationDependencySet(%s): %v", capability, err)
	}
	for _, item := range items {
		if _, found, err := s.checklist.Item(s.ctx, s.instanceID, item); err != nil {
			t.Fatalf("checklist.Item(%s): %v", item, err)
		} else if !found {
			if _, err := s.checklist.Set(s.ctx, recovery.ChecklistEvidenceRequest{
				InstanceID: s.instanceID, ItemKey: item, State: recovery.ChecklistStateEvidenced,
				EvidenceRef: "evidence://015/t045/" + string(item), Actor: recovPrincipal, OperationID: t045Op("iso-set"),
			}); err != nil {
				t.Fatalf("checklist.Set(%s): %v", item, err)
			}
		}
		if _, err := s.checklist.Verify(s.ctx, recovery.ChecklistVerifyRequest{
			InstanceID: s.instanceID, ItemKey: item, Actor: t045Verifier, OperationID: t045Op("iso-verify"),
		}); err != nil {
			t.Fatalf("checklist.Verify(%s): %v", item, err)
		}
	}
}

func (s *t045Scene) approveAt(t *testing.T, principal string, capability recovery.Capability, scope string) recovery.ApprovalOutcome {
	t.Helper()
	out, err := recovery.Approve(s.ctx, s.store, recovery.ApprovalRequest{
		InstanceID: s.instanceID, Capability: capability, ScopeHash: scope,
		Principal: principal, Reason: "t045 approval", OperationID: t045Op("approve"),
	})
	if err != nil {
		t.Fatalf("Approve(%s, %s @ %s): %v", principal, capability, scope, err)
	}
	return out
}

func (s *t045Scene) approve(t *testing.T, principal string, capability recovery.Capability) recovery.ApprovalOutcome {
	t.Helper()
	return s.approveAt(t, principal, capability, t045ScopeFor(capability))
}

func (s *t045Scene) releaseAt(t *testing.T, capability recovery.Capability, scope string) recovery.ReleaseOutcome {
	t.Helper()
	out, err := recovery.Release(s.ctx, s.store, s.gate, recovery.ReleaseRequest{
		InstanceID: s.instanceID, Capability: capability, ScopeHash: scope,
		Principal: recovPrincipal, Reason: "t045 release", OperationID: t045Op("release"),
	})
	if err != nil {
		t.Fatalf("Release(%s @ %s): %v", capability, scope, err)
	}
	return out
}

func (s *t045Scene) release(t *testing.T, capability recovery.Capability) recovery.ReleaseOutcome {
	t.Helper()
	return s.releaseAt(t, capability, t045ScopeFor(capability))
}

// releaseValidAt performs the approvals required by the conservative class of
// capability at scope and releases it there.
func (s *t045Scene) releaseValidAt(t *testing.T, capability recovery.Capability, scope string) recovery.ReleaseOutcome {
	t.Helper()
	class, err := recovery.RequiredApprovalClass(capability)
	if err != nil {
		t.Fatalf("RequiredApprovalClass(%s): %v", capability, err)
	}
	s.approveAt(t, t045Approver1, capability, scope)
	if class == recovery.ApprovalClassDualNonExecutor {
		s.approveAt(t, t045Approver2, capability, scope)
	}
	return s.releaseAt(t, capability, scope)
}

// releaseValid performs the approvals required by the conservative class of
// capability and releases it at the scope of its production entry point.
func (s *t045Scene) releaseValid(t *testing.T, capability recovery.Capability) recovery.ReleaseOutcome {
	t.Helper()
	return s.releaseValidAt(t, capability, t045ScopeFor(capability))
}

// admit evaluates one capability through the same derived gate the process
// uses, at the scope of its production entry point.
func (s *t045Scene) admit(t *testing.T, capability recovery.Capability) recovery.GateDecision {
	t.Helper()
	decision, err := s.gate.Admit(s.ctx, recovery.GateRequest{
		InstanceID: s.instanceID, Capability: capability, ScopeHash: t045ScopeFor(capability),
		Actor: recovPrincipal, OperationID: t045Op("admit"), Action: "test:t045",
	})
	if err != nil {
		t.Fatalf("Admit(%s): %v", capability, err)
	}
	return decision
}

// t045StartServe starts the real serve process bound to the open instance.
func t045StartServe(t *testing.T, s *t045Scene, addr string) *recovServeHandle {
	t.Helper()
	env := s.serveEnv(addr)
	env[config.EnvRecoveryInstance] = s.instanceID
	h := recovStartServe(t, env, addr)
	h.waitReady(90 * time.Second)
	return h
}

// t045NoRecoveryRefusal fails when a response carries a closed-set recovery
// refusal; the released entry point must answer with its original gate.
func t045NoRecoveryRefusal(t *testing.T, status int, body, what string) {
	t.Helper()
	if class, ok := recovRefusalClassFromBody(body); ok {
		t.Fatalf("%s: got recovery refusal %q (status=%d body=%s); a released capability must reach the original gate", what, class, status, recovTruncate(body))
	}
	if status >= 500 {
		t.Fatalf("%s: status=%d body=%s; a released capability must not fail server-side", what, status, recovTruncate(body))
	}
}

func t045ReleaseRows(t *testing.T, s *t045Scene) int {
	t.Helper()
	return recovCount(t, s.ctx, s.controlPool,
		`SELECT count(*) FROM recovery_release WHERE instance_id = $1 AND decision = 'release'`, s.instanceID)
}

// ---------------------------------------------------------------------------
// S8-prior denial and the single-query release
// ---------------------------------------------------------------------------

func TestT045PostWithdrawalsRefusedUntilQueryReleased(t *testing.T) {
	s := t045NewScene(t)
	s.seedIsolation(t, recovery.CapabilityQuery)
	s.seedIsolation(t, recovery.CapabilityChainScan)

	addr := freeAddr(t)
	h := t045StartServe(t, s, addr)
	defer h.stop()

	// Before S8: POST /withdrawals is refused at admission with a
	// release-absence class (no_release family: no_release itself, or the
	// dependency-closed wrapper while a required dependency is unreleased),
	// and no withdrawal row exists.
	before := recovRefusedAuditCount(t, s.recovScene)
	status, body := recovHTTP(t, http.MethodPost, h.base()+"/withdrawals", strings.NewReader("{"), "application/json")
	class := recovRequireRefusal(t, status, body, "", "POST /withdrawals before S8")
	if class != recovery.RefusalNoRelease && class != recovery.RefusalCapabilityDependencyClosed {
		t.Fatalf("POST /withdrawals before S8 refusal = %q, want a release-absence class (no_release family)", class)
	}
	if n := recovCount(t, s.ctx, s.dataPool, `SELECT count(*) FROM withdrawal_requests`); n != 0 {
		t.Fatalf("withdrawal_requests = %d after the refusal, want 0", n)
	}
	if after := recovRefusedAuditCount(t, s.recovScene); after <= before {
		t.Fatalf("the refusal was not audited (before=%d after=%d)", before, after)
	}

	// S8: one non-executor approval + release of query.
	s.approve(t, t045Approver1, recovery.CapabilityQuery)
	s.release(t, recovery.CapabilityQuery)

	// The read path now reaches the original 007 authentication decision
	// (401 without a bearer credential) instead of a recovery refusal.
	status, body = recovHTTP(t, http.MethodGet, h.base()+"/withdrawals/t045-absent", nil, "")
	t045NoRecoveryRefusal(t, status, body, "GET /withdrawals/{id} after the query release")
	if status != http.StatusUnauthorized {
		t.Fatalf("GET /withdrawals/{id} after the query release = %d %s, want the original 401", status, recovTruncate(body))
	}

	// The write path still refuses: the query release must not open new
	// withdrawal creation.
	status, body = recovHTTP(t, http.MethodPost, h.base()+"/withdrawals", strings.NewReader("{"), "application/json")
	recovRequireRefusal(t, status, body, "", "POST /withdrawals after only the query release")

	if got := s.admit(t, recovery.CapabilityQuery); !got.Allowed {
		t.Fatalf("query must be admitted after its release, got %+v", got)
	}
	if got := s.admit(t, recovery.CapabilityNewWithdrawalCreation); got.Allowed {
		t.Fatalf("new_withdrawal_creation must stay refused, got %+v", got)
	}
}

// ---------------------------------------------------------------------------
// S9: progressive release in dependency order
// ---------------------------------------------------------------------------

func TestT045ProgressiveReleaseByDependencyOrder(t *testing.T) {
	s := t045NewScene(t)
	// Seed every isolation set first: each verdict advances the evidence
	// generation, so seeding after a release would invalidate it.
	for _, capability := range []recovery.Capability{
		recovery.CapabilityQuery,
		recovery.CapabilityChainScan,
		recovery.CapabilityDepositConfirmation,
		recovery.CapabilityExistingWithdrawalRecovery,
	} {
		s.seedIsolation(t, capability)
	}

	addr := freeAddr(t)
	h := t045StartServe(t, s, addr)
	defer h.stop()

	// chain_scan (single non-executor approval).
	s.approve(t, t045Approver1, recovery.CapabilityChainScan)
	s.release(t, recovery.CapabilityChainScan)
	if got := s.admit(t, recovery.CapabilityChainScan); !got.Allowed {
		t.Fatalf("chain_scan must be admitted after its release, got %+v", got)
	}
	if got := s.admit(t, recovery.CapabilityDepositConfirmation); got.Allowed || got.RefusalClass != recovery.RefusalNoRelease {
		t.Fatalf("deposit_confirmation before its release = %+v, want %s (its dependency chain_scan is released)", got, recovery.RefusalNoRelease)
	}

	// deposit_confirmation depends on chain_scan (released).
	s.approve(t, t045Approver1, recovery.CapabilityDepositConfirmation)
	s.release(t, recovery.CapabilityDepositConfirmation)
	if got := s.admit(t, recovery.CapabilityDepositConfirmation); !got.Allowed {
		t.Fatalf("deposit_confirmation must be admitted after its release, got %+v", got)
	}

	// existing_withdrawal_recovery is dual: one approval yields zero releases.
	// It is released at the serve scope (the new_withdrawal_creation closure
	// check reads the request scope) and at its own execution entry scope.
	s.approveAt(t, t045Approver1, recovery.CapabilityExistingWithdrawalRecovery, t045ServeScope())
	rowsBefore := t045ReleaseRows(t, s)
	if _, err := recovery.Release(s.ctx, s.store, s.gate, recovery.ReleaseRequest{
		InstanceID: s.instanceID, Capability: recovery.CapabilityExistingWithdrawalRecovery, ScopeHash: t045ServeScope(),
		Principal: recovPrincipal, Reason: "one-person attempt", OperationID: t045Op("release-single"),
	}); err == nil {
		t.Fatal("existing_withdrawal_recovery with a single approval must not release")
	}
	if got := t045ReleaseRows(t, s); got != rowsBefore {
		t.Fatalf("single-person attempt wrote %d release row(s)", got-rowsBefore)
	}
	s.approveAt(t, t045Approver2, recovery.CapabilityExistingWithdrawalRecovery, t045ServeScope())
	s.releaseAt(t, recovery.CapabilityExistingWithdrawalRecovery, t045ServeScope())
	// The execution write path's own entry scope.
	s.releaseValidAt(t, recovery.CapabilityExistingWithdrawalRecovery, t045ExistingScope())
	if got := s.admit(t, recovery.CapabilityExistingWithdrawalRecovery); !got.Allowed {
		t.Fatalf("existing_withdrawal_recovery must be admitted after its dual release, got %+v", got)
	}

	// new_withdrawal_creation is not released: with its dependency closure
	// released the refusal is exactly no_release, and POST /withdrawals stays
	// refused with no row created.
	status, body := recovHTTP(t, http.MethodPost, h.base()+"/withdrawals", strings.NewReader("{"), "application/json")
	recovRequireRefusal(t, status, body, recovery.RefusalNoRelease, "POST /withdrawals after the dependency chain is released")
	if n := recovCount(t, s.ctx, s.dataPool, `SELECT count(*) FROM withdrawal_requests`); n != 0 {
		t.Fatalf("withdrawal_requests = %d after refused POSTs, want 0", n)
	}
}

// ---------------------------------------------------------------------------
// Release does not unlock the original gates; INV-2 is derived
// ---------------------------------------------------------------------------

func TestT045ReleaseDoesNotUnlockOriginalGates(t *testing.T) {
	s := t045NewScene(t)
	for _, capability := range recovery.KnownCapabilities() {
		s.seedIsolation(t, capability)
	}
	// Release every capability at the serve scope, plus the execution write
	// path's own entry scope for existing_withdrawal_recovery.
	for _, capability := range recovery.KnownCapabilities() {
		s.releaseValidAt(t, capability, t045ServeScope())
	}
	s.releaseValidAt(t, recovery.CapabilityExistingWithdrawalRecovery, t045ExistingScope())
	expectedReleases := len(recovery.KnownCapabilities()) + 1
	if got := t045ReleaseRows(t, s); got != expectedReleases {
		t.Fatalf("release rows = %d, want %d (all seven released, existing at both entry scopes)", got, expectedReleases)
	}

	addr := freeAddr(t)
	h := t045StartServe(t, s, addr)
	defer h.stop()

	// Two-phase authority (FR-025): a released capability does not unlock the
	// original fund gates. The phase-two call point still refuses with
	// hard_gate_active while the recovery release is valid.
	failingGate, err := recovery.NewGate(s.store, recovery.GateOptions{
		TTL: 30 * time.Second,
		FundGates: func(context.Context, recovery.GateRequest) error {
			return fmt.Errorf("capacity red line active")
		},
	})
	if err != nil {
		t.Fatalf("NewGate with the phase-two fund gate: %v", err)
	}
	phaseTwo, err := failingGate.Admit(s.ctx, recovery.GateRequest{
		InstanceID: s.instanceID, Capability: recovery.CapabilityQuery, ScopeHash: t045ServeScope(),
		Actor: recovPrincipal, OperationID: t045Op("phase-two"), Action: "test:t045-phase-two",
	})
	if err != nil {
		t.Fatalf("phase-two admission: %v", err)
	}
	if phaseTwo.Allowed || phaseTwo.RefusalClass != recovery.RefusalHardGateActive {
		t.Fatalf("released capability with an active fund gate = %+v, want %s (release never replaces phase two)", phaseTwo, recovery.RefusalHardGateActive)
	}

	// The released read path answers the original 007 authentication decision.
	status, body := recovHTTP(t, http.MethodGet, h.base()+"/withdrawals/t045-absent", nil, "")
	t045NoRecoveryRefusal(t, status, body, "released GET /withdrawals/{id}")
	if status != http.StatusUnauthorized {
		t.Fatalf("released GET /withdrawals/{id} = %d %s, want the original 401", status, recovTruncate(body))
	}

	// The released write path reaches the original decode/auth order: a
	// malformed body is no longer a recovery refusal.
	status, body = recovHTTP(t, http.MethodPost, h.base()+"/withdrawals", strings.NewReader("{"), "application/json")
	t045NoRecoveryRefusal(t, status, body, "released POST /withdrawals (malformed body)")
	if status < 400 {
		t.Fatalf("a malformed released POST /withdrawals = %d %s, want the original refusal", status, recovTruncate(body))
	}

	// The execution write path has its own admission (existing_withdrawal_
	// recovery); it is released, so the original 011 authentication decides.
	env := s.serveEnv(addr)
	env[config.EnvRecoveryInstance] = s.instanceID
	previous := withdrawalExecutionRecoveryLoad
	withdrawalExecutionRecoveryLoad = func(ctx context.Context, chainID uint64, _ func(string) (string, bool)) (*existingWithdrawalRecoveryWiring, error) {
		cfg, err := existingWithdrawalRecoveryConfigFromEnv(envGetter(env), chainID)
		if err != nil {
			return nil, err
		}
		return assembleExistingWithdrawalRecovery(ctx, cfg, envGetter(env))
	}
	defer func() { withdrawalExecutionRecoveryLoad = previous }()
	status, body = recovHTTP(t, http.MethodPost, h.base()+"/withdrawals/t045-exec/execution", nil, "")
	t045NoRecoveryRefusal(t, status, body, "released POST /withdrawals/{id}/execution")
	if status != http.StatusUnauthorized {
		t.Fatalf("released execution write path = %d %s, want the original 401 (release must not bypass 011 auth)", status, recovTruncate(body))
	}

	// Nonce read path: released query still answers the original 008 decision.
	status, body = recovHTTP(t, http.MethodGet, h.base()+"/nonce/bindings/t045-absent", nil, "")
	t045NoRecoveryRefusal(t, status, body, "released GET /nonce/bindings/{id}")

	// INV-2: no writable release boolean exists, and an explicit revoke makes
	// the very next admission refuse again — validity is derived per request.
	if n := recovCount(t, s.ctx, s.controlPool, `
SELECT count(*) FROM information_schema.columns
WHERE table_name = 'recovery_release' AND column_name IN ('released', 'valid', 'approved')`); n != 0 {
		t.Fatalf("recovery_release carries %d writable validity column(s)", n)
	}
	if _, err := recovery.RevokeRelease(s.ctx, s.store, s.gate, recovery.ReleaseRequest{
		InstanceID: s.instanceID, Capability: recovery.CapabilityQuery, ScopeHash: t045ServeScope(),
		Principal: recovPrincipal, Reason: "t045 explicit revoke", OperationID: t045Op("release-revoke"),
	}); err != nil {
		t.Fatalf("RevokeRelease(query): %v", err)
	}
	status, body = recovHTTP(t, http.MethodGet, h.base()+"/withdrawals/t045-absent", nil, "")
	revokedClass, ok := recovRefusalClassFromBody(body)
	if !ok || revokedClass != recovery.RefusalReleaseRevoked {
		t.Fatalf("revoked GET /withdrawals/{id} = %d %s, want a %s refusal exposing the closed class", status, recovTruncate(body), recovery.RefusalReleaseRevoked)
	}
	if got, _ := recovClassInText(body); got != recovery.RefusalReleaseRevoked {
		t.Fatalf("revoked GET /withdrawals/{id} exposes %q in text, want %q", got, recovery.RefusalReleaseRevoked)
	}
	if got := t045ReleaseRows(t, s); got != expectedReleases {
		t.Fatalf("release rows = %d after the revoke, want %d release rows plus the revoke row (append-only)", got, expectedReleases)
	}
}
