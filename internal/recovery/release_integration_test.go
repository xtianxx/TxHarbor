//go:build integration

// release_integration_test.go is T044 [US4]: the release / revoke / tri-state
// / idempotency integration layer against a real PostgreSQL 18.6 control store
// (tags: integration; FR-021–FR-024; SC-004/007; quickstart S8/S9 and F7;
// data-model.md §1.8/§1.9/§3/§4.2/§6; contracts/approval-matrix.md;
// tasks.md T044).
//
// TDD-first. B12 lands the tests before the B13 implementation, so this file
// references the planned T048/T049 API (internal/recovery/approvals.go and
// release.go) that does not exist yet: the integration candidate fails to
// build until B13 lands. Expected API surface (documented at every use site;
// the red-line symbols are listed in the batch report):
//
//	// internal/recovery/approvals.go (T048)
//	type ApprovalRequest struct {
//	    InstanceID  string   // open recovery instance
//	    Capability  Capability
//	    ScopeHash   string   // canonical scope; non-empty
//	    Principal   string   // authenticated caller (TXHARBOR_RECOVERY_PRINCIPAL shape)
//	    Reason      string   // audit annotation only
//	    OperationID string   // idempotency key
//	}
//	type ApprovalOutcome struct {
//	    ApprovalID    string
//	    Decision      string        // approve|revoke
//	    ApprovalClass ApprovalClass // conservative class snapshot (T050 tightens)
//	    Principal     string
//	    PersonID      string        // resolved from the active identity mapping
//	    Recorded      bool          // operation_id replay, zero writes
//	    RefusalClass  RefusalClass  // set on a refusal; the refusal is audited
//	    Reason        string
//	}
//	var ErrApprovalRefused error
//	func Approve(ctx context.Context, store *controlstore.Store, req ApprovalRequest) (ApprovalOutcome, error)
//	func RevokeApproval(ctx context.Context, store *controlstore.Store, req ApprovalRequest) (ApprovalOutcome, error)
//
//	// internal/recovery/release.go (T049)
//	type ReleaseRequest struct { /* same carriage as ApprovalRequest */ }
//	type ReleaseOutcome struct {
//	    ReleaseID    string
//	    Decision     string   // release|revoke
//	    ApprovalRefs []string // deterministic ascending refs on a release
//	    Recorded     bool
//	    RefusalClass RefusalClass
//	    Reason       string
//	}
//	var ErrReleaseRefused error
//	func Release(ctx context.Context, store *controlstore.Store, gate *Gate, req ReleaseRequest) (ReleaseOutcome, error)
//	func RevokeRelease(ctx context.Context, store *controlstore.Store, gate *Gate, req ReleaseRequest) (ReleaseOutcome, error)
//
// Semantics pinned here (all against the real control store; no test double
// stands in for the gate, the checklist or the decision writers):
//
//   - S8/S9 unauthorized approval/release is refused AND audited; an
//     unregistered principal, a principal without an active mapping and a
//     release without any valid approval produce zero decision rows;
//   - the four high-impact capabilities (existing_withdrawal_recovery,
//     new_withdrawal_creation, event_publishing, event_consuming) need two
//     approvals by two distinct principals with distinct person_ids: one
//     approval yields zero releases; the executor itself can never approve
//     (including the same person under another principal);
//   - an evidence-generation change invalidates every existing approval
//     (approval_stale) and every existing release bound to the old token;
//     re-approval plus re-release is required and converges;
//   - repeated approve/release/close calls (>= 10) with the same operation_id
//     replay with zero writes and zero state flips;
//   - revoke is explicit, authorized and audited; a revoked approval or
//     release immediately refuses the next evaluation (release_revoked);
//   - restored != verified != released (F16): restore-probe evidence alone
//     never yields verified or released; verified isolation alone and an
//     approval alone never yield a release; only the derived release
//     evaluation does.
//
// Isolation transitions are seeded through the real checklist write path
// (T028/T029), never by direct row writes; approvals/releases are written only
// through the T048/T049 API, never by direct control-store writes.
//
// Docker missing: the package TestMain (generation_integration_test.go)
// reports NOT RUN locally (exit 0) and fails the package under CI=true /
// TXHARBOR_REQUIRE_DOCKER=1 — an unrun PG layer is never a pass.
package recovery

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
)

// t044Service bundles the real scene: the gate fixture (open recovery
// instance, executor/verifier/approver participants, identity mappings), the
// checklist service and the single derived gate.
type t044Service struct {
	f         *gateFixture
	checklist *Checklist
	gate      *Gate
}

func t044NewService(t *testing.T, opts GateOptions) *t044Service {
	t.Helper()
	f := gateBaseFixture(t)
	checklist, err := NewChecklist(f.store)
	if err != nil {
		t.Fatalf("NewChecklist over the real control store: %v", err)
	}
	gate := gateNewGate(t, f.store, opts)
	return &t044Service{f: f, checklist: checklist, gate: gate}
}

// t044RequireRefused is the uniform refusal check: a sentinel-carrying error,
// a closed-set refusal class, and one additional refused audit row. The row
// count of the decision table is asserted by the caller against the concrete
// table it cares about.
func t044RequireRefused(t *testing.T, s *t044Service, sentinel error, class RefusalClass, err error, auditsBefore int, why string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: expected a refusal, got none", why)
	}
	if sentinel != nil && !errors.Is(err, sentinel) {
		t.Fatalf("%s: error = %v, want %v", why, err, sentinel)
	}
	if class != "" && !class.Known() {
		t.Fatalf("%s: refusal class %q is outside the closed set", why, class)
	}
	if got := s.refusedAudits(t); got <= auditsBefore {
		t.Fatalf("%s: refusal was not audited (refused audit rows %d -> %d)", why, auditsBefore, got)
	}
}

// ---------------------------------------------------------------------------
// Fixture helpers (real write paths only)
// ---------------------------------------------------------------------------

// seedIsolation seeds every isolation dependency item of capability through
// the real checklist path (executor collects, non-executor verifier
// confirms).
func (s *t044Service) seedIsolation(t *testing.T, capability Capability) {
	t.Helper()
	isoVerifyAll(t, s.f, s.checklist, capability)
}

// ensureSecondApprover maps and registers the second approver (idempotent
// across a fixture that already released a dual capability).
func (s *t044Service) ensureSecondApprover(t *testing.T) {
	t.Helper()
	gateMapIdentity(t, s.f.ctx, s.f.store, "auth:approver-2", "person-approver-2")
	if s.count(t, `SELECT count(*) FROM recovery_participant
	    WHERE instance_id = $1 AND principal = 'auth:approver-2' AND role = 'approver'`,
		s.f.instanceID) == 0 {
		gateRegister(t, s.f.ctx, s.f.store, s.f.instanceID, "auth:approver-2", "approver")
	}
}

// approveOK performs one approval through the T048 API and fails the test on a
// refusal.
func (s *t044Service) approveOK(t *testing.T, principal string, capability Capability) ApprovalOutcome {
	t.Helper()
	out, err := Approve(s.f.ctx, s.f.store, ApprovalRequest{
		InstanceID:  s.f.instanceID,
		Capability:  capability,
		ScopeHash:   gateScopeFor(capability),
		Principal:   principal,
		Reason:      "t044 approval",
		OperationID: gateOperation("t044-approve"),
	})
	if err != nil {
		t.Fatalf("Approve(%s, %s): %v", principal, capability, err)
	}
	if out.Decision != "approve" || out.ApprovalID == "" || out.PersonID == "" {
		t.Fatalf("Approve(%s, %s) = %+v, want a recorded approve with a resolved person", principal, capability, out)
	}
	return out
}

func (s *t044Service) approveErr(t *testing.T, principal string, capability Capability) (ApprovalOutcome, error) {
	t.Helper()
	return Approve(s.f.ctx, s.f.store, ApprovalRequest{
		InstanceID:  s.f.instanceID,
		Capability:  capability,
		ScopeHash:   gateScopeFor(capability),
		Principal:   principal,
		Reason:      "t044 refusal probe",
		OperationID: gateOperation("t044-approve-refuse"),
	})
}

func (s *t044Service) releaseOK(t *testing.T, capability Capability) ReleaseOutcome {
	t.Helper()
	out, err := Release(s.f.ctx, s.f.store, s.gate, ReleaseRequest{
		InstanceID:  s.f.instanceID,
		Capability:  capability,
		ScopeHash:   gateScopeFor(capability),
		Principal:   "deploy:executor",
		Reason:      "t044 release",
		OperationID: gateOperation("t044-release"),
	})
	if err != nil {
		t.Fatalf("Release(%s): %v", capability, err)
	}
	if out.Decision != "release" || out.ReleaseID == "" {
		t.Fatalf("Release(%s) = %+v, want a recorded release", capability, out)
	}
	return out
}

func (s *t044Service) releaseErr(t *testing.T, capability Capability) (ReleaseOutcome, error) {
	t.Helper()
	return Release(s.f.ctx, s.f.store, s.gate, ReleaseRequest{
		InstanceID:  s.f.instanceID,
		Capability:  capability,
		ScopeHash:   gateScopeFor(capability),
		Principal:   "deploy:executor",
		Reason:      "t044 refusal probe",
		OperationID: gateOperation("t044-release-refuse"),
	})
}

func (s *t044Service) revokeApprovalOK(t *testing.T, principal string, capability Capability) ApprovalOutcome {
	t.Helper()
	out, err := RevokeApproval(s.f.ctx, s.f.store, ApprovalRequest{
		InstanceID:  s.f.instanceID,
		Capability:  capability,
		ScopeHash:   gateScopeFor(capability),
		Principal:   principal,
		Reason:      "t044 explicit revoke",
		OperationID: gateOperation("t044-approve-revoke"),
	})
	if err != nil {
		t.Fatalf("RevokeApproval(%s, %s): %v", principal, capability, err)
	}
	if out.Decision != "revoke" || out.ApprovalID == "" {
		t.Fatalf("RevokeApproval(%s, %s) = %+v, want a recorded revoke", principal, capability, out)
	}
	return out
}

func (s *t044Service) revokeReleaseOK(t *testing.T, principal string, capability Capability) ReleaseOutcome {
	t.Helper()
	out, err := RevokeRelease(s.f.ctx, s.f.store, s.gate, ReleaseRequest{
		InstanceID:  s.f.instanceID,
		Capability:  capability,
		ScopeHash:   gateScopeFor(capability),
		Principal:   principal,
		Reason:      "t044 explicit release revoke",
		OperationID: gateOperation("t044-release-revoke"),
	})
	if err != nil {
		t.Fatalf("RevokeRelease(%s, %s): %v", principal, capability, err)
	}
	if out.Decision != "revoke" || out.ReleaseID == "" {
		t.Fatalf("RevokeRelease(%s, %s) = %+v, want a recorded revoke", principal, capability, out)
	}
	return out
}

// admit runs one single-action admission of capability at the fixture scope.
func (s *t044Service) admit(t *testing.T, capability Capability) GateDecision {
	t.Helper()
	decision, err := s.gate.Admit(s.f.ctx, GateRequest{
		InstanceID:  s.f.instanceID,
		Capability:  capability,
		ScopeHash:   gateScopeFor(capability),
		Actor:       "deploy:executor",
		OperationID: gateOperation("t044-admit"),
		Action:      "test:action",
	})
	if err != nil {
		t.Fatalf("Admit(%s): %v", capability, err)
	}
	return decision
}

// count runs one constant test query.
func (s *t044Service) count(t *testing.T, sql string, args ...any) int {
	t.Helper()
	return countGenerationRows(t, s.f.ctx, s.f.pool, sql, args...)
}

func (s *t044Service) refusedAudits(t *testing.T) int {
	t.Helper()
	return s.count(t, `SELECT count(*) FROM recovery_audit WHERE result = 'refused'`)
}

func (s *t044Service) approvalRows(t *testing.T, capability Capability) int {
	t.Helper()
	return s.count(t,
		`SELECT count(*) FROM recovery_approval WHERE instance_id = $1 AND capability = $2`,
		s.f.instanceID, string(capability))
}

func (s *t044Service) releaseRows(t *testing.T, capability Capability) int {
	t.Helper()
	return s.count(t,
		`SELECT count(*) FROM recovery_release WHERE instance_id = $1 AND capability = $2 AND decision = 'release'`,
		s.f.instanceID, string(capability))
}

// releaseValid performs the approvals that satisfy the conservative required
// class of capability and then releases it through the T049 API.
func (s *t044Service) releaseValid(t *testing.T, capability Capability) ReleaseOutcome {
	t.Helper()
	class, err := RequiredApprovalClass(capability)
	if err != nil {
		t.Fatalf("RequiredApprovalClass(%s): %v", capability, err)
	}
	s.approveOK(t, "auth:approver", capability)
	if class == ApprovalClassDualNonExecutor {
		s.ensureSecondApprover(t)
		s.approveOK(t, "auth:approver-2", capability)
	}
	return s.releaseOK(t, capability)
}

// releaseAllCapabilities seeds every isolation dependency set and releases all
// seven capabilities in dependency order — the S11 precondition "every
// capability validly released".
func (s *t044Service) releaseAllCapabilities(t *testing.T) {
	t.Helper()
	for _, capability := range KnownCapabilities() {
		s.seedIsolation(t, capability)
	}
	for _, capability := range KnownCapabilities() {
		s.releaseValid(t, capability)
	}
}

// ---------------------------------------------------------------------------
// S8/S9: unauthorized approval/release
// ---------------------------------------------------------------------------

func TestT044UnauthorizedApprovalAndReleaseRefusedAndAudited(t *testing.T) {
	s := t044NewService(t, GateOptions{})
	s.seedIsolation(t, CapabilityQuery)

	// A mapped principal without an approver binding on this instance cannot
	// approve: zero approval rows, one refused audit.
	audits := s.refusedAudits(t)
	out, err := s.approveErr(t, "deploy:admin", CapabilityQuery)
	t044RequireRefused(t, s, ErrApprovalRefused, out.RefusalClass, err, audits,
		"approval by a mapped principal without an approver binding")
	if got := s.approvalRows(t, CapabilityQuery); got != 0 {
		t.Fatalf("unauthorized approval wrote %d approval row(s)", got)
	}

	// A principal without any active identity mapping cannot prove a person.
	audits = s.refusedAudits(t)
	out, err = s.approveErr(t, "auth:stranger", CapabilityQuery)
	t044RequireRefused(t, s, ErrApprovalRefused, out.RefusalClass, err, audits,
		"approval by a principal with no identity mapping")
	if got := s.approvalRows(t, CapabilityQuery); got != 0 {
		t.Fatalf("unmapped approval wrote %d approval row(s)", got)
	}

	// A release without any valid approval basis refuses: no release row.
	audits = s.refusedAudits(t)
	rel, err := s.releaseErr(t, CapabilityQuery)
	t044RequireRefused(t, s, ErrReleaseRefused, rel.RefusalClass, err, audits,
		"release without approvals")
	if got := s.releaseRows(t, CapabilityQuery); got != 0 {
		t.Fatalf("approval-less release wrote %d release row(s)", got)
	}
	if got := s.admit(t, CapabilityQuery); got.Allowed || got.RefusalClass != RefusalNoRelease {
		t.Fatalf("no release must stay no_release, got %+v", got)
	}

	// An unregistered principal cannot release either.
	audits = s.refusedAudits(t)
	rel, err = Release(s.f.ctx, s.f.store, s.gate, ReleaseRequest{
		InstanceID: s.f.instanceID, Capability: CapabilityQuery, ScopeHash: gateScopeFor(CapabilityQuery),
		Principal: "auth:stranger", Reason: "unauthorized release", OperationID: gateOperation("t044-release-unauthorized"),
	})
	t044RequireRefused(t, s, ErrReleaseRefused, rel.RefusalClass, err, audits,
		"release by an unregistered principal")
	if got := s.releaseRows(t, CapabilityQuery); got != 0 {
		t.Fatalf("unauthorized release wrote %d release row(s)", got)
	}
}

// ---------------------------------------------------------------------------
// Dual approval: missing second person yields zero releases
// ---------------------------------------------------------------------------

func TestT044HighImpactCapabilitiesNeedTwoDistinctPeople(t *testing.T) {
	for _, capability := range []Capability{
		CapabilityExistingWithdrawalRecovery,
		CapabilityNewWithdrawalCreation,
		CapabilityEventPublishing,
		CapabilityEventConsuming,
	} {
		t.Run(string(capability), func(t *testing.T) {
			s := t044NewService(t, GateOptions{})
			// Seed every isolation set first (each verdict advances the
			// evidence generation), then release the dependency closure in
			// dependency order.
			closure, err := DependencyClosure(capability)
			if err != nil {
				t.Fatalf("DependencyClosure(%s): %v", capability, err)
			}
			for _, dep := range closure {
				s.seedIsolation(t, dep)
			}
			s.seedIsolation(t, capability)
			for _, dep := range closure {
				s.releaseValid(t, dep)
			}
			before := s.releaseRows(t, capability)

			// One approval only: dual required, zero releases.
			s.approveOK(t, "auth:approver", capability)
			audits := s.refusedAudits(t)
			out, err := s.releaseErr(t, capability)
			t044RequireRefused(t, s, ErrReleaseRefused, out.RefusalClass, err, audits,
				fmt.Sprintf("single approval for dual capability %s", capability))
			if got := s.releaseRows(t, capability); got != before {
				t.Fatalf("single-person release of %s wrote %d new release row(s)", capability, got-before)
			}

			// A second principal mapped to the SAME person is not a second
			// person: still zero releases.
			gateMapIdentity(t, s.f.ctx, s.f.store, "auth:approver-3", "person-approver")
			if s.count(t, `SELECT count(*) FROM recovery_participant
			    WHERE instance_id = $1 AND principal = 'auth:approver-3' AND role = 'approver'`, s.f.instanceID) == 0 {
				gateRegister(t, s.f.ctx, s.f.store, s.f.instanceID, "auth:approver-3", "approver")
			}
			s.approveOK(t, "auth:approver-3", capability)
			audits = s.refusedAudits(t)
			out, err = s.releaseErr(t, capability)
			t044RequireRefused(t, s, ErrReleaseRefused, out.RefusalClass, err, audits,
				fmt.Sprintf("same-person dual approval for %s", capability))
			if got := s.releaseRows(t, capability); got != before {
				t.Fatalf("same-person release of %s wrote %d new release row(s)", capability, got-before)
			}

			// Two different people: released, and the recorded basis carries
			// both approval refs.
			s.ensureSecondApprover(t)
			s.approveOK(t, "auth:approver-2", capability)
			rel := s.releaseOK(t, capability)
			if len(rel.ApprovalRefs) != 2 {
				t.Fatalf("dual release of %s recorded %d approval refs, want 2", capability, len(rel.ApprovalRefs))
			}
			if got := s.admit(t, capability); !got.Allowed {
				t.Fatalf("dual-released %s must be admitted, got %+v", capability, got)
			}
		})
	}
}

func TestT044ExecutorSelfApprovalIsZeroReleases(t *testing.T) {
	s := t044NewService(t, GateOptions{})
	s.seedIsolation(t, CapabilityQuery)
	// The executor is additionally bound as an approver on its own instance:
	// the executor role still excludes it (opened_by or executor binding).
	gateRegister(t, s.f.ctx, s.f.store, s.f.instanceID, "deploy:executor", "approver")

	audits := s.refusedAudits(t)
	out, err := s.approveErr(t, "deploy:executor", CapabilityQuery)
	t044RequireRefused(t, s, ErrApprovalRefused, out.RefusalClass, err, audits, "executor self-approval")
	if out.RefusalClass != RefusalApprovalExecutorExcluded {
		t.Fatalf("executor self-approval class = %q, want %q", out.RefusalClass, RefusalApprovalExecutorExcluded)
	}
	if got := s.approvalRows(t, CapabilityQuery); got != 0 {
		t.Fatalf("executor self-approval wrote %d approval row(s)", got)
	}
	if got := s.admit(t, CapabilityQuery); got.Allowed {
		t.Fatalf("executor self-approval must never release, got %+v", got)
	}
}

// ---------------------------------------------------------------------------
// Evidence change invalidates approvals and releases
// ---------------------------------------------------------------------------

func TestT044EvidenceChangeInvalidatesApprovalsAndRequiresReapproval(t *testing.T) {
	s := t044NewService(t, GateOptions{})
	s.seedIsolation(t, CapabilityQuery)
	s.releaseValid(t, CapabilityQuery)
	if got := s.admit(t, CapabilityQuery); !got.Allowed {
		t.Fatalf("legal base state must be admitted, got %+v", got)
	}

	// A real accepted evidence write advances generation+hash: the old release
	// is invalidated and the old approval is stale.
	s.f.bumpGeneration(t)
	if got := s.admit(t, CapabilityQuery); got.Allowed || got.RefusalClass != RefusalReleaseInvalidatedGeneration {
		t.Fatalf("after evidence change = %+v, want %s", got, RefusalReleaseInvalidatedGeneration)
	}
	before := s.releaseRows(t, CapabilityQuery)
	audits := s.refusedAudits(t)
	out, err := s.releaseErr(t, CapabilityQuery)
	t044RequireRefused(t, s, ErrReleaseRefused, out.RefusalClass, err, audits,
		"release with a stale approval after the evidence change")
	if out.RefusalClass != RefusalApprovalStale {
		t.Fatalf("stale-approval release class = %q, want %q", out.RefusalClass, RefusalApprovalStale)
	}
	if got := s.releaseRows(t, CapabilityQuery); got != before {
		t.Fatalf("stale-approval release wrote %d new release row(s)", got-before)
	}

	// Re-approval plus re-release converges at the new token.
	s.approveOK(t, "auth:approver", CapabilityQuery)
	s.releaseOK(t, CapabilityQuery)
	if got := s.admit(t, CapabilityQuery); !got.Allowed {
		t.Fatalf("re-approved and re-released capability must be admitted, got %+v", got)
	}
}

// ---------------------------------------------------------------------------
// Idempotency: repeated approve/release/close never flip state
// ---------------------------------------------------------------------------

func TestT044RepeatedApproveReleaseCloseDoNotFlipState(t *testing.T) {
	s := t044NewService(t, GateOptions{})
	s.releaseAllCapabilities(t)

	// Repeated approve with the same operation_id: one row, replay flagged.
	op := gateOperation("t044-idem-approve")
	var firstID string
	for i := 0; i < 10; i++ {
		out, err := Approve(s.f.ctx, s.f.store, ApprovalRequest{
			InstanceID: s.f.instanceID, Capability: CapabilityChainScan, ScopeHash: gateScopeFor(CapabilityChainScan),
			Principal: "auth:approver", Reason: "idempotent replay", OperationID: op,
		})
		if err != nil {
			t.Fatalf("idempotent Approve #%d: %v", i+1, err)
		}
		if i == 0 {
			firstID = out.ApprovalID
			if out.Recorded {
				t.Fatal("the first approve must insert, not replay")
			}
		} else if !out.Recorded || out.ApprovalID != firstID {
			t.Fatalf("replayed approve #%d = %+v, want the recorded row %s with Recorded=true", i+1, out, firstID)
		}
	}
	if got := s.count(t,
		`SELECT count(*) FROM recovery_approval WHERE instance_id = $1
		   AND principal = 'auth:approver' AND operation_id = $2`,
		s.f.instanceID, op); got != 1 {
		t.Fatalf("10 repeated approvals produced %d rows, want 1", got)
	}
	// The replay's single new approve row covers the earlier one, so the
	// chain_scan release must be re-derived: re-release it at the current
	// approval before the close below.
	s.releaseOK(t, CapabilityChainScan)
	if got := s.admit(t, CapabilityChainScan); !got.Allowed {
		t.Fatalf("chain_scan must be admitted after the re-release, got %+v", got)
	}

	// Repeated release with the same operation_id: one row, replay flagged.
	s.approveOK(t, "auth:approver", CapabilityEventPublishing)
	s.ensureSecondApprover(t)
	s.approveOK(t, "auth:approver-2", CapabilityEventPublishing)
	relOp := gateOperation("t044-idem-release")
	var releaseID string
	for i := 0; i < 10; i++ {
		out, err := Release(s.f.ctx, s.f.store, s.gate, ReleaseRequest{
			InstanceID: s.f.instanceID, Capability: CapabilityEventPublishing, ScopeHash: gateScopeFor(CapabilityEventPublishing),
			Principal: "deploy:executor", Reason: "idempotent replay", OperationID: relOp,
		})
		if err != nil {
			t.Fatalf("idempotent Release #%d: %v", i+1, err)
		}
		if i == 0 {
			releaseID = out.ReleaseID
			if out.Recorded {
				t.Fatal("the first release must insert, not replay")
			}
		} else if !out.Recorded || out.ReleaseID != releaseID {
			t.Fatalf("replayed release #%d = %+v, want the recorded row %s with Recorded=true", i+1, out, releaseID)
		}
	}
	if got := s.count(t, `SELECT count(*) FROM recovery_release WHERE instance_id = $1 AND operation_id = $2`,
		s.f.instanceID, relOp); got != 1 {
		t.Fatalf("10 repeated releases produced %d rows, want 1", got)
	}
	if got := s.admit(t, CapabilityEventPublishing); !got.Allowed {
		t.Fatalf("released capability must stay admitted after replays, got %+v", got)
	}

	// Close once (all seven released), then 10 repeated closes: zero flips.
	closeOp := gateOperation("t044-idem-close")
	result, err := CloseInstance(s.f.ctx, s.f.store, s.gate, CloseInstanceRequest{
		InstanceID: s.f.instanceID, Actor: "deploy:executor", Reason: "t044 close", OperationID: closeOp,
	})
	if err != nil || !result.Closed {
		t.Fatalf("close over all-released capabilities = (%+v, %v), want closed", result, err)
	}
	if got := s.count(t, `SELECT count(*) FROM recovery_instance WHERE instance_id = $1 AND state = 'closed'
	   AND closed_by = 'deploy:executor'`, s.f.instanceID); got != 1 {
		t.Fatalf("closure row count = %d, want 1", got)
	}
	for i := 0; i < 10; i++ {
		if _, err := CloseInstance(s.f.ctx, s.f.store, s.gate, CloseInstanceRequest{
			InstanceID: s.f.instanceID, Actor: "deploy:executor", Reason: "repeated close", OperationID: closeOp,
		}); err == nil {
			t.Fatalf("repeated close #%d succeeded; a closed instance must never re-close or flip", i+1)
		}
	}
	if got := s.count(t, `SELECT count(*) FROM recovery_instance WHERE instance_id = $1 AND state = 'closed'`,
		s.f.instanceID); got != 1 {
		t.Fatalf("instance state flipped under repeated closes (closed rows = %d)", got)
	}
	if got := s.count(t, `SELECT count(*) FROM recovery_audit
	   WHERE instance_id = $1 AND action = 'instance_close' AND result = 'ok'`, s.f.instanceID); got != 1 {
		t.Fatalf("accepted close audit rows = %d, want exactly 1", got)
	}
}

// ---------------------------------------------------------------------------
// Explicit, authorized revocation
// ---------------------------------------------------------------------------

func TestT044ExplicitAuthorizedRevocationIsAudited(t *testing.T) {
	s := t044NewService(t, GateOptions{})
	s.seedIsolation(t, CapabilityQuery)
	s.releaseValid(t, CapabilityQuery)
	if got := s.admit(t, CapabilityQuery); !got.Allowed {
		t.Fatalf("legal base state must be admitted, got %+v", got)
	}

	// Revoking the approval covers that principal's earlier approve: the next
	// evaluation refuses and the decision is audited.
	s.revokeApprovalOK(t, "auth:approver", CapabilityQuery)
	if got := s.admit(t, CapabilityQuery); got.Allowed || got.RefusalClass != RefusalApprovalMissing {
		t.Fatalf("after approval revoke = %+v, want %s", got, RefusalApprovalMissing)
	}
	if got := s.count(t,
		`SELECT count(*) FROM recovery_audit WHERE instance_id = $1 AND action = 'approval_decision' AND result = 'ok'`,
		s.f.instanceID); got < 2 {
		t.Fatalf("approve+revoke audit rows = %d, want >= 2 (revoke must be explicit and audited)", got)
	}

	// Re-approval alone does not revive the old release (the release references
	// the old approval); re-release is required.
	s.approveOK(t, "auth:approver", CapabilityQuery)
	if got := s.admit(t, CapabilityQuery); got.Allowed {
		t.Fatal("a re-approval must not silently revive a release bound to the old approval")
	}
	s.releaseOK(t, CapabilityQuery)
	if got := s.admit(t, CapabilityQuery); !got.Allowed {
		t.Fatalf("re-approved and re-released capability must be admitted, got %+v", got)
	}

	// Revoking the release: explicit, authorized, audited; the next evaluation
	// refuses with release_revoked.
	s.revokeReleaseOK(t, "deploy:executor", CapabilityQuery)
	if got := s.admit(t, CapabilityQuery); got.Allowed || got.RefusalClass != RefusalReleaseRevoked {
		t.Fatalf("after release revoke = %+v, want %s", got, RefusalReleaseRevoked)
	}
	if got := s.count(t,
		`SELECT count(*) FROM recovery_audit WHERE instance_id = $1 AND action = 'release_decision' AND result = 'ok'`,
		s.f.instanceID); got < 2 {
		t.Fatalf("release+revoke audit rows = %d, want >= 2", got)
	}

	// An unauthorized principal cannot revoke.
	rowsBefore := s.count(t, `SELECT count(*) FROM recovery_release WHERE instance_id = $1`, s.f.instanceID)
	audits := s.refusedAudits(t)
	if _, err := RevokeRelease(s.f.ctx, s.f.store, s.gate, ReleaseRequest{
		InstanceID: s.f.instanceID, Capability: CapabilityQuery, ScopeHash: gateScopeFor(CapabilityQuery),
		Principal: "auth:stranger", Reason: "unauthorized revoke", OperationID: gateOperation("t044-revoke-unauthorized"),
	}); err == nil {
		t.Fatal("an unregistered principal must not revoke a release")
	}
	if got := s.count(t, `SELECT count(*) FROM recovery_release WHERE instance_id = $1`, s.f.instanceID); got != rowsBefore {
		t.Fatalf("unauthorized revoke wrote %d row(s)", got-rowsBefore)
	}
	if got := s.refusedAudits(t); got <= audits {
		t.Fatal("unauthorized revoke was not audited")
	}
}

// ---------------------------------------------------------------------------
// F16: restored != verified != released
// ---------------------------------------------------------------------------

func TestT044RestoredVerifiedReleasedDoNotImpersonate(t *testing.T) {
	s := t044NewService(t, GateOptions{})

	// restored: real restore-probe evidence accepted through the T013
	// protocol. It never implies verified or released.
	t044InsertRestoreProbe(t, s)
	if got := s.count(t,
		`SELECT count(*) FROM recovery_evidence WHERE instance_id = $1 AND kind = 'restore_probe'`,
		s.f.instanceID); got != 1 {
		t.Fatalf("restore-probe evidence rows = %d, want 1", got)
	}
	if got := s.count(t, `SELECT count(*) FROM recovery_verification_item WHERE instance_id = $1`,
		s.f.instanceID); got != 0 {
		t.Fatalf("restore-probe evidence alone produced %d verification items; restored must not show as verified", got)
	}
	if got := s.admit(t, CapabilityQuery); got.Allowed {
		t.Fatalf("restore-probe evidence alone must not release anything, got %+v", got)
	}
	if got := s.releaseRows(t, CapabilityQuery); got != 0 {
		t.Fatalf("restore-probe evidence alone produced %d release rows", got)
	}

	// verified isolation + no release: still not released.
	s.seedIsolation(t, CapabilityQuery)
	if got := s.admit(t, CapabilityQuery); got.Allowed || got.RefusalClass != RefusalNoRelease {
		t.Fatalf("verified isolation without a release = %+v, want %s", got, RefusalNoRelease)
	}

	// approved (approval recorded) is not released either.
	s.approveOK(t, "auth:approver", CapabilityQuery)
	if got := s.admit(t, CapabilityQuery); got.Allowed || got.RefusalClass != RefusalNoRelease {
		t.Fatalf("an approval without a release = %+v, want %s (approved is not a state)", got, RefusalNoRelease)
	}
	if got := s.releaseRows(t, CapabilityQuery); got != 0 {
		t.Fatalf("an approval alone produced %d release rows", got)
	}

	// released: only the derived release evaluation allows.
	s.releaseOK(t, CapabilityQuery)
	got := s.admit(t, CapabilityQuery)
	if !got.Allowed {
		t.Fatalf("released capability must be admitted, got %+v", got)
	}
	// The release is derived per evaluation, never a writable boolean.
	if n := s.count(t, `
SELECT count(*) FROM information_schema.columns
WHERE table_name = 'recovery_release' AND column_name IN ('released', 'valid', 'approved')`); n != 0 {
		t.Fatalf("recovery_release carries %d writable validity column(s); INV-2 requires derivation", n)
	}
}

// t044InsertRestoreProbe commits one accepted restore_probe evidence row
// through the real generation protocol (the same write model restore.go uses).
func t044InsertRestoreProbe(t *testing.T, s *t044Service) {
	t.Helper()
	token, err := CaptureEvidenceToken(s.f.ctx, s.f.pool, s.f.instanceID)
	if err != nil {
		t.Fatalf("capture evidence token: %v", err)
	}
	outcome, err := CommitEvidenceWrite(s.f.ctx, s.f.store, EvidenceWriteRequest{
		InstanceID:  s.f.instanceID,
		Token:       token,
		Kind:        MutationRestoreProbeAccepted,
		Actor:       "deploy:executor",
		Reason:      "t044 restore probe fixture",
		OperationID: gateOperation("t044-restore-probe"),
		Apply: func(ctx context.Context, tx pgx.Tx, accepted EvidenceToken) error {
			_, err := tx.Exec(ctx, `
INSERT INTO recovery_evidence
    (evidence_id, instance_id, generation, kind, scope, artifact_hash, artifact_ref, observed_at, collected_by)
VALUES (gen_random_uuid(), $1, $2, 'restore_probe', '{}'::jsonb, 'sha256:t044-probe', 't044-restore-probe', now(), $3)`,
				accepted.InstanceID, accepted.Generation, "deploy:executor")
			return err
		},
	})
	if err != nil {
		t.Fatalf("commit restore-probe evidence: %v", err)
	}
	if outcome.Discarded {
		t.Fatalf("restore-probe evidence write was discarded: %s", outcome.DiscardReason)
	}
}
