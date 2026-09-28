//go:build integration

// gaps_integration_test.go is T037 [US3]: the evidence-gap / independence /
// escalation integration layer (tags: integration; real PostgreSQL control
// store; FR-019; data-model.md §1.6/§3/§4.3; contracts/verification-items.md
// §2; quickstart S7/F6; tasks.md T037).
//
// TDD-first. B10 lands the tests before the B11 implementation, so this file
// references the planned T041/T038 API (internal/recovery/gaps.go) that does
// not exist yet and the integration candidate fails to build until B11 lands.
// Expected API surface (documented at every use site):
//
//	// internal/recovery/gaps.go (T041)
//	type GapState string
//	const (
//	    GapStateOpen      GapState = "open"
//	    GapStateClosed    GapState = "closed"
//	    GapStateEscalated GapState = "escalated"
//	)
//	func (s GapState) Known() bool
//
//	type Gap struct {
//	    GapID                string
//	    InstanceID           string
//	    ObjectKey            string
//	    Scope                []byte
//	    Timeline             []byte
//	    ExistingEvidence     []byte
//	    RequiredEvidence     []byte
//	    Risk                 []byte
//	    AffectedCapabilities []Capability
//	    DependencyProof      []byte
//	    State                GapState
//	    ClosedBy             string
//	    ClosedAt             time.Time
//	    ClosureEvidence      []byte
//	    Owner                string
//	    EscalationRef        string
//	    CreatedAt            time.Time
//	}
//
//	type OpenGapRequest struct {
//	    InstanceID           string
//	    ObjectKey            string
//	    Scope                []byte
//	    Timeline             []byte
//	    ExistingEvidence     []byte
//	    RequiredEvidence     []byte
//	    Risk                 []byte
//	    AffectedCapabilities []Capability
//	    DependencyProof      []byte // nil/blank = no independence proven
//	    Owner                string
//	    Actor                string
//	    OperationID          string
//	}
//	type CloseGapRequest struct {
//	    InstanceID, GapID string
//	    ClosureEvidence   []byte // required: new evidence; empty refuses
//	    Actor, OperationID, Reason string
//	}
//	type EscalateGapRequest struct {
//	    InstanceID, GapID string
//	    EscalationRef     string // required
//	    Actor, OperationID, Reason string
//	}
//	type GapNoteKind string
//	const (
//	    GapNoteTimeout           GapNoteKind = "timeout"
//	    GapNoteAttemptsExhausted GapNoteKind = "attempts_exhausted"
//	    GapNoteAcknowledged      GapNoteKind = "acknowledged"
//	)
//	type GapNoteRequest struct {
//	    InstanceID, GapID string
//	    Kind              GapNoteKind
//	    Reason, Actor, OperationID string
//	}
//	type ReviewBudget struct{ MaxReads int }
//	type GapReviewRequest struct {
//	    InstanceID, GapID string
//	    Budget            ReviewBudget
//	    Actor, OperationID string
//	}
//	func NewGaps(store *controlstore.Store) (*Gaps, error)
//	func (g *Gaps) Open(ctx context.Context, req OpenGapRequest) (Gap, error)
//	func (g *Gaps) Close(ctx context.Context, req CloseGapRequest) (Gap, error)
//	func (g *Gaps) Escalate(ctx context.Context, req EscalateGapRequest) (Gap, error)
//	func (g *Gaps) Note(ctx context.Context, req GapNoteRequest) (Gap, error)
//	func (g *Gaps) Review(ctx context.Context, req GapReviewRequest) (Gap, error)
//	func (g *Gaps) Get(ctx context.Context, instanceID, gapID string) (Gap, bool, error)
//	func (g *Gaps) List(ctx context.Context, instanceID string) ([]Gap, error)
//
//	const (
//	    ActionGapOpen     = "gap_open"
//	    ActionGapClose    = "gap_close"
//	    ActionGapEscalate = "gap_escalate"
//	    ActionGapNote     = "gap_note"
//	    ActionGapReview   = "gap_review"
//	)
//	var (
//	    ErrGapInput             error // malformed/incomplete request
//	    ErrGapTransition        error // transition refused in this state
//	    ErrGapClosureEvidence   error // closure without new evidence
//	    ErrGapEscalationRef     error // escalation without a reference
//	    ErrGapNotFound          error
//	    ErrReviewBudgetExhausted error
//	)
//
// Pinned behavior:
//
//   - every open gap carries the FR-019 evidence bundle: object and scope,
//     timeline, existing evidence, required external evidence, risk, affected
//     capabilities, owner (responsibility) and the escalation record;
//   - the pause set is conservative: without a valid independence proof the
//     affected set expands from the directly related capability to every
//     capability that transitively depends on it (the amplification
//     direction) — "it is a different module" is not independence; a proof
//     with empty path/evidence entries proves nothing and does not narrow it;
//   - timeout / attempts_exhausted / acknowledged are audit notes, never a
//     state change, never closure and never a release permission;
//   - closure is possible only with new evidence; a closed gap stops blocking
//     the gate (a later generation check may still refuse);
//   - an unreconstructable gap keeps its affected capabilities paused and is
//     escalated (C3): escalation is not closure, no risk acceptance exists;
//   - bounded read-only review: the configured budget is a hard bound; an
//     exhausted (or missing/zero) budget refuses further reviews with an audit
//     row and changes no state.
//
// The gate half of every test runs the real T012 evaluation over the same real
// control store (gateNewGate); no gate substitute is used. Docker discipline:
// the package TestMain (generation_integration_test.go) reports NOT RUN locally
// when no Docker provider is healthy and fails the package under CI=true or
// TXHARBOR_REQUIRE_DOCKER=1. Helpers are gap-prefixed so they cannot collide
// with the gate/iso/bkp helpers of this package.
package recovery

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

// gapFixture is the T041 service over the real control-store fixture (open
// recovery instance, executor/verifier/approver, identity mappings) plus the
// real isolation checklist and the real gate.
func gapFixture(t *testing.T) (*gateFixture, *Checklist, *Gaps) {
	t.Helper()
	f, checklist := isoChecklistFixture(t)
	gaps, err := NewGaps(f.store)
	if err != nil {
		t.Fatalf("NewGaps over the real control store: %v", err)
	}
	return f, checklist, gaps
}

// gapValidRequest returns one complete FR-019 evidence bundle request.
func gapValidRequest(f *gateFixture, objectKey string, caps []Capability) OpenGapRequest {
	return OpenGapRequest{
		InstanceID:           f.instanceID,
		ObjectKey:            objectKey,
		Scope:                []byte(`{"object":"` + objectKey + `","chain_id":31337,"asset":"usdc"}`),
		Timeline:             []byte(`{"restore_point":"2026-09-28T10:00:00Z","observed_at":"2026-09-28T12:00:00Z"}`),
		ExistingEvidence:     []byte(`{"records":[],"note":"nothing observable after the restore point"}`),
		RequiredEvidence:     []byte(`{"required":["external signer/broadcast/chain receipt evidence","operator confirmation"]}`),
		Risk:                 []byte(`{"unproven_external_effect":true,"may_amplify":["existing_withdrawal_recovery"]}`),
		AffectedCapabilities: caps,
		Owner:                "person-verifier",
		Actor:                "deploy:executor",
		OperationID:          gateOperation("gap-open"),
	}
}

func gapOpenValid(t *testing.T, f *gateFixture, gaps *Gaps, objectKey string, caps []Capability) Gap {
	t.Helper()
	record, err := gaps.Open(f.ctx, gapValidRequest(f, objectKey, caps))
	if err != nil {
		t.Fatalf("gaps.Open(%s): %v", objectKey, err)
	}
	return record
}

func gapAdmit(t *testing.T, f *gateFixture, gate *Gate, capability Capability) GateDecision {
	t.Helper()
	decision, err := gate.Admit(f.ctx, GateRequest{
		InstanceID: f.instanceID, Capability: capability, ScopeHash: f.scope,
		Actor: "deploy:executor", OperationID: gateOperation("gap-admit"),
	})
	if err != nil {
		t.Fatalf("gate.Admit(%s): %v", capability, err)
	}
	return decision
}

func gapAuditCount(t *testing.T, f *gateFixture, action, result string) int {
	t.Helper()
	var n int
	if err := f.pool.QueryRow(f.ctx,
		`SELECT count(*) FROM recovery_audit WHERE instance_id = $1 AND action = $2 AND result = $3`,
		f.instanceID, action, result).Scan(&n); err != nil {
		t.Fatalf("count audit rows (%s/%s): %v", action, result, err)
	}
	return n
}

func gapRowCount(t *testing.T, f *gateFixture) int {
	t.Helper()
	var n int
	if err := f.pool.QueryRow(f.ctx,
		`SELECT count(*) FROM recovery_gap WHERE instance_id = $1`, f.instanceID).Scan(&n); err != nil {
		t.Fatalf("count gap rows: %v", err)
	}
	return n
}

// gapJSONEqual compares two JSON payloads semantically (key order and spacing
// are not part of the persisted evidence bundle).
func gapJSONEqual(t *testing.T, want, got []byte) bool {
	t.Helper()
	var a, b any
	if err := json.Unmarshal(want, &a); err != nil {
		t.Fatalf("decode expected JSON: %v", err)
	}
	if err := json.Unmarshal(got, &b); err != nil {
		t.Fatalf("decode returned JSON %q: %v", got, err)
	}
	return reflect.DeepEqual(a, b)
}

func gapCapSet(caps []Capability) map[Capability]bool {
	out := make(map[Capability]bool, len(caps))
	for _, c := range caps {
		out[c] = true
	}
	return out
}

// TestT037GapEvidenceBundleStructure pins the FR-019 bundle: a complete open
// gap round-trips object/scope, timeline, existing evidence, required external
// evidence, risk, affected capabilities and owner; an incomplete request is
// refused with nothing written and a refusal audit row.
func TestT037GapEvidenceBundleStructure(t *testing.T) {
	f, _, gaps := gapFixture(t)
	direct := []Capability{CapabilityExistingWithdrawalRecovery}

	before := gapRowCount(t, f)
	refusedBefore := gapAuditCount(t, f, ActionGapOpen, controlstore.AuditRefused)

	record := gapOpenValid(t, f, gaps, "payment_intent:intent-missing-1", direct)
	if record.GapID == "" || record.InstanceID != f.instanceID || record.ObjectKey != "payment_intent:intent-missing-1" {
		t.Fatalf("gap identity = %+v", record)
	}
	if record.State != GapStateOpen {
		t.Fatalf("new gap state = %q, want open", record.State)
	}
	if !gapJSONEqual(t, []byte(`{"object":"payment_intent:intent-missing-1","chain_id":31337,"asset":"usdc"}`), record.Scope) {
		t.Fatalf("gap scope = %s", record.Scope)
	}
	for name, payload := range map[string][]byte{
		"timeline":          record.Timeline,
		"existing_evidence": record.ExistingEvidence,
		"required_evidence": record.RequiredEvidence,
		"risk":              record.Risk,
	} {
		if len(payload) == 0 {
			t.Fatalf("open gap is missing the %s component of the FR-019 evidence bundle", name)
		}
	}
	if !gapJSONEqual(t, []byte(`{"unproven_external_effect":true,"may_amplify":["existing_withdrawal_recovery"]}`), record.Risk) {
		t.Fatalf("risk = %s", record.Risk)
	}
	if record.Owner != "person-verifier" {
		t.Fatalf("owner (responsibility) = %q, want person-verifier", record.Owner)
	}
	if !record.ClosedAt.IsZero() || record.ClosedBy != "" {
		t.Fatalf("open gap carries closure facts: %+v", record)
	}
	if caps := gapCapSet(record.AffectedCapabilities); !caps[CapabilityExistingWithdrawalRecovery] {
		t.Fatalf("affected capabilities must at least contain the directly related capability: %v", record.AffectedCapabilities)
	}

	// Re-read: the bundle survives persistence unchanged.
	stored, found, err := gaps.Get(f.ctx, f.instanceID, record.GapID)
	if err != nil || !found {
		t.Fatalf("gaps.Get = %+v found=%v err=%v", stored, found, err)
	}
	if stored.ObjectKey != record.ObjectKey || stored.Owner != record.Owner ||
		!gapJSONEqual(t, record.Timeline, stored.Timeline) ||
		!gapJSONEqual(t, record.RequiredEvidence, stored.RequiredEvidence) ||
		!gapJSONEqual(t, record.Risk, stored.Risk) {
		t.Fatalf("stored gap = %+v, want the complete bundle %+v", stored, record)
	}
	if listed, err := gaps.List(f.ctx, f.instanceID); err != nil || len(listed) != 1 || listed[0].GapID != record.GapID {
		t.Fatalf("gaps.List = %+v err=%v, want the one gap", listed, err)
	}
	if got := gapAuditCount(t, f, ActionGapOpen, controlstore.AuditOK); got != 1 {
		t.Fatalf("accepted gap opens audited = %d, want 1", got)
	}

	// Every required component is required: a missing one refuses, writes
	// nothing and is audited.
	mutations := map[string]func(*OpenGapRequest){
		"object_key":            func(r *OpenGapRequest) { r.ObjectKey = "" },
		"scope":                 func(r *OpenGapRequest) { r.Scope = nil },
		"timeline":              func(r *OpenGapRequest) { r.Timeline = nil },
		"existing_evidence":     func(r *OpenGapRequest) { r.ExistingEvidence = nil },
		"required_evidence":     func(r *OpenGapRequest) { r.RequiredEvidence = nil },
		"risk":                  func(r *OpenGapRequest) { r.Risk = nil },
		"owner":                 func(r *OpenGapRequest) { r.Owner = "" },
		"actor":                 func(r *OpenGapRequest) { r.Actor = "" },
		"operation_id":          func(r *OpenGapRequest) { r.OperationID = "" },
		"affected_capabilities": func(r *OpenGapRequest) { r.AffectedCapabilities = nil },
		"unknown_capability":    func(r *OpenGapRequest) { r.AffectedCapabilities = []Capability{"blockchain"} },
	}
	for name, mutate := range mutations {
		req := gapValidRequest(f, "object-"+name, direct)
		mutate(&req)
		if _, err := gaps.Open(f.ctx, req); !errors.Is(err, ErrGapInput) {
			t.Fatalf("Open with missing %s error = %v, want ErrGapInput", name, err)
		}
	}
	if got := gapRowCount(t, f); got != before+1 {
		t.Fatalf("refused opens wrote rows: gap rows %d, want %d", got, before+1)
	}
	if got := gapAuditCount(t, f, ActionGapOpen, controlstore.AuditRefused); got <= refusedBefore {
		t.Fatalf("refused opens must be audited: refused rows %d -> %d", refusedBefore, got)
	}
}

// TestT037ConservativePauseExpansion pins FR-019's conservative pause set: a
// gap on chain_scan drags in every capability that transitively depends on it
// (the amplification direction — "it is a different module" is not
// independence), an "independence proof" without path/edge evidence narrows
// nothing, and the real gate refuses every affected capability (gap_open on
// the gapped capability itself, capability_dependency_closed on its
// dependents) while capabilities outside the pause set are not blocked by the
// gap.
func TestT037ConservativePauseExpansion(t *testing.T) {
	f, checklist, gaps := gapFixture(t)
	gate := gateNewGate(t, f.store, GateOptions{})

	record := gapOpenValid(t, f, gaps, "block:height-9", []Capability{CapabilityChainScan})
	affected := gapCapSet(record.AffectedCapabilities)

	// The direct capability plus every transitive dependent (they would amplify
	// the missing evidence's consequences) are conservatively paused.
	for _, must := range []Capability{
		CapabilityChainScan,
		CapabilityDepositConfirmation,
		CapabilityExistingWithdrawalRecovery,
		CapabilityNewWithdrawalCreation,
		CapabilityEventPublishing,
	} {
		if !affected[must] {
			t.Fatalf("affected set %v must conservatively contain %s", record.AffectedCapabilities, must)
		}
	}
	// Capabilities that do not depend on chain_scan must not be dragged in.
	for _, mustNot := range []Capability{
		CapabilityQuery, CapabilityEventConsuming,
	} {
		if affected[mustNot] {
			t.Fatalf("affected set %v must not contain %s", record.AffectedCapabilities, mustNot)
		}
	}

	// A proof that says "different module" with no path and no per-edge
	// evidence proves nothing: the conservative inclusion stays.
	proof := gapValidRequest(f, "block:height-10", []Capability{CapabilityChainScan})
	proof.DependencyProof = []byte(`{"independent":[{"capability":"deposit_confirmation","note":"different module"}]}`)
	proofRecord, err := gaps.Open(f.ctx, proof)
	if err != nil {
		t.Fatalf("Open with a useless proof: %v", err)
	}
	for _, must := range []Capability{CapabilityDepositConfirmation, CapabilityExistingWithdrawalRecovery} {
		if !gapCapSet(proofRecord.AffectedCapabilities)[must] {
			t.Fatalf("a proof without path/edge evidence narrowed the pause set: %v", proofRecord.AffectedCapabilities)
		}
	}

	// Verify every capability's isolation set so the gap condition is the one
	// that decides the admission (the gate checks isolation before gaps).
	for _, capability := range KnownCapabilities() {
		isoVerifyAll(t, f, checklist, capability)
	}

	// The gapped capability itself: gap_open.
	if d := gapAdmit(t, f, gate, CapabilityChainScan); d.Allowed || d.RefusalClass != RefusalGapOpen {
		t.Fatalf("gapped capability chain_scan = %+v, want a gap_open refusal", d)
	}
	// Dependents: refused through the blocked dependency, naming the gap; the
	// pause propagates instead of leaving an amplification path.
	for _, capability := range []Capability{
		CapabilityDepositConfirmation,
		CapabilityExistingWithdrawalRecovery,
		CapabilityNewWithdrawalCreation,
		CapabilityEventPublishing,
	} {
		d := gapAdmit(t, f, gate, capability)
		if d.Allowed || d.RefusalClass != RefusalCapabilityDependencyClosed {
			t.Fatalf("dependent capability %s = %+v, want capability_dependency_closed", capability, d)
		}
		if !strings.Contains(d.Reason, string(RefusalGapOpen)) {
			t.Fatalf("dependent refusal for %s must name the gap blockage: %s", capability, d.Reason)
		}
	}
	// Outside the pause set: isolation verified and no release exists, but the
	// gap is not the blocker.
	for _, capability := range []Capability{
		CapabilityQuery, CapabilityEventConsuming,
	} {
		if d := gapAdmit(t, f, gate, capability); d.Allowed || d.RefusalClass != RefusalNoRelease {
			t.Fatalf("unaffected capability %s = %+v, want no_release", capability, d)
		}
	}
}

// TestT037TimeoutExhaustionAndKnowledgeDoNotChangeState pins F13/FR-019:
// timeout, attempts-exhausted and human acknowledgement are audit notes; they
// never change the gap state, never close it, never advance the instance
// generation and never let the capability be released.
func TestT037TimeoutExhaustionAndKnowledgeDoNotChangeState(t *testing.T) {
	f, checklist, gaps := gapFixture(t)
	gate := gateNewGate(t, f.store, GateOptions{})

	record := gapOpenValid(t, f, gaps, "attempt:att-missing-1", []Capability{CapabilityChainScan})
	generation, _ := f.token(t)

	for _, kind := range []GapNoteKind{GapNoteTimeout, GapNoteAttemptsExhausted, GapNoteAcknowledged} {
		noted, err := gaps.Note(f.ctx, GapNoteRequest{
			InstanceID: f.instanceID, GapID: record.GapID, Kind: kind,
			Reason: "operator note: " + string(kind), Actor: "deploy:executor",
			OperationID: gateOperation("gap-note-" + string(kind)),
		})
		if err != nil {
			t.Fatalf("gaps.Note(%s): %v", kind, err)
		}
		if noted.State != GapStateOpen {
			t.Fatalf("note %s changed the gap state to %q, want open", kind, noted.State)
		}
	}
	if got, _ := f.token(t); got != generation {
		t.Fatalf("audit notes must not advance the instance generation: %d -> %d", generation, got)
	}
	if got := gapAuditCount(t, f, ActionGapNote, controlstore.AuditOK); got != 3 {
		t.Fatalf("accepted notes audited = %d, want 3", got)
	}

	// An unknown note kind is a caller contract error.
	if _, err := gaps.Note(f.ctx, GapNoteRequest{
		InstanceID: f.instanceID, GapID: record.GapID, Kind: GapNoteKind("resolved_by_hope"),
		Reason: "x", Actor: "deploy:executor", OperationID: gateOperation("gap-note-bad"),
	}); !errors.Is(err, ErrGapInput) {
		t.Fatalf("unknown note kind error = %v, want ErrGapInput", err)
	}

	// Closing without new evidence is refused; the refusal is audited.
	refusedBefore := gapAuditCount(t, f, ActionGapClose, controlstore.AuditRefused)
	if _, err := gaps.Close(f.ctx, CloseGapRequest{
		InstanceID: f.instanceID, GapID: record.GapID,
		ClosureEvidence: nil, Actor: "deploy:executor",
		OperationID: gateOperation("gap-close-empty"), Reason: "timeout expired",
	}); !errors.Is(err, ErrGapClosureEvidence) {
		t.Fatalf("close without evidence error = %v, want ErrGapClosureEvidence", err)
	}
	if got, _ := f.token(t); got != generation {
		t.Fatalf("a refused close must not advance the instance generation: %d -> %d", generation, got)
	}
	if current, found, err := gaps.Get(f.ctx, f.instanceID, record.GapID); err != nil || !found ||
		current.State != GapStateOpen {
		t.Fatalf("gap after refused close = %+v found=%v err=%v, want open", current, found, err)
	}
	if got := gapAuditCount(t, f, ActionGapClose, controlstore.AuditRefused); got <= refusedBefore {
		t.Fatalf("refused closes must be audited: refused rows %d -> %d", refusedBefore, got)
	}

	// The real gate still refuses: a note or a timeout is not a closure and
	// not a release permission.
	isoVerifyAll(t, f, checklist, CapabilityChainScan)
	if d := gapAdmit(t, f, gate, CapabilityChainScan); d.Allowed || d.RefusalClass != RefusalGapOpen {
		t.Fatalf("chain_scan after timeout/notes = %+v, want gap_open", d)
	}
}

// TestT037ClosureOnlyByNewEvidence pins the state machine's only exit: a gap
// closes only when new evidence is supplied; the closed gap stops blocking the
// gate; the refusal/acceptance are audited.
func TestT037ClosureOnlyByNewEvidence(t *testing.T) {
	f, checklist, gaps := gapFixture(t)
	gate := gateNewGate(t, f.store, GateOptions{})

	record := gapOpenValid(t, f, gaps, "attempt:att-missing-2", []Capability{CapabilityChainScan})
	isoVerifyAll(t, f, checklist, CapabilityChainScan)
	if d := gapAdmit(t, f, gate, CapabilityChainScan); d.RefusalClass != RefusalGapOpen {
		t.Fatalf("timeout/notes = %+v, want gap_open", d)
	}

	// Whitespace-only evidence is not evidence.
	if _, err := gaps.Close(f.ctx, CloseGapRequest{
		InstanceID: f.instanceID, GapID: record.GapID, ClosureEvidence: []byte("  \n"),
		Actor: "deploy:executor", OperationID: gateOperation("gap-close-blank"),
	}); !errors.Is(err, ErrGapClosureEvidence) {
		t.Fatalf("blank closure evidence error = %v, want ErrGapClosureEvidence", err)
	}

	closure := []byte(`{"evidence_ref":"evidence://015/attempt/att-missing-2/signer-boundary","reverified_at":"2026-09-28T12:30:00Z"}`)
	closed, err := gaps.Close(f.ctx, CloseGapRequest{
		InstanceID: f.instanceID, GapID: record.GapID, ClosureEvidence: closure,
		Actor: "deploy:executor", OperationID: gateOperation("gap-close-real"),
		Reason: "new signer-boundary evidence supplied",
	})
	if err != nil {
		t.Fatalf("gaps.Close(new evidence): %v", err)
	}
	if closed.State != GapStateClosed || closed.ClosedBy != "deploy:executor" ||
		closed.ClosedAt.IsZero() || !gapJSONEqual(t, closure, closed.ClosureEvidence) {
		t.Fatalf("closed gap = %+v, want full closure facts", closed)
	}
	if got, _ := f.token(t); got == 0 {
		t.Fatal("instance token unreadable")
	}
	if got := gapAuditCount(t, f, ActionGapClose, controlstore.AuditOK); got != 1 {
		t.Fatalf("accepted close audited = %d, want 1", got)
	}

	// The gap no longer blocks: the next refusal is the missing release, not
	// gap_open.
	if d := gapAdmit(t, f, gate, CapabilityChainScan); d.Allowed || d.RefusalClass == RefusalGapOpen {
		t.Fatalf("chain_scan after closure = %+v, want a non-gap refusal", d)
	}
}

// TestT037UnreconstructableGapStaysPausedAndEscalated is C3: when the missing
// record cannot be reconstructed, the affected capabilities stay paused, the
// gap is escalated with a recorded reference, closure still requires new
// evidence, and no risk acceptance or forced resumption exists.
func TestT037UnreconstructableGapStaysPausedAndEscalated(t *testing.T) {
	f, checklist, gaps := gapFixture(t)
	gate := gateNewGate(t, f.store, GateOptions{})

	// Isolation is verified before the gap so later admission classes are
	// decided by the gap (verification of a checklist item advances the
	// generation and would otherwise invalidate a release made earlier).
	isoVerifyAll(t, f, checklist, CapabilityChainScan)
	isoVerifyAll(t, f, checklist, CapabilityExistingWithdrawalRecovery)

	record := gapOpenValid(t, f, gaps, "payment_intent:intent-unreconstructable", []Capability{CapabilityExistingWithdrawalRecovery})

	// Closure without new evidence is impossible, before and after escalation.
	if _, err := gaps.Close(f.ctx, CloseGapRequest{
		InstanceID: f.instanceID, GapID: record.GapID, Actor: "deploy:executor",
		OperationID: gateOperation("gap-close-pre-escalation"),
	}); !errors.Is(err, ErrGapClosureEvidence) {
		t.Fatalf("pre-escalation close error = %v, want ErrGapClosureEvidence", err)
	}

	escalated, err := gaps.Escalate(f.ctx, EscalateGapRequest{
		InstanceID: f.instanceID, GapID: record.GapID,
		EscalationRef: "escalation://ops/INC-015-1",
		Reason:        "payment intent cannot be reconstructed from chain or signer evidence",
		Actor:         "deploy:executor", OperationID: gateOperation("gap-escalate"),
	})
	if err != nil {
		t.Fatalf("gaps.Escalate: %v", err)
	}
	if escalated.State != GapStateEscalated || escalated.EscalationRef != "escalation://ops/INC-015-1" {
		t.Fatalf("escalated gap = %+v, want escalated with the recorded reference", escalated)
	}
	if got := gapAuditCount(t, f, ActionGapEscalate, controlstore.AuditOK); got != 1 {
		t.Fatalf("accepted escalation audited = %d, want 1", got)
	}

	// Escalation without a reference is refused.
	if _, err := gaps.Escalate(f.ctx, EscalateGapRequest{
		InstanceID: f.instanceID, GapID: record.GapID,
		Actor: "deploy:executor", OperationID: gateOperation("gap-escalate-bad"),
	}); !errors.Is(err, ErrGapEscalationRef) {
		t.Fatalf("escalation without reference error = %v, want ErrGapEscalationRef", err)
	}

	// A later acknowledgement does not move an escalated gap.
	acknowledged, err := gaps.Note(f.ctx, GapNoteRequest{
		InstanceID: f.instanceID, GapID: record.GapID, Kind: GapNoteAcknowledged,
		Reason: "human acknowledged the escalation", Actor: "deploy:executor",
		OperationID: gateOperation("gap-ack-escalated"),
	})
	if err != nil || acknowledged.State != GapStateEscalated {
		t.Fatalf("acknowledged escalated gap = %+v err=%v, want escalated", acknowledged, err)
	}

	// Close from escalated without evidence is refused and changes nothing.
	if _, err := gaps.Close(f.ctx, CloseGapRequest{
		InstanceID: f.instanceID, GapID: record.GapID, Actor: "deploy:executor",
		OperationID: gateOperation("gap-close-escalated"),
	}); !errors.Is(err, ErrGapClosureEvidence) && !errors.Is(err, ErrGapTransition) {
		t.Fatalf("close from escalated error = %v, want a closure/transition refusal", err)
	}
	current, found, err := gaps.Get(f.ctx, f.instanceID, record.GapID)
	if err != nil || !found || current.State != GapStateEscalated {
		t.Fatalf("gap after refused close = %+v found=%v err=%v, want escalated", current, found, err)
	}

	// The affected capability stays paused: escalation is not closure and
	// there is no risk acceptance / forced resumption path. chain_scan (which
	// the gap does not touch) is the positive control: it can be released,
	// and the gapped capability still refuses with gap_open through its own
	// facts.
	approval := f.approve(t, CapabilityChainScan, "auth:approver", "person-approver", ApprovalClassSingleNonExecutor)
	f.release(t, CapabilityChainScan, []string{approval})
	if d := gapAdmit(t, f, gate, CapabilityChainScan); !d.Allowed {
		t.Fatalf("chain_scan positive control = %+v, want allowed", d)
	}
	if d := gapAdmit(t, f, gate, CapabilityExistingWithdrawalRecovery); d.Allowed || d.RefusalClass != RefusalGapOpen {
		t.Fatalf("existing_withdrawal_recovery with an escalated gap = %+v, want gap_open", d)
	}
	if d := gapAdmit(t, f, gate, CapabilityNewWithdrawalCreation); d.Allowed {
		t.Fatalf("dependent capability released while its blocking gap is escalated: %+v", d)
	}

	// The instance stays open for manual handling.
	var state string
	if err := f.pool.QueryRow(f.ctx,
		`SELECT state FROM recovery_instance WHERE instance_id = $1`, f.instanceID).Scan(&state); err != nil {
		t.Fatalf("read instance state: %v", err)
	}
	if state != "open" {
		t.Fatalf("instance state = %q, want open (no risk acceptance, no forced close)", state)
	}
}

// TestT037BoundedReadOnlyReviewBudget pins F13: the review budget is a hard
// bound; exceeding it (or not configuring it) refuses with an audit row and
// changes no gap/instance state; timeout and exhaustion never close a gap.
func TestT037BoundedReadOnlyReviewBudget(t *testing.T) {
	f, checklist, gaps := gapFixture(t)
	gate := gateNewGate(t, f.store, GateOptions{})

	record := gapOpenValid(t, f, gaps, "consumer:group-missing", []Capability{CapabilityEventConsuming})
	generation, _ := f.token(t)

	// A missing/zero budget means "not configured": fail-closed, never
	// unbounded.
	if _, err := gaps.Review(f.ctx, GapReviewRequest{
		InstanceID: f.instanceID, GapID: record.GapID, Budget: ReviewBudget{MaxReads: 0},
		Actor: "deploy:executor", OperationID: gateOperation("gap-review-nobudget"),
	}); !errors.Is(err, ErrReviewBudgetExhausted) {
		t.Fatalf("zero review budget error = %v, want ErrReviewBudgetExhausted", err)
	}

	for i := 0; i < 2; i++ {
		reviewed, err := gaps.Review(f.ctx, GapReviewRequest{
			InstanceID: f.instanceID, GapID: record.GapID, Budget: ReviewBudget{MaxReads: 2},
			Actor: "deploy:executor", OperationID: gateOperation("gap-review"),
		})
		if err != nil {
			t.Fatalf("review %d within budget: %v", i+1, err)
		}
		if reviewed.State != GapStateOpen {
			t.Fatalf("review %d changed the gap state to %q, want open", i+1, reviewed.State)
		}
	}
	refusedBefore := gapAuditCount(t, f, ActionGapReview, controlstore.AuditRefused)
	if _, err := gaps.Review(f.ctx, GapReviewRequest{
		InstanceID: f.instanceID, GapID: record.GapID, Budget: ReviewBudget{MaxReads: 2},
		Actor: "deploy:executor", OperationID: gateOperation("gap-review-over"),
	}); !errors.Is(err, ErrReviewBudgetExhausted) {
		t.Fatalf("review past the budget error = %v, want ErrReviewBudgetExhausted", err)
	}
	if got := gapAuditCount(t, f, ActionGapReview, controlstore.AuditRefused); got <= refusedBefore {
		t.Fatalf("exhausted reviews must be audited: refused rows %d -> %d", refusedBefore, got)
	}
	if _, err := gaps.Review(f.ctx, GapReviewRequest{
		InstanceID: f.instanceID, GapID: "00000000-0000-0000-0000-000000000000",
		Budget: ReviewBudget{MaxReads: 2}, Actor: "deploy:executor",
		OperationID: gateOperation("gap-review-missing"),
	}); !errors.Is(err, ErrGapNotFound) {
		t.Fatalf("review of an unknown gap error = %v, want ErrGapNotFound", err)
	}

	// No state changed: same state, same generation, gap still blocks.
	current, found, err := gaps.Get(f.ctx, f.instanceID, record.GapID)
	if err != nil || !found || current.State != GapStateOpen {
		t.Fatalf("gap after exhausted reviews = %+v found=%v err=%v, want open", current, found, err)
	}
	if got, _ := f.token(t); got != generation {
		t.Fatalf("reviews must not advance the instance generation: %d -> %d", generation, got)
	}
	isoVerifyAll(t, f, checklist, CapabilityEventConsuming)
	if d := gapAdmit(t, f, gate, CapabilityEventConsuming); d.Allowed || d.RefusalClass != RefusalGapOpen {
		t.Fatalf("event_consuming after exhausted reviews = %+v, want gap_open", d)
	}
}
