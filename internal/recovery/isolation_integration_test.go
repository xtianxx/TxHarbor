//go:build integration

// isolation_integration_test.go is T024 [US2]: the isolation-checklist state
// machine and the old-instance refusal layer against a real PostgreSQL 18.6
// control store (tags: integration; FR-010/011/012; data-model.md
// §1.7/§3.2/§4.3/§5; contracts/resumption-gate.md §3; tasks.md T024).
//
// TDD-first. This file references the B8/T028 API that does not exist yet, so
// the integration candidate fails to build until B8 lands. Expected API
// surface (documented at every use site):
//
//	// internal/recovery/checklist.go (T028)
//	type ChecklistState string
//	const (
//	    ChecklistStatePending   ChecklistState = "pending"
//	    ChecklistStateEvidenced ChecklistState = "evidenced"
//	    ChecklistStateVerified  ChecklistState = "verified"
//	    ChecklistStateRejected  ChecklistState = "rejected"
//	)
//	type ChecklistItem struct {
//	    InstanceID        string
//	    ItemKey           IsolationItemKey
//	    State             ChecklistState
//	    EvidenceRef       string
//	    CheckpointSummary []byte
//	    CheckedBy         string
//	    CheckedAt         time.Time
//	    VerifiedBy        string
//	    VerifiedAt        time.Time // zero while unset
//	}
//	type ChecklistEvidenceRequest struct {
//	    InstanceID        string
//	    ItemKey           IsolationItemKey
//	    State             ChecklistState // evidenced only (collection)
//	    EvidenceRef       string         // external evidence reference, required
//	    CheckpointSummary []byte         // status notes; never proof on their own
//	    Actor             string         // this instance's executor
//	    OperationID       string
//	}
//	type ChecklistVerifyRequest struct {
//	    InstanceID  string
//	    ItemKey     IsolationItemKey
//	    Actor       string // a participant whose person is not the executor's
//	    OperationID string
//	    Reject      bool   // insufficient evidence -> rejected (re-collect)
//	    Reason      string
//	}
//	func NewChecklist(store *controlstore.Store) (*Checklist, error)
//	func (c *Checklist) Set(ctx context.Context, req ChecklistEvidenceRequest) (ChecklistItem, error)
//	func (c *Checklist) Verify(ctx context.Context, req ChecklistVerifyRequest) (ChecklistItem, error)
//	func (c *Checklist) Item(ctx context.Context, instanceID string, item IsolationItemKey) (ChecklistItem, bool, error)
//	var (
//	    ErrChecklistTransition       error // invalid state transition / missing evidence reference
//	    ErrChecklistSelfVerification error // verifier resolves to the instance executor
//	)
//
// Semantics pinned here: pending (absent row) -> evidenced -> verified, or a
// non-executor verdict rejected -> re-collected; evidence collection
// (evidenced) does not advance the instance evidence generation, while
// verified/rejected do (data-model §5, through the real T013 protocol);
// a status record without an external evidence reference can never be
// evidence; an executor — including the same person under another principal —
// can never verify its own item; refused writes leave the row unchanged,
// write nothing and are audited.
//
// Gate/refusal half: the gate evaluations below read the real control store;
// the F5 negative exercises unbound, old-instance and current-instance
// admissions with unproven isolation. No test double stands in for the gate
// or the checklist. Before B8 landed, the gate+store assertions (tests 5-7)
// were verified in a scratch run with rows seeded through the existing write
// paths; the checklist-driven variants here run once T028 exists.
//
// Docker discipline: the package TestMain (generation_integration_test.go)
// reports NOT RUN (exit 0) locally when no Docker provider is healthy and
// fails the package under CI=true or TXHARBOR_REQUIRE_DOCKER=1. An unrun PG
// layer is never a pass. Helpers are iso-prefixed so they cannot collide with
// the gate/bkp/generation helpers in this package.
package recovery

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

const isoEvidenceRef = "evidence://015/isolation/"

// isoChecklistFixture reuses the gate package fixture (open recovery instance
// with executor/verifier/approver participants and identity mappings) and adds
// the T028 checklist service over the same control store.
func isoChecklistFixture(t *testing.T) (*gateFixture, *Checklist) {
	t.Helper()
	f := gateBaseFixture(t)
	checklist, err := NewChecklist(f.store)
	if err != nil {
		t.Fatalf("NewChecklist over the real control store: %v", err)
	}
	return f, checklist
}

func isoItem(t *testing.T, f *gateFixture, checklist *Checklist, item IsolationItemKey) (ChecklistItem, bool) {
	t.Helper()
	record, found, err := checklist.Item(f.ctx, f.instanceID, item)
	if err != nil {
		t.Fatalf("checklist.Item(%s): %v", item, err)
	}
	return record, found
}

func isoSetEvidence(t *testing.T, f *gateFixture, checklist *Checklist, item IsolationItemKey, ref string, summary []byte) ChecklistItem {
	t.Helper()
	record, err := checklist.Set(f.ctx, ChecklistEvidenceRequest{
		InstanceID:        f.instanceID,
		ItemKey:           item,
		State:             ChecklistStateEvidenced,
		EvidenceRef:       ref,
		CheckpointSummary: summary,
		Actor:             "deploy:executor",
		OperationID:       gateOperation("iso-set"),
	})
	if err != nil {
		t.Fatalf("checklist.Set(%s): %v", item, err)
	}
	return record
}

func isoVerifyItem(t *testing.T, f *gateFixture, checklist *Checklist, item IsolationItemKey) ChecklistItem {
	t.Helper()
	record, err := checklist.Verify(f.ctx, ChecklistVerifyRequest{
		InstanceID:  f.instanceID,
		ItemKey:     item,
		Actor:       "auth:verifier",
		OperationID: gateOperation("iso-verify"),
	})
	if err != nil {
		t.Fatalf("checklist.Verify(%s): %v", item, err)
	}
	return record
}

// isoVerifyAll verifies every isolation dependency item of capability through
// the real checklist path.
func isoVerifyAll(t *testing.T, f *gateFixture, checklist *Checklist, capability Capability) {
	t.Helper()
	items, err := IsolationDependencySet(capability)
	if err != nil {
		t.Fatalf("IsolationDependencySet(%s): %v", capability, err)
	}
	for _, item := range items {
		if _, found := isoItem(t, f, checklist, item); !found {
			isoSetEvidence(t, f, checklist, item, isoEvidenceRef+string(item)+"/real", []byte(`{"source":"integration"}`))
		}
		isoVerifyItem(t, f, checklist, item)
	}
}

func isoAuditCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, instanceID, result string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM recovery_audit WHERE instance_id = $1 AND result = $2`,
		instanceID, result).Scan(&n); err != nil {
		t.Fatalf("count audit rows (result=%s): %v", result, err)
	}
	return n
}

func isoGateAdmissionOK(t *testing.T, ctx context.Context, pool *pgxpool.Pool, instanceID string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM recovery_audit WHERE instance_id = $1 AND action = $2 AND result = $3`,
		instanceID, GateAuditAction, controlstore.AuditOK).Scan(&n); err != nil {
		t.Fatalf("count admitted gate actions: %v", err)
	}
	return n
}

// TestIsolationChecklistStateMachinePendingEvidencedVerified covers the state
// machine: an absent row is the implicit pending state; the executor collects
// evidence (no generation advance); the non-executor verifier confirms
// (generation advance through the real T013 protocol); the row persists.
func TestIsolationChecklistStateMachinePendingEvidencedVerified(t *testing.T) {
	f, checklist := isoChecklistFixture(t)
	item := IsolationItemOldWritersStopped

	if _, found := isoItem(t, f, checklist, item); found {
		t.Fatal("an untouched checklist item must not have a row (absent = pending)")
	}
	if _, _, err := checklist.Item(f.ctx, f.instanceID, IsolationItemKey("old_processes_stopped")); !errors.Is(err, ErrUnknownIsolationItem) {
		t.Fatalf("unknown item key error = %v, want ErrUnknownIsolationItem", err)
	}
	if _, err := checklist.Set(f.ctx, ChecklistEvidenceRequest{
		InstanceID: f.instanceID, ItemKey: IsolationItemKey("old_processes_stopped"),
		State: ChecklistStateEvidenced, EvidenceRef: isoEvidenceRef + "x",
		Actor: "deploy:executor", OperationID: gateOperation("iso-set-bad"),
	}); !errors.Is(err, ErrUnknownIsolationItem) {
		t.Fatalf("Set with an unknown item key = %v, want ErrUnknownIsolationItem", err)
	}

	generation, hash := gateInstanceToken(t, f.ctx, f.pool, f.instanceID)
	auditBefore := isoAuditCount(t, f.ctx, f.pool, f.instanceID, controlstore.AuditOK)
	ref := isoEvidenceRef + "old_writers_stopped/1"
	summary := []byte(`{"service_manager":"stopped","processes_absent":true}`)
	evidenced := isoSetEvidence(t, f, checklist, item, ref, summary)
	if evidenced.State != ChecklistStateEvidenced || evidenced.EvidenceRef != ref ||
		evidenced.CheckedBy != "deploy:executor" || evidenced.CheckedAt.IsZero() {
		t.Fatalf("evidenced record = %+v, want the executor's collection with the external reference", evidenced)
	}
	if got, _ := gateInstanceToken(t, f.ctx, f.pool, f.instanceID); got != generation {
		t.Fatalf("evidence collection must not advance the instance generation: got %d, want %d", got, generation)
	}
	record, found := isoItem(t, f, checklist, item)
	if !found || record.State != ChecklistStateEvidenced || record.EvidenceRef != ref {
		t.Fatalf("persisted evidenced record = %+v (found=%v)", record, found)
	}

	verified := isoVerifyItem(t, f, checklist, item)
	if verified.State != ChecklistStateVerified || verified.VerifiedBy != "auth:verifier" || verified.VerifiedAt.IsZero() {
		t.Fatalf("verified record = %+v, want the non-executor confirmation", verified)
	}
	nextGeneration, nextHash := gateInstanceToken(t, f.ctx, f.pool, f.instanceID)
	if nextGeneration != generation+1 || nextHash == hash {
		t.Fatalf("verified transition must advance the generation by exactly 1 and refresh the hash: generation %d -> %d, hash changed=%v",
			generation, nextGeneration, nextHash != hash)
	}
	record, found = isoItem(t, f, checklist, item)
	if !found || record.State != ChecklistStateVerified || record.VerifiedBy != "auth:verifier" {
		t.Fatalf("persisted verified record = %+v (found=%v)", record, found)
	}
	if after := isoAuditCount(t, f.ctx, f.pool, f.instanceID, controlstore.AuditOK); after <= auditBefore {
		t.Fatalf("accepted checklist transitions must be audited: ok rows %d -> %d", auditBefore, after)
	}
	if refused := isoAuditCount(t, f.ctx, f.pool, f.instanceID, controlstore.AuditRefused); refused != 0 {
		t.Fatalf("the positive path must not write refusal rows, got %d", refused)
	}
}

// TestIsolationChecklistRejectedRequiresRecollection covers the rejected arm:
// a non-executor verdict of insufficient evidence moves the item to rejected
// (generation advance), verification of a rejected item is refused, and the
// executor re-collects with a new external reference before verification.
func TestIsolationChecklistRejectedRequiresRecollection(t *testing.T) {
	f, checklist := isoChecklistFixture(t)
	item := IsolationItemWriterFencingObserved

	isoSetEvidence(t, f, checklist, item, isoEvidenceRef+"writer_fencing/1", []byte(`{"lease":"renewed"}`))
	generation, _ := gateInstanceToken(t, f.ctx, f.pool, f.instanceID)
	rejected, err := checklist.Verify(f.ctx, ChecklistVerifyRequest{
		InstanceID: f.instanceID, ItemKey: item, Actor: "auth:verifier",
		Reject: true, Reason: "lease evidence unreadable",
		OperationID: gateOperation("iso-reject"),
	})
	if err != nil {
		t.Fatalf("checklist.Verify(reject): %v", err)
	}
	if rejected.State != ChecklistStateRejected || rejected.VerifiedBy != "auth:verifier" {
		t.Fatalf("rejected record = %+v, want the non-executor rejection verdict", rejected)
	}
	if got, _ := gateInstanceToken(t, f.ctx, f.pool, f.instanceID); got != generation+1 {
		t.Fatalf("rejected transition must advance the generation by exactly 1: got %d, want %d", got, generation+1)
	}

	// A rejected item cannot be verified; it must be re-collected first.
	if _, err := checklist.Verify(f.ctx, ChecklistVerifyRequest{
		InstanceID: f.instanceID, ItemKey: item, Actor: "auth:verifier", OperationID: gateOperation("iso-verify-rejected"),
	}); !errors.Is(err, ErrChecklistTransition) {
		t.Fatalf("Verify on a rejected item = %v, want ErrChecklistTransition", err)
	}
	record, found := isoItem(t, f, checklist, item)
	if !found || record.State != ChecklistStateRejected {
		t.Fatalf("a refused Verify must leave the rejected row unchanged, got %+v (found=%v)", record, found)
	}

	// Re-collection with a new reference, then verification.
	isoSetEvidence(t, f, checklist, item, isoEvidenceRef+"writer_fencing/2", []byte(`{"lease":"fencing_token=42"}`))
	verified := isoVerifyItem(t, f, checklist, item)
	if verified.State != ChecklistStateVerified || verified.EvidenceRef != isoEvidenceRef+"writer_fencing/2" {
		t.Fatalf("re-collected record = %+v", verified)
	}
}

// TestIsolationChecklistExecutorCannotVerify covers the executor exclusion:
// the executor principal itself and the same person under another principal
// are refused; the refusal is audited and leaves the item evidenced; a real
// non-executor participant verifies it.
func TestIsolationChecklistExecutorCannotVerify(t *testing.T) {
	f, checklist := isoChecklistFixture(t)
	item := IsolationItemNetworkIsolation

	isoSetEvidence(t, f, checklist, item, isoEvidenceRef+"network_isolation/1", []byte(`{"host_fw":"drop"}`))
	generation, _ := gateInstanceToken(t, f.ctx, f.pool, f.instanceID)

	// (1) The executor principal itself.
	refusedBefore := isoAuditCount(t, f.ctx, f.pool, f.instanceID, controlstore.AuditRefused)
	if _, err := checklist.Verify(f.ctx, ChecklistVerifyRequest{
		InstanceID: f.instanceID, ItemKey: item, Actor: "deploy:executor", OperationID: gateOperation("iso-self"),
	}); !errors.Is(err, ErrChecklistSelfVerification) {
		t.Fatalf("executor self-verify = %v, want ErrChecklistSelfVerification", err)
	}
	if got, _ := gateInstanceToken(t, f.ctx, f.pool, f.instanceID); got != generation {
		t.Fatalf("a refused self-verification must not advance the generation: got %d, want %d", got, generation)
	}
	if record, found := isoItem(t, f, checklist, item); !found || record.State != ChecklistStateEvidenced || record.VerifiedBy != "" {
		t.Fatalf("state after refused self-verify = %+v (found=%v), want evidenced", record, found)
	}
	if after := isoAuditCount(t, f.ctx, f.pool, f.instanceID, controlstore.AuditRefused); after <= refusedBefore {
		t.Fatalf("a refused self-verification must be audited: refused rows %d -> %d", refusedBefore, after)
	}

	// (2) The same person under another principal (FR-023: principal strings
	// are not people; the person mapping decides).
	gateMapIdentity(t, f.ctx, f.store, "auth:executor-alt", "person-executor")
	gateRegister(t, f.ctx, f.store, f.instanceID, "auth:executor-alt", "verifier")
	if _, err := checklist.Verify(f.ctx, ChecklistVerifyRequest{
		InstanceID: f.instanceID, ItemKey: item, Actor: "auth:executor-alt", OperationID: gateOperation("iso-alt-self"),
	}); !errors.Is(err, ErrChecklistSelfVerification) {
		t.Fatalf("same-person alternate principal verify = %v, want ErrChecklistSelfVerification", err)
	}
	if record, found := isoItem(t, f, checklist, item); !found || record.State != ChecklistStateEvidenced {
		t.Fatalf("state after refused same-person verify = %+v (found=%v), want evidenced", record, found)
	}

	// (3) A different person verifies.
	verified := isoVerifyItem(t, f, checklist, item)
	if verified.VerifiedBy != "auth:verifier" {
		t.Fatalf("verified_by = %q, want auth:verifier", verified.VerifiedBy)
	}
}

// TestIsolationChecklistStatusRecordIsNotEvidence pins "a state record is not
// proof": evidence collection without an external evidence reference is
// refused (nothing is written), the item stays unverifiable, while an external
// reference alone (no status notes) is accepted.
func TestIsolationChecklistStatusRecordIsNotEvidence(t *testing.T) {
	f, checklist := isoChecklistFixture(t)
	item := IsolationItemOldWritersStopped

	// A bare "the old worker is surely gone" note is not evidence.
	if _, err := checklist.Set(f.ctx, ChecklistEvidenceRequest{
		InstanceID:        f.instanceID,
		ItemKey:           item,
		State:             ChecklistStateEvidenced,
		EvidenceRef:       "  ",
		CheckpointSummary: []byte(`{"verbal":"the old worker is surely gone","observed_at":"2026-09-28T12:00:00Z"}`),
		Actor:             "deploy:executor",
		OperationID:       gateOperation("iso-verbal"),
	}); !errors.Is(err, ErrChecklistTransition) {
		t.Fatalf("status-record-only Set = %v, want ErrChecklistTransition", err)
	}
	if _, found := isoItem(t, f, checklist, item); found {
		t.Fatal("a refused Set must write nothing")
	}
	if _, err := checklist.Verify(f.ctx, ChecklistVerifyRequest{
		InstanceID: f.instanceID, ItemKey: item, Actor: "auth:verifier", OperationID: gateOperation("iso-verify-empty"),
	}); !errors.Is(err, ErrChecklistTransition) {
		t.Fatalf("Verify without collected evidence = %v, want ErrChecklistTransition", err)
	}

	// An external evidence reference alone is sufficient for collection; the
	// status summary is optional and is never the proof.
	collected := isoSetEvidence(t, f, checklist, item, isoEvidenceRef+"old_writers_stopped/2", nil)
	if collected.State != ChecklistStateEvidenced || collected.EvidenceRef == "" {
		t.Fatalf("reference-only collection = %+v", collected)
	}
	verified := isoVerifyItem(t, f, checklist, item)
	if verified.State != ChecklistStateVerified {
		t.Fatalf("reference-only item must verify, got %+v", verified)
	}
}

// TestIsolationReleaseRequiresVerifiedDependencySet pins the release
// prerequisite: query is refused while its isolation dependency set is
// missing/evidenced, allowed only after every item is verified (with a valid
// single approval and release at the current generation), and refused again as
// soon as a later evidence change (rejection) advances the generation.
func TestIsolationReleaseRequiresVerifiedDependencySet(t *testing.T) {
	f, checklist := isoChecklistFixture(t)
	gate := gateNewGate(t, f.store, GateOptions{})
	admit := func(capability Capability) GateDecision {
		t.Helper()
		d, err := gate.Admit(f.ctx, GateRequest{
			InstanceID: f.instanceID, Capability: capability, ScopeHash: f.scope,
			Actor: "deploy:executor", OperationID: gateOperation("iso-admit"),
		})
		if err != nil {
			t.Fatalf("gate.Admit(%s): %v", capability, err)
		}
		return d
	}
	items, err := IsolationDependencySet(CapabilityQuery)
	if err != nil {
		t.Fatalf("IsolationDependencySet(query): %v", err)
	}

	// 1. No checklist evidence at all: the gate refuses before release and
	// names the blocking missing isolation evidence (the derivation reports
	// the first unverified item in dependency-set order; the full per-item
	// status view is T029's checklist surface).
	d := admit(CapabilityQuery)
	if d.Allowed || d.RefusalClass != RefusalIsolationUnproven {
		t.Fatalf("query without isolation evidence = %+v, want isolation_unproven", d)
	}
	if len(items) == 0 || !strings.Contains(d.Reason, string(items[0])) {
		t.Fatalf("refusal must name the blocking item %v: %s", items, d.Reason)
	}

	// 2. Collected (evidenced) is not verified.
	for _, item := range items {
		isoSetEvidence(t, f, checklist, item, isoEvidenceRef+string(item)+"/pending", []byte(`{"state":"collected"}`))
	}
	if d := admit(CapabilityQuery); d.Allowed || d.RefusalClass != RefusalIsolationUnproven {
		t.Fatalf("query with evidenced-only isolation = %+v, want isolation_unproven", d)
	}

	// 3. Every dependency verified, a valid single non-executor approval and a
	// release at the current generation: allowed.
	for _, item := range items {
		isoVerifyItem(t, f, checklist, item)
	}
	approval := f.approve(t, CapabilityQuery, "auth:approver", "person-approver", ApprovalClassSingleNonExecutor)
	f.release(t, CapabilityQuery, []string{approval})
	if d := admit(CapabilityQuery); !d.Allowed || d.Normal {
		t.Fatalf("query with a fully verified dependency set = %+v, want a recovery-mode allow", d)
	}

	// 4. A later evidence change (rejection) advances the generation and makes
	// the next admission refuse at once (F5: the evaluator discovers it, no
	// waiting for the TTL).
	if _, err := checklist.Verify(f.ctx, ChecklistVerifyRequest{
		InstanceID: f.instanceID, ItemKey: items[0], Actor: "auth:verifier",
		Reject: true, Reason: "evidence superseded", OperationID: gateOperation("iso-reject-live"),
	}); err != nil {
		t.Fatalf("reject after release: %v", err)
	}
	if d := admit(CapabilityQuery); d.Allowed || d.RefusalClass != RefusalIsolationUnproven {
		t.Fatalf("query after an isolation rejection = %+v, want isolation_unproven", d)
	}
}

// TestNewWithdrawalCreationCannotPassWhileExistingRecoveryUnreleased pins the
// conservative dependency new_withdrawal_creation -> existing_withdrawal_recovery
// (data-model §3.2, "do not open the entry without the recovery path
// prepared"): chain_scan is made fully release-valid, but
// existing_withdrawal_recovery has no verified isolation, so
// new_withdrawal_creation is refused with capability_dependency_closed naming
// existing_withdrawal_recovery.
func TestNewWithdrawalCreationCannotPassWhileExistingRecoveryUnreleased(t *testing.T) {
	f, checklist := isoChecklistFixture(t)
	gate := gateNewGate(t, f.store, GateOptions{})
	admit := func(capability Capability) GateDecision {
		t.Helper()
		d, err := gate.Admit(f.ctx, GateRequest{
			InstanceID: f.instanceID, Capability: capability, ScopeHash: f.scope,
			Actor: "deploy:executor", OperationID: gateOperation("iso-dependency-admit"),
		})
		if err != nil {
			t.Fatalf("gate.Admit(%s): %v", capability, err)
		}
		return d
	}

	// chain_scan fully release-valid: isolation verified, single approval,
	// release at the current generation. This is the positive control.
	isoVerifyAll(t, f, checklist, CapabilityChainScan)
	approval := f.approve(t, CapabilityChainScan, "auth:approver", "person-approver", ApprovalClassSingleNonExecutor)
	f.release(t, CapabilityChainScan, []string{approval})
	if d := admit(CapabilityChainScan); !d.Allowed {
		t.Fatalf("chain_scan positive control = %+v, want allowed", d)
	}

	// existing_withdrawal_recovery is not release-valid (its isolation set is
	// unverified), so new_withdrawal_creation is blocked by the dependency.
	d := admit(CapabilityNewWithdrawalCreation)
	if d.Allowed || d.RefusalClass != RefusalCapabilityDependencyClosed {
		t.Fatalf("new_withdrawal_creation = %+v, want capability_dependency_closed", d)
	}
	if !strings.Contains(d.Reason, string(CapabilityExistingWithdrawalRecovery)) {
		t.Fatalf("the refusal must name existing_withdrawal_recovery: %s", d.Reason)
	}
}

// TestOldInstanceIsolationUnprovenRefusesAllEffectfulAdmissions is the F5
// negative: while the old writers/senders/deliverers may still be alive (no
// verified isolation evidence exists), every write/send/deliver admission is
// refused 100% — unbound callers and callers bound to an old/unknown instance
// as instance_mismatch, callers bound to the current instance as
// isolation_unproven/capability_dependency_closed with the missing evidence
// named — and a "the old process is unreachable / timed out" status record is
// not acceptable evidence. Timeout or unreachability never infers "stopped".
func TestOldInstanceIsolationUnprovenRefusesAllEffectfulAdmissions(t *testing.T) {
	f, checklist := isoChecklistFixture(t)
	gate := gateNewGate(t, f.store, GateOptions{})

	// A status note claiming the old process timed out / is unreachable can
	// never be collected as evidence.
	if _, err := checklist.Set(f.ctx, ChecklistEvidenceRequest{
		InstanceID:        f.instanceID,
		ItemKey:           IsolationItemOldWritersStopped,
		State:             ChecklistStateEvidenced,
		EvidenceRef:       "",
		CheckpointSummary: []byte(`{"old_process":"unreachable after timeout","observed_at":"2026-09-28T12:00:00Z"}`),
		Actor:             "deploy:executor",
		OperationID:       gateOperation("iso-timeout"),
	}); !errors.Is(err, ErrChecklistTransition) {
		t.Fatalf("timeout/unreachable status note = %v, want ErrChecklistTransition", err)
	}

	effectful := []Capability{
		CapabilityNewWithdrawalCreation,      // write
		CapabilityExistingWithdrawalRecovery, // write / sign / broadcast
		CapabilityEventPublishing,            // send
		CapabilityEventConsuming,             // deliver (effect)
	}
	oldInstance := uuid.NewString()
	refusals := 0
	for _, capability := range effectful {
		// (a) An unbound (old or forgotten) caller cannot act in recovery mode.
		d, err := gate.Admit(f.ctx, GateRequest{
			Capability: capability, ScopeHash: f.scope, Actor: "deploy:old-writer", OperationID: gateOperation("iso-f5-unbound"),
		})
		if err != nil {
			t.Fatalf("unbound Admit(%s): %v", capability, err)
		}
		if d.Allowed || d.RefusalClass != RefusalInstanceMismatch {
			t.Fatalf("unbound %s admission = %+v, want instance_mismatch", capability, d)
		}
		refusals++

		// (b) A caller bound to an old/unknown instance id is refused.
		d, err = gate.Admit(f.ctx, GateRequest{
			InstanceID: oldInstance, Capability: capability, ScopeHash: f.scope, Actor: "deploy:old-writer", OperationID: gateOperation("iso-f5-old"),
		})
		if err != nil {
			t.Fatalf("old-instance Admit(%s): %v", capability, err)
		}
		if d.Allowed || d.RefusalClass != RefusalInstanceMismatch {
			t.Fatalf("old-instance %s admission = %+v, want instance_mismatch", capability, d)
		}
		refusals++

		// (c) A caller bound to the current instance is refused because the
		// isolation evidence is unproven, and the reason names the missing
		// evidence (old_writers_stopped is the first item of every closure).
		d, err = gate.Admit(f.ctx, GateRequest{
			InstanceID: f.instanceID, Capability: capability, ScopeHash: f.scope, Actor: "deploy:executor", OperationID: gateOperation("iso-f5-current"),
		})
		if err != nil {
			t.Fatalf("current-instance Admit(%s): %v", capability, err)
		}
		if d.Allowed {
			t.Fatalf("current-instance %s admission must be refused while isolation is unproven", capability)
		}
		if d.RefusalClass != RefusalIsolationUnproven && d.RefusalClass != RefusalCapabilityDependencyClosed {
			t.Fatalf("current-instance %s refusal = %+v, want isolation_unproven/capability_dependency_closed", capability, d)
		}
		if !strings.Contains(d.Reason, string(IsolationItemOldWritersStopped)) {
			t.Fatalf("refusal for %s must list the missing isolation evidence: %s", capability, d.Reason)
		}
		refusals++
	}
	if refusals != len(effectful)*3 {
		t.Fatalf("expected %d refusals, got %d", len(effectful)*3, refusals)
	}
	// 100% refusal: no external action was admitted, and every refusal left an
	// audit row.
	if ok := isoGateAdmissionOK(t, f.ctx, f.pool, f.instanceID); ok != 0 {
		t.Fatalf("%d gate admission(s) were allowed while isolation is unproven", ok)
	}
	if refused := isoAuditCount(t, f.ctx, f.pool, f.instanceID, controlstore.AuditRefused); refused < refusals {
		t.Fatalf("refused rows = %d, want at least the %d refusals", refused, refusals)
	}
}
