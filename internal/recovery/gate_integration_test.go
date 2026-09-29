//go:build integration

// gate_integration_test.go runs T012's release_valid against a real PostgreSQL
// 18.6 control store (testcontainers). It covers: normal-mode pass-through
// (no open recovery instance; open documentation-only baseline instance);
// default deny while a recovery instance is open (unbound caller, unknown and
// closed instance bindings, missing release, unverified isolation, open gap);
// the legal base state that is allowed; approvals_valid (single/dual,
// executor exclusion, same person under two principals, revoked approvals,
// stale generation, mapping changes); release revocation and generation
// invalidation (old token refuses, new token passes); control-store
// unavailability with a warm cache (no cache-only allow); the phase-two call
// point (hard_gate_active); and the R3 controlled interleaving where a revoke
// that commits after the admission's judgment point cannot invalidate the
// already-admitted action while the next admission refuses.
//
// Fixtures use the real control-store write paths for participants, mappings,
// approvals and releases. Isolation checks and gaps have no write path in this
// batch (T028/T029 arrive later), so their rows are seeded directly; evidence
// generation changes go through T013's real CommitEvidenceWrite protocol.
//
// Docker provider missing: the package TestMain (generation_integration_test.go)
// fails under CI=true or TXHARBOR_REQUIRE_DOCKER=1 and reports NOT RUN locally
// (exit 0) — an unrun PG layer is never a pass.
package recovery

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore/schema"
)

const gatePGImage = "postgres:18.6-trixie"

const gateTestTargetDSN = "postgres://gate-user:gate-pass@gate-target.internal:5432/gate-data"

func gateTestTargetBinding(t *testing.T) GateTargetBinding {
	t.Helper()
	binding, err := GateTargetBindingFromDSN(gateTestTargetDSN)
	if err != nil {
		t.Fatalf("derive trusted gate target binding: %v", err)
	}
	return binding
}

var gateOperationSeq atomic.Int64

func gateOperation(prefix string) string {
	return fmt.Sprintf("gate-test-%s-%d", prefix, gateOperationSeq.Add(1))
}

// gateControlDatabase boots one fresh PostgreSQL container with the migrated
// control-store schema and returns the context, DSN and pool. The pool is not
// closed until the test ends; the version guard is applied separately so the
// T069 negative test can forge a bad version first.
func gateControlDatabase(t *testing.T) (context.Context, string, *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	ctr, err := postgres.Run(ctx, gatePGImage,
		postgres.WithDatabase("txharbor"),
		postgres.WithUsername("txharbor"),
		postgres.WithPassword("txharbor"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2)),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })
	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres connection string: %v", err)
	}
	if err := db.MigrateUp(ctx, db.MigrateOptions{
		DSN: dsn, LockTimeout: 10 * time.Second, ConnectTimeout: 5 * time.Second, FS: schema.FS,
	}, io.Discard); err != nil {
		t.Fatalf("migrate control schema: %v", err)
	}
	pool, err := db.OpenPool(ctx, dsn, 10*time.Second)
	if err != nil {
		t.Fatalf("open control pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return ctx, dsn, pool
}

// gateControlStore is the version-guarded store over a fresh migrated control
// database.
func gateControlStore(t *testing.T) (context.Context, string, *pgxpool.Pool, *controlstore.Store) {
	t.Helper()
	ctx, dsn, pool := gateControlDatabase(t)
	store, err := controlstore.NewStore(ctx, pool)
	if err != nil {
		t.Fatalf("NewStore over migrated control schema: %v", err)
	}
	return ctx, dsn, pool, store
}

// gateNamedPool opens a second pool whose connections carry application_name
// so the interleaving barrier can observe its lock wait in pg_stat_activity.
func gateNamedPool(t *testing.T, dsn, appName string) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	cfg.ConnConfig.RuntimeParams["application_name"] = appName
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("create named pool: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping named pool: %v", err)
	}
	return pool
}

func gateNewGate(t *testing.T, store *controlstore.Store, opts GateOptions) *Gate {
	t.Helper()
	if opts.TTL == 0 {
		opts.TTL = time.Minute
	}
	if opts.TrustedTarget == (GateTargetBinding{}) {
		opts.TrustedTarget = gateTestTargetBinding(t)
	}
	gate, err := NewGate(store, opts)
	if err != nil {
		t.Fatalf("NewGate: %v", err)
	}
	return gate
}

// gateFixture is one open recovery instance with the identity/participant
// baseline every admission needs.
type gateFixture struct {
	ctx        context.Context
	dsn        string
	pool       *pgxpool.Pool
	store      *controlstore.Store
	instanceID string
}

// gateScopeFor returns the canonical capability scope of c for the fixture
// chain (chain 1, asset usdc, business type deposit). T050: the capability
// dimension is part of the canonical scope, and the gate evaluates every
// capability at its own scope.
func gateScopeFor(c Capability) string {
	scope, err := Scope{ChainID: 1, Asset: "usdc", Kind: "deposit", Capability: c}.Canonical()
	if err != nil {
		panic(err) // fixed canonical fixture inputs cannot fail
	}
	return scope
}

// gateScopeOtherAsset is the same canonical scope shape with a different asset
// dimension: a valid but unrelated scope, used to prove a cross-scope approval
// never matches.
func gateScopeOtherAsset(c Capability) string {
	scope, err := Scope{ChainID: 1, Asset: "other", Kind: "deposit", Capability: c}.Canonical()
	if err != nil {
		panic(err) // fixed canonical fixture inputs cannot fail
	}
	return scope
}

func gateOpenRecoveryInstance(t *testing.T, ctx context.Context, store *controlstore.Store) string {
	t.Helper()
	binding := gateTestTargetBinding(t)
	// The fixture starts with a positively-clean known target so tests exercise
	// the release rules unless they deliberately mutate target-guard state.
	if _, err := store.Pool().Exec(ctx, `INSERT INTO recovery_target_guard
		(target_guard_key, disposition, operation_id, clean_at, rebuild_evidence)
		VALUES ($1, 'clean', $2, now(), '{"fixture":"clean"}')
		ON CONFLICT (target_guard_key) DO UPDATE SET disposition='clean', active_writer=FALSE,
		launch_intent=FALSE, attempt_app_name=NULL, launch_intent_at=NULL,
		launched_at=NULL, rebuild_required_at=NULL, clean_at=now(), rebuild_evidence='{"fixture":"clean"}'`,
		binding.TargetGuardKey, gateOperation("clean-target")); err != nil {
		t.Fatalf("seed clean target guard: %v", err)
	}
	result, err := store.OpenInstance(ctx, controlstore.OpenInstanceRequest{
		Kind: "recovery", OpenedBy: "deploy:executor", EntryChainInventory: []uint64{1},
		TargetGuardKey: binding.TargetGuardKey, TargetRoleFingerprint: binding.TargetRoleFingerprint,
	})
	if err != nil {
		t.Fatalf("open recovery instance: %v", err)
	}
	return result.InstanceID
}

func gateMapIdentity(t *testing.T, ctx context.Context, store *controlstore.Store, principal, person string) {
	t.Helper()
	if _, err := store.SetIdentityMapping(ctx, controlstore.SetIdentityMappingRequest{
		Principal: principal, PersonID: person, RecordedBy: "deploy:admin", OperationID: gateOperation("map"),
	}); err != nil {
		t.Fatalf("map identity %s -> %s: %v", principal, person, err)
	}
}

func gateRegister(t *testing.T, ctx context.Context, store *controlstore.Store, instanceID, principal, role string) {
	t.Helper()
	if _, err := store.RegisterParticipant(ctx, controlstore.RegisterParticipantRequest{
		InstanceID: instanceID, Principal: principal, Role: role,
		Actor: "deploy:admin", OperationID: gateOperation("reg"),
	}); err != nil {
		t.Fatalf("register %s as %s: %v", principal, role, err)
	}
}

// gateBaseFixture opens a recovery instance and registers one executor, one
// verifier and one approver with distinct people.
func gateBaseFixture(t *testing.T) *gateFixture {
	t.Helper()
	ctx, dsn, pool, store := gateControlStore(t)
	instanceID := gateOpenRecoveryInstance(t, ctx, store)
	gateMapIdentity(t, ctx, store, "deploy:admin", "person-admin")
	gateMapIdentity(t, ctx, store, "deploy:executor", "person-executor")
	gateMapIdentity(t, ctx, store, "auth:verifier", "person-verifier")
	gateMapIdentity(t, ctx, store, "auth:approver", "person-approver")
	gateRegister(t, ctx, store, instanceID, "deploy:executor", "executor")
	gateRegister(t, ctx, store, instanceID, "auth:verifier", "verifier")
	gateRegister(t, ctx, store, instanceID, "auth:approver", "approver")
	return &gateFixture{ctx: ctx, dsn: dsn, pool: pool, store: store, instanceID: instanceID}
}

func (f *gateFixture) token(t *testing.T) (int64, string) {
	t.Helper()
	return gateInstanceToken(t, f.ctx, f.pool, f.instanceID)
}

func gateInstanceToken(t *testing.T, ctx context.Context, pool *pgxpool.Pool, instanceID string) (int64, string) {
	t.Helper()
	var generation int64
	var hash string
	if err := pool.QueryRow(ctx,
		"SELECT evidence_generation, evidence_hash FROM recovery_instance WHERE instance_id = $1",
		instanceID).Scan(&generation, &hash); err != nil {
		t.Fatalf("read instance evidence token: %v", err)
	}
	return generation, hash
}

func (f *gateFixture) approve(t *testing.T, c Capability, principal, person string, class ApprovalClass) string {
	t.Helper()
	generation, hash := f.token(t)
	result, err := f.store.AppendApprovalDecision(f.ctx, controlstore.ApprovalDecisionRequest{
		InstanceID: f.instanceID, Capability: string(c), ScopeHash: gateScopeFor(c),
		Decision: "approve", ApprovalClassSnapshot: string(class),
		Principal: principal, PersonID: person,
		EvidenceGeneration: generation, EvidenceHash: hash,
		OperationID: gateOperation("approve"),
	})
	if err != nil {
		t.Fatalf("append approval for %s: %v", c, err)
	}
	return result.DecisionID
}

func (f *gateFixture) revokeApproval(t *testing.T, c Capability, principal, person string) string {
	t.Helper()
	generation, hash := f.token(t)
	result, err := f.store.AppendApprovalDecision(f.ctx, controlstore.ApprovalDecisionRequest{
		InstanceID: f.instanceID, Capability: string(c), ScopeHash: gateScopeFor(c),
		Decision: "revoke", ApprovalClassSnapshot: string(ApprovalClassSingleNonExecutor),
		Principal: principal, PersonID: person,
		EvidenceGeneration: generation, EvidenceHash: hash,
		OperationID: gateOperation("approve-revoke"),
	})
	if err != nil {
		t.Fatalf("append approval revoke for %s: %v", c, err)
	}
	return result.DecisionID
}

func (f *gateFixture) release(t *testing.T, c Capability, refs []string) string {
	t.Helper()
	generation, hash := f.token(t)
	result, err := f.store.AppendReleaseDecision(f.ctx, controlstore.ReleaseDecisionRequest{
		InstanceID: f.instanceID, Capability: string(c), ScopeHash: gateScopeFor(c),
		Decision: "release", ApprovalRefs: refs,
		EvidenceGeneration: generation, EvidenceHash: hash,
		Actor: "deploy:executor", OperationID: gateOperation("release"),
	})
	if err != nil {
		t.Fatalf("append release for %s: %v", c, err)
	}
	return result.DecisionID
}

func (f *gateFixture) revokeRelease(t *testing.T, c Capability) string {
	t.Helper()
	generation, hash := f.token(t)
	result, err := f.store.AppendReleaseDecision(f.ctx, controlstore.ReleaseDecisionRequest{
		InstanceID: f.instanceID, Capability: string(c), ScopeHash: gateScopeFor(c),
		Decision: "revoke", EvidenceGeneration: generation, EvidenceHash: hash,
		Actor: "deploy:executor", OperationID: gateOperation("release-revoke"),
	})
	if err != nil {
		t.Fatalf("append release revoke for %s: %v", c, err)
	}
	return result.DecisionID
}

// seedIsolationSet seeds every isolation_dependency_set item of c as verified
// by the non-executor verifier.
func (f *gateFixture) seedIsolationSet(t *testing.T, c Capability) {
	t.Helper()
	items, err := IsolationDependencySet(c)
	if err != nil {
		t.Fatalf("IsolationDependencySet(%s): %v", c, err)
	}
	for _, item := range items {
		f.seedIsolation(t, item, "verified", "auth:verifier")
	}
}

func (f *gateFixture) seedIsolation(t *testing.T, item IsolationItemKey, state, verifiedBy string) {
	t.Helper()
	var evidenceRef, checkedBy, verifier any
	switch state {
	case "pending":
	case "evidenced":
		evidenceRef, checkedBy = "evidence://gate/"+string(item), "auth:verifier"
	case "verified", "rejected":
		evidenceRef, checkedBy, verifier = "evidence://gate/"+string(item), "auth:verifier", verifiedBy
	default:
		t.Fatalf("unsupported isolation fixture state %q", state)
	}
	if _, err := f.pool.Exec(f.ctx, `
INSERT INTO recovery_isolation_check
    (check_id, instance_id, item_key, state, evidence_ref, checked_by, checked_at, verified_by, verified_at)
VALUES (gen_random_uuid(), $1, $2, $3, $4::text, $5::text,
        CASE WHEN $5::text IS NULL THEN NULL ELSE now() END,
        $6::text, CASE WHEN $6::text IS NULL THEN NULL ELSE now() END)
ON CONFLICT ON CONSTRAINT recovery_isolation_check_instance_item_uniq DO UPDATE
SET state = EXCLUDED.state,
    evidence_ref = EXCLUDED.evidence_ref,
    checked_by = EXCLUDED.checked_by,
    checked_at = EXCLUDED.checked_at,
    verified_by = EXCLUDED.verified_by,
    verified_at = EXCLUDED.verified_at`,
		f.instanceID, string(item), state, evidenceRef, checkedBy, verifier); err != nil {
		t.Fatalf("seed isolation item %s: %v", item, err)
	}
}

func (f *gateFixture) openGap(t *testing.T, c Capability) {
	t.Helper()
	if _, err := f.pool.Exec(f.ctx, `
INSERT INTO recovery_gap
    (gap_id, instance_id, object_key, scope, timeline, existing_evidence, required_evidence,
     affected_capabilities, state)
VALUES (gen_random_uuid(), $1, 'gate-test-object', '{}'::jsonb, '{}'::jsonb, '{}'::jsonb,
        '{}'::jsonb, ARRAY[$2]::text[], 'open')`,
		f.instanceID, string(c)); err != nil {
		t.Fatalf("open gap for %s: %v", c, err)
	}
}

// bumpGeneration advances the evidence generation through T013's real
// write-write protocol (an accepted verification-batch write), refreshing the
// instance hash.
func (f *gateFixture) bumpGeneration(t *testing.T) (int64, string) {
	t.Helper()
	token, err := CaptureEvidenceToken(f.ctx, f.pool, f.instanceID)
	if err != nil {
		t.Fatalf("capture evidence token: %v", err)
	}
	outcome, err := CommitEvidenceWrite(f.ctx, f.store, EvidenceWriteRequest{
		InstanceID:   f.instanceID,
		Token:        token,
		Kind:         MutationVerificationBatch,
		Actor:        "deploy:executor",
		Reason:       "gate integration fixture: evidence change",
		OperationID:  gateOperation("generation"),
		ResultDigest: []byte("gate-fixture"),
		Apply: func(ctx context.Context, tx pgx.Tx, accepted EvidenceToken) error {
			_, err := tx.Exec(ctx, `
INSERT INTO recovery_evidence
    (evidence_id, instance_id, generation, kind, scope, artifact_hash, artifact_ref, observed_at, collected_by)
VALUES (gen_random_uuid(), $1, $2, 'verification_batch', '{}'::jsonb, $3, 'gate-fixture', now(), $4)`,
				accepted.InstanceID, accepted.Generation, accepted.Hash, "deploy:executor")
			return err
		},
	})
	if err != nil {
		t.Fatalf("commit evidence write: %v", err)
	}
	if outcome.Discarded {
		t.Fatalf("fixture evidence write was discarded: %s", outcome.DiscardReason)
	}
	return outcome.Token.Generation, outcome.Token.Hash
}

func (f *gateFixture) admit(t *testing.T, gate *Gate, c Capability) GateDecision {
	t.Helper()
	return f.admitScope(t, gate, c, gateScopeFor(c))
}

func (f *gateFixture) admitScope(t *testing.T, gate *Gate, c Capability, scope string) GateDecision {
	t.Helper()
	decision, err := gate.Admit(f.ctx, GateRequest{
		InstanceID: f.instanceID, Capability: c, ScopeHash: scope,
		Actor: "deploy:executor", OperationID: gateOperation("admit"), Action: "test:action",
	})
	if err != nil {
		t.Fatalf("Admit(%s): %v", c, err)
	}
	return decision
}

// warmCache performs one cache-missing admission followed by one that must
// reuse the generation-bound derivation, so tests can then mutate state and
// prove the warm cache is not reused as a stale allow.
func (f *gateFixture) warmCache(t *testing.T, gate *Gate, c Capability) {
	t.Helper()
	if first := f.admit(t, gate, c); !first.Allowed || first.CacheHit {
		t.Fatalf("first admission must be allowed as a cache miss, got %+v", first)
	}
	if second := f.admit(t, gate, c); !second.Allowed || !second.CacheHit {
		t.Fatalf("second admission must be allowed from the generation-bound cache, got %+v", second)
	}
}

func gateAuditCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, refusalClass string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx,
		"SELECT count(*) FROM recovery_audit WHERE action = $1 AND result = 'refused' AND refusal_class = $2",
		GateAuditAction, refusalClass).Scan(&n); err != nil {
		t.Fatalf("count audit rows for %s: %v", refusalClass, err)
	}
	return n
}

func gateAuditTotal(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx,
		"SELECT count(*) FROM recovery_audit WHERE action = $1", GateAuditAction).Scan(&n); err != nil {
		t.Fatalf("count audit rows: %v", err)
	}
	return n
}

// gateAllowedQuery builds the legal base state for capability query: isolation
// set verified, one single non-executor approval, a current release.
func gateAllowedQuery(t *testing.T, f *gateFixture) string {
	t.Helper()
	f.seedIsolationSet(t, CapabilityQuery)
	approvalID := f.approve(t, CapabilityQuery, "auth:approver", "person-approver", ApprovalClassSingleNonExecutor)
	return f.release(t, CapabilityQuery, []string{approvalID})
}

// ---------------------------------------------------------------------------
// Normal mode and binding
// ---------------------------------------------------------------------------

func TestGateNormalModePassThroughWithoutInstance(t *testing.T) {
	ctx, _, pool, store := gateControlStore(t)
	gate := gateNewGate(t, store, GateOptions{})

	decision, err := gate.Admit(ctx, GateRequest{Capability: CapabilityQuery, ScopeHash: gateScopeFor(CapabilityQuery)})
	if err != nil {
		t.Fatalf("Admit: %v", err)
	}
	if !decision.Allowed || !decision.Normal {
		t.Fatalf("no instance must pass through normally, got %+v", decision)
	}
	if decision.RefusalClass != RefusalNoInstance {
		t.Fatalf("normal marker = %q, want %q", decision.RefusalClass, RefusalNoInstance)
	}
	if decision.InstanceID != "" {
		t.Fatalf("normal pass-through must not claim an instance, got %q", decision.InstanceID)
	}
	if n := gateAuditTotal(t, ctx, pool); n != 0 {
		t.Fatalf("normal-mode pass-through wrote %d audit rows, want 0", n)
	}
}

func TestGateBaselineInstanceDoesNotArm(t *testing.T) {
	ctx, dsn, pool, store := gateControlStore(t)
	target, err := controlstore.ParseDSNTarget(dsn)
	if err != nil {
		t.Fatalf("parse fixture target DSN: %v", err)
	}
	targetGuardKey, err := controlstore.TargetGuardKey(target)
	if err != nil {
		t.Fatalf("derive fixture target guard key: %v", err)
	}
	baseline, err := store.OpenInstance(ctx, controlstore.OpenInstanceRequest{
		Kind: "baseline", OpenedBy: "deploy:executor",
		TargetGuardKey: targetGuardKey, TargetRoleFingerprint: target.DataTargetFingerprint().RoleFingerprint,
	})
	if err != nil {
		t.Fatalf("open baseline instance: %v", err)
	}
	gate := gateNewGate(t, store, GateOptions{})

	decision, err := gate.Admit(ctx, GateRequest{Capability: CapabilityQuery, ScopeHash: gateScopeFor(CapabilityQuery)})
	if err != nil {
		t.Fatalf("Admit: %v", err)
	}
	if !decision.Allowed || !decision.Normal {
		t.Fatalf("a documentation-only baseline instance must not arm the gate, got %+v", decision)
	}

	// A binding to a baseline instance is still refused: only an open recovery
	// instance is gateable.
	bound, err := gate.Admit(ctx, GateRequest{InstanceID: baseline.InstanceID, Capability: CapabilityQuery, ScopeHash: gateScopeFor(CapabilityQuery)})
	if err != nil {
		t.Fatalf("bound Admit: %v", err)
	}
	if bound.Allowed || bound.RefusalClass != RefusalInstanceMismatch {
		t.Fatalf("binding to a baseline instance = %+v, want %s", bound, RefusalInstanceMismatch)
	}
	if n := gateAuditCount(t, ctx, pool, string(RefusalInstanceMismatch)); n != 1 {
		t.Fatalf("baseline binding refusal audit rows = %d, want 1", n)
	}
}

func TestGateBoundTargetMustMatchTrustedDatabaseAndRole(t *testing.T) {
	f := gateBaseFixture(t)
	base := gateTestTargetBinding(t)
	cases := []struct {
		name string
		dsn  string
	}{
		{name: "different database", dsn: "postgres://gate-user:gate-pass@gate-target.internal:5432/other-data"},
		{name: "different role", dsn: "postgres://other-user:gate-pass@gate-target.internal:5432/gate-data"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			trusted, err := GateTargetBindingFromDSN(tc.dsn)
			if err != nil {
				t.Fatalf("derive mismatching deployment target: %v", err)
			}
			if base.TargetGuardKey == trusted.TargetGuardKey && base.TargetRoleFingerprint == trusted.TargetRoleFingerprint {
				t.Fatal("test target unexpectedly matches persisted instance target")
			}
			gate := gateNewGate(t, f.store, GateOptions{TrustedTarget: trusted})
			decision := f.admit(t, gate, CapabilityQuery)
			if decision.Allowed || decision.RefusalClass != RefusalInstanceMismatch {
				t.Fatalf("mismatching trusted target must refuse before release evaluation: %+v", decision)
			}
		})
	}
	if n := gateAuditCount(t, f.ctx, f.pool, string(RefusalInstanceMismatch)); n != len(cases) {
		t.Fatalf("target mismatch audit rows = %d, want %d", n, len(cases))
	}
}

func TestGateUnboundWhileRecoveryOpenRefuses(t *testing.T) {
	ctx, _, pool, store := gateControlStore(t)
	instanceID := gateOpenRecoveryInstance(t, ctx, store)
	otherTarget, err := GateTargetBindingFromDSN("postgres://gate-user:gate-pass@gate-target.internal:5432/other-data")
	if err != nil {
		t.Fatalf("derive another trusted target: %v", err)
	}
	gate := gateNewGate(t, store, GateOptions{TrustedTarget: otherTarget})

	decision, err := gate.Admit(ctx, GateRequest{Capability: CapabilityChainScan, ScopeHash: gateScopeFor(CapabilityChainScan)})
	if err != nil {
		t.Fatalf("Admit: %v", err)
	}
	if decision.Allowed || decision.RefusalClass != RefusalInstanceMismatch {
		t.Fatalf("unbound admission while a recovery instance is open = %+v, want %s", decision, RefusalInstanceMismatch)
	}
	if decision.InstanceID != instanceID {
		t.Fatalf("mismatch refusal names instance %q, want %q", decision.InstanceID, instanceID)
	}
	if n := gateAuditCount(t, ctx, pool, string(RefusalInstanceMismatch)); n != 1 {
		t.Fatalf("unbound refusal audit rows = %d, want 1", n)
	}
}

func gateClosedHistoricalRecovery(t *testing.T, ctx context.Context, store *controlstore.Store, pool *pgxpool.Pool, binding GateTargetBinding) string {
	t.Helper()
	if _, err := pool.Exec(ctx, `INSERT INTO recovery_target_guard
		(target_guard_key, disposition, operation_id, clean_at, rebuild_evidence)
		VALUES ($1, 'clean', $2, now(), '{"fixture":"clean"}')
		ON CONFLICT (target_guard_key) DO UPDATE SET disposition='clean', active_writer=FALSE,
		launch_intent=FALSE, attempt_app_name=NULL, launch_intent_at=NULL,
		launched_at=NULL, rebuild_required_at=NULL, clean_at=now(), rebuild_evidence='{"fixture":"clean"}'`,
		binding.TargetGuardKey, gateOperation("historical-clean")); err != nil {
		t.Fatalf("seed historical target guard: %v", err)
	}
	opened, err := store.OpenInstance(ctx, controlstore.OpenInstanceRequest{
		Kind: "recovery", OpenedBy: "deploy:executor", EntryChainInventory: []uint64{1},
		TargetGuardKey: binding.TargetGuardKey, TargetRoleFingerprint: binding.TargetRoleFingerprint,
	})
	if err != nil {
		t.Fatalf("open historical recovery instance: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE recovery_instance SET state='closed', closed_by='gate-test', closed_at=now() WHERE instance_id=$1`, opened.InstanceID); err != nil {
		t.Fatalf("close historical recovery instance: %v", err)
	}
	return opened.InstanceID
}

func TestGateUnboundHistoricalDirtyGuardIsScopedToTrustedTarget(t *testing.T) {
	for _, tc := range []struct {
		name       string
		sameTarget bool
		wantAllow  bool
		missing    bool
		active     bool
	}{
		{name: "distinct trusted target passes", wantAllow: true},
		{name: "same target unresolved guard refuses", sameTarget: true},
		{name: "same target missing guard refuses", sameTarget: true, missing: true},
		{name: "same target active writer refuses", sameTarget: true, active: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, _, pool, store := gateControlStore(t)
			targetA := gateTestTargetBinding(t)
			targetB := targetA
			if !tc.sameTarget {
				var err error
				targetB, err = GateTargetBindingFromDSN("postgres://gate-user:gate-pass@gate-target.internal:5432/other-data")
				if err != nil {
					t.Fatalf("derive trusted target B: %v", err)
				}
			}
			gateClosedHistoricalRecovery(t, ctx, store, pool, targetA)
			if tc.missing {
				if _, err := pool.Exec(ctx, `ALTER TABLE recovery_target_guard DISABLE TRIGGER ALL`); err != nil {
					t.Fatalf("disable historical target foreign-key trigger: %v", err)
				}
				if _, err := pool.Exec(ctx, `DELETE FROM recovery_target_guard WHERE target_guard_key=$1`, targetA.TargetGuardKey); err != nil {
					t.Fatalf("remove historical target guard: %v", err)
				}
				if _, err := pool.Exec(ctx, `ALTER TABLE recovery_target_guard ENABLE TRIGGER ALL`); err != nil {
					t.Fatalf("restore historical target foreign-key trigger: %v", err)
				}
			} else if tc.active {
				if _, err := pool.Exec(ctx, `UPDATE recovery_target_guard SET disposition='unknown', clean_at=NULL, rebuild_evidence=NULL,
					active_writer=TRUE, launch_intent=TRUE, attempt_app_name='gate-test-writer', launch_intent_at=now()
					WHERE target_guard_key=$1`, targetA.TargetGuardKey); err != nil {
					t.Fatalf("activate historical target guard writer: %v", err)
				}
			} else {
				if _, err := pool.Exec(ctx, `UPDATE recovery_target_guard SET disposition='unknown', clean_at=NULL, rebuild_evidence=NULL WHERE target_guard_key=$1`, targetA.TargetGuardKey); err != nil {
					t.Fatalf("make historical target guard unresolved: %v", err)
				}
			}
			decision, err := gateNewGate(t, store, GateOptions{TrustedTarget: targetB}).Admit(ctx,
				GateRequest{Capability: CapabilityQuery, ScopeHash: gateScopeFor(CapabilityQuery)})
			if err != nil {
				t.Fatalf("unbound Admit: %v", err)
			}
			if decision.Allowed != tc.wantAllow {
				t.Fatalf("decision = %+v, allowed=%t want %t", decision, decision.Allowed, tc.wantAllow)
			}
			if !tc.wantAllow && decision.RefusalClass != RefusalIsolationUnproven {
				t.Fatalf("same-target dirty guard refusal = %+v, want %s", decision, RefusalIsolationUnproven)
			}
		})
	}
}

func TestGateUnboundLegacyUnknownHistoricalTargetBlocksAllTargets(t *testing.T) {
	ctx, _, pool, store := gateControlStore(t)
	targetA := gateTestTargetBinding(t)
	gateClosedHistoricalRecovery(t, ctx, store, pool, targetA)
	if _, err := pool.Exec(ctx, `ALTER TABLE recovery_instance DISABLE TRIGGER recovery_instance_target_guard_immutable_trg`); err != nil {
		t.Fatalf("disable immutable binding trigger: %v", err)
	}
	_, updateErr := pool.Exec(ctx, `UPDATE recovery_instance SET target_guard_key=NULL, target_role_fingerprint=NULL WHERE kind='recovery'`)
	_, enableErr := pool.Exec(ctx, `ALTER TABLE recovery_instance ENABLE TRIGGER recovery_instance_target_guard_immutable_trg`)
	if updateErr != nil {
		t.Fatalf("simulate legacy-null target binding: %v", updateErr)
	}
	if enableErr != nil {
		t.Fatalf("restore immutable binding trigger: %v", enableErr)
	}
	targetB, err := GateTargetBindingFromDSN("postgres://gate-user:gate-pass@gate-target.internal:5432/other-data")
	if err != nil {
		t.Fatalf("derive trusted target B: %v", err)
	}
	decision, err := gateNewGate(t, store, GateOptions{TrustedTarget: targetB}).Admit(ctx,
		GateRequest{Capability: CapabilityQuery, ScopeHash: gateScopeFor(CapabilityQuery)})
	if err != nil {
		t.Fatalf("unbound Admit: %v", err)
	}
	if decision.Allowed || decision.RefusalClass != RefusalIsolationUnproven {
		t.Fatalf("legacy-null unknown target must block normal pass-through globally: %+v", decision)
	}
}

func TestGateUnknownAndClosedInstanceBindingsRefuse(t *testing.T) {
	ctx, _, pool, store := gateControlStore(t)
	f := &gateFixture{ctx: ctx, pool: pool, store: store}
	gate := gateNewGate(t, store, GateOptions{})

	unknown, err := gate.Admit(ctx, GateRequest{
		InstanceID: "11111111-2222-3333-4444-555555555555",
		Capability: CapabilityQuery, ScopeHash: gateScopeFor(CapabilityQuery),
	})
	if err != nil {
		t.Fatalf("Admit(unknown instance): %v", err)
	}
	if unknown.Allowed || unknown.RefusalClass != RefusalInstanceMismatch {
		t.Fatalf("unknown instance binding = %+v, want %s", unknown, RefusalInstanceMismatch)
	}

	f.instanceID = gateOpenRecoveryInstance(t, ctx, store)
	// Close the instance directly (T027's close path is a later batch): a
	// binding to a closed instance must still refuse.
	if _, err := pool.Exec(ctx,
		"UPDATE recovery_instance SET state = 'closed', closed_by = 'deploy:admin', closed_at = now() WHERE instance_id = $1",
		f.instanceID); err != nil {
		t.Fatalf("close instance: %v", err)
	}
	closed, err := gate.Admit(ctx, GateRequest{
		InstanceID: f.instanceID, Capability: CapabilityQuery, ScopeHash: gateScopeFor(CapabilityQuery),
	})
	if err != nil {
		t.Fatalf("Admit(closed instance): %v", err)
	}
	if closed.Allowed || closed.RefusalClass != RefusalInstanceMismatch {
		t.Fatalf("closed instance binding = %+v, want %s", closed, RefusalInstanceMismatch)
	}
}

// ---------------------------------------------------------------------------
// Legal base state and default deny
// ---------------------------------------------------------------------------

func TestGateLegalBaseStateAllowsAndAudits(t *testing.T) {
	f := gateBaseFixture(t)
	releaseID := gateAllowedQuery(t, f)
	gate := gateNewGate(t, f.store, GateOptions{})

	first := f.admit(t, gate, CapabilityQuery)
	if !first.Allowed || first.Normal {
		t.Fatalf("legal base state must be allowed in recovery mode, got %+v", first)
	}
	if first.DecisionRef != releaseID {
		t.Fatalf("decision ref = %q, want release %q", first.DecisionRef, releaseID)
	}
	if first.CacheHit {
		t.Fatal("the first admission must be a cache miss")
	}
	if first.EvidenceGeneration != 0 || first.EvidenceHash == "" {
		t.Fatalf("decision must carry the in-lock token, got generation=%d hash=%q", first.EvidenceGeneration, first.EvidenceHash)
	}
	if n := gateAuditTotal(t, f.ctx, f.pool); n != 1 {
		t.Fatalf("allowed admission audit rows = %d, want 1", n)
	}

	second := f.admit(t, gate, CapabilityQuery)
	if !second.Allowed || !second.CacheHit {
		t.Fatalf("second admission must reuse the generation-bound cache, got %+v", second)
	}
	var result, refusal string
	var generation *int64
	if err := f.pool.QueryRow(f.ctx,
		"SELECT result, COALESCE(refusal_class, ''), evidence_generation FROM recovery_audit WHERE action = $1 ORDER BY audit_id DESC LIMIT 1",
		GateAuditAction).Scan(&result, &refusal, &generation); err != nil {
		t.Fatalf("read latest gate audit: %v", err)
	}
	if result != "ok" || refusal != "" || generation == nil || *generation != 0 {
		t.Fatalf("allowed audit row = result=%s refusal=%q generation=%v", result, refusal, generation)
	}
}

func TestGateRequiresCleanPersistedTargetGuardBeforeReleaseEvaluation(t *testing.T) {
	f := gateBaseFixture(t)
	binding := gateTestTargetBinding(t)
	gate := gateNewGate(t, f.store, GateOptions{})
	gateAllowedQuery(t, f)

	clean := f.admit(t, gate, CapabilityQuery)
	if !clean.Allowed {
		t.Fatalf("clean bound target should reach and pass release evaluation, got %+v", clean)
	}
	if _, err := f.pool.Exec(f.ctx, `UPDATE recovery_target_guard
		SET disposition='unknown', clean_at=NULL, rebuild_evidence=NULL
		WHERE target_guard_key=$1`, binding.TargetGuardKey); err != nil {
		t.Fatalf("make target guard unresolved: %v", err)
	}
	dirty := f.admit(t, gate, CapabilityQuery)
	if dirty.Allowed || dirty.RefusalClass != RefusalIsolationUnproven {
		t.Fatalf("unresolved target guard must refuse before release: %+v", dirty)
	}
}

func TestGateAllowsCleanGuardAfterLaunchedAttemptAccepted(t *testing.T) {
	f := gateBaseFixture(t)
	gate := gateNewGate(t, f.store, GateOptions{})
	key := gateTestTargetBinding(t).TargetGuardKey
	tx, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatalf("begin target attempt: %v", err)
	}
	defer tx.Rollback(f.ctx)
	operationID := gateOperation("attempt")
	if err := controlstore.PrepareTargetGuard(f.ctx, tx, key, operationID); err != nil {
		t.Fatalf("prepare target guard: %v", err)
	}
	if err := controlstore.MarkTargetGuardLaunchIntent(f.ctx, tx, key, "gate-test-app", operationID); err != nil {
		t.Fatalf("mark target launch intent: %v", err)
	}
	if err := controlstore.MarkTargetGuardLaunched(f.ctx, tx, key); err != nil {
		t.Fatalf("mark target launched: %v", err)
	}
	if err := controlstore.MarkTargetGuardWriterDrained(f.ctx, tx, key); err != nil {
		t.Fatalf("mark target writer drained: %v", err)
	}
	if err := controlstore.AcceptTargetGuardClean(f.ctx, tx, key, gateOperation("accepted"), []byte(`{"success":true}`)); err != nil {
		t.Fatalf("accept target guard clean: %v", err)
	}
	if err := tx.Commit(f.ctx); err != nil {
		t.Fatalf("commit accepted target attempt: %v", err)
	}
	gateAllowedQuery(t, f)
	decision := f.admit(t, gate, CapabilityQuery)
	if !decision.Allowed {
		t.Fatalf("accepted clean guard with historical launch intent should pass gate, got %+v", decision)
	}
}

func TestGateUnboundDeniesClosedLegacyRecoveryWithNullTargetBinding(t *testing.T) {
	ctx, _, pool, store := gateControlStore(t)
	instanceID := gateOpenRecoveryInstance(t, ctx, store)
	if _, err := pool.Exec(ctx, `UPDATE recovery_instance SET state='closed', closed_by='gate-test', closed_at=now() WHERE instance_id=$1`, instanceID); err != nil {
		t.Fatalf("close fixture recovery instance: %v", err)
	}
	if _, err := pool.Exec(ctx, `ALTER TABLE recovery_instance DISABLE TRIGGER recovery_instance_target_guard_immutable_trg`); err != nil {
		t.Fatalf("temporarily disable immutable binding trigger: %v", err)
	}
	_, updateErr := pool.Exec(ctx, `UPDATE recovery_instance
		SET target_guard_key=NULL, target_role_fingerprint=NULL WHERE instance_id=$1`, instanceID)
	_, enableErr := pool.Exec(ctx, `ALTER TABLE recovery_instance ENABLE TRIGGER recovery_instance_target_guard_immutable_trg`)
	if updateErr != nil {
		t.Fatalf("simulate legacy-null recovery binding: %v", updateErr)
	}
	if enableErr != nil {
		t.Fatalf("restore immutable binding trigger: %v", enableErr)
	}
	decision, err := gateNewGate(t, store, GateOptions{}).Admit(ctx,
		GateRequest{Capability: CapabilityQuery, ScopeHash: gateScopeFor(CapabilityQuery)})
	if err != nil {
		t.Fatalf("unbound Admit: %v", err)
	}
	if decision.Allowed || decision.RefusalClass != RefusalIsolationUnproven {
		t.Fatalf("closed legacy-null recovery binding must deny normal pass-through: %+v", decision)
	}
}

func TestGateUnknownAndDirtyTargetGuardsDeny(t *testing.T) {
	for _, tc := range []struct {
		name  string
		state string
	}{
		{name: "unknown", state: "unknown"},
		{name: "rebuild-required", state: "rebuild_required"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := gateBaseFixture(t)
			binding := gateTestTargetBinding(t)
			if _, err := f.pool.Exec(f.ctx, `UPDATE recovery_target_guard
				SET disposition=$2, clean_at=NULL, rebuild_evidence=NULL,
				    rebuild_required_at=CASE WHEN $2='rebuild_required' THEN now() ELSE NULL END
				WHERE target_guard_key=$1`, binding.TargetGuardKey, tc.state); err != nil {
				t.Fatalf("set target guard state: %v", err)
			}
			decision := f.admit(t, gateNewGate(t, f.store, GateOptions{}), CapabilityQuery)
			if decision.Allowed || decision.RefusalClass != RefusalIsolationUnproven {
				t.Fatalf("target state %q must deny before release evaluation: %+v", tc.state, decision)
			}
		})
	}
}

func TestGateDefaultDenyWithoutRelease(t *testing.T) {
	f := gateBaseFixture(t)
	f.seedIsolationSet(t, CapabilityQuery) // isolation satisfied; the release is missing
	gate := gateNewGate(t, f.store, GateOptions{})

	decision := f.admit(t, gate, CapabilityQuery)
	if decision.Allowed || decision.RefusalClass != RefusalNoRelease {
		t.Fatalf("no release = %+v, want %s", decision, RefusalNoRelease)
	}
	if n := gateAuditCount(t, f.ctx, f.pool, string(RefusalNoRelease)); n != 1 {
		t.Fatalf("no_release audit rows = %d, want 1", n)
	}
}

func TestGateIsolationUnprovenRefused(t *testing.T) {
	// Missing items: nothing seeded at all.
	f := gateBaseFixture(t)
	approvalID := f.approve(t, CapabilityQuery, "auth:approver", "person-approver", ApprovalClassSingleNonExecutor)
	f.release(t, CapabilityQuery, []string{approvalID})
	gate := gateNewGate(t, f.store, GateOptions{})
	if decision := f.admit(t, gate, CapabilityQuery); decision.Allowed || decision.RefusalClass != RefusalIsolationUnproven {
		t.Fatalf("missing isolation items = %+v, want %s", decision, RefusalIsolationUnproven)
	}

	// Self-verified isolation: the verifier resolves to the instance executor.
	f2 := gateBaseFixture(t)
	f2.seedIsolation(t, IsolationItemOldWritersStopped, "verified", "deploy:executor")
	for _, item := range []IsolationItemKey{IsolationItemNetworkIsolation, IsolationItemVersionCompatible} {
		f2.seedIsolation(t, item, "verified", "auth:verifier")
	}
	approvalID2 := f2.approve(t, CapabilityQuery, "auth:approver", "person-approver", ApprovalClassSingleNonExecutor)
	f2.release(t, CapabilityQuery, []string{approvalID2})
	gate2 := gateNewGate(t, f2.store, GateOptions{})
	decision := f2.admit(t, gate2, CapabilityQuery)
	if decision.Allowed || decision.RefusalClass != RefusalIsolationUnproven {
		t.Fatalf("executor self-verified isolation = %+v, want %s", decision, RefusalIsolationUnproven)
	}
}

func TestGateOpenGapRefused(t *testing.T) {
	f := gateBaseFixture(t)
	gateAllowedQuery(t, f)
	f.openGap(t, CapabilityQuery)
	gate := gateNewGate(t, f.store, GateOptions{})

	decision := f.admit(t, gate, CapabilityQuery)
	if decision.Allowed || decision.RefusalClass != RefusalGapOpen {
		t.Fatalf("open gap = %+v, want %s", decision, RefusalGapOpen)
	}
	if n := gateAuditCount(t, f.ctx, f.pool, string(RefusalGapOpen)); n != 1 {
		t.Fatalf("gap_open audit rows = %d, want 1", n)
	}
}

// ---------------------------------------------------------------------------
// Revocation, generation and the cache
// ---------------------------------------------------------------------------

func TestGateRevokeBeforeAdmissionRefuses(t *testing.T) {
	f := gateBaseFixture(t)
	gateAllowedQuery(t, f)
	f.revokeRelease(t, CapabilityQuery)
	gate := gateNewGate(t, f.store, GateOptions{})

	decision := f.admit(t, gate, CapabilityQuery)
	if decision.Allowed || decision.RefusalClass != RefusalReleaseRevoked {
		t.Fatalf("revoke before admission = %+v, want %s", decision, RefusalReleaseRevoked)
	}
	if n := gateAuditCount(t, f.ctx, f.pool, string(RefusalReleaseRevoked)); n != 1 {
		t.Fatalf("release_revoked audit rows = %d, want 1", n)
	}
}

func TestGateGenerationChangeInvalidatesReleaseAndOldApproval(t *testing.T) {
	f := gateBaseFixture(t)
	oldReleaseID := gateAllowedQuery(t, f)
	gate := gateNewGate(t, f.store, GateOptions{})
	f.warmCache(t, gate, CapabilityQuery)

	// A real accepted evidence write advances generation+hash; the cached
	// generation-bound derivation must not be reused and the release bound to
	// the old token must fail.
	generation, hash := f.bumpGeneration(t)
	decision := f.admit(t, gate, CapabilityQuery)
	if decision.Allowed || decision.RefusalClass != RefusalReleaseInvalidatedGeneration {
		t.Fatalf("after evidence change = %+v, want %s", decision, RefusalReleaseInvalidatedGeneration)
	}
	if decision.EvidenceGeneration != generation || decision.EvidenceHash != hash {
		t.Fatalf("refusal must carry the new token, got generation=%d hash=%q", decision.EvidenceGeneration, decision.EvidenceHash)
	}

	// A fresh approval + release at the new token passes (旧令牌拒、新令牌通).
	approvalID := f.approve(t, CapabilityQuery, "auth:approver", "person-approver", ApprovalClassSingleNonExecutor)
	newReleaseID := f.release(t, CapabilityQuery, []string{approvalID})
	decision = f.admit(t, gate, CapabilityQuery)
	if !decision.Allowed || decision.DecisionRef != newReleaseID || decision.DecisionRef == oldReleaseID {
		t.Fatalf("new-token release = %+v, want allow at release %s", decision, newReleaseID)
	}
}

func TestGateStaleApprovalRefused(t *testing.T) {
	f := gateBaseFixture(t)
	f.seedIsolationSet(t, CapabilityQuery)
	approvalID := f.approve(t, CapabilityQuery, "auth:approver", "person-approver", ApprovalClassSingleNonExecutor)
	// The evidence changes after the approval; the release is then recorded at
	// the new token but still references the stale approval.
	f.bumpGeneration(t)
	f.release(t, CapabilityQuery, []string{approvalID})
	gate := gateNewGate(t, f.store, GateOptions{})

	decision := f.admit(t, gate, CapabilityQuery)
	if decision.Allowed || decision.RefusalClass != RefusalApprovalStale {
		t.Fatalf("stale approval = %+v, want %s", decision, RefusalApprovalStale)
	}
}

func TestGateApprovalRevokeAfterCacheWarmRefuses(t *testing.T) {
	f := gateBaseFixture(t)
	gateAllowedQuery(t, f)
	gate := gateNewGate(t, f.store, GateOptions{})
	f.warmCache(t, gate, CapabilityQuery)

	// The approval is revoked without any generation change; the warm cache
	// must not be reused as a stale allow.
	f.revokeApproval(t, CapabilityQuery, "auth:approver", "person-approver")
	decision := f.admit(t, gate, CapabilityQuery)
	if decision.Allowed || decision.RefusalClass != RefusalApprovalMissing {
		t.Fatalf("revoked approval with warm cache = %+v, want %s", decision, RefusalApprovalMissing)
	}
}

func TestGateReleaseRevokeAfterCacheWarmRefuses(t *testing.T) {
	f := gateBaseFixture(t)
	gateAllowedQuery(t, f)
	gate := gateNewGate(t, f.store, GateOptions{})
	f.warmCache(t, gate, CapabilityQuery)

	f.revokeRelease(t, CapabilityQuery)
	decision := f.admit(t, gate, CapabilityQuery)
	if decision.Allowed || decision.RefusalClass != RefusalReleaseRevoked {
		t.Fatalf("revoked release with warm cache = %+v, want %s", decision, RefusalReleaseRevoked)
	}
}

func TestGateMappingChangeAfterCacheWarmRefuses(t *testing.T) {
	f := gateBaseFixture(t)
	gateAllowedQuery(t, f)
	gate := gateNewGate(t, f.store, GateOptions{})
	f.warmCache(t, gate, CapabilityQuery)

	// F19: the approver is re-mapped to another person; the approval recorded
	// under the old mapping is invalid (no generation change involved).
	gateMapIdentity(t, f.ctx, f.store, "auth:approver", "person-approver-other")
	decision := f.admit(t, gate, CapabilityQuery)
	if decision.Allowed || decision.RefusalClass != RefusalApprovalIdentityUnverified {
		t.Fatalf("mapping change with warm cache = %+v, want %s", decision, RefusalApprovalIdentityUnverified)
	}
}

func TestGateControlStoreUnavailableRefusesEvenWithWarmCache(t *testing.T) {
	f := gateBaseFixture(t)
	gateAllowedQuery(t, f)
	now := time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)
	gate := gateNewGate(t, f.store, GateOptions{TTL: time.Minute, Now: func() time.Time { return now }})
	f.warmCache(t, gate, CapabilityQuery)

	// The control store becomes unreachable. The admission must refuse (the
	// authoritative in-lock read is unconditional), never reuse the old allow.
	f.pool.Close()
	decision, err := gate.Admit(f.ctx, GateRequest{
		InstanceID: f.instanceID, Capability: CapabilityQuery, ScopeHash: gateScopeFor(CapabilityQuery),
	})
	if err == nil || !errors.Is(err, ErrGateControlStoreUnavailable) {
		t.Fatalf("unreachable store error = %v, want ErrGateControlStoreUnavailable", err)
	}
	if decision.Allowed || decision.RefusalClass != RefusalControlStoreUnavailable {
		t.Fatalf("unreachable store = %+v, want %s", decision, RefusalControlStoreUnavailable)
	}

	// Expired cache + unreachable store: still refused (same fail-closed path).
	now = now.Add(2 * time.Minute)
	decision, err = gate.Admit(f.ctx, GateRequest{
		InstanceID: f.instanceID, Capability: CapabilityQuery, ScopeHash: gateScopeFor(CapabilityQuery),
	})
	if err == nil || decision.Allowed || decision.RefusalClass != RefusalControlStoreUnavailable {
		t.Fatalf("expired cache + unreachable store = %+v / %v, want %s", decision, err, RefusalControlStoreUnavailable)
	}
}

// ---------------------------------------------------------------------------
// approvals_valid
// ---------------------------------------------------------------------------

func TestGateApprovalMissingCases(t *testing.T) {
	// A release with no approval references refuses.
	f := gateBaseFixture(t)
	f.seedIsolationSet(t, CapabilityQuery)
	f.release(t, CapabilityQuery, nil)
	gate := gateNewGate(t, f.store, GateOptions{})
	if decision := f.admit(t, gate, CapabilityQuery); decision.Allowed || decision.RefusalClass != RefusalApprovalMissing {
		t.Fatalf("release without approvals = %+v, want %s", decision, RefusalApprovalMissing)
	}

	// A release referencing an approval of a different scope refuses as
	// scope_mismatch.
	f2 := gateBaseFixture(t)
	f2.seedIsolationSet(t, CapabilityQuery)
	otherScopeApproval := func() string {
		generation, hash := f2.token(t)
		result, err := f2.store.AppendApprovalDecision(f2.ctx, controlstore.ApprovalDecisionRequest{
			InstanceID: f2.instanceID, Capability: string(CapabilityQuery), ScopeHash: gateScopeOtherAsset(CapabilityQuery),
			Decision: "approve", ApprovalClassSnapshot: string(ApprovalClassSingleNonExecutor),
			Principal: "auth:approver", PersonID: "person-approver",
			EvidenceGeneration: generation, EvidenceHash: hash, OperationID: gateOperation("approve-other-scope"),
		})
		if err != nil {
			t.Fatalf("append other-scope approval: %v", err)
		}
		return result.DecisionID
	}()
	f2.release(t, CapabilityQuery, []string{otherScopeApproval})
	gate2 := gateNewGate(t, f2.store, GateOptions{})
	if decision := f2.admit(t, gate2, CapabilityQuery); decision.Allowed || decision.RefusalClass != RefusalScopeMismatch {
		t.Fatalf("cross-scope approval = %+v, want %s", decision, RefusalScopeMismatch)
	}

	// More approval references than the gate accepts: refused without reading
	// an unbounded basis.
	f4 := gateBaseFixture(t)
	f4.seedIsolationSet(t, CapabilityQuery)
	refs := make([]string, 0, maxGateApprovalRefs+1)
	for i := 0; i <= maxGateApprovalRefs; i++ {
		refs = append(refs, fmt.Sprintf("00000000-0000-4000-8000-%012d", i))
	}
	f4.release(t, CapabilityQuery, refs)
	gate4 := gateNewGate(t, f4.store, GateOptions{})
	if decision := f4.admit(t, gate4, CapabilityQuery); decision.Allowed || decision.RefusalClass != RefusalApprovalMissing {
		t.Fatalf("oversized approval basis = %+v, want %s", decision, RefusalApprovalMissing)
	}

	// The approver resolves to this instance's executor person.
	f3 := gateBaseFixture(t)
	gateMapIdentity(t, f3.ctx, f3.store, "deploy:executor", "person-executor")
	gateRegister(t, f3.ctx, f3.store, f3.instanceID, "deploy:executor", "approver")
	f3.seedIsolationSet(t, CapabilityQuery)
	executorApproval := f3.approve(t, CapabilityQuery, "deploy:executor", "person-executor", ApprovalClassSingleNonExecutor)
	f3.release(t, CapabilityQuery, []string{executorApproval})
	gate3 := gateNewGate(t, f3.store, GateOptions{})
	decision := f3.admit(t, gate3, CapabilityQuery)
	if decision.Allowed || decision.RefusalClass != RefusalApprovalExecutorExcluded {
		t.Fatalf("executor self-approval = %+v, want %s", decision, RefusalApprovalExecutorExcluded)
	}
	if n := gateAuditCount(t, f3.ctx, f3.pool, string(RefusalApprovalExecutorExcluded)); n != 1 {
		t.Fatalf("executor exclusion audit rows = %d, want 1", n)
	}
}

func TestGateDualApprovalRules(t *testing.T) {
	// Same person under two principals is not two people.
	f := gateBaseFixture(t)
	gateMapIdentity(t, f.ctx, f.store, "auth:approver-2", "person-approver") // same person
	gateRegister(t, f.ctx, f.store, f.instanceID, "auth:approver-2", "approver")
	f.seedIsolationSet(t, CapabilityChainScan)
	f.seedIsolationSet(t, CapabilityExistingWithdrawalRecovery)
	chainApproval := f.approve(t, CapabilityChainScan, "auth:approver", "person-approver", ApprovalClassSingleNonExecutor)
	f.release(t, CapabilityChainScan, []string{chainApproval})
	first := f.approve(t, CapabilityExistingWithdrawalRecovery, "auth:approver", "person-approver", ApprovalClassDualNonExecutor)
	second := f.approve(t, CapabilityExistingWithdrawalRecovery, "auth:approver-2", "person-approver", ApprovalClassDualNonExecutor)
	f.release(t, CapabilityExistingWithdrawalRecovery, []string{first, second})
	gate := gateNewGate(t, f.store, GateOptions{})
	decision := f.admit(t, gate, CapabilityExistingWithdrawalRecovery)
	if decision.Allowed || decision.RefusalClass != RefusalApprovalIdentityUnverified {
		t.Fatalf("same person dual approval = %+v, want %s", decision, RefusalApprovalIdentityUnverified)
	}

	// Two distinct people, capability dependency satisfied: allowed.
	f2 := gateBaseFixture(t)
	gateMapIdentity(t, f2.ctx, f2.store, "auth:approver-2", "person-approver-2")
	gateRegister(t, f2.ctx, f2.store, f2.instanceID, "auth:approver-2", "approver")
	f2.seedIsolationSet(t, CapabilityChainScan)
	f2.seedIsolationSet(t, CapabilityExistingWithdrawalRecovery)
	chainApproval2 := f2.approve(t, CapabilityChainScan, "auth:approver", "person-approver", ApprovalClassSingleNonExecutor)
	f2.release(t, CapabilityChainScan, []string{chainApproval2})
	first2 := f2.approve(t, CapabilityExistingWithdrawalRecovery, "auth:approver", "person-approver", ApprovalClassDualNonExecutor)
	second2 := f2.approve(t, CapabilityExistingWithdrawalRecovery, "auth:approver-2", "person-approver-2", ApprovalClassDualNonExecutor)
	f2.release(t, CapabilityExistingWithdrawalRecovery, []string{first2, second2})
	gate2 := gateNewGate(t, f2.store, GateOptions{})
	if decision := f2.admit(t, gate2, CapabilityExistingWithdrawalRecovery); !decision.Allowed {
		t.Fatalf("dual approval by two people = %+v, want allow", decision)
	}

	// Dual required but only one approval referenced: refused.
	f3 := gateBaseFixture(t)
	f3.seedIsolationSet(t, CapabilityChainScan)
	f3.seedIsolationSet(t, CapabilityExistingWithdrawalRecovery)
	chainApproval3 := f3.approve(t, CapabilityChainScan, "auth:approver", "person-approver", ApprovalClassSingleNonExecutor)
	f3.release(t, CapabilityChainScan, []string{chainApproval3})
	single := f3.approve(t, CapabilityExistingWithdrawalRecovery, "auth:approver", "person-approver", ApprovalClassDualNonExecutor)
	f3.release(t, CapabilityExistingWithdrawalRecovery, []string{single})
	gate3 := gateNewGate(t, f3.store, GateOptions{})
	if decision := f3.admit(t, gate3, CapabilityExistingWithdrawalRecovery); decision.Allowed || decision.RefusalClass != RefusalApprovalMissing {
		t.Fatalf("single approval for dual capability = %+v, want %s", decision, RefusalApprovalMissing)
	}
}

func TestGateCapabilityDependencyClosed(t *testing.T) {
	f := gateBaseFixture(t)
	gateMapIdentity(t, f.ctx, f.store, "auth:approver-2", "person-approver-2")
	gateRegister(t, f.ctx, f.store, f.instanceID, "auth:approver-2", "approver")
	f.seedIsolationSet(t, CapabilityChainScan)
	f.seedIsolationSet(t, CapabilityExistingWithdrawalRecovery)
	// existing_withdrawal_recovery is released and approved, but its required
	// chain_scan is not.
	first := f.approve(t, CapabilityExistingWithdrawalRecovery, "auth:approver", "person-approver", ApprovalClassDualNonExecutor)
	second := f.approve(t, CapabilityExistingWithdrawalRecovery, "auth:approver-2", "person-approver-2", ApprovalClassDualNonExecutor)
	f.release(t, CapabilityExistingWithdrawalRecovery, []string{first, second})
	gate := gateNewGate(t, f.store, GateOptions{})

	decision := f.admit(t, gate, CapabilityExistingWithdrawalRecovery)
	if decision.Allowed || decision.RefusalClass != RefusalCapabilityDependencyClosed {
		t.Fatalf("closed dependency = %+v, want %s", decision, RefusalCapabilityDependencyClosed)
	}
	if n := gateAuditCount(t, f.ctx, f.pool, string(RefusalCapabilityDependencyClosed)); n != 1 {
		t.Fatalf("dependency refusal audit rows = %d, want 1", n)
	}
}

// ---------------------------------------------------------------------------
// Phase-two call point and the controlled interleaving
// ---------------------------------------------------------------------------

func TestGateHardGateActiveCallPoint(t *testing.T) {
	f := gateBaseFixture(t)
	gateAllowedQuery(t, f)

	failing := gateNewGate(t, f.store, GateOptions{FundGates: func(context.Context, GateRequest) error {
		return errors.New("capacity red line active")
	}})
	decision := f.admit(t, failing, CapabilityQuery)
	if decision.Allowed || decision.RefusalClass != RefusalHardGateActive {
		t.Fatalf("failing fund gate = %+v, want %s", decision, RefusalHardGateActive)
	}

	passing := gateNewGate(t, f.store, GateOptions{FundGates: func(context.Context, GateRequest) error { return nil }})
	decision = f.admit(t, passing, CapabilityQuery)
	if !decision.Allowed || !decision.PhaseTwoEvaluated {
		t.Fatalf("passing fund gate = %+v, want allow with phase two evaluated", decision)
	}

	// No checker wired: phase two stays the action site's obligation, and the
	// decision says so.
	plain := gateNewGate(t, f.store, GateOptions{})
	decision = f.admit(t, plain, CapabilityQuery)
	if !decision.Allowed || decision.PhaseTwoEvaluated {
		t.Fatalf("unwired phase two = %+v, want allow with PhaseTwoEvaluated=false", decision)
	}
}

// TestGateControlledInterleavingAdmissionBeforeRevoke proves the R3 ordering:
// the judgment point is inside the instance row lock, a revoke that arrives
// while the admission holds the lock cannot invalidate that admitted action
// (in-flight semantics), and the next admission refuses because the revoke
// committed first this time.
func TestGateControlledInterleavingAdmissionBeforeRevoke(t *testing.T) {
	f := gateBaseFixture(t)
	gateAllowedQuery(t, f)

	entered := make(chan struct{})
	releaseAdmission := make(chan struct{})
	gate := gateNewGate(t, f.store, GateOptions{FundGates: func(context.Context, GateRequest) error {
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-releaseAdmission
		return nil
	}})

	type admitResult struct {
		decision GateDecision
		err      error
	}
	admitted := make(chan admitResult, 1)
	go func() {
		decision, err := gate.Admit(f.ctx, GateRequest{
			InstanceID: f.instanceID, Capability: CapabilityQuery, ScopeHash: gateScopeFor(CapabilityQuery),
			Actor: "deploy:executor", Action: "test:interleaving",
		})
		admitted <- admitResult{decision: decision, err: err}
	}()

	// Wait until the admission is inside the judgment point (the injected
	// phase-two call point runs while the instance row lock is held).
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("admission did not reach the judgment point")
	}

	// Start the revoke on a separate named pool; it must block on the same
	// instance row lock until the admission releases it.
	revokePool := gateNamedPool(t, f.dsn, "gate-revoke-writer")
	revokeStore, err := controlstore.NewStore(f.ctx, revokePool)
	if err != nil {
		t.Fatalf("NewStore for revoke writer: %v", err)
	}
	generation, hash := f.token(t)
	revoked := make(chan error, 1)
	go func() {
		_, err := revokeStore.AppendReleaseDecision(f.ctx, controlstore.ReleaseDecisionRequest{
			InstanceID: f.instanceID, Capability: string(CapabilityQuery), ScopeHash: gateScopeFor(CapabilityQuery),
			Decision: "revoke", EvidenceGeneration: generation, EvidenceHash: hash,
			Actor: "deploy:executor", OperationID: gateOperation("interleaved-revoke"),
		})
		revoked <- err
	}()
	waitGateLockWait(t, f.ctx, f.pool, "gate-revoke-writer")
	select {
	case err := <-revoked:
		t.Fatalf("revoke completed while the admission held the lock: %v", err)
	default:
	}

	// Let the admission finish: it was admitted before the revoke, so it must
	// be allowed (in-flight; the gate never rewinds an admitted action).
	close(releaseAdmission)
	result := <-admitted
	if result.err != nil {
		t.Fatalf("interleaved admission: %v", result.err)
	}
	if !result.decision.Allowed {
		t.Fatalf("admission linearized before the revoke must stay allowed, got %+v", result.decision)
	}
	if err := <-revoked; err != nil {
		t.Fatalf("revoke after the admission released the lock: %v", err)
	}

	// The revoke committed before this admission: it refuses.
	decision := f.admit(t, gate, CapabilityQuery)
	if decision.Allowed || decision.RefusalClass != RefusalReleaseRevoked {
		t.Fatalf("revoke before the next admission = %+v, want %s", decision, RefusalReleaseRevoked)
	}
}

// waitGateLockWait waits until the named writer is observably blocked on a
// lock (pg_stat_activity wait_event_type='Lock') — a deterministic barrier,
// never a sleep-based ordering.
func waitGateLockWait(t *testing.T, ctx context.Context, pool *pgxpool.Pool, appName string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var n int
		if err := pool.QueryRow(ctx,
			"SELECT count(*) FROM pg_stat_activity WHERE application_name = $1 AND wait_event_type = 'Lock'",
			appName).Scan(&n); err == nil && n > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("writer %q never reported a lock wait", appName)
}

// ---------------------------------------------------------------------------
// T069: no gate over an unknown/incompatible control store
// ---------------------------------------------------------------------------

func TestGateUnknownControlStoreVersionRefusedByVersionGuard(t *testing.T) {
	ctx, _, pool := gateControlDatabase(t)
	if store, err := controlstore.NewStore(ctx, pool); err != nil {
		t.Fatalf("NewStore at the known version: %v", err)
	} else if store == nil {
		t.Fatal("NewStore returned a nil store at the known version")
	}
	if _, err := pool.Exec(ctx, "UPDATE goose_db_version SET version_id = 999 WHERE is_applied"); err != nil {
		t.Fatalf("forge unknown version: %v", err)
	}
	store, err := controlstore.NewStore(ctx, pool)
	if err == nil {
		t.Fatal("NewStore over an unknown control-store version must refuse")
	}
	var schemaErr *controlstore.SchemaError
	if !errors.As(err, &schemaErr) || schemaErr.RefusalClass != controlstore.RefusalControlStoreUnavailable {
		t.Fatalf("unknown version error = %v, want control_store_unavailable SchemaError", err)
	}
	if store != nil {
		t.Fatal("no store may be handed out over an unknown version")
	}
	// The gate cannot be constructed without the guarded store, so no
	// unguarded evaluation path exists (T012 inherits T069).
	if _, err := NewGate(store, GateOptions{TTL: time.Minute}); !errors.Is(err, ErrGateControlStoreUnavailable) {
		t.Fatalf("NewGate(nil) = %v, want ErrGateControlStoreUnavailable", err)
	}
}
