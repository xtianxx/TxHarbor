//go:build integration

// store_integration_test.go runs the control-store behavior against a real
// PostgreSQL 18.6 (testcontainers): the schema version guard positive and
// negative paths (T069), the instance row lock, the partial-unique open
// conflict, operation_id idempotency with zero-write conflicts, commit-order
// decision reads that ignore created_at, the fail-closed unorderable-decision
// path, audit writes and the credential-free data_target guard.
//
// Docker provider missing: the package fails under CI=true or
// TXHARBOR_REQUIRE_DOCKER=1 and reports NOT RUN locally (exit 0) — an unrun
// PG layer is never a pass.
package controlstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore/schema"
)

const controlPGImage = "postgres:18.6-trixie"

func TestMain(m *testing.M) {
	os.Exit(runControlStoreIntegration(m))
}

func runControlStoreIntegration(m *testing.M) int {
	ctx := context.Background()
	if !controlDockerHealthy(ctx) {
		if os.Getenv("CI") == "true" || os.Getenv("TXHARBOR_REQUIRE_DOCKER") == "1" {
			fmt.Fprintln(os.Stderr, "controlstore integration: docker provider unavailable; failing package (CI=true or TXHARBOR_REQUIRE_DOCKER=1): exit 1")
			return 1
		}
		fmt.Fprintln(os.Stderr, "controlstore integration: docker provider unavailable; skipping package (NOT RUN, exit 0)")
		return 0
	}
	return m.Run()
}

func controlDockerHealthy(ctx context.Context) (healthy bool) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "controlstore integration: docker provider check panicked: %v\n", r)
			healthy = false
		}
	}()
	provider, err := testcontainers.ProviderDocker.GetProvider()
	if err != nil {
		fmt.Fprintf(os.Stderr, "controlstore integration: docker provider: %v\n", err)
		return false
	}
	if err := provider.Health(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "controlstore integration: docker health: %v\n", err)
		return false
	}
	return true
}

// startControlPostgres boots one fresh PostgreSQL container per test and
// returns the DSN of its empty base database.
func startControlPostgres(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	ctr, err := postgres.Run(ctx, controlPGImage,
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
	return dsn
}

func openControlTestPool(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()
	pool, err := db.OpenPool(context.Background(), dsn, 10*time.Second)
	if err != nil {
		t.Fatalf("open control pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func migrateControlSchema(t *testing.T, dsn string) {
	t.Helper()
	if err := db.MigrateUp(context.Background(), db.MigrateOptions{
		DSN: dsn, LockTimeout: 10 * time.Second, ConnectTimeout: 5 * time.Second, FS: schema.FS,
	}, io.Discard); err != nil {
		t.Fatalf("migrate control schema: %v", err)
	}
}

func migratedControlStore(t *testing.T) (context.Context, *pgxpool.Pool, *Store, string) {
	t.Helper()
	dsn := startControlPostgres(t)
	migrateControlSchema(t, dsn)
	pool := openControlTestPool(t, dsn)
	store, err := NewStore(context.Background(), pool)
	if err != nil {
		t.Fatalf("NewStore over migrated control schema: %v", err)
	}
	return context.Background(), pool, store, dsn
}

func openTestInstance(t *testing.T, ctx context.Context, store *Store) string {
	t.Helper()
	result, err := store.OpenInstance(ctx, OpenInstanceRequest{Kind: "recovery", OpenedBy: "deploy:executor"})
	if err != nil {
		t.Fatalf("open instance: %v", err)
	}
	return result.InstanceID
}

func countRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, sql, args...).Scan(&n); err != nil {
		t.Fatalf("count (%s): %v", sql, err)
	}
	return n
}

func TestSchemaGuardPositiveKnown0001(t *testing.T) {
	ctx, pool, store, _ := migratedControlStore(t)
	if store == nil {
		t.Fatal("store must be available at the known 0001 version")
	}
	state, err := InspectSchema(ctx, pool)
	if err != nil {
		t.Fatalf("inspect schema: %v", err)
	}
	if !state.VersionTable || state.Current != 1 || state.Target != 1 || len(state.Unknown) != 0 || len(state.Pending) != 0 {
		t.Fatalf("unexpected schema state: %+v", state)
	}
	if len(state.RecoveryObjects) != len(controlTableNames) {
		t.Fatalf("expected all %d recovery objects, got %v", len(controlTableNames), state.RecoveryObjects)
	}
	if err := state.CheckCompatible(); err != nil {
		t.Fatalf("known 0001 must be compatible: %v", err)
	}
}

func TestSchemaGuardMissingVersionTableRefuses(t *testing.T) {
	dsn := startControlPostgres(t)
	pool := openControlTestPool(t, dsn)

	state, err := InspectSchema(context.Background(), pool)
	if err != nil {
		t.Fatalf("inspect schema: %v", err)
	}
	if state.VersionTable || len(state.RecoveryObjects) != 0 {
		t.Fatalf("fresh database must have no version table and no recovery objects: %+v", state)
	}
	if _, err := NewStore(context.Background(), pool); err == nil {
		t.Fatal("NewStore over a database without a version table must refuse")
	} else if !IsControlStoreUnavailable(err) {
		t.Fatalf("refusal must be control_store_unavailable, got %v", err)
	} else if got := err.Error(); got == "" || !containsAll(got, "version table is missing", "observed_version=0", "target_version=1") {
		t.Fatalf("refusal must annotate the observed state: %s", got)
	}
}

func TestSchemaGuardUnknownVersionRefusesAndAudits(t *testing.T) {
	ctx, pool, _, _ := migratedControlStore(t)
	if _, err := pool.Exec(ctx, "UPDATE goose_db_version SET version_id = 999 WHERE is_applied"); err != nil {
		t.Fatalf("forge unknown version: %v", err)
	}

	_, err := NewStore(ctx, pool)
	if err == nil {
		t.Fatal("NewStore over an unknown control-store version must refuse")
	}
	if !IsControlStoreUnavailable(err) {
		t.Fatalf("refusal must be control_store_unavailable, got %v", err)
	}
	var se *SchemaError
	if !errors.As(err, &se) || se.ObservedVersion != 999 || se.TargetVersion != 1 {
		t.Fatalf("refusal must carry observed/target versions, got %v", err)
	}
	var result, refusalClass, observed string
	if err := pool.QueryRow(ctx, `
SELECT result, refusal_class, detail->>'observed_version'
FROM recovery_audit WHERE action = 'store_connect' ORDER BY audit_id DESC LIMIT 1`).
		Scan(&result, &refusalClass, &observed); err != nil {
		t.Fatalf("read version refusal audit: %v", err)
	}
	if result != AuditRefused || refusalClass != RefusalControlStoreUnavailable || observed != "999" {
		t.Fatalf("unexpected refusal audit row: result=%s refusal_class=%s observed=%s", result, refusalClass, observed)
	}
}

func TestSchemaGuardObjectsWithoutVersionTableRefuse(t *testing.T) {
	dsn := startControlPostgres(t)
	pool := openControlTestPool(t, dsn)
	if _, err := pool.Exec(context.Background(), "CREATE TABLE recovery_instance (placeholder int)"); err != nil {
		t.Fatalf("create stray recovery object: %v", err)
	}
	state, err := InspectSchema(context.Background(), pool)
	if err != nil {
		t.Fatalf("inspect schema: %v", err)
	}
	if !state.VersionTable {
		if err := state.CheckMigratable(); err == nil || !IsControlStoreUnavailable(err) {
			t.Fatalf("objects without a version table must refuse migration, got %v", err)
		}
	}
	if _, err := NewStore(context.Background(), pool); err == nil {
		t.Fatal("NewStore must refuse a store without a version table")
	}
}

func TestOpenInstancePartialUniqueConflictRefuses(t *testing.T) {
	ctx, pool, store, _ := migratedControlStore(t)
	first := openTestInstance(t, ctx, store)

	_, err := store.OpenInstance(ctx, OpenInstanceRequest{Kind: "recovery", OpenedBy: "deploy:executor"})
	if err == nil || !errors.Is(err, ErrInstanceAlreadyOpen) {
		t.Fatalf("second open instance must be refused with ErrInstanceAlreadyOpen, got %v", err)
	}
	if err.Error() == "" || !containsAll(err.Error(), first) {
		t.Fatalf("refusal should name the already-open instance: %v", err)
	}
	if n := countRows(t, ctx, pool, "SELECT count(*) FROM recovery_instance WHERE state = 'open'"); n != 1 {
		t.Fatalf("exactly one open instance expected, got %d", n)
	}
	if n := countRows(t, ctx, pool,
		"SELECT count(*) FROM recovery_audit WHERE action = 'instance_open' AND result = 'refused'"); n != 1 {
		t.Fatalf("exactly one open-refusal audit row expected, got %d", n)
	}

	if _, err := pool.Exec(ctx,
		"UPDATE recovery_instance SET state = 'closed', closed_by = 'deploy:executor', closed_at = now() WHERE instance_id = $1",
		first); err != nil {
		t.Fatalf("close first instance: %v", err)
	}
	second := openTestInstance(t, ctx, store)
	if second == first {
		t.Fatal("instance ids must not be reused")
	}
	if n := countRows(t, ctx, pool, "SELECT count(*) FROM recovery_instance WHERE state = 'open'"); n != 1 {
		t.Fatalf("exactly one open instance expected after close/reopen, got %d", n)
	}
}

func TestLockInstanceSerializesWriters(t *testing.T) {
	ctx, pool, store, _ := migratedControlStore(t)
	instanceID := openTestInstance(t, ctx, store)

	txA, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin txA: %v", err)
	}
	defer func() { _ = txA.Rollback(context.Background()) }()
	tokenA, err := LockInstance(ctx, txA, instanceID)
	if err != nil {
		t.Fatalf("lock instance in txA: %v", err)
	}
	if tokenA.State != "open" || tokenA.EvidenceGeneration != 0 {
		t.Fatalf("unexpected token: %+v", tokenA)
	}

	blocked := make(chan error, 1)
	go func() {
		ctxB, cancel := context.WithTimeout(context.Background(), 750*time.Millisecond)
		defer cancel()
		txB, err := pool.Begin(ctxB)
		if err != nil {
			blocked <- err
			return
		}
		defer func() { _ = txB.Rollback(context.Background()) }()
		_, err = LockInstance(ctxB, txB, instanceID)
		blocked <- err
	}()
	select {
	case err := <-blocked:
		if err == nil {
			t.Fatal("a second writer must not acquire the instance row lock while the first holds it")
		}
		if errors.Is(err, ErrInstanceNotFound) {
			t.Fatalf("unexpected not-found during lock contention: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("second writer never returned")
	}

	if _, err := txA.Exec(ctx, "UPDATE recovery_instance SET evidence_generation = evidence_generation + 1 WHERE instance_id = $1", instanceID); err != nil {
		t.Fatalf("bump generation in txA: %v", err)
	}
	if err := txA.Commit(ctx); err != nil {
		t.Fatalf("commit txA: %v", err)
	}

	txB, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin txB retry: %v", err)
	}
	defer func() { _ = txB.Rollback(context.Background()) }()
	tokenB, err := LockInstance(ctx, txB, instanceID)
	if err != nil {
		t.Fatalf("lock instance after txA committed: %v", err)
	}
	if tokenB.EvidenceGeneration != 1 {
		t.Fatalf("second writer must see txA's committed generation, got %d", tokenB.EvidenceGeneration)
	}
}

func TestAppendReleaseDecisionIdempotentAndConflictZeroWrite(t *testing.T) {
	ctx, pool, store, _ := migratedControlStore(t)
	instanceID := openTestInstance(t, ctx, store)

	req := ReleaseDecisionRequest{
		InstanceID: instanceID, Capability: "query", ScopeHash: "scope-1", Decision: "release",
		EvidenceGeneration: 0, EvidenceHash: EmptyEvidenceHash,
		Actor: "deploy:executor", OperationID: "op-1",
	}
	first, err := store.AppendReleaseDecision(ctx, req)
	if err != nil {
		t.Fatalf("append release: %v", err)
	}
	if first.Recorded || first.DecisionID == "" {
		t.Fatalf("first append must insert a fresh decision: %+v", first)
	}

	replay, err := store.AppendReleaseDecision(ctx, req)
	if err != nil {
		t.Fatalf("replay same input: %v", err)
	}
	if !replay.Recorded || replay.DecisionID != first.DecisionID {
		t.Fatalf("same input must read the recorded row back: %+v vs %+v", replay, first)
	}
	if n := countRows(t, ctx, pool, "SELECT count(*) FROM recovery_release WHERE operation_id = 'op-1'"); n != 1 {
		t.Fatalf("same input must not duplicate decision rows, got %d", n)
	}
	if n := countRows(t, ctx, pool, "SELECT count(*) FROM recovery_audit WHERE operation_id = 'op-1'"); n != 1 {
		t.Fatalf("same input must not duplicate audit rows, got %d", n)
	}

	conflict := req
	conflict.Decision = "revoke"
	if _, err := store.AppendReleaseDecision(ctx, conflict); !errors.Is(err, ErrOperationConflict) {
		t.Fatalf("different input on the same operation_id must refuse with ErrOperationConflict, got %v", err)
	}
	if n := countRows(t, ctx, pool, "SELECT count(*) FROM recovery_release"); n != 1 {
		t.Fatalf("conflict must write zero decision rows, got %d", n)
	}
	if n := countRows(t, ctx, pool, "SELECT count(*) FROM recovery_audit"); n != 2 {
		// one instance_open ok + one release_decision ok
		t.Fatalf("conflict must write zero audit rows, got %d audit rows", n)
	}

	current, err := store.CurrentReleaseDecision(ctx, pool, DecisionKey{InstanceID: instanceID, Capability: "query", ScopeHash: "scope-1"})
	if err != nil {
		t.Fatalf("current release decision: %v", err)
	}
	if !current.Found || current.Decision != "release" || current.CommitSeq == 0 {
		t.Fatalf("unexpected current decision: %+v", current)
	}

	if _, err := store.AppendReleaseDecision(ctx, ReleaseDecisionRequest{
		InstanceID: instanceID, Capability: "query", ScopeHash: "scope-1", Decision: "revoke",
		EvidenceGeneration: 0, EvidenceHash: EmptyEvidenceHash,
		Actor: "deploy:executor", OperationID: "op-2",
	}); err != nil {
		t.Fatalf("append revoke: %v", err)
	}
	current, err = store.CurrentReleaseDecision(ctx, pool, DecisionKey{InstanceID: instanceID, Capability: "query", ScopeHash: "scope-1"})
	if err != nil {
		t.Fatalf("current after revoke: %v", err)
	}
	if current.Decision != "revoke" || current.OperationID != "op-2" {
		t.Fatalf("explicit revoke must be the current state, got %+v", current)
	}
}

func TestCurrentReleaseDecisionUsesCommitOrderNotCreatedAt(t *testing.T) {
	ctx, pool, store, _ := migratedControlStore(t)
	instanceID := openTestInstance(t, ctx, store)

	const (
		releaseEarlier = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
		revokeLater    = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
		releaseOrphan  = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
	)

	// Rows are inserted with explicit, reversed created_at values while the
	// paired audit rows (the commit-order anchor) keep their insertion order:
	// created_at must not decide which decision is current.
	if _, err := pool.Exec(ctx, `
INSERT INTO recovery_release
    (release_id, instance_id, capability, scope_hash, decision, evidence_generation, evidence_hash,
     approval_refs, operation_id, reason, created_at)
VALUES ($1, $2, 'query', 'scope-raw', 'release', 0, $3, '{}', 'seq-a', '', now() + interval '1 hour')`,
		releaseEarlier, instanceID, EmptyEvidenceHash); err != nil {
		t.Fatalf("insert release row: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO recovery_audit (instance_id, actor, action, target, result, evidence_generation, operation_id)
		 VALUES ($1, 'test:seq', 'release_decision', jsonb_build_object('release_id', $2::text), 'ok', 0, 'seq-a')`,
		instanceID, releaseEarlier); err != nil {
		t.Fatalf("insert release audit: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO recovery_release
    (release_id, instance_id, capability, scope_hash, decision, evidence_generation, evidence_hash,
     approval_refs, operation_id, reason, created_at)
VALUES ($1, $2, 'query', 'scope-raw', 'revoke', 0, $3, '{}', 'seq-b', '', now() - interval '1 hour')`,
		revokeLater, instanceID, EmptyEvidenceHash); err != nil {
		t.Fatalf("insert revoke row: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO recovery_audit (instance_id, actor, action, target, result, evidence_generation, operation_id)
		 VALUES ($1, 'test:seq', 'release_decision', jsonb_build_object('release_id', $2::text), 'ok', 0, 'seq-b')`,
		instanceID, revokeLater); err != nil {
		t.Fatalf("insert revoke audit: %v", err)
	}

	current, err := store.CurrentReleaseDecision(ctx, pool, DecisionKey{InstanceID: instanceID, Capability: "query", ScopeHash: "scope-raw"})
	if err != nil {
		t.Fatalf("current decision: %v", err)
	}
	if current.Decision != "revoke" || current.OperationID != "seq-b" {
		t.Fatalf("commit order must win over created_at, got %+v", current)
	}

	// An unpaired decision row (no ok audit row) cannot be ordered: the read
	// refuses instead of guessing which row is current.
	if _, err := pool.Exec(ctx, `
INSERT INTO recovery_release
    (release_id, instance_id, capability, scope_hash, decision, evidence_generation, evidence_hash,
     approval_refs, operation_id, reason)
VALUES ($1, $2, 'query', 'scope-raw', 'release', 0, $3, '{}', 'seq-orphan', '')`,
		releaseOrphan, instanceID, EmptyEvidenceHash); err != nil {
		t.Fatalf("insert orphan release row: %v", err)
	}
	if _, err := store.CurrentReleaseDecision(ctx, pool, DecisionKey{InstanceID: instanceID, Capability: "query", ScopeHash: "scope-raw"}); !errors.Is(err, ErrDecisionUnordered) {
		t.Fatalf("unorderable decision history must refuse, got %v", err)
	}
}

func TestAppendApprovalDecisionScopedToPrincipal(t *testing.T) {
	ctx, pool, store, _ := migratedControlStore(t)
	instanceID := openTestInstance(t, ctx, store)

	base := ApprovalDecisionRequest{
		InstanceID: instanceID, Capability: "query", ScopeHash: "scope-1",
		Decision: "approve", ApprovalClassSnapshot: "single_non_executor",
		EvidenceGeneration: 0, EvidenceHash: EmptyEvidenceHash,
	}
	alice := base
	alice.Principal, alice.PersonID, alice.OperationID = "deploy:alice", "person-a", "op-a"
	bob := base
	bob.Principal, bob.PersonID, bob.OperationID = "deploy:bob", "person-b", "op-b"
	if _, err := store.AppendApprovalDecision(ctx, alice); err != nil {
		t.Fatalf("append alice approval: %v", err)
	}
	if _, err := store.AppendApprovalDecision(ctx, bob); err != nil {
		t.Fatalf("append bob approval: %v", err)
	}
	replayAlice, err := store.AppendApprovalDecision(ctx, alice)
	if err != nil || !replayAlice.Recorded {
		t.Fatalf("alice replay must read back: %+v err=%v", replayAlice, err)
	}
	conflict := alice
	conflict.PersonID = "person-other"
	if _, err := store.AppendApprovalDecision(ctx, conflict); !errors.Is(err, ErrOperationConflict) {
		t.Fatalf("same operation_id with different person must refuse, got %v", err)
	}

	revokeAlice := alice
	revokeAlice.Decision, revokeAlice.OperationID = "revoke", "op-a-revoke"
	if _, err := store.AppendApprovalDecision(ctx, revokeAlice); err != nil {
		t.Fatalf("append alice revoke: %v", err)
	}

	key := DecisionKey{InstanceID: instanceID, Capability: "query", ScopeHash: "scope-1"}
	aliceState, err := store.CurrentApprovalDecision(ctx, pool, key, "deploy:alice")
	if err != nil {
		t.Fatalf("current alice approval: %v", err)
	}
	if aliceState.Decision != "revoke" {
		t.Fatalf("alice's revoke must cover her own approve, got %+v", aliceState)
	}
	bobState, err := store.CurrentApprovalDecision(ctx, pool, key, "deploy:bob")
	if err != nil {
		t.Fatalf("current bob approval: %v", err)
	}
	if bobState.Decision != "approve" {
		t.Fatalf("alice's revoke must not affect bob, got %+v", bobState)
	}
}

func TestWriteAuditValidatesClosedSets(t *testing.T) {
	ctx, pool, _, _ := migratedControlStore(t)

	if err := WriteAudit(ctx, pool, AuditRecord{Actor: "deploy:executor", Action: "probe", Result: "success"}); err == nil {
		t.Fatal("an out-of-set audit result must be refused")
	}
	if err := WriteAudit(ctx, pool, AuditRecord{Actor: "deploy:executor", Action: "probe", Result: AuditRefused, RefusalClass: "made_up"}); err == nil {
		t.Fatal("an out-of-set refusal class must be refused")
	}
	if err := WriteAudit(ctx, pool, AuditRecord{Actor: "", Action: "probe", Result: AuditOK}); err == nil {
		t.Fatal("a blank actor must be refused")
	}
	refusal := int64(7)
	if err := WriteAudit(ctx, pool, AuditRecord{
		Actor: "deploy:executor", Action: "probe", Result: AuditRefused,
		RefusalClass: RefusalControlStoreUnavailable, EvidenceGeneration: &refusal,
		Detail: []byte(`{"observed_version":3}`),
	}); err != nil {
		t.Fatalf("valid refusal audit: %v", err)
	}
	var result, refusalClass string
	var generation int64
	if err := pool.QueryRow(ctx,
		"SELECT result, refusal_class, evidence_generation FROM recovery_audit WHERE action = 'probe'").Scan(&result, &refusalClass, &generation); err != nil {
		t.Fatalf("read audit row: %v", err)
	}
	if result != AuditRefused || refusalClass != RefusalControlStoreUnavailable || generation != 7 {
		t.Fatalf("unexpected audit row: result=%s refusal_class=%s generation=%d", result, refusalClass, generation)
	}
}

func TestOpenInstanceRejectsCredentialDataTarget(t *testing.T) {
	ctx, pool, store, _ := migratedControlStore(t)

	_, err := store.OpenInstance(ctx, OpenInstanceRequest{
		Kind: "recovery", OpenedBy: "deploy:executor",
		DataTarget: []byte(`{"dsn":"postgres://u:supersecret@db.example/txharbor"}`),
	})
	if err == nil {
		t.Fatal("a data_target embedding a DSN must be refused")
	}
	if n := countRows(t, ctx, pool, "SELECT count(*) FROM recovery_instance"); n != 0 {
		t.Fatalf("refused instance must write zero rows, got %d", n)
	}

	fp := DSNTarget{Host: "db.example", Port: 5432, Database: "txharbor", Role: "txharbor"}
	payload := mustJSON(fp.DataTargetFingerprint())
	result, err := store.OpenInstance(ctx, OpenInstanceRequest{
		Kind: "recovery", OpenedBy: "deploy:executor", DataTarget: payload,
	})
	if err != nil {
		t.Fatalf("fingerprint data_target: %v", err)
	}
	var stored []byte
	if err := pool.QueryRow(ctx, "SELECT data_target FROM recovery_instance WHERE instance_id = $1", result.InstanceID).Scan(&stored); err != nil {
		t.Fatalf("read stored data_target: %v", err)
	}
	text := string(stored)
	for _, secret := range []string{"supersecret", "postgres://", "db.example", "password"} {
		if containsAll(text, secret) {
			t.Fatalf("stored data_target leaks %q: %s", secret, text)
		}
	}
}

func TestReleaseDecisionsHaveNoWritableBoolean(t *testing.T) {
	ctx, pool, _, _ := migratedControlStore(t)
	for _, table := range []string{"recovery_release", "recovery_approval"} {
		n := countRows(t, ctx, pool,
			"SELECT count(*) FROM information_schema.columns WHERE table_name = $1 AND data_type = 'boolean'", table)
		if n != 0 {
			t.Fatalf("%s must expose no writable boolean column (INV-2), found %d", table, n)
		}
		n = countRows(t, ctx, pool,
			"SELECT count(*) FROM information_schema.columns WHERE table_name = $1 AND column_name IN ('released', 'is_released')", table)
		if n != 0 {
			t.Fatalf("%s must expose no released flag column", table)
		}
	}
}

func containsAll(haystack string, needles ...string) bool {
	for _, needle := range needles {
		if !strings.Contains(haystack, needle) {
			return false
		}
	}
	return true
}
