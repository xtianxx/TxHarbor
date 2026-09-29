//go:build integration

// generation_integration_test.go runs the T013 evidence-generation protocol
// against a real PostgreSQL 18.6 (testcontainers): an accepted write advances
// evidence_generation and refreshes evidence_hash in the same transaction; a
// stale token is discarded with only a recovery_audit(result='discarded') row
// (no result row, no gap change, no generation advance); a fresh token after
// the discard is accepted; and two writers that captured the same token are
// serialized by the instance row lock, so the later committer is discarded and
// can never overwrite the newer evidence. The concurrency case uses a
// deterministic barrier — writer A holds the instance lock and the test waits
// for writer B's observed lock wait (pg_stat_activity wait_event_type='Lock')
// before releasing A — never a sleep-based ordering.
//
// Docker provider missing: the package fails under CI=true or
// TXHARBOR_REQUIRE_DOCKER=1 and reports NOT RUN locally (exit 0) — an unrun PG
// layer is never a pass.
package recovery

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"regexp"
	"strings"
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

const generationPGImage = "postgres:18.6-trixie"

func TestMain(m *testing.M) {
	os.Exit(runGenerationIntegration(m))
}

func runGenerationIntegration(m *testing.M) int {
	ctx := context.Background()
	if !generationDockerHealthy(ctx) {
		if os.Getenv("CI") == "true" || os.Getenv("TXHARBOR_REQUIRE_DOCKER") == "1" {
			fmt.Fprintln(os.Stderr, "recovery generation integration: docker provider unavailable; failing package (CI=true or TXHARBOR_REQUIRE_DOCKER=1): exit 1")
			return 1
		}
		fmt.Fprintln(os.Stderr, "recovery generation integration: docker provider unavailable; skipping package (NOT RUN, exit 0)")
		return 0
	}
	return m.Run()
}

func generationDockerHealthy(ctx context.Context) (healthy bool) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "recovery generation integration: docker provider check panicked: %v\n", r)
			healthy = false
		}
	}()
	provider, err := testcontainers.ProviderDocker.GetProvider()
	if err != nil {
		fmt.Fprintf(os.Stderr, "recovery generation integration: docker provider: %v\n", err)
		return false
	}
	if err := provider.Health(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "recovery generation integration: docker health: %v\n", err)
		return false
	}
	return true
}

// generationControlStore boots one fresh PostgreSQL container, migrates the
// control-store schema and returns the context, a pool, the guarded store and
// the DSN (for additional named pools used by the concurrency barrier).
func generationControlStore(t *testing.T) (context.Context, string, *pgxpool.Pool, *controlstore.Store) {
	t.Helper()
	ctx := context.Background()
	ctr, err := postgres.Run(ctx, generationPGImage,
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
	store, err := controlstore.NewStore(ctx, pool)
	if err != nil {
		t.Fatalf("NewStore over migrated control schema: %v", err)
	}
	return ctx, dsn, pool, store
}

// namedGenerationPool opens a second pool whose connections carry appName, so
// the barrier can observe this writer's lock wait in pg_stat_activity.
func namedGenerationPool(t *testing.T, dsn, appName string) *pgxpool.Pool {
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

func openGenerationInstance(t *testing.T, ctx context.Context, store *controlstore.Store) string {
	t.Helper()
	result, err := store.OpenInstance(ctx, controlstore.OpenInstanceRequest{
		Kind: "recovery", OpenedBy: "deploy:executor",
	})
	if err != nil {
		t.Fatalf("open instance: %v", err)
	}
	return result.InstanceID
}

// insertVerificationEvidenceTx writes one recovery_evidence row as the result
// of an accepted transition. Real write paths (verification/gaps/checklist)
// persist their own result rows inside Apply; this is the test's minimal
// stand-in, recording the accepted generation the protocol handed over.
func insertVerificationEvidenceTx(ctx context.Context, tx pgx.Tx, accepted EvidenceToken, ref string) error {
	_, err := tx.Exec(ctx, `
INSERT INTO recovery_evidence
    (evidence_id, instance_id, generation, kind, scope, artifact_hash, artifact_ref, observed_at, collected_by)
VALUES (gen_random_uuid(), $1, $2, 'verification_batch', '{}'::jsonb, $3, $4, now(), $5)`,
		accepted.InstanceID, accepted.Generation, accepted.Hash, ref, "deploy:verifier")
	return err
}

func insertOpenGapTx(ctx context.Context, tx pgx.Tx, accepted EvidenceToken, objectKey string) error {
	_, err := tx.Exec(ctx, `
INSERT INTO recovery_gap
    (gap_id, instance_id, object_key, scope, timeline, existing_evidence,
     required_evidence, affected_capabilities, state)
VALUES (gen_random_uuid(), $1, $2, '{}'::jsonb, '{}'::jsonb, '{}'::jsonb,
        '{}'::jsonb, ARRAY['query'], 'open')`, accepted.InstanceID, objectKey)
	return err
}

func countGenerationRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, sql, args...).Scan(&n); err != nil {
		t.Fatalf("count (%s): %v", sql, err)
	}
	return n
}

func instanceEvidenceState(t *testing.T, ctx context.Context, pool *pgxpool.Pool, instanceID string) (int64, string) {
	t.Helper()
	var generation int64
	var hash string
	if err := pool.QueryRow(ctx,
		"SELECT evidence_generation, evidence_hash FROM recovery_instance WHERE instance_id = $1",
		instanceID).Scan(&generation, &hash); err != nil {
		t.Fatalf("read instance evidence state: %v", err)
	}
	return generation, hash
}

func TestGenerationAcceptedWriteAdvancesTokenAndHash(t *testing.T) {
	ctx, _, pool, store := generationControlStore(t)
	instanceID := openGenerationInstance(t, ctx, store)

	captured, err := CaptureEvidenceToken(ctx, pool, instanceID)
	if err != nil {
		t.Fatalf("capture token: %v", err)
	}
	if captured.InstanceID != instanceID || captured.State != "open" ||
		captured.Generation != 0 || captured.Hash != controlstore.EmptyEvidenceHash {
		t.Fatalf("unexpected fresh token: %+v", captured)
	}

	outcome, err := CommitEvidenceWrite(ctx, store, EvidenceWriteRequest{
		InstanceID:  instanceID,
		Token:       captured,
		Kind:        MutationVerificationBatch,
		Actor:       "deploy:verifier",
		Reason:      "first batch",
		OperationID: "generation-accept-1",
		Apply: func(ctx context.Context, tx pgx.Tx, accepted EvidenceToken) error {
			return insertVerificationEvidenceTx(ctx, tx, accepted, "batch-1")
		},
	})
	if err != nil {
		t.Fatalf("accepted write: %v", err)
	}
	if outcome.Discarded || outcome.Mismatch != nil {
		t.Fatalf("matching token must be accepted, got %+v", outcome)
	}
	wantHash := EvidenceChainHash(controlstore.EmptyEvidenceHash, 1,
		MutationVerificationBatch, "generation-accept-1", nil)
	if outcome.Token.Generation != 1 || outcome.Token.Hash != wantHash || outcome.Token.State != "open" {
		t.Fatalf("accepted token = %+v, want generation=1 hash=%s", outcome.Token, wantHash)
	}

	// The instance advanced in the same transaction, and the result row
	// records the NEW generation (what makes "generation = current" checks
	// meaningful for the gate).
	generation, hash := instanceEvidenceState(t, ctx, pool, instanceID)
	if generation != 1 || hash != wantHash {
		t.Fatalf("instance evidence state = (%d, %s), want (1, %s)", generation, hash, wantHash)
	}
	var evidenceGeneration int64
	if err := pool.QueryRow(ctx, `
SELECT generation FROM recovery_evidence
WHERE instance_id = $1 AND artifact_ref = 'batch-1'`, instanceID).Scan(&evidenceGeneration); err != nil {
		t.Fatalf("read result row: %v", err)
	}
	if evidenceGeneration != 1 {
		t.Fatalf("result row generation = %d, want 1 (the accepted generation)", evidenceGeneration)
	}

	// Exactly one protocol audit row: accepted, carrying the captured
	// generation and the accepted components in the target.
	var result string
	var auditGeneration int64
	var acceptedGeneration, kind, operationID string
	if err := pool.QueryRow(ctx, `
SELECT result, evidence_generation, target->>'accepted_generation',
       target->>'kind', operation_id
FROM recovery_audit
WHERE instance_id = $1 AND action = 'evidence_write'`, instanceID).
		Scan(&result, &auditGeneration, &acceptedGeneration, &kind, &operationID); err != nil {
		t.Fatalf("read accepted audit row: %v", err)
	}
	if result != controlstore.AuditOK || auditGeneration != 0 || acceptedGeneration != "1" ||
		kind != string(MutationVerificationBatch) || operationID != "generation-accept-1" {
		t.Fatalf("unexpected accepted audit row: result=%s captured_generation=%d accepted=%s kind=%s op=%s",
			result, auditGeneration, acceptedGeneration, kind, operationID)
	}

	// A fresh capture sees the advanced token, and that token commits.
	fresh, err := CaptureEvidenceToken(ctx, pool, instanceID)
	if err != nil {
		t.Fatalf("capture fresh token: %v", err)
	}
	if !fresh.Matches(outcome.Token) {
		t.Fatalf("fresh capture %+v must match the accepted token %+v", fresh, outcome.Token)
	}
	second, err := CommitEvidenceWrite(ctx, store, EvidenceWriteRequest{
		InstanceID:  instanceID,
		Token:       fresh,
		Kind:        MutationVerificationBatch,
		Actor:       "deploy:verifier",
		OperationID: "generation-accept-2",
		Apply: func(ctx context.Context, tx pgx.Tx, accepted EvidenceToken) error {
			return insertVerificationEvidenceTx(ctx, tx, accepted, "batch-2")
		},
	})
	if err != nil || second.Discarded {
		t.Fatalf("fresh token must commit: outcome=%+v err=%v", second, err)
	}
	if generation, _ := instanceEvidenceState(t, ctx, pool, instanceID); generation != 2 {
		t.Fatalf("instance generation after second write = %d, want 2", generation)
	}
}

func TestGenerationStaleTokenDiscardedAuditedAndFreshTokenAccepted(t *testing.T) {
	ctx, _, pool, store := generationControlStore(t)
	instanceID := openGenerationInstance(t, ctx, store)

	token0, err := CaptureEvidenceToken(ctx, pool, instanceID)
	if err != nil {
		t.Fatalf("capture token: %v", err)
	}
	if _, err := CommitEvidenceWrite(ctx, store, EvidenceWriteRequest{
		InstanceID:  instanceID,
		Token:       token0,
		Kind:        MutationVerificationBatch,
		Actor:       "deploy:verifier",
		OperationID: "stale-1",
		Apply: func(ctx context.Context, tx pgx.Tx, accepted EvidenceToken) error {
			return insertVerificationEvidenceTx(ctx, tx, accepted, "batch-1")
		},
	}); err != nil {
		t.Fatalf("first accepted write: %v", err)
	}
	afterFirst, hashAfterFirst := instanceEvidenceState(t, ctx, pool, instanceID)

	// Same captured token, committed after the first write: discarded. No
	// result row, no generation advance, no overwrite.
	stale, err := CommitEvidenceWrite(ctx, store, EvidenceWriteRequest{
		InstanceID:  instanceID,
		Token:       token0,
		Kind:        MutationVerificationBatch,
		Actor:       "deploy:verifier",
		Reason:      "late batch",
		OperationID: "stale-2",
		Apply: func(ctx context.Context, tx pgx.Tx, accepted EvidenceToken) error {
			return insertVerificationEvidenceTx(ctx, tx, accepted, "batch-2-stale")
		},
	})
	if err != nil {
		t.Fatalf("stale write must be discarded, not fail: %v", err)
	}
	if !stale.Discarded || stale.Mismatch == nil {
		t.Fatalf("stale token must be discarded with a mismatch, got %+v", stale)
	}
	if stale.Mismatch.Captured.Generation != 0 || stale.Mismatch.Observed.Generation != 1 {
		t.Fatalf("mismatch must carry captured=0 observed=1, got %+v", stale.Mismatch)
	}
	if !strings.Contains(strings.Join(stale.Mismatch.Reasons(), " | "), "generation changed during the evidence read: 0 -> 1") {
		t.Fatalf("discard must name the advanced generation, got %v", stale.Mismatch.Reasons())
	}
	if stale.Token.Generation != 1 || stale.Token.Hash != hashAfterFirst {
		t.Fatalf("discarded outcome must carry the observed token, got %+v", stale.Token)
	}
	if !strings.Contains(stale.DiscardReason, "generation changed during the evidence read: 0 -> 1") {
		t.Fatalf("bounded discard reason must name the mismatch, got %q", stale.DiscardReason)
	}

	// The discard path wrote exactly one audit row and nothing else.
	if n := countGenerationRows(t, ctx, pool,
		"SELECT count(*) FROM recovery_evidence WHERE instance_id = $1", instanceID); n != 1 {
		t.Fatalf("discarded write must not insert a result row, got %d evidence rows", n)
	}
	if generation, hash := instanceEvidenceState(t, ctx, pool, instanceID); generation != afterFirst || hash != hashAfterFirst {
		t.Fatalf("discarded write must not advance the instance: (%d, %s)", generation, hash)
	}
	if n := countGenerationRows(t, ctx, pool,
		"SELECT count(*) FROM recovery_audit WHERE instance_id = $1 AND action = 'evidence_write' AND result = 'ok'", instanceID); n != 1 {
		t.Fatalf("accepted audit rows = %d, want 1", n)
	}
	if n := countGenerationRows(t, ctx, pool,
		"SELECT count(*) FROM recovery_audit WHERE instance_id = $1 AND action = 'evidence_write' AND result = 'discarded'", instanceID); n != 1 {
		t.Fatalf("discarded audit rows = %d, want 1", n)
	}
	var capturedGeneration, observedGeneration int
	var discardReason string
	if err := pool.QueryRow(ctx, `
SELECT (target->>'captured_generation')::int, (target->>'observed_generation')::int,
       detail->>'discard_reason'
FROM recovery_audit
WHERE instance_id = $1 AND action = 'evidence_write' AND result = 'discarded'`, instanceID).
		Scan(&capturedGeneration, &observedGeneration, &discardReason); err != nil {
		t.Fatalf("read discard audit: %v", err)
	}
	if capturedGeneration != 0 || observedGeneration != 1 {
		t.Fatalf("discard audit captured/observed = (%d, %d), want (0, 1)", capturedGeneration, observedGeneration)
	}
	if !strings.Contains(discardReason, "generation changed") {
		t.Fatalf("discard audit must carry the bounded reason, got %q", discardReason)
	}

	// Ordering is commit order, never created_at: record the wall-clock delta
	// of the accepted and discarded audits as diagnostic only.
	var deltaSeconds float64
	if err := pool.QueryRow(ctx, `
SELECT abs(extract(epoch FROM ok.created_at - discarded.created_at))
FROM (SELECT created_at FROM recovery_audit
      WHERE instance_id = $1 AND action = 'evidence_write' AND result = 'ok') ok,
     (SELECT created_at FROM recovery_audit
      WHERE instance_id = $1 AND action = 'evidence_write' AND result = 'discarded') discarded`,
		instanceID).Scan(&deltaSeconds); err != nil {
		t.Fatalf("read audit clock delta: %v", err)
	}
	t.Logf("accepted/discarded audit wall-clock delta: %.3fs (ordering decided by the lock, not the clock)", deltaSeconds)

	// Re-capturing after the discard yields a new token that commits: the
	// retry converges instead of being stuck.
	token1, err := CaptureEvidenceToken(ctx, pool, instanceID)
	if err != nil {
		t.Fatalf("re-capture token: %v", err)
	}
	accepted, err := CommitEvidenceWrite(ctx, store, EvidenceWriteRequest{
		InstanceID:  instanceID,
		Token:       token1,
		Kind:        MutationVerificationBatch,
		Actor:       "deploy:verifier",
		OperationID: "stale-3",
		Apply: func(ctx context.Context, tx pgx.Tx, accepted EvidenceToken) error {
			return insertVerificationEvidenceTx(ctx, tx, accepted, "batch-3")
		},
	})
	if err != nil || accepted.Discarded {
		t.Fatalf("re-captured token must commit: outcome=%+v err=%v", accepted, err)
	}
	if generation, _ := instanceEvidenceState(t, ctx, pool, instanceID); generation != 2 {
		t.Fatalf("generation after retry = %d, want 2", generation)
	}
	if n := countGenerationRows(t, ctx, pool,
		"SELECT count(*) FROM recovery_evidence WHERE instance_id = $1", instanceID); n != 2 {
		t.Fatalf("evidence rows after retry = %d, want 2", n)
	}
}

func TestGenerationDiscardLeavesGapUntouched(t *testing.T) {
	ctx, _, pool, store := generationControlStore(t)
	instanceID := openGenerationInstance(t, ctx, store)

	token0, err := CaptureEvidenceToken(ctx, pool, instanceID)
	if err != nil {
		t.Fatalf("capture token: %v", err)
	}
	if _, err := CommitEvidenceWrite(ctx, store, EvidenceWriteRequest{
		InstanceID:  instanceID,
		Token:       token0,
		Kind:        MutationGapOpened,
		Actor:       "deploy:verifier",
		OperationID: "gap-open-1",
		Apply: func(ctx context.Context, tx pgx.Tx, accepted EvidenceToken) error {
			return insertOpenGapTx(ctx, tx, accepted, "gap-object-1")
		},
	}); err != nil {
		t.Fatalf("open gap through the protocol: %v", err)
	}
	if n := countGenerationRows(t, ctx, pool,
		"SELECT count(*) FROM recovery_gap WHERE instance_id = $1 AND state = 'open'", instanceID); n != 1 {
		t.Fatalf("open gaps = %d, want 1", n)
	}

	// Capture, then let another accepted write advance the generation so the
	// gap-close attempt is stale.
	tokenGap, err := CaptureEvidenceToken(ctx, pool, instanceID)
	if err != nil {
		t.Fatalf("capture gap token: %v", err)
	}
	if _, err := CommitEvidenceWrite(ctx, store, EvidenceWriteRequest{
		InstanceID:  instanceID,
		Token:       tokenGap,
		Kind:        MutationIsolationVerified,
		Actor:       "deploy:verifier",
		OperationID: "isolation-1",
		Apply: func(ctx context.Context, tx pgx.Tx, accepted EvidenceToken) error {
			_, err := tx.Exec(ctx, `
INSERT INTO recovery_isolation_check
    (check_id, instance_id, item_key, state, evidence_ref, checked_by, checked_at)
VALUES (gen_random_uuid(), $1, 'old_writers_stopped', 'evidenced', 'evidence://isolation-1', $2, now())`,
				accepted.InstanceID, "deploy:verifier")
			return err
		},
	}); err != nil {
		t.Fatalf("isolation transition: %v", err)
	}

	stale, err := CommitEvidenceWrite(ctx, store, EvidenceWriteRequest{
		InstanceID:  instanceID,
		Token:       tokenGap,
		Kind:        MutationGapClosed,
		Actor:       "deploy:verifier",
		OperationID: "gap-close-stale",
		Apply: func(ctx context.Context, tx pgx.Tx, accepted EvidenceToken) error {
			_, err := tx.Exec(ctx, `
UPDATE recovery_gap SET state = 'closed', closed_by = $2, closed_at = now(),
       closure_evidence = '{"evidence_ref":"late"}'::jsonb
WHERE instance_id = $1 AND object_key = 'gap-object-1'`, accepted.InstanceID, "deploy:verifier")
			return err
		},
	})
	if err != nil {
		t.Fatalf("stale gap close must be discarded, not fail: %v", err)
	}
	if !stale.Discarded {
		t.Fatalf("stale gap close must be discarded, got %+v", stale)
	}

	// The gap is untouched: still open, no closer, no closure evidence.
	var state string
	var closedBy *string
	var closure *string
	if err := pool.QueryRow(ctx, `
SELECT state, closed_by, closure_evidence::text FROM recovery_gap
WHERE instance_id = $1 AND object_key = 'gap-object-1'`, instanceID).
		Scan(&state, &closedBy, &closure); err != nil {
		t.Fatalf("read gap: %v", err)
	}
	if state != "open" || closedBy != nil || closure != nil {
		t.Fatalf("discarded close must leave the gap open and untouched, got state=%s closed_by=%v closure=%v",
			state, closedBy, closure)
	}
	// The isolation transition that invalidated the token is intact.
	if n := countGenerationRows(t, ctx, pool,
		"SELECT count(*) FROM recovery_isolation_check WHERE instance_id = $1 AND state = 'evidenced'", instanceID); n != 1 {
		t.Fatalf("isolation rows = %d, want 1", n)
	}
}

func TestGenerationConcurrentStaleWriteDoesNotOverwriteNewEvidence(t *testing.T) {
	ctx, dsn, pool, store := generationControlStore(t)
	instanceID := openGenerationInstance(t, ctx, store)

	token0, err := CaptureEvidenceToken(ctx, pool, instanceID)
	if err != nil {
		t.Fatalf("capture token: %v", err)
	}

	type writeResult struct {
		outcome EvidenceWriteOutcome
		err     error
	}

	// Writer A takes the instance row lock inside Apply and holds it until the
	// test releases it: the barrier is structural, not a sleep.
	releaseA := make(chan struct{})
	aApplying := make(chan struct{})
	aDone := make(chan writeResult, 1)
	go func() {
		outcome, err := CommitEvidenceWrite(ctx, store, EvidenceWriteRequest{
			InstanceID:  instanceID,
			Token:       token0,
			Kind:        MutationVerificationBatch,
			Actor:       "deploy:verifier-a",
			OperationID: "concurrent-a",
			Apply: func(ctx context.Context, tx pgx.Tx, accepted EvidenceToken) error {
				if err := insertVerificationEvidenceTx(ctx, tx, accepted, "batch-a"); err != nil {
					return err
				}
				close(aApplying)
				<-releaseA
				return nil
			},
		})
		aDone <- writeResult{outcome: outcome, err: err}
	}()
	select {
	case <-aApplying:
	case <-time.After(30 * time.Second):
		t.Fatal("writer A never reached Apply (the instance row lock was not held)")
	}

	// Writer B captured the same token and commits on its own named pool.
	poolB := namedGenerationPool(t, dsn, "generation_stale_writer")
	storeB, err := controlstore.NewStore(ctx, poolB)
	if err != nil {
		t.Fatalf("NewStore for writer B: %v", err)
	}
	bDone := make(chan writeResult, 1)
	go func() {
		outcome, err := CommitEvidenceWrite(ctx, storeB, EvidenceWriteRequest{
			InstanceID:  instanceID,
			Token:       token0,
			Kind:        MutationVerificationBatch,
			Actor:       "deploy:verifier-b",
			OperationID: "concurrent-b",
			Apply: func(ctx context.Context, tx pgx.Tx, accepted EvidenceToken) error {
				return insertVerificationEvidenceTx(ctx, tx, accepted, "batch-b-stale")
			},
		})
		bDone <- writeResult{outcome: outcome, err: err}
	}()

	// Deterministic barrier: wait until B is observed waiting on the lock A
	// holds (pg_stat_activity wait_event_type='Lock'), then release A. The
	// poll interval is only cadence — it decides no ordering.
	waitGenerationLockWait(t, ctx, pool, "generation_stale_writer")
	close(releaseA)

	var a, b writeResult
	select {
	case a = <-aDone:
	case <-time.After(30 * time.Second):
		t.Fatal("writer A did not finish")
	}
	select {
	case b = <-bDone:
	case <-time.After(30 * time.Second):
		t.Fatal("writer B did not finish")
	}
	if a.err != nil || a.outcome.Discarded {
		t.Fatalf("writer A must commit its write: %+v err=%v", a.outcome, a.err)
	}
	if b.err != nil {
		t.Fatalf("writer B must be discarded, not fail: %v", b.err)
	}
	if !b.outcome.Discarded || b.outcome.Token.Generation != 1 {
		t.Fatalf("writer B must be discarded against generation 1, got %+v", b.outcome)
	}

	// Only A's evidence exists; B could not overwrite it.
	if n := countGenerationRows(t, ctx, pool,
		"SELECT count(*) FROM recovery_evidence WHERE instance_id = $1", instanceID); n != 1 {
		t.Fatalf("evidence rows = %d, want only writer A's row", n)
	}
	var ref string
	var generation int64
	if err := pool.QueryRow(ctx,
		"SELECT artifact_ref, generation FROM recovery_evidence WHERE instance_id = $1", instanceID).
		Scan(&ref, &generation); err != nil {
		t.Fatalf("read surviving evidence: %v", err)
	}
	if ref != "batch-a" || generation != 1 {
		t.Fatalf("surviving evidence = (%s, %d), want writer A's accepted row", ref, generation)
	}
	if current, hash := instanceEvidenceState(t, ctx, pool, instanceID); current != 1 || hash != a.outcome.Token.Hash {
		t.Fatalf("instance must carry writer A's accepted token, got (%d, %s)", current, hash)
	}
	if n := countGenerationRows(t, ctx, pool,
		"SELECT count(*) FROM recovery_audit WHERE instance_id = $1 AND action = 'evidence_write' AND result = 'discarded'", instanceID); n != 1 {
		t.Fatalf("discarded audit rows = %d, want 1 (writer B)", n)
	}
	var actor string
	if err := pool.QueryRow(ctx, `
SELECT actor FROM recovery_audit
WHERE instance_id = $1 AND action = 'evidence_write' AND result = 'discarded'`, instanceID).Scan(&actor); err != nil {
		t.Fatalf("read writer B discard audit: %v", err)
	}
	if actor != "deploy:verifier-b" {
		t.Fatalf("discard audit actor = %s, want writer B", actor)
	}
}

// waitGenerationLockWait blocks until a backend with the given
// application_name is observed waiting on a lock. It bounds the wait so a
// broken concurrency setup fails as a test failure instead of hanging.
func waitGenerationLockWait(t *testing.T, ctx context.Context, pool *pgxpool.Pool, appName string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		var waiting int
		if err := pool.QueryRow(ctx, `
SELECT count(*) FROM pg_stat_activity
WHERE application_name = $1 AND wait_event_type = 'Lock'`, appName).Scan(&waiting); err != nil {
			t.Fatalf("read pg_stat_activity: %v", err)
		}
		if waiting > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("writer with application_name=%q never blocked on the instance row lock", appName)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// ===========================================================================
// T047 [US4]: generation and invalidation controlled interleavings (链 5)
//
// TDD-first. B12 lands these tests before B13, so the approval/release calls
// below reference the planned T048/T049 API (internal/recovery/approvals.go /
// release.go) that does not exist until B13 lands:
//
//	func Approve(ctx context.Context, store *controlstore.Store, req ApprovalRequest) (ApprovalOutcome, error)
//	func RevokeApproval(ctx context.Context, store *controlstore.Store, req ApprovalRequest) (ApprovalOutcome, error)
//	func Release(ctx context.Context, store *controlstore.Store, gate *Gate, req ReleaseRequest) (ReleaseOutcome, error)
//	func RevokeRelease(ctx context.Context, store *controlstore.Store, gate *Gate, req ReleaseRequest) (ReleaseOutcome, error)
//
// The generation protocol itself (T013) already exists; T047 covers the
// controlled interleavings of data-model §3.3/§5/§6 and approval-matrix §4:
//
//   - write-write reverse order: a late writer's commit is discarded with
//     exactly one result='discarded' audit row — no result row, no gap change,
//     no generation advance (the otherwise-committed newer evidence wins);
//   - revoke after approval / after release refuses the next evaluation
//     immediately (no waiting for the TTL, warm cache included);
//   - a rejected isolation item stops the evaluation after the check;
//   - an old instance's late verification is discarded against a real lock
//     barrier observed in pg_stat_activity, with only a discarded audit;
//   - the gate TTL is bounded: a warm cache never skips the current
//     authorization check, TTL expiry re-reads the control store, and an
//     unreachable control store refuses even with a warm/expired cache;
//   - the single-action admission protocol (R3): a cache hit never skips the
//     in-lock authority read (cache hit then revoke refuses); an evidence or
//     mapping change refuses the next evaluation without waiting for the TTL;
//     revoke-before-admission refuses (未准入); admission-before-revoke is
//     in-flight + unknown discipline (already-admitted work is not rewound,
//     no retry/compensation entry exists, the next admission refuses);
//     one admission never covers more than its one action.
//
// External side effects are never rewound (INV-3/5/7): the tests assert that
// committed work is not retracted and that no automatic re-pay/re-broadcast/
// re-deliver path exists on the gate.
// ===========================================================================

// t047RetryEntryName matches a method that would look like an automatic
// retry/replay/compensation entry. The unknown discipline keeps an external
// result unknown; nothing may automatically re-pay, re-broadcast or
// re-deliver it.
var t047RetryEntryName = regexp.MustCompile(`(?i)(retry|replay|repay|rebroadcast|redeliver|redrive|compensat)`)

// t047Scene bundles the ge-generation fixture scene (open instance,
// executor/verifier/approver participants), the checklist service and the
// single derived gate.
type t047Scene struct {
	f         *gateFixture
	checklist *Checklist
	gate      *Gate
}

func t047NewScene(t *testing.T, opts GateOptions) *t047Scene {
	t.Helper()
	f := gateBaseFixture(t)
	checklist, err := NewChecklist(f.store)
	if err != nil {
		t.Fatalf("NewChecklist: %v", err)
	}
	return &t047Scene{f: f, checklist: checklist, gate: gateNewGate(t, f.store, opts)}
}

func (s *t047Scene) seed(t *testing.T, capability Capability) {
	t.Helper()
	isoVerifyAll(t, s.f, s.checklist, capability)
}

func (s *t047Scene) approve(t *testing.T, principal string, capability Capability) ApprovalOutcome {
	t.Helper()
	out, err := Approve(s.f.ctx, s.f.store, ApprovalRequest{
		InstanceID: s.f.instanceID, Capability: capability, ScopeHash: gateScopeFor(capability),
		Principal: principal, Reason: "t047 approval", OperationID: gateOperation("t047-approve"),
	})
	if err != nil {
		t.Fatalf("Approve(%s, %s): %v", principal, capability, err)
	}
	return out
}

func (s *t047Scene) revokeApproval(t *testing.T, principal string, capability Capability) ApprovalOutcome {
	t.Helper()
	out, err := RevokeApproval(s.f.ctx, s.f.store, ApprovalRequest{
		InstanceID: s.f.instanceID, Capability: capability, ScopeHash: gateScopeFor(capability),
		Principal: principal, Reason: "t047 explicit revoke", OperationID: gateOperation("t047-approve-revoke"),
	})
	if err != nil {
		t.Fatalf("RevokeApproval(%s, %s): %v", principal, capability, err)
	}
	return out
}

func (s *t047Scene) release(t *testing.T, capability Capability) ReleaseOutcome {
	t.Helper()
	out, err := Release(s.f.ctx, s.f.store, s.gate, ReleaseRequest{
		InstanceID: s.f.instanceID, Capability: capability, ScopeHash: gateScopeFor(capability),
		Principal: "deploy:executor", Reason: "t047 release", OperationID: gateOperation("t047-release"),
	})
	if err != nil {
		t.Fatalf("Release(%s): %v", capability, err)
	}
	return out
}

func (s *t047Scene) revokeRelease(t *testing.T, capability Capability) ReleaseOutcome {
	t.Helper()
	out, err := RevokeRelease(s.f.ctx, s.f.store, s.gate, ReleaseRequest{
		InstanceID: s.f.instanceID, Capability: capability, ScopeHash: gateScopeFor(capability),
		Principal: "deploy:executor", Reason: "t047 explicit release revoke", OperationID: gateOperation("t047-release-revoke"),
	})
	if err != nil {
		t.Fatalf("RevokeRelease(%s): %v", capability, err)
	}
	return out
}

func (s *t047Scene) admit(t *testing.T, capability Capability) GateDecision {
	t.Helper()
	decision, err := s.gate.Admit(s.f.ctx, GateRequest{
		InstanceID: s.f.instanceID, Capability: capability, ScopeHash: gateScopeFor(capability),
		Actor: "deploy:executor", OperationID: gateOperation("t047-admit"), Action: "test:t047",
	})
	if err != nil {
		t.Fatalf("Admit(%s): %v", capability, err)
	}
	return decision
}

func (s *t047Scene) admitErr(t *testing.T, capability Capability) (GateDecision, error) {
	t.Helper()
	return s.gate.Admit(s.f.ctx, GateRequest{
		InstanceID: s.f.instanceID, Capability: capability, ScopeHash: gateScopeFor(capability),
		Actor: "deploy:executor", OperationID: gateOperation("t047-admit-err"),
	})
}

func (s *t047Scene) count(t *testing.T, sql string, args ...any) int {
	t.Helper()
	return countGenerationRows(t, s.f.ctx, s.f.pool, sql, args...)
}

func (s *t047Scene) gateAudits(t *testing.T, result string) int {
	t.Helper()
	return s.count(t, `SELECT count(*) FROM recovery_audit
	    WHERE instance_id = $1 AND action = $2 AND result = $3`,
		s.f.instanceID, GateAuditAction, result)
}

// t047AssertNoAutomaticRetryEntry pins the unknown discipline: the derived
// gate exposes no retry/replay/compensation entry that could automatically
// re-pay, re-broadcast or re-deliver an unknown external result.
func t047AssertNoAutomaticRetryEntry(t *testing.T) {
	t.Helper()
	typ := reflect.TypeOf(&Gate{})
	for i := 0; i < typ.NumMethod(); i++ {
		name := typ.Method(i).Name
		if t047RetryEntryName.MatchString(name) {
			t.Fatalf("Gate.%s looks like an automatic retry/replay entry; an unknown result must stay unknown (never re-pay/re-broadcast/re-deliver)", name)
		}
	}
}

// ---------------------------------------------------------------------------
// Write-write reverse order
// ---------------------------------------------------------------------------

func TestT047WriteWriteReverseOrderDiscardsLateWriterAudited(t *testing.T) {
	ctx, _, pool, store := generationControlStore(t)
	instanceID := openGenerationInstance(t, ctx, store)

	// A captures first, B (the late writer) captures the same token afterwards
	// but commits first.
	tokenA, err := CaptureEvidenceToken(ctx, pool, instanceID)
	if err != nil {
		t.Fatalf("capture token A: %v", err)
	}
	tokenLate, err := CaptureEvidenceToken(ctx, pool, instanceID)
	if err != nil {
		t.Fatalf("capture late token: %v", err)
	}
	if !tokenA.Matches(tokenLate) {
		t.Fatalf("both writers must start from the same token: %+v vs %+v", tokenA, tokenLate)
	}

	accepted, err := CommitEvidenceWrite(ctx, store, EvidenceWriteRequest{
		InstanceID: instanceID, Token: tokenLate, Kind: MutationVerificationBatch,
		Actor: "deploy:verifier-b-first", Reason: "b commits first", OperationID: "t047-b-first",
		Apply: func(ctx context.Context, tx pgx.Tx, accepted EvidenceToken) error {
			return insertVerificationEvidenceTx(ctx, tx, accepted, "t047-b-first")
		},
	})
	if err != nil || accepted.Discarded {
		t.Fatalf("writer B must commit first: outcome=%+v err=%v", accepted, err)
	}

	// A fresh accepted write opens a gap at the new token, so the late
	// writer's discard must leave the gap untouched too.
	tokenGap, err := CaptureEvidenceToken(ctx, pool, instanceID)
	if err != nil {
		t.Fatalf("capture gap token: %v", err)
	}
	gapOutcome, err := CommitEvidenceWrite(ctx, store, EvidenceWriteRequest{
		InstanceID: instanceID, Token: tokenGap, Kind: MutationGapOpened,
		Actor: "deploy:verifier-b-first", Reason: "gap at the new token", OperationID: "t047-gap-open",
		Apply: func(ctx context.Context, tx pgx.Tx, accepted EvidenceToken) error {
			return insertOpenGapTx(ctx, tx, accepted, "t047-gap-object")
		},
	})
	if err != nil || gapOutcome.Discarded {
		t.Fatalf("gap open must commit: outcome=%+v err=%v", gapOutcome, err)
	}

	// A commits last, against its stale captured token: discarded.
	late, err := CommitEvidenceWrite(ctx, store, EvidenceWriteRequest{
		InstanceID: instanceID, Token: tokenA, Kind: MutationVerificationBatch,
		Actor: "deploy:verifier-late", Reason: "late commit", OperationID: "t047-a-late",
		Apply: func(ctx context.Context, tx pgx.Tx, accepted EvidenceToken) error {
			return insertVerificationEvidenceTx(ctx, tx, accepted, "t047-a-late")
		},
	})
	if err != nil {
		t.Fatalf("late commit must be discarded, not fail: %v", err)
	}
	if !late.Discarded || late.Mismatch == nil {
		t.Fatalf("late commit = %+v, want a discard with a token mismatch", late)
	}

	// No result row, no gap change, no generation advance: exactly one
	// discarded audit row for the late writer.
	if got := countGenerationRows(t, ctx, pool,
		"SELECT count(*) FROM recovery_evidence WHERE instance_id = $1", instanceID); got != 1 {
		t.Fatalf("evidence rows = %d, want only writer B's accepted row", got)
	}
	var gapState string
	var closedBy *string
	if err := pool.QueryRow(ctx,
		"SELECT state, closed_by FROM recovery_gap WHERE instance_id = $1 AND object_key = 't047-gap-object'",
		instanceID).Scan(&gapState, &closedBy); err != nil {
		t.Fatalf("read gap: %v", err)
	}
	if gapState != "open" || closedBy != nil {
		t.Fatalf("late discard changed the gap: state=%s closed_by=%v", gapState, closedBy)
	}
	if generation, _ := instanceEvidenceState(t, ctx, pool, instanceID); generation != 2 {
		t.Fatalf("generation = %d, want 2 (two accepted writes, no advance from the discard)", generation)
	}
	if got := countGenerationRows(t, ctx, pool,
		"SELECT count(*) FROM recovery_audit WHERE instance_id = $1 AND action = 'evidence_write' AND result = 'ok'", instanceID); got != 2 {
		t.Fatalf("accepted evidence_write audits = %d, want 2", got)
	}
	if got := countGenerationRows(t, ctx, pool,
		"SELECT count(*) FROM recovery_audit WHERE instance_id = $1 AND action = 'evidence_write' AND result = 'discarded'", instanceID); got != 1 {
		t.Fatalf("discarded evidence_write audits = %d, want exactly 1 (the late writer)", got)
	}
	var actor, captured string
	if err := pool.QueryRow(ctx, `
SELECT actor, COALESCE(target->>'captured_generation', '')
FROM recovery_audit
WHERE instance_id = $1 AND action = 'evidence_write' AND result = 'discarded'`, instanceID).
		Scan(&actor, &captured); err != nil {
		t.Fatalf("read late discard audit: %v", err)
	}
	if actor != "deploy:verifier-late" || captured != "0" {
		t.Fatalf("discard audit = actor=%q captured_generation=%q, want the late writer against generation 0", actor, captured)
	}
}

// ---------------------------------------------------------------------------
// Approval/release revocation and isolation rejection
// ---------------------------------------------------------------------------

func TestT047ApprovalAndReleaseRevokeRefuseNextEvaluation(t *testing.T) {
	s := t047NewScene(t, GateOptions{})
	s.seed(t, CapabilityQuery)
	generationBefore, hashBefore := gateInstanceToken(t, s.f.ctx, s.f.pool, s.f.instanceID)

	s.approve(t, "auth:approver", CapabilityQuery)
	s.release(t, CapabilityQuery)
	if got := s.admit(t, CapabilityQuery); !got.Allowed {
		t.Fatalf("legal base state must be admitted, got %+v", got)
	}

	// Revoking the approval covers that principal's earlier approve: the next
	// evaluation refuses (no TTL, no cache involved).
	s.revokeApproval(t, "auth:approver", CapabilityQuery)
	if got := s.admit(t, CapabilityQuery); got.Allowed || got.RefusalClass != RefusalApprovalMissing {
		t.Fatalf("after approval revoke = %+v, want %s", got, RefusalApprovalMissing)
	}
	// The revoke is not an evidence change: the generation/hash stay put.
	if generation, hash := gateInstanceToken(t, s.f.ctx, s.f.pool, s.f.instanceID); generation != generationBefore || hash != hashBefore {
		t.Fatalf("approval revoke changed the evidence token (%d,%s) -> (%d,%s)", generationBefore, hashBefore, generation, hash)
	}

	// A re-approval does not revive the old release (the release references
	// the old approval); a re-release is required.
	s.approve(t, "auth:approver", CapabilityQuery)
	if got := s.admit(t, CapabilityQuery); got.Allowed {
		t.Fatal("a re-approval must not silently revive a release bound to the old approval")
	}
	s.release(t, CapabilityQuery)
	if got := s.admit(t, CapabilityQuery); !got.Allowed {
		t.Fatalf("re-approved and re-released capability must be admitted, got %+v", got)
	}

	// An explicit release revoke refuses the next evaluation.
	s.revokeRelease(t, CapabilityQuery)
	if got := s.admit(t, CapabilityQuery); got.Allowed || got.RefusalClass != RefusalReleaseRevoked {
		t.Fatalf("after release revoke = %+v, want %s", got, RefusalReleaseRevoked)
	}
}

func TestT047IsolationRejectedStopsReleaseEvaluation(t *testing.T) {
	s := t047NewScene(t, GateOptions{})
	s.seed(t, CapabilityQuery)
	s.approve(t, "auth:approver", CapabilityQuery)
	s.release(t, CapabilityQuery)
	if got := s.admit(t, CapabilityQuery); !got.Allowed {
		t.Fatalf("legal base state must be admitted, got %+v", got)
	}
	generationBefore, _ := gateInstanceToken(t, s.f.ctx, s.f.pool, s.f.instanceID)

	items, err := IsolationDependencySet(CapabilityQuery)
	if err != nil || len(items) == 0 {
		t.Fatalf("IsolationDependencySet(query) = %v, %v", items, err)
	}
	// The non-executor verifier rejects one verified item: the release must
	// stop being evaluable (the isolation check is the first gate condition
	// after the capability facts).
	if _, err := s.checklist.Verify(s.f.ctx, ChecklistVerifyRequest{
		InstanceID: s.f.instanceID, ItemKey: items[0], Actor: "auth:verifier",
		Reject: true, Reason: "t047 rejected evidence", OperationID: gateOperation("t047-iso-reject"),
	}); err != nil {
		t.Fatalf("checklist reject verdict: %v", err)
	}
	if generation, _ := gateInstanceToken(t, s.f.ctx, s.f.pool, s.f.instanceID); generation != generationBefore+1 {
		t.Fatalf("rejection generation = %d, want %d (verified<->rejected advances the generation)", generation, generationBefore+1)
	}
	item, found, err := s.checklist.Item(s.f.ctx, s.f.instanceID, items[0])
	if err != nil || !found || item.State != ChecklistStateRejected {
		t.Fatalf("rejected item = (%+v, found=%v, err=%v), want state rejected", item, found, err)
	}
	got := s.admit(t, CapabilityQuery)
	if got.Allowed || got.RefusalClass != RefusalIsolationUnproven {
		t.Fatalf("after the isolation rejection = %+v, want %s (the release stops being evaluable)", got, RefusalIsolationUnproven)
	}
}

// ---------------------------------------------------------------------------
// Late verification against a real lock barrier
// ---------------------------------------------------------------------------

func TestT047LateVerificationDiscardedAgainstRealLockBarrier(t *testing.T) {
	ctx, dsn, pool, store := generationControlStore(t)
	instanceID := openGenerationInstance(t, ctx, store)

	token0, err := CaptureEvidenceToken(ctx, pool, instanceID)
	if err != nil {
		t.Fatalf("capture token: %v", err)
	}

	type writeResult struct {
		outcome EvidenceWriteOutcome
		err     error
	}

	// The active writer holds the instance row lock inside Apply; the old
	// instance's late verification blocks on that lock.
	releaseActive := make(chan struct{})
	activeApplying := make(chan struct{})
	activeDone := make(chan writeResult, 1)
	go func() {
		outcome, err := CommitEvidenceWrite(ctx, store, EvidenceWriteRequest{
			InstanceID: instanceID, Token: token0, Kind: MutationVerificationBatch,
			Actor: "deploy:verifier-active", Reason: "active write", OperationID: "t047-late-barrier-active",
			Apply: func(ctx context.Context, tx pgx.Tx, accepted EvidenceToken) error {
				if err := insertVerificationEvidenceTx(ctx, tx, accepted, "t047-active"); err != nil {
					return err
				}
				close(activeApplying)
				<-releaseActive
				return nil
			},
		})
		activeDone <- writeResult{outcome: outcome, err: err}
	}()
	select {
	case <-activeApplying:
	case <-time.After(30 * time.Second):
		t.Fatal("the active writer never held the instance row lock")
	}

	latePool := namedGenerationPool(t, dsn, "t047-late-verifier")
	lateStore, err := controlstore.NewStore(ctx, latePool)
	if err != nil {
		t.Fatalf("NewStore for the late verifier: %v", err)
	}
	lateDone := make(chan writeResult, 1)
	go func() {
		outcome, err := CommitEvidenceWrite(ctx, lateStore, EvidenceWriteRequest{
			InstanceID: instanceID, Token: token0, Kind: MutationIsolationVerified,
			Actor: "deploy:verifier-late", Reason: "old instance late verification", OperationID: "t047-late-barrier-late",
			Apply: func(ctx context.Context, tx pgx.Tx, accepted EvidenceToken) error {
				_, err := tx.Exec(ctx, `
INSERT INTO recovery_isolation_check
    (check_id, instance_id, item_key, state, evidence_ref, checked_by, checked_at)
VALUES (gen_random_uuid(), $1, 'old_writers_stopped', 'evidenced', 'evidence://t047/late', $2, now())`,
					accepted.InstanceID, "deploy:verifier-late")
				return err
			},
		})
		lateDone <- writeResult{outcome: outcome, err: err}
	}()

	// Deterministic barrier: wait until the late writer is observed waiting on
	// the lock the active writer holds, then release.
	waitGenerationLockWait(t, ctx, pool, "t047-late-verifier")
	close(releaseActive)

	var active, late writeResult
	select {
	case active = <-activeDone:
	case <-time.After(30 * time.Second):
		t.Fatal("the active writer did not finish")
	}
	select {
	case late = <-lateDone:
	case <-time.After(30 * time.Second):
		t.Fatal("the late writer did not finish")
	}
	if active.err != nil || active.outcome.Discarded {
		t.Fatalf("the active write must commit: %+v err=%v", active.outcome, active.err)
	}
	if late.err != nil {
		t.Fatalf("the late verification must be discarded, not fail: %v", late.err)
	}
	if !late.outcome.Discarded || late.outcome.Token.Generation != 1 {
		t.Fatalf("late verification = %+v, want a discard against generation 1", late.outcome)
	}

	// The late result left no row: no isolation item, only the active
	// writer's evidence, and exactly one discarded audit for the late actor.
	if got := countGenerationRows(t, ctx, pool,
		"SELECT count(*) FROM recovery_isolation_check WHERE instance_id = $1", instanceID); got != 0 {
		t.Fatalf("isolation rows = %d, want 0 (the late verification must not write)", got)
	}
	if got := countGenerationRows(t, ctx, pool,
		"SELECT count(*) FROM recovery_evidence WHERE instance_id = $1", instanceID); got != 1 {
		t.Fatalf("evidence rows = %d, want only the active writer's row", got)
	}
	if got := countGenerationRows(t, ctx, pool, `
SELECT count(*) FROM recovery_audit
WHERE instance_id = $1 AND action = 'evidence_write' AND result = 'discarded' AND actor = 'deploy:verifier-late'`,
		instanceID); got != 1 {
		t.Fatalf("late-verifier discarded audits = %d, want exactly 1", got)
	}
	if generation, _ := instanceEvidenceState(t, ctx, pool, instanceID); generation != 1 {
		t.Fatalf("generation = %d, want 1 (the discard must not advance it)", generation)
	}
}

// ---------------------------------------------------------------------------
// TTL semantics and control-store reachability
// ---------------------------------------------------------------------------

func TestT047TTLIsBoundedAndRevocationRefuses(t *testing.T) {
	now := time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)
	s := t047NewScene(t, GateOptions{TTL: time.Minute, Now: func() time.Time { return now }})
	s.seed(t, CapabilityQuery)
	s.approve(t, "auth:approver", CapabilityQuery)
	s.release(t, CapabilityQuery)

	if got := s.admit(t, CapabilityQuery); !got.Allowed || got.CacheHit {
		t.Fatalf("first admission must be an allowed cache miss, got %+v", got)
	}
	if got := s.admit(t, CapabilityQuery); !got.Allowed || !got.CacheHit {
		t.Fatalf("second admission must reuse the bounded cache, got %+v", got)
	}

	// A revoke is discovered by the evaluator immediately: the warm cache must
	// not be reused as a stale allow, and nothing waits for the TTL.
	s.revokeApproval(t, "auth:approver", CapabilityQuery)
	if got := s.admit(t, CapabilityQuery); got.Allowed || got.RefusalClass != RefusalApprovalMissing {
		t.Fatalf("revoke with a warm cache = %+v, want %s", got, RefusalApprovalMissing)
	}

	// Re-approve/re-release, warm the cache again, then let the TTL lapse: the
	// entry is re-derived (allowed while the basis is still valid), never
	// treated as an eternal permission.
	s.approve(t, "auth:approver", CapabilityQuery)
	s.release(t, CapabilityQuery)
	now = now.Add(2 * time.Minute)
	if got := s.admit(t, CapabilityQuery); !got.Allowed {
		t.Fatalf("TTL-expired entry must be re-derived and allowed while the basis is valid, got %+v", got)
	}

	// Revoke, TTL lapse again, and make the control store unreachable: the
	// admission refuses (fail-closed), never a cache-only allow.
	s.revokeRelease(t, CapabilityQuery)
	now = now.Add(2 * time.Minute)
	s.f.pool.Close()
	decision, err := s.admitErr(t, CapabilityQuery)
	if err == nil || !errors.Is(err, ErrGateControlStoreUnavailable) {
		t.Fatalf("unreachable control store error = %v, want ErrGateControlStoreUnavailable", err)
	}
	if decision.Allowed || decision.RefusalClass != RefusalControlStoreUnavailable {
		t.Fatalf("unreachable control store = %+v, want %s", decision, RefusalControlStoreUnavailable)
	}
}

// ---------------------------------------------------------------------------
// R3: single-action admission, cache hits and the controlled interleavings
// ---------------------------------------------------------------------------

func TestT047SingleActionAdmissionCannotSkipAuthorityOrBeReused(t *testing.T) {
	s := t047NewScene(t, GateOptions{})
	s.seed(t, CapabilityQuery)
	s.approve(t, "auth:approver", CapabilityQuery)
	s.release(t, CapabilityQuery)

	// (1) cache hit then revoke: the hit must not skip the current
	// authorization check, and the next admission refuses.
	if got := s.admit(t, CapabilityQuery); !got.Allowed {
		t.Fatalf("first admission must be allowed, got %+v", got)
	}
	if got := s.admit(t, CapabilityQuery); !got.Allowed || !got.CacheHit {
		t.Fatalf("second admission must be an allowed cache hit, got %+v", got)
	}
	s.revokeRelease(t, CapabilityQuery)
	if got := s.admit(t, CapabilityQuery); got.Allowed || got.RefusalClass != RefusalReleaseRevoked {
		t.Fatalf("cache hit then revoke = %+v, want %s", got, RefusalReleaseRevoked)
	}

	// (2) evidence change: the next evaluation refuses immediately (the
	// evaluator is the discoverer; no waiting for the TTL) and the cached
	// old-generation allow is never reused.
	s.release(t, CapabilityQuery) // approvals are still current
	if got := s.admit(t, CapabilityQuery); !got.Allowed {
		t.Fatalf("re-release must be allowed, got %+v", got)
	}
	s.f.bumpGeneration(t)
	if got := s.admit(t, CapabilityQuery); got.Allowed || got.RefusalClass != RefusalReleaseInvalidatedGeneration {
		t.Fatalf("evidence change with a warm cache = %+v, want %s", got, RefusalReleaseInvalidatedGeneration)
	}

	// (2b) mapping change: same immediacy, different refusal class.
	s.approve(t, "auth:approver", CapabilityQuery)
	s.release(t, CapabilityQuery)
	if got := s.admit(t, CapabilityQuery); !got.Allowed {
		t.Fatalf("fresh-token release must be allowed, got %+v", got)
	}
	gateMapIdentity(t, s.f.ctx, s.f.store, "auth:approver", "person-approver-other")
	if got := s.admit(t, CapabilityQuery); got.Allowed || got.RefusalClass != RefusalApprovalIdentityUnverified {
		t.Fatalf("mapping change with a warm cache = %+v, want %s", got, RefusalApprovalIdentityUnverified)
	}

	// (6) one admission covers one action only: every call re-enters the lock
	// and writes its own audit row; an old decision value can never be
	// replayed because Admit takes no decision input.
	okBefore := s.gateAudits(t, controlstore.AuditOK)
	if got := s.admit(t, CapabilityQuery); got.Allowed {
		t.Fatalf("the mapping-changed basis must still refuse, got %+v", got)
	}
	if got := s.gateAudits(t, controlstore.AuditOK); got != okBefore {
		t.Fatalf("a refused admission wrote %d ok audit row(s)", got-okBefore)
	}
	t047AssertNoAutomaticRetryEntry(t)
}

func TestT047AdmissionBeforeRevokeIsInFlightAndNotRetracted(t *testing.T) {
	s := t047NewScene(t, GateOptions{})
	s.seed(t, CapabilityQuery)
	s.approve(t, "auth:approver", CapabilityQuery)
	s.release(t, CapabilityQuery)

	// Warm the cache so the interleaved admission below is a cache hit; a hit
	// still takes the in-lock judgment (the cache must never be the authority).
	if got := s.admit(t, CapabilityQuery); !got.Allowed {
		t.Fatalf("warm-up admission must be allowed, got %+v", got)
	}
	if got := s.admit(t, CapabilityQuery); !got.Allowed || !got.CacheHit {
		t.Fatalf("warm-up admission must be a cache hit, got %+v", got)
	}

	entered := make(chan struct{})
	releaseAdmission := make(chan struct{})
	s.gate.fundGates = func(context.Context, GateRequest) error {
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-releaseAdmission
		return nil
	}

	type admitResult struct {
		decision GateDecision
		err      error
	}
	inflightOp := gateOperation("t047-inflight")
	admitted := make(chan admitResult, 1)
	go func() {
		decision, err := s.gate.Admit(s.f.ctx, GateRequest{
			InstanceID: s.f.instanceID, Capability: CapabilityQuery, ScopeHash: gateScopeFor(CapabilityQuery),
			Actor: "deploy:executor", OperationID: inflightOp, Action: "test:t047-inflight",
		})
		admitted <- admitResult{decision: decision, err: err}
	}()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the admission never reached the judgment point")
	}

	// The revoke arrives while the admission holds the instance row lock and
	// blocks on it (real lock wait observed in pg_stat_activity).
	revokePool := gateNamedPool(t, s.f.dsn, "t047-revoke-writer")
	revokeStore, err := controlstore.NewStore(s.f.ctx, revokePool)
	if err != nil {
		t.Fatalf("NewStore for the revoke writer: %v", err)
	}
	revoked := make(chan error, 1)
	go func() {
		_, err := RevokeRelease(s.f.ctx, revokeStore, s.gate, ReleaseRequest{
			InstanceID: s.f.instanceID, Capability: CapabilityQuery, ScopeHash: gateScopeFor(CapabilityQuery),
			Principal: "deploy:executor", Reason: "interleaved revoke", OperationID: gateOperation("t047-revoke-interleaved"),
		})
		revoked <- err
	}()
	waitGateLockWait(t, s.f.ctx, s.f.pool, "t047-revoke-writer")
	select {
	case err := <-revoked:
		t.Fatalf("the revoke completed while the admission held the lock: %v", err)
	default:
	}

	// The admitted action was admitted before the revoke: it stays committed
	// (in-flight; the gate never rewinds it). The revoke then applies and the
	// NEXT admission refuses.
	close(releaseAdmission)
	result := <-admitted
	if result.err != nil {
		t.Fatalf("interleaved admission: %v", result.err)
	}
	if !result.decision.Allowed || !result.decision.CacheHit {
		t.Fatalf("an admission linearized before the revoke must stay allowed from the cache hit, got %+v", result.decision)
	}
	if err := <-revoked; err != nil {
		t.Fatalf("revoke after the admission released the lock: %v", err)
	}
	if got := s.admit(t, CapabilityQuery); got.Allowed || got.RefusalClass != RefusalReleaseRevoked {
		t.Fatalf("revoke before the next admission = %+v, want %s", got, RefusalReleaseRevoked)
	}

	// Unknown discipline: the committed admission is not retracted (its ok
	// audit row stands), the revoke is appended, and no automatic retry or
	// compensation entry exists. An unknown external result stays unknown —
	// nothing re-pays, re-broadcasts or re-delivers it.
	var admittedResult string
	if err := s.f.pool.QueryRow(s.f.ctx, `
SELECT result FROM recovery_audit
WHERE instance_id = $1 AND action = $2 AND operation_id = $3`,
		s.f.instanceID, GateAuditAction, inflightOp).Scan(&admittedResult); err != nil {
		t.Fatalf("read the in-flight admission audit: %v", err)
	}
	if admittedResult != controlstore.AuditOK {
		t.Fatalf("the in-flight admission audit = %q, want the committed %q (never retracted)", admittedResult, controlstore.AuditOK)
	}
	if got := s.count(t, `SELECT count(*) FROM recovery_release WHERE instance_id = $1 AND capability = 'query'`,
		s.f.instanceID); got != 2 {
		t.Fatalf("release decisions = %d, want exactly the release and its revoke (append-only, zero rewrite)", got)
	}
	t047AssertNoAutomaticRetryEntry(t)
}
