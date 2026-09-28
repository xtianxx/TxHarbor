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
	"fmt"
	"io"
	"os"
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
