//go:build integration

// withdrawalworker_tracking_integration_test.go pins the Lane-F2 tracking
// scan's scheduling contract against real PostgreSQL: the candidate query
// includes terminal `completed` intents whose tracking is unresolved (and
// excludes genuinely converged/irrelevant ones), the batch rotates through
// the pending set with a keyset cursor so nothing past the first page
// starves, one intent's ConfirmAttempt failure never blocks the rest, and a
// failed intent is retried on the next rotation. No chain and no process are
// needed: the chain probe is the injected ConfirmAttempt recorder.
package app

import (
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/xtianxx/txharbor/internal/db"
)

const (
	trackingChainID   int64 = 31337
	trackingAsset           = "0x1111111111111111111111111111111111111111"
	trackingRecipient       = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	trackingSender          = "0xcccccccccccccccccccccccccccccccccccccccc"
)

// startTrackingPG boots one migrated PostgreSQL container (skips, never
// passes, without Docker).
func startTrackingPG(t *testing.T) string {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()
	ctr, err := postgres.Run(ctx, "postgres:18.6-trixie",
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
		DSN: dsn, LockTimeout: 5 * time.Second, ConnectTimeout: 5 * time.Second,
	}, io.Discard); err != nil {
		t.Fatalf("migrate up: %v", err)
	}
	return dsn
}

func trackingMustExec(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sql string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(ctx, sql, args...); err != nil {
		t.Fatalf("seed exec failed: %v\n%s", err, sql)
	}
}

func trackingHash(n int) string { return fmt.Sprintf("0x%064x", n) }

// trackingSeedIntent seeds one completed/failed intent with (optionally) one
// attempt of the given state and an explicit admitted_at ordering key.
func trackingSeedIntent(t *testing.T, ctx context.Context, pool *pgxpool.Pool, n int, intentState, attemptState string, admittedAt time.Time) string {
	t.Helper()
	auth := fmt.Sprintf("auth-t%d", n)
	req := fmt.Sprintf("req-t%d", n)
	intent := fmt.Sprintf("intent-t%d", n)
	binding := fmt.Sprintf("bind-t%d", n)

	trackingMustExec(t, ctx, pool, `INSERT INTO withdrawal_authorizations
		(authorization_id, caller_id, chain_id, asset, recipient, amount, state)
		VALUES ($1, 1, $2, $3, $4, 1000, 'active')`,
		auth, trackingChainID, trackingAsset, trackingRecipient)
	trackingMustExec(t, ctx, pool, `INSERT INTO withdrawal_requests
		(request_id, caller_id, idempotency_key, authorization_id, chain_id, asset, recipient, amount)
		VALUES ($1, 1, $2, $3, $4, $5, $6, 1000)`,
		req, "idem-"+req, auth, trackingChainID, trackingAsset, trackingRecipient)
	trackingMustExec(t, ctx, pool, `INSERT INTO nonce_bindings
		(binding_id, intent_id, chain_id, sender, nonce, state, authorization_id,
		 authorization_version, registry_seq, allocation_observation_id)
		VALUES ($1, $2, $3, $4, $5, 'allocated', $6, $7, 1, $8)`,
		binding, intent, trackingChainID, trackingSender, n, auth, fmt.Sprintf("%064d", 1), "obs-"+binding)
	trackingMustExec(t, ctx, pool, `INSERT INTO payment_intents
		(intent_id, request_id, chain_id, sender, authorization_id, authorization_version,
		 state, admitted_recovery_version, admitted_at)
		VALUES ($1, $2, $3, $4, $5, 1, $6, 0, $7)`,
		intent, req, trackingChainID, trackingSender, auth, intentState, admittedAt)

	if attemptState == "" {
		return intent
	}
	attempt := fmt.Sprintf("att-t%d", n)
	var effectiveAt, confirmedAt *time.Time
	if attemptState == "confirmed" {
		now := time.Now()
		effectiveAt, confirmedAt = &now, &now
	}
	trackingMustExec(t, ctx, pool, `INSERT INTO tx_attempts (
		attempt_id, signing_request_id, intent_id, binding_ref, authorization_id,
		authorization_version, recovery_version, chain_id, sender, nonce, tx_type,
		to_addr, value, data, gas_limit, gas_price, max_fee_per_gas, max_priority_fee_per_gas,
		asset, recipient, amount, canonical_envelope, content_hash, state, effective_at, confirmed_at)
		VALUES ($1, $2, $3, $4, $5, 1, 0, $6, $7, $8, 2,
			$9, 0, '\x', 100000, NULL, 1000000000, 100000000, $9, $10, 1000, '{}', $11, $12, $13, $14)`,
		attempt, "sr-"+attempt, intent, binding, auth, trackingChainID, trackingSender, n, trackingAsset,
		trackingRecipient, trackingHash(n), attemptState, effectiveAt, confirmedAt)
	return intent
}

// trackingSeedReceipt attaches one receipt row (tx_hash/block_hash unique).
func trackingSeedReceipt(t *testing.T, ctx context.Context, pool *pgxpool.Pool, attemptID string, n, blockNumber int) {
	t.Helper()
	trackingMustExec(t, ctx, pool, `INSERT INTO tx_receipts
		(attempt_id, tx_hash, status, block_number, block_hash, effect, transfer_detail,
		 canonicality, confirmations, confirm_threshold, confirm_policy_seq, confirm_tip_number, confirmed_at, updated_at)
		VALUES ($1, $2, 1, $3, $4, 'effective', '', 'canonical', 2, 1, 1, $3, now(), now())`,
		attemptID, trackingHash(9000+n), blockNumber, trackingHash(8000+n))
}

// trackingSeedChainBlock seeds one canonical chain-truth row.
func trackingSeedChainBlock(t *testing.T, ctx context.Context, pool *pgxpool.Pool, number int, hash string) {
	t.Helper()
	trackingMustExec(t, ctx, pool, `INSERT INTO chain_blocks (chain_id, number, hash, parent_hash, canonical)
		VALUES ($1, $2, $3, $4, TRUE)`, trackingChainID, number, hash, trackingHash(7000))
}

// trackingVisitRecorder is the injected ConfirmAttempt: it records the visit
// order and injects a bounded number of failures per intent.
type trackingVisitRecorder struct {
	mu     sync.Mutex
	visits []string
	fail   map[string]int
}

func (r *trackingVisitRecorder) confirm(_ context.Context, intentID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.visits = append(r.visits, intentID)
	if r.fail[intentID] > 0 {
		r.fail[intentID]--
		return fmt.Errorf("injected tracking failure for %s", intentID)
	}
	return nil
}

func (r *trackingVisitRecorder) take() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := append([]string(nil), r.visits...)
	r.visits = nil
	return out
}

func trackingAssertUnique(t *testing.T, pass []string) {
	t.Helper()
	seen := map[string]bool{}
	for _, id := range pass {
		if seen[id] {
			t.Fatalf("intent %s visited twice in one pass: %v", id, pass)
		}
		seen[id] = true
	}
}

func trackingWantRange(n, from, to int) []string {
	var out []string
	for i := from; i <= to; i++ {
		out = append(out, fmt.Sprintf("intent-t%d", i))
	}
	return out
}

func trackingAssertEqual(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("visits = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("visits = %v, want %v", got, want)
		}
	}
}

// TestWithdrawalWorkerTrackingScanRotation pins the Lane-F2 scan contract:
//
//   - candidates: completed intents whose tracking is unresolved — in-flight
//     attempts, receipts below threshold, and canonical receipts whose block
//     vanished from chain truth while the truth has reached that height;
//   - exclusions: genuinely converged completed intents (matching canonical
//     receipt), failed intents, completed intents whose latest attempt never
//     left prepare/sign, intents without attempts, and receipts whose chain
//     truth has not reached them yet (a lagging indexer is not a reorg);
//   - scheduling: a full first page (32) is followed by the remaining tail
//     page and then wraps to the head; no intent is visited twice in a pass;
//   - failure isolation: a ConfirmAttempt error is logged, the rest of the
//     page still runs, and the failed intent is retried on the next rotation.
func TestWithdrawalWorkerTrackingScanRotation(t *testing.T) {
	dsn := startTrackingPG(t)
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)

	trackingMustExec(t, ctx, pool, `INSERT INTO caller (caller_id, label) VALUES (1, 'tracking')`)
	trackingMustExec(t, ctx, pool, `INSERT INTO nonce_wallet_registry (chain_id, sender, state, registry_seq)
		VALUES ($1, $2, 'active', 1)`, trackingChainID, trackingSender)

	base := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	// 36 in-flight candidates (attempt `sent`).
	for n := 0; n < 36; n++ {
		trackingSeedIntent(t, ctx, pool, n, "completed", "sent", base.Add(time.Duration(n)*time.Second))
	}
	// n=36: confirmed attempt whose canonical receipt's block is no longer
	// canonical while chain truth has reached that height -> candidate (its
	// visit is pinned by the tail-page assertion below).
	trackingSeedIntent(t, ctx, pool, 36, "completed", "confirmed", base.Add(36*time.Second))
	trackingSeedReceipt(t, ctx, pool, fmt.Sprintf("att-t%d", 36), 36, 5)
	trackingSeedChainBlock(t, ctx, pool, 10, trackingHash(8000+37)) // truth head past block 5
	// n=37: confirmed attempt with a matching canonical receipt -> converged.
	intent37 := trackingSeedIntent(t, ctx, pool, 37, "completed", "confirmed", base.Add(37*time.Second))
	trackingSeedReceipt(t, ctx, pool, fmt.Sprintf("att-t%d", 37), 37, 10)
	// n=38: failed intent -> never tracked.
	intent38 := trackingSeedIntent(t, ctx, pool, 38, "failed", "sent", base.Add(38*time.Second))
	// n=39: latest attempt never left prepare -> no chain probe exists.
	intent39 := trackingSeedIntent(t, ctx, pool, 39, "completed", "prepared", base.Add(39*time.Second))
	// n=40: completed intent without any attempt -> nothing to track.
	intent40 := trackingSeedIntent(t, ctx, pool, 40, "completed", "", base.Add(40*time.Second))
	// n=41: canonical receipt whose block is above the chain-truth head (a
	// lagging indexer must not be mistaken for a reorg) -> not a candidate.
	intent41 := trackingSeedIntent(t, ctx, pool, 41, "completed", "confirmed", base.Add(41*time.Second))
	trackingSeedReceipt(t, ctx, pool, fmt.Sprintf("att-t%d", 41), 41, 20)

	rec := &trackingVisitRecorder{fail: map[string]int{"intent-t5": 1}}
	w := &WithdrawalWorker{Pool: pool, ConfirmAttempt: rec.confirm}

	// Pass 1: the full first page in admitted_at order; the failure at t5 is
	// logged and the rest of the page still runs.
	w.runTrackingPass(ctx)
	pass1 := rec.take()
	trackingAssertUnique(t, pass1)
	trackingAssertEqual(t, pass1, trackingWantRange(0, 0, 31))

	// Pass 2: the 5-intent tail (short page -> the next pass wraps).
	w.runTrackingPass(ctx)
	pass2 := rec.take()
	trackingAssertUnique(t, pass2)
	trackingAssertEqual(t, pass2, trackingWantRange(0, 32, 36))

	// Pass 3: wrapped to the head; the failed intent is retried.
	w.runTrackingPass(ctx)
	pass3 := rec.take()
	if len(pass3) != 32 || pass3[5] != "intent-t5" {
		t.Fatalf("rotation re-visits = %v, want the head page with intent-t5 retried at index 5", pass3)
	}
	for _, id := range []string{intent37, intent38, intent39, intent40, intent41} {
		for _, pass := range [][]string{pass1, pass2, pass3} {
			for _, visited := range pass {
				if visited == id {
					t.Fatalf("non-candidate %s was visited: %v", id, pass)
				}
			}
		}
	}
}
