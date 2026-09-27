//go:build integration

// reverify_write_order_integration_test.go pins the T026/T027 write-write
// ordering protocol on real PostgreSQL with controllable barriers (channels,
// never sleeps): A starts its evidence read first and is paused inside it; B
// starts later, commits a newer conclusion (divergent/unknown, or a newer
// consistent) and wins; A then resumes and must be rejected. The stale A
// outcome must not:
//
//   - overwrite B's committed conclusion (latest verdict stays B's),
//   - delete B's coverage gap,
//   - advance the sweep's traversal cursor / progress counters,
//   - make a close accept the stale consistent result.
//
// The assertions use only the public store surface plus SQL, so this file is
// the negative control that fails on the pre-protocol implementation (where A
// wins and B's evidence is overwritten) and passes after it. PostgreSQL comes
// from testcontainers; when no Docker provider is healthy the package reports
// NOT RUN (t.Skip), never a pass.
package reconciliation

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// woErrorChain reports a bounded chain read failure: the ticket entry records
// a fail-closed unknown finding (never consistent).
type woErrorChain struct{ err error }

func (c woErrorChain) Observe(context.Context, ChainFactsQuery) (ChainFactsBundle, error) {
	return ChainFactsBundle{}, c.err
}

// woBlockingEvaluator blocks one sweep evidence read until release.
func woBlockingEvaluator(reached, release chan struct{}, finding ReverifyFinding) ReverifyEvaluator {
	return ReverifyEvaluatorFunc(func(ctx context.Context, item ClosedDiscrepancy, budget ScanQueryBudget) (ReverifyFinding, error) {
		close(reached)
		select {
		case <-release:
			return finding, nil
		case <-ctx.Done():
			return ReverifyFinding{}, ctx.Err()
		}
	})
}

// woDivergentEvaluator returns one divergent finding (new evidence).
func woDivergentEvaluator() ReverifyEvaluator {
	return ReverifyEvaluatorFunc(func(ctx context.Context, item ClosedDiscrepancy, budget ScanQueryBudget) (ReverifyFinding, error) {
		return ReverifyFinding{
			Verdict:     ReverifyDivergent,
			EvidenceRef: "wo:newer-evidence",
			FreshnessAt: time.Now().UTC(),
			Trigger:     InvalidationNewEvidence,
			Detail:      "write-order it: newer evidence observed",
		}, nil
	})
}

// woSeedClosedItem inserts one in-scope closed item for the sweep.
func woSeedClosedItem(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id string, evidenceAt time.Time) {
	t.Helper()
	scope, err := HeightIdentityScope(reconITChainID, 2, 5, BusinessWithdrawal)
	if err != nil {
		t.Fatalf("HeightIdentityScope: %v", err)
	}
	domain, err := PersistedEvidenceDomainJSONFor(
		VersionDomain{BlockNumber: 4, BlockHash: "0x4", EvidenceAt: evidenceAt},
		&scope, BusinessWithdrawal, &DetectionInterval{Kind: ScopeHeight, From: 2, To: 5}, nil)
	if err != nil {
		t.Fatalf("PersistedEvidenceDomainJSONFor: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO discrepancy (
		    discrepancy_id, category, business_key, content_hash, evidence_version_domain, state, close_basis)
		VALUES ($1::uuid, 'missing', $2, $3, $4::jsonb, 'closed', '{"range":"2..5","block":4}'::jsonb)`,
		id, "tx_hash=0x"+strings.Repeat("ab", 32), []byte{0x01, 0x02}, string(domain)); err != nil {
		t.Fatalf("seed closed item: %v", err)
	}
}

// woLatestVerdict reads the newest recorded verdict by insertion order.
func woLatestVerdict(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id string) string {
	t.Helper()
	var verdict string
	err := pool.QueryRow(ctx, `
		SELECT verdict FROM reverify WHERE discrepancy_id = $1::uuid
		ORDER BY created_at DESC, reverify_id DESC LIMIT 1`, id).Scan(&verdict)
	if errors.Is(err, pgx.ErrNoRows) {
		return ""
	}
	if err != nil {
		t.Fatalf("read latest verdict of %s: %v", id, err)
	}
	return verdict
}

// woCount runs one scalar count.
func woCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, query string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(ctx, query, args...).Scan(&n); err != nil {
		t.Fatalf("count query: %v", err)
	}
	return n
}

// TestIntegrationReverifyWriteOrderTicketStaleConsistentCannotOverwriteNewerUnknown
// is the confirmed defect reproduction for the pending_verify entry: A (older
// read) returns consistent, B (later read) commits unknown first, and A's
// stale consistent must be rejected — B's verdict and gap stay, the close
// stays refused, and the rejection is audited.
func TestIntegrationReverifyWriteOrderTicketStaleConsistentCannotOverwriteNewerUnknown(t *testing.T) {
	ctx, pool, store := reconIT(t)
	taskID := uuid.NewString()
	tvSeedTask(t, ctx, pool, taskID)
	discrepancyID := uuid.NewString()
	txHash := "0x" + strings.Repeat("a1", 32)
	tvSeedPendingTicket(t, ctx, pool, tvChainID, discrepancyID, txHash, nil)

	// A: older evidence read; it blocks inside the chain Observe so B can
	// commit first.
	reached := make(chan struct{})
	release := make(chan struct{})
	chainA := &tvFakeChain{bundle: tvCompleteChainBundle(txHash), reached: reached, release: release}
	type outcome struct {
		result TicketVerifyResult
		err    error
	}
	doneA := make(chan outcome, 1)
	go func() {
		result, err := store.VerifyPendingDiscrepancy(ctx,
			tvTicketVerifyRequest(taskID, discrepancyID, chainA))
		doneA <- outcome{result: result, err: err}
	}()
	<-reached

	// B: later evidence read (chain unavailable -> fail-closed unknown),
	// commits first while the ticket is still pending_verify.
	resultB, err := store.VerifyPendingDiscrepancy(ctx, tvTicketVerifyRequest(taskID, discrepancyID,
		woErrorChain{err: errors.New("write-order it: chain unavailable")}))
	if err != nil {
		t.Fatalf("B VerifyPendingDiscrepancy: %v", err)
	}
	if resultB.Discarded {
		t.Fatalf("B was discarded unexpectedly: %+v", resultB)
	}
	if resultB.Consistent || resultB.Verdict != ReverifyUnknown {
		t.Fatalf("B verdict = %s consistent=%t, want unknown", resultB.Verdict, resultB.Consistent)
	}
	if !resultB.GapWritten {
		t.Fatalf("B did not write its visible coverage gap: %+v", resultB)
	}
	if state := woCount(t, ctx, pool, `
		SELECT count(*)::bigint FROM discrepancy WHERE discrepancy_id = $1::uuid AND state = 'pending_verify'`,
		discrepancyID); state != 1 {
		t.Fatalf("ticket left pending_verify after B's committed unknown")
	}

	close(release)
	gotA := <-doneA
	if gotA.err != nil {
		t.Fatalf("A VerifyPendingDiscrepancy: %v", gotA.err)
	}

	// A's stale consistent is rejected: it must not overwrite B, delete B's
	// gap, or make the close accept it.
	if !gotA.result.Discarded {
		t.Fatalf("A result = %+v, want discarded (stale outcome overwrote the newer conclusion)", gotA.result)
	}
	if gotA.result.State != DiscrepancyStatePendingVerify {
		t.Fatalf("A observed state = %s, want pending_verify", gotA.result.State)
	}
	if n := woCount(t, ctx, pool,
		`SELECT count(*)::bigint FROM reverify WHERE discrepancy_id = $1::uuid`, discrepancyID); n != 1 {
		t.Fatalf("reverify rows = %d, want 1 (only B's committed verdict)", n)
	}
	if verdict := woLatestVerdict(t, ctx, pool, discrepancyID); verdict != string(ReverifyUnknown) {
		t.Fatalf("latest verdict = %q, want unknown (B's newer conclusion preserved)", verdict)
	}
	if n := woCount(t, ctx, pool, `
		SELECT count(*)::bigint FROM recon_gap
		WHERE task_id = $1::uuid AND reason = 'query_failed'`, taskID); n != 1 {
		t.Fatalf("B's coverage gap rows = %d, want 1 (A must not delete it)", n)
	}
	if n := woCount(t, ctx, pool, `
		SELECT count(*)::bigint FROM recon_audit
		WHERE action = 'reverify' AND result = 'discarded' AND target->>'discrepancy_id' = $1`,
		discrepancyID); n != 1 {
		t.Fatalf("discard audit rows = %d, want 1", n)
	}

	// The close cannot consume the stale consistent result.
	if _, err := store.CloseDiscrepancy(ctx, DiscrepancyCloseRequest{
		DiscrepancyID:     discrepancyID,
		Actor:             "wo-it-close",
		Reason:            "write-order it",
		CloseBasis:        []byte(`{"range":"2..5"}`),
		ReverifyTolerance: time.Hour,
	}); !errors.Is(err, ErrReverifyRequired) {
		t.Fatalf("close err = %v, want ErrReverifyRequired over B's unknown", err)
	}
}

// TestIntegrationReverifyWriteOrderTicketConcurrentSameStateExactlyOneWins
// pins the same-initial-state race: two entries capture the same token, both
// read evidence, and exactly one commits; the other is discarded. The
// surviving consistent row still supports a close.
func TestIntegrationReverifyWriteOrderTicketConcurrentSameStateExactlyOneWins(t *testing.T) {
	ctx, pool, store := reconIT(t)
	taskID := uuid.NewString()
	tvSeedTask(t, ctx, pool, taskID)
	discrepancyID := uuid.NewString()
	txHash := "0x" + strings.Repeat("b2", 32)
	tvSeedPendingTicket(t, ctx, pool, tvChainID, discrepancyID, txHash, nil)

	reached1 := make(chan struct{})
	reached2 := make(chan struct{})
	release1 := make(chan struct{})
	release2 := make(chan struct{})
	chain1 := &tvFakeChain{bundle: tvCompleteChainBundle(txHash), reached: reached1, release: release1}
	chain2 := &tvFakeChain{bundle: tvCompleteChainBundle(txHash), reached: reached2, release: release2}

	type outcome struct {
		result TicketVerifyResult
		err    error
	}
	done := make(chan outcome, 2)
	run := func(chain ChainFactsReader) {
		result, err := store.VerifyPendingDiscrepancy(ctx,
			tvTicketVerifyRequest(taskID, discrepancyID, chain))
		done <- outcome{result: result, err: err}
	}
	go run(chain1)
	go run(chain2)
	<-reached1
	<-reached2
	close(release1)
	close(release2)

	discarded, accepted := 0, 0
	for i := 0; i < 2; i++ {
		got := <-done
		if got.err != nil {
			t.Fatalf("concurrent VerifyPendingDiscrepancy: %v", got.err)
		}
		if got.result.Discarded {
			discarded++
		} else {
			accepted++
		}
	}
	if discarded != 1 || accepted != 1 {
		t.Fatalf("concurrent outcomes = discarded %d accepted %d, want exactly 1/1", discarded, accepted)
	}
	if n := woCount(t, ctx, pool,
		`SELECT count(*)::bigint FROM reverify WHERE discrepancy_id = $1::uuid`, discrepancyID); n != 1 {
		t.Fatalf("reverify rows = %d, want 1 (exactly one winner)", n)
	}
	if state := woCount(t, ctx, pool, `
		SELECT count(*)::bigint FROM discrepancy WHERE discrepancy_id = $1::uuid AND state = 'pending_verify'`,
		discrepancyID); state != 1 {
		t.Fatalf("ticket left pending_verify after concurrent verifications")
	}
	if _, err := store.CloseDiscrepancy(ctx, DiscrepancyCloseRequest{
		DiscrepancyID:     discrepancyID,
		Actor:             "wo-it-close",
		Reason:            "write-order it",
		CloseBasis:        []byte(`{"range":"2..5"}`),
		ReverifyTolerance: time.Hour,
	}); err != nil {
		t.Fatalf("close over the single surviving consistent row: %v", err)
	}
}

// TestIntegrationReverifyWriteOrderTicketStaleConsistentVsNewerConsistent
// pins the explicit rule that a stale consistent is rejected even when the
// newer committed conclusion is also consistent; a fresh attempt with a new
// token is then legitimately accepted and can close.
func TestIntegrationReverifyWriteOrderTicketStaleConsistentVsNewerConsistent(t *testing.T) {
	ctx, pool, store := reconIT(t)
	taskID := uuid.NewString()
	tvSeedTask(t, ctx, pool, taskID)
	discrepancyID := uuid.NewString()
	txHash := "0x" + strings.Repeat("c3", 32)
	tvSeedPendingTicket(t, ctx, pool, tvChainID, discrepancyID, txHash, nil)

	reached := make(chan struct{})
	release := make(chan struct{})
	chainA := &tvFakeChain{bundle: tvCompleteChainBundle(txHash), reached: reached, release: release}
	type outcome struct {
		result TicketVerifyResult
		err    error
	}
	doneA := make(chan outcome, 1)
	go func() {
		result, err := store.VerifyPendingDiscrepancy(ctx,
			tvTicketVerifyRequest(taskID, discrepancyID, chainA))
		doneA <- outcome{result: result, err: err}
	}()
	<-reached

	// B commits a newer consistent verdict first.
	resultB, err := store.VerifyPendingDiscrepancy(ctx, tvTicketVerifyRequest(taskID, discrepancyID,
		&tvFakeChain{bundle: tvCompleteChainBundle(txHash)}))
	if err != nil {
		t.Fatalf("B VerifyPendingDiscrepancy: %v", err)
	}
	if !resultB.Consistent || resultB.Discarded {
		t.Fatalf("B result = %+v, want accepted consistent", resultB)
	}

	close(release)
	gotA := <-doneA
	if gotA.err != nil {
		t.Fatalf("A VerifyPendingDiscrepancy: %v", gotA.err)
	}
	if !gotA.result.Discarded {
		t.Fatalf("A result = %+v, want discarded (consistent-over-consistent is not exempt)", gotA.result)
	}
	if n := woCount(t, ctx, pool,
		`SELECT count(*)::bigint FROM reverify WHERE discrepancy_id = $1::uuid`, discrepancyID); n != 1 {
		t.Fatalf("reverify rows = %d, want 1 (B's newer consistent only)", n)
	}

	// A fresh attempt captures a new token and is legitimately accepted; the
	// positive close path still works.
	fresh, err := store.VerifyPendingDiscrepancy(ctx, tvTicketVerifyRequest(taskID, discrepancyID,
		&tvFakeChain{bundle: tvCompleteChainBundle(txHash)}))
	if err != nil {
		t.Fatalf("fresh VerifyPendingDiscrepancy: %v", err)
	}
	if !fresh.Consistent || fresh.Discarded {
		t.Fatalf("fresh result = %+v, want accepted consistent", fresh)
	}
	if n := woCount(t, ctx, pool,
		`SELECT count(*)::bigint FROM reverify WHERE discrepancy_id = $1::uuid`, discrepancyID); n != 2 {
		t.Fatalf("reverify rows = %d, want 2 after the fresh re-read", n)
	}
	if _, err := store.CloseDiscrepancy(ctx, DiscrepancyCloseRequest{
		DiscrepancyID:     discrepancyID,
		Actor:             "wo-it-close",
		Reason:            "write-order it",
		CloseBasis:        []byte(`{"range":"2..5"}`),
		ReverifyTolerance: time.Hour,
	}); err != nil {
		t.Fatalf("close over the fresh consistent row: %v", err)
	}
}

// TestIntegrationReverifyWriteOrderSweepStaleConsistentCannotOverwriteNewerDivergent
// is the confirmed defect reproduction for the history sweep: A (older read)
// returns consistent and pauses; B (later read) commits divergent and
// invalidates the item; A's stale consistent must not overwrite B's verdict,
// delete B's gap, advance the traversal cursor, or make a close accept it.
func TestIntegrationReverifyWriteOrderSweepStaleConsistentCannotOverwriteNewerDivergent(t *testing.T) {
	ctx, pool, store := reconIT(t)
	taskID := uuid.NewString()
	reconSeedHeightTask(t, ctx, pool, taskID, 2, 5, "running", "")
	discrepancyID := uuid.NewString()
	woSeedClosedItem(t, ctx, pool, discrepancyID, time.Now().UTC().Add(-time.Hour))

	reached := make(chan struct{})
	release := make(chan struct{})
	type outcome struct {
		result ReverifySweepResult
		err    error
	}
	doneA := make(chan outcome, 1)
	go func() {
		result, err := store.RunReverifySweep(ctx, ReverifySweepRequest{
			TaskID: taskID, Actor: "wo-it",
			Slice: ReverifySlice{MaxItems: 4, MaxPGRequests: 40, MaxItemAttempts: 3},
			Evaluator: woBlockingEvaluator(reached, release, ReverifyFinding{
				Verdict: ReverifyConsistent, EvidenceRef: "wo:a-consistent", FreshnessAt: time.Now().UTC(),
			}),
		})
		doneA <- outcome{result: result, err: err}
	}()
	<-reached

	// B commits the newer divergent conclusion and invalidates the item.
	resultB, err := store.RunReverifySweep(ctx, ReverifySweepRequest{
		TaskID: taskID, Actor: "wo-it",
		Slice:     ReverifySlice{MaxItems: 4, MaxPGRequests: 40, MaxItemAttempts: 3},
		Evaluator: woDivergentEvaluator(),
	})
	if err != nil {
		t.Fatalf("B RunReverifySweep: %v", err)
	}
	if resultB.Divergent != 1 || resultB.InvalidationErrors != 0 {
		t.Fatalf("B divergent/invalidation_errors = %d/%d, want 1/0", resultB.Divergent, resultB.InvalidationErrors)
	}
	if state := woCount(t, ctx, pool, `
		SELECT count(*)::bigint FROM discrepancy WHERE discrepancy_id = $1::uuid AND state = 'pending_verify'`,
		discrepancyID); state != 1 {
		t.Fatalf("item did not enter pending_verify after B's divergent invalidation")
	}

	close(release)
	gotA := <-doneA
	if gotA.err != nil {
		t.Fatalf("A RunReverifySweep: %v", gotA.err)
	}

	if verdict := woLatestVerdict(t, ctx, pool, discrepancyID); verdict != string(ReverifyDivergent) {
		t.Fatalf("latest verdict = %q, want divergent (B's newer conclusion preserved)", verdict)
	}
	if n := woCount(t, ctx, pool, `
		SELECT count(*)::bigint FROM recon_gap
		WHERE task_id = $1::uuid AND reason = 'query_failed'`, taskID); n != 1 {
		t.Fatalf("B's coverage gap rows = %d, want 1 (A must not delete it)", n)
	}
	if gotA.result.Rechecked != 0 {
		t.Fatalf("A rechecked = %d, want 0 (a discarded outcome is not progress)", gotA.result.Rechecked)
	}
	if gotA.result.CursorAdvanced {
		t.Fatalf("A advanced the traversal cursor past an item it never revalidated")
	}
	if gotA.result.VerifiedComplete() {
		t.Fatalf("A claimed verified completeness with a discarded outcome")
	}
	warned := false
	for _, warning := range gotA.result.Warnings {
		if strings.Contains(warning, "discarded") {
			warned = true
		}
	}
	if !warned {
		t.Fatalf("A warnings = %v, want the discard surfaced", gotA.result.Warnings)
	}
	if n := woCount(t, ctx, pool, `
		SELECT count(*)::bigint FROM recon_audit
		WHERE action = 'reverify' AND result = 'discarded' AND target->>'discrepancy_id' = $1`,
		discrepancyID); n != 1 {
		t.Fatalf("discard audit rows = %d, want 1", n)
	}
	if _, err := store.CloseDiscrepancy(ctx, DiscrepancyCloseRequest{
		DiscrepancyID:     discrepancyID,
		Actor:             "wo-it-close",
		Reason:            "write-order it",
		CloseBasis:        []byte(`{"range":"2..5"}`),
		ReverifyTolerance: time.Hour,
	}); !errors.Is(err, ErrReverifyRequired) {
		t.Fatalf("close err = %v, want ErrReverifyRequired over B's divergent", err)
	}
}

// TestIntegrationReverifyWriteOrderSweepConcurrentSameStateExactlyOneWins pins
// the same-initial-state sweep race: exactly one slice commits its verdict and
// the other is discarded.
func TestIntegrationReverifyWriteOrderSweepConcurrentSameStateExactlyOneWins(t *testing.T) {
	ctx, pool, store := reconIT(t)
	taskID := uuid.NewString()
	reconSeedHeightTask(t, ctx, pool, taskID, 2, 5, "running", "")
	discrepancyID := uuid.NewString()
	woSeedClosedItem(t, ctx, pool, discrepancyID, time.Now().UTC().Add(-time.Hour))

	reached1 := make(chan struct{})
	reached2 := make(chan struct{})
	release1 := make(chan struct{})
	release2 := make(chan struct{})
	type outcome struct {
		result ReverifySweepResult
		err    error
	}
	done := make(chan outcome, 2)
	run := func(reached, release chan struct{}) {
		result, err := store.RunReverifySweep(ctx, ReverifySweepRequest{
			TaskID: taskID, Actor: "wo-it",
			Slice: ReverifySlice{MaxItems: 4, MaxPGRequests: 40, MaxItemAttempts: 3},
			Evaluator: woBlockingEvaluator(reached, release, ReverifyFinding{
				Verdict: ReverifyConsistent, EvidenceRef: "wo:concurrent-consistent", FreshnessAt: time.Now().UTC(),
			}),
		})
		done <- outcome{result: result, err: err}
	}
	go run(reached1, release1)
	go run(reached2, release2)
	<-reached1
	<-reached2
	close(release1)
	close(release2)

	winners, discarded := 0, 0
	for i := 0; i < 2; i++ {
		got := <-done
		if got.err != nil {
			t.Fatalf("concurrent RunReverifySweep: %v", got.err)
		}
		if got.result.Rechecked == 1 {
			winners++
		} else {
			discarded++
		}
	}
	if winners != 1 || discarded != 1 {
		t.Fatalf("concurrent sweep outcomes = winners %d discarded %d, want exactly 1/1", winners, discarded)
	}
	if n := woCount(t, ctx, pool,
		`SELECT count(*)::bigint FROM reverify WHERE discrepancy_id = $1::uuid`, discrepancyID); n != 1 {
		t.Fatalf("reverify rows = %d, want 1 (exactly one winner)", n)
	}
	if state := woCount(t, ctx, pool, `
		SELECT count(*)::bigint FROM discrepancy WHERE discrepancy_id = $1::uuid AND state = 'closed'`,
		discrepancyID); state != 1 {
		t.Fatalf("a consistent sweep must not change the closed state")
	}
}

// TestIntegrationReverifyWriteOrderSweepDiscardBlocksCursorPastUnrevalidatedItem
// pins that a discarded item can never be skipped by the traversal cursor: the
// discarded item is the oldest (first) in the slice, a later item is accepted,
// and the cursor must still not advance past the never-revalidated item.
func TestIntegrationReverifyWriteOrderSweepDiscardBlocksCursorPastUnrevalidatedItem(t *testing.T) {
	ctx, pool, store := reconIT(t)
	taskID := uuid.NewString()
	reconSeedHeightTask(t, ctx, pool, taskID, 2, 5, "running", "")
	// A second task over the same scope: its slice carries the superseding
	// write, so the assertion below is about the blocked task's cursor.
	otherTaskID := uuid.NewString()
	reconSeedHeightTask(t, ctx, pool, otherTaskID, 2, 5, "running", "")
	olderID := uuid.NewString()
	newerID := uuid.NewString()
	woSeedClosedItem(t, ctx, pool, olderID, time.Now().UTC().Add(-2*time.Hour))
	woSeedClosedItem(t, ctx, pool, newerID, time.Now().UTC().Add(-time.Hour))

	reached := make(chan struct{})
	release := make(chan struct{})
	evaluator := ReverifyEvaluatorFunc(func(ctx context.Context, item ClosedDiscrepancy, budget ScanQueryBudget) (ReverifyFinding, error) {
		if item.DiscrepancyID == olderID {
			close(reached)
			select {
			case <-release:
			case <-ctx.Done():
				return ReverifyFinding{}, ctx.Err()
			}
		}
		return ReverifyFinding{
			Verdict: ReverifyConsistent, EvidenceRef: "wo:cursor-" + item.DiscrepancyID,
			FreshnessAt: time.Now().UTC(),
		}, nil
	})
	type outcome struct {
		result ReverifySweepResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := store.RunReverifySweep(ctx, ReverifySweepRequest{
			TaskID: taskID, Actor: "wo-it",
			Slice:     ReverifySlice{MaxItems: 2, MaxPGRequests: 40, MaxItemAttempts: 3},
			Evaluator: evaluator,
		})
		done <- outcome{result: result, err: err}
	}()
	<-reached

	// A superseding write lands while the older item's evidence read is in
	// flight: a bounded slice under the second task commits a newer divergent
	// conclusion for the older item (the newer item is untouched).
	superseding, err := store.RunReverifySweep(ctx, ReverifySweepRequest{
		TaskID: otherTaskID, Actor: "wo-it",
		Slice:     ReverifySlice{MaxItems: 1, MaxPGRequests: 40, MaxItemAttempts: 3},
		Evaluator: woDivergentEvaluator(),
	})
	if err != nil {
		t.Fatalf("superseding RunReverifySweep: %v", err)
	}
	if superseding.Divergent != 1 {
		t.Fatalf("superseding slice divergent = %d, want 1", superseding.Divergent)
	}

	close(release)
	got := <-done
	if got.err != nil {
		t.Fatalf("RunReverifySweep: %v", got.err)
	}
	if got.result.Rechecked != 1 {
		t.Fatalf("rechecked = %d, want 1 (only the untouched newer item)", got.result.Rechecked)
	}
	if got.result.CursorAdvanced {
		t.Fatal("cursor advanced past the discarded (never revalidated) item")
	}
	if cursor, err := store.HistorySweepThrough(ctx, taskID); err != nil {
		t.Fatalf("HistorySweepThrough: %v", err)
	} else if cursor != nil {
		t.Fatalf("persisted cursor = %+v, want none while an item was discarded", cursor)
	}
	if n := woCount(t, ctx, pool, `
		SELECT count(*)::bigint FROM recon_audit
		WHERE action = 'reverify' AND result = 'discarded' AND target->>'discrepancy_id' = $1`,
		olderID); n != 1 {
		t.Fatalf("discard audit rows = %d, want 1", n)
	}
	if n := woCount(t, ctx, pool,
		`SELECT count(*)::bigint FROM reverify WHERE discrepancy_id = $1::uuid`, olderID); n != 1 {
		t.Fatalf("older item reverify rows = %d, want 1 (only the superseding verdict)", n)
	}
	if verdict := woLatestVerdict(t, ctx, pool, olderID); verdict != string(ReverifyDivergent) {
		t.Fatalf("older item latest verdict = %q, want the superseding divergent", verdict)
	}
	if n := woCount(t, ctx, pool,
		`SELECT count(*)::bigint FROM reverify WHERE discrepancy_id = $1::uuid`, newerID); n != 1 {
		t.Fatalf("accepted item reverify rows = %d, want 1", n)
	}
}

// TestIntegrationReverifyWriteOrderCloseLatestSameTimestamp pins that the
// close's latest-row read is insertion-ordered, not timestamp-ordered: two
// accepted rows with identical created_at still resolve to the later
// insertion (reverify_id), so a divergent row supersedes an older consistent.
func TestIntegrationReverifyWriteOrderCloseLatestSameTimestamp(t *testing.T) {
	ctx, pool, store := reconIT(t)
	taskID := uuid.NewString()
	tvSeedTask(t, ctx, pool, taskID)
	discrepancyID := uuid.NewString()
	tvSeedPendingTicket(t, ctx, pool, tvChainID, discrepancyID, "0x"+"d4", nil)

	if _, err := pool.Exec(ctx, `
		INSERT INTO reverify (discrepancy_id, verdict, evidence_ref, freshness_at)
		VALUES ($1::uuid, 'consistent', 'wo:old-consistent', now())`, discrepancyID); err != nil {
		t.Fatalf("seed consistent row: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO reverify (discrepancy_id, verdict, evidence_ref, freshness_at)
		VALUES ($1::uuid, 'divergent', 'wo:new-divergent', now())`, discrepancyID); err != nil {
		t.Fatalf("seed divergent row: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE reverify SET created_at = '2026-01-01T00:00:00Z'
		WHERE discrepancy_id = $1::uuid`, discrepancyID); err != nil {
		t.Fatalf("force identical timestamps: %v", err)
	}
	if _, err := store.CloseDiscrepancy(ctx, DiscrepancyCloseRequest{
		DiscrepancyID:     discrepancyID,
		Actor:             "wo-it-close",
		Reason:            "write-order it",
		CloseBasis:        []byte(`{"range":"2..5"}`),
		ReverifyTolerance: time.Hour,
	}); !errors.Is(err, ErrReverifyRequired) {
		t.Fatalf("close err = %v, want ErrReverifyRequired over the later divergent row", err)
	}
}
