//go:build integration

// reverify_write_order_protocol_integration_test.go holds the post-protocol
// assertions that cannot compile against the pre-protocol implementation: the
// observable discard counters and the token component named in the discard
// audit/reason. The behavioural negative-control assertions live in
// reverify_write_order_integration_test.go.
package reconciliation

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// TestIntegrationReverifyWriteOrderProtocolCounters pins the observable
// discard surface of both entries: the sweep counts the discarded outcome
// (and cannot claim completeness), and the ticket entry names the token
// component that invalidated the stale finding.
func TestIntegrationReverifyWriteOrderProtocolCounters(t *testing.T) {
	ctx, pool, store := reconIT(t)

	t.Run("sweep_discard_counter", func(t *testing.T) {
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
					Verdict: ReverifyConsistent, EvidenceRef: "wo:a", FreshnessAt: time.Now().UTC(),
				}),
			})
			doneA <- outcome{result: result, err: err}
		}()
		<-reached
		if _, err := store.RunReverifySweep(ctx, ReverifySweepRequest{
			TaskID: taskID, Actor: "wo-it",
			Slice:     ReverifySlice{MaxItems: 4, MaxPGRequests: 40, MaxItemAttempts: 3},
			Evaluator: woDivergentEvaluator(),
		}); err != nil {
			t.Fatalf("B RunReverifySweep: %v", err)
		}
		close(release)
		gotA := <-doneA
		if gotA.err != nil {
			t.Fatalf("A RunReverifySweep: %v", gotA.err)
		}
		if gotA.result.Discarded != 1 {
			t.Fatalf("A discarded = %d, want 1", gotA.result.Discarded)
		}
		if gotA.result.Rechecked != 0 || gotA.result.Consistent != 0 {
			t.Fatalf("A rechecked/consistent = %d/%d, want 0/0 for a discarded outcome",
				gotA.result.Rechecked, gotA.result.Consistent)
		}
		if gotA.result.VerifiedComplete() {
			t.Fatal("A claimed verified completeness with a discarded outcome")
		}
	})

	t.Run("ticket_discard_names_token_component", func(t *testing.T) {
		taskID := uuid.NewString()
		tvSeedTask(t, ctx, pool, taskID)
		discrepancyID := uuid.NewString()
		txHash := "0x" + strings.Repeat("e5", 32)
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
		if _, err := store.VerifyPendingDiscrepancy(ctx, tvTicketVerifyRequest(taskID, discrepancyID,
			woErrorChain{err: errors.New("write-order it: chain unavailable")})); err != nil {
			t.Fatalf("B VerifyPendingDiscrepancy: %v", err)
		}
		close(release)
		gotA := <-doneA
		if gotA.err != nil {
			t.Fatalf("A VerifyPendingDiscrepancy: %v", gotA.err)
		}
		if !gotA.result.Discarded {
			t.Fatalf("A result = %+v, want discarded", gotA.result)
		}
		if !strings.Contains(gotA.result.DiscardReason, "generation advanced") {
			t.Fatalf("discard reason %q does not name the invalidated token generation", gotA.result.DiscardReason)
		}
		if !strings.Contains(gotA.result.DiscardReason, "ticket re-verification") {
			t.Fatalf("discard reason %q does not name the entry", gotA.result.DiscardReason)
		}
	})
}
