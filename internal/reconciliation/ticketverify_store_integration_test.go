//go:build integration

// ticketverify_store_integration_test.go holds the database-backed regression
// tests of the two concurrency guards the pending_verify re-verification closes:
//
//   - the ticket verify CAS: a ticket that leaves pending_verify while the
//     evidence read is in flight is discarded and audited, never written as
//     fresh evidence;
//   - the close latest-row-wins guard: the latest reverify row is read inside
//     the same row-locked transaction as the state CAS, so a divergent row
//     committed while a close is waiting for the lock can never be bypassed by
//     a stale pre-lock read.
//
// PostgreSQL comes from testcontainers. When no Docker provider is healthy the
// package reports NOT RUN (t.Skip), never a pass.
package reconciliation

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// tvFakeChain is a complete read-only chain-facts bundle carrying the recorded
// candidate; it can block one Observe call so a test can interleave a state
// change between the evidence read and the persist CAS.
type tvFakeChain struct {
	bundle  ChainFactsBundle
	reached chan struct{}
	release chan struct{}
	once    sync.Once
}

func (f *tvFakeChain) Observe(ctx context.Context, q ChainFactsQuery) (ChainFactsBundle, error) {
	if f.reached != nil {
		f.once.Do(func() { close(f.reached) })
	}
	if f.release != nil {
		select {
		case <-f.release:
		case <-ctx.Done():
			return ChainFactsBundle{}, ctx.Err()
		}
	}
	return f.bundle, nil
}

// tvFakePG reports one complete PG business record.
type tvFakePG struct{}

func (tvFakePG) Read(ctx context.Context, req PGReadRequest) (PGStateRecord, error) {
	return PGStateRecord{Status: PGStateComplete, BusinessType: req.BusinessType, BusinessKey: req.Key}, nil
}

// tvFakeEvents reports one complete, empty event-delivery bundle.
type tvFakeEvents struct{}

func (tvFakeEvents) Observe(ctx context.Context, q EventStateQuery) (EventStateEvidence, error) {
	return EventStateEvidence{Status: EventDeliveryComplete, Scope: q.Scope, Interval: q.Interval}, nil
}

// tvChainID is the numeric chain identity the fakes and the seeded task use
// (the production compare resolves the chain id through scanNumericChainID, so
// a symbolic identity would keep the chain party unknown).
const tvChainID = "33137"

// tvSeedTask inserts one running numeric-chain height task.
func tvSeedTask(t *testing.T, ctx context.Context, pool *pgxpool.Pool, taskID string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO recon_task (task_id, scope_chain_id, scope_kind, scope_start, scope_end,
		    business_types, upstream_receipt_source, policy_refs, state, budget, created_by)
		VALUES ($1::uuid, $2, 'height', 2, 5, ARRAY['withdrawal']::text[],
		        '{"withdrawal":{"source":"it","connected":true}}'::jsonb,
		        '{"confirm_threshold_n":3}'::jsonb, 'running', '{}'::jsonb, 'it-recon')`,
		taskID, tvChainID); err != nil {
		t.Fatalf("seed numeric height task: %v", err)
	}
}

// tvSeedPendingTicket inserts one pending_verify ticket with the recorded
// detection metadata (business type, detection interval, member set).
func tvSeedPendingTicket(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID string,
	id, txHash string, members []TxAggregateMember) {
	t.Helper()
	scope, err := HeightIdentityScope(chainID, 2, 5, BusinessWithdrawal)
	if err != nil {
		t.Fatalf("HeightIdentityScope: %v", err)
	}
	domain, err := PersistedEvidenceDomainJSONFor(
		VersionDomain{BlockNumber: 4, BlockHash: "0x4", EvidenceAt: time.Now().UTC().Add(-time.Minute)},
		&scope, BusinessWithdrawal, &DetectionInterval{Kind: ScopeHeight, From: 2, To: 5}, members)
	if err != nil {
		t.Fatalf("PersistedEvidenceDomainJSONFor: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO discrepancy (
		    discrepancy_id, category, business_key, content_hash, evidence_version_domain, state)
		VALUES ($1::uuid, 'missing', $2, $3, $4::jsonb, 'pending_verify')`,
		id, "tx_hash="+txHash, []byte{0x01, 0x02}, string(domain)); err != nil {
		t.Fatalf("seed pending ticket: %v", err)
	}
}

// tvTicketVerifyRequest is the bounded full-comparison request over the fakes.
func tvTicketVerifyRequest(taskID, discrepancyID string, chain ChainFactsReader) TicketVerifyRequest {
	return TicketVerifyRequest{
		TaskID:             taskID,
		DiscrepancyID:      discrepancyID,
		Actor:              "it-verify",
		Limits:             BudgetLimits{MaxConcurrency: 1, MaxSpanPerClaim: 4, MaxDuration: time.Minute, MaxPGRequests: 100, MaxRPCRequests: 20},
		FreshnessTolerance: time.Hour,
		Bounds:             TicketVerifyBounds{MaxPGRequests: 20, MaxItemAttempts: 3},
		Sources: ScanSources{
			Chain:  chain,
			PG:     tvFakePG{},
			Events: tvFakeEvents{},
		},
	}
}

// tvCompleteChainBundle is the complete bundle matching the seeded candidate.
func tvCompleteChainBundle(txHash string) ChainFactsBundle {
	return ChainFactsBundle{
		Status:     ChainFactsComplete,
		CapturedAt: time.Now().UTC(),
		Blocks:     []ChainFactBlock{{Number: 4, Hash: "0x4", Canonical: true, IndexedAt: time.Now().UTC().Add(-time.Minute)}},
		Logs: []ChainFactLog{{
			BlockNumber: 4, BlockHash: "0x4", TxHash: txHash, LogIndex: 0,
			Contract: "0xcontract", Topic0: "0xtopic0",
		}},
	}
}

func TestIntegrationTicketVerifyDiscardsWhenStateChangesDuringRead(t *testing.T) {
	ctx, pool, store := reconIT(t)
	taskID := uuid.NewString()
	tvSeedTask(t, ctx, pool, taskID)
	discrepancyID := uuid.NewString()
	txHash := "0x" + "ab"
	member := TxAggregateMember{BlockNumber: 4, BlockHash: "0x4", TxHash: txHash, LogIndex: 0,
		Contract: "0xcontract", Topic0: "0xtopic0"}
	tvSeedPendingTicket(t, ctx, pool, tvChainID, discrepancyID, txHash, []TxAggregateMember{member})

	reached := make(chan struct{})
	release := make(chan struct{})
	chain := &tvFakeChain{bundle: tvCompleteChainBundle(txHash), reached: reached, release: release}
	type outcome struct {
		result TicketVerifyResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := store.VerifyPendingDiscrepancy(ctx,
			tvTicketVerifyRequest(taskID, discrepancyID, chain))
		done <- outcome{result: result, err: err}
	}()

	// The evidence read has started; change the ticket state before the
	// persist CAS (the exact interleaving an operator close produces).
	<-reached
	if _, err := pool.Exec(ctx, `
		UPDATE discrepancy SET state = 'closed', close_basis = '{"range":"2..5"}'::jsonb
		WHERE discrepancy_id = $1::uuid`, discrepancyID); err != nil {
		t.Fatalf("change state during the read: %v", err)
	}
	close(release)

	got := <-done
	if got.err != nil {
		t.Fatalf("VerifyPendingDiscrepancy: %v", got.err)
	}
	if !got.result.Discarded {
		t.Fatalf("result = %+v, want discarded", got.result)
	}
	if got.result.State != DiscrepancyStateClosed {
		t.Fatalf("result state = %s, want closed", got.result.State)
	}
	if n := tvCount(t, ctx, pool,
		`SELECT count(*)::bigint FROM reverify WHERE discrepancy_id = $1::uuid`, discrepancyID); n != 0 {
		t.Fatalf("reverify rows after the discarded outcome = %d, want 0", n)
	}
	if n := tvCount(t, ctx, pool, `
		SELECT count(*)::bigint FROM recon_audit
		WHERE action = 'reverify' AND result = 'discarded' AND target->>'discrepancy_id' = $1`, discrepancyID); n != 1 {
		t.Fatalf("discard audit rows = %d, want 1", n)
	}
}

func TestIntegrationCloseReadsLatestReverifyUnderRowLock(t *testing.T) {
	ctx, pool, store := reconIT(t)
	taskID := uuid.NewString()
	tvSeedTask(t, ctx, pool, taskID)
	discrepancyID := uuid.NewString()
	tvSeedPendingTicket(t, ctx, pool, tvChainID, discrepancyID, "0x"+"cd", nil)

	// One fresh consistent row: the pre-change evidence a close could accept.
	if _, err := pool.Exec(ctx, `
		INSERT INTO reverify (discrepancy_id, verdict, evidence_ref, freshness_at)
		VALUES ($1::uuid, 'consistent', 'it:fresh', now())`, discrepancyID); err != nil {
		t.Fatalf("seed consistent reverify: %v", err)
	}

	// A competing writer holds the ticket row lock; a divergent row is
	// committed while the close waits for that lock. The close must read the
	// latest row under its own lock, not the pre-lock consistent row.
	holder, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin holder tx: %v", err)
	}
	var state string
	var reopen int64
	if err := holder.QueryRow(ctx,
		`SELECT state, reopen_count FROM discrepancy WHERE discrepancy_id = $1 FOR UPDATE`,
		discrepancyID).Scan(&state, &reopen); err != nil {
		t.Fatalf("holder lock: %v", err)
	}

	type outcome struct {
		result DiscrepancyTransitionResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := store.CloseDiscrepancy(ctx, DiscrepancyCloseRequest{
			DiscrepancyID:     discrepancyID,
			Actor:             "it-close",
			Reason:            "it",
			CloseBasis:        []byte(`{"range":"2..5"}`),
			ReverifyTolerance: time.Hour,
		})
		done <- outcome{result: result, err: err}
	}()

	// Commit the divergent row from under the lock and release it; the close
	// then acquires the lock and must refuse over the latest divergent row.
	if _, err := holder.Exec(ctx, `
		INSERT INTO reverify (discrepancy_id, verdict, evidence_ref, freshness_at)
		VALUES ($1::uuid, 'divergent', 'it:changed', now())`, discrepancyID); err != nil {
		t.Fatalf("insert divergent reverify: %v", err)
	}
	if err := holder.Commit(ctx); err != nil {
		t.Fatalf("commit holder tx: %v", err)
	}

	got := <-done
	if !errors.Is(got.err, ErrReverifyRequired) {
		t.Fatalf("close err = %v, want ErrReverifyRequired", got.err)
	}
	if n := tvCount(t, ctx, pool,
		`SELECT count(*)::bigint FROM discrepancy WHERE discrepancy_id = $1::uuid AND state = 'pending_verify'`,
		discrepancyID); n != 1 {
		t.Fatalf("ticket left pending_verify over the latest divergent row")
	}
	if n := tvCount(t, ctx, pool, `
		SELECT count(*)::bigint FROM recon_audit
		WHERE action = 'refuse' AND result = 'refused' AND target->>'discrepancy_id' = $1`, discrepancyID); n != 1 {
		t.Fatalf("close refusal audits = %d, want 1", n)
	}
}

// tvCount runs one scalar count.
func tvCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, query string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(ctx, query, args...).Scan(&n); err != nil {
		t.Fatalf("count query: %v", err)
	}
	return n
}
