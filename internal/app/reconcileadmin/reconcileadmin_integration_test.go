//go:build integration

// reconcileadmin_integration_test.go is the T033/T034/T036/T038 acceptance
// layer for the real reconciliation entry: the chain-first attribution
// enumeration and the budgeted scan path over the real 000016 schema plus the
// 002/003/004 business sources (quickstart §1/§3/§8; FR-002/004/005/006/009/
// 014/015/019/023):
//
//   - an attributed deposit/withdrawal chain fact without a PG business row is
//     one `missing` ticket with stable identity; a repeated scan merges and a
//     PG-present credit yields zero tickets;
//   - irrelevant assets/addresses/directions are metrics-only, never tickets;
//   - missing/unreadable attribution configuration and uncovered-or-orphaned
//     chain evidence become explicit gaps, never a "no difference" conclusion
//     and never a missing claim;
//   - a proven local missing coexists with ExternalCredit=unverified and no
//     funds-path row (007-012) moves and no disposition/reverify side effect is
//     written;
//   - the real `Run` entry accepts the new config keys only in their positive
//     form (WINDOW_MAX_PROBES), drives one complete budgeted scan, and reports
//     the conservative height-scope outcome honestly.
//
// PostgreSQL comes from testcontainers. When no Docker provider is healthy the
// package reports NOT RUN (t.Skip), never a pass.
package reconcileadmin

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/reconciliation"
	"github.com/xtianxx/txharbor/internal/txlifecycle"
)

const (
	recAdminChainID   = "31337"
	recAdminAsset     = "0x1111111111111111111111111111111111111111"
	recAdminWatch     = "0x2222222222222222222222222222222222222222"
	recAdminSender    = "0x3333333333333333333333333333333333333333"
	recAdminOutsider  = "0x4444444444444444444444444444444444444444"
	recAdminPrincipal = "deploy:recon-it"
)

// recAdminFundsTables are the 007-012 money-path tables a 014 scan must never
// write: no withdrawal request/intent/attempt/signature/payment row may move.
var recAdminFundsTables = []string{
	"withdrawal_requests",
	"withdrawal_authorizations",
	"withdrawal_authorization_scopes",
	"payment_intents",
	"execution_claims",
	"execution_steps",
	"tx_attempts",
	"tx_attempt_signings",
	"tx_send_attempts",
	"tx_reconciliations",
	"tx_receipts",
}

// recAdminPG boots a real PostgreSQL container, applies the embedded migrations
// and reports NOT RUN (never pass) when no Docker provider is available.
func recAdminPG(t *testing.T) (*pgxpool.Pool, string) {
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
	if err := db.MigrateUp(ctx, db.MigrateOptions{DSN: dsn, LockTimeout: 10 * time.Second, ConnectTimeout: 10 * time.Second}, io.Discard); err != nil {
		t.Fatalf("MigrateUp: %v", err)
	}
	pool, err := db.OpenPool(ctx, dsn, 10*time.Second)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool, dsn
}

// recAdminHash is the deterministic lowercase 32-byte hash of a test height.
func recAdminHash(height int64) string {
	return fmt.Sprintf("0x%064x", 0x7a0000+height)
}

// recAdminSeedChain seeds a complete durable index coverage for one chain:
// canonical blocks (with parent linkage) for the range, a tip block, the
// header checkpoint at the tip and a log checkpoint covering the range end.
func recAdminSeedChain(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID, from, to int64) {
	t.Helper()
	// The tip must sit far enough above the range end for the seeded confirm
	// threshold N=3 (confirmations = tip - to + 1).
	tip := to + 7
	parent := fmt.Sprintf("0x%064x", 0x7a0000+from-1)
	for height := from; height <= tip; height++ {
		if _, err := pool.Exec(ctx, `
			INSERT INTO chain_blocks (chain_id, number, hash, parent_hash, canonical, indexed_at)
			VALUES ($1, $2, $3, $4, true, now())`,
			chainID, height, recAdminHash(height), parent); err != nil {
			t.Fatalf("seed chain block %d on %d: %v", height, chainID, err)
		}
		parent = recAdminHash(height)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO indexer_checkpoint (chain_id, height, block_hash, start_height, updated_at)
		VALUES ($1, $2, $3, 0, now())`,
		chainID, tip, recAdminHash(tip)); err != nil {
		t.Fatalf("seed header checkpoint: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO log_checkpoint (chain_id, start_block, config_hash, next_block, updated_at)
		VALUES ($1, 0, $2, $3, now())`,
		chainID, strings.Repeat("cd", 32), to+1); err != nil {
		t.Fatalf("seed log checkpoint: %v", err)
	}
}

// recAdminSeedDepositConfig stores one 004 policy snapshot (the attribution
// source T033/T034 read).
func recAdminSeedDepositConfig(t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	chainID, version, startBlock int64, assets, watches string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO deposit_config_history
		    (chain_id, version_seq, config_hash, start_block, assets, watches, replay_from, operator, reason)
		VALUES ($1, $2, $3, $4, $5, $6, $4, 'it-seed', 'it-seed')`,
		chainID, version, strings.Repeat("ef", 32), startBlock, assets, watches); err != nil {
		t.Fatalf("seed deposit config v%d for %d: %v", version, chainID, err)
	}
}

// recAdminTopicAddr renders one lowercase 20-byte address as a zero-padded
// 32-byte indexed topic.
func recAdminTopicAddr(address string) string {
	return "0x" + strings.Repeat("0", 24) + strings.ToLower(strings.TrimPrefix(address, "0x"))
}

// recAdminInsertLog inserts one ERC-20 Transfer fact attributed by its
// topic1/topic2 direction addresses.
func recAdminInsertLog(t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	chainID, blockNumber int64, txHash string, logIndex int64, contract, from, to string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO erc20_transfer_logs
		    (chain_id, block_number, block_hash, tx_hash, log_index, contract, topic0, topic1, topic2, data, indexed_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, now())`,
		chainID, blockNumber, recAdminHash(blockNumber), txHash, logIndex, contract,
		"0x"+strings.Repeat("d1", 32), recAdminTopicAddr(from), recAdminTopicAddr(to),
		"0x"+strings.Repeat("0", 64)); err != nil {
		t.Fatalf("insert transfer log %s: %v", txHash, err)
	}
}

// recAdminSeedDepositObservation stores one 004 deposit-credit row (the
// authoritative PG presence control).
func recAdminSeedDepositObservation(t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	chainID, version, blockNumber int64, txHash string, logIndex int64, contract, sender, recipient string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO deposit_observations
		    (chain_id, block_hash, tx_hash, log_index, block_number, contract, sender, recipient, amount, version_seq, observed_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, '100', $9, now())`,
		chainID, recAdminHash(blockNumber), txHash, logIndex, blockNumber, contract, sender, recipient, version); err != nil {
		t.Fatalf("seed deposit observation %s: %v", txHash, err)
	}
}

// recAdminSeedHeightTask seeds one running height task.
func recAdminSeedHeightTask(t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	chainID string, from, to int64, businessTypes []string, upstream string) string {
	t.Helper()
	taskID := uuid.NewString()
	if _, err := pool.Exec(ctx, `
		INSERT INTO recon_task (task_id, scope_chain_id, scope_kind, scope_start, scope_end,
		    business_types, upstream_receipt_source, policy_refs, state, budget, created_by)
		VALUES ($1::uuid, $2, 'height', $3, $4, $5::text[], $6::jsonb, '{"confirm_threshold_n":3}'::jsonb,
		        'running', '{}'::jsonb, 'it-seed')`,
		taskID, chainID, from, to, businessTypes, upstream); err != nil {
		t.Fatalf("seed height task on %s: %v", chainID, err)
	}
	return taskID
}

// recAdminCount counts the rows of one table (read-only).
func recAdminCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, table string) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(ctx, `SELECT count(*)::bigint FROM `+table).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

// recAdminTaskAttempts counts the scan attempts of one task.
func recAdminTaskAttempts(t *testing.T, ctx context.Context, pool *pgxpool.Pool, taskID string) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(ctx,
		`SELECT count(*)::bigint FROM recon_scan_attempt WHERE task_id = $1::uuid`, taskID).Scan(&n); err != nil {
		t.Fatalf("count attempts of %s: %v", taskID, err)
	}
	return n
}

// recAdminFundsCounts snapshots the 007-012 money-path row counts.
func recAdminFundsCounts(t *testing.T, ctx context.Context, pool *pgxpool.Pool) map[string]int64 {
	t.Helper()
	out := make(map[string]int64, len(recAdminFundsTables))
	for _, table := range recAdminFundsTables {
		out[table] = recAdminCount(t, ctx, pool, table)
	}
	return out
}

// recAdminEventsComplete is a complete, empty event-delivery source: the event
// party is out of scope for the attribution acceptance and the real height-scope
// event adapter deliberately cannot support a consistent verdict (see the
// entry smoke test).
type recAdminEventsComplete struct{ capturedAt time.Time }

func (f recAdminEventsComplete) Observe(ctx context.Context, q reconciliation.EventStateQuery) (reconciliation.EventStateEvidence, error) {
	return reconciliation.EventStateEvidence{
		Scope: q.Scope, Interval: q.Interval, CapturedAt: f.capturedAt,
		Status: reconciliation.EventDeliveryComplete,
	}, nil
}

// recAdminScanOnce drives one real budgeted scan over the real adapters with
// the chain-first candidate enumeration of the command.
func recAdminScanOnce(t *testing.T, ctx context.Context, pool *pgxpool.Pool, taskID string,
	chainID string, businessTypes []reconciliation.BusinessType, signerSenders []string,
	capturedAt time.Time) (reconciliation.ScanOnceResult, error) {
	t.Helper()
	chainAdapter, err := reconciliation.NewChainFactsAdapter(pool, reconciliation.ChainFactsConfig{
		MaxSourceAge: time.Hour, MaxTipLag: 100, MaxRangeSpan: 4,
	})
	if err != nil {
		t.Fatalf("NewChainFactsAdapter: %v", err)
	}
	pgState, err := reconciliation.NewPGStateAdapter(pool, txlifecycle.NewStore(pool),
		reconciliation.PGStateOptions{FreshnessWindow: time.Hour})
	if err != nil {
		t.Fatalf("NewPGStateAdapter: %v", err)
	}
	store, err := reconciliation.NewStore(pool)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	return store.ScanOnce(ctx, reconciliation.ScanOnceRequest{
		TaskID: taskID, Owner: "it-worker", LeaseTTL: time.Minute,
		Limits: reconciliation.BudgetLimits{
			MaxConcurrency: 1, MaxSpanPerClaim: 4, MaxDuration: time.Minute,
			MaxPGRequests: 100, MaxRPCRequests: 20,
		},
		FreshnessTolerance: time.Hour,
		Sources: reconciliation.ScanSources{
			Chain:  chainAdapter,
			PG:     pgState,
			Events: recAdminEventsComplete{capturedAt: capturedAt},
		},
		Candidates: &reconcileScanCandidates{
			pool:          pool,
			chainID:       chainID,
			limit:         50,
			chain:         chainAdapter,
			businessTypes: businessTypes,
			signerSenders: signerSenders,
		},
	})
}

func TestIntegrationReconcileAdminAttributionAcceptance(t *testing.T) {
	ctx := context.Background()
	pool, _ := recAdminPG(t)
	fundsBefore := recAdminFundsCounts(t, ctx, pool)
	capturedAt := time.Now().UTC().Add(-time.Minute)

	t.Run("attributed_deposit_without_pg_row_is_missing_then_merges_then_absent", func(t *testing.T) {
		const chainID = int64(31337)
		recAdminSeedChain(t, ctx, pool, chainID, 100, 103)
		recAdminSeedDepositConfig(t, ctx, pool, chainID, 1, 100, recAdminAsset+":0", recAdminWatch+":0")
		txHash := "0x" + strings.Repeat("11", 32)
		recAdminInsertLog(t, ctx, pool, chainID, 100, txHash, 0, recAdminAsset, recAdminOutsider, recAdminWatch)

		taskID := recAdminSeedHeightTask(t, ctx, pool, "31337", 100, 103,
			[]string{"deposit"}, `{"deposit":{"source":"ledger","connected":true}}`)
		first, err := recAdminScanOnce(t, ctx, pool, taskID, "31337",
			[]reconciliation.BusinessType{reconciliation.BusinessDeposit}, nil, capturedAt)
		if err != nil {
			t.Fatalf("first ScanOnce: %v", err)
		}
		if first.Candidates != 1 || first.Tickets != 1 || first.Occurrences != 1 || first.Pending != 0 || first.Gaps != 0 {
			t.Fatalf("first scan candidates/tickets/occurrences/pending/gaps = %d/%d/%d/%d/%d, want 1/1/1/0/0",
				first.Candidates, first.Tickets, first.Occurrences, first.Pending, first.Gaps)
		}
		if first.Unattributed != 0 {
			t.Fatalf("attributed deposit counted as unattributed: %d", first.Unattributed)
		}
		var category, state string
		if err := pool.QueryRow(ctx, `
			SELECT category, state FROM discrepancy WHERE business_key = $1`, "tx_hash="+txHash).Scan(&category, &state); err != nil {
			t.Fatalf("read deposit ticket: %v", err)
		}
		if category != string(reconciliation.CategoryMissing) || state != "open_claimable" {
			t.Fatalf("deposit ticket = %s/%s, want missing/open_claimable", category, state)
		}

		// A repeated scan of the same scope merges onto the original ticket.
		secondTask := recAdminSeedHeightTask(t, ctx, pool, "31337", 100, 103,
			[]string{"deposit"}, `{"deposit":{"source":"ledger","connected":true}}`)
		second, err := recAdminScanOnce(t, ctx, pool, secondTask, "31337",
			[]reconciliation.BusinessType{reconciliation.BusinessDeposit}, nil, capturedAt)
		if err != nil {
			t.Fatalf("second ScanOnce: %v", err)
		}
		if second.Tickets != 0 || second.Merged != 1 || second.Occurrences != 1 {
			t.Fatalf("second scan tickets/merged/occurrences = %d/%d/%d, want 0/1/1 (stable identity)",
				second.Tickets, second.Merged, second.Occurrences)
		}
		if n := recAdminCount(t, ctx, pool, "discrepancy"); n != 1 {
			t.Fatalf("discrepancy rows = %d, want 1", n)
		}
		var occurrences int64
		if err := pool.QueryRow(ctx, `
			SELECT count(*)::bigint FROM discrepancy_occurrence o
			JOIN discrepancy d ON d.discrepancy_id = o.discrepancy_id
			WHERE d.business_key = $1`, "tx_hash="+txHash).Scan(&occurrences); err != nil {
			t.Fatalf("count deposit occurrences: %v", err)
		}
		if occurrences != 2 {
			t.Fatalf("deposit occurrences = %d, want 2", occurrences)
		}

		// The PG presence control: once the authoritative 004 credit row
		// exists, the same chain fact produces zero candidates and zero new
		// tickets.
		recAdminSeedDepositObservation(t, ctx, pool, chainID, 1, 100, txHash, 0, recAdminAsset, recAdminOutsider, recAdminWatch)
		thirdTask := recAdminSeedHeightTask(t, ctx, pool, "31337", 100, 103,
			[]string{"deposit"}, `{"deposit":{"source":"ledger","connected":true}}`)
		third, err := recAdminScanOnce(t, ctx, pool, thirdTask, "31337",
			[]reconciliation.BusinessType{reconciliation.BusinessDeposit}, nil, capturedAt)
		if err != nil {
			t.Fatalf("third ScanOnce: %v", err)
		}
		if third.Candidates != 0 || third.Tickets != 0 || third.Merged != 0 {
			t.Fatalf("PG-present scan candidates/tickets/merged = %d/%d/%d, want 0/0/0",
				third.Candidates, third.Tickets, third.Merged)
		}
		if n := recAdminCount(t, ctx, pool, "discrepancy"); n != 1 {
			t.Fatalf("discrepancy rows after the PG-present scan = %d, want 1", n)
		}
	})

	t.Run("attributed_withdrawal_without_pg_row_is_missing", func(t *testing.T) {
		const chainID = int64(31338)
		recAdminSeedChain(t, ctx, pool, chainID, 100, 103)
		recAdminSeedDepositConfig(t, ctx, pool, chainID, 1, 100, recAdminAsset+":0", recAdminWatch+":0")
		txHash := "0x" + strings.Repeat("22", 32)
		recAdminInsertLog(t, ctx, pool, chainID, 101, txHash, 0, recAdminAsset, recAdminSender, recAdminOutsider)

		taskID := recAdminSeedHeightTask(t, ctx, pool, "31338", 100, 103,
			[]string{"withdrawal"}, `{"withdrawal":{"source":"ledger","connected":true}}`)
		result, err := recAdminScanOnce(t, ctx, pool, taskID, "31338",
			[]reconciliation.BusinessType{reconciliation.BusinessWithdrawal}, []string{recAdminSender}, capturedAt)
		if err != nil {
			t.Fatalf("ScanOnce: %v", err)
		}
		if result.Candidates != 1 || result.Tickets != 1 || result.Pending != 0 {
			t.Fatalf("withdrawal scan candidates/tickets/pending = %d/%d/%d, want 1/1/0",
				result.Candidates, result.Tickets, result.Pending)
		}
		var category string
		if err := pool.QueryRow(ctx, `
			SELECT category FROM discrepancy WHERE business_key = $1`, "tx_hash="+txHash).Scan(&category); err != nil {
			t.Fatalf("read withdrawal ticket: %v", err)
		}
		if category != string(reconciliation.CategoryMissing) {
			t.Fatalf("withdrawal ticket category = %s, want missing", category)
		}
	})

	t.Run("irrelevant_asset_address_or_direction_is_metrics_only", func(t *testing.T) {
		const chainID = int64(31339)
		recAdminSeedChain(t, ctx, pool, chainID, 100, 103)
		recAdminSeedDepositConfig(t, ctx, pool, chainID, 1, 100, recAdminAsset+":0", recAdminWatch+":0")
		// Asset not in the allowlist.
		assetTx := "0x" + strings.Repeat("33", 32)
		recAdminInsertLog(t, ctx, pool, chainID, 100, assetTx, 0, recAdminOutsider, recAdminSender, recAdminWatch)
		// Neither a project sender nor a project recipient.
		addressTx := "0x" + strings.Repeat("44", 32)
		recAdminInsertLog(t, ctx, pool, chainID, 101, addressTx, 0, recAdminAsset, recAdminOutsider, recAdminOutsider)
		// Deposit direction with a non-watched recipient (and no project
		// sender): neither direction attributes it.
		directionTx := "0x" + strings.Repeat("55", 32)
		recAdminInsertLog(t, ctx, pool, chainID, 102, directionTx, 0, recAdminAsset, recAdminOutsider, recAdminOutsider)

		before := recAdminCount(t, ctx, pool, "discrepancy")
		taskID := recAdminSeedHeightTask(t, ctx, pool, "31339", 100, 103,
			[]string{"deposit", "withdrawal"}, `{"deposit":{"source":"ledger","connected":true},"withdrawal":{"source":"ledger","connected":true}}`)
		result, err := recAdminScanOnce(t, ctx, pool, taskID, "31339",
			[]reconciliation.BusinessType{reconciliation.BusinessDeposit, reconciliation.BusinessWithdrawal},
			[]string{recAdminSender}, capturedAt)
		if err != nil {
			t.Fatalf("ScanOnce: %v", err)
		}
		if result.Candidates != 0 || result.Tickets != 0 {
			t.Fatalf("unattributed facts candidates/tickets = %d/%d, want 0/0 (metrics-only, never a missing claim)",
				result.Candidates, result.Tickets)
		}
		if result.Unattributed != 3 {
			t.Fatalf("unattributed = %d, want 3 (each decided log once)", result.Unattributed)
		}
		if n := recAdminCount(t, ctx, pool, "discrepancy"); n != before {
			t.Fatalf("discrepancy rows moved on unattributed facts: %d -> %d", before, n)
		}
	})

	t.Run("missing_or_provisional_attribution_stays_a_gap", func(t *testing.T) {
		// No 004 policy history at all: both table sources are unknown and no
		// chain-first candidate may be emitted.
		const noConfigChain = int64(31340)
		recAdminSeedChain(t, ctx, pool, noConfigChain, 100, 103)
		noConfigTx := "0x" + strings.Repeat("66", 32)
		recAdminInsertLog(t, ctx, pool, noConfigChain, 100, noConfigTx, 0, recAdminAsset, recAdminOutsider, recAdminWatch)
		taskID := recAdminSeedHeightTask(t, ctx, pool, "31340", 100, 103,
			[]string{"deposit", "withdrawal"}, `{"deposit":{"source":"ledger","connected":true},"withdrawal":{"source":"ledger","connected":true}}`)
		result, err := recAdminScanOnce(t, ctx, pool, taskID, "31340",
			[]reconciliation.BusinessType{reconciliation.BusinessDeposit, reconciliation.BusinessWithdrawal},
			[]string{recAdminSender}, capturedAt)
		if err != nil {
			t.Fatalf("no-config ScanOnce: %v", err)
		}
		if result.Candidates != 0 || result.Tickets != 0 {
			t.Fatalf("missing config candidates/tickets = %d/%d, want 0/0 (undecidable is never a missing claim)",
				result.Candidates, result.Tickets)
		}
		if result.Gaps == 0 {
			t.Fatalf("missing config produced no gap")
		}
		if n := recAdminCount(t, ctx, pool, "recon_gap"); n == 0 {
			t.Fatalf("missing config produced no recon_gap row")
		}

		// Provisional attribution: the asset allowlist is readable but the
		// deposit watch set is blank. The deposit pass cannot decide; the
		// withdrawal pass decides the log is not a project withdrawal, and
		// neither invents a missing claim.
		const blankWatchChain = int64(31341)
		recAdminSeedChain(t, ctx, pool, blankWatchChain, 100, 103)
		recAdminSeedDepositConfig(t, ctx, pool, blankWatchChain, 1, 100, recAdminAsset+":0", "")
		blankTx := "0x" + strings.Repeat("77", 32)
		recAdminInsertLog(t, ctx, pool, blankWatchChain, 100, blankTx, 0, recAdminAsset, recAdminOutsider, recAdminWatch)
		blankTask := recAdminSeedHeightTask(t, ctx, pool, "31341", 100, 103,
			[]string{"deposit", "withdrawal"}, `{"deposit":{"source":"ledger","connected":true},"withdrawal":{"source":"ledger","connected":true}}`)
		blank, err := recAdminScanOnce(t, ctx, pool, blankTask, "31341",
			[]reconciliation.BusinessType{reconciliation.BusinessDeposit, reconciliation.BusinessWithdrawal},
			[]string{recAdminSender}, capturedAt)
		if err != nil {
			t.Fatalf("blank-watch ScanOnce: %v", err)
		}
		if blank.Candidates != 0 || blank.Tickets != 0 {
			t.Fatalf("blank watch set candidates/tickets = %d/%d, want 0/0", blank.Candidates, blank.Tickets)
		}
		if blank.Gaps == 0 {
			t.Fatalf("blank watch set produced no explicit gap")
		}
		if blank.Unattributed != 1 {
			t.Fatalf("blank watch set unattributed = %d, want 1 (the withdrawal pass decided it)", blank.Unattributed)
		}
	})

	t.Run("uncovered_or_orphaned_chain_evidence_is_a_gap", func(t *testing.T) {
		const chainID = int64(31342)
		recAdminSeedChain(t, ctx, pool, chainID, 100, 103)
		recAdminSeedDepositConfig(t, ctx, pool, chainID, 1, 100, recAdminAsset+":0", recAdminWatch+":0")
		txHash := "0x" + strings.Repeat("88", 32)
		recAdminInsertLog(t, ctx, pool, chainID, 100, txHash, 0, recAdminAsset, recAdminOutsider, recAdminWatch)

		// The durable log stream does not cover the interval: an incomplete
		// discovery set is never read as "no project facts".
		if _, err := pool.Exec(ctx, `UPDATE log_checkpoint SET next_block = 100 WHERE chain_id = $1`, chainID); err != nil {
			t.Fatalf("cover log stream short: %v", err)
		}
		taskID := recAdminSeedHeightTask(t, ctx, pool, "31342", 100, 103,
			[]string{"deposit"}, `{"deposit":{"source":"ledger","connected":true}}`)
		result, err := recAdminScanOnce(t, ctx, pool, taskID, "31342",
			[]reconciliation.BusinessType{reconciliation.BusinessDeposit}, nil, capturedAt)
		if err != nil {
			t.Fatalf("uncovered ScanOnce: %v", err)
		}
		if result.Candidates != 0 || result.Tickets != 0 || result.Pending != 1 || result.Gaps != 1 {
			t.Fatalf("uncovered stream candidates/tickets/pending/gaps = %d/%d/%d/%d, want 0/0/1/1",
				result.Candidates, result.Tickets, result.Pending, result.Gaps)
		}
		if got, ok := recAdminPointer(t, ctx, pool, taskID); !ok || got != 103 {
			t.Fatalf("uncovered scan pointer = %d (present %v), want 103 (results persisted with a gap)", got, ok)
		}
		if _, err := pool.Exec(ctx, `UPDATE log_checkpoint SET next_block = 104 WHERE chain_id = $1`, chainID); err != nil {
			t.Fatalf("restore log coverage: %v", err)
		}

		// An orphaned (non-canonical) block in the interval refuses the
		// discovery set the same way (FR-017).
		if _, err := pool.Exec(ctx,
			`UPDATE chain_blocks SET canonical = false WHERE chain_id = $1 AND number = 102`, chainID); err != nil {
			t.Fatalf("orphan block 102: %v", err)
		}
		orphanTask := recAdminSeedHeightTask(t, ctx, pool, "31342", 100, 103,
			[]string{"deposit"}, `{"deposit":{"source":"ledger","connected":true}}`)
		orphan, err := recAdminScanOnce(t, ctx, pool, orphanTask, "31342",
			[]reconciliation.BusinessType{reconciliation.BusinessDeposit}, nil, capturedAt)
		if err != nil {
			t.Fatalf("orphaned ScanOnce: %v", err)
		}
		if orphan.Candidates != 0 || orphan.Tickets != 0 || orphan.Pending != 1 || orphan.Gaps != 1 {
			t.Fatalf("orphaned evidence candidates/tickets/pending/gaps = %d/%d/%d/%d, want 0/0/1/1",
				orphan.Candidates, orphan.Tickets, orphan.Pending, orphan.Gaps)
		}
	})

	t.Run("proven_missing_coexists_with_unverified_external_credit", func(t *testing.T) {
		// The exact three-way shape a scan mints for a proven local missing:
		// a present chain fact, a definitive PG absence and a connected (but
		// receipt-less) upstream source. The ticket is minted; the external
		// credit dimension stays unverified; no full-consistency claim exists.
		scope, err := reconciliation.HeightIdentityScope("31338", 100, 103, reconciliation.BusinessWithdrawal)
		if err != nil {
			t.Fatalf("HeightIdentityScope: %v", err)
		}
		evidenceAt := time.Now().UTC().Add(-time.Minute)
		verdict := reconciliation.Classify(reconciliation.Observation{
			Scope:        scope,
			BusinessKey:  reconciliation.BusinessKey{Kind: reconciliation.BusinessKeyTxHash, Value: "0x" + strings.Repeat("22", 32)},
			BusinessType: reconciliation.BusinessWithdrawal,
			Chain:        reconciliation.PartyObservation{Status: reconciliation.PartyPresent, Content: []byte(`{"block":101}`)},
			PG:           reconciliation.PartyObservation{Status: reconciliation.PartyAbsent, Content: []byte(`{"absent":true}`)},
			Event:        reconciliation.PartyObservation{Status: reconciliation.PartyAbsent, Content: []byte(`{"absent":true}`)},
			Coverage: reconciliation.Coverage{
				ScanComplete: true, EvidenceAt: evidenceAt, Now: evidenceAt.Add(time.Second),
				FreshnessTolerance: time.Minute,
			},
			Upstream: reconciliation.UpstreamReceiptSource{Source: "ledger", Connected: true, Available: true},
			Version: reconciliation.VersionDomain{
				BlockNumber: 101, BlockHash: recAdminHash(101), EvidenceAt: evidenceAt,
			},
		})
		if !verdict.Ticket || verdict.Category != reconciliation.CategoryMissing {
			t.Fatalf("verdict = ticket %v category %q, want a missing ticket", verdict.Ticket, verdict.Category)
		}
		if verdict.ExternalCredit != reconciliation.ExternalCreditUnverified {
			t.Fatalf("external credit = %q, want %q (never a claim without a receipt)", verdict.ExternalCredit, reconciliation.ExternalCreditUnverified)
		}
		if verdict.FullyConsistent() {
			t.Fatalf("a proven local missing with unverified external credit reported fully consistent")
		}

		// No scan/AI path may have created a disposition or a consistent
		// reverify verdict, and the funds tables must not have moved.
		if n := recAdminCount(t, ctx, pool, "disposition"); n != 0 {
			t.Fatalf("disposition rows = %d, want 0 (014 scans are alert-only)", n)
		}
		if n := recAdminCount(t, ctx, pool, "reverify"); n != 0 {
			t.Fatalf("reverify rows = %d, want 0 (no auto-repair off a scan)", n)
		}
	})

	fundsAfter := recAdminFundsCounts(t, ctx, pool)
	for _, table := range recAdminFundsTables {
		if fundsAfter[table] != fundsBefore[table] {
			t.Fatalf("014 scan wrote the funds table %s: rows %d -> %d; no withdrawal intent/signature/payment side effect is permitted (FR-014/015)",
				table, fundsBefore[table], fundsAfter[table])
		}
	}
}

// recAdminPointer reads the height-typed persisted pointer of one task.
func recAdminPointer(t *testing.T, ctx context.Context, pool *pgxpool.Pool, taskID string) (int64, bool) {
	t.Helper()
	var persisted *int64
	err := pool.QueryRow(ctx, `
		SELECT result_persisted_through FROM recon_checkpoint
		WHERE task_id = $1::uuid ORDER BY seq DESC LIMIT 1`, taskID).Scan(&persisted)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false
	}
	if err != nil {
		t.Fatalf("read pointer of %s: %v", taskID, err)
	}
	if persisted == nil {
		return 0, false
	}
	return *persisted, true
}

// recAdminEnv is a fully valid reconcile command environment.
func recAdminEnv(dsn string, signerSenders []string, windowProbes int) map[string]string {
	env := map[string]string{
		config.EnvPGDSN:                 dsn,
		config.EnvRPCURL:                "http://127.0.0.1:1",
		config.EnvChainID:               "31337",
		config.EnvStartHeight:           "0",
		config.EnvLogStartHeight:        "0",
		config.EnvLogContracts:          recAdminAsset,
		config.EnvDepositStartHeight:    "0",
		config.EnvDepositContracts:      recAdminAsset,
		config.EnvDepositWatchAddresses: recAdminWatch,
		config.EnvConfirmationDepth:     "10",

		config.EnvReconPrincipal:          recAdminPrincipal,
		config.EnvReconConcurrency:        "1",
		config.EnvReconMaxSpanPerClaim:    "4",
		config.EnvReconMaxDuration:        "1m",
		config.EnvReconMaxPGRequests:      "100",
		config.EnvReconMaxRPCRequests:     "20",
		config.EnvReconLeaseTTL:           "1m",
		config.EnvReconFreshnessTolerance: "1h",
		config.EnvReconMaxTipLag:          "100",
		config.EnvReconMaxCandidates:      "50",
		config.EnvReconMaxEventRows:       "100",
		config.EnvReconSettleLimit:        "2",
	}
	if windowProbes > 0 {
		env[config.EnvReconWindowMaxProbes] = fmt.Sprintf("%d", windowProbes)
	}
	if len(signerSenders) > 0 {
		env[config.EnvSignerSenders] = strings.Join(signerSenders, ",")
	}
	return env
}

// recAdminRun drives the real command entry in-process.
func recAdminRun(ctx context.Context, env map[string]string, args ...string) (int, string, string) {
	var stdout, stderr bytes.Buffer
	code := Run(ctx, args, Deps{
		Getenv: func(key string) (string, bool) { value, ok := env[key]; return value, ok },
		Stdout: &stdout,
		Stderr: &stderr,
	})
	return code, stdout.String(), stderr.String()
}

// recAdminField extracts one `name=value` field of the command output.
func recAdminField(t *testing.T, out, name string) string {
	t.Helper()
	for _, field := range strings.Fields(out) {
		if value, ok := strings.CutPrefix(field, name+"="); ok {
			return value
		}
	}
	t.Fatalf("stdout %q has no %s= field", out, name)
	return ""
}

// recAdminGrant inserts one exact-scope permission row for the test principal.
func recAdminGrant(t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	permission reconciliation.Permission, scope reconciliation.AuthScope) {
	t.Helper()
	if err := scope.Validate(); err != nil {
		t.Fatalf("grant scope invalid: %v", err)
	}
	canonical, err := scope.CanonicalJSON()
	if err != nil {
		t.Fatalf("grant scope canonical json: %v", err)
	}
	digest, err := scope.Digest()
	if err != nil {
		t.Fatalf("grant scope digest: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO recon_permission (principal, action, scope, scope_hash, granted_by)
		VALUES ($1, $2, $3::jsonb, $4, 'it-grantor')`,
		recAdminPrincipal, string(permission), string(canonical), digest); err != nil {
		t.Fatalf("grant %s: %v", permission, err)
	}
}

func TestIntegrationReconcileAdminEntrySmoke(t *testing.T) {
	ctx := context.Background()
	pool, dsn := recAdminPG(t)

	t.Run("start_resume_scan_completes_a_budgeted_path", func(t *testing.T) {
		const chainID = int64(31350)
		recAdminSeedChain(t, ctx, pool, chainID, 100, 103)
		recAdminSeedDepositConfig(t, ctx, pool, chainID, 1, 100, recAdminAsset+":0", recAdminWatch+":0")
		txHash := "0x" + strings.Repeat("aa", 32)
		recAdminInsertLog(t, ctx, pool, chainID, 100, txHash, 0, recAdminAsset, recAdminOutsider, recAdminWatch)

		from, to := int64(100), int64(103)
		recAdminGrant(t, ctx, pool, reconciliation.PermissionScanManage, reconciliation.AuthScope{
			ChainID: "31350", Kind: reconciliation.ScopeHeight,
			BusinessTypes: []reconciliation.BusinessType{reconciliation.BusinessDeposit},
			RangeStart:    &from, RangeEnd: &to,
		})
		env := recAdminEnv(dsn, nil, 8)
		taskID := uuid.NewString()

		code, stdout, stderr := recAdminRun(ctx, env, "start",
			"--task-id", taskID, "--chain-id", "31350", "--scope-kind", "height",
			"--from", "100", "--to", "103", "--business-types", "deposit",
			"--confirm-threshold-n", "3", "--upstream-receipts", "deposit=ledger:connected")
		if code != 0 {
			t.Fatalf("start exit = %d, stderr=%q", code, stderr)
		}
		if recAdminField(t, stdout, "state") != "created" {
			t.Fatalf("start stdout = %q, want state=created", stdout)
		}
		var policyRefs string
		if err := pool.QueryRow(ctx,
			`SELECT policy_refs::text FROM recon_task WHERE task_id = $1::uuid`, taskID).Scan(&policyRefs); err != nil {
			t.Fatalf("read policy_refs: %v", err)
		}
		if !strings.Contains(policyRefs, `"confirm_threshold_n": 3`) {
			t.Fatalf("policy_refs = %q, want the frozen confirm threshold 3", policyRefs)
		}

		if code, _, stderr = recAdminRun(ctx, env, "resume", "--task-id", taskID, "--reason", "it"); code != 0 {
			t.Fatalf("resume exit = %d, stderr=%q", code, stderr)
		}
		code, stdout, stderr = recAdminRun(ctx, env, "scan", "--task-id", taskID)
		if code != 0 {
			t.Fatalf("scan exit = %d, stderr=%q stdout=%q", code, stderr, stdout)
		}
		if got := recAdminField(t, stdout, "stop"); got != "scope_exhausted" {
			t.Fatalf("scan stop = %q, want scope_exhausted (stdout %q)", got, stdout)
		}
		if got := recAdminField(t, stdout, "candidates"); got != "1" {
			t.Fatalf("scan candidates = %q, want 1 (the attributed deposit fact)", got)
		}
		if got := recAdminField(t, stdout, "tickets"); got != "0" {
			t.Fatalf("scan tickets = %q, want 0: the real height-scope event evidence cannot prove completeness, so the attributed fact stays pending (FR-004), never a false ticket", got)
		}
		if got := recAdminField(t, stdout, "pending"); got != "1" {
			t.Fatalf("scan pending = %q, want 1", got)
		}
		if got := recAdminField(t, stdout, "gaps"); got != "1" {
			t.Fatalf("scan gaps = %q, want 1 (the uncovered conclusion is visible)", got)
		}
		if got := recAdminField(t, stdout, "unattributed"); got != "0" {
			t.Fatalf("scan unattributed = %q, want 0", got)
		}
		if got := recAdminField(t, stdout, "persisted_through"); got != "103" {
			t.Fatalf("scan persisted_through = %q, want 103", got)
		}
		if !strings.Contains(stdout, "budget_pg=") || !strings.Contains(stdout, "budget_rpc=") {
			t.Fatalf("scan stdout %q lacks the budget usage report", stdout)
		}
		if n := recAdminCount(t, ctx, pool, "discrepancy"); n != 0 {
			t.Fatalf("discrepancy rows = %d, want 0 (pending evidence never mints a ticket)", n)
		}
		if n := recAdminCount(t, ctx, pool, "recon_scan_attempt"); n != 1 {
			t.Fatalf("scan attempts = %d, want 1", n)
		}
	})

	t.Run("window_max_probes_is_refused_by_name_only_when_missing", func(t *testing.T) {
		const chainID = int64(31351)
		recAdminSeedChain(t, ctx, pool, chainID, 100, 103)
		base := time.Now().UTC().Truncate(time.Second)
		timeTaskID := uuid.NewString()
		if _, err := pool.Exec(ctx, `
			INSERT INTO recon_task (task_id, scope_chain_id, scope_kind, scope_start_at, scope_end_at,
			    business_types, upstream_receipt_source, policy_refs, state, budget, created_by)
			VALUES ($1::uuid, '31351', 'time', $2, $3, ARRAY['withdrawal']::text[],
			        '{}'::jsonb, '{}'::jsonb, 'running', '{}'::jsonb, 'it-seed')`,
			timeTaskID, base, base.Add(4*time.Second)); err != nil {
			t.Fatalf("seed time task: %v", err)
		}
		recAdminGrant(t, ctx, pool, reconciliation.PermissionScanManage, reconciliation.AuthScope{
			ChainID:       "31351",
			Kind:          reconciliation.ScopeTime,
			BusinessTypes: []reconciliation.BusinessType{reconciliation.BusinessWithdrawal},
		})

		// Negative: without the key the time scan is refused by name before
		// any claim, RPC dial or resolver construction.
		code, _, stderr := recAdminRun(ctx, recAdminEnv(dsn, nil, 0), "scan", "--task-id", timeTaskID)
		if code == 0 {
			t.Fatalf("time scan without %s exited 0", config.EnvReconWindowMaxProbes)
		}
		if !strings.Contains(stderr, config.EnvReconWindowMaxProbes) {
			t.Fatalf("stderr %q does not name %s", stderr, config.EnvReconWindowMaxProbes)
		}
		if n := recAdminTaskAttempts(t, ctx, pool, timeTaskID); n != 0 {
			t.Fatalf("attempts after the refused time scan = %d, want 0", n)
		}

		// Positive: with the key the same invocation passes the probe gate
		// (the command then fails on the deliberately dead RPC endpoint, which
		// is out of scope here; the window resolver itself is covered by the
		// internal/reconciliation window integration tests).
		code, stdout, stderr := recAdminRun(ctx, recAdminEnv(dsn, nil, 8), "scan", "--task-id", timeTaskID)
		if code == 0 {
			t.Fatalf("dead-RPC time scan exited 0")
		}
		if strings.Contains(stderr, config.EnvReconWindowMaxProbes) {
			t.Fatalf("stderr %q still refuses %s although it is configured", stderr, config.EnvReconWindowMaxProbes)
		}
		if !strings.Contains(stdout, "scan task_id=") {
			t.Fatalf("stdout %q does not show the scan attempt output", stdout)
		}
		if n := recAdminTaskAttempts(t, ctx, pool, timeTaskID); n != 1 {
			t.Fatalf("attempts after the configured time scan = %d, want 1 (claimed before the RPC failure)", n)
		}
	})
}
