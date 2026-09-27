//go:build integration

// txaggregate_integration_test.go is the T035 integration acceptance layer
// over the real 000016 schema (spec FR-007, Q2/Q5, SC-002):
//
//   - a transaction with several member logs becomes ONE ticket, with every
//     member log enumerated in discrepancy_occurrence evidence;
//   - a repeated scan of the same tx merges onto the original ticket and
//     appends member occurrences (no duplicate ticket, no lost members);
//   - a reorg replacement (same tx_hash, new block evidence) follows the Q5
//     invalidation path on the SAME ticket: state -> pending_verify plus an
//     appended occurrence and a reverify audit row, never a new ticket;
//   - the same tx hash under a different scope is a different identity: it
//     creates its own isolated ticket instead of merging.
//
// PostgreSQL comes from testcontainers via the shared reconIT helpers. When no
// Docker provider is healthy the package reports NOT RUN (t.Skip), never a pass.
package reconciliation

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// txAggChainFacts is one complete chain bundle whose block hash is a
// parameter: flipping it models the reorg replacement of the same transaction.
type txAggChainFacts struct {
	blockHash string
	indexedAt time.Time
}

func (f txAggChainFacts) Observe(ctx context.Context, q ChainFactsQuery) (ChainFactsBundle, error) {
	bundle := ChainFactsBundle{
		ChainID: q.ChainID, From: q.From, To: q.To,
		Source: ChainSourceLocalIndex, CapturedAt: f.indexedAt, Status: ChainFactsComplete,
	}
	for height := q.From; height <= q.To; height++ {
		bundle.Blocks = append(bundle.Blocks, ChainFactBlock{
			Number: height, Hash: f.blockHash, Canonical: true, IndexedAt: f.indexedAt,
		})
	}
	return bundle, nil
}

// txAggHash renders a deterministic lowercase 32-byte hex hash.
func txAggHash(seed byte) string {
	return "0x" + strings.Repeat(fmt.Sprintf("%02x", seed), 32)
}

// txAggCandidate builds one tx-aggregate missing candidate carrying its member
// logs.
func txAggCandidate(txHash, blockHash string, blockNumber int64, memberIndexes ...int64) ScanCandidate {
	members := make([]TxAggregateMember, 0, len(memberIndexes))
	for _, index := range memberIndexes {
		members = append(members, TxAggregateMember{
			BlockNumber: blockNumber,
			BlockHash:   blockHash,
			TxHash:      txHash,
			LogIndex:    index,
			Contract:    txAggHash(0xc1),
			Topic0:      txAggHash(0xd0),
		})
	}
	return ScanCandidate{
		BusinessType: BusinessWithdrawal,
		BusinessKey:  BusinessKey{Kind: BusinessKeyTxHash, Value: txHash},
		ChainFact: ChainFactRef{
			BlockNumber: blockNumber,
			BlockHash:   blockHash,
			TxHash:      txHash,
		},
		Members:     members,
		EvidenceRef: "scan:test:txaggregate tx_hash=" + txHash,
	}
}

// txAggTask seeds one running height task with the numeric chain id the
// adapters require.
func txAggTask(t *testing.T, ctx context.Context, pool *pgxpool.Pool, from, to int64) string {
	t.Helper()
	taskID := uuid.NewString()
	reconSeedHeightTask(t, ctx, pool, taskID, from, to, "running", "")
	if _, err := pool.Exec(ctx,
		`UPDATE recon_task SET scope_chain_id = '7' WHERE task_id = $1::uuid`, taskID); err != nil {
		t.Fatalf("set numeric chain id of %s: %v", taskID, err)
	}
	return taskID
}

// txAggRequest is the shared acceptance invocation shape.
func txAggRequest(taskID string, chain txAggChainFacts, candidates func(interval ScanInterval) []ScanCandidate) ScanOnceRequest {
	return ScanOnceRequest{
		TaskID: taskID, Owner: reconITOwner, LeaseTTL: time.Minute,
		Limits: BudgetLimits{
			MaxConcurrency: 1, MaxSpanPerClaim: 4, MaxDuration: time.Minute,
			MaxPGRequests: 50, MaxRPCRequests: 4,
		},
		FreshnessTolerance: time.Hour,
		Sources: ScanSources{
			Chain:  chain,
			PG:     fakeScanPGState{readAt: chain.indexedAt},
			Events: fakeScanEventState{capturedAt: chain.indexedAt},
		},
		Candidates: staticScanCandidates{candidates: candidates},
	}
}

// txAggTicketRow is one discrepancy row of a tx-aggregate ticket.
type txAggTicketRow struct {
	ID       string
	State    string
	HashHex  string
	Category string
}

// txAggTicketRows lists the ticket rows of one business key in creation order.
func txAggTicketRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, businessKey string) []txAggTicketRow {
	t.Helper()
	rows, err := pool.Query(ctx, `
		SELECT discrepancy_id::text, state, encode(content_hash, 'hex'), category
		FROM discrepancy WHERE business_key = $1
		ORDER BY created_at, discrepancy_id`, businessKey)
	if err != nil {
		t.Fatalf("list tickets %s: %v", businessKey, err)
	}
	defer rows.Close()
	var out []txAggTicketRow
	for rows.Next() {
		var row txAggTicketRow
		if err := rows.Scan(&row.ID, &row.State, &row.HashHex, &row.Category); err != nil {
			t.Fatalf("scan ticket row: %v", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("ticket rows %s: %v", businessKey, err)
	}
	return out
}

// txAggOccurrences lists the occurrence evidence refs of one ticket.
func txAggOccurrences(t *testing.T, ctx context.Context, pool *pgxpool.Pool, discrepancyID string) []string {
	t.Helper()
	rows, err := pool.Query(ctx, `
		SELECT evidence_ref FROM discrepancy_occurrence
		WHERE discrepancy_id = $1::uuid ORDER BY occurrence_id`, discrepancyID)
	if err != nil {
		t.Fatalf("list occurrences of %s: %v", discrepancyID, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var ref string
		if err := rows.Scan(&ref); err != nil {
			t.Fatalf("scan occurrence: %v", err)
		}
		out = append(out, ref)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("occurrence rows of %s: %v", discrepancyID, err)
	}
	return out
}

func TestIntegrationTxAggregateIdentityAcceptance(t *testing.T) {
	ctx, pool, store := reconIT(t)
	fixedAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)

	blockA := txAggHash(0xaa)
	blockB := txAggHash(0xbb)
	tx1 := txAggHash(0x11)
	tx2 := txAggHash(0x22)

	t.Run("one_tx_multi_log_is_one_ticket_with_full_member_evidence", func(t *testing.T) {
		taskID := txAggTask(t, ctx, pool, 700, 703)
		chain := txAggChainFacts{blockHash: blockA, indexedAt: fixedAt}
		candidates := func(interval ScanInterval) []ScanCandidate {
			return []ScanCandidate{
				txAggCandidate(tx1, blockA, interval.From.Height, 0, 1),
				txAggCandidate(tx2, blockA, interval.From.Height, 3),
			}
		}

		first, err := store.ScanOnce(ctx, txAggRequest(taskID, chain, candidates))
		if err != nil {
			t.Fatalf("first ScanOnce: %v", err)
		}
		if first.Tickets != 2 || first.Merged != 0 || first.Pending != 0 || first.Gaps != 0 {
			t.Fatalf("first scan tickets/merged/pending/gaps = %d/%d/%d/%d, want 2/0/0/0 (one ticket per tx, never per log)",
				first.Tickets, first.Merged, first.Pending, first.Gaps)
		}
		if first.Occurrences != 3 {
			t.Fatalf("first scan occurrences = %d, want 3 (two members of tx1 plus one of tx2)", first.Occurrences)
		}

		tickets1 := txAggTicketRows(t, ctx, pool, "tx_hash="+tx1)
		if len(tickets1) != 1 {
			t.Fatalf("tx1 tickets = %d, want exactly 1", len(tickets1))
		}
		if tickets1[0].Category != string(CategoryMissing) || tickets1[0].State != "open_claimable" {
			t.Fatalf("tx1 ticket = %s/%s, want missing/open_claimable", tickets1[0].Category, tickets1[0].State)
		}
		refs := txAggOccurrences(t, ctx, pool, tickets1[0].ID)
		if len(refs) != 2 {
			t.Fatalf("tx1 member occurrences = %d, want 2 (member evidence complete)", len(refs))
		}
		for _, index := range []string{"l=0", "l=1"} {
			found := false
			for _, ref := range refs {
				if strings.Contains(ref, index) {
					found = true
				}
			}
			if !found {
				t.Fatalf("tx1 occurrence refs %v lost member %s", refs, index)
			}
		}
		tickets2 := txAggTicketRows(t, ctx, pool, "tx_hash="+tx2)
		if len(tickets2) != 1 || len(txAggOccurrences(t, ctx, pool, tickets2[0].ID)) != 1 {
			t.Fatalf("tx2 tickets/occurrences = %d/%d, want 1/1", len(tickets2),
				len(txAggOccurrences(t, ctx, pool, tickets2[0].ID)))
		}
		tx2Refs := txAggOccurrences(t, ctx, pool, tickets2[0].ID)
		if tx2Refs[0] == refs[0] {
			t.Fatalf("different transactions share occurrence evidence: %q", tx2Refs[0])
		}

		// A second scan of the same scope merges onto the original tickets and
		// appends member evidence; member facts never split a ticket.
		secondTask := txAggTask(t, ctx, pool, 700, 703)
		second, err := store.ScanOnce(ctx, txAggRequest(secondTask, chain, candidates))
		if err != nil {
			t.Fatalf("second ScanOnce: %v", err)
		}
		if second.Tickets != 0 || second.Merged != 2 || second.Occurrences != 3 || second.Pending != 0 {
			t.Fatalf("second scan tickets/merged/occurrences/pending = %d/%d/%d/%d, want 0/2/3/0",
				second.Tickets, second.Merged, second.Occurrences, second.Pending)
		}
		repeat := txAggTicketRows(t, ctx, pool, "tx_hash="+tx1)
		if len(repeat) != 1 || repeat[0].ID != tickets1[0].ID || repeat[0].HashHex != tickets1[0].HashHex {
			t.Fatalf("tx1 ticket drifted across scans: %+v -> %+v", tickets1, repeat)
		}
		if refs := txAggOccurrences(t, ctx, pool, tickets1[0].ID); len(refs) != 4 {
			t.Fatalf("tx1 occurrences after the merge = %d, want 4", len(refs))
		}

		// A reorg replacement of the same tx (new block evidence) invalidates
		// the SAME ticket (Q5): pending_verify + appended occurrence + audit,
		// never a new ticket.
		replacement := txAggChainFacts{blockHash: blockB, indexedAt: fixedAt.Add(time.Second)}
		replacementCandidates := func(interval ScanInterval) []ScanCandidate {
			return []ScanCandidate{txAggCandidate(tx1, blockB, interval.From.Height, 0, 1)}
		}
		thirdTask := txAggTask(t, ctx, pool, 700, 703)
		third, err := store.ScanOnce(ctx, txAggRequest(thirdTask, replacement, replacementCandidates))
		if err != nil {
			t.Fatalf("replacement ScanOnce: %v", err)
		}
		if third.Tickets != 0 || third.Merged != 1 || third.Occurrences != 2 {
			t.Fatalf("replacement scan tickets/merged/occurrences = %d/%d/%d, want 0/1/2",
				third.Tickets, third.Merged, third.Occurrences)
		}
		after := txAggTicketRows(t, ctx, pool, "tx_hash="+tx1)
		if len(after) != 1 {
			t.Fatalf("reorg replacement created a new ticket: %d rows for tx1", len(after))
		}
		if after[0].ID != tickets1[0].ID {
			t.Fatalf("reorg replacement moved the ticket identity: %s -> %s", tickets1[0].ID, after[0].ID)
		}
		if after[0].State != "pending_verify" {
			t.Fatalf("ticket state after reorg replacement = %s, want pending_verify", after[0].State)
		}
		if after[0].HashHex == tickets1[0].HashHex {
			t.Fatalf("ticket evidence hash did not move to the replacement evidence")
		}
		if refs := txAggOccurrences(t, ctx, pool, tickets1[0].ID); len(refs) != 6 {
			t.Fatalf("occurrences after the replacement = %d, want 6 (evidence appended, no ticket split)", len(refs))
		}
		var audits int64
		if err := pool.QueryRow(ctx, `
			SELECT count(*)::bigint FROM recon_audit
			WHERE action = 'reverify' AND target->>'discrepancy_id' = $1`, tickets1[0].ID).Scan(&audits); err != nil {
			t.Fatalf("count invalidation audits: %v", err)
		}
		if audits != 1 {
			t.Fatalf("invalidation audit rows = %d, want 1", audits)
		}
	})

	t.Run("same_tx_in_another_scope_is_an_isolated_ticket", func(t *testing.T) {
		before := txAggTicketRows(t, ctx, pool, "tx_hash="+tx1)
		taskID := txAggTask(t, ctx, pool, 710, 713)
		chain := txAggChainFacts{blockHash: blockA, indexedAt: fixedAt.Add(2 * time.Second)}
		candidates := func(interval ScanInterval) []ScanCandidate {
			return []ScanCandidate{txAggCandidate(tx1, blockA, interval.From.Height, 0, 1)}
		}
		result, err := store.ScanOnce(ctx, txAggRequest(taskID, chain, candidates))
		if err != nil {
			t.Fatalf("isolated-scope ScanOnce: %v", err)
		}
		if result.Tickets != 1 || result.Merged != 0 {
			t.Fatalf("isolated scope tickets/merged = %d/%d, want 1/0 (a different scope is a different identity)",
				result.Tickets, result.Merged)
		}
		after := txAggTicketRows(t, ctx, pool, "tx_hash="+tx1)
		if len(after) != len(before)+1 {
			t.Fatalf("same tx under a different scope merged across scopes: %d -> %d rows", len(before), len(after))
		}
		if after[len(after)-1].ID == before[0].ID {
			t.Fatalf("the different-scope detection reused the original ticket id")
		}
	})
}
