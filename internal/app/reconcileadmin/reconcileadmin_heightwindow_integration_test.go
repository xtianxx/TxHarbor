//go:build integration

// reconcileadmin_heightwindow_integration_test.go is the real-entry acceptance
// layer for the height interval -> event chain-time window chain (T036 reverse
// direction) and the CoverageClosed semantics of the 014 compare loop over the
// real 000015/000016 durable event tables:
//
//   - a real `reconcile-admin start -> resume -> scan` invocation with the real
//     three adapters (T013-T015), a real Anvil chain providing live block
//     headers, and a durable local index seeded with those real canonical block
//     identities detects a project deposit chain fact without its PG business
//     record as one `missing` ticket, and the same for a project withdrawal
//     fact; the blockless business-object event rows are seeded in the real
//     outbox_events / consumer_* tables and are attributed only through the
//     resolved chain-time window (OccurredFrom/To);
//   - matching authoritative PG records (004 credit rows / 011+012 execution
//     rows) mint no missing ticket for either direction; a PG-anchored
//     withdrawal candidate carries its own frozen-013 event aggregate identity
//     (request_id -> aggregate/withdrawal_request/<id>) as EventKey, so a
//     delivered+closed event of the same business object is a three-way match
//     with zero tickets, while a genuinely absent aggregate event row is the
//     one missing-event delivery case that mints exactly one request_id ticket;
//   - event evidence that is genuinely insufficient (an unclosed delivery in
//     the resolved window) stays pending with a visible gap, while an
//     unconnected upstream receipt declaration no longer masks an otherwise
//     fully proven local missing (ChainFactsBundle.CoverageClosed /
//     EventStateEvidence.CoverageClosed), with ExternalCredit staying
//     unverified;
//   - repeated scans keep the stable identity and never write anything outside
//     the 014-owned tables (row counts plus a full-row digest snapshot of the
//     007-012 money-path tables are unchanged around every scan);
//   - an oversized member set keeps every original member log individually
//     queryable in the occurrence evidence, and a gapped interval still refuses
//     `done`.
//
// Fixture boundary: this file seeds the real durable tables directly
// (chain_blocks / indexer_checkpoint / log_checkpoint / erc20_transfer_logs /
// outbox_events / consumer_inbox / consumer_versions / consumer_progress and
// the 004/007/011/012 fixture chain) because the indexer, publisher and
// consumer runtimes are upstream of 014 and their full pipelines are covered by
// their own suites. Every seeded row is a byte-faithful row of the real
// schema; nothing is injected into the 014 read paths themselves, and no event
// bundle is hand-built for the scan entry.
//
// PostgreSQL and Anvil come from testcontainers. When no Docker provider is
// healthy the package reports NOT RUN (t.Skip), never a pass.
package reconcileadmin

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/rpc"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/eth"
	"github.com/xtianxx/txharbor/internal/events"
	"github.com/xtianxx/txharbor/internal/reconciliation"
	"github.com/xtianxx/txharbor/internal/withdrawal"
)

const (
	recHdrScopeFrom = int64(2)
	recHdrScopeTo   = int64(5)
	recHdrConfirmN  = "3"
	// recHdrMemberCount is the oversized member set of one transaction: large
	// enough that the single aggregate evidence ref digests instead of
	// enumerating every member (TxAggregateEvidenceRef bound 512).
	recHdrMemberCount = 40
)

// recHdrEventRow is one outbox_events fixture row. A nil BlockNumber models a
// blockless business_object row that only the resolved OccurredFrom/To window
// can attribute (the T036 height->time path).
type recHdrEventRow struct {
	IdentityKind     string
	EventType        string
	AggregateType    string
	AggregateID      string
	AggregateVersion int64
	OccurredAt       time.Time
	PublishState     string
	PublishedAt      *time.Time
	BlockNumber      *int64
	BlockHash        *string
	TxHash           *string
	LogIndex         *int
}

// recHdrHeader is one live chain header fact.
type recHdrHeader struct {
	Hash       string
	ParentHash string
	Time       time.Time
}

// recHdrAnvil starts one real Anvil node (the foundry image compose.yaml
// already uses), mines a deterministic height ladder with spaced timestamps and
// returns the RPC URL plus the live headers by height.
func recHdrAnvil(t *testing.T) (string, map[int64]recHdrHeader) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "ghcr.io/foundry-rs/foundry:v1.8.1",
			Entrypoint:   []string{"anvil"},
			Cmd:          []string{"--host", "0.0.0.0", "--port", "8545", "--chain-id", "31337", "--silent"},
			ExposedPorts: []string{"8545/tcp"},
			WaitingFor:   wait.ForListeningPort("8545/tcp").WithStartupTimeout(60 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start anvil container: %v", err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })
	host, err := ctr.Host(ctx)
	if err != nil {
		t.Fatalf("anvil host: %v", err)
	}
	port, err := ctr.MappedPort(ctx, "8545/tcp")
	if err != nil {
		t.Fatalf("anvil mapped port: %v", err)
	}
	rpcURL := fmt.Sprintf("http://%s:%s", host, port.Port())

	// Readiness: a real header probe, not a fixed sleep.
	client, err := eth.Dial(ctx, rpcURL, 5*time.Second)
	if err != nil {
		t.Fatalf("dial anvil: %v", err)
	}
	defer client.Close()
	deadline := time.Now().Add(30 * time.Second)
	for {
		if _, err := client.HeaderByNumber(ctx, big.NewInt(0)); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("anvil did not answer header probes")
		}
		time.Sleep(100 * time.Millisecond)
	}

	// One deterministic ladder: heights 1..12 with 2s spacing, pinned ten
	// minutes into the past. anvil_mine alone would step the chain times
	// forward from "now" into the future, and a future chain time fails the
	// compare loop's freshness gate (evidence in the future is unproven),
	// masking the very verdicts these tests assert.
	ladderBase := time.Now().UTC().Add(-10 * time.Minute).Unix()
	recHdrSetTime(t, ctx, rpcURL, ladderBase)
	recHdrMine(t, ctx, rpcURL, 12, 2)
	headers := make(map[int64]recHdrHeader, 12)
	for height := int64(1); height <= 12; height++ {
		header, err := client.HeaderByNumber(ctx, big.NewInt(height))
		if err != nil || header == nil {
			t.Fatalf("header %d: %v", height, err)
		}
		headers[height] = recHdrHeader{
			Hash:       strings.ToLower(header.Hash().Hex()),
			ParentHash: strings.ToLower(header.ParentHash.Hex()),
			Time:       time.Unix(int64(header.Time), 0).UTC(),
		}
	}
	// The ladder must stay strictly in the past (and inside the configured
	// freshness tolerance), otherwise freshness conclusions change meaning.
	now := time.Now().UTC()
	for height := int64(1); height <= 12; height++ {
		if !headers[height].Time.Before(now) {
			t.Fatalf("chain time at height %d = %s is not in the past (now %s)",
				height, headers[height].Time, now)
		}
		if now.Sub(headers[height].Time) > 30*time.Minute {
			t.Fatalf("chain time at height %d = %s is too far in the past (now %s)",
				height, headers[height].Time, now)
		}
	}
	return rpcURL, headers
}

// recHdrSetTime pins the next anvil block timestamp base through the real RPC.
func recHdrSetTime(t *testing.T, ctx context.Context, rpcURL string, unixSeconds int64) {
	t.Helper()
	client, err := rpc.DialContext(ctx, rpcURL)
	if err != nil {
		t.Fatalf("dial anvil rpc: %v", err)
	}
	defer client.Close()
	var ignored any
	if err := client.CallContext(ctx, &ignored, "evm_setTime", fmt.Sprintf("0x%x", unixSeconds)); err != nil {
		t.Fatalf("evm_setTime: %v", err)
	}
}

// recHdrMine mines blocks with an explicit timestamp interval through the real
// Anvil RPC.
func recHdrMine(t *testing.T, ctx context.Context, rpcURL string, blocks, intervalSeconds int64) {
	t.Helper()
	client, err := rpc.DialContext(ctx, rpcURL)
	if err != nil {
		t.Fatalf("dial anvil rpc: %v", err)
	}
	defer client.Close()
	var ignored any
	if err := client.CallContext(ctx, &ignored, "anvil_mine",
		fmt.Sprintf("0x%x", blocks), fmt.Sprintf("0x%x", intervalSeconds)); err != nil {
		t.Fatalf("anvil_mine: %v", err)
	}
}

// recHdrSeedChain seeds the durable local index the way the real 002/003
// indexer leaves it: real canonical block rows (real hashes and parent linkage
// from the live chain), a header checkpoint above the scope end and a log
// checkpoint covering the range end. from..to+1 are seeded because the
// height->time resolver closes the right seam at the next height's chain time;
// to+2 carries the header checkpoint above the confirm threshold.
func recHdrSeedChain(t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	chainID int64, headers map[int64]recHdrHeader, from, to int64) {
	t.Helper()
	for height := from; height <= to+2; height++ {
		header, ok := headers[height]
		if !ok {
			t.Fatalf("no live header at height %d", height)
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO chain_blocks (chain_id, number, hash, parent_hash, canonical, indexed_at)
			VALUES ($1, $2, $3, $4, true, now() - interval '2 minutes')`,
			chainID, height, header.Hash, header.ParentHash); err != nil {
			t.Fatalf("seed chain block %d on %d: %v", height, chainID, err)
		}
	}
	tip := to + 2
	if _, err := pool.Exec(ctx, `
		INSERT INTO indexer_checkpoint (chain_id, height, block_hash, start_height, updated_at)
		VALUES ($1, $2, $3, 0, now() - interval '1 minute')`,
		chainID, tip, headers[tip].Hash); err != nil {
		t.Fatalf("seed header checkpoint: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO log_checkpoint (chain_id, start_block, config_hash, next_block, updated_at)
		VALUES ($1, 0, $2, $3, now() - interval '1 minute')`,
		chainID, strings.Repeat("cd", 32), to+1); err != nil {
		t.Fatalf("seed log checkpoint: %v", err)
	}
}

// recHdrInsertLog inserts one ERC-20 Transfer fact carrying the real canonical
// block identity of the live chain (the candidate ref is matched against
// chain_blocks, so a synthetic block hash would be read as an orphan).
func recHdrInsertLog(t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	chainID, blockNumber int64, blockHash, txHash string, logIndex int64, contract, from, to string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO erc20_transfer_logs
		    (chain_id, block_number, block_hash, tx_hash, log_index, contract, topic0, topic1, topic2, data, indexed_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, now())`,
		chainID, blockNumber, blockHash, txHash, logIndex, contract,
		"0x"+strings.Repeat("d1", 32), recAdminTopicAddr(from), recAdminTopicAddr(to),
		"0x"+strings.Repeat("0", 64)); err != nil {
		t.Fatalf("insert transfer log %s: %v", txHash, err)
	}
}

// recHdrSeedDepositObservation stores one 004 deposit-credit row with the real
// canonical block identity (the authoritative PG presence control).
func recHdrSeedDepositObservation(t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	chainID, version, blockNumber int64, blockHash, txHash string, logIndex int64, contract, sender, recipient string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO deposit_observations
		    (chain_id, block_hash, tx_hash, log_index, block_number, contract, sender, recipient, amount, version_seq, observed_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, '100', $9, now() - interval '1 minute')`,
		chainID, blockHash, txHash, logIndex, blockNumber, contract, sender, recipient, version); err != nil {
		t.Fatalf("seed deposit observation %s: %v", txHash, err)
	}
}

// recHdrSeedOutbox inserts one real outbox_events row and returns its event id.
func recHdrSeedOutbox(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, row recHdrEventRow) uuid.UUID {
	t.Helper()
	eventID := uuid.New()
	if _, err := pool.Exec(ctx, `
		INSERT INTO outbox_events (
		    event_id, identity_kind, event_type, schema_version, aggregate_type, aggregate_id,
		    aggregate_version, payload, payload_hash, occurred_at, chain_id, block_number,
		    block_hash, tx_hash, log_index, publish_state, published_at, created_at)
		VALUES ($1::uuid, $2, $3, 1, $4, $5, $6, '{"it":"height-window-fixture"}'::jsonb, $7, $8,
		        $9, $10, $11, $12, $13, $14, $15, $8)`,
		eventID, row.IdentityKind, row.EventType, row.AggregateType, row.AggregateID,
		row.AggregateVersion, "0x"+strings.Repeat("ab", 32), row.OccurredAt.UTC(),
		chainID, row.BlockNumber, row.BlockHash, row.TxHash, row.LogIndex,
		row.PublishState, row.PublishedAt); err != nil {
		t.Fatalf("seed outbox event %s: %v", row.EventType, err)
	}
	return eventID
}

// recHdrApplyEvent records the reference consumer's durable application of one
// event (inbox + version watermark + progress) — the exact T4 transaction
// shape — so a published event reaches a terminal consumer outcome.
func recHdrApplyEvent(t *testing.T, ctx context.Context, pool *pgxpool.Pool, eventID uuid.UUID,
	aggregateType, aggregateID string, aggregateVersion int64, topic string, partition int, offset int64) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO consumer_inbox (
		    consumer_name, event_id, aggregate_type, aggregate_id, aggregate_version,
		    topic, partition, "offset", applied_at)
		VALUES ($1, $2::uuid, $3, $4, $5, $6, $7, $8, now() - interval '1 minute')`,
		events.RefConsumerName, eventID, aggregateType, aggregateID, aggregateVersion,
		topic, partition, offset); err != nil {
		t.Fatalf("seed consumer_inbox: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO consumer_versions (consumer_name, aggregate_type, aggregate_id, max_version, updated_at)
		VALUES ($1, $2, $3, $4, now() - interval '1 minute')`,
		events.RefConsumerName, aggregateType, aggregateID, aggregateVersion); err != nil {
		t.Fatalf("seed consumer_versions: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO consumer_progress (consumer_name, topic, partition, next_offset, updated_at)
		VALUES ($1, $2, $3, $4, now() - interval '1 minute')
		ON CONFLICT (consumer_name, topic, partition) DO UPDATE SET next_offset = EXCLUDED.next_offset`,
		events.RefConsumerName, topic, partition, offset+1); err != nil {
		t.Fatalf("seed consumer_progress: %v", err)
	}
}

// recHdrGrant inserts one exact-scope permission row for the test principal;
// re-granting the same scope is idempotent (recon_permission PK dedup).
func recHdrGrant(t *testing.T, ctx context.Context, pool *pgxpool.Pool,
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
		VALUES ($1, $2, $3::jsonb, $4, 'it-grantor')
		ON CONFLICT (principal, action, scope_hash) DO NOTHING`,
		recAdminPrincipal, string(permission), string(canonical), digest); err != nil {
		t.Fatalf("grant %s: %v", permission, err)
	}
}

// recHdrTaskAndGrant mints one task id and the matching scan-management grant
// of the test principal over the exact scope; the real `start` command creates
// the task row.
func recHdrTaskAndGrant(t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	chainID string, from, to int64, businessTypes []string) string {
	t.Helper()
	taskID := uuid.NewString()
	types := make([]reconciliation.BusinessType, 0, len(businessTypes))
	for _, name := range businessTypes {
		types = append(types, reconciliation.BusinessType(name))
	}
	rangeStart, rangeEnd := from, to
	recHdrGrant(t, ctx, pool, reconciliation.PermissionScanManage, reconciliation.AuthScope{
		ChainID: chainID, Kind: reconciliation.ScopeHeight,
		BusinessTypes: types, RangeStart: &rangeStart, RangeEnd: &rangeEnd,
	})
	return taskID
}

// recHdrScanAndAssertFunds snapshots the 007-012 money-path tables, drives the
// real start -> resume -> scan entry and proves the scan wrote none of them.
func recHdrScanAndAssertFunds(t *testing.T, ctx context.Context, pool *pgxpool.Pool, env map[string]string,
	taskID, chainID string, from, to int64, businessTypes, upstream string) string {
	t.Helper()
	before := recHdrFundsSnapshot(t, ctx, pool)
	out, _ := recHdrStartResumeScan(t, ctx, env, taskID, chainID, from, to, businessTypes, upstream)
	after := recHdrFundsSnapshot(t, ctx, pool)
	for table, snapshot := range before {
		if after[table] != snapshot {
			t.Fatalf("014 scan wrote the money-path table %s: %s -> %s", table, snapshot, after[table])
		}
	}
	return out
}

// recHdrStartResumeScan drives the real command entry: start (created),
// resume (running), scan (the real budgeted compare loop).
func recHdrStartResumeScan(t *testing.T, ctx context.Context, env map[string]string, taskID, chainID string,
	from, to int64, businessTypes, upstream string) (string, string) {
	t.Helper()
	code, _, stderr := recAdminRun(ctx, env, "start",
		"--task-id", taskID, "--chain-id", chainID, "--scope-kind", "height",
		"--from", fmt.Sprint(from), "--to", fmt.Sprint(to),
		"--business-types", businessTypes, "--confirm-threshold-n", recHdrConfirmN,
		"--upstream-receipts", upstream)
	if code != 0 {
		t.Fatalf("start exit = %d, stderr=%q", code, stderr)
	}
	code, _, stderr = recAdminRun(ctx, env, "resume", "--task-id", taskID, "--reason", "it")
	if code != 0 {
		t.Fatalf("resume exit = %d, stderr=%q", code, stderr)
	}
	code, scanOut, scanErr := recAdminRun(ctx, env, "scan", "--task-id", taskID)
	if code != 0 {
		t.Fatalf("scan exit = %d, stderr=%q stdout=%q", code, scanErr, scanOut)
	}
	return scanOut, scanErr
}

// recHdrFundsSnapshot captures every 007-012 money-path table as a row count
// plus a full-row digest, so not just row counts but every key state column is
// proven untouched across a scan.
func recHdrFundsSnapshot(t *testing.T, ctx context.Context, pool *pgxpool.Pool) map[string]string {
	t.Helper()
	out := make(map[string]string, len(recAdminFundsTables))
	for _, table := range recAdminFundsTables {
		var digest, count string
		if err := pool.QueryRow(ctx, `
			SELECT COALESCE(md5(string_agg(row_text, E'\n' ORDER BY row_text)), 'empty'),
			       count(*)::text
			FROM (SELECT t::text AS row_text FROM `+table+` t) rows`).Scan(&digest, &count); err != nil {
			t.Fatalf("snapshot %s: %v", table, err)
		}
		out[table] = count + ":" + digest
	}
	return out
}

// recHdrFields parses the `name=value` fields of one command output.
func recHdrFields(t *testing.T, out string) map[string]string {
	t.Helper()
	fields := make(map[string]string)
	for _, field := range strings.Fields(out) {
		if name, value, ok := strings.Cut(field, "="); ok {
			fields[name] = value
		}
	}
	return fields
}

// recHdrAssertFields checks the requested `name=value` fields of one command
// output and reports the full stdout on a mismatch.
func recHdrAssertFields(t *testing.T, out string, want map[string]string) map[string]string {
	t.Helper()
	fields := recHdrFields(t, out)
	for name, expected := range want {
		if fields[name] != expected {
			t.Fatalf("scan %s = %q, want %q (stdout %q)", name, fields[name], expected, out)
		}
	}
	return fields
}

// recHdrTicketID reads the discrepancy id of one business key and fails when it
// is missing.
func recHdrTicketID(t *testing.T, ctx context.Context, pool *pgxpool.Pool, businessKey string) string {
	t.Helper()
	var id string
	if err := pool.QueryRow(ctx, `SELECT discrepancy_id::text FROM discrepancy WHERE business_key = $1`,
		businessKey).Scan(&id); err != nil {
		t.Fatalf("read ticket %s: %v", businessKey, err)
	}
	return id
}

// recHdrBusinessKeyCount counts the tickets of one persisted business key.
func recHdrBusinessKeyCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, businessKey string) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(ctx,
		`SELECT count(*)::bigint FROM discrepancy WHERE business_key = $1`, businessKey).Scan(&n); err != nil {
		t.Fatalf("count tickets %s: %v", businessKey, err)
	}
	return n
}

// recHdrOccurrenceRefs lists the occurrence evidence refs of one ticket.
func recHdrOccurrenceRefs(t *testing.T, ctx context.Context, pool *pgxpool.Pool, discrepancyID string) []string {
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

func TestIntegrationReconcileAdminHeightWindowAcceptance(t *testing.T) {
	ctx := context.Background()
	pool, dsn := recAdminPG(t)
	rpcURL, headers := recHdrAnvil(t)
	env := recAdminEnv(dsn, []string{recAdminSender}, 8)
	env[config.EnvRPCURL] = rpcURL
	store, err := reconciliation.NewStore(pool)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}

	// ---- 1. deposit chain fact without a PG row -> missing ticket ----------
	t.Run("deposit_chain_fact_without_pg_row_mints_missing_ticket_then_merges", func(t *testing.T) {
		chainID := int64(32301)
		recHdrSeedChain(t, ctx, pool, chainID, headers, recHdrScopeFrom, recHdrScopeTo)
		recAdminSeedDepositConfig(t, ctx, pool, chainID, 1, 0, recAdminAsset+":0", recAdminWatch+":0")
		txHash := "0x" + strings.Repeat("a1", 32)

		// A blockless deposit business-object event in the claimed interval:
		// only the resolved chain-time window can attribute it (fixture
		// boundary: the durable tables are seeded; no 014 read path is faked).
		occurredAt := headers[3].Time
		eventID := recHdrSeedOutbox(t, ctx, pool, chainID, recHdrEventRow{
			IdentityKind: "business_object", EventType: events.EventTypeDepositObservationStatusChanged,
			AggregateType: "deposit_observation", AggregateID: "obs-" + fmt.Sprint(chainID),
			AggregateVersion: 1, OccurredAt: occurredAt, PublishState: "published",
			PublishedAt: &occurredAt,
		})
		recHdrApplyEvent(t, ctx, pool, eventID, "deposit_observation", "obs-"+fmt.Sprint(chainID), 1, "txharbor.events.v1", 0, 41)

		recHdrInsertLog(t, ctx, pool, chainID, 3, headers[3].Hash, txHash, 0, recAdminAsset, recAdminOutsider, recAdminWatch)

		// First real entry run: start -> resume -> scan.
		firstTask := recHdrTaskAndGrant(t, ctx, pool, fmt.Sprint(chainID), recHdrScopeFrom, recHdrScopeTo, []string{"deposit"})
		out := recHdrScanAndAssertFunds(t, ctx, pool, env, firstTask, fmt.Sprint(chainID),
			recHdrScopeFrom, recHdrScopeTo, "deposit", "deposit=ledger:connected")
		t.Logf("first scan output: %s", out)
		recHdrAssertFields(t, out, map[string]string{
			"stop": "scope_exhausted", "candidates": "1", "tickets": "1", "merged": "0",
			"pending": "0", "gaps": "0", "window_unmappable": "0", "window_invalidated": "0",
		})
		var category, state string
		if err := pool.QueryRow(ctx, `
			SELECT category, state FROM discrepancy WHERE business_key = $1`,
			"tx_hash="+txHash).Scan(&category, &state); err != nil {
			t.Fatalf("read deposit ticket: %v", err)
		}
		if category != string(reconciliation.CategoryMissing) || state != "open_claimable" {
			t.Fatalf("deposit ticket = %s/%s, want missing/open_claimable", category, state)
		}

		// A repeated scan of the same scope (second task, same chain/range)
		// keeps the stable identity: no second ticket, occurrence appended.
		secondTask := recHdrTaskAndGrant(t, ctx, pool, fmt.Sprint(chainID), recHdrScopeFrom, recHdrScopeTo, []string{"deposit"})
		repeat := recHdrScanAndAssertFunds(t, ctx, pool, env, secondTask, fmt.Sprint(chainID),
			recHdrScopeFrom, recHdrScopeTo, "deposit", "deposit=ledger:connected")
		t.Logf("repeat scan output: %s", repeat)
		recHdrAssertFields(t, repeat, map[string]string{
			"tickets": "0", "merged": "1", "occurrences": "1", "pending": "0", "gaps": "0",
		})
		if n := recHdrBusinessKeyCount(t, ctx, pool, "tx_hash="+txHash); n != 1 {
			t.Fatalf("tickets for %s = %d, want exactly 1 (stable identity)", txHash, n)
		}
		if refs := recHdrOccurrenceRefs(t, ctx, pool, recHdrTicketID(t, ctx, pool, "tx_hash="+txHash)); len(refs) != 2 {
			t.Fatalf("occurrences after the repeat scan = %d, want 2 (evidence appended)", len(refs))
		}
	})

	// ---- 2. withdrawal chain fact without a PG row -> missing ticket -------
	t.Run("withdrawal_chain_fact_without_pg_row_mints_missing_ticket", func(t *testing.T) {
		chainID := int64(32302)
		recHdrSeedChain(t, ctx, pool, chainID, headers, recHdrScopeFrom, recHdrScopeTo)
		recAdminSeedDepositConfig(t, ctx, pool, chainID, 1, 0, recAdminAsset+":0", recAdminWatch+":0")
		txHash := "0x" + strings.Repeat("b2", 32)
		recHdrInsertLog(t, ctx, pool, chainID, 4, headers[4].Hash, txHash, 0, recAdminAsset, recAdminSender, recAdminOutsider)

		// Fixture boundary: the request business object of this withdrawal
		// exists and its own delivered event is applied (terminal, inside the
		// window), but the chain transaction has no authoritative 011/012
		// execution row. The chain-first candidate stays tx_hash keyed (no
		// event aggregate) and the PG absence still mints exactly one missing
		// ticket: the delivered request event can not mask it.
		requestID := recHdrSeedWithdrawalRequestOnly(t, ctx, pool, chainID)
		occurredAt := headers[4].Time
		eventID := recHdrSeedOutbox(t, ctx, pool, chainID, recHdrEventRow{
			IdentityKind: "business_object", EventType: events.EventTypeWithdrawalRequestReceived,
			AggregateType: withdrawal.RequestAggregateType, AggregateID: requestID,
			AggregateVersion: 1, OccurredAt: occurredAt, PublishState: "published",
			PublishedAt: &occurredAt,
		})
		recHdrApplyEvent(t, ctx, pool, eventID, withdrawal.RequestAggregateType, requestID, 1, "txharbor.events.v1", 1, 7)

		taskID := recHdrTaskAndGrant(t, ctx, pool, fmt.Sprint(chainID), recHdrScopeFrom, recHdrScopeTo, []string{"withdrawal"})
		out := recHdrScanAndAssertFunds(t, ctx, pool, env, taskID, fmt.Sprint(chainID),
			recHdrScopeFrom, recHdrScopeTo, "withdrawal", "withdrawal=ledger:connected")
		t.Logf("scan output: %s", out)
		recHdrAssertFields(t, out, map[string]string{
			"stop": "scope_exhausted", "candidates": "1", "tickets": "1", "merged": "0",
			"pending": "0", "gaps": "0", "window_unmappable": "0", "window_invalidated": "0",
		})
		var category string
		if err := pool.QueryRow(ctx, `
			SELECT category FROM discrepancy WHERE business_key = $1`,
			"tx_hash="+txHash).Scan(&category); err != nil {
			t.Fatalf("read withdrawal ticket: %v", err)
		}
		if category != string(reconciliation.CategoryMissing) {
			t.Fatalf("withdrawal ticket category = %s, want missing", category)
		}
	})

	// ---- 3a. matching deposit PG row -> zero false positive ----------------
	t.Run("matching_deposit_pg_row_mints_no_ticket", func(t *testing.T) {
		chainID := int64(32303)
		recHdrSeedChain(t, ctx, pool, chainID, headers, recHdrScopeFrom, recHdrScopeTo)
		recAdminSeedDepositConfig(t, ctx, pool, chainID, 1, 0, recAdminAsset+":0", recAdminWatch+":0")
		txHash := "0x" + strings.Repeat("c3", 32)
		recHdrInsertLog(t, ctx, pool, chainID, 3, headers[3].Hash, txHash, 0, recAdminAsset, recAdminOutsider, recAdminWatch)
		recHdrSeedDepositObservation(t, ctx, pool, chainID, 1, 3, headers[3].Hash, txHash, 0, recAdminAsset, recAdminOutsider, recAdminWatch)

		before := recAdminCount(t, ctx, pool, "discrepancy")
		taskID := recHdrTaskAndGrant(t, ctx, pool, fmt.Sprint(chainID), recHdrScopeFrom, recHdrScopeTo, []string{"deposit"})
		out := recHdrScanAndAssertFunds(t, ctx, pool, env, taskID, fmt.Sprint(chainID),
			recHdrScopeFrom, recHdrScopeTo, "deposit", "deposit=ledger:connected")
		t.Logf("scan output: %s", out)
		recHdrAssertFields(t, out, map[string]string{
			"stop": "scope_exhausted", "candidates": "0", "tickets": "0", "merged": "0",
			"pending": "0", "gaps": "0",
		})
		if after := recAdminCount(t, ctx, pool, "discrepancy"); after != before {
			t.Fatalf("discrepancy rows = %d, want unchanged %d (zero false positive)", after, before)
		}
	})

	// ---- 3b. matching withdrawal 011/012 execution chain -> zero tickets ---
	t.Run("matching_withdrawal_execution_chain_mints_no_missing_candidate", func(t *testing.T) {
		chainID := int64(32304)
		recHdrSeedChain(t, ctx, pool, chainID, headers, recHdrScopeFrom, recHdrScopeTo)
		recAdminSeedDepositConfig(t, ctx, pool, chainID, 1, 0, recAdminAsset+":0", recAdminWatch+":0")
		txHash := "0x" + strings.Repeat("d4", 32)
		recHdrInsertLog(t, ctx, pool, chainID, 4, headers[4].Hash, txHash, 0, recAdminAsset, recAdminSender, recAdminOutsider)

		// The authoritative 011/012 execution chain of the same transaction,
		// following the existing txlifecycle fixture shape. The signing row
		// binds the tx hash to the attempt and is exactly the chain-first
		// reverse check's anchor, so the missing candidate is suppressed.
		recHdrSeedWithdrawalChain(t, ctx, pool, chainID, txHash, 4, headers[4].Hash, false)

		before := recAdminCount(t, ctx, pool, "discrepancy")
		taskID := recHdrTaskAndGrant(t, ctx, pool, fmt.Sprint(chainID), recHdrScopeFrom, recHdrScopeTo, []string{"withdrawal"})
		out := recHdrScanAndAssertFunds(t, ctx, pool, env, taskID, fmt.Sprint(chainID),
			recHdrScopeFrom, recHdrScopeTo, "withdrawal", "withdrawal=ledger:connected")
		t.Logf("scan output: %s", out)
		recHdrAssertFields(t, out, map[string]string{
			"candidates": "0", "tickets": "0", "merged": "0", "pending": "0", "gaps": "0",
		})
		if after := recAdminCount(t, ctx, pool, "discrepancy"); after != before {
			t.Fatalf("discrepancy rows = %d, want unchanged %d (zero false positive)", after, before)
		}
	})

	// ---- 3c. receipted withdrawal + delivered event -> consistent, no ticket
	//
	// The PG-anchored candidate carries the candidate's own event aggregate
	// identity as EventKey (request_id -> aggregate/withdrawal_request/<id>), so
	// a delivered+closed withdrawal.request.received event of the same object
	// matches: chain + PG + event all present, no ticket, no pending, no gap.
	t.Run("receipted_withdrawal_with_delivered_event_is_consistent_no_ticket", func(t *testing.T) {
		chainID := int64(32309)
		recHdrSeedChain(t, ctx, pool, chainID, headers, recHdrScopeFrom, recHdrScopeTo)
		recAdminSeedDepositConfig(t, ctx, pool, chainID, 1, 0, recAdminAsset+":0", recAdminWatch+":0")
		txHash := "0x" + strings.Repeat("f6", 32)
		recHdrInsertLog(t, ctx, pool, chainID, 4, headers[4].Hash, txHash, 0, recAdminAsset, recAdminSender, recAdminOutsider)
		requestID, _ := recHdrSeedWithdrawalChain(t, ctx, pool, chainID, txHash, 4, headers[4].Hash, true)

		// Fixture boundary: the request's own delivered event, applied by the
		// reference consumer (terminal), inside the resolved window and keyed
		// by the frozen 013 aggregate convention.
		occurredAt := headers[3].Time
		eventID := recHdrSeedOutbox(t, ctx, pool, chainID, recHdrEventRow{
			IdentityKind: "business_object", EventType: events.EventTypeWithdrawalRequestReceived,
			AggregateType: withdrawal.RequestAggregateType, AggregateID: requestID,
			AggregateVersion: 1, OccurredAt: occurredAt, PublishState: "published",
			PublishedAt: &occurredAt,
		})
		recHdrApplyEvent(t, ctx, pool, eventID, withdrawal.RequestAggregateType, requestID, 1, "txharbor.events.v1", 0, 77)

		before := recAdminCount(t, ctx, pool, "discrepancy")
		taskID := recHdrTaskAndGrant(t, ctx, pool, fmt.Sprint(chainID), recHdrScopeFrom, recHdrScopeTo, []string{"withdrawal"})
		out := recHdrScanAndAssertFunds(t, ctx, pool, env, taskID, fmt.Sprint(chainID),
			recHdrScopeFrom, recHdrScopeTo, "withdrawal", "withdrawal=ledger:connected")
		t.Logf("scan output: %s", out)
		recHdrAssertFields(t, out, map[string]string{
			"stop": "scope_exhausted", "candidates": "1", "tickets": "0", "merged": "0",
			"pending": "0", "absorbed": "0", "gaps": "0",
		})
		if n := recHdrBusinessKeyCount(t, ctx, pool, "request_id="+requestID); n != 0 {
			t.Fatalf("tickets for the receipted withdrawal with a delivered event = %d, want 0 (three-way match)", n)
		}
		if after := recAdminCount(t, ctx, pool, "discrepancy"); after != before {
			t.Fatalf("discrepancy rows = %d, want unchanged %d (zero false positive)", after, before)
		}
	})

	// ---- 3d. receipted withdrawal, aggregate event row actually missing ----
	//
	// The same chain/PG state with no withdrawal.request.received row is the
	// genuine missing-event-delivery case: the PG-anchored candidate stays
	// request_id keyed and mints exactly one missing ticket (never a tx_hash
	// candidate).
	t.Run("receipted_withdrawal_without_event_row_mints_missing", func(t *testing.T) {
		chainID := int64(32310)
		recHdrSeedChain(t, ctx, pool, chainID, headers, recHdrScopeFrom, recHdrScopeTo)
		recAdminSeedDepositConfig(t, ctx, pool, chainID, 1, 0, recAdminAsset+":0", recAdminWatch+":0")
		txHash := "0x" + strings.Repeat("f7", 32)
		recHdrInsertLog(t, ctx, pool, chainID, 4, headers[4].Hash, txHash, 0, recAdminAsset, recAdminSender, recAdminOutsider)
		requestID, _ := recHdrSeedWithdrawalChain(t, ctx, pool, chainID, txHash, 4, headers[4].Hash, true)

		taskID := recHdrTaskAndGrant(t, ctx, pool, fmt.Sprint(chainID), recHdrScopeFrom, recHdrScopeTo, []string{"withdrawal"})
		out := recHdrScanAndAssertFunds(t, ctx, pool, env, taskID, fmt.Sprint(chainID),
			recHdrScopeFrom, recHdrScopeTo, "withdrawal", "withdrawal=ledger:connected")
		t.Logf("scan output: %s", out)
		recHdrAssertFields(t, out, map[string]string{
			"stop": "scope_exhausted", "candidates": "1", "tickets": "1", "merged": "0",
			"pending": "0", "gaps": "0",
		})
		if n := recHdrBusinessKeyCount(t, ctx, pool, "request_id="+requestID); n != 1 {
			t.Fatalf("tickets for the missing aggregate event = %d, want 1", n)
		}
		if n := recHdrBusinessKeyCount(t, ctx, pool, "tx_hash="+txHash); n != 0 {
			t.Fatalf("tx_hash missing tickets = %d, want 0 (the PG-anchored candidate owns the tx)", n)
		}
		var category string
		if err := pool.QueryRow(ctx, `
			SELECT category FROM discrepancy WHERE business_key = $1`,
			"request_id="+requestID).Scan(&category); err != nil {
			t.Fatalf("read receipted withdrawal ticket: %v", err)
		}
		if category != string(reconciliation.CategoryMissing) {
			t.Fatalf("receipted withdrawal ticket category = %s, want missing (the absent aggregate event)", category)
		}
	})

	// ---- 4a. unclosed event evidence -> pending + visible gap --------------
	t.Run("unclosed_event_evidence_stays_pending_with_visible_gap", func(t *testing.T) {
		chainID := int64(32305)
		recHdrSeedChain(t, ctx, pool, chainID, headers, recHdrScopeFrom, recHdrScopeTo)
		recAdminSeedDepositConfig(t, ctx, pool, chainID, 1, 0, recAdminAsset+":0", recAdminWatch+":0")
		txHash := "0x" + strings.Repeat("e5", 32)
		recHdrInsertLog(t, ctx, pool, chainID, 3, headers[3].Hash, txHash, 0, recAdminAsset, recAdminOutsider, recAdminWatch)

		// A published-but-unconsumed blockless event in the window. This is
		// also the positive attribution control for the resolved window: if the
		// blockless row were not attributed, the event bundle would close (the
		// row would be invisible), and the chain fact would mint a missing
		// ticket. Observing pending/gap here proves the row was read through
		// OccurredFrom/To; the resolved window itself is proven by
		// window_unmappable=0.
		occurredAt := headers[3].Time
		recHdrSeedOutbox(t, ctx, pool, chainID, recHdrEventRow{
			IdentityKind: "business_object", EventType: events.EventTypeDepositObservationStatusChanged,
			AggregateType: "deposit_observation", AggregateID: "obs-pending-" + fmt.Sprint(chainID),
			AggregateVersion: 1, OccurredAt: occurredAt, PublishState: "pending",
		})

		taskID := recHdrTaskAndGrant(t, ctx, pool, fmt.Sprint(chainID), recHdrScopeFrom, recHdrScopeTo, []string{"deposit"})
		out := recHdrScanAndAssertFunds(t, ctx, pool, env, taskID, fmt.Sprint(chainID),
			recHdrScopeFrom, recHdrScopeTo, "deposit", "deposit=ledger:connected")
		t.Logf("scan output: %s", out)
		recHdrAssertFields(t, out, map[string]string{
			"stop": "scope_exhausted", "candidates": "1", "tickets": "0", "pending": "1",
			"gaps": "1", "window_unmappable": "0", "window_invalidated": "0", "persisted_through": "5",
		})
		if n := recHdrBusinessKeyCount(t, ctx, pool, "tx_hash="+txHash); n != 0 {
			t.Fatalf("tickets = %d, want 0 (insufficient evidence never mints a ticket)", n)
		}
		if _, err := store.TransitionTask(ctx, reconciliation.TaskTransitionRequest{
			TaskID: taskID, To: reconciliation.TaskStateDone, Reason: "close over the gap", Actor: recAdminPrincipal,
		}); !errors.Is(err, reconciliation.ErrTaskNotComplete) {
			t.Fatalf("done over the gapped interval err = %v, want ErrTaskNotComplete", err)
		}
	})

	// ---- 4b. unconnected upstream does not mask the proven local missing ---
	t.Run("unconnected_upstream_does_not_mask_the_proven_missing", func(t *testing.T) {
		chainID := int64(32306)
		recHdrSeedChain(t, ctx, pool, chainID, headers, recHdrScopeFrom, recHdrScopeTo)
		recAdminSeedDepositConfig(t, ctx, pool, chainID, 1, 0, recAdminAsset+":0", recAdminWatch+":0")
		txHash := "0x" + strings.Repeat("aa", 32)
		recHdrInsertLog(t, ctx, pool, chainID, 3, headers[3].Hash, txHash, 0, recAdminAsset, recAdminOutsider, recAdminWatch)

		taskID := recHdrTaskAndGrant(t, ctx, pool, fmt.Sprint(chainID), recHdrScopeFrom, recHdrScopeTo, []string{"deposit"})
		out := recHdrScanAndAssertFunds(t, ctx, pool, env, taskID, fmt.Sprint(chainID),
			recHdrScopeFrom, recHdrScopeTo, "deposit", "deposit=ledger:unconnected")
		t.Logf("scan output: %s", out)
		recHdrAssertFields(t, out, map[string]string{
			"stop": "scope_exhausted", "candidates": "1", "tickets": "1", "merged": "0",
			"pending": "0", "gaps": "0",
		})
		var category string
		if err := pool.QueryRow(ctx, `
			SELECT category FROM discrepancy WHERE business_key = $1`,
			"tx_hash="+txHash).Scan(&category); err != nil {
			t.Fatalf("read ticket: %v", err)
		}
		if category != string(reconciliation.CategoryMissing) {
			t.Fatalf("ticket category = %s, want missing: the unconnected upstream must not mask the proven local missing", category)
		}

		// The classifier dimension stays honest on the same declaration: the
		// missing ticket exists while external credit is unverified and no
		// full-consistency claim is possible. (The entry-level proof above is
		// the ticket; this pins the ExternalCredit vocabulary.)
		scope, err := reconciliation.HeightIdentityScope(fmt.Sprint(chainID), recHdrScopeFrom, recHdrScopeTo, reconciliation.BusinessDeposit)
		if err != nil {
			t.Fatalf("HeightIdentityScope: %v", err)
		}
		evidenceAt := time.Now().UTC().Add(-time.Minute)
		verdict := reconciliation.Classify(reconciliation.Observation{
			Scope: scope, BusinessType: reconciliation.BusinessDeposit,
			BusinessKey: reconciliation.BusinessKey{Kind: reconciliation.BusinessKeyTxHash, Value: txHash},
			Chain:       reconciliation.PartyObservation{Status: reconciliation.PartyPresent, Content: []byte(`{"chain":"present"}`)},
			PG:          reconciliation.PartyObservation{Status: reconciliation.PartyAbsent, Content: []byte(`{"pg":"absent"}`)},
			Event:       reconciliation.PartyObservation{Status: reconciliation.PartyAbsent, Content: []byte(`{"event":"absent"}`)},
			Coverage: reconciliation.Coverage{
				ScanComplete: true, EvidenceAt: evidenceAt, Now: evidenceAt.Add(time.Second),
				FreshnessTolerance: time.Minute,
			},
			Upstream: reconciliation.UpstreamReceiptSource{Source: "ledger", Connected: false},
			Version: reconciliation.VersionDomain{
				BlockNumber: 3, BlockHash: headers[3].Hash, EvidenceAt: evidenceAt,
			},
		})
		if !verdict.Ticket || verdict.Category != reconciliation.CategoryMissing {
			t.Fatalf("verdict = ticket %v category %q, want a missing ticket", verdict.Ticket, verdict.Category)
		}
		if verdict.ExternalCredit != reconciliation.ExternalCreditUnverified {
			t.Fatalf("external credit = %q, want %q", verdict.ExternalCredit, reconciliation.ExternalCreditUnverified)
		}
		if verdict.FullyConsistent() {
			t.Fatalf("a proven local missing with unverified external credit reported fully consistent")
		}
	})

	// ---- 7a. candidate bound overflow -> explicit gap, never a silent subset
	t.Run("candidate_bound_overflow_stays_gapped_and_cannot_close", func(t *testing.T) {
		chainID := int64(32308)
		recHdrSeedChain(t, ctx, pool, chainID, headers, recHdrScopeFrom, recHdrScopeTo)
		recAdminSeedDepositConfig(t, ctx, pool, chainID, 1, 0, recAdminAsset+":0", recAdminWatch+":0")
		firstTx := "0x" + strings.Repeat("91", 32)
		secondTx := "0x" + strings.Repeat("92", 32)
		recHdrInsertLog(t, ctx, pool, chainID, 3, headers[3].Hash, firstTx, 0, recAdminAsset, recAdminOutsider, recAdminWatch)
		recHdrInsertLog(t, ctx, pool, chainID, 4, headers[4].Hash, secondTx, 0, recAdminAsset, recAdminOutsider, recAdminWatch)

		tight := make(map[string]string, len(env))
		for key, value := range env {
			tight[key] = value
		}
		tight[config.EnvReconMaxCandidates] = "1"

		taskID := recHdrTaskAndGrant(t, ctx, pool, fmt.Sprint(chainID), recHdrScopeFrom, recHdrScopeTo, []string{"deposit"})
		out := recHdrScanAndAssertFunds(t, ctx, pool, tight, taskID, fmt.Sprint(chainID),
			recHdrScopeFrom, recHdrScopeTo, "deposit", "deposit=ledger:connected")
		t.Logf("scan output: %s", out)
		recHdrAssertFields(t, out, map[string]string{
			"stop": "scope_exhausted", "candidates": "0", "tickets": "0",
			"pending": "1", "gaps": "1", "persisted_through": "5",
		})
		for _, txHash := range []string{firstTx, secondTx} {
			if n := recHdrBusinessKeyCount(t, ctx, pool, "tx_hash="+txHash); n != 0 {
				t.Fatalf("overflow minted %d ticket(s) for %s; a truncated discovery set must stay gapped", n, txHash)
			}
		}
		if _, err := store.TransitionTask(ctx, reconciliation.TaskTransitionRequest{
			TaskID: taskID, To: reconciliation.TaskStateDone, Reason: "close over the gap", Actor: recAdminPrincipal,
		}); !errors.Is(err, reconciliation.ErrTaskNotComplete) {
			t.Fatalf("done over the overflow gap err = %v, want ErrTaskNotComplete", err)
		}
	})

	// ---- 7b. oversized member set stays traceable; gapped interval cannot close
	t.Run("oversized_member_set_stays_traceable_and_gapped_interval_cannot_close", func(t *testing.T) {
		chainID := int64(32307)
		scopeFrom, scopeTo := int64(2), int64(7)
		recHdrSeedChain(t, ctx, pool, chainID, headers, scopeFrom, scopeTo)
		recAdminSeedDepositConfig(t, ctx, pool, chainID, 1, 0, recAdminAsset+":0", recAdminWatch+":0")

		// One transaction with recHdrMemberCount member logs inside the first
		// claimed interval [2..5].
		memberTx := "0x" + strings.Repeat("77", 32)
		members := make([]reconciliation.TxAggregateMember, 0, recHdrMemberCount)
		for index := int64(0); index < recHdrMemberCount; index++ {
			recHdrInsertLog(t, ctx, pool, chainID, 3, headers[3].Hash, memberTx, index, recAdminAsset, recAdminOutsider, recAdminWatch)
			members = append(members, reconciliation.TxAggregateMember{
				BlockNumber: 3, BlockHash: headers[3].Hash, TxHash: memberTx, LogIndex: index,
				Contract: recAdminAsset, Topic0: "0x" + strings.Repeat("d1", 32),
			})
		}

		// The unclosed blockless delivery sits in the second claimed interval
		// [6..7] only: its occurred_at is past the first interval's right seam
		// but inside the second interval's window.
		pendingAt := headers[7].Time
		recHdrSeedOutbox(t, ctx, pool, chainID, recHdrEventRow{
			IdentityKind: "business_object", EventType: events.EventTypeDepositObservationStatusChanged,
			AggregateType: "deposit_observation", AggregateID: "obs-overflow-" + fmt.Sprint(chainID),
			AggregateVersion: 1, OccurredAt: pendingAt, PublishState: "pending",
		})
		laterTx := "0x" + strings.Repeat("78", 32)
		recHdrInsertLog(t, ctx, pool, chainID, 6, headers[6].Hash, laterTx, 0, recAdminAsset, recAdminOutsider, recAdminWatch)

		taskID := recHdrTaskAndGrant(t, ctx, pool, fmt.Sprint(chainID), scopeFrom, scopeTo, []string{"deposit"})
		out := recHdrScanAndAssertFunds(t, ctx, pool, env, taskID, fmt.Sprint(chainID),
			scopeFrom, scopeTo, "deposit", "deposit=ledger:connected")
		t.Logf("scan output: %s", out)
		recHdrAssertFields(t, out, map[string]string{
			"stop": "scope_exhausted", "tickets": "1", "merged": "0", "pending": "1",
			"gaps": "1", "persisted_through": "7",
		})
		if n := recHdrBusinessKeyCount(t, ctx, pool, "tx_hash="+memberTx); n != 1 {
			t.Fatalf("tickets for the member tx = %d, want exactly 1 (one tx, one ticket)", n)
		}
		if n := recHdrBusinessKeyCount(t, ctx, pool, "tx_hash="+laterTx); n != 0 {
			t.Fatalf("tickets for the gapped interval tx = %d, want 0", n)
		}

		// Every original member log is individually traceable in the occurrence
		// evidence, even though the single aggregate ref digests: one bounded
		// occurrence row per member log, each enumerating block/index/contract.
		ticketID := recHdrTicketID(t, ctx, pool, "tx_hash="+memberTx)
		refs := recHdrOccurrenceRefs(t, ctx, pool, ticketID)
		if len(refs) != recHdrMemberCount {
			t.Fatalf("member occurrence rows = %d, want %d (one per original member log)", len(refs), recHdrMemberCount)
		}
		seen := make(map[string]bool, recHdrMemberCount)
		for _, ref := range refs {
			if len(ref) > 512 {
				t.Fatalf("occurrence ref exceeds the evidence bound: %d bytes", len(ref))
			}
			for index := int64(0); index < recHdrMemberCount; index++ {
				if strings.Contains(ref, fmt.Sprintf("l=%d:", index)) {
					seen[fmt.Sprint(index)] = true
				}
			}
		}
		for index := int64(0); index < recHdrMemberCount; index++ {
			if !seen[fmt.Sprint(index)] {
				t.Fatalf("member log %d is not traceable in the occurrence evidence", index)
			}
		}
		aggregateRef, err := reconciliation.TxAggregateEvidenceRef(members, 512)
		if err != nil {
			t.Fatalf("TxAggregateEvidenceRef: %v", err)
		}
		if !strings.Contains(aggregateRef, fmt.Sprintf("members=%d", recHdrMemberCount)) ||
			!strings.Contains(aggregateRef, "digest=sha256:") {
			t.Fatalf("aggregate ref %q is not the oversized digest form", aggregateRef)
		}

		// The gapped second interval keeps the task unclosable: no `done` while
		// any part of the scope is uncovered.
		if _, err := store.TransitionTask(ctx, reconciliation.TaskTransitionRequest{
			TaskID: taskID, To: reconciliation.TaskStateDone, Reason: "close over the gap", Actor: recAdminPrincipal,
		}); !errors.Is(err, reconciliation.ErrTaskNotComplete) {
			t.Fatalf("done over the gapped scope err = %v, want ErrTaskNotComplete", err)
		}
	})
}

// recHdrSeedWithdrawalRequestOnly seeds one 007 request business object (and
// its caller/authorization links) without any 011/012 execution row, so the
// chain-first reverse check still sees an uncovered transaction while the
// request's own event aggregate exists.
func recHdrSeedWithdrawalRequestOnly(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64) string {
	t.Helper()
	requestID := uuid.NewString()
	authorizationID := uuid.NewString()
	asset, recipient := recAdminAsset, recAdminOutsider
	amount := "1000"

	if _, err := pool.Exec(ctx, `INSERT INTO caller (caller_id, label) VALUES (1, 'it-height-window') ON CONFLICT (caller_id) DO NOTHING`); err != nil {
		t.Fatalf("seed caller: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO withdrawal_authorizations
		    (authorization_id, caller_id, chain_id, asset, recipient, amount, state, supplied_at)
		VALUES ($1, 1, $2, $3, $4, $5, 'active', now() - interval '1 minute')`,
		authorizationID, chainID, asset, recipient, amount); err != nil {
		t.Fatalf("seed authorization: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO withdrawal_requests
		    (request_id, caller_id, idempotency_key, authorization_id, chain_id, asset, recipient, amount, status, created_at)
		VALUES ($1, 1, $2, $3, $4, $5, $6, $7, 'accepted', now() - interval '1 minute')`,
		requestID, "idem-"+requestID, authorizationID, chainID, asset, recipient, amount); err != nil {
		t.Fatalf("seed request: %v", err)
	}
	return requestID
}

// recHdrSeedWithdrawalChain seeds the authoritative 011/012 execution chain of
// one in-range transaction following the existing txlifecycle fixture shape
// (caller -> authorization -> request -> scope carrier -> intent -> nonce
// binding -> attempt -> signing -> optional canonical receipt). withReceipt
// controls whether the receipt row is present; the signing row alone is the
// chain-first reverse-check anchor.
func recHdrSeedWithdrawalChain(t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	chainID int64, txHash string, blockNumber int64, blockHash string, withReceipt bool) (string, string) {
	t.Helper()
	requestID := uuid.NewString()
	intentID := uuid.NewString()
	authorizationID := uuid.NewString()
	attemptID := uuid.NewString()
	bindingID := uuid.NewString()
	asset, recipient := recAdminAsset, recAdminOutsider
	amount := "1000"

	if _, err := pool.Exec(ctx, `INSERT INTO caller (caller_id, label) VALUES (1, 'it-height-window') ON CONFLICT (caller_id) DO NOTHING`); err != nil {
		t.Fatalf("seed caller: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO nonce_wallet_registry (chain_id, sender, state, registry_seq)
		VALUES ($1, $2, 'active', 1)`,
		chainID, recAdminSender); err != nil {
		t.Fatalf("seed nonce registry: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO withdrawal_authorizations
		    (authorization_id, caller_id, chain_id, asset, recipient, amount, state, supplied_at)
		VALUES ($1, 1, $2, $3, $4, $5, 'active', now() - interval '1 minute')`,
		authorizationID, chainID, asset, recipient, amount); err != nil {
		t.Fatalf("seed authorization: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO withdrawal_requests
		    (request_id, caller_id, idempotency_key, authorization_id, chain_id, asset, recipient, amount, status, created_at)
		VALUES ($1, 1, $2, $3, $4, $5, $6, $7, 'accepted', now() - interval '1 minute')`,
		requestID, "idem-"+requestID, authorizationID, chainID, asset, recipient, amount); err != nil {
		t.Fatalf("seed request: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO withdrawal_authorization_scopes
		    (authorization_id, intent_id, request_id, sender, fee_max_total, fee_max_per_gas,
		     fee_max_priority, allows_fee_replacement, authorization_version, attested_by)
		VALUES ($1, $2, $3, $4, 100000000000000, 1000000000, 100000000, true, 1, 'it-seed')`,
		authorizationID, intentID, requestID, recAdminSender); err != nil {
		t.Fatalf("seed scope carrier: %v", err)
	}
	// withReceipt models the completed execution (final intent state + confirmed
	// attempt + canonical receipt); without it the fixture stops at the signed
	// attempt shape the existing txlifecycle fixture produces.
	intentState, attemptState := "admitted", "signed"
	var effectiveAt, confirmedAt any
	if withReceipt {
		intentState, attemptState = "completed", "confirmed"
		effectiveAt, confirmedAt = time.Now().UTC().Add(-time.Minute), time.Now().UTC().Add(-time.Minute)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO payment_intents
		    (intent_id, request_id, chain_id, sender, authorization_id, authorization_version,
		     state, state_version, admitted_recovery_version, admitted_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, 1, $6, 7, 0, now() - interval '1 minute', now() - interval '1 minute')`,
		intentID, requestID, chainID, recAdminSender, authorizationID, intentState); err != nil {
		t.Fatalf("seed intent: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO nonce_bindings
		    (binding_id, intent_id, chain_id, sender, nonce, state, authorization_id,
		     authorization_version, registry_seq, allocation_observation_id)
		VALUES ($1, $2, $3, $4, 0, 'allocated', $5, $6, 1, 'it-obs')`,
		bindingID, intentID, chainID, recAdminSender, authorizationID, strings.Repeat("0", 64)); err != nil {
		t.Fatalf("seed nonce binding: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO tx_attempts
		    (attempt_id, signing_request_id, intent_id, binding_ref, authorization_id,
		     authorization_version, recovery_version, chain_id, sender, nonce, tx_type,
		     to_addr, value, data, gas_limit, gas_price, asset, recipient, amount,
		     canonical_envelope, content_hash, state, revision_seq, effective_at, confirmed_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, 1, 0, $6, $7, 0, 0, $8, 0, '\x00', 21000, 1,
		        $8, $9, $10, 'it-envelope', $11, $12, 1, $13, $14, now() - interval '1 minute')`,
		attemptID, "sr-"+attemptID, intentID, bindingID, authorizationID,
		chainID, recAdminSender, asset, recipient, amount, "0x"+strings.Repeat("e5", 32),
		attemptState, effectiveAt, confirmedAt); err != nil {
		t.Fatalf("seed attempt: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO tx_attempt_signings (attempt_id, signature, signed_tx_bytes, tx_hash, signed_at)
		VALUES ($1, $2, '\x01', $3, now())`,
		attemptID, "0x"+strings.Repeat("11", 65), txHash); err != nil {
		t.Fatalf("seed signing: %v", err)
	}
	if withReceipt {
		if _, err := pool.Exec(ctx, `
			INSERT INTO tx_receipts
			    (attempt_id, tx_hash, status, block_number, block_hash, effect, canonicality,
			     confirmations, confirm_threshold, confirm_policy_seq, observed_at, confirmed_at, updated_at)
			VALUES ($1, $2, 1, $3, $4, 'effective', 'canonical', 4, 3, 1,
			        now() - interval '1 minute', now() - interval '1 minute', now() - interval '1 minute')`,
			attemptID, txHash, blockNumber, blockHash); err != nil {
			t.Fatalf("seed receipt: %v", err)
		}
	}
	return requestID, attemptID
}
