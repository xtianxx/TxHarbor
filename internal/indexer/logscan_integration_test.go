//go:build integration

// T007/T009/T010/T011/T013/T014/T015 acceptance tests for the 003 log
// scanner on a real PostgreSQL. Anvil produces the real Transfer data
// (including zero-amount logs) for scenes #1/#9's chain source; an httptest
// endpoint injects every RPC fault class; every assertion reads durable rows
// back through a pool. Ordering is never decided by sleeping: start barriers,
// recorded RPC call counts, PostgreSQL's own lock-wait view and bounded
// conditional waits are the only sync points.
package indexer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/xtianxx/txharbor/internal/eth"
)

// Fixture addresses and the two distinct configuration identities (64
// lowercase hex, the log_checkpoint CHECK shape).
const (
	logscanContractA = "0x1111111111111111111111111111111111111111"
	logscanContractB = "0x2222222222222222222222222222222222222222"
)

var (
	logscanConfigHashA = strings.Repeat("ab", 32)
	logscanConfigHashB = strings.Repeat("cd", 32)

	logscanFromAddr = common.HexToAddress("0x00000000000000000000000000000000000000f1")
	logscanToAddr   = common.HexToAddress("0x00000000000000000000000000000000000000f2")
)

func logscanLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

// logscanLease acquires a fresh lease for chainID (win required) with test
// cadence: a TTL long enough for one test case.
func logscanLease(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, owner string) *Lease {
	t.Helper()
	lease := newTestLease(t, pool, chainID, owner, time.Minute, time.Second)
	won, token, err := lease.Acquire(ctx)
	if err != nil || !won {
		t.Fatalf("lease Acquire(%s) = (won=%v token=%d err=%v), want win", owner, won, token, err)
	}
	return lease
}

// logscanNewScanner builds a LogScanner with test-speed knobs; explicit
// config fields always win.
func logscanNewScanner(t *testing.T, pool *pgxpool.Pool, headers HeaderClient, logs LogsClient, lease *Lease, cfg LogConfig) *LogScanner {
	t.Helper()
	if cfg.RPCTimeout == 0 {
		cfg.RPCTimeout = 2 * time.Second
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 25 * time.Millisecond
	}
	if cfg.RetryInitial == 0 {
		cfg.RetryInitial = 25 * time.Millisecond
	}
	if cfg.RetryMax == 0 {
		cfg.RetryMax = 250 * time.Millisecond
	}
	sc, err := NewLogScanner(pool, headers, logs, lease, cfg, logscanLogger())
	if err != nil {
		t.Fatalf("NewLogScanner(): %v", err)
	}
	return sc
}

// logscanServeTo runs ServeLoop until the durable next_block reaches want,
// then cancels and requires a clean nil return.
func logscanServeTo(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, sc *LogScanner, want uint64, timeout time.Duration) {
	t.Helper()
	rctx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- sc.ServeLoop(rctx, nil) }()
	waitUntil(t, time.Now().Add(timeout), fmt.Sprintf("log checkpoint reaches %d", want), func() bool {
		next, ok := logscanCheckpointNext(ctx, pool, chainID)
		return ok && next >= want
	})
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ServeLoop() after cancel = %v, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("ServeLoop() did not return after cancel")
	}
}

// logscanServeUntilPaused runs ServeLoop to its terminal pause and returns the
// stop error.
func logscanServeUntilPaused(t *testing.T, ctx context.Context, sc *LogScanner, timeout time.Duration) error {
	t.Helper()
	rctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return sc.ServeLoop(rctx, nil)
}

// --- durable-state readers -------------------------------------------------

type logscanCheckpointState struct {
	start int64
	hash  string
	next  int64
	stamp time.Time
}

type logscanPauseState struct {
	height int64
	kind   string
	detail string
}

// logscanState is the full durable footprint a refused/paused worker must not
// change: log rows, pause rows and the checkpoint row byte for byte.
type logscanState struct {
	rows       int
	pauses     int
	hasCP      bool
	checkpoint logscanCheckpointState
}

func logscanCheckpointNext(ctx context.Context, pool *pgxpool.Pool, chainID int64) (uint64, bool) {
	var next int64
	if err := pool.QueryRow(ctx, `SELECT next_block FROM log_checkpoint WHERE chain_id = $1`, chainID).Scan(&next); err != nil {
		return 0, false
	}
	return uint64(next), true
}

func logscanReadCheckpoint(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64) (logscanCheckpointState, bool) {
	t.Helper()
	var cp logscanCheckpointState
	err := pool.QueryRow(ctx,
		`SELECT start_block, config_hash, next_block, updated_at FROM log_checkpoint WHERE chain_id = $1`, chainID).
		Scan(&cp.start, &cp.hash, &cp.next, &cp.stamp)
	if errors.Is(err, pgx.ErrNoRows) {
		return cp, false
	}
	if err != nil {
		t.Fatalf("read log_checkpoint: %v", err)
	}
	return cp, true
}

func logscanReadPause(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64) (logscanPauseState, bool) {
	t.Helper()
	var p logscanPauseState
	err := pool.QueryRow(ctx,
		`SELECT height, kind, detail FROM log_pause WHERE chain_id = $1`, chainID).
		Scan(&p.height, &p.kind, &p.detail)
	if errors.Is(err, pgx.ErrNoRows) {
		return p, false
	}
	if err != nil {
		t.Fatalf("read log_pause: %v", err)
	}
	return p, true
}

func logscanSnapshot(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64) logscanState {
	t.Helper()
	var st logscanState
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM erc20_transfer_logs WHERE chain_id = $1`, chainID).Scan(&st.rows); err != nil {
		t.Fatalf("count transfer logs: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM log_pause WHERE chain_id = $1`, chainID).Scan(&st.pauses); err != nil {
		t.Fatalf("count log pauses: %v", err)
	}
	st.checkpoint, st.hasCP = logscanReadCheckpoint(t, ctx, pool, chainID)
	return st
}

func logscanRequireStateUnchanged(t *testing.T, before, after logscanState) {
	t.Helper()
	if before.rows != after.rows || before.pauses != after.pauses || before.hasCP != after.hasCP ||
		before.checkpoint.start != after.checkpoint.start ||
		before.checkpoint.hash != after.checkpoint.hash ||
		before.checkpoint.next != after.checkpoint.next ||
		!before.checkpoint.stamp.Equal(after.checkpoint.stamp) {
		t.Fatalf("durable state changed:\nbefore=%+v\nafter =%+v", before, after)
	}
}

// logscanRequireProgressUnchanged is the no-advance assertion for cases where
// a pause row is the expected durable addition: rows and the checkpoint row
// (every column, including updated_at) must not move.
func logscanRequireProgressUnchanged(t *testing.T, before, after logscanState) {
	t.Helper()
	if before.rows != after.rows || before.hasCP != after.hasCP ||
		before.checkpoint.start != after.checkpoint.start ||
		before.checkpoint.hash != after.checkpoint.hash ||
		before.checkpoint.next != after.checkpoint.next ||
		!before.checkpoint.stamp.Equal(after.checkpoint.stamp) {
		t.Fatalf("progress changed:\nbefore=%+v\nafter =%+v", before, after)
	}
}

func logscanCountLogs(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM erc20_transfer_logs WHERE chain_id = $1`, chainID).Scan(&n); err != nil {
		t.Fatalf("count transfer logs: %v", err)
	}
	return n
}

func logscanCountLogsAt(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, height uint64) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM erc20_transfer_logs WHERE chain_id = $1 AND block_number = $2`, chainID, int64(height)).Scan(&n); err != nil {
		t.Fatalf("count transfer logs at %d: %v", height, err)
	}
	return n
}

func logscanCountLogsBetween(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, from, to uint64) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM erc20_transfer_logs WHERE chain_id = $1 AND block_number BETWEEN $2 AND $3`,
		chainID, int64(from), int64(to)).Scan(&n); err != nil {
		t.Fatalf("count transfer logs in [%d,%d]: %v", from, to, err)
	}
	return n
}

func logscanCountPauses(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM log_pause WHERE chain_id = $1`, chainID).Scan(&n); err != nil {
		t.Fatalf("count log pauses: %v", err)
	}
	return n
}

// logscanStoredRow is one stored Transfer log, raw columns only.
type logscanStoredRow struct {
	blockHash string
	txHash    string
	logIndex  int64
	contract  string
	topic0    string
	topic1    string
	topic2    string
	data      string
}

func logscanReadRowAt(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, height uint64) (logscanStoredRow, bool) {
	t.Helper()
	var r logscanStoredRow
	err := pool.QueryRow(ctx, `
SELECT block_hash, tx_hash, log_index, contract, topic0, topic1, topic2, data
FROM erc20_transfer_logs WHERE chain_id = $1 AND block_number = $2`, chainID, int64(height)).
		Scan(&r.blockHash, &r.txHash, &r.logIndex, &r.contract, &r.topic0, &r.topic1, &r.topic2, &r.data)
	if errors.Is(err, pgx.ErrNoRows) {
		return r, false
	}
	if err != nil {
		t.Fatalf("read transfer row at %d: %v", height, err)
	}
	return r, true
}

// --- seeding ---------------------------------------------------------------

// logscanSeedBlocks inserts canonical chain_blocks rows for [from,to] using
// the same headers the header client serves, so DB truth and the chain view
// agree by construction.
func logscanSeedBlocks(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, chain *scriptChain, from, to uint64) {
	t.Helper()
	type blockRow struct {
		number uint64
		hash   string
		parent string
	}
	rows := make([]blockRow, 0, to-from+1)
	chain.mu.Lock()
	for n := from; n <= to; n++ {
		hdr := chain.heads[n]
		if hdr == nil {
			chain.mu.Unlock()
			t.Fatalf("script chain has no header at %d", n)
		}
		rows = append(rows, blockRow{number: n, hash: hashHex(hdr.Hash()), parent: hashHex(hdr.ParentHash)})
	}
	chain.mu.Unlock()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin seed transaction: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for _, r := range rows {
		if _, err := tx.Exec(ctx, `
INSERT INTO chain_blocks (chain_id, number, hash, parent_hash)
VALUES ($1, $2, $3, $4) ON CONFLICT (chain_id, number, hash) DO NOTHING`,
			chainID, int64(r.number), r.hash, r.parent); err != nil {
			t.Fatalf("seed chain_blocks at %d: %v", r.number, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit seed blocks: %v", err)
	}
}

// logscanSet002Checkpoint points 002's durable checkpoint at height (the log
// scanner's coverage upper bound); the block row must already exist.
func logscanSet002Checkpoint(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, chain *scriptChain, height uint64) {
	t.Helper()
	chain.mu.Lock()
	hdr := chain.heads[height]
	chain.mu.Unlock()
	if hdr == nil {
		t.Fatalf("script chain has no header at %d", height)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO indexer_checkpoint (chain_id, height, block_hash, start_height)
VALUES ($1, $2, $3, 0)
ON CONFLICT (chain_id) DO UPDATE SET height = EXCLUDED.height, block_hash = EXCLUDED.block_hash, updated_at = now()`,
		chainID, int64(height), hashHex(hdr.Hash())); err != nil {
		t.Fatalf("seed indexer_checkpoint at %d: %v", height, err)
	}
}

// logscanSeedLogProgress plants a log_checkpoint row exactly as a previous
// committed interval would have left it.
func logscanSeedLogProgress(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, start uint64, hash string, next uint64) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
INSERT INTO log_checkpoint (chain_id, start_block, config_hash, next_block)
VALUES ($1, $2, $3, $4)`, chainID, int64(start), hash, int64(next)); err != nil {
		t.Fatalf("seed log_checkpoint: %v", err)
	}
}

// logscanCoverage builds the [from,to] canonical hash map the scanner reads.
func logscanCoverage(t *testing.T, chain *scriptChain, from, to uint64) map[uint64]string {
	t.Helper()
	m := make(map[uint64]string, to-from+1)
	chain.mu.Lock()
	defer chain.mu.Unlock()
	for n := from; n <= to; n++ {
		hdr := chain.heads[n]
		if hdr == nil {
			t.Fatalf("script chain has no header at %d", n)
		}
		m[n] = hashHex(hdr.Hash())
	}
	return m
}

// --- log fixtures ----------------------------------------------------------

func logscanTxHash(n, index uint64) string {
	return fmt.Sprintf("0x%064x", 0x7000_0000+n*16+index)
}

func logscanTransferTopics() []string {
	return []string{
		eth.TransferSig.Hex(),
		common.BytesToHash(logscanFromAddr.Bytes()).Hex(),
		common.BytesToHash(logscanToAddr.Bytes()).Hex(),
	}
}

// logscanLogJSON renders one valid Transfer log as an eth_getLogs result item.
func logscanLogJSON(n uint64, blockHash, txHash string, index uint64, contract common.Address, amount uint64) map[string]any {
	return map[string]any{
		"address":          strings.ToLower(contract.Hex()),
		"topics":           logscanTransferTopics(),
		"data":             hexutil.Encode(common.LeftPadBytes(new(big.Int).SetUint64(amount).Bytes(), 32)),
		"blockNumber":      hexutil.EncodeUint64(n),
		"transactionHash":  txHash,
		"transactionIndex": "0x0",
		"blockHash":        blockHash,
		"logIndex":         hexutil.EncodeUint64(index),
		"removed":          false,
	}
}

// logscanLogMessage builds one valid Transfer log from the header the chain
// client serves at n (so its block hash is canonical by construction).
func logscanLogMessage(t *testing.T, chain *scriptChain, n uint64, contract common.Address, amount uint64) map[string]any {
	t.Helper()
	chain.mu.Lock()
	hdr := chain.heads[n]
	chain.mu.Unlock()
	if hdr == nil {
		t.Fatalf("script chain has no header at %d", n)
	}
	return logscanLogJSON(n, hashHex(hdr.Hash()), logscanTxHash(n, 0), 0, contract, amount)
}

// logscanTypesLog builds the same log as a types.Log for direct validation
// calls; index distinguishes identities within one height.
func logscanTypesLog(t *testing.T, chain *scriptChain, n, index uint64, contract common.Address, amount uint64) types.Log {
	t.Helper()
	chain.mu.Lock()
	hdr := chain.heads[n]
	chain.mu.Unlock()
	if hdr == nil {
		t.Fatalf("script chain has no header at %d", n)
	}
	return types.Log{
		Address: contract,
		Topics: []common.Hash{
			eth.TransferSig,
			common.BytesToHash(logscanFromAddr.Bytes()),
			common.BytesToHash(logscanToAddr.Bytes()),
		},
		Data:        common.LeftPadBytes(new(big.Int).SetUint64(amount).Bytes(), 32),
		BlockNumber: n,
		BlockHash:   hdr.Hash(),
		TxHash:      common.HexToHash(logscanTxHash(n, index)),
		Index:       uint(index),
	}
}

func logscanCloneLog(l map[string]any) map[string]any {
	out := make(map[string]any, len(l))
	for k, v := range l {
		out[k] = v
	}
	out["topics"] = append([]string(nil), l["topics"].([]string)...)
	return out
}

// logscanEmitMap builds one valid Transfer log per height.
func logscanEmitMap(t *testing.T, chain *scriptChain, contract common.Address, amounts map[uint64]uint64) map[uint64]map[string]any {
	t.Helper()
	out := make(map[uint64]map[string]any, len(amounts))
	for n, amount := range amounts {
		out[n] = logscanLogMessage(t, chain, n, contract, amount)
	}
	return out
}

// logscanRangeLogs is the fake RPC's default generator: the emitted logs whose
// height falls inside the queried closed range.
func logscanRangeLogs(byHeight map[uint64]map[string]any) func(from, to uint64) []map[string]any {
	return func(from, to uint64) []map[string]any {
		var out []map[string]any
		for n := from; n <= to; n++ {
			if l, ok := byHeight[n]; ok {
				out = append(out, l)
			}
		}
		return out
	}
}

// --- fake JSON-RPC endpoint ------------------------------------------------

type logscanQuery struct {
	from, to uint64
}

// logscanResponse scripts one eth_getLogs reply. Fault fields take precedence
// in this order: gate, delay, HTTP status, raw body, JSON-RPC error; a nil
// logs slice falls through to the default generator.
type logscanResponse struct {
	logs       []map[string]any
	httpStatus int
	raw        string
	rpcCode    int
	rpcMessage string
	delay      time.Duration
	gate       chan struct{}
}

type logscanFakeRPC struct {
	srv          *httptest.Server
	mu           sync.Mutex
	script       []*logscanResponse
	defaultFault *logscanResponse
	defaultLogs  func(from, to uint64) []map[string]any
	queries      []logscanQuery
	onQuery      func(from, to uint64)
}

func logscanNewFakeRPC(t *testing.T, defaultLogs func(from, to uint64) []map[string]any) *logscanFakeRPC {
	t.Helper()
	f := &logscanFakeRPC{defaultLogs: defaultLogs}
	f.srv = httptest.NewServer(http.HandlerFunc(f.handle))
	t.Cleanup(f.srv.Close)
	return f
}

// dial returns an eth.Client against the fake endpoint (FilterLogs goes
// through the real classification code).
func (f *logscanFakeRPC) dial(t *testing.T) *eth.Client {
	t.Helper()
	c, err := eth.Dial(context.Background(), f.srv.URL, 5*time.Second)
	if err != nil {
		t.Fatalf("eth.Dial(fake rpc): %v", err)
	}
	t.Cleanup(c.Close)
	return c
}

func (f *logscanFakeRPC) scriptNext(r *logscanResponse) {
	f.mu.Lock()
	f.script = append(f.script, r)
	f.mu.Unlock()
}

func (f *logscanFakeRPC) setDefaultFault(r *logscanResponse) {
	f.mu.Lock()
	f.defaultFault = r
	f.mu.Unlock()
}

func (f *logscanFakeRPC) setOnQuery(fn func(from, to uint64)) {
	f.mu.Lock()
	f.onQuery = fn
	f.mu.Unlock()
}

func (f *logscanFakeRPC) queryCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.queries)
}

func (f *logscanFakeRPC) recordedQueries() []logscanQuery {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]logscanQuery(nil), f.queries...)
}

func (f *logscanFakeRPC) handle(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID     json.RawMessage   `json:"id"`
		Method string            `json:"method"`
		Params []json.RawMessage `json:"params"`
	}
	body, err := io.ReadAll(r.Body)
	if err != nil || json.Unmarshal(body, &req) != nil {
		http.Error(w, "malformed JSON-RPC request", http.StatusBadRequest)
		return
	}
	switch req.Method {
	case "eth_chainId":
		logscanWriteResult(w, req.ID, "0x7a69")
	case "eth_getLogs":
		var q struct {
			FromBlock string `json:"fromBlock"`
			ToBlock   string `json:"toBlock"`
		}
		if len(req.Params) > 0 {
			_ = json.Unmarshal(req.Params[0], &q)
		}
		from, to := logscanParseHexUint(q.FromBlock), logscanParseHexUint(q.ToBlock)
		f.mu.Lock()
		f.queries = append(f.queries, logscanQuery{from: from, to: to})
		resp := f.defaultFault
		if len(f.script) > 0 {
			resp, f.script = f.script[0], f.script[1:]
		}
		onQuery := f.onQuery
		f.mu.Unlock()
		if onQuery != nil {
			onQuery(from, to)
		}
		if resp != nil {
			if resp.gate != nil {
				select {
				case <-resp.gate:
				case <-r.Context().Done():
					return
				}
			}
			if resp.delay > 0 {
				select {
				case <-time.After(resp.delay):
				case <-r.Context().Done():
					return
				}
			}
			if resp.httpStatus != 0 {
				w.WriteHeader(resp.httpStatus)
				_, _ = w.Write([]byte("injected rpc fault"))
				return
			}
			if resp.raw != "" {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(resp.raw))
				return
			}
			if resp.rpcCode != 0 {
				logscanWriteRPCError(w, req.ID, resp.rpcCode, resp.rpcMessage)
				return
			}
			if resp.logs != nil {
				logscanWriteResult(w, req.ID, resp.logs)
				return
			}
		}
		var logs []map[string]any
		if f.defaultLogs != nil {
			logs = f.defaultLogs(from, to)
		}
		if logs == nil {
			logs = []map[string]any{}
		}
		logscanWriteResult(w, req.ID, logs)
	default:
		logscanWriteRPCError(w, req.ID, -32601, "method not found")
	}
}

func logscanWriteResult(w http.ResponseWriter, id json.RawMessage, result any) {
	if len(id) == 0 {
		id = json.RawMessage("1")
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": id, "result": result})
}

func logscanWriteRPCError(w http.ResponseWriter, id json.RawMessage, code int, message string) {
	if len(id) == 0 {
		id = json.RawMessage("1")
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"jsonrpc": "2.0", "id": id,
		"error": map[string]any{"code": code, "message": message},
	})
}

func logscanParseHexUint(s string) uint64 {
	n, err := strconv.ParseUint(strings.TrimPrefix(s, "0x"), 16, 64)
	if err != nil {
		return 0
	}
	return n
}

func logscanRequireQueries(t *testing.T, got, want []logscanQuery) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("eth_getLogs ranges = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("eth_getLogs range %d = %v, want %v (all: %v)", i, got[i], want[i], got)
		}
	}
}

// --- Anvil node ------------------------------------------------------------

type logscanAnvilNode struct {
	url      string
	rpc      *rpc.Client
	accounts []common.Address
}

// logscanStartAnvilNode boots the pinned Anvil image (real chain data source).
func logscanStartAnvilNode(t *testing.T) *logscanAnvilNode {
	t.Helper()
	ctx := context.Background()
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "ghcr.io/foundry-rs/foundry:v1.8.1",
			ExposedPorts: []string{"8545/tcp"},
			// Image entrypoint is /bin/sh -c: override it, otherwise anvil
			// never receives --host and binds 127.0.0.1 inside the
			// container (unreachable via port mapping).
			Entrypoint: []string{"anvil"},
			Cmd:        []string{"--host", "0.0.0.0", "--port", "8545", "--chain-id", "31337"},
			WaitingFor: wait.ForLog("Listening on"),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start anvil: %v", err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })
	host, err := ctr.Host(ctx)
	if err != nil {
		t.Fatalf("anvil host: %v", err)
	}
	port, err := ctr.MappedPort(ctx, "8545/tcp")
	if err != nil {
		t.Fatalf("anvil port: %v", err)
	}
	url := fmt.Sprintf("http://%s:%s", host, port.Port())
	c, err := rpc.DialContext(ctx, url)
	if err != nil {
		t.Fatalf("dial anvil rpc: %v", err)
	}
	t.Cleanup(c.Close)
	var accounts []common.Address
	if err := c.CallContext(ctx, &accounts, "eth_accounts"); err != nil {
		t.Fatalf("eth_accounts: %v", err)
	}
	if len(accounts) == 0 {
		t.Fatal("anvil returned no unlocked accounts")
	}
	return &logscanAnvilNode{url: url, rpc: c, accounts: accounts}
}

func (n *logscanAnvilNode) mustCall(t *testing.T, out any, method string, args ...any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := n.rpc.CallContext(ctx, out, method, args...); err != nil {
		t.Fatalf("%s: %v", method, err)
	}
}

func (n *logscanAnvilNode) setCode(t *testing.T, addr common.Address, code []byte) {
	t.Helper()
	var ok bool
	n.mustCall(t, &ok, "anvil_setCode", addr, hexutil.Encode(code))
}

func (n *logscanAnvilNode) sendTransfer(t *testing.T, to common.Address) common.Hash {
	t.Helper()
	var txHash common.Hash
	n.mustCall(t, &txHash, "eth_sendTransaction", map[string]any{
		"from": n.accounts[0], "to": to, "data": "0x", "gas": "0x30d40",
	})
	return txHash
}

// sendTransferAt sends one contract call and returns its exact height from the
// receipt: automine makes the block, but only the receipt is authoritative.
func (n *logscanAnvilNode) sendTransferAt(t *testing.T, to common.Address) (common.Hash, uint64) {
	t.Helper()
	txHash := n.sendTransfer(t, to)
	deadline := time.Now().Add(10 * time.Second)
	for {
		var receipt *struct {
			BlockNumber hexutil.Uint64 `json:"blockNumber"`
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := n.rpc.CallContext(ctx, &receipt, "eth_getTransactionReceipt", txHash)
		cancel()
		if err != nil {
			t.Fatalf("eth_getTransactionReceipt(%s): %v", txHash, err)
		}
		if receipt != nil {
			return txHash, uint64(receipt.BlockNumber)
		}
		if time.Now().After(deadline) {
			t.Fatalf("transaction %s was not mined within the deadline", txHash)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func (n *logscanAnvilNode) mine(t *testing.T, blocks uint64) {
	t.Helper()
	before := n.blockNumber(t)
	var res json.RawMessage
	n.mustCall(t, &res, "anvil_mine", hexutil.EncodeUint64(blocks))
	waitUntil(t, time.Now().Add(10*time.Second), fmt.Sprintf("anvil head to grow by %d", blocks), func() bool {
		return n.blockNumber(t) >= before+blocks
	})
}

func (n *logscanAnvilNode) blockNumber(t *testing.T) uint64 {
	t.Helper()
	var num hexutil.Uint64
	n.mustCall(t, &num, "eth_blockNumber")
	return uint64(num)
}

// logscanEmitCode is the runtime bytecode of a contract whose fallback emits
// exactly one Transfer log: LOG3(topic0, topic1, topic2) with 32 bytes of
// data. A nil amount leaves memory zero (raw zero amount); a non-nil amount is
// MSTORE'd first.
func logscanEmitCode(topics [3]common.Hash, amount *big.Int) []byte {
	var code []byte
	if amount != nil {
		code = append(code, 0x7f) // PUSH32 amount
		code = append(code, common.BigToHash(amount).Bytes()...)
		code = append(code, 0x60, 0x00, 0x52) // PUSH1 0; MSTORE
	}
	for _, topic := range [3]common.Hash{topics[2], topics[1], topics[0]} {
		code = append(code, 0x7f) // PUSH32 topic
		code = append(code, topic.Bytes()...)
	}
	code = append(code, 0x60, 0x20, 0x60, 0x00, 0xa3, 0x00) // PUSH1 32; PUSH1 0; LOG3; STOP
	return code
}

// --- commit-acknowledgment loss --------------------------------------------

// logscanCommitDrop dials every pool connection through a wrapper that, once
// armed, forwards the next COMMIT to PostgreSQL and then closes the client
// side before the reply can be read: the transaction commits, the worker
// observes an error. Exactly the uncertain-commit state FR-16/OQ2 describes.
type logscanCommitDrop struct {
	arm     atomic.Bool
	dropped atomic.Int32
}

func (d *logscanCommitDrop) dial(ctx context.Context, network, addr string) (net.Conn, error) {
	c, err := (&net.Dialer{}).DialContext(ctx, network, addr)
	if err != nil {
		return nil, err
	}
	return &logscanCommitDropConn{Conn: c, d: d}, nil
}

type logscanCommitDropConn struct {
	net.Conn
	d *logscanCommitDrop
}

func (c *logscanCommitDropConn) Write(b []byte) (int, error) {
	n, err := c.Conn.Write(b)
	if err == nil && bytes.Contains(b, []byte("commit")) && c.d.arm.CompareAndSwap(true, false) {
		c.d.dropped.Add(1)
		_ = c.Conn.Close() // reply lost; the forwarded COMMIT still lands
	}
	return n, err
}

func logscanOpenCommitDropPool(t *testing.T, dsn string) (*pgxpool.Pool, *logscanCommitDrop) {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse dsn for drop pool: %v", err)
	}
	cfg.MaxConns = 8
	cfg.MinConns = 1
	cfg.MaxConnLifetime = time.Hour
	cfg.MaxConnIdleTime = 30 * time.Minute
	drop := &logscanCommitDrop{}
	cfg.ConnConfig.DialFunc = drop.dial
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("create drop pool: %v", err)
	}
	t.Cleanup(pool.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping drop pool: %v", err)
	}
	return pool, drop
}

// --- scene #1 ---------------------------------------------------------------

// TestLogScanAnvilMixedIntervals covers quickstart scene #1 (FR-02/FR-08,
// SC-01): a real Anvil chain carries a zero-amount Transfer, a normal Transfer
// and log-free intervals; the scanner advances continuously and preserves
// every raw topic/data byte including the zero amount.
func TestLogScanAnvilMixedIntervals(t *testing.T) {
	dsn := startIndexerPostgres(t)
	ctx := context.Background()
	pool := openIndexerPool(t, dsn)
	defer pool.Close()

	node := logscanStartAnvilNode(t)
	client, err := eth.Dial(ctx, node.url, 5*time.Second)
	if err != nil {
		t.Fatalf("dial anvil client: %v", err)
	}
	defer client.Close()

	const chainID = scanChainID // 31337, matching the Anvil chain id
	zeroAddr := common.HexToAddress("0x1000000000000000000000000000000000000001")
	amountAddr := common.HexToAddress("0x1000000000000000000000000000000000000002")
	topics := [3]common.Hash{eth.TransferSig}
	topics[1] = common.BytesToHash(logscanFromAddr.Bytes())
	topics[2] = common.BytesToHash(logscanToAddr.Bytes())

	node.setCode(t, zeroAddr, logscanEmitCode(topics, nil))
	node.setCode(t, amountAddr, logscanEmitCode(topics, big.NewInt(42)))

	// Height plan: a zero transfer, a log-free interval, an amount-42
	// transfer, another log-free interval and a zero transfer. Heights are
	// read back from the receipts, never assumed.
	txZero1, h1 := node.sendTransferAt(t, zeroAddr)
	node.mine(t, 2)
	txAmount, h2 := node.sendTransferAt(t, amountAddr)
	node.mine(t, 1)
	txZero2, h3 := node.sendTransferAt(t, zeroAddr)
	head := node.blockNumber(t)
	if h1 >= h2 || h2 >= h3 || head < h3 {
		t.Fatalf("anvil heights h1=%d h2=%d h3=%d head=%d out of order", h1, h2, h3, head)
	}
	if h2-h1 < 2 || h3-h2 < 2 {
		t.Fatalf("empty intervals missing: h1=%d h2=%d h3=%d", h1, h2, h3)
	}
	emptyHeights := make([]uint64, 0, head)
	for n := uint64(0); n <= head; n++ {
		if n != h1 && n != h2 && n != h3 {
			emptyHeights = append(emptyHeights, n)
		}
	}
	if len(emptyHeights) < 2 {
		t.Fatalf("only %d empty heights, want the two mined intervals", len(emptyHeights))
	}

	// 002 indexes the real headers first: they are the log scanner's coverage.
	// Scanner.Run acquires the lease itself, so the handle must not be
	// pre-acquired; after Run returns it holds the winning token that the log
	// scanner reuses.
	lease := newTestLease(t, pool, chainID, "anvil-002", time.Minute, time.Second)
	sc002, err := NewScanner(pool, client, lease, Config{
		StartHeight: 0, RPCTimeout: 2 * time.Second, PollInterval: 25 * time.Millisecond,
		RetryInitial: 25 * time.Millisecond, RetryMax: 250 * time.Millisecond,
	}, logscanLogger())
	if err != nil {
		t.Fatalf("NewScanner(): %v", err)
	}
	runScanTo(t, sc002, head, 30*time.Second)
	if cp, ok := readCoordCheckpoint(t, ctx, pool, chainID); !ok || cp.height != int64(head) {
		t.Fatalf("002 checkpoint = %+v, want height %d", cp, head)
	}

	ls := logscanNewScanner(t, pool, client, client, lease, LogConfig{
		StartBlock:  0,
		Contracts:   []string{strings.ToLower(zeroAddr.Hex()), strings.ToLower(amountAddr.Hex())},
		ConfigHash:  logscanConfigHashA,
		BatchBlocks: 2,
	})
	logscanServeTo(t, ctx, pool, chainID, ls, head+1, 30*time.Second)

	wants := []struct {
		height   uint64
		contract common.Address
		data     string
		txHash   common.Hash
	}{
		{h1, zeroAddr, hexutil.Encode(make([]byte, 32)), txZero1},
		{h2, amountAddr, hexutil.Encode(common.LeftPadBytes(big.NewInt(42).Bytes(), 32)), txAmount},
		{h3, zeroAddr, hexutil.Encode(make([]byte, 32)), txZero2},
	}
	for _, w := range wants {
		hdr, err := client.HeaderByNumber(ctx, new(big.Int).SetUint64(w.height))
		if err != nil {
			t.Fatalf("anvil header %d: %v", w.height, err)
		}
		got, ok := logscanReadRowAt(t, ctx, pool, chainID, w.height)
		if !ok {
			t.Fatalf("no stored log at height %d", w.height)
		}
		wantTopics := logscanTransferTopics()
		switch {
		case got.blockHash != hashHex(hdr.Hash()):
			t.Fatalf("height %d block_hash = %s, want %s", w.height, got.blockHash, hashHex(hdr.Hash()))
		case got.txHash != strings.ToLower(w.txHash.Hex()):
			t.Fatalf("height %d tx_hash = %s, want %s", w.height, got.txHash, strings.ToLower(w.txHash.Hex()))
		case got.logIndex != 0:
			t.Fatalf("height %d log_index = %d, want 0", w.height, got.logIndex)
		case got.contract != strings.ToLower(w.contract.Hex()):
			t.Fatalf("height %d contract = %s, want %s", w.height, got.contract, strings.ToLower(w.contract.Hex()))
		case got.topic0 != wantTopics[0] || got.topic1 != wantTopics[1] || got.topic2 != wantTopics[2]:
			t.Fatalf("height %d topics = %s/%s/%s, want %s/%s/%s",
				w.height, got.topic0, got.topic1, got.topic2, wantTopics[0], wantTopics[1], wantTopics[2])
		case got.data != w.data:
			t.Fatalf("height %d data = %s, want %s (raw retention)", w.height, got.data, w.data)
		}
	}
	for _, h := range emptyHeights {
		if n := logscanCountLogsAt(t, ctx, pool, chainID, h); n != 0 {
			t.Fatalf("height %d holds %d logs, want 0 (empty interval)", h, n)
		}
	}
	if n := logscanCountLogs(t, ctx, pool, chainID); n != 3 {
		t.Fatalf("total logs = %d, want 3", n)
	}
	cp, ok := logscanReadCheckpoint(t, ctx, pool, chainID)
	if !ok || cp.start != 0 || cp.next != int64(head)+1 || cp.hash != logscanConfigHashA {
		t.Fatalf("log checkpoint = %+v, want start=0 next=%d hash=%s", cp, head+1, logscanConfigHashA)
	}
}

// --- scene #9 ---------------------------------------------------------------

// TestLogScanCatchesUpFromIndependentStart covers quickstart scene #9
// (FR-08/SC-08): 002 has already synced far past the configured log start; the
// log stream catches up from its own start and never queries past 002's
// coverage or below its own start.
func TestLogScanCatchesUpFromIndependentStart(t *testing.T) {
	dsn := startIndexerPostgres(t)
	ctx := context.Background()
	pool := openIndexerPool(t, dsn)
	defer pool.Close()

	const (
		chainID = int64(32501)
		head    = uint64(12)
		start   = uint64(5)
	)
	chain := newScriptChain(chainID, head, 0xD9)
	logscanSeedBlocks(t, ctx, pool, chainID, chain, 0, head)
	logscanSet002Checkpoint(t, ctx, pool, chainID, chain, head) // 002 is already higher

	emits := logscanEmitMap(t, chain, common.HexToAddress(logscanContractA), map[uint64]uint64{7: 777})
	fake := logscanNewFakeRPC(t, logscanRangeLogs(emits))

	lease := logscanLease(t, ctx, pool, chainID, "independent-start")
	ls := logscanNewScanner(t, pool, chain, fake.dial(t), lease, LogConfig{
		StartBlock:  start,
		Contracts:   []string{logscanContractA},
		ConfigHash:  logscanConfigHashA,
		BatchBlocks: 3,
	})
	logscanServeTo(t, ctx, pool, chainID, ls, head+1, 30*time.Second)

	cp, ok := logscanReadCheckpoint(t, ctx, pool, chainID)
	if !ok || cp.start != int64(start) || cp.next != int64(head+1) || cp.hash != logscanConfigHashA {
		t.Fatalf("log checkpoint = %+v, want start=%d next=%d", cp, start, head+1)
	}
	if got := logscanCountLogsBetween(t, ctx, pool, chainID, 0, start-1); got != 0 {
		t.Fatalf("wrote %d rows below the independent start, want 0", got)
	}
	if got := logscanCountLogs(t, ctx, pool, chainID); got != 1 {
		t.Fatalf("total logs = %d, want 1", got)
	}
	if got := logscanCountLogsAt(t, ctx, pool, chainID, 7); got != 1 {
		t.Fatalf("logs at height 7 = %d, want 1", got)
	}
	queries := fake.recordedQueries()
	if len(queries) == 0 {
		t.Fatal("no eth_getLogs queries recorded")
	}
	if queries[0] != (logscanQuery{from: start, to: 7}) {
		t.Fatalf("first query = %+v, want [%d,7]", queries[0], start)
	}
	for _, q := range queries {
		if q.from < start {
			t.Fatalf("queried below the configured start: %+v", q)
		}
		if q.to > head {
			t.Fatalf("queried past 002 coverage: %+v", q)
		}
	}
}

// --- scene #2: duplicate/shuffled convergence -------------------------------

// TestLogScanDuplicateAndShuffledConverges covers quickstart scene #2
// (FR-09/SC-02): a shuffled response carrying an exact duplicate converges to
// one row per identity, and replaying the committed interval through the write
// protocol adds zero rows.
func TestLogScanDuplicateAndShuffledConverges(t *testing.T) {
	dsn := startIndexerPostgres(t)
	ctx := context.Background()
	pool := openIndexerPool(t, dsn)
	defer pool.Close()

	const chainID = int64(32601)
	chain := newScriptChain(chainID, 2, 0xC1)
	logscanSeedBlocks(t, ctx, pool, chainID, chain, 0, 2)
	logscanSet002Checkpoint(t, ctx, pool, chainID, chain, 2)

	contract := common.HexToAddress(logscanContractA)
	emits := logscanEmitMap(t, chain, contract, map[uint64]uint64{0: 1, 1: 2, 2: 3})
	shuffled := []map[string]any{
		emits[2],
		emits[1],
		logscanCloneLog(emits[1]), // exact repeated identity and content
		emits[0],
	}
	fake := logscanNewFakeRPC(t, logscanRangeLogs(emits))
	fake.scriptNext(&logscanResponse{logs: shuffled})

	lease := logscanLease(t, ctx, pool, chainID, "replay")
	ls := logscanNewScanner(t, pool, chain, fake.dial(t), lease, LogConfig{
		StartBlock:  0,
		Contracts:   []string{logscanContractA},
		ConfigHash:  logscanConfigHashA,
		BatchBlocks: 3,
	})
	logscanServeTo(t, ctx, pool, chainID, ls, 3, 30*time.Second)

	var rows, distinct int
	if err := pool.QueryRow(ctx, `
SELECT count(*), count(DISTINCT (block_hash, tx_hash, log_index))
FROM erc20_transfer_logs WHERE chain_id = $1`, chainID).Scan(&rows, &distinct); err != nil {
		t.Fatalf("count/log identities: %v", err)
	}
	if rows != 3 || distinct != 3 {
		t.Fatalf("stored rows = %d (distinct %d), want 3 shuffled logs deduped to 3", rows, distinct)
	}
	for _, h := range []uint64{0, 1, 2} {
		if n := logscanCountLogsAt(t, ctx, pool, chainID, h); n != 1 {
			t.Fatalf("height %d rows = %d, want 1", h, n)
		}
	}
	if cp, ok := logscanReadCheckpoint(t, ctx, pool, chainID); !ok || cp.start != 0 || cp.next != 3 {
		t.Fatalf("checkpoint = %+v, want start=0 next=3", cp)
	}

	// Replay the identical interval: the in-memory dedup converges, and the
	// durable guard refuses a second delivery with zero new rows.
	rcap := testRecoveryCap(t, ctx, pool, chainID)
	coverage := logscanCoverage(t, chain, 0, 2)
	typesLogs := make([]types.Log, 0, len(shuffled))
	for _, want := range []uint64{2, 1, 0} {
		typesLogs = append(typesLogs, logscanTypesLog(t, chain, want, 0, contract, map[uint64]uint64{0: 1, 1: 2, 2: 3}[want]))
	}
	replayed, err := ls.validateLogs(0, 2, coverage, typesLogs)
	if err != nil {
		t.Fatalf("validateLogs(shuffled replay) = %v, want nil", err)
	}
	if len(replayed) != 3 {
		t.Fatalf("replayed batch = %d rows, want 3 after dedup", len(replayed))
	}
	before := logscanSnapshot(t, ctx, pool, chainID)
	if err := ls.commitLogRange(ctx, 0, 2, true, coverage, replayed, rcap); !errors.Is(err, errStaleState) {
		t.Fatalf("replayed interval = %v, want errStaleState", err)
	}
	logscanRequireStateUnchanged(t, before, logscanSnapshot(t, ctx, pool, chainID))
}

// --- scene #2: conflict fails the whole batch --------------------------------

// TestLogScanConflictFailsWholeBatch covers quickstart scene #2 conflict and
// FR-10's second sentence: a batch whose stored identity carries different
// bytes is rejected as a whole (no partial writes, checkpoint unmoved), even
// when another row of the batch is new.
func TestLogScanConflictFailsWholeBatch(t *testing.T) {
	dsn := startIndexerPostgres(t)
	ctx := context.Background()
	pool := openIndexerPool(t, dsn)
	defer pool.Close()

	const chainID = int64(32602)
	chain := newScriptChain(chainID, 2, 0xC2)
	logscanSeedBlocks(t, ctx, pool, chainID, chain, 0, 2)
	logscanSet002Checkpoint(t, ctx, pool, chainID, chain, 2)

	contract := common.HexToAddress(logscanContractA)
	emits := logscanEmitMap(t, chain, contract, map[uint64]uint64{0: 1, 1: 2, 2: 3})
	fake := logscanNewFakeRPC(t, logscanRangeLogs(emits))
	lease := logscanLease(t, ctx, pool, chainID, "conflict")
	ls := logscanNewScanner(t, pool, chain, fake.dial(t), lease, LogConfig{
		StartBlock:  0,
		Contracts:   []string{logscanContractA},
		ConfigHash:  logscanConfigHashA,
		BatchBlocks: 3,
	})

	// First delivery commits [0,2] normally (3 stored rows).
	logscanServeTo(t, ctx, pool, chainID, ls, 3, 30*time.Second)
	orig, ok := logscanReadRowAt(t, ctx, pool, chainID, 0)
	if !ok {
		t.Fatal("height 0 row missing after the first delivery")
	}
	// Rewind the cursor to replay the interval (the only way to re-deliver
	// committed identities through the write protocol).
	if _, err := pool.Exec(ctx, `UPDATE log_checkpoint SET next_block = 0 WHERE chain_id = $1`, chainID); err != nil {
		t.Fatalf("rewind log checkpoint: %v", err)
	}

	// The batch carries the same identity at height 0 with one changed byte
	// (its stored row must win) plus a genuinely new identity at height 1.
	rcap := testRecoveryCap(t, ctx, pool, chainID)
	conflict := logscanTypesLog(t, chain, 0, 0, contract, 999)
	fresh := logscanTypesLog(t, chain, 1, 1, contract, 7)
	coverage := logscanCoverage(t, chain, 0, 2)
	rows, err := ls.validateLogs(0, 2, coverage, []types.Log{conflict, fresh})
	if err != nil {
		t.Fatalf("validateLogs(conflict batch) = %v, want nil (conflict is a DB verdict)", err)
	}
	err = ls.commitLogRange(ctx, 0, 2, false, coverage, rows, rcap)
	wantClass(t, err, classIdentityConflict)
	if !errors.Is(err, errStaleState) && !errors.As(err, new(*logValidationError)) {
		t.Fatalf("commit conflict error = %v, want *logValidationError", err)
	}

	after, ok := logscanReadRowAt(t, ctx, pool, chainID, 0)
	if !ok || after.data != orig.data {
		t.Fatalf("stored height 0 row changed: got %+v, want data %s (never overwritten)", after, orig.data)
	}
	var freshRows int
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM erc20_transfer_logs WHERE chain_id = $1 AND tx_hash = $2`,
		chainID, logscanTxHash(1, 1)).Scan(&freshRows); err != nil {
		t.Fatalf("count fresh identity: %v", err)
	}
	if freshRows != 0 {
		t.Fatalf("fresh row of the failed batch leaked: %d rows, want 0", freshRows)
	}
	if got := logscanCountLogs(t, ctx, pool, chainID); got != 3 {
		t.Fatalf("total rows = %d, want 3 (whole batch rolled back)", got)
	}
	cp, ok := logscanReadCheckpoint(t, ctx, pool, chainID)
	if !ok || cp.next != 0 {
		t.Fatalf("checkpoint next = %+v, want the rewound 0 (failed batch must not advance)", cp)
	}
}

// --- scene #3: crash and uncertain-commit recovery ---------------------------

// TestLogScanCrashAndRestartRecovery covers quickstart scene #3 (FR-11/FR-16,
// SC-03): cancel while the log query is in flight, cancel while the commit
// transaction is blocked, and a lost COMMIT acknowledgment followed by a
// restart that rebuilds from the durable checkpoint.
func TestLogScanCrashAndRestartRecovery(t *testing.T) {
	dsn := startIndexerPostgres(t)
	ctx := context.Background()
	pool := openIndexerPool(t, dsn)
	defer pool.Close()

	t.Run("cancel during query", func(t *testing.T) {
		const chainID = int64(32401)
		chain := newScriptChain(chainID, 2, 0xD1)
		logscanSeedBlocks(t, ctx, pool, chainID, chain, 0, 2)
		logscanSet002Checkpoint(t, ctx, pool, chainID, chain, 2)
		lease := logscanLease(t, ctx, pool, chainID, "crash-query")

		gate := make(chan struct{})
		var releaseOnce sync.Once
		release := func() { releaseOnce.Do(func() { close(gate) }) }
		defer release()
		fake := logscanNewFakeRPC(t, nil)
		fake.scriptNext(&logscanResponse{gate: gate})
		ls := logscanNewScanner(t, pool, chain, fake.dial(t), lease, LogConfig{
			StartBlock: 0, Contracts: []string{logscanContractA}, ConfigHash: logscanConfigHashA, BatchBlocks: 3,
		})

		rctx, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- ls.ServeLoop(rctx, nil) }()
		waitUntil(t, time.Now().Add(10*time.Second), "the gated log query to arrive", func() bool {
			return fake.queryCount() >= 1
		})
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("ServeLoop() after cancel = %v, want nil", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("ServeLoop() did not return after cancel")
		}
		release()
		if got := logscanCountLogs(t, ctx, pool, chainID); got != 0 {
			t.Fatalf("cancelled query left %d rows, want 0", got)
		}
		if _, ok := logscanReadCheckpoint(t, ctx, pool, chainID); ok {
			t.Fatal("cancelled query created a checkpoint, want empty progress")
		}

		// A fresh instance recovers from the durable (empty) state and
		// completes the same range with no gap.
		emits := logscanEmitMap(t, chain, common.HexToAddress(logscanContractA), map[uint64]uint64{1: 9})
		fake2 := logscanNewFakeRPC(t, logscanRangeLogs(emits))
		ls2 := logscanNewScanner(t, pool, chain, fake2.dial(t), lease, LogConfig{
			StartBlock: 0, Contracts: []string{logscanContractA}, ConfigHash: logscanConfigHashA, BatchBlocks: 3,
		})
		logscanServeTo(t, ctx, pool, chainID, ls2, 3, 30*time.Second)
		if got := logscanCountLogsAt(t, ctx, pool, chainID, 1); got != 1 {
			t.Fatalf("recovery rows at height 1 = %d, want 1", got)
		}
		if cp, ok := logscanReadCheckpoint(t, ctx, pool, chainID); !ok || cp.start != 0 || cp.next != 3 {
			t.Fatalf("recovery checkpoint = %+v, want start=0 next=3", cp)
		}
	})

	t.Run("cancel during commit transaction", func(t *testing.T) {
		const chainID = int64(32402)
		chain := newScriptChain(chainID, 2, 0xD2)
		logscanSeedBlocks(t, ctx, pool, chainID, chain, 0, 2)
		logscanSet002Checkpoint(t, ctx, pool, chainID, chain, 2)
		lease := logscanLease(t, ctx, pool, chainID, "crash-commit")

		emits := logscanEmitMap(t, chain, common.HexToAddress(logscanContractA), map[uint64]uint64{1: 5})
		fake := logscanNewFakeRPC(t, logscanRangeLogs(emits))
		ls := logscanNewScanner(t, pool, chain, fake.dial(t), lease, LogConfig{
			StartBlock: 0, Contracts: []string{logscanContractA}, ConfigHash: logscanConfigHashA, BatchBlocks: 3,
		})

		// Hold the coordination lock from an external session so the commit
		// provably blocks after its RPC work is done.
		conn, err := pool.Acquire(ctx)
		if err != nil {
			t.Fatalf("acquire lock session: %v", err)
		}
		defer conn.Release()
		var holderPID int32
		if err := conn.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&holderPID); err != nil {
			t.Fatalf("holder pid: %v", err)
		}
		htx, err := conn.Begin(ctx)
		if err != nil {
			t.Fatalf("begin holder transaction: %v", err)
		}
		defer func() { _ = htx.Rollback(ctx) }()
		if _, err := htx.Exec(ctx, ensureLeaseSQL, chainID, lease.ownerID, lease.Token(), lease.ttl.Seconds()); err != nil {
			t.Fatalf("holder ensure coordination row: %v", err)
		}
		var (
			owner string
			token int64
			valid bool
		)
		if err := htx.QueryRow(ctx, lockCoordSQL, chainID).Scan(&owner, &token, &valid); err != nil {
			t.Fatalf("holder lock coordination row: %v", err)
		}

		rctx, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- ls.ServeLoop(rctx, nil) }()
		waitForBlockedBy(t, ctx, pool, holderPID, 10*time.Second)
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("ServeLoop() after cancel = %v, want nil", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("ServeLoop() did not return after cancel")
		}
		if got := logscanCountLogs(t, ctx, pool, chainID); got != 0 {
			t.Fatalf("interrupted transaction left %d rows, want 0", got)
		}
		if _, ok := logscanReadCheckpoint(t, ctx, pool, chainID); ok {
			t.Fatal("interrupted transaction created a checkpoint, want empty progress")
		}

		if err := htx.Rollback(ctx); err != nil {
			t.Fatalf("release holder lock: %v", err)
		}
		ls2 := logscanNewScanner(t, pool, chain, fake.dial(t), lease, LogConfig{
			StartBlock: 0, Contracts: []string{logscanContractA}, ConfigHash: logscanConfigHashA, BatchBlocks: 3,
		})
		logscanServeTo(t, ctx, pool, chainID, ls2, 3, 30*time.Second)
		if got := logscanCountLogsAt(t, ctx, pool, chainID, 1); got != 1 {
			t.Fatalf("recovery rows at height 1 = %d, want 1", got)
		}
		if cp, ok := logscanReadCheckpoint(t, ctx, pool, chainID); !ok || cp.start != 0 || cp.next != 3 {
			t.Fatalf("recovery checkpoint = %+v, want start=0 next=3", cp)
		}
	})

	t.Run("commit acknowledgment lost then restart from DB", func(t *testing.T) {
		const chainID = int64(32403)
		chain := newScriptChain(chainID, 3, 0xD3)
		logscanSeedBlocks(t, ctx, pool, chainID, chain, 0, 3)
		logscanSet002Checkpoint(t, ctx, pool, chainID, chain, 1) // only [0,1] covered at first

		emits := logscanEmitMap(t, chain, common.HexToAddress(logscanContractA), map[uint64]uint64{0: 1, 1: 2, 2: 3, 3: 4})
		fake := logscanNewFakeRPC(t, logscanRangeLogs(emits))

		dropPool, drop := logscanOpenCommitDropPool(t, dsn)
		lease := logscanLease(t, ctx, dropPool, chainID, "uncertain-commit")
		ls := logscanNewScanner(t, dropPool, chain, fake.dial(t), lease, LogConfig{
			StartBlock: 0, Contracts: []string{logscanContractA}, ConfigHash: logscanConfigHashA, BatchBlocks: 2,
		})

		drop.arm.Store(true)
		rctx, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- ls.ServeLoop(rctx, nil) }()
		waitUntil(t, time.Now().Add(15*time.Second), "the COMMIT reply to be dropped", func() bool {
			return drop.dropped.Load() == 1
		})
		// The forwarded COMMIT landed: the durable checkpoint moved even though
		// the worker saw an error. The retry must accept DB truth, not memory.
		waitUntil(t, time.Now().Add(15*time.Second), "the retry to settle on durable progress", func() bool {
			next, ok := logscanCheckpointNext(ctx, pool, chainID)
			return ok && next == 2
		})
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("ServeLoop() after cancel = %v, want nil", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("ServeLoop() did not return after cancel")
		}
		if got := logscanCountLogs(t, ctx, pool, chainID); got != 2 {
			t.Fatalf("phase 1 rows = %d, want 2 (the uncertain commit is durable)", got)
		}

		// Restart after 002 covers more: the fresh worker must continue from
		// the durable next_block=2, never from its in-memory zero.
		logscanSet002Checkpoint(t, ctx, pool, chainID, chain, 3)
		ls2 := logscanNewScanner(t, dropPool, chain, fake.dial(t), lease, LogConfig{
			StartBlock: 0, Contracts: []string{logscanContractA}, ConfigHash: logscanConfigHashA, BatchBlocks: 2,
		})
		logscanServeTo(t, ctx, pool, chainID, ls2, 4, 30*time.Second)
		for n := uint64(0); n <= 3; n++ {
			if got := logscanCountLogsAt(t, ctx, pool, chainID, n); got != 1 {
				t.Fatalf("restart rows at height %d = %d, want exactly 1", n, got)
			}
		}
		if cp, ok := logscanReadCheckpoint(t, ctx, pool, chainID); !ok || cp.start != 0 || cp.next != 4 {
			t.Fatalf("restart checkpoint = %+v, want start=0 next=4", cp)
		}
	})
}

// --- scene #4: backoff and range shrinking -----------------------------------

// TestLogScanBackoffAndShrink covers quickstart scene #4 (FR-12/FR-13,
// SC-04): timeout, rate limit and parse failure back off without advancing; an
// over-wide query is discarded and re-queried from the same height with a
// shrunk span, and the failed range is never skipped.
func TestLogScanBackoffAndShrink(t *testing.T) {
	dsn := startIndexerPostgres(t)
	ctx := context.Background()
	pool := openIndexerPool(t, dsn)
	defer pool.Close()

	const chainID = int64(32701)
	chain := newScriptChain(chainID, 8, 0xE4)
	logscanSeedBlocks(t, ctx, pool, chainID, chain, 0, 8)
	logscanSet002Checkpoint(t, ctx, pool, chainID, chain, 8)
	logscanSeedLogProgress(t, ctx, pool, chainID, 0, logscanConfigHashA, 0)

	emits := logscanEmitMap(t, chain, common.HexToAddress(logscanContractA), map[uint64]uint64{1: 42})
	fake := logscanNewFakeRPC(t, logscanRangeLogs(emits))
	gate := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(gate) }) }
	defer release()
	fake.scriptNext(&logscanResponse{delay: 300 * time.Millisecond})          // timeout
	fake.scriptNext(&logscanResponse{httpStatus: http.StatusTooManyRequests}) // rate limited
	fake.scriptNext(&logscanResponse{raw: "not json"})                        // parse failure
	fake.scriptNext(&logscanResponse{gate: gate, httpStatus: http.StatusRequestEntityTooLarge})

	lease := logscanLease(t, ctx, pool, chainID, "backoff")
	ls := logscanNewScanner(t, pool, chain, fake.dial(t), lease, LogConfig{
		StartBlock:  0,
		Contracts:   []string{logscanContractA},
		ConfigHash:  logscanConfigHashA,
		BatchBlocks: 8,
		RPCTimeout:  100 * time.Millisecond,
	})
	before := logscanSnapshot(t, ctx, pool, chainID)

	rctx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- ls.ServeLoop(rctx, nil) }()
	waitUntil(t, time.Now().Add(20*time.Second), "the three request-class failures", func() bool {
		return fake.queryCount() >= 3
	})
	// The three request failures must not have advanced anything, and the
	// fourth (fetched but gated) query cannot have committed either.
	logscanRequireStateUnchanged(t, before, logscanSnapshot(t, ctx, pool, chainID))
	release()
	waitUntil(t, time.Now().Add(30*time.Second), "the shrunk and full ranges to commit", func() bool {
		next, ok := logscanCheckpointNext(ctx, pool, chainID)
		return ok && next == 9
	})
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ServeLoop() after cancel = %v, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("ServeLoop() did not return after cancel")
	}

	// Exact fail/retry/shrink walk: four failed [0,7] attempts, then the
	// discarded batch is re-queried from the same height with span 4, and the
	// rest of the coverage advances.
	logscanRequireQueries(t, fake.recordedQueries(), []logscanQuery{
		{0, 7}, {0, 7}, {0, 7}, {0, 7}, {0, 3}, {4, 8},
	})
	if n := logscanCountLogsAt(t, ctx, pool, chainID, 1); n != 1 {
		t.Fatalf("logs at the failed-then-shrunk height 1 = %d, want 1 (never skipped)", n)
	}
	if got := logscanCountLogs(t, ctx, pool, chainID); got != 1 {
		t.Fatalf("total logs = %d, want 1", got)
	}
	if cp, ok := logscanReadCheckpoint(t, ctx, pool, chainID); !ok || cp.next != 9 {
		t.Fatalf("checkpoint = %+v, want next=9", cp)
	}
}

// --- scene #5: single-block incompleteness pauses ----------------------------

// TestLogScanSingleBlockIncompletePauses covers quickstart scene #5
// (FR-12/SC-04): when even a single-block query cannot be confirmed complete
// the worker stops, persists range_incomplete, and a restart stays refused.
func TestLogScanSingleBlockIncompletePauses(t *testing.T) {
	dsn := startIndexerPostgres(t)
	ctx := context.Background()
	pool := openIndexerPool(t, dsn)
	defer pool.Close()

	const chainID = int64(32702)
	chain := newScriptChain(chainID, 3, 0xE5)
	logscanSeedBlocks(t, ctx, pool, chainID, chain, 0, 3)
	logscanSet002Checkpoint(t, ctx, pool, chainID, chain, 3)
	logscanSeedLogProgress(t, ctx, pool, chainID, 0, logscanConfigHashA, 1)

	fake := logscanNewFakeRPC(t, nil)
	fake.setDefaultFault(&logscanResponse{httpStatus: http.StatusRequestEntityTooLarge})

	lease := logscanLease(t, ctx, pool, chainID, "single-block")
	cfg := LogConfig{
		StartBlock:  0,
		Contracts:   []string{logscanContractA},
		ConfigHash:  logscanConfigHashA,
		BatchBlocks: 4,
	}
	ls := logscanNewScanner(t, pool, chain, fake.dial(t), lease, cfg)
	before := logscanSnapshot(t, ctx, pool, chainID)

	err := logscanServeUntilPaused(t, ctx, ls, 30*time.Second)
	if !errors.Is(err, errPaused) {
		t.Fatalf("ServeLoop() = %v, want errPaused", err)
	}
	// The shrink walk ends at the single-block floor: 4 -> 2 -> 1 -> stop.
	logscanRequireQueries(t, fake.recordedQueries(), []logscanQuery{{1, 3}, {1, 2}, {1, 1}})
	pause, ok := logscanReadPause(t, ctx, pool, chainID)
	if !ok || pause.kind != pauseRangeIncomplete || pause.height != 1 {
		t.Fatalf("pause row = %+v, want range_incomplete at 1", pause)
	}
	if !strings.Contains(pause.detail, "class="+pauseRangeIncomplete) {
		t.Fatalf("pause detail = %q, want class=%s", pause.detail, pauseRangeIncomplete)
	}
	logscanRequireProgressUnchanged(t, before, logscanSnapshot(t, ctx, pool, chainID))
	// The durable pause row itself is now part of the baseline.
	paused := logscanSnapshot(t, ctx, pool, chainID)

	// Restart: the durable pause row still refuses every advance.
	ls2 := logscanNewScanner(t, pool, chain, fake.dial(t), lease, cfg)
	if err := logscanServeUntilPaused(t, ctx, ls2, 10*time.Second); !errors.Is(err, errPaused) {
		t.Fatalf("restart ServeLoop() = %v, want errPaused", err)
	}
	if got := logscanCountPauses(t, ctx, pool, chainID); got != 1 {
		t.Fatalf("pause rows = %d, want exactly 1", got)
	}
	logscanRequireStateUnchanged(t, paused, logscanSnapshot(t, ctx, pool, chainID))
}

// --- scene #6: invalid logs pause validation ---------------------------------

// TestLogScanInvalidLogsPauseValidation covers quickstart scene #6
// (FR-07/FR-09/SC-05): each invalid-log class fails its whole batch without
// moving the checkpoint; three deterministic re-queries then persist
// validation_failed with the matching detail.class.
func TestLogScanInvalidLogsPauseValidation(t *testing.T) {
	dsn := startIndexerPostgres(t)
	ctx := context.Background()
	pool := openIndexerPool(t, dsn)
	defer pool.Close()

	contract := common.HexToAddress(logscanContractA)
	cases := []struct {
		name  string
		class string
		logs  func(t *testing.T, chain *scriptChain) []map[string]any
	}{
		{"bad address", classBadAddress, func(t *testing.T, chain *scriptChain) []map[string]any {
			l := logscanLogMessage(t, chain, 1, contract, 1)
			l["address"] = strings.ToLower(logscanContractB)
			return []map[string]any{l}
		}},
		{"bad topic", classBadTopics, func(t *testing.T, chain *scriptChain) []map[string]any {
			l := logscanLogMessage(t, chain, 1, contract, 1)
			topics := l["topics"].([]string)
			l["topics"] = []string{"0x" + strings.Repeat("ff", 32), topics[1], topics[2]}
			return []map[string]any{l}
		}},
		{"bad data", classBadData, func(t *testing.T, chain *scriptChain) []map[string]any {
			l := logscanLogMessage(t, chain, 1, contract, 1)
			l["data"] = "0x" + strings.Repeat("00", 31)
			return []map[string]any{l}
		}},
		{"missing field", classMissingField, func(t *testing.T, chain *scriptChain) []map[string]any {
			l := logscanLogMessage(t, chain, 1, contract, 1)
			l["transactionHash"] = "0x" + strings.Repeat("00", 32)
			return []map[string]any{l}
		}},
		{"removed", classRemoved, func(t *testing.T, chain *scriptChain) []map[string]any {
			l := logscanLogMessage(t, chain, 1, contract, 1)
			l["removed"] = true
			return []map[string]any{l}
		}},
		{"out of range", classOutOfRange, func(t *testing.T, chain *scriptChain) []map[string]any {
			l := logscanLogMessage(t, chain, 1, contract, 1)
			l["blockNumber"] = hexutil.EncodeUint64(3)
			return []map[string]any{l}
		}},
		{"identity conflict", classIdentityConflict, func(t *testing.T, chain *scriptChain) []map[string]any {
			first := logscanLogMessage(t, chain, 1, contract, 1)
			second := logscanCloneLog(first)
			second["data"] = hexutil.Encode(common.LeftPadBytes(big.NewInt(2).Bytes(), 32))
			return []map[string]any{first, second}
		}},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			chainID := int64(32900 + i)
			chain := newScriptChain(chainID, 2, byte(0xF0+i))
			logscanSeedBlocks(t, ctx, pool, chainID, chain, 0, 2)
			logscanSet002Checkpoint(t, ctx, pool, chainID, chain, 2)
			// A committed prefix so "checkpoint unchanged" is a real
			// no-advance assertion: the failed range is [1,2].
			logscanSeedLogProgress(t, ctx, pool, chainID, 0, logscanConfigHashA, 1)
			lease := logscanLease(t, ctx, pool, chainID, "invalid-logs")

			logs := tc.logs(t, chain)
			fake := logscanNewFakeRPC(t, func(from, to uint64) []map[string]any {
				if from <= 1 && 1 <= to {
					return logs
				}
				return nil
			})
			ls := logscanNewScanner(t, pool, chain, fake.dial(t), lease, LogConfig{
				StartBlock: 0, Contracts: []string{logscanContractA}, ConfigHash: logscanConfigHashA, BatchBlocks: 3,
			})
			before := logscanSnapshot(t, ctx, pool, chainID)

			if err := logscanServeUntilPaused(t, ctx, ls, 30*time.Second); !errors.Is(err, errPaused) {
				t.Fatalf("ServeLoop() = %v, want errPaused", err)
			}
			if got := fake.queryCount(); got != maxValidationAttempts {
				t.Fatalf("eth_getLogs attempts = %d, want %d (deterministic re-query)", got, maxValidationAttempts)
			}
			pause, ok := logscanReadPause(t, ctx, pool, chainID)
			if !ok || pause.kind != pauseValidationFailed || pause.height != 1 {
				t.Fatalf("pause row = %+v, want validation_failed at 1", pause)
			}
			if !strings.Contains(pause.detail, "class="+tc.class) {
				t.Fatalf("pause detail = %q, want class=%s", pause.detail, tc.class)
			}
			if ls.State() != StatePaused {
				t.Fatalf("scanner state = %v, want StatePaused", ls.State())
			}
			logscanRequireProgressUnchanged(t, before, logscanSnapshot(t, ctx, pool, chainID))
		})
	}
}

// --- scene #7: chain-view divergence pauses ----------------------------------

// TestLogScanChainViewChangedPauses covers quickstart scene #7 (FR-14/FR-15,
// SC-06): a missing canonical block, a header hash drift and a fork that
// happens while the logs are being fetched all stop the stream without any
// commit or advance.
func TestLogScanChainViewChangedPauses(t *testing.T) {
	dsn := startIndexerPostgres(t)
	ctx := context.Background()
	pool := openIndexerPool(t, dsn)
	defer pool.Close()

	t.Run("missing canonical block", func(t *testing.T) {
		const chainID = int64(33001)
		chain := newScriptChain(chainID, 3, 0x71)
		logscanSeedBlocks(t, ctx, pool, chainID, chain, 0, 3)
		logscanSet002Checkpoint(t, ctx, pool, chainID, chain, 3)
		logscanSeedLogProgress(t, ctx, pool, chainID, 0, logscanConfigHashA, 0)
		if _, err := pool.Exec(ctx, `DELETE FROM chain_blocks WHERE chain_id = $1 AND number = 2`, chainID); err != nil {
			t.Fatalf("delete canonical block 2: %v", err)
		}
		lease := logscanLease(t, ctx, pool, chainID, "view-missing")
		ls := logscanNewScanner(t, pool, chain, logscanNewFakeRPC(t, nil).dial(t), lease, LogConfig{
			StartBlock: 0, Contracts: []string{logscanContractA}, ConfigHash: logscanConfigHashA, BatchBlocks: 4,
		})
		before := logscanSnapshot(t, ctx, pool, chainID)
		if err := logscanServeUntilPaused(t, ctx, ls, 20*time.Second); !errors.Is(err, errPaused) {
			t.Fatalf("ServeLoop() = %v, want errPaused", err)
		}
		pause, ok := logscanReadPause(t, ctx, pool, chainID)
		if !ok || pause.kind != pauseChainViewChanged || pause.height != 2 {
			t.Fatalf("pause row = %+v, want chain_view_changed at 2", pause)
		}
		if !strings.Contains(pause.detail, "actual=missing") {
			t.Fatalf("pause detail = %q, want actual=missing", pause.detail)
		}
		logscanRequireProgressUnchanged(t, before, logscanSnapshot(t, ctx, pool, chainID))
	})

	t.Run("header hash drift", func(t *testing.T) {
		const chainID = int64(33002)
		chain := newScriptChain(chainID, 2, 0x72)
		logscanSeedBlocks(t, ctx, pool, chainID, chain, 0, 2)
		logscanSet002Checkpoint(t, ctx, pool, chainID, chain, 2)
		logscanSeedLogProgress(t, ctx, pool, chainID, 0, logscanConfigHashA, 0)

		canonical := logscanCoverage(t, chain, 2, 2)[2]
		drifted := newScriptChain(chainID, 2, 0x73)
		driftedHash := hashHex(drifted.heads[2].Hash())
		chain.mu.Lock()
		chain.heads[2] = drifted.heads[2]
		chain.mu.Unlock()

		lease := logscanLease(t, ctx, pool, chainID, "view-drift")
		ls := logscanNewScanner(t, pool, chain, logscanNewFakeRPC(t, nil).dial(t), lease, LogConfig{
			StartBlock: 0, Contracts: []string{logscanContractA}, ConfigHash: logscanConfigHashA, BatchBlocks: 3,
		})
		before := logscanSnapshot(t, ctx, pool, chainID)
		if err := logscanServeUntilPaused(t, ctx, ls, 20*time.Second); !errors.Is(err, errPaused) {
			t.Fatalf("ServeLoop() = %v, want errPaused", err)
		}
		pause, ok := logscanReadPause(t, ctx, pool, chainID)
		if !ok || pause.kind != pauseChainViewChanged || pause.height != 2 {
			t.Fatalf("pause row = %+v, want chain_view_changed at 2", pause)
		}
		if !strings.Contains(pause.detail, canonical) || !strings.Contains(pause.detail, driftedHash) {
			t.Fatalf("pause detail = %q, want expected=%s actual=%s", pause.detail, canonical, driftedHash)
		}
		logscanRequireProgressUnchanged(t, before, logscanSnapshot(t, ctx, pool, chainID))
	})

	t.Run("fork while fetching logs", func(t *testing.T) {
		const chainID = int64(33003)
		chain := newScriptChain(chainID, 2, 0x74)
		logscanSeedBlocks(t, ctx, pool, chainID, chain, 0, 2)
		logscanSet002Checkpoint(t, ctx, pool, chainID, chain, 2)
		logscanSeedLogProgress(t, ctx, pool, chainID, 0, logscanConfigHashA, 0)

		canonical := logscanCoverage(t, chain, 2, 2)[2]
		drifted := newScriptChain(chainID, 2, 0x75)
		driftedHash := hashHex(drifted.heads[2].Hash())

		// The node's view changes while the log query is in flight: the
		// pre-query end-block check sees the canonical head, the post-query
		// check must see the fork.
		fake := logscanNewFakeRPC(t, nil)
		fake.setOnQuery(func(from, to uint64) {
			if from <= 2 && 2 <= to {
				chain.mu.Lock()
				chain.heads[2] = drifted.heads[2]
				chain.mu.Unlock()
			}
		})
		lease := logscanLease(t, ctx, pool, chainID, "view-fork")
		ls := logscanNewScanner(t, pool, chain, fake.dial(t), lease, LogConfig{
			StartBlock: 0, Contracts: []string{logscanContractA}, ConfigHash: logscanConfigHashA, BatchBlocks: 3,
		})
		before := logscanSnapshot(t, ctx, pool, chainID)
		if err := logscanServeUntilPaused(t, ctx, ls, 20*time.Second); !errors.Is(err, errPaused) {
			t.Fatalf("ServeLoop() = %v, want errPaused", err)
		}
		pause, ok := logscanReadPause(t, ctx, pool, chainID)
		if !ok || pause.kind != pauseChainViewChanged || pause.height != 2 {
			t.Fatalf("pause row = %+v, want chain_view_changed at 2", pause)
		}
		if !strings.Contains(pause.detail, canonical) || !strings.Contains(pause.detail, driftedHash) {
			t.Fatalf("pause detail = %q, want expected=%s actual=%s", pause.detail, canonical, driftedHash)
		}
		logscanRequireProgressUnchanged(t, before, logscanSnapshot(t, ctx, pool, chainID))
	})

	t.Run("missing canonical block from empty progress", func(t *testing.T) {
		// Regression pin for the first-interval branch: with no log_checkpoint
		// row (first=true) and an existing 002 checkpoint, the pause verdict
		// must consult log_checkpoint, not indexer_checkpoint, or the pause
		// can never be persisted.
		const chainID = int64(33004)
		chain := newScriptChain(chainID, 2, 0x76)
		logscanSeedBlocks(t, ctx, pool, chainID, chain, 0, 2)
		logscanSet002Checkpoint(t, ctx, pool, chainID, chain, 2)
		if _, err := pool.Exec(ctx, `DELETE FROM chain_blocks WHERE chain_id = $1 AND number = 1`, chainID); err != nil {
			t.Fatalf("delete canonical block 1: %v", err)
		}
		lease := logscanLease(t, ctx, pool, chainID, "view-missing-first")
		ls := logscanNewScanner(t, pool, chain, logscanNewFakeRPC(t, nil).dial(t), lease, LogConfig{
			StartBlock: 0, Contracts: []string{logscanContractA}, ConfigHash: logscanConfigHashA, BatchBlocks: 3,
		})
		if err := logscanServeUntilPaused(t, ctx, ls, 20*time.Second); !errors.Is(err, errPaused) {
			t.Fatalf("ServeLoop() = %v, want errPaused", err)
		}
		pause, ok := logscanReadPause(t, ctx, pool, chainID)
		if !ok || pause.kind != pauseChainViewChanged || pause.height != 1 {
			t.Fatalf("pause row = %+v, want chain_view_changed at 1", pause)
		}
		if !strings.Contains(pause.detail, "actual=missing") {
			t.Fatalf("pause detail = %q, want actual=missing", pause.detail)
		}
		if got := logscanCountLogs(t, ctx, pool, chainID); got != 0 {
			t.Fatalf("rows = %d, want 0", got)
		}
		if _, ok := logscanReadCheckpoint(t, ctx, pool, chainID); ok {
			t.Fatal("empty-progress pause created a log checkpoint, want empty progress")
		}
	})
}

// --- scene #8: two real workers, one effective advance -----------------------

// TestLogScanTwoWorkersConverge covers quickstart scene #8 (FR-16/SC-07): two
// workers with independent pools and independent lease handles race the same
// progress. Exactly one effective row per height appears, the stale worker
// stops on the lost lease, and its delayed commit writes zero rows.
func TestLogScanTwoWorkersConverge(t *testing.T) {
	dsn := startIndexerPostgres(t)
	ctx := context.Background()
	pool := openIndexerPool(t, dsn) // observer
	defer pool.Close()
	poolA := openIndexerPool(t, dsn)
	defer poolA.Close()
	poolB := openIndexerPool(t, dsn)
	defer poolB.Close()

	const (
		chainID = int64(32801)
		head    = uint64(4)
	)
	chain := newScriptChain(chainID, head, 0xE1)
	logscanSeedBlocks(t, ctx, pool, chainID, chain, 0, head)
	logscanSet002Checkpoint(t, ctx, pool, chainID, chain, head)

	emits := logscanEmitMap(t, chain, common.HexToAddress(logscanContractA), map[uint64]uint64{0: 1, 1: 2, 2: 3, 3: 4, 4: 5})
	fake := logscanNewFakeRPC(t, logscanRangeLogs(emits))
	// The second query blocks, so worker A provably stops at next_block=2
	// before the takeover.
	gate := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(gate) }) }
	defer release()
	fake.scriptNext(&logscanResponse{})
	fake.scriptNext(&logscanResponse{gate: gate})

	cfg := LogConfig{
		StartBlock:  0,
		Contracts:   []string{logscanContractA},
		ConfigHash:  logscanConfigHashA,
		BatchBlocks: 2,
		// Deterministic expiry: the gated block (RPCTimeout) must outlast the
		// 1.5s lease TTL by a wide margin, otherwise the 2s default timeout
		// fires first, worker A retries to success via default logs, keeps
		// renewing, and the lease never expires (correct implementation
		// behavior, but the takeover setup never materializes).
		RPCTimeout: 60 * time.Second,
	}
	leaseA := newTestLease(t, poolA, chainID, "worker-a", 1500*time.Millisecond, 400*time.Millisecond)
	won, tokenA, err := leaseA.Acquire(ctx)
	if err != nil || !won {
		t.Fatalf("worker A lease Acquire() = (won=%v token=%d err=%v), want win", won, tokenA, err)
	}
	scA := logscanNewScanner(t, poolA, chain, fake.dial(t), leaseA, cfg)

	rctxA, cancelA := context.WithCancel(ctx)
	defer cancelA()
	doneA := make(chan error, 1)
	go func() { doneA <- scA.ServeLoop(rctxA, func() error { return leaseA.Renew(rctxA) }) }()
	waitUntil(t, time.Now().Add(20*time.Second), "worker A to commit [0,1] and block on [2,3]", func() bool {
		next, ok := logscanCheckpointNext(ctx, pool, chainID)
		return ok && next == 2 && fake.queryCount() >= 2
	})

	// Real DB-clock expiry, then the takeover: worker B wins with token+1.
	waitUntil(t, time.Now().Add(20*time.Second), "worker A's lease to expire on the DB clock", func() bool {
		var expired bool
		if err := pool.QueryRow(ctx, `SELECT expires_at <= now() FROM indexer_lease WHERE chain_id = $1`, chainID).Scan(&expired); err != nil {
			t.Fatalf("read lease expiry: %v", err)
		}
		return expired
	})
	leaseB := newTestLease(t, poolB, chainID, "worker-b", 1500*time.Millisecond, 400*time.Millisecond)
	won, tokenB, err := leaseB.Acquire(ctx)
	if err != nil || !won {
		t.Fatalf("worker B takeover Acquire() = (won=%v token=%d err=%v), want win", won, tokenB, err)
	}
	if tokenB != tokenA+1 {
		t.Fatalf("takeover token = %d, want %d", tokenB, tokenA+1)
	}
	scB := logscanNewScanner(t, poolB, chain, fake.dial(t), leaseB, cfg)

	rctxB, cancelB := context.WithCancel(ctx)
	defer cancelB()
	doneB := make(chan error, 1)
	go func() {
		doneB <- scB.ServeLoop(rctxB, func() error {
			err := leaseB.Renew(rctxB)
			if errors.Is(err, context.Canceled) {
				return nil // cancellation is a clean stop, not a lost lease
			}
			return err
		})
	}()
	release() // let worker A's in-flight query finish; its commit must now lose

	select {
	case err := <-doneA:
		if !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("stale worker A = %v, want ErrLeaseLost", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("stale worker A did not stop on the lost lease")
	}
	waitUntil(t, time.Now().Add(30*time.Second), "worker B to reach the coverage end", func() bool {
		next, ok := logscanCheckpointNext(ctx, pool, chainID)
		return ok && next == head+1
	})
	cancelB()
	select {
	case err := <-doneB:
		if err != nil {
			t.Fatalf("worker B ServeLoop() = %v, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("worker B did not return after cancel")
	}

	// Effective advance is exactly one per height: no gap, no duplicate.
	for n := uint64(0); n <= head; n++ {
		if got := logscanCountLogsAt(t, ctx, pool, chainID, n); got != 1 {
			t.Fatalf("height %d rows = %d, want exactly 1 effective advance", n, got)
		}
	}
	var rows, distinct int
	if err := pool.QueryRow(ctx, `
SELECT count(*), count(DISTINCT (block_hash, tx_hash, log_index))
FROM erc20_transfer_logs WHERE chain_id = $1`, chainID).Scan(&rows, &distinct); err != nil {
		t.Fatalf("count/log identities: %v", err)
	}
	if rows != int(head)+1 || distinct != rows {
		t.Fatalf("rows = %d (distinct %d), want %d unique", rows, distinct, head+1)
	}
	if cp, ok := logscanReadCheckpoint(t, ctx, pool, chainID); !ok || cp.start != 0 || cp.next != int64(head+1) {
		t.Fatalf("checkpoint = %+v, want start=0 next=%d", cp, head+1)
	}

	// Delayed stale-token commit for the very range the new owner committed:
	// zero rows, zero progress change.
	before := logscanSnapshot(t, ctx, pool, chainID)
	rcap := testRecoveryCap(t, ctx, pool, chainID)
	staleRows := make([]logRow, 0, 3)
	for n := uint64(2); n <= head; n++ {
		chain.mu.Lock()
		hdr := chain.heads[n]
		chain.mu.Unlock()
		staleRows = append(staleRows, logRow{
			blockNumber: n,
			blockHash:   hashHex(hdr.Hash()),
			txHash:      logscanTxHash(n, 0),
			logIndex:    0,
			contract:    logscanContractA,
			topic0:      hashHex(eth.TransferSig),
			topic1:      hashHex(common.BytesToHash(logscanFromAddr.Bytes())),
			topic2:      hashHex(common.BytesToHash(logscanToAddr.Bytes())),
			data:        "0x" + strings.Repeat("00", 32),
		})
	}
	err = scA.commitLogRange(ctx, 2, head, false, logscanCoverage(t, chain, 2, head), staleRows, rcap)
	if !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale delayed commit = %v, want ErrLeaseLost", err)
	}
	logscanRequireStateUnchanged(t, before, logscanSnapshot(t, ctx, pool, chainID))
}

// --- scene #10: configuration change refused ---------------------------------

// TestLogScanConfigChangeRefused covers quickstart scene #10 (FR-04/FR-05,
// SC-09): a restarted worker with a changed start, a changed whitelist or an
// empty whitelist is refused and the durable progress is untouched.
func TestLogScanConfigChangeRefused(t *testing.T) {
	dsn := startIndexerPostgres(t)
	ctx := context.Background()
	pool := openIndexerPool(t, dsn)
	defer pool.Close()

	const chainID = int64(33201)
	chain := newScriptChain(chainID, 2, 0xCA)
	logscanSeedBlocks(t, ctx, pool, chainID, chain, 0, 2)
	logscanSet002Checkpoint(t, ctx, pool, chainID, chain, 2)

	emits := logscanEmitMap(t, chain, common.HexToAddress(logscanContractA), map[uint64]uint64{0: 1, 1: 2, 2: 3})
	fake := logscanNewFakeRPC(t, logscanRangeLogs(emits))

	leaseA := newTestLease(t, pool, chainID, "cfg-a", 1500*time.Millisecond, 400*time.Millisecond)
	if won, token, err := leaseA.Acquire(ctx); err != nil || !won {
		t.Fatalf("baseline lease Acquire() = (won=%v token=%d err=%v), want win", won, token, err)
	}
	lsA := logscanNewScanner(t, pool, chain, fake.dial(t), leaseA, LogConfig{
		StartBlock: 0, Contracts: []string{logscanContractA}, ConfigHash: logscanConfigHashA, BatchBlocks: 3,
	})
	logscanServeTo(t, ctx, pool, chainID, lsA, 3, 30*time.Second)
	before := logscanSnapshot(t, ctx, pool, chainID)
	if before.rows != 3 || !before.hasCP || before.checkpoint.next != 3 {
		t.Fatalf("baseline state = %+v, want 3 rows and next=3", before)
	}

	waitUntil(t, time.Now().Add(20*time.Second), "the baseline lease to expire", func() bool {
		var expired bool
		if err := pool.QueryRow(ctx, `SELECT expires_at <= now() FROM indexer_lease WHERE chain_id = $1`, chainID).Scan(&expired); err != nil {
			t.Fatalf("read lease expiry: %v", err)
		}
		return expired
	})
	leaseB := newTestLease(t, pool, chainID, "cfg-b", 1500*time.Millisecond, 400*time.Millisecond)
	won, tokenB, err := leaseB.Acquire(ctx)
	if err != nil || !won {
		t.Fatalf("restart lease Acquire() = (won=%v token=%d err=%v), want win", won, tokenB, err)
	}

	plans := []struct {
		name string
		cfg  LogConfig
	}{
		{"start changed", LogConfig{
			StartBlock: 2, Contracts: []string{logscanContractA}, ConfigHash: logscanConfigHashA, BatchBlocks: 3,
		}},
		{"whitelist changed", LogConfig{
			StartBlock: 0, Contracts: []string{logscanContractB}, ConfigHash: logscanConfigHashB, BatchBlocks: 3,
		}},
	}
	for _, plan := range plans {
		t.Run(plan.name, func(t *testing.T) {
			ls := logscanNewScanner(t, pool, chain, fake.dial(t), leaseB, plan.cfg)
			err := logscanServeUntilPaused(t, ctx, ls, 10*time.Second)
			if err == nil || !strings.Contains(err.Error(), "log config changed") {
				t.Fatalf("ServeLoop() = %v, want a log-config-change refusal", err)
			}
			logscanRequireStateUnchanged(t, before, logscanSnapshot(t, ctx, pool, chainID))
		})
	}

	t.Run("empty whitelist", func(t *testing.T) {
		queriesBefore := fake.queryCount()
		_, err := NewLogScanner(pool, chain, fake.dial(t), leaseB, LogConfig{
			StartBlock: 0, Contracts: nil, ConfigHash: logscanConfigHashA, BatchBlocks: 3,
		}, logscanLogger())
		if err == nil || !strings.Contains(err.Error(), "empty contract whitelist") {
			t.Fatalf("NewLogScanner(empty whitelist) = %v, want an empty-whitelist refusal", err)
		}
		if got := fake.queryCount(); got != queriesBefore {
			t.Fatalf("empty whitelist sent %d RPC queries, want 0", got-queriesBefore)
		}
		logscanRequireStateUnchanged(t, before, logscanSnapshot(t, ctx, pool, chainID))
	})
}

// --- pause concurrency -------------------------------------------------------

// TestLogScanPauseConcurrency covers T015: two concurrent pause attempts leave
// exactly one row (first pause wins), and a pause whose evidence was overtaken
// by a committed advance writes nothing.
func TestLogScanPauseConcurrency(t *testing.T) {
	dsn := startIndexerPostgres(t)
	ctx := context.Background()
	pool := openIndexerPool(t, dsn) // observer
	defer pool.Close()
	poolA := openIndexerPool(t, dsn)
	defer poolA.Close()
	poolB := openIndexerPool(t, dsn)
	defer poolB.Close()

	t.Run("first pause wins", func(t *testing.T) {
		const chainID = int64(33101)
		chain := newScriptChain(chainID, 1, 0x81)
		logscanSeedBlocks(t, ctx, pool, chainID, chain, 0, 1)
		logscanSet002Checkpoint(t, ctx, pool, chainID, chain, 1)
		logscanSeedLogProgress(t, ctx, pool, chainID, 0, logscanConfigHashA, 0)
		lease := logscanLease(t, ctx, poolA, chainID, "pause-race")

		cfg := LogConfig{StartBlock: 0, Contracts: []string{logscanContractA}, ConfigHash: logscanConfigHashA, BatchBlocks: 2}
		scA := logscanNewScanner(t, poolA, chain, logscanNewFakeRPC(t, nil).dial(t), lease, cfg)
		scB := logscanNewScanner(t, poolB, chain, logscanNewFakeRPC(t, nil).dial(t), lease, cfg)

		canonical := logscanCoverage(t, chain, 1, 1)[1]
		evA := &chainViewError{height: 1, expected: canonical, actual: coordHash(0xAA)}
		evB := &chainViewError{height: 1, expected: canonical, actual: coordHash(0xBB)}

		start := make(chan struct{})
		errs := make(chan error, 2)
		go func() {
			<-start
			errs <- scA.pauseChainView(ctx, 0, false, evA)
		}()
		go func() {
			<-start
			errs <- scB.pauseChainView(ctx, 0, false, evB)
		}()
		close(start)
		for i := 0; i < 2; i++ {
			if err := <-errs; !errors.Is(err, errPaused) {
				t.Fatalf("pause attempt %d = %v, want errPaused", i, err)
			}
		}
		if got := logscanCountPauses(t, ctx, pool, chainID); got != 1 {
			t.Fatalf("pause rows = %d, want exactly 1 (first pause wins)", got)
		}
		pause, ok := logscanReadPause(t, ctx, pool, chainID)
		if !ok || pause.kind != pauseChainViewChanged || pause.height != 1 {
			t.Fatalf("pause row = %+v, want chain_view_changed at 1", pause)
		}
		if !strings.Contains(pause.detail, "actual="+coordHash(0xAA)) && !strings.Contains(pause.detail, "actual="+coordHash(0xBB)) {
			t.Fatalf("pause detail = %q, want one of the two racing evidence rows", pause.detail)
		}
	})

	t.Run("expired pause after progress moved", func(t *testing.T) {
		const chainID = int64(33102)
		// Coverage stops at height 0, so exactly one interval can commit.
		chain := newScriptChain(chainID, 0, 0x82)
		logscanSeedBlocks(t, ctx, pool, chainID, chain, 0, 0)
		logscanSet002Checkpoint(t, ctx, pool, chainID, chain, 0)
		logscanSeedLogProgress(t, ctx, pool, chainID, 0, logscanConfigHashA, 0)

		emits := logscanEmitMap(t, chain, common.HexToAddress(logscanContractA), map[uint64]uint64{0: 1})
		fake := logscanNewFakeRPC(t, logscanRangeLogs(emits))
		lease := logscanLease(t, ctx, pool, chainID, "pause-stale")
		ls := logscanNewScanner(t, pool, chain, fake.dial(t), lease, LogConfig{
			StartBlock: 0, Contracts: []string{logscanContractA}, ConfigHash: logscanConfigHashA, BatchBlocks: 1,
		})
		// The advance commits while another worker is still holding evidence
		// anchored at next_block=0.
		logscanServeTo(t, ctx, pool, chainID, ls, 1, 20*time.Second)
		advanced := logscanSnapshot(t, ctx, pool, chainID)
		if advanced.checkpoint.next != 1 {
			t.Fatalf("checkpoint = %+v, want next=1", advanced.checkpoint)
		}

		ev := &chainViewError{height: 0, expected: logscanCoverage(t, chain, 0, 0)[0], actual: coordHash(0xCC)}
		err := ls.pauseChainView(ctx, 0, false, ev)
		if !errors.Is(err, errPaused) {
			t.Fatalf("stale pause = %v, want errPaused (stopping is unconditional)", err)
		}
		if got := logscanCountPauses(t, ctx, pool, chainID); got != 0 {
			t.Fatalf("stale pause wrote %d rows, want 0", got)
		}
		logscanRequireStateUnchanged(t, advanced, logscanSnapshot(t, ctx, pool, chainID))
	})
}
