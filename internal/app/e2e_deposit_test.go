//go:build e2e

// e2e_deposit_test.go is the T078 core deposit E2E (quickstart; SC-03): a real
// on-chain transaction is indexed by the real 002/003/004/005 pipelines under
// `serve`, its observation and confirmation are committed, the same
// transactions emit the outbox events, and the real publisher and reference
// consumer move them through a real Kafka broker into the PostgreSQL effect.
// No mock substitutes the chain, the database or the broker.
//
// Statement discipline: delivery is at-least-once and processing is
// idempotent; this test never claims cross-system exactly-once.
package app

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/eth"
	"github.com/xtianxx/txharbor/internal/events"
	"github.com/xtianxx/txharbor/internal/testutil"
)

// --- shared E2E scaffolding (also used by e2e_withdrawal_test.go) ----------

const (
	e2eChainID = "31337"
	e2eAsset   = "0x1111111111111111111111111111111111111111"
	e2eWatch   = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	e2eCaller  = int64(1)
)

// e2eStartPostgres boots a real PostgreSQL container with the embedded
// migrations applied. Skips (never passes) when no Docker provider is
// available.
func e2eStartPostgres(t *testing.T) string {
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
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })
	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres connection string: %v", err)
	}
	if err := db.MigrateUp(ctx, db.MigrateOptions{DSN: dsn, LockTimeout: 10 * time.Second, ConnectTimeout: 10 * time.Second}, io.Discard); err != nil {
		t.Fatalf("MigrateUp: %v", err)
	}
	return dsn
}

// e2eOpenPool opens the shared pool over the migrated database.
func e2eOpenPool(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// e2eStartAnvil boots the pinned Anvil image.
func e2eStartAnvil(t *testing.T) string {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "ghcr.io/foundry-rs/foundry:v1.8.1",
			ExposedPorts: []string{"8545/tcp"},
			Entrypoint:   []string{"anvil"},
			Cmd:          []string{"--host", "0.0.0.0", "--port", "8545", "--chain-id", e2eChainID},
			WaitingFor:   wait.ForLog("Listening on"),
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
	return fmt.Sprintf("http://%s:%s", host, port.Port())
}

// e2eStartKafka boots the shared Kafka helper and ensures the canonical topic.
func e2eStartKafka(t *testing.T) *testutil.Kafka {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)
	kafka, err := testutil.StartKafka(context.Background())
	if err != nil {
		t.Fatalf("start kafka: %v", err)
	}
	t.Cleanup(func() { _ = kafka.Close(context.Background()) })
	if err := kafka.EnsureTopic(context.Background(), testutil.KafkaTopic, testutil.KafkaPartitions); err != nil {
		t.Fatalf("ensure topic: %v", err)
	}
	return kafka
}

// e2eStartRedis boots the 013 non-authoritative Redis carrier for the serve
// process (cache + distributed rate limiting). B7 wires both into serve, and
// PD-1 refuses new withdrawal creation while limiting is unavailable, so the
// full-stack tests must provide a real instance instead of a placeholder
// address.
func e2eStartRedis(t *testing.T) *testutil.Redis {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)
	redisCtr, err := testutil.StartRedis(context.Background())
	if err != nil {
		t.Fatalf("start redis: %v", err)
	}
	t.Cleanup(func() { _ = redisCtr.Close(context.Background()) })
	return redisCtr
}

// e2eFreeAddr reserves one loopback address.
func e2eFreeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// e2eGetenv adapts a map to the Deps.Getenv shape.
func e2eGetenv(env map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		v, ok := env[key]
		return v, ok
	}
}

// e2eBaseEnv is the shared command environment: the real chain/indexer
// configuration plus a complete 013 event configuration (Kafka brokers from
// the caller). The Redis address is a shape-valid placeholder: no B5 runtime
// path contacts Redis (the cache/limiter land in B7).
func e2eBaseEnv(dsn, rpcURL, httpAddr string, brokers []string) map[string]string {
	return map[string]string{
		"TXHARBOR_PG_DSN":                  dsn,
		"TXHARBOR_RPC_URL":                 rpcURL,
		"TXHARBOR_CHAIN_ID":                e2eChainID,
		"TXHARBOR_START_HEIGHT":            "0",
		"TXHARBOR_LOG_START_HEIGHT":        "0",
		"TXHARBOR_LOG_CONTRACTS":           e2eAsset,
		"TXHARBOR_DEPOSIT_START_HEIGHT":    "0",
		"TXHARBOR_DEPOSIT_CONTRACTS":       e2eAsset + ":0",
		"TXHARBOR_DEPOSIT_WATCH_ADDRESSES": e2eWatch + ":0",
		"TXHARBOR_CONFIRMATION_DEPTH":      "1",
		"TXHARBOR_REORG_MAX_DEPTH":         "100",
		"TXHARBOR_HTTP_ADDR":               httpAddr,
		// 013 event infrastructure.
		"TXHARBOR_EVENTS_ENABLED":                      "true",
		"TXHARBOR_KAFKA_BROKERS":                       strings.Join(brokers, ","),
		"TXHARBOR_KAFKA_TOPIC":                         testutil.KafkaTopic,
		"TXHARBOR_REDIS_ADDR":                          "127.0.0.1:6379",
		"TXHARBOR_RATELIMIT_NEW_WITHDRAWAL":            "50/10",
		"TXHARBOR_RATELIMIT_WRITE":                     "50/10",
		"TXHARBOR_RATELIMIT_QUERY":                     "50/10",
		"TXHARBOR_RATELIMIT_OPERATOR":                  "50/10",
		"TXHARBOR_RATELIMIT_RPC":                       "50/10",
		"TXHARBOR_EVENTS_CAPACITY_SOFT_LIMIT":          "100000",
		"TXHARBOR_EVENTS_CAPACITY_HARD_LIMIT":          "200000",
		"TXHARBOR_EVENTS_CAPACITY_RESERVE":             "1000",
		"TXHARBOR_EVENTS_CAPACITY_RETENTION":           "168h",
		"TXHARBOR_EVENTS_CAPACITY_MAX_SHUTDOWN_WINDOW": "1h",
		"TXHARBOR_EVENTS_CAPACITY_DRAIN_TARGET_WINDOW": "5m",
		"TXHARBOR_EVENTS_CONSUMER_GAP_WAIT":            "2s",
		"TXHARBOR_EVENTS_CONSUMER_POLL_INTERVAL":       "200ms",
		"TXHARBOR_EVENTS_PUBLISHER_POLL_INTERVAL":      "200ms",
	}
}

// e2eAnvilCall issues one JSON-RPC call against the real node and fails on an
// RPC error.
func e2eAnvilCall(t *testing.T, rpcURL, method string, params ...any) json.RawMessage {
	t.Helper()
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	if err != nil {
		t.Fatalf("anvil %s marshal: %v", method, err)
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		client := &http.Client{Timeout: 10 * time.Second}
		resp, err := client.Post(rpcURL, "application/json", bytes.NewReader(body))
		if err != nil {
			if time.Now().After(deadline) {
				t.Fatalf("anvil %s: %v", method, err)
			}
			time.Sleep(200 * time.Millisecond)
			continue
		}
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		var out struct {
			Result json.RawMessage `json:"result"`
			Error  *struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatalf("anvil %s decode: %v (%s)", method, err, raw)
		}
		if out.Error != nil {
			t.Fatalf("anvil %s error: %s", method, out.Error.Message)
		}
		return out.Result
	}
}

// e2eAnvilAccounts returns the unlocked accounts of the node.
func e2eAnvilAccounts(t *testing.T, rpcURL string) []common.Address {
	t.Helper()
	var accounts []common.Address
	if err := json.Unmarshal(e2eAnvilCall(t, rpcURL, "eth_accounts"), &accounts); err != nil {
		t.Fatalf("eth_accounts: %v", err)
	}
	if len(accounts) == 0 {
		t.Fatal("anvil returned no unlocked accounts")
	}
	return accounts
}

// e2eEmitTransferCode is the runtime bytecode of a contract whose fallback
// emits exactly one Transfer log: LOG3(topic0, topic1, topic2) with 32 bytes
// of data (the logscan fixture shape).
func e2eEmitTransferCode(topics [3]common.Hash, amount *big.Int) []byte {
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

// e2eSendDeposit plants the emitting contract at the asset address and sends
// one transaction to it, returning the transaction hash and its mined height.
func e2eSendDeposit(t *testing.T, rpcURL string, from, asset, watch common.Address, amount *big.Int) (common.Hash, uint64) {
	t.Helper()
	topics := [3]common.Hash{
		eth.TransferSig,
		common.BytesToHash(from.Bytes()),
		common.BytesToHash(watch.Bytes()),
	}
	e2eAnvilCall(t, rpcURL, "anvil_setCode", asset, hexutil.Encode(e2eEmitTransferCode(topics, amount)))
	var txHash common.Hash
	if err := json.Unmarshal(e2eAnvilCall(t, rpcURL, "eth_sendTransaction", map[string]any{
		"from": from, "to": asset, "data": "0x", "gas": "0x30d40",
	}), &txHash); err != nil {
		t.Fatalf("eth_sendTransaction: %v", err)
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		var receipt *struct {
			BlockNumber hexutil.Uint64 `json:"blockNumber"`
		}
		_ = json.Unmarshal(e2eAnvilCall(t, rpcURL, "eth_getTransactionReceipt", txHash), &receipt)
		if receipt != nil {
			return txHash, uint64(receipt.BlockNumber)
		}
		if time.Now().After(deadline) {
			t.Fatalf("transaction %s was not mined", txHash)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// e2eMine mines n blocks and waits for the head to advance.
func e2eMine(t *testing.T, rpcURL string, n uint64) {
	t.Helper()
	e2eAnvilCall(t, rpcURL, "anvil_mine", hexutil.EncodeUint64(n))
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		var head hexutil.Uint64
		if err := json.Unmarshal(e2eAnvilCall(t, rpcURL, "eth_blockNumber"), &head); err == nil && uint64(head) > 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// e2eWait polls cond until it holds or the timeout expires.
func e2eWait(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// e2eWaitHTTP polls one endpoint until it answers with want.
func e2eWaitHTTP(t *testing.T, url string, want int, timeout time.Duration) {
	t.Helper()
	e2eWaitHTTPOrFail(t, url, want, timeout, nil, nil)
}

// e2eWaitHTTPOrFail polls one endpoint until it answers with want; on timeout
// it reports the captured command output for diagnosis.
func e2eWaitHTTPOrFail(t *testing.T, url string, want int, timeout time.Duration, stdout, stderr *e2eBuffer) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	last, lastErr := 0, error(nil)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			last, lastErr = resp.StatusCode, nil
			if last == want {
				return
			}
		} else {
			lastErr = err
		}
		time.Sleep(100 * time.Millisecond)
	}
	diagnostic := ""
	if stdout != nil {
		diagnostic += "\nstdout:\n" + stdout.String()
	}
	if stderr != nil {
		diagnostic += "\nstderr:\n" + stderr.String()
	}
	t.Fatalf("GET %s never reached %d within %s (last=%d err=%v)%s", url, want, timeout, last, lastErr, diagnostic)
}

// e2eRunCommand starts one command in a goroutine and returns its stop
// function plus the exit-code channel.
func e2eRunCommand(ctx context.Context, run func(context.Context) int) (context.CancelFunc, chan int) {
	cmdCtx, cancel := context.WithCancel(ctx)
	done := make(chan int, 1)
	go func() { done <- run(cmdCtx) }()
	return cancel, done
}

// e2eStopCommand cancels one command and asserts a clean stop.
func e2eStopCommand(t *testing.T, cancel context.CancelFunc, done chan int, what string) {
	t.Helper()
	cancel()
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("%s exited with %d, want 0", what, code)
		}
	case <-time.After(30 * time.Second):
		t.Fatalf("%s did not stop after cancellation", what)
	}
}

// e2eBuffer is a concurrency-safe output buffer for command diagnostics.
type e2eBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

// Write implements io.Writer.
func (b *e2eBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

// String returns the captured output.
func (b *e2eBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestE2EDepositChainToConsumer is the T078 acceptance: chain -> index ->
// observation -> confirmation -> event emit/publish/consume.
func TestE2EDepositChainToConsumer(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dsn := e2eStartPostgres(t)
	rpcURL := e2eStartAnvil(t)
	kafka := e2eStartKafka(t)
	redisCtr := e2eStartRedis(t)
	pool := e2eOpenPool(t, dsn)
	accounts := e2eAnvilAccounts(t, rpcURL)
	env := e2eBaseEnv(dsn, rpcURL, e2eFreeAddr(t), kafka.Brokers())
	env["TXHARBOR_REDIS_ADDR"] = redisCtr.HostPort()

	// The real serve process: header/log/deposit/confirmation/recovery loops.
	var serveOut, serveErr e2eBuffer
	serveStop, serveDone := e2eRunCommand(ctx, func(runCtx context.Context) int {
		return Serve(runCtx, Deps{
			Getenv:  e2eGetenv(env),
			Stdout:  &serveOut,
			Stderr:  &serveErr,
			Signals: make(chan os.Signal),
		})
	})
	e2eWaitHTTPOrFail(t, "http://"+env["TXHARBOR_HTTP_ADDR"]+"/readyz", http.StatusOK, 90*time.Second, &serveOut, &serveErr)

	// One real on-chain deposit: a Transfer log to the watched address.
	txHash, blockNumber := e2eSendDeposit(t, rpcURL, accounts[0], common.HexToAddress(e2eAsset), common.HexToAddress(e2eWatch), big.NewInt(1000))
	var (
		observationID string
		logIndex      int64
		blockHash     string
	)
	e2eWait(t, 120*time.Second, "deposit observation", func() bool {
		var scanErr error
		scanErr = pool.QueryRow(ctx, `
SELECT block_hash, log_index
FROM deposit_observations
WHERE chain_id = 31337 AND tx_hash = $1`, txHash.Hex()).
			Scan(&blockHash, &logIndex)
		return scanErr == nil
	})
	observationID = fmt.Sprintf("%d/%s/%s/%d", 31337, blockHash, txHash.Hex(), logIndex)

	// Confirmation: mine past the threshold and wait for the committed
	// conversion (the same transaction emits the confirmed event).
	e2eMine(t, rpcURL, 6)
	e2eWait(t, 120*time.Second, "deposit confirmation", func() bool {
		var status string
		if err := pool.QueryRow(ctx, `
SELECT status FROM deposit_observations
WHERE chain_id = 31337 AND block_hash = $1 AND tx_hash = $2 AND log_index = $3`,
			blockHash, txHash.Hex(), logIndex).Scan(&status); err != nil {
			return false
		}
		return status == "confirmed"
	})

	// The outbox rows exist and carry the business identity; the created
	// event is an evm_log identity (block height is never the identity) and
	// the confirmed event continues the object version.
	var createdID, createdVersion int64
	var createdIdentity, createdBlockHash, createdTxHash string
	if err := pool.QueryRow(ctx, `
SELECT id, aggregate_version, identity_kind, block_hash, tx_hash
FROM outbox_events
WHERE event_type = 'deposit.observation.created' AND tx_hash = $1`, txHash.Hex()).
		Scan(&createdID, &createdVersion, &createdIdentity, &createdBlockHash, &createdTxHash); err != nil {
		t.Fatalf("read deposit.observation.created row: %v", err)
	}
	if createdIdentity != string(events.IdentityKindEVMLog) || createdVersion != 1 {
		t.Fatalf("created event = (identity %s, version %d), want (evm_log, 1)", createdIdentity, createdVersion)
	}
	if createdBlockHash != blockHash || createdTxHash != txHash.Hex() {
		t.Fatalf("created event chain identity = (%s, %s), want (%s, %s)", createdBlockHash, createdTxHash, blockHash, txHash.Hex())
	}
	var confirmedID, confirmedVersion int64
	var confirmedPayload []byte
	if err := pool.QueryRow(ctx, `
SELECT id, aggregate_version, payload
FROM outbox_events
WHERE event_type = 'deposit.confirmation.confirmed' AND aggregate_id = $1`, observationID).
		Scan(&confirmedID, &confirmedVersion, &confirmedPayload); err != nil {
		t.Fatalf("read deposit.confirmation.confirmed row: %v", err)
	}
	if confirmedVersion != 2 {
		t.Fatalf("confirmed event aggregate_version = %d, want 2 (the object version continues)", confirmedVersion)
	}
	var confirmed map[string]any
	if err := json.Unmarshal(confirmedPayload, &confirmed); err != nil {
		t.Fatalf("decode confirmed payload: %v", err)
	}
	if confirmed["policy_version"] == nil || confirmed["confirmed_block_hash"] == nil {
		t.Fatalf("confirmed payload misses its required facts: %v", confirmed)
	}
	// The event never represents an upstream credit: it is the project's own
	// confirmation policy fact.
	var observationAmount string
	if err := pool.QueryRow(ctx, `
SELECT amount::text FROM deposit_observations
WHERE chain_id = 31337 AND block_hash = $1 AND tx_hash = $2 AND log_index = $3`,
		blockHash, txHash.Hex(), logIndex).Scan(&observationAmount); err != nil {
		t.Fatalf("read observation amount: %v", err)
	}
	if observationAmount != "1000" {
		t.Fatalf("observation amount = %s, want 1000", observationAmount)
	}
	var createdPayload []byte
	if err := pool.QueryRow(ctx, `SELECT payload FROM outbox_events WHERE id = $1`, createdID).Scan(&createdPayload); err != nil {
		t.Fatalf("read created payload: %v", err)
	}
	var created map[string]any
	if err := json.Unmarshal(createdPayload, &created); err != nil {
		t.Fatalf("decode created payload: %v", err)
	}
	if created["observation_id"] != observationID || created["state"] != "pending" {
		t.Fatalf("created payload = %v, want the pending observation fact", created)
	}

	// The real publisher drains the outbox into the real broker.
	pubStop, pubDone := e2eRunCommand(ctx, func(runCtx context.Context) int {
		return EventPublisher(runCtx, nil, Deps{Getenv: e2eGetenv(env), Stdout: io.Discard, Stderr: io.Discard})
	})
	e2eWait(t, 120*time.Second, "outbox drain", func() bool {
		var pending int64
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM outbox_events WHERE publish_state = 'pending'`).Scan(&pending); err != nil {
			return false
		}
		return pending == 0
	})
	e2eStopCommand(t, pubStop, pubDone, "event-publisher")

	// The real reference consumer moves both events into its PostgreSQL
	// effect exactly once.
	consumerStop, consumerDone := e2eRunCommand(ctx, func(runCtx context.Context) int {
		return EventConsumer(runCtx, nil, Deps{Getenv: e2eGetenv(env), Stdout: io.Discard, Stderr: io.Discard})
	})
	e2eWait(t, 180*time.Second, "reference consumer effects", func() bool {
		var n int64
		if err := pool.QueryRow(ctx, `
SELECT count(*) FROM ref_consumer_ledger
WHERE consumer_name = $1 AND event_type IN ('deposit.observation.created','deposit.confirmation.confirmed')`,
			events.RefConsumerName).Scan(&n); err != nil {
			return false
		}
		return n == 2
	})
	e2eStopCommand(t, consumerStop, consumerDone, "event-consumer")
	e2eStopCommand(t, serveStop, serveDone, "serve")

	// Exactly-once effects on the reference consumer and the FR-16 boundary
	// statement the evidence carries.
	for _, eventType := range []string{"deposit.observation.created", "deposit.confirmation.confirmed"} {
		var rows int64
		if err := pool.QueryRow(ctx, `
SELECT count(*) FROM ref_consumer_ledger WHERE consumer_name = $1 AND event_type = $2`,
			events.RefConsumerName, eventType).Scan(&rows); err != nil {
			t.Fatalf("count reference ledger rows for %s: %v", eventType, err)
		}
		if rows != 1 {
			t.Fatalf("reference ledger rows for %s = %d, want 1", eventType, rows)
		}
	}
	var inbox int64
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM consumer_inbox WHERE consumer_name = $1`, events.RefConsumerName).Scan(&inbox); err != nil {
		t.Fatalf("count inbox rows: %v", err)
	}
	if inbox != 2 {
		t.Fatalf("inbox rows = %d, want 2", inbox)
	}
	if !strings.Contains(events.ReferenceBoundaryStatement, "external real ledger") {
		t.Fatalf("boundary statement does not state the FR-16 limit: %q", events.ReferenceBoundaryStatement)
	}
	_ = confirmedID
	_ = logIndex
	_ = blockNumber
}
