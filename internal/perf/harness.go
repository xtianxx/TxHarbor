//go:build perf

// harness.go is the T076 performance harness (test-support; FR-25; adr.md §3;
// verification.md §4; PD-3): the controlled A/B environment, the fixed load
// generator, the fault timeline, the durable/metric sampling and the report
// renderer used by bench_test.go.
//
// Controlled variables (adr.md §3): same host, same PostgreSQL image/spec,
// same Anvil image, same dataset and initial state, the same load generator
// (fixed rate ladder and burst) and the same fault timeline for both paths.
//
// Path A (PG-only): PostgreSQL + Anvil, 013 wiring off. Producers still append
// outbox rows (the 013 Append integration is unconditional; adr.md §3 path A
// explicitly allows "events accumulate in the outbox but are never delivered"),
// no publisher/consumer run, no Redis/Kafka exist.
//
// Path B (full stack): PostgreSQL + Anvil + Redis + Kafka, caching, rate
// limiting, capacity guard, publisher and consumer on.
//
// Discipline: every measurement is taken from the real service (HTTP listener,
// real publisher/consumer command loops, PostgreSQL, Kafka) and recorded as
// measured; a value that could not be measured is marked "待测/待裁决" by the
// renderer and is never rendered as "已达标".
package perf

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/xtianxx/txharbor/internal/app"
	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/eth"
	"github.com/xtianxx/txharbor/internal/events"
	"github.com/xtianxx/txharbor/internal/logx"
	"github.com/xtianxx/txharbor/internal/testutil"
	"github.com/xtianxx/txharbor/internal/withdrawal"
)

// --- fixed benchmark profile -------------------------------------------------

// Path is the closed path vocabulary.
type Path string

const (
	// PathPGOnly is the "persistent store only" baseline path (A).
	PathPGOnly Path = "pg_only"
	// PathFullStack is the full-stack path (B).
	PathFullStack Path = "full_stack"
)

// FaultState is the closed five-state fault timeline vocabulary (the same
// sequence the B9 drills use; adr.md §3).
type FaultState string

const (
	StateNormal    FaultState = "normal"
	StateRedisDown FaultState = "redis_down"
	StateKafkaDown FaultState = "kafka_down"
	StateDual      FaultState = "dual"
	StateRecovery  FaultState = "recovery"
)

// StateSequence is the fixed fault timeline: identical for both paths.
var StateSequence = []FaultState{StateNormal, StateRedisDown, StateKafkaDown, StateDual, StateRecovery}

// StateLabel renders one state for the report.
var StateLabel = map[FaultState]string{
	StateNormal:    "正常（Redis/Kafka 可用）",
	StateRedisDown: "仅 Redis 故障",
	StateKafkaDown: "仅 Kafka 故障",
	StateDual:      "双故障（Redis 停 + Kafka 挂起）",
	StateRecovery:  "恢复追赶",
}

// LadderStep is one step of the fixed query rate ladder.
type LadderStep struct {
	Offset time.Duration
	RPS    int
}

// BurstSpec is the fixed burst inside every state window.
type BurstSpec struct {
	At   time.Duration
	Size int
}

// Profile fixes the load generator and the fault-timeline windows. The
// committed report always uses the fixed default; an override exists only for
// harness debugging and is recorded in the environment spec.
type Profile struct {
	Warmup      time.Duration
	StateWindow time.Duration
	// QueryLadder is the fixed query-class rate ladder, restarted at every
	// state window.
	QueryLadder []LadderStep
	// Burst fires BurstSize immediate query requests at Burst.At inside every
	// window (in addition to the ladder).
	Burst BurstSpec
	// CreateRPS is the fixed withdrawal-create rate for the whole window.
	CreateRPS int
	// DepositRPS is the fixed chain-deposit rate for the whole window.
	DepositRPS int
}

// BenchProfile is the fixed, committed benchmark profile.
var BenchProfile = Profile{
	Warmup:      8 * time.Second,
	StateWindow: 12 * time.Second,
	QueryLadder: []LadderStep{
		{Offset: 0, RPS: 10},
		{Offset: 4 * time.Second, RPS: 25},
		{Offset: 8 * time.Second, RPS: 50},
	},
	Burst:      BurstSpec{At: 8 * time.Second, Size: 50},
	CreateRPS:  2,
	DepositRPS: 1,
}

// Bench-scale fixed inputs. These are benchmark test inputs (like the B9 drill
// bench-scale limits), never proposed production thresholds: production
// capacity/rate thresholds remain 待测/待裁决 (FR-20/24/25).
const (
	BenchPGImage      = "postgres:18.6-trixie"
	BenchAnvilImage   = "ghcr.io/foundry-rs/foundry:v1.8.1"
	BenchChainID      = int64(31337)
	BenchAsset        = "0x1111111111111111111111111111111111111111"
	BenchWatch        = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	BenchRecipient    = "0x3333333333333333333333333333333333333333"
	BenchCaller       = int64(13)
	BenchAmount       = "1000"
	BenchDepositValue = int64(1000)
	BenchSeedRequests = 20
	BenchGrantPool    = 360
	BenchSoftLimit    = "100000"
	BenchHardLimit    = "200000"
	BenchReserve      = "1000"
	benchConcurrency  = 64
)

// PathFromEnv reads the optional debug knobs. The committed report uses the
// fixed BenchProfile unless TXHARBOR_PERF_STATE_WINDOW overrides it (recorded
// in the environment spec).
func benchProfileFromEnv() Profile {
	p := BenchProfile
	if raw := os.Getenv("TXHARBOR_PERF_STATE_WINDOW"); raw != "" {
		if d, err := time.ParseDuration(raw); err == nil && d > 0 {
			p.StateWindow = d
		}
	}
	if raw := os.Getenv("TXHARBOR_PERF_WARMUP"); raw != "" {
		if d, err := time.ParseDuration(raw); err == nil && d > 0 {
			p.Warmup = d
		}
	}
	return p
}

// --- minimal Anvil primitive -------------------------------------------------

// Anvil is one real local chain (the same pinned image the drill/E2E layers
// use).
type Anvil struct {
	ctr testcontainers.Container
	URL string
}

func startAnvil(ctx context.Context) (*Anvil, error) {
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        BenchAnvilImage,
			ExposedPorts: []string{"8545/tcp"},
			Entrypoint:   []string{"anvil"},
			Cmd:          []string{"--host", "0.0.0.0", "--port", "8545", "--chain-id", "31337"},
			WaitingFor:   wait.ForLog("Listening on"),
		},
		Started: true,
	})
	if err != nil {
		return nil, fmt.Errorf("start anvil: %w", err)
	}
	host, err := ctr.Host(ctx)
	if err != nil {
		_ = ctr.Terminate(context.Background())
		return nil, fmt.Errorf("anvil host: %w", err)
	}
	port, err := ctr.MappedPort(ctx, "8545/tcp")
	if err != nil {
		_ = ctr.Terminate(context.Background())
		return nil, fmt.Errorf("anvil port: %w", err)
	}
	return &Anvil{ctr: ctr, URL: fmt.Sprintf("http://%s:%s", host, port.Port())}, nil
}

func (a *Anvil) terminate(ctx context.Context) error {
	if a == nil || a.ctr == nil {
		return nil
	}
	return a.ctr.Terminate(ctx)
}

func (a *Anvil) call(method string, params ...any) (json.RawMessage, error) {
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	if err != nil {
		return nil, fmt.Errorf("anvil %s marshal: %w", method, err)
	}
	deadline := time.Now().Add(20 * time.Second)
	var lastErr error
	for {
		client := &http.Client{Timeout: 10 * time.Second}
		resp, err := client.Post(a.URL, "application/json", bytes.NewReader(body))
		if err != nil {
			lastErr = err
		} else {
			raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
			resp.Body.Close()
			var out struct {
				Result json.RawMessage `json:"result"`
				Error  *struct {
					Message string `json:"message"`
				} `json:"error"`
			}
			if err := json.Unmarshal(raw, &out); err != nil {
				return nil, fmt.Errorf("anvil %s decode: %w (%s)", method, err, raw)
			}
			if out.Error != nil {
				return nil, fmt.Errorf("anvil %s error: %s", method, out.Error.Message)
			}
			return out.Result, nil
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("anvil %s: %w", method, lastErr)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (a *Anvil) accounts() ([]common.Address, error) {
	raw, err := a.call("eth_accounts")
	if err != nil {
		return nil, err
	}
	var accounts []common.Address
	if err := json.Unmarshal(raw, &accounts); err != nil {
		return nil, fmt.Errorf("eth_accounts: %w", err)
	}
	if len(accounts) == 0 {
		return nil, fmt.Errorf("anvil returned no unlocked accounts")
	}
	return accounts, nil
}

// emitTransferCode is the runtime bytecode of a contract whose fallback emits
// exactly one Transfer log (LOG3) — the 003/004 fixture shape.
func emitTransferCode(topics [3]common.Hash, amount int64) []byte {
	var code []byte
	if amount != 0 {
		code = append(code, 0x7f) // PUSH32 amount
		code = append(code, common.BigToHash(big.NewInt(amount)).Bytes()...)
		code = append(code, 0x60, 0x00, 0x52) // PUSH1 0; MSTORE
	}
	for _, topic := range [3]common.Hash{topics[2], topics[1], topics[0]} {
		code = append(code, 0x7f) // PUSH32 topic
		code = append(code, topic.Bytes()...)
	}
	code = append(code, 0x60, 0x20, 0x60, 0x00, 0xa3, 0x00) // PUSH1 32; PUSH1 0; LOG3; STOP
	return code
}

// emitDeposit plants the emitting contract at the asset address and sends one
// transaction to it, returning the mined height. It is a real on-chain fact;
// nothing off-chain fabricates an observation.
func (a *Anvil) emitDeposit(from, asset, watch common.Address, amount int64) (common.Hash, uint64, error) {
	topics := [3]common.Hash{
		eth.TransferSig,
		common.BytesToHash(from.Bytes()),
		common.BytesToHash(watch.Bytes()),
	}
	if _, err := a.call("anvil_setCode", asset, hexutil.Encode(emitTransferCode(topics, amount))); err != nil {
		return common.Hash{}, 0, fmt.Errorf("set emitter code: %w", err)
	}
	raw, err := a.call("eth_sendTransaction", map[string]any{
		"from": from, "to": asset, "data": "0x", "gas": "0x30d40",
	})
	if err != nil {
		return common.Hash{}, 0, fmt.Errorf("eth_sendTransaction: %w", err)
	}
	var txHash common.Hash
	if err := json.Unmarshal(raw, &txHash); err != nil {
		return common.Hash{}, 0, fmt.Errorf("eth_sendTransaction decode: %w", err)
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		raw, err := a.call("eth_getTransactionReceipt", txHash)
		var receipt *struct {
			BlockNumber hexutil.Uint64 `json:"blockNumber"`
		}
		if err == nil {
			_ = json.Unmarshal(raw, &receipt)
		}
		if receipt != nil {
			return txHash, uint64(receipt.BlockNumber), nil
		}
		if time.Now().After(deadline) {
			return common.Hash{}, 0, fmt.Errorf("transaction %s was not mined", txHash)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// --- environment -------------------------------------------------------------

// syncBuffer is a concurrency-safe writer for captured command logs.
type syncBuffer struct {
	mu sync.Mutex
	b  strings.Builder
}

func (w *syncBuffer) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.Write(p)
}

func (w *syncBuffer) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.String()
}

// Env is one running benchmark environment for one path.
type Env struct {
	Path Path
	Run  int

	DSN   string
	Pool  *pgxpool.Pool
	Anvil *Anvil
	Redis *testutil.Redis
	Kafka *testutil.Kafka

	BaseURL  string
	APIKey   string
	QueryIDs []string
	EnvMap   map[string]string

	Evidence *EvidenceWriter
	Spec     EnvSpec

	redisUp bool
	kafkaUp bool

	client *http.Client

	serveCancel context.CancelFunc
	serveDone   chan int
	pubCancel   context.CancelFunc
	consCancel  context.CancelFunc
	pubDone     chan int
	consDone    chan int

	serveOut *syncBuffer
	serveErr *syncBuffer
	logBuf   *syncBuffer
	prevSlog *slog.Logger

	grantSeq  atomic.Int64
	querySeq  atomic.Uint64
	depositMu sync.Mutex

	sender  common.Address
	pgCtr   *postgres.PostgresContainer
	stopped bool
}

// startEnv boots the dependencies for one path, applies the real migrations,
// starts the real serve command and seeds the deterministic dataset.
func startEnv(t *testing.T, ctx context.Context, path Path, run int, profile Profile, evidence *EvidenceWriter) *Env {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	env := &Env{
		Path: path, Run: run, Evidence: evidence,
		serveOut: &syncBuffer{}, serveErr: &syncBuffer{}, logBuf: &syncBuffer{},
		client: &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{
			MaxIdleConnsPerHost: benchConcurrency * 2,
			MaxConnsPerHost:     benchConcurrency * 2,
		}},
	}

	// PostgreSQL: the same image/spec for both paths.
	ctr, err := postgres.Run(ctx, BenchPGImage,
		postgres.WithDatabase("txharbor"),
		postgres.WithUsername("txharbor"),
		postgres.WithPassword("txharbor"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2)),
	)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	env.pgCtr = ctr
	t.Cleanup(env.stop) // safety net; runPath stops the environment explicitly
	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres connection string: %v", err)
	}
	env.DSN = dsn
	if err := db.MigrateUp(ctx, db.MigrateOptions{DSN: dsn, LockTimeout: 10 * time.Second, ConnectTimeout: 10 * time.Second}, io.Discard); err != nil {
		t.Fatalf("migrate up: %v", err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	env.Pool = pool

	anvil, err := startAnvil(ctx)
	if err != nil {
		t.Fatalf("start anvil: %v", err)
	}
	env.Anvil = anvil

	if path == PathFullStack {
		redis, err := testutil.StartRedis(ctx)
		if err != nil {
			t.Fatalf("start redis: %v", err)
		}
		env.Redis = redis
		env.redisUp = true
		kafka, err := testutil.StartKafka(ctx)
		if err != nil {
			t.Fatalf("start kafka: %v", err)
		}
		env.Kafka = kafka
		env.kafkaUp = true
		if err := kafka.EnsureTopic(ctx, testutil.KafkaTopic, testutil.KafkaPartitions); err != nil {
			t.Fatalf("ensure topic: %v", err)
		}
	}

	accounts, err := anvil.accounts()
	if err != nil {
		t.Fatalf("anvil accounts: %v", err)
	}
	env.sender = accounts[0]

	httpAddr := freeLoopbackAddr()
	env.EnvMap = env.buildEnvMap(httpAddr)
	env.BaseURL = "http://" + httpAddr

	// Capture the standard-library logger the scanners use (diagnostics only).
	env.prevSlog = slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(env.logBuf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(env.prevSlog) })

	// The real serve command: HTTP routes, scanners, probes, capacity guard.
	env.serveCancel, env.serveDone = runCommand(ctx, func(runCtx context.Context) int {
		return app.Serve(runCtx, app.Deps{
			Getenv:  envGetenv(env.EnvMap),
			Stdout:  env.serveOut,
			Stderr:  env.serveErr,
			Signals: make(chan os.Signal),
		})
	})
	env.waitReady(t, ctx)

	// The deposit scanner bootstraps the FR-05 policy row from the serve
	// configuration; the create path resolves the allowlist from it.
	if err := waitFor(ctx, 90*time.Second, func() (bool, error) {
		var n int64
		if err := env.Pool.QueryRow(ctx,
			`SELECT count(*) FROM deposit_config_history WHERE chain_id = $1`, BenchChainID).Scan(&n); err != nil {
			return false, err
		}
		return n > 0, nil
	}); err != nil {
		t.Fatalf("deposit config history bootstrap: %v%s", err, env.diagnostics())
	}

	env.seed(t, ctx, env.sender)

	if path == PathFullStack {
		env.startRuntimes(t, ctx)
	}

	env.Spec = env.envSpec(profile)
	if err := env.Evidence.WriteJSON(fmt.Sprintf("env_spec_%s_run%d.json", path, run), env.Spec); err != nil {
		t.Fatalf("write env spec: %v", err)
	}
	return env
}

// buildEnvMap renders the command environment for one path. Path A carries no
// 013 keys at all (the feature switch is off); path B carries the complete
// fail-closed 013 set with bench-scale inputs (test inputs, not production
// thresholds).
func (e *Env) buildEnvMap(httpAddr string) map[string]string {
	out := map[string]string{
		"TXHARBOR_PG_DSN":                  e.DSN,
		"TXHARBOR_RPC_URL":                 e.Anvil.URL,
		"TXHARBOR_CHAIN_ID":                strconv.FormatInt(BenchChainID, 10),
		"TXHARBOR_START_HEIGHT":            "0",
		"TXHARBOR_LOG_START_HEIGHT":        "0",
		"TXHARBOR_LOG_CONTRACTS":           BenchAsset,
		"TXHARBOR_DEPOSIT_START_HEIGHT":    "0",
		"TXHARBOR_DEPOSIT_CONTRACTS":       BenchAsset + ":0",
		"TXHARBOR_DEPOSIT_WATCH_ADDRESSES": BenchWatch + ":0",
		"TXHARBOR_CONFIRMATION_DEPTH":      "1",
		"TXHARBOR_REORG_MAX_DEPTH":         "100",
		"TXHARBOR_HTTP_ADDR":               httpAddr,
	}
	if e.Path == PathFullStack {
		out["TXHARBOR_EVENTS_ENABLED"] = "true"
		out["TXHARBOR_REDIS_ADDR"] = e.Redis.HostPort()
		out["TXHARBOR_REDIS_TIMEOUT"] = "1s"
		out["TXHARBOR_KAFKA_BROKERS"] = strings.Join(e.Kafka.Brokers(), ",")
		out["TXHARBOR_KAFKA_TOPIC"] = testutil.KafkaTopic
		// Bench-scale limiter inputs: high enough not to deny the fixed load
		// in the healthy state, so the fault-state refusals are attributable
		// to the injected fault (PD-1), not to the limiter budget.
		out["TXHARBOR_RATELIMIT_NEW_WITHDRAWAL"] = "500/100"
		out["TXHARBOR_RATELIMIT_WRITE"] = "500/100"
		out["TXHARBOR_RATELIMIT_QUERY"] = "500/100"
		out["TXHARBOR_RATELIMIT_OPERATOR"] = "500/100"
		out["TXHARBOR_RATELIMIT_RPC"] = "500/100"
		out["TXHARBOR_EVENTS_CAPACITY_SOFT_LIMIT"] = BenchSoftLimit
		out["TXHARBOR_EVENTS_CAPACITY_HARD_LIMIT"] = BenchHardLimit
		out["TXHARBOR_EVENTS_CAPACITY_RESERVE"] = BenchReserve
		out["TXHARBOR_EVENTS_CAPACITY_RETENTION"] = "168h"
		out["TXHARBOR_EVENTS_CAPACITY_MAX_SHUTDOWN_WINDOW"] = "1h"
		out["TXHARBOR_EVENTS_CAPACITY_DRAIN_TARGET_WINDOW"] = "5m"
		out["TXHARBOR_EVENTS_CONSUMER_GAP_WAIT"] = "2s"
		out["TXHARBOR_EVENTS_CONSUMER_POLL_INTERVAL"] = "200ms"
		out["TXHARBOR_EVENTS_PUBLISHER_POLL_INTERVAL"] = "200ms"
	}
	return out
}

func envGetenv(env map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		v, ok := env[key]
		return v, ok
	}
}

func freeLoopbackAddr() string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(fmt.Sprintf("reserve loopback address: %v", err))
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

func runCommand(ctx context.Context, run func(context.Context) int) (context.CancelFunc, chan int) {
	cmdCtx, cancel := context.WithCancel(ctx)
	done := make(chan int, 1)
	go func() { done <- run(cmdCtx) }()
	return cancel, done
}

func (e *Env) waitReady(t *testing.T, ctx context.Context) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case code := <-e.serveDone:
			e.serveDone <- code
			t.Fatalf("serve exited early (code %d)%s", code, e.diagnostics())
		default:
		}
		resp, err := e.client.Get(e.BaseURL + "/readyz")
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("serve never became ready%s", e.diagnostics())
}

func (e *Env) diagnostics() string {
	return fmt.Sprintf("\nserve stdout:\n%s\nserve stderr:\n%s\nscanner logs:\n%s",
		e.serveOut.String(), e.serveErr.String(), e.logBuf.String())
}

// startRuntimes starts the real event-publisher and event-consumer commands.
func (e *Env) startRuntimes(t *testing.T, ctx context.Context) {
	t.Helper()
	deps := app.Deps{
		Getenv:  envGetenv(e.EnvMap),
		Stdout:  io.Discard,
		Stderr:  e.serveErr,
		Signals: make(chan os.Signal),
	}
	e.pubCancel, e.pubDone = runCommand(ctx, func(runCtx context.Context) int {
		return app.EventPublisher(runCtx, nil, deps)
	})
	e.consCancel, e.consDone = runCommand(ctx, func(runCtx context.Context) int {
		return app.EventConsumer(runCtx, nil, deps)
	})
	if err := waitFor(ctx, 90*time.Second, func() (bool, error) {
		if err := e.checkRuntimes(); err != nil {
			return false, err
		}
		var exists bool
		if err := e.Pool.QueryRow(ctx,
			`SELECT to_regclass($1) IS NOT NULL`, events.RefLedgerTable).Scan(&exists); err != nil {
			return false, err
		}
		return exists, nil
	}); err != nil {
		t.Fatalf("runtimes did not start: %v%s", err, e.diagnostics())
	}
}

func (e *Env) checkRuntimes() error {
	check := func(name string, done chan int) error {
		if done == nil {
			return nil
		}
		select {
		case code := <-done:
			done <- code
			return fmt.Errorf("%s exited early (code %d)", name, code)
		default:
			return nil
		}
	}
	if err := check("event-publisher", e.pubDone); err != nil {
		return err
	}
	return check("event-consumer", e.consDone)
}

// checkServe fails a run when the serve command has exited. A dead listener
// silently turns every remaining sample into a transport error, so an invalid
// run must abort loudly with the serve diagnostics instead of recording
// fabricated-looking measurements (SC-11 discipline; verification.md §5).
func (e *Env) checkServe() error {
	if e.serveDone == nil {
		return nil
	}
	select {
	case code := <-e.serveDone:
		e.serveDone <- code
		return fmt.Errorf("serve exited (code %d)", code)
	default:
		return nil
	}
}

// serveAlive reports whether the serve process is still running; a dead serve
// invalidates every HTTP measurement, so the runner checks it per state.
func (e *Env) serveAlive() (bool, int) {
	if e.serveDone == nil {
		return true, 0
	}
	select {
	case code := <-e.serveDone:
		e.serveDone <- code
		return false, code
	default:
		return true, 0
	}
}

// writeDiagnostics persists the captured serve/scanner logs into the evidence
// directory (a dead serve or a failed run must be diagnosable from the
// evidence package).
func (e *Env) writeDiagnostics(name string) {
	if e.Evidence == nil {
		return
	}
	_ = e.Evidence.WriteFile(name+".stdout.log", []byte(e.serveOut.String()))
	_ = e.Evidence.WriteFile(name+".stderr.log", []byte(e.serveErr.String()))
	_ = e.Evidence.WriteFile(name+".scanner.log", []byte(e.logBuf.String()))
}

// stop shuts the runtimes, serve, the pool and the containers down. It is
// idempotent (the test cleanup is a safety net behind runPath's explicit
// stop).
func (e *Env) stop() {
	if e == nil {
		return
	}
	if e.stopped {
		return
	}
	e.stopped = true
	e.writeDiagnostics(fmt.Sprintf("serve_%s_run%d", e.Path, e.Run))
	stopLoop := func(name string, cancel context.CancelFunc, done chan int) {
		if cancel == nil {
			return
		}
		cancel()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			fmt.Fprintf(os.Stderr, "perf: %s did not stop\n%s", name, e.diagnostics())
		}
	}
	stopLoop("event-publisher", e.pubCancel, e.pubDone)
	stopLoop("event-consumer", e.consCancel, e.consDone)
	if e.serveCancel != nil {
		e.serveCancel()
		select {
		case <-e.serveDone:
		case <-time.After(30 * time.Second):
			fmt.Fprintf(os.Stderr, "perf: serve did not stop\n%s", e.diagnostics())
		}
	}
	if e.Pool != nil {
		e.Pool.Close()
		e.Pool = nil
	}
	if e.Kafka != nil {
		_ = e.Kafka.Close(context.Background())
		e.Kafka = nil
	}
	if e.Redis != nil {
		_ = e.Redis.Close(context.Background())
		e.Redis = nil
	}
	if e.Anvil != nil {
		_ = e.Anvil.terminate(context.Background())
		e.Anvil = nil
	}
	if e.pgCtr != nil {
		_ = e.pgCtr.Terminate(context.Background())
		e.pgCtr = nil
	}
}

// --- seeding -----------------------------------------------------------------

// seedPrerequisites seeds the real 007/008 prerequisites through the real
// public functions (the same canonical fixture the E2E/drill layers use).
func (e *Env) seed(t *testing.T, ctx context.Context, sender common.Address) {
	t.Helper()
	if _, err := e.Pool.Exec(ctx, `INSERT INTO caller (caller_id, label, can_create) VALUES ($1, 'perf', TRUE)
		ON CONFLICT (caller_id) DO UPDATE SET can_create = TRUE`, BenchCaller); err != nil {
		t.Fatalf("seed caller: %v", err)
	}
	apiKey, _, err := withdrawal.IssueKey(ctx, e.Pool, BenchCaller, "perf")
	if err != nil {
		t.Fatalf("IssueKey: %v", err)
	}
	e.APIKey = apiKey
	if _, err := e.Pool.Exec(ctx, `INSERT INTO execution_caller_permission (caller_id, can_execute, updated_by)
		VALUES ($1, TRUE, 'perf') ON CONFLICT (caller_id) DO UPDATE SET can_execute = TRUE`, BenchCaller); err != nil {
		t.Fatalf("seed execution permission: %v", err)
	}
	senderHex := strings.ToLower(sender.Hex())
	if _, err := e.Pool.Exec(ctx, `INSERT INTO nonce_wallet_registry (chain_id, sender, state, registry_seq)
		VALUES ($1, $2, 'active', 1) ON CONFLICT (chain_id, sender) DO UPDATE SET state = 'active'`,
		BenchChainID, senderHex); err != nil {
		t.Fatalf("seed nonce registry: %v", err)
	}
	if _, err := e.Pool.Exec(ctx, `INSERT INTO nonce_scope_state (chain_id, sender) VALUES ($1, $2)
		ON CONFLICT (chain_id, sender) DO NOTHING`, BenchChainID, senderHex); err != nil {
		t.Fatalf("seed nonce scope: %v", err)
	}
	// The grant pool: each accepted create is bound to exactly one
	// authorization, so the fixed create stream needs a pre-seeded pool. The
	// pool is a benchmark test input (identical for both paths).
	for i := 0; i < BenchGrantPool; i++ {
		authID := fmt.Sprintf("perf-auth-%d", i)
		if _, err := withdrawal.SupplyGrant(ctx, e.Pool, withdrawal.OpInput{
			OperationID:     fmt.Sprintf("perf-op-%d", i),
			Action:          "supply",
			AuthorizationID: authID,
			CallerID:        BenchCaller,
			ChainID:         BenchChainID,
			Asset:           BenchAsset,
			Recipient:       BenchRecipient,
			Amount:          BenchAmount,
		}, "perf", "controlled benchmark supply"); err != nil {
			t.Fatalf("SupplyGrant(%d): %v", i, err)
		}
	}
	// The query target set: pre-created accepted requests (each consumes one
	// grant) the query stream reads back.
	for i := 0; i < BenchSeedRequests; i++ {
		authID := fmt.Sprintf("perf-auth-seed-%d", i)
		if _, err := withdrawal.SupplyGrant(ctx, e.Pool, withdrawal.OpInput{
			OperationID:     fmt.Sprintf("perf-op-seed-%d", i),
			Action:          "supply",
			AuthorizationID: authID,
			CallerID:        BenchCaller,
			ChainID:         BenchChainID,
			Asset:           BenchAsset,
			Recipient:       BenchRecipient,
			Amount:          BenchAmount,
		}, "perf", "controlled benchmark supply"); err != nil {
			t.Fatalf("SupplyGrant(seed %d): %v", i, err)
		}
		status, raw, err := e.doRequest(ctx, http.MethodPost, "/withdrawals", e.createBody(fmt.Sprintf("perf-seed-%d", i), authID))
		if err != nil {
			t.Fatalf("seed create %d: %v", i, err)
		}
		if status != http.StatusCreated {
			t.Fatalf("seed create %d = %d, want 201; body=%s%s", i, status, raw, e.diagnostics())
		}
		var body struct {
			RequestID string `json:"request_id"`
		}
		if err := json.Unmarshal(raw, &body); err != nil || body.RequestID == "" {
			t.Fatalf("decode seed create %d: %v (%s)", i, err, raw)
		}
		e.QueryIDs = append(e.QueryIDs, body.RequestID)
	}
	// Grant pool bookkeeping: the seed creates consumed the seed grants; the
	// create stream starts after them.
	e.grantSeq.Store(int64(BenchSeedRequests))
}

// createBody renders one canonical create request body under an authorization.
func (e *Env) createBody(idemKey, authID string) string {
	return fmt.Sprintf(`{"idempotency_key":%q,"chain_id":%d,"asset":%q,"recipient":%q,"amount":%q,"authorization_id":%q}`,
		idemKey, BenchChainID, BenchAsset, BenchRecipient, BenchAmount, authID)
}

// createAuthID returns the next grant-pool authorization id for the create
// stream. The seed grants carry a distinct prefix, so the pool never collides
// with them.
func (e *Env) createAuthID() string {
	n := e.grantSeq.Add(1) - 1
	return fmt.Sprintf("perf-auth-%d", n)
}

// --- fault injection ---------------------------------------------------------

// enterState applies the fault timeline transition for one state. Path A has
// no Redis/Kafka dependencies, so every transition is a recorded no-op (the
// same timeline, the same load; adr.md §3 controlled variables).
func (e *Env) enterState(ctx context.Context, state FaultState) (bool, error) {
	if e.Path == PathPGOnly {
		return false, nil
	}
	switch state {
	case StateNormal:
		if err := e.ensureKafkaUp(ctx); err != nil {
			return false, err
		}
		if err := e.ensureRedisUp(ctx); err != nil {
			return false, err
		}
	case StateRedisDown:
		if err := e.ensureKafkaUp(ctx); err != nil {
			return false, err
		}
		if err := e.stopRedis(ctx); err != nil {
			return false, err
		}
	case StateKafkaDown:
		if err := e.ensureRedisUp(ctx); err != nil {
			return false, err
		}
		if err := e.suspendKafka(ctx); err != nil {
			return false, err
		}
	case StateDual:
		if err := e.suspendKafka(ctx); err != nil {
			return false, err
		}
		if err := e.stopRedis(ctx); err != nil {
			return false, err
		}
	case StateRecovery:
		if err := e.resumeKafka(ctx); err != nil {
			return false, err
		}
		if err := e.ensureRedisUp(ctx); err != nil {
			return false, err
		}
		// Drill-local determinism (B9 parity): clear the publisher backoff at
		// the recovery moment so the measured catch-up/drain reflects drain
		// and consume capacity, not the backoff phase the outage left behind.
		// It never changes event content, never deletes or overwrites a row.
		if _, err := e.Pool.Exec(ctx,
			`UPDATE outbox_events SET next_attempt_at = now() WHERE publish_state = 'pending'`); err != nil {
			return false, fmt.Errorf("clear publisher backoff at recovery: %w", err)
		}
	}
	return true, nil
}

func (e *Env) stopRedis(ctx context.Context) error {
	if !e.redisUp {
		return nil
	}
	if err := e.Redis.Stop(ctx); err != nil {
		return fmt.Errorf("stop redis: %w", err)
	}
	e.redisUp = false
	return nil
}

func (e *Env) ensureRedisUp(ctx context.Context) error {
	if e.redisUp {
		return nil
	}
	if err := e.Redis.Start(ctx); err != nil {
		return fmt.Errorf("start redis: %w", err)
	}
	e.redisUp = true
	return nil
}

func (e *Env) suspendKafka(ctx context.Context) error {
	if !e.kafkaUp {
		return nil
	}
	if err := e.Kafka.Suspend(ctx); err != nil {
		return fmt.Errorf("suspend kafka: %w", err)
	}
	e.kafkaUp = false
	return nil
}

func (e *Env) resumeKafka(ctx context.Context) error {
	if e.kafkaUp {
		return nil
	}
	if err := e.Kafka.Resume(ctx); err != nil {
		return fmt.Errorf("resume kafka: %w", err)
	}
	e.kafkaUp = true
	return nil
}

func (e *Env) ensureKafkaUp(ctx context.Context) error {
	if e.kafkaUp {
		return nil
	}
	return e.resumeKafka(ctx)
}

// --- HTTP helpers ------------------------------------------------------------

// doRequest issues one authenticated request against the real listener.
func (e *Env) doRequest(ctx context.Context, method, path, body string) (int, []byte, error) {
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, e.BaseURL+path, reader)
	if err != nil {
		return 0, nil, err
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Authorization", "Bearer "+e.APIKey)
	resp, err := e.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return 0, nil, err
	}
	return resp.StatusCode, raw, nil
}

// --- load generator ----------------------------------------------------------

// Sample is one recorded load-generator observation (raw per-request sample).
type Sample struct {
	At         time.Time `json:"at"`
	State      string    `json:"state"`
	Class      string    `json:"class"`
	DurationMS float64   `json:"duration_ms"`
	Status     int       `json:"status"`
	OK         bool      `json:"ok"`
	ErrClass   string    `json:"err_class,omitempty"`
	ErrText    string    `json:"err_text,omitempty"`
}

// ClassStats is the per-class aggregation of one window.
type ClassStats struct {
	Class         string         `json:"class"`
	Count         int            `json:"count"`
	Errors        int            `json:"errors"`
	ErrorRate     float64        `json:"error_rate"`
	ThroughputRPS float64        `json:"throughput_rps"`
	MedianMS      float64        `json:"median_ms"`
	MeanMS        float64        `json:"mean_ms"`
	StdDevMS      float64        `json:"stddev_ms"`
	P50MS         float64        `json:"p50_ms"`
	P95MS         float64        `json:"p95_ms"`
	P99MS         float64        `json:"p99_ms"`
	MaxMS         float64        `json:"max_ms"`
	StatusCounts  map[string]int `json:"status_counts,omitempty"`
}

// computeClassStats aggregates the samples of one class in one window.
func computeClassStats(class string, samples []Sample, windowSeconds float64) ClassStats {
	stats := ClassStats{Class: class, StatusCounts: map[string]int{}}
	var durations []float64
	for _, sample := range samples {
		if sample.Class != class {
			continue
		}
		stats.Count++
		if !sample.OK {
			stats.Errors++
		}
		stats.StatusCounts[strconv.Itoa(sample.Status)]++
		durations = append(durations, sample.DurationMS)
	}
	if stats.Count == 0 {
		return stats
	}
	sort.Float64s(durations)
	if windowSeconds > 0 {
		stats.ThroughputRPS = float64(stats.Count) / windowSeconds
	}
	stats.ErrorRate = float64(stats.Errors) / float64(stats.Count)
	stats.MedianMS = percentile(durations, 50)
	stats.P50MS = stats.MedianMS
	stats.P95MS = percentile(durations, 95)
	stats.P99MS = percentile(durations, 99)
	stats.MaxMS = durations[len(durations)-1]
	var sum float64
	for _, d := range durations {
		sum += d
	}
	stats.MeanMS = sum / float64(len(durations))
	if len(durations) > 1 {
		var sq float64
		for _, d := range durations {
			sq += (d - stats.MeanMS) * (d - stats.MeanMS)
		}
		stats.StdDevMS = math.Sqrt(sq / float64(len(durations)-1))
	}
	return stats
}

// percentile is the linear-interpolation percentile (p in 0..100).
func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return math.NaN()
	}
	if len(sorted) == 1 {
		return sorted[0]
	}
	rank := p / 100 * float64(len(sorted)-1)
	lo := int(math.Floor(rank))
	hi := int(math.Ceil(rank))
	if lo == hi {
		return sorted[lo]
	}
	return sorted[lo] + (sorted[hi]-sorted[lo])*(rank-float64(lo))
}

// runLoadWindow runs the fixed load profile for one state window and returns
// the raw samples.
func (e *Env) runLoadWindow(t *testing.T, ctx context.Context, state FaultState, profile Profile, record bool) []Sample {
	t.Helper()
	start := time.Now()
	end := start.Add(profile.StateWindow)

	var mu sync.Mutex
	var samples []Sample
	var streams, requests sync.WaitGroup
	sem := make(chan struct{}, benchConcurrency)

	recordSample := func(s Sample) {
		if !record {
			return
		}
		mu.Lock()
		samples = append(samples, s)
		mu.Unlock()
	}
	acquire := func() bool {
		select {
		case sem <- struct{}{}:
			return true
		case <-ctx.Done():
			return false
		}
	}
	release := func() { <-sem }

	// Query stream: the fixed ladder, then the fixed burst.
	streams.Add(1)
	go func() {
		defer streams.Done()
		for i, step := range profile.QueryLadder {
			stepEnd := end
			if i+1 < len(profile.QueryLadder) {
				if next := start.Add(profile.QueryLadder[i+1].Offset); next.Before(end) {
					stepEnd = next
				}
			}
			if err := runRate(ctx, stepEnd, step.RPS, func() {
				if !acquire() {
					return
				}
				requests.Add(1)
				go func() {
					defer requests.Done()
					defer release()
					recordSample(e.queryOnce(ctx, state))
				}()
			}); err != nil {
				return
			}
		}
	}()
	streams.Add(1)
	go func() {
		defer streams.Done()
		waitUntilTime(ctx, start.Add(profile.Burst.At))
		for i := 0; i < profile.Burst.Size; i++ {
			if !acquire() {
				return
			}
			requests.Add(1)
			go func() {
				defer requests.Done()
				defer release()
				recordSample(e.queryOnce(ctx, state))
			}()
		}
	}()

	// Create stream: fixed rate for the whole window.
	streams.Add(1)
	go func() {
		defer streams.Done()
		_ = runRate(ctx, end, profile.CreateRPS, func() {
			if !acquire() {
				return
			}
			requests.Add(1)
			go func() {
				defer requests.Done()
				defer release()
				recordSample(e.createOnce(ctx, state))
			}()
		})
	}()

	// Deposit stream: fixed rate for the whole window.
	streams.Add(1)
	go func() {
		defer streams.Done()
		_ = runRate(ctx, end, profile.DepositRPS, func() {
			if !acquire() {
				return
			}
			requests.Add(1)
			go func() {
				defer requests.Done()
				defer release()
				recordSample(e.depositOnce(ctx, state))
			}()
		})
	}()

	streams.Wait()
	requests.Wait()
	return samples
}

// runRate fires fn at rps until the deadline (or ctx cancellation).
func runRate(ctx context.Context, until time.Time, rps int, fn func()) error {
	if rps <= 0 {
		return nil
	}
	period := time.Second / time.Duration(rps)
	if period <= 0 {
		period = time.Millisecond
	}
	ticker := time.NewTicker(period)
	defer ticker.Stop()
	for time.Now().Before(until) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			fn()
		}
	}
	return nil
}

// waitUntilTime sleeps until the deadline or ctx cancellation.
func waitUntilTime(ctx context.Context, until time.Time) {
	d := time.Until(until)
	if d <= 0 {
		return
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

// queryOnce drives one real GET /withdrawals/{id} (the query class).
func (e *Env) queryOnce(ctx context.Context, state FaultState) Sample {
	idx := e.querySeq.Add(1) % uint64(len(e.QueryIDs))
	id := e.QueryIDs[idx]
	start := time.Now()
	sample := Sample{At: start.UTC(), State: string(state), Class: "query"}
	status, _, err := e.doRequest(ctx, http.MethodGet, "/withdrawals/"+id, "")
	sample.DurationMS = float64(time.Since(start).Microseconds()) / 1000
	if err != nil {
		sample.ErrClass = "transport"
		sample.ErrText = truncateError(err)
		return sample
	}
	sample.Status = status
	sample.OK = status < 400
	if !sample.OK {
		sample.ErrClass = strconv.Itoa(status)
	}
	return sample
}

// createOnce drives one real POST /withdrawals (the create class) under the
// next grant-pool authorization.
func (e *Env) createOnce(ctx context.Context, state FaultState) Sample {
	start := time.Now()
	sample := Sample{At: start.UTC(), State: string(state), Class: "create"}
	n := e.grantSeq.Add(1)
	key := fmt.Sprintf("perf-%s-%d", state, n)
	authID := fmt.Sprintf("perf-auth-%d", n)
	status, _, err := e.doRequest(ctx, http.MethodPost, "/withdrawals", e.createBody(key, authID))
	sample.DurationMS = float64(time.Since(start).Microseconds()) / 1000
	if err != nil {
		sample.ErrClass = "transport"
		sample.ErrText = truncateError(err)
		return sample
	}
	sample.Status = status
	sample.OK = status < 400
	if !sample.OK {
		sample.ErrClass = strconv.Itoa(status)
	}
	return sample
}

// truncateError renders a bounded, credential-redacted error string for the
// raw sample evidence.
func truncateError(err error) string {
	if err == nil {
		return ""
	}
	text := logx.Redact(err.Error())
	if len(text) > 200 {
		text = text[:200]
	}
	return text
}

// depositOnce emits one real on-chain deposit (the chain-side load).
func (e *Env) depositOnce(ctx context.Context, state FaultState) Sample {
	start := time.Now()
	sample := Sample{At: start.UTC(), State: string(state), Class: "deposit"}
	e.depositMu.Lock()
	_, _, err := e.Anvil.emitDeposit(e.sender, common.HexToAddress(BenchAsset), common.HexToAddress(BenchWatch), BenchDepositValue)
	e.depositMu.Unlock()
	sample.DurationMS = float64(time.Since(start).Microseconds()) / 1000
	if err != nil {
		sample.ErrClass = "chain"
		return sample
	}
	sample.Status = 0
	sample.OK = true
	return sample
}

// --- durable and resource sampling -------------------------------------------

// DurableState is one point-in-time durable observation (PostgreSQL + Kafka).
type DurableState struct {
	At                     time.Time `json:"at"`
	OutboxPending          int64     `json:"outbox_pending"`
	OutboxPublished        int64     `json:"outbox_published"`
	OutboxBlocked          int64     `json:"outbox_blocked"`
	OutboxAttemptSum       int64     `json:"outbox_attempt_sum"`
	OutboxOldestAgeSeconds float64   `json:"outbox_oldest_age_seconds"`
	ConsumerProgressRows   int64     `json:"consumer_progress_rows"`
	CatchupGap             int64     `json:"catchup_gap"`
	LedgerRows             int64     `json:"ledger_rows"`
	KafkaEndOffsets        int64     `json:"kafka_end_offsets"`
	KafkaCommitted         int64     `json:"kafka_committed"`
}

// sampleState reads the durable state. The PostgreSQL part is required; the
// Kafka part is optional (path A has no broker) and reports -1 when
// unavailable.
func (e *Env) sampleState(ctx context.Context) (DurableState, error) {
	snap := DurableState{At: time.Now().UTC(), KafkaEndOffsets: -1, KafkaCommitted: -1}
	if err := e.Pool.QueryRow(ctx, `
SELECT count(*) FILTER (WHERE publish_state = 'pending'),
       count(*) FILTER (WHERE publish_state = 'published'),
       count(*) FILTER (WHERE publish_state = 'blocked'),
       coalesce(sum(attempt_count), 0),
       coalesce(extract(epoch FROM now() - min(created_at) FILTER (WHERE publish_state = 'pending')), 0)
FROM outbox_events`).
		Scan(&snap.OutboxPending, &snap.OutboxPublished, &snap.OutboxBlocked, &snap.OutboxAttemptSum, &snap.OutboxOldestAgeSeconds); err != nil {
		return DurableState{}, fmt.Errorf("outbox snapshot: %w", err)
	}
	if err := e.Pool.QueryRow(ctx, `SELECT count(*) FROM consumer_progress`).Scan(&snap.ConsumerProgressRows); err != nil {
		return DurableState{}, fmt.Errorf("consumer progress snapshot: %w", err)
	}
	// Path A has no reference consumer, so the simulated-ledger table does
	// not exist: the gap/ledger fields are "not applicable" (-1), never a
	// fabricated zero.
	var ledgerExists bool
	if err := e.Pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, events.RefLedgerTable).Scan(&ledgerExists); err != nil {
		return DurableState{}, fmt.Errorf("ledger table probe: %w", err)
	}
	snap.CatchupGap = -1
	snap.LedgerRows = -1
	if ledgerExists {
		if err := e.Pool.QueryRow(ctx, `
SELECT count(*) FROM outbox_events o
WHERE o.publish_state = 'published'
  AND NOT EXISTS (SELECT 1 FROM `+events.RefLedgerTable+` l
                  WHERE l.consumer_name = $1 AND l.event_id = o.event_id)`, events.RefConsumerName).Scan(&snap.CatchupGap); err != nil {
			return DurableState{}, fmt.Errorf("catch-up gap: %w", err)
		}
		if err := e.Pool.QueryRow(ctx, `SELECT count(*) FROM `+events.RefLedgerTable).Scan(&snap.LedgerRows); err != nil {
			return DurableState{}, fmt.Errorf("ledger rows: %w", err)
		}
	}
	if e.Path == PathFullStack && e.kafkaUp {
		probeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		if ends, err := e.kafkaEndOffsets(probeCtx); err == nil {
			snap.KafkaEndOffsets = ends
		}
		if committed, err := e.kafkaCommitted(probeCtx); err == nil {
			snap.KafkaCommitted = committed
		}
		cancel()
	}
	return snap, nil
}

func (e *Env) kafkaEndOffsets(ctx context.Context) (int64, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(e.Kafka.Brokers()...))
	if err != nil {
		return 0, err
	}
	defer cl.Close()
	ends, err := kadm.NewClient(cl).ListEndOffsets(ctx, testutil.KafkaTopic)
	if err != nil {
		return 0, err
	}
	var total int64
	for _, partitions := range ends {
		for _, listed := range partitions {
			if listed.Err != nil {
				return 0, listed.Err
			}
			if listed.Offset > 0 {
				total += listed.Offset
			}
		}
	}
	return total, nil
}

func (e *Env) kafkaCommitted(ctx context.Context) (int64, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(e.Kafka.Brokers()...))
	if err != nil {
		return 0, err
	}
	defer cl.Close()
	group := e.EnvMap["TXHARBOR_KAFKA_CONSUMER_GROUP_PREFIX"]
	if group == "" {
		group = "txharbor"
	}
	committed, err := kadm.NewClient(cl).FetchOffsets(ctx, group+"."+events.RefConsumerName)
	if err != nil {
		return 0, err
	}
	var total int64
	for _, partitions := range committed {
		for _, offset := range partitions {
			if offset.Err == nil && offset.At > 0 {
				total += offset.At
			}
		}
	}
	return total, nil
}

// ResourceSample is one process/resource observation. Both paths measure the
// same way; the load generator shares the process, so CPU/IO are upper bounds
// and the report states that limitation.
type ResourceSample struct {
	At              time.Time `json:"at"`
	CPUSeconds      float64   `json:"cpu_seconds"`
	RSSBytes        uint64    `json:"rss_bytes"`
	DiskReadBytes   uint64    `json:"disk_read_bytes"`
	DiskWriteBytes  uint64    `json:"disk_write_bytes"`
	NetRxBytes      uint64    `json:"net_rx_bytes"`
	NetTxBytes      uint64    `json:"net_tx_bytes"`
	HeapAllocBytes  uint64    `json:"heap_alloc_bytes"`
	Goroutines      int       `json:"goroutines"`
	PGTotalConns    int32     `json:"pg_total_conns"`
	PGAcquiredConns int32     `json:"pg_acquired_conns"`
}

// ResourceDelta is the before/after pair plus the deltas.
type ResourceDelta struct {
	Before         ResourceSample `json:"before"`
	After          ResourceSample `json:"after"`
	CPUSeconds     float64        `json:"cpu_seconds_delta"`
	RSSBytes       uint64         `json:"rss_bytes_after"`
	DiskReadBytes  uint64         `json:"disk_read_bytes_delta"`
	DiskWriteBytes uint64         `json:"disk_write_bytes_delta"`
	NetRxBytes     uint64         `json:"net_rx_bytes_delta"`
	NetTxBytes     uint64         `json:"net_tx_bytes_delta"`
}

func (e *Env) readResources() ResourceSample {
	sample := ResourceSample{At: time.Now().UTC(), Goroutines: runtime.NumGoroutine()}
	sample.CPUSeconds = readProcessCPUSeconds()
	sample.RSSBytes = readStatusBytes("VmRSS:")
	sample.DiskReadBytes, sample.DiskWriteBytes = readProcIO()
	sample.NetRxBytes, sample.NetTxBytes = readNetDev()
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	sample.HeapAllocBytes = mem.HeapAlloc
	if e.Pool != nil {
		stat := e.Pool.Stat()
		sample.PGTotalConns = stat.TotalConns()
		sample.PGAcquiredConns = stat.AcquiredConns()
	}
	return sample
}

func resourceDelta(before, after ResourceSample) ResourceDelta {
	delta := ResourceDelta{Before: before, After: after, RSSBytes: after.RSSBytes}
	if after.CPUSeconds >= before.CPUSeconds {
		delta.CPUSeconds = after.CPUSeconds - before.CPUSeconds
	}
	if after.DiskReadBytes >= before.DiskReadBytes {
		delta.DiskReadBytes = after.DiskReadBytes - before.DiskReadBytes
	}
	if after.DiskWriteBytes >= before.DiskWriteBytes {
		delta.DiskWriteBytes = after.DiskWriteBytes - before.DiskWriteBytes
	}
	if after.NetRxBytes >= before.NetRxBytes {
		delta.NetRxBytes = after.NetRxBytes - before.NetRxBytes
	}
	if after.NetTxBytes >= before.NetTxBytes {
		delta.NetTxBytes = after.NetTxBytes - before.NetTxBytes
	}
	return delta
}

// readProcessCPUSeconds reads utime+stime from /proc/self/stat (CLK_TCK is 100
// on the Linux targets used here; documented measurement assumption).
func readProcessCPUSeconds() float64 {
	raw, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		return math.NaN()
	}
	// Skip the comm field (it may contain spaces): parse after the last ')'.
	idx := bytes.LastIndexByte(raw, ')')
	if idx < 0 || idx+2 >= len(raw) {
		return math.NaN()
	}
	fields := strings.Fields(string(raw[idx+2:]))
	if len(fields) < 13 {
		return math.NaN()
	}
	utime, err1 := strconv.ParseFloat(fields[11], 64)
	stime, err2 := strconv.ParseFloat(fields[12], 64)
	if err1 != nil || err2 != nil {
		return math.NaN()
	}
	return (utime + stime) / 100
}

// readStatusBytes reads one `<Key>: <n> kB` value from /proc/self/status.
func readStatusBytes(key string) uint64 {
	raw, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if !strings.HasPrefix(line, key) {
			continue
		}
		fields := strings.Fields(strings.TrimPrefix(line, key))
		if len(fields) == 0 {
			return 0
		}
		n, err := strconv.ParseUint(fields[0], 10, 64)
		if err != nil {
			return 0
		}
		return n * 1024
	}
	return 0
}

// readProcIO reads read_bytes/write_bytes from /proc/self/io.
func readProcIO() (uint64, uint64) {
	raw, err := os.ReadFile("/proc/self/io")
	if err != nil {
		return 0, 0
	}
	var read, written uint64
	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		n, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			continue
		}
		switch fields[0] {
		case "read_bytes:":
			read = n
		case "write_bytes:":
			written = n
		}
	}
	return read, written
}

// readNetDev sums rx/tx bytes across interfaces (namespace-level; documented
// as shared with other processes in the same network namespace).
func readNetDev() (uint64, uint64) {
	raw, err := os.ReadFile("/proc/self/net/dev")
	if err != nil {
		return 0, 0
	}
	var rx, tx uint64
	for _, line := range strings.Split(string(raw), "\n") {
		idx := strings.IndexByte(line, ':')
		if idx < 0 {
			continue
		}
		fields := strings.Fields(line[idx+1:])
		if len(fields) < 9 {
			continue
		}
		r, err1 := strconv.ParseUint(fields[0], 10, 64)
		t, err2 := strconv.ParseUint(fields[8], 10, 64)
		if err1 == nil {
			rx += r
		}
		if err2 == nil {
			tx += t
		}
	}
	return rx, tx
}

// --- metrics scrape ----------------------------------------------------------

// ScrapedMetrics is the parsed subset of the serve Prometheus exposition the
// report consumes.
type ScrapedMetrics struct {
	RateLimitDenied      float64 `json:"ratelimit_denied"`
	RateLimitUnavailable float64 `json:"ratelimit_unavailable"`
	CacheHits            float64 `json:"cache_hits"`
	CacheMisses          float64 `json:"cache_misses"`
	CacheFallbacks       float64 `json:"cache_fallbacks"`
	CapacityRefusals     float64 `json:"capacity_refusals"`
	CapacitySoftBreaches float64 `json:"capacity_soft_breaches"`
	CapacityHardBreaches float64 `json:"capacity_hard_breaches"`
	OutboxPendingGauge   float64 `json:"outbox_pending_gauge"`
	OutboxOldestAgeGauge float64 `json:"outbox_oldest_age_gauge"`
	Raw                  string  `json:"-"`
}

// scrapeServeMetrics fetches and parses the serve exposition.
func (e *Env) scrapeServeMetrics(ctx context.Context) ScrapedMetrics {
	resp, err := e.client.Get(e.BaseURL + "/metrics")
	if err != nil {
		return ScrapedMetrics{}
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return ScrapedMetrics{}
	}
	return parseMetrics(string(raw))
}

// parseMetrics sums the value of every listed series (all labels).
func parseMetrics(text string) ScrapedMetrics {
	sums := map[string]float64{}
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name := line
		if idx := strings.IndexByte(name, '{'); idx >= 0 {
			name = name[:idx]
		} else if idx := strings.IndexByte(name, ' '); idx >= 0 {
			name = name[:idx]
		}
		valuePart := line
		if idx := strings.LastIndexByte(line, ' '); idx >= 0 {
			valuePart = line[idx+1:]
		}
		value, err := strconv.ParseFloat(valuePart, 64)
		if err != nil {
			continue
		}
		sums[name] += value
	}
	return ScrapedMetrics{
		RateLimitDenied:      sums["txharbor_ratelimit_denied_total"],
		RateLimitUnavailable: sums["txharbor_ratelimit_unavailable"],
		CacheHits:            sums["txharbor_cache_hits_total"],
		CacheMisses:          sums["txharbor_cache_misses_total"],
		CacheFallbacks:       sums["txharbor_cache_fallback_total"],
		CapacityRefusals:     sums["txharbor_capacity_refusals_total"],
		CapacitySoftBreaches: sums["txharbor_capacity_soft_breaches_total"],
		CapacityHardBreaches: sums["txharbor_capacity_hard_breaches_total"],
		OutboxPendingGauge:   sums["txharbor_outbox_pending_count"],
		OutboxOldestAgeGauge: sums["txharbor_outbox_pending_oldest_age_seconds"],
		Raw:                  text,
	}
}

// --- state runner ------------------------------------------------------------

// StateResult is one measured state window of one path.
type StateResult struct {
	State         FaultState     `json:"state"`
	Label         string         `json:"label"`
	WindowSeconds float64        `json:"window_seconds"`
	FaultActive   bool           `json:"fault_active"`
	Notes         []string       `json:"notes,omitempty"`
	Query         ClassStats     `json:"query"`
	Create        ClassStats     `json:"create"`
	Deposit       ClassStats     `json:"deposit"`
	Before        DurableState   `json:"before"`
	After         DurableState   `json:"after"`
	Scrape        ScrapedMetrics `json:"scrape"`
	Resources     ResourceDelta  `json:"resources"`
	// PublishFailures counts the publisher's structured failure records
	// ("outbox publish failure") observed during the window; QuarantineLogs
	// counts the consumer's quarantine records ("consumer quarantined
	// event"). Both are log-derived measurements (the publisher/consumer
	// commands expose no metrics endpoint in this feature's scope).
	PublishFailures int     `json:"publish_failures_logged"`
	QuarantineLogs  int     `json:"quarantine_logged"`
	CatchupSeconds  float64 `json:"catchup_seconds"`
	DrainSeconds    float64 `json:"drain_seconds"`
}

// logCounts counts the publisher/consumer structured records captured from the
// in-process command runtimes.
type logCounts struct {
	publishFailures int
	quarantine      int
}

func (e *Env) logCounts() logCounts {
	text := e.logBuf.String()
	return logCounts{
		publishFailures: strings.Count(text, "outbox publish failure"),
		quarantine:      strings.Count(text, "consumer quarantined event"),
	}
}

// PathResult is one full path run.
type PathResult struct {
	Path      Path          `json:"path"`
	Run       int           `json:"run"`
	Spec      EnvSpec       `json:"spec"`
	StartedAt time.Time     `json:"started_at"`
	EndedAt   time.Time     `json:"ended_at"`
	States    []StateResult `json:"states"`
}

// runPath runs the full fixed profile for one path once.
func runPath(t *testing.T, ctx context.Context, path Path, run int, profile Profile, evidence *EvidenceWriter) PathResult {
	t.Helper()
	result := PathResult{Path: path, Run: run, StartedAt: time.Now().UTC()}
	env := startEnv(t, ctx, path, run, profile, evidence)
	defer env.stop()
	result.Spec = env.Spec

	// Warm-up: the same generator shape, excluded from every measurement
	// (verification.md §1: the throughput window excludes warm-up).
	t.Logf("perf[%s run %d]: warm-up %s", path, run, profile.Warmup)
	env.runLoadWindow(t, ctx, StateNormal, Profile{
		StateWindow: profile.Warmup,
		QueryLadder: []LadderStep{{Offset: 0, RPS: profile.QueryLadder[0].RPS}},
		Burst:       BurstSpec{},
		CreateRPS:   profile.CreateRPS,
		DepositRPS:  profile.DepositRPS,
	}, false)

	for _, state := range StateSequence {
		active, err := env.enterState(ctx, state)
		if err != nil {
			t.Fatalf("enter state %s: %v", state, err)
		}
		// Let the fault settle before measuring the degraded window.
		time.Sleep(500 * time.Millisecond)

		before, err := env.sampleState(ctx)
		if err != nil {
			t.Fatalf("sample before %s: %v", state, err)
		}
		logsBefore := env.logCounts()
		resourcesBefore := env.readResources()

		recoveryStart := time.Now()
		var probe *catchupProbe
		if state == StateRecovery && path == PathFullStack {
			probe = startCatchupProbe(ctx, env, recoveryStart)
		}

		if alive, code := env.serveAlive(); !alive {
			env.writeDiagnostics(fmt.Sprintf("serve_%s_run%d", path, run))
			t.Fatalf("serve exited before state %s (code %d); stderr:\n%s", state, code, env.serveErr.String())
		}
		windowStart := time.Now()
		samples := env.runLoadWindow(t, ctx, state, profile, true)
		windowSeconds := time.Since(windowStart).Seconds()
		if alive, code := env.serveAlive(); !alive {
			env.writeDiagnostics(fmt.Sprintf("serve_%s_run%d", path, run))
			t.Fatalf("serve exited during state %s (code %d); stderr:\n%s", state, code, env.serveErr.String())
		}
		if err := env.checkServe(); err != nil {
			t.Fatalf("serve died during state %s: %v%s", state, err, env.diagnostics())
		}

		after, err := env.sampleState(ctx)
		if err != nil {
			t.Fatalf("sample after %s: %v", state, err)
		}
		logsAfter := env.logCounts()
		resourcesAfter := env.readResources()
		scrape := env.scrapeServeMetrics(ctx)

		stateResult := StateResult{
			State:           state,
			Label:           StateLabel[state],
			WindowSeconds:   windowSeconds,
			FaultActive:     active,
			Query:           computeClassStats("query", samples, windowSeconds),
			Create:          computeClassStats("create", samples, windowSeconds),
			Deposit:         computeClassStats("deposit", samples, windowSeconds),
			Before:          before,
			After:           after,
			Scrape:          scrape,
			Resources:       resourceDelta(resourcesBefore, resourcesAfter),
			PublishFailures: logsAfter.publishFailures - logsBefore.publishFailures,
			QuarantineLogs:  logsAfter.quarantine - logsBefore.quarantine,
			CatchupSeconds:  -1,
			DrainSeconds:    -1,
		}
		if state == StateRecovery {
			if probe != nil {
				catchup, drain, notes := probe.result(ctx, 180*time.Second)
				stateResult.CatchupSeconds = catchup
				stateResult.DrainSeconds = drain
				stateResult.Notes = append(stateResult.Notes, notes...)
			} else {
				stateResult.Notes = append(stateResult.Notes,
					"路径 A 无事件通道，追赶时间不适用（n/a）")
			}
		}
		if !active && path == PathPGOnly {
			stateResult.Notes = append(stateResult.Notes,
				"路径 A 无 Redis/Kafka 依赖：该故障态按同一时间线记录但无依赖可注入（受控变量）")
		}
		result.States = append(result.States, stateResult)
		t.Logf("perf[%s run %d]: state %s query p95=%.1fms create err=%.0f%% pending=%d gap=%d",
			path, run, state, stateResult.Query.P95MS, stateResult.Create.ErrorRate*100,
			stateResult.After.OutboxPending, stateResult.After.CatchupGap)

		// Evidence: raw samples + state summary + metric exposition.
		if err := env.Evidence.WriteJSONL(fmt.Sprintf("samples_%s_run%d_%s.jsonl", path, run, state), samples); err != nil {
			t.Fatalf("write samples %s: %v", state, err)
		}
		if scrape.Raw != "" {
			if err := env.Evidence.WriteFile(fmt.Sprintf("metrics_%s_run%d_%s.txt", path, run, state), []byte(scrape.Raw)); err != nil {
				t.Fatalf("write metrics %s: %v", state, err)
			}
		}
	}
	result.EndedAt = time.Now().UTC()
	if err := evidence.WriteJSON(fmt.Sprintf("states_%s_run%d.json", path, run), result); err != nil {
		t.Fatalf("write state results: %v", err)
	}
	return result
}

// catchupProbe polls the durable pipeline after recovery and records the
// first moment the outbox is fully published, the first moment the
// published-not-applied gap is zero, and the first moment both hold (full
// drain). Times are relative to the recovery moment.
type catchupProbe struct {
	env        *Env
	recoveryAt time.Time
	published  atomic.Int64 // unix nanos; 0 = not yet
	gapZero    atomic.Int64
	drained    atomic.Int64
	stop       chan struct{}
	done       chan struct{}
}

func startCatchupProbe(ctx context.Context, env *Env, recoveryAt time.Time) *catchupProbe {
	probe := &catchupProbe{env: env, recoveryAt: recoveryAt, stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(probe.done)
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-probe.stop:
				return
			case <-ticker.C:
				snap, err := env.sampleState(ctx)
				if err != nil {
					continue
				}
				if snap.OutboxPending == 0 && probe.published.Load() == 0 {
					probe.published.Store(time.Now().UnixNano())
				}
				if snap.CatchupGap == 0 && probe.gapZero.Load() == 0 {
					probe.gapZero.Store(time.Now().UnixNano())
				}
				if snap.OutboxPending == 0 && snap.CatchupGap == 0 && probe.drained.Load() == 0 {
					probe.drained.Store(time.Now().UnixNano())
					return
				}
			}
		}
	}()
	return probe
}

// result waits (bounded) for the drain and renders the measured times in
// seconds. Unreached milestones are -1 with a "待测/待裁决" note.
func (p *catchupProbe) result(ctx context.Context, bound time.Duration) (float64, float64, []string) {
	deadline := time.Now().Add(bound)
	for time.Now().Before(deadline) && p.drained.Load() == 0 {
		select {
		case <-ctx.Done():
			deadline = time.Now() // stop waiting on cancellation
		case <-time.After(100 * time.Millisecond):
		}
	}
	close(p.stop)
	<-p.done
	toSeconds := func(nanos int64) float64 {
		if nanos == 0 {
			return -1
		}
		return float64(nanos-p.recoveryAt.UnixNano()) / float64(time.Second)
	}
	drained := toSeconds(p.drained.Load())
	published := toSeconds(p.published.Load())
	gapZero := toSeconds(p.gapZero.Load())
	var notes []string
	if drained < 0 {
		notes = append(notes, fmt.Sprintf(
			"恢复追赶：排空未在 %s 界内完成（数值待测/待裁决；已观测 published_at=%s gap_zero_at=%s）",
			bound, renderSeconds(published), renderSeconds(gapZero)))
	}
	return gapZero, drained, notes
}

func renderSeconds(v float64) string {
	if v < 0 {
		return "未达"
	}
	return fmt.Sprintf("%.2fs", v)
}

// --- environment spec / host spec --------------------------------------------

// EnvSpec is the environment record persisted as evidence (no credentials:
// the DSN is deliberately omitted).
type EnvSpec struct {
	Path            Path              `json:"path"`
	Run             int               `json:"run"`
	Commit          string            `json:"commit"`
	GoVersion       string            `json:"go_version"`
	PGImage         string            `json:"pg_image"`
	AnvilImage      string            `json:"anvil_image"`
	KafkaImage      string            `json:"kafka_image"`
	RedisImage      string            `json:"redis_image"`
	KafkaTopic      string            `json:"kafka_topic"`
	KafkaPartitions int32             `json:"kafka_partitions"`
	RedisAddr       string            `json:"redis_addr"`
	StartedAt       time.Time         `json:"started_at"`
	Layer           string            `json:"layer"`
	Profile         Profile           `json:"profile"`
	DatasetScale    string            `json:"dataset_scale"`
	Config          map[string]string `json:"config"`
}

func (e *Env) envSpec(profile Profile) EnvSpec {
	spec := EnvSpec{
		Path:            e.Path,
		Run:             e.Run,
		Commit:          detectCommit(),
		GoVersion:       runtime.Version(),
		PGImage:         BenchPGImage,
		AnvilImage:      BenchAnvilImage,
		KafkaImage:      testutil.KafkaImage,
		RedisImage:      testutil.RedisImage,
		KafkaTopic:      testutil.KafkaTopic,
		KafkaPartitions: testutil.KafkaPartitions,
		StartedAt:       time.Now().UTC(),
		Layer:           "perf",
		Profile:         profile,
		DatasetScale: fmt.Sprintf(
			"seed_requests=%d grants=%d query_ladder=%v burst=%d@%s create_rps=%d deposit_rps=%d state_window=%s",
			BenchSeedRequests, BenchGrantPool, profile.QueryLadder, profile.Burst.Size, profile.Burst.At,
			profile.CreateRPS, profile.DepositRPS, profile.StateWindow),
		Config: map[string]string{},
	}
	if e.Redis != nil {
		spec.RedisAddr = e.Redis.HostPort()
	}
	// Bench configuration inputs (no credentials): the same keys both paths
	// run with, so the report binds the measured numbers to the inputs.
	for _, key := range []string{
		"TXHARBOR_EVENTS_ENABLED", "TXHARBOR_RATELIMIT_NEW_WITHDRAWAL", "TXHARBOR_RATELIMIT_QUERY",
		"TXHARBOR_EVENTS_CAPACITY_SOFT_LIMIT", "TXHARBOR_EVENTS_CAPACITY_HARD_LIMIT",
		"TXHARBOR_EVENTS_CAPACITY_RESERVE", "TXHARBOR_EVENTS_CAPACITY_DRAIN_TARGET_WINDOW",
		"TXHARBOR_EVENTS_CONSUMER_GAP_WAIT", "TXHARBOR_EVENTS_CONSUMER_POLL_INTERVAL",
		"TXHARBOR_EVENTS_PUBLISHER_POLL_INTERVAL", "TXHARBOR_CONFIRMATION_DEPTH",
	} {
		if value, ok := e.EnvMap[key]; ok {
			spec.Config[key] = value
		}
	}
	return spec
}

// detectCommit returns the current commit (best effort; "unknown" when git is
// unavailable).
func detectCommit() string {
	out, err := runGit("rev-parse", "HEAD")
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(out)
}

// HostSpec is the host record bound into the report.
type HostSpec struct {
	CPUModel   string `json:"cpu_model"`
	CPUCores   int    `json:"cpu_cores"`
	MemTotalKB uint64 `json:"mem_total_kb"`
	OS         string `json:"os"`
	Arch       string `json:"arch"`
	Kernel     string `json:"kernel"`
}

func captureHostSpec() HostSpec {
	spec := HostSpec{CPUCores: runtime.NumCPU(), OS: runtime.GOOS, Arch: runtime.GOARCH}
	if raw, err := os.ReadFile("/proc/cpuinfo"); err == nil {
		for _, line := range strings.Split(string(raw), "\n") {
			if strings.HasPrefix(line, "model name") {
				if idx := strings.IndexByte(line, ':'); idx >= 0 {
					spec.CPUModel = strings.TrimSpace(line[idx+1:])
				}
				break
			}
		}
	}
	if raw, err := os.ReadFile("/proc/meminfo"); err == nil {
		for _, line := range strings.Split(string(raw), "\n") {
			if strings.HasPrefix(line, "MemTotal:") {
				fields := strings.Fields(line)
				if len(fields) >= 2 {
					if n, err := strconv.ParseUint(fields[1], 10, 64); err == nil {
						spec.MemTotalKB = n
					}
				}
				break
			}
		}
	}
	if raw, err := os.ReadFile("/proc/version"); err == nil {
		spec.Kernel = strings.TrimSpace(string(raw))
	}
	return spec
}

// --- evidence writer ---------------------------------------------------------

// perfEvidenceDirEnv overrides the perf evidence directory. Evidence never
// defaults into the repository working tree.
const perfEvidenceDirEnv = "TXHARBOR_PERF_EVIDENCE_DIR"

// EvidenceWriter persists the perf evidence: named JSON exports, JSONL raw
// samples and free-form files. It is safe for concurrent use.
type EvidenceWriter struct {
	dir string
	mu  sync.Mutex
}

// NewEvidenceWriter opens the evidence directory (the environment override or
// a fresh temp directory).
func NewEvidenceWriter() (*EvidenceWriter, error) {
	dir := os.Getenv(perfEvidenceDirEnv)
	if dir == "" {
		var err error
		dir, err = os.MkdirTemp("", "txharbor-perf-evidence-*")
		if err != nil {
			return nil, err
		}
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &EvidenceWriter{dir: dir}, nil
}

// Dir returns the evidence directory.
func (w *EvidenceWriter) Dir() string { return w.dir }

// WriteJSON writes one named JSON document.
func (w *EvidenceWriter) WriteJSON(name string, data any) error {
	raw, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}
	return w.WriteFile(name, append(raw, '\n'))
}

// WriteJSONL appends one JSON object per line.
func (w *EvidenceWriter) WriteJSONL(name string, rows []Sample) error {
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	for _, row := range rows {
		if err := encoder.Encode(row); err != nil {
			return err
		}
	}
	return w.WriteFile(name, buf.Bytes())
}

// WriteFile writes one named file.
func (w *EvidenceWriter) WriteFile(name string, body []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return os.WriteFile(filepath.Join(w.dir, name), body, 0o644)
}

// --- report renderer ---------------------------------------------------------

// BenchRun is one repeat: the path A and path B results of the same profile.
type BenchRun struct {
	Run   int
	PathA PathResult
	PathB PathResult
}

// ReportInput is everything the renderer needs.
type ReportInput struct {
	Commit      string
	GoVersion   string
	Profile     Profile
	Host        HostSpec
	Runs        []BenchRun
	RawDir      string
	GeneratedAt time.Time
}

// RenderReport renders the V-BENCH report from measured data. Every numeric
// field is a measured value or "待测/待裁决"; the renderer never writes
// "已达标" (SC-11; verification.md §5).
func RenderReport(in ReportInput) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# 013 对照基准报告（V-BENCH / SC-11）\n\n")
	fmt.Fprintf(&b, "- Feature: `013-reliable-event-infrastructure`\n")
	fmt.Fprintf(&b, "- 生成时间: %s\n", in.GeneratedAt.UTC().Format(time.RFC3339))
	fmt.Fprintf(&b, "- commit: `%s`（证据绑定）\n", in.Commit)
	fmt.Fprintf(&b, "- Go: `%s`\n", in.GoVersion)
	fmt.Fprintf(&b, "- 原始证据目录（未入库）: `%s`\n", in.RawDir)
	fmt.Fprintf(&b, "- 运行次数（每路径每重复一轮）: %d\n", len(in.Runs))
	fmt.Fprintf(&b, "- 层: Performance（独立运行，不进普通 PR；FR-28）\n\n")
	fmt.Fprintf(&b, "> 表述纪律：本报告只报告实测值；未测数值一律标「待测/待裁决」，0 次表述为「已达标」；"+
		"本地单主机结果不构成生产容量结论；Kafka/Redis 范围调整须按 PD-3 另行提交用户决定，本报告不自动删减。\n\n")

	renderMethod(&b, in)
	renderEnvironment(&b, in)
	renderPath(&b, "3. 路径 A（PG-only 基线）结果", in, func(run BenchRun) PathResult { return run.PathA })
	renderPath(&b, "4. 路径 B（全栈：缓存/限流 + 事件通道）结果", in, func(run BenchRun) PathResult { return run.PathB })
	renderComparison(&b, in)
	renderResources(&b, in)
	renderRepeats(&b, in)
	renderConclusions(&b, in)
	renderUnmeasured(&b, in)
	return b.String()
}

func renderMethod(b *strings.Builder, in ReportInput) {
	b.WriteString("## 1. 方法（同负载同故障对照；adr.md §3）\n\n")
	b.WriteString("**路径定义**\n\n")
	b.WriteString("- **A（PG-only）**：PostgreSQL + Anvil；013 接线关闭（无 Redis/Kafka、无发布器/消费者、无缓存/限流/容量门禁）。" +
		"生产者的 Append 集成无条件生效，因此事件在 outbox 累积但不投递（adr.md §3 路径 A 明确允许，仅用于对照实验）。\n")
	b.WriteString("- **B（全栈）**：PostgreSQL + Anvil + Redis + Kafka；缓存、限流、容量门禁、发布器与消费者全开。\n\n")
	b.WriteString("**受控变量**：同一主机；同一 PG 镜像/规格；同一 Anvil 镜像；同一数据集与初始状态（种子请求 + 授权池，两路径一致）；" +
		"同一负载生成器（固定速率阶梯与突发，见下表）；同一故障注入时间线（两路径一致；路径 A 无 Redis/Kafka 依赖，故障态按时间线记录但无依赖可注入）。\n\n")
	b.WriteString("**负载生成器（固定 profile）**\n\n")
	fmt.Fprintf(b, "| 项 | 值 |\n|---|---|\n")
	fmt.Fprintf(b, "| 预热（不计入测量） | %s |\n", in.Profile.Warmup)
	fmt.Fprintf(b, "| 每状态窗口 | %s |\n", in.Profile.StateWindow)
	for _, step := range in.Profile.QueryLadder {
		fmt.Fprintf(b, "| 查询阶梯（窗口内偏移 %s 起） | %d rps |\n", step.Offset, step.RPS)
	}
	fmt.Fprintf(b, "| 查询突发 | %d 请求 @ %s |\n", in.Profile.Burst.Size, in.Profile.Burst.At)
	fmt.Fprintf(b, "| 创建（POST /withdrawals） | %d rps |\n", in.Profile.CreateRPS)
	fmt.Fprintf(b, "| 链上存款（Anvil Transfer 事实） | %d rps |\n", in.Profile.DepositRPS)
	b.WriteString("\n**故障时间线（两路径一致）**\n\n")
	b.WriteString("| # | 状态 | 注入 |\n|---|---|---|\n")
	b.WriteString("| 1 | normal | 无 |\n")
	b.WriteString("| 2 | redis_down | 停止 Redis（Kafka 保持） |\n")
	b.WriteString("| 3 | kafka_down | 挂起 Kafka 进程（SIGSTOP，保留日志与位点；Redis 恢复） |\n")
	b.WriteString("| 4 | dual | Kafka 挂起 + Redis 停止（PG/链保持） |\n")
	b.WriteString("| 5 | recovery | 恢复 Kafka（SIGCONT）+ 启动 Redis，测量排空与消费追赶 |\n\n")
	b.WriteString("**测量口径**\n\n")
	b.WriteString("- API 延迟/吞吐/错误率：负载生成器逐请求采样（原始样本存证），百分位为线性插值；吞吐 = 完成请求数/窗口（排除预热）；" +
		"错误率分子 = 非 2xx/3xx 响应（含 PD-1 可重试拒绝）与传输错误。\n")
	b.WriteString("- RPC：链侧存款操作（Anvil JSON-RPC：setCode/sendTransaction/receipt 轮询）的逐次耗时作为 RPC 路径的实测代理；" +
		"按调用类的服务端 RPC 延迟直方图不在本 feature 的指标集内（标待测/待裁决，见 §9）。\n")
	b.WriteString("- 服务端序列：/metrics 暴露导出（限流/缓存/容量/outbox gauge）；outbox 待发、最老等待、尝试总数、消费缺口（published-not-applied）、" +
		"Kafka 高水位与消费组已提交位点直接读真实 PG/Kafka；发布失败/隔离以发布器与消费者的结构化记录计数（其命令无 metrics 端点）。\n")
	b.WriteString("- 追赶时间：恢复时刻起（250ms 轮询）到消费缺口归零；排空时间：恢复时刻起到 outbox 待发与消费缺口同时归零（界内未达则标待测）。" +
		"恢复时刻执行一次 drill-local 退避清零（`next_attempt_at=now()`，与 B9 一致），使追赶/排空反映发布与消费能力而非退避相位；不改事件内容、不删除/覆盖事件。\n")
	b.WriteString("- 资源：进程级 CPU（/proc/self/stat，CLK_TCK=100 假设）、RSS、/proc/self/io、命名空间网络计数与 PG 连接池；" +
		"负载生成器与服务同进程，故 CPU/IO 为上界（见置信限制）。\n\n")
}

func renderEnvironment(b *strings.Builder, in ReportInput) {
	b.WriteString("## 2. 环境规格（绑定 commit）\n\n")
	fmt.Fprintf(b, "| 项 | 值 |\n|---|---|\n")
	fmt.Fprintf(b, "| CPU | %s（%d 核） |\n", in.Host.CPUModel, in.Host.CPUCores)
	fmt.Fprintf(b, "| 内存 | %d MiB |\n", in.Host.MemTotalKB/1024)
	fmt.Fprintf(b, "| OS/Arch | %s/%s |\n", in.Host.OS, in.Host.Arch)
	fmt.Fprintf(b, "| 内核 | %s |\n", strings.TrimSpace(in.Host.Kernel))
	if len(in.Runs) > 0 {
		spec := in.Runs[0].PathA.Spec
		fmt.Fprintf(b, "| PG 镜像 | `%s`（默认容器参数） |\n", spec.PGImage)
		fmt.Fprintf(b, "| Anvil 镜像 | `%s` |\n", spec.AnvilImage)
		fmt.Fprintf(b, "| Kafka 镜像 / topic / 分区 | `%s` / `%s` / %d |\n", spec.KafkaImage, spec.KafkaTopic, spec.KafkaPartitions)
		fmt.Fprintf(b, "| Redis 镜像 | `%s` |\n", spec.RedisImage)
		fmt.Fprintf(b, "| 数据集规模 | %s |\n", spec.DatasetScale)
	}
	b.WriteString("\n**路径 B 配置输入（bench 输入，非生产阈值；生产阈值待测/待裁决）**\n\n")
	if len(in.Runs) > 0 {
		b.WriteString("| 键 | 值 |\n|---|---|\n")
		keys := make([]string, 0, len(in.Runs[0].PathB.Spec.Config))
		for key := range in.Runs[0].PathB.Spec.Config {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			fmt.Fprintf(b, "| `%s` | `%s` |\n", key, in.Runs[0].PathB.Spec.Config[key])
		}
	}
	b.WriteString("\n")
}

func renderPath(b *strings.Builder, title string, in ReportInput, pick func(BenchRun) PathResult) {
	fmt.Fprintf(b, "## %s\n\n", title)
	for _, run := range in.Runs {
		result := pick(run)
		fmt.Fprintf(b, "### run %d（%s，%s → %s）\n\n", run.Run, result.Path, result.StartedAt.Format(time.RFC3339), result.EndedAt.Format(time.RFC3339))
		renderStateTable(b, result.Path, result.States)
		renderStateResourceTable(b, result.Path, result.States)
	}
}

func renderStateTable(b *strings.Builder, path Path, states []StateResult) {
	b.WriteString("| 状态 | 窗口(s) | 查询 n | 查询 RPS | 查询 p50 (ms) | 查询 p95 (ms) | 查询 p99 (ms) | 查询错误率 | 创建 n | 创建 p50/p95/p99 (ms) | 创建错误率 | 存款 n | Outbox待发(后) | 最老等待(s) | 已发布(后) | 尝试总数(后) | 发布失败(日志) | 消费缺口(后) | 追赶(s) | 排空(s) |\n")
	b.WriteString("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|\n")
	for _, state := range states {
		publishFailures := "n/a"
		if path == PathFullStack {
			publishFailures = strconv.Itoa(state.PublishFailures)
		}
		fmt.Fprintf(b, "| %s | %.1f | %d | %s | %s | %s | %s | %s | %d | %s/%s/%s | %s | %d | %d | %s | %d | %d | %s | %s | %s | %s |\n",
			state.Label, state.WindowSeconds,
			state.Query.Count, num(state.Query.ThroughputRPS),
			ms(state.Query.P50MS), ms(state.Query.P95MS), ms(state.Query.P99MS), pct(state.Query.ErrorRate),
			state.Create.Count, ms(state.Create.P50MS), ms(state.Create.P95MS), ms(state.Create.P99MS), pct(state.Create.ErrorRate),
			state.Deposit.Count,
			state.After.OutboxPending, secs(state.After.OutboxOldestAgeSeconds), state.After.OutboxPublished, state.After.OutboxAttemptSum, publishFailures,
			intOrNA(state.After.CatchupGap),
			secsOrNA(state.CatchupSeconds), secsOrNA(state.DrainSeconds))
	}
	b.WriteString("\n")
	for _, state := range states {
		for _, note := range state.Notes {
			fmt.Fprintf(b, "- %s：%s\n", state.Label, note)
		}
	}
	if len(states) > 0 {
		b.WriteString("\n")
	}
}

func renderStateResourceTable(b *strings.Builder, path Path, states []StateResult) {
	b.WriteString("| 状态 | CPU(s) | RSS(MiB) | 磁盘读(MiB) | 磁盘写(MiB) | 网络rx(MiB) | 网络tx(MiB) | PG连接(总/占用) | 限流拒绝 | 缓存命中/未命中/回退 | 容量拒绝 |\n")
	b.WriteString("|---|---|---|---|---|---|---|---|---|---|---|\n")
	for _, state := range states {
		rateLimit, cache, capacity := "n/a", "n/a", "n/a"
		if path == PathFullStack {
			rateLimit = num(state.Scrape.RateLimitDenied)
			cache = fmt.Sprintf("%s/%s/%s", num(state.Scrape.CacheHits), num(state.Scrape.CacheMisses), num(state.Scrape.CacheFallbacks))
			capacity = num(state.Scrape.CapacityRefusals)
		}
		fmt.Fprintf(b, "| %s | %s | %s | %s | %s | %s | %s | %d/%d | %s | %s | %s |\n",
			state.Label,
			num(state.Resources.CPUSeconds),
			bytesMiB(state.Resources.RSSBytes),
			bytesMiB(state.Resources.DiskReadBytes), bytesMiB(state.Resources.DiskWriteBytes),
			bytesMiB(state.Resources.NetRxBytes), bytesMiB(state.Resources.NetTxBytes),
			state.Resources.After.PGTotalConns, state.Resources.After.PGAcquiredConns,
			rateLimit, cache, capacity)
	}
	b.WriteString("\n")
}

func renderComparison(b *strings.Builder, in ReportInput) {
	b.WriteString("## 5. A/B 对照（同负载同故障）\n\n")
	b.WriteString("### 5.1 正常态与故障态关键指标（跨重复中位数）\n\n")
	b.WriteString("| 状态 | 指标 | 路径 A | 路径 B | B−A |\n|---|---|---|---|---|\n")
	for _, state := range StateSequence {
		label := StateLabel[state]
		fmt.Fprintf(b, "| %s | 查询 p50 (ms) | %s | %s | %s |\n", label,
			medianOf(in, state, func(s StateResult) float64 { return s.Query.P50MS }, false),
			medianOf(in, state, func(s StateResult) float64 { return s.Query.P50MS }, true),
			deltaOf(in, state, func(s StateResult) float64 { return s.Query.P50MS }))
		fmt.Fprintf(b, "| %s | 查询 p95 (ms) | %s | %s | %s |\n", label,
			medianOf(in, state, func(s StateResult) float64 { return s.Query.P95MS }, false),
			medianOf(in, state, func(s StateResult) float64 { return s.Query.P95MS }, true),
			deltaOf(in, state, func(s StateResult) float64 { return s.Query.P95MS }))
		fmt.Fprintf(b, "| %s | 查询 p99 (ms) | %s | %s | %s |\n", label,
			medianOf(in, state, func(s StateResult) float64 { return s.Query.P99MS }, false),
			medianOf(in, state, func(s StateResult) float64 { return s.Query.P99MS }, true),
			deltaOf(in, state, func(s StateResult) float64 { return s.Query.P99MS }))
		fmt.Fprintf(b, "| %s | 查询错误率 | %s | %s | — |\n", label,
			medianOf(in, state, func(s StateResult) float64 { return s.Query.ErrorRate }, false),
			medianOf(in, state, func(s StateResult) float64 { return s.Query.ErrorRate }, true))
		fmt.Fprintf(b, "| %s | 创建 p95 (ms) | %s | %s | %s |\n", label,
			medianOf(in, state, func(s StateResult) float64 { return s.Create.P95MS }, false),
			medianOf(in, state, func(s StateResult) float64 { return s.Create.P95MS }, true),
			deltaOf(in, state, func(s StateResult) float64 { return s.Create.P95MS }))
		fmt.Fprintf(b, "| %s | 创建错误率 | %s | %s | — |\n", label,
			medianOf(in, state, func(s StateResult) float64 { return s.Create.ErrorRate }, false),
			medianOf(in, state, func(s StateResult) float64 { return s.Create.ErrorRate }, true))
	}
	b.WriteString("\n### 5.2 故障期降级行为对比\n\n")
	b.WriteString("| 状态 | 路径 A（无 Redis/Kafka 依赖） | 路径 B（全栈） |\n|---|---|---|\n")
	b.WriteString("| redis_down | 新提款创建继续（无分布式限流载体，错误率见上表）；查询直读 PG | PD-1：新提款创建按可重试 503 拒绝（错误率见上表）；查询回源 PG 并标注降级；链上处理继续 |\n")
	b.WriteString("| kafka_down | 无事件通道可故障；outbox 仅累积不投递（路径 A 定义） | 投递暂停、outbox 积压（见积压表）；业务继续、0 丢失；恢复后发布器排空 |\n")
	b.WriteString("| dual | 同 normal（无依赖） | 新创建拒绝 + 投递暂停；PG/链保持；恢复后补齐 |\n")
	b.WriteString("| recovery | 不适用（无追赶面） | 发布器排空 + 消费者追赶；追赶/排空时间见上表 |\n\n")
	b.WriteString("### 5.3 追赶时间（路径 B，恢复态）\n\n")
	fmt.Fprintf(b, "| 指标 | 值 |\n|---|---|\n")
	fmt.Fprintf(b, "| 消费缺口归零（恢复起，秒，跨重复中位数） | %s |\n",
		medianOf(in, StateRecovery, func(s StateResult) float64 { return s.CatchupSeconds }, true))
	fmt.Fprintf(b, "| 全链路排空（outbox 待发=0 且消费缺口=0，秒） | %s |\n",
		medianOf(in, StateRecovery, func(s StateResult) float64 { return s.DrainSeconds }, true))
	b.WriteString("| 追赶阈值配置项 | 待裁决（当前无独立「lag 阈值」配置；本报告以缺口归零为口径，比阈值口径更严格） |\n\n")
}

// rssMiB renders one state's RSS in MiB for the cross-run medians. The
// per-state tables use bytesMiB; a zero sample stays 待测 rather than 0.0,
// and the §6/§8 values are MiB, never raw bytes (the header says MiB).
func rssMiB(s StateResult) float64 {
	if s.Resources.RSSBytes == 0 {
		return math.NaN()
	}
	return float64(s.Resources.RSSBytes) / (1 << 20)
}

func renderResources(b *strings.Builder, in ReportInput) {
	b.WriteString("## 6. 资源占用（进程级，跨重复中位数）\n\n")
	b.WriteString("| 状态 | A CPU(s) | B CPU(s) | A RSS(MiB) | B RSS(MiB) |\n|---|---|---|---|---|\n")
	for _, state := range StateSequence {
		fmt.Fprintf(b, "| %s | %s | %s | %s | %s |\n", StateLabel[state],
			medianOf(in, state, func(s StateResult) float64 { return s.Resources.CPUSeconds }, false),
			medianOf(in, state, func(s StateResult) float64 { return s.Resources.CPUSeconds }, true),
			medianOf(in, state, rssMiB, false),
			medianOf(in, state, rssMiB, true))
	}
	b.WriteString("\n")
}

func renderRepeats(b *strings.Builder, in ReportInput) {
	b.WriteString("## 7. 重复运行与测量误差\n\n")
	b.WriteString("**单轮内离散度（查询 p95 的样本标准差，跨重复中位数）**\n\n")
	b.WriteString("| 状态 | A stddev (ms) | B stddev (ms) |\n|---|---|---|\n")
	for _, state := range StateSequence {
		fmt.Fprintf(b, "| %s | %s | %s |\n", StateLabel[state],
			medianOf(in, state, func(s StateResult) float64 { return s.Query.StdDevMS }, false),
			medianOf(in, state, func(s StateResult) float64 { return s.Query.StdDevMS }, true))
	}
	b.WriteString("\n")
	if len(in.Runs) < 2 {
		b.WriteString("本次只执行 1 轮重复：跨重复方差**待测/待裁决**（可设 `TXHARBOR_PERF_REPEATS>=2` 复跑）。\n\n")
		return
	}
	b.WriteString("下表为每路径每状态查询 p95 的逐轮值（ms）与极差（max−min），作为测量误差的观测口径；" +
		"单轮内方差见各状态 `stddev_ms`（原始样本存证）。\n\n")
	b.WriteString("| 状态 | A 逐轮 p95 | A 极差 | B 逐轮 p95 | B 极差 |\n|---|---|---|---|---|\n")
	for _, state := range StateSequence {
		var aVals, bVals []string
		var aNums, bNums []float64
		for _, run := range in.Runs {
			a := findState(run.PathA.States, state)
			bb := findState(run.PathB.States, state)
			if a != nil {
				aVals = append(aVals, ms(a.Query.P95MS))
				aNums = append(aNums, a.Query.P95MS)
			}
			if bb != nil {
				bVals = append(bVals, ms(bb.Query.P95MS))
				bNums = append(bNums, bb.Query.P95MS)
			}
		}
		fmt.Fprintf(b, "| %s | %s | %s | %s | %s |\n", StateLabel[state],
			strings.Join(aVals, ", "), spread(aNums), strings.Join(bVals, ", "), spread(bNums))
	}
	b.WriteString("\n")
}

func renderConclusions(b *strings.Builder, in ReportInput) {
	b.WriteString("## 8. 结论与置信限制\n\n")
	b.WriteString("**数据支持的结论（仅限本机本负载本窗口；不构成生产容量结论）**\n\n")
	// Normal-state query p95 A vs B.
	aNormal := medianOf(in, StateNormal, func(s StateResult) float64 { return s.Query.P95MS }, false)
	bNormal := medianOf(in, StateNormal, func(s StateResult) float64 { return s.Query.P95MS }, true)
	fmt.Fprintf(b, "- 正常态查询 p95：A=%s ms，B=%s ms（跨重复中位数；原始样本存证）。\n", aNormal, bNormal)
	// Redis-only create refusal.
	aRedis := medianOf(in, StateRedisDown, func(s StateResult) float64 { return s.Create.ErrorRate }, false)
	bRedis := medianOf(in, StateRedisDown, func(s StateResult) float64 { return s.Create.ErrorRate }, true)
	fmt.Fprintf(b, "- 仅 Redis 故障：新提款创建错误率 A=%s，B=%s（B 的拒绝为 PD-1 已裁决的可重试 503 行为；"+
		"存量流程与链上处理不受影响）。\n", aRedis, bRedis)
	// Kafka-only pending growth.
	aKafka := medianOf(in, StateKafkaDown, func(s StateResult) float64 { return float64(s.After.OutboxPending - s.Before.OutboxPending) }, false)
	bKafka := medianOf(in, StateKafkaDown, func(s StateResult) float64 { return float64(s.After.OutboxPending - s.Before.OutboxPending) }, true)
	fmt.Fprintf(b, "- 仅 Kafka 故障：窗口内 outbox 待发增量 A=%s，B=%s（A 的累积源于「013 接线关闭但 Append 无条件」的路径定义，不代表降级；"+
		"B 的累积为投递暂停、恢复后由发布器排空）。\n", aKafka, bKafka)
	// Recovery.
	fmt.Fprintf(b, "- 恢复追赶（B）：消费缺口归零=%s s，全链路排空=%s s（跨重复中位数；未达界标待测）。\n",
		medianOf(in, StateRecovery, func(s StateResult) float64 { return s.CatchupSeconds }, true),
		medianOf(in, StateRecovery, func(s StateResult) float64 { return s.DrainSeconds }, true))
	// Resources.
	fmt.Fprintf(b, "- 资源（正常态进程级 CPU，跨重复中位数）：A=%s s，B=%s s；RSS A=%s MiB，B=%s MiB（同进程含负载生成器，见限制）。\n",
		medianOf(in, StateNormal, func(s StateResult) float64 { return s.Resources.CPUSeconds }, false),
		medianOf(in, StateNormal, func(s StateResult) float64 { return s.Resources.CPUSeconds }, true),
		medianOf(in, StateNormal, rssMiB, false),
		medianOf(in, StateNormal, rssMiB, true))
	b.WriteString("\n**Kafka/Redis 价值结论（PD-3 边界）**\n\n")
	b.WriteString("上述数值只支持本机本负载下的对照观察：它们**不预判也不取消**Kafka/Redis 的范围；" +
		"若结果支持调整范围，MUST 另行提交具体变更供用户决定（PD-3），本报告不自动删减、不自动扩大。\n\n")
	b.WriteString("**置信限制**\n\n")
	b.WriteString("- 单主机、容器化、短窗口（每状态 12s）与固定合成负载；不是生产负载回放，无生产容量结论。\n")
	b.WriteString("- 负载生成器与服务（serve/发布器/消费者）同进程运行：进程级 CPU/IO 为包含生成器的上界；两路径测量口径一致，差值仍可比，绝对值不可直接外推。\n")
	b.WriteString("- 百分位为线性插值；延迟为客户端观测（含本机 HTTP/容器网络），与生产跨网络观测不可比。\n")
	b.WriteString("- Kafka 故障用进程挂起（保留日志与位点）模拟不可用；Redis 故障用容器停止/重启（同固定地址）。故障注入方式与真实故障模式存在差异。\n")
	b.WriteString("- 故障期数据受「故障注入时刻 + 500ms 稳定 + 12s 窗口」影响；不同窗口长度会得到不同追赶/积压数值。\n\n")
}

func renderUnmeasured(b *strings.Builder, in ReportInput) {
	b.WriteString("## 9. 未测 / 待裁决\n\n")
	b.WriteString("| 项 | 状态 |\n|---|---|\n")
	b.WriteString("| 生产容量阈值（soft/hard/reserve/retention/停机窗口） | 待测/待裁决（本报告只用 bench 输入） |\n")
	b.WriteString("| 服务端 RPC 延迟直方图（按调用类） | 待测/待裁决（本 feature 指标集未含；本报告以链侧 RPC 操作耗时为代理） |\n")
	b.WriteString("| 生产限流速率/突发 | 待测/待裁决（本报告只用 bench 输入） |\n")
	b.WriteString("| 追赶 lag 阈值配置项 | 待裁决（本报告以缺口归零为口径） |\n")
	b.WriteString("| 告警阈值校准 | 待测（T075 规则阈值来自配置/裁决，数值校准待生产测量） |\n")
	b.WriteString("| 长稳（小时级）与多主机/多实例基准 | 未测 |\n")
	b.WriteString("| 生产负载回放 | 未测 |\n")
	b.WriteString("| CI 耗时预算 | 未测（B11 T084） |\n")
	b.WriteString("| 生产 RPC / 公网环境 | 未测（本基准为本地确定性环境） |\n\n")
	b.WriteString("---\n\n")
	b.WriteString("本报告由 `internal/perf` 基准 harness 自动生成；原始逐请求样本、逐状态指标导出与环境规格见上述原始证据目录。\n")
}

// findState returns the state result or nil.
func findState(states []StateResult, state FaultState) *StateResult {
	for i := range states {
		if states[i].State == state {
			return &states[i]
		}
	}
	return nil
}

// medianOf renders the cross-run median of one metric for one path/state.
func medianOf(in ReportInput, state FaultState, metric func(StateResult) float64, pathB bool) string {
	var values []float64
	for _, run := range in.Runs {
		result := run.PathA
		if pathB {
			result = run.PathB
		}
		if s := findState(result.States, state); s != nil {
			values = append(values, metric(*s))
		}
	}
	median := medianFloat(values)
	return num(median)
}

// deltaOf renders B−A of the cross-run medians for one metric.
func deltaOf(in ReportInput, state FaultState, metric func(StateResult) float64) string {
	a := medianFloat(collect(in, state, metric, false))
	b := medianFloat(collect(in, state, metric, true))
	if math.IsNaN(a) || math.IsNaN(b) {
		return "待测"
	}
	return num(b - a)
}

func collect(in ReportInput, state FaultState, metric func(StateResult) float64, pathB bool) []float64 {
	var values []float64
	for _, run := range in.Runs {
		result := run.PathA
		if pathB {
			result = run.PathB
		}
		if s := findState(result.States, state); s != nil {
			values = append(values, metric(*s))
		}
	}
	return values
}

func medianFloat(values []float64) float64 {
	var ok []float64
	for _, value := range values {
		if !math.IsNaN(value) && value >= 0 {
			ok = append(ok, value)
		}
	}
	if len(ok) == 0 {
		return math.NaN()
	}
	sort.Float64s(ok)
	return percentile(ok, 50)
}

func spread(values []float64) string {
	if len(values) < 2 {
		return "待测"
	}
	minV, maxV := values[0], values[0]
	for _, value := range values {
		if value < minV {
			minV = value
		}
		if value > maxV {
			maxV = value
		}
	}
	return fmt.Sprintf("%.1f", maxV-minV)
}

// num renders a number or 待测.
func num(value float64) string {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return "待测"
	}
	return fmt.Sprintf("%.1f", value)
}

// ms renders milliseconds or 待测.
func ms(value float64) string {
	if math.IsNaN(value) || value < 0 {
		return "待测"
	}
	return fmt.Sprintf("%.1f", value)
}

// pct renders a rate as a percentage or 待测.
func pct(value float64) string {
	if math.IsNaN(value) {
		return "待测"
	}
	return fmt.Sprintf("%.1f%%", value*100)
}

// secs renders seconds or 待测.
func secs(value float64) string {
	if math.IsNaN(value) || value < 0 {
		return "待测"
	}
	return fmt.Sprintf("%.2f", value)
}

// secsOrNA renders seconds, "n/a" when the path has no such surface (-1) or
// 待测 when the value is missing (NaN).
func secsOrNA(value float64) string {
	if value < 0 {
		return "n/a"
	}
	return secs(value)
}

// intOrNA renders an integer, "n/a" when the path has no such surface (-1).
func intOrNA(value int64) string {
	if value < 0 {
		return "n/a"
	}
	return strconv.FormatInt(value, 10)
}

// bytesMiB renders bytes as MiB or 待测.
func bytesMiB(value uint64) string {
	if value == 0 {
		return "待测"
	}
	return fmt.Sprintf("%.1f", float64(value)/(1<<20))
}

// --- small shared helpers ----------------------------------------------------

// waitFor polls cond until it holds or the timeout elapses.
func waitFor(ctx context.Context, timeout time.Duration, cond func() (bool, error)) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		ok, err := cond()
		if err != nil {
			lastErr = err
		} else if ok {
			return nil
		}
		if time.Now().After(deadline) {
			if lastErr != nil {
				return fmt.Errorf("condition not reached within %s: %w", timeout, lastErr)
			}
			return fmt.Errorf("condition not reached within %s", timeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// runGit runs one git command (best effort; "unknown" when unavailable).
func runGit(args ...string) (string, error) {
	cmd := exec.Command("git", args...)
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return "", err
	}
	return out.String(), nil
}
